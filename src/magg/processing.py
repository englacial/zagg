"""
Cloud-agnostic processing functions for aggregating HDF5 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.
"""

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Tuple

import h5coro
import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from magg.config import PipelineConfig, default_config, get_agg_fields, get_data_vars
from magg.schema import ProcessingMetadata

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data source configuration
# ---------------------------------------------------------------------------


@dataclass
class DataSourceConfig:
    """Configuration for reading data from HDF5 files.

    Parameters
    ----------
    groups : list[str]
        HDF5 group names to iterate over.
    coordinates : dict[str, str]
        Mapping of coordinate name to HDF5 path template.
        Templates use ``{group}`` placeholder.
    variables : dict[str, str]
        Mapping of output column name to HDF5 path template.
        Templates use ``{group}`` placeholder.
    quality_filter : dict or None
        Optional quality filter with ``dataset`` (path template) and
        ``value`` (the "good" value to keep via equality check).
    """

    groups: list[str]
    coordinates: dict[str, str]
    variables: dict[str, str]
    quality_filter: dict | None = None

    def validate_schema(self, agg_fields: dict | None = None) -> None:
        """Validate that schema aggregation fields reference columns this config provides.

        Parameters
        ----------
        agg_fields : dict, optional
            Schema aggregation fields. If None, reads from default config.

        Raises
        ------
        ValueError
            If any ``source`` or param column reference is not in
            ``self.variables``.
        """
        if agg_fields is None:
            agg_fields = get_agg_fields(default_config())
        available = set(self.variables.keys())
        missing = set()
        for name, meta in agg_fields.items():
            source = meta.get("source")
            if source is not None and source not in available:
                missing.add(source)
            for pval in meta.get("params", {}).values():
                if not isinstance(pval, str):
                    continue
                # Bare column reference
                if pval in available:
                    continue
                # Expression containing column names
                if any(v in pval for v in available):
                    continue
                missing.add(pval)
        if missing:
            raise ValueError(
                f"Schema references columns {missing} not provided by "
                f"DataSourceConfig (available: {available})"
            )

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict (for Lambda payloads)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "DataSourceConfig":
        """Deserialize from a dict."""
        return cls(**d)


ATL06_CONFIG = DataSourceConfig(
    groups=["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"],
    coordinates={
        "latitude": "/{group}/land_ice_segments/latitude",
        "longitude": "/{group}/land_ice_segments/longitude",
    },
    variables={
        "h_li": "/{group}/land_ice_segments/h_li",
        "s_li": "/{group}/land_ice_segments/h_li_sigma",
    },
    quality_filter={
        "dataset": "/{group}/land_ice_segments/atl06_quality_summary",
        "value": 0,
    },
)


def write_dataframe_to_zarr(
    df_out: pd.DataFrame,
    store: Store,
    *,
    chunk_idx: int,
    child_order: int,
    parent_order: int,
) -> Store:
    """
    Write a DataFrame to an existing Zarr store.

    Parameters
    ----------
    df_out : pd.DataFrame
        DataFrame with columns matching CellStatsSchema
    store : Store
        Zarr-compatible store (already contains template)
    chunk_idx : int
        The chunk index for storing data
    child_order : int
        Order of child cells
    parent_order : int
        Order of parent cells

    Returns
    -------
    Store
        The same store, with data written
    """
    if df_out.empty:
        return store
    min_index = int(df_out["cell_ids"].min())
    max_index = int(df_out["cell_ids"].max())

    expected_count = 4 ** (child_order - parent_order)
    actual_count = max_index - min_index + 1
    if actual_count != expected_count:
        raise ValueError(
            f"Expected index range to match range between min and max cell_ids, got index_range={expected_count}, actual_range={actual_count}"
        )

    for name, series in df_out.items():
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store, path=f"{str(child_order)}/{name}", zarr_format=3, consolidated=False
            )
            array.set_block_selection((chunk_idx,), series.values)

    return store


def calculate_cell_statistics(
    df_cell: pd.DataFrame,
    value_col="h_li",
    sigma_col="s_li",
    config: PipelineConfig | None = None,
) -> dict:
    """
    Calculate summary statistics for a cell, driven by pipeline config metadata.

    Parameters
    ----------
    df_cell : pd.DataFrame
        Dataframe containing observations for a single cell
    value_col : str
        Column name for elevation values
    sigma_col : str
        Column name for uncertainty values
    config : PipelineConfig, optional
        Pipeline config to use for dispatch. Defaults to ``default_config()``.

    Returns
    -------
    dict
        Dictionary of statistics keyed by aggregation variable name
    """
    from magg.config import evaluate_expression, resolve_function

    if config is None:
        config = default_config()
    agg_fields = get_agg_fields(config)

    if len(df_cell) == 0:
        return {
            name: (0 if meta.get("function") in ("len", "count") else np.nan)
            for name, meta in agg_fields.items()
        }

    result = {}
    for name, meta in agg_fields.items():
        func_name = meta.get("function")
        expression = meta.get("expression")
        source = meta.get("source") or value_col
        params = dict(meta.get("params", {}))

        # Expression-based aggregation (e.g. h_sigma)
        if expression:
            columns = {col: df_cell[col].values for col in df_cell.columns}
            result[name] = evaluate_expression(expression, columns)
            continue

        values = df_cell[source].values

        # Count via len
        if func_name in ("len", "count"):
            result[name] = len(df_cell)
            continue

        # Resolve params: bare column name -> array, expression -> eval'd
        resolved_params = {}
        for pkey, pval in params.items():
            if isinstance(pval, str) and pval in df_cell.columns:
                resolved_params[pkey] = df_cell[pval].values
            elif isinstance(pval, str) and any(c in pval for c in df_cell.columns):
                ns = {"__builtins__": {}, "np": np, "numpy": np,
                      **{c: df_cell[c].values for c in df_cell.columns}}
                resolved_params[pkey] = eval(pval, ns)  # noqa: S307
            else:
                resolved_params[pkey] = pval

        func = resolve_function(func_name)
        result[name] = float(func(values, **resolved_params))

    return result


def _read_group(h5obj, group: str, data_source: DataSourceConfig, parent_morton: int,
                parent_order: int, geo2mort, clip2order):
    """Read and spatially filter one HDF5 group, returning a DataFrame or None."""
    # Resolve coordinate paths
    coord_paths = [path.format(group=group) for path in data_source.coordinates.values()]
    coord_data = h5obj.readDatasets(coord_paths)

    lat_path = data_source.coordinates["latitude"].format(group=group)
    lon_path = data_source.coordinates["longitude"].format(group=group)
    lats = coord_data[lat_path]
    lons = coord_data[lon_path]

    if len(lats) == 0:
        return None

    # Morton index filtering
    midx18 = geo2mort(lats, lons, order=18)
    midx_parent = clip2order(parent_order, midx18)
    mask_spatial = midx_parent == parent_morton

    if np.sum(mask_spatial) == 0:
        return None

    # Bounding indices for hyperslice read
    indices = np.where(mask_spatial)[0]
    min_idx = int(indices[0])
    max_idx = int(indices[-1]) + 1

    # Build hyperslice dataset list: variables + optional quality filter
    datasets = []
    for path_template in data_source.variables.values():
        path = path_template.format(group=group)
        datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})

    has_quality = data_source.quality_filter is not None
    if has_quality:
        qf_path = data_source.quality_filter["dataset"].format(group=group)
        datasets.append({"dataset": qf_path, "hyperslice": [(min_idx, max_idx)]})

    data = h5obj.readDatasets(datasets)

    # Apply spatial mask to sliced data
    mask_sliced = mask_spatial[min_idx:max_idx]

    # Apply quality filter if configured
    if has_quality:
        qf_path = data_source.quality_filter["dataset"].format(group=group)
        q_flag = data[qf_path][mask_sliced]
        quality_mask = q_flag == data_source.quality_filter["value"]
        if np.sum(quality_mask) == 0:
            return None
    else:
        quality_mask = None

    # Build dataframe
    midx_sliced = midx18[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path_template in data_source.variables.items():
        path = path_template.format(group=group)
        values = data[path][mask_sliced]
        if quality_mask is not None:
            values = values[quality_mask]
        data_dict[col_name] = values

    if quality_mask is not None:
        data_dict["midx"] = midx_sliced[quality_mask]
    else:
        data_dict["midx"] = midx_sliced

    return pd.DataFrame(data_dict)


def process_morton_cell(
    parent_morton: int,
    parent_order: int,
    child_order: int,
    granule_urls: List[str],
    s3_credentials: dict,
    h5coro_driver=None,
    data_source: DataSourceConfig | None = None,
    config: PipelineConfig | None = None,
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """
    Process one parent morton cell: read data, calculate statistics, return DataFrame.

    This is a cloud-agnostic function that processes HDF5 data and returns
    results as a DataFrame. The caller is responsible for writing the output.

    Parameters
    ----------
    parent_morton : int
        Morton index of parent cell
    parent_order : int
        Order of parent morton cell (e.g., 6 or 7)
    child_order : int
        Order of child cells for statistics (typically 12)
    granule_urls : list
        List of S3 URLs or file paths to process
    s3_credentials : dict
        Credentials for accessing data (format depends on driver)
    h5coro_driver : class, optional
        h5coro driver class to use (e.g., s3driver.S3Driver). If None, auto-detect.
    data_source : DataSourceConfig, optional
        Configuration for reading HDF5 data. Defaults to ATL06_CONFIG.
    config : PipelineConfig, optional
        Pipeline config for aggregation dispatch. Defaults to ``default_config()``.

    Returns
    -------
    tuple
        (DataFrame, metadata_dict)
    """
    from mortie import (
        clip2order,
        generate_morton_children,
        geo2mort,
        mort2healpix,
    )

    if config is None:
        config = default_config()
    if data_source is None:
        data_source = ATL06_CONFIG
    data_source.validate_schema()

    logger.info(f"Processing morton cell: {parent_morton}")
    start_time = datetime.now()

    # Auto-detect driver if not provided
    if h5coro_driver is None:
        from h5coro import s3driver

        h5coro_driver = s3driver.S3Driver

    # Prepare metadata
    metadata: ProcessingMetadata = {
        "parent_morton": parent_morton,
        "cells_with_data": 0,
        "total_obs": 0,
        "granule_count": len(granule_urls),
        "files_processed": 0,
        "duration_s": 0.0,
        "error": None,
    }

    # Check for granules
    if not granule_urls:
        logger.info(f"  No granules provided for morton {parent_morton} - skipping")
        metadata["error"] = "No granules found"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    logger.info(f"  Processing {len(granule_urls)} granules from catalog")

    # Prepare credentials for h5coro
    credentials = {
        "aws_access_key_id": s3_credentials.get("accessKeyId")
        or s3_credentials.get("aws_access_key_id"),
        "aws_secret_access_key": s3_credentials.get("secretAccessKey")
        or s3_credentials.get("aws_secret_access_key"),
        "aws_session_token": s3_credentials.get("sessionToken")
        or s3_credentials.get("aws_session_token"),
    }

    all_dataframes = []
    files_processed = 0

    # Read files and filter spatially
    for s3_url in granule_urls:
        try:
            resource_path = s3_url.replace("s3://", "")

            h5obj = h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )

            for g in data_source.groups:
                try:
                    df = _read_group(
                        h5obj, g, data_source, parent_morton, parent_order,
                        geo2mort, clip2order,
                    )
                    if df is not None:
                        all_dataframes.append(df)
                except Exception as e:
                    logger.debug(f"  Error reading track {g}: {e}")
                    continue

            files_processed += 1

        except Exception as e:
            logger.warning(f"  Error processing file {s3_url}: {e}")
            continue

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")
    metadata["files_processed"] = files_processed

    if not all_dataframes:
        logger.info(f"  No data after filtering for morton {parent_morton} - skipping")
        metadata["error"] = "No data after filtering"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    df_all = pd.concat(all_dataframes, ignore_index=True)
    logger.info(f"  Read {len(df_all):,} observations")

    # Calculate statistics for child cells
    logger.info(f"  Calculating statistics for order-{child_order} cells...")

    children = generate_morton_children(parent_morton, child_order)
    df_all["m12"] = clip2order(child_order, df_all["midx"].values)

    n_cells = len(children)
    data_vars = get_data_vars(config)
    agg_fields = get_agg_fields(config)
    stats_arrays = {}
    for name in data_vars:
        meta = agg_fields[name]
        zarr_dtype = np.dtype(meta.get("dtype", "float32"))
        fill_value = meta.get("fill_value", "NaN")
        if fill_value == "NaN":
            stats_arrays[name] = np.full(n_cells, np.nan, dtype=zarr_dtype)
        else:
            stats_arrays[name] = np.zeros(n_cells, dtype=zarr_dtype)

    cells_with_data = 0
    for i, child_morton in enumerate(children):
        df_cell = df_all[df_all["m12"] == child_morton]
        if len(df_cell) > 0:
            cells_with_data += 1
        stats = calculate_cell_statistics(df_cell, value_col="h_li", sigma_col="s_li", config=config)
        for key, value in stats.items():
            stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Create output DataFrame
    child_cell_ids, _ = mort2healpix(children)

    df_out = pd.DataFrame({var: stats_arrays[var] for var in data_vars})
    df_out = df_out.assign(morton=children, cell_ids=child_cell_ids)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed morton {parent_morton} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = int(stats_arrays["count"].sum())
    metadata["duration_s"] = duration

    return df_out, metadata
