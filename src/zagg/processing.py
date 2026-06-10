"""
Cloud-agnostic processing functions for aggregating HDF5 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.
"""

import logging
import warnings
from datetime import datetime
from typing import List, Tuple

import h5coro
import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.config import PipelineConfig, default_config, get_agg_fields, get_data_vars
from zagg.schema import ProcessingMetadata

logger = logging.getLogger(__name__)


def _make_url_rewriter(driver: str | None):
    """Return a function that converts a granule URL for the active h5coro driver.

    The ShardMap carries the driver-appropriate href already (S3 vs HTTPS is
    chosen at dispatch), so this only strips the ``s3://`` scheme for the S3
    driver (h5coro's S3Driver expects ``bucket/key``); HTTPS is used as-is.
    """
    if driver == "https":
        return lambda url: url
    return lambda url: url.replace("s3://", "", 1)


def write_dataframe_to_zarr(
    df_out: pd.DataFrame,
    store: Store,
    *,
    grid,
    chunk_idx: tuple,
) -> Store:
    """Write a per-shard DataFrame to an existing Zarr template.

    Parameters
    ----------
    df_out : pd.DataFrame
        Coordinate + data-variable columns. Row count must equal
        ``prod(grid.chunk_shape)``; rows are in the grid's canonical
        chunk order (``grid.children(shard_key)``).
    store : Store
        Zarr-compatible store with the template already written.
    grid : OutputGrid
        Grid the data was aggregated against. Provides ``group_path`` and
        ``chunk_shape`` for routing the write.
    chunk_idx : tuple of int
        Storage block index for this shard, as returned by
        ``grid.block_index(shard_key)``.

    Returns
    -------
    Store
        The same store, with data written.
    """
    if df_out.empty:
        return store

    expected_count = int(np.prod(grid.chunk_shape))
    if len(df_out) != expected_count:
        raise ValueError(
            f"Expected {expected_count} rows for chunk_shape={grid.chunk_shape}, "
            f"got {len(df_out)}"
        )

    chunk_idx = tuple(int(i) for i in chunk_idx)
    for name, series in df_out.items():
        values = series.values
        if values.shape != grid.chunk_shape:
            values = values.reshape(grid.chunk_shape)
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store, path=f"{grid.group_path}/{name}",
                zarr_format=3, consolidated=False,
            )
            array.set_block_selection(chunk_idx, values)

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
    from zagg.config import evaluate_expression, resolve_function

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


def _read_group(h5obj, group: str, data_source: dict, parent_morton: int, grid):
    """Read and spatially filter one HDF5 group, returning a DataFrame or None."""
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    quality_filter = data_source.get("quality_filter")

    # Resolve coordinate paths
    coord_paths = [path.format(group=group) for path in coordinates.values()]
    coord_data = h5obj.readDatasets(coord_paths)

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = coord_data[lat_path]
    lons = coord_data[lon_path]

    if len(lats) == 0:
        return None

    # Assign points to leaf cells, then filter to the current shard.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == parent_morton

    if np.sum(mask_spatial) == 0:
        return None

    # Bounding indices for hyperslice read
    indices = np.where(mask_spatial)[0]
    min_idx = int(indices[0])
    max_idx = int(indices[-1]) + 1

    # Build hyperslice dataset list: variables + optional quality filter
    datasets = []
    for path_template in variables.values():
        path = path_template.format(group=group)
        datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})

    if quality_filter is not None:
        qf_path = quality_filter["dataset"].format(group=group)
        datasets.append({"dataset": qf_path, "hyperslice": [(min_idx, max_idx)]})

    data = h5obj.readDatasets(datasets)

    # Apply spatial mask to sliced data
    mask_sliced = mask_spatial[min_idx:max_idx]

    # Apply quality filter if configured
    if quality_filter is not None:
        qf_path = quality_filter["dataset"].format(group=group)
        q_flag = data[qf_path][mask_sliced]
        quality_mask = q_flag == quality_filter["value"]
        if np.sum(quality_mask) == 0:
            return None
    else:
        quality_mask = None

    # Build dataframe
    leaf_sliced = leaf_ids[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path_template in variables.items():
        path = path_template.format(group=group)
        values = data[path][mask_sliced]
        if quality_mask is not None:
            values = values[quality_mask]
        data_dict[col_name] = values

    if quality_mask is not None:
        data_dict["leaf_id"] = leaf_sliced[quality_mask]
    else:
        data_dict["leaf_id"] = leaf_sliced

    return pd.DataFrame(data_dict)


def process_shard(
    grid,
    shard_key: int,
    granule_urls: List[str],
    *,
    s3_credentials: dict,
    h5coro_driver=None,
    config: PipelineConfig | None = None,
    driver: str | None = None,
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """Process one shard: read granules, filter to this shard, aggregate, return df.

    Grid-agnostic. For HEALPix, ``shard_key`` is the parent morton ID; for
    rectilinear, the packed ``rb * n_col_blocks + cb`` chunk index.

    Parameters
    ----------
    grid : OutputGrid
        Output grid (provides ``assign``/``shards_of``/``children``/
        ``encode_cell_ids``/``chunk_coords``).
    shard_key : int
        Shard identifier (grid-specific encoding).
    granule_urls : list of str
        S3 URLs or file paths to read.
    s3_credentials : dict
        For S3: ``accessKeyId``/``secretAccessKey``/``sessionToken``.
        For HTTPS: ``{"edl_token": "..."}``.
    h5coro_driver : class, optional
        Overrides ``driver``.
    config : PipelineConfig, optional
        Defaults to ``default_config()``.
    driver : str, optional
        ``"s3"`` (default) or ``"https"``.

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``granule_count``,
        ``files_processed``, ``duration_s``, ``error``.
    """
    if config is None:
        config = default_config()
    data_source = config.data_source

    parent_morton = int(shard_key)
    logger.info(f"Processing shard: {parent_morton}")
    start_time = datetime.now()

    # Resolve driver
    if h5coro_driver is None:
        if driver is None:
            driver = config.data_source.get("driver", "s3")
        if driver == "https":
            from h5coro import webdriver
            h5coro_driver = webdriver.HTTPDriver
        else:
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
    if driver == "https":
        credentials = s3_credentials.get("edl_token", s3_credentials)
    else:
        credentials = {
            "aws_access_key_id": s3_credentials.get("accessKeyId")
            or s3_credentials.get("aws_access_key_id"),
            "aws_secret_access_key": s3_credentials.get("secretAccessKey")
            or s3_credentials.get("aws_secret_access_key"),
            "aws_session_token": s3_credentials.get("sessionToken")
            or s3_credentials.get("aws_session_token"),
        }

    # Build URL rewriter for the active driver
    _rewrite_url = _make_url_rewriter(driver)

    all_dataframes = []
    files_processed = 0

    # Read files and filter spatially
    for s3_url in granule_urls:
        try:
            resource_path = _rewrite_url(s3_url)

            h5obj = h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )

            for g in data_source["groups"]:
                try:
                    df = _read_group(h5obj, g, data_source, parent_morton, grid)
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
    children = grid.children(parent_morton)
    logger.info(f"  Calculating statistics for {len(children)} cells...")

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
    cell_col = grid.cells_of(df_all["leaf_id"].values)
    for i, child_morton in enumerate(children):
        df_cell = df_all[cell_col == child_morton]
        if len(df_cell) > 0:
            cells_with_data += 1
        stats = calculate_cell_statistics(df_cell, value_col="h_li", sigma_col="s_li", config=config)
        for key, value in stats.items():
            stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Create output DataFrame: data_vars + grid-specific per-cell coord columns
    df_out = pd.DataFrame({var: stats_arrays[var] for var in data_vars})
    for col_name, vals in grid.chunk_coords(shard_key).items():
        df_out[col_name] = vals

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {parent_morton} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = int(stats_arrays["count"].sum())
    metadata["duration_s"] = duration

    return df_out, metadata


def process_morton_cell(
    parent_morton: int,
    parent_order: int,
    child_order: int,
    granule_urls: List[str],
    s3_credentials: dict,
    h5coro_driver=None,
    config: PipelineConfig | None = None,
    driver: str | None = None,
    grid=None,
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """Deprecated HEALPix-flavored alias for :func:`process_shard`.

    Constructs a stateless ``HealpixGrid`` and forwards to ``process_shard``.
    """
    warnings.warn(
        "process_morton_cell is deprecated; use process_shard(grid, shard_key, ...) "
        "directly.",
        DeprecationWarning,
        stacklevel=2,
    )
    if grid is None:
        from zagg.grids import HealpixGrid

        grid = HealpixGrid(
            parent_order=parent_order,
            child_order=child_order,
            layout="fullsphere",
            config=config or default_config(),
        )
    return process_shard(
        grid,
        parent_morton,
        granule_urls,
        s3_credentials=s3_credentials,
        h5coro_driver=h5coro_driver,
        config=config,
        driver=driver,
    )
