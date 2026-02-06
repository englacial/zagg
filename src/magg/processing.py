"""
Cloud-agnostic processing functions for ICESat-2 ATL06 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from typing import List, Tuple

import h5coro
import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from magg.schema import DATA_VARS, ProcessingMetadata, _agg_fields, _get_schema_fields

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Aggregation function registry
# ---------------------------------------------------------------------------


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    """Inverse-variance weighted mean."""
    w = 1.0 / (weights**2)
    return float(np.sum(values * w) / np.sum(w))


def _weighted_sigma(values: np.ndarray, weights: np.ndarray) -> float:
    """Uncertainty of inverse-variance weighted mean."""
    w = 1.0 / (weights**2)
    return float(1.0 / np.sqrt(np.sum(w)))


AGG_FUNCTIONS: dict[str, Callable] = {
    "count": lambda values, **kw: len(values),
    "nanmin": lambda values, **kw: float(np.min(values)),
    "nanmax": lambda values, **kw: float(np.max(values)),
    "nanvar": lambda values, **kw: float(np.var(values)),
    "weighted_mean": lambda values, weight_col_values=None, **kw: _weighted_mean(
        values, weight_col_values
    ),
    "weighted_sigma": lambda values, weight_col_values=None, **kw: _weighted_sigma(
        values, weight_col_values
    ),
    "quantile": lambda values, q=0.5, **kw: float(np.quantile(values, q)),
}


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
        DataFrame with columns matching COORDS + DATA_VARS from schema
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
    dict
        Metadata with 'zarr_written': bool, 'error': str or None
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


def calculate_cell_statistics(df_cell: pd.DataFrame, value_col="h_li", sigma_col="s_li") -> dict:
    """
    Calculate summary statistics for a cell, driven by CellStatsSchema metadata.

    Parameters
    ----------
    df_cell : pd.DataFrame
        Dataframe containing observations for a single cell
    value_col : str
        Column name for elevation values
    sigma_col : str
        Column name for uncertainty values

    Returns
    -------
    dict
        Dictionary of statistics with keys matching DATA_VARS
    """
    agg_fields = _agg_fields()

    if len(df_cell) == 0:
        return {
            name: (0 if meta["agg"] == "count" else np.nan) for name, meta in agg_fields.items()
        }

    result = {}
    for name, meta in agg_fields.items():
        agg_name = meta["agg"]
        source = meta.get("source") or value_col
        params = dict(meta.get("params", {}))

        values = df_cell[source].values

        if agg_name == "count":
            result[name] = len(df_cell)
            continue

        # Resolve weight column if needed
        weight_col = params.pop("weight_col", None)
        if weight_col is not None:
            params["weight_col_values"] = df_cell[weight_col].values

        func = AGG_FUNCTIONS[agg_name]
        result[name] = func(values, **params)

    return result


def process_morton_cell(
    parent_morton: int,
    parent_order: int,
    child_order: int,
    granule_urls: List[str],
    s3_credentials: dict,
    h5coro_driver=None,
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """
    Process one parent morton cell: read data, calculate statistics, return DataFrame.

    This is a cloud-agnostic function that processes ICESat-2 data and returns
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

    Returns
    -------
    tuple
        (DataFrame, metadata_dict)
        - DataFrame with columns: child_morton, child_healpix, count, h_mean, h_sigma, h_min, h_max, h_variance, h_q25, h_q50, h_q75
        - metadata_dict with: parent_morton, cells_with_data, total_obs, granule_count, files_processed, duration_s, error
    """
    from mortie import (
        clip2order,
        generate_morton_children,
        geo2mort,
        mort2healpix,
    )

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
            # Convert S3 URL to path format for driver
            resource_path = s3_url.replace("s3://", "")

            # Initialize h5coro with driver
            h5obj = h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )

            # Process each ground track
            for g in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
                try:
                    # Read coordinates for spatial filtering
                    coord_data = h5obj.readDatasets(
                        [f"/{g}/land_ice_segments/latitude", f"/{g}/land_ice_segments/longitude"]
                    )

                    lats = coord_data[f"/{g}/land_ice_segments/latitude"]
                    lons = coord_data[f"/{g}/land_ice_segments/longitude"]

                    if len(lats) == 0:
                        continue

                    # Morton index filtering
                    midx18 = geo2mort(lats, lons, order=18)
                    midx_parent = clip2order(parent_order, midx18)
                    mask_spatial = midx_parent == parent_morton

                    if np.sum(mask_spatial) == 0:
                        continue

                    # Get bounding indices for hyperslice read
                    indices = np.where(mask_spatial)[0]
                    min_idx = int(indices[0])
                    max_idx = int(indices[-1]) + 1

                    # Read only the bounding range using hyperslice
                    data = h5obj.readDatasets(
                        [
                            {
                                "dataset": f"/{g}/land_ice_segments/h_li",
                                "hyperslice": [(min_idx, max_idx)],
                            },
                            {
                                "dataset": f"/{g}/land_ice_segments/h_li_sigma",
                                "hyperslice": [(min_idx, max_idx)],
                            },
                            {
                                "dataset": f"/{g}/land_ice_segments/atl06_quality_summary",
                                "hyperslice": [(min_idx, max_idx)],
                            },
                        ]
                    )

                    # Apply mask to the sliced data
                    mask_sliced = mask_spatial[min_idx:max_idx]
                    h_li = data[f"/{g}/land_ice_segments/h_li"][mask_sliced]
                    s_li = data[f"/{g}/land_ice_segments/h_li_sigma"][mask_sliced]
                    q_flag = data[f"/{g}/land_ice_segments/atl06_quality_summary"][mask_sliced]

                    # Quality filtering
                    quality_mask = q_flag == 0

                    if np.sum(quality_mask) == 0:
                        continue

                    # Build dataframe with quality-filtered data
                    midx_sliced = midx18[min_idx:max_idx][mask_sliced]
                    data_dict = {
                        "h_li": h_li[quality_mask],
                        "s_li": s_li[quality_mask],
                        "midx": midx_sliced[quality_mask],
                    }
                    all_dataframes.append(pd.DataFrame(data_dict))

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
    schema_fields = _get_schema_fields()
    stats_arrays = {}
    for name in DATA_VARS:
        meta = schema_fields[name]
        zarr_dtype = np.dtype(meta.get("zarr_dtype", "float32"))
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
        stats = calculate_cell_statistics(df_cell, value_col="h_li", sigma_col="s_li")
        for key, value in stats.items():
            stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Create output DataFrame
    child_cell_ids, _ = mort2healpix(children)

    df_out = pd.DataFrame({var: stats_arrays[var] for var in DATA_VARS})
    df_out = df_out.assign(morton=children, cell_ids=child_cell_ids)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"✓ Completed morton {parent_morton} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = int(stats_arrays["count"].sum())
    metadata["duration_s"] = duration

    return df_out, metadata
