"""
AWS Lambda handler for processing ICESat-2 ATL06 data by morton cell.

Simple version - outputs plain parquet without geometry (smaller layer, no geopandas).

This function:
1. Receives pre-computed granule URLs from orchestrator (no CMR query needed)
2. Reads HDF5 files directly from S3 using h5coro
3. Calculates summary statistics for child cells
4. Writes plain parquet to S3

Event payload:
{
    "parent_morton": int,
    "parent_order": int,
    "child_order": int,
    "granule_urls": [str, ...],  # List of S3 URLs from catalog
    "s3_bucket": str,
    "s3_prefix": str,
    "s3_credentials": {
        "accessKeyId": str,
        "secretAccessKey": str,
        "sessionToken": str
    }
}

Note: s3_credentials are obtained by the orchestrator via earthaccess.login()
and passed to each Lambda invocation. Credentials are valid for ~1 hour.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import h5coro
import numpy as np
import pandas as pd
from h5coro import s3driver

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def calculate_cell_statistics(df_cell: pd.DataFrame, value_col='h_li', sigma_col='s_li') -> dict:
    """
    Calculate summary statistics for a cell.

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
        Dictionary of statistics
    """
    if len(df_cell) == 0:
        return {
            'count': 0,
            'min': np.nan,
            'max': np.nan,
            'mean_weighted': np.nan,
            'sigma_mean': np.nan,
            'variance': np.nan,
            'q25': np.nan,
            'q50': np.nan,
            'q75': np.nan
        }

    values = df_cell[value_col].values
    sigmas = df_cell[sigma_col].values

    q = np.quantile(values, [0.25, 0.5, 0.75])
    weights = 1.0 / (sigmas ** 2)
    weighted_mean = np.sum(values * weights) / np.sum(weights)
    sigma_mean = 1.0 / np.sqrt(np.sum(weights))

    return {
        'count': len(df_cell),
        'min': float(np.min(values)),
        'max': float(np.max(values)),
        'variance': float(np.var(values)),
        'q25': float(q[0]),
        'q50': float(q[1]),
        'q75': float(q[2]),
        'mean_weighted': float(weighted_mean),
        'sigma_mean': float(sigma_mean)
    }


def process_morton_cell(
    parent_morton: int,
    parent_order: int,
    child_order: int,
    granule_urls: List[str],
    s3_bucket: str,
    s3_prefix: str,
    s3_credentials: dict,
) -> Dict[str, Any]:
    """
    Process one parent morton cell: read from S3, calculate stats, write parquet.

    Uses h5coro S3Driver for in-place reads (no downloads).
    Handles empty results gracefully.

    Parameters
    ----------
    parent_morton : int
        Morton index of parent cell
    parent_order : int
        Order of parent morton cell (e.g., 6 or 7)
    child_order : int
        Order of child cells for statistics (typically 12)
    granule_urls : list
        List of S3 URLs to process (from pre-built catalog)
    s3_bucket : str
        S3 bucket for output parquet files
    s3_prefix : str
        S3 prefix for output parquet files
    s3_credentials : dict
        AWS S3 credentials for NSIDC access

    Returns
    -------
    dict
        Summary of processing: {parent_morton, cells_with_data, total_obs, parquet_path, error}
    """
    from mortie import (
        clip2order,
        generate_morton_children,
        geo2mort,
        mort2healpix,
    )

    logger.info(f"Processing morton cell: {parent_morton}")
    start_time = datetime.now()

    # ========================================================================
    # CHECK FOR GRANULES
    # ========================================================================

    if not granule_urls:
        logger.info(f"  No granules provided for morton {parent_morton} - skipping")
        return {
            'parent_morton': parent_morton,
            'cells_with_data': 0,
            'total_obs': 0,
            'parquet_path': None,
            'error': 'No granules found',
            'duration_s': (datetime.now() - start_time).total_seconds()
        }

    logger.info(f"  Processing {len(granule_urls)} granules from catalog")

    # ========================================================================
    # READ FILES FROM S3 WITH SPATIAL SUBSETTING
    # ========================================================================

    # Prepare credentials for h5coro S3Driver
    credentials = {
        'aws_access_key_id': s3_credentials['accessKeyId'],
        'aws_secret_access_key': s3_credentials['secretAccessKey'],
        'aws_session_token': s3_credentials['sessionToken']
    }

    all_dataframes = []
    files_processed = 0

    for s3_url in granule_urls:
        try:
            # Convert S3 URL to bucket/key format for S3Driver
            resource_path = s3_url.replace('s3://', '')

            # Initialize h5coro with S3Driver
            h5obj = h5coro.H5Coro(
                resource_path,
                s3driver.S3Driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False
            )

            # Process each ground track
            for g in ['gt1l', 'gt1r', 'gt2l', 'gt2r', 'gt3l', 'gt3r']:
                try:
                    # Read coordinates for spatial filtering
                    coord_data = h5obj.readDatasets([
                        f'/{g}/land_ice_segments/latitude',
                        f'/{g}/land_ice_segments/longitude'
                    ])

                    lats = coord_data[f'/{g}/land_ice_segments/latitude']
                    lons = coord_data[f'/{g}/land_ice_segments/longitude']

                    if len(lats) == 0:
                        continue

                    # MORTON INDEX FILTERING
                    midx18 = geo2mort(lats, lons, order=18)
                    midx_parent = clip2order(parent_order, midx18)
                    mask_spatial = midx_parent == parent_morton

                    if np.sum(mask_spatial) == 0:
                        continue

                    # Get bounding indices for hyperslice read
                    indices = np.where(mask_spatial)[0]
                    min_idx = int(indices[0])
                    max_idx = int(indices[-1]) + 1  # exclusive end

                    # Read ONLY the bounding range using hyperslice
                    data = h5obj.readDatasets([
                        {"dataset": f'/{g}/land_ice_segments/h_li', "hyperslice": [(min_idx, max_idx)]},
                        {"dataset": f'/{g}/land_ice_segments/h_li_sigma', "hyperslice": [(min_idx, max_idx)]},
                        {"dataset": f'/{g}/land_ice_segments/atl06_quality_summary', "hyperslice": [(min_idx, max_idx)]}
                    ])

                    # Apply mask to the sliced data (offset by min_idx)
                    mask_sliced = mask_spatial[min_idx:max_idx]
                    h_li = data[f'/{g}/land_ice_segments/h_li'][mask_sliced]
                    s_li = data[f'/{g}/land_ice_segments/h_li_sigma'][mask_sliced]
                    q_flag = data[f'/{g}/land_ice_segments/atl06_quality_summary'][mask_sliced]

                    # Quality filtering
                    quality_mask = q_flag == 0

                    if np.sum(quality_mask) == 0:
                        continue

                    # Build dataframe with quality-filtered data
                    # midx18 needs same slicing as data
                    midx_sliced = midx18[min_idx:max_idx][mask_sliced]
                    data_dict = {
                        'h_li': h_li[quality_mask],
                        's_li': s_li[quality_mask],
                        'midx': midx_sliced[quality_mask],
                    }
                    all_dataframes.append(pd.DataFrame(data_dict))

                except Exception as e:
                    # Track may not exist or may have errors - continue
                    logger.debug(f"  Error reading track {g}: {e}")
                    continue

            files_processed += 1

        except Exception as e:
            # File may be inaccessible or corrupted - continue
            logger.warning(f"  Error processing file {s3_url}: {e}")
            continue

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")

    if not all_dataframes:
        logger.info(f"  No data after filtering for morton {parent_morton} - skipping")
        return {
            'parent_morton': parent_morton,
            'cells_with_data': 0,
            'total_obs': 0,
            'parquet_path': None,
            'error': 'No data after filtering',
            'duration_s': (datetime.now() - start_time).total_seconds()
        }

    df_all = pd.concat(all_dataframes, ignore_index=True)
    logger.info(f"  Read {len(df_all):,} observations")

    # ========================================================================
    # CALCULATE STATISTICS
    # ========================================================================

    logger.info(f"  Calculating statistics for order-{child_order} cells...")

    children = generate_morton_children(parent_morton, child_order)
    df_all['m12'] = clip2order(child_order, df_all['midx'].values)

    n_cells = len(children)
    stats_arrays = {
        'count': np.zeros(n_cells, dtype=np.int32),
        'min': np.full(n_cells, np.nan, dtype=np.float32),
        'max': np.full(n_cells, np.nan, dtype=np.float32),
        'mean_weighted': np.full(n_cells, np.nan, dtype=np.float32),
        'sigma_mean': np.full(n_cells, np.nan, dtype=np.float32),
        'variance': np.full(n_cells, np.nan, dtype=np.float32),
        'q25': np.full(n_cells, np.nan, dtype=np.float32),
        'q50': np.full(n_cells, np.nan, dtype=np.float32),
        'q75': np.full(n_cells, np.nan, dtype=np.float32),
    }

    cells_with_data = 0
    for i, child_morton in enumerate(children):
        df_cell = df_all[df_all['m12'] == child_morton]
        if len(df_cell) > 0:
            cells_with_data += 1
        stats = calculate_cell_statistics(df_cell, value_col='h_li', sigma_col='s_li')
        for key, value in stats.items():
            stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # ========================================================================
    # CREATE PARQUET OUTPUT (no geometry)
    # ========================================================================

    logger.info(f"  Creating parquet output...")

    child_cell_ids, _ = mort2healpix(children)

    # Build output DataFrame (no geometry)
    df_out = pd.DataFrame({
        'child_morton': children,
        'child_healpix': child_cell_ids,
        'count': stats_arrays['count'],
        'h_mean': stats_arrays['mean_weighted'],
        'h_sigma': stats_arrays['sigma_mean'],
        'h_min': stats_arrays['min'],
        'h_max': stats_arrays['max'],
        'h_variance': stats_arrays['variance'],
        'h_q25': stats_arrays['q25'],
        'h_q50': stats_arrays['q50'],
        'h_q75': stats_arrays['q75'],
    })

    # ========================================================================
    # WRITE PARQUET TO S3
    # ========================================================================

    parquet_path = f"s3://{s3_bucket}/{s3_prefix}/{parent_morton}.parquet"

    logger.info(f"  Writing parquet to {parquet_path}...")

    try:
        df_out.to_parquet(parquet_path, index=False, engine='fastparquet')
        logger.info(f"✓ Wrote parquet: {parquet_path}")
    except Exception as e:
        logger.error(f"Failed to write parquet to {parquet_path}: {e}")
        return {
            'parent_morton': parent_morton,
            'cells_with_data': cells_with_data,
            'total_obs': int(stats_arrays['count'].sum()),
            'parquet_path': None,
            'error': f'Failed to write parquet: {str(e)}',
            'duration_s': (datetime.now() - start_time).total_seconds()
        }

    duration = (datetime.now() - start_time).total_seconds()

    logger.info(f"✓ Completed morton {parent_morton} in {duration:.1f}s")

    return {
        'parent_morton': parent_morton,
        'cells_with_data': cells_with_data,
        'total_obs': int(stats_arrays['count'].sum()),
        'parquet_path': parquet_path,
        'error': None,
        'duration_s': duration,
        'granule_count': len(granule_urls),
        'files_processed': files_processed
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function.

    Parameters
    ----------
    event : dict
        Lambda event payload with keys:
        - parent_morton: int
        - parent_order: int
        - child_order: int
        - granule_urls: list of S3 URLs
        - s3_bucket: str
        - s3_prefix: str
        - s3_credentials: dict with accessKeyId, secretAccessKey, sessionToken
    context : LambdaContext
        Lambda context object

    Returns
    -------
    dict
        Processing result
    """
    # Log the event for debugging
    logger.info("=" * 70)
    logger.info("Lambda invocation started")
    logger.info(f"Request ID: {context.aws_request_id}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Memory: {context.memory_limit_in_mb} MB")
    logger.info(f"Timeout: {context.get_remaining_time_in_millis() / 1000:.0f}s")
    logger.info("=" * 70)

    # Log structured event data
    logger.info(json.dumps({
        'event_type': 'lambda_invocation',
        'parent_morton': event.get('parent_morton'),
        'granule_count': len(event.get('granule_urls', [])),
        'child_order': event.get('child_order'),
        'request_id': context.aws_request_id
    }))

    try:
        # Validate required parameters
        required_params = ['parent_morton', 'parent_order', 'child_order', 'granule_urls', 's3_bucket', 's3_prefix', 's3_credentials']
        missing_params = [p for p in required_params if p not in event]

        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg})
            }

        # Validate s3_credentials structure
        s3_creds = event['s3_credentials']
        required_cred_keys = ['accessKeyId', 'secretAccessKey', 'sessionToken']
        missing_cred_keys = [k for k in required_cred_keys if k not in s3_creds]
        if missing_cred_keys:
            error_msg = f"Missing s3_credentials keys: {', '.join(missing_cred_keys)}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg})
            }

        # Process the morton cell
        result = process_morton_cell(
            parent_morton=event['parent_morton'],
            parent_order=event['parent_order'],
            child_order=event['child_order'],
            granule_urls=event['granule_urls'],
            s3_bucket=event['s3_bucket'],
            s3_prefix=event['s3_prefix'],
            s3_credentials=s3_creds,
        )

        # Log structured result
        logger.info(json.dumps({
            'event_type': 'processing_complete',
            'parent_morton': result['parent_morton'],
            'cells_with_data': result['cells_with_data'],
            'total_obs': result['total_obs'],
            'duration_s': result['duration_s'],
            'error': result.get('error'),
            'request_id': context.aws_request_id
        }))

        logger.info("=" * 70)
        logger.info("Lambda invocation completed successfully")
        logger.info("=" * 70)

        return {
            'statusCode': 200 if not result.get('error') else 500,
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Unhandled exception in Lambda handler: {e}")
        logger.exception(e)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Unhandled exception: {str(e)}',
                'parent_morton': event.get('parent_morton'),
                'request_id': context.aws_request_id
            })
        }
