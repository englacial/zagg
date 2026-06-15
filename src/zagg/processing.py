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


def _group_columns(
    col_dict: dict[str, np.ndarray],
    cell_col: np.ndarray,
) -> tuple[dict[str, np.ndarray], dict[int, tuple[int, int]]]:
    """Sort column arrays by cell id; return reordered arrays and per-cell slice map.

    Carrier-agnostic core shared by the pandas and Arrow handoff paths. ``col_dict``
    is a plain ``name -> ndarray`` mapping (extracted from a DataFrame or an Arrow
    table); the math below is identical regardless of carrier, so both paths produce
    byte-for-byte identical groupings and aggregations.

    O(n log n) replacement for the O(n_children x n_obs) boolean-mask loop. The
    returned arrays are sorted (stably) by ascending cell id; each cell's
    observations form a contiguous slice, so ``col_arrays[col][start:end]`` is a
    view.
    """
    sort_idx = np.argsort(cell_col, kind="stable")
    sorted_cells = cell_col[sort_idx]
    col_arrays = {col: arr[sort_idx] for col, arr in col_dict.items()}
    if len(sorted_cells) == 0:
        return col_arrays, {}
    boundaries = np.flatnonzero(np.diff(sorted_cells)) + 1
    starts = np.concatenate([[0], boundaries])
    ends = np.concatenate([boundaries, [len(sorted_cells)]])
    cell_to_slice = {int(sorted_cells[s]): (int(s), int(e)) for s, e in zip(starts, ends)}
    return col_arrays, cell_to_slice


def _build_groups(
    df_all: pd.DataFrame,
    cell_col: np.ndarray,
) -> tuple[dict[str, np.ndarray], dict[int, tuple[int, int]]]:
    """Sort observations by cell id; return reordered column arrays and per-cell slice map.

    Pandas carrier wrapper over :func:`_group_columns` (extracts ``.values`` once).

    Parameters
    ----------
    df_all : pd.DataFrame
        Combined observation DataFrame (all beams / granules for this shard).
    cell_col : np.ndarray
        Cell id for each row in df_all (from ``grid.cells_of``).

    Returns
    -------
    col_arrays : dict[str, np.ndarray]
        Column arrays from df_all, sorted in ascending cell-id order.
    cell_to_slice : dict[int, tuple[int, int]]
        Maps each observed cell id to ``(start, end)`` indices into col_arrays.
    """
    col_dict = {col: df_all[col].values for col in df_all.columns}
    return _group_columns(col_dict, cell_col)


def _concat_and_group(all_reads, grid, handoff: str):
    """Concat the per-group reads and split observations by cell.

    Carrier-agnostic seam shared by :func:`process_shard` and its tests, so the
    Arrow path is exercised end-to-end (including multi-table ``concat_tables``
    ordering) rather than re-assembled inline. Both carriers feed identical numpy
    arrays into :func:`_group_columns`, so the groupings — and the aggregations
    computed from them — are byte-for-byte identical.

    Parameters
    ----------
    all_reads : list
        Per-group reads from ``_read_group``: ``pandas.DataFrame`` for the pandas
        carrier, ``pyarrow.Table`` for the arrow carrier.
    grid : OutputGrid
        Provides ``cells_of`` to map leaf ids to child cell ids.
    handoff : {"pandas", "arrow"}
        Which carrier ``all_reads`` holds.

    Returns
    -------
    col_arrays : dict[str, np.ndarray]
        Column arrays sorted in ascending cell-id order.
    cell_to_slice : dict[int, tuple[int, int]]
        Maps each observed cell id to ``(start, end)`` into ``col_arrays``.
    n_obs_total : int
        Total observation count across all reads.
    """
    if handoff == "arrow":
        import pyarrow as pa

        table = pa.concat_tables(all_reads).combine_chunks()
        # The arrow handoff requires dense, null-free columns: ``_read_group``
        # builds tables from raw h5coro reads (no null mask), so
        # ``to_numpy(zero_copy_only=False)`` is dtype-exact and matches ``.values``
        # on the pandas side. Guard the precondition so a future nullable source
        # can't silently diverge the two carriers instead of failing loudly.
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        cell_col = grid.cells_of(table.column("leaf_id").to_numpy(zero_copy_only=False))
        col_dict = {n: table.column(n).to_numpy(zero_copy_only=False) for n in table.column_names}
        col_arrays, cell_to_slice = _group_columns(col_dict, cell_col)
    else:
        df_all = pd.concat(all_reads, ignore_index=True)
        n_obs_total = len(df_all)
        cell_col = grid.cells_of(df_all["leaf_id"].values)
        col_arrays, cell_to_slice = _build_groups(df_all, cell_col)
    return col_arrays, cell_to_slice, n_obs_total


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
            f"Expected {expected_count} rows for chunk_shape={grid.chunk_shape}, got {len(df_out)}"
        )

    chunk_idx = tuple(int(i) for i in chunk_idx)
    for name, series in df_out.items():
        values = series.values
        if values.shape != grid.chunk_shape:
            values = values.reshape(grid.chunk_shape)
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store,
                path=f"{grid.group_path}/{name}",
                zarr_format=3,
                consolidated=False,
            )
            array.set_block_selection(chunk_idx, values)

    return store


def calculate_cell_statistics(
    cell_data: dict[str, np.ndarray],
    value_col: str = "h_li",
    sigma_col: str = "s_li",
    config: PipelineConfig | None = None,
) -> dict:
    """
    Calculate summary statistics for a cell, driven by pipeline config metadata.

    Parameters
    ----------
    cell_data : dict[str, np.ndarray]
        Column arrays for a single cell. Keys are column names; values are
        numpy arrays of equal length.
    value_col : str
        Column name for elevation values.
    sigma_col : str
        Column name for uncertainty values.
    config : PipelineConfig, optional
        Pipeline config to use for dispatch. Defaults to ``default_config()``.

    Returns
    -------
    dict
        Dictionary of statistics keyed by aggregation variable name.
    """
    from zagg.config import evaluate_expression, resolve_function

    if config is None:
        config = default_config()
    agg_fields = get_agg_fields(config)

    n_obs = len(next(iter(cell_data.values()))) if cell_data else 0
    if n_obs == 0:
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
            result[name] = evaluate_expression(expression, cell_data)
            continue

        values = cell_data[source]

        # Count via len
        if func_name in ("len", "count"):
            result[name] = n_obs
            continue

        # Resolve params: bare column name -> array, expression -> eval'd
        resolved_params = {}
        for pkey, pval in params.items():
            if isinstance(pval, str) and pval in cell_data:
                resolved_params[pkey] = cell_data[pval]
            elif isinstance(pval, str) and any(c in pval for c in cell_data):
                ns = {
                    "__builtins__": {},
                    "np": np,
                    "numpy": np,
                    **cell_data,
                }
                resolved_params[pkey] = eval(pval, ns)  # noqa: S307
            else:
                resolved_params[pkey] = pval

        func = resolve_function(func_name)
        result[name] = float(func(values, **resolved_params))

    return result


def _read_group(
    h5obj, group: str, data_source: dict, parent_morton: int, grid, arrow: bool = False
):
    """Read and spatially filter one HDF5 group.

    Returns a ``pandas.DataFrame`` (default) or, when ``arrow=True``, a
    ``pyarrow.Table`` carrying the identical columns. Returns ``None`` when the
    group has no observations in this shard.
    """
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

    if arrow:
        import pyarrow as pa

        return pa.table(data_dict)
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
    handoff: str = "pandas",
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
    handoff : str, optional
        Per-cell aggregation carrier: ``"pandas"`` (default) or ``"arrow"``.
        Both paths share :func:`_group_columns` and the same numpy reductions, so
        scalar outputs are byte-for-byte identical; only the read→concat→extract
        representation differs. Opt-in while the two are benchmarked (issue #30).

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``granule_count``,
        ``files_processed``, ``duration_s``, ``error``.
    """
    if config is None:
        config = default_config()
    if handoff not in ("pandas", "arrow"):
        raise ValueError(f"handoff must be 'pandas' or 'arrow', got {handoff!r}")
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

    use_arrow = handoff == "arrow"
    all_reads = []
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
                    chunk = _read_group(h5obj, g, data_source, parent_morton, grid, arrow=use_arrow)
                    if chunk is not None:
                        all_reads.append(chunk)
                except Exception as e:
                    logger.debug(f"  Error reading track {g}: {e}")
                    continue

            files_processed += 1

        except Exception as e:
            logger.warning(f"  Error processing file {s3_url}: {e}")
            continue

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")
    metadata["files_processed"] = files_processed

    if not all_reads:
        logger.info(f"  No data after filtering for morton {parent_morton} - skipping")
        metadata["error"] = "No data after filtering"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    # Concat the per-group reads and split observations by cell (carrier-agnostic;
    # both carriers feed identical numpy arrays into _group_columns).
    col_arrays, cell_to_slice, n_obs_total = _concat_and_group(all_reads, grid, handoff)
    logger.info(f"  Read {n_obs_total:,} observations")

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

    # Per-cell observation slices (grouped above, carrier-agnostic).
    _empty: dict[str, np.ndarray] = {col: arr[:0] for col, arr in col_arrays.items()}

    cells_with_data = 0
    for i, child_morton in enumerate(children):
        if child_morton in cell_to_slice:
            start, end = cell_to_slice[child_morton]
            cell_data: dict[str, np.ndarray] = {
                col: arr[start:end] for col, arr in col_arrays.items()
            }
            cells_with_data += 1
        else:
            cell_data = _empty
        stats = calculate_cell_statistics(
            cell_data, value_col="h_li", sigma_col="s_li", config=config
        )
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
        "process_morton_cell is deprecated; use process_shard(grid, shard_key, ...) directly.",
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
