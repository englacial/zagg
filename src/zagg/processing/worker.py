"""Worker-stage orchestration for :mod:`zagg.processing` (split out of the
monolithic ``processing.py`` for the §4 size limit; pure relocation, no behavior
change).

``process_shard`` reads granules once, aggregates per cell, and returns the
output carrier; ``process_morton_cell`` is the deprecated HEALPix alias. This is
the only stage that reaches across read/aggregate/write.

The ``h5coro`` module and the ``_read_group`` / ``_make_url_rewriter`` helpers are
referenced through the :mod:`zagg.processing` package namespace at call time so
existing tests that ``monkeypatch.setattr("zagg.processing._read_group", ...)``
(etc.) continue to patch the symbols ``process_shard`` actually calls.
"""

import logging
import warnings
from datetime import datetime
from typing import Any, List, Tuple

import numpy as np
import pandas as pd

import zagg.processing as _processing
from zagg.config import (
    PipelineConfig,
    default_config,
    get_agg_fields,
    get_data_vars,
    get_output_signature,
)
from zagg.processing.aggregate import (
    _concat_and_group,
    _eval_chunk_precompute,
    _has_ragged_fields,
    _has_vector_fields,
    _kernel_aggregate,
    calculate_cell_statistics,
)
from zagg.processing.write import _build_output
from zagg.schema import ProcessingMetadata

logger = logging.getLogger(__name__)


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
    ragged_out: dict | None = None,
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
        Per-cell aggregation carrier: ``"pandas"`` (default), ``"arrow"``, or the
        EXPERIMENTAL ``"arrow-kernel"``. ``"pandas"`` and ``"arrow"`` share
        :func:`_group_columns` and the same numpy reductions, so scalar outputs
        are byte-for-byte identical; only the read→concat→extract representation
        differs. ``"arrow-kernel"`` (phase 2b of #30) instead reduces via
        ``pyarrow.compute`` hash-aggregate kernels: ``count``/``min``/``max`` stay
        exact vs numpy (NaN included — see :func:`_kernel_aggregate`), while its
        float ``mean``/``variance`` differ by ~1 ULP (agree within
        :data:`KERNEL_RTOL`, not byte identical). All three are opt-in while
        benchmarked (issue #30).
    ragged_out : dict, optional
        Out-param sink for ``kind: ragged`` (CSR) fields (issue #48). When a dict
        is passed, it is filled in place with ``{field_name: (values_list,
        cell_ids)}`` — ``values_list`` the per-populated-cell payload arrays and
        ``cell_ids`` their position in the chunk's ``children`` block — for the
        caller to hand to :func:`zagg.processing.write.write_ragged_to_zarr`. The
        return value stays the 2-tuple ``(df_out, metadata)`` so existing 2-tuple
        callers are unaffected; ``None`` (default) collects-then-discards the
        ragged payloads exactly as before (byte-for-byte unchanged).

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``granule_count``,
        ``files_processed``, ``duration_s``, ``error``. Ragged (CSR) fields are
        delivered out-of-band via ``ragged_out`` (above), not in this tuple.
    """
    if config is None:
        config = default_config()
    if handoff not in ("pandas", "arrow", "arrow-kernel"):
        raise ValueError(f"handoff must be 'pandas', 'arrow', or 'arrow-kernel', got {handoff!r}")
    data_source = config.data_source

    shard_key = int(shard_key)
    logger.info(f"Processing shard: {shard_key}")
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
        "shard_key": shard_key,
        "cells_with_data": 0,
        "total_obs": 0,
        "granule_count": len(granule_urls),
        "files_processed": 0,
        "duration_s": 0.0,
        "error": None,
    }

    # Check for granules
    if not granule_urls:
        logger.info(f"  No granules provided for shard {shard_key} - skipping")
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
    _rewrite_url = _processing._make_url_rewriter(driver)

    use_arrow = handoff in ("arrow", "arrow-kernel")
    all_reads = []
    files_processed = 0

    # Read files and filter spatially
    for s3_url in granule_urls:
        try:
            resource_path = _rewrite_url(s3_url)

            h5obj = _processing.h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )

            for g in data_source["groups"]:
                try:
                    chunk = _processing._read_group(
                        h5obj, g, data_source, shard_key, grid, arrow=use_arrow
                    )
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
        logger.info(f"  No data after filtering for shard {shard_key} - skipping")
        metadata["error"] = "No data after filtering"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    children = grid.children(shard_key)
    data_vars = get_data_vars(config)

    # Ragged-field collectors (issue #48): populated only in the non-kernel path;
    # initialized here so the post-if/else _build_output call can reference them
    # regardless of which branch ran.
    ragged_payloads: dict[str, list] = {}
    ragged_cell_indices: dict[str, list[int]] = {}

    if handoff == "arrow-kernel":
        # EXPERIMENTAL (phase 2b of #30): reduce via pyarrow hash-aggregate kernels
        # instead of the per-cell numpy loop. Not byte-identical to the default
        # path (float mean/variance diverge by ~1 ULP — see KERNEL_RTOL).
        if _has_ragged_fields(config):
            raise NotImplementedError(
                "handoff='arrow-kernel' does not support ragged fields (issue #48); "
                "use handoff='pandas' or 'arrow' instead"
            )
        import pyarrow as pa

        table = pa.concat_tables(all_reads).combine_chunks()
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        cell_col = grid.cells_of(table.column("leaf_id").to_numpy(zero_copy_only=False))
        logger.info(f"  Read {n_obs_total:,} observations")
        # Per-chunk precompute hook (issue #30): reduce each entry ONCE over the
        # pooled arrow table. Columns are dense + null-free (guarded above), so the
        # ``to_numpy`` extraction is zero-copy where the buffer layout allows and
        # dtype-exact otherwise — the same numpy arrays the pandas/arrow carriers
        # feed in. The resulting scalars/arrays are threaded into the kernel
        # fallback per-cell loop (where expression fields resolve).
        pooled = {n: table.column(n).to_numpy(zero_copy_only=False) for n in table.column_names}
        chunk_scalars = _eval_chunk_precompute(config, pooled)
        logger.info(f"  Calculating statistics for {len(children)} cells (kernel)...")
        kernel = _kernel_aggregate(
            table, cell_col, children, "h_li", config, chunk_scalars=chunk_scalars
        )
        stats_arrays = kernel["stats_arrays"]
        cells_with_data = kernel["cells_with_data"]
        n_cells = len(children)
    else:
        # Concat the per-group reads and split observations by cell (carrier-
        # agnostic; both carriers feed identical numpy arrays into _group_columns).
        col_arrays, cell_to_slice, n_obs_total = _concat_and_group(all_reads, grid, handoff)
        logger.info(f"  Read {n_obs_total:,} observations")

        # Per-chunk precompute hook (issue #30, item 1): evaluate each
        # ``chunk_precompute`` entry ONCE over the shard's pooled columns, then
        # inject the resulting chunk-level scalars into every cell's namespace so a
        # per-cell expression can reference a chunk-uniform anchor (e.g. the 128-bin
        # waveform window). Empty when the block is absent, so the per-cell path is
        # byte-for-byte unchanged for configs that do not use the hook.
        chunk_scalars = _eval_chunk_precompute(config, col_arrays)
        logger.info(f"  Calculating statistics for {len(children)} cells...")

        n_cells = len(children)
        agg_fields = get_agg_fields(config)
        stats_arrays: dict = {}
        # Ragged fields (issue #48) are variable-length per-cell; they cannot be
        # preallocated as a dense block. ``ragged_payloads``/``ragged_cell_indices``
        # are pre-initialized before this branch (see above); fill them in the loop.
        for name in data_vars:
            meta = agg_fields[name]
            sig = get_output_signature(meta)
            if sig["kind"] == "ragged":
                ragged_payloads[name] = []
                ragged_cell_indices[name] = []
                continue
            # Vector fields (issue #29) get a per-cell (n_cells, *trailing_shape)
            # block; scalars keep the 1-D (n_cells,) layout, unchanged. Either way
            # ``stats_arrays[name][i] = value`` assigns the cell's result row.
            shape = (n_cells, *sig["trailing_shape"])
            zarr_dtype = np.dtype(meta.get("dtype", "float32"))
            fill_value = meta.get("fill_value", "NaN")
            if fill_value == "NaN":
                stats_arrays[name] = np.full(shape, np.nan, dtype=zarr_dtype)
            else:
                stats_arrays[name] = np.zeros(shape, dtype=zarr_dtype)

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
            # Inject the chunk-level scalars into this cell's namespace (no-op when
            # ``chunk_scalars`` is empty, so non-precompute configs are unchanged).
            cell_namespace: dict[str, Any] = (
                {**cell_data, **chunk_scalars} if chunk_scalars else cell_data
            )
            stats = calculate_cell_statistics(
                cell_namespace, value_col="h_li", sigma_col="s_li", config=config
            )
            for key, value in stats.items():
                if key in ragged_payloads:
                    # Ragged field: collect non-empty payloads with their cell index.
                    # Empty cells (from _empty_cell_value -> []) are skipped; the
                    # CSR writer represents absent cells via ``cell_ids``.
                    arr_val = np.asarray(value)
                    if arr_val.size > 0:
                        ragged_payloads[key].append(arr_val)
                        ragged_cell_indices[key].append(i)
                else:
                    stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Assemble the output carrier: a plain DataFrame for a pure-scalar config
    # (unchanged), or a pyarrow.Table with FixedSizeList vector columns when any
    # field declares a non-scalar output (issue #29). Scalars stay byte-identical.
    # Ragged fields (issue #48) are excluded from the dense carrier — they are
    # returned separately as (payloads, cell_indices) for the CSR writer.
    _agg_fields = get_agg_fields(config)
    dense_vars = [v for v in data_vars if get_output_signature(_agg_fields[v])["kind"] != "ragged"]
    df_out = _build_output(
        stats_arrays,
        dense_vars,
        _agg_fields,
        grid,
        shard_key,
        use_arrow=_has_vector_fields(config),
    )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {shard_key} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = n_obs_total
    metadata["duration_s"] = duration

    # Hand the collected ragged (CSR) payloads back out-of-band (issue #48). The
    # per-cell loop above already gathered ``(payloads, cell_indices)`` per ragged
    # field; thread them to the caller for the CSR write. A field with no
    # populated cell still gets an empty entry so the caller can no-op cleanly.
    # ``handoff="arrow-kernel"`` rejects ragged fields up front, so the collectors
    # are empty there and this is a no-op (the entries are simply absent).
    if ragged_out is not None:
        for name in ragged_payloads:
            ragged_out[name] = (ragged_payloads[name], ragged_cell_indices[name])

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
