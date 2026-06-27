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
import time
import warnings
from datetime import datetime
from typing import List, Tuple

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
    _aggregate_chunk_cells,
    _concat_and_group,
    _eval_chunk_precompute,
    _group_columns,
    _has_ragged_fields,
    _has_vector_fields,
    _kernel_aggregate,
    _pool_chunk_columns,
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
    chunk_results: list | None = None,
    profile: bool = False,
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
        ragged payloads exactly as before (byte-for-byte unchanged). At K>1 (see
        ``chunk_results``) the ragged payloads are delivered per chunk via that
        sink instead, and ``ragged_out`` is left untouched.
    chunk_results : list, optional
        Out-param sink for the multi-chunk-per-worker path (issue #30 item 3).
        When the grid sets a finer ``chunk_inner`` (``K = grid.chunks_per_shard >
        1``), one worker (one shard) owns K finer Zarr chunks: this fills the list
        with one ``(block_index, carrier, ragged)`` tuple per chunk —
        ``block_index`` the chunk's storage block (from ``grid.iter_chunks``),
        ``carrier`` its dense DataFrame/Table, ``ragged`` its
        ``{field: (values_list, cell_ids)}`` CSR map — for the caller to write K
        regions + K companion slices. The returned 2-tuple's ``df_out`` is an empty
        carrier in that case (the real carriers live in ``chunk_results``).
        ``None`` (default) is the K==1 path: the single chunk's carrier is the
        returned ``df_out`` and ragged goes to ``ragged_out`` — byte-for-byte
        unchanged. A caller that passes ``None`` while the grid has K>1 cannot place
        the K carriers, so that combination raises.
    profile : bool, optional
        Opt-in per-phase timing (issue #100 phase 2). When ``True``, fills
        ``metadata["phase_timings"]`` with ``read`` / ``index`` / ``aggregate``
        wall-clock seconds (``time.time()`` deltas) for the in-worker stages.
        Default ``False`` takes the current path unchanged — no added timing
        calls, no ``phase_timings`` key — so the worker pays no probe tax on
        ordinary runs. (The ``write`` phase runs in the lambda handler, outside
        this function.)

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``granule_count``,
        ``files_processed``, ``duration_s``, ``error``. Ragged (CSR) fields are
        delivered out-of-band via ``ragged_out`` (above), not in this tuple. At
        K>1 the per-chunk carriers + ragged are delivered via ``chunk_results``.
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

    # Opt-in per-phase timing (issue #100). Only allocated when profiling so the
    # default path stays byte-identical (no dict, no time.time() calls).
    phase_timings: dict | None = {} if profile else None
    _read_t0 = time.time() if profile else None

    # Read files and filter spatially
    for s3_url in granule_urls:
        h5obj = None
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
        finally:
            # Release this granule's h5coro cache before the next one (issue #66):
            # without it each granule's unevicted cache stays resident for the whole
            # loop → Lambda OOM. ``close()`` is the live path; ``cache.clear()`` is a
            # fallback for builds lacking it. Retained ``all_reads`` data is already
            # copied off the cache lines (see PR #94), so releasing here is safe.
            if h5obj is not None:
                try:
                    if hasattr(h5obj, "close"):
                        h5obj.close()
                    elif getattr(h5obj, "cache", None) is not None:
                        h5obj.cache.clear()
                except Exception:
                    logger.debug("h5coro cache release failed", exc_info=True)

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")
    metadata["files_processed"] = files_processed
    if profile:
        phase_timings["read"] = time.time() - _read_t0

    if not all_reads:
        logger.info(f"  No data after filtering for shard {shard_key} - skipping")
        metadata["error"] = "No data after filtering"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        if profile:
            metadata["phase_timings"] = phase_timings
        return pd.DataFrame(), metadata

    data_vars = get_data_vars(config)
    agg_fields = get_agg_fields(config)
    dense_vars = [v for v in data_vars if get_output_signature(agg_fields[v])["kind"] != "ragged"]
    use_arrow = _has_vector_fields(config)

    # K = number of finer Zarr chunks this shard owns (issue #30 item 3). K==1 is
    # the unchanged single-chunk path; K>1 fans the shard into ``grid.iter_chunks``.
    chunks_per_shard = int(getattr(grid, "chunks_per_shard", 1))
    if chunks_per_shard > 1 and chunk_results is None:
        raise ValueError(
            f"grid has chunks_per_shard={chunks_per_shard} (chunk_inner set, issue #30 "
            f"item 3) but process_shard was called without a chunk_results sink; the K "
            f"per-chunk carriers cannot be returned through the single df_out. Pass "
            f"chunk_results=[] (the runner does)."
        )

    _index_t0 = time.time() if profile else None

    # ---- Pool the shard's reads ONCE (shared across all K chunks) -------------
    # The shard is read+grouped a single time; only the ``chunk_precompute``
    # reduction (``chunk_scalars``, issue #30 item 1) moves INTO the per-chunk loop
    # below (issue #82 phase 6). A ``resolution: chunk`` companion is per Zarr chunk,
    # so the gain/offset anchor must be reduced over each chunk's own observations,
    # not the whole pooled shard. At K==1 the lone chunk == the whole shard, so the
    # anchor is identical to the old shard-level reduction (byte-for-byte unchanged).
    if handoff == "arrow-kernel":
        # EXPERIMENTAL (phase 2b of #30): reduce via pyarrow hash-aggregate kernels.
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
        pooled = {n: table.column(n).to_numpy(zero_copy_only=False) for n in table.column_names}
        # Group the pooled columns by cell ONCE (issue #82 phase 6), exactly as the
        # default path's ``cell_to_slice`` does, so the per-chunk precompute subset is
        # a contiguous gather (O(N) total) rather than a shard-wide ``np.isin`` rescan
        # on every one of the K iterations (O(K·N)).
        pooled_sorted, pooled_to_slice = _group_columns(pooled, cell_col)
    else:
        # Concat the per-group reads and split observations by cell (carrier-
        # agnostic; both carriers feed identical numpy arrays into _group_columns).
        col_arrays, cell_to_slice, n_obs_total = _concat_and_group(all_reads, grid, handoff)
        logger.info(f"  Read {n_obs_total:,} observations")

    if profile:
        phase_timings["index"] = time.time() - _index_t0
        _aggregate_t0 = time.time()

    # ---- Aggregate + build one carrier per finer chunk -----------------------
    # ``iter_chunks`` is the K-chunk seam (issue #30 item 3); a minimal grid (e.g.
    # a test stub) without it is implicitly K==1 — fall back to the single chunk
    # ``(block_index(shard_key), children(shard_key))``, the byte-identical path.
    if hasattr(grid, "iter_chunks"):
        chunk_iter = grid.iter_chunks(shard_key)
    else:
        # Minimal stub: derive the lone chunk's children and (only when a sink
        # needs it) its block index. ``block_index`` may be absent on a stub that
        # never returns through ``chunk_results``; default to () in that case.
        fallback_block = grid.block_index(shard_key) if hasattr(grid, "block_index") else ()
        chunk_iter = iter([(fallback_block, grid.children(shard_key))])

    cells_with_data = 0
    single_carrier = None
    single_ragged: dict = {}
    for block_index, chunk_children in chunk_iter:
        chunk_children = np.asarray(chunk_children)
        if handoff == "arrow-kernel":
            # Per-chunk precompute (issue #82 phase 6): reduce the anchor over only
            # this chunk's observations, gathered from the once-grouped pooled columns
            # (contiguous slices, not a shard-wide rescan). An empty chunk yields
            # length-0 columns, which ``_eval_chunk_precompute`` short-circuits to NaN
            # anchors rather than raising on ``np.min`` of an empty array.
            chunk_pooled = _pool_chunk_columns(pooled_sorted, pooled_to_slice, chunk_children)
            chunk_scalars = _eval_chunk_precompute(config, chunk_pooled)
            kernel = _kernel_aggregate(
                table, cell_col, chunk_children, "h_li", config, chunk_scalars=chunk_scalars
            )
            stats_arrays = kernel["stats_arrays"]
            cells_with_data += kernel["cells_with_data"]
            ragged_payloads: dict[str, list] = {}
        else:
            # Per-chunk precompute (issue #82 phase 6): pool only this chunk's rows
            # from the shard's sorted column arrays, then reduce the anchor over them.
            chunk_pooled = _pool_chunk_columns(col_arrays, cell_to_slice, chunk_children)
            chunk_scalars = _eval_chunk_precompute(config, chunk_pooled)
            stats_arrays, ragged_payloads, ragged_idx, cwd = _aggregate_chunk_cells(
                chunk_children,
                col_arrays,
                cell_to_slice,
                chunk_scalars,
                config,
                data_vars,
                agg_fields,
            )
            cells_with_data += cwd
        carrier = _build_output(
            stats_arrays,
            dense_vars,
            agg_fields,
            grid,
            shard_key,
            use_arrow=use_arrow,
            children=(chunk_children if chunks_per_shard > 1 else None),
        )
        ragged = (
            {name: (ragged_payloads[name], ragged_idx[name]) for name in ragged_payloads}
            if handoff != "arrow-kernel"
            else {}
        )
        if chunk_results is not None:
            chunk_results.append((block_index, carrier, ragged))
        else:
            # K==1 path: stash the lone chunk's carrier + ragged for the 2-tuple
            # return / ``ragged_out`` sink below (byte-for-byte the old behavior).
            single_carrier = carrier
            single_ragged = ragged

    logger.info(f"  Statistics: {cells_with_data} cells with data")

    if profile:
        phase_timings["aggregate"] = time.time() - _aggregate_t0
        metadata["phase_timings"] = phase_timings

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {shard_key} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = n_obs_total
    metadata["duration_s"] = duration

    # K==1: deliver the lone chunk's carrier as the 2-tuple ``df_out`` and its
    # ragged via ``ragged_out`` (unchanged contract). K>1: the carriers + ragged
    # were appended to ``chunk_results``; return an empty carrier here.
    if chunk_results is not None:
        df_out = pd.DataFrame()
    else:
        df_out = single_carrier if single_carrier is not None else pd.DataFrame()
        if ragged_out is not None:
            for name, payload in single_ragged.items():
                ragged_out[name] = payload

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
