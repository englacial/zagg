"""Worker-stage orchestration for :mod:`zagg.processing` (split out of the
monolithic ``processing.py`` for the §4 size limit; pure relocation, no behavior
change).

``process_shard`` reads granules once, aggregates per cell, and returns the
output carrier; ``process_morton_cell`` is the deprecated HEALPix alias. This is
the only stage that reaches across read/aggregate/write.

The ``h5coro`` module and the ``_make_url_rewriter`` helper are referenced
through the :mod:`zagg.processing` package namespace at call time so existing
tests that ``monkeypatch.setattr("zagg.processing.<name>", ...)`` continue to
patch the symbols ``process_shard`` actually calls. Group reads go through the
configured virtual chunk-index backend (issue #160, ``data_source.index``);
the default ``hierarchical`` backend resolves ``zagg.processing._read_group``
the same call-time way, so patching that symbol still intercepts reads.
"""

import logging
import time
import warnings
from datetime import datetime
from typing import Callable, List, Tuple

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
from zagg.index import index_from_config
from zagg.processing.aggregate import (
    _aggregate_chunk_cells,
    _concat_and_group,
    _eval_chunk_precompute,
    _has_vector_fields,
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
    aoi_payload=None,
    write_chunk: Callable | None = None,
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
        Per-cell aggregation carrier: ``"pandas"`` or ``"arrow"``. Both feed
        identical numpy arrays into the same numpy reductions, so scalar outputs
        are byte-for-byte identical; only the read→concat→extract representation
        differs (pandas DataFrames vs ``arro3.core`` Tables). The carrier is
        normally declared per-pipeline in the aggregation config
        (``aggregation.handoff``, default ``"arrow"`` — issue #132) and resolved by
        the caller (``agg`` / the Lambda handler) via
        :func:`zagg.config.get_handoff`; this parameter's own ``"pandas"`` default
        is only a no-config safety net for direct callers. pyarrow is not used on
        either path.
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
    aoi_payload : optional
        The shard's strict-AOI mask payload (issue #101) from the manifest's
        ``aoi_mask`` list — a compact MOC (HEALPix) or in-AOI cell ids
        (rectilinear). When given, each chunk's carrier gains a per-cell ``bool``
        ``aoi_mask`` column (``True`` where the cell is inside the AOI), expanded via
        ``grid.aoi_mask_from_payload`` over the chunk's cells. ``None`` (default,
        flag off) appends nothing — byte-for-byte unchanged.
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
    write_chunk : callable, optional
        Per-chunk write seam for the multi-chunk path (issue #91). When provided,
        each chunk's ``(block_index, carrier, ragged)`` is handed to
        ``write_chunk(block_index, carrier, ragged)`` the moment it is built and its
        local refs are dropped, instead of being appended to ``chunk_results``. This
        caps the worker's output-side footprint at ~1 chunk rather than holding all K
        carriers + ragged at once (the accumulation #91 targets). The callback is the
        consumer's existing per-chunk write body (runner / lambda handler). It is
        accepted as the K>1 sink in place of ``chunk_results`` (passing both raises),
        and at K==1 it streams the lone chunk exactly as the K>1 path would — a true
        no-op vs the accumulated path (output byte-identical). When ``None`` (default),
        the ``chunk_results`` / ``ragged_out`` behavior above is unchanged. The
        sharded path (#108) still bundles all K via ``chunk_results`` /
        ``write_shard_to_zarr`` and does not pass a callback.
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
    if handoff not in ("pandas", "arrow"):
        raise ValueError(f"handoff must be 'pandas' or 'arrow', got {handoff!r}")
    data_source = config.data_source

    # Resolve the virtual chunk-index backend (issue #160). An absent
    # ``data_source.index`` block resolves to ``hierarchical`` — today's read
    # path, byte-identical.
    index_backend = index_from_config(config)

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

    # A-priori chunk-boundary plan (issue #148 arm 2a): ``_read_group`` needs
    # the granule identity to locate its boundary parquet. The kwarg is passed
    # only when the feature is on, so monkeypatched ``_read_group`` fakes (and
    # the production call) keep their existing signature byte-for-byte.
    # Presence check, mirroring ``_read_group``'s dispatch gate exactly.
    _rp = data_source.get("read_plan")
    apriori = isinstance(_rp, dict) and "chunk_boundaries" in _rp

    use_arrow = handoff == "arrow"
    all_reads = []
    files_processed = 0
    read_errors = 0

    # Streaming buffered merge (issue #148 phase 4): when
    # ``aggregation.streaming`` is set, reads accumulate for ``buffer_granules``
    # granules and fold into running per-cell state instead of pooling the whole
    # shard — peak memory is one buffer + digest state, independent of granule
    # count. Validated up front (mergeable reducers only); ``None`` (default) is
    # the unchanged pooled path.
    from zagg.processing.streaming import StreamingAggregator, get_streaming

    streaming_cfg = get_streaming(config)
    buffered = (
        StreamingAggregator(config, grid, handoff, streaming_cfg["buffer_granules"])
        if streaming_cfg is not None
        else None
    )

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
                    read_kwargs = {"arrow": use_arrow}
                    if apriori:
                        read_kwargs["granule_url"] = s3_url
                    chunk = index_backend.read_group(
                        h5obj, g, data_source, shard_key, grid, **read_kwargs
                    )
                    if chunk is not None:
                        if buffered is not None:
                            buffered.add_read(chunk)
                        else:
                            all_reads.append(chunk)
                except Exception as e:
                    # A raised read error is always a real failure: a
                    # legitimately-empty group returns ``None`` (no exception),
                    # so promoting this to WARNING does not get noisy on shards
                    # where many granules simply contribute 0 photons (issue
                    # #116). Logging it at DEBUG hid the dem_h broadcast failure
                    # behind the misleading "No data after filtering" below.
                    read_errors += 1
                    logger.warning(f"  Error reading track {g}: {e}")
                    continue

            # Per-granule backend hook (issue #160): side effects only (e.g.
            # ``inline`` write-back). A failure here never fails the read —
            # the granule's data is already in ``all_reads``.
            try:
                index_backend.finish_granule(h5obj, s3_url)
            except Exception:
                logger.warning(f"  index backend finish_granule failed for {s3_url}", exc_info=True)

            files_processed += 1
            if buffered is not None:
                buffered.granule_done()

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
    if read_errors:
        metadata["read_errors"] = read_errors

    if buffered is not None:
        # Drain the tail buffer (< buffer_granules granules) BEFORE the read
        # stamp: intermediate flushes already run inside the read loop
        # (granule_done -> flush), so under profiling the streaming path
        # deliberately charges ALL group+merge cost to ``read`` — the tail
        # flush must not fall between phases and vanish from the accounting.
        buffered.flush()
    if profile:
        phase_timings["read"] = time.time() - _read_t0

    if buffered.empty if buffered is not None else not all_reads:
        # Distinguish a genuinely-empty read from one where a group read raised
        # (issue #116): a raised read is a real error masquerading as "no data",
        # so report it as such instead of the misleading text. Some groups may
        # have returned ``None`` (legitimately empty) rather than raised, so the
        # message is "no data AND N raised", not "all groups raised".
        if read_errors:
            logger.warning(
                f"  No data after filtering for shard {shard_key} and "
                f"{read_errors} group read(s) raised - skipping"
            )
            metadata["error"] = f"No data after filtering ({read_errors} group reads raised)"
        else:
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
    if chunk_results is not None and write_chunk is not None:
        raise ValueError(
            "process_shard takes either chunk_results (accumulate) or write_chunk "
            "(stream-and-free, issue #91), not both."
        )
    if write_chunk is not None and ragged_out is not None:
        # When streaming, each chunk's ragged goes straight to write_chunk, so a
        # ragged_out sink would be left silently empty — reject the ambiguity (as the
        # chunk_results+write_chunk guard above does) rather than mislead the caller.
        raise ValueError(
            "process_shard ignores ragged_out when write_chunk is given (the chunk's "
            "ragged is delivered to the callback); pass one or the other, not both."
        )
    # A K>1 grid needs one of the two multi-chunk sinks: ``chunk_results`` to
    # accumulate the K carriers or ``write_chunk`` to stream-and-free them (#91).
    streaming = write_chunk is not None
    if chunks_per_shard > 1 and chunk_results is None and not streaming:
        raise ValueError(
            f"grid has chunks_per_shard={chunks_per_shard} (chunk_inner set, issue #30 "
            f"item 3) but process_shard was called without a chunk_results sink or a "
            f"write_chunk callback (issue #91); the K per-chunk carriers cannot be "
            f"returned through the single df_out. Pass chunk_results=[] or write_chunk=... "
            f"(the runner does)."
        )

    _index_t0 = time.time() if profile else None

    # ---- Pool the shard's reads ONCE (shared across all K chunks) -------------
    # The shard is read+grouped a single time; only the ``chunk_precompute``
    # reduction (``chunk_scalars``, issue #30 item 1) moves INTO the per-chunk loop
    # below (issue #82 phase 6). A ``resolution: chunk`` companion is per Zarr chunk,
    # so the gain/offset anchor must be reduced over each chunk's own observations,
    # not the whole pooled shard. At K==1 the lone chunk == the whole shard, so the
    # anchor is identical to the old shard-level reduction (byte-for-byte unchanged).
    # Concat the per-group reads and split observations by cell (carrier-agnostic;
    # both carriers feed identical numpy arrays into _group_columns). The buffered
    # path (issue #148 phase 4) already grouped-and-merged per flush, so its
    # running state replaces the shard-wide pool.
    if buffered is not None:
        col_arrays, cell_to_slice = {}, {}
        n_obs_total = buffered.n_obs_total
        logger.info(f"  Read {n_obs_total:,} observations ({buffered.flushes} buffer flushes)")
    else:
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
        if buffered is not None:
            # Buffered path (issue #148 phase 4): emit this chunk's outputs from
            # the running merged state; chunk_precompute is rejected at validation
            # so there are no chunk scalars to evaluate.
            stats_arrays, ragged_payloads, ragged_idx, cwd = buffered.chunk_outputs(
                chunk_children, agg_fields
            )
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
        # Strict-AOI per-cell mask (issue #101): expand the shard's manifest payload
        # over THIS chunk's cells (order-aligned with the carrier). None when the
        # flag is off, so the carrier is byte-for-byte unchanged. A non-None payload
        # against a grid that can't expand it is a manifest/grid mismatch — raise
        # rather than silently drop the column (which would leave an all-False mask).
        chunk_aoi_mask = None
        if aoi_payload is not None:
            if not hasattr(grid, "aoi_mask_from_payload"):
                raise ValueError(
                    f"manifest carries an aoi_mask payload but grid "
                    f"{type(grid).__name__} cannot expand it (no aoi_mask_from_payload)"
                )
            chunk_aoi_mask = grid.aoi_mask_from_payload(aoi_payload, chunk_children)
        carrier = _build_output(
            stats_arrays,
            dense_vars,
            agg_fields,
            grid,
            shard_key,
            use_arrow=use_arrow,
            children=(chunk_children if chunks_per_shard > 1 else None),
            aoi_mask=chunk_aoi_mask,
        )
        ragged = (
            {name: (ragged_payloads[name], ragged_idx[name]) for name in ragged_payloads}
            if handoff != "arrow-kernel"
            else {}
        )
        if streaming:
            # Stream-and-free (issue #91): write this chunk now and drop its refs so
            # peak output-side memory holds ~1 chunk, not all K. Nothing is stashed.
            write_chunk(block_index, carrier, ragged)
            del carrier, ragged
        elif chunk_results is not None:
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
    # were appended to ``chunk_results`` (accumulate) or already handed to
    # ``write_chunk`` (stream, issue #91); either way nothing is stashed, so return
    # an empty carrier here.
    if chunk_results is not None or streaming:
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
