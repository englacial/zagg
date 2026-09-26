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
import threading
import time
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import islice
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
from zagg.grids.base import shard_label
from zagg.index import index_from_config
from zagg.processing.aggregate import (
    _aggregate_chunk_cells,
    _concat_and_group,
    _eval_chunk_precompute,
    _has_vector_fields,
    _pool_chunk_columns,
)
from zagg.processing.write import _build_output, _channel_entry
from zagg.schema import ProcessingMetadata

logger = logging.getLogger(__name__)

#: Read-error exemplars carried in the result payload (issue #341): the first
#: N DISTINCT exception messages, each truncated — bounded by construction so
#: the wire payload stays small; full tracebacks go to the worker log only.
_EXEMPLAR_LIMIT = 3
_EXEMPLAR_CHARS = 300

#: Distinct messages whose FIRST occurrence logs a full traceback. Deliberately
#: larger than :data:`_EXEMPLAR_LIMIT` (fold review): coupling the two meant the
#: 4th distinct failure got neither an exemplar nor a traceback, so on a shard
#: where three flavors of transient S3 error precede the real cause, the real
#: cause was invisible. The bound still caps the issue #175 WorkerErrorCount
#: metric filter (which matches "Traceback") at N per shard.
_TRACEBACK_LIMIT = 5


#: Error codes / message tokens that mean "the read was DENIED", not "the data
#: was bad" (issue #449). Matched case-insensitively against the exception text
#: and, for botocore-shaped errors, against ``response["Error"]["Code"]``.
#: Deliberately specific: a bare ``403`` would match a granule name.
_AUTH_ERROR_TOKENS = (
    "accessdenied",
    "access denied",
    "invalidaccesskeyid",
    "signaturedoesnotmatch",
    "expiredtoken",
    "expired token",
    "expired credentials",
    "invalidtoken",
    "tokenrefreshrequired",
    "403 forbidden",
    "http 403",
    "status 403",
    "unauthorized",
)


def is_auth_failure(exc: BaseException) -> bool:
    """Whether a granule/group read failure is DEFINITELY credentials-shaped
    (issue #449).

    Two signatures, cheapest first:

    * a **botocore** ``ClientError`` — duck-typed on its ``response`` mapping so
      no botocore import is needed here — carrying HTTP 403 or a denial code;
    * any exception whose text carries one of :data:`_AUTH_ERROR_TOKENS`.

    Both are unambiguous: the status code or the denial token is *in* the
    exception. The empty-body signature is deliberately NOT here — see
    :func:`is_empty_body_failure`, which the caller uses as a hint only.

    Classification only: the caller decides what to do with it. The point is
    that a shard whose every read was denied must not report the data-shaped
    "No data after filtering" — that misdiagnosis cost issue #449 a fleet
    round trip.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        meta = response.get("ResponseMetadata")
        if isinstance(meta, dict) and meta.get("HTTPStatusCode") in (401, 403):
            return True
        error = response.get("Error")
        if isinstance(error, dict):
            code = str(error.get("Code", "")).lower()
            if code in ("403", "401") or any(t in code for t in _AUTH_ERROR_TOKENS):
                return True
    return any(token in str(exc).lower() for token in _AUTH_ERROR_TOKENS)


def is_empty_body_failure(exc: BaseException) -> bool:
    """Whether a read failure is the **empty-body** signature (issue #449).

    A read that returned ``None`` where bytes were expected hands the HDF5
    reader nothing, which surfaces as ``TypeError: memoryview: a bytes-like
    object is required, not 'NoneType'`` — the GEDI first-fleet-run shape,
    where every 403 from ``lp-prod-protected`` arrived as an empty body.

    **Ambiguous by construction** (fold review): ``h5coro.s3driver.S3Driver.
    read`` returns ``None`` from a bare ``except Exception`` as well as from
    ``NoSuchKey``, so a missing object, a ``SlowDown``/503, a read timeout and
    a connection reset all produce this exact exception too — and
    :mod:`zagg.processing.spill`'s ``memoryview(arr).cast("B")`` raises it
    byte-for-byte from a cause with nothing to do with credentials. So it is
    reported as a HINT on the generic no-data message (empty body; likely
    denial / missing object / throttling) and never sets the auth class or the
    ``auth_errors`` counter — the diagnostic value without the false
    certainty. The real boto exception is logged by h5coro at ERROR either way.
    """
    text = str(exc).lower()
    return isinstance(exc, TypeError) and "memoryview" in text and "nonetype" in text


def _entry_url(entry) -> str:
    """Primary URL of a granule entry (issue #425).

    An entry is a plain URL string (single-asset granule — every pre-#425
    payload) or a ``{"url": ..., "assets": {name: url}}`` mapping from a
    paired-asset shard map; labels/logs always use the primary URL.
    """
    return entry["url"] if isinstance(entry, dict) else entry


def _granule_workers(data_source: dict) -> int:
    """Granules in flight per shard (issue #180).

    Read from ``data_source.shard_workers`` — the canonical cross-pipeline
    knob (issue #232: "source units in flight per shard"; the raster path
    reads the same key for acquisition groups) — falling back to the legacy
    ``granule_workers`` name, which shipped configs still carry. Canonical
    wins when both are set.

    Default **4** — picked from the PR #183 K-sweep fleet A/B (issue #185:
    K=4 inline read 296.9 s vs 0.17's 323 s median on the o9 NEON AOI; the
    #170 measure-then-flip discipline, flipped). Output stays byte-identical
    to serial by construction — results fold in submission order — the
    default just no longer *executes* serially; ``1`` restores the serial
    loop. Validated at submission (``validate_config``) and re-checked here
    with the same int>=1 / bool-trap guard so hand-rolled worker payloads
    fail loudly before any read. The dispatcher clamps each cell's width to
    ``min(K, n_granules)`` (issue #184) so small shards don't spin idle
    threads.

    Sizing note (review finding, PR #183): this pool composes multiplicatively
    with ``read_workers`` (and under ``sidecar`` its chunk-fetch pool too), so
    worst-case in-flight GETs is granule_workers x read_workers x fetch width
    against h5coro S3Driver's 100-connection budget — dial ``read_workers``
    down as K rises, and watch ``read_errors`` (queueing + the 5 s timeout can
    surface as spurious read failures, not slowdowns) in the K-sweep A/B.
    """
    w = data_source.get("shard_workers", data_source.get("granule_workers", 4))
    if isinstance(w, bool) or not isinstance(w, int) or w < 1:
        raise ValueError(
            f"data_source.shard_workers (or legacy granule_workers) must be an "
            f"integer >= 1 (got {w!r})"
        )
    return w


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
    occupied_out: list | None = None,
    time_range_of: str | None = None,
    profile: bool = False,
    windows: list[dict] | None = None,
    time_field: str | None = None,
    emit_window: Callable | None = None,
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
        Out-param sink for ``kind: ragged`` fields (issue #48). When a dict
        is passed, it is filled in place with ``{field_name: (values_list,
        cell_ids)}`` — ``values_list`` the per-populated-cell payload arrays and
        ``cell_ids`` their position in the chunk's ``children`` block — for the
        caller to hand to :func:`zagg.processing.write.write_ragged_to_zarr`. A
        located field (issue #87) delivers ``(values_list, cell_ids,
        locations_list)`` instead, the third element its per-cell uint64
        location vectors, index-aligned with ``values_list``. The
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
        ``{field: (values_list, cell_ids)}`` ragged map — for the caller to write K
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
    occupied_out : list, optional
        Out-param sink for the shard's occupied cells (issue #200). When a list
        is passed, one ``uint64`` array of the distinct cell-order morton words
        holding >= 1 observation — the cells ``cells_with_data`` counts — is
        appended after the shard's reads are grouped. The hive write path uses
        it to derive the commit stamp's coverage payload; ``None`` (default)
        records nothing — byte-for-byte unchanged.
    time_range_of : str, optional
        Column name whose observed ``[min, max]`` is reported as
        ``metadata["time_range"]`` (issue #246): the ACTUAL dataset-unit time
        extent of the shard's surviving observations, which the hive write
        path converts to the windowed stamp's ISO-UTC ``time_range`` (D15
        truth). Read AFTER all filters, off the pooled column arrays — so it
        reflects exactly what was written. ``None`` (default) records nothing
        — byte-for-byte unchanged. The streaming buffered path (issue #148)
        does not pool columns, so the key is omitted there; the column must
        be a read column (declared, e.g. the windowing ``time_field``) or the
        key is likewise omitted.
    profile : bool, optional
        Retained knob from the opt-in era (issue #100 phase 2); phase-timing
        COLLECTION is now always-on (issue #297): ``metadata["phase_timings"]``
        always carries ``read`` / ``index`` / ``aggregate`` wall-clock seconds
        (``time.time()`` deltas — a handful of calls per shard, no measurable
        probe tax) so the per-shard stats sidecar is complete by default. The
        flag still gates *verbosity* elsewhere (e.g. the dispatcher's summary
        rollups and the raster per-stage stats); it no longer changes this
        function's behavior. (The ``write`` phase runs in the caller, outside
        this function.)
    windows : list of dict, optional
        Bulk multi-window emit (issue #586 phase 2): the unit's time windows,
        ``{"label", "start", "end", "granules"?}`` — bounds half-open in
        dataset units, ``granules`` the indices (into ``granule_urls``) of the
        granules that belong to the window. The shard is read ONCE; each group
        read is split on ``time_field`` into the windows' sinks
        (:class:`~zagg.processing.windowed.WindowBins`), and the aggregate
        tail then runs once per window, in order, through ``emit_window``:
        ``emit_window(window, aggregate)`` is called per window with a
        callable that takes this function's own sink kwargs
        (``chunk_results`` / ``write_chunk`` / ``ragged_out`` /
        ``occupied_out``) and returns that window's metadata dict — the same
        shape a ``(shard, window)`` unit's metadata has, plus ``window`` and
        ``unit_windows`` — after which the window's reads are released before
        the next window aggregates. The sink kwargs given to this function
        are ignored on that path (each window brings its own); the return is
        an empty carrier plus the shard-level metadata (read-phase fields, the
        windows' summed ``total_obs`` / ``cells_with_data`` / timings). A
        window whose sink is empty reports ``No data after filtering`` exactly
        as a fan-out unit whose filtered read kept nothing.
    time_field : str, optional
        The per-observation timestamp column ``windows`` bins on (the windowing
        declaration's ``time_field``). Required with ``windows``.
    emit_window : callable, optional
        The per-window consumer described under ``windows``. Required with it.

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``total_obs_read`` (issue #374 —
        rows decoded pre-filter, present only when a read route measured it),
        ``granule_count``, ``files_processed``, ``duration_s``, ``error``. Ragged fields are
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
    # Log lines carry the external shard label (decimal morton string for
    # HEALPix — issue #199); ``shard_key`` itself stays the packed int (the
    # canonical wire/metadata form).
    label = shard_label(grid, shard_key)
    logger.info(f"Processing shard: {label}")
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
        logger.info(f"  No granules provided for shard {label} - skipping")
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
    # Granule-scope failures (fold review, issue #341): a fault that kills the
    # WHOLE granule rather than one group -- an H5Coro construction failure, a
    # bad/expired credential, a URL-rewriter fault, a streaming flush blowing up
    # -- is warn-and-continue by design (issue #116 semantics), but it used to
    # leave NO trace in the result: not counted, not exemplared, so a shard whose
    # every granule failed at granule scope returned the blind "No data after
    # filtering". Counted separately from ``read_errors`` because the scope
    # matters to the diagnosis (all groups vs one), and it is arguably the more
    # likely fleet shape: credentials and endpoints kill granules, not groups.
    granule_errors = 0

    # Read-error exemplars (issue #341): the counter alone made the 121-failure
    # strata run a blind "No data after filtering (N group reads raised)" —
    # nothing in the log or the result carried even one message. Record the
    # first _EXEMPLAR_LIMIT distinct messages for the result payload, and log
    # the FULL traceback (WARNING) on the first occurrence of each of the first
    # _TRACEBACK_LIMIT distinct messages — repeats keep the existing one-line
    # warning. The two budgets are SEPARATE (fold review): the payload stays
    # small on the wire while a distinct message beyond the payload cap still
    # gets its traceback in the log, so a real cause behind three flavors of
    # transient noise is not silently dropped. Lock-guarded: group errors are
    # raised inside granule-pool threads (issue #180).
    _exemplar_lock = threading.Lock()
    read_error_exemplars: list = []
    traced_messages: set = set()
    # Auth-shaped failures, counted separately (issue #449): a denied read is a
    # CONFIGURATION fault, not a data fault, and the two want opposite
    # responses -- rerunning a shard whose credentials are wrong for the DAAC
    # just burns another invoke. Only DEFINITE matches count (fold review):
    # see :func:`is_auth_failure`. ``empty_body_errors`` is the ambiguous
    # sibling -- a hint on the generic message, never a classification
    # (:func:`is_empty_body_failure`), so it stays off the wire.
    auth_errors = 0
    empty_body_errors = 0

    def _record_read_error(what: str, exc: Exception) -> None:
        """Warn + exemplar one read failure. ``what`` is the log phrase (and so
        the scope): ``reading track <group>`` or ``processing file <url>``."""
        nonlocal auth_errors, empty_body_errors
        msg = f"{type(exc).__name__}: {exc}"[:_EXEMPLAR_CHARS]
        with _exemplar_lock:
            if is_auth_failure(exc):
                auth_errors += 1
            elif is_empty_body_failure(exc):
                empty_body_errors += 1
            if msg not in read_error_exemplars and len(read_error_exemplars) < _EXEMPLAR_LIMIT:
                read_error_exemplars.append(msg)
            trace = msg not in traced_messages and len(traced_messages) < _TRACEBACK_LIMIT
            if trace:
                traced_messages.add(msg)
        # A raised read error is always a real failure: a legitimately-empty
        # group returns ``None`` (no exception), so WARNING does not get noisy
        # on shards where many granules contribute 0 photons (issue #116).
        # Logging at DEBUG hid the dem_h broadcast failure behind the
        # misleading "No data after filtering"; swallowing the traceback hid
        # the strata AttributeError entirely (issue #341).
        logger.warning(f"  Error {what}: {exc}", exc_info=trace)

    # Streaming buffered aggregation (issue #148 phase 4): when
    # ``aggregation.streaming`` is set, reads accumulate for ``buffer_granules``
    # granules and are flushed instead of pooling the whole shard. ``mode:
    # merge`` (default) folds each flush into running per-cell state (mergeable
    # reducers only, validated up front); ``mode: spill`` (issue #217) appends
    # the grouped flush to per-partition ``/tmp`` files and aggregates once
    # after the reads — full pooled reducer surface, byte-identical to pooled
    # in the single-block regime. ``None`` (default) is the unchanged pooled
    # path.
    from zagg.processing.spill import SpillAggregator, SpillOverflowError, SpillReduceError
    from zagg.processing.streaming import StreamingAggregator, get_streaming

    streaming_cfg = get_streaming(config)
    spill_mode = streaming_cfg is not None and streaming_cfg["mode"] == "spill"

    def _make_buffered():
        if spill_mode:
            return SpillAggregator(
                config,
                grid,
                handoff,
                streaming_cfg["buffer_granules"],
                block_bytes=streaming_cfg["block_bytes"],
            )
        return StreamingAggregator(config, grid, handoff, streaming_cfg["buffer_granules"])

    # Bulk multi-window emit (issue #586): the windows' sinks stand in for the
    # shard's single one — a list of reads per window, or one streaming
    # aggregator per window (each at the fan-out unit's own threshold).
    bins = None
    if windows is not None:
        if not time_field or emit_window is None:
            raise ValueError("process_shard(windows=...) requires time_field and emit_window")
        from zagg.processing.windowed import WindowBins

        bins = WindowBins(windows, time_field, _make_buffered if streaming_cfg else None)
        buffered = None
    else:
        buffered = None if streaming_cfg is None else _make_buffered()

    # Per-phase timing (issue #100; always-on collection since issue #297 —
    # the stats sidecar needs complete timings by default, and the cost is a
    # few time.time() calls per shard).
    phase_timings: dict = {}
    _read_t0 = time.time()

    # Granule fan-out width (issue #180). Resolved before any read so a bad
    # value is a loud config error, not N per-granule warnings. (Backend
    # thread-safety under the pool is a dependency contract, not a runtime
    # check: sidecar's on_miss: build delegate needs h5coro-hidefix >= 0.3.1
    # — its lazy-init race was fixed upstream — enforced by the pyproject pin.)
    granule_workers = _granule_workers(data_source)

    def _read_granule(entry) -> tuple:
        """One granule end-to-end: H5Coro open → group loop → ``finish_granule``
        → close, all in the calling thread (issue #180 — under the pool each
        granule gets its own ``H5Coro``, never shared across threads).

        ``entry`` is a plain URL string (single-asset granule, unchanged), or a
        ``{"url": ..., "assets": {name: url}}`` mapping from a paired-asset
        shard map (issue #425): each sibling asset gets its own ``H5Coro``
        opened beside the primary and handed to the read as ``siblings`` so
        the vlen route's asset filters can join per record; all handles are
        released together in the ``finally``.

        Returns ``(reads, group_errors, io_stats)``: ``reads`` is the carriers
        of the groups that returned data (group order, ``None`` — legitimately
        empty — groups dropped); ``group_errors`` counts raised group reads,
        warned here but folded into ``read_errors`` by the main thread (a
        shared ``+= 1`` from worker threads could race); ``io_stats`` is this
        granule's read counters (issue #374), allocated FRESH here so it is
        written only by the thread that owns the granule and read by the main
        thread after the result is handed back — the same no-lock argument as
        ``group_errors``, rather than a shared dict the pool would race on.
        Raises when the granule itself fails (e.g. the open) — the caller warns
        and skips it, shard continues (issue #116 semantics).
        """
        if isinstance(entry, dict):
            s3_url, asset_urls = entry["url"], dict(entry.get("assets") or {})
        else:
            s3_url, asset_urls = entry, {}
        h5obj = None
        siblings: dict = {}
        reads: list = []
        group_errors = 0
        io_stats: dict = {}
        try:
            resource_path = _rewrite_url(s3_url)

            h5obj = _processing.h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )
            for name, url in asset_urls.items():
                siblings[name] = _processing.h5coro.H5Coro(
                    _rewrite_url(url),
                    h5coro_driver,
                    credentials=credentials,
                    errorChecking=True,
                    verbose=False,
                )

            for g in data_source["groups"]:
                try:
                    read_kwargs = {"arrow": use_arrow, "io_stats": io_stats}
                    if siblings:
                        read_kwargs["siblings"] = siblings
                    if apriori:
                        read_kwargs["granule_url"] = s3_url
                    chunk = index_backend.read_group(
                        h5obj, g, data_source, shard_key, grid, **read_kwargs
                    )
                    if chunk is not None:
                        reads.append(chunk)
                except Exception as e:
                    # Warn + collect an exemplar (see _record_read_error): the
                    # error is folded into ``read_errors`` by the main thread.
                    group_errors += 1
                    _record_read_error(f"reading track {g}", e)
                    continue

            # Per-granule backend hook (issue #160): side effects only (e.g.
            # ``inline`` write-back). A failure here never fails the read —
            # the granule's data is already in ``reads``.
            try:
                index_backend.finish_granule(h5obj, s3_url)
            except Exception as e:
                # Inline the reason instead of ``exc_info=True`` (the sibling
                # tolerated-warning style above): a folded traceback in the
                # log would trip the WorkerErrorCount metric filter (issue
                # #175) on a path that never fails the read.
                logger.warning(f"  index backend finish_granule failed for {s3_url}: {e}")

            return reads, group_errors, io_stats
        finally:
            # Release this granule's h5coro cache before the next one (issue #66):
            # without it each granule's unevicted cache stays resident for the whole
            # loop → Lambda OOM. ``close()`` is the live path; ``cache.clear()`` is a
            # fallback for builds lacking it. Retained ``reads`` data is already
            # copied off the cache lines (see PR #94), so releasing here is safe.
            # Sibling-asset handles (issue #425) release under the same rule.
            for handle in [h5obj, *siblings.values()]:
                if handle is None:
                    continue
                try:
                    if hasattr(handle, "close"):
                        handle.close()
                    elif getattr(handle, "cache", None) is not None:
                        handle.cache.clear()
                except Exception:
                    logger.debug("h5coro cache release failed", exc_info=True)

    def _iter_granule_reads():
        """Yield ``(s3_url, reads, group_errors, io_stats)`` in ``granule_urls`` order.

        ``granule_workers == 1`` reads each granule in this thread — the
        unchanged serial loop. Above 1, up to ``granule_workers`` granules are
        in flight on a bounded ``ThreadPoolExecutor`` (issue #180) and results
        are folded back in submission order: the consumer blocks on the oldest
        future, so an out-of-order completion parks in its future until its
        turn — parked results are bounded by the pool width, and the fold
        (hence the aggregation output) is byte-identical to serial. A granule
        whose read raised is warned, counted (``granule_errors``) and skipped
        here, same as the serial except-continue.
        """
        nonlocal granule_errors
        if granule_workers == 1:
            for index, entry in enumerate(granule_urls):
                s3_url = _entry_url(entry)
                try:
                    yield index, s3_url, *_read_granule(entry)
                except Exception as e:
                    granule_errors += 1
                    _record_read_error(f"processing file {s3_url}", e)
        else:
            with ThreadPoolExecutor(
                max_workers=granule_workers, thread_name_prefix="zagg-granule"
            ) as pool:
                urls = iter(enumerate(granule_urls))
                in_flight = deque(
                    (i, _entry_url(u), pool.submit(_read_granule, u))
                    for i, u in islice(urls, granule_workers)
                )
                while in_flight:
                    index, s3_url, future = in_flight.popleft()
                    try:
                        reads, group_errors, granule_io = future.result()
                    except Exception as e:
                        granule_errors += 1
                        _record_read_error(f"processing file {s3_url}", e)
                        reads = None
                    # Top up BEFORE yielding (review): the head is done either
                    # way, so submitting its replacement here keeps the full
                    # granule_workers in flight while the main thread folds
                    # (including streaming granule_done flushes) — the ≤ K
                    # bound and the fold order are unchanged.
                    for i, u in islice(urls, 1):
                        in_flight.append((i, _entry_url(u), pool.submit(_read_granule, u)))
                    if reads is not None:
                        yield index, s3_url, reads, group_errors, granule_io

    # Observations READ (issue #374): base-rate rows decoded across every
    # granule BEFORE the shard mask, filters, and read-plan segment padding
    # cut them down — the numerator of the read-vs-keep ratio whose
    # denominator is ``total_obs``. ``None`` until some read route reports a
    # count, so a stubbed/older read seam reads as UNMEASURED rather than a
    # real zero (the issue #297 nullable-counter convention).
    obs_read_total: int | None = None

    # Read files and filter spatially, folding granules in original order.
    for index, s3_url, reads, group_errors, granule_io in _iter_granule_reads():
        read_errors += group_errors
        if "obs_read" in granule_io:
            obs_read_total = (obs_read_total or 0) + int(granule_io["obs_read"])
        try:
            if bins is not None:
                bins.add_reads(reads)
            else:
                for chunk in reads:
                    if buffered is not None:
                        buffered.add_read(chunk)
                    else:
                        all_reads.append(chunk)
            files_processed += 1
            if bins is not None:
                bins.granule_done(index)
            elif buffered is not None:
                buffered.granule_done()
        except (SpillOverflowError, SpillReduceError):
            # A spill block overflowed under a non-mergeable config
            # (SpillOverflowError), or an overlap-thread reduce failed and its
            # parked error surfaced at the next block close (SpillReduceError):
            # both are shard-level failures, not per-granule hiccups — a
            # swallowed SpillReduceError would silently drop a block from the
            # emitted output — so propagate loudly instead of warn-and-continue.
            raise
        except Exception as e:
            # Fold-side failure (e.g. a streaming flush): same tolerated
            # warn-and-continue the serial loop's outer ``except`` applied —
            # counted and exemplared like the read-side granule failures, since
            # a shard that folds nothing looks identical to one that read
            # nothing (fold review, issue #341).
            granule_errors += 1
            _record_read_error(f"processing file {s3_url}", e)
            continue

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")
    metadata["files_processed"] = files_processed
    if read_errors:
        metadata["read_errors"] = read_errors
    if granule_errors:
        metadata["granule_errors"] = granule_errors
    if auth_errors:
        # Counted even when the shard still produced data (a partially-denied
        # read is a partially-wrong product), so the run summary can see it.
        metadata["auth_errors"] = auth_errors
    if read_errors or granule_errors:
        # Bounded diagnosis payload (issue #341): messages only, no tracebacks
        # on the wire — full tracebacks were logged above. Success-path
        # payloads (no failures at either scope) are unchanged.
        metadata["read_error_exemplars"] = list(read_error_exemplars)

    if buffered is not None:
        # Drain the tail buffer (< buffer_granules granules) BEFORE the read
        # stamp: intermediate flushes already run inside the read loop
        # (granule_done -> flush), so under profiling the streaming path
        # deliberately charges ALL group+merge cost to ``read`` — the tail
        # flush must not fall between phases and vanish from the accounting.
        buffered.flush()
    if bins is not None:
        bins.flush()
    phase_timings["read"] = time.time() - _read_t0

    # Pre-filter read volume (issue #374), stamped as soon as the read loop is
    # done so it survives the no-data early return below — a shard that decoded
    # millions of photons and kept NONE is precisely the read-vs-keep case this
    # counter exists to expose. Absent when no read route measured it, so
    # absence stays "unmeasured" rather than a fabricated zero.
    if obs_read_total is not None:
        metadata["total_obs_read"] = obs_read_total

    # The aggregate tail, from the no-data check to the carrier return, as a
    # function of its sinks so the bulk multi-window path (issue #586) can run
    # it once per window over that window's reads; the single-unit path calls
    # it exactly once over the shard's. The parameters shadow the enclosing
    # names on purpose — the body is the tail verbatim.
    def _aggregate(
        all_reads,
        buffered,
        *,
        metadata,
        phase_timings,
        chunk_results=None,
        write_chunk=None,
        ragged_out=None,
        occupied_out=None,
    ):
        if buffered.empty if buffered is not None else not all_reads:
            # Distinguish a genuinely-empty read from one where a group read raised
            # (issue #116): a raised read is a real error masquerading as "no data",
            # so report it as such instead of the misleading text. Some groups may
            # have returned ``None`` (legitimately empty) rather than raised, so the
            # message is "no data AND N raised", not "all groups raised".
            if read_errors or granule_errors:
                # Name the SCOPE of the failures, not just a count (fold review):
                # "3 granule reads raised" and "3 group reads raised" point at very
                # different causes (credentials/endpoint vs schema/variable).
                raised = ", ".join(
                    f"{n} {scope} reads raised"
                    for n, scope in ((read_errors, "group"), (granule_errors, "granule"))
                    if n
                )
                exemplars = " | ".join(read_error_exemplars)
                if auth_errors:
                    # Its own failure CLASS (issue #449): the GEDI template shipped
                    # without a credentials_provider, so NSIDC creds hit LP DAAC's
                    # lp-prod-protected and the shard reported the data-shaped "No
                    # data after filtering". The fault is the config, not the data,
                    # and the message must say so. Reached only on a DEFINITE match
                    # (a status code or a denial token in the exception) — the
                    # empty-body shape those 403s also produce is a hint on the
                    # generic branch below, not this class (fold review).
                    logger.error(
                        f"  Access denied reading source granules for shard {label} "
                        f"({auth_errors} auth-shaped failures of {raised}) - skipping"
                    )
                    metadata["error"] = (
                        f"Access denied reading source granules ({auth_errors} auth-shaped "
                        f"failures; {raised}): check data_source.credentials_provider names "
                        f"the DAAC hosting this product; e.g. {exemplars}"
                    )
                else:
                    # Empty-body HINT (fold review, issue #449): the None-body
                    # signature cannot prove a denial — h5coro returns the same
                    # ``None`` for a missing object, throttling, a timeout or a
                    # reset — so it appends likely causes to the generic message
                    # instead of asserting the auth class. Credentials/DAAC
                    # mismatch leads the list because it is the one cause the
                    # operator can only find by being told to look.
                    hint = ""
                    if empty_body_errors:
                        hint = (
                            f"; {empty_body_errors} read(s) got an EMPTY body — likely a denied "
                            f"read (check data_source.credentials_provider names the DAAC hosting "
                            f"this product), a missing object, or throttling"
                        )
                    logger.warning(
                        f"  No data after filtering for shard {label} and {raised}{hint} - skipping"
                    )
                    # Carry the exemplars in the error text too (issue #341): this
                    # string is what surfaces in the status object / run summary, so
                    # it must be a one-glance diagnosis, not just a count.
                    metadata["error"] = (
                        f"No data after filtering ({raised}{hint}; e.g. {exemplars})"
                    )
            else:
                logger.info(f"  No data after filtering for shard {label} - skipping")
                metadata["error"] = "No data after filtering"
            metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
            metadata["phase_timings"] = phase_timings
            return pd.DataFrame(), metadata

        data_vars = get_data_vars(config)
        agg_fields = get_agg_fields(config)
        dense_vars = [
            v for v in data_vars if get_output_signature(agg_fields[v])["kind"] != "ragged"
        ]
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

        _index_t0 = time.time()

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

        # Actual time extent (issue #246): min/max of the declared time column
        # over the pooled (post-filter) observations — two reductions on an array
        # already in hand. The buffered/streaming path holds no pooled columns, so
        # windowed streaming stamps simply omit their time_range (documented).
        if time_range_of is not None and time_range_of in col_arrays and n_obs_total:
            col = col_arrays[time_range_of]
            metadata["time_range"] = [float(col.min()), float(col.max())]

        # Occupied-cell sink (issue #200): both paths already hold the shard's
        # populated cell words — ``cell_to_slice`` pooled, the streaming running
        # state merged (via ``occupied_cells``) — so
        # the occupied set is in hand with no extra observation pass.
        if occupied_out is not None:
            if buffered is not None:
                occupied_out.append(buffered.occupied_cells())
            else:
                occupied_out.append(
                    np.fromiter(cell_to_slice.keys(), dtype=np.uint64, count=len(cell_to_slice))
                )

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
            if spill_mode:
                # Spill path (issue #217): the chunk's partition is read back and
                # driven through the pooled aggregation machinery (single-block) or
                # emitted from the cross-block merged state (multi-block); either
                # way the return is the full _aggregate_chunk_cells 5-tuple, so
                # companion-carrying ragged fields and chunk_precompute are served.
                (
                    stats_arrays,
                    ragged_payloads,
                    ragged_idx,
                    ragged_channels,
                    cwd,
                ) = buffered.chunk_outputs(chunk_children, agg_fields)
            elif buffered is not None:
                # Buffered path (issue #148 phase 4): emit this chunk's outputs from
                # the running merged state; chunk_precompute is rejected at validation
                # so there are no chunk scalars to evaluate. Companion-carrying
                # ragged fields (issue #87's located channel, spec §8.3's temporal
                # one) are likewise rejected by validate_streaming, so the channel
                # sink is empty here by construction.
                stats_arrays, ragged_payloads, ragged_idx, cwd = buffered.chunk_outputs(
                    chunk_children, agg_fields
                )
                ragged_channels = {}
            else:
                # Per-chunk precompute (issue #82 phase 6): pool only this chunk's rows
                # from the shard's sorted column arrays, then reduce the anchor over them.
                chunk_pooled = _pool_chunk_columns(col_arrays, cell_to_slice, chunk_children)
                chunk_scalars = _eval_chunk_precompute(config, chunk_pooled)
                (
                    stats_arrays,
                    ragged_payloads,
                    ragged_idx,
                    ragged_channels,
                    cwd,
                ) = _aggregate_chunk_cells(
                    chunk_children,
                    col_arrays,
                    cell_to_slice,
                    chunk_scalars,
                    config,
                    data_vars,
                    agg_fields,
                    # Already gathered for the precompute above — the toc hoist reuses
                    # it instead of rebuilding the same index (review finding, PR #478).
                    chunk_pooled=chunk_pooled,
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
            # A companion-carrying field appends one element per declared channel, in
            # the ``write._ragged_entry`` order (``locations`` then ``times`` — issue
            # #87 and spec §8.3): the located 3-tuple, the temporal-only 4-tuple with
            # a ``None`` location slot, and the both-channels 4-tuple. A field with
            # neither channel keeps the 2-tuple contract unchanged.
            ragged = (
                {
                    name: (
                        ragged_payloads[name],
                        ragged_idx[name],
                        *_channel_entry(ragged_channels.get(name, {})),
                    )
                    for name in ragged_payloads
                }
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

        if spill_mode:
            # Every partition was consumed by the chunk loop; this releases any
            # remainder (defensive) and the cached grouped partition.
            buffered.close()

        phase_timings["aggregate"] = time.time() - _aggregate_t0
        if spill_mode:
            # The espg-approved /tmp throughput instrumentation (issue #217):
            # exact bytes spilled plus the wall spent in partition appends and
            # read-backs. Read-backs can land in either the read phase (block
            # closes mid-read) or the aggregate phase (single-block reduce).
            phase_timings["spill_write_s"] = buffered.spill_write_s
            phase_timings["spill_read_s"] = buffered.spill_read_s
            phase_timings["spill_bytes"] = buffered.spill_bytes
            # Fold-regime marker (issue #370): blocks closed at the threshold.
            # 0 = exact single-block regime; > 0 = this leaf's outputs were folded
            # across blocks. Split out of the seconds-only timings by build_record,
            # like spill_bytes.
            phase_timings["spill_blocks_closed"] = buffered.closed_blocks
        metadata["phase_timings"] = phase_timings

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed shard {label} in {duration:.1f}s")

        metadata["cells_with_data"] = cells_with_data
        metadata["total_obs"] = n_obs_total
        metadata["duration_s"] = duration
        # "Read" already means "kept" in the two lines above (worker.py:673/:676),
        # which is what CloudWatch greps for today, so this one says "Decoded" —
        # one verb, one meaning (review finding, issue #374).
        if obs_read_total is not None:
            logger.info(
                f"  Decoded {obs_read_total:,} observations pre-filter, kept {n_obs_total:,}"
            )

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

    if windows is None:
        return _aggregate(
            all_reads,
            buffered,
            metadata=metadata,
            phase_timings=phase_timings,
            chunk_results=chunk_results,
            write_chunk=write_chunk,
            ragged_out=ragged_out,
            occupied_out=occupied_out,
        )

    # Bulk multi-window emit (issue #586 phase 2): one aggregate pass per
    # window, in dispatch order, over that window's sink alone; the consumer
    # writes the window's leaf inside ``emit_window`` and the window's reads
    # are released before the next one pools, so the slab, the outputs and
    # the spill read-back are bounded by one window. ``granule_count`` is the
    # window's own membership when the dispatcher sent one (the fan-out
    # unit's ``len(granule_urls)``); the read-phase fields are the shard's.
    totals = {"cells_with_data": 0, "total_obs": 0}
    sums: dict = {}
    try:
        for w in windows:
            reads_w, buffered_w = bins.sink(w["label"])
            meta_w = {**metadata, "window": w["label"], "unit_windows": len(windows)}
            if w.get("granules") is not None:
                meta_w["granule_count"] = len(w["granules"])
            timings_w = dict(phase_timings)

            def _aggregate_window(_r=reads_w, _b=buffered_w, _m=meta_w, _t=timings_w, **sinks):
                _aggregate(_r, _b, metadata=_m, phase_timings=_t, **sinks)
                return _m

            emit_window(w, _aggregate_window)
            bins.release(w["label"])
            for key in totals:
                totals[key] += int(meta_w.get(key) or 0)
            for phase, secs in (meta_w.get("phase_timings") or {}).items():
                if phase != "read":
                    sums[phase] = sums.get(phase, 0.0) + secs
    finally:
        bins.close()
    metadata.update(totals)
    metadata["phase_timings"] = {**phase_timings, **sums}
    metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {label}: {len(windows)} windows in {metadata['duration_s']:.1f}s")
    return pd.DataFrame(), metadata


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
