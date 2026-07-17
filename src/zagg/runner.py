"""Pipeline runner with pluggable backends.

Usage from Python (e.g., Jupyter notebook)::

    from zagg import load_config, agg

    config = load_config("atl06.yaml")
    results = agg(config, catalog="catalog.json", store="./output.zarr", max_cells=5)

    # Lambda backend
    results = agg(config, catalog="catalog.json", backend="lambda")
"""

import json
import logging
import os
import random
import statistics
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import timedelta

from zarr import consolidate_metadata

from zagg.auth import get_edl_token, get_nsidc_s3_credentials
from zagg.concurrency import (
    ConcurrencyReport,
    compute_available_workers,
    raise_for_fd_exhaustion,
)
from zagg.config import (
    PipelineConfig,
    get_child_order,
    get_consolidate_metadata,
    get_coverage_moc,
    get_driver,
    get_handoff,
    get_layout,
    get_output_endpoint_url,
    get_output_region,
    get_parent_order,
    get_pipeline_type,
    get_store_layout,
    get_store_path,
    get_windowing,
)
from zagg.dispatch import (
    LAMBDA_MEMORY_GB,
    LAMBDA_PRICE_PER_GB_SEC,
    LAMBDA_RETRY,
    LOCAL_RETRY,
    LambdaExecutor,
    LocalExecutor,
    PreflightReport,
    dispatch,
)
from zagg.grids.base import shard_label
from zagg.grids.morton import morton_word
from zagg.processing import (
    process_shard,
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
    write_shard_to_zarr,
)
from zagg.store import open_object_store, open_store

logger = logging.getLogger(__name__)

_DENSE_DEPRECATION_MSG = (
    "HEALPix 'dense' layout is deprecated; set output.grid.layout: fullsphere "
    "(or omit; fullsphere is now the default). Dense will be removed in a "
    "future release."
)


def _maybe_warn_dense(layout: str) -> None:
    if layout == "dense":
        warnings.warn(_DENSE_DEPRECATION_MSG, DeprecationWarning, stacklevel=3)


def _resolve_function_name(config: PipelineConfig, function_name: str | None) -> str:
    """Resolve the Lambda function to invoke (issue #235).

    Precedence: an explicit ``function_name`` (``agg`` kwarg /
    ``--function-name``) wins verbatim; otherwise the base name comes from
    ``ZAGG_LAMBDA_FUNCTION_NAME`` (default ``"process-shard"``) and the
    config's optional top-level ``worker:`` block appends the
    pre-provisioned variant suffix — ``-<memory>``, plus ``-disk`` when
    ``extra_disk`` is true (validated against the provisioned set at config
    load). No block -> the bare base name, byte-identical prior behavior.
    Shared by the spatial, raster, and temporal Lambda paths.
    """
    if function_name is not None:
        return function_name
    base = os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard")
    worker = config.worker
    if not worker:
        return base
    suffix = f"-{worker['memory']}"
    if worker.get("extra_disk"):
        suffix += "-disk"
    return base + suffix


def agg(
    config: PipelineConfig,
    *,
    catalog: str | None = None,
    store: str | None = None,
    backend: str = "local",
    driver: str | None = None,
    max_cells: int | None = None,
    morton_cell: str | None = None,
    max_workers: int | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    function_name: str | None = None,
    region: str = "us-west-2",
    output_credentials: dict | None = None,
    output_endpoint_url: str | None = None,
    handoff: str | None = None,
    profile: bool = False,
    max_retries: int = 3,
    invocation: str = "async",
    force_cold: bool = False,
    events=None,
) -> dict:
    """Run the aggregation pipeline.

    Parameters
    ----------
    config : PipelineConfig
        Pipeline configuration (from ``load_config`` or ``default_config``).
    catalog : str, optional
        Path to granule catalog JSON. Overrides ``config.catalog``.
    store : str, optional
        Output store path (local or ``s3://``). Overrides ``config.output.store``.
    backend : str
        Execution backend: ``"local"`` (ThreadPoolExecutor) or
        ``"lambda"`` (AWS Lambda invocation).
    driver : str, optional
        Data access driver: ``"s3"`` (direct S3, us-west-2 only) or
        ``"https"`` (HTTPS, works anywhere). Overrides
        ``config.data_source.driver``. Default ``"s3"``.
    max_cells : int, optional
        Limit number of cells to process (for testing).
    morton_cell : str, optional
        Process a single specific morton cell.
    max_workers : int, optional
        Max concurrent workers. Defaults to 4 (local) or 1700 (lambda).
    overwrite : bool
        Overwrite existing Zarr template.
    dry_run : bool
        Preview what would be processed without running.
    function_name : str, optional
        Lambda function name; an explicit value wins verbatim. Default
        resolves env ``ZAGG_LAMBDA_FUNCTION_NAME`` (or ``"process-shard"``)
        plus the config ``worker:`` block's pre-provisioned variant suffix
        (``-<memory>``/``-disk``, issue #235) via
        :func:`_resolve_function_name`. Only used with ``backend="lambda"``.
    region : str
        AWS region for S3 and Lambda. Default ``"us-west-2"``.
    output_credentials : dict, optional
        Explicit credentials for writing the output store (camelCase
        ``accessKeyId``/``secretAccessKey``/``sessionToken``). Omit to use
        the ambient credential chain / execution role (writes to in-account
        buckets, unchanged behavior). Supply to write an external or
        S3-compatible target (e.g. source.coop). Runtime-only -- never read
        from config.
    output_endpoint_url : str, optional
        Custom S3-compatible endpoint for the output store (e.g. R2, MinIO).
        Overrides ``output.endpoint_url`` in the config.
    handoff : str, optional
        Per-cell aggregation carrier: ``"arrow"`` (an ``arro3.core`` carrier) or
        ``"pandas"``. Both produce byte-for-byte identical scalar outputs (#30);
        ``"arrow"`` is faster and lighter on dense shards (issue #130). Default
        ``None`` reads the carrier from the config (``aggregation.handoff``, itself
        defaulting to ``"arrow"`` — issue #132) via :func:`get_handoff`; an explicit
        kwarg overrides the config, mirroring the ``driver`` precedence. Honored by
        both the ``"local"`` and ``"lambda"`` backends: the lambda backend forwards
        a non-default carrier into each cell event, and an absent key keeps that
        event payload byte-identical (the worker then derives the carrier from the
        forwarded config). pyarrow is not used on either path; the experimental
        ``arrow-kernel`` reducer was dropped with pyarrow.
    profile : bool
        Opt-in per-phase timing (issue #100). On the *point* path, ``True``
        (lambda backend only) forwards ``profile`` into each cell event so the
        worker emits a ``phase_timings`` (read/index/aggregate/write — issue
        #249) sub-dict, and the run prints a per-phase worker breakdown. On the
        *raster* path, ``True`` profiles on **both** backends: the worker emits
        the issue #249 stage set (open/geometry/fetch/decode/gather + write),
        rolled up into ``summary["worker_stage_max"]`` (straggler-maxed seconds)
        and ``summary["worker_stage_counts"]`` (summed work counts) rather than
        printed. Default ``False`` leaves the worker path and per-cell event
        payload byte-identical -- no probe tax.
    max_retries : int
        Lambda-only (issue #119). Per-cell retry budget for *transient*
        client-side faults (throttle/network) in ``_invoke_lambda_cell``;
        deterministic Lambda ``FunctionError``s (timeout, OOM, unhandled
        exception) are never retried regardless. Default ``3``. Ignored by the
        ``"local"`` backend. Set to ``1`` (e.g. the CI benchmark) to measure one
        clean invocation and record a failure as a failure.
    invocation : str
        Lambda-only (issue #151). ``"async"`` (default) dispatches each cell
        with ``InvocationType="Event"`` and polls a per-shard result object the
        worker writes next to the output store
        (``<store>.status/<run_id>/<shard_label>.json``, where the label is
        the decimal morton string for HEALPix — issue #199), so no synchronous
        connection sits idle while the shard runs -- shards longer than a NAT
        idle window (~4 min on GitHub-hosted runners) complete reliably, and
        the 6 MB synchronous response cap no longer applies. Requires (a) a
        deployed worker that honors the ``result_url`` event key and (b) the
        caller to have read access to the output bucket for the poll. Note the
        async request payload cap is 256 KB (vs 6 MB synchronous); pass
        ``"sync"`` for older deployed workers or extreme per-cell payloads
        (granule-dense shards with large AOI masks). Ignored by the
        ``"local"`` backend.
    force_cold : bool
        Lambda-only (issue #171), default ``False``. The explicit big-hammer
        for certification runs -- benchmark baselines, memory forensics, or
        any run that must be provably unaffected by prior container state:
        merge a per-run ``ZAGG_COLD_EPOCH`` marker into the function's
        environment before fan-out and wait for the update to apply -- any
        configuration change invalidates every warm sandbox, so each worker
        starts on a fresh container. Existing environment variables (e.g.
        the issue #143 malloc tunables) are preserved. Requires
        ``lambda:GetFunctionConfiguration`` and
        ``lambda:UpdateFunctionConfiguration`` on the caller -- a broad
        write on the production function, which is why this is opt-in --
        and raises (rather than silently degrading to warm containers) when
        the update cannot be applied: an explicit request hard-fails instead
        of quietly not certifying. Costs one config-update round trip plus a
        few seconds of cold init per container, and chills the warm pool for
        every concurrent user of the function. Routine protection against
        the warm-container RSS ratchet (issues #139/#169) does not need this
        flag: workers self-recycle bloated sandboxes via the
        ``ZAGG_RECYCLE_RSS_MB`` / ``ZAGG_RECYCLE_MAX_INVOCATIONS`` function
        env vars (issue #171), independently of ``force_cold`` -- both can
        be on. Ignored by the ``"local"`` backend.

    events : iterable, optional
        Temporal pipeline only (``pipeline.type: temporal``/``event``), one
        work unit per event. Local backend: ``(event_key, event_mask,
        collections, static_data)`` tuples fed to
        :func:`zagg.temporal.process_event` in-process. Lambda backend:
        URI-shaped dicts (``{"event_key", "event_mask_uri", "collection_uris",
        "static_uris", "s3_credentials"?}``) fanned out one ``process_event``
        invoke per event (see :func:`_run_lambda_events`). Ignored by the
        spatial path. Until the event reader + catalog land, the caller
        supplies events directly (e.g. from a notebook).
    Returns
    -------
    dict
        Summary with keys: ``total_cells``, ``cells_with_data``,
        ``cells_error``, ``total_obs``, ``wall_time_s``, ``store_path``.
    """
    # Pipeline kind picks the strategy (issue #12, Phase 5). The strategy seam
    # is dispatch-level: the spatial path is the existing code, moved verbatim
    # into SpatialStrategy so its behavior/output stays byte-identical; the
    # temporal path drives process_event over the same dispatch.py Executor.
    # A spatial-kind config with ``reader: raster`` routes to the pull-NN
    # raster path (issue #218) — same shard fan-out, lean (time, cells) writes.
    kind = get_pipeline_type(config)
    if kind == "spatial" and (config.data_source or {}).get("reader") == "raster":
        kind = "raster"
    strategy = _get_strategy(kind)
    return strategy.run(
        config,
        catalog=catalog,
        store=store,
        backend=backend,
        driver=driver,
        max_cells=max_cells,
        morton_cell=morton_cell,
        max_workers=max_workers,
        overwrite=overwrite,
        dry_run=dry_run,
        function_name=function_name,
        region=region,
        output_credentials=output_credentials,
        output_endpoint_url=output_endpoint_url,
        handoff=handoff,
        profile=profile,
        max_retries=max_retries,
        invocation=invocation,
        force_cold=force_cold,
        events=events,
    )


class SpatialStrategy:
    """The point-cloud -> grid aggregation path (``pipeline.type: spatial``).

    This is the original ``agg`` body, unchanged: resolve catalog/store, build
    the grid, and fan cells out across the local or Lambda backend. Wrapping it
    in a strategy keeps the spatial output byte-identical -- the dispatch seam
    is the only new thing; the work below is verbatim.
    """

    def run(
        self,
        config,
        *,
        catalog,
        store,
        backend,
        driver,
        max_cells,
        morton_cell,
        max_workers,
        overwrite,
        dry_run,
        function_name,
        region,
        output_credentials,
        output_endpoint_url,
        handoff,
        profile=False,
        max_retries=3,
        invocation="async",
        force_cold=False,
        events=None,
    ):
        # Resolve catalog and store
        catalog_path = catalog or config.catalog
        if not catalog_path:
            raise ValueError("No catalog specified (pass catalog= or set catalog: in config)")
        store_path = store or get_store_path(config)
        if not store_path:
            raise ValueError("No store path specified (pass store= or set output.store: in config)")

        # child_order is HEALPix-specific (leaf order); other grids don't define it.
        grid_type = config.output.get("grid", {}).get("type", "healpix")
        child_order = get_child_order(config) if grid_type == "healpix" else None
        _maybe_warn_dense(get_layout(config))

        # Resolve driver: kwarg > config > default
        resolved_driver = driver or get_driver(config)

        # Resolve carrier: explicit kwarg > config (aggregation.handoff > "arrow").
        # Mirrors the driver precedence above (issue #132).
        resolved_handoff = handoff if handoff is not None else get_handoff(config)

        # Output endpoint/region are non-secret: runtime kwarg > config.
        resolved_endpoint = output_endpoint_url or get_output_endpoint_url(config)
        config_region = get_output_region(config)
        if config_region and region == "us-west-2":
            region = config_region

        # Load catalog and determine cell count for worker capping
        catalog_data = _load_catalog(catalog_path)
        n_cells = len(
            _select_cells(
                catalog_data,
                morton_cell=morton_cell,
                max_cells=max_cells,
            )
        )

        if backend == "local":
            if max_workers is None:
                max_workers = 4
            max_workers = min(max_workers, n_cells)
            return _run_local(
                config,
                catalog_data,
                store_path,
                child_order,
                max_cells=max_cells,
                morton_cell=morton_cell,
                max_workers=max_workers,
                overwrite=overwrite,
                dry_run=dry_run,
                region=region,
                driver=resolved_driver,
                output_credentials=output_credentials,
                output_endpoint_url=resolved_endpoint,
                handoff=resolved_handoff,
            )
        elif backend == "lambda":
            if max_workers is None:
                max_workers = 1700
            max_workers = min(max_workers, n_cells)
            if not store_path.startswith("s3://"):
                raise ValueError(f"Lambda backend requires s3:// store path, got: {store_path}")
            if invocation not in ("async", "sync"):
                raise ValueError(f"Unknown invocation: {invocation!r} (expected 'async' or 'sync')")
            function_name = _resolve_function_name(config, function_name)
            return _run_lambda(
                config,
                catalog_data,
                store_path,
                child_order,
                max_cells=max_cells,
                morton_cell=morton_cell,
                max_workers=max_workers,
                overwrite=overwrite,
                dry_run=dry_run,
                region=region,
                function_name=function_name,
                output_credentials=output_credentials,
                output_endpoint_url=resolved_endpoint,
                handoff=resolved_handoff,
                profile=profile,
                max_retries=max_retries,
                invocation=invocation,
                force_cold=force_cold,
            )
        else:
            raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")


class TemporalStrategy:
    """The event-streaming aggregation path (``pipeline.type: temporal``/``event``).

    Drives :func:`zagg.temporal.process_event` over the merged ``dispatch.py``
    primitives: one work unit per event, fanned out on a
    :class:`~zagg.dispatch.LocalExecutor` (``backend="local"``) or one Lambda
    ``process_event`` invoke per event via :func:`_run_lambda_events`
    (``backend="lambda"``, issue #12 Phase 8). ``specs_from_config(config)`` is
    resolved once and shared across every event. Until the event reader/catalog
    land, events are supplied via the ``events`` argument: in-memory
    ``(event_key, event_mask, collections, static_data)`` tuples on the local
    backend, URI-shaped dicts on the lambda backend (the worker loads inputs
    from S3 itself).
    """

    def run(
        self,
        config,
        *,
        catalog,
        store,
        backend,
        driver,
        max_cells,
        morton_cell,
        max_workers,
        overwrite,
        dry_run,
        function_name,
        region,
        output_credentials,
        output_endpoint_url,
        handoff,
        profile=False,
        max_retries=3,
        invocation="async",
        force_cold=False,
        events=None,
    ):
        from zagg.config import collection_options
        from zagg.temporal import prepare_collection, process_event, specs_from_config

        if backend not in ("local", "lambda"):
            raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")
        if events is None:
            raise ValueError(
                "temporal pipeline requires events= (local backend: an iterable "
                "of (event_key, event_mask, collections, static_data) tuples; "
                "lambda backend: an iterable of URI dicts -- see "
                "_run_lambda_events); the event reader/catalog lands in a later "
                "phase"
            )

        store_path = store or get_store_path(config)
        specs = specs_from_config(config)
        event_list = list(events)
        if max_cells is not None:
            event_list = event_list[:max_cells]

        if dry_run:
            return {
                "dry_run": True,
                "total_events": len(event_list),
                "n_specs": len(specs),
                "store_path": store_path,
                "backend": backend,
            }

        if backend == "lambda":
            return _run_lambda_events(
                config,
                event_list,
                store_path,
                max_workers=max_workers,
                region=region,
                function_name=function_name,
                output_credentials=output_credentials,
                output_endpoint_url=output_endpoint_url,
                max_retries=max_retries,
                invocation=invocation,
                force_cold=force_cold,
            )

        if max_workers is None:
            max_workers = 4
        max_workers = min(max_workers, len(event_list)) if event_list else 1

        # In-memory event tuples skip the reader, so the declarative collection
        # options (issue #213 Phase 3) are applied here to keep the two
        # backends' semantics identical.
        coll_options = collection_options(config)

        # One work unit per event, catching its own exceptions so one bad event
        # counts as an error and the run continues -- mirrors the spatial local
        # path's tagged-envelope contract so ``_accumulate`` stays simple.
        def _event_work(payload):
            event_key, event_mask, collections, static_data = payload
            try:
                collections = {
                    name: prepare_collection(ds, coll_options.get(name))
                    for name, ds in collections.items()
                }
                results, meta = process_event(
                    event_key,
                    event_mask,
                    collections,
                    specs,
                    static_data,
                )
                return {"event_key": event_key, "ok": True, "results": results, "meta": meta}
            except Exception as e:
                return {"event_key": event_key, "ok": False, "error": e}

        executor = LocalExecutor(
            _event_work,
            max_workers=max_workers,
            pool_factory=ThreadPoolExecutor,
        )
        executor.preflight(len(event_list))

        n = len(event_list)

        def _accumulate(report, i, outcome):
            event_key = outcome["event_key"]
            if not outcome["ok"]:
                report.cells_error += 1
                logger.warning(f"  [{i}/{n}] event {event_key}: ERROR {outcome['error']}")
                return
            report.cells_with_data += 1
            report.total_obs += outcome["meta"].get("timesteps_processed", 0)
            report.results.append(
                {"event_key": event_key, "results": outcome["results"], "meta": outcome["meta"]}
            )

        start_time = time.time()
        try:
            report = dispatch(
                executor,
                event_list,
                retry=LOCAL_RETRY,
                accumulate=_accumulate,
            )
        finally:
            executor.shutdown()
        wall_time = time.time() - start_time

        # Persist the event rows to the tabular output the config selects
        # (issue #12, Phase 6). ``output.format`` picks the serialisation; the
        # tabular writer serialises the rows to ``store_path``. A
        # store that is a bare directory (the default) or has no rows leaves the
        # results in-memory only -- the writer is not invoked. ``s3://`` targets
        # serialise the single Parquet/CSV object via obstore (issue #12, Phase
        # 7b), the same S3 stack the Zarr store uses.
        output_path = _write_tabular_output(
            config,
            store_path,
            report.results,
            credentials=output_credentials,
            endpoint_url=output_endpoint_url,
            region=region,
        )

        summary = {
            "total_events": len(event_list),
            "events_with_data": report.cells_with_data,
            "events_error": report.cells_error,
            "timesteps_processed": report.total_obs,
            "wall_time_s": wall_time,
            "store_path": store_path,
            "output_path": output_path,
            "backend": "local",
            "results": report.results,
        }
        logger.info(
            f"Done: {report.cells_with_data} events, {report.total_obs} timesteps, "
            f"{report.cells_error} errors, {wall_time:.1f}s"
        )
        return summary


# Stage keys of the per-shard raster profile (issue #249): float seconds vs
# int counts, split so the rollup straggler-maxes seconds and sums counts.
_RASTER_STAGE_SECONDS = ("open", "geometry", "fetch", "decode", "gather")
_RASTER_STAGE_COUNTS = ("assets", "tiles", "geom_hits")


def _fold_raster_stages(stage_max: dict, stage_counts: dict, stages: dict, write_s) -> None:
    """Fold one shard's stage stats into the run rollup (issue #250).

    Seconds are straggler-maxed across shards (the ``worker_phase_max``
    framing); counts are summed run totals. ``write_s`` (the handler's write
    bucket / the local path's timed slab writes) rides next to the issue #249
    stage set as ``"write"``. Unknown future stages are ignored so the summary
    schema stays stable; an unprofiled shard folds nothing.
    """
    for key in _RASTER_STAGE_SECONDS:
        if key in stages:
            stage_max[key] = max(stage_max.get(key, 0.0), float(stages[key]))
    for key in _RASTER_STAGE_COUNTS:
        if key in stages:
            stage_counts[key] = stage_counts.get(key, 0) + int(stages[key])
    if write_s is not None:
        stage_max["write"] = max(stage_max.get("write", 0.0), float(write_s))


class RasterStrategy:
    """The raster pull-NN path (issue #218): ``reader: raster`` on a spatial grid.

    One work unit per shard, like :class:`SpatialStrategy`, but the worker is
    :func:`~zagg.processing.raster.process_raster_shard` and the writes are
    ``(time, cells)`` slab assignments — the lean path that bypasses the
    aggregation write machinery. The runner owns the global timestep index and
    the template emission (and, later, the single-writer resize on append).

    Backends: ``"local"`` (thread pool, in-process workers) and ``"lambda"``
    (one synchronous ``mode="process_raster"`` invoke per shard —
    espg-confirmed scope on issue #218). The lambda cut is deliberately the
    simple transport: synchronous invokes with transient-only retries; the
    issue-151 async result-object channel and the preflight concurrency probe
    are follow-ups (raster shards are seconds of COG windows, well inside NAT
    idle limits). Either way the runner owns the template + global time index
    before fan-out (the single-writer append design).

    Summary schema note: ``total_obs`` is shared with the spatial/temporal
    strategies so a caller sees one summary shape across pipeline kinds. On the
    raster path there is no per-cell/per-pixel observation tally, so it counts
    the number of shard×timestep slabs written (the raster analogue of an
    observation count); ``timesteps`` separately carries the global datatake
    count. A dashboard summing ``total_obs`` across kinds mixes units — read it
    per kind.

    ``profile=True`` (issue #250) threads the opt-in ``profile`` event key to
    the workers (byte-identical payload when off) and rolls their
    ``phase_timings`` up into ``summary["worker_stage_max"]`` (straggler-maxed
    seconds per issue #249 stage + the ``write`` bucket) and
    ``summary["worker_stage_counts"]`` (summed work counts); the local backend
    profiles the same stages in-process. The lambda summary also carries the
    always-on worker rollups ``template_s`` / ``lambda_time_s`` /
    ``worker_max_s`` / ``worker_median_s`` / ``max_memory_mb`` (null-safe on
    workers predating the fields) so the release benchmark reads one summary.
    """

    def run(
        self,
        config,
        *,
        catalog,
        store,
        backend,
        max_cells,
        morton_cell,
        max_workers,
        overwrite,
        dry_run,
        region,
        output_credentials,
        output_endpoint_url,
        profile=False,
        **_ignored,
    ):
        from zagg.processing.raster import (
            emit_raster_template,
            new_stage_stats,
            process_raster_shard,
            raster_time_index,
            write_raster_coords,
            write_raster_slab,
        )

        catalog_path = catalog or config.catalog
        if not catalog_path:
            raise ValueError("No catalog specified (pass catalog= or set catalog: in config)")
        store_path = store or get_store_path(config)
        if not store_path:
            raise ValueError("No store path specified (pass store= or set output.store: in config)")
        if backend not in ("local", "lambda"):
            raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")
        if backend == "lambda" and not store_path.startswith("s3://"):
            raise ValueError(f"Lambda backend requires s3:// store path, got: {store_path}")
        # Fail fast rather than pay the full fan-out for per-worker 400s: a dense
        # worker's block indexing needs populated-shard state the raster workers
        # do not carry. The handler rejects it too (defense in depth).
        if backend == "lambda" and get_layout(config) == "dense":
            raise ValueError(
                "raster lambda workers require fullsphere; dense block indexing "
                "needs populated-shard state"
            )

        catalog_data = _load_catalog(catalog_path)
        cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
        if dry_run:
            return _dry_run_summary(cells, store_path)

        from zagg.grids import from_config

        all_shards = [int(s) for s in catalog_data["shard_keys"]]
        grid = from_config(config, populated_shards=all_shards)
        _check_signature(grid, catalog_data)
        time_index, times_us = raster_time_index(catalog_data["granules"])
        if not time_index:
            raise ValueError("catalog carries no raster granule entries (no assets/datetime)")

        resolved_endpoint = output_endpoint_url or get_output_endpoint_url(config)
        zarr_store = open_store(
            store_path,
            region=region,
            credentials=output_credentials,
            endpoint_url=resolved_endpoint,
        )
        # Template emission is orchestrator-owned; time it (always-on, the
        # issue #180 bracket convention) so the benchmark harness can split
        # setup-ish wall from the fan-out (issue #250).
        template_t0 = time.time()
        emit_raster_template(zarr_store, grid, config, times_us, overwrite=overwrite)
        template_s = time.time() - template_t0

        if backend == "lambda":
            return self._run_lambda_shards(
                config,
                cells,
                time_index,
                grid,
                store_path,
                max_workers=max_workers,
                region=region,
                function_name=_ignored.get("function_name"),
                max_retries=_ignored.get("max_retries") or 3,
                output_credentials=output_credentials,
                output_endpoint_url=resolved_endpoint,
                profile=profile,
                template_s=template_s,
            )

        source = config.data_source or {}
        src_kwargs = {
            "region": source.get("source_region"),
            "anonymous": source.get("anonymous", True),
        }
        max_workers = min(max_workers or 4, len(cells)) or 1
        t0 = time.time()
        shards_with_data = 0
        errors = 0
        timesteps_written = 0
        last_error = None

        def _one(pair):
            shard_key, granules = pair
            wrote = False
            # Per-stage sample profiling (issue #249), the local flavor of the
            # lambda handler's opt-in ``profile`` key: allocated when profiling
            # (issue #250) or when debug logging is on, so the default path
            # passes None and the sample path times nothing.
            stage_stats = (
                new_stage_stats() if profile or logger.isEnabledFor(logging.DEBUG) else None
            )
            write_s = 0.0

            def _write_slab(t_idx, slab):
                nonlocal wrote, write_s
                w0 = time.time() if profile else 0.0
                write_raster_slab(zarr_store, grid, int(shard_key), t_idx, slab)
                if profile:
                    write_s += time.time() - w0
                wrote = True

            # Stream: write + free each timestep's slab as it completes (issue
            # #231), so a shard holds ~1 slab, not all T.
            _slabs, meta = process_raster_shard(
                grid,
                int(shard_key),
                granules,
                config,
                time_index,
                on_slab=_write_slab,
                stage_stats=stage_stats,
                **src_kwargs,
            )
            if wrote:
                write_raster_coords(zarr_store, grid, int(shard_key))
            if stage_stats is not None:
                logger.debug(
                    f"raster shard {shard_label(grid, int(shard_key))} stages: {stage_stats}"
                )
            return meta, stage_stats, write_s

        stage_max: dict = {}
        stage_counts: dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_one, pair): pair[0] for pair in cells}
            for fut in as_completed(futures):
                label = shard_label(grid, futures[fut])
                try:
                    meta, stage_stats, write_s = fut.result()
                except Exception as e:  # noqa: BLE001 - per-shard isolation, run continues
                    errors += 1
                    last_error = e
                    logger.warning(f"raster shard {label} failed: {e}")
                    continue
                if meta["timesteps"]:
                    shards_with_data += 1
                    timesteps_written += meta["timesteps"]
                if profile and stage_stats is not None:
                    _fold_raster_stages(stage_max, stage_counts, stage_stats, write_s)

        wall_time = time.time() - t0
        # Per-shard isolation lets one bad shard be counted and skipped, but a
        # run where EVERY shard raised (e.g. a config band whose ``asset`` is
        # absent from every granule) would otherwise return a success-shaped,
        # all-fill summary. Fail loudly instead so a caller that does not inspect
        # ``cells_error`` cannot mistake a fully-broken run for an empty AOI.
        if cells and errors == len(cells):
            raise RuntimeError(f"all {errors} raster shard(s) failed; last error: {last_error}")
        summary = {
            "total_cells": len(cells),
            "cells_with_data": shards_with_data,
            "cells_error": errors,
            # Shared summary key across strategies. For the raster path this is
            # the count of shard×timestep slabs written, not a per-cell obs tally
            # (see RasterStrategy docstring); ``timesteps`` is the datatake count.
            "total_obs": timesteps_written,
            "timesteps": int(len(times_us)),
            "wall_time_s": wall_time,
            "template_s": template_s,
            "store_path": store_path,
            "backend": "local",
        }
        if profile:
            # Straggler-maxed stage seconds + summed work counts (issue #250);
            # stage seconds are work volume, never a wall decomposition.
            summary["worker_stage_max"] = stage_max
            summary["worker_stage_counts"] = stage_counts
        logger.info(
            f"Done: {shards_with_data}/{len(cells)} shards, {len(times_us)} timesteps, "
            f"{errors} errors, {wall_time:.1f}s"
        )
        return summary

    def _run_lambda_shards(
        self,
        config,
        cells,
        time_index,
        grid,
        store_path,
        *,
        max_workers,
        region,
        function_name,
        max_retries,
        output_credentials,
        output_endpoint_url,
        profile=False,
        template_s=None,
    ):
        """Fan shards out, one synchronous ``mode="process_raster"`` invoke each.

        The worker gets the shard's ShardMap entries plus only its own slice of
        the global time index (the template — runner-emitted before this — is
        the shared truth for the full axis). Transient invoke faults retry with
        backoff; a Lambda ``FunctionError`` or non-200 envelope is a shard
        error (per-shard isolation, all-error raise as on the local backend).
        """
        import boto3
        from botocore.config import Config

        function_name = _resolve_function_name(config, function_name)
        max_workers = min(max_workers or 64, len(cells)) or 1
        client = boto3.client(
            "lambda",
            region_name=region,
            config=Config(
                max_pool_connections=max(max_workers, 10),
                read_timeout=910,
                retries={"max_attempts": 0},
            ),
        )
        config_dict = {
            "data_source": config.data_source,
            "output": config.output,
            "pipeline": config.pipeline,
        }
        # Normalize creds + resolved endpoint into the camelCase envelope the
        # handler's ``_output_store_kwargs`` requires, exactly as the spatial and
        # temporal lambda paths do — so raster inherits snake_case/STS-PascalCase
        # cred leniency and threads a custom (R2/MinIO) output endpoint to workers.
        output_creds_event = _build_output_creds_event(
            output_credentials, output_endpoint_url, region
        )

        def _event(shard_key, granules):
            keys = {e.get("time_key") or e.get("datetime") for e in granules if e.get("assets")}
            ev = {
                "mode": "process_raster",
                "shard_key": int(shard_key),
                "granules": granules,
                "config": config_dict,
                "store_path": store_path,
                "time_index": {k: time_index[k] for k in keys},
            }
            if profile:
                # Opt-in (issue #250): the worker then emits phase_timings
                # (write bucket + issue #249 stages). Absent -> the payload is
                # byte-identical to the pre-profile path (the #100 convention).
                ev["profile"] = True
            if output_creds_event is not None:
                ev["output_credentials"] = output_creds_event
            return ev

        # Substring markers for a transient invoke fault, matched
        # case-insensitively (parity intent with _invoke_lambda_cell's policy,
        # #119): throttles, service faults, connection resets, and read timeouts
        # retry with jittered backoff; everything else is a deterministic error.
        transient_markers = (
            "toomanyrequests",
            "throttling",
            "serviceexception",
            "connection",
            "timeout",
        )

        def _one(pair):
            payload = json.dumps(_event(*pair))
            last = None
            for attempt in range(max_retries):
                try:
                    resp = client.invoke(
                        FunctionName=function_name,
                        InvocationType="RequestResponse",
                        Payload=payload,
                    )
                    raw_text = resp["Payload"].read().decode("utf-8")
                    if resp.get("FunctionError"):
                        # Deterministic for a given shard (timeout/OOM/unhandled):
                        # never retried, mirroring _invoke_lambda_cell (#119).
                        return {"error": f"Lambda error: {raw_text[:150]}", "body": {}}
                    raw = json.loads(raw_text)
                    body = json.loads(raw.get("body", "{}"))
                    if raw.get("statusCode") != 200:
                        return {
                            "error": body.get("error", f"status {raw.get('statusCode')}"),
                            "body": body,
                        }
                    return {"error": None, "body": body}
                except Exception as e:
                    raise_for_fd_exhaustion(e, max_workers)
                    last = str(e)
                    low = last.lower()
                    if not any(t in low for t in transient_markers) or attempt == max_retries - 1:
                        return {"error": last, "body": {}}
                    # Jitter the backoff so a wide synchronous fan-out does not
                    # retry in a synchronized wave (mirrors _invoke_lambda_cell).
                    time.sleep(min(2**attempt, 8) * (0.5 + random.random() / 2))
            return {"error": last, "body": {}}

        t0 = time.time()
        shards_with_data = 0
        errors = 0
        timesteps_written = 0
        last_error = None
        ok_bodies: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_one, pair): pair[0] for pair in cells}
            for fut in as_completed(futures):
                label = shard_label(grid, futures[fut])
                result = fut.result()
                if result["error"]:
                    errors += 1
                    last_error = result["error"]
                    logger.warning(f"raster shard {label} failed: {result['error']}")
                    continue
                body = result["body"]
                ok_bodies.append(body)
                if body.get("timesteps"):
                    shards_with_data += 1
                    timesteps_written += body["timesteps"]

        wall_time = time.time() - t0
        if cells and errors == len(cells):
            raise RuntimeError(f"all {errors} raster shard(s) failed; last error: {last_error}")
        # Worker telemetry rollup (issue #250): billed durations and peak RSS,
        # null-safe on bodies from a worker that predates either field.
        durations = [float(b["duration_s"]) for b in ok_bodies if b.get("duration_s") is not None]
        mems = [float(b["max_memory_mb"]) for b in ok_bodies if b.get("max_memory_mb") is not None]
        summary = {
            "total_cells": len(cells),
            "cells_with_data": shards_with_data,
            "cells_error": errors,
            # Shard x timestep slab tally (see the class docstring note).
            "total_obs": timesteps_written,
            "timesteps": int(len(time_index)),
            "wall_time_s": wall_time,
            "template_s": template_s,
            "lambda_time_s": sum(durations) if durations else None,
            "worker_max_s": max(durations) if durations else None,
            "worker_median_s": statistics.median(durations) if durations else None,
            "max_memory_mb": max(mems) if mems else None,
            "store_path": store_path,
            "backend": "lambda",
        }
        if profile:
            # Straggler-maxed stage seconds (+ the write bucket) and summed
            # work counts across shards (issue #250); the stage seconds are
            # work volume (overlapped samples), never a wall decomposition.
            stage_max: dict = {}
            stage_counts: dict = {}
            for body in ok_bodies:
                pt = body.get("phase_timings") or {}
                _fold_raster_stages(
                    stage_max, stage_counts, pt.get("stages") or {}, pt.get("write")
                )
            summary["worker_stage_max"] = stage_max
            summary["worker_stage_counts"] = stage_counts
        logger.info(
            f"Done (lambda): {shards_with_data}/{len(cells)} shards, "
            f"{errors} errors, {wall_time:.1f}s"
        )
        return summary


# Strategy registry, keyed by pipeline.type (issue #12, Phase 5). ``event`` and
# ``temporal`` share the event-streaming engine; ``spatial`` is the point-cloud
# path. ``raster`` is selected by ``process_data`` when a spatial-kind config
# declares ``reader: raster`` (issue #218). New pipeline kinds register here
# rather than adding another branch to ``agg``.
_STRATEGIES = {
    "spatial": SpatialStrategy,
    "temporal": TemporalStrategy,
    "event": TemporalStrategy,
    "raster": RasterStrategy,
}


def _get_strategy(pipeline_type: str):
    """Return the strategy instance for a pipeline kind (see :data:`_STRATEGIES`)."""
    try:
        return _STRATEGIES[pipeline_type]()
    except KeyError:  # pragma: no cover - get_pipeline_type already gates the set
        raise ValueError(f"No strategy for pipeline.type={pipeline_type!r}") from None


def _run_lambda_events(
    config,
    events,
    store_path,
    *,
    max_workers,
    region,
    function_name,
    output_credentials=None,
    output_endpoint_url=None,
    max_retries=3,
    invocation="async",
    force_cold=False,
):
    """Fan the temporal pipeline out one event per Lambda invoke (issue #12, Phase 8).

    Mirrors the spatial ``_run_lambda`` transport -- the preflight concurrency
    probe, the pool-sized boto3 client, ``LambdaExecutor`` +
    :func:`zagg.dispatch.dispatch`, and the issue-151 async Event-invoke +
    result-object poll -- but each work unit is one event handled by the
    worker's ``mode="process_event"``, reproducing the antarctic_AR_dataset
    orchestrator's one-invocation-per-storm fan-out on zagg's stack. Workers
    run with ``return_results`` set: each returns its flattened result values
    in the response envelope (mirrored through ``result_url`` on async runs)
    and skips its own store write; the driver collects the rows and writes the
    single tabular object once, so N workers never race the shared
    ``store_path``. No setup/finalize invokes -- tabular output has no zarr
    template or metadata consolidation.

    ``events`` items are URI-shaped dicts (the by-reference twin of the local
    backend's in-memory tuples): ``{"event_key", "event_mask_uri",
    "collection_uris", "static_uris", "s3_credentials"?,
    "input_credentials"?}``. ``input_credentials`` (a creds dict or
    ``"unsigned"``) covers the consumer-owned mask + statics; ``s3_credentials``
    covers only the source collections (issue #223). Events without
    per-event ``s3_credentials`` get the shared credentials fetched once from
    the ``data_source.credentials_provider`` registry name, when the config
    sets one (issue #213 Phase 4). The worker loads
    each event's inputs from S3 itself, which keeps the async request payload
    far under the 256 KB Event cap (masks travel by URI, not by value -- unlike
    the AR repo's inline base64 masks, which only fit a synchronous invoke).
    """
    from dataclasses import asdict

    import boto3
    from botocore.config import Config

    event_list = list(events)
    for ev in event_list:
        if not isinstance(ev, dict) or "event_key" not in ev or "event_mask_uri" not in ev:
            raise ValueError(
                "lambda temporal events must be dicts with 'event_key' and "
                "'event_mask_uri' (plus optional 'collection_uris' / "
                "'static_uris' / 's3_credentials') -- the worker loads inputs "
                f"from S3 by URI; got: {ev!r}"
            )
    if not store_path or not store_path.startswith("s3://"):
        raise ValueError(f"Lambda backend requires s3:// store path, got: {store_path}")
    if not store_path.lower().endswith(_TABULAR_SUFFIXES):
        raise ValueError(
            "lambda temporal output requires a concrete tabular store path "
            f"(suffix in {_TABULAR_SUFFIXES}) so the collected rows have a "
            f"target, got: {store_path!r}"
        )
    from zagg.output import output_format

    if output_format(config) == "zarr":
        # output.format defaults to "zarr" (the spatial store); left there, the
        # post-fan-out _write_tabular_output would silently skip the write and
        # discard every collected row after the Lambda spend. Fail before
        # invoking anything.
        raise ValueError(
            "lambda temporal output requires output.format: parquet/csv/tabular "
            '(it defaults to "zarr", which has no tabular target) -- set it in '
            "the config so the collected rows are written"
        )
    if invocation not in ("async", "sync"):
        raise ValueError(f"Unknown invocation: {invocation!r} (expected 'async' or 'sync')")
    function_name = _resolve_function_name(config, function_name)

    # Orchestrator-side credential fetch (issue #213, Phase 4): a config-named
    # provider is resolved through the registry and called ONCE (DAAC creds
    # last ~1 h, far past the fan-out), then attached to every event that does
    # not carry its own per-event s3_credentials.
    provider_name = (config.data_source or {}).get("credentials_provider")
    if provider_name:
        from zagg import registry as zagg_registry

        shared_creds = zagg_registry.get_credential_provider(provider_name)()
        event_list = [
            ev if ev.get("s3_credentials") else {**ev, "s3_credentials": shared_creds}
            for ev in event_list
        ]

    n = len(event_list)
    logger.info(f"Processing {n} events (lambda)")
    if max_workers is None:
        # The AR-repo orchestrator's default fan-out width (one storm per
        # worker); the preflight probe clamps it to account concurrency anyway.
        max_workers = 1000
    max_workers = min(max_workers, n) if n else 1

    config_dict = asdict(config)
    output_creds_event = _build_output_creds_event(
        output_credentials,
        output_endpoint_url,
        region,
    )

    # Async result channel (issue #151), same contract as the spatial path: a
    # per-run unique status prefix next to the tabular store; each worker
    # mirrors its envelope to <prefix>/<event_key>.json and the dispatch
    # threads poll for it instead of holding a synchronous connection open.
    result_prefix = None
    result_box: dict = {}
    if invocation == "async":
        result_prefix = f"{store_path.rstrip('/')}.status/{uuid.uuid4().hex}"
        logger.info(f"Async worker results at {result_prefix}")

    session = boto3.Session()
    state: dict = {}

    def _preflight(n_units):
        # Same probe/clamp/pool-sizing as the spatial _run_lambda preflight,
        # referencing the same module seams (compute_available_workers /
        # _get_function_timeout_s) so tests patch one set of objects.
        probe_lambda = session.client("lambda", region_name=region)
        cloudwatch_client = session.client("cloudwatch", region_name=region)
        clamped, concurrency_report = compute_available_workers(
            max_workers,
            probe_lambda,
            cloudwatch_client,
            function_name,
        )
        _log_concurrency_report(concurrency_report, clamped)
        boto_config = Config(
            read_timeout=900,
            connect_timeout=10,
            retries={"max_attempts": 0},
            max_pool_connections=clamped,
        )
        state["workers"] = clamped
        state["lambda_client"] = session.client(
            "lambda",
            region_name=region,
            config=boto_config,
        )
        state["function_timeout_s"] = _get_function_timeout_s(state["lambda_client"], function_name)
        if force_cold:
            _force_cold_containers(state["lambda_client"], function_name)
        return PreflightReport(workers=clamped, detail=concurrency_report)

    def _event_work(ev):
        extra = {}
        if result_prefix is not None:
            key = f"{ev['event_key']}.json"
            extra["result_url"] = f"{result_prefix}/{key}"
            extra["result_fetch"] = _result_fetcher(
                result_box, result_prefix, output_creds_event, region, key
            )
            extra["poll_timeout_s"] = state["function_timeout_s"] + _ASYNC_POLL_MARGIN_S
        return _invoke_lambda_event(
            state["lambda_client"],
            ev,
            function_name=function_name,
            config_dict=config_dict,
            output_creds_event=output_creds_event,
            max_retries=max_retries,
            max_workers=state["workers"],
            **extra,
        )

    executor = LambdaExecutor(
        _event_work,
        preflight_fn=_preflight,
        pool_factory=ThreadPoolExecutor,
        # Tabular output has no metadata to consolidate; keep the executor
        # contract without a finalize invoke.
        finalize_fn=lambda: None,
    )
    executor.preflight(n)
    max_workers = state["workers"]

    start_time = time.time()
    rows: list[dict] = []

    def _accumulate(report, i, result):
        event_key = result.get("event_key")
        body = result.get("body") or {}
        error = result.get("error")
        if result.get("status_code") == 200 and not error:
            timesteps = body.get("timesteps_processed") or 0
            report.total_obs += timesteps
            report.cells_with_data += 1
            rows.append(
                {
                    "event_key": event_key,
                    "results": body.get("results") or {},
                    # Full worker meta (n_specs/collections/...) when returned;
                    # fall back to the envelope's timestep count so the row
                    # shape matches the local backend either way.
                    "meta": body.get("meta") or {"timesteps_processed": timesteps},
                }
            )
        else:
            report.cells_error += 1
            logger.warning(f"  [{i}/{n}] event {event_key}: {error}")
        report.results.append(result)

        if i % 50 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            logger.info(f"  [{i:4d}/{n}] {rate:.1f} events/s")

    try:
        report = dispatch(
            executor,
            event_list,
            retry=LAMBDA_RETRY,
            accumulate=_accumulate,
            on_submit_error=lambda e: raise_for_fd_exhaustion(e, max_workers),
        )
    finally:
        executor.shutdown()
    wall_time = time.time() - start_time

    # One tabular write for the whole run, from the driver -- same call the
    # local backend makes, so the output object and summary schema match.
    output_path = _write_tabular_output(
        config,
        store_path,
        rows,
        credentials=output_credentials,
        endpoint_url=output_endpoint_url,
        region=region,
    )

    # Cost presentation mirrors the spatial path: one multiply over the summed
    # per-event durations the report accumulated.
    total_lambda_time = report.cost.compute_time_s
    gb_seconds = total_lambda_time * LAMBDA_MEMORY_GB
    estimated_cost = gb_seconds * LAMBDA_PRICE_PER_GB_SEC

    # Failed events keep their error detail (rows carry successes only), so a
    # caller can see *which* events failed and why, not just the count --
    # mirroring the AR orchestrator's collected errors list.
    failures = [
        {"event_key": r.get("event_key"), "error": r.get("error")}
        for r in report.results
        if r.get("status_code") != 200 or r.get("error")
    ]

    # Container-telemetry rollup (issue #171), same additive fields as the
    # spatial summary; the raw envelopes live in report.results (summary
    # "results" carries the collected tabular rows instead).
    container_stats = _container_telemetry_summary([r.get("body") or {} for r in report.results])

    summary = {
        "total_events": n,
        "events_with_data": report.cells_with_data,
        "events_error": report.cells_error,
        "timesteps_processed": report.total_obs,
        "wall_time_s": wall_time,
        "lambda_time_s": total_lambda_time,
        "gb_seconds": gb_seconds,
        "price_per_gb_sec": LAMBDA_PRICE_PER_GB_SEC,
        "estimated_cost_usd": estimated_cost,
        "function_timeout_s": state.get("function_timeout_s", _DEFAULT_FUNCTION_TIMEOUT_S),
        "store_path": store_path,
        "output_path": output_path,
        "backend": "lambda",
        "results": rows,
        "failures": failures,
        **container_stats,
    }
    logger.info(
        f"Done: {report.cells_with_data} events, {report.total_obs} timesteps, "
        f"{report.cells_error} errors, {wall_time:.1f}s, "
        f"~${estimated_cost:.4f} ({gb_seconds:.1f} GB-s)"
    )
    _log_container_stats(container_stats)
    return summary


#: ``output.store`` suffixes that name a concrete tabular output *file* (vs a
#: bare directory). A temporal run whose store ends in one of these writes its
#: event rows there; any other local store path leaves the rows in-memory only.
_TABULAR_SUFFIXES = (".parquet", ".pq", ".csv")


def _write_tabular_output(
    config,
    store_path,
    rows,
    *,
    credentials=None,
    endpoint_url=None,
    region="us-west-2",
):
    """Persist temporal event rows to the config-selected tabular store.

    Resolves the serialisation from ``output.format`` and routes through
    :func:`zagg.output.write_tabular`, which writes a local file or ``put``s a
    single ``s3://`` object via obstore (issue #12, Phase 7b). Returns the
    written path/URI, or ``None`` when nothing is written -- a directory/empty
    store, a ``zarr`` format (which is gridded, not tabular), or an empty result
    set.
    """
    from zagg.output import output_format, write_tabular

    fmt = output_format(config)
    if fmt == "zarr":
        # The gridded writer is for the spatial path; a temporal config left at
        # the default format has no tabular target, so keep the rows in-memory.
        return None
    if not store_path or not store_path.lower().endswith(_TABULAR_SUFFIXES):
        return None  # a bare directory (or unset) store -- nothing to serialise to
    if not rows:
        return None  # no events produced data -- skip the (column-less) write
    return write_tabular(
        rows,
        store_path,
        output_format=fmt,
        credentials=credentials,
        endpoint_url=endpoint_url,
        region=region,
    )


def _load_catalog(catalog_path: str) -> dict:
    """Load a ShardMap manifest from JSON.

    Returns
    -------
    dict
        ``{"grid_signature": ..., "shard_keys": [...], "granules": [[...]],
        "metadata": ...}`` where each granule is ``{"id", "s3", "https"}``.

    Raises
    ------
    ValueError
        If the file is a pre-Phase-5 catalog (URL-list granules, no
        ``grid_signature``); regenerate it with ``python -m zagg.catalog``.
    """
    with open(catalog_path) as f:
        data = json.load(f)
    if "shard_keys" in data and "granules" in data and "grid_signature" in data:
        return data
    raise ValueError(
        f"Catalog at {catalog_path} is not a Phase-5 ShardMap (needs "
        f"'grid_signature' + {{id,s3,https}} granules). Regenerate with "
        f"`python -m zagg.catalog --config ...`."
    )


def _select_cells(
    catalog_data: dict, *, morton_cell: str | None = None, max_cells: int | None = None
) -> list[tuple]:
    """Select (shard_key, granule_urls) pairs from a loaded catalog.

    Parameters
    ----------
    catalog_data : dict
        Loaded catalog (shard_keys/granules format).
    morton_cell : str, optional
        Process a single shard. For a HEALPix catalog this is the shard's
        decimal morton string (e.g. ``-31123`` — issue #199); for other grids
        it is the stringified shard-key int.
    max_cells : int, optional
        Truncate to the first N shards.

    Returns
    -------
    list of (shard_key, granule_urls) tuples, in a deterministic shuffled
    order. The seed derives from the selected shard keys, so a rerun/resume
    with the same catalog and selection args sees the same order (a different
    ``max_cells`` seeds differently, so its order is unrelated -- inherent to
    truncate-first). Determinism relies on ``random.Random(str)`` seeding and
    ``shuffle`` being stable in practice across CPython 3.12/3.13.
    """
    pairs = list(zip(catalog_data["shard_keys"], catalog_data["granules"]))
    if morton_cell:
        grid_type = (catalog_data.get("grid_signature") or {}).get("type")
        if grid_type == "healpix":
            try:
                target = morton_word(morton_cell)
            except ValueError as e:
                raise ValueError(
                    f"--morton-cell {morton_cell!r} is not a decimal morton id "
                    f"(shard ids are decimal morton strings since issue #199): {e}"
                ) from e
        else:
            target = int(morton_cell)
        matches = [(k, urls) for k, urls in pairs if k == target]
        if not matches:
            msg = f"shard '{morton_cell}' not in catalog"
            # A well-formed decimal id can still miss because the catalog itself
            # predates the packed-word form: legacy shard_keys are signed i64
            # decimal ids (small or negative), while packed words carry a 1..12
            # prefix in the top 4 bits (always >= 2^60). Hard break — no shim —
            # but say why the lookup likely failed (review finding, PR #205).
            keys = catalog_data["shard_keys"]
            if grid_type == "healpix" and any(int(k) < (1 << 60) for k in keys):
                msg += (
                    " (catalog shard_keys look like legacy signed decimal ids, not "
                    "packed morton words — a pre-issue-199 shard map; regenerate it "
                    "with `python -m zagg.catalog`)"
                )
            raise ValueError(msg)
        return matches
    if max_cells:
        pairs = pairs[:max_cells]
    # Shuffle after selection/truncation (max_cells keeps its morton-first-N
    # subset) so concurrent fan-out doesn't write morton-contiguous -- i.e.
    # byte-prefix-sharing -- S3 keys to one partition (issue #197). Seeded from
    # the selected shard keys, so a rerun or resume sees the same order.
    random.Random(",".join(str(k) for k, _ in pairs)).shuffle(pairs)
    return pairs


def _safe_label(grid, shard_key) -> str:
    """Render a shard key for log/report lines; NEVER raises (issue #199).

    ``shard_label`` -> ``morton_decimal`` raises on an invalid word — right at
    path-construction sites (a path component must never be silently wrong),
    wrong in error-*reporting* paths: re-rendering the same malformed key that
    made a cell fail would abort the accumulation loop precisely while
    reporting that failure (review finding, PR #205; mortie's own scalar repr
    is non-raising for the same reason). Falls back to the raw digits.
    """
    try:
        return shard_label(grid, shard_key)
    except Exception:
        return str(shard_key)


def _lambda_dispatch_order(cells: list[tuple]) -> list[tuple]:
    """Order cells for lambda fan-out: biggest work first, in coarse buckets.

    Stable descending sort on ``len(granule_urls).bit_length()`` (log2
    buckets), not the exact count: granule counts are spatially
    autocorrelated, so an exact-count sort would mostly undo
    ``_select_cells``'s anti-prefix-locality shuffle (issue #197). Coarse
    buckets keep the longest-first throughput heuristic while the shuffle
    survives within each bucket.
    """
    return sorted(cells, key=lambda kv: len(kv[1]).bit_length(), reverse=True)


def _aoi_payload_map(catalog_data: dict) -> dict:
    """Map ``shard_key -> AOI mask payload`` from a loaded manifest (issue #101).

    Returns ``{}`` when the manifest has no ``aoi_mask`` list (the flag was off at
    build), so the worker appends no mask column and outputs are byte-identical.
    The ``aoi_mask`` list is parallel to ``shard_keys``.
    """
    aoi = catalog_data.get("aoi_mask")
    if not aoi:
        return {}
    return {int(k): payload for k, payload in zip(catalog_data["shard_keys"], aoi)}


def _granule_time_span(record: dict):
    """A granule record's ``(start, end)`` UTC instants, or ``None`` (issue #246).

    New shardmaps carry ``time_start``/``time_end`` (from the catalog's STAC
    ``start_datetime``/``end_datetime``); raster records carry the instant
    ``datetime``. A record with neither (a legacy shardmap) returns ``None``
    — the fan-out then treats it as intersecting EVERY window (conservative:
    the worker's observation-level filter enforces correctness) and window
    enumeration falls back to ``bounds.temporal``.
    """
    from zagg.windows import parse_utc

    start = record.get("time_start") or record.get("datetime")
    if start is None:
        return None
    end = record.get("time_end") or start
    return parse_utc(start), parse_utc(end)


def _windowed_units(cells: list[tuple], windowing: dict, bounds_temporal: dict | None) -> list:
    """Expand ``(shard, records)`` pairs into ``(shard, records, window)`` units.

    One work unit per (shard, window) with a non-empty granule subset (issue
    #246 phase 5): the run's window labels come from the declared explicit
    list, or — for generative schedules — from the union of the granules'
    time spans (legacy shardmaps without per-granule times fall back to
    ``bounds.temporal``, or fail with a pointed remedy). Each unit's
    ``window`` dict carries the label plus its half-open ``[start, end)``
    bounds converted ONCE to dataset units (the ratified fixed-offset
    conversion); granules subset per window by span intersection, spans
    unknown → every window (the worker filter decides membership).
    """
    from zagg.windows import parse_utc, utc_to_offset, window_range, windows_intersecting

    schedule, declared = windowing["schedule"], windowing.get("windows")
    spans = {id(r): _granule_time_span(r) for _k, records in cells for r in records}
    if schedule == "explicit":
        labels = [w["label"] for w in declared]
    else:
        found: set = set()
        for span in spans.values():
            if span is not None:
                found.update(windows_intersecting(*span, schedule))
        if any(span is None for span in spans.values()):
            if not bounds_temporal:
                raise ValueError(
                    "windowing with a generative schedule needs per-granule time "
                    "metadata to enumerate windows, but this shardmap predates it "
                    "(no time_start/time_end on its granule records) — rebuild the "
                    "shardmap with `python -m zagg.catalog`, or set bounds.temporal "
                    "{start_date, end_date} on the run config"
                )
            # A bare ``YYYY-MM-DD`` end_date means end-of-day; a full ISO
            # instant is parsed verbatim (appending the suffix unconditionally
            # would corrupt e.g. ``2020-12-31T00:00:00Z`` into a parse error).
            end_date = bounds_temporal["end_date"]
            if isinstance(end_date, str) and len(end_date) == 10 and "T" not in end_date:
                end_date = f"{end_date}T23:59:59"
            found.update(
                windows_intersecting(
                    parse_utc(bounds_temporal["start_date"]),
                    parse_utc(end_date),
                    schedule,
                )
            )
        labels = sorted(found)
    to_dataset = {
        "epoch": windowing["epoch"],
        "scale": windowing["scale"],
        "units": windowing["units"],
    }
    windows = []
    for label in labels:
        lo, hi = window_range(label, schedule, declared)
        payload = {
            "label": label,
            "start": utc_to_offset(lo, **to_dataset),
            "end": utc_to_offset(hi, **to_dataset),
        }
        windows.append((payload, lo, hi))
    # Shard-major expansion: the incoming cell order (the issue #197 shuffle,
    # the lambda biggest-first buckets) is preserved, windows fan out within it.
    units = []
    for shard_key, records in cells:
        for payload, lo, hi in windows:
            subset = [
                r
                for r in records
                if (span := spans[id(r)]) is None or (span[0] < hi and span[1] >= lo)
            ]
            if subset:
                units.append((shard_key, subset, payload))
    return units


def _resolve_source_credentials(config) -> dict:
    """S3 read credentials for the source datasets, provider-selected.

    ``data_source.credentials_provider`` names a credential-provider registry
    entry (built-ins ``nsidc``/``gesdisc``; plugins may register others,
    including non-NASA S3-compatible sources -- providers run
    orchestrator-side, so the built-ins-only Lambda rule does not apply).
    Absent, the historical spatial default (NSIDC) stands, via the module
    global so it remains overridable.
    """
    name = (config.data_source or {}).get("credentials_provider")
    if not name:
        return get_nsidc_s3_credentials()
    from zagg import registry

    return registry.get_credential_provider(name)()


def _dry_run_summary(cells: list[tuple], store_path: str) -> dict:
    """Return summary without processing.

    Parameters
    ----------
    cells : list of (shard_key, granule_urls) pairs from ``_select_cells``.
    """
    granule_counts = [len(urls) for _, urls in cells]
    return {
        "dry_run": True,
        "total_cells": len(cells),
        "granules_per_cell_min": min(granule_counts),
        "granules_per_cell_max": max(granule_counts),
        "granules_per_cell_avg": sum(granule_counts) / len(granule_counts),
        "store_path": store_path,
    }


def _resolve_urls(records: list, driver: str | None) -> list[str]:
    """Pick the driver-appropriate href from each granule record.

    ShardMap granules are ``{"id", "s3", "https"}``; the run's
    ``data_source.driver`` selects which endpoint to read.
    """
    key = "https" if driver == "https" else "s3"
    return [r[key] for r in records if r.get(key)]


def _clamped_data_source(data_source: dict, n_granules: int) -> dict | None:
    """Per-cell ``granule_workers`` clamp: ``min(K, n_granules)`` (issue #184).

    The shardmap gives the dispatcher each cell's granule count up front, so a
    2-granule cell never asks the worker for a 4-wide pool (the issue #185
    default). Returns a shallow-copied ``data_source`` carrying the clamped
    width when it is below the configured/default K, else ``None`` — the
    caller then passes the shared config through untouched, keeping unclamped
    cell payloads byte-identical. Just the simple ``min()`` policy; the
    worker still resolves the value through its ``_granule_workers`` guard.
    """
    from zagg.processing.worker import _granule_workers

    k = _granule_workers(data_source)
    clamped = min(k, max(int(n_granules), 1))
    if clamped == k:
        return None
    return {**data_source, "granule_workers": clamped}


def _check_signature(grid, catalog_data: dict) -> None:
    """Refuse a ShardMap built for a different *spatial* grid than the run config.

    A ShardMap is a spatial artifact (shard keys + granule→shard assignment), so
    the guard compares only the spatial signature (#89) — one map is reusable
    across configs that share the spatial grid but declare different aggregation
    fields. The stored ``grid_signature`` is projected onto the spatial keys, so
    both new spatial-only maps and old full-signature maps (which also carry
    ``output_fields``) validate.
    """
    expected = catalog_data.get("grid_signature")
    if expected is None:
        return
    actual = grid.spatial_signature()
    stored_spatial = {k: expected.get(k) for k in actual}
    if stored_spatial != actual:
        raise ValueError(
            "ShardMap was built for a different grid than this run config.\n"
            f"  shard map (spatial): {stored_spatial}\n"
            f"  run config (spatial): {actual}\n"
            f"  shard map (raw stored signature): {expected}"
        )


def _process_and_write(
    shard_key,
    chunk_idx,
    records,
    grid,
    s3_creds,
    zarr_store,
    config,
    driver=None,
    handoff="arrow",
    aoi_payload=None,
):
    """Process a single shard and write its K finer chunks to the store.

    Multi-chunk-per-worker (issue #30 item 3): one shard owns
    ``K = grid.chunks_per_shard`` finer Zarr chunks. ``process_shard`` reads the
    granules once and returns one ``(block_index, carrier, ragged)`` per chunk via
    ``chunk_results``; this writes each chunk's dense region (at its own
    ``block_index``) plus its ragged vlen payloads (issue #209). At K==1
    ``chunk_results`` has exactly one entry whose ``block_index`` equals
    ``chunk_idx``, so the write is byte-for-byte the single-chunk path.
    ``chunk_idx`` is retained for the K==1 callers/signature but the per-chunk
    block index from ``iter_chunks`` is used.
    """

    def _write_chunk(block_index, carrier, ragged):
        # write_dataframe_to_zarr no-ops on an empty carrier (DataFrame or Arrow
        # table), so no carrier-specific emptiness check is needed here.
        write_dataframe_to_zarr(carrier, zarr_store, grid=grid, chunk_idx=block_index)
        # Persist this chunk's ragged fields into their vlen-bytes arrays at the
        # same block (issue #209). The array is regular-chunked on this
        # unsharded path, so per-chunk writes stay independent. No-op when
        # ``ragged`` is empty.
        write_ragged_to_zarr(ragged, zarr_store, grid=grid, chunk_idx=block_index)

    # Sharded output (issue #108): the shard's K inner chunks bundle into one
    # ShardingCodec shard object — write the whole shard in one block selection per
    # dense array (a per-inner-chunk loop would read-modify-write the shard object).
    # That path needs all K at once, so it accumulates via ``chunk_results``; the
    # non-sharded path streams each chunk write-then-free via ``write_chunk`` (#91).
    sharded = getattr(grid, "sharded", False)
    chunk_results: list | None = [] if sharded else None
    _df_out, metadata = process_shard(
        grid,
        int(shard_key),
        _resolve_urls(records, driver),
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        handoff=handoff,
        chunk_results=chunk_results,
        aoi_payload=aoi_payload,
        write_chunk=None if sharded else _write_chunk,
    )
    if sharded:
        write_shard_to_zarr(chunk_results, zarr_store, grid=grid, shard_key=int(shard_key))
    return metadata


def _run_local(
    config,
    catalog_data,
    store_path,
    child_order,
    *,
    max_cells,
    morton_cell,
    max_workers,
    overwrite,
    dry_run,
    region,
    driver="s3",
    output_credentials=None,
    output_endpoint_url=None,
    handoff="arrow",
):
    """Run processing locally via the generic dispatch loop on a thread pool.

    This is the trivial backend: a :class:`~zagg.dispatch.LocalExecutor` over a
    ``ThreadPoolExecutor`` with no metered cost. Per-cell exception handling
    differs from Lambda -- a raised cell exception is *counted* as an error and
    the run continues (Lambda instead only surfaces its run-fatal errno-24) --
    so the work callable catches and tags exceptions and ``_accumulate``
    reproduces the original counting exactly, keeping the summary byte-identical.
    """
    all_shards = list(catalog_data["shard_keys"])

    cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    # Strict-AOI per-shard mask payload (issue #101), keyed by shard for the
    # per-cell lookup. Empty dict when the manifest carries no ``aoi_mask`` (flag
    # off) — the worker then appends no column and outputs stay byte-identical.
    aoi_by_shard = _aoi_payload_map(catalog_data)
    logger.info(
        f"Processing {len(cells)} of {len(all_shards)} cells (local, {max_workers} workers, driver={driver})"
    )

    if dry_run:
        return _dry_run_summary(cells, store_path)

    # Authenticate based on driver
    if driver == "https":
        s3_creds = {"edl_token": get_edl_token()}
    else:
        s3_creds = _resolve_source_credentials(config)

    # Build grid from the run config (single source of truth) and refuse a
    # shard map built for a different grid. For HEALPix-dense, populated_shards
    # order matches the catalog's shard_keys list (sorted at build time).
    from zagg.grids import from_config

    layout = get_layout(config)
    grid_type = config.output.get("grid", {}).get("type", "healpix")
    if grid_type == "healpix" and layout == "dense":
        grid = from_config(config, populated_shards=[int(s) for s in all_shards])
    else:
        grid = from_config(config)
    _check_signature(grid, catalog_data)
    store_layout = get_store_layout(config)
    store_kwargs = {
        "region": region,
        "credentials": output_credentials,
        "endpoint_url": output_endpoint_url,
    }
    windowing = get_windowing(config)
    if store_layout == "hive":
        # Hive layout (issue #199 phase 2): no shared zarr template — zero
        # metadata above the leaves (D5); each shard emits its own leaf
        # template lazily inside hive.process_and_write_hive (the leaf write
        # path lives in zagg.hive, next to the manifest/stamp machinery it
        # exercises). The root manifest (D6) is written HERE, pre-dispatch
        # (issue #252 hybrid): the local dispatcher writes the store
        # directly, so the write costs ~0 wall and a reader can consume
        # completed leaves while the run builds — mirroring the lambda
        # backend's async init-time setup invoke.
        from zagg.hive import (
            build_manifest,
            ensure_manifest,
            process_and_write_hive,
        )

        zarr_store = None
        # Build the manifest once, up front, and reuse it at finalize.
        # ensure_manifest runs the read-only validate_manifest frozen-key
        # precheck before its PUT, so the pre-dispatch fail-fast (review
        # fold, issue #252) is preserved: a rerun into an incompatible
        # existing store refuses in ~0s, before any leaf write (D2), instead
        # of mixing new-order leaves into an old-order store.
        manifest = build_manifest(grid, dataset=catalog_data.get("metadata"), windowing=windowing)
        ensure_manifest(store_path, manifest, overwrite=overwrite, **store_kwargs)
        # Temporal fan-out (issue #246 phase 5): one work unit per (shard,
        # window). None (schedule none/absent) keeps the (shard, records)
        # pairs — dispatch byte-identical to pre-windowing runs.
        if windowing is not None:
            cells = _windowed_units(cells, windowing, (config.bounds or {}).get("temporal"))
    else:
        zarr_store = open_store(store_path, **store_kwargs)
        zarr_store = grid.emit_template(zarr_store, overwrite=overwrite)

    # Per-cell work, catching its own exceptions so one bad cell counts as an
    # error and the run continues (the old loop's ``except`` branch). The
    # outcome is tagged in a private envelope the accumulator unpacks; on the
    # error path nothing is appended to ``results``, matching the old behavior.
    def _cell_work(payload):
        # (shard, records) pairs, or (shard, records, window) triples when a
        # window schedule fanned the dispatch (issue #246).
        shard_key, records = payload[0], payload[1]
        window = payload[2] if len(payload) > 2 else None
        # Only thread aoi_payload when the manifest actually carries a mask (flag
        # on); otherwise omit the kwarg entirely so the flag-off call is identical
        # to the pre-feature signature. Same posture for the window unit.
        extra = {}
        if aoi_by_shard:
            extra["aoi_payload"] = aoi_by_shard.get(int(shard_key))
        if window is not None:
            extra["window"] = window
        # Per-cell granule_workers clamp (issue #184): min(K, n_granules), so
        # a small cell doesn't spin idle reader threads; unclamped cells pass
        # the shared config through untouched. Count the RESOLVED urls — what
        # the worker actually reads — not the raw records: _resolve_urls
        # drops href-less records, so len(records) would under-clamp a
        # partially-resolvable cell (review finding, PR #187). This resolve
        # must stay in lockstep with _process_and_write's own
        # _resolve_urls(records, driver) — same inputs, so same count.
        ds = _clamped_data_source(config.data_source, len(_resolve_urls(records, driver)))
        cell_config = replace(config, data_source=ds) if ds is not None else config
        try:
            if store_layout == "hive":
                meta = process_and_write_hive(
                    shard_key,
                    _resolve_urls(records, driver),
                    grid,
                    s3_creds,
                    store_path,
                    cell_config,
                    store_kwargs=store_kwargs,
                    driver=driver,
                    handoff=handoff,
                    **extra,
                )
            else:
                meta = _process_and_write(
                    shard_key,
                    grid.block_index(int(shard_key)),
                    records,
                    grid,
                    s3_creds,
                    zarr_store,
                    cell_config,
                    driver=driver,
                    handoff=handoff,
                    **extra,
                )
            return {"shard_key": shard_key, "ok": True, "meta": meta}
        except Exception as e:
            return {"shard_key": shard_key, "ok": False, "error": e}

    executor = LocalExecutor(
        _cell_work,
        max_workers=max_workers,
        pool_factory=ThreadPoolExecutor,
    )
    executor.preflight(len(cells))

    n = len(cells)

    def _accumulate(report, i, outcome):
        # _safe_label, not shard_label: a malformed key that failed the cell
        # must not also kill the loop reporting that failure (issue #199).
        label = _safe_label(grid, outcome["shard_key"])
        if not outcome["ok"]:
            report.cells_error += 1
            logger.warning(f"  [{i}/{n}] {label}: ERROR {outcome['error']}")
            return
        meta = outcome["meta"]
        report.results.append(meta)
        if meta.get("error"):
            logger.info(f"  [{i}/{n}] {label}: {meta['error']}")
        else:
            obs = meta.get("total_obs", 0)
            report.total_obs += obs
            report.cells_with_data += 1
            if i % 10 == 0 or n <= 20:
                logger.info(f"  [{i}/{n}] {label}: {obs:,} obs")

    start_time = time.time()
    try:
        report = dispatch(
            executor,
            cells,
            retry=LOCAL_RETRY,
            accumulate=_accumulate,
        )
    finally:
        executor.shutdown()

    # Metadata consolidation is opt-in (issue #191): no zagg reader depends on the
    # consolidated blob and building it is a ~70 s serial-GET finalize tax, so
    # skip it unless output.consolidate_metadata is true.
    if get_consolidate_metadata(config):
        consolidate_metadata(zarr_store, zarr_format=3)
    # End-of-run manifest backstop (issue #252 hybrid): the manifest already
    # landed pre-dispatch above; this idempotent re-ensure (a frozen-key-
    # matching manifest is accepted — no second PUT) self-heals a root whose
    # manifest was lost mid-run, mirroring the lambda path where the init
    # write is a retries-0 Event invoke. Unlike the root coverage.moc below
    # (a regenerable cache, D9), the manifest is REQUIRED reader-facing
    # schema (D6), so a failed backstop raises.
    if store_layout == "hive":
        ensure_manifest(
            store_path,
            manifest,
            overwrite=overwrite,
            **store_kwargs,
        )
    wall_time = time.time() - start_time

    # End-of-run root coverage.moc (issue #200 phase 3; default-on for hive,
    # O9). Built from THIS run's successful completions and GET-unioned with
    # any existing root object; the local dispatcher can write the store
    # directly. Fail-open: the root MOC is a regenerable cache (D9) — a
    # failed write costs readers one walk, never a wrong answer.
    if store_layout == "hive" and get_coverage_moc(config):
        from zagg.hive import build_root_coverage, write_root_coverage
        from zagg.windows import union_time_range

        try:
            # Inside the try so the fail-open claim survives result-envelope
            # refactors (review finding, PR #208 round 3).
            ok_results = [m for m in report.results if not m.get("error")]
            done = [m["shard_key"] for m in ok_results]
            if done:
                # D15: windowed runs union the leaf stamps' ISO time ranges
                # into the root summary; unwindowed metas carry no time_range,
                # the union is None, and the envelope stays byte-identical.
                envelope = build_root_coverage(
                    done,
                    int(grid.parent_order),
                    time_range=union_time_range(*(m.get("time_range") for m in ok_results)),
                )
                write_root_coverage(store_path, envelope, **store_kwargs)
                logger.info(f"Wrote root coverage.moc ({len(envelope['ranges'])} ranges)")
        except Exception as e:
            logger.warning(f"root coverage.moc write failed (fail-open, D9): {e}")

    summary = {
        "total_cells": len(cells),
        "cells_with_data": report.cells_with_data,
        "cells_error": report.cells_error,
        "total_obs": report.total_obs,
        "wall_time_s": wall_time,
        "store_path": store_path,
        "backend": "local",
        "results": report.results,
    }
    logger.info(
        f"Done: {report.cells_with_data} cells, {report.total_obs:,} obs, {report.cells_error} errors, {wall_time:.1f}s"
    )
    return summary


def _run_lambda(
    config,
    catalog_data,
    store_path,
    child_order,
    *,
    max_cells,
    morton_cell,
    max_workers,
    overwrite,
    dry_run,
    region,
    function_name,
    output_credentials=None,
    output_endpoint_url=None,
    handoff="arrow",
    profile=False,
    max_retries=3,
    invocation="async",
    force_cold=False,
):
    """Run processing via AWS Lambda invocation.

    The fan-out -> retry -> measured-cost loop is the generic
    :func:`zagg.dispatch.dispatch`; this function owns the Lambda-specific
    setup (grid, auth, concurrency probe, template/finalize invokes) and cost
    *presentation*. The boto3 seams (``_invoke_lambda_cell`` /
    ``_invoke_lambda_setup`` / ``_invoke_lambda_finalize`` /
    ``compute_available_workers`` / ``ThreadPoolExecutor``) are referenced off
    this module so the spatial path stays byte-identical and existing tests
    that monkeypatch them continue to bind the exact objects in use.
    """
    from dataclasses import asdict

    import boto3
    from botocore.config import Config

    all_shards = list(catalog_data["shard_keys"])
    # Strict-AOI per-shard mask payload (issue #101), keyed by shard for the
    # per-cell event. Empty dict when the manifest carries no ``aoi_mask`` (flag
    # off) — the per-cell invoke then omits the ``aoi_payload`` event key, so the
    # event payload and the worker's outputs stay byte-identical.
    aoi_by_shard = _aoi_payload_map(catalog_data)
    grid_type = config.output.get("grid", {}).get("type", "healpix")
    parent_order = get_parent_order(config) if grid_type == "healpix" else None

    # Biggest work first for throughput, but only in coarse buckets so the
    # issue #197 anti-prefix-locality shuffle survives the sort.
    cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    if not morton_cell:
        cells = _lambda_dispatch_order(cells)

    # Worker count is logged after the pre-flight clamp (see
    # _log_concurrency_report); here max_workers is still the requested value.
    logger.info(f"Processing {len(cells)} of {len(all_shards)} cells (lambda)")

    if dry_run:
        return _dry_run_summary(cells, store_path)

    # Temporal fan-out (issue #246 phase 5): one work unit per (shard,
    # window); the biggest-first bucket order above survives (shard-major
    # expansion). None keeps the pairs — dispatch byte-identical.
    windowing = get_windowing(config)
    if windowing is not None:
        cells = _windowed_units(cells, windowing, (config.bounds or {}).get("temporal"))

    # Authenticate (for per-cell source reads inside the Lambda)
    s3_creds = _resolve_source_credentials(config)

    # Build grid from the run config (single source of truth); enforce the
    # shard map was built for the same grid.
    from zagg.grids import from_config

    layout = get_layout(config)
    if grid_type == "healpix" and layout == "dense":
        grid = from_config(config, populated_shards=[int(s) for s in all_shards])
    else:
        grid = from_config(config)
    _check_signature(grid, catalog_data)
    config_dict = asdict(config)

    # Build the optional output_credentials event block (write side, symmetric
    # to s3_credentials on the read side). None -> execution-role writes.
    output_creds_event = _build_output_creds_event(
        output_credentials,
        output_endpoint_url,
        region,
    )

    # Async result channel (issue #151): a per-run unique status prefix next to
    # the output store. Each worker mirrors its response envelope to
    # <prefix>/<shard_label>.json (decimal morton string for HEALPix, issue
    # #199) and the dispatch threads poll for it instead of
    # holding a synchronous invoke connection open -- GitHub-hosted runners sit
    # behind a ~4 min NAT idle timeout that severed every >250 s benchmark
    # target. The run_id keeps reruns into the same store from reading stale
    # results. ``invocation="sync"`` skips the channel (legacy RequestResponse).
    result_prefix = None
    result_box: dict = {}
    if invocation == "async":
        result_prefix = f"{store_path.rstrip('/')}.status/{uuid.uuid4().hex}"
        logger.info(f"Async worker results at {result_prefix}")

    # The dispatch lambda_client is built inside preflight() (once the probe
    # has clamped the worker count, which sizes its connection pool), so the
    # per-cell / finalize closures read it from this holder rather than closing
    # over a not-yet-built name.
    session = boto3.Session()
    state: dict = {}

    def _preflight(n):
        # Pre-flight concurrency probe: clamp workers to what local file
        # descriptors and account-wide Lambda concurrency can sustain, so we
        # don't silently drop cells (FD exhaustion) or saturate the account
        # pool (#28). Probe with a lightweight session; the dispatch client is
        # sized to the clamped count. Kept behind the Executor.preflight() seam
        # (#63) -- concurrency.py stays a helper module called from here.
        probe_lambda = session.client("lambda", region_name=region)
        cloudwatch_client = session.client("cloudwatch", region_name=region)
        clamped, concurrency_report = compute_available_workers(
            max_workers,
            probe_lambda,
            cloudwatch_client,
            function_name,
        )
        _log_concurrency_report(concurrency_report, clamped)

        # Configure the dispatch boto3 client. max_pool_connections is sized to
        # the clamped worker count so connections cannot outrun the
        # file-descriptor budget. Built here (not before) so the pool tracks
        # the probe's clamp. read_timeout must exceed the function Timeout
        # (900 s since issue #148 — the Lambda ceiling) with headroom, or a
        # shard running to the ceiling trips the client-side botocore read
        # timeout first: that matches the retryable "Read timeout" pattern, so
        # a deterministic function timeout would be re-invoked (and re-billed)
        # instead of surfacing as the Lambda's own "Task timed out" error.
        boto_config = Config(
            read_timeout=960,
            connect_timeout=10,
            retries={"max_attempts": 0},
            max_pool_connections=clamped,
        )
        state["workers"] = clamped
        state["lambda_client"] = session.client(
            "lambda",
            region_name=region,
            config=boto_config,
        )
        # Read the function Timeout once, pre-fan-out: the async poll deadline
        # is keyed to it (issue #151), and the summary reports it (#100).
        state["function_timeout_s"] = _get_function_timeout_s(state["lambda_client"], function_name)
        if force_cold:
            _force_cold_containers(state["lambda_client"], function_name)
        return PreflightReport(workers=clamped, detail=concurrency_report)

    # Per-cell invoke, bound to everything but the (shard_key, records) pair so
    # the executor submits one payload per cell. Mirrors the kwargs the old
    # inline ``executor.submit(_invoke_lambda_cell, ...)`` passed.
    def _cell_work(payload):
        # (shard, records) pairs, or (shard, records, window) triples when a
        # window schedule fanned the dispatch (issue #246).
        shard_key, records = payload[0], payload[1]
        window = payload[2] if len(payload) > 2 else None
        # Rendered once per cell: the status-object name (below) and the
        # payload-cap error message in _invoke_lambda_cell both carry it
        # (issue #199). On ASYNC runs the label becomes a path component (the
        # status key), so it must raise on a malformed key; on SYNC runs it is
        # purely cosmetic and a raise out of _cell_work would be RUN-fatal
        # (dispatch() re-raises), so fall back to the raw digits instead
        # (review finding, PR #205).
        if result_prefix is not None:
            label = shard_label(grid, shard_key)
        else:
            label = _safe_label(grid, shard_key)
        # Only thread aoi_payload when the manifest carries a mask (flag on);
        # otherwise omit the kwarg so the event payload is byte-identical to the
        # pre-feature path (issue #101). Mirrors the local runner's _cell_work.
        extra = {}
        if aoi_by_shard:
            extra["aoi_payload"] = aoi_by_shard.get(int(shard_key))
        if window is not None:
            extra["window"] = window
        # Async dispatch (issue #151): where the worker writes this shard's
        # result, how to poll for it, and how long before giving up (function
        # timeout + queue/write margin). Sync runs pass none of these, keeping
        # the invoke byte-identical to the legacy path.
        if result_prefix is not None:
            # Status objects are named by the shard label — the decimal morton
            # string for HEALPix (issue #199) — not the raw packed word.
            # Windowed units suffix the window label (mirroring the leaf
            # naming) so two windows of one shard cannot clobber each other's
            # status object (issue #246).
            key = f"{label}.json" if window is None else f"{label}_{window['label']}.json"
            extra["result_url"] = f"{result_prefix}/{key}"
            extra["result_fetch"] = _result_fetcher(
                result_box, result_prefix, output_creds_event, region, key
            )
            extra["poll_timeout_s"] = state["function_timeout_s"] + _ASYNC_POLL_MARGIN_S
        # Per-cell granule_workers clamp (issue #184): the worker reads the
        # width from the event's config, so the clamp rides a per-cell copy
        # of it; unclamped cells send the shared config_dict byte-identical.
        # Clamp on the RESOLVED url count — what the worker actually reads —
        # since _resolve_urls drops href-less records (review finding,
        # PR #187).
        granule_urls = _resolve_urls(records, "s3")
        ds = _clamped_data_source(config.data_source, len(granule_urls))
        cell_config_dict = {**config_dict, "data_source": ds} if ds is not None else config_dict
        return _invoke_lambda_cell(
            state["lambda_client"],
            grid.block_index(int(shard_key)),
            int(shard_key),
            parent_order,
            child_order,
            granule_urls,
            store_path,
            s3_creds,
            function_name=function_name,
            config_dict=cell_config_dict,
            output_creds_event=output_creds_event,
            max_retries=max_retries,
            max_workers=state["workers"],
            handoff=handoff,
            profile=profile,
            label=label,
            **extra,
        )

    # Metadata consolidation is opt-in (issue #191): nothing in zagg's read path
    # uses the consolidated blob and the finalize invoke is a ~70 s serial-GET tax,
    # so gate the invoke dispatcher-side. When off we hand the executor a no-op
    # finalize (mirroring the temporal path's ``_run_lambda_events``, which has no
    # metadata to consolidate) so no ``mode: "finalize"`` Lambda is dispatched.
    # Hive (issue #252 hybrid): finalize ALWAYS runs — its ensure_manifest
    # is the idempotent BACKSTOP for the async init-time manifest write (a
    # retries-0 Event invoke is never redelivered if lost; finalize
    # self-heals it) — so the consolidate_metadata gate stays flat-only. The
    # manifest inputs (config + dataset identity from the ShardMap metadata,
    # the same source as the local path) ride the finalize event; flat
    # finalize events stay byte-identical.
    if get_store_layout(config) == "hive":
        md = catalog_data.get("metadata") or {}
        dataset = {"short_name": md.get("short_name"), "version": md.get("version")}

        def _finalize_fn():
            return _invoke_lambda_finalize(
                state["lambda_client"],
                function_name,
                store_path,
                output_creds_event=output_creds_event,
                config_dict=config_dict,
                dataset=dataset,
                parent_order=parent_order,
                overwrite=overwrite,
            )
    elif get_consolidate_metadata(config):

        def _finalize_fn():
            return _invoke_lambda_finalize(
                state["lambda_client"],
                function_name,
                store_path,
                output_creds_event=output_creds_event,
            )
    else:

        def _finalize_fn():
            return None

    executor = LambdaExecutor(
        _cell_work,
        preflight_fn=_preflight,
        pool_factory=ThreadPoolExecutor,
        finalize_fn=_finalize_fn,
    )
    # preflight() runs the probe, builds the sized client, and sizes the pool.
    executor.preflight(len(cells))
    max_workers = state["workers"]

    # Create template via Lambda (flat only). The template write happens
    # inside the function so the orchestrator only needs
    # lambda:InvokeFunction; no direct S3 access to the output bucket is
    # required (works cleanly for cross-account callers like CryoCloud).
    # Orchestrator phase brackets (always-on; just time.time() deltas around
    # calls that already happen, so no worker probe tax -- issue #100). They
    # decompose wall time into setup invoke / fan-out / finalize invoke so
    # "where did wall time go" is answerable from the summary.
    # Hive layout (issue #252 hybrid): NO synchronous manifest-writing setup
    # invoke. First the lightweight ping — fail-fast for a stale deployment
    # plus the read-only frozen-key precheck (see _invoke_lambda_ping; kept
    # while flat exists — issue #251) — then the manifest write fires as a
    # fire-and-forget Event invoke of the existing mode="setup" hive branch
    # (~10 ms, the root-coverage dispatch precedent below): the manifest
    # typically lands within seconds of init (best-effort — the Event invoke
    # shares worker concurrency and runs retries-0, so under throttling or a
    # dropped invoke the write defers to the finalize backstop), so a reader
    # can consume completed leaves while the store builds, and finalize's
    # ensure_manifest demotes to an idempotent backstop. setup_s keeps
    # bracketing the phase (ping + Event dispatch).
    setup_start = time.time()
    if get_store_layout(config) == "hive":
        _invoke_lambda_ping(
            state["lambda_client"],
            function_name,
            store_path,
            config_dict=config_dict,
            dataset=dataset,
            parent_order=parent_order,
            overwrite=overwrite,
            output_creds_event=output_creds_event,
        )
        _invoke_lambda_setup_async(
            state["lambda_client"],
            function_name,
            store_path,
            config_dict=config_dict,
            dataset=dataset,
            parent_order=parent_order,
            overwrite=overwrite,
            output_creds_event=output_creds_event,
        )
    else:
        _invoke_lambda_setup(
            state["lambda_client"],
            function_name,
            store_path,
            parent_order=parent_order,
            child_order=child_order,
            n_parent_cells=(
                len(all_shards) if grid_type == "healpix" and layout == "dense" else None
            ),
            overwrite=overwrite,
            config_dict=config_dict,
            output_creds_event=output_creds_event,
        )
    setup_s = time.time() - setup_start

    start_time = time.time()
    n = len(cells)

    def _accumulate(report, i, result):
        error = result.get("error")
        if result.get("status_code") == 200 and not error:
            obs = result.get("body", {}).get("total_obs", 0)
            report.total_obs += obs
            report.cells_with_data += 1
        elif error not in ("No granules found", "No data after filtering"):
            report.cells_error += 1
            key = result.get("shard_key")
            # _safe_label: error reporting must not raise on the bad key itself.
            label = _safe_label(grid, key) if key is not None else key
            logger.warning(f"  [{i}/{n}] shard {label}: {error}")
        report.results.append(result)

        if i % 50 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            logger.info(f"  [{i:4d}/{n}] {rate:.1f} cells/s")

    try:
        report = dispatch(
            executor,
            cells,
            retry=LAMBDA_RETRY,
            accumulate=_accumulate,
            # _invoke_lambda_cell already re-raises FD exhaustion with ulimit
            # guidance; this is a backstop for exhaustion that surfaces outside
            # the cell body (e.g. at submit time). Other exceptions propagate.
            on_submit_error=lambda e: raise_for_fd_exhaustion(e, max_workers),
        )
    finally:
        executor.shutdown()
    fanout_s = time.time() - start_time

    # Consolidate metadata via Lambda (same rationale as setup -- avoids
    # requiring orchestrator-side S3 access).
    finalize_start = time.time()
    executor.finalize()
    finalize_s = time.time() - finalize_start
    wall_time = time.time() - start_time

    # End-of-run root coverage.moc (issue #200 phase 3; default-on for hive,
    # O9): the dispatcher cannot PUT to S3, so it builds + serializes the MOC
    # and posts ONE fire-and-forget worker invoke that GET-unions-PUTs it.
    # Transport rationale (espg-requested, plan question 3) — serialized
    # ranges IN the event vs the completion list via the status channel:
    #   - Ranges (chosen): the dispatcher already holds the completion list
    #     in memory, so building the MOC costs milliseconds, and the payload
    #     is bounded by construction — spatially coherent coverage collapses
    #     to a few-KB range list, far under Lambda's 256 KB async-invoke cap,
    #     which a raw ~50k-key completion list would break. One hop, and no
    #     read-back race against status objects still landing from retried
    #     stragglers.
    #   - Completion list via .status/ (rejected): payload size would be
    #     run-independent and the artifact replayable from durable state, but
    #     it costs the worker a LIST + N GETs, races in-flight status writes,
    #     and its replayability is already owned by the §7 sweep's
    #     authoritative rebuild — the leaves are the durable truth (D9).
    # Fail-open everywhere: a failed build/invoke logs and the run result is
    # untouched (the root MOC is a regenerable cache).
    if get_store_layout(config) == "hive" and get_coverage_moc(config):
        try:
            from zagg.hive import build_root_coverage
            from zagg.windows import union_time_range

            # Inside the try so the fail-open claim survives result-envelope
            # refactors (review finding, PR #208 round 3).
            ok_results = [
                r for r in report.results if r.get("status_code") == 200 and not r.get("error")
            ]
            done = [r["shard_key"] for r in ok_results]
            if done:
                # D15: union the windowed workers' stamped time ranges (each
                # body mirrors its leaf stamp's ISO strings); unwindowed
                # bodies carry none and the envelope stays byte-identical.
                envelope = build_root_coverage(
                    done,
                    int(parent_order),
                    time_range=union_time_range(
                        *(r.get("body", {}).get("time_range") for r in ok_results)
                    ),
                )
                # An OLD deployment has no coverage mode: the event falls
                # through to its process handler, which returns a LOGGED 400
                # (missing shard_key/granule_urls...) — no writes, no result
                # mirror, and no async redelivery (a returned 400 is a
                # successful invocation to Lambda's Event retry machinery).
                # Harmless under D9, but the CloudWatch line is an ERROR, not
                # silence — mirroring the PR #205 deploy-ordering note.
                logger.info(
                    f"Dispatching root coverage.moc write ({len(envelope['ranges'])} "
                    f"ranges, fire-and-forget) — requires a redeployed function; an "
                    f"older deployment 400s mode=coverage in its process handler "
                    f"(logged, no writes, no retry — harmless under D9)"
                )
                _invoke_lambda_coverage(
                    state["lambda_client"],
                    function_name,
                    store_path,
                    envelope,
                    output_creds_event=output_creds_event,
                )
        except Exception as e:
            logger.warning(f"root coverage.moc dispatch failed (fail-open, D9): {e}")

    # Cost estimate: arm64 pricing = $0.0000133334/GB-second. Compute gb_seconds
    # and cost *once* over the summed Lambda time (the report carries only the
    # accumulated compute_time_s) so the arithmetic order -- and thus the last
    # ULP of estimated_cost_usd -- stays byte-identical to the pre-refactor path
    # (summing per-cell cost_usd would diverge in FP). Runner owns presentation;
    # the per-cell CellCost.cost_usd is for the report's structured breakdown.
    total_lambda_time = report.cost.compute_time_s
    memory_gb = LAMBDA_MEMORY_GB
    gb_seconds = total_lambda_time * memory_gb
    price_per_gb_sec = LAMBDA_PRICE_PER_GB_SEC
    estimated_cost = gb_seconds * price_per_gb_sec

    # Worker-runtime distribution (issue #100). Wall time on a parallel fan-out
    # tracks the *straggler*, not the mean, so surface max / median / pstdev of
    # the billed per-cell durations plus the max's share of the function
    # Timeout -- the safety margin that flags a skewed shardmap (one fat cell
    # dominating wall time). Raw material already lives in report.results.
    function_timeout_s = state.get("function_timeout_s", _DEFAULT_FUNCTION_TIMEOUT_S)
    durations = [r["lambda_duration"] for r in report.results if r.get("lambda_duration")]
    if durations:
        worker_max_s = max(durations)
        worker_median_s = statistics.median(durations)
        worker_pstdev_s = statistics.pstdev(durations)
        worker_pct_timeout = worker_max_s / function_timeout_s if function_timeout_s else None
    else:
        worker_max_s = worker_median_s = worker_pstdev_s = worker_pct_timeout = None

    # Peak worker memory (issue #120). The Lambda handler stamps body[
    # "max_memory_mb"] (RSS high-water mark, KB->MB) on every successful
    # invocation; roll the straggler (max) across cells, matching the wall-time
    # framing. None when no worker reported it (e.g. local backend).
    worker_memory = [
        r["body"]["max_memory_mb"]
        for r in report.results
        if (r.get("body") or {}).get("max_memory_mb") is not None
    ]
    max_memory_mb = max(worker_memory) if worker_memory else None

    # Container-telemetry rollup (issue #171): cold/warm counts + the ratchet
    # view (max start-RSS per sandbox generation) from the worker envelopes.
    container_stats = _container_telemetry_summary([r.get("body") or {} for r in report.results])

    # Per-phase worker breakdown (issue #100 phase 2), only when --profile fed
    # the workers a "profile" event so they emitted body["phase_timings"]. Roll
    # the straggler (max) per phase across cells, matching the wall-time framing.
    # Off by default -> no extra summary key, so the default key set is unchanged.
    worker_phase_max = None
    if profile:
        worker_phase_max = {}
        for r in report.results:
            for phase, secs in (r.get("body", {}).get("phase_timings") or {}).items():
                worker_phase_max[phase] = max(worker_phase_max.get(phase, 0.0), secs)

    summary = {
        "total_cells": len(cells),
        "cells_with_data": report.cells_with_data,
        "cells_error": report.cells_error,
        "total_obs": report.total_obs,
        "wall_time_s": wall_time,
        "lambda_time_s": total_lambda_time,
        "gb_seconds": gb_seconds,
        "price_per_gb_sec": price_per_gb_sec,
        "estimated_cost_usd": estimated_cost,
        "setup_s": setup_s,
        "fanout_s": fanout_s,
        "finalize_s": finalize_s,
        "function_timeout_s": function_timeout_s,
        "worker_max_s": worker_max_s,
        "worker_median_s": worker_median_s,
        "worker_pstdev_s": worker_pstdev_s,
        "worker_pct_timeout": worker_pct_timeout,
        "max_memory_mb": max_memory_mb,
        "store_path": store_path,
        "backend": "lambda",
        "function_name": function_name,
        "results": report.results,
        **container_stats,
    }
    if profile:
        summary["worker_phase_max"] = worker_phase_max
    logger.info(
        f"Done: {report.cells_with_data} cells, {report.total_obs:,} obs, {report.cells_error} errors, {wall_time:.1f}s"
    )
    logger.info(
        f"Lambda compute: {total_lambda_time:.0f}s total, {gb_seconds:.0f} GB-s, ~${estimated_cost:.2f}"
    )
    if worker_max_s is not None:
        pct = f"{worker_pct_timeout:.0%}" if worker_pct_timeout is not None else "n/a"
        logger.info(
            f"Workers: max {worker_max_s:.0f}s ({pct} of {function_timeout_s:.0f}s timeout), "
            f"median {worker_median_s:.0f}s, pstdev {worker_pstdev_s:.0f}s"
        )
    if max_memory_mb is not None:
        cap_mb = memory_gb * 1024.0
        logger.info(
            f"Worker peak memory: {max_memory_mb:.0f} MB ({max_memory_mb / cap_mb:.0%} of "
            f"{cap_mb:.0f} MB cap)"
        )
    if profile and worker_phase_max:
        breakdown = ", ".join(f"{phase} {secs:.0f}s" for phase, secs in worker_phase_max.items())
        logger.info(f"Worker phases (max across cells): {breakdown}")
    _log_container_stats(container_stats)
    return summary


# Maps each canonical camelCase key to the alternate spellings we accept on
# input: boto/``~/.aws/credentials`` snake_case and STS PascalCase. Mirrors the
# read-path leniency in ``processing.process_shard``.
_OUTPUT_CRED_ALIASES = {
    "accessKeyId": ("accessKeyId", "aws_access_key_id", "AccessKeyId"),
    "secretAccessKey": ("secretAccessKey", "aws_secret_access_key", "SecretAccessKey"),
    "sessionToken": ("sessionToken", "aws_session_token", "SessionToken"),
    "region": ("region", "region_name", "Region"),
    "endpointUrl": ("endpointUrl", "endpoint_url"),
}


def normalize_output_credentials(credentials):
    """Normalize an output-credentials dict to the canonical camelCase shape.

    Accepts camelCase (``accessKeyId``), boto snake_case
    (``aws_access_key_id``), and STS PascalCase (``AccessKeyId``) spellings,
    returning a dict keyed only by the canonical camelCase names. Keys that are
    absent (or falsy under every spelling) are simply omitted -- the first
    truthy spelling wins, mirroring the ``or``-chain read-path leniency in
    ``processing.process_shard``. ``None``/empty passes through unchanged so
    callers can keep using execution-role writes.
    """
    if not credentials:
        return credentials
    normalized = {}
    for canonical, aliases in _OUTPUT_CRED_ALIASES.items():
        for alias in aliases:
            if credentials.get(alias):
                normalized[canonical] = credentials[alias]
                break
    return normalized


def _build_output_creds_event(credentials, endpoint_url, region):
    """Build the optional ``output_credentials`` event block, or None.

    Normalizes runtime credentials + non-secret endpoint/region into the
    camelCase event shape the handler expects. Accepts camelCase, snake_case,
    and STS PascalCase key conventions (see ``normalize_output_credentials``).
    Returns ``None`` when no explicit credentials are supplied (execution-role
    writes, unchanged).
    """
    if not credentials:
        return None
    credentials = normalize_output_credentials(credentials)
    missing = [k for k in ("accessKeyId", "secretAccessKey") if k not in credentials]
    if missing:
        raise ValueError(
            "output_credentials is missing required field(s): "
            f"{', '.join(missing)} (accepts camelCase, snake_case, or STS "
            "PascalCase spellings)"
        )
    block = {
        "accessKeyId": credentials["accessKeyId"],
        "secretAccessKey": credentials["secretAccessKey"],
        "region": credentials.get("region", region),
    }
    if credentials.get("sessionToken"):
        block["sessionToken"] = credentials["sessionToken"]
    endpoint = endpoint_url or credentials.get("endpointUrl")
    if endpoint:
        block["endpointUrl"] = endpoint
    return block


def _log_concurrency_report(report: ConcurrencyReport, max_workers: int) -> None:
    """Log the pre-flight concurrency probe outcome and the clamped workers."""
    if report.function_reserved is not None:
        logger.info(f"Function reserved concurrency: {report.function_reserved}")
    if report.account_limit is None:
        logger.warning(
            "Account concurrency unreadable (missing IAM?); bounding workers by "
            f"file-descriptor limit only -> {max_workers}"
        )
    else:
        logger.info(
            f"Account concurrency: limit={report.account_limit}, "
            f"current={report.current_concurrent}, padding={report.padding}, "
            f"available={report.available} -> using {max_workers} workers"
        )


# Function Timeout fallback when get_function_configuration can't be read
# (permission denied, etc.). Mirrors the CloudFormation default in
# deployment/aws/template.yaml (Timeout Default: 900, the Lambda hard
# ceiling — bumped from 720 for the 88S stress shards, issue #148).
_DEFAULT_FUNCTION_TIMEOUT_S = 900

# Async-dispatch polling (issue #151). The poll deadline is the function
# Timeout plus this margin (async queue latency + the worker's result write);
# past it the worker either timed out, was OOM-killed, or crashed before
# writing -- all deterministic, so the shard is recorded failed, not retried.
# The interval keeps a full 1700-worker fan-out to ~a few hundred S3 GETs/s.
_ASYNC_POLL_MARGIN_S = 90.0
_ASYNC_POLL_INTERVAL_S = 5.0

# Async (Event) invoke requests cap at 256 KB (vs 6 MB synchronous). Budget a
# little under it so the dispatch pre-flight fails with a remedy before
# Lambda's raw RequestEntityTooLargeException does; the realistic trigger is a
# large strict-AOI ``aoi_payload`` (issue #101).
_ASYNC_PAYLOAD_CAP_BYTES = 250 * 1024

# The result poller owns its retrying at the loop level (a fetch every
# _ASYNC_POLL_INTERVAL_S until the deadline), so its store gets a short
# per-request policy instead of the paced store-level default (issue #186):
# a 5xx during one fetch must not block for minutes and silently overrun the
# poll deadline, which is only checked between fetches.
_POLL_RETRY_CONFIG = {
    "max_retries": 2,
    "retry_timeout": timedelta(seconds=15),
    "backoff": {
        "init_backoff": timedelta(milliseconds=500),
        "max_backoff": timedelta(seconds=2),
        "base": 2,
    },
}


def _result_fetcher(box, prefix, output_creds_event, region, key):
    """Zero-arg fetch closure for one shard's async result object (#151).

    The obstore store is built lazily on the first poll and shared across all
    cells via ``box`` (a plain dict; a benign first-poll race just builds it
    twice), so runs whose dispatch is fully mocked (tests) never touch
    obstore/S3. Credential resolution mirrors the worker's
    ``_output_store_kwargs``: the explicit output-credentials block when
    supplied, else the ambient chain.
    """

    def fetch():
        store = box.get("store")
        if store is None:
            kwargs = {"region": region, "retry_config": _POLL_RETRY_CONFIG}
            if output_creds_event:
                kwargs["region"] = output_creds_event.get("region", region)
                kwargs["credentials"] = output_creds_event
                if output_creds_event.get("endpointUrl"):
                    kwargs["endpoint_url"] = output_creds_event["endpointUrl"]
            store = open_object_store(prefix, **kwargs)
            box["store"] = store
        return _fetch_result(store, key)

    return fetch


def _fetch_result(result_store, key):
    """Read one worker-written result envelope; None while it hasn't landed."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(result_store, key).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))


def _invoke_lambda_event(
    lambda_client,
    ev,
    *,
    function_name,
    config_dict,
    output_creds_event=None,
    max_retries=3,
    max_workers=None,
    result_url=None,
    result_fetch=None,
    poll_timeout_s=None,
):
    """Invoke the Lambda ``process_event`` mode for one temporal event (issue #12, Phase 8).

    The temporal twin of :func:`_invoke_lambda_cell`: the same deterministic
    no-retry rule for ``FunctionError``s (#119), the same transient client-side
    retry -- classified and backed off by the shared
    :data:`zagg.dispatch.LAMBDA_RETRY` policy, so the retryable-substring list
    lives in one place instead of a third copy -- and the same issue-151 async
    channel (``result_url`` flips the invoke to fire-and-forget and
    :func:`_poll_lambda_result` collects the worker's mirrored envelope).
    Returns the per-event result dict the dispatch accumulator folds, keyed by
    ``event_key`` where the spatial path carries ``shard_key``.

    The payload sets ``return_results`` so the worker returns its flattened
    values in the response body and skips its own tabular write (the driver
    writes once). ``output_creds_event`` is forwarded only for the async result
    mirror; ``ev["s3_credentials"]``, when present, carries per-event read
    credentials for the source datasets.
    """
    wall_start = time.time()

    event = {
        "mode": "process_event",
        "event_key": ev["event_key"],
        "event_mask_uri": ev["event_mask_uri"],
        "collection_uris": ev.get("collection_uris", {}),
        "static_uris": ev.get("static_uris", {}),
        "config": config_dict,
        # The driver collects rows and writes the single tabular object; the
        # worker returns values instead of racing a shared store_path write.
        "return_results": True,
    }
    if ev.get("s3_credentials"):
        event["s3_credentials"] = ev["s3_credentials"]
    if ev.get("input_credentials"):
        event["input_credentials"] = ev["input_credentials"]
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event

    invocation_type = "RequestResponse"
    if result_url is not None:
        event["result_url"] = result_url
        invocation_type = "Event"

    payload = json.dumps(event)
    if invocation_type == "Event" and len(payload) > _ASYNC_PAYLOAD_CAP_BYTES:
        raise ValueError(
            f"event {ev['event_key']!r} payload is {len(payload):,} bytes, over "
            f"the {_ASYNC_PAYLOAD_CAP_BYTES:,}-byte async dispatch budget (Lambda "
            'caps Event invokes at 256 KB): pass invocation="sync" for this run, '
            "or slim the event (masks/collections travel as URIs, not inline data)"
        )

    last_error = None
    for attempt in range(max_retries):
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=payload,
            )
            if result_url is not None:
                # 202 accepted -- the worker's envelope lands at result_url;
                # poll with the shared spatial machinery and re-key the result
                # to the temporal shape.
                polled = _poll_lambda_result(
                    result_fetch,
                    ev["event_key"],
                    0,
                    wall_start,
                    attempt,
                    poll_timeout_s,
                )
                polled["event_key"] = polled.pop("shard_key")
                polled.pop("granule_count", None)
                return polled

            function_error = response.get("FunctionError")
            is_timeout = False
            if function_error:
                # Deterministic for a given event (timeout / OOM / escaped
                # exception) -- never retried, mirroring the spatial rule (#119).
                error_payload = response["Payload"].read().decode("utf-8")
                if "Task timed out" in error_payload:
                    is_timeout = True
                    last_error = f"Lambda timeout: {error_payload[:100]}"
                elif "Runtime.OutOfMemory" in error_payload:
                    last_error = f"Lambda OOM: {error_payload[:100]}"
                else:
                    last_error = f"Lambda error ({function_error}): {error_payload[:100]}"

            result = json.loads(response["Payload"].read()) if not function_error else {}
            try:
                body = json.loads(result.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}

            return {
                "event_key": ev["event_key"],
                "status_code": result.get("statusCode"),
                "body": body,
                "wall_time": time.time() - wall_start,
                "lambda_duration": body.get("duration_s", 0),
                "error": last_error if function_error else body.get("error"),
                "retries": attempt,
                "timeout": is_timeout,
            }
        except Exception as e:
            raise_for_fd_exhaustion(e, max_workers)
            last_error = str(e)
            if LAMBDA_RETRY.classify(e):
                time.sleep(LAMBDA_RETRY.backoff(attempt))
            else:
                break

    return {
        "event_key": ev["event_key"],
        "status_code": None,
        "body": {},
        "wall_time": time.time() - wall_start,
        "lambda_duration": 0,
        "error": last_error,
        "retries": max_retries,
    }


def _poll_lambda_result(
    result_fetch,
    shard_key,
    granule_count,
    wall_start,
    retries,
    poll_timeout_s,
    poll_interval_s=_ASYNC_POLL_INTERVAL_S,
):
    """Poll for one async invoke's worker-written result object (issue #151).

    Returns the same result-dict shape as the synchronous path so the dispatch
    accumulator and summary are unchanged. A result missing at the deadline
    means the worker timed out, was OOM-killed, or crashed before its write --
    deterministic outcomes, so (like sync FunctionErrors, #119) it is recorded
    as the shard's failure rather than re-invoked.
    """
    if poll_timeout_s is None:
        poll_timeout_s = _DEFAULT_FUNCTION_TIMEOUT_S + _ASYNC_POLL_MARGIN_S
    deadline = wall_start + poll_timeout_s
    fetch_error = None
    while True:
        # A fetch fault (S3 blip, throttled GET) must NOT escape into the
        # invoke retry classifier -- re-dispatching a still-running shard
        # duplicates work and cost. Treat it as a miss and keep polling; a
        # *persistent* fault (e.g. missing s3:GetObject) surfaces in the
        # deadline error below instead of masquerading as a worker crash.
        try:
            result = result_fetch()
            fetch_error = None
        except Exception as e:
            fetch_error = e
            result = None
        if result is not None:
            try:
                body = json.loads(result.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            return {
                "shard_key": shard_key,
                "status_code": result.get("statusCode"),
                "body": body,
                "wall_time": time.time() - wall_start,
                "lambda_duration": body.get("duration_s", 0),
                "error": body.get("error"),
                "retries": retries,
                "timeout": False,
                "granule_count": granule_count,
            }
        if time.time() >= deadline:
            cause = (
                f"result fetch failing: {fetch_error}"
                if fetch_error is not None
                else (
                    "worker timed out, was OOM-killed, or crashed before "
                    "writing its result (check CloudWatch) -- or the deployed "
                    "worker predates result_url support: redeploy the "
                    'function, or pass invocation="sync"'
                )
            )
            return {
                "shard_key": shard_key,
                "status_code": None,
                "body": {},
                "wall_time": time.time() - wall_start,
                "lambda_duration": 0,
                "error": f"no worker result within {poll_timeout_s:.0f}s ({cause})",
                "retries": retries,
                "granule_count": granule_count,
            }
        # Sub-second jitter de-synchronizes the fan-out's poll bursts.
        time.sleep(poll_interval_s + (time.time() % 1))


def _force_cold_containers(lambda_client, function_name, *, wait_s=120, poll_interval_s=2.0):
    """Invalidate every warm sandbox before fan-out (issue #171).

    Warm containers retain process RSS across invocations (the #139/#169
    ratchet), so consecutive fleet runs inherit dirty sandboxes that OOM
    within a few generations. Any function-configuration change invalidates
    all warm sandboxes at once: merge a per-run ``ZAGG_COLD_EPOCH`` marker
    into the environment (preserving existing variables -- e.g. the issue
    #143 malloc tunables) and wait for the update to apply before the first
    invoke.

    Unlike :func:`_get_function_timeout_s`, failures raise: the caller asked
    for cold containers explicitly, so silently proceeding warm would defeat
    the run's memory isolation.

    The poll accepts only *this* update's terminal states: the API is
    eventually consistent, so a ``Successful`` read immediately after
    ``update_function_configuration`` can still describe the *prior* update
    -- acceptance additionally requires the polled environment to carry this
    run's marker. A ``ResourceConflictException`` on the update (another
    configuration change in flight -- a concurrent ``force_cold`` run or a
    deploy) retries until the deadline instead of surfacing as a
    permissions error.
    """
    token = uuid.uuid4().hex
    deadline = time.time() + wait_s
    try:
        current = lambda_client.get_function_configuration(FunctionName=function_name)
    except Exception as exc:
        raise RuntimeError(
            f"force_cold: reading {function_name} configuration failed ({exc}). The "
            "caller needs lambda:GetFunctionConfiguration and "
            "lambda:UpdateFunctionConfiguration; pass force_cold=False to dispatch "
            "onto warm containers instead."
        ) from exc
    prior = ((current.get("Environment") or {}).get("Variables") or {}).get("ZAGG_COLD_EPOCH")
    env = dict((current.get("Environment") or {}).get("Variables") or {})
    env["ZAGG_COLD_EPOCH"] = token
    while True:
        try:
            lambda_client.update_function_configuration(
                FunctionName=function_name, Environment={"Variables": env}
            )
            break
        except Exception as exc:
            response = getattr(exc, "response", None)
            code = response.get("Error", {}).get("Code", "") if isinstance(response, dict) else ""
            if code == "ResourceConflictException":
                if time.time() < deadline:
                    time.sleep(poll_interval_s)  # another config update in flight; retry
                    continue
                raise RuntimeError(
                    f"force_cold: {function_name} has had another configuration update "
                    f"in flight for the whole {wait_s}s deadline "
                    "(ResourceConflictException); retry the run, or pass "
                    "force_cold=False to dispatch onto warm containers."
                ) from exc
            raise RuntimeError(
                f"force_cold: updating {function_name} configuration failed ({exc}). The "
                "caller needs lambda:UpdateFunctionConfiguration; pass force_cold=False "
                "to dispatch onto warm containers instead."
            ) from exc
    while True:
        cfg = lambda_client.get_function_configuration(FunctionName=function_name)
        status = cfg.get("LastUpdateStatus")
        marker = ((cfg.get("Environment") or {}).get("Variables") or {}).get("ZAGG_COLD_EPOCH")
        if marker == token:
            # Only this update's states count: a Successful (or Failed) read
            # without the marker describes the prior configuration.
            if status == "Successful":
                logger.info(f"force_cold: warm sandboxes invalidated (ZAGG_COLD_EPOCH={token})")
                return
            if status == "Failed":
                raise RuntimeError(
                    f"force_cold: {function_name} configuration update failed server-side "
                    "(LastUpdateStatus=Failed); check the function state in the console"
                )
        elif marker != prior and status == "Successful":
            # Superseded: a third epoch value means a CONCURRENT update (e.g.
            # another force_cold run) was accepted after ours. Lambda
            # serializes configuration updates, so its acceptance proves ours
            # applied first (or was subsumed) -- either way every warm sandbox
            # is already invalidated, which is the outcome the caller asked
            # for. (Review finding, PR #172: without this branch the poll
            # spins to deadline and reports a failure that didn't happen.)
            logger.info(
                "force_cold: superseded by a concurrent configuration update "
                "(warm sandboxes already invalidated)"
            )
            return
        if time.time() >= deadline:
            raise RuntimeError(
                f"force_cold: {function_name} configuration update still {status!r} "
                f"after {wait_s}s; workers would reuse warm containers"
            )
        time.sleep(poll_interval_s)


def _container_telemetry_summary(bodies):
    """Aggregate worker container telemetry into additive summary fields (issue #171).

    ``bodies`` are the parsed per-unit result bodies. Workers stamp
    ``container_cold`` / ``container_generation`` / ``rss_start_mb`` into every
    envelope (the detect-and-report half of the PR #172 plan), and this rolls
    them up into the fleet-level view that makes the #169 warm-container RSS
    ratchet visible in the run summary instead of requiring CloudWatch
    forensics: how many shards ran on fresh vs reused sandboxes, and the max
    start-RSS at each sandbox generation (a healthy fleet is flat across
    generations; the ratchet climbs). All three fields are ``None`` when no
    worker reported telemetry (older deployed workers), so consumers can
    distinguish "no data" from a genuinely all-warm run.
    """
    reported = [b for b in bodies if b.get("container_generation") is not None]
    if not reported:
        return {
            "worker_cold_starts": None,
            "worker_warm_starts": None,
            "worker_rss_start_max_by_gen": None,
        }
    cold = sum(1 for b in reported if b.get("container_cold"))
    by_gen: dict = {}
    for b in reported:
        rss = b.get("rss_start_mb")
        if rss is not None:
            gen = int(b["container_generation"])
            by_gen[gen] = max(by_gen.get(gen, 0.0), float(rss))
    return {
        "worker_cold_starts": cold,
        "worker_warm_starts": len(reported) - cold,
        "worker_rss_start_max_by_gen": dict(sorted(by_gen.items())),
    }


def _log_container_stats(container_stats):
    """One summary log line for the container-telemetry rollup (issue #171)."""
    if container_stats.get("worker_cold_starts") is None:
        return
    by_gen = container_stats["worker_rss_start_max_by_gen"] or {}
    ratchet = ", ".join(f"gen{g} {mb:.0f}MB" for g, mb in by_gen.items()) or "n/a"
    logger.info(
        f"Containers: {container_stats['worker_cold_starts']} cold / "
        f"{container_stats['worker_warm_starts']} warm; max start-RSS by "
        f"generation: {ratchet}"
    )


def _get_function_timeout_s(lambda_client, function_name):
    """Read the function's configured Timeout (seconds), once.

    Used for ``worker_pct_timeout`` (issue #100). Falls back to
    ``_DEFAULT_FUNCTION_TIMEOUT_S`` (the template default) on any failure --
    permission error, missing client, or a non-integer response -- so the
    percent is exact when available and still populated otherwise.
    """
    try:
        timeout = lambda_client.get_function_configuration(FunctionName=function_name)["Timeout"]
        return int(timeout)
    except Exception:
        return _DEFAULT_FUNCTION_TIMEOUT_S


def _invoke_lambda_setup(
    lambda_client,
    function_name,
    store_path,
    *,
    parent_order,
    child_order,
    n_parent_cells,
    overwrite,
    config_dict,
    output_creds_event=None,
):
    """Invoke Lambda in setup mode to create the zarr template (flat only).

    Hive runs no longer dispatch setup SYNCHRONOUSLY (issue #252 hybrid):
    the morton_hive.json write fires as a fire-and-forget Event invoke of
    the same setup mode instead (``_invoke_lambda_setup_async``), so nothing
    but the ping runs ahead of the fan-out. The flat setup event is
    byte-identical to the pre-#199-phase-3 event, so a new dispatcher keeps
    working against old deployed functions for flat runs.
    """
    event = {
        "mode": "setup",
        "store_path": store_path,
        "parent_order": parent_order,
        "child_order": child_order,
        "n_parent_cells": n_parent_cells,
        "overwrite": overwrite,
        "config": config_dict,
    }
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
    )
    payload = response["Payload"].read().decode("utf-8")
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda setup failed: {payload}")
    result = json.loads(payload)
    if result.get("statusCode") != 200:
        raise RuntimeError(f"Lambda setup error: {result.get('body')}")


def _invoke_lambda_setup_async(
    lambda_client,
    function_name,
    store_path,
    *,
    config_dict,
    dataset=None,
    parent_order=None,
    overwrite=False,
    output_creds_event=None,
):
    """Fire-and-forget hive manifest write at init (issue #252 hybrid).

    One ``InvocationType="Event"`` invoke of the existing ``mode="setup"``
    hive branch (~10 ms of dispatcher wall, the root-coverage dispatch
    precedent), posted immediately after the ping passes: the handler runs
    ``ensure_manifest(build_manifest(...))`` in parallel with the worker
    fan-out, so ``morton_hive.json`` typically lands within seconds of init
    (best-effort: the Event invoke shares worker concurrency and runs
    retries-0 — under throttling or a dropped invoke the write defers to the
    finalize backstop) and a reader can start consuming completed leaves while
    the store builds. The event
    carries the same manifest inputs as the ping/finalize events (``config``
    + ``parent_order`` + ``dataset`` identity + ``overwrite`` — the retired
    synchronous hive setup event's shape). No response is read; a lost Event
    invoke (retries 0, issue #151 hygiene) is self-healed by finalize's
    idempotent ensure_manifest backstop — see ``_invoke_lambda_finalize``.
    """
    event = {
        "mode": "setup",
        "store_path": store_path,
        "parent_order": parent_order,
        "overwrite": overwrite,
        "config": config_dict,
    }
    if dataset is not None:
        event["dataset"] = dataset
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def _invoke_lambda_ping(
    lambda_client,
    function_name,
    store_path,
    *,
    config_dict,
    dataset=None,
    parent_order=None,
    overwrite=False,
    output_creds_event=None,
):
    """Pre-fan-out fail-fast ping for hive dispatch (issue #252).

    One lightweight RequestResponse invoke, decoupled from the manifest WRITE
    (which fires right after this ping as an async Event invoke of the setup
    mode — issue #252 hybrid). Two failure modes are caught before any worker
    is dispatched:

    - **Stale deployment:** a function that predates the issue #252 hive
      lifecycle has no ping mode, so the event falls through to its process
      handler's 400 with ZERO writes — strictly earlier and cheaper than the
      retired PR #205 layout-echo guard, which only fired after a
      pre-#199-phase-3 function had already written the flat GLOBAL template
      at the hive root. This also gates out hive-capable-but-pre-#252
      functions, which lack this precheck and finalize's manifest backstop.
    - **Incompatible existing store:** the handler runs the read-only
      ``zagg.hive.validate_manifest`` against the event's manifest inputs
      (same keys as hive setup/finalize), so a frozen-key mismatch — or an
      overwrite into a root with shard data — refuses up front (D2) instead
      of after the fan-out (PR #255 review fold). This covers sequential
      reruns; two CONCURRENT runs racing into the same fresh root both pass
      here, but with the manifest landing seconds after init (the async
      setup invoke) the loser's collision window shrinks from run-length to
      seconds. Same last-writer caveat the manifest write itself has always
      had.

    Kept while flat exists (issue #251): once flat is removed, a stale
    function just errors and the ping can be dropped.
    """
    event = {
        "mode": "ping",
        "store_path": store_path,
        "parent_order": parent_order,
        "overwrite": overwrite,
        "config": config_dict,
    }
    if dataset is not None:
        event["dataset"] = dataset
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
    )
    payload = response["Payload"].read().decode("utf-8")
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda ping failed: {payload}")
    result = json.loads(payload)
    if result.get("statusCode") != 200:
        try:
            body = json.loads(result.get("body") or "{}")
        except (TypeError, ValueError):
            body = {}
        if body.get("mode") == "ping":
            # The deployed function KNOWS ping — this is validate_manifest
            # refusing the store, not a stale deployment.
            raise RuntimeError(
                f"Lambda ping refused the store at {store_path}: "
                f"{body.get('error')!r} — the store was templated for a "
                f"different configuration; clear the store root (or pick a "
                f"new one) before dispatching this run"
            )
        raise RuntimeError(
            f"Lambda ping failed (response body {result.get('body')!r}): the "
            f"deployed function predates the issue #252 hive dispatch "
            f"lifecycle — redeploy the function before dispatching hive runs"
        )
    try:
        version = json.loads(result.get("body") or "{}").get("zagg_version")
    except (TypeError, ValueError):
        version = None
    logger.info(f"Hive preflight OK (function zagg version {version})")


def _invoke_lambda_finalize(
    lambda_client,
    function_name,
    store_path,
    output_creds_event=None,
    *,
    config_dict=None,
    dataset=None,
    parent_order=None,
    overwrite=False,
):
    """Invoke Lambda in finalize mode.

    Flat: consolidates zarr metadata — the event stays byte-identical to the
    pre-#252 one. Hive (issue #252 hybrid): the event additionally carries
    the manifest inputs (``config`` + ``parent_order`` + ``dataset`` identity
    + ``overwrite``, mirroring the hive setup event) and the worker's
    ``ensure_manifest`` acts as the idempotent BACKSTOP for the async
    init-time write — a frozen-key-matching existing manifest is accepted, no
    second PUT. The backstop is load-bearing: worker Event invokes run with
    retries 0 (template.yaml EventInvokeConfig, issue #151 hygiene), so a
    lost async init write is never redelivered — finalize self-heals it;
    symmetrically, finalize failure is no longer load-bearing for manifest
    existence. The manifest is REQUIRED reader-facing schema (D6), so a
    non-200 still raises — unlike the fail-open root coverage.moc (D9).
    """
    event = {"mode": "finalize", "store_path": store_path}
    if config_dict is not None:
        event["config"] = config_dict
        event["parent_order"] = parent_order
        event["overwrite"] = overwrite
        if dataset is not None:
            event["dataset"] = dataset
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
    )
    payload = response["Payload"].read().decode("utf-8")
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda finalize failed: {payload}")
    result = json.loads(payload)
    if result.get("statusCode") != 200:
        raise RuntimeError(f"Lambda finalize error: {result.get('body')}")


def _invoke_lambda_coverage(
    lambda_client, function_name, store_path, envelope, output_creds_event=None
):
    """Fire-and-forget root ``coverage.moc`` write (issue #200 phase 3).

    ``InvocationType="Event"``: ~10 ms of dispatcher wall clock, run-size
    independent, nothing blocks on it and no response is read — failure is
    harmless by design (the worker-side GET-union-PUT is
    ``zagg.hive.write_root_coverage``; the root MOC is a regenerable cache,
    D9). The envelope rides pre-serialized in the event — see the dispatch
    site for the transport rationale vs the status channel.
    """
    event = {"mode": "coverage", "store_path": store_path, "coverage": envelope}
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def _invoke_lambda_cell(
    lambda_client,
    chunk_idx,
    shard_key,
    parent_order,
    child_order,
    granule_urls,
    store_path,
    s3_credentials,
    *,
    function_name,
    config_dict,
    output_creds_event=None,
    max_retries=3,
    max_workers=None,
    handoff="arrow",
    profile=False,
    aoi_payload=None,
    window=None,
    result_url=None,
    result_fetch=None,
    poll_timeout_s=None,
    label=None,
):
    """Invoke Lambda for a single cell with retry logic.

    ``label`` (issue #199) is the shard's rendered external id (decimal morton
    string for HEALPix) for user-facing messages; ``None`` falls back to the
    raw ``shard_key`` digits. The event payload always carries the int.

    ``max_workers`` is used only for the file-descriptor-exhaustion message
    (#28); it does not affect dispatch. ``profile`` (issue #100) forwards a
    ``"profile": true`` event key so the worker emits ``phase_timings``; when
    False the event payload is byte-identical to the pre-profile path (no key).
    ``aoi_payload`` (issue #101) forwards the per-shard strict-AOI mask payload
    (a compact MOC for HEALPix / in-AOI cell ids for rect) under the
    ``"aoi_payload"`` event key so the worker expands the ``aoi_mask`` column;
    when ``None`` (flag off) the key is omitted and the payload stays identical.
    ``handoff`` (issue #130) forwards a ``"handoff"`` event key selecting the
    worker's carrier; the default ``"arrow"`` adds the key, while an explicit
    ``"pandas"`` omits it, keeping that event byte-identical to the pre-handoff path.
    ``result_url`` (issue #151) switches the invoke to fire-and-forget
    (``InvocationType="Event"``): the worker mirrors its response envelope to
    that object and ``result_fetch`` (a zero-arg callable returning the parsed
    envelope, or None while absent) polls for it until ``poll_timeout_s``. No
    connection sits idle while the shard runs, so long shards survive NAT idle
    timeouts (GitHub-hosted runners sever synchronous invokes at ~4 min). When
    ``None`` (legacy sync path) the invoke is byte-identical to before.
    """
    wall_start = time.time()

    event = {
        "chunk_idx": chunk_idx,
        "shard_key": shard_key,
        "parent_order": parent_order,
        "granule_urls": granule_urls,
        "store_path": store_path,
        "s3_credentials": {
            "accessKeyId": s3_credentials["accessKeyId"],
            "secretAccessKey": s3_credentials["secretAccessKey"],
            "sessionToken": s3_credentials["sessionToken"],
        },
    }
    # child_order is HEALPix-specific; only forward it when set (non-HEALPix
    # grids leave it None and the handler doesn't require it).
    if child_order is not None:
        event["child_order"] = child_order
    if config_dict is not None:
        event["config"] = config_dict
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    # Only add the key when profiling, so default runs stay byte-identical (#100).
    if profile:
        event["profile"] = True
    # Only add the AOI key when the flag is on; flag-off runs stay byte-identical
    # to the pre-feature event (issue #101).
    if aoi_payload is not None:
        event["aoi_payload"] = aoi_payload
    # Temporal window unit (issue #246): {"label", "start", "end"} with the
    # half-open bounds in dataset units, converted once at dispatch. Absent
    # (schedule none) keeps the event byte-identical to pre-windowing runs.
    if window is not None:
        event["window"] = window
    # Add the key for the arrow carrier (the default); an explicit pandas run omits
    # it, staying byte-identical to the pre-handoff path (#130).
    if handoff and handoff != "pandas":
        event["handoff"] = handoff
    # Async dispatch (issue #151): tell the worker where to mirror its response
    # envelope and fire-and-forget. Absent -> the legacy synchronous invoke.
    invocation_type = "RequestResponse"
    if result_url is not None:
        event["result_url"] = result_url
        invocation_type = "Event"

    # json.dumps is ASCII by default, so len() is the request byte size. Gate
    # async payloads against the 256 KB Event cap with a remedy, up front,
    # rather than letting every attempt fail on Lambda's raw
    # RequestEntityTooLargeException (issue #151).
    payload = json.dumps(event)
    if invocation_type == "Event" and len(payload) > _ASYNC_PAYLOAD_CAP_BYTES:
        raise ValueError(
            f"cell {label or shard_key} event payload is {len(payload):,} bytes, over the "
            f"{_ASYNC_PAYLOAD_CAP_BYTES:,}-byte async dispatch budget (Lambda caps "
            'Event invokes at 256 KB): pass invocation="sync" for this run, or '
            "shrink the per-cell payload (e.g. the strict-AOI aoi_payload)"
        )

    last_error = None
    for attempt in range(max_retries):
        try:
            # Note: LogType="Tail" is omitted because it requires CloudWatch
            # log access in the function's account, which is not granted to
            # cross-account callers. The tail data was unused anyway.
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=payload,
            )
            if result_url is not None:
                # 202 accepted -- an Event invoke returns no payload; the
                # worker's envelope lands at result_url instead. Poll for it.
                return _poll_lambda_result(
                    result_fetch,
                    shard_key,
                    len(granule_urls),
                    wall_start,
                    attempt,
                    poll_timeout_s,
                )

            function_error = response.get("FunctionError")
            is_timeout = False
            if function_error:
                # Every ``FunctionError`` on a synchronous invoke is deterministic
                # for a given shard -- a timeout, ``Runtime.OutOfMemory``, or an
                # exception that escaped the handler -- so none are retried: they
                # all return immediately, exactly as timeouts already did (#119).
                # (Transient throttle/network faults are a separate channel,
                # retried with backoff in the ``except`` block below.) The error
                # is tagged with its real mode so a benchmark records an OOM as an
                # OOM rather than masking it behind a later retry's outcome.
                error_payload = response["Payload"].read().decode("utf-8")
                if "Task timed out" in error_payload:
                    is_timeout = True
                    last_error = f"Lambda timeout: {error_payload[:100]}"
                elif "Runtime.OutOfMemory" in error_payload:
                    # ``Runtime.OutOfMemory`` is AWS's documented errorType for an
                    # OOM-killed invocation; if AWS reworded it the tag would just
                    # fall to the generic branch below (still no retry).
                    last_error = f"Lambda OOM: {error_payload[:100]}"
                else:
                    last_error = f"Lambda error ({function_error}): {error_payload[:100]}"

            result = json.loads(response["Payload"].read()) if not function_error else {}
            try:
                body = json.loads(result.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}

            return {
                "shard_key": shard_key,
                "status_code": result.get("statusCode"),
                "body": body,
                "wall_time": time.time() - wall_start,
                "lambda_duration": body.get("duration_s", 0),
                "error": last_error if function_error else body.get("error"),
                "retries": attempt,
                "timeout": is_timeout,
                "granule_count": len(granule_urls),
            }
        except Exception as e:
            # Client-side FD exhaustion is run-fatal (every subsequent cell
            # will hit it too) and was previously swallowed into a
            # status_code=None result -- a silent dropped cell. Surface it
            # loudly with ulimit guidance instead (#28).
            raise_for_fd_exhaustion(e, max_workers)
            last_error = str(e)
            retryable = [
                "TooManyRequestsException",
                "Rate exceeded",
                "Read timeout",
                "timed out",
                "UNEXPECTED_EOF",
            ]
            if any(x in last_error for x in retryable):
                time.sleep((2**attempt) + (time.time() % 1))
            else:
                break

    return {
        "shard_key": shard_key,
        "status_code": None,
        "body": {},
        "wall_time": time.time() - wall_start,
        "lambda_duration": 0,
        "error": last_error,
        "retries": max_retries,
        "granule_count": len(granule_urls),
    }
