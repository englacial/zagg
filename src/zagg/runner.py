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
from concurrent.futures import ThreadPoolExecutor
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
    get_driver,
    get_handoff,
    get_layout,
    get_output_endpoint_url,
    get_output_region,
    get_parent_order,
    get_pipeline_type,
    get_store_path,
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
from zagg.processing import (
    process_shard,
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
    write_shard_to_zarr,
)
from zagg.processing.write import _block_index_key
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
        Lambda function name. Defaults to env ``ZAGG_LAMBDA_FUNCTION_NAME``
        or ``"process-shard"``. Only used with ``backend="lambda"``.
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
        Opt-in per-phase timing (issue #100). When ``True`` (lambda backend),
        forwards ``profile`` into each cell event so the worker emits a
        ``phase_timings`` (read/index/aggregate) sub-dict, and the run prints a
        per-phase worker breakdown. Default ``False`` leaves the worker path and
        per-cell event payload byte-identical -- no probe tax.
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
        (``<store>.status/<run_id>/<shard_key>.json``), so no synchronous
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
    strategy = _get_strategy(get_pipeline_type(config))
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
            if function_name is None:
                function_name = os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard")
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
        from zagg.temporal import process_event, specs_from_config

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

        # One work unit per event, catching its own exceptions so one bad event
        # counts as an error and the run continues -- mirrors the spatial local
        # path's tagged-envelope contract so ``_accumulate`` stays simple.
        def _event_work(payload):
            event_key, event_mask, collections, static_data = payload
            try:
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


# Strategy registry, keyed by pipeline.type (issue #12, Phase 5). ``event`` and
# ``temporal`` share the event-streaming engine; ``spatial`` is the point-cloud
# path. New pipeline kinds register here rather than adding another branch to
# ``agg``.
_STRATEGIES = {
    "spatial": SpatialStrategy,
    "temporal": TemporalStrategy,
    "event": TemporalStrategy,
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
    "collection_uris", "static_uris", "s3_credentials"?}``. The worker loads
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
    if function_name is None:
        function_name = os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard")

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
        Process a single shard, identified by stringified key.
    max_cells : int, optional
        Truncate to the first N shards.

    Returns
    -------
    list of (shard_key, granule_urls) tuples, in a deterministic shuffled
    order (seeded from the selected shard keys, so reruns see the same order).
    """
    pairs = list(zip(catalog_data["shard_keys"], catalog_data["granules"]))
    if morton_cell:
        target = int(morton_cell)
        matches = [(k, urls) for k, urls in pairs if k == target]
        if not matches:
            raise ValueError(f"shard '{morton_cell}' not in catalog")
        return matches
    if max_cells:
        pairs = pairs[:max_cells]
    # Shuffle after selection/truncation (max_cells keeps its morton-first-N
    # subset) so concurrent fan-out doesn't write morton-contiguous -- i.e.
    # byte-prefix-sharing -- S3 keys to one partition (issue #197). Seeded from
    # the selected shard keys, so a rerun or resume sees the same order.
    random.Random(",".join(str(k) for k, _ in pairs)).shuffle(pairs)
    return pairs


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
    ``block_index``) plus its ragged (CSR) companion. At K==1 ``chunk_results`` has
    exactly one entry whose ``block_index`` equals ``chunk_idx``, so the write is
    byte-for-byte the single-chunk path. ``chunk_idx`` is retained for the K==1
    callers/signature but the per-chunk block index from ``iter_chunks`` is used.
    """
    # K==1 vs K>1 is fixed by the grid, not the materialized list (issue #91): at
    # K==1 the lone chunk IS the shard so its CSR subgroup is keyed by ``shard_key``;
    # at K>1 each finer chunk is keyed by its own block index. Deriving it from the
    # grid lets the non-sharded path stream (no materialized count needed).
    single_chunk = int(getattr(grid, "chunks_per_shard", 1)) == 1

    def _write_chunk(block_index, carrier, ragged):
        # write_dataframe_to_zarr no-ops on an empty carrier (DataFrame or Arrow
        # table), so no carrier-specific emptiness check is needed here.
        write_dataframe_to_zarr(carrier, zarr_store, grid=grid, chunk_idx=block_index)
        # Persist this chunk's ragged (CSR) fields — one CSR group per field per
        # chunk (issue #48). No-ops when ``ragged`` is empty.
        ragged_key = int(shard_key) if single_chunk else _block_index_key(block_index, grid)
        write_ragged_to_zarr(ragged, zarr_store, grid=grid, shard_key=ragged_key)

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
        s3_creds = get_nsidc_s3_credentials()

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
    zarr_store = open_store(
        store_path,
        region=region,
        credentials=output_credentials,
        endpoint_url=output_endpoint_url,
    )
    zarr_store = grid.emit_template(zarr_store, overwrite=overwrite)

    # Per-cell work, catching its own exceptions so one bad cell counts as an
    # error and the run continues (the old loop's ``except`` branch). The
    # outcome is tagged in a private envelope the accumulator unpacks; on the
    # error path nothing is appended to ``results``, matching the old behavior.
    def _cell_work(payload):
        shard_key, records = payload
        # Only thread aoi_payload when the manifest actually carries a mask (flag
        # on); otherwise omit the kwarg entirely so the flag-off call is identical
        # to the pre-feature signature.
        extra = {}
        if aoi_by_shard:
            extra["aoi_payload"] = aoi_by_shard.get(int(shard_key))
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
        shard_key = outcome["shard_key"]
        if not outcome["ok"]:
            report.cells_error += 1
            logger.warning(f"  [{i}/{n}] {shard_key}: ERROR {outcome['error']}")
            return
        meta = outcome["meta"]
        report.results.append(meta)
        if meta.get("error"):
            logger.info(f"  [{i}/{n}] {shard_key}: {meta['error']}")
        else:
            obs = meta.get("total_obs", 0)
            report.total_obs += obs
            report.cells_with_data += 1
            if i % 10 == 0 or n <= 20:
                logger.info(f"  [{i}/{n}] {shard_key}: {obs:,} obs")

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
    wall_time = time.time() - start_time

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

    # Sort by granule count (descending) for better throughput
    cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    if not morton_cell:
        cells.sort(key=lambda kv: len(kv[1]), reverse=True)

    # Worker count is logged after the pre-flight clamp (see
    # _log_concurrency_report); here max_workers is still the requested value.
    logger.info(f"Processing {len(cells)} of {len(all_shards)} cells (lambda)")

    if dry_run:
        return _dry_run_summary(cells, store_path)

    # Authenticate (for per-cell NSIDC reads inside the Lambda)
    s3_creds = get_nsidc_s3_credentials()

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
    # <prefix>/<shard_key>.json and the dispatch threads poll for it instead of
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
        shard_key, records = payload
        # Only thread aoi_payload when the manifest carries a mask (flag on);
        # otherwise omit the kwarg so the event payload is byte-identical to the
        # pre-feature path (issue #101). Mirrors the local runner's _cell_work.
        extra = {}
        if aoi_by_shard:
            extra["aoi_payload"] = aoi_by_shard.get(int(shard_key))
        # Async dispatch (issue #151): where the worker writes this shard's
        # result, how to poll for it, and how long before giving up (function
        # timeout + queue/write margin). Sync runs pass none of these, keeping
        # the invoke byte-identical to the legacy path.
        if result_prefix is not None:
            key = f"{int(shard_key)}.json"
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
            **extra,
        )

    # Metadata consolidation is opt-in (issue #191): nothing in zagg's read path
    # uses the consolidated blob and the finalize invoke is a ~70 s serial-GET tax,
    # so gate the invoke dispatcher-side. When off we hand the executor a no-op
    # finalize (mirroring the temporal path's ``_run_lambda_events``, which has no
    # metadata to consolidate) so no ``mode: "finalize"`` Lambda is dispatched.
    if get_consolidate_metadata(config):

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

    # Create template via Lambda. The template write happens inside the
    # function so the orchestrator only needs lambda:InvokeFunction; no
    # direct S3 access to the output bucket is required (works cleanly
    # for cross-account callers like CryoCloud).
    # Orchestrator phase brackets (always-on; just time.time() deltas around
    # calls that already happen, so no worker probe tax -- issue #100). They
    # decompose wall time into setup invoke / fan-out / finalize invoke so
    # "where did wall time go" is answerable from the summary.
    setup_start = time.time()
    _invoke_lambda_setup(
        state["lambda_client"],
        function_name,
        store_path,
        parent_order=parent_order,
        child_order=child_order,
        n_parent_cells=len(all_shards) if grid_type == "healpix" and layout == "dense" else None,
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
            logger.warning(f"  [{i}/{n}] shard {result.get('shard_key')}: {error}")
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
    """Invoke Lambda in setup mode to create the zarr template."""
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


def _invoke_lambda_finalize(lambda_client, function_name, store_path, output_creds_event=None):
    """Invoke Lambda in finalize mode to consolidate zarr metadata."""
    event = {"mode": "finalize", "store_path": store_path}
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
    result_url=None,
    result_fetch=None,
    poll_timeout_s=None,
):
    """Invoke Lambda for a single cell with retry logic.

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
            f"cell {shard_key} event payload is {len(payload):,} bytes, over the "
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
