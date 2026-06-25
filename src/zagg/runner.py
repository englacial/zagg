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
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

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
    get_driver,
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
)
from zagg.processing.write import _block_index_key
from zagg.store import open_store

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
    handoff: str = "pandas",
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
    handoff : str
        Per-cell aggregation carrier: ``"pandas"`` (default) or ``"arrow"``.
        Both produce byte-for-byte identical scalar outputs (#30); ``"arrow"``
        is opt-in for benchmarking. Only honored by the ``"local"`` backend.
    events : iterable, optional
        Temporal pipeline only (``pipeline.type: temporal``/``event``): an
        iterable of ``(event_key, event_mask, collections, static_data)`` tuples
        fed one-per-worker to :func:`zagg.temporal.process_event`. Ignored by
        the spatial path. Until the Phase-6/7 event reader + catalog land, the
        caller supplies events directly (e.g. from a notebook), which is enough
        to run a temporal config end-to-end on the local backend.

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
                handoff=handoff,
            )
        elif backend == "lambda":
            if max_workers is None:
                max_workers = 1700
            max_workers = min(max_workers, n_cells)
            if not store_path.startswith("s3://"):
                raise ValueError(f"Lambda backend requires s3:// store path, got: {store_path}")
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
            )
        else:
            raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")


class TemporalStrategy:
    """The event-streaming aggregation path (``pipeline.type: temporal``/``event``).

    Drives :func:`zagg.temporal.process_event` over the merged ``dispatch.py``
    primitives: one work unit per event, fanned out on a
    :class:`~zagg.dispatch.LocalExecutor` (the Lambda backend lands with the
    Phase-7 handler). ``specs_from_config(config)`` is resolved once and shared
    across every event. Each event is an
    ``(event_key, event_mask, collections, static_data)`` tuple supplied via the
    ``events`` argument until the Phase-6/7 reader + catalog land.
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
        events=None,
    ):
        from zagg.temporal import process_event, specs_from_config

        if backend != "local":
            raise ValueError(
                f"temporal pipeline supports only the 'local' backend today "
                f"(got {backend!r}); the Lambda handler lands in Phase 7"
            )
        if events is None:
            raise ValueError(
                "temporal pipeline requires events= (an iterable of "
                "(event_key, event_mask, collections, static_data) tuples); the "
                "event reader/catalog lands in a later phase"
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
                "backend": "local",
            }

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

        summary = {
            "total_events": len(event_list),
            "events_with_data": report.cells_with_data,
            "events_error": report.cells_error,
            "timesteps_processed": report.total_obs,
            "wall_time_s": wall_time,
            "store_path": store_path,
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
    list of (shard_key, granule_urls) tuples.
    """
    pairs = list(zip(catalog_data["shard_keys"], catalog_data["granules"]))
    if morton_cell:
        target = int(morton_cell)
        matches = [(k, urls) for k, urls in pairs if k == target]
        if not matches:
            raise ValueError(f"shard '{morton_cell}' not in catalog")
        return matches
    if max_cells:
        return pairs[:max_cells]
    return pairs


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
    shard_key, chunk_idx, records, grid, s3_creds, zarr_store, config, driver=None, handoff="pandas"
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
    chunk_results: list = []
    _df_out, metadata = process_shard(
        grid,
        int(shard_key),
        _resolve_urls(records, driver),
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        handoff=handoff,
        chunk_results=chunk_results,
    )
    single_chunk = len(chunk_results) == 1
    for block_index, carrier, ragged in chunk_results:
        # write_dataframe_to_zarr no-ops on an empty carrier (DataFrame or Arrow
        # table), so no carrier-specific emptiness check is needed here.
        write_dataframe_to_zarr(
            carrier,
            zarr_store,
            grid=grid,
            chunk_idx=block_index,
        )
        # Persist this chunk's ragged (CSR) fields — one CSR group per field per
        # chunk (issue #48). At K==1 the chunk IS the shard, so the CSR subgroup is
        # keyed by ``shard_key`` (the phase-4b cell-resolution contract); at K>1
        # each finer chunk is keyed by its own block index so the K groups stay
        # distinct. No-ops when ``ragged`` is empty.
        ragged_key = int(shard_key) if single_chunk else _block_index_key(block_index, grid)
        write_ragged_to_zarr(
            ragged,
            zarr_store,
            grid=grid,
            shard_key=ragged_key,
        )
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
    handoff="pandas",
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
        try:
            meta = _process_and_write(
                shard_key,
                grid.block_index(int(shard_key)),
                records,
                grid,
                s3_creds,
                zarr_store,
                config,
                driver=driver,
                handoff=handoff,
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
        # the probe's clamp.
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
        return PreflightReport(workers=clamped, detail=concurrency_report)

    # Per-cell invoke, bound to everything but the (shard_key, records) pair so
    # the executor submits one payload per cell. Mirrors the kwargs the old
    # inline ``executor.submit(_invoke_lambda_cell, ...)`` passed.
    def _cell_work(payload):
        shard_key, records = payload
        return _invoke_lambda_cell(
            state["lambda_client"],
            grid.block_index(int(shard_key)),
            int(shard_key),
            parent_order,
            child_order,
            _resolve_urls(records, "s3"),
            store_path,
            s3_creds,
            function_name=function_name,
            config_dict=config_dict,
            output_creds_event=output_creds_event,
            max_workers=state["workers"],
        )

    executor = LambdaExecutor(
        _cell_work,
        preflight_fn=_preflight,
        pool_factory=ThreadPoolExecutor,
        finalize_fn=lambda: _invoke_lambda_finalize(
            state["lambda_client"],
            function_name,
            store_path,
            output_creds_event=output_creds_event,
        ),
    )
    # preflight() runs the probe, builds the sized client, and sizes the pool.
    executor.preflight(len(cells))
    max_workers = state["workers"]

    # Create template via Lambda. The template write happens inside the
    # function so the orchestrator only needs lambda:InvokeFunction; no
    # direct S3 access to the output bucket is required (works cleanly
    # for cross-account callers like CryoCloud).
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

    # Consolidate metadata via Lambda (same rationale as setup -- avoids
    # requiring orchestrator-side S3 access).
    executor.finalize()
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
        "store_path": store_path,
        "backend": "lambda",
        "function_name": function_name,
        "results": report.results,
    }
    logger.info(
        f"Done: {report.cells_with_data} cells, {report.total_obs:,} obs, {report.cells_error} errors, {wall_time:.1f}s"
    )
    logger.info(
        f"Lambda compute: {total_lambda_time:.0f}s total, {gb_seconds:.0f} GB-s, ~${estimated_cost:.2f}"
    )
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
):
    """Invoke Lambda for a single cell with retry logic.

    ``max_workers`` is used only for the file-descriptor-exhaustion message
    (#28); it does not affect dispatch.
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

    last_error = None
    for attempt in range(max_retries):
        try:
            # Note: LogType="Tail" is omitted because it requires CloudWatch
            # log access in the function's account, which is not granted to
            # cross-account callers. The tail data was unused anyway.
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(event),
            )

            function_error = response.get("FunctionError")
            is_timeout = False
            if function_error:
                error_payload = response["Payload"].read().decode("utf-8")
                if "Task timed out" in error_payload:
                    is_timeout = True
                    last_error = f"Lambda timeout: {error_payload[:100]}"
                else:
                    last_error = f"Lambda error ({function_error}): {error_payload[:100]}"
                if not is_timeout:
                    continue

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
