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
import statistics
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
    get_handoff,
    get_layout,
    get_output_endpoint_url,
    get_output_region,
    get_parent_order,
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
    handoff: str | None = None,
    profile: bool = False,
    max_retries: int = 3,
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

    Returns
    -------
    dict
        Summary with keys: ``total_cells``, ``cells_with_data``,
        ``cells_error``, ``total_obs``, ``wall_time_s``, ``store_path``.
    """
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
        )
    else:
        raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")


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
        # Only thread aoi_payload when the manifest carries a mask (flag on);
        # otherwise omit the kwarg so the event payload is byte-identical to the
        # pre-feature path (issue #101). Mirrors the local runner's _cell_work.
        extra = {}
        if aoi_by_shard:
            extra["aoi_payload"] = aoi_by_shard.get(int(shard_key))
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
            max_retries=max_retries,
            max_workers=state["workers"],
            handoff=handoff,
            profile=profile,
            **extra,
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
    function_timeout_s = _get_function_timeout_s(state.get("lambda_client"), function_name)
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
# deployment/aws/template.yaml (Timeout Default: 720).
_DEFAULT_FUNCTION_TIMEOUT_S = 720


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
