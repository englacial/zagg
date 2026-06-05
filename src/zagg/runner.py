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
from concurrent.futures import ThreadPoolExecutor, as_completed

from zarr import consolidate_metadata

from zagg import registry
from zagg.auth import get_edl_token, get_nsidc_s3_credentials
from zagg.backends import LocalExecutor
from zagg.config import (
    PipelineConfig,
    get_child_order,
    get_driver,
    get_layout,
    get_pipeline_type,
    get_store_path,
)
from zagg.processing import process_shard, write_dataframe_to_zarr
from zagg.store import open_store
from zagg.temporal import process_event, specs_from_config

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
) -> dict:
    """Run the aggregation pipeline.

    Dispatches on the config's pipeline type (``spatial`` by default, or
    ``temporal``/``event``) to the matching :class:`PipelineStrategy`, which
    runs the work on the requested ``backend``. The two are orthogonal: the
    strategy decides *what* each work unit computes; the backend decides
    *where* it runs (see :mod:`zagg.backends`).

    Parameters
    ----------
    config : PipelineConfig
        Pipeline configuration (from ``load_config`` or ``default_config``).
    catalog : str, optional
        Path to granule/event catalog. Overrides ``config.catalog``.
    store : str, optional
        Output store path (local or ``s3://``). Overrides ``config.output.store``.
    backend : str
        Execution backend: ``"local"`` (in-process) or ``"lambda"``.
    driver : str, optional
        Data access driver: ``"s3"`` or ``"https"`` (spatial pipeline only).
    max_cells : int, optional
        Limit number of work units to process (for testing).
    morton_cell : str, optional
        Process a single specific morton cell (spatial pipeline only).
    max_workers : int, optional
        Max concurrent workers. Defaults to 4 (local) or 1700 (lambda).
    overwrite : bool
        Overwrite existing output template.
    dry_run : bool
        Preview what would be processed without running.
    function_name : str, optional
        Lambda function name. Defaults to env ``ZAGG_LAMBDA_FUNCTION_NAME``
        or ``"process-morton-cell"``. Only used with ``backend="lambda"``.
    region : str
        AWS region for S3 and Lambda. Default ``"us-west-2"``.

    Returns
    -------
    dict
        Run summary (keys vary by pipeline type).
    """
    strategy = get_strategy(get_pipeline_type(config))
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
    )


def _run_spatial(
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
) -> dict:
    """Spatial (point-cloud → grid) pipeline: select cells, process shards, write zarr."""
    # Resolve catalog and store
    catalog_path = catalog or config.catalog
    if not catalog_path:
        raise ValueError("No catalog specified (pass catalog= or set catalog: in config)")
    store_path = store or get_store_path(config)
    if not store_path:
        raise ValueError("No store path specified (pass store= or set output.store: in config)")

    child_order = get_child_order(config)
    _maybe_warn_dense(get_layout(config))

    # Resolve driver: kwarg > config > default
    resolved_driver = driver or get_driver(config)

    # Load catalog and determine cell count for worker capping
    catalog_data = _load_catalog(catalog_path)
    n_cells = len(_select_cells(
        catalog_data, morton_cell=morton_cell, max_cells=max_cells,
    ))

    if backend == "local":
        if max_workers is None:
            max_workers = 4
        max_workers = min(max_workers, n_cells)
        return _run_local(
            config, catalog_data, store_path, child_order,
            max_cells=max_cells, morton_cell=morton_cell,
            max_workers=max_workers, overwrite=overwrite,
            dry_run=dry_run, region=region, driver=resolved_driver,
        )
    elif backend == "lambda":
        if max_workers is None:
            max_workers = 1700
        max_workers = min(max_workers, n_cells)
        if not store_path.startswith("s3://"):
            raise ValueError(f"Lambda backend requires s3:// store path, got: {store_path}")
        if function_name is None:
            function_name = os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-morton-cell")
        return _run_lambda(
            config, catalog_data, store_path, child_order,
            max_cells=max_cells, morton_cell=morton_cell,
            max_workers=max_workers, overwrite=overwrite,
            dry_run=dry_run, region=region,
            function_name=function_name,
        )
    else:
        raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")


# ---------------------------------------------------------------------------
# Pipeline strategies (what to compute) — selected by config pipeline type.
# Each strategy delegates parallelism to a backend executor (where to run).
# ---------------------------------------------------------------------------

class PipelineStrategy:
    """Base class: a pipeline type knows how to ``run`` from a config + kwargs."""

    def run(self, config: PipelineConfig, **kwargs) -> dict:  # pragma: no cover
        raise NotImplementedError


class SpatialStrategy(PipelineStrategy):
    """Point-cloud → grid aggregation (HEALPix/rectilinear, zarr output)."""

    def run(self, config: PipelineConfig, **kwargs) -> dict:
        return _run_spatial(config, **kwargs)


class TemporalStrategy(PipelineStrategy):
    """Event/temporal aggregation (storm-style streaming → tabular output)."""

    def run(self, config: PipelineConfig, **kwargs) -> dict:
        return _run_temporal(config, **kwargs)


_STRATEGIES: dict[str, PipelineStrategy] = {
    "spatial": SpatialStrategy(),
    "temporal": TemporalStrategy(),
    "event": TemporalStrategy(),
}


def get_strategy(pipeline_type: str) -> PipelineStrategy:
    """Return the :class:`PipelineStrategy` for a pipeline type."""
    try:
        return _STRATEGIES[pipeline_type]
    except KeyError:
        raise ValueError(
            f"Unknown pipeline type {pipeline_type!r} "
            f"(expected one of {sorted(_STRATEGIES)})"
        ) from None


def _run_temporal(
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
) -> dict:
    """Temporal/event pipeline: stream each event through ``process_event``.

    Data access is delegated to a registered *reader* (``data_source.reader``)
    so the runner stays domain-agnostic. The reader plans the work units
    (event keys), supplies static data, and opens each event's mask +
    per-collection datasets; :func:`zagg.temporal.process_event` does the
    aggregation. Results are collected into one tabular row per event.
    """
    # Discover plugins (readers, catalog/credential adapters, domain funcs).
    registry.load_plugins()

    reader_name = config.data_source.get("reader")
    if not reader_name:
        raise ValueError("Temporal pipeline requires data_source.reader")
    reader = registry.get_reader(reader_name)

    catalog_path = catalog or config.catalog
    store_path = store or get_store_path(config)
    specs = specs_from_config(config)

    # Optional credential provider (e.g. GES-DISC temp S3 creds), fetched once.
    creds = None
    cred_name = config.data_source.get("credentials")
    if cred_name:
        creds = registry.get_credential_provider(cred_name).fetch(region)

    event_keys = list(
        reader.plan(config, catalog_path, max_cells=max_cells, selection=morton_cell)
    )

    if dry_run:
        return {
            "dry_run": True,
            "pipeline": "temporal",
            "total_events": len(event_keys),
            "store_path": store_path,
        }

    logger.info(
        f"Processing {len(event_keys)} events (temporal, {backend}, reader={reader_name})"
    )
    start_time = time.time()
    if backend == "local":
        rows, errors = _temporal_run_local(
            reader, event_keys, config, specs, creds, max_workers=max_workers,
        )
    elif backend == "lambda":
        rows, errors = _temporal_run_lambda(
            reader, reader_name, event_keys, config, specs, creds,
            function_name=function_name, region=region, max_workers=max_workers,
        )
    else:
        raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")
    wall_time = time.time() - start_time

    if store_path:
        from zagg.output import from_output_config
        from_output_config(config).write(rows, store_path)

    summary = {
        "pipeline": "temporal",
        "total_events": len(event_keys),
        "events_with_data": sum(1 for r in rows.values() if r),
        "events_error": errors,
        "wall_time_s": wall_time,
        "store_path": store_path,
        "backend": backend,
        "results": rows,
    }
    logger.info(f"Done: {len(rows)} events, {errors} errors, {wall_time:.1f}s")
    return summary


def _temporal_run_local(reader, event_keys, config, specs, creds, *, max_workers):
    """Run temporal events in-process via :class:`LocalExecutor`."""
    static_data = (
        reader.load_static(config, creds=creds)
        if hasattr(reader, "load_static")
        else {}
    )
    max_rt = config.data_source.get("max_resident_timesteps")

    def worker(event_key):
        event_mask, collections = reader.open_event(event_key, config, creds=creds)
        results, meta = process_event(
            event_key, event_mask, collections, specs, static_data,
            max_resident_timesteps=max_rt,
        )
        return event_key, results, meta

    outputs = LocalExecutor(max_workers=max_workers or 4).run(event_keys, worker)
    rows = {key: result for (key, result, _meta) in outputs}
    errors = sum(1 for (_k, _r, meta) in outputs if meta.get("error"))
    return rows, errors


def _temporal_run_lambda(reader, reader_name, event_keys, config, specs, creds, *,
                         function_name, region, max_workers):
    """Fan temporal events out to AWS Lambda via :func:`zagg.dispatch.dispatch_lambda`.

    The reader builds the JSON-serialisable per-event payload (e.g. granule
    URLs); the runner wraps it with the ``process_event`` mode, the config, and
    credentials. Each worker reconstructs the event from that payload inside the
    function (see ``lambda_handler`` ``mode='process_event'``).
    """
    from dataclasses import asdict

    from zagg.dispatch import dispatch_lambda

    if not hasattr(reader, "build_event"):
        raise NotImplementedError(
            f"Reader {reader_name!r} does not implement build_event(); the "
            "Lambda backend needs a JSON-serialisable per-event payload."
        )
    function_name = function_name or os.environ.get(
        "ZAGG_LAMBDA_FUNCTION_NAME", "process-event"
    )
    config_dict = asdict(config)
    events = []
    for key in event_keys:
        payload = dict(reader.build_event(key, config, creds=creds))
        payload.update({"mode": "process_event", "reader": reader_name, "config": config_dict})
        if creds is not None:
            payload["s3_credentials"] = creds
        events.append((key, payload))

    results_map, error_list = dispatch_lambda(
        events, function_name, region=region, max_workers=max_workers or 1000,
    )
    rows = {}
    for key in event_keys:
        # dispatch_lambda payload is the Lambda response envelope
        # {"statusCode", "body": "<json>"}; the results live in body.
        resp = results_map.get(key, {}).get("payload", {}) or {}
        body = resp.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except (json.JSONDecodeError, TypeError):
                body = {}
        rows[key] = (body or {}).get("results", {}) or {}
    return rows, len(error_list)


def _load_catalog(catalog_path: str) -> dict:
    """Load granule catalog from JSON file.

    Returns
    -------
    dict
        ``{"metadata": ..., "shard_keys": [...], "granules": [[...], ...]}``.
        The shard_keys + granules format is required as of PR-C; the
        legacy ``{"catalog": {str(int): [urls]}}`` format raises with
        instructions to regenerate.
    """
    with open(catalog_path) as f:
        data = json.load(f)
    if "shard_keys" in data and "granules" in data:
        return data
    if "catalog" in data:
        raise ValueError(
            f"Catalog at {catalog_path} uses the pre-PR-C format "
            f"(dict-keyed). Regenerate with `python -m zagg.catalog` to "
            f"produce the new shard_keys/granules format."
        )
    raise ValueError(
        f"Catalog at {catalog_path} missing required 'shard_keys' and "
        f"'granules' top-level keys."
    )


def _select_cells(catalog_data: dict, *, morton_cell: str | None = None,
                   max_cells: int | None = None) -> list[tuple]:
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


def _process_and_write(shard_key, chunk_idx, granule_urls, grid,
                       s3_creds, zarr_store, config, driver=None, catalog_metadata=None):
    """Process a single shard and write results to store."""
    df_out, metadata = process_shard(
        grid,
        int(shard_key),
        granule_urls,
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        catalog_metadata=catalog_metadata,
    )
    if not df_out.empty:
        write_dataframe_to_zarr(
            df_out, zarr_store,
            grid=grid,
            chunk_idx=chunk_idx,
        )
    return metadata


def _run_local(config, catalog_data, store_path, child_order, *,
               max_cells, morton_cell, max_workers, overwrite, dry_run, region,
               driver="s3"):
    """Run processing locally with ThreadPoolExecutor."""
    metadata = catalog_data["metadata"]
    all_shards = list(catalog_data["shard_keys"])

    cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    logger.info(f"Processing {len(cells)} of {len(all_shards)} cells (local, {max_workers} workers, driver={driver})")

    if dry_run:
        return _dry_run_summary(cells, store_path)

    # Authenticate based on driver
    if driver == "https":
        s3_creds = {"edl_token": get_edl_token()}
    else:
        s3_creds = get_nsidc_s3_credentials()

    # Build grid and template. For HEALPix-dense, populated_shards order
    # matches the catalog's shard_keys list (sorted at build time).
    from zagg.grids import from_config
    parent_order = metadata.get("parent_order")
    layout = get_layout(config)
    grid_type = config.output.get("grid", {}).get("type", "healpix")
    if grid_type == "healpix":
        grid = from_config(
            config,
            parent_order=parent_order,
            populated_shards=[int(s) for s in all_shards] if layout == "dense" else None,
        )
    else:
        grid = from_config(config)
    zarr_store = open_store(store_path, region=region)
    zarr_store = grid.emit_template(zarr_store, overwrite=overwrite)

    start_time = time.time()
    total_obs = 0
    cells_with_data = 0
    cells_error = 0
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_and_write,
                shard_key, grid.block_index(int(shard_key)), urls,
                grid,
                s3_creds, zarr_store, config,
                driver=driver, catalog_metadata=metadata,
            ): shard_key
            for shard_key, urls in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            shard_key = futures[future]
            try:
                meta = future.result()
                results.append(meta)
                if meta.get("error"):
                    logger.info(f"  [{i}/{len(cells)}] {shard_key}: {meta['error']}")
                else:
                    obs = meta.get("total_obs", 0)
                    total_obs += obs
                    cells_with_data += 1
                    if i % 10 == 0 or len(cells) <= 20:
                        logger.info(f"  [{i}/{len(cells)}] {shard_key}: {obs:,} obs")
            except Exception as e:
                cells_error += 1
                logger.warning(f"  [{i}/{len(cells)}] {shard_key}: ERROR {e}")

    consolidate_metadata(zarr_store, zarr_format=3)
    wall_time = time.time() - start_time

    summary = {
        "total_cells": len(cells),
        "cells_with_data": cells_with_data,
        "cells_error": cells_error,
        "total_obs": total_obs,
        "wall_time_s": wall_time,
        "store_path": store_path,
        "backend": "local",
        "results": results,
    }
    logger.info(f"Done: {cells_with_data} cells, {total_obs:,} obs, {cells_error} errors, {wall_time:.1f}s")
    return summary


def _run_lambda(config, catalog_data, store_path, child_order, *,
                max_cells, morton_cell, max_workers, overwrite, dry_run,
                region, function_name):
    """Run processing via AWS Lambda invocation."""
    from dataclasses import asdict

    import boto3
    from botocore.config import Config

    metadata = catalog_data["metadata"]
    all_shards = list(catalog_data["shard_keys"])
    parent_order = metadata.get("parent_order")

    # Sort by granule count (descending) for better throughput
    cells = _select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    if not morton_cell:
        cells.sort(key=lambda kv: len(kv[1]), reverse=True)

    logger.info(f"Processing {len(cells)} of {len(all_shards)} cells (lambda, {max_workers} workers)")

    if dry_run:
        return _dry_run_summary(cells, store_path)

    # Authenticate (for per-cell NSIDC reads inside the Lambda)
    s3_creds = get_nsidc_s3_credentials()

    from zagg.grids import from_config
    layout = get_layout(config)
    grid_type = config.output.get("grid", {}).get("type", "healpix")
    if grid_type == "healpix":
        grid = from_config(
            config,
            parent_order=parent_order,
            populated_shards=[int(s) for s in all_shards] if layout == "dense" else None,
        )
    else:
        grid = from_config(config)
    config_dict = asdict(config)

    # Configure boto3 client (created early so we can use it for setup/finalize)
    boto_config = Config(
        read_timeout=900,
        connect_timeout=10,
        retries={"max_attempts": 0},
        max_pool_connections=max_workers,
    )
    lambda_client = boto3.Session().client(
        "lambda", region_name=region, config=boto_config,
    )

    # Create template via Lambda. The template write happens inside the
    # function so the orchestrator only needs lambda:InvokeFunction; no
    # direct S3 access to the output bucket is required (works cleanly
    # for cross-account callers like CryoCloud).
    _invoke_lambda_setup(
        lambda_client, function_name, store_path,
        parent_order=parent_order, child_order=child_order,
        n_parent_cells=len(all_shards) if grid_type == "healpix" and layout == "dense" else None,
        overwrite=overwrite, config_dict=config_dict,
    )

    start_time = time.time()
    total_obs = 0
    cells_with_data = 0
    cells_error = 0
    total_lambda_time = 0.0
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _invoke_lambda_cell,
                lambda_client, grid.block_index(int(shard_key)), int(shard_key),
                parent_order, child_order,
                urls, store_path, s3_creds,
                function_name=function_name,
                config_dict=config_dict,
            ): shard_key
            for shard_key, urls in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            total_lambda_time += result.get("lambda_duration", 0)

            error = result.get("error")
            if result.get("status_code") == 200 and not error:
                obs = result.get("body", {}).get("total_obs", 0)
                total_obs += obs
                cells_with_data += 1
            elif error not in ("No granules found", "No data after filtering"):
                cells_error += 1
                logger.warning(f"  [{i}/{len(cells)}] morton {result.get('morton')}: {error}")

            if i % 50 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                logger.info(f"  [{i:4d}/{len(cells)}] {rate:.1f} cells/s")

    # Consolidate metadata via Lambda (same rationale as setup -- avoids
    # requiring orchestrator-side S3 access).
    _invoke_lambda_finalize(lambda_client, function_name, store_path)
    wall_time = time.time() - start_time

    # Cost estimate: arm64 pricing = $0.0000133334/GB-second
    memory_gb = 2.0  # Lambda memory in GB
    gb_seconds = total_lambda_time * memory_gb
    price_per_gb_sec = 0.0000133334
    estimated_cost = gb_seconds * price_per_gb_sec

    summary = {
        "total_cells": len(cells),
        "cells_with_data": cells_with_data,
        "cells_error": cells_error,
        "total_obs": total_obs,
        "wall_time_s": wall_time,
        "lambda_time_s": total_lambda_time,
        "gb_seconds": gb_seconds,
        "price_per_gb_sec": price_per_gb_sec,
        "estimated_cost_usd": estimated_cost,
        "store_path": store_path,
        "backend": "lambda",
        "function_name": function_name,
        "results": results,
    }
    logger.info(f"Done: {cells_with_data} cells, {total_obs:,} obs, {cells_error} errors, {wall_time:.1f}s")
    logger.info(f"Lambda compute: {total_lambda_time:.0f}s total, {gb_seconds:.0f} GB-s, ~${estimated_cost:.2f}")
    return summary


def _invoke_lambda_setup(lambda_client, function_name, store_path, *,
                         parent_order, child_order, n_parent_cells,
                         overwrite, config_dict):
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


def _invoke_lambda_finalize(lambda_client, function_name, store_path):
    """Invoke Lambda in finalize mode to consolidate zarr metadata."""
    event = {"mode": "finalize", "store_path": store_path}
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
    lambda_client, chunk_idx, parent_morton, parent_order, child_order,
    granule_urls, store_path, s3_credentials, *,
    function_name, config_dict, max_retries=3,
):
    """Invoke Lambda for a single cell with retry logic."""
    wall_start = time.time()

    event = {
        "chunk_idx": chunk_idx,
        "parent_morton": parent_morton,
        "parent_order": parent_order,
        "child_order": child_order,
        "granule_urls": granule_urls,
        "store_path": store_path,
        "s3_credentials": {
            "accessKeyId": s3_credentials["accessKeyId"],
            "secretAccessKey": s3_credentials["secretAccessKey"],
            "sessionToken": s3_credentials["sessionToken"],
        },
    }
    if config_dict is not None:
        event["config"] = config_dict

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
                "morton": parent_morton,
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
            last_error = str(e)
            retryable = ["TooManyRequestsException", "Rate exceeded",
                         "Read timeout", "timed out", "UNEXPECTED_EOF"]
            if any(x in last_error for x in retryable):
                time.sleep((2 ** attempt) + (time.time() % 1))
            else:
                break

    return {
        "morton": parent_morton,
        "status_code": None,
        "body": {},
        "wall_time": time.time() - wall_start,
        "lambda_duration": 0,
        "error": last_error,
        "retries": max_retries,
        "granule_count": len(granule_urls),
    }
