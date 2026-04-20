"""Pipeline runner with pluggable backends.

Usage from Python (e.g., Jupyter notebook)::

    from magg import load_config, agg

    config = load_config("atl06.yaml")
    results = agg(config, catalog="catalog.json", store="./output.zarr", max_cells=5)

    # Lambda backend
    results = agg(config, catalog="catalog.json", backend="lambda")
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from zarr import consolidate_metadata

from magg.auth import get_edl_token, get_nsidc_s3_credentials
from magg.config import PipelineConfig, get_child_order, get_driver, get_store_path
from magg.processing import process_morton_cell, write_dataframe_to_zarr
from magg.schema import xdggs_zarr_template
from magg.store import open_store

logger = logging.getLogger(__name__)


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
        Lambda function name. Defaults to env ``MAGG_LAMBDA_FUNCTION_NAME``
        or ``"process-morton-cell"``. Only used with ``backend="lambda"``.
    region : str
        AWS region for S3 and Lambda. Default ``"us-west-2"``.

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

    child_order = get_child_order(config)

    # Resolve driver: kwarg > config > default
    resolved_driver = driver or get_driver(config)

    # Load catalog and determine cell count for worker capping
    catalog_data = _load_catalog(catalog_path)
    n_cells = len(_select_cells(
        catalog_data["catalog"], morton_cell=morton_cell, max_cells=max_cells,
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
            function_name = os.environ.get("MAGG_LAMBDA_FUNCTION_NAME", "process-morton-cell")
        return _run_lambda(
            config, catalog_data, store_path, child_order,
            max_cells=max_cells, morton_cell=morton_cell,
            max_workers=max_workers, overwrite=overwrite,
            dry_run=dry_run, region=region,
            function_name=function_name,
        )
    else:
        raise ValueError(f"Unknown backend: {backend!r} (expected 'local' or 'lambda')")


def _load_catalog(catalog_path: str) -> dict:
    """Load granule catalog from JSON file."""
    with open(catalog_path) as f:
        return json.load(f)


def _select_cells(catalog: dict, *, morton_cell: str | None = None,
                   max_cells: int | None = None) -> list[str]:
    """Select cells from catalog, optionally filtering."""
    all_cells = list(catalog.keys())
    if morton_cell:
        if morton_cell not in catalog:
            raise ValueError(f"Morton cell '{morton_cell}' not in catalog")
        return [morton_cell]
    if max_cells:
        return all_cells[:max_cells]
    return all_cells


def _dry_run_summary(cells: list[str], catalog: dict, store_path: str) -> dict:
    """Return summary without processing."""
    granule_counts = [len(catalog[c]) for c in cells]
    return {
        "dry_run": True,
        "total_cells": len(cells),
        "granules_per_cell_min": min(granule_counts),
        "granules_per_cell_max": max(granule_counts),
        "granules_per_cell_avg": sum(granule_counts) / len(granule_counts),
        "store_path": store_path,
    }


def _process_and_write(cell, chunk_idx, granule_urls, parent_order, child_order,
                       s3_creds, zarr_store, config, driver=None, catalog_metadata=None):
    """Process a single cell and write results to store."""
    df_out, metadata = process_morton_cell(
        parent_morton=int(cell),
        parent_order=parent_order,
        child_order=child_order,
        granule_urls=granule_urls,
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        catalog_metadata=catalog_metadata,
    )
    if not df_out.empty:
        write_dataframe_to_zarr(
            df_out, zarr_store,
            chunk_idx=chunk_idx,
            child_order=child_order,
            parent_order=parent_order,
        )
    return metadata


def _run_local(config, catalog_data, store_path, child_order, *,
               max_cells, morton_cell, max_workers, overwrite, dry_run, region,
               driver="s3"):
    """Run processing locally with ThreadPoolExecutor."""
    metadata = catalog_data["metadata"]
    catalog = catalog_data["catalog"]
    parent_order = metadata["parent_order"]
    all_cells = list(catalog.keys())

    cells = _select_cells(catalog, morton_cell=morton_cell, max_cells=max_cells)
    logger.info(f"Processing {len(cells)} of {len(all_cells)} cells (local, {max_workers} workers, driver={driver})")

    if dry_run:
        return _dry_run_summary(cells, catalog, store_path)

    # Authenticate based on driver
    if driver == "https":
        s3_creds = {"edl_token": get_edl_token()}
    else:
        s3_creds = get_nsidc_s3_credentials()

    # Open store and create template
    zarr_store = open_store(store_path, region=region)
    zarr_store = xdggs_zarr_template(
        zarr_store, parent_order, child_order,
        overwrite=overwrite,
        n_parent_cells=metadata["total_cells"],
        config=config,
    )

    cell_to_idx = {cell: idx for idx, cell in enumerate(all_cells)}

    start_time = time.time()
    total_obs = 0
    cells_with_data = 0
    cells_error = 0
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_and_write,
                cell, cell_to_idx[cell], catalog[cell],
                parent_order, child_order,
                s3_creds, zarr_store, config,
                driver=driver, catalog_metadata=metadata,
            ): cell
            for cell in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            cell = futures[future]
            try:
                meta = future.result()
                results.append(meta)
                if meta.get("error"):
                    logger.info(f"  [{i}/{len(cells)}] {cell}: {meta['error']}")
                else:
                    obs = meta.get("total_obs", 0)
                    total_obs += obs
                    cells_with_data += 1
                    if i % 10 == 0 or len(cells) <= 20:
                        logger.info(f"  [{i}/{len(cells)}] {cell}: {obs:,} obs")
            except Exception as e:
                cells_error += 1
                logger.warning(f"  [{i}/{len(cells)}] {cell}: ERROR {e}")

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
    catalog = catalog_data["catalog"]
    parent_order = metadata["parent_order"]
    all_cells = list(catalog.keys())

    # Sort by granule count (descending) for better throughput
    cells = _select_cells(catalog, morton_cell=morton_cell, max_cells=max_cells)
    if not morton_cell:
        cells.sort(key=lambda c: len(catalog[c]), reverse=True)

    logger.info(f"Processing {len(cells)} of {len(all_cells)} cells (lambda, {max_workers} workers)")

    if dry_run:
        return _dry_run_summary(cells, catalog, store_path)

    # Authenticate (for per-cell NSIDC reads inside the Lambda)
    s3_creds = get_nsidc_s3_credentials()

    cell_to_idx = {cell: idx for idx, cell in enumerate(all_cells)}
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
        n_parent_cells=metadata["total_cells"],
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
                lambda_client, cell_to_idx[cell], int(cell),
                parent_order, child_order,
                catalog[cell], store_path, s3_creds,
                function_name=function_name,
                config_dict=config_dict,
            ): cell
            for cell in cells
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
