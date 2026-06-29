"""
AWS Lambda handler for processing data by morton cell.

This is an AWS-specific wrapper around the cloud-agnostic processing module.

Event payload (default / process mode):
{
    "chunk_idx": int,
    "shard_key": int,           # grid-agnostic shard identifier
    "parent_order": int,        # HEALPix only (omit for other grids)
    "child_order": int,         # HEALPix only (omit for other grids)
    "granule_urls": [str, ...],
    "store_path": str,          # e.g. "s3://bucket/prefix.zarr"
    "s3_credentials": {         # creds for reading NSIDC source data
        "accessKeyId": str,
        "secretAccessKey": str,
        "sessionToken": str
    },
    "output_credentials": {     # OPTIONAL -- creds for writing the output store;
        "accessKeyId": str,     #   omit to use the execution role (in-account).
        "secretAccessKey": str, #   Supply to write an external/S3-compatible
        "sessionToken": str,    #   target (e.g. source.coop). sessionToken,
        "endpointUrl": str,     #   endpointUrl, and region are optional.
        "region": str
    },
    "config": dict (optional, pipeline config as dict)
}

Setup mode (creates the zarr template once before per-cell fan-out):
{
    "mode": "setup",
    "store_path": str,
    "parent_order": int,        # HEALPix fallback; config.output.grid wins
    "n_parent_cells": int,      # OPTIONAL -- dense layout only (populated count)
    "overwrite": bool,
    "config": dict,             # single source of truth: child_order, chunk_inner,
                                #   layout, and grid type all come from here
    "output_credentials": dict (optional, same shape as process mode),
}

Finalize mode (consolidates zarr metadata after all cells complete):
{
    "mode": "finalize",
    "store_path": str,
    "output_credentials": dict (optional, same shape as process mode),
}

Setup and finalize exist so callers without direct S3 write access to the
output bucket (e.g. cross-account JupyterHub orchestrators) can run the
full pipeline using only lambda:InvokeFunction.
"""

import json
import logging
import os
import resource
import time
from typing import Any, Dict

from zarr import open_group
from zarr.errors import GroupNotFoundError

# Import cloud-agnostic processing
from zagg.config import load_config_from_dict
from zagg.processing import (
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
    write_shard_to_zarr,
)
from zagg.processing.write import _block_index_key
from zagg.store import open_store

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _max_memory_mb() -> float:
    """Peak resident set size of this worker in MB (issue #120).

    ``ru_maxrss`` is a high-water mark over the whole process, so reading it at
    the end of the invocation captures read+index+aggregate+write. On Linux
    (the Lambda runtime) the field is in kibibytes; tracks CloudWatch's "Max
    Memory Used" closely.
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _output_store_kwargs(event: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve open_store kwargs for the output store from an event.

    Symmetric to the read side: an optional ``output_credentials`` block
    (camelCase ``accessKeyId``/``secretAccessKey``/``sessionToken``, plus
    optional ``endpointUrl``/``region``) injects explicit write credentials.
    When absent, falls back to the execution role and the AWS region env var.

    Returns
    -------
    dict
        Keyword arguments for ``open_store`` (always includes ``region``;
        ``credentials`` and ``endpoint_url`` only when supplied).

    Raises
    ------
    ValueError
        If ``output_credentials`` is present but missing required keys.
    """
    region = os.environ.get("AWS_REGION", "us-west-2")
    creds = event.get("output_credentials")
    if not creds:
        return {"region": region}
    missing = [k for k in ("accessKeyId", "secretAccessKey") if k not in creds]
    if missing:
        raise ValueError(f"output_credentials missing keys: {', '.join(missing)}")
    kwargs: Dict[str, Any] = {
        "region": creds.get("region", region),
        "credentials": creds,
    }
    if creds.get("endpointUrl"):
        kwargs["endpoint_url"] = creds["endpointUrl"]
    return kwargs


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Dispatch on event mode.

    Default ``mode`` (or no mode) runs per-cell processing. ``mode="setup"``
    creates the zarr template; ``mode="finalize"`` consolidates metadata.
    """
    mode = event.get("mode", "process")
    if mode == "setup":
        return _handle_setup(event)
    if mode == "finalize":
        return _handle_finalize(event)
    return _handle_process(event, context)


def _handle_setup(event: Dict[str, Any]) -> Dict[str, Any]:
    """Create the zarr template at ``event['store_path']``."""
    from zagg.grids import from_config

    logger.info(f"Setup mode: creating template at {event.get('store_path')}")
    try:
        config = load_config_from_dict(event["config"])
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        # Build the grid exactly as the worker does (from_config), so the
        # template's chunk structure can't drift from what workers write. The
        # old hand-built HEALPix branch dropped chunk_inner, under-chunking the
        # template at parent_order while workers wrote finer chunk_inner block
        # indices -> "block index out of bounds" (issue #99). from_config reads
        # chunk_inner + layout from the config. n_parent_cells is inert unless
        # the config selects layout: dense, where it threads through as
        # populated_shards (only its count matters for emit_template).
        populated = (
            list(range(event["n_parent_cells"]))
            if event.get("n_parent_cells") is not None
            else None
        )
        grid = from_config(
            config,
            parent_order=event.get("parent_order"),
            populated_shards=populated,
        )
        grid.emit_template(store, overwrite=event.get("overwrite", False))
        return {"statusCode": 200, "body": json.dumps({"ok": True, "mode": "setup"})}
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "setup"})}


def _handle_finalize(event: Dict[str, Any]) -> Dict[str, Any]:
    """Consolidate zarr metadata for the store at ``event['store_path']``."""
    from zarr import consolidate_metadata

    logger.info(f"Finalize mode: consolidating metadata at {event.get('store_path')}")
    try:
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        consolidate_metadata(store, zarr_format=3)
        return {"statusCode": 200, "body": json.dumps({"ok": True, "mode": "finalize"})}
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "finalize"})}


def _handle_process(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Per-cell processing handler (the original lambda_handler body)."""
    # Log the event for debugging
    logger.info("=" * 70)
    logger.info("Lambda invocation started")
    logger.info(f"Request ID: {context.aws_request_id}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Memory: {context.memory_limit_in_mb} MB")
    logger.info(f"Timeout: {context.get_remaining_time_in_millis() / 1000:.0f}s")
    logger.info("=" * 70)

    # Log structured event data
    logger.info(
        json.dumps(
            {
                "event_type": "lambda_invocation",
                "shard_key": event.get("shard_key"),
                "granule_count": len(event.get("granule_urls", [])),
                "child_order": event.get("child_order"),
                "request_id": context.aws_request_id,
                "chunk_idx": event.get("chunk_idx"),
            }
        )
    )

    try:
        # Validate required parameters. ``child_order`` is HEALPix-specific and
        # only required once the grid is known to be HEALPix (checked below);
        # ``parent_order`` is forwarded by the orchestrator for every grid (None
        # for non-HEALPix), so its key is always present.
        required_params = [
            "shard_key",
            "parent_order",
            "granule_urls",
            "store_path",
            "s3_credentials",
        ]
        missing_params = [p for p in required_params if p not in event]

        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        # Validate s3_credentials structure
        s3_creds = event["s3_credentials"]
        required_cred_keys = ["accessKeyId", "secretAccessKey", "sessionToken"]
        missing_cred_keys = [k for k in required_cred_keys if k not in s3_creds]
        if missing_cred_keys:
            error_msg = f"Missing s3_credentials keys: {', '.join(missing_cred_keys)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        # Load pipeline config if provided, otherwise use default
        config = None
        if "config" in event:
            config = load_config_from_dict(event["config"])

        # Build grid (writer needs group_path + chunk_shape; no populated_shards
        # required because the orchestrator already computed chunk_idx).
        from zagg.grids import from_config

        if config is None:
            from zagg.config import default_config

            config = default_config("atl06")

        # child_order is required for HEALPix runs (drives the leaf order); it is
        # absent/unused for non-HEALPix grids.
        grid_type = config.output.get("grid", {}).get("type", "healpix")
        if grid_type == "healpix" and "child_order" not in event:
            error_msg = "Missing required parameters: child_order"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        grid = from_config(config, parent_order=event.get("parent_order"))

        # Process the shard using cloud-agnostic function. A ``chunk_results`` sink
        # is required for K>1 grids (issue #82 phase 7): ``process_shard`` reads the
        # granules once and returns one ``(block_index, carrier, ragged)`` per finer
        # Zarr chunk through the sink. At K==1 the sink holds exactly one entry whose
        # ``block_index`` equals ``event["chunk_idx"]``, so the write is unchanged.
        from zagg.processing import process_shard

        # Opt-in per-phase timing (issue #100). When the orchestrator forwards
        # ``profile``, ``process_shard`` fills ``metadata["phase_timings"]`` with
        # read/index/aggregate deltas; the write phase runs here, so we bracket it
        # below and merge it in. Default (no key) leaves the worker path unchanged.
        profile = event.get("profile", False)
        chunk_results: list = []
        _df_out, metadata = process_shard(
            grid,
            event["shard_key"],
            event["granule_urls"],
            s3_credentials=s3_creds,
            config=config,
            chunk_results=chunk_results,
            profile=profile,
        )

        # Write Zarr to store: one dense region per chunk plus its ragged (CSR)
        # companion. Mirrors the local runner's K>1 write loop (``_process_and_write``).
        _write_t0 = time.time() if profile else None
        if chunk_results:
            store_path = event["store_path"]
            store = open_store(store_path, **_output_store_kwargs(event))

            # Validate that the Zarr template exists before writing. ``store`` is a
            # zarr v3 ``Store`` whose ``exists()`` is async, so open the group via
            # the high-level sync API and catch the missing-node error instead
            # (issue #118), in the same open-and-catch spirit as
            # ``readers/tdigest_tensor.py``. ``GroupNotFoundError`` is raised
            # identically on the LocalStore and obstore-backed (S3) paths; a
            # present-but-wrong-type node surfaces as a real error, not "missing".
            try:
                open_group(store, path=grid.group_path, mode="r", zarr_format=3)
            except GroupNotFoundError:
                error_msg = f"Zarr template not found at {store_path}/{grid.group_path}"
                logger.error(error_msg)
                metadata["error"] = error_msg
                return {
                    "statusCode": 500,
                    "body": json.dumps(metadata),
                }

            logger.info(f"  Writing data to {store_path}...")

            single_chunk = len(chunk_results) == 1
            shard_key = event["shard_key"]
            try:
                # Sharded output (issue #108): bundle the shard's K inner chunks into
                # one ShardingCodec shard object — write the whole shard in one block
                # selection per dense array (mirrors the local runner). A per-inner-
                # chunk loop would read-modify-write the same shard object.
                if getattr(grid, "sharded", False):
                    write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=int(shard_key))
                else:
                    for block_index, carrier, ragged in chunk_results:
                        # write_dataframe_to_zarr no-ops on an empty carrier, so no
                        # per-chunk emptiness check is needed. Use each chunk's own
                        # block_index (from iter_chunks), not event["chunk_idx"].
                        write_dataframe_to_zarr(
                            carrier,
                            store,
                            grid=grid,
                            chunk_idx=block_index,
                        )
                        # Persist this chunk's ragged (CSR) fields (issue #48). At K==1
                        # the chunk IS the shard, so the CSR subgroup is keyed by
                        # ``shard_key`` (cell-resolution contract); at K>1 each finer
                        # chunk is keyed by its own block index. No-ops when empty.
                        ragged_key = (
                            int(shard_key) if single_chunk else _block_index_key(block_index, grid)
                        )
                        write_ragged_to_zarr(
                            ragged,
                            store,
                            grid=grid,
                            shard_key=ragged_key,
                        )
                # Record the write-phase timing (issue #100) only on a clean write:
                # read/index/aggregate come from ``process_shard``; ``write`` is owned
                # here and joins the same sub-dict. Recording inside the ``try`` (and
                # after the early-return template-missing 500) keeps ``write`` absent
                # on every failure path, so the runner's per-phase max rollup never
                # folds in a time-to-failure as a real write duration. The no-data
                # path returns earlier without writing, so ``write`` stays absent too.
                if profile and "phase_timings" in metadata:
                    metadata["phase_timings"]["write"] = time.time() - _write_t0
            except Exception as e:
                logger.error(f"Failed to write zarr to {store_path}: {e}")
                metadata["error"] = f"Failed to write zarr: {e}"

        # Peak worker RSS (issue #120): captured here, after the write phase, so
        # it covers the full invocation. Threaded back via the result body so the
        # orchestrator can surface OOM-proximity without CloudWatch access.
        metadata["max_memory_mb"] = _max_memory_mb()

        # Log structured result
        logger.info(
            json.dumps(
                {
                    "event_type": "processing_complete",
                    "shard_key": metadata["shard_key"],
                    "cells_with_data": metadata["cells_with_data"],
                    "total_obs": metadata["total_obs"],
                    "duration_s": metadata["duration_s"],
                    "max_memory_mb": metadata["max_memory_mb"],
                    "error": metadata.get("error"),
                    "request_id": context.aws_request_id,
                }
            )
        )

        logger.info("=" * 70)
        logger.info("Lambda invocation completed successfully")
        logger.info("=" * 70)

        return {
            "statusCode": 200 if not metadata.get("error") else 500,
            "body": json.dumps(metadata),
        }

    except Exception as e:
        logger.error(f"Unhandled exception in Lambda handler: {e}")
        logger.exception(e)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": f"Unhandled exception: {str(e)}",
                    "shard_key": event.get("shard_key"),
                    "request_id": context.aws_request_id,
                }
            ),
        }
