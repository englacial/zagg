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

        # Process the shard using cloud-agnostic function. A K>1 grid needs a
        # multi-chunk sink (issue #82 phase 7): ``process_shard`` reads the granules
        # once and yields one ``(block_index, carrier, ragged)`` per finer Zarr chunk.
        # The non-sharded path streams each chunk write-then-free via a ``write_chunk``
        # callback (issue #91) so peak output memory holds ~1 chunk instead of all K;
        # the sharded path (#108) must bundle all K, so it still accumulates via
        # ``chunk_results``. At K==1 the lone chunk's ``block_index`` equals
        # ``event["chunk_idx"]`` and the write is byte-identical either way.
        from zagg.processing import process_shard

        # Opt-in per-phase timing (issue #100). When the orchestrator forwards
        # ``profile``, ``process_shard`` fills ``metadata["phase_timings"]`` with
        # read/index/aggregate deltas; the write phase runs in the callback below and
        # is accumulated into the same sub-dict. Default (no key) leaves it unchanged.
        profile = event.get("profile", False)
        # Per-cell carrier (issue #130). Absent key -> "pandas", the byte-identical
        # default worker path; "arrow" opts into the arro3-core read carrier for
        # benchmarks. (Neither imports pyarrow; pyarrow is not in the layer.)
        handoff = event.get("handoff", "pandas")
        sharded = getattr(grid, "sharded", False)
        store_path = event["store_path"]
        shard_key = event["shard_key"]
        # K==1 vs K>1 is fixed by the grid, not the chunk count (issue #91), so the
        # streaming callback can pick the ragged key without a materialized list: at
        # K==1 the chunk IS the shard (keyed by ``shard_key``); at K>1 each finer
        # chunk is keyed by its own block index.
        single_chunk = int(getattr(grid, "chunks_per_shard", 1)) == 1

        # Lazy store + one-time template check, opened on the FIRST chunk write so a
        # no-data shard (zero chunks) never touches the store, exactly as before. A
        # missing template or a failed write is RECORDED (not raised) so ``metadata``
        # from ``process_shard`` survives — the buffered path returned its 500 with
        # that metadata; folding the error in after the stream preserves that body.
        store_box: dict = {}
        write_error: dict = {}
        _write_elapsed = 0.0

        def _get_store():
            """Open + template-check once; returns the store, or None if the template
            is missing (recording the error so the write is skipped)."""
            if "store" in store_box:
                return store_box["store"]
            if write_error:
                return None
            store = open_store(store_path, **_output_store_kwargs(event))
            # Validate the Zarr template exists before writing. ``store`` is a zarr v3
            # ``Store`` whose ``exists()`` is async, so open the group via the high-level
            # sync API and catch the missing-node error instead (issue #118), in the same
            # open-and-catch spirit as ``readers/tdigest_tensor.py``.
            # ``GroupNotFoundError`` is raised identically on LocalStore and obstore (S3);
            # a present-but-wrong-type node surfaces as a real error, not "missing".
            try:
                open_group(store, path=grid.group_path, mode="r", zarr_format=3)
            except GroupNotFoundError:
                msg = f"Zarr template not found at {store_path}/{grid.group_path}"
                logger.error(msg)
                write_error["msg"] = msg
                return None
            logger.info(f"  Writing data to {store_path}...")
            store_box["store"] = store
            return store

        def _write_chunk(block_index, carrier, ragged):
            nonlocal _write_elapsed
            if write_error:
                return  # a prior chunk failed (or template missing) — skip the rest
            store = _get_store()
            if store is None:
                return  # template missing — recorded in write_error, skip the rest
            _t0 = time.time() if profile else None
            try:
                # write_dataframe_to_zarr no-ops on an empty carrier, so no per-chunk
                # emptiness check is needed. Use each chunk's own block_index.
                write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
                ragged_key = int(shard_key) if single_chunk else _block_index_key(block_index, grid)
                write_ragged_to_zarr(ragged, store, grid=grid, shard_key=ragged_key)
            except Exception as e:
                # Mirror the buffered path's ``except``: record the failure, stop
                # writing, and let the run surface a 500 after process_shard returns.
                logger.error(f"Failed to write zarr to {store_path}: {e}")
                write_error["msg"] = f"Failed to write zarr: {e}"
                return
            if profile:
                _write_elapsed += time.time() - _t0

        chunk_results: list | None = [] if sharded else None
        _df_out, metadata = process_shard(
            grid,
            shard_key,
            event["granule_urls"],
            s3_credentials=s3_creds,
            config=config,
            chunk_results=chunk_results,
            write_chunk=None if sharded else _write_chunk,
            handoff=handoff,
            profile=profile,
        )

        # Sharded output (issue #108): bundle the shard's K inner chunks into one
        # ShardingCodec shard object — one block selection per dense array (a per-
        # inner-chunk loop would read-modify-write the same shard object). This path
        # accumulated all K, so it opens + validates + writes here (same recording).
        if sharded and chunk_results:
            store = _get_store()
            if store is not None:
                _write_t0 = time.time() if profile else None
                try:
                    write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=int(shard_key))
                    if profile:
                        _write_elapsed += time.time() - _write_t0
                except Exception as e:
                    logger.error(f"Failed to write zarr to {store_path}: {e}")
                    write_error["msg"] = f"Failed to write zarr: {e}"

        # A recorded template-missing / write failure folds into ``metadata`` so the
        # response surfaces a 500 with the structured log, exactly as the buffered
        # ``except`` / early-return branches did (now carrying the worker metadata).
        if write_error:
            metadata["error"] = write_error["msg"]

        # Record the write-phase timing (issue #100): read/index/aggregate come from
        # ``process_shard``; ``write`` is the time spent in the streaming callback /
        # sharded write. Only attach it on a clean write (no ``error``) so a time-to-
        # failure is never folded in as a real write duration; the no-data path wrote
        # nothing (``_write_elapsed`` stays 0) but also has no chunks, so writing 0 is
        # harmless — gate on a populated ``phase_timings`` and no error to match the
        # old "write absent on failure / no-data" contract.
        if profile and not metadata.get("error") and "phase_timings" in metadata and store_box:
            metadata["phase_timings"]["write"] = _write_elapsed

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
