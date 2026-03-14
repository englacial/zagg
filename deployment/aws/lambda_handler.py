"""
AWS Lambda handler for processing data by morton cell.

This is an AWS-specific wrapper around the cloud-agnostic processing module.

Event payload:
{
    "chunk_idx": int,
    "parent_morton": int,
    "parent_order": int,
    "child_order": int,
    "granule_urls": [str, ...],
    "store_path": str,          # e.g. "s3://bucket/prefix.zarr"
    "s3_credentials": {
        "accessKeyId": str,
        "secretAccessKey": str,
        "sessionToken": str
    },
    "config": dict (optional, pipeline config as dict)
}
"""

import json
import logging
import os
from typing import Any, Dict

# Import cloud-agnostic processing
from magg.config import load_config_from_dict
from magg.processing import process_morton_cell, write_dataframe_to_zarr
from magg.store import open_store

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function.

    Parameters
    ----------
    event : dict
        Lambda event payload with keys:
        - chunk_idx: int
        - parent_morton: int
        - parent_order: int
        - child_order: int
        - granule_urls: list of S3 URLs
        - store_path: str (e.g. "s3://bucket/prefix.zarr")
        - s3_credentials: dict with accessKeyId, secretAccessKey, sessionToken
    context : LambdaContext
        Lambda context object

    Returns
    -------
    dict
        Processing result
    """
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
                "parent_morton": event.get("parent_morton"),
                "granule_count": len(event.get("granule_urls", [])),
                "child_order": event.get("child_order"),
                "request_id": context.aws_request_id,
                "chunk_idx": event.get("chunk_idx"),
            }
        )
    )

    try:
        # Validate required parameters
        required_params = [
            "parent_morton",
            "parent_order",
            "child_order",
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

        # Process the morton cell using cloud-agnostic function
        df_out, metadata = process_morton_cell(
            parent_morton=event["parent_morton"],
            parent_order=event["parent_order"],
            child_order=event["child_order"],
            granule_urls=event["granule_urls"],
            s3_credentials=s3_creds,
            config=config,
        )

        # Write Zarr to store
        if not df_out.empty:
            store_path = event["store_path"]
            region = os.environ.get("AWS_REGION", "us-west-2")
            store = open_store(store_path, region=region)

            # Validate that Zarr template exists before writing
            child_order = event["child_order"]
            template_key = f"{child_order}/zarr.json"
            if not store.exists(template_key):
                error_msg = f"Zarr template not found at {store_path}/{template_key}"
                logger.error(error_msg)
                metadata["error"] = error_msg
                return {
                    "statusCode": 500,
                    "body": json.dumps(metadata),
                }

            logger.info(f"  Writing data to {store_path}...")

            try:
                write_dataframe_to_zarr(
                    df_out,
                    store,
                    chunk_idx=event["chunk_idx"],
                    child_order=event["child_order"],
                    parent_order=event["parent_order"],
                )
            except Exception as e:
                logger.error(f"Failed to write zarr to {store_path}: {e}")
                metadata["error"] = f"Failed to write zarr: {e}"

        # Log structured result
        logger.info(
            json.dumps(
                {
                    "event_type": "processing_complete",
                    "parent_morton": metadata["parent_morton"],
                    "cells_with_data": metadata["cells_with_data"],
                    "total_obs": metadata["total_obs"],
                    "duration_s": metadata["duration_s"],
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
                    "parent_morton": event.get("parent_morton"),
                    "request_id": context.aws_request_id,
                }
            ),
        }
