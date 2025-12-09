#!/usr/bin/env python3
"""
Example: Invoke a single Lambda function with orchestrator-level authentication.

Usage:
    python invoke_single_cell.py

This script demonstrates the authentication flow:
1. Authenticate with NASA Earthdata ONCE (via earthaccess)
2. Get S3 credentials for NSIDC data access
3. Pass credentials to Lambda function invocation
"""

import json
import boto3
from orchestrator_auth import get_nsidc_s3_credentials


def invoke_lambda(
    parent_morton: int,
    cycle: int,
    child_order: int,
    s3_bucket: str,
    s3_prefix: str,
    s3_credentials: dict,
    max_granules: int = None,
    function_name: str = "process-morton-cell"
) -> dict:
    """
    Invoke the Lambda function for a single morton cell.

    Parameters
    ----------
    parent_morton : int
        Morton index of parent cell (order 6)
    cycle : int
        ICESat-2 cycle number
    child_order : int
        Order of child cells for statistics
    s3_bucket : str
        S3 bucket for output zarr files
    s3_prefix : str
        S3 prefix for output zarr files
    s3_credentials : dict
        S3 credentials from get_nsidc_s3_credentials()
    max_granules : int, optional
        Maximum granules to process (for testing)
    function_name : str
        Lambda function name

    Returns
    -------
    dict
        Lambda response payload
    """
    event = {
        "parent_morton": parent_morton,
        "cycle": cycle,
        "child_order": child_order,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "s3_credentials": {
            "accessKeyId": s3_credentials["accessKeyId"],
            "secretAccessKey": s3_credentials["secretAccessKey"],
            "sessionToken": s3_credentials["sessionToken"]
        }
    }

    if max_granules is not None:
        event["max_granules"] = max_granules

    lambda_client = boto3.client('lambda', region_name='us-west-2')
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(event)
    )

    return json.loads(response['Payload'].read())


def main():
    # =========================================================================
    # CONFIGURATION - Update these values for your environment
    # =========================================================================
    S3_BUCKET = "jupyterhub-englacial-scratch-429435741471"
    S3_PREFIX = "atl06/test"
    PARENT_MORTON = -6134114  # Example Antarctic cell
    CYCLE = 22
    CHILD_ORDER = 12
    MAX_GRANULES = 5  # Limit for testing (set to None for full run)

    # =========================================================================
    # STEP 1: Authenticate with NASA Earthdata (ONCE)
    # =========================================================================
    print("=" * 60)
    print("Step 1: Authenticating with NASA Earthdata...")
    print("=" * 60)

    s3_creds = get_nsidc_s3_credentials()
    print(f"Got S3 credentials (expire: {s3_creds.get('expiration', 'N/A')})")

    # =========================================================================
    # STEP 2: Invoke Lambda function
    # =========================================================================
    print()
    print("=" * 60)
    print(f"Step 2: Invoking Lambda for morton cell {PARENT_MORTON}...")
    print("=" * 60)

    result = invoke_lambda(
        parent_morton=PARENT_MORTON,
        cycle=CYCLE,
        child_order=CHILD_ORDER,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        s3_credentials=s3_creds,
        max_granules=MAX_GRANULES
    )

    # =========================================================================
    # STEP 3: Display result
    # =========================================================================
    print()
    print("=" * 60)
    print("Result:")
    print("=" * 60)
    print(json.dumps(result, indent=2))

    # Check for errors
    if result.get('statusCode') == 200:
        body = json.loads(result.get('body', '{}'))
        if body.get('error'):
            print(f"\nProcessing error: {body['error']}")
        else:
            print(f"\nSuccess!")
            print(f"  Cells with data: {body.get('cells_with_data')}")
            print(f"  Total observations: {body.get('total_obs')}")
            print(f"  Zarr path: {body.get('zarr_path')}")
            print(f"  Duration: {body.get('duration_s', 0):.1f}s")
    else:
        print(f"\nLambda error (status {result.get('statusCode')})")


if __name__ == "__main__":
    main()
