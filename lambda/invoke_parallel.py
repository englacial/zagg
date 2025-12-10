#!/usr/bin/env python3
"""
Parallel Lambda orchestrator for processing multiple morton cells.

Usage:
    python invoke_parallel.py --num-cells 100
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
import numpy as np
from mortie import clip2order, geo2mort

from orchestrator_auth import get_nsidc_s3_credentials


def get_antarctic_morton_cells(order: int = 6, num_cells: int = None) -> list:
    """
    Generate order-6 morton cells covering Antarctica (lat < -60).

    Parameters
    ----------
    order : int
        Morton order (default 6)
    num_cells : int, optional
        Limit number of cells returned

    Returns
    -------
    list
        List of morton cell indices
    """
    # Sample points across Antarctica
    lats = np.linspace(-90, -60, 150)
    lons = np.linspace(-180, 180, 150)

    morton_cells = set()
    for lat in lats:
        for lon in lons:
            m = geo2mort([lat], [lon], order=18)[0]
            m6 = clip2order(order, np.array([m]))[0]
            morton_cells.add(int(m6))

    cells = sorted(morton_cells)
    if num_cells:
        cells = cells[:num_cells]
    return cells


def invoke_lambda_async(
    lambda_client,
    parent_morton: int,
    cycle: int,
    child_order: int,
    s3_bucket: str,
    s3_prefix: str,
    s3_credentials: dict,
    function_name: str = "process-morton-cell"
) -> dict:
    """Invoke Lambda and return result with timing."""
    start = time.time()

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

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(event)
        )
        result = json.loads(response['Payload'].read())
        elapsed = time.time() - start

        return {
            "morton": parent_morton,
            "success": result.get('statusCode') == 200,
            "result": result,
            "elapsed": elapsed
        }
    except Exception as e:
        return {
            "morton": parent_morton,
            "success": False,
            "error": str(e),
            "elapsed": time.time() - start
        }


def main():
    parser = argparse.ArgumentParser(description="Parallel Lambda orchestrator")
    parser.add_argument("--num-cells", type=int, default=100, help="Number of cells to process")
    parser.add_argument("--max-workers", type=int, default=50, help="Max concurrent Lambda invocations")
    parser.add_argument("--cycle", type=int, default=22, help="ICESat-2 cycle number")
    parser.add_argument("--child-order", type=int, default=12, help="Child cell order")
    parser.add_argument("--s3-bucket", default="jupyterhub-englacial-scratch-429435741471")
    parser.add_argument("--s3-prefix", default="atl06/parallel_test")
    args = parser.parse_args()

    print("=" * 70)
    print(f"Parallel Lambda Orchestrator - {args.num_cells} cells")
    print("=" * 70)

    # Step 1: Get credentials
    print("\n[1/4] Authenticating with NASA Earthdata...")
    s3_creds = get_nsidc_s3_credentials()
    print(f"      Credentials expire: {s3_creds.get('expiration', 'N/A')}")

    # Step 2: Get morton cells
    print(f"\n[2/4] Getting {args.num_cells} Antarctic morton cells...")
    cells = get_antarctic_morton_cells(num_cells=args.num_cells)
    print(f"      Got {len(cells)} cells")

    # Step 3: Invoke Lambdas in parallel
    print(f"\n[3/4] Invoking {len(cells)} Lambda functions (max {args.max_workers} concurrent)...")

    lambda_client = boto3.client('lambda', region_name='us-west-2')
    results = []
    succeeded = 0
    failed = 0
    total_obs = 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                invoke_lambda_async,
                lambda_client,
                cell,
                args.cycle,
                args.child_order,
                args.s3_bucket,
                args.s3_prefix,
                s3_creds
            ): cell for cell in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)

            if result["success"]:
                succeeded += 1
                body = json.loads(result["result"].get("body", "{}"))
                total_obs += body.get("total_obs", 0)
                status = f"OK ({body.get('cells_with_data', 0)} cells, {body.get('total_obs', 0)} obs)"
            else:
                failed += 1
                error = result.get("error") or json.loads(result["result"].get("body", "{}")).get("error", "unknown")
                status = f"FAILED: {error[:50]}"

            print(f"      [{i:3d}/{len(cells)}] morton {result['morton']:>10d}: {status}")

    total_time = time.time() - start_time

    # Step 4: Summary
    print(f"\n[4/4] Summary")
    print("=" * 70)
    print(f"      Total cells:       {len(cells)}")
    print(f"      Succeeded:         {succeeded}")
    print(f"      Failed:            {failed}")
    print(f"      Total observations:{total_obs:,}")
    print(f"      Total time:        {total_time:.1f}s")
    print(f"      Avg per cell:      {total_time/len(cells):.2f}s")
    print(f"      Throughput:        {len(cells)/total_time:.1f} cells/sec")
    print(f"      Output prefix:     s3://{args.s3_bucket}/{args.s3_prefix}/")
    print("=" * 70)

    # Save results to JSON
    output_file = f"parallel_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump({
            "config": vars(args),
            "summary": {
                "total_cells": len(cells),
                "succeeded": succeeded,
                "failed": failed,
                "total_obs": total_obs,
                "total_time_s": total_time
            },
            "results": results
        }, f, indent=2, default=str)
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
