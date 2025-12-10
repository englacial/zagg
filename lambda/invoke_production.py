#!/usr/bin/env python3
"""
Production Lambda orchestrator for processing ALL Antarctic morton cells.

Usage:
    python invoke_production.py
    python invoke_production.py --dry-run  # Show what would be processed
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
from botocore.config import Config
import numpy as np
import pandas as pd
from mortie import clip2order, geo2mort, mort2geo

from orchestrator_auth import get_nsidc_s3_credentials

# Lambda pricing (us-west-2, x86)
# https://aws.amazon.com/lambda/pricing/
LAMBDA_PRICE_PER_GB_SECOND = 0.0000166667
LAMBDA_MEMORY_MB = 1024
LAMBDA_MEMORY_GB = LAMBDA_MEMORY_MB / 1024

# Path to drainage basin polygons
BASIN_POLYGON_PATH = "/home/espg/software/xagg/Ant_Grounded_DrainageSystem_Polygons.txt"


def latlon_to_xyz(lats, lons):
    """Convert lat/lon arrays to 3D unit vectors."""
    lat_rad = np.radians(lats)
    lon_rad = np.radians(lons)
    x = np.cos(lat_rad) * np.cos(lon_rad)
    y = np.cos(lat_rad) * np.sin(lon_rad)
    z = np.sin(lat_rad)
    return np.column_stack([x, y, z])


def angular_distance(vec1, vec2):
    """Compute angular distance (radians) between two unit vectors."""
    dot = np.clip(np.dot(vec1, vec2), -1.0, 1.0)
    return np.arccos(dot)


def compute_bounding_disc(lats, lons):
    """Compute bounding disc (centroid_vec, radius_radians) for lat/lon points."""
    xyz = latlon_to_xyz(lats, lons)
    centroid = xyz.mean(axis=0)
    centroid = centroid / np.linalg.norm(centroid)
    distances = np.array([angular_distance(centroid, pt) for pt in xyz])
    return centroid, distances.max()


def get_antarctic_morton_cells(order: int = 6) -> list:
    """
    Generate order-6 morton cells covering Antarctic drainage basins.

    Uses bounding disc (query_disc) for each of the 27 drainage basins,
    then combines and deduplicates.

    Parameters
    ----------
    order : int
        Morton order (default 6)

    Returns
    -------
    list
        List of morton cell indices, sorted by latitude (furthest south first)
    """
    import healpy as hp

    nside = 2 ** order

    # Read drainage basin polygons
    df = pd.read_csv(BASIN_POLYGON_PATH, names=["Lat", "Lon", "basin"], sep=r"\s+")

    all_healpix = []

    for basin_id in df['basin'].unique():
        basin_df = df[df['basin'] == basin_id]
        lats = basin_df['Lat'].values
        lons = basin_df['Lon'].values

        if len(lats) < 3:
            continue

        # Compute bounding disc
        centroid, radius = compute_bounding_disc(lats, lons)

        # Query HEALPix
        try:
            pixels = hp.query_disc(nside, centroid, radius, inclusive=True, nest=True)
            all_healpix.append(pixels)
        except Exception:
            continue

    # Combine and deduplicate
    all_pixels = np.unique(np.concatenate(all_healpix))

    # Convert to morton indices
    morton_cells = set()
    for hpix in all_pixels:
        theta, phi = hp.pix2ang(nside, hpix, nest=True)
        lat = 90 - np.degrees(theta)
        lon = np.degrees(phi)
        if lon > 180:
            lon -= 360
        m = geo2mort([lat], [lon], order=18)[0]
        m_clipped = clip2order(order, np.array([m]))[0]
        morton_cells.add(int(m_clipped))

    # Sort by latitude (furthest south first = more data = process first)
    cells_with_lat = []
    for m in morton_cells:
        lat, lon = mort2geo(np.array([m]))
        lat_val = float(np.asarray(lat).flat[0])
        cells_with_lat.append((m, lat_val))
    cells_with_lat.sort(key=lambda x: x[1])  # Ascending (most negative first)

    return [c[0] for c in cells_with_lat]


def parse_lambda_report(log_result: str) -> dict:
    """
    Parse Lambda REPORT line from logs to extract memory usage.

    Example log line:
    REPORT RequestId: xxx Duration: 1234.56 ms Billed Duration: 1235 ms Memory Size: 2048 MB Max Memory Used: 512 MB
    """
    import base64
    import re

    try:
        logs = base64.b64decode(log_result).decode('utf-8')
        # Find the REPORT line
        for line in logs.split('\n'):
            if 'REPORT' in line and 'Max Memory Used' in line:
                # Extract max memory used
                match = re.search(r'Max Memory Used:\s*(\d+)\s*MB', line)
                if match:
                    return {'max_memory_mb': int(match.group(1))}
    except Exception:
        pass
    return {}


def invoke_lambda(
    lambda_client,
    parent_morton: int,
    cycle: int,
    parent_order: int,
    child_order: int,
    s3_bucket: str,
    s3_prefix: str,
    s3_credentials: dict,
    function_name: str = "process-morton-cell",
    max_retries: int = 5
) -> dict:
    """Invoke Lambda and return result with timing. Retries on throttling."""
    wall_start = time.time()

    event = {
        "parent_morton": parent_morton,
        "cycle": cycle,
        "parent_order": parent_order,
        "child_order": child_order,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "s3_credentials": {
            "accessKeyId": s3_credentials["accessKeyId"],
            "secretAccessKey": s3_credentials["secretAccessKey"],
            "sessionToken": s3_credentials["sessionToken"]
        }
    }

    last_error = None
    for attempt in range(max_retries):
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                LogType='Tail',
                Payload=json.dumps(event)
            )

            # Check for Lambda-level errors (timeout, OOM, crash)
            function_error = response.get('FunctionError')
            is_timeout = False
            if function_error:
                error_payload = response['Payload'].read().decode('utf-8')
                if 'Task timed out' in error_payload:
                    is_timeout = True
                    last_error = f"Lambda timeout: {error_payload[:100]}"
                else:
                    last_error = f"Lambda error ({function_error}): {error_payload[:100]}"
                if not is_timeout:
                    continue

            result = json.loads(response['Payload'].read()) if not function_error else {}
            wall_time = time.time() - wall_start

            log_result = response.get('LogResult', '')
            log_info = parse_lambda_report(log_result) if log_result else {}

            try:
                body = json.loads(result.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            lambda_duration = body.get("duration_s", 0)

            return {
                "morton": parent_morton,
                "status_code": result.get("statusCode"),
                "body": body,
                "wall_time": wall_time,
                "lambda_duration": lambda_duration,
                "error": last_error if function_error else body.get("error"),
                "retries": attempt,
                "timeout": is_timeout,
                "max_memory_mb": log_info.get("max_memory_mb"),
            }
        except Exception as e:
            last_error = str(e)
            retryable = ["TooManyRequestsException", "Rate exceeded", "Read timeout", "timed out"]
            if any(x in last_error for x in retryable):
                sleep_time = (2 ** attempt) + (time.time() % 1)
                time.sleep(sleep_time)
            else:
                break

    return {
        "morton": parent_morton,
        "status_code": None,
        "body": {},
        "wall_time": time.time() - wall_start,
        "lambda_duration": 0,
        "error": last_error,
        "retries": max_retries
    }


def main():
    parser = argparse.ArgumentParser(description="Production Lambda orchestrator")
    parser.add_argument("--max-workers", type=int, default=1700, help="Max concurrent Lambda invocations")
    parser.add_argument("--max-cells", type=int, default=None, help="Limit number of cells (for testing)")
    parser.add_argument("--cycle", type=int, default=22, help="ICESat-2 cycle number")
    parser.add_argument("--parent-order", type=int, default=7, help="Parent cell order (default 7)")
    parser.add_argument("--child-order", type=int, default=12, help="Child cell order")
    parser.add_argument("--s3-bucket", default="jupyterhub-englacial-scratch-429435741471")
    parser.add_argument("--s3-prefix", default="atl06/production")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without running")
    args = parser.parse_args()

    print("=" * 70)
    print("Production Lambda Orchestrator - Full Antarctic Run")
    print("=" * 70)

    # Step 1: Get morton cells
    print(f"\n[1/5] Generating Antarctic morton cells (order {args.parent_order})...")
    all_cells = get_antarctic_morton_cells(order=args.parent_order)
    if args.max_cells:
        cells = all_cells[:args.max_cells]
        print(f"      Limited to {len(cells)} cells (of {len(all_cells)} total)")
    else:
        cells = all_cells
        print(f"      Found {len(cells)} cells to process")

    if args.dry_run:
        print("\n[DRY RUN] Would process these cells:")
        print(f"      Total: {len(cells)}")
        print(f"      First 10: {cells[:10]}")
        print(f"      Last 10: {cells[-10:]}")
        print(f"\n      Estimated max cost (if all cells run 12s):")
        max_time = len(cells) * 12  # seconds
        max_cost = max_time * LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SECOND
        print(f"      {max_time:,}s × {LAMBDA_MEMORY_GB}GB × ${LAMBDA_PRICE_PER_GB_SECOND}/GB-s = ${max_cost:.2f}")
        return

    # Step 2: Get credentials
    print("\n[2/5] Authenticating with NASA Earthdata...")
    s3_creds = get_nsidc_s3_credentials()
    print(f"      Credentials expire: {s3_creds.get('expiration', 'N/A')}")

    # Step 3: Invoke Lambdas in parallel
    print(f"\n[3/5] Invoking {len(cells)} Lambda functions (max {args.max_workers} concurrent)...")
    print(f"      Output: s3://{args.s3_bucket}/{args.s3_prefix}/")

    # Configure client with longer timeouts
    boto_config = Config(
        read_timeout=900,
        connect_timeout=10,
        retries={'max_attempts': 0}  # We handle retries ourselves
    )
    lambda_client = boto3.client('lambda', region_name='us-west-2', config=boto_config)
    results = []

    # Counters
    cells_with_data = 0
    cells_no_granules = 0
    cells_no_data = 0
    cells_error = 0
    total_obs = 0
    total_lambda_time = 0.0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                invoke_lambda,
                lambda_client,
                cell,
                args.cycle,
                args.parent_order,
                args.child_order,
                args.s3_bucket,
                args.s3_prefix,
                s3_creds
            ): cell for cell in cells
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)

            total_lambda_time += result["lambda_duration"]
            body = result["body"]
            error = result.get("error")

            # Categorize result
            if result["status_code"] == 200 and not error:
                cells_with_data += 1
                obs = body.get("total_obs", 0)
                total_obs += obs
                status = f"OK ({body.get('cells_with_data', 0)} cells, {obs:,} obs)"
            elif error == "No granules found":
                cells_no_granules += 1
                status = "empty (no granules)"
            elif error == "No data after filtering":
                cells_no_data += 1
                status = "empty (filtered)"
            else:
                cells_error += 1
                status = f"ERROR: {str(error)[:40]}"

            # Progress update every 50 cells or on errors
            if i % 50 == 0 or cells_error > 0 and i <= 10:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(cells) - i) / rate if rate > 0 else 0
                print(f"      [{i:4d}/{len(cells)}] {status} | {rate:.1f} cells/s, ETA {eta/60:.1f}m")

    total_wall_time = time.time() - start_time

    # Step 4: Calculate costs
    print(f"\n[4/5] Cost Calculation")
    print("-" * 70)
    gb_seconds = total_lambda_time * LAMBDA_MEMORY_GB
    cost = gb_seconds * LAMBDA_PRICE_PER_GB_SECOND
    print(f"      Total Lambda execution time: {total_lambda_time:,.1f}s ({total_lambda_time/3600:.2f} hours)")
    print(f"      Memory: {LAMBDA_MEMORY_MB}MB ({LAMBDA_MEMORY_GB}GB)")
    print(f"      GB-seconds: {gb_seconds:,.1f}")
    print(f"      Cost: ${cost:.4f}")

    # Step 5: Summary
    print(f"\n[5/5] Summary")
    print("=" * 70)
    print(f"      Total cells:          {len(cells)}")
    print(f"      With data:            {cells_with_data}")
    print(f"      Empty (no granules):  {cells_no_granules}")
    print(f"      Empty (filtered):     {cells_no_data}")
    print(f"      Errors:               {cells_error}")
    print(f"      Total observations:   {total_obs:,}")
    print("-" * 70)
    print(f"      Wall clock time:      {total_wall_time:.1f}s ({total_wall_time/60:.1f}m)")
    print(f"      Lambda compute time:  {total_lambda_time:,.1f}s ({total_lambda_time/60:.1f}m)")
    print(f"      Throughput:           {len(cells)/total_wall_time:.1f} cells/sec")
    print(f"      Estimated cost:       ${cost:.4f}")
    print("-" * 70)
    print(f"      Output location:      s3://{args.s3_bucket}/{args.s3_prefix}/")
    print("=" * 70)

    # Save results to JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"production_results_{timestamp}.json"
    with open(output_file, 'w') as f:
        json.dump({
            "config": vars(args),
            "summary": {
                "total_cells": len(cells),
                "cells_with_data": cells_with_data,
                "cells_no_granules": cells_no_granules,
                "cells_no_data": cells_no_data,
                "cells_error": cells_error,
                "total_obs": total_obs,
                "wall_time_s": total_wall_time,
                "lambda_time_s": total_lambda_time,
                "gb_seconds": gb_seconds,
                "estimated_cost_usd": cost
            },
            "results": results
        }, f, indent=2, default=str)
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
