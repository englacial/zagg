#!/usr/bin/env python3
"""
Production Lambda orchestrator with cost reporting.

Thin wrapper around zagg.agg(backend="lambda") that adds verbose progress
output, architecture-based cost calculation, and results JSON export.

Usage:
    python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json
    python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json --max-cells 10
    python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json --dry-run
"""

import argparse
import json
import os
from datetime import datetime

import boto3

from zagg.config import default_config, get_store_path, load_config
from zagg.runner import agg

# Lambda pricing (us-west-2)
# https://aws.amazon.com/lambda/pricing/
LAMBDA_PRICE_X86 = 0.0000166667  # per GB-second
LAMBDA_PRICE_ARM = 0.0000133334  # per GB-second (20% cheaper)
LAMBDA_MEMORY_MB = 2048
LAMBDA_MEMORY_GB = LAMBDA_MEMORY_MB / 1024


def get_lambda_architecture(function_name: str, region: str) -> tuple[str, float]:
    """Detect Lambda architecture and return (arch, price_per_gb_second)."""
    try:
        client = boto3.client("lambda", region_name=region)
        response = client.get_function(FunctionName=function_name)
        architectures = response.get("Configuration", {}).get("Architectures", ["x86_64"])
        arch = architectures[0] if architectures else "x86_64"
        price = LAMBDA_PRICE_ARM if arch == "arm64" else LAMBDA_PRICE_X86
        return arch, price
    except Exception:
        return "x86_64", LAMBDA_PRICE_X86


def print_cost_summary(summary: dict, arch: str, price_per_gb_sec: float):
    """Print cost breakdown from agg() results."""
    lambda_time = summary.get("lambda_time_s", 0)
    gb_seconds = lambda_time * LAMBDA_MEMORY_GB
    cost = gb_seconds * price_per_gb_sec

    print("\nCost Calculation")
    print("-" * 70)
    print(f"      Lambda execution time: {lambda_time:,.1f}s ({lambda_time / 3600:.2f} hours)")
    print(f"      Memory: {LAMBDA_MEMORY_MB}MB ({LAMBDA_MEMORY_GB}GB)")
    print(f"      Architecture: {arch}")
    print(f"      GB-seconds: {gb_seconds:,.1f}")
    print(f"      Cost: ${cost:.4f}")

    return {"gb_seconds": gb_seconds, "architecture": arch,
            "price_per_gb_sec": price_per_gb_sec, "estimated_cost_usd": cost}


def save_results(summary: dict, cost_info: dict, args, output_dir: str):
    """Save detailed results to timestamped JSON."""
    # Categorize results for detailed breakdown
    cells_no_granules = 0
    cells_no_data = 0
    for r in summary.get("results", []):
        error = r.get("error")
        if error == "No granules found":
            cells_no_granules += 1
        elif error == "No data after filtering":
            cells_no_data += 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"production_results_{timestamp}.json")
    with open(output_file, "w") as f:
        json.dump(
            {
                "config": vars(args),
                "summary": {
                    "total_cells": summary["total_cells"],
                    "cells_with_data": summary["cells_with_data"],
                    "cells_no_granules": cells_no_granules,
                    "cells_no_data": cells_no_data,
                    "cells_error": summary["cells_error"],
                    "total_obs": summary["total_obs"],
                    "wall_time_s": summary["wall_time_s"],
                    "lambda_time_s": summary.get("lambda_time_s", 0),
                    **cost_info,
                },
                "results": summary.get("results", []),
            },
            f,
            indent=2,
            default=str,
        )
    print(f"\nResults saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Production Lambda orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json
  python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json --max-cells 10
  python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json --dry-run
""",
    )
    parser.add_argument("--config", default=None, help="Pipeline config YAML (default: built-in atl06)")
    parser.add_argument("--catalog", default=None, help="Path to granule catalog JSON (overrides config)")
    parser.add_argument("--store", default=None, help="Output store path (overrides config)")
    parser.add_argument("--max-workers", type=int, default=1700, help="Max concurrent Lambda invocations")
    parser.add_argument("--max-cells", type=int, default=None, help="Limit number of cells (for testing)")
    parser.add_argument("--morton-cell", type=str, default=None, help="Process a specific morton cell")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--overwrite-template", action="store_true", default=False,
                        help="Overwrite existing Zarr template")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument("--output-dir", default=".", help="Directory for output results JSON")
    parser.add_argument(
        "--function-name",
        default=os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-morton-cell"),
        help="Lambda function name (default: env ZAGG_LAMBDA_FUNCTION_NAME or 'process-morton-cell')",
    )
    args = parser.parse_args()

    # Load config
    if args.config:
        config = load_config(args.config)
    else:
        config = default_config()

    # Resolve store for validation
    store_path = args.store or get_store_path(config)
    if store_path and not store_path.startswith("s3://"):
        parser.error(f"Lambda orchestrator requires an S3 store path, got: {store_path}")

    print("=" * 70)
    print("Production Lambda Orchestrator")
    print(f"  Config: {args.config or 'built-in atl06'}")
    print(f"  Store: {store_path}")
    print(f"  Function: {args.function_name}")
    print("=" * 70)

    # Detect architecture for cost calculation
    if not args.dry_run:
        arch, price_per_gb_sec = get_lambda_architecture(args.function_name, args.region)
        print(f"  Architecture: {arch} (${price_per_gb_sec:.10f}/GB-sec)")

    # Run via agg()
    summary = agg(
        config,
        catalog=args.catalog,
        store=args.store,
        backend="lambda",
        max_cells=args.max_cells,
        morton_cell=args.morton_cell,
        max_workers=args.max_workers,
        overwrite=args.overwrite_template,
        dry_run=args.dry_run,
        function_name=args.function_name,
        region=args.region,
    )

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {summary['total_cells']} cells")
        print(f"  Granules per cell: min={summary['granules_per_cell_min']}, "
              f"max={summary['granules_per_cell_max']}, "
              f"avg={summary['granules_per_cell_avg']:.1f}")
        return

    # Cost reporting (CLI-only, not in the library)
    cost_info = print_cost_summary(summary, arch, price_per_gb_sec)

    # Summary
    print("\nSummary")
    print("=" * 70)
    print(f"      Total cells:         {summary['total_cells']}")
    print(f"      With data:           {summary['cells_with_data']}")
    print(f"      Errors:              {summary['cells_error']}")
    print(f"      Total observations:  {summary['total_obs']:,}")
    print(f"      Wall clock time:     {summary['wall_time_s']:.1f}s ({summary['wall_time_s'] / 60:.1f}m)")
    print(f"      Estimated cost:      ${cost_info['estimated_cost_usd']:.4f}")
    print(f"      Output:              {summary['store_path']}")
    print("=" * 70)

    # Save results JSON
    save_results(summary, cost_info, args, args.output_dir)


if __name__ == "__main__":
    main()
