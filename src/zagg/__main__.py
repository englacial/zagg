"""CLI entry point for zagg processing.

Usage:
    python -m zagg --config atl06.yaml --catalog catalog.json
    python -m zagg --config atl06.yaml --catalog catalog.json --store ./test.zarr
    python -m zagg --config atl06.yaml --catalog catalog.json --max-cells 5
    python -m zagg --config atl06.yaml --catalog catalog.json --backend lambda
"""

import argparse
import json
import logging
import os

from zagg.config import load_config
from zagg.runner import agg, normalize_output_credentials


def main():
    parser = argparse.ArgumentParser(
        description="zagg processing runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m zagg --config atl06.yaml --catalog catalog.json
  python -m zagg --config atl06.yaml --catalog catalog.json --store ./test.zarr
  python -m zagg --config atl06.yaml --catalog catalog.json --max-cells 5
  python -m zagg --config atl06.yaml --catalog catalog.json --backend lambda
""",
    )
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--catalog", default=None, help="Path to granule catalog JSON (overrides config)")
    parser.add_argument("--store", default=None, help="Output store path (overrides config)")
    parser.add_argument("--backend", default="local", choices=["local", "lambda"],
                        help="Execution backend (default: local)")
    parser.add_argument("--driver", default=None, choices=["s3", "https"],
                        help="Data access driver (default: from config, or s3)")
    parser.add_argument("--max-cells", type=int, default=None, help="Limit number of cells (for testing)")
    parser.add_argument("--morton-cell", type=str, default=None, help="Process a specific morton cell")
    parser.add_argument("--max-workers", type=int, default=None, help="Max concurrent workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Zarr template")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--output-creds", default=None, metavar="PATH",
        help="Path to a JSON file with credentials for writing the output store "
             "(keys: accessKeyId, secretAccessKey, optional sessionToken/"
             "endpointUrl/region; camelCase, snake_case, or STS PascalCase "
             "spellings accepted). Omit to use the ambient/execution-role creds.",
    )
    parser.add_argument(
        "--function-name",
        default=os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard"),
        help="Lambda function name (default: env ZAGG_LAMBDA_FUNCTION_NAME or 'process-shard')",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = load_config(args.config)

    # Load output credentials from a JSON file (kept out of shell history).
    output_credentials = None
    output_endpoint_url = None
    if args.output_creds:
        with open(args.output_creds) as f:
            output_credentials = json.load(f)
        # Accept camelCase, snake_case, or STS PascalCase key spellings.
        output_credentials = normalize_output_credentials(output_credentials)
        output_endpoint_url = output_credentials.get("endpointUrl")

    results = agg(
        config,
        catalog=args.catalog,
        store=args.store,
        backend=args.backend,
        driver=args.driver,
        max_cells=args.max_cells,
        morton_cell=args.morton_cell,
        max_workers=args.max_workers,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        function_name=args.function_name,
        region=args.region,
        output_credentials=output_credentials,
        output_endpoint_url=output_endpoint_url,
    )

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {results['total_cells']} cells")
        print(f"  Granules per cell: min={results['granules_per_cell_min']}, "
              f"max={results['granules_per_cell_max']}, "
              f"avg={results['granules_per_cell_avg']:.1f}")
        print(f"  Output: {results['store_path']}")
    else:
        print(f"\nDone: {results['cells_with_data']} cells with data, "
              f"{results['total_obs']:,} obs, {results['cells_error']} errors, "
              f"{results['wall_time_s']:.1f}s")
        if "estimated_cost_usd" in results:
            print(f"Lambda compute: {results['lambda_time_s']:.0f}s total, "
                  f"{results['gb_seconds']:.0f} GB-s, ~${results['estimated_cost_usd']:.2f}")
        print(f"Output: {results['store_path']}")


if __name__ == "__main__":
    main()
