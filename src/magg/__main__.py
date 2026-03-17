"""CLI entry point for magg processing.

Usage:
    python -m magg --config atl06.yaml --catalog catalog.json
    python -m magg --config atl06.yaml --catalog catalog.json --store ./test.zarr
    python -m magg --config atl06.yaml --catalog catalog.json --max-cells 5
    python -m magg --config atl06.yaml --catalog catalog.json --backend lambda
"""

import argparse
import logging
import os

from magg.config import load_config
from magg.runner import agg


def main():
    parser = argparse.ArgumentParser(
        description="magg processing runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m magg --config atl06.yaml --catalog catalog.json
  python -m magg --config atl06.yaml --catalog catalog.json --store ./test.zarr
  python -m magg --config atl06.yaml --catalog catalog.json --max-cells 5
  python -m magg --config atl06.yaml --catalog catalog.json --backend lambda
""",
    )
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--catalog", default=None, help="Path to granule catalog JSON (overrides config)")
    parser.add_argument("--store", default=None, help="Output store path (overrides config)")
    parser.add_argument("--backend", default="local", choices=["local", "lambda"],
                        help="Execution backend (default: local)")
    parser.add_argument("--max-cells", type=int, default=None, help="Limit number of cells (for testing)")
    parser.add_argument("--morton-cell", type=str, default=None, help="Process a specific morton cell")
    parser.add_argument("--max-workers", type=int, default=None, help="Max concurrent workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Zarr template")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--function-name",
        default=os.environ.get("MAGG_LAMBDA_FUNCTION_NAME", "process-morton-cell"),
        help="Lambda function name (default: env MAGG_LAMBDA_FUNCTION_NAME or 'process-morton-cell')",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = load_config(args.config)

    results = agg(
        config,
        catalog=args.catalog,
        store=args.store,
        backend=args.backend,
        max_cells=args.max_cells,
        morton_cell=args.morton_cell,
        max_workers=args.max_workers,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        function_name=args.function_name,
        region=args.region,
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
        print(f"Output: {results['store_path']}")


if __name__ == "__main__":
    main()
