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
import sys

from zagg.config import load_config
from zagg.notebook import confirm_max_cost, max_cost_preview
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
    parser.add_argument(
        "--catalog", default=None, help="Path to granule catalog JSON (overrides config)"
    )
    parser.add_argument("--store", default=None, help="Output store path (overrides config)")
    parser.add_argument(
        "--backend",
        default="local",
        choices=["local", "lambda"],
        help="Execution backend (default: local)",
    )
    parser.add_argument(
        "--driver",
        default=None,
        choices=["s3", "https"],
        help="Data access driver (default: from config, or s3)",
    )
    parser.add_argument(
        "--max-cells", type=int, default=None, help="Limit number of cells (for testing)"
    )
    parser.add_argument(
        "--morton-cell",
        type=str,
        default=None,
        help="Process a single shard: its decimal morton id (e.g. -31123) for "
        "HEALPix, or the shard-key int for other grids",
    )
    parser.add_argument("--max-workers", type=int, default=None, help="Max concurrent workers")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Zarr template")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Emit per-phase worker timings (read/index/aggregate) on the lambda "
        "backend. Off by default to avoid the per-worker probe tax (issue #100).",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--output-creds",
        default=None,
        metavar="PATH",
        help="Path to a JSON file with credentials for writing the output store "
        "(keys: accessKeyId, secretAccessKey, optional sessionToken/"
        "endpointUrl/region; camelCase, snake_case, or STS PascalCase "
        "spellings accepted). Omit to use the ambient/execution-role creds.",
    )
    parser.add_argument(
        "--function-name",
        default=None,
        help="Lambda function name; an explicit value wins verbatim. Default: "
        "env ZAGG_LAMBDA_FUNCTION_NAME (or 'process-shard') plus the config "
        "worker: block's variant suffix (issue #235). Resolved in the runner "
        "so the config selection is not silently masked.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip the max-cost confirmation prompt before a Lambda fan-out "
        "(issue #298). The ceiling is still printed.",
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

    # Max-cost gate (issue #298): a Lambda fan-out spends real money, so the
    # CLI blocks on the pre-invoke ceiling with a yes/no prompt; --yes/-y
    # skips. Dry runs invoke nothing, and a run with no resolvable catalog is
    # about to raise agg's own error -- both bypass the gate. The notebook
    # wrapper (zagg.notebook) never blocks; this prompt is CLI-only.
    if args.backend == "lambda" and not args.dry_run and (args.catalog or config.catalog):
        preview = max_cost_preview(
            config,
            args.catalog,
            max_cells=args.max_cells,
            morton_cell=args.morton_cell,
        )
        if not confirm_max_cost(preview, assume_yes=args.yes):
            print("Aborted: max-cost gate declined (rerun with --yes to skip the prompt).")
            sys.exit(1)

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
        profile=args.profile,
    )

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {results['total_cells']} cells")
        print(
            f"  Granules per cell: min={results['granules_per_cell_min']}, "
            f"max={results['granules_per_cell_max']}, "
            f"avg={results['granules_per_cell_avg']:.1f}"
        )
        print(f"  Output: {results['store_path']}")
    else:
        print(
            f"\nDone: {results['cells_with_data']} cells with data, "
            f"{results['total_obs']:,} obs, {results['cells_error']} errors, "
            f"{results['wall_time_s']:.1f}s"
        )
        if "estimated_cost_usd" in results:
            print(
                f"Lambda compute: {results['lambda_time_s']:.0f}s total, "
                f"{results['gb_seconds']:.0f} GB-s, ~${results['estimated_cost_usd']:.2f}"
            )
        if results.get("worker_phase_max"):
            breakdown = ", ".join(
                f"{phase} {secs:.0f}s" for phase, secs in results["worker_phase_max"].items()
            )
            print(f"Worker phases (max across cells): {breakdown}")
        print(f"Output: {results['store_path']}")


if __name__ == "__main__":
    main()
