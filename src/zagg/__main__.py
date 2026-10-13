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

from zagg.config import get_pipeline_type, load_config
from zagg.dispatch_guard import describe
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
    parser.add_argument(
        "--allow-contraction",
        action="store_true",
        help="Rewrite units whose planned inputs DROP granule ids the leaf "
        "already recorded (issue #388). Without it such a unit refuses and is "
        "counted in the run summary. --overwrite is NOT the substitute: it "
        "disables the skip gate outright, so the contraction rewrites unannounced.",
    )
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
    # about to raise agg's own error -- both bypass the gate. Temporal configs
    # have no shardmap ceiling (max_cost_preview raises for them) and via the
    # CLI carry no events source, so agg would raise anyway -- the gate only
    # engages for the catalog-driven kinds (spatial/raster). The notebook
    # wrapper (zagg.notebook) never blocks; this prompt is CLI-only.
    if (
        args.backend == "lambda"
        and not args.dry_run
        and (args.catalog or config.catalog)
        and get_pipeline_type(config) != "temporal"
    ):
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
        allow_contraction=args.allow_contraction,
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
        # Skip-if-current counters (issue #388). A run where every unit skipped
        # or refused writes no run-record parquet and lands zero in the four
        # keys above, so without this line the guard is invisible from the CLI
        # — and the refusal's remedy (--allow-contraction) unfindable.
        current = results.get("cells_current", 0)
        refused = results.get("cells_refused", 0)
        unrecorded = results.get("cells_unrecorded", 0)
        if current or refused or unrecorded:
            print(
                f"Skip-if-current: {current} current (unchanged, not rewritten), "
                f"{refused} refused, {unrecorded} rewritten with the guard inert "
                f"(leaves predating issue #388)"
            )
        if refused:
            print(
                "  Refused units drop granule ids their leaf already recorded. "
                "Rerun with --allow-contraction to rewrite them; --overwrite "
                "disables the guard instead of acknowledging it."
            )
            # The log names only the first five missing ids per unit; the
            # manifest is the durable full diff (issue #388).
            if results.get("refusal_manifest_path"):
                print(f"  Which granules, per unit: {results['refusal_manifest_path']}")
        # The lifecycle touch is fail-open, so a total failure (a read-scoped
        # role, an SCP, an ACL edge — s3:PutObject denied fails EVERY object
        # of EVERY unit) otherwise renders as an unqualified "N current" and
        # exits 0, while those products keep their old purge clock and get
        # deleted on the bucket rule's schedule. Only trace is a per-object
        # warning, buried in thousands of lines of per-cell progress.
        if results.get("touch_failures"):
            print(
                f"  {results['touch_failures']} lifecycle touch(es) FAILED — those objects "
                f"did NOT have their purge clock reset and a bucket lifecycle rule can "
                f"still expire them (fail-open; see the warnings above)"
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
        # The Event fan-out's ramp steps and any breaker trip (issue #652).
        for line in describe(results.get("ramp"), results.get("breaker")):
            print(line)
        # The tail's verdict (issue #610): a run whose rollup sweep did not
        # complete exits 0 by design — the rollups are regenerable caches (D9)
        # — so the shortfall has to be on the operator's last screen, not in
        # the log alone. The v3 California run printed nothing at all.
        if short := results.get("tail_sweep_short"):
            for reason in short:
                print(f"  TAIL SHORT: {reason}")
            print(
                "  Regenerate with `python -m zagg.sweep <store>` and, for the ladder, "
                "`python -m zagg.sweep <store> --stages --scope <subtree>`"
            )
        print(f"Output: {results['store_path']}")


if __name__ == "__main__":
    main()
