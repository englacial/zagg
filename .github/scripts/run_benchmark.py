"""Dispatch the pinned benchmark shard(s) to Lambda and emit metrics (issue #110).

For each requested target this loads the pinned config + shard map, dispatches the
ONE densest shard to AWS Lambda via ``zagg.runner.agg`` (with ``profile=True`` so
the worker phase timings come back), and flattens the run summary into a record
(``bench_metrics.build_record``). It writes the records as JSON (consumed by
``update_series``) and a markdown comment body (posted on PRs).

Run identity (commit/ref/event/pr-number) is passed in by the workflow; this
script does the dispatch + arithmetic only. It needs AWS credentials in the
environment (the workflow supplies them via OIDC role assumption) and the Lambda
to already be deployed -- it never stands up or mutates infrastructure.

Usage::

    python run_benchmark.py --targets tests/data/benchmark/targets.json \\
        --target gain_bias_healpix_o11 --target tdigest_healpix_o11 \\
        --store-prefix s3://my-bucket/zagg-bench \\
        --region us-west-2 --function-name process-shard \\
        --event pr --commit "$SHA" --ref "$REF" --pr-number 123 \\
        --out-json metrics.json --out-comment comment.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

# Allow ``import bench_metrics`` whether run as a script or imported by tests.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402

from zagg.config import load_config  # noqa: E402
from zagg.grids import from_config  # noqa: E402


def load_targets(path: str) -> tuple[dict, Path]:
    """Load the targets manifest; return it plus its directory (for rel paths)."""
    p = Path(path).resolve()
    with open(p) as f:
        return json.load(f), p.parent


def _resolve(base: Path, rel: str) -> Path:
    return (base / rel).resolve()


def all_target_names(manifest: dict) -> list[str]:
    """Names that ``run all`` iterates: the committed merge matrix only.

    ``provisional_targets`` (issue #130) are PR-tree-only and runnable by explicit
    ``--target`` name, but are deliberately excluded here so they never join the
    permanent every-merge matrix until the carrier decision is made.
    """
    return list(manifest["targets"].keys())


def _resolve_target(manifest: dict, name: str) -> dict:
    """Look a target up in the committed matrix, then the provisional block.

    ``provisional_targets`` lets ``/benchmark --target <name>`` run a PR-tree-only
    target (the pandas-vs-arrow carrier comparison, issue #130) without that target
    being part of ``targets`` (the committed merge matrix).
    """
    targets = manifest.get("targets", {})
    if name in targets:
        return targets[name]
    provisional = manifest.get("provisional_targets", {})
    if name in provisional:
        return provisional[name]
    known = list(targets) + list(provisional)
    raise KeyError(f"unknown target '{name}'; have {known}")


def run_target(
    name: str,
    manifest: dict,
    base: Path,
    *,
    store: str,
    region: str,
    function_name: str,
    context: dict,
    dry_run: bool = False,
) -> dict:
    """Dispatch one target's shard and return its benchmark record."""
    from zagg import __version__ as zagg_version

    target = _resolve_target(manifest, name)
    shardmap_key = target["shardmap"]
    shardmap_meta = manifest["shardmaps"][shardmap_key]
    config_path = _resolve(base, target["config"])
    shardmap_path = _resolve(base, shardmap_meta["path"])
    shard_key = int(shardmap_meta["shard_key"])
    n_granules = shardmap_meta.get("n_granules")

    # Per-cell carrier (issue #130). Default "pandas" keeps the dispatched event
    # byte-identical to a pre-handoff run; the arrow (arro3) target sets it.
    handoff = target.get("handoff", "pandas")

    config = load_config(str(config_path))
    # parent_order lives in the config grid for HEALPix; the kwarg is just a
    # legacy fallback. Rect grids ignore it. ``from_config`` gives us the grid
    # object the area/cost derivation needs.
    grid = from_config(config)

    ctx = dict(
        context,
        target=name,
        aggregator=target["aggregator"],
        grid_type=target["grid_type"],
        grid_size=target["grid_size"],
        shard_key=shard_key,
    )

    if dry_run:
        # Wiring check only: no AWS, no dispatch. Emit a record with empty metrics.
        summary: dict = {}
    else:
        from zagg.runner import agg

        summary = agg(
            config,
            catalog=str(shardmap_path),
            store=store,
            backend="lambda",
            morton_cell=str(shard_key),
            region=region,
            function_name=function_name,
            overwrite=True,
            handoff=handoff,
            profile=True,
            # A benchmark measures one clean invocation and records a failure as
            # a failure -- never re-invoke (and never pay) to re-fail (#119).
            max_retries=1,
        )

    return bench_metrics.build_record(
        summary,
        grid=grid,
        context=ctx,
        n_granules=n_granules,
        zagg_version=zagg_version,
    )


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the zagg Lambda benchmark.")
    parser.add_argument("--targets", required=True, help="Path to targets.json manifest")
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Target key to run (repeatable). Omit to run every target.",
    )
    parser.add_argument(
        "--store-prefix",
        default=None,
        help="Output store prefix; each target writes <prefix>/<target>.zarr",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument(
        "--function-name",
        default=os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard"),
        help="Lambda function name",
    )
    parser.add_argument("--event", default="", help="Trigger kind (pr|merge|manual)")
    parser.add_argument("--commit", default="", help="Commit SHA under test")
    parser.add_argument("--ref", default="", help="Branch / PR ref label")
    parser.add_argument("--pr-number", default=None, help="PR number (if a PR run)")
    parser.add_argument("--out-json", default="metrics.json", help="Records JSON output")
    parser.add_argument("--out-comment", default=None, help="Markdown comment output")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Lambda dispatch; emit empty-metric records (wiring check).",
    )
    parser.add_argument(
        "--worker-note",
        default="",
        help="One-line banner for the PR comment when the benchmarked worker is "
        "the stable deploy, not this PR's code (issue #25).",
    )
    args = parser.parse_args(argv)

    manifest, base = load_targets(args.targets)
    # "run all" (no --target) iterates the committed merge matrix only; provisional
    # (PR-tree-only) targets are run by explicit --target name (issue #130).
    names = args.target or all_target_names(manifest)
    known = set(manifest.get("targets", {})) | set(manifest.get("provisional_targets", {}))

    pr_number = int(args.pr_number) if args.pr_number not in (None, "", "0") else None
    context = {
        "timestamp": _utc_now_iso(),
        "commit": args.commit,
        "ref": args.ref,
        "event": args.event,
        "pr_number": pr_number,
    }

    records = []
    for name in names:
        if name not in known:
            raise SystemExit(f"unknown target '{name}'; have {sorted(known)}")
        store = None
        if args.store_prefix:
            store = f"{args.store_prefix.rstrip('/')}/{name}.zarr"
        record = run_target(
            name,
            manifest,
            base,
            store=store,
            region=args.region,
            function_name=args.function_name,
            context=context,
            dry_run=args.dry_run,
        )
        records.append(record)
        print(
            f"[{name}] obs={record['total_obs']} runtime_s={record['runtime_s']} "
            f"cost/shard=${record['cost_per_shard_usd']} "
            f"cost/100km2=${record['cost_per_100km2_usd']} "
            f"max_memory_mb={record['max_memory_mb']}"
        )

    Path(args.out_json).write_text(json.dumps(records, indent=2))
    if args.out_comment:
        Path(args.out_comment).write_text(
            bench_metrics.comment_markdown(records, worker_note=args.worker_note)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
