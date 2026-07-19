"""Dispatch the raster (Sentinel-2) release benchmark and emit records (issue #250).

The raster sibling of ``run_full_aoi_benchmark.py``: one run per target over the
NEON AOI, every shard, one year of datatakes, recorded per release. Dispatch is
``zagg.runner.agg(backend="lambda", profile=True)`` -- the runner's raster path
threads the opt-in ``profile`` key to the workers and rolls their
``phase_timings`` (the issue #249 stage set ``open``/``geometry``/``fetch``/
``decode``/``gather`` + the ``write`` bucket) up into
``summary["worker_stage_max"]`` / ``["worker_stage_counts"]``, alongside the
billed-duration and peak-RSS rollups; the harness just records the summary.

Steps, all validatable offline via ``--dry-run``:

1. Build the shard map from the PINNED S2 catalog (``Catalog.from_geoparquet``
   -> ``ShardMap.build``; the catalog is already AOI+year scoped, so no
   region/temporal cut is needed) and print the dispatch plan.
2. ``agg`` over the map: template + global time index emission, then one
   synchronous ``mode="process_raster"`` invoke per shard (``max_retries=1``:
   a failed shard is a failure, never re-paid -- #119).

``--out-json`` is one run record per target; ``raster_series.py`` retains the
release rows. Stage/count dicts ride the record nested (``stage_max`` /
``stage_counts``) and are flattened by the series, mirroring the ``wt_*`` /
``phase_*`` pattern. Stage seconds are straggler maxes of work volume (never
stacked -- overlapped samples can exceed wall); counts are run totals.

Usage::

    python run_raster_benchmark.py \\
      --targets tests/data/benchmark/targets_raster_neon.json \\
      --store-prefix s3://bucket/zagg-bench/raster \\
      --event release --commit "$SHA" --ref "$TAG" \\
      --out-json raster_metrics.json          # add --dry-run to plan only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from zagg.config import get_store_layout, load_config, validate_config  # noqa: E402
from zagg.dispatch import LAMBDA_MEMORY_GB, LAMBDA_PRICE_PER_GB_SEC  # noqa: E402
from zagg.grids import from_config  # noqa: E402


def load_targets(path: str) -> tuple[dict, Path]:
    p = Path(path).resolve()
    return json.loads(p.read_text()), p.parent


def _resolve(base: Path, rel: str) -> Path:
    return (base / rel).resolve()


def run_target(
    name,
    manifest,
    base,
    *,
    store,
    region,
    function_name,
    context,
    dry_run,
    artifacts_dir,
) -> dict:
    from zagg import __version__ as zagg_version
    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import Catalog

    target = manifest["targets"][name]
    config = load_config(str(_resolve(base, target["config"])))
    # Hive flip path (espg directive on PR #261; blocked on issue #237): a
    # target may pin store_layout, applied + re-validated here so promoting
    # the pending hive target into "targets" is the ONLY change needed when
    # issue #237 lands. Today the raster path rejects hive (issue #239), so
    # the pending target fails fast rather than dispatching a wrong layout.
    if target.get("store_layout"):
        config.output["store_layout"] = target["store_layout"]
        validate_config(config)
    # Worker-variant selection (issue #284): a raster target may pin the
    # pre-provisioned worker variant so the release raster leg dispatches to the
    # SAME function production runs (and the tdigest leg) use -- the disk-spill
    # variant -- instead of the bare base. Mirrors run_benchmark.run_target's
    # suffix resolution: base name + ``-<memory>`` + ``-disk`` when extra_disk.
    # Absent -> the base function name, unchanged.
    resolved_function_name = function_name
    worker = target.get("worker")
    if worker:
        config.worker = dict(worker)
        resolved_function_name = (
            function_name + f"-{worker['memory']}" + ("-disk" if worker.get("extra_disk") else "")
        )
    grid = from_config(config)

    cat = Catalog.from_geoparquet(str(_resolve(base, target["catalog"])))
    t0 = time.perf_counter()
    sm = ShardMap.build(cat, grid)
    build_s = time.perf_counter() - t0
    sm.to_json(str(Path(artifacts_dir) / f"sm_{name}.json"))
    cells = list(zip([int(k) for k in sm.shard_keys], sm.granules, strict=True))
    counts = [len(g) for g in sm.granules]
    print(
        f"[{name}] shards={len(cells)} granules(total pairs)={sum(counts)} "
        f"per-shard={sorted(counts)} shardmap_build={build_s:.1f}s "
        f"-> {resolved_function_name} @ {region}",
        flush=True,
    )

    run = {
        "target": name,
        "timestamp": context["timestamp"],
        "commit": context.get("commit", ""),
        "ref": context.get("ref", ""),
        "event": context.get("event", ""),
        "pr_number": context.get("pr_number"),
        "aoi": manifest.get("aoi", {}).get("name"),
        "collection": target.get("collection"),
        "grid_type": target.get("grid_type"),
        "grid_size": target.get("grid_size"),
        "parent_order": int(grid.parent_order),
        "child_order": int(grid.child_order),
        "n_shards": len(cells),
        # Forward-compatible layout axis (null-safe pre-hive: reads "flat");
        # distinguishes the hive rows the #237 flip will add to the series.
        "store_layout": get_store_layout(config),
        "shardmap_build_s": round(build_s, 2),
        "per_shard_granules": sorted(counts),
        "memory_gb": LAMBDA_MEMORY_GB,
        "price_per_gb_sec": LAMBDA_PRICE_PER_GB_SEC,
        "zagg_version": zagg_version,
    }
    if dry_run:
        return run

    from zagg.runner import agg

    # The runner owns template + time-index emission and the profiled
    # per-shard dispatch (issue #250: RasterStrategy threads the opt-in
    # ``profile`` event key and rolls worker phase_timings/telemetry into the
    # summary); the harness just records the summary.
    summary = agg(
        config,
        catalog=str(Path(artifacts_dir) / f"sm_{name}.json"),
        store=store,
        backend="lambda",
        morton_cell=None,  # ALL shards over the AOI
        region=region,
        function_name=resolved_function_name,
        overwrite=True,
        profile=True,
        max_retries=1,  # a failed shard is a failure -- never re-pay (#119)
    )
    template_s = summary.get("template_s")
    fanout_s = summary.get("wall_time_s")
    lam = summary.get("lambda_time_s")
    run.update(
        n_shards_ok=summary.get("cells_with_data"),
        n_shards_error=summary.get("cells_error"),
        timesteps=summary.get("timesteps"),
        slabs_written=summary.get("total_obs"),
        template_s=None if template_s is None else round(template_s, 2),
        lambda_seconds=None if lam is None else round(lam, 2),
        gb_seconds=None if lam is None else round(lam * LAMBDA_MEMORY_GB, 2),
        cost_usd=None
        if lam is None
        else round(lam * LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC, 6),
        total_wall_s=round((template_s or 0.0) + (fanout_s or 0.0), 2),
        fanout_s=None if fanout_s is None else round(fanout_s, 2),
        worker_max_s=summary.get("worker_max_s"),
        worker_median_s=summary.get("worker_median_s"),
        max_memory_mb=summary.get("max_memory_mb"),
        stage_max=summary.get("worker_stage_max"),
        stage_counts=summary.get("worker_stage_counts"),
    )
    return run


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Raster (S2) release benchmark (issue #250).")
    ap.add_argument("--targets", required=True)
    ap.add_argument(
        "--target", action="append", default=[], help="Target name (repeatable; omit for all)"
    )
    ap.add_argument("--store-prefix", default=None, help="<prefix>/<target>.zarr output store")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--function-name", default="process-shard")
    ap.add_argument("--artifacts-dir", default="./raster_artifacts")
    ap.add_argument("--event", default="")
    ap.add_argument("--commit", default="")
    ap.add_argument("--ref", default="")
    ap.add_argument("--pr-number", default=None)
    ap.add_argument("--out-json", default="raster_metrics.json")
    ap.add_argument(
        "--dry-run", action="store_true", help="Build maps + plan only; no AWS, no billing."
    )
    args = ap.parse_args(argv)

    manifest, base = load_targets(args.targets)
    names = args.target or list(manifest["targets"].keys())
    for n in names:
        if n not in manifest["targets"]:
            raise SystemExit(f"unknown target {n!r}; have {sorted(manifest['targets'])}")
    if not args.dry_run and not args.store_prefix:
        raise SystemExit("--store-prefix is required for a live dispatch")

    Path(args.artifacts_dir).mkdir(parents=True, exist_ok=True)
    pr = int(args.pr_number) if args.pr_number not in (None, "", "0") else None
    context = {
        "timestamp": _utc_now_iso(),
        "commit": args.commit,
        "ref": args.ref,
        "event": args.event,
        "pr_number": pr,
    }

    runs = []
    for name in names:
        store = f"{args.store_prefix.rstrip('/')}/{name}.zarr" if args.store_prefix else None
        runs.append(
            run_target(
                name,
                manifest,
                base,
                store=store,
                region=args.region,
                function_name=args.function_name,
                context=context,
                dry_run=args.dry_run,
                artifacts_dir=args.artifacts_dir,
            )
        )

    Path(args.out_json).write_text(json.dumps(runs, indent=2))
    print(f"wrote {args.out_json} ({len(runs)} runs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
