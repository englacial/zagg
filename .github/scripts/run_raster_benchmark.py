"""Dispatch the raster (Sentinel-2) release benchmark and emit records (issue #250).

The raster sibling of ``run_full_aoi_benchmark.py``: one run per target over the
NEON AOI, every shard, one year of datatakes, recorded per release. It owns its
OWN per-shard ``mode="process_raster"`` Lambda dispatch (mirroring
``zagg.runner.RasterStrategy._run_lambda_shards``' transport) because the
runner's raster lambda path neither threads a ``profile`` key to the workers nor
surfaces their per-shard ``phase_timings`` -- and the whole point of this leg is
capturing the issue #249 stage set (``open``/``geometry``/``fetch``/``decode``/
``gather`` + the ``write`` bucket, PR #256) into ``raster_series.parquet``.

Steps, all validatable offline via ``--dry-run``:

1. Build the shard map from the PINNED S2 catalog (``Catalog.from_geoparquet``
   -> ``ShardMap.build``; the catalog is already AOI+year scoped, so no
   region/temporal cut is needed) and print the dispatch plan.
2. Emit the raster template + global time index (runner-owned in ``agg``;
   harness-owned here) to the output store.
3. Fan out one synchronous ``mode="process_raster"`` invoke per shard with
   ``"profile": true``, roll the per-shard stage seconds up as the straggler
   max (work volume, never stacked -- stage sums can exceed wall) and the
   counts as run totals, and derive cost from summed billed ``duration_s``.

``--out-json`` is one run record per target; ``raster_series.py`` retains the
release rows. Stage/count dicts ride the record nested (``stage_max`` /
``stage_counts``) and are flattened by the series, mirroring the ``wt_*`` /
``phase_*`` pattern.

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

from zagg.config import load_config  # noqa: E402
from zagg.dispatch import LAMBDA_MEMORY_GB, LAMBDA_PRICE_PER_GB_SEC  # noqa: E402
from zagg.grids import from_config  # noqa: E402

# Stage keys of the worker's phase_timings["stages"] (issue #249): float
# seconds vs int counts, split so the rollup maxes seconds and sums counts.
STAGE_SECONDS = ("open", "geometry", "fetch", "decode", "gather")
STAGE_COUNTS = ("assets", "tiles", "geom_hits")

# Synchronous per-shard invokes; NEON fans to a handful of shards, so a small
# pool matches the point release leg's concurrency scale.
_MAX_SHARD_CONCURRENCY = 8


def load_targets(path: str) -> tuple[dict, Path]:
    p = Path(path).resolve()
    return json.loads(p.read_text()), p.parent


def _resolve(base: Path, rel: str) -> Path:
    return (base / rel).resolve()


def stage_rollup(bodies: list[dict]) -> tuple[dict, dict]:
    """Roll per-shard profiled bodies into ``(stage_max, stage_counts)``.

    ``stage_max``: straggler max seconds per stage across shards (the
    max-across-shards framing the point leg uses for its phases), with the
    handler's ``write`` bucket riding next to the issue #249 stage set.
    ``stage_counts``: work-volume totals summed across shards. Bodies without
    ``phase_timings`` (a pre-#256 worker) contribute nothing; all-unprofiled
    -> two empty dicts, so the series columns stay null rather than zero-fake.
    """
    stage_max: dict = {}
    stage_counts: dict = {}
    for body in bodies:
        pt = body.get("phase_timings") or {}
        stages = pt.get("stages") or {}
        for key in STAGE_SECONDS:
            if key in stages:
                stage_max[key] = max(stage_max.get(key, 0.0), float(stages[key]))
        for key in STAGE_COUNTS:
            if key in stages:
                stage_counts[key] = stage_counts.get(key, 0) + int(stages[key])
        if pt.get("write") is not None:
            stage_max["write"] = max(stage_max.get("write", 0.0), float(pt["write"]))
    return stage_max, stage_counts


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0


def _dispatch_shards(
    cells, config, time_index, store: str, *, region: str, function_name: str
) -> list[dict]:
    """One synchronous ``mode="process_raster"`` invoke per shard, profiled.

    Mirrors ``RasterStrategy._run_lambda_shards``' envelope handling
    (FunctionError / non-200 body -> shard error) with the benchmark policy of
    ``run_full_aoi_benchmark``: no invoke retries -- a failed shard is a
    failure, never re-paid (#119). Returns one ``{"shard_key", "error",
    "body"}`` result per shard.
    """
    import json as _json
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import boto3
    from botocore.config import Config

    client = boto3.client(
        "lambda",
        region_name=region,
        config=Config(
            max_pool_connections=max(_MAX_SHARD_CONCURRENCY, 10),
            read_timeout=910,
            retries={"max_attempts": 0},
        ),
    )
    config_dict = {
        "data_source": config.data_source,
        "output": config.output,
        "pipeline": config.pipeline,
    }

    def _event(shard_key, granules):
        # Only the shard's own slice of the global time index rides the event,
        # exactly as the runner's raster transport slices it.
        keys = {e.get("time_key") or e.get("datetime") for e in granules if e.get("assets")}
        return {
            "mode": "process_raster",
            "shard_key": int(shard_key),
            "granules": granules,
            "config": config_dict,
            "store_path": store,
            "time_index": {k: time_index[k] for k in keys},
            "profile": True,  # the point of this harness (issue #249 stages)
        }

    def _one(pair):
        shard_key = int(pair[0])
        try:
            resp = client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=_json.dumps(_event(*pair)),
            )
            raw_text = resp["Payload"].read().decode("utf-8")
            if resp.get("FunctionError"):
                return {"shard_key": shard_key, "error": f"Lambda error: {raw_text[:150]}"}
            raw = _json.loads(raw_text)
            body = _json.loads(raw.get("body", "{}"))
            if raw.get("statusCode") != 200:
                error = body.get("error", f"status {raw.get('statusCode')}")
                return {"shard_key": shard_key, "error": error}
            return {"shard_key": shard_key, "error": None, "body": body}
        except Exception as e:  # noqa: BLE001 - per-shard isolation, run continues
            return {"shard_key": shard_key, "error": str(e)}

    max_workers = min(len(cells), _MAX_SHARD_CONCURRENCY) or 1
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_one, pair) for pair in cells]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results


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
        f"-> {function_name} @ {region}",
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
        "shardmap_build_s": round(build_s, 2),
        "per_shard_granules": sorted(counts),
        "memory_gb": LAMBDA_MEMORY_GB,
        "price_per_gb_sec": LAMBDA_PRICE_PER_GB_SEC,
        "zagg_version": zagg_version,
    }
    if dry_run:
        return run

    from zagg.processing.raster import emit_raster_template, raster_time_index
    from zagg.store import open_store

    time_index, times_us = raster_time_index(sm.granules)
    if not time_index:
        raise SystemExit(f"[{name}] catalog carries no raster granule entries")

    # Template + time coordinate are harness-owned (runner-owned inside agg):
    # the raster path has no setup Lambda, so the orchestrator (here: the
    # benchmark role) writes the store template directly.
    t0 = time.perf_counter()
    emit_raster_template(open_store(store, region=region), grid, config, times_us, overwrite=True)
    template_s = time.perf_counter() - t0

    t0 = time.time()
    results = _dispatch_shards(
        cells, config, time_index, store, region=region, function_name=function_name
    )
    fanout_s = time.time() - t0

    ok = [r for r in results if not r["error"]]
    for r in results:
        if r["error"]:
            print(f"[{name}] shard {r['shard_key']}: {r['error']}", flush=True)
    if cells and not ok:
        raise SystemExit(f"[{name}] all {len(results)} raster shard(s) failed")

    bodies = [r["body"] for r in ok]
    durations = [float(b["duration_s"]) for b in bodies if b.get("duration_s") is not None]
    lam = sum(durations)
    stage_max, stage_counts = stage_rollup(bodies)
    run.update(
        n_shards_ok=sum(1 for b in bodies if b.get("timesteps")),
        n_shards_error=len(results) - len(ok),
        timesteps=int(len(time_index)),
        slabs_written=sum(int(b.get("timesteps") or 0) for b in bodies),
        template_s=round(template_s, 2),
        lambda_seconds=round(lam, 2),
        gb_seconds=round(lam * LAMBDA_MEMORY_GB, 2),
        cost_usd=round(lam * LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC, 6),
        total_wall_s=round(template_s + fanout_s, 2),
        fanout_s=round(fanout_s, 2),
        worker_max_s=max(durations) if durations else None,
        worker_median_s=_median(durations),
        stage_max=stage_max,
        stage_counts=stage_counts,
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
