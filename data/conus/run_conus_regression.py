"""CONUS density-regression dispatch: cold + warm two-pass over 25 stratified shards.

Issue #202 leg 4b input. Dispatches the 25 shards selected by
``select_regression_shards.py`` (granule counts 21..144, even spread) to the
``process-shard`` Lambda **twice**:

  * **cold** -- sidecar cache empty, ``on_miss: build`` populates it (first-run cost).
  * **warm** -- cache now present, reads hit it (repeat cost).

Between passes it verifies the sidecar manifests actually landed in S3 (the #148
403 concern): a sample CONUS granule's ``<store>/<granule_id>.parquet`` must go
absent -> present across the cold pass, else warm isn't warm and the run aborts.

Records per-shard runtime / lambda-seconds / cost / RSS for both passes and fits
``granules -> lambda_seconds`` separately for cold and warm, so the CONUS estimate
has both a first-run and a repeat cost curve. Billed: ~50 invocations, a-priori
well under $1. Reuses the full-AOI harness helpers.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / ".github" / "scripts"))

from run_full_aoi_benchmark import (  # noqa: E402
    LAMBDA_MEMORY_GB,
    LAMBDA_PRICE_PER_GB_SEC,
    _aoi_parts,
    _assert_account,
    _prefilter,
)

DEFAULT_CATALOG = "/Users/espg/software/zagg/data/atl03_v007/atl03_v007_full.parquet"
CONFIG = REPO / "tests/data/benchmark/configs/atl03_tdigest_healpix_o9_cached.yaml"
POLYGON = HERE / "conus.geojson"
SELECTION = HERE / "conus_regression_shards.json"
SUBMAP = HERE / "conus_regression_submap.json"
SIDECAR_STORE = "s3://sliderule-public-cors/zagg-index/ATL03/007"
START, END = "2018-10-13", "2026-03-15"
CONUS_BBOX = (-124.706553, 25.120779, -66.979601, 49.383625)
EXPECT_ACCOUNT = "742127912612"


def build_submap(catalog_path: str, grid, selection_path=SELECTION, submap_path=SUBMAP) -> "object":
    """Full CONUS shard map, subset to the selected shard labels (cached)."""
    from pathlib import Path

    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import Catalog

    selection_path, submap_path = Path(selection_path), Path(submap_path)
    if submap_path.exists():
        print(f"loading cached sub-shardmap {submap_path.name}", flush=True)
        return ShardMap.from_json(str(submap_path))

    want = {s["shard_label"] for s in json.loads(selection_path.read_text())["shards"]}
    print(f"building full CONUS shard map to extract {len(want)} shards ...", flush=True)
    catalog = Catalog.from_geoparquet(catalog_path)
    sub = _prefilter(catalog, CONUS_BBOX, START, END)
    parts, _bbox = _aoi_parts(POLYGON)
    full = ShardMap.build(sub, grid, region=parts, backend="mortie", footprint="swath")

    keep_keys, keep_granules = [], []
    for key, gran in zip(full.shard_keys, full.granules):
        if grid.shard_label(int(key)) in want:
            keep_keys.append(int(key))
            keep_granules.append(gran)
    if len(keep_keys) != len(want):
        raise SystemExit(f"matched {len(keep_keys)}/{len(want)} selected shards -- aborting")

    meta = dict(full.metadata or {})
    meta["subset"] = f"CONUS regression: {len(keep_keys)} stratified shards (issue #202)"
    sub_sm = ShardMap(full.grid_signature, keep_keys, keep_granules, meta, None)
    sub_sm.to_json(str(submap_path))
    print(
        f"wrote {submap_path.name}: {len(keep_keys)} shards, "
        f"{sum(len(g) for g in keep_granules)} granule-reads",
        flush=True,
    )
    return sub_sm


def _sample_granule_id(sm) -> str:
    """A granule id from the sub-map, for the sidecar-write check."""
    gid = sm.granules[0][0]["id"]
    return gid[:-3] if gid.endswith(".h5") else gid


def _manifest_exists(granule_id: str, store: str = SIDECAR_STORE) -> bool:
    uri = f"{store}/{granule_id}.parquet"
    r = subprocess.run(["aws", "s3", "ls", uri], capture_output=True, text=True)
    return r.returncode == 0 and bool(r.stdout.strip())


def dispatch(config, submap_path: str, store: str, region: str, function_name: str) -> list[dict]:
    """One pass over all shards in the sub-map; returns per-shard rows."""
    from zagg.config import get_handoff
    from zagg.grids import from_config
    from zagg.runner import agg

    grid = from_config(config)
    summary = agg(
        config,
        catalog=submap_path,
        store=store,
        backend="lambda",
        morton_cell=None,
        region=region,
        function_name=function_name,
        overwrite=True,
        handoff=get_handoff(config),
        profile=True,
        max_retries=1,
    )
    rows = []
    for r in summary.get("results", []):
        rt = float(r.get("lambda_duration") or 0.0)
        gb = rt * LAMBDA_MEMORY_GB
        body = r.get("body") or {}
        rows.append(
            {
                "shard_label": grid.shard_label(int(r["shard_key"])),
                "shard_key": int(r["shard_key"]),
                "n_granules": r.get("granule_count"),
                "runtime_s": rt,
                "gb_seconds": gb,
                "cost_usd": gb * LAMBDA_PRICE_PER_GB_SEC,
                "max_memory_mb": body.get("max_memory_mb"),
                "wall_time_s": r.get("wall_time"),
                "retries": r.get("retries"),
                "timeout": bool(r.get("timeout")),
                "status_code": r.get("status_code"),
                "error": r.get("error"),
            }
        )
    return rows, summary


def _fit(rows: list[dict]) -> dict:
    ok = [
        (r["n_granules"], r["runtime_s"])
        for r in rows
        if not r["error"] and r["runtime_s"] and r["n_granules"]
    ]
    if len(ok) < 2:
        return {"n_points": len(ok), "note": "insufficient successful shards to fit"}
    g = np.array([x[0] for x in ok], float)
    t = np.array([x[1] for x in ok], float)
    slope, intercept = np.polyfit(g, t, 1)
    pred = slope * g + intercept
    ss_res = float(((t - pred) ** 2).sum())
    ss_tot = float(((t - t.mean()) ** 2).sum())
    return {
        "n_points": len(ok),
        "sec_per_granule": round(float(slope), 4),
        "intercept_s": round(float(intercept), 2),
        "r_squared": round(1 - ss_res / ss_tot, 4) if ss_tot else None,
        "granule_range": [int(g.min()), int(g.max())],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="CONUS cold/warm density regression (issue #202).")
    ap.add_argument("--catalog", default=DEFAULT_CATALOG)
    ap.add_argument("--store-prefix", default="s3://sliderule-public/zagg-bench/conus-regression")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--function-name", default="process-shard")
    ap.add_argument("--out", default=str(HERE / "results" / "conus_regression_results.json"))
    ap.add_argument("--order", type=int, default=9, help="override parent_order (e.g. 8)")
    ap.add_argument("--config", default=str(CONFIG))
    ap.add_argument("--selection", default=str(SELECTION))
    ap.add_argument("--submap", default=str(SUBMAP))
    ap.add_argument(
        "--sidecar-store",
        default=SIDECAR_STORE,
        help="sidecar manifest store (used only when --index-backend is left as sidecar)",
    )
    ap.add_argument(
        "--index-backend",
        default=None,
        help="replace data_source.index with {backend: <this>} (e.g. 'inline' for a "
        "cache-independent, genuinely-uncached cold-start pass). Drops sidecar keys.",
    )
    ap.add_argument(
        "--cold-only",
        action="store_true",
        help="run one uncached pass only (no sidecar write-check, no warm pass) -- the "
        "inline cold-start estimate. Pair with --index-backend inline.",
    )
    ap.add_argument(
        "--buffer-granules",
        type=int,
        default=None,
        help="enable streaming aggregation with this buffer size -- bounds peak memory "
        "to one buffer + running digests instead of pooling the whole shard (lets "
        "coarse orders like o8 fit 4 GB).",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from zagg.config import load_config
    from zagg.grids import from_config

    config = load_config(args.config)
    if args.index_backend:
        config.data_source["index"] = {"backend": args.index_backend}
    else:
        config.data_source["index"]["store"] = args.sidecar_store
    if args.buffer_granules:
        config.aggregation["streaming"] = {"buffer_granules": args.buffer_granules}
    if args.order != 9:
        config.output.setdefault("grid", {})["parent_order"] = args.order
    grid = from_config(config)
    sm = build_submap(args.catalog, grid, args.selection, args.submap)
    sm.to_json(args.submap)
    counts = sorted(len(g) for g in sm.granules)
    print(
        f"{len(sm.shard_keys)}-shard sub-map (o{args.order}): granule counts {counts}", flush=True
    )

    if args.dry_run:
        print("dry-run: built sub-map, no dispatch.")
        return 0

    if args.cold_only:
        _assert_account(args.region, EXPECT_ACCOUNT)
        backend = config.data_source["index"]["backend"]
        print(f"\n=== COLD-ONLY PASS (uncached, backend={backend}) ===", flush=True)
        cold, cold_sum = dispatch(
            config, args.submap, f"{args.store_prefix}-cold", args.region, args.function_name
        )
        out = {
            "issue": 202,
            "temporal": {"start": START, "end": END},
            "n_shards": len(sm.shard_keys),
            "order": args.order,
            "cold_backend": backend,
            "cold": {
                "per_shard": cold,
                "fit": _fit(cold),
                "lambda_seconds": cold_sum.get("lambda_time_s"),
                "cost_usd": cold_sum.get("estimated_cost_usd"),
            },
            "warm": None,
        }
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(out, indent=2))
        print(f"\ncold-only fit ({backend}): {out['cold']['fit']}")
        print(f"wrote {outp}")
        return 0

    _assert_account(args.region, EXPECT_ACCOUNT)
    sample_gid = _sample_granule_id(sm)
    pre = _manifest_exists(sample_gid, args.sidecar_store)
    print(
        f"pre-cold sidecar for {sample_gid} in {args.sidecar_store}: "
        f"{'PRESENT' if pre else 'absent'}",
        flush=True,
    )

    print("\n=== COLD PASS (build sidecars) ===", flush=True)
    cold, cold_sum = dispatch(
        config, args.submap, f"{args.store_prefix}-cold", args.region, args.function_name
    )

    post = _manifest_exists(sample_gid, args.sidecar_store)
    print(f"post-cold sidecar for {sample_gid}: {'PRESENT' if post else 'ABSENT'}", flush=True)
    if not post:
        raise SystemExit(
            "SIDECAR WRITE FAILED: manifest absent after cold pass -- warm pass would be "
            "meaningless. Likely the process-shard execution role lacks s3:PutObject on "
            "zagg-index/* (the #148 role gap). Aborting before the warm pass."
        )

    print("\n=== WARM PASS (read sidecars) ===", flush=True)
    warm, warm_sum = dispatch(
        config, args.submap, f"{args.store_prefix}-warm", args.region, args.function_name
    )

    out = {
        "issue": 202,
        "temporal": {"start": START, "end": END},
        "n_shards": len(sm.shard_keys),
        "order": args.order,
        "sidecar_store": args.sidecar_store,
        "sidecar_write_verified": bool(post and not pre),
        "cold": {
            "per_shard": cold,
            "fit": _fit(cold),
            "lambda_seconds": cold_sum.get("lambda_time_s"),
            "cost_usd": cold_sum.get("estimated_cost_usd"),
        },
        "warm": {
            "per_shard": warm,
            "fit": _fit(warm),
            "lambda_seconds": warm_sum.get("lambda_time_s"),
            "cost_usd": warm_sum.get("estimated_cost_usd"),
        },
    }
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out, indent=2))
    print(f"\ncold fit:  {out['cold']['fit']}")
    print(f"warm fit:  {out['warm']['fit']}")
    print(f"wrote {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
