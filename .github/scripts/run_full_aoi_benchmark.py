"""Dispatch EVERY shard over an AOI and emit per-shard benchmark records (issue #202 legs 1+5).

The sibling of ``run_benchmark.py`` for **full-AOI** cost truth. ``run_benchmark.py``
dispatches ONE pinned densest shard per target (isolating code deltas from data
drift); this harness dispatches **all** shards over the target's AOI, so the
recorded numbers are the real full-AOI cost -- the input the CONUS regression
(issue #202 leg 4b) is fit against, and the leg-5 write-throughput acceptance
check for the #197 dispatch shuffle + #199 hive layout under real concurrency.

It does the same job in three steps, all runnable/validatable without AWS via
``--dry-run``:

1. **Build the shard map** over the target's AOI + temporal + cmr from the local
   full catalog (``Catalog.from_geoparquet`` -> bbox+temporal prefilter ->
   ``ShardMap.build``, ``backend="mortie"``). The AOI-mask build (issue #101),
   when the target sets ``aoi_mask: true``, is timed **separately** and recorded
   as ``aoi_mask_build_s`` -- its own number, not folded into dispatch wall.
2. **Dry-run** prints the exact dispatch plan: shard count, per-shard granule
   counts, the target function + resolved AWS account, and an a-priori
   lambda-seconds/cost estimate from granule counts. No AWS, no billing.
3. **Dispatch** all shards via ``zagg.runner.agg`` (``morton_cell=None`` ->
   every shard; ``profile=True`` for phase timings; ``max_retries=1`` so a
   failed shard is recorded as a failure, never re-fired to re-pay -- #119).
   If (and only if) the manifest sets an optional ``dispatch.expect_account``, it
   asserts the caller's AWS account matches it (STS ``get_caller_identity``) before
   any billed invoke, so a wrong-profile dispatch fails closed instead of billing
   the wrong account. Absent by default (no account pinned -- the release run
   reuses the per-merge benchmark role); a fork can opt in.

Output schema
-------------
Two JSON files. ``--out-json`` is one **run record** per target::

    {
      "target", "timestamp", "commit", "ref", "event", "pr_number",
      "aoi", "temporal": {"start","end"}, "grid_size", "grid_type",
      "aggregator", "index_backend", "aoi_mask",
      "store_layout",    # "flat"|"hive" (issue #240 phase 4)
      "parity_ok",       # flat<->hive parity verdict on a parity_with target (None elsewhere/unknown)
      "parity",          # JSON-only parity detail (shards/arrays checked, mismatches)
      "sidecar_cache",   # "cold"|"warm"|"unknown"|None -- labels a sidecar run as build vs read (issue #202)
      "parent_order", "child_order", "mortie_moc_order",
      "shard_area_km2", "memory_gb", "price_per_gb_sec", "zagg_version",
      "n_shards", "n_shards_ok", "n_shards_error", "total_obs",
      "aoi_mask_build_s", "shardmap_build_s",
      "lambda_seconds", "gb_seconds", "cost_usd",             # Lambda GB-s -- the PRIMARY cost column
      "setup_cost_usd",                                       # setup invoke's billed dollars (issue #250)
      "total_wall_s", "setup_s", "fanout_s", "finalize_s",
      "worker_max_s", "worker_median_s", "worker_pct_timeout", "max_memory_mb",
      "worker_phase_max": {"read", "index", "aggregate", "write"},  # straggler (max) s/phase (#250/#256)
      "objects_total", "objects_expected", "objects_mismatch",  # store object counts (issue #240), record-only
      "write_throughput": {                                    # leg-5 acceptance signal
        "invoke_retries_total", "invoke_throttle_shards",
        "s3_slowdown_shards", "cells_timeout"
      }
    }

``--out-shards-json`` is the flat **per-shard record** list the release plot
consumes (one row per dispatched shard, tagged with its run identity)::

    {
      "target", "commit", "index_backend", "aoi_mask", "grid_size",
      "shard_label", "shard_key", "n_granules",
      "runtime_s",            # billed worker compute (lambda_duration)
      "gb_seconds",           # runtime_s * memory_gb
      "cost_usd",             # gb_seconds * price_per_gb_sec
      "max_memory_mb",        # worker RSS high-water (issue #120)
      "wall_time_s",          # orchestrator-observed wall for this shard
      "retries",              # invoke-level transient-fault retries (throttle)
      "timeout",              # hit the function timeout
      "status_code", "error", # null on success
      "objects"               # store objects attributed to this shard (issue #240; null when unmeasured)
    }

Usage::

    AWS_PROFILE=nasa python run_full_aoi_benchmark.py \\
      --targets tests/data/benchmark/targets_full_aoi_neon.json \\
      --catalog /path/to/atl03_v007_full.parquet \\
      --store-prefix s3://sliderule-public-cors/zagg-bench/full-aoi \\
      --out-json full_aoi_metrics.json --out-shards-json full_aoi_shards.json \\
      --commit "$SHA" --ref "$REF" --event release        # add --dry-run to plan only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402
import bench_objects  # noqa: E402

from zagg.config import (  # noqa: E402
    get_coverage_moc,
    get_handoff,
    get_store_layout,
    load_config,
)
from zagg.dispatch import LAMBDA_MEMORY_GB, LAMBDA_PRICE_PER_GB_SEC  # noqa: E402
from zagg.grids import from_config  # noqa: E402

# Substrings that mark an S3 write throttle surfaced to the orchestrator (the
# worker exhausted obstore's paced 503 retry budget -- store.py issue #186).
_SLOWDOWN_MARKERS = ("SlowDown", "503", "reduce your request rate", "Rate exceeded")


def load_targets(path: str) -> tuple[dict, Path]:
    p = Path(path).resolve()
    return json.loads(p.read_text()), p.parent


def _resolve(base: Path, rel: str) -> Path:
    return (base / rel).resolve()


def _aoi_parts(geojson_path: Path):
    """``[(lats, lons), ...]`` exterior rings from an AOI GeoJSON (coverage form)."""
    import shapely
    from shapely.geometry import shape

    fc = json.loads(Path(geojson_path).read_text())
    geom = shape(fc["features"][0]["geometry"])
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    parts = [
        (np.asarray(p.exterior.coords.xy[1]), np.asarray(p.exterior.coords.xy[0])) for p in polys
    ]
    minx, miny, maxx, maxy = shapely.bounds(geom)
    return parts, (float(minx), float(miny), float(maxx), float(maxy))


def _prefilter(catalog, bbox, start: str, end: str):
    """Subset a Catalog to granules whose bbox overlaps ``bbox`` and datetime is
    in ``[start, end]``. bbox column is lat-exact / lon-conservative (a superset),
    so this cut never drops a real intersector."""
    import pyarrow.compute as pc

    from zagg.catalog.sources import Catalog

    table = catalog.table
    lon0, lat0, lon1, lat1 = bbox
    bbcol = table.column("bbox")  # struct<xmin, ymin, xmax, ymax>
    g_lon_min = pc.struct_field(bbcol, "xmin").to_numpy(zero_copy_only=False)
    g_lat_min = pc.struct_field(bbcol, "ymin").to_numpy(zero_copy_only=False)
    g_lon_max = pc.struct_field(bbcol, "xmax").to_numpy(zero_copy_only=False)
    g_lat_max = pc.struct_field(bbcol, "ymax").to_numpy(zero_copy_only=False)
    overlap = (g_lon_min <= lon1) & (g_lon_max >= lon0) & (g_lat_min <= lat1) & (g_lat_max >= lat0)
    dtc = table.column("datetime").to_numpy(zero_copy_only=False).astype("datetime64[us]")
    in_time = (dtc >= np.datetime64(start)) & (dtc <= np.datetime64(end + "T23:59:59"))
    idx = np.flatnonzero(overlap & in_time)
    meta = dict(catalog.metadata or {})
    meta.update(collection=meta.get("collection", "ATL03_007"), bbox=list(bbox))
    return Catalog(table.take(idx), meta)


def build_shardmap(target, manifest, base: Path, catalog_path: str, grid, out_path: Path):
    """Build the full-AOI shard map for a target; return (build_s, aoi_mask_s, n_shards).

    ``aoi_mask_s`` is the isolated cost of the strict-AOI mask build (issue #101),
    ``None`` when the target does not set ``aoi_mask``. The map is written to
    ``out_path`` for ``agg`` to dispatch from -- an artifact file, never
    ``tests/data/benchmark/shardmaps/`` (owned elsewhere)."""
    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import Catalog

    aoi = manifest.get("aoi", {})
    temporal = manifest.get("temporal", {})
    aoi_geojson = _resolve(base, aoi["file"])
    parts, bbox = _aoi_parts(aoi_geojson)

    catalog = Catalog.from_geoparquet(catalog_path)
    sub = _prefilter(catalog, bbox, temporal["start"], temporal["end"])

    t0 = time.perf_counter()
    sm = ShardMap.build(sub, grid, region=parts, backend="mortie", footprint="swath")
    build_s = time.perf_counter() - t0

    aoi_mask_s = None
    if target.get("aoi_mask"):
        # Time the strict-AOI per-shard mask build on its own (issue #101).
        tm = time.perf_counter()
        aoi_moc = grid.aoi_moc(parts)
        sm.aoi_mask = [[int(w) for w in grid.aoi_shard_moc(aoi_moc, int(k))] for k in sm.shard_keys]
        sm.metadata["aoi_mask"] = True
        aoi_mask_s = time.perf_counter() - tm

    sm.to_json(str(out_path))
    return build_s, aoi_mask_s, sm


def _apply_target_axes(config, target):
    """Mirror run_benchmark's per-target config knobs (index_backend / aoi_mask)."""
    backend = target.get("index_backend")
    if backend == "sidecar":
        config.data_source["index"] = {
            "backend": "sidecar",
            "on_miss": "build",
            "store": "s3://sliderule-public-cors/zagg-index/ATL03/007",
        }
    elif backend == "inline":
        config.data_source["index"] = {"backend": "inline"}
    elif backend == "hierarchical":
        config.data_source["index"] = {"backend": "hierarchical"}
    if "aoi_mask" in target:
        config.output["aoi_mask"] = bool(target["aoi_mask"])


def _sidecar_cache_state(store: str | None, sm) -> str:
    """Probe whether the sidecar cache is already warm for this shard map.

    A ``sidecar`` target runs with ``on_miss: build``, so against an EMPTY cache it
    measures the sidecar *build* (a one-time write cost), not the warm *read* the
    per-release matrix wants (issue #202: the 703 s cold-build once masqueraded as
    a read). This checks one sample granule's manifest object (``<store>/<id>.parquet``)
    and labels the run ``"warm"`` / ``"cold"`` / ``"unknown"`` so the recorded
    number is self-describing. To get a true warm number, run a warming pass first
    (cf. ``data/conus/run_conus_regression.py``, which does the explicit
    cold -> verify -> warm two-pass); this probe is the interpretability guard for
    the single-pass full-AOI harness.
    """
    if not store:
        return "unknown"
    gid = next((s[0]["id"] for s in sm.granules if s), None)
    if gid is None:
        return "unknown"
    gid = gid[:-3] if gid.endswith(".h5") else gid
    try:
        from urllib.parse import urlparse

        import boto3
        from botocore.exceptions import ClientError

        p = urlparse(store.rstrip("/"))
        key = f"{p.path.lstrip('/')}/{gid}.parquet"
        try:
            boto3.client("s3").head_object(Bucket=p.netloc, Key=key)
            return "warm"
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            return "cold" if code in ("404", "NoSuchKey", "NotFound") else "unknown"
    except Exception:
        return "unknown"


def _write_throughput(results: list[dict]) -> dict:
    retries = [int(r.get("retries") or 0) for r in results]
    slowdown = sum(
        1
        for r in results
        if r.get("error") and any(m in str(r["error"]) for m in _SLOWDOWN_MARKERS)
    )
    return {
        "invoke_retries_total": int(sum(retries)),
        "invoke_throttle_shards": int(sum(1 for x in retries if x > 0)),
        "s3_slowdown_shards": int(slowdown),
        "cells_timeout": int(sum(1 for r in results if r.get("timeout"))),
    }


def _shard_records(sm, summary, target, context, grid) -> list[dict]:
    """Flatten agg's per-cell results into per-shard records (the plot's schema)."""
    counts = {int(k): len(g) for k, g in zip(sm.shard_keys, sm.granules)}
    rows = []
    for r in summary.get("results", []):
        key = int(r.get("shard_key"))
        runtime = float(r.get("lambda_duration") or 0.0)
        gb = runtime * LAMBDA_MEMORY_GB
        body = r.get("body") or {}
        rows.append(
            {
                "target": context["target"],
                "commit": context.get("commit", ""),
                "index_backend": target.get("index_backend"),
                "aoi_mask": bool(target.get("aoi_mask")),
                "grid_size": target.get("grid_size"),
                "shard_label": grid.shard_label(key),
                "shard_key": key,
                "n_granules": r.get("granule_count", counts.get(key)),
                "runtime_s": runtime,
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
    return rows


def _apriori_estimate(counts: list[int], sec_per_granule: float) -> dict:
    """Rough lambda-seconds/cost from granule counts alone (dry-run cost guard)."""
    lam = float(sum(sec_per_granule * c + 5.0 for c in counts))  # +5 s fixed overhead/shard
    gb = lam * LAMBDA_MEMORY_GB
    return {
        "assumed_sec_per_granule": sec_per_granule,
        "est_lambda_seconds": round(lam, 1),
        "est_gb_seconds": round(gb, 1),
        "est_cost_usd": round(gb * LAMBDA_PRICE_PER_GB_SEC, 4),
    }


def _values_equal(a, b) -> bool:
    """Content equality for one array region: NaN-aware floats, vlen bytes."""
    a, b = np.asarray(a), np.asarray(b)
    if a.shape != b.shape:
        return False
    if a.dtype == object or b.dtype == object:  # vlen-bytes ragged payloads
        return all(x == y for x, y in zip(a.tolist(), b.tolist(), strict=True))
    if np.issubdtype(a.dtype, np.floating):
        return bool(np.array_equal(a, b, equal_nan=True))
    return bool(np.array_equal(a, b))


def _flat_hive_parity(flat_store, hive_store, grid, shard_keys, *, store_kwargs=None) -> dict:
    """Flat<->hive output parity over dispatched shards (issue #240 item 2).

    Same-config-modulo-layout check: for every dispatched shard, every
    per-cell array's content in the hive LEAF must equal the flat store's
    shard region (the issue #236 parity contract, here verified against the
    REAL fleet-written stores). RECORD-ONLY and never raises (espg ruling on
    PR #242: the release leg must not block on a flaky read) -- a per-shard
    read failure or content mismatch lands in ``mismatches`` (``parity_ok``
    False); a setup failure lands in ``error`` (``parity_ok`` None, unknown).
    Requires the flat sibling target to have dispatched FIRST (targets run in
    manifest order; the ``parity_with`` target is listed before the hive arm).
    """
    result: dict = {"shards_checked": 0, "arrays_checked": 0, "mismatches": []}
    try:
        import zarr

        from zagg import hive as zhive
        from zagg.store import open_store

        kwargs = store_kwargs or {}
        flat = open_store(flat_store, read_only=True, **kwargs)
        # Per-cell arrays only: the shard region of the cells axis is the
        # comparable unit (chunk-grid companions would need their own slicing;
        # none exist in the benchmark configs).
        names = [
            name
            for name, m in grid.spec().members.items()
            if tuple(m.dimension_names or ())[:1] == ("cells",)
        ]
        for key in shard_keys:
            key = int(key)
            label = grid.shard_label(key)
            try:
                leaf = open_store(zhive.shard_leaf_path(hive_store, key), read_only=True, **kwargs)
                base = int(grid.block_index(key)[0]) * grid.cells_per_shard
                for name in names:
                    path = f"{grid.group_path}/{name}"
                    a = zarr.open_array(flat, path=path, mode="r")[
                        base : base + grid.cells_per_shard
                    ]
                    b = zarr.open_array(leaf, path=path, mode="r")[:]
                    if not _values_equal(a, b):
                        result["mismatches"].append({"shard": label, "array": name})
                    result["arrays_checked"] += 1
                result["shards_checked"] += 1
            except Exception as exc:  # a missing/torn leaf is a parity finding
                result["mismatches"].append({"shard": label, "error": str(exc)})
        result["parity_ok"] = not result["mismatches"]
    except Exception as exc:  # record-only: never fail the release run
        result["error"] = str(exc)
        result["parity_ok"] = None  # unknown, not asserted-false
    return result


def _ok_shard_keys(results) -> list[int]:
    """Shard keys that completed WITH data — the ``cells_with_data`` predicate.

    Mirrors the runner's counter (status 200, no error); errored shards AND
    "no granules"/"no data" shards write no hive leaf, so parity must not
    read their absence as content divergence (review, PR #242).
    """
    return [
        int(r["shard_key"])
        for r in results
        if r.get("status_code") == 200 and not r.get("error") and r.get("shard_key") is not None
    ]


def _parity_recorded(
    name, target, store, grid, ok_shards, *, n_shards, session_targets, region
) -> dict | None:
    """Run the flat<->hive parity read-back for a ``parity_with`` target.

    Returns ``None`` when the target declares no sibling (or has no store).
    Skips with ``parity_ok: None`` + a ``skipped`` reason when the sibling was
    not dispatched this session (``--target`` subselection would otherwise
    compare against a STALE flat store from a prior release -- review,
    PR #242); the full-manifest release path always dispatches the sibling
    first (manifest order), so this gate is a no-op there. ``shards_skipped``
    records how many dispatched shards parity did not cover (errored/empty).
    """
    sibling = target.get("parity_with")
    if not (sibling and store):
        return None
    if session_targets is not None and sibling not in session_targets:
        return {
            "parity_ok": None,
            "skipped": f"sibling {sibling!r} not dispatched this session",
        }
    flat_store = f"{store.rsplit('/', 1)[0]}/{sibling}.zarr"
    parity = _flat_hive_parity(flat_store, store, grid, ok_shards, store_kwargs={"region": region})
    parity["shards_skipped"] = int(n_shards) - len(ok_shards)
    if not parity.get("parity_ok"):
        print(f"[{name}] flat<->hive parity NOT clean: {parity}", flush=True)
    return parity


def _measure_objects_recorded(
    name, config, grid, store: str, shard_keys: list[int], n_ok: int, *, region: str
) -> dict:
    """Object-count metric (issue #240), RECORD-ONLY on the release leg.

    A mismatch (or a failed LIST) must never sink the release run -- the series
    data point still lands, and the regression is visible in the recorded
    columns / rendered panel instead. The per-merge harness is the hard-fail
    tripwire (``run_benchmark.py``). Expected counts are modeled over the
    shards that completed WITH data (``n_ok``) -- an errored shard may have
    written partially, so exactness is not claimed over torn writes.
    """
    try:
        objects = bench_objects.measure_objects(
            store,
            grid=grid,
            shard_keys=shard_keys,
            n_shards=n_ok,
            store_layout=get_store_layout(config),
            coverage_moc=get_coverage_moc(config),
            region=region,
        )
    except Exception as exc:  # record-only: never fail the release run
        objects = {"objects_mismatch": f"object-count measurement failed: {exc}"}
    if objects.get("objects_mismatch"):
        print(f"[{name}] store object-count mismatch: {objects['objects_mismatch']}", flush=True)
    return objects


def _setup_cost_usd(setup_s):
    """Billed dollars of the SYNC setup path (issue #250 item 3).

    ``setup_s`` is billed (real Lambda invokes) but excluded from both
    ``total_wall_s`` and ``cost_usd`` (worker durations only), so on a flat
    store the chart's dollar figure ran ~18% low. Kept as its OWN column --
    ``cost_usd`` semantics are untouched, so the retained history stays
    comparable. Layout split (issue #252 hybrid, PR #255): on FLAT rows
    ``setup_s`` is the orchestrator wall around the sync fullsphere-template
    invoke (no billed duration in the summary, so this slightly overstates by
    the round-trip overhead); on HIVE rows it is only the preflight ping +
    the ~10 ms async Event dispatch -- the manifest write's real billed GB-s
    is fire-and-forget and unobservable from the orchestrator, so this column
    measures the sync setup-path residue only, never an invented async cost.
    None-safe (dry runs)."""
    if setup_s is None:
        return None
    return round(float(setup_s) * LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC, 6)


def _assert_account(region: str, expect_account: str | None):
    if not expect_account:
        return None
    import boto3

    ident = boto3.client("sts", region_name=region).get_caller_identity()
    acct = ident["Account"]
    if acct != str(expect_account):
        raise SystemExit(
            f"caller AWS account {acct} != expected {expect_account}; refusing to dispatch "
            f"(wrong --profile? use the deployment-account SSO role)."
        )
    return acct


def run_target(
    name,
    manifest,
    base,
    *,
    catalog_path,
    store,
    region,
    function_name,
    context,
    dry_run,
    sec_per_granule,
    artifacts_dir,
    session_targets=None,
) -> tuple[dict, list[dict]]:
    from zagg import __version__ as zagg_version

    target = manifest["targets"][name]
    config = load_config(str(_resolve(base, target["config"])))
    _apply_target_axes(config, target)
    handoff = target.get("handoff") or get_handoff(config)
    grid = from_config(config)

    sm_path = Path(artifacts_dir) / f"sm_{name}.json"
    build_s, aoi_mask_s, sm = build_shardmap(target, manifest, base, catalog_path, grid, sm_path)
    counts = [len(g) for g in sm.granules]
    n_shards = len(sm.shard_keys)

    area = bench_metrics.shard_area_km2(grid)
    est = _apriori_estimate(counts, sec_per_granule)
    print(
        f"[{name}] shards={n_shards} granules(total pairs)={sum(counts)} "
        f"per-shard granules={sorted(counts)} shardmap_build={build_s:.1f}s "
        f"aoi_mask_build={aoi_mask_s} -> {function_name} @ {region}; "
        f"a-priori {est}",
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
        "temporal": manifest.get("temporal", {}),
        "grid_size": target.get("grid_size"),
        "grid_type": target.get("grid_type"),
        "aggregator": target.get("aggregator"),
        "index_backend": target.get("index_backend"),
        "aoi_mask": bool(target.get("aoi_mask")),
        # Store-layout axis (issue #240 phase 4): "flat"|"hive" from the
        # config; the renderers key the 2x2 panels on flat rows only.
        "store_layout": get_store_layout(config),
        "sidecar_cache": (
            _sidecar_cache_state(config.data_source.get("index", {}).get("store"), sm)
            if target.get("index_backend") == "sidecar" and not dry_run
            else None
        ),
        "parent_order": int(grid.parent_order),
        "child_order": int(grid.child_order),
        "mortie_moc_order": sm.metadata.get("mortie_order"),
        "shard_area_km2": area,
        "memory_gb": LAMBDA_MEMORY_GB,
        "price_per_gb_sec": LAMBDA_PRICE_PER_GB_SEC,
        "zagg_version": zagg_version,
        "n_shards": n_shards,
        "shardmap_build_s": round(build_s, 2),
        "aoi_mask_build_s": None if aoi_mask_s is None else round(aoi_mask_s, 3),
        "per_shard_granules": sorted(counts),
        "apriori_estimate": est,
    }

    if dry_run:
        run.update(
            n_shards_ok=None,
            n_shards_error=None,
            total_obs=None,
            lambda_seconds=None,
            gb_seconds=None,
            cost_usd=None,
            objects_total=None,
            objects_expected=None,
            objects_mismatch=None,
            parity_ok=None,
            parity=None,
        )
        return run, []

    from zagg.runner import agg

    summary = agg(
        config,
        catalog=str(sm_path),
        store=store,
        backend="lambda",
        morton_cell=None,  # ALL shards over the AOI
        region=region,
        function_name=function_name,
        overwrite=True,
        handoff=handoff,
        profile=True,
        max_retries=1,  # a failed shard is a failure -- never re-pay (#119)
    )
    results = summary.get("results", [])
    lam = float(summary.get("lambda_time_s") or 0.0)
    run.update(
        n_shards_ok=summary.get("cells_with_data"),
        n_shards_error=summary.get("cells_error"),
        total_obs=summary.get("total_obs"),
        lambda_seconds=round(lam, 2),
        gb_seconds=summary.get("gb_seconds"),
        cost_usd=summary.get("estimated_cost_usd"),
        total_wall_s=summary.get("wall_time_s"),
        setup_s=summary.get("setup_s"),
        setup_cost_usd=_setup_cost_usd(summary.get("setup_s")),
        fanout_s=summary.get("fanout_s"),
        finalize_s=summary.get("finalize_s"),
        worker_max_s=summary.get("worker_max_s"),
        worker_median_s=summary.get("worker_median_s"),
        worker_pct_timeout=summary.get("worker_pct_timeout"),
        max_memory_mb=summary.get("max_memory_mb"),
        # Worker per-phase straggler split (issue #250): {phase: max seconds
        # across shards}, emitted because this harness runs profile=True. The
        # dict rides the JSON record whole; the series flattens the known
        # phases (read/index/aggregate) into phase_*_s columns, null-safe.
        worker_phase_max=summary.get("worker_phase_max"),
        write_throughput=_write_throughput(results),
    )
    objects: dict = {}
    if store:
        objects = _measure_objects_recorded(
            name,
            config,
            grid,
            store,
            [int(k) for k in sm.shard_keys],
            int(summary.get("cells_with_data") or 0),
            region=region,
        )
    # flat<->hive output parity (issue #240 item 2): manifest-driven -- a target
    # carrying ``parity_with`` names its flat sibling (same config modulo
    # store_layout, already dispatched: manifest order). RECORD-ONLY; the
    # ``parity`` detail is JSON-only, ``parity_ok`` joins the retained series.
    # Compared over the shards that completed WITH data (review, PR #242): an
    # errored or granule-less shard writes no leaf, so including it would flip
    # parity_ok on an infra failure, not a content divergence.
    parity = _parity_recorded(
        name,
        target,
        store,
        grid,
        _ok_shard_keys(results),
        n_shards=n_shards,
        session_targets=session_targets,
        region=region,
    )
    run.update(
        objects_total=objects.get("objects_total"),
        objects_expected=objects.get("objects_expected"),
        objects_mismatch=objects.get("objects_mismatch"),
        parity_ok=(parity or {}).get("parity_ok"),
        parity=parity,
    )
    shard_rows = _shard_records(sm, summary, target, {**context, "target": name}, grid)
    # Per-shard object attribution rides the per-shard records (the plot's
    # schema); null when unmeasured (no store) or unattributed (errored shard).
    per_shard_objects = objects.get("objects_per_shard") or {}
    for row in shard_rows:
        row["objects"] = per_shard_objects.get(row["shard_label"])
    return run, shard_rows


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Full-AOI zagg Lambda benchmark (issue #202).")
    ap.add_argument("--targets", required=True)
    ap.add_argument(
        "--target", action="append", default=[], help="Target name (repeatable; omit for all)"
    )
    ap.add_argument("--catalog", required=True, help="Local full ATL03 stac-geoparquet catalog")
    ap.add_argument("--store-prefix", default=None, help="<prefix>/<target>.zarr output store")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--function-name", default="process-shard")
    ap.add_argument(
        "--artifacts-dir", default="./full_aoi_artifacts", help="Where built shard maps land"
    )
    ap.add_argument(
        "--sec-per-granule",
        type=float,
        default=1.7,
        help="Cold rate for the a-priori estimate (#148)",
    )
    ap.add_argument("--event", default="")
    ap.add_argument("--commit", default="")
    ap.add_argument("--ref", default="")
    ap.add_argument("--pr-number", default=None)
    ap.add_argument("--out-json", default="full_aoi_metrics.json")
    ap.add_argument("--out-shards-json", default="full_aoi_shards.json")
    ap.add_argument(
        "--dry-run", action="store_true", help="Build maps + plan only; no AWS, no billing."
    )
    args = ap.parse_args(argv)

    manifest, base = load_targets(args.targets)
    names = args.target or list(manifest["targets"].keys())
    for n in names:
        if n not in manifest["targets"]:
            raise SystemExit(f"unknown target {n!r}; have {sorted(manifest['targets'])}")

    Path(args.artifacts_dir).mkdir(parents=True, exist_ok=True)
    pr = int(args.pr_number) if args.pr_number not in (None, "", "0") else None
    context = {
        "timestamp": _utc_now_iso(),
        "commit": args.commit,
        "ref": args.ref,
        "event": args.event,
        "pr_number": pr,
    }

    # Fail closed on a wrong-account dispatch *before* building anything billable.
    # expect_account is an optional opt-in guard (absent by default -- no account
    # is pinned, so the release run reuses the per-merge role fork-friendly-ly);
    # _assert_account no-ops and returns None when it's unset.
    if not args.dry_run:
        acct = _assert_account(args.region, manifest.get("dispatch", {}).get("expect_account"))
        if acct:
            print(f"caller AWS account confirmed: {acct}", flush=True)

    runs, shards = [], []
    for name in names:
        store = f"{args.store_prefix.rstrip('/')}/{name}.zarr" if args.store_prefix else None
        run, shard_rows = run_target(
            name,
            manifest,
            base,
            catalog_path=args.catalog,
            store=store,
            region=args.region,
            function_name=args.function_name,
            context=context,
            dry_run=args.dry_run,
            sec_per_granule=args.sec_per_granule,
            artifacts_dir=args.artifacts_dir,
            # Parity gate (review, PR #242): the flat sibling must have been
            # dispatched THIS session, or the read-back would compare against
            # a stale store from a prior release.
            session_targets=set(names),
        )
        runs.append(run)
        shards.extend(shard_rows)

    Path(args.out_json).write_text(json.dumps(runs, indent=2))
    Path(args.out_shards_json).write_text(json.dumps(shards, indent=2))
    print(
        f"wrote {args.out_json} ({len(runs)} runs) and {args.out_shards_json} ({len(shards)} shards)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
