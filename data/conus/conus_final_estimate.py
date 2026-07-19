"""Consolidated CONUS cost estimate, both orders, with 95% intervals.

Assembles the measured **0.36.0** stratified 25-shard regressions into the
full-CONUS dollar table the estimate doc reports. Both orders were re-run on
``process-shard-4096-disk`` (4 GB RAM + ephemeral spill disk), **hive + sidecar +
spill**, in two passes per order:

  * **cold cache (v035)** = pre-store-cache-fix (#287/#288), cold sidecar cache.
    This is the realistic first-pass read cost.
  * **warm cache + fix (v036)** = warm sidecar cache with the #288 store-cache
    fix. This is the current-code operating point and the headline number.

Both passes use the same ``sidecar`` backend / ``hive`` layout / ``spill``
streaming -- the only axis that moves between them is cache warmth plus the #288
fix, so the v035->v036 delta isolates the store-cache effect (small at CONUS's
~80-210-granule scale; decisive at the 88S pole, a different regime -- ref #148).

Each (order, pass) fit is applied to that order's full CONUS per-shard
granule-count distribution and carries a 95% interval propagating the OLS
parameter covariance (systematic, correlated across shards -- the dominant term)
plus the per-shard residual scatter (independent, averages down over N). See
``estimate_with_ci.py`` for the interval math.

The input result files are flat lists of per-shard records under
``data/conus/results/`` (``conus_o{8,9}_v0{35,36}.json``).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

HERE = Path(__file__).parent
RES = HERE / "results"
LAMBDA_MEMORY_GB = 4.0
LAMBDA_PRICE_PER_GB_SEC = 0.0000133334  # current arm64 compute rate
# Ephemeral /tmp on the ``-disk`` worker: the 4096-disk variant provisions
# tmp_mb=6144, of which the first 512 MB is free, so billable_gb = (6144-512)/1024
# = 5.5 GB at the arm64 ephemeral rate. Mirrors production's per-shard
# ``ephemeral_cost_usd`` (verified exact against the v036 records); constants kept
# local here because .github/scripts is out of scope for this branch.
LAMBDA_EPHEMERAL_PRICE_PER_GB_SEC = 0.0000000309
EPHEMERAL_BILLABLE_GB = (6144 - 512) / 1024  # 5.5 GB
Z95 = 1.959964

# order -> full CONUS per-shard granule-count table
COUNTS = {
    8: HERE / "conus_shard_granule_counts_o8.parquet",
    9: HERE / "conus_shard_granule_counts.parquet",
}

# (order, pass) -> flat per-shard results list (0.36.0 stratified 25-shard run)
FILES = {
    (8, "cold"): RES / "conus_o8_v035.json",
    (8, "warm"): RES / "conus_o8_v036.json",
    (9, "cold"): RES / "conus_o9_v035.json",
    (9, "warm"): RES / "conus_o9_v036.json",
}


def _points(rows):
    """(granules, runtime_s) arrays over the succeeded shards of a flat list."""
    ok = [
        (r["n_granules"], r["runtime_s"])
        for r in rows
        if not r.get("error") and r.get("runtime_s") and r.get("n_granules")
    ]
    return (
        np.array([x[0] for x in ok], float),
        np.array([x[1] for x in ok], float),
    )


def _fit_ci(g, t, n_shards, g_total):
    (slope, intercept), cov = np.polyfit(g, t, 1, cov=True)
    resid = t - (slope * g + intercept)
    dof = max(len(g) - 2, 1)
    s_resid = float(np.sqrt((resid**2).sum() / dof))
    ss_tot = float(((t - t.mean()) ** 2).sum())
    r2 = 1 - float((resid**2).sum()) / ss_tot if ss_tot else None
    total_lam = slope * g_total + intercept * n_shards
    jac = np.array([g_total, n_shards], float)
    std_lam = float(np.sqrt(jac @ cov @ jac + n_shards * s_resid**2))
    # Full bill on the -disk worker = compute (RAM GB-s) + ephemeral (/tmp GB-s).
    # Both scale linearly with the extrapolated lambda-seconds, so the CI (from
    # std_lam) carries through at the combined per-lambda-second rate.
    compute_per_lam = LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC
    ephemeral_per_lam = EPHEMERAL_BILLABLE_GB * LAMBDA_EPHEMERAL_PRICE_PER_GB_SEC
    to_usd = compute_per_lam + ephemeral_per_lam
    compute_cost = total_lam * compute_per_lam
    ephemeral_cost = total_lam * ephemeral_per_lam
    cost, half = total_lam * to_usd, Z95 * std_lam * to_usd
    return {
        "slope": round(float(slope), 4),
        "intercept": round(float(intercept), 2),
        "r2": round(r2, 3) if r2 is not None else None,
        "resid_std_s": round(s_resid, 1),
        "n_pts": len(g),
        "lambda_s": float(total_lam),
        "gb_seconds": float(total_lam) * LAMBDA_MEMORY_GB,
        "compute_cost": compute_cost,
        "ephemeral_cost": ephemeral_cost,
        "ephemeral_pct_of_compute": 100 * ephemeral_cost / compute_cost if compute_cost else None,
        "cost": cost,
        "ci": half,
        "lo": cost - half,
        "hi": cost + half,
        "pct": 100 * half / cost if cost else None,
    }


def _dist(order):
    c = np.asarray(pq.read_table(str(COUNTS[order])).column("n_granules").to_pylist(), int)
    return int(len(c)), float(c.sum())


def _envelope(rows):
    """Max per-shard runtime/wall + peak RSS across the measured shards.

    ``setup_s`` (per-worker warmup) is reported informationally: it is the only
    setup-like field the records carry -- there is no separate template/dispatch
    invoke here, so the dispatch/template-emission phase is entirely outside this
    per-shard cost basis (which fits ``runtime_s`` = worker read+agg+write only).
    """
    ok = [r for r in rows if not r.get("error")]
    setups = [r.get("setup_s") or 0 for r in ok]
    return {
        "max_runtime_s": max((r["runtime_s"] for r in ok), default=None),
        "max_wall_s": max((r.get("dispatch_wall_s") or 0 for r in ok), default=None),
        "max_rss_mb": max((r.get("max_memory_mb") or 0 for r in ok), default=None),
        "max_granules": max((r["n_granules"] for r in ok), default=None),
        "mean_setup_s": round(sum(setups) / len(setups), 2) if setups else None,
        "max_setup_s": round(max(setups), 2) if setups else None,
        "n_ok": len(ok),
        "n_err": sum(1 for r in rows if r.get("error")),
    }


def main():
    loaded = {}
    for k, f in FILES.items():
        if f.exists():
            loaded[k] = json.loads(f.read_text())
        else:
            print(f"MISSING: {f.name}")

    out = {}
    for order_i, tag in [(8, "o8"), (9, "o9")]:
        n_shards, g_total = _dist(order_i)
        rec = {"n_shards": n_shards, "g_total": int(g_total)}
        for pass_, key in (("cold", "cold_v035"), ("warm", "warm_v036")):
            rows = loaded.get((order_i, pass_))
            if not rows:
                continue
            g, t = _points(rows)
            rec[key] = _fit_ci(g, t, n_shards, g_total)
            rec[key + "_env"] = _envelope(rows)
        # store-cache-fix effect on the full-CONUS total
        if "cold_v035" in rec and "warm_v036" in rec:
            c0, c1 = rec["cold_v035"]["cost"], rec["warm_v036"]["cost"]
            rec["store_cache_pct"] = round(100 * (c1 - c0) / c0, 1) if c0 else None
        out[tag] = rec

    print(json.dumps(out, indent=2, default=lambda x: round(x, 2) if isinstance(x, float) else x))
    print("\n" + "=" * 82)
    print(f"{'order':6} {'pass':22} {'cost':>10} {'95% CI':>18} {'fit':>28} {'R2':>5}")
    print("-" * 82)
    for tag in ("o8", "o9"):
        r = out.get(tag, {})
        for scen, key in [("cold cache (v035)", "cold_v035"), ("warm+fix (v036)", "warm_v036")]:
            c = r.get(key)
            if not c:
                continue
            print(
                f"{tag:6} {scen:22} ${c['cost']:>8.0f} "
                f"(compute ${c['compute_cost']:.0f} + eph ${c['ephemeral_cost']:.1f} "
                f"= {c['ephemeral_pct_of_compute']:.1f}% eph) "
                f"${c['lo']:>6.0f}..${c['hi']:<6.0f}(+/-{c['pct']:.0f}%) "
                f"{c['slope']:>6.3f}/gran+{c['intercept']:>6.0f}/shard  R2={c['r2']}"
            )
        if r.get("store_cache_pct") is not None:
            print(f"       store-cache-fix effect (v035->v036): {r['store_cache_pct']:+.1f}%")
    print("\n=== time / memory envelope (900 s worker, 4 GB RAM) ===")
    for tag in ("o8", "o9"):
        for scen, key in (("cold v035", "cold_v035_env"), ("warm v036", "warm_v036_env")):
            e = out.get(tag, {}).get(key)
            if e:
                print(
                    f"  {tag} {scen}: max_runtime={e['max_runtime_s']:.0f}s "
                    f"max_wall={e['max_wall_s']:.0f}s max_rss={e['max_rss_mb']:.0f}MB "
                    f"@ {e['max_granules']} gran; setup mean {e['mean_setup_s']}s "
                    f"(max {e['max_setup_s']}s, excluded from cost); "
                    f"ok={e['n_ok']}/{e['n_ok'] + e['n_err']}"
                )
    Path(RES / "conus_final_estimate.json").write_text(json.dumps(out, indent=2))
    print(f"\nwrote {RES / 'conus_final_estimate.json'}")


if __name__ == "__main__":
    main()
