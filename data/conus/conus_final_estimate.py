"""Consolidated CONUS cost estimate, both orders, cold+warm, with 95% intervals.

Assembles the measured 0.24.0-sharded regressions into the full-CONUS dollar
table the estimate doc reports. Per the operational split (espg):

  * **cold (first run)** = ``inline`` backend, genuinely-uncached reads, measured
    cache-independently (does not depend on the shared granule-keyed sidecar cache
    state). This is the realistic first-pass read cost.
  * **warm (repeat)** = ``sidecar`` backend, reads hit the prebuilt manifest cache.
  * **+ sidecar build** = the one-time write to populate the cache on the true
    first run, estimated from the o8 (sidecar on_miss:build cold) - (inline cold)
    delta and applied uniformly, so ``first-run total = cold(inline) + build``.

Each (order, scenario) fit is applied to that order's full CONUS per-shard
granule-count distribution and carries a 95% interval propagating the OLS
parameter covariance (systematic, correlated across shards -- the dominant term)
plus the per-shard residual scatter (independent, averages down over N). See
``estimate_with_ci.py`` for the interval math.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

HERE = Path(__file__).parent
RES = HERE / "results"
LAMBDA_MEMORY_GB = 4.0
LAMBDA_PRICE_PER_GB_SEC = 0.0000133334
Z95 = 1.959964

# order -> full CONUS per-shard granule-count table
COUNTS = {
    8: HERE / "conus_shard_granule_counts_o8.parquet",
    9: HERE / "conus_shard_granule_counts.parquet",
}


def _points(pass_rec):
    ok = [
        (r["n_granules"], r["runtime_s"])
        for r in pass_rec["per_shard"]
        if not r.get("error") and r.get("runtime_s") and r.get("n_granules")
    ]
    return (
        np.array([x[0] for x in ok], float),
        np.array([x[1] for x in ok], float),
        [r for r in pass_rec["per_shard"] if not r.get("error")],
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
    to_usd = LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC
    cost, half = total_lam * to_usd, Z95 * std_lam * to_usd
    return {
        "slope": round(float(slope), 4),
        "intercept": round(float(intercept), 2),
        "r2": round(r2, 3) if r2 is not None else None,
        "resid_std_s": round(s_resid, 1),
        "n_pts": len(g),
        "lambda_s": float(total_lam),
        "cost": cost,
        "ci": half,
        "lo": cost - half,
        "hi": cost + half,
        "pct": 100 * half / cost if cost else None,
    }


def _dist(order):
    c = np.asarray(pq.read_table(str(COUNTS[order])).column("n_granules").to_pylist(), int)
    return int(len(c)), float(c.sum())


def _envelope(pass_rec):
    """Max per-shard wall/lambda time + peak RSS across the measured shards."""
    rows = [r for r in pass_rec["per_shard"] if not r.get("error")]
    return {
        "max_runtime_s": max((r["runtime_s"] for r in rows), default=None),
        "max_wall_s": max((r.get("wall_time_s") or 0 for r in rows), default=None),
        "max_rss_mb": max((r.get("max_memory_mb") or 0 for r in rows), default=None),
        "max_granules": max((r["n_granules"] for r in rows), default=None),
        "n_ok": len(rows),
        "n_err": sum(1 for r in pass_rec["per_shard"] if r.get("error")),
    }


def main():
    files = {
        ("o8", "inline_cold"): RES / "conus_inline_cold_o8.json",
        ("o8", "sidecar"): RES / "conus_regression_results_o8.json",
        ("o9", "inline_cold"): RES / "conus_inline_cold_o9.json",
        ("o9", "sidecar"): RES / "conus_regression_results_o9warm.json",
    }
    loaded = {}
    for k, f in files.items():
        if f.exists():
            loaded[k] = json.loads(f.read_text())
        else:
            print(f"MISSING: {f.name}")

    out = {}
    for order_i, tag in [(8, "o8"), (9, "o9")]:
        n_shards, g_total = _dist(order_i)
        rec = {"n_shards": n_shards, "g_total": int(g_total)}
        # cold = inline cold pass
        ic = loaded.get((tag, "inline_cold"))
        if ic and ic.get("cold"):
            g, t, _ = _points(ic["cold"])
            rec["cold_inline"] = _fit_ci(g, t, n_shards, g_total)
            rec["cold_env"] = _envelope(ic["cold"])
        # warm = sidecar warm pass
        sc = loaded.get((tag, "sidecar"))
        if sc and sc.get("warm"):
            g, t, _ = _points(sc["warm"])
            rec["warm_sidecar"] = _fit_ci(g, t, n_shards, g_total)
            rec["warm_env"] = _envelope(sc["warm"])
        # build-cold (sidecar on_miss:build) -- for the write addend
        if sc and sc.get("cold"):
            g, t, _ = _points(sc["cold"])
            rec["build_cold"] = _fit_ci(g, t, n_shards, g_total)
            rec["build_cold_verified"] = sc.get("sidecar_write_verified")
        out[tag] = rec

    # sidecar-build write addend from o8: (build_cold) - (inline_cold)
    addend = None
    if "cold_inline" in out.get("o8", {}) and "build_cold" in out.get("o8", {}):
        addend = out["o8"]["build_cold"]["cost"] - out["o8"]["cold_inline"]["cost"]
        out["sidecar_build_addend_o8_usd"] = addend

    print(json.dumps(out, indent=2, default=lambda x: round(x, 2) if isinstance(x, float) else x))
    print("\n" + "=" * 78)
    print(f"{'order':6} {'scenario':16} {'cost':>10} {'95% CI':>18} {'fit':>28} {'R2':>5}")
    print("-" * 78)
    for tag in ("o8", "o9"):
        r = out.get(tag, {})
        for scen, key in [("cold (inline)", "cold_inline"), ("warm (sidecar)", "warm_sidecar")]:
            c = r.get(key)
            if not c:
                continue
            print(
                f"{tag:6} {scen:16} ${c['cost']:>8.0f} "
                f"${c['lo']:>6.0f}..${c['hi']:<6.0f}(+/-{c['pct']:.0f}%) "
                f"{c['slope']:>6.3f}/gran+{c['intercept']:>6.0f}/shard  {c['r2']}"
            )
    if addend is not None:
        print(f"\nsidecar-build write addend (o8, applied to first-run): +${addend:.0f}")
    print("\n=== o8 time envelope (900 s worker) ===")
    for scen in ("cold_env", "warm_env"):
        e = out.get("o8", {}).get(scen)
        if e:
            print(
                f"  o8 {scen}: max_wall={e['max_wall_s']}s max_rss={e['max_rss_mb']}MB "
                f"@ {e['max_granules']} gran; ok={e['n_ok']}/{e['n_ok'] + e['n_err']}"
            )
    Path(RES / "conus_final_estimate.json").write_text(json.dumps(out, indent=2))
    print(f"\nwrote {RES / 'conus_final_estimate.json'}")


if __name__ == "__main__":
    main()
