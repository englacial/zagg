"""CONUS cost estimate with a confidence interval (issue #202).

The granules-per-shard -> lambda-seconds regression fits with R^2 ~= 0.73-0.79:
granule count is a *noisy* cost predictor (observation density swings ~10x across
shards). So the CONUS total is not a point value -- it carries the fit's
uncertainty. This propagates that to a 95% interval on the full-CONUS dollar
total, for a given order's measured cold/warm regression.

Two independent uncertainty sources, added in quadrature on the total
lambda-seconds ``total_lam = slope * G_total + intercept * N``:

  * **parameter** (systematic, correlated across all N shards): the OLS
    covariance of (slope, intercept), propagated as ``jac @ cov @ jac.T`` with the
    Jacobian ``jac = [G_total, N]``. This is the confidence band on the *mean*
    cost line and is the dominant term -- it does NOT average out over shards.
  * **residual scatter** (per-shard, independent): each shard's actual time
    deviates from the line by ~``s_resid``; over N independent shards the total
    picks up ``N * s_resid^2`` of variance (std grows as sqrt(N), so it averages
    down relative to the total and is near-negligible at N ~ 50k). Included for
    honesty as the prediction-interval component.

Usage::

    python data/conus/estimate_with_ci.py \
        --results data/conus/results/conus_regression_results_o8.json \
        --counts  data/conus/conus_shard_granule_counts_o8.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

LAMBDA_MEMORY_GB = 4.0
LAMBDA_PRICE_PER_GB_SEC = 0.0000133334
Z95 = 1.959964  # two-sided 95%


def _points(pass_rec: dict) -> tuple[np.ndarray, np.ndarray]:
    ok = [
        (r["n_granules"], r["runtime_s"])
        for r in pass_rec["per_shard"]
        if not r.get("error") and r.get("runtime_s") and r.get("n_granules")
    ]
    g = np.array([x[0] for x in ok], float)
    t = np.array([x[1] for x in ok], float)
    return g, t


def _fit_ci(g, t, n_shards: int, g_total: float) -> dict:
    """Full-CONUS lambda-seconds + 95% interval for one pass's (g, t) points."""
    (slope, intercept), cov = np.polyfit(g, t, 1, cov=True)  # cov scaled by residual var
    pred = slope * g + intercept
    resid = t - pred
    dof = max(len(g) - 2, 1)
    s_resid = float(np.sqrt((resid**2).sum() / dof))
    ss_tot = float(((t - t.mean()) ** 2).sum())
    r2 = 1 - float((resid**2).sum()) / ss_tot if ss_tot else None

    total_lam = slope * g_total + intercept * n_shards  # total lambda-seconds
    jac = np.array([g_total, n_shards], float)
    var_param = float(jac @ cov @ jac)  # systematic (correlated) term
    var_resid = n_shards * s_resid**2  # independent per-shard scatter
    std_lam = float(np.sqrt(var_param + var_resid))

    gb = LAMBDA_MEMORY_GB
    to_usd = gb * LAMBDA_PRICE_PER_GB_SEC
    cost = total_lam * to_usd
    half = Z95 * std_lam * to_usd
    return {
        "sec_per_granule": round(float(slope), 4),
        "intercept_s": round(float(intercept), 2),
        "r_squared": round(r2, 4) if r2 is not None else None,
        "n_points": len(g),
        "resid_std_s": round(s_resid, 1),
        "lambda_seconds": round(float(total_lam), 0),
        "gb_seconds": round(float(total_lam) * gb, 0),
        "cost_usd": round(cost, 0),
        "ci95_half_usd": round(half, 0),
        "ci95_lo_usd": round(cost - half, 0),
        "ci95_hi_usd": round(cost + half, 0),
        "ci95_pct": round(100 * half / cost, 1) if cost else None,
        "std_param_frac": round(np.sqrt(var_param) / np.sqrt(var_param + var_resid), 3),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", required=True, help="conus_regression_results_*.json")
    ap.add_argument("--counts", required=True, help="conus_shard_granule_counts_*.parquet")
    args = ap.parse_args()

    import pyarrow.parquet as pq

    counts = np.asarray(pq.read_table(args.counts).column("n_granules").to_pylist(), dtype=int)
    n_shards = int(len(counts))
    g_total = float(counts.sum())

    res = json.loads(Path(args.results).read_text())
    order = res.get("order", "?")
    out = {
        "order": order,
        "n_shards": n_shards,
        "g_total_pairs": int(g_total),
        "sidecar_write_verified": res.get("sidecar_write_verified"),
        "sidecar_store": res.get("sidecar_store"),
    }
    for pass_ in ("cold", "warm"):
        g, t = _points(res[pass_])
        out[pass_] = _fit_ci(g, t, n_shards, g_total)

    print(json.dumps(out, indent=2))
    for pass_ in ("cold", "warm"):
        c = out[pass_]
        print(
            f"o{order} {pass_:4s}: ${c['cost_usd']:.0f} +/- ${c['ci95_half_usd']:.0f} "
            f"(95%: ${c['ci95_lo_usd']:.0f}..${c['ci95_hi_usd']:.0f}, +/-{c['ci95_pct']}%) "
            f"| {c['sec_per_granule']} s/gran + {c['intercept_s']} s/shard, R2={c['r_squared']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
