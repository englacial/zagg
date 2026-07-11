"""Select <=25 CONUS o9 shards stratified by granule density (issue #202 leg 4b).

The CONUS cost regression (granules-per-shard -> lambda-seconds, fit separately
for the cold/first-run and warm/repeat passes) needs training points that span
the density range, NOT the densest 25 and NOT a random draw. This picks a
stratified sample across the per-shard granule-count distribution and writes the
dispatch plan the full-AOI harness (or ``zagg.runner.agg --morton-cell``) runs
twice per shard (cold: sidecar ``on_miss: build``; warm: re-run so reads hit the
just-written sidecars).

Cold-pass timeout guard
-----------------------
A cold/uncached pass costs ~``COLD_SEC_PER_GRANULE`` s/granule (the #148
uncached rate). To keep the densest picked shard's cold pass comfortably under
the 900 s function timeout we exclude shards above ``MAX_GRANULES`` -- reported
explicitly (the excluded dense range is a gap the regression must note, not a
silent drop). CONUS is mid-latitude (no pole convergence), so few if any shards
should exceed the cap.

Run (after ``build_conus_shardmap.py``):
``python data/conus/select_regression_shards.py``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).parent
COLD_SEC_PER_GRANULE = 1.7  # #148 uncached rate
FIXED_OVERHEAD_S = 5.0  # per-shard cold-start + setup, rough
TIMEOUT_S = 900.0
# Keep the densest cold pass comfortably under timeout: 900 / 1.7 ~= 529; hold a
# margin so a slow shard still lands well inside.
MAX_GRANULES = 400
LAMBDA_MEMORY_GB = 4.0
LAMBDA_PRICE_PER_GB_SEC = 0.0000133334


def stratified(counts, k: int) -> list[int]:
    """Indices of ~``k`` shards spread across the granule-count *value* range.

    Regression coverage wants even spread in the predictor (granule count), not
    in the CDF -- the CONUS distribution is sharply peaked (~90% of shards in a
    narrow band), so a rank/quantile pick would cluster there and leave the
    sparse low/high tails unsampled. Target ``k`` counts evenly spaced over
    ``[min, max]`` and map each to the nearest not-yet-picked shard. Deduped,
    returned low-density first."""
    counts = np.asarray(counts)
    targets = np.linspace(counts.min(), counts.max(), num=min(k, len(counts)))
    picked: list[int] = []
    used: set[int] = set()
    for tgt in targets:
        cand = int(
            np.argmin(
                np.where([i in used for i in range(len(counts))], np.inf, np.abs(counts - tgt))
            )
        )
        used.add(cand)
        picked.append(cand)
    picked.sort(key=lambda i: counts[i])
    return picked


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--counts", default=str(HERE / "conus_shard_granule_counts.parquet"))
    ap.add_argument("--k", type=int, default=25)
    ap.add_argument("--out", default=str(HERE / "conus_regression_shards.json"))
    args = ap.parse_args(argv)

    import pyarrow.parquet as pq

    table = pq.read_table(args.counts)
    labels = np.asarray(table.column("shard_label").to_pylist())
    counts = np.asarray(table.column("n_granules").to_pylist(), dtype=int)

    eligible = counts <= MAX_GRANULES
    n_excluded = int((~eligible).sum())
    excluded_range = None
    if n_excluded:
        ex = counts[~eligible]
        excluded_range = [int(ex.min()), int(ex.max())]

    el_labels = labels[eligible]
    el_counts = counts[eligible]
    idx = stratified(el_counts, args.k)
    picked_labels = [str(el_labels[i]) for i in idx]
    picked_counts = [int(el_counts[i]) for i in idx]

    # A-priori cold lambda-seconds (upper bound: warm is faster, measured live).
    cold_lam = float(sum(COLD_SEC_PER_GRANULE * c + FIXED_OVERHEAD_S for c in picked_counts))
    cold_gb = cold_lam * LAMBDA_MEMORY_GB
    # Both passes billed; warm unknown a-priori, so bound total by 2x cold.
    plan = {
        "issue": 202,
        "purpose": "CONUS density regression training shards (cold + warm passes)",
        "source_counts": args.counts,
        "k_requested": args.k,
        "k_selected": len(picked_labels),
        "max_granules_cap": MAX_GRANULES,
        "cold_sec_per_granule": COLD_SEC_PER_GRANULE,
        "timeout_s": TIMEOUT_S,
        "excluded_over_cap": {"n_shards": n_excluded, "granule_range": excluded_range},
        "density_spread": {
            "min": picked_counts[0] if picked_counts else None,
            "median": float(np.median(picked_counts)) if picked_counts else None,
            "max": picked_counts[-1] if picked_counts else None,
            "counts": picked_counts,
        },
        "apriori_cost_guard": {
            "cold_lambda_seconds": round(cold_lam, 1),
            "cold_gb_seconds": round(cold_gb, 1),
            "cold_cost_usd": round(cold_gb * LAMBDA_PRICE_PER_GB_SEC, 4),
            "both_passes_upper_bound_cost_usd": round(2 * cold_gb * LAMBDA_PRICE_PER_GB_SEC, 4),
            "n_invocations": 2 * len(picked_labels),
        },
        "shards": [
            {"shard_label": lab, "n_granules": c} for lab, c in zip(picked_labels, picked_counts)
        ],
    }
    Path(args.out).write_text(json.dumps(plan, indent=2))
    print(
        f"selected {len(picked_labels)} shards; density {plan['density_spread']['min']}.."
        f"{plan['density_spread']['max']} granules; excluded {n_excluded} over cap "
        f"(range {excluded_range}); a-priori both-pass upper-bound cost "
        f"${plan['apriori_cost_guard']['both_passes_upper_bound_cost_usd']} "
        f"({plan['apriori_cost_guard']['n_invocations']} invokes)"
    )
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
