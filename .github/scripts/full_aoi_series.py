"""Append full-AOI release benchmark records to a retained parquet series (issue #202 leg 1).

The per-release sibling of ``update_series.py``. Where ``update_series.py`` retains
one row per ``(commit, target)`` **merge** point of the single densest-shard
matrix, this retains one row per ``(commit, target)`` **release** run of the
WHOLE-AOI fan-out (``run_full_aoi_benchmark.py --out-json`` -- every shard over the
AOI, not one pinned cell). It lives in its OWN parquet (``full_aoi_series.parquet``)
because the whole-AOI run record differs materially from the single-shard series
(``n_shards``, whole-AOI ``cost_usd``, ``total_wall_s``, write-throughput) and is
recorded at release cadence, not on every merge -- mixing the two would pollute the
per-merge matrix filters and confuse the two cadences.

Only ``release`` runs are retained (the locked design): a stray non-release record
can never evict a retained release point via the ``(commit, target)`` dedup.
Re-running a release (a re-dispatch of the same commit) replaces that commit's rows
rather than double-counting, so the series stays one row per ``(commit, target)``.

``plot_series.make_full_aoi_release_figure`` renders the Pages chart from this same
file (release tag on the x-axis).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# Flat on-disk schema for the full-AOI release series. Ordered for a stable
# on-disk layout; new columns append at the end. The harness's nested
# ``write_throughput`` dict is flattened to the ``wt_*`` scalar columns below, and
# the non-scalar run-record fields (temporal, per_shard_granules, apriori_estimate)
# are dropped -- they are dry-run planning aids, not charted release metrics.
FULL_AOI_COLUMNS = [
    "timestamp",
    "commit",
    "ref",
    "event",
    "pr_number",
    "target",
    "aoi",
    "aggregator",
    "grid_type",
    "grid_size",
    "index_backend",
    "aoi_mask",
    "sidecar_cache",
    "parent_order",
    "child_order",
    "mortie_moc_order",
    "n_shards",
    "n_shards_ok",
    "n_shards_error",
    "total_obs",
    "shard_area_km2",
    "aoi_mask_build_s",
    "shardmap_build_s",
    "lambda_seconds",
    "gb_seconds",
    "cost_usd",
    "total_wall_s",
    "setup_s",
    "fanout_s",
    "finalize_s",
    "worker_max_s",
    "worker_median_s",
    "worker_pct_timeout",
    "max_memory_mb",
    "memory_gb",
    "price_per_gb_sec",
    "zagg_version",
    "wt_invoke_retries_total",
    "wt_invoke_throttle_shards",
    "wt_s3_slowdown_shards",
    "wt_cells_timeout",
]

# run-record write_throughput key -> flat column name.
_WT_MAP = {
    "invoke_retries_total": "wt_invoke_retries_total",
    "invoke_throttle_shards": "wt_invoke_throttle_shards",
    "s3_slowdown_shards": "wt_s3_slowdown_shards",
    "cells_timeout": "wt_cells_timeout",
}

# Nested / planning-only run-record fields that don't belong in the flat series.
_DROP_KEYS = ("temporal", "per_shard_granules", "apriori_estimate")


def flatten_record(record: dict) -> dict:
    """Flatten one run record's nested ``write_throughput`` into ``wt_*`` scalars.

    Missing ``write_throughput`` (e.g. a dry-run record) yields null ``wt_*`` cells
    rather than raising, so a malformed record degrades gracefully. The non-scalar
    planning fields are dropped so the reindex to :data:`FULL_AOI_COLUMNS` produces
    a clean flat frame.
    """
    r = dict(record)
    wt = r.pop("write_throughput", None) or {}
    for src, col in _WT_MAP.items():
        r[col] = wt.get(src)
    for k in _DROP_KEYS:
        r.pop(k, None)
    return r


def records_to_frame(records: list[dict]) -> pd.DataFrame:
    """Build a column-stable DataFrame from full-AOI run records."""
    df = pd.DataFrame([flatten_record(r) for r in records])
    # Reindex to the canonical schema so the parquet columns never reorder or
    # silently drop/add as the record dict evolves.
    return df.reindex(columns=FULL_AOI_COLUMNS)


def load_series(path: str | Path) -> pd.DataFrame:
    """Load the existing full-AOI series, or an empty column-stable frame if absent."""
    p = Path(path)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=FULL_AOI_COLUMNS)


def append_records(existing: pd.DataFrame, records: list[dict]) -> pd.DataFrame:
    """Append records, replacing any prior rows for the same ``(commit, target)``.

    Keeping the last write makes a release re-run idempotent instead of duplicating
    a point in the plotted history.
    """
    new = records_to_frame(records)
    # Avoid concat with an all-empty frame (pandas FutureWarning on dtype union).
    combined = new if existing.empty else pd.concat([existing, new], ignore_index=True)
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["commit", "target"], keep="last").reset_index(
            drop=True
        )
    return combined.reset_index(drop=True)


def save_series(df: pd.DataFrame, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Append full-AOI release benchmark records to the parquet series."
    )
    parser.add_argument("--series", required=True, help="Path to the retained full-AOI parquet")
    parser.add_argument("--records", required=True, help="Records JSON from run_full_aoi_benchmark")
    args = parser.parse_args(argv)

    records = json.loads(Path(args.records).read_text())
    if not isinstance(records, list):
        raise SystemExit("records JSON must be a list of record objects")

    # Only release runs are retained (the locked design). Enforce it at the
    # boundary so a stray non-release record can never evict a retained release
    # point via the (commit, target) dedup. Report drops -- a silent skip would
    # read as "stored".
    retained = [r for r in records if r.get("event") == "release"]
    dropped = len(records) - len(retained)
    if dropped:
        print(f"skipping {dropped} non-release record(s); only release runs are retained")

    existing = load_series(args.series)
    updated = append_records(existing, retained)
    save_series(updated, args.series)
    print(
        f"full-aoi series: {len(existing)} -> {len(updated)} rows "
        f"({len(retained)} records) -> {args.series}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
