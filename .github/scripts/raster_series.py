"""Append raster release benchmark records to a retained parquet series (issue #250).

The raster sibling of ``full_aoi_series.py``. The per-release raster leg
(``run_raster_benchmark.py --out-json``) records the Sentinel-2 pipeline over
the NEON AOI once per release; its record shape differs materially from the
point-pipeline run record (per-stage work volumes + counts instead of the
worker read/index/aggregate/write phases), so it lives in its OWN parquet
(``raster_series.parquet``) -- the same reasoning that split
``full_aoi_series.parquet`` from the per-merge series.

Only ``release`` runs are retained, deduped one row per ``(commit, target)``
(a re-dispatched release replaces its rows), exactly as the sibling series do.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# Flat on-disk schema for the raster release series. Ordered for a stable
# layout; new columns append at the end. The nested run-record ``stage_max`` /
# ``stage_counts`` dicts flatten to the ``stage_*`` / ``count_*`` scalars.
RASTER_COLUMNS = [
    "timestamp",
    "commit",
    "ref",
    "event",
    "pr_number",
    "target",
    "aoi",
    "collection",
    "grid_type",
    "grid_size",
    "parent_order",
    "child_order",
    "n_shards",
    "n_shards_ok",
    "n_shards_error",
    # Datatake count on the time axis, and shard x timestep slabs written (the
    # raster analogue of an observation tally -- RasterStrategy docstring).
    "timesteps",
    "slabs_written",
    "shardmap_build_s",
    "template_s",
    "lambda_seconds",
    "gb_seconds",
    "cost_usd",
    "total_wall_s",
    "fanout_s",
    "worker_max_s",
    "worker_median_s",
    "memory_gb",
    "price_per_gb_sec",
    "zagg_version",
    # Per-stage seconds (issue #249 stage set + the write bucket, PR #256),
    # rolled up as the straggler MAX across shards. Stage seconds are WORK
    # VOLUME, not a wall decomposition: concurrent asset-samples overlap on one
    # event loop, so stage sums can exceed wall (never stack them).
    "stage_open_s",
    "stage_geometry_s",
    "stage_fetch_s",
    "stage_decode_s",
    "stage_gather_s",
    "stage_write_s",
    # Work-volume counts, SUMMED across shards (run totals).
    "count_assets",
    "count_tiles",
    "count_geom_hits",
    # Peak worker RSS in MB, max across shards (issue #250: the raster worker
    # now reports the sampled per-invocation peak, point-path parity). Null on
    # rows recorded before the worker reported it.
    "max_memory_mb",
]

# run-record stage_max key -> flat column ("write" is the handler's write
# bucket riding next to the issue #249 stage seconds).
_STAGE_MAP = {
    "open": "stage_open_s",
    "geometry": "stage_geometry_s",
    "fetch": "stage_fetch_s",
    "decode": "stage_decode_s",
    "gather": "stage_gather_s",
    "write": "stage_write_s",
}

# run-record stage_counts key -> flat column.
_COUNT_MAP = {
    "assets": "count_assets",
    "tiles": "count_tiles",
    "geom_hits": "count_geom_hits",
}

# Planning-only run-record fields dropped from the flat series.
_DROP_KEYS = ("per_shard_granules", "apriori_note")


def flatten_record(record: dict) -> dict:
    """Flatten one raster run record's nested stage dicts into scalar columns.

    Missing ``stage_max`` / ``stage_counts`` (a dry-run record, or a run whose
    workers predate the stage emission) yield null cells rather than raising,
    mirroring the sibling series' ``wt_*`` / ``phase_*`` flattens. A stage the
    worker grows later stays JSON-only until a column is appended here.
    """
    r = dict(record)
    stages = r.pop("stage_max", None) or {}
    for src, col in _STAGE_MAP.items():
        r[col] = stages.get(src)
    counts = r.pop("stage_counts", None) or {}
    for src, col in _COUNT_MAP.items():
        r[col] = counts.get(src)
    for k in _DROP_KEYS:
        r.pop(k, None)
    return r


def records_to_frame(records: list[dict]) -> pd.DataFrame:
    """Build a column-stable DataFrame from raster run records."""
    df = pd.DataFrame([flatten_record(r) for r in records])
    return df.reindex(columns=RASTER_COLUMNS)


def load_series(path: str | Path) -> pd.DataFrame:
    """Load the existing raster series, or an empty column-stable frame."""
    p = Path(path)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=RASTER_COLUMNS)


def append_records(existing: pd.DataFrame, records: list[dict]) -> pd.DataFrame:
    """Append records, replacing prior rows for the same ``(commit, target)``."""
    new = records_to_frame(records)
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
        description="Append raster release benchmark records to the parquet series."
    )
    parser.add_argument("--series", required=True, help="Path to the retained raster parquet")
    parser.add_argument("--records", required=True, help="Records JSON from run_raster_benchmark")
    args = parser.parse_args(argv)

    records = json.loads(Path(args.records).read_text())
    if not isinstance(records, list):
        raise SystemExit("records JSON must be a list of record objects")

    # Release-only retention, reported like the sibling (a silent skip would
    # read as "stored").
    retained = [r for r in records if r.get("event") == "release"]
    dropped = len(records) - len(retained)
    if dropped:
        print(f"skipping {dropped} non-release record(s); only release runs are retained")

    existing = load_series(args.series)
    updated = append_records(existing, retained)
    save_series(updated, args.series)
    print(
        f"raster series: {len(existing)} -> {len(updated)} rows "
        f"({len(retained)} records) -> {args.series}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
