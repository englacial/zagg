"""Append benchmark records to the retained parquet series (issue #110).

The retained history lives as a single parquet file on a data branch (compressed
columnar; arrow is already a core zagg dep). Only **merge** runs are retained --
pre-merge PR runs are reported as an ephemeral comment and dropped (too noisy
while chasing a regression). This module is the read/append/write core plus a
thin CLI; ``plot_series.py`` renders the GitHub Pages charts from the same file.

Re-running a merge (a re-dispatch of the same commit) replaces that commit's rows
rather than double-counting, so the series stays one row per (commit, target).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402


def records_to_frame(records: list[dict]) -> pd.DataFrame:
    """Build a column-stable DataFrame from benchmark records."""
    df = pd.DataFrame(records)
    # Reindex to the canonical schema so the parquet columns never reorder or
    # silently drop/add as the record dict evolves.
    return df.reindex(columns=bench_metrics.RECORD_COLUMNS)


def load_series(path: str | Path) -> pd.DataFrame:
    """Load the existing series, or an empty column-stable frame if absent."""
    p = Path(path)
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame(columns=bench_metrics.RECORD_COLUMNS)


def append_records(existing: pd.DataFrame, records: list[dict]) -> pd.DataFrame:
    """Append records, replacing any prior rows for the same (commit, target).

    Keeping the last write makes a merge re-run idempotent instead of duplicating
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
    parser = argparse.ArgumentParser(description="Append benchmark records to the parquet series.")
    parser.add_argument("--series", required=True, help="Path to the retained parquet series")
    parser.add_argument("--records", required=True, help="Records JSON from run_benchmark")
    args = parser.parse_args(argv)

    records = json.loads(Path(args.records).read_text())
    if not isinstance(records, list):
        raise SystemExit("records JSON must be a list of record objects")

    # Only merge runs are retained (the locked design). Enforce it at the boundary
    # so a stray non-merge record can never evict a retained merge point via the
    # (commit, target) dedup. Report drops -- a silent skip would read as "stored".
    retained = [r for r in records if r.get("event") == "merge"]
    dropped = len(records) - len(retained)
    if dropped:
        print(f"skipping {dropped} non-merge record(s); only merge runs are retained")

    existing = load_series(args.series)
    updated = append_records(existing, retained)
    save_series(updated, args.series)
    print(
        f"series: {len(existing)} -> {len(updated)} rows ({len(retained)} records) -> {args.series}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
