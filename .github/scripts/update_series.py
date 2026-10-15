"""Append benchmark records to the retained parquet series (issue #110).

The retained history lives as a single parquet file on a data branch (compressed
columnar; arrow is already a core zagg dep). Only **merge** runs are retained --
pre-merge PR runs are reported as an ephemeral comment and dropped (too noisy
while chasing a regression). This module is the read/append/write core plus a
thin CLI; ``plot_series.py`` renders the GitHub Pages charts from the same file.

This is also where a run's records are judged RETAINABLE (issue #365): once the
merge job appends whatever the benchmark's tripwires concluded, rejecting a junk
measurement becomes this module's job rather than the job ordering's. That
workflow gate is a later phase and is NOT on this branch -- today the append
still only runs when the tripwires passed, so the filter in :func:`main` is the
precondition for that change, inert until the gate lands.

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

    Appending NOTHING returns the series unchanged, so the unconditional
    :func:`save_series` that follows rewrites it byte-identically and the data
    branch sees no commit: concat against an all-null frame would instead widen
    every integer column to float (issue #365). That path stops being rare once
    the workflow gate lands -- the retention filter in :func:`main` drops an
    unusable run's records, so a silently OOM'd merge will arrive here with an
    empty list, and a run that measured nothing must not be able to change the
    dtypes of every point that came before it.
    """
    if not records:
        return existing.reset_index(drop=True)
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

    # What may be retained is decided HERE, at the retention boundary, rather than
    # by whether the benchmark job got this far (issue #365): once the merge workflow
    # appends even when a tripwire fired, a run whose metrics are junk has to be
    # rejected by this filter instead of by the job aborting before the append. The
    # workflow gate is a later phase, so until it lands this filter only ever sees
    # runs that already passed their tripwires.
    #   - non-merge (the locked design): a stray PR record must never evict a
    #     retained merge point via the (commit, target) dedup.
    #   - empty metrics (issue #145): obs=0 / null peak memory is a silent OOM, a
    #     junk point that would plot as a real dip to zero.
    # An object-count mismatch (issue #240) is NOT in this list -- that run measured
    # fine and is exactly the point worth keeping; its verdict rides the row's
    # ``objects_mismatch`` column. Every drop is reported: a silent skip reads as
    # "stored".
    retained, dropped = [], []
    for r in records:
        if r.get("event") != "merge":
            dropped.append((r.get("target"), "non-merge run"))
        elif bench_metrics.has_empty_metrics(r):
            dropped.append((r.get("target"), "empty metrics (obs=0 / no peak memory)"))
        else:
            retained.append(r)
    for target, why in dropped:
        print(f"skipping record for '{target}': {why}")

    existing = load_series(args.series)
    updated = append_records(existing, retained)
    save_series(updated, args.series)
    print(
        f"series: {len(existing)} -> {len(updated)} rows ({len(retained)} records) -> {args.series}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
