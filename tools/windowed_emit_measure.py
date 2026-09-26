"""Read out a windowed-vs-unwindowed measurement run (issue #586 phase 1).

Given one or more hive store roots — typically the unwindowed BASELINE arm and
the WINDOWED arm of the same order-6 cell, built with
``tools/configs/atl03_windowed_measure.yaml`` — print, per store:

- the fleet numbers off the run parquets (``stats_*.parquet`` at the root):
  units, shards, windows per shard, ``duration_s`` and ``max_memory_mb``
  quantiles, timeouts/errors, GB-seconds;
- the object numbers off the tree: leaves, objects and bytes per shard (one
  LIST per shard node; capped by ``--max-shards``);
- the ladder numbers off the staged sweep record (``sweep_stats_*_stages.json``,
  the newest one; each record kept apart in the JSON), summed per
  ``dispatch_order`` over its per-batch rows: ``icechunk_commits`` / ``icechunk_rebases`` / ``icechunk_commit_s``,
  objects written. Icechunk is OFF on a windowed store (spec section 11.6), so
  its stage rows carry no ``icechunk_*`` keys and the table prints ``-`` (not
  measured), not ``0``: the arms' ladders compare like for like only once
  windowed Icechunk (issue #584) lands.

Read-only: LISTs and small GETs against the stores named, nothing written.
Operator tool (the run itself is a live-AWS invoke, espg's to fire); ``--anon``
for a public bucket.

    uv run python tools/windowed_emit_measure.py s3://b/p/_measure_baseline.zarr \\
        s3://b/p/_measure_windowed.zarr --max-shards 64 [--json out.json]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
from collections import defaultdict

import numpy as np

logger = logging.getLogger(__name__)
_RUN_PARQUET = re.compile(r"^stats_.*\.parquet$")
_STAGE_RECORD = re.compile(r"^sweep_stats_.*_stages\.json$")
_QUANTILES = (0.5, 0.9, 1.0)
# the dispatcher's timeout ``error_class`` shapes: sync "Lambda timeout" and the
# async poll deadline "worker timed out, was OOM-killed, or crashed ..."
_TIMEOUT = r"timeout|timed out"


def _store(store_root: str, store_kwargs: dict):
    from zagg.store import open_object_store

    return open_object_store(store_root, **store_kwargs)


def _root_objects(store) -> list[dict]:
    import obstore

    return list(obstore.list_with_delimiter(store)["objects"])


def _get(store, key: str) -> bytes:
    import obstore

    return bytes(obstore.get(store, key).bytes())


def _quantiles(values) -> dict:
    arr = np.asarray([v for v in values if v is not None and not np.isnan(v)], dtype=float)
    if arr.size == 0:
        return {f"p{int(q * 100)}": None for q in _QUANTILES}
    return {f"p{int(q * 100)}": float(np.quantile(arr, q)) for q in _QUANTILES}


def _run_frames(store) -> list:
    """Every ``stats_*.parquet`` at the root, one frame each."""
    import pandas as pd

    return [
        pd.read_parquet(io.BytesIO(_get(store, o["path"])), engine="fastparquet")
        for o in _root_objects(store)
        if _RUN_PARQUET.match(o["path"].rsplit("/", 1)[-1])
    ]


def _concat(frames):
    """The run frames as one, ``window`` kept even when all-null (a baseline run).

    pandas 2.x warns that all-NA columns will count toward the result dtype;
    the columns are kept (the ``window`` one carries the unwindowed ``None``),
    so only that warning is silenced, scoped to this concat.
    """
    import warnings

    import pandas as pd

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="The behavior of DataFrame concatenation with empty or all-NA"
        )
        df = pd.concat([f.assign(_run=i) for i, f in enumerate(frames)], ignore_index=True)
    if "window" not in df:
        df["window"] = None
    return df


def _total(df, col: str):
    import pandas as pd

    if col not in df:
        return None
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _invokes(df):
    """One row per INVOKE: a bulk multi-window shard unit (issue #586 phase 2)
    writes one run-record row per emitted leaf, each carrying the invoke's
    ``duration_s`` / ``max_memory_mb`` / ``gb_seconds`` / ``n_obs_read`` /
    ``phase_read`` and ``unit_windows`` set, so those rows collapse to the
    first per ``(run, shard_key)`` — except the per-window phases (every
    ``phase_*`` but ``phase_read``: index, aggregate, write, hash, column, the
    spill counters), which are each leaf's own and SUM to the invoke's. Every
    other row is its own invoke."""
    import pandas as pd

    if "unit_windows" not in df:
        return df
    bulk = df["unit_windows"].notna()
    rows, key = df[bulk], ["_run", "shard_key"]
    head = rows.drop_duplicates(subset=key).set_index(key)
    summed = [c for c in rows.columns if c.startswith("phase_") and c != "phase_read"]
    if summed:
        numeric = rows[key].join(rows[summed].apply(pd.to_numeric, errors="coerce"))
        head[summed] = numeric.groupby(key)[summed].sum(min_count=1)
    return pd.concat([df[~bulk], head.reset_index()], ignore_index=True)


def fleet_numbers(store) -> dict:
    """The run-parquet summary: every ``stats_*.parquet`` at the root, concatenated.

    Rows are leaves (one per emitted leaf); the per-invoke numbers
    (``units``, the duration / memory quantiles, GB-seconds) read through
    :func:`_invokes`, so a bulk multi-window run is not counted N times."""
    frames = _run_frames(store)
    if not frames:
        return {"runs": 0, "units": 0}
    df = _concat(frames)
    ok = df[df["success"]] if "success" in df else df
    ok_invokes = _invokes(ok)
    # distinct windows per shard, None (unwindowed) counting as one: a re-run of
    # the same (shard, window) unit is one leaf, not two
    per_shard = ok.groupby("shard_key")["window"].nunique(dropna=False)
    out = {
        "runs": len(frames),
        "units": int(len(_invokes(df))),
        "shards": int(ok["shard_key"].nunique()),
        "windows_per_shard": _quantiles(per_shard.values),
        "duration_s": _quantiles(ok_invokes.get("duration_s", [])),
        "max_memory_mb": _quantiles(ok_invokes.get("max_memory_mb", [])),
        "errors": int((~df["success"]).sum()) if "success" in df else 0,
        "timeouts": int(df["error_class"].fillna("").str.contains(_TIMEOUT, case=False).sum())
        if "error_class" in df
        else 0,
        "gb_seconds": _total(ok_invokes, "gb_seconds"),
        "n_obs": _total(ok, "n_obs"),
    }
    for col in sorted(c for c in ok.columns if c.startswith("phase_")):
        out[col] = _quantiles(ok_invokes[col])
    return out


def _is_leaf(top: str, decimal: str) -> bool:
    """``{id}.zarr`` / ``{id}_{window}.zarr`` of THIS shard; a leaf column
    (``all.pyramid.zarr`` / ``{window}.pyramid.zarr``) or any other object at the
    node is a sibling."""
    from zagg.windows import split_leaf_name

    if not top.endswith(".zarr") or top.endswith(".pyramid.zarr"):
        return False
    try:
        return split_leaf_name(top)[0] == decimal
    except ValueError:
        return False


def object_numbers(store, store_root: str, shard_keys, max_shards: int) -> dict:
    """Leaves, objects and bytes per shard node (one LIST each), plus the siblings
    (leaf columns, overview objects) summed apart by name."""
    import obstore

    from zagg.hive import shard_leaf_path

    leaves, objects, bytes_, siblings = [], [], [], defaultdict(int)
    for shard in list(shard_keys)[:max_shards]:
        leaf = shard_leaf_path(store_root, shard)
        node, base = leaf.rsplit("/", 1)
        node, decimal = node + "/", base.removesuffix(".zarr")
        rel = node[len(store_root.rstrip("/")) + 1 :]
        per_leaf: dict = defaultdict(lambda: [0, 0])
        for batch in obstore.list(store, rel):
            for o in batch:
                top = o["path"][len(rel) :].split("/", 1)[0]
                if _is_leaf(top, decimal):
                    per_leaf[top][0] += 1
                    per_leaf[top][1] += o["size"]
                else:
                    siblings[top.split(".zarr")[0] if ".zarr" in top else top] += o["size"]
        leaves.append(len(per_leaf))
        objects.append(sum(v[0] for v in per_leaf.values()))
        bytes_.append(sum(v[1] for v in per_leaf.values()))
    return {
        "shards_listed": len(leaves),
        "leaves_per_shard": _quantiles(leaves),
        "objects_per_shard": _quantiles(objects),
        "bytes_per_shard": _quantiles(bytes_),
        "total_leaf_bytes": int(sum(bytes_)),
        "sibling_bytes": dict(sorted(siblings.items())),
    }


_LADDER_KEYS = (
    "written",
    "icechunk_commits",
    "icechunk_rebases",
    "icechunk_commit_s",
    "icechunk_refs",
    "icechunk_s",
)


def _per_order(rows: list) -> list:
    """Stage rows summed per ``dispatch_order``: under ``--backend lambda`` the
    finisher writes one row per BATCH, so an order that fanned out to N batches
    has N rows. A counter no row carries stays ``None`` (not measured)."""
    by_order: dict = {}
    for row in rows:
        agg = by_order.setdefault(
            row.get("dispatch_order"), {"batches": 0, **dict.fromkeys(_LADDER_KEYS)}
        )
        agg["batches"] += 1
        for k in _LADDER_KEYS:
            if row.get(k) is not None:
                agg[k] = (agg[k] or 0) + row[k]
    return [
        {"dispatch_order": order, **agg}
        for order, agg in sorted(by_order.items(), key=lambda kv: (kv[0] is None, kv[0] or 0))
    ]


def ladder_numbers(store) -> dict:
    """Per-``dispatch_order`` counters off the staged sweep records at the root.

    Each ``sweep_stats_*_stages.json`` record is kept apart by name (a re-sweep
    writes another; mixing them would double-count). ``stages`` / ``batch_rows``
    are the NEWEST record's (the names are timestamp-first, so the last sorted);
    ``records`` holds every record's per-order rows.
    """
    names = sorted(
        o["path"] for o in _root_objects(store) if _STAGE_RECORD.match(o["path"].rsplit("/", 1)[-1])
    )
    records, rows = {}, []
    for name in names:
        rows = json.loads(_get(store, name)).get("stages") or []
        records[name.rsplit("/", 1)[-1]] = _per_order(rows)
    latest = next(reversed(records), None)
    return {
        "record": latest,
        "batch_rows": len(rows),
        "stages": records.get(latest, []),
        "records": records,
    }


def _listable_shards(frames) -> list[int]:
    """The shard keys to LIST: successful rows only (a failed shard has no leaf and
    would read as an empty one), read frame by frame (never through a concat that
    could coerce exact keys to float), skipping — with a warning, as
    ``zagg.sweep.discover_leaves`` does — keys that cannot name a node: a float
    key that is fractional or past 2^53 (inexact, a pre-issue-#300 parquet) and
    the negative stale-worker sentinel (``-1``)."""
    keys, skipped = set(), 0
    for df in frames:
        ok = df[df["success"]] if "success" in df else df
        for key in ok["shard_key"].dropna().unique():
            if isinstance(key, float) and (key != int(key) or key >= 2**53) or int(key) < 0:
                skipped += 1
                continue
            keys.add(int(key))
    if skipped:
        logger.warning(f"skipped {skipped} shard key(s) that name no node (float/negative)")
    return sorted(keys)


def measure(store_root: str, *, store_kwargs: dict, max_shards: int = 64) -> dict:
    """Everything for one store; the dict the CLI prints."""
    from zagg.hive import read_manifest

    store = _store(store_root, store_kwargs)
    manifest = read_manifest(store_root, **store_kwargs) or {}
    fleet = fleet_numbers(store)
    shards = _listable_shards(_run_frames(store)) if fleet.get("units") else []
    return {
        "store": store_root,
        "schedule": ((manifest.get("temporal") or {}).get("schedule")) or "none",
        "shard_order": manifest.get("shard_order"),
        "fleet": fleet,
        "objects": object_numbers(store, store_root, shards, max_shards),
        "ladder": ladder_numbers(store),
    }


def _fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, dict):
        return " / ".join(_fmt(x) for x in v.values())
    if isinstance(v, float):
        return f"{v:,.1f}"
    return f"{v:,}"


def _stage_sum(result: dict, key: str):
    """``key`` summed over the newest record's per-order rows; ``None`` (printed
    ``-``) when no row carries it — a windowed store's ladder runs without Icechunk."""
    values = [s[key] for s in result["ladder"]["stages"] if s.get(key) is not None]
    return sum(values) if values else None


def print_table(results: list[dict]) -> None:
    rows = [
        ("schedule", lambda r: r["schedule"]),
        (
            "runs / units / shards",
            lambda r: (
                f"{r['fleet'].get('runs')} / {r['fleet'].get('units')} / {r['fleet'].get('shards')}"
            ),
        ),
        ("windows per shard p50/p90/max", lambda r: _fmt(r["fleet"].get("windows_per_shard"))),
        ("duration_s p50/p90/max", lambda r: _fmt(r["fleet"].get("duration_s"))),
        ("max_memory_mb p50/p90/max", lambda r: _fmt(r["fleet"].get("max_memory_mb"))),
        (
            "errors / timeouts",
            lambda r: f"{r['fleet'].get('errors')} / {r['fleet'].get('timeouts')}",
        ),
        ("GB-seconds", lambda r: _fmt(r["fleet"].get("gb_seconds"))),
        ("leaves per shard p50/p90/max", lambda r: _fmt(r["objects"].get("leaves_per_shard"))),
        ("objects per shard p50/p90/max", lambda r: _fmt(r["objects"].get("objects_per_shard"))),
        ("bytes per shard p50/p90/max", lambda r: _fmt(r["objects"].get("bytes_per_shard"))),
        ("total leaf bytes", lambda r: _fmt(r["objects"].get("total_leaf_bytes"))),
        (
            "sweep records / orders / batches",
            lambda r: (
                f"{len(r['ladder']['records'])} / {len(r['ladder']['stages'])}"
                f" / {r['ladder']['batch_rows']}"
            ),
        ),
        ("icechunk commits (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_commits"))),
        ("icechunk rebases (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_rebases"))),
        ("icechunk commit_s (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_commit_s"))),
    ]
    width = max(len(r["store"]) for r in results)
    print(f"{'':32s}" + "".join(f"{r['store']:>{width + 2}s}" for r in results))
    for label, fn in rows:
        print(f"{label:32s}" + "".join(f"{fn(r):>{width + 2}s}" for r in results))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("store_root", nargs="+", help="hive store root(s), baseline first")
    parser.add_argument("--max-shards", type=int, default=64, help="shard nodes to LIST per store")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--anon", action="store_true", help="anonymous access (public buckets)")
    parser.add_argument("--json", default=None, help="also write the full result here")
    args = parser.parse_args(argv)
    store_kwargs: dict = {"region": args.region}
    if args.anon:
        store_kwargs["skip_signature"] = True
    results = [
        measure(root, store_kwargs=store_kwargs, max_shards=args.max_shards)
        for root in args.store_root
    ]
    print_table(results)
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(results, fh, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
