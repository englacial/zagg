"""Read out a windowed-vs-unwindowed measurement run (issue #586 phase 1).

Given one or more hive store roots — typically the unwindowed BASELINE arm and
the WINDOWED arm of the same order-6 cell, built with
``tools/configs/atl03_windowed_measure.yaml`` — print, per store:

- the fleet numbers off the run parquets (``stats_*.parquet`` at the root):
  units, shards, windows per shard, ``duration_s`` and ``max_memory_mb``
  quantiles, timeouts/errors, GB-seconds;
- the object numbers off the tree: leaves, objects and bytes per shard (one
  LIST per shard node; capped by ``--max-shards``);
- the ladder numbers off the staged sweep record (``sweep_stats_*_stages.json``):
  per-stage ``icechunk_commits`` / ``icechunk_rebases`` / ``icechunk_commit_s``,
  objects written.

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
import re
from collections import defaultdict

import numpy as np

_RUN_PARQUET = re.compile(r"^stats_.*\.parquet$")
_STAGE_RECORD = re.compile(r"^sweep_stats_.*_stages\.json$")
_QUANTILES = (0.5, 0.9, 1.0)


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
    """Every ``stats_*.parquet`` at the root, all-null columns dropped (they carry no
    dtype and would only make ``concat`` warn)."""
    import pandas as pd

    return [
        pd.read_parquet(io.BytesIO(_get(store, o["path"]))).dropna(axis=1, how="all")
        for o in _root_objects(store)
        if _RUN_PARQUET.match(o["path"].rsplit("/", 1)[-1])
    ]


def _total(df, col: str):
    import pandas as pd

    if col not in df:
        return None
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def fleet_numbers(store) -> dict:
    """The run-parquet summary: every ``stats_*.parquet`` at the root, concatenated."""
    import pandas as pd

    frames = _run_frames(store)
    if not frames:
        return {"runs": 0, "units": 0}
    df = pd.concat(frames, ignore_index=True)
    ok = df[df["success"]] if "success" in df else df
    # an unwindowed unit has window None: one leaf per shard, counted as one
    per_shard = (
        ok.groupby("shard_key")["window"].nunique(dropna=False)
        if "window" in ok
        else ok.groupby("shard_key").size()
    )
    out = {
        "runs": len(frames),
        "units": int(len(df)),
        "shards": int(ok["shard_key"].nunique()),
        "windows_per_shard": _quantiles(per_shard.values),
        "duration_s": _quantiles(ok.get("duration_s", [])),
        "max_memory_mb": _quantiles(ok.get("max_memory_mb", [])),
        "errors": int((~df["success"]).sum()) if "success" in df else 0,
        "timeouts": int(df["error_class"].fillna("").str.contains("Timeout").sum())
        if "error_class" in df
        else 0,
        "gb_seconds": _total(ok, "gb_seconds"),
        "n_obs": _total(ok, "n_obs"),
    }
    for col in sorted(c for c in ok.columns if c.startswith("phase_")):
        out[col] = _quantiles(ok[col])
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


def ladder_numbers(store) -> dict:
    """Per-stage counters off the staged sweep records at the root."""
    stages = []
    for o in _root_objects(store):
        if _STAGE_RECORD.match(o["path"].rsplit("/", 1)[-1]):
            record = json.loads(_get(store, o["path"]))
            stages.extend(record.get("stages") or [])
    keys = (
        "written",
        "icechunk_commits",
        "icechunk_rebases",
        "icechunk_commit_s",
        "icechunk_refs",
        "icechunk_s",
    )
    rows = [
        {"dispatch_order": s.get("dispatch_order"), **{k: s.get(k) for k in keys}} for s in stages
    ]
    return {"stage_records": len(rows), "stages": rows}


def measure(store_root: str, *, store_kwargs: dict, max_shards: int = 64) -> dict:
    """Everything for one store; the dict the CLI prints."""
    from zagg.hive import read_manifest

    store = _store(store_root, store_kwargs)
    manifest = read_manifest(store_root, **store_kwargs) or {}
    fleet = fleet_numbers(store)
    shards = []
    if fleet.get("units"):
        import pandas as pd

        df = pd.concat(_run_frames(store), ignore_index=True)
        shards = sorted(int(k) for k in df["shard_key"].dropna().unique())
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
        ("stage rows", lambda r: _fmt(r["ladder"].get("stage_records"))),
        (
            "icechunk commits (sum)",
            lambda r: _fmt(sum(s.get("icechunk_commits") or 0 for s in r["ladder"]["stages"])),
        ),
        (
            "icechunk rebases (sum)",
            lambda r: _fmt(sum(s.get("icechunk_rebases") or 0 for s in r["ladder"]["stages"])),
        ),
        (
            "icechunk commit_s (sum)",
            lambda r: _fmt(sum(s.get("icechunk_commit_s") or 0.0 for s in r["ladder"]["stages"])),
        ),
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
