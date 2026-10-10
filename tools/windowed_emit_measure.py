"""Read out the schedule measurement arms (issue #602; issue #586 phases 1 and 5).

Given one or more hive store roots — the arms of one order-6 cell built with
``tools/configs/atl03_windowed_measure_{none,yearly,quarterly,monthly}.yaml``
(one body, four ``output.windowing`` blocks) — print one column per store:

- the fleet numbers off the run parquets (``stats_*.parquet`` at the root):
  units, shards, windows per shard, ``duration_s`` (the read + aggregate
  clock), ``duration_total_s`` (the billed wall, issue #589; ``-`` on a run
  whose workers predate it) and ``max_memory_mb`` quantiles per INVOKE,
  timeouts/errors, GB-seconds, whether one invoke per shard fit the wall, and
  the per-LEAF write side (every ``phase_*`` but read, over leaf rows);
- the tree numbers off ONE recursive LIST of the store: leaves per shard,
  leaf objects and bytes per shard, leaf bytes by array, **chunk occupancy**
  per leaf and summed per shard from each leaf's ``count`` object size (the
  ShardingCodec object is ``index + occupied x inner-chunk bytes``; both
  constants are derived from the leaf's ``count/zarr.json``, 16,384 + 4,100 B
  on the o9/o13 ``int32`` geometry), and the non-leaf objects and bytes by
  class (leaf columns, overviews, sidecars, root, icechunk);
- the staged sweep: the root run records (``sweep_stats_*_stages.json``, the
  newest kept apart) summed per ``dispatch_order``, plus every fleet stage run
  under ``<store>.status/run-stage-*/`` (``stage-<order>-<batch>.json`` per
  invoke and the ``finisher.json``): invokes per order, wall per invoke by unit
  kind, worker seconds, ``fold_cells_read`` per node, ``fold_peak_cells``,
  ``barrier_timed_out`` / ``short_orders``, failed units;
- the all-time accuracy leg (``windowed_emit_accuracy.py``): the all-time
  overview at one node per ladder level against a flat fold of the same leaves
  — exact ``count`` and the digests' rank error in units of 1/δ;
- cost: the leaf fan-out's billed GB-seconds over EVERY invoke, failed ones
  included (``duration_total_s x`` the invoke's memory, falling back to the
  self-reported ``gb_seconds``; a timed-out invoke with no recorded wall at
  ``FUNCTION_TIMEOUT_S`` x its memory, as Lambda bills it), the
  stage runs' (``duration_s x`` the 8 GB tier) and the finisher's, priced at
  ``zagg.dispatch.LAMBDA_PRICE_PER_GB_SEC``.

Icechunk is OFF on a windowed store (spec section 11.6), so its stage rows
carry no ``icechunk_*`` keys and the table prints ``-`` (not measured), not
``0``. Read-only: LISTs and small GETs, nothing written. Operator tool (the
runs are live-AWS invokes, espg's to fire); ``--anon`` for a public bucket.

    uv run python tools/windowed_emit_measure.py s3://b/p/_measure_none.zarr \\
        s3://b/p/_measure_yearly.zarr s3://b/p/_measure_quarterly.zarr \\
        s3://b/p/_measure_monthly.zarr [--json out.json] [--skip-accuracy]
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))  # the sibling accuracy module

logger = logging.getLogger(__name__)
_RUN_PARQUET = re.compile(r"^stats_.*\.parquet$")
_STAGE_RECORD = re.compile(r"^sweep_stats_.*_stages\.json$")
_SHARD_ID = re.compile(r"^-?[0-9]+$")
_QUANTILES = (0.5, 0.9, 1.0)
# the dispatcher's timeout ``error_class`` shapes: sync "Lambda timeout" and the
# async poll deadline "worker timed out, was OOM-killed, or crashed ..."
_TIMEOUT = r"timeout|timed out"
#: The Lambda function ceiling the "fits one invoke per shard" verdict is read against.
FUNCTION_TIMEOUT_S = 900.0
#: The stage tier: ``runner._resolve_stage_function_name`` sends every stage
#: invoke to the run family's ``-8192-disk`` variant (issue #586 interim).
STAGE_TIER_GB = 8.0
#: The issue's occupancy constants, used only when no leaf ``count/zarr.json``
#: can be read: 4,096 ``int32`` cells per inner chunk, 256 chunks per shard.
FALLBACK_CHUNK = {"bytes_per_chunk": 16_384, "index_bytes": 4_100, "chunks": 256, "dtype": None}


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


def _stats(values) -> dict:
    """mean / min / p50 / max — the occupancy shape the #584 readout used."""
    arr = np.asarray([v for v in values if v is not None], dtype=float)
    if arr.size == 0:
        return {"n": 0, "mean": None, "min": None, "p50": None, "max": None}
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "min": float(arr.min()),
        "p50": float(np.median(arr)),
        "max": float(arr.max()),
    }


# ---------------------------------------------------------------------------
# Fleet numbers (the run parquets)
# ---------------------------------------------------------------------------


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
    ``duration_s`` / ``duration_total_s`` / ``max_memory_mb`` / ``gb_seconds`` /
    ``n_obs_read`` / ``phase_read`` and ``unit_windows`` set, so those rows collapse to the
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


def _billed_gb_seconds(invokes) -> float | None:
    """Billed GB-seconds over the invokes: the record's own ``gb_seconds`` (the
    worker prices ``duration_total_s`` — or ``duration_s`` before issue #589 —
    times its memory, ``telemetry.build_record``) where it carries one, else
    that product recomputed from the row (``lambda_memory_mb``, 4096 when
    unrecorded). A failed invoke prices whatever wall it carries, except a
    timed-out one with none (the dispatcher's ``failure_record`` stamps
    ``duration_s`` 0): Lambda billed it the full :data:`FUNCTION_TIMEOUT_S`.
    ``None`` when no invoke has any of it."""
    import pandas as pd

    if len(invokes) == 0:
        return None
    col = lambda name, default=None: pd.to_numeric(  # noqa: E731
        invokes[name]
        if name in invokes
        else pd.Series([default] * len(invokes), index=invokes.index),
        errors="coerce",
    )
    wall = col("duration_total_s").where(col("duration_total_s").notna(), col("duration_s"))
    if "error_class" in invokes:
        timed_out = invokes["error_class"].fillna("").astype(str).str.contains(_TIMEOUT, case=False)
        wall = wall.where(~(timed_out & wall.fillna(0).eq(0)), FUNCTION_TIMEOUT_S)
    computed = wall * col("lambda_memory_mb").fillna(4096) / 1024.0
    recorded = col("gb_seconds")
    billed = recorded.where(recorded.notna(), computed)
    return float(billed.sum()) if billed.notna().any() else None


def fleet_numbers(store) -> dict:
    """The run-parquet summary: every ``stats_*.parquet`` at the root, concatenated.

    Rows are leaves (one per emitted leaf); the per-invoke numbers
    (``units``, the duration / memory quantiles, GB-seconds) read through
    :func:`_invokes`, so a bulk multi-window run is not counted N times; the
    per-leaf write side (``leaf_phase_*``) stays per row."""
    frames = _run_frames(store)
    if not frames:
        return {"runs": 0, "units": 0}
    df = _concat(frames)
    ok = df[df["success"]] if "success" in df else df
    ok_invokes = _invokes(ok)
    # distinct windows per shard, None (unwindowed) counting as one: a re-run of
    # the same (shard, window) unit is one leaf, not two
    per_shard = ok.groupby("shard_key")["window"].nunique(dropna=False)
    timeouts = (
        int(df["error_class"].fillna("").str.contains(_TIMEOUT, case=False).sum())
        if "error_class" in df
        else 0
    )
    wall = ok_invokes.get("duration_total_s")
    wall = wall if wall is not None and wall.notna().any() else ok_invokes.get("duration_s")
    out = {
        "runs": len(frames),
        "units": int(len(_invokes(df))),
        "shards": int(ok["shard_key"].nunique()),
        "leaves": int(len(ok)),
        "windows_per_shard": _quantiles(per_shard.values),
        "duration_s": _quantiles(ok_invokes.get("duration_s", [])),
        "duration_total_s": _quantiles(ok_invokes.get("duration_total_s", [])),
        "max_memory_mb": _quantiles(ok_invokes.get("max_memory_mb", [])),
        "errors": int((~df["success"]).sum()) if "success" in df else 0,
        "timeouts": timeouts,
        # self-reported: the successful invokes' own figure; billed: every
        # invoke, a timed-out one at the full function wall
        "gb_seconds": _total(ok_invokes, "gb_seconds"),
        "billed_gb_seconds": _billed_gb_seconds(_invokes(df)),
        "n_obs": _total(ok, "n_obs"),
        # one invoke per shard fit the function wall: no timeout, no error, and
        # the slowest invoke's billed wall under the ceiling
        "fits_one_invoke": bool(
            timeouts == 0
            and ("success" not in df or df["success"].all())
            and wall is not None
            and wall.notna().any()
            and float(wall.max()) < FUNCTION_TIMEOUT_S
        ),
    }
    for col in sorted(c for c in ok.columns if c.startswith("phase_")):
        out[col] = _quantiles(ok_invokes[col])
        if col != "phase_read":
            out[f"leaf_{col}"] = _quantiles(ok[col])  # per leaf row, not per invoke
    return out


# ---------------------------------------------------------------------------
# Tree numbers (one recursive LIST)
# ---------------------------------------------------------------------------


def _classify(path: str) -> tuple:
    """``(class, detail)`` of one store key.

    ``leaf`` -> ``(shard decimal, window, version, array-or-None, is_chunk)``;
    ``columns`` / ``overviews`` / ``sidecars`` -> the node dir and the
    basename stem; ``root`` / ``icechunk`` / ``multiscales`` -> ``None``.
    """
    from zagg.windows import split_leaf_name

    parts = path.split("/")
    if len(parts) == 1:
        return "root", None
    if parts[0] in ("icechunk", "multiscales"):
        return parts[0], None
    zarr_at = next((i for i, p in enumerate(parts) if p.endswith(".zarr")), None)
    if zarr_at is None:
        return "sidecars", ("/".join(parts[:-1]), parts[-1])
    node, base = "/".join(parts[:zarr_at]), parts[zarr_at]
    if base.endswith(".pyramid.zarr"):
        return "columns", (node, base.removesuffix(".zarr"))
    try:
        full_id, window = split_leaf_name(base)
    except ValueError:
        return "sidecars", (node, base.removesuffix(".zarr"))
    # a leaf's id IS its node path's digits (the hive node invariant); an
    # overview at the node — ``all.zarr`` / ``2019.zarr`` — is not
    if not (_SHARD_ID.match(full_id) and node.replace("/", "") == full_id):
        return "overviews", (node, base.removesuffix(".zarr"))
    rest = parts[zarr_at + 1 :]
    version = None
    if rest and rest[0].startswith("run-"):  # a versioned leaf (issue #585)
        version, rest = rest[0], rest[1:]
    # ``<cell_order>/<array>/...`` names an array; ``<cell_order>/zarr.json`` is
    # the leaf's group metadata, not an array called ``zarr.json``
    if len(rest) >= 3 and rest[0].isdigit():
        return "leaf", (full_id, window, version, rest[1], rest[2] == "c")
    return "leaf", (full_id, window, version, None, False)


def _chunk_constants(store, zarr_json_key: str | None) -> dict:
    """The ``count`` array's ShardingCodec geometry: bytes per inner chunk,
    inner chunks per shard object and the index tail (16 B per chunk + the
    crc32c), from its ``zarr.json``; the issue's constants when unreadable."""
    if zarr_json_key is None:
        logger.warning("occupancy: no leaf count/zarr.json found; using the o9/o13 int32 constants")
        return dict(FALLBACK_CHUNK, source="fallback")
    try:
        meta = json.loads(_get(store, zarr_json_key))
        outer = int(np.prod(meta["chunk_grid"]["configuration"]["chunk_shape"]))
        itemsize = np.dtype(meta["data_type"]).itemsize
        sharding = next(c for c in meta["codecs"] if c["name"] == "sharding_indexed")
        inner = int(np.prod(sharding["configuration"]["chunk_shape"]))
        chunks = outer // inner
        crc = any(c.get("name") == "crc32c" for c in sharding["configuration"]["index_codecs"])
        return {
            "bytes_per_chunk": inner * itemsize,
            "index_bytes": chunks * 16 + (4 if crc else 0),
            "chunks": chunks,
            "dtype": meta["data_type"],
            "source": zarr_json_key,
        }
    except Exception as e:
        logger.warning(f"occupancy: {zarr_json_key} unreadable ({e}); using the int32 constants")
        return dict(FALLBACK_CHUNK, source="fallback")


def _current_version(store, leaf_dir: str, versions: dict) -> str | None:
    """Of a leaf with several version dirs, the one its stamp names ``current``."""
    if len(versions) == 1:
        return next(iter(versions))
    try:
        stamp = json.loads(_get(store, f"{leaf_dir}/zarr.json"))["attributes"]["morton_hive_commit"]
        if stamp.get("current") in versions:
            return stamp["current"]
    except Exception as e:
        logger.warning(
            f"occupancy: {leaf_dir} has {len(versions)} versions, stamp unreadable ({e})"
        )
    return sorted(versions)[-1]


def tree_numbers(store, max_shards: int = 64) -> dict:
    """Everything one recursive LIST of the store says (no data read, one GET
    for the ``count`` chunk geometry, one per multi-version leaf)."""
    import obstore

    from zagg.hive import _decimal_order

    leaf_objects: dict = defaultdict(lambda: [0, 0])  # (shard, window) -> [objects, bytes]
    by_array: dict = defaultdict(int)
    count_chunks: dict = defaultdict(lambda: defaultdict(int))  # leaf -> version -> bytes
    leaf_dirs: dict = {}
    non_leaf: dict = defaultdict(lambda: {"objects": 0, "bytes": 0})
    siblings: dict = defaultdict(int)
    shard_nodes: set = set()
    count_meta_key = None
    total_objects = total_bytes = 0
    for batch in obstore.list(store):
        for o in batch:
            path, size = o["path"], o["size"]
            total_objects, total_bytes = total_objects + 1, total_bytes + size
            cls, detail = _classify(path)
            if cls != "leaf":
                non_leaf[cls]["objects"] += 1
                non_leaf[cls]["bytes"] += size
                if detail is not None:
                    siblings[detail] += size
                continue
            shard, window, version, array, is_chunk = detail
            key = (shard, window)
            leaf_objects[key][0] += 1
            leaf_objects[key][1] += size
            parts = path.split("/")
            leaf_dirs[key] = "/".join(
                parts[: parts.index(next(p for p in parts if p.endswith(".zarr"))) + 1]
            )
            shard_nodes.add("/".join(parts[: len(leaf_dirs[key].split("/")) - 1]))
            if array is None:
                by_array["(leaf metadata)"] += size
                continue
            by_array[array] += size
            if array == "count":
                if is_chunk:
                    count_chunks[key][version] += size
                elif count_meta_key is None and path.endswith("zarr.json"):
                    count_meta_key = path
    shards = sorted({s for s, _w in leaf_objects}, key=lambda d: (_decimal_order(d), d))[
        :max_shards
    ]
    kept = set(shards)
    windows = sorted({w for s, w in leaf_objects if s in kept}, key=lambda w: (w is None, w or ""))
    chunk = _chunk_constants(store, count_meta_key)
    occupied: dict = {}
    for key, versions in count_chunks.items():
        if key[0] not in kept:
            continue
        current = _current_version(store, leaf_dirs[key], versions)
        occ = (versions[current] - chunk["index_bytes"]) / chunk["bytes_per_chunk"]
        if occ < 0 or abs(occ - round(occ)) > 1e-9:
            logger.warning(
                f"occupancy: leaf {key} count object {versions[current]} B is not "
                f"index + n x {chunk['bytes_per_chunk']} B; recorded as-is"
            )
        occupied[key] = occ
    per_shard_occ = defaultdict(float)
    for (s, _w), occ in occupied.items():
        per_shard_occ[s] += occ
    by_window = defaultdict(list)
    for (_s, w), occ in occupied.items():
        by_window[w].append(occ)
    leaves_per_shard = {s: sum(1 for k in leaf_objects if k[0] == s) for s in shards}
    objects_per_shard = {s: sum(v[0] for k, v in leaf_objects.items() if k[0] == s) for s in shards}
    bytes_per_shard = {s: sum(v[1] for k, v in leaf_objects.items() if k[0] == s) for s in shards}
    sibling_bytes = defaultdict(int)
    for (node, stem), size in siblings.items():
        if node in shard_nodes:
            sibling_bytes[stem] += size
    summed = _stats(per_shard_occ.values())
    return {
        "shards": shards,
        "windows": windows,
        "objects": {
            "shards_listed": len(shards),
            "leaves": sum(leaves_per_shard.values()),
            "leaves_per_shard": _quantiles(leaves_per_shard.values()),
            "objects_per_shard": _quantiles(objects_per_shard.values()),
            "bytes_per_shard": _quantiles(bytes_per_shard.values()),
            "total_leaf_bytes": int(sum(bytes_per_shard.values())),
            "leaf_bytes_by_array": dict(sorted(by_array.items())),
            "sibling_bytes": dict(sorted(sibling_bytes.items())),
            "total_objects": total_objects,
            "total_bytes": total_bytes,
        },
        "occupancy": {
            **chunk,
            "per_leaf": _stats(occupied.values()),
            "per_leaf_fraction": (
                None
                if not occupied or not chunk["chunks"]
                else float(np.mean(list(occupied.values())) / chunk["chunks"])
            ),
            "summed_per_shard": summed,
            "full_row_ratio": (
                None
                if summed["mean"] is None or not chunk["chunks"]
                else summed["mean"] / chunk["chunks"]
            ),
            "by_window": {
                (w if w is not None else "all"): float(np.mean(v))
                for w, v in sorted(by_window.items(), key=lambda kv: kv[0] or "")
            },
        },
        "non_leaf": {k: dict(v) for k, v in sorted(non_leaf.items())},
    }


# ---------------------------------------------------------------------------
# The staged sweep: root run records and the fleet's per-invoke stage records
# ---------------------------------------------------------------------------

_LADDER_KEYS = (
    "written",
    "icechunk_commits",
    "icechunk_rebases",
    "icechunk_commit_s",
    "icechunk_refs",
    "icechunk_s",
    # Issue #586 phase 4: the (node, window) units a row ran, the worker
    # seconds it took, and what failed. A stage row predating them carries
    # none (printed ``-``).
    "window_units",
    "close_units",
    "failed",
    "duration_s",
    "fold_cells_read",
    "nodes",
)
#: Stage-row counters that are a HIGH-WATER, not a sum, across a dispatch
#: order's rows: the streamed fold's largest single block of inputs, in source
#: cells (``zagg.sweep_fold.FoldMeter``).
_LADDER_MAX_KEYS = ("fold_peak_cells",)


def _per_order(rows: list) -> list:
    """Stage rows summed per ``dispatch_order``: under ``--backend lambda`` the
    finisher writes one row per BATCH, so an order that fanned out to N batches
    has N rows. A counter no row carries stays ``None`` (not measured)."""
    by_order: dict = {}
    for row in rows:
        agg = by_order.setdefault(
            row.get("dispatch_order"),
            {"batches": 0, **dict.fromkeys((*_LADDER_KEYS, *_LADDER_MAX_KEYS))},
        )
        agg["batches"] += 1
        for k in _LADDER_KEYS:
            if row.get(k) is not None:
                agg[k] = (agg[k] or 0) + row[k]
        for k in _LADDER_MAX_KEYS:
            if row.get(k) is not None:
                agg[k] = max(agg[k] or 0, row[k])
    return [
        {"dispatch_order": order, **agg}
        for order, agg in sorted(by_order.items(), key=lambda kv: (kv[0] is None, kv[0] or 0))
    ]


def ladder_numbers(store) -> dict:
    """Per-``dispatch_order`` counters off the staged sweep records at the root.

    Each ``sweep_stats_*_stages.json`` record is kept apart by name (a re-sweep
    writes another; mixing them would double-count). ``stages`` / ``batch_rows``
    are the NEWEST record's (the names are timestamp-first, so the last sorted);
    ``records`` holds every record's per-order rows, ``barrier_timed_out`` /
    ``short_orders`` the newest record's verdict (issue #610).
    """
    names = sorted(
        o["path"] for o in _root_objects(store) if _STAGE_RECORD.match(o["path"].rsplit("/", 1)[-1])
    )
    records, rows, latest_record = {}, [], {}
    for name in names:
        latest_record = json.loads(_get(store, name))
        rows = latest_record.get("stages") or []
        records[name.rsplit("/", 1)[-1]] = _per_order(rows)
    latest = next(reversed(records), None)
    return {
        "record": latest,
        "batch_rows": len(rows),
        "stages": records.get(latest, []),
        "records": records,
        "barrier_timed_out": latest_record.get("barrier_timed_out"),
        "short_orders": latest_record.get("short_orders"),
        "run_id": latest_record.get("run_id"),
    }


def _stage_run(status, prefix: str, workers: int) -> dict:
    """One fleet stage run's records under ``<store>.status/run-stage-*/``."""
    import obstore

    names = sorted(
        o["path"].rsplit("/", 1)[-1]
        for o in obstore.list_with_delimiter(status, prefix)["objects"]
        if o["path"].endswith(".json")
    )
    with ThreadPoolExecutor(max(1, workers)) as ex:
        loaded = dict(zip(names, ex.map(lambda n: json.loads(_get(status, prefix + n)), names)))
    finisher = loaded.pop("finisher.json", None) or {}
    records = [r for n, r in loaded.items() if n.startswith("stage-")]
    orders: dict = {}
    for rec in records:
        agg = orders.setdefault(
            rec.get("dispatch"),
            {
                "invokes": 0,
                "window_invokes": 0,
                "close_invokes": 0,
                "whole_invokes": 0,
                "wall_s": [],
                "window_wall_s": [],
                "close_wall_s": [],
                "worker_s": 0.0,
                "nodes": 0,
                "fold_cells_read": 0,
                "fold_peak_cells": 0,
                "failed": 0,
                "written": 0,
                "current": 0,
            },
        )
        agg["invokes"] += 1
        kind = rec.get("unit")
        agg[{"window": "window_invokes", "close": "close_invokes"}.get(kind, "whole_invokes")] += 1
        wall = float(rec.get("duration_s") or 0.0)
        agg["wall_s"].append(wall)
        if kind in ("window", "close"):
            agg[f"{kind}_wall_s"].append(wall)
        agg["worker_s"] += wall
        agg["nodes"] += int(rec.get("n_nodes") or 0)
        for row in rec.get("stages") or []:
            agg["fold_cells_read"] += int(row.get("fold_cells_read") or 0)
            agg["fold_peak_cells"] = max(
                agg["fold_peak_cells"], int(row.get("fold_peak_cells") or 0)
            )
            for k in ("failed", "written", "current"):
                agg[k] += int(row.get(k) or 0)
    per_order = []
    for order, agg in sorted(orders.items(), key=lambda kv: -(kv[0] or 0)):
        row = {"dispatch_order": order, **agg}
        for k in ("wall_s", "window_wall_s", "close_wall_s"):
            row[k] = _quantiles(agg[k])
        row["fold_cells_read_per_node"] = (
            agg["fold_cells_read"] / agg["nodes"] if agg["nodes"] else None
        )
        per_order.append(row)
    worker_s = sum(float(r.get("duration_s") or 0.0) for r in records)
    finisher_s = float(finisher.get("duration_s") or 0.0)
    return {
        "run_id": prefix.rstrip("/").removeprefix("run-"),
        "pipeline_run_id": finisher.get("pipeline_run_id")
        or next((r.get("pipeline_run_id") for r in records), None),
        "invokes": len(records),
        "finisher": bool(finisher),
        "finisher_s": finisher_s,
        "barrier_timed_out": finisher.get("barrier_timed_out"),
        "short_orders": finisher.get("short_orders"),
        "worker_s": worker_s,
        "failed": sum(r["failed"] for r in per_order),
        "gb_seconds": (worker_s + finisher_s) * STAGE_TIER_GB,
        "per_order": per_order,
    }


def stage_run_numbers(store_root: str, store_kwargs: dict, workers: int = 32) -> dict:
    """Every fleet stage run under ``<store>.status/`` (``run-stage-*`` prefixes,
    :func:`zagg.client_transport.run_status_prefix`), newest last; ``latest``
    is the one the table prints."""
    import obstore

    from zagg.store import open_object_store

    try:
        status = open_object_store(store_root.rstrip("/") + ".status", **store_kwargs)
        prefixes = sorted(
            p if p.endswith("/") else p + "/"
            for p in obstore.list_with_delimiter(status)["common_prefixes"]
            if p.rstrip("/").rsplit("/", 1)[-1].startswith("run-stage-")
        )
    except Exception as e:
        logger.info(f"no status prefix for {store_root} ({e})")
        return {"runs": [], "latest": None}
    runs = [_stage_run(status, p, workers) for p in prefixes]
    return {"runs": runs, "latest": runs[-1] if runs else None}


# ---------------------------------------------------------------------------
# Cost
# ---------------------------------------------------------------------------


def cost_numbers(fleet: dict, stages: dict) -> dict:
    """GB-seconds and USD: the leaf fan-out (billed where recorded), every stage
    run's invokes and finisher at the 8 GB tier, and the sum."""
    from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC

    leaf = fleet.get("billed_gb_seconds")
    stage = sum(r["gb_seconds"] for r in stages.get("runs") or [])
    total = (leaf or 0.0) + stage
    return {
        "price_per_gb_s": LAMBDA_PRICE_PER_GB_SEC,
        "leaf_gb_seconds": leaf,
        "leaf_usd": None if leaf is None else leaf * LAMBDA_PRICE_PER_GB_SEC,
        "stage_gb_seconds": stage,
        "stage_usd": stage * LAMBDA_PRICE_PER_GB_SEC,
        "total_usd": total * LAMBDA_PRICE_PER_GB_SEC,
    }


# ---------------------------------------------------------------------------
# One store
# ---------------------------------------------------------------------------


def measure(
    store_root: str,
    *,
    store_kwargs: dict,
    max_shards: int = 64,
    accuracy: bool = True,
    accuracy_levels=None,
    workers: int = 16,
) -> dict:
    """Everything for one store; the dict the CLI prints."""
    from zagg.hive import read_manifest

    store = _store(store_root, store_kwargs)
    manifest = read_manifest(store_root, **store_kwargs) or {}
    fleet = fleet_numbers(store)
    tree = tree_numbers(store, max_shards)
    stages = stage_run_numbers(store_root, store_kwargs)
    result = {
        "store": store_root,
        "schedule": ((manifest.get("temporal") or {}).get("schedule")) or "none",
        "shard_order": manifest.get("shard_order"),
        "count_dtype": tree["occupancy"].get("dtype"),
        "fleet": fleet,
        "objects": tree["objects"],
        "occupancy": tree["occupancy"],
        "non_leaf": tree["non_leaf"],
        "ladder": ladder_numbers(store),
        "stage_runs": stages,
        "cost": cost_numbers(fleet, stages),
    }
    if accuracy and manifest.get("pyramid") and tree["shards"]:
        from windowed_emit_accuracy import accuracy_numbers

        result["accuracy"] = accuracy_numbers(
            store_root,
            manifest,
            tree["shards"],
            tree["windows"] or [None],
            store_kwargs=store_kwargs,
            levels=accuracy_levels,
            workers=workers,
        )
    return result


# ---------------------------------------------------------------------------
# The table
# ---------------------------------------------------------------------------


def _fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, dict):
        return " / ".join(_fmt(x) for x in v.values())
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_fmt(x) for x in v) + "]"
    if isinstance(v, float):
        return f"{v:,.1f}"
    return f"{v:,}"


def _stage_sum(result: dict, key: str):
    """``key`` summed over the newest record's per-order rows; ``None`` (printed
    ``-``) when no row carries it — a windowed store's ladder runs without Icechunk."""
    values = [s[key] for s in result["ladder"]["stages"] if s.get(key) is not None]
    return sum(values) if values else None


def _stage_orders(result: dict, key: str) -> str:
    """``key`` per dispatch order, finest first (``8..6 / 5..3 / 2..0`` at width 3):
    the ladder's per-tuple shape, which a sum across orders would hide."""
    rows = sorted(result["ladder"]["stages"], key=lambda s: -(s.get("dispatch_order") or 0))
    return " / ".join(_fmt(s.get(key)) for s in rows) if rows else "-"


def _latest(result: dict) -> dict:
    return (result.get("stage_runs") or {}).get("latest") or {}


def _run_orders(result: dict, fn) -> str:
    rows = _latest(result).get("per_order") or []
    return " / ".join(_fmt(fn(r)) for r in rows) if rows else "-"


def _levels(result: dict, fn) -> str:
    rows = (result.get("accuracy") or {}).get("levels") or []
    return " / ".join(_fmt(fn(r)) for r in rows) if rows else "-"


def _worst_field(level: dict, key: str):
    vals = [f.get(key) for f in (level.get("fields") or {}).values() if f.get(key) is not None]
    return max(vals) if vals else None


def _stat(d: dict | None, *keys):
    d = d or {}
    return {k: d.get(k) for k in keys}


def table_rows(results: list[dict]) -> list[tuple]:
    arrays = sorted({a for r in results for a in r["objects"].get("leaf_bytes_by_array", {})})
    classes = sorted({c for r in results for c in r.get("non_leaf", {})})
    phases = sorted({k for r in results for k in r["fleet"] if k.startswith("leaf_phase_")})
    rows: list[tuple] = [
        ("schedule", lambda r: r["schedule"]),
        ("count dtype", lambda r: r.get("count_dtype") or "-"),
        (
            "runs / units / shards / leaves",
            lambda r: " / ".join(
                _fmt(r["fleet"].get(k)) for k in ("runs", "units", "shards", "leaves")
            ),
        ),
        ("windows per shard p50/p90/max", lambda r: _fmt(r["fleet"].get("windows_per_shard"))),
        ("duration_s p50/p90/max", lambda r: _fmt(r["fleet"].get("duration_s"))),
        ("duration_total_s p50/p90/max", lambda r: _fmt(r["fleet"].get("duration_total_s"))),
        ("max_memory_mb p50/p90/max", lambda r: _fmt(r["fleet"].get("max_memory_mb"))),
        (
            "errors / timeouts / fits 1 invoke",
            lambda r: (
                f"{r['fleet'].get('errors')} / {r['fleet'].get('timeouts')} / "
                f"{_fmt(r['fleet'].get('fits_one_invoke'))}"
            ),
        ),
        (
            "GB-seconds self / billed",
            lambda r: (
                f"{_fmt(r['fleet'].get('gb_seconds'))} / {_fmt(r['fleet'].get('billed_gb_seconds'))}"
            ),
        ),
    ]
    rows += [
        (
            f"{p.removeprefix('leaf_')} per leaf p50/p90/max",
            (lambda p: lambda r: _fmt(r["fleet"].get(p)))(p),
        )
        for p in phases
    ]
    rows += [
        ("leaves per shard p50/p90/max", lambda r: _fmt(r["objects"].get("leaves_per_shard"))),
        ("objects per shard p50/p90/max", lambda r: _fmt(r["objects"].get("objects_per_shard"))),
        ("bytes per shard p50/p90/max", lambda r: _fmt(r["objects"].get("bytes_per_shard"))),
        ("total leaf bytes", lambda r: _fmt(r["objects"].get("total_leaf_bytes"))),
    ]
    rows += [
        (
            f"  leaf bytes: {a}",
            (lambda a: lambda r: _fmt(r["objects"]["leaf_bytes_by_array"].get(a)))(a),
        )
        for a in arrays
    ]
    rows += [
        (
            "occupancy constants chunk B / index B / chunks",
            lambda r: " / ".join(
                _fmt(r["occupancy"].get(k)) for k in ("bytes_per_chunk", "index_bytes", "chunks")
            ),
        ),
        (
            "occupied chunks per leaf mean/min/p50/max",
            lambda r: _fmt(_stat(r["occupancy"].get("per_leaf"), "mean", "min", "p50", "max")),
        ),
        (
            "occupied fraction per leaf (mean)",
            lambda r: _fmt(r["occupancy"].get("per_leaf_fraction")),
        ),
        (
            "occupied chunks summed per shard mean/max",
            lambda r: _fmt(_stat(r["occupancy"].get("summed_per_shard"), "mean", "max")),
        ),
        ("  x one full row", lambda r: _fmt(r["occupancy"].get("full_row_ratio"))),
    ]
    rows += [
        (
            f"non-leaf {c} objects / bytes",
            (
                lambda c: (
                    lambda r: (
                        f"{_fmt(r['non_leaf'].get(c, {}).get('objects'))} / {_fmt(r['non_leaf'].get(c, {}).get('bytes'))}"
                    )
                )
            )(c),
        )
        for c in classes
    ]
    rows += [
        (
            "sweep records / orders / batches",
            lambda r: (
                f"{len(r['ladder']['records'])} / {len(r['ladder']['stages'])} / {r['ladder']['batch_rows']}"
            ),
        ),
        (
            "stage units window / close",
            lambda r: (
                f"{_fmt(_stage_sum(r, 'window_units'))} / {_fmt(_stage_sum(r, 'close_units'))}"
            ),
        ),
        ("stage worker_s per tuple", lambda r: _stage_orders(r, "duration_s")),
        ("stage fold peak cells per tuple", lambda r: _stage_orders(r, "fold_peak_cells")),
        ("stage failed (sum)", lambda r: _fmt(_stage_sum(r, "failed"))),
        (
            "barrier timed out / short orders",
            lambda r: (
                f"{_fmt(r['ladder'].get('barrier_timed_out'))} / {_fmt(r['ladder'].get('short_orders'))}"
            ),
        ),
        ("icechunk commits (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_commits"))),
        ("icechunk rebases (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_rebases"))),
        ("icechunk commit_s (sum)", lambda r: _fmt(_stage_sum(r, "icechunk_commit_s"))),
        # the fleet's per-invoke stage records (newest run)
        (
            "stage runs / invokes (latest)",
            lambda r: (
                f"{len((r.get('stage_runs') or {}).get('runs') or [])} / {_fmt(_latest(r).get('invokes'))}"
            ),
        ),
        ("stage invokes per order", lambda r: _run_orders(r, lambda o: o["invokes"])),
        ("stage wall p50 per order", lambda r: _run_orders(r, lambda o: o["wall_s"]["p50"])),
        ("stage wall max per order", lambda r: _run_orders(r, lambda o: o["wall_s"]["p100"])),
        (
            "stage window-unit wall max per order",
            lambda r: _run_orders(r, lambda o: o["window_wall_s"]["p100"]),
        ),
        (
            "stage close wall max per order",
            lambda r: _run_orders(r, lambda o: o["close_wall_s"]["p100"]),
        ),
        (
            "stage fold cells read per node per order",
            lambda r: _run_orders(r, lambda o: o["fold_cells_read_per_node"]),
        ),
        (
            "stage worker_s / finisher_s / failed",
            lambda r: (
                f"{_fmt(_latest(r).get('worker_s'))} / {_fmt(_latest(r).get('finisher_s'))} / {_fmt(_latest(r).get('failed'))}"
            ),
        ),
        ("all-time probe orders", lambda r: _levels(r, lambda lv: lv["order"])),
        (
            "all-time merges_from_raw per order",
            lambda r: _levels(r, lambda lv: lv.get("merges_from_raw")),
        ),
        (
            "all-time count exact per order",
            lambda r: _levels(
                r, lambda lv: all((lv.get("exact") or {}).values()) if lv.get("exact") else None
            ),
        ),
        (
            "all-time rank err max (x delta) per order",
            lambda r: _levels(r, lambda lv: _worst_field(lv, "rank_err_max_x_delta")),
        ),
        (
            "cost USD leaf / stage / total",
            lambda r: " / ".join(
                _fmt(r["cost"].get(k)) for k in ("leaf_usd", "stage_usd", "total_usd")
            ),
        ),
    ]
    return rows


def print_table(results: list[dict]) -> None:
    width = max(max(len(r["store"]) for r in results), 28)
    label_w = 44
    print(f"{'':{label_w}s}" + "".join(f"{r['store']:>{width + 2}s}" for r in results))
    for label, fn in table_rows(results):
        print(f"{label:{label_w}s}" + "".join(f"{fn(r):>{width + 2}s}" for r in results))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("store_root", nargs="+", help="hive store root(s), baseline first")
    parser.add_argument("--max-shards", type=int, default=64, help="shard nodes to count per store")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--anon", action="store_true", help="anonymous access (public buckets)")
    parser.add_argument("--json", default=None, help="also write the full result here")
    parser.add_argument("--skip-accuracy", action="store_true", help="leave out the all-time leg")
    parser.add_argument(
        "--accuracy-levels", default=None, help="orders to probe, comma-separated (default all)"
    )
    parser.add_argument("--workers", type=int, default=16, help="GET threads for the accuracy leg")
    args = parser.parse_args(argv)
    store_kwargs: dict = {"region": args.region}
    if args.anon:
        store_kwargs["skip_signature"] = True
    levels = {int(x) for x in args.accuracy_levels.split(",")} if args.accuracy_levels else None
    results = [
        measure(
            root,
            store_kwargs=store_kwargs,
            max_shards=args.max_shards,
            accuracy=not args.skip_accuracy,
            accuracy_levels=levels,
            workers=args.workers,
        )
        for root in args.store_root
    ]
    print_table(results)
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(results, fh, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
