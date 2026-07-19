"""Per-shard stats record: standardized schema + mergeable fold (issue #297).

One versioned record per processed shard, built from the worker's existing
``metadata`` dict. Two consumers, one source: the JSON **sidecar** written as a
SIBLING object next to a hive leaf ``.zarr`` (``stats.json``; the
``{hash}.stats.json`` naming arrives with issue #299 — ``template_hash`` is a
nullable placeholder until then), and the **run-level parquet** the dispatcher
writes at the store root (one row per shard, failure rows included).

The schema is mergeable by construction: only associative stats (counts, sums,
min/max — no stored means/medians), so the up-tree rollup is
:func:`merge` — a fold that is associative and commutative up to float
summation order. Identity-like fields (``shard_key``, ``granules_sha256``,
``invoked_by``, ...) merge as equal-or-``None``: a mismatch collapses to
``None`` (absorbing), which keeps the fold associative.

``build_record``/``merge`` are pure (no I/O); the sidecar/parquet helpers below
them do object-store I/O and import their backends lazily.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
from datetime import datetime, timezone
from typing import Any, Iterable

from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC

#: Version stamped into every record; bump on any key change (issue #297).
SCHEMA_VERSION = 1

#: Leaf sidecar object name (sibling of the leaf ``.zarr``, not inside it).
#: Windowed leaves (issue #246) suffix the window label — ``stats_{window}.json``
#: — mirroring the ``{full_id}_{window}.zarr`` leaf naming so two windows of one
#: shard cannot clobber each other's sidecar.
SIDECAR_NAME = "stats.json"

# Merge dispositions (associative + commutative by construction). Floats sum,
# so equality across fold orders holds up to FP summation order.
_SUM_KEYS = ("n_shards", "n_granules", "n_obs", "cells_with_data", "duration_s")
_SUM_OR_NONE_KEYS = ("gb_seconds", "est_cost_usd", "spill_bytes")
_MAX_OR_NONE_KEYS = ("max_memory_mb", "container_hwm_mb")
_EQ_OR_NONE_KEYS = (
    "shard_key",
    "template_hash",
    "granules_sha256",
    "zagg_version",
    "lambda",
    "invoked_by",
    "error",
)


def granules_sha256(granule_ids: Iterable[str] | None) -> str | None:
    """Catalog identity of a shard: sha256 over its sorted granule ids.

    Ids are whatever uniquely names the shard's inputs (granule URLs for the
    aggregation path, item ids/datetimes for raster). Sorted so the hash is
    order-independent; ``None``/empty -> ``None`` (no catalog identity).
    """
    ids = sorted(str(g) for g in granule_ids) if granule_ids else []
    if not ids:
        return None
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def raster_granule_ids(granules: Iterable[dict]) -> list:
    """Catalog-identity inputs for a raster unit's stats record.

    Raster ShardMap entries carry no granule URL; the stable per-acquisition
    identity is the STAC item id when present, else the acquisition datetime.
    """
    return [e.get("id") or e.get("datetime") for e in granules if e.get("id") or e.get("datetime")]


def lambda_env() -> dict | None:
    """The executing Lambda's config block, or ``None`` off-Lambda.

    Read from the standard runtime env vars — the worker needs no event key
    for this. ``function_variant`` is the deployed function name (the ``-disk``
    / benchmark twins are distinct names); request ids / function ARNs are
    deliberately omitted (account-identifying, add nothing — issue #297).
    """
    memory = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    if not memory:
        return None
    return {
        "memory_mb": int(memory),
        "arch": platform.machine(),
        "function_variant": os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
    }


def build_record(
    *,
    shard_key,
    metadata: dict,
    granule_ids: Iterable[str] | None = None,
    invoked_by: dict | None = None,
    lambda_config: dict | None = None,
) -> dict:
    """Build one shard's stats record from the worker's ``metadata`` dict.

    ``metadata`` is the existing worker result (``process_shard`` /
    ``process_and_write_hive`` / the raster metas): ``total_obs``,
    ``cells_with_data``, ``duration_s``, ``phase_timings``, and the memory
    telemetry keys when the caller stamped them. ``invoked_by`` is copied
    VERBATIM from the invoke payload — the dispatcher resolves it via
    ``sts get-caller-identity`` once per run; workers cannot see the invoker.
    ``lambda_config`` is :func:`lambda_env` on Lambda, ``None`` locally;
    when present it prices ``gb_seconds`` / ``est_cost_usd`` from
    ``duration_s`` (the billed-duration approximation the dispatcher's cost
    estimate already uses).
    """
    error = metadata.get("error")
    duration_s = float(metadata.get("duration_s") or 0.0)
    gb_seconds = est_cost = None
    if lambda_config and lambda_config.get("memory_mb"):
        gb_seconds = duration_s * lambda_config["memory_mb"] / 1024.0
        est_cost = gb_seconds * LAMBDA_PRICE_PER_GB_SEC
    phase_entries = {
        k: float(v)
        for k, v in (metadata.get("phase_timings") or {}).items()
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    }
    # Byte-volume metrics (the spill instrumentation, issue #217) must not ride
    # in the seconds-only phase block: the run parquet flattens phase_timings to
    # seconds-typed columns, where a byte count would mislead cost/latency
    # queries. Split any ``*_bytes`` entries out; surface spill volume on its own
    # summed field (issue #297).
    spill_bytes = phase_entries.get("spill_bytes")
    phase_timings = {k: v for k, v in phase_entries.items() if not k.endswith("_bytes")}
    granule_ids = list(granule_ids) if granule_ids is not None else None
    n_granules = metadata.get("granule_count")
    if n_granules is None:
        n_granules = len(granule_ids or [])
    return {
        "schema_version": SCHEMA_VERSION,
        "shard_key": int(shard_key),
        "template_hash": None,  # nullable until issue #299 lands the hasher
        "zagg_version": _zagg_version(),
        "n_shards": 1,
        "n_granules": int(n_granules),
        "granules_sha256": granules_sha256(granule_ids),
        "n_obs": int(metadata.get("total_obs") or 0),
        "cells_with_data": int(metadata.get("cells_with_data") or 0),
        "phase_timings": phase_timings,
        "duration_s": duration_s,
        "spill_bytes": spill_bytes,
        "gb_seconds": gb_seconds,
        "est_cost_usd": est_cost,
        "max_memory_mb": _opt_float(metadata.get("max_memory_mb")),
        "container_hwm_mb": _opt_float(metadata.get("container_hwm_mb")),
        "lambda": dict(lambda_config) if lambda_config else None,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "success": not error,
        "error": str(error) if error else None,
        "invoked_by": dict(invoked_by) if invoked_by else None,
    }


def merge(records: Iterable[dict]) -> dict:
    """Fold stats records into one (associative + commutative; issue #297).

    Counts/sums sum, memory high-waters max, ``timestamp`` takes the latest,
    ``success`` ANDs, ``phase_timings`` sums per key over the key union, and
    identity fields keep their common value or collapse to ``None`` on any
    mismatch (``None`` is absorbing, which is what keeps the fold
    associative). ``merge([r]) == r`` up to key order. Raises ``ValueError``
    on an empty iterable or a ``schema_version`` mismatch.
    """
    records = list(records)
    if not records:
        raise ValueError("merge requires at least one record")
    versions = {r.get("schema_version") for r in records}
    if versions != {SCHEMA_VERSION}:
        raise ValueError(f"cannot merge stats records with schema_version(s) {sorted(versions)}")
    out: dict[str, Any] = {"schema_version": SCHEMA_VERSION}
    for key in _EQ_OR_NONE_KEYS:
        first = records[0].get(key)
        if all(r.get(key) == first for r in records):
            # Defensively copy dict values (``lambda``/``invoked_by``) so a
            # rolled-up record never aliases a leaf's nested dict, mirroring
            # build_record (issue #297).
            out[key] = dict(first) if isinstance(first, dict) else first
        else:
            out[key] = None
    for key in _SUM_KEYS:
        out[key] = sum(r.get(key) or 0 for r in records)
    phase_timings: dict[str, float] = {}
    for r in records:
        for name, secs in (r.get("phase_timings") or {}).items():
            phase_timings[name] = phase_timings.get(name, 0.0) + secs
    out["phase_timings"] = phase_timings
    for key in _SUM_OR_NONE_KEYS:
        vals = [r.get(key) for r in records if r.get(key) is not None]
        out[key] = sum(vals) if vals else None
    for key in _MAX_OR_NONE_KEYS:
        vals = [r.get(key) for r in records if r.get(key) is not None]
        out[key] = max(vals) if vals else None
    stamps = [r.get("timestamp") for r in records if r.get("timestamp") is not None]
    out["timestamp"] = max(stamps) if stamps else None
    out["success"] = all(bool(r.get("success")) for r in records)
    return out


def _opt_float(value) -> float | None:
    return float(value) if value is not None else None


def _zagg_version() -> str:
    import zagg

    return zagg.__version__


# ---------------------------------------------------------------------------
# Leaf sidecar I/O (phase 2) — one small JSON object per hive leaf, written by
# the worker on success only, SIBLING to the leaf ``.zarr`` (never inside it:
# the leaf stays vanilla zarr v3 and the D4 commit stamp stays its final write).
# ---------------------------------------------------------------------------


def sidecar_key(leaf_name: str) -> str:
    """Sidecar object name for a leaf zarr basename.

    Bare leaves get :data:`SIDECAR_NAME`; windowed leaves (issue #246) get
    ``stats_{window}.json`` — a hive node directory holds every window's leaf
    of its one shard, so a bare ``stats.json`` would self-clobber across
    windows. Mirrors the ``{full_id}_{window}.zarr`` leaf naming.
    """
    from zagg.windows import split_leaf_name

    _full_id, window = split_leaf_name(leaf_name)
    if window is None:
        return SIDECAR_NAME
    stem, ext = SIDECAR_NAME.rsplit(".", 1)
    return f"{stem}_{window}.{ext}"


def sidecar_path(leaf_path: str) -> str:
    """Absolute path of a leaf's stats sidecar (sibling of the ``.zarr``)."""
    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    return f"{prefix}/{sidecar_key(name)}"


def write_sidecar(leaf_path: str, record: dict, **store_kwargs) -> None:
    """PUT ``record`` as the leaf's stats sidecar (success path only, #297)."""
    import obstore

    from zagg.store import open_object_store

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    obstore.put(
        open_object_store(prefix, **store_kwargs), sidecar_key(name), json.dumps(record).encode()
    )


def read_sidecar(leaf_path: str, **store_kwargs) -> dict | None:
    """The leaf's stats sidecar record, or ``None`` when absent."""
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    try:
        data = obstore.get(open_object_store(prefix, **store_kwargs), sidecar_key(name)).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))
