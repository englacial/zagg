"""Morton-hive store layout: leaf paths, manifest, and commit stamp (issue #199).

Phase 2 of the layout migration (``docs/design/sparse_coverage.md`` §2-§3).
Under ``output.store_layout: hive`` each shard is its own **self-describing
leaf zarr** under a morton digit tree::

    {store_root}/
      morton_hive.json               <- static manifest (§3); root-only exception
      {sign+base}/{d1}/.../{d_n}/    <- one decimal digit per level (D2)
        {full_id}.zarr/              <- vanilla zarr v3 leaf (D3)

- Ids are morton decimal strings (D1); the leaf path is computed by mortie's
  ``hive_path`` (the convention is owned by the mortie spec) and re-checked
  here against the node invariant.
- **Node invariant (D5)**: below the root a node contains only digit children
  (``[1-4]/``, or the ``{sign+base}`` component at the first level) and
  ``*.zarr`` objects — zero zarr metadata above the leaf, so 2,000 workers
  share no mutable state and a delimiter-LIST with no digit children is a
  definitive "nothing finer exists".
- **Manifest (D6)**: ``morton_hive.json`` is written once at template time and
  never touched during a run; with it every shard path is computable with zero
  requests. The convention is versioned (``morton-hive/1``) from day one.
- **Commit stamp (D4)**: the shard's FINAL write is a root
  ``group.attrs.update(...)`` marking completion (plus cell count, timestamp,
  granule count). A ``.zarr/`` prefix whose root metadata lacks the stamp is
  debris — incomplete, ignorable, safe to overwrite on retry. This is NOT
  consolidated metadata: one small PUT on an object that must exist anyway.

The coverage MOC (§4) and pyramid sweep (§7) are follow-on issues; the
manifest's ``pyramid`` block is declared-only in round one (D11/D12).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import zarr
from zarr.errors import GroupNotFoundError

from zagg.store import open_object_store

#: Convention version recorded in the manifest and the commit stamp (D6).
HIVE_SPEC = "morton-hive/1"
#: Root manifest object name (the root-only exception to the node invariant).
MANIFEST_NAME = "morton_hive.json"
#: Root-group attrs key carrying the commit stamp (D4).
COMMIT_ATTR = "morton_hive_commit"


def shard_leaf_path(store_root: str, shard_key) -> str:
    """Absolute path of a shard's leaf zarr under ``store_root`` (D2/D3).

    Computed by mortie's ``hive_path`` — the layout convention is owned by the
    mortie spec — and re-checked against the node invariant (D5) so a future
    drift in either side fails loudly instead of writing a stray prefix.
    Raises ``ValueError`` on an invalid shard key.
    """
    from mortie import MortonIndexArray

    word = int(shard_key)
    if word < 0:
        raise ValueError(
            f"shard key must be a packed morton word (got {word}); parse a decimal "
            f"id with zagg.grids.morton.morton_word first"
        )
    rel = MortonIndexArray.from_words(np.asarray([word], dtype=np.uint64)).hive_path()[0]
    check_node_invariant(rel)
    return f"{store_root.rstrip('/')}/{rel}"


def check_node_invariant(rel_path: str) -> None:
    """Raise unless ``rel_path`` is a legal hive leaf path (D5).

    Below the root only digit components are allowed — ``{sign+base}``
    (optional ``-``, one digit ``1..6``) at the first level, one ``1..4`` digit
    per level after — terminating in ``{full_id}.zarr`` whose id equals the
    concatenated components. This is the walker's contract: any other name
    under the root (bar the manifest and the future ``coverage.moc``) breaks
    child classification.
    """
    parts = rel_path.strip("/").split("/")
    leaf = parts[-1]
    ok = len(parts) >= 2 and leaf.endswith(".zarr")
    if ok:
        head, digits = parts[0], parts[1:-1]
        base = head[1:] if head.startswith("-") else head
        ok = len(base) == 1 and base in "123456"
        ok = ok and all(len(d) == 1 and d in "1234" for d in digits)
        ok = ok and leaf[: -len(".zarr")] == head + "".join(digits)
    if not ok:
        raise ValueError(f"path {rel_path!r} violates the hive node invariant (D5)")


def build_manifest(grid, dataset: dict | None = None) -> dict:
    """Build the static ``morton_hive.json`` payload for one store (§3, D6).

    ``grid`` supplies the orders; ``dataset`` (typically the ShardMap's
    ``metadata``) supplies identity — only ``short_name`` and ``version`` are
    recorded. The split schedule is implicit under D2 (one digit per level down
    to the shard order) but recorded explicitly for forward compatibility; the
    ``pyramid`` block is declared-only in round one (D11: overviews are a
    second-pass sweep, never written at fan-out time).
    """
    dataset = dataset or {}
    return {
        "spec": HIVE_SPEC,
        "dataset": {
            "short_name": dataset.get("short_name"),
            "version": dataset.get("version"),
        },
        "cell_order": int(grid.child_order),
        "shard_order": int(grid.parent_order),
        "split_schedule": [1] * int(grid.parent_order),
        "pyramid": {"orders": [], "aggregation": {}},
        "generated_at": _utcnow(),
    }


def ensure_manifest(store_root: str, manifest: dict, *, overwrite: bool = False, **store_kwargs):
    """Write the root manifest once at template time; verify it on reruns.

    A retry into an existing hive store must be able to proceed (that is the
    D4 debris/retry model), so an existing manifest is accepted — but only if
    it matches the run's own (``generated_at`` aside): a mismatch means the
    store was templated for different orders/identity, the same guard the flat
    path gets from ``_check_signature``. ``overwrite=True`` replaces it.
    Returns the manifest now in effect.
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    existing = _read_json(store, MANIFEST_NAME)
    if existing is not None and not overwrite:
        drop = ("generated_at",)
        if {k: v for k, v in existing.items() if k not in drop} != {
            k: v for k, v in manifest.items() if k not in drop
        }:
            raise ValueError(
                f"{MANIFEST_NAME} at {store_root} does not match this run "
                f"(existing {existing!r} vs {manifest!r}); pass overwrite=True to "
                f"re-template the store"
            )
        return existing
    obstore.put(store, MANIFEST_NAME, json.dumps(manifest, indent=1).encode())
    return manifest


def read_manifest(store_root: str, **store_kwargs) -> dict | None:
    """Read ``morton_hive.json`` from a store root; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), MANIFEST_NAME)


def stamp_commit(leaf_store, *, cells_with_data: int, granule_count: int) -> None:
    """Stamp a shard leaf complete — the shard's FINAL write (D4).

    One small PUT rewriting the leaf's root ``zarr.json`` (which the template
    already created), not consolidation. Until this lands, the leaf prefix is
    debris: a worker that dies mid-shard leaves no stamp, and a retry may
    overwrite the prefix wholesale.
    """
    group = zarr.open_group(leaf_store, path="", mode="r+", zarr_format=3)
    group.attrs[COMMIT_ATTR] = {
        "spec": HIVE_SPEC,
        "complete": True,
        "cells_with_data": int(cells_with_data),
        "granule_count": int(granule_count),
        "written_at": _utcnow(),
    }


def read_commit(leaf_store) -> dict | None:
    """The leaf's commit stamp, or ``None`` for debris / absent leaves (D4).

    Absence (no root group at all) and an unstamped root are the same answer:
    the shard is not complete. Presence requires the stamp — never infer
    completeness from the ``.zarr/`` prefix existing.
    """
    try:
        group = zarr.open_group(leaf_store, path="", mode="r", zarr_format=3)
    except (FileNotFoundError, GroupNotFoundError):
        return None
    stamp = group.attrs.get(COMMIT_ATTR)
    # A malformed (non-mapping) stamp is debris too — never half-trusted.
    return dict(stamp) if isinstance(stamp, dict) else None


def _read_json(obj_store, key: str) -> dict | None:
    """GET+parse one small JSON object; ``None`` when it does not exist."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(obj_store, key).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    return json.loads(bytes(data))


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "COMMIT_ATTR",
    "HIVE_SPEC",
    "MANIFEST_NAME",
    "build_manifest",
    "check_node_invariant",
    "ensure_manifest",
    "read_commit",
    "read_manifest",
    "shard_leaf_path",
    "stamp_commit",
]
