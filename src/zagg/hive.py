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

- **Coverage tier 0 (§4, issue #200)**: the stamp carries a ``coverage``
  payload — the shard's morton box, the canonical <= 4-member cover of its
  occupied cells (:func:`zagg.grids.morton.morton_box`), padded to exactly
  four decimal-string slots with JSON-null sentinels. Zero extra requests
  (it rides the stamp PUT) and debris semantics are inherited: a torn
  worker's coverage never becomes visible. The budgeted tier-1 MOC, the
  end-of-run root ``coverage.moc``, and the pyramid sweep (§7) are follow-on
  phases; the manifest's ``pyramid`` block is declared-only in round one
  (D11/D12).
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
#: Convention version of the stamp's coverage payload (§4 tier 0, issue #200).
COVERAGE_SPEC = "morton-moc/1"
#: Fixed slot count of the tier-0 morton box (2-4 members, null-padded).
COVERAGE_BOX_SLOTS = 4


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
        ok = _is_base_component(head)
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
    its FROZEN keys match the run's own (:data:`_FROZEN_MANIFEST_KEYS`: orders
    + identity + schedule — the flat path's ``_check_signature`` analogue).
    ``generated_at`` and ``pyramid`` are excluded: the pyramid block is
    populated/updated by the §7 sweep by design (D11), so comparing it would
    brick every resume after the first sweep.

    ``overwrite=True`` replaces the MANIFEST ONLY — it never clears data. To
    guard against the silent-corruption footgun (committed leaves from the old
    orders would survive a "re-template" and be indistinguishable from legal
    mixed-order data, D2), an overwrite that CHANGES the frozen keys refuses
    when the digit tree already has children (one delimiter-LIST); clear the
    store root first. Returns the manifest now in effect.
    """
    import obstore

    store = open_object_store(store_root, **store_kwargs)
    existing = _read_json(store, MANIFEST_NAME)
    frozen_matches = existing is not None and _frozen(existing) == _frozen(manifest)
    if existing is not None and not overwrite:
        if not frozen_matches:
            raise ValueError(
                f"{MANIFEST_NAME} at {store_root} does not match this run "
                f"(existing {existing!r} vs {manifest!r}); this store was templated "
                f"for different orders/identity — clear the store root (or pick a "
                f"new one) before writing with this configuration"
            )
        return existing
    if overwrite and existing is not None and not frozen_matches:
        # One delimiter-LIST: a {sign+base}-shaped child means shards were
        # already written under the OLD configuration. Their leaves are
        # stamped and walker-discoverable, so replacing just the manifest
        # would leave them masquerading as legal mixed-order data (D2).
        listing = obstore.list_with_delimiter(store)
        children = [p.rstrip("/").split("/")[-1] for p in listing["common_prefixes"]]
        if any(_is_base_component(c) for c in children):
            raise ValueError(
                f"refusing to overwrite {MANIFEST_NAME} at {store_root} with "
                f"different orders/identity: the digit tree already has shard "
                f"data (e.g. {children[0]!r}/), and overwrite replaces the "
                f"manifest only — clear the store root first"
            )
    obstore.put(store, MANIFEST_NAME, json.dumps(manifest, indent=1).encode())
    return manifest


#: Manifest keys the resume match-check compares (orders + identity + schedule).
#: ``generated_at`` (a timestamp) and ``pyramid`` (populated by the §7 sweep,
#: D11) are mutable by design and excluded.
_FROZEN_MANIFEST_KEYS = ("spec", "dataset", "cell_order", "shard_order", "split_schedule")


def _frozen(manifest: dict) -> dict:
    """The frozen-key projection of a manifest (resume/overwrite match-check)."""
    return {k: manifest.get(k) for k in _FROZEN_MANIFEST_KEYS}


def _is_base_component(name: str) -> bool:
    """Whether ``name`` is a ``{sign+base}``-shaped hive root child (D5)."""
    base = name[1:] if name.startswith("-") else name
    return len(base) == 1 and base in "123456"


def read_manifest(store_root: str, **store_kwargs) -> dict | None:
    """Read ``morton_hive.json`` from a store root; ``None`` when absent."""
    return _read_json(open_object_store(store_root, **store_kwargs), MANIFEST_NAME)


def build_coverage(shard_key, occupied, cell_order: int) -> dict:
    """Tier-0 coverage payload for one shard's commit stamp (§4, issue #200).

    ``occupied`` is the shard's occupied cell words (mixed order allowed —
    the cells ``cells_with_data`` counts); the box is their canonical
    <= 4-member cover (:func:`zagg.grids.morton.morton_box`). ``None``/empty
    falls back to the trivial 1-member cover, the shard id itself — always a
    valid ancestor of its own coverage. Members are serialized as decimal
    morton strings (D1), padded to exactly :data:`COVERAGE_BOX_SLOTS` slots
    with trailing ``None`` (JSON null) sentinels — the recorded pad lean.
    ``cell_order`` records the order occupancy was measured at; ``source``
    the producer (``"worker"`` at the leaf tier — phase-3 root and
    sweep-composed payloads record theirs). ``generated_at`` is DELIBERATELY
    omitted at the leaf (review finding, PR #208): the payload rides the
    commit stamp, whose ``written_at`` is the one clock and one writer;
    root/ancestor carriers add their own timestamp fields under this same
    spec (per-carrier-optional). Raises ``ValueError`` if the box escapes
    the shard's subtree (occupied cells from another shard are an upstream
    bug, never stamped).
    """
    from zagg.grids.morton import morton_box, morton_decimal

    shard = morton_decimal(shard_key)
    if occupied is None or len(occupied) == 0:
        labels = [shard]
    else:
        labels = [morton_decimal(w) for w in morton_box(occupied)]
    if len(labels) > COVERAGE_BOX_SLOTS or any(not s.startswith(shard) for s in labels):
        raise ValueError(
            f"coverage box {labels} escapes shard {shard}'s subtree — occupied "
            f"cells must be the shard's own (the shard id is always a valid "
            f"trivial cover, so this is an upstream cell-assignment bug)"
        )
    return {
        "spec": COVERAGE_SPEC,
        "box": labels + [None] * (COVERAGE_BOX_SLOTS - len(labels)),
        "cell_order": int(cell_order),
        "source": "worker",
    }


def stamp_commit(
    leaf_store, *, cells_with_data: int, granule_count: int, coverage: dict | None = None
) -> None:
    """Stamp a shard leaf complete — the shard's FINAL write (D4).

    One small PUT rewriting the leaf's root ``zarr.json`` (which the template
    already created), not consolidation. Until this lands, the leaf prefix is
    debris: a worker that dies mid-shard leaves no stamp, and a retry may
    overwrite the prefix wholesale. ``coverage`` (issue #200) attaches the
    tier-0 payload from :func:`build_coverage`; ``None`` writes the
    pre-coverage stamp unchanged.
    """
    group = zarr.open_group(leaf_store, path="", mode="r+", zarr_format=3)
    stamp: dict = {
        "spec": HIVE_SPEC,
        "complete": True,
        "cells_with_data": int(cells_with_data),
        "granule_count": int(granule_count),
        "written_at": _utcnow(),
    }
    if coverage is not None:
        stamp["coverage"] = coverage
    group.attrs[COMMIT_ATTR] = stamp


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


def read_coverage(leaf_store) -> dict | None:
    """The leaf's tier-0 coverage payload, or ``None`` when absent (issue #200).

    Rides :func:`read_commit`: debris and absent leaves read ``None``, and so
    does a committed pre-coverage stamp (issue #199 stores carry no
    ``coverage`` key) — older stores keep reading fine. STRICT on the spec
    (review finding, PR #208): only ``spec == "morton-moc/1"`` payloads are
    returned; a malformed dict or an unknown/future spec reads as absent
    rather than half-parsed, so a new envelope version must be adopted here
    deliberately instead of leaking through to box consumers. Box members are
    decimal morton strings; parse one back with
    :func:`zagg.grids.morton.morton_word`.
    """
    stamp = read_commit(leaf_store)
    if stamp is None:
        return None
    coverage = stamp.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("spec") != COVERAGE_SPEC:
        return None
    return dict(coverage)


def leaf_block_index(grid, block_index, shard_key) -> tuple:
    """Leaf-LOCAL storage block for a chunk in a hive leaf (issue #199 phase 2).

    The hive leaf's arrays are sized to one shard, so a chunk's block index is
    its position WITHIN the shard, not the global block ``iter_chunks`` yields.
    Derived from the existing ``shard_local_region`` seam (the sharded path's
    within-shard placement): the region's start divided by the chunk extent.
    At K==1 this is always ``(0,)``.
    """
    region = grid.shard_local_region(block_index, shard_key)
    return tuple(int(s.start) // int(c) for s, c in zip(region, grid.chunk_shape))


def process_and_write_hive(
    shard_key,
    granule_urls,
    grid,
    s3_creds,
    store_root,
    config,
    *,
    store_kwargs,
    driver=None,
    handoff="arrow",
    aoi_payload=None,
    profile=False,
):
    """Process one shard into its own hive leaf store (issue #199 phase 2).

    The SHARED per-shard write path for both backends (phase 3): the local
    runner's ``_cell_work`` and the Lambda handler's hive branch both call
    this, so leaf templating, chunk placement, CSR naming, and stamp ordering
    cannot drift between dispatchers. The shard's output is a self-describing
    leaf zarr at :func:`shard_leaf_path` ``(store_root, shard_key)`` (D3), with
    dense chunks written at leaf-LOCAL block indices and — as the shard's
    FINAL write — the D4 commit stamp on the leaf's root group. The leaf
    template is emitted lazily on the first chunk write (mirroring the Lambda
    handler's lazy store open), so a no-data shard never creates the
    ``.zarr/`` prefix; a worker that dies mid-shard leaves an UNSTAMPED prefix
    — debris, overwritten wholesale on retry (``overwrite=True`` on the leaf
    template makes the retry idempotent). ``profile`` forwards to
    ``process_shard`` (issue #100); the write phase is interleaved with the
    stream on this path, so no separate ``write`` timing is recorded.
    """
    from zagg.grids.base import shard_label
    from zagg.processing import process_shard, write_dataframe_to_zarr, write_ragged_to_zarr
    from zagg.store import open_store

    leaf_path = shard_leaf_path(store_root, shard_key)
    box: dict = {}

    def _leaf():
        if "store" not in box:
            store = open_store(leaf_path, **store_kwargs)
            # overwrite=True: any existing prefix here is either debris from a
            # torn run (D4) or a prior committed write being redone — both are
            # replaced wholesale; per-leaf state never blocks a retry.
            grid.emit_shard_template(store, overwrite=True)
            box["store"] = store
        return box["store"]

    single_chunk = int(getattr(grid, "chunks_per_shard", 1)) == 1

    def _write_chunk(block_index, carrier, ragged):
        store = _leaf()
        local = leaf_block_index(grid, block_index, shard_key)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=local)
        # CSR subgroup naming inside a leaf mirrors the flat layout: the shard
        # label at K==1; the LOCAL chunk ordinal at K>1 (leaf arrays are
        # 1-D — hive is HEALPix-only, validated at config load).
        ragged_key = shard_label(grid, shard_key) if single_chunk else int(local[0])
        write_ragged_to_zarr(ragged, store, grid=grid, shard_key=ragged_key)

    # Occupied-cell sink (issue #200): the worker already holds the shard's
    # populated cell words; collect them here to derive the stamp's coverage.
    occupied: list = []
    _df_out, metadata = process_shard(
        grid,
        int(shard_key),
        granule_urls,
        s3_credentials=s3_creds,
        config=config,
        driver=driver,
        handoff=handoff,
        aoi_payload=aoi_payload,
        write_chunk=_write_chunk,
        occupied_out=occupied,
        profile=profile,
    )
    # Stamp ONLY a fully-written leaf: an errored shard (or one that streamed
    # no chunks) stays unstamped — debris by definition (D4). The stamp is the
    # last write, so its presence certifies everything before it landed — the
    # coverage payload rides it (zero extra requests) and inherits its debris
    # semantics: a torn worker's coverage never becomes visible.
    if "store" in box and not metadata.get("error"):
        stamp_commit(
            box["store"],
            cells_with_data=metadata.get("cells_with_data", 0),
            granule_count=metadata.get("granule_count", 0),
            coverage=build_coverage(
                shard_key,
                np.concatenate(occupied) if occupied else None,
                grid.child_order,
            ),
        )
    return metadata


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
    "COVERAGE_BOX_SLOTS",
    "COVERAGE_SPEC",
    "HIVE_SPEC",
    "MANIFEST_NAME",
    "build_coverage",
    "build_manifest",
    "check_node_invariant",
    "ensure_manifest",
    "leaf_block_index",
    "process_and_write_hive",
    "read_commit",
    "read_coverage",
    "read_manifest",
    "shard_leaf_path",
    "stamp_commit",
]
