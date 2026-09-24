"""The Icechunk ref ladder: sidecars up the pyramid, one commit per node (spec §11.4).

Phase 6 of issue #580. Per-leaf commits do not scale — 3.1M commits at the
full-globe worst case, and the real limit is that a snapshot's size is set by
its MANIFEST COUNT: one manifest per order-6 cell per array is ~442k
manifests, a 44 MB snapshot read on every open, rebase and commit. So refs
travel the way the digest columns do:

1. the leaf worker writes its ref plan as a compact **sidecar** beside the
   leaf (:func:`write_leaf_refs`) and commits nothing;
2. each stage node of the staged sweep gathers its subtree — the leaf
   sidecars at the finest tuple, its children's node **ref columns** above —
   adds the refs of the overview objects it just wrote, and either writes
   its own node column for the tuple above or **commits** (:func:`stage_node_refs`);
3. the tuple whose order range contains ``icechunk.commit_order`` commits
   everything gathered in ONE commit per node covering every order in its
   subtree (refs into ``/9/…``, ``/8/…``, … of the one repo); coarser tuples
   commit only their own overviews (their children already committed); finer
   tuples write columns only. A manifest (one per ``split_order`` cell, per
   order group) is therefore written by exactly one commit — zero rewrite
   amplification.

The carrier is JSON — a member of the leaf's JSON-sibling family, keyed by
the stats sidecar's grammar — holding a list of *units* ``{"order",
"entries"}`` whose entries are :func:`zagg.icechunk_refs.object_ref_plan`
entries with the shared ``location`` and a ``present`` mask in place of the
per-chunk location list (~40 KB per leaf at production geometry). It is a
writer-internal carrier, not part of the reader contract (the repo is).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Iterable

import numpy as np

from zagg.icechunk_refs import (
    _is_local,
    commit_units,
    container_prefix,
    object_ref_plan,
    open_vetted,
    repo_path,
)

logger = logging.getLogger(__name__)

#: The carrier's revision token, in every sidecar/column.
REFS_SPEC = "zagg-icechunk-refs/1"
#: Base name of the leaf's ref sidecar (``telemetry._sibling_key`` grammar).
LEAF_REFS_NAME = "icechunk_refs.json"
#: Basename of a stage node's ref column, at the node directory.
NODE_REFS_NAME = "icechunk_refs.json"
#: The leaf carrier's geometry block — vetted against the repo's own, keyed
#: off this fixed tuple so a carrier that declares none is refused (§11.4).
GEOMETRY_KEYS = ("shard_order", "chunk_order", "cell_order")
#: The counters :func:`stage_node_refs` adds to a stage row.
STAGE_COUNTS = (
    "icechunk_refs",
    "icechunk_commits",
    "icechunk_rebases",
    "icechunk_commit_s",
    "icechunk_missing",
    "icechunk_failed",
    "icechunk_skipped_levels",
)


# ── the carrier ─────────────────────────────────────────────────────────────


def _checksum_out(value):
    if isinstance(value, datetime):
        return {"last_modified": value.isoformat()}
    return value


def _checksum_in(value):
    if isinstance(value, dict):
        return datetime.fromisoformat(value["last_modified"])
    return value


def _strip(location: str, prefix: str) -> str:
    if location and not location.startswith(prefix):
        raise ValueError(f"location {location!r} is outside the container {prefix!r}")
    return location[len(prefix) :] if location else ""


def pack_units(units: list[dict], prefix: str, **meta) -> bytes:
    """Serialize ``units`` (``[{"order", "entries"}]``) to the JSON carrier.

    Locations are stored **relative to the container prefix** (§11.3), so the
    carrier's bytes do not depend on where the store lives and a relocated
    store's carriers re-expand against the new prefix.
    """
    out_units = []
    for unit in units:
        entries = []
        for entry in unit["entries"]:
            if entry["sharded"]:
                present = [bool(loc) for loc in entry["locations"]]
                entries.append(
                    {
                        "path": entry["path"],
                        "sharded": True,
                        "chunk_grid": [int(c) for c in entry["chunk_grid"]],
                        "arr_offset": [int(o) for o in entry["arr_offset"]],
                        "location": _strip(
                            next((loc for loc in entry["locations"] if loc), ""), prefix
                        ),
                        "present": present,
                        "offsets": [int(o) for o in np.asarray(entry["offsets"])],
                        "lengths": [int(n) for n in np.asarray(entry["lengths"])],
                        "checksum": _checksum_out(entry["checksum"]),
                        "refs": int(entry["refs"]),
                    }
                )
            else:
                entries.append(
                    {
                        "path": entry["path"],
                        "sharded": False,
                        "chunks": [
                            [key, _strip(location, prefix), int(length), _checksum_out(checksum)]
                            for key, location, length, checksum in entry["chunks"]
                        ],
                        "refs": int(entry["refs"]),
                    }
                )
        out_units.append({"order": int(unit["order"]), "entries": entries})
    return json.dumps({"spec": REFS_SPEC, **meta, "units": out_units}).encode()


def unpack_units(raw: bytes, prefix: str) -> tuple[list[dict], dict]:
    """``(units, meta)`` from carrier bytes; the inverse of :func:`pack_units`."""
    meta = json.loads(raw)
    if meta.get("spec") != REFS_SPEC:
        raise ValueError(f"ref carrier declares {meta.get('spec')!r}, not {REFS_SPEC!r}")
    units = []
    for unit in meta.pop("units"):
        entries = []
        for entry in unit["entries"]:
            if entry["sharded"]:
                # ONE string object for the whole entry: every present inner
                # chunk of a sharded array shares the leaf's one shard object
                # (``pack_units`` stores a single ``location`` for that
                # reason), so expanding it per chunk allocated ~256 identical
                # ~80-char copies per array per leaf (review finding).
                loc = prefix + entry["location"]
                entries.append(
                    {
                        "path": entry["path"],
                        "sharded": True,
                        "chunk_grid": tuple(entry["chunk_grid"]),
                        "arr_offset": tuple(entry["arr_offset"]),
                        "locations": [loc if p else "" for p in entry["present"]],
                        "offsets": np.asarray(entry["offsets"], dtype="<u8"),
                        "lengths": np.asarray(entry["lengths"], dtype="<u8"),
                        "checksum": _checksum_in(entry["checksum"]),
                        "refs": int(entry["refs"]),
                    }
                )
            else:
                entries.append(
                    {
                        "path": entry["path"],
                        "sharded": False,
                        "chunks": [
                            (key, prefix + location, int(length), _checksum_in(checksum))
                            for key, location, length, checksum in entry["chunks"]
                        ],
                        "refs": int(entry["refs"]),
                    }
                )
        units.append({"order": int(unit["order"]), "entries": entries})
    return units, meta


def leaf_refs_key(leaf_name: str, spec: str | None = None) -> str:
    """The leaf's ref-sidecar object name — the stats sidecar's sibling grammar."""
    from zagg.telemetry import _sibling_key

    return _sibling_key(leaf_name, LEAF_REFS_NAME, spec)


def write_leaf_refs(
    store_root: str, leaf_path: str, grid, plan: list, *, spec, store_kwargs
) -> dict:
    """PUT the leaf's ref plan beside it; returns ``{"sidecar", "bytes", "refs", "arrays"}``.

    ``sidecar`` is the sibling's KEY (basename), never a path: the record
    rides the leaf's stats sidecar, where a root-dependent string would
    break byte parity between runs.
    """
    from zagg.store import open_object_store, put_object

    prefix, _, name = leaf_path.rstrip("/").rpartition("/")
    key = leaf_refs_key(name, spec)
    raw = pack_units(
        [{"order": int(grid.parent_order), "entries": plan}],
        container_prefix(store_root),
        # The orders only — never the container prefix, which is root-
        # dependent and would break byte parity of the sidecar across roots;
        # the prefix is vetted when the repo is opened (open_vetted).
        geometry={
            "shard_order": int(grid.parent_order),
            "chunk_order": int(grid.chunk_order),
            "cell_order": int(grid.child_order),
        },
    )
    put_object(open_object_store(prefix, **store_kwargs), key, raw)
    return {
        "sidecar": key,
        "bytes": len(raw),
        "refs": int(sum(e["refs"] for e in plan)),
        "arrays": int(sum(1 for e in plan if e["refs"])),
    }


def _get(store_root: str, rel: str, store_kwargs) -> bytes | None:
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    try:
        return bytes(obstore.get(open_object_store(store_root, **store_kwargs), rel).bytes())
    except (FileNotFoundError, NotFoundError):
        return None


def read_leaf_refs(store_root: str, shard_key, *, spec, store_kwargs) -> tuple[list, dict] | None:
    """The leaf's ``(units, meta)``, or ``None`` when no sidecar exists."""
    from zagg.hive import shard_leaf_path

    leaf = shard_leaf_path(store_root, shard_key)
    prefix, _, name = leaf.rpartition("/")
    rel = f"{prefix[len(store_root.rstrip('/')) + 1 :]}/{leaf_refs_key(name, spec)}"
    raw = _get(store_root, rel, store_kwargs)
    return None if raw is None else unpack_units(raw, container_prefix(store_root))


def _node_rel(node: str) -> str:
    from zagg.sweep import _node_rel as rel

    return rel(node)


def write_node_refs(store_root: str, node: str, units: list, *, store_kwargs) -> dict:
    """PUT a stage node's ref column at its node directory."""
    from zagg.store import open_object_store, put_object

    raw = pack_units(units, container_prefix(store_root), node=node)
    put_object(
        open_object_store(store_root, **store_kwargs), f"{_node_rel(node)}/{NODE_REFS_NAME}", raw
    )
    return {"bytes": len(raw), "refs": int(sum(e["refs"] for u in units for e in u["entries"]))}


def read_node_refs(store_root: str, node: str, *, store_kwargs) -> list | None:
    raw = _get(store_root, f"{_node_rel(node)}/{NODE_REFS_NAME}", store_kwargs)
    return None if raw is None else unpack_units(raw, container_prefix(store_root))[0]


# ── the ladder ──────────────────────────────────────────────────────────────


def ladder_context(store_root: str, manifest: dict, *, store_kwargs) -> dict | None:
    """The repo's ``zagg_icechunk`` block, vetted against ``manifest``, or ``None``.

    ``None`` means no companion (``output.icechunk`` off, or the init never
    landed) and the ladder hook is a no-op. A block whose orders or container
    disagree with the manifest raises — refs gathered from this store must
    never land in a repo built for another.
    """
    if manifest.get("temporal") is not None:
        return None  # windowed stores are outside the writer's scope (§11.6)
    try:
        _repo, block = open_vetted(
            store_root,
            store_kwargs=store_kwargs,
            want={
                "shard_order": int(manifest["shard_order"]),
                "cell_order": int(manifest["cell_order"]),
            },
        )
    except ValueError as e:
        if "not initialized" in str(e):
            return None
        raise
    for key in ("commit", "commit_order", "split_order", "levels"):
        if key not in block:
            raise ValueError(
                f"icechunk repo {repo_path(store_root)} predates the ladder (no {key!r})"
            )
    return block


def _overview_grid(k: int, r: int, fields: dict):
    from zagg.grids.healpix import HealpixGrid
    from zagg.sweep_overview import _overview_config

    return HealpixGrid(int(k), int(r), config=_overview_config(fields), sharded=True)


def _overview_units(store_root, node, orders, level_by_order, fields, candidates, store_kwargs):
    """Ref entries of every overview object under ``node`` at the tuple's orders."""
    from zagg.grids.morton import morton_word
    from zagg.sweep_overview import _node_at, _overview_basename
    from zagg.windows import SCHEDULE_NONE_TOKEN

    basename = _overview_basename(SCHEDULE_NONE_TOKEN)
    units = []
    for k in orders:
        grid = _overview_grid(k, level_by_order[k], fields)
        entries: list = []
        for target in sorted({_node_at(d, k) for d in candidates if d.startswith(node)}):
            (rank,) = grid.block_index(morton_word(target))
            entries.extend(
                object_ref_plan(
                    grid,
                    f"{_node_rel(target)}/{basename}",
                    rank,
                    store_root,
                    store_kwargs=store_kwargs,
                )
            )
        if entries:
            units.append({"order": int(k), "entries": entries})
    return units


def _child_units(store_root, node, child_order, shard_order, candidates, block, spec, store_kwargs):
    """Yield ``(units, missing)`` per child, one child at a time.

    A GENERATOR, not a list: the committing node feeds each child's units
    straight into the one session and lets them go before reading the next,
    so the node never holds its whole subtree's carriers at once — at the
    global-scale setting (``commit_order`` 3) that subtree is 4,096 leaves
    (review finding). ``missing`` is 1 for a child whose carrier is absent,
    unreadable or for another geometry, whose siblings are kept regardless:
    a per-leaf fault never costs the node its whole gather (§11.4).
    """
    from zagg.grids.morton import morton_word
    from zagg.sweep_overview import _node_at

    children = sorted({_node_at(d, child_order) for d in candidates if d.startswith(node)})
    for child in children:
        # Per-CHILD fail-open: a foreign spec token, a truncated carrier
        # (``JSONDecodeError`` is a ``ValueError``) or a geometry mismatch is
        # this child's fault, not the node's. Letting it out would cost the
        # node its whole gather — 63 good leaves for one bad one at
        # production geometry — so it counts as ``missing`` like a carrier
        # that was never written (review finding).
        try:
            if child_order == shard_order:
                got = read_leaf_refs(
                    store_root, morton_word(child), spec=spec, store_kwargs=store_kwargs
                )
                if got is None:
                    yield [], 1
                    continue
                child_units, meta = got
                geometry = meta.get("geometry")
                # Keyed off the FIXED tuple, not off the carrier's own dict: a
                # carrier with no geometry block compared {} != {} and passed.
                if not isinstance(geometry, dict) or any(k not in geometry for k in GEOMETRY_KEYS):
                    raise ValueError(f"leaf {child} refs carry no geometry block")
                want = {key: geometry[key] for key in GEOMETRY_KEYS}
                have = {key: block.get(key) for key in GEOMETRY_KEYS}
                if have != want:
                    raise ValueError(f"leaf {child} refs are for {want}, the repo is {have}")
            else:
                child_units = read_node_refs(store_root, child, store_kwargs=store_kwargs)
                if child_units is None:
                    yield [], 1
                    continue
        except (ValueError, KeyError) as e:
            logger.warning(f"icechunk: dropping child {child}'s refs (fail-open, issue #580): {e}")
            yield [], 1
            continue
        yield child_units, 0


def stage_node_refs(
    store_root: str,
    node: str,
    stage: dict,
    *,
    shard_order: int,
    levels: list,
    fields: dict,
    candidates,
    block: dict,
    spec: str | None,
    store_kwargs: dict,
    counts: dict,
) -> dict:
    """One stage node's share of the ladder; the counters it adds to ``counts``.

    ``stage`` is the tuple (``dispatch``, ``orders``, ``child_order``) the
    node was dispatched under; ``block`` the repo's vetted block
    (:func:`ladder_context`). Fail-open at the call site: raises propagate to
    the hook, which counts ``icechunk_failed`` and moves on (D9).
    """
    dispatch, child_order = int(stage["dispatch"]), int(stage["child_order"])
    commit_order, split_order = int(block["commit_order"]), int(block["split_order"])
    level_by_order = {int(e["node"]): int(e["cells"][0]) for e in levels}
    # Against the REPO'S groups, not the manifest's declaration alone: the
    # pyramid is mutable by design (``hive._FROZEN_MANIFEST_KEYS`` excludes
    # it) and ``init_repo``'s reopen branch creates no group for a level
    # declared after the repo was made, so an order the manifest gained would
    # raise ``NodeNotFound`` mid-commit and discard the node's base refs too
    # (review finding). The node commits the levels the repo has.
    have = {int(o) for o in block["levels"]}
    orders = [k for k in stage["orders"] if k in level_by_order]
    lacking = [k for k in orders if k not in have]
    if lacking:
        logger.warning(
            f"icechunk: node {node} skips order(s) {lacking} — declared by the manifest but "
            f"absent from the repo's levels {sorted(have)} (issue #580)"
        )
        counts["icechunk_skipped_levels"] += len(lacking)
        orders = [k for k in orders if k in have]
    per_leaf = block.get("commit") == "leaf"
    own = _overview_units(
        store_root, node, orders, level_by_order, fields, candidates, store_kwargs
    )
    # Which of the three roles this tuple plays (module docstring, §11.4).
    commits_all = dispatch <= commit_order < child_order and not per_leaf
    column_only = dispatch > commit_order and not per_leaf
    record: dict = {"node": node, "refs": 0, "missing": 0}
    source: Iterable[dict]
    args = (store_root, node, child_order, shard_order, candidates, block, spec, store_kwargs)
    if column_only:
        units, missing = [], 0
        for child_units, miss in _child_units(*args):
            units.extend(child_units)
            missing += miss
        written = write_node_refs(store_root, node, units + own, store_kwargs=store_kwargs)
        record.update(refs=written["refs"], missing=missing, column=written["bytes"])
        counts["icechunk_refs"] += written["refs"]
        counts["icechunk_missing"] += missing
        return record
    if commits_all:
        if split_order < dispatch:
            raise ValueError(
                f"split_order {split_order} is finer than the committing node order {dispatch}: "
                f"a manifest would be written by more than one commit"
            )
        # STREAMED into the one session: each child's units are written and
        # released before the next is read, so the node's peak is one child,
        # not its whole subtree (review finding). The commit is unchanged —
        # still one per node, covering every order it gathered.
        tally = [0]

        def stream():
            for child_units, miss in _child_units(*args):
                tally[0] += miss
                yield from child_units
            yield from own

        source = stream()
    else:
        source = iter(own)  # coarser than the committing tuple (or per-leaf): own overviews only
    outcome = commit_units(store_root, source, f"node {node}", store_kwargs=store_kwargs)
    if commits_all:
        record["missing"] = tally[0]
        counts["icechunk_missing"] += tally[0]
    record.update(refs=outcome["refs"], commit=outcome)
    counts["icechunk_refs"] += outcome["refs"]
    if outcome["snapshot"] is not None:
        counts["icechunk_commits"] += 1
        counts["icechunk_rebases"] += outcome["rebases"]
        counts["icechunk_commit_s"] += outcome["commit_s"]
    return record


def stage_hook(
    store_root, node, stage, *, manifest, levels, fields, candidates, block, store_kwargs, counts
):
    """The few-line seam ``sweep_stages.sweep_stage_pass`` calls per node — fail-open."""
    if block is None:
        return None
    t0 = time.perf_counter()
    try:
        return stage_node_refs(
            store_root,
            node,
            stage,
            shard_order=int(manifest["shard_order"]),
            levels=levels,
            fields=fields,
            candidates=candidates,
            block=block,
            spec=manifest.get("spec"),
            store_kwargs=store_kwargs,
            counts=counts,
        )
    except Exception as e:
        logger.warning(f"icechunk ladder failed at node {node} (fail-open, issue #580): {e}")
        counts["icechunk_failed"] += 1
        return {"node": node, "error": f"{type(e).__name__}: {e}"}
    finally:
        counts["icechunk_s"] = counts.get("icechunk_s", 0.0) + (time.perf_counter() - t0)


def local_flag(store_root: str) -> bool:
    return _is_local(store_root)


__all__ = [
    "GEOMETRY_KEYS",
    "LEAF_REFS_NAME",
    "NODE_REFS_NAME",
    "REFS_SPEC",
    "STAGE_COUNTS",
    "ladder_context",
    "leaf_refs_key",
    "pack_units",
    "read_leaf_refs",
    "read_node_refs",
    "stage_hook",
    "stage_node_refs",
    "unpack_units",
    "write_leaf_refs",
    "write_node_refs",
]
