"""Staged dense sweep for ``zagg-pyramid/2`` stores (issue #384; umbrella #381).

The ``/2`` pyramid's above-shard ladder is materialized by **stage workers**,
never by a leaf-reading walk: the fleet's leaf columns (issue #383) already
carry every within-footprint member plus the node-order **universal partial**,
so the sweep reads columns only — a raw leaf is never opened above the shard.

**Tuple grouping is orchestration-only** (#381 point (6)). Ladder orders are
grouped into dispatch tuples of ``tuple_width`` consecutive orders (default
3), dispatched at nodes whose order is ``0 mod tuple_width`` — ``[8,7,6] ->
[5,4,3] -> [2,1,0]`` on an o9 store, ragged finest tuple when ``shard_order``
is not a multiple of the width. A worker owning an order-``D`` subtree is a
:mod:`zagg.sweep_partition` partition with the split at ``D``: same prefix
ownership, same disjointness.

**The merge-source law (espg ruling, 2026-08-09, issue #384).** A stage
worker reads exactly its ``4^width`` immediate child columns, ``width``
orders down — nothing ever reads deeper. Each worker's own column carries,
as a pure gather, the **leaf relay-member partial set for its whole
subtree** (the relay: the leaf columns' res-``shard_order + 2`` member —
:func:`zagg.column.relay_resolution`, the coarsest member still folded from
raw since issue #538 moved the leaf tier's coarser members onto a flat
second merge) alongside whatever gatherable members the parent tuple needs.
Every merge, at every level, in every tuple, consumes **only the relayed
gen-1 partials** — never the worker's own outputs, never a previously merged
tier — so the merge tree is a fixed function of the store, independent of
grouping: builds at ``tuple_width=1`` and ``tuple_width=3`` are
byte-identical, and every upfront merge level records ``merges_from_raw: 2``
uniformly (gather levels carry gen-1 content untouched, ``merges_from_raw:
1``). Gen 3 belongs only to the append-later cascade regime (#381 point (7)),
which is unchanged and remains the path for pre-column ``/1`` stores.

**Source classification is derived, not hardcoded**: a level ``(node k,
cells r)`` is a **gather** when ``r >= shard_order`` (its cells nest within
single child footprints — concatenation of child members) and a **merge**
otherwise (its cells span leaves — a k-way fold of relayed partials).

**Scope** (#381 point (11)) is an optional node-prefix set — a MOC, the same
ownership predicate as partitions; a shardmap is sugar (its keys are already
shard prefixes). Scope selects which dispatch nodes are invoked; a dispatched
worker folds **all** children on disk, so an update adjacent to prior data
folds the old neighbors in automatically. Scope composes with ``partitions=``
by MOC intersection. Unscoped discovery is listing-based (the run records,
:func:`zagg.sweep.discover_leaves`); the root ``coverage.moc`` is an
**accelerator only, never truth** — a fleet append with no subsequent sweep
leaves it stale, and discovery must still find the new leaves (espg ruling).

**Concurrency.** Sweeps serialize per store via the admission lease
(:mod:`zagg.sweep_lease` — control plane: no data object is ever locked).
Fleets run concurrently with a live sweep: the worker validates each column's
stamp before and after reading its groups and re-reads on movement, so a
mid-read leaf rewrite never feeds a torn column into a merge; a mid-sweep
append is ordinary under-coverage, recorded and healed by the next sweep.
Stage-written stamps carry the run id: a skip-if-current read that sees a
FOREIGN fresh stamp aborts loudly (:class:`ForeignSweepError` — the backstop
for residual races the lease cannot see), and the run id is also a TERM of
the skip key (issue #417), so a same-second rewrite cannot read as current.

**Soft barriers.** Inter-stage barriers are scheduling preferences, not
correctness (#381 point (6)): a stage run before its finer tuple landed
under-covers loudly (``source_children``) and self-heals on the next pass —
the skip gate keys on summed child generations, so a healed child moves the
parent's generation and forces the rewrite.

**The finisher** (espg ruling: a single designated finisher-worker, never
the 12 base cells) owns the root singletons after the root tuple completes:
the root ``coverage.moc`` refresh, the manifest RMW writing per-entry
actuals into ``pyramid.overviews`` (which also satisfies the PR #397
lifecycle root-touch for the manifest), and lease release as its final act.

**Units and the streamed fold** (issue #586 phase 4). The unit of stage
work is ``(node, window)``: :func:`stage_node` folds ONE window of a dispatch
node, and on a windowed store a node's windows are separate units that share
no object — the enumeration, the per-node close (the all-time fold over the
node's per-window overviews) and their execution live in
:mod:`zagg.sweep_units`. The fold kernels live in :mod:`zagg.sweep_fold` and
run block by block over the output range, so a worker holds one block of
inputs rather than a level; :func:`write_stage_column` writes a column the
same way, one chunk object per block.

Raster hive stores are column-less by construction (§4.6 is written for the
aggregation pipeline): the sweep refuses them loudly; issue #399's
reducer-keyed folds join this orchestration later under the same schema.
"""

from __future__ import annotations

import json
import logging

import numpy as np

from zagg.sweep_fold import (
    ColumnMovedError,
    _gather_slabs,
    _merge_slabs,
    iter_gather,
    refold_on_move,
)

logger = logging.getLogger(__name__)

#: Per-artifact attrs revision for stage-written ladder overviews — the
#: ``/2`` shape §4.4 deferred to the writers: one resolution group per
#: artifact at the entry's ``cells`` member (``k + d``, not ``/1``'s
#: constant-depth formula), regime + merges-from-raw + source_children.
OVERVIEW_SPEC_V2 = "zagg-overview/2"
#: #381 point (7) regimes a stage-written level records.
STAGE_GATHER = "stage-gather"
STAGE_MERGE = "stage-merge"
#: Dispatch cadence default (#381 point (6)).
DEFAULT_TUPLE_WIDTH = 3


#: Row marker for a candidate child whose column could not be READ (open or
#: root-metadata fault) — distinct from ``None`` (no committed column at all,
#: i.e. missing): the two land in different ``source_children`` counters, and
#: a log line in an exited process cannot make that distinction for a reader.
UNREADABLE = object()


def _is_reader(entry) -> bool:
    return isinstance(entry, _ColumnReader)


class ForeignSweepError(RuntimeError):
    """A foreign run's FRESH stamp was seen mid-run: two sweeps are live.

    The lease (:mod:`zagg.sweep_lease`) makes this unreachable in normal
    operation; reaching it means a residual race (TTL-expiry clock skew, a
    zombie worker from a crashed run) — abort loudly, never fold through it.
    """


# ---------------------------------------------------------------------------
# Phase 1: the stage planner — pure functions over the expanded manifest list.
# ---------------------------------------------------------------------------


def ladder_entries(pyramid: dict, shard_order: int) -> list[dict]:
    """The above-shard ladder from a manifest ``pyramid.overviews`` list.

    Returns the entries the STAGED sweep owns — every ``node < shard_order``
    — sorted finest first, each normalized to ``{"node": int, "cells":
    [int]}``. The leaf entry (``node == shard_order``) is the fleet's own
    column and is excluded. Refuses by name a non-``/2`` block, an empty
    list, or a ladder entry carrying more than one member (the fixed ladder
    guarantees exactly one, §4.4) — the sweep must never widen a malformed
    declaration into a plausible schedule.
    """
    from zagg.pyramid import PYRAMID_SPEC_V2

    if not isinstance(pyramid, dict) or pyramid.get("spec") != PYRAMID_SPEC_V2:
        raise ValueError(
            f"staged sweep requires a {PYRAMID_SPEC_V2!r} manifest pyramid declaration "
            f"(got spec {pyramid.get('spec') if isinstance(pyramid, dict) else None!r}); "
            f"/1 stores keep the zagg.sweep_overview path (the retrofit regime)"
        )
    overviews = pyramid.get("overviews")
    if not isinstance(overviews, list) or not overviews:
        raise ValueError("manifest pyramid.overviews is absent or empty — nothing to sweep")
    entries = []
    for e in overviews:
        node, cells = int(e["node"]), [int(c) for c in e["cells"]]
        if node >= int(shard_order):
            continue  # the leaf entry: the fleet's column, never the sweep's
        if len(cells) != 1:
            raise ValueError(
                f"ladder entry {e!r} carries {len(cells)} members; the fixed every-order "
                f"ladder guarantees exactly one (§4.4) — refusing a malformed declaration"
            )
        entries.append({"node": node, "cells": cells})
    return sorted(entries, key=lambda e: -e["node"])


def stage_tuples(shard_order: int, *, tuple_width: int = DEFAULT_TUPLE_WIDTH) -> list[dict]:
    """Group ladder orders into dispatch tuples, finest tuple first.

    Dispatch nodes sit at orders ``0 mod tuple_width``, so every tuple below
    the finest spans exactly ``tuple_width`` orders and the finest tuple is
    ragged when ``shard_order`` is not a multiple of the width. Each item is
    ``{"dispatch": D, "orders": [finest..D], "child_order": C}`` where ``C``
    is the order of the child columns the tuple's workers read — the previous
    tuple's dispatch order, or ``shard_order`` (the leaf columns) for the
    finest tuple. The grouping changes no bytes (#381 point (6) + the
    merge-source law): it is a dispatch knob, never grammar.
    """
    shard_order, tuple_width = int(shard_order), int(tuple_width)
    if tuple_width < 1:
        raise ValueError(f"tuple_width must be >= 1 (got {tuple_width})")
    if shard_order < 1:
        raise ValueError(f"shard_order {shard_order} has no above-shard ladder to sweep")
    tuples = []
    for dispatch in range(0, shard_order, tuple_width):
        child_order = min(dispatch + tuple_width, shard_order)
        tuples.append(
            {
                "dispatch": dispatch,
                "orders": list(range(child_order - 1, dispatch - 1, -1)),
                "child_order": child_order,
            }
        )
    return list(reversed(tuples))


def classify_level(cells: int, *, shard_order: int) -> str:
    """``stage-gather`` or ``stage-merge`` for a ladder level's cell resolution.

    Derived, never hardcoded: cells at or finer than the shard order nest
    within single child footprints (leaf columns carry those members, stage
    columns relay them) — concatenation. Coarser cells span leaves — a k-way
    merge of the relayed gen-1 partials (:func:`zagg.column.relay_resolution`).
    """
    return STAGE_GATHER if int(cells) >= int(shard_order) else STAGE_MERGE


def column_members(
    levels: list,
    node_order: int,
    *,
    shard_order: int,
    cell_order: int,
    relay: int | None = None,
) -> list[int]:
    """Resolutions a stage column at ``node_order`` carries, finest first.

    The ``relay`` member (:func:`zagg.column.relay_resolution` — the
    subtree's leaf res-``shard_order + 2`` partials, the ruled merge-source
    tier; derived from ``levels`` and ``cell_order`` when the levels carry
    the leaf entry that places the members, else passed by the sweep, whose
    :func:`ladder_entries` exclude it)
    unconditionally, plus every gatherable member (``cells >= shard_order``)
    some coarser level (``node < node_order``) will gather. All members are
    pure gathers of the child columns' members at the same resolution —
    gen-1 content, untouched, so ``merges_from_raw`` stays 1 for every group
    and a parent merge that consumes the relay is exactly 2 merges from raw.
    """
    from zagg.column import relay_resolution

    node_order, shard_order = int(node_order), int(shard_order)
    relay = relay_resolution(levels, shard_order, cell_order) if relay is None else int(relay)
    gatherable = {
        int(c)
        for e in levels
        for c in e["cells"]
        if int(e["node"]) < node_order and int(c) >= shard_order
    }
    return sorted(gatherable | {relay}, reverse=True)


# ---------------------------------------------------------------------------
# Phase 2: the stage worker — fold one dispatch node's tuple from its columns.
# ---------------------------------------------------------------------------


def _same_stamp(a: dict | None, b: dict | None) -> bool:
    """Whether two commit-stamp reads witness the same write."""
    return (a or None) == (b or None)


def _foreign_fresh(stamp: dict | None, run_id: str, run_started: str) -> bool:
    """A stage stamp from ANOTHER run, written since this run started.

    Fleet stamps carry no ``run_id`` and are never foreign (fleet ∥ sweep is
    allowed by the concurrency matrix); a foreign STAGE stamp older than this
    run is a completed prior sweep — ordinary input, not a conflict. The
    comparison is STRICT: stamps resolve to whole seconds, so a prior run
    that completed in the second this run started would otherwise read as
    live. A true same-second race escapes the backstop — admission is the
    LEASE's job; this predicate only has to catch the long-lived residuals
    (zombie workers, TTL clock skew), which keep writing well past our start.
    """
    if not isinstance(stamp, dict):
        return False
    rid = stamp.get("run_id")
    return rid is not None and rid != run_id and str(stamp.get("written_at") or "") > run_started


class _ColumnReader:
    """Stamp-validated reads over one child column (leaf or stage).

    One root-metadata GET serves the D4 stamp gate, the provenance attrs, and
    the generation basis together. Every group read is bracketed by the
    **optimistic stamp validation** the lease ruling requires: stamp before,
    read, stamp after — if the stamp moved (a fleet worker rewrote the leaf
    mid-read, the allowed fleet ∥ sweep regime), the read retries against the
    fresh stamp — but only until the reader has SERVED a read. The first
    served read pins its stamp: the fold reads a member block by block (issue
    #586 phase 4), so after that a moved stamp, or a vanished one (a rewrite
    in flight), raises :class:`zagg.sweep_fold.ColumnMovedError` rather than
    hand the fold half of each write, and a stamp that keeps moving raises it
    too. The caller folds the artifact again from fresh readers. A stage
    column stamped by a foreign run SINCE this run started raises
    :class:`ForeignSweepError` (two live sweeps — the lease backstop).

    Handed ``store`` (the invoke's obstore handle at the store root), ``path``
    is the column's key RELATIVE to it and no client is built (issue #610);
    without it, ``path`` is absolute and opened on its own.
    """

    def __init__(self, path: str, *, run_id: str, run_started: str, store_kwargs: dict, store=None):
        from zarr.storage import StorePath

        from zagg.store import open_store, zarr_view

        self.path = path
        self.revalidated = 0
        self._served = False  # whether a read was handed out (pins self.stamp)
        self._run_id, self._run_started = run_id, run_started
        if store is None:
            self._store = open_store(path, read_only=True, **store_kwargs)
        else:
            self._store = StorePath(zarr_view(store), path)
        self._arrays: dict = {}
        self.stamp, self.attrs = self._root()
        self._foreign_guard(self.stamp)

    def _foreign_guard(self, stamp: dict | None) -> None:
        if _foreign_fresh(stamp, self._run_id, self._run_started):
            raise ForeignSweepError(
                f"column {self.path} carries a fresh stamp from foreign sweep run "
                f"{stamp.get('run_id')!r} (written {stamp.get('written_at')}); "
                f"two sweeps are live on this store — aborting (lease backstop)"
            )

    def _root(self) -> tuple[dict | None, dict]:
        """Root attrs + stamp; absent reads ``(None, {})``, corrupt RAISES.

        The distinction feeds ``source_children``: a cleanly absent column is
        ``missing`` (never generated, or a fleet still in flight); a column
        whose root metadata exists but cannot be read is ``unreadable``
        (review finding — the two must not launder into one counter).
        """
        import zarr
        from zarr.errors import GroupNotFoundError

        from zagg.hive import COMMIT_ATTR

        try:
            attrs = dict(zarr.open_group(self._store, path="", mode="r", zarr_format=3).attrs)
        except (FileNotFoundError, GroupNotFoundError):
            return None, {}
        stamp = attrs.get(COMMIT_ATTR)
        return (dict(stamp) if isinstance(stamp, dict) else None), attrs

    @property
    def committed(self) -> bool:
        return self.stamp is not None

    def generation(self) -> tuple:
        """This column's generation basis: its own, or the leaf identity.

        :func:`zagg.column.stamped_generation_key`'s triple: a stage column's
        recorded ``generation`` block (the summed ratchet) or a leaf column's
        identity — one leaf at its stamp's timestamp — plus the run id THIS
        column's own stamp carries (fleet-written ones carry none, review
        finding). The parent's skip gate keys on the SUM of these.
        """
        from zagg.column import COLUMN_ATTR, stamped_generation_key

        block = (self.attrs.get(COLUMN_ATTR) or {}).get("generation")
        return stamped_generation_key(block, self.stamp)

    def _array(self, res: int, name: str):
        """The member's open array, or ``None`` when it is absent.

        Handles are kept for the reader's life so a member read block by block
        (issue #586 phase 4) pays its metadata opens once; absence is never
        cached, and a moved stamp drops every handle (:meth:`_read`).
        """
        import zarr
        from zarr.errors import GroupNotFoundError

        key = (int(res), name)
        if key not in self._arrays:
            try:
                group = zarr.open_group(self._store, path=str(res), mode="r", zarr_format=3)
                self._arrays[key] = group[name]
            except (KeyError, FileNotFoundError, GroupNotFoundError):
                return None  # the member postdates this column: fill
        return self._arrays[key]

    def has(self, res: int, name: str) -> bool:
        """Whether the column carries ``{res}/{name}`` (metadata only)."""
        return self._array(res, name) is not None

    def _read(self, res: int, name: str, cells: slice) -> np.ndarray | None:
        for _attempt in range(3):
            before = self.stamp
            array = self._array(res, name)
            values = None if array is None else array[cells]
            after, attrs = self._root()
            if after is not None and _same_stamp(before, after):
                self._served = True
                return values
            self._foreign_guard(after)
            if self._served:
                raise ColumnMovedError(
                    f"column {self.path} was rewritten after this fold read from it "
                    f"(stamp {'gone' if after is None else 'moved'})"
                )
            logger.info(f"stage sweep: column {self.path} moved mid-read; re-reading")
            self.stamp, self.attrs = after, attrs
            self._arrays.clear()
            self.revalidated += 1
        raise ColumnMovedError(f"column {self.path} stamp kept moving across re-reads")

    def read(self, res: int, name: str) -> np.ndarray | None:
        """One group array, stamp-validated; ``None`` for an absent member.

        An absent FIELD or an absent GROUP both read ``None`` — schema (or
        declaration) evolution: the member postdates this column, and its
        cells contribute fill until the leaf re-runs (review finding: a
        deepened ``overviews`` declaration over existing columns must
        under-cover, never abort the sweep). Every re-read re-runs the
        foreign-fresh guard: a column rewritten mid-read by a FOREIGN sweep
        is the exact race the backstop exists for (review finding).
        """
        return self._read(res, name, slice(None))

    def read_range(self, res: int, name: str, start: int, stop: int) -> np.ndarray | None:
        """Cells ``[start, stop)`` of one group array — :meth:`read`, for a block.

        What the chunk-streamed fold reads (:mod:`zagg.sweep_fold`): only the
        chunk objects covering the range are fetched, under the same stamp
        validation and foreign-fresh guard as a whole-member read.
        """
        return self._read(res, name, slice(int(start), int(stop)))


def _readers_for(
    store,
    children: list,
    windows: list,
    *,
    run_id: str,
    run_started: str,
    store_kwargs: dict,
    counts: dict,
) -> dict:
    """``{child decimal: [reader-or-None per window]}``, stamp-gated.

    An absent or unstamped column reads ``None`` — under-coverage, recorded
    by the caller (the soft-barrier posture: fold what is on disk, loudly).
    An unreadable one counts ``failed`` and also reads ``None``. Every column
    is read through ``store``, the invoke's one handle (issue #610).
    """
    from zagg.column import column_name
    from zagg.sweep import _node_rel

    readers: dict = {}
    for child in children:
        row = []
        for window in windows:
            path = f"{_node_rel(child)}/{column_name(window)}"
            try:
                reader = _ColumnReader(
                    path,
                    run_id=run_id,
                    run_started=run_started,
                    store_kwargs=store_kwargs,
                    store=store,
                )
            except ForeignSweepError:
                raise
            except Exception as e:
                logger.warning(f"stage sweep: unreadable column {path} ({e})")
                counts["failed"] += 1
                row.append(UNREADABLE)
                continue
            row.append(reader if reader.committed else None)
        readers[child] = row
    return readers


def _summed_generation(rows: list) -> dict:
    """The ratchet key: child generations summed (skip-if-current basis).

    ``run_ids`` is the union over the contributing children — issue #417's
    term; see :func:`zagg.column.generation_key`.
    """
    n, stamps, runs = 0, [], set()
    for row in rows:
        for reader in row:
            if _is_reader(reader):
                count, timestamp, run_ids = reader.generation()
                n += count
                stamps.append(timestamp)
                runs.update(run_ids)
    stamps = [t for t in stamps if t is not None]
    return {
        "n_leaves": int(n),
        "max_leaf_timestamp": max(stamps) if stamps else None,
        "run_ids": sorted(runs),
    }


def _dense_rows(readers: dict, node: str, *, depth: int) -> list:
    """The full rank range of ``node``'s children: reader rows or ``None``."""
    from zagg.sweep_overview import _rel_rank

    rows: list = [None] * (4**depth)
    for child, row in readers.items():
        if child.startswith(node) and len(child) == len(node) + depth:
            rows[_rel_rank(child, node)] = row
    return rows


def _stage_fold(
    node: str,
    k: int,
    r: int,
    readers: dict,
    fields: dict,
    *,
    shard_order: int,
    child_order: int,
    relay: int,
    meter=None,
) -> dict | None:
    """Fold one ``(artifact node, level)`` from the dispatch worker's readers.

    ``readers`` is the dispatch node's ``{child decimal: [reader]}`` for ONE
    window; this densifies to the rank range under ``node`` and folds per
    the level's derived regime; ``relay`` is the member a merge reads
    (:func:`zagg.column.relay_resolution`). Returns the fold dict (slabs +
    generation + provenance) or ``None`` when no child contributed. Both
    kernels stream their inputs block by block (:mod:`zagg.sweep_fold`,
    issue #586 phase 4); ``meter`` accounts for what a block holds. The
    all-time fold of a windowed store is not made here: it merges the node's
    own per-window overviews (:func:`zagg.sweep_units.close_node`).
    """
    rows = _dense_rows(readers, node, depth=child_order - k)
    n_out = 4 ** (r - k)
    regime = classify_level(r, shard_order=shard_order)
    if regime == STAGE_GATHER:
        slabs, folded, missing, unreadable, demotions = _gather_slabs(
            rows, fields, res=r, span=4 ** (r - child_order), n_out=n_out, meter=meter
        )
        merges_from_raw = 1
    else:
        slabs, folded, missing, unreadable, demotions = _merge_slabs(
            rows,
            fields,
            res_src=int(relay),
            src_per_child=4 ** (int(relay) - child_order),
            factor=4 ** (int(relay) - r),
            n_out=n_out,
            meter=meter,
        )
        merges_from_raw = 2
    if folded == 0:
        # No contributor folded cleanly — the level is not materialized, and
        # any packed-rail record dies with it (issue #518, spec §4.3). That
        # is load-bearing for the mis-declared-divisor shape, which fires on
        # EVERY contributor: the `/2` level vanishes rather than landing with
        # a `demotions` record, so the key's absence here is not evidence the
        # rail stayed quiet. Pinned in
        # ``tests/test_demotion_attrs.py::test_every_contributor_firing_drops_the_level``
        # and standing for review — flipping it means making a packed-rail
        # firing a per-field demotion rather than a whole-contributor verdict
        # in ``_merge_slabs``'s ``broken`` set, which is committed
        # ``source_children`` semantics predating #518.
        return None
    return _fold_result(
        node,
        k,
        r,
        fields,
        rows,
        slabs,
        regime=regime,
        merges_from_raw=merges_from_raw,
        source_children=(folded, missing, unreadable),
        demotions=demotions,
    )


def _fold_result(
    node, k, r, fields, rows, slabs, *, regime, merges_from_raw, source_children, demotions
) -> dict:
    """The fold dict a stage overview is written from — one shape, both units.

    ``rows`` are the reader rows the fold consumed (child columns for a
    window unit, the node's per-window overviews for the all-time fold): their
    stamps give the granule count and the time-range union, their generations
    the summed skip key.
    """
    from zagg.sweep_overview import _content_hash
    from zagg.windows import union_time_range

    granules, ranges = 0, []
    for row in rows:
        for reader in row or ():
            if _is_reader(reader) and reader.stamp:
                granules += int(reader.stamp.get("granule_count") or 0)
                if reader.stamp.get("time_range") is not None:
                    ranges.append(reader.stamp["time_range"])
    folded, missing, unreadable = source_children
    fold = {
        "slabs": slabs,
        "generation": _summed_generation([row for row in rows if row is not None]),
        "content_hash": _content_hash(node, k, r, fields, slabs),
        "granule_count": granules,
        "time_range": union_time_range(*ranges) if ranges else None,
        "regime": regime,
        "merges_from_raw": int(merges_from_raw),
        "source_children": {
            "folded": int(folded),
            "missing": int(missing),
            "unreadable": int(unreadable),
        },
    }
    if demotions:
        # The packed guard rail fired at this node (issue #518): carry the
        # record to the writer so the artifact says so (spec §4.3), keyed
        # only when non-empty — a clean fold's attrs are byte-identical.
        fold["demotions"] = demotions
    return fold


def _write_stage_overview(
    store_root,
    node,
    k,
    key,
    r,
    fold,
    fields,
    shard_order,
    cell_order,
    windowed,
    run_id,
    store_kwargs,
) -> str:
    """Write one ``/2`` ladder overview zarr at its node; returns the basename.

    The ``zagg-overview/2`` per-artifact shape §4.4 deferred to the writers:
    ONE resolution group at the entry's own ``cells`` member (``r = k + d``,
    not the ``/1`` constant-depth formula), attrs carrying the #381 point (7)
    provenance (regime, merges-from-raw, ``source_children``) and the run id.
    Write order is the D4 discipline: template (wholesale) -> arrays ->
    role/provenance attrs -> commit stamp LAST (carrying ``run_id`` — the
    lease ruling's backstop), then the D20 sidecar as a fail-open sibling.
    """
    import zarr
    from mortie import generate_morton_children
    from zarr import open_array

    from zagg.content_hash import staged_record
    from zagg.grids.healpix import HealpixGrid
    from zagg.grids.morton import morton_word
    from zagg.hive import _utcnow, stamp_commit
    from zagg.store import open_store
    from zagg.sweep_overview import (
        OVERVIEW_ATTR,
        ROLE_ATTR,
        _field_provenance,
        _overview_basename,
        _overview_config,
        _populated_mask,
    )

    basename = _overview_basename(key)
    path = f"{store_root}/{_node_rel(node)}/{basename}"
    grid = HealpixGrid(int(k), int(r), config=_overview_config(fields), sharded=True)
    store = open_store(path, **store_kwargs)
    grid.emit_shard_template(store, overwrite=True)
    words = np.asarray(generate_morton_children(morton_word(node), int(r)), dtype=np.uint64)
    arr = open_array(store, path=f"{r}/morton", zarr_format=3, consolidated=False)
    arr[:] = words
    for name, slab in fold["slabs"].items():
        arr = open_array(store, path=f"{r}/{name}", zarr_format=3, consolidated=False)
        arr[:] = slab
    populated = _populated_mask(fold["slabs"], fields)
    root = zarr.open_group(store, path="", mode="r+", zarr_format=3)
    provenance = {
        "spec": OVERVIEW_SPEC_V2,
        "node": node,
        "order": int(k),
        "cell_order": int(r),
        "source_shard_order": int(shard_order),
        "source_cell_order": int(cell_order),
        "window": key,
        "fields": {n: _field_provenance(m) for n, m in fields.items()},
        "regime": fold["regime"],
        "merges_from_raw": int(fold["merges_from_raw"]),
        "source_children": dict(fold["source_children"]),
    }
    if fold.get("source_windows") is not None:
        # The all-time fold of a windowed store (issue #586 phase 4): its
        # direct sources are the node's per-window overviews, counted here.
        # Keyed only on that artifact, so every other one is byte-identical.
        provenance["source_windows"] = dict(fold["source_windows"])
    if fold.get("demotions"):
        # Artifact-visible packed-rail demotions (issue #518, spec §4.3) —
        # keyed only when the rail fired, so a clean level's attrs are
        # byte-identical to a pre-#518 writer's.
        provenance["demotions"] = list(fold["demotions"])
    provenance.update(
        {
            "generation": fold["generation"],
            "content_hash": fold["content_hash"],
            "run_id": run_id,
            "generated_at": _utcnow(),
        }
    )
    root.attrs.update({ROLE_ATTR: "overview", OVERVIEW_ATTR: provenance})
    stamp_window = key if windowed else None
    # §5 O11 record BEFORE the stamp so it rides it (issue #580; the /1
    # writer's posture in ``sweep_overview._write_overview``).
    staged = {f"{r}/morton": words}
    staged.update({f"{r}/{name}": slab for name, slab in fold["slabs"].items()})
    hashes = staged_record(store, staged, f"stage sweep at {node}/{basename}")
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(fold["granule_count"]),
        window=stamp_window,
        time_range=fold["time_range"] if stamp_window is not None else None,
        run_id=run_id,
        content_hashes=hashes,
    )
    try:  # D20 sidecar: fail-open telemetry, the same §5 O11 record
        from zagg.telemetry import SPEC_V3, build_record, write_sidecar

        record = build_record(
            shard_key=morton_word(node),
            metadata={
                "cells_with_data": int(populated.sum()),
                "granule_count": int(fold["granule_count"]),
                "content_hashes": hashes,
            },
            window=stamp_window,
        )
        write_sidecar(path, record, spec=SPEC_V3, **store_kwargs)
    except Exception as e:
        logger.warning(f"stage sweep: O11 sidecar failed at {node}/{basename} ({e})")
    return basename


def _column_group_spec(node_order: int, res: int, cfg):
    """One stage-column resolution group's spec, and its cells per chunk.

    A group no wider than one fold block is ONE chunk per array — the layout
    every stage column had before issue #586 phase 4, byte for byte. A wider
    group is laid on regular inner chunks of exactly one block
    (:func:`zagg.sweep_fold.block_cells`), one object per chunk and no
    ShardingCodec: the gather then writes a block as one chunk object and
    drops it, and a parent reads back only the chunks its own block covers.
    (A sharded array would re-read and re-PUT its whole shard object on every
    block.) The chunking is a function of ``(node_order, res)`` alone.
    """
    from zagg.grids.healpix import HealpixGrid
    from zagg.sweep_fold import STAGE_BLOCK_ORDER

    if res - node_order <= STAGE_BLOCK_ORDER:
        grid = HealpixGrid(node_order, res, config=cfg, sharded=True)
        return grid.shard_spec(), grid.cells_per_shard
    grid = HealpixGrid(
        node_order, res, config=cfg, chunk_inner=res - STAGE_BLOCK_ORDER, sharded=False
    )
    return grid.chunked_spec(), grid.cells_per_chunk


def write_stage_column(
    store_root,
    node,
    rows: list,
    fields: dict,
    *,
    members: list,
    child_order: int,
    node_order: int,
    relay: int,
    cell_order: int,
    generation: dict,
    window: str | None = None,
    time_range=None,
    granule_count: int = 0,
    run_id: str | None = None,
    store_kwargs: dict | None = None,
    meter=None,
) -> dict | None:
    """Gather and write one stage column at its dispatch node, block by block.

    ``rows`` is the dispatch node's dense child range (reader rows, one
    window) and ``members`` the resolutions the column carries
    (:func:`column_members`). Returns ``{"object", "source_children"}`` — the
    basename and the relay member's coverage counters — or ``None`` when the
    relay member folds nothing, in which case NOTHING is written and an
    existing column is left as it was. Otherwise the committed column is
    cleared BEFORE the streamed reads that feed it, so a read that fails
    mid-stream leaves it cleared and unstamped: the next tuple reads the child
    as missing (under-coverage, recorded) until a later pass rewrites it — the
    column is a regenerable cache (spec §4.1). :func:`stage_node` therefore
    retries a failed column once, from fresh readers, before counting it.

    The §4.6 column artifact shape (``zagg-column/1``) with the stage
    regime: every group is a PURE GATHER of the child columns' members at
    the same resolution — the ``relay`` (:func:`zagg.column.relay_resolution`:
    the subtree's leaf res-``shard_order + 2`` partials, the ruled
    merge-source tier) plus the gatherable members coarser tuples need — so
    ``merges_from_raw`` stays 1 for every group and the artifact carries
    gen-1 content only. The relay member is the one group a parent merge may
    assume; ``members`` without it is refused by name. Attrs additionally
    record the summed ``generation``
    (the parent's skip-gate basis), ``source_children`` (a gather that
    under-covered says so in the artifact), and the run id; the commit stamp
    carries ``run_id`` too (lease backstop). D4 order throughout; the D20
    sidecar lands after the stamp, fail-open. Single-writer law: the column
    lives under its own node prefix, one writer per lease-serialized run.

    **Streamed** (issue #586 phase 4): each group is gathered and written one
    fold block at a time (:func:`zagg.sweep_fold.iter_gather`) — read the
    child members covering the block, assign, write the chunk, drop it — so
    the worker holds one block's inputs, not the level's. The §5 O11 record
    is accumulated across blocks (:func:`zagg.content_hash.update_hash`, one
    live digest per array) and written once, at the stamp; so are the
    populated-cell count and the relay member's ``source_children``.
    """
    import hashlib

    import zarr
    from mortie import generate_morton_children
    from pydantic_zarr.experimental.v3 import GroupSpec
    from zarr import config as zarr_config
    from zarr import open_array
    from zarr.core.sync import sync

    from zagg.column import (
        COLUMN_ATTR,
        COLUMN_ROLE,
        COLUMN_SPEC,
        _column_provenance,
        _delete_sidecar,
        _sidecar_name,
        _write_sidecar,
        column_name,
        composable_fields,
    )
    from zagg.content_hash import streamed_record, update_hash
    from zagg.grids.base import vlen_dtype_warning_suppressed
    from zagg.grids.morton import morton_word
    from zagg.hive import _utcnow, stamp_commit
    from zagg.store import open_store
    from zagg.sweep_fold import _source_counts, broken_groups
    from zagg.sweep_overview import ROLE_ATTR, _overview_config, _populated_mask
    from zagg.windows import SCHEDULE_NONE_TOKEN

    store_kwargs = dict(store_kwargs or {})
    node_order, child_order, relay = int(node_order), int(child_order), int(relay)
    fields = composable_fields(fields)
    resolutions = sorted((int(r) for r in members), reverse=True)
    if relay not in resolutions:
        raise ValueError(
            f"a stage column must carry the relay member ({relay} — the subtree's leaf "
            f"raw-fold-boundary partials, the ruled merge-source tier); got {resolutions}"
        )
    # Known BEFORE the prefix is cleared: presence alone decides which
    # contributors the relay gather refuses, so a column whose relay member
    # folds nothing is never started.
    folded_n, missing, unreadable = _source_counts(rows, broken_groups(rows, fields, res=relay))
    if folded_n == 0:
        return None
    source_children = {
        "folded": int(folded_n),
        "missing": int(missing),
        "unreadable": int(unreadable),
    }
    node_prefix = f"{store_root}/{_node_rel(node)}"
    basename = column_name(window)
    path = f"{node_prefix}/{basename}"
    cfg = _overview_config(fields)
    specs = {res: _column_group_spec(node_order, res, cfg) for res in resolutions}
    spec = GroupSpec(members={str(res): specs[res][0] for res in resolutions}, attributes={})
    store = open_store(path, **store_kwargs)
    hashers: dict = {}
    populated = 0
    with zarr_config.set({"async.concurrency": 128}), vlen_dtype_warning_suppressed():
        sync(store.delete_dir(""))
        _delete_sidecar(node_prefix, _sidecar_name(basename), store_kwargs)
        spec.to_zarr(store, "", overwrite=True)
        for res in resolutions:
            words = np.asarray(generate_morton_children(morton_word(node), res), dtype=np.uint64)
            arr = open_array(store, path=f"{res}/morton", zarr_format=3, consolidated=False)
            arr[:] = words
            update_hash(hashers.setdefault(f"{res}/morton", hashlib.sha256()), words)
            arrays: dict = {}
            for lo, hi, slabs in iter_gather(
                rows,
                fields,
                res=res,
                span=4 ** (res - child_order),
                n_out=4 ** (res - node_order),
                broken=set(),
                block=specs[res][1],
                meter=meter,
            ):
                for name, slab in slabs.items():
                    key = f"{res}/{name}"
                    if name not in arrays:
                        arrays[name] = open_array(
                            store, path=key, zarr_format=3, consolidated=False
                        )
                    arrays[name][lo:hi] = slab
                    update_hash(hashers.setdefault(key, hashlib.sha256()), slab)
                if res == resolutions[0]:
                    populated += int(_populated_mask(slabs, fields).sum())
    root = zarr.open_group(store, path="", mode="r+", zarr_format=3)
    root.attrs.update(
        {
            ROLE_ATTR: COLUMN_ROLE,
            COLUMN_ATTR: {
                "spec": COLUMN_SPEC,
                "node": node,
                "order": node_order,
                "source_cell_order": int(cell_order),
                "window": window if window is not None else SCHEDULE_NONE_TOKEN,
                "fields": {n: _column_provenance(m) for n, m in fields.items()},
                "groups": {
                    str(res): {
                        "regime": STAGE_GATHER,
                        "merges_from_raw": 1,
                        "n_cells": 4 ** (res - node_order),
                    }
                    for res in resolutions
                },
                "generation": dict(generation),
                "source_children": source_children,
                "cells_with_data_order": resolutions[0],
                "run_id": run_id,
                "generated_at": _utcnow(),
            },
        }
    )
    # §5 O11 record BEFORE the stamp so it rides it (issue #580), then the
    # sidecar carries the SAME record — ``_write_sidecar`` takes the finished
    # record, exactly as ``column.write_column`` does.
    hashes = streamed_record(hashers, f"stage column {node}/{basename}")
    stamp_commit(
        store,
        cells_with_data=populated,
        granule_count=int(granule_count),
        window=window,
        time_range=time_range if window is not None else None,
        run_id=run_id,
        content_hashes=hashes,
    )
    # No record -> no sidecar, the leaf column's gate: a hash-less sidecar on
    # a rewrite reads as a stale-or-absent ambiguity, and the stamp above
    # already stands without the key (spec §5.3, unverifiable not tampered).
    if hashes is not None:
        _write_sidecar(
            store,
            path,
            morton_word(node),
            hashes,
            populated,
            granule_count,
            window,
            store_kwargs,
        )
    return {"object": basename, "source_children": source_children}


def _node_rel(decimal: str) -> str:
    from zagg.sweep import _node_rel as rel

    return rel(decimal)


def _artifact_stamp(store, node, basename, run_id, run_started) -> dict | None:
    """A stage artifact's commit stamp (``None`` when absent), foreign-gated.

    The ruled backstop lives here: a skip-if-current read that encounters a
    FOREIGN stamp written since this run started aborts loudly rather than
    trusting or overwriting a live sibling sweep's output. One GET through
    ``store``, the invoke's one handle at the store root (issue #610).
    """
    from zarr.storage import StorePath

    from zagg.hive import read_commit
    from zagg.store import zarr_view

    if not basename:
        return None
    try:
        stamp = read_commit(StorePath(zarr_view(store), f"{_node_rel(node)}/{basename}"))
    except Exception as e:
        logger.debug(f"stage sweep: cannot confirm {node}/{basename} ({e})")
        return None
    if _foreign_fresh(stamp, run_id, run_started):
        raise ForeignSweepError(
            f"stage artifact {node}/{basename} carries a fresh stamp from foreign sweep run "
            f"{stamp.get('run_id')!r} (written {stamp.get('written_at')}); two sweeps are "
            f"live on this store — aborting (lease backstop)"
        )
    return stamp


def _artifact_entry(store, node, basename, run_id, run_started) -> dict | None:
    """A committed stage overview's skip-gate entry, read off its OWN attrs.

    ``None`` when the artifact is absent, unstamped or carries no
    ``zagg-overview/2`` block. The same fields the node envelope records per
    window (``generation``, ``regime``, ``merges_from_raw``,
    ``source_children``, ``content_hash``, ``run_id``) — every one of them is
    written into the artifact's attrs first — so a ``(node, window)`` unit
    (issue #586 phase 4) decides skip-if-current from the one object it alone
    writes, and N concurrent window units of a node share no read-modify-write.
    One GET, the one :func:`_artifact_stamp` already made, through the
    invoke's one handle (issue #610); foreign-gated the same way.
    """
    import zarr

    from zagg.hive import COMMIT_ATTR
    from zagg.store import zarr_view
    from zagg.sweep_overview import OVERVIEW_ATTR

    try:
        group = zarr.open_group(
            zarr_view(store), path=f"{_node_rel(node)}/{basename}", mode="r", zarr_format=3
        )
        attrs = dict(group.attrs)
    except Exception as e:
        logger.debug(f"stage sweep: cannot confirm {node}/{basename} ({e})")
        return None
    stamp, block = attrs.get(COMMIT_ATTR), attrs.get(OVERVIEW_ATTR)
    if not isinstance(stamp, dict) or not isinstance(block, dict):
        return None
    if _foreign_fresh(stamp, run_id, run_started):
        raise ForeignSweepError(
            f"stage artifact {node}/{basename} carries a fresh stamp from foreign sweep run "
            f"{stamp.get('run_id')!r} (written {stamp.get('written_at')}); two sweeps are "
            f"live on this store — aborting (lease backstop)"
        )
    if block.get("spec") != OVERVIEW_SPEC_V2:
        return None
    return {"object": basename, **block}


def stage_node(
    store,
    store_root,
    node,
    stage,
    levels,
    fields,
    *,
    key,
    window,
    windowed,
    shard_order,
    cell_order,
    relay,
    candidates,
    run_id,
    run_started,
    counts,
    store_kwargs,
    level_actuals=None,
    envelope=True,
    meter=None,
) -> None:
    """One ``(node, window)`` stage unit: fold a dispatch node's tuple for one window.

    ``window`` is the unit's window label (``None`` on an unwindowed store)
    and ``key`` its overview key (the label, or the reserved ``all`` token of
    an unwindowed store). ``relay`` is the merge-source member
    (:func:`zagg.column.relay_resolution` over the manifest's FULL
    ``overviews`` list — ``levels`` here are the above-shard
    :func:`ladder_entries`, which exclude the leaf entry that places it).

    Reads the node's candidate child columns once (stamp-validated), then per
    tuple order materializes every dirty artifact node beneath the dispatch
    node — skip-if-current keyed on SUMMED CHILD GENERATIONS (the ratchet:
    a healed or appended child moves the sum and forces the rewrite; a
    content hash cannot be had without folding, so #417's count/timestamp/
    run-id key is the gate) — and finally its own stage column (relay +
    gatherables), unless this is the root tuple (no parent consumes it).
    Every fold streams its inputs block by block (:mod:`zagg.sweep_fold`).

    ``envelope`` is whether this unit owns the node's sweep-internal envelope
    (``overview.rollup.json``). It does on an unwindowed store, where the unit
    is the node's only writer. A windowed store's window units run
    concurrently (issue #586 phase 4), so they pass ``False``: the gate reads
    the artifact's own attrs (:func:`_artifact_entry`) and no shared object is
    read-modify-written. The all-time fold across windows is not a window
    unit's work (:func:`zagg.sweep_units.close_node`).
    """
    from functools import partial

    from zagg.column import generation_key
    from zagg.sweep_overview import ENVELOPE_NAME, _overview_basename, _read_envelope
    from zagg.windows import union_time_range

    dispatch, child_order = int(stage["dispatch"]), int(stage["child_order"])
    relay = int(relay)
    level_by_order = {int(e["node"]): int(e["cells"][0]) for e in levels}
    orders = [k for k in stage["orders"] if k in level_by_order]
    children = sorted({_node_at(d, child_order) for d in candidates if d.startswith(node)})
    reader_args = dict(run_id=run_id, run_started=run_started, store_kwargs=store_kwargs)
    readers = _readers_for(store, children, [window], counts=counts, **reader_args)
    retired: list = []

    def _fresh_readers():
        # In place: every fold below holds this dict. An unreadable column was
        # counted the first time; its UNREADABLE marker still says so.
        retired.extend(r for row in readers.values() for r in row if _is_reader(r))
        readers.update(_readers_for(store, children, [window], counts={"failed": 0}, **reader_args))

    dispatch_level_current = False
    for k in orders:
        r = level_by_order[k]
        regime_plan = classify_level(r, shard_order=shard_order)
        for target in sorted({_node_at(d, k) for d in candidates if d.startswith(node)}):
            rows = [row for c, row in readers.items() if c.startswith(target)]
            fresh_gen = _summed_generation(rows)
            if envelope:
                stored = _read_envelope(store, target)
                entries = dict((stored or {}).get("windows") or {})
                entry = entries.get(key)
            else:
                entry = _artifact_entry(store, target, _overview_basename(key), run_id, run_started)
            if (
                isinstance(entry, dict)
                and generation_key(entry.get("generation")) == generation_key(fresh_gen)
                and entry.get("regime") == regime_plan
                # An envelope entry is a claim about another object: confirm
                # its stamp. An attrs entry was read off the committed artifact.
                and (
                    not envelope
                    or _artifact_stamp(store, target, entry.get("object"), run_id, run_started)
                    is not None
                )
            ):
                counts["current"] += 1
                _accumulate_actuals(
                    level_actuals,
                    k,
                    r,
                    target,
                    key,
                    entry.get("regime"),
                    entry.get("merges_from_raw"),
                    entry.get("source_children"),
                )
                if k == dispatch and target == node:
                    dispatch_level_current = True
                continue
            try:
                fold = refold_on_move(
                    partial(
                        _stage_fold,
                        target,
                        k,
                        r,
                        readers,
                        fields,
                        shard_order=shard_order,
                        child_order=child_order,
                        relay=relay,
                        meter=meter,
                    ),
                    _fresh_readers,
                    f"node {target} window {key!r}",
                )
            except ColumnMovedError as e:
                logger.warning(f"stage sweep: fold failed at node {target} window {key!r} ({e})")
                counts["failed"] += 1
                _fresh_readers()  # the next artifact starts from unpinned readers
                continue
            if fold is None:
                counts["empty"] += 1
                continue
            if fold["source_children"]["missing"]:
                counts["under_covered"] += 1
            try:
                basename = _write_stage_overview(
                    store_root,
                    target,
                    k,
                    key,
                    r,
                    fold,
                    fields,
                    shard_order,
                    cell_order,
                    windowed,
                    run_id,
                    store_kwargs,
                )
            except Exception as e:
                logger.warning(f"stage sweep: write failed at node {target} window {key!r} ({e})")
                counts["failed"] += 1
                continue
            counts["written"] += 1
            _accumulate_actuals(
                level_actuals,
                k,
                r,
                target,
                key,
                fold["regime"],
                fold["merges_from_raw"],
                fold["source_children"],
            )
            if not envelope:
                continue
            entries[key] = {
                "object": basename,
                "generation": fold["generation"],
                "content_hash": fold["content_hash"],
                "regime": fold["regime"],
                "merges_from_raw": int(fold["merges_from_raw"]),
                "source_children": dict(fold["source_children"]),
                "run_id": run_id,
            }
            fresh = {
                "spec": _sweep_spec(),
                "family": "overview",
                "node": target,
                "order": int(k),
                "windows": entries,
            }
            if fresh != stored:
                from zagg.store import put_object

                put_object(
                    store,
                    f"{_node_rel(target)}/{ENVELOPE_NAME}",
                    json.dumps(fresh, indent=1).encode(),
                )
    counts["revalidated"] += sum(r.revalidated for r in retired) + sum(
        r.revalidated for row in readers.values() for r in row if _is_reader(r)
    )
    if dispatch == 0:
        return
    members = column_members(
        levels, dispatch, shard_order=shard_order, cell_order=cell_order, relay=relay
    )
    fresh_gen = _summed_generation(list(readers.values()))
    if dispatch_level_current and _stage_column_current(
        store_root, node, window, fresh_gen, run_id, run_started, store_kwargs
    ):
        counts["columns_current"] += 1
        return

    def _write_column():
        granules, ranges = 0, []
        for row in readers.values():
            for reader in row:
                if _is_reader(reader) and reader.stamp:
                    granules += int(reader.stamp.get("granule_count") or 0)
                    if reader.stamp.get("time_range") is not None:
                        ranges.append(reader.stamp["time_range"])
        return write_stage_column(
            store_root,
            node,
            _dense_rows(readers, node, depth=child_order - dispatch),
            fields,
            members=members,
            child_order=child_order,
            node_order=dispatch,
            relay=relay,
            cell_order=cell_order,
            generation=_summed_generation(list(readers.values())),
            window=window,
            time_range=union_time_range(*ranges) if ranges else None,
            granule_count=granules,
            run_id=run_id,
            store_kwargs=store_kwargs,
            meter=meter,
        )

    try:
        # The column is cleared before its streamed reads, so a read failing
        # mid-stream would leave the node with none: ANY failure is retried
        # once, from fresh readers, before it is counted.
        written = refold_on_move(
            _write_column,
            _fresh_readers,
            f"the stage column at node {node} window {key!r}",
            retry_on=(Exception,),
        )
    except ForeignSweepError:
        raise
    except Exception as e:
        logger.warning(f"stage sweep: column write failed at node {node} window {key!r} ({e})")
        counts["failed"] += 1
        return
    if written is None:
        return
    counts["columns_written"] += 1
    if written["source_children"]["missing"]:
        counts["under_covered"] += 1


def _stage_column_current(
    store_root, node, window, fresh_gen, run_id, run_started, store_kwargs
) -> bool:
    """Whether the dispatch node's column is committed at the fresh generation."""
    import zarr

    from zagg.column import COLUMN_ATTR, column_name, generation_key
    from zagg.hive import COMMIT_ATTR
    from zagg.store import open_store

    path = f"{store_root}/{_node_rel(node)}/{column_name(window)}"
    try:
        attrs = dict(
            zarr.open_group(
                open_store(path, read_only=True, **store_kwargs), path="", mode="r", zarr_format=3
            ).attrs
        )
    except Exception:
        return False
    stamp = attrs.get(COMMIT_ATTR)
    if not isinstance(stamp, dict):
        return False
    if _foreign_fresh(stamp, run_id, run_started):
        raise ForeignSweepError(
            f"stage column {path} carries a fresh stamp from foreign sweep run "
            f"{stamp.get('run_id')!r}; two sweeps are live on this store — aborting"
        )
    stored = (attrs.get(COLUMN_ATTR) or {}).get("generation")
    return generation_key(stored) == generation_key(fresh_gen)


def _sweep_spec() -> str:
    from zagg.sweep import SWEEP_SPEC

    return SWEEP_SPEC


def _node_at(decimal: str, order: int) -> str:
    from zagg.sweep_overview import _node_at as at

    return at(decimal, order)


def _accumulate_actuals(
    level_actuals, k, r, target, key, regime, merges_from_raw, source_children
) -> None:
    """Record one artifact's provenance into the run's per-LEVEL actuals.

    Keyed per ``(artifact node, window)`` and ASSIGNED, never added — a
    partitioned or multi-pass run re-visits shared coarse ancestors
    (skip-if-current makes that cheap), and summing per visit would inflate
    the manifest's coverage counts by the visit count (review finding).
    :func:`aggregate_actuals` sums the per-artifact rows once, at the end.
    Per-window folds only — the all-time fold is a separate family of
    artifacts whose regime rides their own attrs, and letting its
    ``stage-merge`` override a gather level's recorded regime would
    misdescribe the product declaration the entry stands for.
    """
    if level_actuals is None:
        return
    entry = level_actuals.setdefault(
        int(k),
        {
            "cells": int(r),
            "regime": regime,
            "merges_from_raw": int(merges_from_raw or 0),
            "children": {},
        },
    )
    entry["children"][f"{target}|{key}"] = {
        name: int((source_children or {}).get(name) or 0)
        for name in ("folded", "missing", "unreadable")
    }


def aggregate_actuals(level_actuals: dict) -> dict:
    """The per-level view the finisher records: per-artifact rows summed."""
    out: dict = {}
    for k, entry in level_actuals.items():
        summed = {"folded": 0, "missing": 0, "unreadable": 0}
        for row in entry["children"].values():
            for name in summed:
                summed[name] += row[name]
        out[int(k)] = {
            "cells": entry["cells"],
            "regime": entry["regime"],
            "merges_from_raw": entry["merges_from_raw"],
            "source_children": summed,
        }
    return out
