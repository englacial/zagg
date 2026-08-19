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
as a pure gather, the **leaf node-order partial set for its whole subtree**
(the relay) alongside whatever gatherable members the parent tuple needs.
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
the skip key, with the granule count for the fleet-written children that
carry none (issues #417/#433), so a same-second rewrite cannot read as current.

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

Raster hive stores are column-less by construction (§4.6 is written for the
aggregation pipeline): the sweep refuses them loudly; issue #399's
reducer-keyed folds join this orchestration later under the same schema.
"""

from __future__ import annotations

import json
import logging

import numpy as np

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
    merge of the relayed gen-1 node-order partials.
    """
    return STAGE_GATHER if int(cells) >= int(shard_order) else STAGE_MERGE


def column_members(levels: list, node_order: int, *, shard_order: int) -> list[int]:
    """Resolutions a stage column at ``node_order`` carries, finest first.

    The relay member (``shard_order`` — the subtree's leaf node-order
    partials, the ruled merge-source tier) unconditionally, plus every
    gatherable member (``cells >= shard_order``) some coarser level
    (``node < node_order``) will gather. All members are pure gathers of the
    child columns' members at the same resolution — gen-1 content, untouched,
    so ``merges_from_raw`` stays 1 for every group and a parent merge that
    consumes the relay is exactly 2 merges from raw.
    """
    node_order, shard_order = int(node_order), int(shard_order)
    gatherable = {
        int(c)
        for e in levels
        for c in e["cells"]
        if int(e["node"]) < node_order and int(c) >= shard_order
    }
    return sorted(gatherable | {shard_order}, reverse=True)


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
    fresh stamp, so a merge never consumes a torn (mixed-generation) column.
    A stamp that keeps moving reads as unreadable, never as data. A stage
    column stamped by a foreign run SINCE this run started raises
    :class:`ForeignSweepError` (two live sweeps — the lease backstop).
    """

    def __init__(self, path: str, *, run_id: str, run_started: str, store_kwargs: dict):
        from zagg.store import open_store

        self.path = path
        self.revalidated = 0
        self._run_id, self._run_started = run_id, run_started
        self._store = open_store(path, read_only=True, **store_kwargs)
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

        :func:`zagg.column.stamped_generation_key`'s four terms: a stage
        column's recorded ``generation`` block (the summed ratchet) or a leaf
        column's identity — one leaf at its stamp's timestamp — plus the run
        id and granule count THIS column's own stamp carries (fleet-written
        ones carry no id, only a count). The parent's gate keys on the SUM.
        """
        from zagg.column import COLUMN_ATTR, stamped_generation_key

        block = (self.attrs.get(COLUMN_ATTR) or {}).get("generation")
        return stamped_generation_key(block, self.stamp)

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
        import zarr
        from zarr.errors import GroupNotFoundError

        for _attempt in range(3):
            before = self.stamp
            try:
                group = zarr.open_group(self._store, path=str(res), mode="r", zarr_format=3)
                values = group[name][:]
            except (KeyError, FileNotFoundError, GroupNotFoundError):
                return None  # the member postdates this column: fill
            after, attrs = self._root()
            if _same_stamp(before, after):
                return values
            self._foreign_guard(after)
            logger.info(f"stage sweep: column {self.path} moved mid-read; re-reading")
            self.stamp, self.attrs = after, attrs
            self.revalidated += 1
        raise ValueError(f"column {self.path} stamp kept moving across re-reads")


def _readers_for(
    store_root: str,
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
    An unreadable one counts ``failed`` and also reads ``None``.
    """
    from zagg.column import column_name
    from zagg.sweep import _node_rel

    readers: dict = {}
    for child in children:
        row = []
        for window in windows:
            path = f"{store_root}/{_node_rel(child)}/{column_name(window)}"
            try:
                reader = _ColumnReader(
                    path, run_id=run_id, run_started=run_started, store_kwargs=store_kwargs
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

    ``run_ids`` unions the contributing children's, ``granule_count`` sums
    them (issues #417/#433); see :func:`zagg.column.generation_key`.
    """
    n, granules, stamps, runs = 0, 0, [], set()
    for row in rows:
        for reader in row:
            if _is_reader(reader):
                count, timestamp, run_ids, granule_count = reader.generation()
                n += count
                stamps.append(timestamp)
                runs.update(run_ids)
                granules += granule_count
    stamps = [t for t in stamps if t is not None]
    return {
        "n_leaves": int(n),
        "max_leaf_timestamp": max(stamps) if stamps else None,
        "run_ids": sorted(runs),
        "granule_count": int(granules),
    }


def _source_counts(rows: list, broken=()) -> tuple:
    """``(folded, missing, unreadable)`` over the dense child range.

    A child counts **folded** when at least one of its windows delivered a
    usable column, **missing** when every window's column is cleanly absent
    (never generated, or a fleet still in flight), and **unreadable**
    otherwise — the distinction the two counters must not launder into one.

    ``broken`` is the set of ``(child index, window index)`` contributors a
    fold refused MID-READ: a located payload present without its §9 channel
    (:func:`_located_pair`). Such a contributor is not usable, exactly as an
    unreadable column is not, so it is classified here rather than counted as
    a clean fold — the counters are what ``source_children`` records, and an
    artifact folded without a contributor must say so (spec §4.5).
    """
    folded = missing = unreadable = 0
    for i, row in enumerate(rows):
        if row is None:
            continue
        if any(_is_reader(r) and (i, w) not in broken for w, r in enumerate(row)):
            folded += 1
        elif all(r is None for r in row):
            missing += 1
        else:
            unreadable += 1
    return folded, missing, unreadable


def _companion_group(reader, res: int, name: str, declared: list) -> dict | None:
    """One contributor's ``{array: values}`` at ``res``, or ``None`` if refused.

    A field and EVERY companion it declares are read together and validated
    before any of them is used (issue #410). ``_StageReader.read`` returns
    ``None`` per ARRAY, not per member — its documented schema-evolution
    contract — so a column written before ``location:``/``temporal:`` joined the
    declaration reads its payload fine and its siblings as ``None``. That is not
    under-coverage: the words are exact only *given* the centroid partition the
    payload describes (spec §9.1/§8.3/§2.3), so a payload gathered or folded
    without its words is **corruption**, not a missing member. All absent
    together is the honest schema-evolution case and reads as fill.

    Returns ``None`` for a partial group; the caller counts the contributor
    unreadable and skips it, which is the posture
    ``sweep_overview._fold_node`` takes at a leaf missing a sibling (skip
    loudly, ``failed += 1``) rather than aborting the level — a single stale
    child column must not take a whole stage down.
    """
    arrays = {name: reader.read(res, name)}
    for _kwarg, sibling in declared:
        arrays[sibling] = reader.read(res, sibling)
    present = [k for k, v in arrays.items() if v is not None]
    if not present or len(present) == len(arrays):
        return arrays
    logger.warning(
        f"stage sweep: column {reader.path} carries {sorted(present)} at resolution {res} "
        f"but not {sorted(set(arrays) - set(present))}; counting the contributor "
        f"unreadable rather than writing a payload and channels that describe different "
        f"partitions (spec §9.1/§8.3, §1.1)"
    )
    return None


def _gather_slabs(rows: list, fields: dict, *, res: int, span: int, n_out: int) -> tuple:
    """Concatenate child members at ``res`` — gen-1 content, untouched.

    ``rows`` is the DENSE rank-ordered child range: ``None`` for a child no
    candidate leaf inhabits (genuinely empty — fill, uncounted), else the
    child's reader row (gathers are per-window by construction, one reader,
    itself ``None`` when the candidate's column is missing — under-coverage,
    counted). Payloads are ASSIGNED, never decoded or re-folded — the
    acceptance contract that gather levels carry gen-1 bytes untouched.
    Returns ``(slabs, folded, missing, unreadable)``.

    A located field's sibling is gathered under the same rule as its payload
    (ruling 4 on issue #410): a gather ASSIGNS gen-1 bytes, so the pair stays
    row-aligned by construction — **given that both arrays are present**, which
    is the one thing the read does not establish. The pair is therefore read
    and validated (:func:`_located_pair`) BEFORE anything is assigned, so a
    child carrying one half contributes neither and is counted unreadable
    rather than half-applied into the span.
    """
    from zagg.sweep_overview import _empty_slab, field_companions

    slabs = {name: _empty_slab(meta, n_out) for name, meta in fields.items()}
    companions = {
        name: field_companions(name, meta)
        for name, meta in fields.items()
        if field_companions(name, meta)
    }
    for declared in companions.values():
        for _kwarg, sibling in declared:
            slabs[sibling] = np.full(n_out, b"", dtype=object)
    broken: set = set()
    for i, row in enumerate(rows):
        if row is None:
            continue
        reader = row[0]
        if not _is_reader(reader):
            continue
        grouped: dict = {}
        skip: set = set()
        for name, declared in companions.items():
            group = _companion_group(reader, res, name, declared)
            if group is None:
                # EVERY half stays fill: the child's span for this field reads
                # as under-covered, never as a payload with an absent channel.
                broken.add((i, 0))
                skip |= {name, *(sib for _kw, sib in declared)}
                continue
            grouped.update(group)
        seg = slice(i * span, (i + 1) * span)
        for name in list(slabs):
            if name in skip:
                continue
            values = grouped[name] if name in grouped else reader.read(res, name)
            if values is not None:
                slabs[name][seg] = values
    return (slabs, *_source_counts(rows, broken))


def _merge_slabs(
    rows: list, fields: dict, *, res_src: int, src_per_child: int, factor: int, n_out: int
) -> tuple:
    """K-way fold of the gen-1 tier, ``factor``-to-one — the ruled merge.

    ``rows`` is the DENSE rank-ordered child range (``None`` for uninhabited
    children, else ``[reader-or-None per window]``). The sources are the
    relayed node-order partials (``res_src == shard_order``) or, for the
    all-time fold on a windowed store, the per-window members at the level's
    own resolution (``factor == 1``, windows folding across). Either way the
    inputs are gen-1 leaf-tier content, and each output cell folds in ONE
    flat k-way call — what makes the merge tree independent of
    ``tuple_width`` (the merge-source law; ``merge_tdigests_kway`` is
    order-independent by its sort, #370).

    Memory bound: exact classes accumulate one dense source vector per
    window (scalars); digest classes stream child by child, holding one
    child's slab plus the open output cells' decoded digests (~``factor``
    digests per cell — the envelope the ruling priced). Missing candidates
    contribute fill and are counted (``source_children.missing``).

    A located field's pair is read together (:func:`_located_pair`) and a
    contributor carrying one half is **skipped for that field and counted
    unreadable** — the same posture as the gather, and as
    ``sweep_overview._fold_node``'s at a leaf missing the sibling. It is not a
    raise: one stale child column must not take a whole stage level down, and
    the fold is a fold — dropping a contributor is under-coverage the artifact
    records, where writing a payload without its words would be corruption
    (spec §9.1). Fields whose reads are sound still fold that contributor: the
    read succeeded, so the loss is known per field, and the per-child
    ``unreadable`` count is what says the artifact folded short.
    """
    from zagg.sweep_overview import (
        _empty_slab,
        combine_dense,
        decode_digest,
        field_companions,
        fold_dense,
        fold_digests,
        overview_fold_delta,
    )

    windows = next((len(row) for row in rows if row is not None), 1)
    n_src = src_per_child * len(rows)
    # The coverage counters are computed at the END, from the same
    # ``(child, window)`` classification the gather uses: a contributor whose
    # located pair this fold refuses (:func:`_located_pair`) is unreadable, not
    # a clean fold, and that is only known once the sources have been read.
    broken: set = set()
    slabs: dict = {}
    for name, meta in fields.items():
        if meta["class"] != "exact":
            continue
        law, fill = meta.get("method"), meta.get("fill_value", "NaN")
        out = None
        for w in range(windows):
            acc = _empty_slab(meta, n_src)
            for i, row in enumerate(rows):
                reader = row[w] if row is not None else None
                if not _is_reader(reader):
                    continue
                values = reader.read(res_src, name)
                if values is not None:
                    acc[i * src_per_child : (i + 1) * src_per_child] = values
            part = fold_dense(acc, factor, law, fill)
            out = part if out is None else combine_dense(out, part, law, fill)
        slabs[name] = out
    for name, meta in fields.items():
        if meta["class"] == "exact":
            continue
        dtype = meta.get("dtype") or "float32"
        inner = tuple(meta.get("inner_shape") or (2,))
        delta = overview_fold_delta(meta)
        # A located field folds its sibling in the SAME k-way call as its
        # payload (ruling 4 on issue #410): the merged words are keyed on the
        # centroid partition that merge produces (spec §9.1), so the two are
        # accumulated together per open cell and folded together. A contributor
        # carrying one half of the pair is SKIPPED loudly and counted
        # unreadable — the gather's posture, and ``_fold_node``'s at a leaf
        # missing the sibling — never folded payload-only, and never a raise
        # that takes the whole level down over one stale child column.
        declared = field_companions(name, meta)
        out = np.full(n_out, b"", dtype=object)
        sibling_slabs = {kwarg: np.full(n_out, b"", dtype=object) for kwarg, _ in declared}
        pending: dict[int, list] = {}
        # {kernel kwarg: {open cell: [word vectors]}} — accumulated in lockstep
        # with ``pending`` so a cell closes with its payload and every channel.
        words: dict[str, dict[int, list]] = {kwarg: {} for kwarg, _ in declared}

        def _close(j, out=out, delta=delta, dtype=dtype, declared=declared):
            cell = pending.pop(j)
            if not declared:
                out[j] = fold_digests(cell, delta=delta, dtype=dtype)
                return
            payload, *encoded = fold_digests(
                cell,
                delta=delta,
                dtype=dtype,
                channels={kwarg: words[kwarg].pop(j) for kwarg, _ in declared},
            )
            out[j] = payload
            for (kwarg, _), value in zip(declared, encoded, strict=True):
                sibling_slabs[kwarg][j] = value

        for i, row in enumerate(rows):
            base = i * src_per_child
            for w, reader in enumerate(row or ()):
                if not _is_reader(reader):
                    continue
                group = _companion_group(reader, res_src, name, declared)
                if group is None:
                    broken.add((i, w))
                    continue
                slab = group[name]
                if slab is None:
                    continue
                for pos, payload in enumerate(slab):
                    if payload is None or not len(payload):
                        continue
                    key = (base + pos) // factor
                    pending.setdefault(key, []).append(decode_digest(payload, dtype, inner))
                    for kwarg, sibling in declared:
                        words[kwarg].setdefault(key, []).append(
                            decode_digest(group[sibling][pos], "uint64", ())
                        )
            # Cells wholly covered by children 0..i are complete: fold and
            # free them, so resident state never exceeds the open boundary.
            done = ((i + 1) * src_per_child) // factor
            for j in [j for j in pending if j < done]:
                _close(j)
        for j in list(pending):
            _close(j)
        slabs[name] = out
        for kwarg, sibling in declared:
            slabs[sibling] = sibling_slabs[kwarg]
    return (slabs, *_source_counts(rows, broken))


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
    all_time: bool,
) -> dict | None:
    """Fold one ``(artifact node, level)`` from the dispatch worker's readers.

    ``readers`` is the dispatch node's ``{child decimal: [reader per
    window]}``; this densifies to the rank range under ``node`` and folds per
    the level's derived regime. Returns the fold dict (slabs + generation +
    provenance) or ``None`` when no child contributed. The all-time fold on
    a windowed store is ALWAYS a merge — its inputs are the per-window gen-1
    members at the level's own resolution (never a merged all-time relay,
    which would breach the merge-source law) — so it records ``stage-merge``
    at gen 2 even where the per-window level is a gather.
    """
    from zagg.sweep_overview import _content_hash
    from zagg.windows import union_time_range

    rows = _dense_rows(readers, node, depth=child_order - k)
    n_out = 4 ** (r - k)
    regime = classify_level(r, shard_order=shard_order)
    if regime == STAGE_GATHER and not all_time:
        slabs, folded, missing, unreadable = _gather_slabs(
            rows, fields, res=r, span=4 ** (r - child_order), n_out=n_out
        )
        merges_from_raw = 1
    else:
        res_src = r if (all_time and regime == STAGE_GATHER) else shard_order
        slabs, folded, missing, unreadable = _merge_slabs(
            rows,
            fields,
            res_src=res_src,
            src_per_child=4 ** (res_src - child_order),
            factor=4 ** (res_src - r) if res_src != r else 1,
            n_out=n_out,
        )
        regime, merges_from_raw = STAGE_MERGE, 2
    if folded == 0:
        return None
    granules, ranges = 0, []
    for row in rows:
        for reader in row or ():
            if _is_reader(reader) and reader.stamp:
                granules += int(reader.stamp.get("granule_count") or 0)
                if reader.stamp.get("time_range") is not None:
                    ranges.append(reader.stamp["time_range"])
    return {
        "slabs": slabs,
        "generation": _summed_generation([row for row in rows if row is not None]),
        "content_hash": _content_hash(node, k, r, fields, slabs),
        "granule_count": granules,
        "time_range": union_time_range(*ranges) if ranges else None,
        "regime": regime,
        "merges_from_raw": merges_from_raw,
        "source_children": {
            "folded": int(folded),
            "missing": int(missing),
            "unreadable": int(unreadable),
        },
    }


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
    root.attrs.update(
        {
            ROLE_ATTR: "overview",
            OVERVIEW_ATTR: {
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
                "generation": fold["generation"],
                "content_hash": fold["content_hash"],
                "run_id": run_id,
                "generated_at": _utcnow(),
            },
        }
    )
    stamp_window = key if windowed else None
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(fold["granule_count"]),
        window=stamp_window,
        time_range=fold["time_range"] if stamp_window is not None else None,
        run_id=run_id,
    )
    try:  # D20 sidecar: fail-open telemetry, §5 O11 record (the /1 writer's posture)
        from zagg.content_hash import content_hashes_record, hash_arrays
        from zagg.telemetry import SPEC_V3, build_record, write_sidecar

        staged = {f"{r}/morton": words}
        staged.update({f"{r}/{name}": slab for name, slab in fold["slabs"].items()})
        group = zarr.open_group(store, path="", mode="r", zarr_format=3)
        record = build_record(
            shard_key=morton_word(node),
            metadata={
                "cells_with_data": int(populated.sum()),
                "granule_count": int(fold["granule_count"]),
                "content_hashes": content_hashes_record(hash_arrays(group, staged=staged)),
            },
            window=stamp_window,
        )
        write_sidecar(path, record, spec=SPEC_V3, **store_kwargs)
    except Exception as e:
        logger.warning(f"stage sweep: O11 sidecar failed at {node}/{basename} ({e})")
    return basename


def write_stage_column(
    store_root,
    node,
    folded: dict,
    fields: dict,
    *,
    node_order: int,
    shard_order: int,
    cell_order: int,
    generation: dict,
    source_children: dict,
    window: str | None = None,
    time_range=None,
    granule_count: int = 0,
    run_id: str | None = None,
    store_kwargs: dict | None = None,
) -> str:
    """Write one stage column at its dispatch node; returns the basename.

    The §4.6 column artifact shape (``zagg-column/1``) with the stage
    regime: every group is a PURE GATHER of the child columns' members at
    the same resolution — the relay (``shard_order``: the subtree's leaf
    node-order partials, the ruled merge-source tier) plus the gatherable
    members coarser tuples need — so ``merges_from_raw`` stays 1 for every
    group and the artifact carries gen-1 content only. The relay member is
    the one group a parent merge may assume; a ``folded`` without it is
    refused by name. Attrs additionally record the summed ``generation``
    (the parent's skip-gate basis), ``source_children`` (a gather that
    under-covered says so in the artifact), and the run id; the commit stamp
    carries ``run_id`` too (lease backstop). D4 order throughout; the D20
    sidecar lands after the stamp, fail-open. Single-writer law: the column
    lives under its own node prefix, one writer per lease-serialized run.
    """
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
    from zagg.grids.base import vlen_dtype_warning_suppressed
    from zagg.grids.healpix import HealpixGrid
    from zagg.grids.morton import morton_word
    from zagg.hive import _utcnow, stamp_commit
    from zagg.store import open_store
    from zagg.sweep_overview import ROLE_ATTR, _overview_config, _populated_mask
    from zagg.windows import SCHEDULE_NONE_TOKEN

    store_kwargs = dict(store_kwargs or {})
    node_order = int(node_order)
    fields = composable_fields(fields)
    resolutions = sorted((int(r) for r in folded), reverse=True)
    if int(shard_order) not in resolutions:
        raise ValueError(
            f"a stage column must carry the relay member ({shard_order} — the subtree's "
            f"leaf node-order partials, the ruled merge-source tier); got {resolutions}"
        )
    node_prefix = f"{store_root}/{_node_rel(node)}"
    basename = column_name(window)
    path = f"{node_prefix}/{basename}"
    cfg = _overview_config(fields)
    grids = {res: HealpixGrid(node_order, res, config=cfg, sharded=True) for res in resolutions}
    spec = GroupSpec(
        members={str(res): grids[res].shard_spec() for res in resolutions}, attributes={}
    )
    store = open_store(path, **store_kwargs)
    staged: dict = {}
    with zarr_config.set({"async.concurrency": 128}), vlen_dtype_warning_suppressed():
        sync(store.delete_dir(""))
        _delete_sidecar(node_prefix, _sidecar_name(basename), store_kwargs)
        spec.to_zarr(store, "", overwrite=True)
        for res in resolutions:
            words = np.asarray(generate_morton_children(morton_word(node), res), dtype=np.uint64)
            arr = open_array(store, path=f"{res}/morton", zarr_format=3, consolidated=False)
            arr[:] = words
            staged[f"{res}/morton"] = words
            for name, slab in folded[res].items():
                arr = open_array(store, path=f"{res}/{name}", zarr_format=3, consolidated=False)
                arr[:] = slab
                staged[f"{res}/{name}"] = slab
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
                "source_children": dict(source_children),
                "cells_with_data_order": resolutions[0],
                "run_id": run_id,
                "generated_at": _utcnow(),
            },
        }
    )
    populated = _populated_mask(folded[resolutions[0]], fields)
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(granule_count),
        window=window,
        time_range=time_range if window is not None else None,
        run_id=run_id,
    )
    _write_sidecar(
        store,
        path,
        morton_word(node),
        staged,
        int(populated.sum()),
        granule_count,
        window,
        store_kwargs,
    )
    return basename


def _node_rel(decimal: str) -> str:
    from zagg.sweep import _node_rel as rel

    return rel(decimal)


def _artifact_stamp(store_root, node, basename, run_id, run_started, store_kwargs) -> dict | None:
    """A stage artifact's commit stamp (``None`` when absent), foreign-gated.

    The ruled backstop lives here: a skip-if-current read that encounters a
    FOREIGN stamp written since this run started aborts loudly rather than
    trusting or overwriting a live sibling sweep's output.
    """
    from zagg.hive import read_commit
    from zagg.store import open_store

    if not basename:
        return None
    try:
        stamp = read_commit(
            open_store(f"{store_root}/{_node_rel(node)}/{basename}", **store_kwargs)
        )
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


def stage_node(
    store,
    store_root,
    node,
    stage,
    levels,
    fields,
    *,
    key,
    fold_windows,
    all_time,
    windowed,
    shard_order,
    cell_order,
    candidates,
    run_id,
    run_started,
    counts,
    store_kwargs,
    level_actuals=None,
) -> None:
    """One stage worker: fold a dispatch node's tuple for one window item.

    Reads the node's candidate child columns once (stamp-validated), then per
    tuple order materializes every dirty artifact node beneath the dispatch
    node — skip-if-current keyed on SUMMED CHILD GENERATIONS (the ratchet:
    a healed or appended child moves the sum and forces the rewrite; a
    content hash cannot be had without folding, so the #417/#433
    count/timestamp/run-id/granule key is the gate) — and finally its own
    stage column (relay + gatherables), unless this is the root tuple (no
    parent consumes it) or the all-time item (per-window columns carry the
    gen-1 tier; an all-time column would relay merged content, breaching the
    merge-source law).
    """
    from zagg.column import generation_key
    from zagg.sweep_overview import ENVELOPE_NAME, _read_envelope
    from zagg.windows import union_time_range

    dispatch, child_order = int(stage["dispatch"]), int(stage["child_order"])
    level_by_order = {int(e["node"]): int(e["cells"][0]) for e in levels}
    orders = [k for k in stage["orders"] if k in level_by_order]
    children = sorted({_node_at(d, child_order) for d in candidates if d.startswith(node)})
    readers = _readers_for(
        store_root,
        children,
        fold_windows,
        run_id=run_id,
        run_started=run_started,
        store_kwargs=store_kwargs,
        counts=counts,
    )
    dispatch_level_current = False
    for k in orders:
        r = level_by_order[k]
        regime_plan = (
            STAGE_MERGE
            if (all_time or classify_level(r, shard_order=shard_order) == STAGE_MERGE)
            else STAGE_GATHER
        )
        for target in sorted({_node_at(d, k) for d in candidates if d.startswith(node)}):
            rows = [row for c, row in readers.items() if c.startswith(target)]
            fresh_gen = _summed_generation(rows)
            envelope = _read_envelope(store, target)
            entries = dict((envelope or {}).get("windows") or {})
            entry = entries.get(key)
            if (
                isinstance(entry, dict)
                and generation_key(entry.get("generation")) == generation_key(fresh_gen)
                and entry.get("regime") == regime_plan
                and _artifact_stamp(
                    store_root, target, entry.get("object"), run_id, run_started, store_kwargs
                )
                is not None
            ):
                counts["current"] += 1
                if not all_time:
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
            fold = _stage_fold(
                target,
                k,
                r,
                readers,
                fields,
                shard_order=shard_order,
                child_order=child_order,
                all_time=all_time,
            )
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
            if not all_time:
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
            if fresh != envelope:
                import obstore

                obstore.put(
                    store,
                    f"{_node_rel(target)}/{ENVELOPE_NAME}",
                    json.dumps(fresh, indent=1).encode(),
                )
    counts["revalidated"] += sum(
        r.revalidated for row in readers.values() for r in row if _is_reader(r)
    )
    if dispatch == 0 or (windowed and all_time):
        return
    members = column_members(levels, dispatch, shard_order=shard_order)
    fresh_gen = _summed_generation(list(readers.values()))
    if dispatch_level_current and _stage_column_current(
        store_root, node, fold_windows[0], fresh_gen, run_id, run_started, store_kwargs
    ):
        counts["columns_current"] += 1
        return
    folded: dict = {}
    relay_sc = None
    for res in members:
        slabs, folded_n, missing, unreadable = _gather_slabs(
            _dense_rows(readers, node, depth=child_order - dispatch),
            fields,
            res=res,
            span=4 ** (res - child_order),
            n_out=4 ** (res - dispatch),
        )
        folded[res] = slabs
        if res == shard_order:
            relay_sc = {
                "folded": int(folded_n),
                "missing": int(missing),
                "unreadable": int(unreadable),
            }
    if relay_sc is None or relay_sc["folded"] == 0:
        return
    granules, ranges = 0, []
    for row in readers.values():
        for reader in row:
            if _is_reader(reader) and reader.stamp:
                granules += int(reader.stamp.get("granule_count") or 0)
                if reader.stamp.get("time_range") is not None:
                    ranges.append(reader.stamp["time_range"])
    try:
        write_stage_column(
            store_root,
            node,
            folded,
            fields,
            node_order=dispatch,
            shard_order=shard_order,
            cell_order=cell_order,
            generation=fresh_gen,
            source_children=relay_sc,
            window=fold_windows[0],
            time_range=union_time_range(*ranges) if ranges else None,
            granule_count=granules,
            run_id=run_id,
            store_kwargs=store_kwargs,
        )
        counts["columns_written"] += 1
        if relay_sc["missing"]:
            counts["under_covered"] += 1
    except Exception as e:
        logger.warning(f"stage sweep: column write failed at node {node} window {key!r} ({e})")
        counts["failed"] += 1


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
