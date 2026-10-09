"""The families rollup walk, folded into the staged cascade (issue #610 phase 4).

The JSON-rollup families (``stats``, ``moc``, ``submap``) used to need their
own end-of-run fan-out: :func:`zagg.sweep.run_sweep` partitioned by leaf
count, barriered, and ran a finisher, all beside the ladder's own fan-out
over the same tree (:func:`zagg.sweep_stages.sweep_stage_pass`). espg's
phase-4 ruling on issue #610 is that this is one walk, not two — "a
``(node, window)`` stage unit already reads its children's columns; the
``stats``/``moc``/``submap`` rollups and the per-node TOC contribution are
the same walk. One fan-out, one cascade, nothing else"
(https://github.com/englacial/zagg/issues/610#issuecomment-6053115106).

So the families ride the ladder's units:

- :func:`fold_span` is THE families walk — the engine
  :func:`zagg.sweep._sweep_family` folds the whole tree through and the one
  this module folds a tuple's span through. There is no second walk, the way
  issue #620 left exactly one overview cascade
  (:func:`zagg.sweep_fold.cascade_fold`).
- :class:`FamiliesRider` is one pass's state: the family instances, the
  per-family counts, and the ``moc`` family's §10 temporal accumulator. One
  rider per invoke, so an invoke holds exactly one of each family and its
  accumulator covers every leaf the invoke read.
- :meth:`FamiliesRider.fold_node` folds ONE dispatch node's tuple span. The
  finest tuple's span starts at the leaves (the stats sidecar, the D4 commit
  stamp, the leaf sub-map and the §10 ``temporal.toc`` record — read through
  the invoke's one store handle, issue #610 phase 1); every coarser tuple's
  span starts from the rollups the tuple below it wrote. Dispatch nodes at
  one order own disjoint subtrees, so the split across invokes is free of
  cross-worker dependencies — the same disjointness the ladder relies on.

**Where the walk runs.** :meth:`FamiliesRider.fold_node` is called once per
dispatch node, in the node's CLOSE — the once-per-node bucket of
:func:`zagg.sweep_units.run_tuple` the Icechunk ref hook already rides, and
the only seam at which a node has a single writer. A per-node rollup object
merges every window of the node (its ``windows`` key), so a ``(node,
window)`` unit cannot own it: the node's window units run concurrently and
would read-modify-write each other's object.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: The families that ride the cascade. ``overview`` is deliberately absent —
#: it IS the cascade on a ``/2`` store (:mod:`zagg.sweep_stage`), and its
#: ``/1`` form refuses a ``/2`` manifest before any leaf read. ``columns``
#: and ``debris`` are not rollup families either (issue #520, D22).
CASCADE_FAMILIES = ("stats", "moc", "submap")


def fold_span(
    store, fam, by_shard, *, shard_order, spec, counts, from_order, to_order=0, from_leaves=False
) -> list:
    """Fold one JSON-rollup family up the dirty ancestor paths over ONE span of orders.

    THE families walk, and the only one (issue #610 phase 4): the whole-tree
    pass (:func:`zagg.sweep._sweep_family`), its partition and finisher
    forms, and this module's per-tuple rider all fold through this function,
    so a store's rollups do not depend on which executor produced them.

    ``from_order`` is the order the walk starts at and ``from_leaves`` is what
    it starts FROM — the two are separate because ``from_order ==
    shard_order`` means opposite things to the two callers, and conflating
    them sent the phase-3 finisher back to the leaf walk whenever its
    partitions split at the shard order (review finding). ``from_leaves``
    folds each shard's window leaves through
    :func:`zagg.sweep._rollup_shard_node` — what the cascade's finest tuple
    and an unpartitioned pass do; otherwise the walk starts from the rollups
    ALREADY STORED at ``from_order``, so no leaf is read and a rollup missing
    there contributes nothing, exactly as an emptied child does — what a
    coarser tuple and the finisher do. ``to_order`` is the coarsest order
    written: 0 is the base nodes, and anything above it belongs to another
    span's writer (a partition's finisher, the next dispatch tuple). Returns
    the rollup envelopes of the coarsest frontier it reached, which is what
    :meth:`zagg.sweep.SweepFamily.finish` composes the store-root objects
    from.

    The span decomposition is exact rather than approximate: a node's fold
    reads its four children from ``computed`` or, failing that, off the store
    (:func:`zagg.sweep._rollup_interior`), and a span's start order reads
    exactly what the span below it just wrote — so one walk over
    ``[0, shard_order]`` and a chain of adjacent spans covering it write
    byte-identical rollups.
    """
    from zagg.hive import _decimal_base
    from zagg.sweep import _ancestor, _read_rollup, _rollup_interior, _rollup_shard_node

    start, shard_order, to_order = int(from_order), int(shard_order), int(to_order)
    if from_leaves and start != shard_order:
        raise ValueError(
            f"fold_span was asked to read the leaves at order {start}, but the leaves of this "
            f"store are at {shard_order} — a span starting anywhere else reads rollups"
        )
    computed: dict[str, dict | None] = {}
    if from_leaves:
        for decimal in sorted(by_shard):
            computed[decimal] = _rollup_shard_node(
                store, fam, decimal, by_shard[decimal], shard_order, spec, counts
            )
        top = shard_order - 1
    else:
        for node in sorted({d[: len(_decimal_base(d)) + start] for d in by_shard}):
            computed[node] = _read_rollup(store, fam, node)
            if computed[node] is None:
                counts["empty"] += 1
        top = start - 1
    frontier = [d for d in sorted(computed) if computed[d] is not None]
    for _order in range(top, to_order - 1, -1):
        parents = sorted({a for d in frontier if (a := _ancestor(d)) is not None})
        frontier = []
        for node in parents:
            computed[node] = _rollup_interior(store, fam, node, computed, counts)
            if computed[node] is not None:
                frontier.append(node)
        if not frontier:
            break
    return [computed[d] for d in frontier]


def normalize_families(families) -> tuple:
    """The family names a cascade pass may ride, refused BY NAME if not.

    ``None`` is the default set, ``()`` is "no families" (the pre-phase-4
    ladder). A name outside :data:`CASCADE_FAMILIES` refuses rather than
    silently dropping: ``overview`` would mean a second cascade, and a
    typo'd name must not read as a clean no-op.
    """
    if families is None:
        return tuple(CASCADE_FAMILIES)
    names = tuple(str(n) for n in families)
    unknown = [n for n in names if n not in CASCADE_FAMILIES]
    if unknown:
        raise ValueError(
            f"families {unknown} cannot ride the staged cascade; the rollup families are "
            f"{list(CASCADE_FAMILIES)} (the overview family IS the cascade, issue #620)"
        )
    return names


def rider_for(families):
    """A :class:`FamiliesRider` for the named families, or ``None`` when none ride.

    ``()`` — the ladder-only default of every staged entry point until the
    run tail stops firing the separate families fan-out — is ``None``;
    ``None`` is :data:`CASCADE_FAMILIES`, the same spelling
    :func:`zagg.sweep.run_sweep` gives it.
    """
    names = normalize_families(families)
    return FamiliesRider(names) if names else None


class FamiliesRider:
    """One pass's families state: the family instances, counts and accumulators.

    Constructed once per invoke (one per in-process driver, one per fleet
    stage worker) and handed to :func:`zagg.sweep_stages.sweep_stage_pass`
    the way ``level_actuals`` is — so a driver that runs several passes (the
    ``partitions=`` loop) accumulates across them and the §10 accumulator
    covers every leaf the invoke read.
    """

    def __init__(self, names=None):
        from zagg.sweep import get_family

        self.names = normalize_families(names)
        self.families = [get_family(n) for n in self.names]
        self.counts = {n: {"written": 0, "current": 0, "empty": 0, "failed": 0} for n in self.names}
        #: Dispatch nodes whose span raised; their rollups are left as they
        #: stood and the next pass heals them (D9 — a rollup is a cache).
        self.node_failures: list = []
        self.store = None
        self.shard_order = 0
        self.spec = None
        self.by_shard: dict = {}
        #: Every shard whose LEAVES this invoke actually read, across the
        #: passes bound to it — what the accumulator's ``visited`` names. Not
        #: the work set it was handed: a fleet invoke's scope is its own
        #: dispatch nodes, and an event that overflowed into ``discover: true``
        #: derives the whole store's work set worker-side, so ``by_shard``
        #: would claim coverage no unit of this invoke produced and the
        #: finisher's ``shards_unvisited`` check — the signal that a close
        #: invoke was lost — would never fire. Nor the work set of every span:
        #: only the span at ``shard_order`` reads leaves, and a coarser span
        #: folds from rollups that account for this run's leaves ONLY if that
        #: span already ran — so claiming its work set would say "these
        #: shards are accounted for" on the strength of whatever stood on the
        #: store, which at ``tuple_width`` 1 or 2 is every shard (review
        #: finding).
        self.visited: set = set()
        #: The ``(node, dispatch, child_order)`` spans already folded. The
        #: rollup fold is idempotent, so a repeat would write nothing — but
        #: the §10 accumulation is ADDITIVE (``MocFamily._accumulate_temporal``
        #: appends one entry per leaf read), so a node folded twice publishes
        #: doubled counts. ``run_stage_sweep(partitions=N)`` does exactly
        #: that: it narrows ``scope`` per partition and ``scope_admits``
        #: resolves containment in BOTH directions, so a coarse dispatch node
        #: is admitted by every partition beneath it (review finding).
        self.folded: set = set()

    def bind(self, store, manifest: dict, by_shard: dict) -> None:
        """Adopt one pass's store handle, manifest and work set.

        The handle is the PASS's (issue #610 phase 1): a rider reads every
        leaf and rollup by relative key through it, so an invoke's store
        constructions stay O(1) in the leaf count. A driver that runs several
        passes (``partitions=``) binds once per pass and the counts and the
        §10 accumulator accrue across them.
        """
        self.store = store
        self.shard_order = int(manifest["shard_order"])
        self.spec = manifest.get("spec")
        self.by_shard = by_shard

    def fold_node(self, node: str, *, dispatch: int, child_order: int) -> None:
        """Fold every family over this dispatch node's tuple span.

        The work set is the pass's dirty leaves UNDER ``node`` — the same set
        the whole-tree walk would visit, restricted to this node's subtree;
        untouched siblings keep contributing through their stored rollups
        (:func:`zagg.sweep._rollup_interior`). Fail-open per node and per
        family (D9): a span that raises is warned and recorded, and neither
        the ladder's fold nor the other dispatch nodes are affected.
        """
        if self.store is None:
            raise ValueError("the families rider was never bound to a pass (FamiliesRider.bind)")
        span = (node, int(dispatch), int(child_order))
        if span in self.folded:
            return  # see ``folded``: a repeat would double the §10 counts
        work = {d: w for d, w in self.by_shard.items() if d.startswith(node)}
        if not work:
            return
        self.folded.add(span)
        clean = True
        for fam in self.families:
            try:
                fold_span(
                    self.store,
                    fam,
                    work,
                    shard_order=self.shard_order,
                    spec=self.spec,
                    counts=self.counts[fam.name],
                    from_order=int(child_order),
                    to_order=int(dispatch),
                    # The finest tuple's children ARE the leaves; every
                    # coarser tuple starts from the rollups below it.
                    from_leaves=int(child_order) == self.shard_order,
                )
            except Exception as e:
                logger.warning(
                    f"stage sweep[{fam.name}]: rollup span [{dispatch}, {child_order}) at node "
                    f"{node} failed ({e}) — leaving its rollups as they stood (D9)"
                )
                self.node_failures.append({"node": node, "family": fam.name, "error": str(e)})
                clean = False
        if clean and int(child_order) == self.shard_order:
            # Claimed only when EVERY family folded the span AND the span is
            # the one that reads the leaves. A span that raised left its
            # rollups as they stood; a coarser span never looked at a leaf.
            # Either way they do not account for these shards, and the
            # finisher must not stand the work-set root refresh down on them
            # (review findings).
            self.visited.update(work)

    def summary(self) -> dict:
        """The pass's per-family block: counts, summaries, accumulators, failures.

        The ``accumulator`` is the same §10 block a families PARTITION record
        carries (:meth:`zagg.sweep.SweepFamily.accumulator`, issue #610 phase
        3), so the ladder's finisher composes the root section from the stage
        records exactly as the families finisher composed it from the
        partition records. ``visited`` is the shards whose leaves this invoke
        actually READ (see the attribute), which is what lets the finisher
        tell a work set short of a close invoke from one it covered.
        """
        out: dict = {}
        for fam in self.families:
            block = dict(self.counts[fam.name])
            block.update(fam.summary())
            acc = fam.accumulator()
            if acc is not None:
                acc["visited"] = sorted(self.visited)
                block["accumulator"] = acc
            # Per FAMILY, not a sibling key of the family names: this block
            # becomes the stage record's ``families`` verbatim, which the
            # spec describes as a mapping of family name to that family's
            # per-pass counts (§4.7) — a reader walking ``families.items()``
            # has no way to know one entry is not a family (review finding).
            fails = [f for f in self.node_failures if f["family"] == fam.name]
            if fails:
                block["node_failures"] = fails
            out[fam.name] = block
        return out


def accumulator_blocks(records, name: str) -> list:
    """One family's accumulator blocks, from the records that RODE that family.

    ``records`` is :func:`zagg.sweep_stages.read_stage_records`' list. Only
    the records whose ``families`` names this family contribute: a record that
    rode it and carries no accumulator contributes ``None`` — which
    :meth:`zagg.sweep.SweepFamily.load_accumulators` refuses, so a short
    fan-out degrades loudly in :func:`finish_families` rather than publishing
    a section composed from a subset without saying so.

    A record that rode NO family is skipped rather than contributing ``None``,
    because it is not evidence of a short fan-out. A windowed run makes that
    the normal case: the fleet dispatcher sends ``families`` only on the
    invokes that CLOSE a node (``closing_block`` in
    :mod:`zagg.sweep_fleet`), while every window unit still writes a stage
    record that :func:`zagg.sweep_stages.read_stage_records` returns — so
    counting those as missing accumulators lost the whole §10 section of the
    one windowed shape this phase serves (review finding). The distinction is
    the one :func:`families_in_records` already draws.
    """
    out = []
    for r in records:
        block = r.get("families") or {}
        if isinstance(block, dict) and name in block:
            # ``(block[name] or {}).get`` and not a guarded read: a record
            # whose per-family value is not a mapping must raise here, inside
            # :func:`finish_families`' ``try``, and degrade like a lost one.
            out.append((block[name] or {}).get("accumulator"))
    return out


def families_in_records(records) -> tuple:
    """Which rollup families the run's units actually rode, from their records.

    Taken from the RECORDS rather than from the finisher's own event, the way
    the ladder dispatcher takes ``closes`` from a landed window record
    (:func:`zagg.sweep_units.manifest_closes`): the workers are what decided,
    and a finisher composing a family nobody folded would publish from
    nothing. Kept in :data:`CASCADE_FAMILIES` order and filtered to it, so an
    unknown key in a record cannot steer the finisher.
    """
    seen = {n for r in records for n in (r.get("families") or {})}
    return tuple(n for n in CASCADE_FAMILIES if n in seen)


def finish_families(
    store_root: str, manifest: dict, by_shard: dict, *, rider=None, records=None, store_kwargs=None
) -> dict | None:
    """Compose the families' store-root singletons ONCE, in the ladder's finisher.

    The §10 temporal section inside ``coverage.moc`` and its ``coverage.toc``
    sibling (§10.5) are store-root singletons: plural writers would breach
    the single-writer law, exactly as the manifest RMW would, so they belong
    here and not in a unit (:func:`zagg.sweep_stages.run_finisher`'s own
    ruling). Each family's :meth:`zagg.sweep.SweepFamily.finish` is fed the
    BASE-NODE rollups the root tuple left on the store — at most one GET per
    base cell — so this reads no leaf, exactly as the families finisher of
    phase 3 does.

    ``rider`` is the in-process driver's :class:`FamiliesRider`, whose ``moc``
    family already holds the run's §10 accumulation. ``records`` is the
    fleet's path: the run's stage records, whose accumulator blocks
    (:func:`accumulator_blocks`) are loaded into fresh families — the same
    blocks, the same grammar and the same composition as the partition
    records of phase 3. Returns ``None`` when neither names a family.

    Degradation is the staged finisher's, not the families finisher's. A
    block that is missing or unusable loses that family's SECTION (one
    warning, ``accumulator_error``) and keeps its spatial fold; a work set
    holding shards no record visited is recorded (``shards_unvisited``)
    rather than refused, because the root object composes at a GET-union-PUT
    seam where a partial producer under-reports and the next pass heals it
    (§10.4) — which is the whole reason the ladder's own barrier is allowed
    to be soft. There is no leaf walk to fall back to here: the units read
    the leaves, and re-reading them all in one invoke is the 900 s wall this
    issue is about.

    A WRITE that fails still raises, like :func:`zagg.sweep_stages.run_finisher`'s
    own steps 1-2: the caller records the incomplete finish and leaves the
    lease held, so the run expires into claimability rather than reporting a
    clean finish over a half-written root.
    """
    from zagg.hive import _decimal_base
    from zagg.store import open_object_store
    from zagg.sweep import _read_rollup, get_family

    store_kwargs = dict(store_kwargs or {})
    shard_order = int(manifest["shard_order"])
    if rider is not None:
        names, families, source = rider.names, list(rider.families), "pass"
        visited = set(rider.visited)
    else:
        records = list(records or ())
        names, source = families_in_records(records), "records"
        families = [get_family(n) for n in names]
        visited = set()
    if not names:
        return None
    out: dict = {
        "source": source,
        "families": {},
        "root_moc_written": False,
        # Whether the ``moc`` family's own finish took over the root
        # ``coverage.moc`` refresh — see where it is set.
        "owns_root_moc": False,
    }
    if source == "records":
        out["stage_records"] = len(records)
    store = open_object_store(store_root, **store_kwargs)
    bases = sorted({_decimal_base(d) for d in by_shard})
    told_visited = rider is not None
    moc_composed = moc_section_lost = False
    for fam in families:
        block: dict = {}
        # A family declaring no accumulator needs none loaded: its coarse
        # levels fold from the rollups the units already wrote, which is the
        # whole reason only ``moc`` carries one (issue #610 phase 3).
        if source == "records" and fam.accumulator() is not None:
            try:
                # The EXTRACTION is inside the try too: a record whose
                # ``families`` is not a mapping of mappings raises here, and a
                # malformed record has to degrade like a lost one (the shape
                # :func:`zagg.sweep._load_finisher` wraps for the same reason).
                blocks = accumulator_blocks(records, fam.name)
                taken: set = set()
                kept: list = []
                repeats = 0
                for b in blocks:
                    if not isinstance(b, dict):
                        kept.append(b)  # rode the family, carries nothing: loud
                        continue
                    # ``told_visited`` keys on the KEY, not on the block: a
                    # worker predating it says nothing about what it walked,
                    # and silence is not a claim of full coverage.
                    if "visited" in b:
                        visited.update(b["visited"] or ())
                        told_visited = True
                    shards = b.get("shards")
                    if not isinstance(shards, dict):
                        kept.append(b)  # malformed: ``load_accumulators`` refuses it
                        continue
                    # A shard's share is taken ONCE. The §10 fold is additive
                    # per shard (``load_accumulators`` EXTENDS the shard's
                    # parts), so feeding one twice doubles its published
                    # observation counts while every number this finisher
                    # reports stays put — silently wrong temporal coverage,
                    # the defect class issue #610 was filed about. It is
                    # reachable without a new API: a run re-driven under the
                    # same id at a different batch numbering leaves the dead
                    # attempt's records in place (``read_stage_records``' own
                    # docstring), and the fleet only SNAPSHOTS them so the
                    # barriers ignore them — it never removes them, and the
                    # ``run_id`` filter still matches. The drop keys on the
                    # SHARD rather than on the block's ``visited`` claim, so a
                    # re-split whose records only PARTLY overlap still
                    # contributes the shards it alone read (review finding).
                    if again := taken & set(shards):
                        repeats += len(again)
                        b = {**b, "shards": {k: v for k, v in shards.items() if k not in taken}}
                    taken |= set(shards)
                    kept.append(b)
                if repeats:
                    logger.warning(
                        f"stage sweep[{fam.name}]: {repeats} shard(s) are claimed by more than "
                        "one of the run's stage records — a re-drive under this run id leaves "
                        "the dead attempt's records in place; each share is folded ONCE, so "
                        "the §10 counts are this run's (issue #610 phase 4)"
                    )
                    block["duplicate_shards"] = repeats
                fam.load_accumulators(kept)
            except Exception as e:
                logger.warning(
                    f"stage sweep[{fam.name}]: the run's stage records carry no usable "
                    f"accumulator ({e}) — composing its spatial fold alone, with no §10 "
                    f"section; the next pass heals it (issue #610 phase 4)"
                )
                # A half-loaded family would fold its own partial state into
                # the section; start it over, with nothing accumulated.
                fam = get_family(fam.name)
                block["accumulator_error"] = str(e)
        tops = [r for b in bases if (r := _read_rollup(store, fam, b)) is not None]
        result = fam.finish(store_root, tops, shard_order, store_kwargs)
        out["families"][fam.name] = {**block, "base_rollups": len(tops), **result}
        if result.get("root_moc_written"):
            out["root_moc_written"] = True
        if fam.name == "moc":
            moc_composed = bool(tops)
            moc_section_lost = "accumulator_error" in block
    unvisited = sorted(set(by_shard) - visited) if told_visited else []
    if unvisited:
        logger.warning(
            f"stage sweep: {len(unvisited)} shard(s) of the work set are in no unit's "
            f"families record — the root section under-reports them and the next pass "
            f"heals it (§10.4, issue #610 phase 4)"
        )
        out["shards_unvisited"] = len(unvisited)
    # What the caller stands :func:`zagg.sweep_stages.run_finisher`'s own root
    # refresh down on. Not "a family rode": only the ``moc`` family writes
    # that object, and its words are this run's only when all three hold —
    #   * ``moc`` had base-node rollups to compose from (with none,
    #     ``MocFamily.finish`` returns early and writes nothing);
    #   * every shard of the work set is in some record's ``visited``, so the
    #     rollups it composed actually account for this run's leaves (a lost
    #     close invoke, a scoped fan-out, or a rider whose spans all raised
    #     leaves them out, and then the work-set envelope is the only thing
    #     that would list them);
    #   * some record SAID what it visited at all — an older worker's did
    #     not, and an unverifiable claim is not one to stand down on;
    #   * and the §10 section survived: an ``accumulator_error`` means this
    #     finish published the standing section and none of this run's, so it
    #     is not the authority to stand step 1 down on either (review
    #     finding).
    # Otherwise step 1 runs too: one extra PUT of a subset, which
    # ``write_root_coverage`` unions (review findings).
    out["owns_root_moc"] = bool(
        moc_composed and told_visited and not unvisited and not moc_section_lost
    )
    return out
