"""``2^n`` morton-subtree partitions for the parallel sweep (issue #377).

The unified sweep (:mod:`zagg.sweep`) is ONE walk over a hive store's digit
tree — one ``mode="sweep"`` worker invoke, whose wall clock and resident
memory both scale with the whole store. This module decomposes that walk into
``2^n`` **isolated** units so a fan-out of workers runs it in parallel, each
unit sized ``ceil(leaves / 2^n)``.

**Digit boundaries only.** A D1 decimal morton id is ``{sign+base}{digit}*``
with one base-4 digit — 2 bits — per order, so a ``2^n`` split lands on a
digit boundary exactly when ``n`` is even: ``n = 2k`` splits at **order k**,
and the partition index is the base-4 value of the id's first ``k`` digits.
``partitions`` is therefore a power of FOUR. An odd ``n`` (2, 8, 32, …
partitions) would halve a digit — two partitions sharing one order-k node's
four children — and is **rejected**, loudly and by name, rather than silently
rounded to a neighbouring power of four (:func:`partition_split_order`); the
bit-level alternative is the open design fork on issue #377.

**Disjointness.** Partition *i* owns every id whose first ``k`` digits rank
*i* — one order-k subtree per HEALPix base cell, twelve in all
(``12 * 4^k / 2^n == 12``). Every node at order ``>= k``, and every leaf and
window beneath it, therefore lies wholly inside ONE partition: no two
partitions can write the same ``(node, window)`` artifact, so the workers need
no coordination and the existing D22 generation stamps keep each partition
independently idempotent and resumable.

**What a partition deliberately does not do.** Nodes at orders COARSER than
``k`` span partitions, as does the store-root ``coverage.moc`` refresh
(:meth:`zagg.sweep.MocFamily.finish`). Those belong to the **coarse-level
finisher** — one invoke after the partitions land, folding from the
partitions' already-materialized overview slabs (issue #377, sequenced behind
the issue #376 cascade). A partitioned pass stops its walk at order ``k`` and
defers the finish hook; a partition worker never writes above the split.

Windowed stores expose a second, free parallelism dimension — the window axis
is already independent per ``(node, window)`` artifact, so per-(partition,
window) invokes decompose further. Noted, not implemented (issue #377 v1).
"""

from __future__ import annotations

#: Leaves one partition of the run tail's families pass is sized to hold
#: (issue #610). The v3 California pass folded about a leaf a second per
#: invoke — the rollup PUTs dominate (~2.5 objects a leaf) — so 128 leaves is
#: minutes against the 900 s wall, with room for a queued start.
FAMILIES_TARGET_LEAVES = 128


def families_partitions(leaves, target: int = FAMILIES_TARGET_LEAVES) -> int:
    """The ``4^k`` the run tail splits a families pass into (issue #610).

    Sized from the LEAVES, not a fixed width: ``k`` starts at
    ``ceil(log4(ceil(n / target)))`` — the width that holds ``target`` leaves
    per partition if the keys spread evenly — and is refined upward while the
    largest partition :func:`partition_leaves` actually produces is over
    ``target``, up to the leaves' own order (one shard per partition, the
    finest split :func:`zagg.sweep.run_sweep` admits). Regional stores
    cluster: the 2,959-leaf California store's 64-way split held 1,050 leaves
    in one partition, past the wall at the observed rate, which is why the
    count alone cannot size it (espg, issue #610). ``1`` for a work set of at
    most ``target`` leaves — the single pass the tail fired before.
    """
    from zagg.grids.morton import morton_decimal
    from zagg.hive import _decimal_order

    refs = [tuple(r) if isinstance(r, (tuple, list)) else (r, None) for r in leaves]
    finest = min((_decimal_order(morton_decimal(int(key))) for key, _w in refs), default=0)
    k = 0
    while k < finest and (
        4**k * int(target) < len(refs)
        or max(len(b) for b in partition_leaves(refs, 4**k).values()) > int(target)
    ):
        k += 1
    return 4**k


def partition_split_order(partitions: int) -> int:
    """Morton order a ``partitions``-way split lands on (``2^(2k)`` -> ``k``).

    ``partitions = 1`` returns 0 — the identity partition, one unit owning the
    whole tree, byte-identical to an unpartitioned sweep. Raises ``ValueError``
    on a non-power-of-two, and on an ODD power of two (which would split a
    2-bit morton digit in half); the message names the two valid neighbours so
    the caller fixes it in one pass.
    """
    n = int(partitions)
    if n < 1 or n & (n - 1):
        raise ValueError(f"sweep partitions must be a power of two >= 1 (got {partitions!r})")
    bits = n.bit_length() - 1
    if bits % 2:
        raise ValueError(
            f"sweep partitions={n} (2^{bits}) splits a morton digit in half; a digit is 2 "
            f"bits, so partitions round to digit boundaries — use {n // 2} or {n * 2} "
            f"(bit-level splits are the open fork on issue #377)"
        )
    return bits // 2


def partition_index(decimal: str, partitions: int) -> int:
    """Index of the partition owning the D1 morton id ``decimal``.

    The base-4 value of the id's first ``k`` digits (:func:`_decimal_rank`'s
    convention, digits ``1..4`` -> ``0..3``), where ``k`` is the split order —
    so ownership is a pure prefix test and is disjoint by construction. Raises
    ``ValueError`` for a node COARSER than the split order: such a node spans
    partitions and belongs to the finisher, never to a partition worker.
    """
    from zagg.hive import _decimal_base, _decimal_order, _decimal_rank

    k = partition_split_order(partitions)
    order = _decimal_order(decimal)
    if order < k:
        raise ValueError(
            f"node {decimal} is at order {order}, coarser than the partitions={partitions} "
            f"split order {k}; coarse nodes span partitions (they are the finisher's, "
            f"issue #377)"
        )
    return _decimal_rank(decimal[: len(_decimal_base(decimal)) + k])


def normalize_partition(partition) -> tuple[int, int] | None:
    """Validate a wire ``{"index", "of"}`` block into ``(index, of)``.

    ``None`` passes through as ``None`` (an unpartitioned pass). The ``of``
    count goes through :func:`partition_split_order`, so the digit-boundary
    rule is enforced worker-side too — a malformed or hand-rolled event never
    silently sweeps the wrong subtree.
    """
    if partition is None:
        return None
    try:
        index, of = int(partition["index"]), int(partition["of"])
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(
            f"sweep partition must be {{'index': int, 'of': int}} (got {partition!r})"
        ) from e
    partition_split_order(of)
    if not 0 <= index < of:
        raise ValueError(f"sweep partition index {index} is out of range for of={of}")
    return index, of


def partition_leaves(leaves, partitions: int) -> dict[int, list]:
    """Split a ``(shard_key, window)`` work set into disjoint per-partition lists.

    The dispatch-side half of the decomposition: ``{partition index: work
    set}`` for the NON-EMPTY partitions only, in index order, each list in the
    same ``(int key, window)`` currency :func:`zagg.sweep.run_sweep` takes.

    Sparse deliberately. A dense index-aligned return would allocate one slot
    per partition whatever the work set holds, so an operator typo like
    ``partitions=4**15`` materializes 10^9 empty lists before any ref can
    refuse it — and on an EMPTY work set there is no ref to refuse it at all.
    No caller wants the empty slots either: both drop them on the next line
    rather than fire an empty invoke.

    ``partitions`` is validated FIRST: this is the dispatch-side entry point,
    reached long before any manifest names a shard order, so a bad count must
    fail as a parameter error (even on an empty work set) rather than as a
    per-ref one.
    """
    from zagg.grids.morton import morton_decimal

    partition_split_order(partitions)
    buckets: dict[int, list] = {}
    for ref in leaves:
        key, window = ref if isinstance(ref, (tuple, list)) else (ref, None)
        index = partition_index(morton_decimal(int(key)), partitions)
        buckets.setdefault(index, []).append((int(key), window))
    return dict(sorted(buckets.items()))


def select_partition(by_shard: dict, partitions: int, index: int) -> tuple[dict, int]:
    """Restrict a normalized work set to one partition; ``(kept, n_foreign)``.

    ``n_foreign`` counts DROPPED ``(shard, window)`` leaves — the same currency
    as the summary's ``n_leaves``, so ``n_leaves + foreign_leaves`` is the work
    set the invoke was handed. Counting shard nodes instead would understate it
    by the window multiplicity on a D13 windowed store.

    The worker-side half: the ``discover: true`` transport re-derives the WHOLE
    store's work set from its run records, and a hand-rolled invoke may carry
    anything, so the partition filter is applied where the fold happens rather
    than trusted from the dispatcher. ``by_shard`` is
    :func:`zagg.sweep._normalize_leaves`' ``{shard_decimal: {window, ...}}``.
    ``(index, partitions)`` is re-validated here for the same reason: an
    out-of-range index would otherwise filter EVERYTHING out and read as a
    clean "nothing to do" — the silent wrong answer this filter exists to
    prevent.
    """
    normalize_partition({"index": index, "of": partitions})
    kept = {d: w for d, w in by_shard.items() if partition_index(d, partitions) == index}
    total = sum(len(w) for w in by_shard.values())
    return kept, total - sum(len(w) for w in kept.values())


#: How many ladder nodes one stage invoke should fold (issue #610). A tuple
#: narrows until its fattest dispatch node is within this, so the per-invoke
#: wall is bounded by the store's own density rather than by ``tuple_width``.
#:
#: Derived from the v3 run (``stage-20261008T054003Z-a47e28``): at width 3 the
#: base-cell-``3`` invoke of the ``[2,1,0]`` tuple folded 21 nodes — 1 + 4 + 16
#: covered — and hit the 900 s wall after 12 of its 16 order-2 nodes, ~75 s a
#: node on 2,865 shards. The knob's granularity is coarse because a subtree's
#: node count steps by powers of four (21, 5, 1 for widths 3, 2, 1), so any
#: target in [5, 20] gives that run the same width-2 schedule, ~5 nodes and
#: ~375 s an invoke. 8 sits in the middle of that plateau.
STAGE_TARGET_NODES = 8


def sized_stage_tuples(
    shard_order: int,
    *,
    nodes_at=None,
    tuple_width: int | None = None,
    target: int = STAGE_TARGET_NODES,
) -> list[dict]:
    """:func:`zagg.sweep_stage.stage_tuples` with each width sized to the per-invoke fold.

    The ladder's answer to the families pass's leaf-count sizing (issue #610,
    espg 2026-10-08): a dispatch node folds its WHOLE subtree down to the
    tuple's ``child_order`` inside one invoke, so a fixed ``tuple_width``
    puts ``1 + 4 + ... + 4**(width-1)`` nodes on one worker wherever the
    store is dense — 21 at width 3, which is what walled the v3 ladder's
    coarsest tuple.

    A REFINEMENT of the fixed-width schedule, not a second schedule beside
    it: each :func:`zagg.sweep_stage.stage_tuples` tuple is subdivided inside
    its OWN ``[dispatch, child_order)`` span, taking the widest sub-width
    whose fattest dispatch node folds at most ``target`` nodes. Narrowing
    therefore only ever ADDS a boundary inside a fixed tuple and never moves
    one, so a store needing no narrowing yields ``stage_tuples`` exactly —
    for every ``shard_order``, the ragged finest tuple included. (A walk that
    took full widths down from ``shard_order`` instead put its ragged tuple
    at the COARSE end and so reshaped every boundary of a ragged ladder that
    needed no narrowing at all — review finding.)

    ``nodes_at(order)`` returns the covered nodes at ``order`` as decimals:
    what a stage WORKER folds there, which is its candidate set
    (:func:`zagg.sweep_overview._candidate_decimals` — the run's leaves UNION
    the store's root ``coverage.moc``), NOT the run's work set alone. The work
    set's ancestors are a subset of that — a sibling shard an earlier run
    committed is folded although no leaf of this run names it — so sizing
    from them can only UNDER-estimate the fold, which is the unsafe direction
    (review finding); :func:`zagg.sweep_fleet.candidate_dispatch_nodes` is
    the dispatcher-side spelling of the worker's set.

    ``None`` — a dispatcher holding no coverage MOC, which it may not read for
    itself (D8) — sizes against the DENSE bound ``(4 ** width - 1) // 3`` (1,
    5, 21, 85 for widths 1..4): the fold a dispatcher that cannot see the
    store's density must assume. It never under-estimates, and at
    :data:`STAGE_TARGET_NODES` it picks width 2 everywhere — the width the v3
    store needed. ``nodes_at`` is called once per candidate order and the
    results are reused across candidate widths.

    Returns the :func:`zagg.sweep_stage.stage_tuples` items, finest first and spanning
    ``[0, shard_order)`` without gap or overlap exactly as the fixed-width
    schedule does, each with the ``width`` it was given and the ``fold_max``
    that chose it — measured when ``nodes_at`` was supplied, the dense bound
    otherwise.

    The grouping is a dispatch knob, never grammar: by the merge-source law
    (#381 point (6)) a sized schedule and a fixed one build the same ladder
    OVERVIEWS, which is what the oracle compares and what a reader reads. Not
    every byte, and the suite does not claim it (review finding): the group
    attrs carry per-run provenance that is legitimately grouping-dependent
    (``source_children``, the summed child ``generation``), and a sized arm
    writes stage columns a fixed arm never needs.
    """
    from zagg.sweep_stage import DEFAULT_TUPLE_WIDTH, _node_at, one_stage_tuple, stage_tuples

    shard_order = int(shard_order)
    tuple_width = int(DEFAULT_TUPLE_WIDTH if tuple_width is None else tuple_width)
    target = int(target)
    if tuple_width < 1:
        raise ValueError(f"tuple_width must be >= 1 (got {tuple_width})")
    if target < 1:
        raise ValueError(f"target must be >= 1 node per invoke (got {target})")
    if shard_order < 1:
        raise ValueError(f"shard_order {shard_order} has no above-shard ladder to sweep")

    cache: dict = {}

    def covered(order: int) -> list:
        if order not in cache:
            cache[order] = [str(n) for n in nodes_at(order)]
        return cache[order]

    def fold_max(dispatch: int, child_order: int) -> int:
        """The fattest dispatch node's node count over ``[dispatch, child_order)``."""
        if nodes_at is None:
            # 1 + 4 + ... + 4**(width-1): the bound a dispatcher that cannot
            # see the store's density must assume.
            return (4 ** (child_order - dispatch) - 1) // 3
        counts: dict = {d: 0 for d in covered(dispatch)}
        for order in range(dispatch, child_order):
            for node in covered(order):
                ancestor = _node_at(node, dispatch)
                if ancestor in counts:
                    counts[ancestor] += 1
        return max(counts.values(), default=0)

    tuples = []
    for fixed in stage_tuples(shard_order, tuple_width=tuple_width):
        base, child = int(fixed["dispatch"]), int(fixed["child_order"])
        while child > base:
            # Widest first and INSIDE this fixed tuple's own span, so the
            # result is a refinement of the fixed-width schedule: a store that
            # needs no narrowing keeps that schedule, ragged tuple included.
            # The last candidate is width 1, where a dispatch node folds itself
            # alone (bound 1; measured 1, or 0 where the order is uncovered),
            # so it clears every ``target >= 1`` the refusal above admits —
            # the generator is never empty and ``next`` cannot raise.
            spans = ((d, fold_max(d, child)) for d in range(base, child))
            dispatch, fold = next((d, at) for d, at in spans if at <= target)
            stage = one_stage_tuple(shard_order, dispatch, child)
            tuples.append({**stage, "width": child - dispatch, "fold_max": fold})
            child = dispatch
    return tuples


def sweep_partitions(store_root: str, leaves, *, partitions: int, **kwargs) -> list[dict]:
    """Run every non-empty partition of one sweep pass IN-PROCESS, in index order.

    The single-process mirror of the ``2^n``-invoke fan-out: no parallelism,
    but each pass folds one partition's subtrees only, so peak resident memory
    is bounded by the partition rather than by the store — the half of issue
    #377 a CLI backstop can still buy on a store no single walk fits. Returns
    one :func:`zagg.sweep.run_sweep` summary per non-empty partition;
    ``kwargs`` forward verbatim (``families``, ``store_kwargs``, ``record``).

    Coarse levels above the split order are swept by NO partition here — see
    the module docstring; the finisher leg is issue #377's deferred phase.
    """
    from zagg.sweep import run_sweep

    return [
        run_sweep(store_root, work, partition={"index": index, "of": partitions}, **kwargs)
        for index, work in partition_leaves(leaves, partitions).items()
    ]
