"""Pure-numpy t-digest implementation for approximate quantile sketching.

A t-digest (Dunning & Ertl, 2019) is a mergeable sketch of a distribution
represented as a sorted list of weighted centroids (mean, weight).  The
algorithm groups nearby centroids so that centroids near the tails (quantile
close to 0 or 1) are narrow (high-precision) and centroids near the middle are
wide (lower-precision).

This module is **pure numpy** — no scipy, no numba, no JAX.  It is the Tier-2
ragged consumer for issue #48 and the first ``kind: ragged`` reducer wired into
:func:`zagg.config.resolve_function`.

Algorithm (scale-function sort-and-merge variant)
--------------------------------------------------
``build_tdigest`` sorts the input, then greedily merges adjacent observations
into centroids bounded by Dunning's k1 scale function:

1. Sort all input values.
2. Walk sorted values, accumulating each into the current centroid.
3. A point joins the current centroid while the centroid still spans ≤ 1 unit
   of the k1 scale ``k(q) = δ * (arcsin(2q − 1)/π + 1/2)``, which maps the
   cumulative rank fraction q ∈ [0, 1] onto [0, δ].  ``dk/dq`` is largest at the
   tails (q → 0 or 1) and smallest at the median, so centroids are narrow and
   high-resolution in the tails and wide in the middle — the defining t-digest
   property.
4. When adding the next point would make the centroid span more than 1 k-unit,
   finalize it and start a fresh centroid.

Because k maps onto a fixed ``[0, δ]`` range, a digest holds ~δ centroids
regardless of the observation count (it saturates instead of growing with n),
and is **loss-free** — one centroid per observation, every weight 1 — while the
count stays ≤ δ.  Larger δ therefore means more centroids and higher accuracy.

``merge_tdigests`` concatenates two centroid arrays sorted by mean, then
re-compresses with the same k1-bounded merge so the result respects the same
~δ centroid budget.  The merged sketch matches the one-shot sketch within
typical t-digest accuracy guarantees.

Profiling note (IO vs compute)
-------------------------------
At δ=512, ``build_tdigest`` saturates near ~512 centroids once the observation
count exceeds δ; the output is a ``(k, 2)`` float32 array of ≲4 kB, much smaller
than the raw observation array.  Over a 4096-cell shard with ~500 obs per cell,
the per-cell sort (O(n log n)) and merge loop (O(n)) are the dominant cost at
~10 μs/cell, giving ~40 ms/shard — well below network IO.  At δ=128 the centroid
count (and output size) is ~4× smaller; at δ=1024 the digest carries ~2× more
centroids and is more accurate near the tails.

Usage
-----
Wire as a ragged reducer in a YAML config::

    variables:
      h_tdigest:
        function: zagg.stats.tdigest.build_tdigest
        source: h_li
        kind: ragged
        inner_shape: [2]
        dtype: float32
        params:
          delta: 512

``calculate_cell_statistics`` calls ``build_tdigest(values, delta=512)`` per
cell and stores the ``(k, 2)`` centroid array in the ragged field.

Adding ``location: leaf_id`` to the field (issue #87) passes the cell's
per-observation order-29 morton point words as ``locations=``; the reducer then
returns a ``(digest, locations)`` pair whose second element is the ``(k,)``
uint64 per-centroid location — the deepest morton cell enclosing each
centroid's members (``mortie.common_ancestor``), stored as a companion vlen
array. See ``zagg/configs/atl03_tdigest_located_healpix.yaml``.

Adding ``temporal: per-centroid`` (spec §8.3, issue #410) passes the cell's
per-observation toc words as ``temporal=`` and adds a third element to the
returned tuple: the ``(k,)`` uint64 per-centroid **temporal envelope** — the
grammar's semilattice join (``mortie.toc_reduce``) over each centroid's
members' words, an exact nanosecond timestamp for a 1-observation centroid and
a conservative range otherwise. The two companion channels are independent and
either may be declared alone; when both are, the returned tuple is
``(digest, locations, temporal)`` — always payload first, then the channels in
that order.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from typing import overload

import numpy as np

#: A companion channel's per-centroid reduction: member words + the partition
#: map ``_compress`` returns -> one word per output centroid.
_ChannelFold = Callable[[np.ndarray, np.ndarray, int], np.ndarray]

#: One deferred fold: member words, offset-adjusted starts, placeholder, declared n.
_Deferred = tuple[np.ndarray, np.ndarray, np.ndarray, int]

#: Deferred member rows per channel before the batch folds mid-loop, bounding the
#: words it pins to ~8 MB per channel (issue #476 review finding). The folds are
#: segment-local, so the cut point cannot change an output word.
_BATCH_ROW_CAP = 1_000_000

__all__ = [
    "build_tdigest",
    "build_tdigest_pairwise",
    "cdf_from_tdigest",
    "merge_tdigests",
    "merge_tdigests_kway",
    "quantile_from_tdigest",
]

_DEFAULT_DELTA = 512


def _k1_scale_array(q: np.ndarray, delta: float) -> np.ndarray:
    """Dunning's k1 scale ``k(q) = delta * (arcsin(2q - 1)/pi + 1/2)``, vectorized.

    Maps each cumulative rank fraction ``q ∈ [0, 1]`` onto ``[0, delta]``.  The
    derivative is largest at the tails (q → 0 or 1) and smallest at the median,
    so bounding each centroid to span ≤ 1 unit of k yields narrow, high-
    resolution centroids in the tails and wide centroids in the middle — the
    defining t-digest property.

    Evaluating the ``arcsin`` for every rank in **one** ufunc call is the whole
    point: the scalar per-observation ``float(np.arcsin(...))`` in the old
    compression loop was numpy-dispatch-bound, so hoisting it here is what turns
    the ``O(n)`` Python loop into an ``O(delta)`` one (issue #279).
    """
    qc = np.clip(q, 0.0, 1.0)
    return delta * (np.arcsin(2.0 * qc - 1.0) / np.pi + 0.5)


def _compress(
    means: np.ndarray, weights: np.ndarray, delta: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """k1-bounded greedy compression of sorted weighted sub-centroids.

    Shared core for both :func:`build_tdigest` (unit weights over sorted values)
    and the merge paths (weighted sub-centroids). ``means`` must be ascending
    and ``weights`` strictly positive, both 1-D float64.

    Returns ``(out_means, out_weights, starts)``: the compressed centroid means
    (weighted averages), their weights, and ``starts`` — each output centroid's
    first sub-centroid index, which the located channel reduces over.

    This reproduces the old scalar loop's greedy rule (same k1 values at the same
    rank fractions, same ``span ≤ 1`` join test) and was verified bit-identical
    to it across randomized trials. Exact equality is not guaranteed *by
    construction*, though: the join comparison is re-associated (``searchsorted``
    on ``k_left + 1.0`` vs the loop's ``k_right - k_left <= 1.0``) and the rank
    denominator uses ``cumw[-1]`` vs a pairwise ``.sum()``, so a boundary sitting
    within an ULP of a 1.0 k-span could in principle flip. Means additionally
    differ at the float-ULP level, being formed as a single weighted
    ``sum / weight`` rather than an incremental Welford update.
    Instead of testing every sub-centroid, this jumps straight to each
    centroid's right edge with one ``searchsorted`` per centroid — so the loop
    runs ~delta times regardless of ``n`` (issue #279).
    """
    n = len(means)
    cumw = np.cumsum(weights)
    total = float(cumw[-1])
    # k1 at each sub-centroid's right edge (cumulative rank fraction cumw[i]/total).
    # Monotonic in i (weights > 0), so searchsorted can locate boundaries.
    k_right = _k1_scale_array(cumw / total, delta)
    k_left0 = float(_k1_scale_array(np.zeros(1), delta)[0])

    starts_list: list[int] = []
    s = 0
    while s < n:
        starts_list.append(s)
        # Left edge of the centroid starting at s: k1(cumw[s-1]/total), which is
        # exactly k_right[s-1] (0-edge for s == 0). A sub-centroid i joins while
        # k_right[i] - k_left <= 1; the first i that breaks it opens the next
        # centroid. searchsorted(side="right") is that first breaking index.
        k_left = k_left0 if s == 0 else float(k_right[s - 1])
        e = int(np.searchsorted(k_right, k_left + 1.0, side="right"))
        if e <= s:
            # The very next sub-centroid already overflows the k-span (steep tail):
            # the centroid holds only its start. Guarantees forward progress.
            e = s + 1
        s = e

    starts = np.asarray(starts_list, dtype=np.int64)
    out_weights = np.add.reduceat(weights, starts)
    out_means = np.add.reduceat(means * weights, starts) / out_weights
    return out_means, out_weights, starts


class _CompanionBatch:
    """Cross-cell batcher for the per-centroid companion folds (issue #476).

    Inside :func:`batched_companion_folds`, each :func:`_centroid_ancestors` /
    :func:`_centroid_envelopes` call *defers*: it hands back an unfilled
    ``uint64`` placeholder of the right length and records its ``(member
    words, offset-adjusted starts, n)``. The flush then runs the real fold ONCE
    per channel over the concatenated layout and fills every placeholder in
    place.  Both folds are segment-independent — each output word reduces over
    its own member slice — and ``_compress`` always emits starts partitioning
    ``[0, n)`` from 0, so the concatenation is itself a valid ``(words,
    starts, n)`` fold input and the batched result is byte-for-byte the
    per-call results, sliced back apart.  The same segment-locality bounds the
    memory: a channel that reaches ``_BATCH_ROW_CAP`` deferred rows folds mid-loop
    (:meth:`flush_detached`) instead of holding the whole chunk's words.

    Correctness requires deferred outputs to be **write-only and un-copied
    until the flush**, which holds for the one activation site
    (``_aggregate_chunk_cells``' per-cell loop): the channel vectors are only
    collected for the ragged writer, which runs after that function returns, and
    the one normalization on the way there (``np.ascontiguousarray`` in
    ``calculate_cell_statistics``) is identity-preserving for the contiguous
    ``uint64`` placeholder handed out here.  A reducer that returned a *copy*
    would strand the flush on an orphan, which is why the placeholder is
    ``zeros`` (a refusable reserved word) rather than ``empty``.
    """

    def __init__(self) -> None:
        #: fold -> its deferrals, in call order
        self._pending: dict[_ChannelFold, list[_Deferred]] = {}
        #: fold -> running member-row count (the next deferral's offset)
        self._rows: dict[_ChannelFold, int] = {}

    def defer(self, fold: _ChannelFold, words: np.ndarray, starts, n: int) -> np.ndarray:
        starts = np.asarray(starts, dtype=np.int64)
        # The concatenation is a valid segmented layout only if each deferral's
        # partition starts at 0 (``_compress`` guarantees it). Neither fold would
        # catch a violation — the exact-cover check still passes, the offending
        # rows just fold into the previous entry's last centroid — so make the
        # invariant load-bearing rather than documented (O(1) per deferral).
        if len(starts) and starts[0] != 0:
            raise ValueError(
                f"deferred companion fold got starts[0]={int(starts[0])}; the cross-cell "
                "batch requires each centroid partition to start at 0 (see _compress)"
            )
        # ``zeros``, not ``empty``: if a placeholder ever escapes the flush (a
        # reducer that copies its channel instead of handing the vector straight
        # back — see the ``ascontiguousarray`` note in ``calculate_cell_statistics``)
        # the unfilled vector reads as spec 8.2's reserved unobserved word, which
        # ``_check_words``/``zagg.stats.toc`` refuse outright, rather than as
        # plausible-looking uninitialized bytes. A calloc of <= delta words is free
        # against the fold it replaces.
        out = np.zeros(len(starts), dtype=np.uint64)
        rows = self._rows.get(fold, 0)
        # The declared ``n`` rides along rather than being re-derived from
        # ``len(words)`` at the flush: the two agree for every current caller, but a
        # deferral whose ``n`` overruns its words must fail where the unbatched call
        # would (mortie's exact-cover check) rather than silently fold a different
        # partition on the single-entry arm.
        self._pending.setdefault(fold, []).append((words[:n], starts + rows, out, n))
        self._rows[fold] = rows + n
        # Bound the held member words (review finding, PR #478): the batch pins a
        # copy of every deferral's words until its flush, which at 1.5 M obs is
        # ~12 MB per channel for a whole chunk. Folding mid-loop at the cap is
        # byte-identical — the folds are segment-local, so where the batch is cut
        # cannot move an output word — and keeps essentially the whole FFI win
        # (one crossing per _BATCH_ROW_CAP rows rather than one per cell-field).
        if self._rows[fold] >= _BATCH_ROW_CAP:
            self.flush_detached()
        return out

    def flush_detached(self) -> None:
        """Flush from *inside* the context: deactivate the batch, then fold.

        ``flush`` calls the real folds, which re-enter :func:`_centroid_ancestors`
        / :func:`_centroid_envelopes` — with the batch still installed they would
        defer straight back into it (and recurse). Deactivating first is exactly
        what :func:`batched_companion_folds` does at exit.
        """
        token = _FOLD_BATCH.set(None)
        try:
            self.flush()
        finally:
            _FOLD_BATCH.reset(token)

    def flush(self) -> None:
        """Run each channel's fold once over its concatenated deferrals."""
        for fold, entries in self._pending.items():
            if len(entries) == 1:
                words, starts, out, n = entries[0]
                out[:] = fold(words, starts, n)
                continue
            words_all = np.concatenate([w for w, _, _, _ in entries])
            starts_all = np.concatenate([s for _, s, _, _ in entries])
            reduced = fold(words_all, starts_all, self._rows[fold])
            pos = 0
            for _, _, out, _ in entries:
                out[:] = reduced[pos : pos + len(out)]
                pos += len(out)
        self._pending.clear()
        self._rows.clear()


#: The active cross-cell fold batch; ``None`` outside ``batched_companion_folds``.
_FOLD_BATCH: ContextVar[_CompanionBatch | None] = ContextVar("_FOLD_BATCH", default=None)


@contextmanager
def batched_companion_folds():
    """Defer the per-centroid companion folds; run each once per channel at exit.

    The issue #476 hoist: at CA shard shape the per-cell loop makes ~46k
    segmented ``mortie.toc_reduce`` calls (one per populated cell-field) whose
    cost is Python/FFI dispatch, not reduction — under this context they
    collapse to one call per channel per chunk, byte-identically (see
    :class:`_CompanionBatch`).  Activated by ``_aggregate_chunk_cells`` around
    its per-cell loop; nested activation is a passthrough into the outer batch's
    scope.  The flush runs only on a clean exit — after an exception the
    placeholders are never read, so they are dropped, and the batch is always
    deactivated before flushing so the folds it runs execute for real.
    """
    if _FOLD_BATCH.get() is not None:
        yield
        return
    batch = _CompanionBatch()
    token = _FOLD_BATCH.set(batch)
    try:
        yield
    finally:
        _FOLD_BATCH.reset(token)
    batch.flush()


def _centroid_ancestors(locations: np.ndarray, starts: np.ndarray, n: int) -> np.ndarray:
    """Reduce per-member morton locations to one enclosing cell per centroid.

    ``locations`` is member-ordered (aligned with the sorted values / combined
    centroids), ``starts`` the first member index of each centroid.  Each
    centroid's location is ``mortie.common_ancestor`` over its members' words —
    the deepest cell containing all of them (issue #87).  Mixed-order input is
    fine (a below-order-29 mean-morton from a prior merge folds with fresh
    order-29 points), and a single member returns itself with its point kind
    preserved, so a 1-obs centroid round-trips its exact order-29 point word.

    Below the compression knee (n ≤ δ) every centroid is a singleton, so the
    per-centroid ``common_ancestor`` loop would run once per *photon* — the
    same O(n) Python-loop shape issue #279 removed from the build path.
    Singletons copy their sole member's word through vectorized; the loop only
    covers multi-member centroids.  ``common_ancestor`` also *validates* its
    input (a zero — the configs' fill value — or any word with a bad base-cell
    prefix raises), so the copy-through keeps that guarantee by validating the
    whole singleton set in one kernel pass; otherwise a bad word would raise
    only in the compressed regime.

    Under :func:`batched_companion_folds` the call defers instead (issue
    #476): the singleton validation and the multi-member loop then run once
    over the whole batch's concatenated partition, byte-identically.
    """
    batch = _FOLD_BATCH.get()
    if batch is not None:
        return batch.defer(_centroid_ancestors, locations, starts, n)
    from mortie import common_ancestor, validate_morton

    starts = np.asarray(starts, dtype=np.int64)
    sizes = np.diff(starts, append=n)
    out = np.empty(len(starts), dtype=np.uint64)
    single = sizes == 1
    singles = locations[starts[single]]
    if singles.size:
        validate_morton(singles)
    out[single] = singles
    for j in np.flatnonzero(~single):
        out[j] = common_ancestor(locations[starts[j] : starts[j] + sizes[j]])
    return out


def _centroid_envelopes(temporal: np.ndarray, starts: np.ndarray, n: int) -> np.ndarray:
    """Reduce per-member toc words to one temporal envelope per centroid (§8.3).

    ``temporal`` is member-ordered (aligned with the sorted values / combined
    centroids), ``starts`` the first member index of each centroid.  Each
    centroid's word is the grammar's semilattice join over its members' words
    (``mortie.toc_reduce``, the segmented ``toc_merge`` reduce,
    espg/mortie#177): the conservative envelope containing every observation the
    centroid summarizes.  A single-member centroid returns that member's word
    unchanged, so a 1-observation centroid round-trips its exact nanosecond
    timestamp instead of widening into a range.

    The join being associative, commutative and idempotent buys **cell-level**
    invariance, not per-centroid invariance: a per-centroid vector is indexed by
    the centroid partition, and its words are exact *given* that partition, not
    independently of it (spec §8.3) — the partition moves with the fold tree.  A
    different fold tree over the same observations therefore generally yields
    different per-centroid words, while :func:`zagg.stats.toc.cell_envelope`
    over either vector returns the same word.  That cell-level identity is what
    licenses §8.4's per-centroid → per-cell coarsening, and it is what
    ``test_cell_envelope_is_fold_tree_independent`` pins; the channel's
    exactness does not lift the field's composability class (spec §2.3/§8.3).

    Unlike :func:`_centroid_ancestors` there is no per-centroid Python loop:
    ``starts`` is already the arrow offsets layout ``toc_reduce`` takes, so the
    whole partition crosses into Rust once.  ``_compress`` never emits an empty
    run, which is what the segmented reduce refuses (the join has no identity).
    The reserved-``0`` refusal lives in :func:`_check_words`, which every caller
    passes its words through — including the merge arms that pass a channel
    straight back without reducing it.

    Under :func:`batched_companion_folds` the call defers instead (issue
    #476): one ``toc_reduce`` then crosses into Rust per batch rather than
    per cell-field, byte-identically (the reduce is segmented).
    """
    batch = _FOLD_BATCH.get()
    if batch is not None:
        return batch.defer(_centroid_envelopes, temporal, starts, n)
    from mortie import toc_reduce

    starts = np.asarray(starts, dtype=np.int64)
    offsets = np.empty(len(starts) + 1, dtype=np.int64)
    offsets[:-1] = starts
    offsets[-1] = n
    return np.asarray(toc_reduce(temporal, offsets=offsets), dtype=np.uint64)


def _check_words(words, label: str, shape: tuple) -> np.ndarray:
    """Validate one companion channel's input: packed ``uint64``, shape-aligned,
    no reserved ``0``.

    A silent ``uint64`` cast would turn a mis-declared float column into
    plausible-looking morton or toc words, so both channels require packed
    ``uint64`` outright — what ``HealpixGrid.assign`` supplies as ``leaf_id``
    and what :func:`zagg.time_axis.observation_words` supplies as toc words.

    ``0`` is refused on both channels: it is the configs' fill value, spec §8.2's
    reserved unobserved marker for a toc word, and not a valid morton word
    either.  The refusal belongs here rather than in the per-channel reducers
    because this is the one check every arm runs — including the pass-throughs
    (:func:`merge_tdigests` with an empty side, :func:`merge_tdigests_kway` with
    a single contributor), which return their channels unreduced and so never
    reach ``common_ancestor`` / ``toc_reduce``.  A leaked fill would otherwise
    be stored for an observed cell, which §8.2 forbids outright.
    """
    arr = np.asarray(words)
    if arr.dtype != np.uint64:
        raise ValueError(f"{label} dtype {arr.dtype} is not uint64; pass packed uint64 words")
    if arr.shape != shape:
        raise ValueError(f"{label} shape {arr.shape} does not match the expected {shape}")
    if arr.size and not arr.all():
        raise ValueError(
            f"{label} contains the reserved 0 word (spec §8.2's unobserved marker for a "
            "toc word, and no valid morton word); pass encoded words, not a fill value"
        )
    return arr


@overload
def build_tdigest(
    values: np.ndarray, delta: int = ..., locations: None = ..., temporal: None = ...
) -> np.ndarray: ...
@overload
def build_tdigest(
    values: np.ndarray, delta: int = ..., *, locations: np.ndarray
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def build_tdigest(
    values: np.ndarray, delta: int = ..., *, temporal: np.ndarray
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def build_tdigest(
    values: np.ndarray, delta: int = ..., *, locations: np.ndarray, temporal: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...
def build_tdigest(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    locations: np.ndarray | None = None,
    temporal: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, ...]:
    """Build a t-digest sketch from a 1-D array of values.

    Parameters
    ----------
    values : ndarray
        1-D array of observed values (any finite float).  NaN values are
        silently dropped before sketching.
    delta : int, optional
        Compression parameter.  Larger δ → more centroids → more accurate.
        Default 512.  Typical values: 128, 256, 512, 1024.
    locations : ndarray, optional
        Per-observation ``uint64`` morton point words (issue #87), aligned with
        ``values``; observations dropped for NaN values drop their location too.
        When given, each centroid also carries a location: the deepest morton
        cell enclosing its members' words (``mortie.common_ancestor``), which
        for a 1-obs centroid is that observation's exact point word.  All words
        must share one HEALPix base cell (guaranteed when they come from one
        grid cell's observations; mortie raises otherwise).
    temporal : ndarray, optional
        Per-observation ``uint64`` toc words (spec §8.3, issue #410), aligned
        with ``values`` under the same NaN-drop rule as ``locations``.  When
        given, each centroid also carries a temporal envelope: the grammar's
        join over its members' words (``mortie.toc_reduce``), which for a
        1-obs centroid is that observation's exact nanosecond timestamp.

    Returns
    -------
    ndarray, shape (k, 2), dtype float32
        Sorted centroid array.  Column 0 is centroid mean; column 1 is weight
        (number of observations merged into that centroid).
        With either companion channel, returns a tuple instead — the digest
        followed by each declared channel's ``(k,)`` uint64 vector, in the
        order ``(digest, locations, temporal)``.

    Notes
    -----
    Returns an empty ``(0, 2)`` array when ``values`` is empty or all-NaN (with
    empty channel vectors beside it).  The digest itself is identical whichever
    channels are declared.
    """
    values = np.asarray(values, dtype=np.float64)
    finite = np.isfinite(values)
    channels = []
    if locations is not None:
        locations = _check_words(locations, "locations", values.shape)[finite]
        channels.append(locations)
    if temporal is not None:
        temporal = _check_words(temporal, "temporal", values.shape)[finite]
        channels.append(temporal)
    values = values[finite]
    n = len(values)
    if n == 0:
        empty = np.empty((0, 2), dtype=np.float32)
        if channels:
            return (empty, *(np.empty(0, dtype=np.uint64) for _ in channels))
        return empty

    if channels:
        # Stable co-sort so equal values keep a deterministic channel order.
        order = np.argsort(values, kind="stable")
        sorted_vals = values[order]
        if locations is not None:
            locations = locations[order]
        if temporal is not None:
            temporal = temporal[order]
    else:
        sorted_vals = np.sort(values)

    # Unit-weight sub-centroids (one per observation); ``_compress`` greedily
    # merges adjacent ones under the k1 budget. ``starts`` records each output
    # centroid's first member index in the sorted order, so each companion
    # channel can reduce member words per centroid afterwards.
    out_means, out_weights, starts = _compress(
        sorted_vals, np.ones(n, dtype=np.float64), float(delta)
    )

    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if not channels:
        return out
    folded = []
    if locations is not None:
        folded.append(_centroid_ancestors(locations, starts, n))
    if temporal is not None:
        folded.append(_centroid_envelopes(temporal, starts, n))
    return (out, *folded)


def build_tdigest_where(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    *,
    where: np.ndarray,
    locations: np.ndarray | None = None,
    temporal: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, ...]:
    """t-digest of the observations selected by a row-aligned boolean mask.

    The stratified reducer for issue #321: a config declares two ragged fields
    over the same ``source`` whose ``where`` params are complementary
    expressions (e.g. the ATBD signal predicate over the ``signal_conf``
    columns and its negation), yielding disjoint signal/noise digests whose
    total weights are the exact stratum counts. Everything else —
    serialization, merge laws, the companion channels — is inherited from
    :func:`build_tdigest`, which this delegates to after masking.
    """
    values = np.asarray(values)
    where = np.asarray(where)
    if where.shape != values.shape:
        raise ValueError(f"where shape {where.shape} does not match values shape {values.shape}")
    where = where.astype(bool, copy=False)
    kwargs = {}
    for label, channel in (("locations", locations), ("temporal", temporal)):
        if channel is None:
            continue
        channel = np.asarray(channel)
        if channel.shape != values.shape:
            raise ValueError(
                f"{label} shape {channel.shape} does not match values shape {values.shape}"
            )
        kwargs[label] = channel[where]
    return build_tdigest(values[where], delta=delta, **kwargs)


@overload
def merge_tdigests(
    d1: np.ndarray, d2: np.ndarray, delta: int = ..., locations1: None = ..., locations2: None = ...
) -> np.ndarray: ...
@overload
def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = ...,
    *,
    locations1: np.ndarray,
    locations2: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = ...,
    *,
    temporal1: np.ndarray,
    temporal2: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = ...,
    *,
    locations1: np.ndarray,
    locations2: np.ndarray,
    temporal1: np.ndarray,
    temporal2: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...
def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    locations1: np.ndarray | None = None,
    locations2: np.ndarray | None = None,
    temporal1: np.ndarray | None = None,
    temporal2: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, ...]:
    """Merge two t-digest centroid arrays into one.

    Concatenates the centroid arrays, sorts by mean, and re-compresses with
    the same scale-limited merge as :func:`build_tdigest` so the result
    respects the δ-budget.

    Parameters
    ----------
    d1 : ndarray, shape (k1, 2)
        First centroid array (mean, weight) as returned by :func:`build_tdigest`.
    d2 : ndarray, shape (k2, 2)
        Second centroid array.
    delta : int, optional
        Compression parameter (same default as :func:`build_tdigest`).
    locations1, locations2 : ndarray, optional
        Per-centroid ``uint64`` morton locations (issue #87) aligned with
        ``d1`` / ``d2``, as returned by the located :func:`build_tdigest` (or a
        prior located merge).  Pass both or neither.  A merged centroid's
        location is ``mortie.common_ancestor`` over its members' locations —
        mixed orders fold fine, so an already-collapsed low-order mean-morton
        merges with fresh order-29 point words.  All locations folded into one
        merged centroid must share a HEALPix base cell (guaranteed when both
        digests come from the same grid cell); mortie raises ``ValueError``
        otherwise — cross-base roll-ups need ``mortie.split_base_cells`` and
        are out of scope here.
    temporal1, temporal2 : ndarray, optional
        Per-centroid ``uint64`` toc words (spec §8.3, issue #410) aligned with
        ``d1`` / ``d2``.  Pass both or neither, independently of the located
        channel.  A merged centroid's word is the grammar's join over its
        members' words — exact for the partition it describes, though that
        partition (and so the vector) still moves with the fold tree; only the
        cell-level envelope over the words is fold-tree invariant (spec §8.3).

    Returns
    -------
    ndarray, shape (k_merged, 2), dtype float32
        Merged and re-compressed centroid array.  With either channel, returns
        a tuple — the digest followed by each declared channel's
        ``(k_merged,)`` uint64 vector, in the order
        ``(digest, locations, temporal)``.
    """
    d1 = np.asarray(d1, dtype=np.float64)
    d2 = np.asarray(d2, dtype=np.float64)
    # (reducer, left words, right words) per declared channel, in the fixed
    # return order — everything below is channel-agnostic from here on.
    channels: list[tuple[_ChannelFold, np.ndarray, np.ndarray]] = []
    for label, reducer, c1, c2 in (
        ("locations", _centroid_ancestors, locations1, locations2),
        ("temporal", _centroid_envelopes, temporal1, temporal2),
    ):
        if c1 is None and c2 is None:
            continue
        if (c1 is None) != (c2 is None):
            raise ValueError(f"pass both {label}1 and {label}2, or neither")
        channels.append(
            (
                reducer,
                _check_words(c1, f"{label}1", (len(d1),)),
                _check_words(c2, f"{label}2", (len(d2),)),
            )
        )

    if d1.size == 0 and d2.size == 0:
        empty = np.empty((0, 2), dtype=np.float32)
        if channels:
            return (empty, *(np.empty(0, dtype=np.uint64) for _ in channels))
        return empty
    if d1.size == 0 or d2.size == 0:
        # One contributor is already a valid digest; the channels pass through
        # unreduced. Copy so the return never aliases the caller's array.
        left = d2.size == 0
        kept = np.asarray(d1 if left else d2, dtype=np.float32)
        if not channels:
            return kept
        return (kept, *((c1 if left else c2).copy() for _, c1, c2 in channels))

    combined = np.concatenate([d1, d2], axis=0)
    order = np.argsort(combined[:, 0], kind="stable")
    combined = combined[order]
    joined = [np.concatenate([c1, c2])[order] for _, c1, c2 in channels]

    # Re-compress the concatenated sub-centroids under the same k1 budget as
    # the build path — the weighted variant of the greedy rule (issue #279).
    out_means, out_weights, starts = _compress(combined[:, 0], combined[:, 1], float(delta))

    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if not channels:
        return out
    n = len(combined)
    return (
        out,
        *(fold(words, starts, n) for (fold, _, _), words in zip(channels, joined, strict=True)),
    )


@overload
def merge_tdigests_kway(
    digests: list[np.ndarray], delta: int = ..., locations: None = ..., temporal: None = ...
) -> np.ndarray: ...
@overload
def merge_tdigests_kway(
    digests: list[np.ndarray], delta: int = ..., *, locations: list[np.ndarray]
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def merge_tdigests_kway(
    digests: list[np.ndarray], delta: int = ..., *, temporal: list[np.ndarray]
) -> tuple[np.ndarray, np.ndarray]: ...
@overload
def merge_tdigests_kway(
    digests: list[np.ndarray],
    delta: int = ...,
    *,
    locations: list[np.ndarray],
    temporal: list[np.ndarray],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...
def merge_tdigests_kway(
    digests: list[np.ndarray],
    delta: int = _DEFAULT_DELTA,
    locations: list[np.ndarray] | None = None,
    temporal: list[np.ndarray] | None = None,
) -> np.ndarray | tuple[np.ndarray, ...]:
    """Merge many t-digests in one pass — **order-independent**.

    Concatenates every centroid array, sorts by mean once, and re-compresses
    under the k1 budget. Unlike a left-fold of pairwise :func:`merge_tdigests`,
    the result does **not** depend on the order the digests are combined (t-digest
    merge is not associative), and it holds the centroid count at the δ budget
    instead of drifting above it (issue #279). This is the fold law for the
    standard ``build_tdigest`` reducer on the spill path and for every pyramid
    fold site (:func:`zagg.sweep_overview.fold_digests`).

    Order-independence covers **every** channel: the sort breaks ``(mean,
    weight)`` ties on the location word and then on the toc word, so a
    permutation of the inputs returns the same companion vectors as well as the
    same digest. That is permutation-independence of one flat k-way call, and it
    is *not* fold-tree independence: the temporal channel's reduction is the
    grammar's semilattice join (spec §8.3), but a per-centroid vector is indexed
    by the centroid partition, so what survives an arbitrary fold tree is the
    **cell-level** envelope over the words
    (:func:`zagg.stats.toc.cell_envelope`), not the words themselves — a
    pairwise left-fold reaches a different partition and so different words.

    Parameters
    ----------
    digests : list of ndarray
        Centroid arrays ``(k, 2)`` as returned by :func:`build_tdigest`. Empty
        digests are skipped.
    delta : int, optional
        Compression parameter (same default as :func:`build_tdigest`).
    locations : list of ndarray, optional
        Per-digest ``uint64`` morton location vectors (issue #87), aligned with
        ``digests`` (one array per digest, each aligned with that digest's
        centroids). Pass for the located channel or omit entirely. All folded
        locations must share a HEALPix base cell (mortie raises otherwise).
    temporal : list of ndarray, optional
        Per-digest ``uint64`` toc word vectors (spec §8.3, issue #410), aligned
        with ``digests`` the same way, and declarable independently of
        ``locations``.

    Returns
    -------
    ndarray, shape (k_merged, 2), dtype float32
        Merged, re-compressed centroid array. With either channel, returns a
        tuple — the digest followed by each declared channel's ``(k_merged,)``
        uint64 vector, in the order ``(digest, locations, temporal)``.
    """
    declared = [
        (label, reducer, chans)
        for label, reducer, chans in (
            ("locations", _centroid_ancestors, locations),
            ("temporal", _centroid_envelopes, temporal),
        )
        if chans is not None
    ]
    for label, _, chans in declared:
        if len(chans) != len(digests):
            raise ValueError(f"{label} has {len(chans)} arrays but digests has {len(digests)}")
    arrs: list[np.ndarray] = []
    # Per declared channel, the surviving (non-empty) digests' word vectors.
    kept: list[list[np.ndarray]] = [[] for _ in declared]
    for i, d in enumerate(digests):
        d = np.asarray(d, dtype=np.float64)
        if d.size == 0:
            continue
        arrs.append(d)
        for slot, (label, _, chans) in enumerate(declared):
            kept[slot].append(_check_words(chans[i], f"{label}[{i}]", (len(d),)))

    if not arrs:
        empty = np.empty((0, 2), dtype=np.float32)
        if declared:
            return (empty, *(np.empty(0, dtype=np.uint64) for _ in declared))
        return empty
    if len(arrs) == 1:
        # A single contributor is already a valid digest; no re-compression, and
        # copy the channels so the return never aliases the caller.
        out = arrs[0].astype(np.float32)
        return (out, *(words[0].copy() for words in kept)) if declared else out

    combined = np.concatenate(arrs, axis=0)
    # lexsort on (mean, weight) — primary key means, secondary weights — so the
    # centroid order is intrinsic to (mean, weight) and independent of the input
    # digest order. A stable argsort on mean alone breaks ties by concatenation
    # position (i.e. input order), which makes the k-way fold permutation-
    # dependent when means tie across digests (discrete/quantized sources).
    # Sub-centroids with identical (mean, weight) are interchangeable *for the
    # digest*, but not for a companion channel: a tie straddling a compression
    # boundary would send different words into different output centroids
    # depending on input order. So each declared channel contributes a further
    # tie key, in the fixed channel order — location tertiary, toc quaternary
    # (tertiary when it is the only channel declared) — which changes no digest
    # byte (the tied rows carry identical means and weights) and makes the
    # companions permutation-independent too (issues #279/#370/#410).
    # ``lexsort`` takes its keys least-significant first, hence the reversal.
    joined = [np.concatenate(words) for words in kept]
    order = np.lexsort((*reversed(joined), combined[:, 1], combined[:, 0]))
    joined = [words[order] for words in joined]
    combined = combined[order]
    out_means, out_weights, starts = _compress(combined[:, 0], combined[:, 1], float(delta))
    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if not declared:
        return out
    n = len(combined)
    return (
        out,
        *(
            reducer(words, starts, n)
            for (_, reducer, _), words in zip(declared, joined, strict=True)
        ),
    )


def build_tdigest_pairwise(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    locations: np.ndarray | None = None,
    temporal: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, ...]:
    """t-digest build whose cross-block fold uses the **pairwise** merge law.

    Output is identical to :func:`build_tdigest` — the difference is downstream,
    in the spill reducer: ``build_tdigest_pairwise`` fields fold with a pairwise
    left-fold of :func:`merge_tdigests` (order-dependent, drifts above the δ
    budget, marginally finer far tail), whereas ``build_tdigest`` folds with the
    order-independent :func:`merge_tdigests_kway`. This reducer preserves the
    pre-#279 end-to-end fold semantics for anyone who wants them.

    The temporal channel does not escape that difference either: the two laws
    reach different centroid partitions (different counts, even), so their
    per-centroid toc vectors differ. What the join being a semilattice (spec
    §8.3) buys is the *cell-level* statement —
    :func:`zagg.stats.toc.cell_envelope` over either fold's words returns the
    same token — since the words are exact given the partition they describe,
    not independently of it.
    """
    return build_tdigest(values, delta=delta, locations=locations, temporal=temporal)


def quantile_from_tdigest(digest: np.ndarray, q: float) -> float:
    """Estimate a quantile from a t-digest centroid array.

    Uses the standard t-digest interpolation: each centroid of weight w spans
    a cumulative-count range [lower, upper] = [cum - w, cum].  The quantile
    position is mapped to a centroid boundary and interpolated linearly.

    Parameters
    ----------
    digest : ndarray, shape (k, 2)
        Centroid array (mean, weight) as returned by :func:`build_tdigest`.
    q : float
        Quantile to estimate, in [0, 1].

    Returns
    -------
    float
        Approximate quantile value.  Returns NaN if the digest is empty.
    """
    if len(digest) == 0:
        return float("nan")
    means = np.asarray(digest[:, 0], dtype=np.float64)
    weights = np.asarray(digest[:, 1], dtype=np.float64)
    n = weights.sum()
    # Cumulative count at the upper edge of each centroid.
    upper = np.cumsum(weights)
    # Target rank (0-indexed, 0 = first obs, n-1 = last obs).
    target = q * (n - 1)
    # Find which centroid contains the target rank.
    # Each centroid at index k spans rank range [upper[k-1], upper[k]-1].
    for k in range(len(means)):
        lo = 0.0 if k == 0 else upper[k - 1]
        hi = upper[k] - 1.0
        if target <= hi:
            if hi <= lo:
                return float(means[k])
            frac = (target - lo) / (hi - lo)
            if k == 0:
                # Interpolate between min and current centroid mean.
                lo_val = means[0]
                hi_val = means[0] if len(means) == 1 else (means[0] + means[1]) / 2.0
            else:
                lo_val = (means[k - 1] + means[k]) / 2.0
                hi_val = means[k] if k == len(means) - 1 else (means[k] + means[k + 1]) / 2.0
            return float(lo_val + frac * (hi_val - lo_val))
    return float(means[-1])


def cdf_from_tdigest(digest: np.ndarray, x: float | np.ndarray) -> float | np.ndarray:
    """Estimate cumulative weight at value ``x`` from a t-digest.

    The value→cumulative-weight inverse of :func:`quantile_from_tdigest`: where
    that maps a rank fraction to a value, this maps a value to the cumulative
    *weight* (number of observations) at or below ``x``.  It is the primitive
    needed to fill evenly-spaced *value* bins from a digest (issue #79).

    Each centroid ``k`` (mean ``m_k``, weight ``w_k``) is placed at the centre
    of its cumulative-weight span, i.e. at cumulative weight
    ``cum_before + w_k / 2``.  The CDF interpolates cumulative weight linearly
    in value-space between adjacent centroid means and is clamped flat outside
    ``[m_0, m_{k-1}]``, so it is monotonic non-decreasing in ``x`` with
    endpoints ``0`` (below the first mean) and the total weight (above the
    last).

    Parameters
    ----------
    digest : ndarray, shape (k, 2)
        Centroid array (mean, weight) as returned by :func:`build_tdigest`.
    x : float or ndarray
        Value(s) at which to evaluate the cumulative weight.

    Returns
    -------
    float or ndarray
        Cumulative weight at ``x``, in ``[0, total_weight]``.  Returns NaN
        (matching the shape of ``x``) for an empty digest.

    Notes
    -----
    Returns a scalar ``float`` for scalar ``x`` and an ``ndarray`` (float64)
    for array ``x``.
    """
    x_arr = np.asarray(x, dtype=np.float64)
    scalar = x_arr.ndim == 0

    if len(digest) == 0:
        out = np.full(x_arr.shape, np.nan, dtype=np.float64)
        return float(out) if scalar else out

    means = np.asarray(digest[:, 0], dtype=np.float64)
    weights = np.asarray(digest[:, 1], dtype=np.float64)
    total = float(weights.sum())

    # Cumulative weight at each centroid's centre: cum_before + w/2.
    cum_upper = np.cumsum(weights)
    cum_center = cum_upper - weights / 2.0

    # Single centroid (or all-equal means): step from 0 to total at the mean.
    if len(means) == 1:
        out = np.where(x_arr >= means[0], total, 0.0)
        return float(out) if scalar else out.astype(np.float64)

    # Piecewise-linear interpolation of cumulative weight over centroid means.
    # np.interp clamps to the endpoint values outside [means[0], means[-1]],
    # giving the flat 0 / total tails.
    out = np.interp(x_arr, means, cum_center, left=0.0, right=total)
    return float(out) if scalar else out
