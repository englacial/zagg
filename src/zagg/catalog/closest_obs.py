"""Closest-observation ingest builder: cover-driven epochs -> paired shard map.

Issue #509 — the consumer of the #489/#507 ``coverage.toc`` surface. One
Sentinel-2 (or any raster) store serves several point-cloud reference stores
(ATL03 + GEDI): for every reference *epoch* a shard's covers claim, the
builder selects the single **nearest** acquisition from the raster catalog —
closest observation, not a two-sided bracket; multiple passes bracket
naturally (espg design ruling, 2026-08-23/24).

Epochs are **store-derived**: each reference store's ``coverage.toc`` sibling
(spec §10.5) carries per-shard word-set covers quantized at order 18
(2^45 ns ≈ 9.77 h buckets). A cover *word* is a maximal RUN of those buckets
(``toc_normalize`` coalesces ranges that merely abut), so the epochs are the
midpoints of a word's **constituent buckets**, one per bucket — each within
±4.9 h of every instant its bucket covers, against Sentinel-2's ~4.3-day
revisit. Granule catalogs are *not* an
epoch source — the leaf sub-maps record the dispatched assignment verbatim
and inherit the CMR-hull over-assignment (~70 assigned vs 49 contributing
pass-days on shard ``3231422244``-class cases); covers reflect only data
that landed.

The pairing is a property of the ingest *query*, not the store schema: the
raster store stays a plain raster store, which granules were ingested **is**
the pairing, and coincidence at read time is toc intersection.

Word semantics (bit layout, decode, midpoints) are mortie's; the §10.5 cover
accessors are :mod:`zagg.coverage_toc`'s. This module owns only the join:
covers -> epochs, epochs -> nearest acquisitions, and the resulting
:class:`~zagg.catalog.shardmap.ShardMap`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np

from zagg.coverage_toc import TEMPORAL_COVER_ORDER

logger = logging.getLogger(__name__)


def _word_midpoints(words: np.ndarray, order: int = TEMPORAL_COVER_ORDER) -> np.ndarray:
    """Cover words -> UTC ``datetime64[ns]`` midpoints of their *buckets*.

    A cover word is not one bucket. :func:`zagg.coverage_toc.quantize_words`
    widens each instant to an aligned order-``order`` bucket and then
    canonicalizes with ``toc_normalize``, which "coalesces ranges that merely
    abut" (§10.5) — so a word is a maximal RUN of contiguous buckets and its
    envelope midpoint would name one epoch per *campaign*, not per pass. This
    expands every word back into its constituent buckets and emits one
    midpoint each, which is what restores the ±4.9 h bound: a bucket spans
    ``2**(63 - order)`` ns, so its midpoint is within half a bucket of every
    instant the bucket covers.

    ``toc2time`` decodes a word's conservative envelope ``(start, end)`` on
    the internal-ns scale — ``end`` exclusive for a range, ``end == start``
    for an exact timestamp — so the last covered instant is
    ``max(end, start + 1) - 1``, the same uniform rule ``quantize_words``
    applies, and the covered buckets are ``start >> k`` through ``last >> k``
    inclusive with ``k = 63 - order``. Bucket midpoints use that same rule
    within the bucket: ``(b << k) + 2**(k - 1) - 1``.

    One pass that straddles a bucket edge (its word's envelope reaches into
    the neighbouring bucket) yields **two** epochs rather than one. That is
    benign over-selection, not error: both epochs sit within half a bucket of
    the pass, both pick the same nearest acquisition, and the builder dedupes
    granule ids per shard.

    ``order`` is the block's *effective* temporal order (§10.5 lets a block
    coarsen below the object's pin), defaulting to
    :data:`zagg.coverage_toc.TEMPORAL_COVER_ORDER`.
    """
    import mortie

    words = np.asarray(words, dtype=np.uint64)
    if words.size == 0:
        return np.empty(0, dtype="datetime64[ns]")
    k = int(63 - int(order))
    start, end = mortie.toc2time(words)
    start = np.atleast_1d(np.asarray(start, dtype=np.uint64))
    end = np.atleast_1d(np.asarray(end, dtype=np.uint64))
    last = np.maximum(end, start + np.uint64(1)) - np.uint64(1)
    # Bucket index run [b0, b1] per word; expanded with repeat/arange
    # arithmetic rather than a Python loop over words.
    b0 = (start >> np.uint64(k)).astype(np.int64)
    b1 = (last >> np.uint64(k)).astype(np.int64)
    counts = b1 - b0 + 1
    offsets = np.cumsum(counts) - counts
    within = np.arange(int(counts.sum()), dtype=np.int64) - np.repeat(offsets, counts)
    buckets = np.unique(np.repeat(b0, counts) + within).astype(np.uint64)
    half = np.uint64((1 << k) // 2 - 1)
    mid = np.minimum((buckets << np.uint64(k)) + half, np.uint64(mortie.TOC_MAX_NS))
    return np.asarray(mortie.to_datetime64(mid), dtype="datetime64[ns]")


def _aoi_shard_set(aoi, order: int) -> set[int] | None:
    """Resolve an ``aoi`` argument to the set of shard keys it covers.

    ``None`` passes through (no restriction). Accepted forms, matching the
    catalog layer's existing vocabulary:

    - ``mortie.Moc`` — cast to the flat cell list at ``order``;
    - ``str`` — a GeoJSON path (:func:`zagg.catalog.load_polygon`);
    - ``[(lats, lons), ...]`` ring parts — the ``coverage``/``region`` form.
    """
    if aoi is None:
        return None
    import mortie

    if isinstance(aoi, mortie.Moc):
        return {int(c) for c in aoi.to_order(order)}
    if isinstance(aoi, str):
        from zagg.catalog import load_polygon

        aoi = load_polygon(aoi)
    from zagg.grids.aoi import healpix_aoi_moc

    moc = healpix_aoi_moc(aoi, order)
    return {int(c) for c in mortie.moc_to_order(moc, order)}


def _shard_order(cover: dict, root: str) -> int:
    """A cover object's declared shard (dispatch) ``order``, or a loud refusal.

    §10.5 makes ``order`` mandatory; a body missing it (or carrying a
    non-integer) is debris this builder cannot key shards from, and the bare
    ``int(...)`` it used to get raised an opaque ``TypeError`` naming nothing.
    """
    value = cover.get("order")
    try:
        return int(cover["order"])
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(
            f"reference_epochs: store {root!r} declares a non-integer cover shard order "
            f"{value!r} — §10.5 requires it, and shard keys are parsed against it"
        ) from e


@dataclass
class ReferenceEpochs:
    """Per-shard reference epochs decoded from one or more store covers.

    Attributes
    ----------
    order : int
        The covers' shard (dispatch) order — every contributing store must
        agree on it, and the raster grid the epochs pair against must match.
    epochs : dict of int -> np.ndarray
        Shard key (packed morton word, the canonical in-memory form D1
        renders as a decimal string externally) -> sorted unique UTC
        ``datetime64[ns]`` epoch midpoints, the union across the contributing
        stores' covers. Shards whose cover block decodes to an empty word set
        are omitted (they claim nothing).
    stores : list of str
        The store roots that contributed, in the order given (provenance).
    orders : dict of int -> int
        Shard key -> the **coarsest** temporal order that contributed to that
        shard's epochs. Normally the pinned
        :data:`~zagg.coverage_toc.TEMPORAL_COVER_ORDER`, but §10.5 lets a
        block coarsen below the pin to fit the cover cap, and a coarser order
        means a wider midpoint bound (``2**(62 - order)`` ns). Phase 2's
        ``max_time_offset`` reasoning reads this rather than assuming the pin.
    """

    order: int
    epochs: dict[int, np.ndarray]
    stores: list[str] = field(default_factory=list)
    orders: dict[int, int] = field(default_factory=dict)
    epoch_orders: dict[int, np.ndarray] = field(default_factory=dict)
    """Shard key -> int64 array row-aligned with ``epochs``: each epoch's own
    effective temporal order (espg tolerance ruling, 2026-08-24). :attr:`orders`
    is the per-shard COARSEST — right for a headline warning, too blunt for a
    precision gate: one shard can mix a pinned store's epochs with a coarsened
    store's, and only the coarse ones fail a stated ``max_time_offset``. A
    midpoint claimed at two orders keeps the finest."""

    @property
    def total(self) -> int:
        """Total epoch count across shards (post-union, post-dedupe)."""
        return sum(e.size for e in self.epochs.values())

    def tolerance(self, shard: int) -> np.timedelta64:
        """Half a bucket at ``shard``'s effective order — its epoch bound."""
        order = self.orders.get(shard, TEMPORAL_COVER_ORDER)
        return np.timedelta64(2 ** (62 - int(order)), "ns")


def reference_epochs(reference_stores, *, aoi=None, **store_kwargs) -> ReferenceEpochs:
    """Per-shard epochs from the reference stores' ``coverage.toc`` covers.

    For each store root: fetch the §10.5 sibling
    (:func:`zagg.coverage_toc.read_cover`), strict-decode its per-shard word
    sets (:func:`zagg.coverage_toc.cover_words`), and expand each word into
    its constituent buckets' midpoints (:func:`_word_midpoints`). Per shard
    the result is the **union** across stores, deduplicated (espg ruling: one
    raster store serves both sensors, epochs are the union across the
    reference stores).

    The union is canonical at the **bucket** level, not the word level. Words
    are post-``toc_normalize`` runs whose extent depends on that store's
    *other* data, so two stores sharing one pass routinely emit
    overlapping-but-**unequal** range words — a raw ``np.unique`` over words
    would keep both and represent the shared pass twice, at two displaced
    midpoints. Expanding to buckets first removes that degree of freedom:
    the bucket grid is fixed by the order alone, so a shared pass contributes
    the same bucket midpoint from every store and dedupes exactly.

    §10.5 lets a block coarsen **below** the object's pinned temporal order to
    fit the cover cap, and its landed order is recorded in the block itself.
    :func:`zagg.coverage_toc.cover_words` returns only the words, so this
    reads each block's ``temporal_order`` straight from the grammar, expands
    that block's words at **its** order, logs a warning whenever a
    block sits below the pin (the read half of §10.5's "widening only,
    *loudly recorded*"), and reports the coarsest contributing order per
    shard on :attr:`ReferenceEpochs.orders`.

    A store that carries **no readable cover refuses loudly** — this builder
    is cover-driven by design (store-derived epochs, never the granule
    catalogs), so "no cover yet" means "sweep the store first", not "fall
    back silently". Likewise a shard-order mismatch between stores: D1 ids
    at two orders are not comparable.

    Parameters
    ----------
    reference_stores : str or sequence of str
        Store roots whose covers drive the epochs (e.g. ATL03 + GEDI).
    aoi : optional
        Restrict the shard set: a ``mortie.Moc``, a GeoJSON path, or
        ``[(lats, lons), ...]`` ring parts (see :func:`_aoi_shard_set`).
    **store_kwargs
        Forwarded to the object-store open (region, credentials, ...).

    Returns
    -------
    ReferenceEpochs
    """
    from zagg.coverage_toc import COVER_NAME, cover_words, load_cover, read_cover
    from zagg.grids.morton import morton_word

    if isinstance(reference_stores, str):
        reference_stores = [reference_stores]
    reference_stores = list(reference_stores)
    if not reference_stores:
        raise ValueError("reference_epochs: at least one reference store root is required")

    order: int | None = None
    mids_by_shard: dict[int, list[tuple[np.ndarray, int]]] = {}
    orders: dict[int, int] = {}
    for root in reference_stores:
        obj = read_cover(root, **store_kwargs)
        cover = load_cover(obj)
        if cover is None:
            detail = (
                "no coverage.toc object"
                if obj is None
                else f"unreadable {COVER_NAME} (spec {obj.get('spec') if isinstance(obj, dict) else obj!r})"
            )
            raise ValueError(
                f"reference_epochs: store {root!r} has {detail} — epochs are cover-driven "
                f"(spec §10.5, issue #509); run the rollup sweep that materializes the "
                f"cover before pairing against this store"
            )
        store_order = _shard_order(cover, root)
        if order is None:
            order = store_order
        elif store_order != order:
            raise ValueError(
                f"reference_epochs: store {root!r} covers shard order {store_order}, "
                f"previous stores cover order {order} — D1 ids at two orders are not "
                f"comparable (spec §10.5)"
            )
        # cover_words strict-decodes (and validates the object's pin); the
        # per-block order it drops is read back off the same grammar.
        decoded = cover_words(obj) or {}
        pinned = int(cover.get("temporal_order", TEMPORAL_COVER_ORDER))
        blocks = cover.get("shards") or {}
        for decimal, words in decoded.items():
            if not len(words):
                continue
            block = blocks.get(decimal) or {}
            raw = block.get("temporal_order", pinned)
            # Boundary REFUSAL, not a clip. §10.5's order check in
            # ``coverage_toc._decode_cover_block`` is one-sided (ceiling only),
            # so a corrupt block declaring a negative order decodes fine — and
            # then ``62 - order`` overflows the int64 shift to a huge NEGATIVE
            # half-span that silently PASSES the caller's precision bar, on
            # midpoints ``_word_midpoints`` computed from a shift it cannot
            # express either. Refuse at this read boundary; the durable
            # one-line lower bound belongs beside that ceiling check.
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise ValueError(
                    f"reference_epochs: store {root!r} shard {decimal} cover block declares "
                    f"temporal_order {raw!r} — a block only ever coarsens BELOW the object's "
                    f"pin and never below 0 (spec §10.5); this block is corrupt, and decoding "
                    f"it would yield epochs no precision bar can hold"
                )
            effective = int(raw)
            if effective < TEMPORAL_COVER_ORDER:
                logger.warning(
                    f"reference_epochs: store {root!r} shard {decimal} cover sits at "
                    f"temporal order {effective}, below the pinned {TEMPORAL_COVER_ORDER} "
                    f"(§10.5 cap coarsening) — its buckets span 2^{63 - effective} ns, so "
                    f"these epochs are good to ±2^{62 - effective} ns, not ±4.9 h"
                )
            # Cover blocks are keyed by the D1 decimal id (the external
            # string form, sign included); shard maps key on the packed
            # morton word — parse at the boundary (issue #199).
            shard = morton_word(decimal)
            mids_by_shard.setdefault(shard, []).append(
                (_word_midpoints(words, effective), effective)
            )
            orders[shard] = min(orders.get(shard, effective), effective)

    assert order is not None  # non-empty store list, every cover carried an order
    keep = _aoi_shard_set(aoi, order)
    epochs: dict[int, np.ndarray] = {}
    epoch_orders: dict[int, np.ndarray] = {}
    for shard in sorted(mids_by_shard):
        if keep is not None and shard not in keep:
            continue
        parts = mids_by_shard[shard]
        mids = np.concatenate([m for m, _ in parts])
        ords = np.concatenate([np.full(m.size, o, dtype=np.int64) for m, o in parts])
        # Dedupe exact midpoints keeping each epoch's FINEST claiming order
        # (the precision gate reads per-epoch orders): sort by (mid, -order)
        # so the first row of every equal-midpoint run is the finest.
        ix = np.lexsort((-ords, mids.astype("int64")))
        mids, ords = mids[ix], ords[ix]
        first = np.ones(mids.size, dtype=bool)
        first[1:] = mids[1:] != mids[:-1]
        mids, ords = mids[first], ords[first]
        if mids.size:
            epochs[shard] = mids
            epoch_orders[shard] = ords
    return ReferenceEpochs(
        order,
        epochs,
        reference_stores,
        {k: v for k, v in orders.items() if k in epochs},
        epoch_orders,
    )


def _cap_ns(max_time_offset) -> int | None:
    """``max_time_offset`` as validated non-negative nanoseconds, or ``None``.

    One conversion for the selection gate (:func:`nearest_acquisitions`) and
    the builder's cover-resolution gate, so both refuse the same inputs with
    the same message: NaT, a duration that does not round-trip through
    ``timedelta64[ns]`` (~292-year span), and a negative cap.
    """
    if max_time_offset is None:
        return None
    offset = np.timedelta64(max_time_offset)
    if np.isnat(offset):
        raise ValueError(f"max_time_offset must be a real duration (got {max_time_offset!r})")
    as_ns = offset.astype("timedelta64[ns]")
    if as_ns.astype(offset.dtype) != offset:
        raise ValueError(
            "max_time_offset does not convert exactly to nanoseconds "
            f"(got {max_time_offset!r}; timedelta64[ns] spans ~292 years)"
        )
    cap = int(as_ns.astype("int64"))
    if cap < 0:
        raise ValueError(f"max_time_offset must be non-negative (got {max_time_offset!r})")
    return cap


def nearest_acquisitions(epochs, times, *, max_time_offset=None):
    """Nearest acquisition per epoch — the vectorized closest-1 core.

    The phase-2 selection of the closest-observation join (issue #509): for
    each reference epoch, the single nearest acquisition — closest-1, never a
    two-sided bracket (espg ruling); several epochs bracketing one
    acquisition each select it, and the builder dedupes granules downstream.

    Parameters
    ----------
    epochs : array-like of datetime64[ns]
        One shard's reference epochs (:func:`reference_epochs`). Any order.
        ``NaT`` refuses — see the note below.
    times : array-like of datetime64[ns]
        The shard's acquisition times, in catalog record order. Any order —
        the returned selection indexes THIS array's positions. ``NaT``
        refuses — see the note below.
    max_time_offset : np.timedelta64 or int, optional
        An epoch whose nearest acquisition lies further than this selects
        nothing. Exactly-at selects; one ns past does not. A bare int means
        nanoseconds. ``None`` (default) always selects the nearest, however
        far. Negative refuses, and so does a duration that will not convert
        to nanoseconds (``np.timedelta64(1000, "Y")`` overflows int64 ns). Callers gating against the epochs' own
        precision should widen by :meth:`ReferenceEpochs.tolerance` — a
        cover epoch is a bucket midpoint, good to half a bucket, not exact.

    Returns
    -------
    selection : np.ndarray of int64
        Per epoch, the index into ``times`` of the selected acquisition, or
        ``-1`` where the epoch selects nothing (no acquisitions at all, or
        nearest beyond ``max_time_offset``).
    offsets : np.ndarray of timedelta64[ns]
        Per epoch, the SIGNED offset ``times[nearest] - epoch`` of the
        nearest acquisition — positive when the acquisition follows the
        epoch. Reported for every epoch, dropped ones included (the loud
        record a drop rides — the builder's report needs the near-miss
        distance, not just the fact of the drop). ``NaT`` when ``times`` is
        empty, and for the offset that will not fit ``timedelta64[ns]`` —
        selection is exact over the whole ``datetime64[ns]`` span (~584
        years), but a *difference* past ~292 years is unrepresentable, so it
        saturates to ``NaT`` rather than reporting a wrapped duration. Such
        an epoch selects nothing under any cap (no finite tolerance reaches
        it) and still selects its nearest with ``max_time_offset=None``.

    Raises
    ------
    ValueError
        If either input carries ``NaT``. A missing instant has no nearest
        anything: ``NaT`` sorts last but casts to ``iinfo(int64).min``, so it
        would leave ``ts`` unsorted (silently mis-pairing its neighbours) and
        wrap the offset subtraction. :func:`reference_epochs` never emits
        ``NaT`` and the catalog's time parser refuses a missing acquisition
        time, so ``NaT`` here is caller debris — refused loudly, this
        module's posture.

    Notes
    -----
    A tie — an epoch exactly equidistant between two acquisitions — selects
    the EARLIER acquisition, deterministically. Equal acquisition times (one
    Sentinel-2 datatake stamps many granules with the same instant) are
    broken by catalog record order: the FIRST record of the equal-time run,
    whichever flank the epoch approaches it from.
    """
    epochs = np.asarray(epochs, dtype="datetime64[ns]")
    times = np.asarray(times, dtype="datetime64[ns]")
    for name, arr in (("epochs", epochs), ("times", times)):
        if np.isnat(arr).any():
            raise ValueError(f"{name} carries NaT ({int(np.isnat(arr).sum())} of {arr.size})")
    cap = _cap_ns(max_time_offset)
    selection = np.full(epochs.shape, -1, dtype=np.int64)
    offsets = np.full(epochs.shape, np.timedelta64("NaT"), dtype="timedelta64[ns]")
    if epochs.size == 0 or times.size == 0:
        return selection, offsets
    order = np.argsort(times, kind="stable")
    ts = times[order].astype("int64")
    e = epochs.astype("int64")
    pos = np.searchsorted(ts, e)  # left insertion point
    # Flank distances as UNSIGNED magnitudes. Both are non-negative by
    # construction (``e >= ts[pos - 1]`` and ``ts[pos] >= e``), and mod-2^64
    # subtraction of the int64 bit patterns is exact across the whole
    # datetime64[ns] span — an int64 ``e - ts`` wraps silently for a pair
    # more than ~292 years apart, which then passes the cap gate.
    tsu, eu = ts.view(np.uint64), e.view(np.uint64)
    far = np.uint64(2**64 - 1)  # a missing flank, wider than any real span
    left = np.where(pos > 0, eu - tsu[np.maximum(pos - 1, 0)], far)
    right = np.where(pos < ts.size, tsu[np.minimum(pos, ts.size - 1)] - eu, far)
    # Strict ``<`` keeps a tie on the LEFT (earlier) neighbor; an epoch equal
    # to an acquisition has ``right == 0`` and selects it exactly.
    take_right = right < left
    nearest = np.where(take_right, np.minimum(pos, ts.size - 1), np.maximum(pos - 1, 0))
    # Equal acquisition times form one run in ``ts``; the left flank lands on
    # its END and the right flank on its START, so snap to the run start —
    # with a stable ``argsort`` that is the run's first catalog record, from
    # either side.
    nearest = np.searchsorted(ts, ts[nearest], side="left")
    selection = order[nearest].astype(np.int64)
    magnitude = np.where(take_right, right, left)
    # A magnitude past int64 ns is a real distance the report cannot carry:
    # saturate it to NaT rather than wrap it into a plausible-looking day.
    fits = magnitude <= np.uint64(np.iinfo(np.int64).max)
    signed = np.minimum(magnitude, np.uint64(np.iinfo(np.int64).max)).astype(np.int64)
    offsets = np.where(take_right, signed, -signed).astype("timedelta64[ns]")
    offsets = np.where(fits, offsets, np.timedelta64("NaT"))
    if cap is not None:
        selection = np.where(magnitude <= np.uint64(cap), selection, np.int64(-1))
    return selection, offsets


def _acquisition_times(entries: list[dict], shard: str) -> np.ndarray:
    """Granule entries -> UTC ``datetime64[ns]`` acquisition instants.

    Reads the entry's ``datetime`` (the raster-source acquisition instant,
    #218) falling back to ``time_start`` (the STAC acquisition-range start,
    #246). A granule carrying neither refuses loudly — a catalog without
    acquisition times cannot be temporally paired, and skipping the granule
    would silently thin the product.
    """
    from datetime import datetime, timezone

    out = np.empty(len(entries), dtype="datetime64[ns]")
    for i, entry in enumerate(entries):
        iso = entry.get("datetime") or entry.get("time_start")
        if iso is None:
            raise ValueError(
                f"closest_obs_shardmap: granule {entry.get('id')!r} in shard {shard} "
                f"carries no acquisition time (neither 'datetime' nor 'time_start') — "
                f"the catalog cannot be temporally paired (issue #509)"
            )
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        out[i] = np.datetime64(dt, "ns")
    return out


def closest_obs_shardmap(
    s2_catalog,
    reference_stores,
    *,
    grid,
    aoi=None,
    max_time_offset=None,
    max_granules_per_shard=None,
    estimate=False,
    backend="auto",
    bytes_per_granule=None,
    **store_kwargs,
):
    """Closest-observation ingest map: covers -> epochs -> nearest granules.

    The issue-#509 builder. Epochs come from the reference stores'
    ``coverage.toc`` covers (:func:`reference_epochs`); the raster catalog is
    spatially assigned to shards through the existing machinery
    (:meth:`~zagg.catalog.shardmap.ShardMap.build` — the stored-index /
    batch-cover fast paths included); per shard, each epoch then selects its
    single nearest acquisition (:func:`nearest_acquisitions`) and the
    selected granules, deduplicated, become the shard's ingest list. The
    result is a standard :class:`~zagg.catalog.shardmap.ShardMap` — JSON
    round-trip, ``total_pairs`` bookkeeping — so dispatch consumes it
    unchanged; the pairing is a property of this query, never of the raster
    store's schema (espg ruling).

    Parameters
    ----------
    s2_catalog : Catalog or str
        The raster acquisition catalog (stac-geoparquet), or a path to one
        (``Catalog.from_geoparquet``). Every record must carry an acquisition
        time (``datetime``, or STAC ``start_datetime``).
    reference_stores : str or sequence of str
        Store roots whose covers drive the epochs (e.g. ATL03 + GEDI).
    grid : HealpixGrid
        The raster store's output grid. Its ``parent_order`` must equal the
        covers' shard order — the map's shard keys and the covers' D1 ids
        live on the same grid, and the emitted ``grid_signature`` is what the
        ingest run validates against.
    aoi : optional
        Restrict the shard set (``mortie.Moc``, GeoJSON path, or ring
        parts); intersected with the store coverage (:func:`reference_epochs`).
        Ring parts / a GeoJSON path additionally ride as the spatial build's
        ``region=``, scoping the raster intersection; a ``mortie.Moc`` has no
        parts form, so it restricts the epoch side only and the spatial build
        still covers the whole catalog bbox.
    max_time_offset : np.timedelta64 or int (ns), optional
        An epoch whose nearest acquisition lies beyond this selects nothing —
        recorded per epoch in ``metadata["closest_obs"]["dropped"]`` and
        warned about, never silent. It is also a **precision bar** on the
        epochs themselves (espg tolerance ruling, 2026-08-24): an epoch from
        a cover block coarsened far enough that its bucket half-span exceeds
        this offset cannot be paired to the stated precision, and is dropped
        into the ledger as its own category (rows carrying
        ``temporal_order``/``cover_half_span_ns``; counted in
        ``epochs_dropped_low_resolution``). Half-span exactly at the offset
        stays pairable — the same side the selection gate's exactly-at rule
        pins. ``None`` always selects the nearest, at whatever resolution the
        covers offer (a single warning names the effective resolution when
        any block sits below the §10.5 pin).
    max_granules_per_shard : int, optional
        Cost gate: a shard exceeding this REFUSES loudly (``ValueError``
        naming the worst shards) — never truncates. ``estimate=True`` reports
        violations instead of raising, so the gate can be sized first.
    estimate : bool
        Dry-run: return the per-shard histogram + cost estimate dict (see
        Notes) WITHOUT building the map. The espg-operated ingest runs are
        cost-gated through this.
    backend : {"auto", "spherely", "mortie"}
        Forwarded to :meth:`ShardMap.build`.
    bytes_per_granule : int, optional
        Per-granule ingest volume for the ``estimate`` byte figure; without
        it ``est_bytes`` is ``None`` (the catalog does not carry sizes).
    **store_kwargs
        Forwarded to the reference stores' object-store opens.

    Returns
    -------
    ShardMap or dict
        The ingest map — or, with ``estimate=True``, a dict:
        ``{"shards", "granules", "pairs", "epochs_total", "epochs_paired",
        "epochs_dropped", "epochs_dropped_low_resolution", "per_shard" (decimal -> granule count),
        "histogram" (granule count -> shard count), "est_bytes",
        "max_cost_usd", "violations"}``. ``max_cost_usd`` is the
        :func:`zagg.dispatch.max_cost_usd` ceiling at the production worker
        size (one invoke per shard, 900 s timeout) — a bound, not a forecast.

    Notes
    -----
    A selected entry whose record carries no ``datetime`` (STAC's null-datetime
    + ``start_datetime`` form) is emitted with ``datetime`` backfilled from
    ``time_start`` — the same instant the pairing used, and the key raster
    dispatch requires. Selected granule entries also gain two provenance keys
    so the eventual paired product is reconstructable from the manifest alone: ``paired_epochs``
    (ISO instants of every epoch that selected the granule) and
    ``epoch_offsets_ns`` (row-aligned SIGNED ``acquisition - epoch`` ns).
    :meth:`ShardMap.reproject` carries both through every arm (issue #517), so a
    derived map keeps its per-entry provenance; because these keys are
    accumulated per shard, a coarsen unions the rows of every child a granule
    reached rather than keeping one child's. What reproject still
    cannot fix is ``metadata["closest_obs"]``, which rides through describing the
    SOURCE map — after a coarsen its shard ids name a shard set that no longer
    exists. Rebuild at the target grid rather than reprojecting whenever that
    ledger has to be true of the map carrying it.
    ``metadata["closest_obs"]`` records the query: the reference stores, the
    epoch totals, every dropped epoch with its near-miss offset (``None``
    where there was no acquisition to measure against, so
    ``epochs_total == epochs_paired + epochs_dropped`` holds), shards
    whose epochs found no acquisition at all, and any cover blocks coarsened
    below the §10.5 pin. Epochs are bucket midpoints, good to half a bucket
    (:meth:`ReferenceEpochs.tolerance`) — size ``max_time_offset`` with that
    slack in mind.

    The strict-AOI per-shard mask (``output.aoi_mask``, issue #101) is the
    SPATIAL map's payload and is not carried onto the emitted map, so the
    ``aoi_mask`` metadata claim is dropped with it: an ``output.aoi_mask``
    ingest run must compute its mask at run time rather than read one off
    this manifest.
    """
    from zagg.catalog.shardmap import ShardMap
    from zagg.grids.morton import morton_decimal

    if isinstance(s2_catalog, str):
        from zagg.catalog.sources import Catalog

        s2_catalog = Catalog.from_geoparquet(s2_catalog)

    parent_order = getattr(grid, "parent_order", None)
    if parent_order is None:
        raise ValueError(
            "closest_obs_shardmap: grid must be a HEALPix grid (parent_order) — the "
            "covers' D1 shard ids and the map's shard keys live on the same grid"
        )
    # Resolve the aoi once. Ring parts (and a GeoJSON path, which loads to
    # parts) also scope the SPATIAL build via ``region=``, so an AOI run stops
    # intersecting the whole catalog bbox and discarding the outside shards
    # afterwards; a ``mortie.Moc`` has no ring-parts form and stays epoch-side.
    import mortie

    if isinstance(aoi, str):
        from zagg.catalog import load_polygon

        aoi = load_polygon(aoi)
    region = None if aoi is None or isinstance(aoi, mortie.Moc) else aoi
    ref = reference_epochs(reference_stores, aoi=aoi, **store_kwargs)
    if int(parent_order) != int(ref.order):
        raise ValueError(
            f"closest_obs_shardmap: grid.parent_order {int(parent_order)} != the covers' "
            f"shard order {ref.order} — the emitted map would key shards on a different "
            f"grid than the epochs (spec §10.5)"
        )

    cap_ns = _cap_ns(max_time_offset)
    spatial = ShardMap.build(s2_catalog, grid, region=region, backend=backend)
    spatial_idx = {int(k): i for i, k in enumerate(spatial.shard_keys)}
    aoi_keys = _aoi_shard_set(aoi, ref.order)

    shard_keys: list[int] = []
    granules: list[list[dict]] = []
    dropped: list[dict] = []
    no_acquisitions: list[str] = []
    epochs_paired = 0
    low_resolution = 0
    for shard, epoch_arr in sorted(ref.epochs.items()):
        decimal = morton_decimal(shard)
        # Cover-resolution gate (espg tolerance ruling, 2026-08-24, thread
        # r3845481805): an epoch whose bucket HALF-SPAN exceeds the caller's
        # stated ``max_time_offset`` cannot be paired to that precision — the
        # true pass sits anywhere inside the bucket, so any pairing within the
        # cap would be arbitrary. Dropped loudly, per epoch, as its own ledger
        # category (``temporal_order``/``cover_half_span_ns`` rows), BEFORE the
        # nearest selection. Strictly greater: half-span exactly at the cap
        # stays pairable, the same side the selection gate's exactly-at rule
        # pins. With no cap the caller declared no precision bar — the build
        # warns once (below) and proceeds; widening is lawful (§10.5).
        #
        # It runs AHEAD of the spatial lookup on purpose: unresolvability is a
        # property of the epoch and its own cover block, so which category an
        # epoch lands in must not flip on whether the raster catalog happens to
        # reach the shard. Gating after the lookup made an unreached shard's
        # coarse epochs ledger as no-acquisition rows and read
        # ``epochs_dropped_low_resolution == 0``.
        eo = ref.epoch_orders.get(shard)
        if cap_ns is not None and eo is not None:
            half_span = np.int64(1) << (np.int64(62) - eo)
            unresolvable = half_span > cap_ns
            if unresolvable.any():
                dropped.extend(
                    {
                        "shard": decimal,
                        "epoch": np.datetime_as_string(t),
                        "temporal_order": int(o),
                        "cover_half_span_ns": int(h),
                    }
                    for t, o, h in zip(
                        epoch_arr[unresolvable], eo[unresolvable], half_span[unresolvable]
                    )
                )
                low_resolution += int(unresolvable.sum())
                epoch_arr = epoch_arr[~unresolvable]
        i = spatial_idx.get(shard)
        if i is None:
            no_acquisitions.append(decimal)
            # The shard is named even when the gate took every epoch: this row
            # is about the SHARD, not its epochs. Ledger the SURVIVING epochs
            # too — a shard the catalog never reaches is the largest drop class
            # in practice, and leaving it out of ``dropped`` made the numbers an
            # operator reconciles read "nothing dropped". ``nearest_offset_ns``
            # is None -- no acquisition to measure against, the meaning the key
            # already carries.
            dropped.extend(
                {"shard": decimal, "epoch": np.datetime_as_string(t), "nearest_offset_ns": None}
                for t in epoch_arr
            )
            continue
        if epoch_arr.size == 0:
            continue
        entries = spatial.granules[i]
        times = _acquisition_times(entries, decimal)
        sel, off = nearest_acquisitions(epoch_arr, times, max_time_offset=max_time_offset)
        off_ns = off.astype("int64")
        chosen: dict[int, dict] = {}
        for j in range(epoch_arr.size):
            iso = np.datetime_as_string(epoch_arr[j])
            if sel[j] < 0:
                dropped.append(
                    {
                        "shard": decimal,
                        "epoch": iso,
                        "nearest_offset_ns": None if np.isnat(off[j]) else int(off_ns[j]),
                    }
                )
                continue
            src = entries[sel[j]]
            entry = chosen.setdefault(
                int(sel[j]),
                {
                    **src,
                    # STAC allows ``datetime: null`` beside start/end_datetime,
                    # but raster dispatch keys off ``datetime``
                    # (``runner._raster_windowed_units``). Emit the instant the
                    # pairing actually used so the map dispatches as built.
                    "datetime": src.get("datetime") or src.get("time_start"),
                    "paired_epochs": [],
                    "epoch_offsets_ns": [],
                },
            )
            entry["paired_epochs"].append(iso)
            entry["epoch_offsets_ns"].append(None if np.isnat(off[j]) else int(off_ns[j]))
            epochs_paired += 1
        if chosen:
            shard_keys.append(shard)
            granules.append([chosen[j] for j in sorted(chosen)])

    coarse = {morton_decimal(k): o for k, o in ref.orders.items() if o < TEMPORAL_COVER_ORDER}
    if dropped:
        logger.warning(
            f"closest_obs_shardmap: {len(dropped)} epoch(s) selected nothing "
            f"(max_time_offset={max_time_offset!r}, no acquisitions in the shard, or a "
            f"cover block too coarse for the offset); "
            f"e.g. {dropped[:3]} — every drop is "
            f"recorded in metadata['closest_obs']['dropped']"
        )
    if low_resolution:
        # The arm that DISCARDS epochs must be at least as loud as the no-cap
        # arm below, which discards nothing and still gets a purpose-built
        # line. The summary above cannot carry this: those rows never reached
        # the selection, so its distance/catalog-gap causes are both wrong for
        # them, and ``reference_epochs``' coarsening warning says only that the
        # epochs are coarse — never that they were consequently dropped.
        worst = min(coarse.values()) if coarse else None
        logger.warning(
            f"closest_obs_shardmap: {low_resolution} epoch(s) dropped as UNRESOLVABLE at "
            f"max_time_offset={max_time_offset!r} — a coarsened cover, NOT distance to an "
            f"acquisition: their block's bucket half-span exceeds the offset (coarsest "
            f"temporal order {worst} across {len(coarse)} shard(s), e.g. "
            f"{sorted(coarse)[:5]}) — every row is in metadata['closest_obs']['dropped'] "
            f"with temporal_order/cover_half_span_ns, counted in "
            f"metadata['closest_obs']['epochs_dropped_low_resolution']"
        )
    if no_acquisitions:
        logger.warning(
            f"closest_obs_shardmap: {len(no_acquisitions)} shard(s) carry reference epochs "
            f"but NO spatially-assigned acquisitions at all (catalog gap?): "
            f"{no_acquisitions[:5]}"
        )

    counts = {morton_decimal(k): len(g) for k, g in zip(shard_keys, granules)}
    violations = (
        sorted(
            ((d, n) for d, n in counts.items() if n > max_granules_per_shard),
            key=lambda kv: -kv[1],
        )
        if max_granules_per_shard is not None
        else []
    )

    if coarse and cap_ns is None:
        worst = min(coarse.values())
        logger.warning(
            f"closest_obs_shardmap: cover blocks in {len(coarse)} shard(s) sit below the "
            f"§10.5 pin (coarsest temporal order {worst}); effective epoch resolution is "
            f"±2^{62 - worst} ns (~{2 ** (62 - worst) / 3.6e12:.1f} h) — no max_time_offset "
            f"was set, so pairing proceeds at that resolution"
        )
    closest_meta = {
        "reference_stores": list(ref.stores),
        "shard_order": int(ref.order),
        "max_time_offset_ns": cap_ns,
        "epochs_total": int(ref.total),
        "epochs_paired": int(epochs_paired),
        "epochs_dropped": len(dropped),
        # The cover-resolution category's own count (espg tolerance ruling):
        # these rows are inside ``dropped`` — the ledger invariant stays
        # ``epochs_total == epochs_paired + epochs_dropped`` — but an operator
        # sizing max_time_offset needs them told apart from near-misses.
        "epochs_dropped_low_resolution": int(low_resolution),
        "dropped": dropped,
        "shards_without_acquisitions": no_acquisitions,
        # Coverage disagreement, not self-inflicted clipping: ``ref.epochs`` is
        # AOI-filtered, so an AOI-excluded shard is not a shard the reference
        # stores never observed.
        "spatial_shards_without_epochs": sum(
            1
            for k in spatial.shard_keys
            if k not in ref.epochs and (aoi_keys is None or k in aoi_keys)
        ),
        "coarsened_orders": coarse,
    }

    if estimate:
        from zagg.dispatch import LAMBDA_MEMORY_GB, max_cost_usd

        histogram: dict[int, int] = {}
        for n in counts.values():
            histogram[n] = histogram.get(n, 0) + 1
        distinct = len({g["id"] for shard in granules for g in shard})
        pairs = sum(len(g) for g in granules)
        return {
            **closest_meta,
            "shards": len(shard_keys),
            "granules": distinct,
            "pairs": pairs,
            "per_shard": counts,
            "histogram": dict(sorted(histogram.items())),
            "est_bytes": None if bytes_per_granule is None else pairs * int(bytes_per_granule),
            "max_cost_usd": round(
                max_cost_usd(len(shard_keys), LAMBDA_MEMORY_GB, timeout_s=900.0), 2
            ),
            "violations": violations,
        }

    if violations:
        worst = ", ".join(f"{d}={n}" for d, n in violations[:5])
        raise ValueError(
            f"closest_obs_shardmap: {len(violations)} shard(s) exceed "
            f"max_granules_per_shard={max_granules_per_shard} (worst: {worst}) — refusing "
            f"loudly rather than truncating; raise the gate, tighten max_time_offset/aoi, "
            f"or size the run with estimate=True first (issue #509)"
        )

    meta = {
        **spatial.metadata,
        "total_shards": len(shard_keys),
        "total_pairs": sum(len(g) for g in granules),
        "granules_assigned": len({g["id"] for shard in granules for g in shard}),
        "closest_obs": closest_meta,
    }
    # The spatial build's per-shard strict-AOI mask is NOT carried onto this
    # derived map (``aoi_mask=None`` below), so the metadata must stop
    # advertising one -- the same guard ``reproject`` applies to a derived map
    # (``shardmap.py:1738``).
    meta.pop("aoi_mask", None)
    return ShardMap(spatial.grid_signature, shard_keys, granules, meta, None)


__all__ = [
    "ReferenceEpochs",
    "closest_obs_shardmap",
    "nearest_acquisitions",
    "reference_epochs",
]
