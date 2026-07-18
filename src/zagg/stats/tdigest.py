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
"""

from __future__ import annotations

import numpy as np

__all__ = [
    "build_tdigest",
    "cdf_from_tdigest",
    "merge_tdigests",
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

    The centroid **boundaries and weights are bit-identical** to the old scalar
    loop (same k1 values at the same rank fractions, same ``span ≤ 1`` rule);
    only the means differ, at the float-ULP level, because they are formed as a
    single weighted ``sum / weight`` rather than an incremental Welford update.
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


def _centroid_ancestors(locations: np.ndarray, starts: list[int], n: int) -> np.ndarray:
    """Reduce per-member morton locations to one enclosing cell per centroid.

    ``locations`` is member-ordered (aligned with the sorted values / combined
    centroids), ``starts`` the first member index of each centroid.  Each
    centroid's location is ``mortie.common_ancestor`` over its members' words —
    the deepest cell containing all of them (issue #87).  Mixed-order input is
    fine (a below-order-29 mean-morton from a prior merge folds with fresh
    order-29 points), and a single member returns itself with its point kind
    preserved, so a 1-obs centroid round-trips its exact order-29 point word.
    """
    from mortie import common_ancestor

    bounds = [*starts, n]
    out = np.empty(len(starts), dtype=np.uint64)
    for j in range(len(starts)):
        out[j] = common_ancestor(locations[bounds[j] : bounds[j + 1]])
    return out


def build_tdigest(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    locations: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, np.ndarray]:
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

    Returns
    -------
    ndarray, shape (k, 2), dtype float32
        Sorted centroid array.  Column 0 is centroid mean; column 1 is weight
        (number of observations merged into that centroid).
        With ``locations``, returns a ``(digest, locs)`` tuple instead, where
        ``locs`` is the ``(k,)`` uint64 per-centroid location vector.

    Notes
    -----
    Returns an empty ``(0, 2)`` array when ``values`` is empty or all-NaN
    (an empty ``(digest, locs)`` pair with ``locations``).  The digest itself
    is identical with or without ``locations``.
    """
    values = np.asarray(values, dtype=np.float64)
    finite = np.isfinite(values)
    if locations is not None:
        locations = np.asarray(locations)
        if locations.dtype != np.uint64:
            # A silent uint64 cast would truncate a float column (e.g. a
            # mis-declared ``location:``) into garbage morton words — require
            # packed uint64 words outright (what ``assign`` supplies as leaf_id).
            raise ValueError(
                f"locations dtype {locations.dtype} is not uint64; pass packed "
                f"morton point words (the per-observation leaf_id column)"
            )
        if locations.shape != values.shape:
            raise ValueError(
                f"locations shape {locations.shape} does not match values shape {values.shape}"
            )
        locations = locations[finite]
    values = values[finite]
    n = len(values)
    if n == 0:
        empty = np.empty((0, 2), dtype=np.float32)
        if locations is not None:
            return empty, np.empty(0, dtype=np.uint64)
        return empty

    if locations is not None:
        # Stable co-sort so equal values keep a deterministic location order.
        order = np.argsort(values, kind="stable")
        sorted_vals = values[order]
        locations = locations[order]
    else:
        sorted_vals = np.sort(values)

    # Unit-weight sub-centroids (one per observation); ``_compress`` greedily
    # merges adjacent ones under the k1 budget. ``starts`` records each output
    # centroid's first member index in the sorted order, so the location channel
    # can reduce member words per centroid afterwards.
    out_means, out_weights, starts = _compress(
        sorted_vals, np.ones(n, dtype=np.float64), float(delta)
    )

    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if locations is not None:
        return out, _centroid_ancestors(locations, starts.tolist(), n)
    return out


def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    locations1: np.ndarray | None = None,
    locations2: np.ndarray | None = None,
) -> np.ndarray | tuple[np.ndarray, np.ndarray]:
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

    Returns
    -------
    ndarray, shape (k_merged, 2), dtype float32
        Merged and re-compressed centroid array.  With locations, returns a
        ``(digest, locs)`` tuple, ``locs`` the ``(k_merged,)`` uint64 vector.
    """
    located = locations1 is not None or locations2 is not None
    if located and (locations1 is None) != (locations2 is None):
        raise ValueError("pass both locations1 and locations2, or neither")
    d1 = np.asarray(d1, dtype=np.float64)
    d2 = np.asarray(d2, dtype=np.float64)
    if located:
        locations1 = np.asarray(locations1)
        locations2 = np.asarray(locations2)
        for d, locs, tag in ((d1, locations1, "locations1"), (d2, locations2, "locations2")):
            if locs.dtype != np.uint64:
                raise ValueError(
                    f"{tag} dtype {locs.dtype} is not uint64; pass packed morton words"
                )
            if locs.shape != (len(d),):
                raise ValueError(f"{tag} shape {locs.shape} does not match {len(d)} centroids")

    if d1.size == 0 and d2.size == 0:
        empty = np.empty((0, 2), dtype=np.float32)
        return (empty, np.empty(0, dtype=np.uint64)) if located else empty
    if d1.size == 0:
        d2_out = np.asarray(d2, dtype=np.float32)
        if locations2 is not None:
            # Copy so the returned channel never aliases the caller's array.
            return d2_out, locations2.copy()
        return d2_out
    if d2.size == 0:
        d1_out = np.asarray(d1, dtype=np.float32)
        if locations1 is not None:
            return d1_out, locations1.copy()
        return d1_out

    combined = np.concatenate([d1, d2], axis=0)
    order = np.argsort(combined[:, 0], kind="stable")
    combined = combined[order]
    combined_locs = (
        np.concatenate([locations1, locations2])[order]
        if locations1 is not None and locations2 is not None
        else None
    )

    # Re-compress the concatenated sub-centroids under the same k1 budget as
    # the build path — the weighted variant of the greedy rule (issue #279).
    out_means, out_weights, starts = _compress(combined[:, 0], combined[:, 1], float(delta))

    out = np.empty((len(out_means), 2), dtype=np.float32)
    out[:, 0] = out_means
    out[:, 1] = out_weights
    if combined_locs is not None:
        return out, _centroid_ancestors(combined_locs, starts.tolist(), len(combined))
    return out


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
