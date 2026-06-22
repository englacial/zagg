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
than the raw observation array.  The scale function is evaluated for the whole
rank vector in a single ``arcsin`` call and the centroid means/weights come from
one ``np.add.reduceat`` segment-sum, so the only Python-level work is an O(n)
loop of scalar float comparisons to find centroid boundaries — ~70 μs at n=250
and ~200 μs at n=1000 (≈7× faster than a per-observation ``arcsin``).  Over a
4096-cell shard with ~500 obs per cell that is well below network IO.  At δ=128
the centroid count (and output size) is ~4× smaller; at δ=1024 the digest
carries ~2× more centroids and is more accurate near the tails.

The boundary scan is the only remaining O(n) Python work.  For cells with very
high observation counts (n ≫ centroid count), pass ``method="jump"`` to locate
each boundary with ``np.searchsorted`` over the monotone k1 vector — O(centroids)
iterations instead of O(n), ~15–29× faster at n ≥ 10k.  Output is identical to
the default ``method="loop"``; loop stays the better choice at n ≈ 250–1000
where centroids ≈ n.

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
"""

from __future__ import annotations

import numpy as np

__all__ = ["build_tdigest", "merge_tdigests", "quantile_from_tdigest"]

_DEFAULT_DELTA = 512


def _k1_scale(q, delta: float):
    """Dunning's k1 scale function: k(q) = delta * (arcsin(2q - 1)/pi + 1/2).

    Maps the cumulative rank fraction q ∈ [0, 1] onto [0, delta].  Its
    derivative is largest at the tails (q → 0 or 1) and smallest at the median,
    so bounding each centroid to span ≤ 1 unit of k yields narrow, high-
    resolution centroids in the tails and wide centroids in the middle — the
    defining t-digest property.  A digest holds ~delta centroids regardless of
    the observation count, and is loss-free while the count stays ≤ delta.

    ``q`` may be a scalar or an array; the computation is vectorized so the
    whole rank vector is mapped with a single ``arcsin`` call.
    """
    qc = np.clip(np.asarray(q, dtype=np.float64), 0.0, 1.0)
    return delta * (np.arcsin(2.0 * qc - 1.0) / np.pi + 0.5)


def _segment_starts(k_right: np.ndarray, delta_f: float, method: str) -> np.ndarray:
    """Find centroid start indices from the per-element right-edge k1 values.

    ``k_right[i]`` is the k1 scale at element ``i``'s right cumulative-rank edge;
    the array is strictly increasing (k1 is monotone in q).  A centroid that
    opens at index ``s`` absorbs every following element while it stays within
    1 unit of the k1 scale measured from its left edge ``k_left`` (the k value
    just before its first element, ``k_right[s - 1]``, or ``k(0)`` for ``s = 0``);
    the next centroid opens at the first ``i`` with ``k_right[i] - k_left > 1``.

    Two equivalent paths produce **identical** boundaries:

    ``"loop"`` (default)
        Scan every element with a scalar float compare — ``O(n)`` Python-level
        work.  Optimal in the per-cell regime (n ≈ 250–1000, centroids ≈ n).
    ``"jump"``
        Locate each next boundary with ``np.searchsorted`` over the monotone
        ``k_right`` — ``O(k)`` Python iterations (one per centroid) instead of
        ``O(n)``.  Much faster when n ≫ k (n ≥ 10k); the searchsorted overhead
        makes it ~1.6–2× when k ≈ n.
    """
    n = len(k_right)
    k0 = float(_k1_scale(0.0, delta_f))
    if method == "loop":
        starts = [0]
        k_left = k0
        for i in range(1, n):
            if k_right[i] - k_left > 1.0:
                starts.append(i)
                k_left = float(k_right[i - 1])
        return np.asarray(starts)
    if method == "jump":
        starts = [0]
        k_left = k0
        s = 0
        while True:
            # First index whose right edge exceeds the centroid's k-budget; the
            # ``> s`` guard makes a loss-free element (where one step already
            # exceeds 1 k-unit) still claim its own centroid, matching the loop.
            nxt = int(np.searchsorted(k_right, k_left + 1.0, side="right"))
            if nxt <= s:
                nxt = s + 1
            if nxt >= n:
                break
            starts.append(nxt)
            k_left = float(k_right[nxt - 1])
            s = nxt
        return np.asarray(starts)
    raise ValueError(f"method must be 'loop' or 'jump', got {method!r}")


def build_tdigest(
    values: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    method: str = "loop",
) -> np.ndarray:
    """Build a t-digest sketch from a 1-D array of values.

    Parameters
    ----------
    values : ndarray
        1-D array of observed values (any finite float).  NaN values are
        silently dropped before sketching.
    delta : int, optional
        Compression parameter.  Larger δ → more centroids → more accurate.
        Default 512.  Typical values: 128, 256, 512, 1024.
    method : {"loop", "jump"}, optional
        How centroid boundaries are found (the output is **identical** either
        way — only the boundary search differs).  ``"loop"`` (default) scans
        every observation with a scalar compare, optimal in the per-cell regime
        (n ≈ 250–1000, where centroids ≈ n).  ``"jump"`` uses ``np.searchsorted``
        over the monotone k1 vector to skip a whole centroid's budget at once —
        ``O(k)`` Python iterations instead of ``O(n)``, much faster when n ≫ k
        (n ≥ 10k) but ~1.6–2× when k ≈ n.

    Returns
    -------
    ndarray, shape (k, 2), dtype float32
        Sorted centroid array.  Column 0 is centroid mean; column 1 is weight
        (number of observations merged into that centroid).

    Notes
    -----
    Returns an empty ``(0, 2)`` array when ``values`` is empty or all-NaN.
    """
    values = np.asarray(values, dtype=np.float64)
    values = values[np.isfinite(values)]
    n = len(values)
    if n == 0:
        return np.empty((0, 2), dtype=np.float32)

    sorted_vals = np.sort(values)
    n_total = float(n)
    delta_f = float(delta)

    # k1 scale at every observation's right cumulative-rank edge (rank i + 1 of
    # n), vectorized — one arcsin over the whole array replaces the per-
    # observation call that dominated the sketch cost.
    k_right = _k1_scale(np.arange(1, n + 1, dtype=np.float64) / n_total, delta_f)

    # Greedy partition: observation i joins the current centroid while the
    # centroid still spans ≤ 1 unit of the k1 scale measured from its left edge
    # (the k value just before its first observation), otherwise it opens a
    # fresh centroid. Only the centroid *start* indices are collected here (via
    # ``method``); the centroid means/weights fall out of one vectorized
    # segment-sum below. This keeps the digest to ~delta centroids and loss-free
    # until the count exceeds delta.
    start_idx = _segment_starts(k_right, delta_f, method)
    counts = np.diff(np.append(start_idx, n)).astype(np.float64)
    sums = np.add.reduceat(sorted_vals, start_idx)

    out = np.empty((len(start_idx), 2), dtype=np.float32)
    out[:, 0] = sums / counts
    out[:, 1] = counts
    return out


def merge_tdigests(
    d1: np.ndarray,
    d2: np.ndarray,
    delta: int = _DEFAULT_DELTA,
    method: str = "loop",
) -> np.ndarray:
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
    method : {"loop", "jump"}, optional
        Boundary-search path, forwarded to the same shared logic as
        :func:`build_tdigest`; the output is identical either way.  See
        :func:`build_tdigest` for the n-regime trade-off.

    Returns
    -------
    ndarray, shape (k_merged, 2), dtype float32
        Merged and re-compressed centroid array.
    """
    d1 = np.asarray(d1, dtype=np.float64)
    d2 = np.asarray(d2, dtype=np.float64)

    if d1.size == 0 and d2.size == 0:
        return np.empty((0, 2), dtype=np.float32)
    if d1.size == 0:
        return np.asarray(d2, dtype=np.float32)
    if d2.size == 0:
        return np.asarray(d1, dtype=np.float32)

    combined = np.concatenate([d1, d2], axis=0)
    order = np.argsort(combined[:, 0], kind="stable")
    combined = combined[order]

    delta_f = float(delta)
    c_means = combined[:, 0]
    c_weights = combined[:, 1]
    n_total = float(c_weights.sum())

    # k1 scale at each sub-centroid's right cumulative-*weight* edge (as a
    # fraction of total weight), vectorized like build_tdigest. A sub-centroid
    # merges into the current centroid while the combined span stays ≤ 1 unit of
    # the k1 scale measured from its left edge.
    k_right = _k1_scale(np.cumsum(c_weights) / n_total, delta_f)

    start_idx = _segment_starts(k_right, delta_f, method)
    seg_weight = np.add.reduceat(c_weights, start_idx)
    seg_weighted_mean = np.add.reduceat(c_means * c_weights, start_idx)

    out = np.empty((len(start_idx), 2), dtype=np.float32)
    out[:, 0] = seg_weighted_mean / seg_weight
    out[:, 1] = seg_weight
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
