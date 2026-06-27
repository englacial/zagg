"""Offline-computable read plan for AOI-based hyperslice selection (issue #43, Phase C).

``plan_read`` takes which coarse-level segments (e.g. ATL03 ``land_ice_segments``)
match the AOI -- preferably a precomputed mortie segment->shard ``coarse_mask``
(issue #95), or a bbox fallback -- merges adjacent runs, translates them to
base-level (photon) slices, and packages them as h5coro-compatible hyperslice
lists. ``execute_read_plan`` then drives a caller-supplied read function with
those slices.

Both functions are pure and offline-testable — no h5coro, S3, or credentials needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from shapely.geometry import LineString, box


@dataclass
class ReadPlan:
    """A pre-computed read plan for efficient AOI-scoped HDF5 reads.

    Attributes
    ----------
    parent_runs : list of (int, int)
        Contiguous runs of matched coarse-level parents, as ``(start, end)``
        inclusive index pairs in the coarse array.
    base_slices : list of (int, int)
        Corresponding base-level ``[start, end)`` half-open slices.
    chunk_lists : list of list of (int, int)
        h5coro-style ``[(start, end)]`` hyperslice lists (half-open
        ``[start, end)``, 0-based — matches h5coro's ``h5dataset.py`` "must
        provide as list of ranges [x,y)" contract). Mirrors ``base_slices``
        one-to-one.
    coarse_flag_ranges : list of (int, int)
        Reserved for future use (coarse-level flag read ranges).
    full_read : bool
        True when the plan falls back to reading the full array (selectivity too low).
    """

    parent_runs: list[tuple[int, int]]
    base_slices: list[tuple[int, int]]
    chunk_lists: list[list[tuple[int, int]]]
    coarse_flag_ranges: list[tuple[int, int]] = field(default_factory=list)
    full_read: bool = False


def plan_read(
    lat_arr: np.ndarray,
    lon_arr: np.ndarray,
    index_beg_arr: np.ndarray,
    count_arr: np.ndarray,
    n_base: int,
    bbox: tuple[float, float, float, float] | None = None,
    index_base: int = 0,
    pad: int = 1,
    full_read_threshold: float = 0.9,
    coarse_mask: np.ndarray | None = None,
) -> ReadPlan:
    """Compute a hyperslice-based read plan for an AOI (issue #43, Phase C).

    Which coarse-level parents (segments) match the AOI is decided one of two
    ways:

    * ``coarse_mask`` given (preferred) -- a boolean array, one entry per parent,
      already computed by the caller. The production path passes the **mortie**
      segment->shard mask (``grid.shards_of(grid.assign(seg_lat, seg_lon)) ==
      shard_key``), the same exact test the photon path applies later -- so the
      coarse filter matches the leaf cell, not a loose bbox, and skips the
      per-segment shapely scan entirely (issue #95). Because the mask is
      rep-point based, a boundary segment whose photons straddle the shard edge
      is recovered by ``pad`` (and the exact photon-level filter never
      *over*-includes), so the only residual is a bounded edge omission.
    * ``bbox`` given (no ``coarse_mask``) -- the grid-free fallback: a parent
      matches when its rep-point ``(lat, lon)`` is in ``bbox`` OR the linestring
      to the next parent crosses ``bbox`` (euclidean, fine for 20 m segments).
      Kept for offline/grid-free callers and tests.

    Adjacent matched parents are merged into contiguous runs, padded by ``pad``
    elements on each side, then translated to base-level ``[start, end)`` slices.

    If the total planned base-level read exceeds ``full_read_threshold * n_base``,
    returns a plan with ``full_read=True`` covering everything (cheaper than many
    small reads that still sum to most of the file).

    Parameters
    ----------
    lat_arr, lon_arr : np.ndarray
        Float arrays, shape ``(n_coarse,)``. Rep-point coords of each parent.
        Used only by the ``bbox`` fallback; ignored when ``coarse_mask`` is given
        (then only their length sets ``n_coarse``).
    index_beg_arr : np.ndarray
        Integer array, shape ``(n_coarse,)``. Base-level start for each parent
        (before ``index_base`` adjustment).
    count_arr : np.ndarray
        Integer array, shape ``(n_coarse,)``. Number of base-level children per parent.
    n_base : int
        Total size of the base array.
    bbox : (min_lon, min_lat, max_lon, max_lat), optional
        AOI bounding box for the fallback matcher. Required when ``coarse_mask``
        is not given.
    index_base : int
        0 (default) or 1 (ATL03 1-based ``ph_index_beg``).
    pad : int
        Number of extra parents to include on each side of each run (clamped
        to array bounds). Recovers partial edge segments (the omission knob).
    full_read_threshold : float
        Fraction of ``n_base`` above which the plan falls back to a full read.
    coarse_mask : np.ndarray, optional
        Boolean per-parent match mask. Takes precedence over ``bbox``.

    Returns
    -------
    ReadPlan
    """
    n_coarse = len(index_beg_arr)  # one entry per coarse parent (segment)
    if n_coarse == 0 or n_base == 0:
        return ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[])

    # -- AOI matching --
    if coarse_mask is not None:
        in_aoi = np.asarray(coarse_mask, dtype=bool)
        if in_aoi.shape != (n_coarse,):
            raise ValueError(f"coarse_mask shape {in_aoi.shape} != ({n_coarse},) parents")
    elif bbox is not None:
        # Grid-free fallback: per-segment rep-point / linestring test against bbox.
        aoi_box = box(bbox[0], bbox[1], bbox[2], bbox[3])
        in_aoi = np.zeros(n_coarse, dtype=bool)
        for j in range(n_coarse):
            lat, lon = float(lat_arr[j]), float(lon_arr[j])
            if bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]:
                in_aoi[j] = True
                continue
            # Last segment: no next-segment linestring to check; rep-point only.
            if j + 1 < n_coarse:
                lat2, lon2 = float(lat_arr[j + 1]), float(lon_arr[j + 1])
                seg_line = LineString([(lon, lat), (lon2, lat2)])
                if seg_line.intersects(aoi_box):
                    in_aoi[j] = True
    else:
        raise ValueError("plan_read requires either coarse_mask or bbox")

    if not in_aoi.any():
        return ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[])

    # -- Run merging --
    # Contiguous blocks of True in in_aoi, found vectorized via the rising/falling
    # edges of the mask (sentinel-padded so runs touching either end are closed).
    edges = np.flatnonzero(np.diff(np.concatenate(([False], in_aoi, [False])).astype(np.int8)))
    raw_runs: list[tuple[int, int]] = list(zip(edges[::2].tolist(), (edges[1::2] - 1).tolist()))

    # -- Padding --
    padded_runs: list[tuple[int, int]] = []
    for s, e in raw_runs:
        ps = max(0, s - pad)
        pe = min(n_coarse - 1, e + pad)
        padded_runs.append((ps, pe))

    # Merge overlapping/adjacent padded runs.
    merged: list[tuple[int, int]] = []
    for s, e in padded_runs:
        # +1 merges immediately adjacent runs (closed intervals: [a,b] and [b+1,c] -> [a,c]).
        if merged and s <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))

    # -- Translate to base slices --
    # ``kept_runs`` stays one-to-one with ``base_slices``/``chunk_lists``: a run
    # that yields no slice (all-empty, or collapsed) is dropped from all three,
    # so the returned plan never has phantom ``parent_runs`` without matching
    # reads (which would crash ``execute_read_plan`` on an empty concatenate).
    kept_runs: list[tuple[int, int]] = []
    base_slices: list[tuple[int, int]] = []
    chunk_lists: list[list[tuple[int, int]]] = []
    total_base = 0
    ibeg = np.asarray(index_beg_arr)
    cnt = np.asarray(count_arr)
    for s, e in merged:
        # ATL03 marks empty segments (no photons) with count == 0 and
        # ph_index_beg == 0. Using such a segment as a run boundary translates
        # to a bogus slice: base_start = 0 - index_base clamps to 0, so the read
        # spans from photon 0 to the AOI (~half the granule). That balloons
        # total_base past the selectivity threshold -> full-granule read -> OOM
        # (and ruinous compute). Bound each run by its NON-EMPTY segments only;
        # empty ones carry no photons, so a run with none is skipped. (The single
        # contiguous [first, last] slice relies on the #43 contiguity assumption:
        # non-empty segments are monotonic in ph_index_beg and tile the photons.)
        local = np.flatnonzero(cnt[s : e + 1] > 0)
        if local.size == 0:
            continue
        first, last = s + int(local[0]), s + int(local[-1])
        base_start = int(ibeg[first]) - index_base
        base_end = int(ibeg[last]) - index_base + int(cnt[last])
        base_start = max(0, base_start)
        base_end = min(n_base, base_end)
        if base_end <= base_start:
            continue
        kept_runs.append((s, e))
        base_slices.append((base_start, base_end))
        chunk_lists.append([(base_start, base_end)])  # h5coro half-open [start, end)
        total_base += base_end - base_start

    # -- Selectivity fallback --
    if total_base > full_read_threshold * n_base:
        return ReadPlan(
            parent_runs=[(0, n_coarse - 1)],
            base_slices=[(0, n_base)],
            chunk_lists=[[(0, n_base)]],
            full_read=True,
        )

    return ReadPlan(
        parent_runs=kept_runs,
        base_slices=base_slices,
        chunk_lists=chunk_lists,
    )


def execute_read_plan(plan: ReadPlan, read_fn, dataset_path: str, dtype) -> np.ndarray:
    """Execute a ReadPlan using the supplied read function.

    Parameters
    ----------
    plan : ReadPlan
        A plan produced by :func:`plan_read`.
    read_fn : callable
        ``read_fn(dataset_path, hyperslice=None)`` -> array.
        Passing ``hyperslice=None`` reads the full dataset.
    dataset_path : str
        HDF5 dataset path to read.
    dtype : numpy dtype or str
        Dtype for the returned array.

    Returns
    -------
    np.ndarray
        Concatenated data for all runs in the plan, or an empty array if the
        plan matched nothing. If ``plan.full_read``, returns the full dataset.
    """
    if plan.full_read:
        return np.asarray(read_fn(dataset_path, hyperslice=None), dtype=dtype)
    if not plan.parent_runs:
        return np.empty(0, dtype=dtype)
    parts = [
        np.asarray(read_fn(dataset_path, hyperslice=chunk), dtype=dtype)
        for chunk in plan.chunk_lists
    ]
    return np.concatenate(parts)
