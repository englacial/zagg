"""Offline-computable read plan for AOI-based hyperslice selection (issue #43, Phase C).

``plan_read`` computes which coarse-level segments (e.g. ATL03 ``land_ice_segments``)
overlap a bounding-box AOI, merges adjacent runs, translates them to base-level
(photon) slices, and packages them as h5coro-compatible hyperslice lists.
``execute_read_plan`` then drives a caller-supplied read function with those slices.

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
        h5coro-style ``[(start, end)]`` hyperslice lists (inclusive end, 0-based)
        for each run.
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
    bbox: tuple[float, float, float, float],
    index_base: int = 0,
    pad: int = 1,
    full_read_threshold: float = 0.9,
) -> ReadPlan:
    """Compute a hyperslice-based read plan for an AOI bounding box (issue #43, Phase C).

    For each coarse-level parent (segment), checks whether:
    (a) the rep-point ``(lat_arr[j], lon_arr[j])`` falls within ``bbox``, OR
    (b) the linestring from ``(lat_arr[j], lon_arr[j])`` to the next parent
        crosses ``bbox`` (euclidean approximation, fine for 20 m segments).

    Adjacent matched parents are merged into contiguous runs, padded by ``pad``
    elements on each side, then translated to base-level ``[start, end)`` slices.

    If the total planned base-level read exceeds ``full_read_threshold * n_base``,
    returns a plan with ``full_read=True`` covering everything (cheaper than many
    small reads that still sum to most of the file).

    Parameters
    ----------
    lat_arr : np.ndarray
        Float array, shape ``(n_coarse,)``. Rep-point latitudes of each parent.
    lon_arr : np.ndarray
        Float array, shape ``(n_coarse,)``. Rep-point longitudes of each parent.
    index_beg_arr : np.ndarray
        Integer array, shape ``(n_coarse,)``. Base-level start for each parent
        (before ``index_base`` adjustment).
    count_arr : np.ndarray
        Integer array, shape ``(n_coarse,)``. Number of base-level children per parent.
    n_base : int
        Total size of the base array.
    bbox : (min_lon, min_lat, max_lon, max_lat)
        Bounding box for the AOI.
    index_base : int
        0 (default) or 1 (ATL03 1-based ``ph_index_beg``).
    pad : int
        Number of extra parents to include on each side of each run (clamped
        to array bounds). Helps capture partial edge segments.
    full_read_threshold : float
        Fraction of ``n_base`` above which the plan falls back to a full read.

    Returns
    -------
    ReadPlan
    """
    n_coarse = len(lat_arr)
    if n_coarse == 0 or n_base == 0:
        return ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[])

    aoi_box = box(bbox[0], bbox[1], bbox[2], bbox[3])

    # -- AOI matching --
    in_aoi = np.zeros(n_coarse, dtype=bool)
    for j in range(n_coarse):
        lat, lon = float(lat_arr[j]), float(lon_arr[j])
        if bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]:
            in_aoi[j] = True
            continue
        # Linestring crossing check with the next segment.
        if j + 1 < n_coarse:
            lat2, lon2 = float(lat_arr[j + 1]), float(lon_arr[j + 1])
            seg_line = LineString([(lon, lat), (lon2, lat2)])
            if seg_line.intersects(aoi_box):
                in_aoi[j] = True

    if not in_aoi.any():
        return ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[])

    # -- Run merging --
    # Find contiguous blocks of True in in_aoi.
    raw_runs: list[tuple[int, int]] = []
    start_run = None
    for j in range(n_coarse):
        if in_aoi[j] and start_run is None:
            start_run = j
        elif not in_aoi[j] and start_run is not None:
            raw_runs.append((start_run, j - 1))
            start_run = None
    if start_run is not None:
        raw_runs.append((start_run, n_coarse - 1))

    # -- Padding --
    padded_runs: list[tuple[int, int]] = []
    for s, e in raw_runs:
        ps = max(0, s - pad)
        pe = min(n_coarse - 1, e + pad)
        padded_runs.append((ps, pe))

    # Merge overlapping/adjacent padded runs.
    merged: list[tuple[int, int]] = []
    for s, e in padded_runs:
        if merged and s <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))

    # -- Translate to base slices --
    base_slices: list[tuple[int, int]] = []
    chunk_lists: list[list[tuple[int, int]]] = []
    total_base = 0
    for s, e in merged:
        base_start = int(index_beg_arr[s]) - index_base
        base_end = int(index_beg_arr[e]) - index_base + int(count_arr[e])
        base_start = max(0, base_start)
        base_end = min(n_base, base_end)
        if base_end <= base_start:
            continue
        base_slices.append((base_start, base_end))
        chunk_lists.append([(base_start, base_end - 1)])  # h5coro inclusive end
        total_base += base_end - base_start

    # -- Selectivity fallback --
    if total_base > full_read_threshold * n_base:
        return ReadPlan(
            parent_runs=[(0, n_coarse - 1)],
            base_slices=[(0, n_base)],
            chunk_lists=[[(0, n_base - 1)]],
            full_read=True,
        )

    return ReadPlan(
        parent_runs=merged,
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
    if not plan.parent_runs:
        return np.empty(0, dtype=dtype)
    if plan.full_read:
        return np.asarray(read_fn(dataset_path, hyperslice=None), dtype=dtype)
    parts = [
        np.asarray(read_fn(dataset_path, hyperslice=chunk), dtype=dtype)
        for chunk in plan.chunk_lists
    ]
    return np.concatenate(parts)
