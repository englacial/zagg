"""Client-side reader: per-cell t-digests → fixed ``(64, 64, n_bins)`` tensors.

These are *read helpers* (issue #79) that live in-repo but sit just outside
zagg's core write path: a client consuming a gridded Zarr product wants a dense,
fixed-size tensor per coverage chunk, reconstructed from the per-cell t-digest
ragged field that zagg writes (issue #48).

Store layout consumed
---------------------
The t-digest field is stored per shard as a CSR ragged group (see
:mod:`zagg.csr`): under the field prefix, one subgroup per shard, named by the
shard's **parent morton id** (the coverage cell), each holding the three CSR
arrays ``values`` / ``offsets`` / ``cell_ids``::

    {field}/{parent_morton}/values
    {field}/{parent_morton}/offsets
    {field}/{parent_morton}/cell_ids

``cell_ids[k]`` is a cell's position in the chunk's row-major ``(64, 64)``
children block, and ``values[offsets[k]:offsets[k+1]]`` is that cell's
``(k_centroids, 2)`` ``(mean, weight)`` digest.  The subgroup name is the
chunk's morton index, recovered directly from the store (issue #79 design
decision (3)); when a sibling ``{field}/morton`` ``uint64`` coordinate array is
present it is used in preference, mapping chunk order → morton id.

The reader (generator)
----------------------
:func:`read_tensors` opens the store and yields ``(tensor, morton_index)`` one
chunk at a time.  Per chunk it derives a tail-trimmed z-range from the cells'
``bottom``/``top`` quantiles, anchors a fixed ``n_bins * resolution`` window at
the floor of that range, and rasterizes each cell's digest into per-bin counts
via :func:`zagg.stats.tdigest.cdf_from_tdigest`.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Literal

import numpy as np
import zarr
from zarr.abc.store import Store

from zagg.csr import iter_csr_cells, read_csr
from zagg.stats.tdigest import cdf_from_tdigest, quantile_from_tdigest

__all__ = [
    "rasterize_cell",
    "chunk_z_range",
    "read_tensors",
    "read_raw_values",
    "read_locations",
]

# Coverage chunk is a 64×64 block of child cells (issue #79).
_CHUNK_SIDE = 64
_CHUNK_CELLS = _CHUNK_SIDE * _CHUNK_SIDE

FitMode = Literal["raise", "degrade_resolution", "collapse_bins"]
TensorDtype = Literal["uint16", "uint32", "float32"]

_TENSOR_DTYPES: dict[str, np.dtype] = {
    "uint16": np.dtype(np.uint16),
    "uint32": np.dtype(np.uint32),
    "float32": np.dtype(np.float32),
}


def rasterize_cell(
    digest: np.ndarray,
    z_lo: float,
    resolution: float,
    n_bins: int,
) -> np.ndarray:
    """Rasterize one cell's t-digest into ``n_bins`` per-bin counts.

    The bins are evenly spaced in value-space: bin ``i`` covers
    ``[z_lo + i*resolution, z_lo + (i+1)*resolution)``.  The count in a bin is
    the digest's reconstructed weight in that value interval, using the digest
    CDF::

        count[i] = cdf(edge_{i+1}) - cdf(edge_i)

    where ``cdf = cdf_from_tdigest`` (issue #79).  Weight below the first edge
    or above the last is dropped (the window is fixed; see :func:`chunk_z_range`
    for the fit policy that guards against truncation).

    Parameters
    ----------
    digest : ndarray, shape (k, 2)
        Centroid array ``(mean, weight)`` for one cell.  An empty digest yields
        an all-zero vector.
    z_lo : float
        Left edge of the first bin (meter-aligned chunk floor).
    resolution : float
        Bin width in value units.
    n_bins : int
        Number of bins.

    Returns
    -------
    ndarray, shape (n_bins,), dtype float64
        Per-bin reconstructed counts (not yet cast to the output dtype).
    """
    if len(digest) == 0:
        return np.zeros(n_bins, dtype=np.float64)
    edges = z_lo + resolution * np.arange(n_bins + 1, dtype=np.float64)
    cdf = np.asarray(cdf_from_tdigest(digest, edges), dtype=np.float64)
    counts = np.diff(cdf)
    # CDF is monotonic non-decreasing, so counts are ≥ 0 up to float noise.
    np.clip(counts, 0.0, None, out=counts)
    return counts


def _cell_tail_bounds(digest: np.ndarray, bottom: float, top: float) -> tuple[float, float] | None:
    """Return ``(lo, hi)`` = (``bottom``, ``top``) quantiles, or None if empty."""
    if len(digest) == 0:
        return None
    lo = quantile_from_tdigest(digest, bottom)
    hi = quantile_from_tdigest(digest, top)
    if not (math.isfinite(lo) and math.isfinite(hi)):
        return None
    return lo, hi


def chunk_z_range(
    digests: list[np.ndarray],
    *,
    n_bins: int,
    resolution: float,
    bottom: float,
    top: float,
    fit: FitMode,
) -> tuple[float, int, float]:
    """Derive the chunk's z-window and apply the fit policy.

    Per cell, ``lo_c = quantile_from_tdigest(d, bottom)`` and
    ``hi_c = quantile_from_tdigest(d, top)`` trim the tails.  The window floor is
    ``z_lo = floor(min lo_c)``; the fixed window spans ``n_bins * resolution``
    anchored at ``z_lo``.  If ``ceil(max hi_c) > z_lo + n_bins*resolution`` the
    trimmed data does not fit, and ``fit`` decides what happens:

    - ``"raise"`` (default) — raise :class:`ValueError`.
    - ``"degrade_resolution"`` — double ``resolution`` (powers of two) until the
      window covers the range, keeping ``n_bins`` fixed.
    - ``"collapse_bins"`` — shrink ``n_bins`` to the smallest power of two whose
      window (at the original ``resolution``) covers the range.

    Parameters
    ----------
    digests : list of ndarray
        Per-populated-cell digests for the chunk.
    n_bins, resolution, bottom, top, fit
        See :func:`read_tensors`.

    Returns
    -------
    (z_lo, n_bins, resolution) : tuple
        The window floor and the (possibly adjusted) bin count and resolution.

    Raises
    ------
    ValueError
        If the chunk has no populated cells, or ``fit="raise"`` and the trimmed
        range exceeds the fixed window.
    """
    bounds = [b for b in (_cell_tail_bounds(d, bottom, top) for d in digests) if b is not None]
    if not bounds:
        raise ValueError("chunk has no populated cells with a finite quantile range")

    lo_min = min(b[0] for b in bounds)
    hi_max = max(b[1] for b in bounds)
    z_lo = math.floor(lo_min)
    z_hi = math.ceil(hi_max)
    needed = z_hi - z_lo
    window = n_bins * resolution

    if fit == "collapse_bins":
        # Shrink to the smallest power-of-two bin count (≤ the largest power of
        # two that fits within n_bins) whose window still covers the trimmed
        # range. Only ever reduces the bin count, so it cannot help a range that
        # already exceeds the full n_bins window.
        if needed > window:
            raise ValueError(
                f'fit="collapse_bins" cannot grow the window: trimmed span {needed} '
                f"exceeds {n_bins} bins × {resolution} = {window}"
            )
        # Largest power of two ≤ n_bins (the collapsed count is always pow2).
        n = 1 << (int(n_bins).bit_length() - 1)
        while n // 2 >= 1 and (n // 2) * resolution >= needed:
            n //= 2
        return float(z_lo), n, resolution

    if needed <= window:
        return float(z_lo), n_bins, resolution

    if fit == "raise":
        raise ValueError(
            f"trimmed z-range [{z_lo}, {z_hi}] (span {needed}) exceeds the fixed "
            f"window {n_bins} bins × {resolution} = {window}; pass "
            f'fit="degrade_resolution" or fit="collapse_bins" to adapt'
        )
    if fit == "degrade_resolution":
        res = resolution
        while needed > n_bins * res:
            res *= 2.0
        return float(z_lo), n_bins, res
    raise ValueError(f"unknown fit mode {fit!r}")


def _resolve_chunk_morton(
    store: Store, field: str, shard_keys: list[str], zarr_format: Literal[2, 3]
) -> dict[str, int]:
    """Map each shard subgroup name → its morton id.

    Prefers a sibling ``{field}/morton`` ``uint64`` coordinate array (chunk
    order → morton id); falls back to parsing the subgroup name as the parent
    morton id (the shard key is the parent morton — see :func:`process_shard`).

    The coordinate array is aligned against the subgroup names sorted
    **numerically** (the canonical ascending-morton chunk order), not
    lexicographically — string sorting would mis-align names of differing digit
    counts (e.g. ``"1000"`` before ``"99"``).
    """
    try:
        arr = zarr.open_array(store, path=f"{field}/morton", mode="r", zarr_format=zarr_format)
        morton = np.asarray(arr[...])
    except (FileNotFoundError, KeyError, ValueError):
        return {k: int(k) for k in shard_keys}
    if len(morton) != len(shard_keys):
        # Coordinate present but not 1:1 with subgroups — fall back to the names.
        return {k: int(k) for k in shard_keys}
    return {k: int(m) for k, m in zip(sorted(shard_keys, key=int), morton)}


def read_tensors(
    store: Store,
    field: str,
    *,
    n_bins: int = 128,
    resolution: float = 0.5,
    bottom: float = 0.05,
    top: float = 0.95,
    fit: FitMode = "raise",
    dtype: TensorDtype = "uint32",
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[np.ndarray, int]]:
    """Yield ``(tensor, morton_index)`` per coverage chunk of a t-digest field.

    Opens the CSR ragged store for ``field`` and iterates one Zarr chunk (one
    64×64 parent block) at a time.  For each chunk it trims the per-cell tails,
    derives a fixed z-window (see :func:`chunk_z_range`), rasterizes every
    populated cell's digest into ``n_bins`` counts (see :func:`rasterize_cell`),
    and emits the ``(64, 64, n_bins)`` tensor with the chunk's morton id.

    Parameters
    ----------
    store : Store
        Zarr store holding the per-shard CSR groups under ``field``.
    field : str
        Field prefix (e.g. ``"h_tdigest"``).
    n_bins : int, optional
        Number of z-bins (default 128).
    resolution : float, optional
        Bin width in value units (default 0.5).
    bottom, top : float, optional
        Lower/upper density-trim quantiles (default 0.05 / 0.95) — the window
        spans the cells' ``bottom``→``top`` quantile range.
    fit : {"raise", "degrade_resolution", "collapse_bins"}, optional
        Behaviour when the trimmed range exceeds ``n_bins * resolution``
        (default ``"raise"``).
    dtype : {"uint16", "uint32", "float32"}, optional
        Output tensor dtype (default ``"uint32"``).  ``uint16``/``uint32`` round
        counts to the nearest integer; ``float32`` keeps fractional counts.  A
        per-bin count exceeding the dtype's max (65535 for ``uint16``) wraps on
        cast — keep ``uint32`` for dense cells with many observations per bin.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (tensor, morton_index) : (ndarray, int)
        ``tensor`` has shape ``(64, 64, n_bins_out)`` and the requested dtype;
        ``morton_index`` is the chunk's coverage-cell morton id.

    Raises
    ------
    ValueError
        On an unknown ``dtype``/``fit``, or (with ``fit="raise"``) a chunk whose
        trimmed range overflows the fixed window.
    """
    if dtype not in _TENSOR_DTYPES:
        raise ValueError(f"unknown dtype {dtype!r}; expected one of {sorted(_TENSOR_DTYPES)}")
    out_dtype = _TENSOR_DTYPES[dtype]
    is_float = np.issubdtype(out_dtype, np.floating)

    group = zarr.open_group(store, path=field, mode="r", zarr_format=zarr_format)
    shard_keys = sorted(k for k in group.group_keys() if k != "morton")
    morton_of = _resolve_chunk_morton(store, field, shard_keys, zarr_format)

    for key in shard_keys:
        cells = iter_csr_cells(read_csr(store, f"{field}/{key}", zarr_format=zarr_format))
        digests = [payload for _, payload in cells]
        z_lo, n_bins_c, resolution_c = chunk_z_range(
            digests,
            n_bins=n_bins,
            resolution=resolution,
            bottom=bottom,
            top=top,
            fit=fit,
        )

        tensor = np.zeros((_CHUNK_SIDE, _CHUNK_SIDE, n_bins_c), dtype=out_dtype)
        for cell_id, digest in cells:
            if not (0 <= cell_id < _CHUNK_CELLS):
                raise ValueError(
                    f"cell_id {cell_id} out of range for a {_CHUNK_SIDE}×{_CHUNK_SIDE} chunk"
                )
            counts = rasterize_cell(digest, z_lo, resolution_c, n_bins_c)
            if not is_float:
                counts = np.rint(counts)
            row, col = divmod(cell_id, _CHUNK_SIDE)
            tensor[row, col, :] = counts.astype(out_dtype)

        yield tensor, morton_of[key]


def read_raw_values(
    store: Store,
    field: str,
    *,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[int, int, np.ndarray]]:
    """Yield ``(morton_index, cell_id, values)`` raw samples per populated cell.

    The companion lossless reader (issue #79 follow-up): when a digest was built
    with no merges (every centroid weight 1) its centroid means *are* the
    original observations, so the raw value vector can be recovered exactly.  A
    cell whose digest contains any merged centroid (weight > 1) is **not**
    losslessly recoverable and raises :class:`ValueError`.

    Parameters
    ----------
    store : Store
        Zarr store holding the per-shard CSR groups under ``field``.
    field : str
        Field prefix.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (morton_index, cell_id, values) : (int, int, ndarray)
        ``values`` is the cell's recovered 1-D sample vector (sorted ascending,
        as the digest stores centroids by mean).

    Raises
    ------
    ValueError
        If any cell's digest carries a merged centroid (weight > 1), so the raw
        values cannot be recovered without loss.
    """
    group = zarr.open_group(store, path=field, mode="r", zarr_format=zarr_format)
    shard_keys = sorted(k for k in group.group_keys() if k != "morton")
    morton_of = _resolve_chunk_morton(store, field, shard_keys, zarr_format)

    for key in shard_keys:
        morton = morton_of[key]
        for cell_id, digest in iter_csr_cells(
            read_csr(store, f"{field}/{key}", zarr_format=zarr_format)
        ):
            if len(digest) == 0:
                continue
            weights = np.asarray(digest[:, 1], dtype=np.float64)
            if np.any(weights > 1.0):
                raise ValueError(
                    f"cell {cell_id} (chunk {morton}) has merged centroids "
                    "(weight > 1); raw values are not losslessly recoverable"
                )
            yield int(morton), int(cell_id), np.asarray(digest[:, 0], dtype=np.float64)


def read_locations(
    store: Store,
    field: str,
    *,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[int, int, np.ndarray]]:
    """Yield ``(morton_index, cell_id, locations)`` per populated cell.

    The location-channel reader (issue #87): a located ragged field stores a
    per-centroid ``uint64`` morton location vector in a ``locations`` array
    sharing the CSR ``offsets``/``cell_ids`` with the field's ``values``, so
    the k-th populated cell's locations are ``locations[offsets[k]:offsets[k+1]]``
    — one word per centroid, each the deepest morton cell enclosing that
    centroid's member observations (an exact order-29 point word for a 1-obs
    centroid).  Decode or coarsen the words with mortie (``mort2healpix``,
    ``clip2order``, ``common_ancestor``); geometric queries compose directly on
    the packed words.

    Parameters
    ----------
    store : Store
        Zarr store holding the per-shard CSR groups under ``field``.
    field : str
        Field prefix.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (morton_index, cell_id, locations) : (int, int, ndarray)
        ``locations`` is the cell's ``(k,)`` uint64 per-centroid location
        vector, aligned with the digest rows :func:`read_tensors` /
        :func:`read_raw_values` see for the same cell.

    Raises
    ------
    ValueError
        If the field was not written with a location channel.
    """
    group = zarr.open_group(store, path=field, mode="r", zarr_format=zarr_format)
    shard_keys = sorted(k for k in group.group_keys() if k != "morton")
    morton_of = _resolve_chunk_morton(store, field, shard_keys, zarr_format)

    for key in shard_keys:
        csr = read_csr(store, f"{field}/{key}", zarr_format=zarr_format, locations=True)
        locations = csr["locations"]
        offsets = csr["offsets"]
        for k, cid in enumerate(csr["cell_ids"]):
            yield int(morton_of[key]), int(cid), locations[offsets[k] : offsets[k + 1]]
