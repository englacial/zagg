"""Client-side reader: per-cell t-digests → fixed ``(side, side, n_bins)`` tensors.

These are *read helpers* (issue #79) that live in-repo but sit just outside
zagg's core write path: a client consuming a gridded Zarr product wants a dense,
fixed-size tensor per coverage chunk, reconstructed from the per-cell t-digest
ragged field that zagg writes (issue #48).

Store layout consumed (issue #209 — the sharded vlen-bytes layout)
------------------------------------------------------------------
A ragged field is ONE ``variable_length_bytes`` array on the cell grid::

    {group}/{field}            <- vlen array; cell i holds the raw little-endian
                                  bytes of its (n, *inner_shape) payload
    {group}/{field}_locations  <- located fields only (issue #87): the uint64
                                  per-row location words, row-aligned
    {group}/morton             <- per-cell uint64 morton coordinate (zagg's
                                  standard HEALPix coordinate array)

The element interpretation is self-describing via the array attrs
(``grids.base.RAGGED_ELEMENT_ATTR``)::

    attrs["ragged"] = {"element": {"dtype": "float32", "shape": [-1, 2]},
                       "locations": "<sibling name>"}   # located fields only

so the readers decode what the writer declared rather than hardcoding a dtype,
and bind the location channel by metadata, not naming convention. A store
without these attrs is not a zagg ragged vlen array (pre-issue-209 CSR stores
are a hard break) and raises a pointed error.

The read plan honors the layout the writer chose: a whole-store sweep LISTs the
array's stored chunk objects (one object per shard under the ShardingCodec, or
one per inner chunk on the unsharded flat layout — both self-describing in the
array metadata, so one code path reads either), then decodes per read chunk.
Each read chunk is a square ``(side, side)`` block of cells (row-major
``cell_id`` within the chunk, ``side = isqrt(cells_per_chunk)`` — 64 for the
production ``chunk_inner`` configs); its coverage-cell morton id is derived
from the sibling ``morton`` coordinate. Random access to one cell
(:func:`read_cell`) indexes the vlen array directly — 2 ranged GETs on a
sharded store (index suffix + one inner chunk), never the whole shard.

**Hive products are read one leaf at a time**: a leaf zarr (issue #199) is
exactly this layout scoped to one shard — the same ``{group}`` path, the
``morton`` sibling, the versioned ragged attrs, and the whole-leaf
ShardingCodec (one stored span) — so open the leaf store
(``hive.shard_leaf_path``) and pass the same ``field`` path. The readers are
store-scoped and never traverse the hive digit tree (leaf discovery is the
walker's/coverage MOC's job, issue #200).

The reader (generator)
----------------------
:func:`read_tensors` yields ``(tensor, morton_index)`` one chunk at a time.
Per chunk it derives a tail-trimmed z-range from the cells' ``bottom``/``top``
quantiles, anchors a fixed ``n_bins * resolution`` window at the floor of that
range, and rasterizes each cell's digest into per-bin counts via
:func:`zagg.stats.tdigest.cdf_from_tdigest`.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Literal

import numpy as np
import zarr
from zarr.abc.store import Store

from zagg.grids.base import RAGGED_ELEMENT_ATTR, RAGGED_SPEC
from zagg.stats.tdigest import cdf_from_tdigest, quantile_from_tdigest

__all__ = [
    "rasterize_cell",
    "chunk_z_range",
    "read_tensors",
    "read_raw_values",
    "read_locations",
    "read_cell",
]

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


# --------------------------------------------------------------------------- #
# vlen-bytes store access (issue #209)
# --------------------------------------------------------------------------- #


def _open_ragged(store: Store, field: str, zarr_format) -> tuple:
    """Open a ragged vlen array; return ``(arr, elem_dtype, elem_shape, meta)``.

    ``meta`` is the array's :data:`~zagg.grids.base.RAGGED_ELEMENT_ATTR` attrs
    payload — the element interpretation the WRITER declared, which is what the
    readers decode by (never a hardcoded dtype). Raises a pointed error when
    the attrs are missing/malformed: silently guessing an element layout would
    misinterpret every payload (pre-issue-209 CSR stores are a hard break).
    """
    arr = zarr.open_array(store, path=field, mode="r", zarr_format=zarr_format)
    raw_meta = arr.attrs.get(RAGGED_ELEMENT_ATTR)
    meta: dict = dict(raw_meta) if isinstance(raw_meta, dict) else {}
    element = meta.get("element")
    if not isinstance(element, dict) or "dtype" not in element or "shape" not in element:
        raise ValueError(
            f"{field!r} carries no ragged element declaration "
            f'(attrs["{RAGGED_ELEMENT_ATTR}"]["element"]); it is not a zagg ragged '
            f"vlen array (issue #209). Pre-issue-209 CSR stores are not readable "
            f"— rewrite the store."
        )
    # Version gate (the coverage-envelope discipline): an unknown/future spec
    # must fail loudly, never half-parse. This attrs seam is the INTERIM
    # contract the issue #210 typed vlen-array dtype migration supersedes.
    if meta.get("spec") != RAGGED_SPEC:
        raise ValueError(
            f"{field!r} declares ragged spec {meta.get('spec')!r}; this reader "
            f"understands {RAGGED_SPEC!r} only — a newer writer's layout must be "
            f"adopted deliberately, not half-parsed"
        )
    if arr.ndim != 1:
        raise ValueError(
            f"{field!r} has {arr.ndim} dimensions; the t-digest tensor readers "
            f"consume HEALPix products (1-D cells axis)"
        )
    try:
        dtype = np.dtype(str(element["dtype"])).newbyteorder("<")
    except TypeError as e:
        raise ValueError(
            f"{field!r} declares an unreadable element dtype "
            f'{element["dtype"]!r} in attrs["{RAGGED_ELEMENT_ATTR}"]["element"]'
        ) from e
    shape = tuple(int(s) for s in element["shape"])
    return arr, dtype, shape, meta


def _decode_cell(raw, dtype: np.dtype, shape: tuple) -> np.ndarray:
    """One cell's payload from its raw vlen bytes, per the declared element."""
    return np.frombuffer(bytes(raw), dtype=dtype).reshape(shape)


def _stored_chunk_spans(arr) -> list[tuple[int, int]]:
    """Cell spans ``(start, stop)`` of the array's STORED objects, ascending.

    The whole-store read plan: one LIST of the array's ``c/<ordinal>`` data
    keys instead of probing every chunk of a mostly-fill array. Under the
    ShardingCodec an object spans a whole shard (``arr.shards``); on the
    unsharded flat layout it is one read chunk — both derive from the array's
    own metadata, so the sharded and per-inner-chunk layouts share this path.
    ``zarr.core.sync.sync`` runs the store's async listing on zarr's own event
    loop (zarr 3 exposes no public sync listing).
    """
    from zarr.core.sync import sync

    async def _collect(gen):
        return [key async for key in gen]

    span = int((arr.shards or arr.chunks)[0])
    prefix = f"{arr.path}/c/" if arr.path else "c/"
    keys = sync(_collect(arr.store_path.store.list_prefix(prefix)))
    ordinals = sorted(int(k.rsplit("/", 1)[-1]) for k in keys)
    return [(o * span, min((o + 1) * span, int(arr.shape[0]))) for o in ordinals]


def _iter_populated_chunks(arr) -> Iterator[tuple[int, list]]:
    """Yield ``(chunk_start, [(cell_pos, raw_bytes), ...])`` per populated chunk.

    Restricted to the stored objects (:func:`_stored_chunk_spans`), each read
    in ONE slice: slicing per inner chunk would re-fetch a sharded object's
    index suffix on every ``__getitem__`` (~K redundant GETs per shard at the
    production K=256 — review, PR #211), while the full-span slice fetches
    the stored object ONCE (zarr reads a whole outer chunk in a single GET
    and splits it locally). The held cost is one span's decoded payload —
    ~141 MB at the o8 t-digest scale, the same bound the hive write side
    documents and accepts (``process_and_write_hive``), and this is the
    client-side bulk reader. Chunks whose cells are all absent (the ``b""``
    fill) are skipped; ``cell_pos`` is the cell's row-major position within
    the chunk — the same index the writer placed it at.
    """
    cells_per_chunk = int(arr.chunks[0])
    for span_start, span_stop in _stored_chunk_spans(arr):
        span = arr[span_start:span_stop]
        for offset in range(0, span_stop - span_start, cells_per_chunk):
            block = span[offset : offset + cells_per_chunk]
            populated = [(pos, block[pos]) for pos in range(len(block)) if len(block[pos])]
            if populated:
                yield span_start + offset, populated


def _open_morton(store: Store, field: str, zarr_format):
    """The sibling per-cell ``morton`` coordinate array (chunk identity source)."""
    parent, _, _name = field.rpartition("/")
    path = f"{parent}/morton" if parent else "morton"
    try:
        return zarr.open_array(store, path=path, mode="r", zarr_format=zarr_format)
    except (FileNotFoundError, KeyError) as e:
        raise ValueError(
            f"{field!r} has no sibling 'morton' coordinate array at {path!r}; the "
            f"vlen readers derive each chunk's coverage-cell id from the per-cell "
            f"morton coordinate (issue #209)"
        ) from e


def _chunk_word(words: np.ndarray, field: str, start: int) -> int:
    """A read chunk's coverage-cell morton id from its cells' morton words.

    ``words`` are the chunk's per-cell morton coordinates (packed uint64 area
    words at the cell order; ``0`` is the unwritten fill). The chunk id is any
    written cell's word coarsened to the chunk order — ``cell_order -
    log4(cells_per_chunk)`` — the same parent cell the CSR layout named its
    subgroups by.
    """
    from mortie import clip2order

    from zagg.grids.morton import morton_decimal

    written = words[words != 0]
    if written.size == 0:
        raise ValueError(
            f"chunk at cell {start} of {field!r} has ragged payloads but no written "
            f"'morton' coordinate — the dense coordinate write did not cover it"
        )
    decimal = morton_decimal(int(written[0]))
    cell_order = len(decimal) - (2 if decimal.startswith("-") else 1)
    depth = (int(len(words)).bit_length() - 1) // 2  # log4(cells_per_chunk)
    return int(clip2order(cell_order - depth, written[:1])[0])


def _tensor_side(arr, field: str) -> int:
    """Row-major square side of one read chunk (64 for the production configs)."""
    cells_per_chunk = int(arr.chunks[0])
    side = math.isqrt(cells_per_chunk)
    if side * side != cells_per_chunk:
        raise ValueError(
            f"{field!r} read chunk holds {cells_per_chunk} cells — not a square "
            f"block, so it cannot rasterize to a (side, side, n_bins) tensor"
        )
    return side


# --------------------------------------------------------------------------- #
# public readers
# --------------------------------------------------------------------------- #


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

    Sweeps the field's vlen array one read chunk (one square cell block) at a
    time, visiting only the STORED objects. For each populated chunk it trims
    the per-cell tails, derives a fixed z-window (see :func:`chunk_z_range`),
    rasterizes every populated cell's digest into ``n_bins`` counts (see
    :func:`rasterize_cell`), and emits the ``(side, side, n_bins)`` tensor with
    the chunk's coverage-cell morton id (``side`` is 64 for the production
    ``chunk_inner`` configs).

    Parameters
    ----------
    store : Store
        Zarr store holding the ragged vlen array (issue #209 layout).
    field : str
        Array path (e.g. ``"19/h_tdigest"``).
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
        ``tensor`` has shape ``(side, side, n_bins_out)`` and the requested
        dtype; ``morton_index`` is the chunk's coverage-cell morton id.

    Raises
    ------
    ValueError
        On an unknown ``dtype``/``fit``, a store missing the ragged element
        attrs or the ``morton`` sibling, or (with ``fit="raise"``) a chunk
        whose trimmed range overflows the fixed window.
    """
    if dtype not in _TENSOR_DTYPES:
        raise ValueError(f"unknown dtype {dtype!r}; expected one of {sorted(_TENSOR_DTYPES)}")
    out_dtype = _TENSOR_DTYPES[dtype]
    is_float = np.issubdtype(out_dtype, np.floating)

    arr, elem_dtype, elem_shape, _meta = _open_ragged(store, field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    side = _tensor_side(arr, field)

    for start, populated in _iter_populated_chunks(arr):
        cells = [(pos, _decode_cell(raw, elem_dtype, elem_shape)) for pos, raw in populated]
        z_lo, n_bins_c, resolution_c = chunk_z_range(
            [digest for _pos, digest in cells],
            n_bins=n_bins,
            resolution=resolution,
            bottom=bottom,
            top=top,
            fit=fit,
        )

        tensor = np.zeros((side, side, n_bins_c), dtype=out_dtype)
        for cell_id, digest in cells:
            counts = rasterize_cell(digest, z_lo, resolution_c, n_bins_c)
            if not is_float:
                counts = np.rint(counts)
            row, col = divmod(cell_id, side)
            tensor[row, col, :] = counts.astype(out_dtype)

        yield tensor, _chunk_word(morton[start : start + side * side], field, start)


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
        Zarr store holding the ragged vlen array (issue #209 layout).
    field : str
        Array path.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (morton_index, cell_id, values) : (int, int, ndarray)
        ``values`` is the cell's recovered 1-D sample vector (sorted ascending,
        as the digest stores centroids by mean); ``cell_id`` its row-major
        position within the chunk.

    Raises
    ------
    ValueError
        If any cell's digest carries a merged centroid (weight > 1), so the raw
        values cannot be recovered without loss.
    """
    arr, elem_dtype, elem_shape, _meta = _open_ragged(store, field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    cells_per_chunk = int(arr.chunks[0])

    for start, populated in _iter_populated_chunks(arr):
        word = _chunk_word(morton[start : start + cells_per_chunk], field, start)
        for cell_id, raw in populated:
            digest = _decode_cell(raw, elem_dtype, elem_shape)
            weights = np.asarray(digest[:, 1], dtype=np.float64)
            if np.any(weights > 1.0):
                raise ValueError(
                    f"cell {cell_id} (chunk {word}) has merged centroids "
                    "(weight > 1); raw values are not losslessly recoverable"
                )
            yield word, int(cell_id), np.asarray(digest[:, 0], dtype=np.float64)


def read_locations(
    store: Store,
    field: str,
    *,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[int, int, np.ndarray]]:
    """Yield ``(morton_index, cell_id, locations)`` per populated cell.

    The location-channel reader (issue #87): a located ragged field stores a
    per-centroid ``uint64`` morton location vector in a sibling vlen array,
    row-aligned with the digest — one word per centroid, each the deepest
    morton cell enclosing that centroid's member observations (an exact
    order-29 point word for a 1-obs centroid). The sibling is bound by the
    payload array's attrs declaration (``attrs["ragged"]["locations"]`` —
    issue #209), never by reconstructing the naming convention. Decode or
    coarsen the words with mortie (``mort2healpix``, ``clip2order``,
    ``common_ancestor``); geometric queries compose directly on the packed
    words.

    Parameters
    ----------
    store : Store
        Zarr store holding the ragged vlen arrays (issue #209 layout).
    field : str
        The PAYLOAD array's path (not the sibling's).
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
        If the field declares no locations channel (it was not written as a
        located ragged field).
    """
    _arr, _dt, _sh, meta = _open_ragged(store, field, zarr_format)
    sibling = meta.get("locations")
    if not sibling:
        raise ValueError(
            f"{field!r} declares no locations channel "
            f'(attrs["{RAGGED_ELEMENT_ATTR}"]["locations"]); it was not written as '
            f"a located ragged field (declare 'location:' on the field — issue #87)"
        )
    parent, _, _name = field.rpartition("/")
    loc_field = f"{parent}/{sibling}" if parent else str(sibling)
    # Sweep the SIBLING only — skipping the payload array halves the bytes
    # read for a locations-only pass (the digest payload is not consumed here).
    loc_arr, loc_dtype, loc_shape, _loc_meta = _open_ragged(store, loc_field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    cells_per_chunk = int(loc_arr.chunks[0])

    for start, populated in _iter_populated_chunks(loc_arr):
        word = _chunk_word(morton[start : start + cells_per_chunk], loc_field, start)
        for cell_id, raw in populated:
            yield word, int(cell_id), _decode_cell(raw, loc_dtype, loc_shape)


def read_cell(
    store: Store,
    field: str,
    cell: int,
    *,
    zarr_format: Literal[2, 3] = 3,
) -> np.ndarray:
    """Random-access ONE cell's ragged payload, decoded per the element attrs.

    The issue #209 single-cell path: indexing the vlen array reads exactly two
    ranged GETs on a sharded store — the shard-index suffix, then the one inner
    chunk holding the cell — never the whole shard object. ``cell`` is the
    cell's global position on the array's cells axis (no negative-index wrap).
    An absent/empty cell returns the zero-length ``(0, *inner_shape)`` array;
    an out-of-range index raises ``IndexError`` naming the valid range (zarr
    basic selection would silently clamp the slice — review, PR #211). Works
    on any zagg ragged vlen array — a located field's ``{field}_locations``
    sibling included (its elements decode as ``(n,)`` uint64 words).
    """
    arr, elem_dtype, elem_shape, _meta = _open_ragged(store, field, zarr_format)
    cell = int(cell)
    if not 0 <= cell < int(arr.shape[0]):
        raise IndexError(
            f"cell {cell} out of range for {field!r} with {int(arr.shape[0])} cells "
            f"(valid: 0..{int(arr.shape[0]) - 1}; negative indices do not wrap)"
        )
    (raw,) = arr[cell : cell + 1]
    return _decode_cell(raw, elem_dtype, elem_shape)
