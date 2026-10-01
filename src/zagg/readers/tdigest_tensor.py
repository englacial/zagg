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
    {group}/{field}_times      <- temporal fields only (spec §8.3, issue #410):
                                  the uint64 per-row toc words, row-aligned the
                                  same way. Both shipped templates write it.
    {group}/morton             <- per-cell uint64 morton coordinate (zagg's
                                  standard HEALPix coordinate array)

The element interpretation is self-describing via the array attrs
(``grids.base.RAGGED_ELEMENT_ATTR``)::

    attrs["ragged"] = {"element": {"dtype": "float32", "shape": [-1, 2]},
                       "locations": "<sibling name>"}   # located fields only
    attrs["times"] = "<sibling name>"                   # temporal fields only

so the readers decode what the writer declared rather than hardcoding a dtype,
and bind the location channel by metadata, not naming convention. A store
without these attrs is not a zagg ragged vlen array (pre-issue-209 CSR stores
are a hard break) and raises a pointed error.

**No helper here binds the temporal channel yet.** :func:`read_locations` is the
§9 counterpart and refuses a field by name when it declares no locations
channel; :func:`read_raw_values` decodes a ``(n, 2)`` digest and cannot be
pointed at a flat word vector. Until a ``times`` helper lands, a client decodes
``{field}_times`` directly off its own ``ragged`` attrs and interprets the words
with mortie (``toc_is_range``, ``toc2time``). Recorded here so this layout
description matches what the shipped templates write.

The read plan honors the layout the writer chose: a whole-store sweep LISTs the
array's stored chunk objects (one object per shard under the ShardingCodec, or
one per inner chunk on the unsharded flat layout — both self-describing in the
array metadata, so one code path reads either), then decodes per read chunk.
Each read chunk is a square ``(side, side)`` block of cells
(``side = isqrt(cells_per_chunk)`` — 64 for the production ``chunk_inner``
configs); its coverage-cell morton id is derived from the sibling ``morton``
coordinate. A cell's 2-D position is the **bit deinterleave** of its nested
rank within the chunk (``readers._layout.rank_to_rowcol`` — mortie spec §8 /
gridlook orientation; issue #336), never a row-major reshape: nested order is
a Z-order curve, so ``divmod(rank, side)`` would scramble the block spatially.
Random access to one cell (:func:`read_cell`) indexes the vlen array directly
— 2 ranged GETs on a sharded store (index suffix + one inner chunk), never the
whole shard.

**Hive products are read one leaf at a time**: a leaf zarr (issue #199) is
exactly this layout scoped to one shard — the same ``{group}`` path, the
``morton`` sibling, the versioned ragged attrs, and the whole-leaf
ShardingCodec (one stored span) — so open the leaf store, resolved through
its root stamp (``hive.resolve_leaf``: a versioned leaf's arrays sit under the
version its stamp names, spec §1.5), and pass the same ``field`` path. The readers are
store-scoped and never traverse the hive digit tree (leaf discovery is the
walker's/coverage MOC's job, issue #200).

The reader (generator)
----------------------
:func:`read_tensors` yields ``(tensor, mask, (offset, gain), morton_index)``
— the issue #336 reader contract — one block at a time (a block is one read
chunk by default, or a ``block_order`` subtree of whole chunks). Per block it
derives a tail-trimmed z-range from the cells' ``bottom``/``top`` quantiles,
anchors a fixed ``n_bins * resolution`` window at the floor of that range
(the shared ``(offset, gain)``), rasterizes each cell's digest into per-bin
counts via :func:`zagg.stats.tdigest.cdf_from_tdigest`, and decodes the hive
leaf's ``coverage.moc`` occupancy sidecar (when present) into the uint8
``mask`` channel on the same deinterleaved layout.

All three sweep readers accept ``subtree=`` (issue #351): the nested cells
axis makes "everything below one morton ancestor" a contiguous cell span
(spec §1.5 subtree spans), so a restricted read fetches only the stored
objects the span covers — on a sharded store the index suffix plus the
covering inner chunks, on the flat layout the covering chunk objects.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from itertools import chain, groupby
from typing import Literal

import numpy as np
import zarr
from zarr.abc.store import Store

from zagg.grids.base import RAGGED_ELEMENT_ATTR, RAGGED_SPEC, weights_declaration
from zagg.readers._layout import (
    normalize_subtree,
    rank_to_rowcol,
    rowcol_to_rank,
    subtree_cell_span,
)
from zagg.stats.tdigest import cdf_from_tdigest, quantile_from_tdigest

__all__ = [
    "rasterize_cell",
    "chunk_z_range",
    "read_tensors",
    "has_exact_occupancy",
    "read_raw_values",
    "read_locations",
    "read_cell",
    "cell_index",
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

    The sibling §2.0 ``weights`` declaration is strict-checked here too — the
    reader is the party that spec sentence's MUST is addressed to, and this
    is the seam gating the other spec markers. Both DEFINED values open:
    what the readers bind of flux semantics is issue #426's read-validation
    phase, and this only refuses the undefined ones (a future revision of
    §2.0, which must never be read as either defined value).
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
    weights_declaration(dict(arr.attrs))  # §2.0: refuse an unknown declaration
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


def _iter_populated_chunks(
    arr,
    span: tuple[int, int] | None = None,
    spans: list[tuple[int, int]] | None = None,
) -> Iterator[tuple[int, list]]:
    """Yield ``(chunk_start, [(cell_pos, raw_bytes), ...])`` per populated chunk.

    Restricted to the stored objects (:func:`_stored_chunk_spans`), each read
    in ONE slice: slicing per inner chunk would re-fetch a sharded object's
    index suffix on every ``__getitem__`` (~K redundant GETs per shard at the
    production K=256 — review, PR #211), while the full-span slice fetches
    the stored object ONCE (zarr reads a whole outer chunk in a single GET
    and splits it locally). THIS generator's held cost is one span's decoded
    payload — ~141 MB at the o8 t-digest scale, the same bound the hive write
    side documents and accepts (``process_and_write_hive``), and this is the
    client-side bulk reader. Callers that group the yielded chunks into a
    coarser block hold one BLOCK's payload instead, which is that bound times
    the number of spans the block covers (see :func:`read_tensors`). Chunks whose cells are all absent (the ``b""``
    fill) are skipped; ``cell_pos`` is the cell's chunk-local NESTED RANK —
    the same index the writer placed it at, and the index the readers
    deinterleave to a tensor position (``rank_to_rowcol``, issue #336); it is
    never a row-major position (nested order is a Z-order curve).

    ``span`` (issue #351) restricts the sweep to the stored objects
    overlapping the ``[start, stop)`` cell span, clipped to whole read
    chunks — the spec §1.5 contiguous-slice read plan: only the covering
    portion of each overlapping object is sliced (on a sharded store the
    index suffix plus the covering inner chunks; on the flat layout the
    covering chunk objects), and non-overlapping stored objects are skipped
    without a fetch. A finer-than-chunk restriction is the caller's rank
    filter — this generator always yields whole read chunks.

    ``spans`` passes in an ALREADY-LISTED :func:`_stored_chunk_spans` result,
    so a ``subtree=`` read (whose span :func:`_subtree_span` resolved against
    that same listing) LISTs the array's data keys once instead of twice — the
    only whole-array-scale operation left on a read path framed as "never a
    whole-array sweep", and a paginated LIST per ~1000 objects on the flat
    fullsphere layout (review, PR #357). ``None`` lists here.
    """
    cells_per_chunk = int(arr.chunks[0])
    for span_start, span_stop in _stored_chunk_spans(arr) if spans is None else spans:
        if span is not None:
            span_start = max(span_start, span[0] - span[0] % cells_per_chunk)
            span_stop = min(span_stop, span[1] + -span[1] % cells_per_chunk)
            if span_start >= span_stop:
                continue
        data = arr[span_start:span_stop]
        for offset in range(0, span_stop - span_start, cells_per_chunk):
            block = data[offset : offset + cells_per_chunk]
            populated = [(pos, block[pos]) for pos in range(len(block)) if len(block[pos])]
            if populated:
                yield span_start + offset, populated


def _subtree_span(arr, morton, field, subtree) -> tuple[tuple[int, int] | None, list | None]:
    """Resolve the readers' ``subtree=`` to ``(span, spans)`` on this axis.

    ``span`` is ``None`` = no restriction (no ``subtree`` given); ``(0, 0)`` =
    visit nothing (a disjoint word — :func:`~zagg.readers._layout.subtree_cell_span`
    already warned — or an empty store, which has nothing below any word).
    The anchor is the first stored span's first read chunk of the ``morton``
    coordinate — one small slice, no payload bytes (the dense coordinate
    write covers every chunk of a stored span); its own axis index is passed
    alongside so :func:`~zagg.readers._layout.subtree_cell_span` can CHECK the
    nested-placement identity it derives from. A malformed ``subtree`` raises
    even on an empty store.

    ``spans`` is the :func:`_stored_chunk_spans` listing the span was resolved
    against (``None`` when there was no ``subtree`` and nothing was listed):
    the caller threads it straight into :func:`_iter_populated_chunks` so the
    restricted read LISTs the array once, not twice.
    """
    if subtree is None:
        return None, None
    spans = _stored_chunk_spans(arr)
    if not spans:
        normalize_subtree(subtree)  # malformed still raises; nothing to visit
        return (0, 0), spans
    words = morton[spans[0][0] : spans[0][0] + int(arr.chunks[0])]
    cell_order = _cells_order(words, field, spans[0][0])
    written = int(np.flatnonzero(words)[0])
    span = subtree_cell_span(
        subtree,
        int(words[written]),
        spans[0][0] + written,
        cell_order,
        int(arr.shape[0]),
        field,
        # The disjoint-word warning belongs to the READER's caller: from
        # subtree_cell_span that is here (2), the reader's own frame (3) —
        # a generator, so it runs on the consumer's first next() — and the
        # consumer (4). Anything less points inside zagg.
        stacklevel=4,
    )
    return span, spans


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


def _cells_order(words: np.ndarray, field: str, start: int) -> int:
    """HEALPix order of a chunk's written cell words (the cells-axis order).

    ``words`` are per-cell morton coordinates (packed uint64 area words at the
    cell order; ``0`` is the unwritten fill).
    """
    from zagg.grids.morton import morton_decimal

    written = words[words != 0]
    if written.size == 0:
        raise ValueError(
            f"chunk at cell {start} of {field!r} has ragged payloads but no written "
            f"'morton' coordinate — the dense coordinate write did not cover it"
        )
    decimal = morton_decimal(int(written[0]))
    return len(decimal) - (2 if decimal.startswith("-") else 1)


def _chunk_word(words: np.ndarray, field: str, start: int) -> int:
    """A read chunk's/block's coverage-cell morton id from its cells' words.

    The id is the written cells' common ancestor at ``cell_order -
    log4(len(words))`` — the same parent cell the CSR layout named its
    subgroups by. Any span of a nested-ordered cells axis shares that
    ancestor by construction, INCLUDING a block coarser than the stored
    shard: the block-local index is then still the nested rank
    (``rank_to_xy(R*4**d2 + r, d1+d2) == (X*2**d2 + x, Y*2**d2 + y)``), and
    the axis-length check in :func:`read_tensors` is what rejects a block
    order the axis cannot tile. This raises only when the written cells do
    NOT share the ancestor — the span's ``morton`` coordinate is not
    nested-ordered (not a zagg-written axis), where index arithmetic would
    place cells in the wrong subtree.
    """
    from mortie import clip2order

    written = words[words != 0]
    cell_order = _cells_order(words, field, start)
    order = cell_order - (int(len(words)).bit_length() - 1) // 2  # log4(len)
    ancestors = clip2order(order, written)
    if np.any(ancestors != ancestors[0]):
        raise ValueError(
            f"cells of the block at {start} of {field!r} span more than one "
            f"order-{order} ancestor — the 'morton' coordinate is not in nested "
            f"order over this span, so a cells-axis index does not name a "
            f"position in the block's subtree"
        )
    return int(ancestors[0])


def _read_store_object(store: Store, key: str) -> bytes | None:
    """Raw bytes of one store object (e.g. the ``coverage.moc`` sidecar), or None."""
    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    buf = sync(store.get(key, prototype=default_buffer_prototype()))
    return None if buf is None else buf.to_bytes()


def _coverage_occupancy(store: Store) -> tuple[str, dict, bytes] | None:
    """``(encoding, coverage, sidecar_bytes)`` when the store has EXACT occupancy.

    ``None`` for every store whose mask must degrade to the 2-state
    populated/not channel (the D9 degrade): no commit stamp (every flat
    store), a box-only envelope, or a bitmap envelope whose sidecar object is
    gone. Shared by :func:`_load_occupancy` and :func:`has_exact_occupancy`,
    so the public predicate cannot drift from the mask the reader builds.
    """
    from zagg import hive

    coverage = hive.read_coverage(store)
    if not coverage:
        return None
    encoding = coverage.get("encoding")
    if encoding == "full":
        return "full", coverage, b""  # a full subtree needs no sidecar (D14)
    if encoding != "bitmap" or not coverage.get("sidecar"):
        return None
    payload = _read_store_object(store, str(coverage["sidecar"]))
    if payload is None:
        return None
    return "bitmap", coverage, payload


def _load_occupancy(store: Store, arr, words: np.ndarray, field: str) -> tuple | None:
    """The leaf's exact cell occupancy for the mask channel, or ``None``.

    Reads the hive leaf's coverage envelope (the commit stamp) and its
    ``coverage.moc`` occupancy sidecar (issue #200) — one small GET, no digest
    bytes. Returns ``("full", None)`` for a fully occupied subtree (issue
    #246, D14), ``("bitmap", sorted_words)`` with the decoded occupied cell
    words (:func:`zagg.hive.decode_coverage_bitmap` — the frozen bitmap
    convention is reused, never reimplemented), or ``None`` when the store
    carries no exact occupancy (no stamp — every flat store — or a box-only
    envelope, or a missing sidecar; the D9 degrade). A PRESENT-but-corrupt
    sidecar raises (see ``decode_coverage_bitmap``).

    ``words`` are a populated chunk's written morton words: the shard id is
    their ancestor at ``cell_order - log4(n_cells)`` — a leaf's cells axis is
    exactly one shard subtree, which is also what binds the bitmap's bit
    positions to the axis.
    """
    from zagg import hive

    found = _coverage_occupancy(store)
    if found is None:
        return None
    encoding, coverage, payload = found
    if encoding == "full":
        return "full", None
    cell_order = _cells_order(words, field, 0)
    if int(coverage.get("cell_order", -1)) != cell_order:
        raise ValueError(
            f"{field!r} coverage envelope measured occupancy at order "
            f"{coverage.get('cell_order')} but the cells axis is order {cell_order} — "
            f"the occupancy bitmap cannot be aligned to the tensor cells"
        )
    n = int(arr.shape[0])
    depth = (n.bit_length() - 1) // 2
    if 4**depth != n:
        raise ValueError(
            f"{field!r} carries a leaf coverage stamp but its {n}-cell axis is not a "
            f"power-of-four shard subtree — the occupancy bitmap cannot be bound to "
            f"a shard"
        )
    from mortie import clip2order

    written = words[words != 0]
    shard = int(clip2order(cell_order - depth, written[:1])[0])
    return "bitmap", hive.decode_coverage_bitmap(payload, shard, cell_order)


def _block_mask(words: np.ndarray, occupancy: tuple | None, block_depth: int) -> np.ndarray:
    """The block's ``(side, side)`` uint8 occupancy base (values 0/1).

    ``1`` marks a cell the leaf's occupancy sidecar records as observed;
    the caller upgrades digest-bearing cells to ``2``. Without occupancy truth
    the base stays ``0`` everywhere: the mask degrades to the 2-state
    populated/not channel, where ``0`` means "no stored digest" and asserts
    NOTHING about whether the cell was observed. That degrade is not visible
    in the mask itself — :func:`has_exact_occupancy` is the discriminator.
    """
    side = 1 << block_depth
    mask = np.zeros((side, side), dtype=np.uint8)
    if occupancy is None:
        return mask
    kind, occupied = occupancy
    if kind == "full":
        mask[:] = 1
        return mask
    if occupied.size:
        idx = np.searchsorted(occupied, words)
        hit = (occupied[np.minimum(idx, occupied.size - 1)] == words) & (words != 0)
        rows, cols = rank_to_rowcol(np.flatnonzero(hit), block_depth)
        mask[rows, cols] = 1
    return mask


def _tensor_side(arr, field: str) -> tuple[int, int]:
    """``(side, depth)`` of one read chunk's square block (64, 6 in production).

    ``side = 2**depth``: the deinterleave (mortie spec §8) is defined over
    power-of-four subtrees, so a chunk that is not one cannot form a spatially
    faithful ``(side, side)`` block.
    """
    cells_per_chunk = int(arr.chunks[0])
    side = math.isqrt(cells_per_chunk)
    if side * side != cells_per_chunk or side & (side - 1):
        raise ValueError(
            f"{field!r} read chunk holds {cells_per_chunk} cells — not a power-of-"
            f"four subtree, so it cannot deinterleave to a (side, side, n_bins) tensor"
        )
    return side, side.bit_length() - 1


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
    block_order: int | None = None,
    subtree: int | str | None = None,
    max_block_bytes: int = 2 * 1024**3,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[np.ndarray, np.ndarray, tuple[float, float], int]]:
    """Yield ``(tensor, mask, (offset, gain), morton_index)`` per coverage block.

    The ratified reader contract (issue #336): one tuple per populated block
    of a t-digest field.

    Sweeps the field's vlen array one read chunk (one square cell block) at a
    time, visiting only the STORED objects. For each populated block it trims
    the per-cell tails, derives a fixed z-window (see :func:`chunk_z_range`),
    rasterizes every populated cell's digest into ``n_bins`` counts (see
    :func:`rasterize_cell`), and emits the ``(side, side, n_bins)`` tensor with
    the block's coverage-cell morton id (``side`` is 64 for the production
    ``chunk_inner`` configs at the default per-chunk block). Cell placement is
    the spatially faithful bit deinterleave of the cell's nested rank
    (issue #336): ``row, col = rank_to_rowcol(rank, depth)`` per the
    :mod:`zagg.readers._layout` orientation contract (mortie spec §8; row 0 /
    col 0 at the block subtree's south corner, gridlook texture convention).

    ``block_order`` assembles the ``4**(chunk_order - block_order)`` read
    chunks of one block-order subtree into a single
    ``(2**d, 2**d, n_bins)`` tensor (``d = cell_order - block_order``) — e.g.
    an order-12 block on the production geometry (order-19 cells, 64×64
    chunks) is a 128×128 tensor assembled from 4 inner chunks. The z-window
    and the ``fit`` policy are reconciled **block-wide**: one shared
    offset/gain per block, with degrade/collapse decisions made over all of
    the block's populated cells, not per chunk.

    **The memory bound is per BLOCK, not per stored object.** Two terms are
    held while a block is assembled: (1) the block's decoded digests — the
    block-wide z-window needs all of them before any cell can be rasterized —
    which is ``4**(span_order - block_order)`` times one span's payload
    (~141 MB at the o8 t-digest scale, :func:`_iter_populated_chunks`); and
    (2) the emitted ``4**block_depth * n_bins`` tensor, which grows 4× per
    coarser order — an order-6 block on the production order-19 geometry is a
    34 TB allocation. ``max_block_bytes`` (2 GiB default) refuses term (2)
    with a pointed error naming the size instead of dying in the allocator;
    it is checked against the REQUESTED ``n_bins`` (``fit`` only ever shrinks
    it) and bounds the tensor only — term (1) follows the block order asked
    for. Raise it deliberately when a large block really is intended.

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
        On a field declaring ``weights: "flux"`` (spec §2.0) the weights are
        positive reals, not counts, so an integer dtype rounds each bin's
        photoelectron estimate away — pass ``"float32"`` for a flux field.
        (Rasterization does not otherwise bind flux semantics yet; the flux
        read path is issue #426.)
    block_order : int, optional
        HEALPix order of the emitted blocks (default ``None`` — one block per
        read chunk). Must be at or coarser than the chunk order; a block is
        assembled from whole read chunks with one shared z-window. With a
        ``subtree`` the floor is the order of the span actually VISITED — the
        subtree CLIPPED to this axis — so the composed bound is
        ``max(subtree_order, axis_root_order) <= block_order <= chunk_order``:
        blocks tile the visited span, and a word above the axis root (which
        clips to the whole axis) takes the ROOT's order as its floor, not its
        own.
    subtree : int or str, optional
        Restrict the sweep to the read chunks below this morton ancestor —
        packed area word or decimal string, as in :func:`read_raw_values`
        (issue #351; spec §1.5): only stored objects overlapping the
        subtree's cell span are fetched. The subtree must be at or coarser
        than the READ-CHUNK order — a finer word raises, pointing at the
        per-cell readers, because a sub-chunk block would re-derive its
        z-window from fewer cells and stop being a slice of the chunk tensor
        (the ratified v1 refusal; recover a sub-chunk region client-side as
        a corner slice of the chunk tensor instead, keeping its shared
        ``(offset, gain)``). A well-formed word disjoint from this axis
        warns at most once per call (naming the word and the axis root) and
        yields nothing — so an **empty yield is ambiguous** between
        "in-domain, nothing stored" and "outside the domain"; the warning is
        the only discriminator, and whether it is DELIVERED is the caller's
        ``warnings`` filter's call (the default action dedups a repeat of the
        same warning in the same process). Malformed / too-deep words raise
        ``ValueError``.
    max_block_bytes : int, optional
        Refuse a block whose emitted tensor would exceed this many bytes
        (default 2 GiB) — the guard on the ``block_order`` footgun. Raise it
        explicitly to allow a bigger block.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (tensor, mask, (offset, gain), morton_index) : (ndarray, ndarray, tuple, int)
        ``tensor`` has shape ``(side, side, n_bins_out)`` and the requested
        dtype. ``mask`` is the block's ``(side, side)`` uint8 occupancy
        channel: ``0`` = unobserved, ``1`` = observed but no stored digest,
        ``2`` = observed with data (a stored digest — the cells the tensor
        rasterizes). The observed states come from the hive leaf's
        ``coverage.moc`` occupancy sidecar (issue #200), read WITHOUT
        touching digest bytes; a store without one (every flat store)
        degrades to the 2-state ``{0, 2}`` populated/not mask, where ``0``
        means only "no stored digest" and does NOT assert that the cell was
        unobserved. **The yielded mask does not say which regime it is in** —
        a degraded mask and a 3-state mask over a block with no
        observed-but-empty cell are both ``{0, 2}`` — so a consumer keying on
        the 3-state semantics (``mask == 1``, the issue #334 noise stratum)
        must check :func:`has_exact_occupancy` first, or it reads an empty
        stratum on a degraded store as a genuine absence. Today a pre-#334
        leaf's occupancy equals its digest coverage, so ``1`` does not occur;
        once the issue #334 signal strata land, noise-occupied cells
        (observed, no signal digest) appear as ``1`` automatically — the
        upgrade is data-driven, not a reader flag. ``(offset, gain)`` is
        the block's shared z-window ``(z_lo, resolution)``: bin ``i`` of
        every cell in the block covers ``[offset + i*gain, offset +
        (i+1)*gain)``. ``morton_index`` is the block's coverage-cell morton
        id.

    Raises
    ------
    ValueError
        On an unknown ``dtype``/``fit``, a store missing the ragged element
        attrs or the ``morton`` sibling, a payload declaring a §2.0
        ``weights`` value this zagg does not define, an out-of-range
        ``block_order``, a
        block tensor over ``max_block_bytes``, a corrupt/misaligned occupancy
        sidecar, a ``subtree`` finer than the read chunks (or malformed /
        too-deep), or (with ``fit="raise"``) a block whose trimmed range
        overflows the fixed window.
    """
    if dtype not in _TENSOR_DTYPES:
        raise ValueError(f"unknown dtype {dtype!r}; expected one of {sorted(_TENSOR_DTYPES)}")
    out_dtype = _TENSOR_DTYPES[dtype]
    is_float = np.issubdtype(out_dtype, np.floating)

    arr, elem_dtype, elem_shape, _meta = _open_ragged(store, field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    side, depth = _tensor_side(arr, field)
    cells_per_chunk = side * side

    span, spans = _subtree_span(arr, morton, field, subtree)
    if span is not None and span != (0, 0) and span[1] - span[0] < cells_per_chunk:
        # The ratified issue #351 v1 refusal: the store's smallest fetch unit
        # is the inner chunk (no I/O win), and a sub-chunk block would
        # re-derive its z-window from fewer cells — NOT a slice of the chunk
        # tensor, breaking the subtree ≡ filtered-sweep equality contract.
        raise ValueError(
            f"subtree {subtree!r} is finer than {field!r}'s read chunks "
            f"({cells_per_chunk} cells): read_tensors emits whole-chunk blocks "
            f"only; use read_raw_values / read_locations / read_cell, which "
            f"accept any subtree order down to a single cell"
        )
    chunks = _iter_populated_chunks(arr, span, spans)
    first = next(chunks, None)
    if first is None:
        return
    first_words = morton[first[0] : first[0] + cells_per_chunk]
    occupancy = _load_occupancy(store, arr, first_words, field)
    if block_order is None:
        block_cells, block_depth = cells_per_chunk, depth
    else:
        # The cells-axis order comes from the first populated chunk's words;
        # the block subtree must be whole read chunks (coarser or equal).
        cell_order = _cells_order(first_words, field, first[0])
        chunk_order = cell_order - depth
        min_order = 0
        if span is not None:
            # Blocks must tile INSIDE the subtree span (subtree_order ≤
            # block_order — issue #351): a coarser block would reach past the
            # span and label a partial assembly with the bigger block's word.
            min_order = cell_order - ((span[1] - span[0]).bit_length() - 1) // 2
        if not min_order <= int(block_order) <= chunk_order:
            raise ValueError(
                f"block_order {block_order} is out of range: a block assembles whole "
                f"read chunks within the visited span, so it must be between "
                f"{min_order} and the chunk order {chunk_order} "
                f"(block_order=None reads per chunk)"
            )
        block_depth = cell_order - int(block_order)
        block_cells = 4**block_depth
        if int(arr.shape[0]) % block_cells:
            raise ValueError(
                f"{field!r} has {int(arr.shape[0])} cells — not a whole number of "
                f"order-{block_order} blocks ({block_cells} cells each), so the cells "
                f"axis cannot assemble at this block order"
            )
    block_side = 1 << block_depth
    # Bound the emitted tensor BEFORE allocating it: 4**block_depth grows 4× per
    # coarser block order, so an unbounded block_order dies in the allocator
    # (34 TB at order 6 on the production order-19 geometry) with a bare
    # MemoryError instead of naming its cause. n_bins is the requested count —
    # the fit policy only ever shrinks it, so this is the upper bound.
    block_bytes = 4**block_depth * int(n_bins) * out_dtype.itemsize
    if block_bytes > int(max_block_bytes):
        asked = (
            f"block_order={block_order}" if block_order is not None else "the read chunk geometry"
        )
        raise ValueError(
            f"{asked} emits a {block_side}×{block_side}×{n_bins} {dtype} block tensor "
            f"= {block_bytes} bytes ({block_bytes / 1024**3:.2f} GiB), over the "
            f"{int(max_block_bytes)}-byte max_block_bytes limit; use a finer "
            f"block_order, fewer n_bins, or raise max_block_bytes deliberately"
        )

    for block, group in groupby(chain([first], chunks), key=lambda c: c[0] // block_cells):
        bstart = block * block_cells
        cells = [
            (start - bstart + pos, _decode_cell(raw, elem_dtype, elem_shape))
            for start, populated in group
            for pos, raw in populated
        ]
        z_lo, n_bins_c, resolution_c = chunk_z_range(
            [digest for _rank, digest in cells],
            n_bins=n_bins,
            resolution=resolution,
            bottom=bottom,
            top=top,
            fit=fit,
        )

        words = morton[bstart : bstart + block_cells]
        tensor = np.zeros((block_side, block_side, n_bins_c), dtype=out_dtype)
        mask = _block_mask(words, occupancy, block_depth)
        for rank, digest in cells:
            counts = rasterize_cell(digest, z_lo, resolution_c, n_bins_c)
            if not is_float:
                counts = np.rint(counts)
            row, col = rank_to_rowcol(rank, block_depth)
            tensor[row, col, :] = counts.astype(out_dtype)
            mask[row, col] = 2

        yield tensor, mask, (float(z_lo), float(resolution_c)), _chunk_word(words, field, bstart)


def has_exact_occupancy(store: Store) -> bool:
    """Whether this store's :func:`read_tensors` mask is the 3-state channel.

    The mask's two regimes are indistinguishable from the yielded array (a
    degraded mask is ``{0, 2}``, and so is a 3-state mask over a block with no
    observed-but-empty cell), so this is the discriminator — call it once per
    store before keying on the mask's semantics:

    - ``True`` — the leaf carries exact ``coverage.moc`` occupancy (issue
      #200), so ``0`` really means *unobserved* and ``1`` (observed, no stored
      digest — the issue #334 noise stratum) is reported wherever it occurs.
    - ``False`` — no exact occupancy (no commit stamp, i.e. every flat store;
      a box-only envelope; or a bitmap envelope whose sidecar is gone), the
      mask degrades to 2-state populated/not, and ``0`` means only "no stored
      digest here". Reading ``mask == 1`` as "no noise-occupied cells" would
      be a false negative.

    One attrs read plus (for a bitmap envelope) the small sidecar object; no
    digest bytes. Shares :func:`_coverage_occupancy` with the mask build, so
    it cannot report a regime the reader does not produce.
    """
    return _coverage_occupancy(store) is not None


def read_raw_values(
    store: Store,
    field: str,
    *,
    subtree: int | str | None = None,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[int, tuple[int, int], np.ndarray]]:
    """Yield ``(morton_index, (row, col), values)`` raw samples per populated cell.

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
    subtree : int or str, optional
        Restrict the sweep to the cells below this morton ancestor — a
        packed area word (``int``) or its decimal string (``str``), at ANY
        order down to a single cell (issue #351; the spec §1.5 subtree-span
        identity). Only stored objects overlapping the subtree's contiguous
        cell span are fetched; a finer-than-chunk subtree filters ranks
        inside its one covering read chunk. A well-formed word disjoint
        from this axis warns at most once per call (naming the word and the
        axis root) and yields nothing — so an **empty yield is ambiguous**
        between "in-domain, nothing stored" and "outside the domain"; the
        warning is the only discriminator, and its DELIVERY is the caller's
        ``warnings`` filter's call (the default action dedups a repeat of the
        same warning in the same process). A malformed word, or one deeper
        than the cells-axis order, raises :class:`ValueError`.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (morton_index, (row, col), values) : (int, (int, int), ndarray)
        ``values`` is the cell's recovered 1-D sample vector (sorted ascending,
        as the digest stores centroids by mean); ``(row, col)`` its
        deinterleaved 2-D position within the chunk — the same tensor position
        :func:`read_tensors` places the cell at (issue #336).
        :func:`zagg.readers._layout.rowcol_to_rank` inverts it to the
        **chunk-local** rank; for a :func:`read_cell` key (a GLOBAL cells-axis
        index) compose through :func:`cell_index`, which adds the chunk's
        start offset.

    Raises
    ------
    ValueError
        If any swept cell's digest carries a merged centroid (weight > 1), so
        the raw values cannot be recovered without loss; or on a malformed /
        too-deep ``subtree``.
    """
    arr, elem_dtype, elem_shape, _meta = _open_ragged(store, field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    side, depth = _tensor_side(arr, field)
    cells_per_chunk = side * side

    span, spans = _subtree_span(arr, morton, field, subtree)
    for start, populated in _iter_populated_chunks(arr, span, spans):
        word = _chunk_word(morton[start : start + cells_per_chunk], field, start)
        for rank, raw in populated:
            if span is not None and not span[0] <= start + rank < span[1]:
                continue
            digest = _decode_cell(raw, elem_dtype, elem_shape)
            weights = np.asarray(digest[:, 1], dtype=np.float64)
            if np.any(weights > 1.0):
                raise ValueError(
                    f"cell rank {rank} (chunk {word}) has merged centroids "
                    "(weight > 1); raw values are not losslessly recoverable"
                )
            row, col = rank_to_rowcol(rank, depth)
            yield word, (int(row), int(col)), np.asarray(digest[:, 0], dtype=np.float64)


def read_locations(
    store: Store,
    field: str,
    *,
    subtree: int | str | None = None,
    zarr_format: Literal[2, 3] = 3,
) -> Iterator[tuple[int, tuple[int, int], np.ndarray]]:
    """Yield ``(morton_index, (row, col), locations)`` per populated cell.

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
    subtree : int or str, optional
        Restrict the sweep to the cells below this morton ancestor, at any
        order down to a single cell — packed area word or decimal string,
        exactly as in :func:`read_raw_values` (issue #351). Same caveat: a
        well-formed word disjoint from this axis warns at most once per call
        (delivery subject to the caller's ``warnings`` filter) and yields
        nothing, so an **empty yield is ambiguous** between "in-domain,
        nothing stored" and "outside the domain" — the warning is the only
        discriminator; malformed / too-deep words raise :class:`ValueError`.
    zarr_format : int, optional
        Zarr format version (default 3).

    Yields
    ------
    (morton_index, (row, col), locations) : (int, (int, int), ndarray)
        ``locations`` is the cell's ``(k,)`` uint64 per-centroid location
        vector, aligned with the digest rows :func:`read_tensors` /
        :func:`read_raw_values` see for the same cell; ``(row, col)`` is the
        cell's deinterleaved 2-D position within the chunk (issue #336),
        matching the other readers' reporting.

    Raises
    ------
    ValueError
        If the field declares no locations channel (it was not written as a
        located ragged field), or on a malformed / too-deep ``subtree``.
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
    side, depth = _tensor_side(loc_arr, loc_field)
    cells_per_chunk = side * side

    span, spans = _subtree_span(loc_arr, morton, loc_field, subtree)
    for start, populated in _iter_populated_chunks(loc_arr, span, spans):
        word = _chunk_word(morton[start : start + cells_per_chunk], loc_field, start)
        for rank, raw in populated:
            if span is not None and not span[0] <= start + rank < span[1]:
                continue
            row, col = rank_to_rowcol(rank, depth)
            yield word, (int(row), int(col)), _decode_cell(raw, loc_dtype, loc_shape)


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


def cell_index(
    store: Store,
    field: str,
    morton_index: int,
    row: int,
    col: int,
    *,
    zarr_format: Literal[2, 3] = 3,
) -> int:
    """Global cells-axis index of a reported ``(row, col)`` — the :func:`read_cell` key.

    The sweep readers report a chunk-local position:
    :func:`zagg.readers._layout.rowcol_to_rank` inverts the deinterleave back
    to a rank ``0..4**depth - 1`` **within the chunk**, while
    :func:`read_cell` addresses the array's GLOBAL cells axis — so the
    chunk's own start offset is the missing term, and feeding a bare rank to
    :func:`read_cell` silently reads the wrong cell (it is always in range).
    This resolves the offset from the sibling ``morton`` coordinate and
    returns ``chunk_start + rowcol_to_rank(row, col, depth)``.

    ``morton_index`` is a READ-CHUNK id — the id :func:`read_raw_values`,
    :func:`read_locations`, and the default (per-chunk) :func:`read_tensors`
    report. A coarser ``block_order`` block id names no single chunk and
    raises. Only the array's STORED spans are searched (the populated chunks
    the sweep readers yield from), one small slice of the ``morton``
    coordinate per span — never the whole axis, and no digest bytes.

    Raises
    ------
    ValueError
        If ``row``/``col`` are outside the chunk's ``(side, side)`` block, or
        no stored chunk carries ``morton_index``.
    """
    arr, _dt, _sh, _meta = _open_ragged(store, field, zarr_format)
    morton = _open_morton(store, field, zarr_format)
    side, depth = _tensor_side(arr, field)
    cells_per_chunk = side * side
    if not (0 <= int(row) < side and 0 <= int(col) < side):
        raise ValueError(f"({row}, {col}) is outside {field!r}'s ({side}, {side}) read-chunk block")
    rank = int(rowcol_to_rank(int(row), int(col), depth))
    target = int(morton_index)
    for span_start, span_stop in _stored_chunk_spans(arr):
        span_words = morton[span_start:span_stop]
        for offset in range(0, span_stop - span_start, cells_per_chunk):
            words = span_words[offset : offset + cells_per_chunk]
            start = span_start + offset
            if not np.any(words) or _chunk_word(words, field, start) != target:
                continue
            return start + rank
    raise ValueError(
        f"no stored read chunk of {field!r} carries morton id {target} — "
        f"cell_index resolves the READ-CHUNK ids the sweep readers report "
        f"(a coarser block_order block id names no single chunk)"
    )
