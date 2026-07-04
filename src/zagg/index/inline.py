"""The ``inline`` index backend: chunk map built at read time (issue #160, phase 2).

Selection is identical to ``hierarchical`` (the coarse geolocation read +
``plan_read``); addressing goes through a per-granule **chunk map** built on
the fly by walking each dataset's v1 chunk B-tree with h5coro — pure Python,
metadata-only, ~1 ranged GET + tens of ms per granule (the B-trees live in
the front-of-file metadata block NSIDC keeps inside h5coro's first cache
line; measured on PR #159's ``bench/offsets`` route (b), cross-validated
there against h5py and hidefix chunk-for-chunk over 61 granules with zero
mismatches). Planned reads are then issued on chunk boundaries, so every GET
maps to whole stored chunks; the exact planned element ranges are sliced
back out, keeping output row-identical to ``hierarchical``.

The chunk map is also the write-back payload (phase 3): with
``write_back: true`` the per-granule manifest is persisted to the sidecar
store, which is how the store gets populated before a ``sidecar`` backend
can serve it (the issue's deployment progression).

Known h5coro quirk this backend must sidestep: a hyperslice starting exactly
on an interior chunk boundary (``k * chunk_len``, ``k > 0``) trips h5coro's
B-tree start-edge intersection off-by-one (found in PR #152, discussed on
issue #148) — chunk-aligned reads start one element early and trim.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

from zagg.index import VirtualIndex

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkMap:
    """One dataset's chunk table, sorted by first-axis element offset.

    ``elem_start``/``elem_end`` are half-open element ranges along the first
    (photon) axis, one row per **first-axis chunk position** — for an N-D
    dataset whose chunk grid is wider than 1 in a trailing dimension, the
    trailing chunks share a row's element range and ``nbytes`` sums over
    them (ATL03's datasets are 1-wide in every trailing dim, so this is the
    per-chunk table there). ``byte_offset`` is the file offset of the first
    chunk at that position; ``filter_mask`` ORs the HDF5 per-chunk filter
    masks (0 = all pipeline filters applied).
    """

    dataset: str
    elem_start: np.ndarray
    elem_end: np.ndarray
    byte_offset: np.ndarray
    nbytes: np.ndarray
    filter_mask: np.ndarray
    dims: tuple[int, ...]
    chunk_dims: tuple[int, ...]

    def __len__(self) -> int:
        return len(self.elem_start)

    def aligned_cover(self, start: int, end: int) -> tuple[int, int]:
        """Smallest chunk-aligned half-open range covering ``[start, end)``."""
        if not (0 <= start < end <= self.dims[0]):
            raise ValueError(f"range [{start}, {end}) outside dataset extent {self.dims[0]}")
        i0 = max(int(np.searchsorted(self.elem_start, start, side="right")) - 1, 0)
        i1 = min(int(np.searchsorted(self.elem_end, end, side="left")), len(self) - 1)
        return int(self.elem_start[i0]), int(self.elem_end[i1])


def _walk_chunk_btree(ds) -> list[tuple[tuple[int, ...], int, int, int]]:
    """Enumerate every leaf entry of a chunked dataset's v1 chunk B-tree.

    ``ds`` is a metadata-only ``h5coro.h5dataset.H5Dataset``; nodes are read
    through its ``readField``/``readBTreeNodeV1`` (the same field parsing the
    data path's ``readBTreeV1`` uses, minus the chunk reads and hyperslice
    pruning — so the PR #152 start-edge off-by-one is not on this path).
    Ported from the cross-validated ``bench/offsets/extract_offsets.py``
    route (b) (PR #159). Returns ``[(offset_elems, filter_mask, byte_offset,
    nbytes)]`` in B-tree (element) order.
    """
    from h5coro.h5dataset import FatalError, H5Dataset

    ro = ds.resourceObject
    entries: list[tuple[tuple[int, ...], int, int, int]] = []

    def walk(addr: int) -> None:
        ds.pos = addr
        signature = ds.readField(4)
        node_type = ds.readField(1)
        if signature != H5Dataset.H5_TREE_SIGNATURE_LE:
            raise FatalError(f"invalid b-tree signature: 0x{signature:x}")
        if node_type != 1:
            raise FatalError(f"only raw data chunk b-trees supported: {node_type}")
        node_level = ds.readField(1)
        entries_used = ds.readField(2)
        ds.pos += ro.offsetSize * 2  # skip left/right sibling addresses
        curr = ds.readBTreeNodeV1(ds.meta.ndims)
        for _ in range(entries_used):
            child_addr = ds.readField(ro.offsetSize)
            nxt = ds.readBTreeNodeV1(ds.meta.ndims)
            if node_level > 0:
                pos = ds.pos
                walk(child_addr)
                ds.pos = pos
            else:
                # leaf key: element offset per dim + compressed size + filter mask
                entries.append(
                    (tuple(curr["slice"]), curr["filter_mask"], child_addr, curr["chunk_size"])
                )
            curr = nxt

    walk(ds.meta.address)
    return entries


def build_chunk_map(h5obj, path: str) -> ChunkMap:
    """Build a :class:`ChunkMap` for one dataset by walking its metadata.

    Metadata-only: no chunk is ever read or decompressed. A contiguous-layout
    dataset yields a single pseudo-chunk covering the full first axis
    (mirroring h5py's ``get_offset()`` treatment in the bench extractor).

    Raises ``KeyError`` for an absent path (h5coro's ``metaOnly`` traversal
    never raises on its own — it just leaves default metadata) and
    ``ValueError`` for layouts without file-offset storage (compact).
    """
    from h5coro.h5dataset import INVALID_VALUE, H5Dataset

    ds = H5Dataset(h5obj, path, earlyExit=True, metaOnly=True, enableAttributes=False)
    if ds.meta.typeSize == 0:
        raise KeyError(path)
    dims = tuple(int(x) for x in ds.meta.dimensions or ())

    def _empty() -> ChunkMap:
        z = np.empty(0, dtype=np.int64)
        return ChunkMap(path, z, z, z, z, z, dims or (0,), dims or (0,))

    if not dims or 0 in dims:
        return _empty()
    if ds.meta.address == INVALID_VALUE[h5obj.offsetSize]:
        return _empty()  # no allocated storage

    if ds.meta.layout == H5Dataset.CHUNKED_LAYOUT:
        chunk_dims = tuple(int(x) for x in ds.meta.chunkDimensions)
        rows: dict[int, list[int]] = {}  # e0 -> [byte_offset, nbytes, filter_mask]
        for offset_elems, filter_mask, addr, size in _walk_chunk_btree(ds):
            e0 = int(offset_elems[0])
            row = rows.get(e0)
            if row is None:
                rows[e0] = [int(addr), int(size), int(filter_mask)]
            else:
                # Trailing-dim sibling chunk at the same first-axis position:
                # keep the first byte_offset, sum sizes, OR the masks.
                row[1] += int(size)
                row[2] |= int(filter_mask)
        starts = np.array(sorted(rows), dtype=np.int64)
        return ChunkMap(
            dataset=path,
            elem_start=starts,
            elem_end=np.minimum(starts + chunk_dims[0], dims[0]),
            byte_offset=np.array([rows[int(s)][0] for s in starts], dtype=np.int64),
            nbytes=np.array([rows[int(s)][1] for s in starts], dtype=np.int64),
            filter_mask=np.array([rows[int(s)][2] for s in starts], dtype=np.int64),
            dims=dims,
            chunk_dims=chunk_dims,
        )
    if ds.meta.layout == H5Dataset.CONTIGUOUS_LAYOUT:
        return ChunkMap(
            dataset=path,
            elem_start=np.array([0], dtype=np.int64),
            elem_end=np.array([dims[0]], dtype=np.int64),
            byte_offset=np.array([int(ds.meta.address)], dtype=np.int64),
            nbytes=np.array([int(ds.meta.size)], dtype=np.int64),
            filter_mask=np.zeros(1, dtype=np.int64),
            dims=dims,
            chunk_dims=dims,
        )
    # COMPACT data lives inside the object header, not at a file offset.
    raise ValueError(f"{path}: unsupported storage layout {ds.meta.layout!r} for chunk indexing")


class InlineIndex(VirtualIndex):
    """Chunk map computed at read time; chunk-aligned planned reads.

    Never consults the sidecar store — it recomputes every granule every run
    (the no-store-yet mode and, with ``write_back`` in phase 3, the
    store-population mode; see the issue #160 deployment progression).
    """

    name = "inline"

    @classmethod
    def validate_index_config(cls, index_cfg: dict, data_source: dict | None = None) -> None:
        # The chunk map drives *addressing*; selection still needs the coarse
        # spatial index, so the hierarchical read_plan surface is required.
        if data_source is not None:
            rp = data_source.get("read_plan")
            if not (isinstance(rp, dict) and rp.get("spatial_index")):
                raise ValueError(
                    "index backend 'inline' requires data_source.read_plan.spatial_index "
                    "(chunk-aligned addressing plugs into the planned read path)"
                )

    def read_group(self, h5obj, group, data_source, shard_key, grid, arrow=False):
        from zagg.processing.read import _planned_read_group

        levels = data_source.get("levels")
        base_level = data_source.get("base_level")
        rp = data_source.get("read_plan")
        # Same completeness gate as ``_read_group``'s planned route — reject
        # incomplete configurations explicitly rather than degrading silently.
        if not (isinstance(rp, dict) and rp.get("spatial_index")):
            raise ValueError("index backend 'inline' requires data_source.read_plan.spatial_index")
        if not isinstance(levels, dict) or not levels:
            raise ValueError(
                "data_source.read_plan.spatial_index requires a non-empty 'levels' mapping"
            )
        if not base_level:
            raise ValueError("data_source.read_plan.spatial_index requires 'base_level'")
        return _planned_read_group(
            h5obj,
            group,
            data_source,
            shard_key,
            grid,
            arrow=arrow,
            read_fn=self._chunk_aligned_read_fn(h5obj),
        )

    def _chunk_aligned_read_fn(self, h5obj):
        """Build the addressing seam: planned ranges → whole-chunk reads.

        Chunk maps are built lazily per dataset (first planned read of that
        path) and cached for the life of the returned callable — i.e. one
        ``read_group`` call, which is exactly one (granule, group): a group's
        datasets are disjoint from every other group's, so nothing is
        rebuilt or leaked across granules.
        """
        maps: dict[str, ChunkMap] = {}

        def read_fn(path, hyperslice=None):
            if hyperslice is None:
                return h5obj.readDatasets([path])[path]
            cm = maps.get(path)
            if cm is None:
                cm = maps[path] = build_chunk_map(h5obj, path)
            parts = []
            for s, e in hyperslice:
                if len(cm) == 0:
                    # No allocated storage (degenerate file): defer to the
                    # plain read so the error surface matches hierarchical.
                    parts.append(
                        h5obj.readDatasets([{"dataset": path, "hyperslice": [(s, e)]}])[path]
                    )
                    continue
                cs, ce = cm.aligned_cover(s, e)
                # Start one element early when the cover begins on an interior
                # chunk boundary: h5coro's B-tree start-edge intersection
                # drops exactly-aligned starts (PR #152 off-by-one, issue #148
                # thread) — the same workaround the bench extractor ships.
                lo = cs - 1 if cs > 0 else 0
                arr = h5obj.readDatasets([{"dataset": path, "hyperslice": [(lo, ce)]}])[path]
                parts.append(arr[s - lo : e - lo])
            return parts[0] if len(parts) == 1 else np.concatenate(parts)

        return read_fn
