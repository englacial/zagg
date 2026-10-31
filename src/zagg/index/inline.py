"""The ``inline`` index backend: chunk map built at read time (issue #160, phase 2).

Selection is identical to ``hierarchical`` (the coarse geolocation read +
``plan_read``); addressing goes through a per-granule **chunk map** built on
the fly by walking each dataset's v1 chunk B-tree with h5coro — pure Python,
metadata-only, ~1 ranged GET + tens of ms per granule (the B-trees live in
the front-of-file metadata block NSIDC keeps inside h5coro's first cache
line; measured on PR #159's ``bench/offsets`` route (b), cross-validated
there against h5py and hidefix chunk-for-chunk over 61 granules with zero
mismatches). Decode goes through the compiled h5coro-hidefix reader (issue
#170): the chunk map reconstructs a per-dataset in-memory ``Index`` whose
``read_from_buffers`` inflates exactly the covering chunks, byte-identical
to h5py/h5coro, with the GIL released. Both read routes are served — the
planned (chunk-aligned hyperslice) route for sources with
``read_plan.spatial_index``, and the full-read route for read-plan-less
(flat) sources — so this backend is the package default for every data
source. Datasets the compiled reader cannot serve degrade to the h5coro
decoder per dataset, where the chunk map still lets reads that start
exactly on an interior chunk boundary be detected and shifted one element
early (see below).

The chunk map is also the write-back payload: with ``write_back: true``
(opt-in — issue #160 Q2) plus a ``store``, every granule's accumulated chunk
maps are persisted as a granule-keyed parquet manifest
(``<store>/<granule_id>.parquet``, the PR #159 offsets schema plus the
per-dataset decode metadata) after its last group is read. That is how the
sidecar store gets populated before a ``sidecar`` backend can serve it (the
issue's deployment progression). Coverage defaults to **full** (issue #190):
the manifest carries chunk maps for *every* decodable dataset in the granule,
enumerated from the same front-of-file metadata the read already cached (no
extra chunk GETs), so one build serves any downstream config — the #163 store
design intent. ``write_back_coverage: lazy`` restores the old behavior
(persist only the datasets this run's planned reads touched) for a private
run that wants the smaller manifest.

Known h5coro quirk this backend must sidestep: a hyperslice starting exactly
on an interior chunk boundary (``k * chunk_len``, ``k > 0``) trips h5coro's
B-tree start-edge intersection off-by-one (found in PR #152, discussed on
issue #148) — chunk-aligned reads start one element early and trim.
"""

from __future__ import annotations

import io
import json
import logging
import math
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from urllib.parse import urlsplit

import numpy as np
import pandas as pd

from zagg.index import VirtualIndex

logger = logging.getLogger(__name__)


def _granule_id(resource) -> str:
    """Granule id from a URL / resource path: basename minus extension.

    The write-back manifest **store key** convention (issue #160). Mirrors
    ``_granule_id`` in h5coro-hidefix's ``zagg_backend``. Deliberately NOT the
    in-memory ``_pending`` key: two distinct granules can share a basename
    under different prefixes, so the stem would collapse them and interleave
    chunk maps under ``granule_workers > 1`` (issue #180 review finding) —
    ``_pending`` keys on the full resource URL instead.
    """
    return PurePosixPath(urlsplit(str(resource)).path).stem


#: Manifest row schema, single source of truth (same empty-frame-drift
#: rationale as the bench extractor's ``OFFSETS_DTYPES``). The chunk columns
#: are PR #159's offsets schema; ``chunk_offset``/``dtype``/``shape``/
#: ``chunk_shape``/``gzip``/``shuffle`` are the per-dataset decode metadata
#: the ``sidecar`` consumer's ``Index.from_chunks`` reconstruction needs
#: (tuples as JSON so the parquet stays flat and self-describing). Contract
#: pins (espg decisions relayed on PR #163): ``dtype`` is the byte-order-
#: explicit ``np.dtype(...).str`` form (``<f4``, ``|i1`` -- never bare
#: ``float32``), and ``gzip`` stays a *boolean* (filter presence; the deflate
#: level is invisible to h5coro's metadata parse and irrelevant for decode --
#: the binding maps the bool).
MANIFEST_DTYPES = {
    "dataset": "object",
    "chunk_idx": "int64",
    "elem_start": "int64",
    "elem_end": "int64",
    "byte_offset": "int64",
    "nbytes": "int64",
    "filter_mask": "int64",
    "chunk_offset": "object",
    "dtype": "object",
    "shape": "object",
    "chunk_shape": "object",
    "gzip": "bool",
    "shuffle": "bool",
}


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
    # Decode metadata + the uncollapsed per-chunk entries
    # ``(offset_elems, filter_mask, byte_offset, nbytes)`` — the write-back
    # manifest is built from these so N-D trailing chunks keep their own
    # addresses (the first-axis table above collapses them).
    dtype: str = ""
    gzip: bool = False
    shuffle: bool = False
    raw: tuple = field(default=(), repr=False)

    def __len__(self) -> int:
        return len(self.elem_start)

    def starts_on_boundary(self, elem: int) -> bool:
        """True when ``elem`` is exactly a chunk's first-axis start offset."""
        i = int(np.searchsorted(self.elem_start, elem))
        return i < len(self) and int(self.elem_start[i]) == elem


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


def _cache_keys(h5obj) -> set:
    """Snapshot of ``h5obj.cache``'s keys (empty for cache-less handles).

    ``set(dict)`` is a single C-level copy, so it cannot observe a concurrent
    insertion mid-iteration the way a Python-level comprehension can (review
    finding on PR #462).
    """
    return set(getattr(h5obj, "cache", None) or ())


def _drop_cache_lines(h5obj, keys) -> int:
    """Pop ``keys`` from ``h5obj.cache`` **and** their ``cache_locks`` entries.

    ``H5Coro.cache_locks`` is a ``defaultdict(threading.Lock)`` keyed by the
    same aligned offsets, so evicting the line alone still retains a ``Lock``
    plus a dict slot for every line ever touched — ~13.8k entries for a GEDI
    L1B beam group's ``rxwaveform`` B-tree, ~110k (order 15-20 MB) per granule
    (review finding on PR #462). Pruning it is safe only because every eviction
    point is single-threaded on the handle (immediate: ``_prebuild_group_maps``,
    the full-coverage walk, direct use; deferred: after the read pool joined —
    :func:`_evict_deferred_lines`). Were another thread mid-``ioRequest``, it
    could hold the popped lock while a third thread took the fresh defaultdict
    entry — two locks for one line, and a duplicate fetch. ``pop(k, None)`` on
    both tables (``cache_locks`` is absent on stub handles) keeps this
    non-raising. Returns the number of cache lines actually dropped.
    """
    cache = h5obj.cache
    locks = getattr(h5obj, "cache_locks", None)
    dropped = 0
    for k in keys:
        if cache.pop(k, None) is not None:
            dropped += 1
        if locks is not None:
            locks.pop(k, None)
    return dropped


def _evict_new_cache_lines(h5obj, before: set) -> int:
    """Drop every h5coro cache line added since ``before`` was snapshotted.

    The snapshot-and-evict half of issue #460: ``H5Coro.cache`` retains a full
    ``cacheLineSize`` line (4 MiB default) for every byte range a cached
    ``ioRequest`` touches, and releases nothing until granule close. A chunk
    B-tree walk reads each node exactly once, but on a sparse file the nodes
    rarely share a line (GEDI L1B ``rxwaveform``: 13,790 chunks → +531 MB
    retained per beam group, ~4.2 GB per granule), so the walk's lines are
    pure dead weight afterwards. Evicting them caps residency at one walk's
    transient peak; a later read that does want an evicted line just
    re-fetches it (B-tree nodes are not data, so in practice none does).
    Only keys absent from ``before`` are dropped — pre-existing lines (the
    front-of-file metadata block every parse shares) always survive, and each
    dropped line takes its ``cache_locks`` entry with it
    (:func:`_drop_cache_lines`). Returns the number of lines evicted.

    Structurally non-raising, which matters because callers run it from a
    ``finally`` where a raise would replace a successful ``ChunkMap`` return
    with a housekeeping error (review finding on PR #462): ``list(cache)``
    snapshots the keys in one C-level copy instead of iterating the live dict
    (a Python-level comprehension can hit ``RuntimeError: dictionary changed
    size during iteration``), and ``pop(k, None)`` cannot ``KeyError`` on a
    line another thread already dropped. Both operations complete without
    releasing the GIL, so no blanket ``except`` is needed here.
    """
    cache = getattr(h5obj, "cache", None)
    if not cache:
        return 0
    added = [k for k in list(cache) if k not in before]
    return _drop_cache_lines(h5obj, added)


def _evict_deferred_lines(h5obj, deferred: set) -> int:
    """Drop the cache lines a pooled ``read_fn`` fan-out deferred (issue #460).

    :func:`build_chunk_map`'s immediate eviction is only safe on a handle no
    other thread is reading: ``read_fn``'s lazy map builds run inside
    ``_read_paths_pooled``'s thread pool (``data_source.read_workers``,
    default 8, one shared ``h5obj`` — both the planned and the vlen route fan
    out that way), and h5coro's cached ``ioRequest`` reads ``self.cache``
    *outside* ``cache_locks``, so popping a line another thread is mid-read
    raises there (review finding on PR #462, reproduced as a ``KeyError`` in
    ``ioRequest`` plus silently-degraded ``H5Promise`` parses). Those builds
    therefore only *record* their walk lines, and ``read_group`` evicts them
    through this helper in a ``finally`` — after the route returned, so the
    ``ThreadPoolExecutor`` context has joined and the handle is back to the
    single worker thread that owns this granule (the ``_pending`` docstring's
    "each granule is read by a single worker thread" invariant). Returns the
    number of lines evicted; same non-raising discipline as
    :func:`_evict_new_cache_lines`.
    """
    cache = getattr(h5obj, "cache", None)
    if not cache or not deferred:
        return 0
    evicted = _drop_cache_lines(h5obj, list(deferred))
    deferred.clear()
    if evicted:
        logger.debug(f"  chunk maps: evicted {evicted} deferred cache line(s) after the fan-out")
    return evicted


def build_chunk_map(h5obj, path: str, *, evict_into: set | None = None) -> ChunkMap:
    """Build a :class:`ChunkMap` for one dataset by walking its metadata.

    Metadata-only: no chunk is ever read or decompressed. A contiguous-layout
    dataset yields a single pseudo-chunk covering the full first axis
    (mirroring h5py's ``get_offset()`` treatment in the bench extractor).
    Cache lines the B-tree walk pulls into ``h5obj.cache`` are evicted before
    returning (issue #460); the lines the ``H5Dataset`` parse itself touched
    are deliberately kept (the snapshot is taken after the parse). Parse-line
    residency does grow per *distinct* dataset rather than being fully shared,
    but it is bounded by the group's object-header / metadata footprint, not by
    the chunk count: measured on a real GEDI L1B beam group (PR #462 evidence),
    a whole group's read left **11 lines / 46 MB** resident at the 4 MiB
    default — the data path's own lines included — out of **90 distinct lines**
    ever touched across the group, against the hundreds of MB the unevicted
    walks stranded. The absent-path ``KeyError`` below returns before the
    snapshot on purpose: nothing walk-touched exists yet, and its parse lines
    are retained by the same choice. That retention is exact only on the
    immediate path (single-threaded); the deferred path below drops some parse
    lines too, so the GEDI figure above — measured through it at the default
    ``read_workers`` — is if anything the lower residency of the two.

    ``evict_into`` defers that eviction for a caller whose builds run
    concurrently on one handle: pass a set and the walk's keys are *added* to
    it (a GIL-atomic ``set.update``) instead of popped, for the caller to
    evict once its fan-out has joined — see :func:`_evict_deferred_lines`.
    The window is a plain before/after key diff, so lines *other threads* add
    inside it are swept in as well — measurably, including other datasets'
    object-header parse lines (review finding on PR #462: 23 of 40 concurrent
    rounds on the test fixture). Those lines re-fetch on next touch (one small
    ranged read each), and that bounded cost is the deliberate price of a
    race-free drain: the alternative — subtracting a floor of every build's
    ``before`` snapshot — is extra machinery to protect what is a memory *win*.
    The default (``None``) evicts immediately, which is what every
    single-threaded caller wants (``_prebuild_group_maps``, the full-coverage
    walk, direct use).

    Raises ``KeyError`` for an absent path (h5coro's ``metaOnly`` traversal
    never raises on its own — it just leaves default metadata) and
    ``ValueError`` for layouts without file-offset storage (compact).
    """
    from h5coro.h5dataset import H5Dataset

    ds = H5Dataset(h5obj, path, earlyExit=True, metaOnly=True, enableAttributes=False)
    if ds.meta.typeSize == 0:
        raise KeyError(path)
    before = _cache_keys(h5obj)
    try:
        return _chunk_map_from_dataset(h5obj, ds, path)
    finally:
        if evict_into is not None:
            added = _cache_keys(h5obj) - before
            evict_into.update(added)
            if added:
                # Per-build attribution on the DEFERRED branch too (review
                # finding on PR #462): with ``write_back`` off — the shipped
                # default — every build goes through here, so without this line
                # the only DEBUG record is ``_evict_deferred_lines``' group-level
                # aggregate, which cannot single out the one pathological dataset
                # (GEDI L1B's ``rxwaveform``) inside a group of dozens.
                logger.debug(
                    f"  chunk map {path}: walk touched {len(added)} cache line(s); "
                    "deferred for post-read eviction"
                )
        else:
            evicted = _evict_new_cache_lines(h5obj, before)
            if evicted:
                logger.debug(
                    f"  chunk map {path}: evicted {evicted} cache line(s) the walk touched"
                )


def _chunk_map_from_dataset(h5obj, ds, path: str) -> ChunkMap:
    """Assemble a :class:`ChunkMap` from an already-parsed ``H5Dataset``.

    The metadata-only body of :func:`build_chunk_map`, factored out so the
    full-coverage walk (:func:`_iter_datasets`, issue #190) can map each
    dataset from the single ``H5Dataset`` it already parsed to classify it —
    avoiding a second object-header parse per dataset. ``ds`` must be a
    non-group (``typeSize != 0``) metadata-only dataset.
    """
    from h5coro.h5dataset import INVALID_VALUE, H5Dataset
    from h5coro.h5metadata import H5Metadata

    dims = tuple(int(x) for x in ds.meta.dimensions or ())
    try:
        dtype = np.dtype(
            H5Metadata.TO_NUMPY_TYPE[ds.meta.type][ds.meta.signedval][ds.meta.typeSize]
        ).str
    except KeyError:  # a type h5coro cannot map (string/compound/...) — record blank
        dtype = ""
    gzip = bool(ds.meta.filter.get(H5Metadata.DEFLATE_FILTER))
    shuffle = bool(ds.meta.filter.get(H5Metadata.SHUFFLE_FILTER))

    def _empty() -> ChunkMap:
        z = np.empty(0, dtype=np.int64)
        d = dims or (0,)
        return ChunkMap(path, z, z, z, z, z, d, d, dtype, gzip, shuffle)

    if not dims or 0 in dims:
        return _empty()
    if ds.meta.address == INVALID_VALUE[h5obj.offsetSize]:
        return _empty()  # no allocated storage

    if ds.meta.layout == H5Dataset.CHUNKED_LAYOUT:
        chunk_dims = tuple(int(x) for x in ds.meta.chunkDimensions)
        raw = tuple(sorted(_walk_chunk_btree(ds), key=lambda entry: entry[0]))
        rows: dict[int, list[int]] = {}  # e0 -> [byte_offset, nbytes, filter_mask]
        for offset_elems, filter_mask, addr, size in raw:
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
            dtype=dtype,
            gzip=gzip,
            shuffle=shuffle,
            raw=raw,
        )
    if ds.meta.layout == H5Dataset.CONTIGUOUS_LAYOUT:
        addr, size = int(ds.meta.address), int(ds.meta.size)
        return ChunkMap(
            dataset=path,
            elem_start=np.array([0], dtype=np.int64),
            elem_end=np.array([dims[0]], dtype=np.int64),
            byte_offset=np.array([addr], dtype=np.int64),
            nbytes=np.array([size], dtype=np.int64),
            filter_mask=np.zeros(1, dtype=np.int64),
            dims=dims,
            chunk_dims=dims,
            dtype=dtype,
            gzip=gzip,
            shuffle=shuffle,
            raw=(((0,) * len(dims), 0, addr, size),),
        )
    # COMPACT data lives inside the object header, not at a file offset.
    raise ValueError(f"{path}: unsupported storage layout {ds.meta.layout!r} for chunk indexing")


def _iter_datasets(h5obj):
    """Yield ``(path, H5Dataset)`` for every dataset in the granule.

    Descends the group tree with h5coro (``metaOnly``, attributes off), so the
    whole enumeration is served from the front-of-file metadata block h5coro
    already cached — one ranged GET, no chunk data (issue #190; the full walk
    stays inside the single ~4 MiB cache line the config's read already paid
    for, measured on real ATL03). Since issue #460 that "already cached" is a
    best case rather than a guarantee: each group's deferred drain may have
    swept out object-header lines the read had cached (see ``evict_into`` in
    :func:`build_chunk_map`), so on a sparse file this walk re-fetches some of
    them — bounded extra ranged reads, still no chunk data. A node whose
    ``typeSize == 0`` is a group
    (or a typeless link) and is descended; anything else is a dataset yielded
    with the ``H5Dataset`` its classification already parsed, so the caller
    can build its chunk map without re-parsing the object header. Each node's
    object header is parsed exactly once: parsing a group path populates its
    *direct* children into ``h5obj.pathAddresses`` (h5coro traverses one level
    to resolve the path), so the classification parse doubles as the group's
    enumeration parse. A ``seen`` set of object-header addresses bounds the
    recursion against a (non-ICESat-2) hard-link cycle. Paths are absolute
    (leading ``/``) and visited in sorted order for determinism.
    """
    from h5coro.h5dataset import H5Dataset

    def child_paths(path: str) -> list[str]:
        prefix = "" if path == "/" else path.lstrip("/") + "/"
        kids = set()
        for pp in list(h5obj.pathAddresses.keys()):
            if pp.startswith(prefix):
                rel = pp[len(prefix) :]
                if rel and "/" not in rel:  # a direct child of ``path``
                    kids.add("/" + prefix + rel)
        return sorted(kids)

    seen: set[int] = set()

    def visit(path: str):
        for kp in child_paths(path):
            ds = H5Dataset(h5obj, kp, earlyExit=False, metaOnly=True, enableAttributes=False)
            if ds.meta.typeSize == 0:  # group (or typeless link): descend
                addr = h5obj.pathAddresses.get(kp.lstrip("/"))
                if addr in seen:
                    continue  # hard-link cycle guard
                seen.add(addr)
                yield from visit(kp)  # kp's children are already in pathAddresses
            else:
                yield kp, ds

    # Parse the root once to populate its direct children, then descend.
    H5Dataset(h5obj, "/", earlyExit=False, metaOnly=True, enableAttributes=False)
    yield from visit("/")


def full_granule_maps(h5obj, existing: dict[str, ChunkMap]) -> dict[str, ChunkMap]:
    """Chunk maps for **every** decodable dataset in the granule (issue #190).

    The write-back full-coverage payload: enumerate the whole group tree
    (:func:`_iter_datasets`) and map each dataset, **reusing** any map already
    in ``existing`` (the config's read set, built during the read) so those
    stay byte-identical to the read path and aren't re-walked. Undecodable
    dtypes (strings/compounds) are recorded with a blank ``dtype`` and skipped
    at read by the sidecar consumer (``datasets_from_manifest``) — the
    include-and-skip choice, matching that consumer. COMPACT-layout datasets
    (data in the object header, no file offset) raise :class:`ValueError` and
    are dropped: a ranged read could never serve them.
    """
    maps = dict(existing)
    for path, ds in _iter_datasets(h5obj):
        if path in maps:
            continue
        # Per-dataset snapshot-and-evict, same as build_chunk_map (issue
        # #460): this walk maps EVERY dataset in the granule, so unevicted it
        # accumulates worst of all. Evicting after each dataset keeps the
        # transient peak one walk deep; the enumeration's own object-header
        # lines are in each snapshot (the generator parses the next node
        # before the loop body runs) and stay cached for the datasets after.
        before = _cache_keys(h5obj)
        try:
            maps[path] = _chunk_map_from_dataset(h5obj, ds, path)
        except ValueError:
            continue  # COMPACT (or other offset-less layout): not chunk-indexable
        except Exception as exc:
            # Match the read path's per-dataset tolerance (a bad B-tree or an
            # odd object header degrades that dataset to the h5coro decoder
            # there): drop just this dataset from the manifest instead of
            # sinking the whole granule's write-back.
            logger.warning(f"  write-back: no chunk map for {path} ({exc}); skipping")
        finally:
            evicted = _evict_new_cache_lines(h5obj, before)
            if evicted:
                logger.debug(
                    f"  write-back map {path}: evicted {evicted} cache line(s) the walk touched"
                )
    return maps


def granule_manifest(maps: dict[str, ChunkMap]) -> pd.DataFrame:
    """Assemble one granule's write-back manifest from its chunk maps.

    One row per real HDF5 chunk (uncollapsed — trailing-dim chunks keep their
    own ``byte_offset``/``nbytes``), sorted by ``(dataset, chunk_idx)`` with
    ``chunk_idx`` the row-major linear index over the chunk grid (matching
    the bench extractor's convention).
    """
    rows: list[tuple] = []
    for path in sorted(maps):
        cm = maps[path]
        if not cm.raw:
            continue
        grid = [math.ceil(d / c) for d, c in zip(cm.dims, cm.chunk_dims)]
        step = [1] * len(grid)
        for d in range(len(grid) - 2, -1, -1):
            step[d] = grid[d + 1] * step[d + 1]
        shape_json = json.dumps(list(cm.dims))
        chunk_shape_json = json.dumps(list(cm.chunk_dims))
        for offset_elems, filter_mask, addr, size in cm.raw:
            idx = sum((o // c) * s for o, c, s in zip(offset_elems, cm.chunk_dims, step))
            e0 = int(offset_elems[0])
            e1 = min(e0 + cm.chunk_dims[0], cm.dims[0])
            rows.append(
                (
                    path,
                    idx,
                    e0,
                    e1,
                    int(addr),
                    int(size),
                    int(filter_mask),
                    json.dumps([int(o) for o in offset_elems]),
                    cm.dtype,
                    shape_json,
                    chunk_shape_json,
                    cm.gzip,
                    cm.shuffle,
                )
            )
    cols = list(MANIFEST_DTYPES)
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame({c: [] for c in cols})
    return df.astype(MANIFEST_DTYPES).sort_values(["dataset", "chunk_idx"], ignore_index=True)


def write_manifest(df: pd.DataFrame, store_path: str, granule_id: str) -> str:
    """Persist one granule's manifest to ``<store_path>/<granule_id>.parquet``.

    Routes through :func:`zagg.store.open_object_store`, so ``store_path`` is
    a local directory (created if absent) or an ``s3://bucket/prefix`` URI
    (ambient credentials — the worker/extraction role, per the issue #160 IAM
    notes; granule-read credentials are never used for the store). Parquet is
    written with fastparquet (core dep, layer-safe — no pyarrow), serialized
    in memory. Returns the object key.
    """

    from zagg.store import open_object_store, put_object

    store = open_object_store(store_path)
    key = f"{granule_id}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, engine="fastparquet", index=False)
    put_object(store, key, buf.getvalue())
    return key


class InlineIndex(VirtualIndex):
    """Chunk map computed at read time; chunk-aligned planned reads.

    Never consults the sidecar store — it recomputes every granule every run
    (the no-store-yet mode and, with ``write_back: true``, the
    store-population mode; see the issue #160 deployment progression).
    """

    name = "inline"
    config_keys = frozenset({"write_back", "store", "write_back_coverage"})

    #: Accepted ``write_back_coverage`` values (issue #190).
    _COVERAGE = frozenset({"full", "lazy"})

    def __init__(
        self,
        write_back: bool = False,
        store: str | None = None,
        write_back_coverage: str = "full",
    ):
        self.write_back = bool(write_back)
        self.store = store
        # Write-back coverage (issue #190): ``full`` (default, the #163 store
        # design intent) persists chunk maps for EVERY decodable dataset in the
        # granule — extracted from the metadata the read already cached, so any
        # downstream config can be served from the manifest; ``lazy`` persists
        # only the config's read set (the pre-#190 behavior), an escape hatch
        # for a private run that wants the smaller manifest. Neither changes
        # what the READ path fetches.
        self.write_back_coverage = write_back_coverage
        # Chunk maps accumulated across each in-flight granule's groups,
        # keyed full resource URL -> {dataset path -> ChunkMap} (issue #180:
        # the worker may hold ``data_source.granule_workers`` granules in
        # flight; dataset paths repeat across granules, and even the granule
        # id stem can collide across prefixes — review finding — so only the
        # full URL isolates granules). ``finish_granule`` drains exactly its
        # granule's entry. Each granule is read by a single worker thread, so
        # its sub-dict is never shared; the outer dict's setdefault/pop are
        # atomic under the GIL. Only populated when writing back.
        self._pending: dict[str, dict[str, ChunkMap]] = {}

    @classmethod
    def validate_index_config(cls, index_cfg: dict, data_source: dict | None = None) -> None:
        write_back = index_cfg.get("write_back", False)
        if not isinstance(write_back, bool):
            raise ValueError(f"index.write_back must be a boolean (got {write_back!r})")
        store = index_cfg.get("store")
        if write_back and not (isinstance(store, str) and store):
            raise ValueError(
                "index backend 'inline' with write_back: true requires 'store' "
                "(a local directory or s3://bucket/prefix)"
            )
        if store is not None and not write_back:
            raise ValueError(
                "index.store is only meaningful for backend 'inline' with "
                "write_back: true (inline never reads the store)"
            )
        if "write_back_coverage" in index_cfg and not write_back:
            raise ValueError(
                "index.write_back_coverage is only meaningful for backend 'inline' "
                "with write_back: true"
            )
        coverage = index_cfg.get("write_back_coverage", "full")
        if coverage not in cls._COVERAGE:
            raise ValueError(
                f"index.write_back_coverage must be one of {sorted(cls._COVERAGE)} "
                f"(got {coverage!r})"
            )
        # Both read routes accept this backend (issue #170 phase 2): sources
        # with read_plan.spatial_index take the planned route, read-plan-less
        # (flat) sources the full-read route -- same compiled addressing seam.
        if data_source is not None:
            rp = data_source.get("read_plan")
            if isinstance(rp, dict) and "chunk_boundaries" in rp:
                # The a-priori arm (issue #148 arm 2a) takes precedence over
                # spatial_index inside _read_group, which would silently bypass
                # this backend's chunk-map addressing -- reject the combination.
                raise ValueError(
                    "index backend 'inline' and read_plan.chunk_boundaries (the "
                    "a-priori arm) are mutually exclusive; drop one of them"
                )

    @classmethod
    def from_index_config(cls, index_cfg: dict) -> "InlineIndex":
        return cls(
            write_back=index_cfg.get("write_back", False),
            store=index_cfg.get("store"),
            write_back_coverage=index_cfg.get("write_back_coverage", "full"),
        )

    def _pending_for(self, h5obj) -> dict[str, ChunkMap]:
        """This granule's pending chunk maps (write-back accumulation).

        Keyed by the FULL ``h5obj.resource`` URL — the rewritten URL the
        worker opened the granule with — so concurrent in-flight granules
        (issue #180) never share chunk-map state, even when their basenames
        collide across prefixes (review finding: the id stem would collapse
        ``.../p1/granule.h5`` and ``.../p2/granule.h5`` into one entry and
        serve B's reads through A's chunk maps).
        """
        return self._pending.setdefault(str(h5obj.resource), {})

    def _prebuild_group_maps(self, h5obj, group: str, data_source: dict) -> None:
        """Build the chunk maps for every dataset this group's read can touch.

        The manifest set per group: base-rate coordinates + variables +
        base-level structured-filter datasets (what ``execute_read_plan``
        addresses), plus the spatial-index level's coordinate and link arrays
        (read in full by the planned route, and part of the bench extractor's
        manifest convention). Missing datasets raise ``KeyError`` — the same
        group-read failure the data read would produce, so nothing this
        granule does NOT hold belongs in the set: a vlen source (issue #425)
        contributes its record level's coordinates and link, the endpoint
        datasets its synthesized columns interpolate between, and no
        sibling-asset filter dataset (those live in the paired granule, which
        this backend never opens).
        """
        from zagg.config import filters_from_data_source
        from zagg.processing.read import _level_coord_paths, _variable_specs
        from zagg.processing.read_vlen import path_form_variables, synthesized_specs

        # ``coordinates.level`` names a record LEVEL, not a dataset (the vlen
        # route: the base level has no native coordinates), so it is not a
        # path to map — the record level's own lat/lon are the sibling keys.
        # A coordinate sourced from a sibling asset (issue #678, the GLAH01 ↔
        # GLAH05 join) lives in the paired granule, which this backend never
        # opens; a base-level ``{path, axes}`` coordinate (the swath form) is
        # a dataset on this one.
        paths = [
            (tmpl["path"] if isinstance(tmpl, dict) else tmpl).format(group=group)
            for key, tmpl in data_source["coordinates"].items()
            if key != "level" and not (isinstance(tmpl, dict) and "asset" in tmpl)
        ]
        # Variables go through the read path's normalizer: an entry is either a
        # path-template string or the ``{path, column}`` mapping form (issue
        # #321). Chunk maps are per DATASET, so the column selector is
        # irrelevant here — several selectors on one path collapse in the
        # ``dict.fromkeys`` dedup below (one chunk map for all five
        # ``signal_conf_ph`` surfaces). ``synthesize:`` entries carry no
        # ``path`` at all (the normalizer raises on one); what the read
        # touches for them is the record-rate endpoint pair.
        variables = data_source["variables"]
        paths += [
            tmpl.format(group=group)
            for tmpl, _ in _variable_specs(path_form_variables(variables)).values()
        ]
        for entry in synthesized_specs(variables).values():
            paths += [
                entry[k].format(group=group)
                for k in ("start", "stop", "offsets")
                if isinstance(entry.get(k), str)
            ]
        base_level = data_source.get("base_level")
        for f in filters_from_data_source(data_source):
            # A sibling-asset predicate reads the OTHER granule (issue #425).
            if "expression" in f or f.get("asset") is not None:
                continue
            if f.get("level") is None or f.get("level") == base_level:
                paths.append(f["dataset"].format(group=group))
        # Levels read in full by the group read: the planned route's spatial
        # index, and — on a vlen source — the record level the coordinates and
        # the gather map expand from (the same level, in every config that
        # declares both).
        level_keys = (
            (data_source.get("read_plan") or {}).get("spatial_index"),
            (data_source.get("coordinates") or {}).get("level"),
        )
        for level_key in dict.fromkeys(k for k in level_keys if k):
            lvl = (data_source.get("levels") or {}).get(level_key)
            if not isinstance(lvl, dict):
                continue
            # A record level whose coordinates come from a sibling asset (issue
            # #678) declares none of its own; the data_source coordinates above
            # already covered the primary-held case.
            if isinstance(lvl.get("coordinates"), dict):
                paths.extend(_level_coord_paths(lvl, group))
            link = lvl.get("link") or {}
            for key in ("index_beg", "count"):
                if key in link:
                    paths.append(link[key].format(group=group))
        pending = self._pending_for(h5obj)
        for path in dict.fromkeys(paths):
            if path not in pending:
                pending[path] = build_chunk_map(h5obj, path)

    def finish_granule(self, h5obj, granule_url: str) -> None:
        """Write-back seam: persist the granule's accumulated chunk maps.

        Drains only THIS granule's pending entry (issue #180: other granules
        may still be in flight), keyed as the read side keyed it — the full
        ``h5obj.resource`` URL — with the full ``granule_url`` as fallback
        for resource-less callers. No-op unless ``write_back`` is on. The
        manifest STORE key stays the granule id (URL basename without
        extension — granule ids carry product + version, so reprocessing
        changes the key; issue #160 store convention). Raises on store
        failures — the worker logs and continues, so a broken store degrades
        to plain ``inline`` reads.
        """
        resource = getattr(h5obj, "resource", None)
        maps = self._pending.pop(str(resource), None) if resource is not None else None
        if maps is None:
            maps = self._pending.pop(granule_url, {})
        if not self.write_back:
            return
        if self.write_back_coverage == "full":
            # Widen the persisted set to every decodable dataset in the granule
            # (issue #190): the read already cached the metadata, so this is a
            # metadata-only walk (no extra chunk GETs) that lets the manifest
            # serve any downstream config, not just this run's read set. The
            # read path is untouched — only what write-back PERSISTS grows.
            maps = full_granule_maps(h5obj, maps)
        maps = {path: cm for path, cm in maps.items() if cm.raw}
        if not maps:
            return
        key = write_manifest(granule_manifest(maps), self.store, _granule_id(granule_url))
        logger.info(f"  inline write-back: {len(maps)} dataset(s) -> {self.store}/{key}")

    def read_group(
        self,
        h5obj,
        group,
        data_source,
        shard_key,
        grid,
        arrow=False,
        granule_url=None,
        io_stats=None,
        siblings=None,
    ):
        from zagg.processing.read import (
            _planned_read_group,
            _read_group_full,
            _validate_planned_config,
        )

        # Vlen-packed sources (issue #425) take their own route, which owns
        # the record-expansion and planned/full arms; the compiled read_fn
        # rides along so the flat base datasets still decode through it.
        coords = data_source.get("coordinates")
        if isinstance(coords, dict) and coords.get("level") is not None:
            from zagg.processing.read_vlen import _vlen_read_group

            if self.write_back:
                self._prebuild_group_maps(h5obj, group, data_source)
            # read_fn is hoisted out of the call so the finally can reach its
            # deferred cache lines (issue #460 — see _evict_deferred_lines).
            read_fn = self._chunk_aligned_read_fn(h5obj, planned=True)
            try:
                return _vlen_read_group(
                    h5obj,
                    group,
                    data_source,
                    shard_key,
                    grid,
                    arrow=arrow,
                    read_fn=read_fn,
                    io_stats=io_stats,
                    siblings=siblings,
                )
            finally:
                _evict_deferred_lines(h5obj, read_fn.deferred_lines)

        rp = data_source.get("read_plan")
        # Two routes, one addressing seam (issue #170 phase 2): sources with a
        # spatial index take the planned (chunk-aligned hyperslice) route;
        # read-plan-less (flat) sources take the full-read route — both decode
        # through the compiled read_fn, so non-ATL03-shaped products get the
        # fast path too. Completeness of the planned config is the shared
        # gate from the read module.
        planned = isinstance(rp, dict) and bool(rp.get("spatial_index"))
        if planned:
            _validate_planned_config(data_source)
        if self.write_back:
            # Deterministic write-back coverage (metadata-only, ~ms): built up
            # front so routes that bypass the read seam — empty-shard early
            # returns (and, before issue #179 routed it through read_fn, the
            # ``full_read`` selectivity fallback) — still contribute this
            # group's datasets to the granule manifest.
            self._prebuild_group_maps(h5obj, group, data_source)
        read_fn = self._chunk_aligned_read_fn(h5obj, planned=planned)
        try:
            if planned:
                return _planned_read_group(
                    h5obj,
                    group,
                    data_source,
                    shard_key,
                    grid,
                    arrow=arrow,
                    read_fn=read_fn,
                    io_stats=io_stats,
                )
            return _read_group_full(
                h5obj,
                group,
                data_source,
                shard_key,
                grid,
                arrow=arrow,
                read_fn=read_fn,
                io_stats=io_stats,
            )
        finally:
            # The route's pooled fan-out has joined here, so the lines its
            # lazy chunk-map builds deferred can be dropped (issue #460).
            _evict_deferred_lines(h5obj, read_fn.deferred_lines)

    def _chunk_aligned_read_fn(self, h5obj, *, planned=True):
        """Build the addressing seam: planned ranges, compiled decode.

        The chunk maps this backend already builds are exactly what
        ``h5coro_hidefix.Index.from_chunks`` consumes (the same
        ``granule_manifest`` columns ``write_back`` persists), so decode goes
        through the compiled reader (issue #170): plan the covering chunks,
        fetch their byte ranges through the worker's h5coro driver
        (``ioRequest`` — no second credential path), and inflate with
        ``read_from_buffers`` (byte-identical to h5py/h5coro, GIL released).
        Datasets the compiled route cannot serve — undecodable dtypes
        (strings, compounds), empty chunk maps, reconstruction failures —
        degrade to the h5coro decoder per dataset with a warning, never
        aborting the shard. The h5coro fallback keeps the PR #152 start-edge
        workaround: a range starting exactly on an interior chunk boundary
        is shifted one element early and trimmed (h5coro's B-tree start-edge
        intersection drops the chunk entirely); the compiled reader has
        exact ``[start, end)`` semantics and needs no workaround.

        Chunk maps are built lazily per dataset (first read of that path)
        and cached for the life of the returned callable — i.e. one
        ``read_group`` call, which is exactly one (granule, group): a group's
        datasets are disjoint from every other group's, so nothing is
        rebuilt or leaked across granules. Under ``write_back`` the cache is
        THIS granule's pending sub-dict instead (``_pending_for`` — keyed per
        granule so concurrent in-flight granules never interleave, issue
        #180), accumulating the granule's maps across groups for
        ``finish_granule`` to persist. Those lazy builds defer their cache-line
        eviction onto ``read_fn.deferred_lines`` (issue #460) because they run
        across the read pool's threads; ``read_group`` drains it once the route
        has joined. Each dataset gets
        its own single-dataset in-memory ``Index`` (~ms), so a spec hidefix
        rejects degrades that dataset alone.

        Selection-shaped reads skip the walk (issue #185): on the planned
        route a full-dataset read (``hyperslice is None``) is a segment-rate
        selection read — data reads arrive as hyperslices — and h5coro's
        ``readDatasets`` (4 MiB cache lines) serves those faster than the
        compiled route once the B-tree walk is priced in (the 0.18 inline
        regression, ~41% at ``granule_workers: 1``; the #170 matrix measured
        the same trade). So with ``write_back`` off, an unmapped full-dataset
        read on the planned route goes straight through ``readDatasets`` and
        builds no chunk map. A path already mapped (e.g. one that is both a
        data and a selection read) keeps the compiled route — the map is paid
        for. With ``write_back`` ON nothing changes: the walk's product IS
        the manifest payload, so selection datasets stay mapped (and served
        compiled) to keep manifests covering them for sidecar consumers. The
        full-read route (``planned=False``) is untouched — its compiled
        full-dataset decode is the point of issue #170.
        """
        # Issue #185: with write_back off there is no manifest to enrich, so
        # the planned route's full-dataset (selection) reads bypass the
        # chunk-map build entirely -- see read_fn below.
        selection_direct = planned and not self.write_back
        maps: dict[str, ChunkMap] = self._pending_for(h5obj) if self.write_back else {}
        # One single-dataset Index per path (~ms each): a dataset whose spec
        # hidefix rejects (nonzero filter_mask, non-tiling chunk table) then
        # pins only ITSELF to the fallback — a shared Index rebuilt from all
        # of ``maps`` would fail every subsequent dataset's reconstruction
        # and degrade innocent paths with it (review finding, PR #173).
        indices: dict = {}
        direct: set[str] = set()  # datasets pinned to the h5coro decoder
        # Cache lines the lazy builds below touched, evicted by ``read_group``
        # once this callable's fan-out has joined (issue #460): these builds
        # run on ``read_workers`` threads sharing one h5obj, where popping a
        # line races h5coro's unlocked cached ``ioRequest`` — see
        # :func:`_evict_deferred_lines`. Exposed on the callable so every
        # route can drain it in a ``finally``.
        deferred: set = set()

        def _vidx_for(path):
            vidx = indices.get(path)
            if vidx is None:
                from h5coro_hidefix import Index
                from h5coro_hidefix.manifest import datasets_from_manifest

                vidx = indices[path] = Index.from_chunks(
                    "inline", datasets_from_manifest(granule_manifest({path: maps[path]}))
                )
            return vidx

        def _compiled(path, start, end):
            vidx = _vidx_for(path)
            addrs, sizes, _ = vidx.read_plan(path, start, end)
            buffers = []
            for a, s in zip(addrs, sizes):
                buf = h5obj.ioRequest(int(a), int(s), caching=False)
                if buf is None:
                    # h5coro's S3 driver swallows exceptions and returns None;
                    # surface it as I/O so it is never misclassified as a
                    # decode failure (review finding, PR #173).
                    raise OSError(f"ranged read failed for {path} at {int(a)}+{int(s)}")
                buffers.append(buf)
            return vidx.read_from_buffers(path, buffers, start, end)

        def _h5coro_read(path, hyperslice, cm):
            if hyperslice is None:
                return h5obj.readDatasets([path])[path]
            parts = []
            for s, e in hyperslice:
                lo = s
                if s > 0 and cm is not None and len(cm) and cm.starts_on_boundary(s):
                    lo = s - 1  # h5coro start-edge workaround (see docstring)
                arr = h5obj.readDatasets([{"dataset": path, "hyperslice": [(lo, e)]}])[path]
                parts.append(arr[s - lo :])
            return parts[0] if len(parts) == 1 else np.concatenate(parts)

        def read_fn(path, hyperslice=None):
            cm = maps.get(path)
            if cm is None and path not in direct:
                if selection_direct and hyperslice is None:
                    # Selection-shaped read (issue #185): serve through
                    # h5coro's cache-line path, build no chunk map. Full
                    # readDatasets reads never trip the PR #152 start-edge
                    # off-by-one (that is a ranged-read quirk), so no
                    # boundary workaround is needed here.
                    return h5obj.readDatasets([path])[path]
                try:
                    cm = maps[path] = build_chunk_map(h5obj, path, evict_into=deferred)
                except Exception:
                    if planned and hyperslice is not None:
                        # The planned route required the map before #170 too:
                        # without it the boundary workaround can't run, and a
                        # plain hyperslice read may trip the PR #152 edge.
                        raise
                    direct.add(path)
                    logger.warning(f"  no chunk map for {path}; reading through h5coro")
            if path not in direct:
                try:
                    if hyperslice is None:
                        return _compiled(path, None, None)
                    parts = [_compiled(path, s, e) for s, e in hyperslice]
                    return parts[0] if len(parts) == 1 else np.concatenate(parts)
                except OSError:
                    raise  # transient I/O, not a decode problem: fail the read loudly
                except Exception as exc:
                    direct.add(path)
                    logger.warning(
                        f"  compiled decode unavailable for {path} ({exc}); reading through h5coro"
                    )
            return _h5coro_read(path, hyperslice, cm)

        read_fn.deferred_lines = deferred
        return read_fn
