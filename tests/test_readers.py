"""Tests for the t-digest → tensor read helpers — issue #79.

The readers consume the sharded vlen-bytes ragged layout (issue #209): one
``variable_length_bytes`` array per field, self-describing element attrs, a
sibling ``morton`` coordinate for chunk identity, and (for located fields) an
attrs-declared ``{field}_locations`` sibling. Fixtures build REAL stores
through the production template + writers, on both layouts the writer emits
(K==1 regular chunks and ShardingCodec'd K>1).
"""

import math

import numpy as np
import pandas as pd
import pytest
import zarr
from zarr.storage import MemoryStore

from zagg.config import PipelineConfig
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.processing import write_ragged_to_zarr, write_shard_to_zarr
from zagg.readers.tdigest_tensor import (
    cell_index,
    chunk_z_range,
    rasterize_cell,
    read_cell,
    read_locations,
    read_raw_values,
    read_tensors,
)
from zagg.stats.tdigest import build_tdigest

# Two order-6 shards (decimal morton ids). At K==1 the read chunk IS the
# shard, so the readers report these packed words as the chunk morton ids.
_KEY_A, _KEY_B = "1121121", "2431123"


def _cfg(located=False):
    """Minimal config: one ragged t-digest field + the morton coordinate."""
    meta = {
        "function": "zagg.stats.tdigest.build_tdigest",
        "source": "h",
        "kind": "ragged",
        "inner_shape": [2],
        "dtype": "float32",
        "fill_value": 0,
    }
    if located:
        meta["location"] = "leaf_id"
    return PipelineConfig(
        data_source={"groups": ["g"]},
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {"h_tdigest": meta},
        },
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


def _grid(located=False):
    """Fullsphere K==1 grid: one 4096-cell (64×64) read chunk per shard."""
    return HealpixGrid(6, 12, layout="fullsphere", config=_cfg(located))


def _write_shard(grid, store, morton_key, cell_to_values, *, delta=512, locations=None):
    """Write one shard's digests (and morton coordinate) via the production
    per-chunk writer. ``locations`` maps cell -> per-obs uint64 words for a
    located field."""
    word = morton_word(morton_key) if isinstance(morton_key, str) else int(morton_key)
    block = grid.block_index(word)
    base = block[0] * grid.cells_per_chunk
    morton_arr = zarr.open_array(store, path=f"{grid.group_path}/morton", mode="r+")
    morton_arr[base : base + grid.cells_per_chunk] = grid.children(word)
    cell_ids = sorted(cell_to_values)
    if locations is None:
        payloads = [build_tdigest(np.asarray(cell_to_values[c]), delta=delta) for c in cell_ids]
        ragged = {"h_tdigest": (payloads, cell_ids)}
    else:
        pairs = [
            build_tdigest(np.asarray(cell_to_values[c]), delta=delta, locations=locations[c])
            for c in cell_ids
        ]
        ragged = {"h_tdigest": ([p[0] for p in pairs], cell_ids, [p[1] for p in pairs])}
    write_ragged_to_zarr(ragged, store, grid=grid, chunk_idx=block)
    return word


def _build_store(shards, *, delta=512, located_locs=None):
    """Template + shards on the K==1 layout; returns ``(store, grid, words)``."""
    grid = _grid(located=located_locs is not None)
    store = MemoryStore()
    grid.emit_template(store)
    words = {}
    for key, cell_to_values in shards.items():
        locs = located_locs[key] if located_locs is not None else None
        words[key] = _write_shard(grid, store, key, cell_to_values, delta=delta, locations=locs)
    return store, grid, words


class _CountingStore(MemoryStore):
    """MemoryStore recording every satisfied GET as ``(key, byte_range, nbytes)``
    and every LIST as its prefix (a LIST is not a GET, so the two accountings
    are separate — review, PR #357)."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.gets: list = []
        self.lists: list = []

    def with_read_only(self, read_only=True):
        s = _CountingStore(store_dict=self._store_dict, read_only=read_only)
        s.gets = self.gets
        s.lists = self.lists
        return s

    async def get(self, key, prototype, byte_range=None):
        r = await super().get(key, prototype, byte_range)
        if r is not None:
            self.gets.append((key, byte_range, len(r)))
        return r

    def list_prefix(self, prefix):
        self.lists.append(prefix)
        return super().list_prefix(prefix)


def _sharded_store(store, *, populate, values=None):
    """A K=16 ShardingCodec'd store with one populated cell per chunk in
    ``populate`` (local chunk ordinals); returns ``(grid, shard_word,
    {local: global_cell})``. ``values`` optionally maps a local chunk ordinal
    to that chunk's sample vector (default: 300 uniform draws per chunk)."""
    rng = np.random.default_rng(21)
    cfg = _cfg()
    cfg.output["grid"]["chunk_inner"] = 8
    cfg.output["grid"]["sharded"] = True
    grid = HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
    shard6 = morton_word(_KEY_A)
    grid.emit_template(store)
    morton_arr = zarr.open_array(store, path="12/morton", mode="r+")
    shard_block = grid.block_index(shard6)[0]
    chunk_results, targets = [], {}
    for block, children in grid.iter_chunks(shard6):
        local = int(block[0]) - shard_block * grid.chunks_per_shard
        base = int(block[0]) * grid.cells_per_chunk
        morton_arr[base : base + grid.cells_per_chunk] = np.asarray(children)
        if local not in populate:
            chunk_results.append((block, pd.DataFrame(), {}))
            continue
        vals = rng.uniform(0.0, 30.0, 300) if values is None else np.asarray(values[local])
        payloads = [build_tdigest(vals, delta=512)]
        chunk_results.append((block, pd.DataFrame(), {"h_tdigest": (payloads, [11])}))
        targets[local] = base + 11
    write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=shard6)
    return grid, shard6, targets


def _leaf_store(digest_ranks, *, located=False, seed=35):
    """A hive-leaf store (issue #199): the vlen layout scoped to ONE shard —
    ``_KEY_A``'s 4096-cell single-root axis (order-6 shard, order-12 cells,
    K==1). Returns ``(store, shard_word, {rank: values})``; 5 values per cell,
    so digests stay unmerged (losslessly recoverable)."""
    from zagg.processing import write_ragged_leaf_to_zarr

    rng = np.random.default_rng(seed)
    grid = HealpixGrid(6, 12, layout="fullsphere", config=_cfg(located), sharded=False)
    word = morton_word(_KEY_A)
    store = MemoryStore()
    grid.emit_shard_template(store)
    children = np.asarray(grid.children(word), dtype=np.uint64)
    zarr.open_array(store, path="12/morton", mode="r+")[:] = children
    ranks = sorted(digest_ranks)
    vals = {r: rng.uniform(10.0, 30.0, 5) for r in ranks}
    if located:
        from conftest import point_words

        pairs = [build_tdigest(vals[r], delta=512, locations=point_words(5, r + 1)) for r in ranks]
        ragged = {"h_tdigest": ([p[0] for p in pairs], ranks, [p[1] for p in pairs])}
    else:
        ragged = {"h_tdigest": ([build_tdigest(vals[r], delta=512) for r in ranks], ranks)}
    write_ragged_leaf_to_zarr([((0,), ragged)], store, grid=grid)
    return store, word, vals


def _put_object(store, key, payload):
    """PUT raw bytes into a zarr store (the MemoryStore sidecar write)."""
    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    sync(store.set(key, default_buffer_prototype().buffer.from_bytes(payload)))


def _sharded_leaf_store(digest_ranks, extra_occupied=(), *, stamp=True, seed=41):
    """A committed 16-chunk hive leaf: ``_KEY_A``'s order-6 shard, order-12
    cells, ``chunk_inner=8`` (256-cell read chunks) under one leaf-wide
    ShardingCodec object — with an exact ``coverage.moc`` occupancy sidecar
    when ``stamp``. Returns ``(store, shard_word)``."""
    from zagg import hive
    from zagg.processing import write_ragged_leaf_to_zarr

    rng = np.random.default_rng(seed)
    cfg = _cfg()
    cfg.output["grid"]["chunk_inner"] = 8
    cfg.output["grid"]["sharded"] = True
    grid = HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
    word = morton_word(_KEY_A)
    store = MemoryStore()
    grid.emit_shard_template(store)
    children = np.asarray(grid.children(word), dtype=np.uint64)
    zarr.open_array(store, path="12/morton", mode="r+")[:] = children
    per_chunk: dict[int, list[int]] = {}
    for rank in sorted(digest_ranks):
        per_chunk.setdefault(rank // grid.cells_per_chunk, []).append(rank % grid.cells_per_chunk)
    entries = [
        (
            (block,),
            {
                "h_tdigest": (
                    [build_tdigest(rng.uniform(10.0, 30.0, 60), delta=512) for _ in local],
                    local,
                )
            },
        )
        for block, local in sorted(per_chunk.items())
    ]
    write_ragged_leaf_to_zarr(entries, store, grid=grid)
    if stamp:
        occupied = np.sort(children[sorted(set(digest_ranks) | set(extra_occupied))])
        bitmap = hive.encode_coverage_bitmap(word, occupied, 12)
        _put_object(store, hive.COVERAGE_SIDECAR, bitmap)
        hive.stamp_commit(
            store,
            cells_with_data=len(digest_ranks),
            granule_count=1,
            coverage=hive.build_coverage(word, occupied, 12, bitmap=bitmap),
        )
    return store, word


class TestRasterizeCell:
    def test_empty_digest_all_zero(self):
        out = rasterize_cell(np.empty((0, 2), dtype=np.float32), 0.0, 0.5, 16)
        assert out.shape == (16,)
        assert np.all(out == 0.0)

    def test_counts_sum_to_in_window_weight(self):
        rng = np.random.default_rng(0)
        vals = rng.uniform(10.0, 20.0, size=5_000)
        digest = build_tdigest(vals, delta=512)
        # Window comfortably brackets all data.
        out = rasterize_cell(digest, 9.0, 0.5, 24)  # [9, 21)
        # Total reconstructed weight ≈ N.
        assert out.sum() == pytest.approx(len(vals), rel=0.01)

    def test_counts_non_negative(self):
        rng = np.random.default_rng(1)
        digest = build_tdigest(rng.standard_normal(2_000), delta=256)
        out = rasterize_cell(digest, -4.0, 0.25, 32)
        assert np.all(out >= 0.0)

    def test_matches_histogram_within_tolerance(self):
        """Rasterized counts track np.histogram of the original samples."""
        rng = np.random.default_rng(2)
        vals = rng.normal(50.0, 5.0, size=40_000)
        digest = build_tdigest(vals, delta=512)
        z_lo, resolution, n_bins = 30.0, 1.0, 40  # [30, 70)
        out = rasterize_cell(digest, z_lo, resolution, n_bins)
        edges = z_lo + resolution * np.arange(n_bins + 1)
        hist, _ = np.histogram(vals, bins=edges)
        # Compare as fractions of total; t-digest bin counts track the empirical
        # histogram within a few percent of N over the bulk of the distribution.
        frac_err = np.abs(out - hist) / len(vals)
        assert np.max(frac_err) < 0.02


class TestChunkZRange:
    def _digest(self, lo, hi, n=4_000, seed=0):
        rng = np.random.default_rng(seed)
        return build_tdigest(rng.uniform(lo, hi, size=n), delta=512)

    def test_no_cells_raises(self):
        with pytest.raises(ValueError, match="no populated cells"):
            chunk_z_range([], n_bins=128, resolution=0.5, bottom=0.05, top=0.95, fit="raise")

    def test_all_empty_digests_raises(self):
        empty = [np.empty((0, 2), dtype=np.float32)]
        with pytest.raises(ValueError, match="no populated cells"):
            chunk_z_range(empty, n_bins=128, resolution=0.5, bottom=0.05, top=0.95, fit="raise")

    def test_window_floor_and_fit(self):
        # Data spans ~[100, 120]; trimmed range fits in 128 × 0.5 = 64 m.
        digests = [self._digest(100.0, 120.0, seed=3)]
        z_lo, n_bins, res = chunk_z_range(
            digests, n_bins=128, resolution=0.5, bottom=0.05, top=0.95, fit="raise"
        )
        assert n_bins == 128
        assert res == 0.5
        assert z_lo == math.floor(z_lo)
        # Floor should be at/just below the 5th-percentile minimum (~101).
        assert 99.0 <= z_lo <= 102.0

    def test_raise_when_too_wide(self):
        # Span ~200 m ≫ 64 m window → raise.
        digests = [self._digest(0.0, 200.0, seed=4)]
        with pytest.raises(ValueError, match="exceeds the fixed window"):
            chunk_z_range(digests, n_bins=128, resolution=0.5, bottom=0.0, top=1.0, fit="raise")

    def test_degrade_resolution_doubles_in_pow2(self):
        digests = [self._digest(0.0, 200.0, seed=5)]
        z_lo, n_bins, res = chunk_z_range(
            digests,
            n_bins=128,
            resolution=0.5,
            bottom=0.0,
            top=1.0,
            fit="degrade_resolution",
        )
        assert n_bins == 128
        # resolution must be 0.5 * 2**k and the window must now cover the range.
        ratio = res / 0.5
        assert ratio == pytest.approx(2 ** round(math.log2(ratio)))
        span = math.ceil(max(0.0, 200.0)) - z_lo
        assert span <= n_bins * res

    def test_collapse_bins_shrinks_to_smallest_pow2(self):
        # Span ~10 m fits in far fewer than 128 × 0.5 = 64 m. Smallest pow2
        # window ≥ 10 m at 0.5 m is 32 bins (16 m); 16 bins (8 m) is too small.
        digests = [self._digest(100.0, 110.0, seed=6)]
        z_lo, n_bins, res = chunk_z_range(
            digests,
            n_bins=128,
            resolution=0.5,
            bottom=0.0,
            top=1.0,
            fit="collapse_bins",
        )
        assert res == 0.5
        # n_bins is a power of two ≤ 128 and the window covers the span.
        assert n_bins in (1, 2, 4, 8, 16, 32, 64, 128)
        span = math.ceil(110.0) - z_lo
        assert n_bins * res >= span
        # And halving once more would no longer cover it (smallest that fits).
        assert (n_bins // 2) * res < span

    def test_collapse_bins_pow2_for_non_pow2_n_bins(self):
        # Non-power-of-two n_bins must still collapse to a power of two.
        digests = [self._digest(100.0, 110.0, seed=8)]
        _, n_bins, res = chunk_z_range(
            digests,
            n_bins=100,
            resolution=0.5,
            bottom=0.0,
            top=1.0,
            fit="collapse_bins",
        )
        assert n_bins in (1, 2, 4, 8, 16, 32, 64)  # ≤ largest pow2 ≤ 100 (=64)
        assert res == 0.5

    def test_collapse_bins_cannot_grow_raises(self):
        digests = [self._digest(0.0, 200.0, seed=66)]
        with pytest.raises(ValueError, match="cannot grow"):
            chunk_z_range(
                digests,
                n_bins=128,
                resolution=0.5,
                bottom=0.0,
                top=1.0,
                fit="collapse_bins",
            )

    def test_unknown_fit_raises(self):
        digests = [self._digest(0.0, 300.0, seed=7)]
        with pytest.raises(ValueError, match="unknown fit"):
            chunk_z_range(digests, n_bins=128, resolution=0.5, bottom=0.0, top=1.0, fit="nope")


class TestReadTensors:
    def _store(self):
        rng = np.random.default_rng(10)
        store, _grid_, _words = _build_store(
            {
                _KEY_A: {
                    0: rng.uniform(10.0, 30.0, 3_000),
                    5: rng.uniform(12.0, 28.0, 2_000),
                    4095: rng.uniform(11.0, 29.0, 1_500),
                },
                _KEY_B: {7: rng.uniform(40.0, 60.0, 2_500), 63: rng.uniform(42.0, 58.0, 2_000)},
            }
        )
        return store

    def test_shape_and_dtype_default(self):
        out = {
            m: (t, mask, scale) for t, mask, scale, m in read_tensors(self._store(), "12/h_tdigest")
        }
        assert set(out) == {morton_word(_KEY_A), morton_word(_KEY_B)}
        for t, mask, (offset, gain) in out.values():
            assert t.shape == (64, 64, 128)
            assert t.dtype == np.uint32
            # The contract's mask + offset/gain channels (issue #336): a flat
            # store has no occupancy sidecar, so the mask is the 2-state {0, 2}.
            assert mask.shape == (64, 64) and mask.dtype == np.uint8
            assert set(np.unique(mask)) <= {0, 2}
            assert offset == float(math.floor(offset)) and gain == 0.5

    def test_morton_derived_from_coordinate(self):
        # Chunk identity round trip: the per-cell morton coordinate coarsens
        # to the chunk's coverage-cell id (the packed shard word at K==1).
        mortons = sorted(m for *_tms, m in read_tensors(self._store(), "12/h_tdigest"))
        assert mortons == sorted(morton_word(k) for k in (_KEY_A, _KEY_B))

    def test_populated_cell_placement_deinterleaved(self):
        out = {m: t for t, _mask, _scale, m in read_tensors(self._store(), "12/h_tdigest")}
        t = out[morton_word(_KEY_A)]
        # Deinterleave (issue #336): rank 5 = 0b101 → (row, col) = (y, x) =
        # (0, 3); rank 4095 → (63, 63); rank 0 → (0, 0).
        assert t[0, 0].sum() > 0
        assert t[0, 3].sum() > 0
        assert t[63, 63].sum() > 0
        # The old row-major position of rank 5 is now an unpopulated zero.
        assert t[0, 5].sum() == 0
        assert t[10, 10].sum() == 0

    def test_counts_match_population(self):
        rng = np.random.default_rng(11)
        n = 5_000
        store, _g, words = _build_store({_KEY_A: {0: rng.uniform(0.0, 40.0, n)}})
        t, _mask, _scale, m = next(read_tensors(store, "12/h_tdigest", n_bins=128, resolution=0.5))
        assert m == words[_KEY_A]
        # Most of the population should land in-window (uniform [0,40] in a 64 m
        # window anchored at floor of the 5th pct).
        assert 0.8 * n <= t[0, 0].sum() <= n

    @pytest.mark.parametrize(
        "dtype,np_dtype",
        [("uint16", np.uint16), ("uint32", np.uint32), ("float32", np.float32)],
    )
    def test_dtype_flag(self, dtype, np_dtype):
        rng = np.random.default_rng(12)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 30.0, 2_000)}})
        t, *_rest = next(read_tensors(store, "12/h_tdigest", dtype=dtype))
        assert t.dtype == np_dtype

    def test_raise_when_chunk_too_wide(self):
        rng = np.random.default_rng(14)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 400.0, 5_000)}})
        with pytest.raises(ValueError, match="exceeds the fixed window"):
            next(read_tensors(store, "12/h_tdigest", bottom=0.0, top=1.0))

    def test_degrade_resolution_fits(self):
        rng = np.random.default_rng(15)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 400.0, 5_000)}})
        t, *_rest = next(
            read_tensors(store, "12/h_tdigest", bottom=0.0, top=1.0, fit="degrade_resolution")
        )
        assert t.shape == (64, 64, 128)

    def test_unknown_dtype_raises(self):
        rng = np.random.default_rng(16)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 30.0, 1_000)}})
        with pytest.raises(ValueError, match="unknown dtype"):
            next(read_tensors(store, "12/h_tdigest", dtype="float64"))

    def test_missing_element_attrs_is_pointed_error(self):
        # Forward-compat guard: a vlen array WITHOUT the writer's element
        # declaration must fail loudly, not decode under a guessed layout.
        import warnings

        from zarr.codecs import VLenBytesCodec

        store = MemoryStore()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            a = zarr.create_array(
                store,
                name="12/plain",
                shape=(16,),
                chunks=(16,),
                dtype="bytes",
                serializer=VLenBytesCodec(),
                fill_value=b"",
            )
        obj = np.empty(16, dtype=object)
        obj[:] = b""
        obj[0] = b"\x00\x00\x80?"
        a[:] = obj
        with pytest.raises(ValueError, match="no ragged element declaration"):
            list(read_tensors(store, "12/plain"))

    def test_missing_morton_sibling_is_pointed_error(self):
        # The chunk-identity source is the sibling morton coordinate; a store
        # without it cannot be swept (hard break, no name fallback).
        rng = np.random.default_rng(17)
        store, _grid_, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 30.0, 500)}})
        clone = MemoryStore()
        # Copy everything except the morton coordinate array.
        clone._store_dict.update(
            {k: v for k, v in store._store_dict.items() if not k.startswith("12/morton")}
        )
        with pytest.raises(ValueError, match="no sibling 'morton' coordinate"):
            list(read_tensors(clone, "12/h_tdigest"))

    def test_unknown_spec_raises_pointed(self):
        # Version gate (review, PR #211 round 2): a future writer's attrs must
        # be adopted deliberately, never half-parsed — the coverage-envelope
        # discipline. Issue #210's typed-dtype migration bumps this seam.
        store, _g, _w = _build_store({_KEY_A: {0: np.array([1.0, 2.0])}})
        arr = zarr.open_array(store, path="12/h_tdigest", mode="r+")
        arr.attrs["ragged"] = {**arr.attrs["ragged"], "spec": "zagg-ragged/2"}
        with pytest.raises(ValueError, match="understands 'zagg-ragged/1' only"):
            list(read_tensors(store, "12/h_tdigest"))

    def test_unknown_weights_declaration_raises(self):
        # Spec §2.0 (issue #424): an undefined declaration is a future
        # revision of that section and MUST be refused, never read as either
        # defined value. The reader is the party that MUST is addressed to.
        store, _g, _w = _build_store({_KEY_A: {0: np.array([1.0, 2.0])}})
        arr = zarr.open_array(store, path="12/h_tdigest", mode="r+")
        arr.attrs["weights"] = "photons"
        with pytest.raises(ValueError, match="unknown weights declaration"):
            list(read_tensors(store, "12/h_tdigest"))
        with pytest.raises(ValueError, match="unknown weights declaration"):
            read_cell(store, "12/h_tdigest", 0)

    def test_defined_weights_declarations_open(self):
        # Both DEFINED values read: what the readers BIND of flux semantics
        # is issue #426's read-validation phase, so the gate refuses only the
        # undefined ones (an absent key is "counts" — every other test here).
        rng = np.random.default_rng(19)
        for declared in ("counts", "flux"):
            store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 30.0, 500)}})
            arr = zarr.open_array(store, path="12/h_tdigest", mode="r+")
            arr.attrs["weights"] = declared
            blocks = list(read_tensors(store, "12/h_tdigest", dtype="float32"))
            assert len(blocks) == 1

    def test_garbage_element_dtype_raises_pointed(self):
        # A corrupt dtype string surfaces as this layout's pointed error, not
        # a bare numpy TypeError with no mention of the field or contract.
        store, _g, _w = _build_store({_KEY_A: {0: np.array([1.0, 2.0])}})
        arr = zarr.open_array(store, path="12/h_tdigest", mode="r+")
        meta = dict(arr.attrs["ragged"])
        meta["element"] = {**meta["element"], "dtype": "not-a-dtype"}
        arr.attrs["ragged"] = meta
        with pytest.raises(ValueError, match="unreadable element dtype"):
            list(read_tensors(store, "12/h_tdigest"))

    def test_sharded_and_regular_layouts_read_identically(self):
        """Q1 pin (issue #209): the K>1 flat layouts differ on disk — one
        object per shard (ShardingCodec) vs one per inner chunk (regular) —
        but both are self-describing, so ONE reader path yields identical
        tensors + chunk ids from the same logical data."""
        from mortie import generate_morton_children

        rng = np.random.default_rng(18)
        shard6 = morton_word(_KEY_A)

        # Grid B: order-6 shards of K=16 order-8 inner chunks, ShardingCodec.
        cfg = _cfg()
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["grid"]["sharded"] = True
        grid_b = HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        # Grid A: order-8 shards (chunk == shard), regular chunks — the SAME
        # 256-cell order-8 read chunks as B's inner chunks.
        grid_a = HealpixGrid(8, 12, layout="fullsphere", config=_cfg())
        assert grid_a.cells_per_chunk == grid_b.cells_per_chunk == 256

        # The same per-cell data, addressed by order-8 sub-shard.
        data = {
            int(sub): {int(rng.integers(0, 256)): rng.uniform(5.0, 25.0, 400)}
            for sub in np.asarray(generate_morton_children(shard6, 8))[[0, 5, 11]]
        }

        store_a = MemoryStore()
        grid_a.emit_template(store_a)
        for sub, cells in data.items():
            _write_shard(grid_a, store_a, sub, cells)

        store_b = MemoryStore()
        grid_b.emit_template(store_b)
        morton_b = zarr.open_array(store_b, path="12/morton", mode="r+")
        # iter_chunks enumerates the shard's order-8 sub-cells in the same
        # (ascending morton-children) order generate_morton_children yields.
        subs = [int(s) for s in np.asarray(generate_morton_children(shard6, 8))]
        chunk_results = []
        for (block, children), sub in zip(grid_b.iter_chunks(shard6), subs):
            base = int(block[0]) * grid_b.cells_per_chunk
            morton_b[base : base + grid_b.cells_per_chunk] = np.asarray(children)
            cells = data.get(sub)
            if cells is None:
                chunk_results.append((block, pd.DataFrame(), {}))
                continue
            cell_ids = sorted(cells)
            payloads = [build_tdigest(np.asarray(cells[c]), delta=512) for c in cell_ids]
            chunk_results.append((block, pd.DataFrame(), {"h_tdigest": (payloads, cell_ids)}))
        write_shard_to_zarr(chunk_results, store_b, grid=grid_b, shard_key=shard6)

        out_a = sorted(read_tensors(store_a, "12/h_tdigest"), key=lambda o: o[-1])
        out_b = sorted(read_tensors(store_b, "12/h_tdigest"), key=lambda o: o[-1])
        assert [m for *_o, m in out_a] == [m for *_o, m in out_b] == sorted(data)
        for (t_a, mask_a, scale_a, _ma), (t_b, mask_b, scale_b, _mb) in zip(out_a, out_b):
            assert t_a.shape == (16, 16, 128)
            np.testing.assert_array_equal(t_a, t_b)
            np.testing.assert_array_equal(mask_a, mask_b)
            assert scale_a == scale_b


class TestReadRawValues:
    def test_recovers_unmerged_samples_exactly(self):
        # Few enough values (< delta) that build_tdigest performs no merges.
        vals = np.array([3.0, 1.0, 2.0, 5.0, 4.0])
        store, _g, words = _build_store({_KEY_A: {7: vals}})
        out = list(read_raw_values(store, "12/h_tdigest"))
        assert len(out) == 1
        morton, rowcol, recovered = out[0]
        assert morton == words[_KEY_A]
        # Rank 7 = 0b111 deinterleaves to (row, col) = (y, x) = (1, 3).
        assert rowcol == (1, 3)
        # Digest stores centroids sorted by mean → sorted samples.
        np.testing.assert_allclose(recovered, np.sort(vals))

    def test_merged_digest_raises(self):
        rng = np.random.default_rng(20)
        # Many values at small delta → merges (weight > 1) somewhere.
        store, _g, _w = _build_store({_KEY_A: {0: rng.standard_normal(5_000)}}, delta=64)
        with pytest.raises(ValueError, match="not losslessly recoverable"):
            list(read_raw_values(store, "12/h_tdigest"))


class TestReadLocations:
    """Issue #87: the location-channel reader yields per-cell uint64 morton
    vectors aligned with the digest rows the other readers see, bound through
    the payload array's attrs declaration (issue #209)."""

    @staticmethod
    def _point_words(n, seed):
        from conftest import point_words

        return point_words(n, seed)

    def _located_store(self, delta=512):
        vals = {3: np.array([3.0, 1.0, 2.0]), 9: np.array([5.0, 4.0])}
        locs_in = {3: self._point_words(3, 1), 9: self._point_words(2, 2)}
        store, _g, words = _build_store({_KEY_A: vals}, delta=delta, located_locs={_KEY_A: locs_in})
        return store, vals, locs_in, words[_KEY_A]

    def test_linkage_declared_in_attrs(self):
        store, _vals, _locs, _w = self._located_store()
        payload = zarr.open_array(store, path="12/h_tdigest", mode="r")
        assert payload.attrs["ragged"]["locations"] == "h_tdigest_locations"

    def test_yields_per_cell_uint64_vectors(self):
        store, vals, locs_in, word = self._located_store()
        out = {(m, rc): locs for m, rc, locs in read_locations(store, "12/h_tdigest")}
        # Cell ranks 3 and 9 deinterleave to (row, col) = (1, 1) and (2, 1).
        assert set(out) == {(word, (1, 1)), (word, (2, 1))}
        for cid, rc in [(3, (1, 1)), (9, (2, 1))]:
            locs = out[(word, rc)]
            assert locs.dtype == np.uint64
            # Loss-free regime: locations are the cell's point words co-sorted
            # with the values (digest rows sort by mean).
            expected = locs_in[cid][np.argsort(vals[cid], kind="stable")]
            np.testing.assert_array_equal(locs, expected)

    def test_aligned_with_read_raw_values(self):
        store, _vals, _locs, _w = self._located_store()
        raw = {(m, c): v for m, c, v in read_raw_values(store, "12/h_tdigest")}
        locs = {(m, c): loc for m, c, loc in read_locations(store, "12/h_tdigest")}
        assert set(raw) == set(locs)
        for key in raw:
            assert len(raw[key]) == len(locs[key])

    def test_value_only_field_raises_clearly(self):
        store, _g, _w = _build_store({_KEY_A: {0: np.array([1.0, 2.0])}})
        with pytest.raises(ValueError, match="declares no locations channel"):
            list(read_locations(store, "12/h_tdigest"))


class TestSubtreePerCellReaders:
    """Issue #351 phase 1: ``subtree=`` on the per-cell sweep readers.

    Golden contract (the ratified acceptance criterion): ``reader(...,
    subtree=w)`` equals the whole-store sweep filtered to the cells that are
    descendants of ``w``. The filter here is INDEPENDENT of the reader's span
    arithmetic — each yielded cell's word is resolved through ``cell_index``
    plus the stored ``morton`` coordinate, then coarsened with
    ``mortie.clip2order`` — so the two paths cannot share a bug.
    """

    @staticmethod
    def _filtered(store, field, out, subtree_key):
        """Whole-sweep entries whose CELL word descends from ``subtree_key``."""
        from mortie import clip2order

        from zagg.grids.morton import morton_decimal

        word = morton_word(subtree_key) if isinstance(subtree_key, str) else int(subtree_key)
        decimal = morton_decimal(word)
        order = len(decimal) - (2 if decimal.startswith("-") else 1)
        morton = zarr.open_array(store, path="12/morton", mode="r")
        kept = []
        for m, (row, col), payload in out:
            cell = int(morton[cell_index(store, field, m, row, col)])
            if int(clip2order(order, cell)[0]) == word:
                kept.append((m, (row, col), payload))
        return kept

    @staticmethod
    def _assert_same(got, expected):
        assert [(m, rc) for m, rc, _v in got] == [(m, rc) for m, rc, _v in expected]
        for (_m, _rc, g), (_m2, _rc2, e) in zip(got, expected):
            np.testing.assert_array_equal(g, e)

    @staticmethod
    def _flat():
        """Two populated shards on the flat fullsphere store; every digest
        unmerged so ``read_raw_values`` is lossless."""
        vals = {
            _KEY_A: {0: [3.0, 1.0], 5: [2.0], 100: [7.0, 4.0], 4095: [9.0]},
            _KEY_B: {7: [5.0, 6.0], 63: [8.0]},
        }
        store, _grid, words = _build_store(
            {k: {c: np.asarray(v) for c, v in cells.items()} for k, cells in vals.items()}
        )
        return store, words

    @pytest.mark.parametrize("key", [_KEY_A, _KEY_B])
    def test_flat_store_equals_filtered_sweep_both_currencies(self, key):
        store, words = self._flat()
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        expected = self._filtered(store, "12/h_tdigest", sweep, key)
        assert len(expected) > 0
        # Ratified fork (1): packed area word (int) and decimal string (str)
        # are the same currency — both normalize to the packed word.
        self._assert_same(
            list(read_raw_values(store, "12/h_tdigest", subtree=words[key])), expected
        )
        self._assert_same(list(read_raw_values(store, "12/h_tdigest", subtree=key)), expected)

    def test_sub_chunk_subtree_filters_ranks_inside_the_covering_chunk(self):
        from mortie import generate_morton_children

        store, words = self._flat()
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        # The order-9 first child of shard A covers chunk-local ranks [0, 64):
        # fixture cells 0 and 5, but not 100 or 4095. Ranks 0 and 5
        # deinterleave to (0, 0) and (0, 3) (issue #336).
        sub = int(np.asarray(generate_morton_children(words[_KEY_A], 9))[0])
        got = list(read_raw_values(store, "12/h_tdigest", subtree=sub))
        self._assert_same(got, self._filtered(store, "12/h_tdigest", sweep, sub))
        assert [rc for _m, rc, _v in got] == [(0, 0), (0, 3)]

    def test_single_cell_subtree(self):
        from mortie import generate_morton_children

        store, words = self._flat()
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        # An order-12 subtree IS one cell: rank 100 of shard A.
        cell_word = int(np.asarray(generate_morton_children(words[_KEY_A], 12))[100])
        got = list(read_raw_values(store, "12/h_tdigest", subtree=cell_word))
        assert len(got) == 1
        self._assert_same(got, self._filtered(store, "12/h_tdigest", sweep, cell_word))

    def test_sharded_store_equals_filtered_sweep(self):
        from mortie import generate_morton_children

        store = MemoryStore()
        _grid_, shard6, _targets = _sharded_store(store, populate={0, 3, 7, 12})
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        # The order-7 first child of the shard covers inner chunks 0..3, of
        # which locals 0 and 3 are populated.
        sub = int(np.asarray(generate_morton_children(shard6, 7))[0])
        got = list(read_raw_values(store, "12/h_tdigest", subtree=sub))
        self._assert_same(got, self._filtered(store, "12/h_tdigest", sweep, sub))
        assert len(got) == 2

    def test_leaf_store_subtree_equals_filtered_sweep(self):
        from mortie import generate_morton_children

        store, word, _vals = _leaf_store([0, 5, 100, 4095])
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        sub = int(np.asarray(generate_morton_children(word, 9))[0])
        got = list(read_raw_values(store, "12/h_tdigest", subtree=sub))
        self._assert_same(got, self._filtered(store, "12/h_tdigest", sweep, sub))
        assert len(got) == 2

    def test_out_of_domain_word_warns_once_then_yields_nothing(self):
        import warnings

        store, _word, _vals = _leaf_store([0, 5])
        # Shard B is disjoint from this leaf's single-root axis: the ratified
        # fork (3) — ONE warning per reader call naming the word and the axis
        # root, then an empty yield. The warning is the only discriminator vs
        # "in-domain, nothing stored" (the docstring ambiguity).
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            out = list(read_raw_values(store, "12/h_tdigest", subtree=morton_word(_KEY_B)))
        assert out == []
        msgs = [str(w.message) for w in rec if "outside this axis" in str(w.message)]
        assert msgs == [
            f"subtree {_KEY_B} is outside this axis' order-6 root {_KEY_A} — yielding nothing"
        ]

    @pytest.mark.parametrize("reader", [read_raw_values, read_locations, read_tensors])
    def test_out_of_domain_warning_is_attributed_to_the_caller(self, reader):
        import warnings

        store, _word, _vals = _leaf_store([0, 5], located=True)
        # The readers are generators, so the warning fires on the consumer's
        # first next() — the stacklevel must reach THIS frame, not a zagg one
        # (review, PR #357).
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            list(reader(store, "12/h_tdigest", subtree=morton_word(_KEY_B)))
        (w,) = [r for r in rec if "outside this axis" in str(r.message)]
        assert w.filename == __file__

    def test_ancestor_of_the_axis_root_clips_to_the_whole_axis(self):
        from mortie import clip2order

        store, word, _vals = _leaf_store([0, 4095])
        # A word ABOVE the leaf's root: every stored cell is its descendant,
        # so the subtree read equals the unrestricted sweep (the golden filter
        # keeps everything) — no warning, not an empty yield.
        parent = int(clip2order(3, word)[0])
        got = list(read_raw_values(store, "12/h_tdigest", subtree=parent))
        self._assert_same(got, list(read_raw_values(store, "12/h_tdigest")))
        assert len(got) == 2

    def test_in_domain_empty_subtree_yields_nothing_silently(self):
        import warnings

        from mortie import generate_morton_children

        store, words = self._flat()
        # Both in-domain on the fullsphere axis, both empty: an unpopulated
        # corner INSIDE stored shard A (ranks [128, 192)), and a whole shard
        # with no stored object at all.
        empty_sub = int(np.asarray(generate_morton_children(words[_KEY_A], 9))[2])
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            assert list(read_raw_values(store, "12/h_tdigest", subtree=empty_sub)) == []
            assert list(read_raw_values(store, "12/h_tdigest", subtree="3333333")) == []
        assert [w for w in rec if "outside this axis" in str(w.message)] == []

    @pytest.mark.parametrize("geometry", ["fullsphere", "leaf"])
    def test_misplaced_morton_coordinate_raises(self, geometry):
        """The identity the whole feature rests on — axis position == nested id
        minus the axis root's start — is checked, not assumed (review, PR
        #357). Rolling a stored span's morton coordinate by one cell breaks it
        while leaving every other guard satisfied (the cells still share their
        chunk ancestor), so without the check the span arithmetic would name
        plausible-but-wrong cells silently."""
        if geometry == "leaf":
            store, sub, _vals = _leaf_store([0, 5])
            blocks = [0]
        else:
            store, words = self._flat()
            grid, sub = _grid(), words[_KEY_A]
            blocks = [grid.block_index(w)[0] * grid.cells_per_chunk for w in words.values()]
        arr = zarr.open_array(store, path="12/morton", mode="r+")
        for base in blocks:
            arr[base : base + 4096] = np.roll(arr[base : base + 4096], 1)
        with pytest.raises(ValueError, match="not in canonical nested placement"):
            list(read_raw_values(store, "12/h_tdigest", subtree=sub))

    @pytest.mark.parametrize("bad", ["", "abc", "913", 3, -5])
    def test_malformed_subtree_raises(self, bad):
        store, _words = self._flat()
        with pytest.raises(ValueError):
            list(read_raw_values(store, "12/h_tdigest", subtree=bad))

    def test_deeper_than_the_cells_axis_raises(self):
        store, _words = self._flat()
        with pytest.raises(ValueError, match="deeper than"):
            list(read_raw_values(store, "12/h_tdigest", subtree=_KEY_A + "1111111"))

    def test_empty_store_yields_nothing_but_malformed_still_raises(self):
        store = MemoryStore()
        _grid().emit_template(store)
        assert list(read_raw_values(store, "12/h_tdigest", subtree=_KEY_A)) == []
        with pytest.raises(ValueError):
            list(read_raw_values(store, "12/h_tdigest", subtree="abc"))

    def test_subtree_none_is_the_default_sweep(self):
        store, _words = self._flat()
        self._assert_same(
            list(read_raw_values(store, "12/h_tdigest", subtree=None)),
            list(read_raw_values(store, "12/h_tdigest")),
        )

    def test_read_locations_subtree_equals_filtered_sweep(self):
        from mortie import generate_morton_children

        store, word, _vals = _leaf_store([3, 9, 300], located=True)
        sweep = list(read_locations(store, "12/h_tdigest"))
        # Order-9 first child: ranks [0, 64) — cells 3 and 9, not 300.
        sub = int(np.asarray(generate_morton_children(word, 9))[0])
        got = list(read_locations(store, "12/h_tdigest", subtree=sub))
        self._assert_same(got, self._filtered(store, "12/h_tdigest", sweep, sub))
        assert len(got) == 2

    def test_read_locations_out_of_domain_warns_and_is_empty(self):
        store, _word, _vals = _leaf_store([0, 7], located=True)
        with pytest.warns(UserWarning, match="outside this axis' order-6 root"):
            out = list(read_locations(store, "12/h_tdigest", subtree=morton_word(_KEY_B)))
        assert out == []


class TestSubtreeReadTensors:
    """Issue #351 phase 2: ``subtree=`` on :func:`read_tensors`.

    Golden contract: the subtree read equals the whole-store sweep filtered
    to the blocks below ``w`` — and the equality is BIT-WISE because blocks
    are whole read chunks: each emitted block derives its z-window from the
    same populated cell set in both reads (no cell outside the span ever
    joins a block), so tensor, mask, ``(offset, gain)``, and morton id all
    match exactly. Sub-chunk subtrees are refused in v1 (the z-window would
    re-derive from fewer cells — no longer a slice of the chunk tensor).
    """

    @staticmethod
    def _filtered(out, word, order):
        """Whole-sweep blocks whose morton id descends from ``word``."""
        from mortie import clip2order

        return [o for o in out if int(clip2order(order, o[3])[0]) == word]

    @staticmethod
    def _assert_same(got, expected):
        assert [o[3] for o in got] == [o[3] for o in expected]
        for (t, m, s, _w), (te, me, se, _we) in zip(got, expected):
            np.testing.assert_array_equal(t, te)
            np.testing.assert_array_equal(m, me)
            assert s == se

    def test_flat_store_equals_filtered_sweep_both_currencies(self):
        rng = np.random.default_rng(40)
        store, _grid_, words = _build_store(
            {
                _KEY_A: {0: rng.uniform(10, 30, 500), 4095: rng.uniform(12, 28, 400)},
                _KEY_B: {7: rng.uniform(40, 60, 300)},
            }
        )
        sweep = list(read_tensors(store, "12/h_tdigest"))
        expected = self._filtered(sweep, words[_KEY_A], 6)
        assert len(expected) == 1
        for sub in (words[_KEY_A], _KEY_A):
            self._assert_same(list(read_tensors(store, "12/h_tdigest", subtree=sub)), expected)

    def test_span_covering_two_stored_objects(self):
        """A span wider than ONE stored object (review, PR #357): the order-5
        parent of two populated sibling order-6 shards. The clip loop skips a
        disjoint object AND clips two covered ones in the same read, and
        ``block_order=5`` assembles a single block ACROSS the two objects."""
        sibling = "1121122"  # _KEY_A's order-6 sibling; parent "112112"
        parent = "112112"
        store, _grid_, words = _build_store(
            {
                _KEY_A: {0: np.asarray([3.0, 1.0]), 4095: np.asarray([9.0])},
                sibling: {9: np.asarray([5.0, 6.0]), 200: np.asarray([7.0])},
                _KEY_B: {7: np.asarray([8.0])},  # disjoint: never fetched
            }
        )
        word = morton_word(parent)
        # Per-cell: every cell of BOTH objects, none of shard B's.
        sweep = list(read_raw_values(store, "12/h_tdigest"))
        got_cells = list(read_raw_values(store, "12/h_tdigest", subtree=parent))
        TestSubtreePerCellReaders._assert_same(
            got_cells,
            TestSubtreePerCellReaders._filtered(store, "12/h_tdigest", sweep, parent),
        )
        assert len(got_cells) == 4
        # Per chunk: one block per stored object, both yielded.
        per_chunk = list(read_tensors(store, "12/h_tdigest", subtree=parent))
        assert [o[3] for o in per_chunk] == [words[_KEY_A], words[sibling]]
        self._assert_same(
            per_chunk, self._filtered(list(read_tensors(store, "12/h_tdigest")), word, 5)
        )
        # And ONE order-5 block assembled across the two stored objects, equal
        # to the same block from the unrestricted block_order=5 sweep.
        assembled = self._filtered(
            list(read_tensors(store, "12/h_tdigest", block_order=5)), word, 5
        )
        got = list(read_tensors(store, "12/h_tdigest", subtree=parent, block_order=5))
        assert len(got) == len(assembled) == 1 and got[0][0].shape == (128, 128, 128)
        self._assert_same(got, assembled)

    def test_sharded_store_per_chunk_and_block_assembly(self):
        from mortie import generate_morton_children

        store = MemoryStore()
        _grid_, shard6, _t = _sharded_store(store, populate={0, 3, 7, 12})
        # Order-7 first child of the shard = inner chunks 0..3 (locals 0, 3
        # populated). Per-chunk blocks AND one assembled order-7 block must
        # both equal the filtered whole sweep at the same block_order.
        sub = int(np.asarray(generate_morton_children(shard6, 7))[0])
        per_chunk = self._filtered(list(read_tensors(store, "12/h_tdigest")), sub, 7)
        assert len(per_chunk) == 2
        self._assert_same(list(read_tensors(store, "12/h_tdigest", subtree=sub)), per_chunk)
        assembled = self._filtered(list(read_tensors(store, "12/h_tdigest", block_order=7)), sub, 7)
        got = list(read_tensors(store, "12/h_tdigest", subtree=sub, block_order=7))
        assert len(got) == len(assembled) == 1 and got[0][0].shape == (32, 32, 128)
        self._assert_same(got, assembled)

    def test_sharded_get_accounting_fetches_only_the_covering_span(self):
        from mortie import generate_morton_children

        store = _CountingStore()
        grid, shard6, _t = _sharded_store(store, populate={0, 3, 7, 12})
        sub = int(np.asarray(generate_morton_children(shard6, 7))[0])
        store.gets.clear()
        out = list(read_tensors(store, "12/h_tdigest", subtree=sub))
        assert len(out) == 2
        data_gets = [g for g in store.gets if "/h_tdigest/c/" in g[0]]
        # The §1.5 read plan on a sharded store: the shard-index suffix plus
        # ONLY the covering inner chunks (populated locals 0 and 3 of the
        # span's chunks 0..3; zarr may coalesce the adjacent ranges) — all
        # ranged GETs, never the whole object.
        assert all(rng_ is not None for _k, rng_, _n in data_gets)
        (obj_key, _r0, n0), *chunk_gets = data_gets
        assert n0 == 16 * grid.chunks_per_shard + 4  # the shard-index suffix
        assert len(chunk_gets) >= 1
        # Decode the shard index: local 7's payload starts right after locals
        # 0 and 3, so every fetched chunk range must end BEFORE it — the
        # out-of-span locals 7 and 12 are never touched.
        obj = store._store_dict[obj_key].to_bytes()
        idx = np.frombuffer(obj[-n0:-4], dtype="<u8").reshape(-1, 2)
        chunk7_start = int(idx[7][0])
        assert chunk7_start > 0
        assert all(int(r.end) <= chunk7_start for _k, r, _n in chunk_gets)

    def test_flat_get_accounting_skips_disjoint_objects(self):
        rng = np.random.default_rng(42)
        grid = _grid()
        store = _CountingStore()
        grid.emit_template(store)
        for key in (_KEY_A, _KEY_B):
            _write_shard(grid, store, key, {3: rng.uniform(10.0, 30.0, 200)})
        store.gets.clear()
        out = list(read_tensors(store, "12/h_tdigest", subtree=_KEY_A))
        assert [o[3] for o in out] == [morton_word(_KEY_A)]
        data_gets = [g for g in store.gets if "/h_tdigest/c/" in g[0]]
        # Flat layout: one whole-object GET for the covering chunk; shard B's
        # object is never touched.
        b_block = grid.block_index(morton_word(_KEY_B))[0]
        assert len(data_gets) == 1
        assert not data_gets[0][0].endswith(f"c/{b_block}")

    @pytest.mark.parametrize("reader", [read_tensors, read_raw_values])
    def test_subtree_read_lists_the_data_keys_once(self, reader):
        from mortie import generate_morton_children

        store = _CountingStore()
        _grid_, shard6, _t = _sharded_store(store, populate={0, 3, 7, 12})
        sub = int(np.asarray(generate_morton_children(shard6, 7))[0])
        store.lists.clear()
        assert len(list(reader(store, "12/h_tdigest", subtree=sub))) >= 1
        # The span resolution and the sweep share ONE listing: on the flat
        # fullsphere layout this is a paginated LIST per ~1000 objects, so a
        # second one would double the only whole-array-scale operation left on
        # a read path framed as "never a whole-array sweep" (review, PR #357).
        assert store.lists.count("12/h_tdigest/c/") == 1

    def test_sub_chunk_subtree_refused_pointing_at_per_cell_readers(self):
        from mortie import generate_morton_children

        store, words = TestSubtreePerCellReaders._flat()
        sub = int(np.asarray(generate_morton_children(words[_KEY_A], 9))[0])
        with pytest.raises(ValueError, match="read_raw_values") as excinfo:
            list(read_tensors(store, "12/h_tdigest", subtree=sub))
        assert "read_cell" in str(excinfo.value)

    def test_block_order_coarser_than_the_subtree_raises(self):
        from mortie import generate_morton_children

        store = MemoryStore()
        _grid_, shard6, _t = _sharded_store(store, populate={0, 3})
        sub = int(np.asarray(generate_morton_children(shard6, 7))[0])
        # The composed bound: subtree_order (7) ≤ block_order ≤ chunk_order (8).
        with pytest.raises(ValueError, match="block_order .* out of range"):
            list(read_tensors(store, "12/h_tdigest", subtree=sub, block_order=6))
        # Both in-bound ends still read: block_order == chunk_order (8) gives
        # per-chunk blocks (populated locals 0 and 3), block_order ==
        # subtree_order (7) one assembled block.
        assert len(list(read_tensors(store, "12/h_tdigest", subtree=sub, block_order=8))) == 2
        assert len(list(read_tensors(store, "12/h_tdigest", subtree=sub, block_order=7))) == 1

    def test_ancestor_word_block_order_floor_is_the_axis_root(self):
        from mortie import clip2order

        store, word = _sharded_leaf_store([11, 300])
        # A word ABOVE the leaf's order-6 root clips to the whole axis, so the
        # block_order floor is the ROOT's order — not the subtree's. That is
        # the documented bound max(subtree_order, axis_root_order) ≤
        # block_order ≤ chunk_order (review, PR #357).
        above = int(clip2order(4, word)[0])
        with pytest.raises(ValueError, match="must be between 6 and the chunk order 8"):
            list(read_tensors(store, "12/h_tdigest", subtree=above, block_order=5))
        assert len(list(read_tensors(store, "12/h_tdigest", subtree=above, block_order=6))) == 1

    def test_hive_leaf_subtree_keeps_the_occupancy_mask(self):
        from mortie import generate_morton_children

        store, word = _sharded_leaf_store([11, 300, 1000, 4000], extra_occupied=[12, 301, 2500])
        sub = int(np.asarray(generate_morton_children(word, 7))[0])
        # Child 0 covers leaf cells [0, 1024) = read chunks 0..3: digests
        # 11/300/1000 (chunks 0, 1, 3) and occupancy-only cells 12/301 — but
        # not 4000 (chunk 15) or 2500 (chunk 9, which has no digest at all).
        per_chunk = self._filtered(list(read_tensors(store, "12/h_tdigest")), sub, 7)
        got = list(read_tensors(store, "12/h_tdigest", subtree=sub))
        assert len(got) == 3
        self._assert_same(got, per_chunk)
        # The 3-state mask machinery is untouched: the occupancy-only cells
        # inside the span still read as state 1.
        assert sum(int((m == 1).sum()) for _t, m, _s, _w in got) == 2
        # And the assembled subtree block agrees with the filtered assembly.
        assembled = self._filtered(list(read_tensors(store, "12/h_tdigest", block_order=7)), sub, 7)
        self._assert_same(
            list(read_tensors(store, "12/h_tdigest", subtree=sub, block_order=7)), assembled
        )

    def test_out_of_domain_warns_once_and_empty_in_domain_is_silent(self):
        import warnings

        from mortie import generate_morton_children

        store, word = _sharded_leaf_store([11])
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            assert list(read_tensors(store, "12/h_tdigest", subtree=morton_word(_KEY_B))) == []
        msgs = [str(w.message) for w in rec if "outside this axis" in str(w.message)]
        assert msgs == [
            f"subtree {_KEY_B} is outside this axis' order-6 root {_KEY_A} — yielding nothing"
        ]
        # In-domain child with nothing stored (chunks 12..15): silent empty —
        # the docstring ambiguity; the absent warning is the in-domain signal.
        empty_sub = int(np.asarray(generate_morton_children(word, 7))[3])
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            assert list(read_tensors(store, "12/h_tdigest", subtree=empty_sub)) == []
        assert [w for w in rec if "outside this axis" in str(w.message)] == []

    def test_malformed_and_too_deep_raise(self):
        store, _words = TestSubtreePerCellReaders._flat()
        with pytest.raises(ValueError):
            list(read_tensors(store, "12/h_tdigest", subtree="abc"))
        with pytest.raises(ValueError, match="deeper than"):
            list(read_tensors(store, "12/h_tdigest", subtree=_KEY_A + "1111111"))


class TestReadCell:
    """Issue #209 single-cell path: index the vlen array directly."""

    def test_decodes_declared_element(self):
        vals = np.array([3.0, 1.0, 2.0])
        store, grid, words = _build_store({_KEY_A: {7: vals}})
        base = grid.block_index(words[_KEY_A])[0] * grid.cells_per_chunk
        digest = read_cell(store, "12/h_tdigest", base + 7)
        assert digest.shape == (3, 2)
        np.testing.assert_allclose(digest[:, 0], np.sort(vals))
        # An absent cell decodes to the zero-length element.
        assert read_cell(store, "12/h_tdigest", base + 8).shape == (0, 2)

    def test_reads_locations_sibling(self):
        vals = {3: np.array([3.0, 1.0, 2.0])}
        locs = {3: TestReadLocations._point_words(3, 4)}
        store, grid, words = _build_store({_KEY_A: vals}, located_locs={_KEY_A: locs})
        base = grid.block_index(words[_KEY_A])[0] * grid.cells_per_chunk
        out = read_cell(store, "12/h_tdigest_locations", base + 3)
        assert out.dtype == np.uint64 and out.shape == (3,)

    def test_out_of_range_and_negative_raise_pointed(self):
        """Review (PR #211 round 2): zarr basic selection CLAMPS the slice, so
        without the bounds guard an out-of-range cell died on a bare unpack;
        -1 gets neither numpy wrap nor a comprehensible error."""
        store, _grid_, _w = _build_store({_KEY_A: {0: np.array([1.0, 2.0])}})
        n = zarr.open_array(store, path="12/h_tdigest", mode="r").shape[0]
        with pytest.raises(IndexError, match=f"cell {n} out of range"):
            read_cell(store, "12/h_tdigest", n)
        with pytest.raises(IndexError, match="negative indices do not wrap"):
            read_cell(store, "12/h_tdigest", -1)

    def test_two_ranged_gets_on_sharded_store(self):
        """One cell from a ShardingCodec'd store = exactly 2 ranged GETs on
        the shard object (index suffix + one inner chunk), never the whole
        object — the random-access property the layout was chosen for."""
        store = _CountingStore()
        grid, _shard6, targets = _sharded_store(store, populate=range(16))

        store.gets.clear()
        digest = read_cell(store, "12/h_tdigest", targets[0])
        assert digest.shape[1] == 2 and digest.shape[0] > 0
        data_gets = [g for g in store.gets if "/h_tdigest/c/" in g[0]]
        assert len(data_gets) == 2
        (obj_key, _r0, n0), (_k1, _r1, n1) = data_gets
        obj_size = len(store._store_dict[obj_key].to_bytes())
        assert n0 == 16 * grid.chunks_per_shard + 4  # the shard-index suffix
        assert n1 <= obj_size // 4  # one compressed inner chunk, not the object

    def test_sweep_reads_each_stored_object_once(self):
        """Review (PR #211 round 2): the whole-store sweep reads each stored
        span in ONE slice, so a stored object is fetched exactly ONCE (zarr
        reads a whole outer chunk in a single GET and splits it locally) —
        never a per-inner-chunk index re-fetch (K identical suffix reads per
        shard, ~256 redundant round trips at production geometry)."""
        store = _CountingStore()
        _grid_, _shard6, targets = _sharded_store(store, populate={0, 3, 7, 12})
        assert len(targets) == 4

        store.gets.clear()
        out = list(read_tensors(store, "12/h_tdigest"))
        assert len(out) == 4
        data_gets = [g for g in store.gets if "/h_tdigest/c/" in g[0]]
        assert len(data_gets) == 1  # the whole stored object, once


class TestReadParityWithoutConsolidation:
    """Issue #191: consolidation is now opt-out, so published stores are read
    without a consolidated-metadata blob. Pin that every reader navigates a
    non-consolidated store to the same bytes it would read from a consolidated
    one — readers reach paths directly (``zarr.open_array`` per node), never
    the consolidated blob."""

    @staticmethod
    def _point_words(n, seed):
        from conftest import point_words

        return point_words(n, seed)

    def _build_store(self):
        """A two-shard located t-digest field on the vlen layout (issue #209),
        written through the production template + per-chunk writer, with no
        consolidated metadata."""
        shards = {
            _KEY_A: {0: np.array([10.0, 11.0, 12.0]), 5: np.array([13.0, 14.0])},
            _KEY_B: {2: np.array([20.0, 21.0]), 63: np.array([22.0, 23.0, 24.0])},
        }
        locs = {
            key: {c: self._point_words(len(v), seed) for c, v in cells.items()}
            for seed, (key, cells) in enumerate(shards.items(), start=1)
        }
        store, _g, _w = _build_store(shards, located_locs=locs)
        return store

    @staticmethod
    def _read_all(store):
        tensors = {
            m: t
            for t, _mask, _scale, m in read_tensors(
                store, "12/h_tdigest", n_bins=64, resolution=0.5
            )
        }
        raw = {(m, c): v for m, c, v in read_raw_values(store, "12/h_tdigest")}
        locs = {(m, c): loc for m, c, loc in read_locations(store, "12/h_tdigest")}
        return tensors, raw, locs

    def test_non_consolidated_is_navigable_and_reads_parity(self):
        import warnings

        store = self._build_store()

        # A freshly written store carries NO consolidated-metadata blob — the
        # default now (issue #191). Readers must still navigate it.
        root = zarr.open_group(store, mode="r", zarr_format=3)
        assert root.metadata.consolidated_metadata is None

        tensors_plain, raw_plain, locs_plain = self._read_all(store)
        # Sanity: the readers actually reached the data. Ranks 0 and 63
        # deinterleave to (row, col) = (0, 0) and (7, 7).
        word_a, word_b = morton_word(_KEY_A), morton_word(_KEY_B)
        assert set(tensors_plain) == {word_a, word_b}
        assert (word_a, (0, 0)) in raw_plain and (word_b, (7, 7)) in raw_plain

        # Consolidate the SAME store and re-read: consolidation only adds a
        # metadata blob no reader consults, so every byte must match.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            zarr.consolidate_metadata(store, zarr_format=3)
        assert (
            zarr.open_group(store, mode="r", zarr_format=3).metadata.consolidated_metadata
            is not None
        )

        tensors_cons, raw_cons, locs_cons = self._read_all(store)

        assert set(tensors_cons) == set(tensors_plain)
        for m in tensors_plain:
            np.testing.assert_array_equal(tensors_plain[m], tensors_cons[m])
        assert set(raw_cons) == set(raw_plain)
        for key in raw_plain:
            np.testing.assert_array_equal(raw_plain[key], raw_cons[key])
        assert set(locs_cons) == set(locs_plain)
        for key in locs_plain:
            np.testing.assert_array_equal(locs_plain[key], locs_cons[key])


class TestTimeAxisReader:
    """The §8 time-coordinate read surface (issue #443).

    One code path for both encodings: the reader resolves the declaration
    from the array's own attrs, so a caller never has to know which era a
    store was written in.
    """

    def _store(self, encoding):
        from zagg.config import load_config_from_dict
        from zagg.processing.raster import emit_raster_template, raster_time_index

        cfg = load_config_from_dict(
            {
                "data_source": {
                    "reader": "raster",
                    "bands": {"red": {"asset": "red", "dtype": "uint16"}},
                },
                "output": {
                    "grid": {"type": "healpix", "parent_order": 4, "child_order": 6},
                    "time_encoding": encoding,
                },
            }
        )
        grid = HealpixGrid(4, 6, config=cfg)
        granules = [
            [
                {
                    "id": "a",
                    "assets": {"red": "x"},
                    "datetime": "2025-06-15T15:06:40+00:00",
                    "time_key": "dt-1",
                },
                {
                    "id": "b",
                    "assets": {"red": "y"},
                    "datetime": "2025-06-15T15:06:47+00:00",
                    "time_key": "dt-1",
                },
                {
                    "id": "c",
                    "assets": {"red": "z"},
                    "datetime": "2025-06-16T15:06:40+00:00",
                    "time_key": "dt-2",
                },
            ]
        ]
        _index, times = raster_time_index(granules, encoding=encoding)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, times)
        return store, grid, times

    def test_toc_axis_decodes_to_conservative_bounds(self):
        from zagg.readers import read_time_axis

        store, grid, _times = self._store("toc")
        lo, hi = read_time_axis(store, grid.group_path)
        assert lo.dtype == np.dtype("datetime64[ns]") and lo.shape == (2,)
        # The datatake's two tiles are 7 s apart: a range that contains both.
        assert lo[0] <= np.datetime64("2025-06-15T15:06:40", "ns")
        assert hi[0] > np.datetime64("2025-06-15T15:06:47", "ns")
        # The single-item group stays an exact instant.
        assert lo[1] == hi[1] == np.datetime64("2025-06-16T15:06:40", "ns")

    def test_legacy_axis_reads_through_the_same_call(self):
        from zagg.readers import read_time_axis

        store, grid, _times = self._store("microseconds")
        lo, hi = read_time_axis(store, grid.group_path)
        np.testing.assert_array_equal(lo, hi)
        assert lo[0] == np.datetime64("2025-06-15T15:06:40", "ns")

    def test_window_selection_runs_on_the_stored_words(self):
        import zarr as _zarr

        from zagg.readers import time_axis_overlaps

        store, grid, times = self._store("toc")
        arr = _zarr.open_array(
            store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        mask = time_axis_overlaps(
            arr[:], dict(arr.attrs), "2025-06-16T00:00:00", "2025-06-17T00:00:00"
        )
        np.testing.assert_array_equal(mask, [False, True])
        # No decode happened: the predicate consumed the raw uint64 words.
        assert arr.dtype == np.uint64 and times.dtype == np.uint64
