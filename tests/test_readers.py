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
        out = dict((m, t) for t, m in read_tensors(self._store(), "12/h_tdigest"))
        assert set(out) == {morton_word(_KEY_A), morton_word(_KEY_B)}
        for t in out.values():
            assert t.shape == (64, 64, 128)
            assert t.dtype == np.uint32

    def test_morton_derived_from_coordinate(self):
        # Chunk identity round trip: the per-cell morton coordinate coarsens
        # to the chunk's coverage-cell id (the packed shard word at K==1).
        mortons = sorted(m for _, m in read_tensors(self._store(), "12/h_tdigest"))
        assert mortons == sorted(morton_word(k) for k in (_KEY_A, _KEY_B))

    def test_populated_cell_placement_rowmajor(self):
        out = dict((m, t) for t, m in read_tensors(self._store(), "12/h_tdigest"))
        t = out[morton_word(_KEY_A)]
        # cell 5 → row 0, col 5; cell 4095 → row 63, col 63.
        assert t[0, 5].sum() > 0
        assert t[63, 63].sum() > 0
        # An unpopulated cell stays zero.
        assert t[10, 10].sum() == 0

    def test_counts_match_population(self):
        rng = np.random.default_rng(11)
        n = 5_000
        store, _g, words = _build_store({_KEY_A: {0: rng.uniform(0.0, 40.0, n)}})
        t, m = next(read_tensors(store, "12/h_tdigest", n_bins=128, resolution=0.5))
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
        t, _ = next(read_tensors(store, "12/h_tdigest", dtype=dtype))
        assert t.dtype == np_dtype

    def test_raise_when_chunk_too_wide(self):
        rng = np.random.default_rng(14)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 400.0, 5_000)}})
        with pytest.raises(ValueError, match="exceeds the fixed window"):
            next(read_tensors(store, "12/h_tdigest", bottom=0.0, top=1.0))

    def test_degrade_resolution_fits(self):
        rng = np.random.default_rng(15)
        store, _g, _w = _build_store({_KEY_A: {0: rng.uniform(0.0, 400.0, 5_000)}})
        t, _ = next(
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

        out_a = sorted(read_tensors(store_a, "12/h_tdigest"), key=lambda tm: tm[1])
        out_b = sorted(read_tensors(store_b, "12/h_tdigest"), key=lambda tm: tm[1])
        assert [m for _t, m in out_a] == [m for _t, m in out_b] == sorted(data)
        for (t_a, _ma), (t_b, _mb) in zip(out_a, out_b):
            assert t_a.shape == (16, 16, 128)
            np.testing.assert_array_equal(t_a, t_b)


class TestReadRawValues:
    def test_recovers_unmerged_samples_exactly(self):
        # Few enough values (< delta) that build_tdigest performs no merges.
        vals = np.array([3.0, 1.0, 2.0, 5.0, 4.0])
        store, _g, words = _build_store({_KEY_A: {7: vals}})
        out = list(read_raw_values(store, "12/h_tdigest"))
        assert len(out) == 1
        morton, cell_id, recovered = out[0]
        assert morton == words[_KEY_A]
        assert cell_id == 7
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
        out = {(m, c): locs for m, c, locs in read_locations(store, "12/h_tdigest")}
        assert set(out) == {(word, 3), (word, 9)}
        for (_, cid), locs in out.items():
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

    def test_two_ranged_gets_on_sharded_store(self):
        """One cell from a ShardingCodec'd store = exactly 2 ranged GETs on
        the shard object (index suffix + one inner chunk), never the whole
        object — the random-access property the layout was chosen for."""

        class CountingStore(MemoryStore):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                self.gets: list = []

            def with_read_only(self, read_only=True):
                s = CountingStore(store_dict=self._store_dict, read_only=read_only)
                s.gets = self.gets
                return s

            async def get(self, key, prototype, byte_range=None):
                r = await super().get(key, prototype, byte_range)
                if r is not None:
                    self.gets.append((key, byte_range, len(r)))
                return r

        rng = np.random.default_rng(21)
        cfg = _cfg()
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["grid"]["sharded"] = True
        grid = HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        shard6 = morton_word(_KEY_A)
        chunk_results = []
        target = None
        for block, _children in grid.iter_chunks(shard6):
            payloads = [build_tdigest(rng.uniform(0.0, 30.0, 300), delta=512)]
            chunk_results.append((block, pd.DataFrame(), {"h_tdigest": (payloads, [11])}))
            if target is None:
                target = int(block[0]) * grid.cells_per_chunk + 11
        store = CountingStore()
        grid.emit_template(store)
        write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=shard6)

        store.gets.clear()
        digest = read_cell(store, "12/h_tdigest", target)
        assert digest.shape[1] == 2 and digest.shape[0] > 0
        data_gets = [g for g in store.gets if "/h_tdigest/c/" in g[0]]
        assert len(data_gets) == 2
        (obj_key, _r0, n0), (_k1, _r1, n1) = data_gets
        obj_size = len(store._store_dict[obj_key].to_bytes())
        assert n0 == 16 * grid.chunks_per_shard + 4  # the shard-index suffix
        assert n1 <= obj_size // 4  # one compressed inner chunk, not the object


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
        tensors = {m: t for t, m in read_tensors(store, "12/h_tdigest", n_bins=64, resolution=0.5)}
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
        # Sanity: the readers actually reached the data.
        word_a, word_b = morton_word(_KEY_A), morton_word(_KEY_B)
        assert set(tensors_plain) == {word_a, word_b}
        assert (word_a, 0) in raw_plain and (word_b, 63) in raw_plain

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
