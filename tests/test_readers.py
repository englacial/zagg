"""Tests for the t-digest → tensor read helpers — issue #79."""

import math

import numpy as np
import pytest
import zarr
from zarr.storage import MemoryStore

from zagg.csr import write_csr
from zagg.readers.tdigest_tensor import (
    _resolve_chunk_morton,
    chunk_z_range,
    rasterize_cell,
    read_raw_values,
    read_tensors,
)
from zagg.stats.tdigest import build_tdigest


def _write_chunk(store, field, morton_key, cell_to_values, *, delta=512):
    """Write one shard subgroup of per-cell t-digests under {field}/{morton_key}."""
    cell_ids = sorted(cell_to_values)
    payloads = [build_tdigest(np.asarray(cell_to_values[c]), delta=delta) for c in cell_ids]
    write_csr(store, f"{field}/{morton_key}", payloads, cell_ids)


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
        store = MemoryStore()
        rng = np.random.default_rng(10)
        # Two chunks (parent mortons 100 and 250), a few populated cells each.
        _write_chunk(
            store,
            "h_tdigest",
            100,
            {
                0: rng.uniform(10.0, 30.0, 3_000),
                5: rng.uniform(12.0, 28.0, 2_000),
                4095: rng.uniform(11.0, 29.0, 1_500),
            },
        )
        _write_chunk(
            store,
            "h_tdigest",
            250,
            {7: rng.uniform(40.0, 60.0, 2_500), 63: rng.uniform(42.0, 58.0, 2_000)},
        )
        return store

    def test_shape_and_dtype_default(self):
        out = dict((m, t) for t, m in read_tensors(self._store(), "h_tdigest"))
        assert set(out) == {100, 250}
        for t in out.values():
            assert t.shape == (64, 64, 128)
            assert t.dtype == np.uint32

    def test_morton_recovered_from_subgroup_name(self):
        mortons = sorted(m for _, m in read_tensors(self._store(), "h_tdigest"))
        assert mortons == [100, 250]

    def test_populated_cell_placement_rowmajor(self):
        out = dict((m, t) for t, m in read_tensors(self._store(), "h_tdigest"))
        t = out[100]
        # cell 5 → row 0, col 5; cell 4095 → row 63, col 63.
        assert t[0, 5].sum() > 0
        assert t[63, 63].sum() > 0
        # An unpopulated cell stays zero.
        assert t[10, 10].sum() == 0

    def test_counts_match_population(self):
        store = MemoryStore()
        rng = np.random.default_rng(11)
        n = 5_000
        _write_chunk(store, "f", 1, {0: rng.uniform(0.0, 40.0, n)})
        t, m = next(read_tensors(store, "f", n_bins=128, resolution=0.5))
        assert m == 1
        # Most of the population should land in-window (uniform [0,40] in a 64 m
        # window anchored at floor of the 5th pct).
        assert 0.8 * n <= t[0, 0].sum() <= n

    @pytest.mark.parametrize(
        "dtype,np_dtype",
        [("uint16", np.uint16), ("uint32", np.uint32), ("float32", np.float32)],
    )
    def test_dtype_flag(self, dtype, np_dtype):
        store = MemoryStore()
        rng = np.random.default_rng(12)
        _write_chunk(store, "f", 1, {0: rng.uniform(0.0, 30.0, 2_000)})
        t, _ = next(read_tensors(store, "f", dtype=dtype))
        assert t.dtype == np_dtype

    def test_morton_coord_array_preferred(self):
        store = MemoryStore()
        rng = np.random.default_rng(13)
        _write_chunk(store, "f", 100, {0: rng.uniform(0.0, 30.0, 2_000)})
        _write_chunk(store, "f", 250, {0: rng.uniform(0.0, 30.0, 2_000)})
        # Sibling coord maps sorted chunk order [100, 250] → custom mortons.
        arr = zarr.open_array(
            store, path="f/morton", mode="w", shape=(2,), chunks=(2,), dtype="uint64"
        )
        arr[...] = np.array([900, 901], dtype=np.uint64)
        mortons = sorted(m for _, m in read_tensors(store, "f"))
        assert mortons == [900, 901]

    def test_morton_coord_array_mixed_digit_keys(self):
        # Regression: subgroup names of differing digit counts must align with
        # the coord array in numeric (not lexicographic) order — a lexicographic
        # zip would pair "1000" before "99" and mis-assign mortons.
        store = MemoryStore()
        rng = np.random.default_rng(33)
        for key in (99, 100, 1000):
            _write_chunk(store, "f", key, {0: rng.uniform(0.0, 30.0, 1_000)})
        arr = zarr.open_array(
            store, path="f/morton", mode="w", shape=(3,), chunks=(3,), dtype="uint64"
        )
        # Coord in ascending-morton chunk order (99, 100, 1000).
        arr[...] = np.array([900, 901, 902], dtype=np.uint64)
        mapping = _resolve_chunk_morton(store, "f", ["99", "100", "1000"], 3)
        assert mapping == {"99": 900, "100": 901, "1000": 902}

    def test_raise_when_chunk_too_wide(self):
        store = MemoryStore()
        rng = np.random.default_rng(14)
        _write_chunk(store, "f", 1, {0: rng.uniform(0.0, 400.0, 5_000)})
        with pytest.raises(ValueError, match="exceeds the fixed window"):
            next(read_tensors(store, "f", bottom=0.0, top=1.0))

    def test_degrade_resolution_fits(self):
        store = MemoryStore()
        rng = np.random.default_rng(15)
        _write_chunk(store, "f", 1, {0: rng.uniform(0.0, 400.0, 5_000)})
        t, _ = next(read_tensors(store, "f", bottom=0.0, top=1.0, fit="degrade_resolution"))
        assert t.shape == (64, 64, 128)

    def test_unknown_dtype_raises(self):
        store = MemoryStore()
        rng = np.random.default_rng(16)
        _write_chunk(store, "f", 1, {0: rng.uniform(0.0, 30.0, 1_000)})
        with pytest.raises(ValueError, match="unknown dtype"):
            next(read_tensors(store, "f", dtype="float64"))


class TestReadRawValues:
    def test_recovers_unmerged_samples_exactly(self):
        store = MemoryStore()
        # Few enough values (< delta) that build_tdigest performs no merges.
        vals = np.array([3.0, 1.0, 2.0, 5.0, 4.0])
        _write_chunk(store, "f", 42, {7: vals}, delta=512)
        out = list(read_raw_values(store, "f"))
        assert len(out) == 1
        morton, cell_id, recovered = out[0]
        assert morton == 42
        assert cell_id == 7
        # Digest stores centroids sorted by mean → sorted samples.
        np.testing.assert_allclose(recovered, np.sort(vals))

    def test_merged_digest_raises(self):
        store = MemoryStore()
        rng = np.random.default_rng(20)
        # Many values at small delta → merges (weight > 1) somewhere.
        _write_chunk(store, "f", 1, {0: rng.standard_normal(5_000)}, delta=64)
        with pytest.raises(ValueError, match="not losslessly recoverable"):
            list(read_raw_values(store, "f"))
