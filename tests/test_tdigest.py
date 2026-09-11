"""Tests for the pure-numpy t-digest — issue #48, phase 4."""

import numpy as np
import pytest
from conftest import point_words as _point_words
from conftest import toc_words as _toc_words

from zagg.stats.tdigest import (
    build_tdigest,
    build_tdigest_pairwise,
    cdf_from_tdigest,
    merge_tdigests,
    merge_tdigests_kway,
    quantile_from_tdigest,
)


class TestBuildTDigest:
    def test_empty_input_returns_empty(self):
        out = build_tdigest(np.array([]))
        assert out.shape == (0, 2)
        assert out.dtype == np.dtype("float32")

    def test_all_nan_returns_empty(self):
        out = build_tdigest(np.array([np.nan, np.nan]))
        assert out.shape == (0, 2)

    def test_single_value(self):
        out = build_tdigest(np.array([42.0]))
        assert out.shape == (1, 2)
        assert float(out[0, 0]) == pytest.approx(42.0)
        assert float(out[0, 1]) == pytest.approx(1.0)

    def test_output_shape_2_columns(self):
        rng = np.random.default_rng(1)
        vals = rng.standard_normal(200)
        out = build_tdigest(vals)
        assert out.ndim == 2
        assert out.shape[1] == 2

    def test_dtype_is_float32(self):
        out = build_tdigest(np.arange(10.0))
        assert out.dtype == np.dtype("float32")

    def test_means_are_sorted(self):
        rng = np.random.default_rng(7)
        vals = rng.standard_normal(500)
        out = build_tdigest(vals)
        assert np.all(out[1:, 0] >= out[:-1, 0])

    def test_weights_sum_to_n(self):
        rng = np.random.default_rng(3)
        vals = rng.standard_normal(1000)
        out = build_tdigest(vals)
        np.testing.assert_almost_equal(float(out[:, 1].sum()), len(vals), decimal=5)

    def test_centroid_count_bounded_by_4_delta(self):
        rng = np.random.default_rng(42)
        delta = 128
        vals = rng.standard_normal(10_000)
        out = build_tdigest(vals, delta=delta)
        assert len(out) <= 4 * delta, f"Expected ≤{4 * delta} centroids, got {len(out)}"

    def test_nan_values_dropped(self):
        vals = np.array([1.0, np.nan, 3.0, np.nan, 5.0])
        out = build_tdigest(vals)
        # Weights should sum to 3 (3 finite values).
        np.testing.assert_almost_equal(float(out[:, 1].sum()), 3.0, decimal=5)

    def test_deterministic_at_fixed_delta(self):
        rng = np.random.default_rng(99)
        vals = rng.standard_normal(500)
        d1 = build_tdigest(vals, delta=256)
        d2 = build_tdigest(vals, delta=256)
        np.testing.assert_array_equal(d1, d2)

    def test_quantile_accuracy_median(self):
        """p50 from a large uniform sample is within 2% of the true median."""
        rng = np.random.default_rng(11)
        vals = rng.uniform(0, 100, size=10_000)
        digest = build_tdigest(vals, delta=512)
        est = quantile_from_tdigest(digest, 0.5)
        # t-digest is an approximate sketch; 2% tolerance is standard for δ=512.
        assert abs(est - 50.0) < 2.0, f"Median estimate {est:.2f} too far from 50.0"

    def test_wired_via_resolve_function(self):
        """The dotted path resolves through zagg.config.resolve_function (issue #48)."""
        from zagg.config import PipelineConfig, resolve_function

        f = resolve_function("zagg.stats.tdigest.build_tdigest")
        vals = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        out = f(vals, delta=512)
        assert out.shape[1] == 2

        # Full round-trip: config -> calculate_cell_statistics -> ragged payload.
        from zagg.processing import calculate_cell_statistics

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_tdigest": {
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [2],
                        "dtype": "float32",
                        "params": {"delta": 512},
                    }
                }
            }
        )
        result = calculate_cell_statistics({"h_li": vals}, config=cfg)
        assert "h_tdigest" in result
        digest = result["h_tdigest"]
        assert isinstance(digest, np.ndarray)
        assert digest.shape[1] == 2
        np.testing.assert_almost_equal(digest[:, 1].sum(), len(vals), decimal=4)


class TestMergeTDigests:
    def test_merge_empty_with_empty(self):
        out = merge_tdigests(np.empty((0, 2)), np.empty((0, 2)))
        assert out.shape == (0, 2)

    def test_merge_with_empty(self):
        d = build_tdigest(np.arange(10.0))
        out = merge_tdigests(d, np.empty((0, 2)))
        np.testing.assert_array_equal(out, d.astype(np.float32))

    def test_merge_empty_with_nonempty(self):
        d = build_tdigest(np.arange(10.0))
        out = merge_tdigests(np.empty((0, 2)), d)
        np.testing.assert_array_equal(out, d.astype(np.float32))

    def test_merged_weights_sum_to_total(self):
        rng = np.random.default_rng(5)
        v1 = rng.standard_normal(500)
        v2 = rng.standard_normal(800)
        d1 = build_tdigest(v1)
        d2 = build_tdigest(v2)
        merged = merge_tdigests(d1, d2)
        expected = float(d1[:, 1].sum()) + float(d2[:, 1].sum())
        np.testing.assert_almost_equal(float(merged[:, 1].sum()), expected, decimal=3)

    def test_merged_means_sorted(self):
        rng = np.random.default_rng(6)
        d1 = build_tdigest(rng.standard_normal(300))
        d2 = build_tdigest(rng.standard_normal(400))
        merged = merge_tdigests(d1, d2)
        assert np.all(merged[1:, 0] >= merged[:-1, 0])

    def test_merged_vs_one_shot_quantile_within_tolerance(self):
        """Merged sketch approximates quantiles close to one-shot sketch.

        The merged result should be within 2% of the one-shot median on a
        combined sample large enough for the sketch to be accurate.
        """
        rng = np.random.default_rng(17)
        v1 = rng.standard_normal(2000)
        v2 = rng.standard_normal(2000)
        combined = np.concatenate([v1, v2])
        true_median = float(np.median(combined))

        d1 = build_tdigest(v1, delta=512)
        d2 = build_tdigest(v2, delta=512)
        merged = merge_tdigests(d1, d2, delta=512)
        one_shot = build_tdigest(combined, delta=512)

        merged_est = quantile_from_tdigest(merged, 0.5)
        one_shot_est = quantile_from_tdigest(one_shot, 0.5)

        # Both should be within 5% of the true median.
        tol = 5 * abs(true_median) / 100 + 0.05
        assert abs(merged_est - true_median) < tol, (
            f"Merged p50={merged_est:.3f} too far from true median {true_median:.3f}"
        )
        assert abs(one_shot_est - true_median) < tol, (
            f"One-shot p50={one_shot_est:.3f} too far from true median {true_median:.3f}"
        )

    def test_merged_centroid_count_bounded(self):
        delta = 256
        rng = np.random.default_rng(19)
        d1 = build_tdigest(rng.standard_normal(5000), delta=delta)
        d2 = build_tdigest(rng.standard_normal(5000), delta=delta)
        merged = merge_tdigests(d1, d2, delta=delta)
        assert len(merged) <= 4 * delta, (
            f"Merged has {len(merged)} centroids, expected ≤{4 * delta}"
        )


def _contains(ancestor, member):
    """True when ``ancestor``'s cell contains ``member`` (mortie fold identity)."""
    from mortie import common_ancestor

    return int(common_ancestor(np.array([ancestor, member], dtype=np.uint64))) == int(ancestor)


class TestLocatedBuildTDigest:
    """The ``locations`` channel of ``build_tdigest`` (issue #87)."""

    def test_digest_identical_with_and_without_locations(self):
        rng = np.random.default_rng(87)
        values = rng.standard_normal(3000)
        locs = _point_words(3000, seed=1)
        digest, _ = build_tdigest(values, delta=128, locations=locs)
        assert np.array_equal(digest, build_tdigest(values, delta=128))

    def test_one_obs_centroids_round_trip_exact_point_words(self):
        # Loss-free regime (n <= delta): every centroid holds one observation,
        # so its location is that observation's exact order-29 point word.
        values = np.arange(50, dtype=np.float64)
        locs = _point_words(50, seed=2)
        digest, out_locs = build_tdigest(values, delta=512, locations=locs)
        assert np.all(digest[:, 1] == 1.0)
        assert out_locs.dtype == np.uint64
        assert np.array_equal(out_locs, locs)  # values already sorted

    def test_merged_centroid_location_contains_all_members(self):
        # delta=1 collapses everything into few centroids; each centroid's
        # location must contain every input point word.
        values = np.linspace(0.0, 1.0, 40)
        locs = _point_words(40, seed=3)
        digest, out_locs = build_tdigest(values, delta=1, locations=locs)
        assert len(out_locs) == len(digest)
        from mortie import common_ancestor

        assert int(out_locs[0]) == int(common_ancestor(locs[: int(digest[0, 1])]))
        for enclosing in out_locs:
            assert any(_contains(enclosing, m) for m in locs)

    def test_nan_values_drop_their_locations(self):
        values = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        locs = _point_words(5, seed=4)
        digest, out_locs = build_tdigest(values, delta=512, locations=locs)
        assert len(digest) == 3
        assert np.array_equal(out_locs, locs[[0, 2, 4]])

    def test_empty_returns_empty_pair(self):
        digest, locs = build_tdigest(np.array([]), locations=np.array([], dtype=np.uint64))
        assert digest.shape == (0, 2)
        assert locs.shape == (0,) and locs.dtype == np.uint64

    def test_non_uint64_locations_raise(self):
        # A silent cast would truncate a mis-declared float column into
        # plausible-looking morton words (review fold).
        with pytest.raises(ValueError, match="is not uint64"):
            build_tdigest(np.array([1.0, 2.0]), locations=np.array([1.5, 2.5]))

    def test_mismatched_lengths_raise(self):
        with pytest.raises(ValueError, match="locations shape"):
            build_tdigest(np.array([1.0, 2.0]), locations=_point_words(3, seed=5))

    def test_without_locations_returns_bare_array(self):
        out = build_tdigest(np.array([1.0, 2.0]))
        assert isinstance(out, np.ndarray)


def _centroid_ancestors_reference(locations, starts, n):
    """Pre-fast-path reference: common_ancestor over every centroid, singletons included."""
    from mortie import common_ancestor

    bounds = [*starts.tolist(), n]
    out = np.empty(len(starts), dtype=np.uint64)
    for j in range(len(starts)):
        out[j] = common_ancestor(locations[bounds[j] : bounds[j + 1]])
    return out


class TestCentroidAncestorsFastPath:
    """The singleton fast path matches the all-loop reference (issue #265)."""

    def test_mixed_singleton_and_merged_partitions(self):
        from mortie import common_ancestor, infer_order_from_morton

        from zagg.stats.tdigest import _centroid_ancestors

        locs = _point_words(20, seed=6)
        # A below-order-29 AREA word from a prior merge, spliced in as the
        # singleton member at index 4 — exactly the mixed-order input
        # ``merge_tdigests`` feeds this helper. It must copy through verbatim
        # (kind and order preserved), not be re-reduced to a point word.
        area = np.uint64(common_ancestor(locs[:4]))
        assert infer_order_from_morton(int(area)) < 29
        locs[4] = area
        # Partition mixing singletons with 2-, 3- and 4-member centroids,
        # including singleton runs at both ends.
        starts = np.array([0, 1, 3, 4, 5, 8, 12, 16, 17, 18, 19], dtype=np.int64)
        got = _centroid_ancestors(locs, starts, 20)
        assert got[starts.tolist().index(4)] == area
        assert np.array_equal(got, _centroid_ancestors_reference(locs, starts, 20))

    def test_all_singletons_round_trip_verbatim(self):
        from zagg.stats.tdigest import _centroid_ancestors

        locs = _point_words(32, seed=7)
        starts = np.arange(32, dtype=np.int64)
        assert np.array_equal(_centroid_ancestors(locs, starts, 32), locs)

    def test_build_equivalence_across_compression_knee(self):
        # End-to-end: a delta that leaves some centroids singleton and merges
        # others must produce the same locations as the reference reduction.
        from zagg.stats.tdigest import _compress

        rng = np.random.default_rng(87)
        values = np.sort(rng.standard_normal(300))
        locs = _point_words(300, seed=8)
        digest, out_locs = build_tdigest(values, delta=64, locations=locs)
        weights = digest[:, 1]
        assert np.any(weights == 1.0) and np.any(weights > 1.0), "want a mixed regime"
        _, _, starts = _compress(values, np.ones(300), 64.0)
        assert np.array_equal(out_locs, _centroid_ancestors_reference(locs, starts, 300))

    def test_singleton_fill_value_word_still_raises(self):
        # The removed per-centroid ``common_ancestor`` call validated every
        # word; the copy-through must keep that, or an invalid word (0 is the
        # configs' fill value) would raise only in the compressed regime.
        from zagg.stats.tdigest import _centroid_ancestors

        locs = _point_words(4, seed=9)
        locs[2] = np.uint64(0)
        with pytest.raises(ValueError):
            _centroid_ancestors(locs, np.arange(4, dtype=np.int64), 4)


class TestLocatedMergeTDigests:
    """The ``locations`` channel of ``merge_tdigests`` (issue #87)."""

    @staticmethod
    def _located_pair(n, delta, seed):
        rng = np.random.default_rng(seed)
        values = rng.standard_normal(n)
        locs = _point_words(n, seed=seed + 100)
        return build_tdigest(values, delta=delta, locations=locs), locs

    def test_digest_identical_with_and_without_locations(self):
        (d1, l1), _ = self._located_pair(500, 64, 1)
        (d2, l2), _ = self._located_pair(500, 64, 2)
        merged, _ = merge_tdigests(d1, d2, delta=64, locations1=l1, locations2=l2)
        assert np.array_equal(merged, merge_tdigests(d1, d2, delta=64))

    def test_merged_locations_contain_contributors(self):
        # Mixed-order fold: build-side locations are already collapsed (< order
        # 29) for multi-obs centroids; merging must still yield enclosing cells.
        (d1, l1), raw1 = self._located_pair(200, 8, 3)
        (d2, l2), raw2 = self._located_pair(200, 8, 4)
        merged, locs = merge_tdigests(d1, d2, delta=8, locations1=l1, locations2=l2)
        assert locs.dtype == np.uint64 and len(locs) == len(merged)
        # Every input centroid location is contained by some merged location.
        for member in np.concatenate([l1, l2]):
            assert any(_contains(enclosing, member) for enclosing in locs)

    def test_empty_sides(self):
        (d1, l1), _ = self._located_pair(50, 512, 5)
        empty = np.empty((0, 2), dtype=np.float32)
        no_locs = np.empty(0, dtype=np.uint64)
        merged, locs = merge_tdigests(d1, empty, locations1=l1, locations2=no_locs)
        assert np.array_equal(merged, np.asarray(d1, dtype=np.float32))
        assert np.array_equal(locs, l1)
        merged, locs = merge_tdigests(empty, d1, locations1=no_locs, locations2=l1)
        assert np.array_equal(locs, l1)
        merged, locs = merge_tdigests(empty, empty, locations1=no_locs, locations2=no_locs)
        assert merged.shape == (0, 2) and locs.shape == (0,)

    def test_non_uint64_locations_raise(self):
        (d1, l1), _ = self._located_pair(10, 512, 8)
        with pytest.raises(ValueError, match="is not uint64"):
            merge_tdigests(d1, d1, locations1=l1, locations2=l1.astype(np.int64))

    def test_one_sided_locations_raise(self):
        (d1, l1), _ = self._located_pair(10, 512, 6)
        with pytest.raises(ValueError, match="both locations1 and locations2"):
            merge_tdigests(d1, d1, locations1=l1)

    def test_misaligned_locations_raise(self):
        (d1, l1), _ = self._located_pair(10, 512, 7)
        with pytest.raises(ValueError, match="does not match"):
            merge_tdigests(d1, d1, locations1=l1, locations2=l1[:-1])


class TestQuantileFromTDigest:
    def test_empty_digest_returns_nan(self):
        assert np.isnan(quantile_from_tdigest(np.empty((0, 2)), 0.5))

    def test_single_centroid_returns_its_mean(self):
        digest = np.array([[42.0, 1.0]], dtype=np.float32)
        assert quantile_from_tdigest(digest, 0.0) == pytest.approx(42.0)
        assert quantile_from_tdigest(digest, 0.5) == pytest.approx(42.0)
        assert quantile_from_tdigest(digest, 1.0) == pytest.approx(42.0)

    def test_q0_returns_min_q1_returns_max(self):
        """With enough data and small δ, q0/q1 approximate min/max within 1%."""
        rng = np.random.default_rng(41)
        # Use n=5000 and small δ=32 so the tails form fine-grained centroids.
        vals = rng.uniform(0.0, 100.0, size=5000)
        digest = build_tdigest(vals, delta=32)
        q0 = quantile_from_tdigest(digest, 0.0)
        q1 = quantile_from_tdigest(digest, 1.0)
        assert q0 < 0.5, f"q0={q0:.3f} should be near the minimum"
        assert q1 > 99.5, f"q1={q1:.3f} should be near the maximum"


class TestTDigestDeltaSweep:
    """Phase 5 of issue #48: accuracy/width trade-off across δ ∈ {128, 256, 512, 1024}."""

    @staticmethod
    def _make_data(n: int = 20_000, seed: int = 77) -> np.ndarray:
        rng = np.random.default_rng(seed)
        return rng.standard_normal(n)

    @pytest.mark.parametrize("delta", [128, 256, 512, 1024])
    def test_centroid_count_at_most_4_delta(self, delta):
        """The sketch must stay within Dunning's 4δ centroid bound."""
        vals = self._make_data()
        digest = build_tdigest(vals, delta=delta)
        assert len(digest) <= 4 * delta, (
            f"delta={delta}: got {len(digest)} centroids, expected ≤{4 * delta}"
        )

    @pytest.mark.parametrize("delta", [128, 256, 512, 1024])
    def test_weights_sum_to_n(self, delta):
        """Total weight must equal the number of non-NaN input observations."""
        vals = self._make_data()
        digest = build_tdigest(vals, delta=delta)
        np.testing.assert_almost_equal(float(digest[:, 1].sum()), len(vals), decimal=4)

    @pytest.mark.parametrize(
        "delta,q,tol",
        [
            # Tighter tolerance at higher δ; larger tol at tails vs median.
            (128, 0.5, 0.15),  # median, δ=128: within 0.15 std dev
            (256, 0.5, 0.10),
            (512, 0.5, 0.06),
            (1024, 0.5, 0.04),
            (512, 0.1, 0.15),  # left tail
            (512, 0.9, 0.15),  # right tail
        ],
    )
    def test_quantile_error_within_tolerance(self, delta, q, tol):
        """Quantile error is within ``tol`` standard deviations of N(0,1)."""
        vals = self._make_data(n=50_000)
        true_q = float(np.quantile(vals, q))
        digest = build_tdigest(vals, delta=delta)
        est = quantile_from_tdigest(digest, q)
        err = abs(est - true_q)
        assert err < tol, (
            f"delta={delta}, q={q}: error={err:.4f} > tol={tol} (est={est:.3f}, true={true_q:.3f})"
        )

    @pytest.mark.parametrize("delta", [128, 256, 512, 1024])
    def test_larger_delta_not_worse_than_smaller_for_median(self, delta):
        """Larger δ should have equal or fewer centroid-count as a multiple of δ."""
        vals = self._make_data()
        digest = build_tdigest(vals, delta=delta)
        # Centroid count should grow sub-linearly with δ (proportional, not more).
        ratio = len(digest) / delta
        assert ratio <= 4.0, f"delta={delta}: centroid/delta ratio {ratio:.2f} > 4.0"


class TestScaleFunctionRegression:
    """Guards against the k1-budget regression where δ was inverted.

    Before the scale-function fix the per-centroid weight cap was proportional
    to δ (and independent of n), so larger δ produced *fewer*, coarser centroids
    and even a handful of points collapsed to a single centroid. These tests pin
    the correct behavior: δ is a resolution knob, the digest saturates at ~δ
    centroids, and it is loss-free until the count exceeds δ.
    """

    @pytest.mark.parametrize("n", [10, 100, 500])
    def test_loss_free_when_count_at_most_delta(self, n):
        """With n ≤ δ every observation is kept as its own weight-1 centroid."""
        rng = np.random.default_rng(n)
        vals = rng.standard_normal(n)  # distinct values w.p. 1
        digest = build_tdigest(vals, delta=512)
        assert digest.shape[0] == n, f"n={n}: expected {n} centroids, got {digest.shape[0]}"
        np.testing.assert_array_equal(digest[:, 1], np.ones(n, dtype=np.float32))

    def test_loss_free_at_delta_then_compresses(self):
        """n == δ is guaranteed loss-free; well past δ the digest compresses.

        The k1 bound guarantees loss-free for n ≤ δ (the region actually extends
        to ~1.27·δ because the left edge lags one observation), so this pins the
        guaranteed boundary at n == δ and a clearly-compressing case at n == 2δ.
        """
        delta = 256
        rng = np.random.default_rng(99)
        at = build_tdigest(rng.standard_normal(delta), delta=delta)
        over = build_tdigest(rng.standard_normal(2 * delta), delta=delta)
        assert at.shape[0] == delta, f"n==δ should be loss-free, got k={at.shape[0]}"
        assert over.shape[0] < 2 * delta, f"n==2δ must compress, got k={over.shape[0]}"

    def test_compression_begins_past_delta(self):
        """Once n exceeds δ the digest must actually compress (k < n)."""
        rng = np.random.default_rng(1)
        delta = 256
        vals = rng.standard_normal(4 * delta)
        digest = build_tdigest(vals, delta=delta)
        assert digest.shape[0] < len(vals)
        np.testing.assert_almost_equal(float(digest[:, 1].sum()), len(vals), decimal=4)

    def test_delta_controls_resolution_not_inverted(self):
        """More δ ⇒ more centroids. The old budget did the opposite."""
        rng = np.random.default_rng(2)
        vals = rng.standard_normal(50_000)
        counts = [build_tdigest(vals, delta=d).shape[0] for d in (128, 256, 512, 1024)]
        assert counts == sorted(counts), f"centroid count not monotonic in δ: {counts}"
        assert counts[-1] > 2 * counts[0], f"δ=1024 barely finer than δ=128: {counts}"

    @pytest.mark.parametrize("n", [50_000, 200_000])
    def test_centroid_count_saturates_near_delta(self, n):
        """Count stays ~δ regardless of n (≤ 2δ), instead of growing with n."""
        rng = np.random.default_rng(n)
        vals = rng.standard_normal(n)
        k = build_tdigest(vals, delta=256).shape[0]
        assert k <= 2 * 256, f"n={n}: {k} centroids exceeds 2·δ"

    def test_accuracy_not_degraded_at_high_delta_on_structured_data(self):
        """On a bimodal mixture, large δ must stay accurate.

        This is the user-visible symptom of the inversion bug: interior-quantile
        error blew up as δ grew (≈0.33 here at δ=1024 — ~60× the δ=128 error).
        A correct digest keeps high-δ error small and comparable to low-δ.
        """
        rng = np.random.default_rng(3)
        vals = np.concatenate([rng.normal(-3, 0.3, 5000), rng.normal(3, 0.3, 5000)])
        qs = [0.1, 0.25, 0.5, 0.75, 0.9]
        exact = np.quantile(vals, qs)

        def mean_err(delta):
            d = build_tdigest(vals, delta=delta)
            est = np.array([quantile_from_tdigest(d, q) for q in qs])
            return float(np.abs(est - exact).mean())

        err_lo, err_hi = mean_err(128), mean_err(1024)
        assert err_hi < 0.05, f"δ=1024 interior error {err_hi:.4f} too large"
        assert err_hi < 5 * err_lo, f"δ=1024 error {err_hi:.4f} >> δ=128 error {err_lo:.4f}"

    @pytest.mark.parametrize("q", [0.02, 0.25, 0.5, 0.75, 0.98])
    def test_quantiles_track_exact_within_tolerance(self, q):
        """Estimated quantiles track exact numpy quantiles (independent ground truth)."""
        rng = np.random.default_rng(4)
        vals = rng.standard_normal(20_000)
        digest = build_tdigest(vals, delta=512)
        est = quantile_from_tdigest(digest, q)
        exact = float(np.quantile(vals, q))
        assert abs(est - exact) < 0.05, f"q={q}: est={est:.4f} exact={exact:.4f}"

    def test_merge_saturates_near_delta(self):
        """Merging two saturated digests stays ~δ, not 2δ-and-growing."""
        rng = np.random.default_rng(5)
        delta = 512
        d1 = build_tdigest(rng.standard_normal(50_000), delta=delta)
        d2 = build_tdigest(rng.standard_normal(50_000), delta=delta)
        merged = merge_tdigests(d1, d2, delta=delta)
        assert merged.shape[0] <= 2 * delta
        np.testing.assert_almost_equal(float(merged[:, 1].sum()), 100_000, decimal=3)


class TestCdfFromTDigest:
    def test_empty_returns_nan_scalar(self):
        out = cdf_from_tdigest(np.empty((0, 2), dtype=np.float32), 3.0)
        assert isinstance(out, float)
        assert np.isnan(out)

    def test_empty_returns_nan_array(self):
        out = cdf_from_tdigest(np.empty((0, 2), dtype=np.float32), np.array([1.0, 2.0]))
        assert isinstance(out, np.ndarray)
        assert out.shape == (2,)
        assert np.all(np.isnan(out))

    def test_single_centroid_step(self):
        digest = build_tdigest(np.array([5.0]))
        assert cdf_from_tdigest(digest, 4.0) == pytest.approx(0.0)
        assert cdf_from_tdigest(digest, 5.0) == pytest.approx(1.0)
        assert cdf_from_tdigest(digest, 9.0) == pytest.approx(1.0)

    def test_endpoints_zero_and_total(self):
        rng = np.random.default_rng(1)
        vals = rng.standard_normal(5_000)
        digest = build_tdigest(vals, delta=256)
        total = float(digest[:, 1].sum())
        # Far below the minimum mean → 0; far above the maximum mean → total.
        lo = float(digest[:, 0].min()) - 100.0
        hi = float(digest[:, 0].max()) + 100.0
        assert cdf_from_tdigest(digest, lo) == pytest.approx(0.0)
        assert cdf_from_tdigest(digest, hi) == pytest.approx(total)

    def test_monotonic_non_decreasing(self):
        rng = np.random.default_rng(2)
        vals = rng.standard_normal(8_000)
        digest = build_tdigest(vals, delta=256)
        xs = np.linspace(vals.min() - 1.0, vals.max() + 1.0, 500)
        cdf = cdf_from_tdigest(digest, xs)
        assert np.all(np.diff(cdf) >= -1e-9)

    def test_scalar_in_scalar_out(self):
        digest = build_tdigest(np.arange(100.0))
        out = cdf_from_tdigest(digest, 50.0)
        assert isinstance(out, float)

    def test_array_in_array_out(self):
        digest = build_tdigest(np.arange(100.0))
        out = cdf_from_tdigest(digest, np.array([10.0, 50.0, 90.0]))
        assert isinstance(out, np.ndarray)
        assert out.shape == (3,)

    def test_matches_empirical_cdf_within_tolerance(self):
        """cdf_from_tdigest tracks the empirical CDF of the samples (as a fraction)."""
        rng = np.random.default_rng(3)
        vals = rng.standard_normal(20_000)
        digest = build_tdigest(vals, delta=512)
        total = float(digest[:, 1].sum())
        xs = np.linspace(np.quantile(vals, 0.02), np.quantile(vals, 0.98), 50)
        est_frac = np.asarray(cdf_from_tdigest(digest, xs)) / total
        emp_frac = np.searchsorted(np.sort(vals), xs, side="right") / len(vals)
        # t-digest CDF tracks the empirical CDF within a few percent.
        assert np.max(np.abs(est_frac - emp_frac)) < 0.03

    def test_inverse_consistency_with_quantile(self):
        """cdf(quantile(q)) ≈ q*total over the interior (round-trip within tolerance)."""
        rng = np.random.default_rng(7)
        vals = rng.standard_normal(20_000)
        digest = build_tdigest(vals, delta=512)
        total = float(digest[:, 1].sum())
        for q in (0.1, 0.25, 0.5, 0.75, 0.9):
            x = quantile_from_tdigest(digest, q)
            frac = cdf_from_tdigest(digest, x) / total
            assert abs(frac - q) < 0.03, f"q={q}: round-trip frac={frac:.4f}"


class TestMergeTDigestsKway:
    """The order-independent k-way fold (issue #279)."""

    @staticmethod
    def _digests(seed, k, delta=256):
        rng = np.random.default_rng(seed)
        # Enough obs per digest (well past δ) that the k-way vs pairwise fold
        # actually compresses — the loss-free regime makes them coincide.
        return [build_tdigest(rng.standard_normal(4000), delta=delta) for _ in range(k)]

    def test_order_independent(self):
        ds = self._digests(1, 6)
        ref = merge_tdigests_kway(ds, delta=256)
        assert np.array_equal(ref, merge_tdigests_kway(list(reversed(ds)), delta=256))
        for perm in ([2, 0, 4, 1, 5, 3], [5, 4, 3, 2, 1, 0], [1, 3, 5, 0, 2, 4]):
            assert np.array_equal(ref, merge_tdigests_kway([ds[i] for i in perm], delta=256))

    def test_order_independent_with_tied_means(self):
        """Discrete/quantized sources tie means across digests; the k-way fold
        must still be permutation-invariant (lexsort tie-break, issue #279).

        A stable argsort on mean alone would break these ties by concatenation
        order, so a permuted digest list could emit different bytes.
        """
        rng = np.random.default_rng(279)
        # Integers in a tiny range → every digest shares the same handful of
        # distinct means, so cross-digest ties are guaranteed.
        ds = [
            build_tdigest(rng.integers(0, 5, size=4000).astype(np.float64), delta=256)
            for _ in range(6)
        ]
        ref = merge_tdigests_kway(ds, delta=256)
        assert np.array_equal(ref, merge_tdigests_kway(list(reversed(ds)), delta=256))
        for perm in ([2, 0, 4, 1, 5, 3], [5, 4, 3, 2, 1, 0], [1, 3, 5, 0, 2, 4]):
            assert np.array_equal(ref, merge_tdigests_kway([ds[i] for i in perm], delta=256))

    def test_located_order_independent_with_tied_means(self):
        """The LOCATION channel is permutation-invariant too (issue #370).

        Tied ``(mean, weight)`` sub-centroids are interchangeable for the digest
        but carry different location words; a tie straddling a compression
        boundary used to hand the output centroid a different ancestor depending
        on part order. The location word is now the tertiary lexsort key, so both
        channels agree under permutation — and the digest bytes are unchanged
        (they equal the unlocated fold of the same inputs).
        """
        vals = np.array([1.0, 1.0, 2.0, 2.0, 3.0, 3.0])
        built = [
            build_tdigest(vals, delta=4, locations=_point_words(6, seed=s)) for s in (11, 12, 13)
        ]
        ds = [d for d, _ in built]
        ls = [ln for _, ln in built]
        ref, ref_locs = merge_tdigests_kway(ds, delta=4, locations=ls)
        assert np.array_equal(ref, merge_tdigests_kway(ds, delta=4))
        for perm in ([2, 0, 1], [1, 2, 0], [2, 1, 0]):
            out, out_locs = merge_tdigests_kway(
                [ds[i] for i in perm], delta=4, locations=[ls[i] for i in perm]
            )
            assert np.array_equal(ref, out)
            assert np.array_equal(ref_locs, out_locs)

    def test_pairwise_fold_is_order_dependent(self):
        """Contrast: the pairwise left-fold is NOT associative (issue #279)."""
        ds = self._digests(2, 6)

        def fold(seq):
            held = None
            for d in seq:
                held = d if held is None else merge_tdigests(held, d, delta=256)
            return held

        fwd, rev = fold(ds), fold(list(reversed(ds)))
        assert not np.array_equal(fwd, rev)  # order matters for pairwise

    def test_kway_differs_from_pairwise_past_two(self):
        """3+ compressing digests: k-way (single pass) ≠ pairwise left-fold."""
        ds = self._digests(3, 3)
        kway = merge_tdigests_kway(ds, delta=256)
        pair = merge_tdigests(merge_tdigests(ds[0], ds[1], delta=256), ds[2], delta=256)
        assert not np.array_equal(kway, pair)

    def test_weights_sum_to_total(self):
        ds = self._digests(4, 5)
        merged = merge_tdigests_kway(ds, delta=256)
        expected = sum(float(d[:, 1].sum()) for d in ds)
        np.testing.assert_almost_equal(float(merged[:, 1].sum()), expected, decimal=2)

    def test_centroid_count_bounded(self):
        ds = self._digests(5, 8, delta=128)
        merged = merge_tdigests_kway(ds, delta=128)
        assert len(merged) <= 4 * 128

    def test_means_sorted(self):
        merged = merge_tdigests_kway(self._digests(6, 4), delta=256)
        assert np.all(merged[1:, 0] >= merged[:-1, 0])

    def test_empty_list(self):
        out = merge_tdigests_kway([])
        assert out.shape == (0, 2) and out.dtype == np.dtype("float32")

    def test_all_empty(self):
        empty = np.empty((0, 2), dtype=np.float32)
        out = merge_tdigests_kway([empty, empty])
        assert out.shape == (0, 2)

    def test_single_nonempty_returned_as_is(self):
        d = build_tdigest(np.arange(10.0))
        out = merge_tdigests_kway([d])
        np.testing.assert_array_equal(out, d.astype(np.float32))

    def test_skips_empty_digests(self):
        d1 = build_tdigest(np.arange(100.0), delta=64)
        d2 = build_tdigest(np.arange(100.0, 200.0), delta=64)
        empty = np.empty((0, 2), dtype=np.float32)
        with_gaps = merge_tdigests_kway([empty, d1, empty, d2, empty], delta=64)
        clean = merge_tdigests_kway([d1, d2], delta=64)
        np.testing.assert_array_equal(with_gaps, clean)

    def test_matches_one_shot_quantiles_within_tolerance(self):
        rng = np.random.default_rng(11)
        parts_raw = [rng.standard_normal(4000) for _ in range(5)]
        pooled = np.concatenate(parts_raw)
        merged = merge_tdigests_kway([build_tdigest(p, delta=512) for p in parts_raw], delta=512)
        for q in (0.1, 0.5, 0.9):
            assert abs(quantile_from_tdigest(merged, q) - float(np.quantile(pooled, q))) < 0.05

    def test_located_channel_contains_contributors(self):
        (d1, l1), (d2, l2) = (
            build_tdigest(np.linspace(0, 1, 40), delta=4, locations=_point_words(40, seed=1)),
            build_tdigest(np.linspace(0, 1, 40), delta=4, locations=_point_words(40, seed=2)),
        )
        merged, locs = merge_tdigests_kway([d1, d2], delta=4, locations=[l1, l2])
        assert locs.dtype == np.uint64 and len(locs) == len(merged)
        for member in np.concatenate([l1, l2]):
            assert any(_contains(enclosing, member) for enclosing in locs)

    def test_located_length_mismatch_raises(self):
        d = build_tdigest(np.arange(10.0))
        with pytest.raises(ValueError, match="does not match|arrays but digests"):
            merge_tdigests_kway([d, d], locations=[np.empty(len(d), dtype=np.uint64)])


class TestBuildTDigestPairwise:
    """The pairwise-fold reducer variant (issue #279) — identical build output."""

    def test_output_identical_to_standard(self):
        rng = np.random.default_rng(21)
        vals = rng.standard_normal(3000)
        for delta in (8, 128, 512):
            np.testing.assert_array_equal(
                build_tdigest_pairwise(vals, delta=delta), build_tdigest(vals, delta=delta)
            )

    def test_located_output_identical_to_standard(self):
        vals = np.linspace(0.0, 1.0, 200)
        locs = _point_words(200, seed=3)
        d_p, l_p = build_tdigest_pairwise(vals, delta=32, locations=locs)
        d_s, l_s = build_tdigest(vals, delta=32, locations=locs)
        np.testing.assert_array_equal(d_p, d_s)
        np.testing.assert_array_equal(l_p, l_s)


class TestSingletonPreservation:
    """Issue #424: loss-free below δ, pinned at the exact boundary n = δ.

    The δ = 8,192 raise's whole justification is "no original observation is
    merged away while a cell's count stays ≤ δ" — these tests make that
    load-bearing for the build AND both merge paths, so a future ``_compress``
    change that eagerly re-compresses below saturation fails here.
    """

    def _assert_all_singletons(self, digest, source_values):
        assert digest.shape == (len(source_values), 2)
        np.testing.assert_array_equal(digest[:, 1], np.ones(len(source_values), dtype=np.float32))
        np.testing.assert_array_equal(digest[:, 0], np.sort(source_values).astype(np.float32))

    def test_build_is_loss_free_at_n_equals_delta(self):
        rng = np.random.default_rng(424)
        values = rng.normal(0.0, 100.0, 512)
        self._assert_all_singletons(build_tdigest(values, delta=512), values)

    def test_pairwise_merge_keeps_every_singleton_at_combined_delta(self):
        # Two loss-free digests over overlapping ranges, combined n == δ: the
        # merge re-runs the same greedy rule over 512 unit sub-centroids, so
        # no eager re-compression below saturation may exist.
        rng = np.random.default_rng(4242)
        a_vals = rng.normal(0.0, 100.0, 300)
        b_vals = rng.normal(0.0, 100.0, 212)
        a = build_tdigest(a_vals, delta=512)
        b = build_tdigest(b_vals, delta=512)
        merged = merge_tdigests(a, b, delta=512)
        self._assert_all_singletons(merged, np.concatenate([a_vals, b_vals]))

    def test_kway_merge_keeps_every_singleton_at_combined_delta(self):
        rng = np.random.default_rng(42424)
        parts = [rng.normal(0.0, 100.0, 128) for _ in range(4)]
        digests = [build_tdigest(p, delta=512) for p in parts]
        merged = merge_tdigests_kway(digests, delta=512)
        self._assert_all_singletons(merged, np.concatenate(parts))

    def test_default_delta_unchanged(self):
        # Raising the default would silently change output values under an
        # unchanged semantic hash for any config omitting ``delta`` — the
        # packaged configs are explicit instead (issue #424, plan Q2 ruling).
        from zagg.stats.tdigest import _DEFAULT_DELTA

        assert _DEFAULT_DELTA == 512


class TestTemporalChannel:
    """The ``temporal`` companion channel (spec §8.3, issue #410)."""

    def test_digest_identical_with_and_without_temporal(self):
        rng = np.random.default_rng(410)
        values = rng.standard_normal(3000)
        digest, _ = build_tdigest(values, delta=128, temporal=_toc_words(3000))
        assert np.array_equal(digest, build_tdigest(values, delta=128))

    def test_singleton_centroids_round_trip_exact_timestamps(self):
        # Loss-free regime (n <= delta): every centroid holds one observation,
        # so §8.3 requires its exact nanosecond timestamp word back, never a
        # range widened around it.
        import mortie

        values = np.arange(50, dtype=np.float64)
        words = _toc_words(50)
        digest, out = build_tdigest(values, delta=512, temporal=words)
        assert np.all(digest[:, 1] == 1.0)
        assert out.dtype == np.uint64
        np.testing.assert_array_equal(out, words)
        assert not mortie.toc_is_range(out).any()

    def test_merged_centroid_envelopes_contain_their_members(self):
        # The §8.2/§8.3 conservative-containment claim, asserted rather than
        # assumed: values rise with time here, so centroid i covers a
        # contiguous run of the value-sorted observations.
        import mortie

        n = 600
        values = np.arange(n, dtype=np.float64)
        words = _toc_words(n)
        digest, out = build_tdigest(values, delta=32, temporal=words)
        assert len(out) == len(digest) < n
        assert mortie.toc_is_range(out).any(), "a compressed digest must produce ranges"
        bounds = np.concatenate([[0], np.cumsum(digest[:, 1]).astype(np.int64)])
        for i, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
            members = mortie.toc2time(words[lo:hi])[0]
            start, end = (int(b[0]) for b in mortie.toc2time(out[i : i + 1]))
            assert start <= int(members.min())
            # A range's end is EXCLUSIVE (§8.1); a timestamp's two bounds are
            # both its exact instant, so its sole member sits on the boundary.
            if bool(mortie.toc_is_range(out[i : i + 1])[0]):
                assert int(members.max()) < end
            else:
                assert int(members.max()) == end == start

    def test_reserved_zero_word_refused(self):
        # 0 is §8.2's unobserved marker; no encoder produces it, so its
        # presence is a fill value that leaked into the ingest words.
        words = _toc_words(4)
        words[2] = 0
        with pytest.raises(ValueError, match="reserved 0 word"):
            build_tdigest(np.arange(4.0), temporal=words)

    def test_non_uint64_temporal_raises(self):
        with pytest.raises(ValueError, match="is not uint64"):
            build_tdigest(np.array([1.0, 2.0]), temporal=np.array([1.5, 2.5]))

    def test_mismatched_lengths_raise(self):
        with pytest.raises(ValueError, match="temporal shape"):
            build_tdigest(np.array([1.0, 2.0]), temporal=_toc_words(3))

    def test_nan_values_drop_their_words(self):
        values = np.array([1.0, np.nan, 2.0, np.nan, 3.0])
        words = _toc_words(5)
        digest, out = build_tdigest(values, delta=512, temporal=words)
        assert len(digest) == 3
        np.testing.assert_array_equal(out, words[[0, 2, 4]])

    def test_empty_returns_empty_triple_with_both_channels(self):
        digest, locs, times = build_tdigest(
            np.array([]),
            locations=np.array([], dtype=np.uint64),
            temporal=np.array([], dtype=np.uint64),
        )
        assert digest.shape == (0, 2)
        assert locs.shape == (0,) and locs.dtype == np.uint64
        assert times.shape == (0,) and times.dtype == np.uint64

    def test_both_channels_return_in_fixed_order(self):
        # (digest, locations, temporal) — the documented tuple order, and each
        # channel is byte-identical to what declaring it alone produces.
        values = np.linspace(0.0, 1.0, 200)
        locs = _point_words(200, seed=410)
        words = _toc_words(200)
        digest, out_locs, out_times = build_tdigest(
            values, delta=32, locations=locs, temporal=words
        )
        _, locs_alone = build_tdigest(values, delta=32, locations=locs)
        _, times_alone = build_tdigest(values, delta=32, temporal=words)
        np.testing.assert_array_equal(digest, build_tdigest(values, delta=32))
        np.testing.assert_array_equal(out_locs, locs_alone)
        np.testing.assert_array_equal(out_times, times_alone)

    def test_where_reducer_masks_the_channel(self):
        from zagg.stats.tdigest import build_tdigest_where

        values = np.arange(10, dtype=np.float64)
        words = _toc_words(10)
        keep = np.array([True, False] * 5)
        digest, out = build_tdigest_where(values, 512, where=keep, temporal=words)
        assert len(digest) == 5
        np.testing.assert_array_equal(out, words[keep])

    def test_where_reducer_refuses_a_mismatched_channel(self):
        # ``build_tdigest_where`` carries its own copy of the shape check,
        # because it masks the channel before delegating.
        from zagg.stats.tdigest import build_tdigest_where

        values = np.arange(10.0)
        keep = np.array([True, False] * 5)
        with pytest.raises(ValueError, match="temporal shape .* does not match values shape"):
            build_tdigest_where(values, 512, where=keep, temporal=_toc_words(9))

    def test_pairwise_build_output_identical(self):
        values = np.linspace(0.0, 1.0, 200)
        words = _toc_words(200)
        d_p, t_p = build_tdigest_pairwise(values, delta=32, temporal=words)
        d_s, t_s = build_tdigest(values, delta=32, temporal=words)
        np.testing.assert_array_equal(d_p, d_s)
        np.testing.assert_array_equal(t_p, t_s)


class TestTemporalMergeLaws:
    """The channel's fold laws: pairwise, k-way, and the §8.3 order law."""

    def _parts(self, seed=410, k=4, n=120):
        rng = np.random.default_rng(seed)
        digests, times = [], []
        for i in range(k):
            vals = rng.normal(float(i), 3.0, n)
            d, t = build_tdigest(
                vals, delta=32, temporal=_toc_words(n, base="2020-01-0%d" % (i + 1))
            )
            digests.append(d)
            times.append(t)
        return digests, times

    def test_pairwise_merge_requires_both_sides(self):
        d = build_tdigest(np.arange(10.0), delta=8)
        with pytest.raises(ValueError, match="pass both temporal1 and temporal2"):
            merge_tdigests(d, d, 8, temporal1=_toc_words(len(d)))

    def test_pairwise_merge_passes_an_empty_side_through(self):
        vals = np.arange(10.0)
        words = _toc_words(10)
        d, t = build_tdigest(vals, delta=512, temporal=words)
        empty = np.empty((0, 2), dtype=np.float32)
        merged, out = merge_tdigests(
            d, empty, 512, temporal1=t, temporal2=np.empty(0, dtype=np.uint64)
        )
        np.testing.assert_array_equal(merged, d)
        np.testing.assert_array_equal(out, t)
        assert out is not t, "the channel must not alias the caller's array"

    def test_pairwise_merge_of_two_sides_envelopes_contain_their_members(self):
        # The pairwise law is a distinct code path from the k-way one (plain
        # argsort on means, no channel tie key) and it is what the two shipped
        # spill/streaming call sites use, so its temporal fold needs its own
        # containment assertion, not just the k-way order laws.
        import mortie

        def covers(envelope, member):
            e_start, e_end = (int(b[0]) for b in mortie.toc2time(np.array([envelope], "uint64")))
            m_start, m_end = (int(b[0]) for b in mortie.toc2time(np.array([member], "uint64")))
            return e_start <= m_start and m_end <= e_end

        (d1, d2), (t1, t2) = self._parts(k=2, n=150)
        merged, out = merge_tdigests(d1, d2, 8, temporal1=t1, temporal2=t2)
        assert out.dtype == np.uint64 and len(out) == len(merged) < len(t1) + len(t2)
        assert mortie.toc_is_range(out).any(), "a re-compressed merge must produce ranges"
        for member in np.concatenate([t1, t2]):
            assert any(covers(envelope, member) for envelope in out)

    def test_pairwise_merge_both_channels_independent_of_each_other(self):
        # Pins the (reducer, left, right) / joined-words pairing on the pairwise
        # arm: a channel swap would leave each vector reduced by the other
        # channel's law, so neither would match its declared-alone output.
        rng = np.random.default_rng(4103)
        digests, locs, times = [], [], []
        for i in range(2):
            d, lo, t = build_tdigest(
                rng.normal(float(i), 2.0, 80),
                delta=16,
                locations=_point_words(80, seed=100 + i),
                temporal=_toc_words(80, base="2021-03-0%d" % (i + 1)),
            )
            digests.append(d)
            locs.append(lo)
            times.append(t)
        d_both, l_both, t_both = merge_tdigests(
            *digests,
            16,
            locations1=locs[0],
            locations2=locs[1],
            temporal1=times[0],
            temporal2=times[1],
        )
        _, l_only = merge_tdigests(*digests, 16, locations1=locs[0], locations2=locs[1])
        _, t_only = merge_tdigests(*digests, 16, temporal1=times[0], temporal2=times[1])
        np.testing.assert_array_equal(d_both, merge_tdigests(*digests, 16))
        np.testing.assert_array_equal(l_both, l_only)
        np.testing.assert_array_equal(t_both, t_only)

    def test_pairwise_merge_passes_the_right_side_through(self):
        # The other emptiness arm: ``d1`` empty keeps ``temporal2``, and the
        # selector must not hand back the left channel instead.
        vals = np.arange(10.0)
        words = _toc_words(10)
        d, t = build_tdigest(vals, delta=512, temporal=words)
        empty = np.empty((0, 2), dtype=np.float32)
        merged, out = merge_tdigests(
            empty, d, 512, temporal1=np.empty(0, dtype=np.uint64), temporal2=t
        )
        np.testing.assert_array_equal(merged, d)
        np.testing.assert_array_equal(out, t)
        assert out is not t, "the channel must not alias the caller's array"

    def test_kway_fold_is_permutation_independent(self):
        digests, times = self._parts()
        base = merge_tdigests_kway(digests, delta=32, temporal=times)
        for perm in ([3, 1, 0, 2], [2, 3, 1, 0], [1, 0, 3, 2]):
            d, t = merge_tdigests_kway(
                [digests[i] for i in perm], delta=32, temporal=[times[i] for i in perm]
            )
            np.testing.assert_array_equal(d, base[0])
            np.testing.assert_array_equal(t, base[1])

    def test_cell_envelope_is_fold_tree_independent(self):
        # The §8.2 law the located channel and the digest payload cannot claim:
        # the join is a semilattice, so a CELL-level reduction of the same
        # member words is bit-identical however the tree is shaped — which is
        # what licenses an overview folded from leaves and one cascaded from
        # finer overviews to agree byte for byte (spec §8.4's reduction).
        from zagg.stats.toc import cell_envelope

        _, times = self._parts()
        whole = int(cell_envelope(np.concatenate(times)))
        per_part = int(cell_envelope(np.array([int(cell_envelope(t)) for t in times], "uint64")))
        pairs = [int(cell_envelope(np.concatenate(times[i : i + 2]))) for i in (0, 2)]
        assert whole == per_part == int(cell_envelope(np.asarray(pairs, dtype="uint64")))

    def test_per_cell_envelope_is_the_envelope_of_the_per_centroid_words(self):
        # §8.3's closing clause / §8.4's licensed reduction: an overview's
        # per-cell word IS the envelope of the per-centroid words beneath it,
        # and equally of the raw observations they summarize.
        from zagg.stats.toc import cell_envelope

        words = _toc_words(400)
        _, per_centroid = build_tdigest(np.arange(400.0), delta=16, temporal=words)
        assert int(cell_envelope(per_centroid)) == int(cell_envelope(words))

    def test_kway_both_channels_independent_of_each_other(self):
        rng = np.random.default_rng(4102)
        digests, locs, times = [], [], []
        for i in range(3):
            vals = rng.normal(float(i), 2.0, 90)
            d, lo, t = build_tdigest(
                vals, delta=16, locations=_point_words(90, seed=i), temporal=_toc_words(90)
            )
            digests.append(d)
            locs.append(lo)
            times.append(t)
        d_both, l_both, t_both = merge_tdigests_kway(
            digests, delta=16, locations=locs, temporal=times
        )
        _, l_only = merge_tdigests_kway(digests, delta=16, locations=locs)
        _, t_only = merge_tdigests_kway(digests, delta=16, temporal=times)
        np.testing.assert_array_equal(d_both, merge_tdigests_kway(digests, delta=16))
        np.testing.assert_array_equal(l_both, l_only)
        np.testing.assert_array_equal(t_both, t_only)

    def test_kway_arity_mismatch_raises(self):
        digests, times = self._parts(k=3, n=40)
        with pytest.raises(ValueError, match="temporal has 2 arrays"):
            merge_tdigests_kway(digests, delta=16, temporal=times[:2])

    def test_pass_through_arms_still_refuse_the_reserved_zero(self):
        # §8.2 forbids storing the reserved word for an observed cell, and the
        # arms that hand a channel back unreduced are exactly the ones a
        # reducer-side check misses: a single-block cell (the common spill
        # shape) and a single-contributor overview fold.
        d, t = build_tdigest(np.arange(10.0), delta=512, temporal=_toc_words(10))
        leaked = t.copy()
        leaked[3] = 0
        empty_d = np.empty((0, 2), dtype=np.float32)
        empty_w = np.empty(0, dtype=np.uint64)
        with pytest.raises(ValueError, match="reserved 0 word"):
            merge_tdigests(d, empty_d, 512, temporal1=leaked, temporal2=empty_w)
        with pytest.raises(ValueError, match="reserved 0 word"):
            merge_tdigests(empty_d, d, 512, temporal1=empty_w, temporal2=leaked)
        with pytest.raises(ValueError, match="reserved 0 word"):
            merge_tdigests_kway([d, empty_d], delta=512, temporal=[leaked, empty_w])

    def test_reserved_zero_refusal_is_channel_agnostic(self):
        # The check sits in the shared word validator, so a leaked fill is
        # refused on the located channel's pass-through too — 0 is no more a
        # valid morton word than it is a toc word.
        d, locs = build_tdigest(np.arange(10.0), delta=512, locations=_point_words(10, seed=410))
        leaked = locs.copy()
        leaked[0] = 0
        empty_d = np.empty((0, 2), dtype=np.float32)
        empty_w = np.empty(0, dtype=np.uint64)
        with pytest.raises(ValueError, match="reserved 0 word"):
            merge_tdigests(d, empty_d, 512, locations1=leaked, locations2=empty_w)

    def test_kway_single_contributor_copies_the_channel(self):
        d, t = build_tdigest(np.arange(20.0), delta=512, temporal=_toc_words(20))
        out_d, out_t = merge_tdigests_kway(
            [d, np.empty((0, 2))], delta=512, temporal=[t, np.empty(0, dtype=np.uint64)]
        )
        np.testing.assert_array_equal(out_d, d)
        np.testing.assert_array_equal(out_t, t)
        assert out_t is not t


class TestBatchedCompanionFolds:
    """Cross-cell fold batching (issue #476): identical bytes, one FFI crossing."""

    @staticmethod
    def _cells(deltas=(4, 512, 32), seed=476):
        # Varied sizes and deltas so the batch spans singleton-only partitions
        # (n <= delta) AND compressed multi-member ones, on both channels.
        rng = np.random.default_rng(seed)
        cells = []
        for j, delta in enumerate(deltas):
            n = 30 + 40 * j
            cells.append(
                (
                    rng.standard_normal(n),
                    _point_words(n, seed=seed + j),
                    _toc_words(n),
                    delta,
                )
            )
        return cells

    def test_batched_matches_unbatched_byte_for_byte(self):
        from zagg.stats.tdigest import batched_companion_folds

        cells = self._cells()
        plain = [build_tdigest(v, delta=d, locations=lo, temporal=t) for v, lo, t, d in cells]
        with batched_companion_folds():
            batched = [build_tdigest(v, delta=d, locations=lo, temporal=t) for v, lo, t, d in cells]
        for (pd_, pl, pt), (bd, bl, bt) in zip(plain, batched, strict=True):
            np.testing.assert_array_equal(pd_, bd)
            np.testing.assert_array_equal(pl, bl)
            np.testing.assert_array_equal(pt, bt)

    def test_one_reduce_crossing_per_channel(self, monkeypatch):
        import mortie

        from zagg.stats.tdigest import batched_companion_folds

        counts = {"toc_reduce": 0, "validate_morton": 0}
        orig_reduce, orig_validate = mortie.toc_reduce, mortie.validate_morton
        monkeypatch.setattr(
            mortie,
            "toc_reduce",
            lambda *a, **k: (
                counts.__setitem__("toc_reduce", counts["toc_reduce"] + 1) or orig_reduce(*a, **k)
            ),
        )
        monkeypatch.setattr(
            mortie,
            "validate_morton",
            lambda *a: (
                counts.__setitem__("validate_morton", counts["validate_morton"] + 1)
                or orig_validate(*a)
            ),
        )
        with batched_companion_folds():
            for v, lo, t, d in self._cells():
                build_tdigest(v, delta=d, locations=lo, temporal=t)
            assert counts == {"toc_reduce": 0, "validate_morton": 0}, (
                "folds must defer until the context exits"
            )
        assert counts["toc_reduce"] == 1
        assert counts["validate_morton"] == 1

    def test_nested_context_is_a_passthrough(self):
        from zagg.stats.tdigest import batched_companion_folds

        v, lo, t, d = self._cells()[1]
        plain_d, plain_l, plain_t = build_tdigest(v, delta=d, locations=lo, temporal=t)
        with batched_companion_folds():
            with batched_companion_folds():
                bd, bl, bt = build_tdigest(v, delta=d, locations=lo, temporal=t)
        np.testing.assert_array_equal(plain_d, bd)
        np.testing.assert_array_equal(plain_l, bl)
        np.testing.assert_array_equal(plain_t, bt)

    def test_exception_skips_the_flush(self, monkeypatch):
        import mortie

        from zagg.stats.tdigest import batched_companion_folds

        calls = []
        orig = mortie.toc_reduce
        monkeypatch.setattr(mortie, "toc_reduce", lambda *a, **k: calls.append(1) or orig(*a, **k))
        with pytest.raises(RuntimeError, match="boom"):
            with batched_companion_folds():
                v, lo, t, d = self._cells()[0]
                build_tdigest(v, delta=d, temporal=t)
                raise RuntimeError("boom")
        assert calls == [], "a failed loop must not fold its abandoned placeholders"

    def test_single_entry_flush_folds_with_the_declared_n(self):
        # A lone deferral must fold with the ``n`` it declared, not with
        # ``len(words)``: a partition overrunning its words has to fail exactly
        # where the unbatched call does, not silently fold a truncated one.
        from zagg.stats.tdigest import _centroid_envelopes, batched_companion_folds

        words, starts = _toc_words(6), np.array([0, 3], dtype=np.int64)
        with pytest.raises(ValueError, match="exceeds word array length"):
            _centroid_envelopes(words, starts, 9)
        with pytest.raises(ValueError, match="exceeds word array length"):
            with batched_companion_folds():
                _centroid_envelopes(words, starts, 9)

    def test_a_partition_not_starting_at_zero_is_refused(self):
        # The concatenation rebases on each entry's rows, so a partition that
        # does not start at 0 would silently fold its first rows into the
        # previous entry's last centroid — neither fold notices.
        from zagg.stats.tdigest import _centroid_ancestors, batched_companion_folds

        locs, starts = _point_words(6, seed=3), np.array([1, 4], dtype=np.int64)
        with pytest.raises(ValueError, match="requires each centroid partition to start at 0"):
            with batched_companion_folds():
                _centroid_ancestors(locs, starts, 6)

    def test_the_placeholder_survives_the_aggregate_normalization(self):
        # ``calculate_cell_statistics`` normalizes every channel with
        # ``np.ascontiguousarray`` before collecting it, and the flush fills the
        # placeholder in place afterwards — so that call must be identity here.
        # Unfilled, the placeholder reads as the reserved 0 word (refusable),
        # never as uninitialized bytes.
        from zagg.stats.tdigest import batched_companion_folds

        v, lo, t, d = self._cells()[1]
        with batched_companion_folds():
            _, locs, times = build_tdigest(v, delta=d, locations=lo, temporal=t)
            for channel in (locs, times):
                assert np.ascontiguousarray(np.asarray(channel)) is channel
                assert not channel.any(), "a deferred placeholder must read as reserved 0"

    def test_row_cap_folds_mid_loop_byte_for_byte(self, monkeypatch):
        # The cap bounds the words the batch pins; the folds are segment-local,
        # so cutting the batch mid-loop must not move a byte — and the flush it
        # triggers has to run for real (batch deactivated), not re-defer.
        import mortie

        import zagg.stats.tdigest as td_mod
        from zagg.stats.tdigest import batched_companion_folds

        cells = self._cells()
        plain = [build_tdigest(v, delta=d, locations=lo, temporal=t) for v, lo, t, d in cells]
        crossings = []
        orig = mortie.toc_reduce
        monkeypatch.setattr(
            mortie, "toc_reduce", lambda *a, **k: crossings.append(1) or orig(*a, **k)
        )
        monkeypatch.setattr(td_mod, "_BATCH_ROW_CAP", 40)
        with batched_companion_folds():
            batched = [build_tdigest(v, delta=d, locations=lo, temporal=t) for v, lo, t, d in cells]
        assert len(crossings) > 1, "a tiny cap must fold more than once"
        for (pd_, pl, pt), (bd, bl, bt) in zip(plain, batched, strict=True):
            np.testing.assert_array_equal(pd_, bd)
            np.testing.assert_array_equal(pl, bl)
            np.testing.assert_array_equal(pt, bt)

    def test_batch_deactivates_after_exit(self):
        # The context always resets, so a later unbatched call folds for real.
        from zagg.stats.tdigest import batched_companion_folds

        with batched_companion_folds():
            pass
        v, lo, t, d = self._cells()[2]
        _, _, times = build_tdigest(v, delta=d, locations=lo, temporal=t)
        assert times.dtype == np.uint64 and len(times) > 0
