"""Tests for the pure-numpy t-digest — issue #48, phase 4."""

import numpy as np
import pytest

from zagg.stats.tdigest import (
    build_tdigest,
    cdf_from_tdigest,
    merge_tdigests,
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


def _point_words(n, seed, lat0=45.0, lon0=45.0, spread=1e-4):
    """Order-29 point-kind morton words for ``n`` points near one location.

    Jitter is tiny so all words share a HEALPix base cell (the same guarantee
    one grid cell's observations carry), as ``common_ancestor`` requires.
    """
    from mortie import MortonIndexArray

    rng = np.random.default_rng(seed)
    lats = lat0 + rng.uniform(-spread, spread, n)
    lons = lon0 + rng.uniform(-spread, spread, n)
    arr = MortonIndexArray.from_latlon(lats, lons, points=True)
    return np.asarray(arr._data, dtype=np.uint64)


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
        for anc in out_locs:
            assert any(_contains(anc, m) for m in locs)

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

    def test_mismatched_lengths_raise(self):
        with pytest.raises(ValueError, match="locations shape"):
            build_tdigest(np.array([1.0, 2.0]), locations=_point_words(3, seed=5))

    def test_without_locations_returns_bare_array(self):
        out = build_tdigest(np.array([1.0, 2.0]))
        assert isinstance(out, np.ndarray)


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
            assert any(_contains(anc, member) for anc in locs)

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
