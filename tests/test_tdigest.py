"""Tests for the pure-numpy t-digest — issue #48, phase 4."""

import numpy as np
import pytest

from zagg.stats.tdigest import build_tdigest, merge_tdigests, quantile_from_tdigest


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

    def test_loss_free_transition_at_delta_boundary(self):
        """n == δ is the exact loss-free/compress boundary the fix pins."""
        delta = 256
        rng = np.random.default_rng(99)
        at = build_tdigest(rng.standard_normal(delta), delta=delta)
        over = build_tdigest(rng.standard_normal(delta + 1), delta=delta)
        assert at.shape[0] == delta, f"n==δ should be loss-free, got k={at.shape[0]}"
        assert over.shape[0] < delta + 1, f"n>δ must compress, got k={over.shape[0]}"

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
