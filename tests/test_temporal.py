"""Tests for the temporal aggregation module."""

import numpy as np
import pytest

from magg.temporal import (
    SPATIAL_FUNCTIONS,
    TEMPORAL_REDUCERS,
    FirstLandfallCapture,
    MaxAccumulator,
    MinAccumulator,
    SumAccumulator,
    WeightedMeanAccumulator,
    specs_from_config,
)


# ---------------------------------------------------------------------------
# Accumulator tests
# ---------------------------------------------------------------------------

class TestMaxAccumulator:
    def test_basic(self):
        acc = MaxAccumulator()
        for v in [1.0, 3.0, 2.0]:
            acc.update(v)
        assert acc.finalize() == 3.0

    def test_nan_ignored(self):
        acc = MaxAccumulator()
        acc.update(5.0)
        acc.update(np.nan)
        acc.update(2.0)
        assert acc.finalize() == 5.0

    def test_none_ignored(self):
        acc = MaxAccumulator()
        acc.update(None)
        acc.update(4.0)
        assert acc.finalize() == 4.0

    def test_empty(self):
        acc = MaxAccumulator()
        assert np.isnan(acc.finalize())


class TestMinAccumulator:
    def test_basic(self):
        acc = MinAccumulator()
        for v in [3.0, 1.0, 2.0]:
            acc.update(v)
        assert acc.finalize() == 1.0

    def test_empty(self):
        acc = MinAccumulator()
        assert np.isnan(acc.finalize())


class TestSumAccumulator:
    def test_basic(self):
        acc = SumAccumulator()
        for v in [1.0, 2.0, 3.0]:
            acc.update(v)
        assert acc.finalize() == pytest.approx(6.0)

    def test_nan_ignored(self):
        acc = SumAccumulator()
        acc.update(1.0)
        acc.update(np.nan)
        acc.update(2.0)
        assert acc.finalize() == pytest.approx(3.0)

    def test_empty(self):
        acc = SumAccumulator()
        assert np.isnan(acc.finalize())


class TestWeightedMeanAccumulator:
    def test_basic(self):
        acc = WeightedMeanAccumulator()
        # weighted_sum=10, weight_sum=2 -> mean=5
        acc.update((10.0, 2.0))
        # weighted_sum=30, weight_sum=3 -> mean=6
        acc.update((30.0, 3.0))
        # total: 40/5 = 8
        assert acc.finalize() == pytest.approx(8.0)

    def test_none_ignored(self):
        acc = WeightedMeanAccumulator()
        acc.update(None)
        acc.update((10.0, 2.0))
        assert acc.finalize() == pytest.approx(5.0)

    def test_empty(self):
        acc = WeightedMeanAccumulator()
        assert np.isnan(acc.finalize())


class TestFirstLandfallCapture:
    def test_captures_first(self):
        acc = FirstLandfallCapture()
        acc.update(None)
        acc.update(42.0)
        acc.update(99.0)
        assert acc.finalize() == 42.0

    def test_tuple_value(self):
        acc = FirstLandfallCapture()
        acc.update((10.0, 2.0))
        assert acc.finalize() == pytest.approx(5.0)

    def test_empty(self):
        acc = FirstLandfallCapture()
        assert np.isnan(acc.finalize())


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------

class TestRegistries:
    def test_temporal_reducers_keys(self):
        assert set(TEMPORAL_REDUCERS.keys()) == {
            "max", "min", "sum", "weighted_mean", "first_landfall",
        }

    def test_spatial_functions_keys(self):
        assert set(SPATIAL_FUNCTIONS.keys()) == {
            "max", "min", "weighted_sum", "weighted_mean",
            "max_gradient", "min_over_levels",
        }

    def test_all_reducers_have_protocol(self):
        for name, cls in TEMPORAL_REDUCERS.items():
            acc = cls()
            assert hasattr(acc, "update"), f"{name} missing update()"
            assert hasattr(acc, "finalize"), f"{name} missing finalize()"

    def test_all_spatial_funcs_are_callable(self):
        for name, func in SPATIAL_FUNCTIONS.items():
            assert callable(func), f"{name} not callable"


# ---------------------------------------------------------------------------
# Config bridge
# ---------------------------------------------------------------------------

class TestSpecsFromConfig:
    def test_basic(self):
        from magg.config import default_config
        cfg = default_config("merra2_storm")
        specs = specs_from_config(cfg)
        assert len(specs) > 0

        names = {s["output_name"] for s in specs}
        assert "max_T2m_ais" in names
        assert "cumulative_rainfall_ais" in names

    def test_fields_present(self):
        from magg.config import default_config
        cfg = default_config("merra2_storm")
        specs = specs_from_config(cfg)
        required_keys = {
            "output_name", "variable", "collection",
            "spatial_func", "temporal_reducer",
            "mask", "is_anomaly", "negate", "precip",
        }
        for spec in specs:
            assert set(spec.keys()) == required_keys, f"Bad keys for {spec['output_name']}"

    def test_anomaly_flag(self):
        from magg.config import default_config
        cfg = default_config("merra2_storm")
        specs = specs_from_config(cfg)
        anomaly_specs = [s for s in specs if s["is_anomaly"]]
        assert len(anomaly_specs) >= 2  # T2M and IWV anomalies

    def test_precip_flag(self):
        from magg.config import default_config
        cfg = default_config("merra2_storm")
        specs = specs_from_config(cfg)
        precip_specs = [s for s in specs if s["precip"]]
        assert len(precip_specs) == 2  # rainfall + snowfall


# ---------------------------------------------------------------------------
# Spatial functions with synthetic xarray data
# ---------------------------------------------------------------------------

class TestSpatialFunctions:
    @pytest.fixture
    def grid_3x3(self):
        """Create a 3x3 synthetic grid for testing spatial functions."""
        xr = pytest.importorskip("xarray")
        lat = np.array([-70.0, -69.5, -69.0])
        lon = np.array([0.0, 0.5, 1.0])
        values = np.array([
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
        ])
        var = xr.DataArray(values, dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        mask = xr.DataArray(
            np.array([[1, 1, 0], [1, 1, 0], [0, 0, 0]], dtype=float),
            dims=["lat", "lon"], coords={"lat": lat, "lon": lon},
        )
        areas = xr.DataArray(
            np.ones((3, 3)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon},
        )
        return var, mask, areas

    def test_spatial_max(self, grid_3x3):
        from magg.temporal import spatial_max
        var, mask, areas = grid_3x3
        assert spatial_max(var, mask, areas) == 5.0

    def test_spatial_min(self, grid_3x3):
        from magg.temporal import spatial_min
        var, mask, areas = grid_3x3
        assert spatial_min(var, mask, areas) == 1.0

    def test_spatial_weighted_sum(self, grid_3x3):
        from magg.temporal import spatial_weighted_sum
        var, mask, areas = grid_3x3
        # masked values: 1+2+4+5 = 12, areas=1
        assert spatial_weighted_sum(var, mask, areas) == pytest.approx(12.0)

    def test_spatial_weighted_mean_parts(self, grid_3x3):
        from magg.temporal import spatial_weighted_mean_parts
        var, mask, areas = grid_3x3
        ws, wt = spatial_weighted_mean_parts(var, mask, areas)
        assert ws == pytest.approx(12.0)  # sum of masked values * areas
        assert wt == pytest.approx(4.0)   # sum of mask * areas

    def test_spatial_max_empty_mask(self, grid_3x3):
        from magg.temporal import spatial_max
        xr = pytest.importorskip("xarray")
        var, _, areas = grid_3x3
        empty = xr.zeros_like(var)
        assert np.isnan(spatial_max(var, empty, areas))


# ---------------------------------------------------------------------------
# Orchestrate module (pure functions only)
# ---------------------------------------------------------------------------

class TestOrchestrate:
    def test_estimate_cost_x86(self):
        from magg.orchestrate import estimate_cost
        result = estimate_cost([1000, 2000, 3000], memory_mb=2048, architecture="x86_64")
        assert result["gb_seconds"] > 0
        assert result["total_cost"] > 0
        assert result["total_cost"] == pytest.approx(
            result["compute_cost"] + result["request_cost"]
        )

    def test_estimate_cost_arm_cheaper(self):
        from magg.orchestrate import estimate_cost
        x86 = estimate_cost([5000], memory_mb=2048, architecture="x86_64")
        arm = estimate_cost([5000], memory_mb=2048, architecture="arm64")
        assert arm["compute_cost"] < x86["compute_cost"]

    def test_parse_billed_duration(self):
        import base64
        from magg.orchestrate import parse_billed_duration
        log = "REPORT RequestId: abc Duration: 1234.56 ms Billed Duration: 1235 ms Memory Size: 2048 MB Max Memory Used: 512 MB"
        encoded = base64.b64encode(log.encode()).decode()
        assert parse_billed_duration(encoded) == 1235

    def test_parse_billed_duration_empty(self):
        from magg.orchestrate import parse_billed_duration
        assert parse_billed_duration("") is None
        assert parse_billed_duration(None) is None

    def test_parse_max_memory(self):
        import base64
        from magg.orchestrate import parse_max_memory
        log = "REPORT RequestId: abc Duration: 100 ms Billed Duration: 100 ms Memory Size: 2048 MB Max Memory Used: 512 MB"
        encoded = base64.b64encode(log.encode()).decode()
        assert parse_max_memory(encoded) == 512
