"""Tests for the temporal aggregation primitives (issue #12 Phase 4)."""

import numpy as np
import pytest

from zagg import registry
from zagg.temporal import (
    FirstLandfallCapture,
    MaxAccumulator,
    MinAccumulator,
    SumAccumulator,
    WeightedMeanAccumulator,
    process_event,
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


class TestMeanOfRatiosAccumulator:
    def test_equal_weight_per_timestep(self):
        from zagg.temporal import MeanOfRatiosAccumulator

        acc = MeanOfRatiosAccumulator()
        # ratios 5 and 6 -> 5.5; WeightedMeanAccumulator pools to 40/5 = 8
        acc.update((10.0, 2.0))
        acc.update((30.0, 5.0))
        assert acc.finalize() == pytest.approx(5.5)

    def test_zero_weight_timestep_skipped(self):
        from zagg.temporal import MeanOfRatiosAccumulator

        acc = MeanOfRatiosAccumulator()
        acc.update((10.0, 2.0))
        acc.update((0.0, 0.0))  # empty footprint: no ratio, no count
        assert acc.finalize() == pytest.approx(5.0)

    def test_none_and_nan_ignored(self):
        from zagg.temporal import MeanOfRatiosAccumulator

        acc = MeanOfRatiosAccumulator()
        acc.update(None)
        acc.update((np.nan, 2.0))
        acc.update((10.0, 2.0))
        assert acc.finalize() == pytest.approx(5.0)

    def test_empty(self):
        from zagg.temporal import MeanOfRatiosAccumulator

        acc = MeanOfRatiosAccumulator()
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
# Registry seeding — built-ins land in the canonical zagg.registry (#73)
# ---------------------------------------------------------------------------


class TestRegistrySeeding:
    def test_reducers_registered(self):
        # The registry is extensible (plugins add more); built-ins are a subset.
        assert {
            "max",
            "min",
            "sum",
            "weighted_mean",
            "mean_of_ratios",
            "first_landfall",
        } <= set(registry.list_reducers())

    def test_spatial_funcs_registered(self):
        assert {
            "max",
            "min",
            "weighted_sum",
            "weighted_mean",
            "max_gradient",
            "min_over_levels",
        } <= set(registry.list_spatial_funcs())

    def test_mask_providers_registered(self):
        assert {"full", "ais", "ocean"} <= set(registry.list_mask_providers())

    def test_field_transforms_registered(self):
        assert "monthly_anomaly" in set(registry.list_field_transforms())

    def test_reducers_satisfy_protocol(self):
        for name in ("max", "min", "sum", "weighted_mean", "mean_of_ratios", "first_landfall"):
            acc = registry.get_reducer(name)()
            assert hasattr(acc, "update"), f"{name} missing update()"
            assert hasattr(acc, "finalize"), f"{name} missing finalize()"

    def test_spatial_funcs_callable(self):
        for name in registry.list_spatial_funcs():
            assert callable(registry.get_spatial_func(name)), f"{name} not callable"


# ---------------------------------------------------------------------------
# Config bridge
# ---------------------------------------------------------------------------


def _temporal_config():
    from zagg.config import load_config_from_dict

    return load_config_from_dict(
        {
            "pipeline": {"type": "temporal"},
            "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
            "aggregation": {
                "variables": {
                    "max_t2m_ais": {
                        "variable": "T2M",
                        "collection": "merra2",
                        "spatial_func": "max",
                        "temporal_reducer": "max",
                        "mask": "ais",
                    },
                    "anom_iwv_full": {
                        "variable": "TQV",
                        "collection": "merra2",
                        "spatial_func": "weighted_mean",
                        "temporal_reducer": "weighted_mean",
                        "mask": "full",
                        "anomaly": True,
                    },
                    "rainfall_ocean": {
                        "variable": "PRECTOT",
                        "collection": "merra2",
                        "spatial_func": "weighted_sum",
                        "temporal_reducer": "sum",
                        "mask": "ocean",
                        "trigger": "first_intersection",
                        "trigger_mask": "grounded_mask",
                        "params": {"lookahead_hours": 24},
                    },
                }
            },
            "output": {"format": "tabular", "store": "."},
        }
    )


class TestSpecsFromConfig:
    def test_one_spec_per_variable(self):
        specs = specs_from_config(_temporal_config())
        names = {s["output_name"] for s in specs}
        assert names == {"max_t2m_ais", "anom_iwv_full", "rainfall_ocean"}

    def test_keys_present(self):
        specs = specs_from_config(_temporal_config())
        required = {
            "output_name",
            "variable",
            "collection",
            "spatial_func",
            "temporal_reducer",
            "mask",
            "negate",
            "transform",
            "trigger",
            "trigger_mask",
            "params",
        }
        for spec in specs:
            assert set(spec) == required, f"bad keys for {spec['output_name']}"

    def test_flag_defaults_and_overrides(self):
        specs = {s["output_name"]: s for s in specs_from_config(_temporal_config())}
        # `anomaly: true` desugars to `transform: monthly_anomaly` (issue #12);
        # `is_anomaly` is no longer a spec key.
        assert specs["anom_iwv_full"]["transform"] == "monthly_anomaly"
        assert specs["max_t2m_ais"]["transform"] is None
        assert "is_anomaly" not in specs["anom_iwv_full"]
        # default mask is "ais" when omitted; here every spec sets it explicitly
        assert specs["rainfall_ocean"]["mask"] == "ocean"
        # trigger/trigger_mask carry their configured *values* (not just keys)
        assert specs["rainfall_ocean"]["trigger"] == "first_intersection"
        assert specs["rainfall_ocean"]["trigger_mask"] == "grounded_mask"
        assert specs["max_t2m_ais"]["trigger"] is None
        assert specs["max_t2m_ais"]["trigger_mask"] is None
        # params pass through verbatim for capabilities; default is {}
        assert specs["rainfall_ocean"]["params"] == {"lookahead_hours": 24}
        assert specs["max_t2m_ais"]["params"] == {}

    def test_anomaly_sugar_equals_explicit_transform(self):
        # (a) `anomaly: true` produces the same spec as `transform: monthly_anomaly`.
        from zagg.config import load_config_from_dict

        base = {
            "variable": "TQV",
            "collection": "merra2",
            "spatial_func": "weighted_mean",
            "temporal_reducer": "weighted_mean",
            "mask": "full",
        }
        config = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "sugar": {**base, "anomaly": True},
                        "explicit": {**base, "transform": "monthly_anomaly"},
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )
        specs = {s["output_name"]: s for s in specs_from_config(config)}
        assert specs["sugar"]["transform"] == specs["explicit"]["transform"] == "monthly_anomaly"

    def test_anomaly_and_transform_does_not_double_apply(self):
        # (b) a spec carrying BOTH `anomaly: true` and `transform: monthly_anomaly`
        # resolves to a single `transform` -> applied exactly once, no double-apply.
        from zagg.config import load_config_from_dict

        config = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "both": {
                            "variable": "TQV",
                            "collection": "merra2",
                            "spatial_func": "weighted_mean",
                            "temporal_reducer": "weighted_mean",
                            "mask": "full",
                            "anomaly": True,
                            "transform": "monthly_anomaly",
                        }
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )
        (spec,) = specs_from_config(config)
        assert spec["transform"] == "monthly_anomaly"
        assert "is_anomaly" not in spec

    def test_explicit_transform_wins_over_anomaly_sugar(self):
        # `anomaly: true` is only sugar for the *default* transform: an explicit
        # `transform` naming a different field-transform takes precedence (the
        # anomaly flag is dropped, not applied on top -- no double transform).
        from zagg.config import load_config_from_dict

        config = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "v": {
                            "variable": "TQV",
                            "collection": "merra2",
                            "spatial_func": "weighted_mean",
                            "temporal_reducer": "weighted_mean",
                            "mask": "full",
                            "anomaly": True,
                            "transform": "detrend",
                        }
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )
        (spec,) = specs_from_config(config)
        assert spec["transform"] == "detrend"


# ---------------------------------------------------------------------------
# Spatial functions with synthetic xarray data
# ---------------------------------------------------------------------------


class TestSpatialFunctions:
    @pytest.fixture
    def grid_3x3(self):
        xr = pytest.importorskip("xarray")
        lat = np.array([-70.0, -69.5, -69.0])
        lon = np.array([0.0, 0.5, 1.0])
        values = np.array(
            [
                [1.0, 2.0, 3.0],
                [4.0, 5.0, 6.0],
                [7.0, 8.0, 9.0],
            ]
        )
        var = xr.DataArray(values, dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        mask = xr.DataArray(
            np.array([[1, 1, 0], [1, 1, 0], [0, 0, 0]], dtype=float),
            dims=["lat", "lon"],
            coords={"lat": lat, "lon": lon},
        )
        areas = xr.DataArray(np.ones((3, 3)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        return var, mask, areas

    def test_spatial_max(self, grid_3x3):
        from zagg.temporal import spatial_max

        var, mask, areas = grid_3x3
        assert spatial_max(var, mask, areas) == 5.0

    def test_spatial_min(self, grid_3x3):
        from zagg.temporal import spatial_min

        var, mask, areas = grid_3x3
        assert spatial_min(var, mask, areas) == 1.0

    def test_spatial_weighted_sum(self, grid_3x3):
        from zagg.temporal import spatial_weighted_sum

        var, mask, areas = grid_3x3
        # masked values: 1+2+4+5 = 12, areas=1
        assert spatial_weighted_sum(var, mask, areas) == pytest.approx(12.0)

    def test_spatial_weighted_mean_parts(self, grid_3x3):
        from zagg.temporal import spatial_weighted_mean_parts

        var, mask, areas = grid_3x3
        ws, wt = spatial_weighted_mean_parts(var, mask, areas)
        assert ws == pytest.approx(12.0)  # sum of masked values * areas
        assert wt == pytest.approx(4.0)  # sum of mask * areas

    def test_spatial_max_empty_mask(self, grid_3x3):
        from zagg.temporal import spatial_max

        xr = pytest.importorskip("xarray")
        var, _, areas = grid_3x3
        empty = xr.zeros_like(var)
        assert np.isnan(spatial_max(var, empty, areas))


# ---------------------------------------------------------------------------
# Mask providers
# ---------------------------------------------------------------------------


class TestMaskProviders:
    @pytest.fixture
    def masks(self):
        xr = pytest.importorskip("xarray")
        lat = np.array([-70.0, -69.5])
        lon = np.array([0.0, 0.5])
        coords = {"lat": lat, "lon": lon}
        event = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords=coords)
        ais = xr.DataArray(
            np.array([[1, 0], [0, 1]], dtype=float), dims=["lat", "lon"], coords=coords
        )
        return event, {"ais_mask": ais}

    def test_full_passes_through(self, masks):
        event, static = masks
        out = registry.get_mask_provider("full")(event, static, {})
        assert float(out.sum()) == 4.0

    def test_ais_keeps_ice(self, masks):
        event, static = masks
        out = registry.get_mask_provider("ais")(event, static, {})
        # AIS mask has two True cells -> two retained
        assert float(out.sum()) == 2.0

    def test_ocean_is_complement(self, masks):
        event, static = masks
        out = registry.get_mask_provider("ocean")(event, static, {})
        assert float(out.sum()) == 2.0


# ---------------------------------------------------------------------------
# Event triggers
# ---------------------------------------------------------------------------


def _trigger_inputs():
    """3-timestep event that first touches the single ice cell at t1."""
    xr = pytest.importorskip("xarray")
    lat = np.array([-70.0, -69.5])
    lon = np.array([0.0, 0.5])
    time = np.array(["2020-01-01T00", "2020-01-01T03", "2020-01-01T06"], dtype="datetime64[ns]")
    coords = {"time": time, "lat": lat, "lon": lon}
    footprints = np.array(
        [
            [[0, 1], [1, 1]],  # t0: ocean only
            [[1, 1], [0, 0]],  # t1: first landfall (covers the ice cell)
            [[1, 0], [0, 0]],  # t2: still on ice
        ],
        dtype=float,
    )
    event_mask = xr.DataArray(footprints, dims=["time", "lat", "lon"], coords=coords)
    ais = xr.DataArray(
        np.array([[1, 0], [0, 0]], dtype=float),
        dims=["lat", "lon"],
        coords={"lat": lat, "lon": lon},
    )
    return event_mask, {"ais_mask": ais}, time


class TestFirstIntersectionTrigger:
    def test_returns_first_overlap_timestep(self):
        from zagg.temporal import first_intersection

        event_mask, static, time = _trigger_inputs()
        assert first_intersection(event_mask, static, {}) == time[1]

    def test_no_overlap_returns_empty(self):
        from zagg.temporal import first_intersection

        event_mask, static, _ = _trigger_inputs()
        never = event_mask.where(event_mask["lat"] != -70.0, 0)  # clear the ice row
        out = first_intersection(never, static, {})
        assert np.atleast_1d(out).size == 0

    def test_trigger_mask_key_selects_static_field(self):
        from zagg.temporal import first_intersection

        event_mask, static, time = _trigger_inputs()
        # A shelf mask on the other row: the t0 footprint already covers it.
        shelf = static["ais_mask"].copy()
        shelf.values[:] = [[0, 0], [1, 0]]
        out = first_intersection(event_mask, {"shelf_mask": shelf}, {"trigger_mask": "shelf_mask"})
        assert out == time[0]

    def test_missing_static_field_raises(self):
        # A typo'd trigger_mask must fail with the name and the available
        # fields, not a bare KeyError from deep inside a worker.
        from zagg.temporal import first_intersection

        event_mask, static, _ = _trigger_inputs()
        with pytest.raises(ValueError, match="shelf_mask.*ais_mask"):
            first_intersection(event_mask, static, {"trigger_mask": "shelf_mask"})

    def test_registered_with_landfall_alias(self):
        from zagg.temporal import first_intersection

        assert registry.get_event_trigger("first_intersection") is first_intersection
        assert registry.get_event_trigger("first_landfall") is first_intersection


# ---------------------------------------------------------------------------
# process_event end-to-end on synthetic data
# ---------------------------------------------------------------------------


def _event_inputs():
    xr = pytest.importorskip("xarray")
    lat = np.array([-70.0, -69.5])
    lon = np.array([0.0, 0.5])
    time = np.array(["2020-01-01T00", "2020-01-01T03", "2020-01-01T06"], dtype="datetime64[ns]")
    coords = {"time": time, "lat": lat, "lon": lon}
    event_mask = xr.DataArray(np.ones((3, 2, 2)), dims=["time", "lat", "lon"], coords=coords)
    # T rises each timestep; cell-level max over the footprint then max-over-time.
    temp = xr.DataArray(
        np.stack(
            [
                np.full((2, 2), 1.0),
                np.full((2, 2), 5.0),
                np.full((2, 2), 3.0),
            ]
        ),
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    collections = {"merra2": xr.Dataset({"T2M": temp})}
    areas = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
    static = {"cell_areas": areas}
    specs = [
        {
            "output_name": "max_t2m",
            "variable": "T2M",
            "collection": "merra2",
            "spatial_func": "max",
            "temporal_reducer": "max",
            "mask": "full",
        }
    ]
    return event_mask, collections, specs, static


class TestProcessEvent:
    def test_max_over_time(self):
        event_mask, collections, specs, static = _event_inputs()
        results, meta = process_event("storm1", event_mask, collections, specs, static)
        assert results["max_t2m"] == pytest.approx(5.0)
        assert meta["event_key"] == "storm1"
        assert meta["timesteps_processed"] == 3
        assert meta["n_specs"] == 1
        assert meta["collections"] == ["merra2"]

    def test_batching_is_invariant(self):
        # Streaming in batches of 1 must yield the same reduction as one batch.
        event_mask, collections, specs, static = _event_inputs()
        full, _ = process_event("storm1", event_mask, collections, specs, static)
        batched, meta = process_event(
            "storm1", event_mask, collections, specs, static, max_resident_timesteps=1
        )
        assert batched["max_t2m"] == pytest.approx(full["max_t2m"])
        assert meta["timesteps_processed"] == 3

    def test_negate_flips_extremum(self):
        event_mask, collections, specs, static = _event_inputs()
        specs[0]["negate"] = True
        specs[0]["temporal_reducer"] = "max"
        results, _ = process_event("storm1", event_mask, collections, specs, static)
        # negated max over {-1,-5,-3} is -1
        assert results["max_t2m"] == pytest.approx(-1.0)

    def test_transform_applied_exactly_once(self):
        # The desugar gives a single transform apply path: a spec carrying a
        # `transform` (what `anomaly: true` desugars to) runs it once per
        # timestep, never twice. A counting transform proves no double-apply.
        from zagg import registry
        from zagg.config import load_config_from_dict

        calls = {"n": 0}

        def _counting(var_t, static_data, spec):
            calls["n"] += 1
            return var_t

        registry.register_field_transform("counting_anomaly", _counting, replace=True)
        try:
            event_mask, collections, _, static = _event_inputs()
            config = load_config_from_dict(
                {
                    "pipeline": {"type": "temporal"},
                    "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                    "aggregation": {
                        "variables": {
                            "max_t2m": {
                                "variable": "T2M",
                                "collection": "merra2",
                                "spatial_func": "max",
                                "temporal_reducer": "max",
                                "mask": "full",
                                "anomaly": True,
                                "transform": "counting_anomaly",
                            }
                        }
                    },
                    "output": {"format": "tabular", "store": "."},
                }
            )
            specs = specs_from_config(config)
            _, meta = process_event("storm1", event_mask, collections, specs, static)
            # one apply per timestep, not two; 3 timesteps => 3 calls.
            assert calls["n"] == meta["timesteps_processed"] == 3
        finally:
            registry.FIELD_TRANSFORMS._entries.pop("counting_anomaly", None)

    @pytest.mark.parametrize("batch", [None, 1, 2, 3])
    def test_sum_batching_invariant(self, batch):
        # A non-idempotent reducer (sum) must give the same total no matter how
        # the timesteps are sliced into resident batches — this catches a
        # batching bug that ``max`` alone would mask.
        event_mask, collections, specs, static = _event_inputs()
        specs[0].update(
            output_name="sum_t2m",
            spatial_func="weighted_sum",
            temporal_reducer="sum",
        )
        results, _ = process_event(
            "storm1", event_mask, collections, specs, static, max_resident_timesteps=batch
        )
        # per timestep: weighted_sum over 4 unit cells = value*4; sum over
        # {1,5,3} -> (1+5+3)*4 = 36
        assert results["sum_t2m"] == pytest.approx(36.0)

    def test_missing_cell_areas_raises(self):
        event_mask, collections, specs, _ = _event_inputs()
        specs[0]["spatial_func"] = "weighted_sum"
        specs[0]["temporal_reducer"] = "sum"
        with pytest.raises(ValueError, match="cell_areas"):
            process_event("storm1", event_mask, collections, specs, {})

    def test_params_reach_capabilities(self):
        # spec["params"] is the pass-through channel for capability tuning
        # knobs -- a registered (local-backend) provider must see it verbatim.
        seen = {}

        def _param_probe(event_mask_t, static_data, spec):
            seen["params"] = spec.get("params")
            return event_mask_t

        registry.register_mask_provider("param_probe", _param_probe, replace=True)
        try:
            event_mask, collections, specs, static = _event_inputs()
            specs[0]["mask"] = "param_probe"
            specs[0]["params"] = {"knob": 7}
            process_event("storm1", event_mask, collections, specs, static)
            assert seen["params"] == {"knob": 7}
        finally:
            registry.MASK_PROVIDERS._entries.pop("param_probe", None)

    def _gated_inputs(self):
        # Trigger fixture (landfall at t1) + a variable rising 10/20/30 so the
        # captured value identifies which timestep updated the accumulator.
        xr = pytest.importorskip("xarray")
        event_mask, static, time = _trigger_inputs()
        var = xr.DataArray(
            np.stack([np.full((2, 2), v) for v in (10.0, 20.0, 30.0)]),
            dims=["time", "lat", "lon"],
            coords=event_mask.coords,
        )
        collections = {"merra2": xr.Dataset({"SLP": var})}
        specs = [
            {
                "output_name": "slp_at_landfall",
                "variable": "SLP",
                "collection": "merra2",
                "spatial_func": "min",
                "temporal_reducer": "first_landfall",
                "mask": "full",
                "trigger": "first_landfall",
            }
        ]
        return event_mask, collections, specs, static

    def test_trigger_gates_to_landfall_timestep(self):
        # Without the trigger, first_landfall (a first-value capture) would
        # take t0's value (10); gated, it must take the landfall timestep's.
        event_mask, collections, specs, static = self._gated_inputs()
        results, meta = process_event("storm1", event_mask, collections, specs, static)
        assert results["slp_at_landfall"] == pytest.approx(20.0)
        assert meta["timesteps_processed"] == 3

    def test_never_landfalling_event_yields_nan(self):
        event_mask, collections, specs, static = self._gated_inputs()
        never = event_mask.where(event_mask["lat"] != -70.0, 0)  # clear the ice row
        results, _ = process_event("storm1", never, collections, specs, static)
        assert np.isnan(results["slp_at_landfall"])

    def test_mean_of_ratios_diverges_from_weighted_mean(self):
        # A footprint growing from 1 cell to 4 cells: per-timestep ratios are
        # 10 and 20 (mean 15), while pooled parts give 90/5 = 18. Both specs
        # run in one pass to pin the estimators apart end-to-end.
        xr = pytest.importorskip("xarray")
        lat = np.array([-70.0, -69.5])
        lon = np.array([0.0, 0.5])
        time = np.array(["2020-01-01T00", "2020-01-01T03"], dtype="datetime64[ns]")
        coords = {"time": time, "lat": lat, "lon": lon}
        footprints = np.array([[[1, 0], [0, 0]], [[1, 1], [1, 1]]], dtype=float)
        event_mask = xr.DataArray(footprints, dims=["time", "lat", "lon"], coords=coords)
        var = xr.DataArray(
            np.stack([np.full((2, 2), 10.0), np.full((2, 2), 20.0)]),
            dims=["time", "lat", "lon"],
            coords=coords,
        )
        collections = {"merra2": xr.Dataset({"VFLXQV": var})}
        areas = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        base = {
            "variable": "VFLXQV",
            "collection": "merra2",
            "spatial_func": "weighted_mean",
            "mask": "full",
        }
        specs = [
            {**base, "output_name": "avg_ratio", "temporal_reducer": "mean_of_ratios"},
            {**base, "output_name": "avg_pooled", "temporal_reducer": "weighted_mean"},
        ]
        results, _ = process_event("storm1", event_mask, collections, specs, {"cell_areas": areas})
        assert results["avg_ratio"] == pytest.approx(15.0)
        assert results["avg_pooled"] == pytest.approx(18.0)

    @pytest.mark.parametrize("batch", [1, 2])
    def test_trigger_gating_survives_batching(self, batch):
        # The triggered timestep must fire even when it falls mid-batch.
        event_mask, collections, specs, static = self._gated_inputs()
        results, _ = process_event(
            "storm1", event_mask, collections, specs, static, max_resident_timesteps=batch
        )
        assert results["slp_at_landfall"] == pytest.approx(20.0)


class TestSpatialFunctionEdges:
    def test_max_gradient_finite_across_equator(self):
        # The 1/sin(lat) longitude metric blows up at lat=0; the guard must
        # keep the result finite (the equator row drops out of the gradient).
        xr = pytest.importorskip("xarray")
        from zagg.temporal import spatial_max_gradient

        lat = np.array([-1.0, 0.0, 1.0])
        lon = np.array([0.0, 1.0, 2.0])
        coords = {"lat": lat, "lon": lon}
        var = xr.DataArray(np.arange(9.0).reshape(3, 3), dims=["lat", "lon"], coords=coords)
        mask = xr.DataArray(np.ones((3, 3)), dims=["lat", "lon"], coords=coords)
        result = spatial_max_gradient(var, mask, None)
        assert np.isfinite(result)

    def test_min_over_levels_then_weighted_mean(self):
        xr = pytest.importorskip("xarray")
        from zagg.temporal import spatial_min_level_then_weighted_mean

        lat = np.array([-70.0, -69.5])
        lon = np.array([0.0, 0.5])
        coords = {"lev": [1, 2], "lat": lat, "lon": lon}
        # min over the two levels is the level-1 plane (all smaller); then the
        # area-weighted mean over a uniform mask/area is just its mean.
        data = np.stack([np.full((2, 2), 2.0), np.full((2, 2), 5.0)])
        var = xr.DataArray(data, dims=["lev", "lat", "lon"], coords=coords)
        mask = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        areas = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        ws, wt = spatial_min_level_then_weighted_mean(var, mask, areas)
        assert ws == pytest.approx(8.0)  # 4 cells * min-level value 2.0
        assert wt == pytest.approx(4.0)
        assert ws / wt == pytest.approx(2.0)

    def test_max_min_consistent_on_fractional_mask(self):
        # max and min select the same (unscaled) cells under a non-binary mask.
        xr = pytest.importorskip("xarray")
        from zagg.temporal import spatial_max, spatial_min

        lat = np.array([-70.0, -69.5])
        lon = np.array([0.0, 0.5])
        coords = {"lat": lat, "lon": lon}
        var = xr.DataArray(
            np.array([[10.0, 20.0], [30.0, 40.0]]), dims=["lat", "lon"], coords=coords
        )
        mask = xr.DataArray(np.array([[0.5, 0.0], [0.0, 0.5]]), dims=["lat", "lon"], coords=coords)
        # Only the two 0.5 cells (10, 40) are selected; unscaled values used.
        assert spatial_max(var, mask, None) == pytest.approx(40.0)
        assert spatial_min(var, mask, None) == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# Temporal reader (issue #12, Phase 7b)
# ---------------------------------------------------------------------------


class TestTemporalReader:
    def test_open_dataset_local_zarr(self, tmp_path):
        xr = pytest.importorskip("xarray")
        from zagg.temporal import open_dataset

        ds = xr.Dataset(
            {"m": (("lat", "lon"), np.ones((2, 2)))}, coords={"lat": [0, 1], "lon": [0, 1]}
        )
        path = tmp_path / "x.zarr"
        ds.to_zarr(path)
        back = open_dataset(str(path))
        assert list(back.data_vars) == ["m"]

    def test_open_dataset_s3_netcdf_fetches_bytes(self, monkeypatch):
        # The s3:// non-zarr branch fetches object bytes and opens in-memory;
        # stub obstore + S3Store + xr.open_dataset so no live S3 / file backend.
        xr = pytest.importorskip("xarray")
        import obstore
        import obstore.store

        from zagg.temporal import open_dataset

        captured = {}
        sentinel = xr.Dataset({"ais_mask": (("lat",), np.ones(2))}, coords={"lat": [0, 1]})

        monkeypatch.setattr(
            obstore.store,
            "S3Store",
            lambda bucket, **o: captured.update(bucket=bucket, opts=o) or None,
        )

        class _Bytes:
            @staticmethod
            def bytes():
                return b"NETCDFBYTES"

        monkeypatch.setattr(obstore, "get", lambda store, key: captured.update(key=key) or _Bytes())
        monkeypatch.setattr(
            xr, "open_dataset", lambda buf: captured.update(opened=True) or sentinel
        )

        creds = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}
        out = open_dataset("s3://bucket/static/ais_mask.nc", credentials=creds)
        assert captured["bucket"] == "bucket"
        assert captured["key"] == "static/ais_mask.nc"
        assert captured["opened"] is True
        assert list(out.data_vars) == ["ais_mask"]
        # Pure read of a static input: same short policy as the .zarr branch
        # (issue #186) — not the 180 s write default.
        from zagg.store import _S3_READONLY_RETRY_CONFIG

        assert captured["opts"]["retry_config"] == _S3_READONLY_RETRY_CONFIG

    def test_read_temporal_inputs_squeezes_single_var_static(self, monkeypatch):
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        coll = xr.Dataset({"T2M": (("lat",), np.ones(2))}, coords={"lat": [0, 1]})
        # single-variable static file -> returned as a DataArray, not a Dataset
        areas = xr.Dataset({"cell_areas": (("lat",), np.ones(2))}, coords={"lat": [0, 1]})

        def _fake_open(uri, **k):
            return coll if uri.endswith("merra2.zarr") else areas

        monkeypatch.setattr(temporal, "open_dataset", _fake_open)
        collections, static = read_temporal_inputs(
            {"merra2": "s3://b/merra2.zarr"}, {"cell_areas": "s3://b/areas.nc"}
        )
        assert list(collections) == ["merra2"]
        assert isinstance(static["cell_areas"], xr.DataArray)

    def test_multi_uri_collection_concats_time_sorted(self, monkeypatch):
        # A multi-granule event: URIs open individually, concat along time,
        # and land time-sorted even when the URI order is not.
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        def _day(uri, **k):
            day = int(uri[-4])
            time = np.array([f"2020-01-0{day}T00"], dtype="datetime64[ns]")
            return xr.Dataset({"T2M": (("time",), np.array([float(day)]))}, coords={"time": time})

        monkeypatch.setattr(temporal, "open_dataset", _day)
        collections, _ = read_temporal_inputs({"merra2": ["s3://b/d2.nc", "s3://b/d1.nc"]}, {})
        assert list(collections["merra2"]["T2M"].values) == [1.0, 2.0]

    def test_collection_options_applied_by_reader(self, monkeypatch):
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        time = np.array(["2020-01-01T00:30"], dtype="datetime64[ns]")
        ds = xr.Dataset({"T2M": (("time",), np.array([1.0]))}, coords={"time": time})
        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: ds)
        collections, _ = read_temporal_inputs(
            {"merra2": "s3://b/merra2.nc"},
            {},
            collection_options={"merra2": {"time_offset": "-30min"}},
        )
        assert collections["merra2"]["time"].values[0] == np.datetime64("2020-01-01T00:00", "ns")

    def test_xarray_s3_reader_registered(self):
        assert "xarray_s3" in registry.list_readers()
        assert callable(registry.get_reader("xarray_s3"))

    def test_open_dataset_unsigned_builds_anonymous_store(self, monkeypatch):
        # unsigned=True routes to the skip_signature store construction: no
        # credential provider, anonymous requests (issue #223).
        xr = pytest.importorskip("xarray")
        import obstore
        import obstore.store

        from zagg.temporal import open_dataset

        captured = {}
        sentinel = xr.Dataset({"mask": (("lat",), np.ones(2))}, coords={"lat": [0, 1]})

        monkeypatch.setattr(
            obstore.store,
            "S3Store",
            lambda bucket, **o: captured.update(bucket=bucket, opts=o) or None,
        )

        class _Bytes:
            @staticmethod
            def bytes():
                return b"NETCDFBYTES"

        monkeypatch.setattr(obstore, "get", lambda store, key: captured.update(key=key) or _Bytes())
        monkeypatch.setattr(xr, "open_dataset", lambda buf: sentinel)

        out = open_dataset("s3://public-bucket/masks/storm_00001.nc", unsigned=True)
        assert captured["bucket"] == "public-bucket"
        assert captured["opts"].get("skip_signature") is True
        assert "access_key_id" not in captured["opts"]
        assert list(out.data_vars) == ["mask"]

    def test_open_dataset_unsigned_with_credentials_raises(self):
        from zagg.temporal import open_dataset

        with pytest.raises(ValueError, match="mutually exclusive"):
            open_dataset("s3://b/m.nc", credentials={"accessKeyId": "a"}, unsigned=True)

    def test_input_channel_forms(self):
        from zagg.temporal import _input_channel

        creds = {"accessKeyId": "a"}
        assert _input_channel(creds) == (creds, False)
        assert _input_channel("unsigned") == (None, True)
        assert _input_channel(None) == (None, False)
        with pytest.raises(ValueError, match="input_credentials"):
            _input_channel("anonymous")

    def test_read_temporal_inputs_extent_subsets_loads_and_closes(self, monkeypatch):
        # With an event extent, each granule is subset to it (and the
        # collection's variables), loaded, and closed before the next opens --
        # peak memory ~one granule instead of every collection whole
        # (issue #225). This test asserts close() is invoked once per granule;
        # the actual buffer release was verified empirically in the PR-226
        # review (weakref dies after the loop). The structural guarantee here is
        # that close() runs before the next granule opens.
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        lat = np.arange(-80.0, -60.0, 0.5)
        lon = np.arange(-30.0, 30.0, 0.625)
        closed = []

        def _wide(uri, **kwargs):
            day = int(uri[-4])
            time = np.array([f"1980-01-0{day}T00", f"1980-01-0{day}T03"], dtype="datetime64[ns]")
            ds = xr.Dataset(
                {
                    "T2M": (("time", "lat", "lon"), np.full((2, lat.size, lon.size), float(day))),
                    "SLP": (("time", "lat", "lon"), np.zeros((2, lat.size, lon.size))),
                },
                coords={"time": time, "lat": lat, "lon": lon},
            )
            ds.set_close(lambda u=uri: closed.append(u))
            return ds

        monkeypatch.setattr(temporal, "open_dataset", _wide)
        ev_lats = np.array([-70.0, -69.5])
        ev_lons = np.array([0.0, 0.625])
        collections, _ = read_temporal_inputs(
            {"merra2": ["s3://b/d2.nc", "s3://b/d1.nc"]},
            {},
            collection_options={"merra2": {"variables": ["T2M"]}},
            extent=(ev_lats, ev_lons),
        )
        out = collections["merra2"]
        assert list(out.data_vars) == ["T2M"]
        np.testing.assert_array_equal(out["lat"].values, ev_lats)
        np.testing.assert_array_equal(out["lon"].values, ev_lons)
        # concat + sortby: day 1 before day 2, values loaded as plain numpy
        assert out["T2M"].values[0, 0, 0] == 1.0
        assert isinstance(out["T2M"].data, np.ndarray)
        # close() invoked once per granule (spy replaces the h5netcdf closer);
        # buffer release itself verified empirically in the PR-226 review. The
        # structural guarantee is that close runs before the next granule opens.
        assert len(closed) == 2

    def test_read_temporal_inputs_no_extent_stays_lazy_shaped(self, monkeypatch):
        # Without an extent the reader returns the opened datasets untouched
        # (pre-#225 behavior) -- callers outside the worker keep full control.
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        ds = xr.Dataset({"T2M": (("lat",), np.ones(4))}, coords={"lat": np.arange(4.0)})
        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: ds)
        collections, _ = read_temporal_inputs({"merra2": "s3://b/d1.nc"}, {})
        assert collections["merra2"]["T2M"].sizes["lat"] == 4

    def test_read_temporal_inputs_routes_credential_channels(self, monkeypatch):
        # collections read with the source creds; statics ride the
        # input_credentials channel (here: unsigned) -- issue #223.
        xr = pytest.importorskip("xarray")
        import zagg.temporal as temporal
        from zagg.temporal import read_temporal_inputs

        seen = {}
        ds = xr.Dataset({"T2M": (("lat",), np.ones(2))}, coords={"lat": [0, 1]})

        def _capture(uri, **kwargs):
            seen[uri] = kwargs
            return ds

        monkeypatch.setattr(temporal, "open_dataset", _capture)
        source_creds = {"accessKeyId": "gesdisc"}
        read_temporal_inputs(
            {"merra2": "s3://gesdisc/day1.nc4"},
            {"ais_mask": "s3://public/ais.nc"},
            credentials=source_creds,
            input_credentials="unsigned",
        )
        assert seen["s3://gesdisc/day1.nc4"]["credentials"] == source_creds
        assert not seen["s3://gesdisc/day1.nc4"].get("unsigned")
        assert seen["s3://public/ais.nc"]["credentials"] is None
        assert seen["s3://public/ais.nc"]["unsigned"] is True


# ---------------------------------------------------------------------------
# Declarative collection options (issue #213, Phase 3)
# ---------------------------------------------------------------------------


class TestPrepareCollection:
    @pytest.fixture
    def hourly(self):
        # 6 hourly steps stamped on the half hour (the MERRA-2 FLX layout).
        xr = pytest.importorskip("xarray")
        time = np.array([f"2020-01-01T{h:02d}:30" for h in range(6)], dtype="datetime64[ns]")
        coords = {"time": time, "lat": [-70.0], "lon": [0.0]}
        rate = xr.DataArray(
            np.arange(1.0, 7.0).reshape(6, 1, 1), dims=["time", "lat", "lon"], coords=coords
        )
        return xr.Dataset({"PRECLS": rate, "PRECCU": rate * 2})

    def test_falsy_options_are_identity(self, hourly):
        from zagg.temporal import prepare_collection

        assert prepare_collection(hourly, None) is hourly
        assert prepare_collection(hourly, {}) is hourly

    def test_time_offset_shifts_onto_the_hour(self, hourly):
        from zagg.temporal import prepare_collection

        out = prepare_collection(hourly, {"time_offset": "-30min"})
        assert out["time"].values[0] == np.datetime64("2020-01-01T00:00", "ns")

    def test_resample_scales_rates_into_totals(self, hourly):
        from zagg.temporal import prepare_collection

        out = prepare_collection(
            hourly,
            {"time_offset": "-30min", "resample": {"freq": "3h", "how": "sum", "scale": 3600}},
        )
        # hourly rates 1,2,3 in the first 3h bin -> (1+2+3)*3600
        assert float(out["PRECLS"].isel(time=0, lat=0, lon=0)) == pytest.approx(6 * 3600)
        assert out.sizes["time"] == 2

    def test_derived_expression_materialized(self, hourly):
        from zagg.temporal import prepare_collection

        out = prepare_collection(hourly, {"derived": {"rainfall": "PRECLS + PRECCU"}})
        assert float(out["rainfall"].isel(time=0, lat=0, lon=0)) == pytest.approx(3.0)

    def test_variables_subsets_collection(self, hourly):
        from zagg.temporal import prepare_collection

        out = prepare_collection(hourly, {"variables": ["PRECLS"]})
        assert list(out.data_vars) == ["PRECLS"]

    def test_derived_unknown_name_raises_with_available(self, hourly):
        from zagg.temporal import prepare_collection

        with pytest.raises(ValueError, match="rainfall.*TYPO.*PRECCU"):
            prepare_collection(hourly, {"derived": {"rainfall": "PRECLS + TYPO"}})

    def test_unknown_option_keys_ignored(self, hourly):
        # doi &c. are catalog metadata, not reader options.
        from zagg.temporal import prepare_collection

        out = prepare_collection(hourly, {"doi": "10.5067/EXAMPLE"})
        assert list(out.data_vars) == list(hourly.data_vars)
