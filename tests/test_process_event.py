"""Offline tests for the temporal event worker core (process_event).

Uses small synthetic xarray fixtures — no S3, no network. Expected values are
hand-computed in the docstrings of each test so the engine is checked against an
independent derivation rather than a copy of its own logic.
"""

import numpy as np
import pytest

from zagg import registry
from zagg.temporal import process_event

xr = pytest.importorskip("xarray")


LAT = np.array([-70.0, -69.5, -69.0])
LON = np.array([0.0, 0.5, 1.0])
TIMES = np.array(
    ["2020-01-01T00", "2020-01-01T03", "2020-01-01T06"], dtype="datetime64[ns]"
)

# T2M field per timestep (time, lat, lon)
T2M = np.array([
    [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
    [[10.0, 11.0, 12.0], [13.0, 14.0, 15.0], [16.0, 17.0, 18.0]],
    [[1.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 1.0, 1.0]],
])

# Storm present in the top-left 2x2 block at every timestep.
STORM = np.broadcast_to(
    np.array([[1.0, 1.0, 0.0], [1.0, 1.0, 0.0], [0.0, 0.0, 0.0]]), (3, 3, 3)
)

# AIS = top row only.
AIS = np.array([[1.0, 1.0, 1.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]])


@pytest.fixture
def fixtures():
    event_mask = xr.DataArray(
        STORM.copy(), dims=["time", "lat", "lon"],
        coords={"time": TIMES, "lat": LAT, "lon": LON},
    )
    t2m = xr.DataArray(
        T2M, dims=["time", "lat", "lon"],
        coords={"time": TIMES, "lat": LAT, "lon": LON},
    )
    collections = {"C1": xr.Dataset({"T2M": t2m})}

    # climatology: constant 1.0 for every month/lat/lon
    clim = xr.DataArray(
        np.ones((12, 3, 3)), dims=["month", "lat", "lon"],
        coords={"month": np.arange(1, 13), "lat": LAT, "lon": LON},
    )
    static_data = {
        "ais_mask": xr.DataArray(AIS, dims=["lat", "lon"], coords={"lat": LAT, "lon": LON}),
        "cell_areas": xr.DataArray(np.ones((3, 3)), dims=["lat", "lon"], coords={"lat": LAT, "lon": LON}),
        "climatology": xr.Dataset({"T2M": clim}),
    }
    return event_mask, collections, static_data


def _spec(**overrides):
    base = {
        "output_name": "m", "variable": "T2M", "collection": "C1",
        "spatial_func": "max", "temporal_reducer": "max", "mask": "full",
        "is_anomaly": False, "negate": False, "precip": False,
        "transform": None, "trigger": None,
    }
    base.update(overrides)
    return base


class TestMaskedReductions:
    def test_max_full(self, fixtures):
        """Masked 2x2: t0 max=5, t1 max=14, t2 max=1 -> reducer max = 14."""
        em, coll, static = fixtures
        results, _ = process_event("e", em, coll, [_spec()], static)
        assert results["m"] == 14.0

    def test_max_ais(self, fixtures):
        """storm AND ais (top row, cols 0-1): t0 max=2, t1 max=11, t2=1 -> 11."""
        em, coll, static = fixtures
        results, _ = process_event("e", em, coll, [_spec(mask="ais")], static)
        assert results["m"] == 11.0

    def test_max_ocean(self, fixtures):
        """storm AND NOT ais (row 1, cols 0-1): t0 max=5, t1 max=14, t2=1 -> 14."""
        em, coll, static = fixtures
        results, _ = process_event("e", em, coll, [_spec(mask="ocean")], static)
        assert results["m"] == 14.0

    def test_weighted_mean(self, fixtures):
        """areas=1, full 2x2: weighted_sum totals 12+48+4=64 over weight 12 -> 5.3333."""
        em, coll, static = fixtures
        spec = _spec(spatial_func="weighted_mean", temporal_reducer="weighted_mean")
        results, _ = process_event("e", em, coll, [spec], static)
        assert results["m"] == pytest.approx(64.0 / 12.0)


class TestTransforms:
    def test_negate(self, fixtures):
        """negate then max: max(-v) per t = -1,-10,-1 -> reducer max = -1."""
        em, coll, static = fixtures
        results, _ = process_event("e", em, coll, [_spec(negate=True)], static)
        assert results["m"] == -1.0

    def test_anomaly(self, fixtures):
        """clim=1 so (v-1) max: t0=4, t1=13, t2=0 -> 13."""
        em, coll, static = fixtures
        results, _ = process_event("e", em, coll, [_spec(is_anomaly=True)], static)
        assert results["m"] == 13.0


class TestTriggerGating:
    def test_trigger_limits_timesteps(self, fixtures):
        """A trigger returning only t1 means only t1 (masked max 14) is seen."""
        em, coll, static = fixtures
        try:
            registry.register_event_trigger(
                "only_t1", lambda mask, static_data, spec: TIMES[1], overwrite=True
            )
            spec = _spec(temporal_reducer="first_landfall", trigger="only_t1")
            results, meta = process_event("e", em, coll, [spec], static)
            assert results["m"] == 14.0
            # n_processed still counts all timesteps streamed
            assert meta["timesteps_processed"] == 3
        finally:
            registry.EVENT_TRIGGERS.pop("only_t1", None)


class TestBatchingAndMetadata:
    @pytest.mark.parametrize("batch", [None, 1, 2, 3, 10])
    def test_batching_invariant(self, fixtures, batch):
        em, coll, static = fixtures
        results, _ = process_event(
            "e", em, coll, [_spec()], static, max_resident_timesteps=batch
        )
        assert results["m"] == 14.0

    def test_metadata(self, fixtures):
        em, coll, static = fixtures
        specs = [_spec(output_name="a"), _spec(output_name="b", mask="ais")]
        results, meta = process_event("e42", em, coll, specs, static)
        assert set(results) == {"a", "b"}
        assert meta["event_key"] == "e42"
        assert meta["timesteps_processed"] == 3
        assert meta["n_specs"] == 2
        assert meta["collections"] == ["C1"]
