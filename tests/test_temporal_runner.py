"""End-to-end test of the temporal pipeline through agg() on the local backend.

Registers a tiny in-memory reader plugin (the role the AR domain plugin will
play for real MERRA-2 storms) and drives the whole runner path: pipeline-type
dispatch -> TemporalStrategy -> LocalExecutor -> process_event -> tabular result.
No network, no AWS.
"""

import numpy as np
import pytest

from zagg import registry
from zagg.config import PipelineConfig
from zagg.runner import agg, get_strategy

xr = pytest.importorskip("xarray")


LAT = np.array([-70.0, -69.5, -69.0])
LON = np.array([0.0, 0.5, 1.0])
TIMES = np.array(["2020-01-01T00", "2020-01-01T03", "2020-01-01T06"], dtype="datetime64[ns]")
T2M = np.array([
    [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
    [[10.0, 11.0, 12.0], [13.0, 14.0, 15.0], [16.0, 17.0, 18.0]],
    [[1.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 1.0, 1.0]],
])
STORM = np.broadcast_to(
    np.array([[1.0, 1.0, 0.0], [1.0, 1.0, 0.0], [0.0, 0.0, 0.0]]), (3, 3, 3)
)


class _SyntheticReader:
    """In-memory reader: two events sharing one synthetic collection."""

    EVENTS = ["storm_a", "storm_b"]

    def plan(self, config, catalog_path, *, max_cells=None, selection=None):
        keys = list(self.EVENTS)
        if selection is not None:
            keys = [k for k in keys if k == selection]
        if max_cells is not None:
            keys = keys[:max_cells]
        return keys

    def load_static(self, config, *, creds=None):
        return {
            "cell_areas": xr.DataArray(
                np.ones((3, 3)), dims=["lat", "lon"], coords={"lat": LAT, "lon": LON}
            ),
        }

    def open_event(self, event_key, config, *, creds=None):
        # storm_b is shifted up by 1 so its masked max differs from storm_a.
        offset = 0.0 if event_key == "storm_a" else 100.0
        event_mask = xr.DataArray(
            STORM.copy(), dims=["time", "lat", "lon"],
            coords={"time": TIMES, "lat": LAT, "lon": LON},
        )
        t2m = xr.DataArray(
            T2M + offset, dims=["time", "lat", "lon"],
            coords={"time": TIMES, "lat": LAT, "lon": LON},
        )
        return event_mask, {"C1": xr.Dataset({"T2M": t2m})}


def _temporal_config():
    return PipelineConfig(
        data_source={"reader": "synthetic", "collections": {"C1": {}}},
        aggregation={"variables": {
            "max_T2M": {
                "variable": "T2M", "collection": "C1",
                "spatial_func": "max", "temporal_reducer": "max", "mask": "full",
            },
        }},
        output={"format": "parquet"},
        pipeline={"type": "temporal"},
    )


@pytest.fixture
def synthetic_reader():
    registry.register_reader("synthetic", _SyntheticReader(), overwrite=True)
    try:
        yield
    finally:
        registry.READERS.pop("synthetic", None)


class TestTemporalDispatch:
    def test_strategy_selection(self):
        assert type(get_strategy("temporal")).__name__ == "TemporalStrategy"
        assert type(get_strategy("event")).__name__ == "TemporalStrategy"
        assert type(get_strategy("spatial")).__name__ == "SpatialStrategy"

    def test_unknown_pipeline_type_raises(self):
        with pytest.raises(ValueError, match="Unknown pipeline type"):
            get_strategy("nope")


class TestTemporalRun:
    def test_end_to_end_local(self, synthetic_reader):
        """storm_a masked max over time = 14; storm_b = 114."""
        summary = agg(_temporal_config(), backend="local")
        assert summary["pipeline"] == "temporal"
        assert summary["total_events"] == 2
        assert summary["events_with_data"] == 2
        assert summary["results"]["storm_a"]["max_T2M"] == 14.0
        assert summary["results"]["storm_b"]["max_T2M"] == 114.0

    def test_serial_executor_matches(self, synthetic_reader):
        summary = agg(_temporal_config(), backend="local", max_workers=1)
        assert summary["results"]["storm_a"]["max_T2M"] == 14.0

    def test_max_cells_limits_events(self, synthetic_reader):
        summary = agg(_temporal_config(), backend="local", max_cells=1)
        assert summary["total_events"] == 1

    def test_dry_run(self, synthetic_reader):
        summary = agg(_temporal_config(), backend="local", dry_run=True)
        assert summary["dry_run"] is True
        assert summary["total_events"] == 2

    def test_lambda_backend_not_yet(self, synthetic_reader):
        with pytest.raises(NotImplementedError, match="1d"):
            agg(_temporal_config(), backend="lambda")

    def test_missing_reader_raises(self):
        cfg = _temporal_config()
        cfg.data_source = {"collections": {"C1": {}}}  # no reader
        with pytest.raises(ValueError, match="data_source.reader"):
            agg(cfg, backend="local")

    def test_writes_parquet(self, synthetic_reader, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        store = str(tmp_path / "out.parquet")
        agg(_temporal_config(), backend="local", store=store)
        df = pd.read_parquet(store)
        assert set(df.index) == {"storm_a", "storm_b"}
        assert df.loc["storm_a", "max_T2M"] == 14.0
