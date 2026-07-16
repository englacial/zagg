"""Tests for the Lambda handler's ``mode="process_raster"`` branch (issue #218).

The handler is loaded by path (it lives under ``deployment/aws/``, not an
importable package). Events mirror what ``RasterStrategy``'s lambda backend
dispatches: the shard's ShardMap entries + the orchestrator-owned time index;
the template is emitted by the orchestrator before fan-out.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
from pyproj import CRS, Transformer
from test_raster import ORIGIN, TRANSFORM, UTM18, _index_raster, _write_tiff
from zarr import open_array

from zagg.config import load_config_from_dict
from zagg.grids import from_config
from zagg.processing.raster import emit_raster_template, raster_time_index

REPO_ROOT = Path(__file__).parent.parent
HANDLER_PATH = REPO_ROOT / "deployment" / "aws" / "lambda_handler.py"

T0 = "2026-07-13T16:02:20+00:00"


@pytest.fixture(scope="module")
def handler_mod():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_raster", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _config_dict(store_path, layout=None):
    grid = {"type": "healpix", "parent_order": 10, "child_order": 16}
    if layout:
        grid["layout"] = layout
    return {
        "data_source": {
            "reader": "raster",
            "bands": {"red": {"asset": "red", "dtype": "uint16", "fill_value": 0}},
            "nodata": 0,
        },
        "output": {"grid": grid, "store": str(store_path)},
    }


def _shard_for_raster():
    from mortie import clip2order, geo2mort

    to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
    lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
    leaf = geo2mort(np.array([lat]), np.array([lon]), order=29, points=True)
    return int(clip2order(10, leaf)[0])


@pytest.fixture
def raster_event(tmp_path):
    data = _index_raster()
    _write_tiff(tmp_path / "t0.tif", data)
    store_path = str(tmp_path / "out.zarr")
    cfg_dict = _config_dict(store_path)
    config = load_config_from_dict(cfg_dict)
    entry = {
        "id": "g0",
        "s3": None,
        "https": None,
        "assets": {"red": str(tmp_path / "t0.tif")},
        "datetime": T0,
        "time_key": "dt-1",
    }
    time_index, times = raster_time_index([[entry]])
    grid = from_config(config)
    emit_raster_template(store_path, grid, config, times)
    event = {
        "mode": "process_raster",
        "shard_key": _shard_for_raster(),
        "granules": [entry],
        "config": cfg_dict,
        "store_path": store_path,
        "time_index": time_index,
    }
    return event, grid, data


class TestProcessRasterMode:
    def test_end_to_end_slab_write(self, handler_mod, raster_event):
        event, grid, data = raster_event
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert body["timesteps"] == 1
        assert body["total_obs"] == 1
        assert body["cells_with_data"] == grid.cells_per_shard

        shard = event["shard_key"]
        start = int(grid.block_index(shard)[0]) * grid.cells_per_shard
        red = open_array(
            event["store_path"] + f"/{grid.group_path}/red", zarr_format=3, consolidated=False
        )
        got = red[0, start : start + grid.cells_per_shard]
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
        assert (got[~valid] == 0).all()
        ids = open_array(
            event["store_path"] + f"/{grid.group_path}/cell_ids",
            zarr_format=3,
            consolidated=False,
        )
        np.testing.assert_array_equal(
            ids[start : start + grid.cells_per_shard],
            np.asarray(grid.encode_cell_ids(cells), dtype=np.uint64),
        )

    def test_handler_streams_slabs_incrementally(self, handler_mod, raster_event, monkeypatch):
        # The handler must write + free each timestep's slab as it completes
        # (issue #231), not accumulate all T then loop. A fake worker drives the
        # on_slab sink per timestep and tracks how many slabs are live at each
        # write: exactly one, proving write-then-free rather than accumulate.
        import zagg.processing.raster as raster_mod

        event, grid, _data = raster_event
        n_time = 3
        live = {"cur": 0, "max": 0}
        writes = []
        coords_calls = []

        def _fake_process(grid_, shard_key, granules, config, time_index, *, on_slab=None, **kw):
            assert on_slab is not None  # handler must pass a sink (stream, not buffer)
            for t in range(n_time):
                slab = {"red": np.zeros(grid_.cells_per_shard, dtype=np.uint16)}
                live["cur"] += 1
                live["max"] = max(live["max"], live["cur"])
                on_slab(t, slab)  # handler writes here; the slab is dropped next loop
                live["cur"] -= 1
            return {}, {
                "shard_key": int(shard_key),
                "granule_count": 1,
                "skipped": 0,
                "timesteps": n_time,
            }

        monkeypatch.setattr(raster_mod, "process_raster_shard", _fake_process)
        monkeypatch.setattr(
            raster_mod, "write_raster_slab", lambda store, g, sk, t, slab: writes.append(t)
        )
        monkeypatch.setattr(
            raster_mod, "write_raster_coords", lambda *a, **k: coords_calls.append(1)
        )

        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        assert writes == [0, 1, 2]  # one write per timestep, in stream order
        assert live["max"] == 1  # never more than one slab alive
        assert coords_calls == [1]  # coords written once, after the slabs
        body = json.loads(resp["body"])
        assert body["timesteps"] == n_time
        assert body["cells_with_data"] == grid.cells_per_shard
        assert body["total_obs"] == n_time

    def test_missing_params_400(self, handler_mod):
        resp = handler_mod.lambda_handler({"mode": "process_raster"}, MagicMock())
        assert resp["statusCode"] == 400
        err = json.loads(resp["body"])["error"]
        for key in ("shard_key", "granules", "config", "store_path", "time_index"):
            assert key in err

    def test_dense_layout_500(self, handler_mod, raster_event):
        # The dense layout was removed (issue #88): a worker event still carrying
        # a dense config fails loudly at grid construction.
        event, _grid, _data = raster_event
        event["config"] = _config_dict(event["store_path"], layout="dense")
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 500
        assert "Unknown layout" in json.loads(resp["body"])["error"]

    def test_worker_failure_500(self, handler_mod, raster_event):
        event, _grid, _data = raster_event
        event["granules"] = [
            {
                "id": "bad",
                "assets": {"blue": "/nonexistent.tif"},  # configured 'red' asset absent
                "datetime": T0,
                "time_key": "dt-1",
            }
        ]
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 500
        assert "asset" in json.loads(resp["body"])["error"]


class TestRasterPhaseTimings:
    def test_profile_emits_sample_write_split(self, handler_mod, raster_event):
        event, _grid, _data = raster_event
        event["profile"] = True
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        pt = body["phase_timings"]
        assert set(pt) == {"sample", "write"}
        assert pt["write"] > 0.0
        assert pt["sample"] + pt["write"] == pytest.approx(body["duration_s"], rel=0.05)

    def test_no_profile_key_no_timings(self, handler_mod, raster_event):
        event, _grid, _data = raster_event
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert "phase_timings" not in json.loads(resp["body"])
