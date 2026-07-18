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
        # Worker memory telemetry (issue #250, point-path parity): the sampled
        # per-invocation peak with the container high-water fallback -- always
        # present and positive, profile or not.
        assert body["max_memory_mb"] > 0
        assert body["container_hwm_mb"] > 0
        assert body["max_memory_mb"] <= body["container_hwm_mb"] * 1.5  # same scale

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

    def test_dense_layout_400(self, handler_mod, raster_event):
        event, _grid, _data = raster_event
        event["config"] = _config_dict(event["store_path"], layout="dense")
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 400
        assert "fullsphere" in json.loads(resp["body"])["error"]

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
        assert set(pt) == {"sample", "write", "stages"}
        assert pt["write"] > 0.0
        assert pt["sample"] + pt["write"] == pytest.approx(body["duration_s"], rel=0.05)

    def test_no_profile_key_no_timings(self, handler_mod, raster_event):
        event, _grid, _data = raster_event
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert "phase_timings" not in json.loads(resp["body"])

    def test_profile_emits_stage_split(self, handler_mod, raster_event):
        # Issue #249: the sample bucket split per stage + counts, additive
        # next to the unchanged sample/write keys.
        event, _grid, _data = raster_event
        event["profile"] = True
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        stages = json.loads(resp["body"])["phase_timings"]["stages"]
        floats = ("open", "geometry", "fetch", "decode", "gather")
        assert set(stages) == {*floats, "assets", "tiles", "geom_hits"}
        assert stages["assets"] == 1  # 1 timestep x 1 band
        assert stages["geom_hits"] == 0  # single asset, nothing to hit
        assert stages["tiles"] >= 1
        assert all(stages[k] >= 0.0 for k in floats)
        assert sum(stages[k] for k in floats) > 0.0

    def test_no_profile_passes_no_stage_stats(self, handler_mod, raster_event, monkeypatch):
        # Zero-overhead gate: without ``profile`` the worker must receive
        # stage_stats=None so the sample path makes no timing calls at all.
        import zagg.processing.raster as raster_mod

        event, _grid, _data = raster_event
        seen = {}

        def _fake(grid_, shard_key, granules, config, time_index, *, stage_stats=None, **kw):
            seen["stage_stats"] = stage_stats
            return {}, {
                "shard_key": int(shard_key),
                "granule_count": 1,
                "skipped": 0,
                "timesteps": 0,
            }

        monkeypatch.setattr(raster_mod, "process_raster_shard", _fake)
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        assert seen["stage_stats"] is None
        assert "phase_timings" not in json.loads(resp["body"])


class TestProcessRasterHiveMode:
    """The hive branch of mode="process_raster" (issue #247 phase 4)."""

    def _hive_event(self, tmp_path, window=None, windowing=None):
        data = np.full((96, 96), 555, dtype=np.uint16)
        _write_tiff(tmp_path / "h0.tif", data)
        store_path = str(tmp_path / "hive_out")
        cfg_dict = _config_dict(store_path)
        cfg_dict["output"]["store_layout"] = "hive"
        if windowing:
            cfg_dict["output"]["windowing"] = windowing
        entry = {
            "id": "g0",
            "s3": None,
            "https": None,
            "assets": {"red": str(tmp_path / "h0.tif")},
            "datetime": T0,
            "time_key": "dt-1",
        }
        event = {
            "mode": "process_raster",
            "shard_key": _shard_for_raster(),
            "granules": [entry],
            "config": cfg_dict,
            "store_path": store_path,
        }
        if window:
            event["window"] = window
        return event

    def test_windowed_leaf_written_and_stamped(self, handler_mod, tmp_path):
        from zagg import hive

        event = self._hive_event(
            tmp_path,
            window={"label": "20260713"},
            windowing={"schedule": "daily"},
        )
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert body["timesteps"] == 1 and body["total_obs"] == 1
        # The response mirrors the stamped ISO time range for the dispatcher's
        # root-summary union.
        assert body["time_range"] == [T0, T0]
        leaf = hive.shard_leaf_path(event["store_path"], event["shard_key"], window="20260713")
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["complete"] and stamp["spec"] == "morton-hive/2"
        assert stamp["window"] == "20260713"
        assert body["cells_with_data"] == stamp["cells_with_data"] > 0

    def test_schedule_none_bare_leaf(self, handler_mod, tmp_path):
        from zagg import hive

        event = self._hive_event(tmp_path)  # no window, no windowing block
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert "time_range" not in body
        leaf = hive.shard_leaf_path(event["store_path"], event["shard_key"])
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["spec"] == "morton-hive/1" and "window" not in stamp

    def test_hive_missing_params_omit_time_index(self, handler_mod):
        # A hive event needs no time_index: the 400 for an empty hive event
        # names the other four requirements only.
        event = {
            "mode": "process_raster",
            "config": {"output": {"store_layout": "hive"}},
        }
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 400
        err = json.loads(resp["body"])["error"]
        assert "time_index" not in err
        for key in ("shard_key", "granules", "store_path"):
            assert key in err

    def test_flat_missing_params_unchanged(self, handler_mod):
        # Flat (and config-less) events keep the pre-#247 requirement list.
        resp = handler_mod.lambda_handler({"mode": "process_raster"}, MagicMock())
        assert resp["statusCode"] == 400
        assert "time_index" in json.loads(resp["body"])["error"]

    def test_malformed_output_falls_through_to_flat(self, handler_mod):
        # A config whose ``output`` is a truthy non-dict (e.g. a string) must
        # not raise AttributeError in the pre-load hive peek (an opaque 500);
        # the shape-safe peek treats it as non-hive, so the event falls through
        # to the flat requirements (time_index included) and the clean 400.
        event = {"mode": "process_raster", "config": {"output": "hive"}}
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 400
        err = json.loads(resp["body"])["error"]
        assert "time_index" in err
        for key in ("shard_key", "granules", "store_path"):
            assert key in err

    def test_profile_rides_hive_meta(self, handler_mod, tmp_path):
        event = self._hive_event(tmp_path, window={"label": "20260713"})
        event["profile"] = True
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp
        pt = json.loads(resp["body"])["phase_timings"]
        assert set(pt) == {"sample", "write", "stages"}
        assert pt["write"] > 0.0
        assert pt["stages"]["assets"] == 1
