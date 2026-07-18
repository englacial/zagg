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


class TestRasterSetupMode:
    """Issue #264: ``mode="setup"`` with a raster config writes the
    ``(time, cells)`` template + ``time`` coordinate handler-side, so the
    dispatcher needs only ``lambda:InvokeFunction`` (the CI OIDC invoke-only
    role gets no S3 write access). The success body echoes
    ``"pipeline": "raster"`` — the dispatcher's stale-deployment guard."""

    TIMES = [1752422540000000, 1752854540000000]

    def _setup_event(self, store_path, times_us, **extra):
        return {
            "mode": "setup",
            "store_path": str(store_path),
            "overwrite": False,
            "config": _config_dict(store_path),
            "times_us": [int(t) for t in times_us],
            **extra,
        }

    def test_setup_writes_template_and_time_coord(self, handler_mod, tmp_path):
        store_path = tmp_path / "raster-setup.zarr"
        resp = handler_mod._handle_setup(self._setup_event(store_path, self.TIMES))
        assert resp["statusCode"] == 200, resp["body"]
        assert json.loads(resp["body"]) == {
            "ok": True,
            "mode": "setup",
            "pipeline": "raster",
            "timesteps": 2,
        }
        grid = from_config(load_config_from_dict(_config_dict(store_path)))
        tarr = open_array(
            str(store_path) + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        np.testing.assert_array_equal(tarr[:], np.asarray(self.TIMES, dtype=np.int64))
        red = open_array(
            str(store_path) + f"/{grid.group_path}/red", zarr_format=3, consolidated=False
        )
        assert red.shape[0] == 2

    def test_setup_template_matches_worker_writes(self, handler_mod, tmp_path):
        # The handler-emitted template must be the SAME template the worker
        # slab-writes into: setup then process_raster, end to end.
        data = _index_raster()
        _write_tiff(tmp_path / "t0.tif", data)
        store_path = str(tmp_path / "e2e.zarr")
        cfg_dict = _config_dict(store_path)
        entry = {
            "id": "g0",
            "s3": None,
            "https": None,
            "assets": {"red": str(tmp_path / "t0.tif")},
            "datetime": T0,
            "time_key": "dt-1",
        }
        time_index, times = raster_time_index([[entry]])
        resp = handler_mod._handle_setup(
            {
                "mode": "setup",
                "store_path": store_path,
                "overwrite": False,
                "config": cfg_dict,
                "times_us": [int(t) for t in times],
            }
        )
        assert resp["statusCode"] == 200, resp["body"]
        event = {
            "mode": "process_raster",
            "shard_key": _shard_for_raster(),
            "granules": [entry],
            "config": cfg_dict,
            "store_path": store_path,
            "time_index": time_index,
        }
        resp = handler_mod.lambda_handler(event, MagicMock())
        assert resp["statusCode"] == 200, resp["body"]
        grid = from_config(load_config_from_dict(cfg_dict))
        start = int(grid.block_index(event["shard_key"])[0]) * grid.cells_per_shard
        red = open_array(store_path + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        got = red[0, start : start + grid.cells_per_shard]
        cells = grid.children(event["shard_key"])
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])

    def test_setup_missing_times_us_500(self, handler_mod, tmp_path):
        event = self._setup_event(tmp_path / "x.zarr", self.TIMES)
        del event["times_us"]
        resp = handler_mod._handle_setup(event)
        assert resp["statusCode"] == 500
        assert "times_us" in json.loads(resp["body"])["error"]

    def test_setup_empty_times_us_500(self, handler_mod, tmp_path):
        # An empty times_us would write a degenerate zero-timestep template
        # (0-length time axis, unusable). The runner refuses an empty catalog
        # pre-dispatch; the invoke-only writer must too (issue #264).
        event = self._setup_event(tmp_path / "empty.zarr", [])
        resp = handler_mod._handle_setup(event)
        assert resp["statusCode"] == 500
        assert "times_us" in json.loads(resp["body"])["error"]

    def test_setup_dense_layout_400(self, handler_mod, tmp_path):
        # Symmetry with the worker (test_dense_layout_400): a dense-layout
        # raster config gets the worker's clean 400 instead of an opaque
        # from_config 500 (issue #264).
        store_path = tmp_path / "dense.zarr"
        event = self._setup_event(store_path, self.TIMES)
        event["config"] = _config_dict(store_path, layout="dense")
        resp = handler_mod._handle_setup(event)
        assert resp["statusCode"] == 400
        assert "fullsphere" in json.loads(resp["body"])["error"]

    def test_setup_rerun_overwrite_semantics(self, handler_mod, tmp_path):
        # Local-path parity (test_overwrite_and_rerun): an identical rerun is
        # idempotent; a CHANGED time index refuses without overwrite=True and
        # rewrites cleanly with it.
        store_path = tmp_path / "rerun.zarr"
        first = handler_mod._handle_setup(self._setup_event(store_path, self.TIMES))
        assert first["statusCode"] == 200, first["body"]
        again = handler_mod._handle_setup(self._setup_event(store_path, self.TIMES))
        assert again["statusCode"] == 200, again["body"]
        changed = self.TIMES[:1]
        refuse = handler_mod._handle_setup(self._setup_event(store_path, changed))
        assert refuse["statusCode"] == 500
        redo = handler_mod._handle_setup(self._setup_event(store_path, changed, overwrite=True))
        assert redo["statusCode"] == 200, redo["body"]
        grid = from_config(load_config_from_dict(_config_dict(store_path)))
        tarr = open_array(
            str(store_path) + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        np.testing.assert_array_equal(tarr[:], np.asarray(changed, dtype=np.int64))

    def test_setup_rerun_same_count_different_values_500(self, handler_mod, tmp_path):
        # A rerun over a different catalog with the SAME timestep count but
        # shifted values must refuse (500) without overwrite, not silently
        # rewrite the persisted time coord out from under the workers' time
        # index (issue #264). The original coordinate is left intact.
        store_path = tmp_path / "samect.zarr"
        first = handler_mod._handle_setup(self._setup_event(store_path, self.TIMES))
        assert first["statusCode"] == 200, first["body"]
        shifted = [self.TIMES[0], self.TIMES[1] + 86_400_000_000]
        refuse = handler_mod._handle_setup(self._setup_event(store_path, shifted))
        assert refuse["statusCode"] == 500
        grid = from_config(load_config_from_dict(_config_dict(store_path)))
        tarr = open_array(
            str(store_path) + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        np.testing.assert_array_equal(tarr[:], np.asarray(self.TIMES, dtype=np.int64))
        redo = handler_mod._handle_setup(self._setup_event(store_path, shifted, overwrite=True))
        assert redo["statusCode"] == 200, redo["body"]
        tarr = open_array(
            str(store_path) + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        np.testing.assert_array_equal(tarr[:], np.asarray(shifted, dtype=np.int64))


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
