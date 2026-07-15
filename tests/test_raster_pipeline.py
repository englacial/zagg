"""Tests for the raster pipeline core (issue #218 phase 3).

Config validation, acquisition-group time indexing, per-item multi-band
sampling, the nearest-tile-center ownership rule, and the lean ``(time,
cells)`` template + slab writer — all against synthetic GeoTIFFs from
``test_raster._write_tiff`` (no GDAL, no network) and in-memory Zarr stores.
"""

import numpy as np
import pytest
from pyproj import CRS, Transformer
from test_raster import ORIGIN, RES, TRANSFORM, UTM18, _index_raster, _write_tiff
from zarr import open_array
from zarr.storage import MemoryStore

from zagg.config import get_raster_bands, load_config_from_dict, validate_config
from zagg.grids import HealpixGrid, RectilinearGrid
from zagg.processing.raster import (
    _run_sync,
    _shard_cell_range,
    _shard_workers,
    _write_buffer,
    emit_raster_template,
    process_raster_shard,
    raster_group_spec,
    raster_time_index,
    sample_item_async,
    write_raster_coords,
    write_raster_slab,
)

T0 = "2026-07-13T16:02:20+00:00"
T0B = "2026-07-13T16:02:24+00:00"  # same datatake, adjacent tile: seconds later
T1 = "2026-07-18T16:02:20+00:00"


def _raster_config(bands=None, nodata=0, grid=None):
    return load_config_from_dict(
        {
            "data_source": {
                "reader": "raster",
                "bands": bands
                or {
                    "red": {
                        "asset": "red",
                        "dtype": "uint16",
                        "fill_value": 0,
                        "scale": 0.0001,
                        "offset": -0.1,
                    },
                    "scl": {"asset": "scl", "dtype": "uint16", "fill_value": 0},
                },
                "nodata": nodata,
            },
            "output": {
                "grid": grid or {"type": "healpix", "parent_order": 10, "child_order": 16},
                "store": "memory://",
            },
        }
    )


def _entry(gid, assets, dt, time_key=None):
    e = {"id": gid, "s3": None, "https": None, "assets": assets, "datetime": dt}
    if time_key:
        e["time_key"] = time_key
    return e


class TestRasterConfigValidation:
    def test_valid_config_passes(self):
        validate_config(_raster_config())

    def test_aggregation_section_rejected(self):
        cfg = _raster_config()
        cfg.aggregation = {"variables": {"x": {"function": "mean"}}}
        with pytest.raises(ValueError, match="no aggregation section"):
            validate_config(cfg)

    def test_missing_bands_rejected(self):
        cfg = _raster_config()
        cfg.data_source.pop("bands")
        with pytest.raises(ValueError, match="data_source.bands"):
            validate_config(cfg)

    def test_band_requires_dtype(self):
        with pytest.raises(ValueError, match="requires a string 'dtype'"):
            validate_config(_raster_config(bands={"red": {"asset": "red"}}))

    def test_sharded_rejected(self):
        grid = {"type": "healpix", "parent_order": 10, "child_order": 16, "sharded": True}
        with pytest.raises(ValueError, match="sharded"):
            validate_config(_raster_config(grid=grid))

    def test_rectilinear_grid_rejected_for_now(self):
        grid = {"type": "rectilinear", "crs": UTM18, "resolution": 10, "bounds": [0, 0, 1, 1]}
        with pytest.raises(ValueError, match="healpix"):
            validate_config(_raster_config(grid=grid))

    def test_bool_fill_value_rejected(self):
        with pytest.raises(ValueError, match="fill_value must be a number"):
            validate_config(
                _raster_config(
                    bands={"red": {"asset": "red", "dtype": "uint16", "fill_value": True}}
                )
            )

    def test_get_raster_bands_normalizes(self):
        bands = get_raster_bands(_raster_config())
        assert bands["red"]["attrs"] == {"scale_factor": 0.0001, "add_offset": -0.1}
        assert bands["red"]["fill_value"] == 0
        assert bands["scl"]["attrs"] == {}

    def test_shard_workers_default_and_override(self):
        assert _shard_workers(_raster_config()) == 4  # issue #231 default
        cfg = _raster_config()
        cfg.data_source["shard_workers"] = 8
        assert _shard_workers(cfg) == 8

    def test_shard_workers_rejected(self):
        for bad in (0, -1, True, 2.0):
            cfg = _raster_config()
            cfg.data_source["shard_workers"] = bad
            with pytest.raises(ValueError, match="shard_workers"):
                validate_config(cfg)
            # The worker helper re-checks with the same guard (hand-rolled payload).
            with pytest.raises(ValueError, match="shard_workers"):
                _shard_workers(cfg)


class TestRasterTimeIndex:
    def test_time_key_groups_adjacent_tiles(self):
        granules = [
            [
                _entry("a", {"red": "x"}, T0, time_key="dt-1"),
                _entry("b", {"red": "y"}, T0B, time_key="dt-1"),
                _entry("c", {"red": "z"}, T1, time_key="dt-2"),
            ]
        ]
        index, times = raster_time_index(granules)
        assert index == {"dt-1": 0, "dt-2": 1}
        # Group time is the EARLIEST member datetime.
        assert times[0] == np.int64(1_783_958_540_000_000)
        assert times.dtype == np.int64 and times.shape == (2,)

    def test_datetime_fallback_without_time_key(self):
        granules = [[_entry("a", {"red": "x"}, T0), _entry("b", {"red": "y"}, T1)]]
        index, times = raster_time_index(granules)
        assert index[T0] == 0 and index[T1] == 1
        assert times[1] > times[0]

    def test_non_raster_entries_ignored(self):
        granules = [[{"id": "h5", "s3": "s3://b/g.h5", "https": None}]]
        index, times = raster_time_index(granules)
        assert index == {} and times.size == 0

    def test_missing_datetime_raises(self):
        with pytest.raises(ValueError, match="no datetime"):
            raster_time_index([[{"id": "bad", "assets": {"red": "x"}}]])


class _FakeGrid:
    """Minimal grid for the sampling-concurrency test: only ``children`` is
    touched on the single-item-per-group path (no ownership combine)."""

    def __init__(self, n_cells):
        self._n = n_cells

    def children(self, shard_key):
        return np.arange(self._n)


class TestSampleConcurrency:
    # k=1 serial (peak 1), an interior cap (peak k), and k>=n_groups (peak
    # n_groups): an off-by-one in the semaphore width passes k=3 but fails k=1.
    @pytest.mark.parametrize("k", [1, 3, 10, 12])
    def test_semaphore_bounds_in_flight_groups(self, monkeypatch, k):
        # N single-item acquisition groups sampled under Semaphore(K): each
        # group is one ``sample_item_async`` call, so concurrent calls track
        # concurrent timesteps. An instrumented fake records the peak, which
        # must be capped at min(K, N) yet actually reach it (issue #231: the cap
        # bounds memory without serializing the fan-out).
        import asyncio as _asyncio

        from zagg.processing import raster as raster_mod

        n_cells, n_groups = 8, 10
        state = {"cur": 0, "max": 0}
        lock = _asyncio.Lock()

        async def _fake_sample_item(
            grid, cells, assets, bands, *, nodata=None, region=None, anonymous=True
        ):
            async with lock:
                state["cur"] += 1
                state["max"] = max(state["max"], state["cur"])
            await _asyncio.sleep(0.02)
            async with lock:
                state["cur"] -= 1
            n = len(cells)
            return {f: np.zeros(n, dtype=np.uint16) for f in bands}, np.ones(n, bool), (0.0, 0.0)

        monkeypatch.setattr(raster_mod, "sample_item_async", _fake_sample_item)

        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        cfg.data_source["shard_workers"] = k
        granules = [
            _entry(f"g{i}", {"red": f"r{i}.tif"}, T0, time_key=f"dt-{i}") for i in range(n_groups)
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(_FakeGrid(n_cells), 0, granules, cfg, index)
        assert meta["timesteps"] == n_groups
        assert set(slabs) == set(range(n_groups))
        # Bounded by min(K, N), and the fan-out reaches it (not serialized).
        assert state["max"] == min(k, n_groups)


def _rect_grid(bounds, chunk):
    from zagg.config import default_config

    return RectilinearGrid(UTM18, RES, bounds, chunk, config=default_config("atl06_polar"))


class TestSampleItem:
    def test_multi_band_values_and_nodata(self, tmp_path):
        data = _index_raster()
        data[:8, :8] = 0  # nodata corner (config nodata=0)
        _write_tiff(tmp_path / "red.tif", data)
        _write_tiff(tmp_path / "scl.tif", np.full((96, 96), 4, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cells = np.arange(96 * 96)
        bands = get_raster_bands(_raster_config())
        assets = {"red": str(tmp_path / "red.tif"), "scl": str(tmp_path / "scl.tif")}
        values, valid, center = _run_sync(sample_item_async(grid, cells, assets, bands, nodata=0))
        expect = _index_raster()
        expect[:8, :8] = 0
        np.testing.assert_array_equal(values["red"], expect.ravel())
        assert (values["scl"] == 4).all()
        # nodata corner is invalid; the rest valid.
        assert not valid.reshape(96, 96)[:8, :8].any()
        assert valid.reshape(96, 96)[8:, 8:].all()
        # tile center of the 960 m raster, back-projected.
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
        assert center == pytest.approx((lon, lat), abs=1e-9)

    def test_missing_configured_asset_raises(self, tmp_path):
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        bands = get_raster_bands(_raster_config())
        with pytest.raises(ValueError, match="missing configured asset"):
            _run_sync(sample_item_async(grid, np.arange(4), {"red": "x.tif"}, bands))


class TestOwnership:
    def test_nearest_tile_center_wins_in_overlap(self, tmp_path):
        # Tile A (constant 100) and tile B (constant 200) in one datatake,
        # offset 480 m: overlap x in [300480, 300960); centers 300480 / 300960,
        # midline 300720.
        _write_tiff(tmp_path / "a.tif", np.full((96, 96), 100, dtype=np.uint16))
        _write_tiff(
            tmp_path / "b.tif",
            np.full((96, 96), 200, dtype=np.uint16),
            origin=(ORIGIN[0] + 480.0, ORIGIN[1]),
        )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1440.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 144])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "b.tif")}, T0B, time_key="dt-1"),
        ]
        index, _times = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 1 and set(slabs) == {0}
        red = slabs[0]["red"].reshape(96, 144)
        xs = ORIGIN[0] + (np.arange(144) + 0.5) * RES
        assert (red[:, xs < 300700] == 100).all()  # A side (margin off the midline)
        assert (red[:, (xs > 300740) & (xs < 300960)] == 200).all()  # B side of overlap
        assert (red[:, xs > 300970] == 200).all()  # B-only region
        assert (red[:, xs < 300480] == 100).all()  # A-only region

    def test_missing_time_key_in_index_raises(self, tmp_path):
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [_entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-absent")]
        with pytest.raises(ValueError, match="dt-absent"):
            process_raster_shard(grid, 0, granules, cfg, {})

    def test_two_timesteps_two_items_concurrent(self, tmp_path):
        # 2 datatakes x 2 overlapping tiles sampled in one event loop: each
        # timestep's ownership combine must match the sequential golden.
        for name, const, ox in (
            ("a0", 100, 0.0),
            ("b0", 200, 480.0),
            ("a1", 50, 0.0),
            ("b1", 75, 480.0),
        ):
            _write_tiff(
                tmp_path / f"{name}.tif",
                np.full((96, 96), const, dtype=np.uint16),
                origin=(ORIGIN[0] + ox, ORIGIN[1]),
            )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1440.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 144])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A0", {"red": str(tmp_path / "a0.tif")}, T0, time_key="dt-1"),
            _entry("B0", {"red": str(tmp_path / "b0.tif")}, T0B, time_key="dt-1"),
            _entry("A1", {"red": str(tmp_path / "a1.tif")}, T1, time_key="dt-2"),
            _entry("B1", {"red": str(tmp_path / "b1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 2 and set(slabs) == {0, 1}
        xs = ORIGIN[0] + (np.arange(144) + 0.5) * RES
        for t, aval, bval in ((0, 100, 200), (1, 50, 75)):
            red = slabs[t]["red"].reshape(96, 144)
            assert (red[:, xs < 300480] == aval).all()  # A-only region
            assert (red[:, xs > 300970] == bval).all()  # B-only region
            assert (red[:, (xs > 300740) & (xs < 300960)] == bval).all()  # B side of overlap

    def test_three_item_ownership(self, tmp_path):
        # Three overlapping tiles offset 480 m apart, one datatake, distinct
        # constants: every cell must take the nearest tile center's value.
        for name, const, ox in (("a", 100, 0.0), ("b", 200, 480.0), ("c", 300, 960.0)):
            _write_tiff(
                tmp_path / f"{name}.tif",
                np.full((96, 96), const, dtype=np.uint16),
                origin=(ORIGIN[0] + ox, ORIGIN[1]),
            )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1920.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 192])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "b.tif")}, T0B, time_key="dt-1"),
            _entry("C", {"red": str(tmp_path / "c.tif")}, T0B, time_key="dt-1"),
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 1
        red = slabs[0]["red"].reshape(96, 192)
        xs = ORIGIN[0] + (np.arange(192) + 0.5) * RES
        assert (red[:, xs < 300470] == 100).all()  # A-only region
        assert (red[:, (xs > 300740) & (xs < 300950)] == 200).all()  # A/B overlap, B nearer
        assert (red[:, (xs > 301000) & (xs < 301180)] == 200).all()  # B/C overlap, B nearer
        assert (red[:, (xs > 301220) & (xs < 301430)] == 300).all()  # B/C overlap, C nearer
        assert (red[:, xs > 301450] == 300).all()  # C-only region

    def test_single_item_timesteps_and_skips(self, tmp_path):
        _write_tiff(tmp_path / "a.tif", np.full((96, 96), 7, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0),
            {"id": "h5-styled", "s3": "s3://b/g.h5", "https": None},
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["skipped"] == 1 and meta["granule_count"] == 2
        assert (slabs[0]["red"] == 7).all()

    @pytest.mark.parametrize("wb", [2, 3])
    def test_write_buffer_bounded_and_matches(self, tmp_path, wb):
        # PR #232 double-buffer: with write_buffer=N the sink runs on worker
        # threads, at most N slabs are alive at once, overlap actually occurs,
        # and the streamed output still matches dict mode exactly.
        import threading
        import time as _time

        vals = {"dt-1": 11, "dt-2": 22, "dt-3": 33, "dt-4": 44}
        granules = []
        for i, (tk, v) in enumerate(vals.items()):
            _write_tiff(tmp_path / f"s{i}.tif", np.full((96, 96), v, dtype=np.uint16))
            granules.append(
                _entry(
                    f"g{i}",
                    {"red": str(tmp_path / f"s{i}.tif")},
                    f"2026-07-{13 + i:02d}T16:02:20+00:00",
                    time_key=tk,
                )
            )
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        index, _ = raster_time_index([granules])
        golden, _gm = process_raster_shard(grid, 0, granules, cfg, index)

        cfg.data_source["write_buffer"] = wb
        lock = threading.Lock()
        live = {"now": 0, "max": 0}
        streamed = {}

        def _sink(t_idx, slab):
            with lock:
                live["now"] += 1
                live["max"] = max(live["max"], live["now"])
            _time.sleep(0.05)  # slow write: forces overlap under the buffer
            streamed[t_idx] = slab
            with lock:
                live["now"] -= 1

        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)
        assert slabs == {} and meta["timesteps"] == 4
        assert live["max"] <= wb - 1  # sink calls in flight (slabs alive <= wb)
        assert set(streamed) == set(golden)
        for t in golden:
            np.testing.assert_array_equal(streamed[t]["red"], golden[t]["red"])

    def test_write_buffer_validation(self):
        cfg = _raster_config()
        assert _write_buffer(cfg) == 1  # default: strict serial bound
        cfg.data_source["write_buffer"] = 2
        assert _write_buffer(cfg) == 2
        for bad in (0, -1, 1.5, True, "2"):
            cfg.data_source["write_buffer"] = bad
            with pytest.raises(ValueError, match="write_buffer"):
                _write_buffer(cfg)
            with pytest.raises(ValueError, match="write_buffer"):
                validate_config(cfg)

    def test_write_buffer_sink_error_propagates(self, tmp_path):
        _write_tiff(tmp_path / "t0.tif", np.full((96, 96), 11, dtype=np.uint16))
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 22, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        cfg.data_source["write_buffer"] = 2
        granules = [
            _entry("A", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])

        def _sink(t_idx, slab):
            raise OSError("s3 write failed")

        with pytest.raises(OSError, match="s3 write failed"):
            process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)

    def test_on_slab_streams_and_matches_dict(self, tmp_path):
        # The on_slab sink (issue #231): each timestep's slab is handed off as
        # its group completes and NOT accumulated (returned slabs is empty),
        # yet the streamed slabs match the buffered dict-mode output exactly.
        _write_tiff(tmp_path / "t0.tif", np.full((96, 96), 11, dtype=np.uint16))
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 22, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        granules = [
            _entry("A", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])
        golden, _gm = process_raster_shard(grid, 0, granules, cfg, index)

        streamed = {}

        def _sink(t_idx, slab):
            streamed[t_idx] = slab

        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)
        assert slabs == {}  # streamed + freed, nothing accumulated
        assert meta["timesteps"] == 2
        assert set(streamed) == set(golden) == {0, 1}
        for t in golden:
            np.testing.assert_array_equal(streamed[t]["red"], golden[t]["red"])


def _healpix_setup(tmp_path):
    """Order-10 shard over the synthetic raster; order-16 cells (~97 m)."""
    from mortie import clip2order, geo2mort

    to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
    lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
    leaf = geo2mort(np.array([lat]), np.array([lon]), order=29, points=True)
    shard = int(clip2order(10, leaf)[0])
    cfg = _raster_config(
        bands={"red": {"asset": "red", "dtype": "uint16", "scale": 0.0001, "offset": -0.1}},
        grid={"type": "healpix", "parent_order": 10, "child_order": 16},
    )
    grid = HealpixGrid(10, 16, config=cfg, populated_shards=[shard])
    return cfg, grid, shard


class TestTemplateAndSlabs:
    def test_group_spec_shapes(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_group_spec(grid, cfg, 3)
        red = spec.members["red"]
        assert tuple(red.shape) == (3, 4096)
        cg = red.chunk_grid
        cfg_block = cg["configuration"] if isinstance(cg, dict) else cg.configuration
        assert tuple(cfg_block["chunk_shape"]) == (1, grid.cells_per_chunk)
        assert red.attributes["scale_factor"] == 0.0001
        assert red.attributes["add_offset"] == -0.1
        assert tuple(spec.members["time"].shape) == (3,)
        assert tuple(spec.members["cell_ids"].shape) == (4096,)

    def test_sharded_grid_rejected(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        grid.sharded = True
        with pytest.raises(ValueError, match="sharded"):
            raster_group_spec(grid, cfg, 1)

    def test_end_to_end_two_timesteps(self, tmp_path):
        cfg, grid, shard = _healpix_setup(tmp_path)
        data = _index_raster()
        _write_tiff(tmp_path / "t0.tif", data)
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 321, dtype=np.uint16))
        granules = [
            _entry("g0", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("g1", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, times = raster_time_index([granules])

        store = MemoryStore()
        emit_raster_template(store, grid, cfg, times)
        slabs, meta = process_raster_shard(grid, shard, granules, cfg, index)
        assert set(slabs) == {0, 1}
        for t, slab in slabs.items():
            write_raster_slab(store, grid, shard, t, slab)
        write_raster_coords(store, grid, shard)

        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (2, 4096)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        got_t0 = red[0, :]
        np.testing.assert_array_equal(got_t0[valid], data[rows[valid], cols[valid]])
        assert (got_t0[~valid] == 0).all()  # fill outside the raster footprint
        got_t1 = red[1, :]
        assert (got_t1[valid] == 321).all()
        # time coordinate round-trips as microseconds since epoch.
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], times)
        # cell_ids written for the shard's block, in children order.
        ids = open_array(
            store, path=f"{grid.group_path}/cell_ids", zarr_format=3, consolidated=False
        )
        np.testing.assert_array_equal(
            ids[:], np.asarray(grid.encode_cell_ids(cells), dtype=np.uint64)
        )
        assert valid.sum() > 50  # the 960 m raster covers many ~97 m cells

    def test_zero_time_template(self, tmp_path):
        # An empty-times template (no datatakes yet) must emit, not crash.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_group_spec(grid, cfg, 0)
        assert tuple(spec.members["time"].shape) == (0,)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([], dtype=np.int64))
        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (0, 4096)

    def test_time_attrs_round_trip(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([1_000_000, 2_000_000], dtype=np.int64))
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        assert tarr.attrs["units"] == "microseconds since 1970-01-01T00:00:00"
        assert tarr.attrs["calendar"] == "proleptic_gregorian"

    def test_fullsphere_end_to_end_slab(self, tmp_path):
        # Fullsphere layout: shape 12*4^child, one shard == cells_per_shard.
        from mortie import clip2order, geo2mort

        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
        leaf = geo2mort(np.array([lat]), np.array([lon]), order=29, points=True)
        shard = int(clip2order(4, leaf)[0])
        cfg = _raster_config(
            bands={"red": {"asset": "red", "dtype": "uint16"}},
            grid={"type": "healpix", "parent_order": 4, "child_order": 8},
        )
        grid = HealpixGrid(4, 8, layout="fullsphere", config=cfg)
        data = _index_raster()
        _write_tiff(tmp_path / "r.tif", data)
        granules = [_entry("g", {"red": str(tmp_path / "r.tif")}, T0, time_key="dt-1")]
        index, times = raster_time_index([granules])

        store = MemoryStore()
        emit_raster_template(store, grid, cfg, times)
        slabs, meta = process_raster_shard(grid, shard, granules, cfg, index)
        for t, slab in slabs.items():
            write_raster_slab(store, grid, shard, t, slab)
        write_raster_coords(store, grid, shard)

        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, 12 * 4**8)  # 786432
        start, stop = _shard_cell_range(grid, shard)
        assert stop - start == 256  # 4^(child - parent)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        got = red[0, start:stop]
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
        assert (got[~valid] == 0).all()  # fill outside the raster footprint
