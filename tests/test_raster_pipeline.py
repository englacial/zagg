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

    def test_get_raster_bands_normalizes(self):
        bands = get_raster_bands(_raster_config())
        assert bands["red"]["attrs"] == {"scale_factor": 0.0001, "add_offset": -0.1}
        assert bands["red"]["fill_value"] == 0
        assert bands["scl"]["attrs"] == {}


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
