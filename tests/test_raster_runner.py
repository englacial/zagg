"""Runner-level tests for the raster pipeline (issue #218 phase 4).

``agg(backend="local")`` end-to-end over a ShardMap manifest and synthetic
GeoTIFFs: strategy selection, template emission, shard fan-out, slab writes,
and the shipped ``sentinel2_l2a`` template config.
"""

import numpy as np
import pytest
from pyproj import CRS, Transformer
from test_raster import ORIGIN, TRANSFORM, UTM18, _index_raster, _write_tiff
from zarr import open_array

from zagg.catalog.shardmap import ShardMap
from zagg.config import default_config, load_config_from_dict, validate_config
from zagg.grids import from_config
from zagg.runner import agg

T0 = "2026-07-13T16:02:20+00:00"
T1 = "2026-07-18T16:02:20+00:00"


def _cfg(tmp_path):
    return load_config_from_dict(
        {
            "data_source": {
                "reader": "raster",
                "bands": {
                    "red": {
                        "asset": "red",
                        "dtype": "uint16",
                        "fill_value": 0,
                        "scale": 0.0001,
                        "offset": -0.1,
                    }
                },
                "nodata": 0,
            },
            "output": {
                "grid": {"type": "healpix", "parent_order": 10, "child_order": 16},
                "store": str(tmp_path / "out.zarr"),
            },
        }
    )


def _shard_for_raster():
    from mortie import clip2order, geo2mort

    to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
    lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
    leaf = geo2mort(np.array([lat]), np.array([lon]), order=29, points=True)
    return int(clip2order(10, leaf)[0])


def _entry(gid, href, dt, time_key):
    return {
        "id": gid,
        "s3": None,
        "https": None,
        "assets": {"red": href},
        "datetime": dt,
        "time_key": time_key,
    }


@pytest.fixture
def manifest(tmp_path):
    data = _index_raster()
    _write_tiff(tmp_path / "t0.tif", data)
    _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 555, dtype=np.uint16))
    cfg = _cfg(tmp_path)
    shard = _shard_for_raster()
    grid = from_config(cfg, populated_shards=[shard])
    entries = [
        _entry("g0", str(tmp_path / "t0.tif"), T0, "dt-1"),
        _entry("g1", str(tmp_path / "t1.tif"), T1, "dt-2"),
    ]
    sm = ShardMap(grid.spatial_signature(), [shard], [entries], {"collection": "s2-test"})
    path = str(tmp_path / "shardmap.json")
    sm.to_json(path)
    return cfg, path, shard, data


class TestRasterAgg:
    def test_end_to_end_local(self, tmp_path, manifest):
        cfg, sm_path, shard, data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert summary["total_cells"] == 1
        assert summary["cells_with_data"] == 1
        assert summary["cells_error"] == 0
        assert summary["timesteps"] == 2
        assert summary["total_obs"] == 2

        grid = from_config(cfg, populated_shards=[shard])
        store_path = cfg.output["store"]
        red = open_array(store_path + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        from zagg.processing.raster import _shard_cell_range

        start, stop = _shard_cell_range(grid, shard)
        got = red[0, start:stop]
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
        assert (got[~valid] == 0).all()
        assert (red[1, start:stop][valid] == 555).all()
        # scale/offset ride as CF attrs, data stays exact DN.
        assert red.attrs["scale_factor"] == 0.0001
        assert red.attrs["add_offset"] == -0.1
        # time coordinate matches the two datatakes, ascending.
        tarr = open_array(
            store_path + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        assert tarr.shape == (2,) and tarr[0] < tarr[1]

    def test_dry_run(self, manifest):
        cfg, sm_path, _shard, _data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", dry_run=True)
        assert summary["dry_run"] is True and summary["total_cells"] == 1

    def test_lambda_backend_not_yet(self, manifest):
        cfg, sm_path, _shard, _data = manifest
        with pytest.raises(NotImplementedError, match="Lambda"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")


class TestShippedTemplate:
    def test_sentinel2_l2a_config_loads_and_validates(self):
        cfg = default_config("sentinel2_l2a")
        validate_config(cfg)
        assert cfg.data_source["reader"] == "raster"
        assert cfg.data_source["bands"]["scl"]["dtype"] == "uint8"
        assert cfg.output["grid"]["child_order"] == 19
