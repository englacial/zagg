"""Tests for ShardMap building (shapely + mortie backends; IO; resolution).

The spherely (SpatialIndex) backend is exercised separately in the conda
sidecar env since its build isn't in the default venv.
"""

import json
import tempfile

import pyarrow as pa
import pytest
import stac_geoparquet.arrow as sga

from zagg.catalog.shardmap import ShardMap, _resolve_backend
from zagg.catalog.sources import Catalog
from zagg.config import default_config
from zagg.grids import HealpixGrid, RectilinearGrid


def _item(gid, lon0, lon1, lat0=38.85, lat1=38.93):
    ring = [[lon0, lat0], [lon1, lat0], [lon1, lat1], [lon0, lat1], [lon0, lat0]]
    return {
        "type": "Feature", "stac_version": "1.0.0", "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [lon0, lat0, lon1, lat1],
        "properties": {"datetime": "2025-06-01T00:00:00Z"},
        "collection": "TEST", "stac_extensions": [], "links": [],
        "assets": {
            "data": {"href": f"https://h/{gid}.h5", "roles": ["data"]},
            "data_s3": {"href": f"s3://b/{gid}.h5", "roles": ["data"]},
        },
    }


def _catalog(items):
    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "TEST", "bbox": [-76.62107, 38.84504, -76.50583, 38.93512]},
    )


@pytest.fixture
def grid():
    return RectilinearGrid(
        "EPSG:32618", 10, [359400, 4300740, 369400, 4310740], [250, 250],
        config=default_config("atl06_polar"),
    )


@pytest.fixture
def catalog():
    # West-half, east-half, and a small NE granule over SERC.
    return _catalog([
        _item("Gwest", -76.62, -76.57),
        _item("Geast", -76.55, -76.50),
        _item("GneSmall", -76.55, -76.52, 38.91, 38.93),
    ])


def _granule_shards(sm):
    """Map granule id -> set of shard keys it appears in."""
    out: dict = {}
    for k, g in zip(sm.shard_keys, sm.granules):
        for rec in g:
            out.setdefault(rec["id"], set()).add(k)
    return out


class TestBuildShapely:
    def test_spatial_split(self, catalog, grid):
        sm = ShardMap.build(catalog, grid, backend="shapely")
        gs = _granule_shards(sm)
        # 4x4 chunk grid: col block = shard % 4. West granule only in col 0-1.
        assert gs["Gwest"], "west granule should hit some shards"
        assert all(k % 4 in (0, 1) for k in gs["Gwest"])
        assert all(k % 4 in (2, 3) for k in gs["Geast"])

    def test_option_c_self_contained(self, catalog, grid):
        sm = ShardMap.build(catalog, grid, backend="shapely")
        for g in sm.granules:
            for rec in g:
                assert rec["s3"] and rec["https"]
                assert set(rec) == {"id", "s3", "https"}

    def test_signature_recorded(self, catalog, grid):
        sm = ShardMap.build(catalog, grid, backend="shapely")
        assert sm.grid_signature == grid.signature()

    def test_metadata(self, catalog, grid):
        sm = ShardMap.build(catalog, grid, backend="shapely")
        assert sm.metadata["backend"] == "shapely"
        assert sm.metadata["total_pairs"] == sum(len(g) for g in sm.granules)
        assert sm.metadata["total_granules"] == 3


def _has_spatial_index():
    try:
        import spherely

        return hasattr(spherely, "SpatialIndex")
    except ImportError:
        return False


@pytest.mark.skipif(not _has_spatial_index(),
                    reason="spherely SpatialIndex (fork build) not installed")
class TestBuildSpherely:
    def test_matches_shapely(self, catalog, grid):
        # Exact S2 and WGS84 STRtree agree on a local non-polar grid.
        sph = ShardMap.build(catalog, grid, backend="spherely")
        shp = ShardMap.build(catalog, grid, backend="shapely")
        assert _granule_shards(sph) == _granule_shards(shp)
        assert sph.metadata["backend"] == "spherely"


class TestResolveBackend:
    def test_auto_rectilinear_without_spherely(self, grid):
        # No SpatialIndex spherely in the venv -> rectilinear falls to shapely.
        assert _resolve_backend("auto", grid) in ("spherely", "shapely")

    def test_auto_healpix_without_spherely(self):
        hp = HealpixGrid(6, 12, layout="fullsphere")
        assert _resolve_backend("auto", hp) in ("spherely", "mortie")

    def test_explicit_passthrough(self, grid):
        assert _resolve_backend("shapely", grid) == "shapely"

    def test_unknown_backend_raises(self, catalog, grid):
        with pytest.raises(ValueError, match="unknown backend"):
            ShardMap.build(catalog, grid, backend="nope")


class TestIO:
    def test_round_trip(self, catalog, grid):
        sm = ShardMap.build(catalog, grid, backend="shapely")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            sm.to_json(f.name)
            sm2 = ShardMap.from_json(f.name)
        assert sm2.shard_keys == sm.shard_keys
        assert sm2.granules == sm.granules
        assert sm2.grid_signature == sm.grid_signature

    def test_from_json_missing_key(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"shard_keys": [], "granules": []}, f)
            path = f.name
        with pytest.raises(ValueError, match="missing required key"):
            ShardMap.from_json(path)
