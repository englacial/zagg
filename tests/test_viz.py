"""Tests for the headless shard-map render core (issue #38, phase 1).

Pure Python -- no ipyleaflet / widget stack. Exercises GeoJSON emission off a
small saved ShardMap fixture, the catalog footprint layer, the viewport
grid-on-zoom gate, and antimeridian splitting.
"""

import json

import numpy as np
import pyarrow as pa
import pytest
import stac_geoparquet.arrow as sga

from zagg.catalog.shardmap import ShardMap
from zagg.catalog.sources import Catalog
from zagg.grids import HealpixGrid, RectilinearGrid
from zagg.viz import (
    granule_footprints,
    grid_from_signature,
    render_shardmap,
    shard_outlines,
    viewport_cells,
)
from zagg.viz.shardmap import _is_geojson, _split_antimeridian

# ── fixtures ─────────────────────────────────────────────────────────────────

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


@pytest.fixture
def rect_grid():
    return RectilinearGrid(
        "EPSG:32618", 10, [359400, 4300740, 369400, 4310740], [250, 250]
    )


@pytest.fixture
def shardmap(rect_grid):
    """A tiny hand-built ShardMap over a 4x4 chunk grid (no fetch needed)."""
    keys = [0, 1, 5]
    granules = [
        [{"id": "Ga", "s3": "s3://b/a.h5", "https": "https://h/a.h5"}],
        [{"id": "Gb", "s3": "s3://b/b.h5", "https": "https://h/b.h5"}],
        [
            {"id": "Gb", "s3": "s3://b/b.h5", "https": "https://h/b.h5"},
            {"id": "Gc", "s3": "s3://b/c.h5", "https": "https://h/c.h5"},
        ],
    ]
    return ShardMap(rect_grid.signature(), keys, granules, {"backend": "test"})


@pytest.fixture
def catalog():
    items = [_item("Ga", -76.62, -76.57), _item("Gb", -76.55, -76.50)]
    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "TEST"},
    )


# ── grid_from_signature ──────────────────────────────────────────────────────

class TestGridFromSignature:
    def test_rectilinear_round_trip(self, rect_grid):
        g = grid_from_signature(rect_grid.signature())
        assert g.signature() == rect_grid.signature()
        assert g.shard_footprint(0).equals(rect_grid.shard_footprint(0))

    def test_healpix_round_trip(self):
        hp = HealpixGrid(3, 7, layout="fullsphere")
        g = grid_from_signature(hp.signature())
        assert g.signature() == hp.signature()

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="unknown grid signature type"):
            grid_from_signature({"type": "mystery"})


# ── shard_outlines ───────────────────────────────────────────────────────────

class TestShardOutlines:
    def test_one_feature_per_shard(self, shardmap):
        fc = shard_outlines(shardmap)
        assert _is_geojson(fc)
        assert len(fc["features"]) == len(shardmap.shard_keys)

    def test_properties_and_geometry(self, shardmap):
        fc = shard_outlines(shardmap)
        feat = fc["features"][0]
        assert feat["properties"]["shard_key"] == 0
        assert feat["properties"]["n_granules"] == 1
        assert feat["geometry"]["type"] in ("Polygon", "MultiPolygon")

    def test_wgs84_coords(self, shardmap):
        # UTM 18N grid near 38.9N, -76.6E -> lons in [-77, -76], lats ~[38.8, 39].
        fc = shard_outlines(shardmap)
        ring = fc["features"][0]["geometry"]["coordinates"][0]
        lons = [pt[0] for pt in ring]
        lats = [pt[1] for pt in ring]
        assert all(-78 < lon < -75 for lon in lons)
        assert all(38 < lat < 40 for lat in lats)

    def test_valid_json(self, shardmap):
        fc = shard_outlines(shardmap)
        assert json.loads(json.dumps(fc)) == fc


# ── granule_footprints ───────────────────────────────────────────────────────

class TestGranuleFootprints:
    def test_one_feature_per_granule(self, catalog):
        fc = granule_footprints(catalog)
        assert _is_geojson(fc)
        assert len(fc["features"]) == 2
        ids = {f["properties"]["id"] for f in fc["features"]}
        assert ids == {"Ga", "Gb"}

    def test_geometry_is_polygon(self, catalog):
        fc = granule_footprints(catalog)
        assert fc["features"][0]["geometry"]["type"] == "Polygon"


# ── viewport_cells (grid-on-zoom gate) ───────────────────────────────────────

class TestViewportCells:
    def test_gate_open_few_shards(self, shardmap):
        # Tight viewport over one chunk -> <= max_shards visible, grid drawn.
        fp = grid_from_signature(shardmap.grid_signature).shard_footprint(0)
        lon0, lat0, lon1, lat1 = fp.bounds
        fc = viewport_cells(shardmap, (lon0, lat0, lon1, lat1), max_shards=4)
        assert _is_geojson(fc)
        assert len(fc["features"]) >= 1

    def test_gate_closed_too_many_shards(self, shardmap):
        # Global viewport over all shards but max_shards=1 -> gated empty.
        fc = viewport_cells(shardmap, (-180, -90, 180, 90), max_shards=1)
        assert fc["features"] == []

    def test_gate_closed_no_shards(self, shardmap):
        # Viewport far from the grid -> nothing visible -> empty.
        fc = viewport_cells(shardmap, (10, 10, 11, 11), max_shards=4)
        assert fc["features"] == []

    def test_clipped_to_viewport(self, shardmap):
        grid = grid_from_signature(shardmap.grid_signature)
        fp = grid.shard_footprint(0)
        lon0, lat0, lon1, lat1 = fp.bounds
        # Half-width viewport -> clipped cell stays within the viewport bbox.
        view = (lon0, lat0, (lon0 + lon1) / 2, lat1)
        fc = viewport_cells(shardmap, view, max_shards=4)
        for feat in fc["features"]:
            for ring in feat["geometry"]["coordinates"]:
                for lon, lat in ring:
                    assert view[0] - 1e-6 <= lon <= view[2] + 1e-6


# ── antimeridian splitting ───────────────────────────────────────────────────

class TestAntimeridian:
    def test_healpix_shard_near_antimeridian_splits(self):
        # A HEALPix parent cell straddling +-180 -> MultiPolygon, each part in
        # one hemisphere (no globe-spanning band).
        grid = HealpixGrid(2, 6, layout="fullsphere")
        key = int(
            grid.coverage(
                [(np.array([0.0, 1, 1, 0, 0]), np.array([179.0, 179, 180, 180, 179]))]
            )[0]
        )
        sm = ShardMap(
            grid.signature(), [key],
            [[{"id": "G", "s3": "s", "https": "h"}]], {},
        )
        fc = shard_outlines(sm)
        geom = fc["features"][0]["geometry"]
        assert geom["type"] == "MultiPolygon"
        for poly in geom["coordinates"]:
            lons = [pt[0] for pt in poly[0]]
            assert max(lons) - min(lons) <= 180.0

    def test_non_crossing_polygon_stays_polygon(self):
        from shapely.geometry import Polygon

        poly = Polygon([(10, 0), (12, 0), (12, 2), (10, 2), (10, 0)])
        geom = _split_antimeridian(poly)
        assert geom["type"] == "Polygon"

    def test_wide_non_crossing_polygon_kept_intact(self):
        # A swath that steps continuously from -170 across 0 to +170 spans >180
        # in total but never jumps the seam between consecutive vertices; it
        # must stay a single Polygon, not get split into ±180 slivers (review of
        # #38 phase 1 -- the old total-span gate over-split this).
        from shapely.geometry import Polygon

        ring = [
            (-170, -10), (-85, -10), (0, -10), (85, -10), (170, -10),
            (170, 10), (85, 10), (0, 10), (-85, 10), (-170, 10), (-170, -10),
        ]
        geom = _split_antimeridian(Polygon(ring))
        assert geom["type"] == "Polygon"
        lons = [pt[0] for pt in geom["coordinates"][0]]
        assert min(lons) == -170 and max(lons) == 170

    def test_holes_preserved(self):
        # An exterior ring with an interior hole keeps the hole through GeoJSON.
        from shapely.geometry import Polygon

        shell = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        hole = [(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]
        geom = _split_antimeridian(Polygon(shell, [hole]))
        assert geom["type"] == "Polygon"
        assert len(geom["coordinates"]) == 2  # exterior + one interior ring


# ── render_shardmap assembly ─────────────────────────────────────────────────

class TestRenderShardmap:
    def test_shards_only(self, shardmap):
        out = render_shardmap(shardmap)
        assert _is_geojson(out["shards"])
        assert out["granules"] is None
        assert out["cells"] is None

    def test_with_catalog_and_bbox(self, shardmap, catalog):
        out = render_shardmap(shardmap, catalog, bbox=(-180, -90, 180, 90))
        assert _is_geojson(out["shards"])
        assert _is_geojson(out["granules"])
        assert _is_geojson(out["cells"])

    def test_from_json_path(self, shardmap, tmp_path):
        path = tmp_path / "sm.json"
        shardmap.to_json(str(path))
        out = render_shardmap(str(path))
        assert len(out["shards"]["features"]) == len(shardmap.shard_keys)

    def test_from_geoparquet_path(self, shardmap, catalog, tmp_path):
        cat_path = tmp_path / "cat.parquet"
        catalog.to_geoparquet(str(cat_path))
        out = render_shardmap(shardmap, str(cat_path))
        assert len(out["granules"]["features"]) == 2


# ── phase 2: ipyleaflet wrapper (skips when the viz extra isn't installed) ────

class TestShowShardmap:
    def test_import_core_without_ipyleaflet(self):
        # The headless core and zagg.viz import must not require ipyleaflet.
        import zagg.viz  # noqa: F401

        assert hasattr(zagg.viz, "shard_outlines")

    def test_build_map(self, shardmap, tmp_path):
        pytest.importorskip("ipyleaflet")
        from ipyleaflet import Map

        from zagg.viz import show_shardmap

        path = tmp_path / "sm.json"
        shardmap.to_json(str(path))
        m = show_shardmap(str(path))
        assert isinstance(m, Map)
        # shard layer + grid layer (+ basemap) present.
        assert len(m.layers) >= 2

    def test_build_map_with_catalog(self, shardmap, catalog, tmp_path):
        pytest.importorskip("ipyleaflet")
        from ipyleaflet import Map

        from zagg.viz import show_shardmap

        sm_path = tmp_path / "sm.json"
        cat_path = tmp_path / "cat.parquet"
        shardmap.to_json(str(sm_path))
        catalog.to_geoparquet(str(cat_path))
        m = show_shardmap(str(sm_path), catalog=str(cat_path))
        assert isinstance(m, Map)
        names = {getattr(layer, "name", None) for layer in m.layers}
        assert "granule footprints" in names
