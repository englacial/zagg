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
from zagg.viz.crs import crs_info, is_polar, pick_crs, shardmap_bbox
from zagg.viz.shardmap import _is_geojson, _split_antimeridian

# ── fixtures ─────────────────────────────────────────────────────────────────


def _item(gid, lon0, lon1, lat0=38.85, lat1=38.93):
    ring = [[lon0, lat0], [lon1, lat0], [lon1, lat1], [lon0, lat1], [lon0, lat0]]
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [lon0, lat0, lon1, lat1],
        "properties": {"datetime": "2025-06-01T00:00:00Z"},
        "collection": "TEST",
        "stac_extensions": [],
        "links": [],
        "assets": {
            "data": {"href": f"https://h/{gid}.h5", "roles": ["data"]},
            "data_s3": {"href": f"s3://b/{gid}.h5", "roles": ["data"]},
        },
    }


@pytest.fixture
def rect_grid():
    return RectilinearGrid("EPSG:32618", 10, [359400, 4300740, 369400, 4310740], [250, 250])


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


@pytest.fixture
def healpix_shardmap():
    """A HEALPix shardmap (parent_order=6, child_order=12) over a tight AOI.

    Child cells subdivide each order-6 shard 4-for-1 per order step
    (``4^(12-6)`` per shard), so the grid-on-zoom must show those nested child
    cells -- not the shard outline redrawn.
    """
    g = HealpixGrid(parent_order=6, child_order=12, layout="fullsphere")
    lats = np.array([10.0, 10.5, 10.5, 10.0, 10.0])
    lons = np.array([20.0, 20.0, 20.5, 20.5, 20.0])
    keys = [int(k) for k in g.coverage([(lats, lons)])]
    granules = [[{"id": "G", "s3": "s", "https": "h"}] for _ in keys]
    return ShardMap(g.signature(), keys, granules, {"backend": "test"})


@pytest.fixture
def antarctic_shardmap():
    """ShardMap on an EPSG:3031 grid -> footprints entirely south of -60 deg."""
    g = RectilinearGrid("EPSG:3031", 100000, [-1000000, -1000000, 0, 0], [5, 5])
    return ShardMap(g.signature(), [0, 1], [[], []], {"backend": "test"})


@pytest.fixture
def arctic_shardmap():
    """ShardMap on an EPSG:3413 grid -> footprints entirely north of +60 deg."""
    g = RectilinearGrid("EPSG:3413", 100000, [-1000000, -1000000, 0, 0], [5, 5])
    return ShardMap(g.signature(), [0, 1], [[], []], {"backend": "test"})


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

    def test_footprints_built_once_across_queries(self, shardmap, monkeypatch):
        # Regression for the grid-on-zoom hang (PR #44): footprints must be
        # generated once for the STRtree index and never again per query. Wrap
        # the grid's shard_footprint with a call counter and run many viewports.
        from zagg.grids.rectilinear import RectilinearGrid
        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()  # fresh index for this shardmap
        calls = {"n": 0}
        orig = RectilinearGrid.shard_footprint

        def counting(self, key):
            calls["n"] += 1
            return orig(self, key)

        monkeypatch.setattr(RectilinearGrid, "shard_footprint", counting)

        full = (-180, -90, 180, 90)
        for _ in range(10):
            viewport_cells(shardmap, full, max_shards=10)
        # Exactly one footprint build per shard for index construction; the ten
        # queries add nothing.
        assert calls["n"] == len(shardmap.shard_keys)

    def test_prebuilt_index_reused(self, shardmap, monkeypatch):
        # Passing an explicit index never touches shard_footprint at query time.
        from zagg.grids.rectilinear import RectilinearGrid
        from zagg.viz.shardmap import ShardIndex

        index = ShardIndex(shardmap)

        def boom(self, key):  # pragma: no cover - must not be called
            raise AssertionError("shard_footprint rebuilt during query")

        monkeypatch.setattr(RectilinearGrid, "shard_footprint", boom)
        fc = viewport_cells(shardmap, (-180, -90, 180, 90), max_shards=10, index=index)
        assert _is_geojson(fc)

    def test_index_query_matches_exhaustive_scan(self, shardmap):
        # The STRtree-backed visible set equals the old exhaustive intersects
        # scan, for both a tight and a wide bbox.
        from shapely.geometry import box

        from zagg.viz.shardmap import grid_from_signature, shard_index

        grid = grid_from_signature(shardmap.grid_signature)
        idx = shard_index(shardmap)
        for bbox in ((-180, -90, 180, 90), grid.shard_footprint(0).bounds):
            view = box(*bbox)
            indexed = {k for k, _ in idx.query(view)}
            exhaustive = {
                k for k in shardmap.shard_keys if grid.shard_footprint(k).intersects(view)
            }
            assert indexed == exhaustive


# ── viewport_cells: HEALPix child-cell nesting + viewport bound ──────────────


class TestViewportCellsHealpix:
    def _tight_view(self, grid, key, frac=0.3):
        """A viewport centered in shard ``key``, ``frac`` of its bbox each side."""
        lon0, lat0, lon1, lat1 = grid.shard_footprint(key).bounds
        cx, cy = (lon0 + lon1) / 2, (lat0 + lat1) / 2
        w, h = (lon1 - lon0) * frac / 2, (lat1 - lat0) * frac / 2
        return (cx - w, cy - h, cx + w, cy + h)

    def test_emits_child_order_cells_not_shard_outline(self, healpix_shardmap):
        # The grid-on-zoom for HEALPix is the finer child-cell grid, so a single
        # visible shard must yield *many* child cells -- not one shard-clip
        # feature (the bug @espg reported: grid == shards, no nesting).
        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        grid = grid_from_signature(healpix_shardmap.grid_signature)
        key = healpix_shardmap.shard_keys[0]
        view = self._tight_view(grid, key, frac=0.3)
        fc = viewport_cells(healpix_shardmap, view, max_shards=4, max_cells=5000)
        assert _is_geojson(fc)
        assert len(fc["features"]) > 1  # not a lone shard-outline clip

    def test_cells_nest_within_parent_shard(self, healpix_shardmap):
        # Every emitted cell's parent (clip2order at parent_order) is one of the
        # visible shards, and the cell IDs are genuine order-child_order cells.
        from mortie import clip2order, infer_order_from_morton

        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        grid = grid_from_signature(healpix_shardmap.grid_signature)
        key = int(healpix_shardmap.shard_keys[0])
        view = self._tight_view(grid, key, frac=0.2)
        fc = viewport_cells(healpix_shardmap, view, max_shards=4, max_cells=5000)
        shard_keys = {int(k) for k in healpix_shardmap.shard_keys}
        assert fc["features"]
        for feat in fc["features"]:
            cell = feat["properties"]["cell"]
            parent = feat["properties"]["shard_key"]
            assert int(infer_order_from_morton(cell)) == grid.child_order
            assert int(clip2order(grid.parent_order, np.asarray([cell]))[0]) == parent
            assert parent in shard_keys

    def test_cell_union_tiles_visible_shard(self, healpix_shardmap):
        # A viewport covering exactly one whole shard: the union of the emitted
        # child cells must reconstruct that shard footprint (4-for-1 nesting),
        # confirming the grid lines up inside the shard outline.
        from shapely.ops import unary_union

        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        # Use a smaller level diff so a whole-shard view stays cheap.
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere")
        lats = np.array([10.0, 10.5, 10.5, 10.0, 10.0])
        lons = np.array([20.0, 20.0, 20.5, 20.5, 20.0])
        keys = [int(k) for k in g.coverage([(lats, lons)])]
        sm = ShardMap(g.signature(), keys, [[] for _ in keys], {})
        key = keys[0]
        shard_fp = g.shard_footprint(key)
        # The shard's bbox overlaps its 4 neighbors (a HEALPix diamond's bbox is
        # larger than the diamond), so allow them through the gate; cells are
        # then filtered to this shard via the parent check below.
        view = shard_fp.bounds
        fc = viewport_cells(sm, view, max_shards=8, max_cells=5000)
        cell_polys = [
            g.shard_footprint(f["properties"]["cell"])
            for f in fc["features"]
            if f["properties"]["shard_key"] == key
        ]
        union = unary_union(cell_polys)
        # Union of the shard's child cells reconstructs the shard footprint.
        assert union.intersection(shard_fp).area / shard_fp.area > 0.98

    def test_cell_count_scales_with_viewport_not_fan_out(self, healpix_shardmap):
        # Zooming in (smaller viewport) must emit *fewer* cells. A naive
        # 4^(child-parent) per-shard enumeration would emit a constant 4096/shard
        # regardless of zoom; viewport-bounded coverage shrinks with the view.
        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        grid = grid_from_signature(healpix_shardmap.grid_signature)
        key = healpix_shardmap.shard_keys[0]
        wide = viewport_cells(
            healpix_shardmap,
            self._tight_view(grid, key, 0.4),
            max_shards=4,
            max_cells=100000,
        )
        tight = viewport_cells(
            healpix_shardmap,
            self._tight_view(grid, key, 0.05),
            max_shards=4,
            max_cells=100000,
        )
        assert len(tight["features"]) < len(wide["features"])
        # And both are far below the full 4^(12-6)=4096 per-shard enumeration.
        assert len(wide["features"]) < grid.n_children

    def test_no_full_enumeration_on_zoom(self, healpix_shardmap, monkeypatch):
        # A zoomed-in query must not enumerate every child of a shard. Guard
        # generate_morton_children: the viewport path uses morton_coverage, so a
        # full child enumeration would be a regression.
        import mortie

        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()

        def boom(*a, **k):  # pragma: no cover - must not be called
            raise AssertionError("full child enumeration on a zoomed-in query")

        monkeypatch.setattr(mortie, "generate_morton_children", boom)
        grid = grid_from_signature(healpix_shardmap.grid_signature)
        key = healpix_shardmap.shard_keys[0]
        fc = viewport_cells(
            healpix_shardmap,
            self._tight_view(grid, key, 0.1),
            max_shards=4,
            max_cells=5000,
        )
        assert _is_geojson(fc)

    def test_max_cells_gate_returns_empty(self, healpix_shardmap):
        # A whole-shard view at child_order 12 is tens of thousands of cells;
        # the max_cells gate keeps a refresh bounded by returning empty rather
        # than emitting them all (the dense-viewport bound).
        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        grid = grid_from_signature(healpix_shardmap.grid_signature)
        key = healpix_shardmap.shard_keys[0]
        view = grid.shard_footprint(key).bounds
        fc = viewport_cells(healpix_shardmap, view, max_shards=4, max_cells=200)
        assert fc["features"] == []

    def test_child_cells_respect_antimeridian_seam(self):
        # Child cells straddling +-180 split into hemisphere-local parts (no
        # globe-spanning band) when split_seam=True, and stay single Polygons
        # under a polar CRS (split_seam=False) -- same seam handling as shards.
        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        g = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere")
        key = int(
            g.coverage([(np.array([0.0, 1, 1, 0, 0]), np.array([179.0, 179, 180, 180, 179]))])[0]
        )
        sm = ShardMap(g.signature(), [key], [[]], {})
        view = g.shard_footprint(key).bounds
        split = viewport_cells(sm, view, max_shards=8, max_cells=5000, split_seam=True)
        assert any(f["geometry"]["type"] == "MultiPolygon" for f in split["features"])
        for feat in split["features"]:
            geom = feat["geometry"]
            polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
            for poly in polys:
                lons = [pt[0] for pt in poly[0]]
                assert max(lons) - min(lons) <= 180.0  # no globe-spanning band
        unsplit = viewport_cells(sm, view, max_shards=8, max_cells=5000, split_seam=False)
        assert all(f["geometry"]["type"] == "Polygon" for f in unsplit["features"])

    def test_seam_cell_clipped_to_true_sliver_not_global_band(self):
        # Regression (PR #44 review): a child cell straddling +-180 must be split
        # at the seam *before* clipping. The unsplit cell's flat ring spans
        # ~360 deg; clipping that band to a seam-hugging viewport yields a wrong,
        # oversized geometry (or drops the cell). After the fix, every emitted
        # part is a small hemisphere-local sliver -- never a near-global band.
        from shapely.geometry import shape

        from zagg.viz import shardmap as sm_mod

        sm_mod._INDEX_CACHE.clear()
        g = HealpixGrid(parent_order=4, child_order=9, layout="fullsphere")
        key = int(
            g.coverage([(np.array([0.0, 1, 1, 0, 0]), np.array([179.0, 179, 180, 180, 179]))])[0]
        )
        sm = ShardMap(g.signature(), [key], [[]], {})
        # A narrow viewport hugging the seam at the shard's latitude band.
        lon0, lat0, lon1, lat1 = g.shard_footprint(key).bounds
        view = (179.0, lat0, 180.0, (lat0 + lat1) / 2)
        fc = viewport_cells(sm, view, max_shards=8, max_cells=20000, split_seam=True)
        assert fc["features"]  # the seam region is not silently emptied
        # An order-9 cell footprint is ~0.009 deg^2. Clipping the unsplit flat
        # band to the view instead fills the whole view strip (~0.19 deg^2, 20x
        # bigger) -- so a small per-cell area bound pins the seam-first fix.
        for feat in fc["features"]:
            assert shape(feat["geometry"]).area < 0.05  # true cell, not a band


# ── antimeridian splitting ───────────────────────────────────────────────────


class TestAntimeridian:
    def test_healpix_shard_near_antimeridian_splits(self):
        # A HEALPix parent cell straddling +-180 -> MultiPolygon, each part in
        # one hemisphere (no globe-spanning band).
        grid = HealpixGrid(2, 6, layout="fullsphere")
        key = int(
            grid.coverage([(np.array([0.0, 1, 1, 0, 0]), np.array([179.0, 179, 180, 180, 179]))])[0]
        )
        sm = ShardMap(
            grid.signature(),
            [key],
            [[{"id": "G", "s3": "s", "https": "h"}]],
            {},
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
            (-170, -10),
            (-85, -10),
            (0, -10),
            (85, -10),
            (170, -10),
            (170, 10),
            (85, 10),
            (0, 10),
            (-85, 10),
            (-170, 10),
            (-170, -10),
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


# ── phase C: CRS selection (headless, no browser) ────────────────────────────


class TestCrsSelection:
    def test_midlatitude_is_web_mercator(self, shardmap):
        # UTM 18N grid near 38.9N -> mid-latitude -> Web Mercator default.
        assert pick_crs(shardmap) == "EPSG:3857"

    def test_antarctic_is_3031(self, antarctic_shardmap):
        assert pick_crs(antarctic_shardmap) == "EPSG:3031"

    def test_arctic_is_3413(self, arctic_shardmap):
        assert pick_crs(arctic_shardmap) == "EPSG:3413"

    def test_explicit_override_wins(self, shardmap):
        # A mid-latitude map forced onto a polar CRS returns the override.
        assert pick_crs(shardmap, override="EPSG:3031") == "EPSG:3031"

    def test_bad_override_raises(self, shardmap):
        with pytest.raises(ValueError, match="unsupported crs override"):
            pick_crs(shardmap, override="EPSG:9999")

    def test_bbox_from_footprints(self, antarctic_shardmap):
        lon_min, lat_min, lon_max, lat_max = shardmap_bbox(antarctic_shardmap)
        assert lat_max <= -60.0
        assert lon_min >= -180.0 and lon_max <= 180.0

    def test_empty_map_bbox_raises(self, shardmap):
        empty = ShardMap(shardmap.grid_signature, [], [], {})
        with pytest.raises(ValueError, match="no shards"):
            shardmap_bbox(empty)


class TestCrsInfo:
    def test_web_mercator_has_no_projection_or_basemap(self):
        info = crs_info("EPSG:3857")
        assert info["projection"] is None
        assert info["basemap"] is None
        assert not is_polar("EPSG:3857")

    @pytest.mark.parametrize("crs", ["EPSG:3031", "EPSG:3413"])
    def test_polar_has_projection_and_gibs_basemap(self, crs):
        info = crs_info(crs)
        assert is_polar(crs)
        proj = info["projection"]
        assert proj["name"] == crs
        assert proj["proj4def"].startswith("+proj=stere")
        assert proj["origin"] and proj["bounds"] and proj["resolutions"]
        assert "gibs.earthdata.nasa.gov" in info["basemap"]["url"]
        assert f"epsg{crs.split(':')[1]}" in info["basemap"]["url"]

    def test_bad_crs_raises(self):
        with pytest.raises(ValueError, match="unsupported crs"):
            crs_info("EPSG:9999")


# ── phase C: CRS-aware antimeridian seam ─────────────────────────────────────


class TestSeamAwareLayers:
    def test_polar_skips_antimeridian_split(self):
        # A HEALPix cell straddling +-180 is a MultiPolygon under the Mercator
        # split, but stays a single Polygon when split_seam=False (polar CRS).
        grid = HealpixGrid(2, 6, layout="fullsphere")
        key = int(
            grid.coverage([(np.array([0.0, 1, 1, 0, 0]), np.array([179.0, 179, 180, 180, 179]))])[0]
        )
        sm = ShardMap(
            grid.signature(),
            [key],
            [[{"id": "G", "s3": "s", "https": "h"}]],
            {},
        )
        split = shard_outlines(sm)["features"][0]["geometry"]
        unsplit = shard_outlines(sm, split_seam=False)["features"][0]["geometry"]
        assert split["type"] == "MultiPolygon"
        assert unsplit["type"] == "Polygon"

    def test_granule_footprints_seam_flag(self):
        # A footprint straddling +-180 (ring jumps 178 -> -178): split_seam=True
        # cuts it into a MultiPolygon, but split_seam=False (polar CRS) keeps the
        # single ring -- so the flag, not the data, decides the geometry type.
        ring = [[178.0, 70.0], [-178.0, 70.0], [-178.0, 72.0], [178.0, 72.0], [178.0, 70.0]]
        item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "Gx",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "bbox": [-180.0, 70.0, 180.0, 72.0],
            "properties": {"datetime": "2025-06-01T00:00:00Z"},
            "collection": "TEST",
            "stac_extensions": [],
            "links": [],
            "assets": {"data": {"href": "https://h/Gx.h5", "roles": ["data"]}},
        }
        cat = Catalog(pa.table(sga.parse_stac_items_to_arrow([item])), {"collection": "TEST"})
        split = granule_footprints(cat)["features"][0]["geometry"]
        unsplit = granule_footprints(cat, split_seam=False)["features"][0]["geometry"]
        assert split["type"] == "MultiPolygon"
        assert unsplit["type"] == "Polygon"


# ── debounce wrapper (pure stdlib; no widget stack) ──────────────────────────


class TestDebounce:
    def test_rapid_calls_coalesce_to_one_on_loop(self):
        # Importing leaflet pulls only stdlib + zagg.viz at module level (the
        # ipyleaflet imports are local to the functions), so this is headless.
        # The debounce now schedules on the *running* event loop (the kernel's
        # main thread) rather than a background threading.Timer -- that is the
        # fix for the comm-thread crash (PR #44). Drive it on a loop and assert
        # the burst coalesces to one call, fired on the loop thread.
        import asyncio
        import threading

        from zagg.viz.leaflet import _debounce

        async def scenario():
            calls = {"n": 0, "tid": None}

            def hit():
                calls["n"] += 1
                calls["tid"] = threading.get_ident()

            deb = _debounce(0.05, hit)
            for _ in range(20):
                deb()  # each cancels and reschedules the pending loop callback
            assert calls["n"] == 0  # nothing fired yet (still within the window)
            await asyncio.sleep(0.2)
            return calls

        result = asyncio.run(scenario())
        assert result["n"] == 1  # the burst coalesced into a single refresh
        assert result["tid"] == threading.get_ident()  # ran on the loop's thread

    def test_no_loop_runs_synchronously(self):
        # With no running event loop (plain script / headless), there is no comm
        # to protect and no loop to schedule on, so the call runs inline.
        from zagg.viz.leaflet import _debounce

        calls = {"n": 0}
        deb = _debounce(0.05, lambda: calls.__setitem__("n", calls["n"] + 1))
        deb()
        assert calls["n"] == 1
        deb.cancel()


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
        # Debounced bounds observer is wired with a reachable cancel hook.
        assert callable(getattr(m, "cancel_grid_refresh", None))
        m.cancel_grid_refresh()

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

    def test_polar_map_uses_3031_crs_and_gibs(self, antarctic_shardmap, tmp_path):
        pytest.importorskip("ipyleaflet")
        from zagg.viz import show_shardmap

        path = tmp_path / "sm.json"
        antarctic_shardmap.to_json(str(path))
        m = show_shardmap(str(path))
        # proj4leaflet CRS for EPSG:3031 is wired onto the Map.
        assert m.crs["name"] == "EPSG:3031"
        # GIBS Antarctic tile basemap is present.
        urls = [getattr(layer, "url", "") for layer in m.layers]
        assert any("epsg3031" in u for u in urls)

    def test_midlatitude_map_stays_web_mercator(self, shardmap, tmp_path):
        pytest.importorskip("ipyleaflet")
        from zagg.viz import show_shardmap

        path = tmp_path / "sm.json"
        shardmap.to_json(str(path))
        m = show_shardmap(str(path))
        # No custom proj4leaflet CRS -> ipyleaflet's default Web Mercator.
        assert m.crs["name"] == "EPSG3857"
        assert not m.crs.get("custom", False)

    def test_crs_override(self, shardmap, tmp_path):
        pytest.importorskip("ipyleaflet")
        from zagg.viz import show_shardmap

        path = tmp_path / "sm.json"
        shardmap.to_json(str(path))
        m = show_shardmap(str(path), crs="EPSG:3413")
        assert m.crs["name"] == "EPSG:3413"
