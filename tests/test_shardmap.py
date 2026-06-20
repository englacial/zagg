"""Tests for ShardMap building (spherely + mortie backends; IO; resolution).

The real spherely (SpatialIndex) backend is exercised separately in the conda
sidecar env since its build isn't in the default venv. Here the spherely
*brute* path (elementwise ``spherely.intersects``, no SpatialIndex) and the
absent-spherely error are exercised with a lightweight fake spherely module so
they run in the default venv (#36).
"""

import json
import sys
import tempfile
import types

import numpy as np
import pyarrow as pa
import pytest
import stac_geoparquet.arrow as sga

from zagg.catalog import shardmap
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


# ── fake spherely (brute path) ───────────────────────────────────────────────
#
# A minimal stand-in for the *stock* (no-SpatialIndex) spherely build: polygons
# are reduced to their planar lon/lat bounding box and ``intersects`` is an AABB
# overlap test. On this local, non-polar grid that matches exact S2, so it lets
# the real ``_intersect_spherely`` brute branch run end-to-end. It deliberately
# omits ``SpatialIndex`` to force ``hasattr(spherely, "SpatialIndex")`` False.

class _FakePoly:
    def __init__(self, lons, lats):
        self.x0, self.x1 = float(min(lons)), float(max(lons))
        self.y0, self.y1 = float(min(lats)), float(max(lats))

    def _overlaps(self, other):
        return (self.x0 <= other.x1 and other.x0 <= self.x1
                and self.y0 <= other.y1 and other.y0 <= self.y1)


def _fake_create_polygon(*, shell, oriented):  # noqa: ARG001 (mirror real sig)
    lons = [pt[0] for pt in shell]
    lats = [pt[1] for pt in shell]
    return _FakePoly(lons, lats)


def _fake_intersects(a, b):
    arr = np.atleast_1d(np.asarray(a, dtype=object))
    return np.array([p._overlaps(b) for p in arr], dtype=bool)


@pytest.fixture
def fake_spherely(monkeypatch):
    """Install a brute-only fake spherely module (no SpatialIndex)."""
    mod = types.ModuleType("spherely")
    mod.create_polygon = _fake_create_polygon
    mod.intersects = _fake_intersects
    monkeypatch.setitem(sys.modules, "spherely", mod)
    return mod


class TestBuildSpherelyBrute:
    """The brute (no-SpatialIndex) spherely path via a fake spherely module."""

    def test_no_spatial_index(self, fake_spherely):
        # Sanity: the fake forces the brute branch.
        assert not hasattr(fake_spherely, "SpatialIndex")

    def test_spatial_split(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        gs = _granule_shards(sm)
        # 4x4 chunk grid: col block = shard % 4. West granule only in col 0-1.
        assert gs["Gwest"], "west granule should hit some shards"
        assert all(k % 4 in (0, 1) for k in gs["Gwest"])
        assert all(k % 4 in (2, 3) for k in gs["Geast"])

    def test_option_c_self_contained(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        for g in sm.granules:
            for rec in g:
                assert rec["s3"] and rec["https"]
                assert set(rec) == {"id", "s3", "https"}

    def test_signature_recorded(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert sm.grid_signature == grid.signature()

    def test_metadata(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert sm.metadata["backend"] == "spherely"
        assert sm.metadata["total_pairs"] == sum(len(g) for g in sm.granules)
        assert sm.metadata["total_granules"] == 3

    def test_brute_empty_records_early_out(self, grid, fake_spherely):
        # No records -> no polygons -> {} early-out, no intersect call (#36 brute path).
        from zagg.catalog.shardmap import _intersect_spherely
        assert _intersect_spherely([], grid, {}) == {}


class TestSpherelyAbsent:
    """When spherely is genuinely absent, the backend raises with a pointer."""

    @pytest.fixture
    def no_spherely(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "spherely", None)

    def test_explicit_spherely_raises(self, catalog, grid, no_spherely):
        with pytest.raises(ImportError, match="spherely is required"):
            ShardMap.build(catalog, grid, backend="spherely")

    def test_auto_rectilinear_raises(self, catalog, grid, no_spherely):
        # Non-HEALPix auto resolves to spherely, which then raises loudly --
        # there is no shapely fallback anymore (#36).
        assert _resolve_backend("auto", grid) == "spherely"
        with pytest.raises(ImportError, match="README"):
            ShardMap.build(catalog, grid, backend="auto")


def _has_spatial_index():
    try:
        import spherely

        return hasattr(spherely, "SpatialIndex")
    except ImportError:
        return False


@pytest.mark.skipif(not _has_spatial_index(),
                    reason="spherely SpatialIndex (fork build) not installed")
class TestBuildSpherely:
    def test_spatial_split(self, catalog, grid):
        # Exact S2 with SpatialIndex gives the expected local split.
        sm = ShardMap.build(catalog, grid, backend="spherely")
        gs = _granule_shards(sm)
        assert gs["Gwest"]
        assert all(k % 4 in (0, 1) for k in gs["Gwest"])
        assert all(k % 4 in (2, 3) for k in gs["Geast"])
        assert sm.metadata["backend"] == "spherely"


class TestResolveBackend:
    def test_auto_rectilinear_uses_spherely(self, grid, fake_spherely):
        assert _resolve_backend("auto", grid) == "spherely"

    def test_auto_healpix_without_spherely(self, monkeypatch):
        # No spherely -> HEALPix auto falls to its native mortie MOC path.
        monkeypatch.setitem(sys.modules, "spherely", None)
        hp = HealpixGrid(6, 12, layout="fullsphere")
        assert _resolve_backend("auto", hp) == "mortie"

    def test_auto_healpix_prefers_spherely(self, fake_spherely):
        hp = HealpixGrid(6, 12, layout="fullsphere")
        assert _resolve_backend("auto", hp) == "spherely"

    def test_explicit_passthrough(self, grid):
        assert _resolve_backend("mortie", grid) == "mortie"

    def test_shapely_no_longer_a_backend(self):
        # shapely was removed as an intersection backend (#36).
        assert "shapely" not in shardmap._BACKENDS

    def test_unknown_backend_raises(self, catalog, grid):
        with pytest.raises(ValueError, match="unknown backend"):
            ShardMap.build(catalog, grid, backend="nope")

    def test_cli_rejects_shapely_backend(self, monkeypatch):
        # shapely was dropped as a backend (#36); the CLI must not accept it.
        from zagg.catalog import main
        monkeypatch.setattr(
            sys, "argv",
            ["zagg-catalog", "--config", "x.yaml", "--short-name", "ATL03",
             "--backend", "shapely"],
        )
        with pytest.raises(SystemExit):
            main()

    def test_cli_rejects_bad_footprint(self, monkeypatch):
        from zagg.catalog import main
        monkeypatch.setattr(
            sys, "argv",
            ["zagg-catalog", "--config", "x.yaml", "--short-name", "ATL03",
             "--footprint", "garbage"],
        )
        with pytest.raises(SystemExit):
            main()


# ── beam-corridor footprints (issue #65) ─────────────────────────────────────

from zagg.catalog.beams import beam_tracks_from_cmr_polygon  # noqa: E402

# Real RGT0568 cycle-29 CMR footprint polygon (lon, lat), captured from CMR.
# The granule's measured beam ground-tracks at lat 38.89 are gt1l -76.5475,
# gt2l -76.5106, gt3l -76.4737 -- the decomposition must place a corridor over
# each (issue #65 validation target).
_C29_POLY = [
    (-79.4552, 59.5458), (-79.6776, 59.5342), (-79.5274, 58.7894), (-79.1270, 56.6847),
    (-79.0193, 55.9820), (-78.9553, 55.3096), (-78.5002, 52.5857), (-78.1682, 50.4866),
    (-77.4919, 45.8442), (-76.9446, 41.7520), (-76.4355, 37.6827), (-75.9327, 33.4550),
    (-75.3154, 28.0089), (-75.1996, 26.9469), (-75.0726, 26.9579), (-75.1873, 28.0199),
    (-75.7972, 33.4664), (-76.2927, 37.6939), (-76.7931, 41.7632), (-77.3297, 45.8554),
    (-77.9907, 50.4980), (-78.3143, 52.5970), (-78.7568, 55.3209), (-78.8168, 55.9880),
    (-78.9211, 56.6943), (-79.3096, 58.8011), (-79.4552, 59.5458),
]
_C29_MEASURED = {0: -76.5475, 1: -76.5106, 2: -76.4737}  # pair index -> beam lon @ 38.89


def _swath_latlon(center_lon, center_lat, half_width_deg=0.073, half_height_deg=0.15, n=12):
    """Densified N-S swath polygon ring as (lats, lons) -- down west edge, up east.

    Tall (along-track) >> wide (cross-track), as real quarter-orbit swaths are,
    so the principal axis is the N-S track direction.
    """
    lats_col = np.linspace(center_lat - half_height_deg, center_lat + half_height_deg, n)
    w = center_lon - half_width_deg
    e = center_lon + half_width_deg
    lons = np.concatenate([np.full(n, w), np.full(n, e)[::-1], [w]])
    lats = np.concatenate([lats_col, lats_col[::-1], [lats_col[0]]])
    return lats, lons


def _swath_item(gid, center_lon, center_lat, half_width_deg=0.073, n=12):
    lats, lons = _swath_latlon(center_lon, center_lat, half_width_deg=half_width_deg, n=n)
    ring = [[float(lo), float(la)] for lo, la in zip(lons, lats)]
    return {
        "type": "Feature", "stac_version": "1.0.0", "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())],
        "properties": {"datetime": "2025-06-01T00:00:00Z"},
        "collection": "ATL03_007", "stac_extensions": [], "links": [],
        "assets": {
            "data": {"href": f"https://h/{gid}.h5", "roles": ["data"]},
            "data_s3": {"href": f"s3://b/{gid}.h5", "roles": ["data"]},
        },
    }


def _atl03_catalog(items):
    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "ATL03_007", "bbox": [-76.62107, 38.84504, -76.50583, 38.93512]},
    )


def _fine_grid():
    # 10 km AOI at 10 m, 50-cell (500 m) shards -> 20x20, fine enough that the
    # ~3 km inter-pair gaps contain whole shards.
    return RectilinearGrid(
        "EPSG:32618", 10, [359400, 4300740, 369400, 4310740], [50, 50],
        config=default_config("atl06_polar"),
    )


class TestBeamHelper:
    """Pure-geometry decomposition (pyproj + numpy only)."""

    def test_c29_corridors_contain_measured_beams(self):
        from shapely.geometry import Point, Polygon

        lons = np.array([v[0] for v in _C29_POLY])
        lats = np.array([v[1] for v in _C29_POLY])
        rings = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(rings) == 3
        for k, (rlat, rlon) in enumerate(rings):
            corridor = Polygon(zip(rlon, rlat))
            beam = Point(_C29_MEASURED[k], 38.89)
            assert corridor.contains(beam), f"pair {k} corridor missed its measured beam"

    def test_synthetic_straight_swath_offsets(self):
        from shapely.geometry import LineString, Polygon

        lats, lons = _swath_latlon(-76.50, 38.89, n=12)
        rings = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(rings) == 3
        # corridor centers at lat 38.89 should sit at -3.3 / 0 / +3.3 km cross-track
        deg_per_m = 1.0 / (np.cos(np.radians(38.89)) * 111320.0)
        expected = [-76.50 + d * 3300 * deg_per_m for d in (-1, 0, 1)]
        for k, (rlat, rlon) in enumerate(rings):
            sl = Polygon(zip(rlon, rlat)).intersection(LineString([(-78, 38.89), (-75, 38.89)]))
            xs = [c[0] for g in (sl.geoms if hasattr(sl, "geoms") else [sl]) for c in g.coords]
            center = 0.5 * (min(xs) + max(xs))
            assert abs(center - expected[k]) < 0.003  # ~260 m

    def test_non_beam_product_passthrough(self):
        lats = np.array([v[1] for v in _C29_POLY])
        lons = np.array([v[0] for v in _C29_POLY])
        out = beam_tracks_from_cmr_polygon(lats, lons, product="ATL08")
        assert len(out) == 1
        np.testing.assert_array_equal(out[0][0], lats)
        np.testing.assert_array_equal(out[0][1], lons)

    def test_degenerate_few_vertices_falls_back(self):
        lats = np.array([38.85, 38.85, 38.93, 38.85])
        lons = np.array([-76.6, -76.5, -76.55, -76.6])
        out = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(out) == 1  # too few vertices -> swath fallback, granule kept

    def test_antimeridian_falls_back(self):
        # Swath straddling +/-180 (wrapped lons, ptp ~360) can't be a simple
        # corridor ring -> swath fallback (granule kept, just not tightened).
        col = np.linspace(64.85, 65.15, 10)
        lons = np.concatenate([np.full(10, 179.9), np.full(10, -179.9), [179.9]])
        lats = np.concatenate([col, col[::-1], [col[0]]])
        out = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(out) == 1


class TestBeamFootprintBehavior:
    """Beam mode assigns fewer shards than swath -- proven in both backends."""

    def _granule_shard_set(self, sm, gid):
        return _granule_shards(sm).get(gid, set())

    def test_beam_mode_fewer_shards_spherely(self, fake_spherely):
        grid = _fine_grid()
        cat = _atl03_catalog([_swath_item("G", -76.50, 38.89)])
        swath = ShardMap.build(cat, grid, backend="spherely", footprint="swath")
        beams = ShardMap.build(cat, grid, backend="spherely", footprint="beams")
        sw, bm = self._granule_shard_set(swath, "G"), self._granule_shard_set(beams, "G")
        assert bm, "granule must still be assigned in beam mode"
        assert bm < sw, "beam corridors must hit strictly fewer shards than the swath"

    def test_beam_mode_fewer_shards_mortie(self):
        grid = _fine_grid()
        cat = _atl03_catalog([_swath_item("G", -76.50, 38.89)])
        swath = ShardMap.build(cat, grid, backend="mortie", mortie_order=14, footprint="swath")
        beams = ShardMap.build(cat, grid, backend="mortie", mortie_order=14, footprint="beams")
        sw, bm = self._granule_shard_set(swath, "G"), self._granule_shard_set(beams, "G")
        assert bm
        assert bm < sw

    def test_beam_mode_fewer_shards_healpix(self):
        # HEALPix grid -> the is_healpix mortie MOC sub-path + per-granule dedup.
        hp = HealpixGrid(12, 14, layout="fullsphere")
        cat = _atl03_catalog([_swath_item("G", -76.50, 38.89)])
        region = [(np.array([38.74, 38.74, 39.04, 39.04, 38.74]),
                   np.array([-76.62, -76.42, -76.42, -76.62, -76.62]))]
        swath = ShardMap.build(cat, hp, region=region, backend="mortie",
                               mortie_order=14, footprint="swath")
        beams = ShardMap.build(cat, hp, region=region, backend="mortie",
                               mortie_order=14, footprint="beams")
        sw, bm = self._granule_shard_set(swath, "G"), self._granule_shard_set(beams, "G")
        assert bm
        assert bm < sw

    def test_beam_metadata(self, fake_spherely):
        grid = _fine_grid()
        cat = _atl03_catalog([_swath_item("G", -76.50, 38.89)])
        sm = ShardMap.build(cat, grid, backend="spherely", footprint="beams")
        assert sm.metadata["footprint"] == "beams"

    def test_swath_is_the_default(self, catalog, grid, fake_spherely):
        # Default build == explicit swath build (non-breaking).
        default = ShardMap.build(catalog, grid, backend="spherely")
        swath = ShardMap.build(catalog, grid, backend="spherely", footprint="swath")
        assert default.metadata["footprint"] == "swath"
        assert _granule_shards(default) == _granule_shards(swath)

    def test_invalid_footprint_raises(self, catalog, grid, fake_spherely):
        with pytest.raises(ValueError, match="footprint must be"):
            ShardMap.build(catalog, grid, backend="spherely", footprint="nope")


class TestIO:
    def test_round_trip(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
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
