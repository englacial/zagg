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


def _catalog(items):
    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "TEST", "bbox": [-76.62107, 38.84504, -76.50583, 38.93512]},
    )


@pytest.fixture
def grid():
    return RectilinearGrid(
        "EPSG:32618",
        10,
        [359400, 4300740, 369400, 4310740],
        [250, 250],
        config=default_config("atl06_polar"),
    )


@pytest.fixture
def catalog():
    # West-half, east-half, and a small NE granule over SERC.
    return _catalog(
        [
            _item("Gwest", -76.62, -76.57),
            _item("Geast", -76.55, -76.50),
            _item("GneSmall", -76.55, -76.52, 38.91, 38.93),
        ]
    )


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
        return (
            self.x0 <= other.x1
            and other.x0 <= self.x1
            and self.y0 <= other.y1
            and other.y0 <= self.y1
        )


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
        # The ShardMap stores the spatial signature only (#89) -- no
        # output_fields, so the map is reusable across aggregation configs.
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert sm.grid_signature == grid.spatial_signature()
        assert "output_fields" not in sm.grid_signature

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


@pytest.mark.skipif(
    not _has_spatial_index(), reason="spherely SpatialIndex (fork build) not installed"
)
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
            sys,
            "argv",
            ["zagg-catalog", "--config", "x.yaml", "--short-name", "ATL03", "--backend", "shapely"],
        )
        with pytest.raises(SystemExit):
            main()


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

    def test_round_trip_preserves_spatial_signature(self, catalog, grid, fake_spherely):
        # The stored signature is spatial-only and survives JSON round-trip (#89).
        sm = ShardMap.build(catalog, grid, backend="spherely")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            sm.to_json(f.name)
            sm2 = ShardMap.from_json(f.name)
        assert sm2.grid_signature == grid.spatial_signature()
        assert "output_fields" not in sm2.grid_signature


class TestSpatialSignature:
    """``spatial_signature()`` is the full signature minus ``output_fields`` (#89)."""

    def test_healpix_excludes_output_fields(self):
        g = HealpixGrid(6, 12, layout="fullsphere")
        spatial = g.spatial_signature()
        assert "output_fields" not in spatial
        assert g.signature() == {**spatial, "output_fields": g.signature()["output_fields"]}

    def test_rectilinear_excludes_output_fields(self, grid):
        spatial = grid.spatial_signature()
        assert "output_fields" not in spatial
        full = grid.signature()
        assert full == {**spatial, "output_fields": full["output_fields"]}

    def test_healpix_spatial_signature_invariant_to_agg_fields(self):
        # Same spatial grid, different aggregation configs -> identical spatial sig.
        a = HealpixGrid(6, 12, layout="fullsphere", config=default_config("atl06"))
        b = HealpixGrid(6, 12, layout="fullsphere", config=default_config("atl06_polar"))
        assert a.signature() != b.signature()  # full sigs differ (output_fields)
        assert a.spatial_signature() == b.spatial_signature()  # spatial sigs match

    def test_rectilinear_spatial_signature_invariant_to_agg_fields(self):
        bounds = [359400, 4300740, 369400, 4310740]
        a = RectilinearGrid("EPSG:32618", 10, bounds, [250, 250],
                            config=default_config("atl06"))
        b = RectilinearGrid("EPSG:32618", 10, bounds, [250, 250],
                            config=default_config("atl06_polar"))
        assert a.spatial_signature() == b.spatial_signature()

    def test_high_base_cell_morton_keys_roundtrip(self):
        """Parent-morton shard keys from southern (base 7-11) cells are large
        unsigned words; JSON (de)serialization preserves them exactly (#71).

        These are the keys that, as a signed int64, would read back negative —
        here we assert the manifest carries the unsigned value byte-for-byte.
        """
        from mortie import clip2order, geo2mort

        # Southern points → high base cells whose packed parent word sets bit 63.
        pts = [(-78.5, -132.0), (-72.1, 25.4), (-65.0, -45.0)]
        keys = sorted(
            int(clip2order(6, geo2mort(np.array([lat]), np.array([lon]), order=18))[0])
            for lat, lon in pts
        )
        assert any(k > 2**63 for k in keys)  # at least one bit-63-set key
        sm = ShardMap({"type": "healpix"}, keys, [[] for _ in keys], {})
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            sm.to_json(f.name)
            sm2 = ShardMap.from_json(f.name)
        assert sm2.shard_keys == keys
