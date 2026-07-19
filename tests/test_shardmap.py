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

    def test_cli_rejects_bad_footprint(self, monkeypatch):
        from zagg.catalog import main

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "zagg-catalog",
                "--config",
                "x.yaml",
                "--short-name",
                "ATL03",
                "--footprint",
                "garbage",
            ],
        )
        with pytest.raises(SystemExit):
            main()


class TestMortieOrder:
    """The mortie MOC order must track the grid, not a fixed coarse default (#92).

    A MOC order below ``parent_order`` upsamples in ``moc_to_order``, fattening
    every granule footprint onto all shards under each coarse cell -- the
    order-8-vs-order-13 degeneracy that put ~every granule in ~every shard.
    """

    @pytest.fixture
    def hp_grid(self):
        # parent_order 11 shards (~0.03 deg), child_order 17 leaves over the AOI.
        # chunk_inner unset -> chunk_order == parent_order == 11.
        return HealpixGrid(11, 17, layout="fullsphere")

    def test_default_keys_to_chunk_order(self, catalog):
        # chunk_inner=13 (the shipped ATL03 config) -> MOC order 13, the inner
        # chunk the worker dispatches at.
        g = HealpixGrid(11, 19, layout="fullsphere", chunk_inner=13)
        assert g.chunk_order == 13
        sm = ShardMap.build(catalog, g, backend="mortie")
        assert sm.metadata["mortie_order"] == 13

    def test_default_falls_back_to_parent_order(self, catalog, hp_grid):
        # chunk_inner unset -> chunk_order == parent_order, so the MOC order is the
        # bare shard order (the "else the shard order" branch of the directive).
        assert hp_grid.chunk_order == hp_grid.parent_order == 11
        sm = ShardMap.build(catalog, hp_grid, backend="mortie")
        assert sm.metadata["mortie_order"] == 11

    def test_default_under_mortie_cap_at_leaf_order_19(self, catalog):
        # The shipped production grid (chunk_inner 13) -> 13, under the order-18 cap.
        g = HealpixGrid(11, 19, layout="fullsphere", chunk_inner=13)
        sm = ShardMap.build(catalog, g, backend="mortie")
        assert sm.metadata["mortie_order"] == 13

    def test_coarse_order_rejected(self, catalog, hp_grid):
        # An explicit order coarser than parent_order would fatten -> raise.
        with pytest.raises(ValueError, match="coarser than the grid's parent_order"):
            ShardMap.build(catalog, hp_grid, backend="mortie", mortie_order=8)

    def test_derived_order_clamped_below_parent_rejected(self):
        # The derived path can still trip the guard: when parent_order exceeds the
        # order-18 cap, the clamp drives the order to 18 < parent_order, so the
        # guard fires (#92). chunk_order 19 -> clamped 18 < parent_order 19.
        from zagg.catalog.shardmap import _resolve_mortie_order

        g = HealpixGrid(19, 20, layout="fullsphere")  # chunk_order == parent_order == 19
        with pytest.raises(ValueError, match="coarser than the grid's parent_order"):
            _resolve_mortie_order(None, g)

    def test_derived_order_clamped_to_cap(self):
        # A chunk_order above mortie's order-18 cap is clamped to 18, never an
        # illegal order that mortie would reject (#92). chunk_inner=19 > cap, with
        # parent_order 15 so the clamped 18 still clears the parent_order guard.
        from zagg.catalog.shardmap import MORTIE_MOC_ORDER_CAP, _resolve_mortie_order

        g = HealpixGrid(15, 22, layout="fullsphere", chunk_inner=19)
        assert g.chunk_order == 19
        assert _resolve_mortie_order(None, g) == MORTIE_MOC_ORDER_CAP == 18

    def test_no_fattening_west_east_disjoint(self, hp_grid):
        # A west granule and an east granule must occupy disjoint shard sets --
        # under the old order-8 default both spread onto every AOI shard.
        cat = _catalog([_item("Gwest", -76.62, -76.59), _item("Geast", -76.53, -76.50)])
        sm = ShardMap.build(cat, hp_grid, backend="mortie")
        gs = _granule_shards(sm)
        assert gs["Gwest"] and gs["Geast"]
        assert gs["Gwest"].isdisjoint(gs["Geast"])

    def test_non_healpix_keeps_legacy_default(self, grid):
        # Non-HEALPix grids have no parent/child order -> legacy default of 8.
        from zagg.catalog.shardmap import _resolve_mortie_order

        assert _resolve_mortie_order(None, grid) == 8


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


class TestParquetIO:
    """Issue #135 phase 5: the parquet manifest form carries ``shard_keys`` as
    mortie's ``morton_index`` pyarrow extension type (registered on import) —
    typed morton columns on the catalog side, off the worker path."""

    @staticmethod
    def _sm(aoi_mask=None):
        return ShardMap(
            {"type": "healpix", "indexing_scheme": "nested", "parent_order": 6},
            [1050, 1051, 1201],
            [
                [{"id": "g1", "s3": "s3://a/g1.h5", "https": "https://a/g1.h5"}],
                [],
                [{"id": "g2", "s3": None, "https": "https://a/g2.h5"}],
            ],
            {"backend": "mortie", "total_shards": 3},
            aoi_mask,
        )

    def test_round_trip(self):
        pytest.importorskip("pyarrow")
        sm = self._sm()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            sm.to_parquet(f.name)
            sm2 = ShardMap.from_parquet(f.name)
        assert sm2.shard_keys == sm.shard_keys
        assert sm2.granules == sm.granules
        assert sm2.grid_signature == sm.grid_signature
        assert sm2.metadata == sm.metadata
        assert sm2.aoi_mask is None

    def test_shard_keys_column_is_extension_typed(self):
        pq = pytest.importorskip("pyarrow.parquet")
        import mortie.arrow

        sm = self._sm()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            sm.to_parquet(f.name)
            table = pq.read_table(f.name)
        col_type = table.column("shard_keys").type
        assert col_type.extension_name == mortie.arrow.EXTENSION_NAME
        # The words survive byte-equal through the typed column.
        np.testing.assert_array_equal(
            mortie.arrow.import_c_array(table.column("shard_keys")),
            np.asarray(sm.shard_keys, dtype=np.uint64),
        )

    def test_aoi_mask_round_trips_when_present(self):
        pytest.importorskip("pyarrow")
        aoi = [[1, 2, 3], [], [7]]
        sm = self._sm(aoi_mask=aoi)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            sm.to_parquet(f.name)
            sm2 = ShardMap.from_parquet(f.name)
        assert sm2.aoi_mask == aoi

    def test_foreign_parquet_rejected(self):
        pa_mod = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            pq.write_table(pa_mod.table({"x": pa_mod.array([1, 2])}), f.name)
            with pytest.raises(ValueError, match="not a zagg ShardMap parquet manifest"):
                ShardMap.from_parquet(f.name)

    def test_missing_shard_keys_column_rejected_cleanly(self):
        # A file carrying the zagg meta key and granules but no shard_keys must
        # hit the clean ValueError, not a bare pyarrow KeyError.
        pa_mod = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa_mod.table({"granules": pa_mod.array(["[]"])}).replace_schema_metadata(
            {ShardMap._PARQUET_META_KEY: b'{"metadata": {}, "grid_signature": {}}'}
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            pq.write_table(table, f.name)
            with pytest.raises(ValueError, match="not a zagg ShardMap parquet manifest"):
                ShardMap.from_parquet(f.name)

    def test_extension_stripped_column_still_loads(self):
        # import_c_array reads plain uint64 storage too (verified on mortie
        # 0.8.4), so a manifest whose shard_keys column lost the extension type
        # still rehydrates with the correct keys.
        pa_mod = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        sm = self._sm()
        stripped = pa_mod.table(
            {
                "shard_keys": pa_mod.array(np.asarray(sm.shard_keys, dtype=np.uint64)),
                "granules": pa_mod.array([json.dumps(g) for g in sm.granules]),
            }
        ).replace_schema_metadata(
            {
                ShardMap._PARQUET_META_KEY: json.dumps(
                    {"metadata": sm.metadata, "grid_signature": sm.grid_signature}
                ).encode()
            }
        )
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            pq.write_table(stripped, f.name)
            sm2 = ShardMap.from_parquet(f.name)
        assert sm2.shard_keys == sm.shard_keys
        assert sm2.granules == sm.granules


def _aoi_config(base="atl06_polar"):
    cfg = default_config(base)
    cfg.output = {**cfg.output, "aoi_mask": True}
    return cfg


class TestBuildAOIMask:
    """``ShardMap.build`` precomputes the strict-AOI per-shard payload (issue #101)."""

    def test_off_by_default(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert sm.aoi_mask is None
        assert "aoi_mask" not in sm.metadata

    def test_rectilinear_payload_populated(self, catalog, fake_spherely):
        grid = RectilinearGrid(
            "EPSG:32618",
            10,
            [359400, 4300740, 369400, 4310740],
            [250, 250],
            config=_aoi_config(),
        )
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert sm.aoi_mask is not None
        assert len(sm.aoi_mask) == len(sm.shard_keys)
        assert sm.metadata["aoi_mask"] is True
        # Each payload is a list of in-AOI cell ids that are valid children of the
        # shard (the worker maps them by membership over children()).
        for k, payload in zip(sm.shard_keys, sm.aoi_mask):
            assert isinstance(payload, list)
            children = set(int(c) for c in grid.children(int(k)))
            assert all(int(c) in children for c in payload)

    def test_healpix_payload_is_moc(self):
        # HEALPix uses the native mortie MOC path (no spherely needed).
        grid = HealpixGrid(6, 12, layout="fullsphere", config=_aoi_config("atl06"))
        sm = ShardMap.build(
            catalog=_catalog([_item("G", -76.62, -76.50)]), grid=grid, backend="mortie"
        )
        assert sm.aoi_mask is not None
        assert len(sm.aoi_mask) == len(sm.shard_keys)

    def test_unsupported_grid_raises(self):
        from zagg.catalog.shardmap import _compute_aoi_mask

        class _NoAOIGrid:
            pass

        with pytest.raises(ValueError, match="provides no AOI mask API"):
            _compute_aoi_mask(_NoAOIGrid(), [(np.array([0.0]), np.array([0.0]))], [1])

    def test_round_trip_carries_payload(self, catalog, fake_spherely):
        grid = RectilinearGrid(
            "EPSG:32618",
            10,
            [359400, 4300740, 369400, 4310740],
            [250, 250],
            config=_aoi_config(),
        )
        sm = ShardMap.build(catalog, grid, backend="spherely")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            sm.to_json(f.name)
            sm2 = ShardMap.from_json(f.name)
        assert sm2.aoi_mask == sm.aoi_mask


class TestSpatialSignature:
    """``spatial_signature()`` is the full signature minus the co-aggregation
    components — ``output_fields`` (#89) and, for HEALPix, ``cell_ids_encoding``
    (issue #135)."""

    def test_healpix_excludes_output_fields(self):
        g = HealpixGrid(6, 12, layout="fullsphere")
        spatial = g.spatial_signature()
        assert "output_fields" not in spatial
        assert "cell_ids_encoding" not in spatial
        assert g.signature() == {
            **spatial,
            "output_fields": g.signature()["output_fields"],
            "cell_ids_encoding": "nested",
        }

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
        a = RectilinearGrid("EPSG:32618", 10, bounds, [250, 250], config=default_config("atl06"))
        b = RectilinearGrid(
            "EPSG:32618", 10, bounds, [250, 250], config=default_config("atl06_polar")
        )
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


# ── beam-corridor footprints (issue #65) ─────────────────────────────────────

from zagg.catalog.beams import beam_tracks_from_cmr_polygon  # noqa: E402

# Real RGT0568 cycle-29 CMR footprint polygon (lon, lat), captured from CMR.
# The granule's measured beam ground-tracks at lat 38.89 are gt1l -76.5475,
# gt2l -76.5106, gt3l -76.4737 -- the decomposition must place a corridor over
# each (issue #65 validation target).
_C29_POLY = [
    (-79.4552, 59.5458),
    (-79.6776, 59.5342),
    (-79.5274, 58.7894),
    (-79.1270, 56.6847),
    (-79.0193, 55.9820),
    (-78.9553, 55.3096),
    (-78.5002, 52.5857),
    (-78.1682, 50.4866),
    (-77.4919, 45.8442),
    (-76.9446, 41.7520),
    (-76.4355, 37.6827),
    (-75.9327, 33.4550),
    (-75.3154, 28.0089),
    (-75.1996, 26.9469),
    (-75.0726, 26.9579),
    (-75.1873, 28.0199),
    (-75.7972, 33.4664),
    (-76.2927, 37.6939),
    (-76.7931, 41.7632),
    (-77.3297, 45.8554),
    (-77.9907, 50.4980),
    (-78.3143, 52.5970),
    (-78.7568, 55.3209),
    (-78.8168, 55.9880),
    (-78.9211, 56.6943),
    (-79.3096, 58.8011),
    (-79.4552, 59.5458),
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
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())],
        "properties": {"datetime": "2025-06-01T00:00:00Z"},
        "collection": "ATL03_007",
        "stac_extensions": [],
        "links": [],
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
        "EPSG:32618",
        10,
        [359400, 4300740, 369400, 4310740],
        [50, 50],
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

    def test_wide_lon_polar_does_not_fall_back(self):
        # Near-polar quarter-orbit polygons can sweep > 180 deg of longitude
        # with no antimeridian crossing -- consecutive vertices stay close.
        # The decomposition must run on these, not silently degrade to swath.
        lats = np.array([85.5, 85.7, 85.9, 86.0, 86.1, 86.3, 86.1, 86.0, 85.9, 85.7, 85.5, 85.5])
        lons = np.array(
            [
                -150.0,
                -100.0,
                -50.0,
                0.0,
                50.0,
                100.0,
                105.0,
                55.0,
                5.0,
                -45.0,
                -95.0,
                -150.0,
            ]
        )
        assert float(np.ptp(lons)) > 180.0  # spans >180 deg but no seam
        assert float(np.max(np.abs(np.diff(lons)))) < 180.0  # no neighbour jump
        out = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(out) == 3, "wide-lon polar swath must decompose, not no-op to swath"

    def test_wider_envelope_widens_corridor(self):
        # The CMR envelope's symmetric centerline (mean of the two edges) is
        # only a faithful proxy for the true data axis when the envelope is the
        # expected ~12.6 km width; a wider envelope means extra CMR padding has
        # moved the envelope center away from the true data axis. The adaptive
        # half-width must widen the corridor by the excess so the beams remain
        # covered when the envelope is over-padded.
        from shapely.geometry import Point, Polygon

        center_lon, center_lat = -76.50, 38.89
        deg_per_m = 1.0 / (np.cos(np.radians(center_lat)) * 111320.0)
        # 20 km wide envelope (~10 km half-width vs the ~6.3 km expected).
        col = np.linspace(38.74, 39.04, 12)
        wide = 10_000.0 * deg_per_m
        lats = np.concatenate([col, col[::-1], [col[0]]])
        lons = np.concatenate(
            [np.full(12, center_lon - wide), np.full(12, center_lon + wide), [center_lon - wide]],
        )
        rings = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(rings) == 3
        # Outer corridor must extend ~3.3 km + (10 - 6.3) km of widening = ~7 km
        # from the centerline. Probe the extremes of the corridor at mid-lat.
        outer_offset_m = 3300.0 + (10_000.0 - 6300.0)  # ~7000 m
        eps = 0.001  # ~110 m
        outer_lon_pos = Point(center_lon + outer_offset_m * deg_per_m - eps, center_lat)
        outer_lon_neg = Point(center_lon - outer_offset_m * deg_per_m + eps, center_lat)
        corridors = [Polygon(zip(rlon, rlat)) for rlat, rlon in rings]
        assert any(c.contains(outer_lon_pos) for c in corridors), (
            "adaptive widening must extend corridor outward when envelope is over-padded"
        )
        assert any(c.contains(outer_lon_neg) for c in corridors)

    def test_normal_envelope_keeps_base_corridor_width(self):
        # Converse of the "widen-when-wide" test: a normal ~12.6 km envelope
        # must NOT trigger the adaptive widening, so the inter-pair ~3 km gaps
        # stay unassigned (the original tightening goal of #65). Asserted on
        # corridor extent: gt2 (offset 0) must not extend past ~500 m + the
        # ~260 m centerline-recovery error.
        from shapely.geometry import LineString, Polygon

        lats, lons = _swath_latlon(-76.50, 38.89, n=12)
        rings = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        gt2_lat, gt2_lon = rings[1]
        poly = Polygon(zip(gt2_lon, gt2_lat))
        sl = poly.intersection(LineString([(-78, 38.89), (-75, 38.89)]))
        xs = [c[0] for g in (sl.geoms if hasattr(sl, "geoms") else [sl]) for c in g.coords]
        deg_per_m = 1.0 / (np.cos(np.radians(38.89)) * 111320.0)
        half_extent_m = abs(max(xs) - min(xs)) / 2 / deg_per_m
        # Base ~500 m + ~260 m centerline-recovery error budget; > ~1 km would
        # indicate the adaptive widening over-fired on a normal envelope.
        assert half_extent_m < 1000.0, (
            f"normal envelope should keep corridor narrow; got half-extent {half_extent_m:.0f} m"
        )

    def test_input_order_reversed_yields_same_corridor_union(self):
        # The S->N reorder in ``_centerline`` makes the corridor union order-
        # invariant: input vertices ordered N->S (e.g. a descending track read
        # in scan order) must produce the same coverage as an S->N input. A
        # genuine descending-heading test (azimuth ~170 deg) would require a
        # non-pure-meridional polygon; this test asserts the easier and more
        # important invariant for shard assignment, which is union coverage.
        from shapely.geometry import Point, Polygon

        center_lat = np.linspace(39.04, 38.74, 12)  # N -> S input order
        center_lon = -76.50 + 0.05 * np.linspace(-1, 1, 12)  # mild eastward drift
        deg_per_m = 1.0 / (np.cos(np.radians(38.89)) * 111320.0)
        half_w = 0.073  # ~6.3 km
        west_lon = center_lon - half_w
        east_lon = center_lon + half_w
        lats = np.concatenate([center_lat, center_lat[::-1], [center_lat[0]]])
        lons = np.concatenate([west_lon, east_lon[::-1], [west_lon[0]]])
        rings = beam_tracks_from_cmr_polygon(lats, lons, product="ATL03")
        assert len(rings) == 3
        beams_true = [Point(-76.50 + d * 3300 * deg_per_m, 38.89) for d in (-1, 0, 1)]
        corridors = [Polygon(zip(rlon, rlat)) for rlat, rlon in rings]
        for beam in beams_true:
            assert any(c.contains(beam) for c in corridors)


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
        region = [
            (
                np.array([38.74, 38.74, 39.04, 39.04, 38.74]),
                np.array([-76.62, -76.42, -76.42, -76.62, -76.62]),
            )
        ]
        swath = ShardMap.build(
            cat, hp, region=region, backend="mortie", mortie_order=14, footprint="swath"
        )
        beams = ShardMap.build(
            cat, hp, region=region, backend="mortie", mortie_order=14, footprint="beams"
        )
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

    def test_beams_on_non_beam_catalog_raises(self, catalog, grid, fake_spherely):
        # ``catalog`` fixture has collection "TEST", not ATL03/06. Requesting
        # beams must fail loudly rather than silently degrade to swath -- the
        # opt-in flag would otherwise record ``footprint="beams"`` while no
        # tightening occurred.
        with pytest.raises(ValueError, match="requires an ICESat-2 beam product"):
            ShardMap.build(catalog, grid, backend="spherely", footprint="beams")

    def test_beams_on_missing_collection_metadata_raises(self, grid, fake_spherely):
        # Catalog without ``collection`` metadata at all -> product resolves to
        # the empty string; beams must refuse rather than no-op.
        cat = _catalog([_item("G", -76.55, -76.52)])
        cat.metadata.pop("collection", None)
        with pytest.raises(ValueError, match="requires an ICESat-2 beam product"):
            ShardMap.build(cat, grid, backend="spherely", footprint="beams")


class TestReproject:
    """``ShardMap.reproject`` (issue #294): derive a map at another HEALPix
    order without rebuilding from the catalog -- coarsen is a pure regroup,
    refine is a scoped re-intersection using the source catalog's footprints.
    """

    @pytest.fixture
    def fine_grid(self):
        return HealpixGrid(12, 14, layout="fullsphere")

    @pytest.fixture
    def coarse_grid(self):
        return HealpixGrid(11, 14, layout="fullsphere")

    def test_coarsen_matches_direct_build(self, catalog, fine_grid, coarse_grid):
        sm_fine = ShardMap.build(catalog, fine_grid, backend="mortie")
        sm_coarse_direct = ShardMap.build(catalog, coarse_grid, backend="mortie")
        sm_coarse_reproj = sm_fine.reproject(coarse_grid)

        assert sorted(sm_coarse_reproj.shard_keys) == sorted(sm_coarse_direct.shard_keys)
        assert _granule_shards(sm_coarse_reproj) == _granule_shards(sm_coarse_direct)
        assert sm_coarse_reproj.grid_signature == coarse_grid.spatial_signature()
        assert sm_coarse_reproj.metadata["reproject"] == {
            "source_parent_order": 12,
            "target_parent_order": 11,
            "method": "coarsen",
        }

    def test_coarsen_dedups_granule_spanning_multiple_children(self, fine_grid, coarse_grid):
        # A granule wide enough to land in >=2 fine shards under the same
        # coarse parent must count once in the coarsened granule list. Force
        # and assert the dedup scenario is real -- otherwise the union-across-
        # children branch never runs and the test proves nothing.
        from mortie import clip2order

        cat = _catalog([_item("Gwide", -76.60, -76.52)])
        sm_fine = ShardMap.build(cat, fine_grid, backend="mortie")
        fine_shards = _granule_shards(sm_fine)["Gwide"]
        assert len(fine_shards) >= 2, "Gwide must span >=2 fine shards to exercise dedup"

        # >=2 of those fine shards must coarsen to a common parent, else the
        # coarsen path never unions Gwide across children.
        fine_arr = np.asarray([int(k) for k in fine_shards], dtype=np.uint64)
        parents = clip2order(11, fine_arr).tolist()
        shared = {int(p) for p in parents if parents.count(p) >= 2}
        assert shared, "no coarse parent gathers >=2 of Gwide's fine shards"

        sm_coarse = sm_fine.reproject(coarse_grid)
        gs_coarse = _granule_shards(sm_coarse)
        # The union collapsed >=2 fine children into one coarse shard, so Gwide's
        # coarse shard count is strictly fewer than its fine count.
        assert len(gs_coarse["Gwide"]) < len(fine_shards)
        assert shared <= gs_coarse["Gwide"]
        # And it appears exactly once within each coarsened shard's granule list.
        for gran_list in sm_coarse.granules:
            ids = [g["id"] for g in gran_list]
            assert len(ids) == len(set(ids))

    def test_refine_reproduces_build(self, catalog, fine_grid, coarse_grid):
        sm_coarse = ShardMap.build(catalog, coarse_grid, backend="mortie")
        sm_fine_direct = ShardMap.build(catalog, fine_grid, backend="mortie")
        sm_fine_reproj = sm_coarse.reproject(fine_grid, catalog=catalog)

        assert sorted(sm_fine_reproj.shard_keys) == sorted(sm_fine_direct.shard_keys)
        assert _granule_shards(sm_fine_reproj) == _granule_shards(sm_fine_direct)
        assert sm_fine_reproj.grid_signature == fine_grid.spatial_signature()
        assert sm_fine_reproj.metadata["reproject"]["method"] == "refine"

    def test_refine_without_catalog_raises(self, catalog, coarse_grid, fine_grid):
        sm_coarse = ShardMap.build(catalog, coarse_grid, backend="mortie")
        with pytest.raises(ValueError, match="needs the source Catalog"):
            sm_coarse.reproject(fine_grid)

    def test_round_trip_coarsen_then_refine(self, catalog, fine_grid, coarse_grid):
        sm_fine = ShardMap.build(catalog, fine_grid, backend="mortie")
        sm_coarse = sm_fine.reproject(coarse_grid)
        sm_round = sm_coarse.reproject(fine_grid, catalog=catalog)

        assert sorted(sm_round.shard_keys) == sorted(sm_fine.shard_keys)
        assert _granule_shards(sm_round) == _granule_shards(sm_fine)

    def test_same_order_returns_copy(self, catalog, fine_grid):
        sm = ShardMap.build(catalog, fine_grid, backend="mortie")
        sm2 = sm.reproject(fine_grid)
        assert sm2 is not sm
        assert sm2.shard_keys == sm.shard_keys
        assert sm2.granules == sm.granules
        assert sm2.metadata["reproject"] == {
            "source_parent_order": 12,
            "target_parent_order": 12,
            "method": "noop",
        }

    def test_mismatched_child_order_rejected(self, catalog, fine_grid):
        sm = ShardMap.build(catalog, fine_grid, backend="mortie")
        other_leaf = HealpixGrid(11, 17, layout="fullsphere")  # different child_order
        with pytest.raises(ValueError, match="child_order must match"):
            sm.reproject(other_leaf)

    def test_non_healpix_signature_rejected(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")  # RectilinearGrid
        hp = HealpixGrid(11, 14, layout="fullsphere")
        with pytest.raises(ValueError, match="HEALPix"):
            sm.reproject(hp)


class TestIsBeamProduct:
    def test_known_beam_products(self):
        from zagg.catalog.beams import is_beam_product

        assert is_beam_product("ATL03")
        assert is_beam_product("ATL06")
        assert is_beam_product("atl03")  # case-insensitive

    def test_non_beam_or_missing(self):
        from zagg.catalog.beams import is_beam_product

        assert not is_beam_product("ATL08")
        assert not is_beam_product("")
        assert not is_beam_product(None)
