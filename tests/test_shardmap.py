"""Tests for ShardMap building (spherely + mortie backends; IO; resolution).

The real spherely (SpatialIndex) backend is exercised separately in the conda
sidecar env since its build isn't in the default venv. Here the spherely
*brute* path (elementwise ``spherely.intersects``, no SpatialIndex) and the
absent-spherely error are exercised with a lightweight fake spherely module so
they run in the default venv (#36).
"""

import json
import pathlib
import sys
import tempfile
import time
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


@pytest.fixture
def pre_445_path(monkeypatch):
    """Force ``build`` back onto the pre-#445 records-first path -- the oracle.

    Before issue #445 an unindexed mortie build decoded every record and covered
    each footprint from its rings (``_intersect_mortie``). Returning ``None``
    from the ephemeral plan restores exactly that branch, so the two-stage cover
    can be pinned against the path it replaced without a build kwarg that would
    exist only for tests. The stored-column plan is untouched, so an indexed
    build under this fixture still takes its own fast path.
    """
    monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)


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

    def test_granules_assigned_counts_only_intersectors(self, grid, fake_spherely):
        # total_granules is the records CONSIDERED (input superset);
        # granules_assigned is the distinct set the exact intersection placed
        # on shards -- the fixture must actually prune, or an implementation
        # of len(records) would pass (review finding).
        cat = _catalog(
            [
                _item("Gwest", -76.62, -76.57),
                _item("Geast", -76.55, -76.50),
                _item("Gfar", -70.00, -69.95),  # outside the grid -> pruned
            ]
        )
        sm = ShardMap.build(cat, grid, backend="spherely")
        assert sm.metadata["total_granules"] == 3
        assert sm.metadata["granules_assigned"] == 2
        assert sm.metadata["granules_assigned"] == len(
            {g["id"] for shard in sm.granules for g in shard}
        )

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
        # chunk the worker dispatches at. That is the resolver's contract, and
        # it still governs every path that covers from decoded rings (beams,
        # non-HEALPix, reproject). A HEALPix swath build covers from WKB instead
        # and defaults to parent_order (issue #445), so it records 11 -- pinned
        # beside the resolver so the two defaults can't drift unnoticed.
        from zagg.catalog.shardmap import _resolve_mortie_order

        g = HealpixGrid(11, 19, layout="fullsphere", chunk_inner=13)
        assert g.chunk_order == 13
        assert _resolve_mortie_order(None, g) == 13
        sm = ShardMap.build(catalog, g, backend="mortie")
        assert sm.metadata["mortie_order"] == 11

    def test_default_falls_back_to_parent_order(self, catalog, hp_grid):
        # chunk_inner unset -> chunk_order == parent_order, so the MOC order is the
        # bare shard order (the "else the shard order" branch of the directive).
        assert hp_grid.chunk_order == hp_grid.parent_order == 11
        sm = ShardMap.build(catalog, hp_grid, backend="mortie")
        assert sm.metadata["mortie_order"] == 11

    def test_default_under_mortie_cap_at_leaf_order_19(self, catalog, pre_445_path):
        # The shipped production grid (chunk_inner 13) -> 13, under the order-18
        # cap. Read on the records path, which is where the derived order is
        # still what a build runs at (issue #445 moved the WKB path to
        # parent_order); the leaf order 19 must not leak into either.
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


def _intersect_mortie_serial(records, grid, all_shards, order, footprint="swath", product="ATL03"):
    """The pre-#396 per-granule HEALPix path, verbatim -- the identity oracle.

    Kept here rather than in ``shardmap.py`` so exactly one implementation ships,
    while the batch rewire is still pinned against the logic it replaced:
    ``morton_coverage_moc`` once per ring, ``moc_to_order`` + ``np.unique`` per
    ring, a scalar ``in all_shards`` test per cell, and a ``dict.fromkeys`` dedup
    per shard (``shardmap.py:307-340`` at ad8aa30, the merge-base of this PR).
    """
    from mortie import moc_to_order, morton_coverage_moc

    from zagg.catalog.shardmap import _granule_footprints

    out: dict = {}
    parent_order = grid.parent_order
    for i, rec in enumerate(records):
        for rlats, rlons in _granule_footprints(rec, footprint, product):
            try:
                moc = np.asarray(morton_coverage_moc(rlats, rlons, order=order))
            except Exception:
                continue
            if moc.size == 0:
                continue
            try:
                shards = np.unique(moc_to_order(moc, parent_order))
            except Exception:
                continue
            for s in shards.tolist():
                s = int(s)
                if s in all_shards:
                    out.setdefault(s, []).append(i)
    return {k: list(dict.fromkeys(v)) for k, v in out.items()}


def _overlapping_catalog(n=12):
    """n granules sliding west->east across SERC with overlapping footprints.

    Neighbors overlap (0.02-deg windows every 0.008 deg), so granules on either
    side of any batch-block boundary share shards -- the regroup must gather a
    shard's granules across blocks, not just concatenate disjoint shard sets.
    """
    items = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(n)]
    return _catalog(items)


def _with_null_geometry(cat, row):
    """Copy of ``cat`` with row ``row``'s geometry WKB set to null."""
    field = cat.table.schema.field("geometry")
    wkb = cat.table.column("geometry").to_pylist()
    wkb[row] = None
    table = cat.table.set_column(
        cat.table.column_names.index("geometry"), field, pa.array(wkb, field.type)
    )
    return Catalog(table, dict(cat.metadata or {}))


class TestMortieBatch:
    """The HEALPix mortie path is a batch call into mortie, not a granule loop (#396).

    ``_intersect_mortie`` flattens every granule's rings into mortie's ragged
    layout, covers them a block at a time via ``polygons_to_morton_mocs``, and
    resolves shard membership with ``searchsorted`` instead of a scalar
    ``in all_shards`` per cell. The pin is **exact identity** with the serial
    oracle above -- not just the pair set but the per-shard granule *order*, so
    the manifest a build writes is byte-identical to the pre-#396 one.
    """

    @pytest.fixture
    def hp_grid(self):
        return HealpixGrid(11, 17, layout="fullsphere")

    @staticmethod
    def _inputs(cat, grid, region=None):
        from zagg.catalog.shardmap import _region_parts

        records = cat.granule_records()
        all_shards = {int(s) for s in grid.coverage(_region_parts(region, cat.metadata))}
        return records, all_shards

    def test_batch_equals_serial_swath(self, hp_grid):
        cat = _overlapping_catalog()
        records, all_shards = self._inputs(cat, hp_grid)
        batch = shardmap._intersect_mortie(records, hp_grid, all_shards, order=11)
        serial = _intersect_mortie_serial(records, hp_grid, all_shards, order=11)
        assert len(serial) > 4, "fixture must span multiple shards"
        assert any(len(v) > 1 for v in serial.values()), "fixture must share shards across granules"
        assert batch == serial

    def test_batch_equals_serial_beams(self):
        # ``beams`` yields three corridor rings per granule (issue #65): the one
        # case where the ring -> granule map does real work, since a granule's
        # shards are the union over its rings and the batch is one ring per entry.
        from zagg.catalog.shardmap import _granule_footprints

        hp = HealpixGrid(12, 14, layout="fullsphere")
        cat = _atl03_catalog(
            [_swath_item(gid, lon, 38.89) for gid, lon in (("Ga", -76.52), ("Gb", -76.50))]
        )
        region = [
            (
                np.array([38.74, 38.74, 39.04, 39.04, 38.74]),
                np.array([-76.62, -76.42, -76.42, -76.62, -76.62]),
            )
        ]
        records, all_shards = self._inputs(cat, hp, region=region)
        assert len(_granule_footprints(records[0], "beams", "ATL03")) == 3
        kw = {"order": 14, "footprint": "beams", "product": "ATL03"}
        batch = shardmap._intersect_mortie(records, hp, all_shards, **kw)
        serial = _intersect_mortie_serial(records, hp, all_shards, **kw)
        assert serial
        assert batch == serial

    def test_identity_holds_across_block_boundaries(self, hp_grid, monkeypatch):
        # A block size of 5 over 12 overlapping granules puts cuts at 5/10, and
        # neighbours straddle every cut, so shards must gather granules from more
        # than one block -- and in record order, not block-concatenation order.
        cat = _overlapping_catalog()
        records, all_shards = self._inputs(cat, hp_grid)
        serial = _intersect_mortie_serial(records, hp_grid, all_shards, order=11)
        monkeypatch.setattr(shardmap, "_MOC_BATCH_RINGS", 5)
        batch = shardmap._intersect_mortie(records, hp_grid, all_shards, order=11)
        assert batch == serial
        block_of = {i: i // 5 for i in range(len(records))}
        assert any(len({block_of[i] for i in v}) >= 2 for v in batch.values()), (
            "no shard drew granules from two blocks -- cross-block regroup unexercised"
        )

    def test_build_pair_identity(self, hp_grid):
        # The public path: the manifest a build writes matches the oracle's
        # (shard, granule id) assignment exactly, including per-shard order.
        cat = _overlapping_catalog()
        records, all_shards = self._inputs(cat, hp_grid)
        sm = ShardMap.build(cat, hp_grid, backend="mortie")
        serial = _intersect_mortie_serial(records, hp_grid, all_shards, order=11)
        assert sm.shard_keys == sorted(serial)
        assert [[g["id"] for g in shard] for shard in sm.granules] == [
            [records[i]["id"] for i in serial[k]] for k in sorted(serial)
        ]

    def test_malformed_rings_dropped_not_fatal(self, hp_grid):
        # The batch call is fail-fast for its whole block where the serial loop
        # dropped one granule, so unusable rings are screened before it: a
        # non-finite vertex, a 2-vertex ring, and a lat/lon length mismatch must
        # each vanish quietly while their neighbours still assign.
        good = _overlapping_catalog(n=2).granule_records()
        bad = [
            {
                "id": "Gnan",
                "lats": np.array([38.85, np.nan, 38.9, 38.85]),
                "lons": np.full(4, -76.6),
            },
            {"id": "Gshort", "lats": np.array([38.85, 38.9]), "lons": np.array([-76.6, -76.5])},
            {"id": "Gmismatch", "lats": np.array([38.85, 38.9, 38.95]), "lons": np.full(4, -76.6)},
        ]
        records = [good[0], *bad, good[1]]
        _, all_shards = self._inputs(_overlapping_catalog(n=2), hp_grid)
        out = shardmap._intersect_mortie(records, hp_grid, all_shards, order=11)
        assigned = {i for v in out.values() for i in v}
        assert assigned == {0, 4}, f"only the two well-formed granules assign, got {assigned}"
        # The screen in ``_flatten_rings`` is a Python *copy* of mortie's rejection
        # rules, not a derivation from them, so it is the one place this PR can
        # diverge from the oracle by construction. Pin it against mortie itself:
        # if mortie's accept/reject set moves, the screen starts dropping rings
        # mortie would have covered and only this assertion notices.
        assert out == _intersect_mortie_serial(records, hp_grid, all_shards, order=11)
        # All rings malformed: ``_flatten_rings`` returns None -> {}, and the
        # serial loop swallows every granule -> {} too.
        all_bad = shardmap._intersect_mortie(bad, hp_grid, all_shards, order=11)
        assert all_bad == _intersect_mortie_serial(bad, hp_grid, all_shards, order=11) == {}

    def test_batch_failure_falls_back_to_serial(self, hp_grid, monkeypatch):
        # An undocumented mortie-side failure (e.g. a captured kernel panic)
        # raises for the whole block; the fallback must reproduce the serial
        # result rather than lose the build.
        import mortie

        cat = _overlapping_catalog()
        records, all_shards = self._inputs(cat, hp_grid)
        serial = _intersect_mortie_serial(records, hp_grid, all_shards, order=11)

        def boom(*a, **kw):
            raise RuntimeError("polygon 3: polygon coverage panicked")

        monkeypatch.setattr(mortie, "polygons_to_morton_mocs", boom)
        with pytest.warns(RuntimeWarning, match="fell back to the per-ring path"):
            assert shardmap._intersect_mortie(records, hp_grid, all_shards, order=11) == serial

    def test_fallback_warns_once_per_build(self, hp_grid, monkeypatch):
        # The fallback is correct but several times slower, so it must not be
        # silent -- an operator whose build suddenly takes 4x needs to know a
        # mortie-side failure caused it. Equally it must not shout per block:
        # three blocks fall back here and exactly one warning comes out.
        import mortie

        records, all_shards = self._inputs(_overlapping_catalog(), hp_grid)

        def boom(*a, **kw):
            raise RuntimeError("polygon 3: polygon coverage panicked")

        monkeypatch.setattr(shardmap, "_MOC_BATCH_RINGS", 5)
        monkeypatch.setattr(mortie, "polygons_to_morton_mocs", boom)
        with pytest.warns(RuntimeWarning, match="polygon coverage panicked") as rec:
            shardmap._intersect_mortie(records, hp_grid, all_shards, order=11)
        assert len(rec) == 1, f"three blocks fell back; expected one warning, got {len(rec)}"

    def test_fallback_is_scoped_to_the_failing_block(self, hp_grid, monkeypatch):
        # ``_batch_ring_mocs`` claims the fallback is "for this block only": the
        # surviving blocks stay on the batch path and the regroup still stitches
        # every block into record order. The test above cannot see that -- one
        # block, raising unconditionally -- so raise on the Nth call instead,
        # with the block size cut to 5 so 12 granules make three blocks and each
        # takes a turn as the failing one (including the last, partial, block).
        import mortie

        cat = _overlapping_catalog()
        records, all_shards = self._inputs(cat, hp_grid)
        serial = _intersect_mortie_serial(records, hp_grid, all_shards, order=11)
        real = mortie.polygons_to_morton_mocs
        monkeypatch.setattr(shardmap, "_MOC_BATCH_RINGS", 5)

        def make_flaky(bad):
            state = {"calls": 0, "batched": 0}

            def flaky(*a, **kw):
                state["calls"] += 1
                if state["calls"] - 1 == bad:
                    raise RuntimeError("polygon 3: polygon coverage panicked")
                state["batched"] += 1
                return real(*a, **kw)

            return flaky, state

        for bad_block in (0, 1, 2):
            flaky, state = make_flaky(bad_block)
            monkeypatch.setattr(mortie, "polygons_to_morton_mocs", flaky)
            with pytest.warns(RuntimeWarning, match="fell back to the per-ring path"):
                out = shardmap._intersect_mortie(records, hp_grid, all_shards, order=11)
            assert out == serial
            assert state["calls"] == 3, f"expected three blocks, saw {state['calls']}"
            assert state["batched"] == 2, "the surviving blocks must stay on the batch path"

    def test_empty_inputs_short_circuit(self, hp_grid):
        cat = _overlapping_catalog(n=2)
        records, all_shards = self._inputs(cat, hp_grid)
        assert shardmap._intersect_mortie([], hp_grid, all_shards, order=11) == {}
        assert shardmap._intersect_mortie(records, hp_grid, set(), order=11) == {}

    def test_flatten_rings_offsets_contract(self):
        # mortie's batch contract is strict: offsets[0] == 0 and offsets[-1] ==
        # len(lats) == len(lons) (exact coverage). Rings also stay attributable:
        # the three beam rings of granule 0 all own record 0.
        from zagg.catalog.shardmap import _flatten_rings

        records = _atl03_catalog(
            [_swath_item(gid, lon, 38.89) for gid, lon in (("Ga", -76.52), ("Gb", -76.50))]
        ).granule_records()
        lats, lons, offsets, owners = _flatten_rings(records, "beams", "ATL03")
        assert offsets[0] == 0
        assert offsets[-1] == lats.size == lons.size
        assert np.all(np.diff(offsets) >= 3)
        assert owners.tolist() == [0, 0, 0, 1, 1, 1]
        assert offsets.size == owners.size + 1
        # Swath mode is one ring per granule, so owners are the record indices.
        assert _flatten_rings(records, "swath", "ATL03")[3].tolist() == [0, 1]
        assert _flatten_rings([], "swath", "ATL03") is None


def _shard_ids(sm):
    """``(shard_keys, per-shard granule ids)`` -- the manifest's whole assignment."""
    return sm.shard_keys, [[g["id"] for g in shard] for shard in sm.granules]


def _intersect_cells_serial(rows, values, offsets, grid, all_shards):
    """The pre-phase-4 per-granule stored-MOC loop, verbatim -- the parity oracle.

    Kept here rather than in ``shardmap.py`` so exactly one implementation
    ships, while the mortie 0.9.6 batch swap (``mocs_and``/``mocs_to_orders``,
    espg/mortie#173) stays pinned against the scalar logic it replaced:
    ``moc_and`` + ``moc_to_order`` + ``np.unique`` once per record
    (``shardmap.py:293-333`` at a74a4fc9, the phase-3 tip of this PR). The one
    behavior deliberately not carried over is the ``except Exception: continue``
    around ``moc_to_order``: the batch call refuses the whole build loudly where
    this dropped one granule silently (see ``_intersect_footprint_cells``).
    """
    from mortie import compress_moc, moc_and, moc_to_order

    if len(rows) == 0 or not all_shards:
        return {}
    parent_order = grid.parent_order
    shard_arr = np.fromiter(all_shards, dtype=np.uint64, count=len(all_shards))
    shard_arr.sort()
    aoi_moc = np.asarray(compress_moc(shard_arr))
    hit_shards, hit_owners = [], []
    for i, row in enumerate(rows):
        moc = values[offsets[row] : offsets[row + 1]]
        if moc.size == 0:
            continue
        hit = np.asarray(moc_and(moc, aoi_moc))
        if hit.size == 0:
            continue
        shards = np.unique(np.asarray(moc_to_order(hit, parent_order)))
        if shards.size:
            hit_shards.append(shards.astype(np.uint64, copy=False))
            hit_owners.append(np.full(shards.size, i, dtype=np.int64))
    return shardmap._regroup_hits(hit_shards, hit_owners)


class TestFootprintCells:
    """Phase 3: an indexed catalog answers ``build`` with set algebra (issue #396).

    The oracle here is the **phase-2 geometry path itself**: on single-part
    footprints -- every CMR ATL03/06 granule -- each test asserts the fast path
    reproduces the mortie build it replaces, shard keys and per-shard granule
    order included, so the index can only ever be faster, not different. The one
    intended divergence, a MultiPolygon superset, is pinned separately below.
    What is otherwise genuinely new and therefore pinned on its own is the
    engagement gate (which builds may take it) and the refusal (which must not).
    """

    @pytest.fixture
    def hp_grid(self):
        return HealpixGrid(11, 17, layout="fullsphere")

    def test_indexed_build_matches_the_geometry_build(self, hp_grid):
        cat = _overlapping_catalog()
        geometry = ShardMap.build(cat, hp_grid, backend="mortie")
        fast = ShardMap.build(cat.index_footprints(11), hp_grid, backend="mortie")
        assert len(geometry.shard_keys) > 4, "fixture must span multiple shards"
        assert _shard_ids(fast) == _shard_ids(geometry)
        assert fast.metadata["footprint_cells"] is True
        assert fast.metadata["mortie_order"] == 11

    def test_finer_column_coarsens_to_the_same_build(self, hp_grid):
        # A column may be finer than the shard order -- ``moc_to_order``
        # coarsens it -- and an order-13 index must land the same assignment an
        # order-11 cover does at parent_order 11.
        cat = _overlapping_catalog()
        geometry = ShardMap.build(cat, hp_grid, backend="mortie")
        fast = ShardMap.build(cat.index_footprints(13), hp_grid, backend="mortie")
        assert _shard_ids(fast) == _shard_ids(geometry)
        assert fast.metadata["mortie_order"] == 13

    def test_column_coarser_than_the_shard_order_is_refused(self):
        # The one thing the column cannot answer: a shard order FINER than it.
        # Refining each cell onto all its descendants would put ~every granule
        # in ~every shard (#92), so this raises rather than answering, and
        # rather than quietly falling back to geometry -- which would hide that
        # the index the operator paid for is useless for this grid.
        cat = _overlapping_catalog().index_footprints(9)
        with pytest.raises(ValueError, match="order 9, coarser than.*parent_order 11"):
            ShardMap.build(cat, HealpixGrid(11, 17, layout="fullsphere"), backend="mortie")

    def test_row_alignment_survives_rows_granule_records_drops(self, hp_grid):
        # The column has one entry per TABLE row; ``granule_records`` skips rows
        # with empty or non-polygonal geometry. Record index is therefore not
        # the row index, and a fast path that assumed it would silently hand
        # every granule after the gap its neighbour's footprint.
        good = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(6)]
        pt = _item("PT", -76.62, -76.60)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.61, 38.89]}
        empty = _item("EMPTY", -76.62, -76.60)
        empty["geometry"] = {"type": "Polygon", "coordinates": []}
        cat = _catalog(good[:2] + [pt] + good[2:4] + [empty] + good[4:])
        assert [r["id"] for r in cat.granule_records()] == [g["id"] for g in good]
        geometry = ShardMap.build(cat, hp_grid, backend="mortie")
        fast = ShardMap.build(cat.index_footprints(11), hp_grid, backend="mortie")
        assert _shard_ids(fast) == _shard_ids(geometry)

    def test_multipolygon_is_a_superset_not_an_identity(self, hp_grid, pre_445_path):
        # The one place the fast path is deliberately NOT the geometry path.
        # ``index_footprints`` covers the union of the rings in each blob;
        # ``granule_records`` reads the largest part's exterior ring only. So a
        # two-part footprint gets the second part's shards from the index and
        # not from geometry -- a superset, and the intended answer. Alignment is
        # unaffected: every geometry shard is still present, with the same ids.
        # The oracle is the pre-#445 records path (``pre_445_path``): the live
        # path now shares this cover, so it shares the superset too -- pinned in
        # ``TestLiveCover`` rather than smuggled in here as an equality.
        multi = _item("MULTI", -76.62, -76.60)

        def _ring(lon0, lon1):
            return [
                [lon0, 38.85],
                [lon1, 38.85],
                [lon1, 38.93],
                [lon0, 38.93],
                [lon0, 38.85],
            ]

        # Both parts sit inside the catalog bbox (so the AOI is not what
        # separates them); the second is the smaller, so it is the one
        # ``granule_records`` drops.
        multi["geometry"] = {
            "type": "MultiPolygon",
            "coordinates": [[_ring(-76.62, -76.60)], [_ring(-76.53, -76.52)]],
        }
        cat = _catalog([_item("G00", -76.56, -76.54), multi])
        geometry = ShardMap.build(cat, hp_grid, backend="mortie")
        fast = ShardMap.build(cat.index_footprints(11), hp_grid, backend="mortie")
        extra = set(fast.shard_keys) - set(geometry.shard_keys)
        assert extra, "the second part must contribute shards geometry never sees"
        assert set(geometry.shard_keys) <= set(fast.shard_keys)
        by_shard = dict(zip(fast.shard_keys, fast.granules, strict=True))
        assert all([g["id"] for g in by_shard[k]] == ["MULTI"] for k in extra)

    def test_order_above_the_moc_cap_is_refused(self):
        # ``_resolve_mortie_order`` clamps a derived order to
        # ``MORTIE_MOC_ORDER_CAP``; an index order is the operator's own number,
        # so clamping it would store a cover at an order other than the one
        # ``footprint_cells_order`` records. Refuse instead, so the two paths
        # agree on what "too fine" means.
        cat = _overlapping_catalog(n=2)
        with pytest.raises(ValueError, match="order 19 is above mortie's coverage cap 18"):
            cat.index_footprints(19)
        assert cat.index_footprints(shardmap.MORTIE_MOC_ORDER_CAP).footprint_cells()[2] == 18

    def test_duplicate_granule_ids_are_refused(self, hp_grid):
        # Alignment is by granule id, and a dict lookup is last-wins: with a
        # repeated id every earlier record carrying it would be handed the last
        # row's footprint. That is silent -- the shard count stays plausible
        # (a disjoint-footprint repro gives 16 shards where geometry gives 25) --
        # so refuse instead of misassigning. The geometry path is unaffected: it
        # reads each record's own ring, so it still builds.
        items = [
            _item("DUP", -76.62, -76.60),
            _item("OTHER", -76.56, -76.54),
            _item("DUP", -76.10, -76.08),
        ]
        cat = _catalog(items)
        assert len(ShardMap.build(cat, hp_grid, backend="mortie").shard_keys) > 0
        with pytest.raises(ValueError, match="duplicate granule ids .*1 repeats over 3 rows"):
            ShardMap.build(cat.index_footprints(11), hp_grid, backend="mortie")

    def test_plain_catalog_still_takes_the_geometry_path(self, hp_grid):
        # No column, so neither key: absent means "this catalog was never
        # indexed", which is a different statement from ``False``.
        sm = ShardMap.build(_overlapping_catalog(), hp_grid, backend="mortie")
        assert "footprint_cells" not in sm.metadata
        assert "footprint_cells_order" not in sm.metadata

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"backend": "spherely"}, id="spherely-backend"),
            pytest.param({"backend": "mortie", "mortie_order": 11}, id="pinned-order"),
        ],
    )
    def test_gates_leave_the_index_unused(self, hp_grid, fake_spherely, kwargs):
        # The index is mortie MOCs covered at the column's own order, so it may
        # only answer a build that asked for exactly that: an exact-S2 spherely
        # run must not be silently swapped for a MOC one (espg/mortie#32), and a
        # caller-pinned ``mortie_order`` asks for a cover the column can't restate.
        # And the manifest must SAY the index sat the build out. The catalog's
        # ``footprint_cells_order`` rides in on the metadata spread either way,
        # so a bare absent key next to it reads as if the index had answered.
        cat = _overlapping_catalog().index_footprints(11)
        meta = ShardMap.build(cat, hp_grid, **kwargs).metadata
        assert meta["footprint_cells"] is False
        assert meta["footprint_cells_order"] == 11

    def test_beams_footprint_leaves_the_index_unused(self):
        # The column covers the CMR swath, not the per-beam corridors (#65).
        hp = HealpixGrid(12, 14, layout="fullsphere")
        cat = _atl03_catalog(
            [_swath_item(gid, lon, 38.89) for gid, lon in (("Ga", -76.52), ("Gb", -76.50))]
        )
        region = [
            (
                np.array([38.74, 38.74, 39.04, 39.04, 38.74]),
                np.array([-76.62, -76.42, -76.42, -76.62, -76.62]),
            )
        ]
        sm = ShardMap.build(
            cat.index_footprints(14), hp, region=region, backend="mortie", footprint="beams"
        )
        assert sm.metadata["footprint_cells"] is False

    def test_aoi_restricts_the_assignment(self, hp_grid):
        # ``moc_and`` against the AOI's own shard MOC is what does the
        # restricting here (there is no searchsorted filter after it), so a
        # region smaller than the catalog must cut shards off the result.
        cat = _overlapping_catalog().index_footprints(11)
        region = [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.59, -76.59, -76.62, -76.62]),
            )
        ]
        wide = ShardMap.build(cat, hp_grid, backend="mortie")
        narrow = ShardMap.build(cat, hp_grid, region=region, backend="mortie")
        assert narrow.metadata["footprint_cells"] is True
        assert 0 < len(narrow.shard_keys) < len(wide.shard_keys)
        assert set(narrow.shard_keys) <= set(wide.shard_keys)
        assert _shard_ids(narrow) == _shard_ids(
            ShardMap.build(_overlapping_catalog(), hp_grid, region=region, backend="mortie")
        )

    def test_batched_cells_equal_the_scalar_loop(self, hp_grid, monkeypatch):
        # Phase 4's parity pin -- the PR 400 measurement comment's in-process
        # assertion made permanent: mortie 0.9.6's ``mocs_and``/``mocs_to_orders``
        # over blocks of records return the exact dict (same keys, same granule
        # lists, same order) as the per-granule scalar loop they replaced. The
        # fixture is California-shaped in miniature: a long overlapping granule
        # chain crossing many shards, an AOI narrower than the catalog (so a
        # good fraction of stored MOCs intersect empty and ride through as
        # zero-width slots), and a dropped non-polygonal row mid-catalog so
        # ``rows`` is not the identity and the block gather walks non-contiguous
        # column spans. Swept block sizes cross the 24-record count both ways:
        # 1, 5 and 7 exercise slot re-basing and the ``own[i]`` owner mapping
        # across many (ragged-tail) blocks, 24 is the one-block case the shipped
        # ``_CELLS_BATCH_RECORDS = 512`` collapses to at this size. The
        # empty-slot assertion below is load-bearing for what it does pin:
        # some record must be unassigned, so the ``mocs_intersect`` prefilter
        # genuinely drops records here and the survivor blocks are cut at
        # *survivor* counts, not record counts. It does **not** pin the owner
        # mapping -- survivors happen to be a contiguous 0-based prefix at both
        # index orders here, so ``own[i] == start + i`` and a block-local
        # mapping passes this test unchanged. The pin for that is
        # ``test_prefilter_edges_...`` below, whose middle-AOI case starts
        # survivors above record 0; the all-empty and all-hit edges live there
        # too.
        #
        # Both column orders run, because they hit ``mocs_to_orders``
        # differently. At index 11 == ``parent_order`` it only ever refines
        # (62 hit cells -> 77 shards here), so nothing collapses and the case
        # cannot tell "the dedup works" from "there were never any repeats".
        # Index 13 is the collapsing shape: 294 hit cells -> the same 77
        # shards, ~26 cells onto ~7 per granule. That is what the dropped
        # per-granule ``np.unique`` (``shardmap.py``) used to absorb, and
        # dropping it is a no-op only because mortie's ``mocs_to_orders``
        # coarsens-and-dedups on densify and returns sorted -- an upstream
        # guarantee this file is now silently dependent on, so pin it here.
        items = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(24)]
        pt = _item("PT", -76.60, -76.58)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.59, 38.89]}
        region = [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.55, -76.55, -76.62, -76.62]),
            )
        ]
        for index_order in (11, 13):
            cat = _catalog(items[:11] + [pt] + items[11:]).index_footprints(index_order)
            records = cat.granule_records()
            all_shards = {
                int(s) for s in hp_grid.coverage(shardmap._region_parts(region, cat.metadata))
            }
            values, offsets, order, rows, considered = shardmap._footprint_cells_plan(
                cat, hp_grid, "mortie", "swath", None
            )
            assert considered == len(records), "the plan must count the records it aligns"
            assert order == index_order, "the plan must carry the column's own order"
            oracle = _intersect_cells_serial(rows, values, offsets, hp_grid, all_shards)
            assigned = {g for v in oracle.values() for g in v}
            assert len(oracle) > 4, "fixture must span multiple shards"
            assert 0 < len(assigned) < len(records), "AOI must leave some slots empty"
            for block in (1, 5, 7, 24):
                monkeypatch.setattr(shardmap, "_CELLS_BATCH_RECORDS", block)
                got = shardmap._intersect_footprint_cells(
                    rows, values, offsets, hp_grid, all_shards
                )
                assert got == oracle, f"order {index_order} block {block} diverged from scalar"

    def test_prefilter_edges_all_empty_all_hit_and_middle_run(self, hp_grid, monkeypatch):
        # The ``mocs_intersect`` prefilter's three edge shapes, each against
        # the scalar oracle and swept across survivor-block boundaries. What
        # has to clear a cell is the *gap* between granules, not the spacing:
        # 0.02-deg-wide footprints every 0.15 deg leave 0.13 deg of empty
        # longitude, which at lat 38.89 is 0.13 * cos(lat) = 0.101 deg of arc
        # against an order-11 cell of ~0.0286 deg -- ~3.5 cells. Both the AOI
        # box and each granule round out by up to a cell, so the middle AOI's
        # coverage still cannot bleed onto its neighbours and the
        # ends-unassigned assertions do not ride on quantization.
        #
        # - all-empty: a disjoint (but non-empty) AOI, so the ``not
        #   all_shards`` short-circuit does NOT fire and the predicate itself
        #   must drop every record before the materializing pass.
        # - all-hit: every record survives, pinned exactly; the survivor
        #   gather is the identity and the result is still the oracle's.
        # - middle run: one contiguous run of survivors (records 4 and 5) with
        #   every record before and after it dropped, so ``surv[0] != 0`` --
        #   the leading-empty edge where a block-local owner mapping
        #   (``start + i`` instead of ``own[i]``) would misassign every hit to
        #   an earlier record and fail the equality.
        items = [_item(f"G{i}", -76.62 + 0.15 * i, -76.60 + 0.15 * i) for i in range(10)]
        cat = _catalog(items).index_footprints(11)
        values, offsets, _order, rows, _considered = shardmap._footprint_cells_plan(
            cat, hp_grid, "mortie", "swath", None
        )

        def box(w, e, s=38.84, n=38.94):
            return [(np.array([s, s, n, n, s]), np.array([w, e, e, w, w]))]

        def run(region, block):
            shards = {
                int(s) for s in hp_grid.coverage(shardmap._region_parts(region, cat.metadata))
            }
            assert shards, "every case must exercise the prefilter, not the empty-AOI gate"
            oracle = _intersect_cells_serial(rows, values, offsets, hp_grid, shards)
            monkeypatch.setattr(shardmap, "_CELLS_BATCH_RECORDS", block)
            got = shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, shards)
            assert got == oracle, f"block {block} diverged from scalar"
            return {g for v in got.values() for g in v}

        for block in (1, 3, 4, 10):
            assert run(box(-76.63, -75.24, s=-40.0, n=-39.9), block) == set()
            assert run(box(-76.63, -75.24), block) == set(range(rows.size))
            # Records 4 and 5 exactly -- a middle run, so ``surv[0] != 0`` and
            # the trailing records are dropped too.
            assert run(box(-76.03, -75.84), block) == {4, 5}

    def test_predicate_overreport_still_returns_empty(self, hp_grid, monkeypatch):
        # The survivor loop's ``flat.size`` guard. mortie documents
        # ``mocs_intersect`` as exact (``hits[i]`` iff ``mocs_and``'s slot ``i``
        # would be non-empty), so in a correct stack every survivor block
        # materializes something and the guard never fires. Simulate that
        # contract drifting -- over-report every row as a hit against a
        # disjoint AOI -- and the answer must still be the oracle's ``{}``:
        # without the guard the all-empty concatenation clears
        # ``_regroup_hits``'s ``if not hit_shards`` gate and dies with an
        # ``IndexError`` inside ``_first_of_run``, which names the wrong
        # function.
        import mortie

        items = [_item(f"G{i}", -76.62 + 0.05 * i, -76.60 + 0.05 * i) for i in range(6)]
        cat = _catalog(items).index_footprints(11)
        values, offsets, _order, rows, _considered = shardmap._footprint_cells_plan(
            cat, hp_grid, "mortie", "swath", None
        )
        far = [
            (
                np.array([-40.0, -40.0, -39.9, -39.9, -40.0]),
                np.array([-76.63, -76.14, -76.14, -76.63, -76.63]),
            )
        ]
        shards = {int(s) for s in hp_grid.coverage(shardmap._region_parts(far, cat.metadata))}
        assert shards, "the disjoint AOI must be non-empty, or the empty-AOI gate answers"
        assert _intersect_cells_serial(rows, values, offsets, hp_grid, shards) == {}
        monkeypatch.setattr(
            mortie, "mocs_intersect", lambda _a, _v, off: np.ones(off.size - 1, bool)
        )
        for block in (1, 4, 6):
            monkeypatch.setattr(shardmap, "_CELLS_BATCH_RECORDS", block)
            assert shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, shards) == {}

    def test_batch_refusal_names_the_record_range(self, hp_grid, monkeypatch):
        # mortie's cell-budget ``ValueError`` names the offending MOC by its
        # index *within the call*, which blocking makes block-local: an
        # un-rebased "MOC 2" out of the second block of 5 is a plausible record
        # index and would send the operator to the wrong granule. The wrapper
        # must re-base it to the block's record range and chain the original.
        import mortie

        cat = _overlapping_catalog().index_footprints(11)
        all_shards = {int(s) for s in hp_grid.coverage(shardmap._region_parts(None, cat.metadata))}
        values, offsets, _order, rows, _considered = shardmap._footprint_cells_plan(
            cat, hp_grid, "mortie", "swath", None
        )
        real, calls = mortie.mocs_to_orders, []

        def boom(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise ValueError("MOC 2 exceeds max_cells")
            return real(*args, **kwargs)

        monkeypatch.setattr(shardmap, "_CELLS_BATCH_RECORDS", 5)
        monkeypatch.setattr(mortie, "mocs_to_orders", boom)
        with pytest.raises(ValueError, match=r"records 5-9") as exc:
            shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, all_shards)
        assert "MOC 2 exceeds max_cells" in str(exc.value)
        assert isinstance(exc.value.__cause__, ValueError)

    def test_batch_refusal_range_reads_the_survivor_owners(self, hp_grid, monkeypatch):
        # The shape that tells the re-based message apart from a block-local
        # one. Above, every record survives the prefilter, so ``own`` is
        # ``arange`` and ``start + i`` prints the same range -- the test cannot
        # see the difference. Here a two-box AOI leaves survivors ``[3, 6]``:
        # not 0-based, not contiguous. The message can only name records 3-6 by
        # reading ``own``, and it must say how many survivors the MOC index
        # counts rather than let "3-6" read as four records in the call.
        import mortie

        items = [_item(f"G{i}", -76.62 + 0.15 * i, -76.60 + 0.15 * i) for i in range(8)]
        cat = _catalog(items).index_footprints(11)
        values, offsets, _order, rows, _considered = shardmap._footprint_cells_plan(
            cat, hp_grid, "mortie", "swath", None
        )

        def box(w, e):
            return [(np.array([38.84, 38.84, 38.94, 38.94, 38.84]), np.array([w, e, e, w, w]))]

        region = box(-76.18, -76.14) + box(-75.73, -75.69)
        shards = {int(s) for s in hp_grid.coverage(shardmap._region_parts(region, cat.metadata))}
        got = shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, shards)
        assert {g for v in got.values() for g in v} == {3, 6}, "survivors must be [3, 6]"

        def boom(*args, **kwargs):
            raise ValueError("MOC 1 exceeds max_cells")

        monkeypatch.setattr(mortie, "mocs_to_orders", boom)
        with pytest.raises(ValueError, match=r"records 3-6") as exc:
            shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, shards)
        assert "2 prefilter survivors" in str(exc.value)
        assert "MOC 1 exceeds max_cells" in str(exc.value)

    def test_cli_index_footprints_indexes_the_saved_catalog(self, tmp_path, monkeypatch):
        # ``--index-footprints`` is how an operator ships a pre-indexed clone,
        # so it must both index the catalog it persists AND have this build take
        # the fast path -- indexing after the write would leave the saved file
        # unindexed, and indexing after the build would waste the pass.
        from zagg.catalog import main, sources

        cat = _overlapping_catalog()

        class _FakeCMR:
            def fetch(self, query, **kw):  # unused args: mirrors the real signature
                return cat

        monkeypatch.setattr(sources, "CMRSource", _FakeCMR)
        cat_out, sm_out = str(tmp_path / "cat.parquet"), str(tmp_path / "sm.json")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "zagg-catalog",
                "--config",
                "tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml",
                "--short-name",
                "ATL03",
                "--start-date",
                "2025-06-01",
                "--end-date",
                "2025-06-02",
                "--bbox=-76.62,38.84,-76.50,38.94",
                "--backend",
                "mortie",
                "--index-footprints",
                "9",
                "--catalog-out",
                cat_out,
                "--output",
                sm_out,
            ],
        )
        main()
        saved = Catalog.from_geoparquet(cat_out)
        assert saved.metadata[sources.FOOTPRINT_CELLS_ORDER] == 9
        assert saved.footprint_cells()[2] == 9
        assert ShardMap.from_json(sm_out).metadata["footprint_cells"] is True

    def test_empty_inputs_short_circuit(self, hp_grid):
        # An empty AOI (or no records) must return ``{}`` the way the geometry
        # path does -- ``_intersect_mortie``'s own ``if flat is None or not
        # all_shards: return {}``, so the two paths agree on the same inputs.
        # Not a mortie workaround: on 0.9.6 ``compress_moc`` and the batch
        # twins all handle zero-length arrays cleanly (a malformed morton
        # *word* is what panics -- see ``_intersect_footprint_cells``).
        cat = _overlapping_catalog().index_footprints(11)
        values, offsets, _ = cat.footprint_cells()
        rows = np.arange(len(cat.granule_records()), dtype=np.int64)
        assert shardmap._intersect_footprint_cells(rows, values, offsets, hp_grid, set()) == {}
        assert shardmap._intersect_footprint_cells(rows[:0], values, offsets, hp_grid, {1}) == {}


def _eager_hit_records(catalog, rows, shard_to_idx):  # noqa: ARG001 (mirror real sig)
    """The pre-#439 shape of the fast path's record step -- the parity oracle.

    Before the inversion, ``build`` called ``granule_records()`` up front and the
    intersection's indices *were* indices into that list. Monkeypatching
    ``_hit_records`` to this restores exactly that: every record decoded, the
    assignment untouched. Anything the shipped path serializes differently is a
    regression in the renumbering, not a difference of opinion about ordering.
    """
    return catalog.granule_records(), shard_to_idx


class TestDeferredRecords:
    """Phase 5: the intersection runs BEFORE any record is decoded (issue #439).

    The measured problem: over the 555,867-granule ATL03 clone against a
    California AOI, ``granule_records()`` was ~25 s of a 29.5 s build and 99.6%
    of what it decoded was then discarded. Inverting the two is only safe if the
    manifest is bit-for-bit what the eager order produced, so the oracle here is
    that order itself (``_eager_hit_records``), and the traps it has to clear are
    the ones the id-alignment used to cover: the row screen, and duplicate ids
    among rows the AOI never sees.
    """

    @pytest.fixture
    def hp_grid(self):
        return HealpixGrid(11, 17, layout="fullsphere")

    @staticmethod
    def _payload(sm, tmp_path, name):
        """The serialized manifest, minus the one key that is a stopwatch."""
        path = str(tmp_path / name)
        sm.to_json(path)
        payload = json.loads(pathlib.Path(path).read_text())
        payload["metadata"].pop("build_wall_s")
        return payload

    def test_serializes_identically_to_the_eager_order(self, hp_grid, tmp_path, monkeypatch):
        # The invariant the whole issue rests on: same catalog, same grid, same
        # AOI -> the same JSON, key for key. Includes a screened row and an AOI
        # narrower than the catalog, so the renumbering is exercised over a
        # non-identity ``rows`` map with most records unassigned.
        items = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(24)]
        pt = _item("PT", -76.60, -76.58)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.59, 38.89]}
        cat = _catalog(items[:11] + [pt] + items[11:]).index_footprints(11)
        region = [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.55, -76.55, -76.62, -76.62]),
            )
        ]
        deferred = ShardMap.build(cat, hp_grid, region=region, backend="mortie")
        monkeypatch.setattr(shardmap, "_hit_records", _eager_hit_records)
        eager = ShardMap.build(cat, hp_grid, region=region, backend="mortie")
        assert deferred.metadata["footprint_cells"] is True
        assert 0 < deferred.metadata["granules_assigned"] < deferred.metadata["total_granules"], (
            "the AOI must discard records, or the inversion is untested here"
        )
        assert self._payload(deferred, tmp_path, "d.json") == self._payload(
            eager, tmp_path, "e.json"
        )

    def test_total_granules_still_counts_the_records_considered(self, hp_grid):
        # ``total_granules`` is provenance: the catalog records CONSIDERED, not
        # the ones assigned. The deferred path never builds that list, so it
        # counts the row screen instead -- and must land on the same number the
        # geometry path reports from ``len(records)``, screened row and all.
        good = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(6)]
        pt = _item("PT", -76.62, -76.60)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.61, 38.89]}
        cat = _catalog(good[:3] + [pt] + good[3:])
        assert len(cat.granule_records()) == 6, "the fixture must screen exactly one row"
        geometry = ShardMap.build(cat, hp_grid, backend="mortie")
        fast = ShardMap.build(cat.index_footprints(11), hp_grid, backend="mortie")
        assert fast.metadata["total_granules"] == geometry.metadata["total_granules"] == 6
        assert fast.metadata["granules_assigned"] == geometry.metadata["granules_assigned"]

    def test_screened_row_inside_the_aoi_is_never_assigned(self, hp_grid):
        # The alignment trap. The column is table-row-ordered, so intersecting
        # it first walks rows ``granule_records`` would have dropped -- and a
        # non-polygonal row sitting INSIDE the AOI cover is the case where that
        # matters: ``index_footprints`` gives it an empty MOC, so it must fall
        # out of the intersection rather than be handed a neighbour's slot.
        # (mortie's coverage would refuse the Point outright, so an unscreened
        # index would not even build.)
        pt = _item("PT", -76.60, -76.58)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.59, 38.89]}
        empty = _item("EMPTY", -76.60, -76.58)
        empty["geometry"] = {"type": "Polygon", "coordinates": []}
        items = [_item("G0", -76.62, -76.56), pt, empty, _item("G1", -76.58, -76.52)]
        cat = _catalog(items).index_footprints(11)
        sm = ShardMap.build(cat, hp_grid, backend="mortie")
        assigned = {g["id"] for shard in sm.granules for g in shard}
        assert assigned == {"G0", "G1"}
        assert sm.metadata["total_granules"] == 2

    def test_null_geometry_refuses_on_both_paths(self, hp_grid, fake_spherely):
        # Predicate parity, the one input where the vectorised screen and the
        # row-wise loop see different things: ``shapely.from_wkb`` maps a null
        # WKB to ``None``, whose ``get_type_id`` is -1 (screened) while
        # ``None.is_empty`` raises. Screening it would let an indexed build drop
        # the granule silently -- and not even count it in ``total_granules`` --
        # while the same catalog on the geometry path still crashed. Both refuse.
        items = [_item("G0", -76.62, -76.60), _item("G1", -76.58, -76.56)]
        with pytest.raises(ValueError, match="null geometry .*first at row 0"):
            _with_null_geometry(_catalog(items), 0).index_footprints(11)
        indexed = _with_null_geometry(_catalog(items).index_footprints(11), 0)
        with pytest.raises(ValueError, match="null geometry .*first at row 0"):
            ShardMap.build(indexed, hp_grid, backend="mortie")
        # The unindexed mortie build now runs the same screen (issue #445), so
        # it refuses by name where it used to die on ``None.is_empty``. Still a
        # refusal, and the record-decoding backends still give the old one --
        # what must never happen is one path dropping the granule in silence.
        with pytest.raises(ValueError, match="null geometry .*first at row 0"):
            ShardMap.build(_with_null_geometry(_catalog(items), 0), hp_grid, backend="mortie")
        with pytest.raises(AttributeError):
            ShardMap.build(_with_null_geometry(_catalog(items), 0), hp_grid, backend="spherely")

    def test_duplicate_ids_outside_the_aoi_still_refuse(self, hp_grid):
        # The duplicate-id refusal is a statement about the catalog, not about
        # this build's hits, so it must fire on rows the intersection would have
        # thrown away. Put both copies of the repeated id far outside the region
        # -- under a hits-only check the build would sail through and the
        # operator would never learn the column cannot be trusted.
        far = dict(_item("DUP", 12.00, 12.02))
        far2 = dict(_item("DUP", 12.10, 12.12))
        cat = _catalog([_item("NEAR", -76.62, -76.60), far, far2])
        region = [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.59, -76.59, -76.62, -76.62]),
            )
        ]
        # The geometry path is unaffected, then and now.
        assert ShardMap.build(cat, hp_grid, region=region, backend="mortie").shard_keys
        # The message must state the mechanism the code implements: alignment is
        # positional, so the refusal is about the catalog's integrity, not about
        # an id lookup going last-wins (that lookup was deleted in #439).
        with pytest.raises(
            ValueError,
            match="duplicate granule ids .*1 repeats over 3 rows.*matched to rows by position",
        ) as exc:
            ShardMap.build(cat.index_footprints(11), hp_grid, region=region, backend="mortie")
        assert "by id" not in str(exc.value)

    def test_order_refusal_precedes_any_record_decode(self, hp_grid, monkeypatch):
        # The order guard now runs before ``granule_records`` rather than after
        # it, which must not change WHAT it says -- and while we are here, pin
        # that the refused build decodes nothing at all (the point of the
        # inversion is that no path to a refusal pays the 25 s).
        cat = _overlapping_catalog().index_footprints(9)
        monkeypatch.setattr(
            Catalog, "granule_records", lambda self: pytest.fail("records must not be decoded")
        )
        with pytest.raises(ValueError, match="order 9, coarser than.*parent_order 11"):
            ShardMap.build(cat, hp_grid, backend="mortie")

    def test_multipolygon_superset_survives_the_inversion(self, hp_grid, pre_445_path):
        # The disclosed divergence (the column covers every part, the record
        # reads the largest part's ring) is a property of the COLUMN, so
        # intersecting first must not narrow it -- the second part's shards
        # still appear, still carrying the granule's own record. Oracle is the
        # pre-#445 records path, since the live one now shares the column's
        # cover (issue #445) and so shares its superset.
        def _ring(lon0, lon1):
            return [[lon0, 38.85], [lon1, 38.85], [lon1, 38.93], [lon0, 38.93], [lon0, 38.85]]

        multi = _item("MULTI", -76.62, -76.60)
        multi["geometry"] = {
            "type": "MultiPolygon",
            "coordinates": [[_ring(-76.62, -76.60)], [_ring(-76.53, -76.52)]],
        }
        cat = _catalog([_item("G00", -76.56, -76.54), multi]).index_footprints(11)
        fast = ShardMap.build(cat, hp_grid, backend="mortie")
        geometry = ShardMap.build(_catalog([_item("G00", -76.56, -76.54), multi]), hp_grid)
        extra = set(fast.shard_keys) - set(geometry.shard_keys)
        assert extra, "the second part must contribute shards geometry never sees"
        by_shard = dict(zip(fast.shard_keys, fast.granules, strict=True))
        assert all([g["id"] for g in by_shard[k]] == ["MULTI"] for k in extra)
        assert all(g["s3"] == "s3://b/MULTI.h5" for k in extra for g in by_shard[k])


class TestLiveCover:
    """An UNINDEXED mortie build covers from WKB too (issue #445).

    Same inversion as #439, applied to the path that had kept the old order:
    cover the geometry column, intersect, then decode records for the hits. The
    oracle is the path it replaces -- ``pre_445_path`` restores the record-first
    ``_intersect_mortie`` build -- and the pin is the serialized manifest, so
    "faster" cannot quietly become "different". The intended divergences (the
    MultiPolygon superset, inherited with the cover; the cover order, now
    ``parent_order``, and the superset it admits at coarse shard orders) are
    pinned as divergences, each on its own.
    """

    @pytest.fixture
    def hp_grid(self):
        # chunk_inner=13 with parent_order=11 -- the shipped ATL03 shape, and
        # the one that separates the two defaults: the records path covers at
        # the chunk order 13, this one at the shard order 11.
        return HealpixGrid(11, 19, layout="fullsphere", chunk_inner=13)

    @staticmethod
    def _payload(sm, tmp_path, name, *, drop=("build_wall_s",)):
        """The serialized manifest, minus the keys that are not the assignment."""
        path = str(tmp_path / name)
        sm.to_json(path)
        payload = json.loads(pathlib.Path(path).read_text())
        for key in drop:
            payload["metadata"].pop(key, None)
        return payload

    def _region(self):
        return [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.55, -76.55, -76.62, -76.62]),
            )
        ]

    def test_serializes_identically_to_the_pre_445_path(self, hp_grid, tmp_path, monkeypatch):
        # The invariant the issue rests on: same catalog, same grid, same AOI ->
        # the same JSON, key for key, on the single-part footprints every CMR
        # ATL03/06 granule has. Includes a screened (non-polygonal) row and an
        # AOI narrower than the catalog, so the ``rows`` map is not the identity
        # and most records go unassigned.
        #
        # It doubles as an in-suite check of the order-sweep invariant quoted in
        # ``_resolve_mortie_order``: the oracle covers at the chunk order 13 and
        # this path at the shard order 11, and the assignment is the same,
        # because a shard map states order-11 membership and nothing finer.
        items = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(24)]
        pt = _item("PT", -76.60, -76.58)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.59, 38.89]}
        cat = _catalog(items[:11] + [pt] + items[11:])
        live = ShardMap.build(cat, hp_grid, region=self._region(), backend="mortie")
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        oracle = ShardMap.build(cat, hp_grid, region=self._region(), backend="mortie")
        assert 0 < live.metadata["granules_assigned"] < live.metadata["total_granules"], (
            "the AOI must discard records, or the inversion is untested here"
        )
        assert live.metadata["mortie_order"] == 11
        assert oracle.metadata["mortie_order"] == 13
        drop = ("build_wall_s", "mortie_order")
        assert self._payload(live, tmp_path, "live.json", drop=drop) == self._payload(
            oracle, tmp_path, "oracle.json", drop=drop
        )

    def test_default_order_is_the_shard_order(self, hp_grid, monkeypatch):
        # espg's ruling (2026-08-16): the map records order-11 shard membership,
        # and the sweep in ``_resolve_mortie_order`` measures granules/shard flat
        # for every order >= parent_order, so covering at the chunk order buys
        # nothing and costs ~2x the MOC words per order. One cover, at 11.
        orders = []
        real = Catalog.cover_footprints
        monkeypatch.setattr(
            Catalog,
            "cover_footprints",
            lambda self, order: (orders.append(order), real(self, order))[1],
        )
        sm = ShardMap.build(_overlapping_catalog(), hp_grid, backend="mortie")
        assert orders == [11]
        assert sm.metadata["mortie_order"] == 11
        assert "footprint_cells" not in sm.metadata, "nothing was persisted"

    def test_coarse_shard_order_covers_a_superset(self, monkeypatch):
        # The order default is flat by MEASUREMENT, not identical by
        # construction: mortie's coverage is conservative per order, so a cover
        # at ``parent_order`` can keep a boundary cell that the chunk-order
        # cover refined down does not -- never the reverse. Every production
        # pair measured flat (9/13, 11/13, 8/12, 9/11: 0/200 rows differing over
        # random polygons), so it takes a coarse grid to see the gap at all:
        # parent 6 against chunk 10, one rectangle straddling an order-6 seam.
        # The assertion is the DIRECTION -- a future change that made the
        # default a subset (dropping a real granule/shard pair) fails here.
        coarse = HealpixGrid(6, 12, layout="fullsphere", chunk_inner=10)
        cat = _catalog([_item("G00", 86.389228, 88.222362, lat0=-61.180344, lat1=-60.706814)])
        region = [
            (
                np.array([-62.0, -62.0, -60.0, -60.0, -62.0]),
                np.array([85.0, 89.0, 89.0, 85.0, 85.0]),
            )
        ]
        live = ShardMap.build(cat, coarse, region=region, backend="mortie")
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        oracle = ShardMap.build(cat, coarse, region=region, backend="mortie")
        assert live.metadata["mortie_order"] == 6
        assert oracle.metadata["mortie_order"] == 10
        assert set(oracle.shard_keys) < set(live.shard_keys), (
            "the coarse cover must be a STRICT superset on this fixture, or it has stopped "
            "reproducing the divergence and no longer pins the direction"
        )
        assert len(live.shard_keys) == 4 and len(oracle.shard_keys) == 3
        by_shard = dict(zip(live.shard_keys, live.granules, strict=True))
        extra = set(live.shard_keys) - set(oracle.shard_keys)
        assert all([g["id"] for g in by_shard[k]] == ["G00"] for k in extra)

    def test_pinned_order_is_honored_and_matches_the_oracle(self, hp_grid, tmp_path, monkeypatch):
        # An explicit order is a request for a cover at that order -- nothing is
        # persisted here, so unlike the stored plan (which refuses a pin it
        # cannot restate) this path simply covers there, and must land the
        # oracle's assignment at the same pin.
        cat = _overlapping_catalog()
        live = ShardMap.build(
            cat, hp_grid, region=self._region(), backend="mortie", mortie_order=13
        )
        assert live.metadata["mortie_order"] == 13
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        oracle = ShardMap.build(
            cat, hp_grid, region=self._region(), backend="mortie", mortie_order=13
        )
        assert self._payload(live, tmp_path, "p.json") == self._payload(oracle, tmp_path, "o.json")

    def test_a_pinned_indexed_build_covers_live_too(self, hp_grid, tmp_path, monkeypatch):
        # The third population whose assignment moves. ``_footprint_cells_plan``
        # declines a pinned order (a persisted column cannot restate one), and
        # since #445 what catches that is THIS plan, not the geometry path -- so
        # an indexed catalog built with ``mortie_order=`` covers from WKB and
        # inherits the MultiPolygon superset, exactly as the unindexed build
        # does. Pinned three ways: same assignment as the unindexed pinned
        # build, a strict superset of the pre-#445 records path, and the
        # ``footprint_cells: False`` metadata unchanged (the stored column
        # really did go unused).
        def _ring(lon0, lon1):
            return [[lon0, 38.85], [lon1, 38.85], [lon1, 38.93], [lon0, 38.93], [lon0, 38.85]]

        def _cat():
            multi = _item("MULTI", -76.62, -76.60)
            multi["geometry"] = {
                "type": "MultiPolygon",
                "coordinates": [[_ring(-76.62, -76.60)], [_ring(-76.53, -76.52)]],
            }
            return _catalog([_item("G00", -76.56, -76.54), multi])

        indexed = ShardMap.build(_cat().index_footprints(11), hp_grid, mortie_order=13)
        unindexed = ShardMap.build(_cat(), hp_grid, backend="mortie", mortie_order=13)
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        records = ShardMap.build(_cat().index_footprints(11), hp_grid, mortie_order=13)
        assert indexed.metadata["footprint_cells"] is False
        assert records.metadata["footprint_cells"] is False
        assert indexed.metadata["mortie_order"] == 13
        # ``footprint_cells_order`` is the CATALOG's own metadata, carried
        # through by the indexed build and legitimately absent from the
        # unindexed one; everything else must match key for key.
        drop = ("build_wall_s", "footprint_cells", "footprint_cells_order")
        assert self._payload(indexed, tmp_path, "i.json", drop=drop) == self._payload(
            unindexed, tmp_path, "u.json", drop=drop
        )
        assert set(records.shard_keys) < set(indexed.shard_keys), (
            "the pinned indexed build must inherit the cover's MultiPolygon superset"
        )
        by_shard = dict(zip(indexed.shard_keys, indexed.granules, strict=True))
        extra = set(indexed.shard_keys) - set(records.shard_keys)
        assert all([g["id"] for g in by_shard[k]] == ["MULTI"] for k in extra)

    def test_pinned_order_coarser_than_the_shards_still_refuses(self, hp_grid):
        # The pin is validated exactly as the records path validates it (#92),
        # and before any cover runs.
        with pytest.raises(ValueError, match="coarser than the grid's parent_order"):
            ShardMap.build(_overlapping_catalog(), hp_grid, backend="mortie", mortie_order=8)

    def test_total_granules_counts_the_screen(self, hp_grid, monkeypatch):
        # ``total_granules`` is the records CONSIDERED. The live path never
        # builds that list, so it counts the row screen -- and must land on the
        # number the records path reports from ``len(records)``, screened row
        # and all.
        good = [_item(f"G{i:02d}", -76.62 + 0.008 * i, -76.60 + 0.008 * i) for i in range(6)]
        pt = _item("PT", -76.62, -76.60)
        pt["geometry"] = {"type": "Point", "coordinates": [-76.61, 38.89]}
        cat = _catalog(good[:3] + [pt] + good[3:])
        assert len(cat.granule_records()) == 6, "the fixture must screen exactly one row"
        live = ShardMap.build(cat, hp_grid, backend="mortie")
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        oracle = ShardMap.build(cat, hp_grid, backend="mortie")
        assert live.metadata["total_granules"] == oracle.metadata["total_granules"] == 6
        assert live.metadata["granules_assigned"] == oracle.metadata["granules_assigned"]

    def test_records_are_decoded_for_the_hits_only(self, hp_grid, monkeypatch):
        # The point of the inversion: an AOI that keeps a handful of granules
        # must not pay to decode the rest (~25 s of a 29.5 s clone-scale build).
        cat = _overlapping_catalog(n=24)
        decoded: list = []
        real = Catalog.granule_records
        monkeypatch.setattr(
            Catalog,
            "granule_records",
            lambda self: (decoded.append(self.table.num_rows), real(self))[1],
        )
        narrow = [
            (
                np.array([38.85, 38.85, 38.93, 38.93, 38.85]),
                np.array([-76.62, -76.60, -76.60, -76.62, -76.62]),
            )
        ]
        sm = ShardMap.build(cat, hp_grid, region=narrow, backend="mortie")
        assigned = sm.metadata["granules_assigned"]
        assert 0 < assigned < 24, "the AOI must discard granules, or nothing is being pinned"
        assert decoded == [assigned], f"decoded {decoded} rows for {assigned} assigned granules"

    def test_multipolygon_is_a_superset_of_the_records_path(self, hp_grid, monkeypatch):
        # The disclosed divergence, now inherited by the live path: ``from_wkbs``
        # covers the union of the parts inside each blob, where
        # ``granule_records`` reads the largest part's exterior ring only. So a
        # multi-part footprint assigns to shards the pre-#445 build never saw --
        # a superset, never a swap, and never for a CMR ATL03/06 granule (all
        # single-part).
        def _ring(lon0, lon1):
            return [[lon0, 38.85], [lon1, 38.85], [lon1, 38.93], [lon0, 38.93], [lon0, 38.85]]

        multi = _item("MULTI", -76.62, -76.60)
        multi["geometry"] = {
            "type": "MultiPolygon",
            "coordinates": [[_ring(-76.62, -76.60)], [_ring(-76.53, -76.52)]],
        }
        cat = _catalog([_item("G00", -76.56, -76.54), multi])
        live = ShardMap.build(cat, hp_grid, backend="mortie")
        monkeypatch.setattr(shardmap, "_live_cells_plan", lambda *a, **k: None)
        oracle = ShardMap.build(cat, hp_grid, backend="mortie")
        extra = set(live.shard_keys) - set(oracle.shard_keys)
        assert extra, "the second part must contribute shards the records path never sees"
        assert set(oracle.shard_keys) <= set(live.shard_keys)
        by_shard = dict(zip(live.shard_keys, live.granules, strict=True))
        assert all([g["id"] for g in by_shard[k]] == ["MULTI"] for k in extra)

    def test_build_wall_spans_the_cover_but_not_the_records(self, hp_grid, monkeypatch):
        # ``build_wall_s`` has always covered the plan and the intersection and
        # never the record decode (issue #439); the ephemeral cover is
        # intersection work, so it lands inside. Pinned with a sleep on each
        # side rather than by reading the code.
        real_cover, real_records = Catalog.cover_footprints, Catalog.granule_records
        monkeypatch.setattr(
            Catalog,
            "cover_footprints",
            lambda self, order: (time.sleep(0.05), real_cover(self, order))[1],
        )
        monkeypatch.setattr(
            Catalog, "granule_records", lambda self: (time.sleep(0.2), real_records(self))[1]
        )
        sm = ShardMap.build(_overlapping_catalog(n=4), hp_grid, backend="mortie")
        assert 0.05 <= sm.metadata["build_wall_s"] < 0.2

    def test_the_budget_refusal_names_a_remedy_that_exists(self, hp_grid, monkeypatch):
        # The cell-budget refusal is shared with the stored path, but its
        # remedy is not: "re-index the catalog / drop the footprint_cells
        # column" names two things an unindexed build does not have, and
        # re-indexing would not move it anyway (the cover is flattened to
        # ``parent_order`` whatever order it ran at). ``build`` knows which
        # cover it holds, so the message must say so.
        import mortie

        cat = _overlapping_catalog(n=2)
        indexed_cat = cat.index_footprints(11)

        def boom(*args, **kwargs):
            raise ValueError("MOC 0 would expand to more than 1048576 cells at order 11")

        monkeypatch.setattr(mortie, "mocs_to_orders", boom)
        with pytest.raises(ValueError) as live:
            ShardMap.build(cat, hp_grid, backend="mortie")
        with pytest.raises(ValueError) as stored:
            ShardMap.build(indexed_cat, hp_grid, backend="mortie")
        assert "footprint cover batch failed" in str(live.value)
        assert "no footprint_cells column to re-cut or drop" in str(live.value)
        assert "coarser parent_order" in str(live.value) and "parent_order 11" in str(live.value)
        assert "Re-index the catalog" not in str(live.value)
        assert "footprint_cells batch failed" in str(stored.value)
        assert "Re-index the catalog at a coarser order" in str(stored.value)

    def test_empty_catalog_builds_an_empty_map(self, hp_grid):
        # ``filter_bbox`` can cut a catalog to nothing, and the records path
        # handled that by returning ``{}`` out of ``_flatten_rings``. The cover
        # has to land in the same place: no blob for ``from_wkbs`` to parse, and
        # an offsets array that is still one entry per (zero) row.
        cat = _overlapping_catalog(n=2)
        empty = Catalog(cat.table.slice(0, 0), dict(cat.metadata))
        sm = ShardMap.build(empty, hp_grid, backend="mortie")
        assert sm.shard_keys == []
        assert sm.metadata["total_granules"] == 0

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"chosen": "spherely"}, id="spherely-backend"),
            pytest.param({"footprint": "beams"}, id="beams-footprint"),
        ],
    )
    def test_gated_off_for_the_paths_that_need_records(self, hp_grid, grid, kwargs):
        # Each exclusion is a path whose geometry the cover does not describe:
        # spherely is exact S2 (a MOC swap would change semantics behind the
        # caller, espg/mortie#32), ``beams`` covers per-beam corridors rather
        # than the CMR swath (#65), and a rectilinear grid has no shard order
        # for the cover to key to. Each must fall through to the records path.
        args = {"chosen": "mortie", "footprint": "swath", **kwargs}
        cat = _overlapping_catalog(n=2)
        gated = shardmap._live_cells_plan(cat, hp_grid, args["chosen"], args["footprint"], None)
        assert gated is None
        assert shardmap._live_cells_plan(cat, grid, "mortie", "swath", None) is None
        assert shardmap._live_cells_plan(cat, hp_grid, "mortie", "swath", None) is not None


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
    components — ``output_fields`` (#89) and, for HEALPix, ``emit_cell_ids``
    (issue #135)."""

    def test_healpix_excludes_output_fields(self):
        g = HealpixGrid(6, 12, layout="fullsphere")
        spatial = g.spatial_signature()
        assert "output_fields" not in spatial
        assert "cell_ids_encoding" not in spatial
        assert g.signature() == {
            **spatial,
            "output_fields": g.signature()["output_fields"],
            # The issue #304 transition hatch is part of the full fingerprint
            # (an extra cell_ids array changes the leaf schema), default off.
            # (The #135 cell_ids_encoding field retired with the knob.)
            "emit_cell_ids": False,
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
        # Recomputed for the derived map, like total_shards/total_pairs.
        assert sm_coarse_reproj.metadata["granules_assigned"] == len(
            {g["id"] for shard in sm_coarse_reproj.granules for g in shard}
        )
        assert (
            sm_coarse_reproj.metadata["granules_assigned"]
            == sm_coarse_direct.metadata["granules_assigned"]
        )

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


# ── paired-asset sibling join (issue #425) ───────────────────────────────────


def _gedi_id(product, core="2019108002012_O01959_01_T03909", release="005", version="V002"):
    return f"{product}_{core}_02_{release}_01_{version}"


class TestSiblingJoinKey:
    def test_siblings_share_key_across_release_fields(self):
        from zagg.catalog.shardmap import sibling_join_key

        l1b = sibling_join_key(_gedi_id("GEDI01_B", release="005"))
        l2a = sibling_join_key(_gedi_id("GEDI02_A", release="003"))
        assert l1b is not None
        assert l1b == l2a

    def test_generation_pins_the_pair(self):
        from zagg.catalog.shardmap import sibling_join_key

        v2 = sibling_join_key(_gedi_id("GEDI01_B", version="V002"))
        v3 = sibling_join_key(_gedi_id("GEDI02_A", version="V003"))
        assert v2 != v3

    def test_distinct_acquisitions_distinct_keys(self):
        from zagg.catalog.shardmap import sibling_join_key

        a = sibling_join_key(_gedi_id("GEDI01_B", core="2019108002012_O01959_01_T03909"))
        b = sibling_join_key(_gedi_id("GEDI01_B", core="2019108002012_O01959_02_T03909"))
        assert a != b

    def test_unkeyed_id_returns_none(self):
        from zagg.catalog.shardmap import sibling_join_key

        assert sibling_join_key("ATL03_20220621190618_13851506_007_01") is None
        assert sibling_join_key("") is None


class TestPairedAssetBuild:
    """ShardMap.build with a sibling catalog: pairing, exclusion, reporting."""

    def _catalogs(self):
        # Primary (L1B): three acquisitions. Sibling (L2A): matches for the
        # first two only; plus one orphan L2A with no primary.
        l1b = _catalog(
            [
                _item(_gedi_id("GEDI01_B", core="2019108002012_O01959_01_T03909"), -76.62, -76.57),
                _item(_gedi_id("GEDI01_B", core="2019115002012_O02059_02_T03910"), -76.55, -76.50),
                _item(_gedi_id("GEDI01_B", core="2019120002012_O02159_03_T03911"), -76.55, -76.52),
            ]
        )
        l2a = _catalog(
            [
                _item(
                    _gedi_id("GEDI02_A", core="2019108002012_O01959_01_T03909", release="003"),
                    -76.62,
                    -76.57,
                ),
                _item(
                    _gedi_id("GEDI02_A", core="2019115002012_O02059_02_T03910", release="003"),
                    -76.55,
                    -76.50,
                ),
                _item(
                    _gedi_id("GEDI02_A", core="2019130002012_O02259_04_T03912", release="003"),
                    -76.55,
                    -76.52,
                ),
            ]
        )
        return l1b, l2a

    def test_paired_entries_carry_sibling_assets(self, grid, fake_spherely):
        from zagg.catalog.shardmap import sibling_join_key

        l1b, l2a = self._catalogs()
        sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        seen = {}
        for shard in sm.granules:
            for rec in shard:
                seen[rec["id"]] = rec
        assert seen, "paired build should assign granules"
        for rec in seen.values():
            assert set(rec) == {"id", "s3", "https", "assets"}
            sib = rec["assets"]["l2a"]
            assert sib["id"].startswith("GEDI02_A_")
            assert sib["s3"] and sib["https"]
            # The sibling is THIS acquisition's, not another's.
            assert sibling_join_key(sib["id"]) == sibling_join_key(rec["id"])

    def test_paired_build_skips_the_stored_index_plan(self):
        """Pairing filters records; the positional fast path must not engage.

        The ``footprint_cells`` plan aligns positionally to the raw catalog
        table (issue #439), which cannot represent a pairing-filtered record
        list (issue #425) -- so a paired build takes the geometry path even on
        an indexed catalog, and still pairs/excludes correctly.
        """
        l1b, l2a = self._catalogs()
        hp = HealpixGrid(11, 17, layout="fullsphere")
        sm = ShardMap.build(l1b.index_footprints(11), hp, sibling_catalog=l2a)
        assert sm.metadata.get("footprint_cells") is not True  # plan skipped
        assigned = {rec["id"] for shard in sm.granules for rec in shard}
        assert assigned  # the paired primaries still assign
        for gid in assigned:  # only paired primaries; the pairless third never appears
            assert not gid.startswith("GEDI01_B_2019120")
        assert {p["id"][:10] for p in sm.metadata["pairless"]} == {"GEDI01_B_2", "GEDI02_A_2"}

    def test_paired_build_skips_the_ephemeral_cover_too(self, monkeypatch):
        """Pairing filters records, so neither cover-first path may engage.

        The ephemeral cover (issue #445) aligns positionally to the raw catalog
        table exactly as the stored column does, so a paired build has to stay
        on the records path for the same reason -- and unlike the stored plan,
        this one engages on an *unindexed* catalog, which is every paired build.
        """
        l1b, l2a = self._catalogs()
        monkeypatch.setattr(
            shardmap,
            "_live_cells_plan",
            lambda *a, **k: pytest.fail("a paired build must not cover-and-intersect"),
        )
        sm = ShardMap.build(l1b, HealpixGrid(11, 17, layout="fullsphere"), sibling_catalog=l2a)
        assert {rec["id"] for shard in sm.granules for rec in shard}

    def test_pairless_excluded_and_reported(self, grid, fake_spherely):
        l1b, l2a = self._catalogs()
        sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assigned = {rec["id"] for shard in sm.granules for rec in shard}
        orphan_l1b = _gedi_id("GEDI01_B", core="2019120002012_O02159_03_T03911")
        orphan_l2a = _gedi_id("GEDI02_A", core="2019130002012_O02259_04_T03912", release="003")
        # The pairless primary never enters the map.
        assert orphan_l1b not in assigned
        # Both directions reported: primary missing its sibling, sibling
        # missing its primary (espg amendment: report, never just count).
        pairless = {p["id"]: p["missing"] for p in sm.metadata["pairless"]}
        assert pairless[orphan_l1b] == "l2a"
        assert pairless[orphan_l2a] == "primary"
        assert sm.metadata["sibling_asset"] == "l2a"

    def test_pairless_warning_fires(self, grid, fake_spherely, caplog):
        import logging as _logging

        l1b, l2a = self._catalogs()
        with caplog.at_level(_logging.WARNING):
            ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assert any("pairless" in r.message for r in caplog.records)

    def test_duplicate_sibling_key_is_deterministic_and_reported(self, grid, fake_spherely):
        # The join key ignores the release/production fields, so two sibling
        # records can share one — last-writer-wins made the surviving record
        # arbitrary and dropped the other in silence (review finding, PR #432).
        core = "2019108002012_O01959_01_T03909"
        l1b = _catalog([_item(_gedi_id("GEDI01_B", core=core), -76.62, -76.57)])
        first = _gedi_id("GEDI02_A", core=core, release="003")
        second = _gedi_id("GEDI02_A", core=core, release="004")
        l2a = _catalog([_item(first, -76.62, -76.57), _item(second, -76.62, -76.57)])
        sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assets = {rec["assets"]["l2a"]["id"] for shard in sm.granules for rec in shard}
        assert assets == {first}  # first in catalog order wins, not "whichever last"
        assert {"id": second, "missing": "duplicate-key"} in sm.metadata["pairless"]

    def test_duplicate_primary_keys_are_kept_but_warned(self, grid, fake_spherely, caplog):
        import logging as _logging

        core = "2019108002012_O01959_01_T03909"
        dup_a = _gedi_id("GEDI01_B", core=core, release="005")
        dup_b = _gedi_id("GEDI01_B", core=core, release="006")
        l1b = _catalog([_item(dup_a, -76.62, -76.57), _item(dup_b, -76.62, -76.57)])
        l2a = _catalog([_item(_gedi_id("GEDI02_A", core=core, release="003"), -76.62, -76.57)])
        with caplog.at_level(_logging.WARNING):
            sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assigned = {rec["id"] for shard in sm.granules for rec in shard}
        assert {dup_a, dup_b} <= assigned  # excluding a primary is destructive
        assert any("share a join key" in r.message for r in caplog.records)

    def test_mostly_unpaired_primary_escalates_the_warning(self, grid, fake_spherely, caplog):
        # A mis-scoped sibling query (different AOI, narrower window, dropped
        # page) is per-granule indistinguishable from a genuinely missing
        # sibling, and exclusion is destructive — so say so in aggregate
        # (review finding, PR #432).
        import logging as _logging

        l1b, l2a = self._catalogs()
        l2a = Catalog(l2a.table.slice(0, 1), l2a.metadata)  # 1 of 3 primaries pair
        with caplog.at_level(_logging.WARNING):
            ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assert any("AOI and time window" in r.message for r in caplog.records)

    def test_mostly_paired_primary_does_not_escalate(self, grid, fake_spherely, caplog):
        import logging as _logging

        l1b, l2a = self._catalogs()  # 2 of 3 primaries pair
        with caplog.at_level(_logging.WARNING):
            ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assert any("pairless" in r.message for r in caplog.records)
        assert not any("AOI and time window" in r.message for r in caplog.records)

    def test_fully_paired_reports_empty_list(self, grid, fake_spherely):
        l1b, l2a = self._catalogs()
        # Trim both catalogs to the two matching acquisitions.
        l1b = Catalog(l1b.table.slice(0, 2), l1b.metadata)
        l2a = Catalog(l2a.table.slice(0, 2), l2a.metadata)
        sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        assert sm.metadata["pairless"] == []

    def test_no_sibling_catalog_no_pairless_key(self, catalog, grid, fake_spherely):
        sm = ShardMap.build(catalog, grid, backend="spherely")
        assert "pairless" not in sm.metadata

    def test_paired_map_round_trips_json(self, grid, fake_spherely, tmp_path):
        l1b, l2a = self._catalogs()
        sm = ShardMap.build(l1b, grid, backend="spherely", sibling_catalog=l2a)
        path = str(tmp_path / "paired.json")
        sm.to_json(path)
        sm2 = ShardMap.from_json(path)
        assert sm2.granules == sm.granules
        assert sm2.metadata["pairless"] == sm.metadata["pairless"]


def _item_under(gid, prefix, lon0, lon1, lat0=38.85, lat1=38.93):
    """``_item`` with the hrefs moved under ``prefix`` -- the shape a per-shard
    basename collision takes: one basename, two key prefixes (issue #468)."""
    item = _item(gid, lon0, lon1, lat0, lat1)
    item["assets"] = {
        "data": {"href": f"https://h/{prefix}/{gid}.h5", "roles": ["data"]},
        "data_s3": {"href": f"s3://b/{prefix}/{gid}.h5", "roles": ["data"]},
    }
    return item


class TestBasenameCollisions:
    """Per-shard granule identity is refused at construction (issue #468).

    Post-#420 a granule is recorded by its driver-stripped basename, so two
    granules of one shard differing only in href prefix collapse onto one
    recorded id. PR #420 question (6) ruled that acceptable *because* the state
    is impossible in every catalog zagg reads; these pin that the impossibility
    is enforced where it is owned rather than assumed.
    """

    @pytest.fixture
    def hp_grid(self):
        return HealpixGrid(11, 17, layout="fullsphere")

    @pytest.fixture
    def fine_grid(self):
        return HealpixGrid(12, 14, layout="fullsphere")

    @pytest.fixture
    def coarse_grid(self):
        return HealpixGrid(11, 14, layout="fullsphere")

    def test_build_refuses_one_basename_under_two_prefixes(self, hp_grid):
        # Same footprint, so both land in every shard the granule touches.
        cat = _catalog(
            [
                _item_under("Gdup", "p1", -76.62, -76.57),
                _item_under("Gdup", "p2", -76.62, -76.57),
            ]
        )
        with pytest.raises(ValueError, match="identity collision"):
            ShardMap.build(cat, hp_grid, backend="mortie")

    def test_refusal_names_the_shard_and_both_hrefs(self, hp_grid):
        cat = _catalog(
            [
                _item_under("Gdup", "p1", -76.62, -76.57),
                _item_under("Gdup", "p2", -76.62, -76.57),
            ]
        )
        with pytest.raises(ValueError) as excinfo:
            ShardMap.build(cat, hp_grid, backend="mortie")
        message = str(excinfo.value)
        assert "s3://b/p1/Gdup.h5" in message and "s3://b/p2/Gdup.h5" in message
        assert "'Gdup.h5'" in message
        # The shard the pair collided in, not just a count of them.
        shards = ShardMap.build(
            _catalog([_item("Gdup", -76.62, -76.57)]), hp_grid, backend="mortie"
        )
        assert f"shard {shards.shard_keys[0]} " in message

    def test_build_refuses_ids_colliding_only_in_basename(self, hp_grid):
        # The other spelling of the same collapse: the catalog ids themselves
        # carry the prefix, so they differ while their basenames do not.
        cat = _catalog(
            [
                _item_under("p1/Gdup", "p1", -76.62, -76.57),
                _item_under("p2/Gdup", "p2", -76.62, -76.57),
            ]
        )
        with pytest.raises(ValueError, match="identity collision"):
            ShardMap.build(cat, hp_grid, backend="mortie")

    def test_distinct_basenames_under_one_prefix_build(self, catalog, hp_grid):
        # Control: the ordinary catalog is unaffected -- the check must refuse
        # a collision, not a shard holding several granules.
        sm = ShardMap.build(catalog, hp_grid, backend="mortie")
        assert max(len(g) for g in sm.granules) > 1
        assert sm.metadata["granules_assigned"] == 3

    def test_the_same_granule_listed_twice_is_not_a_collision(self):
        entry = {"id": "G.h5", "s3": "s3://b/p1/G.h5", "https": "https://h/p1/G.h5"}
        shardmap._refuse_basename_collisions([7], [[entry, dict(entry)]])

    def test_an_entry_with_nothing_to_canonicalize_is_skipped(self):
        # Raster entries carry no href and may carry no id (their identity is
        # the acquisition datetime); nothing to name is nothing to collide.
        # The two entries must DIFFER in their distinguishing fields, else the
        # skip branch could be deleted and this would still pass -- they would
        # dedup rather than collide (issue #468 review finding (3)).
        shardmap._refuse_basename_collisions(
            [7], [[{"id": None, "s3": None, "https": None}, {"id": "", "s3": None, "https": None}]]
        )

    def _colliding_fine_map(self, catalog, fine_grid):
        """A fine map whose two sibling shards each hold one of a colliding
        pair -- legal at the fine order, a collapse once coarsened."""
        from mortie import clip2order

        sm_fine = ShardMap.build(catalog, fine_grid, backend="mortie")
        by_parent: dict = {}
        for k in sm_fine.shard_keys:
            parent = clip2order(11, np.asarray([k], dtype=np.uint64))
            by_parent.setdefault(int(parent[0]), []).append(k)
        siblings = next(ks for ks in by_parent.values() if len(ks) >= 2)[:2]
        granules = [
            [{"id": "Gdup.h5", "s3": f"s3://b/{p}/Gdup.h5", "https": f"https://h/{p}/Gdup.h5"}]
            for p in ("p1", "p2")
        ]
        return ShardMap(sm_fine.grid_signature, siblings, granules, dict(sm_fine.metadata))

    def test_coarsen_refuses_a_collision_the_source_order_did_not_have(
        self, catalog, fine_grid, coarse_grid
    ):
        sm_fine = self._colliding_fine_map(catalog, fine_grid)
        # Legal where it stands: one granule per shard, nothing to collide.
        shardmap._refuse_basename_collisions(sm_fine.shard_keys, sm_fine.granules)
        with pytest.raises(ValueError, match="identity collision"):
            sm_fine.reproject(coarse_grid)

    def test_coarsen_names_both_members_of_the_collided_pair(self, catalog, fine_grid, coarse_grid):
        # The pre-#468 dedup keyed on the id alone, so the second granule
        # overwrote the first and the collapse was unobservable. Both hrefs
        # reaching the message is the observable consequence of the merge
        # keeping both; the keeping itself is pinned directly by
        # test_the_union_keeps_both_members_of_a_collided_pair over in
        # tests/test_sweep.py, on the one union the guard does not raise on.
        sm_fine = self._colliding_fine_map(catalog, fine_grid)
        try:
            sm_fine.reproject(coarse_grid)
        except ValueError as e:
            assert "s3://b/p1/Gdup.h5" in str(e) and "s3://b/p2/Gdup.h5" in str(e)
        else:
            pytest.fail("coarsen must refuse the collided pair")

    def test_two_acquisitions_sharing_an_item_id_collide(self):
        # A raster entry carries no href, and its recorded identity is the item
        # id or the acquisition datetime (``raster_granule_ids``). Two
        # acquisitions sharing an item id record as ONE id, so the datetime is
        # what distinguishes them -- without it this pair read as one granule
        # listed twice and slipped through (issue #468 review finding (1)).
        from zagg.telemetry import raster_granule_ids

        a = {"id": "SCENE", "s3": None, "https": None, "datetime": "2025-06-01T00:00:00Z"}
        b = {"id": "SCENE", "s3": None, "https": None, "datetime": "2025-06-02T00:00:00Z"}
        assert raster_granule_ids([a, b]) == ["SCENE", "SCENE"], "the collapse must be real"
        with pytest.raises(ValueError, match="identity collision"):
            shardmap._refuse_basename_collisions([7], [[a, b]])

    def test_one_acquisition_listed_twice_is_still_not_a_collision(self):
        a = {"id": "SCENE", "s3": None, "https": None, "datetime": "2025-06-01T00:00:00Z"}
        shardmap._refuse_basename_collisions([7], [[a, dict(a)]])

    def test_refine_rebuilds_hrefs_from_the_catalog_so_it_cannot_collide(
        self, catalog, fine_grid, coarse_grid
    ):
        # The refine arm looks each entry up in the catalog by id and rebuilds
        # the entry from THAT record, so a source map's per-entry hrefs never
        # reach the new map: a collided pair arrives as one identical entry and
        # the check has nothing left to see. Pinning it because it bounds what
        # the guard can promise -- only ``build`` and coarsen can surface an
        # href collision (issue #468 review finding (2)).
        sm = ShardMap.build(
            _catalog([_item("Gdup", -76.62, -76.57)]), coarse_grid, backend="mortie"
        )
        cat = _catalog([_item("Gdup", -76.62, -76.57)])
        collided = [
            {"id": "Gdup", "s3": f"s3://b/{p}/Gdup.h5", "https": f"https://h/{p}/Gdup.h5"}
            for p in ("p1", "p2")
        ]
        source = ShardMap(
            sm.grid_signature,
            list(sm.shard_keys),
            [collided] + [list(g) for g in sm.granules[1:]],
            dict(sm.metadata),
        )
        refined = source.reproject(fine_grid, catalog=cat)
        entries = [g for shard in refined.granules for g in shard]
        assert all(g["s3"] == "s3://b/Gdup.h5" for g in entries), "hrefs come from the catalog"
        # One granule out, not two: the prefixes -- and with them the collision
        # -- were discarded upstream of the check, not by it.
        assert {g["id"] for g in entries} == {"Gdup"}

    def test_an_id_that_canonicalizes_to_empty_is_skipped(self):
        # ``canonical_granule_id("/")`` strips the separator down to "", which
        # is falsy but not None -- an ``is None`` guard let it through as a
        # live bucket key and would report ``''`` as the collapsed granule id
        # (issue #468 review finding (3)).
        from zagg.telemetry import canonical_granule_id

        assert canonical_granule_id("/") == "" and canonical_granule_id("//") == ""
        # BOTH must canonicalize to "" and differ in their distinguishing
        # fields: with an ``is None`` guard they share the "" bucket and this
        # raises, which is what makes the test fail against the unfixed code.
        shardmap._refuse_basename_collisions(
            [7], [[{"id": "/", "s3": None, "https": None}, {"id": "//", "s3": None, "https": None}]]
        )

    def test_more_than_three_collisions_are_counted_and_truncated(self):
        # The operator-facing message shows the first three groups and says so;
        # a badly mis-scoped catalog hits this branch, not the single-pair one
        # (issue #468 review finding (4)).
        entries = [
            {"id": f"G{n}.h5", "s3": f"s3://b/{p}/G{n}.h5", "https": None}
            for n in range(4)
            for p in ("p1", "p2")
        ]
        with pytest.raises(ValueError) as excinfo:
            shardmap._refuse_basename_collisions([7], [entries])
        message = str(excinfo.value)
        assert "4 per-shard granule identity collision(s)" in message
        assert message.rstrip().endswith("...")
        # Three groups shown, the fourth only counted.
        assert "'G2.h5'" in message and "'G3.h5'" not in message

    def test_a_collision_with_no_href_is_named_by_what_separates_it(self):
        # The label fallback chain reached the id before the datetime, so a
        # raster pair printed as ['SCENE', 'SCENE'] -- the same string twice,
        # naming nothing -- under a message that asserted a prefix cause the
        # raster path does not have (issue #468 review finding (2)).
        a = {"id": "SCENE", "s3": None, "https": None, "datetime": "2025-06-01T00:00:00Z"}
        b = {"id": "SCENE", "s3": None, "https": None, "datetime": "2025-06-02T00:00:00Z"}
        with pytest.raises(ValueError) as excinfo:
            shardmap._refuse_basename_collisions([7], [[a, b]])
        message = str(excinfo.value)
        assert "SCENE @ 2025-06-01T00:00:00Z" in message
        assert "SCENE @ 2025-06-02T00:00:00Z" in message
        # The prefix cause is offered as the usual case, not asserted as the only one.
        assert "Usually one basename under two key prefixes" in message

    def test_an_href_collision_is_still_named_by_its_hrefs(self):
        a = {"id": "G.h5", "s3": "s3://b/p1/G.h5", "https": "https://h/p1/G.h5"}
        b = {"id": "G.h5", "s3": "s3://b/p2/G.h5", "https": "https://h/p2/G.h5"}
        with pytest.raises(ValueError) as excinfo:
            shardmap._refuse_basename_collisions([7], [[a, b]])
        assert "['s3://b/p1/G.h5', 's3://b/p2/G.h5']" in str(excinfo.value)
