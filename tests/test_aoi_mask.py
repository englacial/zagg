"""Tests for the optional strict-AOI cell mask (issue #101).

Phase 1 covers the HEALPix native-morton MOC path: AOI MOC at ``child_order``,
per-shard restriction, and per-children expansion to a cell-order boolean.
Phase 2 covers the rectilinear shapely cell-center ``contains`` path.
"""

import numpy as np
import pytest

from zagg.grids import HealpixGrid
from zagg.grids.aoi import (
    healpix_aoi_moc,
    healpix_mask_for_children,
    healpix_shard_moc,
)


def _box(lat0, lon0, lat1, lon1):
    """Closed WGS84 box as a single ``[(lats, lons)]`` parts list."""
    lats = np.array([lat0, lat0, lat1, lat1, lat0], dtype=float)
    lons = np.array([lon0, lon1, lon1, lon0, lon0], dtype=float)
    return [(lats, lons)]


class TestHealpixMOCMask:
    def test_mask_matches_moc_to_order_membership(self):
        # The mask over a shard's children must equal: which children fall in the
        # flattened (cell-order) AOI MOC. This is the acceptance criterion.
        from mortie import moc_to_order

        grid = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere")
        parts = _box(10.0, 10.0, 20.0, 20.0)
        aoi_moc = grid.aoi_moc(parts)
        assert aoi_moc.size > 0
        flat = np.unique(np.asarray(moc_to_order(aoi_moc, grid.child_order), dtype=np.uint64))

        # Pick a shard the AOI touches (the parent of an in-AOI cell).
        shard_key = int(grid.shards_of(grid.cells_of(flat[:1]))[0])
        children = np.asarray(grid.children(shard_key), dtype=np.uint64)
        shard_moc = grid.aoi_shard_moc(aoi_moc, shard_key)
        mask = grid.aoi_mask_for_children(shard_moc, children)

        # Oracle: which of THIS shard's children land in the whole-AOI flat cover.
        expected = np.isin(children, flat)
        assert mask.dtype == bool
        assert mask.shape == children.shape
        np.testing.assert_array_equal(mask, expected)
        # moc_and genuinely restricted: the flat cover has cells outside this shard
        # (so the mask is not trivially "all of flat"), yet this shard is in-AOI.
        assert flat.size > int(mask.sum())
        assert mask.any()

    def test_interior_shard_is_all_ones(self):
        # A shard fully inside a large AOI must mask every child True.
        grid = HealpixGrid(parent_order=3, child_order=6, layout="fullsphere")
        parts = _box(-40.0, -40.0, 40.0, 40.0)
        aoi_moc = grid.aoi_moc(parts)
        # A parent cell deep in the interior: resolve a center point to its shard.
        center = grid.shards_of(grid.assign(np.array([0.0]), np.array([0.0])))[0]
        shard_key = int(center)
        children = grid.children(shard_key)
        shard_moc = grid.aoi_shard_moc(aoi_moc, shard_key)
        mask = grid.aoi_mask_for_children(shard_moc, children)
        assert mask.all()

    def test_shard_outside_aoi_is_all_false(self):
        grid = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere")
        parts = _box(10.0, 10.0, 20.0, 20.0)
        aoi_moc = grid.aoi_moc(parts)
        # A far-away shard (opposite hemisphere) has no overlap.
        far = int(grid.shards_of(grid.assign(np.array([-60.0]), np.array([200.0])))[0])
        children = grid.children(far)
        shard_moc = grid.aoi_shard_moc(aoi_moc, far)
        assert shard_moc.size == 0
        mask = grid.aoi_mask_for_children(shard_moc, children)
        assert not mask.any()

    def test_edge_shard_is_mixed(self):
        # A shard straddling the AOI boundary has both in- and out-of-AOI children.
        grid = HealpixGrid(parent_order=5, child_order=9, layout="fullsphere")
        parts = _box(0.0, 0.0, 30.0, 30.0)
        aoi_moc = grid.aoi_moc(parts)
        # Find a shard touched by the AOI that is NOT fully contained: scan the
        # MOC's coarsened parents and pick one whose mask is partial.
        from mortie import clip2order

        # NOTE: clip2order can't refine coarse interior MOC words down to
        # parent_order, so this scanned set is boundary-biased — fine as a source of
        # candidate edge shards here, but not a complete in-AOI shard list.
        parents = np.unique(np.asarray(clip2order(grid.parent_order, aoi_moc)))
        found_mixed = False
        for p in parents.tolist():
            children = grid.children(int(p))
            shard_moc = grid.aoi_shard_moc(aoi_moc, int(p))
            mask = grid.aoi_mask_for_children(shard_moc, children)
            if mask.any() and not mask.all():
                found_mixed = True
                break
        assert found_mixed, "expected at least one boundary shard with a mixed mask"

    def test_helpers_match_grid_methods(self):
        # The free functions and the grid-method wrappers agree.
        grid = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere")
        parts = _box(5.0, 5.0, 25.0, 25.0)
        moc_a = grid.aoi_moc(parts)
        moc_b = healpix_aoi_moc(parts, grid.child_order)
        np.testing.assert_array_equal(np.sort(moc_a), np.sort(moc_b))
        shard = int(
            grid.shards_of(grid.cells_of(grid.assign(np.array([15.0]), np.array([15.0]))))[0]
        )
        children = grid.children(shard)
        sm_a = grid.aoi_shard_moc(moc_a, shard)
        sm_b = healpix_shard_moc(moc_b, shard)
        np.testing.assert_array_equal(np.sort(sm_a), np.sort(sm_b))
        np.testing.assert_array_equal(
            grid.aoi_mask_for_children(sm_a, children),
            healpix_mask_for_children(sm_b, children, grid.child_order),
        )

    def test_child_order_above_18_does_not_raise(self):
        # 0.8.2 lifted the MOC cap to 29: a child_order > 18 must build a real MOC
        # (an order-18-capped build would raise). Keep the AOI small so it's fast.
        grid = HealpixGrid(parent_order=10, child_order=20, layout="fullsphere")
        parts = _box(10.0, 10.0, 10.05, 10.05)
        aoi_moc = grid.aoi_moc(parts)
        assert aoi_moc.size > 0


def test_mortie_version_asserted(monkeypatch):
    import mortie

    from zagg.grids import aoi

    for bad in ("0.8.1", "0.8.2.dev1", "0.7.2"):
        monkeypatch.setattr(mortie, "__version__", bad, raising=False)
        with pytest.raises(RuntimeError, match="aoi_mask requires mortie"):
            aoi._assert_mortie_version()
    for ok in ("0.8.2", "0.8.3", "0.9.0", "1.0.0"):
        monkeypatch.setattr(mortie, "__version__", ok, raising=False)
        aoi._assert_mortie_version()  # no raise


class TestRectilinearMask:
    def _grid(self):
        from zagg.grids import RectilinearGrid

        # A 20x20-cell grid at 1000 m resolution in a metres CRS: cell centers fall
        # on 500, 1500, ... (xmin=ymin=0). 4x4 shard tiles -> 5x5 shards.
        return RectilinearGrid(
            crs="EPSG:3413",
            resolution=1000.0,
            bounds=[0.0, 0.0, 20000.0, 20000.0],
            chunk_shape=(4, 4),
        )

    def test_mask_matches_center_contains(self):
        from shapely.geometry import Point

        grid = self._grid()
        # AOI: a WGS84 box; reproject via the grid path and test each child center.
        parts = _box(60.0, -40.0, 75.0, -20.0)
        aoi_geom = grid.aoi_polygon(parts)
        shard = 0
        children = grid.children(shard)
        mask = grid.aoi_mask_for_children(aoi_geom, children)
        xs, ys = grid.cell_centers(children)
        expected = np.fromiter(
            (aoi_geom.contains(Point(x, y)) for x, y in zip(xs, ys)),
            dtype=bool,
            count=len(children),
        )
        assert mask.dtype == bool
        assert mask.shape == np.asarray(children).shape
        np.testing.assert_array_equal(mask, expected)

    def test_reproject_independent_oracle(self):
        # Cross-check the WGS84->grid reprojection path against an INDEPENDENT
        # oracle: reproject each grid-CRS cell center back to WGS84 with pyproj and
        # test membership in the original WGS84 box directly (no shapely contains on
        # the grid-CRS geometry). The two must agree, so this checks the containment
        # result, not just prep-vs-unprep or ordering (review).
        from pyproj import Transformer

        from zagg.grids import RectilinearGrid

        # A mid-latitude UTM grid (not polar) so a WGS84 lat/lon box is a clean
        # oracle. EPSG:32618 over a 20-km tile near the SERC bbox (#100).
        grid = RectilinearGrid(
            "EPSG:32618",
            1000.0,
            [359400, 4300740, 379400, 4320740],
            chunk_shape=(20, 20),
            config=None,
        )
        # AOI: a WGS84 box that cuts diagonally across the tile's footprint.
        to_wgs = Transformer.from_crs(grid.crs, "EPSG:4326", always_xy=True)
        cx, cy = grid.cell_centers(grid.children(0))
        clon0, clat0 = to_wgs.transform(cx, cy)
        # Box covering the lower-left ~half of the cell-center cloud.
        lon0, lat0 = float(clon0.min()) - 0.01, float(clat0.min()) - 0.01
        lon1, lat1 = float(np.median(clon0)), float(np.median(clat0))
        aoi_geom = grid.aoi_polygon(_box(lat0, lon0, lat1, lon1))
        children = grid.children(0)
        mask = grid.aoi_mask_for_children(aoi_geom, children)

        xs, ys = grid.cell_centers(children)
        clon, clat = to_wgs.transform(xs, ys)
        oracle = (clon >= lon0) & (clon <= lon1) & (clat >= lat0) & (clat <= lat1)
        # Straight-chord vs geodesic edge is sub-cell at 1 km / mid-latitude UTM,
        # so the two oracles agree exactly here. Assert exact agreement so a real
        # containment regression (tile shift, boundary-handling change) can't hide
        # under a loose threshold.
        agree = mask == oracle
        assert agree.all()
        # Non-degenerate: the AOI genuinely cuts this shard (independent of contains).
        assert mask.any() and not mask.all()

    def test_aoi_ring_is_densified_before_reproject(self):
        # issue #101: rectilinear_aoi_polygon densifies the WGS84 ring before
        # to_crs (resolution="auto") so AOI edges follow the geodesic instead of
        # collapsing to straight chords in a polar CRS. A 4-corner box reprojected
        # WITHOUT densification keeps ~5 ring vertices; the densified path inserts
        # many. In a curving CRS (EPSG:3413) the two polygons genuinely differ.
        from odc.geo.geom import polygon as odc_polygon

        from zagg.grids.aoi import rectilinear_aoi_polygon

        grid = self._grid()
        parts = _box(60.0, -40.0, 75.0, -20.0)
        densified = rectilinear_aoi_polygon(parts, grid.crs)

        # Same parts, reprojected as straight chords (no densification) -- the old
        # behavior, built here directly as the baseline.
        lats, lons = parts[0]
        ring = [(float(x), float(y)) for x, y in zip(lons, lats)]
        chord = odc_polygon(ring, crs="EPSG:4326").to_crs(grid.crs).geom

        n_dense = len(densified.exterior.coords)
        n_chord = len(chord.exterior.coords)
        assert n_dense > n_chord  # densification inserted edge vertices
        assert n_chord <= 5  # the raw box is just its corners
        # The geodesic-following polygon is not the chord polygon: in a polar CRS
        # the curved edges enclose a measurably different area.
        assert not densified.equals(chord)
        assert abs(densified.area - chord.area) / chord.area > 1e-4

    def test_fully_inside_aoi_all_true(self):
        # An AOI polygon directly in grid CRS covering the whole grid -> all True.
        grid = self._grid()
        # Build a generous WGS84 polygon by reprojecting the grid bounds back out:
        # instead, reproject a grid-CRS box. Use cell-center membership against an
        # explicit grid-CRS rectangle that contains every center.
        from shapely.geometry import box as shapely_box

        aoi_geom = shapely_box(-1000.0, -1000.0, 21000.0, 21000.0)
        for shard in (0, 1, grid.n_col_blocks):
            children = grid.children(shard)
            mask = grid.aoi_mask_for_children(aoi_geom, children)
            assert mask.all()

    def test_outside_aoi_all_false(self):
        from shapely.geometry import box as shapely_box

        grid = self._grid()
        # A grid-CRS rectangle far from any cell center -> no cell inside.
        aoi_geom = shapely_box(-50000.0, -50000.0, -40000.0, -40000.0)
        children = grid.children(0)
        mask = grid.aoi_mask_for_children(aoi_geom, children)
        assert not mask.any()

    def test_partial_aoi_exact_pattern(self):
        from shapely.geometry import box as shapely_box

        grid = self._grid()
        # The 0,0 shard tile is 4x4 cells, col x-centers 500/1500/2500/3500. A box
        # x in [-1000, 2000] keeps only cols 0,1 (centers 500,1500); cols 2,3 are
        # out. This pins the exact column pattern, so a row/col swap in cell_centers
        # would change the mask and fail (review #5).
        aoi_geom = shapely_box(-1000.0, -1000.0, 2000.0, 21000.0)
        children = grid.children(0)
        mask = grid.aoi_mask_for_children(aoi_geom, children).reshape(4, 4)
        expected = np.zeros((4, 4), dtype=bool)
        expected[:, :2] = True  # cols 0,1 in; cols 2,3 out — for every row
        np.testing.assert_array_equal(mask, expected)

    def test_cell_centers_roundtrip(self):
        grid = self._grid()
        children = grid.children(0)
        xs, ys = grid.cell_centers(children)
        # First child is leaf 0 -> row 0, col 0 -> center (xmin+0.5*res, ymax-0.5*res).
        assert xs[0] == grid.xmin + 0.5 * grid.res_x
        assert ys[0] == grid.ymax - 0.5 * grid.res_y


# ── Phase 3: config + schema + manifest + worker write ───────────────────────


def _cfg_with_aoi(name, on):
    from zagg.config import default_config

    cfg = default_config(name)
    if on:
        cfg.output = {**cfg.output, "aoi_mask": True}
    return cfg


class TestConfigFlag:
    def test_default_off(self):
        from zagg.config import default_config, get_aoi_mask

        assert get_aoi_mask(default_config("atl06")) is False

    def test_accessor_true(self):
        from zagg.config import get_aoi_mask

        assert get_aoi_mask(_cfg_with_aoi("atl06", on=True)) is True

    def test_validate_rejects_non_bool(self):
        from zagg.config import default_config, validate_config

        cfg = default_config("atl06")
        cfg.output = {**cfg.output, "aoi_mask": "yes"}
        with pytest.raises(ValueError, match="output.aoi_mask must be a boolean"):
            validate_config(cfg)


class TestSchemaArray:
    def test_healpix_array_only_when_on(self):
        on = HealpixGrid(4, 6, layout="fullsphere", config=_cfg_with_aoi("atl06", on=True))
        off = HealpixGrid(4, 6, layout="fullsphere", config=_cfg_with_aoi("atl06", on=False))
        assert "aoi_mask" in on.spec().members
        assert "aoi_mask" not in off.spec().members
        assert str(on.spec().members["aoi_mask"].data_type) == "bool"

    def test_rectilinear_array_only_when_on(self):
        from zagg.grids import RectilinearGrid

        kw = dict(crs="EPSG:3413", resolution=1000.0, bounds=[0, 0, 4000, 4000], chunk_shape=(4, 4))
        on = RectilinearGrid(config=_cfg_with_aoi("atl06", on=True), **kw)
        off = RectilinearGrid(config=_cfg_with_aoi("atl06", on=False), **kw)
        assert "aoi_mask" in on._spec().members
        assert "aoi_mask" not in off._spec().members


class TestManifestRoundtrip:
    def test_json_roundtrip_carries_aoi(self, tmp_path):
        from zagg.catalog.shardmap import ShardMap

        sm = ShardMap(
            grid_signature={"type": "healpix"},
            shard_keys=[10, 20],
            granules=[[], []],
            metadata={"aoi_mask": True},
            aoi_mask=[[111, 222], [333]],
        )
        path = str(tmp_path / "m.json")
        sm.to_json(path)
        back = ShardMap.from_json(path)
        assert back.aoi_mask == [[111, 222], [333]]

    def test_json_omits_aoi_when_off(self, tmp_path):
        import json

        from zagg.catalog.shardmap import ShardMap

        sm = ShardMap({"type": "healpix"}, [10], [[]], {})
        path = str(tmp_path / "m.json")
        sm.to_json(path)
        assert "aoi_mask" not in json.loads(open(path).read())
        assert ShardMap.from_json(path).aoi_mask is None


class TestBuildOutputColumn:
    def test_aoi_column_appended(self):
        from zagg.processing.write import _build_output

        grid = HealpixGrid(4, 6, layout="fullsphere", config=_cfg_with_aoi("atl06", on=True))
        from zagg.config import get_data_vars

        data_vars = get_data_vars(grid.config)
        shard = int(grid.shards_of(grid.assign(np.array([15.0]), np.array([15.0])))[0])
        children = grid.children(shard)
        n = len(children)
        stats = {v: np.zeros(n, dtype="float32") for v in data_vars}
        mask = np.zeros(n, dtype=bool)
        mask[: n // 2] = True
        df = _build_output(
            stats,
            data_vars,
            {},
            grid,
            shard,
            use_arrow=False,
            aoi_mask=mask,
        )
        assert "aoi_mask" in df.columns
        assert df["aoi_mask"].dtype == bool
        np.testing.assert_array_equal(df["aoi_mask"].values, mask)

    def test_no_column_when_payload_none(self):
        from zagg.config import get_data_vars
        from zagg.processing.write import _build_output

        grid = HealpixGrid(4, 6, layout="fullsphere", config=_cfg_with_aoi("atl06", on=False))
        data_vars = get_data_vars(grid.config)
        shard = int(grid.shards_of(grid.assign(np.array([15.0]), np.array([15.0])))[0])
        n = len(grid.children(shard))
        stats = {v: np.zeros(n, dtype="float32") for v in data_vars}
        df = _build_output(stats, data_vars, {}, grid, shard, use_arrow=False, aoi_mask=None)
        assert "aoi_mask" not in df.columns


class TestPayloadExpansion:
    def test_healpix_payload_roundtrips_through_json_ints(self):
        # The manifest carries MOC words as plain Python ints; expansion from that
        # list must match expansion from the uint64 sub-MOC.
        grid = HealpixGrid(4, 8, layout="fullsphere")
        parts = _box(10.0, 10.0, 20.0, 20.0)
        aoi_moc = grid.aoi_moc(parts)
        shard = int(
            grid.shards_of(grid.cells_of(grid.assign(np.array([15.0]), np.array([15.0]))))[0]
        )
        children = grid.children(shard)
        sub = grid.aoi_shard_moc(aoi_moc, shard)
        payload = [int(w) for w in sub]  # JSON form
        np.testing.assert_array_equal(
            grid.aoi_mask_from_payload(payload, children),
            grid.aoi_mask_for_children(sub, children),
        )

    def test_rectilinear_payload_is_cell_id_membership(self):
        from zagg.grids import RectilinearGrid

        grid = RectilinearGrid(
            crs="EPSG:3413",
            resolution=1000.0,
            bounds=[0.0, 0.0, 20000.0, 20000.0],
            chunk_shape=(4, 4),
        )
        children = grid.children(0)
        true_ids = [int(children[0]), int(children[5])]
        mask = grid.aoi_mask_from_payload(true_ids, children)
        expected = np.isin(np.asarray(children), true_ids)
        np.testing.assert_array_equal(mask, expected)

    def test_empty_payload_is_all_false(self):
        grid = HealpixGrid(4, 6, layout="fullsphere")
        shard = int(grid.shards_of(grid.assign(np.array([15.0]), np.array([15.0])))[0])
        children = grid.children(shard)
        np.testing.assert_array_equal(
            grid.aoi_mask_from_payload([], children),
            np.zeros(len(children), dtype=bool),
        )


def test_runner_aoi_payload_map():
    from zagg.runner import _aoi_payload_map

    cat = {"shard_keys": [10, 20], "granules": [[], []], "aoi_mask": [[1, 2], [3]]}
    assert _aoi_payload_map(cat) == {10: [1, 2], 20: [3]}
    # No aoi_mask key (flag off at build) -> empty map, no column appended.
    assert _aoi_payload_map({"shard_keys": [10], "granules": [[]]}) == {}


class TestEndToEndWrite:
    def test_write_and_read_aoi_array(self, mock_dataframe_factory):
        from zarr import open_group
        from zarr.storage import MemoryStore

        from zagg.processing.write import _build_output, write_dataframe_to_zarr

        parent_order, child_order = 6, 8
        cfg = _cfg_with_aoi("atl06", on=True)
        from zagg.config import get_data_vars

        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        df = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)
        n = len(df)
        mask = np.zeros(n, dtype=bool)
        mask[: n // 3] = True
        # Rebuild the carrier through _build_output so the aoi_mask column rides the
        # same coords seam the worker uses.
        data_vars = get_data_vars(cfg)
        stats = {v: df[v].values for v in data_vars}
        carrier = _build_output(
            stats,
            data_vars,
            {},
            grid,
            None,
            use_arrow=False,
            children=df["morton"].values,
            aoi_mask=mask,
        )
        n_children = 4 ** (child_order - parent_order)
        chunk_idx = (int(df["cell_ids"].min()) // n_children,)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        group = open_group(store=store, mode="r", path=str(child_order))
        lo, hi = int(df["cell_ids"].min()), int(df["cell_ids"].max())
        actual = group["aoi_mask"][lo : hi + 1]
        assert actual.dtype == np.bool_
        np.testing.assert_array_equal(actual, mask)

    def test_flag_off_store_byte_identical(self, mock_dataframe_factory):
        # The flag-off template + write must match a config that never had the
        # feature, key-for-key and byte-for-byte (the "package, don't clip" guarantee).
        from zarr.storage import MemoryStore

        from zagg.processing.write import write_dataframe_to_zarr

        parent_order, child_order = 6, 8

        def build_store(cfg):
            np.random.seed(0)  # identical mock data on both builds
            grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
            store = MemoryStore()
            grid.emit_template(store)
            df = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)
            n_children = 4 ** (child_order - parent_order)
            chunk_idx = (int(df["cell_ids"].min()) // n_children,)
            write_dataframe_to_zarr(df, store, grid=grid, chunk_idx=chunk_idx)
            return store

        from zagg.config import default_config

        baseline = build_store(default_config("atl06"))
        flag_off = build_store(_cfg_with_aoi("atl06", on=False))

        bkeys = sorted(baseline._store_dict.keys())
        fkeys = sorted(flag_off._store_dict.keys())
        assert bkeys == fkeys
        assert not any("aoi_mask" in k for k in fkeys)
        for k in bkeys:
            b = baseline._store_dict[k].to_bytes()
            f = flag_off._store_dict[k].to_bytes()
            assert b == f, k
