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

        # Pick a shard that the AOI touches.
        shards = np.unique(grid.shards_of(grid.assign(np.array([15.0]), np.array([15.0]))))
        shard_key = int(grid.shards_of(grid.cells_of(flat[:1]))[0])
        children = grid.children(shard_key)
        shard_moc = grid.aoi_shard_moc(aoi_moc, shard_key)
        mask = grid.aoi_mask_for_children(shard_moc, children)

        expected = np.isin(np.asarray(children, dtype=np.uint64), flat)
        assert mask.dtype == bool
        assert mask.shape == np.asarray(children).shape
        np.testing.assert_array_equal(mask, expected)
        assert mask.any()  # the chosen shard is in-AOI
        assert len(shards)  # sanity: assign resolved

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

    monkeypatch.setattr(mortie, "__version__", "0.8.1", raising=False)
    with pytest.raises(RuntimeError, match="aoi_mask requires mortie"):
        aoi._assert_mortie_version()
    monkeypatch.setattr(mortie, "__version__", "0.8.2", raising=False)
    aoi._assert_mortie_version()  # no raise


class TestRectilinearMask:
    def _grid(self):
        from zagg.grids import RectilinearGrid

        # A 20x20 m grid in a metres CRS; cell centers fall on (5,15,...)+10k offsets.
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

    def test_partial_aoi_is_mixed(self):
        from shapely.geometry import box as shapely_box

        grid = self._grid()
        # A grid-CRS rectangle covering only the left half of the 0,0 shard tile.
        aoi_geom = shapely_box(-1000.0, -1000.0, 2000.0, 21000.0)
        children = grid.children(0)  # 4x4 tile, cols 0..3 at x-centers 500..3500
        mask = grid.aoi_mask_for_children(aoi_geom, children)
        assert mask.any() and not mask.all()

    def test_cell_centers_roundtrip(self):
        grid = self._grid()
        children = grid.children(0)
        xs, ys = grid.cell_centers(children)
        # First child is leaf 0 -> row 0, col 0 -> center (xmin+0.5*res, ymax-0.5*res).
        assert xs[0] == grid.xmin + 0.5 * grid.res_x
        assert ys[0] == grid.ymax - 0.5 * grid.res_y
