"""Tests for RectilinearGrid."""

import numpy as np
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from zagg.config import default_config
from zagg.grids import OOB_SENTINEL, InconsistentShardError, RectilinearGrid, from_config


@pytest.fixture
def cfg():
    return default_config("atl06_polar")


@pytest.fixture
def grid(cfg):
    return RectilinearGrid(
        crs="EPSG:3031",
        resolution=5000,
        bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
        chunk_shape=(256, 256),
        config=cfg,
    )


class TestConstruction:
    def test_basic(self, grid):
        assert grid.width == 1280  # 6.4 Mm / 5 km
        assert grid.height == 1280
        assert grid.n_row_blocks == 5
        assert grid.n_col_blocks == 5
        assert grid.array_shape == (1280, 1280)
        assert grid.chunk_shape == (256, 256)

    def test_uneven_chunk_rejected(self, cfg):
        with pytest.raises(ValueError, match="chunk_shape.*must divide"):
            RectilinearGrid(
                crs="EPSG:3031",
                resolution=5000,
                bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
                chunk_shape=(300, 300),
                config=cfg,
            )

    def test_invalid_bounds(self, cfg):
        with pytest.raises(ValueError, match="bounds"):
            RectilinearGrid(
                crs="EPSG:3031", resolution=5000,
                bounds=(0, 0, -100, -100), chunk_shape=(2, 2), config=cfg,
            )

    def test_scalar_vs_tuple_resolution(self, cfg):
        g1 = RectilinearGrid(
            crs="EPSG:3031", resolution=5000,
            bounds=(0, 0, 10000, 10000), chunk_shape=(2, 2), config=cfg,
        )
        g2 = RectilinearGrid(
            crs="EPSG:3031", resolution=[5000, 5000],
            bounds=(0, 0, 10000, 10000), chunk_shape=(2, 2), config=cfg,
        )
        assert g1.array_shape == g2.array_shape


class TestAssign:
    def test_south_pole_to_origin(self, grid):
        # South pole in EPSG:3031 is at (0, 0); grid origin is (xmin, ymax).
        # The pole sits at the centre of the grid, near cell (height/2, width/2).
        ids = grid.assign(np.array([-90.0]), np.array([0.0]))
        assert ids[0] != OOB_SENTINEL
        row = ids[0] // grid.width
        col = ids[0] % grid.width
        assert row == grid.height // 2
        assert col == grid.width // 2

    def test_oob_returns_sentinel(self, grid):
        # Northern hemisphere → way outside the Antarctic polar-stereo bounds.
        ids = grid.assign(np.array([60.0]), np.array([0.0]))
        assert ids[0] == OOB_SENTINEL

    def test_assign_returns_int64(self, grid):
        ids = grid.assign(np.array([-78.5]), np.array([-132.0]))
        assert ids.dtype == np.int64


class TestShardOf:
    def test_same_block(self, grid):
        # Two nearby points fall in the same chunk.
        lats = np.array([-78.5, -78.51])
        lons = np.array([-132.0, -132.01])
        leaves = grid.assign(lats, lons)
        shard = grid.shard_of(leaves)
        assert isinstance(shard, int)

    def test_split_shards_raises(self, grid):
        # Two far-apart points fall in different chunks.
        lats = np.array([-78.5, -82.0])
        lons = np.array([-132.0, 45.0])
        leaves = grid.assign(lats, lons)
        # Sanity: actually different shards
        s = grid.shards_of(leaves)
        if s[0] != s[1]:
            with pytest.raises(InconsistentShardError):
                grid.shard_of(leaves)

    def test_shards_of_vectorized(self, grid):
        lats = np.array([-78.5, -78.51, -82.0])
        lons = np.array([-132.0, -132.01, 45.0])
        shards = grid.shards_of(grid.assign(lats, lons))
        assert shards.shape == (3,)
        assert shards.dtype == np.int64

    def test_oob_leaves_get_sentinel(self, grid):
        leaves = np.array([OOB_SENTINEL, OOB_SENTINEL])
        shards = grid.shards_of(leaves)
        assert (shards == OOB_SENTINEL).all()


class TestBlockIndex:
    def test_pack_unpack_round_trip(self, grid):
        for rb in [0, 2, 4]:
            for cb in [0, 1, 4]:
                packed = grid._pack(rb, cb)
                assert grid.block_index(packed) == (rb, cb)

    def test_block_index_in_range(self, grid):
        for shard in range(grid.n_row_blocks * grid.n_col_blocks):
            rb, cb = grid.block_index(shard)
            assert 0 <= rb < grid.n_row_blocks
            assert 0 <= cb < grid.n_col_blocks


class TestChildren:
    def test_children_count(self, grid):
        children = grid.children(0)
        assert len(children) == grid.chunk_h * grid.chunk_w

    def test_children_in_block(self, grid):
        # All children of shard 0 should map back to shard 0.
        children = grid.children(0)
        shards = grid.shards_of(children)
        assert (shards == 0).all()

    def test_children_consistent(self, grid):
        # Same for an interior shard.
        shard = grid._pack(2, 3)
        children = grid.children(shard)
        shards = grid.shards_of(children)
        assert (shards == shard).all()


class TestEmitTemplate:
    def test_template_shape_and_chunks(self, grid):
        store = MemoryStore()
        grid.emit_template(store)
        group = open_group(store, path=grid.group_path, mode="r")
        for name in ("count", "h_mean"):
            assert group[name].shape == grid.array_shape
            assert group[name].chunks == grid.chunk_shape

    def test_coord_arrays_present(self, grid):
        store = MemoryStore()
        grid.emit_template(store)
        group = open_group(store, path=grid.group_path, mode="r")
        assert group["x"].shape == (grid.width,)
        assert group["y"].shape == (grid.height,)

    def test_crs_in_attrs(self, grid):
        store = MemoryStore()
        grid.emit_template(store)
        group = open_group(store, path=grid.group_path, mode="r")
        assert group.attrs["crs"] == "EPSG:3031"


class TestShardFootprint:
    def test_footprint_is_polygon(self, grid):
        footprint = grid.shard_footprint(grid._pack(2, 2))
        assert footprint.geom_type == "Polygon"
        assert footprint.area > 0


class TestFromConfig:
    def test_dispatches_to_rectilinear(self, cfg):
        g = from_config(cfg)
        assert isinstance(g, RectilinearGrid)
        assert g.crs == "EPSG:3031"

    def test_missing_required_field_raises(self):
        cfg = default_config("atl06_polar")
        del cfg.output["grid"]["bounds"]
        with pytest.raises(ValueError, match="missing required"):
            from_config(cfg)


class TestCoverage:
    def test_coverage_contains_point_shard(self, grid):
        # The shard a point assigns to must appear in the coverage of a small
        # polygon around that point (coverage and assign agree).
        lat, lon = -80.0, 10.0
        shard = int(grid.shards_of(grid.assign(np.array([lat]), np.array([lon])))[0])
        d = 0.5
        lats = np.array([lat - d, lat - d, lat + d, lat + d, lat - d])
        lons = np.array([lon - d, lon + d, lon + d, lon - d, lon - d])
        shards = grid.coverage([(lats, lons)])
        assert shards.dtype == np.int64
        assert shard in shards.tolist()

    def test_coverage_in_range(self, grid):
        lats = np.array([-82, -82, -78, -78, -82.0])
        lons = np.array([160, -160, -160, 160, 160.0])
        shards = grid.coverage([(lats, lons)])
        n = grid.n_row_blocks * grid.n_col_blocks
        assert len(shards) > 0
        assert all(0 <= s < n for s in shards)


def _grid(cfg, res, chunk):
    return RectilinearGrid(
        crs="EPSG:3031", resolution=res,
        bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
        chunk_shape=chunk, config=cfg,
    )


class TestSignature:
    def test_fields(self, grid):
        sig = grid.signature()
        assert sig["type"] == "rectilinear"
        assert sig["crs"] == "EPSG:3031"
        assert sig["shape"] == [1280, 1280]
        assert sig["chunk_shape"] == [256, 256]
        assert len(sig["affine"]) == 6

    def test_distinguishes_grids(self, cfg):
        assert _grid(cfg, 5000, (256, 256)).signature() != \
            _grid(cfg, 8000, (200, 200)).signature()


class TestNesting:
    def test_self_nests(self, grid):
        assert grid.nests_with(grid)

    def test_whole_ratio_aligned(self, cfg):
        assert _grid(cfg, 5000, (256, 256)).nests_with(_grid(cfg, 10000, (320, 320)))

    def test_non_whole_ratio(self, cfg):
        # 8000 / 5000 = 1.6 -> not a whole-number ratio.
        assert not _grid(cfg, 5000, (256, 256)).nests_with(_grid(cfg, 8000, (200, 200)))

    def test_cross_crs(self, cfg):
        utm = RectilinearGrid(
            crs="EPSG:32618", resolution=10,
            bounds=[359400, 4300740, 369400, 4310740], chunk_shape=[250, 250],
            config=cfg,
        )
        assert not _grid(cfg, 5000, (256, 256)).nests_with(utm)

    def test_cross_family(self, grid):
        from zagg.grids import HealpixGrid

        assert not grid.nests_with(HealpixGrid(6, 12, layout="fullsphere"))
