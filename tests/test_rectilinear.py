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

    def test_clean_config_not_padded(self, grid):
        # 1280 == 5*256 already, so auto-pad is a no-op and bounds are unchanged.
        assert grid.array_shape == (1280, 1280)
        assert (grid.xmin, grid.ymax, grid.xmax, grid.ymin) == (
            -3_200_000,
            3_200_000,
            3_200_000,
            -3_200_000,
        )

    def test_uneven_chunk_padded(self, cfg):
        # 1280x1280 cells, chunk 300 doesn't divide -> zero-pad up to 1500 (5*300).
        g = RectilinearGrid(
            crs="EPSG:3031",
            resolution=5000,
            bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
            chunk_shape=(300, 300),
            config=cfg,
        )
        assert g.array_shape == (1500, 1500)
        assert g.height % g.chunk_h == 0 and g.width % g.chunk_w == 0
        assert g.n_row_blocks == 5 and g.n_col_blocks == 5
        # origin (xmin, ymax) preserved; far edges (xmax, ymin) extended to fit.
        assert (g.xmin, g.ymax) == (-3_200_000, 3_200_000)
        assert g.xmax == -3_200_000 + 1500 * 5000
        assert g.ymin == 3_200_000 - 1500 * 5000

    def test_partial_extent_rounds_up_to_cover(self, cfg):
        # span not a whole multiple of resolution -> round cells UP to cover it
        # (chunk_shape (1,1) isolates the cover-rounding from chunk-padding).
        g = RectilinearGrid(
            crs="EPSG:3031",
            resolution=5000,
            bounds=(0, 0, 5000 * 130 + 100, 5000 * 130 + 100),
            chunk_shape=(1, 1),
            config=cfg,
        )
        assert g.array_shape == (131, 131)  # ceil(130.02) -> 131

    def test_signature_reflects_padding(self, cfg):
        g = RectilinearGrid(
            crs="EPSG:3031",
            resolution=5000,
            bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
            chunk_shape=(300, 300),
            config=cfg,
        )
        sig = g.signature()
        assert sig["shape"] == [1500, 1500]
        assert sig["chunk_shape"] == [300, 300]

    def test_invalid_bounds(self, cfg):
        with pytest.raises(ValueError, match="bounds"):
            RectilinearGrid(
                crs="EPSG:3031",
                resolution=5000,
                bounds=(0, 0, -100, -100),
                chunk_shape=(2, 2),
                config=cfg,
            )

    def test_scalar_vs_tuple_resolution(self, cfg):
        g1 = RectilinearGrid(
            crs="EPSG:3031",
            resolution=5000,
            bounds=(0, 0, 10000, 10000),
            chunk_shape=(2, 2),
            config=cfg,
        )
        g2 = RectilinearGrid(
            crs="EPSG:3031",
            resolution=[5000, 5000],
            bounds=(0, 0, 10000, 10000),
            chunk_shape=(2, 2),
            config=cfg,
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

    def test_shard_label_is_int_digits(self, grid):
        # Rect shard keys are packed tile ints, not morton words: the external
        # label (issue #199) stays the int's decimal digits.
        packed = grid._pack(2, 3)
        assert grid.shard_label(packed) == str(packed)


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

    def test_sharded_default_off(self, cfg):
        assert from_config(cfg).sharded is False

    def test_sharded_threads_through(self, cfg):
        cfg.output["grid"]["chunk_inner"] = [128, 128]
        cfg.output["grid"]["sharded"] = True
        g = from_config(cfg)
        assert g.sharded is True
        assert g.chunks_per_shard > 1

    def test_sharded_k1_validated(self, cfg):
        cfg.output["grid"]["sharded"] = True  # no chunk_inner -> K == 1
        with pytest.raises(ValueError, match="K>1"):
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
        crs="EPSG:3031",
        resolution=res,
        bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
        chunk_shape=chunk,
        config=cfg,
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
        assert _grid(cfg, 5000, (256, 256)).signature() != _grid(cfg, 8000, (200, 200)).signature()


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
            crs="EPSG:32618",
            resolution=10,
            bounds=[359400, 4300740, 369400, 4310740],
            chunk_shape=[250, 250],
            config=cfg,
        )
        assert not _grid(cfg, 5000, (256, 256)).nests_with(utm)

    def test_cross_family(self, grid):
        from zagg.grids import HealpixGrid

        assert not grid.nests_with(HealpixGrid(6, 12, layout="fullsphere"))

    def test_differing_output_field_set_rejected(self, cfg):
        """Otherwise-nesting grids with a different Option-B output-field set
        (issue #29 phase 4) must not nest, symmetrically."""
        from zagg.config import PipelineConfig

        agg = {
            "coordinates": cfg.aggregation.get("coordinates", {}),
            "variables": {
                "count": {"function": "len", "source": "h_li"},
                "hist": {
                    "function": "np.bincount",
                    "source": "b",
                    "kind": "vector",
                    "trailing_shape": 4,
                    "dtype": "int64",
                },
            },
        }
        vcfg = PipelineConfig(data_source=cfg.data_source, aggregation=agg, output=cfg.output)
        scalar = _grid(cfg, 5000, (256, 256))
        vector = _grid(vcfg, 5000, (256, 256))
        # Same CRS / whole-ratio / aligned — only the field set differs.
        assert not scalar.nests_with(vector)
        assert not vector.nests_with(scalar)


class TestValidateCompatible:
    def test_compatible_pass(self, cfg):
        from zagg.grids import validate_compatible

        validate_compatible([_grid(cfg, 5000, (256, 256)), _grid(cfg, 10000, (320, 320))])

    def test_single_grid_ok(self, grid):
        from zagg.grids import validate_compatible

        validate_compatible([grid])

    def test_cross_family_raises(self, cfg):
        from zagg.grids import HealpixGrid, validate_compatible

        with pytest.raises(ValueError, match="do not nest"):
            validate_compatible(
                [_grid(cfg, 5000, (256, 256)), HealpixGrid(6, 12, layout="fullsphere")]
            )


class TestSharded:
    """Issue #108 phase 4: rectilinear ShardingCodec — one shard object per shard
    tile, K inner chunks bundled inside, byte-identical to the regular path."""

    # shard tile 8x8, inner 4x4 -> K = 4 inner chunks/shard.
    def _grids(self, cfg):
        kw = dict(
            crs="EPSG:3031",
            resolution=100000.0,
            bounds=(-4e5, -4e5, 4e5, 4e5),
            chunk_shape=(8, 8),
            config=cfg,
            chunk_inner=(4, 4),
        )
        return RectilinearGrid(sharded=True, **kw), RectilinearGrid(**kw)

    def test_k1_sharded_rejected(self, cfg):
        with pytest.raises(ValueError, match="K>1"):
            RectilinearGrid(
                "EPSG:3031",
                100000.0,
                (-4e5, -4e5, 4e5, 4e5),
                chunk_shape=(8, 8),
                config=cfg,
                sharded=True,  # no chunk_inner -> K == 1
            )

    def test_template_emits_sharding_codec(self, cfg):
        sharded, _regular = self._grids(cfg)
        store = MemoryStore()
        sharded.emit_template(store)
        group = open_group(store, path="rectilinear", mode="r")
        for name in ("h_mean", "count"):
            arr = group[name]
            assert arr.chunks == (4, 4), name  # inner read chunk
            assert arr.shards == (8, 8), name  # dispatch shard tile
            assert any("sharding" in type(c).__name__.lower() for c in arr.metadata.codecs)
        # 1-D x/y coords are never sharded.
        assert group["x"].shards is None
        assert group["y"].shards is None

    def test_flag_off_byte_identical(self, cfg):
        sharded_off = RectilinearGrid(
            "EPSG:3031",
            100000.0,
            (-4e5, -4e5, 4e5, 4e5),
            chunk_shape=(8, 8),
            config=cfg,
            chunk_inner=(4, 4),
        )
        _sharded, regular = self._grids(cfg)
        assert sharded_off._spec().model_dump() == regular._spec().model_dump()

    def test_whole_shard_write_roundtrip(self, cfg):
        import pandas as pd

        from zagg.processing import write_shard_to_zarr

        sharded, _regular = self._grids(cfg)
        store = MemoryStore()
        sharded.emit_template(store)

        # Build one carrier per inner chunk: each cell carries its own global cell
        # id as the h_mean value, in the chunk's row-major order (as the worker does).
        shard_key = sharded._pack(0, 0)
        chunk_results = []
        for block_index, children in sharded.iter_chunks(shard_key):
            df = pd.DataFrame({"h_mean": np.asarray(children, dtype=np.float32)})
            chunk_results.append((block_index, df, {}))
        write_shard_to_zarr(chunk_results, store, grid=sharded, shard_key=shard_key)

        # Exactly one shard object for the populated dense array.
        h_keys = [k for k in store._store_dict if k.startswith("rectilinear/h_mean/c/")]
        assert h_keys == ["rectilinear/h_mean/c/0/0"]

        # Read-back: every cell in the shard tile holds its own global id.
        group = open_group(store, path="rectilinear", mode="r")
        arr = group["h_mean"][:8, :8]
        expected = (np.arange(8)[:, None] * sharded.width + np.arange(8)[None, :]).astype(
            np.float32
        )
        np.testing.assert_array_equal(arr, expected)


class TestTransformerThreadSafety:
    """Issue #180 (review finding 3): ``assign`` runs on the granule pool's
    worker threads sharing one pyproj ``Transformer``. Concurrent
    ``.transform()`` is thread-safe from pyproj 3.7.0 — enforced by the
    ``pyproject.toml`` floor (espg-authorized dependency change) rather than a
    code-side mitigation."""

    def test_pyproj_floor_matches_pyproject(self):
        # The runtime floor the shared-Transformer contract depends on. If
        # this fails, the environment predates the pyproject pin.
        from importlib.metadata import version

        major, minor, *_ = (int(x) for x in version("pyproj").split(".")[:2])
        assert (major, minor) >= (3, 7)

    def test_shared_transformer_concurrent_use(self, grid):
        import threading

        tx = grid._transformer_to_grid()
        assert grid._transformer_to_grid() is tx  # one shared instance
        expected = tx.transform(-45.0, -70.0)  # always_xy: (lon, lat)
        results = {}

        def use(k):
            results[k] = tx.transform(-45.0, -70.0)

        threads = [threading.Thread(target=use, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(r == expected for r in results.values())

    def test_assign_unchanged(self, grid):
        lats = np.array([-70.0, -75.0])
        lons = np.array([-45.0, 90.0])
        np.testing.assert_array_equal(grid.assign(lats, lons), grid.assign(lats, lons))
