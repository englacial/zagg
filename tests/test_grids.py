"""Tests for the OutputGrid protocol and HealpixGrid implementation."""

import numpy as np
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from zagg.config import default_config
from zagg.grids import HEALPIX_BASE_CELLS, HealpixGrid, InconsistentShardError, from_config


@pytest.fixture
def cfg():
    return default_config("atl06")


class TestConstruction:
    def test_child_lt_parent_raises(self):
        with pytest.raises(ValueError, match="child_order.*must be >="):
            HealpixGrid(parent_order=8, child_order=6)

    def test_unknown_layout_raises(self):
        with pytest.raises(ValueError, match="Unknown layout"):
            HealpixGrid(parent_order=6, child_order=8, layout="weird")

    def test_default_layout_is_dense(self):
        g = HealpixGrid(parent_order=6, child_order=8)
        assert g.layout == "dense"

    def test_dense_needs_populated_shards_for_n(self):
        g = HealpixGrid(parent_order=6, child_order=8, layout="dense")
        with pytest.raises(RuntimeError, match="populated_shards"):
            _ = g.n_shards

    def test_fullsphere_n_shards_is_global(self):
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere")
        assert g.n_shards == HEALPIX_BASE_CELLS * 4**6


def _valid_parents(n: int = 3, parent_order: int = 6):
    """Derive n distinct valid mortie parent IDs from sample lat/lons."""
    from mortie import geo2mort

    sample_pts = [(-78.5, -132.0), (-72.1, 25.4), (-65.0, -45.0), (78.3, 12.0)]
    return [int(geo2mort(lat, lon, order=parent_order)[0]) for lat, lon in sample_pts[:n]]


def _leaves_for(lat: float, lon: float, n: int = 16):
    """Generate n leaf IDs (order-18 morton) by jittering a (lat, lon) point."""
    from mortie import geo2mort

    lats = np.full(n, lat) + np.linspace(0, 1e-5, n)
    lons = np.full(n, lon) + np.linspace(0, 1e-5, n)
    return geo2mort(lats, lons, order=18)


class TestShardOf:
    def test_same_shard_returns_parent(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        leaves = _leaves_for(-78.5, -132.0)
        # All jittered points should fall under one parent.
        expected_parent = int(g.shards_of(leaves)[0])
        assert g.shard_of(leaves) == expected_parent

    def test_split_shards_raises(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        l1 = _leaves_for(-78.5, -132.0)
        l2 = _leaves_for(-72.1, 25.4)
        mixed = np.concatenate([l1[:1], l2[:1]])
        with pytest.raises(InconsistentShardError):
            g.shard_of(mixed)

    def test_shards_of_vectorized(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        l1 = _leaves_for(-78.5, -132.0)
        l2 = _leaves_for(-72.1, 25.4)
        p1 = int(g.shards_of(l1)[0])
        p2 = int(g.shards_of(l2)[0])
        assert p1 != p2
        leaves = np.concatenate([l1, l2])
        parents = g.shards_of(leaves)
        assert np.all(parents[: len(l1)] == p1)
        assert np.all(parents[len(l1) :] == p2)

    def test_bucket_at_child(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        leaves = _leaves_for(-78.5, -132.0)
        buckets = g.bucket_at_child(leaves)
        # Every leaf bucket must be a valid child of the shard.
        parent = g.shard_of(leaves)
        children = set(int(c) for c in g.children(parent))
        for b in buckets:
            assert int(b) in children


class TestBlockIndex:
    def test_fullsphere_returns_healpix_nested(self):
        """Fullsphere chunks are indexed by HEALPix nested ID, not morton."""
        from mortie import mort2healpix

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere")
        parent = _valid_parents(1)[0]
        expected, _ = mort2healpix(np.asarray([parent]))
        assert g.block_index(parent) == (int(expected[0]),)
        # Range check: must lie in [0, 12·4^parent_order)
        assert 0 <= g.block_index(parent)[0] < 12 * 4**6

    def test_dense_uses_position_map(self):
        shards = _valid_parents(3)
        g = HealpixGrid(
            parent_order=6, child_order=8, layout="dense", populated_shards=shards
        )
        for i, s in enumerate(shards):
            assert g.block_index(s) == (i,)

    def test_dense_requires_populated_shards(self):
        g = HealpixGrid(parent_order=6, child_order=8, layout="dense")
        with pytest.raises(RuntimeError, match="populated_shards"):
            g.block_index(_valid_parents(1)[0])

    def test_dense_preserves_insertion_order(self):
        # Critical for byte-identical writes against a fixed catalog.
        shards = _valid_parents(3)
        shards_reordered = [shards[2], shards[0], shards[1]]
        g = HealpixGrid(
            parent_order=6, child_order=8, layout="dense",
            populated_shards=shards_reordered,
        )
        assert g.block_index(shards_reordered[0]) == (0,)
        assert g.block_index(shards_reordered[1]) == (1,)
        assert g.block_index(shards_reordered[2]) == (2,)


class TestEmitTemplate:
    def test_dense_shape(self, cfg):
        n_shards = 3
        shards = list(range(n_shards))
        g = HealpixGrid(
            parent_order=6, child_order=8, layout="dense",
            config=cfg, populated_shards=shards,
        )
        store = MemoryStore()
        g.emit_template(store)
        group = open_group(store, path="8", mode="r")
        expected = (4 ** (8 - 6) * n_shards,)
        for name in group:
            assert group[name].shape == expected

    def test_fullsphere_shape(self, cfg):
        g = HealpixGrid(
            parent_order=6, child_order=8, layout="fullsphere", config=cfg
        )
        store = MemoryStore()
        g.emit_template(store)
        group = open_group(store, path="8", mode="r")
        expected = (HEALPIX_BASE_CELLS * 4**8,)
        for name in group:
            assert group[name].shape == expected

    def test_chunk_shape_matches_shard_size(self, cfg):
        """Chunk-alignment invariant: chunks == 4^(child - parent)."""
        for layout in ("dense", "fullsphere"):
            g = HealpixGrid(
                parent_order=6, child_order=8, layout=layout, config=cfg,
                populated_shards=[1, 2, 3] if layout == "dense" else None,
            )
            store = MemoryStore()
            g.emit_template(store)
            group = open_group(store, path="8", mode="r")
            expected_chunks = (4 ** (8 - 6),)
            for name in group:
                assert group[name].chunks == expected_chunks, (
                    f"layout={layout} var={name}"
                )


class TestRoundTrip:
    def test_assign_to_shards_round_trip(self, cfg):
        """assign(lat, lon) → shards_of(...) reproduces the expected parent."""
        from mortie import clip2order, geo2mort

        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        lat, lon = -78.5, -132.0
        expected_parent = int(clip2order(6, geo2mort(np.array([lat]), np.array([lon]), order=18))[0])
        leaves = g.assign(np.array([lat]), np.array([lon]))
        assert g.shard_of(leaves) == expected_parent

    def test_children_count(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=8, config=cfg)
        parent = _valid_parents(1)[0]
        children = g.children(parent)
        assert len(children) == 4 ** (8 - 6)

    def test_encode_cell_ids_shape(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=8, config=cfg)
        parent = _valid_parents(1)[0]
        children = g.children(parent)
        cell_ids = g.encode_cell_ids(children)
        assert cell_ids.shape == children.shape


class TestFromConfig:
    def test_healpix_default_fullsphere(self, cfg):
        # No layout in YAML → fullsphere (default since dense was deprecated).
        g = from_config(cfg, parent_order=6)
        assert isinstance(g, HealpixGrid)
        assert g.layout == "fullsphere"
        assert g.parent_order == 6

    def test_healpix_explicit_dense_warns(self, cfg):
        cfg.output["grid"]["layout"] = "dense"
        with pytest.warns(DeprecationWarning, match="dense is deprecated"):
            g = from_config(cfg, parent_order=6, populated_shards=_valid_parents(3))
        assert g.layout == "dense"

    def test_healpix_explicit_fullsphere(self, cfg):
        cfg.output["grid"]["layout"] = "fullsphere"
        g = from_config(cfg, parent_order=6)
        assert g.layout == "fullsphere"

    def test_unknown_grid_raises(self, cfg):
        cfg.output["grid"]["type"] = "h3"
        with pytest.raises(ValueError, match="Unknown output.grid.type"):
            from_config(cfg, parent_order=6)


class TestBackcompatWrapper:
    """xdggs_zarr_template (the public API) must keep producing the same
    template structures it did before the HealpixGrid refactor."""

    def test_no_n_parent_cells_means_fullsphere(self, cfg):
        from zagg.schema import xdggs_zarr_template

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8, config=cfg)
        group = open_group(store, path="8", mode="r")
        assert group["count"].shape == (HEALPIX_BASE_CELLS * 4**8,)

    def test_with_n_parent_cells_means_dense(self, cfg):
        from zagg.schema import xdggs_zarr_template

        store = MemoryStore()
        xdggs_zarr_template(
            store, parent_order=6, child_order=8, n_parent_cells=3, config=cfg
        )
        group = open_group(store, path="8", mode="r")
        assert group["count"].shape == (4 ** (8 - 6) * 3,)
