"""Tests for the OutputGrid protocol and HealpixGrid implementation."""

import numpy as np
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from zagg.config import default_config, get_data_vars
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

    def test_cells_of(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        leaves = _leaves_for(-78.5, -132.0)
        buckets = g.cells_of(leaves)
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
        g = HealpixGrid(parent_order=6, child_order=8, layout="dense", populated_shards=shards)
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
            parent_order=6,
            child_order=8,
            layout="dense",
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
            parent_order=6,
            child_order=8,
            layout="dense",
            config=cfg,
            populated_shards=shards,
        )
        store = MemoryStore()
        g.emit_template(store)
        group = open_group(store, path="8", mode="r")
        expected = (4 ** (8 - 6) * n_shards,)
        for name in group:
            assert group[name].shape == expected

    def test_fullsphere_shape(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
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
                parent_order=6,
                child_order=8,
                layout=layout,
                config=cfg,
                populated_shards=[1, 2, 3] if layout == "dense" else None,
            )
            store = MemoryStore()
            g.emit_template(store)
            group = open_group(store, path="8", mode="r")
            expected_chunks = (4 ** (8 - 6),)
            for name in group:
                assert group[name].chunks == expected_chunks, f"layout={layout} var={name}"


class TestRoundTrip:
    def test_assign_to_shards_round_trip(self, cfg):
        """assign(lat, lon) → shards_of(...) reproduces the expected parent."""
        from mortie import clip2order, geo2mort

        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        lat, lon = -78.5, -132.0
        expected_parent = int(
            clip2order(6, geo2mort(np.array([lat]), np.array([lon]), order=18))[0]
        )
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
        xdggs_zarr_template(store, parent_order=6, child_order=8, n_parent_cells=3, config=cfg)
        group = open_group(store, path="8", mode="r")
        assert group["count"].shape == (4 ** (8 - 6) * 3,)


def _vector_config(bins=4, dtype="int64"):
    """A config with one scalar (``count``) and one ``kind: vector`` field
    (issue #29), reusing the atl06 coordinates so grids build normally."""
    from zagg.config import PipelineConfig

    base = default_config("atl06")
    agg = {
        "coordinates": base.aggregation.get("coordinates", {}),
        "variables": {
            "count": {"function": "len", "source": "h_li"},
            "hist": {
                "function": "np.bincount",
                "source": "b",
                "kind": "vector",
                "trailing_shape": bins,
                "dtype": dtype,
            },
        },
    }
    return PipelineConfig(data_source=base.data_source, aggregation=agg, output=base.output)


class TestOutputFieldSignature:
    """Issue #29 phase 4: ``signature()`` carries the Option-B output-field set
    and ``nests_with()`` requires a matching set."""

    def test_signature_includes_output_fields(self, cfg):
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        sig = g.signature()
        assert "output_fields" in sig
        names = {f["name"] for f in sig["output_fields"]}
        assert "count" in names
        # Each entry carries the Option-B keys (inner_shape added in issue #48).
        for f in sig["output_fields"]:
            assert set(f) == {"name", "kind", "trailing_shape", "inner_shape", "dtype"}

    def test_signature_marks_vector_field(self):
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=_vector_config())
        by_name = {f["name"]: f for f in g.signature()["output_fields"]}
        assert by_name["hist"]["kind"] == "vector"
        assert by_name["hist"]["trailing_shape"] == [4]
        assert by_name["count"]["kind"] == "scalar"
        assert by_name["count"]["trailing_shape"] == []

    def test_signature_is_json_serializable(self):
        import json

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=_vector_config())
        fields = g.signature()["output_fields"]
        # Round-trips through JSON unchanged (recorded in a ShardMap as JSON).
        assert json.loads(json.dumps(fields)) == fields

    def test_nests_with_same_field_set(self, cfg):
        a = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        b = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere", config=cfg)
        assert a.nests_with(b) and b.nests_with(a)

    def test_nests_with_differing_field_kind_rejected(self, cfg):
        scalar = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        vector = HealpixGrid(
            parent_order=6, child_order=8, layout="fullsphere", config=_vector_config()
        )
        assert not scalar.nests_with(vector)
        assert not vector.nests_with(scalar)

    def test_nests_with_differing_trailing_shape_rejected(self):
        a = HealpixGrid(
            parent_order=6,
            child_order=8,
            layout="fullsphere",
            config=_vector_config(bins=4),
        )
        b = HealpixGrid(
            parent_order=6,
            child_order=8,
            layout="fullsphere",
            config=_vector_config(bins=8),
        )
        assert not a.nests_with(b)


class TestVectorTemplate:
    """Issue #29 phase 5: a ``kind: vector`` field's template array gets a
    trailing payload dim chunked whole (single-trailing-chunk invariant)."""

    def test_healpix_vector_array_has_trailing_dim(self):
        cfg = _vector_config(bins=4, dtype="int64")
        # int vector field needs an int fill_value (NaN is invalid on int dtype).
        cfg.aggregation["variables"]["hist"]["fill_value"] = 0
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        store = MemoryStore()
        g.emit_template(store)
        grp = open_group(store, path="8", mode="r")
        n_pix = HEALPIX_BASE_CELLS * 4**8
        assert grp["count"].shape == (n_pix,)  # scalar unchanged
        assert grp["hist"].shape == (n_pix, 4)  # trailing payload dim
        # Trailing dim is ONE chunk (block_idx invariant): chunk == full width.
        assert grp["hist"].chunks == (4 ** (8 - 6), 4)
        assert grp["hist"].dtype == np.dtype("int64")

    def test_healpix_dimension_names_extend(self):
        cfg = _vector_config(bins=3, dtype="int64")
        cfg.aggregation["variables"]["hist"]["fill_value"] = 0
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        spec = g.spec()
        names = spec.members["hist"].dimension_names
        assert names == ("cells", "vector")  # spatial + the trailing payload axis

    def test_rectilinear_vector_array_has_trailing_dim(self):
        from zagg.grids import RectilinearGrid

        cfg = _vector_config(bins=4, dtype="int64")
        cfg.aggregation["variables"]["hist"]["fill_value"] = 0
        g = RectilinearGrid(
            "EPSG:3031",
            1000.0,
            (-1e6, -1e6, 1e6, 1e6),
            chunk_shape=(64, 64),
            config=cfg,
        )
        store = MemoryStore()
        g.emit_template(store)
        grp = open_group(store, path="rectilinear", mode="r")
        assert grp["count"].shape == (g.height, g.width)
        assert grp["hist"].shape == (g.height, g.width, 4)
        assert grp["hist"].chunks == (64, 64, 4)  # trailing dim whole


class TestMortonCoordinate:
    """The ``morton`` coordinate is a mortie ``MortonIndexArray`` stored as
    ``uint64`` on disk (#71); ``cell_ids`` stays NESTED ``uint64`` (DGGS)."""

    def test_chunk_coords_morton_is_extension_array(self, cfg):
        from mortie import MortonIndexArray

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        parent = _valid_parents(1)[0]
        coords = g.chunk_coords(parent)
        assert isinstance(coords["morton"], MortonIndexArray)
        # cell_ids stays a plain NESTED integer array, unchanged.
        assert not isinstance(coords["cell_ids"], MortonIndexArray)

    def test_morton_words_match_generate_children(self, cfg):
        """The typed coordinate carries the same packed words generate_morton_children
        returns — the type is a skin, not a re-encoding."""
        from mortie import generate_morton_children

        from zagg.grids.morton import morton_words

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        parent = _valid_parents(1)[0]
        children = generate_morton_children(int(parent), 8)
        coords = g.chunk_coords(parent)
        np.testing.assert_array_equal(morton_words(coords["morton"]), children)

    def test_geo2mort_clip2order_coord_roundtrip(self, cfg):
        """geo2mort → clip2order → coord → reload reconstructs the same words and
        is non-negative for southern (base 7-11) cells where int64 went negative."""
        from mortie import clip2order, geo2mort

        from zagg.grids.morton import morton_words, to_morton_array

        # A southern point lands in a high base cell whose packed word sets bit 63.
        leaf = geo2mort(np.array([-78.5]), np.array([-132.0]), order=18)
        cells = clip2order(8, leaf)
        coord = to_morton_array(cells)
        words = morton_words(coord)
        np.testing.assert_array_equal(words, np.asarray(cells, dtype=np.uint64))
        assert words.dtype == np.uint64
        # Round-trips losslessly back through the uint64 storage form.
        np.testing.assert_array_equal(morton_words(to_morton_array(words)), words)

    def test_morton_stored_uint64_roundtrips_through_zarr(self, cfg):
        """Write a fullsphere chunk and read morton back as the same non-negative
        uint64 words — the int64 sign hazard is gone (#71)."""
        import pandas as pd

        from zagg.grids.morton import morton_words, to_morton_array
        from zagg.processing import write_dataframe_to_zarr

        parent_order, child_order = 6, 8
        g = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        g.emit_template(store)

        parent = _valid_parents(1)[0]
        coords = g.chunk_coords(parent)
        n = len(coords["cell_ids"])
        df = pd.DataFrame({"cell_ids": coords["cell_ids"]})
        df["morton"] = coords["morton"]
        for var in get_data_vars(cfg):
            df[var] = np.zeros(n, dtype=np.int32 if var == "count" else np.float32)

        write_dataframe_to_zarr(df, store, grid=g, chunk_idx=g.block_index(parent))

        grp = open_group(store, path=str(child_order), mode="r")
        assert grp["morton"].dtype == np.uint64
        lo = int(np.asarray(coords["cell_ids"]).min())
        hi = int(np.asarray(coords["cell_ids"]).max())
        stored = grp["morton"][lo : hi + 1]
        expected = morton_words(coords["morton"])
        np.testing.assert_array_equal(stored, expected)
        assert (stored >= 0).all()
        # Reconstructs to the same MortonIndexArray on read.
        np.testing.assert_array_equal(morton_words(to_morton_array(stored)), expected)
