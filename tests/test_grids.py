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


def _chunk_resolution_config():
    """Config with one cell-resolution scalar and one ``resolution: chunk`` field."""
    from zagg.config import PipelineConfig

    base = default_config("atl06")
    agg = {
        "coordinates": base.aggregation.get("coordinates", {}),
        "chunk_precompute": {
            "chunk_anchor": {"expression": "np.float32(np.median(h_li))", "source": "h_li"}
        },
        "variables": {
            "count": {"function": "len", "source": "h_li"},
            "anchor_h": {"expression": "chunk_anchor", "source": "h_li", "resolution": "chunk"},
        },
    }
    return PipelineConfig(data_source=base.data_source, aggregation=agg, output=base.output)


class TestChunkResolutionTemplate:
    """Issue #30 item 2: a ``resolution: chunk`` field emits a companion array
    shaped at the chunk grid (main.shape // chunk_shape), not the cell grid."""

    def test_healpix_companion_at_chunk_grid(self):
        cfg = _chunk_resolution_config()
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        store = MemoryStore()
        g.emit_template(store)
        grp = open_group(store, path="8", mode="r")
        # companion is the chunk grid (12·4^parent), one block per chunk.
        n_chunks = HEALPIX_BASE_CELLS * 4**6
        assert grp["anchor_h"].shape == (n_chunks,)
        assert grp["anchor_h"].chunks == (1,)
        # cell-resolution count keeps the full cell grid.
        assert grp["count"].shape == (HEALPIX_BASE_CELLS * 4**8,)
        assert g.spec().members["anchor_h"].dimension_names == ("chunks",)

    def test_rectilinear_companion_at_chunk_grid(self):
        from zagg.grids import RectilinearGrid

        cfg = _chunk_resolution_config()
        g = RectilinearGrid(
            "EPSG:3031", 1000.0, (-1e6, -1e6, 1e6, 1e6), chunk_shape=(64, 64), config=cfg
        )
        store = MemoryStore()
        g.emit_template(store)
        grp = open_group(store, path="rectilinear", mode="r")
        assert grp["anchor_h"].shape == (g.n_row_blocks, g.n_col_blocks)
        assert grp["anchor_h"].chunks == (1, 1)
        assert grp["count"].shape == (g.height, g.width)
        assert g._spec().members["anchor_h"].dimension_names == ("chunk_y", "chunk_x")


class TestPlainConfigByteIdentical:
    """A config with NO chunk_precompute and NO resolution: chunk field must emit a
    template byte-for-byte identical to the pre-item-2 schema (issue #30 byte-
    identical guarantee): the new attributes are purely additive (default cell)."""

    def test_atl06_template_unchanged_by_resolution_machinery(self):
        # The atl06 config declares no resolution: chunk field, so no agg-field
        # array routes through the chunk companion path: every field keeps the cell
        # grid shape, and the serialized member specs are identical to a re-emit.
        from zagg.processing import _chunk_resolution_fields

        cfg = default_config("atl06")
        assert _chunk_resolution_fields(cfg) == set()
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        spec = g.spec()
        # Every agg field is at the cell grid (no companion), so the serialized
        # GroupSpec equals a re-derived one and no dimension name is a chunk axis.
        n_pix = HEALPIX_BASE_CELLS * 4**8
        for name in get_data_vars(cfg):
            member = spec.members[name]
            assert member.shape == (n_pix,)
            assert member.dimension_names == ("cells",)
        # Full-spec round-trip equality (the additive resolution key is inert).
        assert (
            g.spec().model_dump()
            == HealpixGrid(
                parent_order=6, child_order=8, layout="fullsphere", config=default_config("atl06")
            )
            .spec()
            .model_dump()
        )


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
        # The southern children set bit 63: non-negative as uint64, but at least
        # one would have read back negative under the old int64 coordinate.
        assert (words.view(np.int64) < 0).any()
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
        # The southern parent's order-8 children set bit 63: under the old int64
        # coordinate at least one word would have read back negative. As uint64
        # they are stored intact (this is the sign hazard #71 removes).
        assert stored.view(np.int64).min() < 0
        # Reconstructs to the same MortonIndexArray on read.
        np.testing.assert_array_equal(morton_words(to_morton_array(stored)), expected)


class TestChunkInnerMultiChunk:
    """Issue #30 item 3: an optional finer ``chunk_inner`` level between shard and
    cell, native units per grid (HEALPix order, rectilinear shape). One shard owns
    K chunks; ``iter_chunks`` enumerates them. Default (unset) == today (K == 1)."""

    def test_healpix_default_is_single_chunk_byte_identical(self):
        # Unset chunk_inner: chunk_order == parent_order, K == 1, and the template
        # member specs equal a grid built without the kwarg.
        cfg = default_config("atl06")
        g0 = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        g1 = HealpixGrid(
            parent_order=6, child_order=8, layout="fullsphere", config=cfg, chunk_inner=None
        )
        assert g0.chunk_order == g1.chunk_order == 6
        assert g1.chunks_per_shard == 1
        assert g1.chunk_shape == g0.chunk_shape == (4 ** (8 - 6),)
        assert g0.spec().model_dump() == g1.spec().model_dump()

    def test_healpix_iter_chunks_k1_matches_block_index_children(self):
        from mortie import geo2mort

        cfg = default_config("atl06")
        g = HealpixGrid(parent_order=4, child_order=8, layout="fullsphere", config=cfg)
        parent = int(geo2mort(-78.5, -132.0, order=4)[0])
        chunks = list(g.iter_chunks(parent))
        assert len(chunks) == 1
        (block, children) = chunks[0]
        assert block == g.block_index(parent)
        np.testing.assert_array_equal(children, g.children(parent))

    def test_healpix_chunk_inner_partitions_shard(self):
        from mortie import geo2mort

        cfg = default_config("atl06")
        # parent 4, chunk_inner order 6 -> K = 4^(6-4) = 16 chunks per shard, each
        # holding 4^(8-6) = 16 cells.
        g = HealpixGrid(
            parent_order=4, child_order=8, layout="fullsphere", config=cfg, chunk_inner=6
        )
        assert g.chunks_per_shard == 16
        assert g.cells_per_chunk == 16
        assert g.chunk_shape == (16,)
        # companion chunk grid is the finer 12·4^6, not 12·4^4.
        assert g.chunk_grid_shape == (HEALPIX_BASE_CELLS * 4**6,)

        parent = int(geo2mort(-78.5, -132.0, order=4)[0])
        chunks = list(g.iter_chunks(parent))
        assert len(chunks) == 16
        # Each chunk has cells_per_chunk children; the union equals the shard's cells.
        all_children = np.concatenate([c for _, c in chunks])
        assert len(all_children) == 16 * 16
        np.testing.assert_array_equal(np.sort(all_children), np.sort(g.children(parent)))
        # Block indices are distinct and in-bounds of the finer chunk grid.
        blocks = [b[0] for b, _ in chunks]
        assert len(set(blocks)) == 16
        assert all(0 <= b < g.chunk_grid_shape[0] for b in blocks)

    def test_healpix_chunk_inner_bounds_validated(self):
        cfg = default_config("atl06")
        with pytest.raises(ValueError, match="chunk_inner"):
            HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=3)  # < parent
        with pytest.raises(ValueError, match="chunk_inner"):
            HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=9)  # > child

    def test_healpix_chunk_inner_rejected_for_dense(self):
        cfg = default_config("atl06")
        with pytest.raises(ValueError, match="fullsphere"):
            HealpixGrid(4, 8, layout="dense", config=cfg, chunk_inner=6, populated_shards=[0])

    def test_healpix_chunk_inner_not_in_signature(self):
        # The shard-map fingerprint must be unchanged by chunk_inner (byte-identical
        # guarantee): a K=16 grid signs identically to a K=1 grid.
        cfg = default_config("atl06")
        g0 = HealpixGrid(4, 8, layout="fullsphere", config=cfg)
        g1 = HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=6)
        assert g0.signature() == g1.signature()

    def test_rectilinear_default_is_single_chunk_byte_identical(self):
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl06")
        bounds = (-1e6, -1e6, 1e6, 1e6)
        g0 = RectilinearGrid("EPSG:3031", 1000.0, bounds, chunk_shape=(64, 64), config=cfg)
        g1 = RectilinearGrid(
            "EPSG:3031", 1000.0, bounds, chunk_shape=(64, 64), config=cfg, chunk_inner=None
        )
        assert g1.chunk_shape == g0.chunk_shape == (64, 64)
        assert g0._spec().model_dump() == g1._spec().model_dump()

    def test_rectilinear_iter_chunks_k1_matches(self):
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl06")
        g = RectilinearGrid(
            "EPSG:3031", 100000.0, (-4e5, -4e5, 4e5, 4e5), chunk_shape=(4, 4), config=cfg
        )
        shard = g._pack(1, 1)
        chunks = list(g.iter_chunks(shard))
        assert len(chunks) == 1
        (block, children) = chunks[0]
        assert block == g.block_index(shard)
        np.testing.assert_array_equal(children, g.children(shard))

    def test_rectilinear_chunk_inner_partitions_shard(self):
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl06")
        # shard tile 8x8, inner 4x4 -> K = 4 chunks per shard, each 16 cells.
        g = RectilinearGrid(
            "EPSG:3031",
            100000.0,
            (-4e5, -4e5, 4e5, 4e5),
            chunk_shape=(8, 8),
            config=cfg,
            chunk_inner=(4, 4),
        )
        assert g.chunk_shape == (4, 4)
        assert g.chunks_per_shard == 4
        assert g.chunk_grid_shape == (g.n_inner_row_blocks, g.n_inner_col_blocks)
        assert g.chunk_grid_shape == (g.height // 4, g.width // 4)

        shard = g._pack(0, 0)
        chunks = list(g.iter_chunks(shard))
        assert len(chunks) == 4
        all_children = np.concatenate([c for _, c in chunks])
        # SET equality holds (partition is complete + disjoint)...
        np.testing.assert_array_equal(np.sort(all_children), np.sort(g.children(shard)))
        # ...but the concatenation ORDER does NOT match the shard's row-major order
        # (documented caveat: per-chunk children are row-major within each inner
        # sub-tile, so the writer must place each chunk against its own block).
        assert not np.array_equal(all_children, g.children(shard))
        # Each chunk's own children are row-major within its inner tile.
        first_block, first_children = chunks[0]
        assert first_block == (0, 0)
        expected_first = np.array(
            [r * g.width + c for r in range(4) for c in range(4)], dtype=np.int64
        )
        np.testing.assert_array_equal(first_children, expected_first)
        blocks = [b for b, _ in chunks]
        assert len(set(blocks)) == 4
        # block (0,0) shard -> inner chunks (0,0),(0,1),(1,0),(1,1).
        assert set(blocks) == {(0, 0), (0, 1), (1, 0), (1, 1)}

    def test_rectilinear_chunk_inner_must_divide_shard(self):
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl06")
        with pytest.raises(ValueError, match="evenly divide"):
            RectilinearGrid(
                "EPSG:3031",
                100000.0,
                (-4e5, -4e5, 4e5, 4e5),
                chunk_shape=(8, 8),
                config=cfg,
                chunk_inner=(3, 4),
            )

    def test_rectilinear_chunk_inner_not_in_signature(self):
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl06")
        bounds = (-4e5, -4e5, 4e5, 4e5)
        g0 = RectilinearGrid("EPSG:3031", 100000.0, bounds, chunk_shape=(8, 8), config=cfg)
        g1 = RectilinearGrid(
            "EPSG:3031", 100000.0, bounds, chunk_shape=(8, 8), config=cfg, chunk_inner=(4, 4)
        )
        assert g0.signature() == g1.signature()
