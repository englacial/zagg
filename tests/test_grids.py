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


class TestReferenceOrder:
    """Regression: the HEALPix assign reference order must reach mortie 0.8.1's
    max (29), not the old hardcoded 18 -- otherwise a fine ``child_order`` is
    silently collapsed onto order 18 (no added resolution)."""

    def test_ref_order_supports_fine_child_orders(self):
        from zagg.grids.healpix import HEALPIX_REF_ORDER

        assert HEALPIX_REF_ORDER >= 19  # mortie 0.8.1 resolves up to 29

    def test_child_order_19_refines_order_18(self):
        # The bug: ``assign`` pinned points at order 18, so ``child_order=19``
        # produced the SAME cells as ``child_order=18``. They must now differ.
        rng = np.random.default_rng(0)
        lats = rng.uniform(-89, 89, 2000)
        lons = rng.uniform(-179, 179, 2000)
        g18 = HealpixGrid(parent_order=10, child_order=18, layout="fullsphere")
        g19 = HealpixGrid(parent_order=11, child_order=19, layout="fullsphere")
        c18 = g18.cells_of(g18.assign(lats, lons))
        c19 = g19.cells_of(g19.assign(lats, lons))
        assert not np.array_equal(c19, c18)

    def test_existing_order_assignment_unchanged(self):
        # Raising the reference order must NOT move existing (<= 18) assignments:
        # coarsening order-12 cells from a deeper reference is byte-identical to
        # the pre-fix order-18 reference, so shipped configs' outputs don't drift.
        from mortie import clip2order, geo2mort

        rng = np.random.default_rng(3)
        lats = rng.uniform(-89, 89, 2000)
        lons = rng.uniform(-179, 179, 2000)
        g = HealpixGrid(parent_order=6, child_order=12, layout="fullsphere")
        cells = g.cells_of(g.assign(lats, lons))
        cells_via18 = clip2order(12, geo2mort(lats, lons, order=18))
        assert np.array_equal(cells, cells_via18)

    def test_order_19_nesting_holds(self):
        # An order-19 child cell coarsens back to its order-11 parent shard.
        g = HealpixGrid(parent_order=11, chunk_inner=13, child_order=19, layout="fullsphere")
        leaves = g.assign(np.array([38.89, -45.0]), np.array([-76.5, 30.0]))
        for shard in np.unique(g.shards_of(leaves)):
            children = g.children(int(shard))
            assert len(children) == 4 ** (19 - 11)
            assert np.all(g.shards_of(children) == int(shard))

    def test_assign_morton_exceeds_int64(self):
        # Order-29 ``assign`` produces morton values past int64's max (bit 63
        # set) -- they must be carried unsigned, never silently truncated/signed.
        g = HealpixGrid(parent_order=11, child_order=19, layout="fullsphere")
        rng = np.random.default_rng(11)
        leaves = np.asarray(g.assign(rng.uniform(-89, 89, 50000), rng.uniform(-179, 179, 50000)))
        mx = int(leaves.max())
        assert mx > np.iinfo(np.int64).max  # would be negative if stored signed
        assert mx <= np.iinfo(np.uint64).max

    def test_assign_emits_point_kind_words(self):
        # ``assign`` encodes point-kind (Kind::Point) order-29 words (issue #87),
        # distinct from the order-29 *area* words ``geo2mort`` emits: a point word
        # marks a location of unknown extent, which ``common_ancestor`` preserves
        # for a lone observation.
        from mortie import common_ancestor, geo2mort

        rng = np.random.default_rng(87)
        lats = rng.uniform(-89, 89, 500)
        lons = rng.uniform(-179, 179, 500)
        g = HealpixGrid(parent_order=11, child_order=19, layout="fullsphere")
        leaves = np.asarray(g.assign(lats, lons))
        assert leaves.dtype == np.uint64
        area = np.asarray(geo2mort(lats, lons, order=29))
        assert not np.array_equal(leaves, area)  # point kind, not area
        # A single point word is its own common ancestor (kind preserved).
        for w in leaves[:10]:
            assert int(common_ancestor(np.array([w], dtype=np.uint64))) == int(w)

    def test_point_kind_coarsening_matches_area_kind(self):
        # The byte-identity enabler for issue #87: point and area words share the
        # same path prefix, so ``cells_of``/``shards_of`` coarsening — and every
        # dense output derived from it — is bit-identical to the old area encode.
        from mortie import clip2order, geo2mort

        rng = np.random.default_rng(88)
        lats = rng.uniform(-89, 89, 5000)
        lons = rng.uniform(-179, 179, 5000)
        g = HealpixGrid(parent_order=11, child_order=19, layout="fullsphere")
        leaves = g.assign(lats, lons)
        area = geo2mort(lats, lons, order=29)
        assert np.array_equal(g.cells_of(leaves), clip2order(19, area))
        assert np.array_equal(g.shards_of(leaves), clip2order(11, area))

    def test_order_19_template_emits(self):
        # The order-19 + chunk_inner template emits sanely: cell-resolution arrays
        # at the full 12*4^19 grid, ``resolution: chunk`` companions at the chunk
        # grid (12*4^13). The nominal 3.3e12 cell array is metadata-only.
        cfg = default_config("atl03_gain_bias_healpix")
        g = from_config(cfg)
        store = MemoryStore()
        g.emit_template(store)
        group = open_group(store, path=str(g.child_order), mode="r")
        assert group["waveform_counts"].shape == (HEALPIX_BASE_CELLS * 4**19, 128)
        assert group["offset_h"].shape == (HEALPIX_BASE_CELLS * 4**13,)
        assert group["gain_h"].shape == (HEALPIX_BASE_CELLS * 4**13,)


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

    def test_sharded_default_on_for_multichunk(self, cfg):
        # issue #215: with chunk_inner giving K>1, a HEALPix flat-layout config that
        # OMITS the flag now defaults to sharded — the safe state, so a missing line
        # no longer silently costs the ~K-fold object blow-up.
        cfg.output["grid"]["chunk_inner"] = 8
        g = from_config(cfg, parent_order=6)
        assert g.sharded is True
        assert g.chunks_per_shard > 1

    def test_sharded_default_noop_for_single_chunk(self, cfg):
        # No chunk_inner -> K==1 -> nothing to bundle, so the default is a no-op:
        # single-chunk grids stay unsharded and byte-identical (issue #215).
        g = from_config(cfg, parent_order=6)
        assert g.chunks_per_shard == 1
        assert g.sharded is False

    def test_sharded_default_on_for_hive(self, cfg):
        # issue #236: hive defaults sharded, same as flat — a leaf's dense
        # arrays collapse to one ShardingCodec object each instead of K
        # per-inner-chunk objects PUT onto a single leaf prefix. (The #215
        # hive carve-out is gone; explicit sharded:false still opts out.)
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["store_layout"] = "hive"
        g = from_config(cfg, parent_order=6)
        assert g.chunks_per_shard > 1
        assert g.sharded is True

    def test_sharded_flag_threads_through(self, cfg):
        # sharded on the grid block, with chunk_inner giving K>1, yields a sharded grid.
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["grid"]["sharded"] = True
        g = from_config(cfg, parent_order=6)
        assert g.sharded is True
        assert g.chunks_per_shard > 1

    def test_sharded_explicit_off_disables(self, cfg):
        # An explicit sharded:false opts out even at K>1 (the one-release explicit
        # opt-out — issue #215): storage stays regular per-inner-chunk.
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["grid"]["sharded"] = False
        g = from_config(cfg, parent_order=6)
        assert g.chunks_per_shard > 1
        assert g.sharded is False

    def test_sharded_k1_noop_not_rejected(self, cfg):
        # sharded without chunk_inner (K==1) has nothing to bundle; the grid
        # silently disables it rather than raising (issue #215 — the default is
        # True, so a K==1 grid must not blow up at construction).
        cfg.output["grid"]["sharded"] = True
        g = from_config(cfg, parent_order=6)
        assert g.sharded is False

    def test_sharded_default_matches_explicit_true(self, cfg):
        # The omitted-flag default now produces the SAME template as an explicit
        # sharded:true (both collapse the K>1 inner chunks to one object; issue #215).
        cfg.output["grid"]["chunk_inner"] = 8
        g_default = from_config(cfg, parent_order=6)
        cfg.output["grid"]["sharded"] = True
        g_explicit = from_config(cfg, parent_order=6)
        assert g_default._spec().model_dump() == g_explicit._spec().model_dump()


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


class TestMortonArrowAdapter:
    """The typed Arrow legs of the morton boundary (issue #135): morton_to_arrow /
    morton_from_arrow / is_morton_arrow carry mortie's ``morton_index`` extension
    type over the PyCapsule interface, mirroring the uint64 round-trip suite above."""

    def test_extension_name_pins_mortie_constant(self):
        import mortie.arrow

        from zagg.grids.morton import MORTON_EXTENSION_NAME

        assert MORTON_EXTENSION_NAME == mortie.arrow.EXTENSION_NAME

    def test_to_arrow_carries_extension_type(self, cfg):
        from zagg.grids.morton import is_morton_arrow, morton_to_arrow

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        coords = g.chunk_coords(_valid_parents(1)[0])
        arr = morton_to_arrow(coords["morton"])
        assert is_morton_arrow(arr)

    def test_to_arrow_accepts_raw_words(self):
        from zagg.grids.morton import is_morton_arrow, morton_from_arrow, morton_to_arrow

        words = np.array([123456789, (1 << 63) | 42], dtype=np.uint64)
        arr = morton_to_arrow(words)
        assert is_morton_arrow(arr)
        np.testing.assert_array_equal(np.asarray(morton_from_arrow(arr)._data), words)

    def test_words_roundtrip_through_arrow(self, cfg):
        """to_arrow -> from_arrow reconstructs the same packed words the typed
        coordinate carries — the Arrow legs are a skin, not a re-encoding."""
        from mortie import generate_morton_children

        from zagg.grids.morton import morton_from_arrow, morton_to_arrow, morton_words

        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        parent = _valid_parents(1)[0]
        coords = g.chunk_coords(parent)
        back = morton_from_arrow(morton_to_arrow(coords["morton"]))
        np.testing.assert_array_equal(morton_words(back), generate_morton_children(int(parent), 8))

    def test_southern_bit63_words_roundtrip(self):
        """Southern (base 7-11) words set bit 63; the Arrow legs keep them intact
        as uint64 — the same #71 sign hazard the storage round-trip guards."""
        from mortie import clip2order, geo2mort

        from zagg.grids.morton import morton_from_arrow, morton_to_arrow, morton_words

        leaf = geo2mort(np.array([-78.5]), np.array([-132.0]), order=18)
        cells = np.asarray(clip2order(8, leaf), dtype=np.uint64)
        words = morton_words(morton_from_arrow(morton_to_arrow(cells)))
        np.testing.assert_array_equal(words, cells)
        assert words.dtype == np.uint64
        assert (words.view(np.int64) < 0).any()

    def test_sentinel_null_roundtrip(self):
        """The all-zero empty sentinel crosses the boundary as an Arrow null and
        comes back as the sentinel, so isna() round-trips."""
        from zagg.grids.morton import morton_from_arrow, morton_to_arrow, to_morton_array

        words = np.array([123456789, 0, 42], dtype=np.uint64)
        back = morton_from_arrow(morton_to_arrow(to_morton_array(words)))
        np.testing.assert_array_equal(np.asarray(back._data), words)
        np.testing.assert_array_equal(back.isna(), [False, True, False])

    def test_from_arrow_accepts_chunked_column(self):
        """A table column (ChunkedArray) round-trips too — the write path hands
        columns, not bare arrays."""
        from arro3.core import Table

        from zagg.grids.morton import is_morton_arrow, morton_from_arrow, morton_to_arrow

        words = np.array([7, 11, 13], dtype=np.uint64)
        tbl = Table.from_pydict({"morton": morton_to_arrow(words)})
        col = tbl.column("morton")
        assert is_morton_arrow(col)
        np.testing.assert_array_equal(np.asarray(morton_from_arrow(col)._data), words)

    def test_is_morton_arrow_false_for_plain_columns(self):
        from arro3.core import Array, Table

        from zagg.grids.morton import is_morton_arrow

        tbl = Table.from_pydict({"x": Array.from_numpy(np.arange(3, dtype=np.uint64))})
        assert not is_morton_arrow(tbl.column("x"))
        assert not is_morton_arrow(tbl.column("x").combine_chunks())
        assert not is_morton_arrow(np.arange(3))  # no Arrow field at all


class TestShardLabel:
    """Issue #199: shard ids surface externally as decimal morton strings (D1 in
    ``docs/design/sparse_coverage.md``) — ``morton_decimal``/``morton_word`` are
    the boundary pair, and ``grid.shard_label`` is the grid seam every external
    string (CSR subgroup names, status keys, log lines) routes through."""

    def test_decimal_round_trip_both_hemispheres(self):
        from mortie import geo2mort

        from zagg.grids.morton import morton_decimal, morton_word

        # Southern points render signed, northern unsigned; cover both plus a
        # spread of orders (order-0 base cells through fine cells).
        for lat, lon in [(-78.5, -132.0), (-72.1, 25.4), (78.3, 12.0), (0.1, 0.1)]:
            for order in (0, 6, 9, 18):
                word = int(geo2mort(np.array([lat]), np.array([lon]), order=order)[0])
                s = morton_decimal(word)
                # Grammar: optional sign, base 1..6, then one 1..4 digit per order.
                body = s.lstrip("-")
                assert body[0] in "123456" and all(d in "1234" for d in body[1:])
                assert len(body) == order + 1
                # Packed word -> decimal string -> packed word is lossless.
                assert morton_word(s) == word

    def test_order_29_round_trip_full_grammar_width(self):
        # Orders 19-29 extend the decimal form past the legacy i64; one order-29
        # AREA case pins the full grammar width (review nit, PR #205).
        from mortie import geo2mort

        from zagg.grids.morton import morton_decimal, morton_word

        word = int(geo2mort(np.array([-78.5]), np.array([-132.0]), order=29)[0])
        s = morton_decimal(word)
        assert len(s.lstrip("-")) == 30
        assert morton_word(s) == word

    def test_morton_word_rejects_malformed(self):
        from zagg.grids.morton import morton_word

        for bad in ("", "0", "7", "150", "abc", "11827859996358475782"):
            with pytest.raises(ValueError):
                morton_word(bad)

    def test_morton_decimal_raises_valueerror_on_invalid(self):
        # The emit direction's advertised contract (review finding, PR #205):
        # the empty sentinel raises, and a NEGATIVE input — a legacy signed
        # decimal id where the packed word belongs — raises ValueError (not the
        # opaque uint64-coercion OverflowError) with the remedy named.
        from zagg.grids.morton import morton_decimal

        with pytest.raises(ValueError):
            morton_decimal(0)
        with pytest.raises(ValueError, match="morton_word"):
            morton_decimal(-4211322)

    def test_healpix_shard_label_is_decimal(self, cfg):
        from zagg.grids.morton import morton_word

        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        for parent in _valid_parents(3):
            # The round trip through the parse-back is what carries this test
            # (label == morton_decimal(parent) would be tautological).
            assert morton_word(g.shard_label(parent)) == parent

    def test_base_helper_dispatches_and_falls_back(self, cfg):
        from zagg.grids.base import shard_label

        g = HealpixGrid(parent_order=6, child_order=12, config=cfg)
        parent = _valid_parents(1)[0]
        assert shard_label(g, parent) == g.shard_label(parent)
        # Minimal grid stand-ins without the method keep plain int digits.
        assert shard_label(object(), 42) == "42"


class TestCellIdsEncoding:
    """Issue #135: ``output.grid.cell_ids_encoding`` — ``cell_ids`` stays NESTED
    HEALPix uint64 by default; ``morton`` emits the packed morton words instead
    (a test/prototype capability). Default behavior is byte-identical."""

    @staticmethod
    def _cfg(encoding=None):
        cfg = default_config("atl06")
        if encoding is not None:
            cfg.output["grid"]["cell_ids_encoding"] = encoding
        return cfg

    def _grid(self, encoding=None):
        return HealpixGrid(
            parent_order=6, child_order=8, layout="fullsphere", config=self._cfg(encoding)
        )

    def test_default_and_explicit_nested_identical(self):
        parent = _valid_parents(1)[0]
        default = self._grid().chunk_coords(parent)
        nested = self._grid("nested").chunk_coords(parent)
        np.testing.assert_array_equal(default["cell_ids"], nested["cell_ids"])
        np.testing.assert_array_equal(
            np.asarray(default["morton"]._data), np.asarray(nested["morton"]._data)
        )
        # Default stays the NESTED encode, unchanged.
        g = self._grid()
        np.testing.assert_array_equal(default["cell_ids"], g.encode_cell_ids(g.children(parent)))

    def test_morton_encoding_emits_words(self):
        from zagg.grids.morton import morton_words

        g = self._grid("morton")
        parent = _valid_parents(1)[0]
        coords = g.chunk_coords(parent)
        children = np.asarray(g.children(parent), dtype=np.uint64)
        assert coords["cell_ids"].dtype == np.uint64
        np.testing.assert_array_equal(coords["cell_ids"], children)
        # The morton coordinate itself is unchanged (still the typed array).
        np.testing.assert_array_equal(morton_words(coords["morton"]), children)

    def test_morton_encoding_roundtrips_through_zarr(self):
        import pandas as pd

        from zagg.processing import write_dataframe_to_zarr

        cfg = self._cfg("morton")
        g = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        store = MemoryStore()
        g.emit_template(store)

        parent = _valid_parents(1)[0]
        coords = g.chunk_coords(parent)
        n = len(coords["cell_ids"])
        df = pd.DataFrame({"cell_ids": coords["cell_ids"]})
        df["morton"] = coords["morton"]
        for var in get_data_vars(cfg):
            df[var] = np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
        chunk_idx = g.block_index(parent)
        write_dataframe_to_zarr(df, store, grid=g, chunk_idx=chunk_idx)

        grp = open_group(store, path="8", mode="r")
        start = chunk_idx[0] * n
        stored = grp["cell_ids"][start : start + n]
        np.testing.assert_array_equal(stored, np.asarray(g.children(parent), dtype=np.uint64))
        assert stored.dtype == np.uint64

    def test_dggs_attrs_record_encoding(self):
        assert self._grid()._dggs_attrs()["dggs"]["indexing_scheme"] == "nested"
        assert self._grid("nested")._dggs_attrs()["dggs"]["indexing_scheme"] == "nested"
        assert self._grid("morton")._dggs_attrs()["dggs"]["indexing_scheme"] == "morton"

    def test_spatial_signature_unchanged_by_encoding(self):
        # The encoding changes coordinate VALUES, not the spatial layout, so
        # shard maps stay reusable across the flag (#89).
        assert self._grid("morton").spatial_signature() == self._grid().spatial_signature()

    def test_unknown_encoding_rejected_at_grid_construction(self):
        # coords_of (values) and _dggs_attrs (recorded scheme) both interpret the
        # string, so a grid built from an UNVALIDATED config must reject a third
        # value here — otherwise NESTED values would be recorded under a foreign
        # scheme name and consumers would mis-decode every cell.
        with pytest.raises(ValueError, match="Unknown cell_ids_encoding"):
            self._grid("ring")

    def test_signature_records_encoding(self):
        # The full fingerprint carries the encoding (issue #135) so the
        # validate_compatible rejection message names the actual mismatch;
        # spatial_signature (shard-map reuse) deliberately does not.
        assert self._grid().signature()["cell_ids_encoding"] == "nested"
        assert self._grid("morton").signature()["cell_ids_encoding"] == "morton"
        assert "cell_ids_encoding" not in self._grid("morton").spatial_signature()

    def test_mixed_encodings_do_not_nest(self):
        # NESTED ids and morton words are different id spaces: co-aggregating
        # them would let a consumer join on cell_ids and silently mismatch.
        nested, morton = self._grid(), self._grid("morton")
        assert not nested.nests_with(morton)
        assert not morton.nests_with(nested)
        # Same encoding still nests — including default vs explicit "nested",
        # so the gate changes nothing for anyone not using the flag.
        assert nested.nests_with(self._grid("nested"))
        assert morton.nests_with(self._grid("morton"))


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


class TestSharded:
    """Issue #108: HEALPix ShardingCodec output — one shard object per dispatch
    shard, K inner read-chunks bundled inside (empties omitted)."""

    # parent 4, child 8, chunk_inner 6 -> K = 4^(6-4) = 16 inner chunks/shard,
    # each cells_per_chunk = 4^(8-6) = 16 cells; cells_per_shard = 256.
    def _grid(self, sharded=True):
        cfg = default_config("atl06")
        return HealpixGrid(
            parent_order=4,
            child_order=8,
            layout="fullsphere",
            config=cfg,
            chunk_inner=6,
            sharded=sharded,
        )

    def test_k1_sharded_noop(self):
        cfg = default_config("atl06")
        # No chunk_inner -> K == 1 -> nothing to bundle: sharding is silently
        # disabled rather than rejected, so the True default is safe on a
        # single-chunk grid (issue #215).
        g = HealpixGrid(4, 8, layout="fullsphere", config=cfg, sharded=True)
        assert g.sharded is False

    def test_template_emits_sharding_codec(self):
        g = self._grid()
        assert g.cells_per_shard == 256
        assert g.cells_per_chunk == 16
        store = MemoryStore()
        g.emit_template(store)
        group = open_group(store, path="8", mode="r")
        for name in group:
            arr = group[name]
            # Sharded: zarr chunks == inner read chunk; shards == dispatch shard.
            assert arr.chunks == (16,), name
            assert arr.shards == (256,), name
            assert any("sharding" in type(c).__name__.lower() for c in arr.metadata.codecs), name

    def test_flag_off_byte_identical_to_regular(self):
        # Sharded OFF must reproduce the existing regular per-inner-chunk template
        # exactly. Both sides pass sharded=False explicitly (the constructor default
        # is now True — issue #215), isolating the opt-out storage form.
        g_off = self._grid(sharded=False)
        g_reg = HealpixGrid(
            4, 8, layout="fullsphere", config=default_config("atl06"), chunk_inner=6, sharded=False
        )
        assert g_off._spec().model_dump() == g_reg._spec().model_dump()

    def test_inner_codecs_are_bytes_only(self):
        # The bytes-only/uncompressed policy must survive the sharding wrap: the
        # inner codecs carry no compressor (caveat: zarr.create_array injects zstd).
        g = self._grid()
        spec = g._spec()
        h_mean = spec.members["h_mean"].model_dump()
        (codec,) = h_mean["codecs"]
        assert codec["name"] == "sharding_indexed"
        inner = codec["configuration"]["codecs"]
        assert [c["name"] for c in inner] == ["bytes"]
        assert codec["configuration"]["chunk_shape"] == [16]

    def test_one_shard_object_empties_omitted_and_roundtrip(self):
        from mortie import geo2mort
        from zarr import open_array

        g = self._grid()
        store = MemoryStore()
        g.emit_template(store)
        arr = open_array(store, path="8/h_mean", zarr_format=3, consolidated=False)

        # Populate ONE dispatch shard, only its first inner chunk (sub-shard
        # sparsity): a whole-shard slab that is mostly fill.
        parent = int(geo2mort(-78.5, -132.0, order=4)[0])
        (shard_block,) = g.block_index(parent)
        slab = np.full(g.cells_per_shard, np.nan, dtype="float32")
        slab[: g.cells_per_chunk] = np.arange(g.cells_per_chunk, dtype="float32")
        # Block selection is shard-granular on a sharded array (issue #108): one
        # block == one shard, so this writes the whole shard in a single call.
        arr.set_block_selection((shard_block,), slab)

        # Exactly one shard object on disk for the one populated shard.
        shard_keys = [k for k in store._store_dict if k.startswith("8/h_mean/c/")]
        assert shard_keys == [f"8/h_mean/c/{shard_block}"]

        # Byte-exact read-back of the whole shard.
        lo = shard_block * g.cells_per_shard
        read = arr[lo : lo + g.cells_per_shard]
        np.testing.assert_array_equal(read[: g.cells_per_chunk], np.arange(g.cells_per_chunk))
        assert np.all(np.isnan(read[g.cells_per_chunk :]))

        # Partial decode: read just the populated inner (read) chunk via a slice
        # narrower than the shard — the sharding index serves it without the rest.
        chunk = arr[lo : lo + g.cells_per_chunk]
        np.testing.assert_array_equal(chunk, np.arange(g.cells_per_chunk))

    def test_sparse_shard_omits_empty_inner_chunks(self):
        # A shard object holding one populated 16-cell inner chunk must be far
        # smaller than a fully-dense shard (empty inner chunks absent from index).
        from mortie import geo2mort
        from zarr import open_array

        g = self._grid()
        store = MemoryStore()
        g.emit_template(store)
        arr = open_array(store, path="8/h_mean", zarr_format=3, consolidated=False)
        parent = int(geo2mort(-78.5, -132.0, order=4)[0])
        (shard_block,) = g.block_index(parent)

        sparse = np.full(g.cells_per_shard, np.nan, dtype="float32")
        sparse[: g.cells_per_chunk] = 1.0
        arr.set_block_selection((shard_block,), sparse)
        sparse_bytes = len(store._store_dict[f"8/h_mean/c/{shard_block}"])

        dense = np.ones(g.cells_per_shard, dtype="float32")
        arr.set_block_selection((shard_block,), dense)
        dense_bytes = len(store._store_dict[f"8/h_mean/c/{shard_block}"])

        assert sparse_bytes < dense_bytes

    def test_vector_field_shards_and_roundtrips(self):
        # A kind: vector field's trailing payload dim stays whole inside the shard:
        # outer shard (cells_per_shard, T), inner chunk (cells_per_chunk, T). A whole-
        # shard slab round-trips, and one shard object holds the K inner chunks.
        from mortie import geo2mort
        from zarr import open_array

        g = HealpixGrid(
            parent_order=4,
            child_order=8,
            layout="fullsphere",
            config=_vector_config(bins=4, dtype="float32"),
            chunk_inner=6,
            sharded=True,
        )
        store = MemoryStore()
        g.emit_template(store)
        arr = open_array(store, path="8/hist", zarr_format=3, consolidated=False)
        assert arr.chunks == (g.cells_per_chunk, 4)
        assert arr.shards == (g.cells_per_shard, 4)

        parent = int(geo2mort(-78.5, -132.0, order=4)[0])
        (shard_block,) = g.block_index(parent)
        slab = np.zeros((g.cells_per_shard, 4), dtype="float32")
        slab[: g.cells_per_chunk] = np.arange(g.cells_per_chunk * 4).reshape(g.cells_per_chunk, 4)
        arr.set_block_selection((shard_block, 0), slab)

        shard_keys = [k for k in store._store_dict if k.startswith("8/hist/c/")]
        assert shard_keys == [f"8/hist/c/{shard_block}/0"]
        lo = shard_block * g.cells_per_shard
        np.testing.assert_array_equal(arr[lo : lo + g.cells_per_shard], slab)

    def test_sharded_not_in_signature(self):
        # sharded is a storage form, not a spatial-layout change, so it must not
        # alter the shard-map fingerprint (parallels chunk_inner's exclusion).
        cfg = default_config("atl06")
        g0 = HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=6)
        g1 = HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        assert g0.signature() == g1.signature()
