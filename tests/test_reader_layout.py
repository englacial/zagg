"""Spatial-fidelity goldens for the reader deinterleave — issue #336.

The tensor readers place a cell at the bit deinterleave of its chunk-local
nested rank (``readers._layout``), pinned to the normative convention in
mortie's ``docs/specification.md`` §8 (healpy ``pix2xyf`` face-local frame;
gridlook ``bit_combine(j, i)`` texture orientation: row = y, col = x). The
golden vectors are IMPORTED from mortie's merged test suite
(``mortie/tests/test_rank_xy.py``, espg/mortie#150) rather than re-derived,
so zagg cannot drift from the spec by construction.
"""

import numpy as np
import pytest
from test_readers import _KEY_A, _KEY_B, _build_store, _sharded_store
from zarr.storage import MemoryStore

from zagg.grids.morton import morton_word
from zagg.readers._layout import rank_to_rowcol, rowcol_to_rank, subtree_cell_span
from zagg.readers.tdigest_tensor import cell_index, read_cell, read_raw_values, read_tensors

# Golden (rank, x, y) triples copied verbatim from mortie's merged test file
# mortie/tests/test_rank_xy.py (espg/mortie#150) — provenance: healpy 1.20.0
# `pix2xyf(2**depth, rank, nest=True)` (face 0), rng seed 149; the normative
# convention pinned by mortie docs/specification.md §8. Depth 6 = the 64x64
# inner chunk, depth 8 = the 256x256 shard.
GOLDEN_DEPTH6 = [
    (0, 0, 0),
    (1, 1, 0),
    (2, 0, 1),
    (3, 1, 1),
    (335, 27, 3),
    (907, 17, 27),
    (1069, 35, 6),
    (1265, 45, 12),
    (1873, 61, 16),
    (2048, 0, 32),
    (2135, 15, 33),
    (2511, 27, 43),
    (3024, 28, 56),
    (3369, 49, 38),
    (3592, 32, 50),
    (4095, 63, 63),
]
GOLDEN_DEPTH8 = [
    (0, 0, 0),
    (1, 1, 0),
    (2, 0, 1),
    (3, 1, 1),
    (3149, 43, 34),
    (13006, 74, 91),
    (30094, 242, 75),
    (32094, 254, 99),
    (32768, 0, 128),
    (35193, 29, 166),
    (36422, 42, 177),
    (45487, 83, 207),
    (58565, 171, 200),
    (61371, 181, 255),
    (65052, 230, 242),
    (65535, 255, 255),
]


class TestLayoutGoldens:
    """The helper pair matches the mortie spec §8 golden vectors exactly."""

    @pytest.mark.parametrize("depth,golden", [(6, GOLDEN_DEPTH6), (8, GOLDEN_DEPTH8)])
    def test_rank_to_rowcol_golden(self, depth, golden):
        ranks, xs, ys = (np.array(col, dtype=np.uint64) for col in zip(*golden))
        row, col = rank_to_rowcol(ranks, depth)
        # Orientation contract: row = y, col = x (gridlook bit_combine(j, i)).
        np.testing.assert_array_equal(row, ys)
        np.testing.assert_array_equal(col, xs)

    @pytest.mark.parametrize("depth,golden", [(6, GOLDEN_DEPTH6), (8, GOLDEN_DEPTH8)])
    def test_rowcol_to_rank_golden(self, depth, golden):
        ranks, xs, ys = (np.array(col, dtype=np.uint64) for col in zip(*golden))
        np.testing.assert_array_equal(rowcol_to_rank(ys, xs, depth), ranks)

    @pytest.mark.parametrize("depth", [1, 2, 6, 8, 13])
    def test_round_trip(self, depth):
        rng = np.random.default_rng(336 + depth)
        n = int(min(4**depth, 2048))
        ranks = rng.integers(0, 4**depth, size=n, dtype=np.uint64)
        row, col = rank_to_rowcol(ranks, depth)
        assert row.max() < 2**depth and col.max() < 2**depth
        np.testing.assert_array_equal(rowcol_to_rank(row, col, depth), ranks)
        row2, col2 = rank_to_rowcol(rowcol_to_rank(row, col, depth), depth)
        np.testing.assert_array_equal(row2, row)
        np.testing.assert_array_equal(col2, col)


def _tensor_for(cell_to_values, **kwargs):
    """One 64x64 chunk's float32 tensor from a real store write of _KEY_A."""
    store, _grid, _words = _build_store({_KEY_A: cell_to_values})
    out = list(read_tensors(store, "12/h_tdigest", dtype="float32", **kwargs))
    assert len(out) == 1
    tensor, _mask, _scale, morton = out[0]
    assert morton == morton_word(_KEY_A)
    return tensor


class TestTensorPlacement:
    """Golden-driven placement through the production write + read path."""

    def test_golden_ranks_land_at_golden_xy(self):
        # One distinguishable digest per golden rank: cell i holds 10*(i+1)
        # samples, so the per-cell tensor mass identifies which digest landed
        # where. All cells share one value range → one shared z-window.
        rng = np.random.default_rng(1)
        counts = {rank: 10 * (i + 1) for i, (rank, _x, _y) in enumerate(GOLDEN_DEPTH6)}
        cells = {rank: rng.uniform(10.0, 30.0, n) for rank, n in counts.items()}
        t = _tensor_for(cells, bottom=0.0, top=1.0)
        mass = t.sum(axis=2)
        for rank, x, y in GOLDEN_DEPTH6:
            assert mass[y, x] == pytest.approx(counts[rank], rel=0.01)
        # Exactly the golden positions are populated.
        rows, cols = np.nonzero(mass)
        assert {(int(r), int(c)) for r, c in zip(rows, cols)} == {
            (y, x) for _rank, x, y in GOLDEN_DEPTH6
        }

    def test_sphere_adjacent_cells_land_tensor_adjacent(self):
        """The case the row-major reshape fails (issue #336): ranks 0..3 are
        one nested quad — a 2x2 block of mutually adjacent cells on the
        sphere (mortie spec §8.1: child tuples order (x, y) = (0,0), (1,0),
        (0,1), (1,1)) — so they must land as a 2x2 block in the tensor.
        ``divmod(rank, side)`` strung them along row 0 as (0,0)..(0,3),
        tearing the quad's two sphere-adjacent rows 64 columns apart."""
        rng = np.random.default_rng(2)
        counts = {rank: 10 * (rank + 1) for rank in range(4)}
        cells = {rank: rng.uniform(10.0, 30.0, n) for rank, n in counts.items()}
        mass = _tensor_for(cells, bottom=0.0, top=1.0).sum(axis=2)
        rows, cols = np.nonzero(mass)
        assert {(int(r), int(c)) for r, c in zip(rows, cols)} == {
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
        }
        # And in the spec-§8.1 order: rank 1 east of rank 0, rank 2 north.
        for rank, (row, col) in [(0, (0, 0)), (1, (0, 1)), (2, (1, 0)), (3, (1, 1))]:
            assert mass[row, col] == pytest.approx(counts[rank], rel=0.01)

    def test_reported_rowcol_matches_tensor_position(self):
        """read_raw_values / read_locations report the SAME (row, col) the
        tensor places the cell at — one convention across the readers."""
        from conftest import point_words

        from zagg.readers.tdigest_tensor import read_locations, read_raw_values

        ranks = [rank for rank, _x, _y in GOLDEN_DEPTH6]
        vals = {rank: np.array([float(rank), float(rank) + 1.0]) for rank in ranks}
        locs = {rank: point_words(2, rank + 1) for rank in ranks}
        store, _grid, _words = _build_store({_KEY_A: vals}, located_locs={_KEY_A: locs})
        expected = {(y, x) for _rank, x, y in GOLDEN_DEPTH6}
        raw_positions = {rc for _m, rc, _v in read_raw_values(store, "12/h_tdigest")}
        loc_positions = {rc for _m, rc, _v in read_locations(store, "12/h_tdigest")}
        assert raw_positions == loc_positions == expected
        # Values identify the cell: rank recovered from (row, col) round-trips.
        for _m, (row, col), v in read_raw_values(store, "12/h_tdigest"):
            assert v[0] == float(rowcol_to_rank(row, col, 6))


class TestCellIndexComposition:
    """A reported ``(row, col)`` composes all the way to :func:`read_cell`.

    ``rowcol_to_rank`` inverts to the CHUNK-LOCAL rank; ``read_cell`` keys on
    the global cells axis, so ``cell_index`` supplies the chunk start."""

    _FIELD = "12/h_tdigest"

    def _store(self):
        rng = np.random.default_rng(7)
        # 3 samples per cell → no merged centroids, so read_raw_values recovers
        # the exact means and read_cell's payload can be compared to them.
        vals = {rank: np.sort(rng.uniform(10.0, 30.0, 3)) for rank in (0, 5, 11, 4095)}
        store, _grid, _words = _build_store({_KEY_A: vals, _KEY_B: vals})
        return store

    def test_reported_rowcol_round_trips_to_read_cell(self):
        store = self._store()
        seen = 0
        for morton, (row, col), values in read_raw_values(store, self._FIELD):
            cell = cell_index(store, self._FIELD, morton, row, col)
            digest = read_cell(store, self._FIELD, cell)
            np.testing.assert_allclose(digest[:, 0], values, rtol=1e-6)
            seen += 1
        assert seen == 8  # 4 cells × 2 shards

    def test_bare_chunk_local_rank_reads_the_wrong_cell(self):
        # The composition the docs used to claim: a chunk-local rank is always
        # in range, so read_cell(rank) picks a different cell and does NOT
        # raise — which is why cell_index exists.
        store = self._store()
        for morton, (row, col), _values in read_raw_values(store, self._FIELD):
            rank = int(rowcol_to_rank(row, col, 6))
            cell = cell_index(store, self._FIELD, morton, row, col)
            assert cell != rank
            assert len(read_cell(store, self._FIELD, rank)) == 0  # silently empty

    def test_block_id_and_out_of_block_position_raise(self):
        store = MemoryStore()
        _grid, shard6, targets = _sharded_store(store, populate={0, 1})
        # The order-8 read chunks report order-8 ids; the order-6 shard id
        # (what block_order=6 would yield) names no single chunk.
        with pytest.raises(ValueError, match="no stored read chunk"):
            cell_index(store, self._FIELD, shard6, 3, 1)
        chunk_word = next(m for m, _rc, _v in read_raw_values(store, self._FIELD))
        assert cell_index(store, self._FIELD, chunk_word, 3, 1) == targets[0]
        with pytest.raises(ValueError, match="outside"):
            cell_index(store, self._FIELD, chunk_word, 16, 0)  # 16×16 read chunk


class TestBlockAssembly:
    """``block_order=`` assembles inner-chunk tensors into one block tensor
    (issue #336 phase 3). Geometry: order-6 shard of K=16 order-8 chunks
    (16×16 cells each, cell order 12), so block_order 7 = 4 chunks (32×32)
    and block_order 6 = the whole shard (64×64). Each populated chunk holds
    one digest at cell rank 11 → (row, col) = (3, 1) within its tile."""

    def _store(self, populate, values=None):
        store = MemoryStore()
        _grid, shard6, _targets = _sharded_store(store, populate=populate, values=values)
        return store, shard6

    def test_block_bit_identical_to_per_chunk_tiles(self):
        # Identical values in every populated chunk → each chunk's window ==
        # the shared block window → block tiles are bit-identical to the
        # per-chunk tensors (the acceptance pin for the assembly path).
        vals = np.random.default_rng(3).uniform(5.0, 25.0, 400)
        populate = {0, 3, 7, 12}
        store, shard6 = self._store(populate, values={k: vals for k in populate})

        per_chunk = list(read_tensors(store, "12/h_tdigest"))
        assert len(per_chunk) == len(populate)
        ((block, _mask, _scale, word),) = read_tensors(store, "12/h_tdigest", block_order=6)
        assert word == shard6
        assert block.shape == (64, 64, 128)

        seen = np.zeros((4, 4), dtype=bool)
        for local, (chunk_t, _cm, _cs, _cw) in zip(sorted(populate), per_chunk):
            trow, tcol = rank_to_rowcol(local, 2)  # tile position at depth 2
            tile = block[trow * 16 : (trow + 1) * 16, tcol * 16 : (tcol + 1) * 16, :]
            np.testing.assert_array_equal(tile, chunk_t)
            seen[trow, tcol] = True
        # Every other tile is all zero (unpopulated chunks contribute nothing).
        for trow in range(4):
            for tcol in range(4):
                if not seen[trow, tcol]:
                    assert (
                        block[trow * 16 : (trow + 1) * 16, tcol * 16 : (tcol + 1) * 16].sum() == 0
                    )

    def test_block_order_7_groups_and_coarsens_morton(self):
        from mortie import generate_morton_children

        store, shard6 = self._store({0, 1, 2, 3, 5})
        out = list(read_tensors(store, "12/h_tdigest", block_order=7))
        assert [o[0].shape for o in out] == [(32, 32, 128)] * 2
        # Block ids are the order-7 children of the shard, in nested order.
        kids = [int(k) for k in np.asarray(generate_morton_children(shard6, 7))]
        assert [m for *_o, m in out] == kids[:2]
        # Block 0: chunks 0..3 populated at cell rank 11 → tile (r, c) of the
        # 2x2 chunk grid, cell (3, 1) within each 16x16 tile.
        mass0 = out[0][0].sum(axis=2)
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mass0))} == {
            (3, 1),
            (3, 17),
            (19, 1),
            (19, 17),
        }
        # Block 1: only chunk 5 (local rank 1 within the block → tile (0, 1)).
        mass1 = out[1][0].sum(axis=2)
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mass1))} == {(3, 17)}

    def test_window_reconciled_block_wide(self):
        rng = np.random.default_rng(4)
        # Two chunks whose ranges only fit ONE 64 m window jointly anchored
        # at the global floor: [0, 20] and [40, 60].
        values = {0: rng.uniform(0.0, 20.0, 300), 1: rng.uniform(40.0, 60.0, 300)}
        store, _shard6 = self._store({0, 1}, values=values)
        ((block, _mask, (offset, gain), _w),) = read_tensors(
            store, "12/h_tdigest", block_order=7, bottom=0.0, top=1.0, dtype="float32"
        )
        # One shared offset/gain for the whole block, anchored at the global floor.
        assert offset == 0.0 and gain == 0.5
        mass = block.sum(axis=2)
        # Both cells' full populations are in the one shared window.
        assert mass[3, 1] == pytest.approx(300, rel=0.01)
        assert mass[3, 17] == pytest.approx(300, rel=0.01)

    def test_fit_raise_decided_block_wide(self):
        rng = np.random.default_rng(5)
        # Each chunk fits a 64 m window alone, but jointly they span ~100 m —
        # the block-wide decision must raise where per-chunk reads pass.
        values = {0: rng.uniform(0.0, 20.0, 300), 1: rng.uniform(80.0, 100.0, 300)}
        store, _shard6 = self._store({0, 1}, values=values)
        assert len(list(read_tensors(store, "12/h_tdigest", bottom=0.0, top=1.0))) == 2
        with pytest.raises(ValueError, match="exceeds the fixed window"):
            list(read_tensors(store, "12/h_tdigest", block_order=7, bottom=0.0, top=1.0))

    def test_block_order_out_of_range_raises(self):
        store, _shard6 = self._store({0})
        for bad in (9, -1):  # chunk order is 8
            with pytest.raises(ValueError, match="block_order .* out of range"):
                list(read_tensors(store, "12/h_tdigest", block_order=bad))

    @pytest.mark.parametrize("block_order,side", [(5, 128), (4, 256)])
    def test_block_coarser_than_the_shard_assembles(self, block_order, side):
        """A block COARSER than the stored shard assembles too (fold review):
        on a nested-ordered cells axis the block-local index is still the
        nested rank, so the shard's 64×64 tensor lands whole at the
        deinterleave of the shard's rank within the coarser parent. The
        axis-length check is what rejects an untileable block order."""
        from mortie import clip2order, generate_morton_children

        populate = {0, 3, 7, 12}
        store, shard6 = self._store(populate)
        ((block, _mask, _scale, word),) = read_tensors(
            store, "12/h_tdigest", block_order=block_order
        )
        assert block.shape == (side, side, 128)
        parent = int(clip2order(block_order, shard6)[0])
        assert word == parent
        # The whole order-6 shard tensor, placed at its rank within `parent`.
        ((shard_tensor, *_rest),) = read_tensors(store, "12/h_tdigest", block_order=6)
        kids = [int(k) for k in np.asarray(generate_morton_children(parent, 6))]
        assert len(kids) == 4 ** (6 - block_order)
        row, col = (int(v) * 64 for v in rank_to_rowcol(kids.index(shard6), 6 - block_order))
        np.testing.assert_array_equal(block[row : row + 64, col : col + 64, :], shard_tensor)
        # Nothing outside the shard's tile (every other order-6 child is empty).
        assert block.sum() == shard_tensor.sum()

    def test_block_tensor_over_max_block_bytes_raises(self):
        """The ``block_order`` memory footgun fails with a pointed error naming
        the size and the limit, not a bare ``MemoryError`` from the allocator."""
        store, _shard6 = self._store({0})
        with pytest.raises(ValueError) as exc:
            list(read_tensors(store, "12/h_tdigest", block_order=4, max_block_bytes=1024))
        msg = str(exc.value)
        assert "block_order=4" in msg and "256×256×128 uint32" in msg
        assert "33554432 bytes" in msg and "1024-byte max_block_bytes limit" in msg
        # The same block is admitted under the (generous) default cap.
        assert len(list(read_tensors(store, "12/h_tdigest", block_order=4))) == 1

    def test_morton_not_in_nested_order_raises(self):
        """The corrected guard in ``_chunk_word``: a span whose ``morton``
        coordinate is NOT nested-ordered (cells from two subtrees) cannot be
        indexed by cells-axis arithmetic, so it raises."""
        import zarr

        vals = np.random.default_rng(8).uniform(5.0, 25.0, 100)
        store, grid, words = _build_store({_KEY_A: {0: vals, 1: vals}})
        morton = zarr.open_array(store, path="12/morton", mode="r+")
        base = grid.block_index(words[_KEY_A])[0] * grid.cells_per_chunk
        # Graft a cell from the OTHER shard's subtree over a written cell.
        morton[base + 1] = np.asarray(grid.children(morton_word(_KEY_B)))[1]
        with pytest.raises(ValueError, match="not in nested order"):
            list(read_tensors(store, "12/h_tdigest"))

    def test_chunk_order_block_matches_default(self):
        store, _shard6 = self._store({0, 5})
        default = list(read_tensors(store, "12/h_tdigest"))
        explicit = list(read_tensors(store, "12/h_tdigest", block_order=8))
        assert [m for *_o, m in default] == [m for *_o, m in explicit]
        for (t_d, m_d, s_d, _w1), (t_e, m_e, s_e, _w2) in zip(default, explicit):
            np.testing.assert_array_equal(t_d, t_e)
            np.testing.assert_array_equal(m_d, m_e)
            assert s_d == s_e


def _put_object(store, key, payload):
    """PUT raw bytes into a zarr store (the MemoryStore sidecar write)."""
    from zarr.core.buffer import default_buffer_prototype
    from zarr.core.sync import sync

    sync(store.set(key, default_buffer_prototype().buffer.from_bytes(payload)))


def _del_object(store, key):
    """DELETE one raw store object (a leaf whose sidecar went missing)."""
    from zarr.core.sync import sync

    sync(store.delete(key))


class TestMaskChannel:
    """Issue #336 phase 4: the leaf's ``coverage.moc`` occupancy sidecar
    decodes into the deinterleaved mask channel — 0 unobserved, 1 observed
    with no stored digest, 2 observed with data — reusing the frozen bitmap
    convention (``hive.decode_coverage_bitmap``), one small sidecar object
    and no digest bytes."""

    def _leaf(self, digest_ranks, extra_occupied=(), *, full=False, stamp=True, cell_order=12):
        """A committed hive leaf (K==1 order-6 shard, 64×64 cells) with
        digests at ``digest_ranks``; occupancy = digests + ``extra_occupied``."""
        import zarr
        from test_readers import _cfg

        from zagg import hive
        from zagg.grids import HealpixGrid
        from zagg.processing import write_ragged_leaf_to_zarr
        from zagg.stats.tdigest import build_tdigest

        rng = np.random.default_rng(6)
        grid = HealpixGrid(6, 12, layout="fullsphere", config=_cfg(), sharded=False)
        word = morton_word(_KEY_A)
        store = MemoryStore()
        grid.emit_shard_template(store)
        children = np.asarray(grid.children(word), dtype=np.uint64)
        zarr.open_array(store, path="12/morton", mode="r+")[:] = children
        ranks = sorted(digest_ranks)
        payloads = [build_tdigest(rng.uniform(10.0, 30.0, 50), delta=512) for _ in ranks]
        write_ragged_leaf_to_zarr([((0,), {"h_tdigest": (payloads, ranks)})], store, grid=grid)
        occupied = np.sort(children[sorted(set(ranks) | set(extra_occupied))])
        bitmap = None if full else hive.encode_coverage_bitmap(word, occupied, 12)
        if bitmap is not None:
            _put_object(store, hive.COVERAGE_SIDECAR, bitmap)
        if stamp:
            hive.stamp_commit(
                store,
                cells_with_data=len(ranks),
                granule_count=1,
                coverage=hive.build_coverage(word, occupied, cell_order, bitmap=bitmap, full=full),
            )
        return store

    @staticmethod
    def _read_one(store):
        ((tensor, mask, _scale, _word),) = read_tensors(store, "12/h_tdigest")
        return tensor, mask

    def test_mask_aligns_with_digest_coverage(self):
        digest_ranks = [0, 5, 11, 4095]
        tensor, mask = self._read_one(self._leaf(digest_ranks))
        expected2 = {tuple(map(int, rank_to_rowcol(r, 6))) for r in digest_ranks}
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mask == 2))} == expected2
        # Pre-#334 leaf: occupancy == digest coverage, so no state-1 cells,
        # and the mask aligns with the tensor's per-cell mass exactly.
        assert not np.any(mask == 1)
        np.testing.assert_array_equal(mask == 2, tensor.sum(axis=2) > 0)

    def test_noise_occupied_reads_as_observed_no_signal(self):
        # An occupied cell WITHOUT a stored digest (the #334 noise stratum
        # shape) reads as state 1 with no reader change — data-driven upgrade.
        store = self._leaf([0, 5], extra_occupied=[7, 335])
        _tensor, mask = self._read_one(store)
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mask == 1))} == {
            tuple(map(int, rank_to_rowcol(r, 6))) for r in (7, 335)
        }
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mask == 2))} == {
            tuple(map(int, rank_to_rowcol(r, 6))) for r in (0, 5)
        }

    def test_full_encoding_marks_whole_block_observed(self):
        _tensor, mask = self._read_one(self._leaf([3], full=True))
        assert mask.min() == 1  # D14: the shard id IS the exact MOC
        assert {tuple(map(int, p)) for p in zip(*np.nonzero(mask == 2))} == {
            tuple(map(int, rank_to_rowcol(3, 6)))
        }

    def test_unstamped_store_degrades_to_two_state(self):
        # Sidecar bytes without a commit stamp are debris (D4): the mask
        # degrades to populated/not, never a half-trusted occupancy.
        _tensor, mask = self._read_one(self._leaf([0, 5], stamp=False))
        assert set(np.unique(mask)) == {0, 2}

    def test_has_exact_occupancy_discriminates_the_two_regimes(self):
        """The yielded mask cannot tell a degraded 2-state channel from a
        3-state one with no observed-but-empty cell — both are ``{0, 2}``.
        ``has_exact_occupancy`` is the discriminator (fold review)."""
        from zagg import hive
        from zagg.readers.tdigest_tensor import has_exact_occupancy

        exact = self._leaf([0, 5])
        degraded = self._leaf([0, 5], stamp=False)
        # Identical mask value sets, opposite semantics for `0`.
        assert set(np.unique(self._read_one(exact)[1])) == {0, 2}
        assert set(np.unique(self._read_one(degraded)[1])) == {0, 2}
        assert has_exact_occupancy(exact)
        assert not has_exact_occupancy(degraded)
        assert has_exact_occupancy(self._leaf([3], full=True))
        # A stamped leaf whose sidecar object is gone degrades too, and the
        # predicate tracks the reader (it shares the same resolution path).
        no_sidecar = self._leaf([0, 5])
        _del_object(no_sidecar, hive.COVERAGE_SIDECAR)
        assert not has_exact_occupancy(no_sidecar)
        assert set(np.unique(self._read_one(no_sidecar)[1])) == {0, 2}

    def test_corrupt_sidecar_raises(self):
        # A wrong-size (but valid-zstd) sidecar must raise, not zero-pad — the
        # decode_coverage_bitmap posture, surfaced through the reader.
        from numcodecs import Zstd

        from zagg import hive

        store = self._leaf([0])
        _put_object(store, hive.COVERAGE_SIDECAR, bytes(Zstd().encode(b"\x00")))
        with pytest.raises(ValueError, match="refusing to zero-pad"):
            list(read_tensors(store, "12/h_tdigest"))

    def test_cell_order_mismatch_raises_pointed(self):
        store = self._leaf([0], cell_order=11)
        with pytest.raises(ValueError, match="cannot be aligned"):
            list(read_tensors(store, "12/h_tdigest"))


class TestMaskBlockCrossing:
    """The mask channel (phase 4) crossed with ``block_order`` (phase 3) — the
    one seam where ``_block_mask`` slices a leaf-wide bitmap against a
    MULTI-CHUNK window and deinterleaves at ``block_depth != depth`` (fold
    review). A stamped 16-chunk leaf: order-6 shard, order-12 cells, order-8
    read chunks (16×16), digests at leaf ranks 11/300/1000/4000 and extra
    occupancy at 12/301/2500."""

    DIGESTS = [11, 300, 1000, 4000]
    EXTRA = [12, 301, 2500]

    @staticmethod
    def _leaf(digest_ranks, extra_occupied):
        """A committed 16-chunk leaf (``chunk_inner=8``, ShardingCodec'd)."""
        import zarr
        from test_readers import _cfg

        from zagg import hive
        from zagg.grids import HealpixGrid
        from zagg.processing import write_ragged_leaf_to_zarr
        from zagg.stats.tdigest import build_tdigest

        cfg = _cfg()
        cfg.output["grid"]["chunk_inner"] = 8
        cfg.output["grid"]["sharded"] = True
        grid = HealpixGrid(6, 12, layout="fullsphere", config=cfg, chunk_inner=8, sharded=True)
        word = morton_word(_KEY_A)
        store = MemoryStore()
        grid.emit_shard_template(store)
        children = np.asarray(grid.children(word), dtype=np.uint64)
        zarr.open_array(store, path="12/morton", mode="r+")[:] = children
        # Digests go in at leaf-LOCAL chunk blocks with chunk-local ranks.
        rng = np.random.default_rng(9)
        per_chunk: dict[int, list[int]] = {}
        for rank in sorted(digest_ranks):
            per_chunk.setdefault(rank // grid.cells_per_chunk, []).append(
                rank % grid.cells_per_chunk
            )
        entries = [
            (
                (block,),
                {
                    "h_tdigest": (
                        [build_tdigest(rng.uniform(10.0, 30.0, 50), delta=512) for _ in local],
                        local,
                    )
                },
            )
            for block, local in sorted(per_chunk.items())
        ]
        write_ragged_leaf_to_zarr(entries, store, grid=grid)
        occupied = np.sort(children[sorted(set(digest_ranks) | set(extra_occupied))])
        bitmap = hive.encode_coverage_bitmap(word, occupied, 12)
        _put_object(store, hive.COVERAGE_SIDECAR, bitmap)
        hive.stamp_commit(
            store,
            cells_with_data=len(digest_ranks),
            granule_count=1,
            coverage=hive.build_coverage(word, occupied, 12, bitmap=bitmap),
        )
        return store

    @staticmethod
    def _states(mask):
        return tuple(
            sorted(tuple(map(int, p)) for p in zip(*np.nonzero(mask == state))) for state in (1, 2)
        )

    @pytest.mark.parametrize(
        "block_order,side,expected",
        [
            # Per chunk (block_depth == depth == 4): ranks are chunk-local.
            (8, 16, [([(2, 2)], [(3, 1)]), ([(6, 3)], [(6, 2)]), ([], [(14, 8)]), ([], [(12, 0)])]),
            # 4-chunk blocks (block_depth 5): only POPULATED blocks are yielded,
            # so leaf block 2 (occupancy 2500 but no digest) is not emitted.
            (7, 32, [([(2, 2), (6, 19)], [(3, 1), (6, 18), (30, 24)]), ([], [(28, 16)])]),
            # The whole leaf (block_depth 6): every rank at its leaf-local
            # deinterleave — 12→(2,2), 301→(6,19), 2500→(40,26) observed-only;
            # 11→(3,1), 300→(6,18), 1000→(30,24), 4000→(60,48) with data.
            (
                6,
                64,
                [
                    (
                        [(2, 2), (6, 19), (40, 26)],
                        [(3, 1), (6, 18), (30, 24), (60, 48)],
                    )
                ],
            ),
        ],
    )
    def test_mask_deinterleaves_at_the_block_depth(self, block_order, side, expected):
        store = self._leaf(self.DIGESTS, self.EXTRA)
        out = list(read_tensors(store, "12/h_tdigest", block_order=block_order))
        assert [o[1].shape for o in out] == [(side, side)] * len(expected)
        for (_t, mask, _s, _w), (ones, twos) in zip(out, expected):
            assert self._states(mask) == (ones, twos)

    def test_block_mask_states_agree_with_the_per_chunk_read(self):
        """Assembling the mask must not invent or lose occupancy: the leaf
        block's state counts equal the per-chunk reads' totals."""
        store = self._leaf(self.DIGESTS, self.EXTRA)
        chunk_masks = [mask for _t, mask, _s, _w in read_tensors(store, "12/h_tdigest")]
        ((tensor, block_mask, _s, _w),) = read_tensors(store, "12/h_tdigest", block_order=6)
        assert int((block_mask == 2).sum()) == sum(int((m == 2).sum()) for m in chunk_masks)
        # Cell 2500 is occupancy-only in a chunk with no digest at all, so the
        # per-chunk sweep never yields it — the whole-leaf block does.
        assert int((block_mask == 1).sum()) == sum(int((m == 1).sum()) for m in chunk_masks) + 1
        # And the digest-bearing cells still match the tensor's mass exactly.
        np.testing.assert_array_equal(block_mask == 2, tensor.sum(axis=2) > 0)


class TestSubtreeSpanBit63:
    """Southern (base 7-11) words set bit 63, so ``np.asarray(word)`` infers
    uint64 where a northern word infers int64 — two mortie kernel paths since
    the explicit ``dtype=np.uint64`` boxing came off (issue #543). Every other
    subtree test runs on the northern ``_KEY_A``/``_KEY_B`` stores, so this
    pins the uint64 branch against the int64 one."""

    @staticmethod
    def _spans(shard):
        """``(whole-shard span, per-order-9-child spans)`` on an order-12,
        single-root (4**6) cells axis anchored at the shard's first cell."""
        from mortie import generate_morton_children

        anchor = int(generate_morton_children(shard, 12)[0])
        whole = subtree_cell_span(shard, anchor, 0, 12, 4096, "12/h_tdigest")
        kids = [
            subtree_cell_span(int(c), anchor, 0, 12, 4096, "12/h_tdigest")
            for c in generate_morton_children(shard, 9)
        ]
        return whole, kids

    def test_southern_word_spans_match_the_northern_path(self):
        from mortie import clip2order, geo2mort

        south = int(clip2order(6, geo2mort(-78.5, -132.0, order=18))[0])
        north = morton_word(_KEY_A)
        assert south >= 2**63 and np.asarray(south).dtype == np.uint64
        assert north < 2**63 and np.asarray(north).dtype == np.int64

        whole, kids = self._spans(south)
        assert whole == (0, 4096)
        assert kids == [(i * 64, (i + 1) * 64) for i in range(64)]
        assert (whole, kids) == self._spans(north)
