"""Spill-partition writer/reader (issue #217, phase 1).

The spill files must (a) round-trip every byte exactly (they replace the
in-memory pool, so any loss is silent data corruption), (b) route rows to the
partition of their enclosing inner chunk, (c) leave nothing in ``/tmp`` — the
files are unlinked at birth and every fd is accounted for — and (d) fail
loudly, naming the ``-disk`` variant fix, when ``/tmp`` cannot hold the spill.
"""

import tempfile

import numpy as np
import pytest

from zagg.grids import HealpixGrid
from zagg.processing.spill import (
    SpillBlock,
    check_tmp_headroom,
    partition_ids,
)


def _cols(n, seed=0):
    rng = np.random.default_rng(seed)
    return {
        "h_ph": rng.normal(0.0, 10.0, n).astype(np.float32),
        "leaf_id": rng.integers(0, 2**40, n).astype(np.uint64),
        "q": rng.integers(0, 127, n).astype(np.int8),
    }


class TestRoundTrip:
    def test_single_partition_exact(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        cols = _cols(1000)
        cells = np.sort(np.random.default_rng(1).integers(0, 50, 1000).astype(np.uint64))
        block.append(np.zeros(1000, dtype=np.uint64), cells, cols)
        got_cells, got_cols = block.read_partition(0)
        np.testing.assert_array_equal(got_cells, cells)
        assert got_cells.dtype == cells.dtype
        for name, arr in cols.items():
            np.testing.assert_array_equal(got_cols[name], arr)
            assert got_cols[name].dtype == arr.dtype
        block.close()

    def test_multi_append_concatenates_in_order(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        chunks = [_cols(n, seed=n) for n in (10, 3, 27)]
        cell_chunks = [np.full(len(c["h_ph"]), i, dtype=np.uint64) for i, c in enumerate(chunks)]
        for cells, cols in zip(cell_chunks, chunks):
            block.append(np.zeros(len(cells), dtype=np.uint64), cells, cols)
        got_cells, got_cols = block.read_partition(0)
        np.testing.assert_array_equal(got_cells, np.concatenate(cell_chunks))
        for name in chunks[0]:
            np.testing.assert_array_equal(got_cols[name], np.concatenate([c[name] for c in chunks]))
        block.close()

    def test_read_then_append_then_read(self, tmp_path):
        # A read must not corrupt the append position (single-block reduce
        # never interleaves, but the file contract should not be fragile).
        block = SpillBlock(tmp_dir=str(tmp_path))
        a, b = _cols(5, seed=1), _cols(7, seed=2)
        block.append(np.zeros(5, np.uint64), np.arange(5, dtype=np.uint64), a)
        block.read_partition(0)
        block.append(np.zeros(7, np.uint64), np.arange(7, dtype=np.uint64), b)
        got_cells, got_cols = block.read_partition(0)
        assert len(got_cells) == 12
        np.testing.assert_array_equal(got_cols["h_ph"], np.concatenate([a["h_ph"], b["h_ph"]]))
        block.close()

    def test_schema_drift_raises(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        cols = _cols(4)
        block.append(np.zeros(4, np.uint64), np.arange(4, dtype=np.uint64), cols)
        bad = {name: arr.astype(np.float64) for name, arr in cols.items()}
        with pytest.raises(ValueError, match="schema drift"):
            block.append(np.zeros(4, np.uint64), np.arange(4, dtype=np.uint64), bad)
        block.close()

    def test_empty_append_is_noop(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        cols = _cols(3)
        block.append(np.zeros(3, np.uint64), np.arange(3, dtype=np.uint64), cols)
        empty = {name: arr[:0] for name, arr in cols.items()}
        assert block.append(np.empty(0, np.uint64), np.empty(0, np.uint64), empty) == 0
        assert block.n_rows(0) == 3
        block.close()


class TestPartitionRouting:
    def test_runs_route_to_their_partitions(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        part_ids = np.array([7, 7, 3, 3, 3, 7, 5], dtype=np.uint64)
        cells = np.arange(7, dtype=np.uint64)
        cols = {"v": np.arange(7, dtype=np.float32)}
        block.append(part_ids, cells, cols)
        assert sorted(block.partition_keys()) == [3, 5, 7]
        got_cells, got_cols = block.read_partition(7)
        # Partition 7 got two runs, read back in append order.
        np.testing.assert_array_equal(got_cells, [0, 1, 5])
        np.testing.assert_array_equal(got_cols["v"], [0.0, 1.0, 5.0])
        got_cells, _ = block.read_partition(3)
        np.testing.assert_array_equal(got_cells, [2, 3, 4])
        got_cells, _ = block.read_partition(5)
        np.testing.assert_array_equal(got_cells, [6])
        block.close()

    def test_partition_ids_match_iter_chunks(self):
        # The routing law: a chunk's partition key equals partition_ids of any
        # of its children — clip2order at chunk_order (issue #217 plan).
        grid = HealpixGrid(2, 5, layout="fullsphere", chunk_inner=3)
        assert grid.chunks_per_shard == 4
        from mortie import geo2mort

        shard = int(geo2mort(-78.5, -132.0, order=2)[0])
        seen = []
        for _, children in grid.iter_chunks(shard):
            ids = partition_ids(grid, np.asarray(children))
            assert len(np.unique(ids)) == 1  # whole chunk -> one partition
            seen.append(int(ids[0]))
        assert len(set(seen)) == grid.chunks_per_shard

    def test_k1_grid_is_single_partition(self):
        grid = HealpixGrid(2, 5, layout="fullsphere")
        ids = partition_ids(grid, np.asarray(grid.children(_shard(grid)), dtype=np.uint64))
        assert (ids == 0).all()

    def test_stub_grid_is_single_partition(self):
        class Stub:
            pass

        ids = partition_ids(Stub(), np.arange(10, dtype=np.uint64))
        assert (ids == 0).all()


def _shard(grid):
    from mortie import geo2mort

    return int(geo2mort(-78.5, -132.0, order=grid.parent_order)[0])


class TestFdHygiene:
    def test_no_files_visible_while_open_or_after_close(self, tmp_path):
        # Unlink-at-birth: /tmp persists across warm Lambda invokes, so the
        # spill files must never be reachable by name — not even mid-write.
        block = SpillBlock(tmp_dir=str(tmp_path))
        cols = _cols(100)
        block.append(
            np.repeat(np.arange(4, dtype=np.uint64), 25),
            np.arange(100, dtype=np.uint64),
            cols,
        )
        assert list(tmp_path.iterdir()) == []
        block.close()
        assert list(tmp_path.iterdir()) == []

    def test_close_closes_every_fd(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        block.append(
            np.repeat(np.arange(3, dtype=np.uint64), 5),
            np.arange(15, dtype=np.uint64),
            {"v": np.arange(15, dtype=np.float32)},
        )
        parts = list(block._partitions.values())
        assert len(parts) == 3
        block.close()
        assert all(p.file.closed for p in parts)
        assert block.partition_keys() == []

    def test_read_with_close_frees_the_partition(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        block.append(
            np.zeros(5, np.uint64),
            np.arange(5, dtype=np.uint64),
            {"v": np.arange(5, dtype=np.float32)},
        )
        (part,) = block._partitions.values()
        block.read_partition(0, close=True)
        assert part.file.closed
        assert block.partition_keys() == []
        block.close()

    def test_default_tmp_dir_is_tempfile_gettempdir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        block = SpillBlock()
        assert block.tmp_dir == str(tmp_path)
        block.append(
            np.zeros(2, np.uint64),
            np.arange(2, dtype=np.uint64),
            {"v": np.arange(2, dtype=np.float32)},
        )
        assert list(tmp_path.iterdir()) == []
        block.close()


class TestByteAccounting:
    def test_bytes_written_is_exact(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        cols = _cols(64)
        cells = np.arange(64, dtype=np.uint64)
        part_ids = np.repeat(np.arange(4, dtype=np.uint64), 16)
        ret = block.append(part_ids, cells, cols)
        expect = cells.nbytes + sum(a.nbytes for a in cols.values())
        assert ret == expect
        assert block.bytes_written == expect
        # A second append accumulates.
        block.append(part_ids, cells, cols)
        assert block.bytes_written == 2 * expect
        block.close()


class TestTmpGuard:
    def test_raises_naming_disk_variant_when_tmp_too_small(self, monkeypatch):
        import os as _os

        real = _os.statvfs(tempfile.gettempdir())

        class _Tiny:
            f_bavail = 1
            f_frsize = real.f_frsize

        monkeypatch.setattr(_os, "statvfs", lambda _p: _Tiny())
        with pytest.raises(RuntimeError, match="-disk"):
            check_tmp_headroom(10**9)

    def test_happy_path_is_silent(self):
        check_tmp_headroom(1)

    def test_checks_the_given_dir(self, monkeypatch):
        import os as _os

        seen = {}
        real = _os.statvfs(tempfile.gettempdir())

        def fake(path):
            seen["path"] = path
            return real

        monkeypatch.setattr(_os, "statvfs", fake)
        check_tmp_headroom(1, tmp_dir="/somewhere")
        assert seen["path"] == "/somewhere"
