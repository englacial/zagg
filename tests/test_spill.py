"""Spill-partition aggregation (issue #217).

Phase 1 — the spill files must (a) round-trip every byte exactly (they
replace the in-memory pool, so any loss is silent data corruption), (b) route
rows to the partition of their enclosing inner chunk, (c) leave nothing in
``/tmp`` — the files are unlinked at birth and every fd is accounted for —
and (d) fail loudly, naming the ``-disk`` variant fix, when ``/tmp`` cannot
hold the spill.

Phase 2 — ``aggregation.streaming.mode: spill`` in the worker: single-block
output must be **byte-identical to the pooled path** across the full pooled
reducer surface (scalar functions, expressions, vectors, tdigest, located
ragged, chunk_precompute — reducers merge-mode streaming could never serve);
multi-block must combine mergeable reducers exactly like merge mode and
refuse non-mergeable configs loudly.

Byte-equality tests pin ``shard_workers: 1``: the granule thread pool drains
the shared fake-read iterator from worker threads, so which granule gets
which frame is scheduling-dependent and buffer/pool composition varies run to
run (a trap first hit in the issue #217 arena A/B tests).
"""

import tempfile

import numpy as np
import pandas as pd
import pytest

from zagg.config import PipelineConfig
from zagg.grids import HealpixGrid
from zagg.processing import process_shard
from zagg.processing.spill import (
    SpillAggregator,
    SpillBlock,
    SpillOverflowError,
    SpillReduceError,
    check_tmp_headroom,
    partition_ids,
)
from zagg.processing.streaming import get_streaming

_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}


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

    def test_row_misalignment_raises_before_writing(self, tmp_path):
        block = SpillBlock(tmp_dir=str(tmp_path))
        block.append(np.zeros(3, np.uint64), np.arange(3, dtype=np.uint64), _cols(3))
        # A column longer than cells must fail loudly, not orphan bytes.
        bad = _cols(5)
        with pytest.raises(ValueError, match="row-aligned"):
            block.append(np.zeros(3, np.uint64), np.arange(3, dtype=np.uint64), bad)
        # part_ids length disagreeing is caught the same way.
        with pytest.raises(ValueError, match="row-aligned"):
            block.append(np.zeros(2, np.uint64), np.arange(3, dtype=np.uint64), _cols(3))
        # Stream is intact: the first append round-trips unchanged.
        got_cells, _ = block.read_partition(0)
        np.testing.assert_array_equal(got_cells, [0, 1, 2])
        assert block.n_rows(0) == 3
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

    def test_partition_groups_capped_at_64_contiguous(self):
        # Production geometry (chunk_inner: 13) puts 1,024 inner chunks in an
        # o8 shard; one file per inner chunk would blow Lambda's ~1,024 nofile
        # default. Routing caps at 4**_GROUP_LEVELS = 64 contiguous groups.
        from zagg.processing.spill import _GROUP_LEVELS, _group_order

        grid = HealpixGrid(2, 8, layout="fullsphere", chunk_inner=7)
        assert grid.chunks_per_shard == 1024
        assert _group_order(grid) == grid.parent_order + _GROUP_LEVELS
        shard = _shard(grid)
        children = np.sort(np.asarray(grid.children(shard), dtype=np.uint64))
        ids = partition_ids(grid, children)
        assert len(np.unique(ids)) == 4**_GROUP_LEVELS
        # Contiguous grouping: over morton-sorted cells the group id never
        # revisits an earlier value, so each group is one contiguous cell span
        # (and iter_chunks reads each group's file back exactly once).
        changes = np.flatnonzero(np.diff(ids)) + 1
        assert len(changes) == 4**_GROUP_LEVELS - 1
        # Every inner chunk maps into exactly one group.
        for _, chunk_children in grid.iter_chunks(shard):
            cids = partition_ids(grid, np.asarray(chunk_children))
            assert len(np.unique(cids)) == 1
        # And the writer opens at most 64 files for a full-shard append.
        block = SpillBlock()
        block.append(ids, children, {"v": np.zeros(len(children), dtype=np.float32)})
        assert len(block.partition_keys()) == 4**_GROUP_LEVELS
        block.close()

    def test_group_order_uncapped_below_64(self):
        from zagg.processing.spill import _group_order

        grid = HealpixGrid(2, 5, layout="fullsphere", chunk_inner=3)
        assert _group_order(grid) == grid.chunk_order  # 4 chunks: no grouping

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


def _base_variables(delta=256):
    return {
        "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
        "h_tdigest": {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "source": "h_ph",
            "inner_shape": [2],
            "params": {"delta": delta},
            "dtype": "float32",
            "fill_value": 0,
        },
    }


def _pairwise_variables(delta=256):
    """Like :func:`_base_variables` but the pairwise-fold reducer (issue #279)."""
    variables = _base_variables(delta=delta)
    variables["h_tdigest"]["function"] = "zagg.stats.tdigest.build_tdigest_pairwise"
    return variables


def _config(streaming=None, variables=None, chunk_precompute=None):
    agg = {"variables": variables or _base_variables()}
    if streaming is not None:
        agg["streaming"] = streaming
    if chunk_precompute is not None:
        agg["chunk_precompute"] = chunk_precompute
    return PipelineConfig(
        # Fake reads monkeypatch ``zagg.processing._read_group`` — pin the
        # hierarchical backend so the seam is actually exercised, and pin
        # shard_workers=1 for deterministic buffer composition (see module
        # docstring).
        data_source={
            "reader": "h5coro",
            "driver": "s3",
            "groups": ["gt1l"],
            "index": {"backend": "hierarchical"},
            "shard_workers": 1,
        },
        aggregation=agg,
    )


def _matrix_variables():
    """Every pooled reducer kind: the surface merge-mode streaming rejects."""
    variables = _base_variables()
    variables.update(
        {
            "h_mean": {"function": "nanmean", "source": "h_ph"},
            "h_spread": {"expression": "np.nanmax(h_ph) - np.nanmin(h_ph)"},
            "h_anchor": {"expression": "anchor"},
            "h_hist": {
                "expression": "np.histogram(h_ph, bins=4, range=(-40.0, 40.0))[0]",
                "kind": "vector",
                "trailing_shape": [4],
                "dtype": "float32",
            },
            "h_tdigest_loc": {
                "kind": "ragged",
                "function": "zagg.stats.tdigest.build_tdigest",
                "source": "h_ph",
                "inner_shape": [2],
                "params": {"delta": 128},
                "dtype": "float32",
                "fill_value": 0,
                "location": "leaf_id",
            },
        }
    )
    return variables


_ANCHOR = {"anchor": {"function": "min", "source": "h_ph", "dtype": "float32"}}


def _grid(cfg, parent=6, child=8, **kw):
    return HealpixGrid(parent, child, layout="fullsphere", config=cfg, **kw)


def _shard_key(order=6):
    from mortie import geo2mort

    return int(geo2mort(-78.5, -132.0, order=order)[0])


def _granule_dfs(grid, shard_key, cell_idx_lists, obs_per_cell=50, seed=0, nan_cells=()):
    """One DataFrame per granule over caller-chosen child-cell indices."""
    rng = np.random.default_rng(seed)
    children = grid.children(shard_key)
    dfs = []
    for idxs in cell_idx_lists:
        leaf, h = [], []
        for ci in idxs:
            leaf.extend([int(children[ci])] * obs_per_cell)
            vals = rng.normal(0.0, 10.0, obs_per_cell)
            h.extend([np.nan] * obs_per_cell if ci in nan_cells else vals)
        dfs.append(
            pd.DataFrame(
                {
                    "h_ph": np.array(h, dtype=np.float32),
                    "leaf_id": np.array(leaf, dtype=np.uint64),
                }
            )
        )
    return dfs


def _run(monkeypatch, cfg, grid, shard_key, dfs, **kwargs):
    """process_shard over len(dfs) fake granules; returns (df_out, ragged, meta)."""
    reads = iter(dfs)
    monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: next(reads))
    monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
    monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
    ragged: dict = {}
    if "chunk_results" not in kwargs:
        kwargs["ragged_out"] = ragged
    df_out, meta = process_shard(
        grid,
        shard_key,
        [f"s3://b/g{i}.h5" for i in range(len(dfs))],
        s3_credentials=_CREDS,
        config=cfg,
        **kwargs,
    )
    return df_out, ragged, meta


_CELL_LISTS = [[0, 4, 8], [2, 4, 10], [1, 8, 9], [0, 10, 15], [4, 8, 10]]


def _assert_carrier_identical(a, b):
    """Byte-equality for either carrier (DataFrame, or arro3 Table when the
    config carries vector fields — ``_has_vector_fields`` flips the handoff)."""
    if isinstance(a, pd.DataFrame) and isinstance(b, pd.DataFrame):
        pd.testing.assert_frame_equal(a, b)
        return
    from zagg.processing.write import _iter_carrier_columns

    cols_a, cols_b = dict(_iter_carrier_columns(a)), dict(_iter_carrier_columns(b))
    assert list(cols_a) == list(cols_b)
    for name in cols_a:
        np.testing.assert_array_equal(cols_a[name], cols_b[name])
        assert cols_a[name].dtype == cols_b[name].dtype


def _assert_ragged_identical(ragged_p, ragged_s):
    assert set(ragged_p) == set(ragged_s)
    for name in ragged_p:
        pay_p, idx_p, *locs_p = ragged_p[name]
        pay_s, idx_s, *locs_s = ragged_s[name]
        assert idx_p == idx_s
        for a, b in zip(pay_p, pay_s, strict=True):
            np.testing.assert_array_equal(a, b)
        assert len(locs_p) == len(locs_s)
        if locs_p:
            for a, b in zip(locs_p[0], locs_s[0], strict=True):
                np.testing.assert_array_equal(a, b)


class TestSpillConfig:
    def test_mode_defaults_to_merge(self):
        assert get_streaming(_config(streaming={}))["mode"] == "merge"

    def test_spill_mode_accepted(self):
        assert get_streaming(_config(streaming={"mode": "spill"}))["mode"] == "spill"

    @pytest.mark.parametrize("bad", ["disk", 1, None])
    def test_bad_mode_raises(self, bad):
        with pytest.raises(ValueError, match="mode"):
            get_streaming(_config(streaming={"mode": bad}))

    def test_removed_arena_knobs_rejected_with_spill(self):
        # The #260 knobs are gone (phase 6); a stale arena config must fail
        # loudly as an unknown key, never silently run.
        with pytest.raises(ValueError, match="unknown key"):
            get_streaming(_config(streaming={"mode": "spill", "state_layout": "arena"}))

    def test_non_mergeable_config_is_accepted_by_spill(self):
        # The whole point of spill: reducers merge-mode validation rejects are
        # exact in the single-block regime.
        cfg = _config(
            streaming={"mode": "spill"},
            variables=_matrix_variables(),
            chunk_precompute=_ANCHOR,
        )
        agg = SpillAggregator(cfg, _grid(cfg), "pandas", 25)
        assert not agg._mergeable
        agg.close()

    def test_guard_fires_at_construction(self, monkeypatch):
        import os as _os

        real = _os.statvfs(tempfile.gettempdir())

        class _Tiny:
            f_bavail = 1
            f_frsize = real.f_frsize

        cfg = _config(streaming={"mode": "spill"})
        grid = _grid(cfg)
        monkeypatch.setattr(_os, "statvfs", lambda _p: _Tiny())
        with pytest.raises(RuntimeError, match="-disk"):
            SpillAggregator(cfg, grid, "pandas", 25)


class TestBlockThreshold:
    """The derived threshold must reserve the measured ~3x build peak."""

    def test_derivation_includes_build_multiplier(self, monkeypatch):
        from zagg.processing import spill

        monkeypatch.setattr(spill, "_memory_budget_bytes", lambda: 4096 * 2**20)

        class _Big:  # ~39 GB free: the /tmp cap must not bind
            f_bavail = 10**7
            f_frsize = 4096

        monkeypatch.setattr(spill.os, "statvfs", lambda _p: _Big())
        one = spill._default_block_bytes(1)
        # Pin the measured multiplier: phase-3 replays peaked at ~2.73-3.50x
        # the partition bytes (columns + sort copies + outputs), so the
        # reservation divides by 3. Do not lower without re-measuring the replay.
        assert spill._BUILD_MULT == 3
        # Compare against the literal (not spill._BUILD_MULT), so an unreviewed
        # change to the constant fails here instead of moving the expectation
        # with it — without the /3 a K=1 block builds at ~3x its bytes and OOMs.
        assert one == int(0.8 * 0.75 * 4096 * 2**20 / 3)
        # K divides the build unit (one partition), so it multiplies the block.
        assert spill._default_block_bytes(4) == 4 * one

    def test_tmp_cap_binds_when_small(self, monkeypatch):
        from zagg.processing import spill

        monkeypatch.setattr(spill, "_memory_budget_bytes", lambda: 4096 * 2**20)

        class _Small:
            f_bavail = 1000
            f_frsize = 4096

        monkeypatch.setattr(spill.os, "statvfs", lambda _p: _Small())
        assert spill._default_block_bytes(1) == int(0.45 * 1000 * 4096)


class TestSpillWorkerSingleBlock:
    """Single-block spill == pooled, byte for byte, on every reducer."""

    def _ab(self, monkeypatch, variables, chunk_precompute=None, nan_cells=(), buffer=2):
        key = _shard_key()
        results = []
        for streaming in (None, {"buffer_granules": buffer, "mode": "spill"}):
            cfg = _config(
                streaming=streaming, variables=variables, chunk_precompute=chunk_precompute
            )
            grid = _grid(cfg)
            dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=7, nan_cells=nan_cells)
            results.append(_run(monkeypatch, cfg, grid, key, dfs))
        return results

    def test_full_reducer_matrix_byte_identical_to_pooled(self, monkeypatch):
        (df_p, ragged_p, meta_p), (df_s, ragged_s, meta_s) = self._ab(
            monkeypatch, _matrix_variables(), chunk_precompute=_ANCHOR
        )
        _assert_carrier_identical(df_p, df_s)
        assert meta_p["total_obs"] == meta_s["total_obs"]
        assert meta_p["cells_with_data"] == meta_s["cells_with_data"]
        _assert_ragged_identical(ragged_p, ragged_s)

    def test_nan_cells_byte_identical_to_pooled(self, monkeypatch):
        # An all-NaN cell: zero-length digest but a real count, NaN mean.
        (df_p, ragged_p, _), (df_s, ragged_s, _) = self._ab(
            monkeypatch, _matrix_variables(), chunk_precompute=_ANCHOR, nan_cells={4}
        )
        _assert_carrier_identical(df_p, df_s)
        _assert_ragged_identical(ragged_p, ragged_s)

    def test_tdigest_multi_flush_byte_identical_to_pooled(self, monkeypatch):
        # Merge mode is only exact when one flush == one pooled build; spill
        # must be exact at ANY flush count (no merges exist to approximate).
        (df_p, ragged_p, _), (df_s, ragged_s, _) = self._ab(
            monkeypatch, _base_variables(), buffer=1
        )
        pd.testing.assert_frame_equal(df_p, df_s)
        _assert_ragged_identical(ragged_p, ragged_s)

    def test_k_gt_1_chunks_byte_identical_to_pooled(self, monkeypatch):
        # K=4 partitions: each chunk's outputs come from its own partition.
        key = _shard_key(order=2)
        results = []
        for streaming in (None, {"buffer_granules": 2, "mode": "spill"}):
            cfg = _config(streaming=streaming)
            grid = _grid(cfg, parent=2, child=5, chunk_inner=3)
            assert grid.chunks_per_shard == 4
            dfs = _granule_dfs(grid, key, [[0, 20, 40], [5, 20, 60], [0, 40, 63]], seed=3)
            sink: list = []
            _run(monkeypatch, cfg, grid, key, dfs, chunk_results=sink)
            results.append(sink)
        pooled, spilled = results
        assert len(pooled) == len(spilled) == 4
        for (blk_p, car_p, rag_p), (blk_s, car_s, rag_s) in zip(pooled, spilled, strict=True):
            assert blk_p == blk_s
            pd.testing.assert_frame_equal(car_p, car_s)
            _assert_ragged_identical(rag_p, rag_s)

    def test_capped_groups_byte_identical_to_pooled(self, monkeypatch):
        # Group cap binding (chunks_per_shard > 64): one partition file holds
        # several chunks' rows; each chunk's outputs still gather only its own
        # cells from the cached group, byte-identical to pooled.
        key = _shard_key(order=2)
        results = []
        for streaming in (None, {"buffer_granules": 2, "mode": "spill"}):
            cfg = _config(streaming=streaming)
            grid = _grid(cfg, parent=2, child=6, chunk_inner=6)
            assert grid.chunks_per_shard == 256  # -> 64 groups of 4 chunks
            dfs = _granule_dfs(grid, key, [[0, 40, 200], [5, 40, 255], [0, 200, 254]], seed=3)
            sink: list = []
            _run(monkeypatch, cfg, grid, key, dfs, chunk_results=sink)
            results.append(sink)
        pooled, spilled = results
        assert len(pooled) == len(spilled) == 256
        for (blk_p, car_p, rag_p), (blk_s, car_s, rag_s) in zip(pooled, spilled, strict=True):
            assert blk_p == blk_s
            pd.testing.assert_frame_equal(car_p, car_s)
            _assert_ragged_identical(rag_p, rag_s)

    def test_consumed_group_re_request_trips(self):
        # The single-block exact reduce reads each partition group back exactly
        # once (read_partition close=True deletes it), relying on iter_chunks
        # visiting a group's chunks contiguously. If a consumed group were ever
        # re-requested, _load_partition must raise — not fall into the
        # empty-columns branch and ship silent zeros for a populated group.
        cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid = _grid(cfg, parent=2, child=5, chunk_inner=3)
        assert grid.chunks_per_shard == 4
        agg = SpillAggregator(cfg, grid, "pandas", 2)
        children = np.asarray(grid.children(_shard_key(order=2)), dtype=np.uint64)
        parts = partition_ids(grid, children)
        groups = np.unique(parts)
        assert len(groups) >= 3

        def _append(cell):
            cell = np.asarray([int(cell)], dtype=np.uint64)
            agg._block.append(
                partition_ids(grid, cell),
                cell,
                {"h_ph": np.zeros(1, np.float32), "leaf_id": cell.copy()},
            )

        key_a, key_b = int(groups[0]), int(groups[1])
        _append(children[parts == groups[0]][0])
        _append(children[parts == groups[1]][0])

        agg._load_partition(key_a)  # consume group A
        assert key_a in agg._consumed
        agg._load_partition(key_b)  # single-slot cache evicts A, consumes B
        with pytest.raises(RuntimeError, match="re-requested after it was read-and-closed"):
            agg._load_partition(key_a)  # revisiting A trips the invariant

        # A genuinely-unpopulated group (never read, never consumed) is still a
        # legitimate empty load, indistinguishable code paths kept apart.
        untouched = int(groups[2])
        assert untouched not in agg._consumed
        agg._load_partition(untouched)
        assert agg._loaded[0] == untouched
        assert agg._loaded[2] == {}

    def test_occupied_out_matches_pooled(self, monkeypatch):
        key = _shard_key()
        occ = {}
        for label, streaming in (
            ("pooled", None),
            ("spill", {"buffer_granules": 1, "mode": "spill"}),
        ):
            cfg = _config(streaming=streaming)
            grid = _grid(cfg)
            dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=5)
            sink: list = []
            _run(monkeypatch, cfg, grid, key, dfs, occupied_out=sink)
            occ[label] = np.concatenate(sink) if sink else np.empty(0, dtype=np.uint64)
        assert occ["spill"].size > 0
        np.testing.assert_array_equal(np.sort(occ["spill"]), np.sort(occ["pooled"]))

    def test_empty_shard_matches_pooled_no_data(self, monkeypatch):
        cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid = _grid(cfg)
        monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: None)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        df_out, meta = process_shard(
            grid, _shard_key(), ["s3://b/g0.h5"], s3_credentials=_CREDS, config=cfg
        )
        assert df_out.empty
        assert meta["error"] == "No data after filtering"

    def test_no_tmp_files_left_behind(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, _shard_key(), _CELL_LISTS, seed=1)
        _, _, meta = _run(monkeypatch, cfg, grid, _shard_key(), dfs)
        assert meta["total_obs"] > 0
        assert list(tmp_path.iterdir()) == []

    def test_profile_carries_spill_instrumentation(self, monkeypatch):
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=1)
        _, _, meta = _run(monkeypatch, cfg, grid, key, dfs, profile=True)
        timings = meta["phase_timings"]
        assert set(timings) == {
            "read",
            "index",
            "aggregate",
            "spill_write_s",
            "spill_read_s",
            "spill_reduce_s",
            "spill_bytes",
        }
        assert timings["spill_bytes"] > 0
        assert timings["spill_write_s"] >= 0
        assert timings["spill_read_s"] >= 0
        assert timings["spill_reduce_s"] >= 0


class TestSpillWorkerMultiBlock:
    """Forced multi-block via an injected tiny threshold."""

    def _force_tiny_blocks(self, monkeypatch):
        # Every flush crosses the threshold -> one closed block per flush.
        monkeypatch.setattr("zagg.processing.spill._default_block_bytes", lambda k, tmp_dir=None: 1)

    def test_pairwise_multi_block_byte_identical_to_merge_mode(self, monkeypatch):
        # The pairwise reducer folds identically per block on both paths, so
        # spill multi-block must match merge mode byte-for-byte — even when the
        # fold actually compresses (δ=16 with ~150 obs on the busiest cells, so
        # this is not the loss-free regime where every law coincides). See #279.
        self._force_tiny_blocks(monkeypatch)
        key = _shard_key()
        results = []
        for streaming in (
            {"buffer_granules": 2, "mode": "merge"},
            {"buffer_granules": 2, "mode": "spill"},
        ):
            cfg = _config(streaming=streaming, variables=_pairwise_variables(delta=16))
            grid = _grid(cfg)
            dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=9)
            results.append(_run(monkeypatch, cfg, grid, key, dfs))
        (df_m, ragged_m, meta_m), (df_s, ragged_s, meta_s) = results
        pd.testing.assert_frame_equal(df_m, df_s)
        assert meta_m["total_obs"] == meta_s["total_obs"]
        assert meta_m["cells_with_data"] == meta_s["cells_with_data"]
        vals_m, idx_m = ragged_m["h_tdigest"]
        vals_s, idx_s = ragged_s["h_tdigest"]
        assert idx_m == idx_s
        # At least one cell spans 3 blocks and compresses, so this is a real test.
        assert any(float(a[:, 1].sum()) > 16 * 1.27 for a in vals_s)
        for a, b in zip(vals_m, vals_s, strict=True):
            np.testing.assert_array_equal(a, b)

    def test_standard_kway_diverges_from_pairwise_multi_block(self, monkeypatch):
        # Intended #279 behavior: the standard (k-way) reducer folds a cell's
        # 3+ blocks in one pass, the pairwise reducer folds them left-to-right,
        # so the ragged bytes differ on the busiest cells — while counts (exact
        # by summation) stay identical between the two.
        self._force_tiny_blocks(monkeypatch)
        key = _shard_key()
        out = {}
        for tag, variables in (("std", _base_variables(delta=16)), ("pw", _pairwise_variables(16))):
            cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"}, variables=variables)
            grid = _grid(cfg)
            dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=9)
            out[tag] = _run(monkeypatch, cfg, grid, key, dfs)
        (df_std, ragged_std, _), (df_pw, ragged_pw, _) = out["std"], out["pw"]
        pd.testing.assert_series_equal(df_std["count"], df_pw["count"])  # counts unchanged
        vals_std, idx_std = ragged_std["h_tdigest"]
        vals_pw, idx_pw = ragged_pw["h_tdigest"]
        assert idx_std == idx_pw
        assert any(not np.array_equal(a, b) for a, b in zip(vals_std, vals_pw, strict=True)), (
            "k-way and pairwise should diverge on the multi-block cells"
        )

    def test_standard_kway_multi_block_tracks_pooled_quantiles(self, monkeypatch):
        # Correctness: the standard k-way fold over many blocks estimates the
        # same quantiles (within t-digest tolerance) as a one-shot pooled build.
        from zagg.stats.tdigest import quantile_from_tdigest

        self._force_tiny_blocks(monkeypatch)
        key = _shard_key()
        pooled_cfg = _config()  # no streaming -> one-shot pooled build per cell
        spill_cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid_p, grid_s = _grid(pooled_cfg), _grid(spill_cfg)
        dfs = _granule_dfs(grid_p, key, _CELL_LISTS, obs_per_cell=200, seed=4)
        _, ragged_p, _ = _run(monkeypatch, pooled_cfg, grid_p, key, list(dfs))
        _, ragged_s, _ = _run(monkeypatch, spill_cfg, grid_s, key, list(dfs))
        vals_p, idx_p = ragged_p["h_tdigest"]
        vals_s, idx_s = ragged_s["h_tdigest"]
        assert idx_p == idx_s
        by_cell_p = dict(zip(idx_p, vals_p, strict=True))
        for cell_i, ds in zip(idx_s, vals_s, strict=True):
            dp = by_cell_p[cell_i]
            for q in (0.1, 0.5, 0.9):
                assert abs(quantile_from_tdigest(ds, q) - quantile_from_tdigest(dp, q)) < 1.0

    def test_multi_block_reduce_time_is_captured(self, monkeypatch):
        # The block fold runs only in the multi-block regime, so spill_reduce_s
        # (the #280 reduce-CPU-vs-read-I/O split) is populated here, not on the
        # single-block exact path.
        self._force_tiny_blocks(monkeypatch)
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2, "mode": "spill"})
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS, obs_per_cell=200, seed=3)
        _, _, meta = _run(monkeypatch, cfg, grid, key, dfs, profile=True)
        assert meta["phase_timings"]["spill_reduce_s"] > 0

    def test_multi_block_actually_engaged_and_counts_exact(self, monkeypatch):
        self._force_tiny_blocks(monkeypatch)
        closes = {"n": 0}
        orig = SpillAggregator._close_block

        def counting(self):
            closes["n"] += 1
            orig(self)

        monkeypatch.setattr(SpillAggregator, "_close_block", counting)
        key = _shard_key()
        pooled_cfg = _config()
        spill_cfg = _config(streaming={"buffer_granules": 1, "mode": "spill"})
        grid_p, grid_s = _grid(pooled_cfg), _grid(spill_cfg)
        dfs = _granule_dfs(grid_p, key, _CELL_LISTS, seed=2)
        df_p, _, _ = _run(monkeypatch, pooled_cfg, grid_p, key, list(dfs))
        df_s, _, _ = _run(monkeypatch, spill_cfg, grid_s, key, list(dfs))
        assert closes["n"] >= len(_CELL_LISTS)  # every flush closed a block
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])

    def test_non_mergeable_overflow_raises_loudly(self, monkeypatch):
        # The worker's tolerated per-granule except must NOT swallow this.
        self._force_tiny_blocks(monkeypatch)
        cfg = _config(
            streaming={"buffer_granules": 1, "mode": "spill"},
            variables=_matrix_variables(),
            chunk_precompute=_ANCHOR,
        )
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, _shard_key(), _CELL_LISTS, seed=2)
        with pytest.raises(SpillOverflowError, match="memory tier"):
            _run(monkeypatch, cfg, grid, _shard_key(), dfs)

    def test_overlap_reduce_error_raises_through_worker(self, monkeypatch):
        # A failed overlap-thread reduce surfaces at the next block close, on
        # the granule_done -> flush -> _close_block path inside the worker's
        # read loop. Its tolerated per-granule except must re-raise
        # SpillReduceError, not warn-and-continue and emit merged state that
        # silently drops the failed block (the finding TestSpillOverlap misses
        # by driving SpillAggregator directly).
        self._force_tiny_blocks(monkeypatch)
        cfg = _config(streaming={"buffer_granules": 1, "mode": "spill"})
        grid = _grid(cfg)
        key = _shard_key()
        dfs = _granule_dfs(grid, key, _CELL_LISTS, seed=3)

        def boom(self, block):
            raise ValueError("synthetic reduce failure")

        monkeypatch.setattr(SpillAggregator, "_fold_block", boom)
        with pytest.raises(SpillReduceError, match="overlap thread"):
            _run(monkeypatch, cfg, grid, key, dfs)


class TestSpillOverlap:
    """Async read/reduce overlap (phase 5): one reducer thread, same bytes."""

    def _drive(self, cfg, dfs, **kw):
        grid = _grid(cfg)
        agg = SpillAggregator(cfg, grid, "pandas", 1, block_bytes=1, **kw)
        for df in dfs:
            agg.add_read(df)
            agg.granule_done()
        agg.flush()
        return agg, grid

    def test_overlap_identical_to_sequential(self, monkeypatch):
        from zagg.config import get_agg_fields

        key = _shard_key()
        outs = []
        for overlap in (False, True):
            cfg = _config(streaming={"buffer_granules": 1, "mode": "spill"})
            dfs = _granule_dfs(_grid(cfg), key, _CELL_LISTS, seed=13)
            agg, grid = self._drive(cfg, dfs, overlap=overlap)
            assert agg._closed_blocks == len(_CELL_LISTS)
            out = agg.chunk_outputs(grid.children(key), get_agg_fields(cfg))
            assert agg._reducer is None  # joined before emission
            outs.append(out)
            agg.close()
        (stats_a, pay_a, idx_a, locs_a, cwd_a), (stats_b, pay_b, idx_b, locs_b, cwd_b) = outs
        assert cwd_a == cwd_b
        assert locs_a == locs_b == {}
        for name in stats_a:
            np.testing.assert_array_equal(stats_a[name], stats_b[name])
        for name in pay_a:
            assert idx_a[name] == idx_b[name]
            for x, y in zip(pay_a[name], pay_b[name], strict=True):
                np.testing.assert_array_equal(x, y)

    def test_reduce_error_propagates_at_next_join(self, monkeypatch):
        cfg = _config(streaming={"buffer_granules": 1, "mode": "spill"})
        key = _shard_key()
        dfs = _granule_dfs(_grid(cfg), key, _CELL_LISTS, seed=1)

        def boom(self, block):
            raise ValueError("synthetic reduce failure")

        monkeypatch.setattr(SpillAggregator, "_fold_block", boom)
        with pytest.raises(SpillReduceError, match="overlap thread"):
            self._drive(cfg, dfs)

    def test_reduce_error_propagates_at_finalize(self, monkeypatch):
        from zagg.config import get_agg_fields

        cfg = _config(streaming={"buffer_granules": 1, "mode": "spill"})
        key = _shard_key()
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, _CELL_LISTS[:1], seed=1)
        agg = SpillAggregator(cfg, grid, "pandas", 1, block_bytes=1)
        calls = {"n": 0}
        orig = SpillAggregator._fold_block

        def flaky(self, block):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("synthetic reduce failure")
            orig(self, block)

        monkeypatch.setattr(SpillAggregator, "_fold_block", flaky)
        agg.add_read(dfs[0])
        agg.granule_done()  # one flush -> one closed block, failing on the thread
        agg.flush()
        with pytest.raises(SpillReduceError, match="overlap thread"):
            agg.chunk_outputs(grid.children(key), get_agg_fields(cfg))
        agg.close()


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
