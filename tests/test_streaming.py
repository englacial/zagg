"""Streaming buffered tdigest merge (issue #148, phase 4).

The buffered path must (a) refuse configs whose reducers have no merge law,
(b) reproduce the pooled path *byte-identically* when the shard fits in one
buffer (one flush == one pooled build), and (c) stay within t-digest accuracy
of the pooled quantiles when it actually merges across flushes — while the
counts stay exact in every case.
"""

import numpy as np
import pandas as pd
import pytest

from zagg.config import PipelineConfig, load_config
from zagg.grids import HealpixGrid
from zagg.processing import process_shard
from zagg.processing.streaming import (
    StreamingAggregator,
    get_streaming,
    validate_streaming,
)
from zagg.stats.tdigest import quantile_from_tdigest

BENCH_CONFIG = "tests/data/benchmark/configs/atl03_tdigest_healpix_o10.yaml"

_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}


def _config(streaming=None, delta=128):
    agg = {
        "variables": {
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
    }
    if streaming is not None:
        agg["streaming"] = streaming
    return PipelineConfig(
        # The worker-integration tests below fake reads by monkeypatching
        # ``zagg.processing._read_group`` — the hierarchical backend's seam.
        # Pin it: the inline default (issue #170) reads through the compiled
        # path and never calls ``_read_group``, so the fakes would be bypassed.
        data_source={
            "reader": "h5coro",
            "driver": "s3",
            "groups": ["gt1l"],
            "index": {"backend": "hierarchical"},
        },
        aggregation=agg,
    )


# --- config surface ---------------------------------------------------------


class TestStreamingConfig:
    def test_absent_block_is_none(self):
        assert get_streaming(_config()) is None

    def test_block_defaults_buffer(self):
        assert get_streaming(_config(streaming={})) == {"buffer_granules": 50, "mode": "merge"}

    def test_explicit_buffer(self):
        assert get_streaming(_config(streaming={"buffer_granules": 7})) == {
            "buffer_granules": 7,
            "mode": "merge",
        }

    @pytest.mark.parametrize(
        "key", ["state_layout", "arena_backing", "block_bytes", "buffer_granuels"]
    )
    def test_unknown_keys_rejected(self, key):
        # The removed #260 arena knobs (and any typo) must fail loudly, not
        # silently run the dict path (#239 discipline).
        with pytest.raises(ValueError, match="unknown key"):
            get_streaming(_config(streaming={key: "arena"}))

    @pytest.mark.parametrize("bad", [0, -1, "50", 2.5])
    def test_bad_buffer_raises(self, bad):
        with pytest.raises(ValueError, match="buffer_granules"):
            get_streaming(_config(streaming={"buffer_granules": bad}))

    def test_non_mapping_block_raises(self):
        with pytest.raises(ValueError, match="mapping"):
            get_streaming(_config(streaming=[50]))

    def test_benchmark_tdigest_config_is_streamable(self):
        # The real 88S/NEON tdigest benchmark config must validate as-is.
        validate_streaming(load_config(BENCH_CONFIG))

    def test_expression_field_rejected(self):
        cfg = _config()
        cfg.aggregation["variables"]["h_med"] = {"expression": "np.median(h_ph)"}
        with pytest.raises(ValueError, match="expression"):
            validate_streaming(cfg)

    def test_non_len_scalar_rejected(self):
        cfg = _config()
        cfg.aggregation["variables"]["h_mean"] = {"function": "mean", "source": "h_ph"}
        with pytest.raises(ValueError, match="not.*mergeable|mergeable"):
            validate_streaming(cfg)

    def test_non_tdigest_ragged_rejected(self):
        cfg = _config()
        cfg.aggregation["variables"]["h_raw"] = {
            "function": "np.sort",
            "source": "h_ph",
            "kind": "ragged",
            "inner_shape": [1],
        }
        with pytest.raises(ValueError, match="merge law"):
            validate_streaming(cfg)

    def test_pairwise_tdigest_reducer_is_streamable(self):
        # build_tdigest_pairwise carries the pairwise merge law (issue #279),
        # so it must validate as a mergeable ragged reducer just like the
        # standard build_tdigest.
        cfg = _config()
        cfg.aggregation["variables"]["h_tdigest"]["function"] = (
            "zagg.stats.tdigest.build_tdigest_pairwise"
        )
        validate_streaming(cfg)

    def test_located_ragged_rejected(self):
        # The located channel (issue #87) is not threaded through the
        # streaming state yet; reject rather than silently drop it.
        cfg = _config()
        cfg.aggregation["variables"]["h_tdigest"]["location"] = "leaf_id"
        with pytest.raises(ValueError, match="located ragged fields .* cannot stream"):
            validate_streaming(cfg)

    def test_chunk_precompute_rejected(self):
        cfg = _config()
        cfg.aggregation["chunk_precompute"] = {"anchor": {"function": "mean", "source": "h_ph"}}
        with pytest.raises(ValueError, match="chunk_precompute"):
            validate_streaming(cfg)


# --- worker integration ------------------------------------------------------


def _grid(cfg):
    return HealpixGrid(6, 8, layout="fullsphere", config=cfg)


def _granule_dfs(grid, shard_key, n_granules, obs_per_cell=40, seed=0):
    """One DataFrame per granule: rows over a few cells of the shard."""
    rng = np.random.default_rng(seed)
    children = grid.children(shard_key)
    cells = [int(children[0]), int(children[1]), int(children[-1])]
    dfs = []
    for _ in range(n_granules):
        leaf, h = [], []
        for c in cells:
            leaf.extend([c] * obs_per_cell)
            h.extend(rng.normal(0.0, 10.0, obs_per_cell))
        dfs.append(
            pd.DataFrame(
                {
                    "h_ph": np.array(h, dtype=np.float32),
                    "leaf_id": np.array(leaf, dtype=np.uint64),
                }
            )
        )
    return dfs


def _run(monkeypatch, cfg, grid, shard_key, dfs):
    """process_shard over len(dfs) fake granules; returns (df_out, ragged, meta)."""
    reads = iter(dfs)

    def per_granule(*args, **kwargs):
        return next(reads)

    monkeypatch.setattr("zagg.processing._read_group", per_granule)
    monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
    monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
    ragged: dict = {}
    df_out, meta = process_shard(
        grid,
        shard_key,
        [f"s3://b/g{i}.h5" for i in range(len(dfs))],
        s3_credentials=_CREDS,
        config=cfg,
        ragged_out=ragged,
    )
    return df_out, ragged, meta


def _shard_key():
    from mortie import geo2mort

    return int(geo2mort(-78.5, -132.0, order=6)[0])


class TestStreamingWorker:
    def test_single_buffer_is_byte_identical_to_pooled(self, monkeypatch):
        # buffer_granules >= n_granules -> exactly one flush over the same
        # concatenated reads the pooled path groups: identical bytes out.
        key = _shard_key()
        pooled_cfg = _config()
        stream_cfg = _config(streaming={"buffer_granules": 10})
        dfs = _granule_dfs(_grid(pooled_cfg), key, n_granules=4)

        df_p, ragged_p, meta_p = _run(monkeypatch, pooled_cfg, _grid(pooled_cfg), key, list(dfs))
        df_s, ragged_s, meta_s = _run(monkeypatch, stream_cfg, _grid(stream_cfg), key, list(dfs))

        pd.testing.assert_frame_equal(df_p, df_s)
        assert meta_p["total_obs"] == meta_s["total_obs"]
        assert meta_p["cells_with_data"] == meta_s["cells_with_data"]
        vals_p, idx_p = ragged_p["h_tdigest"]
        vals_s, idx_s = ragged_s["h_tdigest"]
        assert len(vals_p) == 3  # the three synthetic cells actually produced digests
        assert idx_p == idx_s
        for a, b in zip(vals_p, vals_s, strict=True):
            np.testing.assert_array_equal(a, b)

    def test_multi_flush_counts_exact_quantiles_close(self, monkeypatch):
        key = _shard_key()
        pooled_cfg = _config(delta=256)
        stream_cfg = _config(streaming={"buffer_granules": 2}, delta=256)
        dfs = _granule_dfs(_grid(pooled_cfg), key, n_granules=7, obs_per_cell=200, seed=3)

        df_p, ragged_p, meta_p = _run(monkeypatch, pooled_cfg, _grid(pooled_cfg), key, list(dfs))
        df_s, ragged_s, meta_s = _run(monkeypatch, stream_cfg, _grid(stream_cfg), key, list(dfs))

        # Counts merge by summation: exact.
        pd.testing.assert_series_equal(df_p["count"], df_s["count"])
        assert meta_p["total_obs"] == meta_s["total_obs"]
        # Digests merged across 4 flushes: quantiles within t-digest tolerance.
        vals_p, idx_p = ragged_p["h_tdigest"]
        vals_s, idx_s = ragged_s["h_tdigest"]
        assert idx_p == idx_s
        spread = 10.0  # sigma of the synthetic heights
        for dp, ds in zip(vals_p, vals_s, strict=True):
            for q in (0.05, 0.25, 0.5, 0.75, 0.95):
                assert quantile_from_tdigest(ds, q) == pytest.approx(
                    quantile_from_tdigest(dp, q), abs=0.05 * spread
                )

    def test_streaming_releases_buffer_per_flush(self, monkeypatch):
        # The whole point: the buffer never holds more than buffer_granules
        # granules' reads. Track the high-water of buffered reads via the
        # aggregator the worker builds.
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2})
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, n_granules=6)

        highwater = {"n": 0}
        orig_add = StreamingAggregator.add_read

        def tracking_add(self, chunk):
            orig_add(self, chunk)
            highwater["n"] = max(highwater["n"], len(self._buffer))

        monkeypatch.setattr(StreamingAggregator, "add_read", tracking_add)
        _, _, meta = _run(monkeypatch, cfg, grid, key, dfs)
        assert meta["total_obs"] > 0
        assert highwater["n"] <= 2  # one read per granule (single group)

    def test_streaming_via_backend_seam_unpinned_config(self, monkeypatch):
        # Review fold (PR #176): the other worker tests pin the backend in
        # config; this one leaves the config UNPINNED (the inline default,
        # issue #170) and stubs the backend at the worker's
        # ``index_from_config`` seam instead (the test_processing.py idiom)
        # -- guarding that the streaming buffer mechanics are independent of
        # how the backend resolves. Exercising the compiled inline decode
        # itself needs real HDF5 fixtures (issue #175 tracks the gap).
        from zagg.index.hierarchical import HierarchicalIndex

        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2})
        del cfg.data_source["index"]
        monkeypatch.setattr(
            "zagg.processing.worker.index_from_config", lambda c: HierarchicalIndex()
        )
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, n_granules=3)
        df_s, ragged, meta = _run(monkeypatch, cfg, grid, key, dfs)
        assert meta["total_obs"] > 0
        assert "h_tdigest" in ragged

    def test_streaming_empty_shard_matches_pooled_no_data(self, monkeypatch):
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2})
        grid = _grid(cfg)

        monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: None)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        df_out, meta = process_shard(grid, key, ["s3://b/g0.h5"], s3_credentials=_CREDS, config=cfg)
        assert df_out.empty
        assert meta["error"] == "No data after filtering"

    def test_invalid_streaming_config_raises_before_reads(self, monkeypatch):
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2})
        cfg.aggregation["variables"]["h_mean"] = {"function": "mean", "source": "h_ph"}
        grid = _grid(cfg)

        def explode(*a, **k):  # pragma: no cover - must not be reached
            raise AssertionError("reads should not start under an invalid streaming config")

        monkeypatch.setattr("zagg.processing._read_group", explode)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        with pytest.raises(ValueError, match="not streamable"):
            process_shard(grid, key, ["s3://b/g0.h5"], s3_credentials=_CREDS, config=cfg)


def _granule_dfs_cells(grid, shard_key, cell_idx_lists, obs_per_cell=50, seed=0, nan_cells=()):
    """One DataFrame per granule over caller-chosen child-cell indices.

    Unlike ``_granule_dfs`` (fixed three cells), each granule can hit a
    different cell subset. Cells in ``nan_cells`` get all-NaN heights (an
    empty digest but a nonzero count).
    """
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


class TestStreamingOccupied:
    def test_occupied_out_fed_under_streaming(self, monkeypatch):
        # The occupied-cell sink (issue #200, feeds the coverage/MOC stamp)
        # must mirror the pooled path's occupied set under streaming.
        key = _shard_key()
        cell_lists = [[0, 4, 8], [2, 4, 10], [1, 8, 9]]
        occ = {}
        for label, streaming in (("pooled", None), ("merge", {"buffer_granules": 1})):
            cfg = _config(streaming=streaming)
            grid = _grid(cfg)
            dfs = _granule_dfs_cells(grid, key, cell_lists, seed=5)
            reads = iter(dfs)
            monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: next(reads))
            monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
            monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
            sink: list = []
            process_shard(
                grid,
                key,
                [f"s3://b/g{i}.h5" for i in range(len(dfs))],
                s3_credentials=_CREDS,
                config=cfg,
                occupied_out=sink,
            )
            occ[label] = np.concatenate(sink) if sink else np.empty(0, dtype=np.uint64)
        assert occ["merge"].size > 0
        np.testing.assert_array_equal(np.sort(occ["merge"]), np.sort(occ["pooled"]))


class TestStreamingReviewFolds:
    """Folds from the phase-4 adversarial review."""

    def test_sourceless_tdigest_field_defaults_like_pooled(self):
        # Pooled defaults source -> value_col ("h_li"); the aggregator must not
        # die with a bare KeyError on the same config.
        cfg = _config(streaming={"buffer_granules": 2})
        del cfg.aggregation["variables"]["h_tdigest"]["source"]
        agg = StreamingAggregator(cfg, _grid(cfg), "pandas", 2)
        assert agg._digest_fields["h_tdigest"][0] == "h_li"

    def test_mis_declared_inner_shape_rejected(self):
        # The buffered path never runs _coerce_ragged_value, so a non-(2,)
        # inner_shape must fail at validation, not silently diverge on disk.
        cfg = _config()
        cfg.aggregation["variables"]["h_tdigest"]["inner_shape"] = [3]
        with pytest.raises(ValueError, match="inner_shape"):
            validate_streaming(cfg)

    def test_count_alias_of_len_accepted(self):
        # aggregate.py treats ("len", "count") identically; so must streaming.
        cfg = _config()
        cfg.aggregation["variables"]["count"]["function"] = "count"
        validate_streaming(cfg)

    def test_profile_charges_merge_to_read_phase(self, monkeypatch):
        # All flush cost (intermediate AND tail) is deliberately charged to
        # the ``read`` phase; the stamp lands after the tail flush.
        key = _shard_key()
        cfg = _config(streaming={"buffer_granules": 2})
        grid = _grid(cfg)
        dfs = _granule_dfs(grid, key, n_granules=3)
        reads = iter(dfs)
        monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: next(reads))
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        _, meta = process_shard(
            grid,
            key,
            [f"s3://b/g{i}.h5" for i in range(3)],
            s3_credentials=_CREDS,
            config=cfg,
            profile=True,
        )
        timings = meta["phase_timings"]
        assert set(timings) == {"read", "index", "aggregate"}
        assert timings["read"] >= 0
