"""Virtual chunk-index backends: protocol, registry, config, worker seam (issue #160)."""

import numpy as np
import pandas as pd
import pytest

import zagg.index as zindex
from zagg.config import PipelineConfig, validate_config
from zagg.index import (
    VirtualIndex,
    available_index_backends,
    get_index_backend,
    index_from_config,
    validate_index_config,
)
from zagg.index.hierarchical import HierarchicalIndex
from zagg.processing import _read_group, process_shard
from zagg.registry import UnknownCapability

# ---------------------------------------------------------------------------
# Shared stubs (mirror the test_processing.py conventions)
# ---------------------------------------------------------------------------


class _FakeH5:
    """Stub h5coro object: ``readDatasets`` returns canned arrays by path,
    honoring the ``hyperslice`` bound like the real driver."""

    def __init__(self, arrays):
        self._arrays = arrays

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                out[d] = self._arrays[d]
                continue
            path = d["dataset"]
            arr = self._arrays[path]
            hs = d.get("hyperslice")
            if hs is not None:
                lo, hi = hs[0]
                arr = arr[lo:hi]
            out[path] = arr
        return out


class _ShardGrid:
    """Grid stub: leaf id == row index; every row maps to shard 0."""

    @staticmethod
    def assign(lats, lons):
        return np.arange(len(lats))

    @staticmethod
    def shards_of(leaf_ids):
        return np.zeros(len(leaf_ids), dtype=int)


class _CellGrid(_ShardGrid):
    """Adds the post-read surface ``process_shard`` needs (single cell)."""

    def children(self, shard_key):
        return np.array([0], dtype=np.int64)

    def cells_of(self, leaf_ids):
        return np.zeros(len(leaf_ids), dtype=np.int64)

    def chunk_coords(self, shard_key):
        return {"cell_lat": np.zeros(1), "cell_lon": np.zeros(1)}


def _flat_data_source(**extra):
    ds = {
        "groups": ["gt1l"],
        "coordinates": {"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
        "variables": {"h_li": "/{group}/h"},
    }
    ds.update(extra)
    return ds


def _worker_cfg(**ds_extra):
    return PipelineConfig(
        data_source=_flat_data_source(**ds_extra),
        aggregation={
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            }
        },
        output={"store": "unused"},
    )


def _canned_arrays():
    return {
        "/gt1l/lat": np.array([10.0, 11.0, 12.0]),
        "/gt1l/lon": np.array([20.0, 21.0, 22.0]),
        "/gt1l/h": np.array([100.0, 200.0, 300.0], dtype=np.float32),
    }


class _FakeEntryPoint:
    def __init__(self, name, obj=None, error=None):
        self.name = name
        self._obj = obj
        self._error = error

    def load(self):
        if self._error is not None:
            raise self._error
        return self._obj


def _patch_entry_points(monkeypatch, eps):
    def fake_entry_points(*, group):
        assert group == zindex.INDEX_BACKENDS_GROUP
        return list(eps)

    monkeypatch.setattr(zindex.metadata, "entry_points", fake_entry_points)


class _ExternalIndex(VirtualIndex):
    name = "external"


# ---------------------------------------------------------------------------
# Registry: builtins + entry-point discovery
# ---------------------------------------------------------------------------


class TestBackendRegistry:
    def test_builtin_hierarchical_registered(self):
        assert available_index_backends()["hierarchical"] is HierarchicalIndex
        assert get_index_backend("hierarchical") is HierarchicalIndex

    def test_unknown_backend_raises_with_available(self):
        with pytest.raises(UnknownCapability, match="index_backend 'nope'"):
            get_index_backend("nope")
        # UnknownCapability subclasses KeyError, so except KeyError paths still catch.
        with pytest.raises(KeyError):
            get_index_backend("nope")

    def test_entry_point_backend_discovered(self, monkeypatch):
        _patch_entry_points(monkeypatch, [_FakeEntryPoint("external", _ExternalIndex)])
        assert get_index_backend("external") is _ExternalIndex

    def test_entry_point_cannot_shadow_builtin(self, monkeypatch, caplog):
        class Impostor(VirtualIndex):
            name = "hierarchical"

        _patch_entry_points(monkeypatch, [_FakeEntryPoint("hierarchical", Impostor)])
        with caplog.at_level("ERROR", logger="zagg.index"):
            assert get_index_backend("hierarchical") is HierarchicalIndex
        assert any("collides" in r.message for r in caplog.records)

    def test_broken_entry_point_skipped(self, monkeypatch, caplog):
        _patch_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("broken", error=RuntimeError("boom")),
                _FakeEntryPoint("external", _ExternalIndex),
            ],
        )
        with caplog.at_level("ERROR", logger="zagg.index"):
            backends = available_index_backends()
        assert "broken" not in backends
        assert backends["external"] is _ExternalIndex

    def test_entry_point_lookup_failure_falls_back_to_builtins(self, monkeypatch, caplog):
        def exploding_entry_points(*, group):
            raise RuntimeError("importlib.metadata unhappy")

        monkeypatch.setattr(zindex.metadata, "entry_points", exploding_entry_points)
        with caplog.at_level("ERROR", logger="zagg.index"):
            backends = available_index_backends()
        assert backends["hierarchical"] is HierarchicalIndex


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestIndexConfigValidation:
    def test_non_mapping_rejected(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            validate_index_config("hierarchical")

    def test_backend_key_required(self):
        with pytest.raises(ValueError, match="backend is required"):
            validate_index_config({})

    def test_unknown_backend_rejected(self):
        with pytest.raises(UnknownCapability, match="index_backend 'sidecar'"):
            validate_index_config({"backend": "sidecar"})

    def test_irrelevant_keys_are_errors_not_ignored(self):
        # store/on_miss are backend-specific params, not global keys (issue
        # #160 corrected config semantics): hierarchical accepts none.
        with pytest.raises(ValueError, match="not accepted by backend 'hierarchical'"):
            validate_index_config({"backend": "hierarchical", "store": "s3://bucket/prefix/"})

    def test_valid_hierarchical_block_passes(self):
        validate_index_config({"backend": "hierarchical"})

    def test_wired_into_validate_config(self):
        cfg = _worker_cfg(index={"backend": "hierarchical", "on_miss": "fallback"})
        with pytest.raises(ValueError, match="not accepted by backend"):
            validate_config(cfg)
        validate_config(_worker_cfg(index={"backend": "hierarchical"}))
        validate_config(_worker_cfg())  # absent block stays valid

    def test_required_keys_enforced(self, monkeypatch):
        class NeedsStore(VirtualIndex):
            name = "needs_store"
            config_keys = frozenset({"store"})
            required_config_keys = frozenset({"store"})

        _patch_entry_points(monkeypatch, [_FakeEntryPoint("needs_store", NeedsStore)])
        with pytest.raises(ValueError, match="requires keys \\['store'\\]"):
            validate_index_config({"backend": "needs_store"})
        validate_index_config({"backend": "needs_store", "store": "s3://b/p/"})


# ---------------------------------------------------------------------------
# index_from_config resolution
# ---------------------------------------------------------------------------


class TestIndexFromConfig:
    def test_absent_block_resolves_hierarchical(self):
        backend = index_from_config(_worker_cfg())
        assert isinstance(backend, HierarchicalIndex)

    def test_explicit_hierarchical(self):
        backend = index_from_config(_worker_cfg(index={"backend": "hierarchical"}))
        assert isinstance(backend, HierarchicalIndex)

    def test_invalid_block_raises_at_resolution(self):
        # Dict-built configs that skipped validate_config still fail loudly here.
        with pytest.raises(ValueError, match="not accepted by backend"):
            index_from_config(_worker_cfg(index={"backend": "hierarchical", "write_back": True}))

    def test_entry_point_backend_constructed(self, monkeypatch):
        _patch_entry_points(monkeypatch, [_FakeEntryPoint("external", _ExternalIndex)])
        backend = index_from_config(_worker_cfg(index={"backend": "external"}))
        assert isinstance(backend, _ExternalIndex)


# ---------------------------------------------------------------------------
# Hierarchical backend: pure delegation
# ---------------------------------------------------------------------------


class TestHierarchicalDelegation:
    def test_byte_identical_to_read_group(self):
        ds = _flat_data_source()
        df_direct = _read_group(_FakeH5(_canned_arrays()), "gt1l", ds, 0, _ShardGrid())
        df_backend = HierarchicalIndex().read_group(
            _FakeH5(_canned_arrays()), "gt1l", ds, 0, _ShardGrid()
        )
        pd.testing.assert_frame_equal(df_backend, df_direct)
        for col in df_direct.columns:
            assert df_backend[col].to_numpy().tobytes() == df_direct[col].to_numpy().tobytes()

    def test_resolves_read_group_at_call_time(self, monkeypatch):
        # Monkeypatching zagg.processing._read_group must keep intercepting
        # reads that flow through the backend (the seam's compat guarantee).
        sentinel = pd.DataFrame({"leaf_id": [7]})
        monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: sentinel)
        out = HierarchicalIndex().read_group(object(), "gt1l", {}, 0, None)
        assert out is sentinel

    def test_finish_granule_is_noop(self):
        HierarchicalIndex().finish_granule(object(), "s3://bucket/granule.h5")


# ---------------------------------------------------------------------------
# Worker seam
# ---------------------------------------------------------------------------


class TestWorkerSeam:
    def _patch_h5(self, monkeypatch):
        monkeypatch.setattr(
            "zagg.processing.h5coro.H5Coro", lambda *a, **k: _FakeH5(_canned_arrays())
        )
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_explicit_hierarchical_byte_identical_to_default(self, monkeypatch):
        self._patch_h5(monkeypatch)
        df_default, meta_default = process_shard(
            _CellGrid(), 0, ["s3://a"], s3_credentials={}, config=_worker_cfg()
        )
        df_explicit, meta_explicit = process_shard(
            _CellGrid(),
            0,
            ["s3://a"],
            s3_credentials={},
            config=_worker_cfg(index={"backend": "hierarchical"}),
        )
        pd.testing.assert_frame_equal(df_explicit, df_default)
        for col in df_default.columns:
            assert df_explicit[col].to_numpy().tobytes() == df_default[col].to_numpy().tobytes()
        assert meta_explicit["total_obs"] == meta_default["total_obs"] == 3

    def test_finish_granule_called_once_per_granule(self, monkeypatch):
        self._patch_h5(monkeypatch)
        calls: list = []

        class Recording(HierarchicalIndex):
            def finish_granule(self, h5obj, granule_url):
                calls.append(granule_url)

        monkeypatch.setattr("zagg.processing.worker.index_from_config", lambda cfg: Recording())
        process_shard(_CellGrid(), 0, ["s3://a", "s3://b"], s3_credentials={}, config=_worker_cfg())
        assert calls == ["s3://a", "s3://b"]

    def test_finish_granule_failure_never_fails_the_read(self, monkeypatch, caplog):
        self._patch_h5(monkeypatch)

        class Exploding(HierarchicalIndex):
            def finish_granule(self, h5obj, granule_url):
                raise RuntimeError("write-back store unreachable")

        monkeypatch.setattr("zagg.processing.worker.index_from_config", lambda cfg: Exploding())
        with caplog.at_level("WARNING"):
            df_out, meta = process_shard(
                _CellGrid(), 0, ["s3://a"], s3_credentials={}, config=_worker_cfg()
            )
        assert meta["files_processed"] == 1
        assert meta["total_obs"] == 3
        assert meta["error"] is None
        assert any("finish_granule failed" in r.message for r in caplog.records)

    def test_bad_index_block_fails_before_any_read(self, monkeypatch):
        # Resolution happens up front, so a bad block is a loud config error,
        # not N per-granule warnings.
        self._patch_h5(monkeypatch)
        with pytest.raises(ValueError, match="not accepted by backend"):
            process_shard(
                _CellGrid(),
                0,
                ["s3://a"],
                s3_credentials={},
                config=_worker_cfg(index={"backend": "hierarchical", "store": "s3://x/"}),
            )
