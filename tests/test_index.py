"""Virtual chunk-index backends: protocol, registry, config, worker seam (issue #160)."""

from pathlib import Path

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
from zagg.index.inline import (
    MANIFEST_DTYPES,
    InlineIndex,
    build_chunk_map,
    granule_manifest,
)
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

    # Discovery is memoized per interpreter; reset so this test's fake entry
    # points are actually scanned (monkeypatch restores the prior cache after).
    monkeypatch.setattr(zindex, "_EP_BACKENDS", None)
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

        monkeypatch.setattr(zindex, "_EP_BACKENDS", None)
        monkeypatch.setattr(zindex.metadata, "entry_points", exploding_entry_points)
        with caplog.at_level("ERROR", logger="zagg.index"):
            backends = available_index_backends()
        assert backends["hierarchical"] is HierarchicalIndex
        # A failed lookup is not cached: the next call retries the scan.
        assert zindex._EP_BACKENDS is None

    def test_real_environment_discovery(self, monkeypatch):
        # issue #149: h5coro-hidefix is a core dep, but 0.1.1 ships only the
        # compiled reader — no zagg.index_backends entry point — so `sidecar`
        # is NOT expected here yet. Scan the *real* installed entry points
        # (reset the memo, don't fake the lookup): builtins must resolve, and
        # if a future h5coro-hidefix release does register `sidecar`, it must
        # be a VirtualIndex subclass rather than failing this test.
        monkeypatch.setattr(zindex, "_EP_BACKENDS", None)
        backends = available_index_backends()
        assert backends["hierarchical"] is HierarchicalIndex
        assert backends["inline"] is InlineIndex
        if "sidecar" in backends:  # flips once upstream ships the entry point
            assert issubclass(backends["sidecar"], VirtualIndex)

    def test_discovery_memoized(self, monkeypatch):
        calls = []

        def counting_entry_points(*, group):
            calls.append(group)
            return [_FakeEntryPoint("external", _ExternalIndex)]

        monkeypatch.setattr(zindex, "_EP_BACKENDS", None)
        monkeypatch.setattr(zindex.metadata, "entry_points", counting_entry_points)
        assert get_index_backend("external") is _ExternalIndex
        assert get_index_backend("external") is _ExternalIndex
        assert len(calls) == 1  # one scan per interpreter, not per resolution


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

    def test_unknown_backend_rejected(self, monkeypatch):
        # Config validation keeps zagg.config's ValueError contract; the
        # KeyError-shaped UnknownCapability is the runtime-resolution error.
        # Entry points are pinned empty so this stays a *unknown*-name test
        # even after a future h5coro-hidefix release registers `sidecar`
        # for real (issue #149).
        _patch_entry_points(monkeypatch, [])
        with pytest.raises(ValueError, match="index_backend 'sidecar'"):
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
    def test_absent_block_resolves_inline(self):
        # issue #170 phase 3: the compiled inline read is the default for
        # every data source -- flat (like this one) and planned alike.
        backend = index_from_config(_worker_cfg())
        assert isinstance(backend, InlineIndex)
        backend = index_from_config(_worker_cfg(read_plan={"spatial_index": "segments"}))
        assert isinstance(backend, InlineIndex)

    def test_absent_block_with_apriori_boundaries_stays_hierarchical(self):
        # a-priori chunk_boundaries take precedence inside _read_group and
        # are mutually exclusive with inline's addressing.
        backend = index_from_config(
            _worker_cfg(read_plan={"chunk_boundaries": {"prefix": "s3://x/boundaries/"}})
        )
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
        rec = next(r for r in caplog.records if "finish_granule failed" in r.message)
        # Reason inlined, no exc_info: a folded traceback would trip the
        # WorkerErrorCount metric filter (issue #175) on this tolerated path.
        assert "write-back store unreachable" in rec.message
        assert rec.exc_info is None

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


# ---------------------------------------------------------------------------
# Inline backend (issue #160 phase 2): chunk map + chunk-aligned planned reads
#
# These tests read a real (tiny, committed) HDF5 fixture through h5coro's
# FileDriver: the v1 chunk B-tree the inline backend walks is not reachable
# through stubs. See tests/data/index/make_fixture.py for the fixture's
# layout (20 segments x 128 photons, one empty segment, 256-photon chunks,
# gzip+shuffle) and regeneration instructions (h5py, offline only).
# ---------------------------------------------------------------------------

FIXTURE_H5 = Path(__file__).parent / "data" / "index" / "atl03_mini.h5"


def _open_fixture():
    from h5coro import filedriver
    from h5coro import h5coro as h5c

    return h5c.H5Coro(str(FIXTURE_H5), filedriver.FileDriver, errorChecking=True, verbose=False)


def _fixture_data_source(**extra):
    """ATL03-shaped hierarchical data_source over the fixture (mirrors the
    shipped atl03.yaml: photon base level, segment spatial index, TEP filter)."""
    ds = {
        "groups": ["gt1l", "gt2l"],
        "coordinates": {
            "latitude": "/{group}/heights/lat_ph",
            "longitude": "/{group}/heights/lon_ph",
        },
        "variables": {"h_ph": "/{group}/heights/h_ph"},
        "filters": [
            {"dataset": "/{group}/heights/signal_conf_ph", "column": 0, "op": "ne", "value": -2}
        ],
        "base_level": "photons",
        "levels": {
            "photons": {
                "path": "/{group}/heights",
                "coordinates": {"latitude": "lat_ph", "longitude": "lon_ph"},
                "link": None,
            },
            "segments": {
                "path": "/{group}/geolocation",
                "coordinates": {
                    "latitude": "reference_photon_lat",
                    "longitude": "reference_photon_lon",
                },
                "link": {
                    "to": "photons",
                    "index_beg": "/{group}/geolocation/ph_index_beg",
                    "count": "/{group}/geolocation/segment_ph_cnt",
                    "index_base": 1,
                },
            },
        },
        "read_plan": {"spatial_index": "segments", "pad": 1},
    }
    ds.update(extra)
    return ds


class _LeafSetGrid:
    """Fixture grid: leaf id == round(lat) (photon lat == its segment index,
    gt2l offset +100), shard 1 == a chosen leaf set. Segment- and photon-rate
    masks agree exactly, so hierarchical and inline see the same selection."""

    def __init__(self, leaves):
        self._leaves = np.asarray(sorted(leaves), dtype=np.int64)

    def assign(self, lats, lons):
        return np.round(np.asarray(lats)).astype(np.int64)

    def shards_of(self, leaf_ids):
        return np.isin(leaf_ids, self._leaves).astype(np.int64)


class _LeafCellGrid(_LeafSetGrid):
    """Adds the post-read surface ``process_shard`` needs (single cell)."""

    def children(self, shard_key):
        return np.array([0], dtype=np.int64)

    def cells_of(self, leaf_ids):
        return np.zeros(len(leaf_ids), dtype=np.int64)

    def chunk_coords(self, shard_key):
        return {"cell_lat": np.zeros(1), "cell_lon": np.zeros(1)}


# Segment sets chosen against the fixture geometry (128-photon segments,
# 256-photon chunks, segment 8 empty): padding by 1 puts the plan start at
# an ODD multiple of 128 (not a chunk boundary) so the h5coro start-edge
# off-by-one (PR #152) stays out of the *hierarchical* reference path.
_UNALIGNED_LEAVES = (4, 5, 13, 14, 104, 105)  # plan starts 384 / 1408 (+gt2l 384)
# Padding {3,4,5} starts the plan at segment 2 -> photon 256 == chunk boundary.
_ALIGNED_LEAVES = (3, 4, 5)


class TestChunkMap:
    def test_chunked_dataset_map(self):
        h5obj = _open_fixture()
        cm = build_chunk_map(h5obj, "/gt1l/heights/h_ph")
        assert cm.dims == (2432,)
        assert cm.chunk_dims == (256,)
        assert cm.elem_start.tolist() == [256 * k for k in range(10)]
        assert cm.elem_end.tolist() == [256 * k + 256 for k in range(9)] + [2432]
        assert (cm.nbytes > 0).all()
        assert (cm.filter_mask == 0).all()  # gzip+shuffle applied to every chunk
        assert len(set(cm.byte_offset.tolist())) == len(cm)  # distinct file offsets

    def test_2d_dataset_collapses_trailing_dim(self):
        h5obj = _open_fixture()
        cm = build_chunk_map(h5obj, "/gt1l/heights/signal_conf_ph")
        assert cm.dims == (2432, 5)
        assert cm.chunk_dims == (256, 5)
        assert len(cm) == 10  # one row per first-axis chunk position

    def test_contiguous_dataset_pseudo_chunk(self):
        h5obj = _open_fixture()
        cm = build_chunk_map(h5obj, "/gt1l/geolocation/ph_index_beg")
        assert len(cm) == 1
        assert (cm.elem_start[0], cm.elem_end[0]) == (0, 20)
        assert cm.nbytes[0] == 20 * 8  # int64, uncompressed
        assert cm.filter_mask[0] == 0

    def test_missing_dataset_raises_keyerror(self):
        h5obj = _open_fixture()
        with pytest.raises(KeyError, match="nope"):
            build_chunk_map(h5obj, "/gt1l/heights/nope")

    def test_starts_on_boundary(self):
        h5obj = _open_fixture()
        cm = build_chunk_map(h5obj, "/gt1l/heights/h_ph")
        assert cm.starts_on_boundary(0)
        assert cm.starts_on_boundary(256)
        assert cm.starts_on_boundary(2304)  # last (partial) chunk's start
        assert not cm.starts_on_boundary(255)
        assert not cm.starts_on_boundary(257)
        assert not cm.starts_on_boundary(2432)  # dataset end, not a chunk start


class TestInlineReadGroup:
    def _read_both(self, group, leaves, arrow=False):
        ds = _fixture_data_source()
        grid = _LeafSetGrid(leaves)
        out_h = HierarchicalIndex().read_group(_open_fixture(), group, ds, 1, grid, arrow=arrow)
        out_i = InlineIndex().read_group(_open_fixture(), group, ds, 1, grid, arrow=arrow)
        return out_h, out_i

    @pytest.mark.parametrize("group", ["gt1l", "gt2l"])
    def test_row_identical_to_hierarchical(self, group):
        df_h, df_i = self._read_both(group, _UNALIGNED_LEAVES)
        assert df_h is not None and len(df_h) > 0
        pd.testing.assert_frame_equal(df_i, df_h)
        for col in df_h.columns:
            assert df_i[col].to_numpy().tobytes() == df_h[col].to_numpy().tobytes()

    def test_arrow_carrier_identical(self):
        t_h, t_i = self._read_both("gt1l", _UNALIGNED_LEAVES, arrow=True)
        assert t_h.column_names == t_i.column_names
        for name in t_h.column_names:
            a_h = np.asarray(t_h.column(name))
            a_i = np.asarray(t_i.column(name))
            assert a_i.tobytes() == a_h.tobytes()

    def test_empty_shard_returns_none(self):
        df_h, df_i = self._read_both("gt1l", (77,))  # no such leaf
        assert df_h is None and df_i is None

    def test_chunk_boundary_start_succeeds_where_plain_hyperslice_fails(self):
        # Padding {3,4,5} starts the plan exactly on photon 256 (an interior
        # chunk boundary). h5coro 1.0.4's B-tree start-edge intersection drops
        # such hyperslices (PR #152 off-by-one; the read comes back None), so
        # the hierarchical reference CANNOT serve this shard...
        ds = _fixture_data_source()
        grid = _LeafSetGrid(_ALIGNED_LEAVES)
        with pytest.raises(Exception):
            HierarchicalIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        # ...while inline's one-element-early chunk-aligned read sidesteps it.
        # Gate the output against numpy ground truth from full-array reads.
        df_i = InlineIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        h5obj = _open_fixture()
        paths = [
            "/gt1l/heights/lat_ph",
            "/gt1l/heights/lon_ph",
            "/gt1l/heights/h_ph",
            "/gt1l/heights/signal_conf_ph",
        ]
        full = h5obj.readDatasets(paths)
        leaf = np.round(full["/gt1l/heights/lat_ph"]).astype(np.int64)
        keep = np.isin(leaf, np.asarray(_ALIGNED_LEAVES)) & (
            full["/gt1l/heights/signal_conf_ph"][:, 0] != -2
        )
        assert df_i["h_ph"].to_numpy().tobytes() == full["/gt1l/heights/h_ph"][keep].tobytes()
        assert df_i["leaf_id"].to_numpy().tolist() == leaf[keep].tolist()

    def test_compiled_decoder_engaged(self, monkeypatch, caplog):
        # Byte-parity alone can't distinguish the compiled route from a silent
        # per-dataset fallback; pin that the hidefix Index is actually built
        # AND that no dataset degraded (the spy alone fires on the attempt,
        # not the success -- review finding, PR #173).
        import logging

        import h5coro_hidefix.manifest as hh_manifest

        calls = []
        orig = hh_manifest.datasets_from_manifest

        def spy(columns):
            calls.append(1)
            return orig(columns)

        monkeypatch.setattr(hh_manifest, "datasets_from_manifest", spy)
        with caplog.at_level(logging.WARNING, logger="zagg.index.inline"):
            df = InlineIndex().read_group(
                _open_fixture(), "gt1l", _fixture_data_source(), 1, _LeafSetGrid(_UNALIGNED_LEAVES)
            )
        assert df is not None and len(df) > 0
        assert calls, "compiled decode was never engaged"
        assert "compiled decode unavailable" not in caplog.text
        assert "no chunk map" not in caplog.text

    def test_falls_back_to_h5coro_on_compiled_failure(self, monkeypatch, caplog):
        # A broken Index reconstruction degrades per dataset (warning, h5coro
        # decode) and never aborts the shard; rows stay identical.
        import logging

        import h5coro_hidefix.manifest as hh_manifest

        def boom(columns):
            raise RuntimeError("boom")

        monkeypatch.setattr(hh_manifest, "datasets_from_manifest", boom)
        ds = _fixture_data_source()
        grid = _LeafSetGrid(_UNALIGNED_LEAVES)
        with caplog.at_level(logging.WARNING, logger="zagg.index.inline"):
            df_i = InlineIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        df_h = HierarchicalIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        pd.testing.assert_frame_equal(df_i, df_h)
        assert "compiled decode unavailable" in caplog.text

    def test_bad_dataset_degrades_alone(self, monkeypatch, caplog):
        # A dataset hidefix rejects pins only ITSELF to the fallback; the
        # group's other datasets stay compiled (PR #173 review: a shared
        # Index rebuilt from all maps degraded innocent paths in cascade).
        import logging

        import h5coro_hidefix.manifest as hh_manifest

        orig = hh_manifest.datasets_from_manifest
        bad = "/gt1l/heights/h_ph"

        def picky(columns):
            if bad in set(columns["dataset"]):
                raise ValueError("unsupported chunk table")
            return orig(columns)

        monkeypatch.setattr(hh_manifest, "datasets_from_manifest", picky)
        ds = _fixture_data_source()
        grid = _LeafSetGrid(_UNALIGNED_LEAVES)
        with caplog.at_level(logging.WARNING, logger="zagg.index.inline"):
            df_i = InlineIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        df_h = HierarchicalIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        pd.testing.assert_frame_equal(df_i, df_h)
        warned = [r.message for r in caplog.records if "compiled decode unavailable" in r.message]
        assert len(warned) == 1 and bad in warned[0]

    def test_none_buffer_surfaces_as_io_error(self, monkeypatch):
        # h5coro drivers swallow exceptions and return None on failed ranged
        # reads; that's transient I/O, not a decode defect -- it must raise,
        # not silently pin the dataset to the slow path (PR #173 review).
        # Exercised at the read_fn seam: h5coro's own readDatasets also
        # issues caching=False requests, so a read_group-wide patch breaks
        # the selection reads before the compiled path is ever reached.
        h5obj = _open_fixture()
        read_fn = InlineIndex()._chunk_aligned_read_fn(h5obj)
        orig = h5obj.ioRequest

        def flaky(pos, size, caching=True, **kwargs):
            if caching is False:  # the compiled data reads pass this
                return None
            return orig(pos, size, caching=caching, **kwargs)

        monkeypatch.setattr(h5obj, "ioRequest", flaky)
        with pytest.raises(OSError, match="ranged read failed"):
            read_fn("/gt1l/heights/h_ph", [(0, 100)])

    def test_inline_serves_flat_sources(self):
        # issue #170 phase 2: a read-plan-less (flat) data source takes the
        # full-read route through the same compiled seam -- row-identical to
        # hierarchical. This is the shape most non-ATL03 targets have.
        ds = {
            "groups": ["gt1l", "gt2l"],
            "coordinates": {
                "latitude": "/{group}/heights/lat_ph",
                "longitude": "/{group}/heights/lon_ph",
            },
            "variables": {"h_ph": "/{group}/heights/h_ph"},
            "filters": [
                {
                    "dataset": "/{group}/heights/signal_conf_ph",
                    "column": 0,
                    "op": "ne",
                    "value": -2,
                }
            ],
        }
        validate_index_config({"backend": "inline"}, ds)  # accepted, not rejected
        grid = _LeafSetGrid(_UNALIGNED_LEAVES)
        df_i = InlineIndex().read_group(_open_fixture(), "gt1l", ds, 1, grid)
        # Gate against numpy ground truth from full-array reads (the
        # hierarchical reference trips h5coro's PR #152 start-edge off-by-one
        # here: this leaf set's flat-route window starts exactly on a chunk
        # boundary — which the compiled route survives by construction).
        h5obj = _open_fixture()
        full = h5obj.readDatasets(
            [
                "/gt1l/heights/lat_ph",
                "/gt1l/heights/h_ph",
                "/gt1l/heights/signal_conf_ph",
            ]
        )
        leaf = np.round(full["/gt1l/heights/lat_ph"]).astype(np.int64)
        keep = np.isin(leaf, np.asarray(_UNALIGNED_LEAVES)) & (
            full["/gt1l/heights/signal_conf_ph"][:, 0] != -2
        )
        assert df_i is not None and len(df_i) == int(keep.sum()) > 0
        assert df_i["h_ph"].to_numpy().tobytes() == full["/gt1l/heights/h_ph"][keep].tobytes()
        assert df_i["leaf_id"].to_numpy().tolist() == leaf[keep].tolist()

    def test_flat_source_engages_compiled_decoder(self, monkeypatch, caplog):
        # The flat route must actually decode through hidefix, not silently
        # fall back per dataset (mirrors test_compiled_decoder_engaged).
        import logging

        import h5coro_hidefix.manifest as hh_manifest

        calls = []
        orig = hh_manifest.datasets_from_manifest

        def spy(columns):
            calls.append(1)
            return orig(columns)

        monkeypatch.setattr(hh_manifest, "datasets_from_manifest", spy)
        ds = {
            "groups": ["gt1l"],
            "coordinates": {
                "latitude": "/{group}/heights/lat_ph",
                "longitude": "/{group}/heights/lon_ph",
            },
            "variables": {"h_ph": "/{group}/heights/h_ph"},
        }
        with caplog.at_level(logging.WARNING, logger="zagg.index.inline"):
            df = InlineIndex().read_group(
                _open_fixture(), "gt1l", ds, 1, _LeafSetGrid(_UNALIGNED_LEAVES)
            )
        assert df is not None and len(df) > 0
        assert calls, "compiled decode was never engaged on the flat route"
        assert "compiled decode unavailable" not in caplog.text
        assert "no chunk map" not in caplog.text

    def test_read_workers_pool_byte_identical(self):
        # issue #170 phase 4: pooled reads (read_workers > 1) are keyed by
        # path, so completion order cannot leak into output -- byte-identical
        # to the serial form on both routes.
        grid = _LeafSetGrid(_UNALIGNED_LEAVES)
        df1 = InlineIndex().read_group(
            _open_fixture(), "gt1l", _fixture_data_source(read_workers=1), 1, grid
        )
        df8 = InlineIndex().read_group(
            _open_fixture(), "gt1l", _fixture_data_source(read_workers=8), 1, grid
        )
        pd.testing.assert_frame_equal(df8, df1)
        for col in df1.columns:
            assert df8[col].to_numpy().tobytes() == df1[col].to_numpy().tobytes()

    def test_read_workers_validation(self):
        from zagg.processing.read import _read_workers

        for bad in (0, -1, True, "eight", 2.5):
            with pytest.raises(ValueError, match="read_workers"):
                _read_workers({"read_workers": bad})
        assert _read_workers({}) == 8
        assert _read_workers({"read_workers": 3}) == 3

    def test_inline_accepts_no_stray_keys(self):
        with pytest.raises(ValueError, match="not accepted by backend 'inline'"):
            validate_index_config({"backend": "inline", "on_miss": "fallback"})

    def test_inline_rejects_apriori_chunk_boundaries(self):
        # The a-priori arm (issue #148 arm 2a) takes precedence inside
        # _read_group, which would silently bypass inline's chunk-map
        # addressing -- the combination is a config error.
        ds = _fixture_data_source()
        ds["read_plan"]["chunk_boundaries"] = {"prefix": "s3://x/boundaries/"}
        with pytest.raises(ValueError, match="mutually exclusive"):
            validate_index_config({"backend": "inline"}, ds)


class TestInlineWorker:
    def _cfg(self, index=None):
        ds = _fixture_data_source(**({"index": index} if index else {}))
        return PipelineConfig(
            data_source=ds,
            aggregation={
                "variables": {
                    "count": {
                        "function": "len",
                        "source": "h_ph",
                        "dtype": "int32",
                        "fill_value": 0,
                    },
                    "h_min": {"function": "min", "source": "h_ph", "dtype": "float32"},
                    "h_mean": {"function": "mean", "source": "h_ph", "dtype": "float32"},
                }
            },
            output={"store": "unused"},
        )

    def test_process_shard_inline_byte_identical_to_hierarchical(self):
        from h5coro import filedriver

        grid = _LeafCellGrid(_UNALIGNED_LEAVES)
        outs = {}
        for name, index in [("hier", None), ("inline", {"backend": "inline"})]:
            df, meta = process_shard(
                grid,
                1,
                [str(FIXTURE_H5)],
                s3_credentials={},
                h5coro_driver=filedriver.FileDriver,
                config=self._cfg(index),
            )
            assert meta["error"] is None
            assert meta["files_processed"] == 1
            outs[name] = (df, meta)
        df_h, meta_h = outs["hier"]
        df_i, meta_i = outs["inline"]
        assert meta_i["total_obs"] == meta_h["total_obs"] > 0
        pd.testing.assert_frame_equal(df_i, df_h)
        for col in df_h.columns:
            assert df_i[col].to_numpy().tobytes() == df_h[col].to_numpy().tobytes()


# ---------------------------------------------------------------------------
# Inline write-back (issue #160 phase 3): granule-keyed manifest to the store
# ---------------------------------------------------------------------------

_HEIGHTS = ("lat_ph", "lon_ph", "h_ph", "signal_conf_ph")


class TestInlineWriteBackConfig:
    def test_write_back_must_be_bool(self):
        with pytest.raises(ValueError, match="write_back must be a boolean"):
            validate_index_config({"backend": "inline", "write_back": "yes"})

    def test_write_back_requires_store(self):
        with pytest.raises(ValueError, match="requires 'store'"):
            validate_index_config({"backend": "inline", "write_back": True})

    def test_store_without_write_back_rejected(self):
        # inline never READS the store (that's sidecar) -- a bare store key is
        # a config error per the issue's irrelevant-keys-are-errors semantics.
        with pytest.raises(ValueError, match="only meaningful"):
            validate_index_config({"backend": "inline", "store": "s3://b/p/"})

    def test_valid_write_back_block(self, tmp_path):
        ds = _fixture_data_source()
        validate_index_config({"backend": "inline", "write_back": True, "store": str(tmp_path)}, ds)
        backend = index_from_config(
            PipelineConfig(
                data_source=_fixture_data_source(
                    index={"backend": "inline", "write_back": True, "store": str(tmp_path)}
                )
            )
        )
        assert isinstance(backend, InlineIndex)
        assert backend.write_back is True
        assert backend.store == str(tmp_path)


class TestGranuleManifest:
    def test_rows_match_chunk_maps(self):
        h5obj = _open_fixture()
        paths = [
            "/gt1l/heights/h_ph",
            "/gt1l/heights/signal_conf_ph",
            "/gt1l/geolocation/ph_index_beg",
        ]
        maps = {p: build_chunk_map(h5obj, p) for p in paths}
        df = granule_manifest(maps)
        assert list(df.columns) == list(MANIFEST_DTYPES)
        assert df["dataset"].tolist() == sorted(df["dataset"].tolist())

        h_ph = df[df["dataset"] == "/gt1l/heights/h_ph"]
        cm = maps["/gt1l/heights/h_ph"]
        assert len(h_ph) == 10
        assert h_ph["chunk_idx"].tolist() == list(range(10))
        assert h_ph["byte_offset"].tolist() == cm.byte_offset.tolist()
        assert h_ph["nbytes"].tolist() == cm.nbytes.tolist()
        assert h_ph["elem_start"].tolist() == cm.elem_start.tolist()
        assert h_ph["elem_end"].tolist() == cm.elem_end.tolist()
        assert set(h_ph["dtype"]) == {"<f4"}
        assert h_ph["gzip"].all() and h_ph["shuffle"].all()
        assert set(h_ph["shape"]) == {"[2432]"}
        assert set(h_ph["chunk_shape"]) == {"[256]"}
        assert h_ph["chunk_offset"].tolist() == [f"[{256 * k}]" for k in range(10)]

        conf = df[df["dataset"] == "/gt1l/heights/signal_conf_ph"]
        assert len(conf) == 10  # trailing chunk grid is 1-wide, one real chunk per row
        assert set(conf["shape"]) == {"[2432, 5]"}
        assert set(conf["chunk_shape"]) == {"[256, 5]"}
        assert conf["chunk_offset"].iloc[1] == "[256, 0]"

        contig = df[df["dataset"] == "/gt1l/geolocation/ph_index_beg"]
        assert len(contig) == 1
        assert contig.iloc[0]["nbytes"] == 20 * 8
        assert contig.iloc[0]["dtype"] == "<i8"
        assert not contig.iloc[0]["gzip"] and not contig.iloc[0]["shuffle"]

    def test_empty_maps_give_typed_empty_frame(self):
        df = granule_manifest({})
        assert list(df.columns) == list(MANIFEST_DTYPES)
        assert len(df) == 0
        assert df["chunk_idx"].dtype == np.int64

    def test_dtype_strings_carry_explicit_byte_order(self):
        # from_chunks contract pin (espg decision relayed on the PR thread):
        # dtype strings are byte-order-explicit np.dtype(...).str forms
        # ('<f4', '|i1'), never bare names; gzip stays a boolean.
        h5obj = _open_fixture()
        paths = [f"/gt1l/heights/{name}" for name in _HEIGHTS] + ["/gt1l/geolocation/ph_index_beg"]
        df = granule_manifest({p: build_chunk_map(h5obj, p) for p in paths})
        assert set(df[df["dataset"] == "/gt1l/heights/signal_conf_ph"]["dtype"]) == {"|i1"}
        assert set(df[df["dataset"] == "/gt1l/heights/lat_ph"]["dtype"]) == {"<f8"}
        assert df["dtype"].str.match(r"^[<>|][a-z]\d+$").all()
        assert df["gzip"].dtype == np.dtype(bool)


class TestInlineWriteBackWorker:
    def _run(self, tmp_path, index):
        from h5coro import filedriver

        return process_shard(
            _LeafCellGrid(_UNALIGNED_LEAVES),
            1,
            [str(FIXTURE_H5)],
            s3_credentials={},
            h5coro_driver=filedriver.FileDriver,
            config=PipelineConfig(
                data_source=_fixture_data_source(index=index),
                aggregation={
                    "variables": {
                        "count": {
                            "function": "len",
                            "source": "h_ph",
                            "dtype": "int32",
                            "fill_value": 0,
                        }
                    }
                },
                output={"store": "unused"},
            ),
        )

    def _expected_datasets(self, beams=("gt1l", "gt2l")):
        # Deterministic coverage per visited group: base-rate coords +
        # variables + filter datasets, plus the spatial-index level's coord
        # and link arrays (the bench extractor's manifest convention).
        geoloc = (
            "reference_photon_lat",
            "reference_photon_lon",
            "ph_index_beg",
            "segment_ph_cnt",
        )
        return {f"/{beam}/heights/{name}" for beam in beams for name in _HEIGHTS} | {
            f"/{beam}/geolocation/{name}" for beam in beams for name in geoloc
        }

    def test_round_trip_to_local_store(self, tmp_path):
        store = tmp_path / "zagg-index" / "ATL03" / "007"  # created by open_object_store
        df_out, meta = self._run(
            tmp_path, {"backend": "inline", "write_back": True, "store": str(store)}
        )
        assert meta["error"] is None
        manifest_path = store / "atl03_mini.parquet"  # granule id == URL stem
        assert manifest_path.is_file()
        df = pd.read_parquet(manifest_path, engine="fastparquet")
        assert list(df.columns) == list(MANIFEST_DTYPES)
        assert set(df["dataset"]) == self._expected_datasets()
        h5obj = _open_fixture()
        cm = build_chunk_map(h5obj, "/gt1l/heights/h_ph")
        got = df[df["dataset"] == "/gt1l/heights/h_ph"].sort_values("chunk_idx")
        assert got["byte_offset"].tolist() == cm.byte_offset.tolist()
        assert got["nbytes"].tolist() == cm.nbytes.tolist()

    def test_coverage_deterministic_for_empty_groups(self, tmp_path):
        # A shard matching only gt1l leaves: gt2l's read returns None before
        # the read seam runs, but the manifest still covers gt2l's datasets
        # (prebuilt per visited group), so concurrent shards of one granule
        # write identical manifests and last-writer-wins is idempotent.
        from h5coro import filedriver

        store = tmp_path / "idx"
        df_out, meta = process_shard(
            _LeafCellGrid((4, 5)),  # gt1l only; gt2l lats are 100+
            1,
            [str(FIXTURE_H5)],
            s3_credentials={},
            h5coro_driver=filedriver.FileDriver,
            config=PipelineConfig(
                data_source=_fixture_data_source(
                    index={"backend": "inline", "write_back": True, "store": str(store)}
                ),
                aggregation={
                    "variables": {
                        "count": {
                            "function": "len",
                            "source": "h_ph",
                            "dtype": "int32",
                            "fill_value": 0,
                        }
                    }
                },
                output={"store": "unused"},
            ),
        )
        assert meta["error"] is None
        df = pd.read_parquet(store / "atl03_mini.parquet", engine="fastparquet")
        assert set(df["dataset"]) == self._expected_datasets()

    def test_write_back_off_writes_nothing(self, tmp_path):
        df_out, meta = self._run(tmp_path, {"backend": "inline"})
        assert meta["error"] is None
        assert list(tmp_path.rglob("*.parquet")) == []

    def test_store_failure_degrades_to_plain_inline_read(self, tmp_path, caplog):
        # A store path that is an existing FILE cannot become a directory:
        # the write-back raises, the worker logs, and the shard still returns.
        blocker = tmp_path / "blocked"
        blocker.write_text("not a directory")
        with caplog.at_level("WARNING"):
            df_out, meta = self._run(
                tmp_path, {"backend": "inline", "write_back": True, "store": str(blocker)}
            )
        assert meta["error"] is None
        assert meta["files_processed"] == 1
        assert meta["total_obs"] > 0
        assert any("finish_granule failed" in r.message for r in caplog.records)

    def test_finish_granule_drains_pending_when_off(self):
        backend = InlineIndex()
        backend._pending["/x"] = object()
        backend.finish_granule(object(), "s3://b/g.h5")
        assert backend._pending == {}
