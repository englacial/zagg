"""Tests for the AWS Lambda handler's process-mode event contract (#24).

The handler lives under ``deployment/aws/`` (not an importable package module),
so it is loaded by path. These tests exercise the grid-neutral event schema:
the shard identifier is ``shard_key`` (not ``parent_morton``), and the
HEALPix-specific ``child_order`` is required for HEALPix runs but optional for
other grids.
"""

import importlib.util
import json
import warnings
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from zarr import open_group
from zarr.errors import GroupNotFoundError
from zarr.storage import MemoryStore

from zagg.config import default_config
from zagg.grids import HEALPIX_BASE_CELLS, from_config

REPO_ROOT = Path(__file__).parent.parent
HANDLER_PATH = REPO_ROOT / "deployment" / "aws" / "lambda_handler.py"


@pytest.fixture(scope="module")
def handler_mod():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _context():
    ctx = MagicMock()
    ctx.aws_request_id = "req-1"
    ctx.function_name = "process-shard"
    ctx.memory_limit_in_mb = 2048
    ctx.get_remaining_time_in_millis.return_value = 900_000
    return ctx


def _healpix_config_dict():
    return asdict(default_config("atl06"))


def _rectilinear_config_dict():
    return asdict(default_config("atl06_polar"))


_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}


def _base_event(config_dict):
    return {
        "shard_key": 12345,
        "parent_order": 6,
        "granule_urls": ["s3://b/g.h5"],
        "store_path": "s3://out/x.zarr",
        "s3_credentials": _CREDS,
        "config": config_dict,
    }


class TestProcessEventGate:
    def test_missing_shard_key_rejected(self, handler_mod):
        event = _base_event(_healpix_config_dict())
        del event["shard_key"]
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 400
        assert "shard_key" in json.loads(resp["body"])["error"]

    def test_legacy_parent_morton_not_accepted(self, handler_mod):
        # Hard rename: the old field name is no longer a valid shard identifier.
        event = _base_event(_healpix_config_dict())
        del event["shard_key"]
        event["parent_morton"] = 12345
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 400
        assert "shard_key" in json.loads(resp["body"])["error"]

    def test_healpix_requires_child_order(self, handler_mod):
        # child_order omitted on a HEALPix run -> rejected.
        event = _base_event(_healpix_config_dict())
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 400
        assert "child_order" in json.loads(resp["body"])["error"]


class TestProcessEventDispatch:
    """The gate passes and the shard key flows into ``process_shard`` for both
    a HEALPix event (with child_order) and a rectilinear event (without it)."""

    def _run(self, handler_mod, monkeypatch, event):
        import zagg.grids as grids
        import zagg.processing as processing

        captured = {}

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            captured["shard_key"] = shard_key
            captured["handoff"] = kwargs.get("handoff")
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 0,
                "total_obs": 0,
                "granule_count": len(granule_urls),
                "files_processed": 0,
                "duration_s": 0.0,
                "error": None,
            }
            return pd.DataFrame(), meta

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: MagicMock())
        resp = handler_mod._handle_process(event, _context())
        return resp, captured

    def test_healpix_dispatch(self, handler_mod, monkeypatch):
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["shard_key"] == 12345

    def test_handoff_event_key_forwarded_to_worker(self, handler_mod, monkeypatch):
        # issue #130: an explicit handoff event key reaches process_shard so the
        # deployed worker selects the arro3 arrow carrier.
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event["handoff"] = "arrow"
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["handoff"] == "arrow"

    def test_default_handoff_is_pandas(self, handler_mod, monkeypatch):
        # No handoff key -> the worker runs the byte-identical default ("pandas").
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        assert "handoff" not in event
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["handoff"] == "pandas"

    def test_rectilinear_dispatch_without_child_order(self, handler_mod, monkeypatch):
        event = _base_event(_rectilinear_config_dict())
        event["parent_order"] = None  # rectilinear has no parent_order
        assert "child_order" not in event
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["shard_key"] == 12345

    def test_body_reports_max_memory_mb(self, handler_mod, monkeypatch):
        """Issue #120: every successful invocation stamps a positive
        ``max_memory_mb`` (the worker's peak RSS) into the result body so the
        orchestrator can roll up OOM-proximity without CloudWatch access."""
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp, _ = self._run(handler_mod, monkeypatch, event)
        body = json.loads(resp["body"])
        assert isinstance(body["max_memory_mb"], float)
        assert body["max_memory_mb"] > 0.0


class TestProcessEventWriteLoop:
    """Issue #82 phase 7: the handler drives ``process_shard`` with a
    ``chunk_results`` sink and writes each chunk's dense region (at its own
    block_index) plus its ragged (CSR) companion — the same K>1 write loop the
    local runner runs. These mock the writers/store to assert the loop's wiring
    (sink passed, per-chunk block index used, ragged persisted) without a real
    Zarr store."""

    def _patch(self, handler_mod, monkeypatch, chunks):
        """Patch process_shard to fill ``chunk_results`` with ``chunks`` and capture
        every dense/ragged write. Returns the capture dict."""
        import zagg.grids as grids
        import zagg.processing as processing

        cap = {"dense": [], "ragged": []}

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            sink = kwargs["chunk_results"]
            sink.extend(chunks)
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }
            return pd.DataFrame(), meta

        # A store whose template always exists; record nothing on it. The handler
        # checks existence via ``open_group`` (issue #118), so stub that to succeed.
        store = MagicMock()

        def fake_write_dense(carrier, st, *, grid, chunk_idx):
            cap["dense"].append(chunk_idx)

        def fake_write_ragged(ragged, st, *, grid, shard_key):
            cap["ragged"].append((shard_key, ragged))

        # grid stub: a 1-D companion (single-element block index), exposes
        # chunk_grid_shape so _block_index_key is exercised on its real path.
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        # Regular (non-sharded) write path — a MagicMock attr is truthy by default,
        # so pin it off explicitly (issue #108 routes sharded grids elsewhere).
        grid_stub.sharded = False

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        monkeypatch.setattr(handler_mod, "open_group", lambda *a, **k: MagicMock())
        monkeypatch.setattr(handler_mod, "write_dataframe_to_zarr", fake_write_dense)
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", fake_write_ragged)
        return cap

    def test_k_gt_1_writes_each_chunk_region(self, handler_mod, monkeypatch):
        """K=3: the sink loop writes 3 dense regions at distinct block indices, and
        each chunk's ragged is keyed by its own _block_index_key (not shard_key)."""
        chunks = [
            ((0,), pd.DataFrame(), {}),
            ((1,), pd.DataFrame(), {"h_tdigest": ([], [])}),
            ((2,), pd.DataFrame(), {}),
        ]
        cap = self._patch(handler_mod, monkeypatch, chunks)
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        # One dense write per chunk, at each chunk's own block_index.
        assert cap["dense"] == [(0,), (1,), (2,)]
        # K>1 -> ragged keyed by _block_index_key(block_index) == 0/1/2, NOT shard_key.
        assert [k for k, _r in cap["ragged"]] == [0, 1, 2]

    def test_k_eq_1_ragged_keyed_by_shard_key(self, handler_mod, monkeypatch):
        """K=1: the lone chunk's ragged CSR is persisted (the gap this phase closes:
        the old handler never called write_ragged_to_zarr), keyed by shard_key."""
        chunks = [((0,), pd.DataFrame(), {"h_tdigest": ([], [])})]
        cap = self._patch(handler_mod, monkeypatch, chunks)
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        assert cap["dense"] == [(0,)]
        # Single chunk -> ragged keyed by shard_key (cell-resolution contract).
        assert len(cap["ragged"]) == 1
        assert cap["ragged"][0][0] == event["shard_key"]


class TestProcessEventProfile:
    """Issue #100 phase 3: the handler bridges the opt-in ``profile`` event key
    into ``process_shard`` (read/index/aggregate timing) and brackets the
    write phase it owns, merging ``phase_timings`` into the response body. When
    ``profile`` is absent the worker call and body are unchanged."""

    def _patch(
        self,
        handler_mod,
        monkeypatch,
        *,
        worker_phase_timings,
        chunks,
        template_exists=True,
        write_raises=False,
    ):
        """Patch process_shard to record the ``profile`` kwarg and (when given)
        seed ``metadata['phase_timings']``; fill the sink with ``chunks``.
        ``template_exists`` toggles the ``open_group`` existence check
        (template-missing 500 path, via ``GroupNotFoundError``); ``write_raises``
        makes the dense write blow up (failed-write 500 path)."""
        import zagg.grids as grids
        import zagg.processing as processing

        cap = {}

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            cap["profile"] = kwargs.get("profile")
            kwargs["chunk_results"].extend(chunks)
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }
            if worker_phase_timings is not None:
                meta["phase_timings"] = dict(worker_phase_timings)
            return pd.DataFrame(), meta

        store = MagicMock()
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        # Regular (non-sharded) write path — a MagicMock attr is truthy by default,
        # so pin it off explicitly (issue #108 routes sharded grids elsewhere).
        grid_stub.sharded = False

        # Existence check goes through ``open_group`` (issue #118): present ->
        # returns a group; missing -> raises ``GroupNotFoundError``.
        def fake_open_group(*a, **k):
            if not template_exists:
                raise GroupNotFoundError(grid_stub.group_path)
            return MagicMock()

        def fake_write_dense(*a, **k):
            if write_raises:
                raise RuntimeError("boom")

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        monkeypatch.setattr(handler_mod, "open_group", fake_open_group)
        monkeypatch.setattr(handler_mod, "write_dataframe_to_zarr", fake_write_dense)
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", lambda *a, **k: None)
        return cap

    def _profile_event(self):
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event["profile"] = True
        return event

    def test_profile_forwarded_and_write_bracketed(self, handler_mod, monkeypatch):
        # profile=True flows into process_shard, the worker's read/index/aggregate
        # timings pass through unchanged, and the handler-owned write phase is added.
        worker = {"read": 1.0, "index": 0.5, "aggregate": 0.25}
        cap = self._patch(
            handler_mod,
            monkeypatch,
            worker_phase_timings=worker,
            chunks=[((0,), pd.DataFrame(), {})],
        )
        resp = handler_mod._handle_process(self._profile_event(), _context())
        assert resp["statusCode"] == 200
        assert cap["profile"] is True
        timings = json.loads(resp["body"])["phase_timings"]
        assert set(timings) == {"read", "index", "aggregate", "write"}
        # worker-seeded phases pass through untouched; write is a non-negative delta.
        assert {k: timings[k] for k in worker} == worker
        assert isinstance(timings["write"], float) and timings["write"] >= 0.0

    def test_default_off_no_profile_no_timings(self, handler_mod, monkeypatch):
        # No profile key -> process_shard gets profile=False and the body carries
        # no phase_timings (worker path unchanged).
        cap = self._patch(
            handler_mod,
            monkeypatch,
            worker_phase_timings=None,
            chunks=[((0,), pd.DataFrame(), {})],
        )
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        assert cap["profile"] is False
        assert "phase_timings" not in json.loads(resp["body"])

    def test_profile_no_data_omits_write(self, handler_mod, monkeypatch):
        # Empty chunk_results (no-data shard): the worker still seeds read but no
        # write runs, so the body carries phase_timings WITHOUT a write key.
        self._patch(
            handler_mod,
            monkeypatch,
            worker_phase_timings={"read": 1.0},
            chunks=[],
        )
        resp = handler_mod._handle_process(self._profile_event(), _context())
        assert resp["statusCode"] == 200
        timings = json.loads(resp["body"])["phase_timings"]
        assert "write" not in timings

    def test_profile_failed_write_omits_write(self, handler_mod, monkeypatch):
        # A raising write loop (500): write timing is recorded only on a clean
        # write, so a time-to-failure never leaks in as a real write duration.
        self._patch(
            handler_mod,
            monkeypatch,
            worker_phase_timings={"read": 1.0, "index": 0.5, "aggregate": 0.25},
            chunks=[((0,), pd.DataFrame(), {})],
            write_raises=True,
        )
        resp = handler_mod._handle_process(self._profile_event(), _context())
        assert resp["statusCode"] == 500
        timings = json.loads(resp["body"])["phase_timings"]
        assert "write" not in timings

    def test_profile_missing_template_omits_write(self, handler_mod, monkeypatch):
        # Template-missing early return (500) bypasses the write loop, so write
        # stays absent (the worker-seeded phases still ride along in the body).
        self._patch(
            handler_mod,
            monkeypatch,
            worker_phase_timings={"read": 1.0, "index": 0.5, "aggregate": 0.25},
            chunks=[((0,), pd.DataFrame(), {})],
            template_exists=False,
        )
        resp = handler_mod._handle_process(self._profile_event(), _context())
        assert resp["statusCode"] == 500
        timings = json.loads(resp["body"])["phase_timings"]
        assert "write" not in timings


class TestTemplateExistenceGuard:
    """Issue #118: the pre-write template check used ``store.exists()``, whose
    zarr v3 implementation is async — the un-awaited coroutine is always truthy,
    so ``if not store.exists(...)`` never fired (dead code) and emitted
    ``RuntimeWarning: coroutine ... was never awaited``. The fix checks via
    ``open_group(..., mode="r")`` and catches ``GroupNotFoundError``."""

    def _patch(self, handler_mod, monkeypatch, *, raises_missing):
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            kwargs["chunk_results"].append(((0,), pd.DataFrame(), {}))
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }
            return pd.DataFrame(), meta

        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        grid_stub.sharded = False

        def fake_open_group(*a, **k):
            if raises_missing:
                raise GroupNotFoundError(grid_stub.group_path)
            return MagicMock()

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: MagicMock())
        monkeypatch.setattr(handler_mod, "open_group", fake_open_group)
        monkeypatch.setattr(handler_mod, "write_dataframe_to_zarr", lambda *a, **k: None)
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", lambda *a, **k: None)

    def _event(self):
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        return event

    def test_missing_template_returns_500(self, handler_mod, monkeypatch):
        # The guard must FIRE when the template group is absent. Against the
        # pre-fix un-awaited ``store.exists`` this returned 200 (dead guard).
        self._patch(handler_mod, monkeypatch, raises_missing=True)
        resp = handler_mod._handle_process(self._event(), _context())
        assert resp["statusCode"] == 500
        assert "Zarr template not found" in json.loads(resp["body"])["error"]

    def test_present_template_proceeds(self, handler_mod, monkeypatch):
        # Present template (open_group succeeds) -> guard passes through to 200.
        # The meaningful no-RuntimeWarning check rides on the real-store test
        # below; here open_group is mocked, so a coroutine is never created.
        self._patch(handler_mod, monkeypatch, raises_missing=False)
        resp = handler_mod._handle_process(self._event(), _context())
        assert resp["statusCode"] == 200

    def _patch_real_store(self, handler_mod, monkeypatch, store):
        """Wire the handler to a real zarr ``store`` so the existence check runs the
        actual ``open_group`` (not a mock) — the un-awaited-coroutine regression
        would reappear here if the fix were reverted."""
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            kwargs["chunk_results"].append(((0,), pd.DataFrame(), {}))
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }
            return pd.DataFrame(), meta

        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        grid_stub.sharded = False

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        monkeypatch.setattr(handler_mod, "write_dataframe_to_zarr", lambda *a, **k: None)
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", lambda *a, **k: None)

    def test_real_store_missing_group_returns_500(self, handler_mod, monkeypatch):
        # End-to-end through the real ``open_group`` on an empty store: the missing
        # group raises GroupNotFoundError, so the guard returns the 500.
        store = MemoryStore()
        self._patch_real_store(handler_mod, monkeypatch, store)
        resp = handler_mod._handle_process(self._event(), _context())
        assert resp["statusCode"] == 500
        assert "Zarr template not found" in json.loads(resp["body"])["error"]

    def test_real_store_present_group_proceeds(self, handler_mod, monkeypatch):
        # The group exists -> the real ``open_group`` succeeds, the guard passes
        # (200), and no un-awaited-coroutine RuntimeWarning leaks.
        store = MemoryStore()
        open_group(store, path="8", mode="w", zarr_format=3)
        self._patch_real_store(handler_mod, monkeypatch, store)
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            resp = handler_mod._handle_process(self._event(), _context())
        assert resp["statusCode"] == 200

    def test_present_but_not_a_group_is_not_masked_as_missing(self, handler_mod, monkeypatch):
        # A node present at the template path but of the wrong kind (an array) must
        # NOT be swallowed as "template not found" — open_group raises a non-
        # GroupNotFoundError that escapes the guard and surfaces as a real error,
        # so the response is not the clean template-missing 500.
        from zarr import create_array

        store = MemoryStore()
        create_array(store, name="8", shape=(1,), chunks=(1,), dtype="int64", zarr_format=3)
        self._patch_real_store(handler_mod, monkeypatch, store)
        resp = handler_mod._handle_process(self._event(), _context())
        assert "Zarr template not found" not in resp["body"]


class TestSetupTemplate:
    """Issue #99: the setup handler used to hand-build the HEALPix grid and drop
    ``chunk_inner``, so the template was chunked at ``parent_order`` while workers
    (built via ``from_config``) wrote finer ``chunk_inner`` block indices -> Zarr
    "block index out of bounds". Setup now builds the grid via ``from_config`` too,
    so the two paths share one construction path and can't drift."""

    def _setup(self, handler_mod, monkeypatch, config_dict, **event_extra):
        store = MemoryStore()
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        event = {
            "mode": "setup",
            "store_path": "s3://out/x.zarr",
            "parent_order": 11,
            "config": config_dict,
            **event_extra,
        }
        resp = handler_mod._handle_setup(event)
        return resp, store

    @staticmethod
    def _template_chunk_count(store, worker_grid):
        """Number of chunks in the emitted cell-resolution array."""
        group = open_group(store, path=str(worker_grid.child_order), mode="r")
        cell_arr = group["cell_ids"]
        return cell_arr.shape[0] // cell_arr.chunks[0]

    def test_setup_template_chunked_at_chunk_inner(self, handler_mod, monkeypatch):
        # The config sets parent_order 11, chunk_inner 13, child_order 19, so the
        # template must be chunked at order 13 (12*4^13 chunks), matching what the
        # worker grid writes -- NOT the order-11 (12*4^11) grid the old code emitted.
        # This is the exact #99 regression: setup dropping chunk_inner.
        cfg = default_config("atl03_tdigest_healpix")
        resp, store = self._setup(handler_mod, monkeypatch, asdict(cfg))
        assert resp["statusCode"] == 200, json.loads(resp["body"])

        worker_grid = from_config(cfg)
        n_chunks = self._template_chunk_count(store, worker_grid)
        assert (n_chunks,) == worker_grid.chunk_grid_shape
        assert n_chunks == HEALPIX_BASE_CELLS * 4**13

    def test_setup_template_k1_chunked_at_parent_order(self, handler_mod, monkeypatch):
        # K==1 (chunk_inner unset): chunk_order == parent_order, so the template is
        # chunked at parent_order (12*4^6) -- still matching the worker grid. Guards
        # the unset-chunk_inner path the fix must leave byte-identical.
        cfg = default_config("atl06")  # parent_order 6, child_order 12, no chunk_inner
        resp, store = self._setup(handler_mod, monkeypatch, asdict(cfg), parent_order=6)
        assert resp["statusCode"] == 200, json.loads(resp["body"])

        worker_grid = from_config(cfg, parent_order=6)
        n_chunks = self._template_chunk_count(store, worker_grid)
        assert (n_chunks,) == worker_grid.chunk_grid_shape
        assert n_chunks == HEALPIX_BASE_CELLS * 4**6

    def test_setup_dense_layout_threads_populated_shards(self, handler_mod, monkeypatch):
        # n_parent_cells signals the (deprecated) dense layout; the count must thread
        # through as populated_shards so the template is sized to the populated shards
        # (chunk count == n_parent_cells), not the full sphere. Covers the
        # populated_shards branch the fix routes through from_config.
        cfg = default_config("atl06")
        cfg.output["grid"]["layout"] = "dense"
        cfg_dict = asdict(cfg)
        with pytest.warns(DeprecationWarning, match="dense is deprecated"):
            resp, store = self._setup(
                handler_mod, monkeypatch, cfg_dict, parent_order=6, n_parent_cells=5
            )
        assert resp["statusCode"] == 200, json.loads(resp["body"])

        with pytest.warns(DeprecationWarning, match="dense is deprecated"):
            worker_grid = from_config(cfg, parent_order=6, populated_shards=list(range(5)))
        n_chunks = self._template_chunk_count(store, worker_grid)
        assert (n_chunks,) == worker_grid.chunk_grid_shape
        assert n_chunks == 5
