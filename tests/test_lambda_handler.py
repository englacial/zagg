"""Tests for the AWS Lambda handler's process-mode event contract (#24).

The handler lives under ``deployment/aws/`` (not an importable package module),
so it is loaded by path. These tests exercise the grid-neutral event schema:
the shard identifier is ``shard_key`` (not ``parent_morton``), and the
HEALPix-specific ``child_order`` is required for HEALPix runs but optional for
other grids.
"""

import importlib.util
import json
import time
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


@pytest.fixture(autouse=True)
def _no_real_recycle(handler_mod, monkeypatch):
    """Module-wide self-recycle hygiene (issue #171, review finding PR #172).

    A dev shell exporting ``ZAGG_RECYCLE_*`` must never let a mirror-path test
    reach the REAL ``os._exit`` -- that kills pytest mid-suite with a
    success-looking exit code 0 (the module-scoped ``handler_mod`` keeps
    ``_INVOCATIONS_SERVED`` climbing across tests, so the generation cap WILL
    fire). Scrub the knobs and make any unexpected exit LOUD; ``TestSelfRecycle``
    re-enables the knobs and replaces the seam per test.
    """

    def _unexpected_exit(code):
        raise AssertionError(f"unexpected self-recycle _exit({code}) in tests")

    monkeypatch.delenv("ZAGG_RECYCLE_RSS_MB", raising=False)
    monkeypatch.delenv("ZAGG_RECYCLE_MAX_INVOCATIONS", raising=False)
    monkeypatch.setattr(handler_mod, "_exit", _unexpected_exit)


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
            captured["aoi_payload"] = kwargs.get("aoi_payload", "OMITTED")
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
        # deployed worker selects the named carrier (the key still wins, #132 wire (A)).
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event["handoff"] = "pandas"
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["handoff"] == "pandas"

    def test_absent_handoff_key_derives_from_config(self, handler_mod, monkeypatch):
        # issue #132 wire (A): an absent handoff key means "derive from the
        # forwarded config". The default atl06 config sets no aggregation.handoff,
        # so get_handoff -> the "arrow" default (the pandas->arrow flip is on record).
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        assert "handoff" not in event
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["handoff"] == "arrow"

    def test_absent_handoff_key_honors_config_pandas(self, handler_mod, monkeypatch):
        # issue #132: a config declaring aggregation.handoff="pandas" (e.g. a
        # nullable-source pipeline) is honored on the absent-key path.
        config_dict = _healpix_config_dict()
        config_dict["aggregation"]["handoff"] = "pandas"
        event = _base_event(config_dict)
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

    def test_aoi_payload_threaded_to_process_shard(self, handler_mod, monkeypatch):
        # issue #101: a flag-on Lambda event carries an "aoi_payload"; the handler
        # threads it into process_shard so the worker fills the aoi_mask column.
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event["aoi_payload"] = [1, 2, 3]
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["aoi_payload"] == [1, 2, 3]

    def test_no_aoi_payload_passes_none(self, handler_mod, monkeypatch):
        # Flag off (no "aoi_payload" key): the handler passes aoi_payload=None,
        # which equals process_shard's default, so the worker call is unchanged
        # and the aoi_mask column is not allocated (issue #101).
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        assert "aoi_payload" not in event
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["aoi_payload"] is None

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

    def test_body_reports_cpu_seconds(self, handler_mod, monkeypatch):
        """Issue #180 phase 3: every invocation stamps ``cpu_seconds`` (this
        invocation's user+sys CPU across all threads) next to
        ``max_memory_mb`` so the orchestrator can compute utilization =
        cpu_seconds / duration_s per worker without CloudWatch access."""
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp, _ = self._run(handler_mod, monkeypatch, event)
        body = json.loads(resp["body"])
        assert isinstance(body["cpu_seconds"], float)
        assert body["cpu_seconds"] >= 0.0

    def test_cpu_seconds_is_per_invocation_delta(self, handler_mod, monkeypatch):
        """``os.times()`` is process-cumulative (a warm container keeps
        accruing), so the stamped value must be the entry->stamp delta of
        user+sys, never the raw counter."""
        import os as _os

        calls = {"n": 0}

        def fake_times():
            calls["n"] += 1
            if calls["n"] == 1:  # the handler-entry snapshot
                return _os.times_result((10.0, 5.0, 0.0, 0.0, 100.0))
            return _os.times_result((12.5, 5.5, 0.0, 0.0, 103.0))

        monkeypatch.setattr(handler_mod.os, "times", fake_times)
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp, _ = self._run(handler_mod, monkeypatch, event)
        body = json.loads(resp["body"])
        assert body["cpu_seconds"] == pytest.approx(3.0)


class TestReclaimMemory:
    """Issue #139: each invocation hands freed heap back to the OS at teardown so
    a warm-container reuse starts near baseline instead of the prior invocation's
    RSS high-water. The reclaim runs once per invocation in a ``finally`` (both
    the success and unhandled-exception paths)."""

    def test_reclaim_memory_never_raises(self, handler_mod):
        # The helper is guarded: on a non-glibc dev host (no libc.so.6) or a libc
        # lacking malloc_trim it must be a silent no-op, never an exception.
        handler_mod._reclaim_memory()  # would raise on the dev platform if unguarded

    def test_success_path_calls_reclaim_once(self, handler_mod, monkeypatch):
        import zagg.grids as grids
        import zagg.processing as processing

        calls = {"n": 0}
        monkeypatch.setattr(
            handler_mod, "_reclaim_memory", lambda: calls.__setitem__("n", calls["n"] + 1)
        )

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
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

        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        assert calls["n"] == 1

    def test_reclaim_runs_on_exception_path(self, handler_mod, monkeypatch):
        # An unhandled error mid-invocation must still trigger the teardown reclaim
        # (it lives in a ``finally``), so a crashed warm invocation doesn't strand
        # its high-water for the next one.
        import zagg.grids as grids
        import zagg.processing as processing

        calls = {"n": 0}
        monkeypatch.setattr(
            handler_mod, "_reclaim_memory", lambda: calls.__setitem__("n", calls["n"] + 1)
        )

        def boom(*a, **k):
            raise RuntimeError("kaboom")

        monkeypatch.setattr(processing, "process_shard", boom)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: MagicMock())

        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 500
        assert calls["n"] == 1


class TestProcessEventWriteLoop:
    """Issue #82 phase 7: the handler drives ``process_shard`` with a
    ``chunk_results`` sink and writes each chunk's dense region (at its own
    block_index) plus its ragged vlen payloads at the same block (issue #209) —
    the same K>1 write loop the local runner runs. These mock the writers/store
    to assert the loop's wiring (sink passed, per-chunk block index used,
    ragged persisted) without a real Zarr store."""

    def _patch(self, handler_mod, monkeypatch, chunks):
        """Patch process_shard to fill ``chunk_results`` with ``chunks`` and capture
        every dense/ragged write. Returns the capture dict."""
        import zagg.grids as grids
        import zagg.processing as processing

        cap = {"dense": [], "ragged": []}

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            # Non-sharded path streams via write_chunk (issue #91); sharded path
            # accumulates via chunk_results. Mirror process_shard's own dispatch.
            wc = kwargs.get("write_chunk")
            if wc is not None:
                for block_index, carrier, ragged in chunks:
                    wc(block_index, carrier, ragged)
            else:
                kwargs["chunk_results"].extend(chunks)
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

        def fake_write_ragged(ragged, st, *, grid, chunk_idx):
            cap["ragged"].append((chunk_idx, ragged))

        # grid stub: a 1-D companion (single-element block index).
        # ``chunks_per_shard`` fixes K (issue #91).
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        grid_stub.chunks_per_shard = len(chunks)
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
        each chunk's ragged lands at the same per-chunk block (issue #209)."""
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
        # ... and one ragged write per chunk at the SAME block index.
        assert [k for k, _r in cap["ragged"]] == [(0,), (1,), (2,)]

    def test_non_sharded_streams_via_write_chunk_not_chunk_results(self, handler_mod, monkeypatch):
        """Issue #91: the non-sharded handler hands ``process_shard`` a ``write_chunk``
        callback (streaming) and no ``chunk_results`` sink, so output memory is never
        accumulated. The callback writes each chunk as it arrives."""
        import zagg.grids as grids
        import zagg.processing as processing

        seen = {}

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            seen["write_chunk"] = kwargs.get("write_chunk")
            seen["chunk_results"] = kwargs.get("chunk_results")
            # Stream two chunks through the callback.
            kwargs["write_chunk"]((0,), pd.DataFrame(), {})
            kwargs["write_chunk"]((1,), pd.DataFrame(), {})
            return pd.DataFrame(), {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }

        store = MagicMock()
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        grid_stub.chunks_per_shard = 2
        grid_stub.sharded = False
        written = []
        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        # The template check runs via ``open_group`` (issue #118); stub it to succeed.
        monkeypatch.setattr(handler_mod, "open_group", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            handler_mod,
            "write_dataframe_to_zarr",
            lambda c, st, *, grid, chunk_idx: written.append(chunk_idx),
        )
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", lambda *a, **k: None)

        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        assert callable(seen["write_chunk"])  # streaming seam wired
        assert seen["chunk_results"] is None  # no accumulation sink
        assert written == [(0,), (1,)]  # each chunk written as it streamed

    def test_k_eq_1_ragged_written_at_chunk_block(self, handler_mod, monkeypatch):
        """K=1: the lone chunk's ragged payloads are persisted at the chunk's own
        block index — the same block the dense write uses (issue #209)."""
        chunks = [((0,), pd.DataFrame(), {"h_tdigest": ([], [])})]
        cap = self._patch(handler_mod, monkeypatch, chunks)
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200
        assert cap["dense"] == [(0,)]
        assert [k for k, _r in cap["ragged"]] == [(0,)]

    def test_streaming_later_chunk_write_failure_partial_writes_and_500(
        self, handler_mod, monkeypatch
    ):
        """Issue #91: when a LATER streamed chunk's write raises after an earlier one
        already wrote, the failure is recorded → 500, the earlier write stands, and
        remaining chunks are skipped (the write_error short-circuit)."""
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            chunks = [
                ((0,), pd.DataFrame(), {}),
                ((1,), pd.DataFrame(), {}),
                ((2,), pd.DataFrame(), {}),
            ]
            for block_index, carrier, ragged in chunks:
                kwargs["write_chunk"](block_index, carrier, ragged)
            return pd.DataFrame(), {
                "shard_key": shard_key,
                "cells_with_data": 1,
                "total_obs": 1,
                "duration_s": 0.0,
                "error": None,
            }

        store = MagicMock()
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)
        grid_stub.chunks_per_shard = 3
        grid_stub.sharded = False

        written = []

        def fake_write_dense(c, st, *, grid, chunk_idx):
            if chunk_idx == (1,):  # the SECOND chunk's write blows up
                raise RuntimeError("boom")
            written.append(chunk_idx)

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
        # The template check runs via ``open_group`` (issue #118); stub it to succeed.
        monkeypatch.setattr(handler_mod, "open_group", lambda *a, **k: MagicMock())
        monkeypatch.setattr(handler_mod, "write_dataframe_to_zarr", fake_write_dense)
        monkeypatch.setattr(handler_mod, "write_ragged_to_zarr", lambda *a, **k: None)

        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 500
        assert "Failed to write zarr" in json.loads(resp["body"])["error"]
        # Chunk 0 wrote; chunk 1 raised; chunk 2 was skipped (short-circuit).
        assert written == [(0,)]


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
            # Mirror process_shard's dispatch: stream via write_chunk when given,
            # else accumulate into chunk_results (issue #91).
            wc = kwargs.get("write_chunk")
            if wc is not None:
                for block_index, carrier, ragged in chunks:
                    wc(block_index, carrier, ragged)
            else:
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
        # K fixed by the grid (issue #91); these profile cases are all K==1 (≤1 chunk).
        grid_stub.chunks_per_shard = max(len(chunks), 1)
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
            # Non-sharded path streams via write_chunk (issue #91); sharded path
            # accumulates via chunk_results. Mirror process_shard's own dispatch so
            # the template guard (now lazy, inside the write callback) still runs.
            wc = kwargs.get("write_chunk")
            if wc is not None:
                wc((0,), pd.DataFrame(), {})
            else:
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
            # Mirror process_shard's dispatch: stream via write_chunk when given
            # (non-sharded, issue #91), else accumulate into chunk_results, so the
            # real ``open_group`` guard inside the write callback is exercised.
            wc = kwargs.get("write_chunk")
            if wc is not None:
                wc((0,), pd.DataFrame(), {})
            else:
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


class TestProcessHive:
    """Issue #199 phase 3: under ``output.store_layout: hive`` the worker owns
    its WHOLE leaf — it derives the leaf path from ``shard_key`` + the event
    config's orders, emits its own leaf template, writes its data, and stamps
    completion as its FINAL PUT — through ``zagg.hive.process_and_write_hive``,
    the same code path the local dispatcher runs."""

    # Order-6 southern shard; decimal morton string -4211322.
    _WORD = 11827859996358475782

    @staticmethod
    def _hive_config_dict():
        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        # A declared ragged field, so the leaf template carries its vlen-bytes
        # array (issue #209) and the fakes can stream ragged payloads for it.
        cfg.aggregation["variables"]["h"] = {
            "function": "np.sort",
            "source": "h_li",
            "kind": "ragged",
            "inner_shape": [1],
            "dtype": "float32",
            "fill_value": 0,
        }
        return asdict(cfg)

    def _event(self, tmp_path):
        ev = _base_event(self._hive_config_dict())
        ev["shard_key"] = self._WORD
        ev["child_order"] = 12
        ev["store_path"] = str(tmp_path / "hive-out")
        return ev

    @staticmethod
    def _grid():
        from zagg.config import load_config_from_dict

        cfg = load_config_from_dict(TestProcessHive._hive_config_dict())
        return from_config(cfg)

    @staticmethod
    def _carrier(grid, shard):
        import numpy as np

        from zagg.config import get_agg_fields, get_data_vars, get_output_signature

        coords = grid.chunk_coords(shard)
        n = len(coords["cell_ids"])
        agg = get_agg_fields(grid.config)
        df = pd.DataFrame(
            {
                var: np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
                for var in get_data_vars(grid.config)
                if get_output_signature(agg[var])["kind"] != "ragged"
            }
        )
        for name, vals in coords.items():
            df[name] = vals
        return df

    def _streaming_fake(self, grid, ragged=None, die=False):
        def fake(g, shard_key, urls, **kwargs):
            carrier = self._carrier(grid, shard_key)
            kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, ragged or {})
            if die:
                raise RuntimeError("worker died mid-shard")
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 5,
                "total_obs": 7,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
            }

        return fake

    def test_hive_event_writes_leaf_data_and_stamp(self, handler_mod, monkeypatch, tmp_path):
        import numpy as np
        import zarr
        from mortie import MortonIndexArray

        import zagg.processing as processing
        from zagg import hive
        from zagg.store import open_store

        grid = self._grid()
        ragged = {"h": ([np.array([1.0, 2.0])], [0])}
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid, ragged))
        event = self._event(tmp_path)
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200, resp["body"]
        body = json.loads(resp["body"])
        assert body["shard_key"] == self._WORD

        # The leaf sits at EXACTLY mortie's hive_path under the store root.
        words = MortonIndexArray.from_words(np.asarray([self._WORD], dtype="uint64"))
        expected = words.hive_path(root=event["store_path"])[0]
        assert hive.shard_leaf_path(event["store_path"], self._WORD) == expected
        leaf_store = open_store(expected)

        # Dense data landed at the leaf-local block; the ragged payload sits in
        # its vlen-bytes array at the cell position (issue #209), ONE object.
        grp = zarr.open_group(leaf_store, path=grid.group_path, mode="r", zarr_format=3)
        np.testing.assert_array_equal(
            np.asarray(grp["cell_ids"][:]),
            np.asarray(grid.chunk_coords(self._WORD)["cell_ids"]),
        )
        ragged_arr = zarr.open_array(leaf_store, path=f"{grid.group_path}/h", mode="r")
        np.testing.assert_array_equal(np.frombuffer(ragged_arr[0:1][0], "<f4"), [1.0, 2.0])
        import os

        chunk_dir = os.path.join(expected, grid.group_path, "h", "c")
        assert sum(len(files) for _d, _s, files in os.walk(chunk_dir)) == 1

        # The commit stamp is present and carries the worker's counters (D4).
        stamp = hive.read_commit(leaf_store)
        assert stamp["complete"] is True
        assert stamp["cells_with_data"] == 5
        assert stamp["granule_count"] == 1

    def test_hive_sharded_event_writes_single_object_leaf(self, handler_mod, monkeypatch, tmp_path):
        """Issue #236 through the LAMBDA dispatcher: a sharded K>1 hive event
        takes the accumulate switch inside the shared ``process_and_write_hive``
        (``chunk_results`` sink, no ``write_chunk``), so every leaf array lands
        as ONE ShardingCodec object and the leaf is stamped complete."""
        import os

        import numpy as np
        import zarr

        import zagg.processing as processing
        from zagg import hive
        from zagg.config import get_data_vars, load_config_from_dict
        from zagg.store import open_store

        cfg_dict = self._hive_config_dict()
        cfg_dict["output"]["grid"]["chunk_inner"] = 8
        grid = from_config(load_config_from_dict(cfg_dict))
        # Hive defaults sharded now, same as flat (issue #236).
        assert grid.sharded is True and grid.chunks_per_shard == 16

        # Same sharded accumulate contract as test_hive's ``_sharded_accumulate_fake``
        # (sink filled, no ``write_chunk``); kept inline because this variant fills
        # EVERY chunk (no every-4th-empty) so the cell_ids parity assertion below
        # covers the whole leaf, and uses this module's chunk-level ``_carrier_of``.
        def fake(g, shard_key, urls, **kwargs):
            sink = kwargs.get("chunk_results")
            assert sink is not None and kwargs.get("write_chunk") is None
            shard_block = grid.block_index(int(shard_key))[0]
            for block, children in grid.iter_chunks(int(shard_key)):
                local = int(block[0]) - shard_block * grid.chunks_per_shard
                ragged = {"h": ([np.array([1.0, 2.0])], [0])} if local == 0 else {}
                sink.append((block, self._carrier_of(grid, children), ragged))
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 5,
                "total_obs": 7,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
            }

        monkeypatch.setattr(processing, "process_shard", fake)
        event = _base_event(cfg_dict)
        event["shard_key"] = self._WORD
        event["child_order"] = 12
        event["store_path"] = str(tmp_path / "hive-out")
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200, resp["body"]

        leaf = hive.shard_leaf_path(event["store_path"], self._WORD)
        leaf_store = open_store(leaf)
        cfg = load_config_from_dict(cfg_dict)
        for name in ("morton", "cell_ids", "h", *get_data_vars(cfg)):
            chunk_dir = os.path.join(leaf, grid.group_path, name, "c")
            n_objects = sum(len(files) for _d, _s, files in os.walk(chunk_dir))
            assert n_objects == 1, name
        # Whole-leaf contents readable after a fresh open; stamp is present.
        grp = zarr.open_group(leaf_store, path=grid.group_path, mode="r", zarr_format=3)
        np.testing.assert_array_equal(
            np.asarray(grp["cell_ids"][:]),
            np.asarray(grid.chunk_coords(self._WORD)["cell_ids"]),
        )
        assert hive.read_commit(leaf_store)["complete"] is True

    @staticmethod
    def _carrier_of(grid, children):
        """Per-chunk carrier (K>1): coords + zeroed dense vars for one chunk's
        ``children`` (the chunk-level analog of ``_carrier``)."""
        import numpy as np

        from zagg.config import get_agg_fields, get_data_vars, get_output_signature

        coords = grid.coords_of(children)
        n = len(children)
        agg = get_agg_fields(grid.config)
        # count is 1-based so no slab is all-fill (an all-fill slab is omitted
        # from the store entirely, which is correct but not what this asserts).
        df = pd.DataFrame(
            {
                var: (
                    np.arange(1, n + 1, dtype=np.int32)
                    if var == "count"
                    else np.zeros(n, dtype=np.float32)
                )
                for var in get_data_vars(grid.config)
                if get_output_signature(agg[var])["kind"] != "ragged"
            }
        )
        for name, vals in coords.items():
            df[name] = vals
        return df

    def test_hive_worker_death_leaves_debris_then_retry_cleans(
        self, handler_mod, monkeypatch, tmp_path
    ):
        import os

        import numpy as np

        import zagg.processing as processing
        from zagg import hive
        from zagg.store import open_store

        grid = self._grid()
        event = self._event(tmp_path)
        leaf = hive.shard_leaf_path(event["store_path"], self._WORD)

        # Torn worker: writes a chunk, then dies (its accumulated ragged never
        # lands — the leaf ragged write is a single post-stream object, issue
        # #209). The handler surfaces a 500 envelope; the leaf prefix exists
        # but is UNSTAMPED.
        torn = self._streaming_fake(grid, ragged={"h": ([np.array([1.0])], [0])}, die=True)
        monkeypatch.setattr(processing, "process_shard", torn)
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 500
        assert "died mid-shard" in resp["body"]
        assert os.path.exists(leaf)
        assert hive.read_commit(open_store(leaf)) is None  # debris
        # Plant a stray object in the debris the retry will NOT rewrite, so the
        # wholesale-overwrite claim stays pinned (a metadata-only re-template
        # would leave it behind).
        stale = os.path.join(leaf, grid.group_path, "stale-debris")
        with open(stale, "w") as fh:
            fh.write("torn attempt")

        # Retry (no ragged this time): the leaf is overwritten WHOLESALE — the
        # planted debris is gone — and stamped at the end.
        monkeypatch.setattr(processing, "process_shard", self._streaming_fake(grid))
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200, resp["body"]
        assert hive.read_commit(open_store(leaf))["complete"] is True
        assert not os.path.exists(stale)

    def test_hive_no_data_shard_leaves_no_prefix(self, handler_mod, monkeypatch, tmp_path):
        import os

        import zagg.processing as processing
        from zagg import hive

        def fake(g, shard_key, urls, **kwargs):
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 0,
                "total_obs": 0,
                "granule_count": 1,
                "files_processed": 0,
                "duration_s": 0.0,
                "error": "No data after filtering",
            }

        monkeypatch.setattr(processing, "process_shard", fake)
        event = self._event(tmp_path)
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 500  # error envelope, as on flat
        assert not os.path.exists(hive.shard_leaf_path(event["store_path"], self._WORD))


class TestProcessHiveWindowed:
    """Issue #246: a windowed hive event threads ``window`` through the shared
    ``process_and_write_hive`` path — windowed leaf, filter injection, ISO
    ``time_range`` in the response body — and the setup invoke declares the
    ``morton-hive/2`` manifest from the same forwarded config."""

    @staticmethod
    def _windowed_config_dict():
        cfg_dict = TestProcessHive._hive_config_dict()
        cfg_dict["data_source"]["variables"]["delta_time"] = "/{group}/land_ice_segments/delta_time"
        cfg_dict["output"]["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00Z",
            "scale": "gps",
        }
        return cfg_dict

    def test_windowed_event_writes_windowed_leaf_with_time_range(
        self, handler_mod, monkeypatch, tmp_path
    ):
        import zagg.processing as processing
        from zagg import hive
        from zagg.config import load_config_from_dict
        from zagg.store import open_store

        cfg_dict = self._windowed_config_dict()
        grid = from_config(load_config_from_dict(cfg_dict))
        seen = {}

        def fake(g, shard_key, urls, **kwargs):
            seen["filters"] = kwargs["config"].data_source["filters"]
            seen["time_range_of"] = kwargs.get("time_range_of")
            carrier = TestProcessHive._carrier(grid, shard_key)
            kwargs["write_chunk"](grid.block_index(int(shard_key)), carrier, {})
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 5,
                "total_obs": 7,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
                # Dataset units (GPS s since 2018-01-01): days 425 and 695.
                "time_range": [425 * 86400.0, 695 * 86400.0],
            }

        monkeypatch.setattr(processing, "process_shard", fake)
        event = _base_event(cfg_dict)
        event["shard_key"] = TestProcessHive._WORD
        event["child_order"] = 12
        event["store_path"] = str(tmp_path / "hive-out")
        event["window"] = {"label": "2019", "start": 365 * 86400.0, "end": 730 * 86400.0}
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200, resp["body"]

        # The injected window filter pair reached the worker's config.
        assert [f["op"] for f in seen["filters"][-2:]] == ["ge", "lt"]
        assert seen["time_range_of"] == "delta_time"
        # The leaf is the WINDOWED name; its stamp carries the D15 truth.
        leaf = hive.shard_leaf_path(event["store_path"], TestProcessHive._WORD, window="2019")
        stamp = hive.read_commit(open_store(leaf))
        assert stamp["window"] == "2019"
        assert stamp["time_range"] == [
            "2019-03-02T00:00:00+00:00",
            "2019-11-27T00:00:00+00:00",
        ]
        # The response body mirrors the ISO strings for the dispatcher's
        # root-summary union.
        assert json.loads(resp["body"])["time_range"] == stamp["time_range"]

    def test_unwindowed_event_stays_bare(self, handler_mod, monkeypatch, tmp_path):
        import zagg.processing as processing
        from zagg import hive
        from zagg.store import open_store

        grid = TestProcessHive._grid()
        monkeypatch.setattr(
            processing,
            "process_shard",
            TestProcessHive()._streaming_fake(grid),
        )
        event = TestProcessHive()._event(tmp_path)
        resp = handler_mod._handle_process(event, _context())
        assert resp["statusCode"] == 200, resp["body"]
        stamp = hive.read_commit(
            open_store(hive.shard_leaf_path(event["store_path"], TestProcessHive._WORD))
        )
        assert stamp["spec"] == "morton-hive/1"
        assert "window" not in stamp and "time_range" not in stamp
        assert "time_range" not in json.loads(resp["body"])

    def test_setup_declares_v2_manifest(self, handler_mod, tmp_path):
        from zagg import hive

        event = {
            "mode": "setup",
            "store_path": str(tmp_path / "hive-out"),
            "parent_order": 6,
            "overwrite": False,
            "config": self._windowed_config_dict(),
            "dataset": {"short_name": "ATL06", "version": "007"},
        }
        resp = handler_mod._handle_setup(event)
        assert resp["statusCode"] == 200, resp["body"]
        manifest = hive.read_manifest(event["store_path"])
        assert manifest["spec"] == "morton-hive/2"
        assert manifest["temporal"]["schedule"] == "yearly"
        assert manifest["temporal"]["time_field"] == "delta_time"


class TestSetupHive:
    """Issue #199 phase 3: for a hive config, setup mode writes ONLY the
    ``morton_hive.json`` manifest — no global zarr template (D5) — with the
    same frozen-key resume/overwrite semantics as the local path."""

    def _event(self, tmp_path, config_dict, **extra):
        return {
            "mode": "setup",
            "store_path": str(tmp_path / "hive-out"),
            "parent_order": 6,
            "overwrite": False,
            "config": config_dict,
            "dataset": {"short_name": "ATL06", "version": "007"},
            **extra,
        }

    def test_setup_writes_manifest_only(self, handler_mod, tmp_path):
        import os

        from zagg import hive

        event = self._event(tmp_path, TestProcessHive._hive_config_dict())
        resp = handler_mod._handle_setup(event)
        assert resp["statusCode"] == 200, resp["body"]

        manifest = hive.read_manifest(event["store_path"])
        assert manifest["spec"] == "morton-hive/1"
        assert manifest["dataset"] == {"short_name": "ATL06", "version": "007"}
        assert manifest["shard_order"] == 6
        assert manifest["cell_order"] == 12
        # The manifest is the ONLY object: no template arrays, no zarr.json.
        assert sorted(os.listdir(event["store_path"])) == [hive.MANIFEST_NAME]

    def test_setup_frozen_key_mismatch_is_500_clear_root(self, handler_mod, tmp_path):
        cfg = TestProcessHive._hive_config_dict()
        assert handler_mod._handle_setup(self._event(tmp_path, cfg))["statusCode"] == 200
        # A re-setup with different orders must fail with the pointed remedy,
        # matching the local dispatcher's ensure_manifest semantics.
        other = TestProcessHive._hive_config_dict()
        other["output"]["grid"]["parent_order"] = 5
        resp = handler_mod._handle_setup(self._event(tmp_path, other))
        assert resp["statusCode"] == 500
        assert "clear the store root" in json.loads(resp["body"])["error"]

    def test_setup_rerun_with_matching_config_resumes(self, handler_mod, tmp_path):
        cfg = TestProcessHive._hive_config_dict()
        assert handler_mod._handle_setup(self._event(tmp_path, cfg))["statusCode"] == 200
        assert handler_mod._handle_setup(self._event(tmp_path, cfg))["statusCode"] == 200


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


def _temporal_config_dict():
    return {
        "pipeline": {"type": "temporal"},
        "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
        "aggregation": {
            "variables": {
                "max_t2m": {
                    "variable": "T2M",
                    "collection": "merra2",
                    "spatial_func": "max",
                    "temporal_reducer": "max",
                    "mask": "full",
                }
            }
        },
        "output": {"format": "parquet", "store": "s3://out/events.parquet"},
    }


def _temporal_inputs():
    """One synthetic event mask + merra2 collection + cell_areas (max-T2M=5)."""
    xr = pytest.importorskip("xarray")
    import numpy as np

    lat = np.array([-70.0, -69.5])
    lon = np.array([0.0, 0.5])
    time = np.array(["2020-01-01T00", "2020-01-01T03"], dtype="datetime64[ns]")
    coords = {"time": time, "lat": lat, "lon": lon}
    event_mask = xr.DataArray(np.ones((2, 2, 2)), dims=["time", "lat", "lon"], coords=coords)
    temp = xr.DataArray(
        np.stack([np.full((2, 2), 1.0), np.full((2, 2), 5.0)]),
        dims=["time", "lat", "lon"],
        coords=coords,
    )
    collections = {"merra2": xr.Dataset({"T2M": temp})}
    areas = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
    return event_mask, collections, {"cell_areas": areas}


def _temporal_event(**extra):
    return {
        "mode": "process_event",
        "event_key": "storm1",
        "event_mask_uri": "s3://b/mask.nc",
        "collection_uris": {"merra2": "s3://b/merra2.zarr"},
        "static_uris": {"cell_areas": "s3://b/areas.nc"},
        "store_path": "s3://out/events.parquet",
        "config": _temporal_config_dict(),
        "s3_credentials": _CREDS,
        **extra,
    }


class TestProcessEventModeGate:
    def test_missing_event_key_rejected(self, handler_mod):
        event = _temporal_event()
        del event["event_key"]
        resp = handler_mod._handle_process_event(event)
        assert resp["statusCode"] == 400
        assert "event_key" in json.loads(resp["body"])["error"]

    def test_missing_store_path_rejected(self, handler_mod):
        event = _temporal_event()
        del event["store_path"]
        resp = handler_mod._handle_process_event(event)
        assert resp["statusCode"] == 400
        assert "store_path" in json.loads(resp["body"])["error"]


class TestProcessEventMode:
    def _patch(self, handler_mod, monkeypatch):
        """Stub the S3 readers + tabular writer; run the real process_event."""
        import zagg.output as output
        import zagg.temporal as temporal

        event_mask, collections, static = _temporal_inputs()
        captured = {}

        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: event_mask)
        monkeypatch.setattr(temporal, "read_temporal_inputs", lambda *a, **k: (collections, static))

        def _fake_write_tabular(rows, store_path, **kwargs):
            captured["rows"] = rows
            captured["store_path"] = store_path
            captured["kwargs"] = kwargs
            return store_path

        monkeypatch.setattr(output, "write_tabular", _fake_write_tabular)
        return captured

    def test_dispatch_runs_process_event_and_writes_row(self, handler_mod, monkeypatch):
        captured = self._patch(handler_mod, monkeypatch)
        resp = handler_mod._handle_process_event(_temporal_event())
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200, body
        assert body["event_key"] == "storm1"
        assert body["timesteps_processed"] == 2
        assert body["output_path"] == "s3://out/events.parquet"
        # one flattened row carrying the event_key + max-T2M result
        (row,) = captured["rows"]
        assert row["event_key"] == "storm1"
        assert row["results"]["max_t2m"] == pytest.approx(5.0)

    def test_lambda_handler_routes_process_event_mode(self, handler_mod, monkeypatch):
        # the top-level dispatcher routes mode="process_event" here.
        self._patch(handler_mod, monkeypatch)
        resp = handler_mod.lambda_handler(_temporal_event(), _context())
        assert resp["statusCode"] == 200, json.loads(resp["body"])

    def test_output_credentials_forwarded_to_writer(self, handler_mod, monkeypatch):
        captured = self._patch(handler_mod, monkeypatch)
        out_creds = {"accessKeyId": "w", "secretAccessKey": "x", "region": "eu-west-1"}
        handler_mod._handle_process_event(_temporal_event(output_credentials=out_creds))
        assert captured["kwargs"]["credentials"] == out_creds
        assert captured["kwargs"]["region"] == "eu-west-1"

    def test_exception_returns_500(self, handler_mod, monkeypatch):
        import zagg.temporal as temporal

        def _boom(*a, **k):
            raise RuntimeError("read failed")

        monkeypatch.setattr(temporal, "open_dataset", _boom)
        resp = handler_mod._handle_process_event(_temporal_event())
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 500
        assert body["event_key"] == "storm1"
        assert "read failed" in body["error"]

    def test_reader_resolved_through_registry(self, handler_mod, monkeypatch):
        # ``data_source.reader`` names a registered reader (issue #213 Phase 3);
        # the worker forwards the config's collection options to it.
        import zagg.temporal as temporal
        from zagg import registry as zagg_registry

        event_mask, collections, static = _temporal_inputs()
        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: event_mask)
        captured = {}

        def _custom(collection_uris, static_uris, **kwargs):
            captured["uris"] = collection_uris
            captured["options"] = kwargs.get("collection_options")
            return collections, static

        zagg_registry.register_reader("custom_reader", _custom, replace=True)
        try:
            cfg = _temporal_config_dict()
            cfg["data_source"]["reader"] = "custom_reader"
            cfg["data_source"]["collections"] = {"merra2": {"time_offset": "-30min"}}
            event = _temporal_event(config=cfg, return_results=True)
            del event["store_path"]
            resp = handler_mod._handle_process_event(event)
            body = json.loads(resp["body"])
            assert resp["statusCode"] == 200, body
            assert captured["uris"] == {"merra2": "s3://b/merra2.zarr"}
            assert captured["options"] == {"merra2": {"time_offset": "-30min"}}
        finally:
            zagg_registry.READERS._entries.pop("custom_reader", None)

    def test_unknown_reader_returns_500_naming_it(self, handler_mod, monkeypatch):
        self._patch(handler_mod, monkeypatch)
        cfg = _temporal_config_dict()
        cfg["data_source"]["reader"] = "not_a_reader"
        resp = handler_mod._handle_process_event(_temporal_event(config=cfg))
        assert resp["statusCode"] == 500
        assert "not_a_reader" in json.loads(resp["body"])["error"]

    def test_input_credentials_channel_routing(self, handler_mod, monkeypatch):
        # input_credentials covers the mask + statics; s3_credentials covers
        # only the source collections (issue #223).
        import zagg.output as output
        import zagg.temporal as temporal

        event_mask, collections, static = _temporal_inputs()
        captured = {}

        def _open(uri, **kwargs):
            captured["mask_kwargs"] = kwargs
            return event_mask

        def _read(collection_uris, static_uris, **kwargs):
            captured["reader_kwargs"] = kwargs
            return collections, static

        monkeypatch.setattr(temporal, "open_dataset", _open)
        monkeypatch.setattr(temporal, "read_temporal_inputs", _read)
        monkeypatch.setattr(output, "write_tabular", lambda rows, sp, **k: sp)

        event = _temporal_event(input_credentials="unsigned", return_results=True)
        del event["store_path"]
        resp = handler_mod._handle_process_event(event)
        assert json.loads(resp["body"])["ok"] is True
        assert captured["mask_kwargs"]["unsigned"] is True
        assert captured["mask_kwargs"]["credentials"] is None
        assert captured["reader_kwargs"]["credentials"] == _CREDS
        assert captured["reader_kwargs"]["input_credentials"] == "unsigned"
        # the mask's coordinates ride to the reader so granules subset+load to
        # the event extent (issue #225)
        import numpy as np

        lats, lons = captured["reader_kwargs"]["extent"]
        np.testing.assert_array_equal(lats, event_mask["lat"].values)
        np.testing.assert_array_equal(lons, event_mask["lon"].values)

    def test_bad_input_credentials_returns_500(self, handler_mod, monkeypatch):
        self._patch(handler_mod, monkeypatch)
        resp = handler_mod._handle_process_event(_temporal_event(input_credentials="anonymous"))
        assert resp["statusCode"] == 500
        assert "input_credentials" in json.loads(resp["body"])["error"]


class TestProcessEventReturnResults:
    """``return_results`` (issue #12, Phase 8): the fan-out driver's contract.

    The worker returns its flattened result values in the response body and
    skips the tabular write (the driver collects rows and writes the single
    object once), and ``store_path`` drops out of the required-parameter gate.
    ``lambda_handler`` also mirrors process_event envelopes to ``result_url``
    so the async (Event-invoke) fan-out has a pollable result."""

    def _patch(self, handler_mod, monkeypatch):
        import zagg.output as output
        import zagg.temporal as temporal

        event_mask, collections, static = _temporal_inputs()
        captured = {"writes": 0}

        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: event_mask)
        monkeypatch.setattr(temporal, "read_temporal_inputs", lambda *a, **k: (collections, static))

        def _fake_write_tabular(rows, store_path, **kwargs):
            captured["writes"] += 1
            return store_path

        monkeypatch.setattr(output, "write_tabular", _fake_write_tabular)
        return captured

    def test_returns_values_and_skips_write(self, handler_mod, monkeypatch):
        captured = self._patch(handler_mod, monkeypatch)
        event = _temporal_event(return_results=True)
        del event["store_path"]
        resp = handler_mod._handle_process_event(event)
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200, body
        assert body["results"]["max_t2m"] == pytest.approx(5.0)
        assert isinstance(body["results"]["max_t2m"], float)  # JSON-safe scalar
        assert body["output_path"] is None
        assert body["timesteps_processed"] == 2
        assert body["duration_s"] >= 0
        assert body["meta"]["n_specs"] == 1  # full meta rides back for the driver
        assert captured["writes"] == 0  # driver writes; worker must not

    def test_non_float_result_value_passes_through(self, handler_mod, monkeypatch):
        # A custom reducer may return a non-float scalar (e.g. a label); the
        # float cast must not turn that event into a 500 -- it passes through,
        # matching what the direct-write path hands write_tabular.
        import zagg.temporal as temporal

        event_mask, collections, static = _temporal_inputs()
        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: event_mask)
        monkeypatch.setattr(temporal, "read_temporal_inputs", lambda *a, **k: (collections, static))
        monkeypatch.setattr(
            temporal,
            "process_event",
            lambda *a, **k: ({"label": "landfall", "peak": 5.0}, {"timesteps_processed": 2}),
        )
        event = _temporal_event(return_results=True)
        del event["store_path"]
        resp = handler_mod._handle_process_event(event)
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200, body
        assert body["results"] == {"label": "landfall", "peak": 5.0}

    def test_store_path_still_required_without_flag(self, handler_mod):
        event = _temporal_event()
        del event["store_path"]
        resp = handler_mod._handle_process_event(event)
        assert resp["statusCode"] == 400
        assert "store_path" in json.loads(resp["body"])["error"]

    def test_gate_still_rejects_missing_mask_uri(self, handler_mod):
        event = _temporal_event(return_results=True)
        del event["event_mask_uri"]
        resp = handler_mod._handle_process_event(event)
        assert resp["statusCode"] == 400
        assert "event_mask_uri" in json.loads(resp["body"])["error"]

    def test_direct_invoke_body_has_no_results_key(self, handler_mod, monkeypatch):
        # Without the flag the existing single-invoke contract is unchanged.
        self._patch(handler_mod, monkeypatch)
        resp = handler_mod._handle_process_event(_temporal_event())
        assert "results" not in json.loads(resp["body"])

    def test_result_url_mirrors_process_event_envelope(self, handler_mod, monkeypatch, tmp_path):
        self._patch(handler_mod, monkeypatch)
        url = str(tmp_path / "status" / "storm1.json")
        event = _temporal_event(return_results=True, result_url=url)
        del event["store_path"]
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200
        written = json.loads(Path(url).read_text())
        assert written == resp
        assert json.loads(written["body"])["results"]["max_t2m"] == pytest.approx(5.0)

    def test_result_url_mirrors_500_envelope(self, handler_mod, monkeypatch, tmp_path):
        import zagg.temporal as temporal

        def _boom(*a, **k):
            raise RuntimeError("read failed")

        monkeypatch.setattr(temporal, "open_dataset", _boom)
        url = str(tmp_path / "status" / "storm1.json")
        resp = handler_mod.lambda_handler(
            _temporal_event(return_results=True, result_url=url), _context()
        )
        assert resp["statusCode"] == 500
        assert json.loads(Path(url).read_text()) == resp


class TestPeakRSSSampler:
    """Issue #141: per-invocation peak-RSS sampling. ``max_memory_mb`` must reflect
    the CURRENT invocation's peak (sampled ``VmRSS``), not the warm container's
    lifetime ``ru_maxrss`` high-water. Uses a controlled ``_read_vmrss_kib`` feed so
    the assertions don't depend on real RSS or thread timing."""

    @staticmethod
    def _feeder(values_kib):
        """A ``_read_vmrss_kib`` stub that yields ``values_kib`` in order then holds
        the last value (so late samples keep reading the tail, not None)."""
        it = iter(values_kib)
        last = [values_kib[-1]]

        def read():
            try:
                last[0] = next(it)
            except StopIteration:
                pass
            return last[0]

        return read

    def test_read_vmrss_kib_reads_current_rss(self, handler_mod):
        val = handler_mod._read_vmrss_kib()
        if val is None:
            pytest.skip("no /proc/self/status (non-Linux dev host)")
        assert isinstance(val, int) and val > 0

    def test_sampler_reports_peak_not_last(self, handler_mod, monkeypatch):
        # Feed 100 -> 300 -> 150 MB; the sampler must report the PEAK (300), not
        # the first or last sample.
        monkeypatch.setattr(
            handler_mod,
            "_read_vmrss_kib",
            self._feeder([100 * 1024, 300 * 1024, 150 * 1024]),
        )
        s = handler_mod._PeakRSSSampler(interval_s=0.001).start()
        time.sleep(0.05)
        s.stop()
        assert s.peak_mb == pytest.approx(300.0)

    def test_two_samplers_report_independent_peaks(self, handler_mod, monkeypatch):
        # The crux of #141: a light invocation after a heavy one on the same warm
        # process reads its OWN (smaller) peak -- unlike ru_maxrss, which would stay
        # stuck at the heavy high-water.
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", self._feeder([100 * 1024, 900 * 1024]))
        heavy = handler_mod._PeakRSSSampler(interval_s=0.001).start()
        time.sleep(0.03)
        heavy.stop()

        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", self._feeder([90 * 1024, 120 * 1024]))
        light = handler_mod._PeakRSSSampler(interval_s=0.001).start()
        time.sleep(0.03)
        light.stop()

        assert heavy.peak_mb == pytest.approx(900.0)
        assert light.peak_mb == pytest.approx(120.0)
        assert light.peak_mb < heavy.peak_mb  # the whole point of #141

    def test_sampler_unavailable_off_linux_returns_none(self, handler_mod, monkeypatch):
        # No /proc/self/status -> the sampler is a no-op and peak_mb is None, so the
        # caller falls back to ru_maxrss.
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)
        s = handler_mod._PeakRSSSampler().start()
        s.stop()
        assert s.peak_mb is None
        assert s._thread is None  # never spawned


class TestContainerTelemetry:
    """Issue #171 (detect-and-report): every per-unit envelope carries the
    sandbox's container telemetry -- cold/warm sentinel, invocations-served
    generation, start RSS, sandbox id, init timestamp -- stamped once per
    invocation at dispatcher entry, so repeat invocations on one warm process
    report generations 1, 2, ... The per-invocation *peak* stays the existing
    ``max_memory_mb`` (issue #141) -- telemetry adds the start-RSS ratchet
    signal without duplicating it."""

    @pytest.fixture(autouse=True)
    def _fresh_container(self, handler_mod, monkeypatch):
        # handler_mod is module-scoped, so the generation counter persists
        # across tests: reset it so each test starts on a "cold" sandbox.
        monkeypatch.setattr(handler_mod, "_INVOCATIONS_SERVED", 0)

    def _process_event(self, monkeypatch, **extra):
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
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
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event.update(extra)
        return event

    def test_warm_repeat_invocations_ratchet_the_generation(self, handler_mod, monkeypatch):
        # Two invocations in one process (a warm sandbox): the first is cold /
        # generation 1, the second warm / generation 2, same init timestamp.
        event = self._process_event(monkeypatch)
        first = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        second = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        assert first["container_cold"] is True and first["container_generation"] == 1
        assert second["container_cold"] is False and second["container_generation"] == 2
        assert first["container_init_ts"] == second["container_init_ts"]
        assert first["container_init_ts"] == handler_mod._CONTAINER_INIT_TS

    def test_rss_start_sampled_at_entry(self, handler_mod, monkeypatch):
        # The telemetry snapshot is the invocation's FIRST _read_vmrss_kib
        # call (dispatcher entry, before the #141 sampler even probes): feed
        # 512 MB to that call only, 2000 MB to every later one. A post-work
        # sample would report 2000 and corrupt the #169 ratchet signal --
        # start RSS must be what the sandbox retained BEFORE this
        # invocation's work (review finding, PR #172).
        calls = {"n": 0}

        def feed():
            calls["n"] += 1
            return (512 if calls["n"] == 1 else 2000) * 1024

        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", feed)
        event = self._process_event(monkeypatch)
        body = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        assert body["rss_start_mb"] == pytest.approx(512.0)
        assert calls["n"] > 1  # later reads happened (sampler), all post-entry

    def test_rss_start_none_off_linux(self, handler_mod, monkeypatch):
        # No /proc/self/status (dev host): rss_start_mb degrades to None, the
        # same fallback posture as the #141 sampler.
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)
        event = self._process_event(monkeypatch)
        body = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        assert body["rss_start_mb"] is None

    def test_sandbox_id_from_log_stream(self, handler_mod, monkeypatch):
        monkeypatch.setenv("AWS_LAMBDA_LOG_STREAM_NAME", "2026/07/06/[$LATEST]abc123")
        event = self._process_event(monkeypatch)
        body = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        assert body["sandbox_id"] == "2026/07/06/[$LATEST]abc123"

    def test_gate_failure_envelope_carries_telemetry(self, handler_mod, monkeypatch):
        # 400s carry telemetry too: the attach seam sits at the dispatcher, so
        # failures can be stratified by container state (e.g. an OOM'd gen-4).
        event = self._process_event(monkeypatch)
        del event["shard_key"]
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["container_cold"] is True and body["container_generation"] == 1

    def test_setup_counts_toward_generation_but_body_unchanged(self, handler_mod, monkeypatch):
        # A setup invoke warms the sandbox (generation ticks) but its body stays
        # byte-identical -- telemetry rides only in per-unit envelopes.
        monkeypatch.setattr(
            handler_mod, "_handle_setup", lambda event: {"statusCode": 200, "body": "{}"}
        )
        setup_resp = handler_mod.lambda_handler({"mode": "setup"}, _context())
        assert json.loads(setup_resp["body"]) == {}
        event = self._process_event(monkeypatch)
        body = json.loads(handler_mod.lambda_handler(event, _context())["body"])
        assert body["container_generation"] == 2  # setup served first
        assert body["container_cold"] is False

    def test_process_event_mode_carries_telemetry(self, handler_mod, monkeypatch):
        import zagg.output as output
        import zagg.temporal as temporal

        event_mask, collections, static = _temporal_inputs()
        monkeypatch.setattr(temporal, "open_dataset", lambda uri, **k: event_mask)
        monkeypatch.setattr(temporal, "read_temporal_inputs", lambda *a, **k: (collections, static))
        monkeypatch.setattr(output, "write_tabular", lambda rows, store_path, **k: store_path)
        resp = handler_mod.lambda_handler(_temporal_event(), _context())
        body = json.loads(resp["body"])
        assert resp["statusCode"] == 200, body
        assert body["container_cold"] is True and body["container_generation"] == 1

    def test_mirrored_result_carries_telemetry(self, handler_mod, monkeypatch, tmp_path):
        # The result_url mirror writes the POST-attach envelope, so the async
        # poller sees the same telemetry a sync caller would.
        url = str(tmp_path / "status" / "12345.json")
        event = self._process_event(monkeypatch, result_url=url)
        resp = handler_mod.lambda_handler(event, _context())
        written = json.loads(Path(url).read_text())
        assert written == resp
        assert json.loads(written["body"])["container_generation"] == 1


class TestSelfRecycle:
    """Issue #171 (self-recycling workers): after -- and only after -- the
    invocation's result envelope is successfully mirrored to ``result_url``,
    the handler destroys a bloated sandbox via the injectable ``_exit`` seam,
    gated by the ``ZAGG_RECYCLE_RSS_MB`` / ``ZAGG_RECYCLE_MAX_INVOCATIONS``
    env knobs. The #153 async channel makes this safe: the orchestrator polls
    the result object, not the Lambda response, and retries are pinned to 0."""

    @pytest.fixture(autouse=True)
    def _fresh_container(self, handler_mod, monkeypatch):
        monkeypatch.setattr(handler_mod, "_INVOCATIONS_SERVED", 0)
        monkeypatch.setattr(handler_mod, "_ASYNC_INVOCATIONS_SERVED", 0)
        monkeypatch.delenv("ZAGG_RECYCLE_RSS_MB", raising=False)
        monkeypatch.delenv("ZAGG_RECYCLE_MAX_INVOCATIONS", raising=False)

    def _process_event(self, monkeypatch, **extra):
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
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
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        event.update(extra)
        return event

    @staticmethod
    def _spy_exit(handler_mod, monkeypatch, calls):
        monkeypatch.setattr(handler_mod, "_exit", lambda code: calls.append(("exit", code)))

    def test_result_mirror_completes_before_exit(self, handler_mod, monkeypatch):
        # THE load-bearing ordering (issue #153): the S3 result mirror must
        # return before the sandbox dies, or the run loses the shard.
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "100")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 200 * 1024)
        monkeypatch.setattr(
            handler_mod,
            "_write_result",
            lambda url, resp, ev: (calls.append(("mirror", url)), True)[1],
        )
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200
        assert [c[0] for c in calls] == ["mirror", "exit"]
        assert calls[1] == ("exit", 0)

    def test_rss_threshold_triggers(self, handler_mod, monkeypatch, tmp_path, caplog):
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "1400")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 1500 * 1024)
        self._spy_exit(handler_mod, monkeypatch, calls)
        url = str(tmp_path / "status" / "1.json")
        event = self._process_event(monkeypatch, result_url=url)
        with caplog.at_level("INFO"):
            handler_mod.lambda_handler(event, _context())
        assert calls == [("exit", 0)]
        assert Path(url).exists()  # mirror landed before the exit
        # One structured, CloudWatch-searchable line (dashboard metric filter).
        assert (
            "ZAGG_SELF_RECYCLE rss_mb=1500 async_served=1 generation=1 threshold=1400"
            in caplog.text
        )

    def test_below_threshold_no_recycle(self, handler_mod, monkeypatch):
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "1400")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 800 * 1024)
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        assert calls == []

    def test_generation_cap_triggers_on_nth_invocation(self, handler_mod, monkeypatch, caplog):
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "2")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)  # RSS check inert
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        assert calls == []  # generation 1 < cap
        with caplog.at_level("INFO"):
            handler_mod.lambda_handler(event, _context())
        assert calls == [("exit", 0)]  # second async invocation hits the cap
        assert "ZAGG_SELF_RECYCLE rss_mb=n/a async_served=2 generation=2 threshold=2" in caplog.text

    def test_sync_setup_does_not_consume_recycle_budget(self, handler_mod, monkeypatch):
        # Issue #177: a synchronous setup invoke warms the sandbox but must not
        # be billed against the async budget -- with cap 2, setup + one worker
        # stays below the cap (pre-fix, generation 2 >= 2 fired after the FIRST
        # worker), and only the second worker recycles.
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "2")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)  # RSS check inert
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        monkeypatch.setattr(
            handler_mod, "_handle_setup", lambda event: {"statusCode": 200, "body": "{}"}
        )
        self._spy_exit(handler_mod, monkeypatch, calls)
        handler_mod.lambda_handler({"mode": "setup"}, _context())  # sync: no result_url
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        assert calls == []  # async budget spent: 1 < 2 (setup didn't count)
        handler_mod.lambda_handler(event, _context())
        assert calls == [("exit", 0)]  # second async invocation hits the cap

    def test_recycle_log_reports_true_generation(self, handler_mod, monkeypatch, caplog):
        # Issue #177: with cap 1 after a sync setup, the recycle fires on the
        # FIRST async worker (async_served=1) while both the log line and the
        # envelope telemetry keep reporting the true container generation (2).
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "1")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        monkeypatch.setattr(
            handler_mod, "_handle_setup", lambda event: {"statusCode": 200, "body": "{}"}
        )
        self._spy_exit(handler_mod, monkeypatch, calls)
        handler_mod.lambda_handler({"mode": "setup"}, _context())
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        with caplog.at_level("INFO"):
            resp = handler_mod.lambda_handler(event, _context())
        assert calls == [("exit", 0)]
        assert "ZAGG_SELF_RECYCLE rss_mb=n/a async_served=1 generation=2 threshold=1" in caplog.text
        assert json.loads(resp["body"])["container_generation"] == 2  # telemetry: true gen

    def test_failed_mirror_still_bills_recycle_budget(self, handler_mod, monkeypatch):
        # Issue #177 review fold: a failed mirror skips the recycle itself
        # (transient S3 fault -- don't also churn the sandbox), but the async
        # invocation WAS served, so it must still burn the budget: with cap 2,
        # a failed-mirror invoke plus one successful invoke recycles. (Billing
        # only mirrored invokes would let a flaky mirror extend the sandbox
        # past its cap -- the pre-#177 generation counter counted it too.)
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "2")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: None)
        outcomes = iter([False, True])
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: next(outcomes))
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        assert calls == []  # mirror failed: billed, but no recycle
        handler_mod.lambda_handler(event, _context())
        assert calls == [("exit", 0)]  # budget spent (2 >= 2), counting the failed one

    def test_disabled_by_default_and_by_zero(self, handler_mod, monkeypatch):
        # No env vars (and explicit "0") -> never recycles, however bloated:
        # a stack without the template defaults behaves exactly as pre-#171.
        calls = []
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 4000 * 1024)
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "0")
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "0")
        handler_mod.lambda_handler(event, _context())
        assert calls == []

    def test_sync_path_never_recycles(self, handler_mod, monkeypatch):
        # No result_url (synchronous invoke): the response would be lost, so
        # the recycle must not run even with both thresholds crossed.
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "100")
        monkeypatch.setenv("ZAGG_RECYCLE_MAX_INVOCATIONS", "1")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 2000 * 1024)
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch)  # no result_url
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200
        assert calls == []

    def test_failed_mirror_skips_recycle(self, handler_mod, monkeypatch, tmp_path):
        # A failed result write returns False (poll deadline will record the
        # shard failed); the sandbox must NOT also self-destruct on it.
        import obstore

        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "100")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 2000 * 1024)
        monkeypatch.setattr(
            obstore, "put", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("s3 down"))
        )
        self._spy_exit(handler_mod, monkeypatch, calls)
        url = str(tmp_path / "status" / "1.json")
        event = self._process_event(monkeypatch, result_url=url)
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200  # invocation result unaffected (#151)
        assert calls == []

    def test_non_numeric_knob_disables_check(self, handler_mod, monkeypatch):
        calls = []
        monkeypatch.setenv("ZAGG_RECYCLE_RSS_MB", "lots")
        monkeypatch.setattr(handler_mod, "_read_vmrss_kib", lambda: 2000 * 1024)
        monkeypatch.setattr(handler_mod, "_write_result", lambda *a: True)
        self._spy_exit(handler_mod, monkeypatch, calls)
        event = self._process_event(monkeypatch, result_url="s3://b/status/1.json")
        handler_mod.lambda_handler(event, _context())
        assert calls == []


class TestExtractMode:
    """mode="extract" (issue #148): chunk-boundary geometry extraction."""

    def _event(self, **over):
        ev = {
            "mode": "extract",
            "granule_urls": ["s3://bucket/ATL03_a.h5"],
            "output_prefix": "s3://out/boundaries/",
            "s3_credentials": _CREDS,
        }
        ev.update(over)
        return ev

    def test_missing_params_rejected(self, handler_mod):
        result = handler_mod.lambda_handler({"mode": "extract"}, _context())
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "granule_urls" in body["error"]
        assert "output_prefix" in body["error"]

    def test_dispatch_maps_credentials_and_returns_summary(self, handler_mod, monkeypatch):
        import zagg.catalog.extract as ext

        captured = {}

        def fake_run(urls, prefix, *, driver, credentials, **kw):
            captured.update(urls=urls, prefix=prefix, driver=driver, credentials=credentials)
            return [
                {
                    "granule": "ATL03_a.h5",
                    "ok": True,
                    "wall_s": 1.5,
                    "n_chunks": 3,
                    "output": f"{prefix}ATL03_a.boundaries.parquet",
                }
            ]

        monkeypatch.setattr(ext, "run_extraction", fake_run)
        result = handler_mod.lambda_handler(self._event(), _context())
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["mode"] == "extract"
        assert body["granule_count"] == 1 and body["failed"] == 0
        assert body["granules"][0]["wall_s"] == 1.5  # per-granule cost datum (#148)
        assert body["duration_s"] >= 0 and body["max_memory_mb"] > 0
        # process-mode creds are remapped to h5coro's kwargs, s3 driver default.
        assert captured["driver"] == "s3"
        assert captured["credentials"] == {
            "aws_access_key_id": "a",
            "aws_secret_access_key": "s",
            "aws_session_token": "t",
        }

    def test_https_driver_uses_edl_token(self, handler_mod, monkeypatch):
        import zagg.catalog.extract as ext

        captured = {}

        def fake_run(urls, prefix, *, driver, credentials, **kw):
            captured.update(driver=driver, credentials=credentials)
            return []

        monkeypatch.setattr(ext, "run_extraction", fake_run)
        ev = self._event(driver="https", s3_credentials={"edl_token": "tok"})
        handler_mod.lambda_handler(ev, _context())
        assert captured == {"driver": "https", "credentials": "tok"}

    def test_partial_failure_maps_to_500(self, handler_mod, monkeypatch):
        import zagg.catalog.extract as ext

        monkeypatch.setattr(
            ext,
            "run_extraction",
            lambda *a, **k: [
                {"granule": "a.h5", "ok": True, "wall_s": 1.0, "n_chunks": 2, "output": "x"},
                {"granule": "b.h5", "ok": False, "error": "boom"},
            ],
        )
        result = handler_mod.lambda_handler(
            self._event(granule_urls=["s3://b/a.h5", "s3://b/b.h5"]), _context()
        )
        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert body["failed"] == 1 and body["granule_count"] == 2

    def test_block_chunks_forwarded(self, handler_mod, monkeypatch):
        import zagg.catalog.extract as ext

        captured = {}

        def fake_run(urls, prefix, *, driver, credentials, **kw):
            captured.update(kw)
            return []

        monkeypatch.setattr(ext, "run_extraction", fake_run)
        handler_mod.lambda_handler(self._event(block_chunks=4), _context())
        assert captured == {"block_chunks": 4}

    def test_missing_cred_keys_rejected_fast(self, handler_mod):
        # Mirror process mode: malformed creds are a 400 before any read, not a
        # whole-batch 403 burn under the execution role.
        result = handler_mod.lambda_handler(
            self._event(s3_credentials={"accessKeyId": "a"}), _context()
        )
        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "secretAccessKey" in body["error"] and "sessionToken" in body["error"]

    def test_https_driver_missing_edl_token_rejected_fast(self, handler_mod):
        # https branch has the same fail-fast gate: no edl_token is a 400, not
        # a whole-batch 401 burn with the creds dict as the bearer token.
        result = handler_mod.lambda_handler(
            self._event(driver="https", s3_credentials={"accessKeyId": "a"}), _context()
        )
        assert result["statusCode"] == 400
        assert "edl_token" in json.loads(result["body"])["error"]


class TestAsyncResultWrite:
    """``result_url`` (issue #151): ``lambda_handler`` mirrors the process
    response envelope to the orchestrator-supplied object so an async (Event)
    invoke -- whose return value Lambda discards -- has a pollable result.
    Absent key -> no write, byte-identical to the synchronous path."""

    def _ok_event(self, monkeypatch, result_url=None):
        import zagg.grids as grids
        import zagg.processing as processing

        def fake_process_shard(grid, shard_key, granule_urls, **kwargs):
            meta = {
                "shard_key": shard_key,
                "cells_with_data": 0,
                "total_obs": 7,
                "granule_count": len(granule_urls),
                "files_processed": 0,
                "duration_s": 0.5,
                "error": None,
            }
            return pd.DataFrame(), meta

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: MagicMock())
        event = _base_event(_healpix_config_dict())
        event["child_order"] = 12
        if result_url is not None:
            event["result_url"] = result_url
        return event

    def test_result_url_writes_response_envelope(self, handler_mod, monkeypatch, tmp_path):
        url = str(tmp_path / "status" / "12345.json")
        event = self._ok_event(monkeypatch, result_url=url)
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200
        written = json.loads(Path(url).read_text())
        assert written == resp
        assert json.loads(written["body"])["total_obs"] == 7

    def test_gate_failure_envelope_also_written(self, handler_mod, monkeypatch, tmp_path):
        # A 400 (bad event) is mirrored too, so the poller learns fast instead
        # of waiting out the whole deadline.
        url = str(tmp_path / "status" / "bad.json")
        event = self._ok_event(monkeypatch, result_url=url)
        del event["shard_key"]
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 400
        assert json.loads(Path(url).read_text()) == resp

    def test_no_result_url_no_write(self, handler_mod, monkeypatch, tmp_path):
        event = self._ok_event(monkeypatch)
        resp = handler_mod.lambda_handler(event, _context())
        assert resp["statusCode"] == 200
        assert list(tmp_path.iterdir()) == []  # nothing written anywhere

    def test_write_failure_never_raises(self, handler_mod, monkeypatch, tmp_path):
        import obstore

        def boom(*a, **k):
            raise RuntimeError("s3 down")

        monkeypatch.setattr(obstore, "put", boom)
        url = str(tmp_path / "status" / "12345.json")
        event = self._ok_event(monkeypatch, result_url=url)
        resp = handler_mod.lambda_handler(event, _context())
        # The invocation result is unaffected; the failure is CloudWatch-only.
        assert resp["statusCode"] == 200
        assert not Path(url).exists()

    def test_setup_mode_ignores_result_url(self, handler_mod, monkeypatch, tmp_path):
        # The result channel is per-cell (process mode) only.
        monkeypatch.setattr(
            handler_mod, "_handle_setup", lambda event: {"statusCode": 200, "body": "{}"}
        )
        url = str(tmp_path / "status" / "setup.json")
        resp = handler_mod.lambda_handler({"mode": "setup", "result_url": url}, _context())
        assert resp["statusCode"] == 200
        assert not Path(url).exists()

    def test_extract_mode_ignores_result_url(self, handler_mod, monkeypatch, tmp_path):
        # Extract mode (issue #148) dispatches before the result_url mirror,
        # which stays process-mode-only -- pin the seam like setup mode above.
        monkeypatch.setattr(
            handler_mod, "_handle_extract", lambda event, context: {"statusCode": 200, "body": "{}"}
        )
        url = str(tmp_path / "status" / "extract.json")
        resp = handler_mod.lambda_handler({"mode": "extract", "result_url": url}, _context())
        assert resp["statusCode"] == 200
        assert not Path(url).exists()
