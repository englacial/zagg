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

        def fake_write_ragged(ragged, st, *, grid, shard_key):
            cap["ragged"].append((shard_key, ragged))

        # grid stub: a 1-D companion (single-element block index), exposes
        # chunk_grid_shape so _block_index_key is exercised on its real path.
        # ``chunks_per_shard`` fixes K (issue #91): the handler now derives the
        # K==1-vs-K>1 ragged-key choice from the grid, not the chunk count.
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
        assert captured["writes"] == 0  # driver writes; worker must not

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
