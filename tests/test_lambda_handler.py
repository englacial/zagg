"""Tests for the AWS Lambda handler's process-mode event contract (#24).

The handler lives under ``deployment/aws/`` (not an importable package module),
so it is loaded by path. These tests exercise the grid-neutral event schema:
the shard identifier is ``shard_key`` (not ``parent_morton``), and the
HEALPix-specific ``child_order`` is required for HEALPix runs but optional for
other grids.
"""

import importlib.util
import json
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from zagg.config import default_config

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

    def test_rectilinear_dispatch_without_child_order(self, handler_mod, monkeypatch):
        event = _base_event(_rectilinear_config_dict())
        event["parent_order"] = None  # rectilinear has no parent_order
        assert "child_order" not in event
        resp, captured = self._run(handler_mod, monkeypatch, event)
        assert resp["statusCode"] == 200
        assert captured["shard_key"] == 12345


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

        # A store whose template always exists; record nothing on it.
        store = MagicMock()
        store.exists.return_value = True

        def fake_write_dense(carrier, st, *, grid, chunk_idx):
            cap["dense"].append(chunk_idx)

        def fake_write_ragged(ragged, st, *, grid, shard_key):
            cap["ragged"].append((shard_key, ragged))

        # grid stub: a 1-D companion (single-element block index), exposes
        # chunk_grid_shape so _block_index_key is exercised on its real path.
        grid_stub = MagicMock()
        grid_stub.group_path = "8"
        grid_stub.chunk_grid_shape = (4,)

        monkeypatch.setattr(processing, "process_shard", fake_process_shard)
        monkeypatch.setattr(grids, "from_config", lambda *a, **k: grid_stub)
        monkeypatch.setattr(handler_mod, "open_store", lambda *a, **k: store)
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
