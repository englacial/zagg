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
