"""Store-root ``coverage.moc`` — issue #200 phase 3.

The end-of-run shard-order ranges MOC (O1 serialization, default-on for hive,
fail-open on both backends): the config flag, the envelope builder/parser, the
GET-union-PUT writer, the local and Lambda dispatcher legs, and the worker's
``mode: "coverage"`` handler. Split from ``tests/test_coverage.py`` at the
phase-3 seam (review finding, PR #208 round 3); the leaf tiers (box, bitmap,
stamp envelope) live there.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

from zagg import hive
from zagg.config import default_config
from zagg.grids.morton import morton_word

# Order-6 southern shard shared with the leaf-tier suite (decimal -5112333).
SHARD = "-5112333"

HANDLER_PATH = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"


@pytest.fixture(scope="module")
def handler_mod():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_root_coverage", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _words(*decimals):
    return np.asarray([morton_word(d) for d in decimals], dtype=np.uint64)


# ── the root coverage MOC (phase 3) ──────────────────────────────────────────


class TestCoverageMocConfig:
    """O9: default ON for hive (healpix-only by validation); explicit true
    anywhere it cannot land is a pointed error, absent there is simply off."""

    def _cfg(self, **output):
        cfg = default_config("atl06")
        cfg.output.update(output)
        return cfg

    def test_default_on_for_hive(self):
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg(store_layout="hive")
        assert get_coverage_moc(cfg) is True
        validate_config(cfg)

    def test_default_off_for_flat(self):
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg()
        assert get_coverage_moc(cfg) is False
        validate_config(cfg)

    def test_explicit_off_for_hive(self):
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg(store_layout="hive", coverage_moc=False)
        assert get_coverage_moc(cfg) is False
        validate_config(cfg)

    def test_null_falls_back_to_default(self):
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg(store_layout="hive", coverage_moc=None)
        assert get_coverage_moc(cfg) is True
        validate_config(cfg)

    def test_explicit_true_on_flat_rejected(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="store_layout: hive"):
            validate_config(self._cfg(coverage_moc=True))

    def test_explicit_true_on_rectilinear_rejected(self):
        from zagg.config import validate_config

        cfg = self._cfg(coverage_moc=True)
        cfg.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        with pytest.raises(ValueError, match="healpix"):
            validate_config(cfg)

    def test_non_bool_rejected(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="coverage_moc must be a boolean"):
            validate_config(self._cfg(store_layout="hive", coverage_moc="yes"))


class TestRootCoverage:
    """The O1 envelope: shard-order ranges MOC, decimal-string endpoints."""

    def test_envelope_fields_and_run_collapse(self):
        keys = _words("-511", "-512", "-513", "-521", "511", "512")
        env = hive.build_root_coverage(keys, 2)
        assert env["spec"] == hive.COVERAGE_SPEC
        assert env["encoding"] == "ranges"
        assert env["order"] == 2
        assert env["source"] == "dispatcher"
        assert env["generated_at"]
        # Consecutive ranks collapse to one range; runs never span base cells.
        assert ["-511", "-513"] in env["ranges"]
        assert ["-521", "-521"] in env["ranges"]
        assert ["511", "512"] in env["ranges"]
        assert len(env["ranges"]) == 3
        # Endpoints are STRINGS (packed u64 words exceed 2^53 — the JSON
        # float-parser trap O1 specs around), and the payload is JSON-safe.
        assert all(isinstance(e, str) for pair in env["ranges"] for e in pair)
        assert json.loads(json.dumps(env)) == env

    def test_words_round_trip_exact(self):
        keys = _words("-511", "-512", "-514", "521", "611")
        env = hive.build_root_coverage(keys, 2)
        np.testing.assert_array_equal(hive.root_coverage_words(env), np.sort(keys))

    @pytest.mark.parametrize("seed", range(4))
    def test_words_round_trip_property(self, seed):
        rng = np.random.default_rng(seed)
        bases = ["-5", "5", "-1", "3"]
        decs = {
            rng.choice(bases) + "".join(rng.choice(list("1234"), size=4))
            for _ in range(int(rng.integers(1, 60)))
        }
        keys = _words(*decs)
        env = hive.build_root_coverage(keys, 4)
        np.testing.assert_array_equal(hive.root_coverage_words(env), np.sort(keys))

    def test_wrong_order_key_rejected(self):
        with pytest.raises(ValueError, match="shard order"):
            hive.build_root_coverage(_words("-511", "-5112"), 3)

    def test_malformed_range_rejected(self):
        env = hive.build_root_coverage(_words("-511"), 2)
        env["ranges"] = [["-511", "512"]]  # base-crossing run
        with pytest.raises(ValueError, match="malformed coverage range"):
            hive.root_coverage_words(env)

    def test_payload_bounded_for_contiguous_fleet(self):
        # The transport claim (plan question 3): a spatially coherent 50k-shard
        # run serializes to a few-KB envelope, far under Lambda's 256 KB
        # async-invoke cap that a raw 50k-key list would break.
        keys = _words(*("5" + hive._rank_tail(r, 9) for r in range(50_000)))
        env = hive.build_root_coverage(keys, 9)
        assert len(env["ranges"]) == 1
        assert len(json.dumps(env).encode()) < 4096

    def test_write_creates_then_unions(self, tmp_path):
        root = str(tmp_path / "store")
        first = hive.build_root_coverage(_words("-511", "-512"), 2)
        hive.write_root_coverage(root, first)
        stored = hive.read_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(stored), np.sort(_words("-511", "-512"))
        )
        # Incremental run: a second write UNIONS with the existing object.
        second = hive.build_root_coverage(_words("-513", "521"), 2)
        hive.write_root_coverage(root, second)
        merged = hive.read_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(merged),
            np.sort(_words("-511", "-512", "-513", "521")),
        )
        # The adjacent -511..-513 accumulate into one run across runs.
        assert ["-511", "-513"] in merged["ranges"]

    def test_unparsable_existing_is_overwritten(self, tmp_path):
        # The root object is a regenerable cache (D9): garbage is logged and
        # replaced, never merged — the sweep is the authoritative rebuilder.
        root = tmp_path / "store"
        root.mkdir()
        (root / hive.ROOT_COVERAGE_NAME).write_bytes(b"not json {")
        env = hive.build_root_coverage(_words("-511"), 2)
        hive.write_root_coverage(str(root), env)
        assert hive.read_root_coverage(str(root))["ranges"] == [["-511", "-511"]]

    def test_incompatible_existing_is_overwritten(self, tmp_path):
        root = str(tmp_path / "store")
        hive.write_root_coverage(root, hive.build_root_coverage(_words("-5112"), 3))
        env = hive.build_root_coverage(_words("-511"), 2)  # different order
        hive.write_root_coverage(root, env)
        assert hive.read_root_coverage(root)["order"] == 2
        assert hive.read_root_coverage(root)["ranges"] == [["-511", "-511"]]


def _local_agg_catalog(tmp_path, shard):
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": 6,
            "child_order": 12,
            "layout": "fullsphere",
        },
        "shard_keys": [int(shard)],
        "granules": [[{"id": "g1", "s3": "s3://b/g1.h5", "https": "https://h/g1.h5"}]],
    }
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog))
    return str(path)


class TestLocalRootCoverage:
    """End-to-end through the local backend: the run loop's successful shard
    set becomes the root MOC; failures are excluded; flag-off writes nothing."""

    def _agg(self, monkeypatch, tmp_path, *, meta_error=None, coverage_moc=None):
        from zagg import runner
        from zagg.runner import agg

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        if coverage_moc is not None:
            cfg.output["coverage_moc"] = coverage_moc
        shard = morton_word(SHARD)
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            return {"shard_key": int(shard_key), "error": meta_error, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        root = str(tmp_path / "out")
        agg(cfg, catalog=_local_agg_catalog(tmp_path, shard), store=root, backend="local")
        return root, shard

    def test_successful_run_writes_root_moc(self, monkeypatch, tmp_path):
        root, shard = self._agg(monkeypatch, tmp_path)
        env = hive.read_root_coverage(root)
        assert env["order"] == 6
        assert env["source"] == "dispatcher"
        np.testing.assert_array_equal(
            hive.root_coverage_words(env), np.asarray([shard], dtype=np.uint64)
        )

    def test_failed_shards_excluded(self, monkeypatch, tmp_path):
        # Every shard errored -> no successful completions -> no root object.
        root, _shard = self._agg(monkeypatch, tmp_path, meta_error="boom")
        assert hive.read_root_coverage(root) is None

    def test_flag_off_writes_nothing(self, monkeypatch, tmp_path):
        import os

        root, _shard = self._agg(monkeypatch, tmp_path, coverage_moc=False)
        assert hive.read_root_coverage(root) is None
        assert sorted(os.listdir(root)) == [hive.MANIFEST_NAME]


class TestLambdaCoverageDispatch:
    """The dispatcher leg: ONE fire-and-forget invoke with the serialized
    envelope after the fan-out concludes; byte-identical dispatch when off;
    fail-open when the invoke raises."""

    def _agg(self, monkeypatch, tmp_path, captured, *, coverage_moc=None, invoke=None):
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport
        from zagg.runner import agg

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        if coverage_moc is not None:
            cfg.output["coverage_moc"] = coverage_moc
        shard = morton_word(SHARD)
        catalog_path = _local_agg_catalog(tmp_path, shard)

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )
        monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **kw: None)
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_cell",
            lambda *a, **k: {
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": shard,
            },
        )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_coverage",
            invoke
            or (lambda client, fn, store, envelope, **kw: captured.update(envelope=envelope)),
        )
        summary = agg(cfg, catalog=catalog_path, store="s3://out/product", backend="lambda")
        return summary, shard

    def test_dispatches_serialized_envelope(self, monkeypatch, tmp_path):
        captured: dict = {}
        _summary, shard = self._agg(monkeypatch, tmp_path, captured)
        env = captured["envelope"]
        assert env["encoding"] == "ranges" and env["order"] == 6
        np.testing.assert_array_equal(
            hive.root_coverage_words(env), np.asarray([shard], dtype=np.uint64)
        )

    def test_flag_off_dispatch_is_byte_identical(self, monkeypatch, tmp_path):
        captured: dict = {}
        self._agg(monkeypatch, tmp_path, captured, coverage_moc=False)
        assert captured == {}  # no coverage invoke at all

    def test_invoke_failure_is_fail_open(self, monkeypatch, tmp_path):
        def boom(*a, **k):
            raise RuntimeError("event invoke failed")

        summary, _shard = self._agg(monkeypatch, tmp_path, {}, invoke=boom)
        assert summary["cells_with_data"] == 1  # the run result is untouched


class TestHandlerCoverageMode:
    """The worker leg: ``mode: "coverage"`` GET-unions-PUTs the root object."""

    @staticmethod
    def _ctx():
        ctx = MagicMock()
        ctx.aws_request_id = "req-1"
        ctx.function_name = "process-shard"
        ctx.memory_limit_in_mb = 2048
        ctx.get_remaining_time_in_millis.return_value = 900_000
        return ctx

    def _event(self, root, envelope):
        return {"mode": "coverage", "store_path": root, "coverage": envelope}

    def test_writes_root_object(self, handler_mod, tmp_path):
        root = str(tmp_path / "store")
        env = hive.build_root_coverage(_words(SHARD), 6)
        resp = handler_mod.lambda_handler(self._event(root, env), self._ctx())
        assert resp["statusCode"] == 200
        assert json.loads(resp["body"])["mode"] == "coverage"
        stored = hive.read_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(stored), hive.root_coverage_words(env)
        )

    def test_unions_with_existing(self, handler_mod, tmp_path):
        root = str(tmp_path / "store")
        prior = SHARD[:-1] + "4"  # sibling shard from an earlier run
        hive.write_root_coverage(root, hive.build_root_coverage(_words(prior), 6))
        env = hive.build_root_coverage(_words(SHARD), 6)
        resp = handler_mod.lambda_handler(self._event(root, env), self._ctx())
        assert resp["statusCode"] == 200
        np.testing.assert_array_equal(
            hive.root_coverage_words(hive.read_root_coverage(root)),
            np.sort(_words(SHARD, prior)),
        )

    def test_error_returns_500_never_raises(self, handler_mod, tmp_path):
        # Fail-open contract: nobody reads the Event-invoke response, but the
        # handler must degrade to a logged 500, not an unhandled exception.
        event = self._event(str(tmp_path / "store"), hive.build_root_coverage(_words(SHARD), 6))
        del event["coverage"]  # malformed event
        resp = handler_mod.lambda_handler(event, self._ctx())
        assert resp["statusCode"] == 500
        assert json.loads(resp["body"])["mode"] == "coverage"
        # And a malformed EXISTING object never breaks the write: it is
        # overwritten (regenerable cache), still a 200.
        root = tmp_path / "store2"
        root.mkdir()
        (root / hive.ROOT_COVERAGE_NAME).write_bytes(b"garbage {")
        ok = handler_mod.lambda_handler(
            self._event(str(root), hive.build_root_coverage(_words(SHARD), 6)), self._ctx()
        )
        assert ok["statusCode"] == 200
        assert hive.read_root_coverage(str(root))["ranges"] == [[SHARD, SHARD]]
