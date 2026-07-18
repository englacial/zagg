"""Store-root ``coverage.moc`` — issue #200 phases 3-4.

The end-of-run shard-order ranges MOC (O1 serialization, default-on for hive,
fail-open on both backends): the config flag, the envelope builder/parser, the
GET-union-PUT writer, the local and Lambda dispatcher legs, and the worker's
``mode: "coverage"`` handler — plus the phase-4 reader primitives in
``zagg.coverage`` (envelope load, per-tier AOI intersection, O7 lazy
staleness, the explicit refresh walk). Split from ``tests/test_coverage.py``
at the phase-3 seam (review finding, PR #208 round 3); the leaf tiers (box,
bitmap, stamp envelope) live there.
"""

import importlib.util
import json
import warnings
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

    def test_default_on_for_defaulted_hive(self):
        # Issue #253: an empty HEALPix config now defaults to hive, so the
        # coverage default follows (O9: coverage is the hive default).
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg()
        assert get_coverage_moc(cfg) is True
        validate_config(cfg)

    def test_default_off_for_flat(self):
        from zagg.config import get_coverage_moc, validate_config

        cfg = self._cfg(store_layout="flat")
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
            validate_config(self._cfg(store_layout="flat", coverage_moc=True))

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

    def test_docs_reference_example_round_trips(self):
        # The frozen-spec reference example in docs/hive_layout.md was
        # GENERATED by build_root_coverage (PR #208 round 4); parse it
        # straight out of the doc so it can never drift from the code.
        import re

        doc = (Path(__file__).parent.parent / "docs" / "hive_layout.md").read_text()
        blocks = re.findall(r"```json\n(\{.*?\})\n```", doc, flags=re.DOTALL)
        (root_example,) = [
            json.loads(b) for b in blocks if json.loads(b).get("encoding") == "ranges"
        ]
        words = hive.root_coverage_words(root_example)
        expected = _words("-4211321", "-4211322", "-4211323", "-4211324", "5112333")
        np.testing.assert_array_equal(words, np.sort(expected))
        # And it is literally what the serializer emits for those shards.
        rebuilt = hive.build_root_coverage(expected, 6)
        assert rebuilt["ranges"] == root_example["ranges"]
        assert rebuilt["order"] == root_example["order"] == 6

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
        monkeypatch.setattr(runner, "_invoke_lambda_ping", lambda *a, **kw: None)
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


# ── reader primitives (phase 4) ──────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _fresh_staleness_dedup():
    """The warn-once dedup is process-global by design; isolate tests."""
    from zagg import coverage

    coverage._stale_warned.clear()
    yield
    coverage._stale_warned.clear()


class TestLoadCoverage:
    def test_present_envelope_loads(self, tmp_path):
        from zagg.coverage import load_coverage

        root = str(tmp_path / "store")
        hive.write_root_coverage(root, hive.build_root_coverage(_words(SHARD), 6))
        env = load_coverage(root)
        assert env["encoding"] == "ranges" and env["ranges"] == [[SHARD, SHARD]]

    def test_missing_reads_none(self, tmp_path):
        from zagg.coverage import load_coverage

        assert load_coverage(str(tmp_path / "empty")) is None

    def test_garbage_reads_none_never_raises(self, tmp_path):
        from zagg.coverage import load_coverage

        root = tmp_path / "store"
        root.mkdir()
        (root / hive.ROOT_COVERAGE_NAME).write_bytes(b"garbage {")
        assert load_coverage(str(root)) is None

    def test_unknown_spec_or_encoding_reads_none(self, tmp_path):
        from zagg.coverage import load_coverage

        root = tmp_path / "store"
        root.mkdir()
        env = hive.build_root_coverage(_words(SHARD), 6)
        for mutation in ({"spec": "morton-moc/2"}, {"encoding": "bitmap"}):
            (root / hive.ROOT_COVERAGE_NAME).write_text(json.dumps({**env, **mutation}))
            assert load_coverage(str(root)) is None


class TestIntersections:
    def _leaf_cov(self, *decimals):
        return hive.build_coverage(morton_word(SHARD), _words(*decimals), 8)

    def test_root_coverage_and_hit_and_miss(self):
        from zagg.coverage import root_coverage_and

        env = hive.build_root_coverage(_words(SHARD, "-5112334"), 6)
        # An AOI at CELL order inside a covered shard intersects (mixed-order
        # containment via moc_and)...
        hit = root_coverage_and(env, _words(SHARD + "12"))
        assert hit.size > 0
        # ...a disjoint sibling misses.
        assert root_coverage_and(env, _words("-5112331")).size == 0

    def test_root_coverage_and_boundary(self):
        from zagg.coverage import root_coverage_and

        env = hive.build_root_coverage(_words(SHARD), 6)
        # The covered shard itself (range endpoint) intersects exactly.
        np.testing.assert_array_equal(root_coverage_and(env, _words(SHARD)), _words(SHARD))

    def test_box_and_hit_and_miss(self):
        from zagg.coverage import box_and

        cov = self._leaf_cov(SHARD + "12", SHARD + "43")
        assert box_and(cov, _words(SHARD + "12")).size > 0
        assert box_and(cov, _words(SHARD + "121")).size > 0  # deeper AOI cell
        assert box_and(cov, _words(SHARD + "2")).size == 0  # outside both members

    def test_box_and_skips_null_slots(self):
        from zagg.coverage import box_and

        cov = self._leaf_cov(SHARD + "12")  # 1 member + 3 nulls
        assert box_and(cov, _words(SHARD + "12")).size > 0

    def test_bitmap_and_exact_and_none_fallback(self, monkeypatch, tmp_path):
        import zagg.processing as processing
        from zagg.coverage import bitmap_and
        from zagg.grids import HealpixGrid

        cfg = default_config("atl06")
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        word = morton_word(SHARD)
        occupied = _words(SHARD + "12", SHARD + "43")

        def fake(g, shard_key, urls, **kwargs):
            import pandas as pd

            from zagg.config import get_data_vars

            coords = grid.chunk_coords(shard_key)
            df = pd.DataFrame(
                {var: 0.0 for var in get_data_vars(cfg)}, index=range(len(coords["cell_ids"]))
            )
            for name, vals in coords.items():
                df[name] = vals
            kwargs["write_chunk"](grid.block_index(int(shard_key)), df, {})
            kwargs["occupied_out"].append(occupied)
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 2,
                "total_obs": 2,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
            }

        monkeypatch.setattr(processing, "process_shard", fake)
        root = str(tmp_path / "store")
        hive.process_and_write_hive(word, ["s3://b/g.h5"], grid, {}, root, cfg, store_kwargs={})
        leaf = hive.shard_leaf_path(root, word)
        # Exact: an occupied cell intersects; an unoccupied one is a
        # DEFINITIVE miss (the bitmap is exact, unlike the box).
        assert bitmap_and(leaf, _words(SHARD + "12")).size > 0
        assert bitmap_and(leaf, _words(SHARD + "21")).size == 0
        # Box-only leaf (no sidecar): None -> caller falls back to the box.
        empty_leaf = str(tmp_path / "nothing.zarr")
        assert bitmap_and(empty_leaf, _words(SHARD + "12")) is None


class TestWarnIfStale:
    def test_warns_once_per_store_on_mismatch(self, tmp_path):
        from zagg.coverage import warn_if_stale

        env = hive.build_root_coverage(_words("-5112334"), 6)  # sibling only
        root = str(tmp_path / "store")
        with pytest.warns(UserWarning, match="refresh_root_coverage"):
            assert warn_if_stale(root, morton_word(SHARD), env) is True
        # Second mismatch on the SAME store: still stale, but silent.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert warn_if_stale(root, morton_word(SHARD), env) is True
        # A DIFFERENT store warns independently.
        with pytest.warns(UserWarning):
            assert warn_if_stale(str(tmp_path / "other"), morton_word(SHARD), env) is True

    def test_latch_key_is_slash_normalized(self, tmp_path):
        # root and root/ are the same store: one warning, not two (round 4).
        from zagg.coverage import warn_if_stale

        env = hive.build_root_coverage(_words("-5112334"), 6)
        root = str(tmp_path / "store")
        with pytest.warns(UserWarning):
            warn_if_stale(root, morton_word(SHARD), env)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert warn_if_stale(root + "/", morton_word(SHARD), env) is True

    def test_refresh_rearms_the_latch(self, tmp_path):
        # Once per stale EPISODE (round 4): after a successful refresh, a new
        # mismatch on the same store warns again.
        from zagg import coverage
        from zagg.coverage import warn_if_stale
        from zagg.grids import HealpixGrid
        from zagg.store import open_store

        cfg = default_config("atl06")
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(grid))
        leaf = hive.shard_leaf_path(root, morton_word(SHARD))
        store = open_store(leaf)
        grid.emit_shard_template(store, overwrite=True)
        hive.stamp_commit(store, cells_with_data=1, granule_count=1)

        stale_env = hive.build_root_coverage(_words("-5112334"), 6)
        with pytest.warns(UserWarning):
            warn_if_stale(root, morton_word(SHARD), stale_env)
        coverage.refresh_root_coverage(root)
        # New episode on the SAME store: the latch was re-armed.
        with pytest.warns(UserWarning):
            assert warn_if_stale(root, morton_word(SHARD), stale_env) is True

    def test_listed_shard_is_not_stale(self, tmp_path):
        from zagg.coverage import warn_if_stale

        env = hive.build_root_coverage(_words(SHARD, "-5112334"), 6)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert warn_if_stale(str(tmp_path), morton_word(SHARD), env) is False

    def test_no_root_moc_is_absence_not_staleness(self, tmp_path):
        from zagg.coverage import warn_if_stale

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert warn_if_stale(str(tmp_path), morton_word(SHARD), None) is False

    def test_malformed_envelope_counts_as_stale(self, tmp_path):
        from zagg.coverage import warn_if_stale

        with pytest.warns(UserWarning):
            assert warn_if_stale(str(tmp_path), morton_word(SHARD), {"order": 6}) is True


class TestRefreshRootCoverage:
    def _store_with_leaves(self, tmp_path, stamped, debris=()):
        from zagg.grids import HealpixGrid
        from zagg.store import open_store

        cfg = default_config("atl06")
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(grid))
        for dec in (*stamped, *debris):
            leaf = hive.shard_leaf_path(root, morton_word(dec))
            store = open_store(leaf)
            grid.emit_shard_template(store, overwrite=True)
            if dec in stamped:
                hive.stamp_commit(store, cells_with_data=1, granule_count=1)
        return root

    def test_rebuilds_exactly_the_stamped_set(self, tmp_path):
        from zagg.coverage import refresh_root_coverage

        # Both hemispheres (two base dirs) + one unstamped debris leaf.
        root = self._store_with_leaves(
            tmp_path, stamped=(SHARD, "-5112334", "5112333"), debris=("-5112331",)
        )
        env = refresh_root_coverage(root)
        assert env["source"] == "refresh"
        assert env["order"] == 6
        np.testing.assert_array_equal(
            hive.root_coverage_words(env),
            np.sort(_words(SHARD, "-5112334", "5112333")),
        )
        # The written object matches the returned envelope.
        assert hive.read_root_coverage(root) == env

    def test_replaces_stale_root_object(self, tmp_path):
        from zagg.coverage import refresh_root_coverage

        root = self._store_with_leaves(tmp_path, stamped=(SHARD,))
        # A stale root claims a shard that has no stamped leaf.
        hive.write_root_coverage(root, hive.build_root_coverage(_words("-5112334"), 6))
        env = refresh_root_coverage(root)
        # No union: the walk is ground truth and SUPERSEDES the stale cache.
        np.testing.assert_array_equal(hive.root_coverage_words(env), _words(SHARD))

    def test_no_stamped_leaves_removes_the_cache(self, tmp_path):
        from zagg.coverage import refresh_root_coverage

        root = self._store_with_leaves(tmp_path, stamped=(), debris=(SHARD,))
        hive.write_root_coverage(root, hive.build_root_coverage(_words(SHARD), 6))
        assert refresh_root_coverage(root) is None
        assert hive.read_root_coverage(root) is None

    def test_foreign_order_leaf_skipped_with_warning(self, tmp_path, caplog):
        # A stamped leaf at a non-manifest order (old-config survivor or a
        # hand-copied leaf) must not kill the escape hatch (round 4): it is
        # skipped with a logged warning and the root MOC is rebuilt from the
        # conforming set.
        import logging

        from zagg.coverage import refresh_root_coverage
        from zagg.grids import HealpixGrid
        from zagg.store import open_store

        root = self._store_with_leaves(tmp_path, stamped=(SHARD,))
        cfg = default_config("atl06")
        alien_grid = HealpixGrid(parent_order=5, child_order=8, layout="fullsphere", config=cfg)
        alien = SHARD[:-1]  # order-5 ancestor of the legit shard's sibling space
        leaf = hive.shard_leaf_path(root, morton_word(alien))
        store = open_store(leaf)
        alien_grid.emit_shard_template(store, overwrite=True)
        hive.stamp_commit(store, cells_with_data=1, granule_count=1)

        with caplog.at_level(logging.WARNING, logger="zagg.coverage"):
            env = refresh_root_coverage(root)
        assert any("mixed-order stores are unsupported" in r.message for r in caplog.records)
        np.testing.assert_array_equal(hive.root_coverage_words(env), _words(SHARD))

    def test_not_a_hive_root_raises(self, tmp_path):
        from zagg.coverage import refresh_root_coverage

        with pytest.raises(ValueError, match="not a hive store root"):
            refresh_root_coverage(str(tmp_path / "nowhere"))
