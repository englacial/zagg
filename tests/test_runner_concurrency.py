"""Tests for the concurrency wiring in the Lambda runner (#28).

The pre-flight probe and FD guard are exercised through ``_run_lambda`` with
boto3 and the per-cell invoke fully mocked -- no live AWS. The grid, auth, and
setup/finalize invokes are stubbed at the ``runner`` module seam.
"""

from unittest.mock import MagicMock

import pytest

from zagg import runner
from zagg.concurrency import ConcurrencyReport
from zagg.config import default_config


@pytest.fixture
def lambda_env(monkeypatch):
    """Stub the AWS-touching seams of ``_run_lambda`` and capture the pool size."""
    monkeypatch.setattr(
        runner,
        "get_nsidc_s3_credentials",
        lambda: {
            "accessKeyId": "AKIA",
            "secretAccessKey": "s",
            "sessionToken": "t",
        },
    )

    # Stub grid construction (signature must match the catalog).
    grid = MagicMock()
    grid.signature.return_value = {}
    grid.spatial_signature.return_value = {}
    grid.block_index.side_effect = lambda k: (k,)
    import zagg.grids as grids_mod

    monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: grid)

    # Stub setup/finalize invokes (no real Lambda).
    monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)

    # Capture the ThreadPoolExecutor max_workers actually used.
    captured = {}
    real_pool = runner.ThreadPoolExecutor

    def _spy_pool(max_workers=None, **kw):
        captured["max_workers"] = max_workers
        return real_pool(max_workers=max_workers, **kw)

    monkeypatch.setattr(runner, "ThreadPoolExecutor", _spy_pool)

    # Stub boto3.Session so no real clients are created.
    import boto3

    monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())

    return {"grid": grid, "captured": captured}


def _catalog():
    return {
        "metadata": {},
        "grid_signature": {},
        "shard_keys": [10, 11, 12, 13],
        "granules": [[{"s3": f"s3://b/g{i}.h5"}] for i in range(4)],
    }


def _report(available):
    return ConcurrencyReport(
        account_limit=1000,
        current_concurrent=100,
        padding=100,
        available=available,
        function_reserved=None,
    )


class TestProbeClampsWorkers:
    def test_pool_sized_to_clamped_workers(self, lambda_env, monkeypatch):
        # Probe clamps requested 1700 down to 64.
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (64, _report(64)),
        )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_cell",
            lambda *a, **k: {
                "status_code": 200,
                "body": {"total_obs": 5},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": 0,
            },
        )
        cfg = default_config("atl06")
        # Flat lambda lifecycle pinned explicitly (issue #253 defaults hive).
        cfg.output["store_layout"] = "flat"
        summary = runner._run_lambda(
            cfg,
            _catalog(),
            "s3://out/x.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1700,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
            function_name="process-shard",
        )
        assert lambda_env["captured"]["max_workers"] == 64
        assert summary["cells_with_data"] == 4


class TestCostBlock:
    """Structured cost block in the lambda summary (issue #298).

    Reuses the mocked ``_run_lambda`` harness above: 4 shards, each reporting
    a 1 s billed duration, no live AWS.
    """

    def _run(self, monkeypatch, cfg, duration=1.0, **extra):
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (4, _report(4)),
        )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_cell",
            lambda *a, **k: {
                "status_code": 200,
                "body": {"total_obs": 5},
                "error": None,
                "lambda_duration": duration,
                "shard_key": 0,
            },
        )
        return runner._run_lambda(
            cfg,
            _catalog(),
            "s3://out/x.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=4,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
            function_name="process-shard",
            **extra,
        )

    def _flat_cfg(self):
        cfg = default_config("atl06")
        cfg.output["store_layout"] = "flat"
        return cfg

    def test_cost_block_shape_and_ceiling(self, lambda_env, monkeypatch):
        from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC, max_cost_usd

        summary = self._run(monkeypatch, self._flat_cfg())
        cost = summary["cost"]
        # Ceiling: 4 shards x 4 GB x 900 s, computable pre-invoke.
        assert cost["max_cost_usd"] == pytest.approx(max_cost_usd(4, 4.0, timeout_s=900))
        # Estimated stays a None placeholder until issues #297/#299 land.
        assert cost["estimated_cost_usd"] is None
        # Actual mirrors the legacy rollup key (4 x 1 s x 4 GB x rate).
        assert cost["actual_cost_usd"] == pytest.approx(4 * 1.0 * 4.0 * LAMBDA_PRICE_PER_GB_SEC)
        assert cost["actual_cost_usd"] == summary["estimated_cost_usd"]

    def test_max_bounds_actual(self, lambda_env, monkeypatch):
        # Property: even at the worst billed duration (the 900 s function
        # timeout), the pre-invoke ceiling bounds the rollup.
        summary = self._run(monkeypatch, self._flat_cfg(), duration=900.0)
        cost = summary["cost"]
        assert cost["max_cost_usd"] >= cost["actual_cost_usd"]

    def test_worker_variant_prices_its_memory(self, lambda_env, monkeypatch):
        # A worker: 8192 variant (issue #235) bills 8 GB, not the flat 4.
        cfg = self._flat_cfg()
        cfg.worker = {"memory": 8192}
        summary = self._run(monkeypatch, cfg)
        assert summary["gb_seconds"] == pytest.approx(4 * 1.0 * 8.0)
        base = self._run(monkeypatch, self._flat_cfg())
        assert summary["cost"]["max_cost_usd"] == pytest.approx(2 * base["cost"]["max_cost_usd"])

    def test_worker_memory_gb_resolution(self):
        assert runner._worker_memory_gb(self._flat_cfg()) == 4.0
        cfg = self._flat_cfg()
        cfg.worker = {"memory": 2048}
        assert runner._worker_memory_gb(cfg) == 2.0

    def test_on_progress_fires_per_completed_shard(self, lambda_env, monkeypatch):
        # Progress hook (issue #298 phase 2): one call per completed unit with
        # the 1-based done count, the unit total, and the running metered cost.
        from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC

        seen = []
        self._run(
            monkeypatch,
            self._flat_cfg(),
            on_progress=lambda done, total, cost: seen.append((done, total, cost)),
        )
        assert [(d, t) for d, t, _ in seen] == [(1, 4), (2, 4), (3, 4), (4, 4)]
        costs = [c for _, _, c in seen]
        assert costs == sorted(costs)  # running rollup only grows
        assert costs[-1] == pytest.approx(4 * 1.0 * 4.0 * LAMBDA_PRICE_PER_GB_SEC)


class TestCostBlockEvents:
    """Structured cost block on the tabular events path (issue #298).

    The events path (``_run_lambda_events``) carries its own copy of the
    phase-1 cost logic -- the pre-invoke ceiling, the ``memory_gb`` fed to the
    ``LambdaExecutor``, the ``gb_seconds`` rollup, and the ``cost`` block -- so
    it needs its own coverage; ``TestCostBlock`` only drives the spatial
    ``_run_lambda``. Mocks mirror that harness: 3 events, each billing a 1 s
    duration, no live AWS.
    """

    def _events(self, n=3):
        return [{"event_key": f"e{i}", "event_mask_uri": f"s3://b/m{i}.npy"} for i in range(n)]

    def _run(self, monkeypatch, cfg, n=3, duration=1.0):
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (n, _report(n)),
        )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_event",
            lambda client, ev, *a, **k: {
                "status_code": 200,
                "body": {"timesteps_processed": 1, "results": {}, "meta": {}},
                "error": None,
                "event_key": ev["event_key"],
                "lambda_duration": duration,
            },
        )
        # No real tabular write; return a sentinel path like the real writer.
        monkeypatch.setattr(runner, "_write_tabular_output", lambda *a, **k: "s3://out/x.parquet")
        return runner._run_lambda_events(
            cfg,
            self._events(n),
            "s3://out/x.parquet",
            max_workers=n,
            region="us-west-2",
            function_name="process-shard",
            invocation="sync",
        )

    def _cfg(self):
        from zagg.config import PipelineConfig

        # output.format must be tabular (not the "zarr" default) to clear the
        # temporal guard; mirrors the events-path seam test.
        return PipelineConfig(output={"format": "parquet"})

    def test_cost_block_shape_and_ceiling(self, lambda_env, monkeypatch):
        from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC, max_cost_usd

        summary = self._run(monkeypatch, self._cfg())
        cost = summary["cost"]
        # Ceiling: 3 events x 4 GB x 900 s, computable pre-invoke.
        assert cost["max_cost_usd"] == pytest.approx(max_cost_usd(3, 4.0, timeout_s=900))
        # Estimated stays a None placeholder until issues #297/#299 land.
        assert cost["estimated_cost_usd"] is None
        # Actual mirrors the legacy rollup key (3 x 1 s x 4 GB x rate).
        assert cost["actual_cost_usd"] == pytest.approx(3 * 1.0 * 4.0 * LAMBDA_PRICE_PER_GB_SEC)
        assert cost["actual_cost_usd"] == summary["estimated_cost_usd"]

    def test_max_bounds_actual(self, lambda_env, monkeypatch):
        # Even at the worst billed duration (the 900 s timeout), the pre-invoke
        # ceiling bounds the rollup.
        summary = self._run(monkeypatch, self._cfg(), duration=900.0)
        cost = summary["cost"]
        assert cost["max_cost_usd"] >= cost["actual_cost_usd"]

    def test_worker_variant_prices_its_memory(self, lambda_env, monkeypatch):
        # A worker: 8192 variant (issue #235) bills 8 GB in both the gb_seconds
        # rollup and the ceiling -- guarding against a revert to flat 4 GB.
        cfg = self._cfg()
        cfg.worker = {"memory": 8192}
        summary = self._run(monkeypatch, cfg)
        assert summary["gb_seconds"] == pytest.approx(3 * 1.0 * 8.0)
        base = self._run(monkeypatch, self._cfg())
        assert summary["cost"]["max_cost_usd"] == pytest.approx(2 * base["cost"]["max_cost_usd"])


class TestFdExhaustionSurfaces:
    def test_errno_24_in_future_reraised_with_guidance(self, lambda_env, monkeypatch):
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (100, _report(100)),
        )

        def _boom(*a, **k):
            raise OSError(24, "Too many open files")

        monkeypatch.setattr(runner, "_invoke_lambda_cell", _boom)

        cfg = default_config("atl06")
        # Flat lambda lifecycle pinned explicitly (issue #253 defaults hive).
        cfg.output["store_layout"] = "flat"
        with pytest.raises(OSError) as exc_info:
            runner._run_lambda(
                cfg,
                _catalog(),
                "s3://out/x.zarr",
                12,
                max_cells=None,
                morton_cell=None,
                max_workers=100,
                overwrite=False,
                dry_run=False,
                region="us-west-2",
                function_name="process-shard",
            )
        assert exc_info.value.errno == 24
        assert "ulimit -n" in str(exc_info.value)
        assert "--max-workers" in str(exc_info.value)


class TestInvokeLambdaCellFdExhaustion:
    """The cell must re-raise FD exhaustion, not swallow it into a result dict."""

    def _call(self, client):
        return runner._invoke_lambda_cell(
            client,
            (0,),
            10,
            6,
            12,
            ["s3://b/g.h5"],
            "s3://out/x.zarr",
            {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
            function_name="process-shard",
            config_dict=None,
            max_workers=900,
        )

    def test_emfile_reraised_not_swallowed(self):
        import errno

        client = MagicMock()
        client.invoke.side_effect = OSError(errno.EMFILE, "Too many open files")
        with pytest.raises(OSError) as exc_info:
            self._call(client)
        assert exc_info.value.errno == errno.EMFILE
        assert "ulimit -n" in str(exc_info.value)

    def test_botocore_wrapped_emfile_reraised(self):
        import errno

        inner = OSError(errno.EMFILE, "Too many open files")
        wrapped = ConnectionError("Could not connect to the endpoint URL")
        wrapped.__cause__ = inner
        client = MagicMock()
        client.invoke.side_effect = wrapped
        with pytest.raises(OSError) as exc_info:
            self._call(client)
        assert exc_info.value.errno == errno.EMFILE

    def test_unrelated_error_still_returns_result(self):
        # Non-FD errors keep the existing swallow-into-result behavior.
        client = MagicMock()
        client.invoke.side_effect = RuntimeError("some other boto failure")
        result = self._call(client)
        assert result["status_code"] is None
        assert "some other boto failure" in result["error"]


class TestLogConcurrencyReport:
    def test_logs_account_view(self, caplog):
        with caplog.at_level("INFO", logger="zagg.runner"):
            runner._log_concurrency_report(_report(700), 700)
        text = caplog.text
        assert "limit=1000" in text
        assert "available=700" in text
        assert "700 workers" in text

    def test_warns_when_account_unreadable(self, caplog):
        report = ConcurrencyReport(
            account_limit=None,
            current_concurrent=0,
            padding=100,
            available=None,
            function_reserved=None,
        )
        with caplog.at_level("WARNING", logger="zagg.runner"):
            runner._log_concurrency_report(report, 224)
        assert "file-descriptor limit only" in caplog.text
        assert "224" in caplog.text

    def test_reports_function_reserved(self, caplog):
        report = ConcurrencyReport(
            account_limit=1000,
            current_concurrent=0,
            padding=100,
            available=900,
            function_reserved=50,
        )
        with caplog.at_level("INFO", logger="zagg.runner"):
            runner._log_concurrency_report(report, 900)
        assert "reserved concurrency: 50" in caplog.text
