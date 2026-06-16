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
        summary = runner._run_lambda(
            default_config("atl06"),
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

        with pytest.raises(OSError) as exc_info:
            runner._run_lambda(
                default_config("atl06"),
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
