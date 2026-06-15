"""Tests for the pre-flight concurrency probe.

All AWS calls are mocked -- nothing here touches live AWS. The Lambda and
CloudWatch clients are :class:`unittest.mock.MagicMock`, and the file-descriptor
soft limit is faked by monkeypatching ``resource.getrlimit``.
"""

from unittest.mock import MagicMock

import pytest

from zagg import concurrency


@pytest.fixture
def fake_rlimit(monkeypatch):
    """Fake ``resource.getrlimit(RLIMIT_NOFILE)`` to a given (soft, hard)."""

    def _set(soft, hard):
        monkeypatch.setattr(
            concurrency.resource,
            "getrlimit",
            lambda _which: (soft, hard),
        )

    return _set


def _lambda_client(account_limit=1000, reserved=None):
    client = MagicMock()
    client.get_account_settings.return_value = {
        "AccountLimit": {"ConcurrentExecutions": account_limit},
    }
    resp = {}
    if reserved is not None:
        resp["ReservedConcurrentExecutions"] = reserved
    client.get_function_concurrency.return_value = resp
    return client


def _cloudwatch_client(current_max=None):
    client = MagicMock()
    if current_max is None:
        client.get_metric_statistics.return_value = {"Datapoints": []}
    else:
        client.get_metric_statistics.return_value = {
            "Datapoints": [{"Maximum": current_max}],
        }
    return client


class TestFdSafeMaxWorkers:
    def test_subtracts_headroom(self, fake_rlimit):
        fake_rlimit(256, 1024)
        assert concurrency.fd_safe_max_workers() == 256 - concurrency._FD_HEADROOM

    def test_custom_headroom(self, fake_rlimit):
        fake_rlimit(1000, 4096)
        assert concurrency.fd_safe_max_workers(headroom=100) == 900

    def test_floored_at_one(self, fake_rlimit):
        fake_rlimit(16, 16)  # below headroom
        assert concurrency.fd_safe_max_workers() == 1


class TestRaiseForFdExhaustion:
    def test_errno_24_raises_with_guidance(self, fake_rlimit):
        fake_rlimit(256, 1024)
        original = OSError(24, "Too many open files")
        with pytest.raises(OSError) as exc_info:
            concurrency.raise_for_fd_exhaustion(original, max_workers=900)
        msg = str(exc_info.value)
        assert exc_info.value.errno == 24
        assert "ulimit -n" in msg
        assert "--max-workers" in msg
        assert "900" in msg  # the offending worker count
        assert "256" in msg  # the soft limit
        assert exc_info.value.__cause__ is original

    def test_non_errno_24_is_noop(self):
        # A different OSError must pass through untouched (no raise).
        concurrency.raise_for_fd_exhaustion(OSError(13, "Permission denied"), 100)


class TestProbeConcurrency:
    def test_full_read(self, fake_rlimit):
        lam = _lambda_client(account_limit=1000, reserved=50)
        cw = _cloudwatch_client(current_max=200)
        report = concurrency.probe_concurrency(lam, cw, "process-shard")
        assert report.account_limit == 1000
        assert report.current_concurrent == 200
        # padding = max(100, 5% of 1000) = 100
        assert report.padding == 100
        # available = 1000 - 200 - 100
        assert report.available == 700
        assert report.function_reserved == 50

    def test_padding_pct_dominates_floor(self, fake_rlimit):
        lam = _lambda_client(account_limit=10000)
        cw = _cloudwatch_client(current_max=0)
        report = concurrency.probe_concurrency(lam, cw, "fn")
        # 5% of 10000 = 500 > floor 100
        assert report.padding == 500
        assert report.available == 10000 - 0 - 500

    def test_available_floored_at_one(self):
        lam = _lambda_client(account_limit=100)
        cw = _cloudwatch_client(current_max=200)  # over-subscribed
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.available == 1

    def test_missing_account_settings_degrades(self):
        lam = MagicMock()
        lam.get_account_settings.side_effect = Exception("AccessDenied")
        lam.get_function_concurrency.return_value = {}
        cw = _cloudwatch_client(current_max=0)
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.account_limit is None
        assert report.available is None
        assert report.padding == concurrency._CONCURRENCY_PADDING_FLOOR

    def test_missing_cloudwatch_degrades_to_zero(self):
        lam = _lambda_client(account_limit=1000)
        cw = MagicMock()
        cw.get_metric_statistics.side_effect = Exception("AccessDenied")
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.current_concurrent == 0
        assert report.available == 1000 - 0 - 100

    def test_no_datapoints_is_zero(self):
        lam = _lambda_client(account_limit=1000)
        cw = _cloudwatch_client(current_max=None)  # empty datapoints
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.current_concurrent == 0

    def test_unreserved_function_is_none(self):
        lam = _lambda_client(account_limit=1000, reserved=None)
        cw = _cloudwatch_client(current_max=0)
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.function_reserved is None

    def test_function_concurrency_failure_degrades(self):
        lam = _lambda_client(account_limit=1000)
        lam.get_function_concurrency.side_effect = Exception("AccessDenied")
        cw = _cloudwatch_client(current_max=0)
        report = concurrency.probe_concurrency(lam, cw, "fn")
        assert report.function_reserved is None
        # the rest of the probe still works
        assert report.available == 900


class TestComputeAvailableWorkers:
    def test_account_headroom_clamps(self, fake_rlimit):
        fake_rlimit(8192, 8192)  # FD ceiling well above account headroom
        lam = _lambda_client(account_limit=1000)
        cw = _cloudwatch_client(current_max=200)
        workers, report = concurrency.compute_available_workers(1700, lam, cw, "fn")
        # account available = 700 < fd ceiling and < requested
        assert workers == 700
        assert report.available == 700

    def test_fd_ceiling_clamps(self, fake_rlimit):
        fake_rlimit(256, 1024)  # fd ceiling 224 below account headroom
        lam = _lambda_client(account_limit=1000)
        cw = _cloudwatch_client(current_max=0)
        workers, _ = concurrency.compute_available_workers(1700, lam, cw, "fn")
        assert workers == 256 - concurrency._FD_HEADROOM

    def test_requested_below_both_ceilings(self, fake_rlimit):
        fake_rlimit(8192, 8192)
        lam = _lambda_client(account_limit=10000)
        cw = _cloudwatch_client(current_max=0)
        workers, _ = concurrency.compute_available_workers(50, lam, cw, "fn")
        assert workers == 50

    def test_missing_account_falls_back_to_fd(self, fake_rlimit):
        fake_rlimit(256, 1024)
        lam = MagicMock()
        lam.get_account_settings.side_effect = Exception("AccessDenied")
        lam.get_function_concurrency.return_value = {}
        cw = _cloudwatch_client(current_max=0)
        workers, report = concurrency.compute_available_workers(1700, lam, cw, "fn")
        # account unknown -> FD ceiling governs
        assert report.available is None
        assert workers == 256 - concurrency._FD_HEADROOM

    def test_floored_at_one(self, fake_rlimit):
        fake_rlimit(16, 16)
        lam = _lambda_client(account_limit=100)
        cw = _cloudwatch_client(current_max=200)
        workers, _ = concurrency.compute_available_workers(1700, lam, cw, "fn")
        assert workers == 1
