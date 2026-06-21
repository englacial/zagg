"""Tests for the dispatch primitives extracted from runner._run_lambda
(issue #12, Phase 3).

Three surfaces: ``invoke_with_retry`` (Lambda invoke + retry + FD guard),
``estimate_cost`` (GB-second math), and ``preflight_concurrency_probe``
(probe + sized dispatch client). Lambda is mocked end-to-end.
"""

from __future__ import annotations

import errno
import json
import time
from unittest.mock import MagicMock

import pytest

from zagg.concurrency import ConcurrencyReport
from zagg.dispatch import (
    _PRICE_PER_GB_SEC,
    estimate_cost,
    invoke_with_retry,
    preflight_concurrency_probe,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ok_response(*, body: dict | None = None, status: int = 200):
    """Mock a clean Lambda invoke response carrying ``body`` JSON."""
    body = body or {"total_obs": 7, "duration_s": 1.25}
    payload = MagicMock()
    payload.read.return_value = json.dumps(
        {"statusCode": status, "body": json.dumps(body)}
    ).encode()
    return {"Payload": payload, "FunctionError": None}


def _function_error_response(error: str = "Unhandled", message: str = "boom"):
    payload = MagicMock()
    payload.read.return_value = message.encode()
    return {"Payload": payload, "FunctionError": error}


# ---------------------------------------------------------------------------
# invoke_with_retry
# ---------------------------------------------------------------------------


class TestInvokeWithRetry:
    def _event(self):
        return {"chunk_idx": (0,), "shard_key": 42}

    def test_happy_path_returns_decoded_body(self):
        client = MagicMock()
        client.invoke.return_value = _ok_response()
        result = invoke_with_retry(client, "process-shard", self._event())
        assert result["status_code"] == 200
        assert result["body"] == {"total_obs": 7, "duration_s": 1.25}
        assert result["lambda_duration"] == 1.25
        assert result["error"] is None
        assert result["retries"] == 0
        assert result["timeout"] is False
        assert "shard_key" not in result  # dispatch returns generic fields only

    def test_event_serialised_into_payload(self):
        client = MagicMock()
        client.invoke.return_value = _ok_response()
        ev = {"chunk_idx": [0, 1], "store_path": "s3://x/y.zarr"}
        invoke_with_retry(client, "fn", ev)
        sent = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert sent == ev
        assert client.invoke.call_args.kwargs["FunctionName"] == "fn"
        assert client.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"

    def test_function_error_non_timeout_retries(self):
        client = MagicMock()
        client.invoke.side_effect = [
            _function_error_response(error="Unhandled", message="first failure"),
            _ok_response(),
        ]
        result = invoke_with_retry(client, "fn", self._event())
        assert client.invoke.call_count == 2
        assert result["status_code"] == 200
        assert result["retries"] == 1  # succeeded on the 2nd attempt
        assert result["error"] is None

    def test_function_error_timeout_does_not_retry(self):
        client = MagicMock()
        client.invoke.return_value = _function_error_response(
            error="Unhandled", message="Task timed out after 900s"
        )
        result = invoke_with_retry(client, "fn", self._event())
        # Single attempt; the loop falls through to the final ``return`` once
        # the timeout flag is set (matches the pre-extraction behavior).
        assert client.invoke.call_count == 1
        assert result["status_code"] is None
        assert result["timeout"] is True
        assert "timeout" in result["error"].lower()

    def test_all_attempts_exhausted_returns_failure_dict(self):
        client = MagicMock()
        client.invoke.return_value = _function_error_response(
            error="Unhandled", message="never works"
        )
        result = invoke_with_retry(client, "fn", self._event(), max_retries=2)
        assert client.invoke.call_count == 2
        # Failure schema pinned end-to-end: every field the pre-extraction
        # ``_invoke_lambda_cell`` failure return carried (minus the
        # spatial-only ``shard_key`` / ``granule_count``). ``timeout`` is
        # deliberately absent — see the byte-compat note in dispatch.py.
        assert set(result) == {
            "status_code",
            "body",
            "wall_time",
            "lambda_duration",
            "error",
            "retries",
        }
        assert result["status_code"] is None
        assert result["body"] == {}
        assert result["retries"] == 2
        assert result["lambda_duration"] == 0
        assert "never works" in result["error"]
        assert isinstance(result["wall_time"], float)

    def test_wall_start_passthrough_includes_pre_call_cost(self):
        # When the caller times event construction, ``wall_start`` is taken
        # before the call and reaches the result unchanged. The
        # pre-extraction _invoke_lambda_cell relied on this.
        client = MagicMock()
        client.invoke.return_value = _ok_response()
        # Anchor wall_start in the distant past; the returned wall_time
        # should reflect that.
        result = invoke_with_retry(
            client, "fn", self._event(), wall_start=time.time() - 100.0
        )
        assert result["wall_time"] >= 100.0

    def test_emfile_is_reraised_with_ulimit_guidance(self):
        client = MagicMock()
        client.invoke.side_effect = OSError(errno.EMFILE, "Too many open files")
        with pytest.raises(OSError) as exc_info:
            invoke_with_retry(client, "fn", self._event(), max_workers=900)
        assert exc_info.value.errno == errno.EMFILE
        assert "ulimit -n" in str(exc_info.value)

    def test_wrapped_emfile_reraised(self):
        # Botocore sometimes wraps EMFILE inside a ConnectionError.
        inner = OSError(errno.EMFILE, "Too many open files")
        wrapped = ConnectionError("Could not connect to the endpoint URL")
        wrapped.__cause__ = inner
        client = MagicMock()
        client.invoke.side_effect = wrapped
        with pytest.raises(OSError) as exc_info:
            invoke_with_retry(client, "fn", self._event())
        assert exc_info.value.errno == errno.EMFILE

    def test_retryable_client_exception_sleeps_and_retries(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr("zagg.dispatch.time.sleep", lambda s: sleeps.append(s))
        client = MagicMock()
        client.invoke.side_effect = [
            RuntimeError("TooManyRequestsException"),
            _ok_response(),
        ]
        result = invoke_with_retry(client, "fn", self._event())
        assert client.invoke.call_count == 2
        assert result["status_code"] == 200
        assert sleeps  # backoff was applied between attempts

    def test_unretryable_client_exception_breaks_out(self):
        client = MagicMock()
        client.invoke.side_effect = RuntimeError("AccessDeniedException: nope")
        result = invoke_with_retry(client, "fn", self._event(), max_retries=3)
        assert client.invoke.call_count == 1  # broke out, no further attempts
        assert result["status_code"] is None
        assert "AccessDeniedException" in result["error"]

    def test_malformed_body_json_treated_as_empty(self):
        client = MagicMock()
        payload = MagicMock()
        payload.read.return_value = json.dumps({"statusCode": 200, "body": "not json"}).encode()
        client.invoke.return_value = {"Payload": payload, "FunctionError": None}
        result = invoke_with_retry(client, "fn", self._event())
        assert result["status_code"] == 200
        assert result["body"] == {}
        assert result["lambda_duration"] == 0


# ---------------------------------------------------------------------------
# estimate_cost
# ---------------------------------------------------------------------------


class TestEstimateCost:
    def test_defaults_match_pre_extraction_constants(self):
        # Pre-Phase-3 ``_run_lambda`` hard-coded arm64 + 2 GB. Same inputs
        # must produce the same numbers.
        cost = estimate_cost(76048.0)  # the post-refactor reading from #30
        assert cost["gb_seconds"] == pytest.approx(76048.0 * 2.0)
        assert cost["price_per_gb_sec"] == 0.0000133334
        assert cost["estimated_cost_usd"] == pytest.approx(76048.0 * 2.0 * 0.0000133334)

    def test_x86_uses_higher_price(self):
        arm = estimate_cost(100.0, memory_gb=1.0, arch="arm64")
        x86 = estimate_cost(100.0, memory_gb=1.0, arch="x86_64")
        assert x86["price_per_gb_sec"] > arm["price_per_gb_sec"]
        assert x86["estimated_cost_usd"] > arm["estimated_cost_usd"]
        # gb_seconds doesn't depend on arch.
        assert arm["gb_seconds"] == x86["gb_seconds"] == 100.0

    def test_unknown_arch_raises(self):
        with pytest.raises(ValueError, match="unknown Lambda arch"):
            estimate_cost(100.0, arch="bogus")

    def test_zero_compute_time_yields_zero_cost(self):
        cost = estimate_cost(0.0)
        assert cost["gb_seconds"] == 0.0
        assert cost["estimated_cost_usd"] == 0.0

    def test_keys_stable_for_summary_dict(self):
        # ``_run_lambda`` splats the result into the summary; pin the keys
        # so a future field rename doesn't silently break downstream tools.
        cost = estimate_cost(1.0)
        assert set(cost) == {"gb_seconds", "price_per_gb_sec", "estimated_cost_usd"}

    def test_supported_archs_pin(self):
        # Locks the public table so a missing arch is caught here, not in
        # production when a Lambda is misconfigured.
        assert set(_PRICE_PER_GB_SEC) == {"arm64", "x86_64"}


# ---------------------------------------------------------------------------
# preflight_concurrency_probe
# ---------------------------------------------------------------------------


def _make_session_factory(probe_max: int = 64):
    """Build a fake boto3 Session whose Lambda client returns benign data."""
    session = MagicMock()
    probe_client = MagicMock()
    probe_client.get_account_settings.return_value = {
        "AccountLimit": {"ConcurrentExecutions": 1000, "UnreservedConcurrentExecutions": 1000},
    }
    probe_client.get_function_concurrency.return_value = {}
    cloudwatch = MagicMock()
    cloudwatch.get_metric_statistics.return_value = {"Datapoints": []}
    dispatch_client = MagicMock()

    def _client(svc, region_name=None, config=None):
        if svc == "cloudwatch":
            return cloudwatch
        # Two lambda clients are built: the small probe one (no Config), then
        # the dispatch one (with Config). Distinguish them.
        if config is None:
            return probe_client
        # Capture the config so tests can pin the pool size.
        dispatch_client._config = config
        dispatch_client._region = region_name
        return dispatch_client

    session.client.side_effect = _client
    return session, probe_client, cloudwatch, dispatch_client


class TestPreflightConcurrencyProbe:
    def test_returns_clamped_max_workers_and_report(self, monkeypatch):
        captured = {}

        def fake_compute(requested, *args, **kwargs):
            captured["requested"] = requested
            return 64, ConcurrencyReport(
                account_limit=1000,
                current_concurrent=100,
                padding=100,
                available=64,
                function_reserved=None,
            )

        monkeypatch.setattr("zagg.dispatch.compute_available_workers", fake_compute)
        session, *_ = _make_session_factory()
        client, max_workers, report = preflight_concurrency_probe(
            session,
            "process-shard",
            region="us-west-2",
            max_workers=1700,
        )
        assert captured["requested"] == 1700
        assert max_workers == 64
        assert isinstance(report, ConcurrencyReport)
        assert report.available == 64
        assert client is not None

    def test_dispatch_client_pool_sized_to_clamp(self, monkeypatch):
        monkeypatch.setattr(
            "zagg.dispatch.compute_available_workers",
            lambda r, *a, **k: (64, ConcurrencyReport(1000, 100, 100, 64, None)),
        )
        session, _, _, dispatch_client = _make_session_factory()
        client, _, _ = preflight_concurrency_probe(
            session,
            "fn",
            region="us-east-1",
            max_workers=1700,
        )
        # The dispatch client's botocore.Config was built with the clamped
        # pool size, not the raw requested value.
        cfg = dispatch_client._config
        assert cfg.max_pool_connections == 64
        assert dispatch_client._region == "us-east-1"

    def test_two_lambda_clients_built_probe_then_dispatch(self, monkeypatch):
        monkeypatch.setattr(
            "zagg.dispatch.compute_available_workers",
            lambda r, *a, **k: (32, ConcurrencyReport(1000, 100, 100, 32, None)),
        )
        session, *_ = _make_session_factory()
        preflight_concurrency_probe(
            session,
            "fn",
            region="us-west-2",
            max_workers=100,
        )
        # session.client called with: lambda (probe, no config), cloudwatch,
        # lambda (dispatch, with config).
        services = [c.args[0] for c in session.client.call_args_list]
        assert services == ["lambda", "cloudwatch", "lambda"]
