"""Generic Lambda dispatch primitives (issue #12, Phase 3).

Three helpers extracted from ``runner._run_lambda`` so future pipeline kinds
(temporal/event in later #12 phases, and the multi-backend work in #20) share
one retry + cost + preflight implementation instead of forking the spatial
path:

* :func:`invoke_with_retry` — single Lambda invocation with retry, FunctionError
  + timeout parsing, and the file-descriptor-exhaustion guard from #28.
  Returns a backend-agnostic result dict; the caller layers on any
  domain-specific fields (e.g. ``shard_key`` for spatial).
* :func:`estimate_cost` — measured-cost math (GB-seconds → USD) keyed on
  architecture. Replaces the inline arm64 / 2 GB constants in ``_run_lambda``
  with a centralised lookup that x86_64 deployments can opt into.
* :func:`preflight_concurrency_probe` — wraps the
  :mod:`zagg.concurrency` probe + boto3 client setup so the orchestrator
  gets a clamped ``max_workers`` and a ready-to-use dispatch client in one
  call.

Each helper is a *pure refactor* of the corresponding chunk of
``_run_lambda`` / ``_invoke_lambda_cell``. The spatial path remains
byte-for-byte identical: the per-cell event payload, retry behavior, cost
output keys/values, and concurrency clamp are unchanged. Tests live in
``tests/test_dispatch.py``; the runner-side integration tests in
``tests/test_runner.py`` continue to pin event-builder behavior.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from zagg.concurrency import (
    ConcurrencyReport,
    compute_available_workers,
    raise_for_fd_exhaustion,
)

logger = logging.getLogger(__name__)


# AWS Lambda pricing per GB-second, by architecture. Matches the constant
# inlined into ``_run_lambda`` before the Phase 3 extraction.
_PRICE_PER_GB_SEC = {
    "arm64": 0.0000133334,
    "x86_64": 0.0000166667,
}

# Default retry parameters mirror the pre-extraction ``_invoke_lambda_cell``
# behavior so the spatial path stays byte-identical.
_DEFAULT_MAX_RETRIES = 3

# Substrings in a client-side exception message that mark a transient error
# the retry loop should back off on. Copied verbatim from
# ``_invoke_lambda_cell`` so the retry classification doesn't drift.
_RETRYABLE_SUBSTRINGS = (
    "TooManyRequestsException",
    "Rate exceeded",
    "Read timeout",
    "timed out",
    "UNEXPECTED_EOF",
)


def invoke_with_retry(
    lambda_client,
    function_name: str,
    event: dict,
    *,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    max_workers: int | None = None,
    wall_start: float | None = None,
) -> dict:
    """Synchronously invoke ``function_name`` with ``event`` and retry on failure.

    Returns a backend-agnostic result dict::

        {
            "status_code": int | None,
            "body": dict,            # decoded ``result["body"]`` (empty on error)
            "wall_time": float,      # seconds, including retries
            "lambda_duration": float,  # seconds, from body['duration_s'] when present
            "error": str | None,
            "retries": int,          # 0-based attempt count of the *successful* try,
                                     # or ``max_retries`` if every attempt failed
            "timeout": bool,         # last-attempt timeout flag (success path only;
                                     # the all-attempts-exhausted path omits it for
                                     # byte-compat with the pre-extraction result)
        }

    The caller layers on any domain-specific fields (e.g. ``shard_key``,
    ``granule_count`` for the spatial path). File-descriptor-exhaustion
    (errno 24) is re-raised loudly via
    :func:`zagg.concurrency.raise_for_fd_exhaustion` because it's run-fatal
    — every subsequent cell will hit it too.

    Parameters
    ----------
    lambda_client
        A boto3 Lambda client (``session.client('lambda', ...)``). Must
        accept the kwargs ``FunctionName`` / ``InvocationType`` / ``Payload``.
    function_name
        Lambda function name or ARN.
    event
        Event dict, JSON-serialized into the request payload.
    max_retries
        Total invocation attempts (default 3). The retry count in the
        result is 0-based against the *successful* attempt.
    max_workers
        Forwarded to ``raise_for_fd_exhaustion`` so the ulimit guidance can
        recommend a usable cap. Not otherwise consulted.
    wall_start
        Wall-clock start (``time.time()``-style) used to compute
        ``wall_time``. Passed by callers that want event-build cost included
        in the per-cell wall-time (the pre-extraction ``_invoke_lambda_cell``
        started the clock before building the event); defaults to *now* when
        the caller doesn't supply it.
    """
    if wall_start is None:
        wall_start = time.time()
    last_error: str | None = None
    is_timeout = False

    for attempt in range(max_retries):
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(event),
            )

            function_error = response.get("FunctionError")
            is_timeout = False
            if function_error:
                # ``response["Payload"]`` is a single-read stream; the
                # FunctionError branch consumes it here, and the success
                # branch below reads it instead. Mutually exclusive — the
                # ``if not function_error`` guard on the success read
                # enforces that.
                error_payload = response["Payload"].read().decode("utf-8")
                if "Task timed out" in error_payload:
                    is_timeout = True
                    last_error = f"Lambda timeout: {error_payload[:100]}"
                else:
                    last_error = f"Lambda error ({function_error}): {error_payload[:100]}"
                if not is_timeout:
                    # Non-timeout FunctionError: retry with a fresh request.
                    continue

            result = json.loads(response["Payload"].read()) if not function_error else {}
            try:
                body = json.loads(result.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}

            return {
                "status_code": result.get("statusCode"),
                "body": body,
                "wall_time": time.time() - wall_start,
                "lambda_duration": body.get("duration_s", 0),
                "error": last_error if function_error else body.get("error"),
                "retries": attempt,
                "timeout": is_timeout,
            }
        except Exception as e:
            # Client-side FD exhaustion is run-fatal — surface it loudly with
            # ulimit guidance rather than silently dropping the cell (#28).
            raise_for_fd_exhaustion(e, max_workers)
            last_error = str(e)
            if any(sub in last_error for sub in _RETRYABLE_SUBSTRINGS):
                time.sleep((2**attempt) + (time.time() % 1))
            else:
                break

    # All attempts exhausted. The pre-extraction failure return did NOT
    # include ``timeout`` (only the success-path return did), so omitting it
    # here keeps ``summary["results"]`` byte-identical against the previous
    # spatial path. Callers that need the flag in every result should
    # ``result.get("timeout", False)``.
    return {
        "status_code": None,
        "body": {},
        "wall_time": time.time() - wall_start,
        "lambda_duration": 0,
        "error": last_error,
        "retries": max_retries,
    }


def estimate_cost(
    lambda_compute_time_s: float,
    *,
    memory_gb: float = 2.0,
    arch: str = "arm64",
) -> dict:
    """Return the GB-second cost breakdown for a run.

    Centralises the cost math that ``_run_lambda`` used to inline as
    ``gb_seconds = total_lambda_time * 2.0`` plus a hard-coded arm64 price.
    Matches the pre-extraction numbers exactly when called with the
    defaults.

    Parameters
    ----------
    lambda_compute_time_s
        Sum of billed durations across every Lambda invocation in the run.
    memory_gb
        Lambda memory allocation in GB. The runtime size of the deployed
        function (currently 2.0 for the spatial worker).
    arch
        Lambda architecture (``"arm64"`` or ``"x86_64"``); selects the
        per-GB-second price.

    Returns
    -------
    dict
        ``{gb_seconds, price_per_gb_sec, estimated_cost_usd}``.
    """
    try:
        price_per_gb_sec = _PRICE_PER_GB_SEC[arch]
    except KeyError:
        raise ValueError(
            f"unknown Lambda arch {arch!r}; expected one of {sorted(_PRICE_PER_GB_SEC)}"
        ) from None
    gb_seconds = lambda_compute_time_s * memory_gb
    return {
        "gb_seconds": gb_seconds,
        "price_per_gb_sec": price_per_gb_sec,
        "estimated_cost_usd": gb_seconds * price_per_gb_sec,
    }


def preflight_concurrency_probe(
    session,
    function_name: str,
    *,
    region: str,
    max_workers: int,
) -> tuple[Any, int, ConcurrencyReport]:
    """Probe account capacity + build a sized dispatch client.

    Wraps the pattern the spatial runner used to inline before Phase 3:

    1. Probe ``lambda:GetAccountSettings`` + CloudWatch
       ``ConcurrentExecutions`` to find a usable worker ceiling
       (:func:`zagg.concurrency.compute_available_workers`).
    2. Construct the run's main Lambda client with
       ``max_pool_connections`` sized to the clamped worker count so
       sockets can't outrun the file-descriptor budget.

    Returns ``(lambda_client, clamped_max_workers, ConcurrencyReport)``.
    The caller chooses how to log the report (the spatial runner uses
    :func:`runner._log_concurrency_report` so the existing log shape stays
    stable).

    The probe degrades gracefully: if the dispatch identity lacks
    ``lambda:GetAccountSettings`` or CloudWatch read perms, the clamp falls
    back to the file-descriptor ceiling alone and ``ConcurrencyReport``
    flags the missing fields.

    Parameters
    ----------
    session
        A boto3 ``Session``. The probe client and the returned dispatch
        client are both created from it; callers that need to reuse a
        custom session (assumed-role creds, VPC endpoints, ...) thread it
        through here.
    function_name
        Lambda function name (passed to
        :func:`zagg.concurrency.compute_available_workers` for
        per-function reservation lookup).
    region
        AWS region for both clients.
    max_workers
        Requested worker count; the returned value is ``min(requested, clamp)``.
    """
    # boto3 / botocore live in the lambda extra; importing locally keeps
    # ``zagg.dispatch`` usable in test environments that haven't installed
    # the AWS deps and mirrors the lazy-import pattern in
    # ``zagg/__init__.py`` for ``runner``.
    from botocore.config import Config

    probe_lambda = session.client("lambda", region_name=region)
    cloudwatch_client = session.client("cloudwatch", region_name=region)
    max_workers, report = compute_available_workers(
        max_workers, probe_lambda, cloudwatch_client, function_name
    )
    boto_config = Config(
        read_timeout=900,
        connect_timeout=10,
        retries={"max_attempts": 0},
        max_pool_connections=max_workers,
    )
    lambda_client = session.client("lambda", region_name=region, config=boto_config)
    return lambda_client, max_workers, report
