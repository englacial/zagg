"""Pre-flight concurrency probe for the Lambda orchestrator.

The Lambda backend fans out one synchronous ``invoke`` per cell across a
``ThreadPoolExecutor``. Two independent limits can silently cap or drop that
fan-out, and this module surfaces both *before* dispatch:

* **Client file descriptors.** Each in-flight worker holds an open socket to
  the Lambda endpoint. When concurrent workers exceed the process's open-file
  soft limit (``ulimit -n``, often 256), invokes fail with
  ``OSError: [Errno 24] Too many open files`` -- a client-side failure AWS
  never sees, so those cells drop while the run still "completes".
  :func:`fd_safe_max_workers` derives a worker ceiling from
  ``RLIMIT_NOFILE``; :func:`raise_for_fd_exhaustion` turns a raw errno-24 into
  an actionable message.

* **Account Lambda concurrency.** Saturating the account-wide concurrent
  execution pool throttles this run *and* any other Lambda activity in the
  account. :func:`compute_available_workers` reads the account ceiling
  (``lambda:GetAccountSettings``) and current usage (CloudWatch
  ``ConcurrentExecutions``), pads it, and clamps the requested worker count.

The boto3 helpers are deliberately carrier-agnostic -- they take explicit
clients and return plain numbers/dataclasses -- so a future Step Functions
task can import and call them without pulling in the in-process dispatch loop.
IAM-dependent calls degrade gracefully: if the dispatch identity lacks
``lambda:GetAccountSettings`` / ``cloudwatch:GetMetricStatistics`` /
``lambda:GetFunctionConcurrency``, the probe falls back rather than failing.
"""

import errno
import logging
import resource
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Open file descriptors held per worker beyond its endpoint socket (stdio,
# the catalog handle, boto3 metadata sockets, ...). Subtracted from the soft
# limit so the derived ceiling leaves room for the rest of the process.
_FD_HEADROOM = 32

# Account-concurrency padding: keep at least this percentage of the account
# ceiling free (absorbs the ~1 min CloudWatch lag and other processes), with a
# hard floor so small accounts still reserve a usable buffer. Settled on #28.
_CONCURRENCY_PADDING_PCT = 0.05
_CONCURRENCY_PADDING_FLOOR = 100

# CloudWatch ConcurrentExecutions read window. The metric is emitted at 1 min
# resolution; we take the Maximum over a short trailing window (settled on #28).
_CW_PERIOD_S = 60
_CW_LOOKBACK_S = 300


def fd_safe_max_workers(headroom: int = _FD_HEADROOM, soft: int | None = None) -> int:
    """Largest worker count the open-file soft limit can safely sustain.

    Each Lambda worker holds one open socket, so the number of concurrent
    workers is bounded by ``RLIMIT_NOFILE`` (the ``ulimit -n`` soft limit)
    minus headroom for the rest of the process.

    Parameters
    ----------
    headroom : int
        File descriptors to reserve for non-worker use (stdio, catalog,
        boto3 metadata sockets). Default 32.
    soft : int, optional
        The soft limit to use. Defaults to reading ``RLIMIT_NOFILE``; pass it
        to avoid a redundant read when the caller already has it.

    Returns
    -------
    int
        ``soft_limit - headroom``, floored at 1.
    """
    if soft is None:
        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    return max(1, soft - headroom)


def is_fd_exhaustion(exc: BaseException) -> bool:
    """Return True if ``exc`` (or its cause chain) is file-descriptor exhaustion.

    A client-side ``[Errno 24] Too many open files`` is what surfaces when
    concurrent workers exceed the open-file limit. botocore wraps it in a
    ``ConnectionError`` ("Could not connect to the endpoint URL"), so the raw
    ``OSError`` is usually buried in ``__cause__``/``__context__`` rather than
    the top-level exception. This walks the chain for either an ``OSError``
    with ``errno == EMFILE`` or the "too many open files" message text.
    """
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, OSError) and cur.errno == errno.EMFILE:
            return True
        if "too many open files" in str(cur).lower():
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def raise_for_fd_exhaustion(exc: BaseException, max_workers: int) -> None:
    """Re-raise file-descriptor exhaustion with actionable guidance.

    The raw error -- ``[Errno 24] Too many open files``, often wrapped by
    botocore as "Could not connect to the endpoint URL" -- reads like an AWS
    or network fault, not a local file-descriptor limit. Catch it at dispatch
    and call this so the operator knows to raise ``ulimit -n`` or lower
    ``--max-workers``. Anything that is not FD exhaustion
    (:func:`is_fd_exhaustion`) is left untouched (returns without raising).

    Parameters
    ----------
    exc : BaseException
        The caught error (the botocore/OSError, possibly wrapping the real
        ``OSError`` in its cause chain).
    max_workers : int
        The worker count in effect, for the message.

    Raises
    ------
    OSError
        A new ``EMFILE`` ``OSError`` chained to ``exc`` with guidance, when
        ``exc`` is FD exhaustion.
    """
    if not is_fd_exhaustion(exc):
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    raise OSError(
        errno.EMFILE,
        f"Too many open files: {max_workers} Lambda workers exceeded the "
        f"open-file soft limit (ulimit -n = {soft}, hard = {hard}). Cells "
        f"would be silently dropped. Raise the limit (e.g. `ulimit -n 8192`) "
        f"or lower --max-workers to <= {fd_safe_max_workers(soft=soft)}.",
    ) from exc


@dataclass
class ConcurrencyReport:
    """Pre-flight view of account Lambda concurrency headroom.

    Attributes
    ----------
    account_limit : int or None
        Account ``ConcurrentExecutions`` ceiling, or ``None`` if it could not
        be read (missing IAM / API error).
    current_concurrent : int
        Account-wide concurrent executions from CloudWatch (0 if unavailable).
    padding : int
        Slots held free below the ceiling.
    available : int or None
        ``account_limit - current_concurrent - padding`` (floored at 1), or
        ``None`` when the ceiling is unknown.
    function_reserved : int or None
        This function's reserved concurrency, if set (informational only --
        not the limiter). ``None`` if unreserved or unreadable.
    """

    account_limit: int | None
    current_concurrent: int
    padding: int
    available: int | None
    function_reserved: int | None


def _get_account_limit(lambda_client) -> int | None:
    """Account ConcurrentExecutions ceiling, or None on missing IAM/error."""
    try:
        settings = lambda_client.get_account_settings()
        return int(settings["AccountLimit"]["ConcurrentExecutions"])
    except Exception as e:
        logger.warning(
            "Could not read account concurrency ceiling (lambda:GetAccountSettings): %s",
            e,
        )
        return None


def _get_current_concurrent(cloudwatch_client) -> int:
    """Account-wide ConcurrentExecutions (Maximum over recent window), 0 on error."""
    try:
        now = datetime.now(timezone.utc)
        stats = cloudwatch_client.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="ConcurrentExecutions",
            StartTime=now - timedelta(seconds=_CW_LOOKBACK_S),
            EndTime=now,
            Period=_CW_PERIOD_S,
            Statistics=["Maximum"],
        )
        points = stats.get("Datapoints", [])
        if not points:
            return 0
        return int(max(p["Maximum"] for p in points))
    except Exception as e:
        logger.warning(
            "Could not read current concurrency (cloudwatch:GetMetricStatistics): %s",
            e,
        )
        return 0


def _get_function_reserved(lambda_client, function_name: str) -> int | None:
    """This function's reserved concurrency (informational), None if unset/error."""
    try:
        resp = lambda_client.get_function_concurrency(FunctionName=function_name)
        reserved = resp.get("ReservedConcurrentExecutions")
        return int(reserved) if reserved is not None else None
    except Exception as e:
        logger.warning(
            "Could not read function concurrency (lambda:GetFunctionConcurrency) for %s: %s",
            function_name,
            e,
        )
        return None


def probe_concurrency(
    lambda_client,
    cloudwatch_client,
    function_name: str,
    *,
    padding_pct: float = _CONCURRENCY_PADDING_PCT,
    padding_floor: int = _CONCURRENCY_PADDING_FLOOR,
) -> ConcurrencyReport:
    """Probe account Lambda concurrency headroom before fan-out.

    Reports this function's reserved concurrency (informational) and computes
    available headroom from the account ceiling minus current usage minus
    padding. Padding is ``max(padding_floor, padding_pct * account_limit)``
    (5% / floor 100 by default, settled on #28). Every AWS call degrades
    gracefully: a missing-IAM or API failure on any single read narrows the
    report (``account_limit``/``available`` become ``None``, current usage 0)
    rather than raising, so the dispatcher can fall back to the FD ceiling.

    Parameters
    ----------
    lambda_client, cloudwatch_client
        boto3 clients (``"lambda"`` / ``"cloudwatch"``).
    function_name : str
        The dispatch target, for the informational reserved-concurrency read.
    padding_pct : float
        Fraction of the ceiling to hold free. Default 0.05.
    padding_floor : int
        Minimum free slots, regardless of ``padding_pct``. Default 100.

    Returns
    -------
    ConcurrencyReport
    """
    account_limit = _get_account_limit(lambda_client)
    current = _get_current_concurrent(cloudwatch_client)
    function_reserved = _get_function_reserved(lambda_client, function_name)

    if account_limit is None:
        padding = padding_floor
        available = None
    else:
        padding = max(padding_floor, int(account_limit * padding_pct))
        available = max(1, account_limit - current - padding)

    return ConcurrencyReport(
        account_limit=account_limit,
        current_concurrent=current,
        padding=padding,
        available=available,
        function_reserved=function_reserved,
    )


def compute_available_workers(
    requested: int,
    lambda_client,
    cloudwatch_client,
    function_name: str,
    *,
    padding_pct: float = _CONCURRENCY_PADDING_PCT,
    padding_floor: int = _CONCURRENCY_PADDING_FLOOR,
) -> tuple[int, ConcurrencyReport]:
    """Clamp ``requested`` workers to FD- and account-concurrency-safe bounds.

    Combines :func:`fd_safe_max_workers` (local file-descriptor ceiling) with
    :func:`probe_concurrency` (account-wide Lambda headroom). The result is
    ``min(requested, fd_ceiling, account_available)``, where account headroom
    is only applied when it could be read (otherwise the FD ceiling governs).
    Carrier-agnostic: usable by both the in-process dispatcher and a future
    Step Functions task.

    Parameters
    ----------
    requested : int
        The caller's requested ``max_workers``.
    lambda_client, cloudwatch_client
        boto3 clients (``"lambda"`` / ``"cloudwatch"``).
    function_name : str
        Dispatch target (for the concurrency probe).
    padding_pct, padding_floor
        Forwarded to :func:`probe_concurrency`.

    Returns
    -------
    (int, ConcurrencyReport)
        The clamped worker count (floored at 1) and the probe report.
    """
    report = probe_concurrency(
        lambda_client,
        cloudwatch_client,
        function_name,
        padding_pct=padding_pct,
        padding_floor=padding_floor,
    )
    workers = min(requested, fd_safe_max_workers())
    if report.available is not None:
        workers = min(workers, report.available)
    return max(1, workers), report
