"""Generic fan-out -> retry -> measured-cost dispatch loop (issue #63).

Both the spatial pipeline (today) and the temporal / cluster pipelines that
follow (#12, #20) need the same shape: hand a set of work units to a backend,
fan them out, retry the transient failures, measure cost, and report. That loop
used to live as two bespoke functions in ``runner.py`` (``_run_local`` /
``_run_lambda``). This module extracts it once behind a clean seam so every new
pipeline kind inherits local and Lambda execution for free, and a future
ray/dask/slurm backend plugs in behind the same interface.

The seam has three pieces (option (B)+(C), locked on #63):

* :class:`Executor` -- a backend. ``submit(payload) -> Future`` runs one unit;
  ``preflight(n)`` does any pre-fan-out capacity check; ``measure_cost(result)``
  turns one result into a :class:`CellCost`; ``finalize()`` runs the end-of-run
  step and returns a :class:`RunReport`; ``shutdown()`` releases resources.
* :class:`RetryPolicy` -- *how* to retry, factored out of the executor. The only
  per-backend variation is *which* exceptions are retryable, captured by the
  ``classify`` callback. Defaults :data:`LAMBDA_RETRY` / :data:`LOCAL_RETRY`.
* :func:`dispatch` -- the generic loop. It is pipeline- and backend-agnostic:
  it drives ``submit`` / ``measure_cost`` and folds each result into a
  :class:`RunReport` via the caller's ``accumulate`` callback.

``runner.py`` keeps cost *presentation* (it formats ``gb_seconds`` /
``estimated_cost_usd`` from the report); this module only returns structured
data. ``concurrency.py`` stays a helper module called from
``LambdaExecutor.preflight`` -- it is not folded into the executor.
"""

from __future__ import annotations

import time
from concurrent.futures import Future, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Structured results
# ---------------------------------------------------------------------------


@dataclass
class CellCost:
    """Measured cost of a single work unit.

    ``compute_time_s`` is the backend-reported execution time (Lambda
    ``duration_s``; 0 for the local backend, which carries no metered cost).
    ``gb_seconds`` and ``cost_usd`` are derived by the executor's pricing model
    (``compute_time_s * memory_gb`` and ``gb_seconds * price_per_gb_sec`` for
    Lambda; both 0 locally).
    """

    compute_time_s: float = 0.0
    gb_seconds: float = 0.0
    cost_usd: float = 0.0


@dataclass
class PreflightReport:
    """Outcome of an executor's pre-fan-out capacity check.

    ``workers`` is the (possibly clamped) worker count the loop should fan out
    with. ``detail`` carries backend-specific context (e.g. the Lambda
    :class:`~zagg.concurrency.ConcurrencyReport`) for presentation; the generic
    loop does not interpret it.
    """

    workers: int
    detail: Any = None


@dataclass
class RunReport:
    """Structured outcome of a dispatch run.

    The generic loop populates ``results`` (one per unit) and the rolled-up
    counters; cost is accumulated per-result via :meth:`Executor.measure_cost`.
    ``runner.py`` reads this to build the public summary dict and to print cost
    -- this module never formats or prints.
    """

    results: list[dict] = field(default_factory=list)
    cells_with_data: int = 0
    cells_error: int = 0
    total_obs: int = 0
    cost: CellCost = field(default_factory=CellCost)


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


@dataclass
class RetryPolicy:
    """How to retry a transient failure, factored out of the executor (#63).

    The only thing that varies across backends is *which* exceptions are worth
    retrying, captured by ``classify`` (return ``True`` to retry, ``False`` to
    give up immediately). ``max_attempts`` and ``backoff`` are shared mechanism.

    Parameters
    ----------
    max_attempts : int
        Total attempts, including the first. Lambda uses 3; local uses 1.
    backoff : Callable[[int], float]
        Maps a 0-based attempt index to a sleep (seconds) before the next try.
    classify : Callable[[BaseException], bool]
        Returns ``True`` when the exception is retryable. errno-24 / EMFILE is
        *not* retryable (it is run-fatal and re-raised upstream); boto3
        throttling is.
    """

    max_attempts: int
    backoff: Callable[[int], float]
    classify: Callable[[BaseException], bool]


def _no_backoff(attempt: int) -> float:
    return 0.0


def _expjitter_backoff(attempt: int) -> float:
    """Exponential backoff with sub-second jitter, matching the old loop."""
    return (2**attempt) + (time.time() % 1)


# Substrings that mark a transient client-side Lambda failure worth retrying.
# Copied verbatim from the pre-refactor ``_invoke_lambda_cell`` so the retry
# classification does not drift. errno-24 is deliberately absent: it is
# re-raised as run-fatal (see ``concurrency.raise_for_fd_exhaustion``) rather
# than retried.
_LAMBDA_RETRYABLE = (
    "TooManyRequestsException",
    "Rate exceeded",
    "Read timeout",
    "timed out",
    "UNEXPECTED_EOF",
)


def lambda_classify(exc: BaseException) -> bool:
    """True if ``exc`` is a transient Lambda failure worth retrying (boto3
    throttling, read timeouts). errno-24 is excluded -- it is run-fatal."""
    return any(sub in str(exc) for sub in _LAMBDA_RETRYABLE)


def never_classify(exc: BaseException) -> bool:
    """Retry nothing -- the local backend's failures are program errors."""
    return False


# Default policies. Lambda retries throttling/transient errors three times with
# exponential-jitter backoff; local runs each unit once (its failures are
# program errors, not transient capacity, and the old ``_run_local`` did not
# retry).
LAMBDA_RETRY = RetryPolicy(max_attempts=3, backoff=_expjitter_backoff, classify=lambda_classify)
LOCAL_RETRY = RetryPolicy(max_attempts=1, backoff=_no_backoff, classify=never_classify)


# ---------------------------------------------------------------------------
# Executor protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Executor(Protocol):
    """A backend that runs work units (the *where*, not the *what*).

    Pipeline kind (spatial morton cell vs temporal event) is orthogonal: an
    executor runs whatever ``payload`` the pipeline feeds it. Implementations
    in this module: :class:`LocalExecutor` (thread pool) and
    :class:`LambdaExecutor` (boto3 fan-out). ray/dask/slurm plug in here later.
    """

    def preflight(self, n_cells: int) -> PreflightReport:
        """Capacity check before fan-out; returns the clamped worker count."""
        ...

    def submit(self, payload: Any) -> Future:
        """Run one unit, returning a :class:`~concurrent.futures.Future`."""
        ...

    def measure_cost(self, result: dict) -> CellCost:
        """Cost of one completed unit's result dict."""
        ...

    def finalize(self) -> RunReport:
        """Run the end-of-run step; return the aggregate report."""
        ...

    def shutdown(self) -> None:
        """Release any resources (thread pool, clients)."""
        ...


# ---------------------------------------------------------------------------
# Generic dispatch loop
# ---------------------------------------------------------------------------


def dispatch(
    executor: Executor,
    payloads: list[Any],
    *,
    retry: RetryPolicy,
    accumulate: Callable[[RunReport, int, dict], None],
    on_submit_error: Callable[[BaseException], None] | None = None,
) -> RunReport:
    """Fan out ``payloads`` across ``executor``, folding results into a report.

    This is the generic loop both backends share. It is pipeline-agnostic: each
    ``payload`` is whatever the executor's :meth:`Executor.submit` understands.
    Per-result *counting* (which results count as data vs error) is the
    caller's concern -- it differs between backends, so it lives in
    ``accumulate`` rather than being baked in here.

    Parameters
    ----------
    executor : Executor
        The backend. ``preflight`` is *not* called here -- the caller runs it
        first so it can size the executor's worker pool before ``submit``.
    payloads : list
        Work units, one per ``submit``.
    retry : RetryPolicy
        The retry strategy. The in-process executors apply it inside ``submit``
        (Lambda retries transient failures; local runs once), matching the
        pre-refactor behavior so the spatial path stays byte-identical. Carried
        on the dispatch signature so a future loop-level retry (and cluster
        backends) consult one policy object without a contract change.
    accumulate : Callable[[RunReport, int, dict], None]
        Folds one result into the report: appends to ``results`` and bumps the
        ``cells_with_data`` / ``cells_error`` / ``total_obs`` counters with the
        backend's exact rules. Called with the report, the 1-based index, and
        the result dict. Cost is folded in by the loop itself (via
        :meth:`Executor.measure_cost`) before ``accumulate`` runs.
    on_submit_error : Callable[[BaseException], None], optional
        Called with an exception raised out of a future before it is re-raised,
        so the caller can convert run-fatal errors (errno-24) into actionable
        guidance.

    Returns
    -------
    RunReport
    """
    report = RunReport()
    futures: dict[Future, Any] = {executor.submit(payload): payload for payload in payloads}

    for i, future in enumerate(as_completed(futures), 1):
        try:
            result = future.result()
        except Exception as e:
            if on_submit_error is not None:
                on_submit_error(e)
            raise
        cost = executor.measure_cost(result)
        report.cost.compute_time_s += cost.compute_time_s
        report.cost.gb_seconds += cost.gb_seconds
        report.cost.cost_usd += cost.cost_usd
        accumulate(report, i, result)

    return report


# ---------------------------------------------------------------------------
# In-process executors
# ---------------------------------------------------------------------------
#
# Both wrap a ``ThreadPoolExecutor`` and a per-unit work callable. The work
# callable, the pool factory, and (for Lambda) the preflight/finalize callables
# are *injected* by ``runner.py`` rather than imported here. That keeps the
# spatial path byte-identical: ``runner`` passes references off its own module
# namespace, so the existing tests that monkeypatch ``runner._invoke_lambda_*``
# / ``runner.ThreadPoolExecutor`` / ``runner.compute_available_workers`` still
# patch the exact objects the executor calls, and dispatch.py stays free of a
# boto3 import.


class LocalExecutor:
    """Run work units in a local ``ThreadPoolExecutor`` (the trivial backend).

    ``work`` is the per-unit callable (``runner._process_and_write`` for the
    spatial path); ``submit`` hands each payload to it on the pool. Local runs
    carry no metered cost, so :meth:`measure_cost` is always zero and
    :meth:`finalize` returns an empty :class:`RunReport`.
    """

    def __init__(
        self,
        work: Callable[[Any], dict],
        *,
        max_workers: int,
        pool_factory: Callable[..., Any],
    ):
        self._work = work
        self._max_workers = max_workers
        self._pool = pool_factory(max_workers=max_workers)

    def preflight(self, n_cells: int) -> PreflightReport:
        """Local capacity is just the (already cell-clamped) worker count."""
        return PreflightReport(workers=self._max_workers)

    def submit(self, payload: Any) -> Future:
        return self._pool.submit(self._work, payload)

    def measure_cost(self, result: dict) -> CellCost:
        return CellCost()

    def finalize(self) -> RunReport:
        return RunReport()

    def shutdown(self) -> None:
        self._pool.shutdown()


# arm64 Lambda pricing, $/GB-second, and the function's memory in GB. Matches
# the constants inlined into ``_run_lambda`` before this extraction; surfaced
# here so :meth:`LambdaExecutor.measure_cost` and the runner's presentation
# read one source.
LAMBDA_MEMORY_GB = 2.0
LAMBDA_PRICE_PER_GB_SEC = 0.0000133334


class LambdaExecutor:
    """Fan out one synchronous boto3 ``invoke`` per unit (the rich backend).

    The boto3 machinery -- preflight concurrency probe, per-cell invoke with
    retry, setup/finalize invokes -- is injected by ``runner.py`` as callables
    so this module needs no boto3 import and the spatial path stays
    byte-identical (see the module note above). ``preflight`` clamps the worker
    pool to the concurrency probe's result and (re)builds the pool at the
    clamped size before fan-out.

    Parameters
    ----------
    work : Callable[[Any], dict]
        Per-cell invoke (``runner._invoke_lambda_cell`` partial). Returns the
        result dict the dispatch loop accumulates.
    preflight_fn : Callable[[int], PreflightReport]
        Runs the concurrency probe and returns the clamped worker count +
        :class:`~zagg.concurrency.ConcurrencyReport` (in ``detail``). Called by
        :meth:`preflight`.
    pool_factory : Callable[..., Any]
        ``ThreadPoolExecutor``-shaped factory (``runner.ThreadPoolExecutor``),
        sized to the clamped worker count after preflight.
    finalize_fn : Callable[[], None]
        Runs the finalize invoke (metadata consolidation). Called by
        :meth:`finalize`.
    memory_gb, price_per_gb_sec : float
        Pricing model for :meth:`measure_cost`.
    """

    def __init__(
        self,
        work: Callable[[Any], dict],
        *,
        preflight_fn: Callable[[int], PreflightReport],
        pool_factory: Callable[..., Any],
        finalize_fn: Callable[[], None],
        memory_gb: float = LAMBDA_MEMORY_GB,
        price_per_gb_sec: float = LAMBDA_PRICE_PER_GB_SEC,
    ):
        self._work = work
        self._preflight_fn = preflight_fn
        self._pool_factory = pool_factory
        self._finalize_fn = finalize_fn
        self._memory_gb = memory_gb
        self._price_per_gb_sec = price_per_gb_sec
        self._pool: Any = None

    def preflight(self, n_cells: int) -> PreflightReport:
        report = self._preflight_fn(n_cells)
        self._pool = self._pool_factory(max_workers=report.workers)
        return report

    def submit(self, payload: Any) -> Future:
        if self._pool is None:
            raise RuntimeError("LambdaExecutor.preflight() must run before submit()")
        return self._pool.submit(self._work, payload)

    def measure_cost(self, result: dict) -> CellCost:
        compute_s = result.get("lambda_duration", 0) or 0.0
        gb_seconds = compute_s * self._memory_gb
        return CellCost(
            compute_time_s=compute_s,
            gb_seconds=gb_seconds,
            cost_usd=gb_seconds * self._price_per_gb_sec,
        )

    def finalize(self) -> RunReport:
        self._finalize_fn()
        return RunReport()

    def shutdown(self) -> None:
        if self._pool is not None:
            self._pool.shutdown()
