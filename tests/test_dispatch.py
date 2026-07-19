"""Tests for the generic dispatch seam (issue #63).

Covers the :class:`~zagg.dispatch.Executor` protocol, the :class:`RetryPolicy`
defaults and classifiers, the generic :func:`dispatch` loop, and the two
in-process executors (:class:`LocalExecutor` / :class:`LambdaExecutor`). The
runner-side byte-identical behavior is pinned separately in
``tests/test_runner.py`` and ``tests/test_runner_concurrency.py``.
"""

import errno
from concurrent.futures import Future, ThreadPoolExecutor

import pytest

from zagg.dispatch import (
    LAMBDA_ARCH,
    LAMBDA_MEMORY_GB,
    LAMBDA_PRICE_PER_GB_SEC,
    LAMBDA_PRICE_PER_GB_SEC_BY_ARCH,
    LAMBDA_RETRY,
    LOCAL_RETRY,
    CellCost,
    Executor,
    LambdaExecutor,
    LocalExecutor,
    PreflightReport,
    RunReport,
    dispatch,
    lambda_classify,
    max_cost_usd,
    never_classify,
)


class TestRetryPolicy:
    def test_lambda_defaults(self):
        assert LAMBDA_RETRY.max_attempts == 3
        # Exponential-jitter backoff grows with the attempt index.
        assert LAMBDA_RETRY.backoff(2) >= 4.0
        assert LAMBDA_RETRY.classify is lambda_classify

    def test_local_defaults(self):
        assert LOCAL_RETRY.max_attempts == 1
        assert LOCAL_RETRY.backoff(0) == 0.0
        assert LOCAL_RETRY.classify is never_classify

    def test_lambda_classify_retries_throttling_not_emfile(self):
        # boto3 throttling IS retryable; errno-24 / EMFILE is NOT (it is
        # run-fatal and re-raised upstream) -- the locked rule on #63.
        assert lambda_classify(Exception("TooManyRequestsException: Rate exceeded"))
        assert lambda_classify(Exception("Read timeout on endpoint"))
        assert not lambda_classify(OSError(errno.EMFILE, "Too many open files"))
        assert not lambda_classify(Exception("some other boto failure"))

    def test_never_classify_retries_nothing(self):
        assert not never_classify(Exception("TooManyRequestsException"))
        assert not never_classify(RuntimeError("boom"))


class TestProtocolConformance:
    """Both shipped executors satisfy the runtime-checkable Executor protocol."""

    def test_local_executor_is_executor(self):
        ex = LocalExecutor(lambda p: {}, max_workers=1, pool_factory=ThreadPoolExecutor)
        assert isinstance(ex, Executor)
        ex.shutdown()

    def test_lambda_executor_is_executor(self):
        ex = LambdaExecutor(
            lambda p: {},
            preflight_fn=lambda n: PreflightReport(workers=1),
            pool_factory=ThreadPoolExecutor,
            finalize_fn=lambda: None,
        )
        assert isinstance(ex, Executor)


class TestLocalExecutor:
    def test_runs_work_and_reports_zero_cost(self):
        ex = LocalExecutor(
            lambda p: {"value": p * 2},
            max_workers=2,
            pool_factory=ThreadPoolExecutor,
        )
        assert ex.preflight(3).workers == 2
        fut = ex.submit(21)
        assert isinstance(fut, Future)
        assert fut.result() == {"value": 42}
        assert ex.measure_cost({"anything": 1}) == CellCost()
        assert ex.finalize() == RunReport()
        ex.shutdown()


class TestLambdaExecutor:
    def _make(self, **kw):
        return LambdaExecutor(
            kw.get("work", lambda p: {"lambda_duration": 0}),
            preflight_fn=kw.get("preflight_fn", lambda n: PreflightReport(workers=4)),
            pool_factory=ThreadPoolExecutor,
            finalize_fn=kw.get("finalize_fn", lambda: None),
        )

    def test_submit_before_preflight_raises(self):
        ex = self._make()
        with pytest.raises(RuntimeError, match="preflight"):
            ex.submit("x")

    def test_preflight_sizes_pool_and_returns_report(self):
        ex = self._make(preflight_fn=lambda n: PreflightReport(workers=7, detail="d"))
        report = ex.preflight(100)
        assert report.workers == 7
        assert report.detail == "d"
        # The pool is now usable.
        assert ex.submit("x").result() == {"lambda_duration": 0}
        ex.shutdown()

    def test_measure_cost_matches_arm64_pricing(self):
        ex = self._make()
        # 3 s of Lambda compute at LAMBDA_MEMORY_GB (4 GB, issue #193).
        cost = ex.measure_cost({"lambda_duration": 3.0})
        assert cost.compute_time_s == 3.0
        assert cost.gb_seconds == pytest.approx(3.0 * LAMBDA_MEMORY_GB)
        assert cost.cost_usd == pytest.approx(3.0 * LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC)

    def test_measure_cost_handles_missing_duration(self):
        ex = self._make()
        assert ex.measure_cost({}) == CellCost()

    def test_finalize_invokes_hook(self):
        called = {"n": 0}

        def _fin():
            called["n"] += 1

        ex = self._make(finalize_fn=_fin)
        ex.finalize()
        assert called["n"] == 1


class TestMaxCostUsd:
    """Pre-invoke cost ceiling math against the price table (issue #298)."""

    def test_matches_price_table(self):
        # 100 shards x 4 GB x 900 s at the arm64 rate.
        expected = 100 * LAMBDA_PRICE_PER_GB_SEC_BY_ARCH["arm64"] * 4.0 * 900
        assert max_cost_usd(100, 4.0, timeout_s=900) == pytest.approx(expected)

    def test_scales_with_memory_variant(self):
        # An 8 GB worker: variant (issue #235) doubles the 4 GB ceiling.
        four = max_cost_usd(10, 4.0, timeout_s=900)
        eight = max_cost_usd(10, 8.0, timeout_s=900)
        assert eight == pytest.approx(2 * four)

    def test_default_arch_is_the_deployed_fleet(self):
        # Only arm64 is deployed (template.yaml Architecture default); the
        # flat constant must stay an alias into the table.
        assert LAMBDA_ARCH == "arm64"
        assert LAMBDA_PRICE_PER_GB_SEC == LAMBDA_PRICE_PER_GB_SEC_BY_ARCH[LAMBDA_ARCH]
        assert max_cost_usd(1, 1.0, timeout_s=1, arch="arm64") == pytest.approx(
            LAMBDA_PRICE_PER_GB_SEC
        )

    def test_unknown_arch_raises(self):
        # Pricing must fail loudly, never silently fall back to another rate.
        with pytest.raises(KeyError):
            max_cost_usd(1, 4.0, timeout_s=900, arch="riscv")

    def test_zero_shards_costs_nothing(self):
        assert max_cost_usd(0, 4.0, timeout_s=900) == 0.0

    def test_run_report_cost_fields_default_none(self):
        # Local runs never stamp the block; the defaults must read as "no
        # metered cost", and finalize()'s empty-report contract still holds.
        report = RunReport()
        assert report.max_cost_usd is None
        assert report.estimated_cost_usd is None
        assert report.actual_cost_usd is None


class TestDispatchLoop:
    """The generic loop drives submit -> measure_cost -> accumulate per unit."""

    def _accumulate(self, report, i, result):
        report.results.append(result)
        if result.get("ok"):
            report.cells_with_data += 1
            report.total_obs += result.get("obs", 0)
        else:
            report.cells_error += 1

    def test_accumulates_results_cost_and_counts(self):
        ex = LambdaExecutor(
            lambda p: {"ok": True, "obs": p, "lambda_duration": 1.0},
            preflight_fn=lambda n: PreflightReport(workers=3),
            pool_factory=ThreadPoolExecutor,
            finalize_fn=lambda: None,
        )
        ex.preflight(3)
        report = dispatch(ex, [10, 20, 30], retry=LAMBDA_RETRY, accumulate=self._accumulate)
        assert report.cells_with_data == 3
        assert report.total_obs == 60
        assert report.cells_error == 0
        assert len(report.results) == 3
        # Cost is folded in by the loop: 3 cells x 1 s x LAMBDA_MEMORY_GB (4 GB).
        assert report.cost.compute_time_s == pytest.approx(3.0)
        assert report.cost.gb_seconds == pytest.approx(3.0 * LAMBDA_MEMORY_GB)
        ex.shutdown()

    def test_on_submit_error_runs_then_reraises(self):
        seen = {}

        def _boom(payload):
            raise OSError(errno.EMFILE, "Too many open files")

        ex = LocalExecutor(_boom, max_workers=1, pool_factory=ThreadPoolExecutor)
        ex.preflight(1)
        with pytest.raises(OSError) as exc_info:
            dispatch(
                ex,
                ["a"],
                retry=LOCAL_RETRY,
                accumulate=self._accumulate,
                on_submit_error=lambda e: seen.setdefault("errno", e.errno),
            )
        assert exc_info.value.errno == errno.EMFILE
        assert seen["errno"] == errno.EMFILE
        ex.shutdown()

    def test_local_zero_cost_does_not_perturb_report(self):
        ex = LocalExecutor(
            lambda p: {"ok": True, "obs": 5},
            max_workers=2,
            pool_factory=ThreadPoolExecutor,
        )
        ex.preflight(2)
        report = dispatch(ex, [1, 2], retry=LOCAL_RETRY, accumulate=self._accumulate)
        assert report.cells_with_data == 2
        assert report.total_obs == 10
        assert report.cost == CellCost()
        ex.shutdown()
