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
    BENIGN_ERRORS,
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
    StoreFatalError,
    classify_store_error,
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


class TestClassifyStoreError:
    """The leaf write seam's error classes (issue #651)."""

    def test_benign_strings_are_benign(self):
        assert [classify_store_error(e) for e in BENIGN_ERRORS] == ["benign", "benign"]

    def test_a_malformed_response_is_fatal_by_text_or_exception(self):
        # The post-mortem's class: object_store could not parse the body, so
        # obstore raised ``GenericError`` with no status to key on.
        from obstore.exceptions import GenericError

        text = "Generic S3 error: Got invalid DeleteObjects response: Error parsing XML"
        assert classify_store_error(GenericError(text)) == "store-fatal"
        assert classify_store_error(f"leaf column: GenericError: {text}") == "store-fatal"

    def test_refusals_are_fatal_by_type_status_or_credentials(self):
        from obstore.exceptions import NotFoundError, PermissionDeniedError
        from zarr.errors import ContainsGroupError

        assert classify_store_error(PermissionDeniedError("denied")) == "store-fatal"
        assert classify_store_error(NotFoundError("missing parent")) == "store-fatal"
        assert classify_store_error(ContainsGroupError("group where an array goes")) == (
            "store-fatal"
        )
        assert classify_store_error("HTTP status client error (403 Forbidden) for url") == (
            "store-fatal"
        )
        assert classify_store_error("Client error with status 400 Bad Request: no body") == (
            "store-fatal"
        )
        assert classify_store_error(RuntimeError("AccessDenied: no s3:PutObject")) == "store-fatal"
        # The write path's own contract refusal names the spec section.
        assert classify_store_error(ValueError("cannot replace it in place (spec §1.5)")) == (
            "store-fatal"
        )

    def test_a_fatal_cause_classifies_the_wrapper(self):
        # ``_LeafUnit.finish`` re-raises the temporal record's failure as a
        # ``RuntimeError`` *from* the store error: the chain carries the class.
        from obstore.exceptions import PermissionDeniedError

        try:
            try:
                raise PermissionDeniedError("denied")
            except PermissionDeniedError as e:
                raise RuntimeError("leaf temporal record failed to write") from e
        except RuntimeError as wrapped:
            assert classify_store_error(wrapped) == "store-fatal"

    def test_a_fault_raised_while_handling_a_refusal_keeps_its_class(self):
        # zarr's ``ObjectStore.get`` falls back from a suffix range inside
        # ``except NotSupportedError:``; a 503 there carries the refusal only
        # as ``__context__``, which is not a cause.
        from obstore.exceptions import GenericError, NotSupportedError

        try:
            try:
                raise NotSupportedError("suffix range not supported")
            except NotSupportedError:
                raise GenericError("non-2xx status code: 503 Service Unavailable")
        except GenericError as fallback:
            assert isinstance(fallback.__context__, NotSupportedError)
            assert classify_store_error(fallback) == "transient"

    def test_what_the_client_retried_stays_transient(self):
        from obstore.exceptions import GenericError

        retried = (
            "Error after 12 retries in 180s, max_retries:12, retry_timeout:180s, "
            "source:HTTP status server error (503 Service Unavailable) for url"
        )
        assert classify_store_error(GenericError(retried)) == "transient"
        assert classify_store_error("non-2xx status code: 429 Too Many Requests") == "transient"
        assert classify_store_error("error sending request: connection reset by peer") == (
            "transient"
        )
        assert classify_store_error(TimeoutError("timed out")) == "transient"
        # object_store 0.12's retry shape names the status after the URL.
        assert (
            classify_store_error(
                "Error performing PUT https://bucket.s3.amazonaws.com/k in 180s, after 12 retries, "
                "max_retries:12, retry_timeout:180s - Server returned non-2xx status code: "
                "503 Service Unavailable"
            )
            == "transient"
        )

    def test_a_retried_s3_code_outranks_its_400(self):
        # A stalled upload body is S3's ``400 RequestTimeout``, which botocore
        # retries; the bare 400 is still a refusal.
        assert (
            classify_store_error(
                "Client error with status 400 Bad Request: <Code>RequestTimeout</Code>"
            )
            == "transient"
        )
        assert classify_store_error("<Code>SlowDown</Code>: Please reduce your request rate") == (
            "transient"
        )
        assert classify_store_error("Client error with status 400 Bad Request") == "store-fatal"

    def test_bare_words_are_not_refusals(self):
        # ``Connection refused`` is a connection fault, and a ``KeyError``'s
        # ``not found`` is not the store's: the tokens name the refusal.
        assert (
            classify_store_error(
                "Error after 3 retries in 1s, source: error sending request: "
                "tcp connect error: Connection refused (os error 111)"
            )
            == "transient"
        )
        assert classify_store_error(ConnectionRefusedError("Connection refused")) == "transient"
        assert classify_store_error("leaf column: KeyError: variable not found in group") == (
            "transient"
        )
        assert classify_store_error("NoSuchKey: The specified key does not exist.") == (
            "store-fatal"
        )
        assert classify_store_error(ValueError("refusing to overwrite manifest.json")) == (
            "store-fatal"
        )

    def test_the_unnamed_failure_keeps_per_window_isolation(self):
        # Not a store error at all (a window's own reduce): isolated, as
        # issue #586 pinned -- fatal is opt-in by name, never the default.
        assert classify_store_error(RuntimeError("PUT failed")) == "transient"
        assert classify_store_error(MemoryError()) == "transient"
        assert classify_store_error(None) == "transient"

    def test_a_shard_key_never_reads_as_a_status(self):
        # ``_STORE_STATUS_RE`` needs the status word: a decimal key with a
        # 4xx-shaped run of digits inside it is not a refusal.
        assert classify_store_error("shard 3906256950232350729: PUT failed") == "transient"

    def test_store_fatal_error_is_a_runtime_error(self):
        assert issubclass(StoreFatalError, RuntimeError)


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

    def test_estimator_stub_returns_none(self):
        # Phase-3 interface stub (pilot-first / regression tiers): always None
        # until issues #297/#299 land the sidecar history it reads.
        from zagg.dispatch import estimate_cost_usd

        assert estimate_cost_usd() is None
        assert estimate_cost_usd({"shard_keys": [1]}, semantic_hash="abc", history=[]) is None

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
