"""Tests for the dispatch circuit breaker (issue #652).

Pure-module tests on synthetic result streams: each breaker rule trips on its
own symptom, a trip releases the queue for refusal, and the guard paces at the
run's full worker width. No poller, no threads, no AWS.
"""

import pytest

from zagg.dispatch_guard import (
    NOT_DISPATCHED,
    RULE_STORE_FATAL,
    RULE_TIMEOUT_RATIO,
    DispatchGuard,
    FixedWindow,
    GuardConfig,
    classify_result,
)


def _ok():
    """A completed-ok result dict as the poller's ``_result`` shapes it."""
    return {
        "status_code": 200,
        "body": {"total_obs": 7, "duration_total_s": 600.0},
        "error": None,
        "lambda_duration": 600.0,
        "timeout": False,
    }


def _timeout():
    """The Event transport's drop outcome: no status object by the deadline."""
    return {
        "status_code": None,
        "body": {},
        "error": "failed-unknown: no status object within 1050s",
        "lambda_duration": 0,
        "timeout": False,
        "outcome": "failed-unknown",
    }


def _failed(error="boom", store_error_class=None, in_body=False):
    body = {"error": error}
    result = {"status_code": 500, "body": body, "error": error, "lambda_duration": 30.0}
    if store_error_class is not None:
        (body if in_body else result)["store_error_class"] = store_error_class
    return result


def _guard(workers=1000, **knobs):
    cfg = GuardConfig.from_dict({"breaker": knobs}) if knobs else None
    return DispatchGuard(cfg, workers=workers)


def _feed(guard, results):
    for r in results:
        guard.record(r)
    return guard


class TestBreaker:
    def test_timeout_ratio_over_the_window_stops_dispatching(self):
        g = _feed(_guard(), [_ok()] * 39 + [_timeout()] * 11)
        assert g.tripped == {
            "tripped": True,
            "rule": RULE_TIMEOUT_RATIO,
            "at_result": 50,
            "width": 1000,
        }
        assert g.breaker is g.tripped

    def test_timeout_ratio_at_the_threshold_does_not_trip(self):
        g = _feed(_guard(), [_ok()] * 40 + [_timeout()] * 10)
        assert g.tripped is None and g.breaker is None

    def test_timeout_ratio_is_rolling(self):
        g = _feed(_guard(), [_ok()] * 100 + [_timeout()] * 11)  # 11 of the last 50
        assert g.tripped["rule"] == RULE_TIMEOUT_RATIO and g.tripped["at_result"] == 111

    def test_the_window_is_a_breaker_knob(self):
        g = _feed(_guard(window=10), [_ok()] * 7 + [_timeout()] * 3)  # 3 of 10 > 0.20
        assert g.tripped["rule"] == RULE_TIMEOUT_RATIO and g.tripped["at_result"] == 10

    def test_slow_walls_alone_never_trip(self):
        # The ruled ``wall-margin`` rule only stopped a ramp widening; with no
        # ramp it is gone, and a slow-but-finishing run dispatches on.
        slow = [{**_ok(), "lambda_duration": 890.0} for _ in range(200)]
        assert _feed(_guard(), slow).tripped is None

    def test_store_fatal_among_the_first_twenty_stops_dispatching(self):
        g = _feed(_guard(), [_ok()] * 10 + [_failed(store_error_class="store-fatal")] * 3)
        assert g.tripped == {
            "tripped": True,
            "rule": RULE_STORE_FATAL,
            "at_result": 13,
            "width": 1000,
        }

    def test_store_fatal_after_the_first_twenty_does_not_trip(self):
        fatal = _failed(store_error_class="store-fatal")
        g = _feed(_guard(), [_ok()] * 18 + [fatal] * 2 + [_ok()] + [fatal] * 5)
        assert g.tripped is None

    def test_transient_failures_are_not_store_fatal(self):
        g = _feed(_guard(), [_failed(store_error_class="transient")] * 10)
        assert g.tripped is None

    def test_store_error_class_is_read_off_the_body_too(self):
        g = _feed(_guard(), [_failed(store_error_class="store-fatal", in_body=True)] * 3)
        assert g.tripped["rule"] == RULE_STORE_FATAL

    def test_store_error_class_is_the_spelling_pr_658_stamps(self):
        # The result's key, then the body's (PR #658's own readout order).
        fatal = {**_failed(), "store_error_class": "store-fatal"}
        assert classify_result(fatal).store_fatal
        in_body = _failed()
        in_body["body"]["store_error_class"] = "store-fatal"
        assert classify_result(in_body).store_fatal

    def test_telemetry_error_class_never_shadows_the_classifier(self):
        # ``error_class`` is telemetry's exception type (PR #665 review): a body
        # carrying it with a fatal error string still classifies store-fatal,
        # and a transient string stays transient — the value is not a class.
        fatal = _failed(error="NoSuchBucket: the bucket does not exist")
        fatal["body"]["error_class"] = "ValueError"
        assert classify_result(fatal).store_fatal
        transient = _failed(error="SlowDown: please reduce your request rate")
        transient["body"]["error_class"] = "ValueError"
        assert not classify_result(transient).store_fatal

    def test_a_pre_658_error_class_holding_a_store_class_is_honored(self):
        # The pre-#658 spelling, kept as a fallback only when it holds one of
        # the three store classes; ``store_error_class`` wins when both are set.
        legacy = _failed(error="boom")
        legacy["body"]["error_class"] = "store-fatal"
        assert classify_result(legacy).store_fatal
        both = {
            **_failed(error="boom", store_error_class="transient"),
            "error_class": "store-fatal",
        }
        assert not classify_result(both).store_fatal

    @pytest.mark.parametrize(
        "error",
        [
            "window 2018 failed: HTTP status client error (403 Forbidden)",
            "leaf column write failed: Got invalid DeleteObjects response",
            "NoSuchBucket: the specified bucket does not exist",
        ],
    )
    def test_an_unclassified_error_string_is_classified(self, error):
        g = _feed(_guard(), [_failed(error=error)] * 3)
        assert g.tripped["rule"] == RULE_STORE_FATAL

    @pytest.mark.parametrize(
        "error",
        ["SlowDown: please reduce your request rate", "HTTP status server error (503)", "boom"],
    )
    def test_transient_error_strings_do_not_count(self, error):
        g = _feed(_guard(), [_failed(error=error)] * 10)
        assert g.tripped is None

    def test_the_classifier_is_the_workers_own(self):
        # PR #658 merged: the guard classifies a status's error string with the
        # same function the worker stamps ``store_error_class`` from, so the
        # dispatcher-side verdict cannot drift from the leaf's.
        import zagg.dispatch
        import zagg.dispatch_guard as dg

        assert dg.classify_store_error is zagg.dispatch.classify_store_error
        assert dg.STORE_FATAL == zagg.dispatch.STORE_FATAL == "store-fatal"

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            # PR #658's own string cases, through the shared classifier.
            ("HTTP status client error (403 Forbidden) for url", "store-fatal"),
            ("Client error with status 400 Bad Request: no body", "store-fatal"),
            ("AccessDenied: no s3:PutObject", "store-fatal"),
            ("Client error with status 400 Bad Request: <Code>RequestTimeout</Code>", "transient"),
            ("Generic S3 error: Error parsing response", "store-fatal"),
            ("ValueError: cannot replace it in place (spec §1.5)", "store-fatal"),
            ("error decoding response body: failed to parse XML", "store-fatal"),
            ("Too Many Requests", "transient"),
            ("No granules found", "benign"),
        ],
    )
    def test_the_658_string_cases_classify_the_same_here(self, error, expected):
        from zagg.dispatch_guard import classify_store_error

        assert classify_store_error(error) == expected

    def test_a_shard_counts_one_timeout_across_its_refire(self):
        # 10 timeouts of 50 is at the threshold; the re-fired k9 dropping
        # again must not make it 11 of the last 50.
        drops = [{**_timeout(), "shard_key": f"k{i}"} for i in range(10)]
        g = _feed(_guard(), [_ok()] * 40 + drops + [{**_timeout(), "shard_key": "k9"}])
        assert g.tripped is None
        # A later ok completion for the same shard still counts as a record.
        n = len(g._records)
        g.record({**_ok(), "shard_key": "k9"})
        assert len(g._records) == n + 1

    def test_each_window_of_a_shard_counts_its_own_timeout(self):
        # The poller's ``unit`` (one status key per (shard, window)) is the
        # dedupe identity: an 11th timeout from another window of k9 trips.
        drops = [{**_timeout(), "shard_key": f"k{i}", "unit": f"k{i}/2019"} for i in range(10)]
        g = _feed(_guard(), [_ok()] * 39 + drops)
        assert g.tripped is None
        _feed(g, [{**_timeout(), "shard_key": "k9", "unit": "k9/2020"}])
        assert g.tripped["rule"] == "timeout-ratio"

    def test_a_trip_is_sticky_and_records_keep_counting(self):
        g = _feed(_guard(), [_ok()] * 39 + [_timeout()] * 11)
        verdict = dict(g.tripped)
        _feed(g, [_ok()] * 100)
        assert g.tripped == verdict and len(g._records) == 150

    def test_store_fatal_wins_when_both_rules_fire_on_one_record(self):
        # At result 10 store-fatal reaches 3 AND the timeout ratio (7/10) is
        # first evaluable: store-fatal is checked first and the verdict names it.
        fatal = _failed(store_error_class="store-fatal")
        g = _feed(_guard(window=10), [fatal] * 2 + [_timeout()] * 7)
        assert g.tripped is None
        g.record(fatal)
        assert g.tripped["rule"] == RULE_STORE_FATAL and g.tripped["at_result"] == 10


class TestRoomAndRefusal:
    def test_room_is_the_full_worker_width_minus_in_flight(self):
        g = _guard(workers=500)
        assert g.room(queued=2000, in_flight=0) == 500
        assert g.room(queued=2000, in_flight=480) == 20
        assert g.room(queued=2000, in_flight=600) == 0
        assert DispatchGuard(workers=None).room(queued=10, in_flight=5) == 10

    def test_a_trip_releases_every_queued_shard_for_refusal(self):
        g = _feed(_guard(), [_timeout()] * 50)
        assert g.room(queued=1200, in_flight=450) == 1200
        refusal = g.refusal()
        assert refusal["outcome"] == NOT_DISPATCHED
        assert refusal["status_code"] is None and refusal["body"] == {}
        assert "timeout-ratio" in refusal["error"] and "#652" in refusal["error"]

    def test_fixed_window_paces_without_recording(self):
        w = FixedWindow(3)
        w.record(_timeout())
        assert w.tripped is None and w.breaker is None
        assert w.room(queued=10, in_flight=1) == 2
        assert w.room(queued=10, in_flight=5) == 0
        assert FixedWindow(None).room(queued=10, in_flight=5) == 10


class TestClassifyResult:
    def test_ok_result(self):
        rec = classify_result(_ok())
        assert (rec.timeout, rec.store_fatal) == (False, False)

    def test_drop_is_a_timeout(self):
        assert classify_result(_timeout()).timeout

    def test_sync_transport_timeout_flag(self):
        result = {"status_code": None, "body": {}, "error": "Task timed out", "timeout": True}
        assert classify_result(result).timeout

    def test_benign_errors_are_neither(self):
        result = {
            "status_code": 200,
            "body": {},
            "error": "No granules found",
            "lambda_duration": 2.0,
        }
        rec = classify_result(result)
        assert not rec.timeout and not rec.store_fatal

    @pytest.mark.parametrize(
        "error",
        [
            "No data after filtering (e.g. HTTP status client error (404 Not Found))",
            "No data after filtering (3 reads raised; e.g. HTTP status client error "
            "(403 Forbidden))",
        ],
    )
    def test_a_benign_prefixed_error_is_not_store_fatal(self, error):
        result = {"status_code": 500, "body": {}, "error": error, "lambda_duration": 5}
        assert not classify_result(result).store_fatal
        g = _feed(_guard(), [dict(result) for _ in range(5)])
        assert g.tripped is None


class TestGuardConfig:
    def test_defaults_match_the_ruling(self):
        cfg = GuardConfig.from_dict(None)
        assert cfg == GuardConfig()
        assert cfg.as_dict() == {
            "breaker": {
                "window": 50,
                "timeout_ratio": 0.20,
                "store_fatal": 3,
                "store_fatal_first": 20,
            },
        }

    def test_overrides_and_round_trip(self):
        cfg = GuardConfig.from_dict({"breaker": {"window": 20, "timeout_ratio": 0.5}})
        assert (cfg.window, cfg.timeout_ratio, cfg.store_fatal) == (20, 0.5, 3)
        assert GuardConfig.from_dict(cfg.as_dict()) == cfg

    @pytest.mark.parametrize(
        ("block", "needle"),
        [
            # The ramp is gone from this PR (issue #652, 2026-10-14 ruling); the
            # message names what replaced it.
            (
                {"ramp": {"start": 500}},
                r"dispatch\.ramp was removed \(issue #652, 2026-10-14 ruling\): the guard paces "
                r"at the full worker width; the rolling window is breaker\.window",
            ),
            (
                {"breaker": {"wall_fraction": 0.85}},
                r"breaker\.wall_fraction was removed with the ramp \(issue #652\)",
            ),
            ({"breaker": {"begin": 1}}, "unknown key"),
            ({"canary": 8}, "unknown key"),
            ({"breaker": {"window": 0}}, "window must be an integer > 0"),
            ({"breaker": {"window": 1.5}}, "window must be an integer > 0"),
            ({"breaker": {"window": True}}, "window must be an integer > 0"),
            # nan / inf pass every range check and disable the rule (PR #665 review).
            ({"breaker": {"timeout_ratio": float("nan")}}, "timeout_ratio must be a number > 0"),
            ({"breaker": {"timeout_ratio": float("inf")}}, "timeout_ratio must be a number > 0"),
            ({"breaker": {"timeout_ratio": 1.5}}, "timeout_ratio must be < 1.0"),
            ({"breaker": {"timeout_ratio": 1.0}}, "timeout_ratio must be < 1.0"),
            ({"breaker": {"timeout_ratio": "x"}}, "timeout_ratio must be a number > 0"),
            ({"breaker": {"store_fatal": 30}}, "can never trip"),
            ({"breaker": "fast"}, "must be a mapping"),
            ({"breaker": ""}, "dispatch.breaker must be a mapping"),
            ({"breaker": False}, "dispatch.breaker must be a mapping"),
            ("fast", "dispatch must be a mapping"),
            ([("breaker", {})], "dispatch must be a mapping"),
        ],
    )
    def test_rejects_bad_blocks(self, block, needle):
        with pytest.raises(ValueError, match=needle):
            GuardConfig.from_dict(block)


class TestDescribe:
    def test_a_trip(self):
        from zagg.dispatch_guard import describe

        lines = describe(
            {"tripped": True, "rule": RULE_TIMEOUT_RATIO, "at_result": 137, "width": 1000}
        )
        assert lines == [
            "Circuit breaker TRIPPED, dispatch stopped: rule timeout-ratio at result 137, "
            "width 1000 (issue #652)",
        ]

    def test_nothing(self):
        from zagg.dispatch_guard import describe

        assert describe(None) == [] and describe(None, None) == []


class TestParseKnobs:
    def test_dotted_pairs_build_the_nested_block(self):
        from zagg.dispatch_guard import parse_knobs

        block = parse_knobs(["breaker.window=20", "breaker.timeout_ratio=0.3"])
        assert block == {"breaker": {"window": 20, "timeout_ratio": 0.3}}
        assert isinstance(block["breaker"]["window"], int)  # ints stay ints for the validator
        assert GuardConfig.from_dict(block).window == 20

    def test_nothing_means_no_knobs(self):
        from zagg.dispatch_guard import parse_knobs

        assert parse_knobs(None) is None and parse_knobs([]) is None

    @pytest.mark.parametrize(
        ("pair", "needle"),
        [
            ("breaker.window", "expects group.key=value"),
            ("window=20", "expects group.key=value"),
            ("breaker.=20", "expects group.key=value"),
            ("breaker.window=fast", "not a number"),
        ],
    )
    def test_bad_pairs_are_refused(self, pair, needle):
        from zagg.dispatch_guard import parse_knobs

        with pytest.raises(ValueError, match=needle):
            parse_knobs([pair])

    def test_unknown_keys_fall_to_the_validator(self):
        from zagg.dispatch_guard import parse_knobs

        with pytest.raises(ValueError, match="unknown key"):
            GuardConfig.from_dict(parse_knobs(["canary.size=8"]))


class TestPacing:
    def test_describe_leads_with_the_pacing_line(self):
        from zagg.dispatch_guard import PACING_GUARD, describe, pool_width_pacing

        why = pool_width_pacing("invocation", "async")
        assert why == (
            "pool width (invocation='async'): the circuit breaker acts only on "
            "invocation='event' (issue #652)"
        )
        assert describe(None, why) == [f"Dispatch pacing: {why}"]
        assert describe(None, PACING_GUARD) == [f"Dispatch pacing: {PACING_GUARD}"]
        assert describe(None, None) == []
