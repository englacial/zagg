"""Tests for the dispatch ramp + circuit breaker (issue #652).

Pure-module tests on synthetic result streams: the ramp doubles on flat write
timings and holds on slow ones, each breaker rule trips (or holds) on its own
symptom, a trip releases the queue for refusal, and the width never exceeds
the cap or the run's ``workers``. No poller, no threads, no AWS.
"""

import pytest

from zagg.dispatch_guard import (
    NOT_DISPATCHED,
    RULE_STORE_FATAL,
    RULE_TIMEOUT_RATIO,
    RULE_WALL_MARGIN,
    DispatchGuard,
    FixedWindow,
    GuardConfig,
    _classify_fallback,
    classify_result,
)

TIMEOUT_S = 900.0


def _ok(write=100.0, wall=600.0, **timings):
    """A completed-ok result dict as the poller's ``_result`` shapes it."""
    phases = {"read": 50.0, "aggregate": 200.0, "write": write / 2, "column": write / 2}
    phases.update(timings)
    return {
        "status_code": 200,
        "body": {"total_obs": 7, "duration_total_s": wall, "phase_timings": phases},
        "error": None,
        "lambda_duration": wall,
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


def _failed(error="boom", error_class=None, in_body=False):
    body = {"error": error}
    result = {"status_code": 500, "body": body, "error": error, "lambda_duration": 30.0}
    if error_class is not None:
        (body if in_body else result)["error_class"] = error_class
    return result


def _guard(workers=None, **knobs):
    cfg = GuardConfig.from_dict(knobs) if knobs else None
    return DispatchGuard(cfg, workers=workers, function_timeout_s=TIMEOUT_S)


def _feed(guard, results):
    for r in results:
        guard.record(r)
    return guard


class TestRamp:
    def test_starts_at_start_and_doubles_once_the_first_window_is_flat(self):
        g = _guard()
        assert g.width == 500 and g.ramp == [{"width": 500, "at_result": 0}]
        _feed(g, [_ok()] * 49)
        assert g.width == 500  # one short of the window
        g.record(_ok())
        assert g.width == 1000
        assert g.ramp == [{"width": 500, "at_result": 0}, {"width": 1000, "at_result": 50}]

    def test_no_third_step_past_the_cap(self):
        g = _feed(_guard(), [_ok()] * 500)
        assert g.width == 1000 and len(g.ramp) == 2

    def test_never_above_the_runs_workers(self):
        g = _feed(_guard(workers=700), [_ok()] * 100)
        assert g.width == 700 and g.ramp[-1]["width"] == 700
        assert _guard(workers=300).width == 300  # start clamps too

    def test_holds_on_slow_writes_then_resumes_when_they_recover(self):
        # Three steps so the tolerance gate is exercised against a baseline
        # that is not the samples under test: 250 -> 500 -> 1000.
        g = _guard(ramp={"start": 250})
        _feed(g, [_ok(write=100.0)] * 50)
        assert g.width == 500
        _feed(g, [_ok(write=130.0)] * 50)  # 1.3x the baseline: over tolerance
        assert g.width == 500
        _feed(g, [_ok(write=130.0)] * 50)
        assert g.width == 500  # still slow, still held
        _feed(g, [_ok(write=115.0)] * 50)  # within 1.2x: the window at 500 is flat
        assert g.width == 1000
        assert [s["width"] for s in g.ramp] == [250, 500, 1000]

    def test_a_window_at_exactly_the_tolerance_widens(self):
        g = _guard(ramp={"start": 250})
        _feed(g, [_ok(write=100.0)] * 50)
        _feed(g, [_ok(write=120.0)] * 50)  # exactly 1.2x the baseline
        assert g.width == 1000

    def test_the_window_at_the_new_width_starts_fresh(self):
        # After widening, the results completed at the OLD width are not the
        # evidence for the next step: 50 new ones have to land first.
        g = _guard(ramp={"start": 250})
        _feed(g, [_ok()] * 50)
        assert g.width == 500
        _feed(g, [_ok()] * 49)
        assert g.width == 500
        g.record(_ok())
        assert g.width == 1000

    def test_results_fired_at_the_old_width_do_not_advance_the_step(self):
        # Stamped results gate on the width they were dispatched at: the
        # 250-wide wave's stragglers completing after the widen are not
        # evidence for 500 -> 1000.
        g = _guard(ramp={"start": 250})
        _feed(g, [{**_ok(), "dispatch_width": 250} for _ in range(50)])
        assert g.width == 500
        _feed(g, [{**_ok(), "dispatch_width": 250} for _ in range(100)])
        assert g.width == 500
        _feed(g, [{**_ok(), "dispatch_width": 500} for _ in range(49)])
        assert g.width == 500
        g.record({**_ok(), "dispatch_width": 500})
        assert g.width == 1000 and g.ramp[-1]["at_result"] == 200

    def test_results_without_timings_never_widen(self):
        results = [_ok() for _ in range(80)]
        for r in results:
            r["body"].pop("phase_timings")
        g = _feed(_guard(), results)
        assert g.width == 500 and len(g.ramp) == 1

    def test_timeouts_carry_no_write_sample(self):
        # 10 timeouts in the first 50 (ratio 0.20, not over): no trip, and the
        # ramp waits for 50 real samples.
        g = _feed(_guard(), [_timeout()] * 10 + [_ok()] * 40)
        assert g.tripped is None and g.width == 500
        _feed(g, [_ok()] * 10)
        assert g.width == 1000


class TestBreaker:
    def test_timeout_ratio_over_the_window_stops_dispatching(self):
        g = _feed(_guard(), [_ok()] * 39 + [_timeout()] * 11)
        assert g.tripped == {
            "tripped": True,
            "rule": RULE_TIMEOUT_RATIO,
            "at_result": 50,
            "width": 500,
        }
        assert g.breaker is g.tripped

    def test_timeout_ratio_at_the_threshold_does_not_trip(self):
        g = _feed(_guard(), [_ok()] * 40 + [_timeout()] * 10)
        assert g.tripped is None

    def test_timeout_ratio_is_rolling(self):
        g = _feed(_guard(), [_ok()] * 100)
        assert g.width == 1000
        _feed(g, [_timeout()] * 11)  # 11 of the last 50
        assert g.tripped["rule"] == RULE_TIMEOUT_RATIO
        assert g.tripped["at_result"] == 111 and g.tripped["width"] == 1000

    def test_wall_margin_holds_widening_but_keeps_dispatching(self):
        g = _feed(_guard(), [_ok(wall=800.0)] * 50)  # 800 > 0.85 * 900 = 765
        assert g.tripped is None and g.width == 500
        assert g.breaker == {
            "tripped": False,
            "rule": RULE_WALL_MARGIN,
            "at_result": 50,
            "width": 500,
        }
        assert g.room(queued=300, in_flight=200) == 300  # still dispatching

    def test_wall_margin_clears_when_the_window_recovers(self):
        g = _feed(_guard(), [_ok(wall=800.0)] * 50)
        _feed(g, [_ok(wall=600.0)] * 50)
        assert g.width == 1000
        assert g.breaker["rule"] == RULE_WALL_MARGIN  # the hold stays on record

    def test_store_fatal_among_the_first_twenty_stops_dispatching(self):
        g = _feed(_guard(), [_ok()] * 10 + [_failed(error_class="store-fatal")] * 3)
        assert g.tripped == {
            "tripped": True,
            "rule": RULE_STORE_FATAL,
            "at_result": 13,
            "width": 500,
        }

    def test_store_fatal_after_the_first_twenty_does_not_trip(self):
        fatal = _failed(error_class="store-fatal")
        g = _feed(_guard(), [_ok()] * 18 + [fatal] * 2 + [_ok()] + [fatal] * 5)
        assert g.tripped is None

    def test_transient_failures_are_not_store_fatal(self):
        g = _feed(_guard(), [_failed(error_class="transient")] * 10)
        assert g.tripped is None

    def test_error_class_is_read_off_the_body_too(self):
        g = _feed(_guard(), [_failed(error_class="store-fatal", in_body=True)] * 3)
        assert g.tripped["rule"] == RULE_STORE_FATAL

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

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            # PR #658's own string cases, so the fallback cannot drift from it.
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
    def test_the_fallback_mirrors_the_658_string_path(self, error, expected):
        assert _classify_fallback(error) == expected

    def test_a_shard_counts_one_timeout_across_its_refire(self):
        # 10 timeouts of 50 is at the threshold; the re-fired k9 dropping
        # again must not make it 11 of the last 50.
        drops = [{**_timeout(), "shard_key": f"k{i}"} for i in range(10)]
        g = _feed(_guard(), [_ok()] * 40 + drops + [{**_timeout(), "shard_key": "k9"}])
        assert g.tripped is None
        # A later ok completion for the same shard still counts: the 50th
        # write sample lands at result 60, not 61.
        _feed(g, [{**_ok(), "shard_key": "k9"}] + [_ok()] * 9)
        assert g.width == 1000 and g.ramp[-1]["at_result"] == 60

    def test_a_trip_is_sticky_and_records_keep_counting(self):
        g = _feed(_guard(), [_ok()] * 39 + [_timeout()] * 11)
        verdict = dict(g.tripped)
        _feed(g, [_ok()] * 100)
        assert g.tripped == verdict and g.width == 500 and len(g.ramp) == 1

    def test_store_fatal_wins_when_both_rules_fire_on_one_record(self):
        # At result 10 store-fatal reaches 3 AND the timeout ratio (7/10) is
        # first evaluable: store-fatal is checked first and the verdict names it.
        fatal = _failed(error_class="store-fatal")
        g = _feed(_guard(ramp={"window": 10}), [fatal] * 2 + [_timeout()] * 7)
        assert g.tripped is None
        g.record(fatal)
        assert g.tripped["rule"] == RULE_STORE_FATAL and g.tripped["at_result"] == 10

    def test_wall_at_exactly_the_margin_does_not_hold(self):
        g = _feed(_guard(), [_ok(wall=765.0)] * 50)  # 0.85 * 900, not over it
        assert g.breaker is None and g.width == 1000


class TestRoomAndRefusal:
    def test_room_is_width_minus_in_flight(self):
        g = _guard()
        assert g.room(queued=2000, in_flight=0) == 500
        assert g.room(queued=2000, in_flight=480) == 20
        assert g.room(queued=2000, in_flight=600) == 0

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
        assert w.tripped is None
        assert w.room(queued=10, in_flight=1) == 2
        assert w.room(queued=10, in_flight=5) == 0
        assert FixedWindow(None).room(queued=10, in_flight=5) == 10


class TestClassifyResult:
    def test_ok_result(self):
        rec = classify_result(_ok(write=100.0, wall=600.0), function_timeout_s=TIMEOUT_S)
        assert (rec.wall_s, rec.write_s, rec.timeout, rec.store_fatal) == (
            600.0,
            100.0,
            False,
            False,
        )

    def test_icechunk_rides_the_write_phase(self):
        rec = classify_result(_ok(write=100.0, icechunk=40.0), function_timeout_s=TIMEOUT_S)
        assert rec.write_s == 140.0

    def test_drop_is_a_timeout_priced_at_the_function_timeout(self):
        rec = classify_result(_timeout(), function_timeout_s=TIMEOUT_S)
        assert rec.timeout and rec.wall_s == TIMEOUT_S and rec.write_s is None

    def test_sync_transport_timeout_flag(self):
        result = {"status_code": None, "body": {}, "error": "Task timed out", "timeout": True}
        assert classify_result(result, function_timeout_s=TIMEOUT_S).timeout

    def test_benign_errors_are_neither(self):
        result = {
            "status_code": 200,
            "body": {},
            "error": "No granules found",
            "lambda_duration": 2.0,
        }
        rec = classify_result(result, function_timeout_s=TIMEOUT_S)
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
        assert not classify_result(result, function_timeout_s=TIMEOUT_S).store_fatal
        g = _feed(_guard(), [dict(result) for _ in range(5)])
        assert g.tripped is None


class TestGuardConfig:
    def test_defaults_match_the_ruling(self):
        cfg = GuardConfig.from_dict(None)
        assert cfg == GuardConfig()
        assert cfg.as_dict() == {
            "ramp": {"start": 500, "factor": 2, "cap": 1000, "window": 50, "tolerance": 1.2},
            "breaker": {
                "timeout_ratio": 0.20,
                "wall_fraction": 0.85,
                "store_fatal": 3,
                "store_fatal_first": 20,
            },
        }

    def test_overrides_and_round_trip(self):
        block = {"ramp": {"start": 200, "cap": 800}, "breaker": {"timeout_ratio": 0.5}}
        cfg = GuardConfig.from_dict(block)
        assert (cfg.start, cfg.cap, cfg.timeout_ratio, cfg.window) == (200, 800, 0.5, 50)
        assert GuardConfig.from_dict(cfg.as_dict()) == cfg

    @pytest.mark.parametrize(
        ("block", "needle"),
        [
            ({"ramp": {"begin": 1}}, "unknown key"),
            ({"canary": 8}, "unknown key"),
            ({"ramp": {"start": 0}}, "start must be an integer > 0"),
            ({"ramp": {"start": 1.5}}, "start must be an integer > 0"),
            ({"ramp": {"window": True}}, "window must be an integer > 0"),
            ({"ramp": {"tolerance": 0.9}}, "tolerance must be >= 1.0"),
            ({"ramp": {"start": 600, "cap": 500}}, "below ramp.start"),
            ({"breaker": {"timeout_ratio": 1.5}}, "timeout_ratio must be < 1.0"),
            ({"breaker": {"timeout_ratio": 1.0}}, "timeout_ratio must be < 1.0"),
            ({"breaker": {"wall_fraction": 1.0}}, "wall_fraction must be < 1.0"),
            ({"breaker": {"wall_fraction": "x"}}, "wall_fraction must be a number > 0"),
            ({"breaker": {"store_fatal": 30}}, "can never trip"),
            ({"breaker": "fast"}, "must be a mapping"),
            ({"ramp": []}, "dispatch.ramp must be a mapping"),
            ({"ramp": 0}, "dispatch.ramp must be a mapping"),
            ({"breaker": ""}, "dispatch.breaker must be a mapping"),
            ({"breaker": False}, "dispatch.breaker must be a mapping"),
            ("fast", "dispatch must be a mapping"),
            ([("ramp", {})], "dispatch must be a mapping"),
        ],
    )
    def test_rejects_bad_blocks(self, block, needle):
        with pytest.raises(ValueError, match=needle):
            GuardConfig.from_dict(block)
