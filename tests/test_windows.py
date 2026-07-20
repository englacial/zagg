"""Window primitives for morton-hive/2 — issue #246 phase 1.

Golden vectors are pinned to the FROZEN convention table on the mortie spec
page (https://github.com/espg/mortie/issues/62#issuecomment-4986809092):
label grammar, example leaves, UTC half-open boundaries, split-on-first-``_``
parse rule, and the lexicographic = chronological ordering property.
"""

import random
from datetime import datetime, timedelta, timezone

import pytest

from zagg import windows as w

UTC = timezone.utc


def _dt(*args):
    return datetime(*args, tzinfo=UTC)


# ── schedules ────────────────────────────────────────────────────────────────


class TestSchedules:
    def test_implemented_set(self):
        assert w.SCHEDULES == ("none", "yearly", "monthly", "daily", "explicit")
        for s in w.SCHEDULES:
            assert w.check_schedule(s) == s

    def test_quarterly_is_reserved_not_implemented(self):
        # Grammar-reserved on the spec page (YYYYQ[1-4]); this round rejects it
        # pointedly instead of half-supporting the grammar.
        with pytest.raises(ValueError, match="reserved"):
            w.check_schedule("quarterly")
        with pytest.raises(ValueError, match="reserved"):
            w.window_label(_dt(2025, 7, 1), "quarterly")
        with pytest.raises(ValueError, match="reserved"):
            w.windows_intersecting(_dt(2025, 1, 1), _dt(2025, 2, 1), "quarterly")

    def test_unknown_schedule_rejected(self):
        with pytest.raises(ValueError, match="unknown window schedule"):
            w.check_schedule("weekly")


# ── golden vectors: the frozen mortie table ──────────────────────────────────


class TestGoldenVectors:
    """One row per spec-table row: label grammar, example leaf, window range."""

    def test_yearly(self):
        # | yearly | YYYY | -31123_2025.zarr | [2025-01-01T00:00Z, 2026-01-01T00:00Z) |
        assert w.window_label(_dt(2025, 6, 15, 12), "yearly") == "2025"
        assert w.window_range("2025", "yearly") == (_dt(2025, 1, 1), _dt(2026, 1, 1))
        assert w.leaf_name("-31123", "2025") == "-31123_2025.zarr"
        # Width-valid but not a real year: the raise comes from datetime(), not
        # the regex (calendar validity is deferred to window_range).
        with pytest.raises(ValueError):
            w.window_range("0000", "yearly")

    def test_monthly(self):
        # | monthly | YYYYMM | -31123_202511.zarr | calendar month |
        assert w.window_label(_dt(2025, 11, 3), "monthly") == "202511"
        assert w.window_range("202511", "monthly") == (_dt(2025, 11, 1), _dt(2025, 12, 1))
        assert w.leaf_name("-31123", "202511") == "-31123_202511.zarr"
        # December rolls the year.
        assert w.window_range("202512", "monthly") == (_dt(2025, 12, 1), _dt(2026, 1, 1))
        # Width-valid but out-of-range month: raised by datetime(), not the regex.
        with pytest.raises(ValueError):
            w.window_range("202513", "monthly")
        with pytest.raises(ValueError):
            w.window_range("202500", "monthly")

    def test_daily(self):
        # | daily | YYYYMMDD | -31123_20251103.zarr | calendar day |
        assert w.window_label(_dt(2025, 11, 3, 23, 59, 59), "daily") == "20251103"
        assert w.window_range("20251103", "daily") == (_dt(2025, 11, 3), _dt(2025, 11, 4))
        assert w.leaf_name("-31123", "20251103") == "-31123_20251103.zarr"
        # Leap day decodes; a non-date of the right width does not.
        assert w.window_range("20240229", "daily")[0] == _dt(2024, 2, 29)
        with pytest.raises(ValueError):
            w.window_range("20230229", "daily")

    def test_none_bare_leaf(self):
        # | none | (no label; bare name) | -31123.zarr |
        assert w.leaf_name("-31123") == "-31123.zarr"
        assert w.split_leaf_name("-31123.zarr") == ("-31123", None)

    def test_explicit_opaque_label(self):
        # | explicit list | opaque, [0-9A-Za-z-]{1,32} | -31123_melt-2019.zarr |
        declared = [{"label": "melt-2019", "start": "2019-06-01", "end": "2019-09-01"}]
        assert w.leaf_name("-31123", "melt-2019") == "-31123_melt-2019.zarr"
        assert w.window_range("melt-2019", "explicit", declared) == (
            _dt(2019, 6, 1),
            _dt(2019, 9, 1),
        )
        # Labels are opaque: undeclared labels never decode.
        with pytest.raises(ValueError, match="not declared"):
            w.window_range("melt-2020", "explicit", declared)


# ── label grammar ────────────────────────────────────────────────────────────


class TestLabelGrammar:
    @pytest.mark.parametrize(
        "label,schedule",
        [
            ("2025", "yearly"),
            ("202511", "monthly"),
            ("20251103", "daily"),
            ("melt-2019", "explicit"),
            ("a", "explicit"),
            ("A" * 32, "explicit"),
        ],
    )
    def test_valid(self, label, schedule):
        assert w.validate_label(label, schedule) == label

    @pytest.mark.parametrize(
        "label,schedule",
        [
            ("25", "yearly"),  # wrong width
            ("2025-11", "monthly"),  # hyphen in a generative label
            ("2025", "monthly"),  # wrong width for the schedule
            ("melt_2019", "explicit"),  # underscore excluded by construction
            ("", "explicit"),  # empty
            ("A" * 33, "explicit"),  # too long
            ("melt 2019", "explicit"),  # space
        ],
    )
    def test_invalid(self, label, schedule):
        with pytest.raises(ValueError, match="grammar"):
            w.validate_label(label, schedule)


# ── leaf-name split (frozen parse rule) ──────────────────────────────────────


class TestLeafNameSplit:
    def test_split_on_first_underscore(self):
        assert w.split_leaf_name("-31123_2025.zarr") == ("-31123", "2025")
        assert w.split_leaf_name("41123_melt-2019.zarr") == ("41123", "melt-2019")

    def test_round_trip(self):
        for full_id, window in [("-31123", "2025"), ("41123", "melt-2019"), ("-5112333", None)]:
            assert w.split_leaf_name(w.leaf_name(full_id, window)) == (full_id, window)

    def test_rejects_non_zarr(self):
        with pytest.raises(ValueError, match="leaf zarr"):
            w.split_leaf_name("-31123_2025")

    def test_rejects_malformed_window(self):
        # A second underscore lands IN the window part and fails the charset —
        # the one-separator property the frozen charset guarantees.
        with pytest.raises(ValueError, match="grammar"):
            w.split_leaf_name("-31123_2025_x.zarr")

    def test_leaf_name_rejects_bad_window(self):
        with pytest.raises(ValueError, match="grammar"):
            w.leaf_name("-31123", "melt_2019")


# ── leaf_name_v3 (D23 window-only naming) ────────────────────────────────────


class TestLeafNameV3:
    def test_window_and_none(self):
        assert w.leaf_name_v3("2019") == "2019.zarr"
        assert w.leaf_name_v3(None) == f"{w.SCHEDULE_NONE_TOKEN}.zarr"

    def test_explicit_label_equal_to_reserved_token_raises(self):
        # The explicit grammar admits "all", but leaf_name_v3(None) already
        # owns that leaf — an explicit label equal to the token must raise
        # rather than alias the no-schedule leaf (D23 reservation).
        with pytest.raises(ValueError, match="reserved schedule:none token"):
            w.leaf_name_v3(w.SCHEDULE_NONE_TOKEN)

    def test_rejects_bad_window(self):
        with pytest.raises(ValueError, match="grammar"):
            w.leaf_name_v3("melt_2019")


# ── windows_intersecting ─────────────────────────────────────────────────────


class TestWindowsIntersecting:
    def test_yearly_straddle(self):
        got = w.windows_intersecting(_dt(2024, 11, 2), _dt(2025, 2, 3), "yearly")
        assert got == ["2024", "2025"]

    def test_monthly_run(self):
        got = w.windows_intersecting(_dt(2025, 11, 30), _dt(2026, 1, 1), "monthly")
        assert got == ["202511", "202512", "202601"]

    def test_instant_on_boundary_belongs_to_the_later_window(self):
        # Half-open [start, end): the boundary instant is in the LATER window.
        assert w.windows_intersecting(_dt(2026, 1, 1), _dt(2026, 1, 1), "yearly") == ["2026"]

    def test_daily_single_instant(self):
        assert w.windows_intersecting(_dt(2025, 11, 3, 5), _dt(2025, 11, 3, 6), "daily") == [
            "20251103"
        ]

    def test_explicit_overlap_and_boundary(self):
        declared = [
            {"label": "melt-2019", "start": "2019-06-01", "end": "2019-09-01"},
            {"label": "melt-2020", "start": "2020-06-01", "end": "2020-09-01"},
        ]
        assert w.windows_intersecting(_dt(2019, 8, 1), _dt(2020, 7, 1), "explicit", declared) == [
            "melt-2019",
            "melt-2020",
        ]
        # A range starting exactly at a window's half-open END misses it.
        assert w.windows_intersecting(_dt(2019, 9, 1), _dt(2019, 10, 1), "explicit", declared) == []

    def test_reversed_range_rejected(self):
        with pytest.raises(ValueError, match="precedes"):
            w.windows_intersecting(_dt(2025, 2, 1), _dt(2025, 1, 1), "yearly")


# ── lexicographic = chronological (property, per generative schedule) ────────


class TestLexicographicIsChronological:
    @pytest.mark.parametrize("schedule", w.GENERATIVE_SCHEDULES)
    def test_property(self, schedule):
        rng = random.Random(f"issue-246-{schedule}")
        span = _dt(1990, 1, 1), _dt(2120, 12, 31)
        seconds = int((span[1] - span[0]).total_seconds())
        instants = [span[0] + timedelta(seconds=rng.randrange(seconds)) for _ in range(300)]
        labels = [w.window_label(t, schedule) for t in instants]
        starts = {lab: w.window_range(lab, schedule)[0] for lab in labels}
        assert sorted(set(labels)) == sorted(set(labels), key=starts.__getitem__)
        # And label order matches the instants' own order window-wise.
        for t, lab in zip(instants, labels):
            lo, hi = w.window_range(lab, schedule)
            assert lo <= t < hi

    @pytest.mark.parametrize("schedule", w.GENERATIVE_SCHEDULES)
    def test_contiguity(self, schedule):
        # Consecutive windows tile time: each window's end is the next's start.
        t = _dt(2023, 11, 27)
        for _ in range(50):
            lab = w.window_label(t, schedule)
            lo, hi = w.window_range(lab, schedule)
            assert w.window_label(hi, schedule) != lab
            assert w.window_range(w.window_label(hi, schedule), schedule)[0] == hi
            t = hi


# ── UTC <-> dataset time (fixed-offset conversion, ratified #246) ────────────


class TestEpochConversion:
    def test_offsets(self):
        assert w.epoch_offset("utc") == 0
        assert w.epoch_offset("gps") == 18
        assert w.epoch_offset("tai") == 37  # TAI = GPS + 19 s
        with pytest.raises(ValueError, match="scale"):
            w.epoch_offset("tt")

    def test_icesat2_atlas_sdp_epoch(self):
        # ICESat-2 delta_time: GPS seconds since 2018-01-01T00:00:00Z (post-2017
        # epoch -> the constant offset cancels; naive difference is exact).
        epoch = "2018-01-01T00:00:00Z"
        assert w.utc_to_offset(_dt(2019, 1, 1), epoch=epoch, scale="gps") == 365 * 86400.0
        assert w.offset_to_utc(365 * 86400.0, epoch=epoch, scale="gps") == _dt(2019, 1, 1)

    def test_gps_native_epoch(self):
        # GPS seconds since the GPS epoch (1980-01-06, offset 0 by definition):
        # the full current GPS-UTC offset applies.
        epoch = "1980-01-06T00:00:00Z"
        naive = (_dt(2020, 1, 1) - _dt(1980, 1, 6)).total_seconds()
        assert w.utc_to_offset(_dt(2020, 1, 1), epoch=epoch, scale="gps") == naive + 18
        assert w.offset_to_utc(naive + 18, epoch=epoch, scale="gps") == _dt(2020, 1, 1)

    def test_tai_native_epoch(self):
        # TAI seconds since the TAI epoch (1958-01-01, offset 0 by definition):
        # the full current TAI-UTC offset (+37) applies — the pre-2017 branch,
        # symmetric with the GPS +18 case above.
        epoch = "1958-01-01T00:00:00Z"
        naive = (_dt(2020, 1, 1) - _dt(1958, 1, 1)).total_seconds()
        assert w.utc_to_offset(_dt(2020, 1, 1), epoch=epoch, scale="tai") == naive + 37
        assert w.offset_to_utc(naive + 37, epoch=epoch, scale="tai") == _dt(2020, 1, 1)

    def test_utc_scale_is_naive_difference(self):
        epoch = "2000-01-01T00:00:00Z"
        assert w.utc_to_offset(_dt(2000, 1, 2), epoch=epoch) == 86400.0

    def test_days_units(self):
        epoch = "2018-01-01T00:00:00Z"
        assert w.utc_to_offset(_dt(2018, 1, 31), epoch=epoch, units="days") == 30.0
        assert w.offset_to_utc(30, epoch=epoch, units="days") == _dt(2018, 1, 31)
        with pytest.raises(ValueError, match="units"):
            w.utc_to_offset(_dt(2018, 1, 2), epoch=epoch, units="fortnights")

    def test_round_trip(self):
        epoch = "2018-01-01T00:00:00Z"
        for scale in w.EPOCH_SCALES:
            v = w.utc_to_offset(_dt(2024, 7, 15, 6, 30), epoch=epoch, scale=scale)
            assert w.offset_to_utc(v, epoch=epoch, scale=scale) == _dt(2024, 7, 15, 6, 30)


# ── parse/format helpers ─────────────────────────────────────────────────────


class TestParseUtc:
    def test_z_suffix_and_offset(self):
        assert w.parse_utc("2025-01-01T00:00:00Z") == _dt(2025, 1, 1)
        assert w.parse_utc("2025-01-01T02:00:00+02:00") == _dt(2025, 1, 1)

    def test_naive_is_utc(self):
        assert w.parse_utc("2025-01-01") == _dt(2025, 1, 1)

    def test_datetime_passthrough(self):
        assert w.parse_utc(_dt(2025, 1, 1)) == _dt(2025, 1, 1)

    def test_garbage_rejected(self):
        with pytest.raises(ValueError, match="ISO-8601"):
            w.parse_utc("not-a-date")

    def test_iso_utc_rendering(self):
        assert w.iso_utc(_dt(2025, 1, 1, 12, 30, 15)) == "2025-01-01T12:30:15+00:00"
