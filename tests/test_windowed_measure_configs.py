"""The four issue #602 arm configs: one body, four ``output.windowing`` blocks."""

from __future__ import annotations

import re
from dataclasses import asdict
from pathlib import Path

import pytest

from zagg.config import get_windowing, get_windowing_unit, load_config, validate_config
from zagg.windows import validate_label, windows_intersecting

CONFIGS = Path(__file__).parent.parent / "tools" / "configs"
ARMS = ("none", "yearly", "quarterly", "monthly")
#: Whole years 2019..2025 (espg 2026-09-26): 7 / 28 / 84 windows.
SPAN = ("2019-01-01T00:00:00Z", "2025-12-31T23:59:59Z")


def _load(arm):
    return load_config(str(CONFIGS / f"atl03_windowed_measure_{arm}.yaml"))


def _labels(config) -> list[str]:
    """The windows the arm's schedule covers over the catalog span."""
    w = get_windowing(config)
    if w is None:
        return []
    return windows_intersecting(*SPAN, w["schedule"], w["windows"])


@pytest.mark.parametrize("arm", ARMS)
def test_every_arm_validates_with_the_measurement_knobs(arm):
    config = _load(arm)
    validate_config(config)
    assert config.output["sweep"] == "stages"
    assert config.output["pyramid"] == {"overviews": 13, "all_time": True}
    assert config.output["store_layout"] == "hive"
    assert config.output["grid"]["parent_order"] == 9 and config.output["grid"]["chunk_inner"] == 13
    assert config.worker == {"memory": 4096, "extra_disk": True}
    assert (
        get_windowing_unit(config) == "shard"
    )  # the default dispatch; unit: window is the fallback


def test_the_arms_differ_only_in_the_windowing_block():
    bodies = {}
    for arm in ARMS:
        d = asdict(_load(arm))
        d["output"].pop("windowing", None)
        bodies[arm] = d
    assert all(bodies[arm] == bodies["none"] for arm in ARMS)


def test_the_window_counts_are_the_issues():
    assert get_windowing(_load("none")) is None
    assert _labels(_load("yearly")) == [str(y) for y in range(2019, 2026)]
    assert _labels(_load("monthly")) == [
        f"{y}{m:02d}" for y in range(2019, 2026) for m in range(1, 13)
    ]
    assert len(_labels(_load("quarterly"))) == 28


def test_the_quarterly_arm_is_an_explicit_contiguous_list_in_the_reserved_spelling():
    w = get_windowing(_load("quarterly"))
    assert w["schedule"] == "explicit"
    windows = w["windows"]
    labels = [x["label"] for x in windows]
    # 28 quarters, in chronological order, each in the grammar-reserved
    # ``YYYYQ[1-4]`` spelling (so a generative ``quarterly`` would name the same
    # leaves) and valid under the explicit grammar, and sorting lexicographically
    # = chronologically, the property the generative schedules guarantee.
    assert labels == [f"{y}Q{q}" for y in range(2019, 2026) for q in (1, 2, 3, 4)]
    assert all(re.fullmatch(r"[0-9]{4}Q[1-4]", lab) and validate_label(lab) for lab in labels)
    assert (
        labels
        == sorted(labels)
        == sorted(labels, key=lambda lab: next(x["start"] for x in windows if x["label"] == lab))
    )
    # contiguous half-open quarters spanning exactly the whole years
    assert windows[0]["start"] == "2019-01-01T00:00:00+00:00"
    assert windows[-1]["end"] == "2026-01-01T00:00:00+00:00"
    assert all(a["end"] == b["start"] for a, b in zip(windows, windows[1:]))
    months = {(x["start"][5:7], x["end"][5:7]) for x in windows}
    assert months == {("01", "04"), ("04", "07"), ("07", "10"), ("10", "01")}
