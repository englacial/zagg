"""``tools/windowed_emit_measure.py`` — the issue #586 phase-1 readout, on a synthetic store."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, shard_leaf_path
from zagg.telemetry import build_record, failure_record, flatten_record, write_run_parquet

REPO = Path(__file__).parent.parent
sys.path.insert(0, str(REPO / "tools"))

import windowed_emit_measure as tool  # noqa: E402  (tools/ is not installed)

SHARDS = tuple(int(morton_word(d)) for d in ("11111", "11112"))


def _store(tmp_path, *, windows):
    root = tmp_path / ("windowed" if windows else "baseline")
    root.mkdir()
    temporal = {"schedule": "yearly"} if windows else None
    (root / MANIFEST_NAME).write_text(
        json.dumps(
            {"spec": "morton-hive/1", "shard_order": 4, "cell_order": 6, "temporal": temporal}
        )
    )
    rows = []
    for shard in SHARDS:
        for window in windows or (None,):
            leaf = Path(shard_leaf_path(str(root), shard, window=window))
            (leaf / "6").mkdir(parents=True)
            (leaf / "6" / "count").mkdir()
            (leaf / "6" / "count" / "c").write_bytes(b"x" * 100)
            (leaf / "zarr.json").write_bytes(b"{}")
            rows.append(
                flatten_record(
                    build_record(
                        shard_key=shard,
                        metadata={
                            "total_obs": 5,
                            "duration_s": 10.0 if windows else 40.0,
                            "max_memory_mb": 500.0,
                            "phase_timings": {"read": 1.0},
                            "gb_seconds": 2.0,
                        },
                        granule_ids=["g"],
                        window=window,
                    )
                )
            )
    # the leaf columns (one per window, at the shard node) are siblings, not leaves
    node = Path(shard_leaf_path(str(root), SHARDS[0])).parent
    for column in [f"{w}.pyramid.zarr" for w in windows or ("all",)]:
        (node / column).mkdir()
        (node / column / "zarr.json").write_bytes(b"{}" * 10)
    write_run_parquet(str(root), rows, run_id="run-a")
    # the ladder's stage rows: Icechunk is OFF on a windowed store (spec 11.6)
    stages = [
        {"dispatch_order": 3, "written": 4, "icechunk_commits": 0, "icechunk_rebases": 0},
        {"dispatch_order": 3, "written": 2, "icechunk_commits": 1, "icechunk_rebases": 0},
        {
            "dispatch_order": 0,
            "written": 1,
            "icechunk_commits": 2,
            "icechunk_rebases": 1,
            "icechunk_commit_s": 0.5,
        },
    ]
    if windows:
        stages = [{k: v for k, v in s.items() if not k.startswith("icechunk_")} for s in stages]
    (root / "sweep_stats_20260101T000000Z_stages.json").write_text(json.dumps({"stages": stages}))
    # an older sweep record: kept apart, not mixed into the newest one's sums
    older = {"stages": [{"dispatch_order": 0, "written": 99, "icechunk_commits": 99}]}
    (root / "sweep_stats_20251231T000000Z_stages.json").write_text(json.dumps(older))
    return str(root)


def test_measures_both_arms_and_prints_a_table(tmp_path, capsys):
    base = tool.measure(_store(tmp_path, windows=None), store_kwargs={})
    win = tool.measure(_store(tmp_path, windows=("2019", "2020", "2021")), store_kwargs={})
    assert base["schedule"] == "none" and win["schedule"] == "yearly"
    assert base["fleet"]["units"] == 2 and win["fleet"]["units"] == 6
    assert base["fleet"]["shards"] == win["fleet"]["shards"] == 2
    assert base["fleet"]["windows_per_shard"]["p50"] == 1.0
    assert win["fleet"]["windows_per_shard"]["p50"] == 3.0
    assert base["fleet"]["duration_s"]["p50"] == 40.0 and win["fleet"]["duration_s"]["p50"] == 10.0
    assert base["fleet"]["phase_read"]["p100"] == 1.0
    assert base["fleet"]["errors"] == 0 and base["fleet"]["timeouts"] == 0
    # objects: a leaf is two objects here; siblings are counted apart
    assert base["objects"]["leaves_per_shard"]["p50"] == 1.0
    assert win["objects"]["leaves_per_shard"]["p50"] == 3.0
    assert base["objects"]["objects_per_shard"]["p50"] == 2.0
    assert win["objects"]["objects_per_shard"]["p50"] == 6.0
    assert win["objects"]["total_leaf_bytes"] == 3 * 2 * 102
    assert base["objects"]["sibling_bytes"] == {"all.pyramid": 20}
    assert win["objects"]["leaves_per_shard"]["p100"] == 3.0
    assert win["objects"]["sibling_bytes"] == {f"{w}.pyramid": 20 for w in ("2019", "2020", "2021")}
    # ladder: the newest record's batch rows summed per dispatch order
    assert win["ladder"]["batch_rows"] == base["ladder"]["batch_rows"] == 3
    assert base["ladder"]["record"] == "sweep_stats_20260101T000000Z_stages.json"
    assert len(base["ladder"]["records"]) == 2
    assert [
        (s["dispatch_order"], s["batches"], s["written"], s["icechunk_commits"])
        for s in base["ladder"]["stages"]
    ] == [(0, 1, 1, 2), (3, 2, 6, 1)]
    assert [s["icechunk_commits"] for s in win["ladder"]["stages"]] == [None, None]
    tool.print_table([base, win])
    out = capsys.readouterr().out
    assert "windows per shard" in out
    # measured on the baseline, not measured (``-``, never ``0``) on the windowed arm
    (commits,) = [ln.split() for ln in out.splitlines() if ln.startswith("icechunk commits")]
    assert commits[-2:] == ["3", "-"]
    assert out.count("yearly") == 1 and out.count("none") == 1


@pytest.mark.parametrize("windows", [None, ("2019", "2020", "2021")])
def test_a_rerun_unit_is_not_an_extra_window(tmp_path, windows):
    root = _store(tmp_path, windows=windows)
    window = windows[0] if windows else None
    record = build_record(
        shard_key=SHARDS[0], metadata={"duration_s": 1.0}, granule_ids=["g"], window=window
    )
    write_run_parquet(root, [flatten_record(record)], run_id="run-b")
    fleet = tool.measure(root, store_kwargs={})["fleet"]
    assert fleet["runs"] == 2 and fleet["units"] == 2 * len(windows or (None,)) + 1
    assert fleet["windows_per_shard"]["p100"] == len(windows or (None,))


def test_timeouts_match_the_dispatcher_error_strings(tmp_path):
    root = _store(tmp_path, windows=None)
    errors = (
        "Lambda timeout: Task timed out after 900.00 seconds",
        "worker timed out, was OOM-killed, or crashed before writing its result",
        "ValueError: bad granule",
    )
    rows = [flatten_record(failure_record(shard_key=SHARDS[0], error=e)) for e in errors]
    write_run_parquet(root, rows, run_id="run-b")
    fleet = tool.measure(root, store_kwargs={})["fleet"]
    assert fleet["errors"] == 3 and fleet["timeouts"] == 2


def test_lists_only_successful_exact_shard_keys(tmp_path, caplog):
    import pandas as pd

    root = _store(tmp_path, windows=None)
    third = int(morton_word("11113"))  # failed: no leaf, so never LISTed as an empty one
    rows = [
        flatten_record(failure_record(shard_key=third, error="ValueError: x")),
        flatten_record(build_record(shard_key=-1, metadata={}, granule_ids=["g"])),
    ]
    write_run_parquet(root, rows, run_id="run-b")
    frame = pd.DataFrame({"shard_key": [float(2**60), 0.5], "success": [True, True]})
    frame.to_parquet(Path(root) / "stats_run-c.parquet", engine="fastparquet")
    result = tool.measure(root, store_kwargs={})
    assert result["objects"]["shards_listed"] == 2
    assert result["objects"]["leaves_per_shard"]["p50"] == 1.0
    assert "skipped 3 shard key(s)" in caplog.text


def test_cli_writes_json(tmp_path):
    root = _store(tmp_path, windows=None)
    out = tmp_path / "out.json"
    assert tool.main([root, "--json", str(out), "--max-shards", "1"]) == 0
    (result,) = json.loads(out.read_text())
    assert result["objects"]["shards_listed"] == 1 and result["fleet"]["units"] == 2


def test_empty_store_is_reported_not_fatal(tmp_path):
    root = tmp_path / "empty"
    root.mkdir()
    result = tool.measure(str(root), store_kwargs={})
    assert result["fleet"] == {"runs": 0, "units": 0}
    assert result["objects"]["shards_listed"] == 0 and result["ladder"]["batch_rows"] == 0


def test_a_bulk_invoke_sums_its_per_window_phases():
    # Review finding (9), issue #586 phase 2: a two-window bulk invoke writes
    # two rows; the invoke-level columns repeat and collapse to one, the
    # per-window phases sum to the invoke's, a fan-out row stays its own.
    import pandas as pd

    rows = pd.DataFrame(
        {
            "_run": ["r", "r", "r"],
            "shard_key": [1, 1, 2],
            "window": ["2019", "2020", "2019"],
            "unit_windows": [2, 2, None],
            "duration_s": [50.0, 50.0, 30.0],
            "phase_read": [20.0, 20.0, 10.0],
            "phase_write": [4.0, 6.0, 3.0],
            "phase_spill_bytes": [None, None, None],
        }
    )
    out = tool._invokes(rows).set_index("shard_key")
    assert len(out) == 2
    assert out.loc[1, ["duration_s", "phase_read", "phase_write"]].tolist() == [50.0, 20.0, 10.0]
    assert out.loc[2, "phase_write"] == 3.0 and pd.isna(out.loc[1, "phase_spill_bytes"])
