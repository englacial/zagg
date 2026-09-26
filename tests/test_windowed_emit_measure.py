"""``tools/windowed_emit_measure.py`` — the issue #586 phase-1 readout, on a synthetic store."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, shard_leaf_path
from zagg.telemetry import build_record, flatten_record, write_run_parquet

pytest.importorskip("pyarrow")

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
    (root / "sweep_stats_20260101T000000Z_stages.json").write_text(
        json.dumps(
            {
                "stages": [
                    {
                        "dispatch_order": 3,
                        "written": 4,
                        "icechunk_commits": 0,
                        "icechunk_rebases": 0,
                    },
                    {
                        "dispatch_order": 0,
                        "written": 1,
                        "icechunk_commits": 2,
                        "icechunk_rebases": 1,
                        "icechunk_commit_s": 0.5,
                    },
                ]
            }
        )
    )
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
    # ladder: the stage rows' icechunk counters
    assert win["ladder"]["stage_records"] == 2
    assert [s["icechunk_commits"] for s in win["ladder"]["stages"]] == [0, 2]
    tool.print_table([base, win])
    out = capsys.readouterr().out
    assert "windows per shard" in out and "icechunk rebases (sum)" in out
    assert out.count("yearly") == 1 and out.count("none") == 1


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
    assert result["objects"]["shards_listed"] == 0 and result["ladder"]["stage_records"] == 0
