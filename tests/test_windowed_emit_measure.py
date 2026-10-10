"""``tools/windowed_emit_measure.py`` — the issue #602 / #586 readout, on synthetic stores."""

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
#: A tiny ``count`` array: 16 int32 cells in one ShardingCodec object of 4 inner
#: chunks of 4 cells -> 16 B per occupied chunk, index 4 x 16 + 4 (crc32c) = 68 B.
COUNT_ZARR_JSON = {
    "zarr_format": 3,
    "node_type": "array",
    "shape": [16],
    "data_type": "int32",
    "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [16]}},
    "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
    "fill_value": 0,
    "codecs": [
        {
            "name": "sharding_indexed",
            "configuration": {
                "chunk_shape": [4],
                "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
                "index_codecs": [
                    {"name": "bytes", "configuration": {"endian": "little"}},
                    {"name": "crc32c"},
                ],
                "index_location": "end",
            },
        }
    ],
}
CHUNK_B, INDEX_B = 16, 68
#: Occupied inner chunks per (shard index, window index): the second shard's
#: leaves are fuller, every window differs.
OCCUPANCY = {(0, 0): 1, (0, 1): 2, (0, 2): 3, (1, 0): 4, (1, 1): 2, (1, 2): 4}


def _write_leaf_arrays(leaf: Path, occupied: int) -> None:
    (leaf / "6" / "count").mkdir(parents=True)
    (leaf / "6" / "count" / "zarr.json").write_text(json.dumps(COUNT_ZARR_JSON))
    (leaf / "6" / "count" / "c").mkdir()
    (leaf / "6" / "count" / "c" / "0").write_bytes(b"x" * (INDEX_B + occupied * CHUNK_B))
    for name, size in (("morton", 100), ("h_tdigest_signal", 300)):
        (leaf / "6" / name / "c").mkdir(parents=True)
        (leaf / "6" / name / "c" / "0").write_bytes(b"y" * size)


def _store(tmp_path, *, windows, versioned_first=False):
    """Two shards under node ``1/1/1/1``; per window one leaf of 5 objects
    (``zarr.json`` + 4 array objects), a column per window at the shard node,
    overviews above it, a node sidecar, run parquets and stage records."""
    root = tmp_path / ("windowed" if windows else "baseline")
    root.mkdir()
    temporal = {"schedule": "yearly"} if windows else None
    (root / MANIFEST_NAME).write_text(
        json.dumps(
            {"spec": "morton-hive/1", "shard_order": 4, "cell_order": 6, "temporal": temporal}
        )
    )
    rows = []
    for si, shard in enumerate(SHARDS):
        for wi, window in enumerate(windows or (None,)):
            leaf = Path(shard_leaf_path(str(root), shard, window=window))
            leaf.mkdir(parents=True)
            stamp: dict = {"complete": True}
            if versioned_first and si == 0 and wi == 0:
                # a retried leaf: two version dirs, the stamp names the current one
                _write_leaf_arrays(leaf / "run-a-1", occupied=4)
                _write_leaf_arrays(leaf / "run-b-2", occupied=OCCUPANCY[(si, wi)])
                stamp["current"] = "run-b-2"
            else:
                _write_leaf_arrays(leaf, occupied=OCCUPANCY[(si, wi)])
            (leaf / "zarr.json").write_text(
                json.dumps({"attributes": {"morton_hive_commit": stamp}})
            )
            rows.append(
                flatten_record(
                    build_record(
                        shard_key=shard,
                        metadata={
                            "total_obs": 5,
                            "duration_s": 10.0 if windows else 40.0,
                            # the billed wall (issue #589): only the windowed
                            # arm's workers stamp it here
                            "duration_total_s": 14.0 if windows else None,
                            "max_memory_mb": 500.0,
                            "phase_timings": {"read": 1.0, "write": 2.0 + wi},
                            "gb_seconds": 2.0,
                        },
                        granule_ids=["g"],
                        window=window,
                        lambda_config={"memory_mb": 4096, "arch": "arm64"},
                    )
                )
            )
    # the leaf columns (one per window, at the shard node) are siblings, not leaves
    node = Path(shard_leaf_path(str(root), SHARDS[0])).parent
    for column in [f"{w}.pyramid.zarr" for w in windows or ("all",)]:
        (node / column).mkdir()
        (node / column / "zarr.json").write_bytes(b"{}" * 10)
    (node / "stats.json").write_bytes(b"s" * 10)
    # overviews live ABOVE the shard order: ``all.zarr`` and, windowed, one per
    # window — ``2019.zarr`` must not read as the leaf of a shard "2019"
    for name in ["all.zarr", *(f"{w}.zarr" for w in windows or ())]:
        (root / "1" / "1" / name).mkdir(parents=True)
        (root / "1" / "1" / name / "zarr.json").write_bytes(b"o" * 50)
    write_run_parquet(str(root), rows, run_id="run-a")
    # the ladder's root run record: Icechunk is OFF on a windowed store (spec 11.6)
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
        # the (node, window) unit rows of issue #586 phase 4: one row per
        # invoke, each with its own units, worker seconds and fold peak
        units = [(1, 0, 30.0, 256), (1, 0, 20.0, 1024), (0, 1, 5.0, 64)]
        for row, (n_window, n_close, seconds, peak) in zip(stages, units, strict=True):
            row.update(
                window_units=n_window,
                close_units=n_close,
                failed=0,
                duration_s=seconds,
                fold_peak_cells=peak,
            )
    (root / "sweep_stats_20260101T000000Z_stages.json").write_text(
        json.dumps({"stages": stages, "barrier_timed_out": False, "short_orders": []})
    )
    # an older sweep record: kept apart, not mixed into the newest one's sums
    older = {"stages": [{"dispatch_order": 0, "written": 99, "icechunk_commits": 99}]}
    (root / "sweep_stats_20251231T000000Z_stages.json").write_text(json.dumps(older))
    _stage_records(root, windows)
    return str(root)


def _stage_records(root: Path, windows) -> None:
    """The fleet's per-invoke records under ``<store>.status/run-stage-*/``."""
    status = Path(str(root) + ".status") / "run-stage-20260101T000100Z-abcdef"
    status.mkdir(parents=True)

    def rec(dispatch, batch, unit, window, wall, n_nodes, cells, peak, failed=0):
        return {
            "spec": "zagg-sweep-stage-record/1",
            "role": "stage",
            "run_id": "stage-20260101T000100Z-abcdef",
            "pipeline_run_id": "run-a",
            "dispatch": dispatch,
            "batch": batch,
            "unit": unit,
            "window": window,
            "n_nodes": n_nodes,
            "duration_s": wall,
            "stages": [
                {
                    "dispatch_order": dispatch,
                    "nodes": n_nodes,
                    "written": 1,
                    "current": 0,
                    "failed": failed,
                    "fold_cells_read": cells,
                    "fold_peak_cells": peak,
                }
            ],
        }

    if windows:
        records = [
            rec(3, 0, "window", "2019", 12.0, 2, 64, 32),
            rec(3, 1, "window", "2020", 8.0, 2, 64, 32),
            rec(3, 2, "close", None, 3.0, 2, 16, 8),
            rec(0, 0, "window", "2019", 20.0, 1, 128, 128, failed=1),
            rec(0, 1, "close", None, 6.0, 1, 32, 32),
        ]
    else:
        records = [rec(3, 0, None, None, 30.0, 2, 64, 32), rec(0, 0, None, None, 40.0, 1, 128, 128)]
    for r in records:
        (status / f"stage-{r['dispatch']:02d}-{r['batch']:04d}.json").write_text(json.dumps(r))
    finisher = {
        "spec": "zagg-sweep-stage-record/1",
        "role": "finisher",
        "run_id": "stage-20260101T000100Z-abcdef",
        "pipeline_run_id": "run-a",
        "barrier_timed_out": bool(windows),
        "short_orders": [0] if windows else [],
        "duration_s": 2.0,
    }
    (status / "finisher.json").write_text(json.dumps(finisher))
    # an older stage run with one record: the newest is what the table prints
    older = Path(str(root) + ".status") / "run-stage-20251231T000000Z-000000"
    older.mkdir()
    (older / "stage-00-0000.json").write_text(json.dumps(rec(0, 0, None, None, 99.0, 1, 1, 1)))


@pytest.fixture
def arms(tmp_path):
    base = tool.measure(_store(tmp_path, windows=None), store_kwargs={}, accuracy=False)
    win = tool.measure(
        _store(tmp_path, windows=("2019", "2020", "2021"), versioned_first=True),
        store_kwargs={},
        accuracy=False,
    )
    return base, win


def test_fleet_numbers_per_invoke_and_per_leaf(arms):
    base, win = arms
    assert base["schedule"] == "none" and win["schedule"] == "yearly"
    assert base["fleet"]["units"] == 2 and win["fleet"]["units"] == 6
    assert base["fleet"]["shards"] == win["fleet"]["shards"] == 2
    assert base["fleet"]["leaves"] == 2 and win["fleet"]["leaves"] == 6
    assert base["fleet"]["windows_per_shard"]["p50"] == 1.0
    assert win["fleet"]["windows_per_shard"]["p50"] == 3.0
    assert base["fleet"]["duration_s"]["p50"] == 40.0 and win["fleet"]["duration_s"]["p50"] == 10.0
    # the billed wall beside it; an older run's rows carry none (``-``, not 0)
    assert win["fleet"]["duration_total_s"]["p50"] == 14.0
    assert base["fleet"]["duration_total_s"]["p50"] is None
    assert base["fleet"]["phase_read"]["p100"] == 1.0
    # the per-LEAF write side: windows 0/1/2 wrote 2/3/4 s
    assert win["fleet"]["leaf_phase_write"] == {"p50": 3.0, "p90": 4.0, "p100": 4.0}
    assert base["fleet"]["errors"] == 0 and base["fleet"]["timeouts"] == 0
    assert base["fleet"]["fits_one_invoke"] and win["fleet"]["fits_one_invoke"]
    # billed GB-s: the record's gb_seconds (billed wall x 4 GB; duration_s x 4 GB before #589)
    assert win["fleet"]["billed_gb_seconds"] == win["fleet"]["gb_seconds"] == 6 * 14.0 * 4.0
    assert base["fleet"]["billed_gb_seconds"] == base["fleet"]["gb_seconds"] == 2 * 40.0 * 4.0


def test_tree_numbers_leaves_arrays_and_siblings(arms):
    base, win = arms
    assert base["objects"]["leaves_per_shard"]["p50"] == 1.0
    assert win["objects"]["leaves_per_shard"]["p50"] == 3.0
    # a leaf is zarr.json + count/zarr.json + count, morton, digest chunks = 5 objects
    assert base["objects"]["objects_per_shard"]["p50"] == 5.0
    # 3 leaves x 5, plus the first shard's retried leaf keeps its stale version's 4 objects
    assert win["objects"]["objects_per_shard"] == {"p50": 17.0, "p90": 18.6, "p100": 19.0}
    assert base["objects"]["leaves"] == 2 and win["objects"]["leaves"] == 6
    by_array = base["objects"]["leaf_bytes_by_array"]
    assert by_array["morton"] == 200 and by_array["h_tdigest_signal"] == 600
    assert (
        by_array["count"] == 2 * len(json.dumps(COUNT_ZARR_JSON)) + 2 * INDEX_B + (1 + 4) * CHUNK_B
    )
    assert "(leaf metadata)" in by_array
    assert base["objects"]["total_leaf_bytes"] == sum(by_array.values())
    # siblings at the shard node, keyed by stem; overviews above the node are not siblings
    assert base["objects"]["sibling_bytes"] == {"all.pyramid": 20, "stats.json": 10}
    assert win["objects"]["sibling_bytes"] == {
        **{f"{w}.pyramid": 20 for w in ("2019", "2020", "2021")},
        "stats.json": 10,
    }
    # the non-leaf classes: columns, overviews (``2019.zarr`` is NOT a leaf), sidecars, root
    assert base["non_leaf"]["columns"] == {"objects": 1, "bytes": 20}
    assert win["non_leaf"]["columns"] == {"objects": 3, "bytes": 60}
    assert base["non_leaf"]["overviews"] == {"objects": 1, "bytes": 50}
    assert win["non_leaf"]["overviews"] == {"objects": 4, "bytes": 200}
    assert base["non_leaf"]["sidecars"] == {"objects": 1, "bytes": 10}
    assert base["non_leaf"]["root"]["objects"] == 4  # manifest, parquet, two sweep records


def test_occupancy_from_the_count_object_size(arms):
    base, win = arms
    for r in (base, win):
        occ = r["occupancy"]
        assert (occ["bytes_per_chunk"], occ["index_bytes"], occ["chunks"]) == (CHUNK_B, INDEX_B, 4)
        assert occ["dtype"] == "int32" and occ["source"].endswith("count/zarr.json")
    assert base["occupancy"]["per_leaf"] == {
        "n": 2,
        "mean": 2.5,
        "min": 1.0,
        "p50": 2.5,
        "max": 4.0,
    }
    assert base["occupancy"]["summed_per_shard"]["mean"] == 2.5
    assert base["occupancy"]["full_row_ratio"] == 2.5 / 4
    # windowed: the versioned leaf reads its CURRENT version (1 chunk, not the stale 4)
    assert win["occupancy"]["per_leaf"]["n"] == 6 and win["occupancy"]["per_leaf"]["min"] == 1.0
    assert win["occupancy"]["per_leaf"]["mean"] == pytest.approx(16 / 6)
    assert win["occupancy"]["summed_per_shard"] == {
        "n": 2,
        "mean": 8.0,
        "min": 6.0,
        "p50": 8.0,
        "max": 10.0,
    }
    assert win["occupancy"]["full_row_ratio"] == 2.0
    assert win["occupancy"]["by_window"] == {"2019": 2.5, "2020": 2.0, "2021": 3.5}
    assert win["occupancy"]["per_leaf_fraction"] == pytest.approx(16 / 6 / 4)


def test_ladder_and_stage_runs(arms):
    base, win = arms
    # root run records: the newest record's batch rows summed per dispatch order
    assert win["ladder"]["batch_rows"] == base["ladder"]["batch_rows"] == 3
    assert base["ladder"]["record"] == "sweep_stats_20260101T000000Z_stages.json"
    assert len(base["ladder"]["records"]) == 2
    assert [
        (s["dispatch_order"], s["batches"], s["written"], s["icechunk_commits"])
        for s in base["ladder"]["stages"]
    ] == [(0, 1, 1, 2), (3, 2, 6, 1)]
    assert [s["icechunk_commits"] for s in win["ladder"]["stages"]] == [None, None]
    assert [
        (s["dispatch_order"], s["window_units"], s["close_units"], s["duration_s"])
        for s in win["ladder"]["stages"]
    ] == [(0, 0, 1, 5.0), (3, 2, 0, 50.0)]
    assert [s["fold_peak_cells"] for s in win["ladder"]["stages"]] == [64, 1024]
    assert base["ladder"]["barrier_timed_out"] is False and base["ladder"]["short_orders"] == []
    # the fleet's per-invoke stage records: two runs, the newest is ``latest``
    assert len(win["stage_runs"]["runs"]) == 2
    latest = win["stage_runs"]["latest"]
    assert latest["run_id"] == "stage-20260101T000100Z-abcdef"
    assert latest["pipeline_run_id"] == "run-a" and latest["finisher"]
    assert latest["invokes"] == 5 and latest["failed"] == 1
    assert latest["barrier_timed_out"] is True and latest["short_orders"] == [0]
    assert latest["worker_s"] == 49.0 and latest["finisher_s"] == 2.0
    assert latest["gb_seconds"] == 51.0 * tool.STAGE_TIER_GB
    o3, o0 = latest["per_order"]
    assert (o3["dispatch_order"], o3["invokes"], o3["window_invokes"], o3["close_invokes"]) == (
        3,
        3,
        2,
        1,
    )
    assert o3["wall_s"]["p100"] == 12.0 and o3["window_wall_s"]["p50"] == 10.0
    assert o3["close_wall_s"]["p100"] == 3.0
    assert o3["fold_cells_read"] == 144 and o3["fold_cells_read_per_node"] == 24.0
    assert o3["fold_peak_cells"] == 32 and o3["worker_s"] == 23.0
    assert (o0["dispatch_order"], o0["failed"], o0["whole_invokes"]) == (0, 1, 0)
    b = base["stage_runs"]["latest"]
    assert b["per_order"][0]["whole_invokes"] == 1 and b["barrier_timed_out"] is False


def test_cost_sums_leaf_and_stage_gb_seconds(arms):
    base, win = arms
    from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC

    price = LAMBDA_PRICE_PER_GB_SEC

    # both stage runs count (the older one's 99 s invoke included), each at the 8 GB tier
    assert win["cost"]["stage_gb_seconds"] == (51.0 + 99.0) * tool.STAGE_TIER_GB
    assert win["cost"]["leaf_gb_seconds"] == 336.0
    assert win["cost"]["total_usd"] == pytest.approx((336.0 + 150.0 * 8) * price)
    assert base["cost"]["leaf_usd"] == pytest.approx(320.0 * price)


def test_prints_one_column_per_arm(arms, capsys):
    base, win = arms
    tool.print_table([base, win, win, win])  # four arms, as the measurement runs
    out = capsys.readouterr().out
    assert "windows per shard" in out
    assert out.count("yearly") == 3 and out.count("none") == 1
    # measured on the baseline, not measured (``-``, never ``0``) on the windowed arms
    (commits,) = [ln.split() for ln in out.splitlines() if ln.startswith("icechunk commits")]
    assert commits[-4:] == ["3", "-", "-", "-"]
    (peak,) = [ln for ln in out.splitlines() if ln.startswith("stage fold peak cells")]
    assert peak.split()[-3:] == ["1,024", "/", "64"]
    (dtype,) = [ln for ln in out.splitlines() if ln.startswith("count dtype")]
    assert dtype.split()[-4:] == ["int32"] * 4
    for label in (
        "  leaf bytes: morton",
        "occupied chunks per leaf",
        "  x one full row",
        "non-leaf overviews objects / bytes",
        "stage invokes per order",
        "stage fold cells read per node per order",
        "barrier timed out / short orders",
        "all-time probe orders",
        "cost USD leaf / stage / total",
    ):
        assert any(ln.startswith(label) for ln in out.splitlines()), label
    (fit,) = [ln for ln in out.splitlines() if ln.startswith("errors / timeouts / fits")]
    assert fit.split()[-5:] == ["0", "/", "0", "/", "yes"]
    (acc,) = [ln for ln in out.splitlines() if ln.startswith("all-time probe orders")]
    assert acc.split()[-4:] == ["-"] * 4  # accuracy=False: not measured


@pytest.mark.parametrize(
    "path, expected",
    [
        ("morton_hive.json", ("root", None)),
        ("icechunk/refs/x", ("icechunk", None)),
        ("1/1/stats.json", ("sidecars", ("1/1", "stats.json"))),
        ("1/1/1/1/all.pyramid.zarr/zarr.json", ("columns", ("1/1/1/1", "all.pyramid"))),
        ("1/1/all.zarr/3/count/c/0", ("overviews", ("1/1", "all"))),
        ("1/1/2019.zarr/zarr.json", ("overviews", ("1/1", "2019"))),
        ("1/1/1/1/1111.zarr/zarr.json", ("leaf", ("1111", None, None, None, False))),
        ("1/1/1/1/1111_2019.zarr/6/count/c/0", ("leaf", ("1111", "2019", None, "count", True))),
        (
            "1/1/1/1/1111_2019.zarr/6/count/zarr.json",
            ("leaf", ("1111", "2019", None, "count", False)),
        ),
        (
            "-2/1/1/1/-2111.zarr/run-ab-1/6/morton/c/0",
            ("leaf", ("-2111", None, "run-ab-1", "morton", True)),
        ),
        ("-2/1/1/1/-2111.zarr/coverage.moc", ("leaf", ("-2111", None, None, None, False))),
    ],
)
def test_classify(path, expected):
    assert tool._classify(path) == expected


@pytest.mark.parametrize("windows", [None, ("2019", "2020", "2021")])
def test_a_rerun_unit_is_not_an_extra_window(tmp_path, windows):
    root = _store(tmp_path, windows=windows)
    window = windows[0] if windows else None
    record = build_record(
        shard_key=SHARDS[0], metadata={"duration_s": 1.0}, granule_ids=["g"], window=window
    )
    write_run_parquet(root, [flatten_record(record)], run_id="run-b")
    fleet = tool.measure(root, store_kwargs={}, accuracy=False)["fleet"]
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
    fleet = tool.measure(root, store_kwargs={}, accuracy=False)["fleet"]
    assert fleet["errors"] == 3 and fleet["timeouts"] == 2
    assert fleet["fits_one_invoke"] is False


def test_the_tree_is_leaf_driven_not_parquet_driven(tmp_path):
    # A failed shard writes no leaf and a stale-worker / inexact parquet key
    # names no node: neither changes what the LIST says.
    import pandas as pd

    root = _store(tmp_path, windows=None)
    third = int(morton_word("11113"))
    rows = [
        flatten_record(failure_record(shard_key=third, error="ValueError: x")),
        flatten_record(build_record(shard_key=-1, metadata={}, granule_ids=["g"])),
    ]
    write_run_parquet(root, rows, run_id="run-b")
    frame = pd.DataFrame({"shard_key": [float(2**60), 0.5], "success": [True, True]})
    frame.to_parquet(Path(root) / "stats_run-c.parquet", engine="fastparquet")
    result = tool.measure(root, store_kwargs={}, accuracy=False)
    assert result["objects"]["shards_listed"] == 2
    assert result["objects"]["leaves_per_shard"] == {"p50": 1.0, "p90": 1.0, "p100": 1.0}


def test_cli_writes_json(tmp_path):
    root = _store(tmp_path, windows=None)
    out = tmp_path / "out.json"
    assert tool.main([root, "--json", str(out), "--max-shards", "1", "--skip-accuracy"]) == 0
    (result,) = json.loads(out.read_text())
    assert result["objects"]["shards_listed"] == 1 and result["fleet"]["units"] == 2
    assert result["occupancy"]["per_leaf"]["n"] == 1


def test_empty_store_is_reported_not_fatal(tmp_path):
    root = tmp_path / "empty"
    root.mkdir()
    result = tool.measure(str(root), store_kwargs={})
    assert result["fleet"] == {"runs": 0, "units": 0}
    assert result["objects"]["shards_listed"] == 0 and result["ladder"]["batch_rows"] == 0
    assert result["occupancy"]["source"] == "fallback" and result["occupancy"]["per_leaf"]["n"] == 0
    assert result["stage_runs"] == {"runs": [], "latest": None}
    assert "accuracy" not in result


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
            "duration_total_s": [80.0, 80.0, 41.0],
            "phase_read": [20.0, 20.0, 10.0],
            "phase_write": [4.0, 6.0, 3.0],
            "phase_spill_bytes": [None, None, None],
        }
    )
    out = tool._invokes(rows).set_index("shard_key")
    assert len(out) == 2
    assert out.loc[1, ["duration_s", "phase_read", "phase_write"]].tolist() == [50.0, 20.0, 10.0]
    assert out.loc[2, "phase_write"] == 3.0 and pd.isna(out.loc[1, "phase_spill_bytes"])
    assert out["duration_total_s"].to_dict() == {1: 80.0, 2: 41.0}


def test_billed_gb_seconds_prefers_the_record_and_recomputes_the_rest():
    import pandas as pd

    invokes = pd.DataFrame(
        {
            "duration_s": [90.0, 10.0, 50.0],
            "duration_total_s": [100.0, None, 50.0],
            "lambda_memory_mb": [8192, 4096, None],
            "gb_seconds": [1.0, None, None],
        }
    )
    # the recorded 1.0 wins; 10 s (no billed wall) x 4 GB; 50 s x the 4 GB default
    assert tool._billed_gb_seconds(invokes) == 1.0 + 40.0 + 200.0
    assert tool._billed_gb_seconds(invokes.iloc[:0]) is None
    assert tool._billed_gb_seconds(pd.DataFrame({"n_obs": [1]})) is None
