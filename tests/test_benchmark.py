"""Tests for the Lambda-benchmark CI tooling (issue #110).

Covers the pure metric derivations in ``.github/scripts/bench_metrics.py`` and the
target-resolution / record-building wiring in ``run_benchmark.py`` (dry-run, no
AWS). Also pins the committed targets manifest against the shard maps so the
benchmark targets stay internally consistent.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(SCRIPTS))

import bench_metrics  # noqa: E402
import run_benchmark  # noqa: E402

from zagg.grids import HealpixGrid, RectilinearGrid  # noqa: E402

# --- select_densest_shard -------------------------------------------------


def test_select_densest_picks_most_granules():
    sm = {"shard_keys": [10, 20, 30], "granules": [[1], [1, 2, 3], [1, 2]]}
    assert bench_metrics.select_densest_shard(sm) == (20, 3)


def test_select_densest_tiebreak_lowest_key():
    # Two shards tie at 2 granules -> the lower key wins, deterministically.
    sm = {"shard_keys": [99, 7], "granules": [[1, 2], [3, 4]]}
    assert bench_metrics.select_densest_shard(sm) == (7, 2)


def test_select_densest_empty_raises():
    with pytest.raises(ValueError):
        bench_metrics.select_densest_shard({"shard_keys": [], "granules": []})


# --- shard_area_km2 -------------------------------------------------------


def test_healpix_shard_area_orders():
    g11 = HealpixGrid(parent_order=11, child_order=19)
    g10 = HealpixGrid(parent_order=10, child_order=19)
    a11 = bench_metrics.shard_area_km2(g11)
    a10 = bench_metrics.shard_area_km2(g10)
    # order-11 ~10.1 km2, order-10 4x larger.
    assert a11 == pytest.approx(10.13, abs=0.2)
    assert a10 == pytest.approx(4 * a11, rel=1e-9)


def test_rect_shard_area():
    g = RectilinearGrid(
        crs="EPSG:32618",
        resolution=10,
        bounds=[358300, 4299600, 370300, 4311600],
        chunk_shape=(300, 300),
    )
    # 300 cells * 10 m = 3 km per side -> 9 km2.
    assert bench_metrics.shard_area_km2(g) == pytest.approx(9.0, rel=1e-9)


def test_unknown_grid_area_raises():
    with pytest.raises(TypeError):
        bench_metrics.shard_area_km2(object())


# --- build_record ---------------------------------------------------------


def _summary():
    return {
        "total_obs": 5_000_000,
        "wall_time_s": 210.0,
        "lambda_time_s": 200.0,
        "worker_max_s": 205.0,
        "gb_seconds": 410.0,
        "estimated_cost_usd": 0.00547,
        "function_timeout_s": 720,
        "worker_pct_timeout": 0.285,
    }


def test_build_record_cost_per_100km2():
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(
        _summary(),
        grid=g,
        context={
            "target": "t",
            "aggregator": "gain_bias",
            "grid_type": "healpix",
            "grid_size": "o11",
            "commit": "abc1234",
            "shard_key": 5,
        },
        n_granules=44,
        zagg_version="9.9.9",
    )
    area = bench_metrics.shard_area_km2(g)
    assert rec["cost_per_shard_usd"] == 0.00547
    assert rec["cost_per_100km2_usd"] == pytest.approx(0.00547 * 100.0 / area)
    assert rec["runtime_s"] == 205.0  # worker_max_s preferred
    assert rec["n_granules"] == 44
    assert rec["zagg_version"] == "9.9.9"
    assert set(rec) == set(bench_metrics.RECORD_COLUMNS)


def test_build_record_runtime_fallback():
    g = HealpixGrid(parent_order=11, child_order=19)
    summ = {"estimated_cost_usd": 0.001, "gb_seconds": 1.0, "wall_time_s": 9.0}
    rec = bench_metrics.build_record(summ, grid=g, context={})
    assert rec["runtime_s"] == 9.0  # falls back past missing worker_max_s/lambda_time_s


def test_build_record_handles_empty_summary():
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record({}, grid=g, context={"target": "t"})
    assert rec["cost_per_shard_usd"] is None
    assert rec["cost_per_100km2_usd"] is None
    assert rec["runtime_s"] is None


# --- comment_markdown -----------------------------------------------------


def test_comment_markdown_has_marker_and_rows():
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(
        _summary(),
        grid=g,
        context={"target": "gain_bias_healpix_o11", "commit": "abcdef0123"},
    )
    md = bench_metrics.comment_markdown([rec])
    assert "<!-- zagg-benchmark -->" in md  # stable anchor for comment upsert
    assert "gain_bias_healpix_o11" in md
    assert "abcdef0" in md  # short sha


def test_comment_markdown_empty():
    assert "<!-- zagg-benchmark -->" in bench_metrics.comment_markdown([])


# --- run_benchmark wiring (dry-run, no AWS) -------------------------------


def test_run_target_dry_run():
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    rec = run_benchmark.run_target(
        "gain_bias_healpix_o11",
        manifest,
        base,
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["target"] == "gain_bias_healpix_o11"
    assert rec["aggregator"] == "gain_bias"
    assert rec["grid_type"] == "healpix"
    assert rec["shard_key"] == 5347394812217655307
    assert rec["shard_area_km2"] == pytest.approx(10.13, abs=0.2)
    assert rec["total_obs"] is None  # no dispatch in dry-run


def test_main_dry_run_writes_outputs(tmp_path):
    out_json = tmp_path / "metrics.json"
    out_md = tmp_path / "comment.md"
    run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10",
            "--dry-run",
            "--commit",
            "cafe123",
            "--out-json",
            str(out_json),
            "--out-comment",
            str(out_md),
        ]
    )
    records = json.loads(out_json.read_text())
    assert len(records) == 1 and records[0]["target"] == "tdigest_healpix_o10"
    assert "tdigest_healpix_o10" in out_md.read_text()


# --- manifest integrity (the pin is internally consistent) ----------------


def test_targets_manifest_consistent():
    manifest = json.loads((BENCH / "targets.json").read_text())
    for tname, t in manifest["targets"].items():
        assert (BENCH / t["config"]).exists(), f"{tname}: missing config"
        sm_meta = manifest["shardmaps"][t["shardmap"]]
        sm_path = BENCH / sm_meta["path"]
        assert sm_path.exists(), f"{tname}: missing shardmap"
        # Pinned shard_key is the densest shard of its committed shard map.
        sm = json.loads(sm_path.read_text())
        key, n = bench_metrics.select_densest_shard(sm)
        assert key == sm_meta["shard_key"], f"{t['shardmap']}: stale pinned shard_key"
        assert n == sm_meta["n_granules"], f"{t['shardmap']}: stale n_granules"
