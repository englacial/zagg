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
import update_series  # noqa: E402

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
        "max_memory_mb": 1963.0,
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
    assert rec["max_memory_mb"] == 1963.0  # threaded from the summary (issue #120)
    assert set(rec) == set(bench_metrics.RECORD_COLUMNS)


def test_build_record_max_memory_null_safe():
    # An empty/legacy summary leaves max_memory_mb null so old rows degrade.
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record({}, grid=g, context={"target": "t"})
    assert rec["max_memory_mb"] is None
    assert "max_memory_mb" in bench_metrics.RECORD_COLUMNS


def test_memory_pct_of_cap_maps_near_red():
    # 1963 MB on a 2 GB cap -> ~0.96 (the OOM-adjacent run #1 figure, issue #120).
    pct = bench_metrics.memory_pct_of_cap(1963.0, 2.0)
    assert pct == pytest.approx(0.9585, abs=1e-3)


def test_memory_pct_of_cap_null_safe():
    assert bench_metrics.memory_pct_of_cap(None, 2.0) is None
    assert bench_metrics.memory_pct_of_cap(1000.0, None) is None
    assert bench_metrics.memory_pct_of_cap(1000.0, 0.0) is None
    # A legacy parquet row degrades to NaN (not None) -> still treated as missing.
    assert bench_metrics.memory_pct_of_cap(float("nan"), 2.0) is None


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


def test_comment_markdown_worker_note_banner():
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={"target": "t", "commit": "abc"})
    note = "Worker = stable main, not this PR's code."
    md = bench_metrics.comment_markdown([rec], worker_note=note)
    assert f"> ⚠️ {note}" in md  # banner rendered above the table
    assert md.index(note) < md.index("| target |")  # ...and before the table
    # Default (no note) has no banner.
    assert "⚠️" not in bench_metrics.comment_markdown([rec])


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


# --- update_series (parquet store) ----------------------------------------


def _rec_row(commit, target, event="merge", cost=0.005, rt=200.0, mem=1200.0):
    return {
        "timestamp": f"2026-01-01T00:00:0{len(commit) % 10}Z",
        "commit": commit,
        "ref": "main",
        "event": event,
        "pr_number": None,
        "target": target,
        "aggregator": "gain_bias",
        "grid_type": "healpix",
        "grid_size": "o11",
        "shard_key": 1,
        "n_granules": 44,
        "total_obs": 5_000_000,
        "runtime_s": rt,
        "gb_seconds": 400.0,
        "cost_per_shard_usd": cost,
        "shard_area_km2": 10.13,
        "cost_per_100km2_usd": cost * 100 / 10.13,
        "function_timeout_s": 720,
        "worker_pct_timeout": 0.28,
        "memory_gb": 2.0,
        "price_per_gb_sec": 1.33334e-05,
        "zagg_version": "9.9.9",
        "max_memory_mb": mem,
    }


def test_records_to_frame_column_stable():
    df = update_series.records_to_frame([_rec_row("a", "t1")])
    assert list(df.columns) == bench_metrics.RECORD_COLUMNS


def test_append_records_grows_and_dedups():
    df = update_series.load_series("does-not-exist.parquet")
    assert df.empty and list(df.columns) == bench_metrics.RECORD_COLUMNS
    df = update_series.append_records(df, [_rec_row("c1", "t1"), _rec_row("c1", "t2")])
    assert len(df) == 2
    # Re-running the same commit replaces, not duplicates; keep=last wins.
    df = update_series.append_records(df, [_rec_row("c1", "t1", cost=0.009)])
    assert len(df) == 2
    row = df[(df.commit == "c1") & (df.target == "t1")].iloc[0]
    assert row["cost_per_shard_usd"] == 0.009


def test_series_roundtrip(tmp_path):
    path = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame([_rec_row("c1", "t1")]), path)
    update_series.main(
        ["--series", str(path), "--records", str(_write_json(tmp_path, [_rec_row("c2", "t1")]))]
    )
    out = update_series.load_series(path)
    assert set(out["commit"]) == {"c1", "c2"}


def _write_json(tmp_path, obj):
    p = tmp_path / "records.json"
    p.write_text(json.dumps(obj))
    return p


# --- plot_series (smoke; needs matplotlib) --------------------------------


def test_plot_series_smoke(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    rows = [
        _rec_row(f"c{i}", t, cost=0.004 + i * 0.001, rt=180 + i * 10)
        for i in range(3)
        for t in ("t1", "t2")
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    outdir = tmp_path / "site"
    plot_series.main(["--series", str(series), "--out", str(outdir)])
    assert (outdir / "index.html").exists()
    assert (outdir / "cost_per_shard.png").exists()
    assert (outdir / "cost_per_100km2.png").exists()


def test_plot_series_empty_writes_placeholder(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    outdir = tmp_path / "site"
    plot_series.main(["--series", str(tmp_path / "missing.parquet"), "--out", str(outdir)])
    # No data -> index exists with placeholder, no PNGs.
    assert (outdir / "index.html").exists()
    assert not (outdir / "cost_per_shard.png").exists()


def test_update_series_main_retains_merge_only(tmp_path):
    path = tmp_path / "series.parquet"
    recs = [
        _rec_row("m1", "t1", event="merge"),
        _rec_row("p1", "t1", event="pr"),  # ephemeral PR run -> must not be retained
    ]
    update_series.main(["--series", str(path), "--records", str(_write_json(tmp_path, recs))])
    out = update_series.load_series(path)
    assert set(out["commit"]) == {"m1"}


def test_make_figure_returns_false_when_no_targets(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    # Rows that are merges but carry no target label -> nothing to panel, no crash.
    rows = [_rec_row("c1", None, event="merge")]
    df = update_series.records_to_frame(rows)
    assert plot_series.make_figure(df, "cost_per_shard_usd", "cost", tmp_path / "x.png") is False


# --- marker memory colouring (issue #120) ---------------------------------


def test_memory_fractions_maps_each_row():
    import plot_series

    # 1024 MB / 2048 -> 0.5; 1963 MB / 2048 -> ~0.96 (the OOM-adjacent figure).
    sub = update_series.records_to_frame(
        [_rec_row("c1", "t1", mem=1024.0), _rec_row("c2", "t1", mem=1963.0)]
    )
    fracs = plot_series.memory_fractions(sub)
    assert fracs[0] == pytest.approx(0.5)
    assert fracs[1] == pytest.approx(0.9585, abs=1e-3)  # near the red end of the scale


def test_memory_fractions_none_when_memory_missing():
    import plot_series

    sub = update_series.records_to_frame([_rec_row("c1", "t1")])
    sub = sub.assign(max_memory_mb=None)  # legacy row: no memory recorded
    assert plot_series.memory_fractions(sub) == [None]


def test_make_figure_colours_markers_by_memory(tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series
    from matplotlib.collections import PathCollection

    # Stop make_figure from closing the figure so we can introspect the artists.
    captured = {}
    real_close = plt.close

    def _capture_close(fig):
        captured["fig"] = fig  # grab the figure on its way out

    monkeypatch.setattr(plt, "close", _capture_close)

    # Round-trip through parquet so the (commit, target) points land in order and
    # carry real memory; 512/2048 -> 0.25, 1963/2048 -> ~0.96 (near the red end).
    rows = [_rec_row("c0", "t1", mem=512.0), _rec_row("c1", "t1", mem=1963.0)]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    df = update_series.load_series(series)

    out = tmp_path / "fig.png"
    assert plot_series.make_figure(df, "cost_per_shard_usd", "cost", out) is True
    assert out.exists()
    fig = captured["fig"]

    # The cost markers are a scatter whose colour array IS the memory fraction.
    scatters = [c for ax in fig.axes for c in ax.collections if isinstance(c, PathCollection)]
    arrays = [s.get_array() for s in scatters if s.get_array() is not None]
    assert any(
        len(a) == 2 and a[0] == pytest.approx(0.25) and a[1] == pytest.approx(0.9585, abs=1e-3)
        for a in arrays
    )
    # A colorbar (its own axes) is attached for the memory scale.
    assert len(fig.axes) > 1
    real_close(fig)


def test_make_figure_null_memory_renders_uncoloured(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    # A legacy row reads back as NaN memory -> uncoloured marker, no crash.
    rows = [_rec_row("c0", "t1", mem=900.0), _rec_row("c1", "t1")]
    series = tmp_path / "series.parquet"
    df = update_series.records_to_frame(rows)
    df = df.assign(max_memory_mb=[900.0, None])
    update_series.save_series(df, series)
    out = tmp_path / "fig.png"
    assert (
        plot_series.make_figure(
            update_series.load_series(series), "cost_per_shard_usd", "cost", out
        )
        is True
    )
    assert out.exists()
