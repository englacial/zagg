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


def test_codec_column_is_last_and_threaded(monkeypatch):
    # The codec A/B label (issue #133) is the newest schema column -> appended LAST
    # (stable-schema rule), threaded from context, and null on rows that omit it
    # (legacy/frozen).
    assert bench_metrics.RECORD_COLUMNS[-1] == "codec"
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={"codec": "sharded"})
    assert rec["codec"] == "sharded"
    # Absent in context -> null (a frozen/legacy row carries no codec).
    legacy = bench_metrics.build_record(_summary(), grid=g, context={"target": "t"})
    assert legacy["codec"] is None


def test_run_target_threads_codec_into_record():
    # run_benchmark must record the target's codec onto the row (dry-run, no AWS).
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    rec = run_benchmark.run_target(
        "tdigest_healpix_o10_inner",
        manifest,
        base,
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["codec"] == "inner"


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


def test_format_record_cells_shared_formatting():
    cells = bench_metrics.format_record_cells(
        {
            "target": "t",
            "total_obs": 1_234_567,
            "runtime_s": 200.0,
            "cost_per_shard_usd": 0.005,
            "cost_per_100km2_usd": 0.05,
            "worker_pct_timeout": 0.28,
            "max_memory_mb": 1024.0,
            "memory_gb": 2.0,
        }
    )
    assert cells["obs"] == "1,234,567"
    assert cells["cost/shard"] == "$0.00500"
    assert cells["% cap"] == "50%"
    assert cells["mem_frac"] == pytest.approx(0.5)
    # Missing inputs degrade to n/a, not a crash.
    blank = bench_metrics.format_record_cells({})
    assert blank["obs"] == "n/a" and blank["% cap"] == "n/a" and blank["mem_frac"] is None


def test_latest_markdown_is_retained_table_not_a_comment():
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(
        _summary(),
        grid=g,
        context={
            "target": "tdigest_healpix_o11",
            "commit": "abcdef0123",
            "timestamp": "2026-06-29T21:04:28Z",
        },
    )
    md = bench_metrics.latest_markdown([rec])
    assert "<!-- zagg-benchmark -->" not in md  # not a PR-upsert comment
    assert "tdigest_healpix_o11" in md
    assert "abcdef0" in md  # short sha
    assert "| target |" in md  # the shared table block
    assert "metrics.json" in md  # points agents at the machine-readable companion
    assert "pre-merge runs are not retained" not in md  # this IS the retained point


def test_latest_markdown_empty():
    assert bench_metrics.latest_markdown([]) == "No benchmark records were produced."


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
        "tdigest_healpix_o11_sharded",
        manifest,
        base,
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["target"] == "tdigest_healpix_o11_sharded"
    assert rec["aggregator"] == "tdigest"
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
            "tdigest_healpix_o10_inner",
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
    assert len(records) == 1 and records[0]["target"] == "tdigest_healpix_o10_inner"
    assert "tdigest_healpix_o10_inner" in out_md.read_text()


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


# --- forward sharded-vs-inner matrix (issue #133) -------------------------


def test_forward_matrix_is_tdigest_healpix_arrow_codec_ab():
    # The committed merge matrix is the forward 2x3 codec A/B: every target is
    # tdigest / HEALPix / arrow and carries a codec (sharded|inner) matched to its
    # sharded bool, paired per order across o9/o10/o11. The carrier is config-driven
    # (issue #132): targets inherit ``arrow`` from each config rather than restating
    # a redundant per-target ``handoff`` key (test_committed_targets_drop_redundant_handoff).
    manifest = json.loads((BENCH / "targets.json").read_text())
    targets = manifest["targets"]
    by_order: dict[str, set[str]] = {}
    for tname, t in targets.items():
        assert t["aggregator"] == "tdigest", tname
        assert t["grid_type"] == "healpix", tname
        assert "handoff" not in t, tname
        assert t["codec"] in ("sharded", "inner"), tname
        # codec label and the sharded bool run_benchmark applies must agree.
        assert t["sharded"] is (t["codec"] == "sharded"), tname
        # The target name encodes its order + codec, matching the metadata.
        assert tname == f"tdigest_healpix_{t['grid_size']}_{t['codec']}", tname
        by_order.setdefault(t["grid_size"], set()).add(t["codec"])
    # Each present order is a complete A/B pair (both columns), never a half-row.
    for order, codecs in by_order.items():
        assert codecs == {"sharded", "inner"}, f"{order}: incomplete codec pair {codecs}"


def test_o9_row_is_live():
    # The o9 row landed (phase 1): its shard map is built and pinned, both columns
    # reference it, and the _pending_o9 hold-out stanza is gone.
    manifest = json.loads((BENCH / "targets.json").read_text())
    assert "_pending_o9" not in manifest, "o9 hold-out stanza should be removed"
    o9_targets = {n for n, t in manifest["targets"].items() if t["grid_size"] == "o9"}
    assert o9_targets == {"tdigest_healpix_o9_sharded", "tdigest_healpix_o9_inner"}
    for t in (manifest["targets"][n] for n in o9_targets):
        assert t["shardmap"] == "healpix_o9"
        assert t["config"] == "configs/atl03_tdigest_healpix_o9.yaml"
    # The shardmap entry is pinned with a real (numeric) densest shard_key.
    sm = manifest["shardmaps"]["healpix_o9"]
    assert isinstance(sm["shard_key"], int)
    assert sm["path"] == "shardmaps/sm_healpix_o9.json"
    assert (BENCH / sm["path"]).exists()
    assert (BENCH / "configs" / "atl03_tdigest_healpix_o9.yaml").exists()


def test_every_live_shardmap_resolves_to_a_config():
    # The drift test parametrizes over manifest["shardmaps"], and the consistency
    # test over manifest["targets"]; both rely on every LIVE shardmap resolving to
    # a referencing config (the drift test's _config_for_shardmap lookup). This
    # guards that wiring -- the invariant that made o9 drop-in coverage automatic
    # the moment its entry landed, and that keeps any future order covered too.
    import test_benchmark_shardmap as drift

    for sm_key in drift.MANIFEST["shardmaps"]:
        cfg = drift._config_for_shardmap(sm_key)  # raises if no target references it
        assert cfg.exists()


# --- provisional (PR-tree-only) handoff targets (issue #130) ---------------


def test_provisional_targets_excluded_from_merge_matrix():
    # The pandas/arrow carrier-comparison targets are PR-tree-only: "run all"
    # (no --target) must not iterate them, so they never join the merge matrix.
    manifest = json.loads((BENCH / "targets.json").read_text())
    all_names = run_benchmark.all_target_names(manifest)
    provisional = set(manifest["provisional_targets"]) - {"_comment"}
    assert provisional, "expected provisional targets"
    assert provisional.isdisjoint(all_names)
    assert set(manifest["targets"]).issubset(all_names)


def test_provisional_targets_consistent_and_carry_handoff():
    manifest = json.loads((BENCH / "targets.json").read_text())
    expected = {
        "scalar_pandas_healpix_o11": "pandas",
        "scalar_arrow_healpix_o11": "arrow",
    }
    for tname, handoff in expected.items():
        t = manifest["provisional_targets"][tname]
        assert (BENCH / t["config"]).exists(), f"{tname}: missing config"
        assert manifest["shardmaps"][t["shardmap"]]  # shardmap is reused/known
        assert t["handoff"] == handoff


def test_provisional_target_resolves_and_threads_handoff(monkeypatch):
    # /benchmark --target <provisional> must resolve and thread its handoff into agg.
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    captured = {}

    import zagg.runner as runner

    def fake_agg(config, **kwargs):
        captured["handoff"] = kwargs.get("handoff")
        return {}

    monkeypatch.setattr(runner, "agg", fake_agg)
    run_benchmark.run_target(
        "scalar_arrow_healpix_o11",
        manifest,
        base,
        store="s3://b/x.zarr",
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=False,
    )
    assert captured["handoff"] == "arrow"


def test_committed_targets_drop_redundant_handoff(monkeypatch):
    # issue #132: the merge-matrix targets no longer restate the carrier; they
    # inherit it from each config. Only the provisional A/B targets pin handoff.
    manifest = json.loads((BENCH / "targets.json").read_text())
    for tname, t in manifest["targets"].items():
        assert "handoff" not in t, f"{tname}: handoff should be inherited from the config"


def test_committed_target_inherits_handoff_from_config(monkeypatch):
    # issue #132: a committed target with no handoff key inherits the config carrier
    # (get_handoff -> "arrow" for the benchmark configs, which set no handoff).
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    captured = {}

    import zagg.runner as runner

    def fake_agg(config, **kwargs):
        captured["handoff"] = kwargs.get("handoff")
        return {}

    monkeypatch.setattr(runner, "agg", fake_agg)
    run_benchmark.run_target(
        "tdigest_healpix_o11_sharded",
        manifest,
        base,
        store="s3://b/x.zarr",
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=False,
    )
    assert captured["handoff"] == "arrow"


def test_sharded_knob_applied_to_grid_config(monkeypatch):
    # The forward benchmark (issue #133) carries the ShardingCodec as a per-target
    # ``sharded`` key so one config drives both columns: run_target must push it
    # onto config.output['grid']['sharded'] (where get_sharded reads it) before
    # building the grid + dispatching. A synthetic manifest pairs the o9 t-digest
    # config (K=256, so sharded is valid) with each codec value.
    base = BENCH
    captured = {}

    import zagg.runner as runner
    from zagg.config import get_sharded

    def fake_agg(config, **kwargs):
        captured["sharded"] = get_sharded(config)
        return {}

    monkeypatch.setattr(runner, "agg", fake_agg)
    for codec, want in (("sharded", True), ("inner", False)):
        manifest = {
            "shardmaps": {"healpix_o10": {"path": "shardmaps/sm_healpix_o10.json"}},
            "targets": {
                "t": {
                    "config": "configs/atl03_tdigest_healpix_o9.yaml",
                    "shardmap": "healpix_o10",
                    "aggregator": "tdigest",
                    "grid_type": "healpix",
                    "grid_size": "o9",
                    "handoff": "arrow",
                    "codec": codec,
                    "sharded": want,
                }
            },
        }
        # The o10 shardmap meta has no shard_key here; run_target reads it, so add one.
        manifest["shardmaps"]["healpix_o10"]["shard_key"] = 0
        run_benchmark.run_target(
            "t",
            manifest,
            base,
            store="s3://b/x.zarr",
            region="us-west-2",
            function_name="process-shard",
            context={"commit": "deadbee", "event": "pr"},
            dry_run=False,
        )
        assert captured["sharded"] is want, f"codec={codec} -> sharded={want}"


def test_scalar_config_is_genuinely_scalar():
    # The scalar benchmark config must be GENUINELY scalar -- every field a plain
    # per-cell stat (no vector/ragged) -- so the pandas/arrow carrier comparison
    # isolates carrier cost, not output shape (issue #130).
    from zagg.config import get_agg_fields, get_output_signature, load_config

    config = load_config(str(BENCH / "configs" / "atl03_scalar_healpix_o11.yaml"))
    fields = get_agg_fields(config)
    assert fields, "expected aggregation fields"
    for name, meta in fields.items():
        assert get_output_signature(meta)["kind"] == "scalar", f"{name} is not scalar"


# --- per-target AOI override resolution (issue #121) -----------------------


def test_override_resolution_falls_back_to_defaults():
    # A shard map with no override inherits the top-level aoi/temporal/cmr
    # *by identity* -- existing NEON entries resolve byte-identically to today.
    import test_benchmark_shardmap as drift

    aoi, temporal, cmr = drift.resolve_aoi_temporal_cmr({"path": "x", "shard_key": 0})
    assert aoi is drift.MANIFEST["aoi"]
    assert temporal is drift.MANIFEST["temporal"]
    assert cmr is drift.MANIFEST["cmr"]


def test_override_resolution_uses_overrides():
    import test_benchmark_shardmap as drift

    sm_meta = {
        "path": "x",
        "shard_key": 0,
        "aoi": {"file": "antarctic_88s.geojson", "name": "88S dense"},
        "temporal": {"start": "2019-01-01", "end": "2020-01-01"},
        "cmr": {"short_name": "ATL03", "version": "007", "provider": "P", "footprint": "swath"},
    }
    aoi, temporal, cmr = drift.resolve_aoi_temporal_cmr(sm_meta)
    assert aoi == sm_meta["aoi"]
    assert temporal == sm_meta["temporal"]
    assert cmr == sm_meta["cmr"]


def test_override_resolution_partial_override():
    # aoi overridden, temporal/cmr omitted -> override wins, rest falls back.
    import test_benchmark_shardmap as drift

    sm_meta = {"path": "x", "shard_key": 0, "aoi": {"file": "f.geojson", "name": "n"}}
    aoi, temporal, cmr = drift.resolve_aoi_temporal_cmr(sm_meta)
    assert aoi == sm_meta["aoi"]
    assert temporal is drift.MANIFEST["temporal"]
    assert cmr is drift.MANIFEST["cmr"]


def test_antarctic_88s_aoi_fixture_loads_near_turning_latitude():
    # The 88S stress-target AOI ships as a usable fixture even before its built
    # shard map / live target lands (that step needs CMR + a re-pin). It must
    # load and sit near the +/-88 deg ICESat-2 turning latitude (issue #121).
    from zagg.catalog import load_polygon, polygon_to_bbox

    parts = load_polygon(str(BENCH / "antarctic_88s.geojson"))
    minx, miny, maxx, maxy = polygon_to_bbox(parts)
    assert miny <= -87.5 and maxy <= -87.0  # well into the high-density polar band
    assert -90.0 <= miny < maxy <= -80.0


# --- update_series (parquet store) ----------------------------------------


def _rec_row(
    commit,
    target,
    event="merge",
    cost=0.005,
    rt=200.0,
    mem=1200.0,
    agg="gain_bias",
    grid="healpix",
    area=10.13,
):
    return {
        "timestamp": f"2026-01-01T00:00:0{len(commit) % 10}Z",
        "commit": commit,
        "ref": "main",
        "event": event,
        "pr_number": None,
        "target": target,
        "aggregator": agg,
        "grid_type": grid,
        "grid_size": "o11",
        "shard_key": 1,
        "n_granules": 44,
        "total_obs": 5_000_000,
        "runtime_s": rt,
        "gb_seconds": 400.0,
        "cost_per_shard_usd": cost,
        "shard_area_km2": area,
        "cost_per_100km2_usd": cost * 100 / area if area else 0.0,
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
    # Latest-merge snapshot artifacts (issue #110).
    assert (outdir / "latest_table.png").exists()
    assert (outdir / "latest.md").exists()
    assert (outdir / "metrics.json").exists()


def _latest_rows(commit, ts, **kw):
    rows = []
    for t in ("t1", "t2"):
        r = _rec_row(commit, t, **kw)
        r["timestamp"] = ts
        rows.append(r)
    return rows


def test_plot_series_latest_artifacts_pick_newest_merge(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    rows = _latest_rows("old111", "2026-06-01T00:00:00Z", rt=180) + _latest_rows(
        "new999", "2026-06-29T21:00:00Z", rt=250
    )
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    outdir = tmp_path / "site"
    plot_series.main(["--series", str(series), "--out", str(outdir)])

    md = (outdir / "latest.md").read_text()
    assert "new999"[:7] in md  # the newest merge...
    assert "old111" not in md  # ...and only it
    metrics = json.loads((outdir / "metrics.json").read_text())
    assert {r["commit"] for r in metrics} == {"new999"}
    assert {r["target"] for r in metrics} == {"t1", "t2"}


def test_write_latest_metrics_is_null_safe_json(tmp_path):
    import plot_series

    # A merge whose memory wasn't reported -> NaN in the frame must serialise to
    # JSON null, not the bare token NaN (which isn't valid JSON).
    rows = _latest_rows("c12345", "2026-06-29T21:00:00Z", mem=None)
    df = update_series.records_to_frame(rows)
    out = tmp_path / "metrics.json"
    assert plot_series.write_latest_metrics(df, out) is True
    text = out.read_text()
    assert "NaN" not in text  # valid JSON, no bare NaN token
    metrics = json.loads(text)  # parses cleanly
    assert all(r["max_memory_mb"] is None for r in metrics)


def test_latest_records_empty_when_no_merge(tmp_path):
    import plot_series

    # Only a PR row -> no retained merge -> no latest artifacts.
    df = update_series.records_to_frame([_rec_row("p1", "t1", event="pr")])
    assert plot_series.latest_records(df) == []
    assert plot_series.write_latest_markdown(df, tmp_path / "latest.md") is False
    assert plot_series.write_latest_metrics(df, tmp_path / "metrics.json") is False


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


def test_make_figure_circle_runtime_and_bottom_row_labels(tmp_path, monkeypatch):
    # The benchmark-plot polish (PR #123 fold of #125's chart): runtime markers
    # are circles (not squares) so the memory-coloured cost marker shows through,
    # and only the bottom-row panels carry commit labels -- the upper rows are
    # blanked by hand (``tick_params(labelbottom=False)``), not a shared x-axis.
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    # Two aggregators x two grid families -> a 2x2 grid; two merge points each.
    rows = [
        _rec_row(f"c{i}", f"{agg}_{grid}", agg=agg, grid=grid)
        for i in range(2)
        for agg in ("gain_bias", "tdigest")
        for grid in ("rectilinear", "healpix")
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "fig.png"
    assert plot_series.make_figure(
        update_series.load_series(series), "cost_per_shard_usd", "c", out
    )
    fig = captured["fig"]

    # Right-axis runtime lines use round ('o') markers, never squares ('s').
    markers = [ln.get_marker() for ax in fig.axes for ln in ax.get_lines()]
    assert "o" in markers and "s" not in markers

    # Only the bottom-row panels show tick labels; the top row's are hidden so the
    # commit labels aren't repeated up the grid.
    base = [ax for ax in fig.axes if ax.get_title()]  # the 4 panels (twins/cbar have no title)
    base.sort(key=lambda ax: ax.get_subplotspec().rowspan.start)
    top, bottom = base[:2], base[2:]
    assert all(not any(t.get_text() for t in ax.get_xticklabels()) for ax in top)
    assert all(any(t.get_text() for t in ax.get_xticklabels()) for ax in bottom)


def test_make_figure_uneven_commit_sets_label_per_panel(tmp_path, monkeypatch):
    # Regression for the per-target x-label misalignment class: when targets have
    # *different* commit sets (some missing an early merge), each bottom panel must
    # carry ITS OWN commits in order -- not another panel's labels. A uniform-commit
    # fixture can't catch this. Four targets -> a 2x2 grid, so this also exercises
    # the row-hiding pass (top row blanked, bottom row labelled).
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    # ``_rec_row`` derives the timestamp from the commit-name length, so name
    # lengths set the merge order. Two aggregators x two grids -> a 2x2 grid; the
    # layout puts gain_bias on the top row and tdigest on the bottom. The four
    # targets carry deliberately uneven commit sets so each bottom panel must show
    # its own commits in order.
    def recs(agg, grid, commits):
        return [_rec_row(c, f"{agg}_{grid}", agg=agg, grid=grid) for c in commits]

    rows = (
        recs("gain_bias", "rectilinear", ("a", "bb", "ccc"))
        + recs("gain_bias", "healpix", ("dd", "eee"))
        + recs("tdigest", "rectilinear", ("f", "gg"))  # bottom-left
        + recs("tdigest", "healpix", ("h", "ii", "jjj", "kkkk"))  # bottom-right
    )
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "fig.png"
    assert plot_series.make_figure(
        update_series.load_series(series), "cost_per_shard_usd", "c", out
    )
    fig = captured["fig"]

    panels = {ax.get_title(): ax for ax in fig.axes if ax.get_title()}
    # Top row (gain_bias) is hidden; the bottom row (tdigest) carries each panel's
    # own commits, even though the two panels have different commit sets.
    assert not any(t.get_text() for t in panels["gain_bias_rectilinear"].get_xticklabels())
    assert not any(t.get_text() for t in panels["gain_bias_healpix"].get_xticklabels())
    assert [t.get_text() for t in panels["tdigest_rectilinear"].get_xticklabels()] == ["f", "gg"]
    assert [t.get_text() for t in panels["tdigest_healpix"].get_xticklabels()] == [
        "h",
        "ii",
        "jjj",
        "kkkk",
    ]


def test_make_figure_colorbar_has_mb_twin_axis(tmp_path, monkeypatch):
    # The colorbar carries a second axis reading the same scale in absolute MB
    # (issue #120 polish): MB = fraction-of-cap * cap_mb. cap_mb = 2 GB here, so a
    # marker at 1536/2048 = 0.75 of cap must read 1536 MB on the twin axis.
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    rows = [_rec_row("c0", "t1", mem=1024.0), _rec_row("c1", "t1", mem=1536.0)]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "fig.png"
    assert plot_series.make_figure(
        update_series.load_series(series), "cost_per_shard_usd", "c", out
    )
    fig = captured["fig"]

    # The colorbar axes own one secondary (child) axis; its forward transform maps
    # a fraction-of-cap to MB.
    cbar_ax = next(ax for ax in fig.axes if ax.get_xlabel() == "peak memory (% of cap)")
    (mb_ax,) = cbar_ax.child_axes
    assert mb_ax.get_xlabel() == "peak memory (MB)"
    fwd = mb_ax._functions[0]
    assert fwd(0.75) == pytest.approx(1536.0)  # 0.75 * 2 GB cap


def test_make_figure_cost_scatter_drawn_above_runtime_line(tmp_path, monkeypatch):
    # The memory-coloured cost circles must sit ON TOP of the runtime twin's
    # open-circle/dashed-line glyph. The twin axis is drawn after the host, so the
    # fix raises the host axes' z-order above the twin (and hides the host patch).
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series
    from matplotlib.collections import PathCollection

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    rows = [_rec_row("c0", "t1", mem=1024.0), _rec_row("c1", "t1", mem=1536.0)]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "fig.png"
    assert plot_series.make_figure(
        update_series.load_series(series), "cost_per_shard_usd", "c", out
    )
    fig = captured["fig"]

    panel = next(
        ax for ax in fig.axes if any(isinstance(c, PathCollection) for c in ax.collections)
    )
    (twin,) = [ax for ax in fig.axes if ax is not panel and ax.bbox.bounds == panel.bbox.bounds]
    assert panel.get_zorder() > twin.get_zorder()  # cost circles render over the runtime line
    assert panel.patch.get_visible() is False  # so the runtime line shows through gaps
    scatter = next(c for c in panel.collections if isinstance(c, PathCollection))
    assert list(scatter.get_sizes()) == [90]  # marker bumped one size up


# --- panel layout + failed-run handling (issue #121 review) ---------------


def test_panel_layout_rect_left_healpix_right_largest_on_top():
    import plot_series

    # The eight real targets: rect (left) / healpix (right); largest shard per
    # family on top (rect_6km~36, healpix_o10~40 above rect_3km~9, healpix_o11~10).
    specs = [
        ("gain_bias_rect_6km", "gain_bias", "rectilinear", 36.0),
        ("gain_bias_healpix_o10", "gain_bias", "healpix", 40.5),
        ("tdigest_rect_6km", "tdigest", "rectilinear", 36.0),
        ("tdigest_healpix_o10", "tdigest", "healpix", 40.5),
        ("gain_bias_rect_3km", "gain_bias", "rectilinear", 9.0),
        ("gain_bias_healpix_o11", "gain_bias", "healpix", 10.1),
        ("tdigest_rect_3km", "tdigest", "rectilinear", 9.0),
        ("tdigest_healpix_o11", "tdigest", "healpix", 10.1),
    ]
    rows = [_rec_row("c0", t, agg=a, grid=g, area=ar) for t, a, g, ar in specs]
    hist = update_series.records_to_frame(rows)
    grid, nrows, ncols = plot_series._panel_layout(hist)
    assert (nrows, ncols) == (4, 2)
    assert grid == [
        ["gain_bias_rect_6km", "gain_bias_healpix_o10"],  # largest, gain_bias
        ["tdigest_rect_6km", "tdigest_healpix_o10"],  # largest, tdigest
        ["gain_bias_rect_3km", "gain_bias_healpix_o11"],  # smaller, gain_bias
        ["tdigest_rect_3km", "tdigest_healpix_o11"],  # smaller, tdigest
    ]


def test_panel_layout_all_rect_single_column():
    import plot_series

    # No HEALPix targets -> a single (left) column, still largest-shard-first.
    specs = [
        ("gain_bias_rect_6km", "gain_bias", "rectilinear", 36.0),
        ("gain_bias_rect_3km", "gain_bias", "rectilinear", 9.0),
    ]
    rows = [_rec_row("c0", t, agg=a, grid=g, area=ar) for t, a, g, ar in specs]
    grid, nrows, ncols = plot_series._panel_layout(update_series.records_to_frame(rows))
    assert (nrows, ncols) == (2, 1)
    assert grid == [["gain_bias_rect_6km"], ["gain_bias_rect_3km"]]


def test_panel_layout_same_area_distinct_resolutions_dont_collide():
    import plot_series

    # Two same-aggregator rect targets with identical area but different grid_size
    # must land in distinct rows (rank tie-broken on grid_size), not overwrite.
    specs = [
        ("gain_bias_rect_a", "gain_bias", "rectilinear", 9.0),
        ("gain_bias_rect_b", "gain_bias", "rectilinear", 9.0),
    ]
    rows = [_rec_row("c0", t, agg=a, grid=g, area=ar) for t, a, g, ar in specs]
    # Give them distinct grid_size so the tie-break separates them.
    frame = update_series.records_to_frame(rows)
    frame.loc[frame["target"] == "gain_bias_rect_a", "grid_size"] = "3km"
    frame.loc[frame["target"] == "gain_bias_rect_b", "grid_size"] = "4km"
    grid, nrows, ncols = plot_series._panel_layout(frame)
    assert (nrows, ncols) == (2, 1)
    assert {grid[0][0], grid[1][0]} == {"gain_bias_rect_a", "gain_bias_rect_b"}


def test_make_figure_drops_failed_run_zeros(tmp_path, monkeypatch):
    # A zero cost/runtime is a failed run: it must NOT be a connected datapoint.
    # The cost line breaks at the zero (NaN, no dip to 0) and the failure shows as
    # a non-connected 'x' marker so the x-axis/commit alignment is kept.
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import plot_series
    from matplotlib.collections import PathCollection

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    # Middle merge failed (cost=runtime=0); the two flanking merges are real.
    rows = [
        _rec_row("c0", "t1", cost=0.005, rt=200.0),
        _rec_row("c1", "t1", cost=0.0, rt=0.0),
        _rec_row("ccc", "t1", cost=0.006, rt=210.0),
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "fig.png"
    assert plot_series.make_figure(
        update_series.load_series(series), "cost_per_shard_usd", "c", out
    )
    fig = captured["fig"]

    panel = next(ax for ax in fig.axes if ax.get_title() == "t1")
    # Cost line: the failed middle point is NaN, so the line never connects to 0.
    cost_line = next(ln for ln in panel.get_lines() if ln.get_linestyle() == "-")
    ys = cost_line.get_ydata()
    assert np.isnan(ys[1]) and not np.isnan(ys[0]) and not np.isnan(ys[2])
    assert 0.0 not in [y for y in ys if not np.isnan(y)]
    # A distinct, non-line 'x' marker flags the failed run, anchored at the failed
    # x (index 1) -- not joined to the cost line.
    fails = [
        c for c in panel.collections if isinstance(c, PathCollection) and len(c.get_offsets()) == 1
    ]
    assert fails, "expected an 'x' failure marker at the zero point"
    assert fails[0].get_offsets()[0][0] == 1  # the failed merge's x position


# --- forward sharded-vs-inner renderer (issue #133) -----------------------


def _codec_row(commit, order, codec, **kw):
    # A forward-matrix merge row: tdigest/healpix, carrying grid_size + codec so
    # the renderer slots it into the fixed 2x3 (order x sharded/inner) grid.
    r = _rec_row(commit, f"tdigest_healpix_{order}_{codec}", agg="tdigest", grid="healpix", **kw)
    r["grid_size"] = order
    r["codec"] = codec
    return r


def _full_codec_matrix(commit):
    return [
        _codec_row(commit, order, codec)
        for order in ("o9", "o10", "o11")
        for codec in ("sharded", "inner")
    ]


def test_codec_layout_is_fixed_2x3_sharded_inner_by_order():
    import plot_series

    hist = update_series.records_to_frame(_full_codec_matrix("c0"))
    grid, nrows, ncols = plot_series._codec_layout(hist)
    assert (nrows, ncols) == (3, 2)  # rows o9->o11, cols sharded/inner
    assert grid == [
        ["tdigest_healpix_o9_sharded", "tdigest_healpix_o9_inner"],
        ["tdigest_healpix_o10_sharded", "tdigest_healpix_o10_inner"],
        ["tdigest_healpix_o11_sharded", "tdigest_healpix_o11_inner"],
    ]


def test_codec_layout_blanks_missing_order():
    # A history missing an order (here o9) renders that row as two blank cells;
    # the grid is still a fixed 2x3 so the matrix shape is stable.
    import plot_series

    rows = [_codec_row("c0", o, c) for o in ("o10", "o11") for c in ("sharded", "inner")]
    grid, nrows, ncols = plot_series._codec_layout(update_series.records_to_frame(rows))
    assert (nrows, ncols) == (3, 2)
    assert grid[0] == [None, None]  # o9 row blank
    assert grid[1] == ["tdigest_healpix_o10_sharded", "tdigest_healpix_o10_inner"]


def test_codec_and_frozen_histories_split_on_codec():
    import plot_series

    rows = _full_codec_matrix("c0") + [
        _rec_row("c0", "gain_bias_rect_3km", agg="gain_bias", grid="rectilinear")
    ]
    df = update_series.records_to_frame(rows)
    codec = plot_series._codec_history(df)
    frozen = plot_series._frozen_history(df)
    assert set(codec["target"]) == {r["target"] for r in _full_codec_matrix("c0")}
    assert set(frozen["target"]) == {"gain_bias_rect_3km"}


def test_codec_split_handles_parquet_without_codec_column():
    # Backward-compat: the live retained parquet predates the codec column until
    # the first new merge. A frame with NO ``codec`` column must read as all-frozen
    # (the renderer's ``"codec" not in df.columns`` guard), never KeyError.
    import plot_series

    df = update_series.records_to_frame([_rec_row("c0", "t1"), _rec_row("c1", "t1")])
    df = df.drop(columns=["codec"])  # simulate a pre-#133 parquet
    assert not plot_series._codec_mask(df).any()
    assert plot_series._codec_history(df).empty
    assert set(plot_series._frozen_history(df)["target"]) == {"t1"}


def test_make_codec_figure_renders_only_codec_rows(tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import plot_series

    captured = {}
    monkeypatch.setattr(plt, "close", lambda fig: captured.setdefault("fig", fig))

    # Full 2x3 codec matrix + a frozen row that must NOT appear in the codec figure.
    rows = _full_codec_matrix("c0") + [
        _rec_row("c0", "gain_bias_rect_3km", agg="gain_bias", grid="rectilinear")
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    out = tmp_path / "codec.png"
    assert plot_series.make_codec_figure(
        update_series.load_series(series), "cost_per_shard_usd", "cost", out
    )
    assert out.exists()
    titles = {ax.get_title() for ax in captured["fig"].axes if ax.get_title()}
    assert titles == {r["target"] for r in _full_codec_matrix("c0")}
    assert "gain_bias_rect_3km" not in titles


def test_make_figure_frozen_ignores_codec_rows(tmp_path):
    # The frozen figure renders nothing from a series that is all codec rows.
    pytest.importorskip("matplotlib")
    import plot_series

    df = update_series.records_to_frame(_full_codec_matrix("c0"))
    out = tmp_path / "frozen.png"
    assert plot_series.make_figure(df, "cost_per_shard_usd", "cost", out) is False
    assert not out.exists()


def test_make_codec_latest_table_renders_codec_rows(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    df = update_series.records_to_frame(_full_codec_matrix("c0"))
    out = tmp_path / "codec_table.png"
    assert plot_series.make_codec_latest_table(df, out) is True
    assert out.exists()
    # No codec rows -> nothing rendered.
    frozen_only = update_series.records_to_frame([_rec_row("c0", "gain_bias_rect_3km")])
    assert plot_series.make_codec_latest_table(frozen_only, tmp_path / "x.png") is False


def test_plot_main_emits_codec_artifacts_above_frozen(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    rows = _full_codec_matrix("c0") + [
        _rec_row("c0", "gain_bias_rect_3km", agg="gain_bias", grid="rectilinear")
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    outdir = tmp_path / "site"
    plot_series.main(["--series", str(series), "--out", str(outdir)])
    # Forward codec figures + table land beside the frozen ones.
    assert (outdir / "cost_per_shard_codec.png").exists()
    assert (outdir / "codec_table.png").exists()
    assert (outdir / "cost_per_shard.png").exists()  # the frozen rect row still renders
    html = (outdir / "index.html").read_text()
    # The forward (sharded-vs-inner) section is rendered ABOVE the frozen one.
    assert "Sharded vs inner-chunk" in html and "Frozen historical" in html
    assert html.index("Sharded vs inner-chunk") < html.index("Frozen historical")
