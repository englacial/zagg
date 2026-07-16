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
        "setup_s": 5.0,
        "fanout_s": 140.0,
        "finalize_s": 65.0,
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
    # The record is the series schema plus the two JSON-only object-count keys
    # (issue #240) that update_series's reindex deliberately drops.
    assert set(rec) == set(bench_metrics.RECORD_COLUMNS) | {
        "objects_per_shard",
        "objects_mismatch",
    }


def test_build_record_max_memory_null_safe():
    # An empty/legacy summary leaves max_memory_mb null so old rows degrade.
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record({}, grid=g, context={"target": "t"})
    assert rec["max_memory_mb"] is None
    assert "max_memory_mb" in bench_metrics.RECORD_COLUMNS


def test_codec_column_is_last_and_threaded(monkeypatch):
    # Stable-schema rule: new columns append LAST. codec (issue #133) and the
    # read axis (issue #170) are threaded from context and null on rows that
    # omit them; the wall-breakdown columns (issue #180) were appended after
    # them, so codec/read are no longer literally last -- assert order, not
    # tail position.
    cols = bench_metrics.RECORD_COLUMNS
    assert cols.index("codec") < cols.index("read") < cols.index("total_wall_s")
    # The wall breakdown (#180), read backend (#193), object counts (#240) and
    # store layout (#240 phase 4) appended in that order.
    assert cols[-8:] == [
        "total_wall_s",
        "setup_s",
        "fanout_s",
        "finalize_s",
        "index_backend",
        "objects_total",
        "objects_expected",
        "store_layout",
    ]
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={"codec": "sharded"})
    assert rec["codec"] == "sharded"
    # Absent in context -> null (a frozen/legacy row carries no codec).
    cached = bench_metrics.build_record(
        _summary(), grid=g, context={"codec": "inner", "read": "cached"}
    )
    assert cached["read"] == "cached"
    live = bench_metrics.build_record(_summary(), grid=g, context={"index_backend": "sidecar"})
    assert live["index_backend"] == "sidecar"
    legacy = bench_metrics.build_record(_summary(), grid=g, context={"target": "t"})
    assert legacy["codec"] is None
    assert legacy["read"] is None


def test_wall_breakdown_columns_threaded_and_rendered():
    # issue #180: total AOI wall + its phases flow from the agg summary into
    # the record (finalize_s is retained in the series), and total wall surfaces
    # in the rendered table. finalize is kept in the record but dropped from the
    # rendered table (issue #202: it added noise/crowding to the table PNGs).
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={"target": "t"})
    assert rec["total_wall_s"] == 210.0
    assert rec["setup_s"] == 5.0
    assert rec["fanout_s"] == 140.0
    assert rec["finalize_s"] == 65.0
    # A summary missing the phases (older worker) -> null, not a crash.
    bare = bench_metrics.build_record({"total_obs": 1}, grid=g, context={})
    assert bare["total_wall_s"] is None and bare["finalize_s"] is None
    cells = bench_metrics.format_record_cells(rec)
    assert cells["wall (s)"] == "210.0"
    assert "wall (s)" in bench_metrics.TABLE_HEADERS
    # finalize is retained in the series/record but no longer a rendered column.
    assert "finalize_s" in bench_metrics.RECORD_COLUMNS
    assert "finalize (s)" not in bench_metrics.TABLE_HEADERS


def test_run_target_threads_codec_into_record():
    # run_benchmark must record the target's codec onto the row (dry-run, no AWS).
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    rec = run_benchmark.run_target(
        "tdigest_healpix_o10_inline",
        manifest,
        base,
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["index_backend"] == "inline"  # issue #193: the live matrix's A/B axis
    assert rec["codec"] is None  # codec axis retired from the live matrix


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
        "tdigest_healpix_o9_sidecar_nomask",
        manifest,
        base,
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["target"] == "tdigest_healpix_o9_sidecar_nomask"
    assert rec["aggregator"] == "tdigest"
    assert rec["grid_type"] == "healpix"
    assert rec["shard_key"] == 5347395636851376137  # o9 densest cell
    assert rec["shard_area_km2"] == pytest.approx(162.1, abs=1.0)  # o9 HEALPix cell
    assert rec["total_obs"] is None  # no dispatch in dry-run


def _fake_record(target, *, total_obs, max_memory_mb):
    # Minimal record with the fields main()'s per-target print + the empty-metrics
    # guard read (issue #145). Enough to drive main() without a real dispatch.
    return {
        "target": target,
        "total_obs": total_obs,
        "runtime_s": None,
        "cost_per_shard_usd": None,
        "cost_per_100km2_usd": None,
        "max_memory_mb": max_memory_mb,
    }


def test_main_fails_on_empty_target(tmp_path, monkeypatch):
    # A silent OOM comes back as obs=0 / max_memory_mb=None; main() must exit
    # non-zero so the job goes red instead of retaining a junk row (issue #145).
    monkeypatch.setattr(
        run_benchmark,
        "run_target",
        lambda name, *a, **k: _fake_record(name, total_obs=0, max_memory_mb=None),
    )
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 1


def test_main_fails_on_null_memory_alone(tmp_path, monkeypatch):
    # The guard is `not total_obs OR max_memory_mb is None`: a target that recorded
    # observations but no memory reading must still fail, so the `max_memory_mb is
    # None` clause is exercised independently of the obs=0 clause (issue #145).
    monkeypatch.setattr(
        run_benchmark,
        "run_target",
        lambda name, *a, **k: _fake_record(name, total_obs=999, max_memory_mb=None),
    )
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 1


def test_main_no_fail_on_empty_opts_out(tmp_path, monkeypatch):
    monkeypatch.setattr(
        run_benchmark,
        "run_target",
        lambda name, *a, **k: _fake_record(name, total_obs=0, max_memory_mb=None),
    )
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--no-fail-on-empty",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 0


def test_main_passes_on_populated_target(tmp_path, monkeypatch):
    monkeypatch.setattr(
        run_benchmark,
        "run_target",
        lambda name, *a, **k: _fake_record(name, total_obs=12345, max_memory_mb=800.0),
    )
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 0


def test_main_dry_run_writes_outputs(tmp_path):
    out_json = tmp_path / "metrics.json"
    out_md = tmp_path / "comment.md"
    run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
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
    assert len(records) == 1 and records[0]["target"] == "tdigest_healpix_o10_inline"
    assert "tdigest_healpix_o10_inline" in out_md.read_text()


def test_main_dry_run_exempt_from_fail_on_empty(tmp_path):
    # --dry-run emits empty metrics by design, so --fail-on-empty (default on) must
    # NOT trip on it -- main() returns 0 even though total_obs is null (issue #145).
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--dry-run",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 0


def test_main_dispatches_targets_concurrently(tmp_path, monkeypatch):
    # Two targets must be in-flight at once for a Barrier(2) to release; a serial
    # loop would block forever on the first wait() and the timeout would trip it.
    # Also asserts the output records are re-ordered back to the requested order
    # even though completion order is nondeterministic (issue #137).
    import threading

    import zagg.auth as zauth

    monkeypatch.setattr(zauth, "ensure_logged_in", lambda: None)  # no real login
    barrier = threading.Barrier(2, timeout=10)

    def fake(name, *a, **k):
        barrier.wait()  # rendezvous: only clears if both targets run concurrently
        return _fake_record(name, total_obs=1, max_memory_mb=100.0)

    monkeypatch.setattr(run_benchmark, "run_target", fake)
    out_json = tmp_path / "metrics.json"
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--target",
            "tdigest_healpix_o10_sidecar",
            "--commit",
            "cafe123",
            "--out-json",
            str(out_json),
        ]
    )
    assert rc == 0
    recs = json.loads(out_json.read_text())
    assert [r["target"] for r in recs] == [
        "tdigest_healpix_o10_inline",
        "tdigest_healpix_o10_sidecar",
    ]


def test_main_warms_auth_once_before_parallel_dispatch(tmp_path, monkeypatch):
    # With >1 target and a real (non-dry-run) dispatch, main() authenticates ONCE
    # up front so the concurrent targets don't race earthaccess's global auth
    # singleton (issue #137). run_target is stubbed so no real AWS/agg is touched.
    import zagg.auth as zauth

    calls = {"login": 0, "targets": []}
    monkeypatch.setattr(
        zauth, "ensure_logged_in", lambda: calls.__setitem__("login", calls["login"] + 1)
    )
    monkeypatch.setattr(
        run_benchmark,
        "run_target",
        lambda name, *a, **k: (
            calls["targets"].append(name) or _fake_record(name, total_obs=1, max_memory_mb=100.0)
        ),
    )
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--target",
            "tdigest_healpix_o10_sidecar",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 0
    assert calls["login"] == 1  # exactly one warm-up, before the fan-out
    assert len(calls["targets"]) == 2


def test_main_skips_auth_warmup_on_dry_run(tmp_path, monkeypatch):
    # --dry-run does no dispatch, so it must not authenticate (no creds in CI dry
    # runs / local wiring checks).
    import zagg.auth as zauth

    warmed = {"n": 0}
    monkeypatch.setattr(zauth, "ensure_logged_in", lambda: warmed.__setitem__("n", warmed["n"] + 1))
    run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--target",
            "tdigest_healpix_o10_sidecar",
            "--dry-run",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert warmed["n"] == 0


def test_main_unknown_target_fails_before_any_dispatch(tmp_path, monkeypatch):
    # An unknown target name aborts before the pool spins up -- no target is
    # dispatched (so we never pay for a partial run on a typo'd matrix).
    called = {"n": 0}

    def fake(name, *a, **k):
        called["n"] += 1
        return _fake_record(name, total_obs=1, max_memory_mb=100.0)

    monkeypatch.setattr(run_benchmark, "run_target", fake)
    with pytest.raises(SystemExit, match="unknown target"):
        run_benchmark.main(
            [
                "--targets",
                str(BENCH / "targets.json"),
                "--target",
                "tdigest_healpix_o10_inline",
                "--target",
                "does_not_exist",
                "--commit",
                "cafe123",
                "--out-json",
                str(tmp_path / "metrics.json"),
            ]
        )
    assert called["n"] == 0  # no dispatch happened


# --- manifest integrity (the pin is internally consistent) ----------------


def test_targets_manifest_consistent():
    # Provisional targets (issue #130 block) are included: the 88S stress
    # targets (issue #148) live there, and their shard-map pins must be just as
    # internally consistent as the committed matrix's.
    manifest = json.loads((BENCH / "targets.json").read_text())
    provisional = {
        k: v for k, v in manifest.get("provisional_targets", {}).items() if k != "_comment"
    }
    for tname, t in {**manifest["targets"], **provisional}.items():
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
    monkeypatch.setattr(run_benchmark, "_measure_objects", lambda *a, **k: None)
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
    monkeypatch.setattr(run_benchmark, "_measure_objects", lambda *a, **k: None)
    run_benchmark.run_target(
        "tdigest_healpix_o9_sidecar_nomask",
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
    monkeypatch.setattr(run_benchmark, "_measure_objects", lambda *a, **k: None)
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
        # The o10 shardmap meta has no shard_key here; run_target reads it (and
        # renders it through grid.shard_label — issue #199), so add a valid
        # packed morton word.
        manifest["shardmaps"]["healpix_o10"]["shard_key"] = 5347395636851376137
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


def _matrix_rows(commit, **kw):
    """Live reset-matrix rows (issue #202): tdigest o9 x inline/sidecar x
    mask/nomask, carrying index_backend and the ``_mask``/``_nomask`` target
    suffix (the AOI-mask axis is read off the name -- there is no record column
    for it) so plot_series.main renders the 2x2 reset figures."""
    rows = []
    for aoi in ("nomask", "mask"):
        for backend in ("inline", "sidecar"):
            r = _rec_row(commit, f"tdigest_healpix_o9_{backend}_{aoi}", **kw)
            r["grid_size"] = "o9"
            r["index_backend"] = backend
            rows.append(r)
    return rows


def test_plot_series_smoke(tmp_path):
    pytest.importorskip("matplotlib")
    import plot_series

    rows = [
        r for i in range(3) for r in _matrix_rows(f"c{i}", cost=0.004 + i * 0.001, rt=180 + i * 10)
    ]
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    outdir = tmp_path / "site"
    plot_series.main(["--series", str(series), "--out", str(outdir)])
    assert (outdir / "index.html").exists()
    # Live matrix figures (issue #193): *_matrix.png + matrix_table.png.
    assert (outdir / "cost_per_shard_matrix.png").exists()
    assert (outdir / "cost_per_100km2_matrix.png").exists()
    assert (outdir / "matrix_table.png").exists()
    assert (outdir / "latest.md").exists()
    assert (outdir / "metrics.json").exists()
    # The retired codec/frozen figures are NOT regenerated.
    assert not (outdir / "codec_table.png").exists()
    assert not (outdir / "cost_per_shard.png").exists()


def _latest_rows(commit, ts, **kw):
    rows = _matrix_rows(commit, **kw)
    for r in rows:
        r["timestamp"] = ts
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
    assert {r["index_backend"] for r in metrics} == {"inline", "sidecar"}


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
    assert not (outdir / "cost_per_shard_matrix.png").exists()


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


def _cached_row(commit, order):
    # Issue #170 cached-read companion: codec stays "inner" (it still drives
    # output.grid.sharded); the read axis is what slots it into the third
    # renderer column. Built through bench_metrics.build_record so the test
    # pins the WHOLE pipe -- ctx -> record -> frame -> layout (the first fold
    # of this finding wired the layout but dropped the key in the record).
    import bench_metrics

    ctx = dict(
        timestamp=f"2026-01-01T00:00:0{len(commit) % 10}Z",
        commit=commit,
        ref="main",
        event="merge",
        target=f"tdigest_healpix_{order}_cached",
        aggregator="tdigest",
        grid_type="healpix",
        grid_size=order,
        shard_key=0,
        codec="inner",
        read="cached",
    )
    g = HealpixGrid(parent_order=int(order[1:]), child_order=19)
    return bench_metrics.build_record(
        {"lambda_time_s": 200.0, "estimated_cost_usd": 0.005, "max_memory_mb": 1200.0},
        grid=g,
        context=ctx,
    )


def test_codec_layout_is_fixed_2x3_sharded_inner_by_order():
    import plot_series

    hist = update_series.records_to_frame(_full_codec_matrix("c0"))
    grid, nrows, ncols = plot_series._codec_layout(hist)
    assert (nrows, ncols) == (3, 3)  # rows o9->o11, cols sharded/inner/cached (issue #170)
    # Third column is the issue #170 cached-read slot; blank here because this
    # synthetic history carries no cached rows.
    assert grid == [
        ["tdigest_healpix_o9_sharded", "tdigest_healpix_o9_inner", None],
        ["tdigest_healpix_o10_sharded", "tdigest_healpix_o10_inner", None],
        ["tdigest_healpix_o11_sharded", "tdigest_healpix_o11_inner", None],
    ]
    # With cached rows present (issue #170), each lands in its own column and
    # the real inner target keeps its slot -- the cached companion shares
    # codec "inner", so a regression here silently overwrites the inner panel.
    hist = update_series.records_to_frame(
        _full_codec_matrix("c0") + [_cached_row("c0", o) for o in ("o9", "o10", "o11")]
    )
    grid, _, _ = plot_series._codec_layout(hist)
    assert grid == [
        ["tdigest_healpix_o9_sharded", "tdigest_healpix_o9_inner", "tdigest_healpix_o9_cached"],
        [
            "tdigest_healpix_o10_sharded",
            "tdigest_healpix_o10_inner",
            "tdigest_healpix_o10_cached",
        ],
        [
            "tdigest_healpix_o11_sharded",
            "tdigest_healpix_o11_inner",
            "tdigest_healpix_o11_cached",
        ],
    ]


def test_codec_layout_blanks_missing_order():
    # A history missing an order (here o9) renders that row as two blank cells;
    # the grid is still a fixed 2x3 so the matrix shape is stable.
    import plot_series

    rows = [_codec_row("c0", o, c) for o in ("o10", "o11") for c in ("sharded", "inner")]
    grid, nrows, ncols = plot_series._codec_layout(update_series.records_to_frame(rows))
    assert (nrows, ncols) == (3, 3)  # cached col, issue #170
    assert grid[0] == [None, None, None]  # o9 row blank (incl. cached col, issue #170)
    assert grid[1] == ["tdigest_healpix_o10_sharded", "tdigest_healpix_o10_inner", None]


def test_matrix_layout_is_inline_sidecar_by_aoi_mask():
    # issue #202 reset: the live matrix is a fixed (nomask,mask) x (inline,sidecar)
    # grid at o9. Rows = AOI-mask arm (off on top), cols = read backend.
    import plot_series

    rows = _matrix_rows("c0")
    grid, nrows, ncols = plot_series._matrix_layout(update_series.records_to_frame(rows))
    assert (nrows, ncols) == (2, 2)  # rows nomask/mask, cols inline/sidecar
    assert grid == [
        ["tdigest_healpix_o9_inline_nomask", "tdigest_healpix_o9_sidecar_nomask"],
        ["tdigest_healpix_o9_inline_mask", "tdigest_healpix_o9_sidecar_mask"],
    ]


def test_matrix_layout_blanks_missing_arm():
    # An AOI-mask arm with no rows leaves its cells None (grid stays fixed 2x2).
    import plot_series

    rows = [r for r in _matrix_rows("c0") if r["target"].endswith("_mask")]
    grid, nrows, ncols = plot_series._matrix_layout(update_series.records_to_frame(rows))
    assert (nrows, ncols) == (2, 2)
    assert grid[0] == [None, None]  # nomask arm absent
    assert grid[1] == ["tdigest_healpix_o9_inline_mask", "tdigest_healpix_o9_sidecar_mask"]


def test_matrix_history_selects_reset_rows():
    # Only the reset rows (o9 + index_backend + _mask/_nomask suffix) are the live
    # matrix; codec/frozen rows AND pre-reset o9/o10 rows (no aoi suffix) excluded.
    import plot_series

    stale = _rec_row("c0", "tdigest_healpix_o10_inline")  # pre-reset live row
    stale["grid_size"] = "o10"
    stale["index_backend"] = "inline"
    df = update_series.records_to_frame(
        _matrix_rows("c0") + _full_codec_matrix("c0") + [stale, _rec_row("c0", "frozen_t")]
    )
    hist = plot_series._matrix_history(df)
    assert set(hist["index_backend"]) == {"inline", "sidecar"}
    assert all(t.endswith(("_mask", "_nomask")) for t in hist["target"])
    assert "tdigest_healpix_o10_inline" not in set(hist["target"])  # stale row dropped


def test_aoi_axis_reads_target_suffix():
    # issue #202: the AOI-mask arm is derived from the target-name suffix (no
    # record column). ``_nomask`` must NOT read as ``mask``.
    import plot_series

    assert plot_series._aoi_axis("tdigest_healpix_o9_inline_mask") == "mask"
    assert plot_series._aoi_axis("tdigest_healpix_o9_sidecar_nomask") == "nomask"
    assert plot_series._aoi_axis("tdigest_healpix_o9_inline") == "nomask"  # pre-reset name


def test_full_aoi_release_figure_is_skeleton(tmp_path):
    # issue #202 leg 1: the per-release full-AOI NEON figure is a render skeleton
    # (its harness/schema are another agent's deliverable). It must no-op cleanly
    # -- return False, write nothing -- so main() skips it and the index omits the
    # section rather than embedding a broken image.
    import plot_series

    df = update_series.records_to_frame(_matrix_rows("c0"))
    out = tmp_path / "full_aoi.png"
    assert plot_series.make_full_aoi_release_figure(df, "cost_per_shard_usd", "cost", out) is False
    assert not out.exists()


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


def test_plot_main_emits_matrix_and_archives_old(tmp_path):
    # issue #193: main() renders the live inline/sidecar matrix; any retained
    # codec/frozen PNGs already in the outdir are embedded as an ARCHIVED
    # section below (and are NOT regenerated).
    pytest.importorskip("matplotlib")
    import plot_series

    rows = _matrix_rows("c0")
    series = tmp_path / "series.parquet"
    update_series.save_series(update_series.records_to_frame(rows), series)
    outdir = tmp_path / "site"
    outdir.mkdir()
    # A pre-existing archived PNG (as it would sit on the benchmarks branch).
    (outdir / "codec_table.png").write_bytes(b"\x89PNG\r\n\x1a\n")
    plot_series.main(["--series", str(series), "--out", str(outdir)])
    # Live matrix rendered; archived PNG left untouched, embedded below.
    assert (outdir / "matrix_table.png").exists()
    assert (outdir / "cost_per_shard_matrix.png").exists()
    assert not (outdir / "codec_table.png").stat().st_size > 100  # not regenerated
    html = (outdir / "index.html").read_text()
    assert "inline/sidecar × AOI-mask" in html
    assert "Archived (frozen as of issues #193 / #202)" in html
    assert html.index("inline/sidecar × AOI-mask") < html.index("Archived")


def test_88s_nested_pin_invariant():
    # The 88S o10 stress pin must live INSIDE the pinned o9 stress shard (one
    # o9 extraction pass covers both orders, issue #148). This runs offline in
    # milliseconds — the gated weekly drift job must not be the only guard on
    # the nesting or on the nested_in reference itself.
    import test_benchmark_shardmap as drift

    from zagg.config import load_config
    from zagg.grids import from_config

    manifest = json.loads((BENCH / "targets.json").read_text())
    nested = {k: v for k, v in manifest["shardmaps"].items() if "nested_in" in v}
    assert "healpix_o10_88s" in nested  # the pin this guards
    for sm_key, sm_meta in nested.items():
        parent_meta = manifest["shardmaps"].get(sm_meta["nested_in"])
        assert parent_meta is not None, f"{sm_key}: nested_in references a missing entry"
        parent_grid = from_config(
            load_config(str(drift._config_for_shardmap(sm_meta["nested_in"])))
        )
        containing = drift._containing_shard(parent_grid, int(sm_meta["shard_key"]))
        assert containing == int(parent_meta["shard_key"]), (
            f"{sm_key}: pinned shard {sm_meta['shard_key']} is not inside its "
            f"nested_in parent {parent_meta['shard_key']} (got {containing})"
        )


# --- store object-count tripwire (issue #240) --------------------------------


def _objects_payload(mismatch=None):
    return {
        "objects_total": 10,
        "objects_expected": 10,
        "objects_per_shard": {"1121121": 4},
        "objects_mismatch": mismatch,
    }


def test_objects_columns_are_last_and_threaded():
    # Stable-schema rule: new columns append LAST (issue #240; store_layout
    # appended after the object counts in phase 4).
    cols = bench_metrics.RECORD_COLUMNS
    assert cols[-3:] == ["objects_total", "objects_expected", "store_layout"]
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={}, objects=_objects_payload())
    assert rec["objects_total"] == 10
    assert rec["objects_expected"] == 10
    # per_shard/mismatch ride the metrics.json record only -- deliberately NOT
    # series columns (update_series's reindex drops them).
    assert rec["objects_per_shard"] == {"1121121": 4}
    assert rec["objects_mismatch"] is None
    assert "objects_per_shard" not in cols and "objects_mismatch" not in cols
    # No measurement (dry-run / legacy) -> null columns, not a crash.
    bare = bench_metrics.build_record(_summary(), grid=g, context={})
    assert bare["objects_total"] is None and bare["objects_expected"] is None


def test_objects_cell_rendered_in_table():
    assert "objects" in bench_metrics.TABLE_HEADERS
    g = HealpixGrid(parent_order=11, child_order=19)
    rec = bench_metrics.build_record(_summary(), grid=g, context={}, objects=_objects_payload())
    assert bench_metrics.format_record_cells(rec)["objects"] == "10/10"
    # Bounded (non-exact) expectation records measured only.
    bounded = dict(rec, objects_expected=None)
    assert bench_metrics.format_record_cells(bounded)["objects"] == "10"
    # Legacy parquet rows degrade to NaN; empty records have nothing.
    legacy = dict(rec, objects_total=float("nan"))
    assert bench_metrics.format_record_cells(legacy)["objects"] == "n/a"
    assert bench_metrics.format_record_cells({})["objects"] == "n/a"


def test_run_target_measures_objects_when_store_written(monkeypatch, tmp_path):
    # A real (non-dry) dispatch with a store must LIST it and thread the
    # measurement into the record; the pinned shard key is what gets attributed.
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    calls = {}

    def fake_agg(config, **kwargs):
        return {"total_obs": 5, "max_memory_mb": 100.0}

    def fake_measure(config, grid, store, shard_key, *, region):
        calls["measure"] = (store, int(shard_key), region)
        return _objects_payload()

    monkeypatch.setattr("zagg.runner.agg", fake_agg)
    monkeypatch.setattr(run_benchmark, "_measure_objects", fake_measure)
    store = str(tmp_path / "t.zarr")
    rec = run_benchmark.run_target(
        "tdigest_healpix_o9_inline_nomask",
        manifest,
        base,
        store=store,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=False,
    )
    assert calls["measure"] == (store, 5347395636851376137, "us-west-2")
    assert rec["objects_total"] == 10 and rec["objects_expected"] == 10


def test_run_target_dry_run_skips_object_measurement(monkeypatch):
    # Dry-run writes no store, so nothing is LISTed and the columns stay null.
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    monkeypatch.setattr(
        run_benchmark,
        "_measure_objects",
        lambda *a, **k: pytest.fail("dry-run must not LIST a store"),
    )
    rec = run_benchmark.run_target(
        "tdigest_healpix_o9_inline_nomask",
        manifest,
        base,
        store="s3://bucket/t.zarr",
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    assert rec["objects_total"] is None and rec["objects_mismatch"] is None


def test_main_fails_on_object_mismatch(tmp_path, monkeypatch):
    # The tripwire (issues #240/#215): a mismatch hard-fails the run, but only
    # AFTER metrics.json is written so the failing counts are still recorded.
    def fake_run_target(name, *a, **k):
        rec = _fake_record(name, total_obs=100, max_memory_mb=800.0)
        rec["objects_mismatch"] = "total objects 1030 != expected 10"
        return rec

    monkeypatch.setattr(run_benchmark, "run_target", fake_run_target)
    out_json = tmp_path / "metrics.json"
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--commit",
            "cafe123",
            "--out-json",
            str(out_json),
        ]
    )
    assert rc == 1
    assert json.loads(out_json.read_text())[0]["objects_mismatch"].startswith("total objects")


def test_main_no_fail_on_object_mismatch_opts_out(tmp_path, monkeypatch):
    def fake_run_target(name, *a, **k):
        rec = _fake_record(name, total_obs=100, max_memory_mb=800.0)
        rec["objects_mismatch"] = "total objects 1030 != expected 10"
        return rec

    monkeypatch.setattr(run_benchmark, "run_target", fake_run_target)
    rc = run_benchmark.main(
        [
            "--targets",
            str(BENCH / "targets.json"),
            "--target",
            "tdigest_healpix_o10_inline",
            "--no-fail-on-object-mismatch",
            "--commit",
            "cafe123",
            "--out-json",
            str(tmp_path / "metrics.json"),
        ]
    )
    assert rc == 0


# --- hive layout axis (issue #240 phase 4) -----------------------------------


def test_run_target_threads_store_layout():
    # The hive regression arm records store_layout="hive" from its config; the
    # flat matrix arms record "flat" (dry-run, no AWS).
    manifest, base = run_benchmark.load_targets(str(BENCH / "targets.json"))
    common = dict(
        store=None,
        region="us-west-2",
        function_name="process-shard",
        context={"commit": "deadbee", "event": "pr"},
        dry_run=True,
    )
    hive_rec = run_benchmark.run_target("tdigest_healpix_o9_hive", manifest, base, **common)
    assert hive_rec["store_layout"] == "hive"
    assert hive_rec["index_backend"] == "inline"
    assert hive_rec["shard_key"] == 5347395636851376137  # same pinned densest o9 cell
    flat_rec = run_benchmark.run_target(
        "tdigest_healpix_o9_inline_nomask", manifest, base, **common
    )
    assert flat_rec["store_layout"] == "flat"


def test_hive_config_expected_counts_exact():
    # The committed hive arm's model: exact, with the coverage sidecar + the
    # store-root manifest/MOC in the fixed counts (hive defaults coverage_moc).
    import bench_objects

    from zagg.config import get_coverage_moc, get_store_layout, load_config
    from zagg.grids import from_config

    cfg = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9_hive.yaml"))
    assert get_store_layout(cfg) == "hive" and get_coverage_moc(cfg) is True
    grid = from_config(cfg)
    exp = bench_objects.expected_object_counts(
        grid, n_shards=1, store_layout="hive", coverage_moc=True
    )
    # 4 arrays (cell_ids/morton/count/h_tdigest): leaf root+group zarr.json (2)
    # + 4 array zarr.json + 4 sharded data objects + coverage sidecar = 11;
    # store root = morton_hive.json + coverage.moc = 2.
    assert exp == {
        "metadata": 2,
        "per_shard_min": 11,
        "per_shard_max": 11,
        "total_min": 13,
        "total_max": 13,
        "exact": True,
    }


def test_matrix_mask_excludes_hive_rows():
    # Defensive flat-only gate (issue #240 phase 4): even a hive row that
    # carries an aoi suffix + inline backend must not claim a 2x2 panel cell.
    import plot_series

    hive_row = _rec_row("c0", "tdigest_healpix_o9_hive_nomask")
    hive_row["index_backend"] = "inline"
    hive_row["store_layout"] = "hive"
    df = update_series.records_to_frame(_matrix_rows("c0") + [hive_row])
    hist = plot_series._matrix_history(df)
    assert "tdigest_healpix_o9_hive_nomask" not in set(hist["target"])
    assert len(hist) == 4  # the flat 2x2 is untouched
