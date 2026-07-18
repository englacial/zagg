"""Tests for the raster (S2) release benchmark harness + series (issue #250).

No AWS: manifest/catalog consistency, the pure stage-rollup helper, the series
append core, and the offline ``--dry-run`` plan path. The live dispatch
(``run_target`` with ``dry_run=False``) needs credentials and is exercised
operationally, like the point release leg.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(SCRIPTS))

import raster_series as rs  # noqa: E402
import run_raster_benchmark as rrb  # noqa: E402

# --- manifest + pinned catalog consistency ----------------------------------


def test_raster_targets_manifest_consistent():
    manifest, base = rrb.load_targets(str(BENCH / "targets_raster_neon.json"))
    assert manifest["aoi"]["file"] and (base / manifest["aoi"]["file"]).exists()
    assert manifest["dispatch"]["function_name"] and manifest["dispatch"]["region"]
    assert list(manifest["targets"]) == ["raster_s2_neon_2025"]
    t = manifest["targets"]["raster_s2_neon_2025"]
    assert (base / t["config"]).exists()
    assert (base / t["catalog"]).exists()
    assert t["pipeline"] == "raster"
    # The config must route to the raster pipeline on the production hive store
    # layout (issue #237 promoted, ratified on issue #272); grid geometry stays
    # fullsphere (hive/flat is the store axis, not the grid geometry).
    from zagg.config import get_layout, get_store_layout, load_config

    cfg = load_config(str(base / t["config"]))
    assert (cfg.data_source or {}).get("reader") == "raster"
    assert get_layout(cfg) == "fullsphere"
    assert get_store_layout(cfg) == "hive"


def test_pinned_s2_catalog_carries_raster_entries():
    # The pinned catalog is the fixed granule set (offline, no STAC): 2025
    # Earth Search c1 items over the SERC box, with the raster entry fields
    # (assets + datetime + time_key) ShardMap building and the time index need.
    from zagg.catalog.sources import Catalog

    cat = Catalog.from_geoparquet(str(BENCH / "catalogs" / "cat_s2_neon_2025.parquet"))
    assert len(cat) > 0
    meta = cat.metadata
    assert meta["start_date"] == "2025-01-01" and meta["end_date"] == "2025-12-31"
    assert meta["time_key"] == "s2:datatake_id"
    recs = cat.granule_records()
    assert all(r.get("assets") and r.get("datetime") and r.get("time_key") for r in recs)
    assert set(recs[0]["assets"]) == {"red", "green", "blue", "nir", "scl"}


@pytest.mark.slow
def test_dry_run_builds_shardmap_offline(tmp_path):
    # End-to-end offline: the harness builds the whole-AOI S2 shardmap from the
    # pinned catalog and emits a plan record with null metrics. Slow (mortie
    # shardmap build), gated like the point-leg equivalent.
    out = tmp_path / "m.json"
    rc = rrb.main(
        [
            "--targets",
            str(BENCH / "targets_raster_neon.json"),
            "--dry-run",
            "--event",
            "release",
            "--commit",
            "test",
            "--ref",
            "v0.0.0",
            "--out-json",
            str(out),
            "--artifacts-dir",
            str(tmp_path / "art"),
        ]
    )
    assert rc == 0
    runs = json.loads(out.read_text())
    assert len(runs) == 1
    run = runs[0]
    assert run["n_shards"] > 0
    assert run["grid_size"] == "o9" and run["parent_order"] == 9
    # Dry run dispatches nothing: no stage dicts, and the series read of this
    # record leaves the stage columns null.
    assert "stage_max" not in run
    df = rs.records_to_frame(runs)
    assert df.iloc[0]["stage_fetch_s"] is None or str(df.iloc[0]["stage_fetch_s"]) == "nan"


def test_live_dispatch_requires_store_prefix(tmp_path):
    with pytest.raises(SystemExit, match="store-prefix"):
        rrb.main(
            [
                "--targets",
                str(BENCH / "targets_raster_neon.json"),
                "--event",
                "release",
                "--out-json",
                str(tmp_path / "m.json"),
            ]
        )


# --- stage rollup ------------------------------------------------------------


def test_run_target_dispatches_via_agg_and_records_summary(monkeypatch, tmp_path):
    # The harness dispatches through zagg.runner.agg (issue #250: the runner
    # owns the profiled raster transport) with profile=True and the benchmark
    # no-re-pay policy, and maps the summary's rollups onto the run record.
    import zagg.runner as runner_mod

    captured = {}

    def fake_agg(config, **kwargs):
        captured.update(kwargs)
        return {
            "cells_with_data": 4,
            "cells_error": 0,
            "total_obs": 250,
            "timesteps": 70,
            "wall_time_s": 693.0,
            "template_s": 1.1,
            "lambda_time_s": 2036.0,
            "worker_max_s": 687.0,
            "worker_median_s": 480.0,
            "max_memory_mb": 2890.0,
            "worker_stage_max": {"open": 38.0, "fetch": 512.0, "write": 41.0},
            "worker_stage_counts": {"assets": 425, "tiles": 1300, "geom_hits": 81},
        }

    monkeypatch.setattr(runner_mod, "agg", fake_agg)
    manifest, base = rrb.load_targets(str(BENCH / "targets_raster_neon.json"))
    run = rrb.run_target(
        "raster_s2_neon_2025",
        manifest,
        base,
        store="s3://bucket/raster_s2_neon_2025.zarr",
        region="us-west-2",
        function_name="process-shard",
        context={"timestamp": "t", "commit": "c", "ref": "v0.0.0", "event": "release"},
        dry_run=False,
        artifacts_dir=str(tmp_path),
    )
    assert captured["profile"] is True
    assert captured["max_retries"] == 1  # never re-pay a failed shard (#119)
    assert captured["backend"] == "lambda" and captured["morton_cell"] is None
    assert captured["catalog"].endswith("sm_raster_s2_neon_2025.json")
    assert run["lambda_seconds"] == 2036.0
    assert run["gb_seconds"] == pytest.approx(2036.0 * rrb.LAMBDA_MEMORY_GB)
    assert run["total_wall_s"] == pytest.approx(1.1 + 693.0)
    assert run["max_memory_mb"] == 2890.0
    assert run["stage_max"]["fetch"] == 512.0
    assert run["stage_counts"]["tiles"] == 1300
    # The nested dicts flatten into the series columns downstream.
    df = rs.records_to_frame([run])
    assert df.iloc[0]["stage_fetch_s"] == 512.0
    assert df.iloc[0]["count_tiles"] == 1300


def test_run_target_applies_store_layout_override(tmp_path):
    # The harness's per-target store_layout override (the issue #237 hive-flip
    # path) applies + re-validates the layout inside run_target itself. Both
    # layouts are valid since issue #247, so each override rides end-to-end
    # into the record — proving the promotion path executes the override +
    # re-validation, not just a manual validate.
    import copy

    manifest, base = rrb.load_targets(str(BENCH / "targets_raster_neon.json"))
    ctx = {"timestamp": "t", "commit": "c", "ref": "v0.0.0", "event": "release"}

    for layout in ("flat", "hive"):
        over = copy.deepcopy(manifest)
        over["targets"]["raster_s2_neon_2025"]["store_layout"] = layout
        run = rrb.run_target(
            "raster_s2_neon_2025",
            over,
            base,
            store="s3://bucket/raster_s2_neon_2025.zarr",
            region="us-west-2",
            function_name="process-shard",
            context=ctx,
            dry_run=True,
            artifacts_dir=str(tmp_path),
        )
        assert run["store_layout"] == layout


def _record(commit="c0", target="raster_s2_neon_2025", event="release", **over):
    r = {
        "target": target,
        "timestamp": "2026-07-17T00:00:00Z",
        "commit": commit,
        "ref": "v0.32.0",
        "event": event,
        "pr_number": None,
        "aoi": "NEON SERC AOP box",
        "collection": "sentinel-2-c1-l2a",
        "grid_type": "healpix",
        "grid_size": "o9",
        "parent_order": 9,
        "child_order": 19,
        "n_shards": 4,
        "n_shards_ok": 4,
        "n_shards_error": 0,
        "timesteps": 70,
        "slabs_written": 250,
        "shardmap_build_s": 2.0,
        "template_s": 1.1,
        "lambda_seconds": 2036.0,
        "gb_seconds": 8144.0,
        "cost_usd": 0.1086,
        "total_wall_s": 694.1,
        "fanout_s": 693.0,
        "worker_max_s": 687.0,
        "worker_median_s": 480.0,
        "memory_gb": 4.0,
        "price_per_gb_sec": 1.33334e-05,
        "zagg_version": "0.33.0",
        "per_shard_granules": [10, 20, 25, 30],
        # Distinct values so a column transposition can't pass unnoticed.
        "stage_max": {
            "open": 38.0,
            "geometry": 12.0,
            "fetch": 512.0,
            "decode": 301.0,
            "gather": 154.0,
            "write": 41.0,
        },
        "stage_counts": {"assets": 425, "tiles": 1300, "geom_hits": 81},
        # Peak worker RSS, max across shards (issue #250 raster parity).
        "max_memory_mb": 2890.0,
        "store_layout": "flat",  # hive rows arrive with the issue #237 flip
    }
    r.update(over)
    return r


def test_flatten_spreads_stages_and_drops_nested():
    flat = rs.flatten_record(_record())
    assert flat["stage_open_s"] == 38.0
    assert flat["stage_fetch_s"] == 512.0
    assert flat["stage_write_s"] == 41.0
    assert flat["count_tiles"] == 1300
    assert "stage_max" not in flat and "stage_counts" not in flat
    assert "per_shard_granules" not in flat
    # Missing dicts (dry run / unprofiled) -> null cells, not a KeyError.
    flat2 = rs.flatten_record(_record(stage_max=None, stage_counts=None))
    assert flat2["stage_decode_s"] is None and flat2["count_assets"] is None


def test_records_to_frame_column_stable_and_unknown_stage_stays_json():
    df = rs.records_to_frame([_record(stage_max={"open": 1.0, "warp": 9.0})])
    assert list(df.columns) == rs.RASTER_COLUMNS
    assert df.iloc[0]["stage_open_s"] == 1.0
    assert "stage_warp_s" not in df.columns
    # Memory rides the series (issue #250 raster parity): retained, and the
    # renderers colour raster markers from it + memory_gb like the point legs.
    assert df.iloc[0]["max_memory_mb"] == 2890.0
    assert df.iloc[0]["store_layout"] == "flat"
    assert rs.RASTER_COLUMNS[-2:] == ["max_memory_mb", "store_layout"]


def test_append_dedups_and_main_retains_release_only(tmp_path):
    existing = rs.records_to_frame([_record(commit="c0", cost_usd=0.10)])
    updated = rs.append_records(existing, [_record(commit="c0", cost_usd=0.20)])
    assert len(updated) == 1 and updated.iloc[0]["cost_usd"] == 0.20

    records = [
        _record(commit="c1", event="release"),
        _record(commit="c2", event="merge"),  # dropped: not a release
    ]
    recs = tmp_path / "recs.json"
    recs.write_text(json.dumps(records))
    series = tmp_path / "raster_series.parquet"
    assert rs.main(["--series", str(series), "--records", str(recs)]) == 0
    df = rs.load_series(series)
    assert len(df) == 1 and df.iloc[0]["commit"] == "c1"
