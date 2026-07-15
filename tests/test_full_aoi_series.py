"""Tests for the full-AOI release series append (issue #202 leg 1).

Pure/offline: the append core (``full_aoi_series.py``) flattens the harness run
records, reindexes to a stable flat schema, retains only ``release`` runs, and
dedups one row per ``(commit, target)``. No AWS, no network.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
sys.path.insert(0, str(SCRIPTS))

import full_aoi_series as fas  # noqa: E402


def _record(commit="c0", target="full_aoi_neon_o9_inline_nomask", event="release", **over):
    """A minimal full-AOI run record shaped like run_full_aoi_benchmark --out-json."""
    r = {
        "target": target,
        "timestamp": "2026-07-15T00:00:00Z",
        "commit": commit,
        "ref": "v0.25.0",
        "event": event,
        "pr_number": None,
        "aoi": "NEON SERC AOP box",
        "temporal": {"start": "2018-10-13", "end": "2026-03-15"},
        "grid_size": "o9",
        "grid_type": "healpix",
        "aggregator": "tdigest",
        "index_backend": "inline",
        "aoi_mask": False,
        "sidecar_cache": None,
        "parent_order": 9,
        "child_order": 19,
        "mortie_moc_order": 12,
        "shard_area_km2": 162.0,
        "memory_gb": 4.0,
        "price_per_gb_sec": 1.3e-05,
        "zagg_version": "0.25.0",
        "n_shards": 4,
        "n_shards_ok": 4,
        "n_shards_error": 0,
        "total_obs": 1_000_000,
        "aoi_mask_build_s": None,
        "shardmap_build_s": 1.2,
        "per_shard_granules": [10, 20, 30, 66],
        "apriori_estimate": {"est_cost_usd": 0.01},
        "lambda_seconds": 300.0,
        "gb_seconds": 1200.0,
        "cost_usd": 0.016,
        "total_wall_s": 120.0,
        "setup_s": 3.0,
        "fanout_s": 110.0,
        "finalize_s": 7.0,
        "worker_max_s": 100.0,
        "worker_median_s": 70.0,
        "worker_pct_timeout": 0.0,
        "max_memory_mb": 2200.0,
        "write_throughput": {
            "invoke_retries_total": 2,
            "invoke_throttle_shards": 1,
            "s3_slowdown_shards": 0,
            "cells_timeout": 0,
        },
    }
    r.update(over)
    return r


def test_flatten_spreads_write_throughput_and_drops_nested():
    flat = fas.flatten_record(_record())
    # write_throughput lifted into wt_* scalar columns...
    assert flat["wt_invoke_retries_total"] == 2
    assert flat["wt_invoke_throttle_shards"] == 1
    assert flat["wt_s3_slowdown_shards"] == 0
    assert flat["wt_cells_timeout"] == 0
    # ...and the nested / planning-only fields are gone.
    assert "write_throughput" not in flat
    for k in ("temporal", "per_shard_granules", "apriori_estimate"):
        assert k not in flat


def test_flatten_missing_write_throughput_yields_nulls():
    # A dry-run record has no write_throughput -> wt_* are None, not a KeyError.
    flat = fas.flatten_record(_record(write_throughput=None))
    assert flat["wt_invoke_retries_total"] is None
    rec = _record()
    del rec["write_throughput"]
    flat2 = fas.flatten_record(rec)
    assert flat2["wt_cells_timeout"] is None


def test_records_to_frame_is_column_stable():
    df = fas.records_to_frame([_record()])
    # Exactly the canonical schema, in order -- no nested/extra columns leak in.
    assert list(df.columns) == fas.FULL_AOI_COLUMNS
    assert df.iloc[0]["cost_usd"] == 0.016
    assert df.iloc[0]["n_shards"] == 4


def test_append_dedups_last_write_per_commit_target():
    existing = fas.records_to_frame([_record(commit="c0", cost_usd=0.010)])
    # Re-running the same (commit, target) replaces, not appends.
    updated = fas.append_records(existing, [_record(commit="c0", cost_usd=0.020)])
    assert len(updated) == 1
    assert updated.iloc[0]["cost_usd"] == 0.020
    # A different target on the same commit is a distinct row.
    updated2 = fas.append_records(
        updated, [_record(commit="c0", target="full_aoi_neon_o9_sidecar_mask")]
    )
    assert len(updated2) == 2


def test_append_to_empty_series():
    empty = fas.load_series("does-not-exist.parquet")
    assert empty.empty and list(empty.columns) == fas.FULL_AOI_COLUMNS
    updated = fas.append_records(empty, [_record()])
    assert len(updated) == 1


def test_main_retains_only_release_runs(tmp_path):
    records = [
        _record(commit="c1", event="release"),
        _record(commit="c2", event="merge"),  # dropped: not a release
        _record(commit="c3", event=""),  # dropped: dry-run / manual
    ]
    recs = tmp_path / "recs.json"
    recs.write_text(json.dumps(records))
    series = tmp_path / "full_aoi_series.parquet"
    rc = fas.main(["--series", str(series), "--records", str(recs)])
    assert rc == 0
    df = fas.load_series(series)
    assert len(df) == 1 and df.iloc[0]["commit"] == "c1"


def test_main_rejects_non_list(tmp_path):
    recs = tmp_path / "bad.json"
    recs.write_text(json.dumps({"not": "a list"}))
    try:
        fas.main(["--series", str(tmp_path / "s.parquet"), "--records", str(recs)])
    except SystemExit as e:
        assert "list" in str(e.code)
    else:
        raise AssertionError("expected SystemExit on non-list records JSON")


# --- per-release full-AOI renderer (issue #202 leg 1) ---------------------


def _matrix_records(commit, ref, cost):
    """One release's 4 full-AOI targets (inline/sidecar x mask/nomask)."""
    out = []
    for ib in ("inline", "sidecar"):
        for mask in (False, True):
            suffix = "mask" if mask else "nomask"
            out.append(
                _record(
                    commit=commit,
                    ref=ref,
                    target=f"full_aoi_neon_o9_{ib}_{suffix}",
                    event="release",
                    index_backend=ib,
                    aoi_mask=mask,
                    cost_usd=cost,
                )
            )
    return out


def test_full_aoi_history_derives_avg_cost_and_filters_release():
    ps = pytest.importorskip("plot_series")
    recs = _matrix_records("c1", "v0.24.0", cost=0.016) + [
        _record(commit="c2", event="merge")  # non-release must not leak in
    ]
    df = fas.records_to_frame(recs)
    hist = ps._full_aoi_history(df)
    assert set(hist["event"]) == {"release"}  # merge row filtered out
    # AOI-average cost/100 km^2 = cost_usd * 100 / (n_shards * shard_area_km2)
    # = 0.016 * 100 / (4 * 162) = 0.0024691...
    assert hist["cost_per_100km2_usd"].iloc[0] == pytest.approx(0.016 * 100 / (4 * 162.0))


def test_make_full_aoi_release_figure_renders_and_empty_is_false(tmp_path):
    # matplotlib lives in the benchmark/analysis extras, not the `test` extra CI
    # installs -- skip the render (like the other plot_series tests) when absent.
    pytest.importorskip("matplotlib")
    ps = pytest.importorskip("plot_series")
    df = fas.records_to_frame(
        _matrix_records("c1", "v0.24.0", 0.016) + _matrix_records("c2", "v0.25.0", 0.018)
    )
    for name, (col, label) in ps.FULL_AOI_FIGURES.items():
        out = tmp_path / f"{name}.png"
        assert ps.make_full_aoi_release_figure(df, col, label, out) is True
        assert out.exists() and out.stat().st_size > 0
    # Nothing retained yet -> False (Pages index omits the section, no broken image).
    import pandas as pd

    assert (
        ps.make_full_aoi_release_figure(pd.DataFrame(), "cost_usd", "c", tmp_path / "x.png")
        is False
    )
