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
        "setup_cost_usd": 0.000156,
        "fanout_s": 110.0,
        "finalize_s": 7.0,
        "worker_max_s": 100.0,
        "worker_median_s": 70.0,
        "worker_pct_timeout": 0.0,
        "max_memory_mb": 2200.0,
        # Worker per-phase straggler split (issue #250; write = the #256
        # split), emitted under profile=True. Distinct values so a column
        # transposition can't pass.
        "worker_phase_max": {"read": 60.0, "index": 5.0, "aggregate": 30.0, "write": 12.0},
        "write_throughput": {
            "invoke_retries_total": 2,
            "invoke_throttle_shards": 1,
            "s3_slowdown_shards": 0,
            "cells_timeout": 0,
        },
        # Store object counts (issue #240, record-only on this leg). Distinct
        # values so a column transposition can't pass unnoticed (review).
        "objects_total": 27,
        "objects_expected": 25,
        "objects_mismatch": None,
        # Layout axis + flat<->hive parity (issue #240 phase 4).
        "store_layout": "flat",
        "parity_ok": None,
        "parity": None,
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


def test_flatten_spreads_worker_phase_max_and_drops_nested():
    # The worker phase split (issue #250) lifts into phase_*_s scalar columns...
    flat = fas.flatten_record(_record())
    assert flat["phase_read_s"] == 60.0
    assert flat["phase_index_s"] == 5.0
    assert flat["phase_aggregate_s"] == 30.0
    assert flat["phase_write_s"] == 12.0
    assert "worker_phase_max" not in flat
    # ...null-safe: no profiling / dry run -> None cells, not a KeyError.
    flat2 = fas.flatten_record(_record(worker_phase_max=None))
    assert flat2["phase_read_s"] is None
    rec = _record()
    del rec["worker_phase_max"]
    flat3 = fas.flatten_record(rec)
    assert flat3["phase_aggregate_s"] is None
    # A phase the worker grows later stays JSON-only (no stray column leaks).
    df = fas.records_to_frame([_record(worker_phase_max={"read": 1.0, "write": 9.0})])
    assert list(df.columns) == fas.FULL_AOI_COLUMNS
    assert df.iloc[0]["phase_read_s"] == 1.0


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
    # setup_cost_usd rides its own column (issue #250 item 3), never cost_usd.
    assert df.iloc[0]["setup_cost_usd"] == 0.000156


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


# --- per-release phase-breakdown figure (issue #250) ------------------------


def _release_records(commit, ref, cost):
    """One release's single collapsed point-leg record (issue #250)."""
    return [
        _record(
            commit=commit,
            ref=ref,
            target="full_aoi_neon_o9_hive_mask",
            store_layout="hive",
            aoi_mask=True,
            cost_usd=cost,
            setup_cost_usd=0.0002,
            finalize_cost_usd=0.0001,
        )
    ]


def test_point_release_history_derivations():
    # plot_summary display derivations (the series stays emission truth):
    # agg = index + aggregate, and the summed billed total = cost_usd +
    # setup_cost_usd + finalize_cost_usd with an exact seconds<->USD relabel.
    psu = pytest.importorskip("plot_summary")
    df = fas.records_to_frame(
        _release_records("c1", "v0.31.0", 0.016)
        + [_record(commit="c9", event="merge")]  # non-release must not leak in
    )
    hist = psu.point_release_history(df)
    assert set(hist["event"]) == {"release"}
    row = hist.iloc[0]
    assert row["phase_agg_s"] == pytest.approx(5.0 + 30.0)  # index + aggregate
    assert row["total_cost_usd"] == pytest.approx(0.016 + 0.0002 + 0.0001)
    assert row["total_billed_s"] == pytest.approx(row["total_cost_usd"] / psu._USD_PER_LAMBDA_S)
    # Legacy rows (no cost split columns) degrade to the worker cost alone.
    legacy = df[df["event"] == "release"].drop(
        columns=["setup_cost_usd", "finalize_cost_usd", "phase_index_s"]
    )
    lrow = psu.point_release_history(legacy).iloc[0]
    assert lrow["total_cost_usd"] == pytest.approx(0.016)
    assert lrow["phase_agg_s"] == pytest.approx(30.0)  # aggregate alone (min_count=1)


def test_summary_and_point_diagnostics_render(tmp_path):
    pytest.importorskip("matplotlib")
    psu = pytest.importorskip("plot_summary")
    df = fas.records_to_frame(
        _release_records("c1", "v0.31.0", 0.016) + _release_records("c2", "v0.32.0", 0.018)
    )
    hist = psu.point_release_history(df)
    out = tmp_path / "full_aoi_summary.png"
    assert psu.make_summary_figure([("point", hist, "ref")], out) is True
    assert out.exists() and out.stat().st_size > 0
    # Empty rows are skipped; all-empty -> False, nothing written.
    import pandas as pd

    assert psu.make_summary_figure([("point", pd.DataFrame(), "ref")], tmp_path / "x.png") is False
    diag = tmp_path / "full_aoi_point_phases.png"
    assert (
        psu.make_diagnostics_figure(hist, psu.POINT_PHASE_PANELS, "ref", diag, "point phases")
        is True
    )
    assert diag.exists() and diag.stat().st_size > 0
    # The ylabel keyword lets the object-count figure relabel its non-seconds
    # axis (review): renders with a custom label just like the default.
    obj = tmp_path / "full_aoi_point_labelled.png"
    assert (
        psu.make_diagnostics_figure(
            hist, psu.POINT_PHASE_PANELS, "ref", obj, "point phases", ylabel="objects"
        )
        is True
    )
    assert obj.exists() and obj.stat().st_size > 0
    # The approved panel set: read / agg / write / setup / finalize -- never
    # the raw index+aggregate pair, and finalize kept per issue #252.
    cols = [c for c, _t in psu.POINT_PHASE_PANELS]
    assert cols == ["phase_read_s", "phase_agg_s", "phase_write_s", "setup_s", "finalize_s"]


def test_diagnostics_skips_when_no_panel_has_data(tmp_path):
    pytest.importorskip("matplotlib")
    psu = pytest.importorskip("plot_summary")
    import numpy as np

    df = fas.records_to_frame(_release_records("c1", "v0.31.0", 0.016))
    hist = psu.point_release_history(df)
    nulls = hist.assign(**{c: np.nan for c, _t in psu.POINT_PHASE_PANELS})
    out = tmp_path / "y.png"
    assert psu.make_diagnostics_figure(nulls, psu.POINT_PHASE_PANELS, "ref", out, "t") is False
    assert not out.exists()


def test_merge_history_selects_live_target_only():
    # The per-merge derivations key on the collapsed live hive target; retired
    # 2x2 rows and non-merge events stay out.
    psu = pytest.importorskip("plot_summary")
    import pandas as pd

    rows = [
        {
            "timestamp": "2026-07-16T00:00:00Z",
            "commit": "a1",
            "event": "merge",
            "target": psu.LIVE_MERGE_TARGET,
            "cost_per_shard_usd": 0.00443,
            "setup_s": 3.7,
            "finalize_s": 1.2,
            "phase_index_s": 14.0,
            "phase_aggregate_s": 7.8,
            "total_wall_s": 88.0,
            "max_memory_mb": 1650.0,
            "memory_gb": 4.0,
        },
        {
            "timestamp": "2026-07-15T00:00:00Z",
            "commit": "a0",
            "event": "merge",
            "target": "tdigest_healpix_o9_inline_nomask",  # retired arm
            "cost_per_shard_usd": 0.005,
        },
    ]
    hist = psu.merge_history(pd.DataFrame(rows))
    assert list(hist["target"]) == [psu.LIVE_MERGE_TARGET]
    row = hist.iloc[0]
    sync_usd = (3.7 + 1.2) * psu._USD_PER_LAMBDA_S
    assert row["total_cost_usd"] == pytest.approx(0.00443 + sync_usd)
    assert row["phase_agg_s"] == pytest.approx(21.8)


# --- store object-count columns (issue #240) --------------------------------


def test_objects_columns_retained_and_mismatch_dropped():
    # The two scalar object columns are retained in the release series; the
    # mismatch description is JSON-only (dropped by the reindex), mirroring the
    # per-merge series' JSON-only keys.
    df = fas.records_to_frame([_record(objects_mismatch="total objects 999 != expected 25")])
    assert list(df.columns) == fas.FULL_AOI_COLUMNS
    # Appended in order: object counts (phase 3), then layout + parity (phase
    # 4), then the worker phase split + setup cost (issue #250), then the
    # #256 write split + finalize cost.
    assert fas.FULL_AOI_COLUMNS[-10:] == [
        "objects_total",
        "objects_expected",
        "store_layout",
        "parity_ok",
        "phase_read_s",
        "phase_index_s",
        "phase_aggregate_s",
        "setup_cost_usd",
        "phase_write_s",
        "finalize_cost_usd",
    ]
    assert df.iloc[0]["objects_total"] == 27
    assert df.iloc[0]["objects_expected"] == 25
    assert "objects_mismatch" not in df.columns
    # A pre-#240 record (no objects keys) degrades to null cells, not a KeyError.
    legacy = _record()
    for k in ("objects_total", "objects_expected", "objects_mismatch"):
        del legacy[k]
    old = fas.records_to_frame([legacy])
    assert old.iloc[0]["objects_total"] is None or str(old.iloc[0]["objects_total"]) == "nan"


def test_full_aoi_objects_figure_skips_legacy_series(tmp_path):
    # A pre-#240 series parquet has no objects_total column (or an all-null
    # one): the objects figure must skip cleanly (False, no file), while the
    # cost figures still render -- no broken panel on old data.
    pytest.importorskip("matplotlib")
    ps = pytest.importorskip("plot_series")
    assert "full_aoi_objects" in ps.FULL_AOI_FIGURES
    df = fas.records_to_frame(_matrix_records("c1", "v0.24.0", 0.016))
    old = df.drop(columns=["objects_total", "objects_expected"])
    out = tmp_path / "objects.png"
    assert ps.make_full_aoi_release_figure(old, "objects_total", "objects", out) is False
    assert not out.exists()
    # All-null column (post-append reindex of legacy rows) -> same skip.
    import numpy as np

    df_nulls = df.assign(objects_total=np.nan)
    assert ps.make_full_aoi_release_figure(df_nulls, "objects_total", "objects", out) is False
    assert not out.exists()


# --- hive layout axis + parity columns (issue #240 phase 4) -------------------


def test_store_layout_and_parity_columns_retained_parity_detail_dropped():
    rec = _record(
        target="full_aoi_neon_o9_hive",
        store_layout="hive",
        parity_ok=False,
        parity={"mismatches": [{"shard": "1121121", "array": "count"}]},
    )
    df = fas.records_to_frame([rec])
    assert list(df.columns) == fas.FULL_AOI_COLUMNS
    assert fas.FULL_AOI_COLUMNS[-8:-6] == ["store_layout", "parity_ok"]
    row = df.iloc[0]
    assert row["store_layout"] == "hive"
    assert row["parity_ok"] == False  # noqa: E712 -- nullable bool column
    # The nested parity detail is JSON-only (dropped from the flat series).
    assert "parity" not in df.columns
    flat = fas.flatten_record(rec)
    assert "parity" not in flat


def test_full_aoi_history_keeps_flat_rows_only(tmp_path):
    # The hive arm shares index_backend with a flat target: slotting it into
    # the fixed 2x2 would silently overwrite that panel cell, so the renderer
    # keys the panels on flat rows only (hive stays in the parquet).
    ps = pytest.importorskip("plot_series")
    hive_rec = _record(
        target="full_aoi_neon_o9_hive",
        store_layout="hive",
        index_backend="inline",
        cost_usd=0.99,
    )
    df = fas.records_to_frame(_matrix_records("c1", "v0.24.0", 0.016) + [hive_rec])
    hist = ps._full_aoi_history(df)
    assert "full_aoi_neon_o9_hive" not in set(hist["target"])
    assert len(hist) == 4
    # Legacy rows (null store_layout, pre-#240-phase-4 parquet) read as flat.
    import numpy as np

    legacy = df[df["store_layout"] != "hive"].assign(store_layout=np.nan)
    assert len(ps._full_aoi_history(legacy)) == 4
