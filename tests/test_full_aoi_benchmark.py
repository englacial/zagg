"""Tests for the full-AOI benchmark harness (issue #202).

No AWS: covers the target manifest's internal consistency and the harness's pure
metric/throughput helpers. The live dispatch path (``run_target`` with
``dry_run=False``) needs credentials + the local catalog, so it is exercised
operationally, not in unit tests.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(SCRIPTS))

import run_full_aoi_benchmark as rfab  # noqa: E402

_VALID_BACKENDS = {"inline", "sidecar", "hierarchical"}


# --- target manifest consistency ------------------------------------------


def test_full_aoi_targets_manifest_consistent():
    manifest, base = rfab.load_targets(str(BENCH / "targets_full_aoi_neon.json"))
    assert manifest["aoi"]["file"] and (base / manifest["aoi"]["file"]).exists()
    assert manifest["temporal"]["start"] and manifest["temporal"]["end"]
    dispatch = manifest["dispatch"]
    # No account is pinned by default (fork-friendly); expect_account is an
    # optional opt-in guard the harness no-ops when absent (issue #202).
    assert dispatch["function_name"] and dispatch["region"]
    assert "expect_account" not in dispatch
    assert manifest["targets"], "manifest defines no targets"
    for name, t in manifest["targets"].items():
        assert (base / t["config"]).exists(), f"{name}: missing config {t['config']}"
        assert t["index_backend"] in _VALID_BACKENDS, name
        assert isinstance(t["aoi_mask"], bool), name
        for key in ("aggregator", "grid_type", "grid_size"):
            assert t.get(key), f"{name}: missing {key}"


def test_neon_catalog_committed_and_prefilters_nonempty():
    # The pinned NEON catalog (cat_neon.parquet) is the committed granule set the
    # release harness builds its shardmap from -- offline, no CMR (the cat_88s
    # precedent, issue #148). Pinning it makes the per-release full-AOI series
    # measure CODE change, not CMR data drift, over the whole AOI.
    from zagg.catalog.sources import Catalog

    cat_path = BENCH / "catalogs" / "cat_neon.parquet"
    assert cat_path.exists(), "pinned NEON catalog missing"
    cat = Catalog.from_geoparquet(str(cat_path))
    assert len(cat) > 0
    manifest, base = rfab.load_targets(str(BENCH / "targets_full_aoi_neon.json"))
    _parts, bbox = rfab._aoi_parts((base / manifest["aoi"]["file"]).resolve())
    sub = rfab._prefilter(cat, bbox, manifest["temporal"]["start"], manifest["temporal"]["end"])
    # The committed catalog is already the AOI-bbox+temporal subset, so the
    # harness's own prefilter is (near) idempotent and still non-empty.
    assert len(sub) > 0


@pytest.mark.slow
def test_neon_catalog_builds_full_aoi_shardmap(tmp_path):
    # End-to-end offline (no AWS): the release harness builds the whole-AOI
    # shardmap from the pinned catalog via --dry-run. Slow (mortie shardmap build,
    # ~seconds); gated behind the slow marker. One target is enough to prove the
    # catalog -> shardmap build path -- the manifest-consistency test above already
    # covers all four target definitions.
    metrics = tmp_path / "m.json"
    shards = tmp_path / "s.json"
    rc = rfab.main(
        [
            "--targets",
            str(BENCH / "targets_full_aoi_neon.json"),
            "--target",
            "full_aoi_neon_o9_inline_nomask",
            "--catalog",
            str(BENCH / "catalogs" / "cat_neon.parquet"),
            "--dry-run",
            "--event",
            "release",
            "--commit",
            "test",
            "--ref",
            "v0.0.0",
            "--out-json",
            str(metrics),
            "--out-shards-json",
            str(shards),
            "--artifacts-dir",
            str(tmp_path / "art"),
        ]
    )
    assert rc == 0
    runs = json.loads(metrics.read_text())
    assert len(runs) == 1
    run = runs[0]
    assert run["n_shards"] == 4  # the NEON AOI fans to 4 o9 shards
    assert run["apriori_estimate"]["est_cost_usd"] > 0
    # Dry-run writes no store -> object-count columns stay null (issue #240).
    assert run["objects_total"] is None and run["objects_mismatch"] is None


# --- pure helpers ----------------------------------------------------------


def test_apriori_estimate_scales_with_granules():
    est = rfab._apriori_estimate([10, 20], sec_per_granule=1.7)
    # (1.7*10 + 5) + (1.7*20 + 5) = 22 + 39 = 61 lambda-seconds
    assert est["est_lambda_seconds"] == pytest.approx(61.0)
    assert est["est_gb_seconds"] == pytest.approx(61.0 * rfab.LAMBDA_MEMORY_GB)
    assert est["est_cost_usd"] > 0


def test_setup_cost_usd_math_and_none_safe():
    # The setup invoke's billed dollars (issue #250 item 3): wall x GB x $/GB-s,
    # its own column -- cost_usd (worker GB-seconds) is untouched.
    expected = 104.0 * rfab.LAMBDA_MEMORY_GB * rfab.LAMBDA_PRICE_PER_GB_SEC
    assert rfab._setup_cost_usd(104.0) == pytest.approx(expected, abs=1e-6)
    assert rfab._setup_cost_usd(None) is None


def test_write_throughput_counts_retries_and_slowdown():
    results = [
        {"retries": 0, "error": None, "timeout": False},
        {"retries": 2, "error": None, "timeout": False},
        {"retries": 1, "error": "500 SlowDown: reduce your request rate", "timeout": False},
        {"retries": 0, "error": "Task timed out", "timeout": True},
    ]
    wt = rfab._write_throughput(results)
    assert wt["invoke_retries_total"] == 3
    assert wt["invoke_throttle_shards"] == 2
    assert wt["s3_slowdown_shards"] == 1
    assert wt["cells_timeout"] == 1


def test_write_throughput_empty_is_zeroed():
    assert rfab._write_throughput([]) == {
        "invoke_retries_total": 0,
        "invoke_throttle_shards": 0,
        "s3_slowdown_shards": 0,
        "cells_timeout": 0,
    }


def test_sidecar_cache_probe_graceful():
    """The cache-state probe never raises; it degrades to 'unknown' (issue #202)."""
    from types import SimpleNamespace

    # no store -> unknown
    assert rfab._sidecar_cache_state(None, SimpleNamespace(granules=[])) == "unknown"
    # store but no granules -> unknown
    sm_empty = SimpleNamespace(granules=[[]])
    assert rfab._sidecar_cache_state("s3://b/prefix", sm_empty) == "unknown"
    # store + granule but S3 unreachable/no creds -> unknown or cold, never raises
    sm = SimpleNamespace(granules=[[{"id": "ATL03_x_007_01.h5"}]])
    assert rfab._sidecar_cache_state("s3://nonexistent-zagg-test-bucket-xyz/p", sm) in (
        "cold",
        "warm",
        "unknown",
    )


# --- store object-count metric (issue #240, record-only) --------------------


def test_measure_objects_recorded_passthrough(monkeypatch):
    # Happy path: the shared measurement payload passes straight through.
    payload = {
        "objects_total": 27,
        "objects_expected": 27,
        "objects_per_shard": {"1121121": 6},
        "objects_mismatch": None,
    }
    calls = {}

    def fake_measure(store, **kwargs):
        calls["args"] = (store, kwargs)
        return dict(payload)

    monkeypatch.setattr(rfab.bench_objects, "measure_objects", fake_measure)
    from zagg.config import load_config
    from zagg.grids import from_config

    cfg = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9.yaml"))
    grid = from_config(cfg)
    out = rfab._measure_objects_recorded(
        "t", cfg, grid, "s3://b/t.zarr", [1, 2], 2, region="us-west-2"
    )
    assert out == payload
    store, kwargs = calls["args"]
    assert store == "s3://b/t.zarr"
    assert kwargs["shard_keys"] == [1, 2]
    assert kwargs["n_shards"] == 2  # completed-with-data count, not dispatch count
    assert kwargs["store_layout"] == "flat"


def test_measure_objects_recorded_never_raises(monkeypatch):
    # RECORD-ONLY: a failed LIST must not sink the release run -- it degrades
    # to an objects_mismatch note on the record instead.
    def boom(store, **kwargs):
        raise RuntimeError("s3 unreachable")

    monkeypatch.setattr(rfab.bench_objects, "measure_objects", boom)
    from zagg.config import load_config
    from zagg.grids import from_config

    cfg = load_config(str(BENCH / "configs" / "atl03_tdigest_healpix_o9.yaml"))
    grid = from_config(cfg)
    out = rfab._measure_objects_recorded(
        "t", cfg, grid, "s3://b/t.zarr", [1], 1, region="us-west-2"
    )
    assert out["objects_mismatch"].startswith("object-count measurement failed")
    assert "s3 unreachable" in out["objects_mismatch"]
    assert "objects_total" not in out  # nothing measured, columns stay null


# --- flat<->hive output parity (issue #240 item 2, record-only) --------------


def _parity_stores(tmp_path, monkeypatch):
    """Flat + hive stores from the SAME chunk results through the production
    writers (the issue #236 parity contract), for exercising _flat_hive_parity."""
    import numpy as np
    import test_hive as th

    import zagg.processing as processing
    from zagg import hive
    from zagg.config import default_config
    from zagg.grids import from_config
    from zagg.processing import write_shard_to_zarr
    from zagg.store import open_store

    cfg = default_config("atl06")
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"]["chunk_inner"] = 8  # K = 16, sharded (issue #236)
    cfg.aggregation["variables"]["h"] = {
        "function": "np.sort",
        "source": "h_li",
        "kind": "ragged",
        "inner_shape": [1],
        "dtype": "float32",
        "fill_value": 0,
    }
    grid = from_config(cfg)
    shard = th._shard_word()
    fake = th._sharded_accumulate_fake(
        grid,
        th.TestProcessAndWriteHiveSharded._chunk_carrier,
        th.TestProcessAndWriteHiveSharded._meta,
        {0: {"h": ([np.array([2.5], dtype=np.float32)], [1])}},
        occupied=grid.children(shard)[:3],
    )
    monkeypatch.setattr(processing, "process_shard", fake)
    hive_root = str(tmp_path / "hive_store")
    meta = hive.process_and_write_hive(
        shard, ["s3://b/g1.h5"], grid, {}, hive_root, cfg, store_kwargs={}
    )
    assert meta["error"] is None

    chunk_results: list = []
    fake(grid, shard, [], chunk_results=chunk_results, write_chunk=None)
    flat_root = str(tmp_path / "flat_store")
    flat = open_store(flat_root)
    grid.emit_template(flat)
    write_shard_to_zarr(chunk_results, flat, grid=grid, shard_key=shard)
    return grid, shard, flat_root, hive_root


def test_flat_hive_parity_clean(tmp_path, monkeypatch):
    grid, shard, flat_root, hive_root = _parity_stores(tmp_path, monkeypatch)
    out = rfab._flat_hive_parity(flat_root, hive_root, grid, [shard])
    assert out["parity_ok"] is True
    assert out["mismatches"] == []
    assert out["shards_checked"] == 1
    # Every per-cell array compared (coords + data vars + the ragged field).
    n_cell_arrays = sum(
        1 for m in grid.spec().members.values() if tuple(m.dimension_names or ())[:1] == ("cells",)
    )
    assert out["arrays_checked"] == n_cell_arrays


def test_flat_hive_parity_flags_content_divergence(tmp_path, monkeypatch):
    # Tamper ONE cell in the flat store: the mismatch names the shard + array,
    # and the helper still never raises (record-only, espg ruling on PR 242).
    import zarr

    from zagg.store import open_store

    grid, shard, flat_root, hive_root = _parity_stores(tmp_path, monkeypatch)
    base = int(grid.block_index(shard)[0]) * grid.cells_per_shard
    arr = zarr.open_array(open_store(flat_root), path=f"{grid.group_path}/count", mode="r+")
    arr[base + 5] = 999_999
    out = rfab._flat_hive_parity(flat_root, hive_root, grid, [shard])
    assert out["parity_ok"] is False
    assert {"shard": grid.shard_label(shard), "array": "count"} in out["mismatches"]


def test_flat_hive_parity_missing_leaf_is_a_finding_not_a_crash(tmp_path, monkeypatch):
    grid, shard, flat_root, _hive_root = _parity_stores(tmp_path, monkeypatch)
    out = rfab._flat_hive_parity(flat_root, str(tmp_path / "empty_hive"), grid, [shard])
    assert out["parity_ok"] is False
    assert out["mismatches"] and "error" in out["mismatches"][0]
    assert out["mismatches"][0]["shard"] == grid.shard_label(shard)


def test_hive_target_manifest_wiring():
    # The hive arm (issue #240 phase 4): parity_with names an existing flat
    # sibling that runs FIRST (targets dispatch in manifest order), and the
    # hive config resolves to store_layout=hive with the same grid.
    from zagg.config import get_store_layout, load_config

    manifest, base = rfab.load_targets(str(BENCH / "targets_full_aoi_neon.json"))
    names = list(manifest["targets"])
    hive_t = manifest["targets"]["full_aoi_neon_o9_hive"]
    sibling = hive_t["parity_with"]
    assert sibling in manifest["targets"]
    assert names.index(sibling) < names.index("full_aoi_neon_o9_hive")
    cfg = load_config(str(base / hive_t["config"]))
    assert get_store_layout(cfg) == "hive"
    flat_cfg = load_config(str(base / manifest["targets"][sibling]["config"]))
    assert get_store_layout(flat_cfg) == "flat"
    # Same grid modulo layout: the parity comparison is only meaningful then.
    assert cfg.output["grid"] == {**flat_cfg.output["grid"]}


def test_ok_shard_keys_is_the_cells_with_data_predicate():
    # Errored AND granule-less shards write no hive leaf: parity must only
    # cover status-200/no-error shards (review, PR 242 phase 4).
    results = [
        {"shard_key": 1, "status_code": 200, "error": None},
        {"shard_key": 2, "status_code": 200, "error": "worker OOM"},
        {"shard_key": 3, "status_code": 500, "error": None},
        {"shard_key": None, "status_code": 200, "error": None},
        {"shard_key": 4, "status_code": 200, "error": None},
    ]
    assert rfab._ok_shard_keys(results) == [1, 4]
    assert rfab._ok_shard_keys([]) == []


def test_parity_recorded_gates_on_session_targets(monkeypatch, tmp_path):
    # --target subselection: the hive arm alone must NOT compare against a
    # stale flat store from a prior release -- unknown, with the reason kept.
    grid, shard, flat_root, hive_root = _parity_stores(tmp_path, monkeypatch)
    target = {"parity_with": "flat_sibling"}
    skipped = rfab._parity_recorded(
        "hive_arm",
        target,
        hive_root,
        grid,
        [shard],
        n_shards=1,
        session_targets={"hive_arm"},  # sibling NOT dispatched this session
        region="us-west-2",
    )
    assert skipped["parity_ok"] is None
    assert "flat_sibling" in skipped["skipped"]
    # No parity_with (the flat arms) -> None; no store (dry-ish) -> None.
    assert (
        rfab._parity_recorded(
            "flat", {}, hive_root, grid, [shard], n_shards=1, session_targets=None, region="r"
        )
        is None
    )


def test_parity_recorded_runs_over_ok_shards_and_counts_skips(monkeypatch, tmp_path):
    # With the sibling dispatched this session, parity runs over the ok-shard
    # set and records how many dispatched shards it did not cover.
    grid, shard, flat_root, hive_root = _parity_stores(tmp_path, monkeypatch)
    # The harness derives flat_store from the hive store's prefix, so lay the
    # flat store out as a sibling named <parity_with>.zarr.
    import shutil

    sibling_store = str(tmp_path / "flat_sibling.zarr")
    shutil.move(flat_root, sibling_store)
    hive_store = str(tmp_path / "hive_arm.zarr")
    shutil.move(hive_root, hive_store)
    out = rfab._parity_recorded(
        "hive_arm",
        {"parity_with": "flat_sibling"},
        hive_store,
        grid,
        [shard],
        n_shards=4,  # 3 dispatched shards errored/empty -> not covered
        session_targets={"flat_sibling", "hive_arm"},
        region="us-west-2",
    )
    assert out["parity_ok"] is True
    assert out["shards_checked"] == 1
    assert out["shards_skipped"] == 3
