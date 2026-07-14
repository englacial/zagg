"""Tests for the full-AOI benchmark harness (issue #202).

No AWS: covers the target manifest's internal consistency and the harness's pure
metric/throughput helpers. The live dispatch path (``run_target`` with
``dry_run=False``) needs credentials + the local catalog, so it is exercised
operationally, not in unit tests.
"""

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
    assert dispatch["expect_account"] and dispatch["function_name"] and dispatch["region"]
    assert manifest["targets"], "manifest defines no targets"
    for name, t in manifest["targets"].items():
        assert (base / t["config"]).exists(), f"{name}: missing config {t['config']}"
        assert t["index_backend"] in _VALID_BACKENDS, name
        assert isinstance(t["aoi_mask"], bool), name
        for key in ("aggregator", "grid_type", "grid_size"):
            assert t.get(key), f"{name}: missing {key}"


# --- pure helpers ----------------------------------------------------------


def test_apriori_estimate_scales_with_granules():
    est = rfab._apriori_estimate([10, 20], sec_per_granule=1.7)
    # (1.7*10 + 5) + (1.7*20 + 5) = 22 + 39 = 61 lambda-seconds
    assert est["est_lambda_seconds"] == pytest.approx(61.0)
    assert est["est_gb_seconds"] == pytest.approx(61.0 * rfab.LAMBDA_MEMORY_GB)
    assert est["est_cost_usd"] > 0


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
