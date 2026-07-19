"""Tests for the per-shard stats schema + merge fold (issue #297 phase 1)."""

import pytest

from zagg.telemetry import (
    SCHEMA_VERSION,
    build_record,
    granules_sha256,
    merge,
    read_sidecar,
    sidecar_key,
    sidecar_path,
    write_sidecar,
)


def _record(
    shard_key=101,
    *,
    n_obs=1000,
    cells=7,
    duration=12.5,
    granules=("s3://b/g1.h5", "s3://b/g2.h5"),
    phases=None,
    memory=512.0,
    lambda_config=None,
    invoked_by=None,
    error=None,
):
    metadata = {
        "shard_key": shard_key,
        "total_obs": n_obs,
        "cells_with_data": cells,
        "granule_count": len(granules),
        "duration_s": duration,
        "max_memory_mb": memory,
        "container_hwm_mb": memory + 100 if memory is not None else None,
        "phase_timings": {"read": 8.0, "index": 1.0, "aggregate": 2.0}
        if phases is None
        else phases,
        "error": error,
    }
    return build_record(
        shard_key=shard_key,
        metadata=metadata,
        granule_ids=list(granules),
        invoked_by=invoked_by,
        lambda_config=lambda_config,
    )


def _assert_records_close(a, b):
    assert set(a) == set(b)
    for key in a:
        va, vb = a[key], b[key]
        if isinstance(va, float):
            assert va == pytest.approx(vb), key
        elif key == "phase_timings":
            assert set(va) == set(vb)
            for phase in va:
                assert va[phase] == pytest.approx(vb[phase]), phase
        else:
            assert va == vb, key


class TestBuildRecord:
    def test_schema_fields(self):
        rec = _record()
        assert rec["schema_version"] == SCHEMA_VERSION
        assert rec["shard_key"] == 101
        assert rec["template_hash"] is None  # placeholder until issue #299
        assert rec["n_shards"] == 1
        assert rec["n_granules"] == 2
        assert rec["granules_sha256"] == granules_sha256(["s3://b/g1.h5", "s3://b/g2.h5"])
        assert rec["n_obs"] == 1000
        assert rec["cells_with_data"] == 7
        assert rec["duration_s"] == 12.5
        assert rec["phase_timings"] == {"read": 8.0, "index": 1.0, "aggregate": 2.0}
        assert rec["spill_bytes"] is None  # no spill instrumentation off-Lambda
        assert rec["max_memory_mb"] == 512.0
        assert rec["success"] is True
        assert rec["error"] is None
        assert rec["invoked_by"] is None
        assert rec["lambda"] is None
        assert rec["gb_seconds"] is None  # unmetered off-Lambda
        assert rec["est_cost_usd"] is None
        assert isinstance(rec["zagg_version"], str)
        assert rec["timestamp"].endswith("+00:00")

    def test_error_marks_failure(self):
        rec = _record(error="No data after filtering")
        assert rec["success"] is False
        assert rec["error"] == "No data after filtering"

    def test_lambda_config_prices_cost(self):
        cfg = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        rec = _record(duration=10.0, lambda_config=cfg)
        assert rec["lambda"] == cfg
        assert rec["gb_seconds"] == pytest.approx(40.0)
        assert rec["est_cost_usd"] == pytest.approx(40.0 * 0.0000133334)

    def test_invoked_by_copied_verbatim(self):
        ident = {"arn": "arn:aws:sts::123:assumed-role/x/y", "userid": "AROA:me"}
        assert _record(invoked_by=ident)["invoked_by"] == ident

    def test_non_numeric_phase_entries_dropped(self):
        rec = _record(phases={"sample": 3.0, "write": 1.0, "stages": {"open": 2}})
        assert rec["phase_timings"] == {"sample": 3.0, "write": 1.0}

    def test_spill_bytes_split_out_of_timings(self):
        # The spill instrumentation (issue #217) stamps byte counts alongside
        # the ``*_s`` seconds in ``phase_timings``; the record keeps timings
        # seconds-only and surfaces the volume on its own top-level field.
        rec = _record(
            phases={
                "read": 8.0,
                "aggregate": 2.0,
                "spill_write_s": 0.5,
                "spill_read_s": 0.25,
                "spill_bytes": 4096.0,
            }
        )
        assert rec["phase_timings"] == {
            "read": 8.0,
            "aggregate": 2.0,
            "spill_write_s": 0.5,
            "spill_read_s": 0.25,
        }
        assert rec["spill_bytes"] == pytest.approx(4096.0)

    def test_granules_sha256_order_independent(self):
        assert granules_sha256(["b", "a"]) == granules_sha256(["a", "b"])
        assert granules_sha256([]) is None
        assert granules_sha256(None) is None


class TestMerge:
    def test_single_record_is_identity(self):
        rec = _record()
        _assert_records_close(merge([rec]), rec)

    def test_sums_and_maxes(self):
        a = _record(1, n_obs=10, cells=2, duration=1.0, memory=100.0)
        b = _record(2, n_obs=20, cells=3, duration=2.0, memory=300.0)
        m = merge([a, b])
        assert m["n_shards"] == 2
        assert m["n_obs"] == 30
        assert m["cells_with_data"] == 5
        assert m["n_granules"] == 4
        assert m["duration_s"] == pytest.approx(3.0)
        assert m["max_memory_mb"] == 300.0
        assert m["container_hwm_mb"] == 400.0
        assert m["phase_timings"]["read"] == pytest.approx(16.0)
        assert m["timestamp"] == max(a["timestamp"], b["timestamp"])
        assert m["success"] is True

    def test_identity_fields_collapse_to_none_on_mismatch(self):
        a = _record(1)
        b = _record(2, granules=("s3://b/g3.h5",))
        m = merge([a, b])
        assert m["shard_key"] is None  # differing shards -> no single identity
        assert m["granules_sha256"] is None
        assert m["zagg_version"] == a["zagg_version"]  # shared value survives

    def test_success_ands(self):
        assert merge([_record(1), _record(2, error="boom")])["success"] is False

    def test_associative(self):
        a = _record(1, n_obs=10, duration=1.5, memory=100.0)
        b = _record(2, n_obs=20, duration=2.5, memory=None)
        c = _record(3, n_obs=30, duration=3.5, memory=900.0, error="x")
        _assert_records_close(merge([merge([a, b]), c]), merge([a, merge([b, c])]))

    def test_commutative(self):
        a = _record(1, phases={"read": 1.0})
        b = _record(2, phases={"read": 2.0, "write": 0.5})
        _assert_records_close(merge([a, b]), merge([b, a]))

    def test_merge_of_children_equals_direct(self):
        records = [_record(k, n_obs=k * 10, duration=float(k)) for k in range(1, 7)]
        direct = merge(records)
        grouped = merge([merge(records[:2]), merge(records[2:5]), merge(records[5:])])
        _assert_records_close(grouped, direct)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            merge([])

    def test_schema_version_mismatch_raises(self):
        good, bad = _record(1), _record(2)
        bad["schema_version"] = 2
        with pytest.raises(ValueError, match="schema_version"):
            merge([good, bad])


class TestSidecarIO:
    """The leaf sidecar is a SIBLING of the leaf ``.zarr`` (issue #297):
    ``stats.json`` bare, ``stats_{window}.json`` windowed (a node dir holds
    every window's leaf of its one shard)."""

    def test_sidecar_key_bare_and_windowed(self):
        assert sidecar_key("-4211322.zarr") == "stats.json"
        assert sidecar_key("-4211322_20260713.zarr") == "stats_20260713.json"

    def test_sidecar_path_is_sibling_not_inside_leaf(self):
        leaf = "/root/-4/2/1/1/3/2/2/-4211322.zarr"
        assert sidecar_path(leaf) == "/root/-4/2/1/1/3/2/2/stats.json"

    def test_write_read_roundtrip(self, tmp_path):
        leaf = str(tmp_path / "-4" / "2" / "-42.zarr")
        rec = _record()
        write_sidecar(leaf, rec)
        assert read_sidecar(leaf) == rec
        # Sibling object, never inside the leaf prefix.
        assert (tmp_path / "-4" / "2" / "stats.json").exists()
        assert not (tmp_path / "-4" / "2" / "-42.zarr").exists()

    def test_read_absent_returns_none(self, tmp_path):
        (tmp_path / "-4").mkdir()
        assert read_sidecar(str(tmp_path / "-4" / "-4.zarr")) is None
