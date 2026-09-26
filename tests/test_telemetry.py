"""Tests for the per-shard stats schema + merge fold (issue #297 phase 1)."""

import pathlib

import pytest

from zagg.telemetry import (
    SCHEMA_VERSION,
    SPEC_V3,
    billed_seconds,
    build_record,
    failure_record,
    flatten_record,
    granule_ids_key,
    granule_ids_path,
    granules_sha256,
    merge,
    read_granule_ids,
    read_sidecar,
    refusal_manifest_key,
    run_parquet_key,
    sidecar_key,
    sidecar_path,
    write_granule_ids,
    write_refusal_manifest,
    write_run_parquet,
    write_sidecar,
)


def _record(
    shard_key=101,
    *,
    n_obs=1000,
    cells=7,
    duration=12.5,
    duration_total=None,
    granules=("s3://b/g1.h5", "s3://b/g2.h5"),
    phases=None,
    memory=512.0,
    lambda_config=None,
    invoked_by=None,
    run_id=None,
    error=None,
):
    metadata = {
        "shard_key": shard_key,
        "total_obs": n_obs,
        "cells_with_data": cells,
        "granule_count": len(granules),
        "duration_s": duration,
        # Invocation wall (issue #589); absent on records predating the key.
        **({"duration_total_s": duration_total} if duration_total is not None else {}),
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
        run_id=run_id,
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
        assert rec["semantic_hash"] is None  # placeholder until issue #299
        assert rec["run_id"] is None  # local flavor: no dispatcher-stamped id
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
        # No invocation wall on the record (a worker predating issue #589):
        # the price falls back to the aggregate clock, and the column is
        # null — unmeasured, never zero.
        assert rec["duration_total_s"] is None
        assert rec["gb_seconds"] == pytest.approx(40.0)
        assert rec["est_cost_usd"] == pytest.approx(40.0 * 0.0000133334)

    def test_duration_total_prices_cost_when_present(self):
        # Issue #589: the write side (leaf write, hash, column fold, refs)
        # runs AFTER ``duration_s`` is stamped, so the invocation wall is what
        # Lambda bills; ``duration_s`` keeps its read/aggregate meaning.
        cfg = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        rec = _record(duration=10.0, duration_total=13.0, lambda_config=cfg)
        assert rec["duration_s"] == 10.0 and rec["duration_total_s"] == 13.0
        assert rec["gb_seconds"] == pytest.approx(52.0)
        assert rec["est_cost_usd"] == pytest.approx(52.0 * 0.0000133334)
        # Off-Lambda the total is recorded but unpriced, like duration_s.
        local = _record(duration=10.0, duration_total=13.0)
        assert local["duration_total_s"] == 13.0 and local["gb_seconds"] is None

    def test_billed_seconds_prefers_the_invocation_wall(self):
        # Issue #589: the dispatcher's cost/timeout figures bill the same
        # clock build_record prices with -- the total, else duration_s.
        assert billed_seconds({"duration_s": 10.0, "duration_total_s": 13.0}) == 13.0
        assert billed_seconds({"duration_s": 10.0}) == 10.0
        assert billed_seconds({"duration_s": 10.0, "duration_total_s": 0.0}) == 0.0
        assert billed_seconds({}) == 0 and billed_seconds(None) == 0

    def test_unknown_arch_falls_back_to_default_rate(self):
        # The record prices via the #298 arch table; an unmapped arch uses the
        # flat default rate instead of raising in a worker.
        cfg = {"memory_mb": 1024, "arch": "riscv64", "function_variant": "f"}
        rec = _record(duration=10.0, lambda_config=cfg)
        assert rec["est_cost_usd"] == pytest.approx(10.0 * 0.0000133334)

    def test_content_hashes_ride_metadata(self):
        # O11 (issue #342): the hive writer stamps metadata["content_hashes"];
        # the record carries it verbatim, None wherever no writer recorded it
        # (flat layouts, raster, failures) — absence = unverifiable (§5.3).
        assert _record()["content_hashes"] is None
        hashes = {"arrays": {"6/count": "aa"}, "combined": "bb"}
        rec = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": hashes})
        assert rec["content_hashes"] == hashes

    def test_invoked_by_copied_verbatim(self):
        ident = {"arn": "arn:aws:sts::123:assumed-role/x/y", "userid": "AROA:me"}
        assert _record(invoked_by=ident)["invoked_by"] == ident

    def test_run_id_copied_verbatim(self):
        # Threaded like invoked_by (issue #297): dispatcher-known, stamped
        # through the invoke payload, copied verbatim into the record.
        assert _record(run_id="deadbeef")["run_id"] == "deadbeef"

    def test_n_obs_read_absent_reads_as_unmeasured(self):
        # Issue #374: no ``total_obs_read`` in the worker metadata — a raster
        # record, or one from a worker predating the field — reads as None
        # (unmeasured), NOT as zero, so a rollup can't dilute the ratio with
        # rows nobody counted.
        assert _record()["n_obs_read"] is None

    def test_n_obs_read_rides_worker_metadata(self):
        rec = build_record(
            shard_key=1,
            metadata={"duration_s": 1.0, "total_obs": 400, "total_obs_read": 1000},
        )
        assert rec["n_obs"] == 400
        assert rec["n_obs_read"] == 1000
        # The ratio is derived at read time, never stored (the #297 convention).
        assert "read_keep_ratio" not in rec

    def test_n_obs_read_zero_is_distinct_from_absent(self):
        rec = build_record(
            shard_key=1, metadata={"duration_s": 1.0, "total_obs": 0, "total_obs_read": 0}
        )
        assert rec["n_obs_read"] == 0  # measured zero, not None

    def test_raster_counters_default_none_and_populate(self):
        # Off-raster records carry the read-volume fields as None (issue #297).
        rec = _record()
        assert rec["raster_bytes_read"] is None
        assert rec["raster_px_decoded"] is None
        assert rec["raster_px_sampled"] is None
        rec = build_record(
            shard_key=1,
            metadata={
                "duration_s": 1.0,
                "raster_bytes_read": 2048,
                "raster_px_decoded": 4096,
                "raster_px_sampled": 100,
            },
        )
        assert rec["raster_bytes_read"] == 2048
        assert rec["raster_px_decoded"] == 4096
        assert rec["raster_px_sampled"] == 100

    def test_non_numeric_phase_entries_dropped(self):
        rec = _record(phases={"sample": 3.0, "write": 1.0, "stages": {"open": 2}})
        assert rec["phase_timings"] == {"sample": 3.0, "write": 1.0}

    def test_spill_bytes_split_out_of_timings(self):
        # The spill instrumentation (issue #217) stamps byte counts alongside
        # the ``*_s`` seconds in ``phase_timings``; the record keeps timings
        # seconds-only and surfaces the volume on its own top-level field.
        # The issue #370 fold-regime marker splits out the same way (a count,
        # not seconds).
        rec = _record(
            phases={
                "read": 8.0,
                "aggregate": 2.0,
                "spill_write_s": 0.5,
                "spill_read_s": 0.25,
                "spill_bytes": 4096.0,
                "spill_blocks_closed": 3,
            }
        )
        assert rec["phase_timings"] == {
            "read": 8.0,
            "aggregate": 2.0,
            "spill_write_s": 0.5,
            "spill_read_s": 0.25,
        }
        assert rec["spill_bytes"] == pytest.approx(4096.0)
        assert rec["spill_blocks_closed"] == 3

    def test_spill_blocks_closed_absent_reads_none(self):
        # 0/absent = exact regime; a record without the marker carries None.
        assert _record()["spill_blocks_closed"] is None

    def test_spill_blocks_closed_merges_by_sum_and_flattens(self):
        # Rollups sum the fold counts (like spill_bytes); the run parquet gets
        # a real column so folded leaves are queryable after the fact.
        a = _record(phases={"read": 1.0, "spill_blocks_closed": 2})
        b = _record(phases={"read": 1.0, "spill_blocks_closed": 3})
        assert merge([a, b])["spill_blocks_closed"] == 5
        row = flatten_record(a)
        assert row["spill_blocks_closed"] == 2
        assert "phase_spill_blocks_closed" not in row

    def test_granules_sha256_order_independent(self):
        assert granules_sha256(["b", "a"]) == granules_sha256(["a", "b"])
        assert granules_sha256([]) is None
        assert granules_sha256(None) is None

    def test_paired_entries_identify_by_primary_url(self):
        # Paired-asset worker payloads (issue #425) may reach build_record
        # verbatim (the Lambda handler passes event granule_urls); the record's
        # id space and catalog hash must match the resolved-strings local path.
        from zagg.telemetry import build_record

        entries = [
            "s3://b/g1.h5",
            {"url": "s3://b/g2.h5", "assets": {"l2a": "s3://b/g2b.h5"}},
        ]
        rec = build_record(shard_key=1, metadata={"total_obs": 1}, granule_ids=entries)
        assert rec["granules_sha256"] == granules_sha256(["s3://b/g1.h5", "s3://b/g2.h5"])
        assert rec["n_granules"] == 2


class TestCanonicalGranuleIdentity:
    """The D19 hash epoch's canonical granule identity (espg-ruled 2026-08-17,
    PR #420 question (1)(b)): *the granule triggers the hash, not how that
    granule is fetched.* One physical granule is named three ways across the
    paths that record it — a resolved ``s3://`` href, an ``https://`` href, and
    the bare catalog id — and all three must record and hash identically.
    """

    #: One granule set, three spellings. The s3/https pair is what
    #: ``runner._resolve_urls`` picks between on ``data_source.driver``; the
    #: bare form is the catalog's own ``rec["id"]``, which for every CMR/STAC
    #: catalog zagg reads IS the basename of both hrefs.
    S3 = [
        "s3://nsidc-prot/ATLAS/ATL06/007/2024/01/08/ATL06_20240108040423_03102212_007_01.h5",
        "s3://nsidc-prot/ATLAS/ATL06/007/2024/01/06/ATL06_20240105235929_02772210_007_01.h5",
    ]
    HTTPS = [
        "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-prot/ATLAS/ATL06/007/2024/01/08/"
        "ATL06_20240108040423_03102212_007_01.h5",
        "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-prot/ATLAS/ATL06/007/2024/01/06/"
        "ATL06_20240105235929_02772210_007_01.h5",
    ]
    BARE = [
        "ATL06_20240108040423_03102212_007_01.h5",
        "ATL06_20240105235929_02772210_007_01.h5",
    ]

    def test_the_three_spellings_hash_identically(self):
        # The ruling in one line. Pre-epoch these were three catalog
        # identities for one granule set, so a driver switch -- packaging in
        # the D19 core -- made every recorded hash un-reproducible.
        assert granules_sha256(self.S3) == granules_sha256(self.HTTPS)
        assert granules_sha256(self.S3) == granules_sha256(self.BARE)
        # ...and the s3 driver's own stripped form (``bucket/key``, what
        # ``processing._make_url_rewriter`` hands h5coro) is the same granule.
        stripped = [u.replace("s3://", "", 1) for u in self.S3]
        assert granules_sha256(stripped) == granules_sha256(self.BARE)

    def test_the_three_spellings_record_identical_ids(self):
        # BOTH halves of the ruling: the recorded id LIST canonicalizes too,
        # not only the digest over it -- otherwise the `missing` diff a
        # contraction names would still be driver-dependent.
        from zagg.telemetry import canonical_granule_ids

        assert canonical_granule_ids(self.S3) == self.BARE
        assert canonical_granule_ids(self.HTTPS) == self.BARE
        assert canonical_granule_ids(self.BARE) == self.BARE  # idempotent
        assert canonical_granule_ids(None) is None  # unknown stays unknown

    def test_the_sibling_object_records_the_canonical_ids(self, tmp_path):
        # End to end through the production writer: the object a leaf carries
        # is byte-identical whichever driver wrote it.
        def _written(ids):
            leaf = str(tmp_path / ids[0][:4] / "1" / "-42.zarr")
            assert write_granule_ids(leaf, ids) is True
            return read_granule_ids(leaf)

        s3, https = _written(self.S3), _written(self.HTTPS)
        assert s3["granule_ids"] == https["granule_ids"] == sorted(self.BARE)
        assert s3["granules_sha256"] == https["granules_sha256"] == granules_sha256(self.BARE)

    def test_paired_asset_entries_identify_by_their_primary(self, tmp_path):
        # Paired entries (issue #425) identify by their PRIMARY ALONE, which is
        # what keeps the local backend (resolved strings) and the Lambda handler
        # (payload entries verbatim) on one hash. The siblings are dropped
        # outright, so this uses two ENTIRELY DIFFERENT sibling granules on the
        # two sides: the digests must still agree, which is the property, and is
        # also the discriminating form -- the same basename on both sides would
        # pass whether assets are normalized, ignored, or replaced.
        from zagg.telemetry import build_record

        s3_entries = [
            {"url": self.S3[0], "assets": {"l2a": "s3://b/prefix/SIB_A.h5"}},
            self.S3[1],
        ]
        https_entries = [
            {"url": self.HTTPS[0], "assets": {"l2b": "https://h/other/GEDI02_B_WHOLLY_OTHER.h5"}},
            self.HTTPS[1],
        ]
        recs = [
            build_record(shard_key=1, metadata={"total_obs": 1}, granule_ids=e)
            for e in (s3_entries, https_entries)
        ]
        assert recs[0]["granules_sha256"] == recs[1]["granules_sha256"]
        assert recs[0]["granules_sha256"] == granules_sha256(self.BARE)
        assert recs[0]["n_granules"] == recs[1]["n_granules"] == 2

    def test_the_raster_id_space_is_untouched(self):
        # Raster units identify by STAC item id or ISO datetime
        # (``raster_granule_ids``) -- neither carries a path separator, so the
        # canonical form is the identity itself and no raster digest moves.
        from zagg.telemetry import canonical_granule_ids, raster_granule_ids

        ids = raster_granule_ids(
            [{"id": "S2A_MSIL2A_20250101T160000"}, {"datetime": "2019-06-15T10:00:00Z"}]
        )
        assert canonical_granule_ids(ids) == ids

    def test_a_trailing_slash_never_yields_an_empty_identity(self):
        from zagg.telemetry import canonical_granule_id

        assert canonical_granule_id("s3://b/prefix/") == "prefix"
        assert canonical_granule_id("g.h5") == "g.h5"

    @pytest.mark.parametrize("entry", [{"assets": {}}, {"url": None}, {"url": ""}, None, ""])
    def test_a_malformed_entry_raises_rather_than_minting_an_identity(self, entry):
        # Keeps the pre-epoch loudness (`build_record`'s `g["url"]` raised
        # KeyError on a url-less mapping): every such entry would otherwise
        # stringify to the SAME id, so N of them collapse onto one recorded id
        # and silently shrink the recorded set -- the shape that hides a
        # contraction. An identity is never silently wrong.
        from zagg.telemetry import canonical_granule_id, canonical_granule_ids, granules_sha256

        with pytest.raises(ValueError, match="canonical identity"):
            canonical_granule_id(entry)
        # ...and it raises through the list/digest helpers rather than being
        # swallowed on the way to a sidecar.
        with pytest.raises(ValueError, match="canonical identity"):
            canonical_granule_ids([self.S3[0], entry])
        with pytest.raises(ValueError, match="canonical identity"):
            granules_sha256([self.S3[0], entry])


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

    def test_n_obs_read_sums_over_the_fold(self):
        # Issue #374: mergeable by construction, like the raster counters —
        # the up-tree rollup sums reads and keeps, and the ratio is taken
        # after the fold rather than averaged across leaves.
        a = build_record(
            shard_key=1, metadata={"duration_s": 1.0, "total_obs": 40, "total_obs_read": 100}
        )
        b = build_record(
            shard_key=2, metadata={"duration_s": 1.0, "total_obs": 60, "total_obs_read": 300}
        )
        m = merge([a, b])
        assert m["n_obs"] == 100
        assert m["n_obs_read"] == 400

    def test_n_obs_read_back_compat_row_folds(self):
        # A record predating the field (None) must not poison the fold: the
        # _SUM_OR_NONE disposition skips it, so a mixed rollup reports the
        # measured part rather than collapsing to None.
        measured = build_record(
            shard_key=1, metadata={"duration_s": 1.0, "total_obs": 40, "total_obs_read": 100}
        )
        legacy = build_record(shard_key=2, metadata={"duration_s": 1.0, "total_obs": 60})
        assert legacy["n_obs_read"] is None
        assert merge([measured, legacy])["n_obs_read"] == 100
        # All-unmeasured stays unmeasured.
        assert merge([legacy, legacy])["n_obs_read"] is None

    def test_content_hashes_merge_equal_or_none(self):
        # Per-leaf identity (issue #342): shared value survives a fold,
        # differing leaves collapse to None (the absorbing identity).
        hashes = {"arrays": {"6/count": "aa"}, "combined": "bb"}
        a = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": hashes})
        b = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": hashes})
        assert merge([a, b])["content_hashes"] == hashes
        other = {"arrays": {"6/count": "cc"}, "combined": "dd"}
        c = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": other})
        assert merge([a, c])["content_hashes"] is None

    def test_merge_does_not_alias_nested_content_hashes(self):
        # The rollup's defensive copy has to reach the nested ``arrays`` map
        # too: equal by value, never the leaf record's object (issue #342).
        hashes = {"arrays": {"6/count": "aa"}, "combined": "bb"}
        a = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": hashes})
        b = build_record(shard_key=1, metadata={"duration_s": 1.0, "content_hashes": hashes})
        merged = merge([a, b])
        assert merged["content_hashes"] == a["content_hashes"]
        assert merged["content_hashes"] is not a["content_hashes"]
        assert merged["content_hashes"]["arrays"] is not a["content_hashes"]["arrays"]

    def test_cost_fields_fold(self):
        price = 0.0000133334
        cfg_a = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        cfg_b = {"memory_mb": 2048, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        a = _record(1, duration=10.0, lambda_config=cfg_a)  # 40 gb-s
        b = _record(2, duration=5.0, lambda_config=cfg_b)  # 10 gb-s
        m = merge([a, b])
        # _SUM_OR_NONE fold of populated cost: parts add.
        assert m["gb_seconds"] == pytest.approx(50.0)
        assert m["est_cost_usd"] == pytest.approx(50.0 * price)
        # Differing lambda blocks collapse to None (absorbing identity).
        assert m["lambda"] is None

    def test_duration_total_sums_or_none(self):
        # Populated totals sum; a rollup over records that never measured the
        # invocation wall stays None (the merge identity law), and a mixed
        # fold counts an older part at its duration_s (build_record's
        # pricing fallback), so the rollup total never reads below duration_s.
        a = _record(1, duration=10.0, duration_total=13.0)
        b = _record(2, duration=5.0, duration_total=6.5)
        assert merge([a, b])["duration_total_s"] == pytest.approx(19.5)
        assert merge([a, b])["duration_s"] == pytest.approx(15.0)
        assert merge([_record(1), _record(2)])["duration_total_s"] is None
        mixed = merge([a, _record(2, duration=5.0)])
        assert mixed["duration_total_s"] == pytest.approx(13.0 + 5.0)
        assert mixed["duration_total_s"] >= mixed["duration_s"]
        assert merge([a])["duration_total_s"] == 13.0

    def test_cost_fields_shared_lambda_survives(self):
        cfg = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        a = _record(1, duration=10.0, lambda_config=cfg)
        b = _record(2, duration=5.0, lambda_config=cfg)
        m = merge([a, b])
        assert m["lambda"] == cfg
        # ... but as a defensive copy, not an alias of either input's dict.
        assert m["lambda"] is not a["lambda"]
        assert m["lambda"] is not b["lambda"]
        assert m["gb_seconds"] == pytest.approx(60.0)  # (10+5) * 4096/1024

    def test_cost_fields_mixed_populated_and_none(self):
        price = 0.0000133334
        cfg = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        metered = _record(1, duration=10.0, lambda_config=cfg)  # 40 gb-s
        unmetered = _record(2, duration=5.0, lambda_config=None)  # gb_seconds None
        m = merge([metered, unmetered])
        # Only the populated record contributes to the sum.
        assert m["gb_seconds"] == pytest.approx(40.0)
        assert m["est_cost_usd"] == pytest.approx(40.0 * price)
        # One populated + one None lambda block -> mismatch -> None.
        assert m["lambda"] is None

    def test_identity_fields_collapse_to_none_on_mismatch(self):
        a = _record(1)
        b = _record(2, granules=("s3://b/g3.h5",))
        m = merge([a, b])
        assert m["shard_key"] is None  # differing shards -> no single identity
        assert m["granules_sha256"] is None
        assert m["zagg_version"] == a["zagg_version"]  # shared value survives

    def test_run_id_merges_equal_or_none(self):
        # Same-run records keep the id; a cross-run fold collapses to None
        # (absorbing), like every other identity field (issue #297).
        a, b = _record(1, run_id="run1"), _record(2, run_id="run1")
        assert merge([a, b])["run_id"] == "run1"
        assert merge([a, _record(3, run_id="run2")])["run_id"] is None
        assert merge([a, _record(4)])["run_id"] is None

    def test_success_ands(self):
        assert merge([_record(1), _record(2, error="boom")])["success"] is False

    def test_raster_counters_fold(self):
        # Read-volume counters sum; a mixed raster/non-raster fold sums only
        # the populated part (None-aware, like the cost fields).
        a = build_record(
            shard_key=1,
            metadata={"raster_bytes_read": 100, "raster_px_decoded": 10, "raster_px_sampled": 2},
        )
        b = build_record(
            shard_key=2,
            metadata={"raster_bytes_read": 900, "raster_px_decoded": 30, "raster_px_sampled": 6},
        )
        m = merge([a, b])
        assert m["raster_bytes_read"] == 1000
        assert m["raster_px_decoded"] == 40
        assert m["raster_px_sampled"] == 8
        mixed = merge([a, _record(3)])
        assert mixed["raster_bytes_read"] == 100  # None drops out of the sum
        assert merge([_record(1), _record(2)])["raster_bytes_read"] is None

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

    def test_v3_spec_keys_off_leaf_stem(self):
        # D23 window-only naming (morton-hive/3): sidecar = leaf stem +
        # .stats.json, derived from the leaf basename so the schedule-none
        # token has one source (windows.leaf_name_v3). Legacy specs above are
        # frozen — every current writer emits them unchanged.
        from zagg.windows import SCHEDULE_NONE_TOKEN, leaf_name_v3

        assert sidecar_key(leaf_name_v3("2019"), SPEC_V3) == "2019.stats.json"
        assert leaf_name_v3(None) == f"{SCHEDULE_NONE_TOKEN}.zarr"
        assert sidecar_key(leaf_name_v3(None), SPEC_V3) == f"{SCHEDULE_NONE_TOKEN}.stats.json"
        # The /1 and /2 specs (and an absent spec) keep the legacy names.
        assert sidecar_key("-4211322.zarr", "morton-hive/2") == "stats.json"
        assert sidecar_key("-4211322_2019.zarr", "morton-hive/1") == "stats_2019.json"
        with pytest.raises(ValueError, match="not a leaf zarr name"):
            sidecar_key("2019", SPEC_V3)
        with pytest.raises(ValueError, match="not a leaf zarr name"):
            sidecar_key(".zarr", SPEC_V3)

    def test_unknown_spec_raises(self):
        # An unrecognized spec must not silently fall back to the legacy names:
        # a future morton-hive/4 or a typo would key the wrong sidecar and read
        # as absent instead of failing. /1 and /2 (and absent) stay legacy.
        with pytest.raises(ValueError, match="unknown store spec"):
            sidecar_key("-4211322.zarr", "morton-hive/4")
        with pytest.raises(ValueError, match="unknown store spec"):
            sidecar_key("-4211322.zarr", "Morton-Hive/3")
        assert sidecar_key("-4211322.zarr", "morton-hive/1") == "stats.json"
        assert sidecar_key("-4211322_2019.zarr", "morton-hive/2") == "stats_2019.json"

    def test_v3_malformed_stem_raises(self):
        # The v3 branch must be as strict as legacy: a stem that fails the label
        # grammar (embedded ``/`` path escape, forbidden ``_``) raises rather
        # than yielding a malformed sidecar key.
        with pytest.raises(ValueError, match="grammar"):
            sidecar_key("a/b.zarr", SPEC_V3)
        with pytest.raises(ValueError, match="grammar"):
            sidecar_key("foo_bar.zarr", SPEC_V3)
        # The schedule-none token still satisfies the explicit grammar.
        from zagg.windows import SCHEDULE_NONE_TOKEN

        assert (
            sidecar_key(f"{SCHEDULE_NONE_TOKEN}.zarr", SPEC_V3)
            == f"{SCHEDULE_NONE_TOKEN}.stats.json"
        )

    def test_v3_write_read_roundtrip(self, tmp_path):
        from zagg.windows import leaf_name_v3

        leaf = str(tmp_path / "-4" / "2" / leaf_name_v3("2019"))
        rec = _record()
        write_sidecar(leaf, rec, spec=SPEC_V3)
        assert read_sidecar(leaf, spec=SPEC_V3) == rec
        assert (tmp_path / "-4" / "2" / "2019.stats.json").exists()
        # The legacy reader does not see it (different name — spec selects).
        assert read_sidecar(leaf) is None

    def test_read_absent_returns_none(self, tmp_path):
        (tmp_path / "-4").mkdir()
        assert read_sidecar(str(tmp_path / "-4" / "-4.zarr")) is None


class TestRefusalManifest:
    """The store-root refusal record (issue #388, ruled (9)(c)+(a))."""

    def _refused(self, shard, missing, *, window=None, identity="contraction"):
        return {
            "shard_key": shard,
            "window": window,
            "identity": identity,
            "refused": True,
            "missing_granules": missing,
        }

    def test_key_shape_and_traversal_guard(self):
        key = refusal_manifest_key("abc123", timestamp="20260718T010203Z")
        assert key == "refusals_20260718T010203Z_abc123.json"
        # Outside the run-record glob: a refusal object must never read as a
        # shard run record to the sweep's discovery scan.
        assert not key.startswith("stats_") and not key.endswith(".parquet")
        for run_id, ts in (("../../etc/passwd", "20260718T010203Z"), ("abc123", "../evil")):
            with pytest.raises(ValueError):
                refusal_manifest_key(run_id, timestamp=ts)

    def test_writes_the_full_diff_per_unit(self, tmp_path):
        import json

        root = str(tmp_path / "store")
        path = write_refusal_manifest(
            root,
            [
                self._refused(7, ["s3://b/g9.h5", "s3://b/g8.h5"]),
                self._refused(3, ["s3://b/g1.h5"], window="2019", identity="mixed"),
            ],
            run_id="cafe01",
            timestamp="20260718T010203Z",
            semantic_hash="a" * 64,
        )
        assert path.endswith("/refusals_20260718T010203Z_cafe01.json")
        body = json.loads(pathlib.Path(path).read_bytes())
        assert body["spec"] == "zagg-refusals/1"
        assert body["run_id"] == "cafe01" and body["semantic_hash"] == "a" * 64
        assert body["cells_refused"] == 2 and isinstance(body["zagg_version"], str)
        # Sorted by (shard, window) so two runs over one refusal set compare.
        assert [(u["shard_key"], u["window"]) for u in body["units"]] == [(3, "2019"), (7, None)]
        mixed, contraction = body["units"]
        assert mixed["identity"] == "mixed" and mixed["n_missing"] == 1
        # The FULL list, not the log's first five — that is the whole point.
        assert contraction["missing_granules"] == ["s3://b/g9.h5", "s3://b/g8.h5"]
        assert contraction["n_missing"] == 2

    def test_nothing_refused_writes_nothing(self, tmp_path):
        # Ruled (9)(a): a pure-skip run stays row-less AND object-less.
        root = tmp_path / "store"
        assert write_refusal_manifest(str(root), [], run_id="cafe01") is None
        assert not root.exists()

    def test_write_is_fail_open(self, tmp_path, monkeypatch):
        import obstore

        monkeypatch.setattr(obstore, "put", lambda *a, **k: (_ for _ in ()).throw(RuntimeError()))
        got = write_refusal_manifest(
            str(tmp_path / "store"), [self._refused(1, ["s3://b/g1.h5"])], run_id="cafe01"
        )
        assert got is None

    def test_composition_is_fail_open_too(self, tmp_path):
        # It runs BEFORE the summary and the run-stats parquet, so a raise
        # while building the units would destroy the run record of a run that
        # already did all its work. A malformed missing_granules (the shape
        # the isinstance check one line up already anticipates) and the
        # unbounded-size raiser must both cost only the manifest.
        root = str(tmp_path / "store")
        assert write_refusal_manifest(root, [self._refused(1, 7)], run_id="cafe01") is None

        class Boom(list):
            def __iter__(self):
                raise MemoryError("units too big")

        big = self._refused(1, Boom(["s3://b/g1.h5"]))
        assert write_refusal_manifest(root, [big], run_id="cafe01") is None


class TestGranuleIdsSibling:
    """The recorded id list is its own sibling (issue #388, ruled (6)(c)):
    same spec-keyed grammar as the sidecar, self-pairing, fail-open."""

    IDS = ["s3://b/g2.h5", "s3://b/g1.h5"]

    def test_key_shares_the_sidecar_grammar(self):
        from zagg.windows import leaf_name_v3

        assert granule_ids_key("-4211322.zarr") == "granules.json"
        assert granule_ids_key("-4211322_20260713.zarr") == "granules_20260713.json"
        assert granule_ids_key(leaf_name_v3("2019"), SPEC_V3) == "2019.granules.json"
        # And the same strictness: an unknown spec cannot silently fall back.
        with pytest.raises(ValueError, match="unknown store spec"):
            granule_ids_key("-4211322.zarr", "morton-hive/4")

    def test_path_is_a_sibling_beside_the_sidecar(self):
        leaf = "/root/-4/2/1/1/3/2/2/-4211322.zarr"
        assert granule_ids_path(leaf) == "/root/-4/2/1/1/3/2/2/granules.json"

    def test_write_read_roundtrip_sorted_and_self_hashing(self, tmp_path):
        leaf = str(tmp_path / "-4" / "2" / "-42.zarr")
        assert write_granule_ids(leaf, self.IDS) is True
        got = read_granule_ids(leaf)
        # Recorded in the CANONICAL id space (espg-ruled 2026-08-17): the
        # driver-stripped bare granule id, sorted.
        assert got["granule_ids"] == ["g1.h5", "g2.h5"]
        # Its OWN version marker, not the D20 record's schema_version: a
        # record rev must not bump this object, nor this object the record.
        assert got["spec"] == "zagg-granule-ids/1"
        assert "schema_version" not in got
        # Self-pairing: the object carries the hash of the list it holds, so a
        # reader can reject a sibling that does not belong to the sidecar.
        assert got["granules_sha256"] == granules_sha256(self.IDS)
        assert (tmp_path / "-4" / "2" / "granules.json").exists()

    def test_empty_id_set_records_an_empty_list(self, tmp_path):
        leaf = str(tmp_path / "-4" / "2" / "-42.zarr")
        write_granule_ids(leaf, [])
        got = read_granule_ids(leaf)
        assert got["granule_ids"] == [] and got["granules_sha256"] is None

    def test_write_is_fail_open(self, tmp_path, monkeypatch):
        # Called from the seam with the leaf already COMMITTED: a failed PUT
        # must count as "no recorded set" on a later run, never fail the unit.
        import obstore

        def boom(*a, **k):
            raise RuntimeError("nope")

        monkeypatch.setattr(obstore, "put", boom)
        leaf = str(tmp_path / "-4" / "2" / "-42.zarr")
        assert write_granule_ids(leaf, self.IDS) is False

    def test_read_absent_returns_none(self, tmp_path):
        (tmp_path / "-4").mkdir()
        assert read_granule_ids(str(tmp_path / "-4" / "-4.zarr")) is None


class TestRunParquet:
    """Run-level parquet rows (issue #297 phase 3): flattened records plus
    dispatcher-built failure rows, round-trippable by fastparquet and pyarrow."""

    def test_flatten_record_columns(self):
        cfg = {"memory_mb": 4096, "arch": "aarch64", "function_variant": "zagg-process-shard"}
        ident = {"arn": "arn:aws:iam::1:user/x", "userid": "AIDA1"}
        row = flatten_record(
            _record(7, lambda_config=cfg, invoked_by=ident, run_id="cafe01"), retries=2
        )
        assert row["shard_key"] == 7 and row["success"] is True
        assert row["run_id"] == "cafe01" and row["semantic_hash"] is None
        assert row["retries"] == 2 and row["error_class"] is None
        assert row["phase_read"] == 8.0 and row["phase_aggregate"] == 2.0
        assert row["lambda_memory_mb"] == 4096
        assert row["lambda_function_variant"] == "zagg-process-shard"
        assert row["invoked_by"] == ident["arn"]
        assert row["invoked_by_userid"] == ident["userid"]
        assert "raster_bytes_read" in row  # read-volume columns always present
        # The point-path read counter is a column of every run parquet too, so
        # the read-vs-keep ratio is queryable alongside n_obs (issue #374).
        assert "n_obs_read" in row
        # The invocation wall is a column beside duration_s (issue #589): null
        # here (no total on the record), populated when the worker stamped it.
        assert row["duration_total_s"] is None
        assert flatten_record(_record(duration_total=13.0))["duration_total_s"] == 13.0
        assert "phase_timings" not in row and "lambda" not in row  # flattened away

    def test_n_obs_read_flattens_to_a_column(self):
        rec = build_record(
            shard_key=3, metadata={"duration_s": 1.0, "total_obs": 40, "total_obs_read": 100}
        )
        row = flatten_record(rec)
        assert row["n_obs"] == 40 and row["n_obs_read"] == 100
        # Unmeasured stays null in the column rather than defaulting to 0.
        assert flatten_record(_record())["n_obs_read"] is None

    def test_failure_record_and_error_class(self):
        rec = failure_record(
            shard_key=9, error="Lambda timeout: Task timed out", duration_s=901.0, run_id="ab12"
        )
        assert rec["success"] is False and rec["duration_s"] == 901.0
        assert rec["run_id"] == "ab12"  # dispatcher stamps failure rows too
        row = flatten_record(rec, retries=3)
        assert row["error_class"] == "Lambda timeout"  # derived from the string
        row = flatten_record(rec, error_class="TimeoutError")
        assert row["error_class"] == "TimeoutError"  # explicit wins
        assert failure_record(shard_key=None, error="boom")["shard_key"] is None

    def test_run_parquet_key_shape(self):
        # Timestamp-first (D20) so a lexicographic listing is chronological.
        key = run_parquet_key("abc123", timestamp="20260718T010203Z")
        assert key == "stats_20260718T010203Z_abc123.parquet"

    def test_run_parquet_key_rejects_traversal(self):
        # Both components land in the object key; a ``/`` or ``..`` must RAISE
        # (issue #313 makes timestamp caller-supplied) rather than escape root.
        import pytest

        with pytest.raises(ValueError):
            run_parquet_key("../../etc/passwd", timestamp="20260718T010203Z")
        with pytest.raises(ValueError):
            run_parquet_key("ab/cd", timestamp="20260718T010203Z")
        with pytest.raises(ValueError):
            run_parquet_key("abc123", timestamp="../evil")
        with pytest.raises(ValueError):
            run_parquet_key("abc123", timestamp="2026/07/18")

    def test_round_trip(self, tmp_path):
        import pandas as pd

        rows = [
            flatten_record(
                _record(
                    1,
                    lambda_config={"memory_mb": 4096, "arch": "aarch64", "function_variant": "f"},
                    invoked_by={"arn": "arn:x", "userid": "u"},
                ),
                retries=0,
            ),
            flatten_record(_record(2)),  # local flavor: all-None identity columns
            flatten_record(
                failure_record(shard_key=3, error="Lambda OOM: killed", duration_s=100.0),
                retries=2,
            ),
        ]
        root = str(tmp_path / "store")
        path = write_run_parquet(root, rows, run_id="deadbeef")
        assert path.startswith(root + "/stats_")
        assert path.endswith("_deadbeef.parquet")  # timestamp-first, run id last

        df = pd.read_parquet(path, engine="fastparquet")
        assert len(df) == 3
        assert set(df["shard_key"]) == {1, 2, 3}
        by_key = df.set_index("shard_key")
        assert bool(by_key.loc[3, "success"]) is False
        assert by_key.loc[3, "error_class"] == "Lambda OOM"
        assert by_key.loc[3, "retries"] == 2
        assert by_key.loc[1, "invoked_by"] == "arn:x"
        assert pd.isna(by_key.loc[2, "invoked_by"])
        assert by_key.loc[1, "gb_seconds"] > 0

        # pyarrow (the duckdb/Athena reader family) round-trips it too.
        pa_parquet = pytest.importorskip("pyarrow.parquet")
        table = pa_parquet.read_table(path)
        assert table.num_rows == 3
        assert "invoked_by" in table.column_names

    def test_leaf_column_round_trips_all_null_and_mixed(self, tmp_path):
        """Issue #383's deferred key is the first ``_ROW_SCALARS`` string that
        is all-null in the overwhelming majority of runs; ``object_encoding=
        "utf8"`` is what keeps fastparquet from choking on that column (the
        ``invoked_by`` precedent). Pin it rather than leave it accidental."""
        import pandas as pd

        def with_column(key, name=None):
            meta = {"duration_s": 1.0}
            if name is not None:
                meta["leaf_column"] = name
            return flatten_record(build_record(shard_key=key, metadata=meta))

        # All-null: the ordinary run, where no unit wrote a column.
        allnull = write_run_parquet(
            str(tmp_path / "none"), [with_column(1), with_column(2)], run_id="bb01"
        )
        df = pd.read_parquet(allnull, engine="fastparquet")
        assert "leaf_column" in df.columns and df["leaf_column"].isna().all()

        # Mixed: one column-bearing leaf among plain ones, the shape D22
        # discovery actually queries.
        mixed = write_run_parquet(
            str(tmp_path / "mixed"),
            [with_column(1), with_column(2, "all.pyramid.zarr"), with_column(3)],
            run_id="bb02",
        )
        df = pd.read_parquet(mixed, engine="fastparquet").set_index("shard_key")
        assert df.loc[2, "leaf_column"] == "all.pyramid.zarr"
        assert pd.isna(df.loc[1, "leaf_column"]) and pd.isna(df.loc[3, "leaf_column"])
        # pyarrow (the duckdb/Athena reader family) sees it as a string column.
        pa_parquet = pytest.importorskip("pyarrow.parquet")
        table = pa_parquet.read_table(mixed)
        assert "leaf_column" in table.column_names

    def test_empty_rows_raise(self, tmp_path):
        with pytest.raises(ValueError, match="at least one"):
            write_run_parquet(str(tmp_path), [], run_id="x")

    def test_finalize_error_is_a_run_level_column(self, tmp_path):
        """Issue #335: the guarded finalize's failure is DURABLE here — one
        run-level column, constant down the rows, always present (None on a
        clean run) so the column set does not vary run to run."""
        import pandas as pd

        rows = [flatten_record(_record(1)), flatten_record(_record(2))]
        clean = write_run_parquet(str(tmp_path / "ok"), rows, run_id="aa01")
        df = pd.read_parquet(clean, engine="fastparquet")
        assert "finalize_error" in df.columns and df["finalize_error"].isna().all()

        failed = write_run_parquet(
            str(tmp_path / "bad"),
            rows,
            run_id="aa02",
            finalize_error="RuntimeError: invoke failed",
        )
        df = pd.read_parquet(failed, engine="fastparquet")
        # Every row carries it (the run-level broadcast), so any row answers
        # "did finalize fail?" without a second object.
        assert list(df["finalize_error"]) == ["RuntimeError: invoke failed"] * 2


class TestWindowColumn:
    """The per-shard record's ``window`` label (issue #300): threaded like
    run_id, merged equal-or-None, flattened to a run-parquet column."""

    def _windowed(self, window, **kwargs):
        return build_record(
            shard_key=5,
            metadata={"total_obs": 1, "duration_s": 1.0},
            window=window,
            **kwargs,
        )

    def test_recorded_and_defaults_none(self):
        assert self._windowed("2019")["window"] == "2019"
        assert _record(5)["window"] is None

    def test_merge_equal_keeps_mismatch_collapses(self):
        a, b = self._windowed("2019"), self._windowed("2019")
        assert merge([a, b])["window"] == "2019"
        assert merge([a, self._windowed("2020")])["window"] is None  # cross-window rollup

    def test_flattened_to_row(self):
        row = flatten_record(self._windowed("2019"))
        assert row["window"] == "2019"


class TestRunParquetShardKeyExactness:
    """Packed morton words round-trip the run parquet exactly (issue #300):
    the sweep's run-record discovery recomputes leaf paths from these keys, so
    float64 inference (from failure-row nulls) would corrupt them silently."""

    def test_uint64_with_null_round_trips(self, tmp_path):
        import pandas as pd

        word = 10376293541461622786  # morton_word("-311"): > 2^63, float64-inexact
        rows = [
            flatten_record(_record(word)),
            flatten_record(failure_record(shard_key=None, error="boom")),
        ]
        path = write_run_parquet(str(tmp_path / "store"), rows, run_id="ff01")
        df = pd.read_parquet(path, engine="fastparquet")
        assert str(df["shard_key"].dtype) == "UInt64"
        assert int(df["shard_key"].dropna().iloc[0]) == word  # exact, not 2^53-rounded
        assert df["shard_key"].isna().sum() == 1  # the failure row keeps its null


class TestRecordedIdentity:
    """Issue #388 record keys: the catalog hash (the id LIST is a sibling
    object, not a record key), the #383 column basename, and the metadata
    semantic-hash fallback."""

    def test_the_id_list_is_never_a_record_key(self):
        # Ruled (question (6)(c)): the record carries the HASH only, so the
        # per-shard identity GETs and the response envelope stay small.
        rec = build_record(
            shard_key=1,
            metadata={"duration_s": 1.0},
            granule_ids=["s3://b/g2.h5", "s3://b/g1.h5", "s3://b/g1.h5"],
        )
        assert "granule_ids" not in rec
        # Duplicates are kept: the hash is over the sorted multiset.
        assert rec["granules_sha256"] == granules_sha256(
            ["s3://b/g1.h5", "s3://b/g1.h5", "s3://b/g2.h5"]
        )
        assert rec["n_granules"] == 3

    def test_granules_sha256_absent_reads_none(self):
        rec = build_record(shard_key=1, metadata={"duration_s": 1.0})
        assert rec["granules_sha256"] is None

    def test_leaf_column_rides_metadata(self):
        # The #383 deferred run-record key: the worker stamps
        # metadata["leaf_column"] iff the unit wrote a column artifact.
        assert _record()["leaf_column"] is None
        rec = build_record(
            shard_key=1, metadata={"duration_s": 1.0, "leaf_column": "all.pyramid.zarr"}
        )
        assert rec["leaf_column"] == "all.pyramid.zarr"

    def test_semantic_hash_falls_back_to_metadata(self):
        # Issue #388: the shared seams stamp metadata["semantic_hash"], so a
        # caller that never resolved the hash (the Lambda handler) still
        # records the identity half a skip-if-current comparison needs.
        rec = build_record(shard_key=1, metadata={"duration_s": 1.0, "semantic_hash": "f" * 64})
        assert rec["semantic_hash"] == "f" * 64

    def test_junk_metadata_semantic_hash_is_rejected(self):
        # ``metadata`` is the worker's returned BODY on the dispatcher's
        # stale-worker path (runner._lambda_result_rows), so the fallback is
        # validated against the D19 digest shape: a version-skewed worker
        # cannot plant an identity that a later skip check would trust.
        for junk in ["not-a-hash", "F" * 64, "a" * 63, "a" * 65, "", 12345, {"x": 1}, None]:
            rec = build_record(shard_key=1, metadata={"duration_s": 1.0, "semantic_hash": junk})
            assert rec["semantic_hash"] is None, junk

    def test_explicit_semantic_hash_wins_over_metadata(self):
        rec = build_record(
            shard_key=1,
            metadata={"duration_s": 1.0, "semantic_hash": "f" * 64},
            semantic_hash="a" * 64,
        )
        assert rec["semantic_hash"] == "a" * 64

    def test_merge_folds_identity_keys_equal_or_none(self):
        a = _record()
        b = _record()
        b["timestamp"] = a["timestamp"]
        folded = merge([a, b])
        assert folded["granules_sha256"] == a["granules_sha256"]
        assert folded["leaf_column"] is None  # absent in both -> stays None
        mismatched = _record(granules=("s3://b/other.h5",))
        assert merge([a, mismatched])["granules_sha256"] is None

    def test_merge_folds_leaf_column_equal_or_none(self):
        # The reading that matters for a windowed node: two windows of one
        # shard, each having written a column, roll up. Equal survives the
        # fold; a mismatch collapses, like every other identity field.
        def with_column(name):
            return build_record(
                shard_key=1, metadata={"duration_s": 1.0, "leaf_column": name}, window="2019"
            )

        same = [with_column("all.pyramid.zarr"), with_column("all.pyramid.zarr")]
        assert merge(same)["leaf_column"] == "all.pyramid.zarr"
        mixed = [with_column("all.pyramid.zarr"), with_column("h_li.pyramid.zarr")]
        assert merge(mixed)["leaf_column"] is None
        # None is absorbing on this key too (a column-writing unit rolled up
        # with one that wrote none reads as "no common column").
        assert merge([with_column("all.pyramid.zarr"), _record()])["leaf_column"] is None

    def test_merge_single_record_does_not_alias_nested_dicts(self):
        a = _record()
        a["content_hashes"] = {"arrays": {"h_li": "ab"}, "combined": "cd"}
        folded = merge([a])
        folded["content_hashes"]["arrays"]["h_li"] = "mutated"
        assert a["content_hashes"]["arrays"]["h_li"] == "ab"

    def test_flatten_carries_leaf_column_not_the_id_list(self):
        rec = build_record(
            shard_key=1, metadata={"duration_s": 1.0, "leaf_column": "all.pyramid.zarr"}
        )
        row = flatten_record(rec)
        assert row["leaf_column"] == "all.pyramid.zarr"
        assert "granule_ids" not in row
