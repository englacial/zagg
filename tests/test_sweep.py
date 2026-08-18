"""Unified rollup sweep (issue #300): engine + stats family (§8.3 obligations).

Covers the standing D22 test claims on local stores: rollup == direct (stats
fold), sweep idempotence (second pass over an unchanged tree writes nothing),
leaf re-runs making ancestors detectably stale, incremental accumulation
(window and sibling union), and nothing-load-bearing (deleting every rollup
leaves leaf reads intact).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import obstore
import pytest

from zagg import sweep as sweep_mod
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, shard_leaf_path
from zagg.store import open_object_store
from zagg.sweep import SWEEP_SPEC, get_family, run_sweep
from zagg.telemetry import build_record, merge, read_sidecar, write_sidecar

SHARD_ORDER = 2


def _write_manifest(root, shard_order=SHARD_ORDER):
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": shard_order + 2,
        "shard_order": shard_order,
        "split_schedule": [1] * shard_order,
        "pyramid": {"orders": [], "aggregation": {}},
        "generated_at": "2026-01-01T00:00:00+00:00",
    }
    obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())


def _record(decimal, *, n_obs=10, duration_s=0.5, timestamp=None):
    # Exact binary floats so fold order can never perturb equality asserts.
    rec = build_record(
        shard_key=morton_word(decimal),
        metadata={
            "total_obs": n_obs,
            "cells_with_data": 2,
            "duration_s": duration_s,
            "phase_timings": {"read": 0.25, "write": 0.5},
        },
        granule_ids=[f"g-{decimal}"],
    )
    if timestamp is not None:
        rec["timestamp"] = timestamp
    return rec


def _put_leaf(root, decimal, *, window=None, **kwargs):
    leaf = shard_leaf_path(str(root), morton_word(decimal), window=window)
    rec = _record(decimal, **kwargs)
    write_sidecar(leaf, rec)
    return rec


def _rollup(root, decimal, family="stats"):
    from zagg.sweep import _node_rel

    path = root / _node_rel(decimal) / f"{family}.rollup.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _leaf_refs(*decimals, window=None):
    return [(morton_word(d), window) for d in decimals]


class TestStatsRollupEqualsDirect:
    def test_interior_and_base_match_direct_fold(self, tmp_path):
        _write_manifest(tmp_path)
        recs = {d: _put_leaf(tmp_path, d) for d in ("-311", "-312", "-321")}
        summary = run_sweep(str(tmp_path), _leaf_refs("-311", "-312", "-321"))
        # Nodes: -311, -312, -321 (shard), -31, -32 (order 1), -3 (base).
        assert summary["families"]["stats"]["written"] == 6
        assert _rollup(tmp_path, "-31")["payload"] == merge([recs["-311"], recs["-312"]])
        direct = merge([recs["-311"], recs["-312"], recs["-321"]])
        base = _rollup(tmp_path, "-3")
        assert base["payload"] == direct
        assert base["generation"] == {
            "n_leaves": 3,
            "max_leaf_timestamp": direct["timestamp"],
        }
        assert base["spec"] == SWEEP_SPEC and base["order"] == 0 and base["node"] == "-3"

    def test_windowed_leaves_fold_at_the_shard_node(self, tmp_path):
        _write_manifest(tmp_path)
        a = _put_leaf(tmp_path, "-311", window="2019")
        b = _put_leaf(tmp_path, "-311", window="2020")
        run_sweep(str(tmp_path), [(morton_word("-311"), "2019"), (morton_word("-311"), "2020")])
        node = _rollup(tmp_path, "-311")
        assert node["payload"] == merge([a, b])
        assert node["windows"] == ["2019", "2020"]
        assert node["generation"]["n_leaves"] == 2


class TestIdempotenceAndStaleness:
    def test_second_pass_over_unchanged_tree_writes_nothing(self, tmp_path):
        _write_manifest(tmp_path)
        for d in ("-311", "-321"):
            _put_leaf(tmp_path, d)
        refs = _leaf_refs("-311", "-321")
        first = run_sweep(str(tmp_path), refs)["families"]["stats"]
        assert first["written"] == 5 and first["current"] == 0
        before = _rollup(tmp_path, "-3")
        second = run_sweep(str(tmp_path), refs)["families"]["stats"]
        assert second["written"] == 0
        assert second["current"] == 5
        assert _rollup(tmp_path, "-3") == before  # byte-stable, not just skipped

    def test_leaf_rerun_day_apart_bumps_generation_stamp(self, tmp_path):
        # Fast path: distinct timestamps make the generation stamp itself move,
        # so staleness is caught by the (n_leaves, max_leaf_timestamp) compare
        # alone. Timestamps are hand-injected here to force the day-apart gap.
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311", timestamp="2026-01-01T00:00:00+00:00")
        _put_leaf(tmp_path, "-321", timestamp="2026-01-02T00:00:00+00:00")
        run_sweep(str(tmp_path), _leaf_refs("-311", "-321"))
        stale_gen = _rollup(tmp_path, "-3")["generation"]
        # Re-run of one leaf: a rewritten sidecar always carries a LATER
        # timestamp than every earlier record, so ancestor stamps mismatch.
        _put_leaf(tmp_path, "-311", n_obs=99, timestamp="2026-01-03T00:00:00+00:00")
        result = run_sweep(str(tmp_path), _leaf_refs("-311"))["families"]["stats"]
        assert result["written"] == 3  # -311, -31, -3: exactly the stale chain
        fresh = _rollup(tmp_path, "-3")
        assert fresh["generation"] != stale_gen
        assert fresh["generation"]["max_leaf_timestamp"] == "2026-01-03T00:00:00+00:00"
        assert fresh["payload"]["n_obs"] == 99 + 10

    def test_leaf_rerun_same_second_rewrites_via_payload_compare(self, tmp_path, monkeypatch):
        # Backstop path: the production timestamp comes from build_record, which
        # stamps timespec="seconds" — so a real back-to-back re-run carries the
        # SAME (n_leaves, max_leaf_timestamp) and the generation stamp is blind
        # to it. Freeze the wall clock to force that same-second collision (no
        # hand-injected timestamp=; build_record still stamps the record), then
        # assert the payload-equality backstop rewrites the whole chain anyway.
        _write_manifest(tmp_path)
        frozen = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

        class _FrozenClock:
            @staticmethod
            def now(tz=None):
                return frozen

        monkeypatch.setattr("zagg.telemetry.datetime", _FrozenClock)
        _put_leaf(tmp_path, "-311")  # real build_record auto-stamp
        _put_leaf(tmp_path, "-321")
        run_sweep(str(tmp_path), _leaf_refs("-311", "-321"))
        stale = _rollup(tmp_path, "-3")
        # Same wall-clock second, different content: re-run -311 with new obs.
        _put_leaf(tmp_path, "-311", n_obs=99)
        result = run_sweep(str(tmp_path), _leaf_refs("-311"))["families"]["stats"]
        assert result["written"] == 3  # -311, -31, -3: the full stale chain
        fresh = _rollup(tmp_path, "-3")
        # The generation stamp is IDENTICAL (same second, unchanged leaf count),
        # so only the payload compare could have caught the change.
        assert fresh["generation"] == stale["generation"]
        assert fresh["payload"] != stale["payload"]
        assert fresh["payload"]["n_obs"] == 99 + 10

    def test_incremental_append_unions_windows_and_siblings(self, tmp_path):
        _write_manifest(tmp_path)
        a = _put_leaf(tmp_path, "-311", window="2019")
        run_sweep(str(tmp_path), [(morton_word("-311"), "2019")])
        # A later run touches ONLY a new window and a new sibling shard; the
        # sweep is given only those leaves (run-record discovery) yet the
        # rollups keep the earlier contributions.
        b = _put_leaf(tmp_path, "-311", window="2020")
        c = _put_leaf(tmp_path, "-312")
        run_sweep(str(tmp_path), [(morton_word("-311"), "2020"), (morton_word("-312"), None)])
        assert _rollup(tmp_path, "-311")["payload"] == merge([a, b])
        assert _rollup(tmp_path, "-31")["generation"]["n_leaves"] == 3
        assert _rollup(tmp_path, "-31")["payload"] == merge([merge([a, b]), c])

    def test_corrupt_rollup_is_rebuilt(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        run_sweep(str(tmp_path), _leaf_refs("-311"))
        from zagg.sweep import _node_rel

        victim = tmp_path / _node_rel("-31") / "stats.rollup.json"
        victim.write_text("{not json")
        result = run_sweep(str(tmp_path), _leaf_refs("-311"))["families"]["stats"]
        # The corrupt node is rewritten; its parent's stamp still matches.
        assert result["written"] == 1 and result["current"] == 2
        assert _rollup(tmp_path, "-31")["spec"] == SWEEP_SPEC


class TestNothingLoadBearing:
    def test_deleting_every_rollup_leaves_leaf_reads_green(self, tmp_path):
        _write_manifest(tmp_path)
        recs = {d: _put_leaf(tmp_path, d) for d in ("-311", "-312")}
        run_sweep(str(tmp_path), _leaf_refs("-311", "-312"))
        removed = [p for p in tmp_path.rglob("*.rollup.json")]
        assert removed
        for p in removed:
            p.unlink()
        # Leaf sidecars (the truth) read back untouched...
        for d, rec in recs.items():
            leaf = shard_leaf_path(str(tmp_path), morton_word(d))
            assert read_sidecar(leaf) == rec
        # ...and one sweep regenerates identical payloads from them.
        run_sweep(str(tmp_path), _leaf_refs("-311", "-312"))
        assert _rollup(tmp_path, "-3")["payload"] == merge(list(recs.values()))


class TestRecordAndTimings:
    """The sweep's own telemetry (issue #353): durations + store-root record."""

    def test_families_and_total_carry_durations(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        for result in summary["families"].values():
            assert result["duration_s"] >= 0.0
        # The pass total spans every family (and the pre-walk setup).
        assert summary["duration_s"] >= max(r["duration_s"] for r in summary["families"].values())

    def test_record_written_at_root(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        records = sorted(tmp_path.glob("sweep_stats_*.json"))
        assert [r.name for r in records] == [summary["record"]]
        payload = json.loads(records[0].read_text())
        assert payload["spec"] == SWEEP_SPEC
        assert payload["n_leaves"] == 1
        assert set(payload["families"]) == set(summary["families"])
        assert payload["duration_s"] >= 0.0
        # The record is the summary itself; its own key is set only after the PUT.
        assert "record" not in payload

    def test_record_false_writes_nothing(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"), record=False)
        assert "record" not in summary
        assert list(tmp_path.glob("sweep_stats_*.json")) == []

    def test_record_write_failure_is_fail_open(self, tmp_path, monkeypatch, caplog):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        real_put = obstore.put

        def flaky_put(store, key, *args, **kwargs):
            if str(key).startswith("sweep_stats_"):
                raise RuntimeError("boom")
            return real_put(store, key, *args, **kwargs)

        monkeypatch.setattr(obstore, "put", flaky_put)
        with caplog.at_level("WARNING"):
            summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        # The record is telemetry: its failure never touches the fold result.
        assert summary["record"] is None
        assert summary["families"]["stats"]["written"] > 0
        assert any("run record write failed" in m for m in caplog.messages)

    def test_record_name_is_outside_the_run_parquet_glob(self, tmp_path):
        # discover_leaves scans ``stats_.+\.parquet``; the sweep's own record
        # must never read as a shard run record. The CLI makes this a live
        # sequence -- main() discovers first and sweeps second, so every pass
        # discovers over the previous pass's record.
        import re

        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _run_record(tmp_path, [_row("-311")])
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        # The record now sits at the root the NEXT pass discovers from.
        assert discover_leaves(str(tmp_path)) == [(morton_word("-311"), None)]
        assert re.fullmatch(r"sweep_stats_[0-9]{8}T[0-9]{6}Z\.json", summary["record"])


class TestWorkSetEdges:
    def test_missing_sidecar_leaf_is_skipped_not_fatal(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        result = run_sweep(str(tmp_path), _leaf_refs("-311", "-312"))["families"]["stats"]
        assert result["empty"] == 1  # -312 contributed nothing
        assert _rollup(tmp_path, "-31")["generation"]["n_leaves"] == 1
        assert _rollup(tmp_path, "-312") is None

    def test_foreign_order_leaf_ref_is_skipped_with_warning(self, tmp_path, caplog):
        _write_manifest(tmp_path)
        summary = run_sweep(str(tmp_path), [(morton_word("-3111"), None)])
        assert summary["skipped_leaves"] == ["-3111"]
        assert summary["n_leaves"] == 0
        assert "mixed-order" in caplog.text

    def test_no_manifest_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not a hive store root"):
            run_sweep(str(tmp_path), [])

    def test_empty_work_set_writes_nothing(self, tmp_path):
        # A run that touched nothing (or a caller that filtered every leaf out)
        # on a manifest-bearing store: the frontier walk breaks immediately and
        # MocFamily.finish(tops=[]) reports no root MOC. No object is written.
        from zagg.hive import read_root_coverage

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _stamp_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), [])
        assert summary["n_leaves"] == 0
        for fam in ("stats", "moc"):
            counts = summary["families"][fam]
            assert counts["written"] == 0 and counts["current"] == 0
            assert counts["empty"] == 0 and counts["failed"] == 0
        assert summary["families"]["moc"]["root_moc_written"] is False
        assert not list(tmp_path.rglob("*.rollup.json"))
        assert read_root_coverage(str(tmp_path)) is None


def _stamp_leaf(root, decimal, *, window=None, time_range=None):
    """A minimal committed leaf: root zarr group + D4 commit stamp."""
    import zarr

    from zagg.hive import stamp_commit
    from zagg.store import open_store

    leaf = shard_leaf_path(str(root), morton_word(decimal), window=window)
    store = open_store(leaf)
    zarr.open_group(store, mode="w", zarr_format=3)
    stamp_commit(store, cells_with_data=1, granule_count=1, window=window, time_range=time_range)


def _payload_words(payload):
    from zagg.hive import root_coverage_words

    return sorted(int(w) for w in root_coverage_words(payload))


class TestMocRollup:
    def test_union_equals_direct_walk(self, tmp_path):
        _write_manifest(tmp_path)
        decimals = ("-311", "-312", "-321")
        for d in decimals:
            _stamp_leaf(tmp_path, d)
        result = run_sweep(str(tmp_path), _leaf_refs(*decimals), families=("moc",))
        moc = result["families"]["moc"]
        assert moc["written"] == 6 and moc["root_moc_written"] is True
        base = _rollup(tmp_path, "-3", family="moc")
        assert _payload_words(base["payload"]) == sorted(morton_word(d) for d in decimals)
        assert _payload_words(_rollup(tmp_path, "-31", family="moc")["payload"]) == sorted(
            morton_word(d) for d in ("-311", "-312")
        )
        # The refreshed root coverage.moc lists exactly the committed shards.
        from zagg.hive import read_root_coverage

        root_moc = read_root_coverage(str(tmp_path))
        assert root_moc["source"] == "sweep" and root_moc["order"] == SHARD_ORDER
        assert _payload_words(root_moc) == sorted(morton_word(d) for d in decimals)

    def test_unstamped_debris_is_invisible(self, tmp_path):
        import zarr

        from zagg.store import open_store

        _write_manifest(tmp_path)
        _stamp_leaf(tmp_path, "-311")
        # A torn worker's leaf: prefix exists, no commit stamp (D4).
        debris = shard_leaf_path(str(tmp_path), morton_word("-312"))
        zarr.open_group(open_store(debris), mode="w", zarr_format=3)
        result = run_sweep(str(tmp_path), _leaf_refs("-311", "-312"), families=("moc",))
        assert result["families"]["moc"]["empty"] == 1
        assert _payload_words(_rollup(tmp_path, "-31", family="moc")["payload"]) == [
            morton_word("-311")
        ]

    def test_windowed_stamps_union_time_range(self, tmp_path):
        _write_manifest(tmp_path)
        _stamp_leaf(
            tmp_path,
            "-311",
            window="2019",
            time_range=["2019-02-01T00:00:00+00:00", "2019-11-01T00:00:00+00:00"],
        )
        _stamp_leaf(
            tmp_path,
            "-311",
            window="2020",
            time_range=["2020-01-01T00:00:00+00:00", "2020-06-01T00:00:00+00:00"],
        )
        word = morton_word("-311")
        run_sweep(str(tmp_path), [(word, "2019"), (word, "2020")], families=("moc",))
        node = _rollup(tmp_path, "-311", family="moc")
        # Two stamped windows of one shard: one covered word, unioned extent.
        assert node["generation"]["n_leaves"] == 2
        assert _payload_words(node["payload"]) == [word]
        assert node["payload"]["time_range"] == [
            "2019-02-01T00:00:00+00:00",
            "2020-06-01T00:00:00+00:00",
        ]

    def test_second_pass_writes_nothing_including_root(self, tmp_path):
        _write_manifest(tmp_path)
        for d in ("-311", "-321"):
            _stamp_leaf(tmp_path, d)
        refs = _leaf_refs("-311", "-321")
        first = run_sweep(str(tmp_path), refs, families=("moc",))["families"]["moc"]
        assert first["written"] == 5 and first["root_moc_written"] is True
        second = run_sweep(str(tmp_path), refs, families=("moc",))["families"]["moc"]
        assert second["written"] == 0 and second["current"] == 5
        assert second["root_moc_written"] is False

    def test_root_union_keeps_untouched_bases(self, tmp_path):
        from zagg.hive import build_root_coverage, read_root_coverage, write_root_coverage

        _write_manifest(tmp_path)
        # A prior run's root MOC lists a shard under ANOTHER base that this
        # sweep never visits; the refresh must union, not replace (D9).
        other = morton_word("411")
        write_root_coverage(str(tmp_path), build_root_coverage([other], SHARD_ORDER))
        _stamp_leaf(tmp_path, "-311")
        run_sweep(str(tmp_path), _leaf_refs("-311"), families=("moc",))
        assert _payload_words(read_root_coverage(str(tmp_path))) == sorted(
            [other, morton_word("-311")]
        )

    def test_default_families_cover_all_implemented(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _stamp_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        assert set(summary["families"]) == {"stats", "moc", "submap", "overview"}
        assert _rollup(tmp_path, "-3", family="stats") is not None
        assert _rollup(tmp_path, "-3", family="moc") is not None
        # No leaf sub-map was written -> the submap family finds nothing and
        # stays empty without failing the pass.
        assert summary["families"]["submap"]["empty"] >= 1
        assert _rollup(tmp_path, "-3", family="submap") is None
        # The fixture manifest carries no pyramid overview declaration
        # (template-time, issue #201) -> the overview family no-ops.
        assert summary["families"]["overview"]["declared"] is False
        assert summary["families"]["overview"]["written"] == 0


SUBMAP_SIG = {
    "type": "healpix",
    "indexing_scheme": "nested",
    "parent_order": SHARD_ORDER,
    "child_order": SHARD_ORDER + 2,
    "layout": "flat",
}


def _entry(gid):
    return {"id": gid, "s3": f"s3://bucket/{gid}", "https": f"https://host/{gid}"}


def _emit_submap(root, decimal, gids, window=None):
    from zagg.sweep import write_leaf_submap

    write_leaf_submap(
        str(root),
        morton_word(decimal),
        [_entry(g) for g in gids],
        grid_signature=SUBMAP_SIG,
        # stale whole-catalog count: the leaf writer must rewrite it (review,
        # granules_assigned fold)
        metadata={"collection": "TEST_001", "granules_assigned": 22116},
        window=window,
    )


class TestSubmapNaming:
    def test_legacy_and_v3_names(self):
        from zagg.sweep import submap_key

        assert submap_key("-311.zarr") == "shardmap.json"
        assert submap_key("-311_2019.zarr") == "shardmap_2019.json"
        assert submap_key("2019.zarr", spec="morton-hive/3") == "2019.shardmap.json"

    def test_unknown_spec_raises(self):
        from zagg.sweep import submap_key

        with pytest.raises(ValueError, match="unknown store spec"):
            submap_key("-311.zarr", spec="morton-hive/9")


class TestSubmapRollup:
    def test_leaf_submap_is_loadable_shardmap_json(self, tmp_path):
        from zagg.catalog.shardmap import ShardMap
        from zagg.sweep import _node_rel

        _write_manifest(tmp_path)
        _emit_submap(tmp_path, "-311", ["gA", "gB"])
        path = tmp_path / _node_rel("-311") / "shardmap.json"
        sm = ShardMap.from_json(str(path))
        assert sm.shard_keys == [morton_word("-311")]
        assert [g["id"] for g in sm.granules[0]] == ["gA", "gB"]
        assert sm.metadata["total_shards"] == 1 and sm.metadata["total_granules"] == 2
        assert sm.metadata["granules_assigned"] == 2  # rewritten, not the stale 22116
        assert json.loads(path.read_text())["written_at"]  # staleness stamp rides the file

    def test_leaf_submap_strips_the_run_wide_pairless_report(self, tmp_path):
        # ``pairless`` (issue #425) is one entry per unpaired granule in the
        # RUN's catalog — unbounded in its size, and meaningless per leaf — so
        # it is stripped like the other run-wide fields; the small
        # ``sibling_asset`` identity stays (review finding, PR #432).
        from zagg.catalog.shardmap import ShardMap
        from zagg.sweep import _node_rel, write_leaf_submap

        _write_manifest(tmp_path)
        write_leaf_submap(
            str(tmp_path),
            morton_word("-311"),
            [_entry("gA")],
            grid_signature=SUBMAP_SIG,
            metadata={
                "collection": "TEST_001",
                "sibling_asset": "l2a",
                "pairless": [{"id": f"g{i}", "missing": "l2a"} for i in range(500)],
                "build_wall_s": 12.3,
            },
        )
        sm = ShardMap.from_json(str(tmp_path / _node_rel("-311") / "shardmap.json"))
        assert "pairless" not in sm.metadata
        assert "build_wall_s" not in sm.metadata
        assert sm.metadata["sibling_asset"] == "l2a"

    def test_reproject_shim_matches_real_grid_signature(self):
        # Pin the coarsen-target shim to the real grid it stands in for: at the
        # coarsened order N, _ReprojectTarget(SUBMAP_SIG, N).spatial_signature()
        # must equal a genuine HealpixGrid's — otherwise the rollup oracle below
        # would only prove the fold agrees with itself. The signatures match on
        # every key except ``layout``: SUBMAP_SIG carries the store-layout token
        # ("flat"), while a live grid reports its DGGS layout ("fullsphere");
        # reproject only swaps the dispatch order, never the DGGS resolution.
        from zagg.grids.healpix import HealpixGrid
        from zagg.sweep import _ReprojectTarget

        for order in (0, 1, SHARD_ORDER):
            shim = _ReprojectTarget(SUBMAP_SIG, order).spatial_signature()
            real = HealpixGrid(order, SUBMAP_SIG["child_order"]).spatial_signature()
            assert {k: v for k, v in shim.items() if k != "layout"} == {
                k: v for k, v in real.items() if k != "layout"
            }

    def test_rollup_equals_direct_reproject(self, tmp_path):
        from zagg.catalog.shardmap import ShardMap
        from zagg.sweep import _ReprojectTarget

        _write_manifest(tmp_path)
        per_shard = {"-311": ["gA", "gB"], "-312": ["gB"], "-321": ["gC"]}
        for dec, gids in per_shard.items():
            _emit_submap(tmp_path, dec, gids)
        result = run_sweep(str(tmp_path), _leaf_refs(*per_shard), families=("submap",))
        assert result["families"]["submap"]["written"] == 6
        # -31 folds two shards; a granule shared across them (gB) counts once
        # (the #294 dedup rule), and the rollup grid signature carries the
        # node's order.
        r31 = _rollup(tmp_path, "-31", family="submap")["payload"]
        assert r31["shard_keys"] == [morton_word("-31")]
        assert sorted(e["id"] for e in r31["granules"][0]) == ["gA", "gB"]
        assert r31["grid_signature"]["parent_order"] == 1
        assert r31["metadata"]["reproject"]["method"] == "coarsen"
        # Rollup == direct (§8.3): the base rollup equals the run-level
        # ShardMap reprojected straight to order 0 by the production coarsen.
        direct = ShardMap(
            dict(SUBMAP_SIG),
            [morton_word(d) for d in sorted(per_shard)],
            [[_entry(g) for g in per_shard[d]] for d in sorted(per_shard)],
            {"collection": "TEST_001"},
        ).reproject(_ReprojectTarget(SUBMAP_SIG, 0))
        r3 = _rollup(tmp_path, "-3", family="submap")["payload"]
        assert r3["shard_keys"] == [int(k) for k in direct.shard_keys]
        assert {e["id"] for e in r3["granules"][0]} == {e["id"] for e in direct.granules[0]}

    def test_window_union_dedups_by_id(self, tmp_path):
        _write_manifest(tmp_path)
        _emit_submap(tmp_path, "-311", ["gA", "gB"], window="2019")
        _emit_submap(tmp_path, "-311", ["gB", "gC"], window="2020")
        word = morton_word("-311")
        run_sweep(str(tmp_path), [(word, "2019"), (word, "2020")], families=("submap",))
        node = _rollup(tmp_path, "-311", family="submap")
        assert node["generation"]["n_leaves"] == 2
        assert node["payload"]["shard_keys"] == [word]
        assert sorted(e["id"] for e in node["payload"]["granules"][0]) == ["gA", "gB", "gC"]
        # Same-order union, not a reprojection: no fold stamp at the shard node.
        assert "reproject" not in node["payload"]["metadata"]

    def test_idempotent_and_incremental(self, tmp_path):
        _write_manifest(tmp_path)
        _emit_submap(tmp_path, "-311", ["gA"])
        run_sweep(str(tmp_path), _leaf_refs("-311"), families=("submap",))
        second = run_sweep(str(tmp_path), _leaf_refs("-311"), families=("submap",))
        assert second["families"]["submap"]["written"] == 0
        # A later run adds a sibling shard; sweeping ONLY it keeps -311's
        # contribution via the stored sibling rollup.
        _emit_submap(tmp_path, "-312", ["gB"])
        run_sweep(str(tmp_path), _leaf_refs("-312"), families=("submap",))
        r31 = _rollup(tmp_path, "-31", family="submap")["payload"]
        assert sorted(e["id"] for e in r31["granules"][0]) == ["gA", "gB"]

    def test_malformed_submap_counts_failed(self, tmp_path):
        from zagg.sweep import _node_rel

        _write_manifest(tmp_path)
        node = tmp_path / _node_rel("-311")
        node.mkdir(parents=True)
        (node / "shardmap.json").write_text(json.dumps({"shard_keys": [1]}))  # missing keys
        result = run_sweep(str(tmp_path), _leaf_refs("-311"), families=("submap",))
        assert result["families"]["submap"]["failed"] == 1
        assert result["families"]["submap"]["written"] == 0

    def test_submap_emittable_gates_unmergeable_units(self):
        from zagg.sweep import submap_emittable

        # A HEALPix signature with id-bearing entries is the only emittable case.
        assert submap_emittable(SUBMAP_SIG, [_entry("gA"), _entry("gB")])
        # Rectilinear raster signature (no parent_order/child_order) -> skip: the
        # sub-shardmap fold is HEALPix-morton only (reproject is HEALPix-only).
        rect_sig = {
            "type": "rectilinear",
            "crs": "EPSG:4326",
            "affine": [1.0, 0.0, 0.0, 0.0, -1.0, 0.0],
            "shape": [10, 10],
            "chunk_shape": [5, 5],
        }
        raster_entries = [{"s3": "s3://b/t0", "datetime": "2025-01-01T00:00:00Z"}]
        assert not submap_emittable(rect_sig, raster_entries)
        # HEALPix signature but id-less entries (raster ShardMap falls back to
        # datetime, telemetry.py) -> also skip, never a KeyError in the fold.
        assert not submap_emittable(SUBMAP_SIG, raster_entries)
        assert not submap_emittable(None, [_entry("gA")])

    def test_unsupported_signature_counts_empty_not_failed(self, tmp_path):
        # A well-formed-but-unsupported leaf sub-map (all required keys, but a
        # rectilinear grid_signature the HEALPix-only fold cannot coarsen) is a
        # SKIP, not corruption: read_leaf returns None -> "empty", never "failed".
        from zagg.sweep import _node_rel

        _write_manifest(tmp_path)
        node = tmp_path / _node_rel("-311")
        node.mkdir(parents=True)
        (node / "shardmap.json").write_text(
            json.dumps(
                {
                    "grid_signature": {"type": "rectilinear", "shape": [10, 10]},
                    "shard_keys": [morton_word("-311")],
                    "granules": [[{"s3": "s3://b/t0", "datetime": "2025-01-01T00:00:00Z"}]],
                }
            )
        )
        result = run_sweep(str(tmp_path), _leaf_refs("-311"), families=("submap",))
        assert result["families"]["submap"]["empty"] == 1
        assert result["families"]["submap"]["failed"] == 0
        assert result["families"]["submap"]["written"] == 0


class TestLocalRunnerEmitsSubmap:
    """The local backend's in-process worker writes the leaf sub-map on
    success (issue #300) — sibling to the stats sidecar, fail-open."""

    SHARD = "-5112333"  # order 6, matching default_config's parent_order

    def _agg(self, monkeypatch, tmp_path, *, meta_error=None):
        from zagg import hive, runner
        from zagg.config import default_config
        from zagg.runner import agg

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        shard = morton_word(self.SHARD)
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            return {"shard_key": int(shard_key), "error": meta_error, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": int(cfg.output["grid"]["parent_order"]),
                "child_order": int(cfg.output["grid"]["child_order"]),
                "layout": cfg.output["grid"].get("layout", "fullsphere"),
            },
            "shard_keys": [int(shard)],
            "granules": [[_entry("g1")]],
        }
        path = tmp_path / "catalog.json"
        path.write_text(json.dumps(catalog))
        root = str(tmp_path / "out")
        agg(cfg, catalog=str(path), store=root, backend="local")
        return root, shard, catalog

    def test_success_writes_leaf_submap(self, monkeypatch, tmp_path):
        from zagg.catalog.shardmap import ShardMap
        from zagg.sweep import submap_key

        root, shard, catalog = self._agg(monkeypatch, tmp_path)
        from zagg.hive import shard_leaf_path

        leaf = shard_leaf_path(root, shard)
        prefix, _, name = leaf.rpartition("/")
        sub = ShardMap.from_json(f"{prefix}/{submap_key(name)}")
        assert sub.shard_keys == [shard]
        assert [g["id"] for g in sub.granules[0]] == ["g1"]
        assert sub.grid_signature == catalog["grid_signature"]

    def test_failed_shard_writes_none(self, monkeypatch, tmp_path):
        from zagg.hive import shard_leaf_path
        from zagg.sweep import submap_key

        root, shard, _catalog = self._agg(monkeypatch, tmp_path, meta_error="boom")
        leaf = shard_leaf_path(root, shard)
        prefix, _, name = leaf.rpartition("/")
        assert not Path(f"{prefix}/{submap_key(name)}").exists()


class TestFamilyRegistry:
    def test_unknown_family_raises(self):
        with pytest.raises(ValueError, match="unknown sweep family"):
            get_family("nope")

    @pytest.mark.parametrize(
        ("name", "marker"),
        [("debris", "not implemented")],
    )
    def test_stub_families_refuse_with_pointer(self, name, marker):
        assert name in sweep_mod.FAMILIES  # registered slot stays visible
        with pytest.raises(NotImplementedError, match="stubbed"):
            get_family(name)
        try:
            get_family(name)
        except NotImplementedError as e:
            assert marker in str(e)

    def test_overview_family_is_available(self):
        # Issue #201: the reserved slot is implemented — a whole-tree family
        # (sweep_store hook), not a per-node JSON fold.
        fam = get_family("overview")
        assert callable(fam.sweep_store)

    def test_default_families_are_the_implemented_set(self):
        assert sweep_mod.DEFAULT_FAMILIES == ("stats", "moc", "submap", "overview")


def _run_record(root, rows, run_id="r1"):
    from zagg.telemetry import write_run_parquet

    write_run_parquet(str(root), rows, run_id=run_id)


def _row(decimal, *, window=None, success=True):
    from zagg.telemetry import build_record, failure_record, flatten_record

    if success:
        rec = build_record(
            shard_key=morton_word(decimal),
            metadata={"total_obs": 1, "duration_s": 1.0},
            window=window,
        )
    else:
        rec = failure_record(shard_key=morton_word(decimal), error="boom")
    return flatten_record(rec)


class TestDiscoverLeaves:
    def test_column_first_discovery(self, tmp_path):
        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        _run_record(
            tmp_path,
            [
                _row("-311"),
                _row("-312"),
                _row("-321", success=False),  # failure rows never become work
            ],
        )
        refs = discover_leaves(str(tmp_path))
        assert refs == [(morton_word("-311"), None), (morton_word("-312"), None)]

    def test_windowed_column_names_the_leaf(self, tmp_path):
        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        # Windowed store: the manifest carries a temporal block.
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["spec"] = "morton-hive/2"
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        _run_record(
            tmp_path, [_row("-311", window="2019"), _row("-311", window="2020")], run_id="r2"
        )
        refs = discover_leaves(str(tmp_path))
        assert refs == [(morton_word("-311"), "2019"), (morton_word("-311"), "2020")]

    def test_pre_column_windowed_rows_fall_back_to_node_list(self, tmp_path):
        import pandas as pd

        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["spec"] = "morton-hive/2"
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        # A pre-#300 run record: no window column at all. Its shard's windows
        # resolve from the node's sidecar names (one bounded delimiter LIST).
        row = _row("-311", window="2019")
        row.pop("window")
        df = pd.DataFrame([row])
        df["shard_key"] = pd.array([morton_word("-311")], dtype="UInt64")
        df.to_parquet(
            tmp_path / "stats_20260101T000000Z_old1.parquet",
            engine="fastparquet",
            index=False,
            object_encoding="utf8",
        )
        _put_leaf(tmp_path, "-311", window="2019")
        _put_leaf(tmp_path, "-311", window="2020")
        refs = discover_leaves(str(tmp_path))
        assert refs == [(morton_word("-311"), "2019"), (morton_word("-311"), "2020")]

    def test_mixed_deployment_pre_and_post_column_records_union(self, tmp_path):
        # Mixed deployment: one post-#300 record names window "2019" for a
        # shard, and a pre-column record touches the SAME shard whose node also
        # holds window "2020"'s sidecar. The fallback node LIST must fire even
        # though "2019" already named the shard — the subtraction that skipped
        # already-seen keys dropped "2020". LIST + set dedup is idempotent.
        import pandas as pd

        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["spec"] = "morton-hive/2"
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        # Post-column record: names window "2019" for shard -311.
        _run_record(tmp_path, [_row("-311", window="2019")], run_id="new1")
        # Pre-column record for the same shard: no window column at all.
        row = _row("-311", window="2020")
        row.pop("window")
        df = pd.DataFrame([row])
        df["shard_key"] = pd.array([morton_word("-311")], dtype="UInt64")
        df.to_parquet(
            tmp_path / "stats_20260101T000000Z_old3.parquet",
            engine="fastparquet",
            index=False,
            object_encoding="utf8",
        )
        # The node holds BOTH windows' sidecars; only "2020" needs the fallback.
        _put_leaf(tmp_path, "-311", window="2019")
        _put_leaf(tmp_path, "-311", window="2020")
        refs = discover_leaves(str(tmp_path))
        assert refs == [(morton_word("-311"), "2019"), (morton_word("-311"), "2020")]

    def test_inexact_float_keys_skipped_with_warning(self, tmp_path, caplog):
        import pandas as pd

        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        # A pre-fix parquet whose key column collapsed to float64: packed
        # words are inexact there and must be skipped, not silently mangled.
        row = _row("-311")
        df = pd.DataFrame([row])
        df["shard_key"] = df["shard_key"].astype("float64")
        df.to_parquet(
            tmp_path / "stats_20260101T000000Z_old2.parquet",
            engine="fastparquet",
            index=False,
            object_encoding="utf8",
        )
        refs = discover_leaves(str(tmp_path))
        assert refs == []
        assert "inexact" in caplog.text

    def test_no_manifest_raises(self, tmp_path):
        from zagg.sweep import discover_leaves

        with pytest.raises(ValueError, match="not a hive store root"):
            discover_leaves(str(tmp_path))


class TestSweepCli:
    def test_end_to_end_discovers_and_folds(self, tmp_path, capsys):
        from zagg.sweep import main

        _write_manifest(tmp_path)
        rec_a = _put_leaf(tmp_path, "-311")
        rec_b = _put_leaf(tmp_path, "-312")
        _run_record(tmp_path, [_row("-311"), _row("-312")])
        assert main([str(tmp_path), "--families", "stats"]) == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["families"]["stats"]["written"] == 4  # -311, -312, -31, -3
        assert _rollup(tmp_path, "-3")["payload"] == merge([rec_a, rec_b])

    def test_empty_store_is_a_noop(self, tmp_path, capsys):
        from zagg.sweep import main

        _write_manifest(tmp_path)
        assert main([str(tmp_path)]) == 0
        assert "nothing to sweep" in capsys.readouterr().out

    def test_end_to_end_windowed_discovery_folds_both_windows(self, tmp_path):
        # The at-scale seam: a windowed run record threads through
        # discover_leaves into a real run_sweep, and the discovered
        # ``(key, window)`` pairs name the leaves the fold reads — exercising
        # the discovery-side sidecar-name grammar against the fold-side reader.
        from zagg.sweep import discover_leaves

        _write_manifest(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["spec"] = "morton-hive/2"
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        # Real windowed leaf sidecars for two windows of one shard...
        a = _put_leaf(tmp_path, "-311", window="2019")
        b = _put_leaf(tmp_path, "-311", window="2020")
        # ...and a real run record carrying the window column.
        _run_record(tmp_path, [_row("-311", window="2019"), _row("-311", window="2020")])
        refs = discover_leaves(str(tmp_path))
        assert refs == [(morton_word("-311"), "2019"), (morton_word("-311"), "2020")]
        run_sweep(str(tmp_path), refs, families=("stats",))
        node = _rollup(tmp_path, "-311")
        assert node["generation"]["n_leaves"] == 2
        assert node["windows"] == ["2019", "2020"]
        assert node["payload"] == merge([a, b])

    def _config_yaml(self, tmp_path, pyramid=None, grid=None, name="config.yaml"):
        """A real pipeline config file (the shipped atl06, hive layout)."""
        import yaml

        from zagg.config import default_config

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        if pyramid is not None:
            cfg.output["pyramid"] = pyramid
        if grid is not None:
            cfg.output["grid"] = grid
        path = tmp_path / name
        path.write_text(
            yaml.safe_dump(
                {
                    "data_source": cfg.data_source,
                    "aggregation": cfg.aggregation,
                    "output": cfg.output,
                }
            )
        )
        return path

    def test_declare_pyramid_flag_is_declaration_only(self, tmp_path, capsys):
        from zagg.hive import read_manifest
        from zagg.sweep import main

        _write_manifest(tmp_path)  # legacy pyramid block
        # Real sweepable work, so the no-rollup assertion below DISCRIMINATES:
        # without it the fallthrough path writes nothing either (an empty store
        # is a no-op — see test_empty_store_is_a_noop), and the
        # declaration-only claim would go untested (review finding, issue #358).
        # The sidecars carry no commit stamp, so validation still takes the
        # no-committed-leaf branch.
        _put_leaf(tmp_path, "-311")
        _put_leaf(tmp_path, "-312")
        _run_record(tmp_path, [_row("-311"), _row("-312")])
        config_path = self._config_yaml(tmp_path)
        assert main([str(tmp_path), "--declare-pyramid", str(config_path)]) == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["updated"] is True and summary["orders"] == [0]
        # Uncommitted refs on this store: validation records the loud skip.
        assert summary["validated"].startswith("manifest only")
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block["orders"] == [0]  # manifest shard_order 2, spacing 2
        assert block["fields"]["count"]["class"] == "exact"
        # Declaration-only semantics: no sweep pass ran in the invocation —
        # the same store swept normally writes four stats rollups.
        assert not list(tmp_path.rglob("*.rollup.json"))

    def test_declare_pyramid_flag_prints_a_v2_summary(self, tmp_path, capsys):
        # The printed summary mirrors the manifest block: /2 declares `overviews`
        # and `orders` DOES NOT EXIST. An empty `orders` is /1's wire signal
        # for "pyramid declared off" (specification §4.5), so printing it
        # beside a levels list would read to the operator as "no pyramid".
        from zagg.hive import read_manifest
        from zagg.pyramid import PYRAMID_SPEC_V2
        from zagg.sweep import main

        _write_manifest(tmp_path)  # shard_order 2, cell_order 4
        expanded = [{"node": 2, "cells": [3]}, {"node": 1, "cells": [2]}, {"node": 0, "cells": [1]}]
        # The grid matches the STORE (shard 2 / cell 4): config-side
        # validation and the manifest-truth check see the same window.
        config_path = self._config_yaml(
            tmp_path,
            pyramid={"overviews": [3]},
            grid={"type": "healpix", "parent_order": 2, "child_order": 4},
        )
        assert main([str(tmp_path), "--declare-pyramid", str(config_path)]) == 0
        out = capsys.readouterr().out
        summary = json.loads(out)
        assert "orders" not in summary and '"orders"' not in out
        assert summary["overviews"] == expanded and summary["updated"] is True
        block = read_manifest(str(tmp_path))["pyramid"]
        assert block["spec"] == PYRAMID_SPEC_V2 and block["overviews"] == expanded

    def test_declare_pyramid_flag_does_not_sweep_a_sweepable_store(self, tmp_path, capsys):
        # The positive control for the assertion above: the SAME fixture, swept
        # without the flag, writes rollups — so their absence really is the
        # flag's doing (issue #358).
        from zagg.sweep import main

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _put_leaf(tmp_path, "-312")
        _run_record(tmp_path, [_row("-311"), _row("-312")])
        assert main([str(tmp_path), "--families", "stats"]) == 0
        assert json.loads(capsys.readouterr().out)["families"]["stats"]["written"] == 4
        assert list(tmp_path.rglob("*.rollup.json"))

    def test_empty_declare_pyramid_value_never_sweeps(self, tmp_path):
        # An unset shell variable expands to --declare-pyramid="": the flag must
        # fail loudly on the empty path, never fall through to the WRITING sweep
        # pass it advertises it will not run (review finding, issue #358).
        from zagg.sweep import main

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _run_record(tmp_path, [_row("-311")])
        with pytest.raises((FileNotFoundError, IsADirectoryError, OSError)):
            main([str(tmp_path), "--declare-pyramid", ""])
        assert not list(tmp_path.rglob("*.rollup.json"))

    def test_declare_pyramid_flag_is_idempotent(self, tmp_path, capsys):
        from zagg.sweep import main

        _write_manifest(tmp_path)
        config_path = self._config_yaml(tmp_path)
        assert main([str(tmp_path), "--declare-pyramid", str(config_path)]) == 0
        capsys.readouterr()
        assert main([str(tmp_path), "--declare-pyramid", str(config_path)]) == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["updated"] is False and summary["previous"] == "identical"

    def test_declare_pyramid_failures_surface_not_swallowed(self, tmp_path):
        # The CLI wrapper deliberately has no error handling: a refusal escapes
        # main() and the process exits non-zero through the interpreter, the
        # same shape as ``python -m zagg``. Pinned so a future try/except that
        # swallows a refusal to ``return 0`` — the flag's whole promise is that
        # a wrong config does NOT get installed — is caught (issue #358).
        from zagg.sweep import main

        config_path = self._config_yaml(tmp_path)
        store = tmp_path / "not_a_hive_root"
        store.mkdir()
        with pytest.raises(ValueError, match="not a hive store root"):
            main([str(store), "--declare-pyramid", str(config_path)])
        _write_manifest(tmp_path)
        with pytest.raises(FileNotFoundError):
            main([str(tmp_path), "--declare-pyramid", str(tmp_path / "missing.yaml")])


class TestSweepConfig:
    """output.sweep (issue #300): default on for hive, boolean, hive-only —
    mirroring coverage_moc's posture."""

    def _cfg(self, **output):
        from zagg.config import default_config

        cfg = default_config("atl06")
        cfg.output.update(output)
        return cfg

    def test_default_on_for_hive(self):
        from zagg.config import get_sweep, validate_config

        cfg = self._cfg(store_layout="hive")
        assert get_sweep(cfg) is True
        validate_config(cfg)

    def test_explicit_off(self):
        from zagg.config import get_sweep

        assert get_sweep(self._cfg(store_layout="hive", sweep=False)) is False

    def test_null_falls_back_to_default(self):
        from zagg.config import get_sweep

        assert get_sweep(self._cfg(store_layout="hive", sweep=None)) is True

    def test_default_off_for_flat(self):
        from zagg.config import get_sweep

        assert get_sweep(self._cfg(store_layout="flat", coverage_moc=False)) is False

    def test_explicit_true_on_flat_rejected(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="output.sweep requires"):
            validate_config(self._cfg(store_layout="flat", coverage_moc=False, sweep=True))

    def test_non_bool_rejected(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="output.sweep must be a boolean"):
            validate_config(self._cfg(store_layout="hive", sweep="yes"))


class TestSweepHook:
    def test_sweep_after_run_is_fail_open(self, monkeypatch, caplog):
        from zagg import sweep as sm

        def boom(*a, **k):
            raise RuntimeError("kaput")

        monkeypatch.setattr(sm, "run_sweep", boom)
        assert sm.sweep_after_run("/nowhere", [(1, None)]) is None
        assert "fail-open" in caplog.text

    def test_leaves_from_stats_records(self):
        from zagg.sweep import leaves_from_stats_records

        records = [
            {"shard_key": 7, "window": None, "success": True},
            {"shard_key": 7, "window": "2019", "success": True},
            {"shard_key": 7, "window": "2019", "success": True},  # dup collapses
            {"shard_key": 8, "success": True},  # pre-#300 record: no window key
            {"shard_key": 9, "window": None, "success": False},  # failures skipped
            {"shard_key": None, "success": True},  # keyless skipped
            None,  # record-less envelope skipped
        ]
        assert leaves_from_stats_records(records) == [(7, None), (7, "2019"), (8, None)]

    def test_local_run_folds_rollups(self, monkeypatch, tmp_path):
        # End-to-end through the local backend (fake hive write): the
        # in-process hook folds the stats family up the ancestor chain.
        from zagg import hive, runner
        from zagg.config import default_config
        from zagg.runner import agg

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        shard = morton_word("-5112333")
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            return {"shard_key": int(shard_key), "error": None, "total_obs": 1}

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 6,
                "child_order": 12,
                "layout": "fullsphere",
            },
            "shard_keys": [int(shard)],
            "granules": [[_entry("g1")]],
        }
        path = tmp_path / "catalog.json"
        path.write_text(json.dumps(catalog))
        root = str(tmp_path / "out")
        agg(cfg, catalog=str(path), store=root, backend="local")
        # The stats rollup chain reaches the base node (order 0)...
        base = _rollup(Path(root), "-5")
        assert base is not None and base["payload"]["n_shards"] == 1
        # ...and the submap family folded the worker-emitted leaf sub-map.
        sub = _rollup(Path(root), "-5", family="submap")
        assert sub is not None
        assert [g["id"] for g in sub["payload"]["granules"][0]] == ["g1"]

    def test_local_run_sweep_off_writes_no_rollups(self, monkeypatch, tmp_path):
        from zagg import hive, runner
        from zagg.config import default_config
        from zagg.runner import agg

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        cfg.output["sweep"] = False
        shard = morton_word("-5112333")
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        monkeypatch.setattr(
            hive,
            "process_and_write_hive",
            lambda shard_key, *a, **k: {"shard_key": int(shard_key), "error": None, "total_obs": 1},
        )
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 6,
                "child_order": 12,
                "layout": "fullsphere",
            },
            "shard_keys": [int(shard)],
            "granules": [[_entry("g1")]],
        }
        path = tmp_path / "catalog.json"
        path.write_text(json.dumps(catalog))
        root = str(tmp_path / "out")
        agg(cfg, catalog=str(path), store=root, backend="local")
        assert not list(Path(root).rglob("*.rollup.json"))


class TestInvokeLambdaSweep:
    def _client(self):
        from unittest.mock import MagicMock

        return MagicMock()

    def test_inline_leaves_event(self):
        from zagg.runner import _invoke_lambda_sweep

        client = self._client()
        _invoke_lambda_sweep(client, "fn", "s3://b/store", [(7, None), (8, "2019")])
        kwargs = client.invoke.call_args.kwargs
        assert kwargs["InvocationType"] == "Event"
        event = json.loads(kwargs["Payload"])
        assert event["mode"] == "sweep"
        assert event["leaves"] == [[7, None], [8, "2019"]]
        assert "discover" not in event

    def test_oversized_leaves_fall_back_to_discovery(self):
        from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES, _invoke_lambda_sweep

        client = self._client()
        # Packed-word-sized keys (~20 digits each) comfortably overflow the budget.
        n = _ASYNC_PAYLOAD_CAP_BYTES // 16
        _invoke_lambda_sweep(client, "fn", "s3://b/store", [(10**19 + i, "2019") for i in range(n)])
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert "leaves" not in event
        assert event["discover"] is True


def _handler_module():
    import importlib.util

    handler_path = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_sweep", handler_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestHandlerSweepResponse:
    """``mode="sweep"`` returns the pass timings + record key (issue #353)."""

    def test_response_carries_durations_and_record(self, tmp_path):
        mod = _handler_module()

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        event = {
            "mode": "sweep",
            "store_path": str(tmp_path),
            "leaves": [[morton_word("-311"), None]],
        }
        response = mod._handle_sweep(event)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["ok"] is True and body["n_leaves"] == 1
        assert body["duration_s"] >= 0.0
        assert body["families"]["stats"]["duration_s"] >= 0.0
        # The record key round-trips: the object the handler names exists.
        assert (tmp_path / body["record"]).exists()
        # The leaves rode inline: no work set was derived to charge for.
        assert body["discover_s"] is None

    def test_discovery_path_reports_its_own_span(self, tmp_path):
        # discover_leaves is a LIST + a parquet read per run record; it is not
        # inside run_sweep's span, so it gets its own field rather than being
        # folded into (or dropped from) duration_s.
        mod = _handler_module()

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _run_record(tmp_path, [_row("-311")])
        response = mod._handle_sweep({"mode": "sweep", "store_path": str(tmp_path)})
        body = json.loads(response["body"])
        assert body["n_leaves"] == 1
        assert body["discover_s"] >= 0.0
        assert body["duration_s"] >= 0.0

    def test_no_work_response_carries_the_same_keys(self, tmp_path):
        # A no-work invoke (nothing swept, or discovery over a store with no
        # run records) must not change the response shape a driver reads.
        mod = _handler_module()

        _write_manifest(tmp_path)
        for event, derived in (
            ({"mode": "sweep", "store_path": str(tmp_path), "leaves": []}, False),
            ({"mode": "sweep", "store_path": str(tmp_path), "discover": True}, True),
        ):
            response = mod._handle_sweep(event)
            assert response["statusCode"] == 200
            body = json.loads(response["body"])
            assert body["ok"] is True and body["n_leaves"] == 0
            assert body["duration_s"] >= 0.0
            assert body["record"] is None
            assert (body["discover_s"] >= 0.0) if derived else (body["discover_s"] is None)


class TestSubmapIdentityCollisions:
    """The family fold is a third shardmap constructor path, so it owns the
    per-shard identity invariant too (issue #468 review finding (1))."""

    def _collided(self, gid="gDup"):
        return [
            {"id": gid, "s3": f"s3://bucket/{p}/{gid}", "https": f"https://host/{p}/{gid}"}
            for p in ("p1", "p2")
        ]

    def test_the_union_keeps_both_members_of_a_collided_pair(self):
        # Pre-#468 the union keyed on ``entry["id"]`` alone, so the second
        # granule overwrote the first and the collapse was unobservable. It must
        # survive the union to be seen at all.
        from zagg.catalog.shardmap import _recorded_identity

        pair = self._collided()
        keyed = {_recorded_identity(e)[1]: e for e in pair}
        assert len(keyed) == 2, "the distinguishing key must separate the pair"
        assert len({e["id"] for e in pair}) == 1, "an id-keyed union would drop one"

    def test_shard_node_fold_refuses_a_collided_pair(self):
        from zagg.sweep import SubmapFamily

        payload = {
            "grid_signature": dict(SUBMAP_SIG),
            "shard_keys": [morton_word("-311")],
            "granules": [self._collided()],
            "metadata": {"collection": "TEST_001"},
        }
        with pytest.raises(ValueError, match="identity collision"):
            SubmapFamily().merge([payload], node="-311", order=SHARD_ORDER)

    def test_a_refused_fold_is_a_named_skip_not_a_lost_node(self, caplog):
        # ``_merged`` is fail-open, so the refusal lands as a skipped rollup
        # node. Pin that the operator at least gets the collision named in the
        # log rather than a bare "merge failed" -- the fail-open posture itself
        # is raised for review on the PR, not changed here.
        import logging as _logging

        from zagg.sweep import SubmapFamily, _merged

        payload = {
            "grid_signature": dict(SUBMAP_SIG),
            "shard_keys": [morton_word("-311")],
            "granules": [self._collided()],
            "metadata": {"collection": "TEST_001"},
        }
        counts = {"failed": 0}
        with caplog.at_level(_logging.WARNING):
            out = _merged(SubmapFamily(), [payload], "-311", SHARD_ORDER, counts)
        assert out is None and counts["failed"] == 1
        assert any("identity collision" in r.message for r in caplog.records)
