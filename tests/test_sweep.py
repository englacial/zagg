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
        assert set(summary["families"]) == {"stats", "moc", "submap"}
        assert _rollup(tmp_path, "-3", family="stats") is not None
        assert _rollup(tmp_path, "-3", family="moc") is not None
        # No leaf sub-map was written -> the submap family finds nothing and
        # stays empty without failing the pass.
        assert summary["families"]["submap"]["empty"] >= 1
        assert _rollup(tmp_path, "-3", family="submap") is None


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
        metadata={"collection": "TEST_001"},
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
        assert json.loads(path.read_text())["written_at"]  # staleness stamp rides the file

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
        [("overview", "issue #201"), ("debris", "not implemented")],
    )
    def test_stub_families_refuse_with_pointer(self, name, marker):
        assert name in sweep_mod.FAMILIES  # registered slot stays visible
        with pytest.raises(NotImplementedError, match="stubbed"):
            get_family(name)
        try:
            get_family(name)
        except NotImplementedError as e:
            assert marker in str(e)

    def test_default_families_are_the_implemented_set(self):
        assert sweep_mod.DEFAULT_FAMILIES == ("stats", "moc", "submap")
