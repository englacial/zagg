"""Unified rollup sweep (issue #300): engine + stats family (§8.3 obligations).

Covers the standing D22 test claims on local stores: rollup == direct (stats
fold), sweep idempotence (second pass over an unchanged tree writes nothing),
leaf re-runs making ancestors detectably stale, incremental accumulation
(window and sibling union), and nothing-load-bearing (deleting every rollup
leaves leaf reads intact).
"""

import json
from datetime import datetime, timezone

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

    def test_default_families_cover_stats_and_moc(self, tmp_path):
        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        _stamp_leaf(tmp_path, "-311")
        summary = run_sweep(str(tmp_path), _leaf_refs("-311"))
        assert set(summary["families"]) == {"stats", "moc"}
        assert _rollup(tmp_path, "-3", family="stats") is not None
        assert _rollup(tmp_path, "-3", family="moc") is not None


class TestFamilyRegistry:
    def test_unknown_family_raises(self):
        with pytest.raises(ValueError, match="unknown sweep family"):
            get_family("nope")

    @pytest.mark.parametrize(
        ("name", "marker"),
        [("submap", "PR #295"), ("overview", "issue #201"), ("debris", "not implemented")],
    )
    def test_stub_families_refuse_with_pointer(self, name, marker):
        assert name in sweep_mod.FAMILIES  # registered slot stays visible
        with pytest.raises(NotImplementedError, match="stubbed"):
            get_family(name)
        try:
            get_family(name)
        except NotImplementedError as e:
            assert marker in str(e)

    def test_stats_is_a_default_family(self):
        assert "stats" in sweep_mod.DEFAULT_FAMILIES
