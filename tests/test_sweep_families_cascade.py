"""The families rollup walk inside the staged cascade (issue #610 phase 4).

Standing claims:

- there is ONE families walk (:func:`zagg.sweep_families.fold_span`): the
  whole-tree pass and the staged cascade's per-tuple rider fold through the
  same function, so a store's rollups do not depend on which executor
  produced them — the "no second cascade" criterion espg set for issue #620
  (https://github.com/englacial/zagg/issues/620#issuecomment-6068583904),
  read across to the families walk;
- a staged sweep carrying the families writes the rollups the families pass
  writes, byte for byte, at every tuple width — the span decomposition is
  exact, not approximate;
- the rollups ride each dispatch node's CLOSE, so no two writers share one
  rollup object;
- a span that raises is fail-open per node and per family (D9): the ladder's
  own fold is unaffected;
- the ladder's finisher composes the families' store-root singletons — the
  §10 temporal section inside ``coverage.moc`` and its ``coverage.toc``
  sibling — byte-identically to the families pass, from the base-node
  rollups plus the accumulator blocks the records carry, reading NO leaf.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import zarr
from test_sweep_stage import DENSE_16, LEAVES, _stage_store

import zagg.sweep_families as fam_mod
from zagg.grids.morton import morton_word
from zagg.hive import COMMIT_ATTR, shard_leaf_path
from zagg.store import open_store
from zagg.sweep import run_sweep, write_leaf_submap
from zagg.sweep_families import CASCADE_FAMILIES, normalize_families, rider_for
from zagg.sweep_stages import run_stage_sweep
from zagg.telemetry import build_record, write_sidecar

SHARD_ORDER = 3
SUBMAP_SIG = {
    "type": "healpix",
    "indexing_scheme": "nested",
    "parent_order": SHARD_ORDER,
    "child_order": SHARD_ORDER + 2,
    "layout": "flat",
}


def _leaf_families(root, decimals):
    """Give every leaf the three families' artifacts: sidecar, stamp, sub-map."""
    for i, dec in enumerate(decimals):
        word = morton_word(dec)
        leaf = shard_leaf_path(str(root), word)
        write_sidecar(
            leaf,
            build_record(
                shard_key=word,
                metadata={"total_obs": 10 + i, "cells_with_data": 2, "duration_s": 0.5},
                granule_ids=[f"g-{dec}"],
            ),
        )
        group = zarr.open_group(open_store(leaf), path="", mode="a", zarr_format=3)
        group.attrs[COMMIT_ATTR] = {
            "written_at": f"2026-10-0{1 + i % 9}T00:00:00+00:00",
            "shard_key": str(int(word)),
        }
        write_leaf_submap(
            str(root),
            word,
            [{"id": f"g-{dec}", "s3": f"s3://b/{dec}", "https": f"https://h/{dec}"}],
            grid_signature=SUBMAP_SIG,
            metadata={"collection": "TEST_001"},
        )


def _store(root, leaves=LEAVES):
    """A ``/2`` hive store whose leaves carry every rollup family's artifact."""
    root.mkdir(parents=True, exist_ok=True)
    manifest = _stage_store(root, leaves=leaves)
    _leaf_families(root, leaves)
    return manifest


def _twin(root, other):
    """A byte-identical copy of the store, so the two arms fold the same inputs.

    The leaf artifacts carry wall-clock stamps (the stats record's
    ``timestamp``, the sub-map's ``generated_at``), which a second build would
    not reproduce: the claim under test is about the WALK, so both arms must
    start from one store.
    """
    shutil.copytree(root, other)
    return other


def _refs(leaves=LEAVES):
    return [(morton_word(d), None) for d in leaves]


def _explode(*a, **kw):
    raise AssertionError("the finisher read a leaf (issue #610 phase 4)")


def _by_shard_of(leaves) -> dict:
    """``{shard decimal: {window}}`` from the ``(word, window)`` leaf refs."""
    from zagg.grids.morton import morton_decimal

    out: dict = {}
    for word, window in leaves:
        out.setdefault(morton_decimal(int(word)), set()).add(window)
    return out


def _rollups(root: Path) -> dict:
    """Every rollup object on the store, keyed by its store-relative path."""
    return {
        str(p.relative_to(root)): json.loads(p.read_text())
        for p in sorted(root.rglob("*.rollup.json"), key=str)
    }


def _families_only(rollups: dict) -> dict:
    """The three cascade families' rollups — never the overview envelope."""
    return {k: v for k, v in rollups.items() if v.get("family") in CASCADE_FAMILIES}


class TestOneWalk:
    def test_the_staged_rollups_are_the_families_pass_rollups(self, tmp_path):
        pass_root, cascade_root = tmp_path / "pass", tmp_path / "cascade"
        manifest = _store(pass_root)
        _twin(pass_root, cascade_root)
        run_sweep(
            str(pass_root), _refs(), families=CASCADE_FAMILIES, record=False
        )  # the whole-tree walk
        run_stage_sweep(str(cascade_root), _refs(), families=None, record=False)
        assert manifest["shard_order"] == SHARD_ORDER
        expected = _families_only(_rollups(pass_root))
        # 4 leaves under two base cells: shard nodes 1111/1112/1121/-2111,
        # then 111/112/-211, then 11/-21, then 1/-2 — 11 nodes a family.
        assert len(expected) == 33
        assert _families_only(_rollups(cascade_root)) == expected

    def test_the_rollups_do_not_depend_on_the_tuple_width(self, tmp_path):
        source = tmp_path / "source"
        _store(source)
        roots = {}
        for width in (1, 3):
            root = _twin(source, tmp_path / f"w{width}")
            run_stage_sweep(str(root), _refs(), families=None, tuple_width=width, record=False)
            roots[width] = _families_only(_rollups(root))
        assert roots[1] == roots[3]
        assert roots[1]

    def test_every_rollup_is_folded_through_the_one_engine(self, tmp_path, monkeypatch):
        root = tmp_path / "s"
        _store(root)
        spans: list = []
        real = fam_mod.fold_span

        def spy(store, fam, by_shard, **kw):
            spans.append((fam.name, kw["from_order"], kw["to_order"]))
            return real(store, fam, by_shard, **kw)

        monkeypatch.setattr(fam_mod, "fold_span", spy)
        run_stage_sweep(str(root), _refs(), families=None, tuple_width=1, record=False)
        # Every rollup on the store came out of a span of this engine, and the
        # spans are exactly the width-1 ladder's three tuples: [2,3), [1,2),
        # [0,1). Nothing folded outside it.
        assert {sp[1:] for sp in spans} == {(3, 2), (2, 1), (1, 0)}
        assert {s[0] for s in spans} == set(CASCADE_FAMILIES)
        assert _families_only(_rollups(root))


class TestWhereItRuns:
    def test_a_window_unit_folds_no_rollup(self, tmp_path):
        """A rollup object merges every window of its node — only the close owns it."""
        from zagg.sweep_stages import sweep_stage_pass

        root = tmp_path / "s"
        manifest = _store(root)
        rider = rider_for(None)
        sweep_stage_pass(
            str(root),
            manifest,
            {d: {None} for d in LEAVES},
            run_id="W",
            tuple_width=3,
            families=rider,
        )
        # An unwindowed store's one unit per node closes inline, so the
        # rollups DO land here; what is pinned is that the rider was asked
        # once per dispatch node, in that close, and never per window.
        assert _families_only(_rollups(root))
        assert rider.counts["stats"]["written"] == 11

    def test_the_unbound_rider_refuses(self):
        with pytest.raises(ValueError, match="never bound"):
            rider_for(None).fold_node("1", dispatch=0, child_order=3)


class TestFailOpen:
    def test_a_raising_span_leaves_the_ladder_alone(self, tmp_path, monkeypatch, caplog):
        import zagg.sweep as sweep_mod

        root = tmp_path / "s"
        _store(root)

        def boom(*a, **kw):
            raise RuntimeError("leaf read exploded")

        monkeypatch.setattr(sweep_mod, "_rollup_shard_node", boom)
        with caplog.at_level("WARNING"):
            summary = run_stage_sweep(str(root), _refs(), families=None, record=False)
        assert not _families_only(_rollups(root))  # nothing written, nothing crashed
        failures = summary["families"]["node_failures"]
        assert {f["node"] for f in failures} == {"1", "-2"}
        assert {f["family"] for f in failures} == set(CASCADE_FAMILIES)
        assert "leaf read exploded" in caplog.text
        # The ladder folded regardless: its own artifacts are there.
        assert summary["levels"]
        assert sum(row["written"] for row in summary["stages"]) > 0


class TestKnob:
    def test_the_default_is_the_ladder_alone(self, tmp_path):
        root = tmp_path / "s"
        _store(root)
        summary = run_stage_sweep(str(root), _refs(), record=False)
        assert "families" not in summary
        assert not _families_only(_rollups(root))

    def test_none_is_the_default_set_and_empty_is_off(self):
        assert normalize_families(None) == CASCADE_FAMILIES
        assert normalize_families(()) == ()
        assert rider_for(()) is None
        assert rider_for(["stats"]).names == ("stats",)

    def test_the_overview_family_cannot_ride(self):
        with pytest.raises(ValueError, match="overview family IS the cascade"):
            normalize_families(["stats", "overview"])
        with pytest.raises(ValueError, match="cannot ride the staged cascade"):
            normalize_families(["stat"])


class TestDenseStore:
    def test_a_sixteen_leaf_store_matches_the_families_pass(self, tmp_path):
        pass_root, cascade_root = tmp_path / "pass", tmp_path / "cascade"
        _store(pass_root, leaves=DENSE_16)
        _twin(pass_root, cascade_root)
        run_sweep(str(pass_root), _refs(DENSE_16), families=CASCADE_FAMILIES, record=False)
        run_stage_sweep(
            str(cascade_root), _refs(DENSE_16), families=None, tuple_width=2, record=False
        )
        assert _families_only(_rollups(cascade_root)) == _families_only(_rollups(pass_root))


# ---------------------------------------------------------------------------
# Phase 2: the ladder's finisher composes the families' root singletons.
# ---------------------------------------------------------------------------

#: The temporal ``/2`` spec fixture, cloned to N leaves with every family's
#: leaf artifact (``tests/test_sweep_store_handle``): shard_order 4, cells 6,
#: an every-order ladder, a leaf column, a leaf stamp and — on half the
#: leaves — a §10 ``temporal.toc`` record, so both temporal routes run.
TEMPORAL_LEAVES = 8


def _temporal_pair(tmp_path, n=TEMPORAL_LEAVES):
    """``(pass arm, cascade arm, leaf refs)`` over one byte-identical store."""
    from test_sweep_store_handle import _store as _temporal_store

    root, leaves = _temporal_store(tmp_path / "pass", n)
    _twin(Path(root), tmp_path / "cascade")
    return root, str(tmp_path / "cascade"), leaves


def _stable_root(root, name):
    """A root object with every ``generated_at`` dropped — the identity oracle."""
    obj = json.loads((Path(root) / name).read_text())
    obj.pop("generated_at", None)
    (obj.get("temporal") or {}).pop("generated_at", None)
    return obj


class TestRootComposition:
    @pytest.mark.parametrize("name", ["coverage.moc", "coverage.toc"])
    def test_the_root_objects_match_the_families_pass(self, tmp_path, name):
        """The §10 section and its cover sibling are the single pass's, byte for byte."""
        pass_root, cascade_root, leaves = _temporal_pair(tmp_path)
        swept = run_sweep(pass_root, leaves, families=CASCADE_FAMILIES, record=False)
        assert swept["families"]["moc"]["root_moc_written"] is True
        assert swept["families"]["moc"]["temporal_shards"] == TEMPORAL_LEAVES
        summary = run_stage_sweep(cascade_root, leaves, families=None, record=False)
        composed = summary["families"]["finish"]
        assert composed["source"] == "pass"
        assert composed["families"]["moc"]["temporal_shards"] == TEMPORAL_LEAVES
        assert composed["families"]["moc"]["base_rollups"] == 1  # one base cell
        assert _stable_root(cascade_root, name) == _stable_root(pass_root, name)

    def test_the_moc_family_owns_the_root_refresh(self, tmp_path):
        """``run_finisher`` stands down rather than re-PUT a subset of the words."""
        pass_root, cascade_root, leaves = _temporal_pair(tmp_path, 4)
        summary = run_stage_sweep(cascade_root, leaves, families=None, record=False)
        assert summary["finisher"]["root_moc_from"] == "families"
        assert summary["finisher"]["root_moc"] is False
        assert summary["families"]["finish"]["root_moc_written"] is True
        ladder = run_stage_sweep(pass_root, leaves, record=False)  # families off
        assert ladder["finisher"]["root_moc_from"] == "work-set"
        assert ladder["finisher"]["root_moc"] is True


class TestFinisherReadsNoLeaf:
    def test_the_records_arm_composes_the_same_objects_without_a_leaf_read(
        self, tmp_path, monkeypatch
    ):
        """The fleet path: the units' records carry the §10 block, the finisher composes.

        Every leaf read a family could make is monkeypatched to raise, so a
        finisher that touched one fails rather than quietly re-walking the
        tree — that walk is the 900 s wall this issue is about.
        """
        from zagg.hive import read_manifest
        from zagg.sweep import MocFamily, StatsFamily, SubmapFamily
        from zagg.sweep_families import finish_families
        from zagg.sweep_stages import sweep_stage_pass

        driver_root, records_root, leaves = _temporal_pair(tmp_path)
        by_shard = _by_shard_of(leaves)
        # Arm A: the in-process driver, rider and all.
        driver = run_stage_sweep(driver_root, leaves, families=None, record=False)
        assert driver["families"]["finish"]["root_moc_written"] is True
        # Arm B: the units alone, then the finisher from their record's block.
        manifest = read_manifest(records_root)
        rider = rider_for(None)
        sweep_stage_pass(
            records_root, manifest, by_shard, run_id="B", tuple_width=3, families=rider
        )
        record = {"families": rider.summary()}
        assert "accumulator" in record["families"]["moc"]
        for cls in (StatsFamily, MocFamily, SubmapFamily):
            monkeypatch.setattr(cls, "read_leaf", _explode)
        monkeypatch.setattr("zagg.leaf_temporal.leaf_contribution", _explode)
        composed = finish_families(records_root, manifest, by_shard, records=[record])
        assert composed["source"] == "records" and composed["stage_records"] == 1
        assert composed["families"]["moc"]["temporal_shards"] == TEMPORAL_LEAVES
        assert "accumulator_error" not in composed["families"]["moc"]
        for name in ("coverage.moc", "coverage.toc"):
            assert _stable_root(records_root, name) == _stable_root(driver_root, name)

    def test_an_unusable_block_keeps_the_spatial_fold_and_says_so(
        self, tmp_path, monkeypatch, caplog
    ):
        from zagg.hive import read_manifest
        from zagg.sweep_families import finish_families

        _pass, root, leaves = _temporal_pair(tmp_path, 4)
        manifest = read_manifest(root)
        by_shard = _by_shard_of(leaves)
        run_stage_sweep(root, leaves, families=None, record=False)
        with caplog.at_level("WARNING"):
            composed = finish_families(
                root, manifest, by_shard, records=[{"families": {"moc": {}}}]
            )
        assert composed["families"]["moc"]["accumulator_error"]
        assert "temporal_shards" not in composed["families"]["moc"]
        assert "no usable" in caplog.text
        # The spatial fold still happened: the base rollup was read, and the
        # standing §10 section rides the GET-union-PUT seam (§10.4).
        assert composed["families"]["moc"]["base_rollups"] == 1

    def test_a_shard_no_record_visited_is_recorded_not_refused(self, tmp_path, caplog):
        from zagg.hive import read_manifest
        from zagg.sweep_families import finish_families
        from zagg.sweep_stages import sweep_stage_pass

        _pass, root, leaves = _temporal_pair(tmp_path, 4)
        manifest = read_manifest(root)
        by_shard = _by_shard_of(leaves)
        half = dict(list(sorted(by_shard.items()))[:2])
        rider = rider_for(None)
        sweep_stage_pass(root, manifest, half, run_id="H", tuple_width=3, families=rider)
        with caplog.at_level("WARNING"):
            composed = finish_families(
                root, manifest, by_shard, records=[{"families": rider.summary()}]
            )
        assert composed["shards_unvisited"] == 2
        assert "in no unit's families record" in caplog.text
        assert composed["families"]["moc"]["temporal_shards"] == 2


class TestRecordPlumbing:
    def test_which_families_rode_comes_from_the_records(self):
        from zagg.sweep_families import accumulator_blocks, families_in_records

        records = [{"families": {"submap": {}, "moc": {"accumulator": {"shards": {}}}}}, {}]
        assert families_in_records(records) == ("moc", "submap")
        assert families_in_records([{"families": {"overview": {}}}]) == ()
        assert accumulator_blocks(records, "moc") == [{"shards": {}}, None]
        assert accumulator_blocks(records, "stats") == [None, None]

    def test_no_family_composes_nothing(self, tmp_path):
        from zagg.hive import read_manifest
        from zagg.sweep_families import finish_families

        root = tmp_path / "s"
        _store(root)
        assert finish_families(str(root), read_manifest(str(root)), {}, records=[{}]) is None

    def test_the_worker_record_carries_the_families_block(self, tmp_path):
        from zagg.sweep_stages import run_stage_worker

        root = tmp_path / "s"
        _store(root)
        record = run_stage_worker(
            str(root),
            _refs(),
            run_id="R",
            run_started="2026-10-09T00:00:00+00:00",
            dispatch=0,
            nodes=["1", "-2"],
            child_order=3,
            records_from=str(tmp_path / "status"),
            families=None,
        )
        assert set(record["families"]) >= set(CASCADE_FAMILIES)
        assert record["families"]["stats"]["written"] == 11
        assert record["families"]["moc"]["accumulator"]["visited"] == sorted(LEAVES)

    def test_the_default_worker_carries_none(self, tmp_path):
        from zagg.sweep_stages import run_stage_worker

        root = tmp_path / "s"
        _store(root)
        record = run_stage_worker(
            str(root),
            _refs(),
            run_id="R",
            run_started="2026-10-09T00:00:00+00:00",
            dispatch=0,
            nodes=["1", "-2"],
            child_order=3,
            records_from=str(tmp_path / "status"),
        )
        assert "families" not in record


# ---------------------------------------------------------------------------
# Phase 3: the fleet — one fan-out, the families on the invokes that close.
# ---------------------------------------------------------------------------


class TestFleetDispatch:
    """The dispatcher puts the families where a node has one writer."""

    @staticmethod
    def _fleet(root, client, **kwargs):
        from test_sweep_stage_fleet import _fleet

        # A barrier this short is what every other dispatcher test uses: the
        # fake client lands no record, so the claim is what the events carry.
        kwargs.setdefault("barrier_timeout_s", 0.01)
        return _fleet(root, client, **kwargs)

    def test_an_unwindowed_run_rides_the_whole_node_invokes(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda

        root = tmp_path / "s"
        _store(root)
        client = _FakeLambda()
        summary = self._fleet(root, client, families=None)
        assert summary["families"] == list(CASCADE_FAMILIES)
        staged = [b for b in client.blocks() if b.get("role", "stage") == "stage"]
        assert staged and all(b["families"] == list(CASCADE_FAMILIES) for b in staged)
        # The finisher takes which families rode from the RECORDS, so its
        # event carries no families key at all.
        finisher = [b for b in client.blocks() if b.get("role") == "finisher"]
        assert finisher and all("families" not in b for b in finisher)

    def test_the_default_sends_nothing(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda

        root = tmp_path / "s"
        _store(root)
        client = _FakeLambda()
        summary = self._fleet(root, client)
        assert summary["families"] == []
        assert all("families" not in b for b in client.blocks())

    def test_a_windowed_run_rides_the_close_and_not_the_window_units(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda

        root = tmp_path / "s"
        _store(root)
        client = _FakeLambda()
        self._fleet(
            root,
            client,
            families=None,
            windowed=True,
            all_time=True,
            leaves=[(morton_word(d), "2019") for d in LEAVES],
        )
        by_unit = {}
        for b in client.blocks():
            if b.get("role") == "finisher":
                continue
            by_unit.setdefault(b.get("unit"), []).append(b)
        assert set(by_unit) == {"window", "close"}
        assert all("families" not in b for b in by_unit["window"])
        assert all(b["families"] == list(CASCADE_FAMILIES) for b in by_unit["close"])

    def test_a_windowed_store_with_no_close_is_refused_by_name(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda

        root = tmp_path / "s"
        _store(root)
        with pytest.raises(ValueError, match="declares no all-time fold"):
            self._fleet(
                root,
                _FakeLambda(),
                families=None,
                windowed=True,
                all_time=False,
            )

    def test_an_unknown_family_is_refused_before_anything_fires(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda

        root = tmp_path / "s"
        _store(root)
        client = _FakeLambda()
        with pytest.raises(ValueError, match="cannot ride the staged cascade"):
            self._fleet(root, client, families=["overview"])
        assert client.events == []


class TestHandlerForwarding:
    def test_the_stage_arm_forwards_the_families(self, tmp_path, monkeypatch):
        from test_sweep_stage_fleet import _event, _handler_module, _stage_block

        seen = {}
        mod = _handler_module()

        def fake(*a, **kw):
            seen.update(kw)
            return {"stages": [], "record": "r"}

        monkeypatch.setattr("zagg.sweep_stages.run_stage_worker", fake)
        root = tmp_path / "s"
        _store(root)
        block = _stage_block(0, ["1", "-2"], records_from=str(tmp_path / "status"))
        mod.lambda_handler(_event(root, {**block, "families": ["stats", "moc"]}), None)
        assert seen["families"] == ["stats", "moc"]
        seen.clear()
        mod.lambda_handler(_event(root, block), None)
        assert seen["families"] == ()
