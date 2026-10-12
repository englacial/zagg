"""The no-op visit and the operator entry points (issue #620 part 2).

Two things this pins, both about a staged pass that has nothing to do:

* **What a current node reads.** The skip-if-current gate must cost STAMPS —
  root metadata of the node's own artifact, of its sources, and of its column
  — and nothing else: no member array opened, no fold, and no per-node store
  client built. The v3 fleet measured a no-op stage invoke at p50 30.0 s with
  13 credential resolutions
  (https://github.com/englacial/zagg/issues/610#issuecomment-6067171538), so
  every one of those reads is pinned here rather than described.
* **Who may ask for a whole-store pass.** The ladder is chained by the run
  tail, scoped to the run's footprint; the hand-driven forms are recovery and
  have to name their scope.
* **Which stores the fan-out is for** (issue #635, the follow-on to question
  (11) on PR #622). The default keys on the config, so the dispatcher reads
  the STORE's ladder declaration once before it fans out: a store that
  declares none fires nothing, where it used to bill one invoke per dispatch
  node, each dying in ``ladder_entries``.
"""

import json
import logging
from pathlib import Path

import pytest
from test_sweep_stage import (
    DENSE_16,
    LEAVES,
    TestWindowedStageSweep,
    _stage_store,
    _wide_store,
)
from test_sweep_stage_fleet import _FakeLambda

from zagg.pyramid import PYRAMID_SPEC_V2
from zagg.sweep_units import UNIT_CLOSE, UNIT_WINDOW

WINDOWS = TestWindowedStageSweep.WINDOWS


def _tail_fleet(root, client, **kwargs):
    """``run_stage_sweep_fleet`` over a real work set, nothing executed."""
    from zagg.grids.morton import morton_word
    from zagg.sweep_fleet import run_stage_sweep_fleet

    return run_stage_sweep_fleet(
        client,
        "fn",
        str(root),
        [(morton_word(d), None) for d in LEAVES],
        shard_order=3,
        store_kwargs={},
        tuple_width=1,
        poll_interval_s=0.01,
        barrier_timeout_s=0.01,
        **kwargs,
    )


def _fired(client):
    """The stage node sets the dispatcher actually invoked, by dispatch order."""
    fired: dict = {}
    for block in client.blocks():
        if block["role"] == "stage":
            fired.setdefault(block["dispatch"], []).extend(block["nodes"])
    return {d: sorted(n) for d, n in fired.items()}


@pytest.fixture
def swept(tmp_path):
    """A folded ladder plus its work set: everything current, nothing to do."""
    from zagg.sweep_stages import sweep_stage_pass

    root = tmp_path / "store"
    manifest = _wide_store(root, leaves=DENSE_16)
    by_shard = {d: {None} for d in DENSE_16}
    warm = sweep_stage_pass(str(root), manifest, by_shard, run_id="warm", tuple_width=1)
    assert [r["written"] for r in warm["stages"]] == [16, 4, 1]
    return root, manifest, by_shard


#: The windowed arm's work set: four of the dense leaves, two yearly windows.
#: Four is enough — the gate fires once per order-2 dispatch node — and the
#: fixture is rebuilt per test.
WINDOWED_LEAVES = DENSE_16[:4]


@pytest.fixture
def swept_windowed(tmp_path):
    """A WINDOWED store's folded ladder: its ``(node, window)`` units and close.

    ``_stage_column_current``'s ``window`` argument is what picks the key
    (``column_name(window)``), and the config phase 1 pins to ``"stages"``
    (``tools/configs/atl03_windowed_measure_monthly.yaml``) is a windowed store — so
    the windowed arm is the production arm, and espg's acceptance wording
    asks for the per-node read bound on windowed units too
    (https://github.com/englacial/zagg/issues/620#issuecomment-6068583904).
    The wide geometry, because it is the one whose ladder writes a
    dispatch-node stage column at all.
    """
    from zagg.sweep_stages import sweep_stage_pass

    root = tmp_path / "windowed"
    manifest = _wide_store(root, leaves=WINDOWED_LEAVES, windows=WINDOWS)
    by_shard = {d: set(WINDOWS) for d in WINDOWED_LEAVES}
    warm = sweep_stage_pass(str(root), manifest, by_shard, run_id="warm", tuple_width=1)
    assert [r["written"] for r in warm["stages"]] == [12, 3, 3]
    assert warm["stages"][0]["columns_written"] == 8  # one per (node, window)
    return root, manifest, by_shard


@pytest.fixture
def swept_companioned(monkeypatch, tmp_path):
    """A folded ladder WITH its Icechunk companion — the PRODUCTION no-op path.

    ``_wide_store`` writes no companion, so ``ladder_context`` is ``None``
    there and every node's ref hook is skipped — which excludes the one
    O(subtree) thing a current node still does, and the PR's leading
    candidate for the unexplained ~28 s (review finding, issue #620). The v3
    store HAS a companion, so the counts above describe a path production
    does not take. Built by the same local run the ladder suite uses, which
    chains the staged sweep and commits the refs.
    """
    pytest.importorskip("icechunk")
    from test_icechunk_refs import _grid, _ladder_run, _shards

    from zagg import hive
    from zagg.config import default_config
    from zagg.grids.morton import morton_decimal

    cfg = default_config("atl06", validate=False)
    shards = _shards(_grid(cfg), 2)
    _grid_obj, root, _summary = _ladder_run(
        monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
    )
    manifest = hive.read_manifest(root)
    return Path(root), manifest, {morton_decimal(s): {None} for s in shards}


class TestNoOpVisit:
    def _reread(self, root, manifest, by_shard, monkeypatch, *, dispatch, **kwargs):
        """One tuple re-run with every artifact current; the keys it read.

        ``kwargs`` reach :func:`sweep_stage_pass` — ``only_unit`` /
        ``only_window`` are how a windowed store's units are driven one at a
        time, the way a fleet stage invoke runs them.
        """
        import zarr.storage._obstore as zo

        import zagg.store as store_mod
        from zagg.sweep_fold import _ColumnReader
        from zagg.sweep_stages import sweep_stage_pass

        seen: dict = {"keys": [], "arrays": [], "opens": 0}

        inner_get = zo.ObjectStore.get

        async def get(self, key, *args, **kwargs):
            seen["keys"].append(key)
            return await inner_get(self, key, *args, **kwargs)

        monkeypatch.setattr(zo.ObjectStore, "get", get)
        inner_array = _ColumnReader._array
        monkeypatch.setattr(
            _ColumnReader,
            "_array",
            lambda self, res, name: (
                seen["arrays"].append((self.path, res, name)),
                inner_array(self, res, name),
            )[1],
        )
        inner_open = store_mod.open_store

        def open_store(*args, **kwargs):
            seen["opens"] += 1
            return inner_open(*args, **kwargs)

        monkeypatch.setattr(store_mod, "open_store", open_store)
        row = sweep_stage_pass(
            str(root),
            manifest,
            by_shard,
            run_id="noop",
            tuple_width=1,
            only_dispatch=dispatch,
            **kwargs,
        )["stages"][0]
        return row, seen

    @pytest.mark.parametrize("dispatch", [2, 1, 0])
    def test_a_current_node_folds_nothing(self, swept, monkeypatch, dispatch):
        row, seen = self._reread(*swept, monkeypatch, dispatch=dispatch)
        assert row["written"] == 0 and row["current"] == row["nodes"]
        # The fold meter is the whole fold's accounting: zero cells, zero
        # blocks. Nothing streamed, so nothing was decoded or merged.
        assert (row["fold_cells_read"], row["fold_blocks"]) == (0, 0)
        # And no member ARRAY was opened — espg's question on the issue was
        # whether the gate reads the children's arrays rather than their
        # stamps. It reads the stamps: `_ColumnReader._array` is the one door
        # to a member, and the skip gate never goes through it.
        assert seen["arrays"] == []

    @pytest.mark.parametrize("dispatch", [2, 1, 0])
    @pytest.mark.parametrize("unit", [UNIT_WINDOW, UNIT_CLOSE])
    def test_a_current_windowed_unit_costs_the_same(
        self, swept_windowed, monkeypatch, dispatch, unit
    ):
        # The same bound on the arms a fleet stage invoke runs on a WINDOWED
        # store: `unit: "window"` (one unit per dirty window) and
        # `unit: "close"` (the all-time one). espg's acceptance wording asks
        # for the per-node read bound on windowed units too.
        extra = {"only_window": WINDOWS[0]} if unit == UNIT_WINDOW else {}
        row, seen = self._reread(
            *swept_windowed, monkeypatch, dispatch=dispatch, only_unit=unit, **extra
        )
        assert row["written"] == 0 and row["current"] == row["nodes"]
        assert (row["fold_cells_read"], row["fold_blocks"]) == (0, 0)
        assert seen["arrays"] == []
        assert seen["opens"] == 0  # no per-dispatch-node store client, either

    def test_the_column_gate_reads_the_units_own_window_key(self, swept_windowed, monkeypatch):
        # `_stage_column_current(..., window, ...)` is the function phase 3
        # changed, and `window` is what picks the key (`column_name(window)`).
        # It is reached only at the gather tuple, which is the one that writes
        # a dispatch-node column — so this pins that a windowed unit asks
        # about ITS OWN window and finds it current.
        import zagg.sweep_stage as stage_mod

        gate = []
        inner = stage_mod._stage_column_current

        def spy(store, store_root, node, window, *args, **kwargs):
            answer = inner(store, store_root, node, window, *args, **kwargs)
            gate.append((node, window, answer))
            return answer

        monkeypatch.setattr(stage_mod, "_stage_column_current", spy)
        self._reread(
            *swept_windowed,
            monkeypatch,
            dispatch=2,
            only_unit=UNIT_WINDOW,
            only_window=WINDOWS[0],
        )
        assert gate == [(d[:3], WINDOWS[0], True) for d in WINDOWED_LEAVES]
        # The close unit writes no stage column, so it never reaches the gate.
        gate.clear()
        self._reread(*swept_windowed, monkeypatch, dispatch=2, only_unit=UNIT_CLOSE)
        assert gate == []

    @pytest.mark.parametrize("dispatch", [2, 1, 0])
    def test_a_current_node_builds_no_store_client(self, swept, monkeypatch, dispatch):
        # The issue #610 one-handle rule: every read of the pass goes through
        # the invoke's single obstore handle by RELATIVE key. `open_store` on
        # an absolute path builds a fresh client — botocore credential
        # resolution included, ~120 ms a call on S3 and never cache-hit on a
        # per-node path — which `_stage_column_current` used to do once per
        # dispatch node (issue #620 part 2).
        _row, seen = self._reread(*swept, monkeypatch, dispatch=dispatch)
        assert seen["opens"] == 0

    def test_the_gate_reads_exactly_the_stamps(self, swept, monkeypatch):
        # The order-2 gather tuple of the d = 2 fixture: 16 dispatch nodes,
        # one leaf child each, each writing a stage column. Per node the gate
        # reads three root metadata objects — its own artifact, its child
        # column, its own column — plus the node's envelope (a plain GET,
        # outside this counter).
        root, manifest, by_shard = swept
        row, seen = self._reread(root, manifest, by_shard, monkeypatch, dispatch=2)
        assert row["nodes"] == 16
        assert all(key.endswith("zarr.json") for key in seen["keys"]), seen["keys"]
        by_kind: dict = {}
        for key in seen["keys"]:
            by_kind.setdefault(key.rsplit("/", 2)[-2], []).append(key)
        assert {k: len(v) for k, v in by_kind.items()} == {
            "all.zarr": 16,  # the node's own artifact: the skip gate's target
            "all.pyramid.zarr": 32,  # its child's column (gather source) + its own
        }

    @pytest.mark.parametrize("dispatch", [3, 0])
    def test_the_companioned_arm_still_folds_nothing_but_still_commits(
        self, swept_companioned, monkeypatch, dispatch
    ):
        # With the companion present the fold bound is unchanged — and the
        # ref hook RUNS anyway, because its gate is
        # `dirty = any(d.startswith(node) for d in by_shard)`, the work set,
        # not "did anything change". So a full-coverage re-run pays one
        # commit per current node: `icechunk_refs` and `icechunk_s` are the
        # cost the read-set table above does not contain, and the stage
        # records carry them per tuple (PR body question (3)).
        from zagg.icechunk_ladder import ladder_context

        root, manifest, by_shard = swept_companioned
        assert ladder_context(str(root), manifest, store_kwargs={}) is not None
        row, seen = self._reread(root, manifest, by_shard, monkeypatch, dispatch=dispatch)
        assert row["written"] == 0 and row["current"] == row["nodes"]
        assert (row["fold_cells_read"], row["fold_blocks"]) == (0, 0)
        assert seen["arrays"] == []
        # The hook, on a node with nothing to fold:
        assert row["icechunk_commits"] == row["nodes"] and row["icechunk_refs"] > 0
        assert row["icechunk_clean"] == 0  # not the "nothing to do" arm


class TestOperatorScope:
    """The hand-driven forms are recovery, and recovery names its scope."""

    def test_none_refuses_by_name(self):
        from zagg.sweep_stages import operator_scope

        with pytest.raises(ValueError, match="needs an explicit scope"):
            operator_scope(None, what="a pass")

    def test_all_is_the_explicit_whole_store(self):
        from zagg.sweep_stages import operator_scope

        assert operator_scope("all", what="a pass") is None

    def test_prefixes_normalize_like_any_scope(self):
        from zagg.sweep_stages import normalize_scope, operator_scope

        assert list(operator_scope(["111", "112"], what="a pass")) == list(
            normalize_scope(["111", "112"])
        )

    def test_the_fleet_dispatcher_refuses_coverage_without_a_scope(self, tmp_path):
        # `coverage=` resolves the node set from the STORE instead of the work
        # set — the unscoped full-store form. The client is never touched: the
        # refusal lands before anything is invoked or read.
        from zagg.sweep_fleet import run_stage_sweep_fleet

        with pytest.raises(ValueError, match="run_stage_sweep_fleet"):
            run_stage_sweep_fleet(
                None, "fn", str(tmp_path), [], shard_order=3, coverage=[1, 2], scope=None
            )

    def test_the_fleet_dispatcher_takes_the_tail_unscoped(self, tmp_path):
        # Issue #620 ruled item (2): the tail needed no change because its
        # node set is already its work set's ancestors. REAL leaves, because
        # `leaves=[]` bails at "no dispatch nodes" before any scope logic and
        # `scope is None` is then equally true of the tail and of the
        # whole-store form (review finding) — what pins the item is the fired
        # node set, tuple by tuple, against `dispatch_nodes` over the work set.
        from zagg.sweep_fleet import dispatch_nodes

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client)
        assert summary["scope"] is None
        by_shard = {d: {None} for d in LEAVES}
        assert _fired(client) == {d: dispatch_nodes(by_shard, d) for d in (2, 1, 0)}
        # And nothing wider: every node fired is an ancestor of a work leaf.
        assert all(
            any(leaf.startswith(node) for leaf in LEAVES)
            for nodes in _fired(client).values()
            for node in nodes
        )

    def test_the_fleet_dispatcher_records_an_explicit_all(self, tmp_path):
        # `scope="all"` is the operator's explicit whole store: it records as
        # unscoped AND fans out the coverage's own assignment, which on this
        # store is wider than nothing. Same real-leaves reasoning as above.
        from zagg.hive import read_root_coverage, root_coverage_words
        from zagg.sweep_fleet import coverage_dispatch_nodes

        root = tmp_path / "s"
        _stage_store(root)
        words = root_coverage_words(read_root_coverage(str(root)))
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client, coverage=words, scope="all")
        assert summary["scope"] is None and summary["coverage_computed"] is True
        by_shard = {d: {None} for d in LEAVES}
        assert _fired(client) == {d: coverage_dispatch_nodes(by_shard, d, words) for d in (2, 1, 0)}


class TestStagesCliScope:
    def test_stages_without_a_scope_refuses(self, capsys):
        from zagg.sweep import main

        with pytest.raises(SystemExit) as exc:
            main(["/nowhere", "--stages"])
        assert exc.value.code == 2
        assert "--stages needs --scope" in capsys.readouterr().err

    def test_a_scope_without_stages_refuses(self, capsys):
        from zagg.sweep import main

        with pytest.raises(SystemExit) as exc:
            main(["/nowhere", "--scope", "all"])
        assert exc.value.code == 2
        assert "--scope only applies to --stages" in capsys.readouterr().err

    @pytest.mark.parametrize("bad", ["ALL", "9111", "abc", ""])
    def test_a_malformed_scope_refuses_before_the_store_is_listed(self, bad, capsys, monkeypatch):
        # The guard is argv-only (issue #620 review): a bad token must not pay
        # discover_leaves' LIST plus a parquet read per run record and then die
        # on an uncaught ValueError from morton_word. `--scope ALL` is the
        # likely operator typo — SCOPE_ALL is lowercase.
        from zagg import sweep as sweep_mod
        from zagg.sweep import main

        def listed(*a, **k):
            raise AssertionError("discover_leaves must not run for a malformed --scope")

        monkeypatch.setattr(sweep_mod, "discover_leaves", listed)
        with pytest.raises(SystemExit) as exc:
            main(["/nowhere", "--stages", "--scope", bad])
        assert exc.value.code == 2
        assert "scope" in capsys.readouterr().err

    def test_a_scoped_pass_reaches_the_driver_with_its_moc(self, swept, monkeypatch):
        from zagg import sweep as sweep_mod
        from zagg.grids.morton import morton_word
        from zagg.sweep import main

        root, _manifest, _by_shard = swept
        seen: dict = {}
        monkeypatch.setattr(
            sweep_mod, "discover_leaves", lambda *a, **k: [(morton_word(DENSE_16[0]), None)]
        )

        def driver(*a, **k):
            seen["scope"] = k["scope"]
            return {}

        monkeypatch.setattr("zagg.sweep_stages.run_stage_sweep", driver)
        assert main([str(root), "--stages", "--scope", "111,112"]) == 0
        assert list(seen["scope"]) == [morton_word("111"), morton_word("112")]

    def test_scope_all_reaches_the_driver_unscoped(self, swept, monkeypatch):
        from zagg import sweep as sweep_mod
        from zagg.grids.morton import morton_word
        from zagg.sweep import main

        root, _manifest, _by_shard = swept
        seen: dict = {"scope": "unset"}
        monkeypatch.setattr(
            sweep_mod, "discover_leaves", lambda *a, **k: [(morton_word(DENSE_16[0]), None)]
        )

        def driver(*a, **k):
            seen["scope"] = k["scope"]
            return {}

        monkeypatch.setattr("zagg.sweep_stages.run_stage_sweep", driver)
        assert main([str(root), "--stages", "--scope", "all"]) == 0
        assert seen["scope"] is None


class TestStoreLadderGate:
    """The dispatcher keys the staged sweep on the STORE's ladder (issue #635).

    The default is the CONFIG's (``zagg.config.get_sweep_mode``, issue #620)
    and ``pyramid.spec`` is written once, at init — so a ``/1``-manifest hive
    store appended to by a ladder-declaring config used to fan out one stage
    invoke per dispatch node of the schedule (287 on the California o9
    shardmap), every one of them dying in
    ``zagg.sweep_stage.ladder_entries``: billed invokes, zero writes. One
    manifest GET before the first invoke makes that a run that fires nothing.
    """

    def _redeclare(self, root, pyramid):
        """The fixture store's manifest, with another ``pyramid`` block.

        The store the hazard describes: its leaves and columns are the ``/2``
        fixture's and only the declaration differs — which is exactly what an
        append to a pre-#384 store looks like, since no append rewrites
        ``pyramid``.
        """
        from zagg.hive import MANIFEST_NAME

        path = Path(root) / MANIFEST_NAME
        manifest = json.loads(path.read_text())
        manifest["pyramid"] = pyramid
        path.write_text(json.dumps(manifest))

    #: Every declaration the workers refuse, and the clause that names it. A
    #: ``/1`` schedule and a declared-off ``/1`` are the two shapes PR #622's
    #: Migration note describes; the ``/2`` shapes are an empty and two
    #: malformed ``overviews`` lists. A ladder that merely FILTERS to nothing
    #: is not here — the workers accept it, so the dispatcher does too
    #: (``test_a_leaf_only_ladder_still_fans_out``).
    NO_LADDER = [
        ({"spec": "zagg-pyramid/1", "overview": {"orders": [2, 1], "spacing": 1}}, "/1 stores"),
        ({"spec": "zagg-pyramid/1", "overview": {"orders": []}}, "/1 stores"),
        ({"spec": PYRAMID_SPEC_V2, "overviews": []}, "absent or empty"),
        # ``ladder_entries`` normalizes each entry, so a malformed one raises
        # KeyError/TypeError rather than the named ValueError — and a decline
        # is owed for those too (review finding).
        ({"spec": PYRAMID_SPEC_V2, "overviews": [{"node": 2}]}, "KeyError"),
        ({"spec": PYRAMID_SPEC_V2, "overviews": ["2"]}, "TypeError"),
    ]

    @pytest.mark.parametrize("pyramid, named", NO_LADDER)
    def test_a_store_with_no_ladder_fires_nothing(self, tmp_path, caplog, pyramid, named):
        root = tmp_path / "s"
        _stage_store(root)
        self._redeclare(root, pyramid)
        client = _FakeLambda(None)
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            summary = _tail_fleet(root, client)
        # ZERO invokes: not the finisher's either, and no barrier was waited.
        assert client.events == [] and summary["invokes"] == 0
        assert summary["skipped"] == "no zagg-pyramid/2 ladder"
        assert summary["stages"] == []
        assert summary["finisher"] == {"landed": False, "fired": False}
        # ONE warning, and it names the retrofit and what it saw.
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "--declare-pyramid" in warnings[0] and named in warnings[0]

    def test_the_gate_filters_at_the_manifest_shard_order(self, tmp_path):
        # The leaf exclusion is the workers' own, and every worker takes
        # ``int(manifest["shard_order"])`` — so a caller value that disagrees
        # must not decide the verdict (review finding). Pinned on the one
        # declaration where the two differ: a malformed entry at order 2 is
        # validated only when the gate filters at the store's 3, where a
        # caller's 2 would exclude it and pass a declaration every worker
        # refuses by name.
        from zagg.sweep_fleet import no_ladder_reason

        root = tmp_path / "s"
        _stage_store(root)  # shard_order 3, ladder at orders 2..0
        self._redeclare(
            root,
            {
                "spec": PYRAMID_SPEC_V2,
                "overviews": [
                    {"node": 2, "cells": [4, 5]},
                    {"node": 1, "cells": [3]},
                    {"node": 0, "cells": [2]},
                ],
            },
        )
        assert "carries 2 members" in (no_ladder_reason(str(root), 2, {}) or "")

    def test_a_leaf_only_ladder_still_fans_out(self, tmp_path):
        # A /2 declaration whose only entry is the leaf one (the fleet's own
        # column) filters to an EMPTY ladder, and every worker accepts it:
        # the three worker gates call ``ladder_entries`` for its refusals and
        # discard the list, so such a run folds no level but still does the
        # finisher's store-root work — the root MOC, the manifest actuals,
        # the run record. Declining it would move what a /2 store does
        # (review finding), so the gate does not.
        from zagg.sweep_fleet import dispatch_nodes

        root = tmp_path / "s"
        _stage_store(root)
        self._redeclare(root, {"spec": PYRAMID_SPEC_V2, "overviews": [{"node": 3, "cells": [4]}]})
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client)
        assert summary["skipped"] is None
        by_shard = {d: {None} for d in LEAVES}
        assert _fired(client) == {d: dispatch_nodes(by_shard, d) for d in (2, 1, 0)}
        assert summary["finisher"]["fired"] is True

    def test_a_manifest_that_is_not_an_object_fires_nothing(self, tmp_path):
        # The corrupt-manifest arm of the same finding: ``read_manifest``
        # parses whatever JSON is there, so a non-object manifest reaches the
        # gate as an AttributeError on ``manifest.get`` rather than as a
        # refusal — still a decline, never a traceback out of the dispatcher.
        from zagg.hive import MANIFEST_NAME

        root = tmp_path / "s"
        _stage_store(root)
        (root / MANIFEST_NAME).write_text(json.dumps(["not", "a", "manifest"]))
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client)
        assert client.events == [] and summary["invokes"] == 0
        assert summary["skipped"].startswith("no zagg-pyramid/2 ladder")

    def test_a_root_with_no_manifest_fires_nothing(self, tmp_path):
        root = tmp_path / "empty"
        root.mkdir()
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client)
        assert client.events == [] and summary["skipped"] == "no zagg-pyramid/2 ladder"

    def test_a_two_store_is_untouched(self, tmp_path):
        # Acceptance: no change to what a /2 store does. The same fan-out,
        # tuple by tuple, as ``dispatch_nodes`` over the work set.
        from zagg.sweep_fleet import dispatch_nodes

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client)
        assert summary["skipped"] is None
        by_shard = {d: {None} for d in LEAVES}
        assert _fired(client) == {d: dispatch_nodes(by_shard, d) for d in (2, 1, 0)}

    def test_the_gate_is_one_manifest_read(self, tmp_path, monkeypatch):
        # D8 is amended for ONE read, so the count is pinned rather than
        # described: the worker arm is not executed here
        # (``_FakeLambda(None)``), so every call counted is the dispatcher's.
        import zagg.hive as hive_mod

        read = []
        inner = hive_mod.read_manifest
        monkeypatch.setattr(
            hive_mod,
            "read_manifest",
            lambda root, **kw: (read.append(root), inner(root, **kw))[1],
        )
        root = tmp_path / "s"
        _stage_store(root)
        summary = _tail_fleet(root, _FakeLambda(None))
        assert read == [str(root)] and summary["invokes"] > 0

    def test_an_unreadable_manifest_fails_open(self, tmp_path, caplog, monkeypatch):
        # A dispatcher role may hold nothing but ``lambda:InvokeFunction``
        # against the store (the D8 posture ``shard_order`` is handed in for),
        # so a denied GET keeps the fan-out it has today and names the grant
        # that would close the hazard — it never takes the ladder away.
        def denied(*_a, **_kw):
            raise PermissionError("AccessDenied")

        monkeypatch.setattr("zagg.hive.read_manifest", denied)
        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            summary = _tail_fleet(root, client)
        assert summary["skipped"] is None and summary["invokes"] > 0
        assert any("could not be read" in r.getMessage() for r in caplog.records)

    def test_the_families_of_a_ladderless_store_come_back_unswept(self, tmp_path, caplog):
        # The families ride a node CLOSE (issue #610 phase 4), and a run that
        # fires nothing closes nothing — so the tail's own backstop warning is
        # owed, and it reads this flag.
        from zagg.runner import _warn_families_unswept

        root = tmp_path / "s"
        _stage_store(root)
        self._redeclare(root, {"spec": "zagg-pyramid/1", "overview": {"orders": []}})
        client = _FakeLambda(None)
        summary = _tail_fleet(root, client, families=["stats"])
        assert client.events == [] and summary["families_unswept"] is True
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            _warn_families_unswept(["stats"], summary)
        assert any("rode no node close" in r.getMessage() for r in caplog.records)

    def test_a_declined_run_leaves_the_icechunk_finalize_free_to_tag(self, tmp_path):
        # The gate MOVES this verdict and it is deliberate (review finding).
        # Before it, such a run fanned out, no stage record landed and the
        # barriers expired — ``barrier_timed_out`` — so
        # ``_staged_sweep_incomplete`` skipped the finalize and left the run
        # untagged. A declined run fires nothing, so nothing is in flight:
        # that function's own rule ("a sweep that fired nothing has nothing
        # in flight") returns None and the run is tagged. The tag is honest —
        # the leaves are complete and the store declares no ladder to sweep.
        from zagg.runner import _staged_sweep_incomplete

        root = tmp_path / "s"
        _stage_store(root)
        self._redeclare(root, {"spec": "zagg-pyramid/1", "overview": {"orders": [2, 1]}})
        summary = _tail_fleet(root, _FakeLambda(None))
        assert summary["barrier_timed_out"] is False
        assert _staged_sweep_incomplete(summary) is None

    def test_the_run_tail_seam_shares_the_gate(self, tmp_path):
        # The dispatcher's two callers are the runner's tail seam and the
        # hand-called form above; the gate is in the dispatcher, so one gate
        # serves both.
        from zagg.grids.morton import morton_word
        from zagg.runner import _invoke_lambda_stage_sweep

        root = tmp_path / "s"
        _stage_store(root)
        self._redeclare(root, {"spec": "zagg-pyramid/1", "overview": {"orders": [2, 1]}})
        client = _FakeLambda(None)
        summary = _invoke_lambda_stage_sweep(
            client,
            "fn",
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            shard_order=3,
            store_kwargs={},
            barrier_timeout_s=0.01,
        )
        assert client.events == [] and summary["skipped"] == "no zagg-pyramid/2 ladder"
