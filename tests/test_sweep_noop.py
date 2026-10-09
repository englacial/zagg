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
"""

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
    (``tools/configs/atl03_windowed_measure.yaml``) is a windowed store — so
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
