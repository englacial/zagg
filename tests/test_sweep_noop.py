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

import pytest
from test_sweep_stage import DENSE_16, _wide_store


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


class TestNoOpVisit:
    def _reread(self, root, manifest, by_shard, monkeypatch, *, dispatch):
        """One tuple re-run with every artifact current; the keys it read."""
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
        # No coverage: the tail's own call, whose node set IS its work set's
        # ancestors. It must stay unscoped-legal.
        from zagg.sweep_fleet import run_stage_sweep_fleet

        summary = run_stage_sweep_fleet(None, "fn", str(tmp_path), [], shard_order=3)
        assert summary["skipped"] == "no dispatch nodes" and summary["scope"] is None

    def test_the_fleet_dispatcher_records_an_explicit_all(self, tmp_path):
        from zagg.sweep_fleet import run_stage_sweep_fleet

        summary = run_stage_sweep_fleet(
            None, "fn", str(tmp_path), [], shard_order=3, coverage=[1], scope="all"
        )
        assert summary["skipped"] == "no dispatch nodes" and summary["scope"] is None


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
