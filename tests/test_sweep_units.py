"""The ``(node, window)`` stage unit and the chunk-streamed fold (issue #586 phase 4).

Standing claims:

- :func:`zagg.sweep_units.stage_units` is THE enumeration — the in-process
  pass, the fleet worker and the fleet dispatcher all call it;
- a window unit folds its own window from that window's leaves alone, to the
  values an unwindowed sweep of those leaves produces; N window units of a
  node share no object, the node envelope included;
- the node close (the all-time fold) k-way merges the node's per-window
  overviews, exists only where the store declares ``all_time``, and counts a
  window whose unit died as missing;
- a unit that fails costs its own window and is named in the stage record;
- the fold is streamed block by block: same values as the whole-level fold,
  one block's inputs held at a time, the stage column written and read back
  one chunk object per block;
- the stage records carry the pipeline run id beside the sweep's own
  (issue #593), ``null`` for a pass that names none.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import zarr
from test_sweep_stage import (
    BOTH_CHANNEL_FIELDS,
    FIELDS,
    LEAVES,
    TestWindowedStageSweep,
    _artifact,
    _leaf_slabs,
    _stage_store,
    _wide_store,
    _write_leaf,
)
from test_sweep_stage_fleet import (
    _assert_identical,
    _cli_sweep,
    _FakeLambda,
    _handler_module,
    _records,
    _restore,
    _snapshot,
    _write_discovery_record,
)

import zagg.sweep_fold as fold_mod
import zagg.sweep_stages as stages_mod
import zagg.sweep_units as units_mod
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, build_root_coverage, write_root_coverage
from zagg.store import open_store
from zagg.sweep_fold import (
    ColumnMovedError,
    FoldMeter,
    _gather_slabs,
    _merge_slabs,
    refold_on_move,
)
from zagg.sweep_overview import ENVELOPE_NAME, decode_digest
from zagg.sweep_stage import ForeignSweepError, _ColumnReader
from zagg.sweep_stages import (
    FINISHER_RECORD_NAME,
    run_stage_worker,
    stage_record_name,
    sweep_stage_pass,
)
from zagg.sweep_units import UNIT_CLOSE, UNIT_WINDOW, closes_nodes, node_windows, stage_units

WINDOWS = TestWindowedStageSweep.WINDOWS
RUN_STARTED = "2026-10-01T00:00:00+00:00"


def _windowed_store(root, *, all_time=True):
    """Two yearly windows over the four fixture leaves; ``all_time`` declared or not."""
    manifest = TestWindowedStageSweep()._windowed_store(root)
    if not all_time:
        manifest["pyramid"]["overview"]["all_time"] = False
        (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1))
    return manifest


def _by_shard(windows=WINDOWS, leaves=LEAVES):
    return {d: set(windows) for d in leaves}


def _refs(windows=WINDOWS, leaves=LEAVES):
    return [(morton_word(d), w) for d in leaves for w in windows]


def _overview(root, rel):
    """``(attrs block, {array: values})`` of one ladder overview."""
    group = _artifact(root, rel)
    block = dict(group.attrs)["zagg_overview"]
    arrays = group[str(block["cell_order"])]
    return block, {name: arrays[name][:] for name in ("morton", "count", "h_tdigest")}


def _same(x, y):
    if x.dtype == object:
        return all(bytes(p or b"") == bytes(q or b"") for p, q in zip(x, y, strict=True))
    return np.array_equal(x, y)


def _overviews(root, basename):
    return sorted(str(p.relative_to(root)) for p in root.rglob(basename))


# ---------------------------------------------------------------------------
# The enumeration.
# ---------------------------------------------------------------------------


class TestStageUnits:
    def test_an_unwindowed_node_is_one_unit_that_closes_inline(self):
        units = stage_units({d: {None} for d in LEAVES}, 0, windowed=False, all_time=False)
        assert units == [
            {"node": "-2", "windows": [None], "close": False},
            {"node": "1", "windows": [None], "close": False},
        ]

    def test_a_windowed_node_is_one_unit_per_dirty_window(self):
        work = {"1111": {"2019", "2020"}, "1112": {"2020"}, "-2111": {"2021"}}
        units = stage_units(work, 2, windowed=True, all_time=False)
        assert units == [
            {"node": "-211", "windows": ["2021"], "close": False},
            {"node": "111", "windows": ["2019", "2020"], "close": False},
        ]

    def test_the_close_is_gated_on_the_declaration(self):
        assert closes_nodes(windowed=True, all_time=True)
        assert not closes_nodes(windowed=True, all_time=False)
        assert not closes_nodes(windowed=False, all_time=True)  # its one fold IS all-time
        on = stage_units(_by_shard(), 0, windowed=True, all_time=True)
        assert [u["close"] for u in on] == [True, True]
        off = stage_units(_by_shard(), 0, windowed=True, all_time=False)
        assert [u["close"] for u in off] == [False, False]

    def test_scope_and_candidates_shape_the_node_set(self):
        from zagg.sweep_stages import normalize_scope

        work = {"1111": {"2019"}}
        assert [u["node"] for u in stage_units(work, 0, windowed=True, all_time=False)] == ["1"]
        # A wider candidate set (the in-process pass's work set ∪ root MOC)
        # adds nodes; one with no dirty window has a unit only if it closes.
        wide = dict(candidates=["1111", "-2111"])
        assert [u["node"] for u in stage_units(work, 0, windowed=True, all_time=False, **wide)] == [
            "1"
        ]
        closing = stage_units(work, 0, windowed=True, all_time=True, **wide)
        assert closing == [
            {"node": "-2", "windows": [], "close": True},
            {"node": "1", "windows": ["2019"], "close": True},
        ]
        scoped = stage_units(
            work, 0, windowed=True, all_time=True, scope=normalize_scope(["-2"]), **wide
        )
        assert [u["node"] for u in scoped] == ["-2"]

    def test_a_dirt_only_node_is_listed_for_its_refs(self):
        # Issue #580: an unwindowed node whose leaves only moved refs still
        # gets its unit (the pass re-gathers, folds nothing).
        units = stage_units({}, 0, windowed=False, all_time=False, dirt_only={"1111": {None}})
        assert units == [{"node": "1", "windows": [None], "close": False}]

    def test_the_reserved_token_gets_no_window_unit(self, caplog):
        units = stage_units({"1111": {"all", "2019"}}, 0, windowed=True, all_time=False)
        assert units == [{"node": "1", "windows": ["2019"], "close": False}]
        assert "reserved all-time token" in caplog.text

    def test_dispatch_nodes_is_the_same_enumeration(self):
        from zagg.sweep_fleet import dispatch_nodes

        work = {d: {None} for d in LEAVES}
        for order in (0, 1, 2):
            units = stage_units(work, order, windowed=False, all_time=False)
            assert dispatch_nodes(work, order) == [u["node"] for u in units]

    def test_every_executor_calls_the_one_function(self, tmp_path, monkeypatch):
        # The in-process pass (the local backend and the --stages backstop),
        # the fleet worker and the fleet dispatcher: one enumeration.
        from zagg.sweep_fleet import run_stage_sweep_fleet

        calls = []
        real = units_mod.stage_units

        def spy(work, dispatch, **kwargs):
            calls.append((int(dispatch), kwargs["windowed"], kwargs["all_time"]))
            return real(work, dispatch, **kwargs)

        monkeypatch.setattr(units_mod, "stage_units", spy)
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        assert calls == [(0, True, True)]
        del calls[:]
        run_stage_worker(
            str(root),
            _refs(),
            run_id="A",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
            records_from=_records(root),
            unit=UNIT_WINDOW,
            window="2019",
        )
        assert calls == [(0, True, True)]
        del calls[:]
        run_stage_sweep_fleet(
            _FakeLambda(None),
            "fn",
            str(root),
            _refs(),
            shard_order=3,
            store_kwargs={},
            poll_interval_s=0.01,
            barrier_timeout_s=0.01,
            windowed=True,
            all_time=True,
        )
        assert (0, True, True) in calls


# ---------------------------------------------------------------------------
# Window units and the node close, in process.
# ---------------------------------------------------------------------------


class TestWindowUnits:
    def test_a_window_folds_to_what_its_own_leaves_fold_to(self, tmp_path):
        # The reference is independent of the window machinery: an UNWINDOWED
        # store holding one window's leaves, swept on its own. Every ladder
        # level of the windowed store's ``{window}.zarr`` must carry the same
        # arrays — the per-window fold is the pre-phase fold of those leaves.
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        summary = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A", tuple_width=1)
        assert all(row["failed"] == 0 for row in summary["stages"])
        for w, window in enumerate(WINDOWS):
            ref = tmp_path / f"ref-{window}"
            ref_manifest = _stage_store(ref, leaves=(), write_moc=False)
            for i, dec in enumerate(LEAVES):
                _write_leaf(ref, dec, i, slabs=_leaf_slabs(i + 10 * w))
            write_root_coverage(str(ref), build_root_coverage([morton_word(d) for d in LEAVES], 3))
            sweep_stage_pass(
                str(ref), ref_manifest, {d: {None} for d in LEAVES}, run_id="R", tuple_width=1
            )
            names = _overviews(root, f"{window}.zarr")
            assert names == [
                n.replace("all.zarr", f"{window}.zarr") for n in _overviews(ref, "all.zarr")
            ]
            assert len(names) == 7
            for rel in names:
                block, arrays = _overview(root, rel)
                ref_block, ref_arrays = _overview(ref, rel.replace(f"{window}.zarr", "all.zarr"))
                for name in arrays:
                    assert _same(arrays[name], ref_arrays[name]), (rel, name)
                for key in ("regime", "merges_from_raw", "source_children", "content_hash"):
                    assert block[key] == ref_block[key], (rel, key)
                assert block["window"] == window

    def test_the_all_time_overview_is_the_kway_merge_of_the_window_overviews(self, tmp_path):
        from zagg.stats.tdigest import merge_tdigests_kway
        from zagg.sweep_overview import overview_fold_delta

        root = tmp_path / "s"
        manifest = _windowed_store(root)
        sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A", tuple_width=1)
        delta = overview_fold_delta(FIELDS["h_tdigest"])
        names = _overviews(root, "all.zarr")
        assert len(names) == 7
        for rel in names:
            block, arrays = _overview(root, rel)
            per_window = [_overview(root, rel.replace("all.zarr", f"{w}.zarr")) for w in WINDOWS]
            assert np.array_equal(arrays["count"], sum(a["count"] for _b, a in per_window))
            assert np.array_equal(arrays["morton"], per_window[0][1]["morton"])
            for j, payload in enumerate(arrays["h_tdigest"]):
                digests = [decode_digest(a["h_tdigest"][j], "float32") for _b, a in per_window]
                merged = merge_tdigests_kway([d for d in digests if len(d)], delta=delta)
                assert np.array_equal(decode_digest(payload, "float32"), merged), (rel, j)
            # One more merge than its sources: 2 over a gather level's gen-1
            # overviews, 3 over a merge level's gen-2 ones.
            assert block["regime"] == "stage-merge"
            assert block["merges_from_raw"] == 1 + per_window[0][0]["merges_from_raw"]
            assert block["source_windows"] == {"folded": 2, "missing": 0, "unreadable": 0}
            assert block["window"] == "all"
        gens = {
            _overview(root, rel)[0]["order"]: _overview(root, rel)[0]["merges_from_raw"]
            for rel in names
        }
        assert gens == {2: 2, 1: 3, 0: 3}

    def test_a_gather_level_all_time_fold_keeps_its_gen1_inputs(self, tmp_path):
        # At a gather level the per-window overviews ARE the gen-1 members the
        # all-time fold used to read off the child columns: same digests in,
        # so the same cell out as one flat merge of the leaves' own partials.
        from zagg.stats.tdigest import merge_tdigests_kway
        from zagg.sweep_overview import overview_fold_delta

        root = tmp_path / "s"
        manifest = _windowed_store(root)
        sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        block, arrays = _overview(root, "1/1/1/all.zarr")
        assert block["order"] == 2 and block["cell_order"] == 3
        delta = overview_fold_delta(FIELDS["h_tdigest"])
        for j, leaf in enumerate(("1111", "1112")):
            parts = []
            for window in WINDOWS:
                column = _artifact(root, f"1/1/1/{leaf[-1]}/{window}.pyramid.zarr")
                parts.append(decode_digest(column["3"]["h_tdigest"][:][0], "float32"))
            flat = merge_tdigests_kway(parts, delta=delta)
            assert np.array_equal(decode_digest(arrays["h_tdigest"][j], "float32"), flat)

    def test_window_units_share_no_object(self, tmp_path):
        # N window units of one node run concurrently on the fleet, so none of
        # them may read-modify-write a shared object: a windowed store's
        # sweep writes no node envelope at all, and the skip gate reads each
        # artifact's own attrs.
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        first = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        assert not list(root.rglob(ENVELOPE_NAME))
        (row,) = first["stages"]
        assert (row["window_units"], row["close_units"], row["written"]) == (4, 2, 21)
        (again,) = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="B")["stages"]
        assert (again["written"], again["current"], again["failed"]) == (0, 21, 0)
        # An unwindowed store's one unit per node still owns its envelope.
        plain = tmp_path / "plain"
        sweep_stage_pass(str(plain), _stage_store(plain), {d: {None} for d in LEAVES}, run_id="A")
        assert len(list(plain.rglob(ENVELOPE_NAME))) == 7

    def test_no_close_without_the_declaration(self, tmp_path):
        root = tmp_path / "s"
        manifest = _windowed_store(root, all_time=False)
        (row,) = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")["stages"]
        assert (row["window_units"], row["close_units"]) == (4, 0)
        assert not _overviews(root, "all.zarr") and len(_overviews(root, "2019.zarr")) == 7

    def test_the_close_covers_windows_this_run_did_not_touch(self, tmp_path):
        # An append of one window: the all-time fold still holds both. The
        # node's windows are read off the store (one LIST), not the work set.
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        before = _overview(root, "1/all.zarr")
        assert node_windows(open_store_root(root), "1") == set(WINDOWS)
        (row,) = sweep_stage_pass(str(root), manifest, _by_shard(("2020",)), run_id="B")["stages"]
        assert (row["window_units"], row["written"]) == (2, 0)
        after = _overview(root, "1/all.zarr")
        assert after[0]["source_windows"]["folded"] == 2
        assert _same(after[1]["count"], before[1]["count"])

    def test_a_window_only_pass_runs_one_window_and_no_close(self, tmp_path):
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        (row,) = sweep_stage_pass(
            str(root),
            manifest,
            _by_shard(),
            run_id="A",
            only_unit=UNIT_WINDOW,
            only_window="2019",
        )["stages"]
        assert (row["window_units"], row["close_units"]) == (2, 0)
        assert len(_overviews(root, "2019.zarr")) == 7
        assert not _overviews(root, "2020.zarr") and not _overviews(root, "all.zarr")
        (closed,) = sweep_stage_pass(
            str(root), manifest, _by_shard(), run_id="A", only_unit=UNIT_CLOSE
        )["stages"]
        assert (closed["window_units"], closed["close_units"], closed["written"]) == (0, 2, 7)

    def test_a_unit_named_against_an_unwindowed_store_refuses(self, tmp_path):
        root = tmp_path / "s"
        manifest = _stage_store(root)
        with pytest.raises(ValueError, match="unwindowed store"):
            sweep_stage_pass(
                str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", only_unit=UNIT_CLOSE
            )
        with pytest.raises(ValueError, match="unknown stage unit"):
            sweep_stage_pass(
                str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", only_unit="node"
            )


def open_store_root(root):
    from zagg.store import open_object_store

    return open_object_store(str(root))


# ---------------------------------------------------------------------------
# A unit that fails, or dies.
# ---------------------------------------------------------------------------


class TestFailedUnit:
    def _flaky(self, monkeypatch, window="2020", error=RuntimeError("worker died")):
        real = stages_mod.stage_node

        def stage_node(*args, **kwargs):
            if kwargs["window"] == window and args[2] == "1":
                raise error
            return real(*args, **kwargs)

        monkeypatch.setattr(stages_mod, "stage_node", stage_node)

    def test_a_failed_window_unit_fails_only_its_own_window(self, tmp_path, monkeypatch):
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        self._flaky(monkeypatch)
        (row,) = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")["stages"]
        # Visible in the stage record, by name; the pass went on.
        assert row["unit_errors"] == [
            {"node": "1", "unit": "2020", "error": "RuntimeError: worker died"}
        ]
        assert row["failed"] == 1 and row["window_units"] == 4
        # The node's other window, and the other node entirely, are whole.
        assert (root / "1" / "2019.zarr").exists() and not (root / "1" / "2020.zarr").exists()
        assert (root / "-2" / "2020.zarr").exists()
        # The close says which window it folded without.
        block, _ = _overview(root, "1/all.zarr")
        assert block["source_windows"] == {"folded": 1, "missing": 1, "unreadable": 0}
        assert row["under_covered"] >= 1
        whole, _ = _overview(root, "-2/all.zarr")
        assert whole["source_windows"] == {"folded": 2, "missing": 0, "unreadable": 0}

    def test_the_next_pass_heals_it(self, tmp_path, monkeypatch):
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        with monkeypatch.context() as patched:
            self._flaky(patched)
            sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        (row,) = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="B")["stages"]
        assert row["failed"] == 0 and "unit_errors" not in row
        block, _ = _overview(root, "1/all.zarr")
        assert block["source_windows"] == {"folded": 2, "missing": 0, "unreadable": 0}

    def test_a_window_that_never_lands_is_recorded_once_not_rewritten_forever(self, tmp_path):
        # An append whose new window's unit produced nothing: the all-time
        # fold's inputs are unchanged, but the artifact must now say it is one
        # window short — and then stay current until that window lands.
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        assert _overview(root, "1/all.zarr")[0]["source_windows"]["missing"] == 0
        late = {d: {"2021"} for d in LEAVES}  # dirty, but no column ever landed
        (row,) = sweep_stage_pass(str(root), manifest, late, run_id="B")["stages"]
        block, _ = _overview(root, "1/all.zarr")
        assert block["source_windows"] == {"folded": 2, "missing": 1, "unreadable": 0}
        assert row["written"] == 7 and row["under_covered"] == 7
        (again,) = sweep_stage_pass(str(root), manifest, late, run_id="C")["stages"]
        assert (again["written"], again["current"]) == (0, 7)

    def test_a_foreign_sweep_still_aborts_the_pass(self, tmp_path, monkeypatch):
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        self._flaky(monkeypatch, error=ForeignSweepError("two sweeps are live"))
        with pytest.raises(ForeignSweepError):
            sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")

    def test_an_unwindowed_unit_raises_as_it_always_has(self, tmp_path, monkeypatch):
        root = tmp_path / "s"
        manifest = _stage_store(root)
        self._flaky(monkeypatch, window=None)
        with pytest.raises(RuntimeError, match="worker died"):
            sweep_stage_pass(str(root), manifest, {d: {None} for d in LEAVES}, run_id="A")


# ---------------------------------------------------------------------------
# The chunk-streamed fold.
# ---------------------------------------------------------------------------


def _column_reader(path):
    return _ColumnReader(str(path), run_id="A", run_started=RUN_STARTED, store_kwargs={})


class TestStreamedFold:
    def _relay_rows(self, tmp_path):
        """The order-1 stage columns of the wide store: a 256-cell relay each."""
        root = tmp_path / "s"
        manifest = _wide_store(root)
        sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
        )
        rows: list = [None] * 4
        rows[0] = [_column_reader(root / "1" / "1" / "all.pyramid.zarr")]
        return rows

    def test_a_streamed_merge_equals_the_whole_level_merge(self, tmp_path):
        # A multi-chunk level: 4 output cells from a 256-cell relay member,
        # folded 64-to-one, in 16-cell blocks — against the same fold in one.
        rows = self._relay_rows(tmp_path)
        geometry = dict(res_src=5, src_per_child=256, factor=64, n_out=16)
        whole_meter, block_meter = FoldMeter(), FoldMeter()
        whole = _merge_slabs(rows, FIELDS, block=4**10, meter=whole_meter, **geometry)
        streamed = _merge_slabs(rows, FIELDS, block=16, meter=block_meter, **geometry)
        assert streamed[1:] == whole[1:]
        for name in whole[0]:
            assert _same(streamed[0][name], whole[0][name]), name
        assert whole[0]["count"].sum() > 0
        # The whole-level fold holds the level's inputs at once: the child's
        # 256 relay cells, once per field. The streamed one never holds more
        # than ONE output cell's — the 64 sources its flat k-way law needs.
        assert (whole_meter.blocks, whole_meter.peak_cells) == (1, 2 * 256)
        assert (block_meter.blocks, block_meter.peak_cells) == (16, 2 * 64)
        assert block_meter.cells_read == whole_meter.cells_read == 2 * 256

    def test_a_merge_block_is_never_more_than_one_fold_block(self, tmp_path):
        # Where a fold block covers several output cells (factor < block),
        # the bound is the block itself.
        rows = self._relay_rows(tmp_path)
        geometry = dict(res_src=5, src_per_child=256, factor=4, n_out=256)
        whole_meter, block_meter = FoldMeter(), FoldMeter()
        whole = _merge_slabs(rows, FIELDS, block=4**10, meter=whole_meter, **geometry)
        streamed = _merge_slabs(rows, FIELDS, block=64, meter=block_meter, **geometry)
        for name in whole[0]:
            assert _same(streamed[0][name], whole[0][name]), name
        assert block_meter.peak_cells == 2 * 64 < whole_meter.peak_cells == 2 * 256
        assert block_meter.blocks == 16  # 256 output cells, 16 (= 64 sources) per block

    def test_a_streamed_gather_equals_the_whole_level_gather(self, tmp_path):
        rows = self._relay_rows(tmp_path)
        geometry = dict(res=5, span=256, n_out=1024)
        whole_meter, block_meter = FoldMeter(), FoldMeter()
        whole = _gather_slabs(rows, FIELDS, block=4**10, meter=whole_meter, **geometry)
        streamed = _gather_slabs(rows, FIELDS, block=16, meter=block_meter, **geometry)
        assert streamed[1:] == whole[1:] == (1, 0, 0, [])
        for name in whole[0]:
            assert _same(streamed[0][name], whole[0][name]), name
        assert whole_meter.peak_cells == 2 * 256
        assert block_meter.peak_cells == 2 * 16 and block_meter.blocks == 64

    @pytest.mark.parametrize("fields", (FIELDS, BOTH_CHANNEL_FIELDS), ids=("plain", "channels"))
    def test_the_ladder_is_the_same_at_any_block_size(self, tmp_path, monkeypatch, fields):
        # The whole sweep, streamed in 4-cell blocks against one block per
        # level: every ladder array, every content hash, every stage-column
        # value and every O11 record agree.
        builds = {}
        for order in (5, 1):
            monkeypatch.setattr(fold_mod, "STAGE_BLOCK_ORDER", order)
            root = tmp_path / f"b{order}"
            manifest = _stage_store(root, fields=fields)
            summary = sweep_stage_pass(
                str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
            )
            assert all(row["failed"] == 0 for row in summary["stages"])
            builds[order] = (root, summary)
        whole_root, whole = builds[5]
        block_root, streamed = builds[1]
        names = [n for n in _overviews(whole_root, "all.zarr")]
        assert names == _overviews(block_root, "all.zarr") and len(names) == 7
        for rel in names:
            a, b = _artifact(whole_root, rel), _artifact(block_root, rel)
            block_a, block_b = dict(a.attrs)["zagg_overview"], dict(b.attrs)["zagg_overview"]
            assert block_a["content_hash"] == block_b["content_hash"], rel
            r = str(block_a["cell_order"])
            assert sorted(a[r].array_keys()) == sorted(b[r].array_keys())
            for name in a[r].array_keys():
                assert _same(a[r][name][:], b[r][name][:]), (rel, name)
        # The streamed build held less at once, in more blocks.
        for row_whole, row_block in zip(whole["stages"], streamed["stages"], strict=True):
            assert row_block["fold_cells_read"] == row_whole["fold_cells_read"]
            assert row_block["fold_blocks"] >= row_whole["fold_blocks"]
            assert row_block["fold_peak_cells"] <= row_whole["fold_peak_cells"]
        assert streamed["stages"][1]["fold_peak_cells"] < whole["stages"][1]["fold_peak_cells"]
        columns = [n for n in _overviews(whole_root, "all.pyramid.zarr") if n.count("/") < 4]
        assert len(columns) == 5  # the stage columns: orders 2 and 1
        for rel in columns:
            a, b = _artifact(whole_root, rel), _artifact(block_root, rel)
            stamp_a = dict(a.attrs)["morton_hive_commit"]
            stamp_b = dict(b.attrs)["morton_hive_commit"]
            assert stamp_a["content_hashes"] == stamp_b["content_hashes"], rel
            assert stamp_a["cells_with_data"] == stamp_b["cells_with_data"]
            for key in ("groups", "source_children"):
                assert dict(a.attrs)["zagg_column"][key] == dict(b.attrs)["zagg_column"][key]

    def test_a_wide_column_group_is_one_chunk_object_per_block(self, tmp_path, monkeypatch):
        from zagg.content_hash import content_hashes_record, hash_arrays

        monkeypatch.setattr(fold_mod, "STAGE_BLOCK_ORDER", 1)
        root = tmp_path / "s"
        manifest = _wide_store(root)
        sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
        )
        column = root / "1" / "1" / "all.pyramid.zarr"
        group = _artifact(root, "1/1/all.pyramid.zarr")
        relay = group["5"]["h_tdigest"]
        assert relay.shape == (256,) and relay.chunks == (4,)
        assert "sharding_indexed" not in (column / "5" / "h_tdigest" / "zarr.json").read_text()
        # 64 chunks; only the populated ones are objects (three leaves' worth).
        chunks = [p for p in (column / "5" / "count" / "c").iterdir()]
        assert 1 < len(chunks) <= 64
        # The record accumulated across blocks is the record of the artifact.
        stamp = dict(group.attrs)["morton_hive_commit"]
        assert stamp["content_hashes"] == content_hashes_record(hash_arrays(group))
        # A parent reads it back a block at a time: one chunk object per read.
        reader = _column_reader(column)
        whole = reader.read(5, "count")
        assert np.array_equal(reader.read_range(5, "count", 8, 12), whole[8:12])
        assert reader.has(5, "count") and not reader.has(5, "absent")

    def test_a_narrow_column_group_is_laid_out_as_before(self, tmp_path):
        # At the default block a group no wider than one block is ONE chunk —
        # the layout every stage column had, so no existing store moves.
        root = tmp_path / "s"
        manifest = _wide_store(root)
        sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
        )
        relay = _artifact(root, "1/1/all.pyramid.zarr")["5"]["h_tdigest"]
        assert relay.shape == relay.chunks == (256,)

    def test_a_column_whose_relay_folds_nothing_is_never_started(self, tmp_path):
        from zagg.sweep_stage import write_stage_column

        root = tmp_path / "s"
        _wide_store(root)
        written = write_stage_column(
            str(root),
            "11",
            [[None], None, None, None],
            FIELDS,
            members=[5],
            child_order=2,
            node_order=1,
            relay=5,
            cell_order=6,
            generation={},
        )
        assert written is None and not (root / "1" / "1" / "all.pyramid.zarr").exists()

    def _swept_column(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fold_mod, "STAGE_BLOCK_ORDER", 1)
        root = tmp_path / "s"
        manifest = _wide_store(root)
        sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
        )
        return root / "1" / "1" / "all.pyramid.zarr"

    def test_a_block_read_revalidates_the_stamp(self, tmp_path, monkeypatch):
        column = self._swept_column(tmp_path, monkeypatch)
        reader = _column_reader(column)
        # Before anything is served, a moved stamp is re-read under the new one.
        _restamp(column, "2031-01-01T00:00:00+00:00")
        first = reader.read_range(5, "count", 0, 4)
        assert first is not None and reader.revalidated == 1
        assert reader.stamp["written_at"] == "2031-01-01T00:00:00+00:00"
        # Once a block has been served, a rewrite between two block reads is a
        # torn member: the next block raises rather than hand over the new write.
        _restamp(column, "2032-01-01T00:00:00+00:00")
        with pytest.raises(ColumnMovedError, match="rewritten after this fold read"):
            reader.read_range(5, "count", 4, 8)
        assert reader.revalidated == 1

    def test_a_vanished_stamp_never_validates(self, tmp_path, monkeypatch):
        # A rewrite in flight has no stamp (the template is re-emitted first):
        # its bytes are never data, before or after a served read.
        column = self._swept_column(tmp_path, monkeypatch)
        served = _column_reader(column)
        assert served.read_range(5, "count", 0, 4) is not None
        fresh = _column_reader(column)
        _restamp(column, None)
        with pytest.raises(ColumnMovedError, match="stamp gone"):
            served.read_range(5, "count", 4, 8)
        with pytest.raises(ColumnMovedError, match="kept moving"):
            fresh.read_range(5, "count", 0, 4)

    def test_a_torn_overview_is_folded_again_from_fresh_readers(
        self, tmp_path, monkeypatch, caplog
    ):
        # A rewrite lands under a fold after its first block read: the artifact
        # is folded again from scratch and lands with the clean build's content.
        clean = self._swept_column(tmp_path / "clean", monkeypatch).parents[2]
        root = tmp_path / "torn" / "s"
        manifest = _wide_store(root)
        moved = _move_after_first_read(monkeypatch, root, times=1)
        with caplog.at_level("INFO", logger="zagg.sweep_fold"):
            summary = sweep_stage_pass(
                str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
            )
        assert moved == [True] and "folded again from fresh readers" in caplog.text
        assert all(row["failed"] == 0 for row in summary["stages"])
        for rel in _overviews(clean, "all.zarr"):
            a, b = _artifact(clean, rel), _artifact(root, rel)
            assert (
                dict(a.attrs)["zagg_overview"]["content_hash"]
                == dict(b.attrs)["zagg_overview"]["content_hash"]
            ), rel

    def test_an_artifact_torn_twice_fails_and_the_unit_goes_on(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fold_mod, "STAGE_BLOCK_ORDER", 1)
        root = tmp_path / "s"
        manifest = _wide_store(root)
        moved = _move_after_first_read(monkeypatch, root, times=2)
        summary = sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", tuple_width=1
        )
        assert moved == [True, True]
        assert sum(row["failed"] for row in summary["stages"]) == 1
        assert sum(row["written"] for row in summary["stages"]) > 0

    def test_a_torn_all_time_fold_is_folded_again(self, tmp_path, monkeypatch):
        # The close reads the node's window overviews the same way: a source
        # that moves under it costs one refold from fresh readers.
        real, closes = fold_mod.merge_level, []

        def merge_level(rows, *args, **kwargs):
            if kwargs.get("factor") == 1 and len(rows) == 1:  # the all-time fold
                closes.append(list(rows[0]))
                if len(closes) == 1:
                    raise ColumnMovedError("a window overview was rewritten")
            return real(rows, *args, **kwargs)

        monkeypatch.setattr(fold_mod, "merge_level", merge_level)
        root = tmp_path / "s"
        manifest = _windowed_store(root)
        summary = sweep_stage_pass(str(root), manifest, _by_shard(), run_id="A")
        assert all(row["failed"] == 0 for row in summary["stages"])
        assert not set(map(id, closes[0])) & set(map(id, closes[1]))  # fresh readers
        assert len(_overviews(root, "all.zarr")) == 7

    @pytest.mark.parametrize("failures", (1, 2))
    def test_a_read_failure_mid_column_is_retried_once(self, tmp_path, monkeypatch, failures):
        # The column is cleared before its streamed reads: one failed read
        # costs a retry from fresh readers, not the node's column. Two in a
        # row are counted, and leave the column cleared until a later pass.
        import zagg.sweep_stage as stage_mod

        self._swept_column(tmp_path, monkeypatch)
        root = tmp_path / "s"
        manifest = json.loads((root / MANIFEST_NAME).read_text())
        column = root / "-2" / "1" / "1" / "all.pyramid.zarr"  # node -211's, the first written
        assert dict(_artifact(root, "-2/1/1/all.pyramid.zarr").attrs)["morton_hive_commit"]
        monkeypatch.setattr(stage_mod, "_stage_column_current", lambda *a, **k: False)
        real_write, real_fetch = stage_mod.write_stage_column, fold_mod._fetch
        state = {"in": False, "n": 0, "failed": 0}

        def write(*args, **kwargs):
            state["in"], state["n"] = True, 0
            try:
                return real_write(*args, **kwargs)
            finally:
                state["in"] = False

        def fetch(*args, **kwargs):
            if state["in"] and state["failed"] < failures:
                state["n"] += 1
                if state["n"] == 2:
                    state["failed"] += 1
                    raise OSError("simulated transient read failure")
            return real_fetch(*args, **kwargs)

        monkeypatch.setattr(stage_mod, "write_stage_column", write)
        monkeypatch.setattr(fold_mod, "_fetch", fetch)
        summary = sweep_stage_pass(
            str(root), manifest, {d: {None} for d in LEAVES}, run_id="B", tuple_width=1
        )
        assert state["failed"] == failures
        assert sum(row["failed"] for row in summary["stages"]) == failures - 1
        stamp = dict(zarr.open_group(open_store(str(column)), mode="r").attrs).get(
            "morton_hive_commit"
        )
        assert (stamp is not None) is (failures == 1)

    def test_a_foreign_sweep_is_never_refolded(self):
        calls = []

        def fold():
            raise ForeignSweepError("two sweeps")

        with pytest.raises(ForeignSweepError):
            refold_on_move(fold, lambda: calls.append(1), "x", retry_on=(Exception,))
        assert calls == []


def _restamp(column, written_at):
    """Rewrite a column's commit stamp in place (``None`` removes it)."""
    group = zarr.open_group(open_store(str(column)), path="", mode="r+", zarr_format=3)
    stamp = dict(group.attrs["morton_hive_commit"])
    if written_at is None:
        attrs = dict(group.attrs)
        attrs.pop("morton_hive_commit")
        group.attrs.clear()
        group.attrs.update(attrs)
    else:
        group.attrs["morton_hive_commit"] = {**stamp, "written_at": written_at}


def _move_after_first_read(monkeypatch, root, *, times):
    """Restamp the source an overview fold read first, ``times`` folds in a row.

    Hooks the first overview fold of the pass (``_stage_fold``): its first
    served read is followed by a rewrite of that source, so the fold's next
    read of it is torn. Returns the list it appends to per move.
    """
    import zagg.sweep_stage as stage_mod

    moved: list = []
    state = {"fold": 0, "armed": False}
    real_fold, real_fetch = stage_mod._stage_fold, fold_mod._fetch

    def stage_fold(*args, **kwargs):
        state["fold"] += 1
        state["armed"] = state["fold"] <= times
        try:
            return real_fold(*args, **kwargs)
        finally:
            state["armed"] = False

    def fetch(reader, *args, **kwargs):
        values = real_fetch(reader, *args, **kwargs)
        if state["armed"]:
            state["armed"] = False
            _restamp(root / reader.path, f"203{len(moved) + 1}-01-01T00:00:00+00:00")
            moved.append(True)
        return values

    monkeypatch.setattr(stage_mod, "_stage_fold", stage_fold)
    monkeypatch.setattr(fold_mod, "_fetch", fetch)
    return moved


# ---------------------------------------------------------------------------
# The fleet: events, barriers, the handler.
# ---------------------------------------------------------------------------


def _windowed_fleet(root, client, **kwargs):
    from zagg.sweep_fleet import run_stage_sweep_fleet

    kwargs.setdefault("windowed", True)
    kwargs.setdefault("all_time", True)
    return run_stage_sweep_fleet(
        client,
        "zagg-worker",
        str(root),
        _refs(),
        shard_order=3,
        store_kwargs={},
        poll_interval_s=0.01,
        **kwargs,
    )


class TestFleetUnits:
    def test_one_event_per_window_per_node_then_one_close_per_node(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        client = _FakeLambda(mod.lambda_handler)
        summary = _windowed_fleet(root, client)
        blocks = client.blocks()
        assert [(b.get("unit"), b.get("window"), b.get("nodes")) for b in blocks] == [
            ("window", "2019", ["-2"]),
            ("window", "2019", ["1"]),
            ("window", "2020", ["-2"]),
            ("window", "2020", ["1"]),
            ("close", None, ["-2"]),
            ("close", None, ["1"]),
            (None, None, None),
        ]
        assert blocks[-1]["role"] == "finisher"
        # Every unit names its own record: batches run on across the windows
        # and into the close.
        assert [b.get("batch") for b in blocks[:-1]] == [0, 1, 2, 3, 4, 5]
        # A window event carries its own window's refs alone; the close, the
        # node's whole slice.
        window_event, close_event = client.events[1], client.events[5]
        assert window_event["leaves"] == [[morton_word(d), "2019"] for d in LEAVES[:3]]
        assert len(close_event["leaves"]) == 6
        assert len(json.dumps(window_event)) < len(json.dumps(close_event))
        (row,) = summary["stages"]
        assert (row["nodes"], row["batches"], row["records_seen"]) == (2, 4, 4)
        assert (row["close_batches"], row["close_records_seen"]) == (2, 2)
        assert not row["barrier_timed_out"] and "missing_units" not in row
        assert summary["invokes"] == 7 and summary["finisher"]["landed"]
        assert summary["windowed"] and summary["all_time"]

    def test_a_nodes_barrier_counts_its_window_units(self, tmp_path, monkeypatch):
        # The window barrier waits for one record per (node, window); the
        # close units are not fired until it is met.
        import zagg.sweep_fleet as fleet_mod

        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        client = _FakeLambda(mod.lambda_handler)
        waited = []
        real = fleet_mod.await_records

        def spy(records_from, expected, **kwargs):
            waited.append((sorted(expected), len(client.events)))
            return real(records_from, expected, **kwargs)

        monkeypatch.setattr(fleet_mod, "await_records", spy)
        _windowed_fleet(root, client)
        names = [stage_record_name(0, b) for b in range(6)]
        assert (
            waited
            == [
                (names[:4], 4),  # 2 nodes x 2 windows, before any close is fired
                (names[4:], 6),  # then the 2 closes
                ([FINISHER_RECORD_NAME], 7),
            ]
        )

    def test_the_close_rides_the_next_tuples_barrier(self, tmp_path, monkeypatch):
        # A coarser tuple needs only the window units' stage columns, so it is
        # fired with this tuple's closes and they share one barrier.
        import zagg.sweep_fleet as fleet_mod

        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        client = _FakeLambda(mod.lambda_handler)
        waited = []
        real = fleet_mod.await_records

        def spy(records_from, expected, **kwargs):
            waited.append(sorted(expected))
            return real(records_from, expected, **kwargs)

        monkeypatch.setattr(fleet_mod, "await_records", spy)
        summary = _windowed_fleet(root, client, tuple_width=1)
        assert not summary["barrier_timed_out"]
        # tuple 2: 3 nodes x 2 windows; tuple 1: its 4 window units WITH
        # tuple 2's 3 closes; tuple 0 likewise; then tuple 0's own closes.
        assert [len(names) for names in waited] == [6, 4 + 3, 4 + 2, 2, 1]
        assert [s["close_records_seen"] for s in summary["stages"]] == [3, 2, 2]

    def test_no_close_and_no_barrier_without_the_declaration(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root, all_time=False)
        client = _FakeLambda(mod.lambda_handler)
        summary = _windowed_fleet(root, client, all_time=False)
        assert [b.get("unit") for b in client.blocks()] == ["window"] * 4 + [None]
        (row,) = summary["stages"]
        assert "close_batches" not in row and summary["invokes"] == 5
        assert not _overviews(root, "all.zarr") and len(_overviews(root, "2020.zarr")) == 7

    @pytest.mark.parametrize("store_all_time", (True, False), ids=("declared", "undeclared"))
    def test_the_close_follows_the_store_not_the_config(
        self, tmp_path, monkeypatch, store_all_time
    ):
        # ``pyramid`` is not a frozen manifest key, so a run's config can
        # disagree with the store; the workers decide from the manifest and the
        # dispatcher follows their records' ``closes`` — in either direction.
        import zagg.sweep_fleet as fleet_mod

        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root, all_time=store_all_time)
        client = _FakeLambda(mod.lambda_handler)
        waited = []
        real = fleet_mod.await_records

        def spy(records_from, expected, **kwargs):
            waited.append(len(expected))
            return real(records_from, expected, **kwargs)

        monkeypatch.setattr(fleet_mod, "await_records", spy)
        summary = _windowed_fleet(root, client, all_time=not store_all_time)
        assert (summary["all_time"], summary["all_time_from"]) == (store_all_time, "store")
        assert _stage_record(root, 0)["closes"] is store_all_time
        units = [b.get("unit") for b in client.blocks()]
        if store_all_time:
            assert units == ["window"] * 4 + ["close"] * 2 + [None]
            assert len(_overviews(root, "all.zarr")) == 7  # what the CLI writes
        else:
            # No close unit and no close barrier: the window barrier, the finisher.
            assert units == ["window"] * 4 + [None] and waited == [4, 1]
            assert not _overviews(root, "all.zarr")

    def test_without_a_record_the_callers_guess_stands(self, tmp_path):
        root = tmp_path / "s"
        _windowed_store(root, all_time=False)
        client = _FakeLambda(None)  # no worker runs, so no record ever lands
        summary = _windowed_fleet(root, client, barrier_timeout_s=0.01)
        assert [b.get("unit") for b in client.blocks()] == ["window"] * 4 + ["close"] * 2 + [None]
        assert (summary["all_time"], summary["all_time_from"]) == (True, "caller")

    def test_a_dead_window_unit_is_named_and_costs_only_its_window(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        # Node '1', window '2020': the invoke is lost, so no record lands.
        client = _FakeLambda(mod.lambda_handler, drop={stage_record_name(0, 3)})
        summary = _windowed_fleet(root, client, barrier_timeout_s=0.05)
        (row,) = summary["stages"]
        assert row["barrier_timed_out"] and (row["batches"], row["records_seen"]) == (4, 3)
        assert row["missing_unit_count"] == 1
        assert row["missing_units"] == [
            {"batch": 3, "nodes": ["1"], "unit": "window", "window": "2020"}
        ]
        # It propagates: the run's summary, the finisher's event, its record.
        assert summary["barrier_timed_out"] is True
        assert client.blocks()[-1]["barrier_timed_out"] is True
        record = json.loads(
            (root.parent / "s.status").rglob(FINISHER_RECORD_NAME).__next__().read_text()
        )
        assert record["barrier_timed_out"] is True
        # Only that window's overview is missing; the close recorded the gap.
        assert (root / "1" / "2019.zarr").exists() and not (root / "1" / "2020.zarr").exists()
        assert (root / "-2" / "2020.zarr").exists()
        block, _ = _overview(root, "1/all.zarr")
        assert block["source_windows"] == {"folded": 1, "missing": 1, "unreadable": 0}
        close_record = _stage_record(root, 5)
        assert close_record["unit"] == "close" and close_record["stages"][0]["under_covered"] >= 1

    def test_a_failed_window_unit_still_writes_its_record(self, tmp_path, monkeypatch):
        # A unit that RAISES is not a unit that died: its record lands, with
        # the failure in it, so the barrier is met at once instead of waited out.
        real = stages_mod.stage_node

        def stage_node(*args, **kwargs):
            if kwargs["window"] == "2020" and args[2] == "1":
                raise RuntimeError("PUT failed")
            return real(*args, **kwargs)

        monkeypatch.setattr(stages_mod, "stage_node", stage_node)
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        client = _FakeLambda(mod.lambda_handler)
        summary = _windowed_fleet(root, client)
        assert not summary["barrier_timed_out"]
        failed = _stage_record(root, 3)
        assert (failed["unit"], failed["window"]) == ("window", "2020")
        assert failed["stages"][0]["unit_errors"] == [
            {"node": "1", "unit": "2020", "error": "RuntimeError: PUT failed"}
        ]
        body = json.loads(client.responses[3]["body"])
        assert body["ok"] and body["failed"] == 1 and body["unit"] == "window"

    def test_an_unwindowed_event_is_what_it_was(self, tmp_path):
        from zagg.sweep_fleet import run_stage_sweep_fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        run_stage_sweep_fleet(
            client,
            "fn",
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            shard_order=3,
            store_kwargs={},
            poll_interval_s=0.01,
            barrier_timeout_s=0.01,
        )
        stage_block = client.blocks()[0]
        # No `unit`/`window` on an unwindowed event — the claim this pins.
        # `child_order` joined the block in issue #610: the tuple's span, which
        # every stage event carries now that the schedule can be sized.
        assert sorted(stage_block) == [
            "batch",
            "child_order",
            "dispatch",
            "nodes",
            "records_from",
            "role",
            "run_id",
            "run_started",
            "tuple_width",
        ]

    def test_a_windowed_fleet_build_is_byte_identical_to_the_cli_build(self, tmp_path):
        # The (node, window) fan-out against the in-process pass, on the same
        # pre-sweep bytes: every object, the all-time folds included.
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        _write_discovery_record(root, windows=WINDOWS)
        base = _snapshot(root)
        _cli_sweep(root, tuple_width=1)
        cli = _snapshot(root)
        _restore(root, base)
        summary = _windowed_fleet(root, _FakeLambda(mod.lambda_handler), tuple_width=1)
        assert summary["finisher"]["landed"] and not summary["barrier_timed_out"]
        _assert_identical(cli, _snapshot(root))
        assert sum(rel.endswith("/all.zarr/zarr.json") for rel in cli) == 7


def _stage_record(root, batch, dispatch=0):
    (path,) = (root.parent / f"{root.name}.status").rglob(stage_record_name(dispatch, batch))
    return json.loads(path.read_text())


class TestHandlerUnits:
    def _event(self, root, **block):
        return {
            "mode": "sweep",
            "store_path": str(root),
            "leaves": [[key, window] for key, window in _refs()],
            "stage": {
                "role": "stage",
                "run_id": "F",
                "run_started": RUN_STARTED,
                "dispatch": 0,
                "nodes": ["1"],
                "batch": 0,
                "tuple_width": 3,
                "records_from": _records(root),
                **block,
            },
        }

    def test_a_window_unit_folds_one_window(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        response = mod.lambda_handler(self._event(root, unit="window", window="2019"), None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["ok"] and (body["unit"], body["window"]) == ("window", "2019")
        assert body["written"] == 4 and body["failed"] == 0
        assert (root / "1" / "2019.zarr").exists()
        assert not (root / "1" / "2020.zarr").exists() and not (root / "1" / "all.zarr").exists()
        record = _stage_record(root, 0)
        assert (record["unit"], record["window"]) == ("window", "2019")
        assert record["stages"][0]["window_units"] == 1

    def test_a_close_unit_folds_the_all_time_overviews(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        for batch, window in enumerate(WINDOWS):
            event = self._event(root, unit="window", window=window, batch=batch)
            assert mod.lambda_handler(event, None)["statusCode"] == 200
        response = mod.lambda_handler(self._event(root, unit="close", batch=2), None)
        body = json.loads(response["body"])
        assert body["ok"] and body["unit"] == "close" and body["written"] == 4
        block, _ = _overview(root, "1/all.zarr")
        assert block["source_windows"] == {"folded": 2, "missing": 0, "unreadable": 0}
        assert _stage_record(root, 2)["stages"][0]["close_units"] == 1

    def test_an_event_without_a_unit_runs_the_node_whole(self, tmp_path):
        # What a dispatcher predating the units sends: every window, then the
        # close, in one invoke — the same bytes.
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        body = json.loads(mod.lambda_handler(self._event(root), None)["body"])
        assert body["ok"] and body["unit"] is None and body["written"] == 12
        row = _stage_record(root, 0)["stages"][0]
        assert (row["window_units"], row["close_units"]) == (2, 1)

    @pytest.mark.parametrize(
        "block,match",
        (
            ({"unit": "window"}, "names unit 'window' with window None"),
            ({"window": "2019"}, "names unit None with window '2019'"),
            ({"unit": "close", "window": "2019"}, "names unit 'close' with window '2019'"),
            ({"unit": "node"}, "unknown stage unit"),
        ),
    )
    def test_a_malformed_unit_refuses_by_name(self, tmp_path, block, match):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        response = mod.lambda_handler(self._event(root, **block), None)
        assert response["statusCode"] == 500
        assert match in json.loads(response["body"])["error"]
        assert not _overviews(root, "2019.zarr")


# ---------------------------------------------------------------------------
# The pipeline run id (issue #593, the producer half).
# ---------------------------------------------------------------------------


def _root_record(root):
    (path,) = sorted(root.glob("sweep_stats_*_stages.json"))
    return json.loads(path.read_text())


class TestPipelineRunId:
    def test_the_fleet_records_carry_it(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _windowed_store(root)
        client = _FakeLambda(mod.lambda_handler)
        summary = _windowed_fleet(root, client, pipeline_run_id="pipeline-1")
        assert summary["pipeline_run_id"] == "pipeline-1"
        assert all(b["pipeline_run_id"] == "pipeline-1" for b in client.blocks())
        # Beside the sweep's own id, never in place of it.
        record = _root_record(root)
        assert record["pipeline_run_id"] == "pipeline-1"
        assert record["run_id"] == summary["run_id"] != "pipeline-1"
        for batch in range(6):
            stage = _stage_record(root, batch)
            assert (stage["pipeline_run_id"], stage["run_id"]) == ("pipeline-1", summary["run_id"])
        (finisher,) = (root.parent / "s.status").rglob(FINISHER_RECORD_NAME)
        assert json.loads(finisher.read_text())["pipeline_run_id"] == "pipeline-1"

    def test_an_unnamed_fleet_sweep_records_null_and_sends_no_key(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(mod.lambda_handler)
        from zagg.sweep_fleet import run_stage_sweep_fleet

        run_stage_sweep_fleet(
            client,
            "fn",
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            shard_order=3,
            store_kwargs={},
            poll_interval_s=0.01,
        )
        assert not any("pipeline_run_id" in b for b in client.blocks())
        record = _root_record(root)
        assert "pipeline_run_id" in record and record["pipeline_run_id"] is None
        assert _stage_record(root, 0)["pipeline_run_id"] is None

    def test_the_local_chain_records_it(self, tmp_path):
        from zagg.sweep_stages import stage_sweep_after_run

        root = tmp_path / "s"
        _stage_store(root)
        summary = stage_sweep_after_run(
            str(root), [(morton_word(d), None) for d in LEAVES], pipeline_run_id="pipeline-2"
        )
        assert summary["pipeline_run_id"] == "pipeline-2"
        assert _root_record(root)["pipeline_run_id"] == "pipeline-2"
        assert _root_record(root)["run_id"].startswith("stage-")

    def test_the_local_backend_stamps_its_run_id_end_to_end(self, tmp_path, monkeypatch):
        # A real local run: windowed leaves from the bulk emit (no stored
        # ``morton``), their leaf columns, then the chained staged sweep —
        # window units, the close, the finisher — whose run record names the
        # pipeline run that wrote the leaves.
        import test_windowed_emit as emit

        from zagg import runner
        from zagg.config import validate_config

        cfg = emit._digest_cfg()
        cfg.output["sweep"] = "stages"
        cfg.output["pyramid"] = {"overviews": 7, "all_time": True}
        validate_config(cfg)
        catalog_path, _shard = emit._catalog(tmp_path)
        root = tmp_path / "out"
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        emit._patch(monkeypatch)
        summary = runner.agg(cfg, catalog=catalog_path, store=str(root), backend="local")
        record = _root_record(root)
        assert record["pipeline_run_id"] == summary["results"][0]["stats"][0]["run_id"]
        assert record["run_id"].startswith("stage-") and record["lease"]["released"]
        rows = record["stages"]
        assert [(r["window_units"], r["close_units"], r["failed"]) for r in rows] == [
            (3, 1, 0),
            (3, 1, 0),
        ]
        # Three windows and their all-time fold at every ladder node.
        for name in ("2018.zarr", "2019.zarr", "2020.zarr", "all.zarr"):
            assert len(_overviews(root, name)) == 6, name
        block = dict(_artifact(root, "-5/all.zarr").attrs)["zagg_overview"]
        assert block["source_windows"] == {"folded": 3, "missing": 0, "unreadable": 0}

    def test_the_lambda_tail_passes_its_run_id(self):
        import inspect

        from zagg import runner

        tail = inspect.getsource(runner._run_lambda)
        assert "pipeline_run_id=run_id" in tail[tail.index("_invoke_lambda_stage_sweep(") :]

    def test_a_standalone_pass_names_the_run_it_completes_or_none(self, tmp_path, capsys):
        from zagg.sweep import main

        root = tmp_path / "s"
        _stage_store(root)
        _write_discovery_record(root)
        base = _snapshot(root)
        assert main([str(root), "--stages"]) == 0
        assert _root_record(root)["pipeline_run_id"] is None  # vouches for no run
        _restore(root, base)
        assert main([str(root), "--stages", "--pipeline-run-id", "pipeline-3"]) == 0
        assert _root_record(root)["pipeline_run_id"] == "pipeline-3"
        capsys.readouterr()
        with pytest.raises(SystemExit):
            main([str(root), "--pipeline-run-id", "pipeline-3"])
        assert "only applies to --stages" in capsys.readouterr().err

    def test_the_handler_reads_the_event_key(self, tmp_path, monkeypatch):
        seen = {}
        monkeypatch.setattr(
            stages_mod, "run_stage_worker", lambda *a, **k: seen.update(k) or {"stages": []}
        )
        monkeypatch.setattr(
            stages_mod, "run_stage_finisher", lambda *a, **k: seen.update(k) or {"stages": []}
        )
        mod = _handler_module()
        for role in ("stage", "finisher"):
            for named in ("pipeline-4", None):
                block = {
                    "role": role,
                    "run_id": "F",
                    "run_started": RUN_STARTED,
                    "dispatch": 0,
                    "nodes": ["1"],
                    "records_from": str(tmp_path / "p"),
                }
                if named is not None:
                    block["pipeline_run_id"] = named
                event = {"mode": "sweep", "store_path": str(tmp_path / "s"), "leaves": []}
                assert mod.lambda_handler({**event, "stage": block}, None)["statusCode"] == 200
                assert seen.pop("pipeline_run_id", "absent") == named
                seen.clear()


# ---------------------------------------------------------------------------
# The staged tail's tier (the issue #586 interim).
# ---------------------------------------------------------------------------


class TestStageTier:
    @pytest.mark.parametrize(
        "run_function,stage_function",
        (
            ("process-shard", "process-shard-8192-disk"),
            ("process-shard-4096-disk", "process-shard-8192-disk"),
            ("process-shard-2048", "process-shard-8192-disk"),
            ("process-shard-8192-disk", "process-shard-8192-disk"),
            ("process-shard-test-4096-disk", "process-shard-test-8192-disk"),
            ("zagg-dev", "zagg-dev-8192-disk"),
        ),
    )
    def test_stage_invokes_go_to_the_8192_variant_of_the_run_family(
        self, monkeypatch, run_function, stage_function
    ):
        from zagg import runner

        monkeypatch.delenv("ZAGG_LAMBDA_STAGE_FUNCTION_NAME", raising=False)
        assert runner._resolve_stage_function_name(run_function) == stage_function

    def test_the_env_override_wins(self, monkeypatch):
        from zagg import runner

        monkeypatch.setenv("ZAGG_LAMBDA_STAGE_FUNCTION_NAME", "my-stage-fn")
        assert runner._resolve_stage_function_name("process-shard-4096-disk") == "my-stage-fn"

    def test_the_tier_is_a_provisioned_variant(self):
        from zagg import runner
        from zagg.config import WORKER_MEMORIES

        assert runner.STAGE_SWEEP_WORKER == {"memory": 8192, "extra_disk": True}
        assert runner.STAGE_SWEEP_WORKER["memory"] in WORKER_MEMORIES

    def test_the_tail_dispatches_the_staged_sweep_there(self, tmp_path, monkeypatch):
        import inspect

        from zagg import runner

        tail = inspect.getsource(runner._run_lambda)
        call = tail[tail.index("_invoke_lambda_stage_sweep(") :]
        assert "_resolve_stage_function_name(function_name)" in call[: call.index("store_path")]
        # And the seam forwards the declaration and the name it was handed.
        seen = {}

        def fake(client, function_name, store_path, leaves, **kwargs):
            seen.update(kwargs, function_name=function_name)
            return {"run_id": "r", "invokes": 0, "stages": [], "finisher": {"landed": True}}

        import zagg.sweep_fleet

        monkeypatch.setattr(zagg.sweep_fleet, "run_stage_sweep_fleet", fake)
        runner._invoke_lambda_stage_sweep(
            None,
            "process-shard-8192-disk",
            str(tmp_path),
            [],
            shard_order=3,
            windowed=True,
            all_time=True,
            pipeline_run_id="pipeline-5",
        )
        assert seen["function_name"] == "process-shard-8192-disk"
        assert (seen["windowed"], seen["all_time"]) == (True, True)
        assert seen["pipeline_run_id"] == "pipeline-5"
