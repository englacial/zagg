"""The ladder tail's per-tuple sizing and its short-order withholding (issue #610).

Standing claims:

- a dispatch node folds its WHOLE subtree down to the tuple's child order
  inside one invoke, so a fixed ``tuple_width`` puts ``1 + 4 + ... +
  4**(width-1)`` nodes on one worker wherever the store is dense — the 21 that
  walled the v3 ladder's coarsest tuple at 900 s;
- :func:`zagg.sweep_partition.sized_stage_tuples` narrows each tuple to the
  widest width whose fattest dispatch node stays within the target, and leaves
  a sparse ladder on the fixed-width schedule outright;
- the grouping changes no bytes: a sized fleet build's ladder overviews are the
  fixed-width build's (``TestSizedByteIdentity``, the merge-source law);
- a tuple missing a unit record names its ORDERS to the finisher, which then
  leaves their manifest actuals as they stood rather than stamping them from a
  run that did not observe them.
"""

from __future__ import annotations

import itertools
import json

import pytest
from test_sweep_stage import _stage_store

from zagg.sweep_partition import STAGE_TARGET_NODES, sized_stage_tuples
from zagg.sweep_stage import one_stage_tuple, stage_tuples


def _dense(base="3"):
    """``nodes_at`` for one fully dense base cell: ``4**order`` nodes."""

    def nodes_at(order):
        return [base + "".join(d) for d in itertools.product("0123", repeat=int(order))]

    return nodes_at


def _from(nodes):
    """``nodes_at`` for an explicit leaf set, by morton-decimal truncation."""
    from zagg.sweep_overview import _node_at

    def nodes_at(order):
        return sorted({_node_at(d, int(order)) for d in nodes})

    return nodes_at


def _spans(schedule):
    return [(int(st["dispatch"]), int(st["child_order"])) for st in schedule]


class TestSizing:
    def test_a_dense_ladder_narrows_off_the_width_that_walled_the_v3_run(self):
        # The measured shape: at width 3 an order-0 dispatch node folds its
        # 1 + 4 + 16 covered descendants in one invoke. The sizing refuses
        # that and takes width 2, where a dispatch node folds 1 + 4.
        #
        # The schedule is a REFINEMENT of the fixed-width one, so each of the
        # three width-3 tuples splits inside its own span — width 2 then the
        # width-1 remainder — rather than the boundaries sliding down the
        # ladder: 6 tuples, not 5 (review finding).
        schedule = sized_stage_tuples(9, nodes_at=_dense())
        assert [int(st["width"]) for st in schedule] == [2, 1, 2, 1, 2, 1]
        assert _spans(schedule) == [(7, 9), (6, 7), (4, 6), (3, 4), (1, 3), (0, 1)]
        assert all(int(st["fold_max"]) <= STAGE_TARGET_NODES for st in schedule)
        assert max(int(st["fold_max"]) for st in schedule) == 5
        # And the width the run actually used would have folded 21.
        assert _fold_max(9, 0, 3, _dense()) == 21

    def test_a_sparse_ladder_keeps_the_fixed_width_schedule(self):
        # Nothing to narrow: the whole o3 work set's fattest order-0 subtree is
        # four nodes, inside the target, so the sized schedule IS the mirror.
        leaves = ["1111", "1112", "1121", "-2111"]
        sized = sized_stage_tuples(3, nodes_at=_from(leaves))
        assert _spans(sized) == _spans(stage_tuples(3, tuple_width=3))
        assert [list(st["orders"]) for st in sized] == [[2, 1, 0]]
        assert int(sized[0]["fold_max"]) == 4

    @pytest.mark.parametrize("shard_order", range(1, 10))
    @pytest.mark.parametrize("width", (1, 2, 3, 4))
    def test_the_schedule_spans_the_ladder_without_gap_or_overlap(self, shard_order, width):
        # The invariant the worker arm depends on: every order in
        # ``[0, shard_order)`` is folded by exactly one tuple, finest first,
        # and each tuple reads the columns the previous one wrote.
        schedule = sized_stage_tuples(shard_order, nodes_at=_dense(), tuple_width=width, target=5)
        assert int(schedule[0]["child_order"]) == shard_order
        assert int(schedule[-1]["dispatch"]) == 0
        covered = []
        for a, b in itertools.pairwise(schedule):
            assert int(a["dispatch"]) == int(b["child_order"]), "the chain skips an order"
        for st in schedule:
            assert int(st["width"]) <= width
            covered.extend(st["orders"])
        assert sorted(covered) == list(range(shard_order)), covered

    @pytest.mark.parametrize("target", (5, 8, 20))
    def test_every_target_on_the_plateau_gives_the_v3_schedule(self, target):
        # A subtree's node count steps by powers of four (21, 5, 1 for widths
        # 3, 2, 1), so the constant's exact value does not carry the result —
        # what the docstring claims for [5, 20].
        assert _spans(sized_stage_tuples(9, nodes_at=_dense(), target=target)) == _spans(
            sized_stage_tuples(9, nodes_at=_dense(), target=STAGE_TARGET_NODES)
        )

    def test_a_target_of_one_node_degrades_to_width_one_everywhere(self):
        # The floor, and the reason the search always terminates: at width 1 a
        # dispatch node folds itself alone.
        schedule = sized_stage_tuples(9, nodes_at=_dense(), target=1)
        assert [int(st["width"]) for st in schedule] == [1] * 9
        assert _spans(schedule) == _spans(stage_tuples(9, tuple_width=1))

    def test_an_uncovered_order_costs_nothing(self):
        # ``nodes_at`` empty everywhere: a fold of zero clears any target, so
        # the full width stands rather than the schedule collapsing to 1.
        schedule = sized_stage_tuples(9, nodes_at=lambda order: [])
        assert _spans(schedule) == _spans(stage_tuples(9, tuple_width=3))
        assert all(int(st["fold_max"]) == 0 for st in schedule)

    @pytest.mark.parametrize(
        "kwargs,message",
        (
            ({"tuple_width": 0}, "tuple_width must be >= 1"),
            ({"target": 0}, "target must be >= 1"),
        ),
    )
    def test_it_refuses_a_nonsense_knob_by_name(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            sized_stage_tuples(9, nodes_at=_dense(), **kwargs)

    def test_it_refuses_a_shard_order_with_no_ladder(self):
        with pytest.raises(ValueError, match="no above-shard ladder"):
            sized_stage_tuples(0, nodes_at=_dense())

    @pytest.mark.parametrize("span", ((0, 0), (3, 3), (4, 3), (-1, 2), (0, 10)))
    def test_one_stage_tuple_refuses_a_span_outside_the_ladder(self, span):
        with pytest.raises(ValueError, match="not a span of this ladder"):
            one_stage_tuple(9, *span)

    def test_one_stage_tuple_rebuilds_a_fixed_width_tuple_exactly(self):
        # What the worker arm relies on: a span-built tuple and the one the
        # fixed-width schedule names are the same item, ragged tuple included.
        for shard_order, width in ((9, 3), (8, 3), (3, 2), (5, 4)):
            for st in stage_tuples(shard_order, tuple_width=width):
                assert one_stage_tuple(shard_order, st["dispatch"], st["child_order"]) == st


def _fold_max(shard_order, dispatch, child_order, nodes_at):
    """The fattest dispatch node's fold over ``[dispatch, child_order)``."""
    sized = sized_stage_tuples(
        shard_order, nodes_at=nodes_at, tuple_width=child_order - dispatch, target=10**9
    )
    return int(next(st for st in sized if int(st["dispatch"]) == dispatch)["fold_max"])


# ---------------------------------------------------------------------------
# The dispatcher: a sized schedule on the wire, and the short orders it names.
# ---------------------------------------------------------------------------


class TestSizedDispatch:
    """The o3 fixture is sparse — its fattest order-0 fold is four nodes — so a
    target of 2 is what forces the narrowing a dense store meets at the
    default. The schedule it chooses, ``[2]@2`` then ``[1,0]@0``, is the one a
    fixed width 2 would NOT give (that is ``[2]@2``, ``[1,0]@0`` — the same
    here; width 3 gives the single ``[2,1,0]@0`` the default takes)."""

    def test_the_default_is_the_fixed_width_mirror(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _fleet(root, client, barrier_timeout_s=0.01)
        assert summary["stage_target_nodes"] == 0
        assert _spans_of(summary) == _spans(stage_tuples(3, tuple_width=3))
        assert [st["fold_max"] for st in summary["stages"]] == [None]

    def test_a_target_narrows_the_schedule_and_reports_what_chose_it(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _fleet(root, client, barrier_timeout_s=0.01, stage_target_nodes=5)
        assert summary["stage_target_nodes"] == 5
        assert _spans_of(summary) == [(1, 3), (0, 1)]
        rows = {int(st["dispatch_order"]): st for st in summary["stages"]}
        # The dense bound, not a measurement of this four-leaf store: the
        # dispatcher was handed no coverage MOC and may read none (D8).
        assert (rows[1]["width"], rows[1]["fold_max"]) == (2, 5)
        assert (rows[0]["width"], rows[0]["fold_max"]) == (1, 1)

    def test_every_stage_event_carries_its_own_span(self, tmp_path):
        # At target 5 the width-3 tuple refines into ``[2,1]@1`` then
        # ``[0]@0`` — a grouping NO fixed ``tuple_width`` produces, since
        # dispatch 1 is not a multiple of its own width 2. That is why the
        # span rides on the event: a worker re-deriving the tuple from the
        # width would not find a tuple dispatching at order 1 at all.
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _fleet(root, client, barrier_timeout_s=0.01, stage_target_nodes=5)
        assert _spans_of(summary) == [(1, 3), (0, 1)]
        spans = {
            (int(b["dispatch"]), int(b["child_order"]), int(b["tuple_width"]))
            for b in client.blocks()
            if b.get("role") == "stage"
        }
        assert spans == {(1, 3, 2), (0, 1, 1)}

    def test_the_span_names_a_tuple_no_width_could_select(self, tmp_path):
        # The other half of the claim above, said as a refusal: ask the pass
        # for that dispatch order with the width alone and there is no such
        # tuple — which is also what an older worker does with a sized event,
        # loudly, rather than folding the wrong span.
        from zagg.hive import read_manifest
        from zagg.sweep_stages import sweep_stage_pass

        root = tmp_path / "s"
        _stage_store(root)
        manifest = read_manifest(str(root))
        with pytest.raises(ValueError, match="no stage tuple dispatches at order 1"):
            sweep_stage_pass(str(root), manifest, {}, tuple_width=2, run_id="X", only_dispatch=1)
        # And the span is refused without the dispatch order it starts at.
        with pytest.raises(ValueError, match="without the dispatch order"):
            sweep_stage_pass(str(root), manifest, {}, tuple_width=2, run_id="X", only_child_order=3)

    def test_the_worker_folds_the_span_the_event_names(self, tmp_path):
        # End to end on the schedule above: the ``[2,1]@1`` invoke folds two
        # orders in one invoke and the ``[0]@0`` invoke folds the root, so
        # every ladder node of the work set ends up with its artifact.
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(_handler())
        summary = _fleet(root, client, stage_target_nodes=5)
        assert _spans_of(summary) == [(1, 3), (0, 1)]
        assert summary["finisher"]["landed"] and not summary["short_orders"]
        for node in ("1", "-2", "11", "-21", "111", "112", "-211"):
            assert (root / _rel(node) / "all.zarr").exists(), node

    def test_an_explicit_fixed_width_schedule_is_reachable(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        for off in (None, 0):
            summary = _fleet(
                root, _FakeLambda(None), barrier_timeout_s=0.01, stage_target_nodes=off
            )
            assert _spans_of(summary) == _spans(stage_tuples(3, tuple_width=3))

    def test_a_negative_target_is_refused_by_name(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        with pytest.raises(ValueError, match="stage_target_nodes must be >= 1"):
            _fleet(root, _FakeLambda(None), stage_target_nodes=-1)


class TestShortOrders:
    """A tuple missing a unit record names its ORDERS to the finisher. Before,
    the dispatcher sent a bare ``barrier_timed_out`` bool and the finisher
    stamped every level it had an actual for — which is how the v3 ladder
    recorded orders 2..0 from a walled tuple exactly as it recorded 8..3."""

    def test_a_complete_run_names_nothing(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(_handler())
        summary = _fleet(root, client, stage_target_nodes=5, tuple_width=2)
        assert summary["short_orders"] == []
        assert _finisher_block(client)["short_orders"] == []

    def test_a_dropped_tuple_record_names_that_tuples_orders(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        from zagg.sweep_stages import stage_record_name

        root = tmp_path / "s"
        _stage_store(root)
        # The COARSE tuple's record is lost (its invoke never wrote one), so
        # orders 1 and 0 are short while order 2's record stands.
        client = _FakeLambda(_handler(), drop={stage_record_name(0, 0)})
        summary = _fleet(root, client, stage_target_nodes=5, tuple_width=2, barrier_timeout_s=0.05)
        assert summary["short_orders"] == [1, 0]
        assert _finisher_block(client)["short_orders"] == [1, 0]

    def test_the_finisher_withholds_the_short_levels_and_records_the_rest(self, tmp_path):
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        from zagg.hive import MANIFEST_NAME
        from zagg.sweep_stages import stage_record_name

        root = tmp_path / "s"
        _stage_store(root)
        # A prior run's actuals stand on every ladder level, so "withheld"
        # is visibly "left as it stood" rather than "never written".
        _stamp_prior_actuals(root, run_id="PRIOR")
        client = _FakeLambda(_handler(), drop={stage_record_name(0, 0)})
        summary = _fleet(root, client, stage_target_nodes=5, tuple_width=2, barrier_timeout_s=0.05)
        assert summary["short_orders"] == [1, 0]
        entries = {
            int(e["node"]): e
            for e in json.loads((root / MANIFEST_NAME).read_text())["pyramid"]["overviews"]
        }
        # The order that landed is stamped by THIS run; the short ones are not.
        assert entries[2]["actuals"]["run_id"] == summary["run_id"]
        for node in (1, 0):
            assert entries[node]["actuals"]["run_id"] == "PRIOR", node
        # And the leaf tier's own law is not an observation, so it still lands.
        assert entries[3]["actuals"]["regime"] == "leaf-column"

    def test_withhold_levels_is_reported_by_the_finisher(self, tmp_path):
        from zagg.hive import MANIFEST_NAME, read_manifest
        from zagg.sweep_stages import run_finisher

        root = tmp_path / "s"
        _stage_store(root)
        _stamp_prior_actuals(root, run_id="PRIOR")
        manifest = read_manifest(str(root))
        actuals = {
            2: {"regime": "stage-merge", "merges_from_raw": 2, "source_children": {"folded": 1}},
            1: {"regime": "stage-merge", "merges_from_raw": 2, "source_children": {"folded": 1}},
        }
        out = run_finisher(
            str(root), manifest, {}, actuals, run_id="NOW", withhold_levels=[1], store_kwargs={}
        )
        assert out["actuals_withheld"] == [1]
        entries = {
            int(e["node"]): e
            for e in json.loads((root / MANIFEST_NAME).read_text())["pyramid"]["overviews"]
        }
        assert entries[2]["actuals"]["run_id"] == "NOW"
        assert entries[1]["actuals"]["run_id"] == "PRIOR"

    def test_nothing_is_withheld_by_default(self, tmp_path):
        from zagg.hive import MANIFEST_NAME, read_manifest
        from zagg.sweep_stages import run_finisher

        root = tmp_path / "s"
        _stage_store(root)
        _stamp_prior_actuals(root, run_id="PRIOR")
        actuals = {
            1: {"regime": "stage-merge", "merges_from_raw": 2, "source_children": {"folded": 1}}
        }
        out = run_finisher(
            str(root), read_manifest(str(root)), {}, actuals, run_id="NOW", store_kwargs={}
        )
        assert out["actuals_withheld"] == []
        entries = {
            int(e["node"]): e
            for e in json.loads((root / MANIFEST_NAME).read_text())["pyramid"]["overviews"]
        }
        assert entries[1]["actuals"]["run_id"] == "NOW"


class TestSizedByteIdentity:
    def test_a_sized_build_is_the_fixed_width_build(self, tmp_path):
        # The merge-source law across GROUPINGS (#381 point (6)): the sized
        # fleet walks two tuples where the CLI walks one width-3 tuple, so it
        # writes relay stage columns the CLI build never needs — which is why
        # this compares the ladder overviews, the product, exactly as
        # ``test_identity_survives_a_different_tuple_width`` does.
        from test_sweep_stage_fleet import (
            _assert_identical,
            _cli_sweep,
            _FakeLambda,
            _fleet,
            _restore,
            _snapshot,
            _write_discovery_record,
        )

        root = tmp_path / "s"
        _stage_store(root)
        _write_discovery_record(root)
        base = _snapshot(root)
        _cli_sweep(root, tuple_width=3)
        cli = _snapshot(root)
        _restore(root, base)
        summary = _fleet(root, _FakeLambda(_handler()), stage_target_nodes=5)
        # A grouping no fixed ``tuple_width`` produces, so this is not the
        # cross-width arm in another spelling.
        assert _spans_of(summary) == [(1, 3), (0, 1)], "the arm did not size the schedule"
        assert summary["finisher"]["landed"] and not summary["short_orders"]
        _assert_identical(cli, _snapshot(root), ladder_data_only=True)


def _handler():
    from test_sweep_stage_fleet import _handler_module

    return _handler_module().lambda_handler


def _rel(node):
    from zagg.sweep import _node_rel

    return _node_rel(node)


def _spans_of(summary):
    return [(int(st["dispatch_order"]), int(st["orders"][0]) + 1) for st in summary["stages"]]


def _finisher_block(client):
    return next(b for b in client.blocks() if b.get("role") == "finisher")


def _stamp_prior_actuals(root, *, run_id):
    """Give every ladder level an actuals block from an earlier run."""
    from zagg.hive import MANIFEST_NAME

    path = root / MANIFEST_NAME
    manifest = json.loads(path.read_text())
    for entry in manifest["pyramid"]["overviews"]:
        entry["actuals"] = {
            "regime": "stage-merge",
            "merges_from_raw": 2,
            "source_children": {"folded": 1, "missing": 0, "unreadable": 0},
            "run_id": run_id,
            "generated_at": "2026-01-01T00:00:00Z",
        }
    path.write_text(json.dumps(manifest, indent=1))


class TestRunnerSeam:
    """The wall is the TAIL's, so the ask is the tail's: the dispatcher's own
    default is the fixed-width mirror of the in-process pass (the byte-identity
    oracle compares the two arms AT a width), and the seam that meets the 900 s
    ceiling is what turns the sizing on."""

    def _seam(self, root, **kwargs):
        from test_sweep_stage import LEAVES
        from test_sweep_stage_fleet import _FakeLambda

        from zagg.grids.morton import morton_word
        from zagg.runner import _invoke_lambda_stage_sweep

        client = _FakeLambda(None)
        summary = _invoke_lambda_stage_sweep(
            client,
            "zagg-worker",
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            shard_order=3,
            store_kwargs={},
            barrier_timeout_s=0.01,
            total_barrier_budget_s=0.01,
            **kwargs,
        )
        return summary, client

    def test_the_tail_asks_for_the_sized_schedule(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        summary, _ = self._seam(root)
        assert summary["stage_target_nodes"] == STAGE_TARGET_NODES

    def test_an_explicit_target_rides_through(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        summary, _ = self._seam(root, stage_target_nodes=5)
        assert summary["stage_target_nodes"] == 5
        assert _spans_of(summary) == [(1, 3), (0, 1)]

    def test_none_restores_the_fixed_width_schedule(self, tmp_path):
        # `None` MEANS the fixed-width schedule here, which is why the seam's
        # unset sentinel is `"default"` and not `None` (the same discipline
        # `max_nodes_per_invoke` already uses).
        root = tmp_path / "s"
        _stage_store(root)
        summary, _ = self._seam(root, stage_target_nodes=None)
        assert summary["stage_target_nodes"] == 0
        assert _spans_of(summary) == _spans(stage_tuples(3, tuple_width=3))

    def test_the_seam_warns_about_the_orders_it_withheld(self, tmp_path, caplog):
        import logging

        root = tmp_path / "s"
        _stage_store(root)
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            # No handler, so no record lands: every tuple is short.
            summary, _ = self._seam(root, stage_target_nodes=3)
        assert summary["short_orders"] == [2, 1, 0]
        assert "withheld their manifest actuals" in caplog.text


class TestHandlerForwarding:
    def test_the_stage_arm_forwards_the_span(self, tmp_path, monkeypatch):
        seen = {}
        mod = _handler_module_for(monkeypatch, seen, role="stage")
        mod.lambda_handler(_stage_event(tmp_path, dispatch=1, child_order=3), None)
        assert seen["child_order"] == 3

    def test_the_stage_arm_without_a_span_sends_none(self, tmp_path, monkeypatch):
        # An older dispatcher's event: the width selects the tuple, as before.
        seen = {}
        mod = _handler_module_for(monkeypatch, seen, role="stage")
        mod.lambda_handler(_stage_event(tmp_path, dispatch=0), None)
        assert seen["child_order"] is None

    def test_the_finisher_arm_forwards_the_short_orders(self, tmp_path, monkeypatch):
        seen = {}
        mod = _handler_module_for(monkeypatch, seen, role="finisher")
        mod.lambda_handler(_stage_event(tmp_path, role="finisher", short_orders=[2, 1]), None)
        assert list(seen["short_orders"]) == [2, 1]

    def test_the_finisher_arm_without_them_withholds_nothing(self, tmp_path, monkeypatch):
        seen = {}
        mod = _handler_module_for(monkeypatch, seen, role="finisher")
        mod.lambda_handler(_stage_event(tmp_path, role="finisher"), None)
        assert list(seen["short_orders"]) == []


def _handler_module_for(monkeypatch, seen, *, role):
    """The handler with the worker/finisher call it makes captured."""
    from test_sweep_stage_fleet import _handler_module

    import zagg.sweep_stages as stages

    mod = _handler_module()
    name = "run_stage_worker" if role == "stage" else "run_stage_finisher"

    def capture(*args, **kwargs):
        seen.update(kwargs)
        return {"captured": True}

    monkeypatch.setattr(stages, name, capture)
    return mod


def _stage_event(tmp_path, *, role="stage", dispatch=0, child_order=None, **extra):
    block = {
        "role": role,
        "run_id": "R",
        "run_started": "2026-01-01T00:00:00Z",
        "records_from": str(tmp_path / "status"),
        **extra,
    }
    if role == "stage":
        block.update({"dispatch": dispatch, "nodes": ["1"], "batch": 0})
        if child_order is not None:
            block["child_order"] = child_order
    return {
        "mode": "sweep",
        "store_path": str(tmp_path / "s"),
        "stage": block,
        "leaves": [],
    }
