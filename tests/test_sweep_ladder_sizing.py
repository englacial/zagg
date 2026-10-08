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
  fixed-width build's (``TestSizedByteIdentity``, the cascade, issue #620);
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

    @pytest.mark.parametrize("shard_order", (7, 8, 10, 11))
    @pytest.mark.parametrize("width", (3, 4))
    def test_a_ragged_ladder_keeps_the_fixed_width_schedule(self, shard_order, width):
        # The sizing REFINES the fixed-width schedule — it subdivides a tuple
        # inside its own span and never moves a boundary — so a ladder that
        # needs no narrowing reproduces ``stage_tuples`` item for item at a
        # ragged ``shard_order`` too. A walk that took full widths down from
        # ``shard_order`` instead anchored its ragged tuple at the COARSE end
        # and reshaped every boundary here (review finding).
        keys = ("dispatch", "orders", "child_order")
        sized = sized_stage_tuples(shard_order, nodes_at=lambda order: [], tuple_width=width)
        assert [{k: st[k] for k in keys} for st in sized] == stage_tuples(
            shard_order, tuple_width=width
        )

    @pytest.mark.parametrize("width,bound", ((1, 1), (2, 5), (3, 21), (4, 85)))
    def test_the_dense_bound_is_what_a_blind_dispatcher_assumes(self, width, bound):
        # No ``nodes_at``: a dispatcher cannot see the store's density and may
        # not read it (D8), so it assumes the DENSE subtree — 1 + 4 + ... +
        # 4**(width-1). Never an under-estimate, which is the safe direction
        # (review finding: measuring the RUN's work set under-counts every
        # append, because the worker folds the store's coverage too).
        schedule = sized_stage_tuples(width, tuple_width=width, target=bound)
        assert [int(st["width"]) for st in schedule] == [width]
        assert int(schedule[0]["fold_max"]) == bound

    def test_the_blind_default_takes_the_width_the_v3_store_needed(self):
        # One node under the width-3 bound and the dense arm refuses it: the
        # schedule the tail now gets without reading anything.
        schedule = sized_stage_tuples(9, target=STAGE_TARGET_NODES)
        assert [int(st["width"]) for st in schedule] == [2, 1] * 3
        assert [int(st["fold_max"]) for st in schedule] == [5, 1] * 3

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
    """The dispatcher is handed no coverage MOC here (the tail hands none —
    D8), so the sizing takes the DENSE bound rather than measuring this
    four-leaf fixture. At ``shard_order=3`` a target of 21 admits the whole
    width-3 tuple, a target in ``[5, 20]`` refines it into ``[2,1]@1`` then
    ``[0]@0``, and anything under 5 collapses it to width 1 throughout.
    ``[2,1]@1`` is the grouping no fixed ``tuple_width`` produces — width 2
    gives ``[2]@2`` then ``[1,0]@0`` — which is why the span has to ride on
    the event."""

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
        # The RUN's ``tuple_width`` on the wire, unchanged by the sizing: the
        # span is what carries the tuple's own width (review finding).
        assert spans == {(1, 3, 3), (0, 1, 3)}

    def test_an_unsized_ragged_schedule_sends_the_runs_width(self, tmp_path):
        # The wire's ``tuple_width`` is the RUN's on the unsized path too, so a
        # fleet predating the span keeps working on a RAGGED ladder: it
        # re-derives ``stage_tuples(shard_order, tuple_width)`` and filters by
        # dispatch, which lands the right span only when the width it is handed
        # is the one that built the schedule (review finding). At
        # ``shard_order=3, tuple_width=2`` the finest tuple's own width is 1.
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        client = _FakeLambda(None)
        summary = _fleet(root, client, barrier_timeout_s=0.01, tuple_width=2)
        assert _spans_of(summary) == _spans(stage_tuples(3, tuple_width=2))
        widths = {int(b["tuple_width"]) for b in client.blocks() if b.get("role") == "stage"}
        assert widths == {2}
        # The per-tuple width stays on the dispatcher's own row.
        assert [int(st["width"]) for st in summary["stages"]] == [1, 2]

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

    @pytest.mark.parametrize("cap,spans", ((1, [(0, 3)]), (3, [(1, 3), (0, 1)])))
    def test_the_target_is_shared_out_across_an_invokes_nodes(self, tmp_path, cap, spans):
        # ``max_nodes_per_invoke`` nodes ride ONE invoke and each folds its own
        # subtree, so one invoke's fold is up to ``n x fold_max`` and a target
        # of 21 bounds a width-3 tuple only at one node an invoke (review
        # finding). At three it admits width 2 — a third of the target.
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        summary = _fleet(
            root,
            _FakeLambda(None),
            barrier_timeout_s=0.01,
            total_barrier_budget_s=0.01,
            stage_target_nodes=21,
            max_nodes_per_invoke=cap,
        )
        assert _spans_of(summary) == spans

    def test_payload_only_packing_says_the_target_bounds_no_invoke(self, tmp_path, caplog):
        # ``None`` puts a whole tuple on one worker, so the node count is not
        # known here and the target cannot bound the invoke at all. Said once,
        # loudly; the sizing then proceeds as if one node an invoke.
        import logging

        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            summary = _fleet(
                root,
                _FakeLambda(None),
                barrier_timeout_s=0.01,
                total_barrier_budget_s=0.01,
                stage_target_nodes=21,
                max_nodes_per_invoke=None,
            )
        assert "cannot bound what ONE invoke folds" in caplog.text
        assert _spans_of(summary) == [(0, 3)]

    @pytest.mark.parametrize("bad", (-1, True, 2.9, "8", "eight"))
    def test_a_nonsense_target_is_refused_by_name(self, tmp_path, bad):
        # The validation the knob three lines above it in the module already
        # had (review finding): a bool, a non-integral float and a string are
        # refused rather than coerced into a schedule the summary then reports
        # as the caller's.
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        root = tmp_path / "s"
        _stage_store(root)
        with pytest.raises(ValueError, match="stage_target_nodes must be a whole number"):
            # The barrier knobs are pinned low so a knob that is NOT refused
            # ends the run rather than driving its barriers to the default
            # 2700 s — the failure mode this test's own regression check hit.
            _fleet(
                root,
                _FakeLambda(None),
                stage_target_nodes=bad,
                barrier_timeout_s=0.01,
                total_barrier_budget_s=0.01,
            )


class TestFoldsWhatTheWorkerFolds:
    """The sizing must count the tree the INVOKE walks, not the one it is sent
    (review finding). A stage worker folds its candidate set — the run's leaves
    UNION the store's root ``coverage.moc`` (``sweep_overview._candidate_decimals``)
    — so on every append, where the work set is a strict subset of the
    coverage, a schedule sized from the work set's own ancestors keeps a width
    whose invokes fold far more than the target."""

    def test_the_candidate_nodes_are_the_work_set_union_the_coverage(self):
        from test_sweep_stage import LEAVES

        from zagg.grids.morton import morton_word
        from zagg.sweep_fleet import candidate_dispatch_nodes, coverage_dispatch_nodes

        work = {"1111": {None}}  # one leaf of a store that has committed four
        coverage = [morton_word(d) for d in LEAVES]
        assert candidate_dispatch_nodes(work, 0, coverage) == ["-2", "1"]
        assert candidate_dispatch_nodes(work, 2, coverage) == ["-211", "111", "112"]
        # What the dispatcher INVOKES stays the work set's own ancestors: the
        # coverage only filters those, which is exactly why it cannot size.
        assert coverage_dispatch_nodes(work, 0, coverage) == ["1"]
        assert coverage_dispatch_nodes(work, 2, coverage) == ["111"]

    def test_a_scope_filters_the_candidates_the_way_the_fold_does(self):
        from test_sweep_stage import LEAVES

        from zagg.grids.morton import morton_word
        from zagg.sweep_fleet import candidate_dispatch_nodes
        from zagg.sweep_stages import normalize_scope

        coverage = [morton_word(d) for d in LEAVES]
        nodes = candidate_dispatch_nodes({"1111": {None}}, 0, coverage, normalize_scope(["1"]))
        assert nodes == ["1"]

    def test_it_refuses_a_missing_coverage_by_name(self):
        from zagg.sweep_fleet import candidate_dispatch_nodes

        with pytest.raises(ValueError, match="needs the store's coverage MOC"):
            candidate_dispatch_nodes({"1111": {None}}, 0, None)

    def test_a_work_set_inside_the_coverage_is_sized_from_the_coverage(self, tmp_path):
        # The regression for the blocking finding: ONE leaf of the four-leaf
        # fixture, with the store's coverage handed in. The work set's order-0
        # ancestor chain is three nodes — inside a target of 3 — but the
        # invoke folds the coverage's four, so the width must narrow.
        from test_sweep_stage import LEAVES
        from test_sweep_stage_fleet import _FakeLambda, _fleet

        from zagg.grids.morton import morton_word

        root = tmp_path / "s"
        _stage_store(root)
        summary = _fleet(
            root,
            _FakeLambda(None),
            leaves=[(morton_word("1111"), None)],
            coverage=[morton_word(d) for d in LEAVES],
            stage_target_nodes=3,
            barrier_timeout_s=0.01,
            total_barrier_budget_s=0.01,
        )
        assert summary["coverage_computed"] is True
        assert _spans_of(summary) == [(1, 3), (0, 1)]
        rows = {int(st["dispatch_order"]): st for st in summary["stages"]}
        assert (rows[1]["width"], rows[1]["fold_max"]) == (2, 3)
        # And the invoke TARGETS are still this run's own ancestors — one node
        # a tuple, not the coverage's two base cells.
        assert [int(st["nodes"]) for st in summary["stages"]] == [1, 1]


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

    def test_a_lost_close_record_withholds_the_tuples_window_levels(self, tmp_path):
        # A tuple is short on EITHER fan-out: every window unit's record
        # landed here and only the all-time close of node ``-2`` was lost, and
        # the whole span is still withheld (review finding). The safe
        # direction — the close folds the node's windows, so its loss leaves
        # the level's all-time artifact unaccounted for.
        from test_sweep_stage_fleet import _FakeLambda
        from test_sweep_units import _windowed_fleet, _windowed_store

        from zagg.hive import MANIFEST_NAME
        from zagg.sweep_stages import stage_record_name

        root = tmp_path / "s"
        _windowed_store(root)
        _stamp_prior_actuals(root, run_id="PRIOR")
        client = _FakeLambda(_handler(), drop={stage_record_name(0, 4)})
        summary = _windowed_fleet(root, client, barrier_timeout_s=0.05)
        (row,) = summary["stages"]
        assert row["records_seen"] == row["batches"]  # every window unit in
        assert (row["close_batches"], row["close_records_seen"]) == (2, 1)
        assert row["missing_unit_count"] == 1
        assert summary["short_orders"] == [2, 1, 0]
        assert _finisher_block(client)["short_orders"] == [2, 1, 0]
        entries = {
            int(e["node"]): e
            for e in json.loads((root / MANIFEST_NAME).read_text())["pyramid"]["overviews"]
        }
        for node in (2, 1, 0):
            assert entries[node]["actuals"]["run_id"] == "PRIOR", node
        # The leaf tier's law is not an observation of this run, so it lands.
        assert entries[3]["actuals"]["regime"] == "leaf-column"

    def test_an_all_short_run_touches_no_level_entry(self, tmp_path):
        # The state the runner seam's own warning test produces: no record
        # landed, so every order is short and there is no actual to record.
        # ``run_finisher`` writes only ``if changed or level_actuals``, so the
        # manifest is not re-PUT at all — the §4.10 companion and the
        # lifecycle touch stand on their own ``by_shard``/``touch_policy``
        # gates rather than on that write, and the lease release is
        # unconditional (review finding).
        from zagg.hive import MANIFEST_NAME, read_manifest
        from zagg.sweep_stages import run_finisher

        root = tmp_path / "s"
        _stage_store(root)
        _stamp_prior_actuals(root, run_id="PRIOR")
        before = (root / MANIFEST_NAME).read_bytes()
        released = []
        out = run_finisher(
            str(root),
            read_manifest(str(root)),
            {},
            {},
            run_id="NOW",
            withhold_levels=[2, 1, 0],
            release=lambda: released.append(True),
            store_kwargs={},
        )
        assert out["actuals_withheld"] == [2, 1, 0]
        assert out["manifest_updated"] is False
        assert (root / MANIFEST_NAME).read_bytes() == before
        assert released == [True]
        # The leaf tier's ``leaf-column`` law is not written either: its branch
        # is gated on ``level_actuals``, which an all-short run has none of.
        entries = {
            int(e["node"]): e
            for e in json.loads((root / MANIFEST_NAME).read_text())["pyramid"]["overviews"]
        }
        assert {e["actuals"]["run_id"] for e in entries.values()} == {"PRIOR"}

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
        # The cascade across GROUPINGS (issue #620; #381 point (6)): the sized
        # fleet walks two tuples where the CLI walks one width-3 tuple, so it
        # writes stage columns the CLI build never needs — which is why
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

        from zagg.hive import MANIFEST_NAME

        root = tmp_path / "s"
        _stage_store(root)
        _stamp_prior_actuals(root, run_id="PRIOR")
        before = (root / MANIFEST_NAME).read_bytes()
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            # No handler, so no record lands: every tuple is short.
            summary, _ = self._seam(root, stage_target_nodes=3)
        assert summary["short_orders"] == [2, 1, 0]
        assert "withheld their manifest actuals" in caplog.text
        # And an all-short run stamps nothing, leaf tier included.
        assert (root / MANIFEST_NAME).read_bytes() == before


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
