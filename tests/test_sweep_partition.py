"""``2^n`` morton-subtree sweep partitions (issue #377): the decomposition.

The standing claims this file pins. On the decomposition itself: the split
rounds to morton DIGIT boundaries (a power of four) and rejects an odd ``2^n``
by name; ownership is a pure prefix test over the D1 decimal id; and the
decomposition is a partition in the set-theoretic sense — disjoint and
covering. On a partitioned pass: no two partitions write the same object, the
union of every partition equals an unpartitioned pass minus the coarse nodes
above the split, the ``finish`` hook and the coarse overview orders are
deferred, and each partition stays independently idempotent and resumable.
"""

import itertools
import json

import obstore
import pytest

from zagg import sweep as sweep_mod
from zagg import sweep_overview as overview_mod
from zagg.grids.morton import morton_decimal, morton_word
from zagg.hive import MANIFEST_NAME, _decimal_order, shard_leaf_path
from zagg.store import open_object_store
from zagg.sweep import run_sweep
from zagg.sweep_partition import (
    normalize_partition,
    partition_index,
    partition_leaves,
    partition_split_order,
    select_partition,
    sweep_partitions,
)
from zagg.telemetry import build_record, write_sidecar

#: Every order-2 node of one base cell, plus a second base cell's, so the
#: "one order-k subtree per base cell" property is exercised, not assumed.
NODES = [f"{b}{i}{j}" for b in ("-3", "1") for i in "1234" for j in "1234"]

SHARD_ORDER = 2

#: The unpatched writers, captured once so a re-installed spy never nests.
_PUT_ROLLUP = sweep_mod._put_rollup
_WRITE_OVERVIEW = overview_mod._write_overview
_OBSTORE_PUT = obstore.put

#: Leaves spread over three of the four order-1 subtrees AND two base cells,
#: so a partitions=4 (split order 1) pass exercises a partition with several
#: shards, one with a single shard, one spanning two base cells, and an empty
#: one — the four cases a fan-out actually meets.
LEAVES = ("-311", "-312", "-321", "-341", "141", "142")


def _write_manifest(root, *, pyramid=None):
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": SHARD_ORDER + 2,
        "shard_order": SHARD_ORDER,
        "split_schedule": [1] * SHARD_ORDER,
        "pyramid": pyramid if pyramid is not None else {"orders": [], "aggregation": {}},
        "generated_at": "2026-01-01T00:00:00+00:00",
    }
    obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())


def _put_leaf(root, decimal, *, n_obs=10, window=None):
    record = build_record(
        shard_key=morton_word(decimal),
        metadata={"total_obs": n_obs, "cells_with_data": 2, "duration_s": 0.5},
        granule_ids=[f"g-{decimal}"],
    )
    write_sidecar(shard_leaf_path(str(root), morton_word(decimal), window=window), record)
    return record


def _store(tmp_path, decimals=LEAVES, **kwargs):
    _write_manifest(tmp_path, **kwargs)
    for decimal in decimals:
        _put_leaf(tmp_path, decimal)
    return [(morton_word(d), None) for d in decimals]


def _overview_store(tmp_path, decimals=LEAVES, *, orders=(1, 0)):
    """A store with REAL leaf zarrs, so the overview family actually folds.

    ``_store`` above writes telemetry sidecars only — enough for the JSON
    families, whose leaf IS the sidecar, but the overview family reads leaf
    zarrs, so on such a store every ``_roll_node`` fails and ``written``
    stays 0. An overview assertion on that store passes vacuously, which is
    exactly how the manifest-RMW fence came to be untested (issue #377).
    """
    from test_sweep_overview import _make_leaf
    from test_sweep_overview import _write_manifest as _write_overview_manifest

    _write_overview_manifest(tmp_path, orders=orders)
    for decimal in decimals:
        _make_leaf(tmp_path, decimal, {0: [1.0, 2.0], 5: [10.0]})
    return [(morton_word(d), None) for d in decimals]


def _overview_written(monkeypatch):
    """Capture every OVERVIEW object key a pass writes (envelope AND slab).

    The overview family never reaches ``_put_rollup``: it PUTs its envelope
    directly and writes the slab through ``_write_overview``, so :func:`_written`
    is structurally blind to it — and the overview slabs are the artifacts
    issue #377 exists to parallelize. Delegates to the PRISTINE writers, so a
    re-install replaces the spy rather than nesting it.
    """
    keys: list[str] = []

    def put_spy(store, path, *args, **kwargs):
        if str(path).endswith(overview_mod.ENVELOPE_NAME):
            keys.append(str(path))
        return _OBSTORE_PUT(store, path, *args, **kwargs)

    def write_spy(store_root, node, *args, **kwargs):
        basename = _WRITE_OVERVIEW(store_root, node, *args, **kwargs)
        keys.append(f"{overview_mod._node_rel(node)}/{basename}")
        return basename

    monkeypatch.setattr(obstore, "put", put_spy)
    monkeypatch.setattr(overview_mod, "_write_overview", write_spy)
    return keys


def _run_record(root, decimals=LEAVES, run_id="r1"):
    """The store-root run parquet ``discover_leaves`` (and so the CLI) reads."""
    from zagg.telemetry import flatten_record, write_run_parquet

    rows = [
        flatten_record(
            build_record(shard_key=morton_word(d), metadata={"total_obs": 1, "duration_s": 1.0})
        )
        for d in decimals
    ]
    write_run_parquet(str(root), rows, run_id=run_id)


def _written(monkeypatch):
    """Capture every rollup object key a pass PUTs (the write-conflict probe).

    Always delegates to the PRISTINE writer, so calling this twice in one test
    replaces the spy rather than nesting it — each returned list holds exactly
    the keys of the pass it was installed for.
    """
    keys: list[str] = []

    def spy(store, fam, decimal, envelope):
        keys.append(sweep_mod._rollup_key(fam, decimal))
        return _PUT_ROLLUP(store, fam, decimal, envelope)

    monkeypatch.setattr(sweep_mod, "_put_rollup", spy)
    return keys


class TestSplitOrder:
    def test_powers_of_four_split_on_a_digit(self):
        assert [partition_split_order(4**k) for k in range(5)] == [0, 1, 2, 3, 4]

    def test_odd_power_of_two_is_rejected_by_name(self):
        # The design fork: 2^n with odd n halves a 2-bit digit. Conservative
        # option ruled here — reject, naming both valid neighbours (issue #377).
        with pytest.raises(ValueError, match=r"splits a morton digit in half") as excinfo:
            partition_split_order(8)
        assert "use 4 or 16" in str(excinfo.value)

    @pytest.mark.parametrize("bad", [0, -4, 3, 6, 12])
    def test_non_power_of_two_is_rejected(self, bad):
        with pytest.raises(ValueError, match=r"power of two"):
            partition_split_order(bad)

    def test_one_partition_is_the_identity(self):
        # partitions=1 -> split order 0 -> a single unit owning the whole tree,
        # which is exactly today's unpartitioned sweep.
        assert partition_split_order(1) == 0
        assert {partition_index(d, 1) for d in NODES} == {0}


class TestOwnership:
    def test_index_is_the_prefix_rank(self):
        # partitions=4 splits at order 1: the first digit IS the index.
        assert [partition_index(f"-3{d}", 4) for d in "1234"] == [0, 1, 2, 3]
        assert [partition_index(f"1{d}77", 4) for d in "1234"] == [0, 1, 2, 3]

    def test_a_node_and_every_descendant_share_a_partition(self):
        for node in NODES:
            deep = [node + tail for tail in ("", "1", "4", "1234", "4321")]
            assert len({partition_index(d, 16) for d in deep}) == 1

    def test_one_subtree_per_base_cell(self):
        # 12 base cells x 4^k order-k subtrees / 4^k partitions == 12 nodes each.
        bases = [f"{s}{b}" for s in ("", "-") for b in "123456"]
        owned: dict[int, list[str]] = {}
        for base, i, j in itertools.product(bases, "1234", "1234"):
            owned.setdefault(partition_index(f"{base}{i}{j}", 16), []).append(f"{base}{i}{j}")
        assert sorted(owned) == list(range(16))
        assert all(len(v) == 12 for v in owned.values())

    def test_coarser_than_the_split_is_refused(self):
        # An order-1 node under a 16-way (order-2) split spans four partitions;
        # it is the finisher's, and asking which partition owns it is an error.
        with pytest.raises(ValueError, match=r"coarser than the partitions=16 split order 2"):
            partition_index("-31", 16)
        # Exactly at the split order is fine — that node is wholly owned.
        assert _decimal_order("-311") == 2 and partition_index("-311", 16) == 0


class TestNormalizePartition:
    def test_none_passes_through(self):
        assert normalize_partition(None) is None

    def test_valid_block_round_trips(self):
        assert normalize_partition({"index": 3, "of": 16}) == (3, 16)
        assert normalize_partition({"index": "3", "of": "16"}) == (3, 16)

    @pytest.mark.parametrize("block", [{"of": 4}, {"index": 0}, {"index": None, "of": 4}, 4, "0/4"])
    def test_malformed_block_is_refused(self, block):
        with pytest.raises(ValueError, match=r"partition must be"):
            normalize_partition(block)

    @pytest.mark.parametrize("index", [-1, 4, 99])
    def test_index_out_of_range_is_refused(self, index):
        with pytest.raises(ValueError, match=r"out of range"):
            normalize_partition({"index": index, "of": 4})

    def test_digit_boundary_rule_is_enforced_worker_side_too(self):
        # A hand-rolled event carrying an odd 2^n must not sweep the wrong
        # subtree: the same validator runs where the fold happens.
        with pytest.raises(ValueError, match=r"splits a morton digit in half"):
            normalize_partition({"index": 0, "of": 8})


class TestPartitionLeaves:
    def _refs(self, decimals, window=None):
        return [(morton_word(d), window) for d in decimals]

    def test_split_is_disjoint_and_covering(self):
        refs = self._refs(NODES)
        buckets = partition_leaves(refs, 16)
        assert sorted(buckets) == list(range(16))  # every index owns a NODES leaf
        flat = [r for b in buckets.values() for r in b]
        assert sorted(flat) == sorted(refs)  # covering, and no duplication
        for a, b in itertools.combinations(buckets.values(), 2):
            assert not set(a) & set(b)

    def test_every_ref_lands_in_its_own_index(self):
        for index, bucket in partition_leaves(self._refs(NODES), 16).items():
            for key, _window in bucket:
                assert partition_index(morton_decimal(key), 16) == index

    def test_windows_ride_along_and_bare_keys_normalize(self):
        buckets = partition_leaves([morton_word("-311"), (morton_word("-321"), "2019")], 4)
        assert buckets[0] == [(morton_word("-311"), None)]
        assert buckets[1] == [(morton_word("-321"), "2019")]

    def test_empty_partitions_are_absent_not_slotted(self):
        # Sparse by design: the empty partitions carry no slot, so the return
        # never costs one allocation per partition (see the width test below).
        assert partition_leaves(self._refs(["-311"]), 16) == {0: [(morton_word("-311"), None)]}

    def test_one_partition_returns_the_whole_work_set(self):
        refs = self._refs(NODES)
        assert partition_leaves(refs, 1) == {0: refs}


class TestSelectPartition:
    def test_filters_and_counts_dropped_leaves(self):
        by_shard = {d: {None} for d in NODES}
        kept, foreign = select_partition(by_shard, 16, 0)
        assert set(kept) == {d for d in NODES if partition_index(d, 16) == 0}
        assert foreign == len(NODES) - len(kept)

    def test_foreign_is_counted_in_leaves_not_shard_nodes(self):
        # A windowed store has several (shard, window) leaves per shard node;
        # the dropped count must be in the same currency as the summary's
        # n_leaves, or "seen minus kept" no longer adds up.
        by_shard = {d: {"2019", "2020", "2021"} for d in NODES}
        kept, foreign = select_partition(by_shard, 16, 0)
        assert foreign == 3 * (len(NODES) - len(kept))
        assert foreign + 3 * len(kept) == 3 * len(NODES)

    def test_union_over_indices_is_the_whole_work_set(self):
        by_shard = {d: {None} for d in NODES}
        seen: set[str] = set()
        for index in range(16):
            kept, _foreign = select_partition(by_shard, 16, index)
            assert not seen & set(kept)
            seen |= set(kept)
        assert seen == set(by_shard)


class TestPartitionsValidatedAsAParameter:
    """A bad ``partitions`` is a PARAMETER error, not a per-ref one."""

    def test_empty_work_set_still_refuses_an_odd_power_of_two(self):
        with pytest.raises(ValueError, match=r"splits a morton digit in half"):
            partition_leaves([], 8)

    def test_refs_are_indexed_before_the_buckets_exist(self):
        # A too-deep split is caught by the ownership test, not after 4^k
        # empty lists have been materialized.
        with pytest.raises(ValueError, match=r"coarser than the partitions="):
            partition_leaves([morton_word("-311")], 4**15)

    @pytest.mark.parametrize("width", [4, 16, 4**15])
    def test_an_empty_work_set_costs_nothing_at_any_valid_width(self, width):
        # The cell the two tests above straddle: a VALID power of four with an
        # EMPTY work set. No ref exists to raise, so a dense index-aligned
        # return would allocate `width` empty lists here — at 4**15 that is
        # 1,073,741,824 of them, and the call never returns.
        assert partition_leaves([], width) == {}

    def test_select_partition_range_checks_its_index(self):
        with pytest.raises(ValueError, match=r"out of range"):
            select_partition({d: {None} for d in NODES}, 4, 9)


class TestPartitionedPass:
    """``run_sweep(partition=...)``: exactly what one partition worker writes."""

    def test_writes_only_its_own_subtrees_and_stops_at_the_split(self, tmp_path, monkeypatch):
        refs = _store(tmp_path)
        keys = _written(monkeypatch)
        summary = run_sweep(
            str(tmp_path), refs, families=["stats"], partition={"index": 0, "of": 4}
        )
        # Partition 0 owns the "-31" subtree: its two shards plus their order-1
        # parent. The base node -3 sits at order 0, above the split -> nobody's.
        assert summary["partition"] == {"index": 0, "of": 4, "split_order": 1}
        assert summary["n_leaves"] == 2 and summary["foreign_leaves"] == 4
        assert sorted(keys) == [
            "-3/1/1/stats.rollup.json",
            "-3/1/2/stats.rollup.json",
            "-3/1/stats.rollup.json",
        ]

    def test_no_two_partitions_write_the_same_object(self, tmp_path, monkeypatch):
        # The disjointness obligation, against the real writer rather than the
        # index arithmetic: the four partitions' key sets are pairwise disjoint.
        refs = _store(tmp_path)
        per_partition = []
        for index in range(4):
            keys = _written(monkeypatch)
            run_sweep(str(tmp_path), refs, families=["stats"], partition={"index": index, "of": 4})
            per_partition.append(set(keys))
        for a, b in itertools.combinations(per_partition, 2):
            assert not a & b
        assert per_partition[2] == set()  # no leaf under -33/13: an empty partition
        assert "1/4/stats.rollup.json" in per_partition[3]  # and one spanning two base cells

    def test_union_equals_an_unpartitioned_pass_minus_the_coarse_nodes(self, tmp_path, monkeypatch):
        whole, split = tmp_path / "whole", tmp_path / "split"
        refs_whole, refs_split = _store(whole), _store(split)
        full = _written(monkeypatch)
        run_sweep(str(whole), refs_whole, families=["stats"], record=False)
        full_keys = set(full)
        parted = _written(monkeypatch)
        for index in range(4):
            run_sweep(
                str(split),
                refs_split,
                families=["stats"],
                record=False,
                partition={"index": index, "of": 4},
            )
        # Every node the whole-tree pass wrote — except the order-0 base nodes,
        # which span partitions — was written by exactly one partition.
        coarse = {"-3/stats.rollup.json", "1/stats.rollup.json"}
        assert coarse <= full_keys
        assert set(parted) == full_keys - coarse

    def test_finish_is_deferred_so_partitions_never_race_on_the_root_moc(self, tmp_path):
        refs = _store(tmp_path)
        result = run_sweep(str(tmp_path), refs, families=["moc"], partition={"index": 0, "of": 4})
        moc = result["families"]["moc"]
        assert moc["finish_deferred"] is True
        assert "root_moc_written" not in moc
        assert not (tmp_path / "coverage.moc").exists()

    def test_the_deferred_orders_are_named_not_just_flagged(self, tmp_path):
        # `finish_deferred` says THAT the finisher is owed something; the
        # orders say how much, the way the overview family already reports it.
        # split == shard_order is permitted, and then every node above the
        # leaves is owed — half this fixture's tree (issue #377).
        refs = _store(tmp_path)
        block = dict(families=["stats"], record=False)
        four = run_sweep(str(tmp_path), refs, partition={"index": 0, "of": 4}, **block)
        assert four["families"]["stats"]["deferred_orders"] == [0]
        deep = run_sweep(str(tmp_path), refs, partition={"index": 0, "of": 16}, **block)
        assert deep["families"]["stats"]["deferred_orders"] == [0, 1]
        whole = run_sweep(str(tmp_path), refs, **block)["families"]["stats"]
        assert "deferred_orders" not in whole and "finish_deferred" not in whole

    def test_identity_partition_matches_an_unpartitioned_pass(self, tmp_path, monkeypatch):
        whole, one = tmp_path / "whole", tmp_path / "one"
        refs_whole, refs_one = _store(whole), _store(one)
        keys_whole = _written(monkeypatch)
        run_sweep(str(whole), refs_whole, families=["stats", "moc"], record=False)
        keys_one = _written(monkeypatch)
        summary = run_sweep(
            str(one),
            refs_one,
            families=["stats", "moc"],
            record=False,
            partition={"index": 0, "of": 1},
        )
        assert sorted(keys_one) == sorted(keys_whole)
        # of=1 splits at order 0: the base nodes ARE owned, so the finish hook
        # runs (it reports its own key) rather than being deferred.
        assert "root_moc_written" in summary["families"]["moc"]
        assert "finish_deferred" not in summary["families"]["moc"]


class TestWorkerSideFiltering:
    """The partition filter lives where the fold happens, not in the dispatcher."""

    def test_a_discover_style_whole_store_work_set_is_filtered(self, tmp_path):
        # ``discover: true`` re-derives the WHOLE store's leaves worker-side; a
        # partition invoke must still sweep only its own.
        refs = _store(tmp_path)
        summary = run_sweep(
            str(tmp_path), refs, families=["stats"], partition={"index": 3, "of": 4}
        )
        assert summary["n_leaves"] == 3  # -341, 141, 142
        assert summary["foreign_leaves"] == 3

    def test_a_windowed_work_set_is_filtered_and_counted_in_leaves(self, tmp_path):
        # The D13 windowed layout: several (shard, window) leaves per shard
        # node. Window membership follows the shard's, and the counts stay in
        # leaf currency on both sides.
        _write_manifest(tmp_path)
        windows = ("2019", "2020")
        refs = []
        for decimal in LEAVES:
            for window in windows:
                _put_leaf(tmp_path, decimal, window=window)
                refs.append((morton_word(decimal), window))
        summary = run_sweep(
            str(tmp_path), refs, families=["stats"], partition={"index": 0, "of": 4}
        )
        assert summary["n_leaves"] == 2 * len(windows)  # -311, -312 x two windows
        assert summary["n_leaves"] + summary["foreign_leaves"] == len(refs)

    def test_split_finer_than_the_shard_order_is_refused(self, tmp_path):
        refs = _store(tmp_path)
        with pytest.raises(ValueError, match=r"splits at order 3, finer than the store's"):
            run_sweep(str(tmp_path), refs, families=["stats"], partition={"index": 0, "of": 64})


class TestPartitionIdempotence:
    """The D22 generation stamps make each partition resumable on its own."""

    def test_second_pass_of_one_partition_writes_nothing(self, tmp_path):
        refs = _store(tmp_path)
        block = {"index": 0, "of": 4}
        first = run_sweep(str(tmp_path), refs, families=["stats"], partition=block)
        second = run_sweep(str(tmp_path), refs, families=["stats"], partition=block)
        assert first["families"]["stats"]["written"] == 3
        assert second["families"]["stats"]["written"] == 0
        assert second["families"]["stats"]["current"] == 3

    def test_a_rerun_leaf_rewrites_only_its_own_partition_chain(self, tmp_path, monkeypatch):
        refs = _store(tmp_path)
        for index in range(4):
            run_sweep(str(tmp_path), refs, families=["stats"], partition={"index": index, "of": 4})
        _put_leaf(tmp_path, "-341", n_obs=99)
        keys = _written(monkeypatch)
        for index in range(4):
            run_sweep(str(tmp_path), refs, families=["stats"], partition={"index": index, "of": 4})
        assert sorted(keys) == ["-3/4/1/stats.rollup.json", "-3/4/stats.rollup.json"]


class TestPartitionRecord:
    """Per-partition timing rows — the issue #354 record, extended."""

    def test_record_key_carries_the_partition_so_the_fan_out_cannot_clobber(self, tmp_path):
        refs = _store(tmp_path)
        records = [
            run_sweep(str(tmp_path), refs, families=["stats"], partition={"index": i, "of": 4})[
                "record"
            ]
            for i in (0, 1)
        ]
        # The 2^n invokes of one fan-out land in the same wall-clock second.
        assert records[0].endswith("_p0of4.json") and records[1].endswith("_p1of4.json")
        body = json.loads((tmp_path / records[1]).read_text())
        assert body["partition"] == {"index": 1, "of": 4, "split_order": 1}
        assert body["families"]["stats"]["duration_s"] >= 0.0

    def test_an_unpartitioned_record_keeps_its_name_and_carries_no_partition(self, tmp_path):
        refs = _store(tmp_path)
        summary = run_sweep(str(tmp_path), refs, families=["stats"])
        assert summary["record"].endswith("Z.json") and "_p" not in summary["record"]
        assert "partition" not in summary and "foreign_leaves" not in summary


class TestOverviewOrdersAreClamped:
    """Every assertion here runs against REAL leaves and asserts ``written``.

    Both of these tests are only load-bearing on a pass that actually folded
    something: the unfenced manifest RMW is reached ``if counts["written"]``,
    so a pass whose every fold failed cannot tell the fence from its absence.
    """

    def test_orders_coarser_than_the_split_are_deferred_to_the_finisher(self, tmp_path):
        whole_root, split_root = tmp_path / "whole", tmp_path / "split"
        parted = run_sweep(
            str(split_root),
            _overview_store(split_root),
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert parted["deferred_orders"] == [0] and parted["written"] > 0
        whole = run_sweep(
            str(whole_root), _overview_store(whole_root), families=["overview"], record=False
        )["families"]["overview"]
        assert "deferred_orders" not in whole and whole["written"] > 0

    def test_the_manifest_pyramid_rmw_is_left_to_the_finisher(self, tmp_path, monkeypatch):
        # `_update_manifest_pyramid` is a GET-modify-PUT of the ONE store-root
        # manifest: 2^n partitions racing it would lose updates, so a
        # partitioned pass must not touch it at all.
        refs = _overview_store(tmp_path, orders=(1,))
        monkeypatch.setattr(
            overview_mod,
            "_update_manifest_pyramid",
            lambda *a, **k: pytest.fail("manifest RMW in a partition"),
        )
        before = (tmp_path / MANIFEST_NAME).read_bytes()
        result = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert result["written"] > 0  # the fence is reached, not skipped
        assert result["manifest_deferred"] is True
        assert "manifest_updated" not in result
        assert (tmp_path / MANIFEST_NAME).read_bytes() == before

    def test_the_deferred_manifest_update_carries_its_payload(self, tmp_path):
        # A deferral the finisher can act on from the record alone: the order
        # set the RMW would have unioned, not just "somebody owes an update".
        refs = _overview_store(tmp_path)
        parted = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        # Order 0 is deferred, so only the order-1 overview materialized here.
        assert parted["materialized_orders"] == [1]
        # ...and the regime that WROTE it (issue #376's §4.5 fold_sources
        # actual): the deferred RMW's whole payload rides the record, or the
        # finisher re-derives it from the envelopes.
        assert parted["materialized_fold_sources"] == {"1": "leaves"}
        # The other arm of the actuals: a re-run's levels are all current, so
        # the order set still rides (the RMW would still union it) but NO
        # regime is claimed for a level that did not write this pass.
        rerun = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert rerun["materialized_orders"] == [1]
        assert "materialized_fold_sources" not in rerun
        whole_root = tmp_path / "whole"
        whole = run_sweep(
            str(whole_root), _overview_store(whole_root), families=["overview"], record=False
        )["families"]["overview"]
        assert "materialized_orders" not in whole and whole["manifest_updated"] is True
        assert "materialized_fold_sources" not in whole

    def test_a_cascade_level_at_the_split_runs_inside_the_partition(self, tmp_path):
        # Plans derive from the FULL declared schedule (issue #376) and only
        # then does the partition clamp drop the coarse tail (issue #377):
        # with orders (2, 1, 0) and a split at 1, order 2 folds the leaves,
        # order 1 CASCADES from order 2 entirely inside the partition's own
        # prefix, and order 0 stays the finisher's.
        from test_sweep_overview import _make_leaf
        from test_sweep_overview import _write_manifest as _write_overview_manifest

        _write_overview_manifest(tmp_path, orders=(2, 1, 0), shard_order=3, cell_order=5)
        decimals = ("-3111", "-3124")  # one order-1 subtree: partition 0 of 4
        for decimal in decimals:
            _make_leaf(tmp_path, decimal, {0: [1.0, 2.0], 5: [10.0]}, shard_order=3, cell_order=5)
        refs = [(morton_word(d), None) for d in decimals]
        parted = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert parted["deferred_orders"] == [0] and parted["written"] > 0
        assert parted["materialized_fold_sources"] == {"2": "leaves", "1": "cascade"}

    def test_a_v2_declaration_refuses_identically_under_a_partition(self, tmp_path):
        # The /1 family generates nothing for /2 (the STAGED sweep owns it,
        # issue #384). The gate fires BEFORE the partition clamp, so a
        # partitioned pass no-ops exactly like an unpartitioned one: nothing
        # is folded, so nothing is deferred and no partition key rides the
        # counts.
        refs = _overview_store(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["pyramid"] = {
            "spec": "zagg-pyramid/2",
            "overviews": [{"node": 1, "cells": [2]}, {"node": 0, "cells": [1]}],
        }
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        parted = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        whole = run_sweep(str(tmp_path), refs, families=["overview"], record=False)["families"][
            "overview"
        ]
        for counts in (parted, whole):
            assert counts["regime"] == "stages" and counts["written"] == 0
        assert "deferred_orders" not in parted and "manifest_deferred" not in parted
        assert {k: v for k, v in parted.items() if k != "duration_s"} == {
            k: v for k, v in whole.items() if k != "duration_s"
        }


class TestRootMocIsAlsoAnInput:
    """The fenced list's reciprocal: a partitioned pass READS what it defers.

    Overview discovery unions the dirty set with the store-root
    ``coverage.moc`` — the refresh a partitioned pass skips — so the degraded
    read is the partitioned steady state and belongs in the per-partition
    record, not only in a log line several frames down (issue #377).
    """

    def test_a_degraded_root_moc_read_is_reported_not_only_logged(self, tmp_path):
        refs = _overview_store(tmp_path)
        result = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert result["root_moc_stale"] is True

    def test_a_usable_root_moc_is_not_reported_stale(self, tmp_path):
        from zagg.hive import build_root_coverage, write_root_coverage

        refs = _overview_store(tmp_path)
        write_root_coverage(
            str(tmp_path), build_root_coverage([morton_word(d) for d in LEAVES], SHARD_ORDER)
        )
        result = run_sweep(
            str(tmp_path),
            refs,
            families=["overview"],
            record=False,
            partition={"index": 0, "of": 4},
        )["families"]["overview"]
        assert "root_moc_stale" not in result and result["written"] > 0


class TestOverviewDisjointness:
    """The overview family's own write-conflict evidence.

    The ``_put_rollup`` spy the JSON-family tests use is blind to this family
    (:func:`_overview_written`), and the overview slabs are the artifacts issue
    #377 exists for — the o9 blow-up in the issue is overview folds, not JSON
    rollups. So the pairwise-disjointness obligation is discharged here too,
    against the real writers.
    """

    def test_no_two_partitions_write_the_same_overview_object(self, tmp_path, monkeypatch):
        refs = _overview_store(tmp_path)
        per_partition = []
        for index in range(4):
            keys = _overview_written(monkeypatch)
            run_sweep(
                str(tmp_path),
                refs,
                families=["overview"],
                record=False,
                partition={"index": index, "of": 4},
            )
            per_partition.append(set(keys))
        for a, b in itertools.combinations(per_partition, 2):
            assert not a & b
        assert per_partition[2] == set()  # no leaf under -33/13: an empty partition
        # Both object kinds are in the audit — the slab and its envelope.
        assert {"-3/1/all.zarr", f"-3/1/{overview_mod.ENVELOPE_NAME}"} <= per_partition[0]
        assert "1/4/all.zarr" in per_partition[3]  # and one spanning two base cells

    def test_overview_union_equals_a_whole_tree_pass_minus_the_coarse_nodes(
        self, tmp_path, monkeypatch
    ):
        whole, split = tmp_path / "whole", tmp_path / "split"
        refs_whole, refs_split = _overview_store(whole), _overview_store(split)
        full = _overview_written(monkeypatch)
        run_sweep(str(whole), refs_whole, families=["overview"], record=False)
        full_keys = set(full)
        parted = _overview_written(monkeypatch)
        for index in range(4):
            run_sweep(
                str(split),
                refs_split,
                families=["overview"],
                record=False,
                partition={"index": index, "of": 4},
            )
        # The order-0 overviews span partitions and are the finisher's; every
        # other overview object was written by exactly one partition.
        coarse = {
            f"{node}/{name}"
            for node in ("-3", "1")
            for name in ("all.zarr", overview_mod.ENVELOPE_NAME)
        }
        assert coarse <= full_keys
        assert set(parted) == full_keys - coarse


class TestSweepPartitionsDriver:
    def test_runs_every_non_empty_partition_in_index_order(self, tmp_path):
        refs = _store(tmp_path)
        summaries = sweep_partitions(str(tmp_path), refs, partitions=4, families=["stats"])
        assert [s["partition"]["index"] for s in summaries] == [0, 1, 3]  # 2 is empty
        assert sum(s["n_leaves"] for s in summaries) == len(LEAVES)


class TestDispatchFanOut:
    """``_invoke_lambda_sweep(partitions=2**n)``: the D8 worker-invoke leg."""

    def _client(self):
        from unittest.mock import MagicMock

        return MagicMock()

    def _events(self, client):
        return [json.loads(c.kwargs["Payload"]) for c in client.invoke.call_args_list]

    def test_default_is_the_single_invoke_form_byte_identical(self):
        from zagg.runner import _invoke_lambda_sweep

        client = self._client()
        fired = _invoke_lambda_sweep(client, "fn", "s3://b/store", [(7, None), (8, "2019")])
        assert fired == 1
        event = self._events(client)[0]
        assert event == {
            "mode": "sweep",
            "store_path": "s3://b/store",
            "leaves": [[7, None], [8, "2019"]],
        }

    def test_fan_out_fires_one_invoke_per_non_empty_partition(self):
        from zagg.runner import _invoke_lambda_sweep

        client = self._client()
        refs = [(morton_word(d), None) for d in LEAVES]
        fired = _invoke_lambda_sweep(client, "fn", "s3://b/store", refs, partitions=4)
        events = self._events(client)
        assert fired == 3  # partition 2 is empty -> no invoke
        assert all(c.kwargs["InvocationType"] == "Event" for c in client.invoke.call_args_list)
        assert [e["partition"] for e in events] == [
            {"index": 0, "of": 4},
            {"index": 1, "of": 4},
            {"index": 3, "of": 4},
        ]

    def test_each_invoke_carries_only_its_own_disjoint_slice(self):
        from zagg.runner import _invoke_lambda_sweep

        client = self._client()
        refs = [(morton_word(d), None) for d in LEAVES]
        _invoke_lambda_sweep(client, "fn", "s3://b/store", refs, partitions=4)
        slices = [[tuple(leaf) for leaf in e["leaves"]] for e in self._events(client)]
        for a, b in itertools.combinations(slices, 2):
            assert not set(a) & set(b)
        assert sorted(leaf for s in slices for leaf in s) == sorted(
            (key, window) for key, window in refs
        )
        for event in self._events(client):
            for key, _window in event["leaves"]:
                assert partition_index(morton_decimal(key), 4) == event["partition"]["index"]

    def test_an_oversized_slice_falls_back_to_partition_scoped_discovery(self):
        from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES, _invoke_lambda_sweep

        client = self._client()
        # Packed-word-sized keys under one order-1 subtree, enough to overflow.
        n = _ASYNC_PAYLOAD_CAP_BYTES // 16
        refs = [(morton_word("-311"), f"w{i:06d}") for i in range(n)]
        _invoke_lambda_sweep(client, "fn", "s3://b/store", refs, partitions=4)
        event = self._events(client)[0]
        assert "leaves" not in event and event["discover"] is True
        # The block still rides: the worker re-derives the WHOLE store's work
        # set and select_partition narrows it, so discovery stays per-partition.
        assert event["partition"] == {"index": 0, "of": 4}

    def test_a_bad_partition_count_is_refused_at_dispatch(self):
        from zagg.runner import _invoke_lambda_sweep

        client = self._client()
        refs = [(morton_word(d), None) for d in LEAVES]
        with pytest.raises(ValueError, match=r"splits a morton digit in half"):
            _invoke_lambda_sweep(client, "fn", "s3://b/store", refs, partitions=8)
        client.invoke.assert_not_called()


class TestSweepCLI:
    def test_partitions_flag_folds_one_partition_at_a_time(self, tmp_path, capsys):
        from zagg.sweep import main

        _store(tmp_path)
        _run_record(tmp_path)
        assert main([str(tmp_path), "--families", "stats", "--partitions", "4"]) == 0
        summaries = json.loads(capsys.readouterr().out)
        assert [s["partition"]["index"] for s in summaries] == [0, 1, 3]

    def test_default_prints_the_single_whole_tree_summary(self, tmp_path, capsys):
        from zagg.sweep import main

        _store(tmp_path)
        _run_record(tmp_path)
        assert main([str(tmp_path), "--families", "stats"]) == 0
        summary = json.loads(capsys.readouterr().out)
        assert "partition" not in summary and summary["n_leaves"] == len(LEAVES)

    @pytest.mark.parametrize("bad", ["8", "0", "-4", "3"])
    def test_a_bad_partition_count_is_refused_before_the_store_is_read(self, bad, monkeypatch):
        # From argv alone: no manifest read, no run-record LIST, no fold. The
        # store path is deliberately nonexistent — reaching it would raise
        # something else.
        from zagg import sweep as mod

        monkeypatch.setattr(
            mod, "discover_leaves", lambda *a, **k: pytest.fail("store read before validation")
        )
        with pytest.raises(ValueError, match=r"power of two|morton digit"):
            mod.main(["s3://nowhere/store", "--families", "stats", "--partitions", bad])


class TestHandlerForwardsThePartitionContract:
    """The worker transport must carry the ``partition``/``families`` blocks
    (issue #527).

    Every test above exercises :func:`run_sweep` directly, so a transport that
    drops the blocks is invisible to all of them: the contract holds, and the
    partition simply never reaches it. That is what shipped — the handler
    called ``run_sweep(store_path, leaves, store_kwargs=...)`` and nothing
    else — and it cost the CA campaign a 2,726-leaf sweep: on the ``discover``
    transport every one of 16 partition workers derived the WHOLE store's work
    set and died at the 900 s wall. Two shapes are pinned here: the call the
    handler makes, and the objects a partitioned invoke actually writes.
    """

    def _handler(self):
        from test_sweep import _handler_module

        return _handler_module()

    def _spy_on_run_sweep(self, monkeypatch):
        seen: dict = {}
        pristine = sweep_mod.run_sweep

        def spy(store_root, leaves, **kwargs):
            seen.update(kwargs)
            return pristine(store_root, leaves, **kwargs)

        monkeypatch.setattr(sweep_mod, "run_sweep", spy)
        return seen

    def test_partition_and_families_reach_run_sweep(self, tmp_path, monkeypatch):
        refs = _store(tmp_path)
        seen = self._spy_on_run_sweep(monkeypatch)
        block = {"index": 0, "of": 4}
        response = self._handler()._handle_sweep(
            {
                "mode": "sweep",
                "store_path": str(tmp_path),
                "leaves": [[int(word), window] for word, window in refs],
                "partition": block,
                "families": ["stats"],
            }
        )
        assert response["statusCode"] == 200
        assert seen["partition"] == block  # verbatim, not reconstructed
        assert seen["families"] == ["stats"]

    def test_absent_blocks_forward_as_none(self, tmp_path, monkeypatch):
        # An unpartitioned, family-less invoke must call run_sweep exactly as
        # the pre-#527 handler did: the fix adds reach, never new behaviour.
        refs = _store(tmp_path)
        seen = self._spy_on_run_sweep(monkeypatch)
        self._handler()._handle_sweep(
            {
                "mode": "sweep",
                "store_path": str(tmp_path),
                "leaves": [[int(word), window] for word, window in refs],
            }
        )
        assert seen["partition"] is None
        assert seen["families"] is None

    def test_a_partitioned_invoke_writes_only_its_own_subtree(self, tmp_path, monkeypatch):
        # The behavioural proof, end to end through the transport: partition 0
        # of 4 owns the "-31" subtree and nothing above the split order. With
        # the block dropped this wrote the whole tree from a partial work set
        # -- the coarse nodes above the split, from one partition's leaves.
        refs = _store(tmp_path)
        keys = _written(monkeypatch)
        response = self._handler()._handle_sweep(
            {
                "mode": "sweep",
                "store_path": str(tmp_path),
                "leaves": [[int(word), window] for word, window in refs],
                "partition": {"index": 0, "of": 4},
                "families": ["stats"],
            }
        )
        # _handle_sweep swallows every exception into a 500, so without this
        # the key set alone would go green on an invoke that raised after the
        # last rollup PUT (review finding, issue #527).
        assert response["statusCode"] == 200
        assert sorted(keys) == [
            "-3/1/1/stats.rollup.json",
            "-3/1/2/stats.rollup.json",
            "-3/1/stats.rollup.json",
        ]
