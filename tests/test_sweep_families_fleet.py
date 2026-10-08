"""The run tail's families sweep over the fleet (issue #610, phase 2).

Before, both Lambda tails fired the families pass as ONE fire-and-forget
``mode="sweep"`` invoke whatever the run's size, and a run with more leaves
than one invoke can fold inside the 900 s wall got a dead invoke, no record
and a handle that returned success — the 2,726-leaf California tail. The
tail now sizes a ``4^k`` split from the leaves, fires the partitions, waits
for their store-root records, fires the finisher, waits for its record, and
reports what landed. The barrier is the staged sweep's status-channel poller,
pointed at the store root and keyed on the record's partition tag.
"""

import json
import re
from pathlib import Path

import pytest

from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME
from zagg.store import open_object_store, put_object
from zagg.sweep import _write_sweep_record
from zagg.sweep_fleet import run_families_sweep_fleet
from zagg.sweep_partition import FAMILIES_TARGET_LEAVES, families_partitions, partition_leaves


@pytest.fixture(autouse=True)
def _tail_families_sweep_lands_nothing():
    """Override ``conftest``'s stub by name: this file drives the real seam."""
    yield


def _decimal(prefix: str, i: int, digits: int) -> str:
    """``prefix`` + ``i`` in base 4 — consecutive ``i`` pack ONE subtree densely."""
    tail, k = "", i
    for _ in range(digits):
        tail, k = "1234"[k % 4] + tail, k // 4
    return prefix + tail


def _spread(prefix: str, i: int, digits: int) -> str:
    """``prefix`` + ``i``'s base-4 digits reversed — consecutive ``i`` spread over subtrees."""
    return prefix + _decimal("", i, digits)[::-1]


#: The v3 California store's 64-way split as the hand pass measured it
#: (issue #610): 2,959 order-9 leaves over 22 non-empty order-3 subtrees.
V3_SIZES = [1050, 752, 518, 412, 28, 22, 12] + [11] * 15


def _v3_keys() -> list:
    assert sum(V3_SIZES) == 2959
    keys = []
    for j, size in enumerate(V3_SIZES):
        prefix = _decimal("1", j, 3)  # one order-3 node per partition
        keys += [(morton_word(_decimal(prefix, i, 6)), None) for i in range(size)]
    return keys


def _largest(leaves, partitions: int) -> int:
    return max(len(bucket) for bucket in partition_leaves(leaves, partitions).values())


class TestSizing:
    def test_a_small_run_is_one_pass(self):
        leaves = [(morton_word(_decimal("1", i, 4)), None) for i in range(FAMILIES_TARGET_LEAVES)]
        assert families_partitions(leaves) == 1
        assert families_partitions([]) == 1

    def test_the_count_alone_sizes_an_even_spread(self):
        # 2,959 leaves / 128 -> 24 partitions -> the first power of four over
        # it, when the keys spread evenly (about 46 leaves per partition).
        leaves = [(morton_word(_spread("1", i, 9)), None) for i in range(2959)]
        assert families_partitions(leaves, target=4096) == 1
        assert _largest(leaves, 64) <= 128 and families_partitions(leaves) == 64

    def test_the_v3_shape_is_split_finer_than_the_count_says(self):
        """espg (issue #610): sizing is by the keys, not a fixed ``4^k``.

        ``ceil(log4(2959 / 128))`` says 64, whose largest partition holds
        1,050 leaves — past the wall at the observed rate. The split is refined
        until no partition is over the target.
        """
        leaves = _v3_keys()
        assert sorted(
            (len(b) for b in partition_leaves(leaves, 64).values()), reverse=True
        ) == sorted(V3_SIZES, reverse=True)
        chosen = families_partitions(leaves)
        assert chosen > 64 and _largest(leaves, chosen) <= FAMILIES_TARGET_LEAVES
        assert chosen == 4096  # 1024 still holds 256 in one partition

    def test_the_split_never_passes_the_leaves_own_order(self):
        # 200 leaves under ONE order-4 shard's windows: nothing finer than the
        # shard order exists, so the split stops there even though it is over.
        word = morton_word(_decimal("1", 0, 4))
        leaves = [(word, str(2000 + i)) for i in range(200)]
        assert families_partitions(leaves) == 4**4
        assert families_partitions(leaves, target=1) == 4**4


# ---------------------------------------------------------------------------


class _FakeLambda:
    """A Lambda client that lands the record each ``mode="sweep"`` invoke owes.

    ``drop`` names partition indexes (or ``"finisher"``) whose record never
    lands — a lost or timed-out invoke, what the barrier exists to report.
    """

    def __init__(self, root: str, drop=()):
        self.root, self.drop, self.events = root, set(drop), []

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803 (boto3 spelling)
        event = json.loads(Payload)
        self.events.append(event)
        part = event.get("partition")
        if ("finisher" if part is None else part["index"]) not in self.drop:
            summary = {"n_leaves": len(event.get("leaves") or []), "families": {}}
            if part is not None:
                summary["partition"] = part
            _write_sweep_record(open_object_store(self.root), summary)
        return {"StatusCode": 202}


def _root(tmp_path) -> str:
    root = tmp_path / "store"
    root.mkdir()
    put_object(open_object_store(str(root)), MANIFEST_NAME, b"{}")
    # Records standing before the tail fires: an earlier pass's, with the same
    # tags this tail will use. The barrier must not count them.
    for name in ("sweep_stats_20250101T000000Z.json", "sweep_stats_20250101T000000Z_p0of4.json"):
        (root / name).write_text("{}")
    return str(root)


def _fleet(client, root, leaves, **kwargs):
    return run_families_sweep_fleet(
        client,
        "zagg-worker",
        root,
        leaves,
        store_kwargs={},
        barrier_timeout_s=kwargs.pop("barrier_timeout_s", 5.0),
        poll_interval_s=0.01,
        **kwargs,
    )


#: Twelve order-4 leaves, three in each of the four order-1 subtrees of base 1.
LEAVES = [(morton_word(f"1{a}{b}11"), None) for a in "1234" for b in "123"]


class TestTail:
    def test_partitions_then_the_finisher_each_behind_its_barrier(self, tmp_path):
        root = _root(tmp_path)
        client = _FakeLambda(root)
        out = _fleet(client, root, LEAVES, target=3)
        n = out["partitions"]
        assert n == 4 and [e.get("partition") for e in client.events[:-1]] == [
            {"index": i, "of": 4} for i in sorted(partition_leaves(LEAVES, 4))
        ]
        finisher = client.events[-1]
        assert "partition" not in finisher and len(finisher["leaves"]) == len(LEAVES)
        assert out == {
            "partitions": 4,
            "fired": 4,
            "landed": 4,
            "finisher": "ok",
            "duration_s": out["duration_s"],
        }
        # The partition records landed before the finisher fired: the finisher
        # is the (n + 1)-th event, and n records stood at the root by then.
        tags = sorted(
            re.fullmatch(r"sweep_stats_\d{8}T\d{6}Z_(p\d+of4)\.json", p.name).group(1)
            for p in Path(root).glob("sweep_stats_*_p*of4.json")
            if not p.name.startswith("sweep_stats_2025")
        )
        assert tags == sorted(f"p{i}of4" for i in partition_leaves(LEAVES, 4))

    def test_a_small_run_is_one_invoke_and_one_barrier(self, tmp_path):
        root = _root(tmp_path)
        client = _FakeLambda(root)
        out = _fleet(client, root, LEAVES)
        assert [e.get("partition") for e in client.events] == [None]
        assert client.events[0]["leaves"] == [[int(k), w] for k, w in LEAVES]
        assert (out["partitions"], out["fired"], out["landed"], out["finisher"]) == (1, 1, 1, "ok")

    def test_a_partition_record_that_never_lands_is_reported(self, tmp_path, caplog):
        import logging

        root = _root(tmp_path)
        client = _FakeLambda(root, drop={1})
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = _fleet(client, root, LEAVES, target=3, barrier_timeout_s=0.2)
        assert (out["fired"], out["landed"], out["finisher"]) == (4, 3, "records_short")
        # The finisher still fired (it folds what is there) and its record landed.
        assert "partition" not in client.events[-1] and len(client.events) == 5
        assert "families sweep" in caplog.text and "records_short" in caplog.text

    def test_a_finisher_that_never_lands_is_timed_out(self, tmp_path, caplog):
        import logging

        root = _root(tmp_path)
        client = _FakeLambda(root, drop={"finisher"})
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = _fleet(client, root, LEAVES, target=3, barrier_timeout_s=0.2)
        assert (out["fired"], out["landed"], out["finisher"]) == (4, 4, "timed_out")
        assert "timed_out" in caplog.text

    def test_a_single_pass_that_never_lands_is_timed_out(self, tmp_path):
        root = _root(tmp_path)
        out = _fleet(_FakeLambda(root, drop={"finisher"}), root, LEAVES, barrier_timeout_s=0.2)
        assert (out["partitions"], out["fired"], out["landed"], out["finisher"]) == (
            1,
            1,
            0,
            "timed_out",
        )


class TestRunnerSeam:
    def test_the_seam_is_fail_open(self, caplog):
        import logging

        from zagg.runner import _invoke_lambda_families_sweep

        class Boom:
            def invoke(self, **kwargs):
                raise RuntimeError("throttled")

        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            assert _invoke_lambda_families_sweep(Boom(), "fn", "/nowhere", LEAVES) is None
        assert "families sweep dispatch failed" in caplog.text

    def test_both_lambda_tails_and_the_facade_go_through_the_seam(self):
        import inspect

        from zagg import client, runner

        src = inspect.getsource(runner)
        assert src.count("= _invoke_lambda_families_sweep(") == 2  # the raster and agg tails
        assert "handle.families_sweep = runner._invoke_lambda_families_sweep(" in inspect.getsource(
            client
        )
        # The single-invoke form is the fan-out primitive, no tail's call.
        assert not re.search(r"^\s+_invoke_lambda_sweep\(", src, re.M)
