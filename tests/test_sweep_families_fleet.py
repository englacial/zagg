"""The run tail's families sweep over the fleet (issue #610, phase 2).

Before, both Lambda tails fired the families pass as ONE fire-and-forget
``mode="sweep"`` invoke whatever the run's size, and a run with more leaves
than one invoke can fold inside the 900 s wall got a dead invoke, no record
and a handle that returned success — the 2,726-leaf California tail. The
tail now sizes a ``4^k`` split from the leaves, fires the partitions, waits
for their records under the run's status prefix, fires the finisher, waits
for its record, and reports what landed. The barrier is the staged sweep's
status-channel poller, on deterministic ``families-*.json`` names.
"""

import json
import re
from pathlib import Path

import pytest

from zagg.client_transport import run_status_prefix
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME
from zagg.store import open_object_store, put_object
from zagg.sweep_fleet import families_record_name, run_families_sweep_fleet
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
    """A Lambda client whose ``mode="sweep"`` invokes land their status record LAZILY.

    ``invoke()`` only queues; the queue drains on the dispatcher's next LIST
    of the status prefix (the drained-client pattern of
    ``tests/test_sweep_stage_fleet.py``), and each drained event PUTs the copy
    the handler would — ``families_record_name(partition)`` under the event's
    ``records_from``. A record therefore stands only because a barrier
    polled for it, and the finisher's invoke asserts every partition's record
    already does, so a finisher fired before the partition barrier fails.
    ``drop`` names partition indexes (or ``"finisher"``) whose record never
    lands — a lost or timed-out invoke, what the barrier exists to report.
    """

    def __init__(self, monkeypatch, drop=()):
        import zagg.sweep_fleet

        self.drop, self.events, self.pending = set(drop), [], []
        present = zagg.sweep_fleet._present

        def drained(records_from, store_kwargs):
            while self.pending:
                self._land(self.pending.pop(0))
            return present(records_from, store_kwargs)

        monkeypatch.setattr(zagg.sweep_fleet, "_present", drained)

    def _land(self, event) -> None:
        part = event.get("partition")
        if ("finisher" if part is None else part["index"]) not in self.drop:
            store = open_object_store(event["records_from"])
            put_object(store, families_record_name(part), b"{}")

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803 (boto3 spelling)
        event = json.loads(Payload)
        if "partition" not in event:
            owed = {
                families_record_name(e["partition"])
                for e in self.events
                if e["partition"]["index"] not in self.drop
            }
            landed = {p.name for p in Path(event["records_from"]).glob("families-p*.json")}
            assert owed <= landed, "the finisher fired before the partition barrier"
        self.events.append(event)
        self.pending.append(event)
        return {"StatusCode": 202}


RUN_ID = "run-610"


def _root(tmp_path) -> str:
    root = tmp_path / "store"
    root.mkdir()
    put_object(open_object_store(str(root)), MANIFEST_NAME, b"{}")
    return str(root)


def _fleet(client, root, leaves, **kwargs):
    return run_families_sweep_fleet(
        client,
        "zagg-worker",
        root,
        leaves,
        store_kwargs={},
        run_id=RUN_ID,
        barrier_timeout_s=kwargs.pop("barrier_timeout_s", 5.0),
        poll_interval_s=0.01,
        **kwargs,
    )


#: Twelve order-4 leaves, three in each of the four order-1 subtrees of base 1.
LEAVES = [(morton_word(f"1{a}{b}11"), None) for a in "1234" for b in "123"]


class TestTail:
    def test_partitions_then_the_finisher_each_behind_its_barrier(self, tmp_path, monkeypatch):
        root = _root(tmp_path)
        client = _FakeLambda(monkeypatch)
        out = _fleet(client, root, LEAVES, target=3)
        prefix = run_status_prefix(root, RUN_ID)
        assert [e.get("partition") for e in client.events[:-1]] == [
            {"index": i, "of": 4} for i in sorted(partition_leaves(LEAVES, 4))
        ]
        assert {e["records_from"] for e in client.events} == {prefix}
        finisher = client.events[-1]
        assert "partition" not in finisher and len(finisher["leaves"]) == len(LEAVES)
        assert out == {
            "partitions": 4,
            "fired": 4,
            "landed": 4,
            "finisher": "ok",
            "run_id": RUN_ID,
            "records_from": prefix,
            "duration_s": out["duration_s"],
        }

    def test_a_small_run_is_one_invoke_and_one_barrier(self, tmp_path, monkeypatch):
        root = _root(tmp_path)
        client = _FakeLambda(monkeypatch)
        out = _fleet(client, root, LEAVES)
        assert [e.get("partition") for e in client.events] == [None]
        assert client.events[0]["leaves"] == [[int(k), w] for k, w in LEAVES]
        assert (out["partitions"], out["fired"], out["landed"], out["finisher"]) == (1, 1, 1, "ok")

    def test_a_partition_record_that_never_lands_is_reported(self, tmp_path, monkeypatch, caplog):
        import logging

        root = _root(tmp_path)
        client = _FakeLambda(monkeypatch, drop={1})
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = _fleet(client, root, LEAVES, target=3, barrier_timeout_s=0.2)
        assert (out["fired"], out["landed"], out["finisher"]) == (4, 3, "records_short")
        # The finisher still fired (it folds what is there) and its record landed.
        assert "partition" not in client.events[-1] and len(client.events) == 5
        assert "families sweep" in caplog.text and "records_short" in caplog.text

    def test_a_finisher_that_never_lands_is_timed_out(self, tmp_path, monkeypatch, caplog):
        import logging

        # An earlier attempt's records stand under THIS run's prefix, with the
        # names this tail awaits: they must not satisfy either barrier.
        root = _root(tmp_path)
        prefix = Path(run_status_prefix(root, RUN_ID))
        prefix.mkdir(parents=True)
        for name in (families_record_name(None), families_record_name({"index": 0, "of": 4})):
            (prefix / name).write_text("{}")
        client = _FakeLambda(monkeypatch, drop={0, "finisher"})
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = _fleet(client, root, LEAVES, target=3, barrier_timeout_s=0.2)
        assert (out["fired"], out["landed"], out["finisher"]) == (4, 3, "timed_out")
        assert "timed_out" in caplog.text

    def test_a_single_pass_that_never_lands_is_timed_out(self, tmp_path, monkeypatch):
        root = _root(tmp_path)
        client = _FakeLambda(monkeypatch, drop={"finisher"})
        out = _fleet(client, root, LEAVES, barrier_timeout_s=0.2)
        assert (out["partitions"], out["fired"], out["landed"], out["finisher"]) == (
            1,
            1,
            0,
            "timed_out",
        )

    def test_a_failed_stale_capture_is_logged_not_trusted(self, tmp_path, monkeypatch, caplog):
        """The capture's LIST faults: logged, and the barrier still runs on its own LISTs."""
        import logging

        import zagg.sweep_fleet

        root = _root(tmp_path)
        client = _FakeLambda(monkeypatch)
        drained, calls = zagg.sweep_fleet._present, []

        def first_faults(records_from, store_kwargs):
            calls.append(records_from)
            return (set(), False) if len(calls) == 1 else drained(records_from, store_kwargs)

        monkeypatch.setattr(zagg.sweep_fleet, "_present", first_faults)
        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = _fleet(client, root, LEAVES)
        assert "cannot capture" in caplog.text
        assert out["finisher"] == "ok" and out["records_from"] == run_status_prefix(root, RUN_ID)

    def test_the_handler_lands_the_copy_under_the_prefix(self, tmp_path):
        from test_sweep import _handler_module, _put_leaf, _write_manifest

        _write_manifest(tmp_path)
        _put_leaf(tmp_path, "-311")
        prefix = run_status_prefix(str(tmp_path), RUN_ID)
        event = {
            "mode": "sweep",
            "store_path": str(tmp_path),
            "leaves": [[morton_word("-311"), None]],
            "records_from": prefix,
        }
        body = json.loads(_handler_module()._handle_sweep(event)["body"])
        assert body["status_record"] == f"{prefix}/{families_record_name(None)}"
        # The SAME bytes as the store-root record, which still lands.
        copy = Path(prefix) / families_record_name(None)
        assert copy.read_bytes() == (tmp_path / body["record"]).read_bytes()


class TestRunnerSeam:
    def test_the_seam_is_fail_open(self, tmp_path, caplog):
        import logging

        from zagg.runner import _invoke_lambda_families_sweep

        class Boom:
            def invoke(self, **kwargs):
                raise RuntimeError("throttled")

        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            assert _invoke_lambda_families_sweep(Boom(), "fn", str(tmp_path), LEAVES) is None
        assert "families sweep dispatch failed" in caplog.text

    def test_a_failure_after_an_invoke_fired_is_the_outcome(self, tmp_path, caplog):
        import logging

        from zagg.runner import _invoke_lambda_families_sweep

        class SecondFails:
            calls = 0

            def invoke(self, **kwargs):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("TooManyRequestsException")
                return {"StatusCode": 202}

        with caplog.at_level(logging.WARNING, logger="zagg.sweep_fleet"):
            out = run_families_sweep_fleet(
                SecondFails(), "fn", str(tmp_path), LEAVES, store_kwargs={}, target=3
            )
        assert (out["partitions"], out["fired"], out["landed"]) == (4, 1, 0)
        assert out["finisher"] == "dispatch_failed" and "TooManyRequests" in out["error"]
        assert "dispatch_failed" in caplog.text
        # Through the seam it is the outcome, not the "nothing was fired" None.
        assert (
            _invoke_lambda_families_sweep(
                SecondFails(),
                "fn",
                str(tmp_path),
                [(morton_word(_spread("1", i, 9)), None) for i in range(2959)],
            )["finisher"]
            == "dispatch_failed"
        )

    def test_no_tail_fires_the_single_invoke_form(self):
        """The tails reach the fleet through the seam; what they carry is pinned
        behaviourally in ``test_runner.py`` and ``test_client.py``."""
        import inspect

        from zagg import client, runner

        for module in (runner, client):
            assert not re.search(
                r"^\s+(runner\.)?_invoke_lambda_sweep\(", inspect.getsource(module), re.M
            )
