"""One leaf temporal record per window leaf (issues #575, #586).

A leaf's ``temporal.toc`` (spec §10.6) is written by its worker before its
stamp, fail-closed, and by nothing else. On a windowed store every window
leaf is a leaf, so every window leaf carries its own record — counted from
that window's observations alone — whichever unit wrote it: the bulk
per-shard invoke that emits all the shard's windows from one read, or the
``(shard, window)`` fan-out. The two must write the same record, byte for
byte, in every fold regime; a record failure is its own window's failure;
and the families sweep composes the root section from the per-window
records, exact, with no shard left uncounted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import test_windowed_emit as emit
from test_sweep_stage_fleet import _handler_module

from zagg import hive, leaf_temporal, runner
from zagg.config import validate_config

WINDOWS = ("2018", "2019", "2020")
#: ``emit._dense_fakes``: 60 observations per day — A has one 2018 day and two
#: 2019 ones, B two 2020 days, C one day either side of the 2019/2020 line.
N_OBS = {"2018": 60, "2019": 180, "2020": 180}
#: The three fold regimes of issue #575: pooled, spill in one block, spill folded
#: across blocks (a one-byte threshold closes a block per flush).
REGIMES = {
    "pooled": {},
    "spill-single-block": {"mode": "spill", "buffer_granules": 1},
    "spill-multi-block": {"mode": "spill", "buffer_granules": 1, "block_bytes": 1},
}


def _cfg(regime="pooled", *, temporal=True):
    cfg = emit._digest_cfg(**REGIMES[regime])
    if temporal:
        cfg.aggregation["variables"]["h_tdigest"]["temporal"] = "per-centroid"
        validate_config(cfg)
    return cfg


def _records(root):
    """``{window: record}`` of every ``temporal.toc`` under ``root``."""
    out = {}
    for path in sorted(Path(root).rglob(leaf_temporal.LEAF_TEMPORAL_NAME)):
        leaf = next(p for p in path.parents if p.name.endswith(".zarr"))
        out[leaf.name.removesuffix(".zarr").split("_", 1)[1]] = json.loads(path.read_text())
    return out


def _tree(root):
    """``emit._tree`` with each record's write clock scrubbed (it is JSON too)."""
    tree = emit._tree(root)
    for key, value in tree.items():
        if key.endswith(leaf_temporal.LEAF_TEMPORAL_NAME):
            tree[key] = emit._scrub(json.loads(value))
    return tree


def _stamped(root, label):
    from zagg.store import open_store

    leaf = hive.shard_leaf_path(str(root), emit._shard_word(), window=label)
    return hive.read_commit(open_store(leaf, read_only=True)) is not None


class TestBulkAndFanOutAgree:
    @pytest.mark.parametrize("regime", list(REGIMES))
    def test_every_window_leaf_carries_the_same_record_either_way(
        self, monkeypatch, tmp_path, regime
    ):
        cfg = _cfg(regime)
        fan_root, bulk_root = str(tmp_path / "fan"), str(tmp_path / "bulk")
        emit._run_fanout(monkeypatch, cfg, fan_root, fakes=emit._dense_fakes())
        meta = emit._run_bulk(monkeypatch, cfg, bulk_root, fakes=emit._dense_fakes())
        assert meta["error"] is None
        fan, bulk = _records(fan_root), _records(bulk_root)
        assert sorted(bulk) == sorted(fan) == list(WINDOWS)
        for label in WINDOWS:
            # Byte for byte but for the write clock (``generated_at``).
            raw = {
                root: Path(hive.shard_leaf_path(root, emit._shard_word(), window=label))
                / leaf_temporal.LEAF_TEMPORAL_NAME
                for root in (fan_root, bulk_root)
            }
            assert emit._scrub(json.loads(raw[fan_root].read_text())) == emit._scrub(
                json.loads(raw[bulk_root].read_text())
            )
            record = bulk[label]
            # Counted from THIS window's observations alone, each once.
            assert (
                record["n_obs"]
                == N_OBS[label]
                == emit._leaf_obs(bulk_root, emit._shard_word(), label, emit._grid(cfg))
            )
            assert record["source"] == "worker" and record["fields"] == ["h_tdigest"]
            word, counts = leaf_temporal.leaf_temporal_contribution(record)
            assert int(counts.obs.sum()) == N_OBS[label]
        # ... and so is everything else under the shard node.
        assert _tree(fan_root) == _tree(bulk_root)

    def test_a_window_record_covers_its_own_window_only(self, monkeypatch, tmp_path):
        # The windows' envelope words are disjoint and in window order: an
        # accumulator shared across the bulk unit's windows would join them.
        from mortie import toc2time

        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, _cfg(), root, fakes=emit._dense_fakes())
        records = _records(root)
        spans = [tuple(int(x) for x in toc2time(int(records[w]["word"]))) for w in WINDOWS]
        assert all(lo <= hi for lo, hi in spans)
        assert spans[0][1] < spans[1][0] and spans[1][1] < spans[2][0], spans


class TestFailClosedPerWindow:
    def test_a_failed_record_write_is_that_windows_failure(self, monkeypatch, tmp_path):
        write = leaf_temporal.write_leaf_temporal

        def flaky(leaf_root, record, **kw):
            if "_2019.zarr" in str(leaf_root):
                raise OSError("PUT failed")
            return write(leaf_root, record, **kw)

        monkeypatch.setattr(leaf_temporal, "write_leaf_temporal", flaky)
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, _cfg(), root, fakes=emit._dense_fakes())
        errors = [m["error"] for m in meta["windows"]]
        assert errors[0] is None and errors[2] is None
        assert errors[1].startswith("RuntimeError: leaf temporal record for ")
        assert "failed to write (PUT failed)" in errors[1]
        assert meta["error"].startswith("window 2019: RuntimeError: leaf temporal record")
        # The failed window's leaf is unstamped debris; the others landed,
        # each with its record.
        assert [_stamped(root, label) for label in WINDOWS] == [True, False, True]
        assert sorted(_records(root)) == ["2018", "2020"]
        assert meta["total_obs"] == N_OBS["2018"] + N_OBS["2020"]

    def test_a_window_with_no_clocked_observation_stamps_without_a_record(
        self, monkeypatch, tmp_path
    ):
        # The first unit built is 2018's; its accumulator hears nothing, as
        # for a fold whose observations carry no clock. No record, no failure.
        made: list = []
        real = leaf_temporal.LeafTemporalAccumulator

        class _Accumulator(real):
            def __init__(self):
                super().__init__()
                made.append(self)

            def add_words(self, words):
                if self is not made[0]:
                    super().add_words(words)

        calls: list = []
        write = leaf_temporal.write_leaf_temporal
        monkeypatch.setattr(leaf_temporal, "LeafTemporalAccumulator", _Accumulator)
        monkeypatch.setattr(
            leaf_temporal,
            "write_leaf_temporal",
            lambda leaf_root, record, **kw: (
                calls.append(leaf_root),
                write(leaf_root, record, **kw),
            ),
        )
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, _cfg(), root, fakes=emit._dense_fakes())
        assert meta["error"] is None and len(made) == 3
        assert [_stamped(root, label) for label in WINDOWS] == [True, True, True]
        assert sorted(_records(root)) == ["2019", "2020"] and len(calls) == 2

    @pytest.mark.parametrize("regime", list(REGIMES))
    def test_a_non_temporal_config_is_untouched(self, monkeypatch, tmp_path, regime):
        called: list = []
        monkeypatch.setattr(leaf_temporal, "write_leaf_temporal", lambda *a, **k: called.append(a))
        monkeypatch.setattr(
            leaf_temporal,
            "LeafTemporalAccumulator",
            lambda *a, **k: pytest.fail("no accumulator without a temporal field"),
        )
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(
            monkeypatch, _cfg(regime, temporal=False), root, fakes=emit._dense_fakes()
        )
        assert meta["error"] is None and not called and _records(root) == {}


class TestSweepComposesFromTheRecords:
    def _run(self, monkeypatch, tmp_path, cfg):
        catalog_path, shard = emit._catalog(tmp_path)
        root = tmp_path / "out"
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
        emit._patch(monkeypatch, emit._dense_fakes())
        summary = runner.agg(cfg, catalog=catalog_path, store=str(root), backend="local")
        return root, shard, summary

    def test_the_root_section_is_the_sum_of_the_window_records(self, monkeypatch, tmp_path):
        from zagg.coverage_toc import coverage_toc_counts, coverage_toc_uncounted
        from zagg.grids.morton import morton_decimal

        root, shard, summary = self._run(monkeypatch, tmp_path, _cfg())
        assert summary["cells_error"] == 0
        records = _records(root)
        assert sorted(records) == list(WINDOWS)
        envelope = hive.read_root_coverage(str(root))
        counts = coverage_toc_counts(envelope)
        assert int(counts.obs.sum()) == sum(r["n_obs"] for r in records.values()) == 420
        # Every window leaf contributed its record: none left the shard uncounted.
        assert coverage_toc_uncounted(envelope) == 0
        assert list(envelope["temporal"]["shards"]) == [morton_decimal(shard)]

    def test_a_window_whose_record_failed_is_swept_and_covered_by_none(self, monkeypatch, tmp_path):
        # Phase A's rule decides what the tail sees: the failed window is in
        # neither the sweep nor the root section; the landed windows' records
        # are, exact.
        from zagg.coverage_toc import coverage_toc_counts, coverage_toc_uncounted

        write = leaf_temporal.write_leaf_temporal

        def flaky(leaf_root, record, **kw):
            if "_2019.zarr" in str(leaf_root):
                raise OSError("PUT failed")
            return write(leaf_root, record, **kw)

        monkeypatch.setattr(leaf_temporal, "write_leaf_temporal", flaky)
        root, _shard, summary = self._run(monkeypatch, tmp_path, _cfg())
        assert summary["cells_error"] == 1
        envelope = hive.read_root_coverage(str(root))
        assert int(coverage_toc_counts(envelope).obs.sum()) == N_OBS["2018"] + N_OBS["2020"]
        assert coverage_toc_uncounted(envelope) == 0
        assert envelope["time_range"][1].startswith("2020-")


class TestHandler:
    """The fleet path: the bulk event on a temporal config, through the real handler."""

    def _event(self, tmp_path):
        return emit._handler_event(_cfg(), tmp_path)

    def test_a_bulk_event_lands_a_record_in_every_window_leaf(self, monkeypatch, tmp_path):
        emit._patch(monkeypatch, emit._dense_fakes())
        event = self._event(tmp_path)
        response = emit._handle(_handler_module(), event)
        assert response["statusCode"] == 200, response["body"]
        body = json.loads(response["body"])
        assert [(r["window"], r["success"]) for r in body["stats"]] == [(w, True) for w in WINDOWS]
        records = _records(event["store_path"])
        assert {w: r["n_obs"] for w, r in records.items()} == N_OBS
        assert [r["n_obs"] for r in body["stats"]] == [N_OBS[w] for w in WINDOWS]

    def test_a_failed_record_write_is_reported_as_that_windows_error(self, monkeypatch, tmp_path):
        from zagg.telemetry import read_sidecar

        write = leaf_temporal.write_leaf_temporal

        def flaky(leaf_root, record, **kw):
            if "_2019.zarr" in str(leaf_root):
                raise OSError("PUT failed")
            return write(leaf_root, record, **kw)

        monkeypatch.setattr(leaf_temporal, "write_leaf_temporal", flaky)
        emit._patch(monkeypatch, emit._dense_fakes())
        event = self._event(tmp_path)
        response = emit._handle(_handler_module(), event)
        body = json.loads(response["body"])
        assert response["statusCode"] == 500
        assert body["error"].startswith("window 2019: RuntimeError: leaf temporal record for ")
        assert [(r["window"], r["success"]) for r in body["stats"]] == [
            ("2018", True),
            ("2019", False),
            ("2020", True),
        ]
        store, shard = event["store_path"], event["shard_key"]
        assert sorted(_records(store)) == ["2018", "2020"]
        assert [_stamped(store, label) for label in WINDOWS] == [True, False, True]
        # No D20 sidecar certifies the failed window, so a re-run redoes it.
        sidecars = [read_sidecar(hive.shard_leaf_path(store, shard, window=w)) for w in WINDOWS]
        assert [s is not None for s in sidecars] == [True, False, True]
