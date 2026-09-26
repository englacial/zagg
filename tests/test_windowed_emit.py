"""Bulk per-shard multi-window emit (issue #586 phase 2).

One invoke per shard reads its granules once, bins observations into the
run's windows and emits one leaf per window — byte-identical to the
``(shard, window)`` fan-out of issue #246 — on both backends (the local
runner and the Lambda handler), with one D20 record per emitted leaf.
"""

import importlib.util
import json
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from zagg import hive
from zagg.config import default_config, get_windowing, get_windowing_unit, validate_config
from zagg.grids import HealpixGrid

HANDLER_PATH = Path(__file__).resolve().parent.parent / "deployment" / "aws" / "lambda_handler.py"
DAY = 86400.0


def _shard_word(order=6):
    from mortie import geo2mort

    return int(geo2mort(np.array([-78.5]), np.array([-132.0]), order=order)[0])


def _timed_rec(n, start, end):
    return {
        "id": f"g{n}",
        "s3": f"s3://bucket/granule{n}.h5",
        "https": f"https://h/g{n}.h5",
        "time_start": start,
        "time_end": end,
    }


class _FakeH5:
    def __init__(self, arrays):
        self._arrays = arrays

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                out[d] = self._arrays[d]
                continue
            arr = self._arrays[d["dataset"]]
            hs = d["hyperslice"]
            if hs:
                lo, hi = hs[0]
                arr = arr[lo:hi]
            out[d["dataset"]] = arr
        return out


def _cfg(**streaming):
    cfg = default_config("atl06")
    cfg.data_source = {
        "reader": "h5coro",
        "driver": "s3",
        "groups": ["g1"],
        "coordinates": {"latitude": "/lat", "longitude": "/lon"},
        "variables": {"h_li": "/h", "s_li": "/s", "delta_time": "/dt"},
    }
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"] = {"type": "healpix", "parent_order": 6, "child_order": 8}
    cfg.output["windowing"] = {
        "schedule": "yearly",
        "time_field": "delta_time",
        "epoch": "2018-01-01T00:00:00Z",
        "scale": "gps",
    }
    if streaming:
        cfg.aggregation["streaming"] = streaming
    validate_config(cfg)
    return cfg


def _h5(days):
    n = len(days)
    return _FakeH5(
        {
            "/lat": np.full(n, -78.5),
            "/lon": np.full(n, -132.0),
            "/h": np.arange(n, dtype=np.float32),
            "/s": np.ones(n, dtype=np.float32),
            "/dt": np.asarray(days, dtype=np.float64) * DAY,
        }
    )


def _fakes():
    """gA: one 2018 obs + three 2019; gB: three 2020; gC straddles 2019/2020."""
    return {
        "s3://bucket/granuleA.h5": _h5([300.0, 400.0, 401.0, 402.0]),
        "s3://bucket/granuleB.h5": _h5([800.0, 801.0, 802.0]),
        "s3://bucket/granuleC.h5": _h5([729.5, 729.75, 730.0, 730.25, 730.5]),
    }


def _records():
    return [
        _timed_rec("A", "2018-10-28T00:00:00Z", "2019-02-07T00:00:00Z"),
        _timed_rec("B", "2020-03-10T00:00:00Z", "2020-03-13T00:00:00Z"),
        _timed_rec("C", "2019-12-31T12:00:00Z", "2020-01-01T12:00:00Z"),
    ]


def _patch(monkeypatch, fakes=None):
    from zagg.index.hierarchical import HierarchicalIndex

    fakes = fakes if fakes is not None else _fakes()
    monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda path, *a, **k: fakes[path])
    monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
    monkeypatch.setattr("zagg.processing.worker.index_from_config", lambda cfg: HierarchicalIndex())


def _grid(cfg):
    return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)


def _bulk_unit(cfg, records=None):
    """The one shard unit the default dispatch builds: ``(urls, windows payloads)``."""
    from zagg.runner import _resolve_granule_entries, _shard_window_payloads, _shard_window_units

    ((shard, recs, windows),) = _shard_window_units(
        [(_shard_word(), records or _records())], get_windowing(cfg), None
    )
    return shard, _resolve_granule_entries(recs, "s3"), _shard_window_payloads(recs, windows, "s3")


def _run_fanout(monkeypatch, cfg, root, fakes=None, records=None, **kw):
    from zagg.runner import _windowed_units

    _patch(monkeypatch, fakes)
    out = {}
    for shard, recs, window in _windowed_units(
        [(_shard_word(), records or _records())], get_windowing(cfg), None
    ):
        urls = [r["s3"] for r in recs]
        out[window["label"]] = hive.process_and_write_hive(
            shard, urls, _grid(cfg), {}, root, cfg, store_kwargs={}, window=window, **kw
        )
    return out


def _run_bulk(monkeypatch, cfg, root, fakes=None, records=None, **kw):
    _patch(monkeypatch, fakes)
    shard, urls, windows = _bulk_unit(cfg, records)
    return hive.process_and_write_hive(
        shard, urls, _grid(cfg), {}, root, cfg, store_kwargs={}, windows=windows, **kw
    )


_CLOCKS = {"written_at", "timestamp"}


def _scrub(obj):
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items() if k not in _CLOCKS}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def _tree(root):
    """``{relative path: bytes-or-scrubbed-json}`` of every object under ``root``."""
    out = {}
    for p in sorted(Path(root).rglob("*")):
        if p.is_file():
            data = p.read_bytes()
            if p.suffix == ".json":
                data = _scrub(json.loads(data))
            out[str(p.relative_to(root))] = data
    return out


def _leaf_obs(root, shard, label, grid):
    import zarr

    from zagg.store import open_store

    leaf, _stamp = hive.resolve_leaf(hive.shard_leaf_path(root, shard, window=label))
    grp = zarr.open_group(open_store(leaf), path=grid.group_path, mode="r", zarr_format=3)
    return int(np.asarray(grp["count"][:]).sum())


# ── the knob ─────────────────────────────────────────────────────────────────


class TestUnitKnob:
    def test_default_is_shard_and_window_opts_back(self):
        cfg = _cfg()
        assert get_windowing_unit(cfg) == "shard"
        cfg.output["windowing"]["unit"] = "window"
        validate_config(cfg)
        assert get_windowing_unit(cfg) == "window"

    def test_bad_unit_rejected(self):
        cfg = _cfg()
        cfg.output["windowing"]["unit"] = "granule"
        with pytest.raises(ValueError, match="output.windowing.unit must be one of"):
            validate_config(cfg)

    def test_raster_rejects_the_key_and_reads_window(self):
        cfg = default_config("atl06")
        cfg.data_source = {
            "reader": "raster",
            "bands": {"red": {"asset": "red", "dtype": "uint16"}},
        }
        cfg.aggregation = {}
        cfg.output["grid"] = {"type": "healpix", "parent_order": 6, "child_order": 12}
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "yearly"}
        validate_config(cfg)
        assert get_windowing_unit(cfg) == "window"
        cfg.output["windowing"]["unit"] = "shard"
        with pytest.raises(ValueError, match="does not apply to raster"):
            validate_config(cfg)

    def test_unit_moves_neither_the_declaration_nor_the_hash(self):
        # The unit is how a run is DISPATCHED: leaves, manifest and D19
        # identity are the same either way.
        from zagg.semantics import semantic_hash

        shard, window = _cfg(), _cfg()
        window.output["windowing"]["unit"] = "window"
        assert get_windowing(shard) == get_windowing(window)
        assert "unit" not in get_windowing(shard)
        assert semantic_hash(shard) == semantic_hash(window)
        assert hive.build_manifest(
            _grid(shard), windowing=get_windowing(shard)
        ) == hive.build_manifest(_grid(window), windowing=get_windowing(window))


# ── dispatch units ───────────────────────────────────────────────────────────


class TestShardWindowUnits:
    def test_one_unit_per_shard_with_its_windows_and_granule_union(self):
        from zagg.runner import _shard_window_units, _unit_windows, _windowed_units

        cfg = _cfg()
        recs = _records()
        cells = [(11, recs), (22, [recs[1]]), (33, [])]
        units = _shard_window_units(cells, get_windowing(cfg), None)
        # Shard 33 has no window -> dropped, as the fan-out drops it.
        assert [u[0] for u in units] == [11, 22]
        shard, records, windows = units[0]
        assert records == recs  # incoming order kept; every record belongs to a window
        assert [p["label"] for p, _s in windows] == ["2018", "2019", "2020"]
        # The window subsets are the fan-out's own.
        fan = {
            w["label"]: s for _k, s, w in _windowed_units([(11, recs)], get_windowing(cfg), None)
        }
        assert {p["label"]: s for p, s in windows} == fan
        assert _unit_windows(units[0]) == (None, windows)
        assert _unit_windows((11, recs, fan_window := {"label": "2019"})) == (fan_window, None)
        assert _unit_windows((11, recs)) == (None, None)

    def test_explicit_schedule_drops_granules_outside_every_window(self):
        from zagg.runner import _shard_window_units

        cfg = _cfg()
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00Z",
            "windows": [{"label": "y2020", "start": "2020-01-01", "end": "2021-01-01"}],
        }
        validate_config(cfg)
        recs = _records()
        ((_shard, records, windows),) = _shard_window_units([(11, recs)], get_windowing(cfg), None)
        assert [r["id"] for r in records] == ["gB", "gC"]
        assert [(p["label"], [r["id"] for r in s]) for p, s in windows] == [("y2020", ["gB", "gC"])]

    def test_payloads_index_the_resolved_granule_list(self):
        from zagg.runner import (
            _resolve_granule_entries,
            _shard_window_payloads,
            _shard_window_units,
        )

        cfg = _cfg()
        recs = _records()
        # An href-less record is dropped by _resolve_granule_entries, so the
        # indices skip it exactly as the worker's list does.
        recs.insert(
            1,
            {
                "id": "gX",
                "s3": None,
                "https": None,
                "time_start": "2019-05-01T00:00:00Z",
                "time_end": "2019-05-02T00:00:00Z",
            },
        )
        ((_shard, records, windows),) = _shard_window_units([(11, recs)], get_windowing(cfg), None)
        payloads = _shard_window_payloads(records, windows, "s3")
        urls = _resolve_granule_entries(records, "s3")
        assert urls == [
            "s3://bucket/granuleA.h5",
            "s3://bucket/granuleB.h5",
            "s3://bucket/granuleC.h5",
        ]
        assert [(p["label"], p["granules"]) for p in payloads] == [
            ("2018", [0]),
            ("2019", [0, 2]),
            ("2020", [1, 2]),
        ]
        assert payloads[0]["start"] < payloads[0]["end"] and set(payloads[0]) == {
            "label",
            "start",
            "end",
            "granules",
        }


# ── the read-side bins ───────────────────────────────────────────────────────


class TestBinChunk:
    WINDOWS = [
        {"label": "a", "start": 0.0, "end": 10.0},
        {"label": "b", "start": 10.0, "end": 20.0},
    ]

    @pytest.mark.parametrize("carrier", ["pandas", "arrow"])
    def test_straddler_splits_on_the_half_open_boundary(self, carrier):
        from zagg.processing.windowed import bin_chunk

        data = {"t": np.array([1.0, 10.0, 9.99, 25.0]), "v": np.arange(4, dtype=np.int64)}
        if carrier == "arrow":
            from arro3.core import Table

            chunk = Table.from_pydict(data)
        else:
            chunk = pd.DataFrame(data)
        parts = dict(bin_chunk(chunk, "t", self.WINDOWS))
        assert list(parts) == ["a", "b"]  # 25.0 falls in no window

        def col(part, name):
            if carrier == "arrow":
                return part.column(name).combine_chunks().to_numpy().tolist()
            return part[name].tolist()

        assert col(parts["a"], "v") == [0, 2] and col(parts["a"], "t") == [1.0, 9.99]
        assert col(parts["b"], "v") == [1]  # ge start: the boundary instant is b's
        if carrier == "pandas":
            assert list(parts["a"].index) == [0, 1]

    def test_whole_chunk_is_yielded_uncopied(self):
        from zagg.processing.windowed import bin_chunk

        chunk = pd.DataFrame({"t": [1.0, 2.0], "v": [0, 1]})
        ((label, part),) = bin_chunk(chunk, "t", self.WINDOWS)
        assert label == "a" and part is chunk


class _FakeAgg:
    def __init__(self):
        self.reads, self.done, self.flushed, self.closed = [], 0, 0, False

    def add_read(self, chunk):
        self.reads.append(chunk)

    def granule_done(self):
        self.done += 1

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed = True


class TestWindowBins:
    def test_granule_done_follows_membership(self):
        from zagg.processing.windowed import WindowBins

        windows = [
            {"label": "a", "start": 0.0, "end": 10.0, "granules": [0]},
            {"label": "b", "start": 10.0, "end": 20.0, "granules": [0, 1]},
            {"label": "c", "start": 20.0, "end": 30.0},  # no membership: every granule
        ]
        bins = WindowBins(windows, "t", _FakeAgg)
        bins.add_reads([pd.DataFrame({"t": [1.0, 15.0]})], 0)
        bins.granule_done(0)
        bins.add_reads([pd.DataFrame({"t": [5.0]})], 1)  # not a member of a
        bins.granule_done(1)
        a, b, c = (bins.buffered[k] for k in "abc")
        assert (a.done, b.done, c.done) == (1, 2, 2)
        assert len(a.reads) == 1 and len(b.reads) == 1 and c.reads == []
        bins.flush()
        assert (a.flushed, b.flushed, c.flushed) == (1, 1, 1)
        bins.release("a")
        assert a.closed and "a" not in bins.buffered
        bins.close()
        assert b.closed and c.closed and bins.buffered == {}

    def test_pooled_bins_are_lists_per_window(self):
        from zagg.processing.windowed import WindowBins

        bins = WindowBins(TestBinChunk.WINDOWS, "t")
        bins.add_reads([pd.DataFrame({"t": [1.0, 15.0]}), pd.DataFrame({"t": [2.0]})], 0)
        bins.granule_done(0)
        reads, buffered = bins.sink("a")
        assert buffered is None and [len(r) for r in reads] == [1, 1]
        assert [len(r) for r in bins.sink("b")[0]] == [1]
        bins.release("a")
        assert bins.sink("a") == ([], None)

    def test_shared_tmp_cap_closes_the_largest_open_block(self):
        from zagg.processing.spill import SpillAggregator
        from zagg.processing.windowed import WindowBins

        class _Spill(SpillAggregator):
            def __init__(self):  # no /tmp, no config: just the cap's two seams
                self.tmp_dir, self._open, self.closed = None, 0, 0

            @property
            def open_block_bytes(self):
                return self._open

            def add_read(self, chunk):
                self._open += 100

            def granule_done(self):
                pass

            def close_block(self):
                self.closed += 1
                self._open = 0

        bins = WindowBins(TestBinChunk.WINDOWS, "t", _Spill)
        assert bins._tmp_cap is not None  # sized off the spill dir at construction
        bins._tmp_cap = 250
        bins.add_reads([pd.DataFrame({"t": [1.0, 15.0]})], 0)  # 100 each -> 200 open
        bins.granule_done(0)
        a, b = bins.buffered["a"], bins.buffered["b"]
        assert (a.closed, b.closed) == (0, 0)
        bins.add_reads([pd.DataFrame({"t": [1.0]})], 1)  # a: 200, b: 100 -> 300 >= cap
        bins.granule_done(1)
        assert (a.closed, b.closed) == (1, 0) and a.open_block_bytes == 0


# ── the worker seam, end to end on the real read path ────────────────────────


class TestBulkEndToEnd:
    @pytest.mark.parametrize("handoff", ["pandas", "arrow"])
    def test_bulk_leaves_are_byte_identical_to_the_fanout(self, monkeypatch, tmp_path, handoff):
        cfg = _cfg()
        cfg.aggregation["handoff"] = handoff
        fan_root, bulk_root = str(tmp_path / "fan"), str(tmp_path / "bulk")
        fan = _run_fanout(monkeypatch, cfg, fan_root, handoff=handoff)
        bulk = _run_bulk(monkeypatch, cfg, bulk_root, handoff=handoff)
        assert sorted(fan) == ["2018", "2019", "2020"]
        assert [m["window"] for m in bulk["windows"]] == ["2018", "2019", "2020"]
        # Every object of the shard node — the three leaves, their stamps,
        # coverage sidecars and granule-id siblings — is the same bytes.
        fan_tree, bulk_tree = _tree(fan_root), _tree(bulk_root)
        assert set(fan_tree) == set(bulk_tree) and len(fan_tree) > 3
        assert fan_tree == bulk_tree
        grid = _grid(cfg)
        assert [_leaf_obs(bulk_root, _shard_word(), w, grid) for w in ("2018", "2019", "2020")] == [
            1,
            5,
            6,
        ]

    def test_rows_bin_only_into_member_windows(self, monkeypatch, tmp_path):
        # Review finding (3): C without time_end collapses to a 2019 instant,
        # so C is no 2020 member; its 2020 rows must not reach the 2020 leaf
        # (the fan-out never reads C there, and the leaf's granule ids omit it).
        records = _records()
        del records[2]["time_end"]
        cfg = _cfg()
        fan_root, bulk_root = str(tmp_path / "fan"), str(tmp_path / "bulk")
        _run_fanout(monkeypatch, cfg, fan_root, records=records)
        bulk = _run_bulk(monkeypatch, cfg, bulk_root, records=records)
        assert [m["total_obs"] for m in bulk["windows"]] == [1, 5, 3]
        assert _tree(fan_root) == _tree(bulk_root)

    def test_bulk_under_spill_streaming_matches_the_fanout(self, monkeypatch, tmp_path):
        cfg = _cfg(mode="spill", buffer_granules=1)
        fan_root, bulk_root = str(tmp_path / "fan"), str(tmp_path / "bulk")
        _run_fanout(monkeypatch, cfg, fan_root)
        bulk = _run_bulk(monkeypatch, cfg, bulk_root)
        assert _tree(fan_root) == _tree(bulk_root)
        # One spill aggregator per window, flushed on the window's own granule
        # cadence; every one exact (no block closed).
        assert [m["phase_timings"]["spill_blocks_closed"] for m in bulk["windows"]] == [0, 0, 0]
        assert [m["phase_timings"]["spill_bytes"] > 0 for m in bulk["windows"]] == [True] * 3

    def test_records_contract(self, monkeypatch, tmp_path):
        meta = _run_bulk(monkeypatch, _cfg(), str(tmp_path / "bulk"))
        windows = meta["windows"]
        # One metadata dict per window, the shape a (shard, window) unit
        # returns, plus the bulk keys.
        assert [m["window"] for m in windows] == ["2018", "2019", "2020"]
        assert [m["unit_windows"] for m in windows] == [3, 3, 3]
        assert [m["granule_count"] for m in windows] == [1, 2, 2]  # A | A,C | B,C
        assert [m["total_obs"] for m in windows] == [1, 5, 6]
        assert [m["cells_with_data"] > 0 for m in windows] == [True] * 3
        assert windows[1]["time_range"] == [
            "2019-02-05T00:00:00+00:00",
            "2019-12-31T18:00:00+00:00",
        ]
        assert all(m["error"] is None and m["shard_key"] == meta["shard_key"] for m in windows)
        # The read phase is the shard's, once; the rest are per window.
        assert len({m["phase_timings"]["read"] for m in windows}) == 1
        assert all(
            {"index", "aggregate", "write", "hash"} <= set(m["phase_timings"]) for m in windows
        )
        # The invoke's duration rides every leaf's record.
        assert (
            len({m["duration_s"] for m in windows}) == 1
            and windows[0]["duration_s"] == meta["duration_s"]
        )
        # The shard meta: sums, the union, the read once, no error.
        assert (
            meta["total_obs"] == 12 and meta["granule_count"] == 3 and meta["files_processed"] == 3
        )
        assert meta["time_range"] == ["2018-10-28T00:00:00+00:00", "2020-03-13T00:00:00+00:00"]
        assert meta["error"] is None and "current" not in meta and "refused" not in meta
        assert meta["phase_timings"]["read"] == windows[0]["phase_timings"]["read"]

    def test_gate_skips_current_windows_and_the_shard_reads_once(self, monkeypatch, tmp_path):
        from zagg.telemetry import build_record, write_sidecar

        cfg = _cfg()
        root = str(tmp_path / "bulk")
        gate = dict(skip_if_current=True, sidecar_spec="morton-hive/2", run_id="r1")
        meta = _run_bulk(monkeypatch, cfg, root, **gate)
        shard, urls, windows = _bulk_unit(cfg)
        by_label = {w["label"]: w for w in windows}
        for m in meta["windows"]:
            rec = build_record(
                shard_key=shard,
                metadata=m,
                granule_ids=[urls[i] for i in by_label[m["window"]]["granules"]],
                window=m["window"],
            )
            write_sidecar(
                hive.shard_leaf_path(root, shard, window=m["window"]), rec, spec="morton-hive/2"
            )

        # Same inputs again: every window is current, the shard reads nothing.
        def _no_read(*a, **k):
            raise AssertionError("the shard must not be read when every window is current")

        monkeypatch.setattr("zagg.processing.worker._concat_and_group", _no_read)
        again = _run_bulk(monkeypatch, cfg, root, **{**gate, "run_id": "r2"})
        assert again["current"] is True
        assert [m["current"] for m in again["windows"]] == [True, True, True]
        assert [m["unit_windows"] for m in again["windows"]] == [3, 3, 3]
        # A new granule in 2020 only: 2018 and 2019 stay current, 2020 rewrites.
        monkeypatch.undo()
        fakes = {**_fakes(), "s3://bucket/granuleD.h5": _h5([900.0])}
        records = _records() + [_timed_rec("D", "2020-06-18T00:00:00Z", "2020-06-19T00:00:00Z")]
        third = _run_bulk(
            monkeypatch, cfg, root, fakes=fakes, records=records, **{**gate, "run_id": "r3"}
        )
        assert "current" not in third
        assert [m.get("current") for m in third["windows"]] == [True, True, None]
        assert third["windows"][2]["total_obs"] == 7 and third["windows"][2][
            "leaf_version"
        ].startswith("run-r3")
        assert third["total_obs"] == 7  # only what this invoke wrote

    def test_a_shard_that_reads_nothing_fails_every_window(self, monkeypatch, tmp_path):
        class _Dead:
            def readDatasets(self, datasets):  # noqa: N802
                raise OSError("boom")

        fakes = {url: _Dead() for url in _fakes()}
        meta = _run_bulk(monkeypatch, _cfg(), str(tmp_path / "bulk"), fakes=fakes)
        assert [m["error"].startswith("No data after filtering") for m in meta["windows"]] == [
            True
        ] * 3
        assert meta["error"].startswith("window 2018: No data after filtering") and meta[
            "error"
        ].endswith("(+2 more)")
        assert not (tmp_path / "bulk").exists()  # no leaf prefix for a no-data shard

    def test_an_empty_window_is_benign_beside_landed_ones(self, monkeypatch, tmp_path):
        # Review finding (1): A's span covers 2018 but its in-shard rows are
        # all 2019, so the 2018 window's sink is empty — a benign no-data
        # unit on the fan-out, never a failed shard.
        from zagg.dispatch import BENIGN_ERRORS

        fakes = {**_fakes(), "s3://bucket/granuleA.h5": _h5([400.0, 401.0, 402.0])}
        meta = _run_bulk(monkeypatch, _cfg(), str(tmp_path / "bulk"), fakes=fakes)
        assert [m["error"] for m in meta["windows"]] == ["No data after filtering", None, None]
        assert meta["error"] is None and meta["total_obs"] == 11
        # Every written window benign: the bare string, so the shard is no_data.
        far = {u: _h5([400.0]) for u in _fakes()}
        for h in far.values():
            h._arrays["/lat"] = np.full(1, 10.0)
        meta = _run_bulk(monkeypatch, _cfg(), str(tmp_path / "far"), fakes=far)
        assert meta["error"] in BENIGN_ERRORS

    def test_window_and_windows_are_exclusive(self, monkeypatch, tmp_path):
        with pytest.raises(ValueError, match="not both"):
            hive.process_and_write_hive(
                1,
                [],
                _grid(_cfg()),
                {},
                str(tmp_path),
                _cfg(),
                store_kwargs={},
                window={"label": "2019"},
                windows=[],
            )


# ── the dispatchers ──────────────────────────────────────────────────────────


def _catalog(tmp_path):
    shard = _shard_word()
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": 6,
            "child_order": 8,
            "layout": "fullsphere",
        },
        "shard_keys": [shard],
        "granules": [_records()],
    }
    p = tmp_path / "catalog.json"
    p.write_text(json.dumps(catalog))
    return str(p), shard


class TestLocalRunner:
    def test_default_dispatch_is_one_unit_per_shard_with_one_record_per_leaf(
        self, monkeypatch, tmp_path
    ):
        from zagg import runner
        from zagg.runner import agg
        from zagg.telemetry import read_sidecar

        cfg = _cfg()
        catalog_path, shard = _catalog(tmp_path)
        root = str(tmp_path / "out")
        calls = []
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            calls.append((int(shard_key), list(granule_urls), kw["windows"]))
            windows = [
                {
                    "shard_key": int(shard_key),
                    "window": w["label"],
                    "unit_windows": len(kw["windows"]),
                    "error": None,
                    "total_obs": 1,
                    "cells_with_data": 1,
                    "granule_count": len(w["granules"]),
                    "duration_s": 3.0,
                    "time_range": [
                        f"{w['label']}-03-01T00:00:00+00:00",
                        f"{w['label']}-11-01T00:00:00+00:00",
                    ],
                }
                for w in kw["windows"]
            ]
            return {
                "shard_key": int(shard_key),
                "error": None,
                "total_obs": 3,
                "duration_s": 3.0,
                "time_range": [windows[0]["time_range"][0], windows[-1]["time_range"][1]],
                "windows": windows,
            }

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        summary = agg(cfg, catalog=catalog_path, store=root, backend="local")
        # ONE call for the shard, carrying every window and its membership.
        ((key, urls, windows),) = calls
        assert key == shard and len(urls) == 3
        assert [(w["label"], w["granules"]) for w in windows] == [
            ("2018", [0]),
            ("2019", [0, 2]),
            ("2020", [1, 2]),
        ]
        assert summary["total_cells"] == 1 and summary["cells_with_data"] == 1
        # One D20 record per emitted leaf, each its own sidecar, one row each.
        for label in ("2018", "2019", "2020"):
            rec = read_sidecar(
                hive.shard_leaf_path(root, shard, window=label), spec="morton-hive/2"
            )
            assert (
                rec["window"] == label
                and rec["unit_windows"] == 3
                and rec["run_id"] == summary["results"][0]["stats"][0]["run_id"]
            )
        rows = pd.read_parquet(summary["run_stats_path"], engine="fastparquet")
        assert sorted(rows["window"]) == ["2018", "2019", "2020"]
        assert rows["unit_windows"].tolist() == [3, 3, 3] and rows["n_granules"].tolist() == [
            1,
            2,
            2,
        ]
        # The root summary unions the windows' ranges.
        env = hive.read_root_coverage(root)
        assert env["time_range"] == ["2018-03-01T00:00:00+00:00", "2020-11-01T00:00:00+00:00"]

    def test_identity_counts_and_refusals_read_per_window(self, tmp_path):
        from zagg.runner import _identity_counts, _write_refusals

        bulk = {
            "shard_key": 7,
            "refused": True,
            "windows": [
                {
                    "shard_key": 7,
                    "window": "2018",
                    "current": True,
                    "touched_objects": 2,
                    "touch_failed": 0,
                },
                {"shard_key": 7, "window": "2019", "refused": True, "missing_granules": ["g1"]},
            ],
        }
        counts = _identity_counts([bulk, {"shard_key": 8, "current": True, "touched_objects": 1}])
        assert (counts["cells_current"], counts["cells_refused"], counts["objects_touched"]) == (
            2,
            1,
            3,
        )
        path = _write_refusals(str(tmp_path), [bulk], counts, "rid", "h" * 64, {})
        manifest = json.loads(Path(path).read_text())
        assert [(u["shard_key"], u["window"]) for u in manifest["units"]] == [(7, "2019")]


class TestLambdaDispatch:
    def test_cell_event_carries_windows_only_when_given(self):
        from zagg.runner import _build_cell_event

        creds = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}
        windows = [{"label": "2019", "start": 365 * DAY, "end": 730 * DAY, "granules": [0]}]
        event = _build_cell_event(
            (0,), 1, 6, 8, ["s3://b/g.h5"], "s3://out", creds, config_dict=None, windows=windows
        )
        assert event["windows"] == windows and "window" not in event
        assert "windows" not in _build_cell_event(
            (0,), 1, 6, 8, ["s3://b/g.h5"], "s3://out", creds, config_dict=None
        )

    def test_invoke_forwards_windows(self):
        from zagg.runner import _invoke_lambda_cell

        payload_box = MagicMock()
        payload_box.read.return_value = json.dumps(
            {"statusCode": 200, "body": json.dumps({"total_obs": 1})}
        ).encode()
        client = MagicMock()
        client.invoke.return_value = {"Payload": payload_box, "FunctionError": None}
        windows = [{"label": "2019", "start": 365 * DAY, "end": 730 * DAY, "granules": [0]}]
        _invoke_lambda_cell(
            client,
            (0,),
            _shard_word(),
            6,
            8,
            ["s3://b/g1.h5"],
            "s3://out/store",
            {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
            function_name="process-shard",
            config_dict=None,
            windows=windows,
        )
        assert json.loads(client.invoke.call_args.kwargs["Payload"])["windows"] == windows

    def test_result_rows_and_sweep_leaves_read_the_record_list(self):
        from zagg.runner import _lambda_result_rows
        from zagg.sweep import dirt_only_leaves, leaves_from_stats_records
        from zagg.telemetry import build_record

        recs = [
            build_record(
                shard_key=7,
                metadata={"total_obs": 1, "unit_windows": 2, "duration_s": 2.0},
                window=w,
            )
            for w in ("2019", "2020")
        ]
        results = [
            {
                "status_code": 200,
                "shard_key": 7,
                "body": {"stats": recs, "windows": [{}, {}]},
                "retries": 1,
            },
            {"status_code": 200, "shard_key": 8, "body": {"stats": recs[0]}},
            {
                "status_code": 200,
                "shard_key": 9,
                "body": {
                    "current": True,
                    "windows": [
                        {"shard_key": 9, "window": "2019", "current": True, "icechunk_dirty": True},
                        {"shard_key": 9, "window": "2020", "current": True},
                    ],
                },
            },
        ]
        rows, inline = _lambda_result_rows(results, run_id="rid")
        assert [(r["shard_key"], r["window"], r["unit_windows"], r["retries"]) for r in rows] == [
            (7, "2019", 2, 1),
            (7, "2020", 2, 1),
            (7, "2019", 2, None),
        ]
        assert inline == []
        bodies = [r["body"] for r in results]
        assert leaves_from_stats_records([b.get("stats") for b in bodies]) == [
            (7, "2019"),
            (7, "2020"),
        ]
        assert dirt_only_leaves(bodies) == [(9, "2019")]

    def test_rows_from_status_reads_a_record_list(self, tmp_path):
        from zagg.telemetry import build_record, rows_from_status

        prefix = tmp_path / "status" / "rid"
        prefix.mkdir(parents=True)
        recs = [
            build_record(shard_key=7, metadata={"unit_windows": 2}, window=w) for w in ("a", "b")
        ]
        (prefix / "shard-7.json").write_text(json.dumps({"body": {"stats": recs}}))
        (prefix / "shard-8.json").write_text(json.dumps({"body": json.dumps({"stats": recs[0]})}))
        (prefix / "bad.json").write_text("{")
        rows = rows_from_status(str(prefix))
        assert sorted((r["shard_key"], r["window"]) for r in rows) == [(7, "a"), (7, "a"), (7, "b")]


# ── the record ───────────────────────────────────────────────────────────────


class TestRecord:
    def test_unit_windows_rides_flattens_and_merges(self):
        from zagg.telemetry import build_record, flatten_record, merge, stats_records, window_metas

        a = build_record(shard_key=1, metadata={"unit_windows": 3}, window="2019")
        b = build_record(shard_key=1, metadata={"unit_windows": 3}, window="2020")
        c = build_record(shard_key=1, metadata={})
        assert (a["unit_windows"], c["unit_windows"]) == (3, None)
        assert flatten_record(a)["unit_windows"] == 3
        assert merge([a, b])["unit_windows"] == 3 and merge([a, c])["unit_windows"] is None
        assert (
            stats_records(a) == [a]
            and stats_records([a, "x", b]) == [a, b]
            and stats_records(None) == []
        )
        shard = {"shard_key": 1, "windows": [{"window": "2019"}, None]}
        assert window_metas(shard) == [{"window": "2019"}] and window_metas({"shard_key": 1}) == [
            {"shard_key": 1}
        ]
        assert window_metas(None) == []


# ── the Lambda handler ───────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def handler_mod():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_586", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestHandler:
    def test_bulk_event_emits_every_window_with_one_record_each(
        self, handler_mod, monkeypatch, tmp_path
    ):
        from zagg.store import open_store
        from zagg.sweep import submap_key
        from zagg.telemetry import read_sidecar

        cfg = _cfg()
        shard, urls, windows = _bulk_unit(cfg)
        _patch(monkeypatch)
        records = _records()
        signature = {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": 6,
            "child_order": 8,
            "layout": "fullsphere",
        }
        event = {
            "shard_key": shard,
            "parent_order": 6,
            "child_order": 8,
            "granule_urls": urls,
            "store_path": str(tmp_path / "hive-out"),
            "s3_credentials": {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
            "config": asdict(cfg),
            "windows": windows,
            "run_id": "rid",
            "submap": {
                "grid_signature": signature,
                "metadata": {"short_name": "ATL06", "version": "007"},
                "granules": records,
            },
        }
        ctx = MagicMock()
        ctx.aws_request_id, ctx.function_name, ctx.memory_limit_in_mb = (
            "req-1",
            "process-shard",
            2048,
        )
        ctx.get_remaining_time_in_millis.return_value = 900_000
        resp = handler_mod._handle_process(event, ctx)
        assert resp["statusCode"] == 200, resp["body"]
        body = json.loads(resp["body"])
        # The body: the shard's totals plus one metadata per window, and the
        # records as a LIST — one per emitted leaf, each with the invoke's
        # memory telemetry.
        assert body["total_obs"] == 12 and [m["window"] for m in body["windows"]] == [
            "2018",
            "2019",
            "2020",
        ]
        assert [r["window"] for r in body["stats"]] == ["2018", "2019", "2020"]
        assert [r["unit_windows"] for r in body["stats"]] == [3, 3, 3]
        assert [r["n_granules"] for r in body["stats"]] == [1, 2, 2]
        assert [r["n_obs"] for r in body["stats"]] == [1, 5, 6]
        assert (
            len({r["max_memory_mb"] for r in body["stats"]}) == 1
            and body["stats"][0]["run_id"] == "rid"
        )
        assert body["time_range"] == ["2018-10-28T00:00:00+00:00", "2020-03-13T00:00:00+00:00"]
        grid = _grid(cfg)
        store = event["store_path"]
        for label, n_obs, ids in (
            ("2018", 1, ["gA"]),
            ("2019", 5, ["gA", "gC"]),
            ("2020", 6, ["gB", "gC"]),
        ):
            leaf = Path(hive.shard_leaf_path(store, shard, window=label))
            assert _leaf_obs(store, shard, label, grid) == n_obs
            assert hive.read_commit(open_store(str(leaf)))["window"] == label
            # Its own sidecar and its own sub-map (the window's granule subset).
            assert read_sidecar(str(leaf))["window"] == label
            submap = json.loads((leaf.parent / submap_key(leaf.name)).read_text())
            assert [[g["id"] for g in shard_granules] for shard_granules in submap["granules"]] == [
                ids
            ]
