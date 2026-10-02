"""``pyramid_check`` on a windowed store (issue #586, PR question (10)).

The store under test is written by the real pipeline — the bulk per-shard
emit, the leaf columns, the staged sweep's ``(node, window)`` units and the
node close — over three shards and three yearly windows, in two runs (the
second appends), so the all-time fold exists in a superseded form too. The
harness must PASS it, and FAIL by node and window each of: a missing window
overview, a stale all-time fold, a window's overview built from another
window's leaves.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import numpy as np
import pytest
import test_windowed_emit as emit

from zagg import runner
from zagg.config import validate_config
from zagg.grids.morton import morton_decimal
from zagg.hive import MANIFEST_NAME
from zagg.pyramid_check import format_report, main, validate_pyramid
from zagg.pyramid_check_windowed import CHECKS_WINDOWED

WINDOWS = ("2018", "2019", "2020")
#: Two siblings under one order-5 node and a cousin that joins them at order 2.
SITES = ((-78.5, -132.0), (-79.0, -131.0), (-79.2, -132.0))


def _shard(site):
    from mortie import geo2mort

    return int(geo2mort(np.array([site[0]]), np.array([site[1]]), order=6)[0])


SHARDS = tuple(morton_decimal(_shard(site)) for site in SITES)


def _granule(site, days, seed):
    h5 = emit._dense_h5(days, seed)
    n = len(h5._arrays["/h"])
    h5._arrays["/lat"] = np.full(n, site[0])
    h5._arrays["/lon"] = np.full(n, site[1])
    return h5


def _sources(first_run: bool):
    """``(fakes, records per shard)``: granule A alone, or A + B + C (the append)."""
    fakes, records = {}, []
    for i, site in enumerate(SITES):
        plan = [
            ("A", [300.0, 400.0, 401.0], "2018-10-28T00:00:00Z", "2019-02-07T00:00:00Z"),
            ("B", [800.0, 801.0], "2020-03-10T00:00:00Z", "2020-03-13T00:00:00Z"),
            ("C", [729.5, 730.25], "2019-12-31T12:00:00Z", "2020-01-01T12:00:00Z"),
        ]
        recs = []
        for j, (tag, days, start, end) in enumerate(plan[: 1 if first_run else 3]):
            rec = emit._timed_rec(f"{tag}{i}", start, end)
            fakes[rec["s3"]] = _granule(site, days, seed=10 * i + j)
            recs.append(rec)
        records.append(recs)
    return fakes, records


def _run(monkeypatch, tmp_path, root, *, first_run, all_time=True):
    cfg = emit._digest_cfg()
    cfg.output["sweep"] = "stages"
    cfg.output["pyramid"] = {"overviews": 7, "all_time": all_time}
    validate_config(cfg)
    fakes, records = _sources(first_run)
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": 6,
            "child_order": 8,
            "layout": "fullsphere",
        },
        "shard_keys": [_shard(site) for site in SITES],
        "granules": records,
    }
    path = tmp_path / f"catalog-{'a' if first_run else 'abc'}.json"
    path.write_text(json.dumps(catalog))
    monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})
    emit._patch(monkeypatch, fakes)
    return runner.agg(cfg, catalog=str(path), store=str(root), backend="local")


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """The windowed store, and a copy of it as it stood after the first run."""
    tmp_path = tmp_path_factory.mktemp("windowed-pyramid")
    root, early = tmp_path / "store", tmp_path / "after-first-run"
    with pytest.MonkeyPatch.context() as monkeypatch:
        _run(monkeypatch, tmp_path, root, first_run=True)
        shutil.copytree(root, early)
        # Stamps resolve to one second and a leaf column's carries no run id
        # (spec §4.5), so an append inside the first run's second would read
        # as current to the staged sweep. No fleet rewrites a leaf that fast.
        time.sleep(1.1)
        _run(monkeypatch, tmp_path, root, first_run=False)
    return root, early


@pytest.fixture
def store(built, tmp_path):
    """A private copy a test may damage."""
    root = tmp_path / "store"
    shutil.copytree(built[0], root)
    return root


def _rel(node: str, name: str) -> str:
    """``-511`` + ``2019.zarr`` -> ``-5/1/1/2019.zarr``."""
    return "/".join([node[:2], *node[2:], name])


def _attrs(path: Path) -> dict:
    return json.loads((path / "zarr.json").read_text())["attributes"]


def _set_attrs(path: Path, edit) -> None:
    meta = json.loads((path / "zarr.json").read_text())
    edit(meta["attributes"])
    (path / "zarr.json").write_text(json.dumps(meta))


def _failed(report):
    return sorted(name for name, c in report["checks"].items() if c["status"] == "fail")


class TestACorrectStore:
    def test_every_check_passes_in_full_mode(self, built):
        report = validate_pyramid(str(built[0]), full=True, resweep=True)
        assert report["passed"], format_report(report)
        checks = report["checks"]
        # No packed field and no located field in this store: those two are
        # reported as not checked; everything else ran and passed.
        assert {name: c["status"] for name, c in checks.items()} == {
            **dict.fromkeys(CHECKS_WINDOWED, "pass"),
            "composition": "skip",
            "coordinates": "skip",
        }
        assert report["roster"] == {"source": "run records", "leaves": 9, "windows": 3}
        assert report["windows"] == list(WINDOWS)
        # Ladder nodes -5, -51, -511 (shared), then 2 / 2 / 2 at orders 3..5:
        # nine per window.
        assert (
            report["materialization"]["declared"],
            report["materialization"]["materialized"],
        ) == (27, 27)
        assert report["all_time_nodes"] == {"declared": 9, "materialized": 9, "missing": []}
        assert report["columns"] == {
            "declared": 9,
            "materialized": 9,
            "missing": [],
            "partial": [],
            "probe_errors": [],
        }
        assert "one ladder per window" in checks["declaration"]["detail"]
        assert "all-time fold declared" in checks["declaration"]["detail"]
        assert "k-way fold of their own per-window overviews" in checks["all_time"]["detail"]
        assert "no-op" in checks["idempotency"]["detail"]

    def test_the_sampled_default_passes_and_says_what_it_sampled(self, built):
        report = validate_pyramid(str(built[0]), sample_windows=2, sample_nodes=1, sample_cells=2)
        assert report["passed"], format_report(report)
        assert any("value checks ran on 2 of 3 window(s)" in w for w in report["warnings"])
        text = format_report(report)
        assert "[PASS] all_time" in text and "9 (shard, window) leaves in 3 window(s)" in text
        assert text.endswith("VERDICT: PASS")

    def test_the_cli_exits_zero(self, built, capsys):
        assert main([str(built[0])]) == 0
        assert "VERDICT: PASS" in capsys.readouterr().out

    @pytest.mark.parametrize("mode", ["list", "moc"])
    def test_every_roster_source_finds_the_same_leaves(self, built, mode):
        from zagg.hive import read_manifest
        from zagg.pyramid_check_windowed import window_roster

        manifest = read_manifest(str(built[0]))
        records, source = window_roster(str(built[0]), manifest, {}, "auto")
        assert source == "run records"
        assert records == sorted((d, w) for d in SHARDS for w in WINDOWS)
        assert window_roster(str(built[0]), manifest, {}, mode)[0] == records

    def test_the_first_run_alone_is_a_correct_store_too(self, built):
        # One granule per shard: windows 2018 and 2019, and their all-time fold.
        report = validate_pyramid(str(built[1]), full=True)
        assert report["passed"], format_report(report)
        assert report["windows"] == ["2018", "2019"]

    def test_without_the_declaration_the_all_time_check_is_not_applicable(
        self, tmp_path, monkeypatch
    ):
        root = tmp_path / "store"
        _run(monkeypatch, tmp_path, root, first_run=False, all_time=False)
        report = validate_pyramid(str(root), full=True)
        assert report["passed"], format_report(report)
        entry = report["checks"]["all_time"]
        assert entry["status"] == "skip" and entry["detail"].startswith("not applicable")
        assert "[SKIP] all_time" in format_report(report)
        assert "all-time fold not declared" in report["checks"]["declaration"]["detail"]


class TestDamage:
    def test_a_missing_window_overview_fails_by_node_and_window(self, store):
        shutil.rmtree(store / _rel("-511", "2019.zarr"))
        report = validate_pyramid(str(store), full=True)
        assert not report["passed"]
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail" and "missing: ['-511[2019]']" in entry["detail"]
        assert report["materialization"]["missing"] == ["-511[2019]"]
        # The node's all-time fold consumed three windows and the node holds two.
        assert any(
            m.startswith("-511[all]: STALE all-time fold")
            for m in report["checks"]["all_time"]["mismatches"]
        )

    def test_a_stale_all_time_fold_fails_by_node(self, built, store):
        # The node's all-time fold as the FIRST run left it: two windows, and
        # the leaves of 2019 as they were before the append.
        node = store / _rel("-5", "all.zarr")
        shutil.rmtree(node)
        shutil.copytree(built[1] / _rel("-5", "all.zarr"), node)
        report = validate_pyramid(str(store), full=True)
        assert _failed(report) == ["all_time"]
        (finding,) = report["checks"]["all_time"]["mismatches"]
        assert finding.startswith("-5[all]: STALE all-time fold — it folded 2 window overview(s)")
        assert "['2018', '2019', '2020']" in finding
        assert "[FAIL] all_time" in format_report(report)

    def test_a_fold_that_is_stale_only_by_value_fails_by_node(self, store):
        # Same sources on disk, same recorded provenance — one cell's count is
        # not the sum across the node's windows.
        import zarr

        from zagg.store import open_store

        group = zarr.open_group(open_store(str(store / _rel("-5", "all.zarr"))), path="1")
        counts = group["count"][:]
        cell = int(np.flatnonzero(counts)[0])
        counts[cell] += 1
        group["count"][:] = counts
        report = validate_pyramid(str(store), full=True)
        assert _failed(report) == ["all_time"]
        (finding,) = report["checks"]["all_time"]["mismatches"]
        assert finding.startswith(f"-5[{cell}]: stored ") and "!= fold" in finding

    def test_a_window_built_from_another_windows_leaves_fails_by_node_and_window(self, store):
        # 2019's overview under 2018's name, its attrs rewritten to match the
        # name: only the values can tell.
        target = store / _rel("-5112", "2018.zarr")
        shutil.rmtree(target)
        shutil.copytree(store / _rel("-5112", "2019.zarr"), target)
        _set_attrs(target, lambda a: a["zagg_overview"].update(window="2018"))
        report = validate_pyramid(str(store), full=True)
        assert not report["passed"]
        findings = report["checks"]["counts"]["mismatches"]
        assert findings and all(f.startswith("window 2018: -5112[") for f in findings)
        # ... and the all-time fold at that node no longer equals its windows' fold.
        assert report["checks"]["all_time"]["status"] == "fail"

    def test_an_overview_filed_under_the_wrong_window_fails_read_back(self, store):
        target = store / _rel("-5112", "2018.zarr")
        shutil.rmtree(target)
        shutil.copytree(store / _rel("-5112", "2019.zarr"), target)
        # Sampled: the window key is checked on every artifact, not a sample.
        report = validate_pyramid(str(store), sample_windows=1, sample_nodes=1, sample_cells=1)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any(
            m.startswith("-5112[2018]: 'zagg_overview' records window '2019'")
            for m in entry["mismatches"]
        )

    def test_a_missing_window_column_fails_by_leaf_and_window(self, store):
        leaf = SHARDS[0]
        shutil.rmtree(store / _rel(leaf[:-1], f"{leaf[-1]}/2019.pyramid.zarr"))
        report = validate_pyramid(str(store), full=True)
        entry = report["checks"]["columns"]
        assert entry["status"] == "fail" and f"missing: ['{leaf}[2019]']" in entry["detail"]

    @pytest.mark.parametrize(
        ("node", "key", "value", "expect"),
        [
            # node 5 is the gather level (cells 6 == the shard order): 2, not 3.
            ("-511233", "merges_from_raw", 3, "merges_from_raw 3 != 2"),
            ("-5", "merges_from_raw", 2, "merges_from_raw 2 != 3"),
            ("-5", "regime", "stage-gather", "regime 'stage-gather' != 'stage-merge'"),
            ("-5", "source_children", {"folded": 0, "missing": 0, "unreadable": 0}, "summed"),
            ("-5", "source_windows", None, "lacks the folded/missing/unreadable counters"),
            (
                "-5",
                "source_windows",
                {"folded": "x", "missing": 0, "unreadable": 0},
                "carries a non-integer counter",
            ),
            ("-5", "source_children", "bogus", "summed"),
            ("-5", "generation", {"n_leaves": 1, "run_ids": 7}, "STALE all-time fold"),
        ],
    )
    def test_the_all_time_provenance_is_held_to_the_spec(self, store, node, key, value, expect):
        _set_attrs(
            store / _rel(node, "all.zarr"), lambda a: a["zagg_overview"].__setitem__(key, value)
        )
        report = validate_pyramid(str(store), full=True)
        assert _failed(report) == ["all_time"]
        assert any(
            m.startswith(f"{node}[all]: ") and expect in m
            for m in report["checks"]["all_time"]["mismatches"]
        )

    def test_an_honestly_unreadable_window_passes_with_a_warning(self, store):
        # The packed-field rail fired on one committed window (§4.3): the
        # writer counts it unreadable, folds the other two, and sums
        # source_children over those; a re-sweep cannot clear it, so it is
        # not stale. Its values are not compared, and the report says so.
        def edit(a):
            sc = a["zagg_overview"]["source_children"]
            a["zagg_overview"]["source_windows"] = {"folded": 2, "missing": 0, "unreadable": 1}
            a["zagg_overview"]["source_children"] = {**sc, "folded": sc["folded"] - 1}

        _set_attrs(store / _rel("-5", "all.zarr"), edit)
        report = validate_pyramid(str(store), full=True)
        assert report["passed"], format_report(report)
        assert any(
            w.startswith("-5[all]: the all-time fold under-covers") and "not compared" in w
            for w in report["warnings"]
        )

    def test_an_unreadable_count_that_cannot_cover_the_node_is_stale(self, store):
        _set_attrs(
            store / _rel("-5", "all.zarr"),
            lambda a: a["zagg_overview"].update(
                source_windows={"folded": 1, "missing": 0, "unreadable": 1}
            ),
        )
        report = validate_pyramid(str(store), full=True)
        assert _failed(report) == ["all_time"]
        (finding,) = report["checks"]["all_time"]["mismatches"]
        assert finding.startswith("-5[all]: STALE all-time fold — it folded 1 window overview(s)")

    @pytest.mark.parametrize("torn", [True, False])
    def test_a_missing_window_is_honest_only_while_it_is_uncommitted(self, store, torn):
        from zagg.hive import COMMIT_ATTR

        folded = ["2018", "2020"] if torn else list(WINDOWS)
        if torn:  # the 2019 unit died before its stamp: the close counts it missing
            _set_attrs(store / _rel("-511", "2019.zarr"), lambda a: a.pop(COMMIT_ATTR))
        # The provenance the close records over the windows it folded (§4.4).
        attrs = [_attrs(store / _rel("-511", f"{w}.zarr")) for w in folded]
        blocks = [a["zagg_overview"] for a in attrs]
        generation = {
            "n_leaves": sum(b["generation"]["n_leaves"] for b in blocks),
            "max_leaf_timestamp": max(b["generation"]["max_leaf_timestamp"] for b in blocks),
            "run_ids": sorted(
                {r for b in blocks for r in b["generation"].get("run_ids") or []}
                | {a[COMMIT_ATTR]["run_id"] for a in attrs}
            ),
        }
        _set_attrs(
            store / _rel("-511", "all.zarr"),
            lambda a: a["zagg_overview"].update(
                source_windows={"folded": len(folded), "missing": 1, "unreadable": 0},
                generation=generation,
            ),
        )
        report = validate_pyramid(str(store), full=True)
        stale = [
            m
            for m in report["checks"]["all_time"].get("mismatches", [])
            if m.startswith("-511[all]: STALE")
        ]
        under = [w for w in report.get("warnings", []) if w.startswith("-511[all]: the all-time")]
        if torn:
            # Its absence fails materialization; the fold itself is honest.
            assert report["materialization"]["partial"] == ["-511[2019]"]
            assert not stale and under
        else:
            assert stale == [
                "-511[all]: STALE all-time fold — it records 1 missing window(s) and only 0 of "
                "the node's 3 are uncommitted now; re-sweep"
            ]

    @pytest.mark.parametrize(
        ("key", "value"),
        [("generation", "bogus"), ("generation", {"n_leaves": "x"}), ("source_children", [1])],
    )
    def test_a_malformed_window_overview_is_a_finding_not_a_traceback(self, store, key, value):
        # The all-time leg sums the per-window blocks; a corrupt one is named.
        _set_attrs(
            store / _rel("-5", "2019.zarr"),
            lambda a: a["zagg_overview"].__setitem__(key, value),
        )
        report = validate_pyramid(str(store), full=True)
        assert "-5[2019]: malformed 'zagg_overview'" in str(
            report["checks"]["all_time"]["mismatches"]
        )

    def test_a_missing_all_time_fold_fails_by_node(self, store):
        shutil.rmtree(store / _rel("-511", "all.zarr"))
        report = validate_pyramid(str(store), full=True)
        assert _failed(report) == ["all_time"]
        assert "['-511']" in report["checks"]["all_time"]["mismatches"][0]


class TestNotModelled:
    def test_a_windowed_v1_declaration_is_refused_by_name(self, store):
        manifest = json.loads((store / MANIFEST_NAME).read_text())
        fields = manifest["pyramid"]["overview"]["fields"]
        manifest["pyramid"] = {
            "spec": "zagg-pyramid/1",
            "overview": {"orders": [4], "spacing": 2, "all_time": False, "fields": fields},
        }
        (store / MANIFEST_NAME).write_text(json.dumps(manifest))
        report = validate_pyramid(str(store), full=True)
        entry = report["checks"]["declaration"]
        assert entry["status"] == "fail" and "windowed zagg-pyramid/1" in entry["detail"]
        # The leaves are still checked, and nothing else is reported as run.
        assert report["checks"]["coordinates"]["status"] in ("pass", "skip")
        assert report["checks"]["materialization"]["status"] == "skip"
