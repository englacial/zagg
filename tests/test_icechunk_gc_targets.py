"""``tools/icechunk_gc_targets.py`` — the virtual-target collector (spec §11.4, issue #582)."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest
from test_icechunk_refs import _grid, _shards, _write_leaf

from zagg import hive, icechunk_refs
from zagg.config import default_config
from zagg.icechunk_finalize import finalize_repo

REPO = Path(__file__).parent.parent
sys.path.insert(0, str(REPO / "tools"))

import icechunk_gc_targets as tool  # noqa: E402  (tools/ is not installed)

RUNS = ["r%s" % c * 8 for c in "abcd"]


@pytest.fixture
def cfg():
    cfg = default_config("atl06", validate=False)
    cfg.output["store_layout"] = "hive"
    cfg.output["icechunk"] = {"commit": "leaf"}
    return cfg


def _versions(leaf: str) -> list[str]:
    return sorted(n for n in os.listdir(leaf) if n.startswith(hive.VERSION_PREFIX))


def _store(monkeypatch, cfg, tmp_path):
    """Three finalized runs of one versioned shard (K = 1) plus one legacy shard.

    After run C's finalize the only run tag is ``run-C``; run A's finalize
    snapshot expired with B's, so version A is referenced by nothing retained.
    """
    import zagg.sweep as sweep

    grid = _grid(cfg)
    root = str(tmp_path / "store")
    versioned, legacy = _shards(grid, 2)
    icechunk_refs.init_repo(root, grid, cfg, run_id=RUNS[0], store_kwargs={})
    versions = []
    for i, run in enumerate(RUNS[:3]):
        meta = _write_leaf(
            monkeypatch, grid, root, versioned, fill=float(i + 1), refs=True, run_id=run
        )
        assert "error" not in meta["icechunk"], meta["icechunk"]
        versions.append(meta["leaf_version"])
        time.sleep(1.1)  # the stamp clock is whole seconds: put the finalize in a later one
        finalize_repo(root, run_id=run, semantic_hash="h", retain_runs=1, store_kwargs={})
    _write_leaf(monkeypatch, grid, root, legacy, refs=True)  # legacy: no run_id
    monkeypatch.setattr(
        sweep, "discover_leaves", lambda store_root, **kw: [(versioned, None), (legacy, None)]
    )
    return grid, root, versioned, legacy, versions


class TestCollect:
    def test_dry_run_targets_the_unreferenced_superseded_version_only(
        self, monkeypatch, cfg, tmp_path
    ):
        _grid_, root, versioned, legacy, (va, vb, vc) = _store(monkeypatch, cfg, tmp_path)
        leaf = hive.shard_leaf_path(root, versioned)
        assert hive.read_commit(leaf)["current"] == vc
        report = tool.collect(root, store_kwargs={})
        assert report["leaves"] == 2 and report["newest_run_tag"] == f"run-{RUNS[2]}"
        # A: only expired snapshots name it -> superseded. B: run B's finalize
        # snapshot is retained (the K = 1 cutoff) -> referenced. C: current.
        assert [(t["version"], t["reason"]) for t in report["targets"]] == [(va, "superseded")]
        assert report["targets"][0]["leaf"] == leaf[len(root) + 1 :]
        assert report["bytes"] > 0 and report["deleted"] == 0
        assert _versions(leaf) == sorted([va, vb, vc])  # dry-run: nothing deleted
        assert _versions(hive.shard_leaf_path(root, legacy)) == []

    def test_execute_deletes_the_targets_and_nothing_else(self, monkeypatch, cfg, tmp_path):
        _grid_, root, versioned, legacy, (va, vb, vc) = _store(monkeypatch, cfg, tmp_path)
        leaf = hive.shard_leaf_path(root, versioned)
        legacy_leaf = hive.shard_leaf_path(root, legacy)
        before = {p for p in Path(legacy_leaf).rglob("*")}
        report = tool.collect(root, store_kwargs={}, execute=True)
        assert report["deleted"] == 1
        assert _versions(leaf) == sorted([vb, vc])
        assert not (Path(leaf) / va).exists()
        assert {p for p in Path(legacy_leaf).rglob("*")} == before
        # Idempotent: a second pass finds nothing.
        assert tool.collect(root, store_kwargs={})["targets"] == []

    def test_in_flight_and_dead_attempts(self, monkeypatch, cfg, tmp_path):
        _grid_, root, versioned, _legacy, (va, vb, vc) = _store(monkeypatch, cfg, tmp_path)
        grid = _grid(cfg)
        leaf = hive.shard_leaf_path(root, versioned)
        # Run D, not finalized: two attempts, the first superseded and
        # unreferenced (refs off) but NEWER than the newest run tag -> kept.
        cfg.output["icechunk"] = False
        d1 = _write_leaf(monkeypatch, grid, root, versioned, refs=None, run_id=RUNS[3])[
            "leaf_version"
        ]
        d2 = _write_leaf(monkeypatch, grid, root, versioned, refs=None, run_id=RUNS[3])[
            "leaf_version"
        ]
        assert hive.read_commit(leaf)["current"] == d2
        # A dead attempt (unstamped debris) of the finalized run C -> reclaimed;
        # one of a run whose tag does not exist -> kept.
        dead_c = f"{leaf}/run-{RUNS[2]}-dead0000"
        dead_x = f"{leaf}/run-nosuchrun-dead0000"
        for d in (dead_c, dead_x):
            os.makedirs(f"{d}/6/count/c")
            Path(f"{d}/6/count/c/0").write_bytes(b"x" * 10)
        report = tool.collect(root, store_kwargs={})
        got = {t["version"]: t["reason"] for t in report["targets"]}
        assert got == {va: "superseded", os.path.basename(dead_c): "dead-attempt"}
        assert d1 not in got and d2 not in got and os.path.basename(dead_x) not in got
        # Finalizing run D moves the newest tag past d1: now collectable.
        time.sleep(1.1)
        finalize_repo(root, run_id=RUNS[3], semantic_hash="h", retain_runs=1, store_kwargs={})
        got = {t["version"]: t["reason"] for t in tool.collect(root, store_kwargs={})["targets"]}
        assert got[d1] == "superseded" and d2 not in got

    def test_a_repo_less_store_is_refused(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        cfg.output["icechunk"] = False
        _write_leaf(monkeypatch, grid, root, shard, refs=None, run_id=RUNS[0])
        with pytest.raises(ValueError, match="not initialized"):
            tool.collect(root, store_kwargs={})


class TestCli:
    def test_dry_run_prints_and_anon_cannot_execute(self, monkeypatch, cfg, tmp_path, capsys):
        _grid_, root, _v, _l, (va, _vb, _vc) = _store(monkeypatch, cfg, tmp_path)
        assert tool.main([root]) == 0
        out = capsys.readouterr().out
        assert va in out and "dry-run" in out and "1 version(s)" in out
        with pytest.raises(SystemExit):
            tool.main([root, "--anon", "--execute"])
