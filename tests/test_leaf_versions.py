"""Versioned leaves behind a stable pointer stamp (issue #582 phase 3, spec §1.5).

The writer (version subgroup, version stamp, refs against the version's
objects, pointer swap last), the one reader rule (``resolve_leaf``), the
legacy path and its refusal, replacement without rewriting, the lifecycle
footprint, the coverage sidecar, and the ``output.leaf_versions`` knob.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest
import zarr
from test_icechunk_refs import _grid, _ladder_run, _shards, _write_leaf

from zagg import hive, icechunk_refs, lifecycle
from zagg.config import default_config

RUN_A, RUN_B = "a" * 32, "b" * 32


@pytest.fixture
def cfg():
    cfg = default_config("atl06", validate=False)
    cfg.output["store_layout"] = "hive"
    return cfg


def _mtimes(root: str) -> dict:
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            p = os.path.join(dirpath, name)
            out[os.path.relpath(p, root)] = (os.stat(p).st_mtime_ns, os.stat(p).st_size)
    return out


def _versions(leaf: str) -> list[str]:
    return sorted(n for n in os.listdir(leaf) if n.startswith(hive.VERSION_PREFIX))


class TestWriter:
    def test_versioned_write_lands_behind_the_pointer(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)
        leaf = hive.shard_leaf_path(root, shard)
        version = meta["leaf_version"]
        assert version.startswith(f"run-{RUN_A}-") and len(version) == len(f"run-{RUN_A}-") + 8
        assert _versions(leaf) == [version]
        # The stable root is a pointer stamp: the version's stamp plus
        # ``current``; its ``spec`` stays /1 (no new token, D23 owns /3).
        pointer = hive.read_commit(leaf)
        assert pointer["current"] == version and pointer["complete"] is True
        assert pointer["spec"] == hive.HIVE_SPEC
        stamp = hive.read_commit(f"{leaf}/{version}")
        assert "current" not in stamp
        assert {k: v for k, v in pointer.items() if k != "current"} == stamp
        assert (
            stamp["content_hashes"]["arrays"].keys() == pointer["content_hashes"]["arrays"].keys()
        )
        # No arrays at the root; the one reader rule finds them.
        assert not (Path(leaf) / "6").exists()
        data_path, seen = hive.resolve_leaf(leaf)
        assert data_path == f"{leaf}/{version}" and seen == pointer
        group = zarr.open_group(data_path, mode="r")["6"]
        assert group["count"].shape == (16,)
        # The sidecar siblings are unversioned, beside the stable root.
        from zagg.telemetry import granule_ids_path

        assert Path(granule_ids_path(leaf, None)).exists()

    def test_legacy_write_without_a_run_id(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=False)
        leaf = hive.shard_leaf_path(root, shard)
        assert "leaf_version" not in meta and _versions(leaf) == []
        stamp = hive.read_commit(leaf)
        assert "current" not in stamp and (Path(leaf) / "6" / "count").exists()
        assert hive.resolve_leaf(leaf) == (leaf, stamp)

    def test_the_knob_off_writes_a_legacy_leaf_with_a_run_id(self, monkeypatch, cfg, tmp_path):
        cfg.output["leaf_versions"] = False
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)
        leaf = hive.shard_leaf_path(root, shard)
        assert "leaf_version" not in meta and "current" not in hive.read_commit(leaf)

    def test_replacement_writes_a_new_version_and_moves_the_pointer(
        self, monkeypatch, cfg, tmp_path
    ):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        first = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)
        leaf = hive.shard_leaf_path(root, shard)
        v1 = first["leaf_version"]
        before = _mtimes(f"{leaf}/{v1}")
        second = _write_leaf(monkeypatch, grid, root, shard, fill=9.0, refs=False, run_id=RUN_B)
        v2 = second["leaf_version"]
        assert v2 != v1 and _versions(leaf) == sorted([v1, v2])
        assert hive.read_commit(leaf)["current"] == v2
        # The superseded version is byte-for-byte untouched.
        assert _mtimes(f"{leaf}/{v1}") == before
        old = zarr.open_group(f"{leaf}/{v1}", mode="r")["6"]["h_mean"][:]
        new = zarr.open_group(hive.resolve_leaf(leaf)[0], mode="r")["6"]["h_mean"][:]
        assert not np.array_equal(old, new)

    def test_a_same_run_retry_is_a_new_attempt(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        v1 = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)["leaf_version"]
        v2 = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)["leaf_version"]
        leaf = hive.shard_leaf_path(root, shard)
        assert v1 != v2 and v1.rsplit("-", 1)[0] == v2.rsplit("-", 1)[0] == f"run-{RUN_A}"
        assert _versions(leaf) == sorted([v1, v2])
        assert hive.read_commit(leaf)["current"] == v2

    def test_a_legacy_writer_refuses_a_versioned_root(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        v1 = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)["leaf_version"]
        leaf = hive.shard_leaf_path(root, shard)
        before = _mtimes(leaf)
        with pytest.raises(ValueError, match="is versioned"):
            _write_leaf(monkeypatch, grid, root, shard, refs=False)
        assert _mtimes(leaf) == before and hive.read_commit(leaf)["current"] == v1

    def test_a_versioned_writer_over_a_legacy_leaf_keeps_the_root_arrays(
        self, monkeypatch, cfg, tmp_path
    ):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        _write_leaf(monkeypatch, grid, root, shard, refs=False)
        leaf = hive.shard_leaf_path(root, shard)
        legacy = _mtimes(f"{leaf}/6")
        v = _write_leaf(monkeypatch, grid, root, shard, fill=3.0, refs=False, run_id=RUN_A)[
            "leaf_version"
        ]
        assert hive.read_commit(leaf)["current"] == v
        assert _mtimes(f"{leaf}/6") == legacy  # the converted leaf's root arrays stay
        assert hive.resolve_leaf(leaf)[0] == f"{leaf}/{v}"


class TestPointerRule:
    def test_leaf_data_path(self):
        assert hive.leaf_data_path("/s/1.zarr", None) == "/s/1.zarr"
        assert hive.leaf_data_path("/s/1.zarr", {"complete": True}) == "/s/1.zarr"
        assert hive.leaf_data_path("/s/1.zarr/", {"current": "run-x-1"}) == "/s/1.zarr/run-x-1"
        for bad in ("6", "../x", "run-x/y"):
            with pytest.raises(ValueError, match="invalid version"):
                hive.leaf_data_path("/s/1.zarr", {"current": bad})

    def test_version_names(self):
        assert hive.leaf_version_name("r", "abcd1234") == "run-r-abcd1234"
        a, b = hive.leaf_version_name("r"), hive.leaf_version_name("r")
        assert a != b and a.startswith("run-r-") and len(a) == len("run-r-") + 8

    def test_a_dangling_pointer_reads_as_debris(self, tmp_path):
        leaf = tmp_path / "1.zarr"
        group = zarr.open_group(str(leaf), mode="w")
        group.attrs[hive.COMMIT_ATTR] = {"complete": True, "current": "run-r-deadbeef"}
        data_path, stamp = hive.resolve_leaf(str(leaf))
        assert data_path == f"{leaf}/run-r-deadbeef" and stamp["current"] == "run-r-deadbeef"
        assert hive.read_commit(data_path) is None  # the version is absent: debris


class TestRefsAndHistory:
    def test_refs_point_into_the_version_and_history_survives_a_replacement(
        self, monkeypatch, cfg, tmp_path
    ):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_A, store_kwargs={})
        first = _write_leaf(monkeypatch, grid, root, shard, refs=True, run_id=RUN_A)
        v1, snap1 = first["leaf_version"], first["icechunk"]["snapshot"]
        leaf = hive.shard_leaf_path(root, shard)
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        leaf_name = leaf.rsplit("/", 1)[1]
        locations = [
            loc
            for loc in repo.readonly_session(snapshot_id=snap1).all_virtual_chunk_locations()
            if f"/{leaf_name}/" in loc
        ]
        assert locations and all(f"/{leaf_name}/{v1}/6/" in loc for loc in locations)
        old_values = zarr.open_group(repo.readonly_session(snapshot_id=snap1).store, mode="r")["6"][
            "h_mean"
        ][:]
        # Replace: the new run commits refs into ITS version; the old
        # snapshot keeps reading the old version's bytes (nothing rewritten).
        second = _write_leaf(monkeypatch, grid, root, shard, fill=9.0, refs=True, run_id=RUN_B)
        v2, snap2 = second["leaf_version"], second["icechunk"]["snapshot"]
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        assert all(
            f"/{leaf_name}/{v2}/6/" in loc
            for loc in repo.readonly_session(snapshot_id=snap2).all_virtual_chunk_locations()
            if f"/{leaf_name}/" in loc
        )
        again = zarr.open_group(repo.readonly_session(snapshot_id=snap1).store, mode="r")["6"][
            "h_mean"
        ][:]
        np.testing.assert_array_equal(again, old_values)
        new = zarr.open_group(repo.readonly_session("main").store, mode="r")["6"]["h_mean"][:]
        assert not np.array_equal(new, old_values)
        assert hive.read_commit(leaf)["current"] == v2


class TestLifecycle:
    def test_touch_covers_the_pointer_root_and_siblings_only(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        v = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id=RUN_A)["leaf_version"]
        leaf = hive.shard_leaf_path(root, shard)
        aged = 1_000_000_000
        for dirpath, _dirs, files in os.walk(os.path.dirname(leaf)):
            for name in files:
                os.utime(os.path.join(dirpath, name), ns=(aged, aged))
        counts = lifecycle.touch_current_unit(leaf, current=v)
        assert counts["failed"] == 0 and counts["touched"] >= 2
        version_tree = _mtimes(f"{leaf}/{v}")
        assert all(m == aged for m, _size in version_tree.values())  # never a version object
        assert os.stat(f"{leaf}/zarr.json").st_mtime_ns > aged  # the pointer root
        from zagg.telemetry import granule_ids_path

        assert os.stat(granule_ids_path(leaf, None)).st_mtime_ns > aged  # a sibling

    def test_a_current_versioned_unit_is_not_re_planned(self, monkeypatch, cfg, tmp_path):
        # An identical rerun skips every unit; its touch moved no version
        # object, so there is no refs re-plan and no dirt-only marker (spec
        # §11.6: that path is the legacy leaf's) — and the repo still reads.
        import time

        shards = _shards(_grid(cfg), 2)
        block = {"commit": "ladder"}
        grid, root, first = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block=block, shards=shards
        )
        versions = {s: _versions(hive.shard_leaf_path(root, s)) for s in shards}
        assert all(len(v) == 1 for v in versions.values())
        time.sleep(1.1)  # past the ceiled-second checksum of the first write
        _grid_, _root, again = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block=block, shards=shards
        )
        assert again["cells_current"] == 2
        for meta in again["results"]:
            assert meta["current"] is True and meta["touched_objects"] > 0
            # The gate hands the live version over (no second root read).
            assert meta["leaf_version"] == versions[meta["shard_key"]][0]
            assert "icechunk" not in meta and not meta.get("icechunk_dirty")
        for s in shards:
            assert _versions(hive.shard_leaf_path(root, s)) == versions[s]
        from test_icechunk_refs import _open

        group, _repo = _open(root)
        for shard in shards:
            (rank,) = grid.block_index(shard)
            leaf = zarr.open_group(hive.resolve_leaf(hive.shard_leaf_path(root, shard))[0])["6"]
            np.testing.assert_array_equal(
                group["6"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
            )


class TestSkipGate:
    def test_a_dangling_pointer_is_rewritten_not_skipped(self, monkeypatch, cfg, tmp_path):
        # The spec §1.5 rule on the WRITER's gate: a pointer naming a missing
        # version is debris, so a matching identity still rewrites (a fresh
        # version lands and the pointer moves) instead of skipping forever.
        import shutil

        shards = _shards(_grid(cfg), 1)
        block = {"commit": "ladder"}
        _g, root, _first = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block=block, shards=shards
        )
        leaf = hive.shard_leaf_path(root, shards[0])
        (gone,) = _versions(leaf)
        shutil.rmtree(f"{leaf}/{gone}")
        _g, _root, again = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block=block, shards=shards
        )
        assert again["cells_current"] == 0
        (meta,) = again["results"]
        assert not meta.get("current") and meta["identity"] == "unstamped-leaf"
        (fresh,) = _versions(leaf)
        assert fresh != gone and hive.read_commit(leaf)["current"] == fresh
        assert hive.read_commit(f"{leaf}/{fresh}") is not None

    def test_an_unstamped_version_is_debris_to_the_gate(self, cfg, tmp_path):
        from zagg.store import open_store

        leaf = str(tmp_path / "1.zarr")
        version = hive.leaf_version_name(RUN_A, "cafef00d")
        zarr.open_group(open_store(f"{leaf}/{version}"), mode="w")  # torn: no stamp
        hive.write_pointer_stamp(open_store(leaf), {"complete": True}, version)
        assert hive._leaf_is_committed(leaf, {}, 1) is None
        hive.stamp_commit(open_store(f"{leaf}/{version}"), cells_with_data=1, granule_count=1)
        assert hive._leaf_is_committed(leaf, {}, 1)["current"] == version


class TestKnob:
    def test_default_on_for_hive_off_otherwise(self, cfg):
        from zagg.config import get_leaf_versions

        assert get_leaf_versions(cfg) is True
        cfg.output["leaf_versions"] = False
        assert get_leaf_versions(cfg) is False
        cfg.output["store_layout"] = "flat"
        cfg.output.pop("leaf_versions")
        assert get_leaf_versions(cfg) is False

    def test_validation(self, cfg):
        from zagg.config import validate_config

        cfg.output["leaf_versions"] = "yes"
        with pytest.raises(ValueError, match="leaf_versions must be a boolean"):
            validate_config(cfg)
        cfg.output["leaf_versions"] = True
        cfg.output["store_layout"] = "flat"
        with pytest.raises(ValueError, match="requires output.store_layout: hive"):
            validate_config(cfg)

    def test_outside_the_semantic_core(self, cfg):
        from zagg.semantics import semantic_hash

        before = semantic_hash(cfg)
        cfg.output["leaf_versions"] = False
        assert semantic_hash(cfg) == before


class TestCoverageSidecar:
    def test_bitmap_resolves_into_the_current_version(self, cfg, tmp_path):
        # The sidecar sits beside the version's arrays; the root stamp is the
        # pointer the reader follows (one GET it already pays).
        from mortie import generate_morton_children

        from zagg.store import open_store

        grid = _grid(cfg)
        (shard,) = _shards(grid, 1)
        leaf = hive.shard_leaf_path(str(tmp_path / "store"), shard)
        version = hive.leaf_version_name(RUN_A, "cafef00d")
        words = np.asarray(generate_morton_children(int(shard), 6), dtype=np.uint64)[:5]
        bitmap = hive.encode_coverage_bitmap(shard, words, 6)
        data_path = f"{leaf}/{version}"
        zarr.open_group(open_store(data_path), mode="w")
        hive.write_coverage_sidecar(data_path, bitmap)
        stamp = hive.stamp_commit(
            open_store(data_path),
            cells_with_data=5,
            granule_count=1,
            coverage=hive.build_coverage(shard, words, 6, bitmap=bitmap),
        )
        hive.write_pointer_stamp(open_store(leaf), stamp, version)
        assert not (Path(leaf) / hive.COVERAGE_SIDECAR).exists()
        got = hive.read_coverage_bitmap(leaf)
        assert got is not None and len(got) == 5
        # The envelope-only entry point resolves through the root too.
        assert len(hive.read_coverage_bitmap(leaf, coverage=stamp["coverage"])) == 5
