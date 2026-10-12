"""Native chunks for the overview levels (issue #584 plan phase 3, spec §11.3).

A §4 overview object is rewritten in place by every later run that re-folds
it, so a virtual ref into one goes stale and a tag cut before the rewrite
cannot read that level. These tests pin the copy that replaces the ref: the
bytes land in the repo, the base and column levels stay virtual, and an
object that moved between the plan and the copy is refused rather than
copied blind.
"""

from __future__ import annotations

import os

import numpy as np
import pytest
import zarr
from test_icechunk_refs import _grid, _ladder_run, _leaf_arrays, _open, _shards, _write_leaf
from test_icechunk_rows import _init, _refs, _windowed

from zagg import hive, icechunk_native, icechunk_refs
from zagg.config import default_config

icechunk = pytest.importorskip("icechunk")

VIRTUAL = 2  # icechunk.ChunkType.virtual; native is 1 and a small chunk inlines as 3


@pytest.fixture
def cfg():
    return default_config("atl06", validate=False)


def _kinds(repo, level: str, array: str, session=None) -> set:
    """The chunk kinds one level array holds."""
    session = session or repo.readonly_session(icechunk_refs.BRANCH)
    return {kind for kind, _loc, _off, _len in _refs(session, f"{level}/{array}").values()}


class TestWhichLevelsAreNative:
    def test_the_overview_artifact_alone_is_copied_in(self):
        assert icechunk_native.native_level({"artifact": "overview"})
        for artifact in ("leaf", "column"):
            assert not icechunk_native.native_level({"artifact": artifact})
        assert not icechunk_native.native_level(None)
        assert not icechunk_native.native_level({})


class TestTheCopy:
    """:func:`icechunk_native.stage_entry` against real leaf objects."""

    def _staged(self, monkeypatch, cfg, tmp_path, *, row="2019"):
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        version = _write_leaf(monkeypatch, grid, root, shard, fill=1.0, refs=False, run_id="a")
        _init(root, grid, cfg, manifest, [row])
        plan = icechunk_refs.leaf_ref_plan(
            grid, shard, root, store_kwargs={}, version=version["leaf_version"]
        )
        assert plan, "the leaf wrote no object to copy"
        return grid, root, shard, plan

    def _copy(self, root, plan, *, row=0, order=6, etags=False):
        """Copy ``plan`` into the repo natively; the bytes written."""
        from zagg.store import open_object_store

        repo = icechunk_refs.open_repo(root, store_kwargs={})
        session = repo.writable_session(icechunk_refs.BRANCH)
        store = open_object_store(root, **{})
        prefix = icechunk_refs.container_prefix(root)
        written = sum(
            icechunk_native.stage_entry(session, order, row, entry, prefix, store, etags=etags)
            for entry in plan
            if entry["refs"]
        )
        session.commit("native copy")
        return repo, written

    def test_the_copied_chunks_read_back_as_the_objects_own_values(
        self, monkeypatch, cfg, tmp_path
    ):
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        repo, written = self._copy(root, plan)
        assert written == sum(
            int(n) for entry in plan if entry["sharded"] for n in entry["lengths"]
        ) + sum(int(c[2]) for entry in plan if not entry["sharded"] for c in entry["chunks"])
        leaf = _leaf_arrays(root, shard)
        group = zarr.open_group(repo.readonly_session(icechunk_refs.BRANCH).store, mode="r")
        (rank,) = grid.block_index(shard)
        span = slice(rank * 16, (rank + 1) * 16)
        np.testing.assert_array_equal(group["6"]["count"][0, span], leaf["count"][:])
        # Not a virtual ref: the bytes are the repo's own, so no location and
        # no checksum of the object they came from.
        assert VIRTUAL not in _kinds(repo, "6", "count")

    def test_an_object_that_moved_since_the_plan_is_refused(self, monkeypatch, cfg, tmp_path):
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        # The race the copy has to catch: another writer re-folds the object
        # between the plan's HEAD and the copy's GET. ``file://`` checksums
        # are the ceiled mtime (§11.3), so moving it two seconds on is a
        # rewrite as far as the guard is concerned.
        prefix = icechunk_refs.container_prefix(root)
        entry = next(e for e in plan if e["sharded"] and e["refs"])
        target = next(loc for loc in entry["locations"] if loc)[len(prefix) :]
        path = f"{root}/{target}"
        stat = os.stat(path)
        os.utime(path, (stat.st_atime + 2, stat.st_mtime + 2))
        with pytest.raises(ValueError, match="moved between the ref plan"):
            self._copy(root, [entry])

    def test_the_etag_form_is_the_one_every_object_store_takes(self, monkeypatch, cfg, tmp_path):
        # ``etags`` is True for every ``s3://`` container, so this is the arm
        # the fleet runs and the local tests above never reach (issue #584
        # review). ``obstore`` carries an ``e_tag`` on a local store too, so
        # the comparison itself is testable here.
        import obstore

        from zagg.store import open_object_store

        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        prefix = icechunk_refs.container_prefix(root)
        entry = next(e for e in plan if e["sharded"] and e["refs"])
        target = next(loc for loc in entry["locations"] if loc)[len(prefix) :]
        etag = obstore.head(open_object_store(root), target)["e_tag"]
        assert etag, "the object store recorded no ETag to compare"
        _repo, written = self._copy(root, [{**entry, "checksum": etag}], etags=True)
        assert written == sum(int(n) for n in entry["lengths"])
        with pytest.raises(ValueError, match="moved between the ref plan"):
            self._copy(root, [{**entry, "checksum": f"{etag}-stale"}], etags=True)

    def test_a_truncated_whole_object_chunk_is_refused(self, monkeypatch, cfg, tmp_path):
        # The regular (unsharded) plan shape carries the object's size as the
        # chunk length; a copy whose GET returns fewer bytes would write a
        # short chunk that decodes as garbage.
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        entry = next(e for e in plan if e["sharded"] and e["refs"])
        key, location, length, checksum = (
            "count/c/0",
            next(loc for loc in entry["locations"] if loc),
            10**9,
            None,
        )
        whole = {
            "path": "count",
            "sharded": False,
            "chunks": [(key, location, length, checksum)],
            "refs": 1,
        }
        with pytest.raises(ValueError, match="the plan read"):
            self._copy(root, [whole])


class TestTheLadderCommitsOverviewsNatively:
    def test_the_overview_levels_are_native_and_the_base_stays_virtual(
        self, monkeypatch, cfg, tmp_path
    ):
        # Plan phase 3: "the stage node's commit writes the overview levels'
        # chunks natively; the S3 overview objects stay, authoritative for
        # the ladder and for path readers, and the repo stops referencing
        # them."
        shards = _shards(_grid(cfg), 6)
        _grid_, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        assert summary["cells_error"] == 0
        block = icechunk_refs.read_block(root, store_kwargs={})
        group, repo = _open(root)
        seen = {"overview": 0, "leaf": 0}
        for level, declared in block["levels"].items():
            artifact = declared["artifact"]
            for array in group[level].array_keys():
                kinds = _kinds(repo, level, array)
                if not kinds:
                    continue
                seen[artifact] = seen.get(artifact, 0) + 1
                if artifact == "overview":
                    assert VIRTUAL not in kinds, f"{level}/{array} still references the object"
                else:
                    assert kinds == {VIRTUAL}, f"{level}/{array} is not virtual"
        assert seen["overview"] and seen["leaf"]

    def test_an_overview_rewritten_under_the_node_fails_that_node_open(
        self, monkeypatch, cfg, tmp_path
    ):
        # The copy's new failure surface (issue #584 review): the staleness
        # guard raises INSIDE the commit, which costs the node its whole
        # gather — the base refs with it — where a virtual ref would merely
        # have gone stale. What must hold is that the run itself is
        # unharmed: the repo is fail-open (D9), the hook counts the node and
        # the next sweep re-gathers it.
        from pathlib import Path

        from test_icechunk_refs import _stage_rows

        from zagg import icechunk_ladder

        real = icechunk_ladder.object_ref_plan

        def rewritten(grid, object_rel, rank, store_root, **kw):
            entries = real(grid, object_rel, rank, store_root, **kw)
            for path in Path(f"{store_root}/{object_rel}").rglob("*"):
                if path.is_file():  # another writer re-folding it after the plan
                    stat = path.stat()
                    os.utime(path, (stat.st_atime + 2, stat.st_mtime + 2))
            return entries

        monkeypatch.setattr(icechunk_ladder, "object_ref_plan", rewritten)
        _grid_, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=_shards(_grid(cfg), 2)
        )
        assert summary["cells_error"] == 0
        rows = _stage_rows(root)
        assert sum(row["icechunk_failed"] for row in rows) >= 1
        assert sum(row["icechunk_commits"] for row in rows) == 0

    def test_the_overview_objects_values_survive_the_copy(self, monkeypatch, cfg, tmp_path):
        from pathlib import Path

        from mortie import mort2healpix

        from zagg.grids.morton import morton_word

        shards = _shards(_grid(cfg), 6)
        _grid_, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        assert summary["cells_error"] == 0
        group, _repo = _open(root)
        manifest = hive.read_manifest(root)
        levels = {int(e["node"]): int(e["cells"][0]) for e in manifest["pyramid"]["overviews"]}
        read = 0
        for k, r in levels.items():
            for obj in sorted(Path(root).glob("/".join(["*"] * (k + 1)) + "/all.zarr")):
                node = "".join(obj.relative_to(root).parts[:-1])
                rank = mort2healpix(morton_word(node))[0]
                src = zarr.open_group(str(obj), mode="r")[str(r)]
                n = 4 ** (r - k)
                np.testing.assert_array_equal(
                    group[str(r)]["count"][0, rank * n : (rank + 1) * n], src["count"][:]
                )
                read += 1
        assert read, "no overview object was written"
