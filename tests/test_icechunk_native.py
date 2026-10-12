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
    """:func:`icechunk_native.stage_unit` against real leaf objects."""

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
        written = icechunk_native.stage_unit(
            session,
            order,
            row,
            [entry for entry in plan if entry["refs"]],
            prefix,
            store,
            etags=etags,
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

    def test_the_objects_are_read_in_gathered_windows_not_one_at_a_time(
        self, monkeypatch, cfg, tmp_path
    ):
        # These GETs are the commit's whole IO cost and a committing node
        # pays one for every overview object in its SUBTREE, not for one
        # array (issue #584 review), so a unit's reads are gathered rather
        # than issued serially — and gathered in fixed windows, which is what
        # keeps the memory peak at a window rather than the subtree. Pinned:
        # more than one window, none wider than ``READ_BATCH``, and every
        # object still copied.
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        entries = [entry for entry in plan if entry["refs"]]
        prefix = icechunk_refs.container_prefix(root)
        assert len(icechunk_native._tasks(entries, prefix)) > 2, "nothing to batch here"
        windows: list[int] = []
        real = icechunk_native._read

        def counted(store, tasks, **kwargs):
            windows.append(len(tasks))
            return real(store, tasks, **kwargs)

        monkeypatch.setattr(icechunk_native, "READ_BATCH", 2)
        monkeypatch.setattr(icechunk_native, "_read", counted)
        _repo, written = self._copy(root, entries)
        assert len(windows) > 1 and max(windows) <= 2, windows
        assert sum(windows) == len(icechunk_native._tasks(entries, prefix))
        assert written == sum(
            int(n) for entry in entries if entry["sharded"] for n in entry["lengths"]
        ) + sum(int(c[2]) for entry in entries if not entry["sharded"] for c in entry["chunks"])

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

    def test_a_shrinking_rewrite_inside_the_checksum_window_is_refused(
        self, monkeypatch, cfg, tmp_path
    ):
        # The §11.3 same-second hole, which the COPY can close where a
        # virtual ref structurally cannot: the object is re-folded shorter
        # and its mtime restored, so the ceiled-second checksum still
        # matches and only the object's own shard index shows the rewrite.
        # Without the index guard ``data[start : start + length]`` cannot
        # raise, so the last present chunk is sliced SHORT and committed —
        # silently, and permanently into a tag (issue #584 review).
        from pathlib import Path

        import obstore

        from zagg.store import open_object_store

        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        prefix = icechunk_refs.container_prefix(root)
        entry = next(e for e in plan if e["sharded"] and e["refs"])
        location = next(loc for loc in entry["locations"] if loc)
        path = Path(f"{root}/{location[len(prefix) :]}")
        offsets, lengths = np.asarray(entry["offsets"]), np.asarray(entry["lengths"])
        present = [i for i, loc in enumerate(entry["locations"]) if loc]
        last = max(present, key=lambda i: int(offsets[i]) + int(lengths[i]))
        short_by = min(30, int(lengths[last]) - 1)
        stat = path.stat()
        path.write_bytes(path.read_bytes()[: int(offsets[last]) + int(lengths[last]) - short_by])
        os.utime(path, (stat.st_atime, stat.st_mtime))
        # The checksum does NOT catch it — that is the hole the index closes.
        icechunk_native._vet_meta(
            location,
            obstore.head(open_object_store(root), location[len(prefix) :]),
            entry.get("checksum"),
            etags=False,
        )
        with pytest.raises(ValueError, match="a slice of it would be SHORT"):
            self._copy(root, [entry])

    def test_a_shard_index_that_disagrees_with_the_plan_is_refused(
        self, monkeypatch, cfg, tmp_path
    ):
        # The same guard on a real, intact object whose index simply is not
        # the one the plan read — the shape a re-fold that re-compressed an
        # inner chunk takes. Shortening a chunk the object does not END with
        # leaves the payload extent untouched, so it is the index comparison
        # itself that refuses, not the bounds arm above.
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        entry = next(e for e in plan if e["sharded"] and e["refs"] > 1)
        offsets, lengths = np.asarray(entry["offsets"]), np.asarray(entry["lengths"])
        first = min(
            (i for i, loc in enumerate(entry["locations"]) if loc),
            key=lambda i: int(offsets[i]) + int(lengths[i]),
        )
        stale = lengths.copy()
        stale[first] -= 1
        with pytest.raises(ValueError, match="no longer matches the ref plan"):
            self._copy(root, [{**entry, "lengths": stale}])

    def test_a_whole_object_chunk_whose_object_changed_size_is_refused(
        self, monkeypatch, cfg, tmp_path
    ):
        # The regular (unsharded) plan shape — every overview and coarse
        # column array — records the object's SIZE as the chunk length, so a
        # rewrite that changed the size is a short or long chunk that would
        # decode as garbage. The fixture is a real object whose recorded
        # length is one byte off, which is what such a rewrite looks like
        # (issue #584 review).
        grid, root, shard, plan = self._staged(monkeypatch, cfg, tmp_path)
        prefix = icechunk_refs.container_prefix(root)
        entry = next(e for e in plan if e["sharded"] and e["refs"])
        location = next(loc for loc in entry["locations"] if loc)
        size = os.path.getsize(f"{root}/{location[len(prefix) :]}")
        whole = {
            "path": "count",
            "sharded": False,
            "chunks": [("count/c/0", location, size - 1, None)],
            "refs": 1,
        }
        with pytest.raises(ValueError, match=f"is {size} bytes, the plan read {size - 1}"):
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

    def test_the_stage_rows_count_the_bytes_the_copy_wrote(self, monkeypatch, cfg, tmp_path):
        # The price of a tag that reads its own fold, measured rather than
        # asserted: the committing tuples' rows carry it, every row has the
        # key (``STAGE_COUNTS`` is pre-seeded), and the total is the overview
        # objects' chunk bytes — here, every byte of every overview object
        # the run wrote, since this geometry's overview arrays are one chunk.
        from pathlib import Path

        from test_icechunk_refs import _stage_rows

        _grid_, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=_shards(_grid(cfg), 6)
        )
        assert summary["cells_error"] == 0
        rows = _stage_rows(root)
        assert all("icechunk_native_bytes" in row for row in rows)
        copied = sum(row["icechunk_native_bytes"] for row in rows)
        objects = sum(
            obj.stat().st_size for obj in Path(root).rglob("all.zarr/**/c/*") if obj.is_file()
        )
        assert copied == objects > 0, (copied, objects)
        # A leaf's own record never carries it: its units name no overview
        # level, so the counter lives on the ladder's rows alone.
        for meta in summary["results"]:
            assert "native_bytes" not in meta["icechunk"], meta["icechunk"]

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
