"""The row dimension of the Icechunk companion repo (issue #584, spec §11.2).

``zagg-icechunk/2``: every level array is ``(n_rows, n_cells)``. These tests
pin the row law (allocated once, by label, in order of first appearance,
never reordered), the root coordinate, the two-axis manifest split, the
refusal of a ``/1`` repo — and that a run adding a row leaves every earlier
ref byte-identical. A windowed store's repo is driven through ``init_repo``
and ``commit_units`` directly: the config guard still resolves the companion
off for windowed stores (§11.6), but the model must already hold them.
"""

from __future__ import annotations

import asyncio

import numpy as np
import pytest
import zarr
from test_icechunk_refs import (
    RUN_ID,
    _block_splits,
    _grid,
    _leaf_arrays,
    _open,
    _saved_row_splits,
    _saved_splits,
    _shards,
    _write_leaf,
)

from zagg import hive, icechunk_refs, icechunk_rows
from zagg.config import default_config
from zagg.icechunk_refs import ICECHUNK_ATTR

icechunk = pytest.importorskip("icechunk")

#: A yearly schedule in Unix seconds (the normalized ``get_windowing`` shape).
_YEARLY = {
    "schedule": "yearly",
    "time_field": "h_li",
    "epoch": "1970-01-01T00:00:00Z",
    "scale": "utc",
    "units": "seconds",
}
#: Unix seconds at each year's start: a yearly window is ``[_Y[y], _Y[y + 1])``.
_Y = {y: int(np.datetime64(f"{y}-01-01", "s").astype(int)) for y in range(2019, 2025)}


@pytest.fixture
def cfg():
    return default_config("atl06", validate=False)


def _windowed(cfg, tmp_path):
    """``(grid, root, manifest)`` for a yearly-windowed store's repo."""
    grid = _grid(cfg)
    return grid, str(tmp_path / "store"), hive.build_manifest(grid, windowing=_YEARLY)


def _init(root, grid, cfg, manifest, rows, run_id=RUN_ID):
    return icechunk_refs.init_repo(
        root, grid, cfg, run_id=run_id, store_kwargs={}, manifest=manifest, rows=rows
    )


def _refs(session, array_path: str) -> dict:
    """``{chunk coords: (kind, location, offset, length)}`` of one array's refs."""

    async def collect():
        out = {}
        batches = session.store.array_chunk_iterator(array_path)
        async for coords, kinds, paths, offsets, lengths, _inline in batches:
            for i in range(len(kinds)):
                key = tuple(int(c) for c in coords[i])
                out[key] = (int(kinds[i]), paths[i], int(offsets[i]), int(lengths[i]))
        return out

    return asyncio.run(collect())


def _fold_objects(root) -> dict:
    """``{key: sha256}`` of every all-time overview chunk object under ``root``.

    The bytes, not the mtime: what a virtual ref's checksum catches is a
    rewrite of the object's CONTENT, and a rewrite inside one second is
    exactly the case a timestamp proxy would miss (spec §11.3).
    """
    import hashlib
    from pathlib import Path

    return {
        str(obj): hashlib.sha256(obj.read_bytes()).hexdigest()
        for obj in sorted(Path(root).rglob("all.zarr/**/c/*"))
        if obj.is_file()
    }


class TestRunRows:
    def test_an_unwindowed_run_writes_the_all_row(self):
        assert icechunk_rows.run_rows(None) == ["all"]
        assert icechunk_rows.run_rows(None, ["2019"]) == ["all"]
        assert icechunk_rows.ALL_ROW == "all"

    def test_a_windowed_run_writes_its_labels_in_first_appearance_order(self):
        assert icechunk_rows.run_rows(_YEARLY, ["2020", "2019", "2020"]) == ["2020", "2019"]
        assert icechunk_rows.run_rows(_YEARLY) == []

    def test_store_rows_vets_the_labels_against_the_store(self):
        assert icechunk_rows.store_rows(None, None) == ["all"]
        assert icechunk_rows.store_rows(None, ["all"]) == ["all"]
        assert icechunk_rows.store_rows(_YEARLY, ["2020", "all", "2020"]) == ["2020", "all"]
        # A windowed store's ``all`` row is allocated only when named, and an
        # init naming nothing is refused rather than making a zero-row repo.
        assert icechunk_rows.store_rows(_YEARLY, ["2020"]) == ["2020"]
        for nothing in (None, []):
            with pytest.raises(ValueError, match="names no row label"):
                icechunk_rows.store_rows(_YEARLY, nothing)
        with pytest.raises(ValueError, match="an unwindowed store has the one row 'all'"):
            icechunk_rows.store_rows(None, ["2019"])
        with pytest.raises(ValueError, match="grammar"):
            icechunk_rows.store_rows(_YEARLY, ["201906"])  # not a yearly label


class TestRowBounds:
    def test_a_window_is_its_half_open_range_in_the_manifests_encoding(self):
        assert icechunk_rows.row_bounds("2019", _YEARLY) == (_Y[2019], _Y[2020])
        days = {**_YEARLY, "units": "days", "epoch": "2018-01-01T00:00:00Z"}
        assert icechunk_rows.row_bounds("2019", days) == (365, 730)
        assert icechunk_rows.row_bounds("2020", days) == (730, 1096)  # a leap year

    def test_the_all_row_has_no_bound(self):
        assert icechunk_rows.row_bounds("all", None) is None
        assert icechunk_rows.row_bounds("all", _YEARLY) is None

    def test_a_boundary_off_the_unit_grid_is_refused_not_rounded(self):
        explicit = {
            **_YEARLY,
            "schedule": "explicit",
            "units": "days",
            "windows": [
                {"label": "w1", "start": "2020-01-01T12:00:00Z", "end": "2020-01-02T00:00:00Z"}
            ],
        }
        with pytest.raises(ValueError, match="not a whole number of days"):
            icechunk_rows.row_bounds("w1", explicit)


class TestAllocation:
    def test_init_allocates_the_runs_rows_and_their_coordinate(self, cfg, tmp_path):
        grid, root, manifest = _windowed(cfg, tmp_path)
        out = _init(root, grid, cfg, manifest, ["2020", "2019"])
        assert out["created"] is True and out["rows"] == ["2020", "2019"]
        group, _repo = _open(root)
        assert group.attrs[ICECHUNK_ATTR]["rows"] == ["2020", "2019"]
        # Every level array has one row per label; the row is the label's
        # position in the block's list — order of first appearance, not sorted.
        for level in out["ladder"]:
            for _name, arr in group[str(level)].arrays():
                assert arr.shape[0] == 2 and arr.chunks[0] == 1
        start, end = group["window_start"], group["window_end"]
        assert start[:].tolist() == [_Y[2020], _Y[2019]]
        assert end[:].tolist() == [_Y[2021], _Y[2020]]
        # The manifest's time encoding, CF-shaped, on both coordinate arrays.
        want = {
            "units": "seconds since 1970-01-01T00:00:00Z",
            "calendar": "proleptic_gregorian",
            "scale": "utc",
        }
        assert dict(start.attrs) == dict(end.attrs) == want

    def test_a_windowed_init_naming_no_row_creates_no_repo(self, cfg, tmp_path):
        grid, root, manifest = _windowed(cfg, tmp_path)
        with pytest.raises(ValueError, match="names no row label"):
            _init(root, grid, cfg, manifest, None)
        assert icechunk_refs.read_block(root, store_kwargs={}) is None

    def test_a_later_run_appends_its_new_rows_and_never_reorders(self, cfg, tmp_path):
        grid, root, manifest = _windowed(cfg, tmp_path)
        _init(root, grid, cfg, manifest, ["2020"])
        # A window BEFORE the existing one still lands after it (the row law),
        # and a label the repo already has is a no-op.
        out = _init(root, grid, cfg, manifest, ["2019", "2020", "all"], run_id="run-2")
        assert out["created"] is False and out["rows"] == ["2020", "2019", "all"]
        group, repo = _open(root)
        assert group.attrs[ICECHUNK_ATTR]["rows"] == ["2020", "2019", "all"]
        assert group["6"]["count"].shape == (3, 12 * 4**6)
        assert group["3"]["count"].shape[0] == 3  # every level, not the base alone
        # The ``all`` fold has no bound: both ends read the fill.
        fill = icechunk_rows.ROW_FILL
        assert group["window_start"][:].tolist() == [_Y[2020], _Y[2019], fill]
        assert group["window_end"][:].tolist() == [_Y[2021], _Y[2020], fill]
        # The allocation rides the run's own init commit: no extra commit.
        assert [s.message for s in repo.ancestry(branch="main")][:2] == [
            "init run-2",
            f"init {RUN_ID}",
        ]
        # Idempotent: the same labels again change nothing but the empty init.
        again = _init(root, grid, cfg, manifest, ["2019", "all"], run_id="run-3")
        assert again["rows"] == out["rows"]
        assert _open(root)[0]["6"]["count"].shape == (3, 12 * 4**6)

    def test_an_unwindowed_store_has_the_all_row_and_no_other(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        out = icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        assert out["rows"] == ["all"]
        with pytest.raises(ValueError, match="an unwindowed store has the one row 'all'"):
            icechunk_refs.init_repo(root, grid, cfg, run_id="run-2", store_kwargs={}, rows=["2019"])
        assert icechunk_refs.read_block(root, store_kwargs={})["rows"] == ["all"]

    def test_two_runs_allocating_at_once_both_land(self, cfg, tmp_path):
        # Both sessions resize every array, which no rebase reconciles
        # (``RebaseFailedError``, issue #584 phase 0): the loser retries in a
        # fresh session and appends after the winner's row.
        grid, root, manifest = _windowed(cfg, tmp_path)
        _init(root, grid, cfg, manifest, ["2019"])
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        calls = []

        def commit(session):
            if not calls:
                # A competing run's init lands between this session's open and
                # its commit.
                _init(root, grid, cfg, manifest, ["2021"], run_id="run-other")
            calls.append(1)
            return icechunk_refs._commit(session, "init run-2", local=True, allow_empty=True)[0]

        block = icechunk_refs.read_block(root, store_kwargs={})
        _snapshot, rows = icechunk_rows.commit_rows(repo, block, {}, ["2020"], _YEARLY, commit)
        assert len(calls) == 2  # the refused attempt, then the fresh session's
        assert rows == ["2019", "2021", "2020"]
        group, _repo = _open(root)
        assert group.attrs[ICECHUNK_ATTR]["rows"] == rows
        assert group["6"]["count"].shape[0] == 3
        assert group["window_start"][:].tolist() == [_Y[2019], _Y[2021], _Y[2020]]

    def test_a_lost_allocation_gives_up_after_its_tries(self, cfg, tmp_path, monkeypatch):
        grid, root, manifest = _windowed(cfg, tmp_path)
        _init(root, grid, cfg, manifest, ["2019"])
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        monkeypatch.setattr(icechunk_rows, "ROW_ALLOC_TRIES", 2)
        years = iter(("2021", "2022", "2023"))

        def commit(session):
            _init(root, grid, cfg, manifest, [next(years)], run_id="run-other")
            return icechunk_refs._commit(session, "init run-2", local=True, allow_empty=True)[0]

        block = icechunk_refs.read_block(root, store_kwargs={})
        with pytest.raises(icechunk.RebaseFailedError):
            icechunk_rows.commit_rows(repo, block, {}, ["2020"], _YEARLY, commit)
        assert "2020" not in icechunk_refs.read_block(root, store_kwargs={})["rows"]

    @pytest.mark.parametrize("b_split", [1, 2])
    def test_a_lost_race_never_reapplies_a_stale_ratchet(self, cfg, tmp_path, monkeypatch, b_split):
        # Run A ratchets the store's split 3 -> 2 while run B lands its own
        # ratchet. A's updates were computed from the order-3 block: over B's
        # 3 -> 1 they would move the split back toward finer (§11.5 moves one
        # way), so A raises (its init fails open); over B's identical 3 -> 2
        # they are B's own, and A's retry lands.
        grid, root = _grid(cfg), str(tmp_path / "store")

        def init(split, run_id):
            cfg.output["icechunk"] = {"split_order": split, "commit_order": split}
            return icechunk_refs.init_repo(root, grid, cfg, run_id=run_id, store_kwargs={})

        init(3, "run-0")
        real = icechunk_rows.commit_rows

        def racing(repo, existing, updates, labels, temporal, commit, **kw):
            def commit_after_b(session):
                if not raced:
                    raced.append(None)  # B's own init goes through here too
                    raced[0] = init(b_split, "run-B")
                return commit(session)

            return real(repo, existing, updates, labels, temporal, commit_after_b, **kw)

        raced: list = []
        monkeypatch.setattr(icechunk_refs, "commit_rows", racing)
        if b_split == 1:
            with pytest.raises(ValueError, match=r"block changed under this init \(.*split_order"):
                init(2, "run-A")
        else:
            assert init(2, "run-A")["split_ratchet"] == {"from": 3, "to": 2}
        assert raced[0]["split_ratchet"] == {"from": 3, "to": b_split}
        block = icechunk_refs.read_block(root, store_kwargs={})
        assert block["split_order"] == block["commit_order"] == b_split
        assert block["levels"]["6"]["split"]["order"] == b_split
        # Either way the saved splitting config is the committed block's (issue #597).
        assert _saved_splits(root) == _block_splits(root)


class TestRefsLandAtTheirRow:
    def _two_versions(self, monkeypatch, grid, root, shard):
        """Two versions of one leaf (fills 1 and 50): one object set per row."""
        a = _write_leaf(monkeypatch, grid, root, shard, fill=1.0, refs=False, run_id="a")
        b = _write_leaf(monkeypatch, grid, root, shard, fill=50.0, refs=False, run_id="b")
        return a["leaf_version"], b["leaf_version"]

    def _unit(self, grid, root, shard, row, version):
        plan = icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={}, version=version)
        return {"level": 6, "row": row, "entries": plan}

    def test_a_second_run_adding_a_row_leaves_every_earlier_ref_identical(
        self, monkeypatch, cfg, tmp_path
    ):
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        (rank,) = grid.block_index(shard)
        v1, v2 = self._two_versions(monkeypatch, grid, root, shard)
        # Run 1: the window 2019.
        _init(root, grid, cfg, manifest, ["2019"])
        first = icechunk_refs.commit_units(
            root, [self._unit(grid, root, shard, "2019", v1)], "leaf", store_kwargs={}
        )
        _group, repo = _open(root)
        before = {
            name: _refs(repo.readonly_session(snapshot_id=first["snapshot"]), f"6/{name}")
            for name in ("count", "h_mean", "morton")
        }
        assert all(before.values())
        assert {coords[0] for refs in before.values() for coords in refs} == {0}
        # Run 2: the window 2020 — a new row, grown inside its init commit.
        _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
        second = icechunk_refs.commit_units(
            root, [self._unit(grid, root, shard, "2020", v2)], "leaf", store_kwargs={}
        )
        group, repo = _open(root)
        tip = repo.readonly_session(snapshot_id=second["snapshot"])
        for name, earlier in before.items():
            now = _refs(tip, f"6/{name}")
            # Every row-0 ref is byte-identical: same chunk, location, offset, length.
            assert {k: v for k, v in now.items() if k[0] == 0} == earlier
            # The new refs are row 1, at the same cell-axis chunks (§11.3).
            assert {k[1] for k in now if k[0] == 1} == {k[1] for k in earlier}
            assert {k[1] for k in earlier} <= set(range(rank * 4, (rank + 1) * 4))
        # Each row reads its own version's values, the cells unchanged.
        span = slice(rank * 16, (rank + 1) * 16)
        np.testing.assert_array_equal(group["6"]["count"][0, span][:4], np.full(4, 1))
        np.testing.assert_array_equal(group["6"]["count"][1, span][:4], np.full(4, 50))
        # The snapshot from before the second row still reads, one row deep.
        old = zarr.open_group(repo.readonly_session(snapshot_id=first["snapshot"]).store, mode="r")
        assert old["6"]["count"].shape == (1, 12 * 4**6)
        np.testing.assert_array_equal(old["6"]["count"][0, span][:4], np.full(4, 1))

    def test_the_manifest_split_cuts_the_row_axis_at_one_row(self, monkeypatch, cfg, tmp_path):
        # One manifest per ROW per split cell (§11.5, the 2026-10-02 ruling):
        # each manifest's row extent is a single row, so an append adds
        # manifests instead of rewriting the rows already written.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        v1, v2 = self._two_versions(monkeypatch, grid, root, shard)
        _init(root, grid, cfg, manifest, ["2019", "2020"])
        out = icechunk_refs.commit_units(
            root,
            [self._unit(grid, root, shard, "2019", v1), self._unit(grid, root, shard, "2020", v2)],
            "node",
            store_kwargs={},
        )
        _group, repo = _open(root)
        nodes = {n["path"]: n for n in repo.inspect_snapshot(out["snapshot"])["nodes"]}
        refs = nodes["/6/count"]["manifest_refs"]
        assert len(refs) == 2
        assert sorted(r["extents"][0] for r in refs) == [[0, 1], [1, 2]]

    def test_a_block_of_rows_per_manifest_is_a_knob(self, monkeypatch, cfg, tmp_path):
        # ``rows_per_manifest`` widens the row block; a global store sets one.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        v1, v2 = self._two_versions(monkeypatch, grid, root, shard)
        cfg.output["icechunk"] = {"rows_per_manifest": 4}  # after _write_leaf pops the block
        out = _init(root, grid, cfg, manifest, ["2019", "2020"])
        assert out["options"]["rows_per_manifest"] == 4
        assert out["levels"]["6"]["split"]["rows"] == 4
        committed = icechunk_refs.commit_units(
            root,
            [self._unit(grid, root, shard, "2019", v1), self._unit(grid, root, shard, "2020", v2)],
            "node",
            store_kwargs={},
        )
        _group, repo = _open(root)
        nodes = {n["path"]: n for n in repo.inspect_snapshot(committed["snapshot"])["nodes"]}
        # Both rows fall inside the one 4-row block, so there is ONE
        # manifest; its extent is clipped to the rows the array has.
        (ref,) = nodes["/6/count"]["manifest_refs"]
        assert ref["extents"][0] == [0, 2]

    def test_an_appended_row_supersedes_no_earlier_manifest(self, monkeypatch, cfg, tmp_path):
        # The point of the row cut: run 2 adds manifest files and the row-0
        # manifests keep their ids, so a reader's cached manifests stay valid.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        v1, v2 = self._two_versions(monkeypatch, grid, root, shard)
        _init(root, grid, cfg, manifest, ["2019"])
        first = icechunk_refs.commit_units(
            root, [self._unit(grid, root, shard, "2019", v1)], "leaf", store_kwargs={}
        )
        _group, repo = _open(root)

        def manifest_ids(snapshot):
            # The LEVEL arrays only: the root row coordinate is a small
            # native array that run 2 writes its new row's bounds into, so
            # its manifest is rewritten by design (§11.2).
            nodes = {n["path"]: n for n in repo.inspect_snapshot(snapshot)["nodes"]}
            return {
                path: {r["id"] for r in node["manifest_refs"]}
                for path, node in nodes.items()
                if node.get("manifest_refs") and path.startswith("/6/")
            }

        before = manifest_ids(first["snapshot"])
        assert before
        _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
        second = icechunk_refs.commit_units(
            root, [self._unit(grid, root, shard, "2020", v2)], "leaf", store_kwargs={}
        )
        _group, repo = _open(root)
        after = manifest_ids(second["snapshot"])
        for path, ids in before.items():
            # Every row-0 manifest survives by id, and the row-1 refs arrive
            # as NEW manifests alongside them -- nothing is rewritten.
            assert ids <= after[path], path
            assert len(after[path]) == len(ids) + 1, path

    def test_the_row_cut_is_fixed_at_creation_and_a_later_config_adopts_it(
        self, monkeypatch, cfg, tmp_path, caplog
    ):
        # ``rows_per_manifest`` is baked into every manifest already written,
        # so it does NOT ratchet: a later run adopts the store's and warns.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        self._two_versions(monkeypatch, grid, root, shard)
        cfg.output["icechunk"] = {"rows_per_manifest": 4}
        assert _init(root, grid, cfg, manifest, ["2019"])["options"]["rows_per_manifest"] == 4
        cfg.output["icechunk"] = {"rows_per_manifest": 1}
        with caplog.at_level("WARNING"):
            again = _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
        assert again["options"]["rows_per_manifest"] == 4
        assert "fixed at creation" in caplog.text
        assert _saved_row_splits(root)["6"] == 4

    def test_a_repo_recording_no_row_cut_is_refused_with_the_remedy(
        self, monkeypatch, cfg, tmp_path
    ):
        # A ``/2`` repo written before the row-split ruling records no
        # ``rows_per_manifest`` and its levels no ``rows``. Interim repos are
        # disposable (ruling of 2026-10-12, issue #584): every operation
        # refuses it with the remedy — ahead of the level check and of any
        # save — and nothing keeps an every-row-in-one cut alive for it.
        from zagg import icechunk_ops

        grid, root, manifest = _windowed(cfg, tmp_path)
        _init(root, grid, cfg, manifest, ["2019"])
        _group, repo = _open(root)
        session = repo.writable_session(icechunk_refs.BRANCH)
        group = zarr.open_group(session.store, mode="a")
        block = dict(group.attrs[ICECHUNK_ATTR])
        block.pop("rows_per_manifest")
        block["levels"] = {
            o: {**lvl, "split": {k: v for k, v in lvl["split"].items() if k != "rows"}}
            for o, lvl in block["levels"].items()
        }
        group.attrs[ICECHUNK_ATTR] = block
        session.commit("a pre-ruling block")
        saved = _saved_row_splits(root)
        remedy = r"records no rows_per_manifest.*Clear .*icechunk and re-run"
        with pytest.raises(ValueError, match=remedy):
            _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
        with pytest.raises(ValueError, match=remedy):
            icechunk_ops.declare_pyramid(root, cfg, store_kwargs={}, manifest=manifest, grid=grid)
        # Nothing was written, grown or re-saved on its account.
        assert icechunk_refs.read_block(root, store_kwargs={})["rows"] == ["2019"]
        assert _saved_row_splits(root) == saved
        assert not hasattr(icechunk_rows, "LEGACY_ROW_SPLIT")

    def test_the_adopted_cut_reaches_the_handle_the_run_writes_through(
        self, monkeypatch, cfg, tmp_path
    ):
        # Adoption has to move the REPO HANDLE, not only the returned
        # options: a config handed to ``open_or_create`` overrides the repo's
        # saved one, so an init opened at the config's cut would write this
        # run's manifests at a value the block does not record.
        grid, root, manifest = _windowed(cfg, tmp_path)
        cfg.output["icechunk"] = {"rows_per_manifest": 4}
        _init(root, grid, cfg, manifest, ["2019"])
        real, reopened = icechunk_refs._reopen_splits, []

        def spy(repo, store_root, splits, store_kwargs):
            reopened.append({o: s["rows"] for o, s in splits.items()})
            return real(repo, store_root, splits, store_kwargs)

        monkeypatch.setattr(icechunk_refs, "_reopen_splits", spy)
        cfg.output["icechunk"] = {"rows_per_manifest": 1}
        out = _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
        assert out["options"]["rows_per_manifest"] == 4
        levels = icechunk_refs.read_block(root, store_kwargs={})["levels"]
        assert reopened == [dict.fromkeys(levels, 4)]
        # Nothing is persisted over the store's own config by the adoption.
        assert _saved_row_splits(root) == dict.fromkeys(levels, 4)
        # A config that agrees with the store reopens nothing.
        reopened.clear()
        cfg.output["icechunk"] = {"rows_per_manifest": 4}
        _init(root, grid, cfg, manifest, ["2021"], run_id="run-3")
        assert reopened == []

    def test_a_row_the_repo_never_allocated_is_refused(self, monkeypatch, cfg, tmp_path):
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        v1, _v2 = self._two_versions(monkeypatch, grid, root, shard)
        _init(root, grid, cfg, manifest, ["2019"])
        with pytest.raises(ValueError, match="row '2020' is not allocated"):
            icechunk_refs.commit_units(
                root, [self._unit(grid, root, shard, "2020", v1)], "leaf", store_kwargs={}
            )
        # Nothing landed: the head is still the init.
        assert next(iter(_open(root)[1].ancestry(branch="main"))).message == f"init {RUN_ID}"

    def test_an_unsharded_levels_chunk_key_gains_the_row(self):
        assert icechunk_rows.row_key("count/c/9", 2) == "count/c/2/9"
        assert icechunk_rows.row_key("h/c/9/0", 0) == "h/c/0/9/0"

    def test_leaf_units_name_the_row(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        _write_leaf(monkeypatch, grid, root, shard, refs=False)
        units = icechunk_refs.leaf_units(grid, cfg, shard, root, column=None, store_kwargs={})
        assert [u["row"] for u in units] == ["all"]
        # The row IS the window: it names the unit's row AND selects the
        # leaf the plan reads (``{id}_2019.zarr``), so on this unwindowed
        # store the 2019 plan finds no object (issue #584 phase 2).
        windowed = icechunk_refs.leaf_units(
            grid, cfg, shard, root, column=None, store_kwargs={}, row="2019"
        )
        assert [u["row"] for u in windowed] == ["2019"]
        assert windowed[0]["entries"] == []
        # The plan itself is the object's cell-axis plan: row-free.
        (rank,) = grid.block_index(shard)
        count = next(e for e in units[0]["entries"] if e["path"] == "count")
        assert count["chunk_grid"] == (4,) and count["arr_offset"] == (rank * 4,)


class TestWindowedStoresAreInScope:
    """Issue #584 phase 2: §11.6 no longer refuses a windowed hive store."""

    def test_the_knob_defaults_on_and_validates_for_a_windowed_store(self, cfg):
        from zagg.config import get_icechunk, validate_config

        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "h_li",
            "epoch": "1970-01-01T00:00:00Z",
            "scale": "utc",
            "units": "seconds",
        }
        assert get_icechunk(cfg) is True
        cfg.output["icechunk"] = True
        validate_config(cfg)

    def test_the_dispatchers_rows_are_its_units_window_labels(self, cfg):
        # The init allocates exactly the rows the run can write: the labels
        # its expanded dispatch units carry, plus ``all`` when the store
        # maintains the cross-window fold.
        from zagg import runner

        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = _YEARLY
        shard_units = [
            (1, [], [({"label": "2019"}, []), ({"label": "2020"}, [])]),
            (2, [], [({"label": "2019"}, [])]),
        ]
        assert runner._unit_window_labels(shard_units, "shard") == ["2019", "2020"]
        window_units = [(1, [], {"label": "2020"}), (2, [], {"label": "2019"})]
        assert runner._unit_window_labels(window_units, "window") == ["2020", "2019"]
        # ``unit: shard`` is the default, so the shard-major units are read.
        # A windowed store maintains the cross-window fold unless its pyramid
        # is off (PR #623: an absent ``all_time`` resolves true), so ``all``
        # is allocated at birth; spelling the key changes nothing.
        assert runner._icechunk_rows(cfg, shard_units) == ["2019", "2020", "all"]
        cfg.output["pyramid"] = {"all_time": True}
        assert runner._icechunk_rows(cfg, shard_units) == ["2019", "2020", "all"]
        cfg.output["pyramid"] = False
        assert runner._icechunk_rows(cfg, shard_units) == ["2019", "2020"]
        cfg.output.pop("pyramid")
        # An unwindowed run is the one ``all`` row, whatever the units are.
        cfg.output.pop("windowing")
        assert runner._icechunk_rows(cfg, shard_units) == ["all"]

    def test_the_init_rows_refuse_a_windowed_config_with_no_units(self, cfg):
        # ``zagg.client`` calls this with no ``cells`` (it fans out nothing);
        # it is safe today only because it refuses windowed configs earlier,
        # so the seam says so rather than returning an empty row list that
        # would make a repo with no row for any window (issue #584 review).
        from zagg import runner

        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = _YEARLY
        with pytest.raises(ValueError, match="needs the run's cells"):
            runner._icechunk_rows(cfg)
        # An EMPTY fan-out is legitimate; an absent one is not.
        cfg.output["pyramid"] = {"all_time": True}
        assert runner._icechunk_rows(cfg, []) == ["all"]
        # An unwindowed config never needs them.
        cfg.output.pop("windowing")
        assert runner._icechunk_rows(cfg) == ["all"]

    def test_a_unit_of_the_wrong_shape_is_named_not_unpacked(self, cfg):
        from zagg import runner

        labels = runner._unit_window_labels
        # The un-fanned ``(shard, records)`` pair the reordering exists to rule out.
        with pytest.raises(ValueError, match="is not an expanded"):
            labels([(1, [])], "shard")
        # A shard-major unit read as window-major, and the reverse.
        with pytest.raises(ValueError, match="names no window label"):
            labels([(1, [], [({"label": "2019"}, [])])], "window")
        with pytest.raises(ValueError, match=r"not the \(payload, subset\) pairs"):
            labels([(1, [], {"label": "2019"})], "shard")
        # A payload that lost its label.
        for unit, mode in (((1, [], {"label": None}), "window"), ((1, [], [({}, [])]), "shard")):
            with pytest.raises(ValueError, match="names no window label"):
                labels([unit], mode)

    def test_a_window_leaf_records_its_refs_at_its_window_row(self, monkeypatch, cfg, tmp_path):
        # The end-to-end worker path (``hive._leaf_icechunk_refs``): two
        # windows of ONE shard, whose leaves share a shard rank, land at
        # two different rows and read back their own values.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        (rank,) = grid.block_index(shard)
        _init(root, grid, cfg, manifest, ["2019", "2020"])
        cfg.output["windowing"] = _YEARLY  # the worker reads its time_field
        cfg.output["icechunk"] = {"commit": "leaf"}
        for label, fill in (("2019", 1.0), ("2020", 50.0)):
            year = int(label)
            window = {"label": label, "start": _Y[year], "end": _Y[year + 1]}
            out = _write_leaf(
                monkeypatch, grid, root, shard, fill=fill, refs=True, run_id=label, window=window
            )
            assert out["icechunk"].get("error") is None, out["icechunk"]
        group, _repo = _open(root)
        span = slice(rank * 16, (rank + 1) * 16)
        np.testing.assert_array_equal(group["6"]["count"][0, span][:4], np.full(4, 1))
        np.testing.assert_array_equal(group["6"]["count"][1, span][:4], np.full(4, 50))


#: A windowed ladder run's granules: one per shard per year.
_WINDOW_GRANULES = (
    ("2019", "2019-03-01T00:00:00Z", "2019-03-02T00:00:00Z"),
    ("2020", "2020-03-01T00:00:00Z", "2020-03-02T00:00:00Z"),
)


class TestAMissingWindowLabelIsRefused:
    """Issue #584 review: ``all`` is the fold's row, never a default."""

    def test_unit_row_reads_a_label_a_unit_dict_or_nothing(self):
        for window in ("2019", {"label": "2019"}, {"label": "2019", "start": 0, "end": 1}):
            assert icechunk_rows.unit_row(window, windowed=True) == "2019"
        # An unwindowed leaf names no window and takes the one ``all`` row.
        assert icechunk_rows.unit_row(None, windowed=False) == "all"
        assert icechunk_rows.unit_row({}, windowed=False) == "all"

    def test_a_windowed_unit_that_lost_its_label_raises(self):
        for window in (None, {}, {"label": None}, {"label": ""}, ""):
            with pytest.raises(ValueError, match="names no window label"):
                icechunk_rows.unit_row(window, windowed=True)

    def test_a_payload_of_another_shape_raises_rather_than_falling_through(self):
        for window in (2019, ["2019"], object()):
            for windowed in (True, False):
                with pytest.raises(ValueError, match="is not a window label"):
                    icechunk_rows.unit_row(window, windowed=windowed)

    def test_record_leaf_refuses_it_against_a_windowed_repo(self, monkeypatch, cfg, tmp_path):
        # The row is read off the repo: an unwindowed store's init allocates
        # ``all`` and no other row, so a repo holding more is windowed.
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        _init(root, grid, cfg, manifest, ["2019", "all"])
        cfg.output["windowing"] = _YEARLY
        window = {"label": "2019", "start": _Y[2019], "end": _Y[2020]}
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id="2019", window=window)
        out = icechunk_refs.record_leaf(
            root, grid, shard, store_kwargs={}, window=window, version=meta["leaf_version"]
        )
        assert out["refs"] > 0
        with pytest.raises(ValueError, match="names no window label"):
            icechunk_refs.record_leaf(root, grid, shard, store_kwargs={}, window={"label": None})

    def test_the_worker_seam_fails_open_with_the_reserved_row_named(
        self, monkeypatch, cfg, tmp_path
    ):
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        _init(root, grid, cfg, manifest, ["2019", "all"])
        cfg.output["windowing"] = _YEARLY
        cfg.output["icechunk"] = {"commit": "leaf"}
        window = {"label": "2019", "start": _Y[2019], "end": _Y[2020]}
        _write_leaf(monkeypatch, grid, root, shard, refs=False, run_id="2019", window=window)
        leaf = hive.shard_leaf_path(root, shard, window="2019")
        out = hive._leaf_icechunk_refs(
            root,
            grid,
            cfg,
            shard,
            leaf,
            column=None,
            window={"label": None},
            sidecar_spec=manifest["spec"],
            store_kwargs={},
        )
        # Fail-open at the leaf (D9), but named — not a silent write to ``all``.
        assert "names no window label" in out["error"]


class TestTheLadderWalksAWindowedStore:
    """Issue #584 phase 2: a windowed store's nodes gather and commit per row."""

    def test_a_windowed_ladder_run_commits_every_row_through_the_repo(
        self, monkeypatch, cfg, tmp_path
    ):
        from test_icechunk_refs import _ladder_run, _stage_rows

        cfg.output["pyramid"] = {"all_time": True}
        shards = _shards(_grid(cfg), 2)
        grid, root, summary = _ladder_run(
            monkeypatch,
            cfg,
            tmp_path,
            icechunk_block={"commit": "ladder"},
            shards=shards,
            windowing={**_YEARLY, "unit": "window"},
            windows=_WINDOW_GRANULES,
        )
        assert summary["cells_error"] == 0
        init = summary["icechunk"]
        assert init["options"]["commit"] == "ladder"
        assert init["rows"] == ["2019", "2020", "all"]
        # Every leaf wrote a SIDECAR, not a commit: the node commits.
        for meta in summary["results"]:
            assert "sidecar" in meta["icechunk"], meta["icechunk"]
        rows = {r["dispatch_order"]: r for r in _stage_rows(root)}
        assert rows[3]["icechunk_commits"] > 0 and rows[3]["icechunk_failed"] == 0
        assert rows[3]["icechunk_missing"] == 0
        group, _repo = _open(root)
        # Each window's leaf reads back at its own row, and the all-time
        # fold at the reserved ``all`` row of the OVERVIEW levels (§11.2).
        for shard in shards:
            (rank,) = grid.block_index(shard)
            span = slice(rank * 16, (rank + 1) * 16)
            for row, label in enumerate(("2019", "2020")):
                path, _stamp = hive.resolve_leaf(hive.shard_leaf_path(root, shard, window=label))
                leaf = zarr.open_group(path, mode="r")["6"]
                assert leaf["count"][:].any(), "the window leaf wrote nothing to index"
                np.testing.assert_array_equal(group["6"]["count"][row, span], leaf["count"][:])
        assert (group["6"]["count"][2, :] == 0).all()  # no all-time leaf at the base
        # The row — not the shard rank — is what separates one shard's
        # windows: each row's refs point into that window's OWN leaf object.
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        refs = _refs(repo.readonly_session(icechunk_refs.BRANCH), "6/count")
        by_row: dict = {}
        for (row, _cell), (_kind, location, _offset, _length) in refs.items():
            leaf = next(part for part in location.split("/") if part.endswith(".zarr"))
            by_row.setdefault(row, set()).add(leaf.rpartition("_")[2])
        assert by_row == {0: {"2019.zarr"}, 1: {"2020.zarr"}}

    def test_a_node_plans_the_rows_the_run_writes_not_the_repos_whole_list(self):
        from zagg import sweep_units

        by_shard = {"11111": {"2021"}, "11112": {"2021", "2022"}}
        dirt = {"11113": {"2019"}}  # refs-only: its row still has to be planned
        # An unwindowed store is the one ``all`` row whatever the units say.
        assert sweep_units.node_rows("1111", by_shard, dirt, windowed=False, close=False) == ["all"]
        assert sweep_units.node_rows("1111", by_shard, dirt, windowed=True, close=False) == [
            "2019",
            "2021",
            "2022",
        ]
        # The close is the cross-window fold, written at the ``all`` row.
        assert sweep_units.node_rows("1111", by_shard, None, windowed=True, close=True) == [
            "2021",
            "2022",
            "all",
        ]
        # A node with no dirty leaf beneath it plans only what it closes.
        assert sweep_units.node_rows("1112", by_shard, None, windowed=True, close=True) == ["all"]
        assert sweep_units.node_rows("1112", by_shard, None, windowed=True, close=False) == []

    def test_an_appended_window_replans_no_earlier_rows_objects(self, monkeypatch, cfg, tmp_path):
        # The harm the run's rows avoid: planning the repo's whole row list
        # HEADs every window the store ever allocated, per node per level,
        # and re-commits the untouched ones at their rows — rewriting those
        # rows' manifests, the opposite of the cut's point (issue #584 review).
        from test_icechunk_refs import _ladder_run

        from zagg import icechunk_ladder

        cfg.output["pyramid"] = {"all_time": True}
        shards = _shards(_grid(cfg), 2)
        run = dict(
            icechunk_block={"commit": "ladder"},
            shards=shards,
            windowing={**_YEARLY, "unit": "window"},
        )
        _grid_, root, _summary = _ladder_run(
            monkeypatch, cfg, tmp_path, windows=_WINDOW_GRANULES[:1], **run
        )
        planned: list = []
        real = icechunk_ladder.object_ref_plan

        def spy(grid, object_rel, rank, store_root, **kw):
            planned.append(object_rel)
            return real(grid, object_rel, rank, store_root, **kw)

        monkeypatch.setattr(icechunk_ladder, "object_ref_plan", spy)
        append = (("2020", "2020-03-01T00:00:00Z", "2020-03-02T00:00:00Z"),)
        _grid_, _root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, windows=append, root=root, **run
        )
        assert summary["cells_error"] == 0
        assert icechunk_refs.read_block(root, store_kwargs={})["rows"] == ["2019", "all", "2020"]
        # The append planned its own window's overviews and the all-time
        # fold it rewrote — never 2019's, whose refs stand as committed.
        assert planned, "the ladder planned nothing"
        assert not [rel for rel in planned if rel.endswith("/2019.zarr")], sorted(set(planned))

    def test_the_all_time_fold_reads_back_at_the_reserved_row(self, monkeypatch, cfg, tmp_path):
        # Plan phase 2 acceptance (1): a windowed run reads back per window
        # AND all-time through the repo. The all-time fold is the overview
        # levels' ``all.zarr``, indexed at the reserved ``all`` row.
        from pathlib import Path

        from mortie import mort2healpix
        from test_icechunk_refs import _ladder_run

        from zagg.grids.morton import morton_word

        cfg.output["pyramid"] = {"all_time": True}
        shards = _shards(_grid(cfg), 2)
        _grid_, root, summary = _ladder_run(
            monkeypatch,
            cfg,
            tmp_path,
            icechunk_block={"commit": "ladder"},
            shards=shards,
            windowing={**_YEARLY, "unit": "window"},
            windows=_WINDOW_GRANULES,
        )
        assert summary["cells_error"] == 0
        rows = icechunk_refs.read_block(root, store_kwargs={})["rows"]
        fold = rows.index("all")
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
                    group[str(r)]["count"][fold, rank * n : (rank + 1) * n], src["count"][:]
                )
                read += 1
        assert read, "no all-time overview object was written"
        # It is the fold of the windows, not a copy of one of them.
        level = str(levels[min(levels)])
        window = rows.index("2019")
        assert group[level]["count"][fold, :].sum() > group[level]["count"][window, :].sum()

    def test_a_tag_cut_before_an_append_still_reads_every_level(self, monkeypatch, cfg, tmp_path):
        # Plan phase 2 acceptance (3), and phase 3's: "two runs over one
        # store; the first run's tag reads its own ``all`` overview after the
        # second run rewrote ``all.zarr``." A windowed store maintains the
        # fold (PR #623) and ``all.zarr`` is the one overview object an
        # append REWRITES, which a virtual ref could not survive; the fold's
        # row is the repo's own bytes (spec §11.3), so the tag reads the
        # ``all`` row it was cut with and not the append's. Every per-window
        # row is immutable and reads either way.
        from test_icechunk_refs import _ladder_run

        shards = _shards(_grid(cfg), 2)
        run = dict(
            icechunk_block={"commit": "ladder"},
            shards=shards,
            windowing={**_YEARLY, "unit": "window"},
        )
        _grid_, root, first = _ladder_run(
            monkeypatch, cfg, tmp_path, windows=_WINDOW_GRANULES[:1], **run
        )
        tag = first["icechunk_finalize"]["tag"]
        assert first["icechunk_finalize"]["tagged"]
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        rows = icechunk_refs.read_block(root, store_kwargs={})["rows"]
        assert sorted(rows) == ["2019", "all"]  # the one window and the fold's row
        window, fold = rows.index("2019"), rows.index("all")
        tagged = zarr.open_group(repo.readonly_session(tag=tag).store, mode="r")
        levels = list(icechunk_refs.read_block(root, store_kwargs={})["levels"])
        before = {level: tagged[level]["count"][window] for level in levels}
        folded = {level: tagged[level]["count"][fold] for level in levels}
        assert before and any(values.any() for values in folded.values()), (
            "the first run wrote no all-time fold to read back"
        )
        objects = _fold_objects(root)
        assert objects, "no all-time overview object was written"
        _grid_, _root, second = _ladder_run(
            monkeypatch,
            cfg,
            tmp_path,
            windows=(("2020", "2020-03-01T00:00:00Z", "2020-03-02T00:00:00Z"),),
            root=root,
            **run,
        )
        assert second["cells_error"] == 0
        assert icechunk_refs.read_block(root, store_kwargs={})["rows"] == [*rows, "2020"]
        # The append rewrote the fold's objects under the tag's feet -- which
        # is what makes the ``all`` row below a test of the copy and not of
        # immutable bytes.
        rewritten = _fold_objects(root)
        assert [key for key in objects if rewritten.get(key) != objects[key]], (
            "the append left every all-time overview object as it was"
        )
        # The tag reads what it was cut with: its window row AND its ``all``
        # row, every level, the same values -- the append added a row, moved
        # no window ref, and re-folded the fold without disturbing the rows
        # already committed.
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        tagged = zarr.open_group(repo.readonly_session(tag=tag).store, mode="r")
        for level, values in before.items():
            np.testing.assert_array_equal(tagged[level]["count"][window], values)
        for level, values in folded.items():
            np.testing.assert_array_equal(tagged[level]["count"][fold], values)
        # And the branch tip carries the new row, the window row unchanged,
        # and a fold that is now the two windows' -- so the tag is reading
        # its own history, not the same bytes twice.
        tip, _repo = _open(root)
        for level, values in before.items():
            assert tip[level]["count"].shape[0] == 3
            np.testing.assert_array_equal(tip[level]["count"][window], values)
        assert [
            level
            for level, values in folded.items()
            if tip[level]["count"][fold].sum() > values.sum()
        ], "the append did not grow the all-time fold"


class TestRowGrowthUnderStagedRefs:
    """Issue #584 phase 3: another run's init grows the row axis under a session's staged refs.

    The phase-0 spike's item (iii): refs staged in EXISTING rows cannot rebase
    over a commit that resized the arrays (``Chunks updated in updated
    array``), so without a replay the node's whole gather is lost — a race
    between concurrent runs on one store, the fleet's normal shape.
    """

    def _staged(self, monkeypatch, cfg, tmp_path):
        """A windowed repo at row ``2019`` and one leaf's unit for that row."""
        grid, root, manifest = _windowed(cfg, tmp_path)
        (shard,) = _shards(grid, 1)
        version = _write_leaf(monkeypatch, grid, root, shard, fill=1.0, refs=False, run_id="a")
        _init(root, grid, cfg, manifest, ["2019"])
        plan = icechunk_refs.leaf_ref_plan(
            grid, shard, root, store_kwargs={}, version=version["leaf_version"]
        )
        return grid, root, manifest, shard, {"level": 6, "row": "2019", "entries": plan}

    @staticmethod
    def _grown_under(unit, grow):
        """A unit source that lands ``grow()`` once, AFTER ``unit`` is staged and before the commit."""
        fired = []

        def units():
            yield unit
            if not fired:  # the replay streams the source again; the other run landed once
                fired.append(grow())

        return units

    def test_a_row_grown_under_staged_refs_is_replayed_in_a_fresh_session(
        self, monkeypatch, cfg, tmp_path, caplog
    ):
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)
        (rank,) = grid.block_index(shard)
        grown = []

        def grow():
            grown.append(_init(root, grid, cfg, manifest, ["2020"], run_id="run-2")["rows"])

        with caplog.at_level("WARNING"):
            out = icechunk_refs.commit_units(
                root, self._grown_under(unit, grow), "node 1", store_kwargs={}
            )
        assert grown == [["2019", "2020"]] and out["snapshot"]
        # The record says a replay happened, once; the log says why. The
        # first session's rebase round (the one the growth defeated) and its
        # commit time are carried into the record, not dropped with it.
        assert out["replays"] == 1 and out["rebases"] == 1 and out["commit_s"] > 0.0
        assert "fresh session" in caplog.text and "row axis grew" in caplog.text
        group, repo = _open(root)
        refs = _refs(repo.readonly_session(snapshot_id=out["snapshot"]), "6/count")
        # Every ref landed at ITS row (2019 = 0), none in the grown row, and
        # exactly the plan's chunks — nothing doubled, nothing dropped.
        assert {k[0] for k in refs} == {0}
        assert len(refs) == next(e["refs"] for e in unit["entries"] if e["path"] == "count")
        assert group["6"]["count"].shape[0] == 2
        np.testing.assert_array_equal(group["6"]["count"][0, rank * 16 : rank * 16 + 4], 1)
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages.count("node 1") == 1 and "init run-2" in messages
        # Idempotent: the same units again, with nothing racing, change no ref.
        again = icechunk_refs.commit_units(root, [unit], "node 1 again", store_kwargs={})
        assert again["replays"] == 0
        assert _refs(repo.readonly_session(snapshot_id=again["snapshot"]), "6/count") == refs

    def test_without_the_replay_the_node_loses_its_refs_to_the_growth(
        self, monkeypatch, cfg, tmp_path
    ):
        # The replay is load-bearing: with none allowed, the conflict is the
        # spike's class and nothing of the node lands.
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)
        monkeypatch.setattr(icechunk_refs, "REPLAY_TRIES", 0)
        with pytest.raises(icechunk.RebaseFailedError) as info:
            icechunk_refs.commit_units(
                root,
                self._grown_under(
                    unit, lambda: _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
                ),
                "node 1",
                store_kwargs={},
            )
        kinds = {repr(c.conflict_type) for c in info.value.conflicts}
        assert kinds == {"ChunksUpdatedInUpdatedArray"}
        _group, repo = _open(root)
        assert _refs(repo.readonly_session(icechunk_refs.BRANCH), "6/count") == {}
        assert "node 1" not in [s.message for s in repo.ancestry(branch="main")]

    def test_a_replay_that_stages_nothing_still_records_its_contention(
        self, monkeypatch, cfg, tmp_path
    ):
        # The replay's source re-read yields no refs (a carrier vanished
        # between passes): nothing is committed, but the record still carries
        # the replay and the first session's rebase round.
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)
        passes = []

        def units():
            passes.append(1)
            if len(passes) == 1:
                yield unit
                _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")

        out = icechunk_refs.commit_units(root, units, "node 1", store_kwargs={})
        assert out["snapshot"] is None and out["refs"] == 0
        assert out["replays"] == 1 and out["rebases"] == 1 and out["commit_s"] > 0.0

    def test_a_replay_is_bounded(self, monkeypatch, cfg, tmp_path):
        # A growth landing under the REPLAY too is one more than the bound
        # allows: the node raises (fail-open at the hook) rather than looping.
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)
        years = iter(["2020", "2021", "2022"])

        def units():
            yield unit
            _init(root, grid, cfg, manifest, [next(years)], run_id="run-2")

        with pytest.raises(icechunk.RebaseFailedError):
            icechunk_refs.commit_units(root, units, "node 1", store_kwargs={})
        _group, repo = _open(root)
        # One replay was attempted (two sessions, two growths), then it raised.
        assert next(years) == "2022"
        assert "node 1" not in [s.message for s in repo.ancestry(branch="main")]

    def test_another_conflict_class_is_refused_not_replayed(self, monkeypatch, cfg, tmp_path):
        # Only the row-growth class is replayed: the same chunks set by two
        # writers is a double update, refused as before — replaying it would
        # silently overwrite the other writer's refs.
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)

        def other_writer():
            icechunk_refs.commit_units(root, [unit], "node 1 (other)", store_kwargs={})

        with pytest.raises(icechunk.RebaseFailedError) as info:
            icechunk_refs.commit_units(
                root, self._grown_under(unit, other_writer), "node 1", store_kwargs={}
            )
        kinds = {repr(c.conflict_type) for c in info.value.conflicts}
        assert kinds == {"ChunkDoubleUpdate"}
        _group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages.count("node 1 (other)") == 1 and "node 1" not in messages

    def test_a_double_update_after_the_growth_is_refused_not_replayed(
        self, monkeypatch, cfg, tmp_path
    ):
        # The rebase stops at the FIRST commit it cannot reconcile: with the
        # growth first, its conflicts are growth only and the later double
        # update is never examined. The diff since the session's base catches
        # it, so the other writer's refs stand — the same outcome as the
        # reverse order above (review finding, two all-time runs on one node).
        grid, root, manifest, shard, unit = self._staged(monkeypatch, cfg, tmp_path)

        def grow_then_other_writer():
            _init(root, grid, cfg, manifest, ["2020"], run_id="run-2")
            icechunk_refs.commit_units(root, [unit], "node 1 (other)", store_kwargs={})

        with pytest.raises(icechunk.RebaseFailedError) as info:
            icechunk_refs.commit_units(
                root, self._grown_under(unit, grow_then_other_writer), "node 1", store_kwargs={}
            )
        kinds = {repr(c.conflict_type) for c in info.value.conflicts}
        assert kinds == {"ChunksUpdatedInUpdatedArray"}
        _group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages[:2] == ["node 1 (other)", "init run-2"] and "node 1" not in messages

    def test_the_ladder_replays_a_node_and_counts_it(self, monkeypatch, cfg, tmp_path, caplog):
        # The fleet path: ``stage_node_refs`` streams the node's children into
        # one session; a run-2 init landing mid-stream grows the rows under
        # it, the node replays, and the stage row counts the replay.
        from test_icechunk_refs import _ladder_run, _stage_rows

        from zagg import icechunk_ladder

        cfg.output["pyramid"] = {"all_time": True}
        shards = _shards(_grid(cfg), 2)
        root = str(tmp_path / "store")
        real, grown = icechunk_ladder._child_units, []

        def child_units(*args):
            for item in real(*args):
                yield item
                if not grown:  # once: after the first child's refs are staged
                    out = icechunk_refs.init_repo(
                        root, _grid(cfg), cfg, run_id="run-2", store_kwargs={}, rows=["2021"]
                    )
                    grown.append(out["rows"])

        monkeypatch.setattr(icechunk_ladder, "_child_units", child_units)
        with caplog.at_level("WARNING"):
            grid, root, summary = _ladder_run(
                monkeypatch,
                cfg,
                tmp_path,
                icechunk_block={"commit": "ladder"},
                shards=shards,
                windowing={**_YEARLY, "unit": "window"},
                windows=_WINDOW_GRANULES,
                root=root,
            )
        assert grown == [["2019", "2020", "all", "2021"]]
        assert summary["cells_error"] == 0
        rows = {r["dispatch_order"]: r for r in _stage_rows(root)}
        assert rows[3]["icechunk_failed"] == 0 and rows[3]["icechunk_replays"] == 1
        assert rows[0]["icechunk_replays"] == 0  # the coarser tuple raced nothing
        assert "row axis grew" in caplog.text
        # Every window's leaf still reads back at its own row.
        group, _repo = _open(root)
        for shard in shards:
            (rank,) = grid.block_index(shard)
            span = slice(rank * 16, (rank + 1) * 16)
            for row, label in enumerate(("2019", "2020")):
                path, _stamp = hive.resolve_leaf(hive.shard_leaf_path(root, shard, window=label))
                leaf = zarr.open_group(path, mode="r")["6"]
                np.testing.assert_array_equal(group["6"]["count"][row, span], leaf["count"][:])
        assert group["6"]["count"].shape[0] == 4  # the raced-in 2021 row is there, empty


class TestRevisionOneIsRefused:
    def _downgrade(self, root):
        """Rewrite the block's spec token to ``/1``: a repo of the earlier revision."""
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="r+")
        group.attrs[ICECHUNK_ATTR] = {**group.attrs[ICECHUNK_ATTR], "spec": "zagg-icechunk/1"}
        session.commit("as /1")

    def test_every_writer_path_refuses_with_the_re_init_remedy(self, monkeypatch, cfg, tmp_path):
        from zagg import icechunk_ops
        from zagg.icechunk_finalize import finalize_repo
        from zagg.icechunk_ladder import ladder_context

        grid = _grid(cfg)
        root = str(tmp_path / "store")
        (shard,) = _shards(grid, 1)
        _write_leaf(monkeypatch, grid, root, shard, refs=False)
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        self._downgrade(root)
        n = len(list(_open(root)[1].ancestry(branch="main")))
        match = (
            r"is 'zagg-icechunk/1', this writer is 'zagg-icechunk/2'.*"
            r"Clear .*icechunk and re-run"
        )
        with pytest.raises(ValueError, match=match):
            icechunk_refs.init_repo(root, grid, cfg, run_id="run-2", store_kwargs={})
        with pytest.raises(ValueError, match=match):
            icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
        with pytest.raises(ValueError, match=match):
            icechunk_refs.commit_units(root, [], "node", store_kwargs={})
        with pytest.raises(ValueError, match=match):
            finalize_repo(root, run_id="run-2", semantic_hash="h", store_kwargs={})
        with pytest.raises(ValueError, match=match):
            icechunk_ops.set_attrs(root, "/", {"a": 1}, store_kwargs={})
        with pytest.raises(ValueError, match=match):
            ladder_context(root, hive.build_manifest(grid), store_kwargs={})
        # Nothing was written into the ``/1`` repo, and no tag was cut.
        _group, repo = _open(root)
        assert len(list(repo.ancestry(branch="main"))) == n and list(repo.list_tags()) == []

    def test_the_local_run_fails_open_and_the_leaf_still_lands(self, monkeypatch, cfg, tmp_path):
        # The dispatcher's init is fail-open (D9): a ``/1`` repo costs the run
        # its index, with the remedy in the recorded error, never a leaf.
        from zagg import runner

        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        self._downgrade(root)
        out = runner._init_icechunk_local(cfg, grid, root, "run-2", {})
        assert set(out) == {"error"} and "Clear" in out["error"]
        (shard,) = _shards(grid, 1)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=True)
        assert meta.get("error") is None and "zagg-icechunk/1" in meta["icechunk"]["error"]
        assert int(_leaf_arrays(root, shard)["count"][:].sum()) > 0
