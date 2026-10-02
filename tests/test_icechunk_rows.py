"""The row dimension of the Icechunk companion repo (issue #584, spec §11.2).

``zagg-icechunk/2``: every level array is ``(n_rows, n_cells)``. These tests
pin the row law (allocated once, by label, in order of first appearance,
never reordered), the root coordinate, the cell-axis-only manifest split, the
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
from test_icechunk_refs import RUN_ID, _grid, _leaf_arrays, _open, _shards, _write_leaf

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

        def racing(repo, existing, updates, labels, temporal, commit):
            def commit_after_b(session):
                if not raced:
                    raced.append(None)  # B's own init goes through here too
                    raced[0] = init(b_split, "run-B")
                return commit(session)

            return real(repo, existing, updates, labels, temporal, commit_after_b)

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

    def test_the_manifest_split_is_the_cell_axis_alone(self, monkeypatch, cfg, tmp_path):
        # One manifest per split cell spans EVERY row (§11.5): the snapshot's
        # manifest count does not grow with the rows.
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
        (ref,) = nodes["/6/count"]["manifest_refs"]
        assert ref["extents"][0] == [0, 2]  # both rows in the one manifest

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
        units = icechunk_refs.leaf_units(
            grid, cfg, shard, root, column=None, store_kwargs={}, row="2019"
        )
        assert [u["row"] for u in units] == ["2019"]
        # The plan itself is the object's cell-axis plan: row-free.
        (rank,) = grid.block_index(shard)
        count = next(e for e in units[0]["entries"] if e["path"] == "count")
        assert count["chunk_grid"] == (4,) and count["arr_offset"] == (rank * 4,)


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
