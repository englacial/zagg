"""``zagg.icechunk_ops`` — metadata commits as operations (spec §11.4, issue #582)."""

from __future__ import annotations

import copy
import io
import json
import time

import pytest
import zarr
from test_icechunk_refs import _grid, _ladder_run, _open, _shards, _write_leaf

from zagg import hive, icechunk_ops, icechunk_refs
from zagg.config import default_config
from zagg.icechunk_refs import ICECHUNK_ATTR, MULTISCALES_ATTR

RUN = "r1"


@pytest.fixture
def cfg():
    cfg = default_config("atl06", validate=False)
    cfg.output["store_layout"] = "hive"
    cfg.output["icechunk"] = {"commit": "leaf"}
    # ``from_config`` must rebuild ``_grid``'s geometry (shard 4 / chunk 5 / cell 6).
    cfg.output["grid"] = {
        "type": "healpix",
        "indexing_scheme": "nested",
        "parent_order": 4,
        "child_order": 6,
        "chunk_inner": 5,
    }
    return cfg


def _write_manifest(root, grid):
    from zagg.store import open_object_store, put_object

    manifest = hive.build_manifest(grid)
    put_object(open_object_store(root), hive.MANIFEST_NAME, json.dumps(manifest).encode())
    return manifest


def _store(monkeypatch, cfg, tmp_path, *, leaf=True):
    """A declared-off store (base level only) with its repo and one committed leaf."""
    cfg.output["pyramid"] = False
    grid = _grid(cfg)
    root = str(tmp_path / "store")
    _write_manifest(root, grid)
    out = icechunk_refs.init_repo(root, grid, cfg, run_id=RUN, store_kwargs={})
    assert out["ladder"] == [6]
    if leaf:
        (shard,) = _shards(grid, 1)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=True, run_id=RUN)
        assert "error" not in meta["icechunk"]
    return grid, root


def _messages(root):
    _group, repo = _open(root)
    return [s.message for s in repo.ancestry(branch="main")]


class TestSetAttrs:
    def test_root_level_and_array_attrs_each_commit_once(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        before = icechunk_ops._array_model(_open(root)[1].readonly_session("main"))
        r = icechunk_ops.set_attrs(root, "/", {"title": "ATL06"}, store_kwargs={})
        assert r["operation"] == "set-attrs" and r["message"] == "set-attrs /"
        assert r["keys"] == ["title"] and r["attrs"]["title"] == "ATL06"
        group, repo = _open(root)
        assert group.attrs["title"] == "ATL06"
        assert ICECHUNK_ATTR in group.attrs  # the block rides along untouched
        # A level group's convention block (the dggs latitude token, §11 head).
        dggs = {**group["6"].attrs["dggs"], "latitude": "geodetic"}
        r = icechunk_ops.set_attrs(root, "/6", {"dggs": dggs}, store_kwargs={})
        assert _open(root)[0]["6"].attrs["dggs"]["latitude"] == "geodetic"
        # An array's attrs; ``None`` deletes.
        icechunk_ops.set_attrs(root, "/6/count", {"units": "1", "note": "x"}, store_kwargs={})
        r = icechunk_ops.set_attrs(root, "/6/count", {"note": None}, store_kwargs={})
        assert r["attrs"] == {"units": "1"}
        assert dict(_open(root)[0]["6"]["count"].attrs) == {"units": "1"}
        # The array model never moved, and the history reads as a log.
        _group, repo = _open(root)
        assert icechunk_ops._array_model(repo.readonly_session("main")) == before
        head = next(iter(repo.ancestry(branch="main")))
        assert head.message == "set-attrs /6/count"
        assert head.metadata["operation"] == "set-attrs" and head.metadata["node"] == "/6/count"
        assert head.metadata["keys"] == ["note"] and "zagg_version" in head.metadata
        assert _messages(root)[:4] == [
            "set-attrs /6/count",
            "set-attrs /6/count",
            "set-attrs /6",
            "set-attrs /",
        ]

    def test_identical_attrs_commit_nothing(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        icechunk_ops.set_attrs(root, "/", {"title": "x"}, store_kwargs={})
        n = len(_messages(root))
        r = icechunk_ops.set_attrs(root, "/", {"title": "x"}, store_kwargs={}, message="again")
        assert r["unchanged"] is True and r["snapshot"] is None
        assert len(_messages(root)) == n

    def test_reserved_root_keys_bad_input_and_missing_node_refuse(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        n = len(_messages(root))
        for key in (ICECHUNK_ATTR, MULTISCALES_ATTR):
            with pytest.raises(ValueError, match="refuses the root keys"):
                icechunk_ops.set_attrs(root, "/", {key: {}}, store_kwargs={})
        with pytest.raises(ValueError, match="non-empty JSON object"):
            icechunk_ops.set_attrs(root, "/", {}, store_kwargs={})
        with pytest.raises(zarr.errors.GroupNotFoundError):
            icechunk_ops.set_attrs(root, "/7", {"a": 1}, store_kwargs={})
        assert len(_messages(root)) == n
        with pytest.raises(ValueError, match="not initialized"):
            icechunk_ops.set_attrs(str(tmp_path / "bare"), "/", {"a": 1}, store_kwargs={})


class TestValidation:
    def test_a_mutation_that_moves_the_array_model_is_discarded(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        n = len(_messages(root))

        def add_array(session, _block):
            zarr.create_array(session.store, name="6/extra", shape=(4,), dtype="i4")
            return {}

        def resize(session, _block):
            zarr.open_array(session.store, path="6/count", mode="r+").resize((1, 8))
            return {}

        def drop_block(session, _block):
            root_group = zarr.open_group(session.store, mode="r+")
            del root_group.attrs[ICECHUNK_ATTR]
            return {}

        def edit_block(**updates):
            def mutate(session, _block):
                root_group = zarr.open_group(session.store, mode="r+")
                block = {**root_group.attrs[ICECHUNK_ATTR], **updates}
                root_group.attrs[ICECHUNK_ATTR] = block
                return {}

            return mutate

        repo = icechunk_refs.repo_path(root)
        for mutate, msg in (
            (add_array, "would add array"),
            (resize, "would change the array model"),
            (drop_block, "would remove the root"),
            (edit_block(shard_order=3), f"icechunk repo {repo} was built for"),
            (edit_block(cell_order=7), "would move the block's cell_order"),
            (edit_block(levels={}), "would delist the base level /6"),
        ):
            with pytest.raises(ValueError, match=msg):
                icechunk_ops._operation(root, "probe", mutate, store_kwargs={})
        assert len(_messages(root)) == n  # nothing landed
        assert "6/extra" not in icechunk_ops._array_model(_open(root)[1].readonly_session("main"))

    @staticmethod
    def _grow(session, rows, *, arrays=True, block=True, skip=()):
        """Append ``rows`` the way an allocation does; each half can be left out."""
        root_group = zarr.open_group(session.store, mode="r+")
        have = list(root_group.attrs[ICECHUNK_ATTR]["rows"])
        if arrays:
            for path, node in root_group.members(max_depth=None):
                if isinstance(node, zarr.Array) and path not in skip:
                    node.resize((len(have) + len(rows), *node.shape[1:]))
        if block:
            root_group.attrs[ICECHUNK_ATTR] = {
                **root_group.attrs[ICECHUNK_ATTR],
                "rows": [*have, *rows],
            }

    def test_rows_may_grow_and_nothing_else(self, monkeypatch, cfg, tmp_path):
        # The identity check's one allowance (§11.4): labels appended to the
        # block's ``rows`` with EVERY array holding that many rows. Anything
        # short of that — a shrink, a cell-extent change, arrays and block out
        # of step, a reordered list, a label allocated twice — is refused and
        # nothing lands.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        n = len(_messages(root))
        grow = self._grow

        def shrink(session, _block):
            zarr.open_array(session.store, path="6/count", mode="r+").resize((0, 12 * 4**6))
            return {}

        def grow_cells_too(session, _block):
            grow(session, ["2019"])
            arr = zarr.open_array(session.store, path="6/count", mode="r+")
            arr.resize((arr.shape[0], arr.shape[1] + 4))
            return {}

        def reorder(session, _block):
            grow(session, ["2019"])
            root_group = zarr.open_group(session.store, mode="r+")
            block = dict(root_group.attrs[ICECHUNK_ATTR])
            root_group.attrs[ICECHUNK_ATTR] = {**block, "rows": block["rows"][::-1]}
            return {}

        for mutate, msg in (
            (shrink, "would change the array model of '6/count'"),
            (grow_cells_too, "would change the array model of '6/count'"),
            (lambda s, _b: grow(s, ["2019"], block=False) or {}, "off the block's 1 rows"),
            (lambda s, _b: grow(s, ["2019"], arrays=False) or {}, "off the block's 2 rows"),
            (lambda s, _b: grow(s, ["2019"], skip=("6/count",)) or {}, r"\['6/count'\] off"),
            (reorder, "would reorder or drop rows"),
            (lambda s, _b: grow(s, ["all"]) or {}, "would allocate a row twice"),
            (lambda s, _b: grow(s, ["2019", "2019"]) or {}, "would allocate a row twice"),
        ):
            with pytest.raises(ValueError, match=msg):
                icechunk_ops._operation(root, "probe", mutate, store_kwargs={})
        assert len(_messages(root)) == n  # nothing landed

    def test_an_allocation_passes_the_check(self, cfg, tmp_path):
        # The init's own allocation (``grow_rows`` plus the block's ``rows``,
        # §11.4) on a windowed store is exactly the growth the check allows.
        from test_icechunk_rows import _Y, _YEARLY

        from zagg import icechunk_rows

        cfg.output["pyramid"] = False
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        manifest = hive.build_manifest(grid, windowing=_YEARLY)
        icechunk_refs.init_repo(
            root, grid, cfg, run_id=RUN, store_kwargs={}, manifest=manifest, rows=["2019"]
        )

        def allocate(session, block):
            rows = icechunk_rows.grow_rows(session, block["rows"], ["2020", "2021"], _YEARLY)
            root_group = zarr.open_group(session.store, mode="r+")
            root_group.attrs[ICECHUNK_ATTR] = {**root_group.attrs[ICECHUNK_ATTR], "rows": rows}
            return {}

        out = icechunk_ops._operation(root, "probe", allocate, store_kwargs={})
        assert out["snapshot"] and _messages(root)[0] == "probe"
        group, _repo = _open(root)
        assert group.attrs[ICECHUNK_ATTR]["rows"] == ["2019", "2020", "2021"]
        assert group["6"]["count"].shape == (3, 12 * 4**6)
        assert group["window_start"][:].tolist() == [_Y[2019], _Y[2020], _Y[2021]]
        assert int(group["6"]["count"][1:, :].sum()) == 0  # the new rows read fill


class TestDeclarePyramid:
    def test_adds_the_declared_levels_and_the_mirror(self, monkeypatch, cfg, tmp_path):
        grid, root = _store(monkeypatch, cfg, tmp_path)
        cfg.output.pop("pyramid")  # the default declaration: overviews at every ancestor order
        manifest = _write_manifest(root, grid)
        assert manifest.get(MULTISCALES_ATTR)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert r["operation"] == "declare-pyramid" and r["snapshot"]
        assert r["added"] == ["1", "2", "3", "4", "5"] and r["dropped"] == []
        assert r["multiscales"] is True
        group, repo = _open(root)
        block = group.attrs[ICECHUNK_ATTR]
        # Exactly what a fresh init of the declared store would record.
        fresh = icechunk_refs.init_repo(
            str(tmp_path / "fresh"), grid, cfg, run_id=RUN, store_kwargs={}
        )
        assert block["levels"] == fresh["levels"] == r["levels"]
        assert group.attrs[MULTISCALES_ATTR] == manifest[MULTISCALES_ATTR]
        assert {k for k, _ in group.groups()} == set(block["levels"])
        # Each new group carries the level's array model, and the repo's
        # persisted manifest splits cover it (a commit into it cuts at its
        # own order, §11.5).
        fresh_group, fresh_repo = _open(str(tmp_path / "fresh"))
        for order in r["added"]:
            assert icechunk_ops._group_matches(
                repo.readonly_session("main"),
                order,
                icechunk_refs.level_group_spec(
                    icechunk_refs.level_grids(manifest, grid)[int(order)]["grid"]
                ),
            )
            assert dict(group[order].attrs) == dict(fresh_group[order].attrs)
        assert repo.config.manifest.splitting == fresh_repo.config.manifest.splitting
        head = next(iter(repo.ancestry(branch="main")))
        assert head.message == "declare-pyramid"
        assert head.metadata["added"] == r["added"] and head.metadata["dropped"] == []
        assert head.metadata["semantic_hash"] == manifest["semantic_hash"]
        # The base level's refs are untouched: the leaf still reads.
        assert int(group["6"]["count"][:].sum()) > 0
        # Idempotent.
        again = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert again["unchanged"] is True and again["added"] == []
        assert _messages(root)[0] == "declare-pyramid" and _messages(root)[1] != "declare-pyramid"

    def test_a_ref_commit_into_an_added_level_reads_back(self, monkeypatch, cfg, tmp_path):
        import numpy as np
        from zarr.core.buffer import default_buffer_prototype
        from zarr.storage import LocalStore

        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert "5" in r["added"]
        group, repo = _open(root)
        target = group["5"]["count"]
        # An object carrying one chunk of the level's own array model.
        obj = LocalStore(str(tmp_path / "store" / "obj"))
        meta = target.metadata.to_buffer_dict(default_buffer_prototype())["zarr.json"]
        zarr.core.sync.sync(obj.set("count/zarr.json", meta))
        written = zarr.open_array(obj, path="count", mode="r+")
        chunk = tuple(slice(0, c) for c in written.chunks)
        values = np.arange(np.prod(written.chunks), dtype=written.dtype).reshape(written.chunks)
        written[chunk] = values
        key = "c/" + "/".join("0" * written.ndim)
        location = icechunk_refs.container_prefix(root) + f"obj/count/{key}"
        length = (tmp_path / "store" / "obj" / "count" / key).stat().st_size
        # A unit's chunk keys are the object's cell-axis plan; the commit
        # places them at the unit's row (the ``all`` row here, §11.3).
        unit = {
            "level": 5,
            "row": "all",
            "entries": [
                {
                    "path": "count",
                    "refs": 1,
                    "sharded": False,
                    "chunks": [("count/c/0", location, length, None)],
                }
            ],
        }
        out = icechunk_refs.commit_units(root, [unit], "refs into /5", store_kwargs={})
        assert out["levels"] == [5] and out["snapshot"]
        group, repo = _open(root)
        np.testing.assert_array_equal(group["5"]["count"][chunk], values)
        # The repo's persisted split for /5 is the level's own cut (§11.5).
        block = group.attrs[ICECHUNK_ATTR]
        assert icechunk_refs.block_splits(block)["5"] == block["levels"]["5"]["split"]
        want = icechunk_refs._repo_config(root, icechunk_refs.block_splits(block), {})
        assert repo.config.manifest.splitting == want.manifest.splitting
        manifests = repo.inspect_snapshot(out["snapshot"])["manifests"]
        assert [m["num_chunk_refs"] for m in manifests] == [1]

    def test_a_refused_declaration_leaves_the_split_config(self, monkeypatch, cfg, tmp_path):
        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        before = _open(root)[1].config.manifest.splitting

        def refuse(*_a, **_k):
            raise ValueError("refused")

        monkeypatch.setattr(icechunk_ops, "_validate", refuse)
        with pytest.raises(ValueError, match="refused"):
            icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert _open(root)[1].config.manifest.splitting == before

    def test_the_splits_are_cut_from_the_block_as_committed(self, monkeypatch, cfg, tmp_path):
        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        commit = icechunk_ops._commit

        def ratchet_lands_too(session, message, **kw):
            # An init ratchet lands right after this operation's commit.
            out = commit(session, message, **kw)
            _group, repo = _open(root)
            ratchet = repo.writable_session("main")
            root_group = zarr.open_group(ratchet.store, mode="r+")
            root_group.attrs[ICECHUNK_ATTR] = {**root_group.attrs[ICECHUNK_ATTR], "split_order": 1}
            commit(ratchet, "ratchet", local=True)
            return out

        monkeypatch.setattr(icechunk_ops, "_commit", ratchet_lands_too)
        icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        group, repo = _open(root)
        block = group.attrs[ICECHUNK_ATTR]
        assert block["split_order"] == 1
        want = icechunk_refs._repo_config(root, icechunk_refs.block_splits(block), {})
        assert repo.config.manifest.splitting == want.manifest.splitting

    def test_delisting_keeps_the_group_and_redeclaring_reuses_it(self, monkeypatch, cfg, tmp_path):
        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        cfg.output["pyramid"] = False
        _write_manifest(root, grid)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert r["dropped"] == ["1", "2", "3", "4", "5"] and r["added"] == []
        assert r["multiscales"] is False
        group, _repo = _open(root)
        assert list(group.attrs[ICECHUNK_ATTR]["levels"]) == ["6"]
        assert MULTISCALES_ATTR not in group.attrs
        assert {k for k, _ in group.groups()} == {"1", "2", "3", "4", "5", "6"}  # kept
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert r["added"] == ["1", "2", "3", "4", "5"] and r["snapshot"]
        assert sorted(_open(root)[0].attrs[ICECHUNK_ATTR]["levels"], key=int) == [
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
        ]

    def test_levels_follow_the_repos_rows(self, cfg, tmp_path):
        # A level declared after the repo's rows were allocated is built at
        # those rows, and a delisted level's surviving group grows with every
        # later allocation, so relisting it still finds the declared model
        # (§11.2, §11.4). A windowed manifest drives it: the model must hold
        # one before the writer indexes windowed stores (§11.6).
        from test_icechunk_rows import _YEARLY

        def declare():
            manifest = hive.build_manifest(grid, windowing=_YEARLY)
            return icechunk_ops.declare_pyramid(
                root, cfg, store_kwargs={}, manifest=manifest, grid=grid
            )

        def rows_of(order):
            return {arr.shape[0] for _name, arr in _open(root)[0][order].arrays()}

        cfg.output["pyramid"] = False
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        bare = hive.build_manifest(grid, windowing=_YEARLY)
        icechunk_refs.init_repo(
            root, grid, cfg, run_id=RUN, store_kwargs={}, manifest=bare, rows=["2019", "2020"]
        )
        cfg.output.pop("pyramid")
        assert declare()["added"] == ["1", "2", "3", "4", "5"]
        assert rows_of("5") == rows_of("6") == {2}
        cfg.output["pyramid"] = False
        assert declare()["dropped"] == ["1", "2", "3", "4", "5"]
        out = icechunk_refs.init_repo(
            root, grid, cfg, run_id="r2", store_kwargs={}, manifest=bare, rows=["2021"]
        )
        assert out["rows"] == ["2019", "2020", "2021"]
        assert rows_of("5") == rows_of("6") == {3}  # the retired group grew too
        cfg.output.pop("pyramid")
        relisted = declare()
        assert relisted["added"] == ["1", "2", "3", "4", "5"] and relisted["snapshot"]
        assert all(rows_of(order) == {3} for order in ("1", "2", "3", "4", "5", "6"))

    @pytest.mark.parametrize("relist", [False, True])
    def test_an_init_landing_after_the_vet_is_built_at_its_rows(
        self, monkeypatch, cfg, tmp_path, relist
    ):
        # An init that allocates rows between the vet and the operation's
        # session: the new (or relisted) groups are built and checked at the
        # rows the session reads, so the declaration lands (§11.2, §11.4).
        from test_icechunk_rows import _YEARLY

        cfg.output["pyramid"] = False
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        bare = hive.build_manifest(grid, windowing=_YEARLY)
        icechunk_refs.init_repo(
            root, grid, cfg, run_id=RUN, store_kwargs={}, manifest=bare, rows=["2019"]
        )
        cfg.output.pop("pyramid")
        full = hive.build_manifest(grid, windowing=_YEARLY)
        if relist:
            icechunk_ops.declare_pyramid(root, cfg, store_kwargs={}, manifest=full, grid=grid)
            icechunk_ops.declare_pyramid(root, cfg, store_kwargs={}, manifest=bare, grid=grid)
        vet = icechunk_ops.open_vetted
        run2 = copy.deepcopy(cfg)
        run2.output["pyramid"] = False

        def init_lands(*a, **k):
            out = vet(*a, **k)
            icechunk_refs.init_repo(
                root, grid, run2, run_id="r2", store_kwargs={}, manifest=bare, rows=["2020"]
            )
            return out

        monkeypatch.setattr(icechunk_ops, "open_vetted", init_lands)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={}, manifest=full, grid=grid)
        assert r["added"] == ["1", "2", "3", "4", "5"] and r["snapshot"]
        group, _repo = _open(root)
        assert group.attrs[ICECHUNK_ATTR]["rows"] == ["2019", "2020"]
        for order in ("1", "2", "3", "4", "5", "6"):
            assert {arr.shape[0] for _name, arr in group[order].arrays()} == {2}

    def test_a_delisted_level_keeps_its_split_through_a_ratchet_and_relisting(
        self, monkeypatch, cfg, tmp_path
    ):
        cfg.output["icechunk"] = {"commit": "leaf", "split_order": 3}
        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)

        def saved():
            group, repo = _open(root)
            block = group.attrs[ICECHUNK_ATTR]
            want = icechunk_refs._repo_config(root, icechunk_refs.block_splits(block), {})
            assert repo.config.manifest.splitting == want.manifest.splitting
            # One condition per level group, listed or retired, plus the catch-all.
            assert len(repo.config.manifest.splitting.split_sizes) == 7
            return block

        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        cfg.output["pyramid"] = False
        _write_manifest(root, grid)
        icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})  # delist /1../5
        block = saved()
        assert sorted(block["retired"], key=int) == ["1", "2", "3", "4", "5"]
        # An init ratchet re-saves the splits: the retired groups keep theirs, recut.
        cfg.output["icechunk"] = {"commit": "leaf", "split_order": 2, "commit_order": 2}
        out = icechunk_refs.init_repo(root, grid, cfg, run_id="r2", store_kwargs={})
        assert out["split_ratchet"] == {"from": 3, "to": 2}
        block = saved()
        assert block["retired"]["5"]["split"] == icechunk_refs.block_splits(block)["5"]
        # Relisting moves the entries back.
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        r = icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        assert r["added"] == ["1", "2", "3", "4", "5"]
        block = saved()
        assert "retired" not in block and len(block["levels"]) == 6

    def test_another_geometry_or_a_changed_level_is_refused(self, monkeypatch, cfg, tmp_path):
        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        other = default_config("atl06", validate=False)
        other.output.update(cfg.output)
        other.output["grid"] = {**cfg.output["grid"], "child_order": 7}
        with pytest.raises(ValueError, match="was built for"):
            icechunk_ops.declare_pyramid(root, other, store_kwargs={})
        # A recorded level whose geometry the manifest would move.
        icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        group, repo = _open(root)
        session = repo.writable_session("main")
        root_group = zarr.open_group(session.store, mode="r+")
        block = dict(root_group.attrs[ICECHUNK_ATTR])
        block["levels"] = {**block["levels"], "5": {**block["levels"]["5"], "chunk_order": 3}}
        root_group.attrs.put({**root_group.attrs.asdict(), ICECHUNK_ATTR: block})
        session.commit("tamper")
        with pytest.raises(ValueError, match="a new revision, not an operation"):
            icechunk_ops.declare_pyramid(root, cfg, store_kwargs={})
        with pytest.raises(ValueError, match="no morton_hive.json"):
            icechunk_ops.declare_pyramid(str(tmp_path / "bare"), cfg, store_kwargs={})


class TestRetrofitFollowThrough:
    """``sweep_overview.declare_pyramid`` declares both planes in one step."""

    def test_the_manifest_retrofit_mirrors_into_the_repo(self, monkeypatch, cfg, tmp_path):
        from zagg.sweep_overview import declare_pyramid

        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        # ``chunk_order=`` is the grid-less retrofit's /2 lever (issue #520).
        summary = declare_pyramid(root, cfg, chunk_order=5)
        assert summary["updated"] is True
        assert summary["icechunk"]["operation"] == "declare-pyramid"
        assert summary["icechunk"]["added"] == ["1", "2", "3", "4", "5"]
        group, _repo = _open(root)
        assert group.attrs[MULTISCALES_ATTR] == hive.read_manifest(root)[MULTISCALES_ATTR]
        # An unchanged declaration writes neither plane, but still vets the repo.
        n = len(_messages(root))
        assert declare_pyramid(root, cfg, chunk_order=5)["icechunk"]["unchanged"] is True
        assert len(_messages(root)) == n

    def test_an_unchanged_manifest_still_repairs_a_lagging_repo(self, monkeypatch, cfg, tmp_path):
        from zagg.sweep_overview import declare_pyramid

        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)  # the manifest is declared, the repo is not
        summary = declare_pyramid(root, cfg, chunk_order=5)
        assert summary["updated"] is False
        assert summary["icechunk"]["added"] == ["1", "2", "3", "4", "5"]

    def test_a_config_grid_off_the_store_follows_the_block(self, monkeypatch, cfg, tmp_path):
        from zagg.sweep_overview import declare_pyramid

        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)  # built at chunk 5
        cfg.output.pop("pyramid")
        cfg.output["grid"] = {k: v for k, v in cfg.output["grid"].items() if k != "chunk_inner"}
        summary = declare_pyramid(root, cfg, chunk_order=5)  # the config's grid says chunk 4
        assert "error" not in summary["icechunk"]
        assert summary["icechunk"]["added"] == ["1", "2", "3", "4", "5"]

    def test_without_a_repo_and_on_a_repo_error(self, monkeypatch, cfg, tmp_path):
        from zagg.sweep_overview import declare_pyramid

        cfg.output["pyramid"] = False
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        _write_manifest(root, grid)
        cfg.output.pop("pyramid")
        assert declare_pyramid(root, cfg, chunk_order=5)["icechunk"] is None
        # With a repo whose update fails, the manifest write still stands (D9).
        cfg.output["pyramid"] = False
        _write_manifest(root, grid)
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN, store_kwargs={})
        cfg.output.pop("pyramid")
        head = _messages(root)

        def boom(*_a, **_k):
            raise RuntimeError("boom")

        # The callee raises, so ``_declare_into_repo``'s own fail-open catch runs.
        monkeypatch.setattr(icechunk_ops, "declare_pyramid", boom)
        summary = declare_pyramid(root, cfg, chunk_order=5)
        assert summary["updated"] is True and summary["icechunk"] == {
            "error": "RuntimeError('boom')"
        }
        assert hive.read_manifest(root)[MULTISCALES_ATTR]
        assert _messages(root) == head


class TestCli:
    def test_set_attrs_and_declare_pyramid(self, monkeypatch, cfg, tmp_path, capsys):
        import zagg.config as config_mod

        grid, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        assert icechunk_ops.main([root, "set-attrs", "/", '{"title": "x"}']) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["operation"] == "set-attrs" and out["attrs"]["title"] == "x"
        cfg.output.pop("pyramid")
        _write_manifest(root, grid)
        monkeypatch.setattr(config_mod, "load_config", lambda path: cfg)
        assert icechunk_ops.main([root, "declare-pyramid", "cfg.yaml"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["operation"] == "declare-pyramid" and out["added"] == ["1", "2", "3", "4", "5"]

    def test_error_paths(self, monkeypatch, cfg, tmp_path, capsys):
        _grid_, root = _store(monkeypatch, cfg, tmp_path, leaf=False)
        n = len(_messages(root))
        with pytest.raises(json.JSONDecodeError):
            icechunk_ops.main([root, "set-attrs", "/", "{not json"])
        with pytest.raises(SystemExit):
            icechunk_ops.main([root, "rename-template", "x"])
        assert "invalid choice" in capsys.readouterr().err
        with pytest.raises(ValueError, match="not initialized"):
            icechunk_ops.main([str(tmp_path / "bare"), "set-attrs", "/", '{"a": 1}'])
        assert len(_messages(root)) == n


# -- finalize (issue #588) --------------------------------------------------------


class _FinalizeFixtures:
    """A run's dispatch manifest and staged-sweep records, as the workers write them."""

    DISPATCHED = "2026-01-01T00:00:00+00:00"

    def _manifest(self, root, run_id, cfg, *, dispatched_at=DISPATCHED, slim=False):
        """The run's dispatch manifest, as the setup worker writes it (issue #327).

        ``slim``: through the real halves — the dispatcher's slim block, the
        worker's write — as a large hive run's setup event carries it.
        """
        from dataclasses import asdict

        import obstore

        from zagg import client_transport as ct
        from zagg.semantics import semantic_hash
        from zagg.store import open_object_store

        if slim:
            block = ct.build_run_manifest_block(run_id, [1, 2, 3], cfg)
            event = {
                "store_path": root,
                "config": asdict(cfg),
                "run_manifest": ct.slim_run_manifest_block(block),
            }
            ct.write_dispatch_manifest(event, {})
            return
        manifest = {
            "schema_version": 1,
            "run_id": run_id,
            "shards": ["1"],
            "semantic_hash": semantic_hash(cfg),
            "dispatched_at": dispatched_at,
            "dataset": None,
            "config": asdict(cfg),
        }
        store = open_object_store(ct.run_status_prefix(root, run_id))
        obstore.put(store, ct.MANIFEST_NAME, json.dumps(manifest).encode())

    @staticmethod
    def _stamp(offset_s=60):
        """A record-key stamp ``offset_s`` from now (the run's init commit is ~now)."""
        from datetime import datetime, timedelta, timezone

        return (datetime.now(timezone.utc) + timedelta(seconds=offset_s)).strftime("%Y%m%dT%H%M%SZ")

    def _record(self, root, ts=None, **fields):
        """A staged-sweep run record at the store root, as the finisher writes it.

        The root-record shape of ``sweep_stages.run_stage_finisher`` through
        ``_write_stage_record`` (``{"spec", "mode": "stages", **summary}``),
        with the sweep's own run id and ``run_finisher``'s block; ``fields``
        override it.
        """
        from zagg.store import open_object_store, put_object
        from zagg.sweep import SWEEP_SPEC

        ts = ts or self._stamp()
        record = {
            "spec": SWEEP_SPEC,
            "mode": "stages",
            "run_id": "sweep-0001",
            # The run the sweep completes (issue #593): what finalize matches.
            "pipeline_run_id": RUN,
            "store_root": root,
            "shard_order": 4,
            "transport": "lambda",
            "n_leaves": 1,
            "skipped_leaves": 0,
            "stage_records": 1,
            "stages": [],
            "levels": {},
            "barrier_timed_out": False,
            "finisher": {
                "root_moc": True,
                "manifest_updated": True,
                "objects_touched": 0,
                "touch_failures": 0,
                "lease_released": True,
            },
            "lease": {"released": True},
            "duration_s": 1.0,
            **fields,
        }
        key = f"sweep_stats_{ts}_stages.json"
        put_object(open_object_store(root), key, json.dumps(record, indent=1).encode())
        return key


class TestFinalizeOperation(_FinalizeFixtures):
    """``finalize <run_id>``: tag a completed, untagged ladder run — the newest only."""

    def test_tags_a_completed_newest_untagged_run(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        # The dispatched config is what the manifest carries: its knob rules.
        cfg.output["icechunk"] = {"commit": "ladder", "retain_runs": 2}
        self._manifest(root, RUN, cfg)
        key = self._record(root)
        out = icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert out["operation"] == "finalize" and out["run_id"] == RUN
        assert out["stage_record"] == key and out["tagged"] is True and "skipped" not in out
        assert out["retain_runs"] == 2  # the run config's knob, not a flag
        _group, repo = _open(root)
        assert repo.lookup_tag(f"run-{RUN}") == out["snapshot"]
        head = next(iter(repo.ancestry(branch="main")))
        assert head.message == f"finalize {RUN}"
        assert head.metadata["run_id"] == RUN and head.metadata["retain_runs"] == 2
        # Idempotent: the tag exists, nothing is written, the report says so.
        n = len(_messages(root))
        again = icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert again["tagged"] is False and again["skipped"] == f"run-{RUN} already exists"
        assert again["snapshot"] == out["snapshot"] and len(_messages(root)) == n

    def test_tags_a_run_whose_manifest_is_slim(self, monkeypatch, cfg, tmp_path):
        # A large hive run's manifest carries no shard list (issue #588); the
        # config is all finalize reads, so the run is tagged all the same.
        from zagg import client_transport as ct

        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        cfg.output["icechunk"] = {"commit": "ladder", "retain_runs": 2}
        self._manifest(root, RUN, cfg, slim=True)
        manifest = ct.read_dispatch_manifest(ct.run_status_prefix(root, RUN), {})
        assert manifest["shards"] is None and manifest["shards_omitted"] == 3
        key = self._record(root)
        out = icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert out["tagged"] is True and out["stage_record"] == key
        assert out["retain_runs"] == 2
        assert _open(root)[1].lookup_tag(f"run-{RUN}") == out["snapshot"]

    @pytest.mark.parametrize(
        "fields, reason",
        [
            ({"barrier_timed_out": True}, "a barrier expired"),
            ({"finisher": None}, "no finisher block"),
            # The in-process sweep's failure record; refused even with a finisher block.
            ({"error": "RuntimeError: boom", "finisher": {"lease_released": True}}, "sweep failed"),
        ],
    )
    def test_refuses_an_incomplete_sweep_record(self, monkeypatch, cfg, tmp_path, fields, reason):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root, **fields)
        n = len(_messages(root))
        with pytest.raises(ValueError, match=reason):
            icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert len(_messages(root)) == n and not _open(root)[1].list_tags()

    @pytest.mark.parametrize("newer_complete", [False, True])
    def test_the_newest_record_decides(self, monkeypatch, cfg, tmp_path, newer_complete):
        # Two records since the init: only the newest stands for the ladder. A
        # newer barrier-expired record refuses despite an older complete one;
        # a newer complete one tags despite an older incomplete one.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root, ts=self._stamp(60), barrier_timed_out=newer_complete)
        newer = self._record(root, ts=self._stamp(120), barrier_timed_out=not newer_complete)
        if newer_complete:
            out = icechunk_ops.finalize(root, RUN, store_kwargs={})
            assert out["tagged"] is True and out["stage_record"] == newer
        else:
            with pytest.raises(ValueError, match=f"{newer} does not show a completed sweep"):
                icechunk_ops.finalize(root, RUN, store_kwargs={})
            assert not _open(root)[1].list_tags()

    @pytest.mark.parametrize("named", ["another-run", None, "absent"])
    def test_refuses_a_record_that_names_another_run_or_none(
        self, monkeypatch, cfg, tmp_path, named
    ):
        # Issue #593: the only completed record since the run opened is a
        # sibling run's sweep, an unnamed ``--stages`` pass, or one written
        # before the key existed. Time alone no longer vouches.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        key = self._record(root, pipeline_run_id=named)
        if named == "absent":
            store_path = f"{root}/{key}"
            record = json.loads(open(store_path).read())
            del record["pipeline_run_id"]
            open(store_path, "w").write(json.dumps(record))
        n = len(_messages(root))
        recorded = None if named == "absent" else named
        with pytest.raises(icechunk_ops.FinalizeRefusedError) as refused:
            icechunk_ops.finalize(root, RUN, store_kwargs={})
        message = str(refused.value)
        assert f"names this run: the newest, {key}, records pipeline_run_id {recorded!r}" in message
        assert f"--stages --pipeline-run-id {RUN}" in message  # the remedy, runnable as written
        assert not _open(root)[1].list_tags() and len(_messages(root)) == n

    def test_the_runs_own_record_tags_whatever_landed_after_it(self, monkeypatch, cfg, tmp_path):
        # The newest record NAMING the run decides: a later unnamed pass and a
        # sibling run's sweep neither vouch nor un-vouch.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        own = self._record(root, ts=self._stamp(60))
        self._record(root, ts=self._stamp(120), pipeline_run_id=None)
        self._record(root, ts=self._stamp(180), pipeline_run_id="another-run", error="boom")
        out = icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert out["tagged"] is True and out["stage_record"] == own

    def test_the_runs_own_record_must_postdate_its_init_commit(self, monkeypatch, cfg, tmp_path):
        # The time anchor stands as the second condition: a record naming the
        # run but older than its init commit is a previous run's of that name.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root, ts=self._stamp(-3600))
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="no staged-sweep record"):
            icechunk_ops.finalize(root, RUN, store_kwargs={})

    def test_a_named_stages_pass_completes_a_run_whose_dispatcher_died(
        self, monkeypatch, cfg, tmp_path
    ):
        # Issue #593's third acceptance: the dispatcher dies after the fan-out
        # — no staged sweep, no finalize. A ``--stages`` pass that names no
        # run builds the ladder but vouches for none; the same pass naming
        # the run writes the record ``finalize`` accepts. Real records, from
        # the real CLI, over a real ladder run.
        import zagg.sweep_stages as stages_mod
        from zagg import runner
        from zagg.sweep import main as sweep_main

        monkeypatch.setattr(stages_mod, "stage_sweep_after_run", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_finalize_icechunk_local", lambda *a, **k: None)
        shards = _shards(_grid(cfg), 3)
        _grid_, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        run_id = summary["results"][0]["stats"]["run_id"]
        assert summary["icechunk_finalize"] is None and not _open(root)[1].list_tags()
        # A Lambda dispatcher's setup invoke writes the manifest finalize reads.
        self._manifest(root, run_id, runner._pin_icechunk_commit(cfg, _grid_, stages=True))
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="no staged-sweep record"):
            icechunk_ops.finalize(root, run_id, store_kwargs={})
        assert sweep_main([root, "--stages"]) == 0
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="pipeline_run_id None"):
            icechunk_ops.finalize(root, run_id, store_kwargs={})
        assert not _open(root)[1].list_tags()
        time.sleep(1.1)  # record keys resolve to one second
        assert sweep_main([root, "--stages", "--pipeline-run-id", run_id]) == 0
        out = icechunk_ops.finalize(root, run_id, store_kwargs={})
        assert out["tagged"] is True and out["tag"] == f"run-{run_id}"
        record = json.loads(open(f"{root}/{out['stage_record']}").read())
        assert record["pipeline_run_id"] == run_id and record["lease"]["released"]

    def test_refuses_without_a_record_since_the_init_commit(self, monkeypatch, cfg, tmp_path):
        # No record at all, then one older than the run's init commit — though
        # newer than the manifest's ``dispatched_at``: the anchor is the repo's
        # clock, so neither can stand for this run's ladder.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        with pytest.raises(ValueError, match="no staged-sweep record"):
            icechunk_ops.finalize(root, RUN, store_kwargs={})
        self._record(root, ts=self._stamp(-3600))
        with pytest.raises(ValueError, match="since run r1's init commit"):
            icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert not _open(root)[1].list_tags()

    def test_refuses_a_run_with_no_init_commit(self, monkeypatch, cfg, tmp_path):
        # A manifest and a record, but the repo never saw the run open.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, "r9", cfg)
        self._record(root)
        with pytest.raises(ValueError, match="no init commit for run r9"):
            icechunk_ops.finalize(root, "r9", store_kwargs={})
        assert not _open(root)[1].list_tags()

    def test_refuses_without_a_dispatch_manifest(self, monkeypatch, cfg, tmp_path):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._record(root)
        with pytest.raises(ValueError, match="no dispatch manifest") as err:
            icechunk_ops.finalize(root, RUN, store_kwargs={})
        # A large run has a slim manifest, so the text names only what is
        # left: a wrong store/run_id or a pre-#327 dispatcher first, then a
        # lost write, or a block that did not fit even slim.
        assert "slim (no shard list) when the run is large" in str(err.value)
        assert "wrong store/run_id, a dispatcher predating issue #327" in str(err.value)
        assert 'dispatch_manifest: "dropped"' in str(err.value)
        assert "the next run's tag covers its commits" in str(err.value)

    def test_skips_a_run_that_is_no_longer_the_newest(self, monkeypatch, cfg, tmp_path):
        # A later run's init commit makes this one an older untagged run: the
        # next run's tag covers it, and tagging it would name later commits.
        grid, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root)
        icechunk_refs.init_repo(root, grid, cfg, run_id="r2", store_kwargs={})
        n = len(_messages(root))
        out = icechunk_ops.finalize(root, RUN, store_kwargs={})
        assert out["tagged"] is False and out["snapshot"] is None
        assert out["skipped"] == f"a later run has committed since run {RUN}"
        assert len(_messages(root)) == n and not _open(root)[1].list_tags()

    def test_cli(self, monkeypatch, cfg, tmp_path, capsys):
        # A local store finalizes in-process, whatever function is named: no
        # worker can reach the path, and no Lambda client is ever built.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root)
        monkeypatch.setattr(icechunk_ops, "_lambda_client", _fail("built a Lambda client"))
        assert icechunk_ops.main([root, "finalize", RUN, "--function-name", "fn"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["operation"] == "finalize" and out["tagged"] is True
        assert out["tag"] == f"run-{RUN}" and "invoke_s" not in out

    def test_refusals_are_finalize_refused(self, monkeypatch, cfg, tmp_path):
        # The four precondition refusals share one type (a ``ValueError``), so
        # the worker can tell a refusal from a failure.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="no dispatch manifest"):
            icechunk_ops.finalize_run(root, RUN, store_kwargs={})
        self._manifest(root, RUN, cfg)
        self._manifest(root, "r9", cfg)
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="no init commit"):
            icechunk_ops.finalize_run(root, "r9", store_kwargs={})
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="no staged-sweep record"):
            icechunk_ops.finalize_run(root, RUN, store_kwargs={})
        self._record(root, barrier_timed_out=True)
        with pytest.raises(icechunk_ops.FinalizeRefusedError, match="a barrier expired"):
            icechunk_ops.finalize_run(root, RUN, store_kwargs={})
        assert issubclass(icechunk_ops.FinalizeRefusedError, ValueError)


# -- finalize on a non-local store: the worker's (issue #588 phase 4) --------------

REMOTE = "s3://bucket/store"

#: The whole operator event: no ``config``, no init record.
OPERATOR_EVENT = {
    "mode": "icechunk_finalize",
    "store_path": REMOTE,
    "run_id": RUN,
    "newest_only": True,
    "operator_checks": True,
}


def _fail(what):
    def touched(*_a, **_k):
        raise AssertionError(f"the operator's host {what}")

    return touched


class _Worker:
    """A stub Lambda client: a canned response, or the REAL handler on a local store.

    With ``handler`` the invoke runs ``lambda_handler`` in-process on
    ``root``, which stands for the bucket the event names; ``working`` is
    true only inside the invoke, so the audit can tell the worker's store
    access from the host's.
    """

    def __init__(self, body=None, *, status=200, handler=None, root=None):
        self.events: list = []
        self.working = False
        self._body, self._status, self._handler, self._root = body, status, handler, root

    def invoke(self, **kwargs):
        event = json.loads(kwargs["Payload"])
        self.events.append((kwargs["FunctionName"], kwargs["InvocationType"], event))
        if self._handler is None:
            result = {"statusCode": self._status, "body": json.dumps(self._body)}
        else:
            self.working = True
            try:
                result = self._handler.lambda_handler({**event, "store_path": self._root}, None)
            finally:
                self.working = False
        return {"Payload": io.BytesIO(json.dumps(result).encode())}


@pytest.fixture
def on_host(monkeypatch):
    """``with on_host(worker):`` — the operator's host, where any store or repo access fails.

    The D8 audit for the operator command: inside the block the object
    reads, writes and listings, the repo open and
    :func:`icechunk_ops.finalize_run` itself may run only while ``worker``
    is serving an invoke (never, without a worker).
    """
    import contextlib

    import obstore

    state = {"host": False, "worker": None}

    def guard(module, name):
        real = getattr(module, name)

        def guarded(*a, **k):
            worker = state["worker"]
            if state["host"] and not (worker is not None and worker.working):
                raise AssertionError(f"the operator's host called {name}")
            return real(*a, **k)

        monkeypatch.setattr(module, name, guarded)

    for name in ("get", "put", "list", "list_with_delimiter"):
        guard(obstore, name)
    guard(icechunk_refs, "open_repo")
    guard(icechunk_ops, "finalize_run")

    @contextlib.contextmanager
    def host(worker=None):
        state.update(host=True, worker=worker)
        try:
            yield
        finally:
            state.update(host=False, worker=None)

    return host


@pytest.fixture(scope="module")
def handler_mod():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_588", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_REPORT = {
    "ok": True,
    "mode": "icechunk_finalize",
    "operation": "finalize",
    "run_id": RUN,
    "stage_record": "sweep_stats_20260101T000100Z_stages.json",
    "path": f"{REMOTE}/icechunk",
    "tag": f"run-{RUN}",
    "snapshot": "SNAP",
    "tagged": True,
}


class TestFinalizeInvoke:
    """On a non-local store the command fires one worker invoke and writes nothing itself."""

    def _client(self, monkeypatch, worker):
        regions = []
        monkeypatch.setattr(
            icechunk_ops, "_lambda_client", lambda region: regions.append(region) or worker
        )
        return regions

    def test_cli_fires_one_invoke_and_touches_no_store(self, monkeypatch, on_host, capsys):
        worker = _Worker(_REPORT)
        regions = self._client(monkeypatch, worker)
        argv = [REMOTE, "--region", "us-east-9", "finalize", RUN, "--function-name", "fn"]
        with on_host():
            assert icechunk_ops.main(argv) == 0
        assert worker.events == [("fn", "RequestResponse", OPERATOR_EVENT)]
        assert regions == ["us-east-9"]
        out = json.loads(capsys.readouterr().out)
        assert out.pop("invoke_s") >= 0.0
        assert out.pop("function_name") == "fn"  # the report names the function invoked
        assert out == {k: v for k, v in _REPORT.items() if k not in ("ok", "mode")}

    def test_function_name_is_the_flag_then_the_environment(self, monkeypatch):
        monkeypatch.setenv("ZAGG_LAMBDA_FUNCTION_NAME", "process-shard-test")
        worker = _Worker(_REPORT)
        by_env = icechunk_ops.finalize(REMOTE, RUN, store_kwargs={}, lambda_client=worker)
        by_flag = icechunk_ops.finalize(
            REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
        )
        assert [name for name, _kind, _event in worker.events] == ["process-shard-test", "fn"]
        assert [by_env["function_name"], by_flag["function_name"]] == ["process-shard-test", "fn"]
        assert all(event == OPERATOR_EVENT for _name, _kind, event in worker.events)

    @pytest.mark.parametrize("env", [None, ""])
    def test_defaults_to_the_runners_function_and_never_runs_on_the_host(
        self, monkeypatch, on_host, capsys, env
    ):
        # Neither the flag nor the environment (unset, or set empty) names a
        # function: one invoke at the runner's own default, still no
        # in-process finalize and no store access from the host.
        from zagg import runner

        if env is None:
            monkeypatch.delenv("ZAGG_LAMBDA_FUNCTION_NAME", raising=False)
        else:
            monkeypatch.setenv("ZAGG_LAMBDA_FUNCTION_NAME", env)
        worker = _Worker(_REPORT)
        self._client(monkeypatch, worker)
        with on_host():
            assert icechunk_ops.main([REMOTE, "finalize", RUN]) == 0
        assert worker.events == [("process-shard", "RequestResponse", OPERATOR_EVENT)]
        assert runner.DEFAULT_FUNCTION_NAME == "process-shard"
        assert json.loads(capsys.readouterr().out)["function_name"] == "process-shard"

    def test_the_default_is_the_runners_rule_without_a_worker_block(self, monkeypatch, cfg):
        # One default, not two: what the dispatchers resolve for a config with
        # no ``worker:`` block is what the operator finalize invokes.
        from zagg import runner

        monkeypatch.delenv("ZAGG_LAMBDA_FUNCTION_NAME", raising=False)
        assert not cfg.worker
        worker = _Worker(_REPORT)
        icechunk_ops.finalize(REMOTE, RUN, store_kwargs={}, lambda_client=worker)
        assert worker.events[0][0] == runner._resolve_function_name(cfg, None)

    def test_a_worker_refusal_is_raised_with_its_reason(self, on_host):
        worker = _Worker({"ok": False, "mode": "icechunk_finalize", "refused": "no record"})
        with on_host(), pytest.raises(icechunk_ops.FinalizeRefusedError, match="^no record$"):
            icechunk_ops.finalize(
                REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
            )
        assert len(worker.events) == 1

    @pytest.mark.parametrize(
        "body, status, match",
        [
            ({"error": "'config'", "mode": "icechunk_finalize"}, 500, "'config'"),
            ({"error": "Missing shard_key"}, 400, "statusCode 400"),
            ({"ok": True, "mode": "icechunk_finalize"}, 200, "unexpected icechunk_finalize body"),
        ],
    )
    def test_a_worker_failure_raises_and_is_not_retried(self, on_host, body, status, match):
        worker = _Worker(body, status=status)
        with on_host(), pytest.raises(RuntimeError, match=match) as err:
            icechunk_ops.finalize(
                REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
            )
        assert "nothing was written from this host" in str(err.value)
        # Only the missing-config failure is read as a worker older than the operation.
        stale = status == 500
        assert ("predates the operator finalize" in str(err.value)) == stale
        assert ("did not finalize" in str(err.value)) == stale
        assert ("may not have finalized" in str(err.value)) != stale
        assert len(worker.events) == 1

    def test_a_lost_response_is_an_unknown_outcome_with_a_rerun_hint(self, on_host, caplog):
        # The request may have reached the worker: the host cannot say it did not tag.
        class _Dropped(_Worker):
            def invoke(self, **kwargs):
                super().invoke(**kwargs)
                raise ConnectionError("connection dropped")

        worker = _Dropped(_REPORT)
        with (
            caplog.at_level("WARNING", logger="zagg.runner"),
            on_host(),
            pytest.raises(RuntimeError, match="may not have finalized") as err,
        ):
            icechunk_ops.finalize(
                REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
            )
        assert f"re-run finalize, which reports an existing run-{RUN} tag" in str(err.value)
        assert "fail-open" not in caplog.text
        assert "raised to the operator, issue #588" in caplog.text
        assert len(worker.events) == 1


class TestFinalizeThroughTheWorker(_FinalizeFixtures):
    """``operator_checks`` in the real handler: the checks, the refusals, the tag."""

    def _invoke(self, handler_mod, root, run_id=RUN, **extra):
        event = {**OPERATOR_EVENT, "store_path": root, "run_id": run_id, **extra}
        resp = handler_mod.lambda_handler(event, None)
        return resp["statusCode"], json.loads(resp["body"])

    def test_cli_finalizes_a_slim_manifest_run_through_the_worker(
        self, monkeypatch, on_host, handler_mod, cfg, tmp_path, capsys
    ):
        # The whole path: the command's one invoke, the handler reading the
        # (slim) manifest for the config, checking and tagging. Every store
        # access is inside the invoke.
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        cfg.output["icechunk"] = {"commit": "ladder", "retain_runs": 2}
        self._manifest(root, RUN, cfg, slim=True)
        key = self._record(root)
        worker = _Worker(handler=handler_mod, root=root)
        monkeypatch.setattr(icechunk_ops, "_lambda_client", lambda region: worker)
        with on_host(worker):
            assert icechunk_ops.main([REMOTE, "finalize", RUN, "--function-name", "fn"]) == 0
        assert worker.events == [("fn", "RequestResponse", OPERATOR_EVENT)]
        out = json.loads(capsys.readouterr().out)
        assert out["operation"] == "finalize" and out["run_id"] == RUN
        assert out["tagged"] is True and out["stage_record"] == key and "skipped" not in out
        assert out["retain_runs"] == 2  # the manifest config's knob: the event has no config
        _group, repo = _open(root)
        assert repo.lookup_tag(f"run-{RUN}") == out["snapshot"]
        head = next(iter(repo.ancestry(branch="main")))
        assert head.message == f"finalize {RUN}" and head.metadata["retain_runs"] == 2
        # Idempotent through the worker too: the tag exists, nothing is written.
        n = len(_messages(root))
        with on_host(worker):
            again = icechunk_ops.finalize(
                REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
            )
        assert again["tagged"] is False and again["skipped"] == f"run-{RUN} already exists"
        assert len(_messages(root)) == n

    @pytest.mark.parametrize(
        "case, reason",
        [
            ("no manifest", "no dispatch manifest"),
            ("no init commit", "no init commit for run r9"),
            ("no record", "no staged-sweep record"),
            ("old record", "since run r1's init commit"),
            ("barrier", "a barrier expired"),
            ("no finisher", "no finisher block"),
            ("sweep error", "the sweep failed"),
        ],
    )
    def test_each_refusal_comes_back_from_the_handler(
        self, monkeypatch, handler_mod, cfg, tmp_path, case, reason
    ):
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        run_id = "r9" if case == "no init commit" else RUN
        if case != "no manifest":
            self._manifest(root, run_id, cfg)
        if case == "old record":
            self._record(root, ts=self._stamp(-3600))
        elif case != "no record":
            fields = {
                "barrier": {"barrier_timed_out": True},
                "no finisher": {"finisher": None},
                "sweep error": {"error": "RuntimeError: boom"},
            }.get(case, {})
            self._record(root, **fields)
        n = len(_messages(root))
        status, body = self._invoke(handler_mod, root, run_id)
        assert status == 200
        assert set(body) == {"ok", "mode", "refused"}
        assert body["ok"] is False and body["mode"] == "icechunk_finalize"
        assert reason in body["refused"]
        assert len(_messages(root)) == n and not _open(root)[1].list_tags()

    def test_a_run_no_longer_the_newest_is_skipped_not_refused(
        self, monkeypatch, handler_mod, cfg, tmp_path
    ):
        grid, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        self._record(root)
        icechunk_refs.init_repo(root, grid, cfg, run_id="r2", store_kwargs={})
        n = len(_messages(root))
        # ``operator_checks`` implies ``newest_only``, stated in the event or not.
        for extra in ({}, {"newest_only": False}):
            status, body = self._invoke(handler_mod, root, **extra)
            assert status == 200 and body["ok"] is True and body["tagged"] is False
            assert body["skipped"] == f"a later run has committed since run {RUN}"
        assert len(_messages(root)) == n and not _open(root)[1].list_tags()

    def test_a_missing_repo_is_a_500_not_a_refusal(self, handler_mod, cfg, tmp_path):
        root = str(tmp_path / "bare")
        self._manifest(root, RUN, cfg)
        status, body = self._invoke(handler_mod, root)
        assert status == 500 and "not initialized" in body["error"]

    def test_a_worker_predating_the_flag_fails_before_any_write(
        self, monkeypatch, handler_mod, cfg, tmp_path
    ):
        # A deployed worker that does not know ``operator_checks`` runs the
        # dispatcher's branch on the operator's event — the event below, the
        # flag unseen. It carries no ``config``, so the worker fails on the
        # key before it opens the repo; had the config ridden along, it would
        # have tagged ``newest_only`` with none of the sweep checks (no stage
        # record exists here).
        _grid_, root = _store(monkeypatch, cfg, tmp_path)
        self._manifest(root, RUN, cfg)
        assert "config" not in OPERATOR_EVENT and "icechunk_init" not in OPERATOR_EVENT
        stale = {k: v for k, v in OPERATOR_EVENT.items() if k != "operator_checks"}
        opened = []
        real = icechunk_refs.open_repo
        monkeypatch.setattr(
            icechunk_refs, "open_repo", lambda *a, **k: opened.append(a) or real(*a, **k)
        )
        resp = handler_mod.lambda_handler({**stale, "store_path": root}, None)
        assert resp["statusCode"] == 500
        assert json.loads(resp["body"]) == {"error": "'config'", "mode": "icechunk_finalize"}
        assert opened == []
        assert _messages(root)[0] != f"finalize {RUN}" and not _open(root)[1].list_tags()
        # The operator's host turns that 500 into a raise that says so.
        worker = _Worker(json.loads(resp["body"]), status=500)
        with pytest.raises(RuntimeError, match="predates the operator finalize"):
            icechunk_ops.finalize(
                REMOTE, RUN, store_kwargs={}, lambda_client=worker, function_name="fn"
            )
