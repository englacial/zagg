"""``zagg.icechunk_ops`` — metadata commits as operations (spec §11.4, issue #582)."""

from __future__ import annotations

import json

import pytest
import zarr
from test_icechunk_refs import _grid, _open, _shards, _write_leaf

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
            zarr.open_array(session.store, path="6/count", mode="r+").resize((8,))
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
        assert {k for k, _ in group.members()} == set(block["levels"])
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
            icechunk_refs._update_block(repo, {"split_order": 1}, "ratchet", local=True, path="")
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
        assert {k for k, _ in group.members()} == {"1", "2", "3", "4", "5", "6"}  # kept
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
        with pytest.raises(ValueError, match="/2 revision, not an operation"):
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
