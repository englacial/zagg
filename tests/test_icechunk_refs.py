"""Icechunk companion repos (issue #580, spec §11): init, per-leaf refs, read-back.

Everything runs on ``local_filesystem_storage`` with a ``file://`` container —
the same code path the local backend takes, end to end through the production
leaf writer (``hive.process_and_write_hive``), so the ref arithmetic is pinned
against real shard objects, never against a hand-built index.
"""

from __future__ import annotations

import json
import threading
import time

import numpy as np
import pandas as pd
import pytest
import zarr

from zagg import hive, icechunk_refs
from zagg.config import default_config, get_data_vars
from zagg.grids.healpix import HealpixGrid
from zagg.grids.morton import morton_decimal, morton_word

icechunk = pytest.importorskip("icechunk")

RUN_ID = "run-580"


@pytest.fixture
def cfg():
    return default_config("atl06", validate=False)


def _grid(cfg, *, sharded=True):
    cfg.aggregation["variables"].setdefault(
        "h",
        {
            "function": "np.sort",
            "source": "h_li",
            "kind": "ragged",
            "inner_shape": [1],
            "dtype": "float32",
            "fill_value": 0,
        },
    )
    # Shard order 4 / chunk order 5 / cell order 6: 16 cells per leaf, K = 4
    # inner chunks of 4 cells — the spec §7 fixture geometry.
    return HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=sharded)


#: A minimal explicit windowing declaration (§11.6's first out-of-scope shape).
_WINDOWING = {
    "schedule": "explicit",
    "time_field": "h_li",  # a declared column, so validate_config passes
    "epoch": "2018-01-01T00:00:00Z",
    "windows": [{"label": "w1", "start": "2020-01-01T00:00:00Z", "end": "2021-01-01T00:00:00Z"}],
}

#: Six distinct shard-order-4 leaves under base cell 1, as decimal ids.
_LEAVES = ("11111", "11112", "11113", "11114", "11121", "11122")


def _shards(grid, n):
    assert grid.parent_order == 4
    return [int(morton_word(d)) for d in _LEAVES[:n]]


def _carrier(grid, shard, fill):
    from zagg.config import get_agg_fields, get_output_signature

    coords = grid.chunk_coords(shard)
    n = len(coords["morton"])
    agg = get_agg_fields(grid.config)
    df = pd.DataFrame(
        {
            var: np.full(n, fill, dtype=np.int32 if var == "count" else np.float32)
            for var in get_data_vars(grid.config)
            if get_output_signature(agg[var])["kind"] != "ragged"
        }
    )
    for name, vals in coords.items():
        df[name] = vals
    return df


def _write_leaf(
    monkeypatch, grid, root, shard, *, fill=1.0, ragged=None, skip_chunks=(), refs=False
):
    """One leaf through the production writer; returns the worker metadata.

    ``refs`` arms the worker-side refs commit (``output.icechunk``); the
    ``record_leaf``-level tests keep it OFF so they own the commit themselves.
    Tri-state: ``None`` leaves ``output`` untouched, so the worker resolves the
    knob from its PRODUCTION default (absent key -> on for a hive writer).
    """
    import zagg.processing as processing

    if refs is not None:
        # ``refs``: True -> the per-leaf commit; "ladder" -> the sidecar;
        # False -> off; None -> leave the config untouched (the fleet default).
        if refs is None:
            grid.config.output.pop("icechunk", None)
        elif refs is False:
            grid.config.output["icechunk"] = False
        else:
            grid.config.output["icechunk"] = {"commit": "leaf" if refs is True else refs}

    def fake(g, shard_key, urls, **kwargs):
        sink = kwargs.get("chunk_results")
        for i, (block, _children) in enumerate(grid.iter_chunks(int(shard_key))):
            if i in skip_chunks:
                continue
            carrier = _carrier(grid, shard_key, fill + i)
            local = grid.shard_local_region(block, int(shard_key))
            sub = carrier.iloc[local[0]]
            rag = ragged if (ragged and i == 0) else {}
            if sink is not None:
                sink.append((block, sub, rag))
            else:
                kwargs["write_chunk"](block, sub, rag)
        return pd.DataFrame(), {
            "shard_key": int(shard_key),
            "cells_with_data": 5,
            "total_obs": 7,
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": None,
            # The real process_shard seeds these; the write/hash/icechunk
            # timings ride an existing dict only.
            "phase_timings": {"read": 0.0, "index": 0.0, "aggregate": 0.0},
        }

    monkeypatch.setattr(processing, "process_shard", fake)
    return hive.process_and_write_hive(
        shard, ["s3://bucket/g.h5"], grid, {}, root, grid.config, store_kwargs={}
    )


def _open(root):
    repo = icechunk_refs.open_repo(root, store_kwargs={})
    return zarr.open_group(repo.readonly_session("main").store, mode="r"), repo


class TestSplit:
    def test_exponent_is_one_manifest_per_split_order_cell(self):
        # Production geometry (chunk order 13): split at the default commit
        # order 6 -> 4^7 chunks per manifest, one order-6 cell.
        assert icechunk_refs.split_exponent(13, 6) == 7
        # The global-scale setting: split 4 -> one order-4 cell per manifest.
        assert icechunk_refs.split_exponent(13, 4) == 9

    def test_exponent_never_below_one_chunk(self):
        # An overview repo whose chunks are coarser than the split (one chunk
        # per order-3 node, split 6): one chunk per manifest.
        assert icechunk_refs.split_exponent(3, 6) == 0

    def test_exponent_caps_at_a_base_cell(self):
        assert icechunk_refs.split_exponent(5, 0) == 5

    def test_block_names_the_cell_order(self, cfg):
        grid = HealpixGrid(9, 19, config=cfg, chunk_inner=13)
        assert icechunk_refs.split_block(grid, 6) == {"chunks": 4**7, "order": 6}
        assert icechunk_refs.split_block(grid, 4) == {"chunks": 4**9, "order": 4}

    def test_split_clears_the_dictionary_gate_by_construction(self):
        # §11.5 (informative): a leaf is 4^(chunk_order - shard_order) chunks
        # and the split is never finer than the commit node, so at production
        # geometry every manifest carries >= one leaf's 256 refs per array --
        # and at the default split 6, 4^7 = 16,384, far above the 1,000 gate.
        assert 4 ** icechunk_refs.split_exponent(13, 6) > icechunk_refs.LOCATION_DICT_MIN_CHUNKS


class TestOptions:
    def _cfg(self, cfg, **block):
        cfg.output["store_layout"] = "hive"
        cfg.output["icechunk"] = block
        return cfg

    def test_defaults_commit_at_the_finest_dispatch_node(self, cfg):
        # shard 9 / width 3 -> 6 (shard_order - tuple_width); shard 4 -> 3.
        assert icechunk_refs.finest_dispatch_order(9) == 6
        assert icechunk_refs.finest_dispatch_order(4) == 3
        assert icechunk_refs.resolve_options(self._cfg(cfg), 9) == {
            "commit": "ladder",
            "commit_order": 6,
            "split_order": 6,
        }
        assert icechunk_refs.resolve_options(self._cfg(cfg), 4)["commit_order"] == 3

    def test_global_scale_setting(self, cfg):
        opts = icechunk_refs.resolve_options(self._cfg(cfg, split_order=4, commit_order=3), 9)
        assert opts == {"commit": "ladder", "commit_order": 3, "split_order": 4}

    def test_split_finer_than_commit_is_refused(self, cfg):
        with pytest.raises(ValueError, match="whole manifests"):
            icechunk_refs.resolve_options(self._cfg(cfg, split_order=2, commit_order=3), 9)
        # The same invariant at config validation, when both are given.
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="finer than commit_order"):
            validate_config(self._cfg(cfg, split_order=2, commit_order=3))

    def test_orders_above_the_shard_order_are_refused(self, cfg):
        with pytest.raises(ValueError, match="shard_order"):
            icechunk_refs.resolve_options(self._cfg(cfg, commit_order=5), 4)
        with pytest.raises(ValueError, match="shard_order"):
            icechunk_refs.resolve_options(self._cfg(cfg, split_order=5), 4)

    def test_ladder_commit_at_the_shard_order_is_refused(self, cfg):
        # No stage tuple's [dispatch, child_order) range contains the shard
        # order, so a ladder commit there gathers no leaf sidecar at all.
        with pytest.raises(ValueError, match="no stage tuple covers the shard order"):
            icechunk_refs.resolve_options(self._cfg(cfg, commit_order=4), 4)
        with pytest.raises(ValueError, match="no stage tuple covers the shard order"):
            icechunk_refs.resolve_options(self._cfg(cfg, commit_order=4, split_order=4), 4)
        # ``commit: "leaf"`` commits per leaf and is unaffected.
        assert icechunk_refs.resolve_options(self._cfg(cfg, commit="leaf", commit_order=4), 4) == {
            "commit": "leaf",
            "commit_order": 4,
            "split_order": 4,
        }

    def test_block_shape_is_validated(self, cfg):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="unknown key"):
            validate_config(self._cfg(cfg, nope=1))
        with pytest.raises(ValueError, match="'ladder' or 'leaf'"):
            validate_config(self._cfg(cfg, commit="node"))
        with pytest.raises(ValueError, match="non-negative integer"):
            validate_config(self._cfg(cfg, commit_order=-1))
        validate_config(self._cfg(cfg, commit="leaf", commit_order=3, split_order=3))


class TestInit:
    def test_creates_the_hierarchy_and_block(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        out = icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        assert out["created"] is True
        assert out["path"] == f"{root}/icechunk"
        assert out["ladder"][-1] == 4  # the base group is the coarsest-to-finest list's last
        group, repo = _open(root)
        block = group.attrs[icechunk_refs.ICECHUNK_ATTR]
        assert block["spec"] == "zagg-icechunk/1"
        assert block["shard_order"] == 4 and block["chunk_order"] == 5 and block["cell_order"] == 6
        assert block["url_prefix"] == icechunk_refs.container_prefix(root)
        # A direct init with no ladder knobs: the ladder defaults (commit at
        # the finest dispatch node, 3 here; split at it).
        assert (block["commit"], block["commit_order"], block["split_order"]) == ("ladder", 3, 3)
        # One repo, a group per order: the base (4) plus the declared
        # overview orders, each with its own split block.
        assert sorted(block["levels"], key=int) == [str(o) for o in out["ladder"]]
        assert block["levels"]["4"] == {
            "chunk_order": 5,
            "cell_order": 6,
            "split": {"chunks": 16, "order": 3},
        }
        assert out["levels"] == block["levels"] and out["options"]["commit_order"] == 3
        # The manifest's §4.9 multiscales mirror rides the root attrs.
        assert group.attrs["multiscales"] == hive.build_manifest(grid)["multiscales"]
        assert {str(o) for o in out["ladder"]} == {k for k, _ in group.members()}
        assert block["url_prefix"].startswith("file://") and block["url_prefix"].endswith("/")
        # The resolution group mirrors the leaf's attrs (dggs + conventions,
        # latitude token included), never a commit stamp.
        leaf_attrs = grid.shard_spec().attributes
        assert dict(group["4"].attrs) == leaf_attrs
        assert group["4"].attrs["dggs"]["latitude"] == "authalic-wgs84"
        assert hive.COMMIT_ATTR not in group.attrs
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages[0] == f"init {RUN_ID}"

    def test_arrays_are_rerooted_on_the_order(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        group, _repo = _open(root)
        leaf = grid.shard_spec().members
        for name, spec in leaf.items():
            arr = group["4"][name]
            leaf_shape = tuple(spec.shape)
            assert arr.shape == (grid.n_shards * leaf_shape[0], *leaf_shape[1:])
            assert arr.shape[0] == 12 * 4**grid.child_order or name.endswith("_chunk")
            assert arr.chunks == tuple(grid.chunk_shape)  # the INNER chunk
            assert arr.shards is None  # the sharding wrapper is gone
            assert arr.metadata.dimension_names == tuple(spec.dimension_names)
            assert arr.metadata.data_type.to_json(zarr_format=3) == spec.data_type
        # Ragged: vlen-bytes + zstd inner chain, the §1.2 block intact.
        rag = group["4"]["h"]
        assert [c.__class__.__name__ for c in rag.metadata.codecs] == [
            "VLenBytesCodec",
            "ZstdCodec",
        ]
        assert rag.attrs["ragged"]["spec"] == "zagg-ragged/1"
        assert list(rag.attrs["ragged"]["element"]["shape"]) == [-1, 1]

    def test_persists_split_and_container(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        out = icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        repo = icechunk_refs.open_repo(root, store_kwargs={})  # no config passed
        containers = repo.config.virtual_chunk_containers
        assert list(containers) == [icechunk_refs.container_prefix(root)]
        splitting = repo.config.manifest.splitting
        assert splitting is not None
        # One split per order group (path-matched, first wins) plus the
        # catch-all: the base group's run is its ``levels`` block's.
        sizes = splitting.split_sizes
        assert len(sizes) == len(out["levels"]) + 1
        (condition, dims) = sizes[0]
        assert isinstance(condition, icechunk.ManifestSplitCondition.PathMatches)
        ((axis, size),) = dims
        assert isinstance(axis, icechunk.ManifestSplitDimCondition.Axis)
        assert axis._0 == 0 and size == out["levels"]["4"]["split"]["chunks"]
        assert isinstance(sizes[-1][0], icechunk.ManifestSplitCondition.AnyArray)

    def test_rerun_reopens_without_a_commit(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        first = icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        second = icechunk_refs.init_repo(root, grid, cfg, run_id="run-2", store_kwargs={})
        assert second["created"] is False
        assert second["snapshot"] == first["snapshot"]
        assert second["levels"] == first["levels"]
        _group, repo = _open(root)
        assert [s.message for s in repo.ancestry(branch="main")][0] == f"init {RUN_ID}"

    def test_rerun_with_another_geometry_raises(self, cfg, tmp_path):
        # A store whose leaves were cleared but whose root survived reopens the
        # stale repo under a new-geometry manifest; the idempotent branch is a
        # match check, so the second init refuses instead of silently reusing
        # an array model this run's leaves do not fit.
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, _grid(cfg), cfg, run_id=RUN_ID, store_kwargs={})
        other = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        with pytest.raises(ValueError, match="was built for"):
            icechunk_refs.init_repo(root, other, cfg, run_id="run-2", store_kwargs={})

    def _init(self, cfg, root, **block):
        grid = _grid(cfg)
        cfg.output["icechunk"] = block
        return icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})

    def test_split_ratchet_equal_is_a_no_op(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        first = self._init(cfg, root, split_order=3)
        again = self._init(cfg, root, split_order=3)
        assert again["created"] is False and again["split_ratchet"] is None
        assert again["snapshot"] == first["snapshot"]  # nothing committed
        assert again["options"]["split_order"] == 3

    def test_split_ratchet_finer_config_adopts_the_store(self, cfg, tmp_path, caplog):
        import logging

        root = str(tmp_path / "store")
        first = self._init(cfg, root, split_order=2, commit_order=2)
        assert first["levels"]["4"]["split"] == {"chunks": 4**3, "order": 2}
        with caplog.at_level(logging.WARNING, logger="zagg.icechunk_refs"):
            again = self._init(cfg, root, split_order=3, commit_order=2)  # finer than the store
        assert "finer than the store's recorded 2" in caplog.text
        assert again["options"]["split_order"] == 2 and again["split_ratchet"] is None
        assert again["levels"] == first["levels"] and again["snapshot"] == first["snapshot"]
        # Reopened repo: the persisted split is untouched (still the store's).
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        (cond, dims), *_ = repo.config.manifest.splitting.split_sizes
        assert dims[0][1] == 4**3
        assert icechunk_refs.read_block(root, store_kwargs={})["split_order"] == 2

    def test_split_ratchet_finer_config_with_a_too_fine_commit_order_refuses(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        self._init(cfg, root, split_order=2, commit_order=2)
        with pytest.raises(ValueError, match="exceeds the store's recorded split_order 2"):
            self._init(cfg, root, split_order=3, commit_order=3)

    def test_split_ratchet_coarser_config_recuts_before_any_commit(self, cfg, tmp_path, caplog):
        import logging

        root = str(tmp_path / "store")
        first = self._init(cfg, root, split_order=3)
        with caplog.at_level(logging.WARNING, logger="zagg.icechunk_refs"):
            again = self._init(cfg, root, split_order=2, commit_order=2)
        assert "ratchets 3 -> 2" in caplog.text
        assert again["split_ratchet"] == {"from": 3, "to": 2}
        assert again["snapshot"] != first["snapshot"]  # the block rewrite is a commit
        assert again["levels"]["4"]["split"] == {"chunks": 4**3, "order": 2}
        block = icechunk_refs.read_block(root, store_kwargs={})
        assert block["split_order"] == 2 and block["levels"] == again["levels"]
        _group, repo = _open(root)
        assert [s.message for s in repo.ancestry(branch="main")][0].startswith("split ratchet 3->2")
        # The saved splitting config already cuts at the new order.
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        (cond, dims), *_ = repo.config.manifest.splitting.split_sizes
        assert dims[0][1] == 4**3
        # Never back: a finer config afterwards adopts 2 again.
        assert self._init(cfg, root, split_order=3, commit_order=2)["options"]["split_order"] == 2
        # And the run record flags it.
        import pandas as pd

        from zagg.telemetry import write_run_parquet

        rows = TestRunRecord._rows()
        df = pd.read_parquet(write_run_parquet(root, rows, run_id="r", icechunk_init=again))
        assert df["icechunk_split_ratchet"].tolist() == ["3->2"]

    def test_commit_order_is_per_run_not_compared(self, cfg, tmp_path):
        root = str(tmp_path / "store")
        first = self._init(cfg, root, commit_order=3, split_order=3)
        again = self._init(cfg, root, commit_order=0, split_order=3)
        assert again["created"] is False and again["split_ratchet"] is None
        assert again["snapshot"] != first["snapshot"]  # the run's value is recorded for its nodes
        assert icechunk_refs.read_block(root, store_kwargs={})["commit_order"] == 0

    def test_read_block(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        assert icechunk_refs.read_block(root, store_kwargs={}) is None
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        assert icechunk_refs.read_block(root, store_kwargs={})["shard_order"] == 4

    def test_repo_name_is_reserved(self):
        with pytest.raises(ValueError):
            hive.validate_product_name("icechunk")


class TestLeafRefs:
    def test_plan_reads_the_shard_index(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        shard = _shards(grid, 1)[0]
        meta = _write_leaf(
            monkeypatch,
            grid,
            root,
            shard,
            ragged={"h": ([np.array([1.0, 2.0], dtype=np.float32)], [0])},
            skip_chunks=(2,),
        )
        assert meta.get("error") is None
        plan = {
            e["path"]: e for e in icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={})
        }
        (rank,) = grid.block_index(shard)
        count = plan["count"]
        assert count["sharded"] is True
        assert count["chunk_grid"] == (4,)
        assert count["arr_offset"] == (rank * 4,)
        # Chunk ordinal 2 was never written: the sentinel emits no ref.
        assert [bool(loc) for loc in count["locations"]] == [True, True, False, True]
        assert count["refs"] == 3
        # file:// container: the checksum is the object's ceiled last_modified.
        assert count["checksum"].microsecond == 0
        leaf_rel = hive.shard_leaf_path("", shard).lstrip("/")
        assert (
            count["locations"][0] == f"{icechunk_refs.container_prefix(root)}{leaf_rel}/6/count/c/0"
        )
        # Offsets/lengths are the shard index's own u64 words.
        obj = (tmp_path / "store" / leaf_rel / "6" / "count" / "c" / "0").read_bytes()
        index = np.frombuffer(obj[-(16 * 4 + 4) : -4], dtype="<u8").reshape(4, 2)
        np.testing.assert_array_equal(count["offsets"][[0, 1, 3]], index[[0, 1, 3], 0])
        np.testing.assert_array_equal(count["lengths"][[0, 1, 3]], index[[0, 1, 3], 1])
        # The ragged array is one shard object too, with refs only where the
        # payload landed (chunk 0).
        rag = plan["h"]
        assert rag["sharded"] is True and rag["refs"] == 1
        assert [bool(loc) for loc in rag["locations"]] == [True, False, False, False]

    @pytest.mark.parametrize(
        "mutate, match",
        [
            ({"index_location": "start"}, "start-located"),
            (
                {"index_codecs": [{"name": "bytes", "configuration": {"endian": "little"}}]},
                "the ref plan reads",
            ),
        ],
    )
    def test_plan_refuses_an_unreadable_shard_index(
        self, monkeypatch, cfg, tmp_path, mutate, match
    ):
        # The suffix read assumes an end-located, uncompressed index; both
        # facts are in the array's own codec config, so the plan checks them
        # instead of reading chunk payload as (offset, length) pairs (§11.3).
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        shard = _shards(grid, 1)[0]
        _write_leaf(monkeypatch, grid, root, shard)
        real = grid.shard_spec

        def spec():
            members = {}
            for name, member in real().members.items():
                data = member.model_dump()
                codecs = list(data["codecs"])
                codecs[0] = dict(codecs[0])
                codecs[0]["configuration"] = {**codecs[0]["configuration"], **mutate}
                data["codecs"] = codecs
                members[name] = type(member)(**data)
            return type(real())(members=members, attributes=real().attributes)

        monkeypatch.setattr(grid, "shard_spec", spec)
        with pytest.raises(ValueError, match=match):
            icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={})

    def test_record_leaf_commits_and_reads_back(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shards = _shards(grid, 2)
        for i, shard in enumerate(shards):
            _write_leaf(
                monkeypatch,
                grid,
                root,
                shard,
                fill=10.0 * (i + 1),
                ragged={"h": ([np.array([1.0, 2.0], dtype=np.float32)], [0])},
                skip_chunks=(2,),
            )
            out = icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
            assert out["refs"] > 0 and out["rebases"] == 0
            assert out["checksum"] == "last_modified"
            assert out["path"] == f"{root}/icechunk"
        group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages[:2] == [
            f"leaf {morton_decimal(shards[1])}",
            f"leaf {morton_decimal(shards[0])}",
        ]
        for shard in shards:
            (rank,) = grid.block_index(shard)
            leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
            span = slice(rank * 16, (rank + 1) * 16)
            for name in ("count", "h_mean", "morton"):
                np.testing.assert_array_equal(group["4"][name][span], leaf[name][:])
            # Ragged bytes match cell for cell (chunk 0 populated, the rest fill).
            got = group["4"]["h"][span]
            want = leaf["h"][:]
            assert [bytes(a) for a in got] == [bytes(b) for b in want]
            assert bytes(got[0]) == np.array([1.0, 2.0], dtype="<f4").tobytes()
        # Cells of a leaf nobody wrote read as fill.
        other = _shards(grid, 3)[2]
        (rank,) = grid.block_index(other)
        assert np.isnan(group["4"]["h_mean"][rank * 16 : (rank + 1) * 16]).all()

    def test_local_refs_catch_a_wholesale_rewrite(self, monkeypatch, cfg, tmp_path):
        # The file:// container DOES validate a checksum: a LastUpdatedAt
        # datetime, compared at whole-second granularity, so the plan records
        # ceil(last_modified) (§11.3). A wholesale leaf replacement a second
        # later then fails the read loudly instead of decoding the new bytes
        # at the discarded attempt's offsets.
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        _write_leaf(monkeypatch, grid, root, shard, fill=1.0)
        out = icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
        assert out["checksum"] == "last_modified"
        (rank,) = grid.block_index(shard)
        span = slice(rank * 16, (rank + 1) * 16)
        group, _repo = _open(root)
        np.testing.assert_array_equal(group["4"]["count"][span][:4], np.full(4, 1))
        time.sleep(1.1)  # icechunk compares at whole-second granularity
        _write_leaf(monkeypatch, grid, root, shard, fill=99.0)
        group, _repo = _open(root)
        with pytest.raises(icechunk.StorageError, match="checksum"):
            group["4"]["count"][span]

    def test_windowed_and_empty_units_are_skipped(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        assert icechunk_refs.record_leaf(root, grid, shard, store_kwargs={}, window="2019") == {
            "skipped": "windowed"
        }
        assert icechunk_refs.record_leaf(root, grid, shard, store_kwargs={}) == {"skipped": "empty"}

    def test_record_leaf_requires_an_initialized_matching_repo(self, monkeypatch, cfg, tmp_path):
        # The repo is vetted before the plan's ~20 object-store requests: an
        # uninitialized repo and one built for another geometry both raise, so
        # refs never land at indices that mean something else (§11.3).
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        shard = _shards(grid, 1)[0]
        _write_leaf(monkeypatch, grid, root, shard)
        calls = []
        monkeypatch.setattr(
            icechunk_refs,
            "leaf_ref_plan",
            lambda *a, **kw: calls.append(1) or [],
        )
        with pytest.raises(ValueError, match="is not initialized"):
            icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
        monkeypatch.undo()
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        other = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        with pytest.raises(ValueError, match="was built for"):
            icechunk_refs.record_leaf(root, other, shard, store_kwargs={})
        assert calls == []  # neither refusal paid for a plan

    def test_unsharded_leaf_refs_one_object_per_chunk(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg, sharded=False)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        _write_leaf(monkeypatch, grid, root, shard, skip_chunks=(1,))
        plan = {
            e["path"]: e for e in icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={})
        }
        count = plan["count"]
        assert count["sharded"] is False and count["refs"] == 3
        (rank,) = grid.block_index(shard)
        keys = sorted(k for k, _l, _n, _e in count["chunks"])
        assert keys == sorted(f"count/c/{rank * 4 + j}" for j in (0, 2, 3))
        out = icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
        # The record names the FORM, never a per-chunk value.
        assert out["refs"] > 0 and out["checksum"] == "last_modified"
        group, _repo = _open(root)
        leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
        np.testing.assert_array_equal(
            group["4"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
        )

    def test_concurrent_leaf_commits_all_land(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shards = _shards(grid, 6)
        for i, shard in enumerate(shards):
            _write_leaf(monkeypatch, grid, root, shard, fill=float(i + 1))
        outs: dict = {}
        errors: list = []

        def work(shard):
            try:
                outs[shard] = icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
            except Exception as e:  # pragma: no cover - the assertion below reports it
                errors.append(e)

        threads = [threading.Thread(target=work, args=(s,)) for s in shards]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert len(outs) == len(shards)
        group, repo = _open(root)
        assert len([s for s in repo.ancestry(branch="main")]) == len(shards) + 2  # + init + birth
        for i, shard in enumerate(shards):
            (rank,) = grid.block_index(shard)
            assert (group["4"]["count"][rank * 16 : (rank + 1) * 16] >= i + 1).all()

    def test_rebase_is_counted(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        repo = icechunk_refs.open_repo(root, store_kwargs={})
        stale = repo.writable_session("main")
        fresh = repo.writable_session("main")
        fresh.store.set_virtual_ref(
            "4/count/c/0", icechunk_refs.container_prefix(root) + "x", offset=0, length=4
        )
        icechunk_refs._commit(fresh, "a", local=True)
        stale.store.set_virtual_ref(
            "4/count/c/1", icechunk_refs.container_prefix(root) + "y", offset=0, length=4
        )
        snapshot, rebases = icechunk_refs._commit(stale, "b", local=True)
        assert rebases == 1
        assert repo.lookup_branch("main") == snapshot


def test_local_commit_lock_is_per_repo_path():
    # icechunk's local-filesystem storage is unsafe for concurrent commits,
    # but that is a PER-REPO fact: two local runs into different stores in one
    # interpreter must not serialize against each other.
    a = icechunk_refs._local_commit_lock("/tmp/one/icechunk/4")
    b = icechunk_refs._local_commit_lock("/tmp/two/icechunk/4")
    assert a is not b
    assert icechunk_refs._local_commit_lock("/tmp/one/icechunk/4") is a
    # Different orders of one store are different repos, hence different locks.
    assert icechunk_refs._local_commit_lock("/tmp/one/icechunk/5") is not a


# ── the invoke seams (phase 3) ───────────────────────────────────────────────


class _Payload:
    def __init__(self, raw: bytes):
        self._raw = raw

    def read(self):
        return self._raw


def _envelope(body: dict, status: int = 200, function_error: str | None = None) -> dict:
    raw = json.dumps({"statusCode": status, "body": json.dumps(body)}).encode()
    out: dict = {"Payload": _Payload(raw)}
    if function_error:
        out["FunctionError"] = function_error
    return out


class _Client:
    def __init__(self, response=None, raise_exc=None):
        self.events: list = []
        self._response = response
        self._raise = raise_exc

    def invoke(self, **kwargs):
        self.events.append((kwargs["InvocationType"], json.loads(kwargs["Payload"])))
        if self._raise is not None:
            raise self._raise
        return self._response


class TestLambdaInitInvoke:
    def _call(self, client):
        from zagg import runner

        return runner._invoke_lambda_icechunk_init(
            client,
            "fn",
            "s3://b/p",
            config_dict={"x": 1},
            parent_order=4,
            run_id="r1",
            output_creds_event={"accessKeyId": "a", "secretAccessKey": "s"},
        )

    def test_event_shape_and_record(self):
        body = {
            "ok": True,
            "mode": "icechunk_init",
            "path": "s3://b/p/icechunk",
            "snapshot": "SNAP",
            "created": True,
            "options": {"commit": "ladder", "commit_order": 6, "split_order": 6},
            "levels": {
                "9": {"chunk_order": 13, "cell_order": 19, "split": {"chunks": 16384, "order": 6}}
            },
            "ladder": [9],
        }
        client = _Client(_envelope(body))
        out = self._call(client)
        # The blocking round-trip times itself (it may carry a cold start) and
        # the measurement rides the record, not a sibling summary key --
        # ``setup_s`` keeps its pre-fan-out bracket meaning.
        assert out.pop("invoke_s") >= 0.0
        assert out == {
            k: body[k] for k in ("path", "snapshot", "created", "options", "levels", "ladder")
        }
        ((kind, event),) = client.events
        assert kind == "RequestResponse"  # blocks the fan-out
        assert event == {
            "mode": "icechunk_init",
            "store_path": "s3://b/p",
            "parent_order": 4,
            "run_id": "r1",
            "config": {"x": 1},
            "output_credentials": {"accessKeyId": "a", "secretAccessKey": "s"},
        }

    @pytest.mark.parametrize(
        "response, raise_exc, match",
        [
            (_envelope({"error": "boom", "mode": "icechunk_init"}, status=500), None, "boom"),
            # A stale deployment: the unknown mode falls to the process
            # handler's 400.
            (_envelope({"error": "Missing shard_key"}, status=400), None, "statusCode 400"),
            (_envelope({}, function_error="Unhandled"), None, "RuntimeError"),
            (None, ConnectionError("throttled"), "throttled"),
            # A 200 that is not the handler's success envelope: an older or
            # mis-routed worker. {} is falsy, so an empty record would read as
            # "the knob was off" all the way into the run parquet.
            (_envelope({"zagg_version": "stub"}), None, "unexpected icechunk_init body"),
            (_envelope({"ok": True, "mode": "icechunk_init"}), None, "unexpected"),
        ],
    )
    def test_failures_are_fail_open(self, response, raise_exc, match, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            out = self._call(_Client(response, raise_exc))
        assert set(out) == {"error"} and match in out["error"]
        assert "fail-open, issue #580" in caplog.text


class TestLocalInit:
    def test_record_none_and_error(self, cfg, tmp_path, caplog):
        import logging

        from zagg import runner

        grid = _grid(cfg)
        root = str(tmp_path / "store")
        cfg.output["store_layout"] = "hive"
        out = runner._init_icechunk_local(cfg, grid, root, RUN_ID, {})
        assert out["created"] is True and out["path"] == f"{root}/icechunk"
        cfg.output["icechunk"] = False
        assert runner._init_icechunk_local(cfg, grid, root, RUN_ID, {}) is None
        cfg.output["icechunk"] = True
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            # An unwritable repo path fails open with the error recorded.
            out = runner._init_icechunk_local(cfg, grid, "/dev/null/nope", RUN_ID, {})
        assert set(out) == {"error"}
        assert "fail-open, issue #580" in caplog.text


@pytest.fixture(scope="module")
def handler_mod():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_580", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestHandlerMode:
    def _event(self, root, cfg):
        from dataclasses import asdict

        cfg.output["store_layout"] = "hive"
        cfg.output["grid"] = {
            **cfg.output.get("grid", {}),
            "type": "healpix",
            "parent_order": 4,
            "child_order": 6,
            "chunk_inner": 5,
        }
        return {
            "mode": "icechunk_init",
            "store_path": root,
            "parent_order": 4,
            "run_id": RUN_ID,
            "config": asdict(cfg),
        }

    def test_init_then_idempotent_reopen(self, handler_mod, cfg, tmp_path):
        root = str(tmp_path / "store")
        resp = handler_mod.lambda_handler(self._event(root, cfg), None)
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert body["mode"] == "icechunk_init" and body["created"] is True
        assert body["path"] == f"{root}/icechunk"
        assert icechunk_refs.read_block(root, store_kwargs={})["shard_order"] == 4
        again = json.loads(handler_mod.lambda_handler(self._event(root, cfg), None)["body"])
        assert again["created"] is False and again["snapshot"] == body["snapshot"]

    def test_error_returns_500_never_raises(self, handler_mod, cfg, tmp_path):
        event = self._event(str(tmp_path / "store"), cfg)
        del event["config"]
        resp = handler_mod.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        assert json.loads(resp["body"])["mode"] == "icechunk_init"

    def test_run_id_is_required_not_defaulted(self, handler_mod, cfg, tmp_path):
        # §11.4 fixes the commit grammar as ``init {run_id}``; an
        # unattributable ``init unknown`` cannot be rewritten out of the
        # repo's permanent ancestry, so a caller that omits it 500s through
        # the existing except. Both dispatchers always send one.
        root = str(tmp_path / "store")
        event = self._event(root, cfg)
        del event["run_id"]
        resp = handler_mod.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        assert "run_id" in json.loads(resp["body"])["error"]
        assert icechunk_refs.read_block(root, store_kwargs={}) is None


class TestWorkerWiring:
    """Phase 4: ``process_and_write_hive`` records refs after the stamp, fail-open."""

    def test_leaf_write_records_refs_and_reads_back(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        meta = _write_leaf(monkeypatch, grid, root, shard, skip_chunks=(2,), refs=True)
        ice = meta["icechunk"]
        assert ice["refs"] > 0 and ice["rebases"] == 0 and ice["commit_s"] >= 0.0
        assert ice["path"] == f"{root}/icechunk"
        assert meta["phase_timings"]["icechunk"] >= 0.0
        # The commit is the leaf's, on top of init, and the repo reads the leaf.
        group, repo = _open(root)
        assert [s.message for s in repo.ancestry(branch="main")][
            0
        ] == f"leaf {morton_decimal(shard)}"
        assert repo.lookup_branch("main") == ice["snapshot"]
        (rank,) = grid.block_index(shard)
        leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
        np.testing.assert_array_equal(
            group["4"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
        )
        # The record rides the D20 record and flattens to parquet scalars.
        from zagg.telemetry import build_record, flatten_record

        record = build_record(shard_key=shard, metadata=meta, granule_ids=["g"])
        assert record["icechunk"] == ice
        row = flatten_record(record)
        assert row["icechunk_snapshot"] == ice["snapshot"]
        assert row["icechunk_rebases"] == 0 and row["icechunk_refs"] == ice["refs"]
        assert row["icechunk_error"] is None and row["icechunk_skipped"] is None
        # The checksum FORM is a column of its own: it varies by store scheme,
        # so no other column derives it (file:// here, ETag on S3).
        assert row["icechunk_checksum"] == "last_modified"

    def test_missing_repo_fails_open_after_the_stamp(self, monkeypatch, cfg, tmp_path, caplog):
        import logging

        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")  # no init: the repo does not exist
        shard = _shards(grid, 1)[0]
        with caplog.at_level(logging.WARNING, logger="zagg.hive"):
            meta = _write_leaf(monkeypatch, grid, root, shard, refs=True)
        assert meta.get("error") is None
        assert set(meta["icechunk"]) == {"error"}
        assert "fail-open, issue #580" in caplog.text
        # The leaf itself is committed and hashed regardless.
        stamp = hive.read_commit(hive.shard_leaf_path(root, shard))
        assert stamp["complete"] is True and "content_hashes" in stamp
        from zagg.telemetry import build_record, flatten_record

        row = flatten_record(build_record(shard_key=shard, metadata=meta, granule_ids=["g"]))
        assert row["icechunk_error"] and row["icechunk_snapshot"] is None

    def test_refs_are_recorded_last_after_the_stamp_and_the_column(
        self, monkeypatch, cfg, tmp_path
    ):
        # The order the whole design rests on, which nothing else asserted:
        # at ``record_leaf`` time the stamp has landed (so the refs point at
        # objects it certified) AND the issue #383 column fold is done — the
        # last post-stamp phase that can still fail the unit, whose retry
        # rewrites the leaf wholesale at new offsets. Both directions matter,
        # so the spy checks state at call time, not after the call returns.
        import zagg.column as column_mod
        from zagg.telemetry import read_granule_ids

        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, cfg, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        order: list = []
        seen: dict = {}
        real_column = column_mod.write_leaf_column
        real_record = icechunk_refs.record_leaf

        def column_spy(*args, **kwargs):
            order.append("column")
            return real_column(*args, **kwargs)

        def record_spy(store_root, g, key, **kwargs):
            order.append("icechunk")
            leaf = hive.shard_leaf_path(store_root, int(key))
            seen["stamp"] = hive.read_commit(leaf)
            seen["ids"] = read_granule_ids(leaf)
            return real_record(store_root, g, key, **kwargs)

        monkeypatch.setattr(column_mod, "write_leaf_column", column_spy)
        monkeypatch.setattr(icechunk_refs, "record_leaf", record_spy)
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=True)
        assert order == ["column", "icechunk"]
        assert seen["stamp"]["complete"] is True and "content_hashes" in seen["stamp"]
        assert seen["ids"]["granule_ids"] == ["g.h5"]
        assert meta["icechunk"]["refs"] > 0

    def test_the_production_default_writes_a_ref_sidecar(self, monkeypatch, cfg, tmp_path):
        # Phase 6: with no ``icechunk`` key at all, a hive worker commits
        # nothing itself — it writes the leaf's ref sidecar for the ladder.
        from zagg.icechunk_ladder import leaf_refs_key, read_leaf_refs

        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        shard = _shards(grid, 1)[0]
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=None, skip_chunks=(2,))
        ice = meta["icechunk"]
        assert ice["refs"] > 0 and ice["arrays"] > 0 and ice["bytes"] > 0
        assert "snapshot" not in ice and ice["checksum"] == "last_modified"
        leaf = hive.shard_leaf_path(root, shard)
        assert ice["sidecar"] == leaf_refs_key(leaf.rpartition("/")[2]) == "icechunk_refs.json"
        assert not (tmp_path / "store" / "icechunk").exists()  # no repo, no session
        units, meta_ = read_leaf_refs(root, shard, spec=None, store_kwargs={})
        assert [u["order"] for u in units] == [4]
        assert meta_["geometry"] == {"shard_order": 4, "chunk_order": 5, "cell_order": 6}
        # The sidecar round-trips the plan the worker computed.
        plan = icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={})
        by_path = {e["path"]: e for e in units[0]["entries"]}
        for entry in plan:
            got = by_path[entry["path"]]
            assert got["locations"] == entry["locations"]
            np.testing.assert_array_equal(got["offsets"], entry["offsets"])
            np.testing.assert_array_equal(got["lengths"], entry["lengths"])
            assert got["checksum"] == entry["checksum"] and got["refs"] == entry["refs"]
        from zagg.telemetry import build_record, flatten_record

        row = flatten_record(build_record(shard_key=shard, metadata=meta, granule_ids=["g"]))
        assert row["icechunk_sidecar"] == ice["sidecar"] and row["icechunk_bytes"] == ice["bytes"]
        assert row["icechunk_snapshot"] is None

    def test_knob_off_records_nothing(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        shard = _shards(grid, 1)[0]
        meta = _write_leaf(monkeypatch, grid, root, shard, refs=False)
        assert "icechunk" not in meta and "icechunk" not in meta["phase_timings"]
        assert not (tmp_path / "store" / "icechunk").exists()


class TestRunRecord:
    """Phase 5: the run parquet's two run-level icechunk columns."""

    @staticmethod
    def _rows():
        from zagg.telemetry import build_record, flatten_record

        return [
            flatten_record(build_record(shard_key=1, metadata={"total_obs": 1}, granule_ids=["g"]))
        ]

    def test_columns_broadcast_the_init_record(self, tmp_path):
        import pandas as pd

        from zagg.telemetry import write_run_parquet

        init = {"path": f"{tmp_path}/icechunk/4", "snapshot": "SNAP", "created": True}
        path = write_run_parquet(str(tmp_path), self._rows(), run_id="abc", icechunk_init=init)
        df = pd.read_parquet(path)
        assert df["icechunk_init_repo"].tolist() == [f"{tmp_path}/icechunk/4"]
        assert df["icechunk_init_snapshot"].tolist() == ["SNAP"]
        assert df["icechunk_init_error"].isna().all()
        # A fail-open init lands in its OWN column, never in the snapshot's.
        path = write_run_parquet(
            str(tmp_path), self._rows(), run_id="run-b", icechunk_init={"error": "RuntimeError: x"}
        )
        df = pd.read_parquet(path)
        assert df["icechunk_init_error"].tolist() == ["RuntimeError: x"]
        assert df["icechunk_init_repo"].isna().all()
        assert df["icechunk_init_snapshot"].isna().all()
        # Off-hive / opted out: all null, columns still present.
        df = pd.read_parquet(write_run_parquet(str(tmp_path), self._rows(), run_id="run-c"))
        for col in ("icechunk_init_repo", "icechunk_init_snapshot", "icechunk_init_error"):
            assert df[col].isna().all()

    def test_dispatch_event_carries_the_record(self, monkeypatch):
        from zagg import runner

        client = _Client(_envelope({"ok": True}))
        runner._dispatch_run_stats(
            client,
            "fn",
            "s3://b/p",
            self._rows(),
            run_id="abc",
            icechunk_init={"path": "s3://b/p/icechunk/4", "snapshot": "SNAP"},
        )
        ((_kind, event),) = client.events
        assert event["mode"] == "stats"
        assert event["icechunk_init"] == {"path": "s3://b/p/icechunk/4", "snapshot": "SNAP"}


def test_flatten_record_coerces_a_non_dict_block():
    # ``metadata`` is not always locally built — the dispatcher's stale-worker
    # path passes the JSON body a remote worker returned — so a version-skewed
    # body carrying e.g. ``"icechunk": "ok"`` must flatten to nulls rather than
    # raising AttributeError and taking the whole run parquet with it.
    from zagg.telemetry import build_record, flatten_record

    row = flatten_record(build_record(shard_key=1, metadata={"icechunk": "ok"}, granule_ids=["g"]))
    assert row["icechunk_snapshot"] is None and row["icechunk_refs"] is None
    assert row["icechunk_error"] is None and row["icechunk_skipped"] is None
    assert row["icechunk_checksum"] is None


class TestLocalRunEndToEnd:
    """Phase 5: a local-backend run — init, leaves, refs, run record — end to end."""

    def test_two_leaves_read_back_as_one_order(self, monkeypatch, cfg, tmp_path):
        import pandas as pd

        import zagg.processing as processing
        from zagg import runner

        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        cfg.output["grid"] = {
            **cfg.output.get("grid", {}),
            "type": "healpix",
            "parent_order": 4,
            "child_order": 6,
            "chunk_inner": 5,
        }
        cfg.output["sweep"] = False  # the rollup sweep is not under test
        shards = _shards(grid, 2)
        monkeypatch.setattr(
            runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a", "secretAccessKey": "s"}
        )

        def fake(g, shard_key, urls, **kwargs):
            for i, (block, _children) in enumerate(g.iter_chunks(int(shard_key))):
                if i == 2:
                    continue  # one absent inner chunk per leaf: no ref, reads fill
                carrier = _carrier(g, shard_key, float(shards.index(int(shard_key)) + 1) * 10 + i)
                local = g.shard_local_region(block, int(shard_key))
                rag = {"h": ([np.array([1.0, 2.0], dtype=np.float32)], [0])} if i == 0 else {}
                kwargs["chunk_results"].append((block, carrier.iloc[local[0]], rag))
            return pd.DataFrame(), {
                "shard_key": int(shard_key),
                "cells_with_data": 12,
                "total_obs": 12,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
                "phase_timings": {"read": 0.0, "index": 0.0, "aggregate": 0.0},
            }

        monkeypatch.setattr(processing, "process_shard", fake)
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            # The full signature ``HealpixGrid.spatial_signature()`` returns,
            # so the production guard runs for real (tests/test_hive.py
            # ``TestRunnerWiring._catalog`` spells it the same way).
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 4,
                "child_order": 6,
                "layout": "fullsphere",
            },
            "shard_keys": shards,
            "granules": [[{"id": f"g{i}", "s3": f"s3://b/g{i}.h5"}] for i in range(len(shards))],
        }
        root = str(tmp_path / "store")
        summary = runner._run_local(
            cfg,
            catalog,
            root,
            6,
            max_cells=None,
            morton_cell=None,
            max_workers=1,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert summary["cells_with_data"] == 2 and summary["cells_error"] == 0
        init = summary["icechunk"]
        assert init["created"] is True and init["path"] == f"{root}/icechunk"
        # Every leaf committed its refs on top of the init commit.
        for meta in summary["results"]:
            assert meta["icechunk"]["refs"] > 0 and "error" not in meta["icechunk"]
        group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert (
            len(messages) == len(shards) + 2
            and messages[-2] == f"init {summary['results'][0]['stats']['run_id']}"
        )
        # The order reads as one zarr: dense values equal the leaf reads, the
        # ragged raw bytes match cell for cell, absent chunks are fill.
        for shard in shards:
            (rank,) = grid.block_index(shard)
            span = slice(rank * 16, (rank + 1) * 16)
            leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
            for name in ("count", "h_mean", "morton"):
                np.testing.assert_array_equal(group["4"][name][span], leaf[name][:])
            assert np.isnan(group["4"]["h_mean"][span][8:12]).all()  # inner chunk 2
            assert [bytes(a) for a in group["4"]["h"][span]] == [bytes(b) for b in leaf["h"][:]]
        # The run parquet carries the init as run-level columns and each leaf's
        # commit as row columns.
        df = pd.read_parquet(summary["run_stats_path"])
        assert set(df["icechunk_init_repo"]) == {init["path"]}
        assert set(df["icechunk_init_snapshot"]) == {init["snapshot"]}
        assert df["icechunk_init_error"].isna().all()
        # One worker: the leaves commit sequentially, so the contention
        # counter must read exactly zero on both — a nonzero value here would
        # mean a commit rebased against nothing. Contention itself is pinned
        # deterministically by TestLeafRefs::test_rebase_is_counted, which
        # forces a stale session and asserts exactly one rebase.
        assert df["icechunk_rebases"].tolist() == [0, 0]
        assert set(df["icechunk_snapshot"]) == set(
            m["icechunk"]["snapshot"] for m in summary["results"]
        )
        assert df["icechunk_commit_s"].notna().all()


class TestCarrier:
    def test_round_trip_both_entry_kinds(self):
        from datetime import datetime, timezone

        from zagg.icechunk_ladder import pack_units, unpack_units

        when = datetime(2026, 9, 24, 3, 0, 0, tzinfo=timezone.utc)
        units = [
            {
                "order": 4,
                "entries": [
                    {
                        "path": "6/count",
                        "sharded": True,
                        "chunk_grid": (4,),
                        "arr_offset": (8,),
                        "locations": ["file:///r/a/c/0", "", "file:///r/a/c/0", ""],
                        "offsets": np.array([0, 0, 64, 0], dtype="<u8"),
                        "lengths": np.array([64, 0, 64, 0], dtype="<u8"),
                        "checksum": when,
                        "refs": 2,
                    },
                    {
                        "path": "6/x",
                        "sharded": False,
                        "chunks": [("6/x/c/9", "file:///r/x/c/1", 12, '"etag"')],
                        "refs": 1,
                    },
                ],
            },
            {"order": 3, "entries": []},
        ]
        back, meta = unpack_units(pack_units(units, "file:///r/", node="1111"), "file:///r/")
        assert meta == {"spec": "zagg-icechunk-refs/1", "node": "1111"}
        assert [u["order"] for u in back] == [4, 3]
        sharded, regular = back[0]["entries"]
        assert sharded["locations"] == units[0]["entries"][0]["locations"]
        assert sharded["chunk_grid"] == (4,) and sharded["arr_offset"] == (8,)
        np.testing.assert_array_equal(sharded["offsets"], [0, 0, 64, 0])
        assert sharded["checksum"] == when and sharded["refs"] == 2
        assert regular["chunks"] == [("6/x/c/9", "file:///r/x/c/1", 12, '"etag"')]

    def test_foreign_spec_is_refused(self):
        from zagg.icechunk_ladder import unpack_units

        with pytest.raises(ValueError, match="nope/1"):
            unpack_units(json.dumps({"spec": "nope/1", "units": []}).encode(), "file:///r/")


def _ladder_run(monkeypatch, cfg, tmp_path, *, icechunk_block, shards, workers=1):
    """A local run with the staged sweep chained: leaves, columns, ladder, refs."""
    import pandas as pd

    import zagg.processing as processing
    from zagg import runner

    grid = _grid(cfg)
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"] = {
        **cfg.output.get("grid", {}),
        "type": "healpix",
        "parent_order": 4,
        "child_order": 6,
        "chunk_inner": 5,
    }
    cfg.output["sweep"] = "stages"
    cfg.output["icechunk"] = icechunk_block
    monkeypatch.setattr(
        runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a", "secretAccessKey": "s"}
    )

    def fake(g, shard_key, urls, **kwargs):
        for i, (block, _children) in enumerate(g.iter_chunks(int(shard_key))):
            if i == 2:
                continue
            carrier = _carrier(g, shard_key, float(shards.index(int(shard_key)) + 1) * 10 + i)
            local = g.shard_local_region(block, int(shard_key))
            rag = {"h": ([np.array([1.0, 2.0], dtype=np.float32)], [0])} if i == 0 else {}
            kwargs["chunk_results"].append((block, carrier.iloc[local[0]], rag))
        return pd.DataFrame(), {
            "shard_key": int(shard_key),
            "cells_with_data": 12,
            "total_obs": 12,
            "granule_count": 1,
            "files_processed": 1,
            "duration_s": 0.0,
            "error": None,
            "phase_timings": {"read": 0.0, "index": 0.0, "aggregate": 0.0},
        }

    monkeypatch.setattr(processing, "process_shard", fake)
    catalog = {
        "metadata": {"short_name": "ATL06", "version": "007"},
        "grid_signature": {
            "type": "healpix",
            "indexing_scheme": "nested",
            "parent_order": 4,
            "child_order": 6,
            "layout": "fullsphere",
        },
        "shard_keys": shards,
        "granules": [[{"id": f"g{i}", "s3": f"s3://b/g{i}.h5"}] for i in range(len(shards))],
    }
    root = str(tmp_path / "store")
    summary = runner._run_local(
        cfg,
        catalog,
        root,
        6,
        max_cells=None,
        morton_cell=None,
        max_workers=workers,
        overwrite=False,
        dry_run=False,
        region="us-west-2",
    )
    return grid, root, summary


def _stage_rows(root):
    """The staged sweep's run record rows (``sweep_stats_*_stages.json``)."""
    from pathlib import Path

    records = sorted(Path(root).glob("sweep_stats_*_stages.json"))
    assert records, "no staged-sweep run record"
    return json.loads(records[-1].read_text())["stages"]


class TestLadder:
    def test_default_commits_at_the_finest_dispatch_node(self, monkeypatch, cfg, tmp_path):
        # Shard 4 / width 3: tuples dispatch at 3 (child 4, the leaves) and 0.
        # commit_order defaults to 3: each o3 node gathers its leaf sidecars
        # and commits into the base repo (4) and its own overview repo (3);
        # the root tuple commits its own overviews (2, 1, 0) only.
        shards = _shards(_grid(cfg), 6)  # under o3 nodes 1111 (4 leaves) and 1112 (2)
        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        assert summary["cells_error"] == 0
        init = summary["icechunk"]
        assert init["options"] == {"commit": "ladder", "commit_order": 3, "split_order": 3}
        assert init["ladder"] == [0, 1, 2, 3, 4]
        assert init["levels"]["4"]["split"] == {"chunks": 4**2, "order": 3}  # base: chunk order 5
        assert init["levels"]["3"]["split"] == {"chunks": 1, "order": 3}  # one chunk per o3 node
        for meta in summary["results"]:
            assert "sidecar" in meta["icechunk"] and "snapshot" not in meta["icechunk"]
        rows = {r["dispatch_order"]: r for r in _stage_rows(root)}
        assert rows[3]["icechunk_commits"] == 2  # ONE commit per o3 node, orders {4, 3}
        assert rows[3]["icechunk_missing"] == 0 and rows[3]["icechunk_failed"] == 0
        assert rows[0]["icechunk_commits"] == 1  # own overviews at 2, 1, 0, one commit
        assert rows[0]["icechunk_rebases"] == 0 and rows[3]["icechunk_rebases"] == 0
        assert list((tmp_path / "store").rglob("icechunk_refs.json")), "sidecars exist"
        # The base repo reads every leaf back — dense and ragged — via the ladder.
        group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        # Two o3 node commits (leaves + own o3 overviews) and the root's own.
        assert sorted(m for m in messages if m.startswith("node ")) == [
            "node 1",
            "node 1111",
            "node 1112",
        ]
        for shard in shards:
            (rank,) = grid.block_index(shard)
            span = slice(rank * 16, (rank + 1) * 16)
            leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
            for name in ("count", "h_mean", "morton"):
                np.testing.assert_array_equal(group["4"][name][span], leaf[name][:])
            assert np.isnan(group["4"]["h_mean"][span][8:12]).all()
            assert [bytes(a) for a in group["4"]["h"][span]] == [bytes(b) for b in leaf["h"][:]]
        # Every overview level reads its node's object back at the node's rank,
        # from the SAME repo, discoverable from its multiscales root attrs.
        manifest = hive.read_manifest(root)
        levels = {int(e["node"]): int(e["cells"][0]) for e in manifest["pyramid"]["overviews"]}
        assert group.attrs["multiscales"] == manifest["multiscales"]
        from pathlib import Path

        from mortie import mort2healpix

        from zagg.grids.morton import morton_word

        for k in (3, 2, 1, 0):
            r = levels[k]
            objects = sorted(Path(root).glob("/".join(["*"] * (k + 1)) + "/all.zarr"))
            assert objects, f"no order-{k} overview written"
            for obj in objects:
                node = "".join(obj.relative_to(root).parts[:-1])
                rank = int(np.asarray(mort2healpix(np.asarray([morton_word(node)]))[0])[0])
                src = zarr.open_group(str(obj), mode="r")[str(r)]
                n = 4 ** (r - k)
                np.testing.assert_array_equal(
                    group[str(k)]["count"][rank * n : (rank + 1) * n], src["count"][:]
                )
        # The overview-stage gap is closed: every declared order has its level.
        assert set(int(o) for o in init["levels"]) == {0, 1, 2, 3, 4}

    def test_two_level_gather_with_a_coarser_commit_order(self, monkeypatch, cfg, tmp_path):
        # commit_order 0: the o3 nodes write ref COLUMNS (leaf sidecars + own
        # o3 overviews), and the root gathers those columns, adds its own
        # overviews and commits everything -- one commit per repo order.
        shards = _shards(_grid(cfg), 6)
        grid, root, summary = _ladder_run(
            monkeypatch,
            cfg,
            tmp_path,
            icechunk_block={"commit_order": 0, "split_order": 0},
            shards=shards,
        )
        assert summary["icechunk"]["options"] == {
            "commit": "ladder",
            "commit_order": 0,
            "split_order": 0,
        }
        from zagg.icechunk_ladder import read_node_refs

        for node in ("1111", "1112"):
            units = read_node_refs(root, node, store_kwargs={})
            assert units is not None
            assert sorted({u["order"] for u in units}) == [3, 4]
        rows = {r["dispatch_order"]: r for r in _stage_rows(root)}
        assert rows[3]["icechunk_commits"] == 0 and rows[3]["icechunk_refs"] > 0
        assert rows[0]["icechunk_commits"] == 1  # ONE commit, orders 4, 3, 2, 1, 0
        assert rows[0]["icechunk_missing"] == 0
        group, repo = _open(root)
        assert [s.message for s in repo.ancestry(branch="main")][0] == "node 1"
        for shard in shards:
            (rank,) = grid.block_index(shard)
            leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
            np.testing.assert_array_equal(
                group["4"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
            )

    def test_missing_sidecars_are_counted_not_fatal(self, monkeypatch, cfg, tmp_path):
        shards = _shards(_grid(cfg), 2)
        import os

        from zagg.icechunk_ladder import leaf_refs_key

        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        # Delete one leaf's sidecar and re-run the staged sweep alone.
        leaf = hive.shard_leaf_path(root, shards[0])
        os.remove(f"{leaf.rpartition('/')[0]}/{leaf_refs_key(leaf.rpartition('/')[2])}")
        from zagg.sweep_stages import run_stage_sweep

        out = run_stage_sweep(root, [(s, None) for s in shards], store_kwargs={})
        rows = {r["dispatch_order"]: r for r in out["stages"]}
        assert rows[3]["icechunk_missing"] == 1 and rows[3]["icechunk_failed"] == 0

    def test_a_corrupt_sidecar_costs_its_leaf_only(self, monkeypatch, cfg, tmp_path):
        # A per-leaf fault must not be given node-wide blast radius: the
        # sibling leaves' refs still commit, and the bad carrier is counted.
        shards = _shards(_grid(cfg), 2)
        from pathlib import Path

        from zagg.icechunk_ladder import leaf_refs_key

        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        leaf = hive.shard_leaf_path(root, shards[0])
        sidecar = f"{leaf.rpartition('/')[0]}/{leaf_refs_key(leaf.rpartition('/')[2])}"
        Path(sidecar).write_bytes(b"{not json at all")
        from zagg.sweep_stages import run_stage_sweep

        out = run_stage_sweep(root, [(s, None) for s in shards], store_kwargs={})
        rows = {r["dispatch_order"]: r for r in out["stages"]}
        assert rows[3]["icechunk_missing"] == 1 and rows[3]["icechunk_failed"] == 0
        assert rows[3]["icechunk_commits"] == 1 and rows[3]["icechunk_refs"] > 0
        # The surviving sibling is still indexed by the node's commit.
        group, _repo = _open(root)
        (rank,) = grid.block_index(shards[1])
        leaf_group = zarr.open_group(hive.shard_leaf_path(root, shards[1]), mode="r")["6"]
        np.testing.assert_array_equal(
            group["4"]["count"][rank * 16 : (rank + 1) * 16], leaf_group["count"][:]
        )

    def test_a_carrier_without_a_geometry_block_is_refused(self, monkeypatch, cfg, tmp_path):
        # Keyed off the carrier's own dict, an absent geometry block compared
        # {} != {} and sailed through the vet.
        from pathlib import Path

        from zagg.icechunk_ladder import GEOMETRY_KEYS, REFS_SPEC, leaf_refs_key

        shards = _shards(_grid(cfg), 2)
        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        leaf = hive.shard_leaf_path(root, shards[0])
        sidecar = Path(f"{leaf.rpartition('/')[0]}/{leaf_refs_key(leaf.rpartition('/')[2])}")
        carrier = json.loads(sidecar.read_text())
        assert set(GEOMETRY_KEYS) == set(carrier.pop("geometry"))
        assert carrier["spec"] == REFS_SPEC
        sidecar.write_text(json.dumps(carrier))
        from zagg.sweep_stages import run_stage_sweep

        out = run_stage_sweep(root, [(s, None) for s in shards], store_kwargs={})
        rows = {r["dispatch_order"]: r for r in out["stages"]}
        assert rows[3]["icechunk_missing"] == 1 and rows[3]["icechunk_failed"] == 0

    def test_a_level_the_repo_lacks_is_skipped_not_fatal(self, monkeypatch, cfg, tmp_path):
        # The manifest's pyramid is mutable by design, so it can declare a
        # level the repo has no group for; the node must still commit the
        # levels the repo HAS -- the base leaf refs above all.
        shards = _shards(_grid(cfg), 2)
        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={}, shards=shards
        )
        import zagg.icechunk_ladder as ladder_mod

        real = ladder_mod.ladder_context

        def drop_order_three(store_root, manifest, *, store_kwargs):
            block = real(store_root, manifest, store_kwargs=store_kwargs)
            levels = {k: v for k, v in block["levels"].items() if k != "3"}
            return {**block, "levels": levels}

        monkeypatch.setattr(ladder_mod, "ladder_context", drop_order_three)
        from zagg.sweep_stages import run_stage_sweep

        out = run_stage_sweep(root, [(s, None) for s in shards], store_kwargs={})
        rows = {r["dispatch_order"]: r for r in out["stages"]}
        # The o3 tuple's one order is gone, so the node commits its children's
        # base refs alone -- and says so rather than losing the whole node.
        assert rows[3]["icechunk_skipped_levels"] == 1
        assert rows[3]["icechunk_commits"] == 1 and rows[3]["icechunk_failed"] == 0
        assert rows[3]["icechunk_refs"] > 0
        assert rows[0]["icechunk_skipped_levels"] == 0
        group, _repo = _open(root)
        for shard in shards:
            (rank,) = grid.block_index(shard)
            leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
            np.testing.assert_array_equal(
                group["4"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
            )

    def test_leaf_mode_ladder_commits_overviews_only(self, monkeypatch, cfg, tmp_path):
        shards = _shards(_grid(cfg), 2)
        grid, root, summary = _ladder_run(
            monkeypatch, cfg, tmp_path, icechunk_block={"commit": "leaf"}, shards=shards
        )
        for meta in summary["results"]:
            assert meta["icechunk"]["snapshot"]  # the leaf committed itself
        _group, repo = _open(root)
        messages = [s.message for s in repo.ancestry(branch="main")]
        # The leaves committed themselves; the ladder commits overviews only.
        assert sorted(m for m in messages if m.startswith("leaf ")) == ["leaf 11111", "leaf 11112"]
        assert sorted(m for m in messages if m.startswith("node ")) == ["node 1", "node 1111"]
        rows = {r["dispatch_order"]: r for r in _stage_rows(root)}
        assert rows[3]["icechunk_commits"] == 1  # the o3 overviews of node 1111 only
        assert rows[0]["icechunk_commits"] == 1


class TestKnob:
    def test_default_on_for_hive_off_otherwise(self, cfg):
        from zagg.config import get_icechunk

        cfg.output["store_layout"] = "hive"
        assert get_icechunk(cfg) is True
        cfg.output["icechunk"] = None
        assert get_icechunk(cfg) is True
        cfg.output["icechunk"] = False
        assert get_icechunk(cfg) is False
        cfg.output["store_layout"] = "flat"
        cfg.output.pop("icechunk")
        assert get_icechunk(cfg) is False

    def test_default_follows_the_writer_scope_not_the_layout(self, cfg):
        # Spec §11.6: a windowed store's leaves share a shard rank and a
        # raster product is never sharded, so stage 1 records no refs for
        # either — the default resolves OFF rather than standing up a repo no
        # leaf can ever fill.
        from zagg.config import get_icechunk, get_store_layout

        cfg.output["store_layout"] = "hive"
        assert get_icechunk(cfg) is True
        cfg.output["windowing"] = _WINDOWING
        assert get_icechunk(cfg) is False
        cfg.output.pop("windowing")
        raster = default_config("sentinel2_l2a")
        assert get_store_layout(raster) == "hive" and get_icechunk(raster) is False

    def test_validate_rejects_non_bool_and_out_of_scope_shapes(self, cfg):
        from zagg.config import validate_config

        cfg.output["store_layout"] = "hive"
        cfg.output["icechunk"] = "yes"
        with pytest.raises(ValueError, match="output.icechunk must be a boolean"):
            validate_config(cfg)
        cfg.output["store_layout"] = "flat"
        cfg.output["icechunk"] = True
        with pytest.raises(ValueError, match="requires output.store_layout: hive"):
            validate_config(cfg)
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = _WINDOWING
        with pytest.raises(ValueError, match=r"windowed stores \(spec §11.6\)"):
            validate_config(cfg)
        raster = default_config("sentinel2_l2a")
        raster.output["icechunk"] = True
        with pytest.raises(ValueError, match=r"raster products \(spec §11.6\)"):
            validate_config(raster)

    def test_knob_is_outside_the_semantic_core(self, cfg):
        from zagg.semantics import semantic_hash

        cfg.output["store_layout"] = "hive"
        before = semantic_hash(cfg)
        cfg.output["icechunk"] = False
        assert semantic_hash(cfg) == before
        # The ladder knobs are layout, not semantics: split/commit orders
        # change no leaf byte (the §11.5 ratchet relies on this).
        cfg.output["icechunk"] = {"commit": "ladder", "commit_order": 3, "split_order": 4}
        assert semantic_hash(cfg) == before


class TestS3Kwargs:
    """The S3 halves of ``_storage``/``_container`` — the ONLY path the fleet
    takes, and the one every other test in this file sidesteps by running on
    ``local_filesystem_storage``.

    Pure kwargs assembly: the four icechunk constructors are captured, never
    called for real, so this pins the claim that the module mirrors
    :mod:`zagg.store`'s credential and ACL rules (issue #495).
    """

    @pytest.fixture
    def captured(self, monkeypatch):
        seen: dict = {}

        def recorder(name):
            def fake(**kwargs):
                seen[name] = kwargs
                return f"<{name}>"

            return fake

        def static(**kwargs):
            seen["static"] = kwargs
            return "<static>"

        def refreshable(fn):
            seen["refreshable"] = fn
            return "<refreshable>"

        real_store = icechunk.s3_store

        def store(**kwargs):
            # VirtualChunkContainer takes a real ObjectStoreConfig, so this one
            # records AND builds; the storage constructor is fully faked.
            seen["s3_store"] = kwargs
            return real_store(**kwargs)

        monkeypatch.setattr(icechunk, "s3_storage", recorder("s3_storage"))
        monkeypatch.setattr(icechunk, "s3_store", store)
        monkeypatch.setattr(icechunk, "s3_static_credentials", static)
        monkeypatch.setattr(icechunk, "s3_refreshable_credentials", refreshable)
        return seen

    def test_ambient_storage_and_container(self, captured):
        assert icechunk_refs._storage("s3://my-bucket/runs/o4", {"region": "us-west-2"}) == (
            "<s3_storage>"
        )
        kwargs = captured["s3_storage"]
        assert kwargs["bucket"] == "my-bucket" and kwargs["prefix"] == "runs/o4"
        assert kwargs["region"] == "us-west-2" and kwargs["endpoint_url"] is None
        assert kwargs["allow_http"] is False and kwargs["force_path_style"] is False
        # Ambient: the refreshable botocore chain, not frozen keys — and an
        # in-account bucket takes no ACL header.
        assert callable(kwargs["get_credentials"])
        assert "access_key_id" not in kwargs and "write_headers" not in kwargs

        container, creds = icechunk_refs._container("s3://my-bucket/runs", {"region": "us-west-2"})
        store = captured["s3_store"]
        assert store["region"] == "us-west-2" and store["endpoint_url"] is None
        assert store["allow_http"] is False and store["force_path_style"] is False
        assert creds == "<refreshable>"
        assert captured["refreshable"] is icechunk_refs._boto3_credentials
        assert isinstance(container, icechunk.VirtualChunkContainer)

    def test_injected_credentials_are_static_and_take_the_acl(self, captured):
        creds = {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TOK"}
        icechunk_refs._storage("s3://theirs/p", {"region": "us-west-2", "credentials": creds})
        kwargs = captured["s3_storage"]
        assert kwargs["access_key_id"] == "AK" and kwargs["secret_access_key"] == "SK"
        assert kwargs["session_token"] == "TOK" and "get_credentials" not in kwargs
        # Issue #495: injected credentials mean a target this account does not
        # own, so every object-creating request carries the canned ACL.
        assert kwargs["write_headers"] == {"x-amz-acl": "bucket-owner-full-control"}

        _container, cred = icechunk_refs._container("s3://theirs", {"credentials": creds})
        assert cred == "<static>"
        assert captured["static"] == {
            "access_key_id": "AK",
            "secret_access_key": "SK",
            "session_token": "TOK",
        }

    def test_published_bucket_takes_the_acl_on_ambient_credentials(self, captured):
        from zagg.store import _PUBLISHED_BUCKETS

        bucket = sorted(_PUBLISHED_BUCKETS)[0]
        icechunk_refs._storage(f"s3://{bucket}/zagg", {"region": "us-west-2"})
        kwargs = captured["s3_storage"]
        assert callable(kwargs["get_credentials"])
        assert kwargs["write_headers"] == {"x-amz-acl": "bucket-owner-full-control"}

    def test_custom_endpoint_is_path_style_http_and_never_external(self, captured):
        kw = {
            "region": "us-east-1",
            "endpoint_url": "http://localhost:9000",
            "credentials": {"accessKeyId": "AK", "secretAccessKey": "SK"},
        }
        icechunk_refs._storage("s3://minio/p", kw)
        kwargs = captured["s3_storage"]
        assert kwargs["endpoint_url"] == "http://localhost:9000"
        assert kwargs["allow_http"] is True and kwargs["force_path_style"] is True
        # A custom endpoint excludes both external routes (zagg.store's rule),
        # so no ACL header even with injected credentials.
        assert "write_headers" not in kwargs
        icechunk_refs._container("s3://minio", kw)
        store = captured["s3_store"]
        assert store["allow_http"] is True and store["force_path_style"] is True


def test_container_prefix_forms():
    assert icechunk_refs.container_prefix("s3://b/p/") == "s3://b/p/"
    assert icechunk_refs.container_prefix("s3://b/p") == "s3://b/p/"
    local = icechunk_refs.container_prefix("/tmp/x/")
    assert local.startswith("file:///") and local.endswith("/x/")


def test_repo_path():
    assert icechunk_refs.repo_path("s3://b/p/") == "s3://b/p/icechunk"


def test_block_json_round_trips(cfg, tmp_path):
    grid = _grid(cfg)
    manifest = hive.build_manifest(grid)
    options = icechunk_refs.resolve_options(cfg, 4)
    spec = icechunk_refs.repo_group_spec(grid, str(tmp_path), options, manifest)
    json.dumps(spec.attributes)
