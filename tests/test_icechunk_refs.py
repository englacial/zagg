"""Icechunk companion repos (issue #580, spec §11): init, per-leaf refs, read-back.

Everything runs on ``local_filesystem_storage`` with a ``file://`` container —
the same code path the local backend takes, end to end through the production
leaf writer (``hive.process_and_write_hive``), so the ref arithmetic is pinned
against real shard objects, never against a hand-built index.
"""

from __future__ import annotations

import json
import threading

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
    """
    import zagg.processing as processing

    grid.config.output["icechunk"] = bool(refs)

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


def _open(root, order):
    repo = icechunk_refs.open_repo(root, order, store_kwargs={})
    return zarr.open_group(repo.readonly_session("main").store, mode="r"), repo


class TestSplit:
    def test_exponent_clears_the_dictionary_gate_with_margin(self):
        # 4^6 = 4096 >= 4 * 1000; 4^5 = 1024 does not.
        assert icechunk_refs.split_exponent(9, 13) == 6
        assert 4**6 >= icechunk_refs.LOCATION_DICT_MARGIN * icechunk_refs.LOCATION_DICT_MIN_CHUNKS
        assert 4**5 < icechunk_refs.LOCATION_DICT_MARGIN * icechunk_refs.LOCATION_DICT_MIN_CHUNKS

    def test_exponent_never_splits_a_leaf(self):
        # A wide leaf (shard 6 / chunk 13: 4^7 chunks) forces m up to the leaf.
        assert icechunk_refs.split_exponent(6, 13) == 7

    def test_exponent_caps_at_a_base_cell(self):
        # Tiny test geometry: the gate would need m=6 but the axis only has
        # order-5 chunks, so one manifest per base cell.
        assert icechunk_refs.split_exponent(4, 5) == 5

    def test_block_names_the_cell_order(self, cfg):
        # Production geometry: 4096 chunks = one order-7 cell = 16 leaves.
        grid = HealpixGrid(9, 19, config=cfg, chunk_inner=13)
        assert icechunk_refs.split_block(grid) == {"chunks": 4096, "order": 7}


class TestInit:
    def test_creates_the_hierarchy_and_block(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        out = icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        assert out["created"] is True
        assert out["path"] == f"{root}/icechunk/4"
        assert out["order"] == 4
        group, repo = _open(root, 4)
        block = group.attrs[icechunk_refs.ICECHUNK_ATTR]
        assert block["spec"] == "zagg-icechunk/1"
        assert block == {
            "spec": "zagg-icechunk/1",
            "order": 4,
            "shard_order": 4,
            "chunk_order": 5,
            "cell_order": 6,
            "url_prefix": icechunk_refs.container_prefix(root),
            "split": out["split"],
        }
        assert block["url_prefix"].startswith("file://") and block["url_prefix"].endswith("/")
        # The resolution group mirrors the leaf's attrs (dggs + conventions,
        # latitude token included), never a commit stamp.
        leaf_attrs = grid.shard_spec().attributes
        assert dict(group["6"].attrs) == leaf_attrs
        assert group["6"].attrs["dggs"]["latitude"] == "authalic-wgs84"
        assert hive.COMMIT_ATTR not in group.attrs
        messages = [s.message for s in repo.ancestry(branch="main")]
        assert messages[0] == f"init {RUN_ID}"

    def test_arrays_are_rerooted_on_the_order(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        group, _repo = _open(root, 4)
        leaf = grid.shard_spec().members
        for name, spec in leaf.items():
            arr = group["6"][name]
            leaf_shape = tuple(spec.shape)
            assert arr.shape == (grid.n_shards * leaf_shape[0], *leaf_shape[1:])
            assert arr.shape[0] == 12 * 4**grid.child_order or name.endswith("_chunk")
            assert arr.chunks == tuple(grid.chunk_shape)  # the INNER chunk
            assert arr.shards is None  # the sharding wrapper is gone
            assert arr.metadata.dimension_names == tuple(spec.dimension_names)
            assert arr.metadata.data_type.to_json(zarr_format=3) == spec.data_type
        # Ragged: vlen-bytes + zstd inner chain, the §1.2 block intact.
        rag = group["6"]["h"]
        assert [c.__class__.__name__ for c in rag.metadata.codecs] == [
            "VLenBytesCodec",
            "ZstdCodec",
        ]
        assert rag.attrs["ragged"]["spec"] == "zagg-ragged/1"
        assert list(rag.attrs["ragged"]["element"]["shape"]) == [-1, 1]

    def test_persists_split_and_container(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        out = icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        repo = icechunk_refs.open_repo(root, 4, store_kwargs={})  # no config passed
        containers = repo.config.virtual_chunk_containers
        assert list(containers) == [icechunk_refs.container_prefix(root)]
        splitting = repo.config.manifest.splitting
        assert splitting is not None
        # The structured form, not a substring of the repr: §11.5 splits on
        # Axis(0) of any array, and the size must be attached to THAT axis and
        # THAT condition, which a repr search cannot tell apart from an
        # unrelated field carrying the same number.
        ((condition, dims),) = splitting.split_sizes
        assert isinstance(condition, icechunk.ManifestSplitCondition.AnyArray)
        ((axis, size),) = dims
        assert isinstance(axis, icechunk.ManifestSplitDimCondition.Axis)
        assert axis._0 == 0 and size == out["split"]["chunks"]

    def test_rerun_reopens_without_a_commit(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        first = icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        second = icechunk_refs.init_repo(root, grid, run_id="run-2", store_kwargs={})
        assert second["created"] is False
        assert second["snapshot"] == first["snapshot"]
        assert second["split"] == first["split"]
        _group, repo = _open(root, 4)
        assert [s.message for s in repo.ancestry(branch="main")][0] == f"init {RUN_ID}"

    def test_rerun_with_another_geometry_raises(self, cfg, tmp_path):
        # A store whose leaves were cleared but whose root survived reopens the
        # stale repo under a new-geometry manifest; the idempotent branch is a
        # match check, so the second init refuses instead of silently reusing
        # an array model this run's leaves do not fit.
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, _grid(cfg), run_id=RUN_ID, store_kwargs={})
        other = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        with pytest.raises(ValueError, match="was built for"):
            icechunk_refs.init_repo(root, other, run_id="run-2", store_kwargs={})

    def test_read_block(self, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        assert icechunk_refs.read_block(root, 4, store_kwargs={}) is None
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        assert icechunk_refs.read_block(root, 4, store_kwargs={})["order"] == 4

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
        count = plan["6/count"]
        assert count["sharded"] is True
        assert count["chunk_grid"] == (4,)
        assert count["arr_offset"] == (rank * 4,)
        # Chunk ordinal 2 was never written: the sentinel emits no ref.
        assert [bool(loc) for loc in count["locations"]] == [True, True, False, True]
        assert count["refs"] == 3
        assert count["checksum"] is None  # file:// container: no checksum
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
        rag = plan["6/h"]
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
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
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
            assert out["refs"] > 0 and out["rebases"] == 0 and out["checksum"] is None
            assert out["path"] == f"{root}/icechunk/4"
        group, repo = _open(root, 4)
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
                np.testing.assert_array_equal(group["6"][name][span], leaf[name][:])
            # Ragged bytes match cell for cell (chunk 0 populated, the rest fill).
            got = group["6"]["h"][span]
            want = leaf["h"][:]
            assert [bytes(a) for a in got] == [bytes(b) for b in want]
            assert bytes(got[0]) == np.array([1.0, 2.0], dtype="<f4").tobytes()
        # Cells of a leaf nobody wrote read as fill.
        other = _shards(grid, 3)[2]
        (rank,) = grid.block_index(other)
        assert np.isnan(group["6"]["h_mean"][rank * 16 : (rank + 1) * 16]).all()

    def test_windowed_and_empty_units_are_skipped(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
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
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        other = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        with pytest.raises(ValueError, match="was built for"):
            icechunk_refs.record_leaf(root, other, shard, store_kwargs={})
        assert calls == []  # neither refusal paid for a plan

    def test_unsharded_leaf_refs_one_object_per_chunk(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg, sharded=False)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        _write_leaf(monkeypatch, grid, root, shard, skip_chunks=(1,))
        plan = {
            e["path"]: e for e in icechunk_refs.leaf_ref_plan(grid, shard, root, store_kwargs={})
        }
        count = plan["6/count"]
        assert count["sharded"] is False and count["refs"] == 3
        (rank,) = grid.block_index(shard)
        keys = sorted(k for k, _l, _n, _e in count["chunks"])
        assert keys == sorted(f"6/count/c/{rank * 4 + j}" for j in (0, 2, 3))
        out = icechunk_refs.record_leaf(root, grid, shard, store_kwargs={})
        assert out["refs"] > 0
        group, _repo = _open(root, 4)
        leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
        np.testing.assert_array_equal(
            group["6"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
        )

    def test_concurrent_leaf_commits_all_land(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
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
        group, repo = _open(root, 4)
        assert len([s for s in repo.ancestry(branch="main")]) == len(shards) + 2  # + init + birth
        for i, shard in enumerate(shards):
            (rank,) = grid.block_index(shard)
            assert (group["6"]["count"][rank * 16 : (rank + 1) * 16] >= i + 1).all()

    def test_rebase_is_counted(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        repo = icechunk_refs.open_repo(root, 4, store_kwargs={})
        stale = repo.writable_session("main")
        fresh = repo.writable_session("main")
        fresh.store.set_virtual_ref(
            "6/count/c/0", icechunk_refs.container_prefix(root) + "x", offset=0, length=4
        )
        icechunk_refs._commit(fresh, "a", local=True)
        stale.store.set_virtual_ref(
            "6/count/c/1", icechunk_refs.container_prefix(root) + "y", offset=0, length=4
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
    out = {"Payload": _Payload(raw)}
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
            "path": "s3://b/p/icechunk/4",
            "order": 4,
            "snapshot": "SNAP",
            "created": True,
            "split": {"chunks": 1024, "order": 0},
        }
        client = _Client(_envelope(body))
        out = self._call(client)
        # The blocking round-trip times itself (it may carry a cold start) and
        # the measurement rides the record, not a sibling summary key --
        # ``setup_s`` keeps its pre-fan-out bracket meaning.
        assert out.pop("invoke_s") >= 0.0
        assert out == {
            "path": "s3://b/p/icechunk/4",
            "order": 4,
            "snapshot": "SNAP",
            "created": True,
            "split": {"chunks": 1024, "order": 0},
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
        assert out["created"] is True and out["path"] == f"{root}/icechunk/4"
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
        assert body["path"] == f"{root}/icechunk/4"
        assert icechunk_refs.read_block(root, 4, store_kwargs={})["shard_order"] == 4
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
        assert icechunk_refs.read_block(root, 4, store_kwargs={}) is None


class TestWorkerWiring:
    """Phase 4: ``process_and_write_hive`` records refs after the stamp, fail-open."""

    def test_leaf_write_records_refs_and_reads_back(self, monkeypatch, cfg, tmp_path):
        grid = _grid(cfg)
        cfg.output["store_layout"] = "hive"
        root = str(tmp_path / "store")
        icechunk_refs.init_repo(root, grid, run_id=RUN_ID, store_kwargs={})
        shard = _shards(grid, 1)[0]
        meta = _write_leaf(monkeypatch, grid, root, shard, skip_chunks=(2,), refs=True)
        ice = meta["icechunk"]
        assert ice["refs"] > 0 and ice["rebases"] == 0 and ice["commit_s"] >= 0.0
        assert ice["path"] == f"{root}/icechunk/4"
        assert meta["phase_timings"]["icechunk"] >= 0.0
        # The commit is the leaf's, on top of init, and the repo reads the leaf.
        group, repo = _open(root, 4)
        assert [s.message for s in repo.ancestry(branch="main")][
            0
        ] == f"leaf {morton_decimal(shard)}"
        assert repo.lookup_branch("main") == ice["snapshot"]
        (rank,) = grid.block_index(shard)
        leaf = zarr.open_group(hive.shard_leaf_path(root, shard), mode="r")["6"]
        np.testing.assert_array_equal(
            group["6"]["count"][rank * 16 : (rank + 1) * 16], leaf["count"][:]
        )
        # The record rides the D20 record and flattens to parquet scalars.
        from zagg.telemetry import build_record, flatten_record

        record = build_record(shard_key=shard, metadata=meta, granule_ids=["g"])
        assert record["icechunk"] == ice
        row = flatten_record(record)
        assert row["icechunk_snapshot"] == ice["snapshot"]
        assert row["icechunk_rebases"] == 0 and row["icechunk_refs"] == ice["refs"]
        assert row["icechunk_error"] is None and row["icechunk_skipped"] is None

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
        assert df["icechunk_repo"].tolist() == [f"{tmp_path}/icechunk/4"]
        assert df["icechunk_init"].tolist() == ["SNAP"]
        # A fail-open init records its error in the same column.
        path = write_run_parquet(
            str(tmp_path), self._rows(), run_id="abd", icechunk_init={"error": "RuntimeError: x"}
        )
        df = pd.read_parquet(path)
        assert df["icechunk_repo"].isna().all() and df["icechunk_init"].tolist() == [
            "RuntimeError: x"
        ]
        # Off-hive / opted out: both null, columns still present.
        df = pd.read_parquet(write_run_parquet(str(tmp_path), self._rows(), run_id="abe"))
        assert df["icechunk_repo"].isna().all() and df["icechunk_init"].isna().all()

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
        monkeypatch.setattr(runner, "_check_signature", lambda *a, **k: None)

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
            "grid_signature": {"type": "healpix", "parent_order": 4, "child_order": 6},
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
            max_workers=2,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert summary["cells_with_data"] == 2 and summary["cells_error"] == 0
        init = summary["icechunk"]
        assert init["created"] is True and init["path"] == f"{root}/icechunk/4"
        # Every leaf committed its refs on top of the init commit.
        for meta in summary["results"]:
            assert meta["icechunk"]["refs"] > 0 and "error" not in meta["icechunk"]
        group, repo = _open(root, 4)
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
                np.testing.assert_array_equal(group["6"][name][span], leaf[name][:])
            assert np.isnan(group["6"]["h_mean"][span][8:12]).all()  # inner chunk 2
            assert [bytes(a) for a in group["6"]["h"][span]] == [bytes(b) for b in leaf["h"][:]]
        # The run parquet carries the init as run-level columns and each leaf's
        # commit as row columns.
        df = pd.read_parquet(summary["run_stats_path"])
        assert set(df["icechunk_repo"]) == {init["path"]} and set(df["icechunk_init"]) == {
            init["snapshot"]
        }
        # Two workers on local storage: the commit lock serializes them, and a
        # session opened before the other's commit rebases once — the counter
        # the fleet's contention question reads. Never more than one here.
        assert set(df["icechunk_rebases"]) <= {0, 1} and df["icechunk_rebases"].sum() <= 1
        assert set(df["icechunk_snapshot"]) == set(
            m["icechunk"]["snapshot"] for m in summary["results"]
        )
        assert df["icechunk_commit_s"].notna().all()


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
    assert icechunk_refs.repo_path("s3://b/p/", 9) == "s3://b/p/icechunk/9"


def test_block_json_round_trips(cfg, tmp_path):
    grid = _grid(cfg)
    spec = icechunk_refs.repo_group_spec(grid, str(tmp_path), icechunk_refs.split_block(grid))
    json.dumps(spec.attributes)
