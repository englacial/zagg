"""``grids.base.emit_hierarchy``: one ``create_hierarchy`` pass, the same bytes (issue #657).

The leaf and column writers template through this helper instead of
pydantic-zarr's node-at-a-time ``to_zarr``. The contract it has to keep is
byte identity — every metadata document the one-pass route writes equals the
one ``to_zarr`` writes for the same spec — plus a handle for every node, so
the data writes that follow never re-open an array.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import zarr
from pydantic_zarr.experimental.v3 import GroupSpec
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync
from zarr.errors import ContainsGroupError
from zarr.storage import MemoryStore

from zagg.column_shards import column_grid
from zagg.config import default_config
from zagg.grids import HealpixGrid
from zagg.grids.base import emit_hierarchy, vlen_dtype_warning_suppressed
from zagg.sweep_overview import _overview_config

#: A wide column group's fields (§4.6): a NaN-filled float beside the counts
#: and a vector-celled digest, so the group is one ShardingCodec object per array.
COLUMN_FIELDS = {
    "count": {"class": "exact", "method": "sum", "dtype": "int64", "fill_value": 0},
    "h_mean": {"class": "exact", "method": "mean", "dtype": "float32", "fill_value": "NaN"},
    "h_tdigest": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 4096,
    },
}


def _documents(store: MemoryStore) -> dict[str, bytes]:
    async def _keys():
        return [key async for key in store.list()]

    prototype = default_buffer_prototype()
    return {key: sync(store.get(key, prototype)).to_bytes() for key in sync(_keys())}


def _leaf_spec(**kwargs) -> tuple[HealpixGrid, GroupSpec]:
    grid = HealpixGrid(
        9, 19, layout="fullsphere", config=default_config("atl03_tdigest_strata_healpix"), **kwargs
    )
    spec = GroupSpec(
        members={grid.group_path: grid.shard_spec(cell_coordinate=False)}, attributes={}
    )
    return grid, spec


def _column_spec() -> GroupSpec:
    grid = column_grid(9, 15, _overview_config(COLUMN_FIELDS))
    assert grid.sharded and grid.chunks_per_shard > 1
    return GroupSpec(members={"15": grid.shard_spec()}, attributes={})


@pytest.mark.parametrize(
    "build",
    [
        pytest.param(lambda: _leaf_spec(chunk_inner=15, sharded=True)[1], id="leaf-sharded"),
        pytest.param(lambda: _leaf_spec(chunk_inner=15, sharded=False)[1], id="leaf-unsharded"),
        pytest.param(lambda: _leaf_spec()[1], id="leaf-one-chunk"),
        pytest.param(_column_spec, id="column-wide"),
    ],
)
def test_one_pass_writes_the_same_bytes_as_to_zarr(build):
    """Dense sharded / unsharded, vlen ragged + located/timed siblings, a NaN-filled column."""
    spec = build()
    via_to_zarr, via_one_pass = MemoryStore(), MemoryStore()
    with vlen_dtype_warning_suppressed():
        spec.to_zarr(via_to_zarr, "", overwrite=True)
        emit_hierarchy(via_one_pass, spec)
    expected, got = _documents(via_to_zarr), _documents(via_one_pass)
    assert set(got) == set(expected)
    assert [k for k in expected if expected[k] != got[k]] == []


def test_the_identity_cases_cover_sharding_and_a_nan_fill():
    """The parametrize cannot collapse to one spec again (K == 1 turns sharding off)."""
    docs = [
        node.model_dump()
        for build in (lambda: _leaf_spec(chunk_inner=15, sharded=True)[1], _column_spec)
        for node in build().to_flat().values()
    ]
    arrays = [doc for doc in docs if doc.get("node_type") == "array"]
    assert any(c["name"] == "sharding_indexed" for doc in arrays for c in doc["codecs"])
    assert any(str(doc["fill_value"]) == "NaN" for doc in arrays)


def test_every_node_comes_back_as_a_handle():
    grid, spec = _leaf_spec()
    store = MemoryStore()
    with vlen_dtype_warning_suppressed():
        nodes = emit_hierarchy(store, spec)
    flat = {key.lstrip("/") for key in spec.to_flat()}
    assert set(nodes) == flat
    assert isinstance(nodes[""], zarr.Group) and isinstance(nodes[grid.group_path], zarr.Group)
    arrays = {k: v for k, v in nodes.items() if isinstance(v, zarr.Array)}
    assert set(arrays) == {k for k in flat if k.count("/") == 1}
    # The handle is the array on the store: a write through it lands there.
    count = arrays[f"{grid.group_path}/count"]
    count[:4] = 7
    assert zarr.open_array(store, path=f"{grid.group_path}/count", mode="r")[:4].tolist() == [7] * 4


def test_the_pass_issues_no_reads(monkeypatch):
    """No probe: the caller cleared the prefix, so every existence GET would 404 (PR #661)."""
    _grid, spec = _leaf_spec()
    store = MemoryStore()

    async def no_read(self, *args, **kwargs):
        raise AssertionError("emit_hierarchy read from the store")

    for method in ("get", "exists", "list", "list_dir", "list_prefix", "delete_dir"):
        monkeypatch.setattr(MemoryStore, method, no_read)
    with vlen_dtype_warning_suppressed():
        nodes = emit_hierarchy(store, spec)
    assert set(nodes) == {k.lstrip("/") for k in spec.to_flat()}


def test_emit_shard_template_refuses_an_occupied_prefix_without_overwrite():
    """``to_zarr`` accepted an identical template here; one root probe raises (issue #657)."""
    grid, _spec = _leaf_spec()
    store = MemoryStore()
    with vlen_dtype_warning_suppressed():
        grid.emit_shard_template(store)
        with pytest.raises(ContainsGroupError):
            grid.emit_shard_template(store)
        grid.emit_shard_template(store, overwrite=True)


def _companion_leaf(sharded: bool):
    """A K=16 leaf carrying every writer branch the handle map must cover.

    Dense ``h_min``, a ``resolution: chunk`` scalar companion ``h_anchor``, a
    per-cell ragged ``h_raw`` and a ``resolution: chunk`` ragged companion
    ``h_raw_chunk``; every 4th inner chunk empty. Returns ``(grid, shard_key,
    chunk_results, expected)`` with blocks GLOBAL (the ``process_shard`` sink
    shape) and ``expected[local] = (h_min, anchor, {cell: payload}, chunk_payload)``.
    """
    from mortie import geo2mort

    from zagg.config import PipelineConfig

    cfg = PipelineConfig(
        data_source={"groups": ["g"]},
        aggregation={
            "chunk_precompute": {
                "chunk_anchor": {"expression": "np.float32(0.0)", "source": "h_li"}
            },
            "variables": {
                "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                "h_anchor": {
                    "expression": "chunk_anchor",
                    "source": "h_li",
                    "resolution": "chunk",
                    "dtype": "float32",
                },
                "h_raw": {
                    "function": "np.sort",
                    "source": "h_li",
                    "kind": "ragged",
                    "inner_shape": [1],
                    "dtype": "float32",
                },
                "h_raw_chunk": {
                    "function": "np.sort",
                    "source": "h_li",
                    "kind": "ragged",
                    "resolution": "chunk",
                    "inner_shape": [1],
                    "dtype": "float32",
                },
            },
        },
    )
    grid = HealpixGrid(4, 8, layout="fullsphere", config=cfg, chunk_inner=6, sharded=sharded)
    shard_key = int(geo2mort(-78.5, -132.0, order=4)[0])
    shard_block = grid.block_index(shard_key)[0]
    rng = np.random.default_rng(11)
    chunk_results: list = []
    expected: dict = {}
    for block, _children in grid.iter_chunks(shard_key):
        local = int(block[0]) - shard_block * grid.chunks_per_shard
        if local % 4 == 3:
            chunk_results.append((block, pd.DataFrame(), {}))
            continue
        h_min = rng.standard_normal(grid.cells_per_chunk).astype(np.float32)
        anchor = np.float32(local)
        carrier = pd.DataFrame(
            {"h_min": h_min, "h_anchor": np.full(grid.cells_per_chunk, anchor, np.float32)}
        )
        cells = sorted(int(c) for c in rng.choice(grid.cells_per_chunk, 2, replace=False))
        raw = [rng.standard_normal((int(n), 1)).astype(np.float32) for n in (2, 3)]
        chunk_payload = np.full((local + 1, 1), local, dtype=np.float32)
        ragged = {"h_raw": (raw, cells), "h_raw_chunk": ([chunk_payload] * 2, cells)}
        chunk_results.append((block, carrier, ragged))
        expected[local] = (h_min, anchor, dict(zip(cells, raw)), chunk_payload)
    return grid, shard_key, chunk_results, expected


def _no_reopen(monkeypatch):
    from zagg.processing import write as write_mod

    def no_reopen(*_args, **_kwargs):
        raise AssertionError("the leaf path re-opened an array (issue #657)")

    monkeypatch.setattr(write_mod, "open_array", no_reopen)


def _assert_leaf(store, grid, expected):
    """Every branch's values landed: dense, both companions, the ragged slab."""
    path = grid.group_path
    h_min = zarr.open_array(store, path=f"{path}/h_min", mode="r")[:]
    anchor = zarr.open_array(store, path=f"{path}/h_anchor", mode="r")[:]
    raw = zarr.open_array(store, path=f"{path}/h_raw", mode="r")[:]
    raw_chunk = zarr.open_array(store, path=f"{path}/h_raw_chunk", mode="r")[:]
    n = grid.cells_per_chunk
    for local, (dense, value, payloads, chunk_payload) in expected.items():
        np.testing.assert_array_equal(h_min[local * n : (local + 1) * n], dense)
        assert anchor[local] == value
        for cell, payload in payloads.items():
            got = np.frombuffer(raw[local * n + cell], np.float32).reshape(-1, 1)
            np.testing.assert_array_equal(got, payload)
        got = np.frombuffer(raw_chunk[local], np.float32).reshape(-1, 1)
        np.testing.assert_array_equal(got, chunk_payload)
    assert np.all(np.isnan(anchor[sorted(set(range(grid.chunks_per_shard)) - set(expected))]))


@pytest.mark.parametrize("sharded", [True, False], ids=["sharded", "streaming"])
def test_the_leaf_writers_write_through_the_template_handles(monkeypatch, sharded):
    """``emit_shard_nodes`` hands every leaf writer its arrays: no ``open_array`` follows.

    Sharded is ``write_leaf_to_zarr``; streaming (the default when
    ``chunk_inner`` is unset) is ``write_dataframe_to_zarr`` per chunk +
    ``write_ragged_leaf_to_zarr`` — each with both ``resolution: chunk`` kinds.
    """
    from zagg.hive import leaf_block_index
    from zagg.processing import write as write_mod

    grid, shard_key, chunk_results, expected = _companion_leaf(sharded)
    store = MemoryStore()
    with vlen_dtype_warning_suppressed():
        nodes = grid.emit_shard_nodes(store, overwrite=True)
    _no_reopen(monkeypatch)
    if sharded:
        write_mod.write_leaf_to_zarr(
            chunk_results, store, grid=grid, shard_key=shard_key, arrays=nodes
        )
    else:
        ragged_chunks = []
        for block, carrier, ragged in chunk_results:
            local = leaf_block_index(grid, block, shard_key)
            write_mod.write_dataframe_to_zarr(
                carrier, store, grid=grid, chunk_idx=local, arrays=nodes
            )
            ragged_chunks.append((local, ragged))
        write_mod.write_ragged_leaf_to_zarr(ragged_chunks, store, grid=grid, arrays=nodes)
    _assert_leaf(store, grid, expected)


@pytest.mark.parametrize("sharded", [True, False], ids=["sharded", "streaming"])
def test_the_hive_leaf_write_never_reopens_an_array(monkeypatch, tmp_path, sharded):
    """``process_and_write_hive`` threads the template's map to every writer (issue #657)."""
    from zarr.storage import LocalStore

    import zagg.processing as processing
    from zagg import hive

    grid, shard_key, chunk_results, expected = _companion_leaf(sharded)
    hive.ensure_manifest(str(tmp_path), hive.build_manifest(grid, dataset={"short_name": "T"}))

    def fake(g, key, urls, **kwargs):
        for entry in chunk_results:
            if kwargs.get("write_chunk") is not None:
                kwargs["write_chunk"](*entry)
            else:
                kwargs["chunk_results"].append(entry)
        kwargs["occupied_out"].append(np.zeros(0, dtype=np.uint64))
        return pd.DataFrame(), {"shard_key": int(key), "error": None, "total_obs": 1}

    monkeypatch.setattr(processing, "process_shard", fake)
    _no_reopen(monkeypatch)
    meta = hive.process_and_write_hive(
        shard_key, ["s3://fixture/a.h5"], grid, {}, str(tmp_path), grid.config, store_kwargs={}
    )
    assert meta.get("error") is None, meta
    leaf = tmp_path / hive.shard_leaf_path("", shard_key).lstrip("/")
    _assert_leaf(LocalStore(str(leaf)), grid, expected)


def test_a_path_missing_from_the_handle_map_raises_not_reopens(monkeypatch):
    """A template/writer key mismatch is loud: no silent ``open_array`` fallback."""
    from zagg.processing import write as write_mod

    grid, _spec = _leaf_spec()
    store = MemoryStore()
    nodes = grid.emit_shard_nodes(store, overwrite=True)
    monkeypatch.setattr(write_mod, "open_array", lambda *a, **k: pytest.fail("re-opened"))
    with pytest.raises(KeyError, match="did not declare"):
        write_mod._leaf_array(store, grid, "not_a_field", nodes)
    assert write_mod._leaf_array(store, grid, "count", nodes) is nodes[f"{grid.group_path}/count"]
