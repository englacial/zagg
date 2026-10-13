"""``grids.base.emit_hierarchy``: one ``create_hierarchy`` pass, the same bytes (issue #657).

The leaf and column writers template through this helper instead of
pydantic-zarr's node-at-a-time ``to_zarr``. The contract it has to keep is
byte identity — every metadata document the one-pass route writes equals the
one ``to_zarr`` writes for the same spec — plus a handle for every node, so
the data writes that follow never re-open an array.
"""

from __future__ import annotations

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


def test_an_occupied_prefix_is_refused_not_overwritten():
    """The writers clear the prefix themselves; the pass never ``delete_dir``s per node."""
    _grid, spec = _leaf_spec()
    store = MemoryStore()
    with vlen_dtype_warning_suppressed():
        emit_hierarchy(store, spec)
        with pytest.raises(ContainsGroupError):
            emit_hierarchy(store, spec)


def test_the_leaf_writer_writes_through_the_template_handles(monkeypatch):
    """``emit_shard_nodes`` hands the leaf writer its arrays: no ``open_array`` follows."""
    import test_processing

    from zagg.processing import write as write_mod

    grid, shard_key, chunk_results, _payloads, dense = test_processing.TestWriteLeafToZarr._setup()
    store = MemoryStore()
    nodes = grid.emit_shard_nodes(store, overwrite=True)
    assert nodes[f"{grid.group_path}/h_min"].shape == (grid.cells_per_shard,)

    def no_reopen(*_args, **_kwargs):
        raise AssertionError("the leaf path re-opened an array (issue #657)")

    monkeypatch.setattr(write_mod, "open_array", no_reopen)
    write_mod.write_leaf_to_zarr(chunk_results, store, grid=grid, shard_key=shard_key, arrays=nodes)
    written = zarr.open_array(store, path=f"{grid.group_path}/h_min", mode="r")[:]
    for local, values in dense.items():
        start = local * grid.cells_per_chunk
        assert written[start : start + grid.cells_per_chunk].tolist() == values.tolist()


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
