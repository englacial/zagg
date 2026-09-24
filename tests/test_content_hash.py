"""§5 O11 content-hash recipe port: parity + raise-gate tests (issue #342).

Phase-1 gates: byte-identical parity against the committed conformance
fixtures' PINNED ``content_hashes`` (recomputed from the fixture stores —
per-array AND combined), the §5.2 element→bytes normalization including its
"anything else → raise" gate, and an optional cross-check against the
moczarr reference implementation (``moczarr.stats``) when it is importable.
The recipe is not zagg's to adjust: a parity failure here is a spec/fixture
finding to raise on issue #342, never something to patch around.
"""

import importlib.util
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import zarr
from zarr.storage import LocalStore

from zagg.content_hash import (
    StreamingArrayHash,
    combined_hash,
    content_hashes_record,
    hash_array,
    hash_arrays,
)

SPEC_DATA = Path(__file__).parent / "data" / "spec"
FIXTURES = ("minimal", "kitchen_sink")


def _expected(name: str) -> dict:
    return json.loads((SPEC_DATA / f"{name}.expected.json").read_text())


def _leaf_group(name: str, exp: dict):
    leaf = SPEC_DATA / name / exp["leaf"]
    return zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)


class TestFixtureParity:
    """The pinned fixture hashes (spec §7) recomputed through the port."""

    @pytest.mark.parametrize("name", FIXTURES)
    def test_per_array_hashes_match_pinned(self, name):
        exp = _expected(name)
        assert hash_arrays(_leaf_group(name, exp)) == exp["content_hashes"]["arrays"]

    @pytest.mark.parametrize("name", FIXTURES)
    def test_combined_hash_matches_pinned(self, name):
        exp = _expected(name)
        hashes = hash_arrays(_leaf_group(name, exp))
        assert combined_hash(hashes) == exp["content_hashes"]["combined"]

    @pytest.mark.parametrize("name", FIXTURES)
    def test_record_shape_matches_pinned_and_is_key_sorted(self, name):
        exp = _expected(name)
        record = content_hashes_record(hash_arrays(_leaf_group(name, exp)))
        assert record == exp["content_hashes"]
        assert list(record["arrays"]) == sorted(record["arrays"])

    @pytest.mark.parametrize("name", FIXTURES)
    def test_moczarr_reference_agrees(self, name):
        """Optional cross-check against the reference implementation."""
        moczarr_stats = pytest.importorskip("moczarr.stats")
        exp = _expected(name)
        theirs = moczarr_stats.hash_arrays(str(SPEC_DATA / name), exp["leaf"])
        ours = hash_arrays(_leaf_group(name, exp))
        assert theirs == ours
        assert moczarr_stats.combined_hash(theirs) == combined_hash(ours)


class TestVlenRecipe:
    """§5.2 element→bytes normalization, one test per contract row."""

    def _obj(self, elements):
        values = np.empty(len(elements), dtype=object)
        values[:] = elements
        return values

    def test_length_prefix_is_injective(self):
        # Without the u64le prefix these two cell grids would collide.
        assert hash_array(self._obj([b"ab", b"c"])) != hash_array(self._obj([b"a", b"bc"]))

    def test_none_hashes_as_empty_payload(self):
        # An unwritten cell may decode as None, not b"" — identical by design.
        assert hash_array(self._obj([None, b"x"])) == hash_array(self._obj([b"", b"x"]))

    def test_str_is_utf8(self):
        assert hash_array(self._obj(["hé"])) == hash_array(self._obj(["hé".encode()]))

    def test_ndarray_element_is_le_c_order_bytes(self):
        # A typed /2 cell decodes to an ndarray whose LE C-order bytes are the
        # /1 cell's payload bytes (spec §6.2) — the same hash, no recipe change.
        payload = np.array([[1.5, 2.0], [3.0, 4.0]], dtype="<f4")
        assert hash_array(self._obj([payload])) == hash_array(self._obj([payload.tobytes()]))

    def test_big_endian_ndarray_element_is_byteswapped(self):
        le = np.array([1, 2, 3], dtype="<u8")
        assert hash_array(self._obj([le.astype(">u8")])) == hash_array(self._obj([le]))

    def test_unsupported_element_raises(self):
        # The "anything else → raise" gate: never hash a repr/pointer buffer.
        with pytest.raises(ValueError, match="no O11 byte recipe"):
            hash_array(self._obj([3.5]))


GENERATOR = Path(__file__).parent.parent / "tools" / "generate_spec_fixtures.py"


def _generator():
    """The spec fixture generator, loaded as a module (the tools/ precedent)."""
    spec = importlib.util.spec_from_file_location("zagg_spec_fixture_generator", GENERATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_kitchen_sink(root: Path, *, sharded: bool = True):
    """One kitchen-sink leaf through the PRODUCTION write path (generator inputs).

    Returns ``(metadata, leaf_dir)`` — the worker metadata now carries the
    staged-source ``content_hashes`` (issue #342 phase 2). ``sharded=False``
    drives the same inputs through the per-chunk STREAMING leaf write (the
    path a ``chunk_inner``-unset grid always takes).
    """
    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid
    from zagg.grids.morton import morton_word

    gen = _generator()
    cfg = gen._config(kitchen_sink=True)
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=sharded)
    shard = morton_word(gen.SHARD_KEY)
    by_chunk, _cells = gen._build_cells(grid, shard, kitchen_sink=True)
    hive.ensure_manifest(
        str(root), hive.build_manifest(grid, dataset={"short_name": "O11_TEST", "version": "1"})
    )
    inner = gen._fake_process_shard(grid, by_chunk, kitchen_sink=True)

    def fake(*args, **kwargs):
        # The generator's fake only knows the ``chunk_results`` sink; the
        # streaming path passes ``write_chunk`` instead, so collect the chunks
        # locally and replay them through the callback.
        write_chunk = kwargs.get("write_chunk")
        if kwargs.get("chunk_results") is None:
            kwargs["chunk_results"] = []
        df, meta = inner(*args, **kwargs)
        if write_chunk is not None:
            for block, carrier, ragged in kwargs["chunk_results"]:
                write_chunk(block, carrier, ragged)
        # The generator's fake seeds no phase_timings; the hash timing rides
        # an existing dict only (the write-stamp gate), so seed one here.
        meta["phase_timings"] = {"read": 0.0, "index": 0.0, "aggregate": 0.0}
        return df, meta

    original = processing.process_shard
    processing.process_shard = fake
    try:
        meta = hive.process_and_write_hive(
            shard, ["s3://fixture/a.h5"], grid, {}, str(root), cfg, store_kwargs={}
        )
    finally:
        processing.process_shard = original
    assert meta.get("error") is None, meta
    leaf_rel = hive.shard_leaf_path("", shard).lstrip("/")
    return meta, root / leaf_rel


@pytest.fixture(scope="module")
def kitchen_leaf(tmp_path_factory):
    root = tmp_path_factory.mktemp("o11_store")
    return _write_kitchen_sink(root)


class TestWorkerWiring:
    """Issue #342 phase 2: staged in-worker hashing on the hive leaf write."""

    def test_metadata_carries_structured_record(self, kitchen_leaf):
        meta, _leaf = kitchen_leaf
        record = meta["content_hashes"]
        assert set(record) == {"arrays", "combined"}
        assert record["combined"] == combined_hash(record["arrays"])
        assert list(record["arrays"]) == sorted(record["arrays"])

    def test_write_read_parity(self, kitchen_leaf):
        # THE acceptance gate for the ratified staged-source decision (4):
        # hashes computed from the staged arrays must equal hashes recomputed
        # from a full store read-back — the dtype-cast/fill drift guard.
        meta, leaf = kitchen_leaf
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        read_back = hash_arrays(group)
        assert meta["content_hashes"]["arrays"] == read_back
        assert meta["content_hashes"]["combined"] == combined_hash(read_back)

    def test_matches_pinned_fixture_hashes(self, kitchen_leaf):
        # Same seeded inputs as the committed kitchen_sink fixture, so the
        # worker-recorded hashes must reproduce the PINNED values exactly.
        meta, _leaf = kitchen_leaf
        assert meta["content_hashes"] == _expected("kitchen_sink")["content_hashes"]

    def test_hash_phase_timing_recorded(self, kitchen_leaf):
        meta, _leaf = kitchen_leaf
        assert meta["phase_timings"]["hash"] >= 0.0

    def test_unused_staged_key_warns(self, kitchen_leaf, caplog):
        # A staged key matching no enumerated array is a broken writer seam,
        # not a correctness bug: the hashes still equal the unstaged call, but
        # it has to be LOUD or a totally dead staging pipeline is invisible.
        _meta, leaf = kitchen_leaf
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        with caplog.at_level(logging.WARNING, logger="zagg.content_hash"):
            hashes = hash_arrays(group, staged={"nonexistent/key": np.zeros(1)})
        assert hashes == hash_arrays(group)
        assert "staged values for ['nonexistent/key']" in caplog.text

    def test_staged_values_take_priority_over_the_store(self, kitchen_leaf):
        # Proof the staged mapping is the hash source when present: a doctored
        # staged value flips exactly that key, everything else read-back.
        _meta, leaf = kitchen_leaf
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        honest = hash_arrays(group)
        doctored = hash_arrays(group, staged={"6/count": np.zeros(16, dtype=np.int32)})
        assert doctored["6/count"] != honest["6/count"]
        assert {k: v for k, v in doctored.items() if k != "6/count"} == {
            k: v for k, v in honest.items() if k != "6/count"
        }


class TestStampRecord:
    """Issue #580 phase 2: the §5.3 record rides the commit stamp itself."""

    def test_stamp_carries_the_sidecar_record(self, kitchen_leaf):
        # Computed from the staged arrays BEFORE the stamp lands: the stamp
        # copy, the metadata (sidecar) copy and a full read-back all agree.
        from zagg.hive import read_commit

        meta, leaf = kitchen_leaf
        stamp = read_commit(LocalStore(str(leaf)))
        assert stamp["content_hashes"] == meta["content_hashes"]
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        assert stamp["content_hashes"]["arrays"] == hash_arrays(group)

    def test_stamp_key_absent_when_hashing_fails(self, tmp_path, monkeypatch):
        # Fail-open (§5.3): a §5.2 raise gate lands as a stamp WITHOUT the key
        # — the leaf still commits, and absence reads unverifiable.
        from zagg.hive import read_commit

        monkeypatch.setattr(
            "zagg.content_hash.hash_arrays",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        meta, leaf = _write_kitchen_sink(tmp_path / "nohash")
        assert "content_hashes" not in meta
        stamp = read_commit(LocalStore(str(leaf)))
        assert stamp["complete"] is True and "content_hashes" not in stamp

    def test_moczarr_reference_agrees_with_the_stamp(self, kitchen_leaf):
        # The parity gate the issue asks for: moczarr's reference recipe over
        # the written leaf reproduces the stamp's record exactly.
        moczarr_stats = pytest.importorskip("moczarr.stats")
        from zagg.hive import read_commit

        _meta, leaf = kitchen_leaf
        root = leaf.parents[5]  # the leaf path is 1/1/2/1/3/11213.zarr under the root
        theirs = moczarr_stats.hash_arrays(str(root), str(leaf.relative_to(root)))
        stamp = read_commit(LocalStore(str(leaf)))
        assert stamp["content_hashes"]["arrays"] == theirs
        assert stamp["content_hashes"]["combined"] == moczarr_stats.combined_hash(theirs)


class TestStreamingLeafStaging:
    """The UNSHARDED (streaming) hive leaf stages its dense arrays too.

    ``sharded`` is forced off whenever a leaf holds one inner chunk — the
    ``chunk_inner``-unset default — so this is the common hive config, not a
    corner. Decision (4) ("staged, no read-back GETs") has to hold on it.
    """

    def test_unsharded_leaf_reproduces_pinned_hashes_from_staged_values(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="zagg.content_hash"):
            meta, leaf = _write_kitchen_sink(tmp_path / "unsharded", sharded=False)
        # Decoded-value hashing is packaging-invariant (§5): the same inputs
        # through the streaming write must reproduce the sharded fixture's
        # PINNED hashes exactly, despite the different object layout.
        assert meta["content_hashes"] == _expected("kitchen_sink")["content_hashes"]
        # ... and still equal a full read-back (the dtype-cast/fill guard).
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        assert meta["content_hashes"]["arrays"] == hash_arrays(group)
        # Every staged key landed on an enumerated array — the dense arrays
        # (``morton`` included) came from the staged slabs, not a read-back.
        assert "staged values for" not in caplog.text


class TestCoverageCompleteness:
    """Issue #342 phase 3: the hash set is the leaf's OWN array inventory.

    Enumeration is discovery-based (§5.1, ``group.members``) — never a
    hardcoded field list — so a new array cannot silently drop out of the
    hash set. The literal key sets below are the anti-circularity pin (the
    implementation and the inventory walk would otherwise agree trivially).
    """

    def test_kitchen_sink_hash_set_equals_inventory(self, kitchen_leaf):
        meta, leaf = kitchen_leaf
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        inventory = {k for k, node in group.members(max_depth=None) if isinstance(node, zarr.Array)}
        assert set(meta["content_hashes"]["arrays"]) == inventory
        # The full stratified surface, by name: the morton coordinate, dense
        # fields, ragged payloads AND their locations siblings.
        assert inventory == {
            "6/morton",
            "6/count",
            "6/composition",
            "6/h_tdigest_signal",
            "6/h_tdigest_signal_locations",
            "6/h_tdigest_noise",
            "6/h_tdigest_noise_locations",
        }

    def test_companion_field_enters_hash_set_via_readback_fallback(
        self, monkeypatch, tmp_path, caplog
    ):
        # A ``resolution: chunk`` companion is written per chunk-block, never
        # as one staged slab — the one production case where ``hash_arrays``
        # falls back to reading the (tiny) array back. Driven through the
        # REAL process_shard + hive leaf write (the TestShardedMixedFieldKinds
        # config: scalar + vector + companion + ragged).
        import zagg.processing as processing  # noqa: F401  (patch target)
        from zagg import hive
        from zagg.config import default_config
        from zagg.grids import HealpixGrid
        from zagg.index.hierarchical import HierarchicalIndex

        cfg = default_config()
        agg = cfg.aggregation
        agg.setdefault("chunk_precompute", {})["chunk_base"] = {
            "expression": "np.float32(np.mean(h_li))",
            "source": "h_li",
        }
        agg["variables"]["h_edges"] = {
            "expression": "np.array([np.min(h_li), np.max(h_li)])",
            "source": "h_li",
            "kind": "vector",
            "trailing_shape": 2,
            "dtype": "float32",
        }
        agg["variables"]["h_chunk_base"] = {
            "expression": "chunk_base",
            "resolution": "chunk",
            "dtype": "float32",
        }
        agg["variables"]["h_ragged"] = {
            "function": "np.sort",
            "source": "h_li",
            "kind": "ragged",
            "inner_shape": [1],
            "dtype": "float32",
        }
        grid = HealpixGrid(4, 7, layout="fullsphere", config=cfg, chunk_inner=6, sharded=True)
        from mortie import geo2mort

        shard_key = int(geo2mort(-78.5, -132.0, order=4)[0])
        children = grid.children(shard_key)
        idx = [0, len(children) // 2, len(children) - 1]
        df = pd.DataFrame(
            {
                "h_li": np.array([3.0, 7.0, 2.0], dtype=np.float32),
                "s_li": np.array([0.1, 0.1, 0.1], dtype=np.float32),
                "leaf_id": np.array([int(children[i]) for i in idx], dtype=np.uint64),
            }
        )
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df.copy() if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        monkeypatch.setattr(
            "zagg.processing.worker.index_from_config", lambda cfg: HierarchicalIndex()
        )
        with caplog.at_level(logging.WARNING, logger="zagg.content_hash"):
            meta = hive.process_and_write_hive(
                shard_key,
                ["s3://x"],
                grid,
                {},
                str(tmp_path / "store"),
                cfg,
                store_kwargs={},
                handoff="pandas",  # the one_shot stub yields pandas carriers
            )
        assert meta.get("error") is None, meta
        # The writer's staged keys all land on enumerated arrays — no silent
        # key-composition drift between write.py and the members() walk.
        assert "staged values for" not in caplog.text
        leaf = hive.shard_leaf_path(str(tmp_path / "store"), shard_key)
        group = zarr.open_group(LocalStore(leaf), mode="r", zarr_format=3)
        inventory = {k for k, node in group.members(max_depth=None) if isinstance(node, zarr.Array)}
        recorded = meta["content_hashes"]["arrays"]
        assert set(recorded) == inventory
        assert {"7/h_chunk_base", "7/h_edges", "7/h_ragged", "7/morton"} <= inventory
        # Write-read parity holds across BOTH sources: staged slabs (dense,
        # vector, ragged) and the companion's read-back fallback.
        assert recorded == hash_arrays(group)


class TestStreamingArrayHash:
    """Issue #342 phase 5: the incremental digest equals the whole-array one.

    The identity the raster path rests on — a live sha256 fed one
    leading-axis row at a time finalizes to exactly ``hash_array`` over the
    assembled array — plus the two preconditions that identity needs, which
    the accumulator ENFORCES rather than assumes.
    """

    def _array(self, n_rows=5, n_cols=4, dtype="uint16"):
        return np.arange(n_rows * n_cols, dtype=dtype).reshape(n_rows, n_cols)

    def test_in_order_rows_equal_whole_array_hash(self):
        values = self._array()
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        for i, row in enumerate(values):
            stream.update(i, row)
        assert stream.finalize() == hash_array(values)

    def test_out_of_order_rows_equal_whole_array_hash(self):
        # The raster sink delivers timesteps in COMPLETION order, so this is
        # the production arrival pattern, not an edge case.
        values = self._array()
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        for i in (3, 0, 4, 2, 1):
            stream.update(i, values[i])
        assert stream.finalize() == hash_array(values)

    def test_unwritten_rows_contribute_fill_bytes(self):
        # Trap (1): the recipe covers the array's FULL contents, so a row
        # that never gets a slab still hashes — as fill.
        values = np.full((4, 3), 7, dtype="int32")
        values[1] = 0  # the fill value: this row is never written
        values[3] = 0
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        stream.update(0, values[0])
        stream.update(2, values[2])
        assert stream.finalize() == hash_array(values)

    def test_float_nan_fill_matches_whole_array(self):
        values = np.full((3, 2), np.nan, dtype="float32")
        values[0] = [1.5, 2.5]
        stream = StreamingArrayHash(values.shape, values.dtype, np.float32("nan"))
        stream.update(0, values[0])
        assert stream.finalize() == hash_array(values)

    def test_no_rows_written_is_all_fill(self):
        stream = StreamingArrayHash((2, 2), np.dtype("uint16"), 0)
        assert stream.finalize() == hash_array(np.zeros((2, 2), dtype="uint16"))

    def test_big_endian_rows_hash_little_endian(self):
        values = self._array(dtype="<u2")
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        for i, row in enumerate(values):
            stream.update(i, row.astype(">u2"))
        # A big-endian row is byteswapped to the canonical form, so the
        # dtype guard rejects it rather than hashing swapped bytes.
        assert stream.finalize() is None
        assert "expected" in stream.invalid_reason

    @pytest.mark.parametrize(
        "bad, match",
        [
            ("duplicate", "more than once"),
            ("out_of_range", "outside the array"),
            ("wrong_shape", "expected"),
        ],
    )
    def test_precondition_violations_drop_the_hash(self, bad, match):
        # Trap (2): never a digest that cannot be stood behind — finalize
        # returns None (§5.3 unverifiable) and says why.
        values = self._array()
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        stream.update(0, values[0])
        if bad == "duplicate":
            stream.update(0, values[0])
        elif bad == "out_of_range":
            stream.update(99, values[1])
        else:
            stream.update(1, values[1][:2])
        assert stream.finalize() is None
        assert match in stream.invalid_reason

    def test_parking_budget_overflow_drops_the_hash(self, monkeypatch):
        monkeypatch.setattr("zagg.content_hash.STREAM_PENDING_MAX_BYTES", 8)
        values = self._array(n_rows=6, n_cols=8)
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        for i in (5, 4, 3):  # all parked behind the missing row 0
            stream.update(i, values[i])
        assert stream.finalize() is None
        assert "parking budget" in stream.invalid_reason

    def test_gap_without_a_fill_value_drops_the_hash(self):
        stream = StreamingArrayHash((3, 2), np.dtype("uint16"), None)
        stream.update(0, np.zeros(2, dtype="uint16"))
        assert stream.finalize() is None
        assert "fill_value" in stream.invalid_reason

    def test_concurrent_updates_are_serialized(self):
        # The sink hands slabs to worker threads at write_buffer > 1.
        from concurrent.futures import ThreadPoolExecutor

        values = self._array(n_rows=32, n_cols=8)
        stream = StreamingArrayHash(values.shape, values.dtype, 0)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: stream.update(i, values[i]), range(len(values))))
        assert stream.finalize() == hash_array(values)


class TestDenseRecipe:
    def test_big_endian_array_hashes_as_little_endian(self):
        values = np.array([1, 2, 3], dtype="<u4")
        assert hash_array(values.astype(">u4")) == hash_array(values)

    def test_non_contiguous_hashes_as_c_order(self):
        values = np.arange(12, dtype="<f8").reshape(3, 4)
        assert hash_array(values.T) == hash_array(np.ascontiguousarray(values.T))

    def test_combined_is_order_immune_but_value_sensitive(self):
        hashes = {"a": "00", "b": "ff"}
        assert combined_hash(hashes) == combined_hash({"b": "00", "a": "ff"})
        assert combined_hash(hashes) != combined_hash({"a": "00", "b": "fe"})
