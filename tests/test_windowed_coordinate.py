"""A windowed leaf stores no per-cell ``morton`` array (issue #586 phase 3).

The cell word is derived from the leaf id and the rank (spec §1.5 "The cell
coordinate"): the writer omits the array on every windowed path, every reader
derives it when it is absent, and ``pyramid_check`` holds the derivation
against the located fields' words. Unwindowed leaves are unchanged.
"""

import importlib.util
import json
import shutil
from pathlib import Path

import numpy as np
import pytest
import zarr
from test_sweep_overview import (
    CELL_ORDER,
    SHARD_ORDER,
    _leaf_cfg,
    _overview_group,
    _run_record,
    _write_manifest,
)
from test_windowed_emit import (
    HANDLER_PATH,
    _cfg,
    _digest_cfg,
    _grid,
    _handle,
    _handler_event,
    _patch,
    _run_bulk,
    _run_fanout,
    _shard_word,
)

from zagg import hive
from zagg.config import get_windowing, validate_config
from zagg.grids import HealpixGrid
from zagg.grids.morton import cell_words, morton_decimal, morton_word, words_in_cell
from zagg.store import open_store

GENERATOR = Path(__file__).parent.parent / "tools" / "generate_spec_fixtures.py"
LABELS = ("2018", "2019", "2020")


def _generator():
    spec = importlib.util.spec_from_file_location("zagg_spec_fixture_generator_586", GENERATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _leaf_group(root, shard, label, grid):
    """``(group, stamp)`` of a leaf's cell-order group, through its root stamp."""
    data, stamp = hive.resolve_leaf(hive.shard_leaf_path(str(root), shard, window=label))
    return zarr.open_group(open_store(data), path=grid.group_path, mode="r", zarr_format=3), stamp


def _sharded(cfg):
    """``cfg`` on a K=4 sharded grid, so the whole-leaf write path runs."""
    cfg.output["grid"]["chunk_inner"] = 7
    validate_config(cfg)
    return cfg


# ── the derivation law ───────────────────────────────────────────────────────


class TestDerivation:
    @pytest.mark.parametrize("decimal", ["3232131144", "-5112333142"])
    def test_order_19_is_one_stride_across_an_order_9_shard(self, decimal):
        # The production geometry, one shard per hemisphere: the southern
        # word sets bit 63 and still derives as a plain uint64 progression.
        shard = morton_word(decimal)
        assert (shard >> 63) == (1 if decimal.startswith("-") else 0)
        words = cell_words(shard, 19)
        assert words.dtype == np.uint64 and words.shape == (4**10,)
        want = words[0] + np.arange(4**10, dtype=np.uint64) * np.uint64(4_194_304)
        np.testing.assert_array_equal(words, want)
        assert morton_decimal(int(words[0])) == decimal + "1" * 10
        assert morton_decimal(int(words[-1])) == decimal + "4" * 10

    @pytest.mark.parametrize("cell_order", [10, 13, 19, 27])
    def test_the_stride_is_two_bits_per_order(self, cell_order):
        # A parent three orders above the cells (64 children), at any depth:
        # never derive a deep order from a coarse shard — 4**18 words.
        parent = morton_word("-5" + ("112333142" + "1" * 18)[: cell_order - 3])
        words = cell_words(parent, cell_order)
        assert len(words) == 64
        assert set(np.diff(words).tolist()) == {2 ** (60 - 2 * cell_order)}

    def test_the_grid_children_are_the_same_function(self):
        grid = HealpixGrid(9, 19, chunk_inner=13)
        shard = morton_word("-5112333142")
        np.testing.assert_array_equal(grid.children(shard), cell_words(shard, 19))

    def test_words_in_cell_decodes_each_words_order(self):
        from mortie import clip2order, common_ancestor, geo2mort, orders_of

        points = geo2mort(np.array([-78.5, -78.5 + 1e-6]), np.array([-132.0, -132.0]), order=29)
        (cell,) = set(clip2order(19, points).tolist())
        merged = common_ancestor(points)  # an area word between 19 and 29
        assert 19 <= int(orders_of(np.array([merged]))[0]) < 29
        neighbour = int(cell_words(clip2order(18, points)[0], 19)[0])
        if neighbour == cell:
            neighbour = int(cell_words(clip2order(18, points)[0], 19)[1])
        coarse = int(clip2order(12, points)[0])  # the cell lies inside it
        elsewhere = int(clip2order(12, geo2mort(np.array([40.0]), np.array([10.0]), order=29))[0])
        words = [int(points[0]), int(merged), cell, neighbour, coarse, elsewhere, 0]
        assert words_in_cell(words, cell).tolist() == [True, True, True, False, True, False, False]
        assert words_in_cell([], cell).shape == (0,)


# ── the writer ───────────────────────────────────────────────────────────────


class TestWriter:
    @pytest.mark.parametrize("run", [_run_bulk, _run_fanout], ids=["unit-shard", "unit-window"])
    @pytest.mark.parametrize(
        "make_cfg",
        [
            _cfg,
            lambda: _sharded(_cfg()),
            lambda: _cfg(mode="spill", buffer_granules=1),
            lambda: _sharded(_digest_cfg()),
            lambda: _digest_cfg(mode="merge", buffer_granules=2),
        ],
        ids=["streamed", "sharded", "spill", "sharded-digest", "merge-digest"],
    )
    def test_a_windowed_leaf_carries_no_morton(self, run, make_cfg, monkeypatch, tmp_path):
        cfg = make_cfg()
        root = str(tmp_path / "store")
        run(monkeypatch, cfg, root)
        grid, shard = hive_grid(cfg), _shard_word()
        for label in LABELS:
            group, stamp = _leaf_group(root, shard, label, grid)
            arrays = set(group.array_keys())
            assert "morton" not in arrays and "count" in arrays
            # §5.3: the key set, on the stamp, has no entry for the coordinate
            # — and it is the leaf's whole array set, nothing missing.
            keys = set(stamp["content_hashes"]["arrays"])
            assert keys == {f"{grid.group_path}/{name}" for name in arrays}
            assert stamp["spec"] == hive.HIVE_SPEC_V2 and stamp["window"] == label
            # The dggs block still names the coordinate (derived, not stored).
            assert group.attrs["dggs"]["coordinate"] == "morton"

    def test_bulk_and_fanout_agree_on_the_key_set(self, monkeypatch, tmp_path):
        cfg = _sharded(_digest_cfg())
        a, b = str(tmp_path / "bulk"), str(tmp_path / "fanout")
        _run_bulk(monkeypatch, cfg, a)
        _run_fanout(monkeypatch, cfg, b)
        grid, shard = hive_grid(cfg), _shard_word()
        for label in LABELS:
            hashes = [_leaf_group(root, shard, label, grid)[1]["content_hashes"] for root in (a, b)]
            assert hashes[0] == hashes[1]

    def test_an_unwindowed_leaf_still_stores_it(self, monkeypatch, tmp_path):
        cfg = _sharded(_cfg())
        del cfg.output["windowing"]
        validate_config(cfg)
        _patch(monkeypatch)
        grid, shard = hive_grid(cfg), _shard_word()
        root = str(tmp_path / "store")
        urls = [f"s3://bucket/granule{n}.h5" for n in "ABC"]
        meta = hive.process_and_write_hive(shard, urls, grid, {}, root, cfg, store_kwargs={})
        assert meta.get("error") is None
        group, stamp = _leaf_group(root, shard, None, grid)
        stored = np.asarray(group["morton"][:])
        derived = cell_words(shard, grid.child_order)
        # Stored == derived on every written chunk (an unwritten one would
        # hold the 0 fill, §7 — the sparsity a derived coordinate lacks).
        written = stored != 0
        assert written.any()
        np.testing.assert_array_equal(stored[written], derived[written])
        assert f"{grid.group_path}/morton" in stamp["content_hashes"]["arrays"]
        assert stamp["spec"] == hive.HIVE_SPEC

    def test_the_unwindowed_fixture_regenerates_unchanged(self, tmp_path):
        # No committed unwindowed leaf moves: `minimal/` rebuilt through
        # today's writer carries the same arrays with the same decoded
        # values — `6/morton` among them — so its §5 record still equals the
        # committed one, whose combined digest is a FROZEN literal in the
        # conformance suite. (Decoded values, not object bytes: a shard
        # object's inner-chunk order is not deterministic across writes.)
        gen = _generator()
        gen.build(tmp_path / "minimal", kitchen_sink=False)
        committed = Path(__file__).parent / "data" / "spec"
        fresh = json.loads((tmp_path / "minimal.expected.json").read_text())
        want = json.loads((committed / "minimal.expected.json").read_text())
        assert fresh["content_hashes"] == want["content_hashes"]
        assert "6/morton" in fresh["content_hashes"]["arrays"]
        groups = [
            zarr.open_group(open_store(str(root / want["leaf"])), path="6", mode="r", zarr_format=3)
            for root in (tmp_path / "minimal", committed / "minimal")
        ]
        assert set(groups[0].array_keys()) == set(groups[1].array_keys()) >= {"morton"}
        for name in groups[1].array_keys():
            got, ref = groups[0][name][:], groups[1][name][:]
            assert got.dtype == ref.dtype and got.tolist() == ref.tolist()

    def test_a_versioned_windowed_leaf(self, monkeypatch, tmp_path):
        cfg = _sharded(_digest_cfg())
        root = str(tmp_path / "store")
        _run_bulk(monkeypatch, cfg, root, run_id="rid")
        grid, shard = hive_grid(cfg), _shard_word()
        for label in LABELS:
            group, pointer = _leaf_group(root, shard, label, grid)
            assert pointer["current"].startswith("run-rid-")
            assert "morton" not in set(group.array_keys())
            assert f"{grid.group_path}/morton" not in pointer["content_hashes"]["arrays"]

    def test_the_cell_ids_hatch_still_writes_its_array(self, monkeypatch, tmp_path):
        # The legacy NESTED array is an explicit opt-in for readers that need
        # it on disk; the ruling drops `morton`, not the hatch.
        cfg = _sharded(_cfg())
        cfg.output["grid"]["emit_cell_ids"] = True
        validate_config(cfg)
        root = str(tmp_path / "store")
        _run_bulk(monkeypatch, cfg, root)
        grid = hive_grid(cfg)
        group, _stamp = _leaf_group(root, _shard_word(), "2019", grid)
        arrays = set(group.array_keys())
        assert "cell_ids" in arrays and "morton" not in arrays

    def test_a_rerun_is_current_not_torn(self, monkeypatch, tmp_path):
        # The identity gate must not read the absent array as a changed or
        # torn leaf (nor the column gate, which verifies the artifact): an
        # identical re-run skips every window and reads nothing.
        from test_windowed_emit import _bulk_unit

        from zagg.telemetry import build_record, write_sidecar

        cfg = _sharded(_digest_cfg())
        root = str(tmp_path / "store")
        gate = dict(skip_if_current=True, sidecar_spec="morton-hive/2", run_id="r1")
        first = _run_bulk(monkeypatch, cfg, root, **gate)
        shard, urls, windows = _bulk_unit(cfg)
        by_label = {w["label"]: w for w in windows}
        for m in first["windows"]:
            assert m.get("leaf_column")
            record = build_record(
                shard_key=shard,
                metadata=m,
                granule_ids=[urls[i] for i in by_label[m["window"]]["granules"]],
                window=m["window"],
            )
            leaf = hive.shard_leaf_path(root, shard, window=m["window"])
            write_sidecar(leaf, record, spec="morton-hive/2")

        def _no_read(*a, **k):
            raise AssertionError("a current windowed leaf must not be re-read")

        monkeypatch.setattr("zagg.processing.worker._concat_and_group", _no_read)
        again = hive.process_and_write_hive(
            shard,
            urls,
            hive_grid(cfg),
            {},
            root,
            cfg,
            store_kwargs={},
            windows=windows,
            **{**gate, "run_id": "r2"},
        )
        assert again["current"] is True
        assert [m["current"] for m in again["windows"]] == [True, True, True]


def hive_grid(cfg):
    """The grid a config's ``output.grid`` block resolves to (chunk_inner included)."""
    block = cfg.output["grid"]
    if "chunk_inner" not in block:
        return _grid(cfg)
    return HealpixGrid(
        parent_order=block["parent_order"],
        child_order=block["child_order"],
        layout="fullsphere",
        config=cfg,
        chunk_inner=block["chunk_inner"],
    )


# ── the readers ──────────────────────────────────────────────────────────────


def _derived_leaf(root, decimal, cells, *, window, shard_order=SHARD_ORDER, cell_order=CELL_ORDER):
    """``test_sweep_overview._make_leaf`` for a windowed leaf as it is now written:
    no ``morton`` member in the template, none on disk."""
    from zagg.stats.tdigest import build_tdigest
    from zagg.sweep_overview import encode_digest

    grid = HealpixGrid(shard_order, cell_order, config=_leaf_cfg())
    store = open_store(hive.shard_leaf_path(str(root), morton_word(decimal), window=window))
    grid.emit_shard_template(store, overwrite=True, cell_coordinate=False)
    group = zarr.open_group(store, path=str(cell_order), mode="r+", zarr_format=3)
    assert "morton" not in set(group.array_keys())
    n = 4 ** (cell_order - shard_order)
    count = np.zeros(n, np.int32)
    h_min = np.full(n, np.nan, np.float32)
    digest = np.full(n, b"", dtype=object)
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        count[i], h_min[i] = len(obs), obs.min()
        digest[i] = encode_digest(build_tdigest(obs, delta=64), "float32")
    group["count"][:] = count
    group["h_min"][:] = h_min
    group["h_tdigest"][:] = digest
    hive.stamp_commit(store, cells_with_data=len(cells), granule_count=1, window=window)


class TestSweepFold:
    def test_the_leaf_fold_reads_leaves_without_a_coordinate(self, tmp_path):
        from zagg.sweep import run_sweep

        _write_manifest(tmp_path, orders=(1,), windowed=True, all_time=True)
        _derived_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        _derived_leaf(tmp_path, "-311", {0: [10.0]}, window="2020")
        word = morton_word("-311")
        result = run_sweep(str(tmp_path), [(word, "2019"), (word, "2020")], families=("overview",))
        family = result["families"]["overview"]
        assert family["written"] == 3 and family["failed"] == 0
        assert _overview_group(tmp_path, "-3/1", "2019.zarr", 3)["count"][0] == 2
        assert _overview_group(tmp_path, "-3/1", "all.zarr", 3)["count"][0] == 3
        # The overview itself still STORES its coordinate (leaves only moved).
        overview = _overview_group(tmp_path, "-3/1", "2019.zarr", 3)
        np.testing.assert_array_equal(overview["morton"][:], cell_words(morton_word("-31"), 3))

    def test_the_mixed_order_guard_stands_on_a_declared_field(self, tmp_path, caplog):
        # Issue #347: a leaf at another cell order is skipped loudly. With no
        # `morton` to measure, the guard measures a declared array instead.
        from zagg.sweep import run_sweep

        _write_manifest(tmp_path, orders=(1,), windowed=True)
        _derived_leaf(tmp_path, "-311", {0: [1.0]}, window="2019")
        _derived_leaf(tmp_path, "-312", {0: [5.0]}, window="2019", cell_order=CELL_ORDER + 1)
        # The odd leaf sits at the manifest's group NAME but the wrong extent.
        odd = Path(hive.shard_leaf_path(str(tmp_path), morton_word("-312"), window="2019"))
        (odd / str(CELL_ORDER + 1)).rename(odd / str(CELL_ORDER))
        refs = [(morton_word(d), "2019") for d in ("-311", "-312")]
        with caplog.at_level("WARNING"):
            result = run_sweep(str(tmp_path), refs, families=("overview",))
        assert result["families"]["overview"]["failed"] >= 1
        assert "mixed-order source leaves" in caplog.text
        assert _overview_group(tmp_path, "-3/1", "2019.zarr", 3)["count"][0] == 1

    def test_shape_helper_keeps_the_unwindowed_footing(self, tmp_path):
        _derived_leaf(tmp_path, "-311", {0: [1.0]}, window="2019")
        group = zarr.open_group(
            open_store(hive.shard_leaf_path(str(tmp_path), morton_word("-311"), window="2019")),
            path=str(CELL_ORDER),
            mode="r",
            zarr_format=3,
        )
        n = 4 ** (CELL_ORDER - SHARD_ORDER)
        assert hive.leaf_cells_shape(group, ["nope", "count"], windowed=True) == (n,)
        assert hive.leaf_cells_shape(group, ["nope"], windowed=True) is None
        # An UNWINDOWED leaf must carry the array: its absence still raises.
        with pytest.raises(KeyError):
            hive.leaf_cells_shape(group, ["count"], windowed=False)


class TestColumnBackfill:
    def _fields(self):
        return json.loads(Path(self.root, hive.MANIFEST_NAME).read_text())["pyramid"]["overview"][
            "fields"
        ]

    def test_stored_slabs_read_a_leaf_without_a_coordinate(self, tmp_path):
        from zagg.column_backfill import stored_leaf_slabs

        self.root = tmp_path
        _write_manifest(tmp_path, windowed=True)
        _derived_leaf(tmp_path, "-311", {0: [1.0, 2.0], 5: [3.0]}, window="2019")
        leaf = hive.shard_leaf_path(str(tmp_path), morton_word("-311"), window="2019")
        n = 4 ** (CELL_ORDER - SHARD_ORDER)
        slabs = stored_leaf_slabs(leaf, self._fields(), cell_order=CELL_ORDER, n_cells=n)
        assert slabs["count"].tolist()[:6] == [2, 0, 0, 0, 0, 1]
        with pytest.raises(ValueError, match="mixed-order source leaves"):
            stored_leaf_slabs(leaf, self._fields(), cell_order=CELL_ORDER, n_cells=n + 1)


class TestTensorReaders:
    """The sweep readers on a derived coordinate report what a stored one does."""

    @pytest.fixture(scope="class")
    def twins(self, tmp_path_factory):
        """``(derived, stored, expected)``: the windowed fixture leaf, and a copy
        carrying the array a pre-phase-3 windowed leaf stored (written chunks
        hold their words, the empty chunk the 0 fill)."""
        tmp = tmp_path_factory.mktemp("twins")
        gen = _generator()
        gen.build_windowed(tmp / "a")
        exp = json.loads((tmp / "a.expected.json").read_text())
        shutil.copytree(tmp / "a", tmp / "b")
        grid = HealpixGrid(4, 6, config=gen._windowed_config(), chunk_inner=5, sharded=True)
        stored = open_store(str(tmp / "b" / exp["leaf"]))
        grid.shard_spec().members["morton"].to_zarr(stored, f"{exp['group']}/morton")
        words = cell_words(morton_word(exp["shard"]), 6)
        lo = exp["empty_chunk"] * exp["cells_per_chunk"]
        words[lo : lo + exp["cells_per_chunk"]] = 0
        zarr.open_array(stored, path=f"{exp['group']}/morton", mode="r+")[:] = words
        return open_store(str(tmp / "a" / exp["leaf"])), stored, exp

    @staticmethod
    def _rows(gen):
        return [(word, pos, np.asarray(values).tolist()) for word, pos, values in gen]

    def test_read_locations_and_subtree(self, twins):
        from zagg.readers import read_locations

        derived, stored, exp = twins
        field = f"{exp['group']}/h_tdigest"
        rows = self._rows(read_locations(derived, field))
        assert rows == self._rows(read_locations(stored, field)) and len(rows) == 4
        chunk = morton_decimal(rows[0][0])
        assert len(chunk) == len(exp["shard"]) + 1  # an order-5 read chunk
        sub = self._rows(read_locations(derived, field, subtree=chunk))
        assert sub == self._rows(read_locations(stored, field, subtree=chunk))
        assert sub and all(row[0] == rows[0][0] for row in sub)

    def test_read_tensors_words_masks_and_blocks(self, twins):
        from zagg.readers import read_tensors

        derived, stored, exp = twins
        field = f"{exp['group']}/h_tdigest"
        for kwargs in ({}, {"block_order": 4}):
            got = list(read_tensors(derived, field, n_bins=8, resolution=4.0, **kwargs))
            want = list(read_tensors(stored, field, n_bins=8, resolution=4.0, **kwargs))
            assert len(got) == len(want) > 0
            for (t1, m1, s1, w1), (t2, m2, s2, w2) in zip(got, want, strict=True):
                assert w1 == w2 and s1 == s2
                np.testing.assert_array_equal(t1, t2)
                np.testing.assert_array_equal(m1, m2)
        # The whole-shard block is the shard: its id is the leaf id.
        (block,) = list(read_tensors(derived, field, n_bins=8, resolution=4.0, block_order=4))
        assert block[3] == morton_word(exp["shard"])

    def test_cell_index_takes_occupancy_from_the_payload(self, twins):
        from mortie import clip2order

        from zagg.readers import cell_index, read_locations

        derived, stored, exp = twins
        field = f"{exp['group']}/h_tdigest"
        for word, (row, col), _locs in read_locations(derived, field):
            assert cell_index(derived, field, word, row, col) == cell_index(
                stored, field, word, row, col
            )
        # The empty chunk has a derived word but no payload: on both twins its
        # id names no stored chunk (the stored twin says so by its 0 fill).
        words = cell_words(morton_word(exp["shard"]), 6)
        empty = int(clip2order(5, words[exp["empty_chunk"] * exp["cells_per_chunk"]])[0])
        for store in (derived, stored):
            with pytest.raises(ValueError, match="no stored read chunk"):
                cell_index(store, field, empty, 0, 0)

    def test_a_versioned_windowed_leaf_derives_from_its_version_stamp(self, monkeypatch, tmp_path):
        from zagg.readers import read_tensors

        cfg = _digest_cfg()
        root = str(tmp_path / "store")
        _run_bulk(monkeypatch, cfg, root, run_id="rid")
        shard = _shard_word()
        data, stamp = hive.resolve_leaf(hive.shard_leaf_path(root, shard, window="2019"))
        assert stamp["current"] and data.endswith(stamp["current"])
        blocks = list(read_tensors(open_store(data), "8/h_tdigest", n_bins=8, resolution=4.0))
        assert [b[3] for b in blocks] == [shard]  # K == 1: the read chunk is the shard

    def test_an_unwindowed_leaf_without_the_array_is_still_refused(self, twins, tmp_path):
        from zagg.readers import read_locations

        _derived, stored, exp = twins
        # Strip the window from the stamp: nothing licenses a derivation.
        shutil.copytree(stored.root, tmp_path / "leaf")
        shutil.rmtree(tmp_path / "leaf" / exp["group"] / "morton")
        meta = json.loads((tmp_path / "leaf" / "zarr.json").read_text())
        del meta["attributes"][hive.COMMIT_ATTR]["window"]
        (tmp_path / "leaf" / "zarr.json").write_text(json.dumps(meta))
        with pytest.raises(ValueError, match="no sibling 'morton' coordinate"):
            list(read_locations(open_store(str(tmp_path / "leaf")), f"{exp['group']}/h_tdigest"))

    def test_the_stamp_names_the_shard(self, twins):
        derived, _stored, exp = twins
        stamp = hive.read_commit(derived)
        np.testing.assert_array_equal(
            hive.derived_leaf_words(stamp, 16), cell_words(morton_word(exp["shard"]), 6)
        )
        assert hive.derived_leaf_words({**stamp, "window": None}, 16) is None
        assert hive.derived_leaf_words(None, 16) is None
        with pytest.raises(ValueError, match="cannot identify the shard"):
            hive.derived_leaf_words({**stamp, "coverage": None}, 16)
        with pytest.raises(ValueError, match="cannot identify the shard"):
            hive.derived_leaf_words(stamp, 15)


# ── pyramid_check ────────────────────────────────────────────────────────────


class TestPyramidCheck:
    def _located(self, tmp_path):
        gen = _generator()
        root = tmp_path / "windowed"
        gen.build_windowed(root)
        exp = json.loads((tmp_path / "windowed.expected.json").read_text())
        _run_record(root, [exp["shard"]], window=exp["window"])
        return root, exp

    def test_containment_passes_on_a_located_windowed_store(self, tmp_path):
        from zagg.pyramid_check import validate_pyramid

        root, exp = self._located(tmp_path)
        report = validate_pyramid(str(root), full=True)
        entry = report["checks"]["coordinates"]
        n_words = sum(len(c["h_tdigest_locations"]) for c in exp["cells"])
        assert entry["status"] == "pass", entry
        assert entry["detail"].startswith(f"{n_words} location word(s) in 4 cell(s) of 1 leaf(s)")
        assert "0 stored coordinate(s) agree" in entry["detail"]
        assert report["roster"] == {"source": "run records", "leaves": 1}
        # The rest of the harness is unwindowed-only, as before.
        assert report["checks"]["declaration"]["status"] == "fail"
        assert report["checks"]["materialization"]["detail"] == "windowed store"

    def test_a_misplaced_location_word_fails_by_name(self, tmp_path):
        from zagg.pyramid_check import format_report, validate_pyramid

        root, exp = self._located(tmp_path)
        store = open_store(str(root / exp["leaf"]))
        arr = zarr.open_array(store, path=f"{exp['group']}/h_tdigest_locations", mode="r+")
        slab = arr[:]
        a, b = exp["cells"][0]["index"], exp["cells"][2]["index"]
        donor = np.frombuffer(bytes(slab[b]), dtype="<u8")
        victim = np.frombuffer(bytes(slab[a]), dtype="<u8").copy()
        victim[0] = donor[0]  # row-aligned still; one word names another cell
        slab[a] = victim.tobytes()
        arr[:] = slab
        report = validate_pyramid(str(root), full=True)
        entry = report["checks"]["coordinates"]
        assert entry["status"] == "fail" and not report["passed"]
        assert (
            f"[{a}]/h_tdigest_locations: 1 of {len(victim)} location word(s) lie outside"
            in (entry["mismatches"][0])
        )
        assert str(int(donor[0])) in entry["mismatches"][0]
        assert "[FAIL] coordinates" in format_report(report)

    def test_a_word_coarser_than_its_cell_passes_but_is_counted(self, tmp_path):
        from zagg.pyramid_check import validate_pyramid

        root, exp = self._located(tmp_path)
        entry = validate_pyramid(str(root), full=True)["checks"]["coordinates"]
        assert "coarser than their cell" not in entry["detail"]  # a point-ingest store makes none
        store = open_store(str(root / exp["leaf"]))
        arr = zarr.open_array(store, path=f"{exp['group']}/h_tdigest_locations", mode="r+")
        slab = arr[:]
        a = exp["cells"][0]["index"]
        victim = np.frombuffer(bytes(slab[a]), dtype="<u8").copy()
        victim[0] = np.uint64(int(exp["shard_word"]))  # on the ancestor line of every cell
        slab[a] = victim.tobytes()
        arr[:] = slab
        entry = validate_pyramid(str(root), full=True)["checks"]["coordinates"]
        assert entry["status"] == "pass", entry
        assert entry["detail"].endswith(
            "; 1 word(s) coarser than their cell, accepted under §9.1 coarse ingest only"
        )

    @pytest.mark.parametrize("torn", [False, True], ids=["invalid-word", "torn-payload"])
    def test_an_undecodable_location_payload_fails_not_raises(self, tmp_path, torn):
        from zagg.pyramid_check import validate_pyramid

        root, exp = self._located(tmp_path)
        store = open_store(str(root / exp["leaf"]))
        arr = zarr.open_array(store, path=f"{exp['group']}/h_tdigest_locations", mode="r+")
        slab = arr[:]
        a = exp["cells"][0]["index"]
        victim = np.frombuffer(bytes(slab[a]), dtype="<u8").copy()
        victim[0] = np.uint64(2**64 - 1)  # mortie refuses it
        slab[a] = victim.tobytes()[:-3] if torn else victim.tobytes()
        arr[:] = slab
        entry = validate_pyramid(str(root), full=True)["checks"]["coordinates"]
        assert entry["status"] == "fail", entry
        assert f"[{a}]/h_tdigest_locations: undecodable location word(s)" in entry["mismatches"][0]

    def test_an_unlocated_windowed_store_reports_derivation_only(self, monkeypatch, tmp_path):
        from zagg.pyramid_check import format_report, validate_pyramid

        cfg = _sharded(_digest_cfg())
        root = tmp_path / "store"
        grid = hive_grid(cfg)
        hive.ensure_manifest(
            str(root),
            hive.build_manifest(
                grid, dataset={"short_name": "T", "version": "1"}, windowing=get_windowing(cfg)
            ),
        )
        _run_bulk(monkeypatch, cfg, str(root))
        for label in LABELS:
            _run_record(root, [morton_decimal(_shard_word())], run_id=f"r{label}", window=label)
        report = validate_pyramid(str(root), full=True)
        entry = report["checks"]["coordinates"]
        # Not a pass: nothing on disk was compared. Not a fail: a legal store.
        assert entry["status"] == "skip"
        assert entry["detail"].startswith("derivation only:")
        assert "neither a located field nor a stored coordinate" in entry["detail"]
        assert "[SKIP] coordinates     derivation only" in format_report(report)

    def test_an_unwindowed_store_keeps_the_stored_comparison(self, tmp_path):
        from zagg.pyramid_check import validate_pyramid

        gen = _generator()
        root = tmp_path / "ks"
        gen.build(root, kitchen_sink=True)
        exp = json.loads((tmp_path / "ks.expected.json").read_text())
        report = validate_pyramid(str(root), full=True, roster="list")
        entry = report["checks"]["coordinates"]
        assert entry["status"] == "pass", entry
        assert "location word(s) in 4 cell(s) of 1 leaf(s)" in entry["detail"]
        assert "4 stored coordinate(s) agree" in entry["detail"]
        # A stored word that disagrees with the derivation is a failure.
        store = open_store(str(root / exp["leaf"]))
        arr = zarr.open_array(store, path=f"{exp['group']}/morton", mode="r+")
        words = arr[:]
        cell = exp["cells"][0]["index"]
        words[cell] = words[cell + 1]
        arr[:] = words
        entry = validate_pyramid(str(root), full=True, roster="list")["checks"]["coordinates"]
        assert entry["status"] == "fail"
        assert f"[{cell}]: stored morton" in entry["mismatches"][0]

    def test_an_unlocated_unwindowed_store_passes_on_the_stored_array(self, tmp_path):
        from zagg.pyramid_check import validate_pyramid

        gen = _generator()
        gen.build(tmp_path / "minimal", kitchen_sink=False)
        entry = validate_pyramid(str(tmp_path / "minimal"), full=True, roster="list")["checks"][
            "coordinates"
        ]
        assert entry["status"] == "pass"
        assert "no located field — containment not checked" in entry["detail"]


# ── the fleet path ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def handler():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_586p3", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestHandler:
    """The Lambda handler reaches the same writer and the same readers."""

    def test_a_bulk_process_event_writes_leaves_without_a_coordinate(
        self, handler, monkeypatch, tmp_path
    ):
        cfg = _cfg()
        _patch(monkeypatch)
        event = _handler_event(cfg, tmp_path)
        resp = _handle(handler, event)
        assert resp["statusCode"] == 200, resp["body"]
        body = json.loads(resp["body"])
        grid = _grid(cfg)
        for record in body["stats"]:
            group, stamp = _leaf_group(
                event["store_path"], event["shard_key"], record["window"], grid
            )
            assert "morton" not in set(group.array_keys())
            # The record the handler returns, the stamp and the leaf agree on
            # the key set — no `{cell_order}/morton` in any of them.
            assert record["content_hashes"] == stamp["content_hashes"]
            assert f"{grid.group_path}/morton" not in record["content_hashes"]["arrays"]

    def test_a_per_window_process_event_writes_the_same_leaf(self, handler, monkeypatch, tmp_path):
        cfg = _cfg()
        _patch(monkeypatch)
        event = _handler_event(cfg, tmp_path)
        windows = event.pop("windows")
        window = next(w for w in windows if w["label"] == "2019")
        event["window"] = {k: window[k] for k in ("label", "start", "end")}
        event["granule_urls"] = [event["granule_urls"][i] for i in window["granules"]]
        resp = _handle(handler, event)
        assert resp["statusCode"] == 200, resp["body"]
        group, stamp = _leaf_group(event["store_path"], event["shard_key"], "2019", _grid(cfg))
        assert "morton" not in set(group.array_keys()) and stamp["window"] == "2019"

    def test_a_sweep_event_folds_leaves_without_a_coordinate(self, handler, tmp_path):
        _write_manifest(tmp_path, orders=(1,), windowed=True)
        _derived_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        _derived_leaf(tmp_path, "-312", {0: [4.0]}, window="2019")
        resp = handler._handle_sweep(
            {
                "mode": "sweep",
                "store_path": str(tmp_path),
                "leaves": [[morton_word(d), "2019"] for d in ("-311", "-312")],
                "families": ["overview"],
            }
        )
        assert resp["statusCode"] == 200, resp["body"]
        overview = _overview_group(tmp_path, "-3/1", "2019.zarr", 3)
        # One order-3 cell per four leaf cells: -311 fills [0, 4), -312 [4, 8).
        counts = overview["count"][:].tolist()
        assert (counts[0], counts[4]) == (2, 1) and sum(counts) == 3
