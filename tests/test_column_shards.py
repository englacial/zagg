"""The leaf column's shard layout (issue #631): fine groups on inner chunks.

Spec §4.6's layout rule — a column group narrower than ``4^5`` cells is one
unsharded chunk per array, a wider one is ONE ``sharding_indexed`` object
per array on power-of-four inner chunks of at most ``4^5`` cells and at
least four per object — as the writer lays it, as the backfill's structure
gate and the Icechunk ref planner derive it, and with the parity contract
that both layouts of the same leaf fold to the same values and §5 hashes.
"""

import json
from pathlib import Path

import numpy as np
import pytest
import zarr

from zagg import column_shards
from zagg.column import fold_column, write_column
from zagg.column_shards import (
    COLUMN_INNER_ORDER,
    column_grid,
    expected_layout,
    inner_chunk_cells,
    inner_chunk_order,
    stored_layout,
)
from zagg.grids.healpix import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.hive import COMMIT_ATTR
from zagg.stats.tdigest import build_tdigest
from zagg.store import open_store
from zagg.sweep_overview import _overview_config, encode_digest

#: A geometry with a FINE tier: node 1 / cell 7, groups {6, 5, 4, 3, 2, 1} —
#: 6 is 4^5 cells (sharded, four 256-cell inner chunks), 5 is 256 (whole).
NODE, CELL = 1, 7
RESOLUTIONS = [6, 5, 4, 3, 2, 1]
LEAF_CELLS = 4 ** (CELL - NODE)
DELTA = 64
FIELDS = {
    "count": {"class": "exact", "method": "sum", "dtype": "int64", "fill_value": 0},
    "h_tdigest": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": DELTA,
    },
}
#: Leaf cells placed so group 6's rows land in inner chunks 0, 1 and 3 —
#: chunk 2 stays empty, the shard index's absence sentinel.
CELLS = {0: [1.0, 2.0], 1000: [10.0, 4.0], 2047: [7.0], 4095: [3.0, 8.0, 5.0]}
SHARD = "-31"


def _slabs(cells=CELLS, n=LEAF_CELLS) -> dict:
    count = np.zeros(n, np.int64)
    digest = np.full(n, b"", dtype=object)
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        count[i] = len(obs)
        digest[i] = encode_digest(build_tdigest(obs, delta=DELTA), "float32")
    return {"count": count, "h_tdigest": digest}


def _write(root) -> tuple[str, dict]:
    folded = fold_column(_slabs(), FIELDS, cell_order=CELL, resolutions=RESOLUTIONS)
    basename = write_column(
        str(root), morton_word(SHARD), folded, FIELDS, node_order=NODE, cell_order=CELL
    )
    return f"{root}/-3/1/{basename}", folded


def _array_meta(path: str, res: int, name: str) -> dict:
    return json.loads(Path(f"{path}/{res}/{name}/zarr.json").read_text())


class TestRule:
    """The layout rule is a function of ``(node_order, res)`` alone."""

    @pytest.mark.parametrize(
        ("res", "order", "cells"),
        [
            (16, 11, 1024),
            (15, 10, 1024),
            (14, 10, 256),
            (13, None, 256),
            (12, None, 64),
            (9, None, 1),
        ],
    )
    def test_reference_geometry(self, res, order, cells):
        # 19/13/9 with ``overviews: [16, 15, 14, 13]`` (the v4 ATL03 rebuild):
        # 16 inner chunks of 1,024 at 16, four of 1,024 at 15, four of 256 at
        # 14; 13 and the default tier below it stay one whole chunk.
        assert inner_chunk_order(9, res) == order
        assert inner_chunk_cells(9, res) == cells
        assert expected_layout(9, res) == {"sharded": order is not None, "chunk_cells": cells}

    def test_inner_chunks_are_powers_of_four_with_at_least_four_per_object(self):
        for node in range(0, 10):
            for res in range(node, node + 12):
                n = 4 ** (res - node)
                cells = inner_chunk_cells(node, res)
                assert cells == 4 ** int(np.log2(cells) / 2)  # a power of four
                if inner_chunk_order(node, res) is None:
                    assert cells == n and n < 4**COLUMN_INNER_ORDER
                else:
                    assert cells == min(4**COLUMN_INNER_ORDER, n // 4)
                    assert n // cells >= 4

    def test_the_inner_chunk_is_the_stage_fold_block(self):
        # A stage gather reads a child column one fold block at a time; over
        # a fine leaf column each block read is then exactly one inner chunk.
        from zagg.sweep_fold import STAGE_BLOCK_ORDER

        assert COLUMN_INNER_ORDER == STAGE_BLOCK_ORDER

    def test_coarse_groups_keep_the_pre_631_spec_byte_for_byte(self):
        cfg = _overview_config(FIELDS)
        for res in (13, 12, 11, 10, 9):
            legacy = HealpixGrid(9, res, config=cfg, sharded=True).shard_spec()
            assert column_grid(9, res, cfg).shard_spec().model_dump() == legacy.model_dump()

    def test_stored_layout_reads_both_shapes(self):
        cfg = _overview_config(FIELDS)
        for res in (16, 14, 13):
            spec = column_grid(9, res, cfg).shard_spec()
            for member in spec.members.values():
                assert stored_layout(member.model_dump()) == expected_layout(9, res)


class TestWrittenLayout:
    """The writer lays the fine group on inner chunks, one object per array."""

    def test_fine_group_is_one_sharded_object_per_array(self, tmp_path):
        path, folded = _write(tmp_path)
        for name in ("morton", "count", "h_tdigest"):
            meta = _array_meta(path, 6, name)
            assert meta["shape"] == [1024]
            assert meta["chunk_grid"]["configuration"]["chunk_shape"] == [1024]
            (codec,) = meta["codecs"]
            assert codec["name"] == "sharding_indexed"
            cfg = codec["configuration"]
            assert cfg["chunk_shape"] == [256]
            assert cfg["index_location"] == "end"
            assert cfg["index_codecs"] == [
                {"name": "bytes", "configuration": {"endian": "little"}},
                {"name": "crc32c"},
            ]
            # One object per array per leaf, the shard index inside it.
            assert sorted(p.name for p in Path(f"{path}/6/{name}/c").iterdir()) == ["0"]
            assert (
                stored_layout(meta)
                == expected_layout(NODE, 6)
                == {
                    "sharded": True,
                    "chunk_cells": 256,
                }
            )
        # Inner codecs are the member's own: the leaves' bytes-only policy for
        # dense arrays, vlen-bytes + zstd for the ragged digest.
        inner = {
            n: [c["name"] for c in _array_meta(path, 6, n)["codecs"][0]["configuration"]["codecs"]]
            for n in ("morton", "count", "h_tdigest")
        }
        assert inner == {
            "morton": ["bytes"],
            "count": ["bytes"],
            "h_tdigest": ["vlen-bytes", "zstd"],
        }

    def test_coarser_groups_stay_one_whole_unsharded_chunk(self, tmp_path):
        path, _ = _write(tmp_path)
        for res in (5, 4, 3, 2, 1):
            for name in ("morton", "count", "h_tdigest"):
                meta = _array_meta(path, res, name)
                n = 4 ** (res - NODE)
                assert meta["chunk_grid"]["configuration"]["chunk_shape"] == [n]
                assert meta["codecs"][0]["name"] != "sharding_indexed"
                assert stored_layout(meta) == {"sharded": False, "chunk_cells": n}

    def test_values_round_trip_through_the_shard(self, tmp_path):
        path, folded = _write(tmp_path)
        store = open_store(path)
        for res in RESOLUTIONS:
            group = zarr.open_group(store, path=str(res), mode="r", zarr_format=3)
            np.testing.assert_array_equal(group["count"][:], folded[res]["count"])
            assert [bytes(p) for p in group["h_tdigest"][:]] == [
                bytes(p) for p in folded[res]["h_tdigest"]
            ]
        # A range read of one inner chunk fetches that chunk alone (zarr
        # reads the index then the ranged inner chunk) and sees the values.
        fine = zarr.open_group(store, path="6", mode="r", zarr_format=3)
        np.testing.assert_array_equal(fine["count"][768:1024], folded[6]["count"][768:1024])
        assert int(fine["count"][1023]) == 3 and int(fine["count"][0]) == 2

    def test_empty_inner_chunk_is_absent_from_the_shard_index(self, tmp_path):
        path, _ = _write(tmp_path)
        obj = Path(f"{path}/6/count/c/0").read_bytes()
        index = np.frombuffer(obj[-(16 * 4 + 4) : -4], dtype="<u8").reshape(4, 2)
        present = index[:, 0] != 2**64 - 1
        # Leaf cells 0, 1000, 2047, 4095 -> group-6 rows 0, 250, 511, 1023 ->
        # inner chunks 0, 0, 1, 3. Chunk 2 holds nothing but fill — 0 for the
        # dense count, ``b""`` for the digest — and zarr omits an all-fill
        # inner chunk from the shard (the §1.5 absence sentinel), so a
        # reader of either array fetches three ranges, never four.
        assert present.tolist() == [True, True, False, True]
        rag = Path(f"{path}/6/h_tdigest/c/0").read_bytes()
        index = np.frombuffer(rag[-(16 * 4 + 4) : -4], dtype="<u8").reshape(4, 2)
        assert (index[:, 0] != 2**64 - 1).tolist() == [True, True, False, True]


class TestLayoutParity:
    """Both layouts of the same leaf: identical values, attrs and §5 hashes."""

    def test_whole_chunk_and_sharded_layouts_fold_to_identical_bytes(self, tmp_path, monkeypatch):
        sharded, folded = _write(tmp_path / "sharded")
        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        whole, _ = _write(tmp_path / "whole")
        for res in RESOLUTIONS:
            a = zarr.open_group(open_store(sharded), path=str(res), mode="r", zarr_format=3)
            b = zarr.open_group(open_store(whole), path=str(res), mode="r", zarr_format=3)
            for name in ("morton", "count"):
                np.testing.assert_array_equal(a[name][:], b[name][:])
            assert [bytes(p) for p in a["h_tdigest"][:]] == [bytes(p) for p in b["h_tdigest"][:]]
        # The §5 recipe hashes decoded values, so the stamps' records agree;
        # the one layout difference is the array metadata.
        stamps = [
            dict(zarr.open_group(open_store(p), mode="r", zarr_format=3).attrs[COMMIT_ATTR])
            for p in (sharded, whole)
        ]
        assert stamps[0]["content_hashes"] == stamps[1]["content_hashes"]
        assert stamps[0]["cells_with_data"] == stamps[1]["cells_with_data"] == 4
        assert _array_meta(whole, 6, "count")["codecs"][0]["name"] == "bytes"
        assert _array_meta(sharded, 6, "count")["codecs"][0]["name"] == "sharding_indexed"


class TestConsumersDeriveTheSameLayout:
    """The backfill gate and the Icechunk planner share the writer's grid."""

    def test_backfill_structure_is_the_written_structure(self, tmp_path, monkeypatch):
        from zagg.column_backfill import column_structure, stored_column_structure

        path, _ = _write(tmp_path)
        group = zarr.open_group(open_store(path), mode="r", zarr_format=3)
        want = column_structure(FIELDS, node_order=NODE, resolutions=RESOLUTIONS)
        assert stored_column_structure(group) == want
        assert want["6"]["count"]["codecs"][0]["name"] == "sharding_indexed"
        # A pre-#631 column (the fine group written whole) is drift the gate
        # sees: the backfill is the pass that re-lays it.
        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        legacy, _ = _write(tmp_path / "legacy")
        stored = stored_column_structure(
            zarr.open_group(open_store(legacy), mode="r", zarr_format=3)
        )
        assert stored != want
        assert {r for r in want if stored[r] != want[r]} == {"6"}

    def test_icechunk_plan_reads_the_fine_groups_shard_index(self, tmp_path):
        from zagg import icechunk_refs

        root = tmp_path / "store"
        path, _ = _write(root)
        cfg = _overview_config(FIELDS)
        leaf_grid = HealpixGrid(NODE, CELL, config=cfg)
        (rank,) = leaf_grid.block_index(morton_word(SHARD))
        rel = path[len(str(root)) + 1 :]
        plan = {
            e["path"]: e
            for e in icechunk_refs.object_ref_plan(
                column_grid(NODE, 6, cfg), rel, rank, str(root), store_kwargs={}
            )
        }
        count, rag = plan["count"], plan["h_tdigest"]
        assert count["sharded"] is True and count["chunk_grid"] == (4,)
        assert count["arr_offset"] == (rank * 4,)
        assert rag["sharded"] is True and rag["refs"] == 3
        assert [bool(loc) for loc in rag["locations"]] == [True, True, False, True]
        # The unsharded member below is the whole object, as before.
        (whole,) = [
            e
            for e in icechunk_refs.object_ref_plan(
                column_grid(NODE, 5, cfg), rel, rank, str(root), store_kwargs={}
            )
            if e["path"] == "count"
        ]
        assert whole["sharded"] is False

    def test_level_grids_shard_the_fine_column_level(self):
        from zagg import icechunk_refs
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {
                    "count": {"function": "len", "dtype": "int64", "fill_value": 0},
                    "h_tdigest": {
                        "kind": "ragged",
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "inner_shape": [2],
                        "dtype": "float32",
                        "fill_value": 0,
                    },
                },
            },
            output={"grid": {"type": "healpix", "parent_order": NODE, "child_order": CELL}},
        )
        grid = HealpixGrid(NODE, CELL, config=cfg, chunk_inner=3, sharded=True)
        manifest = {
            "pyramid": {"overview": {"fields": FIELDS}},
            "multiscales": [
                {
                    "datasets": [
                        {"order": NODE, "cells": [6, 5], "artifact": "column"},
                        {"order": 0, "cells": [4], "artifact": "overview"},
                    ]
                }
            ],
        }
        levels = icechunk_refs.level_grids(manifest, grid)
        assert levels[6]["grid"].sharded and levels[6]["grid"].chunk_order == 2
        assert levels[6]["grid"].cells_per_chunk == 256
        assert not levels[5]["grid"].sharded and levels[5]["grid"].cells_per_chunk == 256
        assert not levels[4]["grid"].sharded  # an overview level is untouched
        assert icechunk_refs.level_geometry(levels[6]["grid"]) == {
            "chunk_order": 2,
            "cell_order": 6,
        }
