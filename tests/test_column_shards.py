"""The leaf column's shard layout (issue #631): fine groups on inner chunks.

Spec §4.6's layout rule — a column group narrower than ``4^5`` cells is one
unsharded chunk per array, a wider one is ONE ``sharding_indexed`` object
per array on power-of-four inner chunks of at most ``4^5`` cells and at
least four per object — as the writer lays it, as the backfill's structure
gate and the Icechunk ref planner derive it, as the cascade and
``pyramid_check`` read it, and with the parity contract that both layouts
of the same leaf fold to the same values.
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
from zagg.hive import COMMIT_ATTR, shard_leaf_path
from zagg.stats.tdigest import build_tdigest
from zagg.store import open_store
from zagg.sweep_overview import _overview_config, encode_digest

#: The smallest DECLARABLE geometry with a fine tier: a base at ``node + 5``
#: needs ``node >= 4`` for the fixed ladder to stay contiguous (the base is at
#: most ``2 * node + 1``, spec §4.4). Node 4 / cell 10 / ``overviews: [9]``:
#: groups {9, 8, 7, 6, 5, 4} — 9 is 4^5 cells (sharded, four 256-cell inner
#: chunks), 8 is 256 cells (one whole chunk), the rest narrower.
NODE, CELL, BASE = 4, 10, 9
RESOLUTIONS = [9, 8, 7, 6, 5, 4]
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
#: Leaf cells placed so group 9's rows land in inner chunks 0, 1 and 3 —
#: chunk 2 stays empty, the shard index's absence sentinel.
CELLS = {0: [1.0, 2.0], 1000: [10.0, 4.0], 2047: [7.0], 4095: [3.0, 8.0, 5.0]}
SHARD = "-31111"


def _slabs(cells=CELLS, n=LEAF_CELLS) -> dict:
    count = np.zeros(n, np.int64)
    digest = np.full(n, b"", dtype=object)
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        count[i] = len(obs)
        digest[i] = encode_digest(build_tdigest(obs, delta=DELTA), "float32")
    return {"count": count, "h_tdigest": digest}


def _node_prefix(root, dec: str) -> str:
    return shard_leaf_path(str(root), morton_word(dec)).rstrip("/").rsplit("/", 1)[0]


def _write(root, dec=SHARD, cells=CELLS) -> tuple[str, dict]:
    folded = fold_column(_slabs(cells), FIELDS, cell_order=CELL, resolutions=RESOLUTIONS)
    basename = write_column(
        str(root), morton_word(dec), folded, FIELDS, node_order=NODE, cell_order=CELL
    )
    return f"{_node_prefix(root, dec)}/{basename}", folded


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
            meta = _array_meta(path, BASE, name)
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
            assert sorted(p.name for p in Path(f"{path}/{BASE}/{name}/c").iterdir()) == ["0"]
            assert (
                stored_layout(meta)
                == expected_layout(NODE, BASE)
                == {
                    "sharded": True,
                    "chunk_cells": 256,
                }
            )
        # Inner codecs are the member's own: the leaves' bytes-only policy for
        # dense arrays, vlen-bytes + zstd for the ragged digest.
        inner = {
            n: [
                c["name"]
                for c in _array_meta(path, BASE, n)["codecs"][0]["configuration"]["codecs"]
            ]
            for n in ("morton", "count", "h_tdigest")
        }
        assert inner == {
            "morton": ["bytes"],
            "count": ["bytes"],
            "h_tdigest": ["vlen-bytes", "zstd"],
        }

    def test_coarser_groups_stay_one_whole_unsharded_chunk(self, tmp_path):
        path, _ = _write(tmp_path)
        for res in RESOLUTIONS[1:]:
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
        fine = zarr.open_group(store, path=str(BASE), mode="r", zarr_format=3)
        np.testing.assert_array_equal(fine["count"][768:1024], folded[BASE]["count"][768:1024])
        assert int(fine["count"][1023]) == 3 and int(fine["count"][0]) == 2

    def test_empty_inner_chunk_is_absent_from_the_shard_index(self, tmp_path):
        path, _ = _write(tmp_path)
        obj = Path(f"{path}/{BASE}/count/c/0").read_bytes()
        index = np.frombuffer(obj[-(16 * 4 + 4) : -4], dtype="<u8").reshape(4, 2)
        present = index[:, 0] != 2**64 - 1
        # Leaf cells 0, 1000, 2047, 4095 -> group-9 rows 0, 250, 511, 1023 ->
        # inner chunks 0, 0, 1, 3. Chunk 2 holds nothing but fill — 0 for the
        # dense count, ``b""`` for the digest — and zarr omits an all-fill
        # inner chunk from the shard (the §1.5 absence sentinel), so a
        # reader of either array fetches three ranges, never four.
        assert present.tolist() == [True, True, False, True]
        rag = Path(f"{path}/{BASE}/h_tdigest/c/0").read_bytes()
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
        assert _array_meta(whole, BASE, "count")["codecs"][0]["name"] == "bytes"
        assert _array_meta(sharded, BASE, "count")["codecs"][0]["name"] == "sharding_indexed"


class TestConsumersDeriveTheSameLayout:
    """The backfill gate and the Icechunk planner share the writer's grid."""

    def test_backfill_structure_is_the_written_structure(self, tmp_path, monkeypatch):
        from zagg.column_backfill import column_structure, stored_column_structure

        path, _ = _write(tmp_path)
        group = zarr.open_group(open_store(path), mode="r", zarr_format=3)
        want = column_structure(FIELDS, node_order=NODE, resolutions=RESOLUTIONS)
        assert stored_column_structure(group) == want
        assert want[str(BASE)]["count"]["codecs"][0]["name"] == "sharding_indexed"
        # A pre-#631 column (the fine group written whole) is drift the gate
        # sees: the backfill is the pass that re-lays it.
        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        legacy, _ = _write(tmp_path / "legacy")
        stored = stored_column_structure(
            zarr.open_group(open_store(legacy), mode="r", zarr_format=3)
        )
        assert stored != want
        assert {r for r in want if stored[r] != want[r]} == {str(BASE)}

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
                column_grid(NODE, BASE, cfg), rel, rank, str(root), store_kwargs={}
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
                column_grid(NODE, BASE - 1, cfg), rel, rank, str(root), store_kwargs={}
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
        grid = HealpixGrid(NODE, CELL, config=cfg, chunk_inner=7, sharded=True)
        manifest = {
            "pyramid": {"overview": {"fields": FIELDS}},
            "multiscales": [
                {
                    "datasets": [
                        {"order": NODE, "cells": [9, 8], "artifact": "column"},
                        {"order": 0, "cells": [5], "artifact": "overview"},
                    ]
                }
            ],
        }
        levels = icechunk_refs.level_grids(manifest, grid)
        assert levels[9]["grid"].sharded and levels[9]["grid"].chunk_order == 5
        assert levels[9]["grid"].cells_per_chunk == 256
        assert not levels[8]["grid"].sharded and levels[8]["grid"].cells_per_chunk == 256
        assert not levels[5]["grid"].sharded  # an overview level is untouched
        assert icechunk_refs.level_geometry(levels[9]["grid"]) == {
            "chunk_order": 5,
            "cell_order": 9,
        }


# ---------------------------------------------------------------------------
# Phase 2: the readers — the cascade's range reads, the backfill upgrade.
# ---------------------------------------------------------------------------

#: A cascade that READS a sharded member: the ladder's finest gather is at
#: ``base - 1``, so a sharded member there needs ``base >= node + 6``, and a
#: contiguous ladder ``base <= 2 * node + 1`` — node 5 is the smallest. Node
#: 5 / cell 12 / ``overviews: [11]``: the ``(4, [10])`` gather reads the leaf
#: columns' member 10 — 4^5 cells, four 256-cell inner chunks — by range;
#: member 11 (4,096 cells, four inner chunks of 1,024) is the finest group.
CASCADE_NODE, CASCADE_CELL, CASCADE_BASE = 5, 12, 11
CASCADE_LEAVES = ["111111", "111112"]


def _cascade_store(root, *, fields=FIELDS):
    from zagg.column import column_resolutions
    from zagg.hive import MANIFEST_NAME, _utcnow, build_root_coverage, write_root_coverage
    from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews

    root.mkdir(parents=True, exist_ok=True)
    levels = expand_overviews([CASCADE_BASE], parent_order=CASCADE_NODE)
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "001"},
        "semantic_hash": "t",
        "cell_order": CASCADE_CELL,
        "shard_order": CASCADE_NODE,
        "split_schedule": [1] * CASCADE_NODE,
        "path_grouping": 1,
        "pyramid": {
            "spec": PYRAMID_SPEC_V2,
            "overviews": levels,
            "overview": {
                "all_time": False,
                "fold_source": "cascade",
                "exact_levels": 1,
                "fields": fields,
            },
        },
        "generated_at": _utcnow(),
    }
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1))
    n = 4 ** (CASCADE_CELL - CASCADE_NODE)
    resolutions = column_resolutions(levels, CASCADE_NODE)
    for i, dec in enumerate(CASCADE_LEAVES):
        # Populated cells spread over the leaf so every inner chunk of the
        # fine members holds something, with one left empty (the sentinel).
        cells = {c: [float(i * 100 + c % 17), float(c % 5) + 1.0] for c in range(0, n, 257)}
        cells.pop(n // 2 + 1, None)
        folded = fold_column(
            _slabs(cells, n), fields, cell_order=CASCADE_CELL, resolutions=resolutions
        )
        write_column(
            str(root),
            morton_word(dec),
            folded,
            fields,
            node_order=CASCADE_NODE,
            cell_order=CASCADE_CELL,
            granule_count=1,
        )
    write_root_coverage(
        str(root), build_root_coverage([morton_word(d) for d in CASCADE_LEAVES], CASCADE_NODE)
    )
    return manifest


def _ladder_arrays(root) -> dict:
    out = {}
    for p in sorted(root.rglob("all.zarr"), key=str):
        g = zarr.open_group(open_store(str(p), read_only=True), mode="r", zarr_format=3)
        attrs = dict(g.attrs)
        if attrs.get("role") != "overview":
            continue
        r = attrs["zagg_overview"]["cell_order"]
        out[str(p.relative_to(root))] = {
            "morton": g[str(r)]["morton"][:],
            "count": g[str(r)]["count"][:],
            "h_tdigest": [bytes(x or b"") for x in g[str(r)]["h_tdigest"][:]],
        }
    return out


class TestCascadeReadsShardedMembers:
    """The staged sweep over sharded leaf columns is the sweep over whole ones."""

    def test_both_layouts_fold_to_the_same_ladder(self, tmp_path, monkeypatch):
        from zagg.sweep_stages import sweep_stage_pass

        fine = tmp_path / "fine"
        manifest = _cascade_store(fine)
        # The member the (4, [10]) gather reads IS sharded here.
        col = f"{_node_prefix(fine, CASCADE_LEAVES[0])}/all.pyramid.zarr"
        assert stored_layout(_array_meta(col, CASCADE_BASE - 1, "h_tdigest")) == {
            "sharded": True,
            "chunk_cells": 256,
        }
        assert stored_layout(_array_meta(col, CASCADE_BASE, "h_tdigest")) == {
            "sharded": True,
            "chunk_cells": 1024,
        }
        by_shard = {d: {None} for d in CASCADE_LEAVES}
        summary = sweep_stage_pass(
            str(fine), manifest, by_shard, run_id="A", tuple_width=3, scope=None
        )
        assert sum(row["failed"] for row in summary["stages"]) == 0
        assert sum(row["written"] for row in summary["stages"]) > 0
        fine_ladder = _ladder_arrays(fine)
        assert any(rel.startswith("1/1/1/1/1/all") for rel in fine_ladder), sorted(fine_ladder)

        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        whole = tmp_path / "whole"
        manifest = _cascade_store(whole)
        whole_col = f"{_node_prefix(whole, CASCADE_LEAVES[0])}/all.pyramid.zarr"
        assert stored_layout(_array_meta(whole_col, CASCADE_BASE - 1, "count")) == {
            "sharded": False,
            "chunk_cells": 1024,
        }
        sweep_stage_pass(str(whole), manifest, by_shard, run_id="A", tuple_width=3, scope=None)
        whole_ladder = _ladder_arrays(whole)
        assert set(fine_ladder) == set(whole_ladder)
        for rel in fine_ladder:
            np.testing.assert_array_equal(fine_ladder[rel]["morton"], whole_ladder[rel]["morton"])
            np.testing.assert_array_equal(fine_ladder[rel]["count"], whole_ladder[rel]["count"])
            assert fine_ladder[rel]["h_tdigest"] == whole_ladder[rel]["h_tdigest"]

    def test_gather_level_is_the_sharded_members_bytes(self, tmp_path):
        from zagg.sweep_stages import sweep_stage_pass

        manifest = _cascade_store(tmp_path)
        sweep_stage_pass(
            str(tmp_path),
            manifest,
            {d: {None} for d in CASCADE_LEAVES},
            run_id="A",
            tuple_width=3,
            scope=None,
        )
        # The (2, [8]) node above leaf 1111: its cells are the leaf column's
        # member 8, assigned — read through the shard index, byte for byte.
        node = zarr.open_group(
            open_store(f"{tmp_path}/1/1/1/all.zarr", read_only=True), path="8", mode="r"
        )
        column = zarr.open_group(
            open_store(
                f"{_node_prefix(tmp_path, CASCADE_LEAVES[0])}/all.pyramid.zarr", read_only=True
            ),
            path="8",
            mode="r",
        )
        n = 4 ** (8 - CASCADE_NODE)
        # Leaf 1111 is the first child of node 111: rows [0, n) of the gather.
        np.testing.assert_array_equal(node["count"][0:n], column["count"][:])
        assert [bytes(p or b"") for p in node["h_tdigest"][0:n]] == [
            bytes(p or b"") for p in column["h_tdigest"][:]
        ]


# -- the backfill: a whole-chunk fine column is re-laid, values unchanged.


def _leaf_cfg():
    from zagg.config import PipelineConfig

    return PipelineConfig(
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
        }
    )


def _committed_leaf(root, dec=SHARD, cells=CELLS) -> dict:
    """One committed node-4 / cell-10 leaf holding ``cells``; returns its slabs."""
    from mortie import generate_morton_children

    from zagg.hive import stamp_commit

    word = morton_word(dec)
    grid = HealpixGrid(NODE, CELL, config=_leaf_cfg())
    store = open_store(shard_leaf_path(str(root), word))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(CELL), mode="r+", zarr_format=3)
    group["morton"][:] = np.asarray(generate_morton_children(word, CELL), dtype=np.uint64)
    slabs = _slabs(cells)
    for name, slab in slabs.items():
        group[name][:] = slab
    stamp_commit(store, cells_with_data=len(cells), granule_count=1)
    return slabs


def _backfill_manifest() -> dict:
    from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews

    return {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "001"},
        "cell_order": CELL,
        "shard_order": NODE,
        "split_schedule": [1] * NODE,
        "path_grouping": 1,
        "pyramid": {
            "spec": PYRAMID_SPEC_V2,
            "overviews": expand_overviews([BASE], parent_order=NODE),
            "overview": {
                "all_time": False,
                "fold_source": "cascade",
                "exact_levels": 1,
                "fields": FIELDS,
            },
        },
    }


class TestBackfillRelaysWholeChunkColumns:
    """The /1 -> /2 backfill is the upgrade pass for a pre-#631 fine column."""

    def test_whole_chunk_fine_column_is_structure_drift_and_is_relaid(self, tmp_path, monkeypatch):
        from zagg.column import COLUMN_ATTR
        from zagg.column_backfill import (
            _column_state,
            _leaf_stamp,
            backfill_columns,
            column_is_current,
        )

        _committed_leaf(tmp_path)
        manifest = _backfill_manifest()
        word = morton_word(SHARD)
        # A pre-#631 worker's column: the fine group one whole chunk.
        real = column_shards.inner_chunk_order
        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        legacy_path, _ = _write(tmp_path)
        legacy_hashes = dict(
            zarr.open_group(open_store(legacy_path), mode="r").attrs[COMMIT_ATTR]["content_hashes"]
        )
        monkeypatch.setattr(column_shards, "inner_chunk_order", real)

        leaf_stamp = _leaf_stamp(str(tmp_path), word, None, {})
        stamp, attrs, structure = _column_state(str(tmp_path), word, None, {})
        current, reason = column_is_current(
            leaf_stamp,
            stamp,
            attrs.get(COLUMN_ATTR),
            structure,
            node_order=NODE,
            cell_order=CELL,
            resolutions=RESOLUTIONS,
            fields=FIELDS,
        )
        assert (current, reason) == (False, "structure-drift")

        counts = backfill_columns(str(tmp_path), manifest, {SHARD: {None}})
        assert counts["written"] == 1 and counts["failed"] == 0, counts
        relaid = zarr.open_group(open_store(legacy_path), mode="r")
        assert relaid.attrs[COMMIT_ATTR]["content_hashes"] == legacy_hashes  # same values
        assert stored_layout(_array_meta(legacy_path, BASE, "h_tdigest")) == {
            "sharded": True,
            "chunk_cells": 256,
        }
        # ...and the re-laid column is current: a second pass writes nothing.
        again = backfill_columns(str(tmp_path), manifest, {SHARD: {None}})
        assert again["current"] == 1 and again["written"] == 0, again


class TestPyramidCheckOnAFineTier:
    """``pyramid_check`` passes a fine-tier store, counts included, and holds the layout."""

    LEAVES = [SHARD, "-31112"]

    def _store(self, root) -> dict:
        from zagg.column_backfill import backfill_columns
        from zagg.hive import MANIFEST_NAME, build_root_coverage, write_root_coverage
        from zagg.sweep_stages import run_stage_sweep

        for i, dec in enumerate(self.LEAVES):
            cells = {
                c + i: [float(v + i) for v in obs] for c, obs in CELLS.items() if c + i < LEAF_CELLS
            }
            _committed_leaf(root, dec, cells)
        manifest = _backfill_manifest()
        (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1))
        write_root_coverage(
            str(root), build_root_coverage([morton_word(d) for d in self.LEAVES], NODE)
        )
        counts = backfill_columns(str(root), manifest, {d: {None} for d in self.LEAVES})
        assert counts["written"] == len(self.LEAVES) and counts["failed"] == 0, counts
        summary = run_stage_sweep(str(root), [[morton_word(d), None] for d in self.LEAVES])
        assert sum(row["failed"] for row in summary["stages"]) == 0, summary
        return manifest

    def test_full_checklist_passes_and_the_layout_leg_holds(self, tmp_path, monkeypatch):
        from zagg.column_backfill import backfill_columns
        from zagg.pyramid_check import format_report, validate_pyramid

        manifest = self._store(tmp_path)
        col = f"{_node_prefix(tmp_path, SHARD)}/all.pyramid.zarr"
        assert stored_layout(_array_meta(col, BASE, "count"))["sharded"] is True
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is True, format_report(report)
        assert report["checks"]["counts"]["status"] == "pass"
        assert not any("layout" in w for w in report.get("warnings") or [])

        # Re-lay one leaf's column whole (a pre-#631 worker's bytes): the
        # values are the same, so only the §4.6 layout leg can see it — and
        # it must, by name.
        monkeypatch.setattr(column_shards, "inner_chunk_order", lambda node, res: None)
        relaid = backfill_columns(str(tmp_path), manifest, {SHARD: {None}}, force=True)
        assert relaid["written"] == 1, relaid
        assert stored_layout(_array_meta(col, BASE, "count"))["sharded"] is False
        monkeypatch.undo()  # the CHECKER runs under the real rule
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False
        assert report["checks"]["readback"]["status"] == "fail"
        mismatches = report["checks"]["readback"]["mismatches"]
        assert {m.split(": ")[0] for m in mismatches} == {
            f"{SHARD}[{BASE}]/{name}" for name in ("morton", "count", "h_tdigest")
        }, format_report(report)
        why = (
            "layout is unsharded, 1024 cells per chunk; §4.6 requires sharded, 256 cells per chunk"
        )
        assert all(why in m for m in mismatches), mismatches
        assert report["checks"]["counts"]["status"] == "pass"  # the values never moved
