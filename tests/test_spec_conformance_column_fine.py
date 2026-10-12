"""Spec §4.6 fine-tier layout conformance over the committed ``column_fine/`` fixture (issue #631).

The leaf-shaped suite (``tests/test_spec_conformance.py``) already runs over
this fixture's leaf; this module pins what is new about its COLUMN: group 9
(``4^5`` cells) is one ``sharding_indexed`` object per array on four 256-cell
inner chunks, groups 8 and below are one whole unsharded chunk each, and the
committed bytes decode by the §1.5 index recipe alone — no zarr on the read
side — to the values the generator recorded from its inputs. moczarr vendors
the same fixture for its parity gate.
"""

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest
from numcodecs import Zstd
from test_spec_conformance import (
    SENTINEL,
    SPEC_DATA,
    _decode_framing,
    _expected,
)
from test_spec_conformance import TestContentHashes as _Hashes  # aliased: not re-collected here

FIXTURE = "column_fine"
#: The §4.6 formula, spelled here independently of ``zagg.column_shards`` so
#: the fixture's recorded ``column.layout`` and this suite cannot both drift
#: with the implementation: a group of fewer than 4^5 cells is one unsharded
#: chunk; a wider one is sharded on ``min(4^5, n / 4)``-cell inner chunks.
INNER_ORDER = 5
#: FROZEN §5 combined digest of the column (the recipe-drift pin, §5): the
#: only committed multi-group record with a SHARDED member among its keys.
FROZEN_COMBINED_COLUMN_FINE = "1ba73dfe9b4523ab5e010dffd6d59d66e57bf2840ebcb6095bf33f70bfedb7f5"


def _rule(node: int, res: int) -> dict:
    n = 4 ** (res - node)
    if res - node < INNER_ORDER:
        return {"sharded": False, "chunk_cells": n}
    return {"sharded": True, "chunk_cells": min(4**INNER_ORDER, n // 4)}


def _column_dir(exp) -> Path:
    return (SPEC_DATA / FIXTURE / exp["leaf"]).parent / exp["column"]["object"]


def _meta(exp, res, name) -> dict:
    return json.loads((_column_dir(exp) / str(res) / name / "zarr.json").read_text())


def _index(blob: bytes, k: int) -> np.ndarray:
    """The §1.5 shard-index suffix: ``k`` ``(offset, nbytes)`` u64 pairs + crc32c."""
    return np.frombuffer(blob[-(16 * k + 4) : -4], dtype="<u8").reshape(k, 2)


def _inner_chunks(blob: bytes, k: int) -> dict:
    """``{ordinal: raw inner-chunk bytes}``; absent ordinals (sentinel) omitted."""
    out = {}
    for ordinal, (offset, nbytes) in enumerate(_index(blob, k)):
        if int(offset) == SENTINEL:
            assert int(nbytes) == SENTINEL
            continue
        out[ordinal] = blob[int(offset) : int(offset) + int(nbytes)]
    return out


class TestFineTierLayout:
    """Every group's ``zarr.json`` against the §4.6 rule and the recorded layout."""

    def test_geometry_is_the_smallest_declarable_fine_tier(self):
        exp = _expected(FIXTURE)
        assert (exp["shard_order"], exp["chunk_order"], exp["cell_order"]) == (4, 7, 10)
        assert exp["declared"] == {"overviews": 9}
        groups = exp["column"]["zagg_column"]["groups"]
        assert sorted(groups, key=int, reverse=True) == ["9", "8", "7", "6", "5", "4"]
        assert groups["9"]["n_cells"] == 4**5

    def test_recorded_layout_is_the_formula(self):
        exp = _expected(FIXTURE)
        layout = exp["column"]["layout"]
        assert layout == {str(r): _rule(exp["shard_order"], r) for r in range(9, 3, -1)}
        assert layout["9"] == {"sharded": True, "chunk_cells": 256}
        assert layout["8"] == {"sharded": False, "chunk_cells": 256}

    @pytest.mark.parametrize("name", ["morton", "count", "h_tdigest"])
    def test_fine_group_arrays_are_one_sharded_object(self, name):
        exp = _expected(FIXTURE)
        meta = _meta(exp, 9, name)
        assert meta["shape"] == [1024]
        # The outer chunk IS the array: one object per array per leaf.
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
        inner = [c["name"] for c in cfg["codecs"]]
        # The member's own chain: §1.3 for the ragged payload, bytes-only dense.
        assert inner == (["vlen-bytes", "zstd"] if name == "h_tdigest" else ["bytes"])
        objects = sorted(p.name for p in (_column_dir(exp) / "9" / name / "c").iterdir())
        assert objects == ["0"]

    @pytest.mark.parametrize("res", [8, 7, 6, 5, 4])
    @pytest.mark.parametrize("name", ["morton", "count", "h_tdigest"])
    def test_coarser_groups_are_one_whole_unsharded_chunk(self, res, name):
        exp = _expected(FIXTURE)
        meta = _meta(exp, res, name)
        n = 4 ** (res - exp["shard_order"])
        assert meta["shape"] == [n]
        assert meta["chunk_grid"]["configuration"]["chunk_shape"] == [n]
        assert meta["codecs"][0]["name"] != "sharding_indexed"
        assert sorted(p.name for p in (_column_dir(exp) / str(res) / name / "c").iterdir()) == ["0"]


class TestFineGroupDecodesBySpecText:
    """Group 9 from the committed object bytes, by §1.5 + §1.4 alone."""

    def test_inner_chunk_two_is_absent_from_the_data_arrays(self):
        exp = _expected(FIXTURE)
        for name in ("morton", "count", "h_tdigest"):
            blob = (_column_dir(exp) / "9" / name / "c" / "0").read_bytes()
            index = _index(blob, 4)
            present = (index[:, 0] != SENTINEL).tolist()
            # Leaf cells 0, 2, 1089, 3263 -> rows 0, 0, 272, 815 -> chunks 0,
            # 1, 3: ``count`` and ``h_tdigest`` hold fill alone in chunk 2 and
            # omit it (the §1.5 sentinel); the column's ``morton`` coordinate
            # is written for every cell, so all four of its chunks are present.
            assert present == ([True] * 4 if name == "morton" else [True, True, False, True]), name
            # Present chunks tile the object before the index, in order.
            offsets = [int(o) for o, p in zip(index[:, 0], present, strict=True) if p]
            assert offsets[0] == 0 and offsets == sorted(offsets)

    def test_digest_payloads_decode_to_the_recorded_rows(self):
        exp = _expected(FIXTURE)
        want = exp["column"]["groups"]["9"]["h_tdigest"]
        blob = (_column_dir(exp) / "9" / "h_tdigest" / "c" / "0").read_bytes()
        cells: list = [b""] * 1024
        for ordinal, raw in _inner_chunks(blob, 4).items():
            payloads = _decode_framing(bytes(Zstd().decode(raw)), 256)
            cells[ordinal * 256 : (ordinal + 1) * 256] = payloads
        assert len(cells) == 1024
        for payload, rows in zip(cells, want, strict=True):
            got = np.frombuffer(payload, "<f4").reshape(-1, 2)
            np.testing.assert_array_equal(got, np.array(rows, "<f4").reshape(-1, 2))
        # The absent chunk's cells are the ``b""`` fill: nothing recorded there.
        assert all(len(c) == 0 for c in cells[512:768])
        assert all(rows == [] for rows in want[512:768])

    @pytest.mark.parametrize(("name", "dtype", "fill"), [("count", "<i8", 0), ("morton", "<u8", 0)])
    def test_dense_arrays_decode_as_raw_little_endian_words(self, name, dtype, fill):
        exp = _expected(FIXTURE)
        want = exp["column"]["groups"]["9"][name]
        meta = _meta(exp, 9, name)
        width = np.dtype(dtype).itemsize
        values = np.full(1024, fill, dtype=dtype)
        blob = (_column_dir(exp) / "9" / name / "c" / "0").read_bytes()
        for ordinal, raw in _inner_chunks(blob, 4).items():
            assert len(raw) == 256 * width  # bytes-only inner chain: no compression
            values[ordinal * 256 : (ordinal + 1) * 256] = np.frombuffer(raw, dtype)
        assert meta["fill_value"] == fill
        assert [str(v) if name == "morton" else int(v) for v in values] == want
        if name == "morton":
            # The coordinate is dense (every chunk present) and ascends: the
            # node's order-9 descendants in canonical nested order (§1.5).
            assert all(b > a for a, b in zip(values.tolist(), values.tolist()[1:], strict=False))
        else:
            # Chunk 2 has no index entry (the §1.5 sentinel) and the generator
            # recorded fill for its cells, so filling an absent chunk with
            # ``fill_value`` is what reproduces the recorded values.
            assert _index(blob, 4)[2, 0] == SENTINEL
            assert set(want[512:768]) == {fill}

    def test_group_nine_is_the_from_leaves_fold_of_the_committed_leaf(self):
        exp = _expected(FIXTURE)
        # §4.6 parity for the sharded group too: counts by direct sum over
        # the leaf's own cells (4 per row), and every populated row's digest
        # is the §4.4 k-way fold of its leaf cells.
        import zarr
        from zarr.storage import LocalStore

        from zagg.sweep_overview import decode_digest, fold_digests

        leaf = zarr.open_group(
            LocalStore(str(SPEC_DATA / FIXTURE / exp["leaf"])), path=exp["group"], mode="r"
        )
        counts = np.asarray(leaf["count"][:]).reshape(1024, 4).sum(axis=1)
        assert counts.tolist() == exp["column"]["groups"]["9"]["count"]
        payloads = leaf["h_tdigest"][:]
        for j, rows in enumerate(exp["column"]["groups"]["9"]["h_tdigest"]):
            cell = [
                decode_digest(bytes(p), "float32", (2,))
                for p in payloads[j * 4 : (j + 1) * 4]
                if p is not None and len(p)
            ]
            want = fold_digests(cell, delta=exp["delta"], dtype="float32") if cell else b""
            assert np.frombuffer(want, "<f4").reshape(-1, 2).tolist() == rows


class TestFineColumnRecord:
    """Attrs, stamp and §5 hashes of the sharded column."""

    def test_attrs_and_stamp(self):
        exp = _expected(FIXTURE)
        attrs = json.loads((_column_dir(exp) / "zarr.json").read_text())["attributes"]
        assert attrs["role"] == "column"
        assert attrs["zagg_column"] == exp["column"]["zagg_column"]
        block = attrs["zagg_column"]
        assert block["order"] == 4 and block["source_cell_order"] == 10
        # The raw-fold boundary is order + 2 = 6: groups 9..6 fold from raw,
        # 5 and 4 flat from the boundary member (§4.6).
        assert {g: e["merges_from_raw"] for g, e in block["groups"].items()} == {
            "9": 1,
            "8": 1,
            "7": 1,
            "6": 1,
            "5": 2,
            "4": 2,
        }
        stamp = attrs["morton_hive_commit"]
        assert {k: v for k, v in stamp.items() if k != "written_at"} == exp["column"]["commit"]
        assert block["cells_with_data_order"] == 9 and stamp["cells_with_data"] == 3

    def test_hashes_recompute_over_the_sharded_arrays(self):
        exp = _expected(FIXTURE)
        arrays = _Hashes._hash_leaf(_column_dir(exp))
        combined = hashlib.sha256("\n".join(sorted(arrays.values())).encode()).hexdigest()
        assert arrays == exp["column"]["content_hashes"]["arrays"]
        assert combined == exp["column"]["content_hashes"]["combined"]
        assert combined == FROZEN_COMBINED_COLUMN_FINE
        node_dir = _column_dir(exp).parent
        record = json.loads((node_dir / "all.pyramid.stats.json").read_text())
        assert record["content_hashes"] == exp["column"]["content_hashes"]

    def test_groups_five_and_four_equal_the_column_fixtures(self):
        # The same observations fold to the same res-5 rows on both leaves,
        # so the deeper leaf and the fine-tier fold leave the coarse groups'
        # values unchanged: the two fixtures' groups 5 and 4 hash identically,
        # array for array. Groups 5 and 4 fold flat from the unsharded member
        # 6 and never read group 9, so this says nothing about the sharded
        # layout; tests/test_column_shards.py::TestLayoutParity pins that.
        fine, column = _expected(FIXTURE), _expected("column")
        for key in ("5/count", "5/h_tdigest", "5/morton", "4/count", "4/h_tdigest", "4/morton"):
            assert (
                fine["column"]["content_hashes"]["arrays"][key]
                == column["column"]["content_hashes"]["arrays"][key]
            ), key

    def test_manifest_declares_the_fine_tier(self):
        manifest = json.loads((SPEC_DATA / FIXTURE / "morton_hive.json").read_text())
        assert manifest["cell_order"] == 10 and manifest["shard_order"] == 4
        assert manifest["pyramid"]["overviews"][0] == {"node": 4, "cells": [9]}
        # The multiscales mirror names the column level — the sharded one.
        datasets = manifest["multiscales"][0]["datasets"]
        assert datasets[0] == {"order": 4, "cells": [9], "artifact": "column"}
