"""Leaf-worker pyramid columns (issue #383): fold core.

Phase 1 covers the pure fold core: the resolution set a ``zagg-pyramid/2``
declaration puts in a leaf-node column, the staged-sink adapter, and the
per-resolution folds — with the headline byte-parity check against the
sweep's own from-leaves fold (``sweep_overviews``) over the same committed
leaf, which is the issue #383 acceptance contract.
"""

import json
import pathlib

import numpy as np
import obstore
import pytest
import zarr

from zagg.column import (
    column_resolutions,
    fold_column,
    generation_key,
    leaf_slabs,
    stamped_generation_key,
)
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, shard_leaf_path, stamp_commit
from zagg.stats.tdigest import build_tdigest, merge_tdigests_kway
from zagg.store import open_object_store, open_store
from zagg.sweep_overview import PYRAMID_SPEC, decode_digest, encode_digest, fold_dense

SHARD_ORDER = 2
CELL_ORDER = 4
LEAF_CELLS = 4 ** (CELL_ORDER - SHARD_ORDER)
DELTA = 64

#: The /2 declaration's per-field map (same shape under both revisions).
FIELDS = {
    "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
    "h_min": {"class": "exact", "method": "min", "dtype": "float32", "fill_value": "NaN"},
    "h_tdigest": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": DELTA,
    },
}


def _leaf_cfg():
    from zagg.config import PipelineConfig

    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_min": {"function": "min", "dtype": "float32"},
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


def _cell_slabs(cells: dict) -> dict:
    """The leaf's resident per-cell slabs (``{leaf row: observations}``)."""
    count = np.zeros(LEAF_CELLS, np.int32)
    h_min = np.full(LEAF_CELLS, np.nan, np.float32)
    digest = np.full(LEAF_CELLS, b"", dtype=object)
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        count[i] = len(obs)
        h_min[i] = obs.min()
        digest[i] = encode_digest(build_tdigest(obs, delta=DELTA), "float32")
    return {"count": count, "h_min": h_min, "h_tdigest": digest}


def _make_leaf(root, decimal, cells):
    """One committed leaf on disk; returns its resident slabs (fold inputs)."""
    from mortie import generate_morton_children

    from zagg.grids.healpix import HealpixGrid

    grid = HealpixGrid(SHARD_ORDER, CELL_ORDER, config=_leaf_cfg())
    word = morton_word(decimal)
    store = open_store(shard_leaf_path(str(root), word))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(CELL_ORDER), mode="r+", zarr_format=3)
    group["morton"][:] = np.asarray(generate_morton_children(word, CELL_ORDER), dtype=np.uint64)
    slabs = _cell_slabs(cells)
    for name, slab in slabs.items():
        group[name][:] = slab
    stamp_commit(store, cells_with_data=len(cells), granule_count=1)
    return slabs


class TestGenerationKey:
    """The staged sweep's skip-gate key as a PURE function (issues #417/#433).
    The sweep's own tests pin the gate's behavior; these pin the grammar
    ``docs/specification.md`` §4.5 declares normative — term order, the
    non-block sentinel, and the additive MUST ("absent reads as the zero,
    never as a wildcard") per term and for both at once."""

    BLOCK = {
        "n_leaves": 2,
        "max_leaf_timestamp": "2026-08-11T00:00:00+00:00",
        "run_ids": ["stage-b", "stage-a"],
        "granule_count": 7,
    }

    def test_term_order_and_normalization(self):
        # The third term is a sorted SET: recorded order is not a property.
        ts, ids = "2026-08-11T00:00:00+00:00", ("stage-a", "stage-b")
        assert generation_key(self.BLOCK) == (2, ts, ids, 7)

    def test_a_non_block_matches_no_generation(self):
        for value in (None, [], "generation", 0):
            assert generation_key(value) == ()

    @pytest.mark.parametrize("absent", ["run_ids", "granule_count"])
    def test_an_absent_term_reads_as_its_zero_not_a_wildcard(self, absent):
        older = {k: v for k, v in self.BLOCK.items() if k != absent}
        assert generation_key(older) != generation_key(self.BLOCK)
        assert generation_key(older) == generation_key({**older, absent: None})

    def test_a_pre_417_block_carries_neither_term(self):
        older = {k: self.BLOCK[k] for k in ("n_leaves", "max_leaf_timestamp")}
        assert generation_key(older) == (2, self.BLOCK["max_leaf_timestamp"], (), 0)

    def test_the_leaf_arm_is_the_identity_plus_the_stamp_terms(self):
        """Issue #433's arm — a fleet-written leaf column has no ``generation``
        block and a stamp with a count but no ``run_id`` (§4.6), so the granule
        count is the only term a same-second rewrite can move."""
        stamp = {"written_at": "2026-08-11T00:00:00+00:00", "granule_count": 3}
        assert stamped_generation_key(None, stamp) == (1, stamp["written_at"], (), 3)
        appended = stamped_generation_key(None, {**stamp, "granule_count": 4})
        assert appended != stamped_generation_key(None, stamp)

    def test_the_stage_arm_unions_the_id_and_takes_the_stamp_count(self):
        stamp = {"written_at": "…", "run_id": "stage-c", "granule_count": 7}
        assert stamped_generation_key(self.BLOCK, stamp) == (
            2,
            self.BLOCK["max_leaf_timestamp"],
            ("stage-a", "stage-b", "stage-c"),
            7,
        )
        # A pre-#433 stage column reads the stamp's count, never falls back to 0.
        older = {k: v for k, v in self.BLOCK.items() if k != "granule_count"}
        assert stamped_generation_key(older, stamp)[3] == 7


class TestColumnResolutions:
    def test_default_schedule_reaches_node_and_members(self):
        from zagg.pyramid import default_overviews

        # 19/13/9 reference geometry under the fixed every-order ladder
        # (espg ruling, the PR #389 thread): base (9,[13]) plus the ladder's
        # within-footprint members (8,[12]) (7,[11]) (6,[10]) (5,[9]) — every
        # coarser rung's cells fall below the node — and the node-order member.
        levels = default_overviews(9, 13, child_order=19)
        assert column_resolutions(levels, 9) == [13, 12, 11, 10, 9]

    def test_small_geometry_default(self):
        from zagg.pyramid import default_overviews

        # (2,[3]) + ladder (1,[2]) (0,[1]): within-footprint members {3, 2}.
        levels = default_overviews(SHARD_ORDER, 3, child_order=CELL_ORDER)
        assert column_resolutions(levels, SHARD_ORDER) == [3, 2]

    def test_lone_base_entry_adds_the_node_member(self):
        assert column_resolutions([{"node": 2, "cells": [3]}], 2) == [3, 2]

    def test_spelled_node_member_dedupes(self):
        assert column_resolutions([{"node": 2, "cells": [3, 2]}], 2) == [3, 2]

    def test_coarser_members_are_not_column_groups(self):
        # (0,[1])'s resolution is coarser than the node: the leaf's
        # contribution to it is the node-order member itself.
        levels = [{"node": 2, "cells": [3]}, {"node": 0, "cells": [1]}]
        assert column_resolutions(levels, 2) == [3, 2]

    def test_no_leaf_node_entry_means_no_column(self):
        levels = [{"node": 1, "cells": [3]}, {"node": 0, "cells": [1]}]
        assert column_resolutions(levels, 2) == []
        assert column_resolutions([], 2) == []

    def test_all_none_fields_mean_no_column_at_all(self):
        # The gate's third arm (§4.6): leaf-node levels but nothing composable
        # writes NO artifact — not a morton-only column.
        from dataclasses import replace

        from zagg.column import leaf_column_plan
        from zagg.grids import HealpixGrid

        cfg = _leaf_cfg()
        cfg = replace(
            cfg,
            aggregation={**cfg.aggregation, "variables": {"h_mean": {"function": "mean"}}},
            output={**(cfg.output or {}), "pyramid": {"overviews": 3}},
        )
        grid = HealpixGrid(SHARD_ORDER, CELL_ORDER, config=cfg)
        assert column_resolutions([{"node": SHARD_ORDER, "cells": [3]}], SHARD_ORDER) == [
            3,
            SHARD_ORDER,
        ]
        assert leaf_column_plan(cfg, grid) is None


class TestLeafSlabs:
    def test_staged_refs_pass_through(self):
        slabs = _cell_slabs({0: [1.0, 2.0]})
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        out = leaf_slabs(staged, FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)
        assert out["count"] is slabs["count"] and out["h_tdigest"] is slabs["h_tdigest"]

    def test_absent_fields_synthesize_fill(self):
        out = leaf_slabs({}, FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)
        np.testing.assert_array_equal(out["count"], np.zeros(LEAF_CELLS, np.int32))
        assert np.isnan(out["h_min"]).all() and out["h_min"].dtype == np.float32
        assert all(p == b"" for p in out["h_tdigest"])

    def test_wrong_extent_refuses(self):
        staged = {f"{CELL_ORDER}/count": np.zeros(3, np.int32)}
        with pytest.raises(ValueError, match="cell extent"):
            leaf_slabs(staged, FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)


class TestNoneClassFields:
    """D24 ``class: "none"`` entries never become column content (option A)."""

    #: What `zagg.pyramid.declared_fields` records for a non-composable field:
    #: the class alone. `h_vec` stands for a vector field (staged dense at
    #: (n_cells, k)), `h_chunks` for a chunk-resolution companion (written per
    #: chunk-block, so never in the leaf's cell-slab sink).
    DECL = dict(FIELDS, h_vec={"class": "none"}, h_chunks={"class": "none"})

    def test_staged_vector_field_is_not_a_grid_disagreement(self):
        # Failure mode (1): the (n_cells, k) slab tripping the extent refusal
        # and killing the whole column fold, blaming a sink that is correct.
        slabs = _cell_slabs({0: [1.0, 2.0]})
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        staged[f"{CELL_ORDER}/h_vec"] = np.zeros((LEAF_CELLS, 3), np.float32)
        out = leaf_slabs(staged, self.DECL, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)
        assert set(out) == set(FIELDS)

    def test_absent_companion_is_not_an_empty_ragged_group(self):
        # Failure mode (2): synthesizing b"" fill for a field that exists ONLY
        # at native resolution, then writing it as an all-empty column group.
        slabs = _cell_slabs({0: [1.0, 2.0]})
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        out = leaf_slabs(staged, self.DECL, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)
        assert "h_chunks" not in out
        folded = fold_column(out, self.DECL, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        assert all(set(group) == set(FIELDS) for group in folded.values())

    def test_filtered_fold_matches_the_pre_filtered_one(self):
        slabs = _cell_slabs({0: [1.0, 2.0], 5: [10.0, 4.0]})
        a = fold_column(slabs, self.DECL, cell_order=CELL_ORDER, resolutions=[3])
        b = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3])
        np.testing.assert_array_equal(a[3]["count"], b[3]["count"])
        assert [bytes(p) for p in a[3]["h_tdigest"]] == [bytes(p) for p in b[3]["h_tdigest"]]


class TestFoldColumn:
    CELLS = {0: [1.0, 2.0], 5: [10.0, 4.0], 6: [7.0], 15: [3.0, 8.0, 5.0]}

    def test_exact_fields_match_the_sweep_kernel(self):
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, 2])
        for res in (3, 2):
            factor = 4 ** (CELL_ORDER - res)
            np.testing.assert_array_equal(
                folded[res]["count"], fold_dense(slabs["count"], factor, "sum", 0)
            )
            np.testing.assert_array_equal(
                folded[res]["h_min"], fold_dense(slabs["h_min"], factor, "min", "NaN")
            )

    def test_exact_fields_match_direct_aggregation(self):
        # The D24 exact contract one hop further: byte-equal to aggregating
        # the observations directly at the coarser order (nan-skipping).
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3])
        np.testing.assert_array_equal(folded[3]["count"][[0, 1, 3]], [2, 3, 3])
        assert folded[3]["h_min"][0] == np.float32(1.0)
        assert folded[3]["h_min"][1] == np.float32(4.0)
        assert np.isnan(folded[3]["h_min"][2])

    def test_digests_are_the_kway_fold_of_resident_cells(self):
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3])
        # Row 1 pools cells 5 and 6 — a genuine multi-input k-way merge.
        oracle = merge_tdigests_kway(
            [decode_digest(slabs["h_tdigest"][i], "float32") for i in (5, 6)], delta=DELTA
        )
        assert bytes(folded[3]["h_tdigest"][1]) == encode_digest(oracle, "float32")
        # Row 0 has one contributor: passes through un-recompressed.
        assert bytes(folded[3]["h_tdigest"][0]) == bytes(slabs["h_tdigest"][0])
        # Empty rows keep the ragged fill.
        assert folded[3]["h_tdigest"][2] == b""

    def test_node_member_is_the_whole_footprint_aggregate(self):
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[SHARD_ORDER])
        group = folded[SHARD_ORDER]
        assert group["count"].shape == (1,) and group["count"][0] == 8
        assert group["h_min"][0] == np.float32(1.0)
        oracle = merge_tdigests_kway(
            [decode_digest(slabs["h_tdigest"][i], "float32") for i in sorted(self.CELLS)],
            delta=DELTA,
        )
        assert bytes(group["h_tdigest"][0]) == encode_digest(oracle, "float32")

    def test_declared_delta_bounds_an_over_budget_merge(self):
        # Every CELLS digest is far under the δ budget, so no merge above ever
        # re-compresses and `delta` is unobservable. These two cells share
        # order-3 row 1 and their merge IS over budget, so the payload depends
        # on the declared δ — one of the three values that decide the bytes.
        slabs = _cell_slabs({4: np.arange(200.0), 5: np.arange(200.0, 400.0)})
        parts = [decode_digest(slabs["h_tdigest"][i], "float32") for i in (4, 5)]
        merged = merge_tdigests_kway(parts, delta=DELTA)
        assert len(merged) < sum(len(p) for p in parts)  # re-compression bit
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3])
        assert bytes(folded[3]["h_tdigest"][1]) == encode_digest(merged, "float32")
        # ... and a different δ is different bytes: the DECLARED one is used.
        other = merge_tdigests_kway(parts, delta=DELTA // 4)
        assert bytes(folded[3]["h_tdigest"][1]) != encode_digest(other, "float32")

    def test_resolution_finer_than_the_cells_refuses_by_name(self):
        # 4^(cell_order - res) would be fractional; the downstream divisibility
        # guards read 16 % 0.25 as clean and fail opaquely inside numpy.
        slabs = _cell_slabs(self.CELLS)
        with pytest.raises(ValueError, match="FINER than the leaf's cell order"):
            fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[CELL_ORDER + 1])

    def test_indivisible_ragged_slab_refuses_by_name(self):
        # The ragged guard, reached with only the approximate field declared
        # (the exact class gets the identically-worded refusal from fold_dense).
        ragged = {"h_tdigest": FIELDS["h_tdigest"]}
        slabs = {"h_tdigest": _cell_slabs(self.CELLS)["h_tdigest"][:6]}
        with pytest.raises(ValueError, match="cannot fold 6 cells 4-to-one for 'h_tdigest'"):
            fold_column(slabs, ragged, cell_order=CELL_ORDER, resolutions=[3])

    def test_fold_is_deterministic(self):
        slabs = _cell_slabs(self.CELLS)
        a = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, 2])
        b = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, 2])
        for res in (3, 2):
            for name in FIELDS:
                if FIELDS[name]["class"] == "exact":
                    np.testing.assert_array_equal(a[res][name], b[res][name])
                else:
                    assert [bytes(p) for p in a[res][name]] == [bytes(p) for p in b[res][name]]


#: The same declaration with the digest field located (ruling 4, issue #410).
LOCATED_FIELDS = {**FIELDS, "h_tdigest": {**FIELDS["h_tdigest"], "location": "leaf_id"}}
#: ONE field carrying BOTH channels — the arity the ``channels=`` map exists for
#: (espg ruling of 2026-08-17: temporal is per-centroid at every level too).
BOTH_CHANNEL_FIELDS = {
    **LOCATED_FIELDS,
    "h_tdigest": {**LOCATED_FIELDS["h_tdigest"], "temporal": "per-centroid"},
}

#: ``{field meta key: (kernel kwarg, sibling suffix)}`` for the harness below.
_CHANNELS = {"location": ("locations", "locations"), "temporal": ("temporal", "times")}


def _located_cell_slabs(cells: dict, fields: dict = LOCATED_FIELDS) -> tuple[dict, dict]:
    """``_cell_slabs`` plus every companion sibling ``fields`` declares.

    Both are built in ONE ``build_tdigest`` call per cell, so the siblings
    describe the one partition their payload does. Returns the slabs and the
    per-channel truth: ``{kernel kwarg: {cell: per-observation words}}``.
    """
    from conftest import TOC_BASE, point_words, toc_words

    declared = [key for key in _CHANNELS if fields["h_tdigest"].get(key) is not None]
    slabs = _cell_slabs(cells)
    sibs = {key: np.full(LEAF_CELLS, b"", dtype=object) for key in declared}
    truth: dict = {_CHANNELS[key][0]: {} for key in declared}
    for i, obs in cells.items():
        kw = {}
        if "location" in declared:
            kw["locations"] = point_words(len(obs), seed=2000 + i)
        if "temporal" in declared:
            # A distinct instant run per cell, so no two envelopes are confusable.
            when = np.datetime64(TOC_BASE, "ns") + np.timedelta64(600 * (i + 1), "s")
            kw["temporal"] = toc_words(len(obs), base=str(when))
        d, *words = build_tdigest(np.asarray(obs, dtype=np.float64), DELTA, **kw)
        slabs["h_tdigest"][i] = encode_digest(d, "float32")
        for key in declared:
            kwarg = _CHANNELS[key][0]
            sibs[key][i] = encode_digest(words[list(kw).index(kwarg)], "uint64")
            truth[kwarg][i] = kw[kwarg]
    for key in declared:
        slabs[f"h_tdigest_{_CHANNELS[key][1]}"] = sibs[key]
    return slabs, truth


class TestLocatedColumn:
    """The §4.6 column artifact carries the §9 channel (review finding).

    ``_overview_config`` emits the sibling array and the payload's §1.2 binding
    for every located field, so a fold that returned payload slabs only
    committed populated payload rows against ``b""`` sibling rows — §1.1's
    row-alignment MUST broken under a §9 declaration, and hashed into the §5
    sidecar as content.
    """

    CELLS = {0: [1.0, 2.0], 5: [10.0, 4.0], 6: [7.0], 15: [3.0, 8.0, 5.0]}

    def test_leaf_slabs_picks_up_the_sibling(self):
        slabs, _truth = _located_cell_slabs(self.CELLS)
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        out = leaf_slabs(staged, LOCATED_FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)
        assert out["h_tdigest_locations"] is slabs["h_tdigest_locations"]

    def test_leaf_slabs_checks_the_sibling_extent(self):
        slabs, _truth = _located_cell_slabs(self.CELLS)
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        staged[f"{CELL_ORDER}/h_tdigest_locations"] = np.full(3, b"", dtype=object)
        with pytest.raises(ValueError, match="cell extent"):
            leaf_slabs(staged, LOCATED_FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS)

    def test_the_fold_returns_the_sibling_row_aligned(self):
        from mortie import validate_morton

        slabs, _truth = _located_cell_slabs(self.CELLS)
        folded = fold_column(slabs, LOCATED_FIELDS, cell_order=CELL_ORDER, resolutions=[3, 2])
        for res in (3, 2):
            group = folded[res]
            assert "h_tdigest_locations" in group
            for payload, raw in zip(group["h_tdigest"], group["h_tdigest_locations"], strict=True):
                words = decode_digest(raw, "uint64", ())
                assert words.shape == (decode_digest(payload, "float32").shape[0],)
                if len(words):
                    validate_morton(words)
        # Row 1 of order 3 pools cells 5 and 6: the pair comes from ONE k-way
        # call, so it is byte-equal to the located merge of the two members.
        oracle, oracle_words = merge_tdigests_kway(
            [decode_digest(slabs["h_tdigest"][i], "float32") for i in (5, 6)],
            delta=DELTA,
            locations=[
                decode_digest(slabs["h_tdigest_locations"][i], "uint64", ()) for i in (5, 6)
            ],
        )
        assert bytes(folded[3]["h_tdigest"][1]) == encode_digest(oracle, "float32")
        assert bytes(folded[3]["h_tdigest_locations"][1]) == encode_digest(oracle_words, "uint64")
        # An empty row keeps the ragged fill on BOTH arrays.
        assert folded[3]["h_tdigest"][2] == b"" and folded[3]["h_tdigest_locations"][2] == b""

    def test_a_missing_sibling_slab_refuses_by_name(self):
        slabs, _truth = _located_cell_slabs(self.CELLS)
        del slabs["h_tdigest_locations"]
        with pytest.raises(ValueError, match="declares a locations channel"):
            fold_column(slabs, LOCATED_FIELDS, cell_order=CELL_ORDER, resolutions=[3])

    def test_a_short_sibling_row_refuses_rather_than_writing_it(self):
        # The failure this whole path guards: a populated payload row against an
        # empty sibling row. ``fold_digests``' length check makes it loud.
        slabs, _truth = _located_cell_slabs(self.CELLS)
        slabs["h_tdigest_locations"][0] = b""
        with pytest.raises(ValueError, match="row-aligned with its payload"):
            fold_column(slabs, LOCATED_FIELDS, cell_order=CELL_ORDER, resolutions=[3])

    def test_the_written_column_carries_populated_words(self, tmp_path):
        from zagg.column import write_column
        from zagg.grids.base import located_declaration

        slabs, _truth = _located_cell_slabs(self.CELLS)
        folded = fold_column(
            slabs, LOCATED_FIELDS, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER]
        )
        basename = write_column(
            str(tmp_path),
            morton_word("-311"),
            folded,
            LOCATED_FIELDS,
            node_order=SHARD_ORDER,
            cell_order=CELL_ORDER,
            granule_count=3,
        )
        store = open_store(f"{tmp_path}/-3/1/1/{basename}")
        for res in (3, SHARD_ORDER):
            group = zarr.open_group(store, path=str(res), mode="r", zarr_format=3)
            stored = [bytes(p) for p in group["h_tdigest_locations"][:]]
            assert stored == [bytes(p) for p in folded[res]["h_tdigest_locations"]]
            # ... and it is not the all-empty array a payload-only fold left.
            assert any(stored), "the §9 sibling is populated, not a bound empty array"
            assert located_declaration(dict(group["h_tdigest_locations"].attrs)) is not None
            assert dict(group["h_tdigest"].attrs)["ragged"]["locations"] == "h_tdigest_locations"


class TestBothChannelsColumn:
    """One field, TWO channels, through ``fold_column`` (issue #410).

    Every test above declares a single channel, where the ``channels=`` map has
    one key and the fold's slot order cannot be observed. These declare both, so
    each sibling is pinned against its OWN oracle — a swapped pair is a
    well-formed word in either grammar and raises nowhere on its own.
    """

    CELLS = TestLocatedColumn.CELLS

    def test_the_fold_returns_both_siblings_against_their_own_oracles(self):
        slabs, _truth = _located_cell_slabs(self.CELLS, BOTH_CHANNEL_FIELDS)
        folded = fold_column(slabs, BOTH_CHANNEL_FIELDS, cell_order=CELL_ORDER, resolutions=[3, 2])
        for res in (3, 2):
            group = folded[res]
            assert "h_tdigest_locations" in group and "h_tdigest_times" in group
            rows = zip(
                group["h_tdigest"],
                group["h_tdigest_locations"],
                group["h_tdigest_times"],
                strict=True,
            )
            for payload, raw_l, raw_t in rows:
                n = decode_digest(payload, "float32").shape[0]
                assert decode_digest(raw_l, "uint64", ()).shape == (n,)
                assert decode_digest(raw_t, "uint64", ()).shape == (n,)
                # A populated row's two siblings are never the same bytes.
                assert n == 0 or bytes(raw_l) != bytes(raw_t)
        # Row 1 of order 3 pools cells 5 and 6, and both siblings must be
        # byte-equal to the SAME k-way call's corresponding output — the slot
        # order the kernel fixes, not the order the caller's dict happened to be
        # built in.
        oracle, ow_l, ow_t = merge_tdigests_kway(
            [decode_digest(slabs["h_tdigest"][i], "float32") for i in (5, 6)],
            delta=DELTA,
            locations=[
                decode_digest(slabs["h_tdigest_locations"][i], "uint64", ()) for i in (5, 6)
            ],
            temporal=[decode_digest(slabs["h_tdigest_times"][i], "uint64", ()) for i in (5, 6)],
        )
        assert bytes(folded[3]["h_tdigest"][1]) == encode_digest(oracle, "float32")
        assert bytes(folded[3]["h_tdigest_locations"][1]) == encode_digest(ow_l, "uint64")
        assert bytes(folded[3]["h_tdigest_times"][1]) == encode_digest(ow_t, "uint64")
        # Row 0 pools cells 0..3, of which only cell 0 is populated, so it takes
        # ``fold_digests``' single-contributor arm — the one that bypasses the
        # k-way merge, and the majority case at the finest level. Each sibling
        # must pass its OWN cell's bytes through, not the other's.
        for name in ("h_tdigest", "h_tdigest_locations", "h_tdigest_times"):
            assert bytes(folded[3][name][0]) == bytes(slabs[name][0])
        # An empty row keeps the ragged fill on ALL THREE arrays.
        for name in ("h_tdigest", "h_tdigest_locations", "h_tdigest_times"):
            assert folded[3][name][2] == b""

    def test_leaf_slabs_picks_up_both_siblings(self):
        slabs, _truth = _located_cell_slabs(self.CELLS, BOTH_CHANNEL_FIELDS)
        staged = {f"{CELL_ORDER}/{n}": s for n, s in slabs.items()}
        out = leaf_slabs(
            staged, BOTH_CHANNEL_FIELDS, group_path=str(CELL_ORDER), n_cells=LEAF_CELLS
        )
        assert out["h_tdigest_locations"] is slabs["h_tdigest_locations"]
        assert out["h_tdigest_times"] is slabs["h_tdigest_times"]

    def test_a_missing_temporal_slab_refuses_by_name(self):
        # The located arm's counterpart: the refusal names the channel that is
        # absent, so a half-declared column is diagnosable.
        slabs, _truth = _located_cell_slabs(self.CELLS, BOTH_CHANNEL_FIELDS)
        del slabs["h_tdigest_times"]
        with pytest.raises(ValueError, match="declares a temporal channel"):
            fold_column(slabs, BOTH_CHANNEL_FIELDS, cell_order=CELL_ORDER, resolutions=[3])

    def test_an_unlocated_column_is_unchanged(self):
        # The channel is opt-in: the same cells under the unlocated declaration
        # produce exactly the pre-#410 groups, sibling absent.
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        assert all("h_tdigest_locations" not in group for group in folded.values())


def _assert_group_matches(overview, group: dict, n: int) -> None:
    """The leaf's ``n`` overview rows are byte-equal to its column group."""
    for name, meta in FIELDS.items():
        stored = overview[name][:n]
        if meta["class"] == "exact":
            assert stored.dtype == group[name].dtype
            np.testing.assert_array_equal(stored, group[name][:n])
        else:
            assert [bytes(p) for p in stored] == [bytes(p) for p in group[name][:n]]


class TestSweepParity:
    """The issue #383 headline: column groups == the sweep's from-leaves fold."""

    def test_column_matches_the_overview_sweep_bytes(self, tmp_path):
        from zagg.sweep import run_sweep

        # A /1 store sourced from ONE leaf, so the overview rows covering that
        # leaf are exactly the leaf's own fold. Both column group kinds are
        # covered: the order-1 overview holds cells at order 3 (the resolution
        # a (2, [3]) column group folds, `_fold_node`'s target >= shard arm),
        # and the order-0 overview holds cells at order 2 == the shard order —
        # the node-order member, where the leaf folds whole into one cell.
        # `fold_source: "leaves"` because from-leaves IS the parity contract.
        manifest = {
            "spec": "morton-hive/1",
            "dataset": {"short_name": "TEST", "version": "1"},
            "cell_order": CELL_ORDER,
            "shard_order": SHARD_ORDER,
            "split_schedule": [1] * SHARD_ORDER,
            "pyramid": {
                "spec": PYRAMID_SPEC,
                "overview": {
                    "spacing": 2,
                    "orders": [1, 0],
                    "fold_source": "leaves",
                    "all_time": False,
                    "fields": dict(FIELDS),
                },
            },
            "generated_at": "2026-01-01T00:00:00+00:00",
        }
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        # Cells 5 and 6 hold enough observations that their merge is over the
        # δ budget at both folded resolutions, so the declared delta decides
        # the parity bytes rather than being carried unobserved.
        cells = {0: [1.0, 2.0], 5: np.arange(200.0), 6: np.arange(200.0, 400.0), 15: [3.0, 8.0]}
        slabs = _make_leaf(tmp_path, "-311", cells)
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["written"] == 2

        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        overview = zarr.open_group(
            open_store(f"{tmp_path}/-3/1/all.zarr"), path="3", mode="r", zarr_format=3
        )
        # Leaf -311 is child 0 of node -31: overview rows 0..3 are its fold.
        _assert_group_matches(overview, folded[3], 4 ** (3 - SHARD_ORDER))
        # The node-order member: leaf -311 is _rel_rank 0 under node -3, and
        # the order-0 fold's factor is the whole leaf (4^(shard_order - 0)).
        node_overview = zarr.open_group(
            open_store(f"{tmp_path}/-3/all.zarr"), path=str(SHARD_ORDER), mode="r", zarr_format=3
        )
        _assert_group_matches(node_overview, folded[SHARD_ORDER], 1)


def _root_meta_sans_timestamps(store) -> dict:
    """The column root's attrs with the wall-clock provenance stripped."""
    from zagg.column import COLUMN_ATTR
    from zagg.hive import COMMIT_ATTR

    attrs = dict(zarr.open_group(store, mode="r", zarr_format=3).attrs)
    attrs[COLUMN_ATTR] = {k: v for k, v in attrs[COLUMN_ATTR].items() if k != "generated_at"}
    attrs[COMMIT_ATTR] = {k: v for k, v in attrs[COMMIT_ATTR].items() if k != "written_at"}
    return attrs


class TestWriteColumn:
    """Phase 2: the column writer — D4 order, attrs, naming, sidecar."""

    CELLS = {0: [1.0, 2.0], 5: [10.0, 4.0], 6: [7.0], 15: [3.0, 8.0, 5.0]}

    def _write(self, root, *, window=None, time_range=None, cells=None):
        from zagg.column import fold_column, write_column

        slabs = _cell_slabs(cells if cells is not None else self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        basename = write_column(
            str(root),
            morton_word("-311"),
            folded,
            FIELDS,
            node_order=SHARD_ORDER,
            cell_order=CELL_ORDER,
            window=window,
            time_range=time_range,
            granule_count=3,
        )
        return basename, folded

    def test_layout_groups_and_morton(self, tmp_path):
        from mortie import generate_morton_children

        basename, folded = self._write(tmp_path)
        assert basename == "all.pyramid.zarr"
        word = morton_word("-311")
        store = open_store(f"{tmp_path}/-3/1/1/{basename}")
        for res, n in ((3, 4), (SHARD_ORDER, 1)):
            group = zarr.open_group(store, path=str(res), mode="r", zarr_format=3)
            np.testing.assert_array_equal(
                group["morton"][:], np.asarray(generate_morton_children(word, res), np.uint64)
            )
            for name, meta in FIELDS.items():
                if meta["class"] == "none":
                    assert name not in group
                elif meta["class"] == "exact":
                    np.testing.assert_array_equal(group[name][:], folded[res][name])
                else:
                    stored = [bytes(p) for p in group[name][:]]
                    assert stored == [bytes(p) for p in folded[res][name]]

    def test_role_and_provenance_attrs(self, tmp_path):
        from zagg.column import COLUMN_ATTR, COLUMN_ROLE, COLUMN_SPEC, LEAF_REGIME

        basename, _folded = self._write(tmp_path)
        root = zarr.open_group(open_store(f"{tmp_path}/-3/1/1/{basename}"), mode="r", zarr_format=3)
        assert root.attrs["role"] == COLUMN_ROLE
        attrs = dict(root.attrs[COLUMN_ATTR])
        assert attrs["spec"] == COLUMN_SPEC
        assert attrs["node"] == "-311" and attrs["order"] == SHARD_ORDER
        assert attrs["source_cell_order"] == CELL_ORDER and attrs["window"] == "all"
        assert set(attrs["fields"]) == {"count", "h_min", "h_tdigest"}
        assert attrs["fields"]["count"] == {"class": "exact", "method": "sum", "nan_policy": "skip"}
        # An approximate entry also carries what decided its centroid bytes:
        # a #370 k-way gather cannot recover δ from the leaf. Since issue #424
        # that is the split overview_delta (here the DELTA≤512 fallback), the
        # budget fold_column actually compressed at; delta stays the leaf's.
        assert attrs["fields"]["h_tdigest"] == {
            "class": "approximate",
            "method": "tdigest_kway",
            "delta": DELTA,
            "overview_delta": DELTA,
            "dtype": "float32",
            "inner_shape": [2],
        }
        assert attrs["groups"] == {
            "3": {"regime": LEAF_REGIME, "merges_from_raw": 1, "n_cells": 4},
            str(SHARD_ORDER): {"regime": LEAF_REGIME, "merges_from_raw": 1, "n_cells": 1},
        }
        # A column has N grids, so the stamp's count needs its denominator
        # named: the finest group, which is declaration-dependent.
        assert attrs["cells_with_data_order"] == 3

    def test_stamp_covers_the_column_and_is_last(self, tmp_path, monkeypatch):
        from zagg.column import fold_column, write_column
        from zagg.hive import read_commit

        basename, _folded = self._write(tmp_path)
        store = open_store(f"{tmp_path}/-3/1/1/{basename}")
        stamp = read_commit(store)
        # CELLS occupies leaf cells 0, 5, 6, 15 -> base-group rows 0, 1, 3:
        # the stamp counts populated cells of the FINEST group (3), like the
        # overview writer's populated mask.
        assert stamp is not None and stamp["cells_with_data"] == 3
        assert stamp["granule_count"] == 3

        # An interrupted writer (death before the stamp) leaves ignorable
        # debris: the artifact prefix exists, but read_commit sees no stamp.
        import zagg.column as column_mod

        def _boom(*a, **k):
            raise RuntimeError("interrupted before the stamp")

        monkeypatch.setattr("zagg.hive.stamp_commit", _boom)
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        with pytest.raises(RuntimeError, match="interrupted"):
            write_column(
                str(tmp_path),
                morton_word("-312"),
                folded,
                FIELDS,
                node_order=SHARD_ORDER,
                cell_order=CELL_ORDER,
            )
        debris = open_store(f"{tmp_path}/-3/1/2/{column_mod.column_name(None)}")
        group = zarr.open_group(debris, path="3", mode="r", zarr_format=3)  # arrays landed
        assert group["count"].shape == (4,)
        assert read_commit(debris) is None  # ...but the column is debris (D4)

    def test_windowed_naming_and_stamp(self, tmp_path):
        from zagg.hive import read_commit

        rng = ["2019-01-02T00:00:00+00:00", "2019-11-30T00:00:00+00:00"]
        basename, _folded = self._write(tmp_path, window="2019", time_range=rng)
        assert basename == "2019.pyramid.zarr"
        stamp = read_commit(open_store(f"{tmp_path}/-3/1/1/{basename}"))
        assert stamp["window"] == "2019" and stamp["time_range"] == rng

    def test_sidecar_lands_after_the_stamp_and_fails_open(self, tmp_path, monkeypatch):
        basename, _folded = self._write(tmp_path)
        sidecar = tmp_path / "-3" / "1" / "1" / "all.pyramid.stats.json"
        record = json.loads(sidecar.read_text())
        assert record["cells_with_data"] == 3 and record["n_granules"] == 3
        hashes = record["content_hashes"]["arrays"]
        assert set(hashes) >= {"3/morton", "3/count", "3/h_tdigest", f"{SHARD_ORDER}/morton"}

        # Fail-open: a hashing failure costs the sidecar, never the column —
        # and on a REWRITE the outcome is ABSENT, not the previous run's
        # record. A stale sidecar verifies as a mismatch (a false tamper
        # signal), the one outcome D20 exists to prevent; the wholesale clear
        # drops it before the template lands, so this rewrite is over a
        # sidecar that is still on disk.
        monkeypatch.setattr(
            "zagg.content_hash.hash_arrays",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert sidecar.exists()
        basename, _folded = self._write(tmp_path, cells={0: [1.0]})
        from zagg.hive import read_commit

        assert read_commit(open_store(f"{tmp_path}/-3/1/1/{basename}")) is not None
        assert not sidecar.exists()

    def test_attrs_window_round_trips_through_the_name(self, tmp_path):
        # The attrs record the unwindowed column's window as the reserved
        # SCHEDULE_NONE_TOKEN, and `leaf_name_v3` RAISES on that token — so
        # the basename derivation normalizes it back, exactly as
        # `_overview_basename` does, and attrs -> name round-trips.
        from zagg.column import COLUMN_ATTR, column_name
        from zagg.windows import SCHEDULE_NONE_TOKEN

        basename, _folded = self._write(tmp_path)
        root = zarr.open_group(open_store(f"{tmp_path}/-3/1/1/{basename}"), mode="r", zarr_format=3)
        assert root.attrs[COLUMN_ATTR]["window"] == SCHEDULE_NONE_TOKEN
        assert column_name(root.attrs[COLUMN_ATTR]["window"]) == basename
        assert column_name(SCHEDULE_NONE_TOKEN) == column_name(None) == "all.pyramid.zarr"

    def test_sidecar_import_failure_never_fails_the_column(self, tmp_path, monkeypatch):
        # Every sidecar-only import sits INSIDE the fail-open try: a
        # telemetry-class ImportError (a slimmed runtime, say) must cost the
        # sidecar, never a column whose stamp already landed.
        import builtins

        from zagg.hive import read_commit

        real_import = builtins.__import__

        def _boom(name, *args, **kwargs):
            if name == "zagg.telemetry":
                raise ImportError("no telemetry on this runtime")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _boom)
        basename, _folded = self._write(tmp_path)
        monkeypatch.undo()
        assert read_commit(open_store(f"{tmp_path}/-3/1/1/{basename}")) is not None
        assert not (tmp_path / "-3" / "1" / "1" / "all.pyramid.stats.json").exists()

    def test_idempotent_rewrite_same_array_bytes(self, tmp_path):
        """A re-run reproduces the DATA bytes; provenance timestamps move.

        What is pinned: every resolution group's array bytes, and the root
        attrs + commit stamp modulo their timestamps. What is NOT: the root
        ``zarr.json`` byte-for-byte, nor the sidecar — ``generated_at``, the
        stamp's ``written_at`` and the sidecar's ``timestamp`` are wall-clock
        provenance and legitimately differ (the same posture as
        ``_write_overview``). Comparing raw objects would make the outcome
        clock-dependent: identical inside one second, different a second later.
        """
        basename, first = self._write(tmp_path)
        store = open_store(f"{tmp_path}/-3/1/1/{basename}")
        before = {
            (res, name): ([bytes(p) for p in arr[:]] if arr.dtype == object else arr[:].tobytes())
            for res in (3, SHARD_ORDER)
            for name, arr in zarr.open_group(store, path=str(res), mode="r", zarr_format=3).arrays()
        }
        meta_before = _root_meta_sans_timestamps(store)
        basename2, _second = self._write(tmp_path)
        assert basename2 == basename
        after = {
            (res, name): ([bytes(p) for p in arr[:]] if arr.dtype == object else arr[:].tobytes())
            for res in (3, SHARD_ORDER)
            for name, arr in zarr.open_group(store, path=str(res), mode="r", zarr_format=3).arrays()
        }
        assert before == after
        # The root object is rewritten too: same attrs and same stamp CONTENT
        # (a regression that changed either would pass a data-bytes-only check).
        assert _root_meta_sans_timestamps(store) == meta_before

    def test_rewrite_spares_the_sibling_leaf_and_its_sidecar(self, tmp_path):
        # The wholesale clear (issue #341 semantics) is scoped to the column's
        # own prefix: the node dir now holds three siblings, and the leaf is
        # the one object a mis-scoped delete would eat.
        from zagg.hive import read_commit

        slabs = _make_leaf(tmp_path, "-311", self.CELLS)
        leaf_sidecar = tmp_path / "-3" / "1" / "1" / "-311.stats.json"
        leaf_sidecar.write_text("{}")
        basename, _folded = self._write(tmp_path)
        basename, _folded = self._write(tmp_path)  # ...and again, the rewrite

        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        assert read_commit(leaf) is not None
        group = zarr.open_group(leaf, path=str(CELL_ORDER), mode="r", zarr_format=3)
        np.testing.assert_array_equal(group["count"][:], slabs["count"])
        assert leaf_sidecar.read_text() == "{}"
        assert (tmp_path / "-3" / "1" / "1" / "all.pyramid.stats.json").exists()

    def test_clear_is_scoped_on_an_obstore_prefix_store(self, tmp_path, monkeypatch):
        # The string-prefix hazard only exists on the fleet backend, where
        # `open_store` builds `zarr.storage.ObjectStore(store=S3Store(prefix=…))`
        # and zarr's `ObjectStore.delete_dir` leaves the EMPTY prefix un-slashed
        # — scoping rests on obstore's prefix store re-adding the delimiter.
        # Mirrors tests/test_hive.py's
        # `test_overwrite_clear_is_scoped_on_an_obstore_prefix_store`, using
        # obstore's own LocalStore in prefix mode (the same composition).
        import obstore as _obstore
        from zarr.storage import ObjectStore

        import zagg.store as store_mod

        def _prefixed(path, **kwargs):
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
            return ObjectStore(store=_obstore.store.LocalStore(prefix=path))

        monkeypatch.setattr(store_mod, "open_store", _prefixed)
        basename, _folded = self._write(tmp_path)
        node = tmp_path / "-3" / "1" / "1"
        # The adversarial shape: a name-prefix sibling of the column, which a
        # missing delimiter would sweep up with it.
        twin = node / f"{basename}.status"
        twin.mkdir()
        (twin / "r.json").write_text("{}")
        stray = node / basename / "3" / "stray.json"
        stray.write_text("{}")  # in-column debris
        self._write(tmp_path)

        assert not stray.exists()  # the clear really ran
        assert (twin / "r.json").read_text() == "{}"
        assert (node / "all.pyramid.stats.json").exists()

    def test_narrowed_rewrite_drops_the_retired_group(self, tmp_path):
        # The docstring's "a prior torn write never survives": a rewrite under
        # a narrower declaration leaves no orphan group behind.
        from zagg.column import fold_column, write_column

        self._write(tmp_path)
        node = tmp_path / "-3" / "1" / "1" / "all.pyramid.zarr"
        assert (node / "3").exists()
        slabs = _cell_slabs(self.CELLS)
        write_column(
            str(tmp_path),
            morton_word("-311"),
            fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[SHARD_ORDER]),
            FIELDS,
            node_order=SHARD_ORDER,
            cell_order=CELL_ORDER,
        )
        assert not (node / "3").exists()
        assert (node / str(SHARD_ORDER)).exists()

    def test_missing_node_member_refuses_by_name(self, tmp_path):
        # The node-order member is the universal partial #384's gather may
        # assume (#381 point (2)); a column without it would stamp complete
        # and fold short, so the writer refuses before anything lands.
        from zagg.column import fold_column, write_column

        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, FIELDS, cell_order=CELL_ORDER, resolutions=[3])
        with pytest.raises(ValueError, match="node-order member"):
            write_column(
                str(tmp_path),
                morton_word("-311"),
                folded,
                FIELDS,
                node_order=SHARD_ORDER,
                cell_order=CELL_ORDER,
            )
        assert not (tmp_path / "-3").exists()

    def test_empty_fold_refuses_by_name(self, tmp_path):
        # Same guard, the degenerate input: an empty `folded` used to reach
        # `folded[resolutions[0]]` and IndexError past the template write.
        from zagg.column import write_column

        with pytest.raises(ValueError, match="node-order member"):
            write_column(
                str(tmp_path),
                morton_word("-311"),
                {},
                FIELDS,
                node_order=SHARD_ORDER,
                cell_order=CELL_ORDER,
            )

    def test_none_class_fields_never_reach_the_template(self, tmp_path):
        from zagg.column import fold_column, write_column

        fields = dict(FIELDS, h_mean={"class": "none"})
        slabs = _cell_slabs(self.CELLS)
        folded = fold_column(slabs, fields, cell_order=CELL_ORDER, resolutions=[3, SHARD_ORDER])
        basename = write_column(
            str(tmp_path),
            morton_word("-311"),
            folded,
            fields,
            node_order=SHARD_ORDER,
            cell_order=CELL_ORDER,
        )
        group = zarr.open_group(
            open_store(f"{tmp_path}/-3/1/1/{basename}"), path="3", mode="r", zarr_format=3
        )
        assert "h_mean" not in group
        root = zarr.open_group(open_store(f"{tmp_path}/-3/1/1/{basename}"), mode="r", zarr_format=3)
        assert "h_mean" not in root.attrs["zagg_column"]["fields"]


# ---------------------------------------------------------------------------
# Phase 3: worker integration — the real leaf write path grows a column.
# ---------------------------------------------------------------------------

GENERATOR = (
    __import__("pathlib").Path(__file__).parent.parent / "tools" / "generate_spec_fixtures.py"
)


def _generator():
    """The spec fixture generator, loaded as a module (test_content_hash precedent)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("zagg_column_fixture_generator", GENERATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _column_bytes(column) -> dict:
    """``{(resolution, array): bytes}`` for every array under a column."""
    store = open_store(str(column))
    out = {}
    for res in (5, 4):
        group = zarr.open_group(store, path=str(res), mode="r", zarr_format=3)
        for name, arr in group.arrays():
            v = arr[:]
            out[(res, name)] = [bytes(p) for p in v] if v.dtype == object else v.tobytes()
    return out


def _run_unit(
    root,
    monkeypatch,
    *,
    pyramid=None,
    window=None,
    windowing=None,
    fail=False,
    sharded=True,
    time_range=None,
    **seam_kwargs,
):
    """One shard through the REAL ``process_and_write_hive`` (generator inputs).

    Returns ``(metadata, leaf_dir)``; the generator's fake ``process_shard``
    feeds the production leaf write, so the staged sink the column folds from
    is exactly what the leaf stored. ``sharded=False`` drives the same inputs
    through the per-chunk STREAMING leaf writer instead — a different route to
    ``staged`` (per-chunk lazy placement + ``write_ragged_leaf_to_zarr``), and
    the one every ``chunk_inner``-unset grid takes. ``time_range`` is the D15
    observed extent in DATASET units the fake reports; the caller converts it
    to the stamp's ISO pair (``windows.iso_time_range``) and rewrites
    ``metadata["time_range"]`` with the result.
    """
    from dataclasses import replace

    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid

    gen = _generator()
    cfg = gen._config(kitchen_sink=False, pyramid=pyramid)
    if windowing is not None:
        # The window filter injection (windowed_cell_config) needs the
        # time_field's base-rate dataset path; the fake reader ignores it.
        ds = {**cfg.data_source, "variables": {windowing["time_field"]: "g/t"}}
        cfg = replace(cfg, data_source=ds, output={**cfg.output, "windowing": windowing})
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=sharded)
    shard = morton_word(gen.SHARD_KEY)
    by_chunk, _cells = gen._build_cells(grid, shard, kitchen_sink=False)
    hive.ensure_manifest(
        str(root), hive.build_manifest(grid, dataset={"short_name": "COL_TEST", "version": "1"})
    )
    inner = gen._fake_process_shard(grid, by_chunk, kitchen_sink=False)

    def fake(*args, **kwargs):
        if fail:
            return None, {"error": "synthetic failure", "shard_key": int(args[1])}
        # The generator's fake only knows the sharded ``chunk_results`` sink;
        # the streaming path passes ``write_chunk`` instead, so collect the
        # chunks locally and replay them through the callback (the
        # ``test_content_hash._write_kitchen_sink`` precedent).
        write_chunk = kwargs.get("write_chunk")
        if kwargs.get("chunk_results") is None:
            kwargs["chunk_results"] = []
        df, meta = inner(*args, **kwargs)
        if write_chunk is not None:
            for block, carrier, ragged in kwargs["chunk_results"]:
                write_chunk(block, carrier, ragged)
        meta["phase_timings"] = {"read": 0.0, "index": 0.0, "aggregate": 0.0}
        if time_range is not None:
            meta["time_range"] = list(time_range)
        return df, meta

    monkeypatch.setattr(processing, "process_shard", fake)
    meta = hive.process_and_write_hive(
        shard,
        ["s3://fixture/a.h5"],
        grid,
        {},
        str(root),
        cfg,
        store_kwargs={},
        window=window,
        **seam_kwargs,
    )
    label = window["label"] if window else None
    leaf_rel = hive.shard_leaf_path("", shard, window=label).lstrip("/")
    return meta, root / leaf_rel


class TestWorkerIntegration:
    PYRAMID = {"overviews": 5}
    WINDOWING = {"schedule": "yearly", "time_field": "t", "epoch": "2018-01-01T00:00:00Z"}

    def test_column_rides_the_leaf_write(self, tmp_path, monkeypatch):
        from zagg.hive import read_commit
        from zagg.sweep_overview import fold_dense

        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        assert meta.get("error") is None
        assert meta["leaf_column"] == "all.pyramid.zarr"
        assert "column" in meta["phase_timings"]
        assert read_commit(open_store(str(leaf))) is not None
        column = leaf.parent / "all.pyramid.zarr"
        col_store = open_store(str(column))
        assert read_commit(col_store) is not None
        # End-to-end parity, disk to disk: the column's groups equal a fold of
        # the COMMITTED leaf's read-back arrays (the sweep's own kernels).
        leaf_group = zarr.open_group(open_store(str(leaf)), path="6", mode="r", zarr_format=3)
        for res in (5, 4):
            got = zarr.open_group(col_store, path=str(res), mode="r", zarr_format=3)
            factor = 4 ** (6 - res)
            np.testing.assert_array_equal(
                got["count"][:], fold_dense(leaf_group["count"][:], factor, "sum", 0)
            )
            oracle = _refold_digests(leaf_group["h_tdigest"][:], factor, delta=_generator().DELTA)
            assert [bytes(p) for p in got["h_tdigest"][:]] == oracle

    def test_default_declaration_writes_the_column(self, tmp_path, monkeypatch):
        # The ruled issue #384 /2 default flip: no knob now means overviews at
        # the grid's resolved chunk order (5 on the fixture grid) — the same
        # column an explicit ``overviews: 5`` writes.
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=None)
        assert meta.get("error") is None
        assert meta["leaf_column"] == "all.pyramid.zarr"
        assert (leaf.parent / "all.pyramid.zarr").exists()

    def test_no_column_when_pyramid_declared_off(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=False)
        assert meta.get("error") is None and "leaf_column" not in meta
        assert not list(leaf.parent.glob("*.pyramid.zarr"))

    def test_dropping_the_knob_clears_the_previous_column(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        assert meta["leaf_column"] == "all.pyramid.zarr"
        assert (leaf.parent / "all.pyramid.zarr").exists()
        assert (leaf.parent / "all.pyramid.stats.json").exists()
        # Same leaf, declaration turned OFF (since the issue #384 default
        # flip, absent means declared — ``pyramid: false`` is the off state):
        # the fresh leaf must not keep a STAMPED column folded from the
        # superseded run's cells.
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=False)
        assert meta.get("error") is None and "leaf_column" not in meta
        assert not (leaf.parent / "all.pyramid.zarr").exists()
        assert not (leaf.parent / "all.pyramid.stats.json").exists()
        assert (leaf / "zarr.json").exists()

    def test_windowed_unit_gets_a_window_named_column(self, tmp_path, monkeypatch):
        from zagg.hive import read_commit

        meta, leaf = _run_unit(
            tmp_path,
            monkeypatch,
            pyramid=self.PYRAMID,
            window={"label": "2019", "start": 0.0, "end": 1.0},
            windowing=self.WINDOWING,
            time_range=[31536000.0, 31536060.0],
        )
        assert meta.get("error") is None
        assert meta["leaf_column"] == "2019.pyramid.zarr"
        stamp = read_commit(open_store(str(leaf.parent / "2019.pyramid.zarr")))
        assert stamp is not None and stamp["window"] == "2019"
        # The D15 truth half: the worker's observed extent, converted to the
        # stamp's ISO pair, reached the COLUMN's stamp and not just the leaf's.
        assert stamp["time_range"] == meta["time_range"]
        assert stamp["time_range"] == ["2019-01-01T00:00:00+00:00", "2019-01-01T00:01:00+00:00"]

    def test_two_windows_get_side_by_side_columns(self, tmp_path, monkeypatch):
        from zagg.hive import read_commit

        def run(label, start, end):
            return _run_unit(
                tmp_path,
                monkeypatch,
                pyramid=self.PYRAMID,
                window={"label": label, "start": start, "end": end},
                windowing=self.WINDOWING,
            )

        meta, leaf = run("2019", 0.0, 1.0)
        assert meta["leaf_column"] == "2019.pyramid.zarr"
        first = _column_bytes(leaf.parent / "2019.pyramid.zarr")
        meta = run("2020", 1.0, 2.0)[0]
        assert meta["leaf_column"] == "2020.pyramid.zarr"
        # The D13 case: the second window's WHOLESALE clear is scoped to its
        # own basename, so the first window's column and sidecar are untouched.
        assert _column_bytes(leaf.parent / "2019.pyramid.zarr") == first
        assert (leaf.parent / "2019.pyramid.stats.json").exists()
        assert (leaf.parent / "2020.pyramid.stats.json").exists()
        for label in ("2019", "2020"):
            stamp = read_commit(open_store(str(leaf.parent / f"{label}.pyramid.zarr")))
            assert stamp is not None and stamp["window"] == label

    def test_rerun_rewrites_the_column_to_the_same_bytes(self, tmp_path, monkeypatch):
        _meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        column = leaf.parent / "all.pyramid.zarr"
        before = _column_bytes(column)
        _meta, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        assert _column_bytes(column) == before

    def test_streaming_leaf_writes_the_same_column(self, tmp_path, monkeypatch):
        """The other leaf writer: same inputs, per-chunk stream, same bytes.

        ``staged`` is built by a completely different route on the unsharded
        path (per-chunk lazy placement in ``write_dataframe_to_zarr`` plus
        ``write_ragged_leaf_to_zarr``, not the sharded single-slab pass), so
        the phase's claim — the column folds exactly what the leaf stored —
        needs pinning on both.
        """
        _meta, leaf = _run_unit(tmp_path / "sharded", monkeypatch, pyramid=self.PYRAMID)
        meta, stream_leaf = _run_unit(
            tmp_path / "stream", monkeypatch, pyramid=self.PYRAMID, sharded=False
        )
        assert meta.get("error") is None and meta["leaf_column"] == "all.pyramid.zarr"
        assert _column_bytes(stream_leaf.parent / "all.pyramid.zarr") == _column_bytes(
            leaf.parent / "all.pyramid.zarr"
        )

    def test_column_failure_fails_the_unit(self, tmp_path, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("column write exploded")

        from zagg.hive import read_commit

        monkeypatch.setattr("zagg.column.write_column", boom)
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        # Reported, not raised: the caller keeps a coherent metadata dict to
        # build its failure record from, and the unit still reports FAILED.
        assert meta["error"] == "leaf column: column write exploded"
        assert meta["column_error"] == "column write exploded"
        assert "leaf_column" not in meta
        # The state the retry has to repair (§4.6 failure identity): the leaf
        # is COMMITTED and stamped, and no column stands beside it.
        assert read_commit(open_store(str(leaf))) is not None
        assert not list(leaf.parent.glob("*.pyramid.zarr"))

    def test_gate_refusal_names_the_shard_and_window(self, tmp_path, monkeypatch):
        # The gate re-validates a declaration the templating path already
        # accepted, so a divergence fires on EVERY shard at once — the wrap is
        # what makes one Lambda log line attributable to a unit and a seam.
        def boom(*a, **k):
            raise ValueError("resolution 7 is not strictly between")

        monkeypatch.setattr("zagg.column.leaf_column_plan", boom)
        meta, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        shard = morton_word(_generator().SHARD_KEY)
        assert meta["error"] == (
            f"leaf column: column gate refused shard {shard} window None: "
            "resolution 7 is not strictly between"
        )

    def test_errored_shard_writes_neither_leaf_nor_column(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID, fail=True)
        assert meta.get("error") == "synthetic failure"
        assert "leaf_column" not in meta
        assert not leaf.exists()
        assert not list(leaf.parent.glob("*.pyramid.zarr"))


def _refold_digests(payloads, factor, *, delta):
    """Re-run the approximate fold kernels over the READ-BACK leaf.

    NOT an independent oracle: same ``decode_digest``, same ``fold_digests``,
    same child slicing and skip-empty predicate as ``column.fold_column``, so
    as a check on the fold LAW it is circular. What it adds is the read-back
    axis — it folds the COMMITTED leaf off disk where the writer folded the
    in-memory staged slabs, which is the staged-vs-stored half of the phase's
    claim (``TestSweepParity`` owns the law itself).
    """
    from zagg.sweep_overview import decode_digest, fold_digests

    out = []
    for j in range(len(payloads) // factor):
        cell = [
            decode_digest(bytes(p), "float32", (2,))
            for p in payloads[j * factor : (j + 1) * factor]
            if p is not None and len(p)
        ]
        out.append(fold_digests(cell, delta=delta, dtype="float32") if cell else b"")
    return out


class TestColumnDefeatsTheSkipGate:
    """Issue #388: the identity PAIR does not move when the ``output.pyramid``
    declaration is added or dropped — ``output`` is outside
    ``semantics.semantic_core`` and ``pyramid`` is deliberately not a frozen
    manifest key (D11) — so the leaf skip gate verifies the COLUMN ARTIFACT
    itself. Without that, enabling the column over an existing store would
    skip every leaf and never write a column."""

    PYRAMID = {"overviews": 5}
    URLS = ["s3://fixture/a.h5"]

    def _seal(self, meta, leaf):
        """The D20 sidecar the dispatcher writes after a successful unit (#297)."""
        from zagg.telemetry import build_record, write_sidecar

        record = build_record(
            shard_key=int(meta["shard_key"]),
            metadata=meta,
            granule_ids=list(self.URLS),
            run_id="r1",
            semantic_hash=meta["semantic_hash"],
        )
        write_sidecar(str(leaf), record)

    def test_the_declaration_moves_neither_identity_half(self):
        # The premise: the recorded pair is blind to the declaration, so the
        # gate cannot learn about the column from the record.
        from zagg.semantics import semantic_hash

        gen = _generator()
        with_col = gen._config(kitchen_sink=False, pyramid=self.PYRAMID)
        without = gen._config(kitchen_sink=False, pyramid=None)
        assert semantic_hash(with_col) == semantic_hash(without)

    def test_enabling_the_column_defeats_the_skip(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=False)
        self._seal(meta, leaf)
        assert not (leaf.parent / "all.pyramid.zarr").exists()
        # Same inputs, same D19 hash; only the declaration changed.
        redo, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID, skip_if_current=True)
        assert redo["identity"] == "column-drift" and "current" not in redo
        assert redo["leaf_column"] == "all.pyramid.zarr"
        assert (leaf.parent / "all.pyramid.zarr").exists()

    def test_dropping_the_column_defeats_the_skip(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        self._seal(meta, leaf)
        assert (leaf.parent / "all.pyramid.zarr").exists()
        redo, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=False, skip_if_current=True)
        assert redo["identity"] == "column-drift" and "current" not in redo
        # The rewrite runs _clear_column, so the superseded artifact is gone —
        # a skip would have stranded a STAMPED column no run declares.
        assert not (leaf.parent / "all.pyramid.zarr").exists()

    def test_an_unchanged_declaration_still_skips(self, tmp_path, monkeypatch):
        # Both arms of the check agree: the column exists and is declared.
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        self._seal(meta, leaf)
        redo, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID, skip_if_current=True)
        assert redo["current"] is True and redo["identity"] == "equal"

    def test_no_column_declared_and_none_present_still_skips(self, tmp_path, monkeypatch):
        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=False)
        self._seal(meta, leaf)
        redo, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=False, skip_if_current=True)
        assert redo["current"] is True and redo["identity"] == "equal"

    def test_the_column_check_is_per_window(self, tmp_path, monkeypatch):
        # The column is named per window, so the gate must read THIS unit's
        # column: a 2019 unit is not certified by a sibling window's artifact.
        windowing = TestWorkerIntegration.WINDOWING
        window = {"label": "2019", "start": 0.0, "end": 1.0}
        meta, leaf = _run_unit(
            tmp_path,
            monkeypatch,
            pyramid=False,
            window=window,
            windowing=windowing,
            time_range=[31536000.0, 31536060.0],
        )
        self._seal(meta, leaf)
        redo, _leaf = _run_unit(
            tmp_path,
            monkeypatch,
            pyramid=self.PYRAMID,
            window=window,
            windowing=windowing,
            time_range=[31536000.0, 31536060.0],
            skip_if_current=True,
        )
        assert redo["identity"] == "column-drift" and redo["leaf_column"] == "2019.pyramid.zarr"
        assert (leaf.parent / "2019.pyramid.zarr").exists()

    def test_a_skip_touches_the_column_family_too(self, tmp_path, monkeypatch):
        # Phase 3 (issue #388): the lifecycle touch covers the WHOLE unit
        # footprint — the declared column tree and its own stats sidecar
        # included — so a purge rule scoped anywhere under the node sees the
        # skip. The gate already certified declaration == artifact, so the
        # touch cannot resurrect the column-drift ambiguity.
        import os

        meta, leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID)
        self._seal(meta, leaf)
        node = leaf.parent
        epoch = 10_000
        n_files = 0
        for dirpath, _dirs, files in os.walk(node):
            for name in files:
                os.utime(os.path.join(dirpath, name), (epoch, epoch))
                n_files += 1
        redo, _leaf = _run_unit(tmp_path, monkeypatch, pyramid=self.PYRAMID, skip_if_current=True)
        assert redo["current"] is True
        stale = [
            os.path.join(dirpath, name)
            for dirpath, _dirs, files in os.walk(node)
            for name in files
            if os.stat(os.path.join(dirpath, name)).st_mtime_ns <= epoch * 10**9
        ]
        assert stale == []  # column tree + column sidecar moved with the leaf
        assert redo["touched_objects"] == n_files and redo["touch_failed"] == 0
