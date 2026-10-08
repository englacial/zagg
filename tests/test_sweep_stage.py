"""Staged dense sweep (issue #384): planner, worker, finisher, orchestration.

Standing claims:

- tuple grouping is orchestration-only — dispatch nodes at ``0 mod width``,
  ragged finest tuple, child columns one tuple down;
- source classification is derived: gather at/below the shard's resolution
  window, merge above it; every merge cascades from its children's artifacts
  at the next finer level (issue #620), reading four slabs per node whatever
  the subtree holds, so ``tuple_width=1`` and ``tuple_width=3`` builds of the
  same store carry the same values and ``merges_from_raw`` is the fold depth;
- scope is a MOC over node prefixes (shardmap keys as sugar), composing with
  partitions by intersection;
- soft barriers: partial coverage is recorded (``source_children``) and
  self-heals; skip-if-current keys on summed child generations;
- the finisher owns the root singletons; the lease serializes sweeps.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import numpy as np
import pytest
import zarr

import zagg.sweep_stage as stage_mod
import zagg.sweep_stages as stages_mod
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, _utcnow, build_root_coverage, write_root_coverage
from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews
from zagg.store import open_store
from zagg.sweep_overview import ENVELOPE_NAME, encode_digest
from zagg.sweep_stage import (
    STAGE_GATHER,
    STAGE_MERGE,
    ForeignSweepError,
    classify_level,
    column_members,
    finer_levels,
    ladder_entries,
    stage_tuples,
)
from zagg.sweep_stages import (
    compose_scope,
    normalize_scope,
    partition_words,
    run_finisher,
    scope_admits,
    sweep_stage_pass,
)


def _pyramid_block(resolutions, shard_order):
    return {
        "spec": PYRAMID_SPEC_V2,
        "overviews": expand_overviews(resolutions, parent_order=shard_order),
        "overview": {"all_time": False, "fold_source": "cascade", "fields": {}},
    }


class TestLadderEntries:
    def test_excludes_leaf_entry_and_sorts_finest_first(self):
        entries = ladder_entries(_pyramid_block([13], 9), 9)
        assert [e["node"] for e in entries] == list(range(8, -1, -1))
        assert all(e["cells"] == [e["node"] + 4] for e in entries)

    def test_refuses_v1_block(self):
        with pytest.raises(ValueError, match="zagg-pyramid/2"):
            ladder_entries({"spec": "zagg-pyramid/1", "overview": {"orders": [3]}}, 9)

    def test_refuses_empty_overviews(self):
        block = {"spec": PYRAMID_SPEC_V2, "overviews": []}
        with pytest.raises(ValueError, match="absent or empty"):
            ladder_entries(block, 9)

    def test_refuses_multi_member_ladder_entry(self):
        block = _pyramid_block([13], 9)
        block["overviews"][1]["cells"] = [12, 11]
        with pytest.raises(ValueError, match="exactly one"):
            ladder_entries(block, 9)


class TestStageTuples:
    def test_reference_geometry_o9_width_3(self):
        tuples = stage_tuples(9, tuple_width=3)
        assert [t["dispatch"] for t in tuples] == [6, 3, 0]
        assert [t["orders"] for t in tuples] == [[8, 7, 6], [5, 4, 3], [2, 1, 0]]
        assert [t["child_order"] for t in tuples] == [9, 6, 3]

    def test_ragged_finest_tuple(self):
        tuples = stage_tuples(8, tuple_width=3)
        assert [t["orders"] for t in tuples] == [[7, 6], [5, 4, 3], [2, 1, 0]]
        assert [t["child_order"] for t in tuples] == [8, 6, 3]

    def test_width_1_is_one_tuple_per_order(self):
        tuples = stage_tuples(3, tuple_width=1)
        assert [t["orders"] for t in tuples] == [[2], [1], [0]]
        assert [t["child_order"] for t in tuples] == [3, 2, 1]

    def test_width_wider_than_ladder_is_one_root_tuple(self):
        (t,) = stage_tuples(3, tuple_width=3)
        assert t == {"dispatch": 0, "orders": [2, 1, 0], "child_order": 3}

    def test_orders_cover_ladder_exactly_once(self):
        for width in (1, 2, 3, 4):
            covered = [k for t in stage_tuples(9, tuple_width=width) for k in t["orders"]]
            assert sorted(covered) == list(range(9))

    def test_refusals(self):
        with pytest.raises(ValueError, match="tuple_width"):
            stage_tuples(9, tuple_width=0)
        with pytest.raises(ValueError, match="no above-shard ladder"):
            stage_tuples(0)


class TestClassifyLevel:
    def test_reference_geometry_first_merge_at_node_4(self):
        # o9/d=4: gather at nodes >= 5 (cells 9..12), merge at nodes <= 4
        # (#381 point (6): "the first true k-way merge appears at node 4").
        for e in ladder_entries(_pyramid_block([13], 9), 9):
            expected = STAGE_GATHER if e["node"] >= 5 else STAGE_MERGE
            assert classify_level(e["cells"][0], shard_order=9) == expected

    def test_boundary_is_the_shard_order(self):
        assert classify_level(9, shard_order=9) == STAGE_GATHER
        assert classify_level(8, shard_order=9) == STAGE_MERGE


class TestColumnMembers:
    def test_reference_geometry(self):
        levels = expand_overviews([13], parent_order=9)
        # o6 column: the members nodes 5..0 gather — cells {9..12} from nodes
        # {5,4,3,2,1,0} intersect >= 9 -> {9} at node 5 only; nodes 4.. are
        # merges, which read artifacts, never a column (issue #620).
        assert column_members(levels, 6, shard_order=9) == [9]
        # o3 column: coarser levels' cells (7..2 + 4) are all < 9 — nothing
        # gathers from it, so the [2,1,0] tuple reads its order-3 children's
        # artifacts alone and the dispatch node writes no column.
        assert column_members(levels, 3, shard_order=9) == []

    def test_a_gapped_ladder_needs_no_special_member(self):
        # A ladder that straddles the raw-fold boundary WITHOUT carrying it
        # (members {12, 10, 9} — no group 11) is refused at DECLARATION since
        # the contiguity ruling (PR #567 thread); a hand-built manifest can
        # still carry it, and a stage column then gathers exactly what the
        # coarser gather levels need (nothing, at d = 1) — the ladder no
        # longer relays a leaf member for its merges.
        from zagg.pyramid import validate_overviews

        for ov in ([12, 10], [13, 10]):
            with pytest.raises(ValueError, match="column tier must be contiguous"):
                validate_overviews(ov, parent_order=9, child_order=19)
            assert column_members(expand_overviews(ov, parent_order=9), 6, shard_order=9) == []

    def test_finer_declaration_widens_the_gather_tier(self):
        # d=1 (overviews [10] on o9): node-8 level gathers cells 9 straight
        # from the leaf columns; every level below is a merge, so a width-3
        # column at 6 has nothing to carry.
        levels = expand_overviews([10], parent_order=9)
        assert column_members(levels, 6, shard_order=9) == []

    def test_multi_resolution_leaf_declaration(self):
        # overviews [14, 13] on o9: d = 13 - 9 = 4; ladder unchanged, and the
        # finer 14 member is leaf-only (no coarser level gathers it).
        levels = expand_overviews([14, 13], parent_order=9)
        assert column_members(levels, 6, shard_order=9) == [9]

    def test_wide_window_carries_gather_members(self):
        # d=5 on o6 (overviews [11]): coarser levels than the o3 column with
        # gatherable cells are node 2 (cells 7) and node 1 (cells 6); its own
        # level (cells 8) is an artifact, never one of its members; node 0's
        # level (cells 5) is a merge, so the o1 column has nothing to carry.
        levels = expand_overviews([11], parent_order=6)
        assert column_members(levels, 3, shard_order=6) == [7, 6]
        assert column_members(levels, 1, shard_order=6) == []


class TestFinerLevels:
    def test_every_order_ladder_folds_from_the_order_above(self):
        levels = ladder_entries(_pyramid_block([13], 9), 9)
        assert finer_levels(levels) == {k: (k + 1, k + 5) for k in range(8)}
        assert 8 not in finer_levels(levels)  # the finest level: a gather by construction

    def test_a_gapped_hand_built_ladder_folds_from_the_nearest_finer_entry(self):
        levels = [{"node": 2, "cells": [4]}, {"node": 0, "cells": [2]}]
        assert finer_levels(levels) == {0: (2, 4)}


class TestScope:
    def test_none_passes_through(self):
        assert normalize_scope(None) is None
        assert scope_admits("-4211", None)

    def test_decimals_ints_and_shardmap_keys(self):
        from zagg.grids.morton import morton_word

        w = morton_word("-42113")
        assert list(normalize_scope(["-42113"])) == [w]
        assert list(normalize_scope([w])) == [w]
        assert list(normalize_scope({"-42113": {"granules": []}})) == [w]

    def test_empty_scope_refuses(self):
        with pytest.raises(ValueError, match="empty"):
            normalize_scope([])

    def test_ancestor_prefixes_admit(self):
        scope = normalize_scope(["-42113221", "-42113222"])
        for node in ("-4", "-42", "-421", "-4211", "-42113221"):
            assert scope_admits(node, scope)
        assert not scope_admits("-43", scope)
        assert not scope_admits("3", scope)

    def test_partition_compose(self):
        scope = normalize_scope(["-42113", "31222"])
        # order-1 split: partition of index rank('2')==1 owns every subtree
        # whose first digit is 2.
        part = partition_words(4, 1)
        composed = compose_scope(scope, part)
        assert scope_admits("-42113", composed)
        assert not scope_admits("31222", composed)
        assert compose_scope(scope, None) is scope
        assert compose_scope(None, part) is part

    def test_partition_words_identity(self):
        assert partition_words(1, 0).size == 12


# ---------------------------------------------------------------------------
# Phase 2: the stage worker over a real (tiny) /2 store.
# ---------------------------------------------------------------------------

FIELDS = {
    "count": {
        "class": "exact",
        "method": "sum",
        "nan_policy": "skip",
        "dtype": "int32",
        "fill_value": 0,
    },
    "h_tdigest": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 16,
    },
}
#: Order-3 shards under two base cells; 16 order-5 cells per leaf.
LEAVES = ["1111", "1112", "1121", "-2111"]


def _leaf_slabs(i, n=16):
    counts = (np.arange(n, dtype="int32") + 1) * (i + 1)
    dig = np.full(n, b"", dtype=object)
    for j in range(n):
        dig[j] = encode_digest(np.asarray([[float(i * 100 + j), 1.0]], dtype=np.float32), "float32")
    return {"count": counts, "h_tdigest": dig}


#: The same declaration with the digest field located (ruling 4, issue #410).
LOCATED_FIELDS = {**FIELDS, "h_tdigest": {**FIELDS["h_tdigest"], "location": "leaf_id"}}
#: Two located fields, so ``_merge_slabs``' per-field ``pending``/``words`` (which
#: ``_close`` closes over BY NAME, rebound each iteration) are exercised by more
#: than one iteration — the late-binding shape a later edit breaks silently.
TWO_LOCATED_FIELDS = {**LOCATED_FIELDS, "h2_tdigest": dict(LOCATED_FIELDS["h_tdigest"])}
#: ONE field carrying BOTH channels — the arity the ``channels=`` map exists for
#: (espg ruling of 2026-08-17: temporal is per-centroid at every level too).
#: ``TWO_LOCATED_FIELDS`` above is two located FIELDS, a different shape.
BOTH_CHANNEL_FIELDS = {
    **LOCATED_FIELDS,
    "h_tdigest": {**LOCATED_FIELDS["h_tdigest"], "temporal": "per-centroid"},
}
#: And the temporal channel alone, which is what a digest field with a clock and
#: no ``location:`` declares.
TIMED_FIELDS = {
    **FIELDS,
    "h_tdigest": {**FIELDS["h_tdigest"], "temporal": "per-centroid"},
}


def _companion_kwargs(meta):
    """The ``build_tdigest`` channel kwargs ``meta`` declares, in table order."""
    return [
        kwarg
        for key, kwarg in (("location", "locations"), ("temporal", "temporal"))
        if meta.get(key) is not None
    ]


def _located_leaf_slabs(i, fields=LOCATED_FIELDS, n=16):
    """``_leaf_slabs`` plus each digest field's declared companion siblings.

    ``{field}_locations`` for a ``location:`` field and ``{field}_times`` for a
    ``temporal:`` one — both when it declares both, built in the SAME
    ``build_tdigest`` call so the two siblings describe one partition.
    """
    from conftest import TOC_BASE, point_words, toc_words

    from zagg.stats.tdigest import build_tdigest

    slabs = _leaf_slabs(i, n)
    for name, meta in fields.items():
        declared = _companion_kwargs(meta)
        if not declared:
            continue
        dig = np.full(n, b"", dtype=object)
        sibs = {kwarg: np.full(n, b"", dtype=object) for kwarg in declared}
        for j in range(n):
            kw = {}
            if "locations" in declared:
                kw["locations"] = point_words(1, seed=3000 + i * n + j)
            if "temporal" in declared:
                # A distinct instant per (leaf, cell), so no two envelopes are
                # confusable and a word from the wrong cell is visible.
                when = np.datetime64(TOC_BASE, "ns") + np.timedelta64(60 * (i * n + j), "s")
                kw["temporal"] = toc_words(1, base=str(when))
            d, *words = build_tdigest(np.asarray([float(i * 100 + j)]), 16, **kw)
            dig[j] = encode_digest(d, "float32")
            for kwarg, w in zip(kw, words, strict=True):
                sibs[kwarg][j] = encode_digest(w, "uint64")
        slabs[name] = dig
        if "locations" in declared:
            slabs[f"{name}_locations"] = sibs["locations"]
        if "temporal" in declared:
            slabs[f"{name}_times"] = sibs["temporal"]
    return slabs


def _write_leaf(root, dec, i, granules=1, fields=FIELDS, slabs=None):
    from zagg.column import column_resolutions, fold_column, write_column
    from zagg.pyramid import expand_overviews

    levels = expand_overviews([4], parent_order=3)
    res = column_resolutions(levels, 3)
    folded = fold_column(
        _leaf_slabs(i) if slabs is None else slabs, fields, cell_order=5, resolutions=res
    )
    write_column(
        str(root),
        morton_word(dec),
        folded,
        fields,
        node_order=3,
        cell_order=5,
        granule_count=granules,
    )


def _stage_store(root, leaves=LEAVES, skip_columns=(), write_moc=True, fields=FIELDS):
    """A tiny /2 hive store: manifest + leaf columns + root MOC (accelerator)."""
    from zagg.pyramid import expand_overviews

    root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "001"},
        "semantic_hash": "t",
        "cell_order": 5,
        "shard_order": 3,
        "split_schedule": [1, 1, 1],
        "path_grouping": 1,
        "pyramid": {
            "spec": PYRAMID_SPEC_V2,
            "overviews": expand_overviews([4], parent_order=3),
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
    companioned = any(_companion_kwargs(m) for m in fields.values())
    for i, dec in enumerate(leaves):
        if dec not in skip_columns:
            slabs = _located_leaf_slabs(i, fields) if companioned else None
            _write_leaf(root, dec, i, fields=fields, slabs=slabs)
    if write_moc:
        write_root_coverage(str(root), build_root_coverage([morton_word(d) for d in leaves], 3))
    return manifest


def _by_shard(leaves=LEAVES):
    return {d: {None} for d in leaves}


def _sweep(root, manifest, *, width=3, run_id="A", leaves=LEAVES, scope=None):
    return sweep_stage_pass(
        str(root), manifest, _by_shard(leaves), run_id=run_id, tuple_width=width, scope=scope
    )


def _artifact(root, rel):
    store = open_store(str(root / rel), read_only=True)
    return zarr.open_group(store, path="", mode="r", zarr_format=3)


def _restamp(root, rel, **keys):
    """Rewrite commit-stamp keys in place — the #380 spy pattern's injection arm."""
    g = zarr.open_group(open_store(str(root / rel)), path="", mode="r+", zarr_format=3)
    stamp = dict(g.attrs["morton_hive_commit"])
    stamp.update(keys)
    g.attrs["morton_hive_commit"] = stamp


def _ladder_arrays(root):
    """Every ladder artifact's arrays, keyed by store-relative path."""
    out = {}
    for p in sorted(root.rglob("all.zarr"), key=str):
        g = _artifact(root, p.relative_to(root))
        attrs = dict(g.attrs)
        if attrs.get("role") != "overview":
            continue
        r = attrs["zagg_overview"]["cell_order"]
        out[str(p.relative_to(root))] = {
            name: g[str(r)][name][:] for name in ("morton", "count", "h_tdigest")
        }
    return out


def _payload_equal(x, y):
    return all(bytes(p or b"") == bytes(q or b"") for p, q in zip(x, y, strict=True))


#: The issue #538 geometry: shard 3 / cells 6 (64 cells per leaf), overviews
#: [5] — the leaf column carries {5, 4, 3} with 5 the raw-fold boundary, so
#: 4 and 3 are flat second merges; the ladder is (2, 4), (1, 3) gathers and
#: (0, 2) the one merge, so the order-2 dispatch columns carry member 3.
WIDE_CELL_ORDER = 6


def _wide_store(root, leaves=LEAVES, fields=FIELDS, cell_order=WIDE_CELL_ORDER, overviews=(5,)):
    """A deeper leaf (``4 ** (cell_order - 3)`` cells) under a sized ladder."""
    from zagg.column import column_resolutions, fold_column, write_column
    from zagg.pyramid import expand_overviews

    manifest = json.loads(json.dumps(_stage_store(root, leaves=(), write_moc=False, fields=fields)))
    manifest["cell_order"] = cell_order
    levels = expand_overviews(list(overviews), parent_order=3)
    manifest["pyramid"]["overviews"] = levels
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1))
    res = column_resolutions(levels, 3)
    n = 4 ** (cell_order - 3)
    companioned = any(_companion_kwargs(m) for m in fields.values())
    for i, dec in enumerate(leaves):
        slabs = _located_leaf_slabs(i, fields, n) if companioned else _leaf_slabs(i, n)
        folded = fold_column(slabs, fields, cell_order=cell_order, resolutions=res, node_order=3)
        write_column(
            str(root),
            morton_word(dec),
            folded,
            fields,
            node_order=3,
            cell_order=cell_order,
            granule_count=1,
        )
    write_root_coverage(str(root), build_root_coverage([morton_word(d) for d in leaves], 3))
    return manifest


#: Base cell '1' with every order-2 node inhabited by one leaf, and by two:
#: the same four order-1 children either way, so a node's cascade reads the
#: same four slabs however many leaves sit beneath them.
DENSE_16 = [f"1{a}{b}1" for a in "1234" for b in "1234"]
DENSE_32 = DENSE_16 + [f"1{a}{b}2" for a in "1234" for b in "1234"]


def _per_node_reads(summary):
    """``{dispatch order: source cells read per node}`` from a pass summary."""
    return {s["dispatch_order"]: s["fold_cells_read"] / s["nodes"] for s in summary["stages"]}


class TestCascade:
    """Issue #620: a merge level folds its children's artifacts, never the leaf tier.

    The ladder of ``_stage_store`` (d = 1): (2, 3) gathers the leaf columns'
    node member, (1, 2) cascades the four (2, 3) artifacts, (0, 1) the four
    (1, 2) ones — two fields, so a merge node reads 4 x 4 x 2 = 32 cells
    whatever its subtree holds (the relay regime read 4^(3 - k) x 2: 32 at
    order 1, 128 at order 0, and 4.46M / 18.9M at orders 1 / 0 of the v3
    ladder).
    """

    def test_a_merge_folds_its_four_childrens_artifacts(self, tmp_path):
        from zagg.sweep_overview import decode_digest, fold_digests

        m = _wide_store(tmp_path / "s")
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["failed"] == 0
        # d = 2: node 2 carries 4 and node 1 carries 3 (gathers); node 0 —
        # the base cell '1' — carries cells 2, the one merge level, folded
        # from the (1, 3) artifacts: parent cell 0 ('111') is child '11'
        # artifact's cells 0..3 ('1111'..'1114'), ONE k-way call, 4-to-one.
        level = _artifact(tmp_path / "s", "1/all.zarr")
        attrs = dict(level.attrs)["zagg_overview"]
        assert attrs["regime"] == "stage-merge" and attrs["merges_from_raw"] == 3
        child = _artifact(tmp_path / "s", "1/1/all.zarr")["3"]
        parts = [decode_digest(p, "float32") for p in child["h_tdigest"][:4] if len(p)]
        assert bytes(level["2"]["h_tdigest"][:][0]) == fold_digests(
            parts, delta=16, dtype="float32"
        )
        assert list(level["2"]["count"][:]) == [int(child["count"][:4].sum()), 2080 * 3] + [0] * 14
        assert attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 0}

    def test_a_gather_records_the_depth_of_the_groups_it_concatenates(self, tmp_path):
        # The leaf columns of ``_wide_store`` carry {5, 4, 3} with boundary 5,
        # so members 4 and 3 are at depth 2 (issue #538): the gathers at
        # cells 4 and 3 copy them untouched and record 2, the stage column
        # relaying member 3 records 2 for it, and the one merge records 3.
        m = _wide_store(tmp_path / "w")
        summary = _sweep(tmp_path / "w", m, width=1)
        assert all(s["failed"] == 0 for s in summary["stages"])
        depths = {
            rel: dict(_artifact(tmp_path / "w", rel).attrs)["zagg_overview"]["merges_from_raw"]
            for rel in ("1/1/1/all.zarr", "1/1/all.zarr", "1/all.zarr")
        }
        assert depths == {"1/1/1/all.zarr": 2, "1/1/all.zarr": 2, "1/all.zarr": 3}
        column = dict(_artifact(tmp_path / "w", "1/1/1/all.pyramid.zarr").attrs)["zagg_column"]
        assert column["groups"]["3"]["merges_from_raw"] == 2
        assert {k: v["merges_from_raw"] for k, v in summary["levels"].items()} == {
            "2": 2,
            "1": 2,
            "0": 3,
        }

    def test_a_source_at_another_cell_order_is_unreadable_not_folded(self, tmp_path, caplog):
        # Another geometry's child artifact has no group at the cells the
        # merge reads: it would fold as all-fill, so it counts unreadable.
        from zagg.store import open_object_store
        from zagg.sweep_overview import _candidate_decimals
        from zagg.sweep_stage import one_stage_tuple, stage_node

        root = tmp_path / "s"
        m = _stage_store(root)
        _sweep(root, m)
        g = zarr.open_group(open_store(str(root / "1/1/2/all.zarr")), mode="r+", zarr_format=3)
        g.attrs["zagg_overview"] = {**dict(g.attrs)["zagg_overview"], "cell_order": 4}
        candidates, _ = _candidate_decimals(str(root), 3, _by_shard(), {})
        counts = dict.fromkeys(("written", "current", "empty", "failed", "under_covered"), 0)
        counts["revalidated"] = 0
        stage_node(
            open_object_store(str(root)),
            str(root),
            "11",
            one_stage_tuple(3, 1, 2),
            ladder_entries(m["pyramid"], 3),
            FIELDS,
            key="all",
            window=None,
            windowed=False,
            shard_order=3,
            cell_order=5,
            candidates=candidates,
            run_id="B",
            run_started=_utcnow(),
            counts=counts,
            store_kwargs={},
        )
        assert counts["written"] == 1
        attrs = dict(_artifact(root, "1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 1}
        assert "records cell_order 4, not 3" in caplog.text

    def test_the_depth_is_recorded_truthfully_at_every_level(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        summary = _sweep(tmp_path / "s", m)
        depths = {
            rel: dict(_artifact(tmp_path / "s", rel).attrs)["zagg_overview"]["merges_from_raw"]
            for rel in ("1/1/1/all.zarr", "1/1/all.zarr", "1/all.zarr")
        }
        # Gather 1; the (1, 2) cascade of gathers 2; the (0, 1) cascade of
        # those 3 — one more than its sources, never pinned to 2.
        assert depths == {"1/1/1/all.zarr": 1, "1/1/all.zarr": 2, "1/all.zarr": 3}
        actuals = {k: v["merges_from_raw"] for k, v in summary["levels"].items()}
        assert actuals == {"2": 1, "1": 2, "0": 3}

    def test_per_node_reads_are_constant_in_the_subtree_and_the_depth(self, tmp_path):
        reads = {}
        for name, leaves in (("16", DENSE_16), ("32", DENSE_32)):
            m = _stage_store(tmp_path / name, leaves=leaves)
            summary = _sweep(tmp_path / name, m, width=1, leaves=leaves)
            assert all(s["failed"] == 0 for s in summary["stages"])
            reads[name] = _per_node_reads(summary)
        # The gather tier reads the leaves (once, for the whole pyramid)...
        assert reads["32"][2] == 2 * reads["16"][2]
        # ...and above it every merge node reads its four children's 4-cell
        # slabs, two fields: 32 cells at order 1 AND at order 0, with one or
        # two leaves per order-2 node alike.
        assert reads["16"][1] == reads["16"][0] == reads["32"][1] == reads["32"][0] == 32

    def test_the_windowed_units_hold_the_same_bound(self, tmp_path):
        reads = {}
        for name, leaves in (("16", DENSE_16), ("32", DENSE_32)):
            root = tmp_path / name
            m = TestWindowedStageSweep()._windowed_store(root, leaves=leaves)
            by_shard = {d: set(TestWindowedStageSweep.WINDOWS) for d in leaves}
            summary = sweep_stage_pass(str(root), m, by_shard, run_id="A", tuple_width=1)
            assert all(s["failed"] == 0 for s in summary["stages"])
            reads[name] = _per_node_reads(summary)
        # Per node: two window units of 32 cells each, plus the close (two
        # 4-cell window overviews, two fields) — 80, at both merge orders.
        assert reads["16"][1] == reads["16"][0] == reads["32"][1] == reads["32"][0] == 80

    def test_count_is_the_exact_sum_of_the_leaves_at_every_level(self, tmp_path):
        m = _stage_store(tmp_path / "s", leaves=DENSE_32)
        _sweep(tmp_path / "s", m, leaves=DENSE_32)
        leaf_sum = {dec: 136 * (i + 1) for i, dec in enumerate(DENSE_32)}
        for rel, node, r in (
            ("1/all.zarr", "1", 1),
            ("1/2/all.zarr", "12", 2),
            ("1/3/4/all.zarr", "134", 3),
        ):
            counts = list(_artifact(tmp_path / "s", rel)[str(r)]["count"][:])
            cells = [node + d for d in "1234"]
            assert counts == [
                sum(v for dec, v in leaf_sum.items() if dec.startswith(c)) for c in cells
            ]

    def test_the_pass_and_a_direct_stage_node_call_write_the_same_ladder(self, tmp_path):
        """One entry point: the local-backend pass is ``stage_node`` per dispatch node."""
        from zagg.store import open_object_store
        from zagg.sweep_overview import _candidate_decimals
        from zagg.sweep_stage import _node_at, aggregate_actuals, stage_node

        passed, direct = tmp_path / "pass", tmp_path / "direct"
        m = _stage_store(passed)
        _stage_store(direct)
        summary = _sweep(passed, m, run_id="R")
        (stage,) = stage_tuples(3)
        candidates, _ = _candidate_decimals(str(direct), 3, _by_shard(), {})
        counts = dict.fromkeys(
            (
                "written",
                "current",
                "empty",
                "failed",
                "under_covered",
                "columns_written",
                "columns_current",
                "revalidated",
            ),
            0,
        )
        actuals: dict = {}
        for node in sorted({_node_at(d, 0) for d in candidates}):
            stage_node(
                open_object_store(str(direct)),
                str(direct),
                node,
                stage,
                ladder_entries(m["pyramid"], 3),
                FIELDS,
                key="all",
                window=None,
                windowed=False,
                shard_order=3,
                cell_order=5,
                candidates=candidates,
                run_id="R",
                run_started=_utcnow(),
                counts=counts,
                store_kwargs={},
                level_actuals=actuals,
            )
        assert counts["written"] == 7 == summary["stages"][0]["written"]
        a, b = _ladder_arrays(passed), _ladder_arrays(direct)
        assert set(a) == set(b) and a
        for rel in a:
            for name in a[rel]:
                assert (
                    _payload_equal(a[rel][name], b[rel][name])
                    if a[rel][name].dtype == object
                    else np.array_equal(a[rel][name], b[rel][name])
                ), (rel, name)
            x = dict(_artifact(passed, rel).attrs)["zagg_overview"]
            y = dict(_artifact(direct, rel).attrs)["zagg_overview"]
            assert {k: v for k, v in x.items() if k != "generated_at"} == {
                k: v for k, v in y.items() if k != "generated_at"
            }, rel
        assert {str(k): v for k, v in aggregate_actuals(actuals).items()} == summary["levels"]


class TestStagePass:
    def test_first_pass_materializes_the_ladder(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        summary = _sweep(tmp_path / "s", m)
        (row,) = summary["stages"]
        # order 2: 111, 112, -21; order 1: 11, -2; order 0: 1, -2.
        assert row["written"] == 7
        assert row["failed"] == 0 and row["under_covered"] == 0
        g = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        attrs = dict(g.attrs)["zagg_overview"]
        assert attrs["spec"] == "zagg-overview/2"
        assert attrs["regime"] == "stage-gather" and attrs["merges_from_raw"] == 1
        assert attrs["source_children"] == {"folded": 2, "missing": 0, "unreadable": 0}
        assert attrs["run_id"] == "A"
        # The merges count their children's ARTIFACTS: '1' has one order-1
        # child ('11'); its depth is one more than the (1, 2) cascade's.
        root_attrs = dict(_artifact(tmp_path / "s", "1/all.zarr").attrs)["zagg_overview"]
        assert root_attrs["regime"] == "stage-merge" and root_attrs["merges_from_raw"] == 3
        assert root_attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 0}

    def test_child_columns_read_through_the_invoke_handle(self, tmp_path, monkeypatch):
        """Issue #610: no stage unit opens a store per child column it reads."""
        import zagg.store as store_mod

        m = _stage_store(tmp_path / "s")
        opened, real = [], store_mod.open_store

        def counting(path, *a, **k):
            if k.get("read_only"):  # a read; the stage-column writes open their own
                opened.append(path)
            return real(path, *a, **k)

        monkeypatch.setattr(store_mod, "open_store", counting)
        summary = _sweep(tmp_path / "s", m, width=1)  # every tuple reads child columns
        assert all(s["failed"] == 0 and s["written"] for s in summary["stages"])
        assert not [p for p in opened if p.endswith(".pyramid.zarr")]

    def test_exact_fold_math(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        # Node-order partial of leaf i is sum(1..16)*(i+1) = 136*(i+1).
        g = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        assert list(g["3"]["count"][:]) == [136, 272, 0, 0]
        g = _artifact(tmp_path / "s", "1/all.zarr")
        assert list(g["1"]["count"][:]) == [136 + 272 + 408, 0, 0, 0]

    def test_gather_carries_gen1_bytes_untouched(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        leaf = _artifact(tmp_path / "s", "1/1/1/1/all.pyramid.zarr")
        parent = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        # '1111' ranks 0 under '111': its node-order payload IS parent cell 0.
        # (vlen arrays are read [:]-first: scalar indexing yields a 0-d view.)
        leaf_payloads = leaf["3"]["h_tdigest"][:]
        parent_payloads = parent["3"]["h_tdigest"][:]
        assert bytes(parent_payloads[0]) == bytes(leaf_payloads[0]) != b""
        assert bytes(parent_payloads[2]) == b""  # uninhabited: fill

    def test_second_pass_is_current(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        (row,) = _sweep(tmp_path / "s", m, run_id="B")["stages"]
        assert row["written"] == 0 and row["current"] == 7

    def test_ratchet_rewrites_on_child_change(self, tmp_path, monkeypatch):
        import zagg.hive as hive_mod

        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        # Rewrite one leaf column with new content, stamped one hour later
        # (stamps resolve to seconds; the ratchet keys on the generation key).
        later = (datetime.fromisoformat(_utcnow()) + timedelta(hours=1)).isoformat(
            timespec="seconds"
        )
        monkeypatch.setattr(hive_mod, "_utcnow", lambda: later)
        _write_leaf(tmp_path / "s", "1111", 9)
        monkeypatch.undo()
        (row,) = _sweep(tmp_path / "s", m, run_id="B")["stages"]
        assert row["written"] > 0
        g = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        assert list(g["3"]["count"][:])[0] == 136 * 10

    def test_width_1_writes_only_the_columns_a_coarser_gather_needs(self, tmp_path):
        # d = 1: nothing above order 2 gathers, so no dispatch node writes a
        # column at all — the [1] and [0] tuples read artifacts alone.
        m = _stage_store(tmp_path / "s")
        summary = _sweep(tmp_path / "s", m, width=1)
        cols = [(s["dispatch_order"], s["columns_written"]) for s in summary["stages"]]
        assert cols == [(2, 0), (1, 0), (0, 0)]
        assert not [p for p in (tmp_path / "s").rglob("*.pyramid.zarr") if len(p.parts) < 4]
        # d = 2: the (1, 3) gather needs its order-2 children's member 3, so
        # the order-2 dispatch nodes write one; the order-1 nodes still none.
        m = _wide_store(tmp_path / "w")
        summary = _sweep(tmp_path / "w", m, width=1)
        cols = [(s["dispatch_order"], s["columns_written"]) for s in summary["stages"]]
        assert cols == [(2, 3), (1, 0), (0, 0)]
        g = _artifact(tmp_path / "w", "1/1/1/all.pyramid.zarr")
        attrs = dict(g.attrs)["zagg_column"]
        assert attrs["groups"] == {
            "3": {"regime": "stage-gather", "merges_from_raw": 2, "n_cells": 4}
        }
        assert attrs["generation"]["n_leaves"] == 2
        # '1111'/'1112' node members at ranks 0 and 1 of 4 (sum(1..64) = 2080).
        assert list(g["3"]["count"][:]) == [2080, 4160, 0, 0]
        assert not (tmp_path / "w" / "1" / "1" / "all.pyramid.zarr").exists()

    def test_stage_column_stamps_and_sidecars_the_same_o11_record(self, tmp_path):
        # Issue #580: the stage column is the sixth stamp writer, and it
        # records the §5.3 content-hash record in BOTH planes — the stamp it
        # rides and the D20 sidecar beside it — from one computation, so the
        # two can never disagree. It carries the finished record, never the
        # staged slabs (those are not JSON-serializable, and the fail-open
        # sidecar would swallow the TypeError into a silent drop).
        from zagg.hive import read_commit

        m = _wide_store(tmp_path / "s")
        _sweep(tmp_path / "s", m, width=1)
        column = tmp_path / "s" / "1" / "1" / "1"
        stamp = read_commit(open_store(str(column / "all.pyramid.zarr")))
        record = json.loads((column / "all.pyramid.stats.json").read_text())
        hashes = stamp["content_hashes"]
        assert hashes == record["content_hashes"]
        # The gathered member and its morton sibling are both covered.
        assert set(hashes["arrays"]) >= {"3/morton", "3/count", "3/h_tdigest"}
        assert len(hashes["combined"]) == 64


class TestLocatedStageSweep:
    """The staged sweep's §9 located paths (ruling 4 on issue #410).

    ``_gather_slabs``' sibling prealloc and read loop, ``_merge_slabs``' per-cell
    pair accumulation and its open-boundary close, and ``write_stage_column``'s
    sibling write-through — none of which had coverage, which is precisely where
    the half-read-pair bug lived (review finding).
    """

    def _words(self, group, res, name="h_tdigest", sibling="locations"):
        from zagg.sweep_overview import decode_digest

        payloads = group[str(res)][name][:]
        siblings = group[str(res)][f"{name}_{sibling}"][:]
        return [
            (
                decode_digest(bytes(p or b""), "float32"),
                decode_digest(bytes(s or b""), "uint64", ()),
            )
            for p, s in zip(payloads, siblings, strict=True)
        ]

    def _assert_row_aligned(self, group, res, name="h_tdigest"):
        from mortie import validate_morton

        seen = 0
        for payload, words in self._words(group, res, name):
            assert words.shape == (payload.shape[0],), "§1.1: one word per centroid row"
            if len(words):
                validate_morton(words)
                seen += 1
        assert seen, "no populated rows -- the assertion would be vacuous"

    def test_the_ladder_carries_the_channel_row_aligned(self, tmp_path):
        m = _stage_store(tmp_path / "s", fields=LOCATED_FIELDS)
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["written"] == 7 and row["failed"] == 0
        # A gather level (order 2, cells 3) and a merge level (order 0, cells 1).
        self._assert_row_aligned(_artifact(tmp_path / "s", "1/1/1/all.zarr"), 3)
        self._assert_row_aligned(_artifact(tmp_path / "s", "1/all.zarr"), 1)

    def test_the_gather_assigns_gen1_words_untouched(self, tmp_path):
        m = _stage_store(tmp_path / "s", fields=LOCATED_FIELDS)
        _sweep(tmp_path / "s", m)
        leaf = _artifact(tmp_path / "s", "1/1/1/1/all.pyramid.zarr")
        parent = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        src = leaf["3"]["h_tdigest_locations"][:]
        out = parent["3"]["h_tdigest_locations"][:]
        assert bytes(out[0]) == bytes(src[0]) != b""
        assert bytes(out[2]) == b""  # uninhabited: fill on the sibling too

    def test_the_merge_words_contain_their_contributors(self, tmp_path):
        from mortie import common_ancestor

        m = _stage_store(tmp_path / "s", fields=LOCATED_FIELDS)
        _sweep(tmp_path / "s", m)
        # The root merge over node '1' folds the three '1***' leaves' relay
        # partials into one cell; every output word must contain its sources'.
        merged = self._words(_artifact(tmp_path / "s", "1/all.zarr"), 1)[0][1]
        sources = []
        for dec in ("1111", "1112", "1121"):
            col = _artifact(tmp_path / "s", f"1/{dec[1]}/{dec[2]}/{dec[3]}/all.pyramid.zarr")
            for _payload, words in self._words(col, 3):
                sources.extend(words.tolist())
        assert len(merged) and sources
        hull = int(common_ancestor(np.asarray(sorted(set(sources)), dtype=np.uint64)))
        for word in merged.tolist():
            assert int(common_ancestor(np.asarray([hull, word], dtype=np.uint64))) == hull

    def _drop_member(self, root, rel, res, name):
        """Delete one array member from a written column (schema evolution)."""
        import shutil

        shutil.rmtree(root / rel / str(res) / name)

    def test_a_gather_source_without_its_channel_is_not_written_half(self, tmp_path):
        # The bug this class exists for: ``_StageReader.read`` returns None per
        # ARRAY, so a column predating the ``location:`` declaration reads its
        # payload fine and its sibling as None. Assigning the payload anyway
        # would commit populated rows against b"" words (spec §9.1/§1.1).
        root = tmp_path / "s"
        m = _stage_store(root, fields=LOCATED_FIELDS)
        self._drop_member(root, "1/1/1/1/all.pyramid.zarr", 3, "h_tdigest_locations")
        (row,) = _sweep(root, m)["stages"]
        assert row["failed"] == 0, "one stale child must not fail the level"
        g = _artifact(root, "1/1/1/all.zarr")
        payloads = g["3"]["h_tdigest"][:]
        assert bytes(payloads[0]) == b"", "the refused contributor's span stays fill"
        assert bytes(payloads[1]) != b"", "its sibling child still gathers"
        self._assert_row_aligned(g, 3)
        # ... and the artifact SAYS it folded short rather than reporting clean.
        counts = dict(g.attrs)["zagg_overview"]["source_children"]
        assert counts == {"folded": 1, "missing": 0, "unreadable": 1}
        # The skip is PER FIELD: the read succeeded, so the exact class -- which
        # has no pair contract -- still folds that contributor, and the per-child
        # ``unreadable`` count is what says the artifact folded short.
        assert list(g["3"]["count"][:]) == [136, 272, 0, 0]

    def test_a_merge_source_without_its_channel_is_skipped_not_raised(self, tmp_path):
        # A leaf column missing its sibling is refused by the GATHER that
        # reads it (counted unreadable there, its exact class still folded);
        # the merges above cascade the gather's committed artifact, which is
        # whole, so they fold clean, row-aligned and short of nothing.
        root = tmp_path / "s"
        m = _stage_store(root, fields=LOCATED_FIELDS)
        self._drop_member(root, "1/1/1/1/all.pyramid.zarr", 3, "h_tdigest_locations")
        (row,) = _sweep(root, m)["stages"]
        assert row["failed"] == 0 and row["written"] == 7
        gather = dict(_artifact(root, "1/1/1/all.zarr").attrs)["zagg_overview"]
        assert gather["source_children"] == {"folded": 1, "missing": 0, "unreadable": 1}
        g = _artifact(root, "1/1/all.zarr")
        assert dict(g.attrs)["zagg_overview"]["regime"] == "stage-merge"
        self._assert_row_aligned(g, 2)
        assert dict(g.attrs)["zagg_overview"]["source_children"] == {
            "folded": 2,
            "missing": 0,
            "unreadable": 0,
        }
        # The refused contributor is absent from the digest but present in the
        # exact fold, and the pair it did write is aligned.
        assert list(g["2"]["count"][:]) == [136 + 272, 408, 0, 0]

    def test_two_located_fields_close_independently(self, tmp_path):
        # ``_close`` closes over ``pending``/``words`` BY NAME, and both are
        # rebound each field iteration; a second located field pins that.
        m = _stage_store(tmp_path / "s", fields=TWO_LOCATED_FIELDS)
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["written"] == 7 and row["failed"] == 0
        merged = _artifact(tmp_path / "s", "1/all.zarr")
        for name in ("h_tdigest", "h2_tdigest"):
            self._assert_row_aligned(merged, 1, name)
        # The two fields carry identical inputs, so identical bytes -- a
        # cross-field leak through the closures would break that.
        first = self._words(merged, 1, "h_tdigest")[0]
        second = self._words(merged, 1, "h2_tdigest")[0]
        np.testing.assert_array_equal(first[1], second[1])

    def test_the_stage_column_writes_the_sibling(self, tmp_path):
        # ``write_stage_column``'s write-through: the member a parent gather
        # consumes must carry the channel, or the next tier reads a payload
        # with no words.
        from zagg.grids.base import located_declaration

        m = _wide_store(tmp_path / "s", fields=LOCATED_FIELDS)
        _sweep(tmp_path / "s", m, width=1)
        col = _artifact(tmp_path / "s", "1/1/1/all.pyramid.zarr")
        assert "h_tdigest_locations" in col["3"]
        self._assert_row_aligned(col, 3)
        assert located_declaration(dict(col["3"]["h_tdigest_locations"].attrs)) is not None
        assert dict(col["3"]["h_tdigest"].attrs)["ragged"]["locations"] == "h_tdigest_locations"

    def test_an_unlocated_store_is_byte_identical(self, tmp_path):
        # The channel is opt-in: the ladder of an unlocated declaration must be
        # exactly what it was before ruling 4.
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        g = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        assert "h_tdigest_locations" not in g["3"]
        assert "locations" not in dict(g["3"]["h_tdigest"].attrs)["ragged"]


class TestBothChannelsStageSweep:
    """One field, TWO channels, through the staged ladder (issue #410).

    Every located test above declares a single channel, where the ``channels=``
    map has one key and ``_companion_group``'s partial rule degenerates to the
    old two-array shape. These declare both on ``h_tdigest``, so
    ``_gather_slabs``' prealloc/read loop and ``_merge_slabs``' per-cell
    accumulation carry a 3-array group — and each sibling is checked against its
    OWN grammar, which is what catches a channel dropped, folded separately, or
    swapped with the other (the two word grammars accept each other's words, so
    a swap raises nowhere by itself).
    """

    _words = TestLocatedStageSweep._words

    def _pairs(self, group, res, sibling):
        return [(p, w) for p, w in self._words(group, res, sibling=sibling) if len(p)]

    def test_the_ladder_carries_both_channels_row_aligned(self, tmp_path):
        from mortie import toc_is_range, validate_morton

        m = _stage_store(tmp_path / "s", fields=BOTH_CHANNEL_FIELDS)
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["written"] == 7 and row["failed"] == 0
        # A gather level (order 2) and a merge level (order 0).
        for rel, res in (("1/1/1/all.zarr", 3), ("1/all.zarr", 1)):
            g = _artifact(tmp_path / "s", rel)
            locs = self._pairs(g, res, "locations")
            times = self._pairs(g, res, "times")
            assert locs and len(locs) == len(times)
            for (payload, lw), (_p, tw) in zip(locs, times, strict=True):
                assert lw.shape == tw.shape == (payload.shape[0],), "§1.1, both siblings"
                validate_morton(lw)
                toc_is_range(tw)  # a well-formed word in its own grammar
                assert not np.array_equal(lw, tw)

    def test_the_gather_relays_each_channel_into_its_own_slot(self, tmp_path):
        # The gather assigns a gen-1 child's bytes through untouched, so the
        # parent's two siblings must be the leaf's two siblings — not each
        # other's. A swap here is byte-detectable, which is the point.
        m = _stage_store(tmp_path / "s", fields=BOTH_CHANNEL_FIELDS)
        _sweep(tmp_path / "s", m)
        leaf = _artifact(tmp_path / "s", "1/1/1/1/all.pyramid.zarr")
        parent = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        for sibling in ("locations", "times"):
            src = leaf["3"][f"h_tdigest_{sibling}"][:]
            out = parent["3"][f"h_tdigest_{sibling}"][:]
            assert bytes(out[0]) == bytes(src[0]) != b""
            assert bytes(out[2]) == b""  # uninhabited: fill on both siblings
        assert bytes(parent["3"]["h_tdigest_locations"][:][0]) != bytes(
            parent["3"]["h_tdigest_times"][:][0]
        )

    def test_the_merge_satisfies_both_containment_claims(self, tmp_path):
        # The root merge folds the three '1***' leaves' relay partials into one
        # cell. Its located words must ENCLOSE their sources' (§9.1) and its toc
        # words must COVER their sources' instants (§8.3) — one partition, two
        # claims, and neither holds for the other channel's bytes.
        from mortie import common_ancestor

        from zagg.stats.toc import cell_envelope

        m = _stage_store(tmp_path / "s", fields=BOTH_CHANNEL_FIELDS)
        _sweep(tmp_path / "s", m)
        merged = _artifact(tmp_path / "s", "1/all.zarr")
        src_l, src_t = [], []
        for dec in ("1111", "1112", "1121"):
            col = _artifact(tmp_path / "s", f"1/{dec[1]}/{dec[2]}/{dec[3]}/all.pyramid.zarr")
            for _p, w in self._pairs(col, 3, "locations"):
                src_l.extend(w.tolist())
            for _p, w in self._pairs(col, 3, "times"):
                src_t.extend(w.tolist())
        out_l = self._words(merged, 1, sibling="locations")[0][1]
        out_t = self._words(merged, 1, sibling="times")[0][1]
        assert len(out_l) and len(out_t) and src_l and src_t
        hull = int(common_ancestor(np.asarray(sorted(set(src_l)), dtype=np.uint64)))
        for word in out_l.tolist():
            assert int(common_ancestor(np.asarray([hull, word], dtype=np.uint64))) == hull
        span = int(cell_envelope(np.asarray(src_t, dtype=np.uint64)))
        assert int(cell_envelope(out_t)) == span, "§8.3 cell-level envelope identity"
        # The located bytes do NOT satisfy the temporal claim, which is exactly
        # what a swapped slot would have to.
        assert int(cell_envelope(np.asarray(out_l, dtype=np.uint64))) != span

    def test_one_channel_missing_refuses_the_whole_group(self, tmp_path):
        # ``_companion_group``'s partial rule at the arity it was added for:
        # payload + ONE of two siblings present. The half-group must be refused
        # whole, never written as a payload with one channel row-aligned and the
        # other at b"" (spec §1.1).
        root = tmp_path / "s"
        m = _stage_store(root, fields=BOTH_CHANNEL_FIELDS)
        import shutil

        shutil.rmtree(root / "1/1/1/1/all.pyramid.zarr" / "3" / "h_tdigest_times")
        (row,) = _sweep(root, m)["stages"]
        assert row["failed"] == 0, "one stale child must not fail the level"
        g = _artifact(root, "1/1/1/all.zarr")
        payloads = g["3"]["h_tdigest"][:]
        assert bytes(payloads[0]) == b"", "the refused contributor stays fill"
        intact = g["3"]["h_tdigest_locations"][:]
        assert bytes(intact[0]) == b"", "and so does its INTACT sibling"
        assert bytes(payloads[1]) != b"", "its sibling child still gathers"
        assert dict(g.attrs)["zagg_overview"]["source_children"] == {
            "folded": 1,
            "missing": 0,
            "unreadable": 1,
        }

    def test_a_timed_only_field_folds_through_the_ladder(self, tmp_path):
        # A digest field with a clock and no ``location:`` — the temporal channel
        # on its own, which no other test declares.
        m = _stage_store(tmp_path / "s", fields=TIMED_FIELDS)
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["written"] == 7 and row["failed"] == 0
        g = _artifact(tmp_path / "s", "1/all.zarr")
        assert "h_tdigest_locations" not in g["1"]
        pairs = self._pairs(g, 1, "times")
        assert pairs
        for payload, words in pairs:
            assert words.shape == (payload.shape[0],)


class TestSameSecondSkipGate:
    """Issue #417: the skip key carries the stamping RUN IDS, not the timestamp
    alone. ``hive._utcnow`` resolves to one second, so a child rewritten inside
    the same second at an unchanged leaf count moved neither term of the old
    ``(n_leaves, max_leaf_timestamp)`` pair — the gate read it as current and
    served the stale fold."""

    def test_same_second_foreign_rewrite_is_refolded(self, tmp_path):
        root = tmp_path / "s"
        m = _stage_store(root)
        _sweep(root, m)
        leaf = "1/1/1/1/all.pyramid.zarr"
        was = dict(_artifact(root, leaf).attrs)["morton_hive_commit"]["written_at"]
        # A DIFFERENT run rewrites the column with new content and restamps it
        # inside the recorded second: leaf count and timestamp both unmoved.
        _write_leaf(root, "1111", 9)
        _restamp(root, leaf, written_at=was, run_id="fleet-2")
        assert dict(_artifact(root, leaf).attrs)["morton_hive_commit"]["written_at"] == was
        (row,) = _sweep(root, m, run_id="B")["stages"]
        assert row["written"] > 0
        # The parent carries the REWRITE's partial (136 * 10), not the stale one,
        # and so does every level above it (the gate is per artifact node, so a
        # partial re-fold would satisfy the count alone — review finding).
        assert list(_artifact(root, "1/1/1/all.zarr")["3"]["count"][:])[0] == 136 * 10
        assert list(_artifact(root, "1/1/all.zarr")["2"]["count"][:])[0] == 136 * 10 + 272
        assert list(_artifact(root, "1/all.zarr")["1"]["count"][:])[0] == 136 * 10 + 272 + 408

    def test_same_second_foreign_rewrite_of_a_stage_column_is_refolded(self, tmp_path):
        """The arm the gather tier rests on. At ``width=1`` on the d = 2 store
        the (1, 3) gather's children are STAGE columns, whose stamps carry a
        real ``run_id`` (:func:`zagg.sweep_stage.write_stage_column` writes it)
        — so this arm needs no injected grammar for the id, only the
        same-second restamp; the merge above cascades the re-gathered artifact
        through its own run-id term."""
        root = tmp_path / "s"
        m = _wide_store(root)
        _sweep(root, m, width=1)
        col = "1/1/1/all.pyramid.zarr"
        was = dict(_artifact(root, col).attrs)["morton_hive_commit"]["written_at"]
        before = list(_artifact(root, "1/1/all.zarr")["3"]["count"][:])
        root_before = list(_artifact(root, "1/all.zarr")["2"]["count"][:])
        # A foreign run rewrites the order-2 column's gathered member inside
        # its own recorded second.
        g = zarr.open_group(open_store(str(root / col)), path="", mode="r+", zarr_format=3)
        g["3"]["count"][0] = 1000
        _restamp(root, col, written_at=was, run_id="C")
        _sweep(root, m, width=1, run_id="B")
        after = list(_artifact(root, "1/1/all.zarr")["3"]["count"][:])
        root_after = list(_artifact(root, "1/all.zarr")["2"]["count"][:])
        # The '1111' cell re-gathers the tampered member (1000) where it held
        # 2080, and the root cell '111' re-folds the re-gathered artifact.
        assert before[0] == 2080 and after[0] == 1000
        assert root_before[0] == 2080 + 4160 and root_after[0] == 1000 + 4160

    def test_entry_without_run_ids_stays_current(self, tmp_path):
        """The upgrade path: a pre-#417 entry keys on the empty run-id set, and
        fleet-written leaf columns carry no run id, so a gather level whose
        entry lost the key still reads as current. A merge level's sources are
        stage-written artifacts, whose run id IS a term of the key."""
        root = tmp_path / "s"
        m = _stage_store(root)
        _sweep(root, m)
        for path in root.rglob(ENVELOPE_NAME):
            envelope = json.loads(path.read_text())
            if envelope["order"] != 2:
                continue
            for entry in envelope["windows"].values():
                assert entry["generation"].pop("run_ids") == []
            path.write_text(json.dumps(envelope, indent=1))
        (row,) = _sweep(root, m, run_id="B")["stages"]
        assert row["written"] == 0 and row["current"] == 7


class TestCascadeLaw:
    def test_identity_across_tuple_widths(self, tmp_path):
        """Grouping changes no values: in memory or off the store, the same four slabs."""
        m1 = _stage_store(tmp_path / "w1")
        m3 = _stage_store(tmp_path / "w3")
        _sweep(tmp_path / "w1", m1, width=1, run_id="W1")
        _sweep(tmp_path / "w3", m3, width=3, run_id="W3")
        a1, a3 = _ladder_arrays(tmp_path / "w1"), _ladder_arrays(tmp_path / "w3")
        assert set(a1) == set(a3) and a1
        for rel in a1:
            for name in a1[rel]:
                x, y = a1[rel][name], a3[rel][name]
                if x.dtype == object:
                    assert _payload_equal(x, y), (rel, name)
                else:
                    assert np.array_equal(x, y), (rel, name)

    def test_a_merge_reads_its_childrens_artifacts_and_nothing_below(self, tmp_path, monkeypatch):
        """Store reads: a merge opens the artifacts one order finer, never a column or leaf."""
        from pathlib import Path

        reads = []
        orig = stage_mod._ColumnReader._read

        def spy(self, res, name, cells):
            reads.append((self.path, res))  # the store-relative key
            return orig(self, res, name, cells)

        monkeypatch.setattr(stage_mod._ColumnReader, "_read", spy)
        m = _stage_store(tmp_path / "w1")
        _sweep(tmp_path / "w1", m, width=1)
        by_order = {}
        for p, r in reads:
            parts = Path(p).parts
            by_order.setdefault((len(parts) - 2, parts[-1]), set()).add(r)
        # Width 1: the [2] tuple gathers the leaf columns' member 3; the [1]
        # tuple reads the order-2 artifacts' member 3; the [0] tuple the
        # order-1 artifacts' member 2 — and nothing else.
        assert by_order == {
            (3, "all.pyramid.zarr"): {3},
            (2, "all.zarr"): {3},
            (1, "all.zarr"): {2},
        }
        # Width 3: one unit holds the whole ladder, so the coarser orders fold
        # the slabs just written and the store is read at the leaf tier alone.
        reads.clear()
        m = _stage_store(tmp_path / "w3")
        _sweep(tmp_path / "w3", m, width=3)
        assert {(len(Path(p).parts) - 2, Path(p).parts[-1], r) for p, r in reads} == {
            (3, "all.pyramid.zarr", 3)
        }


class TestSoftBarrier:
    def test_missing_column_under_covers_loudly_then_heals(self, tmp_path):
        m = _stage_store(tmp_path / "s", skip_columns={"1121"})
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["under_covered"] > 0
        # '112' has no usable child at all -> empty (no artifact); the order-1
        # node above it records the under-coverage in its own attrs.
        assert not (tmp_path / "s" / "1" / "1" / "2" / "all.zarr").exists()
        attrs = dict(_artifact(tmp_path / "s", "1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"]["missing"] == 1
        # The fleet lands the missing leaf; the ratchet heals end-to-end.
        _write_leaf(tmp_path / "s", "1121", 2)
        (row,) = _sweep(tmp_path / "s", m, run_id="B")["stages"]
        assert row["written"] > 0
        attrs = dict(_artifact(tmp_path / "s", "1/1/2/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 0}
        attrs = dict(_artifact(tmp_path / "s", "1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"]["missing"] == 0
        g = _artifact(tmp_path / "s", "1/all.zarr")
        assert list(g["1"]["count"][:])[0] == 136 + 272 + 408


class TestScopedSweep:
    def test_scope_dispatches_ancestor_prefixes_only(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        summary = _sweep(tmp_path / "s", m, scope=normalize_scope(["1111"]))
        (row,) = summary["stages"]
        assert row["nodes"] == 1  # base cell '1' alone; '-2' is out of scope
        assert not (tmp_path / "s" / "-2" / "all.zarr").exists()

    def test_scoped_update_folds_old_neighbors_in(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        # A NEON-adjacent append: one new leaf lands next to old data.
        _write_leaf(tmp_path / "s", "1122", 4)
        write_root_coverage(
            str(tmp_path / "s"),
            build_root_coverage([morton_word(d) for d in LEAVES + ["1122"]], 3),
        )
        summary = sweep_stage_pass(
            str(tmp_path / "s"),
            m,
            {"1122": {None}},
            run_id="B",
            scope=normalize_scope(["1122"]),
        )
        (row,) = summary["stages"]
        assert row["nodes"] == 1
        g = _artifact(tmp_path / "s", "1/1/2/all.zarr")
        attrs = dict(g.attrs)["zagg_overview"]
        # Old neighbor '1121' folded in beside the appended '1122'.
        assert attrs["source_children"]["folded"] == 2
        assert list(g["3"]["count"][:]) == [408, 680, 0, 0]

    def test_clean_nodes_noop_under_scope(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        summary = _sweep(tmp_path / "s", m, run_id="B", scope=normalize_scope(["-2111"]))
        (row,) = summary["stages"]
        assert row["nodes"] == 1 and row["written"] == 0 and row["current"] > 0


class TestConcurrencyBackstops:
    def test_foreign_fresh_stamp_aborts_loudly(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _sweep(tmp_path / "s", m)
        # Inject a foreign FRESH stamp on a stage artifact (the #380 spy
        # pattern's injection arm): a zombie sibling sweep's write.
        store = open_store(str(tmp_path / "s" / "1" / "1" / "1" / "all.zarr"))
        g = zarr.open_group(store, path="", mode="r+", zarr_format=3)
        stamp = dict(g.attrs["morton_hive_commit"])
        stamp["run_id"] = "zombie"
        stamp["written_at"] = "299-01-01T00:00:00+00:00".replace("299", "2999")
        g.attrs["morton_hive_commit"] = stamp
        with pytest.raises(ForeignSweepError, match="zombie"):
            _sweep(tmp_path / "s", m, run_id="B")

    def test_stamp_validation_rereads_a_moved_column(self, tmp_path, monkeypatch):
        m = _stage_store(tmp_path / "s")
        state = {"fired": False}
        orig = stage_mod._ColumnReader.read

        def racy(self, res, name):
            if not state["fired"] and self.path.endswith("1/1/1/1/all.pyramid.zarr"):
                state["fired"] = True  # a fleet worker rewrites the leaf mid-read
                _write_leaf(tmp_path / "s", "1111", 9, granules=7)
            return orig(self, res, name)

        monkeypatch.setattr(stage_mod._ColumnReader, "read", racy)
        summary = _sweep(tmp_path / "s", m)
        (row,) = summary["stages"]
        assert state["fired"] and row["revalidated"] >= 1
        # The merge consumed the COHERENT post-rewrite column, not a torn mix.
        g = _artifact(tmp_path / "s", "1/1/1/all.zarr")
        assert list(g["3"]["count"][:])[0] == 136 * 10

    def test_fleet_append_during_live_sweep_heals_next_pass(self, tmp_path, monkeypatch):
        # The matrix's middle row: candidates include a leaf whose column has
        # not landed when the sweep reads (fleet in flight) — the sweep
        # completes, records under-coverage, and the NEXT sweep heals.
        m = _stage_store(tmp_path / "s", skip_columns={"1121"})
        (row,) = _sweep(tmp_path / "s", m)["stages"]
        assert row["under_covered"] > 0
        # '112' folds nothing and writes no artifact, so the merge at '11'
        # records it missing; the root's one child '11' is whole.
        attrs = dict(_artifact(tmp_path / "s", "1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 1, "missing": 1, "unreadable": 0}
        root_attrs = dict(_artifact(tmp_path / "s", "1/all.zarr").attrs)["zagg_overview"]
        assert root_attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 0}
        _write_leaf(tmp_path / "s", "1121", 2)  # the fleet lands mid/after
        (row,) = _sweep(tmp_path / "s", m, run_id="B")["stages"]
        attrs = dict(_artifact(tmp_path / "s", "1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 2, "missing": 0, "unreadable": 0}
        # ...and the healed child's generation moved the root, which re-folded.
        assert list(_artifact(tmp_path / "s", "1/all.zarr")["1"]["count"][:])[0] == 136 + 272 + 408


class TestDisjointness:
    def test_no_two_workers_write_the_same_object(self, tmp_path, monkeypatch):
        """The #380 spy pattern against the REAL stage writers."""
        m = _stage_store(tmp_path / "s")
        per_worker: dict = {}
        orig_node = stage_mod.stage_node
        orig_ov = stage_mod._write_stage_overview
        orig_col = stage_mod.write_stage_column
        current = {}

        def spy_node(store, store_root, node, stage, *args, **kwargs):
            current["worker"] = (stage["dispatch"], node)
            per_worker.setdefault(current["worker"], set())
            return orig_node(store, store_root, node, stage, *args, **kwargs)

        def spy_ov(store_root, node, k, key, *args, **kwargs):
            basename = orig_ov(store_root, node, k, key, *args, **kwargs)
            per_worker[current["worker"]].add(f"{node}/{basename}")
            return basename

        def spy_col(store_root, node, *args, **kwargs):
            written = orig_col(store_root, node, *args, **kwargs)
            if written is not None:
                per_worker[current["worker"]].add(f"{node}/{written['object']}")
            return written

        monkeypatch.setattr(stages_mod, "stage_node", spy_node)
        monkeypatch.setattr(stage_mod, "_write_stage_overview", spy_ov)
        monkeypatch.setattr(stage_mod, "write_stage_column", spy_col)
        _sweep(tmp_path / "s", m, width=1)
        written = [keys for keys in per_worker.values() if keys]
        assert written and sum(len(k) for k in written) > 4  # never vacuous
        for i, a in enumerate(written):
            for b in written[i + 1 :]:
                assert not (a & b)


# ---------------------------------------------------------------------------
# Phase 3: the designated finisher-worker + the ruled /2 default flip.
# ---------------------------------------------------------------------------


class TestFinisher:
    def _run(self, root, m, *, run_id="A", released=None):
        from zagg.hive import read_manifest

        summary = sweep_stage_pass(str(root), m, _by_shard(), run_id=run_id, tuple_width=3)
        actuals = {int(k): v for k, v in summary["levels"].items()}
        out = run_finisher(
            str(root),
            m,
            _by_shard(),
            actuals,
            run_id=run_id,
            release=released,
        )
        return out, read_manifest(str(root)), summary

    def test_per_entry_actuals_land_in_the_manifest(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        out, fresh, _ = self._run(tmp_path / "s", m)
        assert out["manifest_updated"] and out["root_moc"]
        entries = {e["node"]: e for e in fresh["pyramid"]["overviews"]}
        # The leaf entry records the leaf-column law.
        assert entries[3]["actuals"]["regime"] == "leaf-column"
        assert entries[3]["actuals"]["merges_from_raw"] == 1
        # Gather level: gen-1, merges-from-raw 1; merge levels: one more than
        # their sources, 2 then 3 (the cascade depth, issue #620).
        assert entries[2]["actuals"]["regime"] == "stage-gather"
        assert entries[2]["actuals"]["merges_from_raw"] == 1
        for node, depth in ((1, 2), (0, 3)):
            assert entries[node]["actuals"]["regime"] == "stage-merge"
            assert entries[node]["actuals"]["merges_from_raw"] == depth
        assert entries[0]["actuals"]["source_children"]["missing"] == 0
        # The family dict's /1-era keys are untouched; no `materialized` is
        # written on /2 (one source of truth — the recorded lean).
        assert "materialized" not in fresh["pyramid"]["overview"]

    def test_root_moc_refreshed_with_sweep_source(self, tmp_path):
        from zagg.hive import read_root_coverage

        m = _stage_store(tmp_path / "s")
        self._run(tmp_path / "s", m)
        env = read_root_coverage(str(tmp_path / "s"))
        assert env["source"] == "sweep" and env["order"] == 3

    def test_lease_release_is_the_final_act(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        calls = []
        out, _, _ = self._run(tmp_path / "s", m, released=lambda: calls.append(1) or True)
        assert out["lease_released"] and calls == [1]

    def test_finisher_is_idempotent(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        _, first, _ = self._run(tmp_path / "s", m)
        _, second, _ = self._run(tmp_path / "s", m, run_id="B")
        e1 = {e["node"]: e.get("actuals") for e in first["pyramid"]["overviews"]}
        e2 = {e["node"]: e.get("actuals") for e in second["pyramid"]["overviews"]}
        for node in e1:
            for key in ("regime", "merges_from_raw"):
                assert e1[node][key] == e2[node][key]


class TestDefaultFlip:
    """The ruled /2 default flip: every new store declares the multiresolution
    grammar (build_pyramid_block) and its workers write the columns it
    promises (leaf_column_plan) — the two gates must agree."""

    def _config(self, **grid):
        from zagg.config import PipelineConfig

        return PipelineConfig(
            aggregation={
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {
                    "count": {"function": "sum", "dtype": "int32", "fill_value": 0},
                },
            },
            output={"grid": {"type": "healpix", **grid}},
        )

    def test_default_declaration_is_v2_at_the_chunk_order(self):
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        block = build_pyramid_block(cfg, 4, chunk_order=5)
        assert block["spec"] == PYRAMID_SPEC_V2
        assert block["overviews"][0] == {"node": 4, "cells": [5]}
        assert block["overviews"][-1] == {"node": 0, "cells": [1]}

    def test_k1_grid_keeps_the_v1_fallback(self):
        from zagg.sweep_overview import PYRAMID_SPEC, build_pyramid_block

        cfg = self._config(parent_order=4, child_order=6)
        block = build_pyramid_block(cfg, 4, chunk_order=4)  # K == 1: no interior
        assert block["spec"] == PYRAMID_SPEC

    def test_explicit_legacy_schedule_stays_v1(self):
        from zagg.sweep_overview import PYRAMID_SPEC, build_pyramid_block

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        cfg.output["pyramid"] = {"orders": [2]}
        block = build_pyramid_block(cfg, 4, chunk_order=5)
        assert block["spec"] == PYRAMID_SPEC and block["overview"]["orders"] == [2]

    def test_raster_is_exempt(self):
        from zagg.sweep_overview import PYRAMID_SPEC, build_pyramid_block

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        cfg.data_source = {"reader": "raster"}
        block = build_pyramid_block(cfg, 4, chunk_order=5)
        assert block["spec"] == PYRAMID_SPEC

    def test_worker_gate_mirrors_the_default(self):
        from zagg.column import leaf_column_plan
        from zagg.grids import from_config

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        plan = leaf_column_plan(cfg, from_config(cfg))
        assert plan is not None
        resolutions, fields = plan
        assert resolutions == [5, 4] and "count" in fields

    def test_worker_gate_declines_without_interior_chunk(self):
        from zagg.column import leaf_column_plan
        from zagg.grids import from_config

        cfg = self._config(parent_order=4, child_order=6)
        cfg.output["sharded"] = False  # K == 1 stays K == 1
        assert leaf_column_plan(cfg, from_config(cfg)) is None

    def test_worker_gate_declines_legacy_schedule(self):
        from zagg.column import leaf_column_plan
        from zagg.grids import from_config

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        cfg.output["pyramid"] = {"orders": [2]}
        assert leaf_column_plan(cfg, from_config(cfg)) is None

    def test_manifest_and_worker_gate_agree_end_to_end(self):
        from zagg.column import leaf_column_plan
        from zagg.grids import from_config
        from zagg.hive import build_manifest

        cfg = self._config(parent_order=4, child_order=6, chunk_inner=5)
        grid = from_config(cfg)
        manifest = build_manifest(grid)
        declared = manifest["pyramid"]["spec"] == PYRAMID_SPEC_V2
        assert declared == (leaf_column_plan(cfg, grid) is not None) is True


# ---------------------------------------------------------------------------
# Phase 4: orchestration — admission, discovery, partitions, chaining, CLI.
# ---------------------------------------------------------------------------


def _write_run_record(root, leaves):
    """A minimal stats parquet the listing-based discovery reads."""
    import pandas as pd

    df = pd.DataFrame(
        {
            "shard_key": pd.array([morton_word(d) for d in leaves], dtype="UInt64"),
            "success": [True] * len(leaves),
            "window": [None] * len(leaves),
        }
    )
    df.to_parquet(root / "stats_20260809T000000Z_test.parquet", engine="fastparquet")


class TestRunStageSweep:
    def test_end_to_end_with_lease_and_finisher(self, tmp_path):
        from zagg.hive import read_manifest
        from zagg.sweep_lease import read_lease
        from zagg.sweep_stages import run_stage_sweep

        _stage_store(tmp_path / "s")
        summary = run_stage_sweep(str(tmp_path / "s"), [(morton_word(d), None) for d in LEAVES])
        assert summary["stages"] and summary["finisher"]["manifest_updated"]
        assert summary["lease"]["released"] and read_lease(str(tmp_path / "s")) is None
        entries = {e["node"]: e for e in read_manifest(str(tmp_path / "s"))["pyramid"]["overviews"]}
        assert entries[0]["actuals"]["merges_from_raw"] == 3
        # The record landed outside the stats_*.parquet glob, mode-tagged.
        assert summary["record"].startswith("sweep_stats_") and summary["record"].endswith(
            "_stages.json"
        )
        record = json.loads((tmp_path / "s" / summary["record"]).read_text())
        assert record["mode"] == "stages" and record["lease"]["released"] is True

    def test_second_sweep_refused_naming_the_runner(self, tmp_path):
        from zagg.sweep_lease import SweepRefusedError, acquire_lease
        from zagg.sweep_stages import run_stage_sweep

        _stage_store(tmp_path / "s")
        acquire_lease(str(tmp_path / "s"), run_id="live-runner")
        with pytest.raises(SweepRefusedError, match="live-runner"):
            run_stage_sweep(str(tmp_path / "s"), [(morton_word(d), None) for d in LEAVES])

    def test_expired_claim_completes_a_partial_prior_run(self, tmp_path):
        from zagg.sweep_lease import LEASE_NAME, acquire_lease, read_lease
        from zagg.sweep_stages import run_stage_sweep

        m = _stage_store(tmp_path / "s")
        # Run A swept only base cell '1', then died holding the lease.
        sweep_stage_pass(
            str(tmp_path / "s"),
            m,
            _by_shard(),
            run_id="A",
            scope=normalize_scope(["1"]),
        )
        lease = acquire_lease(str(tmp_path / "s"), run_id="A")
        stale = dict(lease, heartbeat_at="2020-01-01T00:00:00+00:00")
        (tmp_path / "s" / LEASE_NAME).write_text(json.dumps(stale))
        summary = run_stage_sweep(
            str(tmp_path / "s"), [(morton_word(d), None) for d in LEAVES], run_id="C"
        )
        assert summary["lease"]["claimed_from"] == "A"
        assert summary["lease"]["released"] and read_lease(str(tmp_path / "s")) is None
        # The claimant finished what A left: '-2' materialized, '1' current.
        assert (tmp_path / "s" / "-2" / "all.zarr").exists()
        (row,) = summary["stages"]
        assert row["current"] > 0 and row["written"] > 0

    def test_unscoped_discovery_survives_a_stale_root_moc(self, tmp_path):
        from zagg.sweep_stages import run_stage_sweep

        # Fleet appended '-2111' (run record + column) but NO sweep ran, so
        # the root MOC only knows the '1*' leaves — the ruled acceptance case:
        # discovery is listing-based; the MOC is an accelerator, never truth.
        root = tmp_path / "s"
        _stage_store(root, write_moc=False)
        write_root_coverage(
            str(root),
            build_root_coverage([morton_word(d) for d in LEAVES if d != "-2111"], 3),
        )
        _write_run_record(root, LEAVES)
        summary = run_stage_sweep(str(root))  # leaves=None: discovery
        assert summary["n_leaves"] == len(LEAVES)
        # The stale MOC did not hide the new base cell from the sweep.
        assert (root / "-2" / "all.zarr").exists()

    def test_partitions_compose_under_one_lease(self, tmp_path):
        from zagg.sweep_stages import run_stage_sweep

        _stage_store(tmp_path / "s")
        summary = run_stage_sweep(
            str(tmp_path / "s"),
            [(morton_word(d), None) for d in LEAVES],
            partitions=4,
        )
        assert summary["lease"]["released"]
        # Every leaf here has first digit '1', so partition 0 owns ALL the
        # writing; coarse dispatch nodes span partitions (their subtrees
        # intersect every partition MOC), so later partitions re-visit them
        # and read current — the ratchet is the in-process dedupe.
        by_part: dict = {}
        for row in summary["stages"]:
            index = row.get("partition", {}).get("index")
            by_part[index] = by_part.get(index, 0) + row["written"]
        assert by_part[0] > 0
        assert all(v == 0 for i, v in by_part.items() if i != 0)
        assert (tmp_path / "s" / "1" / "all.zarr").exists()
        assert (tmp_path / "s" / "-2" / "all.zarr").exists()
        # Review finding: the re-visits later partitions make (skip-if-current
        # on shared coarse ancestors) must not inflate the manifest actuals —
        # per-artifact rows are assigned, then summed once. Node 0 sums its
        # two base-cell artifacts: one order-1 child each ('11' and '-21').
        from zagg.hive import read_manifest

        entries = {e["node"]: e for e in read_manifest(str(tmp_path / "s"))["pyramid"]["overviews"]}
        assert entries[0]["actuals"]["source_children"] == {
            "folded": 2,
            "missing": 0,
            "unreadable": 0,
        }

    def test_failure_leaves_the_lease_held(self, tmp_path, monkeypatch):
        import zagg.sweep_stages as stages_mod
        from zagg.sweep_lease import read_lease
        from zagg.sweep_stages import run_stage_sweep

        _stage_store(tmp_path / "s")

        def boom(*a, **k):
            raise RuntimeError("stage died")

        monkeypatch.setattr(stages_mod, "sweep_stage_pass", boom)
        with pytest.raises(RuntimeError, match="stage died"):
            run_stage_sweep(str(tmp_path / "s"), [(morton_word(d), None) for d in LEAVES])
        held = read_lease(str(tmp_path / "s"))
        assert held is not None  # claimable after TTL; never released mid-wreck

    def test_v1_store_refuses_loudly(self, tmp_path):
        from zagg.sweep_stages import run_stage_sweep

        root = tmp_path / "s"
        root.mkdir()
        manifest = {
            "spec": "morton-hive/1",
            "dataset": {"short_name": "TEST", "version": "001"},
            "semantic_hash": "t",
            "cell_order": 5,
            "shard_order": 3,
            "split_schedule": [1, 1, 1],
            "pyramid": {"spec": "zagg-pyramid/1", "overview": {"orders": [1]}},
            "generated_at": _utcnow(),
        }
        (root / MANIFEST_NAME).write_text(json.dumps(manifest))
        with pytest.raises(ValueError, match="zagg-pyramid/2"):
            run_stage_sweep(str(root), [(morton_word("1111"), None)])


class TestChainingAndCli:
    def test_stage_sweep_after_run_scopes_to_the_fleet_footprint(self, tmp_path):
        from zagg.sweep_stages import stage_sweep_after_run

        _stage_store(tmp_path / "s")
        summary = stage_sweep_after_run(
            str(tmp_path / "s"), [(morton_word("1111"), None), (morton_word("1112"), None)]
        )
        assert summary is not None and summary["lease"]["released"]
        # Scoped to base '1': the untouched '-2' base was not invoked.
        assert not (tmp_path / "s" / "-2" / "all.zarr").exists()

    def test_stage_sweep_after_run_honours_the_never_touch_policy(self, tmp_path):
        # Issue #501 through the ONE sweep caller that holds a config. The
        # finisher's step 3 self-copies `aggregation.yaml`; on an archival
        # destination an operator who declared `never` must not get one new
        # full-size root-core version per staged sweep. Delete the
        # `touch_policy=` kwarg anywhere along
        # stage_sweep_after_run -> run_stage_sweep -> run_finisher and this
        # fails (review finding on PR #496).
        from zagg.sweep_stages import stage_sweep_after_run

        _stage_store(tmp_path / "s")
        (tmp_path / "s" / "aggregation.yaml").write_text("dataset: TEST\n")

        never = stage_sweep_after_run(
            str(tmp_path / "s"), [(morton_word("1111"), None)], touch_policy="never"
        )
        assert never["finisher"]["objects_touched"] == 0
        assert never["finisher"]["touch_skipped_paths"] == 1

        # The default is `auto` -- the issue #495 phase 4 inference -- so the
        # CLI entry point, which has no config to read a policy from, is
        # unchanged: a local core is still touched.
        auto = stage_sweep_after_run(str(tmp_path / "s"), [(morton_word("1111"), None)])
        assert auto["finisher"]["objects_touched"] == 1
        assert "touch_skipped_paths" not in auto["finisher"]

    def test_stage_sweep_after_run_is_fail_open(self, tmp_path):
        from zagg.sweep_lease import acquire_lease
        from zagg.sweep_stages import stage_sweep_after_run

        _stage_store(tmp_path / "s")
        acquire_lease(str(tmp_path / "s"), run_id="other")
        assert stage_sweep_after_run(str(tmp_path / "s"), [(morton_word("1111"), None)]) is None

    def test_cli_stages_backstop(self, tmp_path, capsys):
        from zagg.sweep import main

        root = tmp_path / "s"
        _stage_store(root)
        _write_run_record(root, LEAVES)
        assert main([str(root), "--stages"]) == 0
        out = json.loads(capsys.readouterr().out)
        assert out["mode" if "mode" in out else "run_id"]  # summary printed
        assert out["lease"]["released"] is True
        assert (root / "1" / "all.zarr").exists()

    def test_sweep_knob_accepts_stages(self):
        from zagg.config import PipelineConfig, validate_config

        cfg = PipelineConfig(
            data_source={"reader": "generic", "variables": {"h_li": "gt1l/h_li"}},
            aggregation={
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {"count": {"function": "sum", "dtype": "int32", "fill_value": 0}},
            },
            output={
                "grid": {"type": "healpix", "parent_order": 4, "child_order": 6},
                "store_layout": "hive",
                "sweep": "stages",
            },
        )
        validate_config(cfg)  # must not raise
        cfg.output["sweep"] = "bogus"
        with pytest.raises(ValueError, match="boolean or 'stages'"):
            validate_config(cfg)


class TestHeartbeatCadence:
    def test_on_node_seam_fires_per_dispatch_node(self, tmp_path):
        # Review finding: a per-tuple-only beat lets the holder's own lease
        # expire inside an unbounded tuple. The pass must expose a per-node
        # seam the orchestrator's throttled heartbeat rides.
        m = _stage_store(tmp_path / "s")
        nodes = []
        summary = sweep_stage_pass(
            str(tmp_path / "s"),
            m,
            _by_shard(),
            run_id="A",
            tuple_width=1,
            on_node=nodes.append,
        )
        assert len(nodes) == sum(row["nodes"] for row in summary["stages"])

    def test_orchestrator_beats_inside_a_tuple(self, tmp_path, monkeypatch):
        import zagg.sweep_lease as lease_mod
        from zagg.sweep_stages import run_stage_sweep

        _stage_store(tmp_path / "s")
        real = lease_mod.heartbeat_lease
        beats = []

        def counting(store_root, lease, **kwargs):
            beats.append(lease["run_id"])
            return real(store_root, lease, **kwargs)

        monkeypatch.setattr(lease_mod, "heartbeat_lease", counting)
        # ttl_s=0 opens the throttle on every node, so the beat count must
        # exceed the per-tuple count (1 stage at width 3 on this store).
        summary = run_stage_sweep(
            str(tmp_path / "s"),
            [(morton_word(d), None) for d in LEAVES],
            lease_ttl_s=1,
        )
        assert summary["lease"]["released"] and len(beats) >= 1


class TestSoftBarrierReadFaults:
    def test_missing_member_group_fills_never_aborts(self, tmp_path):
        # Review finding: a declaration deepened over existing columns (or a
        # transient object fault) leaves a column short a group — the sweep
        # must under-fill and complete, never abort.
        import shutil

        m = _stage_store(tmp_path / "s")
        shutil.rmtree(tmp_path / "s" / "1" / "1" / "1" / "1" / "all.pyramid.zarr" / "3")
        summary = sweep_stage_pass(str(tmp_path / "s"), m, _by_shard(), run_id="A")
        (row,) = summary["stages"]
        assert row["written"] > 0
        payloads = _artifact(tmp_path / "s", "1/1/1/all.zarr")["3"]["h_tdigest"][:]
        assert bytes(payloads[0]) == b""  # the short child's cell: fill
        assert bytes(payloads[1]) != b""  # its sibling folded normally

    def test_corrupt_column_counts_unreadable_not_missing(self, tmp_path):
        m = _stage_store(tmp_path / "s")
        garbage = tmp_path / "s" / "1" / "1" / "1" / "2" / "all.pyramid.zarr" / "zarr.json"
        garbage.write_text("not zarr metadata")
        summary = sweep_stage_pass(str(tmp_path / "s"), m, _by_shard(), run_id="A")
        (row,) = summary["stages"]
        assert row["failed"] > 0 and row["written"] > 0
        attrs = dict(_artifact(tmp_path / "s", "1/1/1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 1, "missing": 0, "unreadable": 1}


class TestForeignStampAtReread:
    def test_reread_reruns_the_foreign_guard(self, tmp_path, monkeypatch):
        # Review finding: the stamp-validation re-read must re-run the
        # foreign-fresh guard — a column rewritten mid-read by a FOREIGN
        # sweep is exactly the residual race the backstop exists for.
        m = _stage_store(tmp_path / "s")
        state = {"fired": False}
        orig = stage_mod._ColumnReader.read

        def racy(self, res, name):
            if not state["fired"] and self.path.endswith("1/1/1/1/all.pyramid.zarr"):
                state["fired"] = True
                _write_leaf(tmp_path / "s", "1111", 9, granules=7)
                store = open_store(str(tmp_path / "s/1/1/1/1/all.pyramid.zarr"))
                g = zarr.open_group(store, path="", mode="r+", zarr_format=3)
                stamp = dict(g.attrs["morton_hive_commit"])
                stamp["run_id"] = "intruder"
                stamp["written_at"] = "2999-01-01T00:00:00+00:00"
                g.attrs["morton_hive_commit"] = stamp
            return orig(self, res, name)

        monkeypatch.setattr(stage_mod._ColumnReader, "read", racy)
        with pytest.raises(ForeignSweepError, match="intruder"):
            sweep_stage_pass(str(tmp_path / "s"), m, _by_shard(), run_id="A")
        assert state["fired"]


class TestWindowedStageSweep:
    """Review finding: the windowed / all-time arm was implemented but
    untested. Two windows, ``all_time: true`` — per-window artifacts fold as
    gathers/merges exactly like the unwindowed path, and the all-time fold is
    ALWAYS a stage-merge, of the node's own per-window overviews since issue
    #586 phase 4 (``tests/test_sweep_units.py`` owns that fold); no all-time
    stage column exists."""

    WINDOWS = ("2019", "2020")

    def _windowed_store(self, root, leaves=LEAVES):
        from zagg.pyramid import expand_overviews

        m = _stage_store(root, leaves=leaves)
        m["spec"] = "morton-hive/2"
        m["temporal"] = {
            "schedule": "yearly",
            "time_field": "t",
            "epoch": "2018-01-01T00:00:00Z",
        }
        m["pyramid"]["overview"]["all_time"] = True
        (root / MANIFEST_NAME).write_text(json.dumps(m, indent=1))
        # Replace the unwindowed columns with per-window ones.
        for leaf in root.rglob("all.pyramid.zarr"):
            import shutil

            shutil.rmtree(leaf)
        from zagg.column import column_resolutions, fold_column, write_column

        levels = expand_overviews([4], parent_order=3)
        res = column_resolutions(levels, 3)
        for i, dec in enumerate(leaves):
            for w, window in enumerate(self.WINDOWS):
                folded = fold_column(_leaf_slabs(i + 10 * w), FIELDS, cell_order=5, resolutions=res)
                write_column(
                    str(root),
                    morton_word(dec),
                    folded,
                    FIELDS,
                    node_order=3,
                    cell_order=5,
                    window=window,
                    granule_count=1,
                )
        return m

    def test_per_window_and_all_time_folds(self, tmp_path):
        root = tmp_path / "s"
        m = self._windowed_store(root)
        by_shard = {d: set(self.WINDOWS) for d in LEAVES}
        summary = sweep_stage_pass(str(root), m, by_shard, run_id="A")
        (row,) = summary["stages"]
        assert row["failed"] == 0 and row["written"] > 0
        # Per-window artifacts exist side by side with the all-time fold.
        for name in ("2019.zarr", "2020.zarr", "all.zarr"):
            assert (root / "1" / "1" / "1" / name).exists()
        # Per-window gather carries gen-1 bytes; regimes as unwindowed.
        w_attrs = dict(_artifact(root, "1/1/1/2019.zarr").attrs)["zagg_overview"]
        assert w_attrs["regime"] == "stage-gather" and w_attrs["merges_from_raw"] == 1
        # The all-time fold is a merge even at a gather level, where its
        # sources — the per-window overviews — are gen-1 content: exactly 2
        # merges from raw.
        a_attrs = dict(_artifact(root, "1/1/1/all.zarr").attrs)["zagg_overview"]
        assert a_attrs["regime"] == "stage-merge" and a_attrs["merges_from_raw"] == 2
        # Exact math: all-time root cell = both windows' leaf sums.
        per_window = [
            list(_artifact(root, f"1/{w}.zarr")["1"]["count"][:])[0] for w in self.WINDOWS
        ]
        all_time = list(_artifact(root, "1/all.zarr")["1"]["count"][:])[0]
        assert all_time == sum(per_window) > 0
        # No stage column exists at any order: nothing above order 2 gathers
        # on this ladder, and an all-time column never does (merged content).
        summary1 = sweep_stage_pass(str(root), m, by_shard, run_id="B", tuple_width=1)
        assert all(s["failed"] == 0 for s in summary1["stages"])
        for node in ("1/1", "1"):
            assert not (root / node / "2019.pyramid.zarr").exists()
            assert not (root / node / "all.pyramid.zarr").exists()
