"""Unit tests for the pure size-accounting logic in ``.github/scripts/moc_sizing.py``.

The network/read half is not exercised here (it needs NSIDC EDL auth); these
cover the morton-interval, size, over-coverage, morton-box and carrier accounting
that the recommendation on #200 rests on.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pytest

_SPEC = importlib.util.spec_from_file_location(
    "moc_sizing", Path(__file__).resolve().parents[1] / ".github/scripts/moc_sizing.py"
)
moc = importlib.util.module_from_spec(_SPEC)
sys.modules["moc_sizing"] = moc  # dataclass resolves annotations via sys.modules
_SPEC.loader.exec_module(moc)

pytest.importorskip("mortie")

SHARD_KEY = 5347395636851376137  # pinned NEON o9 shard
SHARD_ORDER = 9


def _leaves(n: int, seed: int = 0) -> np.ndarray:
    """A deterministic occupied order-19 cell set inside the pinned shard."""
    from mortie import generate_morton_children

    all_leaves = np.sort(
        np.asarray(generate_morton_children(SHARD_KEY, moc.CHILD_ORDER), dtype=np.uint64)
    )
    rng = np.random.RandomState(seed)
    idx = np.unique(rng.randint(0, all_leaves.size, size=n))
    return all_leaves[idx]


def test_step19_matches_descendant_spacing():
    from mortie import generate_morton_children

    kids = np.sort(
        np.asarray(generate_morton_children(SHARD_KEY, moc.CHILD_ORDER), dtype=np.uint64)
    )
    diffs = np.unique(np.diff(kids.astype(object)))
    assert diffs.tolist() == [moc.STEP19]


def test_full_subtree_is_one_interval_and_one_member():
    # every order-19 leaf occupied -> compresses to the single shard cell, one range
    occ = np.asarray(
        __import__("mortie").generate_morton_children(SHARD_KEY, moc.CHILD_ORDER), dtype=np.uint64
    )
    m = moc.moc_at_order(occ, moc.CHILD_ORDER)
    assert m.size == 1
    ivals = moc.moc_intervals(m)
    assert len(ivals) == 1
    # covers exactly 4^10 order-19 cells, over-coverage 1.0
    assert moc.covered_leaf_count(m) == 4**10
    assert moc.over_coverage(m, occ.size) == pytest.approx(1.0)


def test_intervals_merge_adjacent_members():
    # two morton-adjacent order-18 cells -> 8 leaves, one merged interval
    from mortie import clip2order

    leaves = np.sort(
        np.asarray(
            __import__("mortie").generate_morton_children(SHARD_KEY, moc.CHILD_ORDER),
            dtype=np.uint64,
        )
    )
    occ = np.concatenate([leaves[0:4], leaves[4:8]])  # first two order-18 blocks
    m = moc.moc_at_order(occ, moc.CHILD_ORDER)
    assert m.size == 2  # two order-18 members
    assert clip2order(18, occ).size == 8
    ivals = moc.moc_intervals(m)
    assert len(ivals) == 1  # contiguous -> single range


def test_over_coverage_is_monotone_nondecreasing_with_coarsening():
    occ = _leaves(1500, seed=1)
    n = int(np.unique(occ).size)
    prev = 0.0
    for order in range(SHARD_ORDER + 3, moc.CHILD_ORDER + 1):
        oc = moc.over_coverage(moc.moc_at_order(occ, order), n)
        assert oc >= 1.0
        assert oc >= prev - 1e-9  # coarser (lower order, earlier) never under-covers finer
        prev_order_oc = oc
    # exact order-19 is 1.0
    assert moc.over_coverage(moc.moc_at_order(occ, moc.CHILD_ORDER), n) == pytest.approx(1.0)
    assert prev_order_oc == pytest.approx(1.0)


def test_morton_box_at_most_four_members_and_conservative():
    occ = _leaves(2000, seed=2)
    box, box_order = moc.morton_box(occ, SHARD_ORDER)
    assert box.size <= 4
    assert box_order > SHARD_ORDER
    # the box is a conservative superset of the occupancy
    assert moc.over_coverage(box, int(np.unique(occ).size)) >= 1.0


def test_json_ranges_endpoints_are_strings_over_2p53():
    occ = _leaves(500, seed=3)
    m = moc.moc_at_order(occ, moc.CHILD_ORDER)
    s = moc.intervals_json(moc.moc_intervals(m))
    parsed = json.loads(s)
    assert parsed and all(isinstance(lo, str) and isinstance(hi, str) for lo, hi in parsed)
    # endpoints exceed 2^53 -- the reason O1 mandates strings
    assert int(parsed[0][0]) > 2**53


def test_packed_sizes_raw_and_b64():
    m = moc.moc_at_order(_leaves(300, seed=4), moc.CHILD_ORDER)
    raw, b64 = moc.packed_sizes(m)
    assert raw == m.size * 8
    assert b64 > raw  # base64 inflates ~4/3 plus quotes


def test_coarsen_to_fit_respects_budget_and_deepens_with_budget():
    occ = _leaves(2500, seed=5)
    res = moc.analyze_occupancy(
        "t",
        SHARD_KEY,
        SHARD_ORDER,
        occ,
        n_granules_total=1,
        n_granules_read=1,
        sampled=False,
        n_points=int(occ.size),
    )
    by = {b["budget"]: b for b in res.budgets}
    for label, b in by.items():
        if b.get("fits") and b["budget_bytes"] is not None:
            assert b["ranges_bytes"] <= b["budget_bytes"], label
    # a bigger budget never achieves a shallower depth
    orders = [
        by[k]["achieved_order"]
        for k in ("1KB", "4KB", "16KB", "64KB", "256KB")
        if by[k].get("fits")
    ]
    assert orders == sorted(orders)


def test_leaf_zarr_json_grows_with_payload():
    small = moc.leaf_zarr_json_size('["1","2"]')
    big = moc.leaf_zarr_json_size('["1","2"]' * 500)
    ref = moc.leaf_zarr_json_size(None)  # sidecar: pointer only, no inline payload
    # a large inline payload bloats the leaf zarr.json every metadata open GETs;
    # the sidecar (ref) holds only a pointer, so it stays near the tiny-inline size
    assert big > small
    assert big > ref
    assert abs(small - ref) < 200


def test_bitmap_sizes_shape_and_bounds():
    occ = _leaves(1000, seed=6)
    bmp = moc.bitmap_sizes(occ, SHARD_KEY, SHARD_ORDER, SHARD_ORDER + 6)
    assert bmp["raw_bytes"] == (4**6) // 8
    assert 0 < bmp["n_set"] <= 4**6
    assert bmp["zstd_bytes"] > 0
