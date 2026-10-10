"""``tools/windowed_emit_accuracy.py`` — the all-time overview vs a flat fold of the
same leaves (issue #602; PR #587 question (16)), on a tiny windowed ``/2`` store
swept in-process."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pytest

from zagg.column import column_resolutions, fold_column, write_column
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, _utcnow, build_root_coverage, write_root_coverage
from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews
from zagg.stats.tdigest import build_tdigest
from zagg.sweep_overview import encode_digest
from zagg.sweep_stages import sweep_stage_pass

REPO = Path(__file__).parent.parent
sys.path.insert(0, str(REPO / "tools"))

import windowed_emit_accuracy as acc  # noqa: E402  (tools/ is not installed)

#: Shard 3 / cells 5 / overviews [4] (d = 1): the leaf column carries {4, 3};
#: the ladder is node 2 (cells 3), node 1 (cells 2), node 0 (cells 1).
LEAVES = ["1111", "1112", "1121", "1122"]
WINDOWS = ("2019", "2020")
DELTA = 16
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
        "delta": DELTA,
    },
}


def _slabs(seed: int, n: int = 16) -> dict:
    """Multi-centroid digests from skewed samples, so the folds actually compress."""
    rng = np.random.default_rng(seed)
    counts = np.zeros(n, dtype="int32")
    dig = np.full(n, b"", dtype=object)
    for j in range(n):
        values = rng.lognormal(mean=float(j % 4), sigma=0.7, size=200)
        counts[j] = len(values)
        dig[j] = encode_digest(build_tdigest(values, DELTA), "float32")
    return {"count": counts, "h_tdigest": dig}


def _windowed_store(root: Path, leaves=LEAVES, windows=WINDOWS) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    levels = expand_overviews([4], parent_order=3)
    manifest = {
        "spec": "morton-hive/2",
        "dataset": {"short_name": "TEST", "version": "001"},
        "semantic_hash": "t",
        "cell_order": 5,
        "shard_order": 3,
        "split_schedule": [1, 1, 1],
        "path_grouping": 1,
        "temporal": {"schedule": "yearly", "time_field": "t", "epoch": "2018-01-01T00:00:00Z"},
        "pyramid": {
            "spec": PYRAMID_SPEC_V2,
            "overviews": levels,
            "overview": {
                "all_time": True,
                "fold_source": "cascade",
                "exact_levels": 1,
                "fields": FIELDS,
            },
        },
        "generated_at": _utcnow(),
    }
    (root / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1))
    res = column_resolutions(levels, 3)
    for i, dec in enumerate(leaves):
        for w, window in enumerate(windows):
            folded = fold_column(_slabs(100 * w + i), FIELDS, cell_order=5, resolutions=res)
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
    write_root_coverage(str(root), build_root_coverage([morton_word(d) for d in leaves], 3))
    return manifest


@pytest.fixture(scope="module")
def swept(tmp_path_factory):
    root = tmp_path_factory.mktemp("acc") / "s"
    manifest = _windowed_store(root)
    by_shard = {d: set(WINDOWS) for d in LEAVES}
    summary = sweep_stage_pass(str(root), manifest, by_shard, run_id="A", tuple_width=1)
    assert all(s["failed"] == 0 for s in summary["stages"])
    return root, manifest


def test_probe_nodes_one_per_order_finest_first():
    assert acc.probe_nodes(LEAVES, 3) == [(2, "111"), (1, "11"), (0, "1")]
    assert acc.probe_nodes(["-2111", "3111"], 3) == [(2, "-211"), (1, "-21"), (0, "-2")]
    assert acc.probe_nodes([], 3) == []


def test_member_for_clamps_to_the_shard_order():
    groups = [13, 12, 11, 10, 9]
    assert [acc._member_for(r, groups) for r in (12, 11, 10, 9, 8, 4)] == [12, 11, 10, 9, 9, 9]


def test_all_time_against_the_flat_fold(swept):
    root, manifest = swept
    out = acc.accuracy_numbers(str(root), manifest, LEAVES, list(WINDOWS), store_kwargs={})
    assert out["quantiles"] == list(acc.QUANTILES)
    levels = {lv["order"]: lv for lv in out["levels"]}
    assert list(levels) == [2, 1, 0]
    # every level read its sources from the shard-order member (the coarsest the
    # column carries), 2 shards x 2 windows under node 111, 4 x 2 under 11 and 1
    assert [lv["member"] for lv in out["levels"]] == [3, 3, 3]
    assert [lv["sources"] for lv in out["levels"]] == [4, 8, 8]
    assert all(lv["sources_unreadable"] == 0 for lv in out["levels"])
    # the close is one merge further from raw than its per-window sources:
    # gather 1 -> 2 at node 2, cascade 2 -> 3 at node 1, 3 -> 4 at node 0
    assert [lv["merges_from_raw"] for lv in out["levels"]] == [2, 3, 4]
    assert all(lv["source_windows"]["folded"] == 2 for lv in out["levels"])
    # the exact leg: the all-time count IS the flat sum at every level
    assert all(lv["exact"] == {"count": True} for lv in out["levels"])
    assert all(lv["exact_cells_off"] == {"count": 0} for lv in out["levels"])
    # node 2 (cells 3 == the member): the close merged exactly the digests the
    # flat fold merges, so the payloads are byte-identical and the error is 0
    f2 = levels[2]["fields"]["h_tdigest"]
    assert f2["delta"] == DELTA and f2["cells_compared"] == 2
    assert f2["cells_identical"] == 2 and f2["rank_err_max"] == 0.0
    # below it the cascade's depth shows: a bounded, non-negative rank error
    for k in (1, 0):
        f = levels[k]["fields"]["h_tdigest"]
        assert f["cells_compared"] >= 1
        assert 0.0 <= f["rank_err_max"] < 0.5 and f["rank_err_p50"] <= f["rank_err_max"]
        assert f["rank_err_max_x_delta"] == pytest.approx(f["rank_err_max"] * DELTA)


def test_levels_filter_and_missing_artifact(swept, tmp_path):
    root, manifest = swept
    out = acc.accuracy_numbers(
        str(root), manifest, LEAVES, list(WINDOWS), store_kwargs={}, levels={1}
    )
    assert [lv["order"] for lv in out["levels"]] == [1]
    # a store whose ladder was never swept: the artifact is reported missing, not fatal
    bare = tmp_path / "bare"
    _windowed_store(bare, leaves=["1111"])
    out = acc.accuracy_numbers(str(bare), manifest, ["1111"], list(WINDOWS), store_kwargs={})
    assert all(lv["status"].startswith("artifact unreadable") for lv in out["levels"])


def test_rank_errors_are_zero_for_identical_digests():
    d = encode_digest(build_tdigest(np.random.default_rng(0).normal(size=500), DELTA), "float32")
    assert acc._rank_errors(d, d, "float32", (2,)) == [0.0] * len(acc.QUANTILES)
    assert acc._rank_errors(b"", d, "float32", (2,)) == []
    shifted = encode_digest(
        build_tdigest(np.random.default_rng(0).normal(size=500) + 1.0, DELTA), "float32"
    )
    errs = acc._rank_errors(shifted, d, "float32", (2,))
    assert len(errs) == len(acc.QUANTILES) and max(errs) > 0.2  # a shifted digest ranks high
