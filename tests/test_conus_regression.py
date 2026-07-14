"""Tests for the CONUS regression-shard tooling (issue #202).

No AWS: covers the value-based stratifier that picks the regression-training
shards. The live dispatch path (``run_conus_regression.py``) needs credentials +
the local catalog, so it is exercised operationally, not in unit tests.
"""

import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _load_from_path(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_stratified_spans_value_range_on_peaked_distribution():
    sel = _load_from_path(
        "select_regression_shards", REPO / "data" / "conus" / "select_regression_shards.py"
    )
    # Sharply peaked: 3 sparse tails, a dense cluster at 70. Value-based
    # stratification must reach the tails, not cluster at the mode.
    counts = [20] + [70] * 200 + [140]
    idx = sel.stratified(counts, k=5)
    picked = sorted(counts[i] for i in idx)
    assert len(idx) == 5
    assert len(set(idx)) == 5  # no shard picked twice
    assert picked[0] == 20 and picked[-1] == 140  # both tails represented
