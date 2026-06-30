"""Synthetic benchmark for the per-cell aggregation handoff (issue #30 / #130).

Times the per-shard grouping + aggregation for three approaches on synthetic
in-memory observations:

  * ``mask-loop``    -- the pre-#30 O(n_children x n_obs) boolean-mask loop (reference)
  * ``pandas-group`` -- sort/hash grouping, pandas carrier (current default)
  * ``arrow-group``  -- sort/hash grouping, arro3-core Arrow carrier (opt-in)

All three feed identical numpy arrays into the reducer, so their stats are
asserted byte-for-byte identical. (The earlier ``arrow-kernel`` pyarrow.compute
hash-aggregate path was dropped with pyarrow in the #130 path-C pivot — arro3 has
no hash-aggregate — so this benchmark now isolates only the carrier cost.)

This is the CI-runnable half of #30's benchmark: it isolates the grouping
algorithm and the carrier representation cost with no I/O, so it runs anywhere
without credentials. The real-data (ATL03 region) carrier timings land via the
``provisional_targets`` benchmark matrix (needs earthaccess/S3).

Memory is reported via ``tracemalloc`` (Python-domain peak): it does not capture
raw numpy data buffers, but it does capture the pandas BlockManager/Index and
Arrow wrapper overhead, which is where the carriers actually differ. The
phase-3 real-shard script reports process RSS instead.

Run::

    uv run python benchmarks/handoff_bench.py --n-obs 2000000 --n-cells 4096
"""

import argparse
import time
import tracemalloc

import numpy as np
import pandas as pd

from zagg.config import default_config
from zagg.processing import (
    _build_groups,
    _group_columns,
    calculate_cell_statistics,
)


def make_synthetic(n_obs: int, n_cells: int, seed: int = 0):
    """Random observations spread across ``n_cells`` cells (shuffled, not pre-sorted)."""
    rng = np.random.default_rng(seed)
    cells = rng.integers(0, n_cells, size=n_obs).astype(np.int64)
    h_li = (rng.standard_normal(n_obs) * 50.0).astype(np.float32)
    s_li = (np.abs(rng.standard_normal(n_obs)) + 0.01).astype(np.float32)
    return {"h_li": h_li, "s_li": s_li, "leaf_id": cells}, cells


def agg_mask_loop(col_dict, cell_col, children, cfg):
    """Reference: one boolean mask per child cell (the pre-#30 hot loop)."""
    stats = {}
    for child in children:
        mask = cell_col == child
        cell_data = {k: v[mask] for k, v in col_dict.items()}
        stats[int(child)] = calculate_cell_statistics(cell_data, config=cfg)
    return stats


def agg_grouped(col_arrays, cell_to_slice, children, cfg):
    """Sort/hash grouping: one contiguous slice per child cell."""
    empty = {k: v[:0] for k, v in col_arrays.items()}
    stats = {}
    for child in children:
        child = int(child)
        if child in cell_to_slice:
            s, e = cell_to_slice[child]
            cell_data = {k: v[s:e] for k, v in col_arrays.items()}
        else:
            cell_data = empty
        stats[child] = calculate_cell_statistics(cell_data, config=cfg)
    return stats


def timed(fn):
    tracemalloc.start()
    t0 = time.perf_counter()
    out = fn()
    dt = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return out, dt, peak / 1e6


def stats_equal(a, b):
    for child in a:
        for key in a[child]:
            x, y = a[child][key], b[child][key]
            if np.isnan(x) and np.isnan(y):
                continue
            if x != y:
                return False, (child, key, x, y)
    return True, None


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n-obs", type=int, default=2_000_000)
    ap.add_argument("--n-cells", type=int, default=4096)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    cfg = default_config()
    col_dict, cell_col = make_synthetic(args.n_obs, args.n_cells, args.seed)
    children = np.arange(args.n_cells, dtype=np.int64)

    ref, dt_mask, mem_mask = timed(lambda: agg_mask_loop(col_dict, cell_col, children, cfg))

    def run_pandas():
        df = pd.DataFrame(col_dict)
        col_arrays, cell_to_slice = _build_groups(df, cell_col)
        return agg_grouped(col_arrays, cell_to_slice, children, cfg)

    res_pd, dt_pd, mem_pd = timed(run_pandas)

    def run_arrow():
        from arro3.core import Array, Table

        table = Table.from_pydict({k: Array.from_numpy(v) for k, v in col_dict.items()})
        carrier = {n: table.column(n).combine_chunks().to_numpy() for n in table.column_names}
        col_arrays, cell_to_slice = _group_columns(carrier, carrier["leaf_id"])
        return agg_grouped(col_arrays, cell_to_slice, children, cfg)

    res_ar, dt_ar, mem_ar = timed(run_arrow)

    ok_pd, diff_pd = stats_equal(ref, res_pd)
    ok_ar, diff_ar = stats_equal(ref, res_ar)
    assert ok_pd, f"pandas grouping diverged from mask loop: {diff_pd}"
    assert ok_ar, f"arrow grouping diverged from mask loop: {diff_ar}"

    print(f"n_obs={args.n_obs:,}  n_cells={args.n_cells:,}  parity: OK (pandas == arrow)")
    print(f"{'approach':<16}{'wall_s':>10}{'peak_MB':>12}")
    for name, dt, mem in [
        ("mask-loop", dt_mask, mem_mask),
        ("pandas-group", dt_pd, mem_pd),
        ("arrow-group", dt_ar, mem_ar),
    ]:
        print(f"{name:<16}{dt:>10.3f}{mem:>12.1f}")


if __name__ == "__main__":
    main()
