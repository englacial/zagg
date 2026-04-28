"""Materialize dense-pack and full-sphere zarr layouts for empirical comparison.

Writes synthetic zagg-style HEALPix output stores at multiple child_orders, in
two layouts:

  * dense       --  array shape = 4^(child-parent) * n_parent_cells.
                    Block index = position in the populated set.
                    What zagg writes today.
  * fullsphere  --  array shape = 12 * 4^child_order.
                    Block index = morton parent ID.
                    Sparse-on-write: only populated chunks land on disk.

Synthetic AOI: ~1300 random morton parents drawn from the four southern HEALPix
base cells (8-11), to mimic an Antarctic catalog without doing a real CMR query.

Local run (fast, recommended for sanity check first)::

    python bench/layout_materialize.py --base ./bench_out --orders 8 10

S3 run (real test)::

    python bench/layout_materialize.py --base s3://my-bucket/bench-layout \\
        --orders 8 10 12 --region us-west-2

Companion notebook: ``bench/layout_access.ipynb`` opens the resulting stores and
measures access-pattern costs (RSS deltas, .sel(), .values, reductions).
"""

from __future__ import annotations

import argparse
import logging
import time

import numpy as np
import zarr
from zarr import open_array

from zagg.config import default_config
from zagg.schema import xdggs_zarr_template
from zagg.store import open_store

logger = logging.getLogger(__name__)


def fake_antarctic_parents(parent_order: int, n_parents: int = 1300, seed: int = 0):
    """Synthetic morton parents biased to southern base cells (8, 9, 10, 11).

    Returns a sorted unique array of int64 morton parent IDs at ``parent_order``.
    Mimics realistic Antarctic AOI density without a CMR query.
    """
    rng = np.random.default_rng(seed)
    parents_per_base = 4 ** parent_order
    out = []
    for base in (8, 9, 10, 11):
        n = max(1, n_parents // 4)
        sub = rng.choice(parents_per_base, size=min(n, parents_per_base), replace=False)
        out.extend(base * parents_per_base + sub)
    return np.sort(np.unique(np.array(out, dtype=np.int64)))


def synth_chunk(chunk_size: int, leaf_offset: int) -> dict[str, np.ndarray]:
    """Generate per-chunk synthetic values for one parent cell's leaves.

    ``leaf_offset`` is the morton/cell-id of the first leaf in the chunk
    (= parent_id * chunk_size when the parent's leaves are contiguous in
    morton order, which is the HEALPix nested invariant).
    """
    cell_ids = np.arange(leaf_offset, leaf_offset + chunk_size, dtype=np.uint64)
    morton = cell_ids.astype(np.int64)
    h_mean = np.full(chunk_size, 1500.0, dtype=np.float32)
    return {
        "cell_ids": cell_ids,
        "morton": morton,
        "count": np.full(chunk_size, 100, dtype=np.int32),
        "h_mean": h_mean,
        "h_min": h_mean - 100.0,
        "h_max": h_mean + 100.0,
        "h_sigma": np.full(chunk_size, 5.0, dtype=np.float32),
        "h_variance": np.full(chunk_size, 25.0, dtype=np.float32),
        "h_q25": h_mean - 10.0,
        "h_q50": h_mean,
        "h_q75": h_mean + 10.0,
    }


def _open_store(path: str, region: str):
    if path.startswith("s3://"):
        return open_store(path, region=region)
    return open_store(path)


def write_layout(
    base_path: str,
    layout: str,
    parent_order: int,
    child_order: int,
    parents: np.ndarray,
    region: str,
) -> tuple[str, dict]:
    """Write one zarr store in the given layout. Returns (path, stats)."""
    if layout not in ("dense", "fullsphere"):
        raise ValueError(f"unknown layout {layout!r}")

    suffix = "dense" if layout == "dense" else "full"
    store_path = f"{base_path.rstrip('/')}/{suffix}_p{parent_order}_c{child_order}.zarr"
    store = _open_store(store_path, region=region)
    config = default_config("atl06")

    n_parent_cells = len(parents) if layout == "dense" else None
    xdggs_zarr_template(
        store, parent_order, child_order,
        n_parent_cells=n_parent_cells,
        overwrite=True, config=config,
    )

    chunk_size = 4 ** (child_order - parent_order)
    var_names = list(synth_chunk(chunk_size, 0).keys())

    # Pre-open arrays once
    arrays = {
        name: open_array(store, path=f"{child_order}/{name}",
                         zarr_format=3, consolidated=False)
        for name in var_names
    }

    def _write_one(i, parent_id):
        leaf_offset = int(parent_id) * chunk_size
        chunk = synth_chunk(chunk_size, leaf_offset)
        block_idx = i if layout == "dense" else int(parent_id)
        for name, arr in arrays.items():
            arr.set_block_selection((block_idx,), chunk[name])

    from concurrent.futures import ThreadPoolExecutor
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=32) as ex:
        list(ex.map(lambda args: _write_one(*args), enumerate(parents)))
    write_s = time.time() - t0

    t0 = time.time()
    zarr.consolidate_metadata(store, zarr_format=3)
    consolidate_s = time.time() - t0

    return store_path, {
        "layout": layout,
        "parent_order": parent_order,
        "child_order": child_order,
        "n_populated_chunks": len(parents),
        "chunk_size": chunk_size,
        "n_total_chunks": (
            len(parents) if layout == "dense"
            else 12 * 4 ** parent_order
        ),
        "array_shape": (
            chunk_size * len(parents) if layout == "dense"
            else 12 * 4 ** child_order
        ),
        "write_s": write_s,
        "consolidate_s": consolidate_s,
        "path": store_path,
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--base", required=True,
                        help="Output base (local dir or s3://bucket/prefix)")
    parser.add_argument("--orders", type=int, nargs="+", default=[8, 10, 12],
                        help="child_orders to materialize (parent_order = child - 6)")
    parser.add_argument("--parent-offset", type=int, default=6,
                        help="parent_order = child_order - parent_offset (default 6)")
    parser.add_argument("--n-parents", type=int, default=1300,
                        help="synthetic AOI cell count")
    parser.add_argument("--layouts", nargs="+", default=["dense", "fullsphere"],
                        choices=["dense", "fullsphere"])
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    rows = []
    for child_order in args.orders:
        parent_order = max(0, child_order - args.parent_offset)
        parents = fake_antarctic_parents(
            parent_order, n_parents=args.n_parents, seed=args.seed,
        )
        logger.info(
            f"\n=== child_order={child_order} parent_order={parent_order} "
            f"n_parents={len(parents)} chunk_size={4**(child_order-parent_order)} ==="
        )
        for layout in args.layouts:
            try:
                path, stats = write_layout(
                    args.base, layout, parent_order, child_order, parents,
                    region=args.region,
                )
                rows.append(stats)
                logger.info(
                    f"  {layout:11s} shape={stats['array_shape']:>13,d} "
                    f"chunks={stats['n_total_chunks']:>10,d} "
                    f"populated={stats['n_populated_chunks']:>5,d} "
                    f"write={stats['write_s']:6.1f}s "
                    f"consolidate={stats['consolidate_s']:5.1f}s"
                )
            except Exception as e:
                logger.error(f"  {layout}: FAILED ({e})")

    print("\n=== Summary ===")
    print(f"{'layout':12s} {'p':>2s} {'c':>2s} {'shape':>13s} "
          f"{'chunks':>10s} {'pop':>5s} {'write_s':>8s} {'path'}")
    for r in rows:
        print(
            f"{r['layout']:12s} {r['parent_order']:>2d} {r['child_order']:>2d} "
            f"{r['array_shape']:>13,d} {r['n_total_chunks']:>10,d} "
            f"{r['n_populated_chunks']:>5,d} {r['write_s']:>8.1f}  {r['path']}"
        )


if __name__ == "__main__":
    main()
