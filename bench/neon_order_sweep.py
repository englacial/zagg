"""NEON-box MOC intersection-order sweep (issue #202, espg hypothesis, #65).

Rebuilds the NEON-box shard map from the full local ATL03 catalog with the
mortie MOC intersection order swept (9, 12, 13, 16) and records the total unique
granule count + wall time at each. Discriminates:

  * count collapses toward CMR's 59 as order rises  -> MOC-rasterization
    coarseness, fixable zagg-side with a higher ``mortie_order`` knob; or
  * count plateaus above 59 even at order 16         -> residual is the coarse
    #65 swath-envelope polygon or the o9-cell AOI-coverage width, not order.

Also runs ``footprint="beams"`` at the top order to test the #65 residual.

Run: ``uv run python bench/neon_order_sweep.py``
"""

from __future__ import annotations

import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from zagg.catalog import load_polygon, polygon_to_bbox
from zagg.catalog.shardmap import ShardMap
from zagg.catalog.sources import Catalog
from zagg.config import load_config
from zagg.grids import from_config

REPO = "/Users/espg/software/zagg"
FULL = f"{REPO}/data/atl03_v007/atl03_v007_full.parquet"
AOI = f"{REPO}/tests/data/benchmark/AOP_NEON.geojson"
CFG = f"{REPO}/tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml"
START, END = "2018-10-13", "2025-06-01"
CMR_N = 59  # the committed pin, == raw CMR bbox query


def total_granules(sm):
    ids = set()
    for shard in sm.granules:
        for g in shard:
            ids.add(g["id"])
    return len(ids)


def build_subset():
    grid = from_config(load_config(CFG))
    parts = load_polygon(AOI)
    aoi_bbox = polygon_to_bbox(parts)
    full = pq.read_table(FULL)
    lo = np.datetime64(f"{START}T00:00:00").astype("datetime64[us]").astype("int64")
    hi = np.datetime64(f"{END}T23:59:59").astype("datetime64[us]").astype("int64")
    s, e = full.column("start_datetime"), full.column("end_datetime")
    tsub = full.filter(pc.and_(
        pc.less_equal(s, pa.scalar(hi, "int64").cast(s.type)),
        pc.greater_equal(e, pa.scalar(lo, "int64").cast(e.type)),
    ))
    bb = tsub.column("bbox")
    x0, y0, x1, y1 = aoi_bbox
    cand = tsub.filter(pc.and_(
        pc.and_(pc.less_equal(pc.struct_field(bb, "xmin"), x1 + 3),
                pc.greater_equal(pc.struct_field(bb, "xmax"), x0 - 3)),
        pc.and_(pc.less_equal(pc.struct_field(bb, "ymin"), y1 + 3),
                pc.greater_equal(pc.struct_field(bb, "ymax"), y0 - 3)),
    ))
    cat = Catalog(cand, {"collection": "ATL03_007", "bbox": list(aoi_bbox)})
    return grid, parts, cat, cand.num_rows


def main():
    grid, parts, cat, nrows = build_subset()
    print(f"candidate rows fed to build (superset pre-filter): {nrows}")
    print(f"CMR bbox query (production pin): {CMR_N}\n")
    print(f"{'order':>6} {'granules':>9} {'densest':>8} {'wall_s':>8}", flush=True)
    for order in (9, 12, 13, 16):
        t0 = time.perf_counter()
        sm = ShardMap.build(cat, grid, region=parts, backend="mortie",
                            mortie_order=order, footprint="swath")
        wall = time.perf_counter() - t0
        dens = max((len(g) for g in sm.granules), default=0)
        print(f"{order:>6} {total_granules(sm):>9} {dens:>8} {wall:>8.2f}", flush=True)

    # #65 residual test: per-beam corridors at the top order.
    t0 = time.perf_counter()
    sm_b = ShardMap.build(cat, grid, region=parts, backend="mortie",
                          mortie_order=16, footprint="beams")
    print(f"\nfootprint='beams' @order16: granules={total_granules(sm_b)} "
          f"densest={max((len(g) for g in sm_b.granules), default=0)} "
          f"wall={time.perf_counter()-t0:.2f}s", flush=True)


if __name__ == "__main__":
    main()
