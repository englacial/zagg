"""NEON AOP box granule-selection investigation (issue #202, cross-ref #65).

Quantifies why building a shard map for the tiny NEON AOP box from the *full*
local ATL03 catalog selects a superset of granules relative to a CMR bbox query.

Three-way count comparison:

  (A) CMR bbox query                       -- the production spatial gate
  (B) full-catalog ``ShardMap.build``      -- feed the whole local catalog in
  (C) CMR-fetch THEN ``ShardMap.build``    -- the real production path

plus two membership experiments over the full local catalog:

  (D) footprint polygon intersects the AOI *bbox*   (CMR query semantics)
  (E) footprint polygon intersects the AOI *polygon* (exact AOI)

The gap (B) - (A) is decomposed into
  * bbox-vs-o9-cell coverage widening   ((D) - (A) at the query level), and
  * swath-envelope over-assignment (#65) ((B) - (D), footprints whose coarse
    quarter-orbit CMR envelope reaches the o9 cells though no beam does).

Run: ``uv run python bench/neon_selection_investigation.py``
Set ``ZAGG_NO_CMR=1`` to skip the two network steps (A) and (C).
"""

from __future__ import annotations

import os

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely
from shapely.geometry import box

from zagg.catalog import load_polygon, polygon_to_bbox
from zagg.catalog.shardmap import ShardMap
from zagg.catalog.sources import Catalog, Query
from zagg.config import load_config
from zagg.grids import from_config

REPO = "/Users/espg/software/zagg"
FULL = f"{REPO}/data/atl03_v007/atl03_v007_full.parquet"
AOI = f"{REPO}/tests/data/benchmark/AOP_NEON.geojson"
CFG = f"{REPO}/tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml"
START, END = "2018-10-13", "2025-06-01"
SHORT_NAME, VERSION, PROVIDER = "ATL03", "007", "NSIDC_CPRD"


def pa_scalar(np_dt, typ):
    return pa.scalar(int(np_dt.astype("datetime64[us]").astype("int64")), type="int64").cast(typ)


def temporal_subset(table):
    """Rows whose [start,end] datetime overlaps the mission window."""
    lo = np.datetime64(f"{START}T00:00:00")
    hi = np.datetime64(f"{END}T23:59:59")
    s = table.column("start_datetime")
    e = table.column("end_datetime")
    mask = pc.and_(
        pc.less_equal(s, pa_scalar(hi, s.type)),
        pc.greater_equal(e, pa_scalar(lo, e.type)),
    )
    return table.filter(mask)


def bbox_prefilter(table, region_bbox, pad):
    """Rows whose stored footprint bbox intersects a padded region bbox.

    A strict SUPERSET of any granule whose footprint can physically reach the
    o9 coverage cells, so building the shard map on this subset is identical to
    building it on the whole catalog -- just tractable (avoids 500k MOC calls).
    """
    x0, y0, x1, y1 = region_bbox
    x0, y0, x1, y1 = x0 - pad, y0 - pad, x1 + pad, y1 + pad
    bb = table.column("bbox")
    xmin, ymin = pc.struct_field(bb, "xmin"), pc.struct_field(bb, "ymin")
    xmax, ymax = pc.struct_field(bb, "xmax"), pc.struct_field(bb, "ymax")
    mask = pc.and_(
        pc.and_(pc.less_equal(xmin, x1), pc.greater_equal(xmax, x0)),
        pc.and_(pc.less_equal(ymin, y1), pc.greater_equal(ymax, y0)),
    )
    return table.filter(mask)


def total_granules(sm: ShardMap) -> int:
    """Unique granule ids across all shards of a shard map."""
    ids = set()
    for shard in sm.granules:
        for g in shard:
            ids.add(g["id"])
    return len(ids)


def polygon_membership_counts(table, aoi_parts, aoi_bbox):
    """(D) footprint intersects AOI bbox, (E) footprint intersects AOI polygon."""
    aoi_poly = shapely.geometry.Polygon(
        list(zip(np.asarray(aoi_parts[0][1]), np.asarray(aoi_parts[0][0])))
    )
    aoi_box = box(aoi_bbox[0], aoi_bbox[1], aoi_bbox[2], aoi_bbox[3])
    geoms = table.column("geometry").to_pylist()
    n_bbox = n_poly = 0
    for wkb in geoms:
        g = shapely.from_wkb(wkb)
        if g.is_empty:
            continue
        if g.intersects(aoi_box):
            n_bbox += 1
        if g.intersects(aoi_poly):
            n_poly += 1
    return n_bbox, n_poly


def main():
    grid = from_config(load_config(CFG))
    parts = load_polygon(AOI)
    aoi_bbox = polygon_to_bbox(parts)
    print(f"AOI bbox (lon_min,lat_min,lon_max,lat_max): {aoi_bbox}")
    print(
        f"grid: healpix parent_order={grid.parent_order} child_order={grid.child_order} "
        f"chunk_order={getattr(grid, 'chunk_order', None)}"
    )

    o9_cells = grid.coverage(parts)
    print(f"o9 coverage cells over AOI: {len(o9_cells)} -> {list(map(int, o9_cells))}")

    full = pq.read_table(FULL)
    print(f"\nfull catalog rows: {full.num_rows}")
    tsub = temporal_subset(full)
    print(f"temporal-window rows: {tsub.num_rows}")

    # Superset spatial pre-filter so the full-catalog build is tractable but
    # answer-identical. Verify stability by widening the pad.
    for pad in (1.0, 2.0):
        sub = bbox_prefilter(tsub, aoi_bbox, pad=pad)
        cat = Catalog(sub, {"collection": "ATL03_007", "bbox": list(aoi_bbox)})
        sm = ShardMap.build(cat, grid, region=parts, backend="mortie", footprint="swath")
        print(
            f"(B) full-catalog ShardMap.build [bbox pre-filter pad={pad} -> "
            f"{sub.num_rows} rows]: total unique granules = {total_granules(sm)}, "
            f"densest shard = {max((len(g) for g in sm.granules), default=0)}"
        )

    # (D)/(E) membership over the full temporal-window catalog.
    n_bbox, n_poly = polygon_membership_counts(tsub, parts, aoi_bbox)
    print(f"\n(D) footprint intersects AOI bbox    (full catalog): {n_bbox}")
    print(f"(E) footprint intersects AOI polygon (full catalog): {n_poly}")

    if os.environ.get("ZAGG_NO_CMR") == "1":
        print("\n[ZAGG_NO_CMR=1] skipping CMR steps (A) and (C)")
        return

    from zagg.catalog.sources import CMRSource

    q = Query(SHORT_NAME, VERSION, START, END, region=aoi_bbox, provider=PROVIDER)
    cmr_cat = CMRSource().fetch(q)
    print(f"\n(A) CMR bbox query granules: {len(cmr_cat)}")

    sm_c = ShardMap.build(cmr_cat, grid, region=parts, backend="mortie", footprint="swath")
    print(
        f"(C) CMR-fetch THEN ShardMap.build: total unique granules = {total_granules(sm_c)}, "
        f"densest shard = {max((len(g) for g in sm_c.granules), default=0)}"
    )


if __name__ == "__main__":
    main()
