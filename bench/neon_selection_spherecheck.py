"""Sphere-correct footprint intersection for the NEON box (issue #202, #65).

Planar shapely footprint-intersects-bbox (158) is corrupted by antimeridian
wrap. Recompute the membership sphere-correctly with mortie MOCs at a fine
order, against (i) the exact AOI polygon and (ii) the AOI bbox rectangle, to get
the honest local-vs-CMR gap and separate it from the o9-cell granularity.

Run: ``uv run python bench/neon_selection_spherecheck.py``
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely
from mortie import moc_to_order, morton_coverage_moc

from zagg.catalog import load_polygon, polygon_to_bbox
from zagg.catalog.sources import CMRSource, Query

REPO = "/Users/espg/software/zagg"
FULL = f"{REPO}/data/atl03_v007/atl03_v007_full.parquet"
AOI = f"{REPO}/tests/data/benchmark/AOP_NEON.geojson"
START, END = "2018-10-13", "2025-06-01"
ORDER = 14  # ~0.4 km cells: fine enough that the tiny AOI is tightly resolved


def cells(lats, lons, order=ORDER):
    moc = np.asarray(morton_coverage_moc(np.asarray(lats), np.asarray(lons), order=order))
    if moc.size == 0:
        return np.empty(0, dtype=np.uint64)
    return np.unique(moc_to_order(moc, order))


def main():
    parts = load_polygon(AOI)
    aoi_bbox = polygon_to_bbox(parts)
    lats, lons = parts[0]
    poly_cells = cells(lats, lons)
    x0, y0, x1, y1 = aoi_bbox
    box_cells = cells(np.array([y0, y0, y1, y1, y0]), np.array([x0, x1, x1, x0, x0]))
    print(f"AOI polygon order-{ORDER} cells: {poly_cells.size};  bbox cells: {box_cells.size}")

    q = Query("ATL03", "007", START, END, region=aoi_bbox, provider="NSIDC_CPRD")
    cmr_ids = set(CMRSource().fetch(q).table.column("id").to_pylist())

    full = pq.read_table(FULL)
    lo = np.datetime64(f"{START}T00:00:00").astype("datetime64[us]").astype("int64")
    hi = np.datetime64(f"{END}T23:59:59").astype("datetime64[us]").astype("int64")
    s, e = full.column("start_datetime"), full.column("end_datetime")
    tsub = full.filter(
        pc.and_(
            pc.less_equal(s, pa.scalar(hi, "int64").cast(s.type)),
            pc.greater_equal(e, pa.scalar(lo, "int64").cast(e.type)),
        )
    )
    bb = tsub.column("bbox")
    cand = tsub.filter(
        pc.and_(
            pc.and_(
                pc.less_equal(pc.struct_field(bb, "xmin"), x1 + 3),
                pc.greater_equal(pc.struct_field(bb, "xmax"), x0 - 3),
            ),
            pc.and_(
                pc.less_equal(pc.struct_field(bb, "ymin"), y1 + 3),
                pc.greater_equal(pc.struct_field(bb, "ymax"), y0 - 3),
            ),
        )
    )
    ids = cand.column("id").to_pylist()
    geoms = cand.column("geometry").to_pylist()

    box_hits, poly_hits = set(), set()
    for gid, wkb in zip(ids, geoms):
        g = shapely.from_wkb(wkb)
        if g.is_empty:
            continue
        poly = g if g.geom_type == "Polygon" else max(g.geoms, key=lambda p: p.area)
        gx, gy = poly.exterior.coords.xy
        fp = cells(np.asarray(gy), np.asarray(gx))
        if fp.size == 0:
            continue
        if np.intersect1d(fp, box_cells, assume_unique=True).size:
            box_hits.add(gid)
        if np.intersect1d(fp, poly_cells, assume_unique=True).size:
            poly_hits.add(gid)

    print(f"\nCMR bbox query                               : {len(cmr_ids)}")
    print(f"sphere-correct footprint ∩ AOI bbox  (mortie): {len(box_hits)}")
    print(f"sphere-correct footprint ∩ AOI polygon(mortie): {len(poly_hits)}")
    print(f"  box_hits  ⊇ CMR? missing={len(cmr_ids - box_hits)}")
    print(f"  poly_hits ⊇ CMR? missing={len(cmr_ids - poly_hits)}")
    print(f"  box_hits over-select vs CMR : {len(box_hits - cmr_ids)}")


if __name__ == "__main__":
    main()
