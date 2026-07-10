"""Compare CMR vs local-catalog granule ID SETS for the NEON box (issue #202).

Why does CMR return 59 while a local footprint-polygon-intersects-bbox test
returns 158? Compare the actual id sets and characterize the discordant
granules' footprint geometry (area, bbox span) to test the hypothesis that the
stored STAC footprint is a coarse quarter-orbit envelope (#65) far looser than
the orbit geometry CMR searches on internally.

Run: ``uv run python bench/neon_selection_idsets.py``
"""

from __future__ import annotations

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely
from shapely.geometry import box

from zagg.catalog import load_polygon, polygon_to_bbox
from zagg.catalog.sources import CMRSource, Query

REPO = "/Users/espg/software/zagg"
FULL = f"{REPO}/data/atl03_v007/atl03_v007_full.parquet"
AOI = f"{REPO}/tests/data/benchmark/AOP_NEON.geojson"
START, END = "2018-10-13", "2025-06-01"


def main():
    parts = load_polygon(AOI)
    aoi_bbox = polygon_to_bbox(parts)
    aoi_box = box(aoi_bbox[0], aoi_bbox[1], aoi_bbox[2], aoi_bbox[3])

    # --- CMR id set ---
    q = Query("ATL03", "007", START, END, region=aoi_bbox, provider="NSIDC_CPRD")
    cmr = CMRSource().fetch(q)
    cmr_ids = set(cmr.table.column("id").to_pylist())
    print(f"CMR granules: {len(cmr_ids)}")

    # --- local footprint-intersects-bbox id set ---
    full = pq.read_table(FULL)
    lo = np.datetime64(f"{START}T00:00:00").astype("datetime64[us]").astype("int64")
    hi = np.datetime64(f"{END}T23:59:59").astype("datetime64[us]").astype("int64")
    import pyarrow as pa
    s = full.column("start_datetime")
    e = full.column("end_datetime")
    tmask = pc.and_(
        pc.less_equal(s, pa.scalar(hi, "int64").cast(s.type)),
        pc.greater_equal(e, pa.scalar(lo, "int64").cast(e.type)),
    )
    tsub = full.filter(tmask)
    bb = tsub.column("bbox")
    bmask = pc.and_(
        pc.and_(pc.less_equal(pc.struct_field(bb, "xmin"), aoi_bbox[2]),
                pc.greater_equal(pc.struct_field(bb, "xmax"), aoi_bbox[0])),
        pc.and_(pc.less_equal(pc.struct_field(bb, "ymin"), aoi_bbox[3]),
                pc.greater_equal(pc.struct_field(bb, "ymax"), aoi_bbox[1])),
    )
    cand = tsub.filter(bmask)
    ids = cand.column("id").to_pylist()
    geoms = cand.column("geometry").to_pylist()
    local_ids, geom_by_id = set(), {}
    for gid, wkb in zip(ids, geoms):
        g = shapely.from_wkb(wkb)
        if not g.is_empty and g.intersects(aoi_box):
            local_ids.add(gid)
            geom_by_id[gid] = g
    print(f"local footprint-intersects-bbox granules: {len(local_ids)}")

    inter = cmr_ids & local_ids
    cmr_only = cmr_ids - local_ids
    local_only = local_ids - cmr_ids
    print(f"\nCMR & local  : {len(inter)}")
    print(f"CMR only     : {len(cmr_only)}  (in CMR, NOT in local bbox-intersect)")
    print(f"local only   : {len(local_only)}  (in local bbox-intersect, NOT in CMR)")

    def describe(g):
        minx, miny, maxx, maxy = g.bounds
        return f"area={g.area:8.3f} deg^2  lon[{minx:8.3f},{maxx:8.3f}] lat[{miny:7.3f},{maxy:7.3f}]"

    print("\n-- sample of CMR-selected granule footprints (should be tight ground swaths) --")
    for gid in list(inter)[:6]:
        print(f"  {describe(geom_by_id[gid])}  {gid[:60]}")

    print("\n-- sample of LOCAL-ONLY (over-selected) footprints (expect coarse envelopes) --")
    lo_list = sorted(local_only, key=lambda i: -geom_by_id[i].area)
    for gid in lo_list[:8]:
        print(f"  {describe(geom_by_id[gid])}  {gid[:60]}")

    if cmr_only:
        print("\n-- CMR-ONLY ids (unexpected; CMR selects a granule the local bbox test misses) --")
        for gid in list(cmr_only)[:8]:
            print(f"  {gid}")

    areas = np.array([geom_by_id[i].area for i in local_ids])
    print(f"\nfootprint area deg^2 over the 158: median={np.median(areas):.3f} "
          f"p90={np.percentile(areas,90):.3f} max={areas.max():.3f}")
    print(f"CMR-selected areas: median={np.median([geom_by_id[i].area for i in inter]):.3f}")
    print(f"local-only areas  : median={np.median([geom_by_id[i].area for i in local_only]):.3f}")


if __name__ == "__main__":
    main()
