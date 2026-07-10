"""Build the CONUS order-9 shard map + per-shard stats for the cost estimate (issue #202).

Leg 4a of issue #202: this is the *shard-map reference* for the CONUS cost
estimate. It does NOT dispatch anything and NOT commit a multi-hundred-MB shard
map -- the load-bearing artifacts are the per-shard granule-count table
(``conus_shard_granule_counts.parquet``) and the summary/distribution JSON
(``conus_shard_stats.json``), which the estimate doc and the (blocked) regression
consume.

Pipeline
--------
1. Load the full local ATL03 v007 catalog (``Catalog.from_geoparquet``; 555,867
   granules, mission launch -> 2026-03-15 -- the entire collection).
2. **bbox prefilter** the catalog to the CONUS bounding box before the exact
   polygon intersection. The catalog ``bbox`` column is latitude-exact and
   longitude-conservative (a superset -- README of data/atl03_v007), so an
   overlap test can never drop a real intersector; it just discards the ~90% of
   ICESat-2 granules that are polar/oceanic and can't touch CONUS. (CONUS is
   mid-latitude, so there is no geodesic-sag subtlety here -- ``granule_lat_bounds``
   would give the same latitude cut.)
3. **temporal filter** to ``[start, end]`` (default the full window, a no-op on
   this catalog) via each granule's ``datetime``.
4. Build the o9 shard map over the CONUS polygon (``ShardMap.build``,
   ``backend="mortie"``, ``footprint="swath"``).
5. **Leak sanity check** (mortie #103 guard): assert every covered shard's cell
   centre lies inside the CONUS bbox (+ a one-cell margin). #103 is fixed in
   mortie 0.9.0, so this is a cheap regression guard, not a workaround.
6. Emit the per-shard count parquet + the stats JSON.

Run: ``python data/conus/build_conus_shardmap.py`` (needs the local catalog; no
network, no AWS).
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

DEFAULT_CATALOG = "/Users/espg/software/zagg/data/atl03_v007/atl03_v007_full.parquet"
DEFAULT_CONFIG = "tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml"
HERE = Path(__file__).parent


def _conus_parts(geojson_path: str):
    """Return (polygon_parts, bbox) from the CONUS GeoJSON.

    ``polygon_parts`` is ``[(lats, lons), ...]`` -- one exterior ring per part,
    the form ``HealpixGrid.coverage`` consumes. ``bbox`` is
    ``(lon_min, lat_min, lon_max, lat_max)``.
    """
    import shapely
    from shapely.geometry import shape

    fc = json.loads(Path(geojson_path).read_text())
    geom = shape(fc["features"][0]["geometry"])
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    parts = []
    for p in polys:
        x, y = p.exterior.coords.xy
        parts.append((np.asarray(y), np.asarray(x)))
    minx, miny, maxx, maxy = shapely.bounds(geom)
    return parts, (float(minx), float(miny), float(maxx), float(maxy))


def _bbox_temporal_prefilter(catalog, bbox, start: str, end: str):
    """Subset the catalog to granules whose bbox overlaps CONUS and whose
    datetime is within ``[start, end]``. Returns a new ``Catalog``."""
    import pyarrow.compute as pc

    from zagg.catalog.sources import Catalog

    table = catalog.table
    lon0, lat0, lon1, lat1 = bbox
    # bbox column: struct<xmin, ymin, xmax, ymax> (stac-geoparquet) per granule;
    # lat-exact, lon-conservative (README), so an overlap test drops nothing real.
    bbcol = table.column("bbox")
    g_lon_min = pc.struct_field(bbcol, "xmin").to_numpy(zero_copy_only=False)
    g_lat_min = pc.struct_field(bbcol, "ymin").to_numpy(zero_copy_only=False)
    g_lon_max = pc.struct_field(bbcol, "xmax").to_numpy(zero_copy_only=False)
    g_lat_max = pc.struct_field(bbcol, "ymax").to_numpy(zero_copy_only=False)
    overlap = (g_lon_min <= lon1) & (g_lon_max >= lon0) & (g_lat_min <= lat1) & (g_lat_max >= lat0)
    # Temporal: granule ``datetime`` (tz-aware) -> UTC datetime64 for a numpy
    # compare against the naive-UTC window bounds.
    dt = table.column("datetime").to_numpy(zero_copy_only=False).astype("datetime64[us]")
    in_time = (dt >= np.datetime64(start)) & (dt <= np.datetime64(end + "T23:59:59"))
    mask = overlap & in_time
    idx = np.flatnonzero(mask)
    sub = table.take(idx)
    meta = dict(catalog.metadata or {})
    meta.update(
        collection=meta.get("collection", "ATL03_007"),
        bbox=list(bbox),
        start_date=start,
        end_date=end,
        region="CONUS",
    )
    return Catalog(sub, meta), int(mask.sum()), int(len(mask))


def _leak_check(grid, shard_keys, bbox, margin_deg: float = 1.0) -> dict:
    """Assert every shard cell centre is inside the CONUS bbox (+ margin).

    Guards against a mortie #103-style base-cell fill regression. Returns the
    covered-cell lat/lon extent for the record.
    """
    from mortie.tools import mort2polygon

    lon0, lat0, lon1, lat1 = bbox
    lats, lons = [], []
    for k in shard_keys:
        verts = mort2polygon(int(k), step=4)
        clat = float(np.mean([v[0] for v in verts]))
        clon = float(np.mean([v[1] for v in verts]))
        lats.append(clat)
        lons.append(clon)
    lats = np.asarray(lats)
    lons = np.asarray(lons)
    out_of_box = (
        (lats < lat0 - margin_deg)
        | (lats > lat1 + margin_deg)
        | (lons < lon0 - margin_deg)
        | (lons > lon1 + margin_deg)
    )
    n_leak = int(out_of_box.sum())
    if n_leak:
        bad = [
            (grid.shard_label(int(shard_keys[i])), round(lats[i], 3), round(lons[i], 3))
            for i in np.flatnonzero(out_of_box)[:10]
        ]
        raise AssertionError(
            f"leak check FAILED: {n_leak} shard cell centre(s) outside CONUS bbox "
            f"+{margin_deg} deg (e.g. {bad}) -- possible mortie #103 regression"
        )
    return {
        "passed": True,
        "margin_deg": margin_deg,
        "cell_lat_min": round(float(lats.min()), 4),
        "cell_lat_max": round(float(lats.max()), 4),
        "cell_lon_min": round(float(lons.min()), 4),
        "cell_lon_max": round(float(lons.max()), 4),
    }


def _distribution(counts: list[int]) -> dict:
    arr = np.asarray(sorted(counts))
    all_edges = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
    # Keep only edges the data reaches, then close the top bin at max+1 so the
    # bins stay strictly increasing (a max below the last edge would otherwise
    # make bins non-monotonic).
    hist_edges = [e for e in all_edges if e <= int(arr.max())]
    bins = [*hist_edges, int(arr.max()) + 1] if len(hist_edges) >= 1 else [0, int(arr.max()) + 1]
    hist, _ = np.histogram(arr, bins=bins)
    return {
        "n_shards": int(len(arr)),
        "min": int(arr.min()),
        "median": float(np.median(arr)),
        "mean": round(float(arr.mean()), 2),
        "p90": float(np.percentile(arr, 90)),
        "p99": float(np.percentile(arr, 99)),
        "max": int(arr.max()),
        "total_pairs": int(arr.sum()),
        "histogram_edges": hist_edges,
        "histogram_counts": [int(x) for x in hist],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--catalog", default=DEFAULT_CATALOG)
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--polygon", default=str(HERE / "conus.geojson"))
    ap.add_argument("--start", default="2018-10-13")
    ap.add_argument("--end", default="2026-03-15")
    ap.add_argument("--order", type=int, default=9, help="HEALPix parent_order (shard order)")
    ap.add_argument("--out-parquet", default=str(HERE / "conus_shard_granule_counts.parquet"))
    ap.add_argument("--out-stats", default=str(HERE / "conus_shard_stats.json"))
    args = ap.parse_args(argv)

    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import Catalog
    from zagg.config import load_config
    from zagg.grids import from_config

    print(f"loading catalog {args.catalog} ...", flush=True)
    catalog = Catalog.from_geoparquet(args.catalog)
    parts, bbox = _conus_parts(args.polygon)
    print(
        f"CONUS bbox={tuple(round(b, 3) for b in bbox)}, {len(parts)} polygon part(s)", flush=True
    )

    sub, n_kept, n_total = _bbox_temporal_prefilter(catalog, bbox, args.start, args.end)
    print(f"prefilter: {n_kept:,}/{n_total:,} granules survive bbox+temporal cut", flush=True)

    config = load_config(args.config)
    config.output.setdefault("grid", {})["parent_order"] = args.order
    grid = from_config(config)

    t0 = time.perf_counter()
    sm = ShardMap.build(sub, grid, region=parts, backend="mortie", footprint="swath")
    wall = time.perf_counter() - t0
    print(f"shard map built in {wall:.1f}s: {sm.metadata['total_shards']:,} shards", flush=True)

    counts = [len(g) for g in sm.granules]
    leak = _leak_check(grid, sm.shard_keys, bbox)
    print(f"leak check: {leak}", flush=True)

    # Per-shard count table (the load-bearing artifact -- keeps the shard-key ->
    # granule-count mapping without the multi-hundred-MB href payload).
    import pyarrow as pa
    import pyarrow.parquet as pq

    labels = [grid.shard_label(int(k)) for k in sm.shard_keys]
    table = pa.table(
        {
            "shard_key": pa.array([int(k) for k in sm.shard_keys], type=pa.uint64()),
            "shard_label": pa.array(labels),
            "n_granules": pa.array(counts, type=pa.int32()),
        }
    )
    pq.write_table(table, args.out_parquet)

    dist = _distribution(counts)
    stats = {
        "issue": 202,
        "region": "CONUS (contiguous US, lower-48 + DC)",
        "polygon_reference": {
            "path": "data/conus/conus.geojson",
            "bbox_lonlat": [round(b, 6) for b in bbox],
            "n_parts": len(parts),
            "provenance": json.loads(Path(args.polygon).read_text())["features"][0]["properties"][
                "provenance"
            ],
            "area_km2_epsg5070": json.loads(Path(args.polygon).read_text())["features"][0][
                "properties"
            ]["area_km2_epsg5070"],
        },
        "temporal": {"start": args.start, "end": args.end},
        "grid": {
            "type": "healpix",
            "parent_order": args.order,
            "child_order": int(grid.child_order),
            "mortie_moc_order": sm.metadata.get("mortie_order"),
            "backend": sm.metadata.get("backend"),
            "footprint": sm.metadata.get("footprint"),
        },
        "catalog": {
            "path": args.catalog,
            "total_granules": n_total,
            "granules_after_prefilter": n_kept,
            "granules_intersecting_conus": sm.metadata["total_granules"],
        },
        "summary": {
            "total_shards": sm.metadata["total_shards"],
            "total_granules_intersecting": None,  # set below from distinct pairs' granules
            "total_pairs": sm.metadata["total_pairs"],
            "build_wall_s": round(wall, 1),
        },
        "granules_per_shard": dist,
        "leak_check": leak,
    }
    # distinct granules that land in >=1 CONUS shard
    distinct = {g["id"] for gl in sm.granules for g in gl}
    stats["summary"]["total_granules_intersecting"] = len(distinct)
    Path(args.out_stats).write_text(json.dumps(stats, indent=2))

    print(f"wrote {args.out_parquet} and {args.out_stats}", flush=True)
    print(
        f"SUMMARY: shards={dist['n_shards']:,} granules={len(distinct):,} "
        f"pairs={dist['total_pairs']:,} max/shard={dist['max']:,} "
        f"median/shard={dist['median']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
