#!/usr/bin/env python3
"""
Build a local granule catalog from CMR to avoid per-Lambda CMR queries.

This script:
1. Queries CMR for granules matching a date range, product, and spatial extent
2. Discovers parent morton cells via morton_coverage on an input polygon
3. Intersects cells with granule polygons via shapely STRtree
4. Builds a mapping: parent_morton -> [granule S3 URLs]

Usage:
    # General (date range + polygon):
    python -m zagg.catalog --start-date 2024-01-06 --end-date 2024-04-07 \\
        --polygon antarctica.geojson --parent-order 6

    # ICESat-2 convenience (cycle computes dates automatically):
    python -m zagg.catalog --cycle 22 --parent-order 6

    # Custom product:
    python -m zagg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \\
        --short-name ATL08 --polygon my_region.geojson --parent-order 6
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from importlib import resources
from typing import Dict, List

import numpy as np
import requests
from mortie import morton_coverage
from mortie.tools import mort2polygon

logger = logging.getLogger(__name__)

# ICESat-2 constants
_ICESAT2_LAUNCH = datetime(2018, 10, 13)
_ICESAT2_CYCLE_DAYS = 91


def cycle_to_dates(cycle: int) -> tuple[datetime, datetime]:
    """
    Convert an ICESat-2 repeat cycle number to a date range.

    Parameters
    ----------
    cycle : int
        ICESat-2 cycle number (1-based)

    Returns
    -------
    tuple of (start_date, end_date)
        Start and end datetimes for the cycle
    """
    start = _ICESAT2_LAUNCH + timedelta(days=(cycle - 1) * _ICESAT2_CYCLE_DAYS)
    end = start + timedelta(days=_ICESAT2_CYCLE_DAYS)
    return start, end


def load_polygon(geojson_path: str) -> list[tuple]:
    """
    Load polygon(s) from a GeoJSON file.

    Supports Feature, FeatureCollection, Polygon, and MultiPolygon geometries.

    Parameters
    ----------
    geojson_path : str
        Path to a GeoJSON file

    Returns
    -------
    list of (lats, lons)
        One (lats, lons) array pair per polygon ring, suitable for
        morton_coverage multipart input.
    """
    from shapely.geometry import shape

    with open(geojson_path) as f:
        geojson = json.load(f)

    if geojson.get("type") == "FeatureCollection":
        features = geojson["features"]
    elif geojson.get("type") == "Feature":
        features = [geojson]
    else:
        features = [{"geometry": geojson}]

    parts = []
    for feat in features:
        geom = shape(feat["geometry"])
        if geom.geom_type == "Polygon":
            coords = np.array(geom.exterior.coords)
            parts.append((coords[:, 1], coords[:, 0]))  # GeoJSON is (lon, lat)
        elif geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                coords = np.array(poly.exterior.coords)
                parts.append((coords[:, 1], coords[:, 0]))
    return parts


def polygon_to_bbox(parts: list[tuple]) -> tuple[float, float, float, float]:
    """
    Compute a bounding box from polygon parts.

    Parameters
    ----------
    parts : list of (lats, lons)
        Polygon parts as returned by load_polygon

    Returns
    -------
    tuple of (lon_min, lat_min, lon_max, lat_max)
        Bounding box in CMR format
    """
    all_lats = np.concatenate([p[0] for p in parts])
    all_lons = np.concatenate([p[1] for p in parts])
    return (
        float(all_lons.min()),
        float(all_lats.min()),
        float(all_lons.max()),
        float(all_lats.max()),
    )


def load_antarctic_basins(filepath=None):
    """
    Load Antarctic drainage basin polygons.

    Parameters
    ----------
    filepath : str, optional
        Path to basin polygon file. Defaults to the file shipped with mortie.

    Returns
    -------
    list of (lats, lons)
        One (lats, lons) pair per basin, suitable for morton_coverage multipart input.
    """
    import pandas as pd

    if filepath is None:
        import mortie.tests

        filepath = str(
            resources.files(mortie.tests) / "Ant_Grounded_DrainageSystem_Polygons.txt"
        )

    df = pd.read_csv(filepath, names=["Lat", "Lon", "basin"], sep=r"\s+")
    basins = []
    for _, group in df.groupby("basin"):
        basins.append((group["Lat"].values, group["Lon"].values))
    return basins


def discover_cells(parent_order, polygon_parts=None):
    """
    Discover morton cells at parent_order covering a polygon.

    Parameters
    ----------
    parent_order : int
        Morton order for parent cells (e.g., 6)
    polygon_parts : list of (lats, lons), optional
        Polygon parts for coverage. Defaults to Antarctic drainage basins.

    Returns
    -------
    numpy.ndarray
        Sorted array of unique morton indices at parent_order
    """
    if polygon_parts is None:
        polygon_parts = load_antarctic_basins()

    lats_parts = [p[0] for p in polygon_parts]
    lons_parts = [p[1] for p in polygon_parts]
    cells = morton_coverage(lats_parts, lons_parts, order=parent_order)
    logger.info(
        f"Cell discovery: {len(cells)} cells at order {parent_order} from {len(polygon_parts)} parts"
    )
    return cells


def query_cmr(
    start_date: str,
    end_date: str,
    short_name: str = "ATL06",
    version: str = "007",
    provider: str = "NSIDC_CPRD",
    bbox: tuple = None,
    page_size: int = 2000,
) -> List[dict]:
    """
    Query CMR for granules matching temporal and spatial filters.

    Parameters
    ----------
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    short_name : str
        CMR short name (e.g., ATL06, ATL08)
    version : str
        Product version
    provider : str
        CMR provider
    bbox : tuple of (lon_min, lat_min, lon_max, lat_max), optional
        Bounding box filter
    page_size : int
        Results per page

    Returns
    -------
    list
        List of granule metadata dicts
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    temporal = f"{start_date}T00:00:00Z,{end_date}T23:59:59Z"

    params: Dict[str, str | int] = {
        "provider": provider,
        "short_name": short_name,
        "version": version,
        "page_size": page_size,
        "sort_key": "start_date",
        "temporal": temporal,
        "offset": 0,
    }

    if bbox is not None:
        params["bounding_box"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

    headers = {"Accept": "application/vnd.nasa.cmr.umm_json+json"}

    logger.info(f"Querying CMR for {short_name} v{version}")
    logger.info(f"  Temporal: {temporal}")
    if bbox:
        logger.info(f"  Bounding box: {params['bounding_box']}")

    all_granules = []
    total_hits = None

    while True:
        response = requests.get(cmr_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        if total_hits is None:
            total_hits = int(response.headers.get("CMR-Hits", 0))
            logger.info(f"  Total matching granules: {total_hits}")

        data = response.json()
        items = data.get("items", [])

        if not items:
            break

        all_granules.extend(items)
        logger.info(f"  Retrieved {len(all_granules)}/{total_hits}...")

        if len(items) < page_size or len(all_granules) >= total_hits:
            break

        params["offset"] = int(params["offset"]) + len(items)
        time.sleep(0.1)  # Be nice to CMR

    logger.info(f"Retrieved {len(all_granules)} granules from CMR")
    return all_granules


def extract_granule_info(granule: dict) -> dict:
    """
    Extract S3 URL and geometry points from a CMR granule.

    Parameters
    ----------
    granule : dict
        UMM-JSON granule from CMR

    Returns
    -------
    dict
        Keys: granule_id, s3_url, points (list of (lat, lon) tuples)
    """
    umm = granule.get("umm", {})
    granule_id = umm.get("GranuleUR", "")

    related_urls = umm.get("RelatedUrls", [])
    s3_url = None
    https_url = None
    for url_obj in related_urls:
        url = url_obj.get("URL", "")
        if url.startswith("s3://") and url.endswith(".h5"):
            s3_url = url
        elif url.startswith("https://") and url.endswith(".h5") and url_obj.get("Type") == "GET DATA":
            https_url = url

    points = []
    spatial_extent = umm.get("SpatialExtent", {})
    horiz_spatial = spatial_extent.get("HorizontalSpatialDomain", {})
    geometry = horiz_spatial.get("Geometry", {})
    gpolygons = geometry.get("GPolygons", [])

    if gpolygons:
        boundary = gpolygons[0].get("Boundary", {})
        boundary_points = boundary.get("Points", [])
        for p in boundary_points:
            if "Latitude" in p and "Longitude" in p:
                points.append((p["Latitude"], p["Longitude"]))

    return {
        "granule_id": granule_id,
        "s3_url": s3_url,
        "https_url": https_url,
        "points": points,
    }


def build_catalog(
    granules: List[dict],
    parent_order: int,
    polygon_parts: list = None,
) -> tuple:
    """
    Build a granule catalog using morton_coverage for cell discovery
    and shapely STRtree for granule-to-cell intersection.

    Parameters
    ----------
    granules : list
        List of granule metadata from CMR
    parent_order : int
        Morton order for parent cells (e.g., 6)
    polygon_parts : list of (lats, lons), optional
        Polygon parts for cell discovery. Defaults to Antarctic drainage basins.

    Returns
    -------
    catalog : dict
        Mapping of parent_morton (int) -> list of S3 URLs
    timings : dict
        Wall-clock seconds for each pipeline step
    """
    from pyproj import Transformer
    from shapely import STRtree, make_valid
    from shapely.geometry import Polygon

    timings = {}
    t_total = time.perf_counter()

    to_stereo = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)

    # --- Pass 1: Cell discovery via morton_coverage ---
    t0 = time.perf_counter()
    all_cells = discover_cells(parent_order, polygon_parts)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_cells)} cells")

    # --- Build granule polygons in EPSG:3031 ---
    t0 = time.perf_counter()
    granule_polys = []
    granule_urls = []

    for granule in granules:
        info = extract_granule_info(granule)
        if not info["s3_url"] or len(info["points"]) < 3:
            continue

        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])

        x, y = to_stereo.transform(lons, lats)
        coords = list(zip(x, y))
        try:
            poly = Polygon(coords)
            if not poly.is_valid:
                poly = make_valid(poly)
            if poly.is_empty:
                continue
        except Exception:
            continue

        granule_polys.append(poly)
        granule_urls.append(info["s3_url"])

    timings["granule_polygons"] = time.perf_counter() - t0
    logger.info(f"Built {len(granule_polys)} granule polygons")

    # --- STRtree construction ---
    t0 = time.perf_counter()
    tree = STRtree(granule_polys)
    timings["strtree_construction"] = time.perf_counter() - t0

    # --- Cell polygons via mort2polygon ---
    t0 = time.perf_counter()
    cell_shapely = []
    for cell_id in all_cells:
        verts = mort2polygon(int(cell_id), step=32)
        lats_c = np.array([v[0] for v in verts])
        lons_c = np.array([v[1] for v in verts])
        x, y = to_stereo.transform(lons_c, lats_c)
        poly = Polygon(zip(x, y))
        if not poly.is_valid:
            poly = make_valid(poly)
        cell_shapely.append(poly)
    timings["cell_polygons"] = time.perf_counter() - t0

    # --- STRtree queries ---
    t0 = time.perf_counter()
    catalog: Dict[int, List[str]] = {}
    for i, cell_id in enumerate(all_cells):
        hits = tree.query(cell_shapely[i], predicate="intersects")
        if len(hits) > 0:
            catalog[int(cell_id)] = [granule_urls[j] for j in hits]
    timings["strtree_queries"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total
    logger.info(f"Pass 2: {len(catalog)} cells with granule mappings")

    return catalog, timings


def _extract_base_urls(granules: list) -> dict:
    """Extract S3 and HTTPS base URLs from the first granule with both.

    The base URLs are stored in catalog metadata so the runner can rewrite
    S3 URLs to HTTPS URLs at runtime without hardcoding provider paths.

    The S3 URL ``s3://bucket/path/file.h5`` maps to the HTTPS URL
    ``https://host/bucket/path/file.h5``. The file path (everything after
    the bucket name) is identical in both, so we extract the S3 bucket
    prefix and the HTTPS host+bucket prefix.

    Parameters
    ----------
    granules : list
        CMR granule list (only the first with both URLs is used).

    Returns
    -------
    dict
        ``{"s3_base": "s3://bucket", "https_base": "https://host/bucket"}``
        or empty.
    """
    for granule in granules:
        info = extract_granule_info(granule)
        s3_url = info.get("s3_url")
        https_url = info.get("https_url")
        if s3_url and https_url:
            # S3: s3://bucket/path/file.h5 → bucket
            s3_after = s3_url.split("//", 1)[1]  # bucket/path/file.h5
            s3_bucket = s3_after.split("/", 1)[0]  # bucket
            s3_base = f"s3://{s3_bucket}"

            # HTTPS: https://host/bucket/path/file.h5 → https://host/bucket
            https_after = https_url.split("//", 1)[1]  # host/bucket/path/file.h5
            # Find where the bucket name appears in the HTTPS path
            bucket_idx = https_after.find(f"/{s3_bucket}")
            if bucket_idx >= 0:
                https_base = "https://" + https_after[: bucket_idx + 1 + len(s3_bucket)]
            else:
                # Bucket name not in HTTPS path — can't derive mapping
                continue
            return {"s3_base": s3_base, "https_base": https_base}
    return {}


def main():
    parser = argparse.ArgumentParser(
        description="Build granule catalog from CMR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # ICESat-2 cycle (convenience):
  python -m zagg.catalog --cycle 22 --parent-order 6

  # Explicit date range:
  python -m zagg.catalog --start-date 2024-01-06 --end-date 2024-04-07 --parent-order 6

  # Custom region and product:
  python -m zagg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \\
      --short-name ATL08 --polygon my_region.geojson --parent-order 6
""",
    )

    # Temporal
    temporal = parser.add_argument_group("temporal (choose one)")
    temporal.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    temporal.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    temporal.add_argument(
        "--cycle", type=int, help="ICESat-2 cycle number (computes dates automatically)"
    )

    # Product
    parser.add_argument("--short-name", default="ATL06", help="CMR short name (default: ATL06)")
    parser.add_argument("--version", default="007", help="Product version (default: 007)")
    parser.add_argument("--provider", default="NSIDC_CPRD", help="CMR provider")

    # Spatial
    spatial = parser.add_argument_group("spatial")
    spatial.add_argument(
        "--polygon",
        help="GeoJSON file for area of interest (used for cell discovery and CMR bbox)",
    )
    spatial.add_argument(
        "--bbox",
        help="Bounding box override: lon_min,lat_min,lon_max,lat_max",
    )

    # Output
    parser.add_argument("--parent-order", type=int, default=6, help="Parent morton order")
    parser.add_argument("--output", default=None, help="Output JSON file path")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # --- Resolve temporal ---
    if args.cycle:
        start_dt, end_dt = cycle_to_dates(args.cycle)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        parser.error("Provide either --cycle or both --start-date and --end-date")

    # --- Resolve spatial ---
    polygon_parts = None
    bbox = None

    if args.polygon:
        polygon_parts = load_polygon(args.polygon)
        bbox = polygon_to_bbox(polygon_parts)
        logger.info(f"Loaded polygon from {args.polygon}: {len(polygon_parts)} parts")
        logger.info(f"  Auto-computed bbox: {bbox}")

    if args.bbox:
        bbox = tuple(float(x) for x in args.bbox.split(","))

    # --- Query CMR ---
    start_time = time.time()

    granules = query_cmr(
        start_date=start_date,
        end_date=end_date,
        short_name=args.short_name,
        version=args.version,
        provider=args.provider,
        bbox=bbox,
    )

    if not granules:
        print("No granules found!")
        return

    # --- Build catalog ---
    catalog, timings = build_catalog(granules, args.parent_order, polygon_parts)

    print("\nTimings:")
    for step, sec in timings.items():
        print(f"  {step}: {sec:.3f}s")

    granule_counts = [len(urls) for urls in catalog.values()]
    print("\nCatalog statistics:")
    print(f"  Parent cells: {len(catalog)}")
    print(
        f"  Granules per cell: min={min(granule_counts)}, "
        f"max={max(granule_counts)}, avg={np.mean(granule_counts):.1f}"
    )

    # --- Save catalog ---
    if args.output:
        output_file = args.output
    elif args.cycle:
        output_file = f"catalog_{args.short_name}_cycle{args.cycle}_order{args.parent_order}.json"
    else:
        output_file = (
            f"catalog_{args.short_name}_{start_date}_{end_date}_order{args.parent_order}.json"
        )

    # Derive base URLs from first granule for driver URL rewriting
    access_urls = _extract_base_urls(granules)

    output_metadata = {
        "short_name": args.short_name,
        "version": args.version,
        "provider": args.provider,
        "start_date": start_date,
        "end_date": end_date,
        "parent_order": args.parent_order,
        "total_granules": len(granules),
        "total_cells": len(catalog),
        "created": datetime.now().isoformat(),
        **access_urls,
    }
    if args.cycle:
        output_metadata["cycle"] = args.cycle
    if bbox:
        output_metadata["bbox"] = list(bbox)
    if args.polygon:
        output_metadata["polygon"] = args.polygon

    output_data = {
        "metadata": output_metadata,
        "catalog": {str(k): v for k, v in catalog.items()},
    }

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    elapsed = time.time() - start_time
    print(f"\nCatalog saved to: {output_file}")
    print(f"Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
