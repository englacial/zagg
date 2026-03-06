#!/usr/bin/env python3
"""
Build a local granule catalog from CMR to avoid per-Lambda CMR queries.

This script:
1. Queries CMR ONCE for all ATL06 granules in a cycle, south of 60S
2. Discovers parent morton cells via morton_coverage on Antarctic drainage basins
3. Intersects cells with granule polygons via shapely STRtree
4. Builds a mapping: parent_morton -> [granule S3 URLs]

Usage:
    python -m magg.catalog --cycle 22 --parent-order 6
    python -m magg.catalog --cycle 22 --parent-order 6 --output catalog.json
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

# Default path to Antarctic drainage basin polygon file (shipped with mortie tests)
_BASIN_FILE = None


def _get_basin_file():
    global _BASIN_FILE
    if _BASIN_FILE is None:
        import mortie.tests

        _BASIN_FILE = str(
            resources.files(mortie.tests) / "Ant_Grounded_DrainageSystem_Polygons.txt"
        )
    return _BASIN_FILE


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

    filepath = filepath or _get_basin_file()
    df = pd.read_csv(filepath, names=["Lat", "Lon", "basin"], sep=r"\s+")
    basins = []
    for _, group in df.groupby("basin"):
        basins.append((group["Lat"].values, group["Lon"].values))
    return basins


def discover_cells(parent_order, basin_file=None):
    """
    Discover all morton cells at parent_order that cover Antarctica.

    Uses mortie.morton_coverage on each of the 27 Antarctic drainage basins,
    then unions the results.

    Parameters
    ----------
    parent_order : int
        Morton order for parent cells (e.g., 6)
    basin_file : str, optional
        Path to basin polygon file

    Returns
    -------
    numpy.ndarray
        Sorted array of unique morton indices at parent_order
    """
    basins = load_antarctic_basins(basin_file)
    lats_parts = [b[0] for b in basins]
    lons_parts = [b[1] for b in basins]
    cells = morton_coverage(lats_parts, lons_parts, order=parent_order)
    logger.info(f"Cell discovery: {len(cells)} cells at order {parent_order} from {len(basins)} basins")
    return cells


def query_cmr_antarctica(
    cycle: int,
    version: str = "007",
    provider: str = "NSIDC_CPRD",
    south_of: float = -60.0,
    page_size: int = 2000,
) -> List[dict]:
    """
    Query CMR for ALL ATL06 granules in Antarctica for a given cycle.

    Parameters
    ----------
    cycle : int
        ICESat-2 cycle number
    version : str
        ATL06 version
    provider : str
        CMR provider
    south_of : float
        Southern latitude bound (default -60)
    page_size : int
        Results per page

    Returns
    -------
    list
        List of granule metadata dicts
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    launch_date = datetime(2018, 10, 13)
    cycle_duration = 91
    cycle_start = launch_date + timedelta(days=(cycle - 1) * cycle_duration)
    cycle_end = cycle_start + timedelta(days=cycle_duration + 1)
    temporal = (
        f"{cycle_start.strftime('%Y-%m-%d')}T00:00:00Z,{cycle_end.strftime('%Y-%m-%d')}T23:59:59Z"
    )

    bbox = f"-180,-90,180,{south_of}"

    params: Dict[str, str | int] = {
        "provider": provider,
        "short_name": "ATL06",
        "version": version,
        "page_size": page_size,
        "sort_key": "start_date",
        "temporal": temporal,
        "bounding_box": bbox,
        "offset": 0,
    }

    headers = {"Accept": "application/vnd.nasa.cmr.umm_json+json"}

    logger.info(f"Querying CMR for ATL06 v{version} cycle {cycle}")
    logger.info(f"  Temporal: {temporal}")
    logger.info(f"  Bounding box: {bbox}")

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
    Extract S3 URL and geometry points from a granule.

    Returns
    -------
    dict with keys:
        - granule_id: str
        - s3_url: str or None
        - points: list of (lat, lon) tuples
        - rgt: int
        - cycle: int
        - region: int
    """
    umm = granule.get("umm", {})
    granule_id = umm.get("GranuleUR", "")

    parts = granule_id.split("_")
    rgt, cycle, region = None, None, None
    if len(parts) >= 3:
        rgt_cycle_region = parts[2]
        if len(rgt_cycle_region) >= 8:
            rgt = int(rgt_cycle_region[0:4])
            cycle = int(rgt_cycle_region[4:6])
            region = int(rgt_cycle_region[6:8])

    related_urls = umm.get("RelatedUrls", [])
    s3_url = None
    for url_obj in related_urls:
        url = url_obj.get("URL", "")
        if url.startswith("s3://") and url.endswith(".h5"):
            s3_url = url
            break

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
        "points": points,
        "rgt": rgt,
        "cycle": cycle,
        "region": region,
    }


def build_catalog(
    granules: List[dict],
    parent_order: int,
    basin_file: str = None,
) -> tuple:
    """
    Build a granule catalog using morton_coverage for cell discovery
    and shapely STRtree for granule-to-cell intersection.

    Two passes:
    1. Discover all parent cells covering Antarctica via morton_coverage
       on the 27 Antarctic drainage basins.
    2. Build granule polygons in EPSG:3031 and use STRtree to intersect
       each cell with granule polygons.

    Parameters
    ----------
    granules : list
        List of granule metadata from CMR
    parent_order : int
        Morton order for parent cells (e.g., 6)
    basin_file : str, optional
        Path to Antarctic drainage basin polygon file

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
    all_cells = discover_cells(parent_order, basin_file)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_cells)} cells from drainage basins")

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


def main():
    parser = argparse.ArgumentParser(description="Build granule catalog from CMR")
    parser.add_argument("--cycle", type=int, required=True, help="ICESat-2 cycle number")
    parser.add_argument("--parent-order", type=int, default=6, help="Parent morton order")
    parser.add_argument("--version", default="007", help="ATL06 version")
    parser.add_argument("--output", default=None, help="Output JSON file path")
    parser.add_argument("--south-of", type=float, default=-60.0, help="Southern latitude bound")
    parser.add_argument(
        "--basin-file", default=None, help="Path to Antarctic drainage basin polygon file"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    start_time = time.time()

    # Query CMR
    granules = query_cmr_antarctica(
        cycle=args.cycle,
        version=args.version,
        south_of=args.south_of,
    )

    if not granules:
        print("No granules found!")
        return

    # Build catalog
    catalog, timings = build_catalog(granules, args.parent_order, args.basin_file)

    print("\nTimings:")
    for step, sec in timings.items():
        print(f"  {step}: {sec:.3f}s")

    # Summary stats
    granule_counts = [len(urls) for urls in catalog.values()]
    print("\nCatalog statistics:")
    print(f"  Parent cells: {len(catalog)}")
    print(
        f"  Granules per cell: min={min(granule_counts)}, max={max(granule_counts)}, avg={np.mean(granule_counts):.1f}"
    )

    # Save catalog
    output_file = args.output or f"granule_catalog_cycle{args.cycle}_order{args.parent_order}.json"

    output_data = {
        "metadata": {
            "cycle": args.cycle,
            "parent_order": args.parent_order,
            "version": args.version,
            "south_of": args.south_of,
            "method": "morton_coverage",
            "total_granules": len(granules),
            "total_cells": len(catalog),
            "created": datetime.now().isoformat(),
        },
        "catalog": {str(k): v for k, v in catalog.items()},
    }

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    elapsed = time.time() - start_time
    print(f"\nCatalog saved to: {output_file}")
    print(f"Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
