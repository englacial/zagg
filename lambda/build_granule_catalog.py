#!/usr/bin/env python3
"""
Build a local granule catalog from CMR to avoid per-Lambda CMR queries.

This script:
1. Queries CMR ONCE for all ATL06 granules in a cycle, south of 60°S
2. Extracts track geometry from each granule
3. Converts geometry points to morton indices at the parent order
4. Builds a mapping: parent_morton → [granule S3 URLs]

Usage:
    python build_granule_catalog.py --cycle 22 --parent-order 6
    python build_granule_catalog.py --cycle 22 --parent-order 6 --output catalog.json
"""

import argparse
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set

import numpy as np
import requests
from mortie import clip2order, geo2mort


def densify_polygon(lats: np.ndarray, lons: np.ndarray, max_spacing_km: float = 5.0) -> tuple:
    """
    Densify polygon by interpolating points between vertices in polar stereographic space.

    At finer morton orders, sparse CMR polygon vertices miss cells that
    the actual track passes through. This function projects to Antarctic
    polar stereographic (EPSG:3031), interpolates linearly in XY, then projects back.

    Parameters
    ----------
    lats : np.ndarray
        Latitude values of polygon vertices
    lons : np.ndarray
        Longitude values of polygon vertices
    max_spacing_km : float
        Maximum spacing between points in kilometers (default 5km)

    Returns
    -------
    tuple
        (dense_lats, dense_lons) arrays with interpolated points
    """
    from pyproj import Transformer

    # EPSG:4326 (WGS84) <-> EPSG:3031 (Antarctic Polar Stereographic)
    to_stereo = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)
    to_latlon = Transformer.from_crs("EPSG:3031", "EPSG:4326", always_xy=True)

    # Project all vertices to stereo (lon, lat order for always_xy=True)
    x_verts, y_verts = to_stereo.transform(lons, lats)

    dense_lats = []
    dense_lons = []

    for i in range(len(lats)):
        x1, y1 = x_verts[i], y_verts[i]
        x2, y2 = x_verts[(i + 1) % len(lats)], y_verts[(i + 1) % len(lats)]

        # Distance in stereo space (meters)
        dist_m = np.sqrt((x2 - x1)**2 + (y2 - y1)**2)
        dist_km = dist_m / 1000.0

        n_points = max(2, int(dist_km / max_spacing_km) + 1)

        # Interpolate in stereo space
        for j in range(n_points - 1):
            t = j / (n_points - 1)
            x_interp = x1 + t * (x2 - x1)
            y_interp = y1 + t * (y2 - y1)

            # Project back to lat/lon
            lon_interp, lat_interp = to_latlon.transform(x_interp, y_interp)
            dense_lats.append(lat_interp)
            dense_lons.append(lon_interp)

    return np.array(dense_lats), np.array(dense_lons)


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
        Southern latitude bound (default -60°)
    page_size : int
        Results per page

    Returns
    -------
    list
        List of granule metadata dicts
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    # Calculate temporal bounds for cycle
    launch_date = datetime(2018, 10, 13)
    cycle_duration = 91
    cycle_start = launch_date + timedelta(days=(cycle - 1) * cycle_duration)
    cycle_end = cycle_start + timedelta(days=cycle_duration + 1)
    temporal = f"{cycle_start.strftime('%Y-%m-%d')}T00:00:00Z,{cycle_end.strftime('%Y-%m-%d')}T23:59:59Z"

    # Bounding box for Antarctica: whole globe longitude, south of -60
    # Format: west,south,east,north
    bounding_box = f"-180,{-90},-180,{south_of},180,{south_of},180,{-90},-180,{-90}"
    # Actually CMR wants simple bbox: west,south,east,north
    bbox = f"-180,-90,180,{south_of}"

    params = {
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

    print(f"Querying CMR for ATL06 v{version} cycle {cycle}")
    print(f"  Temporal: {temporal}")
    print(f"  Bounding box: {bbox}")

    all_granules = []
    total_hits = None

    while True:
        response = requests.get(cmr_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        if total_hits is None:
            total_hits = int(response.headers.get("CMR-Hits", 0))
            print(f"  Total matching granules: {total_hits}")

        data = response.json()
        items = data.get("items", [])

        if not items:
            break

        all_granules.extend(items)
        print(f"  Retrieved {len(all_granules)}/{total_hits}...", end="\r")

        if len(items) < page_size or len(all_granules) >= total_hits:
            break

        params["offset"] += len(items)
        time.sleep(0.1)  # Be nice to CMR

    print(f"\nRetrieved {len(all_granules)} granules from CMR")
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

    # Parse RGT, cycle, region from granule ID
    # Format: ATL06_20240107023115_02341901_007_01.h5
    parts = granule_id.split("_")
    rgt, cycle, region = None, None, None
    if len(parts) >= 3:
        rgt_cycle_region = parts[2]
        if len(rgt_cycle_region) >= 8:
            rgt = int(rgt_cycle_region[0:4])
            cycle = int(rgt_cycle_region[4:6])
            region = int(rgt_cycle_region[6:8])

    # Get S3 URL
    related_urls = umm.get("RelatedUrls", [])
    s3_url = None
    for url_obj in related_urls:
        url = url_obj.get("URL", "")
        if url.startswith("s3://") and url.endswith(".h5"):
            s3_url = url
            break

    # Get geometry points from GPolygons
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


def build_morton_catalog(
    granules: List[dict],
    parent_order: int,
    densify: bool = True,
    densify_spacing_km: float = 5.0,
) -> Dict[int, List[str]]:
    """
    Build a catalog mapping parent morton cells to granule S3 URLs.

    For each granule:
    1. Convert its geometry points to morton indices at order 18
    2. Clip to parent_order
    3. Get unique parent cells
    4. Add granule URL to each parent cell's list

    Parameters
    ----------
    granules : list
        List of granule metadata from CMR
    parent_order : int
        Morton order for parent cells (e.g., 6 or 7)
    densify : bool
        If True, interpolate points along polygon edges to capture all cells
        the track passes through (default True)
    densify_spacing_km : float
        Maximum spacing between interpolated points in km (default 5.0)

    Returns
    -------
    dict
        Mapping of parent_morton (int) → list of S3 URLs
    """
    catalog: Dict[int, Set[str]] = {}
    granules_processed = 0
    granules_with_geometry = 0

    print(f"\nBuilding morton catalog at order {parent_order}...")

    for i, granule in enumerate(granules):
        info = extract_granule_info(granule)

        if not info["s3_url"]:
            continue

        if not info["points"]:
            continue

        granules_with_geometry += 1

        # Convert points to morton indices
        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])

        # Optionally densify polygon to capture all cells the track passes through
        # At finer orders, sparse CMR vertices miss intermediate cells
        if densify:
            lats, lons = densify_polygon(lats, lons, max_spacing_km=densify_spacing_km)

        # Get morton indices at order 18, then clip to parent order
        morton_18 = geo2mort(lats, lons, order=18)
        morton_parent = clip2order(parent_order, morton_18)

        # Get unique parent cells this granule touches
        unique_cells = set(int(m) for m in morton_parent)

        # Add granule to each cell's list
        for cell in unique_cells:
            if cell not in catalog:
                catalog[cell] = set()
            catalog[cell].add(info["s3_url"])

        granules_processed += 1

        if (i + 1) % 500 == 0:
            print(f"  Processed {i + 1}/{len(granules)} granules, {len(catalog)} cells...", end="\r")

    print(f"\nProcessed {granules_processed} granules with geometry")
    print(f"  Total unique parent cells: {len(catalog)}")

    # Convert sets to lists for JSON serialization
    return {k: list(v) for k, v in catalog.items()}


def main():
    parser = argparse.ArgumentParser(description="Build granule catalog from CMR")
    parser.add_argument("--cycle", type=int, required=True, help="ICESat-2 cycle number")
    parser.add_argument("--parent-order", type=int, default=6, help="Parent morton order")
    parser.add_argument("--version", default="007", help="ATL06 version")
    parser.add_argument("--output", default=None, help="Output JSON file path")
    parser.add_argument("--south-of", type=float, default=-60.0, help="Southern latitude bound")
    parser.add_argument("--no-densify", action="store_true", help="Disable polygon densification")
    parser.add_argument("--densify-spacing", type=float, default=5.0, help="Densification spacing in km (default 5)")
    args = parser.parse_args()

    start_time = time.time()

    # Query CMR once
    granules = query_cmr_antarctica(
        cycle=args.cycle,
        version=args.version,
        south_of=args.south_of,
    )

    if not granules:
        print("No granules found!")
        return

    # Build morton catalog
    densify = not args.no_densify
    catalog = build_morton_catalog(
        granules,
        args.parent_order,
        densify=densify,
        densify_spacing_km=args.densify_spacing,
    )

    # Summary stats
    granule_counts = [len(urls) for urls in catalog.values()]
    print(f"\nCatalog statistics:")
    print(f"  Parent cells: {len(catalog)}")
    print(f"  Granules per cell: min={min(granule_counts)}, max={max(granule_counts)}, avg={np.mean(granule_counts):.1f}")

    # Save catalog
    output_file = args.output or f"granule_catalog_cycle{args.cycle}_order{args.parent_order}.json"

    output_data = {
        "metadata": {
            "cycle": args.cycle,
            "parent_order": args.parent_order,
            "version": args.version,
            "south_of": args.south_of,
            "densify": densify,
            "densify_spacing_km": args.densify_spacing if densify else None,
            "total_granules": len(granules),
            "total_cells": len(catalog),
            "created": datetime.now().isoformat(),
        },
        "catalog": {str(k): v for k, v in catalog.items()},  # JSON keys must be strings
    }

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    elapsed = time.time() - start_time
    print(f"\nCatalog saved to: {output_file}")
    print(f"Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
