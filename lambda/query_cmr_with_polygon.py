#!/usr/bin/env python3
"""
Modified query_atl06_cmr function that accepts a polygon for server-side CMR filtering.
NASA's CMR API performs the spatial subsetting on their side.
"""
from __future__ import annotations

import time
import requests
from typing import List, Optional, Dict, Union, Tuple
import pandas as pd
try:
    import geopandas as gpd
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False
from shapely.geometry import box, Polygon, LineString, Point
from shapely import wkt
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


def _cmr_request_with_retry(url, params, headers, max_retries=15):
    """
    Make CMR request with retry logic for transient errors.

    Retries on 5xx server errors and connection errors with exponential backoff.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            # Retry on 5xx server errors
            if response.status_code >= 500:
                last_error = f"{response.status_code} Server Error: {response.text[:100]}"
                if attempt < max_retries - 1:
                    sleep_time = (2 ** attempt) + (time.time() % 1)  # Exponential backoff with jitter
                    time.sleep(sleep_time)
                    continue
                response.raise_for_status()
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            # Retry on connection/timeout errors
            if attempt < max_retries - 1:
                sleep_time = (2 ** attempt) + (time.time() % 1)
                time.sleep(sleep_time)
            else:
                raise requests.HTTPError(f"CMR request failed after {max_retries} retries: {last_error}")
    raise requests.HTTPError(f"CMR request failed after {max_retries} retries: {last_error}")


def format_polygon_for_cmr(polygon: Union[Polygon, List[Tuple[float, float]], List[List[float]], str]) -> str:
    """
    Format a polygon for CMR API spatial query.

    CMR expects polygons as a comma-separated list of lon,lat pairs.
    The polygon must be closed (first point = last point).

    Parameters
    ----------
    polygon : Union[Polygon, List[Tuple[float, float]], List[List[float]], str]
        Polygon as Shapely object, list of (lon, lat) tuples/lists, or WKT string

    Returns
    -------
    str
        Comma-separated string of lon,lat pairs for CMR
    """
    if isinstance(polygon, str):
        # Check if it's already a CMR-formatted string (contains only numbers, commas, periods, and minus signs)
        import re
        if re.match(r'^[\d,.\-\s]+$', polygon):
            # Already formatted for CMR
            return polygon
        # Otherwise try to parse as WKT
        polygon = wkt.loads(polygon)

    if isinstance(polygon, Polygon):
        # Extract exterior coordinates from Shapely polygon (already in lon,lat order)
        coords = list(polygon.exterior.coords)
        # Format as comma-separated string (lon,lat already correct)
        coord_str = ",".join([f"{lon},{lat}" for lon, lat in coords])
    elif isinstance(polygon, list):
        # Assume it's a list of [lat, lon] pairs (from mortie's mort2polygon)
        coords = polygon
        # Ensure polygon is closed
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        # Format as comma-separated string
        # CMR expects: lon1,lat1,lon2,lat2,lon3,lat3,...
        # mortie's mort2polygon returns [[lat, lon], ...] so swap the order
        coord_str = ",".join([f"{lon},{lat}" for lat, lon in coords])
    else:
        raise ValueError(f"Unsupported polygon type: {type(polygon)}")

    return coord_str


def query_atl06_cmr_with_polygon(
    polygon: Optional[Union[Polygon, List[Tuple[float, float]], str]] = None,
    bounding_box: Optional[Union[List[float], Tuple[float, float, float, float]]] = None,
    point: Optional[Union[Point, Tuple[float, float]]] = None,
    radius_km: Optional[float] = None,
    cycle: Optional[int] = None,
    regions: List[int] = None,
    rgts: Optional[List[int]] = None,
    version: str = "006",
    provider: str = "NSIDC_CPRD",
    page_size: int = 2000,
    temporal: Optional[str] = None,
    year: Optional[int] = None,
    max_granules: Optional[int] = None,
    geometry_type: str = "polygon",
) -> gpd.GeoDataFrame:
    """
    Query NASA CMR for ATL06 data with server-side polygon filtering.

    The spatial subsetting is performed by NASA's CMR API, not locally.

    Parameters
    ----------
    polygon : Optional[Union[Polygon, List[Tuple[float, float]], str]]
        Polygon for spatial filtering. Can be:
        - Shapely Polygon object
        - List of (lon, lat) tuples
        - WKT string
        The polygon will be used by CMR for server-side spatial filtering.
    bounding_box : Optional[Union[List[float], Tuple[float, float, float, float]]]
        Bounding box as [west, south, east, north] for spatial filtering.
        Alternative to polygon for rectangular areas.
    point : Optional[Union[Point, Tuple[float, float]]]
        Center point for circular search as Point object or (lon, lat) tuple.
        Must be used with radius_km.
    radius_km : Optional[float]
        Radius in kilometers for circular search around point.
    cycle : int, optional
        Orbital cycle number (e.g., 22). If provided, temporal will be calculated
        unless explicitly overridden.
    regions : List[int], optional
        List of granule region numbers (1-14), e.g., [10, 11, 12].
        If None, all regions are returned.
    rgts : Optional[List[int]], optional
        List of specific Reference Ground Tracks to filter
    version : str, optional
        ATL06 version, by default "006"
    provider : str, optional
        CMR provider, by default "NSIDC_CPRD" (cloud-hosted)
    page_size : int, optional
        Number of results per page, by default 2000
    temporal : str, optional
        Temporal filter in ISO format
    year : int, optional
        Convenience parameter to search an entire year
    max_granules : int, optional
        Maximum number of granules to retrieve from CMR
    geometry_type : str, optional
        Type of geometry to use in returned GeoDataFrame

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with granule metadata and geometries

    Examples
    --------
    # Search with a polygon (server-side filtering)
    >>> from shapely.geometry import Polygon
    >>> poly = Polygon([(-120, 30), (-120, 40), (-110, 40), (-110, 30), (-120, 30)])
    >>> gdf = query_atl06_cmr_with_polygon(polygon=poly, cycle=22)

    # Search with a bounding box
    >>> gdf = query_atl06_cmr_with_polygon(
    ...     bounding_box=[-120, 30, -110, 40],
    ...     cycle=22
    ... )

    # Search within radius of a point
    >>> gdf = query_atl06_cmr_with_polygon(
    ...     point=(-115, 35),
    ...     radius_km=100,
    ...     cycle=22
    ... )
    """

    # CMR granule search endpoint
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    # Handle temporal filtering
    if year is not None:
        temporal = f"{year}-01-01,{year}-12-31"
    elif temporal is None and cycle is not None:
        launch_date = datetime(2018, 10, 13)
        cycle_duration = 91
        cycle_start = launch_date + timedelta(days=(cycle - 1) * cycle_duration)
        cycle_end = cycle_start + timedelta(days=cycle_duration + 1)
        temporal = f"{cycle_start.strftime('%Y-%m-%d')}T00:00:00Z,{cycle_end.strftime('%Y-%m-%d')}T23:59:59Z"

    # Build query parameters
    params = {
        "provider": provider,
        "short_name": "ATL06",
        "version": version,
        "page_size": page_size,
        "sort_key": "start_date",
    }

    # Add temporal filter if available
    if temporal:
        params["temporal"] = temporal

    # Add spatial filters (server-side filtering by CMR)
    spatial_params_used = []

    if polygon is not None:
        # Format polygon for CMR
        polygon_str = format_polygon_for_cmr(polygon)
        params["polygon"] = polygon_str
        spatial_params_used.append("polygon")

        # If polygon is provided as Shapely object, we can show its bounds
        if isinstance(polygon, Polygon):
            bounds = polygon.bounds
            print(f"  Polygon bounds: [{bounds[0]:.2f}, {bounds[1]:.2f}, {bounds[2]:.2f}, {bounds[3]:.2f}]")

    elif bounding_box is not None:
        # CMR expects: lower_left_lon,lower_left_lat,upper_right_lon,upper_right_lat
        if len(bounding_box) == 4:
            west, south, east, north = bounding_box
            params["bounding_box"] = f"{west},{south},{east},{north}"
            spatial_params_used.append(f"bbox [{west:.2f}, {south:.2f}, {east:.2f}, {north:.2f}]")
        else:
            raise ValueError("Bounding box must have 4 values: [west, south, east, north]")

    elif point is not None and radius_km is not None:
        # CMR point format: lon,lat,radius_in_meters
        if isinstance(point, Point):
            lon, lat = point.x, point.y
        elif isinstance(point, (list, tuple)) and len(point) == 2:
            lon, lat = point
        else:
            raise ValueError("Point must be a Point object or (lon, lat) tuple")

        # Convert km to meters for CMR
        radius_m = radius_km * 1000
        params["point"] = f"{lon},{lat}"
        params["circle"] = f"{lon},{lat},{radius_m}"
        spatial_params_used.append(f"circle center=({lon:.2f}, {lat:.2f}), radius={radius_km}km")

    # Print query info
    print(f"Querying CMR for ATL06 v{version}:")
    print(f"  Provider: {provider}")
    if spatial_params_used:
        print(f"  Spatial filter: {', '.join(spatial_params_used)} (server-side)")
    if cycle:
        print(f"  Cycle: {cycle}")
    if regions:
        print(f"  Regions: {regions} (client-side filter)")
    if temporal:
        print(f"  Temporal: {temporal}")
    if rgts:
        print(f"  RGTs: {rgts} (client-side filter)")

    all_granules = []
    headers = {"Accept": "application/vnd.nasa.cmr.umm_json+json"}

    # Add offset for pagination
    params["offset"] = 0
    total_hits = None

    # Fetch all pages using offset-based pagination
    while True:
        response = _cmr_request_with_retry(cmr_url, params, headers)

        # Get total number of hits from header (only on first request)
        if total_hits is None:
            total_hits = int(response.headers.get("CMR-Hits", 0))
            print(f"  Total matching granules in CMR (after spatial filter): {total_hits}")

        data = response.json()
        items = data.get("items", [])

        if not items:
            break

        all_granules.extend(items)

        # Update progress
        print(f"  Retrieved {len(all_granules)} of {total_hits} granules...", end="\r")

        # Check if we've reached the max_granules limit
        if max_granules and len(all_granules) >= max_granules:
            print(f"\n  Stopped at max_granules limit: {max_granules}")
            all_granules = all_granules[:max_granules]
            break

        # Check if there are more pages
        if len(items) < page_size or len(all_granules) >= total_hits:
            break

        # Update offset for next page
        params["offset"] += len(items)

    print(f"\nRetrieved {len(all_granules)} granules from CMR (after server-side spatial filtering)")

    # Client-side filtering by cycle, region, and RGT (these aren't supported by CMR directly)
    filtered_granules = []
    for granule in all_granules:
        umm = granule.get("umm", {})
        granule_ur = umm.get("GranuleUR", "")

        # Parse granule filename
        try:
            parts = granule_ur.split("_")
            if len(parts) >= 3:
                rgt_cycle_region = parts[2]

                granule_rgt = int(rgt_cycle_region[0:4])
                granule_cycle = int(rgt_cycle_region[4:6])
                granule_region = int(rgt_cycle_region[6:8])

                # Client-side filters
                if cycle is not None and granule_cycle != cycle:
                    continue
                if regions is not None and granule_region not in regions:
                    continue
                if rgts and granule_rgt not in rgts:
                    continue

                filtered_granules.append(granule)

        except (ValueError, IndexError) as e:
            print(f"Warning: Could not parse granule UR {granule_ur}: {e}")
            continue

    # Report client-side filtering if applied
    if cycle is not None or regions is not None or rgts is not None:
        filter_desc = []
        if cycle is not None:
            filter_desc.append(f"cycle {cycle}")
        if regions is not None:
            filter_desc.append(f"regions {regions}")
        if rgts is not None:
            filter_desc.append(f"RGTs {rgts}")
        print(f"After client-side filtering ({', '.join(filter_desc)}): {len(filtered_granules)} granules")
    else:
        filtered_granules = all_granules

    # Convert to GeoDataFrame
    records = []
    for granule in filtered_granules:
        umm = granule.get("umm", {})
        granule_id = umm.get("GranuleUR", "")

        # Get spatial extent
        spatial_extent = umm.get("SpatialExtent", {})
        horiz_spatial = spatial_extent.get("HorizontalSpatialDomain", {})
        geometry_obj = horiz_spatial.get("Geometry", {})

        # Try to get geometry from GPolygons
        gpolygons = geometry_obj.get("GPolygons", [])
        if gpolygons:
            boundary_points = gpolygons[0].get("Boundary", {}).get("Points", [])
            if not boundary_points:
                continue

            coords = [(p["Longitude"], p["Latitude"]) for p in boundary_points
                      if "Longitude" in p and "Latitude" in p]

            if len(coords) < 3:
                continue

            polygon_geom = Polygon(coords)
            west, south, east, north = polygon_geom.bounds

            # Choose geometry based on user preference
            if geometry_type == "bbox":
                geom = box(west, south, east, north)
            elif geometry_type == "centerline":
                n_points = len(coords)
                if n_points > 4:
                    centerline_points = []
                    half = n_points // 2
                    for i in range(half):
                        pt1 = coords[i]
                        pt2 = coords[n_points - 1 - i]
                        mid = ((pt1[0] + pt2[0])/2, (pt1[1] + pt2[1])/2)
                        centerline_points.append(mid)
                    geom = LineString(centerline_points)
                else:
                    geom = LineString([
                        ((coords[0][0] + coords[2][0])/2, (coords[0][1] + coords[2][1])/2),
                        ((coords[1][0] + coords[3][0])/2, (coords[1][1] + coords[3][1])/2)
                    ])
            else:
                geom = polygon_geom
        else:
            # Fallback to BoundingRectangles
            bounding_rectangles = geometry_obj.get("BoundingRectangles", [])
            if not bounding_rectangles:
                continue

            bbox_dict = bounding_rectangles[0]
            west = bbox_dict.get("WestBoundingCoordinate", 0)
            south = bbox_dict.get("SouthBoundingCoordinate", 0)
            east = bbox_dict.get("EastBoundingCoordinate", 0)
            north = bbox_dict.get("NorthBoundingCoordinate", 0)
            geom = box(west, south, east, north)

        # Get temporal info
        temporal = umm.get("TemporalExtent", {})
        range_date_times = temporal.get("RangeDateTime", {})
        begin_date = range_date_times.get("BeginningDateTime", "")
        end_date = range_date_times.get("EndingDateTime", "")

        # Get URLs
        related_urls = umm.get("RelatedUrls", [])
        data_urls = []
        for url_obj in related_urls:
            url_type = url_obj.get("Type", "")
            if "GET DATA" in url_type:
                data_urls.append(url_obj.get("URL", ""))

        # Parse granule components
        parts = granule_id.split("_")
        rgt_cycle_region = parts[2] if len(parts) > 2 else ""
        granule_rgt = int(rgt_cycle_region[0:4]) if len(rgt_cycle_region) >= 4 else None
        granule_cycle = int(rgt_cycle_region[4:6]) if len(rgt_cycle_region) >= 6 else None
        granule_region = int(rgt_cycle_region[6:8]) if len(rgt_cycle_region) >= 8 else None

        record = {
            "granule_id": granule_id,
            "rgt": granule_rgt,
            "cycle": granule_cycle,
            "region": granule_region,
            "bbox_west": west,
            "bbox_south": south,
            "bbox_east": east,
            "bbox_north": north,
            "geometry": geom,
            "begin_datetime": begin_date,
            "end_datetime": end_date,
            "urls": data_urls,
            "n_urls": len(data_urls),
        }
        records.append(record)

    # Create DataFrame (or GeoDataFrame if geopandas available)
    if records:
        if HAS_GEOPANDAS:
            gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
        else:
            gdf = pd.DataFrame(records)
    else:
        columns = [
            "granule_id", "rgt", "cycle", "region",
            "bbox_west", "bbox_south", "bbox_east", "bbox_north",
            "geometry", "begin_datetime", "end_datetime",
            "urls", "n_urls"
        ]
        if HAS_GEOPANDAS:
            gdf = gpd.GeoDataFrame(columns=columns)
            gdf = gdf.set_geometry("geometry")
            gdf.set_crs("EPSG:4326", inplace=True, allow_override=True)
        else:
            gdf = pd.DataFrame(columns=columns)

    return gdf


def save_to_geoparquet(gdf: gpd.GeoDataFrame, output_path: str):
    """Save GeoDataFrame to GeoParquet format."""
    gdf_copy = gdf.copy()
    gdf_copy["urls"] = gdf_copy["urls"].apply(lambda x: "|".join(x) if x else "")
    gdf_copy.to_parquet(output_path, index=False)
    print(f"\nSaved {len(gdf_copy)} records to {output_path}")


if __name__ == "__main__":
    # Example 1: Query with a polygon (server-side filtering)
    print("=" * 60)
    print("Example 1: Polygon search (server-side filtering)")
    print("=" * 60)

    # Create a polygon around Greenland
    greenland_poly = Polygon([
        (-55, 70),   # Southwest
        (-55, 80),   # Northwest
        (-30, 80),   # Northeast
        (-30, 70),   # Southeast
        (-55, 70)    # Close polygon
    ])

    gdf_polygon = query_atl06_cmr_with_polygon(
        polygon=greenland_poly,
        cycle=22,
        version="006",
        provider="NSIDC_CPRD",
        max_granules=100  # Limit for testing
    )

    print(f"\nFound {len(gdf_polygon)} granules in polygon area")
    if len(gdf_polygon) > 0:
        print(f"RGT range: {gdf_polygon['rgt'].min()} - {gdf_polygon['rgt'].max()}")
        print(f"Region distribution: {dict(gdf_polygon['region'].value_counts().sort_index())}")

    # Example 2: Query with a bounding box
    print("\n" + "=" * 60)
    print("Example 2: Bounding box search (server-side filtering)")
    print("=" * 60)

    gdf_bbox = query_atl06_cmr_with_polygon(
        bounding_box=[-120, 30, -110, 40],  # California region
        cycle=22,
        regions=[10, 11, 12],  # Client-side filter
        version="006",
        provider="NSIDC_CPRD",
        max_granules=50
    )

    print(f"\nFound {len(gdf_bbox)} granules in bounding box")

    # Example 3: Query with point and radius
    print("\n" + "=" * 60)
    print("Example 3: Circular search around point (server-side)")
    print("=" * 60)

    gdf_circle = query_atl06_cmr_with_polygon(
        point=(-115, 35),  # Near Las Vegas
        radius_km=200,
        cycle=22,
        version="006",
        provider="NSIDC_CPRD",
        max_granules=50
    )

    print(f"\nFound {len(gdf_circle)} granules within radius")