#!/usr/bin/env python3
"""
Alternative approach: Query NASA CMR directly (not STAC) for ICESat-2 ATL06 data
with support for orbital cycle and region filtering via native CMR parameters.

The CMR API has better native support for ICESat-2 orbital parameters than CMR-STAC.
"""

from datetime import datetime, timedelta
from typing import List, Optional

import geopandas as gpd
import requests
from shapely.geometry import LineString, Polygon, box


def query_atl06_cmr(
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
    Query NASA CMR directly for ATL06 data with cycle and region filtering.

    Parameters
    ----------
    cycle : int, optional
        Orbital cycle number (e.g., 22). If provided, temporal will be calculated
        unless explicitly overridden.
    regions : List[int], optional
        List of granule region numbers (1-14), e.g., [10, 11, 12].
        If None, all regions are returned.
    rgts : Optional[List[int]], optional
        List of specific Reference Ground Tracks to filter, by default None
    version : str, optional
        ATL06 version, by default "006"
    provider : str, optional
        CMR provider, by default "NSIDC_CPRD" (cloud-hosted)
    page_size : int, optional
        Number of results per page, by default 2000
    temporal : str, optional
        Temporal filter in ISO format. Examples:
        - "2023-01-01,2023-12-31" for all of 2023
        - "2024-04-06T00:00:00Z,2024-07-07T23:59:59Z" for precise times
        If not provided and cycle is given, will be calculated based on cycle.
    year : int, optional
        Convenience parameter to search an entire year (e.g., year=2023).
        Overrides cycle-based temporal calculation.
    max_granules : int, optional
        Maximum number of granules to retrieve from CMR before filtering.
        Useful for testing or when you don't need all results.
        By default retrieves all matching granules.
    geometry_type : str, optional
        Type of geometry to use: "polygon" (actual ground track polygon),
        "bbox" (bounding box), or "centerline" (LineString along track center).
        Default is "polygon".

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with granule metadata and geometries
    """

    # CMR granule search endpoint
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    # Handle temporal filtering
    if year is not None:
        # If year is specified, search the entire year
        temporal = f"{year}-01-01,{year}-12-31"
    elif temporal is None and cycle is not None:
        # Calculate temporal range based on cycle if not provided
        # ICESat-2 first data: October 13, 2018
        # Each cycle is 91 days
        launch_date = datetime(2018, 10, 13)
        cycle_duration = 91

        # Calculate start and end dates for the cycle
        cycle_start = launch_date + timedelta(days=(cycle - 1) * cycle_duration)
        cycle_end = cycle_start + timedelta(days=cycle_duration + 1)  # Add 1 day buffer

        # Format temporal string
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

    # Note: CMR doesn't support cycle as a direct parameter for ATL06
    # We'll need to filter by filename pattern after retrieval
    # The cycle is encoded in the granule filename: ATL06_YYYYMMDDhhmmss_ttttccnn_rrr_vv
    # where cc is the cycle number

    print(f"Querying CMR for ATL06 v{version}:")
    print(f"  Provider: {provider}")
    if cycle:
        print(f"  Cycle: {cycle}")
    if regions:
        print(f"  Regions: {regions}")
    if temporal:
        print(f"  Temporal: {temporal}")
    if rgts:
        print(f"  RGTs: {rgts}")

    all_granules = []
    headers = {"Accept": "application/vnd.nasa.cmr.umm_json+json"}

    # Add offset for pagination
    params["offset"] = 0
    total_hits = None

    # Fetch all pages using offset-based pagination
    while True:
        response = requests.get(cmr_url, params=params, headers=headers)
        response.raise_for_status()

        # Get total number of hits from header (only on first request)
        if total_hits is None:
            total_hits = int(response.headers.get("CMR-Hits", 0))
            print(f"  Total matching granules in CMR: {total_hits}")

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

    print(f"Retrieved {len(all_granules)} granules from CMR")

    # Filter by region and optionally by RGT from granule names
    filtered_granules = []
    for granule in all_granules:
        umm = granule.get("umm", {})
        granule_ur = umm.get("GranuleUR", "")

        # Parse granule filename
        # Format: ATL06_YYYYMMDDhhmmss_ttttccnn_rrr_vv
        try:
            parts = granule_ur.split("_")
            if len(parts) >= 3:
                rgt_cycle_region = parts[2]

                # Extract RGT (first 4 digits)
                granule_rgt = int(rgt_cycle_region[0:4])

                # Extract cycle (next 2 digits)
                granule_cycle = int(rgt_cycle_region[4:6])

                # Extract region (last 2 digits)
                granule_region = int(rgt_cycle_region[6:8])

                # Check if cycle matches (if specified)
                if cycle is not None and granule_cycle != cycle:
                    continue

                # Filter by region (if specified)
                if regions is not None and granule_region not in regions:
                    continue

                # Filter by RGT if specified
                if rgts and granule_rgt not in rgts:
                    continue

                filtered_granules.append(granule)

        except (ValueError, IndexError) as e:
            print(f"Warning: Could not parse granule UR {granule_ur}: {e}")
            continue

    filter_desc = []
    if cycle is not None:
        filter_desc.append(f"cycle {cycle}")
    if regions is not None:
        filter_desc.append(f"regions {regions}")
    if rgts is not None:
        filter_desc.append(f"RGTs {rgts}")

    if filter_desc:
        print(f"Filtered to {len(filtered_granules)} granules matching {' and '.join(filter_desc)}")
    else:
        print(f"Found {len(filtered_granules)} granules")

    # Convert to GeoDataFrame
    records = []
    for granule in filtered_granules:
        umm = granule.get("umm", {})

        # Get granule ID
        granule_id = umm.get("GranuleUR", "")

        # Get bounding box from spatial extent
        spatial_extent = umm.get("SpatialExtent", {})
        horiz_spatial = spatial_extent.get("HorizontalSpatialDomain", {})
        geometry_obj = horiz_spatial.get("Geometry", {})

        # Try to get geometry from GPolygons (UMM-JSON format)
        gpolygons = geometry_obj.get("GPolygons", [])
        if gpolygons:
            # Get polygon points
            boundary_points = gpolygons[0].get("Boundary", {}).get("Points", [])
            if not boundary_points:
                continue

            # Extract coordinates
            coords = [
                (p["Longitude"], p["Latitude"])
                for p in boundary_points
                if "Longitude" in p and "Latitude" in p
            ]

            if len(coords) < 3:
                continue

            # Create polygon (actual ground track coverage)
            polygon = Polygon(coords)

            # Get bounding box coordinates for metadata
            west, south, east, north = polygon.bounds

            # Choose geometry based on user preference
            if geometry_type == "bbox":
                # Convert to bounding box
                geom = box(west, south, east, north)
            elif geometry_type == "centerline":
                # Extract centerline from polygon
                # Simple method: connect midpoints of polygon segments
                n_points = len(coords)
                if n_points > 4:
                    # For complex polygons, compute centerline
                    centerline_points = []
                    half = n_points // 2
                    for i in range(half):
                        pt1 = coords[i]
                        pt2 = coords[n_points - 1 - i]
                        mid = ((pt1[0] + pt2[0]) / 2, (pt1[1] + pt2[1]) / 2)
                        centerline_points.append(mid)
                    geom = LineString(centerline_points)
                else:
                    # Simple polygon - just connect opposite midpoints
                    geom = LineString(
                        [
                            ((coords[0][0] + coords[2][0]) / 2, (coords[0][1] + coords[2][1]) / 2),
                            ((coords[1][0] + coords[3][0]) / 2, (coords[1][1] + coords[3][1]) / 2),
                        ]
                    )
            else:  # Default to polygon
                geom = polygon
        else:
            # Fallback to BoundingRectangles if available
            bounding_rectangles = geometry_obj.get("BoundingRectangles", [])
            if not bounding_rectangles:
                continue

            bbox_dict = bounding_rectangles[0]
            west = bbox_dict.get("WestBoundingCoordinate", 0)
            south = bbox_dict.get("SouthBoundingCoordinate", 0)
            east = bbox_dict.get("EastBoundingCoordinate", 0)
            north = bbox_dict.get("NorthBoundingCoordinate", 0)

            # Create geometry (bbox since no polygon available)
            geom = box(west, south, east, north)

        # Get temporal info
        temporal = umm.get("TemporalExtent", {})
        range_date_times = temporal.get("RangeDateTime", {})
        begin_date = range_date_times.get("BeginningDateTime", "")
        end_date = range_date_times.get("EndingDateTime", "")

        # Get URLs from related URLs
        related_urls = umm.get("RelatedUrls", [])
        data_urls = []
        for url_obj in related_urls:
            url_type = url_obj.get("Type", "")
            if "GET DATA" in url_type:
                data_urls.append(url_obj.get("URL", ""))

        # Parse granule components from filename
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

    # Create GeoDataFrame
    if records:
        gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    else:
        # Create an empty GeoDataFrame with the expected schema
        gdf = gpd.GeoDataFrame(
            columns=[
                "granule_id",
                "rgt",
                "cycle",
                "region",
                "bbox_west",
                "bbox_south",
                "bbox_east",
                "bbox_north",
                "geometry",
                "begin_datetime",
                "end_datetime",
                "urls",
                "n_urls",
            ]
        )
        # Set geometry column and CRS
        gdf = gdf.set_geometry("geometry")
        gdf.set_crs("EPSG:4326", inplace=True, allow_override=True)

    return gdf


def save_to_geoparquet(gdf: gpd.GeoDataFrame, output_path: str):
    """Save GeoDataFrame to GeoParquet format."""
    gdf_copy = gdf.copy()
    gdf_copy["urls"] = gdf_copy["urls"].apply(lambda x: "|".join(x) if x else "")
    gdf_copy.to_parquet(output_path, index=False)
    print(f"\nSaved {len(gdf_copy)} records to {output_path}")


if __name__ == "__main__":
    # Query for cycle 22, regions 10-12
    cycle = 22
    regions = [10, 11, 12]

    gdf = query_atl06_cmr(
        cycle=cycle,
        regions=regions,
        version="006",
        provider="NSIDC_CPRD",
    )

    # Display results
    print("\nResults summary:")
    print(f"Total granules: {len(gdf)}")

    if len(gdf) > 0:
        print("\nRGT distribution:")
        print(gdf["rgt"].value_counts().sort_index().head(10))

        print("\nRegion distribution:")
        print(gdf["region"].value_counts().sort_index())

        print("\nSample granules:")
        print(gdf[["granule_id", "rgt", "region", "bbox_west", "bbox_north"]].head(10))

        # Save to GeoParquet
        output_file = f"/mnt/user-data/outputs/atl06_cycle{cycle}_regions_{'_'.join(map(str, regions))}_cmr.parquet"
        save_to_geoparquet(gdf, output_file)
