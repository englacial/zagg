#!/usr/bin/env python3
"""
Query NASA CMR-STAC for ICESat-2 ATL06 data for a specific orbital cycle and regions.
Output as a STAC catalog that can be saved as GeoParquet.
"""

from typing import List, Optional

import geopandas as gpd
import pystac_client
from shapely.geometry import box


def query_atl06_stac(
    cycle: int,
    regions: List[int],
    version: str = "006",
    provider: str = "NSIDC_CPRD",  # Cloud-hosted data
    temporal: Optional[tuple] = None,
) -> gpd.GeoDataFrame:
    """
    Query NASA CMR-STAC for ATL06 data for specific cycle and granule regions.

    Parameters
    ----------
    cycle : int
        Orbital cycle number (e.g., 22)
    regions : List[int]
        List of granule region numbers (1-14), e.g., [10, 11, 12]
    version : str, optional
        ATL06 version, by default "006"
    provider : str, optional
        CMR provider, by default "NSIDC_CPRD" (cloud-hosted)
        Use "NSIDC_ECS" for on-prem data
    temporal : Optional[tuple], optional
        Temporal range as (start, end) ISO 8601 strings, by default None
        If None, uses the cycle dates

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with columns: granule_id, bbox, geometry, urls, etc.
    """

    # ICESat-2 cycle dates (cycle 22: December 18, 2023 - March 18, 2024)
    # You can update this dictionary or calculate programmatically
    cycle_dates = {
        22: ("2023-12-18", "2024-03-18"),
        23: ("2024-03-18", "2024-06-17"),
        # Add more as needed
    }

    # Use provided temporal range or look up cycle dates
    if temporal is None:
        if cycle in cycle_dates:
            temporal = cycle_dates[cycle]
        else:
            raise ValueError(f"Cycle {cycle} dates not defined. Please provide temporal parameter.")

    # Connect to CMR-STAC API
    cmr_stac_url = f"https://cmr.earthdata.nasa.gov/stac/{provider}"
    catalog = pystac_client.Client.open(cmr_stac_url)

    # Collection ID for ATL06
    collection_id = f"ATL06.v{version}"

    print(f"Searching for ATL06 v{version} data:")
    print(f"  Provider: {provider}")
    print(f"  Cycle: {cycle}")
    print(f"  Regions: {regions}")
    print(f"  Temporal: {temporal[0]} to {temporal[1]}")

    # Search for items
    search = catalog.search(
        collections=[collection_id],
        datetime=f"{temporal[0]}/{temporal[1]}",
        max_items=10000,  # Adjust as needed
    )

    # Collect all items
    items = list(search.items())
    print(f"\nFound {len(items)} total granules")

    # Filter by cycle and region from granule filenames
    # ATL06 filename format: ATL06_YYYYMMDDhhmmss_ttttccnn_rrr_vv.h5
    # where: tttt=RGT, cc=cycle, nn=granule region number

    filtered_items = []
    for item in items:
        granule_id = item.id

        # Parse filename components
        # Example: ATL06_20231218120000_0010_22_10_006_01.h5
        # Or without .h5: ATL06_20231218120000_001022101_006_01
        try:
            parts = granule_id.split("_")
            if len(parts) >= 3:
                # The RGT+cycle+region is typically in parts[2]
                rgt_cycle_region = parts[2]

                # Extract cycle (positions 4-5, 0-indexed)
                granule_cycle = int(rgt_cycle_region[4:6])

                # Extract region (positions 6-7, 0-indexed)
                granule_region = int(rgt_cycle_region[6:8])

                # Filter by cycle and region
                if granule_cycle == cycle and granule_region in regions:
                    filtered_items.append(item)
        except (ValueError, IndexError) as e:
            print(f"Warning: Could not parse granule ID {granule_id}: {e}")
            continue

    print(
        f"Filtered to {len(filtered_items)} granules matching cycle {cycle} and regions {regions}"
    )

    # Convert to GeoDataFrame
    records = []
    for item in filtered_items:
        # Extract bbox
        bbox = item.bbox  # [west, south, east, north]

        # Create geometry from bbox
        geom = box(*bbox)

        # Get data URLs
        urls = []
        for asset_key, asset in item.assets.items():
            if asset.href:
                urls.append(asset.href)

        record = {
            "granule_id": item.id,
            "bbox_west": bbox[0],
            "bbox_south": bbox[1],
            "bbox_east": bbox[2],
            "bbox_north": bbox[3],
            "geometry": geom,
            "datetime": item.datetime,
            "collection": item.collection_id,
            "urls": urls,
            "n_assets": len(item.assets),
        }
        records.append(record)

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

    return gdf


def save_to_geoparquet(gdf: gpd.GeoDataFrame, output_path: str):
    """
    Save GeoDataFrame to GeoParquet format.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to save
    output_path : str
        Output file path (should end in .parquet or .geoparquet)
    """
    # URLs are lists, need to convert to string for parquet
    gdf_copy = gdf.copy()
    gdf_copy["urls"] = gdf_copy["urls"].apply(lambda x: "|".join(x) if x else "")

    gdf_copy.to_parquet(output_path, index=False)
    print(f"\nSaved {len(gdf_copy)} records to {output_path}")


if __name__ == "__main__":
    # Example usage
    cycle = 22
    regions = [10, 11, 12]

    # Query the data
    gdf = query_atl06_stac(
        cycle=cycle,
        regions=regions,
        version="006",
        provider="NSIDC_CPRD",
    )

    # Display results
    print("\nResults:")
    print(f"Total granules: {len(gdf)}")
    print("\nFirst few records:")
    print(
        gdf[["granule_id", "bbox_west", "bbox_south", "bbox_east", "bbox_north", "n_assets"]].head()
    )

    # Save to GeoParquet
    output_file = (
        f"/mnt/user-data/outputs/atl06_cycle{cycle}_regions_{'_'.join(map(str, regions))}.parquet"
    )
    save_to_geoparquet(gdf, output_file)

    # You can also inspect the geometry
    print("\nBounding box of all granules:")
    print(f"  West: {gdf.total_bounds[0]:.2f}")
    print(f"  South: {gdf.total_bounds[1]:.2f}")
    print(f"  East: {gdf.total_bounds[2]:.2f}")
    print(f"  North: {gdf.total_bounds[3]:.2f}")
