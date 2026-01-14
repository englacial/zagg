#!/usr/bin/env python3
"""
Example usage of both CMR query approaches for ATL06 data.

This demonstrates querying for ICESat-2 ATL06 data for:
- Orbital cycle 22 (December 18, 2023 - March 18, 2024)
- Granule regions 10, 11, 12
"""

# Example 1: Using CMR-STAC (STAC-compliant interface)
print("=" * 80)
print("EXAMPLE 1: CMR-STAC API")
print("=" * 80)

from query_cmr_stac_atl06 import query_atl06_stac, save_to_geoparquet

gdf_stac = query_atl06_stac(
    cycle=22,
    regions=[10, 11, 12],
    version="006",
    provider="NSIDC_CPRD",  # Cloud-hosted data in AWS S3
)

print(f"\nFound {len(gdf_stac)} granules using CMR-STAC")
print(gdf_stac[["granule_id", "n_assets"]].head())

# Save to GeoParquet
save_to_geoparquet(gdf_stac, "/mnt/user-data/outputs/atl06_stac_example.parquet")


# Example 2: Using direct CMR API (better ICESat-2 support)
print("\n" + "=" * 80)
print("EXAMPLE 2: Direct CMR API")
print("=" * 80)

from query_cmr_direct_atl06 import query_atl06_cmr

gdf_cmr = query_atl06_cmr(
    cycle=22,
    regions=[10, 11, 12],
    version="006",
    provider="NSIDC_CPRD",
)

print(f"\nFound {len(gdf_cmr)} granules using direct CMR")
print(gdf_cmr[["granule_id", "rgt", "region"]].head())

# Save to GeoParquet
save_to_geoparquet(gdf_cmr, "/mnt/user-data/outputs/atl06_cmr_example.parquet")


# Example 3: Additional filtering by RGT
print("\n" + "=" * 80)
print("EXAMPLE 3: Filter by specific RGTs")
print("=" * 80)

# Query for specific Reference Ground Tracks in the Arctic
arctic_rgts = [100, 200, 300, 400, 500]  # Example RGTs

gdf_filtered = query_atl06_cmr(
    cycle=22,
    regions=[10, 11, 12],
    rgts=arctic_rgts,
    version="006",
)

print(f"\nFound {len(gdf_filtered)} granules for RGTs: {arctic_rgts}")
if len(gdf_filtered) > 0:
    print("\nRGT distribution:")
    print(gdf_filtered["rgt"].value_counts().sort_index())


# Example 4: Spatial analysis
print("\n" + "=" * 80)
print("EXAMPLE 4: Spatial Analysis")
print("=" * 80)

if len(gdf_cmr) > 0:
    print("\nSpatial coverage of all granules:")
    bounds = gdf_cmr.total_bounds
    print(f"  West:  {bounds[0]:7.2f}°")
    print(f"  South: {bounds[1]:7.2f}°")
    print(f"  East:  {bounds[2]:7.2f}°")
    print(f"  North: {bounds[3]:7.2f}°")

    # Filter for high-latitude regions
    high_lat = gdf_cmr[gdf_cmr["bbox_north"] > 70]
    print(f"\nGranules with coverage > 70°N: {len(high_lat)}")

    # Group by RGT
    print(f"\nNumber of unique RGTs: {gdf_cmr['rgt'].nunique()}")
    print("Granules per RGT (top 5):")
    print(gdf_cmr["rgt"].value_counts().head())


print("\n" + "=" * 80)
print("Examples complete!")
print("=" * 80)
