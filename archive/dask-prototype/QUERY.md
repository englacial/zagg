# Querying NASA CMR for ICESat-2 ATL06 Data

This repository contains two approaches for querying NASA's Common Metadata Repository (CMR) for ICESat-2 ATL06 (Land Ice Height) data with filtering by orbital cycle and granule regions.

## Background

### ICESat-2 Data Organization

- **Orbital Cycle**: 91-day repeat period (e.g., Cycle 22: Dec 18, 2023 - Mar 18, 2024)
- **Reference Ground Tracks (RGT)**: 1,387 unique ground tracks numbered 0001-1387
- **Granule Regions**: Each orbit is divided into 14 regions (01-14) based on latitude boundaries

### Granule Naming Convention

```
ATL06_YYYYMMDDhhmmss_ttttccnn_rrr_vv.h5
```

Where:
- `YYYYMMDD`: Date of acquisition
- `hhmmss`: Start time (UTC)
- `tttt`: Reference Ground Track (RGT) number (0001-1387)
- `cc`: Cycle number
- `nn`: Granule region number (01-14)
- `rrr`: Data release number
- `vv`: Version number

Example: `ATL06_20231218120000_001022​10_006_01.h5`
- RGT: 0010
- Cycle: 22
- Region: 10
- Version: 006

## Approach 1: CMR-STAC API

**File**: `query_cmr_stac_atl06.py`

Uses the CMR-STAC (SpatioTemporal Asset Catalog) API endpoint.

### Pros
- STAC-compliant interface
- Works with standard STAC tooling (pystac-client, intake-stac, etc.)
- Returns STAC Items with well-structured metadata
- Better for integration with cloud-native geospatial workflows

### Cons
- Limited native support for ICESat-2-specific parameters (cycle, region)
- Requires post-filtering by parsing granule filenames
- Provider-specific endpoints (must specify NSIDC_CPRD or NSIDC_ECS)

### Usage

```python
from query_cmr_stac_atl06 import query_atl06_stac, save_to_geoparquet

# Query for cycle 22, regions 10-12
gdf = query_atl06_stac(
    cycle=22,
    regions=[10, 11, 12],
    version="006",
    provider="NSIDC_CPRD",  # Cloud-hosted data
)

# Save to GeoParquet
save_to_geoparquet(gdf, "atl06_cycle22_regions_10_11_12.parquet")
```

### CMR-STAC Endpoint

```
https://cmr.earthdata.nasa.gov/stac/{PROVIDER}
```

Providers:
- `NSIDC_CPRD`: Cloud-hosted collections (AWS S3)
- `NSIDC_ECS`: On-premises NSIDC archive

## Approach 2: Direct CMR API

**File**: `query_cmr_direct_atl06.py`

Uses the direct CMR granule search API with UMM-JSON format.

### Pros
- Native support for ICESat-2 `cycle` parameter
- More efficient filtering on the server side
- Single endpoint for all providers
- Access to full UMM metadata

### Cons
- Non-STAC format (less standardized)
- Still requires filename parsing for region filtering
- More complex pagination handling

### Usage

```python
from query_cmr_direct_atl06 import query_atl06_cmr, save_to_geoparquet

# Query for cycle 22, regions 10-12
gdf = query_atl06_cmr(
    cycle=22,
    regions=[10, 11, 12],
    version="006",
    provider="NSIDC_CPRD",
)

# Optionally filter by specific RGTs
gdf = query_atl06_cmr(
    cycle=22,
    regions=[10, 11, 12],
    rgts=[10, 20, 30],  # Specific tracks
    version="006",
)

# Save to GeoParquet
save_to_geoparquet(gdf, "atl06_cycle22_regions_10_11_12.parquet")
```

### CMR Granule Search Endpoint

```
https://cmr.earthdata.nasa.gov/search/granules.umm_json
```

## GeoParquet Output Schema

Both scripts produce a GeoDataFrame with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `granule_id` | string | Full granule identifier |
| `rgt` | int | Reference Ground Track number |
| `cycle` | int | Orbital cycle number |
| `region` | int | Granule region number (1-14) |
| `bbox_west` | float | Western bounding longitude |
| `bbox_south` | float | Southern bounding latitude |
| `bbox_east` | float | Eastern bounding longitude |
| `bbox_north` | float | Northern bounding latitude |
| `geometry` | geometry | Shapely Polygon of bounding box |
| `begin_datetime` | string | Start time of granule |
| `end_datetime` | string | End time of granule |
| `urls` | string | Pipe-delimited data URLs |
| `n_urls` | int | Number of data URLs |

## Installation

```bash
pip install pystac-client geopandas pandas shapely requests pyarrow
```

## Example: Full Workflow

```python
import geopandas as gpd
from query_cmr_direct_atl06 import query_atl06_cmr

# 1. Query CMR for ATL06 data
gdf = query_atl06_cmr(
    cycle=22,
    regions=[10, 11, 12],
    version="006",
    provider="NSIDC_CPRD",
)

print(f"Found {len(gdf)} granules")

# 2. Explore the data
print("\nRGT distribution:")
print(gdf['rgt'].value_counts().sort_index())

print("\nSpatial extent:")
print(f"Longitude: {gdf.total_bounds[0]:.2f} to {gdf.total_bounds[2]:.2f}")
print(f"Latitude: {gdf.total_bounds[1]:.2f} to {gdf.total_bounds[3]:.2f}")

# 3. Filter further (e.g., Arctic regions only)
arctic_gdf = gdf[gdf['bbox_north'] > 60]

# 4. Save to GeoParquet
arctic_gdf.to_parquet("atl06_arctic_cycle22.parquet")

# 5. Load and use the GeoParquet later
loaded_gdf = gpd.read_parquet("atl06_arctic_cycle22.parquet")

# Convert pipe-delimited URLs back to list
loaded_gdf['urls'] = loaded_gdf['urls'].str.split('|')
```

## Notes

1. **Authentication**: For downloading actual data files, you'll need NASA Earthdata Login credentials
2. **Cloud Access**: Use `NSIDC_CPRD` provider for cloud-hosted data (AWS S3 us-west-2)
3. **Versions**: ATL06 is currently at version 006 (as of Jan 2025)
4. **Region Filtering**: Both scripts parse granule filenames since CMR doesn't have a native "region" parameter
5. **Cycle Dates**: Cycle 22 ran from December 18, 2023 to March 18, 2024

## References

- [NASA CMR-STAC Documentation](https://github.com/nasa/cmr-stac)
- [CMR API Documentation](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)
- [ICESat-2 Data Products](https://nsidc.org/data/icesat-2/products)
- [ATL06 User Guide](https://nsidc.org/sites/nsidc.org/files/ATL06-V006-UserGuide.pdf)
- [STAC Specification](https://stacspec.org/)
