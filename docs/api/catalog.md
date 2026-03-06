# Catalog

The catalog module builds a local mapping of parent morton cells to granule S3 URLs by querying NASA's CMR (Common Metadata Repository). This avoids per-worker CMR queries during parallel processing.

## Building a Catalog

The catalog CLI accepts date ranges, product names, and spatial polygons:

```bash
# ICESat-2 convenience (cycle → date range):
python -m magg.catalog --cycle 22 --parent-order 6

# Explicit date range:
python -m magg.catalog --start-date 2024-01-06 --end-date 2024-04-07 --parent-order 6

# Custom region via GeoJSON polygon:
python -m magg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \
    --polygon my_region.geojson --parent-order 6

# Different product:
python -m magg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \
    --short-name ATL08 --polygon my_region.geojson --parent-order 6
```

When `--polygon` is provided, it is used for two things:

1. **Cell discovery** — `morton_coverage` runs on the polygon to find parent cells
2. **CMR bounding box** — automatically computed from the polygon's extent

When no polygon is given, Antarctic drainage basins are used as the default (suitable for ATL06 ice sheet work).

## Temporal Helpers

::: magg.catalog.cycle_to_dates

## Spatial Helpers

::: magg.catalog.load_polygon

::: magg.catalog.polygon_to_bbox

## Cell Discovery

::: magg.catalog.load_antarctic_basins

::: magg.catalog.discover_cells

## CMR Query

::: magg.catalog.query_cmr

## Catalog Builder

::: magg.catalog.build_catalog

## Granule Parsing

::: magg.catalog.extract_granule_info
