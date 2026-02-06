# Catalog

The catalog module builds a local mapping of parent morton cells to granule S3 URLs by querying NASA's CMR (Common Metadata Repository). This avoids per-worker CMR queries during parallel processing.

## Catalog Builder

::: magg.catalog.build_morton_catalog

::: magg.catalog.query_cmr_antarctica

## Granule Parsing

::: magg.catalog.extract_granule_info

## Geometry

::: magg.catalog.densify_polygon
