# Catalog

The catalog module builds a local mapping of parent morton cells to granule S3 URLs by querying NASA's CMR (Common Metadata Repository). This avoids per-worker CMR queries during parallel processing.

## Generating a Catalog

```bash
# Query CMR for cycle 22 granules and build cell-to-granule mapping
uv run python -m magg.catalog --cycle 22 --parent-order 6

# Custom output path
uv run python -m magg.catalog --cycle 22 --parent-order 6 --output deployment/data/catalogs/cycle22.json
```

Cell discovery uses `mortie.morton_coverage()` on Antarctic drainage basin polygons to find the exact set of parent cells covering Antarctica. Granule-to-cell assignment uses shapely STRtree intersection in EPSG:3031.

## Cell Discovery

::: magg.catalog.load_antarctic_basins

::: magg.catalog.discover_cells

## Catalog Builder

::: magg.catalog.build_catalog

::: magg.catalog.query_cmr_antarctica

## Granule Parsing

::: magg.catalog.extract_granule_info
