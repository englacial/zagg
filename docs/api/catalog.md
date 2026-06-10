# Catalog

Catalog construction has two separable concerns:

1. **Fetch** — query a STAC endpoint (CMR-STAC) for *what / when / where* → a
   `Catalog` (a stac-geoparquet table of granule metadata, reusable across
   many grids).
2. **Shard map** — take a `Catalog` plus an output grid → a `ShardMap`: the
   work-distribution manifest mapping shard keys to granules.

The CLI chains them, building the output grid from the **same pipeline config
the aggregator uses**, so a shard map can never be built against a different
grid than the run (enforced at run time via `grid.signature()`).

## Building a shard map (CLI)

```bash
# HEALPix grid from atl06.yaml, an ICESat-2 cycle, Antarctic polygon:
python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson

# Rectilinear (UTM) grid from a config, explicit dates, over a bbox:
python -m zagg.catalog --config serc_atl03.yaml --short-name ATL03 \
    --start-date 2025-01-01 --end-date 2025-12-31 \
    --bbox=-76.62107,38.84504,-76.50583,38.93512

# Persist the fetched Catalog too (reusable for other grids):
python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson --catalog-out cycle22.parquet
```

`--polygon` drives both the CMR query bbox and the coverage mask; `--bbox`
gives the query box directly (coverage falls back to that rectangle). The
geometry backend (`--backend`) defaults to `auto`: exact-S2 spherely if the
`catalog` extra is installed, else mortie (HEALPix) / shapely (rectilinear).

Endpoint selection (S3 vs HTTPS) is **not** made here — each granule record
keeps both hrefs, and the aggregator picks one at run time via
`data_source.driver`.

## Fetch

::: zagg.catalog.sources.Query

::: zagg.catalog.sources.CMRSource

::: zagg.catalog.sources.Catalog

## Shard map

::: zagg.catalog.shardmap.ShardMap

## Convenience

::: zagg.catalog.make_shardmap

## Temporal / spatial helpers

::: zagg.catalog.cycle_to_dates

::: zagg.catalog.load_polygon

::: zagg.catalog.polygon_to_bbox

::: zagg.catalog.load_antarctic_basins
