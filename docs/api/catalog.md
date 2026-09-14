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
spherely fork (with `SpatialIndex`) is installed separately, else mortie
(HEALPix) / shapely (rectilinear).

Endpoint selection (S3 vs HTTPS) is **not** made here — each granule record
keeps both hrefs, and the aggregator picks one at run time via
`data_source.driver`.

## The `footprint_cells` convention column

The per-granule footprint→cells cover is identical for every shard map built
against the same catalog, so a catalog can carry it
([issue #396](https://github.com/englacial/zagg/issues/396)):

```bash
# index while fetching, so the saved catalog ships pre-indexed
python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson --catalog-out cycle22.parquet \
    --index-footprints 9
```

```python
cat = Catalog.from_geoparquet("cycle22.parquet").index_footprints(9)
cat.to_geoparquet("cycle22_indexed.parquet")
```

`footprint_cells` is a ragged `large_list` of morton MOC words — mortie's
`morton_index` extension type, the same typed-morton convention as
[morton arrow](../morton_arrow.md) — one entry per table row. This is a **zagg
convention column, not a stac-geoparquet upstream field**: an indexed catalog is
still an ordinary stac-geoparquet file, and any other reader ignores the extra
column. It is *not* part of the [store specification](../specification.md),
which governs the Zarr store the aggregator writes, not the catalog it reads.

**It cannot go stale.** The column rides in the same file as the `geometry` it
was covered from, so there is no second artifact to keep in sync and no version
skew to detect: subsetting the catalog (`filter_bbox`) carries the column with
the rows, and rewriting the geometry means rewriting the file.

`ShardMap.build` against an indexed catalog does no geometry at all — each
granule's stored MOC is intersected (`moc_and`) with the AOI's own shard MOC —
and records `metadata["footprint_cells"] = True` when it took that path, `False`
when the catalog is indexed but the build took the geometry path anyway. The
catalog's own `footprint_cells_order` rides into the manifest either way and only
says the column exists, so read `footprint_cells` for the verdict; a manifest
with neither key came from a catalog that was never indexed. The
fast path engages only where the stored cover is exactly what the build asked
for: the mortie backend, `footprint="swath"`, and no caller-pinned
`mortie_order`. Anything else takes the geometry path unchanged, so an
exact-S2 spherely run is never silently swapped for a MOC one.

The assignment it produces is the geometry path's, with one intended exception:
a **MultiPolygon** footprint. `index_footprints` covers the union of the rings in
each blob, while `granule_records` reads only the largest part's exterior ring —
so for a multi-part granule the column is a superset and the index can place it
in shards the geometry path misses. No CMR ATL03/06 footprint is multi-part
today; antimeridian-split STAC footprints are the natural producer.

### What indexing buys, after #445

An **unindexed** mortie `swath` build on a HEALPix grid now covers the geometry
column itself — the same `from_wkb` cover, at the grid's `parent_order`, thrown
away when the build ends — and runs the same intersection
([issue #445](https://github.com/englacial/zagg/issues/445)). The column no
longer buys a *different* code path; it buys **skipping the cover**, which is
most of a first build. Index when many builds share one catalog; skip it for a
one-shot build and pay the cover once, in memory. The MultiPolygon superset
above and the null-geometry refusal are properties of that shared cover, so the
unindexed path has them too. `footprint="beams"`, the spherely backend,
rectilinear grids and paired builds still cover from decoded records, and the
`footprint_cells` metadata verdict still means "the stored column answered this
build" — an unindexed build that covered its own is not that.

### Choosing the order

**Index at the grid's `parent_order`.** A MOC answers any shard order coarser
than or equal to its own; a build against a grid whose `parent_order` is *finer*
than the column is refused outright, because answering it would refine every
cell onto all its descendants and put ~every granule in ~every shard
([issue #92](https://github.com/englacial/zagg/issues/92)). The other end is
mortie's order-18 coverage cap, which `index_footprints` refuses above — the
same bound `ShardMap.build` clamps its own derived MOC order to.

Going finer than you need is expensive in both directions — words per granule
roughly double per order:

| order | words/granule (88S) | column, 35,639 granules | index pass | build (35,639 granules) |
| --- | --- | --- | --- | --- |
| 9 | 288 | 17 MB parquet | 2.6 s | 1.6 s (vs 4.8 s from geometry) |
| 13 | 7,207 | 560 MB parquet | 53.3 s | 22.9 s (vs 59.3 s from geometry) |

At the shard order the index pays for itself on the first build. At the chunk
order it does not: the pass costs about as much as the build it replaces, and
the column is two orders of magnitude larger for resolution the shard cells
cannot see.

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

## Closest-observation pairing (issue #509)

One raster store (e.g. Sentinel-2 L2A) can serve several point-cloud
reference stores (ATL03 + GEDI): for every reference *epoch* a shard's
stores actually observed, ingest the single **nearest** acquisition from the
raster catalog. The pairing is a property of the ingest *query*, not the
store schema — the raster store stays a plain raster store, which granules
were ingested *is* the pairing, and coincidence at read time is toc
intersection.

Epochs are **store-derived**, never catalog-derived: each reference store's
`coverage.toc` sibling (spec §10.5) records per-shard word-set covers of the
data that actually landed, quantized at temporal order 18 (2^45 ns ≈ 9.77 h
buckets). The builder expands each cover word into its constituent buckets
and takes one epoch per bucket midpoint — good to ±4.9 h against Sentinel-2's
~4.3-day revisit. Granule catalogs would inherit the CMR-hull
over-assignment (~70 assigned granules vs 49 contributing pass-days on a
measured Californian shard); covers reflect contribution, not assignment.

```python
from zagg.catalog.closest_obs import closest_obs_shardmap
from zagg.catalog.sources import Catalog
from zagg.grids import HealpixGrid
import numpy as np

grid = HealpixGrid(9, 13)  # parent_order must equal the covers' shard order
s2 = Catalog.from_geoparquet("catalog_s2_ca.parquet")

# Size the run first — the dry-run builds nothing and prices the fan-out:
est = closest_obs_shardmap(
    s2,
    ["s3://bucket/atl03_store", "s3://bucket/gedi_store"],
    grid=grid,
    aoi="california.geojson",
    max_time_offset=np.timedelta64(3, "D"),
    max_granules_per_shard=200,  # the same gate the build below applies
    estimate=True,
)
# violations is [] unless the gate is passed here too -- estimate returns
# before the build's raise, so this is the safe way to size it.
est["histogram"], est["max_cost_usd"], est["violations"]

# Then build the map; dispatch consumes it like any other ShardMap:
sm = closest_obs_shardmap(
    s2,
    ["s3://bucket/atl03_store", "s3://bucket/gedi_store"],
    grid=grid,
    aoi="california.geojson",
    max_time_offset=np.timedelta64(3, "D"),
    max_granules_per_shard=200,
)
sm.to_json("s2_closest_obs.json")
```

Everything refuses or records **loudly**, never silently: a reference store
with no readable `coverage.toc` raises (sweep the store first); an epoch
whose nearest acquisition lies beyond `max_time_offset` selects nothing and
is recorded per-epoch in `metadata["closest_obs"]["dropped"]` with its
near-miss offset; a shard past `max_granules_per_shard` raises naming the
worst shards (`estimate=True` reports the violations instead, so the gate
can be sized first); a cover block coarsened below the §10.5 pin is warned
about and reported in `coarsened_orders` — and under a `max_time_offset`,
epochs whose coarse-bucket half-span exceeds the stated offset cannot be
paired to that precision, so they drop into the ledger as their own category
(`epochs_dropped_low_resolution`, rows naming the block's effective order;
espg tolerance ruling 2026-08-24). Selected granule entries carry
`paired_epochs` / `epoch_offsets_ns` provenance so the paired product is
reconstructable from the manifest alone. Epochs are bucket midpoints —
size `max_time_offset` with `ReferenceEpochs.tolerance()`'s half-bucket
slack in mind.

::: zagg.catalog.closest_obs.reference_epochs

::: zagg.catalog.closest_obs.ReferenceEpochs

::: zagg.catalog.closest_obs.nearest_acquisitions

::: zagg.catalog.closest_obs.closest_obs_shardmap
