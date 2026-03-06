# Architecture

## Core Axioms

1. **Data selection is declarative using STAC** --- query interfaces, not file paths
2. **Aggregation doesn't duplicate the at-rest data** --- we fetch files for aggregation but discard source data after processing

## The Challenge: Sparse Point Data

When casting point data (ATL06, OPR, etc) to a grid, we encounter two issues:

1. **Too dense** --- multiple points per cell
2. **Too sparse** --- no points for many cells

The sparsity is annoying but workable. The density is the real problem: xarray doesn't handle collisions natively, so we must define what to do with overlapping observations. Hence the "agg" in magg.

## Prior Art

Previously at the ICESat-2 project science office, we tackled this using hierarchical indexing: resharding ATL06 using healpix-based morton indexing to files in hive format. This kept data in columnar format (rasterized on-the-fly via vaex), but violated axiom #2 by requiring duplicated storage.

That approach also required high-memory nodes because we built the spatial tree root-to-leaves.

## Innovation: Building Leaves-to-Root

We invert the tree construction order, which:

- Enables parallel processing with small, commodity workers
- Avoids high-memory node requirements
- Allows on-the-fly processing without data duplication
- Outputs significantly compacted results via aggregation

## Vocabulary

- **Base Aggregation Cell**: Finest resolution of aggregation (leaf nodes). For ICESat-2: order 12 (~1.5km cells)
- **Shard**: Lowest level of chunking (not divisible). Contains explicit links to underlying raw granules. For our implementation: order 6 (~100km, containing 4096 base cells)

## Spatial Indexing

Uses HEALPix nested indexing via morton indices:

| Level | Order | Resolution | Purpose |
|-------|-------|------------|---------|
| Parent/Shard | 6 | ~100km | Processing unit, defines what granules to read |
| Child/Base Cell | 12 | ~1.5km | Output resolution, matches ICESat-2 beam pair spacing |

Target coverage: ~1,300 cells covering Antarctic grounded ice drainage basins (exact count determined by `mortie.morton_coverage`).

## End-to-End Flow

```
                        ┌───────────────────────────────┐
                        │   ICESat-2 ATL06 on S3        │
                        │   (NSIDC DAAC, ~2,000 HDF5    │
                        │    granules per cycle)         │
                        └───────────────┬───────────────┘
                                        │
            ┌───────────────────────────┐│┌───────────────────────────┐
            │                           │││                           │
            ▼                           ▼│▼                           │
┌───────────────────────┐  ┌────────────┴──────────────┐             │
│ 1. BUILD CATALOG      │  │ 2. AUTHENTICATE           │             │
│                       │  │                           │             │
│ catalog.py            │  │ auth.py                   │             │
│                       │  │                           │             │
│ Query CMR for cycle   │  │ earthaccess.login()       │             │
│         │             │  │         │                 │             │
│         ▼             │  │         ▼                 │             │
│ Extract S3 URLs +     │  │ Temporary S3 creds        │             │
│ polygon geometry      │  │ (valid ~1 hour)           │             │
│         │             │  └─────────────┬─────────────┘             │
│         ▼             │                │                           │
│ morton_coverage on    │                │                           │
│ 27 drainage basins    │                │                           │
│         │             │                │                           │
│         ▼             │                │                           │
│ STRtree intersection  │                │                           │
│ Map parent cells to   │                │                           │
│ granule S3 URLs       │                │                           │
│         │             │                │                           │
│         ▼             │                │                           │
│ catalog.json          │                │                           │
│ {morton: [urls]}      │                │                           │
└───────────┬───────────┘                │                           │
            │                            │                           │
            ▼                            ▼                           │
┌──────────────────────────────────────────────────────┐             │
│ 3. CREATE ZARR TEMPLATE                              │             │
│                                                      │             │
│ schema.py → xdggs_zarr_template()                    │             │
│                                                      │             │
│ CellStatsSchema metadata ──▶ GroupSpec ──▶ Zarr v3   │             │
│                                                      │             │
│ Shape:  12 × 4^child_order  (786,432 cells at O12)   │             │
│ Chunks: 4^(child - parent)  (4,096 cells at O12-O6)  │             │
│ Arrays: cell_ids, morton, count, h_min, h_max,       │             │
│         h_mean, h_sigma, h_variance, h_q25-75        │             │
│                                                      │             │
│ Written to: s3://bucket/prefix/12/                   │             │
└──────────────────────────┬───────────────────────────┘             │
                           │                                         │
                           ▼                                         │
┌──────────────────────────────────────────────────────────────────┐ │
│ 4. PARALLEL EXECUTION                                            │ │
│                                                                  │ │
│ invoke_lambda.py (orchestrator)                                  │ │
│ ThreadPoolExecutor(max_workers=1700)                             │ │
│                                                                  │ │
│ For each parent morton cell in catalog:                          │ │
│ ┌──────────────────────────────────────────────────────────────┐ │ │
│ │                    AWS Lambda Worker                         │ │ │
│ │                    (ARM64, 2GB, 15min)                       │ │ │
│ │                                                              │ │ │
│ │  lambda_handler.py                                           │ │ │
│ │       │                                                      │ │ │
│ │       ▼                                                      │ │ │
│ │  ┌──────────────────────────────────────────────────────┐   │ │ │
│ │  │  process_morton_cell()           processing.py       │   │◄┘ │
│ │  │                                                      │   │   │
│ │  │  For each granule URL:                               │   │   │
│ │  │    For each ground track (gt1l..gt3r):               │   │   │
│ │  │    ┌─────────────────────────────────────────────┐   │   │   │
│ │  │    │ READ: lat, lon via h5coro (S3 byte-range)  │◄──┼───┘
│ │  │    │                                             │   │
│ │  │    │ FILTER: geo2mort(lat,lon,O18)               │   │
│ │  │    │         clip2order(parent) == parent_morton  │   │
│ │  │    │                                             │   │
│ │  │    │ READ: h_li, h_li_sigma, quality_summary     │   │
│ │  │    │       (hyperslice on bounding indices)      │   │
│ │  │    │                                             │   │
│ │  │    │ QUALITY: keep only quality_summary == 0     │   │
│ │  │    │                                             │   │
│ │  │    │ OUTPUT: DataFrame(h_li, s_li, midx)         │   │
│ │  │    └─────────────────────────────────────────────┘   │
│ │  │                                                      │
│ │  │  Concatenate all track DataFrames                    │
│ │  │       │                                              │
│ │  │       ▼                                              │
│ │  │  clip2order(child_order=12, midx_18)                 │
│ │  │  generate_morton_children(parent, child_order)        │
│ │  │       │                                              │
│ │  │       ▼                                              │
│ │  │  For each of 4,096 child cells:                      │
│ │  │  ┌───────────────────────────────────────────────┐   │
│ │  │  │ calculate_cell_statistics()                   │   │
│ │  │  │                                               │   │
│ │  │  │ Schema-driven dispatch via AGG_FUNCTIONS:     │   │
│ │  │  │   count      → len(values)                    │   │
│ │  │  │   nanmin     → np.min(h_li)                   │   │
│ │  │  │   nanmax     → np.max(h_li)                   │   │
│ │  │  │   nanvar     → np.var(h_li)                   │   │
│ │  │  │   weighted   → Σ(h_li/σ²) / Σ(1/σ²)         │   │
│ │  │  │     _mean      using s_li as weights          │   │
│ │  │  │   weighted   → 1/√Σ(1/σ²)                    │   │
│ │  │  │     _sigma                                    │   │
│ │  │  │   quantile   → np.quantile(h_li, q)          │   │
│ │  │  │               for q ∈ {0.25, 0.50, 0.75}     │   │
│ │  │  └───────────────────────────────────────────────┘   │
│ │  │       │                                              │
│ │  │       ▼                                              │
│ │  │  mort2healpix(children) → cell_ids                   │
│ │  │  Assemble output DataFrame (11 columns × 4,096 rows) │
│ │  └──────────────────────────┬───────────────────────────┘
│ │                             │
│ │                             ▼
│ │  ┌──────────────────────────────────────────────────────┐
│ │  │  write_dataframe_to_zarr()         processing.py    │
│ │  │                                                      │
│ │  │  For each column:                                    │
│ │  │    open_array(store, "{child_order}/{col}")           │
│ │  │    array.set_block_selection(chunk_idx, values)       │
│ │  │                                                      │
│ │  │  One chunk per parent cell → concurrent-write-safe   │
│ │  └──────────────────────────────────────────────────────┘
│ └──────────────────────────────────────────────────────────┘
│                                                                  │
│ Results collected, retried on transient failures                 │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────┐
│ 5. CONSOLIDATE + ANALYZE                             │
│                                                      │
│ zarr.consolidate_metadata(store)                     │
│                                                      │
│ Output Zarr store:                                   │
│   s3://bucket/prefix/                                │
│   └── 12/                                            │
│       ├── cell_ids   (uint64, fill=0)                │
│       ├── morton     (int64,  fill=0)                │
│       ├── count      (int32,  fill=0)                │
│       ├── h_min      (float32, fill=NaN)             │
│       ├── h_max      (float32, fill=NaN)             │
│       ├── h_mean     (float32, fill=NaN)             │
│       ├── h_sigma    (float32, fill=NaN)             │
│       ├── h_variance (float32, fill=NaN)             │
│       ├── h_q25      (float32, fill=NaN)             │
│       ├── h_q50      (float32, fill=NaN)             │
│       └── h_q75      (float32, fill=NaN)             │
│                                                      │
│ Open with xarray + xdggs for visualization           │
└──────────────────────────────────────────────────────┘
```

## Module Dependency Graph

```
              ┌──────────────┐
              │  schema.py   │  Single source of truth
              │              │  CellStatsSchema, xdggs_zarr_template
              └──────┬───────┘
                     │
          ┌──────────┼──────────┐
          │          │          │
          ▼          ▼          ▼
  ┌──────────┐ ┌──────────┐ ┌───────────────────┐
  │ auth.py  │ │processing│ │ catalog.py        │
  │          │ │  .py     │ │                   │
  │ S3 creds │ │ AGG_FUNC │ │ CMR query,        │
  │          │ │ read/agg │ │ morton mapping     │
  │          │ │ write    │ │                   │
  └────┬─────┘ └────┬─────┘ └─────────┬─────────┘
       │             │                 │
       └──────┬──────┘                 │
              │                        │
              ▼                        │
  ┌───────────────────────┐            │
  │ lambda_handler.py     │            │
  │ (AWS-specific wrapper)│            │
  └───────────┬───────────┘            │
              │                        │
              ▼                        ▼
  ┌────────────────────────────────────────┐
  │ invoke_lambda.py                       │
  │ (orchestrator: catalog + auth +        │
  │  template + parallel Lambda dispatch)  │
  └────────────────────────────────────────┘
```

## Key Design Decisions

**Why one chunk per parent cell?** Each Lambda writes to exactly one chunk of the Zarr store. Since chunks are the atomic unit of Zarr writes, all workers can write concurrently without coordination or locking.

**Why h5coro?** Reading HDF5 from S3 normally requires downloading entire files. h5coro reads individual datasets via S3 byte-range requests, fetching only the data needed. A granule may be hundreds of MB, but we only read the few datasets we need for the tracks that intersect our cell.

**Why a pre-built catalog?** Without a catalog, each Lambda would need to query CMR independently to discover which granules intersect its cell. The catalog is built once (~30s) and passed to all workers, avoiding redundant CMR queries.

**Why morton indexing?** Morton (Z-order) curves preserve spatial locality --- nearby cells have nearby indices. This means a contiguous range of child indices maps to exactly one parent cell, enabling efficient `clip2order` operations and chunk-aligned writes.

## Output Format

Results are written to a [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) store following the [DGGS convention](https://github.com/zarr-conventions/dggs). The template is generated by [`xdggs_zarr_template`][magg.schema.xdggs_zarr_template] from the pandera schema, with one chunk per parent shard cell.

See [Schema](schema.md) for details on the output schema and aggregation dispatch.
