# Architecture

## Design Philosophy

### Core Axioms

1. **Data selection is declarative using STAC** - Query interfaces, not file paths
2. **Aggregation doesn't duplicate the at-rest data** - We don't reprocess and save the entire dataset; we fetch files for aggregation but discard source data after processing

### The Challenge: Sparse Point Data

When casting point data (ATL06, OPR, etc) to a grid, we encounter two issues:

1. **Too dense** - Multiple points per cell
2. **Too sparse** - No points for many cells

The sparsity is annoying but workable. The density is the real problem: xarray doesn't handle collisions natively, so we must define what to do with overlapping observations. Hence the "agg" in magg.

### Prior Art

Previously at the ICESat-2 project science office, we tackled this using hierarchical indexing: resharding ATL06 using healpix-based morton indexing to files in hive format. This kept data in columnar format (rasterized on-the-fly via vaex), but violated axiom #2 by requiring duplicated storage.

That approach also required high-memory nodes because we built the spatial tree root-to-leaves.

### Innovation: Building Leaves-to-Root

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

Target coverage: 1,872 cells covering Antarctic grounded ice drainage basins.

## Processing Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  1. Pre-process │     │  2. Parallel    │     │  3. Post-process│
│                 │────▶│     Execution   │────▶│                 │
│  Build catalog  │     │  1,700+ workers │     │  Visualize      │
│  Get S3 creds   │     │  Process shards │     │  with xdggs     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

Each worker:
1. Reads HDF5 files from S3 using h5coro (no downloads)
2. Filters points by morton cell index
3. Calculates statistics for child cells (order 12)
4. Writes parquet to S3

## Output Format

Each shard produces a parquet file with:

| Column | Description |
|--------|-------------|
| `child_morton` | Morton index at order 12 |
| `child_healpix` | HEALPix cell ID |
| `count` | Number of observations |
| `h_mean` | Weighted mean elevation |
| `h_sigma` | Uncertainty |
| `h_min`, `h_max` | Elevation range |
| `h_variance` | Variance |
| `h_q25`, `h_q50`, `h_q75` | Quartiles |

## Implementation

See [LAMBDA.md](LAMBDA.md) for AWS Lambda deployment details.
