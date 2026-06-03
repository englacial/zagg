# Rectilinear (projected) output grids

zagg supports rectilinear projected grids as write targets, alongside HEALPix
DGGS. The canonical case is polar stereographic (EPSG:3031 for Antarctic
ATL06 work), but any CRS pyproj can transform to is supported.

## Config

```yaml
output:
  grid:
    type: rectilinear
    crs: EPSG:3031
    resolution: 5000              # cell size in CRS units; scalar or [res_x, res_y]
    bounds: [-3200000, -3200000, 3200000, 3200000]   # xmin, ymin, xmax, ymax
    chunk_shape: [256, 256]       # [chunk_h, chunk_w] cells per chunk
```

Constraint: `chunk_shape` must divide the grid shape evenly. With the
example above the grid is `1280×1280` cells (6.4 Mm / 5 km each side),
yielding a `5×5` chunk grid of `256×256` blocks.

## Storage layout

- 2D Zarr arrays at path `rectilinear/<varname>`, shape `(height, width)`.
- 1D coord arrays `x` and `y` (CF projection coordinates).
- Group attrs: `crs`, `resolution`, `bounds` (GeoZarr-style).
- One chunk per shard; no remap from spatial identity to storage position.

## Building a catalog

`build_catalog()` takes an optional `grid` parameter. Pass a
`RectilinearGrid` instance to enumerate shards via `grid.coverage` and
`grid.shard_footprint` rather than the HEALPix-specific defaults:

```python
import json
from zagg.catalog import query_cmr, build_catalog, load_polygon
from zagg.config import load_config
from zagg.grids import from_config

cfg = load_config("src/zagg/configs/atl06_polar.yaml")
grid = from_config(cfg)

granules = query_cmr(start_date="2024-01-06", end_date="2024-04-07",
                     short_name="ATL06", version="007")
polygon_parts = load_polygon("antarctica.geojson")
catalog, _ = build_catalog(granules, polygon_parts=polygon_parts, grid=grid)

shard_keys = sorted(catalog.keys())
out = {
    "metadata": {"grid_type": "rectilinear", "total_cells": len(catalog)},
    "shard_keys": shard_keys,
    "granules": [catalog[k] for k in shard_keys],
}
with open("catalog_atl06_polar.json", "w") as f:
    json.dump(out, f)
```

The catalog format is the shard_keys/granules layout (PR-C). Old
dict-keyed catalogs are rejected with a clear error — regenerate them.

A `--grid-type rectilinear` flag for the `python -m zagg.catalog` CLI is
on the roadmap; for now use the programmatic form above.

## Running

```python
from zagg import load_config, agg

cfg = load_config("src/zagg/configs/atl06_polar.yaml")
results = agg(
    cfg,
    catalog="catalog_atl06_polar_cycle22.json",
    store="s3://my-bucket/atl06_polar_cycle22.zarr",
    backend="lambda",
)
```

## Cell indexing

Internally, rectilinear cells are addressed as flat row-major indices
`r * width + c`. Shard keys are packed `rb * n_col_blocks + cb`. Both are
plain ints, so they JSON-serialize cleanly in catalog files. Pipeline
code never sees raw `(row, col)` tuples; only `block_index()` returns the
2-tuple that zarr's `set_block_selection` needs.

## When to choose rectilinear over HEALPix

| Use case | Choose |
|---|---|
| Antarctic/Arctic ice-sheet work, want raster co-located with climate models | **rectilinear (EPSG:3031 / EPSG:3413)** |
| Global DGGS analysis, downstream xdggs viz | **healpix fullsphere** |
| Polygon-zone aggregation (basins, watersheds) | (Phase 2 — `PolygonZoneGrid`, not yet shipped) |

Polar stereographic is conformal but not equal-area — the scale factor is 1
at the standard parallel (~71°S in EPSG:3031) and varies away from it.
Doesn't affect write-partitioning but matters for area-weighted downstream
statistics.
