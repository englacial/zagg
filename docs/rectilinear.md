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

## Building a shard map

The CLI builds the grid from the config, so a rectilinear config gets a
shard map the same way HEALPix does — no special flag:

```bash
python -m zagg.catalog --config src/zagg/configs/atl06_polar.yaml \
    --short-name ATL06 --start-date 2024-01-06 --end-date 2024-04-07 \
    --polygon antarctica.geojson
```

Programmatically, `ShardMap.build` takes a `Catalog` and any grid:

```python
from zagg.catalog import load_polygon, make_shardmap
from zagg.catalog.sources import Query
from zagg.config import load_config
from zagg.grids import from_config

cfg = load_config("src/zagg/configs/atl06_polar.yaml")
grid = from_config(cfg)
parts = load_polygon("antarctica.geojson")

q = Query("ATL06", "007", "2024-01-06", "2024-04-07", region="antarctica.geojson")
sm = make_shardmap(q, grid, region=parts)   # auto backend: spherely / mortie / shapely
sm.to_json("shardmap_atl06_polar.json")
```

The shard map records `grid.signature()`; a run against a mismatched grid is
refused with a clear error.

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
