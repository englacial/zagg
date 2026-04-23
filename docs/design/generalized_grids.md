# Broadening input/output grid structures

**Status**: draft · partial (§1–§4). Tracks [#11](https://github.com/englacial/zagg/issues/11).

> All design decisions (both made and open) are consolidated in §11. Inline references use **D#** for decisions made (rationale in §11.1) and **O#** for open items needing input (§11.2). Revisit before merging.

## 1. Motivation

The initial scope (discussion [#2](https://github.com/englacial/zagg/issues/2)) settled on *point → HEALPix DGGS* gridding, demonstrated against ICESat-2 ATL06. The pipeline works at continental scale on AWS Lambda ($2–3 for an Antarctic orbital cycle), and the core pattern — catalog once, per-cell parallel read + aggregate + write — is substantially grid-agnostic.

At our call with @espg we agreed that the HEALPix-only output is artificially narrow. Broadening output to *any* regular grid opens several real uses:

- **Rectilinear output** (lat-lon or projected, e.g. EPSG:3031 for Antarctic work) so zagg products interop directly with climate reanalysis and other rastered datasets. Polar stereographic is in fact the cleanest rectilinear case — the pole is a regular grid cell, there's no dateline wrap, and chunk-block writes partition naturally.
- **Polygon-zone output** — drainage basins, watersheds, political boundaries, flux-tower footprints. Arguably the biggest value-add for glaciology/hydrology users who think in basins, not cells.
- **H3 output** for downstream users on Uber/carto stacks and for simpler integer-cell semantics in web viz.
- **Positioning**: zagg becomes the cloud-native point-to-any-grid aggregation engine. Closest existing peers (`xagg`, `geopandas.sjoin + groupby`) lack the serverless fan-out.

Secondary benefit: the resulting grid abstraction aligns directly with the broader community consolidation around modular I/O + indexing + resampling — see the [Cloud-native resampling and reprojection](https://developmentseed.org/warp-resample-profiling/) site (Ecosystem & Roadmap page) for the landscape overview, and [Sean Harkins' modular-libraries proposal](https://gist.github.com/sharkinsspatial/f1c3a8f871b58416fa30c377178b5f9c) for one concrete instance of the argument.

## 2. Scope and non-goals

### In scope (v1)
- Multiple **output** grid types from the same input pipeline.
- Config-driven grid selection (same pattern as existing aggregation dispatch in `schema.py`).
- HEALPix/ATL06 path continues to work without user-visible change.
- Write-partition strategy per grid type (see §5).
- Zarr-template generation per grid type (see §6).

### Out of scope (v1)
- **Arbitrary input grids.** Point-cloud input (HDF5 via h5coro) is the only input data model. Input generalization is deferred to v2 (**D1**).
- **Grid-to-grid regridding.** zagg remains an F4 point-to-cell engine, to stay in one mathematical category.
- **Generalized downsampling of raster inputs.** Out of scope by the input-restriction above.

### Explicit constraints
- Backwards compatibility for the HEALPix path is a hard requirement. The existing `configs/atl06.yaml` and all downstream consumers (xdggs viz, etc.) must keep working byte-for-byte.
- Each new grid backend is an optional install — don't drag `h3-py`, `s2sphere`, or `grid-indexing` into the default dependency set.

## 3. Background: what's HEALPix-specific today

Walking the pipeline from `docs/design/architecture.md`, here's what stays generic vs. what needs pluggable behavior.

| Stage | Component | Currently HEALPix-specific? |
|---|---|---|
| **1. Catalog** | CMR query | generic |
| | `morton_coverage` (enumerate parent shards for a region) | **yes** — DGGS shard enumeration |
| | STRtree granule-to-cell intersection | generic (uses shapely on cell footprints) |
| **2. Auth** | `earthaccess.login` | generic |
| **3. Template** | `xdggs_zarr_template` | **yes** — xdggs-conformant metadata, 1D `12 × 4^order` shape |
| **4a. Parallel exec — read** | h5coro byte-range reads | generic |
| **4b. Point filter** | `geo2mort`, `clip2order` | **yes** — DGGS cell assignment |
| **4c. Aggregation** | `calculate_cell_statistics`, config-driven dispatch | generic (this is the best existing extension point) |
| **4d. Child enumeration** | `generate_morton_children` | **yes** — DGGS hierarchy walk |
| **4e. Encode cell IDs** | `mort2healpix` | **yes** — DGGS-specific encoding |
| **4f. Write** | `array.set_block_selection(chunk_idx, values)` | generic, *given* chunk_idx computed by a grid-aware function |
| **5. Consolidate** | `zarr.consolidate_metadata` | generic |

The schema-driven aggregation dispatch (§4c) and the Zarr block-write (§4f) are already grid-agnostic. The grid-specific pieces cluster cleanly around three operations: **shard enumeration**, **point-to-cell assignment**, and **template emission** — exactly what the protocol in §4 formalizes.

## 4. Key abstraction: the `OutputGrid` protocol

A small protocol (duck-typed or `typing.Protocol` ABC, implementer's choice) that encapsulates every grid-specific operation the pipeline needs.

### Proposed interface

```python
class OutputGrid(Protocol):
    def coverage(self, bbox: BBox) -> Iterable[ShardKey]:
        """Enumerate shard-level cells covering a region. (Catalog stage.)"""

    def assign(self, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
        """Map points to leaf cell IDs. (Per-worker processing.)"""

    def shard_of(self, cell_ids: np.ndarray) -> ShardKey:
        """Which write-partition does a set of cell IDs belong to? (Write stage.)
        Must return a contention-free key; see §5."""

    def emit_template(self, schema: AggregationSchema) -> ZarrStore:
        """Initialize the output Zarr store with the right shape, chunks,
        metadata, and fill values for this grid. (Template stage.)"""

    def cell_geometry(self, cell_ids: np.ndarray) -> geopandas.GeoSeries:
        """Optional: return cell polygons. Useful for downstream viz/validation."""
```

Five methods. The first four are load-bearing; `cell_geometry` is a convenience for viewers and is optional.

### Proposed implementations

| Grid | Backend | Install group | Priority |
|---|---|---|---|
| `HealpixGrid` | `mortie` (current) | (default) | Phase 1 — must stay identical to today |
| `H3Grid` | `h3-py` | `zagg[h3]` | Phase 1 — easy, high user value |
| `RectilinearGrid` | affine transform via `pyproj` | `zagg[rectilinear]` | Phase 1 — climate interop |
| `S2Grid` | `s2sphere` | `zagg[s2]` | Phase 2 — lower priority |
| `PolygonZoneGrid` | `grid-indexing` for R-tree point-in-polygon | `zagg[polygons]` | Phase 2 — hardest, see §5 |
| `CurvilinearGrid` / mesh | TBD | — | Out of scope for v1 |

Phase 1 targets HEALPix + H3 + RectilinearGrid (**D2**); polygon zones ship in Phase 2 once the write-partition abstraction (§5.4) is settled. Full schedule in §10.

### Config surface

Each grid type has a distinct YAML block. Keep the rest of the config (`aggregation`, `data_source`, etc.) unchanged.

```yaml
# Current — unchanged
output_grid:
  type: healpix
  parent_order: 6
  child_order: 12
```

```yaml
# New — H3
output_grid:
  type: h3
  parent_resolution: 4
  child_resolution: 7
```

```yaml
# New — projected rectilinear (Antarctic Polar Stereographic)
output_grid:
  type: rectilinear
  crs: EPSG:3031
  resolution: 5000
  bounds: [-3200000, -3200000, 3200000, 3200000]
  chunk_shape: [256, 256]
```

```yaml
# New — polygon zones (Phase 2)
output_grid:
  type: polygon_zones
  cells_uri: s3://bucket/drainage_basins.parquet
  partition:
    strategy: sfc_centroid       # or: manual, file
    n_partitions: 128
```

A small `OutputGrid.from_config(config_dict)` factory selects the right implementation and handles optional-dependency errors cleanly.

### Design principles

- **Stateless interface.** `OutputGrid` instances hold grid parameters (orders, bounds, resolutions, zone geometries) but no per-job state. Safe to pickle across Lambda boundaries.
- **Opt-in dependencies.** Each backend ships as an extra. Default install stays lean.
- **Share with the ecosystem.** Where the same operation exists in `grid-indexing` (point-in-polygon, R-tree coverage), `PolygonZoneGrid` delegates rather than reimplements. Where `xdggs` defines conformant metadata, `HealpixGrid.emit_template` and `H3Grid.emit_template` emit exactly that. See §9.
- **`RectilinearGrid` accepts (or can convert to) an `odc.geo.GeoBox`.** This is the idiom OpenEO / Pangeo use for grid specs; accepting it directly keeps zagg composable with the rest of the Layer-4 ecosystem.
- **No xarray coupling in the protocol.** The protocol returns numpy arrays and Zarr stores. Users who want xarray open the Zarr with xarray afterwards. Keeps the core lightweight and matches the "xarray-independent" design goal shared by other emerging indexing/resampling libraries.

## 5. The write-partition problem

This is the load-bearing design section. Write-partitioning is the property that gave zagg $2-per-Antarctic-cycle economics; any generalization that loses it has failed. HEALPix gives it for free by construction. Other grids give it for free *if* chosen carefully. Polygon zones don't give it at all — which is exactly why they're Phase 2.

### 5.1 Why this matters

Zarr has no write-lock protocol. Two workers writing to the same chunk race, and whichever `put_object` lands last wins. So the pipeline's parallel safety depends on one invariant:

> **Each output chunk is written by exactly one worker, and no worker writes to another's chunk.**

In the current HEALPix pipeline, each parent cell (order 6) covers 4,096 leaf cells (order 12), which maps to exactly one Zarr chunk. Number of chunks = number of parent cells = number of Lambda invocations. Each Lambda owns one chunk. Zero contention, 1,700-way concurrency.

Lose this invariant and you lose either (a) parallelism (serialize writes), (b) correctness (race and pray), or (c) scalability (locking adds coordination overhead that defeats the serverless model).

### 5.2 Three conditions for a contention-free partition

Any output grid that preserves zagg's scale-out properties must satisfy:

1. **Hierarchical coarsening.** There's a "shard" level above the leaf level that partitions the leaf-cell space into disjoint blocks.
2. **Chunk alignment.** Zarr chunk boundaries coincide with shard boundaries. One shard's leaves fill exactly one chunk.
3. **Cheap leaf enumeration.** Given a shard, the ordered list of its leaf cells can be produced without reading data.

HEALPix/Morton satisfies all three trivially. Walking each Phase 1 grid through the same lens:

### 5.3 Phase 1 grids — hierarchical sharding for free

#### HealpixGrid (current)

Shard order < child order; leaves per shard = `4^(child - shard)`. Chunks = `4^(child - shard)`. Children enumerated via `generate_morton_children()`. No change from today.

#### H3Grid

H3 has a native parent/child relation with ~7× fan-out per resolution step (the pentagon cells have 6 children instead of 7, but H3's library handles this). Each shard resolution gives a fixed number of cells globally, and each parent hex resolves to a known child list via `h3.cell_to_children()`.

- Leaves per shard: `7^(child_res − shard_res)` minus pentagon corrections.
- Chunk strategy: `chunk_size = 7^Δ` rounded up, with fill-value padding for pentagon shards (small storage waste).
- `shard_of(cells) = h3.cell_to_parent(cells[0], shard_resolution)` after asserting all cells share that parent.
- Pentagon-path chunks are fill-value padded rather than jagged (**D3**).

#### RectilinearGrid (incl. polar stereographic)

The chunk grid *is* the shard partition. Pick a chunk shape (e.g., 256×256) and every non-edge chunk has a fixed cell count; shards and chunks are the same object.

- `shard_of((rows, cols)) = (rows[0] // chunk_h, cols[0] // chunk_w)` after asserting all cells share that block.
- Polar stereographic inherits this trivially: EPSG:3031 is a plane, chunks tile the plane, no wrap-around at dateline, no degenerate pole row. Under standard half-open `[low, high)` cell assignment the pole at (0,0) lands in exactly one cell even when the chosen bounds put it on a chunk corner (or offset bounds by half a cell to make it strictly interior). Cleaner than global lat/lon grids — which *do* need dateline handling and pole-row special-casing, worth calling out as an implementation note when someone adds `LatLonGlobalGrid` later.
- Caveat (science, not partitioning): polar stereographic is conformal, not equal-area. A fixed projected chunk size corresponds to different ground areas at different latitudes (scale factor = 1 at the standard parallel ~71°S, varies away from it). Doesn't affect write-partitioning but matters for any downstream area-weighted statistics on the output.
- Shard = chunk, one-level partitioning (**D4**). Workers may batch multiple chunks per invocation, but each chunk still has exactly one writer.

#### S2Grid (Phase 2 but same family)

Same structure as H3 (quadtree with ~4 children per parent step). Folds cleanly into the same hierarchical-sharding model when it lands.

### 5.4 Phase 2: polygon zones — no natural hierarchy

User-supplied polygon zones (drainage basins, watersheds, counties, flux-tower footprints) have no inherent hierarchy. You can't coarsen "27 Antarctic drainage basins" into "6 super-basins" without extra information the user hasn't provided.

Five options considered:

| Option | Pro | Con |
|---|---|---|
| **A. User supplies a partition file** | Explicit, auditable, zero compute | Burden on user; easy to get wrong |
| **B. SFC over centroids** (Morton/Hilbert) | Preserves spatial locality → I/O-friendly | Requires centroid-CRS choice; uneven zone density produces uneven work |
| **C. K-means clustering** | Even partition by count | Loses spatial locality; centroid-reshuffle on each run non-deterministic |
| **D. Single-writer fallback** | Trivially correct | Destroys zagg's value prop |
| **E. Zone-per-chunk** | Zero contention, trivially correct, trivially parallel | Per-chunk Zarr overhead if zone count is huge |

For the current use cases (ice-sheet basins at ≤~1000 zones, HydroBASINS at ≤~10k), **Option E (zone-per-chunk) is the clean default**. One zone = one 1D chunk = one worker. No SFC math, no partition-file authoring, no hierarchy to maintain. The Zarr chunk-metadata overhead for 1k–10k chunks is on the order of tens of KB — negligible.

Above ~10k zones, per-chunk metadata starts to dominate. At that scale, **Option B (SFC-centroid batching)** groups N zones per chunk using a Morton index on centroids in the user's working CRS (EPSG:3031 for polar work, EPSG:4326 for global). Within-chunk writes happen in one worker, across-chunk writes stay disjoint.

Default: **Option E ≤ 10k zones**, auto-switch to **Option B** above (SFC with `n_zones_per_chunk = 100`). **Option A** remains as an explicit override via `partition: { strategy: file, path: … }` (**D5**; threshold is **O1**).

### 5.5 The `shard_of` contract

Formalizing what every grid implementation must guarantee:

```python
def shard_of(self, cell_ids: np.ndarray) -> ShardKey:
    """Given a batch of leaf cell IDs, return the shared shard key.

    Contract:
      - All input cells MUST belong to the same shard; raise InconsistentShardError otherwise.
      - Output is a hashable, serializable key that uniquely identifies one Zarr chunk.
      - Two distinct ShardKeys map to disjoint Zarr byte ranges.
      - For any cell c, shard_of([c]) is stable across Python sessions and machines.
    """
```

Per-grid implementations:

| Grid | `shard_of([cell, …])` |
|---|---|
| HealpixGrid | `mortie.clip2order(cells, shard_order)[0]` after same-shard assertion |
| H3Grid | `h3.cell_to_parent(cells[0], shard_resolution)` after same-parent assertion |
| RectilinearGrid | `(rows[0] // chunk_h, cols[0] // chunk_w)` after same-block assertion |
| PolygonZoneGrid (E) | `zones[0]` (each zone is its own shard) |
| PolygonZoneGrid (B) | `partition_table[zones[0]]` (precomputed at template time) |

### 5.6 Catalog-stage interaction

The catalog currently emits `{morton_parent: [granule_urls]}` via `morton_coverage(bbox) ∩ STRtree(granules)`. Generalizing, the catalog emits `{shard_key: [granule_urls]}` using `grid.coverage(bbox)` to enumerate shards and per-shard polygonal footprints for the STRtree step.

Each grid therefore owes the catalog a `shard_footprint(shard_key) → shapely.Polygon`:

- HealpixGrid: the parent cell's boundary (mortie already provides).
- H3Grid: `h3.cell_to_boundary()`.
- RectilinearGrid: the chunk's bbox in the grid CRS, reprojected to WGS84 for granule intersection. The chunk grid is wrap-free in the projected plane, but lat/lon granule footprints aren't — reprojection must densify polygon edges and handle antimeridian-crossing source geometry (e.g. `pyproj.Transformer.transform_bounds()` or shapely densification before projecting), or footprints get clipped at the seam and granules get dropped from coverage.
- PolygonZoneGrid: the zone's own geometry (Option E) or the union of zones in the partition (Option B).

`shard_footprint(key) → Geometry` is added as a sixth method on `OutputGrid` to keep catalog code grid-agnostic (**D6**).

### 5.7 The chunk-alignment invariant

Two places in the grid implementation must agree on shape:

- `emit_template(schema)` — sets Zarr chunk shape during template creation.
- `shard_of(cell_ids)` — defines which chunk a set of cells belongs to.

If these disagree, concurrent writes silently corrupt. The protocol can't enforce this programmatically, but each implementation's tests must verify:

```python
def test_shard_of_matches_template():
    template = grid.emit_template(minimal_schema)
    chunk_shape_from_template = template['/12/h_mean'].chunks
    assert shard_size(grid) == chunk_shape_from_template
```

This test is required in the `OutputGrid` suite, enforced by a shared parametrized fixture across all implementations (**D7**; fixture design is **O4**).

### 5.8 Summary

For Phase 1 (HEALPix, H3, rectilinear incl. polar stereo), the write-partition story is **one story**: hierarchical sharding, shard key = parent/block identifier, chunk aligned to shard, one writer per chunk. No new algorithmic work beyond per-grid boilerplate.

For Phase 2 polygon zones, the default is **zone-per-chunk** (trivially correct for ≤10k zones), with an SFC-centroid escape hatch for large zone counts and a user-supplied partition file for advanced cases. No other option preserves zagg's scale-out without user burden.

The one invariant that matters throughout: **shard_of matches emit_template matches the Zarr chunk shape**, verified by the shared test suite.

## 6. Template generalization

Currently `zagg.schema.xdggs_zarr_template()` constructs an xdggs-conformant Zarr v3 group via `pydantic-zarr`. The schema-driven aggregation variables (count, h_mean, h_min, quantiles, …) stay grid-agnostic; what changes per grid is **shape, chunking, coordinate arrays, and metadata attributes**.

The generalization is straightforward: each `OutputGrid` implementation owns its own `emit_template(schema) → ZarrStore`. The schema object carries the variable definitions (dtype, fill, function/expression) unchanged across grid types. Each grid's `emit_template` decides the structural layout.

| Grid | Array shape | Chunk shape | Coord arrays | Metadata conformance |
|---|---|---|---|---|
| HealpixGrid | `(12 · 4^child_order,)` | `4^(child − shard)` | `cell_ids` (uint64), `morton` (int64) | **xdggs** (unchanged) |
| H3Grid | `(n_cells_at_resolution,)` | `≈ 7^Δ` padded | `h3_index` (uint64) | **xdggs** (H3 variant) |
| S2Grid | `(n_cells_at_resolution,)` | `≈ 4^Δ` | `s2_cell_id` (uint64) | **xdggs** (S2 variant) |
| RectilinearGrid | `(height, width)` 2D | user-specified, typically `(256, 256)` | `x`, `y` + `crs` attr | **CF + GeoZarr** |
| PolygonZoneGrid | `(n_zones,)` 1D | `1` (Option E) or `n_zones_per_chunk` (Option B) | `zone_id` + optional `zone_geometry_uri` | minimal structural spec (no community standard) |

A few design notes:

- **`pydantic-zarr` stays the template builder for every grid.** Each grid supplies a `GroupSpec` constructor function; the aggregation variables (from the YAML config) are appended identically across all grids.
- **The `xdggs_zarr_template` function becomes `HealpixGrid.emit_template` internally.** The public helper stays available (see §8).
- **For `RectilinearGrid`, `emit_template` accepts an `odc.geo.GeoBox` directly.** This is the path to OpenEO/xarray-regrid/odc-stac interop without zagg reinventing grid-spec parsing.
- **For `PolygonZoneGrid`, there's no community standard**, so `emit_template` emits a minimal spec: a 1D cell axis indexed by `zone_id`, a `zone_geometry_uri` attribute pointing at the source geopackage/parquet (so downstream viewers can rehydrate zone polygons), and a `crs` attribute describing the working CRS used for indexing.
- **The aggregation variable block is shared code.** Only coord arrays and structural metadata differ per grid.

## 7. Input-side considerations

v1 is explicitly output-only per §2. Worth briefly acknowledging three input-side directions that may arise in later conversations, and why they don't need resolution now:

- **Gridded point inputs** (e.g., already-binned ATL06 products, HDF5 files with per-cell tables rather than per-shot observations). These could be supported by adding a reader adapter that converts per-cell records to per-shot tuples, but they don't change the output pipeline.
- **Raster inputs → different grid** (F4 within-cell reduction). This is a different pipeline entirely — downsample one regular grid to another with `σ` over contributing source cells. Close in math to what zagg does but with raster I/O (COGs, Zarr) instead of HDF5 point reads. Arguably a separate tool, or a sibling entry point to `zagg` that reuses the aggregation dispatch and `emit_template` machinery.
- **Polygon inputs with values** (areas already carrying measurements, being re-aggregated). This is F3 conservative territory (area-weighted regridding). Squarely out of scope — use `grid-weights` or `xESMF`.

Revisit after v1 ships (**D1**).

## 8. Backwards compatibility

Non-negotiable constraints:

- **Existing `configs/atl06.yaml` works unchanged.** Zagg auto-detects the old schema and treats a config without `output_grid` block as `output_grid: {type: healpix, parent_order: 6, child_order: 12}`.
- **HEALPix output is byte-identical to the current pipeline.** Regression test: produce an atl06 cycle under the new code path, compare against a golden Zarr store produced by the current code path. Any bit-level drift is a bug.
- **Public helper `zagg.schema.xdggs_zarr_template` keeps its signature.** Internally, it delegates to `HealpixGrid.emit_template`. Users and notebooks calling it today see no change.
- **The existing CLI entry points (`build-catalog`, `invoke-lambda`, etc.) work on new configs and old configs alike.** Detection happens at config-load time, not at the CLI boundary.

Migration path: none required for existing users. New grids are opt-in via the `output_grid.type` field.

Testing strategy:

- **Regression**: byte-identical HEALPix output vs. a checked-in golden store.
- **Protocol conformance**: each grid backend passes the shared `OutputGrid` test suite (including the chunk-alignment invariant from §5.7).
- **Integration**: end-to-end tests on a synthetic small dataset for each grid type.

## 9. Ecosystem alignment

A useful framing: zagg's `OutputGrid` protocol is the point-assignment cousin of a broader grid-indexing primitive the ecosystem is converging on. Aligning rather than forking matters for long-term maintenance and cross-pollination.

**Direct dependencies we should consider taking on:**

- **`grid-indexing`** (Justus Magin, Rust + PyO3 R*-tree over polygon cells). Natural backend for `PolygonZoneGrid.assign()`. Handles bulk point-in-polygon at Rust speed. Taken as a dependency via the `zagg[polygons]` extra (**D8**).
- **`xdggs`** (xarray-contrib). Metadata standard for HealpixGrid, H3Grid, S2Grid outputs. Already the target for the current pipeline; stays the target for Phase 1 DGGS grids.
- **`odc.geo.GeoBox`**. Accept-as-input for `RectilinearGrid`. Provides free interop with the Pangeo/OpenEO separable-reprojection workflow (`odc-geo.xr_reproject` is the community consensus for lazy CRS warp).

**Alignment targets (not necessarily deps, but worth harmonizing protocol shape with):**

- **A proposed generic, transform-aware indexing library** ([modular-libraries proposal by Sean Harkins, public gist](https://gist.github.com/sharkinsspatial/f1c3a8f871b58416fa30c377178b5f9c)). A chunk-indexing abstraction for Zarr and COG, Rust-based and potentially TS-portable. Zagg's `OutputGrid` is the point-assignment and write-partition sibling of the same concept. If the protocol shape can be shared (even where implementations differ — zagg cares about cell IDs; the proposed library cares about byte ranges), the ecosystem consolidates on one spec.
- **[Cloud-native resampling and reprojection](https://developmentseed.org/warp-resample-profiling/)** (Ecosystem & Roadmap page). A broader landscape view of cloud-native resampling: mathematical families, architectural layers, and a tools-vs-families coverage matrix. zagg occupies Layer 4b (point-cloud → grid aggregation); this design doc is the detailed spec for expanding it.
- **`xarray.DataTree` / VirtualiZarr `ManifestStore` / STAC `ItemCollection`**. Not deps for zagg, but they're the existing landscape for "hold disparate arrays lazily." Zagg's output consumers often want to co-load a zagg Zarr with other datasets — keep `emit_template` output clean and CF/xdggs-conformant so downstream DataTree / xarray / rioxarray / xdggs paths just work.

**Deliberate divergences worth naming:**

- Zagg is **xarray-independent** at the core. The protocol returns numpy arrays and Zarr stores. xarray users open the output with xarray afterwards. This matches the modular-libraries design goals and keeps zagg usable from non-Python runtimes (future WASM, TS port) without an xarray port.
- Zagg does **not** try to be a general-purpose regridder. F3 conservative, F5 mesh projection, and F1 kernel interpolation stay with xESMF, grid-weights, ESMF, and GDAL. The `OutputGrid` abstraction is narrowly scoped to point-to-cell assignment (F4) plus the write-partition concerns that follow.

**What this means for cross-project alignment:**

Zagg generalized along these lines becomes concrete prior art for the modular `I/O + indexing + resampling` approach — it ships the separation pattern, just with HDF5 point inputs and F4 aggregation instead of COGs and F1 warp. Worth surfacing in public forums (Pangeo, xarray-ecosystem venues, the [warp-resample-profiling ecosystem page](https://developmentseed.org/warp-resample-profiling/)) once the design is stable.

## 10. Phased roadmap

### Phase 0 — Protocol extraction (no behavioral change)

Pure refactor. Introduce the `OutputGrid` protocol. Move existing HEALPix-specific code into `HealpixGrid`:

- `xdggs_zarr_template` → `HealpixGrid.emit_template` (alias preserved)
- `geo2mort` / `clip2order` → `HealpixGrid.assign`
- `morton_coverage` → `HealpixGrid.coverage`
- parent cell derivation → `HealpixGrid.shard_of`
- Add `HealpixGrid.shard_footprint`

Regression test confirms byte-identical output. Merge this first — everything else depends on it.

### Phase 1 — New hierarchical grids

Each of these is independent and unblocks once Phase 0 lands.

- `H3Grid` via `h3-py` (pentagon padding per §5.3)
- `RectilinearGrid` via `pyproj` + `odc.geo.GeoBox` acceptance (incl. polar stereographic as the cleanest case)

Ship each behind its own extra (`zagg[h3]`, `zagg[rectilinear]`).

### Phase 2 — Polygon zones

- `PolygonZoneGrid` via `grid-indexing`
- Default partition strategy: zone-per-chunk ≤10k zones
- SFC-centroid batching strategy (Morton over centroids in working CRS)
- User-partition-file escape hatch

Ship behind `zagg[polygons]`.

### Phase 3 — Deferred

- `S2Grid` (mechanically Phase-1 family but lower priority)
- `CurvilinearGrid`, mesh, time-varying grids — revisit after Phase 2 lands

### Success criteria per phase

Phase 0: regression test passes; no user-visible change.
Phase 1: end-to-end ATL06 run with output in H3 and EPSG:3031 polar stereo. Cost and wall-clock within 2× of HEALPix baseline.
Phase 2: Antarctic drainage basin aggregation in ≤10 min on Lambda, demonstrably correct against a single-worker reference.
Phase 3: as scoped.

## 11. Decisions registry

All design calls made in drafting this document, plus the items still needing input. Inline references above use **D#** / **O#** from this table.

### 11.1 Decisions made (rationale recorded)

| ID | Decision | Rationale | Where |
|---|---|---|---|
| **D1** | v1 scope is output-side only; input generalization deferred to v2 | Bounded protocol work; real user value concentrated on the output side | §2, §7 |
| **D2** | Phase 1 grid set = HEALPix + H3 + RectilinearGrid (incl. polar stereographic) | All three share hierarchical sharding — §5 generalizes uniformly | §4, §10 |
| **D3** | H3 pentagon cells → fill-value padded chunks (not jagged) | ≤0.1% storage waste; vastly simpler than jagged-chunk support | §5.3 |
| **D4** | RectilinearGrid: shard = chunk (one-level, not two-level like HEALPix) | Chunks are already the right partition size; no shard/chunk distinction to maintain | §5.3 |
| **D5** | PolygonZoneGrid partition: zone-per-chunk ≤ 10k zones, SFC-centroid above, user-file override | Trivially correct for realistic zone counts; scales with an explicit escape hatch | §5.4 |
| **D6** | `shard_footprint(key) → Geometry` is a sixth method on the `OutputGrid` protocol | Keeps catalog-stage code grid-agnostic | §5.6 |
| **D7** | Chunk-alignment invariant enforced via a required shared parametrized test fixture | Only way to catch `emit_template` / `shard_of` drift automatically | §5.7 |
| **D8** | `PolygonZoneGrid` takes a `grid-indexing` dependency via `zagg[polygons]` extra | Rust-speed R*-tree point-in-polygon; standard in the emerging ecosystem | §9 |
| **D9** | Protocol mechanism = `typing.Protocol` (interface) + optional `abc.ABC` (shared helpers) | Structural typing for extensibility; ABC only if useful defaults emerge | §4 |
| **D10** | ShardKey type per grid (tuple / int / bytes / …); contract is "hashable and JSON-serializable" | Each grid's natural shard identifier is different; no value in forcing a common type | §5.5 |
| **D11** | `cell_geometry()` is optional in the protocol; Phase 1 implementations add it only if cheap | Nice-to-have for viz, not load-bearing | §4 |
| **D12** | `RectilinearGrid` accepts both `odc.geo.GeoBox` (recommended) and a manual `{crs, resolution, bounds}` dict | GeoBox is the OpenEO/Pangeo idiom; manual dict is bootstrap-friendly | §4, §9 |
| **D13** | Protocol surface is xarray-independent (numpy arrays + Zarr stores) | Cross-language portability (WASM/TS futures); matches the modular-libraries design goals | §9 |
| **D14** | §9 cites only public references (warp-resample-profiling ecosystem page, Sean Harkins' public gist); no private-discussion references | Keeps the design doc reviewable by anyone without access to private context | §9, §12 |

### 11.2 Open for review (input needed)

- **O1 — Zone-per-chunk threshold.** `n_zones ≤ 10_000` is a rough estimate based on Zarr chunk-metadata overhead. Validate against the zone counts target workloads actually use (Antarctic ice-sheet basins, HydroBASINS levels, county shapefiles).
- **O2 — SFC choice for `PolygonZoneGrid`.** Morton (proposed — simpler, matches existing code) vs. Hilbert (better locality). For typical workloads (≤10k zones with clean centroids), does the locality difference matter?
- **O3 — Long-thin zones.** River corridors and fjord polygons have centroids far from their polygon mass. Does SFC-centroid batching still preserve I/O locality for these cases? Empirical question for Phase 2.
- **O4 — Test-suite parametrization.** How to parametrize the shared `OutputGrid` test suite across backends without making each backend's test setup heavyweight. Preferred shape: pytest parametrization with a lightweight factory per grid type.
- **O5 — Phase 1 success target.** "Within 2× of HEALPix baseline" for H3 and rectilinear cost + wall-clock is a guess. Validate with a prototype benchmark before committing to the target.
- **O7 — `cell_geometry()` priority.** Phase 1 viz uses xdggs viewers that generate geometry themselves. Does zagg need to expose `cell_geometry()` in v1 at all, or defer to Phase 2?

## 12. Prior art and references

### Zagg-adjacent
- [discussion #2](https://github.com/englacial/zagg/issues/2) — original design discussion; Shane's open question #4 is the seed of this work.
- `docs/design/architecture.md` — current HEALPix-specific pipeline description.
- `docs/design/schema.md` — existing config-driven aggregation dispatch (stays generic across grids).
- `mortie` — current HEALPix/Morton implementation.

### Point-to-grid and zonal aggregation
- [`xagg`](https://github.com/ks905383/xagg) — zonal aggregation of raster data onto polygons. Closest conceptual peer for the PolygonZoneGrid case.
- `geopandas.sjoin + groupby` — ad-hoc Python pattern for small-scale point-to-polygon aggregation.
- `pyresample.kd_tree` — scattered-point-to-grid with kernel weighting (F2 territory; orthogonal to zagg).

### Grid indexing and protocols
- [`grid-indexing`](https://github.com/keewis/grid-indexing) — Rust R*-tree over polygon cells. Proposed backend for `PolygonZoneGrid.assign`.
- [`grid-weights`](https://github.com/keewis/grid-weights) — area-weighted conservative regridding. Uses `grid-indexing` and shares the sparse-weight-matrix pattern.
- [`xdggs`](https://github.com/xarray-contrib/xdggs) — xarray-contrib DGGS support; metadata target for HealpixGrid, H3Grid, S2Grid outputs.
- [`odc-geo`](https://github.com/opendatacube/odc-geo) — `GeoBox` abstraction, `xr_reproject`. Consumption target for `RectilinearGrid`.

### Broader ecosystem context
- [Modular-libraries proposal by Sean Harkins (public gist)](https://gist.github.com/sharkinsspatial/f1c3a8f871b58416fa30c377178b5f9c) — separates I/O, indexing, and resampling into composable Rust-based libraries. One concrete instance of the modularity push this design doc aligns with.
- [Cloud-native resampling and reprojection](https://developmentseed.org/warp-resample-profiling/) — broader landscape; Ecosystem & Roadmap page explains zagg's Layer 4b position in the overall resampling taxonomy. Currently benchmarks F1-on-continuous workloads; roadmap includes F3 conservation and F4 point-to-cell additions that would cover zagg-class workloads.
- [Pangeo discourse thread on lazy reprojection (Sep 2024)](https://discourse.pangeo.io/t/can-a-reprojection-change-of-crs-operation-be-done-lazily-using-rioxarray/4468) — established `odc-geo.xr_reproject` as the community consensus for lazy CRS warp; motivates `RectilinearGrid`'s `odc.geo.GeoBox` acceptance.

### Standards
- [xdggs spec](https://github.com/xarray-contrib/xdggs/blob/main/docs/specifications.md) — DGGS Zarr metadata.
- [GeoZarr](https://github.com/zarr-developers/geozarr-spec) — rectilinear/CF Zarr metadata. Target for `RectilinearGrid.emit_template`.
- CF Conventions — baseline for coordinate/axis metadata.

---

*Status*: design doc at first-draft complete. Ready for review + comments on open questions in §11. Phase 0 extraction is the natural next concrete step once the shape is agreed.
