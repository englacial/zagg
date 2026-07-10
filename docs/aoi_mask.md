# Strict-AOI cell mask

zagg's shard universe is every `parent_order` cell that *overlaps* the area of
interest (AOI), and each worker then aggregates every child cell of its shard
with no final clip to the AOI polygon. So the collected region overhangs the AOI
by roughly one shard-cell all around, and that overhang scales with
`parent_order` — the same AOI reports a different observation count at different
shard orders (issue #100).

## The shard is the unit of work, not the AOI

This has a consequence worth stating plainly, because it surprises people reading
granule counts: **a shard map built for an AOI intentionally reads more granules
than a query against that AOI would return.** A granule is assigned to — and read
for — a shard whenever its footprint touches that shard, *even if the granule
never crosses the AOI polygon itself*. The shard is the unit of work: once a shard
overlaps the AOI, zagg processes the whole shard, and that means reading every
granule whose data lands anywhere in it.

Concretely, over the NEON SERC box the three counts differ and all three are
"right" for different questions:

| count | question it answers |
| --- | --- |
| **59** | granules a CMR bounding-box query returns for the AOI |
| **69** | granules whose footprint sphere-correctly intersects the AOI polygon |
| **99** | granules the o9 shard map reads — everything in the shards that cover the AOI |

The **99 is the correct processing count.** Those are the granules whose
observations fall in the shards being aggregated, so reading them is required, not
wasted — they populate the overhang cells that the covering shards legitimately
include. The 59/69 figures describe the AOI itself, which is not the processing
unit. (The gap grows with coarser `parent_order`, since a coarser shard reaches
further past the AOI; it shrinks toward the AOI count as the shard order
approaches the AOI's own scale.)

If you need the *output* clipped back to the strict AOI, use `output.aoi_mask`
(below) — but note that only masks the written cells; the granules are still read
either way, because the shards are still processed in full.

The **strict-AOI cell mask** (`output.aoi_mask`, default off) packages an
optional per-cell boolean aligned to the output cell grid — `True` where the
cell falls inside the AOI — so a client can recover the order-independent
strict-AOI subset. It is **"package, don't clip"**: no observation is dropped,
and a run with the flag off is byte-identical to one without the feature.

## Config

```yaml
output:
  store: s3://bucket/atl06.zarr
  aoi_mask: true        # default false
  grid:
    type: healpix
    parent_order: 11
    child_order: 13
```

When `aoi_mask` is off (the default), nothing is computed, declared, or written.
When on, the store gains one extra array:

- **HEALPix** — `bool` array `aoi_mask` at `<child_order>/aoi_mask`, aligned to
  the `cells` dimension (same shape and chunking as the data variables).
- **Rectilinear** — `bool` array `aoi_mask` at `rectilinear/aoi_mask`, aligned to
  the `(y, x)` cell grid.

`fill_value` is `False`, so any cell the run never writes (an out-of-AOI shard,
or a cell with no data) reads as not-in-AOI.

## How it is computed

The mask is computed once at the **shard-map build stage** — it depends only on
the grid geometry and the AOI, never on the observations — and carried per shard
in the shard-map JSON (`aoi_mask`, parallel to `shard_keys`). Each worker expands
its shard's payload to a per-cell boolean over the cells it already enumerates
and writes it alongside the data columns.

- **HEALPix** uses native morton, no lat/lon-center decode: a compact
  multi-order coverage (MOC) of the AOI at `child_order`
  (`morton_coverage_moc`), intersected per shard (`moc_and`) and expanded to the
  cell order (`moc_to_order`) for membership against the shard's `children()`.
  This requires `mortie >= 0.8.3` (the order-29 MOC coverage cap plus the public
  WKB/WKT cover entry points, below); the mask code asserts the resolved version at
  use.
- **Rectilinear** reprojects the AOI polygon to the grid CRS (the same `to_crs`
  reprojection `coverage` uses) and tests each cell center with a
  prepared-geometry shapely `contains`. The WGS84 ring is **densified** before
  reprojection (odc.geo `to_crs` resolution densification — the same mechanism
  `shard_footprint` uses, here with `resolution="auto"`), so the AOI edges follow
  the geodesic rather than collapsing to straight chords in a polar / large-extent
  CRS — edge-cell
  membership no longer drifts by the chord-vs-arc deviation. This is rect-only:
  the HEALPix path tessellates the native `(lats, lons)` ring on the sphere and
  never reprojects a polygon.

## Supplying the AOI: rings or WKB/WKT geometry

The AOI polygon can be supplied two ways, and both produce the **identical** mask:

- the original `[(lats, lons), ...]` exterior-ring parts (e.g. from
  `zagg.catalog.load_polygon` on a GeoJSON), or
- a native geometry as **WKB** bytes or **WKT** text (`mortie >= 0.8.3`).

WKB/WKT is wired through `ShardMap.build(..., aoi=...)` and
`make_shardmap(..., aoi=...)`, and from the CLI via `--aoi-wkt` /
`--aoi-wkb`:

```bash
python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --bbox -180,-90,180,-60 \
    --aoi-wkt antarctic.wkt          # strict-AOI polygon for output.aoi_mask
```

```python
from zagg.catalog import make_shardmap
sm = make_shardmap(query, grid, region=parts, aoi=wkb_bytes)   # or aoi=wkt_str
```

On the HEALPix engine the WKB/WKT geometry rides mortie's public `from_wkb` /
`from_wkt` cover entry points (espg/mortie#89) with `moc=True`, which decompose the
geometry and route Polygon/MultiPolygon to the same `morton_coverage_moc` used for
ring input — so the compact MOC, and therefore the mask, is bit-for-bit the same as
the equivalent ring. On the rectilinear engine the shapely-loaded geometry's
exterior rings reproject through the same densify + `to_crs` path as a ring AOI.

A couple of points worth knowing:

- **The AOI can differ from the coverage region.** `region` (or the bbox) drives
  the *shard universe* (`grid.coverage`), while `aoi` drives the *strict-AOI mask*.
  They default to the same polygon (`aoi=None` reuses `region`), but you may pass a
  tighter `aoi` than the coverage region — that is exactly the #100 overhang
  premise (coverage ⊇ strict AOI). The mask is always computed against `aoi`.
- **Holes are honored on HEALPix, not on rectilinear.** mortie's `from_geometry`
  subtracts interior rings (holes) natively, so a WKB/WKT polygon-with-holes masks
  the holes out on the HEALPix engine. The rectilinear engine is exterior-ring
  strict for *both* ring and WKB/WKT input (it has always reprojected exteriors
  only), so a hole is **not** subtracted there — the same engine-family asymmetry
  documented just below.

## Boundary inclusivity differs by grid family

The two engines settle a cell that *straddles* the AOI edge differently, and the
difference is **intentional and kept as-is** — each rule is the natural one for
its engine. A client filtering on `aoi_mask` should expect slightly different
edge semantics across grid families:

- **HEALPix is overlap/coverage-based (inclusive at the boundary).**
  `morton_coverage_moc` builds a MOC that *covers* the AOI, so a leaf cell whose
  area overlaps the AOI — even partially, including one whose center lies just
  outside — is in the MOC and marked `True`. The MOC is a superset cover, so the
  HEALPix mask leans **inclusive**: it never drops a cell that touches the AOI.

- **Rectilinear is a strict center test (exclusive at the boundary).** Each cell
  is marked `True` only if its **center** falls inside the reprojected AOI polygon
  (`prepared.contains(center_point)`). `contains` is strict — a center lying
  *exactly on* the boundary returns `False` — so a cell straddling the edge whose
  center is outside (or on) the polygon is **excluded**. This matches the
  "keep-whole-cell-if-its-center-is-in" rule.

So for the same AOI edge, the HEALPix mask tends to **include** an edge-overlapping
cell while the rectilinear mask **excludes** one whose center is outside. Both are
defensible in isolation (the HEALPix MOC is the native, decode-free primitive; the
rect center test matches how rect cells are addressed), and the asymmetry is at
most one cell-width at the boundary. Keep this in mind when comparing strict-AOI
counts across a HEALPix and a rectilinear run of the same region.

## Reading the mask

The mask is plain metadata; filter the store to the strict AOI by selecting
`aoi_mask == True`:

```python
import xarray as xr

ds = xr.open_zarr("s3://bucket/atl06.zarr/13")   # HEALPix child_order=13 group
strict = ds.where(ds["aoi_mask"])                # NaN out the overhang cells
n_in_aoi = int(ds["aoi_mask"].sum())
```

For a rectilinear store the same pattern applies on the `(y, x)` grid:

```python
ds = xr.open_zarr("s3://bucket/grid.zarr/rectilinear")
strict = ds.where(ds["aoi_mask"])
```

## Notes and limits

- **Both backends.** The mask is written end-to-end by the local runner **and**
  the AWS Lambda backend. The orchestrator threads each shard's payload through
  the Lambda event (`aoi_payload`) and `deployment/aws/lambda_handler.py` forwards
  it to `process_shard`, mirroring the local wiring. When the flag is off the
  event omits the key, so a flag-off Lambda run is byte-identical.
- **WKB/WKT AOI input** is supported (see *Supplying the AOI* above): pass `aoi=`
  WKB bytes / WKT text to `ShardMap.build` / `make_shardmap`, or `--aoi-wkt` /
  `--aoi-wkb` on the CLI. It rides mortie's public WKB/WKT cover entry points
  (espg/mortie#89, `mortie >= 0.8.3`) and yields the identical mask to the
  equivalent `(lats, lons)` ring.

See the runnable, data-free example in
[`notebooks/aoi_mask.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/aoi_mask.ipynb),
which builds a small HEALPix grid + AOI box and shows the mask is `True` exactly
for the in-AOI cells. The notebook is self-contained (no remote data) and runs
anywhere `zagg` (with `mortie>=0.8.3`) is installed. Binder launch additionally
needs the repo-wide `.binder/` environment, which lands separately via #105; until
then the Binder badge won't resolve `zagg` + `mortie` on a default build.
