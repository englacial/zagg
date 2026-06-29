# Strict-AOI cell mask

zagg's shard universe is every `parent_order` cell that *overlaps* the area of
interest (AOI), and each worker then aggregates every child cell of its shard
with no final clip to the AOI polygon. So the collected region overhangs the AOI
by roughly one shard-cell all around, and that overhang scales with
`parent_order` — the same AOI reports a different observation count at different
shard orders (issue #100).

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
  This requires `mortie >= 0.8.2` (the order-29 MOC coverage cap); the mask code
  asserts the resolved version at use.
- **Rectilinear** reprojects the AOI polygon to the grid CRS (the same `to_crs`
  reprojection `coverage` uses) and tests each cell center with a
  prepared-geometry shapely `contains`.

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
- **WKB/WKT AOI input** (passing a native geometry rather than `(lats, lons)`
  rings) is **deferred**, blocked on upstream mortie geometry I/O
  (espg/mortie#89): the pinned `mortie>=0.8.2` has no WKB/WKT entry point, so the
  AOI is still supplied as `(lats, lons)` rings. It will be rolled in once that
  upstream PR lands.

See the runnable, data-free example in
[`notebooks/aoi_mask.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/aoi_mask.ipynb),
which builds a small HEALPix grid + AOI box and shows the mask is `True` exactly
for the in-AOI cells. The notebook is self-contained (no remote data) and runs
anywhere `zagg` (with `mortie>=0.8.2`) is installed. Binder launch additionally
needs the repo-wide `.binder/` environment, which lands separately via #105; until
then the Binder badge won't resolve `zagg` + `mortie` on a default build.
