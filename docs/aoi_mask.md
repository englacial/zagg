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

A cell is in-AOI by its **center** (HEALPix: the MOC cell membership; rectilinear:
the explicit center-in-polygon test), matching the cells the store already
addresses. Boundary inclusivity differs slightly between engines — the HEALPix
MOC keeps boundary-overlapping cells, while the rectilinear `contains` test is
strict on the cell center — so a cell straddling the AOI edge may be marked
differently across grid families.

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

- **Local backend.** The mask is written end-to-end by the local runner. The AWS
  Lambda worker path additionally needs the per-shard payload threaded through
  the Lambda event/handler; that wiring is a follow-up (the handler lives in the
  deployment infra).
- **WKB/WKT AOI input** (passing a native geometry rather than `(lats, lons)`
  rings) is a deferred future phase, pending upstream mortie support
  (espg/mortie#71).

See the runnable, data-free example in
[`notebooks/aoi_mask.ipynb`](https://github.com/englacial/zagg/blob/main/notebooks/aoi_mask.ipynb),
which builds a small HEALPix grid + AOI box and shows the mask is `True` exactly
for the in-AOI cells. The notebook is self-contained (no remote data) and runs
anywhere `zagg` (with `mortie>=0.8.2`) is installed. Binder launch additionally
needs the repo-wide `.binder/` environment, which lands separately via #105; until
then the Binder badge won't resolve `zagg` + `mortie` on a default build.
