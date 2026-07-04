# Typed morton boundary

zagg's `morton` output coordinate is a real datatype, not an anonymous integer
column. mortie defines the `morton_index` type once, and zagg carries it typed
across every surface it crosses (issue
[#135](https://github.com/englacial/zagg/issues/135)):

| Surface | Carrier | Form |
| --- | --- | --- |
| pandas carrier (worker) | `pandas.DataFrame` | `mortie.MortonIndexArray` extension array (#71) |
| arro3 carrier (worker) | `arro3.core.Table` | `morton_index` Arrow **extension type** over the PyCapsule C Data Interface (mortie ≥ 0.8.4) |
| catalog / shardmap parquet | `pyarrow.Table` | `morton_index` pyarrow extension type (registered by mortie on import) |
| Zarr store (on disk) | Zarr v3 array | plain `uint64` — the packed words, byte-for-byte |

## Who owns what

**mortie owns the interchange type; zagg stores `uint64`.** The extension
metadata (`ARROW:extension:name == "mortie.morton_index"`) lives at the
interchange layer only. The write boundary
(`zagg.processing.write._iter_carrier_columns`) extracts the packed `uint64`
words from a typed column — on either carrier — so the on-disk output is
identical to a plain-`uint64` write. Reads reconstruct the typed array via
`zagg.grids.morton.to_morton_array`.

The adapter lives in `zagg.grids.morton`:

- `morton_to_arrow(values)` — `MortonIndexArray` (or raw words) →
  `arro3.core.Array` carrying the extension type, zero-copy via
  `__arrow_c_array__`;
- `morton_from_arrow(col)` — any Arrow C-Data source (array, chunked column, or
  capsule pair) → `MortonIndexArray`;
- `is_morton_arrow(col)` — detects the extension type from field metadata.

No pyarrow is needed on any worker path: the arro3 carrier and mortie's
PyCapsule surface speak the Arrow C Data Interface directly (pyarrow stays in
the off-Lambda `catalog` extra). Arrow nulls map to mortie's all-zero empty
sentinel word in both directions, so missing values round-trip.

## `cell_ids` encoding

`cell_ids` stays the standardized **NESTED HEALPix** id (`uint64`) by default.
For test and prototyping flows, `output.grid.cell_ids_encoding: morton` emits
the packed morton words as `cell_ids` instead (HEALPix grids only; the store's
`dggs.indexing_scheme` attribute records the active encoding):

```yaml
output:
  grid:
    type: healpix
    parent_order: 6
    child_order: 12
    cell_ids_encoding: morton   # default: nested
```

The default (key absent or `nested`) is byte-identical to a pre-flag run.

## Shardmap parquet manifests

`ShardMap.to_parquet` / `ShardMap.from_parquet` are the Arrow-native siblings
of the JSON manifest: `shard_keys` is a typed `morton_index` column, granule
and AOI payloads ride as per-shard JSON strings, and provenance lives in the
schema metadata (mirroring the `Catalog` geoparquet convention). This path
requires pyarrow (`pip install zagg[catalog]`) and runs off-worker.
