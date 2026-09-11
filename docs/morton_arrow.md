# Typed morton boundary

zagg's `morton` output coordinate is a real datatype, not an anonymous integer
column. mortie defines the `morton_index` type once, and zagg carries it typed
across every surface it crosses (issue
[#135](https://github.com/englacial/zagg/issues/135)):

| Surface | Carrier | Form |
| --- | --- | --- |
| pandas carrier (worker) | `pandas.DataFrame` | `mortie.MortonIndexArray` extension array (#71) |
| arro3 carrier (worker) | `arro3.core.Table` | `morton_index` Arrow **extension type** over the PyCapsule C Data Interface (added in mortie 0.8.4) |
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

## `cell_ids`: morton-only storage (D16)

Since the issue #304 flip, **`morton` is the only stored cell coordinate**:
the packed `uint64` words carry the cell identity (and its order,
intrinsically), the store's `dggs` attrs declare grid `name: "morton"` +
`coordinate: "morton"`, and NESTED HEALPix ids are **derived at read** (the
moczarr fabrication layer). The issue #135 `cell_ids_encoding` knob is
retired — a config still carrying it fails validation with a migration
pointer.

For transition stores (browser-direct demos until the gridlook morton decode
lands), the escape hatch keeps writing the legacy NESTED array *in addition
to* `morton`:

```yaml
output:
  grid:
    type: healpix
    parent_order: 6
    child_order: 12
    emit_cell_ids: true   # default: false — morton only
```

A hatch store's declared coordinate stays `morton`; `cell_ids` is an extra
legacy member. The grid's `spatial_signature()` (recorded in ShardMap
manifests) is unchanged by the hatch — deliberately, so shard maps remain
reusable — while the full `signature()` records `emit_cell_ids` (the hatch
changes the leaf schema, so mixed states never co-aggregate). (The
pre-existing `output.grid.indexing_scheme` config key is descriptive only and
must stay `nested`.)

## Native-only viewer/reader track (no xdggs)

Standing ruling (espg, 2026-09-10 — issue
[#550](https://github.com/englacial/zagg/issues/550)): the viewer/reader
track carries **no xdggs translation layer** — native code only. The viewer
path decodes native morton words directly, via mortie-core / healpix-geo (TS
codec + wasm — [espg/gridlook#8](https://github.com/espg/gridlook/issues/8)),
never through the xdggs library or its conventions; moczarr's gridlook feeder
surfaces stay native moczarr reads (`open_leaf` / `read_tensors` /
`read_ragged` — moczarr's `dggs.py` is excluded from the feeder path); and
the gridlook-fork hive shim's fabricated `dggs` attrs are a transitional
crutch to be retired by the phase 6 v2 native awareness, not an architecture.
zagg's multiscales metadata work
([#392](https://github.com/englacial/zagg/issues/392) /
[#394](https://github.com/englacial/zagg/issues/394)) is scoped to
zagg-native zarr metadata accordingly — xdggs is not a dependency or bridge
anywhere on this track. The self-describing `dggs` attrs block zagg stores
carry (D16, above) is unaffected: it is a zarr convention entry a native
reader may ignore, not an xdggs dependency.

## Shardmap parquet manifests

`ShardMap.to_parquet` / `ShardMap.from_parquet` are the Arrow-native siblings
of the JSON manifest: `shard_keys` is a typed `morton_index` column, granule
and AOI payloads ride as per-shard JSON strings, and provenance lives in the
schema metadata (mirroring the `Catalog` geoparquet convention). This path
requires pyarrow (`pip install zagg[catalog]`) and runs off-worker.
