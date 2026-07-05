# Virtual chunk-index backends

The read path resolves a **virtual index** per run — a backend that answers,
per (granule, shard, config), exactly two questions: which chunks / element
ranges of the configured datasets intersect this shard (**selection**), and
what column arrays result from fetching + decoding them (**addressing +
decode**). Everything else — data-dependent filters like `signal_conf_ph`,
expression filters, aggregation — stays downstream and backend-invariant, so
any backend that returns the same columns produces byte-identical output
(issue #160).

The default is today's hierarchical read; a config without an `index` block
is untouched by this feature.

## Config

```yaml
data_source:
  # index: absent            → hierarchical: today's path, zero change
  index:
    backend: inline           # compute the chunk map at read time
    write_back: true          # optional (default false): persist manifests
    store: s3://sliderule-public-cors/zagg-index/ATL03/007/   # required with write_back
```

Keys other than `backend` are **backend-specific**: a key the named backend
does not accept (e.g. `store` under `hierarchical`, or `on_miss` under
`inline`) is a config **error**, not ignored.

## Backends

| backend | ships in | mechanism |
|---|---|---|
| `hierarchical` (default) | zagg | coarse geolocation read + `plan_read` + h5coro hyperslices — the pre-existing path behind the protocol seam |
| `inline` | zagg | builds each dataset's chunk map at read time (pure-Python B-tree walk, metadata-only) and issues boundary-safe planned reads; optional `write_back` persists granule manifests to the store |
| `sidecar` | external (`h5coro-hidefix`) | precomputed granule-keyed sidecar manifests fetched from the store; discovered via the entry-point group below |

`inline` never *reads* the store — it recomputes every granule every run. It
is the no-store-yet mode and (with `write_back: true`) the store-*population*
mode; consuming a populated store is the `sidecar` backend's job.

### Requirements for `inline`

Selection still comes from the coarse spatial index, so `inline` requires the
hierarchical read surface: `data_source.read_plan.spatial_index` plus
`levels`/`base_level` (see the shipped `atl03.yaml`). Planned reads are
issued as-is — h5coro already inflates exactly the covering chunks — so
output is row-identical to `hierarchical`, with one exception the chunk map
makes detectable: a read that starts exactly on an interior chunk boundary
(which h5coro's B-tree start-edge intersection drops entirely) is shifted
one element early and trimmed, so `inline` survives shards the plain
hyperslice read fails on.

## Write-back manifests

With `write_back: true`, after a granule's last group is read the accumulated
chunk maps are written to `<store>/<granule_id>.parquet` (the granule id is
the URL basename without extension, so it carries product + version and
reprocessing changes the key). `store` may be a local directory or an
`s3://bucket/prefix` URI; S3 writes use ambient credentials (the execution
role), never the granule-read credentials. A failed write is logged and the
read continues.

Coverage per visited group is deterministic — every dataset the config can
touch (base-rate coordinates, variables, and filter datasets, plus the
spatial-index level's coordinate and link arrays), built metadata-only up
front, so a group that contributes no rows to this shard (or degrades to a
full read) is still fully covered and concurrent shards of one granule write
identical manifests. One row per HDF5 chunk:

| column | meaning |
|---|---|
| `dataset` | full HDF5 path |
| `chunk_idx` | row-major linear index over the chunk grid |
| `elem_start`, `elem_end` | half-open element range along the first axis |
| `byte_offset`, `nbytes` | stored (compressed) chunk extent in the file |
| `filter_mask` | HDF5 per-chunk filter mask (0 = all filters applied) |
| `chunk_offset` | per-dim dataspace offset, JSON list |
| `dtype` | byte-order-explicit numpy dtype string (`np.dtype(...).str`, e.g. `<f4`, `|i1`) |
| `shape`, `chunk_shape` | per-dataset dims, JSON lists |
| `gzip`, `shuffle` | filter-pipeline flags (booleans by contract — deflate level is irrelevant for decode) |

Contiguous (unchunked) datasets appear as a single pseudo-chunk. This is the
schema a `sidecar` consumer reconstructs its decode index from.

## Registering an external backend

Backends resolve by name: zagg's built-ins live in a static dict merged with
the `zagg.index_backends` entry-point group (the same pattern as h5coro
registering itself as an xarray engine — zagg core never imports an external
backend's dependencies). Each entry point resolves directly to a
`zagg.index.VirtualIndex` subclass:

```toml
[project.entry-points."zagg.index_backends"]
sidecar = "my_package.backend:SidecarIndex"
```

A subclass implements `read_group(h5obj, group, data_source, shard_key, grid,
arrow=False)` (the same contract as the worker's group read: a carrier or
`None`), may override the per-granule `finish_granule(h5obj, granule_url)`
hook, and declares its config keys via the `config_keys` /
`required_config_keys` class attributes plus an optional
`validate_index_config` hook. A builtin name cannot be shadowed, and a broken
entry point is logged and skipped.
