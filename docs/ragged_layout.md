# Ragged store layout (zagg-ragged/1)

A `kind: ragged` field (a per-cell t-digest, a per-cell photon list — anything
whose per-cell length varies) is stored as **one self-describing
`variable_length_bytes` array** on the cell grid. This is the
[issue #209](https://github.com/englacial/zagg/issues/209) layout; it replaced
the per-inner-chunk CSR subgroups (`values`/`offsets`/`cell_ids`, ~7 objects per
populated inner chunk) with a single vlen array per field. The convention is
versioned `zagg-ragged/1` and stamped into the array's attrs.

The design goals it meets: **one object per shard** on the write side (deleting
the ~K×7 tiny-PUT storm and the [issue #142](https://github.com/englacial/zagg/issues/142)
write-fanout thread pool that existed only to parallelize it), **2-GET random
access** to any single cell on the read side, and a wire format frozen tightly
enough that the [issue #210](https://github.com/englacial/zagg/issues/210)
typed-dtype migration is metadata-only.

## Layout

A ragged field `{field}` under a product group is up to three sibling arrays:

```
{group}/{field}             <- vlen array; populated cell i holds the raw
                               little-endian bytes of its (n, *inner_shape) payload
{group}/{field}_locations   <- LOCATED fields only (issue #87): the per-row uint64
                               location words, row-aligned with {field}
{group}/morton              <- per-cell uint64 morton coordinate (zagg's standard
                               HEALPix coordinate array; the chunk-identity source)
```

Each populated cell's value is the raw little-endian bytes of an
`(n, *inner_shape)` array (`n` varies per cell — e.g. `(k_centroids, 2)` for a
t-digest whose `inner_shape` is `(2,)`). Empty cells keep the `b""` fill, and an
inner chunk with no ragged data is omitted from disk entirely — the same
sub-shard sparsity the dense arrays get.

The array's codec chain is `[vlen-bytes, zstd(level=3)]`. The zstd deviates from
the dense arrays' bytes-only/uncompressed policy deliberately: a vlen payload has
no fixed-width raw layout to preserve, and level 3 matches the coverage-sidecar
precedent (`zagg.hive._ZSTD_LEVEL`), fixed so identical payloads produce
identical objects across workers.

## The attrs contract

The element interpretation is **self-describing** in the array's attrs under the
`ragged` key (`grids.base.RAGGED_ELEMENT_ATTR`), so a reader decodes exactly what
the writer declared rather than hardcoding a dtype:

```json
"ragged": {
  "spec": "zagg-ragged/1",
  "element": {"dtype": "float32", "shape": [-1, 2]},
  "locations": "h_tdigest_locations"
}
```

- **`spec`** (`grids.base.RAGGED_SPEC`) — the convention version. Readers
  strict-check it (`readers/tdigest_tensor._open_ragged`): an unknown/future spec
  **raises**, never half-parses. This is the coverage-envelope discipline applied
  to the ragged layout.
- **`element`** — `{"dtype": "<numpy dtype>", "shape": [-1, *inner_shape]}`. The
  `-1` marks the per-cell varying count, so a reader reconstructs cell `i` as
  `np.frombuffer(a[i], dtype).reshape(-1, *inner_shape)`. The bytes are always
  little-endian, independent of the producing machine.
- **`locations`** — present only on a LOCATED field's payload array (issue #87);
  its value is the name of the sibling uint64 array carrying the per-row location
  words. A reader binds the location channel **by this declaration**, never by
  reconstructing the `{field}_locations` naming convention. An unlocated field
  records nothing here.

A vlen array without a well-formed `element` declaration is **not** a zagg ragged
array — pre-issue-209 CSR stores are a hard break, and the readers raise a
pointed error rather than decode under a guessed layout.

## Wire framing (golden-pinned)

Within one inner chunk the `variable_length_bytes` codec frames the cells before
compression as (little-endian throughout):

```
u32  cell_count
per cell:  u32 payload_length  ||  payload_bytes
```

i.e. numcodecs' `VLenBytes`/`VLenArray` framing — a `u32` count of cells, then
for each cell a `u32` byte length followed by that many payload bytes (`0` for an
empty cell). The `payload_bytes` are `np.ascontiguousarray(value).tobytes()` of
the cell's `(n, *inner_shape)` array in the declared dtype.

This exact byte vector is frozen by a golden test
(`tests/test_processing.py::TestRaggedVlenLayout::test_golden_inner_chunk_framing`):
round-trip tests pass under any self-consistent encoding, so only a fixed byte
vector pins the convention. It is what guarantees the later metadata-only
migration to a typed `vlen-array<float32>` dtype (byte-compatible with numcodecs
`VLenArray`) without rewriting data — see the
[migration note](#issue-210-typed-dtype-migration).

## Sharded vs per-inner-chunk layouts

Both layouts hold the same logical data and are self-describing in the array's
own metadata (its `chunk_grid` and whether it has a `shards` outer chunk), so a
single reader code path reads either. Which one a product gets depends on the
write path (`grids.base.ragged_array_spec`, `shard_shape` argument):

| layout | when | on disk | single-cell read |
|---|---|---|---|
| **sharded** (`ShardingCodec`) | sharded flat path (`write_shard_to_zarr`) and every hive leaf (`write_ragged_leaf_to_zarr`) | ONE object per shard; the shard's K inner chunks live inside it with an internal index | 2 GETs (index suffix + one ranged inner chunk) |
| **per-inner-chunk** (regular array) | UNSHARDED per-chunk write (`write_ragged_to_zarr`, the runner / Lambda streaming callback) | one object per inner chunk | 1 GET (the object) |

The GET counts are for the data objects only and exclude the one-time array-open
metadata read (amortized across all cells of a store). The sharded 2-GET count is
pinned by `test_two_ranged_gets_on_sharded_store`; the unsharded 1-GET count is
analytic (a regular array indexes the single chunk holding the cell).

**Why the unsharded flat K>1 path keeps per-inner-chunk objects** (the review's
Q1 resolution): the streaming write path writes each chunk independently as it is
produced, then frees it — the [issue #91](https://github.com/englacial/zagg/issues/91)
stream-and-free bound. Folding all K chunks into one sharded object would force a
read-modify-write of that shared object on every chunk, defeating stream-and-free
and re-introducing the memory the sharded slab pass is careful to bound. So the
regular-chunked (one object per inner chunk) layout is retained there. It is
**not** a reader fork: the reader derives the stored span from `arr.shards or
arr.chunks` and reads either identically (pinned by
`test_sharded_and_regular_layouts_read_identically` — same logical data through
both writers yields identical tensors and chunk ids).

Empty inner chunks are omitted from the sharded object's index (the `2^64-1`
sentinel in the shard footer of K `(offset, nbytes)` u64 pairs + crc32c), so
object size scales with **populated** chunks only.

## The hive-leaf reader contract

A [hive](hive_layout.md) leaf zarr is exactly this layout scoped to one shard.
Under the leaf's `{group}` path a reader finds:

- the ragged vlen array `{group}/{field}` with its versioned `ragged` attrs,
- the sibling `morton` coordinate array (chunk identity),
- and, for a located field, the `{field}_locations` sibling —

all sharded as one whole-leaf `ShardingCodec` object (one stored span). So a hive
product is **read one leaf at a time**: open the leaf store
(`hive.shard_leaf_path`) and pass the same `field` path to the readers. The
readers are store-scoped and never traverse the hive digit tree — leaf discovery
is the coverage MOC's / walker's job ([issue #200](https://github.com/englacial/zagg/issues/200)).
The flat-sharded and hive-leaf writers are pinned to store byte-identical
per-cell payloads (`test_hive_leaf_parity_with_flat_sharded`) so the two backends
cannot drift.

## Read paths

`zagg.readers.tdigest_tensor` consumes the layout two ways:

- **Whole-store sweep** (`read_tensors`, `read_raw_values`, `read_locations`) —
  one LIST of the array's stored `c/<ordinal>` objects (`_stored_chunk_spans`),
  then a per-read-chunk decode. The sweep visits only written data; each stored
  object is read in one slice (a sharded object's index suffix is fetched once,
  not re-fetched per inner chunk). Each read chunk is a square `(side, side)`
  block of cells (`side = isqrt(cells_per_chunk)`, 64 for the production
  `chunk_inner` configs), and its coverage-cell morton id is derived from the
  sibling `morton` coordinate coarsened to the chunk order.
- **Single-cell random access** (`read_cell`) — indexes the vlen array directly.
  On a sharded store that is exactly **2 ranged GETs** (the shard-index suffix,
  then the one inner chunk holding the cell), never the whole shard object. An
  out-of-range index raises `IndexError` naming the valid range (no negative-index
  wrap); an absent cell returns the zero-length `(0, *inner_shape)` array. Works
  on the `{field}_locations` sibling too.

## Issue #210 typed-dtype migration

The `ragged` attrs block is the **interim** element contract. The
[issue #210](https://github.com/englacial/zagg/issues/210) migration moves the
element declaration (dtype + inner shape) into the zarr data type itself — a
typed `vlen-array<T>` dtype — and supersedes (bumps or removes) the `spec`
marker. Because the on-wire cell framing is already byte-compatible with
numcodecs `VLenArray` and frozen by the golden test above, that migration is
**metadata-only**: the stored chunk bytes do not change, only the array's
declared data type and the `spec` gate. Existing `zagg-ragged/1` data stays
readable; a reader that predates the bump fails loudly on the new `spec` rather
than mis-decoding.
