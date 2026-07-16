# Sharded output (zarr ShardingCodec)

zagg can bundle a dispatch shard's inner read-chunks into **one zarr
`ShardingCodec` shard object** instead of writing them as many independent
regular chunk objects. This decouples the **write/dispatch granularity** (the
unit a Lambda worker processes) from the **read granularity** (the small chunk a
reader partial-decodes), without drowning the reader in millions of tiny objects
at global/dense scale.

It builds directly on `chunk_inner` — it changes only the *storage form* of the
inner chunks, not the grid geometry. For **HEALPix** grids it is the **default**
whenever `chunk_inner` gives `K > 1`, on both store layouts (issue #215 flat,
issue #236 hive); an explicit `sharded: false` opts out. For **rectilinear**
grids it stays opt-in.

## How it relates to `chunk_inner`

`chunk_inner` sets the geometry: the **shard** is the dispatch unit (HEALPix
`parent_order`; rectilinear `chunk_shape`), the **inner chunk** is the smaller
read chunk (HEALPix `chunk_inner` order, e.g. 13 = a 64×64 = 4096-cell chunk;
rectilinear `chunk_inner` = `[inner_h, inner_w]`), and one shard owns
`K = (shard cells) / (inner-chunk cells)` inner chunks.

`sharded` only picks how those `K` inner chunks are stored:

| | `sharded: false` | `sharded: true` (HEALPix default at K > 1) |
|---|---|---|
| storage | `K` regular chunk objects per shard | **1** shard object per shard |
| object count | high (empties absent) | low (~`K`× fewer) |
| sub-shard sparsity | absent objects | shard-index entries inside the object |
| reader 64×64 access | one object per chunk | byte-range partial decode within the shard |

With `zarr-shard == dispatch-shard`, each worker writes exactly one whole shard
object, alone — the canonical single-writer pattern: no read-modify-write, no
cross-worker contention. Empty inner chunks (e.g. a track corridor crossing only
part of a shard) are omitted from the shard index, so sub-shard sparsity is
preserved *inside* the object.

## Config

The knob lives on the grid/chunk block, next to `chunk_inner`:

```yaml
# HEALPix
output:
  grid:
    type: healpix
    parent_order: 8      # dispatch shard
    child_order: 19      # leaf cell order
    chunk_inner: 13      # inner read chunk (64×64 = 4096 cells) -> K = 4^(13-8) = 1024
    sharded: true
```

```yaml
# Rectilinear
output:
  grid:
    type: rectilinear
    crs: EPSG:3031
    resolution: 5000
    bounds: [-3200000, -3200000, 3200000, 3200000]
    chunk_shape: [256, 256]   # dispatch shard tile
    chunk_inner: [64, 64]     # inner read chunk -> K = (256/64)^2 = 16
    sharded: true
```

`sharded` only does something when `chunk_inner` makes `K > 1` (a shard with
more than one inner chunk). At `K == 1` there is nothing to bundle:

- **HEALPix** silently no-ops it (issue #215) — since `sharded` is the default,
  a single-chunk grid must not fail at construction. An **explicit**
  `sharded: true` at `K == 1` likewise validates and no-ops: the output is
  byte-identical to the unsharded write (one chunk == one object either way).
- **Rectilinear** (where `sharded` is opt-in) **rejects** `K == 1` at grid
  construction with a clear message — an explicit flag that cannot do anything
  is a config mistake there, not a default to tolerate.

## Storage layout

- The dense per-cell arrays (`<group>/<varname>`) carry a `sharding_indexed`
  codec: the **outer** chunk is the whole shard, the **inner** chunk is the read
  chunk. Inner codecs stay **bytes-only/uncompressed** (zagg's policy), so the
  on-disk bytes for a populated inner chunk are identical to the regular path.
- `resolution: chunk` companion arrays and (HEALPix) the 1-D coordinate / (rect)
  the 1-D `x`/`y` arrays are **not** sharded — they keep their regular layout.
- The worker writes the whole shard in **one** `set_block_selection` per dense
  array (block selection is shard-granular on a sharded array), so a single shard
  object is produced per dispatch shard.
- Under the **hive** store layout (issue #236) the same applies inside each
  leaf: every dense per-cell array (and each ragged field's vlen array, issue
  #209) is ONE `ShardingCodec` object spanning the whole leaf, written at leaf
  block 0 — hive output is byte-identical to the flat sharded shard region.
  `grid.shard_order` (the issue #133 object split, a flat-path memory bound) is
  rejected with hive: a leaf's arrays are one whole-leaf object each by
  construction.

## Reader note

This is currently a **writer-side** feature plus offline round-trip read-back.
Consuming sharded stores from the higher-level read helpers (shard-index /
byte-range reads instead of `list_prefix` enumeration) is tracked as a
follow-up; the underlying zarr partial-decode of a 64×64 chunk within a shard
already works through the standard zarr array API.
