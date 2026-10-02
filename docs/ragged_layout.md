# Ragged store layout (`zagg-ragged/1`)

A `kind: ragged` field (a per-cell t-digest, a per-cell photon list — anything
whose per-cell length varies) is stored as **one self-describing
`variable_length_bytes` array** on the cell grid. This is the
[issue #209](https://github.com/englacial/zagg/issues/209) layout; it replaced
the per-inner-chunk CSR subgroups (`values`/`offsets`/`cell_ids`, ~7 objects per
populated inner chunk) with a single vlen array per field.

> **Normative home.** The byte-level contract — the sibling-array layout, the
> versioned `ragged` attrs block, the codec chain, the golden wire framing,
> the ShardingCodec geometry, and the succession plan — is
> [`specification.md` §1](specification.md#1-zagg-ragged1), with the digest
> payload semantics in [§2](specification.md#2-digest-payload-semantics).
> This page is the narrative companion: the *why* behind the layout, and how
> zagg's own readers consume it. Where the two disagree, the spec wins.

The design goals the layout meets: **one object per shard** on the write side
(deleting the ~K×7 tiny-PUT storm and the
[issue #142](https://github.com/englacial/zagg/issues/142) write-fanout thread
pool that existed only to parallelize it), **2-GET random access** to any
single cell on the read side, and a wire format frozen tightly enough that the
[issue #210](https://github.com/englacial/zagg/issues/210) typed-dtype
migration is metadata-only.

## Why the attrs are the contract

The element interpretation (dtype, inner shape, the companion siblings' names)
is self-describing in the array's attrs — the
[spec §1.2 block](specification.md#12-the-ragged-attrs-block) — so a reader
decodes exactly what the writer declared rather than hardcoding a dtype or
reconstructing a naming convention (review finding, PR #211). The block is
versioned (`spec: "zagg-ragged/1"`) and strict-checked
(`readers/tdigest_tensor._open_ragged`): an unknown/future spec **raises**,
never half-parses — the coverage-envelope discipline applied to the ragged
layout. A vlen array without a well-formed `element` declaration is **not** a
zagg ragged array — pre-issue-209 CSR stores are a hard break, and the readers
raise a pointed error rather than decode under a guessed layout.

## Why the companion words are declared, not implied

A companion sibling holds **packed words**, not self-describing scalars — a
morton cell in `{field}_locations`, a toc time in `{field}_times` — so
nothing in a uint64 array says which grammar produced it. The spec's
word-typed declaration
([§8](specification.md#8-zagg-toc1), instantiated for space in
[§9](specification.md#9-zagg-located1)) closes that: a `{spec, shape,
grammar}` block, stamped by the writer on the array that *holds* the words,
with the payload array carrying only the *binding* to it. One rule follows
from that split and is worth stating plainly here: **read the declaration off
the sibling you bound, never off the payload.**

Two properties the family is designed around:

- **Absence is never a refusal.** Every located store written before §9
  carries no `located` key, and reads exactly as
  [§2.2](specification.md#22-the-location-channel) always said it did. The
  declaration adds self-description and the shape vocabulary; it never
  reinterprets a byte.
- **Composition gates on `{shape, grammar}`** — the same discipline the
  [§2.0 `weights`](specification.md#20-the-weights-declaration) declaration
  established, and for the same reason: merging two arrays whose words mean
  different things yields an array that means neither.

The binding for the temporal sibling is a **sibling attrs key** (`times`),
not a key inside the versioned `ragged` block, matching `weights` and for the
same reason — the block is retired wholesale under
[`zagg-ragged/2`](specification.md#6-zagg-ragged2), and a key outside it
survives that metadata-only migration untouched. `locations` predates that
ruling and stays where it is; the asymmetry is history, not design.

## Why the wire framing is golden-pinned

Round-trip tests pass under *any* self-consistent encoding, so only a fixed
byte vector freezes a convention. The
[spec §1.4 framing](specification.md#14-wire-framing) is pinned by a golden
test
(`tests/test_processing.py::TestRaggedVlenLayout::test_golden_inner_chunk_framing`)
and by the committed conformance fixtures
([spec §7](specification.md#7-conformance-fixtures)). That pin is what
guarantees the later metadata-only migration to a typed `vlen-ndarray` dtype
(byte-compatible with numcodecs `VLenArray`) without rewriting data — see
[spec §6](specification.md#6-zagg-ragged2).

## Why the unsharded flat K>1 path keeps per-inner-chunk objects

Both stored geometries ([spec §1.5](specification.md#15-storage-geometries))
hold the same logical data; which one a product gets depends on the write
path (`grids.base.ragged_array_spec`, `shard_shape` argument). The streaming
write path (`write_ragged_to_zarr`, the runner / Lambda streaming callback)
writes each chunk independently as it is produced, then frees it — the
[issue #91](https://github.com/englacial/zagg/issues/91) stream-and-free
bound. "Streaming" here is the **output-side** write discipline, which the
production runner uses on every path — including the default pooled-input
aggregation (the shard's reads are pooled once, then each chunk's output
carrier is built, written through the callback, and freed). It is distinct
from the buffered/streaming *input* path (issue #148 phase 4); input pooling
and output stream-and-free are orthogonal, and a pooled-input production run
still writes chunk-at-a-time through this path. Folding all K chunks into one sharded object would force a
read-modify-write of that shared object on every chunk, defeating
stream-and-free and re-introducing the memory the sharded slab pass is careful
to bound. So the regular-chunked (one object per inner chunk) layout is
retained there (the PR #211 review's Q1 resolution). It is **not** a reader
fork: the reader derives the stored span from `arr.shards or arr.chunks` and
reads either identically (pinned by
`test_sharded_and_regular_layouts_read_identically` — same logical data
through both writers yields identical tensors and chunk ids).

The GET counts in the spec's geometry table are for the data objects only and
exclude the one-time array-open metadata read (amortized across all cells of a
store). The sharded 2-GET count is pinned by
`test_two_ranged_gets_on_sharded_store`; the unsharded 1-GET count is analytic
(a regular array indexes the single chunk holding the cell).

## The hive-leaf reader contract

A [hive](hive_layout.md) leaf zarr is exactly this layout scoped to one shard.
Under the leaf's `{group}` path a reader finds the ragged vlen array with its
versioned `ragged` attrs, the sibling `morton` coordinate array (chunk
identity), and the field's companion siblings — `{field}_locations` for a
located field, `{field}_times` for one carrying the per-centroid temporal
companion ([spec §8.3](specification.md#83-shape-per-centroid)) — all
sharded as one whole-leaf `ShardingCodec` object (one stored span). So a hive
product is **read one leaf at a time**: open the leaf store
(`hive.shard_leaf_path`) and pass the same `field` path to the readers. A
**windowed** leaf stores no `morton` array (spec §1.5, issue #586): there the
readers derive the words from the leaf's own stamp, and take occupancy from
the payload array rather than from the coordinate's `0` fill. The
readers are store-scoped and never traverse the hive digit tree — leaf
discovery is the coverage MOC's / walker's job
([issue #200](https://github.com/englacial/zagg/issues/200)). The flat-sharded
and hive-leaf writers are pinned to store byte-identical per-cell payloads
(`test_hive_leaf_parity_with_flat_sharded`) so the two backends cannot drift.

## Read paths

`zagg.readers.tdigest_tensor` consumes the layout two ways:

- **Whole-store sweep** (`read_tensors`, `read_raw_values`, `read_locations`) —
  one LIST of the array's stored `c/<ordinal>` objects (`_stored_chunk_spans`),
  then a per-read-chunk decode. The sweep visits only written data; each stored
  object is read in one slice (a sharded object's index suffix is fetched once,
  not re-fetched per inner chunk). Each read chunk is a square `(side, side)`
  block of cells (`side = isqrt(cells_per_chunk)`, 64 for the production
  `chunk_inner` configs), and its coverage-cell morton id is derived from the
  sibling `morton` coordinate coarsened to the chunk order. Cell placement
  within the block is the spec-pinned bit deinterleave — see
  [the tensor section below](#spatially-faithful-tensors-deinterleave-blocks-mask).
- **Single-cell random access** (`read_cell`) — indexes the vlen array directly.
  On a sharded store that is exactly **2 ranged GETs** (the shard-index suffix,
  then the one inner chunk holding the cell), never the whole shard object. An
  out-of-range index raises `IndexError` naming the valid range (no negative-index
  wrap); an absent cell returns the zero-length `(0, *inner_shape)` array. Works
  on the `{field}_locations` and `{field}_times` siblings too — both are
  themselves `zagg-ragged/1` uint64 arrays.
- **Sub-leaf subtree read** (`subtree=` on the sweep readers —
  [issue #351](https://github.com/englacial/zagg/issues/351)) — the nested
  cells axis makes the subtree below any ancestor one **contiguous cell span**
  (normative in the spec's §1.5 subtree-spans clause), so `read_tensors`,
  `read_raw_values`, and `read_locations` take `subtree=` (a packed area word
  or its decimal string) and slice only that span: the shard-index suffix plus
  the covering inner chunks — `read_cell`'s 2-GET pattern generalized to a
  span, never the whole-array sweep. The per-cell readers accept any order
  down to a single cell; `read_tensors` emits whole read chunks only
  (`max(subtree_order, axis_root_order) ≤ block_order ≤ chunk_order` — the
  floor is the order of the span actually *visited*, i.e. the subtree clipped
  to the axis, so a word above the axis root takes the root's order) and
  refuses finer words, pointing at the per-cell readers. A well-formed word
  disjoint from the axis warns at most once per call and yields nothing — so an
  empty yield is ambiguous without the warning, and whether the warning is
  *delivered* is the caller's `warnings` filter's call (the default action
  dedups a repeat of the same warning in one process) — while malformed or
  too-deep words raise.

## Spatially faithful tensors (deinterleave, blocks, mask)

The nested cells axis traces a **Z-order curve** within each chunk subtree, so
a row-major reshape of the 1-D axis scrambles the 2-D block spatially beyond
2-cell runs. The readers instead place each cell at the **bit deinterleave**
of its chunk-local nested rank
([issue #336](https://github.com/englacial/zagg/issues/336)):
`mortie.rank_to_xy` / `xy_to_rank` (added in mortie 0.9.3), whose contract is
normative in mortie's `docs/specification.md` **§8** and frozen for mortie 1.x
— `x` gathers the rank's even bits, `y` its odd bits, origin `(0, 0)` at the
subtree's south corner, equal to healpy's face-local `pix2xyf` (nest)
convention. zagg pins **row = y, col = x** (`readers/_layout.py`), matching
gridlook's `bit_combine(j, i)` texture convention
(`gridlook/src/ui/grids/Healpix.vue`), so an emitted tensor uploads to a
gridlook texture with no further shuffle. The golden placement vectors in
`tests/test_reader_layout.py` are imported from mortie's spec-pinned test
suite, never re-derived.

`read_tensors` yields the full reader contract per block:

```python
for tensor, mask, (offset, gain), morton in read_tensors(store, field):
    ...  # tensor: (side, side, n_bins); mask: (side, side) uint8
```

- **Blocks** — by default one block per read chunk; `block_order=` assembles
  the `4**(chunk_order - block_order)` chunks of one block-order subtree into
  a single `(2**d, 2**d, n_bins)` tensor (`d = cell_order - block_order`; an
  order-12 block on production geometry is 128×128 from 4 inner chunks). The
  z-window and `fit` policy are reconciled **block-wide**: one shared
  `(offset, gain) = (z_lo, resolution)` per block, so bin `i` covers
  `[offset + i*gain, offset + (i+1)*gain)` for every cell in the block.
  Coarser than the stored shard assembles too (the block-local index is still
  the nested rank), but the memory bound is then per *block*: the block's
  decoded digests are all held for the block-wide window, and the emitted
  tensor grows 4× per coarser order (an order-6 block on the production
  order-19 geometry would be 34 TB). `max_block_bytes=` (2 GiB default)
  refuses that allocation with a pointed error instead of a bare
  `MemoryError`.
- **Mask** — the block's occupancy channel on the same deinterleaved layout:
  `0` unobserved, `1` observed with no stored digest, `2` observed with data.
  States 1/2 come from the hive leaf's `coverage.moc` occupancy sidecar
  ([issue #200](https://github.com/englacial/zagg/issues/200); decoded by the
  frozen `hive.decode_coverage_bitmap` convention, one small sidecar object,
  no digest bytes). A store without exact occupancy (every flat store, or a
  box-only/`full`-less missing sidecar) degrades to the 2-state `{0, 2}`
  populated/not mask, where `0` means only "no stored digest" and asserts
  nothing about whether the cell was observed. **The mask does not carry which
  regime it is in** — a degraded mask and a 3-state mask over a block with no
  observed-but-empty cell are both `{0, 2}` — so check
  `has_exact_occupancy(store)` before keying on `mask == 1`; without it, an
  empty noise stratum on a degraded store reads as a genuine absence. Today
  occupancy equals digest coverage, so state `1`
  does not occur; once the
  [issue #334](https://github.com/englacial/zagg/issues/334) signal strata
  land, noise-occupied cells appear as `1` automatically — the 3-state
  upgrade is data-driven, not a reader flag.

`read_raw_values` / `read_locations` report the same deinterleaved
`(row, col)` per cell. `readers._layout.rowcol_to_rank` inverts that to the
**chunk-local** nested rank (`0..4**depth - 1`) — *not* a `read_cell` key,
which is a **global** cells-axis index; the chunk's start offset is the
missing term, and a bare rank is always in range so `read_cell` would read
the wrong cell without raising. `cell_index` composes the two:

```python
for morton, (row, col), values in read_raw_values(store, field):
    cell = cell_index(store, field, morton, row, col)  # chunk_start + rank
    assert (read_cell(store, field, cell)[:, 0] == values).all()
```

It resolves the offset from the sibling `morton` coordinate, searching only
the array's stored spans (the populated chunks the sweep readers yield from)
and no digest bytes (on a windowed leaf, whose coordinate is derived, it
reads the one matching inner chunk's payload to confirm the chunk is stored). `morton_index` must be a read-chunk id — a coarser
`block_order` block id names no single chunk and raises.

## Issue #210 typed-dtype migration

The `ragged` attrs block is the element contract for `/1` stores — pinned as
the 1.0 wire contract, valid indefinitely. The
[issue #210](https://github.com/englacial/zagg/issues/210) migration adds a
`/2` revision whose chunk bytes are identical and whose element declaration
moves into a typed `vlen-ndarray` data type; the succession mechanics are
normative in [spec §1.6](specification.md#16-succession) and
[spec §6](specification.md#6-zagg-ragged2).
