# zagg store specification (1.0)

This page is the **normative record** of zagg's byte-level store conventions:
the ragged vlen-bytes layout, the t-digest payload bytes, the packed
composition word, the pyramid/overview declarations, the O11 content-hash
recipe, the temporal and located declarations on the word-typed
coordinate and companion arrays, and the temporal section of the store-root
coverage sidecar. It exists so an independent reader
([moczarr](https://github.com/espg/moczarr)) can decode a zagg store from this
page and the committed conformance fixtures alone — no zagg import, no
reverse-engineering of `grids/base.py`
([issue #340](https://github.com/englacial/zagg/issues/340), the
reader-migration gate).

The precedent is mortie's
[`docs/specification.md`](https://github.com/espg/mortie/blob/main/docs/specification.md),
which governs everything *below* this page: the packed morton word, the
decimal path grammar, the morton-hive tree layout and leaf naming, the
coverage-MOC serializations, and the rank-space deinterleave. This page owns
the **array-level** contracts inside a leaf; it cites mortie's page for path
and word semantics and never restates them.

**Latitude convention of the words** (*informative* — see
[Normative language](#normative-language) below; issue
[#549](https://github.com/englacial/zagg/issues/549)): the packed words are
produced by mortie's geodetic ingress (`geo2mort`) under **mortie's default
latitude convention** — zagg passes no `latitude=` override anywhere
(`zagg.grids.healpix.HealpixGrid.assign`). That default is `authalic-wgs84`
(geodetic latitude converted to authalic before the spherical HEALPix
mapping, mortie spec §9) from mortie 0.9.8 on
([espg/mortie#186](https://github.com/espg/mortie/issues/186), on PyPI
2026-08-16); earlier mortie encoded `geodetic-spherical`. The convention a
store carries is therefore mortie's default *at write time*, not a property
zagg declares: zagg ≥ 0.45.0 floors mortie at ≥ 0.9.8 (issue
[#438](https://github.com/englacial/zagg/issues/438)), so any environment
that resolved those declared dependencies encodes authalic — but on the
fleet mortie comes from the separately built Lambda layer, not from the
function zip, so a worker whose layer predates the bump would write
`geodetic-spherical`, and nothing in the store records which. The
**published o9 stores are authalic-wgs84** on stronger, artifact-level
evidence: they were written by zagg 0.52.0, whose temporal encode path runs
`from mortie import TOC_MAX_NS` (a name mortie exports only from 0.9.10
on), so a worker on a stale layer raises there instead of writing. The two
conventions are
non-corresponding partitions of the sphere (mortie spec §9): the offset
peaks near 45° latitude at 0.12830° (~14.26 km), so a cover or store built
under one convention does not describe cells under the other, and composing
them is meaningless (mortie spec §9 is where the prohibition binds). The
`dggs` attrs block
(`zagg.grids.healpix.HealpixGrid._dggs_attrs`) stamps mortie's `latitude`
token — `"latitude": "authalic-wgs84"`, mandatory for a writer at the current
mortie spec version, since [#580](https://github.com/englacial/zagg/issues/580)
(folding #549's resolution; `zagg.grids.morton.LATITUDE_CONVENTION`) —
beside `ellipsoid: {name: WGS84, semimajor_axis: 6378137.0,
inverse_flattening: 298.257223563}` and no sphere radius: that entry is the
**ingress datum** (the geodetic coordinates fed to `geo2mort`, equal-area on
that ellipsoid by construction), not an instruction to compute cell geometry
on the ellipsoid — the words themselves live on the R = 6371.0088 km
authalic sphere the conversion maps onto. A store whose `dggs` block carries
no `latitude` key predates the token; this paragraph is its record, and it is
authalic by the version evidence above. The absence rule is **per artifact**,
not per store: a partially rewritten store legitimately carries both vintages
— a sweep, stage or column backfill re-templates the overviews and columns it
writes, so those pick up the token while the leaves beside them, never
rewritten, do not — and each artifact's own `dggs` block is authoritative for
that artifact. A reader reproducing cell geometry
(e.g. a viewer's boundary golden test) needs the geodetic ↔ authalic
conversion at every geodetic seam, exactly as mortie spec §9 prescribes.

Design *rationale* — why each decision was made, with trade studies and
ratification records — lives in
[`design/sparse_coverage.md`](design/sparse_coverage.md) (the D/O-numbered
decisions registry) and in the narrative companions
[`ragged_layout.md`](ragged_layout.md) and
[`signal_strata.md`](signal_strata.md). Those documents *cite* this page; this
page is the spec. Byte layouts, attrs grammars, and constants are normative
**here only** — duplicated normative text drifts.

## Normative language

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are used as in
RFC 2119. Text marked **Contract** is frozen for the revision that carries it;
text marked *informative* explains or motivates and binds nothing.

## Conformance

- Every versioned convention on this page is signaled in store metadata — by a
  `spec` marker (`"zagg-ragged/1"`, `"zagg-composition/1"`, …), the
  coverage-envelope discipline, **or, where a section names it, by the array's
  registered zarr data type**. A conforming reader MUST strict-check whichever
  signal the owning section names and **fail loudly on an unknown or future
  revision**, never half-parse under a guessed layout.
- Marker-*absence* is legal only where a section names the data type that
  replaces the marker. Today there is exactly one such carve-out: the typed
  `vlen-ndarray` dtype **is** the `zagg-ragged/2` signal, and the `ragged`
  attrs marker is deliberately retired (not written) on those arrays
  (§1.6/§6.1) — so a reader MUST NOT treat a missing marker there as an
  unknown revision. Everywhere else absence is a hard failure: a
  `variable_length_bytes` array with no `ragged` block is not signaled at all
  and MUST be refused (§1.2).
- A revision, once published here, is **frozen**: its text never changes
  semantics, and stores written under it remain valid indefinitely. New
  behavior is a new revision (`/2`, `/3`, …) with its own section and an
  explicit succession clause; readers add revisions, they never drop them.
- The committed conformance fixtures (§7) are part of the contract: a reader
  implementation that reproduces the fixtures' expected decoded values and
  content hashes conforms to §1–§3, §5, §8, §9 and §10. zagg's own test suite asserts the
  same expectations (`tests/test_spec_conformance.py`), so the spec, the
  fixtures, and the shipping reader cannot drift apart silently.

Contents:

1. [`zagg-ragged/1` — the vlen-bytes ragged layout](#1-zagg-ragged1)
2. [Digest payload semantics](#2-digest-payload-semantics)
3. [`zagg-composition/1` — the packed composition word](#3-zagg-composition1)
4. [Pyramid / overview declarations](#4-pyramid-overview-declarations)
5. [O11 content hashes](#5-o11-content-hashes)
6. [`zagg-ragged/2` — the typed `vlen-ndarray` revision](#6-zagg-ragged2)
7. [Conformance fixtures](#7-conformance-fixtures)
8. [`zagg-toc/1` — the temporal declaration](#8-zagg-toc1)
9. [`zagg-located/1` — the located declaration](#9-zagg-located1)
10. [`zagg-coverage-toc/1` — the root coverage temporal section](#10-zagg-coverage-toc1)
11. [Icechunk companion repo — the virtual-ref index](#11-icechunk-companion-repo)

---

## 1. `zagg-ragged/1`

**Status: contract — pinned as the 1.0 wire contract**
([#340 amendment](https://github.com/englacial/zagg/issues/340)). This is the
shipping format, not a placeholder: `/1` stores remain valid indefinitely,
existing stores never require rewriting, and every conforming reader MUST
support `/1` unconditionally. Rationale and history:
[`ragged_layout.md`](ragged_layout.md),
[issue #209](https://github.com/englacial/zagg/issues/209).

A `kind: ragged` field (per-cell variable-length data — e.g. a t-digest) is
stored as **one `variable_length_bytes` zarr v3 array per field** on the cells
axis. Source of truth in code: `zagg.grids.base.ragged_array_spec` /
`RAGGED_ELEMENT_ATTR` / `RAGGED_SPEC`.

### 1.1 Layout

A ragged field `{field}` under a product group is up to four sibling arrays:

```text
{group}/{field}             <- vlen payload array; populated cell i holds the raw
                               little-endian bytes of its (n, *inner_shape) payload
{group}/{field}_locations   <- LOCATED fields only (issue #87): per-row uint64
                               location words, row-aligned with {field}
{group}/{field}_times       <- TEMPORAL fields only (§8.3, issue #410): per-row
                               uint64 toc words, row-aligned with {field}
{group}/morton              <- per-cell uint64 morton coordinate (zagg's standard
                               HEALPix coordinate array; the chunk-identity source)
```

- Each populated cell's value MUST be the raw **little-endian** bytes of an
  `(n, *inner_shape)` array (`n` varies per cell — e.g. `(k_centroids, 2)` for
  a t-digest whose `inner_shape` is `(2,)`), C-order, in the declared element
  dtype, independent of the producing machine.
- Empty cells keep the `b""` fill (`fill_value: ""`); an inner chunk with no
  ragged data MUST be **absent from the store** — omitted as an object in the
  per-inner-chunk geometry, and marked absent with the §1.5 sentinel inside
  the shard object when sharded (the default). Either way object size scales
  with populated chunks only — the same sub-shard sparsity the dense arrays
  get.
- A **located** field's sibling `{field}_locations` array is itself a
  `zagg-ragged/1` vlen array (element dtype `uint64`, empty `inner_shape`)
  with the same shape and chunk geometry as the payload array, and MUST be
  **row-aligned**: cell `i` of the sibling holds exactly one `uint64` word per
  payload row of cell `i`. Readers MUST bind the sibling by the payload
  array's `locations` attrs declaration (§1.2), never by reconstructing the
  `{field}_locations` naming convention.
- A **temporal** field's sibling `{field}_times` array (§8.3) is the same
  shape of thing under the same row-alignment rule, bound by the payload
  array's `times` attrs key. A field may carry either sibling, both, or
  neither.

### 1.2 The `ragged` attrs block

The element interpretation is **self-describing** in the payload array's attrs
under the `ragged` key:

```json
"ragged": {
  "spec": "zagg-ragged/1",
  "element": {"dtype": "float32", "shape": [-1, 2]},
  "locations": "h_tdigest_locations"
}
```

- **`spec`** — the convention revision. Readers MUST strict-check it: an
  unknown/future spec raises, never half-parses.
- **`element`** — `{"dtype": "<numpy dtype>", "shape": [-1, *inner_shape]}`.
  The `-1` marks the per-cell varying count; a reader reconstructs cell `i` as
  `np.frombuffer(a[i], dtype).reshape(-1, *inner_shape)`, with the dtype read
  little-endian.
- **`locations`** — present only on a located field's payload array; its value
  is the name of the sibling uint64 array carrying the per-row location words.
  An unlocated field records nothing here.

A vlen array without a well-formed `element` declaration is **not** a
`zagg-ragged/1` array; a reader MUST refuse it with a pointed error rather
than decode under a guessed layout (pre-issue-209 CSR stores are a hard
break). The `ragged` attrs key is reserved: config-declared field attrs MUST
NOT shadow it (enforced at config validation). The §2.0 `weights` key and
§8.3's `times` binding are likewise spec-owned on a ragged payload array —
writer-stamped from the field's declaration, never author-transcribed — as
are the §8/§9 `temporal` and `located` declaration blocks on the companion
arrays that carry them. A located field's provenance attrs (e.g. `stratum`,
`signal_threshold` — §3.3) land on the **payload array only**; a sibling
array carries no **user** attrs, only the spec-owned declaration its own
section defines.

### 1.3 Codec chain

The per-chunk codec chain MUST be `[vlen-bytes, zstd(level=3, checksum=false)]`.
The zstd deviates from the dense arrays' bytes-only/uncompressed policy
deliberately: a vlen payload has no fixed-width raw layout to preserve, and
the level is fixed so identical payloads produce identical objects across
workers. (zarr-python names the dtype `variable_length_bytes` in array
metadata while the v3 registry name is `bytes` — zarr-python#3517, accepted
both ways on read; readers MUST accept the `variable_length_bytes` spelling.)

### 1.4 Wire framing

**Contract (golden-pinned).** Within one inner chunk the `vlen-bytes` codec
frames the cells before compression as (little-endian throughout):

```text
u32  cell_count
per cell:  u32 payload_length  ||  payload_bytes
```

i.e. numcodecs' `VLenBytes`/`VLenArray` framing — a `u32` count of cells, then
for each cell a `u32` byte length followed by that many payload bytes (`0` for
an empty cell). The `payload_bytes` are
`np.ascontiguousarray(value).tobytes()` of the cell's `(n, *inner_shape)`
array in the declared dtype. This exact byte vector is frozen by a golden test
(`tests/test_processing.py::TestRaggedVlenLayout::test_golden_inner_chunk_framing`)
and exercised by the §7 fixtures; it is what makes the §6 typed-dtype
revision a metadata-only migration.

### 1.5 Storage geometries

Both geometries hold the same logical data and are self-describing in the
array's own zarr metadata (its `chunk_grid`, and whether a `sharding_indexed`
codec wraps the chain), so a single reader code path MUST read either —
deriving the stored span from the array's shard shape when sharded, else its
chunk shape:

| geometry | on disk | single-cell read |
|---|---|---|
| **sharded** (`ShardingCodec`; every hive leaf and the sharded flat path) | ONE object per shard; the shard's K inner chunks live inside it with an internal index | 2 ranged GETs (index suffix + one ranged inner chunk) |
| **per-inner-chunk** (regular array; the unsharded streaming path) | one object per inner chunk | 1 GET (the object) |

When sharded, the §1.3 chain rides INSIDE a `sharding_indexed` codec whose
outer chunk spans the shard, with index codecs
`[bytes(endian=little), crc32c]` and `index_location: end`: the shard object's
suffix is K `(offset, nbytes)` u64 pairs plus a crc32c, and an inner chunk
with no ragged data is marked absent with the `2^64 - 1` sentinel in both
fields (zarr v3 sharding spec) — object size scales with **populated** chunks
only. The 2-GET random-access recipe follows: fetch the
`16*K + 4`-byte index suffix, then the one ranged inner chunk holding the
cell.

**Subtree spans.** The cells axis MUST be in canonical nested order — the
per-cell `morton` coordinate ascending, every aligned power-of-four span
sharing its ancestor cell (the ordering every §1 identity derivation and the
rank-space deinterleave already presuppose; a zagg writer has never produced
anything else, this sentence makes it citable). Consequently the order-`k`
subtree below an ancestor at nested rank `r` on an order-`c` cells axis
occupies exactly the contiguous index span `[r·4^(c−k), (r+1)·4^(c−k))`, and
a reader MAY serve "everything below one morton node" as a contiguous-slice
read: on the sharded geometry the index suffix plus only the covering inner
chunks (the 2-GET recipe generalized to a span), on the per-inner-chunk
geometry only the covering chunk objects — never a whole-array sweep. The
span property is normative; a dedicated subtree reader is implementation
(zagg: [issue #351](https://github.com/englacial/zagg/issues/351)).

**Leaf immutability.** A committed leaf's data objects are **write-once**:
once the commit stamp lands, a writer MUST NOT modify bytes at an existing
leaf key in place — no partial rewrite, no append, no re-encode of one
array. The only legal change to a stamped leaf is **wholesale replacement**
under the D4 retry discipline: the leaf template clears the prefix, rewrites
it, and lands the stamp last. This is what lets a byte-range index into a
leaf — the §11 Icechunk companion refs, moczarr's 2-GET reads — trust
`(key, offset, length)` for as long as the stamp it was taken under stands.

**Versioned leaves.** On a **legacy** leaf (a stamp without `current`) a
replacement is not a change of **keys**: clear-then-template
rewrites the same key layout, and on an unversioned bucket the new object
lands at exactly the key an old reference names — so a reference taken under
a superseded stamp does *not* 404: it reads the new object's bytes at a stale
offset and would silently mis-decode. Every recorded reference therefore
carries the referenced object's **ETag** — or, on a local store, its
`last_modified` — **as its Icechunk checksum** (§11.3), which Icechunk
verifies on read, so a stale reference fails **loudly** instead of returning
wrong bytes (issue [#580](https://github.com/englacial/zagg/issues/580)).

A **versioned** leaf retires the case (issue
[#582](https://github.com/englacial/zagg/issues/582), espg ruling
2026-09-26). Its stable prefix `{id}.zarr/` (§4.2; the D3 address every
reader computes) is a **pointer root**: the root `zarr.json` carries the
commit stamp as before plus one key, `current`, naming the **version
subgroup** that holds the arrays — `{id}.zarr/{current}/{cell_order}/…`, a
complete, self-describing zarr leaf with its own stamp, whose objects are
**never rewritten once stamped**. The version name is per **attempt**:
`run-{run_id}-{attempt}`, where `run_id` is the run's id (`uuid4().hex`)
and `attempt` is a per-invocation nonce — 8 hex characters of a fresh
`uuid4`, drawn by the writer for each unit it writes — so two writers of one
unit (a duplicate-invoke retry, a redundant fleet worker) never share a
prefix. The run's tag `run-{run_id}` in the §11 repo still groups all of a
run's versions by prefix. A
replacement writes a **new** version subgroup and moves the pointer; the
superseded version's objects stay at their keys until the §11 garbage
collector finds no retained snapshot naming them. Consequently a reference
taken under any stamp stays valid for as long as its version exists — a run
tag reads every **base leaf** exactly as that run left it — and a same-key
rewrite of a base leaf's committed bytes never happens. Only base leaves are
versioned: the §4.6 leaf column and every overview level, which §11.3 also
indexes, are still rewritten at their keys, so a run tag reads them only as
the latest run left them and a superseded ref into one fails checksum-loud
(the legacy semantics) until they are native Icechunk overviews (issue
[#584](https://github.com/englacial/zagg/issues/584)).

The **write order** is normative: (1) write the version subgroup's arrays;
(2) stamp the version (its own root `zarr.json`, whose `spec` is `/1` or `/2`
exactly as the legacy rule assigns it — windowed ⇒ `/2`); (3) record the
refs against the version's objects (§11.3) — a commit on `main` under
`commit: "leaf"`, the ladder ref sidecar under `commit: "ladder"` (§11.4);
(4) swap the pointer — one PUT of the stable root `zarr.json`, mirroring the
version's stamp and naming it as `current`. What readers see: a **path**
reader (the D3 address, the stamp, the pointer) sees the previous state until
(4) lands; an **Icechunk** reader of `main` sees the new version at its
commit — under `commit: "leaf"` that is step (3), before the swap; under the
ladder it is the staged sweep's node commit, which may follow (4). A retry **always** writes a
new version — same run or not, it draws a fresh `attempt` — so no writer
ever opens, clears or resumes a prefix another writer may hold. An attempt
that died before its pointer swap leaves a version that is not `current`:
unreferenced (under `commit: "ladder"`, where only the sidecar named it) or
referenced by `main` (under `commit: "leaf"`, refs already committed); the
§11.4 collector reclaims it under its reference rule.

**The lifecycle touch never reaches a version.** The #388 skip-path touch of
a versioned unit (a write path's objects are fresh and need none) refreshes
exactly the stable root `zarr.json` — the pointer stamp, one object, never
`{root}/` as a tree — and the unit's sibling objects: the stats sidecar, the
granule-id sibling, the sub-map, the Icechunk ref sidecar, and the column
tree plus its sidecar when declared. Those siblings stay unversioned, keyed
off the leaf stem and rewritten per run. A version subgroup's objects —
current or superseded — are **never touched**, so no touch re-mints an ETag
or moves an mtime that a recorded reference carries (§11.3). Their lifetime
is governed by the §11.4 collector, **not** by bucket expiration: an
expiration rule on a store holding versioned leaves MUST NOT cover version
subgroups — the same posture the repo's own `icechunk/` prefix takes — or
the untouched current version ages out under a live pointer.

**Readers carry one rule**: open the stable root; if its stamp names
`current`, the arrays are under `{root}/{current}/`, else under `{root}/`
itself. **Absent `current` is a legacy leaf** — every store written before
this revision — so no store requires migration and a store legitimately
mixes legacy and versioned leaves after its first post-revision write. The
manifest's own `spec` (§4.2's window-naming dialect) does not move, and no
stamp `spec` value marks versioning either: a leaf is versioned exactly when
its root stamp names `current` — the one marker, orthogonal to the naming
dialect the `spec` token carries.

The root's states, for readers and for a writer's skip gate: **no root
`zarr.json`** — debris (D4), as today; the (4) PUT is what creates the root
group of a fresh leaf. **A root stamp without `current`** — a legacy leaf,
arrays at the root. **A root stamp naming `current`** — arrays at the
version. A pointer naming a **missing or unstamped** version is a corrupted
leaf that a reader, and a writer's skip gate, treats as debris: the pointer
is only ever written after the version is stamped, so the state arises only
from out-of-band deletion. The root's mirrored §5.3 `content_hashes` are
computed over the version store, so their keys are relative to the version
root (`{cell_order}/morton`, not `{current}/{cell_order}/morton`) — identical
in form to a legacy leaf's; a verifier opening the pointer resolves
`current` first. **A writer about to clear-and-template a leaf root whose
stamp names `current` MUST refuse**: it is a legacy or stale writer against
a versioned leaf, and clearing the root would delete every version and
every earlier tag's referents with it.

### 1.6 Succession

The `ragged` attrs block is `/1`'s element contract. The candidate successor
is the [§6](#6-zagg-ragged2) typed-dtype revision (`zagg-ragged/2`,
[issue #210](https://github.com/englacial/zagg/issues/210)). The signaling
mechanics are normative now:

- An array whose zarr data type is the typed `vlen-ndarray` dtype **is**
  `zagg-ragged/2`; on such arrays the `ragged` attrs marker is **retired**
  (not written).
- An array with the `variable_length_bytes`/`bytes` dtype and a
  `spec: "zagg-ragged/1"` attrs block **is** `zagg-ragged/1`.
- `/1` remains valid indefinitely — existing stores never require rewriting,
  and every conforming reader supports `/1` unconditionally, whatever timing
  the `/2` implementation lands on.

## 2. Digest payload semantics

**Status: contract (payload bytes); the digest algebra is deliberately NOT
specified.**

A t-digest field is a `zagg-ragged/1` (or `/2`) array whose element
declaration is `{"dtype": "float32", "shape": [-1, 2]}`. Source of truth in
code: `zagg.stats.tdigest`.

### 2.0 The `weights` declaration

**Contract** ([issue #422](https://github.com/englacial/zagg/issues/422)).
A digest payload array declares the semantics of its weight column under the
**`weights`** attrs key — a **sibling** of the §1.2 `ragged` block on the
payload array, never a key inside it (the `ragged` block is retired wholesale
under `/2` — §1.6/§6.3 — so a sibling key survives that metadata-only
migration untouched). Two values are defined:

- **`"counts"`** — weights are observation counts: integers ≥ 1 whose sum is
  the cell's exact observation count, per §2.1. **An absent `weights` key
  MUST be read as `"counts"`** — every store written before this revision is
  conformant verbatim, no byte rewritten.
- **`"flux"`** — weights are calibrated flux: positive finite float32 reals
  (a zero-weight observation carries no flux and MUST NOT produce a row);
  `sum(weights)` estimates the cell's detected **photoelectrons**, not an
  observation count. A flux-declared array MUST record its calibration
  provenance in the same attrs: a `gain` key carrying at minimum the gain
  constant's `name` and `version` (the operating point of any write-time
  clip rides alongside, writer-defined).

A reader MUST strict-check the value: an unknown declaration is a future
revision of this section and MUST be refused, never read as either defined
value. **Merges are legal only between payloads carrying the same
declaration** (counts with counts, flux with flux — an absent key is
`"counts"` for this rule too): a mixed merge would produce a weight column
whose sum means neither thing, so a merging reader or writer MUST refuse it.
The declaration rides the payload array only; a located field's
`{field}_locations` sibling carries no `weights` key (§1.2's no-user-attrs
rule for siblings is unchanged).

### 2.1 Centroid array

**Contract.** A populated cell's decoded payload is a `(k, 2)` **float32**
array of weighted centroids:

- column 0 is the centroid **mean**; column 1 is the centroid **weight** —
  under the `"counts"` declaration (§2.0, the default) the number of
  observations merged into it, an integer ≥ 1; under `"flux"` a positive
  real per §2.0;
- rows MUST be sorted **ascending by mean**;
- under `"counts"`, `sum(weights)` MUST equal the cell's **exact** observation count — the
  number of finite `source` values the digest was built over (non-finite
  source rows are dropped before building) — **while that count is
  representable in float32, i.e. `<= 2^24` (16,777,216)**; above that bound
  the weights and their sum are the nearest float32 values to the true counts,
  so a reader recovering counts from weights (§3.3 tells it to, for
  `N_signal`) gets the exact integer at or below the bound and a rounded one
  above it. The bound is comfortable at leaf cell orders and is the one to
  watch at coarse overview orders (§4.4). For a stratified product (§3) each
  stratum digest's total weight is the exact stratum count, under the same
  bound;
- under `"flux"` (§2.0) `sum(weights)` is a float32 photoelectron estimate,
  not a count: the exact-count recovery above (and §3.3's) is undefined for
  a flux payload, and no integrality holds;
- an absent cell decodes as the zero-length `(0, 2)` array (the `b""` fill).

### 2.2 The location channel

**Contract.** A located digest field (issue #87) carries one **uint64 morton
word per centroid row** in its `{field}_locations` sibling (§1.1), row-aligned
with the payload:

- **A word's claim is keyed on its kind**, carried by the word's own
  encoding (mortie spec §4), never by the payload's weights — under a
  `"flux"` payload (§2.0) `sum(weights)` is not a member count, so weight
  identifies nothing:
  - a **point word** (order-29 by mortie's grammar) is the observation's
    reported position, carrying **no area claim** — not an assertion that
    the observation is dimensionless;
  - an **area word** is a cell known to contain every observation beneath
    it — the finest such cell its producer could establish. Under this
    section an area word arises only from a fold (the deepest common
    ancestor of the fold's input words); ingesting one directly is the
    [§9](#9-zagg-located1) declaration's grant, not this section's.
- **Per-observation ingest under this section is order-29 point words**
  (`HealpixGrid.assign`): an observation enters as its reported position's
  point word, and area words appear only as fold products. A writer whose
  observations are resolved only to a cell (a pre-gridded or pre-aggregated
  input) MUST NOT narrow them into points — §8.1's discipline, spatially —
  and therefore MUST write them under the §9 declaration, which grants
  area-word ingest; this section deliberately does not. The restriction is
  the freeze rule at work: these semantics predate the declaration, and a
  store read "as §2.2 verbatim" keeps exactly the meaning this section was
  published with.
- A merged centroid carries the **deepest common ancestor of its members'
  words** (`mortie.common_ancestor`): a cell containing every observation
  merged into it, and the finest one those words establish — a fold sees
  words, never the observations beneath them, so a coarse input word bounds
  how fine the result can be. A centroid folded from a single member carries
  that member's word unchanged. Point and area words share the same path
  prefix, so mixed inputs (a fresh point word folded with an earlier merge's
  coarser area word) compose under the one rule.
- **Orders are heterogeneous, leaf arrays included.** A reader MUST decode
  each word's order and kind from the word itself (mortie §1/§4) and MUST
  NOT assume a uniform order per array, per cell, or per store. A store whose
  leaf words happen to be uniformly order-29 point words (every shipped
  config's output today) is an observation about particular bytes, never an
  inference this section licenses — the same duty §9.1 spells out for a
  declared companion, stated here so the undeclared read path (§9's
  absent-key clause) carries it too.

A morton cell encodes **containment, not calibrated uncertainty**
(informative): a word records the resolution at which the producer located
the observation, never its error budget — a small error disk straddling a
cell boundary is honestly enclosed only by a much coarser cell, so a reader
MUST NOT read a word's cell as an uncertainty region, and an error-radius
channel, if ever wanted, is a new companion declaration, not a reading of
this one. This is a reader's limit, not a writer's obligation to coarsen:
every position-resolved observation still enters as its point word.

A writer-side spill-block close (`aggregation.streaming.mode: spill` crossing
its block threshold, issue #370) is an additional merge source under the same
rule: an overflow shard's centroids may carry coarser common-ancestor words,
with the stored layout and byte-level contract above unchanged.

Word semantics (bit layout, kind marking, coarsening) are mortie's
specification §1/§4, not restated here. A located sibling MAY additionally
carry the [§9](#9-zagg-located1) `located` declaration, which self-describes
that grammar in the store; its absence is this section verbatim, never a
refusal (§9).

### 2.3 What is deliberately not specified (informative)

The build/merge **algebra** — Dunning's k1 scale function, the `delta`
compression budget, merge order, `merge_tdigests` / `merge_tdigests_kway` —
is zagg-owned and referenced informatively only (`zagg.stats.tdigest`
docstrings). Readers do not need it to decode: the stored bytes above are the
whole contract, and keeping the algebra out of the spec preserves zagg's
freedom to optimize it (issue #279) without a spec revision. Two consequences
a consumer should know (informative): digest merging is **order-dependent**
(approximate composability class — `np.isclose`, not byte equality, across
different fold orders), and quantile estimates from the centroids are
approximations with the usual t-digest accuracy profile (tight tails, looser
middle).

## 3. `zagg-composition/1`

**Status: contract.** Source of truth in code: `zagg.stats.composition`
(issue #321). Rationale and narrative:
[`signal_strata.md`](signal_strata.md).

A composition field is one dense **uint64** word per cell carrying eight
8-bit lanes of quantized fractions of the cell's **signal stratum**
(`N_signal` = the signal digest's total weight — magnitude lives in the
digest, composition here). An empty signal stratum packs to `0`, and a
composition array **MUST** declare `fill_value: 0` so an *unwritten* cell
decodes to the same word: readers key presence off `lane > 0` (§3.2), so a
nonzero fill would make every unwritten cell report spurious flag presence
(a fill of `1` reads as lanes `[1,0,0,0,0,0,0,0]` — "`land` occurred
exactly"). Enforced at config validation, alongside the §3.3 `of`/`threshold`
cross-checks.

### 3.1 Word layout

**Contract.** Lanes are packed **LSB-first**: lane `i` occupies bits
`8*i .. 8*i + 7` of the word. Lane order (`LANES`):

| lane (byte) | meaning |
|---|---|
| 0–4 | per-surface fractions, `signal_conf_ph` column order: `land`, `ocean`, `sea_ice`, `land_ice`, `inland_water` — the count of signal photons whose per-surface confidence clears the threshold, over `N_signal` |
| 5–7 | `low` / `med` / `high`: signal photons whose *strongest* per-surface confidence is exactly 2 / 3 / 4, over `N_signal` |

- The per-surface lanes are **overlapping marginals** (`surf_type` is
  multi-hot): they do not sum to 255 and cannot split the height distribution
  per surface.
- The level lanes are **absolute** — always `conf == 2 / 3 / 4`, never
  renumbered against the signal `threshold`. A product committing a higher
  threshold ships **empty** lower lanes rather than shifted ones
  (`threshold=3` leaves `low` structurally 0; `threshold=4` leaves `low` and
  `med` 0), so one lane layout serves every product. For ATL03 confidences
  (`-2..4`) the three level lanes partition the signal stratum exactly; a
  source with confidences above 4 is out of contract for this revision.

**Golden word.** For a single signal photon with per-surface confidences
`[4, -1, 0, 3, 1]` at `threshold=2`, the lanes are
`[255, 0, 0, 255, 0, 0, 0, 255]` (land, land_ice; strongest = 4 ⇒ high) and
the packed word is exactly

```text
0xFF000000FF0000FF
```

(an MSB-first layout would give `0xFF0000FF000000FF`). Pinned by
`tests/test_composition.py::TestPackComposition::test_golden_word_pins_lsb_first_byte_order`
and the §7 kitchen-sink fixture.

### 3.2 Quantization: the presence floor

**Contract.** Lanes quantize as `k = round(255 * c / N)` — round-half-even,
clipped to `0..255` — **except any nonzero count quantizes to at least 1**
(the presence floor). Consequences:

- `lane > 0` means "this flag occurred" **exactly, at every N**, through
  arbitrary merge chains.
- Count recovery `round(k * N / 255)` is exact whenever `N <= 254`
  (quantization error `<= N/510 < 1/2`).
- Above that, counts are within `±N/510` (plus `O(N/510)` per re-quantizing
  merge); presence stays exact.
- A cell with one signal photon has lanes in `{0, 255}` — the lanes *are*
  that photon's flags.

### 3.3 The attrs block

**Contract.** The composition array's attrs carry the versioned
`composition` block; readers MUST bind to it, never to config conventions,
and MUST strict-check `spec` per the conformance rule:

```json
"composition": {
  "spec": "zagg-composition/1",
  "lanes": ["land", "ocean", "sea_ice", "land_ice", "inland_water", "low", "med", "high"],
  "of": "h_tdigest_signal",
  "threshold": 2
}
```

- **`lanes`** — the lane names in bit order (LSB byte first). For `/1` the
  value is exactly the §3.1 order — lane order is **not** a product knob: the
  packer writes lane `i` at bits `8*i .. 8*i+7` in that fixed order, so a
  permuted, truncated, or renamed `lanes` value is **out of contract** and a
  writer MUST reject it rather than emit it.
- `spec` and `lanes` are **writer-stamped, not author-declared**: the store's
  values come from the writer's own constants
  (`zagg.stats.composition.COMPOSITION_SPEC` / `LANES`, stamped onto the array
  spec by `grids.base.apply_field_attrs`), the same posture as the §1.2
  `ragged` block, and a config declaration that disagrees with either is
  rejected at config validation. `of` and `threshold` are per-product and
  author-declared, cross-checked at config validation (`of` must name a
  declared `kind: ragged` field; `threshold` must equal the reducer's own
  `params.threshold`).
- **`of`** — the name of the sibling digest field whose total weight is the
  `N_signal` the lanes are fractions of. The composition word is
  uninterpretable without it: readers recover counts by pairing the word with
  that digest's `sum(weights)`.
- **`threshold`** — the committed signal cut (`conf >= threshold`; the ATBD
  predicate is `> 1`, i.e. `threshold=2`). Each stratum digest's payload
  array carries the companion provenance attrs `stratum`
  (`"signal"`/`"noise"`) and `signal_threshold`, which MUST agree with this
  value.

### 3.4 Merge law

**Contract** (normative here, not a zagg implementation detail: a reader
folding views — e.g. cells into a coarser cell — must reproduce it). Two
`(word, n_signal)` pairs fold as the digest-weighted mean per lane,
re-quantized with the same presence floor:

```text
lane_merged = quantize((n_a * lane_a + n_b * lane_b) / (n_a + n_b))
```

where `quantize` rounds half-even, clips to `0..255`, and floors a lane that
is nonzero on **either** input to at least 1. The identity element is
`(0, 0)`; a pair with `n <= 0` returns the other word unchanged. The
operation is symmetric and, up to the bounded re-quantization error,
associative — fold order never affects presence, and affects counts only
within `O(n/510)`. The `n` inputs come from the `of` digests' total weights
(§3.3).

**The k-way form.** A writer folding many parts at once collapses them in
ONE pass under the same law — `lane = quantize(Σ nᵢ·laneᵢ / Σ nᵢ)`, the same
presence floor, parts with `n <= 0` skipped, an all-empty part set folding to
`0` — quantizing **once**, so the result is order-independent (a permutation
of the parts returns identical bytes) and the count error stays one
quantization wide however many parts there are. This is zagg's writer-side
fold everywhere a stored composition folds (the spill block close, issue
#370, and every pyramid/overview fold site, issue #515;
`zagg.stats.composition.merge_composition_kway`); the binary law above stays
the reader-facing contract, the k-way form being inside its documented
associativity tolerance and strictly tighter than a binary chain.

**Composability (§4.5).** A composition field folds through the pyramid
under D24 class **`packed`**, method **`composition_kway`** — the k-way form,
with each contributor's `n` taken from its `of` digest's weight at the same
cell. An overview's composition array carries the same §3.3 attrs block a
leaf does, and its `N_signal` at every level is the folded `of` digest's
total weight at that level — exact up to §2.1's float32 bound: past `2^24`
pooled observations that sum is the nearest float32 to the true count (§4.6),
and the fold's divisor rounds that float to an integer. The residual is
relative ~1e-7, orders below one lane step of 1/255, so it never moves a
published lane; it is stated because a reader recovering counts from
`N_signal` is entitled to the same bound §2.1 gives the digest itself. One
consequence a reader must hold: each fold re-quantizes once, so a cascaded
level's counts drift by at most one quantization **per level** while
presence stays exact through the whole chain — deterministic, but not
byte-equal to a direct aggregation, which is why the class is neither
`exact` nor `approximate` (§4.5).

## 4. Pyramid / overview declarations

**Status: ratified design; implementation in flight
([#201](https://github.com/englacial/zagg/issues/201)).** The decisions this
section records are ratified — D11/D22–D24 in
[`design/sparse_coverage.md`](design/sparse_coverage.md), plus three rulings on
the #201 thread that this section's grammar traces to directly:
[`all.zarr` + `role: overview` confirmed, display schedule every 2 orders](https://github.com/englacial/zagg/issues/201#issuecomment-5025459421),
[the D24 `none`-class ruling (per-field exclusion the default, declared derived summary the opt-in, never the semantic core)](https://github.com/englacial/zagg/issues/201#issuecomment-5025509889),
and
[the A/B/C/D option space (C an espg-flagged opt-in phase, D rejected)](https://github.com/englacial/zagg/issues/201#issuecomment-5025519604);
the grammar
below is what the #201 implementation lands and what moczarr's level-node
reader plans against (espg/moczarr#15, the 8b seam). Any divergence
discovered while landing #201 is resolved **on this section first** — the
implementation conforms to the spec, never the reverse.

The **level grammar** is revision `zagg-pyramid/2`
([#382](https://github.com/englacial/zagg/issues/382); design record
[#381](https://github.com/englacial/zagg/issues/381), points (2)–(5), as
collapsed by the espg grammar ruling on the declaring PR): the config
declares **leaf cell resolutions only**, and everything above the shard is
the **fixed every-order ladder** of §4.4; the manifest records the fully
expanded `(node, cells)` list, of which the original constant-depth
`zagg-pyramid/1` grammar is the special case
`cells = [node + (cell_order - shard_order)]`. `/1` stores stay readable
under their own rule forever (§4.5 — we are the sole consumer; there is no
migration machinery). A `/2` declaration is materialized by the leaf
columns ([#383](https://github.com/englacial/zagg/issues/383), §4.6) and the
**staged sweep** ([#384](https://github.com/englacial/zagg/issues/384)):
declared-but-unswept remains a legal recorded state (#381 point (11):
declaring is free, sweeping is the operational decision), and since the
issue #384 default flip a **default declaration** (no schedule spelled) is
`/2` at the grid's resolved chunk order whenever that order is strictly
interior to the shard's resolution window (raster configs, explicit legacy
`orders`/`spacing` schedules, and K == 1 grids keep `/1`).

A `zagg-pyramid/2` store is therefore inherently a **multiresolution
statistical grid**: every HEALPix order from 0 through the declared base,
plus the native resolution, each level spec-guaranteed and individually
addressable — a reader picks its resolution and reads it; skipping levels
is a reader-side choice, never a store property. `output.pyramid: false`
is the degenerate single-resolution opt-out.

### 4.1 Overview zarrs at ancestor nodes

An **overview zarr** is a sweep-built coarse materialization of a subtree's
committed leaves, written at an **ancestor digit node** of the hive tree
(tree layout and path grammar: mortie's specification). It has the same
structure as a leaf (§4.4), one basename dialect (§4.2), and is classified by
attrs alone (§4.3).

Overviews are **regenerable caches**, never load-bearing: deleting every
overview MUST leave all leaf reads intact, and a reader MUST NOT require
them. They are stale-detectable, not stale-proof — after a leaf re-run an
ancestor overview may lag until the sweep regenerates it; the generation
stamp (§4.3) is what makes that detectable.

### 4.2 Naming

Overviews inherit the leaf window-naming dialect (D23; grammar frozen on the
mortie spec page): at an ancestor node an overview for time window `{window}`
is `{window}.zarr`, and the reserved token **`all`** names the all-time fold
(`all.zarr` — the same token that names a `schedule: none` store's leaves;
excluded from the window grammar forever). Nothing about the *name*
distinguishes an overview from a leaf — classification is §4.3's job.

A **versioned leaf** (§1.5; a stamp naming `current`) adds nothing at the node: its
stable `{id}.zarr` / `{id}_{window}.zarr` entry is unchanged, and its
versions are **subgroups** of that entry named `run-{run_id}-{attempt}` — so the D5
node invariant, the walker's child classification and every prefix-scoped
rule see the same children as before. The stable root's stamp names the
current version; the version subgroup carries the arrays. A version name is
never a cell-order digit group (it always begins with `run-`), so a reader
enumerating a leaf root tells the two apart by name.

### 4.3 The `role` and `zagg_overview` attrs

**Contract.** Classification is carried in the zarr's **root-group attrs**,
never inferred from tree position or depth — a shallow zarr may equally be
*coarse source* in a sparse region (D24: one product tree may carry
regionally heterogeneous resolution).

- **`role`** — `"overview"` on every sweep-built overview. **Source leaves
  carry no `role` key: absence means source.** A reader MUST check `role` on
  every zarr it opens at an overview-carrying order; analysis readers reject
  or skip `role: overview` zarrs, display readers MAY stop at one.
- **`zagg_overview`** — the versioned provenance block, present exactly when
  `role` is `"overview"`:

```json
"zagg_overview": {
  "spec": "zagg-overview/1",
  "node": "-3111",
  "order": 3,
  "cell_order": 11,
  "source_shard_order": 5,
  "source_cell_order": 13,
  "window": "2019",
  "fields": {"count": {"class": "exact", "method": "sum", "nan_policy": "skip"},
             "h_tdigest": {"class": "approximate", "method": "tdigest_kway"}},
  "fold_source": "leaves",
  "generation": {"n_leaves": 16, "max_leaf_timestamp": "2026-07-20T00:00:00Z"},
  "content_hash": "…",
  "generated_at": "2026-07-21T00:00:00Z"
}
```

  `spec` follows the conformance rule (strict-check, fail loudly on an
  unknown revision). `node` is the ancestor's morton decimal string and
  `order` its order — mortie's decimal grammar puts one digit per order after
  the base-cell digit, so `order` is always `len(digits) - 1` for the node
  string (`"-3111"` is order 3; the leading `-` marks a southern base cell and
  is not a digit); `cell_order` the overview's own cell order
  (`source_cell_order - (source_shard_order - order)` — constant tree depth,
  §4.4); `window` the §4.2 window key (`"all"` for the all-time fold);
  `generation` the D22 staleness stamp (merged-leaf count + max leaf commit
  timestamp); `content_hash` a sweep-internal skip-if-current digest
  (informative — not the §5 O11 recipe and not part of the reader contract).

  **`fields` enumerates the materialized fields only** — exactly the fields
  present as arrays in *this* overview, each recording the fold that was
  actually applied. A `none`-class field is absent from the zarr (§4.4) and so
  MUST be absent from this map: its recorded absence lives in the manifest's
  `pyramid.overview.fields` (§4.5), which is the map that enumerates **every**
  declared field. Consequently a reader MAY treat this map as the overview's
  variable list and MUST be able to open every array it names; cross-checking
  it against the arrays present is a valid integrity check. Each entry carries
  at least `class` and `method`, and MAY carry further fold provenance — an
  `exact` entry records the reduction's `nan_policy` (`"skip"`: nan-skipping,
  never NaN-propagating) — so readers MUST tolerate additional keys.

  **`fold_source` names the regime that produced this level**
  ([#376](https://github.com/englacial/zagg/issues/376)) — the one piece of
  provenance a reader cannot recover from the arrays:

  - `"leaves"` — folded directly from the subtree's source leaves: **single
    quantization**, and for the `exact` class byte-equal to a direct
    aggregation at this cell order (§4.4);
  - `"cascade"` — folded from an already-materialized **finer overview**
    (fold-of-folds), whose order the entry then also names:

    ```json
    "fold_source": "cascade", "fold_from_order": 3
    ```

  The distinction is material for the `approximate` and `packed` classes: the
  exact merge laws are associative, so a cascaded `sum`/`min`/`max` is the
  same value either way, while a cascaded digest is a **merge of merges** — it
  inherits the merge's documented behavior once per level and carries **no
  precision guarantee** — and a cascaded composition word re-quantizes once
  per level (§3.4: presence stays exact through the chain; counts drift at
  most one lane quantization per level). That is in contract: overviews are display
  artifacts, and the precision promise stops at the levels declared exact
  (`pyramid.overview.exact_levels`, §4.5). A reader that needs the exact
  regime MUST check this key rather than the level's depth, and MUST read an
  overview that carries **no** `fold_source` as `"leaves"` — the only fold
  that existed before #376.

  **`source_children` records the cascade's coverage** — present on a
  `"cascade"` overview only, and the companion a reader needs to interpret
  the level's *fill* cells:

  ```json
  "source_children": {"folded": 15, "missing": 1, "unreadable": 0}
  ```

  A cascade folds the child overviews that are **on disk**, where a
  `"leaves"` fold folds every leaf the coverage MOC knows about. `folded`
  counts the children that contributed, `missing` those with no materialized
  (D4-stamped) overview, and `unreadable` those that failed to open or did
  not classify as an overview at `fold_from_order`. When either of the latter
  two is nonzero the level **under-covers its subtree**: the spans those
  children own hold the fill value, and a fill cell there is **not** evidence
  that the subtree is empty. Such a level is repaired by a later sweep, which
  sees the parent's summed generation change once the child exists. A reader
  MUST tolerate the key's absence (a `"leaves"` level, or a pre-#376
  artifact) and MUST NOT read absence as `missing: 0`.

  **`demotions` records the `packed` guard rail firing at this node**
  ([#518](https://github.com/englacial/zagg/issues/518)) — present exactly
  when the fold that **last wrote this artifact** demoted a `packed`-class
  field, on **both** attrs revisions (`zagg-overview/1` and `/2`, §4.4):

  ```json
  "demotions": [
    {"field": "composition", "class": "packed", "reason": "word-missing",
     "contributors": 1, "of": "h_tdigest_signal", "cells": 4}
  ]
  ```

  One record per `(field, reason)`, sorted by that pair. `reason` is one of:

  - `"word-missing"` — a contributor carried the field's §3.3 **divisor**
    digest but not the word. The fold blanks the output cells that
    contributor's rows reach to the fill word (`cells` counts them):
    publishing the surviving siblings' word over an `N_signal` that counts
    rows the word never covered would be a §3.3 skew — absence over
    wrongness;
  - `"divisor-missing"` — a contributor carried the **word** but not its
    `of` digest, so it contributes nothing for the field (the fold's `n`
    inputs are that digest's per-cell weights and are never guessed). This
    is the shape a **mis-declared divisor** leaves at every level above the
    leaves: a manifest naming an `of` whose own entry is `class: "none"`
    (§4.5 — manifests outlive their writer) materializes no divisor array
    in any overview or column, so every contributor fires this arm and the
    field silently never folds upward.

  On `zagg-overview/2` the record survives only a level that materializes.
  The stage fold counts a contributor the rail fired on as `unreadable`, and
  a level where **no** contributor folded cleanly is not written at all — so
  the mis-declared-divisor shape above, which fires on *every* contributor,
  leaves no artifact and therefore no `demotions`. A reader MUST NOT read the
  key's absence at a `/2` level as evidence the rail did not fire there; the
  absent level is the evidence. The `/1` fold paths do not share this: a
  demoted contributor is still a counted leaf (or child), so the level
  materializes and carries the record.

  `field`/`class`/`reason`/`contributors` are always present. A
  **contributor** is one *source read* the fold performed: a `(child,
  window)` staged column on a `/2` merge (so one child missing a half in `W`
  windows counts `W`), a `(leaf, window)` column read on a `/1` fold from
  leaves, and one child overview on a `/1` cascade. `contributors` counts the
  ones this `(field, reason)` fired on, in that unit. `of` names
  the §3.3 linkage; `cells` is keyed whenever the demotion left output cells
  at the fill word that no surviving contributor covers — always in the
  `word-missing` direction, and in the `divisor-missing` direction only where
  contributors own **disjoint** spans of the level (the `/1` cascade of child
  overviews, where a child skipped for the field leaves its whole span fill).
  Where many contributors share each output cell — a `/1` fold from leaves, a
  `/2` stage merge — a dropped contributor blanks nothing and the key is
  absent. A reader MUST NOT read its absence as "nothing was blanked".
  Readers MUST tolerate additional keys, MUST tolerate the key's absence (a
  clean fold, or any pre-#518 artifact — a clean fold's attrs are
  byte-identical to a pre-#518 writer's), and MUST NOT read absence as "no
  demotion ever occurred" on a pre-#518 store. The record is the per-field
  refinement of `source_children`: those counters say a whole contributor
  was unusable, this key says **which field** folded short and **why**. The
  two counters are **not in the same unit** and MUST NOT be compared:
  `source_children` counts **children**, classifying each child once however
  many windows it contributed, while `contributors` counts the per-read
  instances defined above — they coincide only in the single-window,
  one-read-per-child case. A
  demoted field's cells hold the fill value, which §3.2 makes
  byte-indistinguishable from genuine emptiness, so the attrs are the only
  place the distinction can live. Like `fields`, `source_children` and
  `fold_from_order` beside it, a `demotions` state is **as current as the
  artifact's own write** and never a live claim about the manifest in force:
  the skip-if-current gates key on generations and regime, not on the field
  declarations, so after a manifest edit (the mis-declared `of` being
  introduced or corrected — §4.5, manifests outlive their writer) a level
  whose bytes have not changed stays current carrying the previous fold's
  answer. The `demoted/` conformance fixture (§7) commits one fired and one
  clean level.

An overview also carries the standard D4 **commit stamp** as its final
write: an unstamped overview prefix is debris, exactly as for leaves. The
stamp carries the §5.3 `content_hashes` record over the overview's own
arrays when one was computed (keyed only then — absence reads unverifiable,
never tampered). Write order is pinned — template, arrays, `role`/provenance attrs, stamp
LAST — so presence of the stamp certifies the `role` attr landed; a reader
MUST ignore unstamped overview prefixes.

### 4.4 Structure

**Contract.** A pyramid is an ordered list of **level entries**
`{node, cells}` (§4.5): `node` is the hive-tree **ancestor-or-self** order
whose artifact carries the level — one artifact per `(node, window)`, named
by §4.2 — and `cells` the **reader-facing cell resolutions** stored there.
The list is not free-form; it is determined by two declarations and one
law:

- the **leaf entry** — `node == shard_order` (not an ancestor artifact at
  all: the leaf's own level column, #381 point (2)) carries every declared
  leaf resolution, each strictly between `shard_order` and `cell_order`.
  The set of levels whose cells are **at or above the shard order** MUST be
  contiguous from the finest leaf resolution down to the shard order
  (increments of one — `[13, 12, 11, 10, 9]` on the 19/13/9 geometry, never
  `[13, 12, 10, 9]`): that set is the §4.6 column tier, and contiguity
  guarantees the raw-fold boundary member exists whenever anything coarser
  is declared. The requirement scopes to that tier and nothing else:
  levels whose cells are **below** the shard order are the ladder's own,
  DERIVED by the every-order law below, so no gap can arise there to
  permit or forbid;
- the **fixed every-order ladder** — with `d = base - shard_order` (`base`
  the coarsest leaf resolution, so `d >= 1`), every order `k` from
  `shard_order - 1` down to **0 inclusive** carries exactly one member at
  resolution `k + d`. Two numbers — `shard_order` and `d` — determine
  everything above the shard; every store roots at order 0. (Non-normative:
  the ladder is cheap by construction — cell counts shrink 4× per rung, and
  digest bytes are cap-bounded per cell — which is why it is law rather
  than a knob.)

For each member resolution `r` the artifact holds one **resolution group**:
the zarr group named `str(r)`, with the same layout as a source leaf's cell
group. Concretely, for a member `r` at an order-`k` node:

- the group's `morton` coordinate array holds the `4^(r - k)` order-`r`
  words the node covers, in canonical nested order — its **descendant**
  words where `r > k`, its **own** word where `r == k`. For a manifest
  **level member** `r > k` always, by the window and ladder rules above;
  the one recorded `r == k` group is the §4.6 column's **node-order
  member** (the whole-footprint aggregate of #381 point (2)), which is a
  recorded group of that artifact and still never a manifest member. Since
  [issue #538](https://github.com/englacial/zagg/issues/538) it is not the
  ladder's merge source: that is the **relay member** — the leaf column's
  coarsest group still folded from raw: its group at `shard_order + 2`
  (§4.6's raw-fold boundary) when the column CARRIES that group, else the
  node-order member — so that every above-shard merge stays exactly 2
  merges from raw. No separate partial grammar or `partial/` path exists anywhere;
- each **included** field is the same array kind as at the leaves: dense
  fields as dense arrays, digest fields as `zagg-ragged/1` (or `/2`) vlen
  arrays — §1–§3 of this page apply to overview arrays unchanged, **including
  §2.1's float32 exactness bound**: a coarse overview cell can pool more than
  `2^24` observations, and there `sum(weights)` is the nearest float32 to the
  true count rather than the count itself;
- field inclusion is gated by the field's **composability class** (§4.5):
  `exact`, `approximate` and `packed` fields appear, `none` fields are
  **absent**.

Under `zagg-pyramid/1` every artifact holds exactly **one** resolution
group, at the constant depth `k_cell = c - (s - k)` for shard order `s` and
cell order `c` (cells coarsen 4× per order of ascent — the pyramid is the
store's resolution axis, partially materialized): the `/1` grammar is the
special case `cells = [node + (c - s)]`, and §4.1–§4.2 apply to both
revisions unchanged. §4.3's per-artifact `zagg_overview` attrs block — in
particular its single scalar `cell_order = c - (s - k)` — is specified for
`/1`'s single-group artifacts. Stage-written `/2` ladder artifacts (issue
#384) carry attrs revision **`zagg-overview/2`**: the same keys with
`cell_order` the entry's own `cells` member (`k + d`, not the constant-depth
formula), the `fold_source`/`fold_from_order` pair replaced by the #381
point (7) provenance — `regime` (`stage-gather` | `stage-merge`),
`merges_from_raw` (1 for a gather of gen-1 members, 2 for a merge of the
relayed gen-1 relay-member partials — never 3 for an upfront level, which
is why the relay is the boundary member and not the node-order one; gen 3
belongs only to the append-later cascade regime), and `source_children`
(present in both
stage regimes: a gather that under-covers says so exactly like a merge) —
plus `run_id`, the sweep run that wrote the artifact, and a `generation`
block summing the consumed children (the stage skip gate's ratchet key —
`{n_leaves, max_leaf_timestamp, run_ids}`, composition in §4.5). §4.3's
`demotions` key rides `/2` artifacts unchanged (same grammar, same
present-exactly-when-fired rule).
The commit stamp of a stage-written artifact carries the same `run_id` key
(additive to the stamp grammar; fleet-written stamps never carry it, and a
reader treats its absence as "not a stage artifact", never an error): it is
the residual-race backstop of the sweep-admission lease. The `/2` leaf
entry's artifact is the §4.6 column — declared-but-unmaterialized remains
legal (§4.5).

An overview's variable set may therefore be a *subset* of the leaf's —
heterogeneous variable sets across level nodes are in contract, and a reader
MUST NOT assume every leaf field exists at every overview order (the
manifest declaration below is the zero-open way to know).

The fold **regime** (§4.3's `fold_source`) does not enter this contract: a
cascaded level has exactly the layout above. It differs in the values its
`approximate` fields carry and, when it under-covers its subtree, in *which
cells are populated at all* — §4.3's `source_children` is the key that says
so, in every class.

**Accuracy doctrine (`/2`).** For `/2` levels the #376-era "display
artifacts, no precision guarantee past the exact levels" posture is
superseded (espg ruling on the declaring PR): **exact-class** fields are
exactly correct at every order — the reductions are associative, so the
pyramid is a true downsampling pyramid for them — and **approximate-class**
fields are **analysis-grade at their recorded generation**: the per-entry
merges-from-raw count (#381 point (7), recorded in the §4.5 `actuals` and
per artifact in `zagg_overview`) is
the contract a reader holds them to, not a blanket display-only caveat.
The `/1` operational caveats about the deprecated leaves/cascade regimes
(§4.3, §4.5) stay as written for `/1` artifacts.

### 4.5 The manifest `pyramid` block

**Contract.** The product manifest (`morton_hive.json`; manifest bootstrap
semantics: mortie's specification and
[`design/sparse_coverage.md`](design/sparse_coverage.md) §3) declares the
overview family under the versioned `pyramid` block:

```json
"pyramid": {
  "spec": "zagg-pyramid/1",
  "overview": {
    "spacing": 2,
    "orders": [3, 1],
    "all_time": false,
    "fold_source": "cascade",
    "exact_levels": 1,
    "fields": {
      "count":     {"class": "exact", "method": "sum", "nan_policy": "skip",
                    "dtype": "int32", "fill_value": 0},
      "h_tdigest": {"class": "approximate", "method": "tdigest_kway",
                    "dtype": "float32", "inner_shape": [2], "delta": 512},
      "photon_ids": {"class": "none"}
    }
  }
}
```

Under revision `/2` ([#382](https://github.com/englacial/zagg/issues/382))
the schedule is the **block-level `overviews`** list instead — the FULLY
EXPANDED form of §4.4's leaf entry + fixed ladder; `orders` and `spacing`
do not exist in a `/2` block; every other key is unchanged:

```json
"pyramid": {
  "spec": "zagg-pyramid/2",
  "overviews": [
    {"node": 3, "cells": [5, 4]},
    {"node": 2, "cells": [3]},
    {"node": 1, "cells": [2]},
    {"node": 0, "cells": [1]}
  ],
  "overview": {
    "all_time": false,
    "fold_source": "cascade",
    "exact_levels": 1,
    "fields": {"…": "…"}
  }
}
```

The split is deliberate: the block-level `overviews` list is the
**store-wide product declaration** — what fleet writers, stage sweeps,
declare-time forecasts, and external readers consume — while the singular
`overview` dict remains the **overview sweep family's execution regime**
(D22 per-family bookkeeping: `all_time`, the #376 fold keys, `fields`, the
sweep's `materialized` actuals), so the two never re-nest. Per-entry
materialization actuals (#381 point (7)) nest inside the block-level
entries themselves — the `actuals` key below, written by the issue #384
staged sweep's finisher.

- **`orders`** (`/1`) — the ancestor orders that carry overviews
  (descending; empty = pyramid declared off). `spacing` records the schedule
  step (default 2 — the ratified display schedule). Schedules are per
  artifact family and deliberately decoupled from the tree's
  `path_grouping`.
- **`overviews`** (`/2`) — the level entries of §4.4, recorded at **block
  level** (a sibling of `spec` and `overview`, never inside the family
  dict) and always **fully expanded**: the leaf entry first, then the fixed
  every-order ladder down to node 0 — a reader never re-derives the ladder,
  the recorded list IS the contract. Every entry is a `{node, cells}`
  mapping with `cells` a list. The corresponding **config grammar** is leaf
  resolutions only: `output.pyramid.overviews` is an int (sugar for one) or
  a strictly descending list of ints, each **strictly between**
  `shard_order` and `cell_order` (a member at the shard's own order is the
  writer-side aggregate, never declared; a member at the base data's own
  order would *be* the base data), and **consecutive** — the fixed ladder
  fills `[max(shard_order, d), base)` by itself (its coarsest cell is node
  0's, `d = base - shard_order`), so a gap in the list is a gap in the
  §4.4/§4.6 column tier and is refused by name (`missing [11]`). Where the
  ladder's floor is the coarser of the two — `base > 2 * shard_order + 1`,
  so `d` clears `shard_order` by more than one — the tier gaps below it
  whatever the list says, and the refusal names `base` instead; omitted,
  the default is one resolution at the grid's resolved chunk order —
  normative since the issue #384 default flip: a default declaration emits
  this `/2` block for every new store whose resolved chunk order is
  strictly interior (raster configs, explicit legacy `orders`/`spacing`
  schedules, and K == 1 grids keep `/1`), and the worker column gate (§4.6)
  derives the SAME default from the grid, so declaration and artifact can
  never disagree. Consecutiveness is a **writer-side** rule and carries
  **no marker bump** — the `/2` wire grammar is unchanged, only the
  declaration it admits is narrower — so a reader MUST still decode a
  stored declaration whose tier gaps (one written before the rule) by its
  recorded `overviews` list, member for member, exactly as always: such a
  store is not retroactively non-conformant. Only the acceptance harness's
  *declaration* leg reports the gap, and it reports it as a declaration
  finding. There is no
  above-shard configurability: no per-node spelling, no gather declaration,
  no member promotion — the ladder makes every within-footprint member
  spec-guaranteed (espg grammar-collapse ruling on the declaring PR).
- **The declared-off form is smaller, and `orders` is the only key a reader
  may bind unconditionally.** With the pyramid knob off the block is exactly

  ```json
  "pyramid": {"spec": "zagg-pyramid/1", "overview": {"orders": []}}
  ```

  — `spacing`, `all_time`, `fold_source`, `exact_levels`, `fields`, and
  `summarize` are **absent**, not empty. Recording absence never needs the
  new grammar, so the declared-off form is always this `/1` shape — a `/2`
  block's `overviews` list is **never empty**.
  A reader MUST branch on `spec` first, then on the revision's schedule key:
  under `/1` that is `orders` — an empty `orders` (or no `pyramid` block at
  all — pre-pyramid manifests) means no overview family exists and no other
  key of the block may be assumed — and under `/2` it is the block-level
  `overviews`. When the schedule key is non-empty, `all_time` and `fields`
  MUST be present in the `overview` family dict (`spacing` too under `/1`;
  `summarize` stays optional), so the zero-open field query of §4.4 is
  well-defined exactly when there is something to query.
- **`fold_source` / `exact_levels`** — the declared fold regime
  ([#376](https://github.com/englacial/zagg/issues/376)): `"cascade"` (the
  default) folds each declared level from the next **finer** declared level's
  overviews, and `exact_levels` is how many of the finest levels are folded
  from the leaves instead — so under `{"orders": [3, 1], "fold_source":
  "cascade", "exact_levels": 1}` order 3 is exact and order 1 is a fold of
  order 3's folds. `"leaves"` is the deprecated exact-from-leaves regime,
  where every declared level folds the raw leaves and no `exact_levels` key
  is written (every level is exact there, so a boundary would name a
  distinction the store does not have). These two keys are written by
  #376-and-later writers; a reader that finds **no `fold_source`** MUST read
  the declaration as `"leaves"`, which is the only regime that existed
  before. They declare what the **next** sweep will do — what is on disk is
  `materialized.fold_sources` below and, per artifact, §4.3's
  `zagg_overview.fold_source`, which is authoritative for a given overview.
  Under `/2` the pair is declared identically; `exact_levels` counts **level
  entries** from the finest end, exactly as it counts `orders` under `/1`.
  (`exact_levels` predates the `overviews` key-naming convention and is
  deliberately untouched by it — ruled vestigial-in-waiting under the #381
  regime law, which makes the exact/approximate boundary structural; the
  `/2` accuracy contract is §4.4's doctrine, not this pair.)
- **`fields`** — every aggregation field, keyed by name, with its
  **composability class**: `exact` (folds byte-equal — count/sum/min/max),
  `approximate` (t-digest merge — `np.isclose` equality class), `packed`
  (the §3 composition word — deterministic order-independent k-way fold,
  presence exact, counts within one lane quantization per fold), or `none`
  (non-composable). A `"class": "none"` entry is the **recorded absence**
  (the ruled D24 default, option A): the field exists only at native
  resolution, and this declaration is how a reader knows without opening
  anything. A `none` entry carries **`class` only** — no `method`, no
  dtype/shape metadata: there is no fold to name, and stamping a default fold
  method on an excluded field would declare a t-digest array that does not
  exist. `exact`/`approximate`/`packed` entries carry the fold `method`, any
  further fold provenance (an `exact` fold's `nan_policy`), and enough
  dtype/shape metadata to know the overview array's form up front. An `approximate`
  entry MAY additionally carry `overview_delta` — the compression budget
  overview folds run at when it is split from the leaf `delta`
  ([issue #424](https://github.com/englacial/zagg/issues/424); both budgets
  are fold algebra, informative per §2.3) — and, for a non-default §2.0
  declaration, `weights` together with the `gain` calibration provenance
  §2.0 requires beside it (the overview writer reconstructs its arrays from
  this entry alone, so a declaration recorded here without its provenance
  would write an overview whose calibration is unrecoverable); a reader MUST
  tolerate entry keys it does not bind. This map is the
  **all-fields** view; the per-overview `zagg_overview.fields` attrs map
  (§4.3) is the materialized subset.
- **A located ragged field is `approximate`, and its entry carries
  `location`.** A `location:` declaration (§9) does not exclude a field from
  the pyramid: the located k-way merge reduces the `{field}_locations` words
  over the same centroid partition the digest merge produces, so a located
  digest field folds through every level and the class is `approximate` like
  any other digest field ([ruling 4 on
  issue #410](https://github.com/englacial/zagg/issues/410#issuecomment-5310502887)).
  Its entry carries `location` — the source column the leaves' words were
  ingested from — keyed **only when set**, so an unlocated field's entry is
  unchanged. The key is load-bearing for the same reason `weights`/`gain` are:
  the overview writer reconstructs a level's arrays from this entry alone, so
  without it the overview template emits no sibling array and the fold has
  nowhere to write its words. An overview level of a located field therefore
  carries the `{field}_locations` sibling, its §9 `located` declaration, and
  the payload's §1.2 `locations` binding, exactly as a leaf does — with the
  words at the **heterogeneous orders** §9.1 makes normative.
- **A temporal ragged field composes the same way, and its entry carries
  `temporal`.** A **`"per-centroid"`** `temporal:` declaration (§8.3) does
  not exclude a field from the pyramid either: zagg's digest pyramids keep
  the companion `"per-centroid"` at every level, symmetric with the located
  channel — the
  temporal k-way merge reduces the `{field}_times` words over the same
  centroid partition the digest merge produces (espg-ruled 2026-08-17,
  amending [ruling 3 on
  issue #410](https://github.com/englacial/zagg/issues/410#issuecomment-5310502887);
  §8.4 records the amendment and keeps its per-cell reduction licensed but
  unused by this writer), so a temporal digest field folds through every
  level and the class is `approximate` like any other digest field. Its
  entry carries `temporal` — the companion **shape** (`"per-centroid"`),
  which is all a level above the leaf can act on, the leaf's ingest column
  never being re-read by any fold — keyed **only when set**, so a field
  without the companion has an unchanged entry. The key is load-bearing
  exactly as `location` is: the overview writer reconstructs a level's
  arrays from this entry alone, so without it the overview template emits no
  sibling array and the fold has nowhere to write its words. An overview
  level of a temporal field therefore carries the `{field}_times` sibling,
  its §8.3 `temporal` declaration, and the payload's §8.3 `times` binding,
  exactly as a leaf does. The §8.2 dense `"per-cell"` shape is the
  exception: a field declaring it is classed `none` whatever its reducer
  (`zagg.semantics.field_composability`), that shape's fold law being the
  word grammar's join over a cell group rather than the field's own reducer,
  so it exists at native resolution only.
- **A packed composition field is `packed`, and its entry carries `of`.**
  The §3 composition word ([issue
  #515](https://github.com/englacial/zagg/issues/515); espg ruling: admit)
  folds by `method: "composition_kway"` — §3.4's k-way form over
  `(word, n_signal)` pairs, each contributor's `n` taken from its `of`
  digest's per-cell weight. The entry carries the dense `dtype`/`fill_value`
  (a packed field's fill is the `0` word, §3) plus the §3.3 linkage: **`of`**
  — the digest field whose weights are the fold's `n` inputs and whose folded
  weight is the level's `N_signal` — and, when declared, `threshold`. `of` is
  load-bearing exactly as `location` is above: the overview writer
  reconstructs a level's arrays from this entry alone, so without it the fold
  has no divisor and the overview array's §3.3 attrs block could not be
  written. A writer MUST declare a composition field `packed` only when the
  digest `of` names is itself declared `approximate` in the same map **and
  carries the default §2.0 `weights: counts`** — the law divides by that
  digest's weight at every level and `N_signal` is a photon COUNT, so a
  composition whose divisor is excluded from the pyramid, or whose divisor is
  flux-weighted (a different quantity), is declared `{"class": "none"}`
  instead. Readers of an older declaration finding an unknown class token
  MUST treat the field as non-composable for their purposes (fold nothing,
  read native resolution) rather than erroring — which is exactly how
  pre-#515 zagg behaves.
- **`all_time`** — whether the `all.zarr` all-time fold is materialized at
  the declared orders (windowed stores only; a `schedule: none` store's
  single fold is already all-time).
- **`summarize`** (optional) — the opt-in **declared derived summary** for
  `none`-class fields: a mapping from a new, *different* field name to its
  derivation (e.g. an auto-digest of a roster field's raw values), living in
  the pyramid block and **never** in the semantic core — leaf truth is
  unchanged, and overview schema never silently differs from source except
  by declaration. ([Ruled on the #201 thread](https://github.com/englacial/zagg/issues/201#issuecomment-5025509889);
  deterministic seeded subsampling is **deferred as an opt-in phase, not
  rejected** — it shares this block's declaration grammar and stays
  declared/never-default when it lands
  ([option space](https://github.com/englacial/zagg/issues/201#issuecomment-5025519604));
  roster concatenation is rejected.)
- The sweep MAY additionally record materialized-actuals bookkeeping in the
  block; the template-time declaration above is never rewritten by the
  sweep, and the `pyramid` block is excluded from the manifest's frozen
  append-guard keys. zagg's sweep writes

  ```json
  "materialized": {"orders": [3, 1],
                   "fold_sources": {"3": "leaves", "1": "cascade"},
                   "generated_at": "2026-08-04T00:00:00Z"}
  ```

  — the orders it has written and, per level, the §4.3 regime that wrote it
  (keys are the orders as strings, JSON having no integer keys). Both are
  **actuals**, accumulated across sweeps: a level swept under an earlier
  declaration keeps the regime it was made with until it is regenerated, so
  this map can disagree with the declaration above, and that disagreement is
  informative rather than an error. It is a convenience — a reader MAY
  instead open the overviews and read §4.3 — and, like every other actual,
  it says nothing about an overview still being present (overviews are
  regenerable caches, §4.1). On a `/2` store this family-dict `materialized`
  map is the **`/1`-era inventory**, preserved verbatim across a declaration
  revision bump: a retrofit never discards actuals, and the overviews it
  names stay on disk as regenerable-cache debris; the staged sweep never
  writes this family-dict map on a `/2` store — the per-entry `actuals`
  below are the one source of truth for `/2` materialization. `/2`
  materialization actuals nest **inside the level entry that owns them**
  (#381 point (7)), written by the issue #384 finisher's manifest RMW:

  ```json
  {"node": 2, "cells": [3],
   "actuals": {"regime": "stage-merge", "merges_from_raw": 2,
               "source_children": {"folded": 3, "missing": 0, "unreadable": 0},
               "run_id": "stage-20260809T000000Z-ab12cd",
               "generated_at": "2026-08-09T00:00:00+00:00"}}
  ```

  — `regime` is `leaf-column` (the leaf entry: the fleet's own column;
  its `merges_from_raw` is the **maximum** over the entry's declared cells
  of the §4.6 per-group value — 1 when every declared leaf resolution is at
  or above the raw-fold boundary, 2 when the entry declares a resolution
  below it; no `source_children` — its source is complete by construction),
  `stage-gather` (a concatenation of gen-1 members, merges-from-raw 1) or
  `stage-merge` (a k-way fold of the relayed gen-1 relay-member partials,
  §4.4, merges-from-raw 2 — **never 3 for an upfront level**; gen 3 belongs
  only to the append-later cascade regime). `source_children`
  accumulates the run's per-artifact coverage counts; `run_id` names the
  sweep run (stage entries only). The key is **additive**: a reader MUST
  tolerate additional keys on a level entry, and `actuals` says nothing
  about artifacts still being present (overviews are regenerable caches,
  §4.1).
- **The stage skip key** (`generation`, recorded per artifact in §4.4's
  attrs, per stage column in §4.6, and in the sweep-internal envelope) is
  the triple

  ```json
  "generation": {"n_leaves": 16,
                 "max_leaf_timestamp": "2026-08-09T00:00:00+00:00",
                 "run_ids": ["stage-20260809T000000Z-ab12cd"]}
  ```

  — the summed leaf count of the consumed children, the newest child stamp,
  and the **sorted set of `run_id`s** those children's stamps carry, unioned
  with the ids their own recorded blocks relay
  ([#417](https://github.com/englacial/zagg/issues/417)). A level is folded
  again exactly when this triple moves. The third term is load-bearing:
  stamps resolve to **one second**, so the first two alone read a child
  rewritten inside its own recorded second at an unchanged leaf count as
  *current*, and the `/1` content-hash backstop cannot apply without doing
  the fold the skip exists to avoid. A run may not rewrite its own object
  mid-run (single-writer law, §4.8), so a same-second rewrite is a foreign
  run's and moves the set. `run_ids` is **additive**: absent, it MUST be
  read as the empty set (a pre-#417 entry, or children that are all
  fleet-written leaf columns — those stamps carry no `run_id`), never as a
  wildcard that matches any set, so an upgraded store folds once more
  rather than inheriting the blind spot. Fleet-written leaf columns
  contribute no run id, so at the finest tuple the term is empty and the
  gate rests on the pair alone.

### 4.6 Leaf column artifacts (`zagg-column/1`)

**Status: contract — issue [#383](https://github.com/englacial/zagg/issues/383)
(umbrella [#381](https://github.com/englacial/zagg/issues/381) points
(1)–(3)).** A **column artifact** is the leaf worker's own pyramid
contribution, written at aggregation time while the shard's cell data is
resident: one zarr per `(leaf, window)`, a **sibling of the leaf under the
leaf's own node prefix**. A column exists exactly when the **writing run's**
pyramid declaration is `zagg-pyramid/2` (§4.5), carries leaf-node levels —
the expanded `overviews` list always places the declared resolutions at the
shard node — **and declares at least one composable field** (§4.5): a
declaration whose fields are all `none`-class writes **no artifact at all**
(not a morton-only column), and clears any prior one exactly as the
no-levels arm below does. When it does exist it is written by the same
worker invocation that commits the leaf, after the leaf's own stamp. The gate is the config the unit carries, not
a store read: workers never open the manifest, so the manifest's `pyramid`
block is the **reader- and sweep-facing** declaration and MAY lag a
config-only change until the store is re-templated or retrofitted
(`ensure_manifest` deliberately excludes `pyramid` from the keys it freezes,
so a re-run into an existing store never re-PUTs it). A reader that needs to
know which leaves actually carry columns therefore reads the columns, not the
manifest. A run whose declaration carries no leaf-node levels **deletes** any
column and sidecar a previous declaration left at that `(leaf, window)`, so a
column never outlives the declaration that wrote it.
Like overviews, columns are derived artifacts a reader MUST NOT require;
unlike overviews they are **not** regenerated by a rollup sweep — the writer
of a column is its leaf's worker (no locking anywhere), and repair is
re-invoking the idempotent leaf, never a sweep-side fold from raw cells
during a rollup pass.

The **one** sanctioned second writer is the `/1 -> /2` **column backfill**
([#520](https://github.com/englacial/zagg/issues/520)): an explicit upgrade
pass that recomputes a committed leaf's column from that leaf's own **stored
arrays**, for stores built before their fields were declared composable —
which is what lets a published store take the `/2` staged sweep without
re-aggregation. It writes the same artifact under the same discipline, so it
inherits the single-writer law rather than repealing it, under two conditions
a reader MAY assume of any store:

- it is serialized against every other sweep by the §4.8 admission lease,
  held for the whole pass and heartbeated on the **wall clock** (a beat lands
  within a fixed fraction of `ttl_s` of the last one, however long a single
  leaf takes). §4.8's expiry rule is the residual and is not repealed here: a
  holder that stalls past its `ttl_s` without beating is claimable like any
  other, so what a reader MAY assume is the lease's guarantee, not a stronger
  one; and
- it MUST NOT run while an aggregation run may write the same
  `(leaf, window)`. This is the one pass for which §4.8's fleet ∥ sweep
  disjointness does not hold, and no control-plane object can enforce it: it
  is an operator precondition, and running a backfill against a live fleet
  risks a chimera column exactly as two concurrent sweeps would.

The backfill changes no byte of this format. A backfilled column is
byte-identical to the one the leaf's worker would have written from the same
leaf — the two provenance timestamps (`zagg_column.generated_at` and the
stamp's `written_at`) aside — and it is written only where the manifest's own
`/2` declaration (§4.5) carries leaf-node levels **and** at least one
composable field; a store still declaring `/1`, declared off, or `class:
"none"` on every field is refused and MUST be re-declared first, never
guessed at.

- **Naming.** One column per `(node, window)`, and its basename MUST be
  `{window stem}.pyramid.zarr` — the stem derived from the D23 **window
  alone** (the §4.2 overview dialect), never from the leaf's own basename
  stem, so the rule is independent of the store's leaf-naming revision: the
  unwindowed / `schedule: none` leaf takes the reserved token (§4.2), giving
  the §7 `column/` fixture's committed pair `11213.zarr` and
  `all.pyramid.zarr` side by side, and a `/1` windowed leaf
  `11213_2019.zarr` is paired with `2019.pyramid.zarr`. The `.pyramid.zarr`
  suffix is the one name seam, and it is **normative** for name-grammar
  consumers (e.g. the root-MOC walker): a basename ending in `.pyramid.zarr`
  MUST NOT be read as a leaf or an overview. The seam is unambiguous because
  the frozen D23 window-label charset `[0-9A-Za-z-]{1,32}` (the grammar §4.2
  inherits, generative labels being digits) admits no `.`,
  so no legitimate leaf or overview basename can end that way; classification
  for everything else is the attrs below.
- **Structure.** One zarr group per **resolution group**, named by its cell
  order (the `{order}/{field}` layout of §4.4): every declared leaf
  resolution, every within-footprint rung of the fixed ladder
  (`node < cells ≤ base`), and the **node-order member** — `cells == node`,
  one cell: the leaf's whole-footprint aggregate. This tier MUST be
  contiguous from the finest declared resolution down to the node order
  (§4.4's contiguity rule; only the stage-merge levels below the shard
  order MAY gap). (There is no `partial/`
  grammar; a coarse level declared later never rewrites a leaf; the ladder's
  merge source is the relay member named under **Fold laws**, not this
  group). Each group holds the `morton`
  coordinate (the node's order-`r` descendant words, ascending) and one
  array per **composable** field (§4.5 classes; `none` fields are absent),
  plus **every channel sibling** that field's §4.5 entry declares — the
  `{field}_locations` sibling for a `location` entry (carrying the §9
  declaration), the `{field}_times` sibling for a `temporal` entry (carrying
  the §8.3 declaration) — each row-aligned with its payload, exactly as an
  overview level does. Each pair is written together or not at all: the
  words are exact only *given* the centroid partition the payload describes
  (§9.1, §8.3), so a group holding a populated payload against an empty
  sibling is non-conformant, not merely short.
  A group's arrays are **single-chunk and unsharded** — `chunk_shape` equals
  `shape` (`4^(r - node)` cells), no `sharding_indexed` codec — whatever the
  leaf's own `chunk_inner`/sharding: a column group is small by construction,
  and a reader sizes its GETs accordingly (§1.5's per-inner-chunk geometry,
  read from the array metadata as §1.5 requires).
  Under the default `[chunk_inner]` declaration on the 19/13/9 reference
  geometry a column carries groups {13, 12, 11, 10, 9}.
- **Fold laws.** The leaf tier has a **raw-fold boundary** at resolution
  `order + 2` (the constant `zagg.column.RAW_MEMBER_DEPTH = 2`; a constant,
  never a per-store knob —
  [issue #538](https://github.com/englacial/zagg/issues/538)). Groups at or
  above it (`cells ≥ order + 2`) fold **directly from the leaf's resident
  cell data** — exact classes by their §4.5 merge law (nan-skipping, §4.3),
  approximate classes by the order-independent k-way digest merge, packed
  classes by §3.4 — and record `merges_from_raw` 1. Column bytes there MUST
  equal the sweep-kernel fold of the committed leaf's arrays at that
  resolution — the from-leaves parity contract, which reads differently per
  class: for **exact** fields it is checkable from this page (the §4.5
  merge law plus §4.3's nan policy, so an external reader can reproduce a
  group by direct aggregation), while for **approximate** fields the merge
  algebra is zagg-owned and deliberately unspecified (§2.3), so the MUST
  binds implementations sharing those kernels and is pinned on committed
  bytes by the §7 `column/` fixture rather than derivable from spec text.
  The groups **below** the boundary (`order + 1` and `order`) fold **flat
  from the boundary group**: one k-way merge per output cell over the
  `4^(order + 2 − r)` boundary cells it contains (approximate payloads with
  their §9/§8.3 channels in the same call; packed words from the boundary
  group's `(word, n)` pairs, `n` the boundary `of` digest's weight) — never
  chained through the intermediate group — and record `merges_from_raw` 2.
  Their bytes MUST equal that flat fold of the column's own boundary group.
  **Exact** classes are unaffected in value (they reduce from raw at every
  group; the reductions are associative). Rationale: a from-raw fold of the
  member `d` orders above the node merges `1/4^d` of the shard's centroid
  rows in one call, and at the node member that is the whole shard — ~100 B
  of peak memory per row, the 0.52 fleet's OOMs at 4 GB — whereas the
  boundary bounds any single merge to **one boundary cell** — a sixteenth
  of the shard only under uniform occupancy, and the bound is the cell, not
  the fraction. Measured across the 39 fattest shards of the CA ATL03
  store: the largest res-(order+2) cell holds 6.04 M rows, 9.8% of its
  61.7 M-row shard, with a p50 of 9.6% and a maximum of 14.0% — ~0.6 GB at
  ~100 B per row, against the 4 GB worker tier. When the
  boundary is not strictly below the cell order (`order + 2 ≥ cell order`),
  or the column carries **no group at `order + 2`** (a finest declared
  resolution of `order + 1`; under the §4.4 contiguity rule a validated
  declaration cannot skip the boundary while declaring something coarser —
  `[12, 10]` on the 19/13/9 geometry, members {12, 10, 9}, is refused, though
  a hand-built manifest may still carry it and membership stays the
  predicate), every group folds from raw with `merges_from_raw` 1 — the
  §7 `column/` fixture's 4/6 geometry is the first case, which is why its
  groups below record 1.
- **The `role` and `zagg_column` attrs.** `role` is `"column"`;
  `zagg_column` is the versioned provenance block, present exactly when
  `role` is `"column"`:

```json
"zagg_column": {
  "spec": "zagg-column/1",
  "node": "11213",
  "order": 4,
  "source_cell_order": 6,
  "window": "all",
  "fields": {"count": {"class": "exact", "method": "sum", "nan_policy": "skip"},
             "h_tdigest": {"class": "approximate", "method": "tdigest_kway",
                            "delta": 16, "overview_delta": 16,
                            "dtype": "float32", "inner_shape": [2]}},
  "groups": {"5": {"regime": "leaf-column", "merges_from_raw": 1, "n_cells": 4},
             "4": {"regime": "leaf-column", "merges_from_raw": 1, "n_cells": 1}},
  "cells_with_data_order": 5,
  "generated_at": "2026-08-05T00:00:00+00:00"
}
```

  `node` is the leaf's morton decimal and `order` its (shard) order;
  `source_cell_order` the leaf's own cell order; `window` the §4.2 window
  key (`"all"` unwindowed — the basename and this key round-trip);
  `cells_with_data_order` names the group whose populated-cell count the
  commit stamp's `cells_with_data` records (the finest group).
  `fields` follows §4.3's materialized-fields contract (approximate entries
  additionally carry `dtype`/`inner_shape`/`delta` — enough to decode
  without the manifest — and `overview_delta`, the budget this column's fold
  actually compressed at, which is the split pyramid-fold budget rather than
  the leaf `delta` ([issue #424](https://github.com/englacial/zagg/issues/424);
  both are fold algebra, informative per §2.3). A reader MUST tolerate entry
  keys it does not bind, exactly as in §4.5). `groups` carries the per-group
  provenance slots:
  the fold **regime** (`"leaf-column"` — folded from the leaf's own
  resident cells; `source_children` never rides this regime, its source is
  complete by construction), the `merges_from_raw` integer (1 at or above
  the raw-fold boundary, 2 below it — on the 19/13/9 reference geometry
  `{"13": 1, "12": 1, "11": 1, "10": 2, "9": 2}`; the example above is the
  4/6 fixture geometry, whose boundary is the cell order), and `n_cells`
  — the group's **grid** size `4^(r - order)`, i.e. its arrays' length, not
  its populated-cell count (that is the stamp's `cells_with_data`, for the
  `cells_with_data_order` group only). `n_cells` is derivable and recorded
  as a convenience.
- **Write discipline.** The leaf's own D4 order: template (wholesale — the
  column prefix, and any stale stats sidecar, are deleted first) → every
  group's arrays → `role`/`zagg_column` attrs → **one commit stamp last
  covering the whole column**. The stamp is the D15 `morton_hive_commit`
  root-attrs block, the same key and grammar a leaf carries:

```json
"morton_hive_commit": {"spec": "morton-hive/1", "complete": true,
                       "cells_with_data": 3, "granule_count": 1,
                       "content_hashes": {"arrays": {"…": "…"}, "combined": "…"},
                       "written_at": "2026-08-05T00:00:00+00:00"}
```

  `cells_with_data` is the populated-cell count of the group named by
  `cells_with_data_order`; `granule_count` is the **leaf's** granule count,
  not a column quantity; `content_hashes` is the §5.3 record over the
  column's own arrays, keyed only when one was computed; and a column stamp
  carries **no `coverage` payload** (a leaf's does), so a stamp reader MUST
  NOT require one. On a
  **windowed** store the column's stamp is `spec: "morton-hive/2"` and
  carries the D15 half exactly as the leaf's does — `window` plus the
  observed `time_range` — so a reader that strict-checks the `spec` marker
  must accept both revisions here. An unstamped column prefix is debris and
  MUST be ignored; an idempotent re-run rewrites the whole column to the
  same array bytes (provenance timestamps move). The D20 stats sidecar —
  `{stem}.stats.json`, e.g. `all.pyramid.stats.json`, carrying the §5
  record for the column's own arrays — is a sibling object PUT **after**
  the stamp, fail-open: absence reads unverifiable, never tampered.
- **Failure identity.** A column-write failure fails the worker unit; the
  retry rewrites leaf and column wholesale. A committed leaf whose column
  is absent or unstamped therefore reads as **one of three** things: a torn
  worker; a leaf whose writing declaration carried no column (the gate and
  the clear above); or — since the backfill sanctioned above — a leaf that
  a `/1 -> /2` upgrade pass has not reached, or failed on. The manifest
  cannot always separate them, since its `pyramid` block MAY lag, and after
  a re-declaration it necessarily leads the leaves. Readers never require a
  column, so absence is never an error state; the three differ only in
  their **repair**:
  - where the **writing** declaration is known to carry leaf-node levels,
    absence is the torn-worker signature and the repair is re-invoking the
    idempotent leaf;
  - where the leaf-node levels arrived by RE-DECLARATION — the `/1 -> /2`
    upgrade, whose whole point is that the leaf is not rewritten — absence
    is the backfill's signature instead, and the repair is re-running the
    idempotent backfill. Re-invoking the leaf would re-aggregate from
    source granules, the one cost the upgrade exists to avoid;
  - and a leaf that predates any column declaration needs no repair at all.

  A backfill counts a leaf it cannot read or fold and carries on, so a
  partially-completed pass leaves exactly this state on the store; what
  distinguishes it is the pass's own summary (`failed`), not the artifacts.

**Stage columns (issue #384).** The staged sweep writes the SAME artifact
shape at its dispatch nodes (`{window}.pyramid.zarr` under an ancestor
node's prefix, `zagg-column/1` attrs, D4 order, one commit stamp last, D20
sidecar after): every group is a **pure gather** of the child columns'
members at the same resolution — `groups` entries record `regime:
"stage-gather"` with `merges_from_raw: 1` — and the artifact MUST carry the
**relay member** (the leaf columns' coarsest from-raw group for the whole
subtree — `shard_order + 2`, the raw-fold-boundary partials, when the
columns carry that group, else the node-order partials — the merge-source
tier every coarser merge consumes: the espg
merge-source ruling on the #384 thread, re-based onto the boundary member
by issue #538 so that no upfront merge is ever 3 from raw). Stage-column
attrs additionally
carry `generation` (`{n_leaves, max_leaf_timestamp, run_ids}` summed over
the consumed children — the parent's skip-gate key, §4.5), `source_children` (a
gather that under-covered says so in the artifact), and `run_id`; the
commit stamp carries `run_id` too. Cadence decides placement (columns sit
at dispatch orders), so column EXISTENCE at a given ancestor order is
orchestration, never contract — a reader binds to the ladder artifacts of
§4.4, not to stage columns. The root tuple writes no column (nothing
consumes it). Raster hive stores are column-less by construction: nothing
in this section applies to them (issue #399 owns their overview regime).

### 4.7 What §4 does not cover (informative)

The sweep's *other* derived-artifact families (MOC regeneration, stats and
sub-shardmap rollups, debris collection) are operational concerns recorded
in [`design/sparse_coverage.md`](design/sparse_coverage.md) D22 — they add
no new byte layouts (the stats rollup reuses the D20 sidecar schema; the
sub-shardmap is ShardMap JSON) — except the §4.8 sweep-admission lease,
which is control plane rather than data. The fold *algebra* for overview
contents is zagg-owned per §2.3; a reader consumes overview arrays exactly
as it consumes leaf arrays.

### 4.8 The sweep-admission lease (`zagg-sweep-lease/1`)

**Status: contract — issue #384 (espg admission ruling).** Pyramid sweeps
**serialize per store**: a column is a multi-object artifact whose D4
stamp-last discipline proves completeness only under a single writer, so
two concurrent sweeps could interleave PUTs into a *chimera* column that
validates as complete. Admission is one atomic conditional PUT
(`If-None-Match: *`) of the store-root **intent object** `sweep.lease.json`:

```json
{
  "spec": "zagg-sweep-lease/1",
  "run_id": "stage-20260809T000000Z-ab12cd",
  "scope": null,
  "acquired_at": "2026-08-09T00:00:00+00:00",
  "heartbeat_at": "2026-08-09T00:05:00+00:00",
  "ttl_s": 900,
  "claimed_from": "stage-20260808T230000Z-9f00aa"
}
```

— `scope` is the admitted run's node-prefix set (informative; the lease is
**store-granular by correctness**: scope-disjointness does not imply
write-disjointness, because disjoint-leaf sweeps converge on shared coarse
ancestors). A live intent refuses admission naming the runner; a
`heartbeat_at` older than `ttl_s` is **claimable** (crash recovery —
`claimed_from` records the takeover), and the claimant simply completes the
partial prior run under the ratchet. The finisher deletes the intent as its
final act. **Control plane, explicitly**: no data object is ever locked —
the lease is what makes "every data object has exactly one writer, ever"
true *across* runs, extending (never amending) the no-locking law. Fleets
are unaffected: fleet ∥ fleet is governed by the leaf single-writer law and
fleet ∥ sweep is allowed (the stage workers validate every column stamp
before and after reading its groups and re-read on movement; stage stamps
carry `run_id`, and a skip-if-current read that sees a foreign stamp
written after the run started aborts loudly). The same `run_id` is a **term
of the skip key** (§4.5): the abort covers a foreign stamp written *since
this run started*, and the key covers the foreign rewrite that landed
before it — inside the second the timestamp cannot resolve.

### 4.9 The `multiscales` discovery mirror (`zagg-multiscales/1`)

**Status: contract — issue
[#392](https://github.com/englacial/zagg/issues/392) (espg re-scope ruling
2026-09-10: zagg-native convention metadata only).** A `zagg-pyramid/2`
store is inherently a multiresolution grid (§4 intro), but that identity is
discoverable only through §4.5's own grammar. The manifest therefore ALSO
carries a **machine-readable convention declaration** of the ladder — the
OME/zarr-style multiscales attr grammar adapted to the node-tree reality —
so a reader discovers the whole ladder from **one metadata read**
(`morton_hive.json`).

**Contract.** The manifest key `multiscales` (a top-level sibling of
`pyramid`) is present **exactly when** the manifest's `pyramid` block
declares `zagg-pyramid/2`: a `/1`, declared-off, or pre-pyramid manifest
carries no `multiscales` key, and its absence means *pre-convention (or
single-resolution) store* — never an error, and no `spec` marker bump
anywhere. The value is a **list of one** multiscale object (the OME-style
shape: one declared product, one entry):

```json
"multiscales": [
  {
    "spec": "zagg-multiscales/1",
    "name": "SPEC_FIXTURE",
    "base": {"order": 3, "cells": [6]},
    "datasets": [
      {"order": 3, "cells": [5, 4], "artifact": "column"},
      {"order": 2, "cells": [3], "artifact": "overview"},
      {"order": 1, "cells": [2], "artifact": "overview"},
      {"order": 0, "cells": [1], "artifact": "overview"}
    ],
    "order2res": {"3": [5, 4], "2": [3], "1": [2], "0": [1]},
    "fields": {"count": "exact", "h_min": "exact",
               "h_tdigest": "approximate", "h_mean": "none"},
    "fold": {"fold_source": "cascade", "exact_levels": 1}
  }
]
```

- **`spec`** — the revision marker; conformance rule as everywhere
  (strict-check, fail loudly on an unknown revision).
- **`name`** — the manifest's `dataset.short_name` (`null` when the
  manifest records none): identity, not grammar.
- **`datasets`** — one entry per §4.4 level entry, **finest first**, in the
  recorded `pyramid.overviews` order: `order` and `cells` are copied
  **verbatim** from the level entry (`cells` keeps §4.5's vocabulary — the
  reader-facing cell resolutions stored at that node order), and
  `artifact` names the kind that carries the level — `"column"` for the
  leaf entry (`node == shard_order`, the §4.6 per-leaf
  `{window}.pyramid.zarr`), `"overview"` for every ladder entry (the §4.1
  ancestor-node `{window}.zarr`). Artifact *paths* are not recorded here:
  node addressing stays the manifest + mortie hive-path grammar, and
  declared-but-unmaterialized remains legal (§4.5), so a path list would
  claim presence this block cannot promise.
- **`order2res`** — the flat per-order lookup `{str(order): cells}` (JSON
  has no integer keys): the cell resolutions of the **pyramid levels** at
  order k, whose key set is exactly the orders present. It is a projection
  of `datasets` and MUST agree with it entry for entry. It does **not**
  carry the base's native `cells`: those ride in `base` alone, which shares
  the shard order's key (in the example above, `order2res["3"]` is
  `[5, 4]` while `base` adds 6 at that same order 3). So the resolutions
  readable at order k are `order2res[str(k)]` **unioned with**
  `base["cells"]` when `k == base["order"]` — a reader assembling a
  complete per-order map MUST union the base in, never overwrite the entry
  with it.
- **`base`** — the native source data (`{"order": shard_order, "cells":
  [cell_order]}`): the finest rung of the resolution ladder a reader picks
  from, deliberately **not** a `datasets` entry — it is not a pyramid level
  (§4.5: a member at the base data's own order would *be* the base data).
- **`fields`** — `{name: class}` projected from the §4.5 D24 map: the
  zero-open composability answer (which fields exist at every ladder order,
  which are exact there, which exist only at native resolution). The class
  tokens and their meaning are §4.5's; the accuracy contract per class is
  §4.4's doctrine.
- **`fold`** — the declared fold provenance: `fold_source` always, and
  `exact_levels` exactly when the `pyramid` family dict declares it (§4.5's
  presence rule — the deprecated `leaves` regime records no boundary).

**Derivation and precedence.** The mirror is **derived, never
authoritative**: zagg computes it from the `pyramid` block in the same
write that installs that block (`hive.build_manifest` at template time;
`declare_pyramid` on retrofit, which also installs the mirror on an
already-`/2` store whose declaration predates this section, and removes it
when a retrofit leaves `/2`). On ANY disagreement between this block and
the `pyramid` block, **the `pyramid` block wins** — a reader that consumes
the mirror MAY cross-check it against `pyramid.overviews` and MUST resolve
a conflict in the `pyramid` block's favor. Sweep actuals never enter the
mirror: materialization truth stays in §4.5's `actuals`/`materialized`
bookkeeping and §4.3's per-artifact attrs. A reader MUST tolerate
additional keys on the multiscale object and on its entries (additive
grammar, same rule as §4.5).

### 4.10 The multiscales companion group

**Status: contract — issue
[#394](https://github.com/englacial/zagg/issues/394) (espg re-scope ruling
2026-09-10: zagg-native, references only — never data copies).** The §4.9
mirror makes the ladder *discoverable*; the **companion group** makes it
*walkable by stock zarr tooling*: one literal zarr v3 group hierarchy at
the reserved store-root path **`multiscales/`**, mirroring the **coarse
ladder** — one child group per §4.4 ladder order, `shard_order - 1` down
to 0 — that a plain zarr reader opens with zero custom code. It holds
**metadata only**: group documents and attrs. No arrays and no data bytes
— every entry *references* node artifacts by store-root-relative path.

**Contract.**

- **Layout.** `multiscales/zarr.json` is a zarr v3 group document whose
  `attributes` carry the §4.9 `multiscales` list (derived fresh from the
  manifest's `pyramid` block at write time — the block wins over any
  recorded copy, §4.9) plus the `zagg_multiscales` provenance stamp, and
  whose `consolidated_metadata` block (`kind: "inline"`, `must_understand:
  false`) inlines every child group document identically **as JSON** — the
  inlined copy and the standalone `multiscales/{order}/zarr.json` decode to
  equal documents, not to equal bytes (the nested copy is indented deeper),
  so one GET of that object walks the whole tree. Each ladder order additionally has
  its own `multiscales/{order}/zarr.json` group document, so
  non-consolidated walkers work too. Write order is child documents first,
  the root document LAST — the root doc is the commit marker; a prefix
  without it is debris.
- **The root stamp** —

  ```json
  "zagg_multiscales": {
    "spec": "zagg-multiscales/1",
    "window": "all",
    "members_source": "coverage.moc",
    "generated_at": "2026-09-11T00:00:00+00:00",
    "members_generated_at": "2026-09-10T23:12:04+00:00"
  }
  ```

  `window` is the §4.2 window token the member references resolve —
  **`"all"` only in this revision**: the companion mirrors the all-time
  fold (per-window mirrors are a declared non-goal of `/1`, issue #394
  decision (2)). `members_source` records where the member node set came
  from: `"coverage.moc"` (the root MOC, one GET) or `"run-records"` (the
  D22 discovery fallback). `generated_at` is when the companion was
  WRITTEN; `members_generated_at` is the age of the INPUT the member sets
  were derived from — the root MOC envelope's own `generated_at`, present
  exactly when `members_source` is `"coverage.moc"` and that envelope
  carries the field. The two together are the staleness discriminator (see
  below). A reader MUST tolerate its absence (a `"run-records"` companion,
  or a MOC that carries no stamp).
- **Per-order groups** — the child group named `str(order)` carries

  ```json
  "zagg_multiscales_level": {
    "spec": "zagg-multiscales/1",
    "order": 2, "cells": [3], "artifact": "overview", "window": "all",
    "members": {"-311": "-3/1/1/all.zarr/3"}
  }
  ```

  `order`/`cells`/`artifact` are the §4.9 dataset entry verbatim;
  `members` maps each **node decimal** the ladder owns at this order —
  the order-`k` ancestors of the store's occupied shards — to the
  **store-root-relative path of that node's resolution group**:
  `{hive_path(node)}/all.zarr/{cells[0]}` (path grammar: mortie's
  hive-path convention; a ladder entry has exactly one member resolution,
  §4.4). The leaf entry and the native resolution are deliberately **not**
  mirrored: their artifact count is the shard count, which is exactly the
  scale the hive grammar exists to handle — the companion is the coarse,
  cap-bounded tier (issue #394 decision (1)); both remain declared in the
  root attrs' §4.9 list.
- **References claim ownership, never presence.** A member names where the
  node's overview artifact lives **when materialized**; overviews are
  regenerable caches (§4.1) and declared-but-unswept is legal (§4.5), so a
  reader following a member path MUST treat an absent or unstamped
  artifact as "not materialized", never as a broken store. An empty
  `members` map (a declared-but-unoccupied store) is likewise legal.
- **Presence and gating.** The companion is written for a `/2` store whose
  all-time fold exists by declaration: an unwindowed store always (its
  single fold IS all-time, §4.2), a windowed store only when
  `pyramid.overview.all_time` is true. Absence of the whole group is
  always legal (a pre-convention store, or a gated windowed store). The
  name `multiscales` is **reserved** at the store root: it is excluded
  from the D19 product-name grammar (like the base-component exclusion) so
  a multi-product root walker can never classify it as a product. The
  reservation is retroactive on a store predating it that holds a product
  literally named `multiscales`: that product is no longer discoverable or
  addressable and MUST be renamed — but the exclusion is **reported, not
  silent** (a product carries a `morton_hive.json` at its root and the
  companion group never does, so discovery discriminates the two and warns
  on the collision).
- **Writers.** The staged sweep's designated finisher refreshes the
  companion after its manifest RMW, gated on the run having touched shards
  and **fail-open** (the finisher's load-bearing steps never fail on it);
  `python -m zagg.sweep <root> --write-multiscales` writes/refreshes it
  standalone. Single-writer discipline is inherited from the finisher's
  own (§4.8: one finisher per admitted run).
- **Removal.** A store that stops declaring `/2` MUST NOT keep a readable
  companion: the group asserts the ladder *without* consulting the
  manifest, so the §4.9 precedence rule has no reader to apply. The same
  write that drops the §4.9 mirror (`declare_pyramid`, on a retrofit that
  leaves `/2`) therefore deletes `multiscales/zarr.json` — the commit
  marker, so removing it alone un-commits the tree and any surviving child
  prefix is debris a later companion write overwrites. The delete is
  best-effort (the manifest is truth and is already written); a failure is
  logged, not unwound.
- **Normativity and staleness.** The manifest + hive remain truth; the
  companion is a **recorded mirror** (D9 cache class): regenerable at any
  time, self-healing on the sweep ratchet (the finisher rewrites it each
  admitted run), and stale-detectable — but the test is
  `members_generated_at`, not `generated_at`: the write-time stamp only
  says when the mirror was last rewritten, so a companion derived from a
  stale root MOC carries a *fresh* `generated_at`. A
  `members_generated_at` older than the manifest's pyramid declaration or
  the newest §4.5 `actuals` means the member sets may lag store truth; its
  absence means the input age is unknown and the member sets carry no
  freshness proof at all. On any disagreement the manifest wins,
  exactly as §4.9. A reader MUST tolerate additional keys in both attrs
  blocks.

## 5. O11 content hashes

**Status: contract — frozen on
[#342](https://github.com/englacial/zagg/issues/342).** The recipe was
pinned by the moczarr verify reader
([espg/moczarr PR #23](https://github.com/espg/moczarr/pull/23),
`moczarr.stats.hash_arrays` / `combined_hash`); zagg's writer, when it lands
(#342), MUST adopt it verbatim. The O11 decision record (scope, compute-at-
write, exact-bytes rationale) is
[`design/sparse_coverage.md`](design/sparse_coverage.md) §8.2 O11.

The **logical content hash** of a leaf is per-array sha256 over *decoded*
values — never stored object bytes, so codec and packaging changes
(ShardingCodec inner chunks, compressor upgrades, §1.5 geometry) are
invisible by construction, while any value change flips the hash (exact
bytes, no float tolerance — interpretation pairs the hash with the recorded
zagg version).

### 5.1 Scope and keys

**Contract.** The hash set covers **every named zarr array beneath the leaf
root** — data fields, the ragged vlen payload arrays and their
`{field}_locations` siblings, `morton`, every coordinate — keyed by the
array's **path relative to the leaf root** (e.g. `"8/morton"`).

The scope is therefore **discovery-based**: both shipped implementations
enumerate (`group.members(max_depth=None)`), so the key set is whatever named
arrays exist under the leaf root, not what the template declared. That has
one normative consequence a verifier MUST honour: **a key-set difference is a
distinct outcome from a digest mismatch.** Debris inside a leaf — a foreign
array prefix, the [issue #341](https://github.com/englacial/zagg/issues/341)
Bug A class, an
[issue #327](https://github.com/englacial/zagg/issues/327) `.zarr.status/`
prefix — adds a key and so changes `combined`, which means a verifier
comparing `combined` alone reports an intact leaf as tampered. A verifier MUST
compare the per-array map first and report extra or missing keys as their own
outcome ("extra array present" / "array missing"), reserving "mismatch" for a
differing digest on a **shared** key. Hashing is the only path that
enumerates: the read and fold paths open leaf arrays **by name** (no member
enumeration, no LIST — [#344](https://github.com/englacial/zagg/pull/344)), so
debris is inert everywhere else.

### 5.2 Per-array recipe

**Contract.**

- **Fixed-width arrays**: sha256 over the array's full decoded contents as
  raw **C-order little-endian** bytes at the declared dtype (a big-endian
  dtype is byte-swapped to little-endian before hashing).
- **Vlen (ragged) arrays**: an object-dtype array has no flat buffer
  (`ndarray.tobytes()` on it would serialize per-process pointer
  addresses). It is hashed instead as, over cells in flat C order:

```text
sha256( for each cell:  u64_le(len(payload)) || payload )
```

  where `payload` is the cell's decoded bytes — exactly the §1.4
  `payload_bytes` for a `zagg-ragged/1` array (an empty or unwritten cell
  contributes its zero length; a locations sibling's payload is its raw
  little-endian `uint64` words). The 8-byte length prefix is what makes the
  digest injective (`[b"ab", b"c"]` and `[b"a", b"bc"]` must not collide),
  and it covers the cell *grid*, not just the payloads.

  The **element → bytes** normalization is itself normative, because a `/2`
  (§6) cell decodes to an ndarray rather than to bytes:

  | decoded element | payload bytes |
  |---|---|
  | `None` — an unwritten vlen cell may decode as `None`, not `b""` | zero-length |
  | `bytes` / `bytearray` / `memoryview` (a `/1` cell) | as-is |
  | `str` (a vlen-utf8 future) | UTF-8 encoded |
  | ndarray (a typed `/2` cell) | **C-contiguous, little-endian** bytes at the declared element dtype |
  | anything else | the recipe **does not apply**: a verifier MUST raise rather than hash |

  The last row is deliberate: a digest that is silently wrong is worse than no
  digest, so hashing a `repr` or a pointer buffer is forbidden. `None` and
  `b""` hash identically by construction — a `b""` fill is distinguishable
  from a missing cell only by position, which is the intent.

### 5.3 Combined hash and sidecar record

**Contract.** The combined hash is sha256 over the **sorted** per-array hex
digests joined by `"\n"`, hashed as ASCII — array names deliberately
excluded ("hash of the sorted per-array hashes").

*(Informative.)* Because it sorts the *digests*, `combined` is immune to the
order in which the §5.1 enumeration happened to yield arrays — a real
robustness property, and the reason two implementations agree without agreeing
on traversal order. The recorded `arrays` map is a different matter: a writer
SHOULD record it key-sorted so a regenerated record diffs cleanly.

The hashes are recorded in the leaf's **commit stamp** (`morton_hive_commit`,
[issue #580](https://github.com/englacial/zagg/issues/580)) and in its D20
stats sidecar, both under `content_hashes`, in the structured shape:

```json
"content_hashes": {
  "arrays": {"8/count": "…", "8/h_tdigest": "…", "8/morton": "…"},
  "combined": "…"
}
```

A writer MUST emit the structured shape. A reader SHOULD also accept the
flat shape (`{array_key: hash, "combined": hash}` — `combined` is reserved
and is not a legal zagg array name). A leaf with no recorded
`content_hashes` is **unverifiable, not tampered**: verification MUST
report "nothing recorded" as a distinct outcome from a mismatch (the
conservative dedup posture — an unverifiable leaf is never a hit).

**Contract (the stamp copy).** The writer computes the record from the
arrays it just wrote, *before* the stamp lands, so the D4 seal certifies the
digest of the bytes it seals; the stamp copy and the sidecar copy are the
same record. A reader verifying a leaf SHOULD prefer the stamp's copy (one
root-metadata GET, already read for the stamp) and fall back to the sidecar
for leaves written before this key existed. A stamp without the key is a
pre-#580 writer or a writer that could not stand behind a digest (§5.2's
raise gate) — unverifiable, never tampered.

**Contract.** A sweep-built **overview** (§4) records its hashes in a sidecar
the same way, and that sidecar is named from the overview's own basename —
the stem plus `.stats.json` (`all.zarr` → `all.stats.json`, `2019.zarr` →
`2019.stats.json`), a sibling object at the ancestor node. Unlike a source
leaf's sidecar name, which is keyed to the store's `spec` revision, the stem
grammar applies to an overview sidecar **unconditionally, at every
revision**: one ancestor node holds every window's overview (§4.2), so a
revision-keyed bare name would resolve all of them to a single `stats.json`
at that node.

*(Informative.)* The O11 hash is the verification half of the D19 identity
split — the `semantic_hash` says two leaves were *intended* identical; O11
says they *are* byte-identical — and doubles as the mismatch localizer
("only `h_tdigest` differs in this leaf") and the detection mechanism for
stamped-but-torn leaves under the concurrency contract's out-of-contract
case.

*(Informative.)* The `semantic_hash` itself is **not** part of this
specification: its derivation is the D19 record
([`design/sparse_coverage.md`](design/sparse_coverage.md)) as implemented by
`zagg.semantics`, it is subject to hash **epochs** (a change to the
canonicalized subset moves every digest by design — the 2026-08-17 epoch,
issue #415, and the 2026-09-13 **index-exclusion epoch**, issue #499, under
which `data_source.index` is read machinery outside the core), and no
fixture's `*.expected.json` records one — the committed §7 fixture manifests
do carry the digest, and `tests/test_spec_conformance.py` reproduces each
from its build config, so an epoch that moves a fixture config's digest
regenerates those manifests. A reader should treat it as an opaque frozen
manifest key: compare it for equality rather than recomputing it. Operator
consequences of an epoch live in
[`hive_layout.md`](hive_layout.md#migration-the-index-exclusion-epoch-issue-499).

## 6. `zagg-ragged/2`

**Status: specified, implementation pending
([#210](https://github.com/englacial/zagg/issues/210); timing ratified —
the dtype package ships on its own release train, the zagg writer knob and
reader dispatch are gated on 1.0).** `/2` **adds to** `/1`, it does not
replace it: `/1` is the pinned 1.0 wire contract (§1), existing stores keep
it forever, and every conforming reader supports `/1` unconditionally.

`/2` moves the element declaration out of the §1.2 attrs block and into the
zarr **data type itself**: a parameterized typed vlen dtype, so a generic
zarr stack knows the element interpretation without zagg's attrs convention.

### 6.1 The typed dtype

**Contract.** The `/2` data type is the registered zarr v3 extension
**`vlen-ndarray`** (espg-ratified name; reference implementation: the
`zarr-vlen-ndarray` package under `github.com/espg`), parameterized by the
element dtype and trailing inner shape — exactly the pair the `/1` attrs
block declares:

- element dtype `float32`, inner shape `(2,)` for a digest payload array;
- element dtype `uint64`, inner shape `()` for a locations sibling (§2.2), and
  the same pair for a `{field}_times` temporal sibling (§8.3) — the two
  companions are one dtype, differing only in the declaration they carry.

A cell's logical value is the `(n, *inner_shape)` array itself rather than
its raw bytes; everything else about the array — shape, cells axis,
`fill_value` (the empty cell), located sibling alignment (§1.1), storage
geometries (§1.5) — is unchanged from §1.

### 6.2 Byte identity

**Contract.** The `/2` codec chain MUST produce chunk bytes **byte-identical
to `/1`'s**: the §1.4 wire framing and the §1.3
`[…, zstd(level=3, checksum=false)]` chain are unchanged, with the typed
array↔bytes codec serializing each cell as the same
`np.ascontiguousarray(value).tobytes()` little-endian payload. The typed
dtype changes *interpretation only*, never stored bytes. Consequences (the
point of the revision):

- migrating an existing `/1` store to `/2` is a **metadata-only** rewrite
  (`zarr.json` objects; no data object is touched);
- the §7 conformance fixtures serve both revisions — a `/1` fixture's chunk
  objects re-labeled `/2` MUST decode identically through the typed path;
- the §5 O11 vlen recipe is unaffected (it hashes decoded payload bytes,
  which are identical by construction) — this is exactly what §5.2's
  element→bytes normalization buys: a `/2` cell decodes to an ndarray, whose
  C-contiguous little-endian bytes are the `/1` cell's bytes.

### 6.3 Revision signaling

**Contract** (restating §1.6 from the `/2` side):

- An array whose zarr data type is `vlen-ndarray` **is** `zagg-ragged/2`;
  the `ragged` attrs marker is retired on such arrays (not written). The
  element declaration lives in the dtype configuration alone — a reader
  MUST NOT require the attrs block on a `/2` array. (§8.3's `times` binding
  and §2.0's `weights` are already top-level keys beside the block, so they
  ride the migration unchanged — this clause is about the bindings that
  live *inside* it.)
- A located `/2` payload array still declares its sibling binding in
  **metadata, never by naming convention** (the §1.2 rule survives the
  revision), and it does so under a **new top-level attrs key** — not a
  residual `ragged` block with `spec`/`element` dropped. "Retired" is
  literal: no `ragged` key is written on a `/2` array. That is a ruling, not
  a leftover choice — a `ragged` block carrying only `locations` would be, by
  §1.2's own words, an array with no well-formed `element` declaration, which
  a `/1`-only reader MUST refuse with a pointed "not a `zagg-ragged/1` array"
  error, i.e. the misleading path instead of the actionable "install
  `zarr-vlen-ndarray`" one below. The `ragged` key stays **reserved but
  unwritten** under `/2` (config-declared attrs still MUST NOT shadow it), and
  the new key's exact name is for the `/2` implementation PR to pin *in this
  section* before any `/2` store exists. The sibling-alignment semantics of
  §1.1 are unchanged.
- An array with the `variable_length_bytes`/`bytes` dtype and a
  `spec: "zagg-ragged/1"` attrs block **is** `zagg-ragged/1`.
- A reader without the `vlen-ndarray` extension installed MUST surface an
  actionable "install `zarr-vlen-ndarray` to read this store" failure, not
  a silent mis-decode (and cannot half-parse: the dtype is unknown to its
  zarr stack by construction).

*(Informative.)* Writing `/2` will be a per-product opt-in
(`output.ragged_encoding: typed`), which shifts the product's
`semantic_hash` — a new product identity, by design. That shift is not
automatic: `output.*` keys reach the semantic core only by being listed as
leaf-shaping (`zagg.semantics.OUTPUT_LEAF_SHAPING_KEYS`, issue #415), so the
`/2` implementation PR must add the knob there in the same change. The
default stays `/1`; flipping it is a schema epoch deferred to its own ruling
(public/interop stores may deliberately stay `/1` for vanilla-zarr
openability).

## 7. Conformance fixtures

**Status: contract.** The committed stores under
[`tests/data/spec/`](https://github.com/englacial/zagg/tree/main/tests/data/spec)
are part of this specification: a reader implementation that reproduces
their expected decoded values and content hashes conforms to §1–§3, §5,
§8, §9 and §10. They are generated by
[`tools/generate_spec_fixtures.py`](https://github.com/englacial/zagg/blob/main/tools/generate_spec_fixtures.py)
through zagg's **production write path** (manifest, sharded leaf template,
dense + ragged writes, coverage sidecar, commit stamp), so writer↔spec
drift fails zagg's own suite (`tests/test_spec_conformance.py`) on
whichever side moved. moczarr vendors the same fixtures for its parity
gates (espg/moczarr#19/#20).

The leaf fixtures were committed before this revision and are
unregenerated: no stamp carries the §5.3 copy of `content_hashes`, and no
`dggs` block carries the §1 `latitude` token, so each fixture is the
absent-key ⇒ pre-[#580](https://github.com/englacial/zagg/issues/580) pin
for both (the sidecar copy and §1's own evidence paragraph are the record
for what those artifacts mean). The one **versioned** leaf (§1.5; a stamp
naming `current`) is `versioned/` below, added standalone with the writer
that produces it (issue [#582](https://github.com/englacial/zagg/issues/582));
the older leaf fixtures stay legacy and pin the absent-`current` rule.
Regeneration of those is deferred because it would
also install the §4.9 `multiscales` mirror that `column/` pins the
**absence** of, retiring an unrelated pin.

Seven tiny single-shard hive stores plus two metadata-only ones (the
`pyramid/` declaration and the `multiscales/` companion), all on the same
deliberately small geometry — shard order 4, inner-chunk order 5, cell
order 6 (16 cells, K = 4 inner chunks of 4 cells), sharded (the hive
default; `raster_toc/` is the one exception — a `(time, cells)` product is
never sharded, §8/#247):

- **`minimal/`** — one *unlocated* digest field (`h_tdigest`) plus `count`.
  The smallest thing that is a conforming store.
- **`versioned/`** — `minimal/`'s inputs written as a **versioned leaf**
  (§1.5, issue [#582](https://github.com/englacial/zagg/issues/582)): the
  stable root `{id}.zarr/zarr.json` is a pointer stamp naming `current`
  (`run-{run_id}-{attempt}`), and the arrays, the coverage sidecar and the
  version's own stamp sit in that subgroup — nothing else at the root.
  `versioned.expected.json` records `pointer` (the root), `version` (the
  `current` value) and `run_id`, with `leaf` pointing at the version so
  every leaf-shaped assertion applies unchanged; the conformance suite
  decodes the pointer from the on-disk JSON alone and pins that one reader
  rule (open the root; follow `current` when named, else read the root)
  resolves `minimal/` and `versioned/` alike. The pointer's `spec` is the
  legacy token (`/1`): versioning is the `current` key, never a token.
- **`flux/`** — the §2.0 `weights` declaration surface: one flux-declared
  digest field (`rx_flux`, `weights: "flux"` stamped beside the `ragged`
  block, `gain` provenance attrs) plus `count`. Its payloads carry
  fractional positive weights whose per-cell sums are **not** integers —
  the pin that a flux reader must not round-trip weights through counts —
  while `minimal/` (committed before this revision, unregenerated) pins the
  absent-key ⇒ `"counts"` default.
- **`kitchen_sink/`** — the full stratified-product surface: located
  signal/noise digest strata (payload + `{field}_locations` siblings,
  `stratum`/`signal_threshold` provenance attrs), the `composition` word
  (including a single-photon cell packing the §3.1 golden word
  `0xFF000000FF0000FF` and a noise-only cell whose signal payload is the
  empty `(0, 2)` array), and `count`.
- **`pyramid/`** — MANIFEST ONLY: the §4.5 `zagg-pyramid/2` declaration
  grammar. The committed `morton_hive.json` was produced by the production
  declaration paths end to end — templated `/1` (`hive.build_manifest`),
  given sweep actuals by the production bookkeeping writer, then retrofitted
  to `/2` with `declare_pyramid` — and carries every `/2` reading a decoder
  must tell apart: a multi-resolution leaf entry (`{"node": 3, "cells":
  [5, 4]}` — this fixture is shard order 3, not the leaf fixtures' 4, so
  the leaf window has two interior resolutions), the §4.4 fixed every-order
  ladder rooted at node 0, the #376 fold keys (`fold_source`,
  `exact_levels`), and the preserved `/1`-era `materialized.fold_sources`
  actuals — and, via the committed `pyramid.expected.json`, which records
  the raw config knob, the leaf-resolution declaration the expansion was
  derived from. It writes no store beneath it on purpose: the pyramid block
  is a template-time manifest artifact, decodable from `morton_hive.json`
  alone — the `/2` artifacts a fleet writes are `column/`'s job below
  (sweep-side levels are #384's). The same manifest carries the §4.9
  `multiscales` discovery mirror the retrofit installs beside a `/2`
  block; `column/`, committed before §4.9 and unregenerated, is the
  absent-key ⇒ pre-convention pin.
- **`column/`** — the `minimal/` inputs plus an explicit
  `output.pyramid.overviews: 5` knob, so the same worker invocation that
  committed the leaf also wrote its §4.6 **column**: `all.pyramid.zarr`
  beside the leaf, groups `{5, 4}` (the declared base and the node-order
  member — this geometry has no interior ladder rung between them), `role:
  column` + the `zagg_column` attrs grammar, its own commit stamp, and the
  `all.pyramid.stats.json` D20 sidecar. `column.expected.json` records the
  decoded group values, the attrs block verbatim, the stamp's clock-free
  fields, and the column's §5 hashes; the conformance tests additionally
  re-derive the base group from the committed leaf through the §4.4 fold
  kernels — the §4.6 from-leaves parity contract, pinned on committed
  bytes. The two-group set is forced, not chosen: on this 4/5/6 geometry
  §4.4 admits only `overviews: 5`, so no committed golden here can carry an
  **interior** ladder rung (a three-or-more-group column, its group
  ordering, or a declared base distinct from an implied rung). That case is
  pinned zagg-side by `tests/test_column.py`; a committed multi-rung golden
  arrives with the sweep-side fixtures of
  [#384](https://github.com/englacial/zagg/issues/384).

- **`multiscales/`** — the §4.10 companion-group surface: METADATA ONLY —
  a `zagg-pyramid/2` manifest on `pyramid/`'s grid, a production root
  `coverage.moc` naming two occupied order-3 shards, and the companion
  tree at the reserved root path `multiscales/`: a consolidated root
  group document (the §4.9 mirror + the `zagg_multiscales` stamp) plus
  one child group per coarse ladder order, each mapping the order's
  ancestor nodes to store-root-relative resolution-group paths. No
  arrays, no leaves, no overview artifacts — the committed golden pins
  that §4.10 references claim ownership, never presence, and that the
  tree opens with stock zarr alone.
- **`raster_toc/`** — the §8 temporal declaration surface: one raster
  `(time, cells)` hive leaf whose `time` coordinate is `uint64` toc words
  carrying `temporal: {"spec": "zagg-toc/1", "shape": "coordinate", …}` and no CF
  `units`/`calendar` attrs, beside `morton` and two band arrays. Its axis
  mixes both word variants deliberately — one single-item timestep encoded
  as an exact **timestamp** word and two multi-item acquisition groups
  encoded as **range** words — so a reader that implements only one variant
  fails a §7 fixture. `raster_toc.expected.json` records the words as
  decimal strings (JSON numbers cannot carry `uint64` faithfully) together
  with the `(start, end)` nanoseconds a conforming decode yields and the
  real acquisition spans they must contain, which is the §8 conservative
  containment claim pinned on committed bytes. The other four fixtures,
  which carry no `temporal` key anywhere, are the absent-key ⇒ legacy pin.

- **`temporal/`** — the §8.2/§8.3/§9 **companion** surface: `minimal/`'s
  geometry and cell plan with one digest field (`h_tdigest`) carrying both
  companions — the located sibling `h_tdigest_locations` (declaring
  `located`, §9) and the temporal sibling `h_tdigest_times` (declaring
  `temporal` at `shape: "per-centroid"`, bound from the payload's `times`
  key) — plus the dense `uint64` `observed` array declaring
  `shape: "per-cell"`, and `count`. Both toc variants are committed **in
  both shapes**: the single-observation cell is an exact **timestamp** word
  and every multi-observation cell and merged centroid a conservative
  **range**, so a reader implementing one variant fails a §7 fixture. The
  unpopulated cells of `observed` hold the §8.2 reserved `0`.
  `temporal.expected.json` records the words as decimal strings, the three
  declarations verbatim, and — derived from the generator's inputs, never
  transcribed — the real member instants each word must contain and each
  centroid's true member run, so the conformance suite asserts §8.2/§8.3's
  containment (and that a cell's per-cell envelope encloses every
  per-centroid envelope beneath it) on committed bytes. Its located sibling
  carries **heterogeneous orders** — order-29 point words on unmerged
  centroids, coarser ancestors on merged ones — which is §9.1's claim
  pinned. `kitchen_sink/`, committed before §9 and unregenerated, is the
  absent-`located` ⇒ §2.2 pin, exactly as `minimal/` is §2.0's.

  Its §4.6 leaf column carries the **folded** companions: every resolution
  group holds `h_tdigest` with both siblings, each declaring `per-centroid`,
  row-aligned with the folded payload. That makes it the fixture set's only
  golden for a companion produced by a *merge* rather than by ingest — both
  channels reduced over the centroid partition that merge produced, at every
  level (espg-ruled 2026-08-17, amending
  [ruling 3](https://github.com/englacial/zagg/issues/410#issuecomment-5310502887)).
  The dense `observed` array is deliberately absent from those groups: the
  `"per-cell"` shape's fold law is the grammar's join over a cell group rather
  than the field's own reducer, so it exists at native resolution only.

  It is also the fixture set's only store with a **root `coverage.moc`**, and
  so §10's golden: the object was written by the production sweep writer (the
  MOC family's leaf read plus its finisher) and carries the
  `zagg-coverage-toc/1` section — the shard's tier-1 envelope word and the
  tier-2 root time-digest in the native ragged `(k, 2)` + word-sibling form.
  `temporal.expected.json`'s `root_coverage` block records the tier-1 word
  **derived from the generator's inputs** (the join over every per-centroid
  word it fed the writer, so the writer is pinned rather than self-certified),
  the decoded digest rows read back — the same exception `column/`'s group
  values are — and `obs_total`, the cell plan's own observation count, which
  §10.3's weight rule says the digest's total weight MUST equal. The other
  fixtures have **no root coverage object at all**: none of them declares a
  temporal field, so a sweep of one produces no section, and their committed
  trees are byte-identical to their pre-§10 selves — which is exactly §10's
  absence rule, pinned as bytes. From issue #489 the same fixture also
  carries the **§10.5 `coverage.toc` sibling**, PUT by the same production
  finisher beside the carrier (whose section now bears the §10.1 `cover`
  marker); `temporal.expected.json`'s `cover` block records the derived
  word set — the §10.5 quantization of the same inputs the tier-1 word is
  derived from — the object's declared `cap`/`fields`/`element`/`encoding`
  and the block's `count`, and `gap_ns`, an interval on the internal scale
  the committed cover MUST NOT claim. The fixture's last cell is deliberately
  clocked whole buckets past the rest of the plan, so the cover is a
  MULTI-word set with a real hole in it: a reader that quantized every word
  into one bucket would satisfy the parity and containment claims and fail
  `gap_ns`. The other fixtures carry no `coverage.toc` either, the §10.5
  absence pin.

- **`demoted/`** — the §4.3 `demotions` surface
  ([#518](https://github.com/englacial/zagg/issues/518)): the
  `kitchen_sink/` store swept under a hand-installed `/1` cascade manifest
  whose composition `of` divisor is **mis-declared** `class: "none"` (§4.5:
  manifests outlive their writer). Two committed overviews pin the pair of
  readings: the exact-from-leaves level folds `composition` cleanly (the
  leaf arrays carry the divisor regardless of its declared class) and
  carries **no** `demotions` key — the absence pin — while the cascaded
  level above it fires `divisor-missing` on its one contributor, holds the
  all-fill word slab, and records the demotion — `cells: 4`, the skipped
  child's whole disjoint span of the 16-cell level — in its `zagg_overview`
  attrs. `demoted.expected.json` records both levels' paths, fold sources
  and the demotions list beside the leaf record.

`minimal/` and `kitchen_sink/` pin the layout edge cases a reader must
handle (`column/`'s leaf is `minimal/`'s, so it pins them again): inner chunk
ordinal 2 is **empty** (absent from the shard index — the §1.5 sentinel, and
that sparsity reaches the dense arrays too: the `morton` coordinate and
`count` hold their fill across that chunk, so a reader MUST NOT assume the
coordinate is dense across a shard), populated chunks contain empty cells
(the `b""` fill), and one cell's digest carries **merged** centroids whose
location words are common ancestors (§2.2).

Every fixture **leaf** is **sharded**, so §7's leaf conformance claim is
scoped to the §1.5 sharded geometry. The per-inner-chunk geometry — identical
§1.4 framing, one object per inner chunk, no shard index — now has a
committed golden: `column/`'s resolution groups are single-chunk unsharded
arrays (§4.6), including a `zagg-ragged/1` payload array (`h_tdigest`,
`chunk_shape == shape`, no `sharding_indexed`), so a reader that hard-codes
the shard-index suffix fails a §7 fixture instead of sailing through. The
unsharded **multi**-chunk case remains pinned zagg-side by
`tests/test_processing.py::TestRaggedVlenLayout`. A reader that derives the
stored span from the array's own metadata, as §1.5 requires, reads all of
these from one code path.

Each fixture ships a sibling **`{name}.expected.json`** recording the shard
key, leaf path, geometry, every populated cell's decoded values (digest
centroids, location words and composition words as decimal strings — JSON
numbers cannot carry `uint64` faithfully), the per-stratum exact counts,
and the §5 O11 `content_hashes` (per-array + combined). The expected **leaf
decoded values** were computed from the generator's *inputs* — the arrays
handed to the writers — never read back through a zagg reader, so the reader
is pinned, not self-certified. The `column` record's group values are the
one exception: they are the writer's committed output, read back. Their
independence comes from elsewhere — the conformance suite **re-derives** the
base group from the committed leaf through the §4.4 fold kernels and asserts
byte equality (the §4.6 from-leaves parity contract), so the recorded values
are a regression pin over an independently derived result, not a
self-certification. The `content_hashes` are necessarily computed
from the **written leaf** (a content hash of nothing else would mean
anything), so they are pinned differently: the suite carries the combined
digest and one per-array digest per §5.2 element kind as **frozen hex
literals**, and recomputes the vlen digests a second time from the shard
objects alone (§1.4/§1.5 byte recipes, no zarr). A recipe change — prefix
width, joiner, key set — therefore fails a test instead of agreeing with
itself on both sides, which is also the only mechanism that catches a future
zagg↔moczarr divergence (neither side's fixture can: espg/moczarr#23).

**Conformance criteria for an external reader**: decode every ragged array
per §1–§2, the composition array per §3, and every declared word-typed
array — the time coordinate and both temporal companion shapes per §8, the
located companion per §9 — reproducing the expected decoded values exactly (byte-exact
float32/uint64 — no tolerance), and reproduce `content_hashes` per §5. zagg's own suite additionally decodes
the shard objects with **spec-text-only** decoders (struct + zstd, no zagg
read path) to prove the byte recipes in §1.4/§1.5 are sufficient on their
own.

Regenerating the fixtures reproduces the same logical values (seeded rng);
stamp timestamps and compressed bytes may differ across zstd versions —
conformance is over *decoded* values, never stored object bytes (the same
principle as §5). In the committed D20 sidecar
(`column/…/all.pyramid.stats.json`) only `content_hashes` and
`cells_with_data` are pinned: `timestamp`, `zagg_version`, `run_id` and the
run counters are **informative provenance**, they churn on every
regeneration, and conformance never asserts them.

---

## 8. `zagg-toc/1`

**Status: contract** ([issue #443](https://github.com/englacial/zagg/issues/443)
— the first shape of the temporal series,
[#410](https://github.com/englacial/zagg/issues/410)).

An array whose elements are **packed words** rather than self-describing
scalars carries a **word-typed coordinate declaration**: a spec-owned attrs
block, keyed by its domain, holding `{spec, shape, grammar}` — the
convention revision, *where the words sit* relative to the store's cells,
and *which word grammar* the values follow. The key is spec-owned: the
writer stamps it, never author-transcribed (§1.2's reserved-key discipline,
extended to coordinate arrays). The `shape` vocabulary is domain-neutral and
defined once, here:

- **`"coordinate"`** — the declaring array **is** the coordinate variable of
  a dimension (CF/xarray sense): one word per index along that dimension,
  row-aligned with every array sharing it.
- **`"per-cell"`** — one word per cell of the store's cell grid, aligned
  with the `morton` coordinate.
- **`"per-centroid"`** — one word per centroid inside a cell's ragged
  payload, aligned element-for-element with the §1/§2 digest it accompanies.

This section instantiates that pattern for the **temporal** domain: attrs key
`temporal`, `spec: "zagg-toc/1"`, `grammar: "mortie-toc/1"`.

**An absent `temporal` key MUST be read as the legacy encoding** — signed
`int64` microseconds since `1970-01-01T00:00:00` UTC, self-described by the
CF `units`/`calendar` attrs the writer stamps beside it. Every store written
before this revision is conformant verbatim, no byte rewritten, and a reader
MUST NOT refuse a store for lacking the declaration.

```json
"temporal": {
  "spec": "zagg-toc/1",
  "shape": "coordinate",
  "grammar": "mortie-toc/1"
}
```

Three keys, and deliberately no more —
[ruled on #410](https://github.com/englacial/zagg/issues/410#issuecomment-5310533396):
the declaration is `{shape, grammar revision}` under the `spec` marker, with
**no per-store epoch or quantization guards**.

- **`spec`** — the convention revision. Readers MUST strict-check it: an
  unknown or future revision raises, never half-parses under a guessed
  layout.
- **`shape`** — the vocabulary value above. This revision defines all three
  for `temporal`, each with its own contract section: **`"coordinate"`**
  (§8.1 — the declaring array **is** the time coordinate of a
  `(time, cells)` product), **`"per-cell"`** (§8.2 — one word per cell of
  the cell grid), and **`"per-centroid"`** (§8.3 — one word per centroid of
  the digest it accompanies). A reader MUST refuse a `shape` it does not
  implement — including one a later revision adds, which is why refusing is
  the required behavior and guessing never is. Adding a shape is therefore
  additive within the revision: `spec` does not move, stores already written
  stay valid verbatim, and a reader that implements only some shapes fails
  loudly on the rest rather than mis-decoding them.
- **`grammar`** — the word grammar the values follow, named as a **grammar
  revision** in the `{name}/{major}` style this document uses throughout
  (`zagg-ragged/1`, `morton-hive/2`). It is a fixed token of this revision —
  not a documentation URL, and not a stamp of the writer's installed mortie
  — so a store's bytes move neither when a dependency floor moves nor when
  the documentation that describes the grammar moves. The pointer to that
  documentation is prose (below), which is where it can be updated freely. A
  reader MUST refuse a `grammar` it does not implement, and SHOULD record it
  as what it decoded against.

**The declaring array is the array that holds the words.** A block declares
the values of the array it sits on, never a neighbour's: the time coordinate
declares its own axis (§8.1), a per-cell companion its own dense array
(§8.2), a per-centroid companion its own ragged sibling (§8.3). Where a
companion is reached through another array, that array carries a *binding*
(a sibling array name) and the companion carries the *declaration* — the
same split §1.2 already makes for the located sibling.

**Instantiations** ([#410](https://github.com/englacial/zagg/issues/410)).
The temporal companions — one word per cell, one word per centroid — are the
`"per-cell"` (§8.2) and `"per-centroid"` (§8.3) shapes under *this* `spec`
and *this* grammar. The **located** (spatial) companion family declares
under the same pattern with `grammar: "mortie-morton/1"` and its own domain
key, in [§9](#9-zagg-located1). None of them re-declares the store's
**primary** morton axis: that surface stays
declared by the `morton-hive/{1,2}` manifest grammar (`morton_hive.json`,
`docs/hive_layout.md`) and the store's [DGGS-convention](https://github.com/zarr-conventions/dggs)
`dggs` attrs (`docs/morton_arrow.md`). This pattern is for word-typed
coordinate and companion arrays *beyond* that primary surface, never a
second, competing declaration of it.

**The epoch, the timescale, and the range variant's rounding quanta are
properties of the cited grammar and are deliberately NOT echoed here.** A
store cannot re-base them — a differing origin would be a different grammar,
which is precisely what `grammar` (and `spec` above it) already discriminates
— so a per-store copy could only ever restate a constant, while entering the
committed conformance bytes and the §5 content hash and forcing a fixture
regeneration for any upstream constant that changed.

A writer MAY add further **informative** keys to the block (source-time
lineage and the like). They are non-normative: nothing in §8 is decoded from
them, and a reader MUST ignore keys it does not recognize rather than refuse
the store.

### The word grammar is mortie's

A `zagg-toc/1` value is a mortie **toc word** (temporal order coverage): one
`uint64` that is a tagged union of an exact nanosecond **timestamp** and an
outward-rounded, conservative **range**, on a continuous, leap-free,
GPS-aligned timescale with a fixed `1850-01-01T00:00:00` epoch. The bit
layout, the flag position, the unsigned sort order, and the semilattice
merge law are normative in mortie's
[`mortie.toc` reference](https://espg.github.io/mortie/0.9.6/api/toc/) and
its decision ledger
([espg/mortie#175](https://github.com/espg/mortie/issues/175)), and are
**not restated here**. What follows is zagg's half of the contract.

The stored token is the grammar revision `mortie-toc/1`; the **URL above is
the documentation pointer**, and it lives in this prose precisely so it can
be re-pointed without moving a store byte. It is **release-pinned
deliberately**: mortie's documentation is `mike`-versioned and the
unversioned `/api/toc/` path was never published, so only a versioned URL
resolves; the pin is `0.9.6`, the earliest release carrying the reference,
and the page's normative words are unchanged through the current dependency
floor.

**It is also not yet the same class of citation as §2.2's**, and §8 does not
claim it is. §2.2 defers the morton word's layout to mortie's
`docs/specification.md` §1/§4 — sections that document's §10 *"Frozen for
1.x"* enumerates as immutable within the major version. Mortie's frozen
specification contains **no toc section**: the grammar above is normative as
a module reference pinned by mortie's own golden fixtures, which is a weaker
guarantee. [espg/mortie#193](https://github.com/espg/mortie/issues/193)
tracks adding the frozen section; when it lands, this pointer swaps to it,
the stored `grammar` token does not move, and nothing else in §8 changes.

### 8.1 `shape: "coordinate"`

**Contract.**

- The declaring array's element type is `uint64`, one word per timestep.
- A timestep whose real acquisition is a single instant MUST be encoded as a
  **timestamp** word, exact to the nanosecond; a timestep covering a real
  interval MUST be encoded as a **range** word whose envelope conservatively
  contains that interval. A conforming writer therefore never widens an
  instant into a range, and never narrows a real interval into an instant.
- **Row order is the acquisition-group order**: timesteps are ordered by the
  group's **earliest member observation time** — the order the time axis has
  always had, the order the `(time, cells)` slabs were indexed by, and
  identical under both encodings, so a row assignment never drifts with the
  encoding.
- **Stored word order is not that key, and a reader MUST NOT assume the
  stored words ascend.** Unsigned word order is order by the *encoded* start,
  which is the conservative envelope start — and an envelope may begin before
  its group's earliest member observation time, by an amount nothing in this
  section bounds. Where it does, the word leads the row key and the stored
  axis is materially out of order: `np.sort(words)` yields ascending
  *envelope-start* order, which is **not guaranteed to equal row order**. In
  particular a reader MUST NOT bisect the stored axis to resolve a time
  window; use the overlap predicate below, which is correct regardless of
  stored order.
- Decoding to wall time yields **`(start, end)`** per timestep: for a
  timestamp both bounds are its exact instant; for a range `end` is the
  envelope's **exclusive** upper bound. A reader that must present one
  instant per timestep SHOULD present `start`, and MUST NOT present a
  midpoint as if it were the observation time — the midpoint of a
  conservative envelope is not an observation.
- Temporal window selection is the grammar's overlap predicate applied to
  the stored words directly: it over-reports by at most one quantum at a
  window edge and **never under-reports**, so a selection is a conservative
  superset of the timesteps whose real acquisition intersects the window.

**Composition.** Time axes compose only between **matching declarations** —
`zagg-toc/1` with `zagg-toc/1`, legacy with legacy (an absent key is legacy
for this rule too). A concatenation across encodings would produce an axis
whose values mean two different things, so a reader or writer joining two
stores' time axes MUST refuse a mismatch. This is a *join* rule only:
reading either store on its own is always legal.

**What §8.1 does not cover** (informative). Its clauses are the
`"coordinate"` shape's alone: the companion shapes carry their own in §8.2
and §8.3, and a clause here binds a companion only where that section
restates it. Nothing in §8 constrains the `(time, cells)` band arrays
themselves, which are unchanged.

### 8.2 `shape: "per-cell"`

**Contract** ([#410](https://github.com/englacial/zagg/issues/410)). One
word per cell of the store's cell grid: a **dense `uint64` array on the
cells axis**, index-aligned with the `morton` coordinate (row `i` is the
cell `morton[i]` addresses), sharing that array's shape, chunk grid and
storage geometry (§1.5). It is an ordinary dense array — not a
`zagg-ragged/1` array — and carries the `temporal` block in its own attrs.

- The array's `fill_value` MUST be `0`, and **`0` is reserved**: it marks a
  cell the writer left unobserved and MUST NOT be read as an acquisition. A
  writer MUST NOT store `0` for an observed cell. **The reservation is
  cost-free**: `0` is not a value the grammar's encoders can produce, and it
  is empty where it is decoded, so the sentinel excludes nothing.
  - No encoder emits it. The epoch instant encodes as `2147483648` (the flag
    bit sits at position 31, not at the bottom of the word), and the shortest
    range word is `1`, because the range encoder's end code is a
    strictly-greater ceiling and is therefore `≥ 1` for every input, `(0, 0)`
    included.
  - `toc_merge` cannot introduce it. The join is over encoded words, so a
    reduction over words no encoder produced as `0` does not produce `0`.
  - It is empty, not an instant. `0` decodes as a **range** word whose
    envelope is the half-open `[0, 0)` — it overlaps no window, including one
    containing the epoch — so a reader that meets it under the grammar's
    overlap predicate selects nothing, with or without this reservation.
- Each observation enters under §8.1's discipline, restated here with its
  force (§8.1's clauses bind a companion only where its section restates
  them): a writer MUST encode an instant as a **timestamp** word, exact to
  the nanosecond, and a real interval (an integration window — an
  observation need not be instantaneous) as a **range** word conservatively
  containing it. A cell's stored word is the
  join of its observations' words (below): a **timestamp** word exactly when
  that join is a single instant — a cell covering one instantaneous
  observation, or several sharing one instant — and a **range** word
  conservatively containing every observation pooled into it otherwise.
  Instants never widened, intervals never narrowed, **per observation, not
  per count**.
- **The pooled word is the grammar's join.** A cell's word over a set of
  observations is `toc_merge` (the grammar's semilattice join) reduced over
  their individual words: the conservative envelope. The join is
  associative, commutative and idempotent, so the stored word is
  **independent of the order and tree** any producer reduced in — bit
  identity, not approximate agreement. Whether that reduction happens in one
  pass, in spill blocks, or up a pyramid is a producer's business and is not
  specified here.
- The claim a reader may make from the word is exactly the envelope:
  every observation the cell summarizes fell inside `[start, end)` (both
  bounds equal for a timestamp). It is **not** a claim that the cell was
  observed throughout that interval, and the envelope's midpoint is not an
  observation.
- Stored word order is not the cells' order and carries no meaning: the axis
  is ordered by `morton` (§1.5), so a reader MUST NOT sort or bisect this
  array to resolve a time window. Selection is the grammar's overlap
  predicate applied to the stored words directly — a conservative superset
  of the cells whose real observations intersect the window, over-reporting
  by at most one quantum at an edge and never under-reporting.

### 8.3 `shape: "per-centroid"`

**Contract** ([#410](https://github.com/englacial/zagg/issues/410)). One
word per centroid of a digest field: a **`zagg-ragged/1` vlen sibling array**
(element dtype `uint64`, empty `inner_shape`) with the same shape and chunk
geometry as the payload array, **row-aligned** with it exactly as the
located sibling is (§1.1) — cell `i` of the sibling holds one `uint64` word
per payload row of cell `i`, so the two arrays share the payload's per-cell
row counts. The sibling carries the `temporal` block in its own attrs.

- **Binding.** The payload array declares the sibling by name under a
  spec-owned **`times`** attrs key — a *sibling* of the §1.2 `ragged` block
  on the payload array, exactly as §2.0's `weights` is, and for the same
  reason: the `ragged` block is retired wholesale under `/2` (§1.6/§6.3), so
  a key outside it survives that metadata-only migration untouched. The
  `zagg-ragged/1` block grammar is therefore **unchanged by this revision** —
  no key is added to it, and a `/1` reader that ignores unknown top-level
  attrs decodes such a store exactly as before, minus the companion.

  ```json
  {
    "ragged": {
      "spec": "zagg-ragged/1",
      "element": {"dtype": "float32", "shape": [-1, 2]},
      "locations": "h_tdigest_locations"
    },
    "times": "h_tdigest_times"
  }
  ```

  (The asymmetry with `locations` — inside the block — is deliberate and
  historical: `locations` predates the sibling-key ruling and cannot move
  without a `/1` revision, while a new key has no such constraint.)

  A reader MUST bind the sibling by that declaration, never by
  reconstructing a naming convention, and MUST read the `temporal` block off
  the **sibling** it binds — a payload array carries the binding, never the
  declaration. An absent `times` key means the field has no temporal
  companion. The key is spec-owned: a writer stamps it from the field's
  declaration, and config-declared attrs MUST NOT shadow it (§1.2's reserved-key
  discipline, enforced at config validation).
- **A word's claim is keyed on its variant**, carried by the word itself,
  never by the payload's weights — under a `"flux"` payload (§2.0)
  `sum(weights)` is not a member count, so weight identifies nothing. Each
  observation enters under §8.1's discipline, restated here with its force
  (§8.1 binds a companion only where its section restates a clause): a writer
  MUST encode an instant as a **timestamp** word, exact to the nanosecond,
  and a real interval (an integration window) as a **range** word
  conservatively containing it. A merged centroid carries the grammar's
  `toc_merge` join over its members' words, with the
  same order-independence §8.2 states, and a centroid folded from a single
  observation carries that observation's word unchanged. (Zagg's own
  writers ingest per-observation instants today — range ingest is legal,
  not emitted; informative.) Both variants therefore coexist in one array,
  and a reader that implements only one is not conforming.
- **Row order is the payload's, and it is not time order.** §2.1 sorts
  payload rows ascending by mean, and the sibling is row-aligned with them,
  so the stored words are in *value* order. A reader MUST NOT assume the
  words ascend and MUST NOT bisect them; window selection is the grammar's
  overlap predicate on the words directly, exactly as in §8.2, and its
  result is a conservative superset of the centroids whose observations
  intersect the window.
- The digest payload it accompanies is **approximate** across fold orders
  (§2.3) while this companion's join is exact. The companion's exactness
  does not lift the field's composability class: a merged payload's
  centroids may differ, and the words are exact **given** the centroid
  partition they describe, not independently of it.
- Provenance attrs still ride the payload array only (§1.2); the temporal
  sibling carries the spec-owned `temporal` block and no user attrs.

**Per-level shapes need not match** (contract). The two companion shapes
describe the same information at different granularities, so one product may
carry `"per-centroid"` on its leaves and `"per-cell"` on the summaries folded
above them — a per-cell range is the honest temporal statement for a lossy
summary, and it is the envelope of the per-centroid words beneath it. Each
array is read through **its own** declaration; nothing requires a store's
levels to agree, and a reader MUST NOT infer one array's shape from another's.

### 8.4 Composition and merge legality

**Scope.** §8.4 governs the composition of the **companion** shapes —
`"per-cell"` (§8.2) and `"per-centroid"` (§8.3). It does **not** govern the
`"coordinate"` shape: two time axes join under §8.1's Composition paragraph,
which is the stricter rule, and **§8.1 takes precedence wherever both could
be read to apply**. The two differ exactly on absence, and deliberately: an
undeclared *companion* carries no information, so §8.4 composes with it and
drops the channel; an undeclared *coordinate* array carries the legacy
encoding (§8), which is information — a rival encoding of the same axis — so
§8.1 MUST-refuses a legacy ↔ `zagg-toc/1` join. Nothing in §8.4's absent-key
clause below licenses that join. This is the same boundary §8.1 draws from
its own side under "What §8.1 does not cover".

§9.2 imports this section verbatim with `located` in place of `temporal`, and
inherits this scope with it: it governs the located companions — whose only
shape this revision defines is `"per-centroid"` (§9) — and never a coordinate
array.

**Peer joins** (contract). Two payloads compose as *peers* — like joined with
like, at one granularity — only when their temporal declarations **match on
`{shape, grammar}`** — the §2.0 weights-gate discipline, for the same reason:
words from two grammars, or from two shapes, describe different things, and a
merged array carrying either would mean neither. A reader or writer joining
declared payloads whose `shape` or `grammar` differ MUST refuse, and the
composed result carries its contributors' shape unchanged. (`spec` is
strict-checked before this rule is reached: an unknown revision is already a
refusal.)

**Shape-coarsening reductions are licensed, and are not peer joins**
(contract). A reduction folds a finer companion into a coarser one, so by
construction its output shape differs from its contributors' and it can never
satisfy the peer gate above. This revision licenses exactly one — the
`"per-centroid"` → `"per-cell"` fold that §8.3's closing *"Per-level shapes
need not match"* clause describes and that
[ruling 3 on #410](https://github.com/englacial/zagg/issues/410#issuecomment-5310502887)
first called for ("per-cell toc *range* at overview levels, even where leaves
are per-centroid"). That ruling was **amended on 2026-08-17**: zagg's own digest
pyramids keep companions `"per-centroid"` at every level, symmetric with the
located channel, so the reduction below is **licensed but unused by this
writer** — the terms stand unchanged for any producer that wants it, and a
reader must still implement the mixed-level case §8.3 permits. Its terms:

- The contributors MUST be peers **of each other**: every one declares
  `shape: "per-centroid"` under the same `grammar`. A reduction over
  contributors that fail the peer gate is itself a refusal.
- The output is a §8.2 array declaring `shape: "per-cell"` and the **same
  `grammar`** — a reduction coarsens the shape, never the word grammar, and
  never the `spec`.
- The output word for cell `i` is `toc_merge` reduced over the per-centroid
  words of cell `i`: the envelope §8.3's closing clause requires ("it is the
  envelope of the per-centroid words beneath it"), exact and order-independent
  by §8.2's join clause.
- No other cross-shape combination is defined. A writer MUST NOT invent one,
  and a reader MUST refuse any it meets under the peer rule above.

So **"matching" is never ambiguous**: a peer join matches its contributors
against *each other* and inherits their shape; a reduction matches its
contributors against each other and **declares its own, coarser output
shape**, which by construction does not match theirs. Which of the two is in
play is a property of the operation being performed, never something a reader
infers from the declarations it finds.

**An absent `temporal` key is never a refusal.** A payload, cell array or
digest that declares nothing composes freely with anything — this is the
schema-evolution rule that keeps every store written before this revision
conformant verbatim. Two consequences, both normative, and both binding on
reductions as well as peer joins:

- The composed result MUST NOT carry a temporal declaration unless **every**
  contributor carried one matching the others'. Dropping the channel is the honest
  outcome: a word that omits an undeclared contributor's observations is not
  a conservative envelope, and silently narrowing one is the failure this
  clause exists to prevent.
- An undeclared store supports **no temporal subsetting**. A reader MUST NOT
  infer an encoding, an epoch, or a window from anything else in the store,
  and MUST report that the query is unanswerable rather than approximate it.

---

## 9. `zagg-located/1`

**Status: contract** ([issue #410](https://github.com/englacial/zagg/issues/410)).
The **spatial** instantiation of §8's word-typed coordinate declaration:
attrs key `located`, `spec: "zagg-located/1"`, `grammar:
"mortie-morton/1"`. Everything §8 says about the pattern — the three keys
and no more, strict-checking, the declaring array being the array that holds
the words, informative extra keys ignored rather than refused — applies here
unchanged and is not restated. The marker is named for the domain, not for
the word type, precisely so it cannot read as a second declaration of the
store's primary morton axis: it is not one, per §8's carve-out.

```json
"located": {
  "spec": "zagg-located/1",
  "shape": "per-centroid",
  "grammar": "mortie-morton/1"
}
```

**`shape`** — `"per-centroid"` is the only value this revision defines: the
declaring array is the `{field}_locations` sibling of §1.1/§2.2, one word
per centroid row of the payload it accompanies, bound from the payload
array's `ragged` block `locations` key (§1.2). A reader MUST refuse a shape
it does not implement.

**An absent `located` key MUST be read as §2.2 verbatim** — kind-keyed
words (a point word a reported position carrying no area claim, an area word
a cell containing everything beneath it), **order-29 point-word ingest**,
area words only as fold products (the deepest common ancestor of the
members' **words**) — never as an unknown encoding, and never as grounds to
refuse. Every located store written before this revision is conformant as it
stands, no byte rewritten. What the declaration adds is self-description (a
generic reader learns the word grammar from the array rather than from this
page), the §8 shape vocabulary, **the coarse-ingest grant** (§9.1 — a
latitude §2.2 deliberately withholds, so its published semantics never
move), and the overview clause below, which §2.2 does not cover.

### The word grammar is mortie's

A `zagg-located/1` value is a mortie **morton word**: one `uint64` packing
the HEALPix nested cell and its order, with the order-29 point/area kind
carried by the encoding. The bit layout, the order range, the kind
convention and the raw-sort Z-order property are normative in mortie's
[`docs/specification.md`](https://github.com/espg/mortie/blob/main/docs/specification.md)
§1 and §4, and are **not restated here**. Unlike §8's toc citation, this is
the same class of citation §2.2 already makes: mortie's §10 *"Frozen for
1.x"* enumerates §1's bit layout and §4's encoding-carried kind convention
as immutable within that major version, so the `mortie-morton/1` token names
a grammar that cannot move under a conforming reader.

### 9.1 Leaf and overview words

**Contract.**

- A word's claim is keyed on its **kind** (§2.2): a **point word** is the
  observation's reported position, carrying no area claim; an **area word**
  is a cell known to contain every observation beneath it — the finest such
  cell its producer could establish, from cell-resolved ingest or from a
  fold. A centroid folded from a single observation carries that
  observation's word unchanged.
- **Coarse ingest is this declaration's grant.** An observation located to
  a position enters as its point word; one resolved only to a cell enters
  as that cell's area word — positions never narrowed into points, §8.1's
  discipline spatially. A store whose ingest words include area words MUST
  carry this declaration: undeclared ingest is §2.2's (order-29 point words
  only), so the absent-key route never widens under a reader's feet. (Zagg's
  writers ingest point instruments as order-29 point words,
  `HealpixGrid.assign`; no shipped config emits area-word ingest today —
  informative.)
- A merged centroid — at a leaf, at a spill-block close, or at any level of
  a pyramid — carries the **deepest common ancestor of its contributors'
  words**: a cell containing every observation merged into it, and the
  finest one those words establish (a fold reduces words, so an ingested
  coarse word bounds the result). Containment is the whole claim a reader may
  make from the word: it is not a centroid position, not a mean, not a cell
  the data fills, and not the finest cell containing the observations
  themselves.
- **Overview words sit at heterogeneous orders, within one array.** A fold
  coarsens only as far as its contributors force, so one overview array's
  words routinely carry different orders — an unmerged centroid's order-29
  point word beside a merged centroid's coarse area word. A reader MUST
  decode each word's order from the word itself (mortie §1/§4) and MUST NOT
  assume a uniform order per array, per level, or per store, nor infer one
  from the level's cell order. **No order uniformity is promised anywhere —
  leaf arrays included** (this declaration admits cell-resolved area-word
  ingest, and even §2.2's strict ingest leaves spill-folded leaf centroids
  coarse): a uniform order is an observation about particular bytes, never
  an inference.
- The ancestor reduction is exact and order-independent — point and area
  words share a path prefix, so mixed inputs compose under the one rule
  (§2.2) — but, as in §8.3, it does not lift the accompanying digest's
  composability class (§2.3): the words are exact **given** the centroid
  partition they describe.

### 9.2 Composition and merge legality

**Contract.** §8.4 applies verbatim with `located` in place of `temporal`:
declared companions compose only on matching `{shape, grammar}` and MUST be
refused otherwise; an absent declaration is never a refusal. The one
difference follows from the absent-key rule above — an undeclared located
sibling is not information-free the way an undeclared temporal one is, since
§2.2 already pins its word semantics, so composing a declared companion with
an undeclared one is legal **and** the result MAY carry the declaration: the
words mean the same thing on both sides. A reader that cannot establish that
— because §2.2 does not apply to the array in hand — MUST drop the
declaration rather than assert it.

---

## 10. `zagg-coverage-toc/1`

**Status: contract** ([issue #480](https://github.com/englacial/zagg/issues/480)).

The **temporal section of the store-root coverage sidecar** — the one object a
reader GETs to bootstrap discovery. Its carrier is the `morton-moc/1` root
envelope, whose body (`{spec, encoding: "ranges", order, source,
generated_at, ranges}`) is described in
[`hive_layout.md`](hive_layout.md#coverage) and whose word and decimal
grammars are mortie's; this page owns exactly one addition to it — a
**`temporal` key** — so that a spatiotemporal candidate query resolves from
metadata alone, before any leaf is opened.

Two tiers, both derived from the §8.3 `"per-centroid"` companions the leaves
already carry:

| tier | key | what | answers |
|---|---|---|---|
| 1 | `shards` | one toc word per populated shard — the join over that shard's sibling words | *which* shards hold data during a window |
| 2 | `digest` | a weighted t-digest over acquisition times — mass placed at the per-centroid toc envelope midpoints that are also its companion | *how much* data falls in a window |

Neither tier is new information and neither is truth: like the spatial ranges
beside them they are a **regenerable accelerator** over the leaf arrays (§8.3,
D9), written at end of walk while leaves stamp continuously, so a reader MUST
treat them under the same staleness posture as the `ranges` — a shard the
section does not list is not proof the shard has no data in the window.

**Absence is the whole-section rule.** A store with no temporal channel
carries no `temporal` key, and its root object is byte-identical to one
written before this revision. A reader MUST read that absence as "this store
publishes no temporal coverage" and MUST NOT refuse the store, the sidecar, or
a windowed query because of it — the standing absence posture of §8/§9,
restated here with its force. Absence of the `digest` sub-block alone says the
same thing one tier down: tier 1 stands without it.

**Versioned key discipline.** The section carries its own `spec` marker,
independent of the carrier's. A reader MUST strict-check it. Unlike the
array-level sections of this page, an unknown revision here reads as
**absent**, not as a hard failure: the section is an accelerator whose truth
is elsewhere, and this matches the strict-gate-then-degrade rule the sibling
coverage envelopes already use (the leaf stamp's `coverage` payload,
`hive_layout.md`). A future revision is therefore a new `spec` string and a
new section here; keys are never repurposed in place.

### 10.1 Section grammar

**Contract.**

```json
"temporal": {
  "spec": "zagg-coverage-toc/1",
  "source": "sweep",
  "generated_at": "2026-08-17T22:59:35+00:00",
  "fields": ["h_tdigest"],
  "cover": "zagg-coverage-toc-cover/1",
  "shards": {"11213": "10689250968998768172"},
  "digest": {
    "delta": 64,
    "weights": "counts",
    "value": "toc-ns",
    "element": {"dtype": "float32", "shape": [-1, 2]},
    "encoding": "base64",
    "centroids": 35,
    "weight_total": 346.0,
    "payload": "…",
    "times": "…"
  }
}
```

- **`spec`** (required) — `"zagg-coverage-toc/1"`, gated as above.
- **`source`** (required) — which producer wrote the section: `"sweep"` (the
  sweep's leaf walk) or `"refresh"` (the explicit whole-store rebuild). The
  vocabulary is open, exactly as the carrier's `source` is.
- **`generated_at`** (required) — ISO-8601 UTC. The section's own clock: it
  and the carrier's may differ, because a producer with no temporal
  contribution rewrites the ranges and leaves the section standing (§10.4).
- **`fields`** (required) — the sorted payload field names whose §8.3
  companions the **`shards` map** was derived from. It is the map's
  provenance, and it composes as a **union** across producers (§10.4): after a
  merge it names every field any contributing producer read, which is not
  necessarily the set the installed `digest` was built over. A reader MUST
  therefore treat it as an upper bound when applying the once-per-field weight
  rule of §10.3, not as a per-digest field list. Informative for tier 1 (the
  words are already unioned across fields by construction).
- **`shards`** (required) — tier 1, below.
- **`digest`** (optional) — tier 2, below.
- **`cover`** (optional, added under this revision — issue
  [#489](https://github.com/englacial/zagg/issues/489)) — the **presence
  marker** for the word-set cover *sibling object* (§10.5): its value is
  that object's `spec` string (`"zagg-coverage-toc-cover/1"`), telling a
  temporal consumer one GET ahead that a `coverage.toc` stands beside this
  root, and at which revision. It is a hint under the sidecar staleness
  posture, never a promise: a reader MUST fall back to tier 1 (or to
  opening leaves) when the object turns out to be absent, unreadable, or at
  a revision it does not implement — and, conversely, MAY probe for the
  object even without the marker (a marker-less root simply predates it).
  Pre-marker readers ignore the key under the unknown-keys rule above,
  which is what makes this an in-revision addition rather than a `spec`
  bump: no meaning of any existing key changed.

A reader MUST ignore keys it does not recognize; a producer MUST NOT put
anything under `temporal` that is not defined here or by a later revision.

### 10.2 Tier 1 — the per-shard envelope word map

**Contract.** `shards` maps a **shard id** to one **toc word**:

- keys are D1 decimal shard ids at the carrier's `order`, spelled exactly as
  the `ranges` endpoints are;
- values are toc words as **decimal strings**, for the same reason the range
  endpoints are strings: a `uint64` word exceeds 2^53 and a float-based JSON
  parser would silently mangle a raw number;
- the word is the grammar's join (mortie `toc_reduce`) over **every** §8.3
  companion word the shard's leaves hold, **unioned across all of the store's
  temporal-carrying fields** — coverage means "any data", not "data in this
  field". Where a shard is split into window leaves (`morton-hive/2`), the one
  word is the join over all of them: the map is shard-keyed, never
  window-keyed.

The word therefore **conservatively contains** every observation instant in
that shard, exactly as §8.2's per-cell envelope does one level down, and the
grammar's own predicates apply unchanged: `toc_overlaps` never under-reports
(every shard whose real content intersects the window tests true, possibly
with edge over-report of up to one quantum) and `toc_contains` never
over-reports. A reader MUST use those predicates on the words rather than
decoding to bounds and comparing itself.

The map lists only shards a producer actually walked. Its key set therefore
need not equal the carrier's `ranges` — a shard listed by `ranges` but absent
from `shards` is one whose temporal contribution has not been rolled up yet
(or one carrying no companion at all), and MUST be treated as *unknown*, i.e.
a candidate, never as *empty*.

That escape hatch is the *only* one: containment is a claim about a **listed**
shard, so a producer whose read of any input behind a shard's word FAILED —
one window leaf of several, one unreadable companion — MUST omit that shard
from `shards` rather than publish the word it managed to join over the rest.
Dropping the shard costs a reader one candidate it must open; publishing a
partial word costs it data it will never look for. A field a leaf simply does
not carry (one added to the store after that leaf was written) is absence, not
failure: it holds no observations, so the word over the remaining fields is
still whole.

> **Open question, flagged not decided** ([#480](https://github.com/englacial/zagg/issues/480)):
> whether a multi-field store should additionally publish **per-field** maps
> instead of (or beside) this union. The union is the default because coverage
> is a "is there any data here" question; a store whose fields have genuinely
> different temporal extents would be better served by per-field maps. If that
> case earns it, it arrives as an additional key under `temporal` (e.g.
> `shards_by_field`) in a later revision — `shards` keeps this meaning
> unchanged.

### 10.3 Tier 2 — the root time-digest

**Contract, optional.** The `digest` block is a t-digest over acquisition
times, carried in the store's **native** forms so a reader needs no grammar it
does not already implement for the leaves:

- **`payload`** is base64 of a §2.1 centroid array's bytes — the `(k, 2)`
  little-endian C-order `float32` buffer of §1.4, exactly what one ragged
  element holds — declared by `element` and `encoding` in the block. Rows MUST
  be sorted ascending by mean, as §2.1 requires.
- **`times`** is base64 of the row-aligned §8.3 companion: `k` little-endian
  `uint64` toc words, one per centroid, carrying the same claim §8.3 gives
  them (a single-observation centroid an exact timestamp, a merged one the
  `toc_merge` join over its members). `centroids` records `k`; a reader MUST
  refuse a block whose two buffers disagree on it (§1.1's row alignment,
  broken).
- **Column 0 is an instant on §8's internal nanosecond scale** (`value:
  "toc-ns"`), directly comparable with `toc2time` output and needing no unit
  conversion. It is **derived from the companion words, not measured from the
  observations**: each contributing centroid enters the fold at the MIDPOINT
  of its own §8.3 word's `toc2time` envelope, and a merged centroid's mean is
  the weight-weighted mean of those midpoints. Two consequences a reader MUST
  plan for:
  - a **weight-1 centroid is exact**. Its word is a timestamp under §8.3's
    kind-keyed semantics, `toc2time` returns `(t, t)`, and the midpoint is
    that instant. This is the one exact arm of the value axis.
  - every other mean is a **convex combination of envelope midpoints**, so it
    lies inside that centroid's own word but at no particular observation.
    The partition is the one the **value** distribution produced (zagg re-keys
    each §8.3 companion's existing digest onto its words), so a centroid's
    members are grouped by payload value, not by time: a heavy centroid whose
    members straddle a campaign gap places all of its mass at a point inside
    that gap, where the store may hold no data at all.

  Column 0 is also `float32`, carrying ~2^-24 **relative** precision — near
  present-day magnitudes a quantum of roughly ten minutes, enough that both
  statements above hold only up to that rounding. It is the smaller half of
  the same approximation, and deliberate: the **companion word beside each
  centroid is the exact temporal claim**, and a reader needing exactness MUST
  use the words, never the means.
- **Column 1 is a weight under the §2.0 `"counts"` declaration** (`weights`,
  restated in the block): observation counts, so `sum(weights)` — recorded as
  `weight_total` — is the total number of temporal observations the listed
  fields contributed, under §2.1's float32 representability bound. Where
  `fields` names more than one field, an observation that contributes to
  several of them is counted **once per field**; this is the same open
  question §10.2 flags, seen from the weight side. `fields` bounds that set
  from above rather than naming it exactly (§10.1): a digest installed by one
  producer sits beside a field list unioned over all of them.
- **`delta`** records the compression budget the fold used (64 in zagg's
  writer). It is provenance, not a promise about `k`.

The digest MUST be produced by ONE k-way merge over its contributors
(zagg: `zagg.stats.tdigest.merge_tdigests_kway` with the `temporal` channel),
so that it is permutation-independent in the contributors' order and its
companion words describe **the centroid partition that merge produced** — the
§8.3 exactness-given-the-partition rule, which is why the payload and its
companion MUST come from one call and MUST NOT be folded in separate passes.

Density over a window is then the existing algebra: a CDF difference over the
payload, with the companion words available to bound (and, near a window edge,
to correct) which centroids may legitimately contribute. Total weight is
**exact** — the fold conserves it, so `weight_total` is the observation count
however coarse the value axis is — while the placement of that weight along
the axis is only as time-resolved as the centroid partition above.

Gaps between campaign clusters stay visible in the **envelope words**: `k`
centroids carry `k` words, and a gap between two clusters shows as the
absence of any word covering it. That is a claim about the `times` buffer,
not about column 0 — the means can and do land inside a gap when a single
centroid's members straddle one. A reader answering "is there data in this
window at all" MUST read the words; the CDF answers "roughly how much",
resolved to the partition, and nothing finer.

### 10.4 Composition

**Contract.** The root object is written GET-union-PUT (incremental runs
accumulate; concurrent runs race benignly), and the section composes across
that seam as follows:

- **Tier 1 unions elementwise** under `toc_merge`: a shard on both sides
  merges to the join of its two words, a shard on one side carries over
  unchanged. The join is idempotent, so re-walking unchanged leaves reproduces
  the identical map.
- **Tier 2 is never unioned.** Its weights are counts, and merging two digests
  over overlapping shard sets would double-count them. It is **replaced**, and
  only by a producer whose own map covered every shard the merged map lists;
  a producer that covered only part of the store publishes no digest and
  leaves the standing one alone, and a merge that can find no whole-covering
  digest on either side drops the block rather than publish a partial one.
- **A producer with no temporal contribution at all leaves an existing section
  untouched** — it is not evidence of absence, only of a walk that did not
  look. Conversely a producer that overwrites the carrier wholesale (an
  unparsable or incompatible existing root — the D9 regenerable-cache rule)
  discards the stale section with it.
- **The `cover` marker carries through the merge**: the incoming section's
  value wins when both sides carry one, and a marker on either side alone
  survives — the object it points at composes under §10.5's own seam, so a
  producer that did not write the sibling must not erase the standing
  pointer to it. One exception, the succession rule below applied to the
  marker: an incoming marker naming the producer's OWN cover revision MUST
  NOT overwrite a standing marker naming a revision that producer cannot
  read. §10.5 preserves such an unknown-revision object on every write path,
  so the standing marker still names the object that is actually there;
  downgrading it would leave the carrier advertising a revision the sibling
  does not carry, contradicting §10.1's definition of the value, and would
  leave producers at two revisions rewriting the root object forever.
- **An existing section at an unknown revision is preserved verbatim, and
  never downgraded.** The strict `spec` gate above makes such a section read
  as absent, but composition is a *write*, and dropping the key is not the
  same as ignoring it: a producer MUST copy an unreadable standing section
  through unchanged, both when it has no section of its own and when the
  section it holds is at an older revision. This is the page's standing
  succession rule (readers add revisions, they never drop them) applied at
  the one seam a mixed-version fleet actually meets — without it the oldest
  producer in the fleet wins. An unmarked carrier (a `temporal` value with no
  `spec` string at all) claims no revision and is debris a producer MAY
  replace.

Conformance for an external reader is §7's `temporal/` fixture: its root
`coverage.moc` carries this section, and the fixture's `temporal.expected.json`
records the shard word and the decoded digest so the containment and weight
claims above are pinned on committed bytes.

### 10.5 The word-set cover sibling — `zagg-coverage-toc-cover/1`

**Status: contract** ([issue #489](https://github.com/englacial/zagg/issues/489)).

Tier 1 gives one envelope word per shard, so **gaps are invisible below shard
granularity**: a shard observed in 2019 and again in 2025 answers every window
in between. The cover restores the grammar's never-bridge law at sub-shard
resolution — per shard, a **canonical multi-word set** (`mortie.toc_normalize`
over that shard's §8.3 companion words, quantized as below) whose surviving
decoded gaps are floors on the true gaps. It is the multi-order-MOC analog on
the time axis: the §10.2 word is the overview, this is the index.

It lives in its **own root object**, `{store_root}/coverage.toc`, beside the
bootstrap sidecar — never inline: at the published CA store's shape it is
~50–60 words per shard (~1.5 MB store-wide, ~15× the tier-1-carrying
bootstrap object it sits beside, and ~300× a spatial-only pre-#480 root), and
a spatial-only reader must not pay that on every bootstrap GET. Temporal
consumers GET it on demand, discovering it through §10.1's `cover` marker.
Like the section it refines, it is a **regenerable accelerator** over the
leaf arrays (D9), under the same staleness posture: a shard it does not list
is *unknown*, a candidate — never *empty*.

**Contract.**

```json
{
  "spec": "zagg-coverage-toc-cover/1",
  "source": "sweep",
  "generated_at": "2026-08-23T02:41:00+00:00",
  "order": 4,
  "temporal_order": 18,
  "cap": 512,
  "fields": ["h_tdigest"],
  "element": {"dtype": "uint64", "shape": [-1]},
  "encoding": "base64",
  "shards": {
    "11213": {"words": "…", "count": 3}
  }
}
```

- **`spec`** (required) — `"zagg-coverage-toc-cover/1"`, the object's OWN
  marker (it is a root object, not a section, so it gates itself the way the
  carrier envelope does). A reader MUST strict-check it, and an unknown
  revision reads as **absent** — degrade to tier 1 or open leaves, never
  refuse the store. A future revision is a new `spec` string; keys are never
  repurposed in place.
- **`source`** / **`generated_at`** (required) — as §10.1's, the object's own
  provenance and clock.
- **`order`** (required) — the shard order of the `shards` keys, spelled as
  the carrier's `order` is; keys are D1 decimal shard ids at that order,
  exactly as §10.2's are.
- **`temporal_order`** (required) — the object's pinned quantization order,
  below. This revision's producers write **18**.
- **`cap`** (required) — the overflow cap the producer enforced, below. This
  revision's producers write **512**.
- **`fields`** (required) — provenance, with §10.1 `fields` semantics
  verbatim: the union of payload field names whose companions any
  contributing producer read; an upper bound, not a per-shard list.
- **`element`** / **`encoding`** (required) — the one byte grammar, declared
  once for every shard block: each `words` value is base64 of the words'
  §1.4 element bytes — `count` little-endian `uint64` toc words, exactly
  what one `zagg-ragged/1` uint64 element holds — so a reader decodes it
  with the leaf decoder it already has.
- **`shards`** (required) — one block per covered shard:
  - **`words`** — the base64 word set: the canonical (`toc_normalize`-form)
    cover of every §8.3 companion word the shard's leaves hold, unioned
    across the store's temporal-carrying fields (§10.2's union rule),
    quantized at the shard's effective temporal order. Sorted, duplicate-free
    uint64, mixed timestamp/range members legal under the grammar's
    kind-keyed semantics.
  - **`count`** — the word count. A reader MUST refuse a block whose buffer
    disagrees with it (§10.3's `centroids` rule, one buffer instead of two).
  - **`temporal_order`** (optional) — present **iff** this shard coarsened
    below the object's pinned order under the cap; absence means **the
    object's** `temporal_order` (not any order a reader's own build pins),
    which is also the ceiling: always ≤ the object's `temporal_order`, and a
    reader MUST refuse a block declaring an order above it.

**Quantization.** Temporal order `o` partitions the toc scale (2^63 ns from
the grammar's 1850 epoch — the span the 31-bit end code at 2^32 ns closes)
into `2^o` **aligned buckets** of `2^(63 − o)` ns. A word enters the cover
with its conservative decoded envelope (`toc2time`) **widened to the bucket
grid** — start floored, end ceiled — re-encoded as a range word, and the set
canonicalized with `toc_normalize`. Bucket bounds are exactly representable
on the grammar's own encoding grids for every `o ≤ 31` (a bucket start is a
multiple of 2^31 ns and its end of 2^32 ns), so the re-encoding adds no
rounding of its own; the one exception is the scale ceiling, where the top
bucket's end clamps to the grammar's maximum encodable end (`TOC_MAX_NS`) —
still containing every encodable input word.

The pinned **cover order is 18**: bucket span `2^45` ns ≈ 9.77 h
(espg-ruled on [issue #489](https://github.com/englacial/zagg/issues/489),
2026-08-24). The ladder is the grammar's own power-of-two structure; the
rung on it is chosen for the *consumer*, because nothing else constrains
it: correctness is order-independent (quantization only widens at any
order), and storage is flat (a pass is ~one word at any rung near this
one). Order 18 resolves consecutive-day revisits that ≥-day spans fuse,
and holds the epoch error of a cover-bucket midpoint to ±half a span
≈ ±4.9 h — small against the closest-observation Sentinel-2 consumer's
~4.3-day revisit cadence, where a ±19.5 h midpoint error (the ≥-1-day
rung, order 16) would not be. A future time-dense source is absorbed by
the cap below, never by re-pinning. Note the bucket span is not the *gap*
promise: a surviving gap needs a whole aligned bucket to itself (below),
so the **guaranteed gap floor is two spans**, `2 × 2^45` ns ≈ 19.5 h ≈
0.81 days.

Three consequences, all normative:

- **Widening only.** The cover's decoded coverage **contains** the shard's
  true observation set: quantization widens, `toc_normalize` is
  coverage-exact, and the union across leaves and fields is a union. A
  cover may over-claim — by strictly less than one bucket at each cluster
  edge — and MUST NEVER false-negative. `toc_overlaps` on the words
  therefore never under-reports, and `toc_contains` never over-reports,
  exactly as in §10.2.
- **Gaps survive at bucket resolution.** The never-bridge law holds on the
  quantized words, with an exact criterion: **a gap survives iff it contains
  a whole aligned bucket.** Widening lands the instants either side of a gap
  in their own buckets, and `toc_normalize` coalesces ranges that merely
  abut, so a gap of exactly one bucket span always closes; a gap of two
  spans or more always survives; and a gap between one and two spans
  survives only when it happens to straddle a bucket boundary the right
  way — alignment, i.e. data-dependent, so a consumer MUST NOT reason on it.
  The guaranteed floor a consumer may rely on is therefore `2 × 2^(63 − o)`
  ns — at the pinned order 18, ≈ 19.5 h. "Is there data in `[t0, t1)`"
  answers per shard from this object alone, down to that floor.
- **Quantization commutes with union and with the envelope join**, which is
  what makes the per-leaf fold exact (the cover of a union of leaves is the
  normalize of the union of their covers) and the parity invariant below
  well-defined.

**The cap.** A shard's block holds at most `cap` words. A producer whose
cover lands above it MUST coarsen **by order** — re-quantize at `o − 1`,
halving the bucket count — until it fits, recording the landing order in the
block's `temporal_order`. The same widening law (order 0 is a single bucket,
so the loop terminates); a producer MUST NOT truncate the word list instead,
which would silently drop coverage.

**Parity invariant.** For every shard listed both here and in §10.2's map,
the words MUST satisfy

> `toc_reduce(words)` = `toc_reduce(quantize({§10.2 word}, o))`

at the shard's effective order `o` — the cover's own envelope is exactly the
quantized tier-1 envelope. This follows from the commutation above and is
the cross-object consistency check a reader MAY apply cheaply; zagg's suite
asserts it on every shard it writes. (Plain equality with the §10.2 word
itself does NOT hold: that word lives on the grammar's native 2^31/2^32
grids, the cover on the bucket grid.)

**Composition.** The object is written GET-union-PUT across the same seam as
the carrier (§10.4), composing per shard: the union of the two word sets,
**re-quantized at the coarser of the two effective orders** (a union alone
could interleave two orders' bucket bounds and break the parity claim), then
re-capped — which may coarsen further, under the same widening law. A shard
on one side carries over unchanged. The §10.4 succession rules apply
verbatim at object level: an unknown-revision standing object is preserved
and never downgraded (on the refresh's replace path too); an unmarked or
unparsable one is debris a producer replaces — "unparsable" including a
standing object that is valid JSON but not a decodable cover (a block
disagreeing with its own `count`, a corrupt `words` buffer, a non-object
block), since the truth is in the leaves and this object is regenerable; a
producer whose read of any input behind a shard FAILED omits that shard
(§10.2's whole-word rule); a producer with no temporal contribution leaves
the standing object untouched.

`order` gates the union, exactly as it does for the carrier (§10.4): the
`shards` keys are D1 ids at that order and ids at two orders are not
comparable, so when the standing object's `order` differs from the incoming
one's the standing object is incompatible debris and the incoming side
REPLACES it wholesale rather than unioning. A re-shard therefore moves both
objects together instead of leaving the sibling holding two orders' ids
under one declared `order`.

The sibling's life is tied to the carrier's. A producer that DELETES
`{store_root}/coverage.moc` wholesale — the refresh path, when no stamped
leaf remains — SHOULD discard `{store_root}/coverage.toc` with it:
the §10.1 `cover` marker lives in the carrier, so a surviving sibling is an
orphan no reader discovers, and a later carrier at a different shape would
find ids it cannot key. A reader that reaches the sibling anyway is still
safe: an orphan only over-claims, and a cover shard absent from the carrier's
map composes under the standing staleness posture — **unknown, a candidate,
never authoritative**, exactly as a shard the cover does not list is unknown
rather than empty.

That licence is the deletion case only. A producer that merely REWRITES the
carrier while producing no cover of its own does not discard the sibling: it
has looked at no temporal input, which is not evidence of absence, and the
composition rule above governs instead — the standing object is left
untouched. Otherwise a defect that costs a walk its inputs (a manifest
missing `cell_order`, companions not yet written) would destroy a perfectly
good cover on the strength of the defect, and the two producers would answer
differently on identical evidence.

Conformance is §7's `temporal/` fixture again: it is the only fixture
carrying a `coverage.toc`, written by the production sweep writer beside its
`coverage.moc`, and `temporal.expected.json`'s `cover` block records the
decoded word set — derived from the generator's inputs through the
quantization law above, never transcribed — plus the parity claim on
committed bytes, the required keys above, and the `gap_ns` interval the
fixture's two clusters leave uncovered, which is what makes the object a
test of the never-bridge law rather than of a single bucket. The other fixtures carry no `coverage.toc` at all,
which pins the absence rule as bytes, exactly as §10's section absence is
pinned.

---

## 11. Icechunk companion repo

**Status: contract** (`zagg-icechunk/1`, issue
[#580](https://github.com/englacial/zagg/issues/580), stage 1 — refs-only,
additive; issue [#582](https://github.com/englacial/zagg/issues/582), stage
2 — the two-plane statement, run tags and finalize below). The leaves remain
the normative, self-describing data plane (§1–§10); the companion is a
derived index over their bytes.

**Two planes.** A hive store is two planes with different mutability:

- the **data plane** is the leaf shard objects — write-once under §1.5: no
  byte at a stamped leaf key is modified in place, and the one legal change
  is wholesale replacement under the same keys (a replaced leaf's earlier
  references fail loudly by checksum, §1.5/§11.3, so a run tag guarantees
  the leaves not replaced since it); a leaf's content identity is its §5.3
  O11 digest in the commit stamp plus the ETag every virtual reference
  carries (§11.3);
- the **metadata plane** is the one repository — authoritative for what
  *evolves*: convention and attrs blocks (the `dggs` `latitude` token, spec
  markers, `/1`→`/2` flips), the pyramid declaration mirrored as
  `multiscales`, and the run history (tags, §11.4). A leaf's own
  `zarr.json` keeps being written (a leaf stays a valid standalone zarr) but
  is **frozen with the leaf**: shape, dtype, chunking and codecs never
  diverge from the repo's array model; attrs may, by design, and the repo's
  are the ones a reader binds to.

"Backfill" is therefore not a category: an evolving fact is written to the
repo in a commit, never by rewriting leaves. Stage 2's remaining items —
native overview chunks, the metadata-commit operations, the virtual-target
sweep — are tracked on the issue.

**Succession.** A change to the repo's ARRAY MODEL is a `/2` revision,
declared — as `/1` is — in the `zagg_icechunk.spec` token (§11.1), so a
reader discriminates the two from the repo's own attrs; run tags and the
finalize commit are additive on `/1`. `/1` remains valid and readable
indefinitely: existing repos never require rewriting, whatever timing `/2`
lands on.

A morton hive is many leaf zarrs. The companion presents every leaf of one
order as **one zarr hierarchy** by recording each leaf's inner chunks as
[Icechunk](https://icechunk.io) **virtual chunk references** —
`(location, offset, length)` byte ranges into the leaf objects that already
exist — so an Icechunk reader (icechunk-py, icechunk-js in a browser) opens
the hive as a single array per field without moczarr's hand-built virtual
store, and every hive commit maps onto an Icechunk snapshot.

### 11.1 Placement and naming

**Contract.** One repository per store, at the store root, with one zarr
**group per level, named by the level's CELL order** — exactly the
manifest's `zagg-multiscales/1` datasets (§4.9) plus the base:

```text
{store_root}/icechunk/          <- the Icechunk repository
   /19                           <- the base: the source leaves (node order 9, cells 19)
   /13                           <- the §4.6 leaf columns' declared member (node 9, cells 13)
   /12, /11, … /4                <- the §4 overviews, one level per ancestor order (node 8 … 0)
```

| level (group) | artifact per node | node order `n` | cells per object `4^(c−n)` | object arrays |
|---|---|---|---|---|
| `/19` | `{leaf}.zarr/19/…` | 9 (shard) | 4^10, as K = 256 inner chunks of 4^6 (§1.5, sharded) | the leaf template's |
| `/13` | `{node}/all.pyramid.zarr/13/…` (the leaf's sibling column) | 9 | 256, ONE unsharded chunk | the column's declared member |
| `/12` … `/4` | `{node}/all.zarr/{c}/…` (the ancestor's overview) | 8 … 0 (= c − 4) | 256, ONE unsharded chunk | the overview's |

(production geometry: shard 9 / chunk 13 / cell 19, declared leaf-node
cells 13 — every ladder level keeps `c − n = 4`; the general rule is the
manifest's own `datasets`). A column holds more members than its declared
one — the within-footprint intermediates and the node-order partial the
stage gather reads (`13`, `12`, `11`, `10` on the live California store) —
and those are **deliberately not levels**: they overlap the overview levels
for the same cells and are sweep inputs, not reader-facing artifacts. A
level whose artifact a node never wrote (a column exists only under a `/2`
declaration with composable fields; an overview only once swept) simply has
no refs there and reads as fill. Every group is created by the once-per-run
init (§11.4) from the manifest's declaration, and the repo's **root attrs
mirror the manifest's `zagg-multiscales/1` block** verbatim as
`multiscales`, so a reader opens one repo and discovers every level — its
node order, cell order and artifact kind — from its root (the
GeoZarr/OME-NGFF-style multiscales convention;
earth-mover/icechunk-multiscales-demo; what gridlook's level resolver
reads). `icechunk/` is a **reserved store-root child name** on the same
footing as the §4.10 `multiscales/` companion: it is excluded from the **D19
product-name grammar** (like the base-component exclusion), so a
multi-product root walker can never classify it as a product, and
`zagg.hive.validate_product_name` refuses the name outright — a product MUST
NOT be named `icechunk`.

The repo's root group carries a `zagg_icechunk` attrs block that makes it
self-describing:

```json
"zagg_icechunk": {
  "spec": "zagg-icechunk/1",
  "shard_order": 9, "chunk_order": 13, "cell_order": 19,
  "url_prefix": "s3://bucket/product/",
  "commit": "ladder", "commit_order": 6, "split_order": 6,
  "levels": {
    "19": {"node_order": 9, "artifact": "leaf",     "chunk_order": 13, "cell_order": 19, "split": {"chunks": 16384, "order": 6}},
    "13": {"node_order": 9, "artifact": "column",   "chunk_order": 9,  "cell_order": 13, "split": {"chunks": 16384, "order": 2}},
    "12": {"node_order": 8, "artifact": "overview", "chunk_order": 8,  "cell_order": 12, "split": {"chunks": 16384, "order": 1}},
    "…":  "one entry per level, keyed by cell order",
    "4":  {"node_order": 0, "artifact": "overview", "chunk_order": 0,  "cell_order": 4,  "split": {"chunks": 1,     "order": 0}}
  }
},
"multiscales": [ … the manifest's zagg-multiscales/1 block, verbatim … ]
```

`shard_order` / `chunk_order` / `cell_order` mirror the manifest and the
base grid; `url_prefix` is the virtual chunk container's prefix (§11.3);
`levels` carries, per level group (keyed by cell order), its node order,
its artifact kind, its chunk-axis order (the inner-chunk order for the base,
the node order for a one-chunk-per-node level), its cell order and its
manifest split (§11.5); `commit`, `commit_order` and `split_order` are
the ladder's knobs (§11.4, §11.5), read back by every stage node so the
sweep needs no config. The repo root group's attrs are exactly these two
keys: `zagg_icechunk` and the `multiscales` mirror (the mirror is absent
only on a store whose manifest declares no `zagg-multiscales/1` block). A
leaf root group carries only its own commit stamp, which is a per-leaf fact
and so has nothing to mirror. Each **level group** mirrors its artifact's
resolution-group attrs verbatim (the `dggs` block, `zarr_conventions`), and
**never the commit stamp**.

### 11.2 Array model

**Contract.** For every named array a level's template declares (a leaf
array `{cell_order}/{name}` for the base, a column's declared-member array
`{c}/{name}` for the column level, an overview's `{c}/{name}` for an
overview level), the repo holds `/{c}/{name}` whose metadata is the
template array's, re-rooted on the whole sphere at that cell order (`n` the
level's node order, `n_shards = 12·4^n`, the level's global shape
`12·4^c`):

| field | repo array | derivation |
|---|---|---|
| `shape` | `(n_shards · L₀, *L[1:])` | `L` the level's per-node object array's shape (the leaf's for the base); `n_shards = 12·4^n`, `n` the level's node order |
| chunk shape | the object's **inner** chunk shape | the `sharding_indexed` codec's `chunk_shape` when the object array is sharded (every base leaf, §1.5), else its own `chunk_grid` — the whole object for a column or overview level, one chunk per node |
| `codecs` | the **inner** codec chain | the `sharding_indexed` wrapper is absent; `[bytes]` for dense fields, `[vlen-bytes, zstd]` for `zagg-ragged/1` (§1.3) |
| `data_type`, `fill_value`, `dimension_names`, `attributes` | verbatim from the object array | so a §1.2 `ragged` block, §2.0 `weights`, §8/§9 declarations bind identically |

Zarr metadata therefore passes through untouched: a reader that decodes a
leaf array per §1–§3 decodes the repo array the same way, chunk by chunk.

### 11.3 Chunk index law and refs

**Contract.** Every level's chunk axis is in **canonical nested order**
(§1.5 "Subtree spans"), so one node's chunks are one contiguous run. For a
level of cell order `c` and node order `n`, an object at nested rank `r` at
order `n` (`r ∈ [0, 12·4^n)`, the rank `block_index` gives) holds `4^(c−n)`
cells; its chunk `j` (C-order within the object's chunk grid along the cells
axis, `j ∈ [0, C)`, `C` the object's chunk count) sits at global chunk index

```text
r · C + j        (trailing axes keep their object-local chunk index, 0 for a single-chunk payload dim)
```

For the base level `C = 4^(chunk_order − shard_order)` inner chunks; for a
column or overview level the object is ONE chunk, `C = 1`, and the global
index is the node's rank itself.

At the production geometry (shard 9 / chunk 13 / cell 19) `C = 256`.

Each **populated** inner chunk is recorded as one virtual reference:

- **sharded leaf array** (every hive leaf, §1.5): `location` is the leaf's
  single shard object — the array's **outer**-chunk key, `{leaf}/{p}/c/0` for
  the 1-D cells arrays the hive writes — and `offset`/`length` are the
  chunk's entry in the shard index suffix, the same two `u64` words the §1.5
  2-GET recipe reads — the writer fetches that suffix itself, once per array
  (§11.4). An inner chunk the index marks **absent** (the
  `2^64 − 1` sentinel in both words) gets **no reference** and reads as
  `fill_value`.
- **regular (unsharded) array** — a leaf array on a `chunk_inner`-less
  grid, and every column and overview array (one chunk per object):
  `location` is the chunk object (`{leaf}/{p}/c/{j}`; `{object}/{p}/c/0` for
  a single-chunk array), `offset` 0, `length` the object size (§11.4); a
  missing object gets no reference.

Chunk keys are the array's own, under the `chunk_key_encoding` its
`zarr.json` declares — zagg emits the `default` encoding with the `/`
separator throughout — so the chunk at grid index `(i₀, i₁, …)` is

```text
{leaf}/{p}/c/{i₀}/{i₁}…
```

with the trailing axes at their **leaf-local** chunk index (`0` for a
dimension the array holds in a single chunk). A 1-D array's key is therefore
`c/0` for its one chunk, and `c/{j}` where the cells axis is chunked. Keys
are used **verbatim** as the path part of `location` (`//` and `.`/`..` are
preserved), so a character that is reserved in a URL but part of the key MUST
be percent-encoded — `?` → `%3F`, `#` → `%23`, `%` → `%25`
(`set_virtual_refs_arr`). zagg's own keys contain none of the three; the rule
is normative for a reader reconstructing a key from a recorded `location`.

Every reference also carries a **checksum**, in the form its container
validates. Into an **object-store** container it is the **ETag** of the
object it points into, read from the HEAD the writer performs against that
object after the leaf write (§11.4). Into a **`file://`** container it is
that object's **`last_modified`**, ceiled to the next whole second: Icechunk
compares a recorded datetime against the object's modification time at
**whole-second granularity**, so the exact sub-second `mtime` fails the very
object it was read from while the ceiling passes it. Icechunk verifies either
form on read, so a reference into a **legacy** leaf that has since been
wholesale-replaced (§1.5 — same keys, new bytes) fails loudly rather than
decoding the replacement at a stale offset. That granularity is the local
form's one caveat: a replacement landing within the **same second** as the
original passes the check (an object store's ETag has no such window). On a
**versioned** leaf (§1.5; a stamp naming `current`) the recorded `location` is the
version subgroup's object — `{leaf}/run-{run_id}-{attempt}/{p}/c/0` — which is never
rewritten and never touched (§1.5), so the checksum is a guard against
out-of-band tampering only and neither a replacement nor a lifecycle touch
invalidates an earlier reference. The writer records which form it used under
`icechunk.checksum` in the leaf's stats sidecar (`"etag"` or
`"last_modified"`).

`location` is `url_prefix + key`, `key` the object's path relative to the
store root. The repo declares exactly one **virtual chunk container** whose
`url_prefix` is the store root URL **with a trailing `/`**
(`s3://bucket/product/`, `file:///…/product/`); a reader authorizes that
prefix with the same credentials it reads the leaves with. That prefix is
**absolute**, so the companion does not relocate with the store the way the
root-relative manifest and MOC sidecars do: a mirrored or moved store must
declare the container prefix afresh (and the recorded `url_prefix`, §11.1) at its
new location before its refs resolve.

### 11.4 Commits

**Why not one commit per leaf (informative).** A per-leaf commit does not
scale. At the full-globe worst case (3,145,728 order-9 leaves, 49,152
order-6 cells) it is 3.1M commits — and the real limit is not the commit
count but the **snapshot**: an Icechunk snapshot lists every manifest, so its
size is set by the manifest count. One manifest per order-6 cell per array is
≈442k manifests ≈ **44 MB read on every open, every rebase and every
commit** (≈2.2 TB of snapshot traffic over one run). Balancing snapshot bytes
(≈100 B per manifest entry) against per-manifest bytes (≈2.5 KB on disk per
leaf-array) gives leaves-per-manifest ≈ 0.6·√N: ≈30 at California scale
(order 6–7 cells), ≈1,000 at the full globe (order-4 cells → 27k manifests, a
2.7 MB snapshot and 2.6 MB of manifests — the shape of the live ISMIP repo,
27,423 manifests / 2.2 MB snapshot). So both the manifest split and the
commit granularity are configurable and derived, and a **base** manifest is
written by **exactly one commit** (§11.5).

**Contract — the ladder.** Refs travel up the §4 pyramid the way the digest
columns do:

1. **The leaf worker writes a ref sidecar, not a commit.** After the leaf's
   stamp, its granule-id sibling and the §4.6 column fold — last in the unit,
   behind every post-stamp phase that can still fail it — the worker computes
   the leaf's ref plan (the reads below) and writes it as one compact object
   beside the leaf, named by the stats sidecar's sibling grammar with the
   base `icechunk_refs.json` (`{stem}.icechunk_refs.json` under `morton-hive/3`).
   The carrier is JSON declaring `zagg-icechunk-refs/1`, the writer's
   geometry (`shard_order`, `chunk_order`, `cell_order` — never the
   container prefix, which is vetted at the repo) and the entries (≈40 KB per
   leaf at production geometry). It is a **writer-internal carrier**, not part
   of the reader contract — the repo is. No Icechunk session is opened on
   the leaf path.
2. **Stage nodes gather.** Each dispatch node of the staged sweep (§4,
   `zagg.sweep_stages`) reads its subtree's carriers — the leaf sidecars at
   the finest tuple, its children's **node ref columns** (the same carrier at
   `{node}/icechunk_refs.json`) above — and adds the refs of the overview
   objects at every order of its tuple beneath it (read the same way a
   leaf's are). A missing carrier is counted (`icechunk_missing`) and
   tolerated — under-coverage, never a failure — and a carrier whose
   recorded geometry disagrees with the repo's block is refused.
3. **One tuple commits.** Let `c` be `commit_order`, `d` a tuple's dispatch
   order and `d′` its child order (the tuple covers orders `[d, d′)`):
   - `d ≤ c < d′` — the **committing tuple**: every node commits all it
     gathered in **one commit** — every level's refs into its group of the
     same repo, keyed by cell order (§11.1): the base leaves into
     `/{cell_order}/…` (`/19`), the column's declared member into `/{c}/…`
     (`/13`) and each overview level into `/{c}/…` (`/12` … `/4`) —
     message `node {decimal}`;
   - `d′ ≤ c` — a coarser tuple: its nodes commit only their **own**
     overview refs (their children already committed);
   - `d > c` — a finer tuple: its nodes write their node ref column and
     commit nothing.

   Commits rebase on conflict exactly as before (`ConflictDetector`; nodes
   touch disjoint chunk ranges), and each stage row records
   `icechunk_commits`, `icechunk_rebases`, `icechunk_commit_s`,
   `icechunk_refs`, `icechunk_missing`, `icechunk_failed`,
   `icechunk_skipped_levels`, `icechunk_clean`, `icechunk_regathered` and
   `icechunk_s` — every one
   pre-seeded, so a row's key set does not depend on whether a node did work
   — plus `icechunk_nodes`, one entry per node that did, carrying the
   snapshot it committed (the leaf→snapshot join).

   The ladder runs over the **dirty set** — the nodes with a dirty leaf
   beneath them — not over every candidate node: a node whose whole subtree
   is clean had its refs committed by the run that dirtied it, so it is
   skipped whole and counted (`icechunk_clean`). An append therefore costs
   O(dirty), not O(store). A **dirt-only** leaf (§11.6) joins the dirty set
   for the ladder alone: a node with dirt-only leaves and no dirty one runs
   the gather and commit, folds nothing, and is counted
   (`icechunk_regathered`). A full re-gather (repairing a repo against the
   leaves, after a run whose commits were lost) is a **manual staged sweep
   over the whole store**, where every leaf is dirty by construction.

`commit_order` defaults to the **finest dispatch node** of the staged sweep
(`shard_order − tuple_width` when the shard order is a multiple of the width:
6 at production), `split_order` to `commit_order`. Both ride the
`zagg_icechunk` block, with different standing: `split_order` is the
**store's** value and ratchets (§11.5); `commit` and `commit_order` are
**per-run** — the init writes this run's values so its stage nodes can read
them, and they are never a compatibility key. `commit: "leaf"` keeps the per-leaf commit of the
first revision — the leaf commits `leaf {decimal}` itself and the ladder
commits overview refs only. An unset `commit` resolves to the ladder only
when the run walks it — the dispatcher chains the staged sweep
(`output.sweep: "stages"`) and a `/2` ladder with a composable field is
declared — and to `"leaf"` otherwise, on every backend: a ladder-mode run
that walks no ladder would leave the repo empty while every leaf reported a
sidecar.

**Init.** `init {run_id}` — the once-per-run initialization, before the
fan-out: the repo exists with **every** level group (§11.1 — the base, the
column's declared member and one per declared overview level, keyed by cell
order, created here once because Icechunk's create is not safe under
concurrent callers), its array nodes (§11.2) defined and the `multiscales`
mirror in its root attrs. Idempotent: a repo that already carries a matching
block is reopened, never re-templated; a block for another geometry or
container is refused. The ladder settings are not compared: `split_order`
follows the §11.5 ratchet (a finer config adopts the store's value, a
coarser one re-cuts), and `commit` / `commit_order` are per-run.

**Finalize.** `finalize {run_id}` — the once-per-run close
(`mode="icechunk_finalize"` on Lambda, in-process on the local backend),
invoked by the dispatcher AFTER every commit of the run has landed: after
the staged sweep returned under the ladder (its finisher makes the last
commit), after the fan-out drained under `commit: "leaf"`. Never by a stage
node — tags, expiry and collection are singleton repo operations, and never
inside the staged sweep's finisher, which is lease-scoped, load-bearing
store-root machinery while the repo is fail-open (and which a per-leaf run
does not have). On the Lambda backend the stage nodes are `Event` invokes,
so a staged sweep that did not complete — its dispatch failed, a barrier
expired or the finisher did not land — may still have node commits in
flight: the dispatcher then does NOT finalize and records
`icechunk_finalize: {skipped: <reason>}` and the run stays untagged (its
commits are kept; the next run's tag covers them). One finalize, in order:

1. **retention** — `output.icechunk.retain_runs` = K. `0` (the default)
   keeps every run and does nothing here. K > 0: the run tags beyond the
   K − 1 newest are deleted (only `run-` tags, only here), snapshots older
   than the oldest retained run's finalize commit **expire**
   (`expire_snapshots`), and repo objects no retained snapshot references
   and older than it are **collected** (`garbage_collect`) — the cutoff is
   always a run tag's commit time, never "now", so a concurrent writer's
   in-flight objects are never collected. Its **session** is not protected:
   a writer (or reader) whose base snapshot is older than the cutoff loses
   that base, and its commit fails on rebase (a storage error, not a
   conflict — no retry recovers it; the leaf's or node's refs are lost
   fail-open until a re-gather). The cutoff is never newer than the previous
   run's finalize (K = 1 or 2; a larger K reaches further back), so the only
   casualty is a session opened before the previous run's finalize and still
   open across this one — overlapping runs on one store. The default K = 0
   never expires anything. Expiry squashes every commit
   older than the oldest retained run's finalize — that run's own `init`,
   leaf and stage-node commits included — into that finalize snapshot; the
   runs newer than it (the K − 1 newest, this one included) keep their
   intermediate commits until a later finalize's cutoff passes them. History
   therefore reads one snapshot per run only up to the oldest retained tag,
   and per commit after it. Neither step touches a virtual target: Icechunk
   manages none of the leaf objects (the virtual-target sweep is a stage-2
   item on the issue). Retention is fail-open inside finalize: an error (two
   finalizes racing on one tag delete, a collection cut off by the invoke's
   ceiling) is recorded as `retention_error` and steps 2 and 3 still run;
2. one content-free **`finalize {run_id}` commit** whose commit metadata
   identifies the run — `run_id`, `semantic_hash` (the D19 digest the leaves
   were stamped with), `zagg_version`, the block's `commit` /
   `commit_order` / `split_order` — and records the retention counts
   (`retain_runs`, `tags_deleted`, `snapshots_expired`, `gc`,
   `retention_error`), so the repo is its own durable run record (`ancestry(tag=…)` answers "which config
   built this");
3. the **tag `run-{run_id}`** on that commit. Tags are immutable and keyed
   by the run, not the semantic hash: every append under one template
   shares a hash, so a hash-named tag would collide on the second run.

Idempotent: a finalize whose tag already exists returns it and writes
nothing. Fail-open (D9) at the dispatcher like the init; the record rides
the run summary as `icechunk_finalize` (`{path, tag, snapshot, tagged,
retain_runs, tags_deleted, snapshots_expired, gc, retention_error,
rewrite_pending, commit_s}`, `{error}` or `{skipped}`), not the run parquet,
whose write precedes the staged sweep on both backends; the tag itself is the durable outcome.
`rewrite_manifests` is NOT run by finalize: a §11.5 split ratchet is a
rare, deliberate, whole-repo operation with no run to attach to (at the
globe it may exceed one invoke), so finalize reports it as
`rewrite_pending: {from, to}` for the operator step.

A **replaced** leaf is re-indexed by the run that replaces it (per-leaf
commit or the next staged sweep): the new refs supersede the old ones on
`main`. On a **legacy** leaf (§1.5) snapshots older than that commit still
reference the replaced keys — and because clear-then-template reuses those
keys, their refs resolve to a **live** object rather than 404ing; the per-ref
checksum (§11.3) is what makes that a loud failure instead of a silent
mis-decode. On a **versioned** leaf the old refs name the superseded
version's objects, which stay in place, so every earlier snapshot and run
tag keeps reading. Icechunk's garbage collection never touches a virtual
target; the **virtual-target collector** (`tools/icechunk_gc_targets.py`,
operator-run, dry-run by default) deletes a leaf's version subgroups that
are neither its `current` nor referenced by any **retained** snapshot,
reports the reclaimable bytes, never touches a legacy leaf, and refuses a
store without a repo. **Retained** means every snapshot in the ancestry of
every branch and of every tag — all tags, not only `run-` ones — after
expiry; a version only an expired snapshot references is collectable.
**In-flight guard:** the collector never deletes a version whose stamp
`written_at` is newer than the newest `run-` tag's finalize commit, since an
in-flight run's versions have no tag yet. An unstamped version (an attempt
that died before (2)) has no `written_at`; the collector reclaims it only
once its own run's `run-{run_id}` tag exists. A crash between (3) and (4)
of the §1.5 write order therefore leaves a version that is either
**referenced** (per-leaf mode — retained until expiry drops it) or
**unreferenced but young** (ladder mode — guarded); once its run is
finalized without the version being gathered it is garbage, and the next
collection reclaims it. A **converted** legacy leaf — one whose first
versioned write left the legacy `{cell_order}/…` arrays at its root — keeps
those root arrays forever: they are not a version, the collector never
reclaims them, and any earlier snapshot that references them keeps reading,
since nothing rewrites them.

**The reads the writer performs.** Recording refs costs I/O — the offsets
come out of the object's own index, but the sizes and checksums do not. For
each array of a leaf (at the leaf worker) or of an overview object (at its
stage node) the writer issues:

- **sharded array**: one ranged `GET` of the shard index suffix (the §1.5
  recipe's second read, which yields every inner chunk's `(offset, length)`
  at once) **and** one `HEAD` of the shard object, for its checksum — the
  `ETag`, or `last_modified` on a local store (§11.3);
- **single-chunk array** — every column and overview array (one chunk per
  object): one `HEAD` of its one chunk object, for its size (the ref's
  `length`) and checksum;
- **multi-chunk regular (unsharded) array** — a leaf array on a
  `chunk_inner`-less grid: one `LIST` of the array's `c/` chunk prefix, which
  yields every chunk object's key, size and checksum in one request — and,
  unlike probing, discovers which chunks were actually written.

Plus, per stage node, one small `GET` per child carrier. Small beside the
leaf write, but not nothing.

Writing refs is **fail-open** (D9) at every rung: a leaf's sidecar failure
is logged and recorded in its D20 stats sidecar (`icechunk.error`) and never
fails the leaf; a stage node's failure counts `icechunk_failed` and never
fails the sweep — the leaf is normative, the index is regenerable (a later
staged sweep re-gathers). The leaf's `icechunk` block records the sidecar it
wrote — `{sidecar, bytes, refs, arrays, checksum}` — or, under
`commit: "leaf"`, its commit `{path, snapshot, arrays, refs, rebases,
commit_s, checksum}`; `{skipped: reason}` for a unit stage 1 does not index
(§11.6); `{error: message}` on failure. The run parquet flattens the same
fields to `icechunk_*` columns.

### 11.5 Manifest splitting

**Contract.** Manifests are split along the chunk axis into runs of `4^m`
chunks, and `m` is fixed **once, from the base level**:

```text
m_base = chunk_order_base − split_order          (7 at production: 4^7 = 16,384 chunks)
m      = min(m_base, chunk_order_level)          (per level)
```

Because the chunk axis is in nested order, one run of the **base** is
exactly the chunks of one HEALPix cell at order `split_order`, so the split
is stated as "one base manifest per order-`split_order` cell". Every other
level holds the same **number of chunks** per manifest — not one
`split_order` cell — capped at its own chunk axis (`chunk_order` is the
group's own: the inner-chunk order for the base, 13 at production; the node
order for a column or overview group, one chunk per node). At the defaults
that is one order-2 cell per manifest at `/13`, one order-1 cell at `/12`,
and one base cell (12 manifests per array) at `/11` and every coarser level;
no manifest ever spans more than a base cell. Holding the chunk count
constant is what keeps the manifest count — and with it the snapshot, which
lists every manifest — at the base's: applying "one manifest per
`split_order` cell" to every level would multiply both by the number of
levels (≈5× at production).

The split is configured **per order group** (Icechunk's path-matched split
conditions, `^/{order}/`) and recorded per level as `levels.{order}.split`
(`chunks` = `4^m`, `order` = `chunk_order − m` — the cell order one
manifest spans, which differs per level by design) and once as
`split_order` in the `zagg_icechunk` block (§11.1), never assumed by
readers. `split_order` MUST satisfy `commit_order ≤ split_order ≤
shard_order`: the committing node (§11.4) owns every leaf under its order,
so a **base** manifest keyed to a cell at or below it is written by that one
commit and no other — **zero rewrite amplification** at the only heavy
level. A coarse-level manifest spans more than one committing node's
subtree and is rewritten by each node that touches it (a rebase on disjoint
chunks, `ConflictDetector`); that is deliberate: those manifests are tens of
KB, so the amplification is negligible in bytes, while their **count** is
what every snapshot read pays for.

**Contract — the ratchet.** The store's recorded `split_order` is
authoritative and moves **one way, toward coarser**. At `init`, the run
config's `split_order` is compared with the block's: a config value FINER
than the store's (numerically higher) is not applied — the init adopts the
store's value and logs a warning naming both (templates are hash-pinned build
configs reused by appends; refusing would break every append after a
ratchet); an equal value is a no-op; a COARSER value is an intentional
ratchet: the init records the new `split_order` and per-level splits in the
block and in the repo's saved splitting config **before any commit of the
run**, so every manifest the run writes is already at the new cut, and
flags the run record with `icechunk_split_ratchet: "{from}->{to}"`, which
the run's finalize (§11.4) reports as `rewrite_pending` for an **operator**
`rewrite_manifests` pass over the old manifests (not run by the writer or
by finalize; mixed cuts are valid — each manifest carries its own extents). A repo is never re-split finer. `split_order` is
a layout knob outside the D19 semantic core: it changes no leaf byte.
`commit_order` is per-run and unchecked beyond `split_order ≥ commit_order`.

At the defaults (`split_order = commit_order = 6` at production) a
shard-order manifest is `4^7` = 16,384 chunks — one order-6 cell, 64 leaves —
which is ≈43 order-6 cells × 9 arrays ≈ 390 base manifests at California
scale. The coarse levels add a few hundred (`/13` at one order-2 cell per
manifest, `/12` at one order-1 cell, 12 per array at `/11` and coarser), so
the snapshot is ≈390 + a few hundred entries; at the full-globe worst case it
is ≈442k + ~1k, not the ≈2.2M a per-level `split_order` cut would list.
The **global-scale** setting is `split_order: 4, commit_order: 3`: `4^9` chunks
per manifest, one order-4 cell (1,024 leaves), 3,072 manifests per array
(27k across the arrays, a ≈2.7 MB snapshot), committed by the 768 order-3
nodes (768 commits) — the numbers §11.4's rationale derives.

*(Informative.)* Icechunk deduplicates a manifest's `location` strings only
from `min_num_chunks` chunks upward (a configurable setting defaulting to
1,000, not a format constant). The **default** split clears it comfortably —
one manifest per order-6 cell is `4^7` = 16,384 chunks per array, 64 leaves.
The finest admissible split does not: `split_order == shard_order` is one
whole leaf, `4^4` = 256 chunks per array at production, below the 1,000
default, so such a manifest carries no dictionary. The dictionary is worth
having precisely because all refs of one leaf array carry the **same**
`location` (the leaf's single shard object), so a hand-set
`split_order == shard_order` trades it away.

### 11.6 What §11 does not cover (informative)

Windowed leaves (`{id}_{window}.zarr`, `morton-hive/2`) share a shard rank
across windows and so cannot share one chunk axis; stage 1 records no refs
for them (the sidecar says `skipped: windowed`) and the ladder does not run
on a windowed store. Raster hive products (`(time, cells)` arrays, never
sharded) are likewise out of stage 1's writer scope. Both are tracked as a
`/2` array-model revision (a leading window dimension per level; issue
[#584](https://github.com/englacial/zagg/issues/584)); the finalize and
tags of §11.4 apply to them unchanged once they have a repo. The sweep-built §4
overviews are **in** scope since the ladder (§11.4): every declared overview
level has its group in the store's one repo, and its refs. None of these change the leaf format, the
t-digest storage or the moczarr reader.

**Scale (informative).** Snapshot size is proportional to the manifest
count (≈100 B per entry) and manifest bytes to leaves per manifest
(≈2.5 KB per leaf-array), which is what §11.5's split rule balances; a
per-level squash is not available in one repo (a snapshot lists every
manifest, `rewrite_manifests` is whole-repo, there is no branch merge), so
finalize is one whole-repo pass per run. **One repo per o3 region** — o6
nodes committing into their region's repo, the hive manifest becoming a
repo catalog, each region keeping the group-per-level layout — is the
documented escape hatch should a single repo's snapshot ever be the
bottleneck; it is never a default at any store size (espg, 2026-09-26): the
split ratchet governs that dimension first, and the hatch trades "one zarr"
for "one zarr per region". Whether it is ever needed is read off the first
global run's per-node `icechunk_rebases` / `icechunk_commit_s` stage rows.

A skip-if-current **legacy** unit's lifecycle touch (#388) refreshes its
objects in place and so moves their checksums (§11.3); the unit re-plans its refs from
fresh HEADs — committed at once under `commit: "leaf"`; under the ladder it
rewrites its sidecar and enters the same run's staged sweep as a
**dirt-only** leaf, whose nodes re-gather and commit its refs without
re-folding their overviews (§11.4). On the fleet the worker's response body
carries `icechunk_dirty`, the dispatcher assembles the dirt-only set from
those bodies, and each stage event carries its node slice as `dirt_only`
(`[[shard_key, window], …]`, absent when empty). A current unit therefore still writes no
stats record, sidecar or sub-map, but may enter the sweep work set as
dirt-only when its touch moved ref checksums and the repo is on. A **versioned** unit (§1.5) never
enters this path: its touch reaches no version object, moves no ref checksum,
and so is never dirt-only — its `main` refs stand as committed.
