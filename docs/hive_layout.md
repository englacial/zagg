# Hive store layout (morton-hive/1, /2)

zagg can write each dispatch shard as its **own self-describing leaf zarr**
under a morton digit tree, instead of into one shared flat store. The layout is
the write-side half of the sparse-coverage design record
([`docs/design/sparse_coverage.md`](design/sparse_coverage.md) §2–§3, decisions
D1–D6); the convention itself is owned by the mortie spec and versioned as
`morton-hive/1`. A store that declares a **time-window schedule** (D13–D15,
[Time windows](#time-windows-morton-hive2) below) is `morton-hive/2` — a
strict superset: a `/1` store *is* a `/2` store with `schedule: none`.

It is the **default for HEALPix output** (issue #253) — point aggregation
and the raster pipeline alike (issue #247; the hive/flat split sits one
abstraction above the pipeline kind): an omitted `output.store_layout`
resolves to `hive`. An explicit `store_layout: flat` (the single shared
store) remains for interop/debug but is deprecated — removal is gated on the
sparse-DGGS read path (issue #251 phase 3);
rectilinear grids keep the flat shared store. Hive is
wired to **both backends** — the local runner and the Lambda handler share
the same per-shard write path (see [Status](#status)).

## Layout

```
{store_root}/
  morton_hive.json               <- static manifest (root-only exception)
  {sign+base}/{d1}/.../{d_n}/    <- one decimal digit per level (D2)
    {full_id}.zarr/              <- vanilla zarr v3 leaf, one per shard (D3)
    {full_id}_{window}.zarr/     <- time-windowed leaf (D13, morton-hive/2)
```

- **Ids are morton decimal strings** (D1): sign + base digit (`1..6` /
  `-1..-6`), then one digit per order, digits `1..4`, never `0`. A string
  prefix *is* the spatial ancestor, so cross-resolution containment is
  `fine_id.startswith(coarse_id)` — arithmetic, not I/O.
- **One digit per path component** (D2), so shards at mixed orders nest
  naturally: every order is a legal node.
- **Full id at the leaf** (D3): `.../-5/1/1/2/3/3/3/-5112333.zarr` is
  self-describing without parsing its directory chain — greppable in
  inventories, unambiguous if moved. Each leaf is a vanilla zarr v3
  store: the same group/array template as the flat layout, sized to one shard
  (dense arrays hold `cells_per_shard` cells; `resolution: chunk` companions
  hold the shard's K inner chunks). When `sharded` (the K > 1 **default**,
  matching flat — [issue #236](https://github.com/englacial/zagg/issues/236))
  each dense array is ONE `ShardingCodec` object spanning the whole leaf,
  written at leaf block 0. A ragged field's vlen-bytes array is one whole-leaf
  object whenever K > 1 — **sharded or not**
  ([issue #209](https://github.com/englacial/zagg/issues/209)), independent of
  the dense `sharded` toggle. The
  ShardingCodec is itself vanilla zarr v3, so the leaf stays self-describing.
  One recorded exception
  ([issue #200](https://github.com/englacial/zagg/issues/200), O8): the
  `coverage.moc` occupancy-bitmap sidecar inside the leaf — a single foreign
  key that zarr readers ignore (data reads are unaffected; member
  enumeration like `members()`/`tree()` emits a `ZarrUserWarning` and skips
  it).
- **Node invariant** (D5): below the root, a node contains *only* digit
  children (`[1-4]/`) and `*.zarr` objects — zero zarr metadata above the
  leaf, no shared mutable state across workers. The root alone also carries
  the manifest (and, in a follow-on, `coverage.moc`). `zagg.hive`
  re-checks every computed leaf path against this invariant before writing.

## Config

```yaml
output:
  store: s3://bucket/product        # becomes the hive root
  store_layout: hive                # the HEALPix default (issue #253); may be omitted
  grid:
    type: healpix                   # hive is HEALPix-only (morton digit tree)
    parent_order: 9                 # shard order -> tree depth
    child_order: 13                 # cell order
```

`sharded` output ([docs/sharding.md](sharding.md)) is supported and is the
default whenever `chunk_inner` gives K > 1 — same contract as flat
([issue #236](https://github.com/englacial/zagg/issues/236)): each leaf's
dense arrays collapse to one object apiece instead of K per-inner-chunk
objects PUT onto a single leaf prefix. An explicit `sharded: false` opts the
**dense** arrays back into K streaming objects — the ragged vlen array stays
one whole-leaf object regardless
([issue #209](https://github.com/englacial/zagg/issues/209)); an explicit
`sharded: true` at K == 1 validates and is a no-op (nothing to bundle — the
leaf is byte-identical either way).

Validation rejects `hive` with a rectilinear grid (node names are morton
digits) and with `consolidate_metadata: true` (there is no store-root zarr
hierarchy to consolidate — D5/D12). (The manifest's `shard_order` field below
records the dispatch/tree order — it is not a config knob.)

## Time windows (morton-hive/2)

A store may partition each shard's time series into **one write-once leaf per
window** ([issue #246](https://github.com/englacial/zagg/issues/246), design
D13–D15; grammar and boundary semantics frozen on the
[mortie spec page](https://github.com/espg/mortie/issues/62#issuecomment-4986809092)).
Windowed leaves keep full D4/D5 semantics — stamped, binary debris, zero
shared state — so **backfill** is just a new earlier-window leaf, concurrent
runs on different windows share no object, and the window is the unit of
idempotent reprocessing (re-dispatching a window replaces its leaf wholesale).

```yaml
output:
  store_layout: hive
  windowing:                        # absent = schedule none = morton-hive/1
    schedule: yearly                # none | yearly | monthly | daily | explicit
    time_field: delta_time          # per-observation timestamp column
                                    #   (a declared data_source column)
    epoch: "2018-01-01T00:00:00Z"   # dataset zero as an ISO-8601 UTC instant
    scale: gps                      # utc (default) | gps | tai
    units: seconds                  # seconds (default) | days
    windows:                        # explicit schedule only:
      - {label: melt-2019, start: "2019-06-01", end: "2019-09-01"}
      - {label: melt-2020, start: "2020-06-01", end: "2020-09-01"}
```

- **Leaf naming is frozen**: `{full_id}_{window}.zarr`, underscore separator,
  parse by splitting on the FIRST `_`. Generative labels are ISO-derived and
  hyphen-free (`2025`, `202511`, `20251103`), so lexicographic order =
  chronological order; explicit labels are opaque (`[0-9A-Za-z-]{1,32}`) and
  decode only through the declared list. `quarterly` is grammar-reserved but
  not implemented (validation rejects it).
- **Boundaries are UTC calendar terms, half-open `[start, end)`.** Window
  bounds are converted to dataset units once at dispatch, using the declared
  `epoch`/`scale`/`units` and a fixed scale offset (`GPS−UTC = 18 s`,
  `TAI−UTC = 37 s`; stdlib `datetime` has no leap-second table) — boundaries
  are accurate to ≤ 1 leap second, none declared since 2017.
- **Dispatch fans one work unit per (shard, window).** The ShardMap's
  per-granule `time_start`/`time_end` subset granules per window; inside the
  worker an observation-level filter on `time_field` (a pair of structured
  `ge`/`lt` predicates riding the ordinary filter machinery) splits
  boundary-straddling granules exactly — an observation on a boundary instant
  belongs to the *later* window. Legacy shardmaps without granule times
  dispatch every granule to every window (the filter keeps it correct) and
  need `bounds.temporal` to enumerate generative windows.
- **Stamps carry the truth, the manifest the schema** (D15): each windowed
  leaf's commit stamp records its `window` label and the ACTUAL written
  `time_range` as ISO-8601 UTC strings; the root `coverage.moc` summary
  carries the run's time-range union (cache, regenerable); temporal *extent*
  never lives in the manifest, which stays write-once. Appending a new year
  to a `yearly` store adds leaves the schedule already describes — no
  manifest touch; the explicit list is the noted exception (appending outside
  it re-templates).
- **Coverage gains `encoding: "full"`** (D14): a popcount at stamp time marks
  a fully-occupied subtree — no bitmap sidecar object is written, and readers
  short-circuit the exact intersection through the shard's own MOC
  membership. Partial shards keep the bitmap sidecar.

Validation: `output.windowing` requires the hive layout on a healpix grid;
`time_field` must be a declared `data_source` column (the worker can only
filter what it reads); explicit windows must be well-formed (frozen label
grammar, `start < end`, unique labels, disjoint ranges). On the raster path
([issue #247](https://github.com/englacial/zagg/issues/247)) membership is
the acquisition's STAC `datetime`: `time_field` is optional (fixed to
`datetime`) and the `epoch`/`scale`/`units` conversion knobs are rejected.
Changing the windowing of an existing store fails the frozen-key
manifest check like an orders change — clear the root first.

## The manifest (`morton_hive.json`)

Written **asynchronously at init**
([issue #252](https://github.com/englacial/zagg/issues/252) hybrid): the
local dispatcher writes it directly before dispatch; the Lambda leg fires
the existing `mode: "setup"` hive branch as a fire-and-forget Event invoke
immediately after the `mode: "ping"` preflight passes, so the manifest
typically lands within seconds of init (best-effort: the Event invoke shares
worker concurrency and runs retries-0, deferring to the finalize backstop
under throttling or a dropped invoke) and a reader can start consuming
completed leaves while the store builds. Finalize re-ensures it as an
**idempotent backstop** (a
frozen-key-matching manifest is accepted — no second PUT): worker Event
invokes run with retries 0, so a lost async init write self-heals at end of
run, and a run that crashes mid-fan-out still left a manifest at init.
Otherwise never touched during a run (D6); the read-only frozen-key precheck
(`zagg.hive.validate_manifest`) still runs before the fan-out so an
incompatible existing store refuses up front on reruns (two concurrent first
writes into a fresh root now collide within seconds of init, not at the
losing run's finalize).
With the manifest, every shard
path is computable arithmetically with zero requests:

```json
{
 "spec": "morton-hive/1",
 "dataset": {"short_name": "ATL03", "version": "007"},
 "cell_order": 13,
 "shard_order": 9,
 "split_schedule": [1, 1, 1, 1, 1, 1, 1, 1, 1],
 "pyramid": {"orders": [], "aggregation": {}},
 "generated_at": "2026-07-10T12:00:00+00:00"
}
```

`split_schedule` is implicit under D2 (one digit per level down to the shard
order) but recorded explicitly for forward compatibility. `pyramid` is
declared-only in round one: overview zarrs are generated by a later
post-process sweep (D11), never at fan-out time.

A windowed store ([Time windows](#time-windows-morton-hive2)) additionally
declares `spec: "morton-hive/2"` and a `temporal` block — schedule,
`time_field`, `epoch`/`scale`/`units`/`calendar`, the explicit windows list,
and the append policy. Temporal *extent* is deliberately not manifest data
(D15): actual ranges live on leaf stamps (truth) and the root summary (cache).

A rerun into an existing root verifies the manifest's **frozen keys** match
the run's own configuration (`spec`, `dataset`, `cell_order`, `shard_order`,
`split_schedule`, `temporal`) and fails loudly on a mismatch — the hive analogue of the
flat layout's shard-map signature guard. The sweep-mutable `pyramid` block and
the `generated_at` timestamp are deliberately excluded, so a swept store still
resumes (and the sweep's pyramid declaration is preserved, not clobbered).

**Re-templating does not remove existing leaves.** Overwriting the manifest
replaces *only* the JSON — committed leaves written under the old
configuration would survive, stamped and walker-discoverable, and (because
mixed shard orders are legal under D2) indistinguishable from intentional
data. The writer therefore refuses an overwrite that changes the frozen keys
while the digit tree has any `{sign+base}` children (one delimiter-LIST):
clear the store root, or pick a new one, before writing with a different
configuration.

## The commit stamp

S3 has no empty directories and LIST is strongly consistent, so **absence is
trustworthy**: a delimiter-LIST with no digit children means nothing finer
exists. **Presence is not** — a worker that dies mid-shard has already created
the `.zarr/` prefix. So the shard's *final* write is a root
`group.attrs.update(...)` recording completion (D4):

```json
"morton_hive_commit": {
  "spec": "morton-hive/1",
  "complete": true,
  "cells_with_data": 412,
  "granule_count": 17,
  "written_at": "2026-07-10T12:03:41+00:00"
}
```

A leaf whose root metadata lacks the stamp is **debris**: incomplete,
ignorable, safe to overwrite on retry (the writer re-emits the leaf template
with `overwrite=True`, so retries are idempotent). This is *not* consolidated
metadata — one small PUT rewriting the root `zarr.json`, which the leaf
template creates anyway. A shard that errors, or streams no chunks (no data),
leaves no stamp; a fully empty shard leaves no `.zarr/` prefix at all (the
leaf is created lazily on the first chunk write).

The stamp also carries the shard's **coverage envelope** — see
[Coverage](#coverage) below. The sidecar it points to is written before the
stamp, so coverage shares the debris semantics: no stamp, no visible coverage.
A windowed leaf's stamp ([Time windows](#time-windows-morton-hive2)) declares
`spec: "morton-hive/2"` and adds `window` (the label) plus `time_range` — the
actual `[t_min, t_max]` written, as ISO-8601 UTC strings.

## Coverage

Where the data is, declared hierarchically
([issue #200](https://github.com/englacial/zagg/issues/200), design §4 as
amended by PR #206; O8/O9 resolved on the issue thread). Three tiers per
shard plus one store-root object:

| tier | what | where | cost to read |
|---|---|---|---|
| 0 — morton box | canonical ≤ 4-member cover of the occupied cells (DCA children, each tightened) | `coverage` payload on the commit stamp | free — rides the stamp GET readers already make |
| 1 — exact bitmap | zstd-compressed bit field over the shard subtree at `cell_order` | `{full_id}.zarr/coverage.moc` sidecar | one opt-in GET |
| 2 — exact truth | the leaf's `morton` coordinate array | the leaf's data plane | array read; the tiers above are indexes, never truth (D9) |
| root | shard-order ranges MOC over all completed shards | `{store_root}/coverage.moc` | one GET — the discovery bootstrap |

**Leaf envelope** (on the stamp, `zagg.hive.read_coverage`; strict
`spec: morton-moc/1` gate — unknown specs read as absent):

```json
"coverage": {
  "spec": "morton-moc/1",
  "box": ["-42113221", "-42113224", null, null],
  "cell_order": 12,
  "source": "worker",
  "encoding": "bitmap",
  "sidecar": "coverage.moc",
  "nbytes": 213,
  "raw_nbytes": 512
}
```

`box` is always exactly 4 slots, nulls trailing; members are D1 decimal
strings. `encoding`/`sidecar`/sizes appear only when the bitmap exists — a
box-only envelope (phase-1-era leaf, or a depth-0 `child_order ==
parent_order` config) is read as "box only". No `generated_at`: the stamp's
`written_at` is the one clock and one writer. Bit convention (frozen with
the mortie-side spec): bit i = the i-th shard-subtree cell in ascending
packed-word order (base-4 value of the D1 digit tail, digits 1..4 → 0..3),
MSB-first per byte. A corrupt sidecar (bad zstd, wrong size) **raises**; a
missing one degrades to `None` — a truncated bitmap must never read as a
plausible partial cell set. The sidecar is the one foreign key inside the
otherwise-vanilla leaf: zarr data reads are unaffected, but member
enumeration (`members()`/`tree()`) emits a `ZarrUserWarning` and skips it.

**Root envelope** (`{store_root}/coverage.moc`, `zagg.coverage.load_coverage`):

```json
{
  "spec": "morton-moc/1",
  "encoding": "ranges",
  "order": 6,
  "source": "dispatcher",
  "generated_at": "2026-07-10T22:59:35+00:00",
  "ranges": [["5112333", "5112333"], ["-4211321", "-4211324"]]
}
```

The example above is `zagg.hive.build_root_coverage` output for the shards
`-4211321..-4211324` plus `5112333` (all order 6) and round-trips through
`root_coverage_words`; the test suite parses it straight out of this file so
the reference example can never drift from the implementation.

A range is an inclusive run of same-order cells within one base cell,
consecutive in digit-tail rank; endpoints are decimal **strings** (packed
u64 words exceed 2^53 and raw JSON numbers get mangled by float-based
parsers). `source` is `"dispatcher"` (end-of-run write) or `"refresh"` (the
explicit walk rebuild); the sweep will add its own.

**Reader flow** (`zagg.coverage`): `load_coverage` → `root_coverage_and`
against the AOI to pick candidate shards (one GET, no walk); per leaf,
`box_and` on the stamp payload for the cheap reject, then `bitmap_and` for
exact cell-level filtering (falls back to the box verdict with `None` when
the leaf is box-only), then the `morton` coordinate as truth. The box is a
conservative superset — false positives cost one wasted read, false
negatives are impossible; the bitmap and the root MOC are exact for what
they list.

**Staleness (O7)**: readers trust silently on the hot path. The root object
is written fail-open at **end of run** while leaves stamp continuously, so
the most common gap is benign — a run still in progress. Beyond that, a
crashed run, an out-of-band write, or the benign concurrent-run union race
(GET-union-PUT is not atomic; last writer wins until the next re-union)
leaves it missing shards, which degrades to "reader doesn't see the newest
run", never a wrong answer. `zagg.coverage.warn_if_stale` implements the
lazy detection lean: when a reader opens a commit-stamped leaf the root MOC
doesn't list, it warns once per store and suggests
`zagg.coverage.refresh_root_coverage` — the explicit delimiter-LIST walk
that rebuilds the root MOC from the stamped leaves (debris excluded) and
writes it with `source: "refresh"`. No reader ever auto-walks (D10).

**Deploy note** (the sync-invoke analogue is the `mode: "ping"` preflight,
which replaced the PR #205 setup echo — issue #252): the Lambda leg posts
one fire-and-forget `mode: "coverage"` invoke, which requires the
redeployed function. An **older deployment 400s the event in its process
handler** — a logged error line in CloudWatch, but no writes, no result
mirror, and no async redelivery — so the failure is fail-open by
construction; the root object simply doesn't appear until the sweep or a
refresh builds it.

## Raster hive stores (issue #247)

Raster (pull-NN) pipelines write the same tree with **windowed `(time, cells)`
leaves**: one vanilla zarr v3 leaf per **(shard, window)** unit at
`shard_leaf_path(root, shard, window=label)`, each carrying leaf-local `time`
(int64 microseconds, CF attrs) and `cell_ids` coords plus one
`(T_leaf, cells_per_shard)` array per configured band, chunked
`(1, cells_per_chunk)`. The leaf's time axis is the unit's **own acquisition
groups** — known at dispatch from the catalog, so both dispatchers produce
identical leaves — and its coords are written at template time (nothing is
deferred to a per-shard coords pass). There is no flat global template on the
hive branch: template time writes only `morton_hive.json` (D5/D6).

Differences from the aggregation path, all espg-ratified on
[issue #247](https://github.com/englacial/zagg/issues/247):

- **Window membership is the acquisition's STAC `datetime`**, decided at
  dispatch — there is no per-observation timestamp column, so no
  observation-level filter is injected. `output.windowing.time_field` is
  optional (fixed to `datetime`, which the manifest temporal block records);
  the `epoch`/`scale`/`units` conversion knobs are rejected (STAC datetimes
  are already ISO-8601 UTC). An acquisition *group* (entries sharing a
  `time_key` — one datatake's adjacent MGRS tiles) belongs to the window
  containing its earliest datetime within the shard, so a group never splits
  across leaves at a boundary.
- **Schedule `none` is supported for consistency**
  ([ratified](https://github.com/englacial/zagg/issues/247#issuecomment-5007157978)):
  one bare `{full_id}.zarr` leaf per shard carrying the full time axis;
  re-run = whole-leaf replacement; D14 `"full"` gated off exactly as
  aggregation gates it. The append cost (a re-run rewrites the whole leaf)
  is the user's explicit choice, visible in the manifest.
- **Coverage is popcount-decided per D14** from the spatial union of the
  unit's acquisitions (per-timestep validity stays data-plane nodata, D9):
  an interior shard — every child cell covered — stamps
  `encoding: "full"` with **no sidecar PUT**; an edge-of-scene/swath shard
  writes the real bitmap sidecar.
- **The D15 stamp truth** is the window label plus the actual ISO-UTC
  `[min, max]` of the unit's acquisition datetimes and the acquisition
  count; the root `coverage.moc` unions the per-leaf ranges as cache.
- **`sharded: true` is permanently excluded** on the raster path (not
  deferred): per-timestep slab streaming would read-modify-write each
  `ShardingCodec` object once per timestep, and raster object count is
  time-axis-dominated anyway.

The shared worker (`zagg.processing.raster.process_and_write_raster_hive`,
the raster analog of `process_and_write_hive`) runs identically under the
local dispatcher and the Lambda `mode: "process_raster"` hive branch; hive
events carry no `time_index` (the leaf axis is unit-local) plus an optional
`window`, while flat raster events stay byte-identical to pre-#247 runs. The
manifest rides the same ping → async-setup → finalize-backstop lifecycle as
aggregation ([issue #252](https://github.com/englacial/zagg/issues/252)).

## Reading a hive store

There is no store-root `zarr.open()` (deliberately — D12; a root hierarchy can
be added later by the sweep as a derived artifact). Readers:

1. GET `morton_hive.json` (once, cacheable) → `shard_order`, `cell_order`.
2. GET `coverage.moc` (`zagg.coverage.load_coverage`) → the covered shard
   set, intersected with the AOI (`root_coverage_and`) — see
   [Coverage](#coverage).
3. Compute a shard's leaf path by string arithmetic on its decimal id
   (`zagg.hive.shard_leaf_path`), open the leaf zarr, and **check the commit
   stamp** (`zagg.hive.read_commit`) before trusting the contents; the
   stamp's coverage payload pre-filters the AOI (`box_and`/`bitmap_and`).
4. Discovery without a root MOC falls back to the delimiter-LIST walk:
   recurse on `[1-4]/` children; a `*.zarr` entry is data at that node; no
   digit children ⇒ nothing finer. Never LIST per observation in a join
   loop (D10).

The store-root `coverage.moc` ([issue #200](https://github.com/englacial/zagg/issues/200)
phase 3, default-on for hive) removes the walk from the bootstrap path: one
GET of the root object yields the shard-order coverage MOC (JSON ranges,
decimal-string endpoints). It is written fail-open at end of run — by the
dispatcher directly (local) or one fire-and-forget `mode: "coverage"` worker
invoke (Lambda; an older deployment has no coverage mode and 400s the event
in its process handler — logged, no writes, no async retry — which is safe:
the object is a regenerable cache under D9, and readers degrade to the walk).
Incremental runs union with the existing object; concurrent runs race
benignly (GET-union-PUT is not atomic: last writer wins, and its union may
miss the loser's shards until the sweep or the next run re-unions — accepted
under D9/O7). The §7 sweep remains the authoritative rebuilder.

## Status

- **Both backends** write hive stores end-to-end through the same
  `zagg.hive.process_and_write_hive` code path. On **Lambda**
  ([issue #199](https://github.com/englacial/zagg/issues/199) phase 3)
  the manifest write fires as an async `mode: "setup"` Event invoke at init,
  with `mode: "finalize"` as its idempotent backstop
  ([issue #252](https://github.com/englacial/zagg/issues/252) hybrid; a
  lightweight `mode: "ping"` preflight keeps the pre-fan-out fail-fast) —
  the orchestrator still needs no S3 access — and each worker derives its leaf
  path from its `shard_key` + the event config's orders, emits its own leaf
  template, and stamps completion as its final PUT. The async status channel stays at the flat
  sibling prefix (`{store_root}.status/<run_id>/…`), outside the digit tree.
- **Coverage ships** ([issue #200](https://github.com/englacial/zagg/issues/200)
  phases 1–4): the tier-0 morton box on the commit stamp, the exact
  zstd-bitmap `coverage.moc` sidecar inside each leaf, the end-of-run
  store-root `coverage.moc` (shard-order ranges MOC, `output.coverage_moc`,
  default on for hive) for the one-GET bootstrap, plus the `zagg.coverage`
  reader primitives (per-tier AOI intersection, O7 staleness lean, explicit
  refresh).
- **Dense arrays shard inside the leaf**
  ([issue #236](https://github.com/englacial/zagg/issues/236)): hive output is
  byte-identical to the flat sharded layout — one `ShardingCodec` object per
  dense array per leaf (plus one per ragged field,
  [issue #209](https://github.com/englacial/zagg/issues/209)), the default at
  K > 1, so a leaf costs one PUT per dense array instead of K per-inner-chunk
  PUTs concentrated on a single prefix.
- Write-throughput validation at fleet scale is tracked with the benchmark
  machinery in [issue #202](https://github.com/englacial/zagg/issues/202).
