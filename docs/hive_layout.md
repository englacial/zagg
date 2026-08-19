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
      - {label: scene-a, timestamp: "2021-03-14T12:00:31.024Z"}  # point form:
                                    #   the acquisition's OWN instant, copied
                                    #   from the item — not a rounded-off one
```

- **Leaf naming is frozen**: `{full_id}_{window}.zarr`, underscore separator,
  parse by splitting on the FIRST `_`. Generative labels are ISO-derived and
  hyphen-free (`2025`, `202511`, `20251103`), so lexicographic order =
  chronological order; explicit labels are opaque (`[0-9A-Za-z-]{1,32}`) and
  decode only through the declared list. `quarterly` is grammar-reserved but
  not implemented (validation rejects it).
- **An explicit entry may be a point** ([issue #355](https://github.com/englacial/zagg/issues/355)):
  `{label, timestamp}` is sugar for the second-wide half-open `[t, t + 1s)`,
  for time axes that are effectively discrete (single acquisitions, scene
  timestamps, isolated campaign instants). Each entry declares *exactly one* of
  `timestamp` or `start`+`end` — mixing them in one entry is rejected. The
  desugaring happens in `zagg.config.get_windowing`, so the manifest and every
  downstream consumer only ever see an ordinary `{label, start, end}` window.
  One second is the grammar's own resolution (boundaries are whole seconds
  throughout), which also keeps membership off float equality on observation
  timestamps (a sub-second `timestamp` normalizes to the whole second
  containing it). *Consequence*: two acquisitions within the same wall-clock
  second share a window, and two point entries inside one second are rejected
  as overlapping — as is a point that lands inside an already-declared range
  window, which is an ordinary overlap (see the validation paragraph below).
  If that ever bites, the fix is an explicit `width` key on the point form, not
  a guessed default; until it exists, any key on an entry beyond its own form's
  (`label`/`timestamp` or `label`/`start`/`end`; `width` included) is rejected
  rather than ignored.
- **A point window that matches nothing fails SILENTLY** — it covers only the
  whole second containing `t`, so the declared `timestamp` must be the
  acquisition's own instant, copied from the item, not a rounded-off
  approximation of it. STAC datetimes are rarely round seconds, so this is the
  likely first-contact mistake: `timestamp: "2021-03-14T12:00:00Z"` matches
  nothing for a scene whose `datetime` is `2021-03-14T12:00:31.024Z`. On the
  point pipeline the window still dispatches and the worker's `ge`/`lt` filter
  matches nothing, leaving an empty leaf; on raster the group is dropped at
  dispatch (`runner._raster_windowed_units`) and no work unit, leaf, or warning
  is produced at all. Neither path errors.
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
grammar, `start < end`, unique labels, disjoint ranges — point entries are
desugared first, so a point landing inside another window's range is a genuine
overlap and is rejected). Range bounds render at whole-second granularity
exactly as a point `timestamp` does — **each bound truncates to the second
containing it** — so a fractional bound silently retimes that edge of the
window: `[12:00:00.0Z, 12:00:01.5Z)` dispatches as
`[12:00:00, 12:00:01)`, dropping the declared half second of coverage at the
tail. A range is never widened to recover the truncated fraction. The one
case refused outright is total collapse: when *both* bounds render to the
same second (say `12:00:01.0Z` → `12:00:01.4Z`) the window dispatches as an
empty `ge x`/`lt x` pair, so it is rejected — the point form is the spelling
for one-second intent. A sub-second range that *straddles* a second boundary
(`12:00:01.9Z` → `12:00:02.1Z`) is still valid; it renders to the one-second
window its truncated bounds describe. On the raster path
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
 "semantic_hash": "9f1c…",
 "cell_order": 13,
 "shard_order": 9,
 "split_schedule": [1, 1, 1, 1, 1, 1, 1, 1, 1],
 "path_grouping": 1,
 "pyramid": {
  "spec": "zagg-pyramid/1",
  "overview": {"spacing": 2, "orders": [7, 5, 3, 1], "all_time": false,
               "fold_source": "cascade", "exact_levels": 1, "fields": {"…": "…"}}
 },
 "generated_at": "2026-07-10T12:00:00+00:00"
}
```

`split_schedule` is implicit under D2 (one digit per level down to the shard
order) but recorded explicitly for forward compatibility. `pyramid` carries
the overview family's order schedule and each field's D24 composability class
(issue #201) — or, under `zagg-pyramid/2`, its grouped `(node, cells)` level
schedule ([Pyramid overviews](#pyramid-overviews-zagg-pyramid2), issue #382); an
`overview` with an empty `orders` list is the declared-*off*
form. It is **declaration** only — overview zarrs are generated by a later
post-process sweep (D11), never at fan-out time, and the sweep adds its
`materialized` actuals under the same key. Declaring it is not birth-only:
see [Retrofitting the pyramid declaration](#retrofitting-the-pyramid-declaration).

`fold_source` declares **how** the sweep builds those levels (issue #376).
Under the default `cascade`, only the finest `exact_levels` (default 1) fold
from the source leaves; every coarser level folds the level below it, so the
leaves are read once for the whole pyramid and a node's fold input is its 16
child overview slabs rather than its entire subtree. The cost is precision in
the `approximate` class — a cascaded t-digest is a merge of merges — which is
why each level records the regime that made it, in the overview's own attrs
and in `materialized.fold_sources`. The pre-#376 behavior is
`fold_source: leaves` (every level re-folds the raw leaves, exactly);
it is **deprecated** and kept only as an explicit opt-in:

```yaml
output:
  pyramid:
    orders: [7, 5, 3, 1]
    exact_levels: 1        # cascade boundary; 1 is the default
    # fold_source: leaves  # deprecated: exact at every level, unbounded per-node input
```

### Pyramid overviews (`zagg-pyramid/2`)

The declaration also comes in a second revision
([issue #382](https://github.com/englacial/zagg/issues/382); design record
[issue #381](https://github.com/englacial/zagg/issues/381), as collapsed by
the espg grammar ruling on the declaring PR): **`overviews`** — the **leaf
cell resolutions**, and nothing else. A scalar is sugar for one resolution;
a list is strictly descending, each member strictly between `parent_order`
and `child_order`; omitted, the default is one resolution at the grid's
resolved chunk order:

```yaml
output:
  pyramid:
    overviews: [16, 13]   # leaf cell resolutions; replaces orders/spacing WHOLESALE
    # overviews: 13       # scalar sugar for [13]
```

There is no above-shard configurability: everything coarser than the shard
is the **fixed every-order ladder** — with `d` = coarsest leaf resolution −
`parent_order`, every order from `parent_order − 1` down to 0 carries one
member at `order + d`. A `/2` store is therefore inherently a
**multiresolution statistical grid**: every HEALPix order from 0 through
the base plus the native resolution, each level spec-guaranteed and
individually addressable (`pyramid: false` is the single-resolution
opt-out; skipping levels when *reading* is a reader-side choice, never a
store property). Spelling `overviews` writes the manifest block as
`zagg-pyramid/2` with the **fully expanded** `(node, cells)` list at block
level — `pyramid.overviews`, the store-wide product declaration; readers
never re-derive the ladder — while the singular `pyramid.overview` family
dict keeps the sweep leg's execution regime (`all_time`, the #376 fold
keys, `fields`, `materialized`) exactly as under `/1`. Since the issue #384
default flip, **omitting the knob also declares `/2`** — at the grid's
resolved chunk order — whenever that order is strictly interior to the
shard's resolution window; raster configs, explicit legacy
`orders`/`spacing` schedules, and K == 1 grids keep `/1`. The grammar's **validation rules** and
the ladder law are normative in
[the specification §4.4–§4.5](specification.md); refusals are loud and
named, never silently widened. Two practice points:

- **Declaring is free.** A declared-but-unswept level costs nothing
  (declared intent and swept actuals are separate — [#381 point
  (11)](https://github.com/englacial/zagg/issues/381)), and the fixed
  ladder makes every coarser level spec-guaranteed without spelling
  anything. **Sweeping, not declaring, is the operational decision** — one
  aggregation template serves both a state-scale AOI (sweep immediately)
  and disjoint small deployments (sweep later, or never).
- **A `/2` store is swept by the [staged sweep](#the-staged-sweep-issue-384)**
  ([issue #384](https://github.com/englacial/zagg/issues/384)) — the
  `/1` overview family generates nothing for it (its per-level leaf fold is
  exactly what the column regime ends) and reports the routing in its
  counts (`regime: "stages"`). `/1` stores sweep exactly as before — the
  #379 cascade remains the path for pre-column stores, and stays the regime
  for finer-than-base appends (where gen 3 lives).

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

### Leaf columns (`zagg-column/1`)

Under a `/2` declaration the **leaf worker itself** writes the finest pyramid
levels, at aggregation time, while the shard's cell data is resident
([issue #383](https://github.com/englacial/zagg/issues/383); the fleet side
of [#381](https://github.com/englacial/zagg/issues/381) points (1)–(3)).
The gate is the **writing run's own config**, not a store read — workers
never open the manifest, and the manifest's `pyramid` block (excluded from
the frozen keys above) MAY lag a config-only change — so a `/2` manifest
does not by itself imply columns on disk: read the columns, not the block.
Each `(leaf, window)` gains one **column artifact** beside the leaf under
its own node directory — `{window}.pyramid.zarr`, `all.pyramid.zarr`
unwindowed — holding one resolution group per within-footprint level: the
declared base resolution(s), every ladder rung down to the shard order, and
the **node-order member** (one cell — the leaf's whole-footprint aggregate,
its *universal partial* for every coarser cell, which is why no coarse level
declared later ever rewrites a leaf). Groups fold **from the raw resident
cells only** (merges-from-raw 1; exact classes exactly, digests by the
order-independent k-way merge), so a column group is byte-identical to the
sweep kernels' fold of the committed leaf. The write discipline is the
leaf's own: template → groups → `role: column` + `zagg_column` attrs →
**one commit stamp last**, then the `{stem}.stats.json` sidecar — an
unstamped column is debris. A run that declares no leaf-node levels (or no
composable field) writes no column and **deletes** any the previous
declaration left at that `(leaf, window)`, so a column never outlives the
declaration that wrote it — which is why an absent column is not by itself
a fault: under a declaration known to carry leaf columns it is a torn
worker, and the repair is re-invoking the idempotent leaf, never a
sweep-side fold from raw cells.
Byte grammar: [`specification.md`](specification.md) §4.6.

### Retrofitting the pyramid declaration

The `pyramid` block is normally written at template time
(`build_pyramid_block`, issue #201), but declaration is not birth-only: a
store templated without it — any pre-#344 store — can gain (or change) the
declaration later, because the block is a pure function of the pipeline
config plus the manifest's own `shard_order`, and `pyramid` is already the
one manifest key legally rewritten after templating (D11). The retrofit is
`zagg.sweep_overview.declare_pyramid(store_root, config)`, or from the
shell:

```
python -m zagg.sweep s3://bucket/store --declare-pyramid config.yaml
```

(declaration-only: no sweep pass runs in the same invocation). The tool
derives the block through the same code path template time uses, then
**validates it against store truth before writing**, in two layers:

- **Semantics** — the config's D19 `semantic_hash` must equal the manifest's
  frozen one, so a config that did not build this store cannot install its
  fold laws. This is the layer that covers *reducers*: no leaf records which
  function produced a field, so nothing downstream could catch a config
  declaring `max` over a store of minima. Adding `output.pyramid` to the
  original config hashes identically, so the retrofit never false-refuses —
  the whole `pyramid` block is deliberately outside the semantic core, and
  keeping this workflow working is one of the reasons why (the D19 hash epoch
  of issue #415 put the *leaf-shaping* `output` knobs into the core, listed in
  `zagg.semantics.OUTPUT_LEAF_SHAPING_KEYS`; `pyramid` is not one of them).
  A pre-#299 manifest carries no hash; the comparison is then skipped, loudly.
- **Typing** — every declared field must exist in a committed leaf with the
  declared dtype (dense fields), or the declared ragged element dtype and
  `inner_shape` at a ragged spec revision this zagg understands (t-digests).

Drift in either layer refuses loudly, naming the field and the mismatch, and
nothing is half-written. What is *not* cross-checked: the config's `dataset`
identity and its grid `parent_order`/`child_order` against the manifest's
orders — the declared overview orders are checked only for being ancestor
orders of the store's own `shard_order`. A store with no committed leaf yet
is still declarable; the field checks are then skipped, loudly, and the
summary says which case it was.
The rewrite is idempotent (an identical declaration is not re-PUT) and
preserves any `materialized` actuals the sweep has recorded.
`output.pyramid: false` installs the declared-off block — recording absence
is a valid retrofit, so a reader can skip probing for overviews entirely
(the D24 option-A posture the block enables; no reader in this repo consumes
the declaration yet), and overviews at now-undeclared orders are left in
place as regenerable-cache debris (D24). After a retrofit, the overview
family materializes the declared orders on the next sweep — it is in
`DEFAULT_FAMILIES`, so a plain `python -m zagg.sweep <root>` picks it up
(the fold itself is issue #201 / PR #344; the retrofit is issue #358).

A config that spells `overviews`
([Pyramid overviews](#pyramid-overviews-zagg-pyramid2)) retrofits the
`zagg-pyramid/2` declaration through the same path: the level entries are
re-validated against the **manifest's** own `shard_order`/`cell_order` (the
store's truth wins over the config's grid block), and any `/1`-era
`materialized` actuals are preserved verbatim across the revision bump. The
staged sweep ([below](#the-staged-sweep-issue-384)) then materializes the
ladder from the leaf columns; a retrofit onto a store whose leaves predate
the column regime under-covers loudly (`source_children`) until the leaves
are re-run — declared-but-unswept stays a legal recorded state, not an
error, and the #379 cascade remains the `/1` path for pre-column stores.

### Partitioning a sweep that will not fit

A sweep pass holds the fold state for the subtree it is walking, so on a large
store a single pass can exceed the worker's (or the laptop's) memory. Split it
into `2^n` disjoint morton-subtree partitions:

```
python -m zagg.sweep s3://bucket/store --partitions 16
```

Each partition folds one order-`k` subtree per HEALPix base cell (`n = 2k`), so
peak memory is bounded by the partition rather than by the store, and the
command prints one summary per non-empty partition. `--partitions` must be a
power of **four** — a morton digit is 2 bits, so that is where the split lands
cleanly; an odd `2^n` is refused with the two valid neighbours named.

Partitions are disjoint by construction, so they are also safe to run
concurrently: no two write the same object, and each is independently
idempotent (a re-run folds only what actually changed). What a partitioned
pass deliberately does *not* do is anything **above** the split order — the
coarse rollup levels, the root `coverage.moc` refresh, and the manifest's
`pyramid.materialized` bookkeeping all span partitions and are left to a
coarse-level finisher (issue #377).

Until that finisher lands, the coarse levels of the **JSON rollup families**
(`stats`, `moc`, `submap`) can be picked up by following a partitioned sweep
with a plain `python -m zagg.sweep <root> --families stats,moc,submap`: the
partitions' work is skip-if-current, and an interior fold reads its children's
rollups rather than the leaves, so the extra pass is cheap. The **overview**
family's deferred coarse levels likewise fold from the finer overviews the
partitions already materialized (the default `fold_source: cascade`, issue
[#376](https://github.com/englacial/zagg/issues/376)) — but that follow-up is
not cheap the way the JSON one is: an overview's currency check folds before
it compares, so every leaves-fold level re-reads its raw leaf zarrs
store-wide even where nothing changed. And the follow-up performs
only half the deferred manifest bookkeeping: its RMW unions the materialized
orders, but records a fold regime only for a level that wrote in that same
pass — the regimes the partitions wrote under ride each partition's own run
record as `materialized_fold_sources`, and reach the manifest only with a
finisher that carries them. Leaves-fold levels are not only the deprecated
`fold_source: leaves`, where that is every level — the blow-up the cascade
exists to remove: under `cascade` the finest `exact_levels` levels fold from
the leaves too, as does any level declared a wider gap than the node slab can
divide. Prefer the finisher for the overview family. (A `zagg-pyramid/2`
store never gets that far: its overview materialization is the staged
sweep's, [below](#the-staged-sweep-issue-384) — which retires this
two-call recipe wholesale for `/2` stores, since the stage run carries its
own designated finisher.)

### The staged sweep (issue #384)

A `zagg-pyramid/2` store's above-shard ladder is materialized by **stage
workers over the leaf columns** — a raw leaf is never read above the shard:

```
python -m zagg.sweep s3://bucket/store --stages            # CLI backstop
python -m zagg.sweep s3://bucket/store --stages --partitions 16
```

or in code `zagg.sweep_stages.run_stage_sweep(root, leaves, scope=...)`, or
chained immediately after a fleet run with the opt-in `output.sweep:
"stages"` (auto-scoped to the run's own footprint). Work is discovered from
the **run records** (listing-based; the root `coverage.moc` is an
accelerator for sibling candidates, never the source of truth — a fleet
append with no subsequent sweep leaves it stale, and discovery still finds
the new leaves).

**Cadence.** Ladder orders are grouped into dispatch tuples of
`tuple_width` consecutive orders (default 3: `[8,7,6] → [5,4,3] → [2,1,0]`
on an o9 store). Each stage worker reads exactly its `4^width` immediate
child columns, emits its tuple's level artifacts, and writes its own column
carrying — as a pure gather — the **relayed gen-1 leaf partials** for its
subtree. Every merge, at every level, consumes only that relayed gen-1
tier (the espg merge-source ruling), so **the cadence changes no bytes**:
`--tuple-width 1` and `--tuple-width 3` build byte-identical ladders, and
every upfront merge level is uniformly 2 merges from raw (gathers are 1;
gen 3 is append-later cascade territory only).

**Scope.** The only argument a sweep takes about *where* is an optional
node-prefix set — a MOC; a shardmap is accepted as sugar (its keys are the
prefixes). Scope selects which dispatch nodes are invoked; a dispatched
worker folds **all** children on disk, so an update adjacent to prior data
folds the old neighbors in automatically, and clean nodes no-op under the
generation ratchet. `--partitions` composes with scope by intersection,
swept under one admission.

**Concurrency** (the ruled matrix):

| regime | allowed? | governed by |
|---|---|---|
| fleet ∥ fleet | yes, iff their (window, shard) write sets are disjoint | the existing leaf single-writer law |
| fleet ∥ sweep | yes | disjoint object sets; the stage worker validates every column stamp before and after reading its groups and re-reads on movement, so a mid-read leaf rewrite never feeds a torn column into a merge; a mid-sweep append is recorded-and-healed under-coverage |
| sweep ∥ sweep | **no — serialized per store** | the admission lease |

Only pyramid sweeps serialize. Admission is one conditional PUT of the
store-root intent object `sweep.lease.json` (spec §4.8): a live intent
refuses **naming the running sweep**; an expired heartbeat is claimable,
and the claimant simply completes the partial prior run. The lease is
**control plane** — no data object is ever locked; it is what makes "every
data object has exactly one writer, ever" true *across* runs. It is
store-granular by correctness: scope-disjoint sweeps still converge on
shared coarse ancestors (base cells, the root MOC, the manifest). The
**designated finisher-worker** (never the 12 base cells) owns the root
singletons after the root tuple — the root `coverage.moc` refresh, the
manifest RMW writing the per-entry `actuals`, the `aggregation.yaml`
lifecycle touch — and deletes the intent as its final act. A run that dies
mid-sweep leaves its intent to expire into claimability; stage stamps carry
the run id, and a worker that sees a foreign fresh stamp aborts loudly (the
residual-race backstop).

**Soft barriers.** Stage order is a scheduling preference, not
correctness: a tuple run before its finer tuple landed under-covers loudly
(`source_children`) and self-heals on the next pass — the skip gate keys on
summed child generations, so a healed child forces the parent rewrite. That
key has four terms — leaf count, newest child stamp, the set of `run_id`s
those stamps carry ([issue #417](https://github.com/englacial/zagg/issues/417))
and their summed `granule_count`
([issue #433](https://github.com/englacial/zagg/issues/433),
[specification §4.5](specification.md)) — because stamps resolve to one
second: without them a child rewritten inside its own recorded second at an
unchanged leaf count would read as current and the parent would keep the
stale fold. The last two divide the work by who wrote the child: a stage
column carries the run id that wrote it, a fleet-written leaf column carries
none — so at the finest tuple it is the granule count, which every writer
stamps, that moves under a re-run over more granules.

**Aging.** Stage reruns refresh the ancestor artifacts they revisit (a
rewrite is a fresh PUT), and the finisher's manifest RMW + root-MOC write
refresh the store-root objects every run — so a store swept on any cadence
keeps its pyramid's `LastModified` moving. A store that only ever SKIPS
(all-current fleet runs, no sweeps) still ages its sweep-written ancestor
overviews out under a bucket lifecycle rule: the per-unit lifecycle touch
covers leaf footprints, not ancestor artifacts (the PR #397 finding — this
is the recorded posture, not an oversight; re-sweeping is the refresh).

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

A temporal-declaring store adds one more key here: `temporal`, the
`zagg-coverage-toc/1` section (per-shard toc envelope words plus an optional
root time-digest) whose grammar is normative in
[`specification.md`](specification.md) §10 — one metadata GET then answers
"which shards hold data DURING my window" before any leaf is opened. A store
with no temporal channel carries no such key and its root object is
byte-identical to a pre-#480 one; absence is never a refusal.

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

## Migration: the D19 hash epoch

> **Every `semantic_hash` written before this release is invalidated, by
> design** — and so is every `granules_sha256` taken over resolved hrefs
> (item 5). Nothing in any store changed on disk; what changed is the
> *derivation* of the digests that label it. If you are upgrading a store
> built by an earlier zagg, read this section before rerunning into it.

[Issue #415](https://github.com/englacial/zagg/issues/415) closed two ruled
defects in the semantic core ([PR #397](https://github.com/englacial/zagg/pull/397)
questions (7) and (8)), and carried two further ruled exclusions that had to
ride the same epoch: the credential mechanism
([issue #449](https://github.com/englacial/zagg/issues/449)) and the
byte-movement knobs (espg-ruled 2026-08-17 on the epoch PR). Those four change
what `zagg.semantics.semantic_hash` digests, so each moves every digest —
which is why they were deliberately landed in one release rather than one at a
time. A fifth ruling (item 5 below) canonicalizes **granule** identity, moving
the sidecars' `granules_sha256` rather than `semantic_hash`; it rides the same
release for the same reason, since both halves of the identity PAIR are what
the skip gate compares.

### What changed

1. **The granule fan-out width left the core.** `shard_workers` /
   `granule_workers` are now in `zagg.semantics.DATA_SOURCE_PACKAGING_KEYS`.
   D19's ratified exclusion list already called worker size packaging, but
   the keys were never listed, so the digest moved with the pool width. That
   was not merely untidy: both dispatchers hand each cell a `data_source`
   clamped to `min(K, n_granules)`, so a *small shard's* worker computed a
   different digest from the run's, and any rollup mixing clamped and
   unclamped shards collapsed `semantic_hash` to `null`.
2. **The leaf-shaping `output` knobs entered the core** —
   `zagg.semantics.OUTPUT_LEAF_SHAPING_KEYS` (`aoi_mask`, `windowing`) and
   `GRID_LEAF_SHAPING_KEYS` (`sharded`). Before, the whole
   `output` block was outside the core, so a config edit that changed what a
   leaf *contains* moved neither half of the skip gate's identity pair and a
   rerun read the stale leaf as `current`. `output.grid.emit_cell_ids` meets
   that criterion too and is still **excluded** (espg-ruled 2026-08-17): the
   D16 hatch is scheduled for removal
   ([issue #304](https://github.com/englacial/zagg/issues/304)), and hashing it
   would leave every store built with it ON carrying a digest that no legal
   config can reproduce once the knob is gone. A leaf's *array inventory* is
   verified by reading the leaf — the same argument that keeps `output.pyramid`
   out.
3. **The credential mechanism left the core.** `data_source.credentials_provider`
   joined `DATA_SOURCE_PACKAGING_KEYS` (espg-ruled 2026-08-17): the provider
   name selects *how* source bytes are fetched, never *what* is computed from
   them, so the same granules read with `lpdaac` credentials, `gesdisc`
   credentials, or an anonymous open are one product. It is the same class as
   the read knobs and as `anonymous`, already excluded — the same class, not
   one knob under two names: `anonymous` is read only by the raster
   source-store kwargs, `credentials_provider` only by the point and temporal
   paths, so no single run consults both. A wrong credential fails
   the fetch loudly (a 403 at read time), so nothing depended on the digest to
   catch it. The operator consequence is the one that made it urgent: a
   **credential migration over unchanged data** — re-registering an existing
   store's source under a different provider, or adding the key to a config
   that ran without it — no longer refuses the store and rewrites it to produce
   the same bytes.

4. **Three more byte-movement knobs left the core.** `read_workers`,
   `write_buffer` and `source_region` joined `DATA_SOURCE_PACKAGING_KEYS`
   (espg-ruled 2026-08-17): `read_workers` is the third fan-out width beside
   the two spellings item 1 excluded, `write_buffer` bounds how many slabs are
   alive under the streamed raster sink, and `source_region` is the raster
   source store's AWS region kwarg — which sat in the *same dict literal* as
   the already-excluded `anonymous`, so hashing one and not the other split a
   single "how do we open the source" decision across both sides of the line.
   Each fails loudly in its own direction (a small pool is slower, a wrong
   region is a connection error, an over-large buffer is an OOM), so nothing
   depended on the digest to catch them. The operator consequence matches item
   3: **retuning machinery over unchanged data no longer rehashes.** The live
   demonstration is dated, and it covers the **fan-out widths only** — two GEDI
   flux builds of the same shard on 2026-08-17 produced identical `total_obs`
   and `cells_with_data` in the exact single-block spill regime and still hashed
   apart on `read_workers` and the two `*_workers` spellings of item 1. It
   cannot cover the other two: `write_buffer` and `source_region` are read only
   on the raster path, and a GEDI flux build is the point path, so their
   exclusion rests on the arguments above rather than on this measurement.
5. **Granule identity is now the driver-stripped bare granule id.** This one
   moves the *other* digest — `granules_sha256`, the **catalog** identity half
   recorded in every D20 sidecar, and the id list in its `granules.json`
   sibling. espg-ruled 2026-08-17: *"we want the granule to trigger the hash,
   not how that granule is fetched."* A single granule is named three ways
   across the paths that record it — a resolved `s3://bucket/key/FILE` href, an
   `https://host/path/FILE` href (`data_source.driver` picks one), or the bare
   catalog id, which for every catalog zagg reads *is* the basename of both
   hrefs. Pre-epoch each spelling hashed differently, so a driver switch —
   packaging in the semantic core since forever — made every leaf's recorded
   catalog identity un-reproducible and sent the skip gate down the
   `expansion` arm for a rerun over exactly the same granules. Both halves
   canonicalize: the digest **and** the recorded id list, so the `missing` ids
   a contraction names are driver-independent too. Recorded lists written
   before this release keep working — the classifier canonicalizes the
   *recorded* side as well, so a pre-epoch leaf diffs cleanly instead of
   reading as a full contraction. Accepted cost of the ruling: two granules
   whose hrefs differ only in prefix collapse to one identity; every catalog
   zagg reads names granules globally uniquely, which is why the catalog's own
   id equals the basename. That cost lands in two places, not one — besides the
   identity collision it **degrades the contraction guard** inside a collapsed
   group, and the safe way is not the way it falls: dropping one member of a
   collided pair leaves the set diff unchanged, so the leaf reads
   `id-multiset-drift` and rewrites where the pre-epoch diff refused and named
   the dropped href. Any leaf whose recorded ids collapse is logged loudly for
   exactly that reason; making it refuse instead is a standing question on the
   contraction predicate (PR #420 review finding (2)).

Deliberately **not** changed: the orders (`parent_order` / `child_order` /
`chunk_inner`) stay packaging — hashing them would make o8 and o9 runs
different products and block mixed-order processing (D24) — and the whole
`pyramid` block stays out, so
[retrofitting a pyramid declaration](#retrofitting-the-pyramid-declaration)
onto the config that built a store still hashes identically and still works.
`emit_cell_ids` stays out for the reason recorded under item 2.

### Why re-hashing is correct, not a defect

The pre-epoch digest answered "do these two configs produce the same leaves"
**wrongly in both directions**: it separated configs that were identical in
every effect (the clamp), and it equated configs that write different leaves
(the `output` knobs). A digest that is wrong in both directions cannot be
preserved for compatibility — preserving it would mean preserving a false
answer to the only question it is asked. The stores themselves are
untouched: no leaf byte moves, and the §5 `content_hashes` (which are over
decoded values, not over identity) are unchanged. Only the *label* is
restated, more accurately.

### What an operator sees, and the three ways forward

`semantic_hash` is a frozen manifest key, so a rerun into a pre-epoch store
refuses **up front**, before any leaf is written:

```
morton_hive.json at s3://bucket/store does not match this run
(existing {...} vs {...}); this store was templated for different
orders/identity — clear the store root (or pick a new one) before
writing with this configuration
```

`--overwrite` does not bypass it: when the digit tree already holds shard
data, the overwrite path refuses too, because replacing only the manifest
would leave the old leaves masquerading as legal data (D2). Pre-#299 stores
that carry no hash at all are unaffected — the guard exempts a missing hash
on either side, so they stay resumable exactly as before.

1. **New product root (recommended).** Write under a new `output.product_name`
   (or a new store root). Both products coexist under one root (D19), the old
   one stays readable, and nothing is rewritten or lost.
2. **Rebuild.** Clear the store root and rerun. Correct and simple; you pay
   the full aggregation again.
3. **Restamp the manifest in place — expert path, read the whole entry.**
   The data really was produced by this config, so writing the new digest
   into `morton_hive.json` is *semantically* correct. It is also the only
   path that can make things worse, in three ways:
   - **It is not one field.** `semantic_hash` is recorded in leaf attrs and
     in every D20 stats sidecar as well as the manifest, and `dedup`'s
     identity checks read the *sidecar's* copy, not the manifest's. A
     manifest-only restamp therefore leaves a **mixed-identity store** — root
     says one thing, leaves say another — until every leaf has been
     rewritten. Nothing else in zagg expects that state to persist.
   - **The rewrite is not a heal, it is the same re-aggregation as (2)**,
     paid unpredictably across future runs instead of once deliberately:
     each unit classifies `semantic-mismatch` and rewrites wholesale.
   - **It manually defeats the guard that catches a wrong config.** Nothing
     verifies that the config you hash is the one that built the store, and
     once a foreign digest is installed the frozen-key check will never fire
     for that store again. Before restamping, confirm the config is the
     store's own by recomputing its **pre-epoch** digest under the previous
     zagg and matching it against the recorded one.

   There is no zagg tool for any of this; it is a deliberate operator action,
   and paths (1) and (2) have none of these failure modes.

Whichever path you take, the first post-epoch run over a store is a **full
rewrite** of everything it touches — on a fleet-scale store that is a full
re-aggregation, and it is the headline cost of the epoch, not a footnote. The
skip gate cannot certify a leaf as current against an identity that was
computed by a different rule, and pretending otherwise is exactly the false
skip the epoch exists to prevent.

On the Lambda path the manifest write is asynchronous (issue #252 hybrid,
above), but the refusal is **not** deferred with it: the read-only frozen-key
precheck (`zagg.hive.validate_manifest`) runs on the `mode: "ping"` preflight
*before* fan-out, so an epoch mismatch costs one preflight, not a few thousand
worker invocations.

### What the epoch buys

The break is the price of arming the fleet's leaf identity gate. Skip-if-current
([below](#re-runs-skip-if-current-the-contraction-guard-and-the-lifecycle-touch))
is armed by default only on the local backend today; fleet arming was
explicitly gated on this epoch
([issue #415](https://github.com/englacial/zagg/issues/415), sequencing), for
the reason phase (7) records: pre-epoch the worker-side fallback hash was
clamp-sensitive, so a small shard would have compared its leaf against a
digest the run never wrote and rewritten forever without self-healing. After
the epoch that comparison is sound, and the second post-epoch run over an
unchanged store is the no-op the gate was built for.

## Re-runs: skip-if-current, the contraction guard, and the lifecycle touch

Re-dispatching a shard used to mean an unconditional wholesale rewrite (D4)
— correct and deterministic, but wasted compute, and invisible to bucket
lifecycle policies that purge "old" objects. Since
[issue #388](https://github.com/englacial/zagg/issues/388) each
`(shard, window)` unit runs a worker-side **identity gate** before any read
or fold, on both leaf families (the aggregation seam
`zagg.hive.process_and_write_hive` and the raster seam
`zagg.processing.raster.process_and_write_raster_hive`).

**Identity is a pair**: the run's `semantic_hash` (D19 — *what/how*) × the
unit's planned granule-id set (*over what*), compared against the leaf's
recorded stats sidecar (fast path: one `granules_sha256` compare). A
matched pair alone is **not** sufficient — the unit's artifacts must be
current too, verified by reading them, never trusted from the record: the
leaf must carry its commit stamp, and the [leaf
column](#leaf-columns-zagg-column1) must agree with the run's declaration
(the [specification §4.6](specification.md) config-decides gate: the
declaration moves *neither* identity half, so the gate reads the artifact).
Three verdicts:

| verdict | when | what happens |
|---|---|---|
| **current** | both halves match, leaf stamped, column agrees with the declaration | fold no-ops; the unit writes **nothing** (no arrays, no stamp, no sidecar, no sub-map, no column — zero sweep dirtiness); the lifecycle touch below runs; counted as `cells_current` |
| **refused** | the planned set drops recorded ids: `recorded ∖ planned ≠ ∅` — deliberately *not* strict-subset, so a shardmap that grew while silently dropping old granules (an upstream purge behind a fresh catalog query) still trips it | the unit refuses, writes nothing, and counts as `cells_refused` — never as an error. The log names the **first five** missing ids and their total; the **full per-unit diff is the refusal manifest** at the store root ([below](#the-refusal-manifest)). `--allow-contraction` (`agg(allow_contraction=True)`; on Lambda an `allow_contraction` event field) turns it into a normal rewrite |
| **rewrite** | everything else — expansion (new cycles), a semantic change, no/unreadable sidecar, an unstamped leaf, column drift, or a pre-#388 sidecar (below) | today's wholesale D4 rewrite, column included |

The gate is **on by default for the local backend**; `--overwrite` disables
it entirely (the operator's unconditional-rewrite hammer — it does not
acknowledge a contraction, it bypasses the guard). The **deployed Lambda
handler has not yet opted in**: the seams default off, so fleet re-runs
still rewrite unconditionally today (PR #397 question (1)). What fleet
sidecars *record* splits by family, though, and only one half waits on that
enablement:

- **Vector — nothing to wait for.** The handler passes the seam's own
  metadata dict straight to `build_record`, the seam stamps
  `semantic_hash` into that dict, and the record's validated fallback picks
  it up. So from this release's deploy onward — gate still off fleet-side,
  zero handler changes — fleet vector sidecars carry **both** identity
  halves, and a later local re-run over that store can skip.
- **Raster — never, until the handler changes.** The raster branch rebuilds
  its record body from a closed key list that omits `semantic_hash`, so
  raster fleet sidecars keep recording `semantic_hash: null`. That fails the
  fast path and classifies `semantic-mismatch` → rewrite on every run, with
  no self-heal until the leaf has been rewritten under a handler that
  carries the key. The consequence is **local**, not merely fleet-side:
  re-running a fleet-built raster store locally — where the gate *is* on by
  default — reports `cells_current: 0` indefinitely until question (1)'s
  key-list fix lands.

The *catalog* half needs no enablement in either family: the granule-id
sibling is written by the **seam**, not by the dispatcher that writes the
sidecar, so every leaf a fleet worker commits from this release on records
its input set — which is what the contraction guard, running in whatever
process re-dispatches that unit later, diffs against.

**Where the recorded id list lives.** Not in the stats sidecar: in a
**sibling object beside it** — `granules.json` (`granules_{window}.json`
windowed), on the sidecar's own spec-keyed naming grammar — written by the
leaf seam right after the commit stamp, carrying the ids *and* the
`granules_sha256` they hash to. Identity equality never reads it: that is the
sidecar's hash compare, which is what keeps a fan-out dedup check
(`dedup.shard_status` per shard, one GET) and the worker-side run-record
assembly small. The list is fetched exactly once, only when the hashes
disagree, and only to **name** which granules a contraction dropped — a pole
shard's ~4,600 ids are ≈550 KB, and they would otherwise ride every one of
those GETs. A sibling whose `granules_sha256` does not match the sidecar's
(a torn rewrite: one of the two writes lost) is rejected as unrecorded rather
than trusted.

**The contraction guard cannot protect leaves written before this
release.** That sibling arrives with issue #388. A leaf written before it has
only the recorded hash — there is no recorded set to diff — so any
*granule-hash* mismatch over such a leaf classifies `unrecorded-ids` and
performs the **silent wholesale rewrite the guard exists to prevent**,
contraction or not. (A config-only rerun — same inputs, changed semantic core
— is not one of these: the hashes agree, so it reads `semantic-mismatch`
without any sibling read, and never inflates `cells_unrecorded`.) The guard only
protects a leaf after that leaf has been rewritten at least once under this
release. These rewrites are counted apart (`cells_unrecorded` in the run
summary; the CLI prints them as "rewritten with the guard inert") so an
operator can see how much of a store is still unguarded; making them refuse
instead is a standing design fork (PR #397 question (4)).

One identity caveat operators still hit: the identity pair does **not** cover
`output.grid.chunk_inner`, which changes the leaf's object set through K while
both halves hold, so changing it still needs `--overwrite`
(espg-ruled 2026-08-17 as the bounded state — closing it costs part of D24;
`sharded` itself *is* covered since the epoch, see
[the migration note](#migration-the-d19-hash-epoch) item 2).

The **driver-dependence** caveat is gone, in two different strengths either
side of the epoch. The recorded id space used to be the resolved href, so
flipping `data_source.driver` between runs read as a full mixed contraction and
refused per leaf. Since the epoch both sides are reduced to the canonical
driver-stripped bare granule id (item 5 of the migration note), so:

* against a leaf written **at or after** the epoch, a driver switch over
  unchanged granules reads `equal` and skips — its recorded
  `granules_sha256` is already in the canonical space, so the hash fast path
  matches and no sibling is read;
* against a **pre-epoch** leaf it does not skip, and cannot: the fast path
  compares the *stored* digest, which was taken over resolved hrefs, so it
  mismatches by construction and the leaf is rewritten once (expected at an
  epoch — the note's "every pre-epoch `granules_sha256` invalidated" headline
  is this). What canonicalizing the *recorded* side buys is the direction of
  that rewrite: the sibling's hrefs reduce to the same bare ids as the plan, so
  the leaf reads `id-multiset-drift`/`expansion` and rewrites, instead of
  **refusing as a spurious contraction**. A driver switch over a pre-epoch
  store therefore no longer needs `--allow-contraction` to get past the guard.

### The refusal manifest

A refused unit writes **nothing** — no leaf, no sidecar, no run-parquet row —
so a run that refuses leaves the store byte-identical and the only in-band
trace is a worker log line truncated to five missing ids. Since issue #388 a
run that ends with `cells_refused > 0` also writes one small JSON object at
the store root:

```
refusals_{YYYYmmddTHHMMSSZ}_{run_id}.json
```

Timestamp-first like the `stats_*.parquet` run records (and outside their
glob, so run-record discovery never mistakes one for the other). It carries
the run context needed to act on it — `run_id`, write timestamp, the run's
`semantic_hash`, the zagg version — and one entry per refused unit: its
`shard_key` and `window`, the classification (`contraction` or `mixed`), and
the **complete** `missing_granules` list, which is exactly what the guard read
from that leaf's granule-id sibling in order to name the drop. Triage the
diff from this object, not from the logs.

Two deliberate limits. A **pure-skip** run (nothing refused) writes nothing
here and stays row-less — the counters and the leaf sidecars are the record of
a no-op run, and an empty manifest per rerun would be litter. And the manifest
is written by the **local backend only**: D8 keeps the Lambda dispatcher from
writing to the store, and no once-per-run worker-side seam exists yet to carry
it — the same gap, with the same resolution, as the store-root touch below
(PR #397 question (10)). A fleet run's refusals are visible in its summary
counters and worker logs, not in a durable object.

### The lifecycle touch

A skipped unit still resets the purge clock: every object in its footprint
gets a fresh `LastModified` (lifecycle rules act per object) — the leaf
`.zarr` tree (stamp, arrays, in-leaf `coverage.moc`), the stats sidecar, its
granule-id sibling and the sub-map sibling, and the declared column tree plus
its own sidecar.
Local stores use `os.utime`; S3 uses a server-side self-copy (`CopyObject`
onto itself, `MetadataDirective: REPLACE`) that preserves content, the ETag
of non-multipart objects, and the object's storage class. A local run's
wrap-up also touches the **store-root trio** no unit owns
(`morton_hive.json`, `aggregation.yaml`, root `coverage.moc`) — an all-skip
run re-PUTs none of them, and the manifest is REQUIRED reader-facing schema
that would otherwise expire first, bricking a store whose data objects are
all fresh.

The touch is **best-effort and fail-open**: a failed touch logs, counts,
and degrades to today's behavior — it never fails the unit and never
un-skips it. Watch the counters: `objects_touched` / `touch_failures` ride
the run summary, and the CLI warns explicitly when touches failed, because
those objects **did not** get their purge clock reset (the run still exits
0). S3 caveats, documented rather than solved: a versioned bucket mints a
new object version per touch; an object already transitioned to
`GLACIER`/`DEEP_ARCHIVE` cannot be self-copied (counts as failed); ACLs are
not preserved (moot under `BucketOwnerEnforced`, the modern default, but a
public-read-**by-ACL** bucket would see touched objects go private); under
SSE-KMS the copy re-encrypts with the bucket-default key and needs
`kms:Decrypt` + `kms:GenerateDataKey`.

**Known gaps** — plan lifecycle rules around them, they are not promised
away: the [design §7](design/sparse_coverage.md) sweep's **ancestor-node
artifacts belong to no unit's footprint** and a skip produces zero sweep
dirtiness by construction. That is the whole ancestor layer, not the
overviews alone — all four rollup families age identically
(`stats.rollup.json`, `moc.rollup.json`, `submap.rollup.json`,
`overview.rollup.json` at every digit node, one per `sweep.DEFAULT_FAMILIES`
entry), plus each pass's root `sweep_stats_{ts}.json` run record. And
re-running the sweep does **not** refresh them: a second sweep over an
unchanged tree recomputes but PUTs nothing, so the obvious "just sweep
periodically to keep them alive" workaround silently no-ops. The store-root
trio, separately, is touched by the
**local backend only** — as is the refusal manifest above, for the same reason
(D8 keeps the Lambda dispatcher from writing to the store, and the handler has
no once-per-run root-write mode yet; both wait on the same resolution — PR #397
question (10)).

**There is no lifecycle rule that separates leaves from the ancestor
artifacts above them.** An S3 lifecycle filter keys on prefix, object tags,
and size only; zagg's writers set no object tags; and per
[the specification §4.1–§4.2](specification.md) an overview lives at an
**ancestor digit node inside the same prefix tree** as the leaves it
summarizes, where "nothing about the *name* distinguishes an overview from a
leaf" (classification is by root-group attrs, which no lifecycle engine
reads). So "scope expiry to the leaf data planes"
is not writable as a rule: the only prefix that selects leaves while
excluding ancestors is a per-shard-node prefix, against a 1,000-rule bucket
cap. The honest options today are:

1. **No expiry rule on a pyramid-declaring hive store.** The safe default
   while the gaps above are open.
2. **Accept expiry over the digit tree**, understanding that it takes the
   ancestor artifacts with it. Those are regenerable caches, never
   load-bearing ([specification §4.1](specification.md)), so the cost is a
   full rebuild by a
   sweep — not data loss. Exclude the store-root trio by name (three known
   objects at a known prefix); `morton_hive.json` is REQUIRED reader-facing
   schema and expiring it bricks the store.
3. **The levers that would make a leaf-scoped rule writable do not exist
   yet**: object tags stamped at write time (nothing in zagg writes any), or
   a scheduled refresher that re-PUTs the ancestor layer — the staged sweep
   of [issue #384](https://github.com/englacial/zagg/issues/384) is the
   obvious candidate, but nothing about it promises a refresh today. Both
   are open; see PR #397 question (10) for the related fleet-side root
   touch.

## Raster hive stores (issue #247)

Raster (pull-NN) pipelines write the same tree with **windowed `(time, cells)`
leaves**: one vanilla zarr v3 leaf per **(shard, window)** unit at
`shard_leaf_path(root, shard, window=label)`, each carrying leaf-local `time`
(int64 microseconds with CF attrs by default, or `uint64` mortie toc words
carrying the spec §8.1 `temporal` declaration at `shape: "coordinate"`
and no CF attrs under
`output.time_encoding: toc` — which the shipped Sentinel-2 config sets, issue
#443) and `morton` (packed u64 words) as the sole cell
coordinate — `cell_ids` (NESTED) rides only the `emit_cell_ids` transition hatch
(issue #304) — plus one
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
- **Skip-if-current re-runs** ship for the local backend
  ([issue #388](https://github.com/englacial/zagg/issues/388)): the
  worker-side identity gate, the contraction guard, and the lifecycle touch
  — see [Re-runs](#re-runs-skip-if-current-the-contraction-guard-and-the-lifecycle-touch).
  The Lambda handler enablement is a named follow-up (the seams default
  off, so deployed workers are byte-identical until it lands).
- Write-throughput validation at fleet scale is tracked with the benchmark
  machinery in [issue #202](https://github.com/englacial/zagg/issues/202).
