# Sparse coverage & the cross-resolution read path

**Status**: draft. Tracks [#198](https://github.com/englacial/zagg/issues/198);
temporal-partitioning amendments (D13–D15) ratified on
[#237](https://github.com/englacial/zagg/issues/237); morton-only-storage
amendment ratified on
[#262](https://github.com/englacial/zagg/issues/262) (D16; open item O10
carved from it); 2026-07 consolidation (D17–D24, O3 resolved, O11 opened)
recording decisions settled on the
[#251](https://github.com/englacial/zagg/issues/251)/[#236](https://github.com/englacial/zagg/issues/236)/[#209](https://github.com/englacial/zagg/issues/209)
and [#296](https://github.com/englacial/zagg/issues/296)-family threads —
per-entry provenance (thread-ratified vs in-session-recorded) is cited
inline.

> All design decisions (both made and open) are consolidated in the
> [Decisions registry](#8-decisions-registry). Inline references use **D#** for
> decisions made and **O#** for open items needing input. Revisit before
> implementation issues are carved out.

## 1. Motivation

Three pressures converged in the
[mortie #48](https://github.com/espg/mortie/issues/48) v1.0 discussion, and one
set of primitives answers all of them:

1. **S3 write contention at global scale.** A CONUS-scale run is ~50,000
   order-9 shards over ~2,000 concurrent workers, each shard issuing ≥8 PUTs.
   S3 throttles at ~3,500 PUT/s *per partition*, so a single-prefix output
   store is intractable. Morton indices decompose into a spatially-local,
   hive-like prefix hierarchy — that layout is Layer 0 below.
2. **Multi-dataset, multi-resolution reads.** ICESat-2 (~12 m morton cells),
   GEDI (~25 m, one order coarser), Sentinel-2 (~10 m; possibly re-encoded
   finer) each get their own store at their own cell/shard orders — Lambdas
   are always per-dataset (D7). A reader wants cross-resolution joins: "for
   this fine ICESat-2 observation, what GEDI observation contains it?"
   Because a morton decimal string prefix *is* the spatial ancestor, that
   join is **truncation** — arithmetic, not I/O.
3. **xdggs assumes dense full-sphere coordinates.** The legacy `fullsphere`
   layout (deprecated, dense path removed in 0.x — D17) materialized a
   coordinate entry for every cell of the global grid.
   For sparse-coverage data (a continent, a flight campaign) this is waste at
   best and intractable at global orders. The fix is a **domain declaration**:
   a coverage MOC conservatively declaring where data exists, letting the
   xarray extension keep coordinates sparse and fabricate dense views lazily.

The stack, bottom to top: hive store layout (§2) → static manifest (§3) →
coverage MOCs (§4) → reader architecture (§5) → xarray/xdggs extension (§6), with
the pyramid sweep (§7) as the post-process phase that generates all derived
artifacts.

## 2. Layer 0: the hive store layout

The layout convention is **owned by the mortie spec** (the frozen 1.x
contract: [mortie #48](https://github.com/espg/mortie/issues/48) discussion →
[mortie #62](https://github.com/espg/mortie/issues/62) spec page). Summary of
what zagg consumes:

```
{store_root}/                    <- multi-product form (D19): a directory of
  {name}/                        <- NAMED product root — each product subtree
    morton_hive.json                is a COMPLETE morton-hive store: bare-named
    coverage.moc                    manifest (§3) + optional root MOC (§4, O9)
    aggregation.yaml             <- canonical semantic core (D19); its hash is
                                    a frozen manifest key, not a path name
    <run records (parquet)>      <- run-level telemetry, one row/shard;
                                    timestamp-first names (D20)
    {sign+base}/{d1}/{d2}/.../   <- one digit per level (D2; digit-chunking is
                                    the manifest path_grouping param, D21)
      {window}.zarr/             <- leaf, basename = time window (D23,
                                    morton-hive/3); `all.zarr` for
                                    schedule: none (reserved token — lean).
                                    /1–/2 stores keep {full_id}.zarr and
                                    {full_id}_{window}.zarr (D3/D13)
      stats_{window}.json        <- per-shard stats sidecar, sibling; ratified
                                    D20 naming (stats.json for schedule: none).
                                    D23 lean, not ratified: {window}.stats.json
                                    / all.stats.json (follow-up to #302)
      <sub-shardmap JSON>        <- leaf sub-map for sweep rollups (D22)
```

A bare single-product store (today's layout — `morton_hive.json` at the
store root, no `{name}/` level) remains fully valid: the D19 product root is
*additive*, and a product subtree is byte-identical to a bare store. A
reader distinguishes the two forms by what sits at the root (a manifest ⇒
bare store; only name-shaped prefixes ⇒ product directory). Product names
must not match the base-component grammar (`-?[1-6]`) so the walker's child
classification stays unambiguous; gridlook and other viewers enumerate
products by listing `{store_root}/` and reading each
`{name}/morton_hive.json` directly — no name↔hash translation layer.

- **Ids are morton decimal strings** (D1): sign + base digit (constant width,
  12 values `1..6`/`-1..-6`), then one digit per order, digits `1-4`, never
  `0`. String prefix = spatial ancestor at every level.
- **One digit per path component** (D2), because shards live at mixed orders —
  across datasets *and* within a store (coarse shards in sparse regions) —
  so every order must be a legal node. D21 makes the digit-chunking a
  declared manifest parameter (`path_grouping`, default `1` = this layout);
  readers chunk the digit string per the manifest, never by assumption.
- **Full morton id at the leaf** (D3): `.../1/2/3/-31123.zarr/` is
  self-describing without parsing its path, greppable in inventories,
  unambiguous if moved. (Unchanged by D19 — product identity lives at the
  product *root*, above the tree. *Superseded by D23 for `morton-hive/3`
  stores*: the basename becomes the time window; the full id stays
  recoverable from the path arithmetically and from the stamp/sidecar
  `shard_key`.)
- **Time-windowed leaves** (D13, ratified on
  [#237](https://github.com/englacial/zagg/issues/237)): a store whose
  manifest declares a temporal window schedule (§3) partitions each shard's
  time series into **one write-once leaf per window** at the shard node,
  rather than one growing leaf. The node invariant is unchanged (a node may
  hold several `*.zarr` objects — mixed orders already require that; the
  walker classifies them as data as before). Every windowed leaf carries its
  own D4 commit stamp, so all append/retry semantics reduce to the existing
  ones: a torn window is debris, overwritable; **backfill** (extending the
  series to *earlier* data) is just a new leaf for an earlier window — no
  `resize`, no read-modify-write of committed objects, no time-axis
  reordering; concurrent runs on different windows share no object; the
  window is the unit of idempotent reprocessing. The rejected alternatives —
  a high-water time index, and per-run stamp entries with array `resize` —
  both reintroduce mutable shared state at the leaf and break the binary
  debris rule (rationale on the #237 thread). Cross-window reads open W
  leaves and concatenate along time; paths stay arithmetic because the
  schedule lives in the manifest (D10 preserved). The no-partitioning
  degenerate case (`schedule: none`) keeps the bare `{full_id}.zarr` name and
  is byte-identical to the pre-D13 layout — a `morton-hive/1` store *is* a
  `/2` store with `schedule: none`. (Leaf *naming* is revised by D23 for
  `morton-hive/3` stores; the windowing semantics here are unchanged.)
- **Node invariant**: below a product root, a node contains *only* digit
  children (`[1-4]/`), `*.zarr` objects, and the declared leaf-adjacent
  sidecars — the per-shard stats record (D20) and the sub-shardmap JSON
  (D22) — with nothing else, ever: the walker's child classification
  depends on the name set being closed. The product root alone also carries
  the manifest, MOC objects, the semantic core (`aggregation.yaml`, D19),
  and run-level telemetry records (D20); the *store* root of a
  multi-product directory carries only `{name}/` product roots (D19).
- **Termination condition**: S3 has no empty directories (a prefix exists iff
  ≥1 object lies beneath it) and LIST is strongly consistent, so a
  delimiter-LIST returning no digit-shaped children is a definitive "nothing
  finer exists." Absence is trustworthy.
- **Presence needs a commit stamp** (D4): a worker that dies mid-shard has
  already created the `.zarr/` prefix. The shard's *final* write is a root
  `group.attrs.update(...)` stamping completion (plus cheap payload: cell
  count, write timestamp, source granule count). A `.zarr/` prefix whose root
  metadata lacks the stamp is debris — incomplete, ignorable, safe to
  overwrite on retry. This is **not** consolidated metadata: one tiny PUT
  rewriting an object that must exist anyway, no store-wide aggregation.
- **The write path needs zero metadata above the leaf** (D5). No zarr group
  objects at digit nodes, no shared mutable state, no create-group races
  across 2,000 workers.

Zarr-version note: implicit groups were a *draft*-v3 feature, dropped before
finalization; v2 requires explicit `.zgroup` objects too. Neither models the
digit tree for free — and we don't need either to. The hive tree is
effectively our own implicit-group layer (viable because names are constrained
and LIST is strongly consistent), sitting above completely vanilla zarr v3
leaf stores. No zarr-version coupling in either direction.

## 3. Layer 1: the static manifest (`morton_hive.json`)

Written **asynchronously at init** (issue #252 hybrid — the write comes off
the synchronous pre-dispatch path: the Lambda leg posts a fire-and-forget
setup invoke right after the fail-fast ping, so the manifest typically lands
within seconds of init (best-effort: the Event invoke shares worker
concurrency and runs retries-0, deferring to the finalize backstop under
throttling or a dropped invoke) and readers can consume completed leaves
mid-run; finalize
keeps an idempotent backstop that self-heals a lost async write; a read-only
frozen-key precheck keeps the up-front refusal on reruns — concurrent first
writes now collide within seconds of init). O(1); otherwise never touched
again during a run. Contents:

- `spec`: convention version string (e.g. `"morton-hive/1"`) — the convention
  itself is versioned from day one (D6).
- Dataset identity (short name, product/version).
- `cell_order`, `shard_order` — each as a declared **allowed set/range**
  when the store permits region-dependent orders (D24: shard order is pure
  packaging; cell order is a resolution axis with per-leaf truth in the
  morton words + MOC). The allowed sets are the frozen keys; runs within
  the set pass the append precheck.
- `semantic_hash` (D19): sha256 of the canonical semantic core — a frozen
  key, so reusing a product name with different aggregation semantics
  refuses up front, exactly as an order mismatch does.
- Split schedule (implicit under D2: one digit per level to `shard_order`;
  recorded explicitly for forward compatibility).
- `path_grouping` (D21): how many morton digits each path component chunks.
  Existing stores are retroactively `1`; new stores default `1`; changing the
  default later is a parameter flip for new stores, never a schema break.
  When compared as a frozen key, an absent field normalizes to `1` on both
  sides, so appends to pre-D21 stores never refuse on it.
- Rollup/pyramid declaration (D22 extends the original overview-only form):
  which ancestor orders carry which derived artifact family — overview
  zarrs, stats rollups, sub-shardmap rollups — declared **per artifact
  family** (schedules may differ; display overviews as dense as rendering
  warrants, metadata rollups sparser), populated/updated by the §7 sweep.
  Tree shape (`path_grouping`) and rollup schedules are deliberately
  decoupled.
- **Temporal block** (D15, ratified on
  [#237](https://github.com/englacial/zagg/issues/237)): a store carrying this
  block declares `spec: "morton-hive/2"` (the version string of D6 covers these
  temporal/windowed-leaf extensions; `/3` = the same semantics under D23
  window-only leaf naming). It records time
  encoding/units/epoch/calendar, the membership timestamp field, the **window
  schedule** (`none` | `yearly` | `monthly` | `daily` | explicit range list;
  `quarterly` grammar-reserved), and the append policy. Label grammar and
  boundary semantics (UTC calendar terms, half-open `[start, end)`,
  lexicographic = chronological) are frozen on the
  [mortie spec page](https://github.com/espg/mortie/issues/62#issuecomment-4986809092).
  Generative schedules keep the manifest
  write-once and static as data accrues: appending a new year to a
  `yearly` store adds leaves the schedule already describes — no manifest
  touch, and **no new manifests**: each product tree has exactly one
  `morton_hive.json` (under D19 a multi-product store is a directory of
  product trees, each with its own bare-named manifest), and each new
  windowed leaf brings only its own zarr metadata + D4 stamp. The explicit-range-list form is the noted exception:
  appending a window outside the declared list re-templates the manifest (a
  rare, single-writer, template-time operation, not a worker-race write) —
  append-heavy stores should prefer generative schedules. (This exception is
  an implication recorded here from the ratified schedule set, not a point
  separately ratified on the #237 thread.) Temporal *extent* is
  deliberately **not** manifest data: actual ranges live on the leaf stamps
  (truth) and in the root summary (cache), splitting static schema from
  accruing state exactly the way coverage splits under D9. The default
  schedule is `none` (no temporal partitioning; a re-run replaces the leaf —
  the honest rename of the drafted `mission`, which made a completeness claim
  it couldn't keep for ongoing missions) — existing aggregation stores are
  unchanged. Ongoing missions with append intent declare a generative
  schedule (`yearly` is the expected production default; t-digest
  mergeability makes mission-scale statistics a read-time merge over window
  leaves, the same approximation class as the worker's existing cross-buffer
  merge).

This file is the reader's bootstrap: with it, every shard path is computable
arithmetically with zero requests.

## 4. Layer 2: coverage MOCs (hierarchical domain declaration)

Coverage is declared hierarchically — **worker-owned at the leaves,
sweep-composed above** — tiered the way cloud-geo formats split bbox /
geometry / data (GeoParquet's `bbox` covering column vs. the WKB column vs.
the data itself). Tracks [#200](https://github.com/englacial/zagg/issues/200).

**The three tiers, per shard:**

- **Tier 0 — the morton box** (fixed width): the minimal MOC with **≤ 4
  members** (mixed order allowed) covering the shard's occupied cells.
  Existence is guaranteed: within one base cell, any coverage has a deepest
  common ancestor whose ≤ 4 intersecting children form a valid cover — and a
  shard's coverage is within one base cell *by construction* (a shard is a
  single subtree; its id alone is the trivial 1-member cover, so the box is
  what buys sub-shard resolution). Padded to exactly 4 slots for fixed width
  (32 B raw; four decimal strings in attrs); pad-sentinel *lean* is null
  (base-0 words / JSON `null`), with repetition-padding the viable
  alternative since repeats are idempotent under MOC algebra — the choice
  is frozen with the mortie-side spec (O8). Future *store-level* covers that cross base
  cells generalize to **≤ 12 members** (the 12 base cells). Readers
  AOI-reject on the box without parsing anything larger.
- **Tier 1 — the exact shard bitmap** *(as O8 resolved it — the originally
  drafted "budgeted, coarsen-to-fit MOC" was superseded by the
  [#202 item (6) measurement](https://github.com/englacial/zagg/issues/200#issuecomment-4939264286):
  for linear-track occupancy, coarsen-to-fit ranges at KB budgets deliver
  only box-level filtering while a compressed bitmap reaches exact at
  ~25 KB)*: the shard's cell-order occupancy as a **zstd-compressed bit
  field**, one bit per subtree cell in ascending packed-word order, stored
  as the in-leaf `coverage.moc` sidecar. Raw size is deterministic
  (`ceil(4^depth/8)`) regardless of fragmentation; no coarsened variant is
  built (one code path, sidecar-only). The stamp attrs carry only the box +
  the bitmap's order + pointer + byte sizes (one extra GET, paid only by
  readers that pass the box test). The sidecar is the *noted exception* to
  §2's "vanilla zarr v3" leaf: one foreign key inside the leaf, ignored by
  zarr readers (data reads unaffected; member enumeration warns and skips
  it).

  *D14 amendment (ratified on
  [#237](https://github.com/englacial/zagg/issues/237)): the stamp envelope's
  `encoding` discriminator gains a third value, **`"full"`**, meaning
  "coverage = the entire shard subtree" — no sidecar is written. Decided by
  one popcount at stamp time. This is the fast path for dense-by-construction
  workloads (pull-NN raster, #218/#237): interior shards skip the sidecar
  and its GET entirely, while edge-of-scene/swath shards write the real
  bitmap — the **spatial union across the leaf window's acquisitions**
  (per-timestep validity stays in the data plane as nodata, D9 applied one
  level down). One code path with a cheap branch, not a raster special
  case; the tier-0 box is carried unconditionally either way.*
- **Tier 2 — exact**: the `morton` coordinate array in the leaf *is* the
  exact cell list. The MOC tiers are indexes, never truth (D9 discipline,
  applied one level down).

**Ownership and lifecycle:**

- **Leaf MOCs are worker-owned and ride the commit stamp** (D4): the payload
  lands on (or is finalized before) the shard's final root `attrs.update()`
  PUT. Zero extra requests in the attrs case, and debris semantics are
  inherited automatically — a torn worker's MOC never becomes visible.
- **Ancestor and root MOCs are composed by the §7 sweep**: union of
  children, re-coarsened to the same budget, in the same bottom-up
  level-by-level orchestration as overview zarrs. All regenerable caches.
- **Optional end-of-run root `coverage.moc`** (O9): flag-gated. The
  dispatcher cannot write S3 (only the worker execution role can), so it
  posts its completion list to a **fire-and-forget worker invoke**
  (`InvocationType="Event"`, ~10 ms of dispatcher wall clock, run-size
  independent) that writes a shard-order root MOC for the one-GET
  bootstrap. Under D15 this root summary also carries the time-range union
  alongside the MOC (cache, sweep-regenerable). Failure is harmless: readers
  degrade to the sweep MOC or the
  walk, never to wrong answers. Incremental runs: the leaves carry durable
  truth, so the sweep is always a correct rebuilder, and the end-of-run
  write may union with a prior root object.
- **Everything above the leaf is a cache, not truth** (D9). Timestamped,
  regenerable (from leaf stamps or a tree walk). The strongly-consistent
  LIST walk (§2) remains ground truth; a run that crashes before any root
  MOC exists degrades to walking, never to wrong answers.
- **This replaces consolidated metadata** for extent/discovery. Consolidation
  measured +70 s per worker and is disabled by default; the coverage tiers
  cost effectively nothing and answer the actual question readers ask
  ("where is there data?") in one GET.

O8 and O9 are **resolved** (espg-ratified on the
[#200 thread](https://github.com/englacial/zagg/issues/200#issuecomment-4939477871),
implemented on PR #208): the shard tier is an **exact cell-order occupancy
bitmap, zstd-compressed, as an in-leaf sidecar object** — attrs carry only
the tier-0 box + order + pointer + sizes, with a null pad sentinel — and the
end-of-run root MOC **defaults on** for hive stores. The root object
serializes per O1 as JSON ranges with decimal-string endpoints.

## 5. Layer 3: reader architecture

The hot path is arithmetic; LIST-walking is the fallback. This is substantial
new wiring on the read side.

**Single-dataset flow:**

1. GET `morton_hive.json` (once per store, cacheable).
2. GET `coverage.moc`; intersect with the query AOI's MOC (`moc_and`) →
   the populated shard set within the region. Zero LISTs.
3. For each shard id: compute the hive path by string arithmetic
   (digits → components), open the leaf zarr, check the commit stamp. The
   stamp's morton box / shard MOC (§4) lets the reader AOI-reject the leaf
   before touching chunk data.
4. **Fallback / discovery walk** (no MOC, mixed orders, or verification):
   from any node, delimiter-LIST; recurse on `[1-4]/` children; a `*.zarr`
   entry is data at that node; no digit children ⇒ nothing finer. At each
   zarr encountered, the `role` attribute (§7) says whether it's a summary
   you may stop at (display) or source you must not conflate (analysis).

**Cross-resolution join** (the ICESat-2 × GEDI case):

1. Read the *target* dataset's manifest → its `cell_order`/`shard_order`.
2. Truncate the fine observation's morton decimal string to the target's
   cell order (equivalently `rust_mi_coarsen` on the packed word) → the
   containing target cell id. Truncate further to the target's shard order →
   its shard's hive path. Zero requests to *locate*; one leaf open to *read*.
3. The nesting predicate is literally `fine_id.startswith(coarse_id)`.
4. Within the leaf zarr, the `morton` coordinate locates the observation(s)
   in the containing cell.

Per-observation LISTs are forbidden in the join loop: at 2,000 workers ×
millions of photons the walk is the robustness path, not the join path (D10).

## 6. Layer 4: the xarray/xdggs extension ("sparse DGGS")

This layer ships as zagg's own standalone extension — **moczarr** (O3
resolved: standalone-first, offered upstream; xdggs adoption of
`MortonIndexDtype` ([#72](https://github.com/englacial/zagg/issues/72)) is a
follow-on, not a gate). Broad scope:

- **Domain = MOC.** A dataset declares its coverage as a MOC instead of
  materializing dense full-sphere coordinates. The accessor uses it to
  truncate the notional global grid to where data exists — top-level MOC
  coverage/polygons drive what the extension exposes.
- **Coordinates stay sparse, and morton is the only stored coordinate**
  (D16). The `morton` coordinate (packed u64 words, `MortonIndexDtype` in
  memory — upstream ask tracked in
  [#72](https://github.com/englacial/zagg/issues/72)) is the sole cell
  labeling on disk. NESTED `cell_ids` remains the *interop* encoding but is
  fabricated exactly, on demand (`mort2healpix` is vectorized arithmetic):
  moczarr fabricates it Python-side, the gridlook-jupyter hub proxy at serve
  time, a TS decode for browser-direct stores. The `cell_ids_encoding` knob
  ([#135](https://github.com/englacial/zagg/issues/135)) retires with the
  D16 writer flip.
- **Dense views are fabricated lazily**, per-region, on demand — never stored.
- **Multi-store alignment**: opening several stores (datasets) over one AOI
  yields aligned sparse views whose join semantics are the §5 truncation
  rules. What this looks like as an xarray API (alignment? a join accessor?)
  is the biggest open design question (O4).

## 7. The pyramid / post-process sweep

Everything derived or stale-prone lives in a second pass, never at write time
(D11) — overviews aggregate across worker-shard boundaries, so they *can't*
be produced by shard workers anyway.

The sweep owns four derived artifact families (D22; the original scope was
the first two):

- **Overview zarrs at ancestor nodes**, explicitly marked
  (`role: overview` + source order + aggregation method in attrs). Never
  inferred from position: a shallow zarr may equally be *coarse source* in a
  sparse region. Full pyramid cost is a geometric ~1/3 extra storage
  (4 children per order).
- **MOC (re)generation** — compose ancestor MOCs bottom-up from the leaf
  stamps (union, re-coarsen to budget) and refresh the root `coverage.moc`.
- **Sub-shardmap rollups** (D22): each leaf prefix carries its shard's
  sub-map as full ShardMap JSON; the sweep folds them up-tree via the
  coarsen path (`ShardMap.reproject` — exact pure regroup, granule union
  deduped by id; [#294](https://github.com/englacial/zagg/issues/294)).
- **Stats/cost rollups** (D22): the per-shard stats sidecars (D20) fold
  up-tree — the schema is associative by construction, so the rollup is the
  same fold shape as the pyramid.
- **Optional interop materialization**: if a use case ever demands a
  `zarr.open(store_root)`-able hierarchy or a one-GET consolidated index,
  the sweep generates it *as a derived artifact* here. Round one ships
  without it (D12).

Every rollup is stamped with **generation info** (merged-leaf count + max
leaf timestamp): after a leaf re-run, ancestors are *detectably* stale —
staleness is detected, not prevented, and rollups are regenerated
opportunistically (D9 semantics). The core test obligation is
**rollup == direct**: the folded artifact at level N must equal direct
computation at level N, for every family. Triggering is an end-of-run
dispatcher hook plus a manual CLI; the sweep discovers work from the run
record, not by listing.

The sweep is idempotent and can fail or lag without corrupting anything: the
write path (§2) is load-bearing; this phase is optimization — deleting every
rollup leaves all leaf reads intact.

## 8. Decisions registry

### 8.1 Decisions made (rationale recorded)

- **D1 — Ids are morton decimal strings; packed u64 is canonical storage.**
  Settled on mortie #48 (Option A): packed `uint64` kernel as the compute
  substrate, the signed decimal string as the render-only repr and the
  external/path form. Type-stable: strings always, at every order; never
  data-dependent int-vs-string emits.
- **D2 — One digit per path component.** Mixed shard orders are real (across
  datasets and within a store), so every order must be a node boundary.
  Grouped-digit and two-level schedules are dead ends. Note the schedule is
  *logical only*: S3 partitioning is delimiter-blind, so slashes buy zero
  throughput ([#197](https://github.com/englacial/zagg/issues/197) is the
  throughput fix). *Amended by D21*: digit-chunking becomes the declared
  `path_grouping` manifest parameter (default `1` = this rule); the
  "grouped-digit schedules are dead ends" verdict is thereby softened to
  "grouping is a parameter, never a schema fork."
- **D3 — Full morton id at the leaf** (`{full_id}.zarr`), self-describing.
  (Unchanged by D19: product identity lives at the product root, above the
  tree. *Superseded by D23 for `morton-hive/3` stores*: the basename becomes
  the time window; the full id stays recoverable from the path and from the
  stamp/sidecar `shard_key`.)
- **D4 — Commit stamp via final root-attrs update.** Absence (LIST) is
  trustworthy; presence requires the stamp. Torn shards are debris,
  overwritable on retry. One small PUT; not consolidation.
- **D5 — Zero metadata above the leaf on the write path.** No zarr groups at
  digit nodes, no shared mutable state during fan-out.
- **D6 — The convention is versioned** (`morton-hive/1`) in the manifest.
- **D7 — One store per dataset, own orders.** Workers never mix datasets;
  interop between any pair of stores is truncation against each manifest.
- **D8 — Coverage MOCs are hierarchical and worker-owned at the leaves**
  (amended per #200). Each shard's tier-0 morton box + tier-1 budgeted MOC
  rides the D4 commit stamp; ancestor/root MOCs are sweep-composed unions;
  an optional end-of-run root MOC is written by a fire-and-forget worker
  invoke from the dispatcher's completion list (the orchestrator has no S3
  write access). Replaces consolidated metadata (disabled; measured
  +70 s/worker).
- **D9 — MOC is a regenerable cache; the tree walk is ground truth.**
- **D10 — Arithmetic-first reads; no LISTs in join loops.** *Strengthened
  with D22 (espg-ratified in-session, recorded on
  [#300](https://github.com/englacial/zagg/issues/300#issuecomment-5017464707)):
  MOC-first is the reader contract — full recursive enumeration is
  **out-of-contract** for readers, reserved for prefix-sharded audit
  tooling. The §5 discovery walk remains the robustness/verification path,
  never the read path.*
- **D11 — Pyramids/overviews are a second-pass sweep**, `role: overview`
  attrs, never inferred from tree position. *Scope extended by D22 (four
  artifact families, generation stamps, per-family manifest schedules).*
- **D12 — Plain manifest, not a zarr-native hierarchy, in round one.**
  Hierarchy metadata at nodes reintroduces the metadata-op storm
  ([#189](https://github.com/englacial/zagg/issues/189),
  [#194](https://github.com/englacial/zagg/issues/194)) and couples the
  layout to still-settling zarr v3 hierarchy semantics. One-way door avoided:
  a root `zarr.json` can be added later by the §7 sweep without breaking
  anything.
- **D13 — Appendable time series = time-windowed, write-once leaves**
  (espg-ratified on [#237](https://github.com/englacial/zagg/issues/237)).
  One leaf per (shard, window) under a manifest-declared window schedule;
  every leaf keeps full D4/D5 semantics (write-once, stamped, binary
  debris, zero shared mutable state). Backfill is a new earlier-window
  leaf; re-running a window is idempotent replacement. Rejected: a
  high-water time index (can't extend backward) and per-run stamp entries
  with array `resize` (mutable shared state at the leaf — attrs RMW +
  metadata rewrite races, non-binary debris, unordered time axis on
  backfill). Not raster-specific: `none` (default; no partitioning, bare
  leaf names) reproduces today's aggregation stores byte-identically;
  per-year 88S runs, seasonal subsets, and append-as-acquired are all
  window leaves under the same convention. Leaf naming is **frozen**:
  `{full_id}_{window}.zarr`, underscore separator, split on the first `_`
  — grammar and boundary semantics recorded on the
  [mortie spec page](https://github.com/espg/mortie/issues/62#issuecomment-4986809092)
  as part of `morton-hive/2`. (Unchanged by D19 — leaf naming survives the
  product-root design intact. *Naming revised by D23 for `morton-hive/3`*:
  the basename becomes the window alone; the `/2` grammar stays frozen for
  `/2` stores.) Reserved (lean, not decided): §7
  overview/pyramid zarrs inherit window naming (per-window overviews, with
  an optional all-time overview as a derived artifact).
- **D14 — Coverage `encoding: "full"` fast path**
  (espg-ratified on [#237](https://github.com/englacial/zagg/issues/237)).
  The stamp's coverage envelope discriminator becomes
  `"ranges" | "bitmap" | "full"`; `"full"` = whole-subtree coverage, no
  sidecar written, chosen by a popcount at stamp time. Tier-0 box is
  always carried. Edge-of-scene/swath shards (the partial case) write the
  real bitmap as the spatial union across the leaf window's acquisitions;
  per-timestep validity remains data-plane nodata.
- **D15 — Temporal declaration splits like coverage**
  (espg-ratified on [#237](https://github.com/englacial/zagg/issues/237)).
  Manifest (`morton-hive/2`, static): time encoding + window schedule +
  append policy. Leaf stamps (truth): each windowed leaf's actual time
  range + acquisition/granule count. Root summary (cache): the end-of-run
  root coverage object gains the time-range union alongside the MOC;
  sweep-regenerable, never truth. Extent never lives in the manifest, so
  the manifest stays write-once (§3; written async at init with a finalize
  backstop since issue #252); the noted exception
  is the explicit-range-list schedule, where appending outside the list
  re-templates the manifest — append-heavy stores should prefer generative
  schedules. (That exception is an implication recorded from the ratified
  schedule set, not separately ratified on the thread.)
- **D16 — Morton-only storage: NESTED is fabricated, never stored**
  (espg-ratified in-session 2026-07-17, filed on
  [#262](https://github.com/englacial/zagg/issues/262); both verification
  checks — artools clean, gridlook requirement satisfiable by fabrication —
  [confirmed on the thread](https://github.com/englacial/zagg/issues/262#issuecomment-5007833161)).
  zagg stops writing the `cell_ids` (NESTED uint64) array to leaves;
  `morton` is the only stored cell coordinate and the declared convention
  coordinate. Rationale: (1) morton words carry order intrinsically, so
  mixed-order arrays (D2 coarse shards, §7 pyramids) are first-class where
  NESTED u64 needs side-metadata or zuniq/nuniq; (2) kills the
  dual-encoding cost — one u64 array per leaf (8 B/cell, plus one array's
  objects per leaf in the
  [#236](https://github.com/englacial/zagg/issues/236)/[#240](https://github.com/englacial/zagg/issues/240)
  object-count currency) and the NESTED↔morton double-encode maintenance
  ([#72](https://github.com/englacial/zagg/issues/72)); (3) the reader
  stack is ready — the moczarr fabrication layer was the named gate and is
  merged (espg/moczarr PR #7), with `open_hive` + xdggs
  `grid_name: "morton"` + MOC-backed lazy index. Third instance of the
  derived-views principle (§6 dense views, D9 caches). Ratified
  refinements: the dggs attrs use a **distinct grid `name: "morton"`** —
  never `name: "healpix"` + `indexing_scheme: "morton"`, which scheme-blind
  readers silently misread as NESTED (garbage renders); a distinct name
  makes them hard-reject with a diagnostic, and matches moczarr's xdggs
  registration. Unknown-resolution point encodings at order 29 are clipped
  on the fly to order 24 for Number-safe browser paths (NESTED ids are
  float64-exact only through order 24; genuinely-finer-than-24 data takes
  other measures — hub-side fabrication, aggregation). The writer flip
  sequences as 0.x phases (emit knob default-on → default flip → removal).
  (An earlier plan bundled the flip with a #299 leaf-basename rename;
  D19's product-root revision made #299 additive, so this writer flip is
  the one remaining breaking store change in this family.) The order-29
  discriminator metadata is O10.

- **D17 — Hive+sharded is the HEALPix default; flat/fullsphere deprecated;
  dense leaf arrays write through the ShardingCodec.**
  (Phase-1 ratification recorded on
  [#251](https://github.com/englacial/zagg/issues/251#issuecomment-4989697559);
  landed via PR #257 (default flip + dense-layout removal), #233 (`sharded:
  true` default), #241 (hive dense parity); leaf object model ratified
  in-session, recorded on
  [#236](https://github.com/englacial/zagg/issues/236#issuecomment-4986241727).)
  Every HEALPix config — point aggregation and raster — targets hive; the
  grid keys the default layout (hive for HEALPix; PR #257 / #253), and an
  explicit `store_layout: flat` survives only as a deprecated escape hatch
  (emits a `DeprecationWarning`) until the O3-gated flat removal; rect keeps
  its bounded flat store. Hive leaf dense arrays are one object per
  array per leaf via the ShardingCodec (always-accumulate); `sharded: false`
  remains a legitimate opt-out, and **raster leaves skip the sharding codec
  by design** — the only raster/aggregation difference at the leaf
  ([espg on PR #257](https://github.com/englacial/zagg/pull/257#issuecomment-5005249126)).
  `grid.shard_order` (sub-shard object granularity) is removed
  ([#238](https://github.com/englacial/zagg/issues/238#issuecomment-4986885796));
  the manifest's `shard_order` (tree/dispatch order) is unrelated and stays.
  Full flat-machinery removal remains gated on the O3 reader (the #251
  phase-2 gate).
- **D18 — Ragged output = sharded vlen-bytes** (measurement + recommendation
  on
  [#209](https://github.com/englacial/zagg/issues/209#issuecomment-4940116927),
  with espg's in-session ratification of the vlen-bytes adoption recorded in
  the [#210](https://github.com/englacial/zagg/issues/210) body; tracking
  [#210](https://github.com/englacial/zagg/issues/210)). Ragged
  t-digest arrays store as `bytes` dtype + vlen-bytes codec under the
  ShardingCodec — one object per shard, 2-GET single-cell reads — with
  element interpretation as an attrs convention and a **golden-bytes framing
  pin** byte-compatible with numcodecs `VLenArray`. The CSR-per-inner-chunk
  fanout is deleted. A typed `vlen-array<float32>` ZDType is deferred behind
  three gates (upstream zarr-extensions convergence, at-scale proof, a
  second consumer); the framing pin guarantees any future migration is
  metadata-only. This is the second noted soft-exception to "vanilla zarr
  v3" leaves (after the §4 coverage sidecar): plain zarr readers see opaque
  bytes, not garbage.
- **D19 — Named product roots + semantic-core hash: a multi-product store
  is a directory of stores; the name is the address, the hash is the
  integrity check** (concept settled across
  [#296](https://github.com/englacial/zagg/issues/296#issuecomment-5014826849)
  and [#299](https://github.com/englacial/zagg/issues/299) — espg
  on-thread:
  [registry + per-product manifests](https://github.com/englacial/zagg/issues/299#issuecomment-5017263033);
  revision history, all espg-ratified in-session 2026-07-20, trade
  studies on the PR #306 thread: leaf-hash basenames (phase 2) →
  hash-named product roots (phase 3) → **named roots with the hash demoted
  to metadata (this revision)**). Each product lives under its own
  human-readable root prefix `{name}/`; a product subtree is a *complete,
  unmodified* morton-hive store — bare-named manifest and MOC,
  `{full_id}.zarr` leaves under `/1`–`/2` grammars (`{window}.zarr` under
  `/3` — D23) — so existing single-product stores are already valid and
  the change is **additive, not breaking**. Readers distinguish the two
  root forms by content (manifest ⇒ bare store; name-shaped prefixes ⇒
  product directory; names must not match the base-component grammar);
  viewers enumerate products by listing the root and reading each
  `{name}/morton_hive.json` — no name↔hash translation layer.
  **Identity is split**: the *name* addresses the product; the
  **`semantic_hash`** verifies it — sha256 over the canonicalized
  **output-defining subset only**: the `aggregation` block (functions +
  params + dtypes + fills + ragged kinds), the `data_source` semantics
  (dataset/product, groups, coordinates, variables, filters), and the
  grid *type + indexing scheme*. Excluded as packaging: cell order (a
  resolution axis — D24), parent/shard order, `chunk_inner`/`sharded`,
  worker size, streaming mode (merge-vs-spill lands `np.isclose` and
  shares one store, with the actual mode recorded per-run), and read
  knobs — hashing the whole template would have made o8 and o9 runs
  different products and blocked mixed-order processing. The hash is a
  **frozen manifest key** (reusing a name with different aggregation
  semantics refuses up front, like any frozen-key mismatch) and is
  recorded in leaf attrs and D20 sidecars. The *literal* template is
  deliberately **not** the product-level record (it carries run-varying
  packaging): the product root holds the canonical semantic core as
  `aggregation.yaml` (deterministic, valid YAML); each run archives its
  literal template with its run record, and sidecars carry the run id to
  join back. (This factoring formalizes a seam the code already has:
  `spatial_signature` vs `output_field_signature`, the #89 split.)
  Rationale for product-root prefixes (unchanged from the hash-first
  study): S3's only cheap scoping primitive is the prefix, and
  product-scoped operations (delete, lifecycle/expiration, access policy,
  inventory) dominate — each is one prefix rule. Cost prediction for
  appending new shards is likewise scoped (espg-noted in-session): the
  product root holds its own telemetry history, so the pilot-first
  estimator's priors (the #298 design) are exactly the product's own
  records. What a shared spatial tree would have offered — one prefix
  spanning all products — serves no planned workload (§5 reads are
  arithmetic). **Catalog identity lives in the sidecar, never the name**:
  granule count + sha256 of sorted granule ids + zagg version;
  dedup/`has_run` consults the computed path, the `semantic_hash`, *and*
  the sidecar catalog identity (a catalog-grown shard is "stale", not
  "hit"). Immutable-provenance naming (product root
  `{name}+{catalog-hash}/`) stays an opt-in for frozen-catalog archival
  runs. The output content hash that makes dedup *verifiable* is O11
  (proposal; it complements the semantic hash — "intended identical" vs
  "actually byte-identical"). A community registry maps names → semantic
  cores + hashes; cross-deployment name collisions are disambiguated by
  the hash in metadata rather than prevented by unreadable paths. D7
  generalizes cleanly: "one store per dataset" becomes "one product tree
  per semantic core" under a shared root, with cross-product joins
  unchanged.
- **D20 — Per-shard telemetry sidecar + run records** (espg on-thread:
  [sibling placement + envelope ride](https://github.com/englacial/zagg/issues/297#issuecomment-5016910923),
  [caller identity](https://github.com/englacial/zagg/issues/297#issuecomment-5016901381);
  schema decisions recorded on
  [#296](https://github.com/englacial/zagg/issues/296#issuecomment-5014826849)).
  Each successful shard writes a versioned stats record as a **sibling**
  object next to the leaf (not inside the `.zarr/`): timings
  (read/index/aggregate/write/spill), counts, memory, cost (GB-s ×
  price), catalog identity (D19), zagg version, and `invoked_by` (caller
  identity resolved once per run by the dispatcher via STS and stamped
  through the invoke payload — workers cannot see the caller). The schema
  is **mergeable by construction** — only associative stats (counts, sums,
  min/max, t-digests; never stored means) — so up-tree rollups are a pure
  fold (D22). The record also rides the async result envelope; the
  dispatcher writes a **run-level parquet at the product root** — a run maps
  to one product ⇒ one product tree (D7/D19), so it lands under `{name}/`,
  never the multi-product store root, whose node invariant admits only
  `{name}/` product roots — with one row per
  shard, *including failure rows* sourced from the run report — sidecars
  exist only on success; CloudWatch structured logs remain the failure
  forensics channel). Run-record names are **timestamp-first**
  (`stats_{timestamp}_{run_id}.parquet`) so lexicographic listing is
  chronological and time-range queries prune on keys before reading;
  per-user scoping stays the `invoked_by` *column* (names stay stable
  identifiers). Sidecars carry the `run_id` (joining leaf → run record →
  that run's archived literal template, D19) and the D19 `semantic_hash`;
  they omit account-identifying fields (request ids, ARNs beyond the
  caller identity).
- **D21 — `path_grouping` is a manifest parameter, not a layout**
  (espg-ratified in-session, recorded on
  [#300](https://github.com/englacial/zagg/issues/300#issuecomment-5017464707)).
  The manifest declares how many morton digits each path component chunks;
  existing stores are retroactively `1`; new stores default `1`; readers
  chunk the digit string per the manifest. Rationale: hive paths are
  *computed* (manifest + MOC → arithmetic paths → parallel GETs — zero
  LISTs on every hot path), so grouping is walk ergonomics, not
  performance; hard-adopting a grouped layout would have forked the path
  dialect permanently for ~$1.60 and ~1.6 s per rare full walk. A future
  default flip (e.g. to 3-order groups) is a parameter change, never a
  schema break.
- **D22 — One unified second-pass sweep owns all derived artifacts**
  (espg on-thread:
  [trigger + sub-map format + schedule discussion](https://github.com/englacial/zagg/issues/300#issuecomment-5017291722);
  schedule decoupling ratified in-session, recorded on
  [#300](https://github.com/englacial/zagg/issues/300#issuecomment-5017464707)).
  Four families — overview zarrs, MOC regen, sub-shardmap rollups (full
  ShardMap JSON at leaf prefixes, folded via the exact coarsen regroup,
  [#294](https://github.com/englacial/zagg/issues/294)), stats rollups
  (D20 fold) — in one idempotent pass with per-family, manifest-declared
  order schedules (decoupled from `path_grouping`). Generation stamps
  (merged-leaf count + max leaf timestamp) make staleness detectable, not
  prevented; **rollup == direct** is the standing test obligation per
  family; trigger is end-of-run hook + manual CLI; the sweep discovers work
  from the run record, never by listing. Nothing is load-bearing: deleting
  every rollup leaves leaf reads intact (D9 semantics). (The refine
  direction of `ShardMap.reproject` and its no-region semantics are still
  under review on PR #295 — the sweep depends only on the exact coarsen
  direction.)
- **D23 — Leaf basename = time window (`{window}.zarr`), `morton-hive/3`**
  (espg-proposed and ratified in-session, 2026-07-20; recorded here and on
  the PR #306 thread). Completes the axis separation D19 began: product =
  root prefix (D19), space = digit path (D1/D2), **time = basename** —
  each identity axis in exactly one place, none encoded twice. This
  removes the last path/basename redundancy (the #296 observation that
  started the naming work, applied to its final instance): the morton id
  currently appears in both the path and the leaf name. Listing a shard
  node returns the temporal inventory directly (`2019.zarr`,
  `2020.zarr`, …) — what append planning and time-series discovery
  actually ask. Spec bump to `morton-hive/3` under D6 versioning: `/1`
  and `/2` stores remain valid forever under their frozen grammars;
  readers discriminate by the manifest `spec` string; the `/3` grammar is
  to be re-frozen on the mortie spec page
  ([mortie#62](https://github.com/espg/mortie/issues/62)). Costs accepted
  with the decision: the name==path self-check moves to the stamp attrs /
  D20 sidecar `shard_key` (an fsck pays one GET per leaf); D3's
  "unambiguous if moved" softens to "recoverable from attrs" — the moved
  case reduces to a downloader that materializes the prefix tree into
  folder names (espg-noted in-session). **Leans recorded, not ratified**:
  the `schedule: none` reserved token — proposal `all.zarr` (reads as
  all-time; cannot collide with the digit-shaped window grammar;
  `none.zarr` matches the schedule literal but reads worse) — and the
  sidecar alignment `{window}.stats.json` / `all.stats.json` (D20
  naming; follow-up to the merged PR #302). Rejected alternative: time
  as a *path* level (`{name}/{window}/{morton…}`) would make
  window-scoped ops prefix-cheap but duplicates the morton tree per
  window and shatters the dominant read — a time series at a location —
  across W prefixes; reads dominate window expiry, so time stays at the
  leaf.
- **D24 — Resolution polymorphism: cell order is a query/packaging axis,
  not product identity** (espg-proposed and ratified in-session,
  2026-07-20; rationale recorded on the PR #306 thread). Aggregation
  *composes* across orders — finer cells fold to coarser under the same
  merge law (exactly for count/sum/min/max; `np.isclose` for t-digest,
  whose merge is order-dependent — the same epistemic class as
  merge-vs-spill, already ruled one-store) — so a product's cell order is
  excluded from the D19 `semantic_hash`, and one product tree may carry
  **regionally heterogeneous resolution** (e.g. o19 cells in polar
  shards, o17 mid-latitude). The design had already committed to the
  pillars: D22's rollup==direct obligation *is* the composition claim;
  D16 chose morton words because they carry order intrinsically; D11's
  `role` attr anticipated coarse *source*; the #217 mergeable-reducer
  machinery provides per-aggregator merge laws. Consequences frozen with
  the decision: (1) **composability class** — `exact | approximate |
  none` — is declared in the semantic core, derived from the product's
  aggregator set; a `none` product pins its cell order (resolution *is*
  identity there) and refuses mixed-order appends. (2) Per (shard,
  window) there is **one resolution at a time**: heterogeneity is
  regional, across shards. A same-cell-order rerun is D4 idempotent
  replacement; **writing a different cell order into an occupied leaf
  refuses with a useful error** (espg-directed) — intentional
  re-resolution means rerunning at a parent order that isn't occupied, or
  explicitly clearing the leaf. (3) Manifest `cell_order` and
  `shard_order` become declared allowed sets/ranges (§3); per-leaf truth
  is the morton words + MOC. (4) Coarse source and sweep-built overviews
  unify — the same multi-order tree, distinguished only by `role`/
  provenance attrs; the pyramid is the store's resolution axis, partially
  materialized. Reader support is gated on mortie#116 mixed-order morton
  (tracked as moczarr#8); the schema needs no bump — this is what the
  coordinate system was built for.

### 8.2 Open for review (input needed)

- **O1 — MOC serialization format** for `coverage.moc`: JSON of nested-range
  pairs? Packed-word `.npy`? Needs to be frozen alongside the mortie spec
  (FITS/IVOA interop is an explicit non-goal per mortie #50).
- **O2 — MOC depth ceiling: resolved (mortie 0.9.0)** *(entry kept here
  for O# id stability)*. The cap was a stale
  `MAX_DEPTH = 18` constant, not a u64 limit; the coverage/MOC paths now
  reach the packed-u64 kernel ceiling (order 29), so cell-order MOCs at
  order 19 are representable today.
- **O3 — Upstream target: RESOLVED — standalone-first (moczarr)**
  ([espg-ratified, recorded on
  #251](https://github.com/englacial/zagg/issues/251#issuecomment-4989697559)):
  the sparse-DGGS reader ships as zagg's own standalone xarray extension
  (espg/moczarr, published from our side and offered upstream); xdggs
  adoption of MortonIndexDtype (#72) is a follow-on, not a 1.0 gate.
  Near-drop-in `xr.open_zarr()`-grade ergonomics is a hard acceptance
  criterion (it gates the #251 flat removal).
- **O4 — Multi-dataset join API**: what does the cross-resolution join look
  like in xarray terms — alignment, an accessor method, a lazy index?
- **O5 — Sentinel-2 encoding**: native ~10 m order, one order finer (6 m,
  collision-free), or much finer + nearest-neighbor cell groups? Per-dataset
  choice; the tree doesn't care, but the join ergonomics might.
- **O6 — Status channel layout**: stays flat (`<store>.status/<run>/...`)
  for now; revisit if poller LIST pagination becomes a bottleneck at
  global shard counts.
- **O7 — MOC staleness policy**: stamp `generated_at` + source (dispatcher
  vs sweep); do readers warn, re-walk, or trust silently when stale?
  Current lean (#200 thread): trust silently on the hot path (false
  negatives only, per D9), detect lazily (warn on a stamped leaf the MOC
  doesn't list), regenerate explicitly (`refresh=True` / the sweep); no
  wall-clock staleness horizon. The incremental-run half is settled by the
  §4 lifecycle (leaves are durable truth; the sweep rebuilds correctly).
  Implemented in this shape by PR #208's reader primitives
  (`zagg.coverage`: `warn_if_stale` — once per store, never auto-walk —
  and `refresh_root_coverage`, the explicit walk); the concurrent-run
  GET-union-PUT race (last writer wins until re-union/sweep) is recorded
  there as accepted under this same lean.
- **O8 — shard-MOC budget, serialization, carrier, pad sentinel**:
  **RESOLVED** ([espg-ratified](https://github.com/englacial/zagg/issues/200#issuecomment-4939477871),
  from the [#202 item (6) measurement](https://github.com/englacial/zagg/issues/200#issuecomment-4939264286)):
  no budgeted/coarsened tier — the leaf encoding is an **exact cell-order
  bitmap, zstd-compressed, as the in-leaf `coverage.moc` sidecar** (raw size
  deterministic at `ceil(4^depth/8)`, immune to the ragged worst case where
  exact ranges hit MB-scale on linear-track occupancy at ~1.1 cells/range).
  Stamp attrs carry only the tier-0 box + the bitmap's order + pointer +
  byte sizes; pad sentinel is JSON null; the envelope gains an
  `encoding: "ranges" | "bitmap"` discriminator (later extended by D14 to add
  a third value, `"full"`). Bit convention (frozen
  with the mortie spec, golden-vector-pinned on PR #208): bit i = the i-th
  shard-subtree cell in ascending packed-word order (base-4 D1 digit tail,
  digits 1..4 → 0..3), MSB-first per byte.
- **O9 — end-of-run root MOC default**: **RESOLVED — on** for hive stores
  ([espg](https://github.com/englacial/zagg/issues/200#issuecomment-4938859764):
  coverage MOCs are the default for healpix templates; PR #208 implements
  `output.coverage_moc`, default true under `store_layout: hive`, explicit
  true rejected elsewhere). The write is fail-open on both backends; the
  Lambda leg is one fire-and-forget `mode: "coverage"` Event invoke with the
  pre-serialized ranges envelope.
- **O10 — order-29 resolution discriminator** (carved from D16): two
  order-29 encodings exist — (a) genuinely order-29 resolution, and (b)
  unknown resolution point-encoded at order 29, the default for any raw
  lat/lon conversion and expected to be common. Readers need declared
  metadata to tell them apart, because the D16 clip rule (29→24) applies
  only to (b). Proposal: `resolution: "exact" | "point"` in the dggs attrs
  block, defaulting to `"point"` for raw conversions. Intersects mortie's
  documented point-id/area-word parse non-injectivity at order 29; field
  name, values, and placement to be frozen with the mortie spec page
  ([mortie#62](https://github.com/espg/mortie/issues/62)).
- **O11 — logical content hash of outputs** (carved from D19; **proposal
  awaiting espg sign-off** — espg asked for expanded context on
  [#299](https://github.com/englacial/zagg/issues/299#issuecomment-5017263033),
  expansion posted
  [in reply](https://github.com/englacial/zagg/issues/299#issuecomment-5017334704)).
  Proposal: per-array sha256 over *decoded* values (raw C-order bytes at the
  declared dtype, after decompression — never stored object bytes, which
  churn on codec/library upgrades), recorded in the D20 sidecar as
  `{array_name: hash}` plus one combined hash. Exact bytes, no float
  tolerance: any value change — including flagged code changes that pass
  `np.isclose` (the PR #282 class) — flips the hash by design;
  interpretation pairs the hash with the sidecar's recorded zagg version.
  Motivation: outputs have been byte-identical between runs in practice, so
  this turns D19's "probably already ran" dedup into a verifiable claim
  without folding catalog identity into leaf names.

## 9. References

Community precedents for the §4 tiered-coverage conventions (budgeted
conservative summary in the metadata plane, exact geometry in the data
plane):

- STAC best practices — footprint simplification discipline:
  <https://github.com/radiantearth/stac-spec/blob/master/best-practices.md>
- STAC item spec — bbox + geometry as the queryable summary:
  <https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md>
- stactools raster-footprint — densify → reproject → simplify-to-tolerance:
  <https://element84.com/geospatial/the-stactools-raster-footprint-utility/>
- CMR ingest API — geometry-complexity constraints at ingest:
  <https://cmr.earthdata.nasa.gov/ingest/site/docs/ingest/api.html>
- GeoParquet — `bbox` covering column (fixed-size conservative cover in
  metadata, exact WKB in data; the direct analog of the tier-0 morton box):
  <https://github.com/opengeospatial/geoparquet>
- PostgreSQL TOAST — the ~2 KB inline threshold (portability footnote for
  the tier-1 budget):
  <https://www.postgresql.org/docs/current/storage-toast.html>
- IVOA MOC recommendation — degraded-order MOC practice; NUNIQ int64
  order-29 ceiling: <https://www.ivoa.net/documents/MOC/>
- H3 `compactCells` — mixed-resolution minimal covers:
  <https://h3geo.org/docs/api/hierarchy>
