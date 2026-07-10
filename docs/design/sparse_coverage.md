# Sparse coverage & the cross-resolution read path

**Status**: draft. Tracks [#198](https://github.com/englacial/zagg/issues/198).

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
3. **xdggs assumes dense full-sphere coordinates.** The current `fullsphere`
   layout materializes a coordinate entry for every cell of the global grid.
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
{store_root}/
  morton_hive.json               <- static manifest (§3); root-only exception
  coverage.moc                   <- optional root MOC (§4, O9); root-only exception
  {sign+base}/{d1}/{d2}/.../     <- one digit per level (D2)
    {full_id}.zarr/              <- self-describing leaf (D3), vanilla zarr v3
```

- **Ids are morton decimal strings** (D1): sign + base digit (constant width,
  12 values `1..6`/`-1..-6`), then one digit per order, digits `1-4`, never
  `0`. String prefix = spatial ancestor at every level.
- **One digit per path component** (D2), because shards live at mixed orders —
  across datasets *and* within a store (coarse shards in sparse regions) —
  so every order must be a legal node.
- **Full morton id at the leaf** (D3): `.../1/2/3/-31123.zarr/` is
  self-describing without parsing its path, greppable in inventories,
  unambiguous if moved.
- **Node invariant**: below the root, a node contains *only* digit children
  (`[1-4]/`) and `*.zarr` objects. Nothing else, ever — the walker's child
  classification depends on it. The root alone also carries the manifest and
  MOC objects.
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

Written **once at template time**, before any shard dispatches. O(1); never
touched again during a run. Contents:

- `spec`: convention version string (e.g. `"morton-hive/1"`) — the convention
  itself is versioned from day one (D6).
- Dataset identity (short name, product/version).
- `cell_order`, `shard_order` (and, if a store permits region-dependent shard
  orders, the allowed set).
- Split schedule (implicit under D2: one digit per level to `shard_order`;
  recorded explicitly for forward compatibility).
- Pyramid declaration: which ancestor orders carry overview zarrs, and their
  aggregation methods (populated/updated by the §7 sweep).

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
- **Tier 1 — the budgeted shard MOC**: cell-level coverage, exact if it fits
  the budget, else **coarsened until it fits**. Coarsening only over-covers
  (conservative superset): false positives cost one wasted leaf read; false
  negatives are impossible. The achieved depth is recorded with the payload.
  Budget scale is open (O8) and is matched to **S3/cloud conventions, not
  database-page conventions** — candidates span KB to low MB (the PostgreSQL
  ~2 KB TOAST inline threshold is retained only as a portability footnote).
  The budget also picks the carrier: a KB-scale payload rides the commit
  stamp attrs (zero extra objects); a hundreds-of-KB tier becomes its own
  object inside the leaf, with only the box + achieved depth + pointer in
  attrs (one extra GET, paid only by readers that want it). If the in-leaf
  object survives O8, it is a *noted exception* to §2's "vanilla zarr v3"
  leaf: one foreign key inside the leaf, invisible to zarr readers.
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
  bootstrap. Failure is harmless: readers degrade to the sweep MOC or the
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

Open items: budget scale / serialization / carrier / pad sentinel for the
shard MOC tiers (O8, measured on
[#202](https://github.com/englacial/zagg/issues/202) item (6)); the
end-of-run root MOC default (O9); root-object serialization format (O1).

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

This is the layer that should eventually land upstream — in xdggs if the
convention generalizes, or as its own xarray extension if not (O3). Broad
scope:

- **Domain = MOC.** A dataset declares its coverage as a MOC instead of
  materializing dense full-sphere coordinates. The accessor uses it to
  truncate the notional global grid to where data exists — top-level MOC
  coverage/polygons drive what the extension exposes.
- **Coordinates stay sparse.** The `morton` coordinate (packed u64 words,
  `MortonIndexDtype` in memory — upstream ask tracked in
  [#72](https://github.com/englacial/zagg/issues/72)) is the native cell
  labeling; `cell_ids` (HEALPix NESTED) remains the interop encoding
  (`cell_ids_encoding`, [#135](https://github.com/englacial/zagg/issues/135)).
- **Dense views are fabricated lazily**, per-region, on demand — never stored.
- **Multi-store alignment**: opening several stores (datasets) over one AOI
  yields aligned sparse views whose join semantics are the §5 truncation
  rules. What this looks like as an xarray API (alignment? a join accessor?)
  is the biggest open design question (O4).

## 7. The pyramid / post-process sweep

Everything derived or stale-prone lives in a second pass, never at write time
(D11) — overviews aggregate across worker-shard boundaries, so they *can't*
be produced by shard workers anyway.

- **Overview zarrs at ancestor nodes**, explicitly marked
  (`role: overview` + source order + aggregation method in attrs). Never
  inferred from position: a shallow zarr may equally be *coarse source* in a
  sparse region. Full pyramid cost is a geometric ~1/3 extra storage
  (4 children per order).
- **MOC (re)generation** — compose ancestor MOCs bottom-up from the leaf
  stamps (union, re-coarsen to budget) and refresh the root `coverage.moc`.
- **Optional interop materialization**: if a use case ever demands a
  `zarr.open(store_root)`-able hierarchy or a one-GET consolidated index,
  the sweep generates it *as a derived artifact* here. Round one ships
  without it (D12).

The sweep is idempotent and can fail or lag without corrupting anything: the
write path (§2) is load-bearing; this phase is optimization.

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
  throughput fix).
- **D3 — Full morton id at the leaf** (`{full_id}.zarr`), self-describing.
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
- **D10 — Arithmetic-first reads; no LISTs in join loops.**
- **D11 — Pyramids/overviews are a second-pass sweep**, `role: overview`
  attrs, never inferred from tree position.
- **D12 — Plain manifest, not a zarr-native hierarchy, in round one.**
  Hierarchy metadata at nodes reintroduces the metadata-op storm
  ([#189](https://github.com/englacial/zagg/issues/189),
  [#194](https://github.com/englacial/zagg/issues/194)) and couples the
  layout to still-settling zarr v3 hierarchy semantics. One-way door avoided:
  a root `zarr.json` can be added later by the §7 sweep without breaking
  anything.

### 8.2 Open for review (input needed)

- **O1 — MOC serialization format** for `coverage.moc`: JSON of nested-range
  pairs? Packed-word `.npy`? Needs to be frozen alongside the mortie spec
  (FITS/IVOA interop is an explicit non-goal per mortie #50).
- **O2 — MOC depth ceiling: resolved (mortie 0.9.0)** *(entry kept here
  for O# id stability)*. The cap was a stale
  `MAX_DEPTH = 18` constant, not a u64 limit; the coverage/MOC paths now
  reach the packed-u64 kernel ceiling (order 29), so cell-order MOCs at
  order 19 are representable today.
- **O3 — Upstream target**: extend xdggs vs. standalone xarray extension.
  Depends partly on xdggs's appetite for MortonIndexDtype (#72) and
  non-dense coordinate models.
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
- **O8 — shard-MOC budget, serialization, carrier, pad sentinel**: byte
  scale matched to S3/cloud conventions (candidates KB → low MB); ranges
  vs. bitmap serialization; commit-stamp attrs payload vs. in-leaf object
  carrier; box pad sentinel (base-0/null vs. repetition). Decided using
  the #202 item (6) measurement (SERC + 88S shards; the tier-0 box is the
  baseline each budget must beat; over-coverage reported in STAC
  simplification-error terms). Frozen alongside the mortie spec.
- **O9 — end-of-run root MOC default**: on (the fire-and-forget Event
  invoke is fail-open and run-size independent) vs. off until #202's
  full-AOI runs record the invoke round trip. Either is a one-line flag;
  espg leaning cautious pending the benchmark-impact numbers.

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
