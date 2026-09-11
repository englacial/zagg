# Pyramid sweep partitioning proposal — batched cascade over the stage transport

Store-track S1 artifact (plan of 2026-09-10, Track C). Scope: materializing the
RULED dense every-order ladder (`[8..0]` above the o9 shard) on the two live
stores — `atl03_tdigest_o9.zarr` and `gedi_flux_o9.zarr` — on **Lambda**, via
the PR #525 stage transport (ruled load-bearing, not fallback), after the
PR #524 column backfill and the `tools/redeclare_dense_ladder.py`
re-declaration. The topology numbers below come from the 2026-09-10 anonymous
live-store audit, and the per-node geometry from `expand_overviews`; the one
unmeasured quantity — bytes per overview node — is labelled as such wherever
it is used, because assuming instead of deriving is what killed the 08-25
attempt (see the forensics section).

Version gate, restated because it is silent when violated: the stores were
written by **zagg 0.52.0** (`composition` classifies `packed`); every leg —
declare, backfill, sweep, and the Lambda layer — must run ≥0.52.0. The tool
refuses a pre-0.52 environment; the worker layer currently stamps
`zagg_version: 0.0.0+unknown` (missing version metadata — small-fix owed) so
the layer's real version must be verified by build provenance, not by the
stamp.

## Audited topology (the partitioning input)

ATL03: 2,918 committed leaves; above-shard node counts

| order | 8 | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0 (base) |
|---|---|---|---|---|---|---|---|---|---|
| nodes | 844 | 272 | 110 | 58 | 36 | 22 | 9 | 6 | 3 |

Total 1,360 dense overview nodes (~31k objects). GEDI: 2,927 leaves, ~1,353
nodes. The runbook derives GEDI's per-order counts the same way at dispatch
time — from the run-record work set / coverage MOC expansion — never from this
table.

**Per-node size is the one quantity below that is not measured** — neither
store has a materialized overview node yet — so it is labelled everywhere it
is used. What *is* derived: `expand_overviews` sets `d = base − shard_order`
(`src/zagg/pyramid.py`, the fixed every-order ladder), so with
`--overviews 13` on the o9 ATL03 store `d = 4` and **every above-shard node
holds `4^d` = 256 cells**, constant depth — not a leaf's `4^(19−9)` ≈ 1.05M
native cells. GEDI's `--overviews 12` gives `d = 3`, i.e. 64 cells per node.

A node's bytes are dominated by its two digest fields (count and the
composition word are a few bytes per cell), and those have a hard ceiling: the
δ=4096 centroid cap at 8 B per centroid (`inner_shape [2]`, float32). So
**≤16 MiB per saturated ATL03 node** (256 × 2 × 4096 × 8 B), i.e. **≤ ~23 GB
per store** across 1,360 nodes. That is arithmetic, not a measurement — real
nodes at the finer orders fold well under δ centroids. The espg-side audit's
**~136 GB** (1,360 × ~100 MB/node) is a *conservative upper bound*, carried
below as such; the first materialized node replaces both numbers with an
observation.

## The schedule: three tuples plus the finisher

`stage_tuples(shard_order=9, tuple_width=3)` produces exactly the ruled
batching — workers fold 3 rungs each, dispatch nodes at orders `0 mod 3`,
finest tuple first.

**Every dispatch-node count in this table is COMPUTED, never pinned** (espg
generality ruling, 2026-09-11): the store's own coverage MOC expanded to the
tuple's dispatch order, intersected with the shard set, is the worker
assignment — `zagg.sweep_fleet.coverage_dispatch_nodes` (from a coverage MOC
the dispatcher already holds; D8 stands) or `dispatch_nodes` (from the work
set) is what the runbook evaluates at dispatch time. The numbers shown are
today's evaluation of ATL03's audited coverage; a different store, or the same
store after more appends, evaluates to different counts through the same
computation.

| tuple | orders folded | dispatch order | ATL03 dispatch nodes (computed — today's evaluation) | reads (one tuple finer) | writes |
|---|---|---|---|---|---|
| T1 | [8, 7, 6] | 6 | **110** | leaf columns (2,918 total; ≤64 per node, avg 26.5) | 1,226 overview nodes + 110 stage columns |
| T2 | [5, 4, 3] | 3 | **22** | o6 stage columns (110; ≤64 per node, avg 5) | 116 overview nodes + 22 stage columns |
| T3 | [2, 1, 0] | 0 | **3** (base cells) | o3 stage columns (22; avg 7.3 per base cell) | 18 overview nodes |
| finisher | — | — | **1** | stage records | root singletons + manifest actuals, lease release |

Each dispatch node owns a **disjoint** subtree that no other node touches, so
the batch a node rides in cannot change the bytes it writes — that is an
argument from the partitioning, and it is what licenses the max-nodes-per-invoke
knob in (c) below, which moves only batch membership. The PR #525 acceptance
tests exactly that axis — `test_identity_survives_a_multi_batch_fan_out`, in
`TestByteIdentityOracle` (`pr525:tests/test_sweep_stage_fleet.py`) — rebuilding
the ladder across several batches and demanding byte-identity with the CLI
build. (The tuple-width oracle `test_byte_identity_across_tuple_widths` is a
*different* axis: how many rungs one worker folds, compared at two widths. It
does not cover this claim.)
Proposed worker counts are therefore **one invoke per dispatch node** — now
the dispatcher's default (`max_nodes_per_invoke=1`, ruled 2026-09-11):
110 → 22 → 3 → 1 per store (~136 invokes/store), every count evaluated from
the coverage at dispatch time as above. GEDI's first level will be of the
same magnitude (its o6-equivalent count, derived at dispatch).

Why one-per-node and not coarser grouping: the per-invoke wall bound is the
fattest dispatch node, and at o6 that is already up to 64 leaf columns
(a full 4^3 subtree). Grouping two o6 nodes per invoke (the "~64 workers"
variant) doubles the worst case to 128 column reads plus ~4 GB of overview
writes — still probably inside 900 s, but with half the headroom for no cost
benefit (Lambda bills GB-seconds; the same work costs the same split finer).
See Q2 below.

**The dispatcher addition — LANDED on this branch** (was: required before the
run). `sweep_fleet.pack_batches` split a tuple's nodes by the 250 KB async
payload cap **only**: the whole ATL03 T1 fan-out — 110 nodes + 2,918 inline
leaf refs — is ~90 KB, so the old packer emitted **one batch = one worker for
the entire first rung**, hours of work against a 900 s wall.
`run_stage_sweep_fleet` / `pack_batches` now take `max_nodes_per_invoke` /
`max_nodes`, **default 1** — one dispatch node per invoke at every tuple, the
ruled T1 fan-out — composing with the payload cap (whichever binds first
closes a batch); `None` restores payload-only packing. Orchestration-only
(changes no bytes, like `tuple_width`): the byte-identity oracle re-runs at
`max_nodes=1` (`test_identity_survives_the_one_node_per_invoke_fan_out`), and
the coverage-computed assignment is pinned against a fixture store's own
`coverage.moc` (`TestCoverageComputedAssignment`, both in
`tests/test_sweep_stage_fleet.py`).

## Wall-time per batch (against the 900 s wall)

Assumptions: 4 GB workers (the benchmark-envelope shape; $ anchors below are
memory-independent since Lambda bills GB-s), leaf columns are the few-MB
`all.pyramid.zarr` per-shard artifacts (orders 9–13 fields), S3 sustained
~50–100 MB/s per worker. Per-node write bytes are bracketed by the two figures
from the topology section — the δ=4096 ceiling (≤16 MiB/node) and the audit's
conservative ~100 MB/node — and every write line below states which it uses.

- **T1** (the binding batch): worst node = 64 leaf columns. Read ≤64 ×
  ~2–10 MB ≈ 0.1–0.6 GB; fold (k-way digest merge + packed composition) over
  21 nodes × 256-cell slabs ≈ 5.4k cells worst case; write ≤21 nodes — that is
  **≤0.35 GB at the ceiling, ≤2.1 GB at the audit bound**. Estimate
  **90–350 s worst-case, ~60–150 s typical**: the read leg binds at the
  ceiling, the write leg at the audit bound, and the range spans both —
  ≥2.5× headroom against the 900 s wall either way. All 110 invokes fire
  concurrently; the tuple completes in one worker-wall.
- **T2 / T3**: reads are stage columns (small); writes are the same ≤21-node
  share (avg 5.3 nodes per T2 worker) on the same two brackets — ≤0.35 GB /
  ≤2.1 GB worst case, well under either at the average. **≤120 s / ≤60 s.**
- **Finisher**: record merge + root singletons, **≤60 s**.
- Barriers: dispatcher soft-barrier per tuple (bounded 2,700 s each, 7,200 s
  total budget — comfortable for 3 tuples whose workers finish in minutes).

**End-to-end ladder sweep: ~20–40 min wall-clock per store.**

The backfill leg is the long pole: anchored on the observed full-store sweep
costs (below), per-leaf work is ~141 s (ATL03) / ~311 s (GEDI) at 4 GB. At
64-way concurrency: ATL03 ≈ **1.8–2 h**, GEDI ≈ **4 h**. Each *leaf* is far
inside the 900 s wall; concurrency only moves wall-clock, not cost.

## Cost estimate (anchored on observed prior sweeps)

Anchors: prior full-store sweeps — the same read-every-leaf-once shape as the
backfill — cost **$27.48 (ATL03)** and **$60.73 (GEDI)**. These are
measurements; the table's compute rows scale from them. Audited read volume is
~300 GB of leaf reads per store; overview write volume is the bracket above
(≤~23 GB at the δ=4096 ceiling, ≤~136 GB at the audit bound).

| leg | ATL03 | GEDI | basis |
|---|---|---|---|
| column backfill | ~$25–30 | ~$55–65 | the prior-sweep anchor is exactly this workload (every leaf read once, digests recomputed); ATL03 skips 204 current columns (−7%) |
| ladder sweep | ~$2–5 | ~$2–5 | ~136 invokes, T1-dominated: 110 × ~150–350 s × 4 GB ≈ $1–2.5 compute; ~31k PUTs ≈ negligible, and insensitive to the write bracket (S3 PUT bills per request, not per byte); margin ×2 |
| re-declaration | ~$0 | ~$0 | one manifest RMW each |
| **one-time total** | **~$30–35** | **~$60–70** | **campaign ≈ $90–105** |
| *storage (recurring)* | *~$0.5–3/mo* | *~$0.5–3/mo* | the campaign's only ongoing cost: ~23–136 GB of **new** overview objects per store at ~$0.023/GB-month (S3 Standard, us-west-2). The bracket collapses once one node is measured |

## Why 2026-08-25 failed, and why this schedule cannot fail the same way

The 1,680 no-op `sweep_stats` records show two mis-sized partitionings of the
**/1 `[7,5,3,1]`** declaration through the `--partitions` morton-subtree
splitter (`sweep_partition.py`), whose contract is: a `4^k` split lands at
order *k*; ladder orders **coarser than k are deferred** to a coarse-level
finisher (the issue #377 deferred phase); partitions are blind order-k grid
cells, not coverage-derived.

- **split_order 5**: the split landed at order 5, but the declared /1 ladder
  was `[7,5,3,1]` — only order 7 sits strictly below the split, so each
  invoke's in-scope share was a single rung and orders **5, 3 and 1 were
  deferred** to a coarse-level finisher that never ran. The invokes were not
  empty-partition invokes: `partition_leaves` returns "``{partition index:
  work set}`` for the NON-EMPTY partitions only"
  (`src/zagg/sweep_partition.py:113-136`), and both fan-outs built on it fire
  one invoke per *occupied* prefix (`runner._invoke_lambda_sweep`;
  `sweep_partitions`). What the 1,680 no-op `sweep_stats` records therefore
  show is invokes whose declared rungs fell outside their partition's scope,
  plus the one that matched and failed — not work scattered into empty cells.
  (The records do not name the dispatch path; a `discover: true` fan-out
  enumerating all `4^k` indices and filtering worker-side via
  `select_partition` would also produce no-ops at this count. Either way the
  mechanism is scope-vs-declaration, not geometry-vs-coverage.)
- **split_order 8**: every declared order (7, 5, 3, 1) is coarser than 8, so
  **the entire ladder was deferred** to the finisher — which never ran.

Both failures are the same root cause: a partition geometry chosen a priori,
**disconnected from the declared ladder** — the split order decided which rungs
each invoke could reach, and the declaration had no say in it. The staged /2
schedule removes the mechanism itself:

1. **Dispatch order and ladder scope are chosen together** — the dispatcher's
   nodes come from the run-record work set (the same discovery the audit used
   to count 844/272/110/…) *at the tuple's own dispatch order*, so each invoke
   names existing nodes and owns exactly the rungs of its tuple. Coverage-
   derived dispatch is not itself new — `partition_leaves` already fires only
   occupied partitions — what is new is that no split order is picked
   independently of the rungs it has to cover.
2. **No coarse-order deferral exists** — every rung belongs to exactly one
   tuple and every tuple is dispatched by the same driver in sequence; the
   designated finisher (implemented and dispatched last in PR #525) handles
   only root singletons and the manifest actuals, never whole rungs.
3. **Lost work is loud** — each invoke must land its named stage record; a
   missing record fails the soft barrier by name instead of silently
   deferring, and `nodes` are validated to sit exactly at the dispatch order
   so no invoke can widen or shrink its share.

## Backfill scoping (PR #524 path vs the audited populations)

- **ATL03, ~2,713 count-only columns → 4-field 0.52 shape: covered.** After
  the /2 re-declaration, `column_is_current`'s terms 2–3 (declaration +
  realized-structure match against the template) read a count-only column as
  stale — its per-field provenance and array set lack the two digests and the
  composition word — so it rewrites; the ~204 shards already carrying 0.52
  4-field columns pass all five terms and **skip idempotently**, provided the
  re-declared block matches what 0.52 build-time wrote (same field set,
  deltas, companions, `overviews: [13]` — i.e. the original δ=4096 config;
  see gap (a)). If the re-declared block differs at all, all 2,918 rewrite —
  correct but forfeits the 7% skip.
- **GEDI, 2,927 columns from scratch: covered.** Term 1 (no column commit
  stamp) fails for every leaf → written from scratch. The declaration gate
  requires the /2 re-declaration first (an `orders: []` manifest refuses by
  name), which is exactly the runbook order.

**Gaps (noted, not fixed here):**

- (a) **The original build configs are not in the repo** — a **step 1**
  blocker only. The store manifests pin δ=4096 (a parent_order-9 config
  variant); the repo carries δ=8192, and the semantic guard refuses any config
  that did not build the store, so espg must supply the original ATL03 and
  GEDI configs for the **re-declaration**. (`output.*` edits — the pyramid
  knob — do not move the hash.) The **backfill takes no config at all**:
  `backfill_columns(store_root, manifest, by_shard, ...)` derives its plan
  from the manifest (`plan = manifest_column_plan(manifest)`, then
  `column_structure(plan.fields, ...)`) and consults no semantic hash, and the
  `columns` family hook passes the same three arguments. That is precisely
  *why* getting step 1's block exactly right matters: from there on the
  declaration **is** the config, and the backfill's skip-if-current gate
  compares realized structure against whatever that block says (the ATL03
  bullet above).
- (b) **Fleet-scale backfill concurrency needs one hop of `run_id` plumbing,
  and then a release/heartbeat ruling** (PR #524, question 3). The admission
  pattern itself already exists: `acquire_lease` re-admits a same-run sibling
  idempotently —

  ```python
  existing = read_lease(store_root, store_kwargs=store_kwargs)
  if existing is not None and existing.get("run_id") == str(run_id):
      return existing  # already ours (an idempotent re-admission)
  ```

  (`src/zagg/sweep_lease.py:135-137`) — and `backfill_columns` exposes both
  levers (`run_id: str | None = None`, `lease: bool = True`, the latter
  documented for callers already holding the lease). What is missing is that
  no dispatcher-chosen `run_id` can *reach* them: the handler forwards
  `families` and `partition` only on `mode="sweep"`
  (`pr524:deployment/aws/lambda_handler.py`, the `run_sweep(...)` call), and
  `ColumnFamily.sweep_store(store_root, manifest, by_shard, store_kwargs,
  min_order)` has nowhere to put `run_id`/`lease`. So each partition invoke
  mints its own `f"backfill-{uuid4}"` and the store-granular lease then
  refuses the siblings — "exactly one is admitted" is a plumbing artifact, not
  a design property. Thread one `run_id` through and they are all admitted;
  the genuine open question is **who releases**, since `release_lease` honours
  any holder of the matching `run_id` and `backfill_columns` releases in a
  `finally` — the first sibling to finish would drop the lease out from under
  the rest, and every sibling heartbeats the same object. A single
  unpartitioned invoke cannot do the backfill either (2,918 leaves × ~141 s ≫
  900 s), and a local/EC2 in-process run is not a sanctioned write path on the
  published stores (the bucket policy names the fleet role, #495/#496) — so
  both the plumbing and the ruling are **hard prerequisites** for the backfill
  leg. See Q3.
- (c) **CLOSED — the stage dispatcher's worker-count knob landed on this
  branch** (`max_nodes_per_invoke`, default one node per invoke, coverage-
  computed assignment; see the schedule section).
- (d) GEDI's families rollups are incomplete (moc 249/353, submap 244/353) —
  separate from the pyramid, could ride the same fleet window as a cheap
  families pass, or wait.
- (e) The 1,680 no-op `sweep_stats` records at the ATL03 root are regenerable
  debris; deleting them is optional and espg's call.

## Runbook order (per store; ATL03 first, GEDI second)

1. `tools/redeclare_dense_ladder.py <root> --config <original>.yaml
   --overviews 13|12` — dry-run, review the diff, then espg re-runs with
   `--execute`.
2. Column backfill (`--families columns`) over the fleet, once (b) is ruled;
   gate step 3 on `failed: 0`.
3. Staged fleet sweep (`run_stage_sweep_fleet`, tuple_width 3) at the default
   one node per invoke: 110 → 22 → 3 → finisher on today's evaluation of
   ATL03's coverage (computed at dispatch, per the schedule section).
4. zagg#434 E2E acceptance against the swept store before any LoD claim (S3).

## Questions for espg (blocking the final runbook)

> **Update 2026-09-11 — all three RULED** (plus the `/2` grammar), in
> [the issue #547 rulings comment](https://github.com/englacial/zagg/issues/547#issuecomment-5641047734):
> (1) batches `[8,7,6]/[5,4,3]/[2,1,0]`, preceded by a one-node T1 canary;
> (2) one dispatch node per invoke at T1, the count computed from the store's
> own coverage (never hardcoded — the knob and computation landed on this
> branch, see the schedule section); (3) strictly serial declare → backfill →
> coverage/rollup refresh → ladder sweep, interleaving rejected and the
> lease/heartbeat ruling thereby avoided. The questions below stand as asked,
> for the record.

1. **Batch boundaries** — confirm `tuple_width 3` ⇒ `[8,7,6] / [5,4,3] /
   [2,1,0]` (dispatch nodes at o6/o3/base, matching the table above), or
   prefer `[8,7] / [6,5,4] / [3,2,1,0]` (dispatch at o7 = 272 first-level
   workers, ≤16 leaf columns each — smaller per-worker fan-in, more invokes)?
   The bytes are identical either way; only wall-headroom and invoke count
   move.
2. **First-level worker count** — one invoke per dispatch node (110 at o6;
   worst case 64 leaf columns, est ≤350 s) as proposed, or your ~64 (two o6
   nodes per invoke; worst case ~128 columns + ~4 GB writes, thinner
   headroom)? And sign-off on adding the max-nodes-per-invoke knob to
   `sweep_fleet` (without it the packer emits one worker per tuple).
3. **Backfill sequencing + lease ownership among siblings** — backfill fully
   before the ladder sweep (recommended: T1's fold gate needs 4-field columns
   under every dispatch node; interleaving per-subtree buys ~nothing at these
   costs), and — the hard one — rule how a same-`run_id` sibling set shares
   one store lease. Admission is not the question (`acquire_lease` already
   re-admits same-run siblings; only the `run_id` plumbing in gap (b) is
   missing, which is a small mechanical change). **Release and heartbeat
   are**: with `backfill_columns` releasing in a `finally`, the first sibling
   to finish drops the lease while the others are still writing. Options:
   (1) only the dispatcher acquires and releases, workers run `lease=False`;
   (2) the workers refcount — last one out releases; or (3) keep
   per-worker release and accept that a late sibling runs unleased. (1) looks
   right (it matches the stage transport's finisher-releases shape) but it
   puts lease liveness on the dispatcher for the full multi-hour backfill —
   your call.
