# Pyramid sweep partitioning proposal — batched cascade over the stage transport

Store-track S1 artifact (plan of 2026-09-10, Track C). Scope: materializing the
RULED dense every-order ladder (`[8..0]` above the o9 shard) on the two live
stores — `atl03_tdigest_o9.zarr` and `gedi_flux_o9.zarr` — on **Lambda**, via
the PR #525 stage transport (ruled load-bearing, not fallback), after the
PR #524 column backfill and the `tools/redeclare_dense_ladder.py`
re-declaration. All numbers below come from the 2026-09-10 anonymous
live-store audit; nothing is guessed (that is what killed the 08-25 attempt —
see the forensics section).

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

Total 1,360 dense overview nodes (~31k objects, ~136 GB written; each node is
constant-depth, same shape class as a leaf, ~100 MB). GEDI: 2,927 leaves,
~1,353 nodes, same shape. The runbook derives GEDI's per-order counts the same
way at dispatch time — from the run-record work set / coverage MOC expansion —
never from this table.

## The schedule: three tuples plus the finisher

`stage_tuples(shard_order=9, tuple_width=3)` produces exactly the ruled
batching — workers fold 3 rungs each, dispatch nodes at orders `0 mod 3`,
finest tuple first:

| tuple | orders folded | dispatch order | ATL03 dispatch nodes | reads (one tuple finer) | writes |
|---|---|---|---|---|---|
| T1 | [8, 7, 6] | 6 | **110** | leaf columns (2,918 total; ≤64 per node, avg 26.5) | 1,226 overview nodes + 110 stage columns |
| T2 | [5, 4, 3] | 3 | **22** | o6 stage columns (110; ≤64 per node, avg 5) | 116 overview nodes + 22 stage columns |
| T3 | [2, 1, 0] | 0 | **3** (base cells) | o3 stage columns (22; avg 7.3 per base cell) | 18 overview nodes |
| finisher | — | — | **1** | stage records | root singletons + manifest actuals, lease release |

Each dispatch node owns a disjoint subtree, so splitting a tuple across
invokes changes no bytes (the PR #525 byte-identity oracle covers widths 1–3).
Proposed worker counts are therefore **one invoke per dispatch node**:
110 → 22 → 3 → 1 per store (~136 invokes/store). GEDI's first level will be
of the same magnitude (its o6-equivalent count, derived at dispatch).

Why one-per-node and not coarser grouping: the per-invoke wall bound is the
fattest dispatch node, and at o6 that is already up to 64 leaf columns
(a full 4^3 subtree). Grouping two o6 nodes per invoke (the "~64 workers"
variant) doubles the worst case to 128 column reads plus ~4 GB of overview
writes — still probably inside 900 s, but with half the headroom for no cost
benefit (Lambda bills GB-seconds; the same work costs the same split finer).
See Q2 below.

**A required dispatcher addition before the run**: `sweep_fleet.pack_batches`
(PR #525) splits a tuple's nodes by the 250 KB async payload cap **only**.
The whole ATL03 T1 fan-out — 110 nodes + 2,918 inline leaf refs — is ~90 KB,
so today's packer emits **one batch = one worker for the entire first rung**,
which is hours of work against a 900 s wall. The runbook needs a
max-nodes-per-invoke (equivalently target-worker-count) knob on
`run_stage_sweep_fleet` / `pack_batches`. Small, orchestration-only (changes
no bytes, like `tuple_width`); proposed as a follow-up commit on the #525
branch or the runbook branch. Without it the fleet path cannot express this
schedule at all.

## Wall-time per batch (against the 900 s wall)

Assumptions: 4 GB workers (the benchmark-envelope shape; $ anchors below are
memory-independent since Lambda bills GB-s), leaf columns are the few-MB
`all.pyramid.zarr` per-shard artifacts (orders 9–13 fields), overview nodes
~100 MB each, S3 sustained ~50–100 MB/s per worker.

- **T1** (the binding batch): worst node = 64 leaf columns. Read ≤64 ×
  ~2–10 MB ≈ 0.1–0.6 GB; fold (k-way digest merge + packed composition over
  ~1M-cell slabs × 21 nodes worst case); write ≤21 nodes × ~100 MB ≈ 2.1 GB.
  Estimate **90–350 s worst-case, ~60–150 s typical** — ≥2.5× headroom.
  All 110 invokes fire concurrently; the tuple completes in one worker-wall.
- **T2 / T3**: reads are stage columns (small), writes ≤ a few hundred MB.
  **≤120 s / ≤60 s.**
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
backfill — cost **$27.48 (ATL03)** and **$60.73 (GEDI)**; audit volumes are
~300 GB leaf reads and ~136 GB overview writes per store.

| leg | ATL03 | GEDI | basis |
|---|---|---|---|
| column backfill | ~$25–30 | ~$55–65 | the prior-sweep anchor is exactly this workload (every leaf read once, digests recomputed); ATL03 skips 204 current columns (−7%) |
| ladder sweep | ~$2–5 | ~$2–5 | ~136 invokes, T1-dominated: 110 × ~150–350 s × 4 GB ≈ $1–2.5 compute; 136 GB PUT + ~31k requests ≈ negligible; margin ×2 |
| re-declaration | ~$0 | ~$0 | one manifest RMW each |
| **total** | **~$30–35** | **~$60–70** | **campaign ≈ $90–105** |

## Why 2026-08-25 failed, and why this schedule cannot fail the same way

The 1,680 no-op `sweep_stats` records show two mis-sized partitionings of the
**/1 `[7,5,3,1]`** declaration through the `--partitions` morton-subtree
splitter (`sweep_partition.py`), whose contract is: a `4^k` split lands at
order *k*; ladder orders **coarser than k are deferred** to a coarse-level
finisher (the issue #377 deferred phase); partitions are blind order-k grid
cells, not coverage-derived.

- **split_order 5**: 1,024 blind partitions over a store whose coverage is a
  sparse Antarctic ring — nearly every partition owned no leaves and matched
  nothing; orders 5, 3, 1 were deferred regardless; the one partition that
  matched tried and failed. Work scattered into empty partitions.
- **split_order 8**: every declared order (7, 5, 3, 1) is coarser than 8, so
  **the entire ladder was deferred** to the finisher — which never ran.

Both failures are the same root cause: partition geometry chosen a priori,
disconnected from both the declared ladder and the store's actual coverage.
The staged /2 schedule removes the mechanism itself:

1. **Dispatch sets are derived, not guessed** — the dispatcher's nodes come
   from the run-record work set (the same discovery the audit used to count
   844/272/110/…), so every invoke names nodes that exist and owns leaves.
   Empty partitions cannot occur.
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

- (a) **The original build configs are not in the repo.** The store manifests
  pin δ=4096 (a parent_order-9 config variant); the repo carries δ=8192. The
  semantic guard refuses any config that did not build the store, so espg
  must supply the original ATL03 and GEDI configs for both the re-declaration
  and the backfill. (`output.*` edits — the pyramid knob — do not move the
  hash.)
- (b) **Fleet-scale backfill concurrency is gated on the lease-vs-partition
  ruling** (PR #524, question 3). #528 landed the handler forwarding
  (`families`/`partition` reach workers), but the runner fires partitions as
  concurrent Event invokes while the sweep lease is store-granular — exactly
  one is admitted. A single unpartitioned invoke cannot do the backfill
  either (2,918 leaves × ~141 s ≫ 900 s), and a local/EC2 in-process run is
  not a sanctioned write path on the published stores (the bucket policy
  names the fleet role, #495/#496). So the ruling is a **hard prerequisite**
  for the backfill leg — see Q3.
- (c) **The stage dispatcher needs the worker-count knob** (payload-only
  packing → one worker per tuple today; see the schedule section).
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
3. Staged fleet sweep (`run_stage_sweep_fleet`, tuple_width 3) with the
   worker-count knob from (c): 110 → 22 → 3 → finisher.
4. zagg#434 E2E acceptance against the swept store before any LoD claim (S3).

## Questions for espg (blocking the final runbook)

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
3. **Backfill sequencing + lease** — backfill fully before the ladder sweep
   (recommended: T1's fold gate needs 4-field columns under every dispatch
   node; interleaving per-subtree buys ~nothing at these costs), and — the
   hard one — rule PR #524's lease-vs-partition question so the Lambda
   backfill can run concurrently at all: scoped per-partition lease, or a
   partition-aware admission for same-run siblings (the stage transport's
   own idempotent re-admission pattern)?
