# Benchmark results

Cost and runtime of the zagg compute pipeline on AWS Lambda over the NEON SERC
AOP box, tracked as **two cadences** (issue #250 restructure, espg-approved on
PR #256):

- **Per release — full-AOI truth.** Every shard over the `AOP_NEON` box, once
  per version tag: the **point pipeline** (ATL03 t-digest) and the **raster
  pipeline** (Sentinel-2, year 2025).
- **Per commit to `main` — regression tracking.** The **single densest shard**
  of one pinned configuration, so deltas track *code* changes, not data drift.

All runs: arm64, **4 GB** worker, **o9** dispatch shards, tdigest (point
pipeline), `granule_workers=4`. Costs are reported for the full run — **no
per-100 km² normalization** (dropped from all rendered outputs; the
`cost_per_100km2_usd` column stays in the parquet history). Charts render on
merge/release to the `benchmarks` data branch and embed here by raw URL (no
docs rebuild). See [Lambda benchmark CI/CD setup](benchmark-cicd.md) for the
wiring.

## Per-release benchmarks (full-AOI NEON)

Two release legs (`lambda-benchmark-fullaoi.yml`, tag-triggered):

- **Point pipeline** (`targets_full_aoi_neon.json`, one target) — o9, **hive**
  store layout, sharded, tdigest, inline read, **with** the strict AOI binary
  mask. Dispatched with `profile=True`, so the worker phase split lands in
  `full_aoi_series.parquet`.
- **Raster pipeline** (`targets_raster_neon.json`, one target) — Sentinel-2
  Collection-1 L2A over 2025 (the pinned Earth Search catalog
  `cat_s2_neon_2025.parquet`, 85 items — offline, fixed across releases), o9
  shards, pull-NN `(time, cells)` ingest. The harness
  (`run_raster_benchmark.py`) dispatches through `agg(profile=True)` — the
  runner's raster path threads the profile key to the workers and rolls their
  stage timings, billed durations and peak RSS into the summary — and retains
  its own `raster_series.parquet`. The dispatch is invoke-only end to end
  (issue #264): after the issue #252 preflight ping, the `(time, cells)`
  template is written by a synchronous `mode="setup"` Lambda invoke before
  fan-out — recorded as `template_s`, the raster analogue of the point path's
  `setup_s` — so the leg runs cleanly under the `zagg-benchmark` OIDC role's
  no-S3 grant (the "template write happens inside the Lambda" invariant in
  [Lambda benchmark CI/CD setup](benchmark-cicd.md) now holds for both legs).
  Two deviations from the target end-state:
  **the live target still pins `store_layout: flat`** — raster + hive is now
  legal and is the defaulted profile (issues #247/#253: an unpinned healpix
  raster config writes hive leaves), so the manifest's `pending_targets` hive
  variant is unblocked and awaits promotion as a deliberate benchmark-manifest
  change — and **no strict AOI mask** (issue #101 is point-path-only); the
  AOI scoping is the catalog's STAC-query clip.

### Summary — total billed cost and wall time

One row per pipeline: **total billed cost** on a dual axis — billed
lambda-seconds (left) and USD (right, an *exact* relabeling at the fixed 4 GB
price) — and overall wall, against the release tag. Markers on both rows carry
the peak-RSS colour scale (green→red % of the 4 GB cap, MB twin axis): the
raster worker reports the same sampled per-invocation peak as the point path
(issue #141 convention; raster parity added by issue #250).

The point pipeline's dollar figure is the **summed total**:
`cost_usd + setup_cost_usd + finalize_cost_usd`. The sync setup and finalize
invokes are billed but excluded from the worker-GB-seconds `cost_usd`, so the
sum is the honest whole-run figure; the three stay separate columns in the
series so the retained `cost_usd` history remains comparable (see
"Cost columns" below).

![Per-release summary](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/full_aoi_summary.png)

### Diagnostics — point pipeline per-phase seconds

One panel per phase, seconds (straggler **max across shards**, matching the
wall framing) vs release tag — never stacked:

- **read / agg / write** — the worker split emitted under `profile=True`
  (`worker_phase_max`). The displayed **agg = index + aggregate** (the
  espg-approved mapping); the series retains the raw emitted phases
  (`phase_index_s`, `phase_aggregate_s`, plus the PR #256 `write` split) so
  the display mapping can change without rewriting history.
- **setup** — the sync setup path. On flat it was the fullsphere-template
  invoke (~104–110 s, the `docs/design/sparse_coverage.md` §1 waste); on hive
  (the issue #252 hybrid) it is only the preflight ping + ~10 ms async Event
  dispatch of the manifest write, so the flat→hive migration reads as a
  visible collapse of this panel.
- **finalize** — kept per issue #252: ~0 on flat, but on hive the load-bearing
  idempotent `morton_hive.json` manifest backstop (the async init write runs
  with retries 0; finalize self-heals a lost manifest).

Phase cells are null on releases recorded before a capture landed, so panels
simply start at the first release that recorded them.

![Point per-phase diagnostics](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/full_aoi_point_phases.png)

### Diagnostics — raster pipeline per-stage seconds

The issue #249 stage set — `open` (store lookup + TIFF headers), `geometry`
(pull-NN mapping; geom-cache hits ≈ 0), `fetch` (tile GETs), `decode`,
`gather` (tile-index derivation + scatter/gather) — plus the worker `write`
bucket, each as its own panel (max shard) vs release tag.

**Stage seconds are work volume, not a wall decomposition:** concurrent
asset-samples overlap on one event loop, so stage sums can exceed wall. The
panels therefore show each stage as its own series and are never stacked to a
wall total. The series also records run-total work counts
(`count_assets` / `count_tiles` / `count_geom_hits`).

![Raster per-stage diagnostics](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/full_aoi_raster_phases.png)

### Store object count (issue #240 tripwire)

The output store's object total vs the config-derived expectation, per
release. A sharded-write bypass multiplies the count ~K-fold (the issue #215
blow-up), so a write-path regression reads as a step here. **Record-only** on
the release leg (a flaky release is never blocked on it); the per-merge
harness *hard-fails* on the same mismatch.

What the audited total does **not** include (issue #362): the second-pass sweep
caches (rollups, overview zarrs) and per-run root telemetry — the run stats
parquet and sweep run record, whose names are unique per run. Those are tallied
in their own buckets and reported (`objects_telemetry` on the record), never
audited; the store path is scoped by commit so the write-path objects a run
counts are only its own.

![Store objects](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/full_aoi_objects.png)

### Cost columns

`cost_usd` is the workers' billed GB-seconds — its semantics never change, so
the retained history stays comparable. The sync orchestrator invokes get their
own columns at the same fixed price: **`setup_cost_usd`** (`setup_s × 4 GB ×
$/GB-s`; on hive rows `setup_s` is only the ping + async-dispatch residue —
the fire-and-forget manifest write's billed GB-s is unobservable from the
orchestrator and is never invented) and **`finalize_cost_usd`** (same pattern
for the finalize invoke). The summary chart displays the sum; the parquet
keeps the parts.

## Per commit to `main`

Single densest shard from the NEON AOI, **one configuration only** (issue
#250 collapse): o9, **hive**, sharded, tdigest, inline read, **no** AOI mask,
`granule_workers=4`, 4 GB — the former inline/sidecar × AOI-mask 2×2 is
retired to the [archived section](#archived). The live target doubles as the
write-path regression arm: the object-count tripwire **hard-fails** the merge
run on a sharded-write bypass. Runs with `profile=True`, so the same
read/agg/write phase split lands in `series.parquet`.

### Latest merge

![Latest merge table](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/merge_table.png)

**Referencing these numbers programmatically?** Pull the machine-readable
companions instead of scraping the image:
[`metrics.json`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/metrics.json)
(the latest merge's records) or
[`latest.md`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/latest.md)
(the same table as markdown). The full retained history is `series.parquet`
(per-merge), `full_aoi_series.parquet` (per-release point) and
`raster_series.parquet` (per-release raster) on the
[`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks).

### Summary — total billed cost and wall time

Same dual-axis convention as the per-release summary (billed lambda-seconds ⇔
USD, exact relabeling; memory-coloured markers), merge sha on the x-axis. The
total derives the sync-invoke dollars from `setup_s`/`finalize_s` at the fixed
price and adds them to `cost_per_shard_usd`.

![Per-merge summary](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/merge_summary.png)

### Diagnostics — per-phase seconds

The same read / agg / write / setup / finalize panels as the per-release
point diagnostics, against merge history.

![Per-merge diagnostics](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/merge_phases.png)

> Images appear after the first post-restructure merge/release renders to the
> `benchmarks` branch; until then GitHub shows a broken-image placeholder.

### Container regime

Benchmark points run with the default `force_cold=False`, so they measure the
**warm regime** — the same reused containers a routine fleet sees. Since issue
#171 each run's summary carries container telemetry (`worker_cold_starts` /
`worker_warm_starts` / `worker_rss_start_max_by_gen`, rolled up from the
per-worker `container_cold` / `container_generation` / `rss_start_mb` envelope
fields), so a memory outlier can be stratified by whether its shard landed on a
fresh or a reused (higher-generation) sandbox. `agg(..., force_cold=True)`
remains the explicit all-cold certification baseline (it needs
`lambda:UpdateFunctionConfiguration` on the caller — see
[Warm-container memory and self-recycle](lambda.md#warm-container-memory-and-self-recycle)).

## On demand — the 88°S latitude sweep (issue #148)

Neither cadence above covers the polar worst case, and it is not meant to: the
88°S ring is where all ~1,387 ICESat-2 RGTs converge, its densest o9 shard holds
~84× the granules of the NEON one, and a timeout there is a result rather than a
regression. It runs **on demand** instead, off its own manifest
(`tests/data/benchmark/targets_88s_latitude_sweep.json`) by explicit
`--target` — never on merge, never on release, so a red stress run cannot fail
either series.

The matrix is **7 latitude bands × 2 worker arms**: one o9 shard per 0.3° band at
85 / 85.5 / 86 / 86.5 / 87 / 87.5 / 88 °S, each on a 4 GB-disk and an 8 GB-disk
worker (`process-shard-{4096,8192}-disk`). Every arm is the shipped per-merge
sidecar configuration — hive, sidecar read, spill streaming, sharded, no AOI
mask, `granule_workers=4` — so **latitude and worker size are the only variables**
and the numbers are directly comparable to the per-merge series.

What it answers is where the tractable band ends. On zagg 0.36.0 the crossover
was **85°S at 8 GB** (865 s, 190 M obs, 2,221 MB peak of 8,192 — memory is not
the wall, spill holds it), with every band poleward timing out and the winning
run splitting read 671 s / aggregate 194 s / write 10 s. That makes the pole
**read-bound**, which is why the next lever is granule concurrency (issue #290)
rather than memory. Those figures are a property of that code point, not of the
bands: the same sweep on 0.35.0 timed out everywhere, and the store-cache fix
(issue #287 / PR #288) is what moved it.

Unlike every series above, these figures are **not** derived from a rendered
artifact — there is no recorded series for an on-demand sweep, so they are
transcribed. The manifest's own `description` is the record of truth and the
thing to update when the sweep is next run; treat this section as a copy of it.

Only the 88°S row is pinned today — it reuses the committed stress map and pin
verbatim. The six equatorward bands need catalog snapshots they do not have yet;
their rows sit in `pending_targets` with the pinning recipe beside them. See
[the benchmark matrix README](https://github.com/englacial/zagg/blob/main/tests/data/benchmark/README.md)
for the band AOIs, the generator, and that recipe.

## Archived

Retired series and figures are retained on the
[`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks) (and
embedded at the bottom of the published Pages index) but no longer
regenerated:

- **inline/sidecar × AOI-mask 2×2** (issues #193/#202, retired by the issue
  #250 collapse) — its four per-merge targets stay runnable on demand via
  `/benchmark --target` from `provisional_targets`; the retired full-AOI 2×2
  and parity arms' rows remain in `full_aoi_series.parquet`.
- **Cost per 100 km² figures** (per-area normalization dropped, issue #250
  item 7).
- **Sharded vs inner-chunk codec matrix** (issue #133, frozen as of #193) and
  the **pre-#133 rect/gain_bias historical matrix** — the pre-existing
  archived tiers, unchanged.
