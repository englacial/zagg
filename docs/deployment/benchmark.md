# Benchmark results

Cost and runtime of the zagg compute pipeline on AWS Lambda over the NEON SERC AOP
box (issue #110), tracked as **two complementary series** (issue #202):

- **Per merge to `main` — the live single-shard matrix.** Each point dispatches the
  **single densest shard** per target — arm64, **4 GB**, one shard/target, capped by
  the 900 s deploy timeout (the Lambda ceiling, issue #148) — so deltas track *code*
  changes, not data drift. This is the [live matrix](#live-matrix--inlinesidecar--aoi-mask-per-merge) below.
- **Per release — the full-AOI NEON run.** The whole `AOP_NEON` box fanned over
  **every** shard, recorded per release for **dollar-cost truth** (real total across
  all shards). Its harness is a separate deliverable (issue #202 leg 1); its plot is
  a [skeleton](#per-release-full-aoi-neon-issue-202-leg-1) until that schema lands.

The pinned window is the full mission, `2018-10-13 .. 2026-03-15` (the last CMR
granule is `2026-03-11`). The charts below are rendered on merge/release and
published to the `benchmarks` data branch; they update live (the docs embed them by
raw URL, so no docs rebuild is needed). See
[Lambda benchmark CI/CD setup](benchmark-cicd.md) for how the pipeline is wired.

## Live matrix — inline/sidecar × AOI-mask (per merge)

The live per-merge matrix (issue #202 reset) is **tdigest, sharded output,
`granule_workers=4`, at 4 GB, o9 only** — a **2×2** over the read-backend A/B
(`inline` vs `sidecar`) **×** the strict-AOI-mask A/B (`mask` vs `nomask`). Four
targets: `tdigest_healpix_o9_{inline,sidecar}_{mask,nomask}`. The `mask` arm
dispatches the `aoi_mask`-carrying shard map (`healpix_o9_aoimask`), the `nomask`
arm the plain one; the mask is manifest-driven, so the arms differ only by which map
they dispatch (issue #101/#202). The chart columns are the read backend (`inline`
left, `sidecar` right); the rows are the AOI-mask arm (`nomask` top, `mask` bottom).

o10 was retired from this live set (an o9-only reset); its read-backend rows are
frozen under `provisional_targets` (retained, not rerun), and the pre-reset live
datapoints are dropped from the corrected series at the render layer, so the 2×2
starts fresh at the first post-reset merge.

![inline/sidecar × AOI-mask — latest merge](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/matrix_table.png)

### Cost per shard vs runtime (inline/sidecar × AOI-mask)

![Live matrix — cost per shard](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_shard_matrix.png)

### Cost per 100 km² vs runtime (inline/sidecar × AOI-mask)

![Live matrix — cost per 100 km²](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_100km2_matrix.png)

**Referencing these numbers programmatically?** Pull the machine-readable
companions instead of scraping the image:
[`metrics.json`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/metrics.json)
(the latest merge's records) or
[`latest.md`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/latest.md)
(the same table as markdown). The full retained history is `series.parquet` on the
[`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks).

## Per-release full-AOI NEON (issue #202 leg 1)

The complementary **per-release** series: the whole `AOP_NEON` box fanned over
**every** shard (not just the densest one), recorded once per release, for the
real dollar-cost total the single-shard matrix can't show. This is cost *truth*,
where the per-merge matrix is a code-drift *regression* tracker.

> **Skeleton — pending leg 1.** The full-AOI run harness and its pinned targets
> (`targets_full_aoi_neon.json`) are a separate deliverable (issue #202 leg 1). The
> render side is staged: `plot_series.make_full_aoi_release_figure` returns `False`
> (renders nothing) until leg 1's recorded schema is fixed, so the Pages index
> simply omits this section for now. When the schema lands, this section will embed
> the per-release full-AOI cost + wall-time chart (release tag on the x-axis) here.

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

## Archived: sharded vs inner-chunk (tdigest, HEALPix)

**Frozen as of issue #193** (the read-backend A/B superseded it as the live matrix)
and retained through the issue #202 reset — the retained rows + PNGs stay on the
`benchmarks` branch but no new points are appended. The codec matrix (issue #133)
was a **2×3 matrix** measuring the ShardingCodec
([#108](https://github.com/englacial/zagg/issues/108)) head-to-head against regular
inner chunks: all `tdigest` / HEALPix / arrow, across orders **o9 / o10 / o11**.
Each order pins the same densest shard and runs it twice — `sharded` (the codec
bundles a shard's K inner chunks into one Zarr shard object) vs `inner` (K
independent chunk objects) — so the two columns are a clean A/B of the codec's
memory / runtime / cost. o9 (K=256) is the heaviest case and the most interesting
for the codec; the o9 row appears once its shard map lands (its build is pending a
catalog query — see the PR thread).

The table is the latest merge's numbers (the `% cap` cell shaded green→red on the
same scale as the chart markers); the charts below track each cell over merge
history.

![Sharded vs inner-chunk benchmark table](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/codec_table.png)

(The `metrics.json` / `latest.md` companions now track the [live matrix](#live-matrix--inlinesidecar--aoi-mask-per-merge),
not this archived codec matrix; its retained rows live in `series.parquet` on the
[`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks).)

### Cost per shard vs runtime (sharded vs inner)

![Sharded vs inner — cost per shard](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_shard_codec.png)

### Cost per 100 km² vs runtime (sharded vs inner)

![Sharded vs inner — cost per 100 km²](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_100km2_codec.png)

The 2×3 grid lays the two codec columns (`sharded` left, `inner` right) across the
three orders (o9 top → o11 bottom, largest shard first). A blank row is an order
whose shard map hasn't landed yet.

---

## Frozen historical benchmark

The sections below are the **frozen** pre-#133 matrix — the rect / gain_bias
targets, retired from the every-merge run. Their retained rows are kept and
rendered unchanged for historical reference; no new points are appended.

### Latest merge

A snapshot of the most recent merge's per-target numbers — runtime, cost, and peak
memory (the `% cap` cell is shaded green→red on the same scale as the chart
markers). Like the charts, it updates live by raw URL.

![Latest benchmark table](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/latest_table.png)

### Cost per shard vs runtime

![Cost per shard](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_shard.png)

### Cost per 100 km² vs runtime

![Cost per 100 km²](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/cost_per_100km2.png)

---

Per-target panels show **cost** (left axis) and **Lambda runtime** (right axis)
over merge history, for the gain/bias and t-digest aggregators at HEALPix order 11
and 10 (and the matched rectilinear grids). The full retained history lives as
`series.parquet` on the [`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks).

> If the images above are blank, the pipeline hasn't run a merge yet — they
> appear after the first merge to `main` once the
> [setup](benchmark-cicd.md) is complete.
