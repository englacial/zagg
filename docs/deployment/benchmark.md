# Benchmark results

Cost and runtime of the zagg compute pipeline on AWS Lambda, tracked per merge to
`main` (issue #110). Each point dispatches the **single densest shard** over the
NEON SERC AOP box — arm64, 2 GB, one shard per target, capped by the 900 s deploy
timeout (the Lambda ceiling, issue #148) — so deltas track code changes, not data drift.

The charts below are rendered on every merge and published to the `benchmarks`
data branch; they update live (the docs embed them by raw URL, so no docs rebuild
is needed). See [Lambda benchmark CI/CD setup](benchmark-cicd.md) for how the
pipeline is wired.

## Sharded vs inner-chunk (tdigest, HEALPix)

The forward benchmark (issue #133) is a **2×3 matrix** measuring the ShardingCodec
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

**Referencing these numbers programmatically?** Pull the machine-readable
companions instead of scraping the image:
[`metrics.json`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/metrics.json)
(the latest merge's records) or
[`latest.md`](https://raw.githubusercontent.com/englacial/zagg/benchmarks/site/latest.md)
(the same table as markdown). The full retained history is `series.parquet` on the
[`benchmarks` branch](https://github.com/englacial/zagg/tree/benchmarks).

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
