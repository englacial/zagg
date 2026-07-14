# CONUS cost estimate (issue #202, leg 4)

> **⚠️ Under active revision (streaming).** The o7/o8 **"memory-infeasible"**
> conclusion in §4c is being overturned as this is written: those OOMs are an
> artifact of the **pooled** default aggregation (the worker holds the whole
> shard's photons at once), **not** a hard limit. `aggregation.streaming:
> {buffer_granules: N}` bounds peak memory to one buffer + running digests,
> independent of shard size — and the benchmark t-digest fields are streamable.
> A 4 GB streaming re-run of the OOM'd o8 shards + a `buffer_granules` sweep is
> **in flight**; once it lands, §4c and the o8 numbers get rewritten (o8 likely
> becomes feasible, reopening the coarser-is-cheaper comparison). **The order-9
> numbers below stand;** treat the o8/o7 "infeasible" framing as provisional.

**This is an estimate, not a benchmark result.** We are *not* running CONUS. This
document sizes what a full contiguous-US (lower-48) ATL03 aggregation *would*
cost, from (a) the real CONUS order-9 shard map we build offline and (b) per-shard
cost regressions fit from **measured 0.24.0 Lambda data** (25 stratified CONUS
shards, both read modes, §4b). All numbers are at zagg 0.24.0 — the **sharded**
t-digest write (issues #209 / #211), so the pre-#211 write bloat is already gone.

> **Headline: order 9 at 4 GB is the operating point — the only order that runs
> CONUS at all.** The coarser orders that would amortise per-shard overhead are
> **memory-infeasible**: an o8 shard OOMs on ~20 % of the continent at 4 GB (and
> even at 8 GB), and o7 OOMs outright. Per-shard memory is set by **photon
> density, not granule count**, and memory is billed — so coarsening buys nothing
> at continental scale (§4c). The remaining upper-bound lever is the #65 swath
> over-assignment (§4d).

| Order 9 @ 4 GB (measured) | cost (95 % CI) | wall @ 2,000 workers |
| --- | ---: | ---: |
| **First run** (cold, `inline` uncached reads) | **$471** ($406–536, ±14 %) | **~1.2 h** |
| **Repeat** (warm, `sidecar` cached reads) | **$419** ($367–472, ±13 %) | **~1.1 h** |

Everything here is reproducible offline from the committed artifacts (the dollar
totals additionally require the measured regression JSONs under
`data/conus/results/`, from a real billed 25-shard dispatch):

| Artifact | What |
| --- | --- |
| `data/conus/conus.geojson` | the polygon reference the shard map is built over |
| `data/conus/build_conus_polygon.py` | builds `conus.geojson` (provenance below) |
| `data/conus/build_conus_shardmap.py` | builds the o9 (or `--order N`) shard map + artifacts |
| `data/conus/conus_shard_granule_counts.parquet` | per-shard granule-count table (the load-bearing artifact) |
| `data/conus/conus_shard_stats.json` | summary stats + granule distribution |
| `data/conus/select_regression_shards.py` | stratified <=25-shard regression-training plan |
| `data/conus/run_conus_regression.py` | billed cold/warm dispatch driver (`--index-backend inline` / sidecar) |
| `data/conus/estimate_with_ci.py`, `conus_final_estimate.py` | apply the fits to the CONUS distribution with a 95 % interval |

## 1. Polygon reference

- **Region:** CONUS — the 48 contiguous states plus DC (Alaska, Hawaii, Puerto
  Rico excluded).
- **Source:** `us-states.json` from PublicaMundi/MappingAPI (a widely-used,
  Census-derived, simplified US state outline; MIT-licensed).
- **Construction:** `unary_union` of the 48 contiguous states + DC, `buffer(0)`
  to heal inter-state seams. No further simplification (the source is already
  ~800 vertices). See `data/conus/build_conus_polygon.py`.
- **Bounding box (lon/lat):** `[-124.707, 25.121, -66.980, 49.384]`.
- **Parts:** 5 (mainland + coastal-island groups).
- **Area:** **7,805,341 km²** (EPSG:5070 CONUS Albers equal-area).

The bbox edges do not lie on a HEALPix base-cell boundary (lon ≡ 0 mod 45° or
lat 0), and the outline is irregular, so the mortie base-cell polygon-fill bug
(espg/mortie#103, fixed in mortie 0.9.0) is not a concern; the build still runs
a cheap post-build **leak check** asserting every covered shard's cell centre
lies inside the CONUS bbox.

## 2. Summary statistics (order-9 shard map)

- **Grid:** HEALPix nested, `parent_order=9` (shard/dispatch unit), `child_order=19`
  (~10 m leaf cell), mortie MOC intersection.
- **Temporal:** `2018-10-13 → 2026-03-15` (mission launch → last granule in CMR;
  the entire ATL03 v007 collection, 555,867 granules).
- **Catalog prefilter:** the CONUS bbox + temporal cut leaves **28,429** of
  555,867 granules for the exact polygon intersection (bbox column is
  latitude-exact / longitude-conservative, so the cut drops nothing real).

<!-- CONUS_SUMMARY_TABLE -->
| Quantity | Value |
| --- | ---: |
| CONUS area (EPSG:5070) | 7,805,341 km² |
| Total o9 shards | **49,285** |
| One o9 shard area | 162.15 km² |
| Shard coverage area (49,285 × 162.15) | 7,991,345 km² (1.024× polygon — edge-shard overhang) |
| Distinct granules intersecting CONUS | **14,068** |
| Total (shard, granule) pairs (o9 reads) | **3,560,313** |
| Catalog granules (full ATL03 v007) | 555,867 |
| Survived bbox+temporal prefilter | 28,429 |
| Shard-map build wall | 291 s (mortie MOC order 13) |
| Leak check (mortie #103 guard) | **passed** — all cells in-bbox |

The shard coverage (7.99 M km²) exceeds the polygon area (7.81 M km²) by 2.4 %
because o9 shards on the boundary are kept whole (the AOI-overhang effect, issue
#101) — a real cost the estimate carries, since those edge shards dispatch in full.

## 3. Per-shard granule-count distribution

The regression's input variable is granules-per-shard. The full distribution is
in `data/conus/conus_shard_stats.json`; the per-shard table is
`data/conus/conus_shard_granule_counts.parquet`.

<!-- CONUS_DISTRIBUTION_TABLE -->
| Statistic | Granules/shard |
| --- | ---: |
| min | 21 |
| median | 70 |
| mean | 72.24 |
| p90 | 84 |
| p99 | 99 |
| max | 144 |

**The distribution is sharply peaked**: ~99 % of CONUS o9 shards carry 50–100
granules (median 70), a thin tail to 144, a handful below 50 — the mid-latitude
regime (no polar RGT convergence). Consequences for the regression:

- Every CONUS o9 shard runs well under the 900 s timeout, so **no shard is
  excluded** from the regression-training selection (0 excluded).
- The fit is an **interpolation** across the realised 21–144 granule band, not an
  extrapolation. But granule count is a *noisy* predictor (§4b, R² 0.60–0.72):
  per-shard observation density swings ~10× (surface brightness × crossing
  geometry), so the totals carry a **±13–14 % interval**, not a point quote.

## 4. Operational-cost model

Cost is accounted in **four columns** with **Lambda GB-second the primary**, for
**two read scenarios** applied across every CONUS o9 shard (per espg):

- **First run (cold)** — `inline` index backend: reads are genuinely uncached
  (byte-range HDF5 every invoke), **cache-independent**. This is the realistic
  one-shot / first-pass read cost.
- **Repeat (warm)** — `sidecar` index backend: reads hit the prebuilt
  granule-keyed chunk manifests, so a re-run is cheaper. The manifest cache is
  built once (a small one-time write, now that #211 collapses the ragged t-digest
  to one object/shard); every subsequent reprocess is warm.

Each scenario has its **own** measured regression (§4b), applied to the CONUS
per-shard granule counts and summed.

| Cost column | What it counts | First run (cold) | Repeat (warm) |
| --- | --- | ---: | ---: |
| **Lambda GB-s** (primary) | `Σ λ-seconds × 4 GB × $0.0000133334/GB-s`, via the per-scenario regression (§4b) | 35.3 M GB-s ≈ **$471** | 31.4 M GB-s ≈ **$419** |
| **S3 PUT/GET** | output PUTs (now **1 t-digest object/shard** post-#211, no write storm) + one-time sidecar-manifest write on the first sidecar run; GETs are granule byte-range reads (NSIDC bucket) | small one-time (sharded write) | ~$0 (no sidecar/CSR re-write) |
| **CMR / catalog build** | one-time STAC/geoparquet catalog build (offline/local for CONUS) | ~$0 | ~$0 |
| **CloudWatch / logs** | ~one log stream per shard × 49,285 | ~$1–3 | ~$1–3 |

The repeat cache saves **~$52/run (11 %)** on reads. The pre-#211 cold **S3 PUT
storm (~$440, ~1,792 objects/shard)** is **gone** — sharding writes one t-digest
object per shard, so the write phase no longer dominates cold.

### 4a. Wall-clock at scale

Idealised perfect-packing wall = `Σ λ-seconds / N_workers`, floored by the slowest
single shard (~400 s / 7 min for o9 — throughput-bound, not concurrency-bound):

| scenario | Σ λ-seconds | **wall @ 2,000 workers** | wall @ 1,000 workers |
| --- | ---: | ---: | ---: |
| First run (cold) | 8.83 M | **~1.2 h** (74 min) | ~2.5 h |
| Repeat (warm) | 7.86 M | **~1.1 h** (66 min) | ~2.2 h |

**2,000 concurrent is above the current 1,000-per-account Lambda limit** — it
assumes a limit increase; at the default 1,000 the walls are ~2.5 h / ~2.2 h.

### 4b. Regression — measured (25-shard CONUS dispatch, zagg 0.24.0 sharded)

Fit from a **real 25-shard stratified CONUS run** on the production `process-shard`
Lambda (4 GB, arm64), spanning the full 21–144 granule/shard band, in both read
modes. All 25 shards succeeded in both modes (RSS ≤ 2.5 GB — o9 fits 4 GB
cleanly). Raw per-shard points: `data/conus/results/conus_inline_cold_o9.json`
(cold) and `conus_regression_results_o9warm.json` (warm).

| scenario | fit (granules → λ-seconds) | R² | CONUS total | 95 % CI |
| --- | --- | ---: | ---: | ---: |
| **cold** (first run, `inline`) | `1.96 × granules + 38 s/shard` | 0.60 | $471 | $406–536 |
| **warm** (repeat, `sidecar`) | `2.03 × granules + 13 s/shard` | 0.72 | $419 | $367–472 |

*(The warm total reproduces the independently-measured sharded $417 from an
earlier run — cross-checked.)*

**Confidence interval.** Granule count is a noisy cost predictor (R² 0.60–0.72),
so the CONUS total is not a point value. The 95 % interval propagates two sources
in quadrature on `Σ λ-seconds = slope·G_total + intercept·N`:

1. **parameter uncertainty** (OLS covariance of slope/intercept) — *systematic*,
   correlated across all 49,285 shards; this is the **dominant** term and does not
   average out.
2. **per-shard residual scatter** — independent, so its contribution to the total
   grows only as √N and is near-negligible at N ≈ 50 k (a prediction-interval
   component, included for honesty).

See `estimate_with_ci.py` / `conus_final_estimate.py`.

### 4c. Order feasibility — why o9 is the ceiling for coarsening

Coarser shards (fewer of them) would amortise the per-shard intercept — but at
CONUS scale they run out of **memory**, not time. Per-shard peak RSS is driven by
**photon volume (surface density), not granule count** — so it only shows up once
you sample the whole continent, not a single site.

| order | shard area | CONUS shards | 4 GB result | evidence |
| --- | ---: | ---: | --- | --- |
| **o7** | 2,594 km² | — | **OOM outright** | 1/1 NEON shard (181 gran) died ~990 s; 16.7 M cells → est. ~7 GB |
| **o8** | 649 km² | 12,596 | **OOMs ~20 %** | 5/25 CONUS shards OOM at 4 GB; survivors already 3.5 GB (§ below) |
| **o9** | 162 km² | 49,285 | **fits cleanly** | 25/25 CONUS shards, RSS ≤ 2.5 GB |
| **o10** | 41 km² | — | fits | 9/9 NEON shards, ~560–680 MB |

**The o8 memory wall (measured).** A 25-shard stratified CONUS o8 run had **5
shards OOM at 4 GB**, deterministically (the same 5 in both read modes), and the
survivors already peaked at **3.5 GB**. It is **not a leak** and **not granule
count**: an 85-granule shard OOMs while a 211-granule shard runs at 1.6 GB.
Re-running the 5 OOM'd shards at **8 GB** confirmed the memory is genuinely large
and photon-driven:

| granules | runtime | RSS @ 8 GB | result |
| ---: | ---: | ---: | --- |
| 85 | 623 s | 7,207 MB | ✓ |
| 120 | 518 s | 6,486 MB | ✓ |
| 148 | — | — | **still fails** |
| 155 | 857 s | 7,689 MB (94 %) | ✓ (pinned to *both* the 8 GB and 900 s ceilings) |
| 176 | 635 s | 5,795 MB | ✓ |

So **8 GB does not rescue o8**: 4 of 5 clear but at 5.8–7.7 GB, one still fails,
and one pins both ceilings at once. o8 would need the 10 GB Lambda maximum just for
margin — at **2.5× the per-GB-s price**, *and* with a residual failure/retry tail.
There is no memory tier at which coarsening to o8 is cheaper than o9 running
cleanly at 4 GB. (An earlier 2-shard NEON o8 test passed at 1.5–1.8 GB — but two
shards over one uniform forest site did not sample CONUS's photon-density range;
the continental regression is what exposed the tail.)

### 4d. Remaining upper-bound caveat

The #209 write bloat that dominated the old cold estimate is **fixed** (#211,
0.24.0 sharded). The one remaining upper-bound axis is **#65 swath
over-assignment**: granule→shard assignment uses the coarse CMR swath polygon, so
reads are an upper bound on granules that truly contribute photons. This is *only*
the swath-vs-beams envelope: CONUS is ~98.6 % fully-covered **interior** shards,
where every assigned granule genuinely crosses the shard — the AOI-edge
over-assignment that inflates a tiny box AOI does **not** apply at continental
scale. **No AOI mask**: CONUS is a bulk grid, so `output.aoi_mask` is off.

## 5. Reproducibility

```bash
# 1. polygon (one-time network fetch of the public source outline)
python data/conus/build_conus_polygon.py
# 2. o9 shard map + stats (needs the local full ATL03 v007 catalog)
python data/conus/build_conus_shardmap.py
# 3. stratified regression-training shard plan
python data/conus/select_regression_shards.py
# 4. billed cold + warm dispatch (AWS profile 'nasa', account 742127912612):
#    cold = inline uncached reads; warm = sidecar cached reads
AWS_PROFILE=nasa python data/conus/run_conus_regression.py --order 9 \
  --config tests/data/benchmark/configs/atl03_tdigest_healpix_o9_cached.yaml \
  --index-backend inline --cold-only --out data/conus/results/conus_inline_cold_o9.json
AWS_PROFILE=nasa python data/conus/run_conus_regression.py --order 9 \
  --config tests/data/benchmark/configs/atl03_tdigest_healpix_o9_cached.yaml \
  --out data/conus/results/conus_regression_results_o9warm.json
# 5. apply fits to the CONUS distribution with a 95% interval
python data/conus/conus_final_estimate.py
```

Temporal window `2018-10-13 → 2026-03-15`, catalog
`data/atl03_v007/atl03_v007_full.parquet` (555,867 granules), grid config
`tests/data/benchmark/configs/atl03_tdigest_healpix_o9_cached.yaml`.
