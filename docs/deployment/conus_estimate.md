# CONUS cost estimate (issue #202, leg 4)

**This is an estimate, not a benchmark result.** We are *not* running CONUS. This
document sizes what a full contiguous-US (lower-48) ATL03 aggregation *would*
cost, from (a) the real CONUS order-9 shard map we can build offline and (b) a
per-shard cost regression fit from measured Lambda data. The shard map, its
summary statistics, and **the fitted cold/warm regressions and dollar totals**
(from a real 25-shard CONUS cold/warm dispatch, §4b) are landed here.

> **The totals are an upper bound on today's codebase.** They include the issue
> #209 t-digest write bloat (write is ~⅓ of a CONUS shard) and the #65 swath
> over-assignment. Both are labelled below; #209 in particular revises the number
> down materially once fixed. Recorded matrix *series* numbers are refreshed only
> after the #209 follow-on lands (see the PR checklist).

Everything here is reproducible offline from the committed artifacts (no AWS, no
network beyond the one-time polygon fetch):

| Artifact | What |
| --- | --- |
| `data/conus/conus.geojson` | the polygon reference the shard map is built over |
| `data/conus/build_conus_polygon.py` | builds `conus.geojson` (provenance below) |
| `data/conus/build_conus_shardmap.py` | builds the o9 shard map + the two artifacts below |
| `data/conus/conus_shard_granule_counts.parquet` | per-shard granule-count table (the load-bearing artifact) |
| `data/conus/conus_shard_stats.json` | summary stats + granule distribution |
| `data/conus/select_regression_shards.py` | stratified <=25-shard regression-training plan |

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
| Total (shard, granule) pairs | **3,560,313** |
| Catalog granules (full ATL03 v007) | 555,867 |
| Survived bbox+temporal prefilter | 28,429 |
| Shard-map build wall | 291 s (mortie MOC order 13) |
| Leak check (mortie #103 guard) | **passed** — all cells in-bbox (lat 25.12–49.42, lon −124.73…−66.94) |

The shard coverage (7.99 M km²) exceeds the polygon area (7.81 M km²) by 2.4%
because o9 shards on the boundary are kept whole (the AOI-overhang effect, issue
#101) — a real cost the estimate must carry, since those edge shards are
dispatched in full.

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

Histogram (granule-count bin → shards):

| Bin | Shards |
| --- | ---: |
| 10–25 | 6 |
| 25–50 | 223 |
| 50–100 | 48,595 |
| 100–144 | 461 |

**The distribution is sharply peaked**: ~99% of CONUS o9 shards carry 50–100
granules (median 70), with a thin high tail to 144 and only a handful below 50.
This is the mid-latitude regime — no polar RGT convergence — so the whole domain
sits in a **narrow 21–144 granule band**. Consequences for the regression:

- A cold pass of the densest shard (144 granules × ~1.7 s/granule ≈ 245 s) fits
  comfortably under the 900 s timeout, so **no CONUS shard is excluded** from the
  regression-training selection (`conus_regression_shards.json`, 0 excluded).
- Because the band is narrow, the regression is an **interpolation** across
  21–144 granules, not an extrapolation — but it is also a *narrow* training
  range. A broader density spread (88°S rings reach ~5,600 granules/shard) would
  be needed before applying the fit to any higher-density domain; for CONUS
  itself the fit covers the full realised range.

## 4. Operational-cost model (column structure)

Per espg, the estimate accounts for cost in **four separate columns**, with
**Lambda GB-second the primary**. The dollar figure is **not one number** — it is
computed for **two scenarios** applied across every CONUS o9 shard:

- **First-run (cold, no sidecars):** the sidecar index is built and written on
  this pass (`data_source.index.on_miss: build`). Reads are uncached (~1.7
  s/granule, the #148 rate) and this pass also incurs the sidecar-write S3 PUTs.
- **Repeat (warm, with sidecars):** the sidecars now exist, so reads hit the
  cache; cheaper compute, no sidecar-write PUTs.

Each scenario gets its **own** regression (granules-per-shard → lambda-seconds),
fit from the measured full-AOI per-shard data, then applied to the CONUS
per-shard granule counts and summed.

| Cost column | What it counts | Cold (first run) | Warm (repeat) |
| --- | --- | --- | --- |
| **Lambda GB-s** (primary) | `Σ lambda_seconds × 4 GB × $0.0000133334/GB-s`, via the per-scenario regression (§4b) | 86.2 M GB-s ≈ **$1,149** | 66.9 M GB-s ≈ **$892** |
| **S3 PUT/GET** | PUTs dominated by the #209 t-digest write (~1,792 objects/shard × 49,285 ≈ **88 M PUTs ≈ $440** cold) + sidecar writes; GETs are granule byte-range reads (NSIDC bucket) | ~**$440** (mostly #209 bloat) | ~$0.3 (no sidecar/CSR re-write; #209 fix cuts cold to ~$0.25 too) |
| **CMR / catalog build** | one-time STAC/geoparquet catalog build (offline/local for CONUS); the run reads the committed catalog | ~$0 (one-time, offline) | ~$0 |
| **CloudWatch / logs** | per-invocation log ingestion + storage (~one stream per shard × 49,285) | ~$1–3 | ~$1–3 |

Compute (Lambda GB-s) is the dominant column; the notable operational cost is the
**cold S3 PUT storm (~$440)**, which is almost entirely the #209 write bloat and
collapses to ~$0.25 once the ragged t-digest writes one object/shard.

### 4b. Regression — measured (25-shard CONUS cold/warm dispatch)

Fit from a **real 25-shard stratified CONUS run** on the production `process-shard`
Lambda (4 GB, inline→sidecar), spanning the full 21–144 granule/shard density band,
each shard run twice: cold (`on_miss: build`, populates sidecars) then warm (reads
them). Sidecar writes were verified in S3 between passes. Raw per-shard points:
`data/conus/results/conus_regression_results.json`.

| pass | fit (granules → lambda-seconds) | R² |
| --- | --- | ---: |
| **cold** (first run, builds sidecars) | `3.14 × granules + 210 s/shard` | 0.75 |
| **warm** (repeat, reads sidecars) | `2.38 × granules + 167 s/shard` | 0.74 |

Applied across the CONUS shape (**49,285 shards, 3,560,313 granule-reads**):

| scenario | lambda-seconds | GB-s | **cost** | wall @ 2,000 workers |
| --- | ---: | ---: | ---: | ---: |
| **First run** (cold) | 21.5 M | 86.2 M | **$1,149** | ~3 h 00 m |
| **Repeat** (warm) | 16.7 M | 66.9 M | **$892** | ~2 h 19 m |

The sidecar cache saves ~$260/run (22%) on every reprocess, for a one-time
~$1,149 first pass. (2,000 concurrent is above the current 1,000 account limit —
assumes a limit increase; at 1,000 the walls are ~6 h / ~4.6 h. Wall is idealized
perfect-packing; the slowest single shard is ~12 min, so it is concurrency-bound.)

**Where the cost is: ~half is per-shard fixed overhead.** The ~200 s/shard
intercept × 49,285 shards is $552 (cold) / $440 (warm) — **48–49% of the total**,
as costly as all the per-granule reading combined. So the dominant cost lever is
**shard count**, not read speed or the cache. (This is why the earlier flat-rate
proxy — 1.7 s/granule + ~5 s/shard — anchored at ~$336: it under-modelled the
per-shard overhead ~40×. The measured intercept is ~200 s, not ~5 s.)

### 4c. Upper-bound caveats and the levers that revise it down

The totals are honest for **today's** codebase but are an upper bound on three axes:

1. **#209 t-digest write bloat (the big one).** Write is ~⅓ of a CONUS shard and
   ~$440 of cold S3 PUTs, because the ragged t-digest writes ~1,792 objects/shard.
   Fixing #209 (one sharded vlen array/shard) cuts the write phase and the PUT
   storm directly — the single largest revision.
2. **Coarser sharding is blocked on #209, not memory.** A NEON o8/o9/o10 order
   sweep (`data/conus/results/order_sweep_*.json`) found o9 ~15% cheaper/shard
   than o10 (fewer shards amortize the intercept), but **o8 times out at 900 s** —
   an o8 shard has 4× the cells → 4× the #209 write bloat. Memory is not the
   limit (o9/o10 peaked ~560–680 MB). So the ~half-the-cost fixed-overhead lever
   (fewer, coarser shards) is real but gated on #209; **#209 first, then re-test o8.**
3. **#65 swath over-assignment.** Granule→shard assignment uses the coarse CMR
   swath polygon, so reads are an upper bound on granules that truly contribute
   photons. Note this is *only* the swath-vs-beams envelope: CONUS is ~98.6%
   fully-covered **interior** shards, where every assigned granule genuinely
   crosses the shard and is correctly read — the AOI-edge over-assignment that
   inflates a tiny box AOI does **not** apply at continental scale.

**No AOI mask.** CONUS is a bulk grid, not a strict-AOI product; the vast majority
of shards are fully covered, so `output.aoi_mask` is off and the estimate carries
no mask compute/write.

The R² ≈ 0.74 scatter reflects that granules-per-shard is a noisy cost predictor —
observation density swings ~10× across shards (surface brightness + crossing
geometry), so treat the totals as an **order-of-magnitude envelope**, not a quote.

## 5. Reproducibility

```bash
# 1. polygon (one-time network fetch of the public source outline)
python data/conus/build_conus_polygon.py
# 2. o9 shard map + stats (needs the local full ATL03 v007 catalog)
python data/conus/build_conus_shardmap.py
# 3. stratified regression-training shard plan (cold/warm passes)
python data/conus/select_regression_shards.py
```

Temporal window `2018-10-13 → 2026-03-15`, catalog
`data/atl03_v007/atl03_v007_full.parquet` (555,867 granules), grid config
`tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml`.
