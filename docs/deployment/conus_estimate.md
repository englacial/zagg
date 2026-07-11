# CONUS cost estimate (issue #202, leg 4)

**This is an estimate, not a benchmark result.** We are *not* running CONUS. This
document sizes what a full contiguous-US (lower-48) ATL03 aggregation *would*
cost, from (a) the real CONUS order-9 shard map we can build offline and (b) a
per-shard cost regression fit from measured full-AOI Lambda data. The shard map,
its summary statistics, and the operational-cost column structure are landed
here now; the fitted regression and the dollar total are left as explicit
placeholders until the measured per-shard data exists (leg 1 / leg 4b).

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
| **Lambda GB-s** (primary) | `Σ lambda_seconds × 4 GB × $0.0000133334/GB-s` over all shards, via the per-scenario regression | _pending regression_ | _pending regression_ |
| **S3 PUT/GET** | GETs: granule byte-range reads + (warm) sidecar reads. PUTs: zarr leaf writes for every shard + (cold) sidecar-manifest writes — so **cold PUT volume is higher** | _pending_ | _pending_ |
| **CMR / catalog build** | one-time STAC/geoparquet catalog build for the CONUS+temporal query (amortized across the run) | _pending_ | _pending_ |
| **CloudWatch / logs** | per-invocation log ingestion + storage (~one log stream per shard) | _pending_ | _pending_ |

### 4b. Regression — PENDING measured per-shard data (leg 1 / leg 4b)

> **Blocked on measured full-AOI per-shard data.** The cold and warm regressions
> require per-shard `(granules → lambda-seconds, cost, RSS)` points across a real
> density range. The 4-shard NEON full-AOI run (leg 1) spans too narrow a density
> band to fit a curve; the CONUS 25-shard stratified cold/warm run (leg 4b) is the
> intended training set — see `data/conus/select_regression_shards.py` for the
> stratified plan and its a-priori cold cost guard. Until those points are
> recorded, the two totals below stay unfilled.

- **Cold total lambda-seconds / cost:** _pending cold regression._
- **Warm total lambda-seconds / cost:** _pending warm regression._
- **Provisional cold proxy (not a fit, upper bound):** applying the flat #148
  uncached rate (1.7 s/granule + ~5 s/shard overhead) to the CONUS totals
  (3,560,313 pairs, 49,285 shards) gives a cold **ceiling** of ≈ **6.30 M
  lambda-seconds ≈ 25.2 M GB-s ≈ $336** (single cold pass, 4 GB workers,
  $0.0000133334/GB-s). This is an order-of-magnitude anchor only — the flat rate
  ignores per-shard fixed overhead amortization and warm caching. Replace with
  the fitted cold/warm regressions when the measured per-shard data lands; the
  warm total will be lower (cached reads).

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
