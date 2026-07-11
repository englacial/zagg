# Lambda benchmark matrix (issue #110 / #25)

**This directory *is* the benchmark matrix.** Which targets run is defined by the
data here — not by the workflow or the runner. So adding, removing, or reshaping
benchmarks is a **data edit under `tests/data/benchmark/`**, with no change to
`.github/workflows/lambda-benchmark.yml` or `.github/scripts/run_benchmark.py`
(those are stable plumbing). A routine that can't touch CI/CD can still curate
the matrix from here.

## Layout

| Path | Role |
| --- | --- |
| `targets.json` | the matrix: the list of targets + the shared shard maps and their pinned densest shard |
| `configs/*.yaml` | one pipeline config per target (data_source + aggregation + `output.grid`) |
| `shardmaps/*.json` | one shard map per `(grid, order)` — **shared** by both aggregators on that grid |
| `AOP_NEON.geojson` | the **default** AOI (NEON SERC AOP box) shard maps are built over |
| `antarctic_88s.geojson` | a non-default AOI near the ±88° turning latitude, for an override 88°S stress target (issue #121) |

The workflow calls `run_benchmark.py --targets tests/data/benchmark/targets.json`,
which loads `targets.json` and dispatches **one shard per target** (the pinned
densest cell). `gain_bias` and `tdigest` over the same grid+order **share one
shard map** (the densest shard depends only on the data + grid, not the
aggregator).

## Conventions

- **Live matrix (issue #202 reset):** tdigest, sharded output, `granule_workers=4`,
  at **4 GB** workers, **o9 only** — a **2×2** over the read-backend A/B
  (`inline` vs `sidecar`) **×** the strict-AOI-mask A/B (`mask` vs `nomask`). Four
  targets: `tdigest_healpix_o9_{inline,sidecar}_{mask,nomask}`. The `mask` arm
  dispatches the `aoi_mask`-carrying shard map (`healpix_o9_aoimask`); the `nomask`
  arm the plain one — the mask is **manifest-driven** (`runner._aoi_payload_map`
  reads it from the shard map, not the config), so the two arms differ only by
  which map they dispatch. **o10 is retired** from the live set (moved to
  `provisional_targets`, retained not rerun); the earlier sharded/inner codec +
  `cached` axes stay frozen too.
- **Per-merge vs per-release (issue #202).** This single-shard live matrix is the
  **per-merge-to-`main`** series — one densest shard/target, so cost/runtime deltas
  track *code*, not data drift. The complementary **per-release full-AOI NEON**
  series (the whole `AOP_NEON` box fanned over every shard, for dollar-cost truth)
  is recorded per release, not every merge; its harness + pinned targets
  (`targets_full_aoi_neon.json`) are a separate deliverable (issue #202 leg 1) and
  its plot is a skeleton until that schema lands (see `benchmark.md`).
- **One shard per target** — the *densest* cell over the AOI, so cost/runtime
  deltas track code, not data drift.
- **Densest = most granules; ties broken by lowest `shard_key`** — the
  deterministic rule in `bench_metrics.select_densest_shard`.
- **Temporal pin:** `2018-10-13 .. 2026-03-15` (full-mission slice — the last CMR
  granule is `2026-03-11`; issue #202 re-pin), recorded in `targets.json` under
  `temporal`. AOI = `AOP_NEON.geojson`. The 88°S stress pins (issue #148) keep
  their **own** temporal window and are not re-pinned.
- **Cost model:** arm64, 2 GB, capped at the 900 s deploy timeout (see
  `zagg.dispatch`).
- **AOI is per shard map (issue #121).** The top-level `aoi`/`temporal`/`cmr` in
  `targets.json` are **defaults**; a shard-map entry may override any of them.
  A shard map built over a non-NEON box carries its own `aoi` (and usually
  `temporal`/`cmr`); entries that omit a key inherit the default, so existing
  NEON shard maps resolve exactly as before. The drift test
  (`test_benchmark_shardmap.py`) rebuilds each map over its *resolved* AOI.

## Plot layout (`plot_series.py`)

`plot_series.py` renders the **live matrix** on top, and embeds the retained
**archived** figures (frozen as of issue #193) below.

### Live matrix — inline/sidecar × AOI-mask, fixed 2×2 (`*_matrix.png` + `matrix_table.png`)

The live matrix (issue #202 reset) is **tdigest, sharded, `granule_workers=4`, at
4 GB, o9 only**. The charts (`cost_per_shard_matrix.png`,
`cost_per_100km2_matrix.png`) lay the rows out in a **fixed** 2×2 grid keyed to the
experiment:

- **Columns = the read backend.** Left = `inline`, right = `sidecar`
  (`index_backend`).
- **Rows = the strict-AOI-mask arm.** Top = `nomask`, bottom = `mask`. There is no
  record column for the mask A/B (`bench_metrics`/`run_benchmark` are stable
  plumbing), so the renderer reads the arm off the **target-name suffix**
  (`plot_series._aoi_axis`: `…_mask` vs `…_nomask`). An arm that hasn't landed
  renders blank, so the shape stays stable.

`matrix_table.png` is the latest-merge table for these rows; `latest.md` /
`metrics.json` track this matrix.

**Series reset.** The matrix selector (`plot_series._matrix_mask`) admits only rows
matching the reset scheme — `grid_size == "o9"`, a set `index_backend`, and a
`…_mask`/`…_nomask` target suffix. The pre-reset live datapoints (issue #193's
`o9`/`o10` inline-vs-sidecar rows, whose names carry no mask suffix) fall outside
that scheme and drop out, so the corrected 2×2 begins **from zero** at the first
post-reset merge without pruning the benchmarks-branch `series.parquet` (a physical
prune, if wanted, is an operator action — agent pushes are scoped to `claude/*`).

### Per-release full-AOI NEON (`full_aoi_*.png` — skeleton, issue #202 leg 1)

The whole-`AOP_NEON`-box run fanned over **every** shard, recorded **per release**
for dollar-cost truth (complementing the per-merge single-shard matrix above). Its
harness + pinned targets (`targets_full_aoi_neon.json`) are a separate deliverable;
`plot_series.make_full_aoi_release_figure` is a skeleton (returns `False`, renders
nothing) until that output schema lands, so the Pages index simply omits the
section for now. See `docs/deployment/benchmark.md`.

### Archived: forward codec matrix (frozen, `*_codec.png` + `codec_table.png`)

**Retired as of issue #193** — the ShardingCodec sharded-vs-inner + read (`cached`)
A/B. Its PNGs are **retained on the `benchmarks` branch** and embedded in the
archived section of the index, but **no longer regenerated**. It was a fixed 2×3
grid: columns `sharded` / `inner` / `cached`, rows `o9` → `o10` → `o11`. Split on
the `codec` column (issue #133). The render functions (`make_codec_figure`,
`_codec_layout`) remain in `plot_series.py` for on-demand regeneration.

### Archived: frozen historical — data-driven (`*.png` + `latest_table.png`)

**Also frozen as of issue #193** (retained, not regenerated). The charts
(`cost_per_shard.png`, `cost_per_100km2.png`) keep the **data-driven** layout
(issue #121 review) for the retired rect/gain_bias rows —
nothing is keyed to a fixed target list:

- **Columns = grid family.** The **left** column is the rectilinear (`rect_*`)
  targets; the **right** column is the HEALPix targets.
- **Rows = aggregator + resolution, aligned across families.** A row pairs the two
  families at the **same aggregator** and the **same shard-size rank within their
  family**, so e.g. `rect_6km` lines up with the largest HEALPix shard
  (`healpix_o10`) and `rect_3km` with `healpix_o11`. `tdigest` rows and
  `gain_bias` rows stay aligned.
- **Rows ordered largest-shard-first.** The largest shards sit at the **top** and
  shard size **descends** down the rows (size is ranked from `shard_area_km2`);
  same-size rows break ties on the aggregator name.

### Both families

- **Zeros are failed runs, not data.** A zero cost/runtime means the shard run
  failed, so it is **not** plotted as a real datapoint: the connecting line
  **breaks** at that merge (it never dips to 0) on **both** the cost and runtime
  series. The failure is shown as a single non-line-connected **`x`** marker on the
  cost axis (distinct from the normal cost circle / runtime open-circle), pinned
  near the axis floor so it keeps the x-axis/commit alignment without dragging the
  cost axis back down to 0. A failed shard zeros both series at the same merge, so
  the one cost `x` flags the failure for both.

When adding or rearranging targets, keep these conventions — the **live matrix**
layout is derived from `grid_size` / `index_backend`, and the archived layouts from
`grid_size` / `codec` (forward) and `grid_type` / `aggregator` / `shard_area_km2`
(frozen historical), so getting that metadata right in `targets.json` is what
places the panel.

## Add a target

1. **Config** — copy the closest `configs/*.yaml` and edit only what differs
   (the `aggregation` block and/or `output.grid`). HEALPix uses the `mortie`
   backend; rectilinear needs the exact-S2 `spherely` backend.

2. **Shard map** — only if no existing `(grid, order)` map fits. Build it
   anonymously (no Earthdata creds needed for the catalog build):

   ```bash
   python -m zagg.catalog \
     --config tests/data/benchmark/configs/<your_config>.yaml \
     --short-name ATL03 --version 007 \
     --start-date 2018-10-13 --end-date 2026-03-15 \
     --polygon tests/data/benchmark/AOP_NEON.geojson \
     --backend <mortie|spherely> \
     --output tests/data/benchmark/shardmaps/sm_<grid>.json
   ```

   > **Reproducing / re-pinning the NEON maps.** Build from **CMR** (as the CLI
   > above and the drift test do), *not* from a local full-catalog geoparquet: an
   > ATL03 STAC granule footprint is a coarse bounding quad that blankets the whole
   > tiny NEON box, so intersecting those quads over the full catalog over-includes
   > (a superset of CMR's finer orbit-geometry bbox search) and inflates the pins.
   > The #148 lat-ring maps *can* rebuild offline from the catalog only because
   > complete-ring lat-overlap is exact; a box AOI has no such equivalence.

   Then find the densest shard to pin:

   ```python
   import json, sys; sys.path.insert(0, ".github/scripts")
   import bench_metrics
   sm = json.load(open("tests/data/benchmark/shardmaps/sm_<grid>.json"))
   print(bench_metrics.select_densest_shard(sm))   # -> (shard_key, n_granules)
   ```

3. **`targets.json`** — add the shard map (once per grid+order) and the target:

   ```json
   "shardmaps": {
     "<grid_key>": { "path": "shardmaps/sm_<grid>.json",
                     "shard_key": <key>, "n_granules": <n> }
   },
   "targets": {
     "<agg>_<grid_size>": { "config": "configs/<your_config>.yaml",
                            "shardmap": "<grid_key>",
                            "aggregator": "<gain_bias|tdigest|...>",
                            "grid_type": "<healpix|rectilinear>",
                            "grid_size": "<o11|3km|...>" }
   }
   ```

4. **Verify** — these guard the pin (no AWS needed):

   ```bash
   pytest tests/test_benchmark.py          # incl. test_targets_manifest_consistent
   pytest tests/test_benchmark_shardmap.py # drift check (set ZAGG_BENCHMARK_DRIFT=1; needs network + spherely for rect)
   ```

   `test_targets_manifest_consistent` re-derives the densest shard from the
   committed map and fails if the pinned `shard_key`/`n_granules` is stale.

## The 88°S stress targets (issue #148)

`tdigest_healpix_o9_88s` / `tdigest_healpix_o10_88s` are **provisional**
(PR-tree-only) probes against the worst-case shard of the whole mission: the
ICESat-2 turning-latitude ring, where all ~1,387 RGTs converge. They reuse the
NEON tdigest configs and temporal window; only the AOI override differs.

- **Pins.** o9: the ring's densest shard by granule count (5,620 granules over
  576 ring shards / 2.04M pairs / 35,639 catalog granules — ~95× NEON o9's 59).
  o10: the densest o10 shard **nested inside** the pinned o9 shard
  (`nested_in` in `targets.json`; 4,605 granules vs the global o10 max of
  4,642 at another longitude), so one o9 boundary-extraction pass covers both
  orders. The drift test derives the same nested quantity when `nested_in` is
  set.
- **Pruned shard maps.** The full ring maps are ~0.7 GB (o9) / ~1.7 GB (o10) of
  JSON, so the committed `sm_healpix_o{9,10}_88s.json` keep **only the pinned
  shard** (see `metadata.pruned`). The benchmark dispatches only that shard;
  re-pinning means rebuilding the full map (step 2 below).
- **Build the catalog once.** The ring catalog is a ~20 min CMR fetch, so save
  it when (re)building (`--catalog-out
  tests/data/benchmark/catalogs/cat_88s.parquet`) and point the shard-map
  entry's `catalog_parquet` key at the committed snapshot: the drift test then
  rebuilds from the parquet instead of re-fetching CMR weekly (it becomes a
  deterministic, offline guard on the shardmap build + pin). Regenerate the
  snapshot only to deliberately re-pin.
- **Run on demand** via explicit `--target` (they are provisional so a red
  stress run never fails the every-merge matrix): OOM/timeout at 2 GB / 900 s
  is the *expected* baseline result until the issue #148 streaming/cached-read
  work lands.

## Add a target over a different AOI (latitude sweep / polar stress)

The default AOI is NEON SERC. To benchmark a **different** AOI — runtime vs.
latitude, or the `antarctic_88s.geojson` 88°S stress target — give that shard map
its own `aoi`/`temporal`/`cmr` *override*; absent keys fall back to the
top-level default (issue #121).

1. **AOI geojson** — drop the polygon under this directory (e.g.
   `antarctic_88s.geojson`, already shipped). HEALPix is CRS-agnostic, so it is
   the simplest grid for a clean latitude sweep; a rectilinear analog at high
   latitude needs a polar CRS (e.g. `EPSG:3031`) in its config.

   **Full-longitude rings must be sectorized.** A single lat/lon rectangle
   spanning `-180..180` collapses under spherical polygon fill (mortie traces
   the ring's edges as great circles, so coverage degenerates to an
   antimeridian sliver — 10 cells instead of ~576 at o9). `antarctic_88s.geojson`
   is therefore a MultiPolygon of eight 45° sectors with vertices sampled every
   1° of longitude; follow that pattern for any AOI that wraps the globe.

2. **Build the shard map over that AOI** — point `--polygon` at the new geojson
   (and `--start-date`/`--end-date` if the override narrows the window):

   ```bash
   python -m zagg.catalog \
     --config tests/data/benchmark/configs/<your_config>.yaml \
     --short-name ATL03 --version 007 \
     --start-date 2018-10-13 --end-date 2025-06-01 \
     --polygon tests/data/benchmark/antarctic_88s.geojson \
     --backend mortie \
     --output tests/data/benchmark/shardmaps/sm_healpix_o11_88s.json
   ```

   Pin the densest shard with `bench_metrics.select_densest_shard` as above.

3. **`targets.json`** — add the shard map **with its override** and a target:

   ```json
   "shardmaps": {
     "healpix_o11_88s": { "path": "shardmaps/sm_healpix_o11_88s.json",
                          "shard_key": <key>, "n_granules": <n>,
                          "aoi": { "file": "antarctic_88s.geojson", "name": "88S dense" } }
   },
   "targets": {
     "gain_bias_healpix_o11_88s": { "config": "configs/atl03_gain_bias_healpix_o11.yaml",
                                    "shardmap": "healpix_o11_88s",
                                    "aggregator": "gain_bias",
                                    "grid_type": "healpix",
                                    "grid_size": "o11_88s" }
   }
   ```

   Omit `temporal`/`cmr` to inherit the defaults, or set them to narrow the
   window. The drift test then rebuilds *this* map over its resolved 88°S AOI.
   A `cmr` override carries `short_name`/`version`/`provider`/`footprint` (like
   the top-level default) but **not** `backend`: the drift test reads the
   build backend from the committed shard map's `metadata.backend`, not from
   `cmr`.

   **High-latitude density → failure is the expected result.** Near ±88° the
   densest shard is far heavier than NEON's 44–50 granules, so the target will
   likely OOM or hit the 900 s / 2 GB timeout — that is the point of a stress
   target. Label it as such (it compounds the OOM issues #117 / #119).

## Remove a target

Delete its entry from `targets.json` `targets`. Drop its `config` (and the shard
map + `shardmaps` entry only if no other target references it).

## Notes

- Shard maps with a **tie** for densest (several shards at the same granule
  count) are noted inline in `targets.json`; the drift test is tie-tolerant
  (compares granule count, not exact key).
- Keep `targets.json` the single source of truth — the manifest-consistency and
  drift tests parametrize over it, so a new target is covered automatically.
