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

- **One shard per target** — the *densest* cell over the AOI, so cost/runtime
  deltas track code, not data drift.
- **Densest = most granules; ties broken by lowest `shard_key`** — the
  deterministic rule in `bench_metrics.select_densest_shard`.
- **Temporal pin:** `2018-10-13 .. 2025-06-01` (full multi-year slice), recorded
  in `targets.json` under `temporal`. AOI = `AOP_NEON.geojson`.
- **Cost model:** arm64, 2 GB, capped at the 900 s deploy timeout (see
  `zagg.dispatch`).
- **AOI is per shard map (issue #121).** The top-level `aoi`/`temporal`/`cmr` in
  `targets.json` are **defaults**; a shard-map entry may override any of them.
  A shard map built over a non-NEON box carries its own `aoi` (and usually
  `temporal`/`cmr`); entries that omit a key inherit the default, so existing
  NEON shard maps resolve exactly as before. The drift test
  (`test_benchmark_shardmap.py`) rebuilds each map over its *resolved* AOI.

## Plot layout (`plot_series.py`)

`plot_series.py` renders **two figure families**, split on the `codec` column
(issue #133): the forward sharded-vs-inner matrix (`codec` non-null) on top, and
the frozen historical series (`codec` null) below.

### Forward matrix — fixed 2×3 (`*_codec.png` + `codec_table.png`)

The forward charts (`cost_per_shard_codec.png`, `cost_per_100km2_codec.png`) lay
the codec rows out in a **fixed** grid keyed to the experiment, not the data:

- **Columns = the ShardingCodec A/B.** Left = `sharded`, right = `inner`.
- **Rows = order, largest-first.** `o9` (top) → `o10` → `o11` (bottom). A row whose
  order hasn't landed yet (e.g. `o9` before its shard map is built) renders blank,
  so the matrix shape stays stable.

`codec_table.png` is the latest-merge table for these rows.

### Frozen historical — data-driven (`*.png` + `latest_table.png`)

The frozen charts (`cost_per_shard.png`, `cost_per_100km2.png`) keep the
**data-driven** layout (issue #121 review) for the retired rect/gain_bias rows —
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

When adding or rearranging targets, keep these conventions — the frozen layout is
derived from `grid_type` / `aggregator` / `shard_area_km2`, and the forward layout
from `grid_size` / `codec`, so getting that metadata right in `targets.json` is
what places the panel.

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
     --start-date 2018-10-13 --end-date 2025-06-01 \
     --polygon tests/data/benchmark/AOP_NEON.geojson \
     --backend <mortie|spherely> \
     --output tests/data/benchmark/shardmaps/sm_<grid>.json
   ```

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

## Add a target over a different AOI (latitude sweep / polar stress)

The default AOI is NEON SERC. To benchmark a **different** AOI — runtime vs.
latitude, or the `antarctic_88s.geojson` 88°S stress target — give that shard map
its own `aoi`/`temporal`/`cmr` *override*; absent keys fall back to the
top-level default (issue #121).

1. **AOI geojson** — drop the polygon under this directory (e.g.
   `antarctic_88s.geojson`, already shipped). HEALPix is CRS-agnostic, so it is
   the simplest grid for a clean latitude sweep; a rectilinear analog at high
   latitude needs a polar CRS (e.g. `EPSG:3031`) in its config.

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
