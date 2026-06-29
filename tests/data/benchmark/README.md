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
| `AOP_NEON.geojson` | the AOI (NEON SERC AOP box) every shard map is built over |

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
- **Cost model:** arm64, 2 GB, capped at the 720 s deploy timeout (see
  `zagg.dispatch`).

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

## Remove a target

Delete its entry from `targets.json` `targets`. Drop its `config` (and the shard
map + `shardmaps` entry only if no other target references it).

## Notes

- Shard maps with a **tie** for densest (several shards at the same granule
  count) are noted inline in `targets.json`; the drift test is tie-tolerant
  (compares granule count, not exact key).
- Keep `targets.json` the single source of truth — the manifest-consistency and
  drift tests parametrize over it, so a new target is covered automatically.
