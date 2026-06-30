# TODO: build `sm_healpix_o9.json` (issue #133, phase 1)

The forward benchmark (issue #133) adds a HEALPix **order-9** row to the
sharded-vs-inner matrix. That needs the densest order-9 shard over the NEON AOP
box pinned into `sm_healpix_o9.json` and a `healpix_o9` entry in `targets.json`
`shardmaps`.

**Status: blocked in the routine environment.** Building the map requires an
anonymous CMR/STAC catalog query, and outbound HTTPS to
`cmr.earthdata.nasa.gov` is denied by this environment's egress policy
(`403` on `CONNECT`). `mortie` (0.8.2) and the order-9 HEALPix grid both work
offline (`parent_order=9, child_order=19` -> 1024x1024 = 1048576 cells, K=256
inner chunks at `chunk_inner 13`), so the only missing input is the granule
catalog. The pinned `shard_key`/`n_granules` are **not** fabricated; this file
is the placeholder until the map can be built where CMR is reachable.

## Build command (from a network where CMR is reachable)

Mirrors the documented procedure in `../README.md` ("Add a target"), using the
order-9 t-digest config added in phase 2:

```bash
python -m zagg.catalog \
  --config tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml \
  --short-name ATL03 --version 007 \
  --start-date 2018-10-13 --end-date 2025-06-01 \
  --polygon tests/data/benchmark/AOP_NEON.geojson \
  --backend mortie \
  --output tests/data/benchmark/shardmaps/sm_healpix_o9.json
```

Then pin the densest shard:

```python
import json, sys; sys.path.insert(0, ".github/scripts")
import bench_metrics
sm = json.load(open("tests/data/benchmark/shardmaps/sm_healpix_o9.json"))
print(bench_metrics.select_densest_shard(sm))   # -> (shard_key, n_granules)
```

and add the `healpix_o9` entry to `targets.json` `shardmaps` with those values.
The drift test (`tests/test_benchmark_shardmap.py`) and the consistency test
(`test_targets_manifest_consistent`) both parametrize over `targets.json`, so
order-9 is covered automatically the moment the entry exists -- no test code
change is needed. Delete this file once `sm_healpix_o9.json` lands.
