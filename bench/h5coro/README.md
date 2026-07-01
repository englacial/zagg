# h5coro read-path benchmark (issue #149)

Measures where zagg's granule-read time actually goes, and what compiled
alternatives buy, on the *exact* read workload the worker issues — decode cost
isolated from S3/HTTPS latency by reading locally cached granules with
`FileDriver`.

Four variants are compared on the same frozen request lists:

| variant | what it is |
|---|---|
| `h5coro-1.0.4` | pure-Python h5coro as zagg pins it today (baseline) |
| `h5coro-numpy` | local h5coro with numpy-vectorized hot spots (comparable) |
| `shim` | sliderule's C++ H5Coro via a benchmark-only pybind11 shim |
| `hidefix` | pure-Rust index-first HDF5 reader (github.com/gauteh/hidefix) |

## Workflow

1. **Capture** (once per shard order; needs the zagg dev venv and the local
   granule cache — granule `.h5` files are NASA EOSDIS data and are *not*
   committed, only the request lists and results are):

   ```sh
   python bench/h5coro/capture_requests.py --order 10 \
       --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules
   ```

   Runs the real `process_shard` (t-digest config, densest benchmark shard from
   `tests/data/benchmark/shardmaps/`) with `H5Coro.readDatasets` wrapped, and
   writes every call's `(dataset, hyperslice)` entries to
   `requests/o<order>.json`.

2. **Baseline + checksums**:

   ```sh
   python bench/h5coro/bench_replay.py --requests bench/h5coro/requests/o10.json \
       --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules \
       --variant h5coro-1.0.4 --write-baseline
   ```

   Records wall/CPU/peak-RSS to `results/replay_o10_h5coro-1.0.4.json` and the
   per-array sha256 reference to `results/checksums_o10.json`.

3. **Any other variant** replays the same requests and is hard-gated on byte
   equality against the reference checksums (`--baseline ...`); a variant that
   returns different bytes exits non-zero instead of producing a row.

4. **Decomposition**: add `--profile` to bucket cProfile self-time into
   inflate / shuffle / B-tree / slice-assembly / metadata / field-unpack /
   file-io / zlib. The raw `.pstats` lands next to the JSON summary.

## Linux arm64 (Lambda-like) rows

`Containerfile` builds the replay environment (numpy + h5coro + py-spy);
Apple Silicon runs it natively under podman — see the header comment in the
Containerfile for build/run one-liners. Container numbers are the primary
rows in the report; macOS host numbers are recorded for reference.

## Layout

```
capture_requests.py   freeze the worker's readDatasets workload to JSON
bench_replay.py       replay + metrics + correctness gate + profile buckets
requests/             captured workloads (o9, o10)
results/              replay metrics, checksums, profiles, REPORT.md
patches/              numpy-vectorization patch against upstream h5coro (phase 2)
shim/                 pybind11 shim over sliderule C++ H5Coro (phase 3)
```
