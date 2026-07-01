# h5coro read-path benchmark — report (issue #149)

**Question asked:** where does zagg's granule-read time go, and what would a
compiled read path buy — a zagg-specific C++ extension, upstream accelerators,
or a Rust reader?

**Method:** the worker's exact `readDatasets` workload was captured once per
benchmark shard (o10 densest: 50 granules / 939,217 obs; o9 densest: 59
granules / 3,112,138 obs — NEON test-shard cache, local `FileDriver`, so decode
cost is isolated from S3 latency) and replayed call-for-call by every variant.
Every returned array is sha256-gated against the h5coro-1.0.4 baseline —
**all rows below are byte-identical**, zero mismatches, zero omissions. Primary
rows ran serially in a podman linux/arm64 container (the Lambda target
architecture); timing covers the read windows only (`gate_s` separate).

## Where the time goes (cProfile self-time, o10, h5coro 1.0.4)

| bucket | seconds | share |
|---|---|---|
| `shuffleChunk` (pure-Python de-shuffle) | 64.2 | **68%** |
| zlib inflate (already C) | 15.6 | 16% |
| file I/O | 2.4 | 3% |
| B-tree walk + slice assembly + metadata + field unpack | 1.6 | 2% |
| other (orchestration) | 9.3 | 10% |

The counter-intuitive headline: the big float coordinate datasets aren't
shuffled at all — only `signal_conf_ph` is, and at `n_photons x 5` int8 it is
the largest dataset by element count, de-shuffled element-wise in Python. o9
shows the same shape (97.1 s of 139.6 s). After the numpy patch the same
profile drops shuffle to 0.5 s, leaving zlib (7.1 s) as the compiled floor.

## Results (linux/arm64 container, read wall, byte-equality gate passing)

| variant | o10 | o9 | speedup | peak RSS (o10/o9) |
|---|---|---|---|---|
| h5coro 1.0.4 (baseline) | 93.5 s | 129.0 s | 1x | 248 / 282 MB |
| h5coro + numpy `shuffleChunk` (`patches/`) | 24.5 s | 34.0 s | **3.8x** | 255 / 286 MB |
| hidefix 0.12.0 (Rust, index-first) | 15.5 s | 20.8 s | **6.0x / 6.2x** | 155 / 161 MB |
| sliderule C++ H5Coro (pybind shim) | 10.3 s | 14.2 s | **9.1x** | 187 / 208 MB |

Notes: single-machine variance ~±15% (two shim runs: 10.3/11.7 s). The hidefix
rows *include* per-granule chunk-index construction (o10: 10.6 s of 15.5 s);
with a pre-built serialized index its replay is ~5–8 s — in shim territory —
and that index is exactly the per-chunk `(addr, size, offset)` payload #148's
offset cache needs (~0.2 s to build and ~600 KB serialized per ~1.9 GB granule;
see `hidefix_spike_notes.md`). espg's local h5coro `main` (memory-handling
commits) is ~4% faster than 1.0.4 (host rows in `results/`).

**End-to-end worker check (macOS host, o10 shard):** `process_shard` wall drops
**91.9 s → 23.5 s (3.9x)** with only the numpy patch swapped in — identical
output (939,217 obs, same 1,312 calls). Reads were ~93% of worker wall; the
read path was the right lever.

## Lambda footprint

| route | added deployment weight |
|---|---|
| numpy patch (upstream h5coro) | zero — pure Python, ships in the existing dep |
| C++ shim | ~2.2 MiB read path (`h5shim` .so 0.19 MiB + `libsliderule` 1.97 MiB); ~3–5 MiB with lua/curl/readline/uuid — well inside the ~100 MiB headroom, but vendors a patched sliderule build (`shim/sliderule-minimal-build.patch`) into the layer pipeline |
| hidefix | no linux-aarch64 wheel on PyPI (source build with pinned Rust, `Containerfile.hidefix` shows the recipe); pip package drags xarray/netcdf4/pandas (layer-size impact needs its own measurement); LGPL-3.0; Python binding today is local-file only (no S3 driver) and cannot save/load/enumerate its index |

## Recommendation

Honoring the maintenance-outside-zagg preference:

1. **Land the numpy `shuffleChunk` patch upstream in h5coro now** (staged in
   `patches/`, byte-identical incl. the ragged-tail and out-of-range guard
   semantics; unit-verified against the original loop). 3.8x reads / 3.9x
   worker for ~10 lines, zero deployment cost, benefits every h5coro user.
   Upstream submission is a hand-off (outside this repo's push scope).
2. **Pursue hidefix as the compiled step, via a small upstream PR** exposing
   index save/load(+chunk enumeration) in the Python binding. That single
   change (a) closes most of the gap to the C++ shim once indices are cached,
   and (b) *is* the #148 chunk-offset-cache mechanism — one dependency serves
   both. Prerequisites before adoption: the dependency discussion (LGPL,
   transitive py-deps, source-build in the layer), and an S3 read story
   (obstore-backed range reader or upstream driver).
3. **Keep the C++ shim as the measured ceiling (9.1x), not the product path.**
   It exists, is reproducible (`shim/build.sh all`), and its footprint is
   acceptable — but it vendors a patched sliderule build and a pybind surface
   zagg would own forever, against the stated preference. Revisit only if
   hidefix upstream stalls *and* the post-numpy read time still blocks #148's
   900 s budget at 88S.

## Caveats

- Local-file replay isolates decode; S3 latency/concurrency is a separate
  axis (h5coro's per-dataset threading vs hidefix/shim concurrency models
  differ — re-benchmark on the S3 path before final adoption).
- Single shard-pair workload (NEON o9/o10 densest); 88S (#148) multiplies
  granule counts ~8x, which amortizes per-granule fixed costs differently —
  the index-cache advantage grows.
