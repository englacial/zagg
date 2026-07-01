# hidefix spike notes (phase 4 of issue #149)

Evaluation of [hidefix](https://github.com/gauteh/hidefix) 0.12.0 (pure-Rust,
index-first HDF5 reader) on the frozen o10/o9 workloads, plus serialized
chunk-index build time/size measurements that feed issue #148's chunk-offset
cache design. Spike driver: `bench/h5coro/hidefix_spike.py`; container:
`bench/h5coro/Containerfile.hidefix`.

Timed runs were serial, on 2026-07-01 (o10 container row started 14:52:13 PDT,
o9 at 14:53:02 PDT), after the container image build and the concurrent
phase-2 (h5coro-numpy) runs had finished — the primary container rows should
be clean. The macOS host rows (14:48/14:49 PDT) *did* overlap the Rust
compile of the container image and are reference-only; re-run for canonical
host numbers if needed.

## Install experience

- **Versions**: `hidefix==0.12.0` (PyPI latest; crate v0.12.0), Rust toolchain
  1.96.0, Python 3.12, numpy 2.5.0 (host venv) / numpy in the
  `zagg-bench-h5coro` base image (container).
- **macOS arm64**: prebuilt wheel installs cleanly (`uv pip install hidefix`).
- **Linux aarch64: no wheel on PyPI** (only manylinux x86_64, macOS arm64,
  win amd64). The container builds from the sdist, which needs a Rust
  toolchain + cmake + build-essential (the crate's default `static` feature
  compiles and bundles libhdf5 via `hdf5-metno-src`, so **no system libhdf5**
  is needed at build or run time — `apt-get install libhdf5-*` was not
  required). Sdist build adds several minutes of image build; runtime image
  works with no extra shared libraries.
- **Dependency footprint**: the pip package hard-depends on `xarray`,
  `netcdf4` and (transitively) `pandas` — pulled in even though we only use
  `hidefix.Index`. Heavier than a reader needs to be.
- **License**: LGPL-3.0-or-later — flag for the dependency discussion if
  hidefix is ever proposed as a real zagg dependency (§4 of CLAUDE.md).

## API surface used

```python
idx = hidefix.Index(path)              # walks the file, builds chunk index
ds  = idx.dataset("/gt1l/heights/lat_ph")
ds.shape(); ds.chunk_shape()           # numpy uint64 arrays
arr = ds[(slice(a, b), slice(0, n))]   # -> numpy ndarray, native dtype
```

- **Partial reads (hyperslices) are native** — no covering-slice fallback was
  needed; every captured `[start, end)` request maps directly to a slice.
  Reads release the GIL and decode chunks with rayon.
- Quirks (both compensated for in `hidefix_spike.py`):
  - `__getitem__` accepts only a **tuple** of slices (a bare `slice` raises
    `TypeError`).
  - Returned arrays **squeeze every dimension with count <= 1**
    (`read_py_array` in the crate's `src/python.rs`), so a length-1 slice of
    the 2-D `signal_conf_ph` comes back `(5,)` instead of `(1, 5)`. The spike
    reshapes to the request's expected shape before checksumming.
- **No serialization from Python**: `pickle.dumps(idx)` raises
  `TypeError: cannot pickle 'builtins.Index' object`; `__getstate__` returns
  `None`; the pyclass exposes only `dataset`/`datasets`/`__getitem__`. All
  serialized-index numbers below therefore come from the crate's `hfxidx` CLI
  (`cargo install hidefix --version 0.12.0 --features clap,bincode,flexbuffers`),
  built into the container image and invoked per granule by the spike.

## Coverage and correctness

Every dataset in both workloads read correctly — **no unreadable datasets**:

- 1-D f8/f4 (`lat_ph`, `lon_ph`, `h_ph`, `reference_photon_lat/lon`), 1-D
  int (`ph_index_beg`, `segment_ph_cnt`), and the 2-D `(n_photons, 5)` int8
  `signal_conf_ph` — all gzip+shuffle chunked — match the h5coro reference
  checksums byte-for-byte (gate imported from `bench_replay.py`).
- o10: **pass, 2212/2212 arrays**; o9: **pass, 3528/3528 arrays** (host and
  container).
- Filter support in 0.12.0 is gzip (libdeflater) + shuffle + byteorder, which
  covers ATL03 fully; datasets with other filters (szip, scaleoffset, lzf)
  would fail at index time.

## Replay results (linux-arm64 container = primary rows)

| variant | requests | wall_s | cpu_s | max_rss_mb | correctness |
|---|---|---|---|---|---|
| h5coro-1.0.4-linux-arm64 (baseline) | o10 | 93.5 | 98.2 | 248 | pass |
| **hidefix-0.12.0-linux-arm64** | o10 | **15.5** | 16.1 | 155 | pass |
| h5coro-1.0.4-linux-arm64 (baseline) | o9 | 129.0 | 133.6 | 282 | pass |
| **hidefix-0.12.0-linux-arm64** | o9 | **20.8** | 20.9 | 161 | pass |

≈ **6.0× (o10) / 6.2× (o9)** faster than h5coro 1.0.4, at ~55–60% of the RSS.
(All four rows use the aligned read-window timing — index build + reads timed,
checksum gate excluded and reported as `gate_s`; hidefix rows re-measured in
the phase-5 canonical pass.) macOS host reference rows (not primary): o10
13.3 s, o9 16.1 s vs h5coro 84.8 s / 116.5 s.

**Where hidefix's time goes**: index build dominates. Of the 16.0 s o10 wall,
10.2 s is `hidefix.Index()` construction (the metadata/B-tree walk, hidefix's
analogue of h5coro's per-granule metadata parse) and only ~5.0 s is actual
chunk reads (o9: 13.7 s build / 9.2 s reads of 24.0 s). With a *pre-built*
index — exactly what #148's offset cache would provide — the replay would be
roughly 5–10 s, i.e. **~15–20× over the h5coro baseline**. Today that is
unreachable from Python because the binding can neither save nor load an
index (see below).

## Index build cost and serialized size (feeds #148)

Per granule (ATL03, ~1.5–2.0 GB files; 50 granules in o10, 59 in o9),
linux-arm64 container:

| metric | o10 mean / min / max | o9 mean / min / max |
|---|---|---|
| in-process index build (s) | 0.204 / 0.184 / 0.234 | 0.233 / 0.186 / 0.589 |
| `hfxidx` wall incl. startup + re-index (s) | 0.23 / 0.20 / 0.30 | 0.26 / 0.21 / 0.73 |
| serialized size, bincode (bytes) | 599,960 / 386,284 / 1,179,636 | 601,978 / 386,284 / 1,179,636 |
| serialized size, flexbuffers (bytes) | 587,355 (mean) | 589,014 (mean) |

- Totals: o10 index build 10.2 s / 30.0 MB bincode across 50 granules; o9
  13.7 s / 35.5 MB across 59.
- ≈ **0.6 MB of index per ~1.9 GB granule (~0.03% of file size)** — cheap to
  store next to a granule cache or in the shardmap pipeline.
- `hfxidx` wall minus in-process build (~0.02–0.05 s) shows the
  encode+write cost itself is negligible; **indexing, not serialization, is
  the cost**, and it is ~0.2 s/granule (vs h5coro spending ~2 s/granule on
  the whole read).
- Curiosity: bincode size for the same granule differs ~1.1% between the
  macOS and linux builds (e.g. 683,591 vs 675,806 bytes for the first o10
  granule). Both pass the checksum gate and report the same root dataset
  count; likely minor dependency-resolution drift between the two `cargo
  install` runs. Not load-bearing at these magnitudes, but worth pinning with
  `--locked` if the sizes are ever used as identities.

## Can it serve #148's offset cache?

**The data is there; the Python door isn't.**

- The serialized index *does* contain exactly what #148 needs, in public,
  documented crate types: `hidefix::idx::Chunk` is
  `{ addr: u64 /* byte offset in file */, size: u64 /* stored bytes */,
  offset: [u64; D] /* dataspace coords */ }` and `Dataset` carries `dtype`,
  `shape`, `chunk_shape`, `shuffle`, `gzip` plus the sorted chunk table
  (`src/idx/chunk.rs`, `src/idx/dataset/dataset.rs`; all serde-serializable,
  bincode or flexbuffers).
- But the **format is bincode/flexbuffers of Rust structs** — an
  implementation detail, not a documented stable interchange format — and the
  **Python binding exposes neither serialization nor the chunk table**, so a
  pure-Python zagg cannot read or produce these indexes without either an
  upstream PR (expose `Index` save/load and/or chunk enumeration in
  `python.rs` — both look small) or a sidecar Rust tool, or parsing an
  unstable format.
- Practical #148 takeaway independent of hidefix adoption: a complete chunk
  offset cache for an ATL03 granule costs ~0.2 s to build and ~0.6 MB to
  store, and byte offsets + compressed sizes per chunk are sufficient to skip
  the B-tree walk entirely — hidefix's whole design demonstrates the win.

## Verdict

- **(a) as the compiled reader**: works today — full coverage and byte-exact
  correctness on this workload, 6.0–6.2× wall over h5coro 1.0.4, native
  hyperslices, lower RSS. Costs to weigh: no linux-aarch64 wheel (source
  build with Rust in the Lambda layer pipeline, or vendor a wheel), heavy pip
  deps (xarray/netcdf4/pandas), LGPL-3.0, local-file API only (no
  S3/fsspec driver in the Python binding — zagg's S3 path would need
  download-then-read or upstream work).
- **(b) as #148's offset-cache mechanism**: not as-is from Python. The
  serialized index has the right contents at the right cost (~0.2 s /
  ~0.6 MB per granule), but hidefix 0.12.0's Python API cannot save, load,
  or introspect it. Route (a) upstream PR to expose save/load (then cached
  replay drops to ~5–10 s, ~15–20× over baseline), or (b) use hidefix's
  numbers as the cost model and implement the offset cache natively in zagg.
