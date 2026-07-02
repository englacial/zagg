# ATL03 per-chunk byte-offset extraction (issue #158)

The byte/chunk-offset arm PR #152 deferred: for every heights dataset (and
the geolocation link arrays the read path needs), one row per HDF5 chunk

```
(granule, beam, dataset, chunk_idx, elem_start, elem_end, byte_offset, nbytes, filter_mask)
```

so an arm-(2b) worker (issue #148) can turn a shard read into pure ranged
GETs with **zero HDF5 metadata I/O**. Self-contained bench tooling — no
imports from unmerged PR #150/#152 code.

## Files

```
extract_offsets.py     the extractor: route (a) h5py reference, route (b) h5coro
                       B-tree walk (pure Python, Lambda-deployable), parquet I/O, CLI
crosscheck_hidefix.py  three-way gate: (a) vs (b) vs hidefix's serialized index
fetch_88s_sample.py    88S pinned-shard sample over HTTPS/EDL (stream + download)
results/               committed artifacts (gate reports, offsets parquet, timings)
```

## Workflow

```sh
# 1. extract, both routes, all cached granules (zagg dev venv has h5py + h5coro)
python bench/offsets/extract_offsets.py ~/ignore/zagg_neon_atl03_test_shard/granules \
    --out-dir <scratch>/out --timings-out <scratch>/timings_neon.json

# 2. cross-validate against hidefix (podman image zagg-bench-hidefix from PR #150's
#    Containerfile.hidefix; flexbuffers parsing needs the pure-Python `flatbuffers`
#    package on PYTHONPATH — bench-tool-only, not a zagg dependency)
PYTHONPATH=<dir with flatbuffers> python bench/offsets/crosscheck_hidefix.py \
    --offsets-dir <scratch>/out --granule-dir ~/ignore/zagg_neon_atl03_test_shard/granules \
    --fx-dir <scratch>/fx --make-index --report-out bench/offsets/results/crosscheck_neon.json

# 3. 88S sample (EDL creds via ~/.netrc; downloads are temp-dir only and deleted)
PYTHONPATH=<dir with flatbuffers> python bench/offsets/fetch_88s_sample.py \
    --out-dir <scratch>/out_88s --report-out bench/offsets/results/report_88s.json
```

## Results

### Cross-validation gate (NEON cache, 61 granules)

**61 granules, 184,616 chunks total, zero mismatches** — `(elem_start, elem_end,
byte_offset, nbytes)` byte-identical chunk-for-chunk across h5py, the h5coro
B-tree walk (plus `filter_mask` between those two), and hidefix's index
(`hfxidx` flexbuffers dump; chunk records decoded as little-endian u64
`(addr, size, offset[D])`). Report: `results/crosscheck_neon.json`.

### 88S sample (pinned o9 shard `11530494877603201033`)

Three granules — first / middle / last of the pinned shard's 5,620-granule
list (2018-10-14, 2022-01-25, 2025-06-01), from a workstation over
HTTPS/EDL. Streamed offsets parquets committed under
`results/offsets_88s_sample/`; full report in `results/report_88s.json`.
**15,472 chunks, every gate green**: h5py vs h5coro vs hidefix on the
downloaded files, and the HTTPS-*streamed* route-(b) frame identical to the
local one.

| granule | chunks | stream GETs | stream wall | download wall |
|---|---|---|---|---|
| ATL03_20181014103720 | 5,116 | 1 (4 MiB) | 5.0 s | 458 s |
| ATL03_20220125022450 | 7,368 | 1 (4 MiB) | 3.6 s | 848 s |
| ATL03_20250601165401 | 2,988 | 1 (4 MiB) | 3.7 s | 206 s |

(Honesty note: the committed `report_88s.json` was produced before the
extractor-vs-extractor compares gained `filter_mask` parity with the NEON
gate, so its 88S compares cover `(elem_start, elem_end, byte_offset,
nbytes)`; `filter_mask` is 0 on all 15,472 chunks of the committed streamed
parquets, and the strict compare is what the script runs now.)

The headline: **streaming extraction needs one 4 MiB ranged GET.** NSIDC's
files keep the metadata (object headers + chunk B-trees) in the front of the
file, inside h5coro's first 4 MiB cache line, so route (b) over HTTPS costs
one GET and ~4–5 s of wall (TLS + EDL redirect dominated) — the multi-GB
granule is never touched. The full-download leg (~25 min for three granules
over a workstation link) existed only so h5py and hidefix could arbitrate;
that is exactly the step that becomes irrelevant in-region, where the (2b)
fan-out fetches from S3 in seconds. Downloads were temp-dir only and are
deleted on exit (EOSDIS no-redistribute) — only offsets and reports are
committed.

### Extraction cost (refining the #152 estimate)

Local (M-series, 61 NEON granules, metadata-only):

| route | mean | median | max |
|---|---|---|---|
| (a) h5py | 0.020 s | 0.019 s | 0.038 s |
| (b) h5coro | 0.028 s | 0.026 s | 0.060 s |

vs 0.54–2.46 s/granule for the #152 boundary-geometry scan — offsets are
~30–80× cheaper because no chunk is ever inflated. Cost model for a
standalone offsets pass (Lambda arm64 @ 2 GB, S3 in-region, ~1–2 GETs +
sub-second CPU → ~1 s billed): **~$0.000034/granule** → pinned o9 shard
(5,620 granules) ≈ **$0.19**, full ~500k-granule ATL03 catalog ≈ **$17**.
Piggybacked on the #152 geometry extraction (which already walks the B-tree)
the marginal cost is ~zero — the numbers above are the *standalone* ceiling.

## Manifest layout (proposal for the arm-(2b) reader)

```
<prefix>/offsets/<granule_id>.offsets.parquet   # one file per granule
```

- `<granule_id>` is the `.h5` basename without extension, mirroring #152's
  `<granule_id>.boundaries.parquet` convention; the two caches join on
  `(granule, beam, chunk_idx)` — boundaries answer *which* chunks a shard
  needs, offsets answer *where the bytes are*.
- Per-granule parquet metadata (key `zagg:offsets_meta`) carries provenance
  (`route`, `wall_s`, `schema_version`, `missing_beams`).
- Per-granule files (~50–90 KB) keep the Lambda fan-out write pattern of
  #152 (one worker → one object, no coordination); a per-shard rollup is a
  cheap post-pass if GET count ever matters. The committed
  `results/neon_offsets.parquet` is such a rollup (61 granules, 184,616
  rows, 1.3 MB gzip): the route-(b) per-granule frames from step 1
  concatenated and rewritten with fastparquet `compression="gzip"` (meta key
  `zagg:offsets_meta` records source + row count).
- Provenance of the committed 88S sample parquets: written by
  `fetch_88s_sample.py`'s *streamed* leg (route (b) over `HTTPDriver`);
  verifiable against `results/report_88s.json`, whose per-granule
  `stream.extract_wall_s` equals each parquet's `zagg:offsets_meta`
  `wall_s`. A `transport` field in the meta dict is a worthwhile refinement
  if this graduates from bench tooling.

## Convergence with the hidefix index (#155)

Per #158's option (3): this parquet is the **interchange/debug artifact**;
the serialized hidefix index (~600 KB/granule flexbuffers) is the intended
**production cache**. The binding surface #155 was waiting on now exists —
`github.com/espg/h5coro-hidefix` (main @ `93d02ba`) exposes
`Index.save`/`Index.load`, `chunks()`, `read_plan()` and
`read_from_buffers()`. This parquet and that index are the two halves of the
same (2b) contract: the parquet's rows are what `chunks()` enumerates (the
gate above proves the per-chunk `(addr, size)` content is identical), and
the Lambda flow the binding enables is

```
Index.load(source=...) → read_plan(dataset, slice) → obstore ranged GETs → read_from_buffers()
```

— no libhdf5, no B-tree walk at read time. The arm-(2b) benchmark reader
should keep its interface narrow (per the decision on the #148 thread:
parquet primary, hidefix drop-in) — either backend answers "give me
`(byte_offset, nbytes)` for these `(granule, beam, chunk_idx)` keys."
