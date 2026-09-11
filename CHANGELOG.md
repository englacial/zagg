# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- every `-disk` worker variant gets Lambda's 10240 MB `/tmp` ceiling (#536)
  ([#537](https://github.com/englacial/zagg/pull/537))
  - The spill block is **disk-bound at every memory tier**:
    `_default_block_bytes` is `min(0.2 x memory x K, 0.45 x free_tmp)` and
    `K = 4 ** (group_order - parent_order)` is 64 on the production grids, so
    the memory term is 26–102 GiB while the disk term was 2.70–4.50 GiB.
  - 249 of 2,726 shards on the CA GEDI build raised `SpillOverflowError` at the
    6144-derived 2.70 GiB cap — the waveform reducers have no cross-block fold
    law, so the config is exact-single-block-only and refuses rather than
    approximate. The 10 densest re-ran clean at 10240 MB (peak spill 3.56 GiB
    against 4.50 GiB, `spill_blocks_closed = 0`).
  - Supersedes issue #235's `memory + 2048`, under which only the 8192 variant
    reached the ceiling. Extra ephemeral storage costs $0.08 across a
    2,726-shard run. This raises the ceiling only: the 900 s timeout still
    scales with vCPU, so it is not a licence to drop a memory tier.

- an ACL-bearing write is a single `PutObject`, never a multipart upload (#534)
  ([#535](https://github.com/englacial/zagg/pull/535))
  - `x-amz-acl` is legal on `PutObject` and `CreateMultipartUpload` and illegal
    on `UploadPart`, which S3 answers `400 InvalidArgument` / "The specified
    header is not valid in this context". obstore attaches the canned ACL as a
    `client_options.default_headers` entry, so it rode every request the write
    twin made, and any object over obstore's 5 MiB multipart threshold failed —
    52 of 60 sampled shards on the CA GEDI build, and a deliberate single-shard
    test whose store is missing exactly its two ragged chunks.
  - Both write seams (`_AclWriteObjectStore.set`/`set_if_not_exists` and
    `put_object`) now pass `use_multipart=False` when the target carries the
    ACL, leaving `PutObject` as the only request the twin can issue — a
    property of the handle rather than a list of permitted operations, since
    enumerating that list once and missing `UploadPart` is how this shipped.
    In-account targets are untouched and still multipart.
  - `_SINGLE_PUT_MAX_BYTES` refuses, by key, a payload past S3's 5 GiB
    `PutObject` ceiling; published chunk objects run 1–17 MB.

- the sweep Lambda handler forwards the `partition` and `families` blocks (#527)
  ([#528](https://github.com/englacial/zagg/pull/528))
  - `_handle_sweep` dropped both blocks on the floor, so a fleet `mode="sweep"`
    invoke could never be partitioned and could never be scoped to a family
    subset — `run_sweep` has accepted both since #377/#520, but nothing
    worker-side ever received them. On the `discover` transport every
    "partition" worker therefore derived and swept the WHOLE store; the CA
    ATL03 toc/overview sweep died at the 900 s wall in all 16 workers with
    nothing written. On the inline transport a partitioned pass walked to the
    root with only its own subset, so nodes above the split order were written
    from partial data by racing workers.
  - Absent keys forward as `None`, so an unpartitioned invoke is behaviourally
    unchanged. This adds reach, not new behaviour.
  - Operator note: a partitioned pass writes nothing below the split order and
    defers `finish()`, so the root `coverage.moc`/`coverage.toc` and the
    manifest `pyramid.materialized` update are still owed by a subsequent
    partition-less pass. Pick the width so that
    `leaves x s_per_leaf / partitions < 900 s`, rounded up to a power of four.
- published-bucket store handles no longer 403 on reads: the canned ACL rides a
  separate write handle (#522) ([#523](https://github.com/englacial/zagg/pull/523))
  - The `x-amz-acl: bucket-owner-full-control` header that issue #495 attached
    to Source Cooperative handles rode obstore's `client_options.default_headers`,
    and obstore signs default headers on every request except `ListObjectsV2`.
    S3 rejects an unsigned `x-amz-acl` outright, so every handle that both
    published and listed died on its first LIST — the per-leaf template guard
    and the client status poller included, which blocked all source.coop fleet
    builds and sweeps.
  - `zagg.store` now opens two handles for such a target: the one callers hold is
    clean, and an ACL-bearing twin takes the object-creating requests.
    `open_store` returns a Zarr store that routes its own writes; raw-obstore
    writes go through the new `zagg.store.put_object`. Ownership semantics are
    unchanged — every write still carries `bucket-owner-full-control`, signed.
  - Reads through `open_object_store` with explicit credentials (the issue #223
    consumer-input channel, e.g. `temporal.open_dataset`'s NetCDF branch) now
    send no ACL header at all rather than an inert one.

- rename parent_morton event field to shard_key (#24) ([#42](https://github.com/englacial/zagg/pull/42)) by @espg
- Concurrency-aware Lambda orchestrator: pre-flight probe + FD-exhaustion guard ([#41](https://github.com/englacial/zagg/pull/41)) by @espg
- drop shapely as an intersection backend (#36) ([#39](https://github.com/englacial/zagg/pull/39)) by @espg
- CLAUDE.md: per-issue claude/ branches, multi-PR, and PR label states ([#37](https://github.com/englacial/zagg/pull/37)) by @espg
- docs: mark SSO execution-role path out of date ([#35](https://github.com/englacial/zagg/pull/35)) by @espg
- sort/hash grouping refactor (#30) ([#33](https://github.com/englacial/zagg/pull/33)) by @espg
- Rectilinear grid: chunk-driven auto-padding + run enablement ([#32](https://github.com/englacial/zagg/pull/32)) by @espg

## [0.46.0] - 2026-08-17

- unindexed shard-map builds cover at `parent_order` and intersect before decoding records (#445) ([#447](https://github.com/englacial/zagg/pull/447)) by @espg
  - An unpinned unindexed HEALPix mortie `swath` build now covers at the output
    grid's `parent_order` instead of its chunk order, so `metadata["mortie_order"]`
    in newly built manifests records the shard order (e.g. 9, not 13). Assignment
    is measurably identical at the production order pairs (the
    `bench/neon_order_sweep.py` invariant, verified at 555,867 granules); at
    coarse grids the new default is a documented conservative superset, never a
    subset. The order an explicit `mortie_order=` pin resolves to is unchanged
    (it is still honored literally and still validated against `parent_order`).
  - Unindexed builds cover from the catalog's WKB column and intersect before
    materializing granule records. At clone scale (555,867 granules) the
    unpinned default — the case the bullet above changes — goes 1,075 s -> 39 s
    (~27x), no longer covering every footprint at the chunk order to answer a
    shard-order question; a build that already pinned `mortie_order=9` goes
    86.6 s -> 39.3 s (~2.2x).
  - Disclosed: an explicit `mortie_order=` pin always covers live, indexed or not,
    so a MultiPolygon footprint assigns as a union-of-parts superset; single-part
    CMR granules are unaffected.

## [0.3.0] - 2026-06-11

- Add bring-your-own-role path for IAM-constrained deploys; creds handling for external s3 bucket writes ([#27](https://github.com/englacial/zagg/pull/27)) by @espg

## [0.2.2] - 2026-06-10

- spherely install / distribution packaging fixes

## [0.2.1] - 2026-06-10

### Catalog API reconcile ([#24](https://github.com/englacial/zagg/issues/24))

- Split catalog construction into **fetch** (`zagg.catalog.sources`: `Query`,
  `CMRSource`, `Catalog`) and **shard-map build** (`zagg.catalog.shardmap.ShardMap`).
  `Catalog` is a reusable stac-geoparquet artifact fetched from CMR-STAC.
- `ShardMap` is a self-contained JSON manifest with `{id, s3, https}` granule
  records (endpoint chosen at run time via `data_source.driver`) and the build
  `grid.signature()`; the runner refuses a shard map built for a different grid.
- **Single source of truth:** the output grid (including HEALPix `parent_order`)
  is defined entirely by the pipeline config; the CLI is now
  `python -m zagg.catalog --config X.yaml …` and **rectilinear grids get a CLI
  path for the first time**.
- `RectilinearGrid` is backed by `odc.geo.GeoBox`; grids gain `signature()` and
  `nests_with()`, plus a `validate_compatible()` stub for future multi-product
  aggregation. Spherely catalog backend uses the S2 `SpatialIndex`.
- **Removed:** `build_catalog`, the EPSG:3031 / grid-driven catalog paths,
  `query_cmr`, `extract_granule_info`, the `s3_base`/`https_base` URL-rewriter,
  and the `healpy` dependency.

## [0.2.0] - 2026-06-05

- Enable user side infrastructure standup ([#22](https://github.com/englacial/zagg/pull/22)) by @espg
- Spherical geometry backend and dispatch for build_catalog ([#19](https://github.com/englacial/zagg/pull/19)) by @espg
- Implementing #17 api redesign ([#18](https://github.com/englacial/zagg/pull/18)) by @espg
- WIP: add design doc for generalized output ([#17](https://github.com/englacial/zagg/pull/17)) by @maxrjones

## [0.1.0] - 2026-04-20

- setting up publishing, library rename ([#16](https://github.com/englacial/zagg/pull/16)) by @espg
- Python API for notebook/JupyterHub use (#13) ([#14](https://github.com/englacial/zagg/pull/14)) by @espg
- Config-driven data source for process_morton_cell (Phase 1) ([#10](https://github.com/englacial/zagg/pull/10)) by @espg
- Decouple ICESat-2 for general CMR queries ([#9](https://github.com/englacial/zagg/pull/9)) by @espg
- Lambda function based orchestration for horizontal scaling of aggregations ([#1](https://github.com/englacial/zagg/pull/1)) by @espg
- Update lambda function to write directly to zarr ([#6](https://github.com/englacial/zagg/pull/6)) by @maxrjones
