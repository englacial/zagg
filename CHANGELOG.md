# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- rename parent_morton event field to shard_key (#24) ([#42](https://github.com/englacial/zagg/pull/42)) by @espg
- Concurrency-aware Lambda orchestrator: pre-flight probe + FD-exhaustion guard ([#41](https://github.com/englacial/zagg/pull/41)) by @espg
- drop shapely as an intersection backend (#36) ([#39](https://github.com/englacial/zagg/pull/39)) by @espg
- CLAUDE.md: per-issue claude/ branches, multi-PR, and PR label states ([#37](https://github.com/englacial/zagg/pull/37)) by @espg
- docs: mark SSO execution-role path out of date ([#35](https://github.com/englacial/zagg/pull/35)) by @espg
- sort/hash grouping refactor (#30) ([#33](https://github.com/englacial/zagg/pull/33)) by @espg
- Rectilinear grid: chunk-driven auto-padding + run enablement ([#32](https://github.com/englacial/zagg/pull/32)) by @espg

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
