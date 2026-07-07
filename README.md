# zagg - Multi-resolution Aggregation

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks)

Aggregate point observations to multi-resolution grids using HEALPix spatial indexing and serverless compute.

## Overview

zagg aggregates sparse point data (e.g., ICESat-2 ATL06 elevation measurements) to gridded products using HEALPix/morton spatial indexing. Processing runs in parallel on AWS Lambda — each worker handles one spatial cell independently, writing to a shared [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) store following the [DGGS convention](https://github.com/zarr-conventions/dggs).

## Features

- **Pre-computed granule catalogs** — query CMR once, process many times
- **Morton-based spatial indexing** — HEALPix nested scheme for hierarchical grids
- **Massive parallelism** — tested with up to 1,700 concurrent Lambda workers
- **Direct S3 access** — h5coro reads HDF5 via byte-range requests, no downloads
- **Cost-effective** — ~$0.006/cell (~$2 per full Antarctica run on ARM64)

## End-to-End Workflow

### Step 1: Build a Granule Catalog

Query NASA's CMR-STAC to build a shard map of grid cells to granules. The grid
comes from the **same pipeline config** the aggregator uses (`--config`), so the
shard map can't be built against a different grid than the run.

```bash
# Install the catalog extra (STAC fetch + shard-map build). The geometry
# backend defaults to `auto`: exact-S2 spherely if its fork is installed (used
# for all grids), else mortie (HEALPix MOC); rectilinear grids require spherely.
pip install 'zagg[catalog]'

# Optional: the exact-S2 spherely SpatialIndex backend is a fork not on PyPI
# (benbovy/spherely#118) — install it separately (pick the wheel for your
# python/platform from the release assets):
# pip install "spherely @ https://github.com/espg/spherely/releases/download/v0.1.1-spatialindex/spherely-0.1.1+spatialindex-cp312-cp312-manylinux_2_28_x86_64.whl"

# ICESat-2 convenience — cycle number computes dates automatically:
uv run python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon my_region.geojson

# General — explicit date range and a bbox:
uv run python -m zagg.catalog \
    --config atl06.yaml --short-name ATL06 \
    --start-date 2024-01-06 --end-date 2024-04-07 \
    --polygon my_region.geojson
```

`--polygon` drives both the CMR query bbox and the coverage mask; `--bbox` gives
the query box directly. Each granule record keeps both its S3 and HTTPS hrefs;
the run picks one via `data_source.driver`.

Output: `shardmap_ATL06_2024-01-06_2024-04-07.json`

See [Catalog API](docs/api/catalog.md) for full options.

### Step 2: Deploy the Lambda Function

**Quick standup (CloudFormation).** Stand up the whole backend — IAM role,
dependency layer, and function — in your own AWS account from the pre-built
release zips:

```bash
OUTPUT_BUCKET=my-results-bucket bash deployment/aws/stand_up.sh
# don't have the results bucket yet? add CREATE_BUCKET=true
# deploying outside us-west-2? add REGION=... STAGING_BUCKET=a-bucket-you-own-in-that-region
```

In **us-west-2** the stack reads the Lambda code straight from the public
distribution bucket (`s3://sliderule-public-cors/<minor>/`) — no staging bucket
of your own needed. Outside us-west-2, CloudFormation requires the code in a
same-region bucket, so pass a `STAGING_BUCKET` you own and the zips are copied
there from the distribution bucket first. Deploys
[`deployment/aws/template.yaml`](deployment/aws/template.yaml); the artifacts
are keyed by zagg minor version (derive from your install, pin with
`LAMBDA_VERSION=0.N`, or use `LAMBDA_VERSION=latest` for the newest published
minor). Override `ARCH` for x86_64.

**Build from source** (maintainers, or to customize the layer):

```bash
# Build the function package
bash deployment/aws/build_function.sh

# Build the dependency layer (ARM64)
bash deployment/aws/build_layer.sh arm64

# Deploy (updates an already-deployed function from CI artifacts)
bash deployment/aws/deploy.sh
```

See [Lambda Deployment](docs/deployment/lambda.md) and [ARM64 Build Guide](docs/deployment/arm64.md).

### Step 3: Run Processing

Processing reads a pipeline config YAML (data source, aggregation, output store) and a granule catalog. Run locally or dispatch to Lambda.

```bash
# Local processing (write to local Zarr):
uv run python -m zagg --config atl06.yaml --catalog catalog.json --store ./output.zarr

# Local processing (write to S3):
uv run python -m zagg --config atl06.yaml --catalog catalog.json --store s3://bucket/output.zarr

# Lambda dispatch (requires deployed Lambda function):
uv run python deployment/aws/invoke_lambda.py \
    --config atl06.yaml --catalog catalog.json

# Test with a few cells:
uv run python -m zagg --config atl06.yaml --catalog catalog.json --max-cells 5

# Dry run:
uv run python -m zagg --config atl06.yaml --catalog catalog.json --dry-run
```

The store path and output grid parameters are defined in the YAML config (`output.store`, `output.grid.child_order`) and can be overridden via `--store` on the command line.

**Read backends** (`data_source.index`, issue #160/#170). Reads go through a pluggable chunk-index backend; the default is the compiled fast path:

- `inline` *(default)* — builds each granule's chunk map at read time (metadata-only, ~1 ranged GET) and decodes through the compiled [h5coro-hidefix](https://github.com/espg/h5coro-hidefix) reader. Works for every chunked-HDF5 data source — planned (ATL03-style hierarchical) and flat alike; datasets the compiled reader cannot serve degrade to h5coro per dataset. With `write_back: true` + `store:`, persists the chunk maps as granule-keyed parquet manifests, populating the sidecar cache.
- `sidecar` — fetches precomputed manifests from `store:` instead of walking metadata; `on_miss: fallback | error | build` controls behavior for uncovered granules (`build` self-populates the store). Fastest once the cache exists.
- `hierarchical` — the pure-Python h5coro read, byte-compatible baseline; select explicitly to pin it (the benchmark matrix's uncached column does).

`data_source.read_workers` (default 8) bounds the per-worker read fan-out on the compiled paths: each in-flight read overlaps S3 latency and decodes with the GIL released. Peak worker memory grows with width — dial down for dense shards.

### Step 4: Visualize Results

The output Zarr is a public DGGS dataset. The included notebook rasterizes HEALPix cells to a polar stereographic grid for fast rendering with `imshow`.

```bash
uv run jupyter notebook notebooks/rasterized_zarr.ipynb
```

Adjust `GRID_SPACING` in the notebook to control output resolution.

## Example Notebooks

The notebooks under `notebooks/` run on [Binder](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks) — no install, no credentials. They install `zagg[analysis]` via the `.binder/` conda config and read only synthetic in-notebook data or the **anonymous, public** [source.coop](https://source.coop/englacial/zagg/benchmarks) benchmark store.

| Notebook | What it shows | Binder |
|----------|---------------|--------|
| `custom_aggregations.ipynb` | Config-driven aggregation API on synthetic data | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/custom_aggregations.ipynb) |
| `rasterized_zarr.ipynb` | Rasterize the published HEALPix store to an 8 km polar-stereo grid | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/rasterized_zarr.ipynb) |
| `jupyterhub_example.ipynb` | Drive the API from a science hub; read & visualize a published result | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/englacial/zagg/main?urlpath=lab/tree/notebooks/jupyterhub_example.ipynb) |
| `cryocloud_example.ipynb` | End-to-end ISMIP6 read + **AWS Lambda fan-out** on CryoCloud | **not Binder-runnable** (needs live AWS + Earthdata credentials) |

`cryocloud_example.ipynb` is the only Lambda demo; it dispatches to a deployed AWS Lambda and reads private-account S3 via the CryoCloud IRSA role, so it cannot run on Binder.

## Project Structure

```
zagg/
├── src/zagg/              # Main package (cloud-agnostic)
│   ├── __main__.py        # Local processing runner (python -m zagg)
│   ├── config.py          # YAML pipeline configuration
│   ├── processing.py      # Core aggregation pipeline
│   ├── catalog.py         # CMR query + catalog building
│   ├── schema.py          # Output schema + Zarr template
│   ├── store.py           # Store factory (local or S3)
│   ├── auth.py            # NASA Earthdata authentication
│   └── configs/           # Built-in pipeline configs (atl06.yaml)
├── deployment/            # Cloud-specific deployment
│   └── aws/               # Lambda handler, orchestrator, build scripts
├── notebooks/             # Visualization
├── docs/                  # Documentation
└── tests/                 # Test suite
```

## Documentation

- **[Architecture](docs/design/architecture.md)** — design philosophy, end-to-end flow diagram, key decisions
- **[Schema](docs/design/schema.md)** — aggregation dispatch, extending with new statistics
- **[API Reference](docs/api/catalog.md)** — catalog, processing, schema, auth modules
- **[Lambda Deployment](docs/deployment/lambda.md)** — AWS setup and production use
- **[ARM64 Build Guide](docs/deployment/arm64.md)** — building Lambda layers for ARM64

## Development

```bash
# Install
uv sync --all-groups

# Run tests
uv run pytest

# Lint
uv run ruff check src/
```

Requires Python >= 3.12, [uv](https://docs.astral.sh/uv/), AWS credentials (for Lambda), and a [NASA Earthdata](https://urs.earthdata.nasa.gov/) account (for data access).

## Performance

| Metric | Value |
|--------|-------|
| Execution time | 2–3 min average per cell |
| Memory | 2 GB configured, 1–1.5 GB typical |
| Throughput | Tested with up to 1,700 concurrent workers |
| Cost | ~$0.006/cell (~$2 per full Antarctica run on ARM64) |

## License

MIT — see LICENSE file.
