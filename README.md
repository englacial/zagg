# zagg - Multi-resolution Aggregation

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

Query NASA's CMR to build a mapping of spatial cells to granule S3 URLs.

```bash
# ICESat-2 convenience — cycle number computes dates automatically:
uv run python -m zagg.catalog --cycle 22 --parent-order 6

# General — explicit date range and spatial polygon:
uv run python -m zagg.catalog \
    --start-date 2024-01-06 --end-date 2024-04-07 \
    --short-name ATL06 \
    --polygon my_region.geojson \
    --parent-order 6
```

When `--polygon` is provided, the bounding box for the CMR query is computed automatically from the polygon's extent, and `morton_coverage` uses the polygon for cell discovery. When no polygon is given, Antarctic drainage basins are used as the default.

Output: `catalog_ATL06_2024-01-06_2024-04-07_order6.json`

See [Catalog API](docs/api/catalog.md) for full options.

### Step 2: Deploy the Lambda Function

Build and deploy the Lambda function and its dependency layer.

```bash
# Build the function package
bash deployment/aws/build_function.sh

# Build the dependency layer (ARM64)
bash deployment/aws/build_arm64_layer.sh

# Deploy
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

### Step 4: Visualize Results

The output Zarr is a public DGGS dataset. The included notebook rasterizes HEALPix cells to a polar stereographic grid for fast rendering with `imshow`.

```bash
uv run jupyter notebook notebooks/rasterized_zarr.ipynb
```

Adjust `GRID_SPACING` in the notebook to control output resolution (default 2 km).

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
