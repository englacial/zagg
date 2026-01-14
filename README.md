# magg - Multi-resolution Aggregation

ICESat-2 ATL06 processing using morton/healpix indexing and AWS Lambda.

## Overview

magg implements a serverless, scalable system for aggregating sparse ICESat-2 ATL06 elevation data to multi-resolution grids using HEALPix spatial indexing. The system processes Antarctic ice sheet data by building spatial trees from leaves (fine resolution) to root (coarse resolution), enabling efficient parallel processing without data duplication.

## Features

- **Serverless AWS Lambda processing** - Python 3.12 on ARM64 for cost efficiency
- **Pre-computed granule catalogs** - Eliminates CMR query rate limiting
- **Morton-based spatial indexing** - HEALPix nested scheme for hierarchical grids
- **Massive parallelism** - Processes 1,872 Antarctic cells with 1,700+ concurrent workers
- **Direct S3 access** - h5coro reads HDF5 without local downloads
- **Cost-effective** - ~$12-15 per full Antarctica run

## Project Structure

```
magg/
├── src/magg/              # Main package (cloud-agnostic)
│   ├── processing.py      # Core processing functions
│   ├── catalog.py         # CMR granule catalog building
│   ├── auth.py            # NASA Earthdata authentication
├── deployment/            # Cloud-specific deployment code
│   ├── aws/               # AWS Lambda implementation
│   │   ├── lambda_handler.py    # AWS Lambda wrapper
│   │   ├── invoke_lambda.py     # AWS orchestrator
│   │   └── build_*_layer.sh     # Layer build scripts
│   ├── layers/            # Pre-built Lambda layers
│   └── data/              # Granule catalogs and results
├── notebooks/             # Analysis and visualization
├── docs/                  # Comprehensive documentation
├── tests/                 # Test suite
└── archive/               # Historical Dask prototype
```

## Quick Start

### Installation

```bash
# Sync dependencies and install package
uv sync

# Install with optional dependency groups
uv sync --all-extras
```

### Build Granule Catalog

```bash
# Query CMR for cycle 22 granules
uv run python -m magg.catalog --cycle 22 --parent-order 6
```

### Run Production Processing (AWS Lambda)

```bash
# Process all cells
uv run python deployment/aws/invoke_lambda.py --catalog deployment/data/catalogs/granule_catalog_cycle22_order6.json

# Test with limited cells
uv run python deployment/aws/invoke_lambda.py --catalog deployment/data/catalogs/granule_catalog_cycle22_order6.json --max-cells 10 --dry-run
```

### Visualize Results

```bash
# Launch Jupyter and open visualization notebook
uv run jupyter notebook notebooks/visualize_production_results.ipynb
```

## Documentation

- **[Architecture Overview](docs/ARCHITECTURE.md)** - Design philosophy and approach
- **[Lambda Deployment](docs/LAMBDA.md)** - AWS setup and production use
- **[ARM64 Build Guide](docs/LAMBDA_ARM64.md)** - Building Lambda layers for ARM64

## Development

### Requirements

- Python >= 3.12
- uv (install from https://docs.astral.sh/uv/)
- AWS credentials (for Lambda deployment)
- NASA Earthdata account (for data access)

```

### Building Lambda Layers

```bash
# For ARM64 (on Apple Silicon)
bash deployment/aws/build_arm64_layer.sh

# For x86_64
bash deployment/aws/build_layer_v14.sh x86_64
```

## Performance

- **Execution time**: 2-3 minutes average per cell
- **Memory**: 2048 MB configured, 1-1.5 GB typical usage
- **Throughput**: 1,700 concurrent Lambda invocations
- **Cost**: ~$0.006 per cell with ARM64

## License

MIT License - see LICENSE file

## Acknowledgments

Built for processing ICESat-2 ATL06 Land Ice Height data from NASA's ICESat-2 mission.
