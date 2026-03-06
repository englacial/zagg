# magg

Multi-resolution aggregation for point observations using morton/healpix indexing.

## Overview

`magg` aggregates sparse point data (e.g., ICESat-2 ATL06 elevation measurements) into gridded products using HEALPix/morton spatial indexing. It is designed for massively parallel execution on commodity cloud workers (e.g., AWS Lambda), producing [Zarr v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) output following the [DGGS convention](https://github.com/zarr-conventions/dggs).

The library is organized into four modules:

- **`magg.catalog`**: CMR granule catalog builder — queries NASA CMR with date ranges, product names, and spatial polygons, then maps parent morton cells to S3 granule URLs
- **`magg.schema`**: Output schema definition via [`CellStatsSchema`][magg.schema.CellStatsSchema], Zarr template creation, and derived constants
- **`magg.processing`**: Core aggregation pipeline — reading HDF5, spatial filtering, statistics calculation, and Zarr writing
- **`magg.auth`**: NASA Earthdata authentication for S3 access to NSIDC data

## End-to-End Workflow

### 1. Build a granule catalog

```bash
# ICESat-2 cycle (convenience):
uv run python -m magg.catalog --cycle 22 --parent-order 6

# General (date range + polygon):
uv run python -m magg.catalog \
    --start-date 2024-01-06 --end-date 2024-04-07 \
    --polygon my_region.geojson --parent-order 6
```

See [Catalog API](api/catalog.md) for full options.

### 2. Run production processing

```bash
uv run python deployment/aws/invoke_lambda.py \
    --catalog catalog_ATL06_cycle22_order6.json
```

See [Lambda Deployment](deployment/lambda.md) for setup and configuration.

### 3. Visualize results

```bash
uv run jupyter notebook notebooks/rasterized_zarr.ipynb
```

## Design Philosophy

1. **Data selection is declarative** — query CMR with date ranges and polygons, not file paths
2. **Aggregation doesn't duplicate the at-rest data** — source data is fetched for processing but discarded after aggregation
3. **Build leaves-to-root** — invert the tree construction order to enable parallel processing with small workers

## Quick example

```python
from magg.processing import process_morton_cell
from magg.auth import get_nsidc_s3_credentials

# Authenticate once
creds = get_nsidc_s3_credentials()

# Process a single parent cell
df_out, metadata = process_morton_cell(
    parent_morton=-6134114,
    parent_order=6,
    child_order=12,
    granule_urls=["s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/..."],
    s3_credentials=creds,
)
```

## Contributing

1. Clone the repository: `git clone https://github.com/englacial/magg.git`
2. Install development dependencies: `uv sync --all-groups`
3. Run the test suite: `uv run pytest`

## License

`magg` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
