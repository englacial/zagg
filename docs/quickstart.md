# Quickstart

Zero-to-working setup for `magg`.

## Prerequisites

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- [NASA Earthdata account](https://urs.earthdata.nasa.gov/) (free)
- For Lambda deployment: AWS account, Docker (for building layers)

## Installation

```bash
git clone https://github.com/englacial/magg.git
cd magg
uv sync --all-groups
```

## NASA Earthdata Authentication

`magg` reads ICESat-2 ATL06 data from NSIDC's S3 buckets in AWS `us-west-2`.
This requires NASA Earthdata credentials. Three methods, in order of precedence:

**1. Environment variables**

```bash
export EARTHDATA_USERNAME=your_username
export EARTHDATA_PASSWORD=your_password
```

**2. `~/.netrc` file (recommended for persistent use)**

```
machine urs.earthdata.nasa.gov
    login YOUR_USERNAME
    password YOUR_PASSWORD
```

**3. Interactive prompt**

The `earthaccess` library will prompt for credentials as a fallback if neither
of the above is configured.

!!! note
    These are **read** credentials for source data at NSIDC. Output store
    credentials (for writing results to S3) are separate AWS credentials --
    see [Writing to S3](#writing-to-s3-optional) below.

## Building a Catalog

The catalog step queries NASA's CMR API (public, no auth needed) and builds a
morton-cell-to-granule mapping:

```bash
uv run python -m magg.catalog --cycle 22 --parent-order 6
```

This produces a JSON file (e.g., `catalog_ATL06_cycle22_order6.json`) that maps
parent morton cells to the S3 URLs of HDF5 granules containing data for those
cells. The processing step consumes this file.

## Local Processing

The simplest path -- no AWS Lambda needed:

```bash
# Process one cell, write to local Zarr:
uv run python -m magg --config src/magg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json \
    --store ./output.zarr --max-cells 1

# Dry run (shows what would happen, no processing):
uv run python -m magg --config src/magg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json --dry-run
```

## Writing to S3 (Optional)

To write output to S3, set the store path to an S3 URI:

```bash
uv run python -m magg --config src/magg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json \
    --store s3://my-bucket/output.zarr
```

This requires AWS credentials configured via one of:

- `~/.aws/credentials`
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- An IAM role (e.g., on EC2 or Lambda)

## Lambda Deployment (Optional)

For full-scale processing, `magg` dispatches work to AWS Lambda. See
[Lambda Deployment](deployment/lambda.md) for details. The short version:

1. **AWS prerequisites**: IAM role with S3 + Lambda permissions, S3 bucket for output
2. **Build**: `deployment/aws/build_function.sh` and `deployment/aws/build_arm64_layer.sh`
3. **Deploy**: `deployment/aws/deploy.sh`
4. **Run**:
```bash
python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json
```

The function name defaults to `process-morton-cell` but is configurable via
the `MAGG_LAMBDA_FUNCTION_NAME` environment variable.

## Configuration

Pipeline behavior is defined in a YAML config with three required sections:

```yaml
data_source:
  reader: h5coro
  groups: [gt1l, gt1r, gt2l, gt2r, gt3l, gt3r]
  coordinates:
    latitude: "/{group}/land_ice_segments/latitude"
    longitude: "/{group}/land_ice_segments/longitude"
  variables:
    h_li: "/{group}/land_ice_segments/h_li"

aggregation:
  coordinates:
    cell_ids:
      dtype: uint64
      fill_value: 0
  variables:
    count:
      function: len
      source: h_li
      dtype: int32
      fill_value: 0

output:
  grid:
    type: healpix
    indexing_scheme: nested
    child_order: 12

# Optional top-level fields:
catalog: catalog_ATL06_cycle22_order6.json
store: ./output.zarr
```

See `src/magg/configs/atl06.yaml` for a complete example and the
[custom aggregations notebook](https://github.com/englacial/magg/blob/main/notebooks/custom_aggregations.ipynb)
for customization examples.
