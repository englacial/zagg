# Quickstart

Zero-to-working setup for `zagg`.

## Prerequisites

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) package manager
- [NASA Earthdata account](https://urs.earthdata.nasa.gov/) (free)
- For Lambda deployment: AWS account, Docker (for building layers)

## Installation

```bash
git clone https://github.com/englacial/zagg.git
cd zagg
uv sync --all-groups
```

## NASA Earthdata Authentication

`zagg` reads ICESat-2 ATL06 data from NSIDC's S3 buckets in AWS `us-west-2`.
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

The catalog step queries NASA's CMR-STAC (public, no auth needed) and builds a
shard-to-granule mapping for the grid in your config:

```bash
uv run python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson
```

This produces a JSON file (e.g., `shardmap_ATL06_2024-01-06_2024-04-07.json`) that maps
parent morton cells to the S3 URLs of HDF5 granules containing data for those
cells. The processing step consumes this file.

To inspect the chunking interactively -- shard outlines, granule footprints,
and a grid that appears on zoom -- use the shard-map viewer
(`pip install zagg[viz]`). See the
[shard-map viewer notebook](https://github.com/englacial/zagg/blob/main/notebooks/shardmap_viewer.ipynb),
which runs on a synthetic example (no network needed) and includes manual
in-browser verification instructions.

## Local Processing

The simplest path -- no AWS Lambda needed:

```bash
# Process one cell, write to local Zarr:
uv run python -m zagg --config src/zagg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json \
    --store ./output.zarr --max-cells 1

# Dry run (shows what would happen, no processing):
uv run python -m zagg --config src/zagg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json --dry-run
```

## Writing to S3 (Optional)

To write output to S3, set the store path to an S3 URI:

```bash
uv run python -m zagg --config src/zagg/configs/atl06.yaml \
    --catalog catalog_ATL06_cycle22_order6.json \
    --store s3://my-bucket/output.zarr
```

This requires AWS credentials configured via one of:

- `~/.aws/credentials`
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- An IAM role (e.g., on EC2 or Lambda)

## Lambda Deployment (Optional)

For full-scale processing, `zagg` dispatches work to AWS Lambda. See
[Lambda Deployment](deployment/lambda.md) for details. The short version:

1. **AWS prerequisites**: IAM role with S3 + Lambda permissions, S3 bucket for output
2. **Build**: `deployment/aws/build_function.sh` and `deployment/aws/build_layer.sh`
3. **Deploy**: `deployment/aws/deploy.sh`
4. **Run**:
```bash
python deployment/aws/invoke_lambda.py --config atl06.yaml --catalog catalog.json
```

The function name defaults to `process-shard` but is configurable via
the `ZAGG_LAMBDA_FUNCTION_NAME` environment variable.

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

See `src/zagg/configs/atl06.yaml` for a complete example and the
[custom aggregations notebook](https://github.com/englacial/zagg/blob/main/notebooks/custom_aggregations.ipynb)
for customization examples.
