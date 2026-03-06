# AWS Lambda

AWS Lambda function for processing ICESat-2 ATL06 data by morton cell.

## Overview

The Lambda function processes a single morton cell (order 6) by:

1. Reading HDF5 files directly from S3 using h5coro (no downloads)
2. Spatial filtering using morton indexing
3. Calculating summary statistics for child cells (order 12)
4. Writing xdggs-enabled Zarr to S3

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Lambda Function (process-morton-cell)                      │
│  ──────────────────────────────────────────────────────────  │
│  Runtime: Python 3.12                                       │
│  Memory: 2048 MB (2 GB)                                     │
│  Timeout: 720s (12 minutes)                                 │
│  ──────────────────────────────────────────────────────────  │
│  Code (~5 MB):                                              │
│    - deployment/aws/lambda_handler.py (AWS wrapper)         │
│    - src/magg/ package (processing, auth, catalog)          │
│  ──────────────────────────────────────────────────────────  │
│  Layer (~70 MB compressed, ~240 MB uncompressed):           │
│    - numpy, pandas, h5coro, mortie, healpy                  │
│    - fastparquet, cramjam, shapely, astropy, earthaccess    │
│    - pydantic-zarr, zarr, obstore, pyarrow                  │
└─────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `deployment/aws/lambda_handler.py` | AWS Lambda wrapper function |
| `src/magg/processing.py` | Cloud-agnostic core processing logic |
| `src/magg/auth.py` | NASA Earthdata authentication helper |
| `src/magg/catalog.py` | CMR granule catalog builder |
| `deployment/aws/invoke_lambda.py` | Orchestration script |
| `deployment/aws/build_arm64_layer.sh` | ARM64 Lambda layer build script |

## Event Payload

```json
{
  "parent_morton": 123456,
  "parent_order": 6,
  "child_order": 12,
  "granule_urls": [
    "s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/2023/12/18/...",
    "s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/007/2023/12/19/..."
  ],
  "s3_bucket": "your-output-bucket",
  "s3_prefix": "atl06/production",
  "s3_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "..."
  }
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `parent_morton` | int | Yes | Morton index of parent cell (order 6) |
| `parent_order` | int | Yes | Order of parent cell (typically 6) |
| `child_order` | int | Yes | Order of child cells for statistics (typically 12) |
| `granule_urls` | list | Yes | Pre-computed list of S3 URLs from catalog |
| `s3_bucket` | str | Yes | S3 bucket for output Zarr files |
| `s3_prefix` | str | Yes | S3 prefix for output Zarr files |
| `s3_credentials` | dict | Yes | S3 credentials from orchestrator |

### S3 Credentials

Credentials are obtained by the orchestrator once before invoking Lambda functions:

```python
from magg.auth import get_nsidc_s3_credentials

# Get credentials (valid for ~1 hour)
s3_creds = get_nsidc_s3_credentials()

# Pass to each Lambda invocation
event = {
    "parent_morton": -6134114,
    "parent_order": 6,
    "child_order": 12,
    "granule_urls": [...],
    "s3_bucket": "output-bucket",
    "s3_prefix": "atl06/production",
    "s3_credentials": s3_creds,
}
```

This approach avoids rate limiting from 1,872 simultaneous NASA logins and eliminates an AWS Secrets Manager dependency.

## Deployment

### Step 1: Create the function package

```bash
cd /path/to/magg

# Create function.zip with handler and magg package
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip magg/ -i "*.py" && cd ..
```

### Step 2: Build and deploy the Lambda layer

See [ARM64 Layer](arm64.md) for building and deploying the Lambda layer.

### Step 3: Create the Lambda function

```bash
aws lambda create-function \
  --function-name process-morton-cell \
  --runtime python3.12 \
  --architectures arm64 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler deployment.aws.lambda_handler.lambda_handler \
  --zip-file fileb://deployment/aws/function.zip \
  --timeout 720 \
  --memory-size 2048 \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:magg-layer-arm64:VERSION
```

### Updating function code

```bash
# Re-create the zip
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip magg/ -i "*.py" && cd ..

# Update the Lambda function
aws lambda update-function-code \
  --function-name process-morton-cell \
  --zip-file fileb://deployment/aws/function.zip
```

## Testing

```bash
# Build a granule catalog
uv run python -m magg.catalog --cycle 22 --parent-order 6

# Dry run with the orchestrator
uv run python deployment/aws/invoke_lambda.py \
  --catalog deployment/data/catalogs/granule_catalog_cycle22_order6.json \
  --dry-run --max-cells 1
```

## Performance

| Metric | Value |
|--------|-------|
| Average execution time | 2--3 minutes per cell |
| Maximum execution time | 10 minutes |
| Lambda timeout | 12 minutes (720s) |
| Configured memory | 2048 MB |
| Typical memory usage | 1--1.5 GB |
| Cold start | 3--5 seconds |

## Cost Estimate

**Per invocation** (180s average, 2 GB memory): ~$0.006

**Full run** (~1,300 cells at order 6): ~$2 including S3 and CloudWatch costs.

## Troubleshooting

!!! warning "Missing s3_credentials"
    Ensure your orchestrator script calls [`get_nsidc_s3_credentials`][magg.auth.get_nsidc_s3_credentials] and passes the credentials to each Lambda invocation.

!!! info "No granules found"
    This is normal for cells outside the data coverage area. The function returns gracefully with `error: "No granules found"`.

!!! warning "S3 write permission denied"
    Check that the Lambda execution role has `s3:PutObject` permission for the output bucket.

!!! warning "Too many open files"
    Decrease max workers (e.g., `--max-workers 50`) or increase ulimit (`ulimit -n 10000`).
