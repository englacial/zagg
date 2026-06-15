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
│  Lambda Function (process-shard)                      │
│  ──────────────────────────────────────────────────────────  │
│  Runtime: Python 3.12                                       │
│  Memory: 2048 MB (2 GB)                                     │
│  Timeout: 720s (12 minutes)                                 │
│  ──────────────────────────────────────────────────────────  │
│  Code (~5 MB):                                              │
│    - deployment/aws/lambda_handler.py (AWS wrapper)         │
│    - src/zagg/ package (processing, auth, catalog)          │
│  ──────────────────────────────────────────────────────────  │
│  Layer (~70 MB compressed, ~240 MB uncompressed):           │
│    - numpy, pandas, h5coro, mortie, pyproj, odc-geo         │
│    - fastparquet, cramjam, shapely, astropy, earthaccess    │
│    - pydantic-zarr, zarr, obstore, pyarrow                  │
└─────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `deployment/aws/lambda_handler.py` | AWS Lambda wrapper function |
| `src/zagg/processing.py` | Cloud-agnostic core processing logic |
| `src/zagg/auth.py` | NASA Earthdata authentication helper |
| `src/zagg/catalog/` | CMR/STAC shard-map (granule catalog) builder (`python -m zagg.catalog`) |
| `deployment/aws/invoke_lambda.py` | Orchestration script |
| `deployment/aws/build_layer.sh` | Lambda layer build script (`x86_64`/`arm64`) |

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
  "store_path": "s3://your-output-bucket/atl06/production.zarr",
  "s3_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "..."
  },
  "output_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "...",
    "endpointUrl": "https://...",
    "region": "us-west-2"
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
| `store_path` | str | Yes | Output Zarr store path (e.g. `s3://bucket/prefix.zarr`) |
| `s3_credentials` | dict | Yes | NSIDC S3 credentials for reading source data |
| `output_credentials` | dict | No | Explicit credentials for *writing* the output store. Omit to use the execution role (in-account writes). Supply to write an external / S3-compatible target. Keys: `accessKeyId`, `secretAccessKey`, optional `sessionToken`/`endpointUrl`/`region`. |

!!! note "Shard vs. `parent_order` naming"
    The unit of work is a **shard** — one parent (order-6) HEALPix cell. The
    orchestrator and the catalog use that vocabulary (`python -m zagg.catalog`
    emits a shard map with `shard_keys` + a `grid_signature`). The Lambda
    **event** schema, however, still carries the HEALPix-shaped field names
    `parent_morton` / `parent_order` / `child_order` shown above (the handler
    requires them — see `deployment/aws/lambda_handler.py`). A grid-neutral
    rename of those event fields is tracked in
    [#24](https://github.com/englacial/zagg/issues/24), not done here, so the
    field names above are the current, accurate ones.

### S3 Credentials

Credentials are obtained by the orchestrator once before invoking Lambda functions:

```python
from zagg.auth import get_nsidc_s3_credentials

# Get credentials (valid for ~1 hour)
s3_creds = get_nsidc_s3_credentials()

# Pass to each Lambda invocation
event = {
    "parent_morton": -6134114,
    "parent_order": 6,
    "child_order": 12,
    "granule_urls": [...],
    "store_path": "s3://output-bucket/atl06/production.zarr",
    "s3_credentials": s3_creds,
}
```

This approach avoids rate limiting from 1,872 simultaneous NASA logins and eliminates an AWS Secrets Manager dependency.

### Output Credentials (external write targets)

By default the function writes the output store with its **execution role**
against the in-account bucket; omit `output_credentials` entirely to keep this
behavior. To write an **external or S3-compatible target** (another account, or
e.g. source.coop) without changing the execution role, supply
`output_credentials` in the event — symmetric to how `s3_credentials` injects
read credentials:

```python
from zagg import load_config, agg

results = agg(
    config, catalog="catalog.json", backend="lambda",
    store="s3://us-west-2.opendata.source.coop/org/dataset.zarr",
    output_credentials={  # runtime-only; never store in config/YAML
        "accessKeyId": "ASIA...",
        "secretAccessKey": "...",
        "sessionToken": "...",        # optional
        # "endpointUrl": "https://...",  # optional: R2/MinIO etc.
        # "region": "us-west-2",         # optional
    },
)
```

From the CLI, point `--output-creds` at a JSON file holding that dict (keeps
secrets out of shell history):

```bash
python -m zagg --config atl06.yaml --catalog catalog.json --backend lambda \
  --store s3://us-west-2.opendata.source.coop/org/dataset.zarr \
  --output-creds /path/to/output-creds.json
```

The non-secret `endpoint_url` / `region` may also be set in the config's
`output:` section (overridable at runtime); **credentials are runtime-only**.
source.coop uses the standard AWS S3 endpoint with injected STS credentials —
`endpointUrl` is only needed for non-AWS S3-compatible stores. Dotted bucket
names (e.g. `us-west-2.opendata.source.coop`) and custom endpoints use
path-style addressing automatically.

## Deployment

### Recommended: CloudFormation standup

The recommended way to stand up the backend in a fresh AWS account is the
committed CloudFormation template, driven by `stand_up.sh`, which creates the
execution role, dependency layer, and function in one stack:

```bash
OUTPUT_BUCKET=my-results-bucket bash deployment/aws/stand_up.sh
```

See **[Standing Up the Backend](standup.md)** for the full walkthrough: what the
script does, the parameter/environment-variable reference, cross-region staging,
and teardown. By default (`CreateExecutionRole=true`) the stack creates the IAM
execution role for you; the only exception is an account whose deploy identity
*cannot* create IAM roles (e.g. an AWS SSO "power user" set) — see
[Execution Role](execution-role.md) for that IAM-constrained, legacy/unverified
path.

### Legacy / manual deploy {#legacy-manual-deploy}

!!! warning "Not the recommended path"
    The steps below hand-assemble the function zip and create/update the Lambda
    with raw `aws lambda` calls. They are kept for understanding what the
    template builds and for one-off tweaks, but the
    **[CloudFormation standup](standup.md)** above is the preferred, reproducible
    way to deploy. The maintainer in-place code updater
    `deployment/aws/deploy.sh` (pulls the latest CI artifacts and runs
    `aws lambda update-function-code`) is a convenience over the manual
    `update-function-code` step; it updates an already-deployed function and does
    not create the role/function/bucket.

#### Step 1: Create the function package

```bash
cd /path/to/zagg

# Create function.zip with handler and zagg package
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip zagg/ -i "*.py" && cd ..
```

#### Step 2: Build and deploy the Lambda layer

See [ARM64 Layer](arm64.md) for building and deploying the Lambda layer.

#### Step 3: Create the Lambda function

```bash
aws lambda create-function \
  --function-name process-shard \
  --runtime python3.12 \
  --architectures arm64 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://deployment/aws/function.zip \
  --timeout 720 \
  --memory-size 2048 \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:zagg-layer-arm64:VERSION
```

#### Updating function code

```bash
# Re-create the zip
zip -j deployment/aws/function.zip deployment/aws/lambda_handler.py && \
  cd src && zip -ur ../deployment/aws/function.zip zagg/ -i "*.py" && cd ..

# Update the Lambda function
aws lambda update-function-code \
  --function-name process-shard \
  --zip-file fileb://deployment/aws/function.zip
```

## Testing

```bash
# Raise the open-file limit before fanning out: each concurrent worker holds
# one socket to the Lambda endpoint, and the default soft limit (often 256)
# would otherwise cap concurrency. See "Concurrency, workers, and file
# descriptors" below.
ulimit -n 8192

# Build a shard map
uv run python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \
    --polygon antarctica.geojson

# Test locally first (no Lambda required)
uv run python -m zagg --config atl06.yaml --catalog catalog.json \
  --store ./test.zarr --max-cells 1

# Dry run with the Lambda orchestrator
uv run python deployment/aws/invoke_lambda.py \
  --config atl06.yaml --catalog catalog.json --dry-run
```

## Concurrency, workers, and file descriptors

The Lambda backend fans out one synchronous `invoke` per cell across a thread
pool, and each in-flight worker holds an open socket to the Lambda endpoint.
Two limits bound how many can run at once, and the orchestrator checks both
**before** dispatch so cells are never silently dropped:

- **Open file descriptors (`ulimit -n`).** If concurrent workers exceed the
  process's open-file soft limit (256 on stock macOS / many Linux shells),
  invokes fail with `OSError: [Errno 24] Too many open files` — a client-side
  failure AWS never sees. The runner derives a safe ceiling from the soft limit
  and surfaces errno-24 with actionable guidance instead of a raw connection
  error. Raise the limit before a large run: `ulimit -n 8192`.
- **Account Lambda concurrency.** The runner reads the account
  `ConcurrentExecutions` ceiling and current usage (CloudWatch) and clamps
  workers to the available headroom (5% padding, floored at 100 free slots), so
  a run can't saturate the account pool and throttle itself or other Lambda
  activity. This degrades gracefully if the dispatch role lacks
  `lambda:GetAccountSettings` / `cloudwatch:GetMetricStatistics` — it then
  bounds workers by the FD limit alone.

Keep `--max-workers ≤ min(ulimit -n − headroom, account concurrency)`. The
orchestrator enforces this automatically; setting `ulimit -n` higher simply
raises the FD ceiling it can use.

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
    Ensure your orchestrator script calls [`get_nsidc_s3_credentials`][zagg.auth.get_nsidc_s3_credentials] and passes the credentials to each Lambda invocation.

!!! info "No granules found"
    This is normal for cells outside the data coverage area. The function returns gracefully with `error: "No granules found"`.

!!! warning "S3 write permission denied"
    Check that the Lambda execution role has `s3:PutObject` permission for the output bucket.

!!! warning "Too many open files"
    `[Errno 24] Too many open files` means concurrent workers exceeded the
    open-file soft limit and cells would be dropped. Raise it (`ulimit -n 8192`)
    or lower `--max-workers`. See "Concurrency, workers, and file descriptors"
    above — the orchestrator now clamps workers to the FD and account-concurrency
    limits automatically.
