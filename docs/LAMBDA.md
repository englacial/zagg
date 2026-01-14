# Lambda Deployment

AWS Lambda function for processing ICESat-2 ATL06 data by morton cell.

## Overview

This Lambda function processes a single morton cell (order 6) by:
1. Querying NASA CMR for ATL06 granules intersecting the cell
2. Reading HDF5 files directly from S3 using h5coro (no downloads)
3. Spatial filtering using morton indexing
4. Calculating summary statistics for child cells (order 12)
5. Writing xdggs-enabled zarr to S3

## Files

- **`deployment/aws/lambda_handler.py`**: AWS Lambda wrapper function (deployed to Lambda)
- **`src/magg/processing.py`**: Cloud-agnostic core processing logic
- **`src/magg/auth.py`**: NASA Earthdata authentication helper
- **`deployment/aws/build_arm64_layer.sh`**: Script to build the Lambda layer for ARM64
- **`deployment/aws/build_layer_v14.sh`**: Script to build the Lambda layer for x86_64
- **`src/magg/catalog.py`**: CMR granule catalog builder
- **`deployment/aws/invoke_lambda.py`**: Orchestration script for invoking Lambda functions
- **[`LAMBDA_ARM64.md`](LAMBDA_ARM64.md)**: Instructions for building ARM64 layer

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Lambda Function (process-morton-cell)                      │
│  ──────────────────────────────────────────────────────────  │
│  Runtime: Python 3.12                                        │
│  Memory: 2048 MB (2 GB)                                      │
│  Timeout: 720s (12 minutes)                                  │
│  ──────────────────────────────────────────────────────────  │
│  Code (~5 MB):                                               │
│    - deployment/aws/lambda_handler.py (AWS wrapper)          │
│    - src/magg/ package (processing, auth, catalog)           │
│  ──────────────────────────────────────────────────────────  │
│  Layer (57 MB compressed, 227 MB uncompressed):              │
│    - numpy, pandas, h5coro, mortie, healpy                   │
│    - fastparquet, cramjam, shapely, astropy, earthaccess     │
└─────────────────────────────────────────────────────────────┘
```

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
| `s3_bucket` | str | Yes | S3 bucket for output parquet files |
| `s3_prefix` | str | Yes | S3 prefix for output parquet files |
| `s3_credentials` | dict | Yes | S3 credentials from orchestrator (see below) |

### S3 Credentials

The `s3_credentials` are obtained by the orchestrator ONCE before invoking Lambda functions:

```python
from magg.auth import get_nsidc_s3_credentials

# Get credentials (valid for ~1 hour)
s3_creds = get_nsidc_s3_credentials()

# Pass to each Lambda invocation
event = {
    "parent_morton": -6134114,
    "parent_order": 6,
    "child_order": 12,
    "granule_urls": [...],  # From pre-built catalog
    "s3_bucket": "output-bucket",
    "s3_prefix": "atl06/production",
    "s3_credentials": s3_creds,
}
```

This approach:
- Avoids rate limiting from 1,872 simultaneous NASA logins
- Eliminates AWS Secrets Manager dependency
- Simplifies deployment

## Return Value

```json
{
  "statusCode": 200,
  "body": {
    "parent_morton": -6134114,
    "cells_with_data": 342,
    "total_obs": 15234,
    "zarr_path": "s3://bucket/prefix/-6134114.zarr",
    "error": null,
    "duration_s": 145.3,
    "granule_count": 12,
    "files_processed": 12
  }
}
```

## AWS Resources Required

### 1. IAM Role

The Lambda function needs an execution role with these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-output-bucket/*",
        "arn:aws:s3:::your-output-bucket"
      ]
    }
  ]
}
```

### 2. S3 Bucket

Create an S3 bucket for zarr outputs:

```bash
aws s3 mb s3://your-output-bucket
```

## Deployment

### Step 1: Create the function package

Since all dependencies are in the Lambda layer, the function package needs the handler and the magg package:

```bash
cd /path/to/magg

# Create function.zip with handler and magg package
zip -r function.zip deployment/aws/lambda_handler.py src/magg/
```

This creates a small (~20 KB) `function.zip` with the Lambda handler and core package code.

### Step 2: Build and deploy the Lambda layer

See [LAMBDA_ARM64.md](LAMBDA_ARM64.md) for building and deploying the Lambda layer. Note the layer ARN from the output.

### Step 3: Create the Lambda function

```bash
aws lambda create-function \
  --function-name process-morton-cell \
  --runtime python3.12 \
  --architectures arm64 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler deployment.aws.lambda_handler.lambda_handler \
  --zip-file fileb://function.zip \
  --timeout 720 \
  --memory-size 2048 \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:xagg-complete-stack:VERSION

```

Replace:
- `ACCOUNT_ID` with your AWS account ID
- `REGION` with your AWS region (e.g., `us-east-1`)
- `VERSION` with the layer version from the previous step
- `lambda-execution-role` with your IAM role name (needs S3 write permissions)
- `xagg-complete-stack` with `magg-dependencies`

**Update function code** (after making changes to lambda_handler.py or src/magg/):

```bash
# Re-create the zip
zip -r function.zip deployment/aws/lambda_handler.py src/magg/

# Update the Lambda function
aws lambda update-function-code \
  --function-name process-morton-cell \
  --zip-file fileb://function.zip
```

## Testing

### Test with the orchestrator

Use the provided orchestration script:

```bash
cd /path/to/magg

# First, build a granule catalog
uv run python -m magg.catalog --cycle 22 --parent-order 6

# Then test with the orchestrator
uv run python deployment/aws/invoke_lambda.py \
  --catalog deployment/data/catalogs/granule_catalog_cycle22_order6.json \
  --dry-run --max-cells 1
```

The script handles NASA authentication and orchestrates Lambda invocations.

### Check CloudWatch Logs

```bash
aws logs tail /aws/lambda/process-morton-cell --follow
```

## Performance

### Execution Time
- **Average**: 2-3 minutes per cell
- **Maximum**: 10 minutes (anomalies)
- **Lambda limit**: 12 minutes (720 seconds)

### Memory Usage
- **Configured**: 2048 MB (2 GB)
- **Typical usage**: 1-1.5 GB
- Monitor via CloudWatch metrics and adjust if needed

### Cold Start
- **First invocation**: 3-5 seconds
- **Warm invocations**: <100 ms

## Monitoring

### CloudWatch Metrics

Monitor these key metrics:
- **Duration**: Execution time per invocation
- **Errors**: Failed invocations
- **Throttles**: Concurrent execution limit reached
- **MemoryUtilization**: Actual memory usage

### CloudWatch Logs Insights Queries

**Find slowest cells**:
```
fields @timestamp, @duration, parent_morton
| filter event_type = "processing_complete"
| sort @duration desc
| limit 20
```

**Error rate by morton cell**:
```
fields parent_morton, error
| filter event_type = "processing_complete" and error != null
| stats count() by error
```

**Overall progress**:
```
fields parent_morton
| filter event_type = "processing_complete" and error = null
| stats count() as completed
```

## Troubleshooting

### Common Errors

**1. Missing s3_credentials**

```
Error: Missing required parameters: s3_credentials
```

**Solution**: Ensure your orchestrator script calls `get_nsidc_s3_credentials()` and passes the credentials to the Lambda invocation.

**2. No granules found**

```
{"error": "No granules found"}
```

**Solution**: This is normal for cells outside the data coverage area. The function returns gracefully.

**3. S3 write permission denied**

```
Error: Failed to write zarr: Access Denied
```

**Solution**: Check that the Lambda execution role has `s3:PutObject` permission for the output bucket.

**4. Function timeout**

```
Task timed out after 720.00 seconds
```

**Solution**:
- Check CloudWatch Logs to identify which step is slow
- Profile with `max_granules` parameter to limit data
- Consider splitting very large cells into smaller ones


**5. Too many open files**

```
ERROR: Could not connect to the endpoint URL: "https://lambda.us-west-2.amazonaws.com/2015-03-31/functions/process-morton-cell/invocations"
ERROR: SSL validation failed for https://lambda.us-west-2.amazonaws.com/2015-03-31/functions/process-morton-cell/invocations [Errno 24] Too many open files
```

**Solution**: Decrease max workers (e.g., `uv run python deployment/aws/invoke_lambda.py --max-workers 50`) or increase ulimit (e.g., `ulimit -n 10000`)

### Debug Mode

To enable more verbose logging, set the log level:

```python
# In deployment/aws/lambda_handler.py or src/magg/processing.py
logger.setLevel(logging.DEBUG)
```

## Cost Estimate

### Per Invocation (1 morton cell)

**Average case** (180 seconds, 2 GB memory):
```
Duration: 180s × 2 GB × $0.0000166667/GB-s = $0.006
Requests: 1 × $0.0000002 = $0.0000002
Total: ~$0.006 per cell
```

**Full run** (1,872 cells):
```
Total: 1,872 × $0.006 = ~$11-15
```

**Additional costs**:
- S3 PUT requests: ~$0.94
- S3 storage: ~$0.04/month
- CloudWatch Logs: ~$0.05

**Total per run**: ~$12-15

## Next Steps

1. **Test with a few cells** to validate functionality
2. **Build orchestration** (Step Functions or boto3 script)
3. **Run integration test** with 50-100 cells
4. **Production run** with all 1,872 cells
5. **Monitor and optimize** based on CloudWatch metrics

## Related Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Design philosophy and spatial indexing approach
- [LAMBDA_ARM64.md](LAMBDA_ARM64.md) - Building Lambda layers for ARM64

## Support

For issues or questions:
- Check CloudWatch Logs for detailed error messages
- Review the troubleshooting section above
- Check GitHub issues: https://github.com/englacial/xagg/issues
