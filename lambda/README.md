# xagg Lambda Function

AWS Lambda function for processing ICESat-2 ATL06 data by morton cell.

## Overview

This Lambda function processes a single morton cell (order 6) by:
1. Querying NASA CMR for ATL06 granules intersecting the cell
2. Reading HDF5 files directly from S3 using h5coro (no downloads)
3. Spatial filtering using morton indexing
4. Calculating summary statistics for child cells (order 12)
5. Writing xdggs-enabled zarr to S3

## Files

- **`lambda_handler.py`**: Main Lambda function handler (the only file in function.zip)
- **`orchestrator_auth.py`**: Helper for getting NASA credentials (used by orchestrator, not Lambda)
- **`build_arm64_layer.sh`**: Script to build the Lambda layer for ARM64
- **`build_layer_v14.sh`**: Script to build the Lambda layer
- **`build_granule_catalog.py`**: Script to build granule catalog for orchestrator
- **`invoke_production.py`**: Orchestration script for invoking Lambda functions
- **`README.md`**: This file
- **`INSTRUCTIONS_ARM.md`**: Instructions for building ARM64 layer

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
│    - lambda_handler.py                                       │
│    - query_cmr_with_polygon.py                               │
│  ──────────────────────────────────────────────────────────  │
│  Layer (75 MB compressed, 227 MB uncompressed):              │
│    - numpy, pandas, xarray, botocore                         │
│    - arro3, shapely, geopandas, earthaccess                  │
│    - xdggs, h5coro, mortie, healpix                          │
└─────────────────────────────────────────────────────────────┘
```

## Event Payload

```json
{
  "parent_morton": -6134114,
  "cycle": 22,
  "child_order": 12,
  "s3_bucket": "your-output-bucket",
  "s3_prefix": "atl06/cycle_22",
  "s3_credentials": {
    "accessKeyId": "ASIA...",
    "secretAccessKey": "...",
    "sessionToken": "..."
  },
  "max_granules": null
}
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `parent_morton` | int | Yes | Morton index of parent cell (order 6) |
| `cycle` | int | Yes | ICESat-2 cycle number (e.g., 22) |
| `child_order` | int | Yes | Order of child cells for statistics (typically 12) |
| `s3_bucket` | str | Yes | S3 bucket for output zarr files |
| `s3_prefix` | str | Yes | S3 prefix for output zarr files |
| `s3_credentials` | dict | Yes | S3 credentials from orchestrator (see below) |
| `max_granules` | int | No | Maximum number of granules to process (for testing) |

### S3 Credentials

The `s3_credentials` are obtained by the orchestrator ONCE before invoking Lambda functions:

```python
from orchestrator_auth import get_nsidc_s3_credentials

# Get credentials (valid for ~1 hour)
s3_creds = get_nsidc_s3_credentials()

# Pass to each Lambda invocation
event = {
    "parent_morton": -6134114,
    "s3_credentials": s3_creds,
    # ... other params
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

Since all dependencies are in the Lambda layer, the function package only needs the handler:

```bash
cd /path/to/magg/lambda
zip function.zip lambda_handler.py
```

This creates a small (~17 KB) `function.zip` with just the Lambda handler code.

### Step 2: Deploy the Lambda layer and function

**Upload the Lambda layer**:

```bash
cd /path/to/magg/lambda

aws lambda publish-layer-version \
  --layer-name xagg-complete-stack \
  --zip-file fileb://lambda_layer_arm64.zip \
  --compatible-runtimes python3.12 \
  --compatible-architectures arm64 \
  --description "xagg complete stack: numpy, pandas, xarray, xdggs, h5coro, mortie"
```

Note the layer ARN from the output (e.g., `arn:aws:lambda:us-east-1:123456789012:layer:xagg-complete-stack:1`)

See `INSTRUCTIONS_ARM.md` for details on building the layer.

**Create the Lambda function**:

```bash
aws lambda create-function \
  --function-name process-morton-cell \
  --runtime python3.12 \
  --architectures arm64 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://function.zip \
  --timeout 720 \
  --memory-size 2048 \
  --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:xagg-complete-stack:VERSION

```

Replace:
- `ACCOUNT_ID` with your AWS account ID
- `REGION` with your AWS region (e.g., `us-east-1`)
- `VERSION` with the layer version from the previous step
- `lambda-execution-role` with your IAM role name

**Update function code** (after making changes to lambda_handler.py):

```bash
# Re-create the zip
zip function.zip lambda_handler.py

# Update the Lambda function
aws lambda update-function-code \
  --function-name process-morton-cell \
  --zip-file fileb://function.zip
```

## Testing

### Test with the orchestrator

Use the provided `invoke_production.py` script:

```bash
cd /path/to/magg/lambda

# Edit the script to configure your S3 bucket and cells to process
# Then run:
uv run python invoke_production.py --dry-run --max-cells 1
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

**Solution**: Decrease max workers (e.g., `uv run python invoke_production.py --max-workers 50`) or increase ulimit (e.g., `ulimit -n 10000`)'

### Debug Mode

To enable more verbose logging, set the log level:

```python
# In lambda_handler.py
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

- [../lambda_notes.md](../lambda_notes.md) - Comprehensive refactoring plan
- [../LAMBDA_BUILD_SUCCESS.md](../LAMBDA_BUILD_SUCCESS.md) - Build results and layer details
- [../lambda_package_size_analysis.md](../lambda_package_size_analysis.md) - Package size analysis
- [../demo_s3_xdggs.ipynb](../demo_s3_xdggs.ipynb) - Original Dask implementation

## Support

For issues or questions:
- Check CloudWatch Logs for detailed error messages
- Review the troubleshooting section above
- Check GitHub issues: https://github.com/englacial/xagg/issues
