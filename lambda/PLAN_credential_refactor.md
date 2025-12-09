# Plan: Refactor NASA Earthdata Credentials to Orchestrator-Level Auth

## Goal

Move NASA Earthdata authentication from individual Lambda functions to a single orchestrator-level authentication. Pass the short-lived S3 credentials (~1 hour) as input to each Lambda invocation.

## Rationale

1. **Avoid rate limits**: 1,872 simultaneous `earthaccess.login()` calls could trigger NASA rate limiting
2. **Reduce complexity**: Eliminates AWS Secrets Manager setup and IAM permissions
3. **Faster cold starts**: No secrets retrieval on Lambda startup
4. **Lower cost**: No Secrets Manager charges (~$0.49/month saved)
5. **Simpler deployment**: One fewer AWS resource to configure

## Key Insight

- **CMR queries are anonymous**: No authentication needed for `query_atl06_cmr_with_polygon()`
- **S3 reads require auth**: Only h5coro S3 reads need the credentials
- **Token lifetime**: S3 credentials last ~1 hour, Lambda runs max 10-12 minutes

## Changes Required

### 1. Modify `lambda_handler.py`

**Remove**:
- `get_nasa_credentials()` function (lines 44-72)
- `authenticate_earthaccess()` function (lines 75-110)
- `earthaccess` import (not needed in Lambda anymore)
- References to `NASA_EARTHDATA_SECRET` environment variable

**Update event payload** to accept credentials directly:
```python
# OLD event:
{
    "parent_morton": -6134114,
    "cycle": 22,
    "child_order": 12,
    "s3_bucket": "bucket",
    "s3_prefix": "prefix",
    "max_granules": null
}

# NEW event:
{
    "parent_morton": -6134114,
    "cycle": 22,
    "child_order": 12,
    "s3_bucket": "bucket",
    "s3_prefix": "prefix",
    "max_granules": null,
    "s3_credentials": {
        "accessKeyId": "ASIA...",
        "secretAccessKey": "...",
        "sessionToken": "..."
    }
}
```

**Update `lambda_handler()`**:
- Validate `s3_credentials` in required parameters
- Pass `event['s3_credentials']` directly to `process_morton_cell()`

### 2. Create orchestrator authentication module

**New file**: `lambda/orchestrator_auth.py`

Provides helper functions for orchestrator scripts:
```python
def get_nsidc_s3_credentials() -> dict:
    """
    Authenticate with NASA Earthdata and return S3 credentials.

    Call this ONCE in the orchestrator before invoking Lambdas.
    Credentials are valid for ~1 hour.

    Returns
    -------
    dict
        S3 credentials with keys: accessKeyId, secretAccessKey, sessionToken
    """
    import earthaccess
    auth = earthaccess.login()
    return auth.get_s3_credentials(daac="NSIDC")
```

### 3. Update Lambda layer

**Remove from layer** (optional optimization):
- `earthaccess` package (saves ~5-10 MB)
- Only needed in orchestrator environment, not Lambda

**Note**: Can defer this optimization - earthaccess in layer doesn't hurt, just wastes space.

### 4. Update IAM role

**Remove permission**:
```json
{
    "Effect": "Allow",
    "Action": ["secretsmanager:GetSecretValue"],
    "Resource": "arn:aws:secretsmanager:*:*:secret:nasa-earthdata-*"
}
```

Role now only needs:
- CloudWatch Logs (write)
- S3 (read/write to output bucket)

### 5. Update documentation

**Files to update**:
- `lambda/README.md` - Remove Secrets Manager setup, update event payload
- `DEPLOYMENT_READY.md` - Remove Step 1 (Secrets Manager), simplify checklist
- `lambda_monday.md` - Update "What's NOT Yet Done" section

### 6. Create example orchestrator script

**New file**: `lambda/invoke_single_cell.py`

Example script showing the new auth flow:
```python
#!/usr/bin/env python3
"""Example: Invoke Lambda with orchestrator-level authentication."""

import json
import boto3
import earthaccess

def main():
    # Authenticate ONCE
    print("Authenticating with NASA Earthdata...")
    auth = earthaccess.login()
    s3_creds = auth.get_s3_credentials(daac="NSIDC")
    print(f"✓ Got S3 credentials (expire: {s3_creds.get('expiration', 'N/A')})")

    # Prepare event with credentials
    event = {
        "parent_morton": -6134114,
        "cycle": 22,
        "child_order": 12,
        "s3_bucket": "your-bucket",
        "s3_prefix": "atl06/test",
        "max_granules": 5,
        "s3_credentials": {
            "accessKeyId": s3_creds["accessKeyId"],
            "secretAccessKey": s3_creds["secretAccessKey"],
            "sessionToken": s3_creds["sessionToken"]
        }
    }

    # Invoke Lambda
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName='process-morton-cell',
        InvocationType='RequestResponse',
        Payload=json.dumps(event)
    )

    result = json.loads(response['Payload'].read())
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
```

## Implementation Order

1. **Update `lambda_handler.py`** - Remove auth functions, accept credentials in event
2. **Create `orchestrator_auth.py`** - Helper module for orchestrator scripts
3. **Create `invoke_single_cell.py`** - Example/test script
4. **Update `deploy_function.sh`** - Rebuild function.zip
5. **Update `README.md`** - Document new event payload and auth flow
6. **Update `DEPLOYMENT_READY.md`** - Remove Secrets Manager steps

## Testing Plan

1. **Local test**: Run `invoke_single_cell.py` against deployed Lambda
2. **Verify**: Lambda receives credentials and reads from S3 successfully
3. **Verify**: No Secrets Manager calls in CloudWatch Logs

## Rollback Plan

If issues arise, the old approach can be restored by:
1. Reverting `lambda_handler.py` changes
2. Re-adding Secrets Manager secret
3. Adding back IAM permissions

## Files Changed

| File | Action |
|------|--------|
| `lambda/lambda_handler.py` | Modify - remove auth, accept creds in event |
| `lambda/orchestrator_auth.py` | Create - helper for orchestrator auth |
| `lambda/invoke_single_cell.py` | Create - example script |
| `lambda/README.md` | Update - new event payload, remove Secrets Manager |
| `lambda/requirements.txt` | No change (boto3 still needed) |
| `lambda/deploy_function.sh` | No change (still packages same files) |
| `DEPLOYMENT_READY.md` | Update - remove Secrets Manager step |

## Benefits Summary

| Before | After |
|--------|-------|
| 1,872 NASA auth calls | 1 NASA auth call |
| Secrets Manager required | No Secrets Manager |
| 3 IAM permissions | 2 IAM permissions |
| Risk of rate limiting | No rate limit risk |
| ~$0.49/month Secrets cost | $0 |
| Complex Lambda startup | Simple Lambda startup |
