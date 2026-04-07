"""
Orchestrator authentication helpers for NASA Earthdata access.

Two credential types:

- **S3**: ``get_nsidc_s3_credentials()`` returns STS temporary credentials
  for direct S3 access. Only works from within us-west-2.
- **HTTPS**: ``get_edl_token()`` returns a bearer token for HTTPS access.
  Works from anywhere.

Call ONCE in the orchestrator before processing. Credentials are valid
for approximately 1 hour.
"""

import earthaccess


def get_edl_token() -> str:
    """Return an Earthdata Login bearer token for HTTPS data access.

    Works from any network location (not region-restricted like S3).
    The token is used by h5coro's HTTPDriver.

    Returns
    -------
    str
        Bearer token string.
    """
    auth = earthaccess.login()
    return auth.token["access_token"]


def get_nsidc_s3_credentials() -> dict:
    """
    Authenticate with NASA Earthdata and return S3 credentials for NSIDC.

    Call this ONCE in the orchestrator before invoking Lambda functions.
    Credentials are valid for ~1 hour, which is longer than Lambda max
    execution time (15 minutes).

    Returns
    -------
    dict
        S3 credentials with keys:
        - accessKeyId: str
        - secretAccessKey: str
        - sessionToken: str
        - expiration: str (ISO timestamp)

    Examples
    --------

    ```python
    creds = get_nsidc_s3_credentials()
    print(f"Credentials expire: {creds.get('expiration')}")

    # Pass to Lambda invocation
    event = {
        "parent_morton": -6134114,
        "s3_credentials": creds,
        # ... other params
    }
    ```
    """
    auth = earthaccess.login()
    return auth.get_s3_credentials(daac="NSIDC")
