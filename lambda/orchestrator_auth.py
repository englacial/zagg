"""
Orchestrator authentication helper for NASA Earthdata S3 access.

Call get_nsidc_s3_credentials() ONCE in your orchestrator before invoking
Lambda functions. Pass the returned credentials to each Lambda invocation.

Credentials are valid for approximately 1 hour.
"""

import earthaccess


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
    >>> creds = get_nsidc_s3_credentials()
    >>> print(f"Credentials expire: {creds.get('expiration')}")

    >>> # Pass to Lambda invocation
    >>> event = {
    ...     "parent_morton": -6134114,
    ...     "s3_credentials": creds,
    ...     # ... other params
    ... }
    """
    auth = earthaccess.login()
    return auth.get_s3_credentials(daac="NSIDC")
