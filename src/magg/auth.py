"""
Orchestrator authentication helpers for NASA Earthdata access.

Two credential types:

- **S3**: ``get_s3_credentials()`` returns STS temporary credentials
  for direct S3 access. Only works from within us-west-2.
- **HTTPS**: ``get_edl_token()`` returns a bearer token for HTTPS access.
  Works from anywhere.

Call ONCE in the orchestrator before processing. Credentials are valid
for approximately 1 hour.
"""

from functools import partial

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


def get_s3_credentials(daac: str = "NSIDC") -> dict:
    """Authenticate with NASA Earthdata and return temporary S3 credentials.

    Parameters
    ----------
    daac : str
        DAAC name (e.g. ``"NSIDC"``, ``"GES_DISC"``). Default ``"NSIDC"``.

    Returns
    -------
    dict
        S3 credentials with keys ``accessKeyId``, ``secretAccessKey``,
        ``sessionToken``, ``expiration``.
    """
    auth = earthaccess.login()
    return auth.get_s3_credentials(daac=daac)


# Backward-compatible alias
get_nsidc_s3_credentials = partial(get_s3_credentials, daac="NSIDC")
