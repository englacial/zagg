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

import logging
import time

import earthaccess

from . import registry

logger = logging.getLogger(__name__)

# ``earthaccess.login()`` makes an HTTPS call to ``urs.earthdata.nasa.gov`` to
# validate/mint credentials, and that call fails transiently on CI runners -- an
# IPv6 no-route (``OSError: [Errno 101] Network is unreachable``, addressed at the
# resolver level in the benchmark workflow, issue #137) or a brief network blip.
# A bounded retry/backoff lets a genuinely transient failure self-heal instead of
# taking down the whole run/benchmark on the first attempt (issue #137).
_LOGIN_MAX_ATTEMPTS = 3
_LOGIN_BACKOFF_BASE_S = 2.0


def _login_with_retry():
    """Return an authenticated ``earthaccess`` session, retrying transient failures.

    Retries ``earthaccess.login()`` up to ``_LOGIN_MAX_ATTEMPTS`` with exponential
    backoff (2s, 4s, ...) on ``OSError``. That base class is the whole ``requests``
    IO/HTTP-error family -- every ``requests.exceptions.RequestException`` subclasses
    ``IOError``/``OSError`` -- so it covers the bare ``OSError: [Errno 101]`` no-route,
    ``ConnectionError``, and ``Timeout`` that the login can raise transiently. A
    genuinely-rejected credential surfaces as ``earthaccess``'s own
    ``LoginAttemptFailure`` (not an ``OSError``), so it fails fast rather than being
    retried; and the final attempt's exception propagates unchanged either way.
    """
    last_exc: OSError | None = None
    for attempt in range(1, _LOGIN_MAX_ATTEMPTS + 1):
        try:
            return earthaccess.login()
        except OSError as exc:
            last_exc = exc
            if attempt == _LOGIN_MAX_ATTEMPTS:
                break
            backoff = _LOGIN_BACKOFF_BASE_S * (2 ** (attempt - 1))
            logger.warning(
                "earthaccess.login() failed (attempt %d/%d): %s; retrying in %.0fs",
                attempt,
                _LOGIN_MAX_ATTEMPTS,
                exc,
                backoff,
            )
            time.sleep(backoff)
    assert last_exc is not None  # loop ran >=1 time and every attempt failed
    raise last_exc


def ensure_logged_in() -> None:
    """Warm the ``earthaccess`` auth singleton once, up front (issue #137).

    ``earthaccess.login()`` mutates process-global auth/store singletons, so a
    caller that fans work out across threads (e.g. the parallel benchmark) should
    authenticate ONCE before the fan-out -- otherwise the concurrent workers race
    to initialize that shared singleton. Calling this first populates it serially,
    so the in-worker logins become cheap hits. Uses the same bounded retry as the
    credential helpers; honors this module's "call once in the orchestrator"
    contract.
    """
    _login_with_retry()


def get_edl_token() -> str:
    """Return an Earthdata Login bearer token for HTTPS data access.

    Works from any network location (not region-restricted like S3).
    The token is used by h5coro's HTTPDriver.

    Returns
    -------
    str
        Bearer token string.
    """
    auth = _login_with_retry()
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
        "shard_key": -6134114,
        "s3_credentials": creds,
        # ... other params
    }
    ```
    """
    return get_daac_s3_credentials("NSIDC")


def get_daac_s3_credentials(daac: str) -> dict:
    """Return temporary S3 credentials for a NASA DAAC (direct S3 access).

    The generic form of :func:`get_nsidc_s3_credentials` (issue #213, Phase 4):
    same orchestrator-side call-once contract and ~1 h validity, with the DAAC
    selected by whatever name ``earthaccess`` accepts (e.g. ``"NSIDC"``,
    ``"GES_DISC"``). DAAC S3 endpoints are in-region only (us-west-2).
    """
    auth = _login_with_retry()
    return auth.get_s3_credentials(daac=daac)


def get_gesdisc_s3_credentials() -> dict:
    """Return temporary S3 credentials for GES DISC (MERRA-2 and friends)."""
    return get_daac_s3_credentials("GES_DISC")


registry.register_credential_provider(
    "nsidc",
    get_nsidc_s3_credentials,
    description="Earthdata temporary S3 credentials for NSIDC (ICESat-2 products)",
)
registry.register_credential_provider(
    "gesdisc",
    get_gesdisc_s3_credentials,
    description="Earthdata temporary S3 credentials for GES DISC (MERRA-2 reanalysis)",
)

#: The credential-provider registry (``zagg.registry.CREDENTIAL_PROVIDERS``).
CREDENTIAL_PROVIDERS = registry.CREDENTIAL_PROVIDERS
