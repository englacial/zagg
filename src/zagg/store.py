"""Store factory for opening Zarr stores from path strings."""

from datetime import timedelta
from pathlib import Path

from zarr.abc.store import Store
from zarr.storage import LocalStore

# S3 retry pacing (issue #186). obstore's default policy retries 5xx/connection
# errors up to 10 times with jittered exponential backoff from 100 ms — under a
# sustained 503 SlowDown burst the jitter draws small sleeps and the whole
# budget is spent in ~2-4 s of near-immediate retries, hammering the throttled
# prefix and then surfacing the error (the trapped fleet failures burned all 10
# retries in 1.8-3.1 s). These defaults pace retries seconds apart with ~2 min
# of headroom, which is what S3's "Please reduce your request rate" asks for.
# ``retry_timeout`` stays at obstore's 180 s default, below the 5-minute
# credential-validity bound its docs warn about. Callers can pass their own
# ``retry_config`` through ``**kwargs`` to override.
_S3_RETRY_CONFIG = {
    "max_retries": 12,
    "retry_timeout": timedelta(seconds=180),
    "backoff": {
        "init_backoff": timedelta(seconds=1),
        "max_backoff": timedelta(seconds=30),
        "base": 2,
    },
}


def open_store(
    path: str,
    read_only: bool = False,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    **kwargs,
) -> Store:
    """Open a Zarr store from a path string.

    Parameters
    ----------
    path : str
        Store path. ``s3://bucket/prefix`` opens an S3-backed store;
        all other paths open a local filesystem store.
    read_only : bool
        Whether to open in read-only mode.
    credentials : dict, optional
        Explicit S3 credentials (camelCase keys ``accessKeyId``,
        ``secretAccessKey``, optional ``sessionToken``). When omitted the
        store falls back to the ambient credential chain (execution role).
        Ignored for local stores.
    endpoint_url : str, optional
        Custom S3-compatible endpoint (e.g. Cloudflare R2, MinIO). Ignored
        for local stores.
    **kwargs
        For S3 stores: ``region`` (default ``"us-west-2"``).

    Returns
    -------
    Store
    """
    if path.startswith("s3://"):
        return _open_s3_store(
            path,
            read_only=read_only,
            credentials=credentials,
            endpoint_url=endpoint_url,
            **kwargs,
        )
    return LocalStore(Path(path).resolve(), read_only=read_only)


def open_object_store(
    path: str,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    **kwargs,
):
    """Open a raw obstore store for small side-channel objects (issue #151).

    Unlike :func:`open_store` (which wraps the backend in a Zarr ``Store``),
    this returns the bare obstore store for plain byte get/put of non-Zarr
    objects -- e.g. the per-shard async result JSON a Lambda worker writes next
    to the output store for the orchestrator to poll. Path forms and credential
    handling match ``open_store``; a local directory is created if absent.
    """
    if path.startswith("s3://"):
        return _s3_object_store(
            path,
            credentials=credentials,
            endpoint_url=endpoint_url,
            **kwargs,
        )
    from obstore.store import LocalStore as ObstoreLocalStore

    local = Path(path).resolve()
    local.mkdir(parents=True, exist_ok=True)
    return ObstoreLocalStore(local)


def _open_s3_store(
    path: str,
    read_only: bool = False,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    **kwargs,
) -> Store:
    """Open an S3-backed Zarr store.

    With no ``credentials`` and no ``endpoint_url`` the store behaves exactly
    as before: ambient credentials via ``Boto3CredentialProvider`` against the
    default AWS endpoint. When explicit ``credentials`` and/or an
    ``endpoint_url`` are supplied, the store is opened with those instead and
    path-style addressing is enabled (so dotted bucket names and
    S3-compatible endpoints work over TLS).
    """
    from zarr.storage import ObjectStore

    s3 = _s3_object_store(path, credentials=credentials, endpoint_url=endpoint_url, **kwargs)
    return ObjectStore(store=s3, read_only=read_only)


def _s3_object_store(
    path: str,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    **kwargs,
):
    """Build the raw obstore ``S3Store`` for ``path`` (credential rules above)."""
    from obstore.store import S3Store

    bucket, prefix = parse_s3_path(path)
    region = kwargs.pop("region", "us-west-2")
    kwargs.setdefault("retry_config", _S3_RETRY_CONFIG)

    if credentials or endpoint_url:
        opts = {
            "bucket": bucket,
            "prefix": prefix,
            "region": region,
            # Path-style addressing: required for dotted bucket names (TLS) and
            # for non-AWS S3-compatible endpoints.
            "virtual_hosted_style_request": False,
        }
        if credentials:
            opts["access_key_id"] = credentials["accessKeyId"]
            opts["secret_access_key"] = credentials["secretAccessKey"]
            if credentials.get("sessionToken"):
                opts["session_token"] = credentials["sessionToken"]
        if endpoint_url:
            opts["endpoint"] = endpoint_url
        s3 = S3Store(**opts, **kwargs)
    else:
        from obstore.auth.boto3 import Boto3CredentialProvider

        s3 = S3Store(
            bucket,
            prefix=prefix,
            region=region,
            credential_provider=Boto3CredentialProvider(),
            **kwargs,
        )
    return s3


def parse_s3_path(path: str) -> tuple[str, str]:
    """Parse an ``s3://bucket/prefix`` path into bucket and prefix.

    Parameters
    ----------
    path : str
        S3 URI (must start with ``s3://``).

    Returns
    -------
    tuple of (bucket, prefix)

    Raises
    ------
    ValueError
        If path does not start with ``s3://``.
    """
    if not path.startswith("s3://"):
        raise ValueError(f"Not an S3 path: {path}")
    parts = path[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


__all__ = ["open_object_store", "open_store", "parse_s3_path"]
