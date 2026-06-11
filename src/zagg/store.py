"""Store factory for opening Zarr stores from path strings."""

from pathlib import Path

from zarr.abc.store import Store
from zarr.storage import LocalStore


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
    from obstore.store import S3Store
    from zarr.storage import ObjectStore

    bucket, prefix = parse_s3_path(path)
    region = kwargs.pop("region", "us-west-2")

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
    return ObjectStore(store=s3, read_only=read_only)


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


__all__ = ["open_store", "parse_s3_path"]
