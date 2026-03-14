"""Store factory for opening Zarr stores from path strings."""

from pathlib import Path

from zarr.abc.store import Store
from zarr.storage import LocalStore


def open_store(path: str, read_only: bool = False, **kwargs) -> Store:
    """Open a Zarr store from a path string.

    Parameters
    ----------
    path : str
        Store path. ``s3://bucket/prefix`` opens an S3-backed store;
        all other paths open a local filesystem store.
    read_only : bool
        Whether to open in read-only mode.
    **kwargs
        For S3 stores: ``region`` (default ``"us-west-2"``).

    Returns
    -------
    Store
    """
    if path.startswith("s3://"):
        return _open_s3_store(path, read_only=read_only, **kwargs)
    return LocalStore(Path(path).resolve(), read_only=read_only)


def _open_s3_store(path: str, read_only: bool = False, **kwargs) -> Store:
    """Open an S3-backed Zarr store."""
    from obstore.auth.boto3 import Boto3CredentialProvider
    from obstore.store import S3Store
    from zarr.storage import ObjectStore

    bucket, prefix = parse_s3_path(path)
    region = kwargs.pop("region", "us-west-2")

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
