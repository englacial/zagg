"""Store factory for opening Zarr stores from path strings."""

import copy
import threading
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
# credential-validity bound its docs warn about — and since the nominal sleep
# sum of 12 paced retries exceeds it, the timeout (not ``max_retries``) is the
# effective bound under a long burst. Callers can pass their own
# ``retry_config`` through ``**kwargs`` to override (``None`` means this
# default, not obstore's).
_S3_RETRY_CONFIG = {
    "max_retries": 12,
    "retry_timeout": timedelta(seconds=180),
    "backoff": {
        "init_backoff": timedelta(seconds=1),
        "max_backoff": timedelta(seconds=30),
        "base": 2,
    },
}

# Read-only carve-out: stores opened via ``open_store(read_only=True)`` (e.g.
# readers, read-back analysis, temporal .zarr reads) — for them a genuinely
# failing endpoint should surface a clear error quickly (nominal sleep sum
# 15 s, so ``max_retries`` is the effective bound and the 30 s timeout a
# ceiling), not feel hung for the full write policy above. Still paced (rides
# a typical throttle burst), and reads are far harder to throttle anyway
# (S3's per-prefix GET budget is ~5,500/s vs ~3,500 for PUT). Fleet workers
# open read-write, so the issue #186 fix is unaffected. ``open_object_store``
# has no read-only concept — its read-path callers pass ``retry_config``
# explicitly (e.g. ``temporal.open_dataset``'s NetCDF branch).
_S3_READONLY_RETRY_CONFIG = {
    "max_retries": 4,
    "retry_timeout": timedelta(seconds=30),
    "backoff": {
        "init_backoff": timedelta(seconds=1),
        "max_backoff": timedelta(seconds=8),
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
        For S3 stores: ``region`` (default ``"us-west-2"``) and any obstore
        ``S3Store`` option — notably ``retry_config``, which defaults to the
        paced :data:`_S3_RETRY_CONFIG` policy (issue #186), or the shorter
        :data:`_S3_READONLY_RETRY_CONFIG` when ``read_only=True``; and
        ``skip_signature=True`` for anonymous reads of public buckets (no
        AWS credentials needed, e.g. binder).

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


# Ambient-credential object-store cache (issue #287): one obstore ``S3Store``
# per ``s3://`` path per PROCESS, for the ambient (execution-role) hot path only.
# The sidecar index backend (``h5coro_hidefix.zagg_backend.SidecarIndex``) calls
# ``open_object_store(self.store)`` once per granule to fetch that granule's
# manifest parquet; without this cache each call built a fresh
# ``Boto3CredentialProvider`` whose ``__init__`` eagerly walks the botocore
# credential chain (~300 ms of client/TLS + "Found credentials..." per granule),
# on the read critical path — 675 rebuilds on one 784-granule o9 shard. Mirrors
# the raster ``_STORE_CACHE`` (issue #244). Module lifetime == sandbox lifetime:
# ``Boto3CredentialProvider`` refreshes per call (30-min ttl) and Lambda role
# creds are static per sandbox, so a cached store cannot outlive its creds.
# Scoped deliberately to the ``credentials is None and endpoint_url is None and
# not kwargs`` case (the sidecar's exact call): explicit-credential output
# writes, custom endpoints, and retry-config/anonymous callers fall through to a
# fresh build, byte-identical to before — a statically-supplied token must NOT
# be cached (it would freeze on a warm worker).
_OBJECT_STORE_CACHE: dict = {}
_OBJECT_STORE_LOCK = threading.Lock()


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

    Ambient ``s3://`` stores (no explicit ``credentials``/``endpoint_url`` and no
    extra ``kwargs``) are cached per process and reused across calls (issue #287)
    -- this is the sidecar manifest-fetch hot path. Every other call builds a
    fresh store, unchanged.
    """
    if path.startswith("s3://"):
        if credentials is None and endpoint_url is None and not kwargs:
            with _OBJECT_STORE_LOCK:
                store = _OBJECT_STORE_CACHE.get(path)
                if store is None:
                    store = _s3_object_store(path)
                    _OBJECT_STORE_CACHE[path] = store
            return store
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

    if read_only and kwargs.get("retry_config") is None:
        # Interactive read population: fail fast on a dead endpoint (comment
        # on the constant). Set here so _s3_object_store's write-policy
        # default doesn't kick in; an explicit caller retry_config still wins.
        kwargs["retry_config"] = _S3_READONLY_RETRY_CONFIG
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
    if kwargs.get("retry_config") is None:
        kwargs["retry_config"] = _S3_RETRY_CONFIG
    # Deep copy unconditionally so no store's kwargs alias a module-level
    # default — whichever seam it arrived through (here, the read-only branch
    # in _open_s3_store, or a caller passing a constant like the runner's
    # _POLL_RETRY_CONFIG). obstore only reads it at construction, but a
    # future mutation of one store's config must not edit a shared global.
    kwargs["retry_config"] = copy.deepcopy(kwargs["retry_config"])

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
    elif kwargs.get("skip_signature"):
        # Anonymous read of a public bucket: no credential provider —
        # Boto3CredentialProvider raises without ambient AWS credentials,
        # which anonymous environments (e.g. binder) lack by definition.
        # Addressing style is deliberately left to obstore's default, exactly
        # matching the construction the example notebooks used directly
        # (unlike the credentialed branch, which pins path-style above).
        s3 = S3Store(bucket, prefix=prefix, region=region, **kwargs)
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
