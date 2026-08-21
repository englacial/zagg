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


# Cross-account object ownership on external writes (issue #495). S3 object
# ownership follows the WRITING account, so under the ``ObjectWriter`` setting a
# cross-account PUT without this canned ACL creates objects the BUCKET owner can
# neither manage nor delete. Source Cooperative's in-region upload path (their
# "Option 3" grant) requires it for exactly that reason -- it is what retires the
# ``data.source.coop`` proxy hop, and with it the egress the CA campaign paid.
# Since phase 3 the fleet reaches that bucket with the AMBIENT execution role,
# so the trigger is the destination bucket (:data:`_PUBLISHED_BUCKETS`) as well
# as injected credentials -- see :func:`_external_target`.
# The value is correct in all three Object Ownership modes: ``BucketOwnerEnforced``
# ignores ACLs, but AWS explicitly carves out this one canned value instead of
# failing the request, so it is sent unconditionally rather than gated on the
# target's configuration.
#
# obstore exposes no ACL config key (``aws_acl``/``acl``/``x-amz-acl`` all raise
# ``UnknownConfigurationKeyError``), so it rides as a default request header --
# verified end-to-end against a real ACL-enabled bucket, and traced per-request
# against a local endpoint. obstore applies ``default_headers`` on the reqwest
# client, i.e. AFTER object_store signs, so the header rides OUTSIDE the
# signature: ``x-amz-acl`` never appears in ``SignedHeaders``, and AWS accepts it
# because S3 ignores unsigned non-required ``x-amz-*`` on header-auth requests --
# it would NOT survive a presigned-URL path, which rejects unsigned ``x-amz-*``.
# The header rides ``CreateMultipartUpload`` (``POST ?uploads``) as well as a
# single-shot ``PUT``, and that is the load-bearing half: the create request is
# what sets a multipart object's ACL (``UploadPart``/``CompleteMultipartUpload``
# ignore it), and at ~131 MB/shard multipart is the normal write path. S3
# interprets ``x-amz-acl`` only on object-creating requests, so a GET/LIST issued
# by the same store carries the header inertly. That inertness is what makes the
# one route that cannot tell reads from writes safe: ``open_object_store`` has no
# read-only concept, so its credentialed callers -- including
# ``temporal.open_dataset``'s NetCDF branch, a pure GET of a consumer INPUT
# bucket (issue #223) -- still send the header. ``open_store(read_only=True)``
# does not: it knows, so it is gated (see ``_s3_object_store``).
_BUCKET_OWNER_ACL = "bucket-owner-full-control"

# Buckets this account writes to but does not OWN, reached with the ambient
# execution role (issue #495). Since phase 3 the fleet publishes to Source
# Cooperative as itself -- no injected credentials -- so "did the caller pass
# credentials?" no longer separates our buckets from theirs, and keying the
# canned ACL on that alone would silently publish owner-less objects. The
# destination is the thing that decides, so the destination is what the
# predicate reads. A fixed external fact, of the same class as the literal
# bucket ARNs in ``deployment/aws/template.yaml``: this is the bucket named in
# Source Cooperative's grant, and it changes only when that grant does.
#
# Deliberately NOT "every AWS-endpoint write": the header requires
# ``s3:PutObjectAcl`` on the target, which zagg holds on this bucket alone --
# sending it everywhere would 403 every self-hoster's own output bucket and
# ``sliderule-public-cors``, whose bucket policy is not ours to change.
_PUBLISHED_BUCKETS = frozenset({"us-west-2.opendata.source.coop"})


def _external_target(credentials, endpoint_url, bucket=None) -> bool:
    """Whether these store kwargs describe a target this account does not own.

    True on either route to a not-ours destination: explicit write credentials
    against the AWS endpoint (the un-negotiated targets injection still exists
    for), or an ambient write to a bucket in :data:`_PUBLISHED_BUCKETS`. A
    custom ``endpoint_url`` excludes both, unchanged.

    The issue #495 predicate, in one place because it has a second caller
    outside this module: ``zagg.lifecycle``'s skip-run touch re-creates objects
    with a boto3 ``CopyObject`` -- an object-CREATING request that never passes
    through :func:`_s3_object_store` -- and must apply
    :data:`_BUCKET_OWNER_ACL` on exactly this condition, or it strips the
    ownership an earlier PUT handed over.
    """
    if endpoint_url:
        return False
    return bool(credentials) or bucket in _PUBLISHED_BUCKETS


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
        AWS credentials needed, e.g. binder); and ``client_options``, whose
        ``default_headers`` is where the canned-ACL override lives (see Notes).

    Notes
    -----
    A write target this account does not own makes the store send
    ``x-amz-acl: bucket-owner-full-control`` on every request, so the bucket
    owner owns what it writes (issue #495; see :data:`_BUCKET_OWNER_ACL`). Two
    shapes qualify: explicit ``credentials`` without an ``endpoint_url``, and an
    ambient write to a bucket in :data:`_PUBLISHED_BUCKETS` (Source Cooperative,
    which the execution role now reaches directly). ``read_only=True``
    suppresses it: a read opened with explicit credentials is the issue #223
    consumer-INPUT channel (somebody else's input bucket, as
    ``temporal.open_dataset`` opens it), not a write target of ours.

    That header is a DEFAULT, not a fixture (issue #500). It is merged with
    ``setdefault``, so a caller-supplied value WINS -- honoured as passed,
    neither merged with ours nor overwritten (key case is irrelevant; see
    :func:`_with_bucket_owner_acl`)::

        open_store(path, credentials=creds,
                   client_options={"default_headers": {"x-amz-acl": "private"}})

    The permission requirement rides along: any ACL-carrying PUT needs
    ``s3:PutObjectAcl`` on the target, so overriding to a different canned value
    carries the same requirement rather than a lesser one. On those external
    targets -- and only there, since the gate in :func:`_s3_object_store` is the
    one thing that routes ``client_options`` through
    :func:`_with_bucket_owner_acl` -- passing ``None`` as the value strips the
    header instead of setting one. Anywhere else (our own buckets,
    ``read_only=True``, ``skip_signature=True``, any ``endpoint_url``) no ACL is
    sent to begin with, and a ``None`` reaches obstore raw, which rejects it.

    Injected ``credentials`` are never REFRESHED (issue #500, folded from #498).
    ``output_credentials`` are resolved once at dispatch and embedded in every
    worker's invoke payload, so a worker inherits the dispatcher's clock rather
    than starting its own: a run that outlasts its credential lifetime fails in
    the tail, at write time, after the compute is already paid for, and
    concentrated on the slowest shards. The ceiling depends on how the
    credentials were obtained -- ``sts:AssumeRole`` from an already-assumed role
    (SSO included) is role chaining, which AWS hard-caps at one hour and which
    ``MaxSessionDuration`` cannot raise, while ``AssumeRoleWithWebIdentity`` is
    not chaining and honours ``MaxSessionDuration`` up to 12 hours. This does
    NOT affect the fleet's published writes, which go out under the ambient
    execution role that Lambda rotates transparently; the limitation is specific
    to the injected-credential escape hatch (issue #26).

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

    Side-channel objects are real writes to the output store (status envelopes,
    hive manifests, stats sidecars, the temporal tabular object), so the
    external-target canned ACL applies here exactly as it does to
    :func:`open_store` -- both routes share :func:`_s3_object_store`, so an
    ambient write to a published bucket carries the header here too, cached
    store included (the cache is keyed by path, and the path is what decides)
    (issue #495). Known exception: this route has no ``read_only`` concept, so a
    credentialed READER built through it sends the header too -- notably
    ``temporal.open_dataset``'s NetCDF branch, a pure GET of a consumer-input
    bucket (issue #223). It is inert there (S3 interprets ``x-amz-acl`` only on
    object-creating requests); ``open_store(read_only=True)``, which can tell,
    suppresses it.

    The rest of the injected-credential contract is shared with
    :func:`open_store` and documented there (issue #500): an ``x-amz-acl`` the
    caller sets in ``client_options["default_headers"]`` wins over the canned
    default, and on an external target ``None`` strips the header; any
    ACL-carrying PUT needs ``s3:PutObjectAcl`` on the target; and injected
    ``credentials`` are resolved once at dispatch and never refreshed, so a long
    run fails at write time in the tail rather than up front. The sentinel is
    scoped, and the dominant call here is on the other side of that scope: an
    ambient write to our own output store is not an external target, so no ACL
    is sent to begin with and a ``None`` value would be rejected by obstore.
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
    s3 = _s3_object_store(
        path,
        credentials=credentials,
        endpoint_url=endpoint_url,
        read_only=read_only,
        **kwargs,
    )
    return ObjectStore(store=s3, read_only=read_only)


def _s3_object_store(
    path: str,
    credentials: dict | None = None,
    endpoint_url: str | None = None,
    read_only: bool = False,
    **kwargs,
):
    """Build the raw obstore ``S3Store`` for ``path`` (credential rules above).

    ``read_only`` is consumed here, never forwarded to ``S3Store`` (obstore has
    no such option): it only gates the issue #495 canned ACL, since a read
    opened with explicit credentials is an input we do not write.
    """
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

    if (
        _external_target(credentials, endpoint_url, bucket)
        and not read_only
        and not kwargs.get("skip_signature")
    ):
        # A WRITE target this account does not own (issue #495), reached either
        # way: injected credentials against the AWS endpoint (the ambient
        # execution role covers every in-account store, so injected write
        # credentials exist precisely to write somewhere else), or an ambient
        # write to a published bucket -- which is how the fleet reaches Source
        # Cooperative since phase 3, and is why this gate reads the BUCKET and
        # not just the credential shape.
        #
        # ``read_only`` is the other shape of injected credentials -- the issue
        # #223 consumer-INPUT channel reading somebody else's bucket -- and is
        # excluded, as is ``skip_signature`` (an anonymous public read). A
        # custom ``endpoint_url`` is excluded deliberately, and that exclusion
        # covers TWO shapes: the S3-compatible stores behind that knob (R2,
        # MinIO) do not implement canned ACLs at all, so the header would be
        # noise at best there; and an endpoint-routed AWS target (the retired
        # ``data.source.coop`` proxy hop was reached exactly that way) is
        # excluded with them. Retiring that hop -- and the egress it paid -- is
        # what this header buys, so the exclusion costs nothing under the
        # no-egress rule.
        kwargs["client_options"] = _with_bucket_owner_acl(kwargs.get("client_options"))

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


def _with_bucket_owner_acl(client_options):
    """Merge the issue #495 canned ACL into obstore ``client_options``.

    Additive rather than replacing: any other client option survives, and an
    ``x-amz-acl`` the caller set explicitly wins -- the header is a default for
    external targets, not an override of a caller who knows better.

    Caller header keys are lowercased first, which is lossless (obstore
    lowercases them itself) and is what makes that precedence real: a
    mixed-case ``X-Amz-Acl`` would slip past the ``setdefault`` and then lose
    to our key inside obstore, where last insertion wins.

    Passing ``{"x-amz-acl": None}`` in ``default_headers`` REMOVES the header
    instead of setting one -- the escape hatch for a future external AWS target
    that must send no ACL at all. It exists because neither obstore-legal value
    can express absence (obstore rejects a ``None`` header value, and ``""`` is
    a live empty ``x-amz-acl`` S3 rejects), and it keeps the derivation itself
    knob-free: no config surface, no per-run flag.
    """
    options = dict(client_options or {})
    headers = {str(k).lower(): v for k, v in (options.get("default_headers") or {}).items()}
    headers.setdefault("x-amz-acl", _BUCKET_OWNER_ACL)
    if headers["x-amz-acl"] is None:
        del headers["x-amz-acl"]
    options["default_headers"] = headers
    return options


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
