"""Store factory for opening Zarr stores from path strings."""

import copy
import threading
from datetime import timedelta
from pathlib import Path

from zarr.abc.store import Store
from zarr.storage import LocalStore, ObjectStore

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
# ``UnknownConfigurationKeyError``) and no per-request one either -- an
# ``x-amz-acl`` passed through ``put(attributes=...)`` is not recognised, so it
# lands as USER METADATA (``x-amz-meta-x-amz-acl``) and sets no ACL at all. The
# header therefore rides as an obstore ``default_headers`` entry. It reaches the
# wire on every request the handle makes, but it only reaches the SIGNATURE on
# some of them, and issue #522 is that difference:
#
# * every request EXCEPT ``ListObjectsV2`` carries ``x-amz-acl`` inside
#   ``SignedHeaders`` -- the keyed ones (``PUT``, ``CreateMultipartUpload``,
#   ``GET``, ``HEAD``) and the bucket-level ``POST ?delete`` bulk delete alike;
# * ``ListObjectsV2`` picks the default headers up after object_store has
#   signed, so the header is present-but-unsigned.
#
# S3 rejects the second shape outright -- ``403 AccessDenied``, "There were
# headers present in the request which were not signed",
# ``<HeadersNotSigned>x-amz-acl</HeadersNotSigned>`` -- so a handle that carries
# the ACL cannot list. That is not a corner: the per-leaf template guard lists
# the digit tree and the status poller lists its channel, which is how 2,726/2,726
# fleet workers died on the first source.coop build.
#
# So the header rides a SEPARATE handle from the one that reads (issue #522):
# :func:`_s3_object_store` returns a clean store and hangs an ACL-bearing twin
# off it (:data:`_ACL_WRITE_STORE_ATTR`), and object-creating requests -- and
# only those -- are routed to the twin by :func:`put_object` and
# :class:`_AclWriteObjectStore`. Reads and lists never see an ACL header, which
# also retires the old inert-header exception: ``open_object_store`` has no
# read-only concept, so ``temporal.open_dataset``'s NetCDF branch (a pure GET of
# a consumer INPUT bucket, issue #223) used to send the header and now does not.
#
# And that handle issues exactly ONE operation, ``PutObject`` (issue #534).
# Signing the header is necessary but not sufficient, because S3's rules differ
# per operation: ``x-amz-acl`` is legal on ``PutObject`` and on
# ``CreateMultipartUpload``, and ILLEGAL on ``UploadPart``, which answers
# ``400 InvalidArgument``, "The specified header is not valid in this context",
# ``<ArgumentName>x-amz-acl</ArgumentName>``. obstore has no per-request ACL and
# no way to withhold a default header from one operation, so a multipart write on
# this handle cannot be made legal -- it can only be avoided. Both write seams
# therefore pass ``use_multipart=False`` (:func:`put_object` and
# :class:`_AclWriteObjectStore`), which leaves ``PutObject`` as the only request
# the twin is capable of making. That is deliberately a property of the handle
# rather than a list of permitted operations: enumerating the operations once,
# and missing ``UploadPart``, is exactly how #534 reached the fleet, where it
# failed 52 of 60 sampled shards on the CA GEDI build.
#
# The cost is a multipart upload's parallelism, on objects that do not need it:
# zagg publishes 1-17 MB chunk objects (an ATL03 leaf's largest measured 16.9 MB,
# a GEDI ``rx_flux`` chunk ~11 MB) against obstore's 5 MiB multipart threshold,
# so only the handful of ragged chunks per leaf were multipart at all.
# :data:`_SINGLE_PUT_MAX_BYTES` is where that stops being true.
#
# The asymmetry above is pinned by ``tests/test_store_acl_signing.py`` against
# captured wire bytes, so an obstore change to it fails there rather than in a
# fleet run.
_BUCKET_OWNER_ACL = "bucket-owner-full-control"

# S3's ``PutObject`` ceiling. A larger object can only be written as a multipart
# upload, which is the one thing a handle carrying the canned ACL cannot do
# (issue #534) -- so this is not a limit to absorb quietly but the point at which
# publishing to an external bucket needs a different answer: a finer chunk grid,
# or a clean multipart write followed by a ``PutObjectAcl`` to hand ownership
# over (the grant already includes that action). Today's published objects are
# ~300x below it; the check exists so that the day one is not, it surfaces here
# with the key that caused it rather than as a 400 partway through a fleet run.
_SINGLE_PUT_MAX_BYTES = 5 * 1024**3

# Where the ACL-bearing twin hangs off the clean handle. An attribute rather
# than a registry so the pairing travels with the store -- including through
# ``pickle``, which zarr's ``ObjectStore`` uses to ship a store to a worker
# process, and which preserves an obstore store's Python attributes.
_ACL_WRITE_STORE_ATTR = "_zagg_acl_write_store"

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
        AWS credentials needed, e.g. binder).

    Notes
    -----
    A write target this account does not own makes the store send
    ``x-amz-acl: bucket-owner-full-control`` on the requests that CREATE
    objects, so the bucket owner owns what it writes (issue #495; see
    :data:`_BUCKET_OWNER_ACL`). Two shapes qualify: explicit ``credentials``
    without an ``endpoint_url``, and an ambient write to a bucket in
    :data:`_PUBLISHED_BUCKETS` (Source Cooperative, which the execution role now
    reaches directly). ``read_only=True`` suppresses it: a read opened with
    explicit credentials is the issue #223 consumer-INPUT channel (somebody
    else's input bucket, as ``temporal.open_dataset`` opens it), not a write
    target of ours.

    Reads and lists never carry the header -- S3 rejects an unsigned
    ``x-amz-acl`` on a ``ListObjectsV2``, and obstore cannot sign a default
    header there (issue #522) -- so such a target returns a
    :class:`_AclWriteObjectStore`, which reads through a clean handle and writes
    through an ACL-bearing twin. Callers see an ordinary Zarr ``Store``.

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
    ambient write to a published bucket gets the ACL handle here too, cached
    store included (the cache is keyed by path, and the path is what decides)
    (issue #495).

    The store RETURNED is always the clean one, because this route has no
    ``read_only`` concept and its reads must work (issue #522): write through
    :func:`put_object`, which routes to the ACL twin, and read through the
    returned store directly. That also settles the old exception -- a
    credentialed READER built here, notably ``temporal.open_dataset``'s NetCDF
    branch (a pure GET of a consumer-input bucket, issue #223), now sends no ACL
    header at all rather than an inert one.
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
    if read_only and kwargs.get("retry_config") is None:
        # Interactive read population: fail fast on a dead endpoint (comment
        # on the constant). Set here so _s3_object_store's write-policy
        # default doesn't kick in; an explicit caller retry_config still wins.
        kwargs["retry_config"] = _S3_READONLY_RETRY_CONFIG
    s3, acl_s3 = _s3_store_pair(path, credentials, endpoint_url, read_only, kwargs)
    if acl_s3 is None:
        return ObjectStore(store=s3, read_only=read_only)
    setattr(s3, _ACL_WRITE_STORE_ATTR, acl_s3)
    return _AclWriteObjectStore(s3, acl_s3, read_only=read_only)


class _AclWriteObjectStore(ObjectStore):
    """Zarr store whose writes carry the canned ACL and whose reads do not.

    zarr's obstore adapter drives every operation off a single ``self.store``,
    and an ACL-bearing handle cannot list (issue #522), so the two roles are
    split: ``self.store`` is the clean handle the inherited read, list and
    delete paths use unchanged, and the object-creating methods are re-pointed
    at a second adapter wrapping the ACL twin. Deletes stay on the clean handle
    deliberately: an ACL means nothing on a delete, and ``delete_dir`` LISTs
    before it deletes (``await obs.list(self.store, prefix).collect_async()``),
    so moving the delete surface to the twin would put a ``ListObjectsV2`` back
    on the ACL handle -- the one request that 403s. The bulk delete itself is
    not the hazard: ``tests/test_store_acl_signing.py`` measures ``POST
    ?delete`` signing the header fine.

    Both write methods are spelled out here rather than delegated to a second
    adapter, because the adapter cannot express the one thing that makes them
    legal: it calls ``put_async(store, key, buf)`` bare, and the ACL handle must
    pass ``use_multipart=False`` (issue #534, see :data:`_BUCKET_OWNER_ACL`).
    ``set_if_not_exists`` gets it too, although obstore already forces a
    non-multipart upload for any ``mode`` other than ``"overwrite"`` -- relying
    on that would leave the twin's single-operation property resting on an
    obstore implementation detail. The wire cannot tell the two apart, so
    ``tests/test_store_acl_seam.py`` spies on ``obstore.put_async`` and asserts
    the argument itself; drop it here and that test goes red.
    """

    def __init__(self, store, acl_store, *, read_only: bool = False) -> None:
        super().__init__(store, read_only=read_only)
        # The RAW obstore handle, not a second zarr adapter: the writes below
        # drive obstore directly, so an adapter around it would be an unused
        # wrapper whose own ``read_only`` flag could drift from this store's.
        self._acl_store = acl_store

    def with_read_only(self, read_only: bool = False):
        # docstring inherited
        return type(self)(self.store, self._acl_store, read_only=read_only)

    def __eq__(self, other: object) -> bool:
        # The inherited __eq__ compares only ``read_only`` and ``self.store``
        # -- and ``self.store`` is the CLEAN handle, which a plain ObjectStore
        # can hold too. So a plain store compares equal to this one while
        # writing owner-less objects, and any dedupe/cache/normalization keyed
        # on equality (zarr's StorePath, a future "we already hold this store"
        # check) can swap the twin away with no error and no log -- the silent
        # shape, not the 403 shape. The two are not interchangeable, so both
        # handles have to match (issue #522).
        return (
            isinstance(other, _AclWriteObjectStore)
            and super().__eq__(other)
            and self._acl_store == other._acl_store
        )

    async def set(self, key, value):
        # docstring inherited
        import obstore as obs

        self._check_writable()
        buf = value.as_buffer_like()
        _check_single_put(key, buf)
        await obs.put_async(self._acl_store, key, buf, use_multipart=False)

    async def set_if_not_exists(self, key, value):
        # docstring inherited
        import contextlib

        import obstore as obs

        self._check_writable()
        buf = value.as_buffer_like()
        _check_single_put(key, buf)
        with contextlib.suppress(obs.exceptions.AlreadyExistsError):
            await obs.put_async(self._acl_store, key, buf, mode="create", use_multipart=False)


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

    The returned store is the CLEAN handle; when the target needs the canned
    ACL its twin is attached as :data:`_ACL_WRITE_STORE_ATTR`, and
    :func:`put_object` is what routes an object-creating request to it
    (issue #522).
    """
    store, acl_store = _s3_store_pair(path, credentials, endpoint_url, read_only, kwargs)
    if acl_store is not None:
        setattr(store, _ACL_WRITE_STORE_ATTR, acl_store)
    return store


def _s3_store_pair(path, credentials, endpoint_url, read_only, kwargs):
    """Build ``(read_store, acl_write_store)`` for ``path``.

    ``acl_write_store`` is ``None`` unless the target needs the issue #495
    canned ACL, and ``kwargs`` is consumed (not copied) -- both callers pass
    their own dict.
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

    provider = None
    if not (credentials or endpoint_url or kwargs.get("skip_signature")):
        from obstore.auth.boto3 import Boto3CredentialProvider

        # Built once and shared by both handles: its ``__init__`` eagerly walks
        # the botocore credential chain (~300 ms), and the twin below must not
        # pay it a second time (issue #287).
        provider = Boto3CredentialProvider()

    def build(client_options):
        opts = dict(kwargs)
        # ...and re-copy the one nested value, because ``dict()`` is shallow:
        # without this both handles hold the SAME retry_config object, which
        # is a smaller instance of the aliasing the deepcopy above exists to
        # prevent. Runs twice per external store open, not per request.
        opts["retry_config"] = copy.deepcopy(opts["retry_config"])
        if client_options is None:
            opts.pop("client_options", None)
        else:
            opts["client_options"] = client_options
        if credentials or endpoint_url:
            named = {
                "bucket": bucket,
                "prefix": prefix,
                "region": region,
                # Path-style addressing: required for dotted bucket names (TLS)
                # and for non-AWS S3-compatible endpoints.
                "virtual_hosted_style_request": False,
            }
            if credentials:
                named["access_key_id"] = credentials["accessKeyId"]
                named["secret_access_key"] = credentials["secretAccessKey"]
                if credentials.get("sessionToken"):
                    named["session_token"] = credentials["sessionToken"]
            if endpoint_url:
                named["endpoint"] = endpoint_url
            return S3Store(**named, **opts)
        if provider is None:
            # Anonymous read of a public bucket: no credential provider —
            # Boto3CredentialProvider raises without ambient AWS credentials,
            # which anonymous environments (e.g. binder) lack by definition.
            # Addressing style is deliberately left to obstore's default, exactly
            # matching the construction the example notebooks used directly
            # (unlike the credentialed branch, which pins path-style above).
            return S3Store(bucket, prefix=prefix, region=region, **opts)
        return S3Store(
            bucket,
            prefix=prefix,
            region=region,
            credential_provider=provider,
            **opts,
        )

    if not (
        _external_target(credentials, endpoint_url, bucket)
        and not read_only
        and not kwargs.get("skip_signature")
    ):
        return build(kwargs.get("client_options")), None

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
    #
    # TWO handles, not one (issue #522): the returned store is clean, so its
    # reads and lists are ordinary signed requests, and the ACL rides a twin
    # used for object-creating requests only.
    read_options, write_options = _acl_client_options(kwargs.get("client_options"))
    return build(read_options), (None if write_options is None else build(write_options))


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


def _acl_client_options(client_options):
    """Split ``client_options`` into a read pair and a write pair (issue #522).

    The write options are exactly what issue #495 has always produced; the read
    options are the caller's own, with any ``x-amz-acl`` removed -- because that
    is the header S3 refuses to accept unsigned on a ``ListObjectsV2``, and the
    handle that lists is the one that must not carry it.

    Returns ``(read_options, write_options)``. ``read_options`` is ``None`` when
    nothing is left, so an external target's READ handle is constructed
    byte-identically to an in-account one (no ``client_options`` kwarg at all).
    ``write_options`` is ``None`` when the merge leaves no ACL to send -- the
    ``{"x-amz-acl": None}`` escape hatch -- in which case there is no second
    handle to build and the read handle is the whole story.
    """
    read = dict(client_options or {})
    headers = {str(k).lower(): v for k, v in (read.get("default_headers") or {}).items()}
    headers.pop("x-amz-acl", None)
    if headers:
        read["default_headers"] = headers
    else:
        read.pop("default_headers", None)
    write = _with_bucket_owner_acl(client_options)
    if "x-amz-acl" not in write["default_headers"]:
        # The escape hatch: nothing to send, so no twin -- and the read handle
        # takes the SAME cleaned options as any other branch, which is what
        # collapses an empty ``default_headers`` to no ``client_options`` at
        # all rather than to ``{"default_headers": {}}``.
        return (read or None), None
    return (read or None), write


def acl_write_store(store):
    """The handle an object-creating request must use (issue #522).

    The ACL-bearing twin when ``store`` has one, else ``store`` itself -- so
    every caller can route writes through this unconditionally without knowing
    whether its target is external.
    """
    return getattr(store, _ACL_WRITE_STORE_ATTR, store)


def _check_single_put(key, value):
    """Refuse a payload only a multipart upload could carry (issue #534).

    Sized off the buffer where it can be, and skipped where it cannot: obstore
    accepts paths, file objects and iterators too, and none of zagg's write
    seams pass one. A miss here costs the clear error, not correctness -- S3
    still rejects the request.
    """
    size = getattr(value, "nbytes", None)
    if not isinstance(size, int):
        size = len(value) if isinstance(value, bytes | bytearray) else None
    if size is not None and size > _SINGLE_PUT_MAX_BYTES:
        raise ValueError(
            f"{key}: {size} bytes exceeds the {_SINGLE_PUT_MAX_BYTES}-byte single-PUT "
            "ceiling, and a write carrying the bucket-owner canned ACL cannot use a "
            "multipart upload (issue #534)"
        )


def put_object(store, key, value, **kwargs):
    """``obstore.put`` onto the handle that carries the canned ACL (issue #522).

    Every raw-obstore write in zagg goes through here rather than calling
    ``obstore.put`` directly -- status envelopes, hive manifests, coverage and
    stats sidecars, sweep rollups, leases, the temporal tabular object. Reads
    keep using the store the caller already holds, which is the clean one.
    ``tests/test_store.py`` fails the build if a direct ``obstore.put`` is
    reintroduced, because a missed site publishes an object Source Cooperative
    cannot manage and says nothing about it. A second guard there covers the
    boto3 writes the first cannot see (it scans obstore names only), against an
    allowlist naming why each of the two that exist may stand outside this seam
    -- so a new one fails the build rather than quietly publishing an
    owner-less object.

    A write that lands on the ACL twin is forced to a single ``PutObject``
    (issue #534). Most objects on this route are small JSON, but not all of them
    -- the inline index buffer, the temporal tabular object and a column
    backfill payload can all cross obstore's 5 MiB multipart threshold, and any
    of them multiparting would fail the same way a chunk write did.
    """
    import obstore

    target = acl_write_store(store)
    if target is not store:
        # Overridden, not defaulted: ``use_multipart=True`` on this handle is a
        # request for a 400, so there is nothing for a caller to know better
        # about. In-account targets keep obstore's own choice untouched.
        kwargs["use_multipart"] = False
        _check_single_put(key, value)
    return obstore.put(target, key, value, **kwargs)


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


__all__ = ["acl_write_store", "open_object_store", "open_store", "parse_s3_path", "put_object"]
