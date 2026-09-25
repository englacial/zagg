"""Lifecycle touch for skip-if-current leaf units (issue #388 phase 3).

A skipped unit writes nothing — correct for the store, but invisible to
bucket lifecycle policies: sliderule-public purges older objects, so a
re-request that no-ops the pipeline must still reset the purge clock on the
products it just certified current. The touch refreshes ``LastModified``
across the unit's WHOLE footprint (lifecycle rules act per object):

- the leaf ``.zarr`` tree — commit stamp, arrays, and the in-leaf
  ``coverage.moc`` sidecar included (they are objects under the prefix);
- the D20 stats sidecar, its issue #388 granule-id list, and the D22 sub-map
  SIBLINGS of the leaf (:func:`zagg.telemetry.sidecar_key` /
  :func:`zagg.telemetry.granule_ids_key` / :func:`zagg.sweep.submap_key`, the
  spec-keyed naming seams);
- the issue #383 leaf column tree plus its own stats sidecar, when the run
  declares one — the identity gate has already verified declaration and
  artifact agree (``column-drift`` never reaches the touch), so the caller
  simply passes the column path on the declared arm and ``None`` otherwise.

**Not applicable to a PUBLISHED bucket** (issue #495 phase 4). The touch
exists to defeat an expiration rule; an archival published bucket has none, so
there is nothing to defeat — and on a VERSIONED bucket the self-copy is
actively harmful, for the reason the ``versioned`` bullet below already names:
it writes a new FULL-SIZE version and demotes the old one to noncurrent, where
it keeps consuming storage. Source Cooperative's bucket is versioned, so one
full-skip run over the CA store would add ~332 GB of noncurrent versions and
double the footprint — invisibly, since ``ListObjectsV2`` reports only current
versions — on a bucket where AWS pays the bill as an Open Data sponsor. See
:func:`_skip_published`.

Mechanism: local stores get ``os.utime``; S3 gets a server-side self-copy
(``CopyObject`` onto itself with ``MetadataDirective="REPLACE"`` — S3
rejects an identity copy without it; already a ``boto3`` dependency, and
obstore's ``copy`` exposes no metadata directive). The self-copy preserves
content (and the ETag for non-multipart objects).

``MetadataDirective="REPLACE"`` covers SYSTEM-defined metadata too, not just
``x-amz-meta-*``, so the copy request is what decides the new object's
properties. Exactly what that costs, and what this module does about it:

- **user metadata** is replaced with none — zagg's writers attach none;
- **storage class** would otherwise reset to STANDARD on every touch,
  silently defeating a lifecycle *transition* policy and re-paying the
  transition each skip run. SOLVED: the class is echoed back from the LIST
  entry (tree arm) or a ``HeadObject`` (named objects). Still a caveat: an
  object already in ``GLACIER``/``DEEP_ARCHIVE`` cannot be copied at all
  (``InvalidObjectState``) and counts as failed;
- **ACL**: NOT preserved — ``CopyObject`` grants the destination the
  requester's default private ACL unless ``x-amz-acl``/``x-amz-grant-*``
  rides the request. SOLVED for the case that matters: on an
  INJECTED-CREDENTIAL external target the copy carries
  ``x-amz-acl: bucket-owner-full-control``, so a touch cannot claw back
  ownership the writing PUT handed to the bucket owner. ``zagg.store``'s
  issue #495 predicate has a second arm — an ambient write to a published
  bucket — that is UNREACHABLE from here since phase 4: the touch skips
  those paths before any copy. The predicate is still imported whole, and
  still resolved against the destination bucket, only so this raw-boto3
  path cannot drift from the store seam's. Still a caveat for the
  in-account case: a public-read-BY-ACL bucket would have the touched
  objects made private (a no-op on ``BucketOwnerEnforced``, the default
  since 2023);
- **SSE-KMS**: a self-copy re-encrypts under the BUCKET DEFAULT key, not
  the source object's key, and needs ``kms:Decrypt`` +
  ``kms:GenerateDataKey``. Fails loudly into ``failed`` when the role lacks
  them. Documented, not solved;
- a **versioned** bucket mints a new object version per touch, and an
  object >= 5 GB would need a multipart copy and simply counts as failed
  (leaf objects never approach it).

The unit footprint is not the whole store: the STORE-ROOT objects have no
unit that owns them, and an all-skip run re-PUTs none of them
(:func:`zagg.hive.ensure_manifest` early-returns on a frozen-key match, and
every skip-capable run has ``overwrite=False``). :func:`touch_store_root`
covers them once per run — the D6 ``morton_hive.json``, the
``aggregation.yaml`` semantic core, and the root ``coverage.moc`` — and the
local backend calls it from the same post-units wrap-up that already unions
the root MOC (that process is also the worker; the D8 orchestrator-no-write
rule constrains only the Lambda paths, which therefore do NOT get this
today — see the PR #397 question).

Everything here is BEST-EFFORT and fail-open (the D9 posture): a failed
touch logs and counts, and the run degrades to today's behavior — it never
fails the unit, never un-skips it, and :func:`touch_current_unit` never
raises out of the seam. An ABSENT path is neither touched nor failed: a
unit legitimately has no sub-map (non-HEALPix grids, id-less entries).

Documented gaps, not solved here: the §7 sweep's ancestor-node rollup
sidecars — ``{family}.rollup.json`` for every
:data:`zagg.sweep.DEFAULT_FAMILIES` entry, plus each pass's root
``sweep_stats_{ts}.json`` — belong to no unit footprint and are not
store-root objects either, and a skip produces zero sweep dirtiness by
construction, so a skip-only store ages its whole pyramid above the leaves
while the leaves and the root survive. A repeat sweep does not refresh them
either (an unchanged tree recomputes but PUTs nothing). Walking them would
mean a store-wide walk this module deliberately does not own (PR #397
question).
"""

from __future__ import annotations

import logging
import os
import threading

logger = logging.getLogger(__name__)

#: Process-wide S3 clients, keyed by the store kwargs that built them. The
#: touch runs once per (shard, window) unit and the local dispatcher runs
#: those units in a ThreadPoolExecutor, so a per-call client would mean one
#: ~0.1-0.3 s ``boto3.client()`` construction per unit (minutes of pure
#: construction on a few-thousand-shard all-skip rerun) AND concurrent
#: construction off the shared default session, which botocore does not
#: guarantee is thread-safe. A BUILT client is safe to share across threads;
#: building is what needs the lock (and its own ``Session``).
_CLIENTS: dict = {}
_CLIENT_LOCK = threading.Lock()


def touch_current_unit(
    leaf_path,
    *,
    column_path=None,
    sidecar_spec=None,
    store_kwargs=None,
    policy="auto",
) -> dict:
    """The issue #388 skip-path touch for ONE ``(shard, window)`` unit.

    Assembles the unit's footprint from its leaf path — the leaf tree, the
    stats-sidecar and sub-map siblings named by ``sidecar_spec`` (the
    manifest spec in effect), and ``column_path``'s tree + sidecar when the
    caller passes one — then touches it via :func:`touch_unit_footprint`.
    Returns the counts dict; NEVER raises (assembly errors count as one
    failure and the touch is skipped).
    """
    try:
        from zagg.column import _sidecar_name as column_sidecar_name
        from zagg.icechunk_ladder import leaf_refs_key
        from zagg.sweep import submap_key
        from zagg.telemetry import granule_ids_path, sidecar_path

        prefix, _, name = str(leaf_path).rstrip("/").rpartition("/")
        trees = [str(leaf_path)]
        objects = [
            sidecar_path(str(leaf_path), sidecar_spec),
            # The recorded id list (issue #388) ages with the sidecar it
            # pairs with: losing it to a lifecycle rule would silently
            # disarm the contraction guard on a leaf that is otherwise fresh.
            granule_ids_path(str(leaf_path), sidecar_spec),
            f"{prefix}/{submap_key(name, sidecar_spec)}",
            # The Icechunk ref sidecar (issue #580 phase 6): the ladder's
            # input for this leaf; a skipped leaf must keep it as fresh as
            # the leaf, or the next staged sweep gathers a hole.
            f"{prefix}/{leaf_refs_key(name, sidecar_spec)}",
        ]
        if column_path:
            trees.append(str(column_path))
            # The column's D20 sidecar, named by the seam that OWNS the
            # grammar rather than re-derived here: a drifted name reads as
            # ABSENT (neither touched nor failed, by design), so a second
            # source would stop touching the sidecar in total silence.
            col_prefix, _, col_name = str(column_path).rstrip("/").rpartition("/")
            objects.append(f"{col_prefix}/{column_sidecar_name(col_name)}")
    except Exception as e:
        logger.warning(f"lifecycle touch skipped — footprint unresolved (fail-open): {e}")
        return {"touched": 0, "failed": 1}
    return touch_unit_footprint(trees, objects, store_kwargs=store_kwargs, policy=policy)


def touch_store_root(store_root, *, store_kwargs=None, policy="auto") -> dict:
    """Touch the store-ROOT objects no unit footprint covers (issue #388).

    A run whose every unit skipped writes nothing at the root either:
    :func:`zagg.hive.ensure_manifest` accepts a frozen-key-matching manifest
    with no second PUT (and ``aggregation.yaml`` is written only inside that
    PUT branch), and every skip-capable run has ``overwrite=False``. So the
    REQUIRED reader-facing ``morton_hive.json`` (D6) would expire under a
    lifecycle rule while 100% of the data objects it indexes are fresh.
    Three objects, O(1) requests, all fail-open (an absent root
    ``coverage.moc`` — ``output.coverage_moc`` off — is not a failure).
    """
    try:
        from zagg.hive import AGGREGATION_CORE_NAME, MANIFEST_NAME, ROOT_COVERAGE_NAME

        root = str(store_root).rstrip("/")
        names = (MANIFEST_NAME, AGGREGATION_CORE_NAME, ROOT_COVERAGE_NAME)
    except Exception as e:
        logger.warning(f"store-root touch skipped — names unresolved (fail-open): {e}")
        return {"touched": 0, "failed": 1}
    return touch_unit_footprint(
        [], [f"{root}/{name}" for name in names], store_kwargs=store_kwargs, policy=policy
    )


def touch_unit_footprint(trees, objects, *, store_kwargs=None, policy="auto") -> dict:
    """Touch every object under each of ``trees`` + each single ``objects`` path.

    Paths are either local filesystem paths or ``s3://`` URLs; ``store_kwargs``
    is the writers' store-kwargs dict (``region`` / ``credentials`` /
    ``endpoint_url``) and keys the S3 client. Returns ``{"touched": n,
    "failed": m}`` and never raises — an unexpected error (client build, LIST
    fault) counts one failure and abandons the remainder (best-effort).

    A path the touch does not apply to under ``policy`` — a published bucket
    under ``auto`` (issue #495 phase 4), any path at all under ``never`` (issue
    #501) — is NOT APPLICABLE rather than failed (:func:`_touch_applies`): it is
    counted under a
    ``"skipped_paths"`` key, added only when non-zero, and never touches
    ``"failed"`` — so a skipped run cannot be read as an error in the run
    parquet or the status objects. That key counts INPUT PATHS, not objects,
    unlike its two siblings: a tree path is one skip here but would have been
    one ``"touched"`` per listed key, so the three are not summable (review
    finding on PR #496). It is omitted when zero to leave every existing
    caller's dict byte-identical; the callers that record it are named in
    :func:`zagg.runner._identity_counts`.
    """
    counts = {"touched": 0, "failed": 0}
    skipped = 0
    skipped_buckets: set = set()
    try:
        for tree in trees:
            bucket = _split_s3(tree)[0] if _is_s3(tree) else None
            if not _touch_applies(bucket, policy):
                skipped += 1
                if bucket is not None:
                    skipped_buckets.add(bucket)
                continue
            if _is_s3(tree):
                _touch_s3_tree(
                    _client(store_kwargs), tree, counts, _copy_acl(store_kwargs, bucket), policy
                )
            else:
                _touch_local_tree(tree, counts)
        for obj in objects:
            bucket, key = _split_s3(obj) if _is_s3(obj) else (None, None)
            if not _touch_applies(bucket, policy):
                skipped += 1
                if bucket is not None:
                    skipped_buckets.add(bucket)
                continue
            if _is_s3(obj):
                acl = _copy_acl(store_kwargs, bucket)
                _touch_s3_object(_client(store_kwargs), bucket, key, counts, acl=acl, policy=policy)
            else:
                _touch_local_object(obj, counts)
    except Exception as e:
        counts["failed"] += 1
        logger.warning(f"lifecycle touch aborted mid-footprint (fail-open, issue #388): {e}")
    if skipped:
        counts["skipped_paths"] = skipped
        # This fires once per (shard, window) unit — thousands of times on an
        # all-skip run — so it IDENTIFIES rather than explains: the rationale
        # lives in _touch_applies and docs/hive_layout.md, where a reader can
        # afford it. It names the POLICY, not the inference: a skip is now
        # either the issue #495 phase 4 published-bucket call or the operator's
        # own `output.touch` (issue #501), and under `never` there may be no
        # bucket at all (a local store) — so the bucket list appears only when
        # non-empty, and when it does it is what the operator needs at 3 a.m.
        # to tell the published target from something new.
        where = f" on bucket(s) {sorted(skipped_buckets)}" if skipped_buckets else ""
        logger.info(
            f"lifecycle touch not applicable for {skipped} path(s) under "
            f"policy={policy!r}{where} — see zagg.lifecycle._touch_applies (issue #501)"
        )
    return counts


def _is_s3(path) -> bool:
    return str(path).startswith("s3://")


def _split_s3(path) -> tuple:
    bucket, _, key = str(path)[len("s3://") :].partition("/")
    return bucket, key


def _client(store_kwargs):
    """The process-wide S3 client for these store kwargs (never built locally).

    Built once per distinct store-kwargs identity and shared thereafter, with
    construction serialized — see ``_CLIENTS``.
    """
    store_kwargs = store_kwargs or {}
    creds = store_kwargs.get("credentials") or {}
    key = (
        store_kwargs.get("region"),
        store_kwargs.get("endpoint_url"),
        creds.get("accessKeyId"),
        creds.get("secretAccessKey"),
        creds.get("sessionToken"),
    )
    with _CLIENT_LOCK:
        if key not in _CLIENTS:
            _CLIENTS[key] = _s3_client(store_kwargs)
        return _CLIENTS[key]


def _skip_published(bucket) -> bool:
    """Whether ``bucket`` is a published target the touch must leave alone.

    Guarded on membership in :data:`zagg.store._PUBLISHED_BUCKETS`, NOT on
    ``_external_target(...)``, and the difference is load-bearing:
    ``_external_target`` is also true for injected-credential targets, whose
    lifecycle and versioning configuration we do not know. Skipping the touch
    on one of those could let a collaborator's data expire — the exact failure
    the touch exists to prevent. Only the published set is known not to expire.

    Note the set is doing DOUBLE DUTY here. It enumerates buckets that are both
    (a) not ours, which is what the canned ACL keys on, and (b) not expiring,
    which is what this skip keys on. Those two properties coincide today rather
    than being the same thing; if they ever diverge — a published bucket that
    does expire, or a non-owned bucket that does not — the touch needs its own
    set and this predicate must stop borrowing that one.
    """
    from .store import _PUBLISHED_BUCKETS

    return bucket in _PUBLISHED_BUCKETS


def _touch_applies(bucket, policy="auto") -> bool:
    """Whether the touch applies to this path under ``policy`` (issue #501).

    The operator's ``output.touch`` declaration layered ON TOP of the issue
    #495 phase 4 inference, which it overrides rather than replaces:

    ``auto``
        Fall through to :func:`_skip_published` — the phase 4 behaviour
        verbatim, and the default, so every config predating the knob is
        byte-identical.
    ``always``
        Applies everywhere, published buckets included. For an un-negotiated
        external target with an expiry rule the bucket name does not encode.
    ``never``
        Applies nowhere — LOCAL paths included, since "never touch" is a
        statement about the run, not about S3.

    ``bucket`` is None for a local path, which only ``never`` acts on.
    """
    if policy == "never":
        return False
    if policy == "always":
        return True
    return not (bucket is not None and _skip_published(bucket))


def _copy_acl(store_kwargs, bucket) -> str | None:
    """Canned ACL the self-copy must carry, or ``None`` (issue #495).

    ``CopyObject`` CREATES an object, so on a cross-account target it re-creates
    every touched object owned by THIS account under the requester's default
    private ACL -- stripping the ownership the writing store handed to the
    bucket owner, and doing it silently (the touch is fail-open). Predicate and
    value both come from :mod:`zagg.store`, the seam every store write already
    goes through, so this raw-boto3 path cannot drift from it.

    ``bucket`` is the DESTINATION being touched, and since issue #501 the
    BUCKET arm of ``_external_target`` is LIVE here — do not delete this
    argument. Under ``output.touch: always`` (:func:`_touch_applies`) a
    published bucket reaches this function, and under the ambient execution
    role — no injected credentials — the bucket arm is the ONLY arm that can
    fire. Dropping the argument would therefore strip
    ``bucket-owner-full-control`` from every object an ``always`` run
    self-copies to Source Cooperative: the exact fail-open, invisible ownership
    strip the phase-4 grant round added this ACL to prevent. Pinned by
    ``test_policy_always_touches_the_published_destination_too``. It is resolved
    per path rather than once per call because one footprint's paths need not
    share a bucket.
    """
    from .store import _BUCKET_OWNER_ACL, _external_target

    store_kwargs = store_kwargs or {}
    if _external_target(store_kwargs.get("credentials"), store_kwargs.get("endpoint_url"), bucket):
        return _BUCKET_OWNER_ACL
    return None


def _s3_client(store_kwargs: dict):
    """boto3 S3 client from the writers' store-kwargs (camelCase credentials).

    Built through its OWN ``boto3.session.Session()`` rather than the module
    -level default session: callers reach here from worker threads, and
    concurrent construction off the shared default session is the documented
    botocore hazard (its failure would be swallowed by the fail-open counting
    into a silently unrefreshed object). ``_CLIENT_LOCK`` serializes this.
    """
    import boto3

    kwargs: dict = {}
    if store_kwargs.get("region"):
        kwargs["region_name"] = store_kwargs["region"]
    if store_kwargs.get("endpoint_url"):
        kwargs["endpoint_url"] = store_kwargs["endpoint_url"]
    creds = store_kwargs.get("credentials")
    if creds:
        kwargs["aws_access_key_id"] = creds["accessKeyId"]
        kwargs["aws_secret_access_key"] = creds["secretAccessKey"]
        if creds.get("sessionToken"):
            kwargs["aws_session_token"] = creds["sessionToken"]
    return boto3.session.Session().client("s3", **kwargs)


def _touch_local_tree(root, counts) -> None:
    if not os.path.isdir(root):
        return
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            _touch_local_object(os.path.join(dirpath, name), counts)


def _touch_local_object(path, counts) -> None:
    if not os.path.isfile(path):
        return
    try:
        os.utime(path, None)
        counts["touched"] += 1
    except OSError as e:
        counts["failed"] += 1
        logger.warning(f"lifecycle touch failed for {path} (fail-open): {e}")


def _touch_s3_tree(s3, path, counts, acl=None, policy="auto") -> None:
    bucket, key = _split_s3(path)
    prefix = key.rstrip("/") + "/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents") or []:
            # The LIST already carries the class; echoing it back is what
            # keeps the self-copy from demoting the object to STANDARD.
            _touch_s3_object(
                s3, bucket, entry["Key"], counts, entry.get("StorageClass"), acl, policy
            )


def _touch_s3_object(s3, bucket, key, counts, storage_class=None, acl=None, policy="auto") -> None:
    if not _touch_applies(bucket, policy):
        # Belt and braces (issue #495 phase 4, review finding). The counting
        # guard lives in touch_unit_footprint — the only route here today —
        # but this is the one function that issues CopyObject, so putting the
        # invariant HERE makes it unbypassable by a future entry point rather
        # than a property of two ``if``s in one loop body. Blast radius if it
        # ever leaked: ~332 GB of noncurrent versions per full-skip run,
        # invisible to ListObjectsV2, on a bucket AWS sponsors. It counts
        # NOTHING — the path-based accounting above already counted this
        # path, and counting again would double it — and it is unreachable in
        # practice, so a hit means a new caller skipped the seam: WARNING.
        logger.warning(
            f"lifecycle touch reached the copy seam for s3://{bucket}/{key} under "
            f"policy={policy!r} — refused (issue #495 phase 4, issue #501); a caller "
            "bypassed touch_unit_footprint's guard"
        )
        return
    try:
        if storage_class is None:
            # A named object has no LIST entry: one HEAD buys its class (and
            # detects absence a request earlier than the copy would).
            storage_class = s3.head_object(Bucket=bucket, Key=key).get("StorageClass")
        params = {
            "Bucket": bucket,
            "Key": key,
            "CopySource": {"Bucket": bucket, "Key": key},
            "MetadataDirective": "REPLACE",
            # S3 omits StorageClass for STANDARD in both LIST and HEAD.
            "StorageClass": storage_class or "STANDARD",
        }
        if acl:
            # The re-created object keeps the bucket owner's ownership
            # (issue #495); omitted in-account, where there is nothing to hand
            # over and an ACL would be a change the touch has no business making.
            params["ACL"] = acl
        s3.copy_object(**params)
        counts["touched"] += 1
    except Exception as e:
        code = ""
        response = getattr(e, "response", None)
        if isinstance(response, dict):
            code = (response.get("Error") or {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return  # an absent sibling (e.g. no sub-map) is not a failure
        counts["failed"] += 1
        logger.warning(f"lifecycle touch failed for s3://{bucket}/{key} (fail-open): {e}")


__all__ = ["touch_current_unit", "touch_store_root", "touch_unit_footprint"]
