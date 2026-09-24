"""Icechunk companion repos: the per-order virtual-ref index (spec §11, issue #580).

A morton hive is many leaf zarrs. The companion repository at
``{store_root}/icechunk/{order}/`` presents every leaf of one order as ONE
zarr hierarchy by recording each leaf's inner chunks as Icechunk **virtual
chunk references** — ``(location, offset, length)`` byte ranges into the leaf
objects that already exist. Stage 1 is refs-only and additive: the leaves stay
the normative, self-describing data plane; the repo is a derived index.

Two entry points, both fleet-first and worker-side (the dispatcher never
writes, D8):

- :func:`init_repo` — the once-per-run initialization (``mode="icechunk_init"``
  on Lambda, in-process on the local backend): create-or-open the repo, define
  every array node re-rooted on the whole order (§11.2), the manifest split
  (§11.5) and the virtual chunk container (§11.3), commit ``init {run_id}``.
  Idempotent: an initialized repo is reopened, never re-templated.
- :func:`record_leaf` — after a leaf's commit stamp lands, size and index the
  objects the leaf wrote (one HEAD + one ranged GET of the shard-index suffix
  per sharded array, one LIST per regular array), write the refs at the leaf's
  global chunk range (§11.3, ``r·C + j``) and commit ``leaf {decimal}`` with
  rebase-on-conflict. Fail-open at the caller (D9): the leaf never fails on
  the index.

Concurrency: leaves write disjoint chunk ranges of the same arrays, so a lost
compare-and-swap on the branch ref is a local rebase + retry
(``ConflictDetector`` reports no conflict). Icechunk's local-filesystem
storage is NOT safe for concurrent commits (it says so on open), so the local
backend's thread pool serializes its commits through a per-repo-path lock
(:func:`_local_commit_lock`) — process-local, and only against the repo being
written; object stores use conditional writes and need no lock.
"""

from __future__ import annotations

import contextlib
import logging
import math
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

#: The ``zagg_icechunk`` root-attrs block's revision token (spec §11.1).
ICECHUNK_SPEC = "zagg-icechunk/1"
#: Root-group attrs key carrying the block.
ICECHUNK_ATTR = "zagg_icechunk"
#: The one branch every commit lands on (§11.4).
BRANCH = "main"
#: Icechunk's configurable default ``min_num_chunks`` below which a manifest
#: carries no location dictionary (§11.5).
LOCATION_DICT_MIN_CHUNKS = 1000
#: Margin over that gate so absent inner chunks (no ref) cannot drop a
#: manifest under it (§11.5).
LOCATION_DICT_MARGIN = 4
#: Rebase-and-retry ceiling on a leaf commit (the issue #580 ruling: icechunk's
#: own rebase loop, no backoff/jitter machinery).
REBASE_TRIES = 20
#: zarr v3 sharding-index sentinel for an absent inner chunk (spec §1.5).
_ABSENT = np.uint64(2**64 - 1)
#: ``(offset, nbytes)`` u64 pair per inner chunk, plus the crc32c suffix.
_INDEX_ENTRY_BYTES = 16
_INDEX_CRC_BYTES = 4
#: The ONE shard-index layout this module knows how to read (spec §1.5): the
#: index is the object's trailing suffix, uncompressed little-endian u64 pairs
#: with a crc32c. Both facts ride the array's own codec config, so the plan
#: checks them rather than assuming them.
_INDEX_LOCATION = "end"
_INDEX_CODECS = [
    {"name": "bytes", "configuration": {"endian": "little"}},
    {"name": "crc32c"},
]
#: One commit lock PER local repo path, guarded by a meta-lock: icechunk's
#: local-filesystem storage is unsafe for concurrent commits, but that is a
#: per-repo fact, so two runs into different stores in one interpreter (a
#: notebook, a test session, ``demo/``) must not serialize against each other.
#: Process-local only — two PROCESSES writing one local repo are still unsafe
#: and nothing here prevents that; object stores use conditional writes and
#: take no lock at all.
_LOCAL_COMMIT_LOCKS: dict[str, threading.Lock] = {}
_LOCAL_COMMIT_LOCKS_GUARD = threading.Lock()


def _local_commit_lock(repo_path: str) -> threading.Lock:
    with _LOCAL_COMMIT_LOCKS_GUARD:
        return _LOCAL_COMMIT_LOCKS.setdefault(repo_path, threading.Lock())


def repo_path(store_root: str, order: int) -> str:
    """``{store_root}/icechunk/{order}`` — the repository root (§11.1)."""
    from zagg.hive import ICECHUNK_DIR_NAME

    return f"{store_root.rstrip('/')}/{ICECHUNK_DIR_NAME}/{int(order)}"


def container_prefix(store_root: str) -> str:
    """The virtual chunk container's ``url_prefix``: the store root, trailing ``/``."""
    root = store_root.rstrip("/")
    if root.startswith("s3://"):
        return root + "/"
    return f"file://{Path(root).resolve()}/"


def _ceil_second(when):
    """``when`` ceiled to the next whole second — the ``file://`` checksum (§11.3).

    Icechunk compares a recorded ``LastUpdatedAt`` checksum against the
    object's ``last_modified`` at WHOLE-SECOND granularity, so an object's
    exact sub-second mtime fails the object it was read from. The ceiling
    passes an unchanged object and still fails any rewrite landing a second
    or more later; a rewrite inside the same second is the form's caveat.
    """
    if when.microsecond == 0:
        return when
    return when.replace(microsecond=0) + timedelta(seconds=1)


def split_exponent(shard_order: int, chunk_order: int) -> int:
    """``m`` such that one manifest spans ``4^m`` chunks — one order-``(chunk_order − m)`` cell.

    The smallest ``m`` clearing the location-dictionary gate with margin
    (``4^m >= LOCATION_DICT_MARGIN · LOCATION_DICT_MIN_CHUNKS``), never below a
    whole leaf (``chunk_order − shard_order``, so every leaf commit rewrites
    whole manifests) and never above a whole base cell (``chunk_order``).
    """
    m0 = math.ceil(math.log(LOCATION_DICT_MARGIN * LOCATION_DICT_MIN_CHUNKS, 4))
    return min(max(int(chunk_order) - int(shard_order), m0), int(chunk_order))


def split_block(grid) -> dict:
    """The ``zagg_icechunk.split`` block for ``grid`` (§11.5)."""
    m = split_exponent(grid.parent_order, grid.chunk_order)
    return {"chunks": 4**m, "order": int(grid.chunk_order) - m}


def _is_local(store_root: str) -> bool:
    return not store_root.startswith("s3://")


def _boto3_credentials():
    """One refreshable credential snapshot off the botocore chain.

    The same chain obstore's ``Boto3CredentialProvider`` walks for the leaf
    writes (:mod:`zagg.store`), so the repo is reachable exactly where the
    leaves are — the execution role on Lambda, a profile locally.
    """
    import boto3
    from icechunk import S3StaticCredentials

    creds = boto3.Session().get_credentials()
    if creds is None:
        raise RuntimeError("no AWS credentials resolved for the icechunk repo")
    frozen = creds.get_frozen_credentials()
    return S3StaticCredentials(
        access_key_id=frozen.access_key,
        secret_access_key=frozen.secret_key,
        session_token=frozen.token,
        expires_after=getattr(creds, "_expiry_time", None),
    )


def _s3_credentials(store_kwargs: dict):
    import icechunk

    creds = store_kwargs.get("credentials")
    if creds:
        return icechunk.s3_static_credentials(
            access_key_id=creds["accessKeyId"],
            secret_access_key=creds["secretAccessKey"],
            session_token=creds.get("sessionToken"),
        )
    return icechunk.s3_refreshable_credentials(_boto3_credentials)


def _storage(path: str, store_kwargs: dict):
    """Icechunk storage for the repo at ``path`` (credential rules of :mod:`zagg.store`)."""
    import icechunk

    if _is_local(path):
        return icechunk.local_filesystem_storage(str(Path(path).resolve()))
    from zagg.store import _BUCKET_OWNER_ACL, _external_target, parse_s3_path

    bucket, prefix = parse_s3_path(path)
    endpoint = store_kwargs.get("endpoint_url")
    kwargs: dict[str, Any] = {
        "bucket": bucket,
        "prefix": prefix,
        "region": store_kwargs.get("region"),
        "endpoint_url": endpoint,
        "allow_http": bool(endpoint and endpoint.startswith("http://")),
        "force_path_style": bool(endpoint),
    }
    creds = store_kwargs.get("credentials")
    if creds:
        kwargs.update(
            access_key_id=creds["accessKeyId"],
            secret_access_key=creds["secretAccessKey"],
            session_token=creds.get("sessionToken"),
        )
    else:
        kwargs["get_credentials"] = _boto3_credentials
    if _external_target(creds, endpoint, bucket):
        # Issue #495: a target this account does not own gets the canned ACL on
        # every object-creating request, exactly like the leaf writes.
        kwargs["write_headers"] = {"x-amz-acl": _BUCKET_OWNER_ACL}
    return icechunk.s3_storage(**kwargs)


def _container(store_root: str, store_kwargs: dict):
    """``(VirtualChunkContainer, credential)`` for the store root (§11.3)."""
    import icechunk

    prefix = container_prefix(store_root)
    if _is_local(store_root):
        store = icechunk.local_filesystem_store(str(Path(store_root.rstrip("/")).resolve()))
        return icechunk.VirtualChunkContainer(
            prefix, store
        ), icechunk.credentials.LocalFileSystemAccess
    endpoint = store_kwargs.get("endpoint_url")
    store = icechunk.s3_store(
        region=store_kwargs.get("region"),
        endpoint_url=endpoint,
        allow_http=bool(endpoint and endpoint.startswith("http://")),
        force_path_style=bool(endpoint),
    )
    return icechunk.VirtualChunkContainer(prefix, store), _s3_credentials(store_kwargs)


def _repo_config(store_root: str, split_chunks: int, store_kwargs: dict):
    import icechunk

    config = icechunk.RepositoryConfig.default()
    container, _creds = _container(store_root, store_kwargs)
    config.set_virtual_chunk_container(container)
    config.manifest = icechunk.ManifestConfig(
        splitting=icechunk.ManifestSplittingConfig.from_dict(
            {
                icechunk.ManifestSplitCondition.AnyArray(): {
                    icechunk.ManifestSplitDimCondition.Axis(0): int(split_chunks)
                }
            }
        )
    )
    return config


def open_repo(store_root: str, order: int, *, store_kwargs: dict, split_chunks: int | None = None):
    """Open the order's repo; with ``split_chunks`` create it if absent (init only).

    The container and the manifest split persist with the repo at creation, so
    a plain open (the per-leaf path) carries no config of its own — only the
    container's credential, which never persists.
    """
    import icechunk

    storage = _storage(repo_path(store_root, order), store_kwargs)
    _container_obj, creds = _container(store_root, store_kwargs)
    auth = {container_prefix(store_root): creds}
    if split_chunks is None:
        return icechunk.Repository.open(storage, authorize_virtual_chunk_access=auth)
    return icechunk.Repository.open_or_create(
        storage,
        config=_repo_config(store_root, split_chunks, store_kwargs),
        authorize_virtual_chunk_access=auth,
    )


def _reroot(spec, n_shards: int):
    """One leaf array spec re-rooted on the whole order (§11.2).

    Shape ``(n_shards · L₀, *L[1:])``; when the leaf array is sharded the chunk
    grid becomes the INNER chunk shape and the codecs the INNER chain (the
    ``sharding_indexed`` wrapper is gone); dtype, fill, dims and attrs pass
    through verbatim.
    """
    data = spec.model_dump()
    codecs = list(data["codecs"])
    if codecs and codecs[0]["name"] == "sharding_indexed":
        inner = codecs[0]["configuration"]
        data["chunk_grid"] = {
            "name": "regular",
            "configuration": {"chunk_shape": list(inner["chunk_shape"])},
        }
        data["codecs"] = list(inner["codecs"])
    shape = list(data["shape"])
    shape[0] *= int(n_shards)
    data["shape"] = tuple(shape)
    return type(spec)(**data)


def repo_group_spec(grid, store_root: str, split: dict):
    """The repo's hierarchy: ``zagg_icechunk`` root block, the leaf's resolution group re-rooted."""
    from pydantic_zarr.experimental.v3 import GroupSpec

    leaf = grid.shard_spec()
    members = {name: _reroot(spec, grid.n_shards) for name, spec in leaf.members.items()}
    block = {
        "spec": ICECHUNK_SPEC,
        "order": int(grid.parent_order),
        "shard_order": int(grid.parent_order),
        "chunk_order": int(grid.chunk_order),
        "cell_order": int(grid.child_order),
        "url_prefix": container_prefix(store_root),
        "split": dict(split),
    }
    return GroupSpec(
        members={grid.group_path: GroupSpec(members=members, attributes=leaf.attributes)},
        attributes={ICECHUNK_ATTR: block},
    )


def _session_block(session) -> dict | None:
    """The ``zagg_icechunk`` block on a session's root group, or ``None``."""
    import zarr
    from zarr.errors import GroupNotFoundError

    try:
        attrs = zarr.open_group(session.store, mode="r").attrs
    except GroupNotFoundError:
        return None
    block = attrs.get(ICECHUNK_ATTR)
    return dict(block) if isinstance(block, dict) else None


def _geometry(grid, store_root: str) -> dict:
    """The block fields that fix where a leaf's refs land (§11.2, §11.3)."""
    return {
        "shard_order": int(grid.parent_order),
        "chunk_order": int(grid.chunk_order),
        "cell_order": int(grid.child_order),
        "url_prefix": container_prefix(store_root),
    }


def _check_geometry(block: dict, grid, store_root: str, path: str) -> None:
    """Raise unless a persisted block describes THIS run's array model.

    A store whose leaves were cleared but whose root survived reopens the
    stale repo underneath a new-geometry manifest; writing this run's refs
    into that repo puts them at indices that mean something else, which no
    per-ref checksum catches — the objects are exactly the ones recorded
    (§11.3). The caller is fail-open, so a mismatched store loses the index
    instead.
    """
    want = _geometry(grid, store_root)
    have = {key: block.get(key) for key in want}
    if have != want:
        raise ValueError(f"icechunk repo {path} was built for {have}, this run is {want}")


def read_block(store_root: str, order: int, *, store_kwargs: dict) -> dict | None:
    """The repo's ``zagg_icechunk`` block, or ``None`` when absent/uninitialized."""
    import icechunk

    try:
        repo = open_repo(store_root, order, store_kwargs=store_kwargs)
    except icechunk.IcechunkError:
        return None
    return _session_block(repo.readonly_session(BRANCH))


def init_repo(store_root: str, grid, *, run_id: str, store_kwargs: dict) -> dict:
    """Create-or-open the order's repo and define its arrays (§11.4 ``init``).

    Returns ``{"path", "order", "snapshot", "created", "split"}``; ``created``
    is ``False`` when the repo already carried the block (reopened, nothing
    committed) — the idempotent rerun. The rerun is a *match* check, not a
    presence check: a repo whose array model is not this run's raises.
    """
    from zagg.grids.base import vlen_dtype_warning_suppressed

    order = int(grid.parent_order)
    split = split_block(grid)
    path = repo_path(store_root, order)
    repo = open_repo(store_root, order, store_kwargs=store_kwargs, split_chunks=split["chunks"])
    ro = repo.readonly_session(BRANCH)
    existing = _session_block(ro)
    if existing is not None:
        _check_geometry(existing, grid, store_root, path)
        return {
            "path": path,
            "order": order,
            "snapshot": ro.snapshot_id,
            "created": False,
            "split": dict(existing.get("split") or split),
        }
    session = repo.writable_session(BRANCH)
    with vlen_dtype_warning_suppressed():
        repo_group_spec(grid, store_root, split).to_zarr(session.store, "", overwrite=False)
    snapshot, _rebases = _commit(session, f"init {run_id}", local=_is_local(store_root), path=path)
    return {"path": path, "order": order, "snapshot": snapshot, "created": True, "split": split}


def _leaf_rel(store_root: str, leaf_path: str) -> str:
    root = store_root.rstrip("/")
    if not leaf_path.startswith(root + "/"):
        raise ValueError(f"leaf {leaf_path!r} is not under store root {root!r}")
    return leaf_path.rstrip("/")[len(root) + 1 :]


def leaf_ref_plan(grid, shard_key, store_root: str, *, store_kwargs: dict) -> list[dict]:
    """Per-array virtual refs for one committed leaf (§11.3), read off its objects.

    One entry per template array the leaf holds an object for:
    ``{"path", "sharded", "chunk_grid", "arr_offset", "locations", "offsets",
    "lengths", "checksum", "refs"}`` for a sharded array (one shard object,
    refs from its index suffix) or ``{"path", "sharded": False, "chunks":
    [(key, location, length, checksum), ...], "refs"}`` for a regular one (one
    object per chunk, from a LIST). Arrays with no object emit no entry.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.hive import shard_leaf_path
    from zagg.store import open_object_store

    leaf_rel = _leaf_rel(store_root, shard_leaf_path(store_root, shard_key))
    prefix = container_prefix(store_root)
    # Checksums (§11.3): every ref carries the form its container validates —
    # the ETag on an object store, the object's ``last_modified`` ceiled to
    # the next whole second on ``file://``. Both come off the HEAD/LIST this
    # plan already issues, so staleness detection costs zero extra requests.
    etags = prefix.startswith("s3://")
    store = open_object_store(store_root, **store_kwargs)
    (rank,) = grid.block_index(int(shard_key))
    plan: list[dict] = []
    for name, spec in grid.shard_spec().members.items():
        data = spec.model_dump()
        path = f"{grid.group_path}/{name}"
        shape = tuple(int(s) for s in data["shape"])
        codecs = list(data["codecs"])
        outer = tuple(int(c) for c in data["chunk_grid"]["configuration"]["chunk_shape"])
        sharded = bool(codecs) and codecs[0]["name"] == "sharding_indexed"
        inner = (
            tuple(int(c) for c in codecs[0]["configuration"]["chunk_shape"]) if sharded else outer
        )
        chunk_grid = tuple(-(-s // c) for s, c in zip(shape, inner))
        arr_offset = (int(rank) * chunk_grid[0],) + (0,) * (len(shape) - 1)
        n = math.prod(chunk_grid)
        key_prefix = f"{leaf_rel}/{path}/c/"
        if sharded:
            if any(-(-s // c) != 1 for s, c in zip(shape, outer)):
                raise ValueError(f"{path}: a hive leaf array is one shard object (§11.3)")
            shard_cfg = codecs[0]["configuration"]
            location = shard_cfg.get("index_location", _INDEX_LOCATION)
            index_codecs = [dict(c) for c in shard_cfg.get("index_codecs", _INDEX_CODECS)]
            if location != _INDEX_LOCATION or index_codecs != _INDEX_CODECS:
                # A start-located or compressed index would make the suffix
                # read below return chunk payload as (offset, length) pairs;
                # the per-ref checksum does not catch that (the object is the
                # one recorded, the offsets are the nonsense — §11.3).
                raise ValueError(
                    f"{path}: shard index is {location}-located with {index_codecs}; "
                    f"the ref plan reads {_INDEX_LOCATION}-located {_INDEX_CODECS}"
                )
            key = key_prefix + "/".join("0" for _ in shape)
            try:
                head = obstore.head(store, key)
            except (FileNotFoundError, NotFoundError):
                continue
            index_bytes = _INDEX_ENTRY_BYTES * n + _INDEX_CRC_BYTES
            suffix = bytes(
                obstore.get_range(store, key, start=head["size"] - index_bytes, end=head["size"])
            )
            index = np.frombuffer(suffix[: _INDEX_ENTRY_BYTES * n], dtype="<u8").reshape(n, 2)
            present = index[:, 0] != _ABSENT
            offsets = np.where(present, index[:, 0], 0).astype("<u8")
            lengths = np.where(present, index[:, 1], 0).astype("<u8")
            plan.append(
                {
                    "path": path,
                    "sharded": True,
                    "chunk_grid": chunk_grid,
                    "arr_offset": arr_offset,
                    "locations": [prefix + key if p else "" for p in present],
                    "offsets": offsets,
                    "lengths": lengths,
                    "checksum": head["e_tag"] if etags else _ceil_second(head["last_modified"]),
                    "refs": int(present.sum()),
                }
            )
            continue
        chunks = []
        for meta in obstore.list(store, prefix=key_prefix).collect():
            coords = tuple(int(x) for x in meta["path"][len(key_prefix) :].split("/"))
            global_index = tuple(o + c for o, c in zip(arr_offset, coords))
            chunks.append(
                (
                    f"{path}/c/" + "/".join(str(i) for i in global_index),
                    prefix + meta["path"],
                    int(meta["size"]),
                    meta["e_tag"] if etags else _ceil_second(meta["last_modified"]),
                )
            )
        if chunks:
            plan.append({"path": path, "sharded": False, "chunks": chunks, "refs": len(chunks)})
    return plan


def _commit(session, message: str, *, local: bool, path: str = "") -> tuple[str, int]:
    """Commit with rebase-on-conflict; returns ``(snapshot_id, rebases)``.

    The loop is icechunk's own ``rebase_with`` pattern, unrolled so the
    attempt count is observable (recorded in the leaf's stats sidecar — the
    fleet's first real measurement of commit contention).

    On the local backend the commit is serialized through this process's lock
    for ``path`` (:func:`_local_commit_lock`); other repos in the same
    interpreter are unaffected.
    """
    import icechunk

    rebases = 0
    lock = _local_commit_lock(path) if local else contextlib.nullcontext()
    with lock:
        while True:
            try:
                return session.commit(message), rebases
            except icechunk.ConflictError:
                if rebases >= REBASE_TRIES:
                    raise
                session.rebase(icechunk.ConflictDetector())
                rebases += 1


def record_leaf(store_root: str, grid, shard_key, *, store_kwargs: dict, window=None) -> dict:
    """Write one committed leaf's refs and commit ``leaf {decimal}`` (§11.4).

    Returns the record the caller rides into the leaf's stats sidecar:
    ``{"path", "snapshot", "arrays", "refs", "rebases", "commit_s",
    "checksum"}`` (``checksum`` the form the refs carry: ``"etag"`` or
    ``"last_modified"``, §11.3), or ``{"skipped": reason}`` for a unit stage 1 does not
    index (a windowed leaf, §11.6; a leaf with no chunk objects). Raises on
    failure — the caller is fail-open.
    """
    import icechunk

    from zagg.grids.morton import morton_decimal

    if window is not None:
        return {"skipped": "windowed"}
    checksum = "etag" if container_prefix(store_root).startswith("s3://") else "last_modified"
    # Open and vet the repo BEFORE the plan: the plan costs ~20 object-store
    # requests, and refs must never land in a repo built for another geometry.
    order = int(grid.parent_order)
    path = repo_path(store_root, order)
    # The message stays free of the store root: it rides the leaf's stats
    # sidecar, where a root-dependent string would break byte parity.
    absent = f"the order-{order} icechunk repo is not initialized"
    try:
        repo = open_repo(store_root, order, store_kwargs=store_kwargs)
        block = _session_block(repo.readonly_session(BRANCH))
    except icechunk.IcechunkError as exc:
        raise ValueError(absent) from exc
    if block is None:
        raise ValueError(absent)
    _check_geometry(block, grid, store_root, path)
    plan = leaf_ref_plan(grid, shard_key, store_root, store_kwargs=store_kwargs)
    if not any(entry["refs"] for entry in plan):
        return {"skipped": "empty"}
    session = repo.writable_session(BRANCH)
    refs = 0
    for entry in plan:
        if not entry["refs"]:
            continue
        if entry["sharded"]:
            rejected = session.store.set_virtual_refs_arr(
                entry["path"],
                entry["chunk_grid"],
                entry["locations"],
                entry["offsets"],
                entry["lengths"],
                arr_offset=entry["arr_offset"],
                checksum=entry["checksum"],
            )
            if rejected:
                raise RuntimeError(f"{entry['path']}: {len(rejected)} virtual refs rejected")
        else:
            for key, location, length, chunk_checksum in entry["chunks"]:
                session.store.set_virtual_ref(
                    key, location, offset=0, length=length, checksum=chunk_checksum
                )
        refs += entry["refs"]
    t0 = time.perf_counter()
    snapshot, rebases = _commit(
        session, f"leaf {morton_decimal(int(shard_key))}", local=_is_local(store_root), path=path
    )
    return {
        "path": path,
        "snapshot": snapshot,
        "arrays": sum(1 for entry in plan if entry["refs"]),
        "refs": refs,
        "rebases": rebases,
        "commit_s": time.perf_counter() - t0,
        "checksum": checksum,
    }


__all__ = [
    "BRANCH",
    "ICECHUNK_ATTR",
    "ICECHUNK_SPEC",
    "LOCATION_DICT_MIN_CHUNKS",
    "REBASE_TRIES",
    "container_prefix",
    "init_repo",
    "leaf_ref_plan",
    "open_repo",
    "read_block",
    "record_leaf",
    "repo_group_spec",
    "repo_path",
    "split_block",
    "split_exponent",
]
