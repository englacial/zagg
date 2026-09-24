"""Icechunk companion repo: the virtual-ref index over a hive (spec §11, issue #580).

A morton hive is many leaf zarrs. The companion repository at
``{store_root}/icechunk/`` presents the whole store as ONE zarr hierarchy —
a **group per level, named by CELL order** (``/19`` the base leaves,
``/13`` the §4.6 leaf columns' declared member, ``/12`` … ``/4`` the
declared overview orders — exactly the manifest's ``multiscales`` datasets),
each holding that level's arrays re-rooted on the whole sphere, plus the
manifest's ``zagg-multiscales/1`` block mirrored into the root attrs as
``multiscales`` — by recording every object's inner chunks as Icechunk
**virtual chunk references**: ``(location, offset, length)`` byte ranges
into the objects that already exist. Stage 1 is refs-only and additive: the
leaves stay the normative, self-describing data plane; the repo is a derived
index.

Entry points, all fleet-first and worker-side (the dispatcher never writes,
D8):

- :func:`init_repo` — the once-per-run initialization (``mode="icechunk_init"``
  on Lambda, in-process on the local backend): create-or-open the repo,
  define every order's group and array nodes (§11.2), the per-order manifest
  splits (§11.5) and the virtual chunk container (§11.3), commit
  ``init {run_id}``. Idempotent: an initialized repo is reopened, never
  re-templated; one built for another geometry or ladder setting is refused.
- :func:`leaf_ref_plan` / :func:`object_ref_plan` — size and index the
  objects a leaf (or an overview) wrote: one HEAD + one ranged GET of the
  shard-index suffix per sharded array, one LIST per regular array.
- :func:`commit_units` — write ref-plan entries for any set of orders into
  one session and commit once, rebase-on-conflict, rebase count + wall time
  recorded. :func:`record_leaf` is the per-leaf commit (``commit: "leaf"``);
  the ladder (:mod:`zagg.icechunk_ladder`, the fleet default) commits at a
  stage node instead.

Concurrency: writers touch disjoint chunk ranges of the same arrays, so a
lost compare-and-swap on the branch ref is a local rebase + retry
(``ConflictDetector`` reports no conflict). Icechunk's local-filesystem
storage is NOT safe for concurrent commits (it says so on open), so the
local backend serializes commits through a per-repo, process-local lock;
object stores use conditional writes and need no lock.
"""

from __future__ import annotations

import contextlib
import logging
import math
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

import numpy as np

logger = logging.getLogger(__name__)

#: The ``zagg_icechunk`` root-attrs block's revision token (spec §11.1).
ICECHUNK_SPEC = "zagg-icechunk/1"
#: Root-group attrs key carrying the block.
ICECHUNK_ATTR = "zagg_icechunk"
#: Root-group attrs key carrying the manifest's multiscales mirror (§11.1).
MULTISCALES_ATTR = "multiscales"
#: The one branch every commit lands on (§11.4).
BRANCH = "main"
#: Icechunk's configurable default ``min_num_chunks`` below which a manifest
#: carries no location dictionary (§11.5, informative): the DEFAULT split —
#: one manifest per ``commit_order`` cell, 16,384 chunks per array at
#: production — clears it comfortably; the finest admissible one,
#: ``split_order == shard_order`` (one whole leaf, 256 chunks), does not, and
#: trades the dictionary away.
LOCATION_DICT_MIN_CHUNKS = 1000
#: Rebase-and-retry ceiling on a commit (icechunk's own ``rebase_tries``
#: semantics: this many rebase rounds; no backoff/jitter machinery).
REBASE_TRIES = 20
#: zarr v3 sharding-index sentinel for an absent inner chunk (spec §1.5).
_ABSENT = np.uint64(2**64 - 1)
#: ``(offset, nbytes)`` u64 pair per inner chunk, plus the crc32c suffix.
_INDEX_ENTRY_BYTES = 16
_INDEX_CRC_BYTES = 4
#: The shard-index layout the suffix read below assumes (spec §1.5): zarr's
#: ``create_array`` default, which ``grids.base.sharded_array_spec`` /
#: ``ragged_array_spec`` emit. Anything else is refused, not misread.
_INDEX_LOCATION = "end"
_INDEX_CODECS = [
    {"name": "bytes", "configuration": {"endian": "little"}},
    {"name": "crc32c"},
]
#: The ``output.icechunk.commit`` modes (§11.4).
COMMIT_MODES = ("ladder", "leaf")
_LOCAL_COMMIT_LOCKS: dict[str, threading.Lock] = {}
_LOCAL_COMMIT_LOCKS_GUARD = threading.Lock()


def _local_commit_lock(repo_path: str) -> threading.Lock:
    with _LOCAL_COMMIT_LOCKS_GUARD:
        return _LOCAL_COMMIT_LOCKS.setdefault(repo_path, threading.Lock())


def repo_path(store_root: str) -> str:
    """``{store_root}/icechunk`` — the one repository root (§11.1)."""
    from zagg.hive import ICECHUNK_DIR_NAME

    return f"{store_root.rstrip('/')}/{ICECHUNK_DIR_NAME}"


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


# ── options and splits ──────────────────────────────────────────────────────


def split_exponent(base_chunk_order: int, split_order: int, chunk_order: int | None = None) -> int:
    """``m`` such that one manifest spans ``4^m`` chunks, held constant across levels (§11.5).

    The exponent is fixed ONCE from the base level: ``m_base = base_chunk_order
    − split_order`` (7 at production — 16,384 inner chunks, one order-6 cell
    of the base). Every level then holds the same ``4^m`` CHUNKS per manifest,
    capped at its own chunk axis (``chunk_order``: one chunk per node at a
    column or overview level), so a coarse level's manifests span coarser
    cells — ``/13`` one order-2 cell, ``/12`` one order-1 cell, ``/11`` and
    coarser one base cell — rather than one ``split_order`` cell apiece, which
    would multiply the manifest count (and the snapshot) by the number of
    levels. Never above a whole base cell.
    """
    m = max(int(base_chunk_order) - int(split_order), 0)
    return min(m, int(base_chunk_order if chunk_order is None else chunk_order))


def split_block(grid, split_order: int, *, base_chunk_order: int | None = None) -> dict:
    """The per-level ``split`` block for ``grid`` at ``split_order`` (§11.5).

    ``base_chunk_order`` is the BASE level's chunk axis (the store's
    ``chunk_order``); ``grid`` is this level's. Absent, ``grid`` is the base.
    """
    base = grid.chunk_order if base_chunk_order is None else base_chunk_order
    m = split_exponent(base, split_order, grid.chunk_order)
    return {"chunks": 4**m, "order": int(grid.chunk_order) - m}


def finest_dispatch_order(shard_order: int, tuple_width: int | None = None) -> int:
    """The finest staged-sweep dispatch node order — the default ``commit_order``.

    ``shard_order − tuple_width`` when the shard order is a multiple of the
    width (6 at the production shard order 9, width 3); in general the
    dispatch order of the finest tuple (:func:`zagg.sweep_stage.stage_tuples`).
    """
    from zagg.sweep_stage import DEFAULT_TUPLE_WIDTH, stage_tuples

    width = DEFAULT_TUPLE_WIDTH if tuple_width is None else int(tuple_width)
    return int(stage_tuples(int(shard_order), tuple_width=width)[0]["dispatch"])


def ladder_walks(config, grid) -> bool:
    """Whether this run's staged sweep walks the ref ladder (§11.4).

    The unset-``commit`` default hangs on it. True only when the dispatcher
    chains the staged sweep (``output.sweep: "stages"``) AND the config
    declares a ``/2`` ladder with at least one composable field — the same
    derivation :func:`zagg.column.leaf_column_plan` runs; without either,
    :func:`zagg.sweep_stages.sweep_stage_pass` returns before its first
    node, and a ladder-mode run would leave the repo empty while every
    leaf's sidecar reported success (review finding). Config-only, so the
    init step and every worker resolve the same answer. A dispatcher that
    chains no staged sweep at all (the ``client`` facade) pins ``commit:
    "leaf"`` in the config it ships instead.
    """
    if config.output.get("sweep") != "stages":
        return False
    from zagg.column import leaf_column_plan

    try:
        return leaf_column_plan(config, grid) is not None
    except ValueError:
        return False  # a declaration the sweep would refuse walks no ladder either


def resolve_options(config, shard_order: int, *, tuple_width: int | None = None, grid=None) -> dict:
    """``{"commit", "commit_order", "split_order"}`` for this run, defaults applied.

    Defaults: ``commit: "ladder"`` when the run walks the ladder
    (:func:`ladder_walks` on ``grid``), else ``"leaf"`` — never a mode whose
    commits no step of the run makes; without a ``grid`` an unset ``commit``
    is ``"leaf"``. ``commit_order`` the finest dispatch node;
    ``split_order = commit_order``. Validation (§11.5): a commit must write
    whole manifests, so ``split_order >= commit_order``; both at most the
    shard order (a leaf is the finest thing a commit or a manifest can be
    keyed to); ``commit_order`` non-negative. Under ``commit: "ladder"`` the
    shard order itself is refused: no stage tuple's ``[dispatch, child_order)``
    range contains it, so it would commit no leaf refs at all.
    """
    from zagg.config import get_icechunk_options

    raw = get_icechunk_options(config)
    shard_order = int(shard_order)
    commit = raw.get("commit") or (
        "ladder" if grid is not None and ladder_walks(config, grid) else "leaf"
    )
    if commit not in COMMIT_MODES:
        raise ValueError(f"output.icechunk.commit must be one of {COMMIT_MODES} (got {commit!r})")
    commit_order = raw.get("commit_order")
    commit_order = (
        finest_dispatch_order(shard_order, tuple_width)
        if commit_order is None
        else int(commit_order)
    )
    split_order = raw.get("split_order")
    split_order = commit_order if split_order is None else int(split_order)
    if not 0 <= commit_order <= shard_order:
        raise ValueError(
            f"output.icechunk.commit_order {commit_order} must lie in [0, shard_order {shard_order}]"
        )
    if commit == "ladder" and commit_order == shard_order:
        # Every stage tuple covers orders ``[dispatch, child_order)`` with
        # ``child_order <= shard_order``, so no tuple's range contains the
        # shard order itself: the committing role would never be assigned, no
        # leaf sidecar would ever be gathered, and the base group would stay
        # empty while the run record showed commits (review finding).
        raise ValueError(
            f"output.icechunk.commit_order {commit_order} must lie in [0, shard_order "
            f"{shard_order}) under commit: 'ladder' — no stage tuple covers the shard "
            f"order itself, so no leaf refs would be gathered; use commit: 'leaf' for "
            f"per-leaf commits (spec §11.4)"
        )
    if not commit_order <= split_order <= shard_order:
        raise ValueError(
            f"output.icechunk.split_order {split_order} must lie in [commit_order {commit_order}, "
            f"shard_order {shard_order}] — a commit must write whole manifests (spec §11.5)"
        )
    return {"commit": commit, "commit_order": commit_order, "split_order": split_order}


# ── storage and credentials ─────────────────────────────────────────────────


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


def _repo_config(store_root: str, splits: dict, store_kwargs: dict):
    """Repo config: the container and one manifest split per order group (§11.5).

    ``splits`` maps an order to its ``split`` block; conditions are matched
    first-wins on the array path, so each ``/{order}/`` prefix gets its own
    run length and a catch-all keeps anything else at one chunk per manifest.
    """
    import icechunk
    from icechunk import ManifestSplitCondition as C
    from icechunk import ManifestSplitDimCondition as D

    config = icechunk.RepositoryConfig.default()
    container, _creds = _container(store_root, store_kwargs)
    config.set_virtual_chunk_container(container)
    sizes = {
        C.path_matches(regex=rf"^/{int(order)}/.*"): {D.Axis(0): int(split["chunks"])}
        for order, split in sorted(splits.items(), key=lambda kv: -int(kv[0]))
    }
    sizes[C.AnyArray()] = {D.Axis(0): 1}
    config.manifest = icechunk.ManifestConfig(
        splitting=icechunk.ManifestSplittingConfig.from_dict(sizes)
    )
    return config


def open_repo(store_root: str, *, store_kwargs: dict, splits: dict | None = None):
    """Open the store's repo; with ``splits`` create it if absent (init only).

    The container and the manifest splits persist with the repo at creation,
    so a plain open (every later path) carries no config of its own — only
    the container's credential, which never persists.
    """
    import icechunk

    storage = _storage(repo_path(store_root), store_kwargs)
    auth = _auth(store_root, store_kwargs)
    if splits is None:
        return icechunk.Repository.open(storage, authorize_virtual_chunk_access=auth)
    return icechunk.Repository.open_or_create(
        storage,
        config=_repo_config(store_root, splits, store_kwargs),
        authorize_virtual_chunk_access=auth,
    )


def _auth(store_root: str, store_kwargs: dict) -> dict:
    _container_obj, creds = _container(store_root, store_kwargs)
    return {container_prefix(store_root): creds}


def _save_splits(repo, store_root: str, splits: dict, store_kwargs: dict):
    """Persist a new per-level split config on the repo (the §11.5 ratchet); the reopened repo."""
    repo = repo.reopen(
        config=_repo_config(store_root, splits, store_kwargs),
        authorize_virtual_chunk_access=_auth(store_root, store_kwargs),
    )
    repo.save_config()
    return repo


# ── the hierarchy ───────────────────────────────────────────────────────────


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


def level_group_spec(grid):
    """One order's group: the leaf's resolution-group attrs, every array re-rooted."""
    from pydantic_zarr.experimental.v3 import GroupSpec

    leaf = grid.shard_spec()
    members = {name: _reroot(spec, grid.n_shards) for name, spec in leaf.members.items()}
    return GroupSpec(members=members, attributes=leaf.attributes)


def level_geometry(grid) -> dict:
    """The per-level block: the orders that fix where a ref lands (§11.2, §11.3)."""
    return {"chunk_order": int(grid.chunk_order), "cell_order": int(grid.child_order)}


def level_grids(manifest: dict, grid) -> dict:
    """``{cell_order: {"grid", "node", "artifact"}}`` — the repo's levels (§11.1).

    Exactly the manifest's ``zagg-multiscales/1`` datasets plus the base:
    the base leaves at the store's cell order (``node == shard_order``,
    artifact ``leaf``), the §4.6 leaf column's declared member (``node ==
    shard_order``, artifact ``column``) and one overview level per declared
    ancestor order (artifact ``overview``). Each level's grid is the one its
    artifact is written with — ``grid`` itself for the base, the
    ``sweep_stage`` / ``column`` writers' ``HealpixGrid(node, cells,
    config=_overview_config(fields), sharded=True)`` for the rest — so the
    array model cannot drift from the objects. A column's OTHER members (its
    within-footprint intermediates and node-order partial) are sweep inputs,
    not levels: they overlap the overview levels for the same cells and are
    deliberately not indexed. A store with no ``/2`` declaration has the base
    level only.
    """
    from zagg.column import composable_fields
    from zagg.grids.healpix import HealpixGrid
    from zagg.sweep_overview import _overview_config

    levels = {
        int(grid.child_order): {"grid": grid, "node": int(grid.parent_order), "artifact": "leaf"}
    }
    mirror = manifest.get(MULTISCALES_ATTR) or []
    block = mirror[0] if mirror and isinstance(mirror[0], dict) else None
    if block is None:
        return levels
    decl = (manifest.get("pyramid") or {}).get("overview")
    fields = composable_fields((decl.get("fields") if isinstance(decl, dict) else None) or {})
    if not fields:
        return levels
    config = _overview_config(fields)
    for entry in block.get("datasets") or []:
        node, artifact = int(entry["order"]), entry.get("artifact")
        # EVERY cell resolution the entry declares is its own level, at that
        # entry's node order and artifact: ``expand_overviews`` puts the whole
        # declared leaf run on the one shard-node entry, so ``overviews: [13,
        # 12]`` gives ``{"order": 9, "cells": [13, 12]}`` — two column levels,
        # not one (review finding).
        for cells in (int(c) for c in entry["cells"]):
            if cells in levels:
                # A column member declared AT the store's cell order (a chunk
                # order equal to the cell order, a test geometry) duplicates
                # the base leaves; the base is the level, the member is not
                # indexed.
                continue
            levels[cells] = {
                "grid": HealpixGrid(node, cells, config=config, sharded=True),
                "node": node,
                "artifact": artifact,
            }
    return levels


def repo_group_spec(grid, store_root: str, options: dict, manifest: dict):
    """The repo's hierarchy: ``zagg_icechunk`` + ``multiscales`` root attrs, a group per level."""
    from pydantic_zarr.experimental.v3 import GroupSpec

    grids = level_grids(manifest, grid)
    levels = {
        str(cells): {
            "node_order": int(level["node"]),
            "artifact": level["artifact"],
            **level_geometry(level["grid"]),
            "split": split_block(
                level["grid"], options["split_order"], base_chunk_order=grid.chunk_order
            ),
        }
        for cells, level in sorted(grids.items(), reverse=True)
    }
    block = {
        "spec": ICECHUNK_SPEC,
        "shard_order": int(grid.parent_order),
        "chunk_order": int(grid.chunk_order),
        "cell_order": int(grid.child_order),
        "url_prefix": container_prefix(store_root),
        "levels": levels,
        # The ladder's knobs (§11.4/§11.5), read back by every stage node.
        # ``split_order`` is the store's authoritative, ratcheting value;
        # ``commit`` / ``commit_order`` are per-run — recorded so this run's
        # stage nodes can read them, never a compatibility key.
        **{k: options[k] for k in ("commit", "commit_order", "split_order")},
    }
    attributes: dict = {ICECHUNK_ATTR: block}
    mirror = manifest.get(MULTISCALES_ATTR)
    if mirror is not None:
        # The §4.9 discovery mirror, verbatim: one repo, every level findable
        # from its root attrs (the icechunk-multiscales convention).
        attributes[MULTISCALES_ATTR] = mirror
    return GroupSpec(
        members={str(cells): level_group_spec(level["grid"]) for cells, level in grids.items()},
        attributes=attributes,
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


def _check_block(block: dict, want: dict, path: str) -> None:
    """Raise unless a persisted block agrees with ``want`` on every key of ``want``.

    A store whose leaves were cleared but whose root survived reopens the
    stale repo underneath a new-geometry manifest; writing this run's refs
    into that repo puts them at indices that mean something else, which no
    per-ref checksum catches — the objects are exactly the ones recorded
    (§11.3). Callers are fail-open, so a mismatched store loses the index
    instead.

    A ``cell_order`` in ``want`` also vets the LEVEL KEYING: before the §11.1
    amendment the groups were named by node order, so such a repo agrees on
    every key here and then raises ``NodeNotFound`` on every commit — an
    index that silently never fills. No published store carries that shape,
    so the fix is to clear the repo and re-init.
    """
    have = {key: block.get(key) for key in want}
    if have != want:
        raise ValueError(f"icechunk repo {path} was built for {have}, this run is {want}")
    if "cell_order" in want and str(int(want["cell_order"])) not in (block.get("levels") or {}):
        raise ValueError(
            f"icechunk repo {path} keys its levels {sorted(block.get('levels') or {}, key=int)} "
            f"by node order, not cell order (pre-§11.1-amendment): every commit into it would "
            f"raise NodeNotFound. Clear {path} and re-init (spec §11.1, issue #580)."
        )


def read_block(store_root: str, *, store_kwargs: dict) -> dict | None:
    """The repo's ``zagg_icechunk`` block, or ``None`` when absent/uninitialized."""
    import icechunk

    try:
        repo = open_repo(store_root, store_kwargs=store_kwargs)
    except icechunk.IcechunkError:
        return None
    return _session_block(repo.readonly_session(BRANCH))


#: Block keys a reopened repo must agree on (§11.2): the array model and the
#: container. The ladder knobs are NOT among them — ``split_order`` ratchets
#: (§11.5) and ``commit``/``commit_order`` are per-run.
_COMPAT_KEYS = ("shard_order", "chunk_order", "cell_order", "url_prefix")


def _update_block(repo, updates: dict, message: str, *, local: bool, path: str) -> str:
    """Rewrite root-attrs keys of the repo in one commit; the snapshot id."""
    import zarr

    session = repo.writable_session(BRANCH)
    root = zarr.open_group(session.store, mode="r+")
    block = dict(cast("Mapping[str, Any]", root.attrs[ICECHUNK_ATTR]))
    block.update(updates)
    root.attrs[ICECHUNK_ATTR] = block
    snapshot, _rebases = _commit(session, message, local=local, path=path)
    return snapshot


def init_repo(
    store_root: str, grid, config, *, run_id: str, store_kwargs: dict, manifest: dict | None = None
) -> dict:
    """Create-or-open the store's repo and define every level (§11.4 ``init``).

    One repo per store, a group per LEVEL keyed by cell order — the base
    leaves, the §4.6 column's declared members and every declared overview
    order (:func:`level_grids`) — created here, once, because Icechunk's
    create is not safe under concurrent callers and the stage nodes that
    commit fan out. ``manifest`` is the store's (an append run indexes the
    declared ladder, not this config's); ``None`` reads it, then builds it
    from ``config``. Returns ``{"path", "snapshot", "created", "options",
    "levels", "ladder", "split_ratchet"}``; ``levels`` is keyed by cell order
    and ``ladder`` is those keys sorted. ``created`` is ``False`` when the
    repo already carried a block for this array model and container
    (reopened) — a block for another geometry raises.

    **The split ratchet (§11.5).** The store's recorded ``split_order`` is
    authoritative and moves one way, toward coarser: a config FINER than the
    store (a higher order) adopts the store's value with a warning — templates
    are hash-pinned build configs reused by appends, so refusing would break
    every append after a ratchet; an equal one is a no-op; a COARSER one is an
    intentional ratchet, recorded in the block and in the repo's saved
    splitting config BEFORE any commit of this run (so every manifest this run
    writes is already at the new cut) and flagged as ``split_ratchet:
    {from, to}`` for a later ``rewrite_manifests`` of the old manifests (not
    run here — mixed cuts are valid, each manifest carries its own extents).
    ``commit_order`` is per-run: it is written to the block for this run's
    stage nodes whenever it differs, never compared.
    """
    from zagg.grids.base import vlen_dtype_warning_suppressed
    from zagg.hive import build_manifest, read_manifest

    options = resolve_options(config, grid.parent_order, grid=grid)
    if manifest is None:
        manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        from zagg.config import get_windowing

        manifest = build_manifest(grid, windowing=get_windowing(config))
    path = repo_path(store_root)
    local = _is_local(store_root)
    spec = repo_group_spec(grid, store_root, options, manifest)
    block = spec.attributes[ICECHUNK_ATTR]
    splits = {order: level["split"] for order, level in block["levels"].items()}
    repo = open_repo(store_root, store_kwargs=store_kwargs, splits=splits)
    ro = repo.readonly_session(BRANCH)
    existing = _session_block(ro)
    if existing is None:
        session = repo.writable_session(BRANCH)
        with vlen_dtype_warning_suppressed():
            spec.to_zarr(session.store, "", overwrite=False)
        snapshot, _rebases = _commit(session, f"init {run_id}", local=local, path=path)
        return {
            "path": path,
            "snapshot": snapshot,
            "created": True,
            "options": options,
            "levels": block["levels"],
            "ladder": sorted(int(o) for o in block["levels"]),
            "split_ratchet": None,
        }
    _check_block(existing, {k: block[k] for k in _COMPAT_KEYS}, path)
    stored = int(existing.get("split_order", options["split_order"]))
    wanted = int(options["split_order"])
    ratchet = None
    updates: dict = {}
    if wanted > stored:
        logger.warning(
            f"output.icechunk.split_order {wanted} is finer than the store's recorded "
            f"{stored} at {path}; the split ratchets one way (coarser) — using {stored} "
            f"(spec §11.5)"
        )
        options = {**options, "split_order": stored}
        if options["commit_order"] > stored:
            raise ValueError(
                f"output.icechunk.commit_order {options['commit_order']} exceeds the store's "
                f"recorded split_order {stored}: a commit must write whole manifests (§11.5)"
            )
    elif wanted < stored:
        ratchet = {"from": stored, "to": wanted}
        spec = repo_group_spec(grid, store_root, options, manifest)
        block = spec.attributes[ICECHUNK_ATTR]
        splits = {order: level["split"] for order, level in block["levels"].items()}
        repo = _save_splits(repo, store_root, splits, store_kwargs)
        updates.update(split_order=wanted, levels=block["levels"])
        logger.warning(
            f"icechunk split_order ratchets {stored} -> {wanted} at {path}: new manifests are "
            f"cut at order {wanted}; the old ones await a rewrite_manifests pass (spec §11.5)"
        )
    levels = updates.get("levels") or existing.get("levels") or block["levels"]
    for key in ("commit", "commit_order"):
        if existing.get(key) != options[key]:
            updates[key] = options[key]
    snapshot = ro.snapshot_id
    if updates:
        label = (
            f"split ratchet {ratchet['from']}->{ratchet['to']} {run_id}"
            if ratchet
            else f"init {run_id}"
        )
        snapshot = _update_block(repo, updates, label, local=local, path=path)
    return {
        "path": path,
        "snapshot": snapshot,
        "created": False,
        "options": options,
        "levels": levels,
        "ladder": sorted(int(o) for o in levels),
        "split_ratchet": ratchet,
    }


# ── ref plans ───────────────────────────────────────────────────────────────


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
    object per chunk, from a LIST). ``path`` is the array's name in its order
    group (``count``); ``key`` a chunk key under it. Arrays with no object
    emit no entry.
    """
    from zagg.hive import shard_leaf_path

    (rank,) = grid.block_index(int(shard_key))
    leaf_rel = _leaf_rel(store_root, shard_leaf_path(store_root, shard_key))
    return object_ref_plan(grid, leaf_rel, rank, store_root, store_kwargs=store_kwargs)


def object_ref_plan(
    grid, object_rel: str, rank: int, store_root: str, *, store_kwargs
) -> list[dict]:
    """:func:`leaf_ref_plan` for any object ``grid`` templates: a leaf or a §4 overview.

    ``object_rel`` is the object's store-relative path, ``rank`` its nested
    rank on the level's shard axis (the leaf's shard rank; an overview's node
    rank at its order).
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.store import open_object_store

    object_rel = object_rel.strip("/")
    prefix = container_prefix(store_root)
    # Checksums (§11.3): every ref carries the form its container validates —
    # the ETag on an object store, the object's ``last_modified`` ceiled to
    # the next whole second on ``file://``. Both come off the HEAD/LIST this
    # plan already issues, so staleness detection costs zero extra requests.
    etags = prefix.startswith("s3://")
    store = open_object_store(store_root, **store_kwargs)
    plan: list[dict] = []
    for name, spec in grid.shard_spec().members.items():
        data = spec.model_dump()
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
        key_prefix = f"{object_rel}/{grid.group_path}/{name}/c/"
        if sharded:
            if any(-(-s // c) != 1 for s, c in zip(shape, outer)):
                raise ValueError(f"{name}: a hive leaf array is one shard object (§11.3)")
            shard_cfg = codecs[0]["configuration"]
            location = shard_cfg.get("index_location", _INDEX_LOCATION)
            index_codecs = [dict(c) for c in shard_cfg.get("index_codecs", _INDEX_CODECS)]
            if location != _INDEX_LOCATION or index_codecs != _INDEX_CODECS:
                # A start-located or compressed index would make the suffix
                # read below return chunk payload as (offset, length) pairs;
                # the per-ref checksum does not catch that (the object is the
                # one recorded, the offsets are the nonsense — §11.3).
                raise ValueError(
                    f"{name}: shard index is {location}-located with {index_codecs}; "
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
                    "path": name,
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
        listing: list[Any]
        if n == 1:
            # A single-chunk array (every column and overview array, §11.2):
            # one HEAD of the one key — the whole object is the ref (offset
            # 0, length = size), no LIST and no index read.
            key = key_prefix + "/".join("0" for _ in shape)
            try:
                head = obstore.head(store, key)
            except (FileNotFoundError, NotFoundError):
                continue
            listing = [{**head, "path": key}]
        else:
            listing = list(obstore.list(store, prefix=key_prefix).collect())
        for meta in listing:
            coords = tuple(int(x) for x in meta["path"][len(key_prefix) :].split("/"))
            global_index = tuple(o + c for o, c in zip(arr_offset, coords))
            chunks.append(
                (
                    f"{name}/c/" + "/".join(str(i) for i in global_index),
                    prefix + meta["path"],
                    int(meta["size"]),
                    meta["e_tag"] if etags else _ceil_second(meta["last_modified"]),
                )
            )
        if chunks:
            plan.append({"path": name, "sharded": False, "chunks": chunks, "refs": len(chunks)})
    return plan


# ── commits ─────────────────────────────────────────────────────────────────


def _commit(session, message: str, *, local: bool, path: str = "") -> tuple[str, int]:
    """Commit with rebase-on-conflict; returns ``(snapshot_id, rebases)``.

    The loop is icechunk's own ``rebase_with`` pattern, unrolled so the
    attempt count is observable (recorded in the stats sidecar / stage row —
    the fleet's first real measurement of commit contention).

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


def open_vetted(store_root: str, *, store_kwargs: dict, want: dict | None = None):
    """``(repo, block)`` for the store, refusing a missing or mismatched repo.

    ``want`` are block keys that must agree (:func:`_check_block`); the
    container prefix is always checked. The message stays free of the store
    root: it rides the leaf's stats sidecar, where a root-dependent string
    would break byte parity.
    """
    import icechunk

    path = repo_path(store_root)
    absent = "the icechunk repo is not initialized"
    try:
        repo = open_repo(store_root, store_kwargs=store_kwargs)
        block = _session_block(repo.readonly_session(BRANCH))
    except icechunk.IcechunkError as exc:
        raise ValueError(absent) from exc
    if block is None:
        raise ValueError(absent)
    _check_block(block, {"url_prefix": container_prefix(store_root), **(want or {})}, path)
    return repo, block


def commit_units(
    store_root: str, units: Iterable[dict], message: str, *, store_kwargs: dict, repo=None
) -> dict:
    """Write ref-plan units for any set of orders into ONE session and commit once.

    ``units`` is any ITERABLE of ``{"level", "entries"}``: each entry lands
    under its level's group (``/{level}/{path}``, the level's CELL order). A
    generator is the point — the ladder streams a committing node's subtree
    through here one child at a time, so the node's peak is one child's
    carriers rather than the whole subtree's (§11.4). Returns ``{"path",
    "snapshot", "refs", "levels", "rebases", "commit_s"}`` (``snapshot``
    ``None`` when nothing was written). ``repo`` skips the open/vet when the
    caller already holds a vetted handle.
    """
    if repo is None:
        repo, _block = open_vetted(store_root, store_kwargs=store_kwargs)
    session = repo.writable_session(BRANCH)
    refs = 0
    orders: set = set()
    for unit in units:
        order = int(unit["level"])
        for entry in unit["entries"]:
            if not entry["refs"]:
                continue
            array_path = f"{order}/{entry['path']}"
            if entry["sharded"]:
                rejected = session.store.set_virtual_refs_arr(
                    array_path,
                    tuple(entry["chunk_grid"]),
                    list(entry["locations"]),
                    np.asarray(entry["offsets"], dtype="<u8"),
                    np.asarray(entry["lengths"], dtype="<u8"),
                    arr_offset=tuple(entry["arr_offset"]),
                    checksum=entry["checksum"],
                )
                if rejected:
                    raise RuntimeError(f"{array_path}: {len(rejected)} virtual refs rejected")
            else:
                for key, location, length, chunk_checksum in entry["chunks"]:
                    session.store.set_virtual_ref(
                        f"{order}/{key}",
                        location,
                        offset=0,
                        length=int(length),
                        checksum=chunk_checksum,
                    )
            refs += int(entry["refs"])
            orders.add(order)
    path = repo_path(store_root)
    if not refs:
        return {
            "path": path,
            "snapshot": None,
            "refs": 0,
            "levels": [],
            "rebases": 0,
            "commit_s": 0.0,
        }
    t0 = time.perf_counter()
    snapshot, rebases = _commit(session, message, local=_is_local(store_root), path=path)
    return {
        "path": path,
        "snapshot": snapshot,
        "refs": refs,
        "levels": sorted(orders, reverse=True),
        "rebases": rebases,
        "commit_s": time.perf_counter() - t0,
    }


def leaf_units(
    grid, config, shard_key, store_root: str, *, column: str | None, store_kwargs: dict
) -> list[dict]:
    """The units a committed leaf contributes (§11.4): its base arrays + its column's level.

    The base unit at the store's cell order from :func:`leaf_ref_plan`, plus
    — when the unit wrote its §4.6 column (``column`` is its basename) — one
    unit per column member that is a repo LEVEL: the declared leaf-node
    cells (:func:`zagg.column.leaf_level_cells` — ``[13]`` at production),
    never the within-footprint intermediates (``12, 11, 10``) or the
    node-order partial, which are sweep inputs that overlap the overview
    levels (:func:`level_grids`). Each column array is one unsharded chunk,
    so its ref is the whole object.
    """
    from zagg.column import leaf_column_plan, leaf_level_cells
    from zagg.grids.healpix import HealpixGrid
    from zagg.hive import shard_leaf_path
    from zagg.sweep_overview import _overview_config

    (rank,) = grid.block_index(int(shard_key))
    leaf_rel = _leaf_rel(store_root, shard_leaf_path(store_root, shard_key))
    units = [
        {
            "level": int(grid.child_order),
            "entries": object_ref_plan(grid, leaf_rel, rank, store_root, store_kwargs=store_kwargs),
        }
    ]
    plan = leaf_column_plan(config, grid) if column else None
    if plan is None:
        return units
    _resolutions, fields = plan
    node_rel = leaf_rel.rsplit("/", 1)[0]
    cfg = _overview_config(fields)
    for res in leaf_level_cells(config, grid):
        if int(res) >= int(grid.child_order):
            continue  # a member at the store's own cell order duplicates the base (level_grids)
        column_grid = HealpixGrid(int(grid.parent_order), int(res), config=cfg, sharded=True)
        entries = object_ref_plan(
            column_grid, f"{node_rel}/{column}", rank, store_root, store_kwargs=store_kwargs
        )
        if entries:
            units.append({"level": int(res), "entries": entries})
    return units


def vet_leaf_repo(store_root: str, grid, *, store_kwargs: dict):
    """The store's repo, opened and vetted for ``grid``'s leaves (§11.4); raises otherwise.

    What a per-leaf commit checks BEFORE its plan (:func:`leaf_units`, ~20
    object-store requests): a missing repo, or one built for another
    geometry, refuses here, so refs never land at indices that mean something
    else and a refusal never pays for the plan. The worker seam calls it
    ahead of :func:`leaf_units` and hands the handle to :func:`record_leaf`.
    """
    repo, _block = open_vetted(
        store_root,
        store_kwargs=store_kwargs,
        want={
            "shard_order": int(grid.parent_order),
            "chunk_order": int(grid.chunk_order),
            "cell_order": int(grid.child_order),
        },
    )
    return repo


def record_leaf(
    store_root: str, grid, shard_key, *, store_kwargs: dict, window=None, units=None, repo=None
) -> dict:
    """The per-leaf commit (``commit: "leaf"``): refs + ``leaf {decimal}`` (§11.4).

    ``units`` are the leaf's :func:`leaf_units` when the caller computed
    them (the worker seam, which knows whether it wrote a column); ``None``
    commits the base arrays alone. Returns the record the caller rides into
    the leaf's stats sidecar: ``{"path", "snapshot", "arrays", "refs",
    "levels", "rebases", "commit_s", "checksum"}`` (``checksum`` the form the
    refs carry: ``"etag"`` or ``"last_modified"``, §11.3), or ``{"skipped":
    reason}`` for a unit stage 1 does not index (a windowed leaf, §11.6; a
    leaf with no chunk objects). Raises on failure — the caller is fail-open.
    ``repo`` is a handle :func:`vet_leaf_repo` already returned (the worker
    seam vets before it plans); ``None`` opens and vets here, still BEFORE
    the plan.
    """
    from zagg.grids.morton import morton_decimal

    if window is not None:
        return {"skipped": "windowed"}
    checksum = "etag" if container_prefix(store_root).startswith("s3://") else "last_modified"
    if repo is None:
        repo = vet_leaf_repo(store_root, grid, store_kwargs=store_kwargs)
    if units is None:
        plan = leaf_ref_plan(grid, shard_key, store_root, store_kwargs=store_kwargs)
        units = [{"level": int(grid.child_order), "entries": plan}]
    if not any(entry["refs"] for unit in units for entry in unit["entries"]):
        return {"skipped": "empty"}
    outcome = commit_units(
        store_root,
        units,
        f"leaf {morton_decimal(int(shard_key))}",
        store_kwargs=store_kwargs,
        repo=repo,
    )
    arrays = sum(1 for unit in units for entry in unit["entries"] if entry["refs"])
    return {**outcome, "arrays": arrays, "checksum": checksum}


__all__ = [
    "BRANCH",
    "COMMIT_MODES",
    "ICECHUNK_ATTR",
    "ICECHUNK_SPEC",
    "LOCATION_DICT_MIN_CHUNKS",
    "MULTISCALES_ATTR",
    "REBASE_TRIES",
    "commit_units",
    "container_prefix",
    "finest_dispatch_order",
    "init_repo",
    "ladder_walks",
    "leaf_units",
    "level_grids",
    "leaf_ref_plan",
    "level_group_spec",
    "object_ref_plan",
    "open_repo",
    "open_vetted",
    "read_block",
    "record_leaf",
    "repo_group_spec",
    "repo_path",
    "resolve_options",
    "split_block",
    "split_exponent",
    "vet_leaf_repo",
]
