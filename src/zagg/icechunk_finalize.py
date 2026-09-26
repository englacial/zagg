"""Run finalize for the Icechunk companion repo (spec §11.4 ``finalize``, issue #582).

The once-per-run closing step, the twin of :func:`zagg.icechunk_refs.init_repo`
(``mode="icechunk_finalize"`` on Lambda, in-process on the local backend),
fired by the dispatcher AFTER every commit of the run has landed — after the
staged sweep's finisher under the ladder, after the fan-out under
``commit: "leaf"`` — and never by a stage node: tags, expiry and garbage
collection are singleton repo operations, and the root tuple dispatches
twelve o0 nodes. It is separate from the staged sweep's finisher because
that finisher is lease-scoped, load-bearing store-root machinery while the
repo is fail-open, and because a per-leaf run has no finisher at all.

One finalize does, in order:

1. retention — with ``retain_runs`` = K > 0, the run tags beyond the K − 1
   newest are deleted, snapshots older than the oldest retained run expire,
   and unreferenced repo objects older than it are garbage-collected. The
   default K = 0 retains everything and runs none of it. Only ``run-`` tags
   are ever deleted, and only here; virtual targets (the leaves) are never
   touched by any of it (icechunk manages none of them);
2. one empty ``finalize {run_id}`` commit whose METADATA identifies the run
   (``run_id``, ``semantic_hash``, ``zagg_version``, the ladder knobs) and
   records the retention counts, so the repo is its own durable run record;
3. the tag ``run-{run_id}`` on that commit.

``rewrite_manifests`` is NOT run here: a split ratchet (§11.5) is a rare,
deliberate, whole-repo operation with no run to attach to; finalize reports
it as ``rewrite_pending`` for the operator step. Idempotent: a run whose tag
exists returns it and does nothing else, so a retried invoke is harmless.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from zagg.icechunk_refs import (
    BRANCH,
    _commit,
    _is_local,
    open_vetted,
    repo_path,
)

logger = logging.getLogger(__name__)

#: Run tags are ``run-{run_id}`` (§11.4); nothing else under the prefix is
#: ever written, and nothing outside it is ever deleted.
TAG_PREFIX = "run-"


def run_tag(run_id: str) -> str:
    """The run's tag name, ``run-{run_id}``."""
    return f"{TAG_PREFIX}{run_id}"


def resolve_retain_runs(config) -> int:
    """``output.icechunk.retain_runs`` (issue #582): ``0`` = keep every run (default)."""
    from zagg.config import get_icechunk_options

    value = get_icechunk_options(config).get("retain_runs")
    return int(value) if value else 0


def _run_tags_newest_first(repo) -> list[tuple[str, datetime]]:
    """Every ``run-`` tag with its snapshot's ``written_at``, newest first."""
    tags = []
    for tag in repo.list_tags():
        if tag.startswith(TAG_PREFIX):
            tags.append((tag, repo.lookup_snapshot(repo.lookup_tag(tag)).written_at))
    return sorted(tags, key=lambda t: t[1], reverse=True)


def _retain(repo, retain_runs: int) -> dict:
    """Apply the retention policy BEFORE this run's tag; the counts.

    K = ``retain_runs`` counts the run being finalized, so K − 1 earlier
    run tags stay. The expiry cutoff is a RUN TAG's commit time — the oldest
    retained one, or the newest dropped one when K = 1 — never ``now``: this
    run's commits are all newer than any earlier tag, and a concurrent
    writer's in-flight objects are never collected. With no earlier tag
    there is nothing to expire.
    """
    counts: dict[str, Any] = {
        "retain_runs": retain_runs,
        "tags_deleted": 0,
        "snapshots_expired": 0,
        "gc": None,
    }
    if retain_runs <= 0:
        return counts
    tags = _run_tags_newest_first(repo)
    keep, drop = tags[: retain_runs - 1], tags[retain_runs - 1 :]
    for tag, _when in drop:
        repo.delete_tag(tag)
    counts["tags_deleted"] = len(drop)
    cutoff: datetime | None = keep[-1][1] if keep else (drop[0][1] if drop else None)
    if cutoff is None:
        return counts
    counts["snapshots_expired"] = len(repo.expire_snapshots(cutoff))
    gc = repo.garbage_collect(cutoff)
    counts["gc"] = {
        k: int(getattr(gc, k))
        for k in ("snapshots_deleted", "manifests_deleted", "chunks_deleted", "bytes_deleted")
    }
    return counts


def finalize_repo(
    store_root: str,
    *,
    run_id: str,
    semantic_hash: str | None,
    retain_runs: int = 0,
    store_kwargs: dict,
    split_ratchet: dict | None = None,
) -> dict:
    """Retention, the ``finalize {run_id}`` commit and the run tag; the record.

    Refuses (raises) a missing or mismatched repo — callers are fail-open
    (D9). Returns ``{"path", "tag", "snapshot", "tagged", "retain_runs",
    "tags_deleted", "snapshots_expired", "gc", "rewrite_pending",
    "commit_s"}``; ``tagged`` is ``False`` when the tag already existed (a
    retried finalize), in which case nothing is written.
    """
    from zagg import __version__

    t0 = time.perf_counter()
    path = repo_path(store_root)
    repo, block = open_vetted(store_root, store_kwargs=store_kwargs)
    tag = run_tag(run_id)
    record: dict = {"path": path, "tag": tag, "rewrite_pending": split_ratchet}
    if tag in repo.list_tags():
        logger.info(f"icechunk finalize: {tag} already exists at {path}; nothing to do")
        return {
            **record,
            "snapshot": repo.lookup_tag(tag),
            "tagged": False,
            "retain_runs": retain_runs,
            "tags_deleted": 0,
            "snapshots_expired": 0,
            "gc": None,
            "commit_s": time.perf_counter() - t0,
        }
    counts = _retain(repo, retain_runs)
    metadata = {
        "run_id": run_id,
        "semantic_hash": semantic_hash,
        "zagg_version": __version__,
        **{k: block.get(k) for k in ("commit", "commit_order", "split_order")},
        **counts,
    }
    session = repo.writable_session(BRANCH)
    snapshot, _rebases = _commit(
        session,
        f"finalize {run_id}",
        local=_is_local(store_root),
        path=path,
        metadata=metadata,
        allow_empty=True,
    )
    repo.create_tag(tag, snapshot)
    if split_ratchet:
        logger.warning(
            f"icechunk finalize: split ratchet {split_ratchet['from']}->{split_ratchet['to']} "
            f"at {path} awaits an operator rewrite_manifests pass (spec §11.5)"
        )
    logger.info(f"Icechunk repo {path} tagged {tag} at snapshot {snapshot}")
    return {
        **record,
        "snapshot": snapshot,
        "tagged": True,
        **counts,
        "commit_s": time.perf_counter() - t0,
    }
