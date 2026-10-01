"""Reclaim superseded leaf versions no retained Icechunk snapshot references (spec §11.4).

Icechunk's own garbage collector never touches a virtual target, and a
versioned leaf (spec §1.5, issue #582) keeps every version it ever wrote
under its stable root until something removes it. This is that something:
an operator-run pass over a hive store that deletes the version subgroups
which are

- not the leaf's ``current`` (the root stamp's pointer), and
- not referenced by any **retained** snapshot — every snapshot in the
  ancestry of every branch and every tag of the store's repo, all tags, not
  only ``run-`` ones — and
- not **in flight**: a stamped version whose ``written_at`` is newer than the
  newest ``run-`` tag's finalize commit is kept (its run has no tag yet); an
  unstamped version (an attempt that died before its stamp) is reclaimed
  only once its own run's ``run-{run_id}`` tag exists.

A legacy leaf (a root stamp without ``current``) is never touched, and
neither are a converted leaf's legacy root arrays. A store without a repo
is refused. **DRY-RUN is the default**: the targets and their bytes are
printed and nothing is deleted; ``--execute`` deletes them.

    uv run python tools/icechunk_gc_targets.py s3://bucket/prefix/store.zarr [--execute] [--anon]

Cost: one ``all_virtual_chunk_locations`` per retained snapshot (a few
hundred thousand refs each at California scale), one stamp GET per leaf,
one LIST per versioned leaf. Operator-side only; never a worker's job.
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def referenced_versions(repo, container_prefix: str) -> tuple[set[tuple[str, str]], set[str]]:
    """``({(leaf_rel, version)}, {snapshot ids})`` over every retained snapshot.

    Retained is the ancestry of every branch and every tag. A location is
    ``url_prefix + key``; a key under ``{leaf}.zarr/run-…/`` names a version.
    """
    from zagg.hive import VERSION_PREFIX

    snapshots: set[str] = set()
    for branch in repo.list_branches():
        snapshots.update(s.id for s in repo.ancestry(branch=branch))
    for tag in repo.list_tags():
        snapshots.update(s.id for s in repo.ancestry(tag=tag))
    referenced: set[tuple[str, str]] = set()
    for snapshot in snapshots:
        for location in repo.readonly_session(snapshot_id=snapshot).all_virtual_chunk_locations():
            if not location.startswith(container_prefix):
                continue
            key = location[len(container_prefix) :]
            head, sep, tail = key.partition(".zarr/")
            if not sep:
                continue
            version = tail.split("/", 1)[0]
            if version.startswith(VERSION_PREFIX):
                referenced.add((f"{head}.zarr", version))
    return referenced, snapshots


def newest_run_tag(repo) -> tuple[str, datetime] | None:
    """The newest ``run-`` tag and its finalize commit's ``written_at``, or ``None``."""
    from zagg.icechunk_finalize import _run_tags_newest_first

    tags = _run_tags_newest_first(repo)
    return tags[0] if tags else None


def _run_of(version: str) -> str:
    """``run-{run_id}`` for a version name ``run-{run_id}-{attempt}``."""
    return version.rsplit("-", 1)[0]


def leaf_targets(
    leaf_path: str,
    leaf_rel: str,
    *,
    referenced: set[tuple[str, str]],
    newest_tag: tuple[str, datetime] | None,
    run_tags: set[str],
    store_kwargs: dict,
) -> list[dict]:
    """The collectable versions of one leaf: ``[{version, bytes, objects, reason}]``.

    ``reason`` says why each version is collectable (``superseded`` — stamped,
    older than the newest run tag, unreferenced; ``dead-attempt`` — unstamped,
    its run finalized). Kept versions are not returned.
    """
    import obstore

    from zagg.hive import VERSION_PREFIX, read_commit
    from zagg.store import open_object_store, open_store
    from zagg.windows import parse_utc

    stamp = read_commit(open_store(leaf_path, read_only=True, **store_kwargs))
    current = (stamp or {}).get("current")
    if not current:
        return []  # a legacy leaf (or debris): never a target
    store = open_object_store(leaf_path, **store_kwargs)
    listing = obstore.list_with_delimiter(store)
    versions = [
        p.rstrip("/").rsplit("/", 1)[-1]
        for p in listing["common_prefixes"]
        if p.rstrip("/").rsplit("/", 1)[-1].startswith(VERSION_PREFIX)
    ]
    out = []
    for version in versions:
        if version == current or (leaf_rel, version) in referenced:
            continue
        vstamp = read_commit(open_store(f"{leaf_path}/{version}", read_only=True, **store_kwargs))
        if vstamp is not None:
            # The stamp's clock is whole seconds (``isoformat(timespec="seconds")``)
            # while the tag's is sub-second: floor the tag so a stamp in the
            # same second as the finalize reads as in flight, never as older.
            floor = newest_tag[1].replace(microsecond=0) if newest_tag else None
            if floor is None or parse_utc(vstamp["written_at"]) >= floor:
                continue  # in flight: its run has no tag yet
            reason = "superseded"
        else:
            if _run_of(version) not in run_tags:
                continue  # an attempt of a run not yet finalized
            reason = "dead-attempt"
        objects = [o for batch in obstore.list(store, f"{version}/") for o in batch]
        out.append(
            {
                "version": version,
                "objects": len(objects),
                "bytes": sum(o["size"] for o in objects),
                "reason": reason,
            }
        )
    return out


def collect(store_root: str, *, store_kwargs: dict, execute: bool = False) -> dict:
    """The whole pass; the report ``{leaves, versioned, targets, bytes, deleted}``."""
    import obstore

    from zagg.hive import shard_leaf_path
    from zagg.icechunk_refs import container_prefix, open_vetted
    from zagg.store import open_object_store
    from zagg.sweep import discover_leaves

    repo, _block = open_vetted(store_root, store_kwargs=store_kwargs)  # refuses a repo-less store
    referenced, snapshots = referenced_versions(repo, container_prefix(store_root))
    newest = newest_run_tag(repo)
    run_tags = {t for t in repo.list_tags() if t.startswith("run-")}
    report: dict = {
        "store": store_root,
        "retained_snapshots": len(snapshots),
        "referenced_versions": len(referenced),
        "newest_run_tag": newest[0] if newest else None,
        "leaves": 0,
        "versioned": 0,
        "targets": [],
        "bytes": 0,
        "deleted": 0,
    }
    root = store_root.rstrip("/")
    per_leaf: dict = defaultdict(list)
    for shard_key, window in discover_leaves(store_root, store_kwargs=store_kwargs):
        leaf = shard_leaf_path(store_root, shard_key, window=window)
        leaf_rel = leaf[len(root) + 1 :]
        report["leaves"] += 1
        targets = leaf_targets(
            leaf,
            leaf_rel,
            referenced=referenced,
            newest_tag=newest,
            run_tags=run_tags,
            store_kwargs=store_kwargs,
        )
        if targets:
            per_leaf[leaf_rel] = targets
        for t in targets:
            report["targets"].append({"leaf": leaf_rel, **t})
            report["bytes"] += t["bytes"]
    report["versioned"] = sum(1 for _ in per_leaf)
    if execute:
        import shutil

        for target in report["targets"]:
            leaf = f"{root}/{target['leaf']}"
            store = open_object_store(leaf, **store_kwargs)
            keys = [
                o["path"] for batch in obstore.list(store, f"{target['version']}/") for o in batch
            ]
            obstore.delete(store, keys)
            if not root.startswith("s3://"):
                # An object store has no directories; a local one keeps the
                # emptied tree, which would still list as a version.
                shutil.rmtree(f"{leaf}/{target['version']}", ignore_errors=True)
            report["deleted"] += 1
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Delete the targets. Without it this is a dry-run: nothing is deleted",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument("--anon", action="store_true", help="Anonymous store access (dry-run only)")
    args = parser.parse_args(argv)
    store_kwargs: dict = {"region": args.region}
    if args.anon:
        if args.execute:
            parser.error("--anon cannot delete; drop --execute")
        store_kwargs["skip_signature"] = True
    report = collect(args.store_root, store_kwargs=store_kwargs, execute=args.execute)
    print(
        f"{report['store']}: {report['leaves']} leaves, {report['retained_snapshots']} retained "
        f"snapshots, {report['referenced_versions']} referenced versions, newest run tag "
        f"{report['newest_run_tag'] or '-'}"
    )
    for t in report["targets"]:
        print(
            f"  {t['leaf']}/{t['version']}  {t['objects']} objects  {t['bytes']:,} bytes  {t['reason']}"
        )
    verb = "deleted" if args.execute else "reclaimable (dry-run; --execute to delete)"
    print(f"{len(report['targets'])} version(s), {report['bytes']:,} bytes {verb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
