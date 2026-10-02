"""Metadata commits as first-class operations (spec §11.4 **Operations**, issue #582).

The repo is the store's metadata plane (spec §11 head): everything that
*evolves* — attrs and convention blocks, the pyramid declaration mirrored as
``multiscales`` and its level groups — is changed by a **commit on the
repo**, never by rewriting leaves. Each operation here is one such commit:
a message naming the operation, commit metadata ``{"operation",
"zagg_version", …}`` so history reads as a log, a **validation pass before
the commit** (the array model — shape, dtype, chunks, codecs, fill of every
array — must be identical before and after but for row growth: rows
appended, to every array at once; the ``zagg_icechunk``
compatibility keys must hold, every listed level must have its group; a
mutation that fails is discarded, nothing lands), and no leaf touched.

- ``set-attrs`` — merge JSON into the attrs of the root group, a level group
  or an array (``null`` deletes a key). The root's ``zagg_icechunk`` and
  ``multiscales`` blocks are the writer's and ``declare-pyramid``'s: refused.
- ``declare-pyramid`` — bring the repo's levels and ``multiscales`` mirror to
  the manifest's declaration (the §4.9 block ``sweep_overview.declare_pyramid``
  installs): a newly declared level gains its group and its manifest split; a
  level the manifest no longer declares moves from ``levels`` to ``retired``
  and keeps its group and split (its refs stay readable on every snapshot
  that names them); a level whose
  geometry the manifest would change is refused — an array-model change is a
  new revision, never an operation. The retrofit tool calls this itself
  when the store has a repo, so one operator step declares both planes.
- ``finalize`` — tag a ladder run its dispatcher left untagged (issue
  #588): the run's staged sweep completed but the dispatcher died before
  its finalize. Reads the run's dispatch manifest for its config (a large
  run's manifest is slim — no shard list — and serves as well; a run whose
  manifest write was lost, or whose slim block did not fit, is refused), refuses
  unless the newest staged-sweep record written since the run's init
  commit (the repo's clock, not the dispatcher's) shows a completed sweep,
  then runs the §11.4 finalize ``newest_only`` — so it can
  only ever tag the repo's newest run (an older untagged run stays covered
  by the next run's tag; tagging it would name later commits). No
  ``--force``: a run whose sweep did not complete has no tip that means
  "this run" — ``python -m zagg.sweep <store> --stages`` completes the
  ladder first. Retention is the run config's ``retain_runs``; no override.
  The record is tied to the run by time only (it names the sweep's own run
  id): with overlapping runs on one store (§11.4's documented casualty
  case) a sibling run's completed sweep record can vouch for this run.
  All of that runs **where the writer is**: on an ``s3://`` store the command
  fires one synchronous ``mode="icechunk_finalize"`` invoke and prints the
  worker's report — the checks, the commit, the tag and the retention are
  the worker's, and the operator's host reads nothing from the store and
  writes nothing (an operator holds invoke rights, not the bucket's; and
  the repo is not read out of its region). The worker is
  ``--function-name``, else ``ZAGG_LAMBDA_FUNCTION_NAME``, else
  ``process-shard``. Only a local store finalizes in-process.

    python -m zagg.icechunk_ops <store> set-attrs <path> '<json>'
    python -m zagg.icechunk_ops <store> declare-pyramid <config.yaml>
    python -m zagg.icechunk_ops <store> finalize <run_id>

``<path>`` is ``/`` (the root), ``/{cells}`` (a level group) or
``/{cells}/{array}``. ``set-attrs`` and ``declare-pyramid`` run on the
operator's host, never in a worker; ``finalize`` is operator-invoked and
worker-run (above). The pre-commit check is zagg's half of the validation
moczarr's reader will run (issue #582 phase 6).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from zagg.icechunk_refs import (
    BRANCH,
    ICECHUNK_ATTR,
    MULTISCALES_ATTR,
    _check_block,
    _commit,
    _is_local,
    _save_splits,
    _session_block,
    block_splits,
    open_vetted,
    repo_group_spec,
    repo_path,
)
from zagg.icechunk_rows import array_model as _array_model
from zagg.icechunk_rows import check_array_model, stored_rows_per_manifest

logger = logging.getLogger(__name__)

#: Root attrs no ``set-attrs`` may touch: the writer's block and the mirror.
RESERVED_ROOT_KEYS = (ICECHUNK_ATTR, MULTISCALES_ATTR)

#: Block keys an operation may never move (the array model and the container).
_FIXED_BLOCK_KEYS = ("spec", "shard_order", "chunk_order", "cell_order", "url_prefix")


def _node_path(path: str) -> str:
    return path.strip().strip("/")


def _validate(
    session, before: dict[str, dict], allow_new: tuple[str, ...], block: dict, path: str
) -> None:
    """The pre-commit check; raises ``ValueError`` and the caller discards the session.

    The array model may move by ROW GROWTH and nothing else
    (:func:`zagg.icechunk_rows.check_array_model`): labels appended to the
    block's ``rows``, every array holding exactly that many rows.

    It runs on the session as mutated, not after a rebase: ``_commit`` rebases
    with ``ConflictDetector``, which refuses any conflicting change, so a
    rebase never merges a foreign array-model change into what was checked.
    """
    import zarr

    root = zarr.open_group(session.store, mode="r")
    got = root.attrs.get(ICECHUNK_ATTR)
    if not isinstance(got, dict):
        raise ValueError(f"operation would remove the root {ICECHUNK_ATTR!r} block")
    check_array_model(before, _array_model(session), block["rows"], got.get("rows"), allow_new)
    # ``cell_order`` apart: in ``_check_block`` it would also vet the level
    # keying and blame the repo for what is this operation's refusal.
    _check_block(got, {k: block.get(k) for k in _FIXED_BLOCK_KEYS if k != "cell_order"}, path)
    if got.get("cell_order") != block.get("cell_order"):
        raise ValueError(f"operation would move the block's cell_order at {path}")
    levels = set(got.get("levels") or {})
    if str(block.get("cell_order")) not in levels:
        raise ValueError(f"operation would delist the base level /{block.get('cell_order')}")
    groups = {name for name, _ in root.members()}
    missing = sorted(levels - groups, key=int)
    if missing:
        raise ValueError(f"operation lists levels {missing} that have no group")


def _operation(
    store_root: str,
    name: str,
    mutate: Callable[[Any, dict], dict],
    *,
    store_kwargs: dict,
    message: str | None = None,
    metadata: dict | None = None,
    allow_new: tuple[str, ...] = (),
    repo=None,
    block: dict | None = None,
) -> dict:
    """Run ``mutate(session, block)`` as one validated commit; the report.

    ``mutate`` returns the report's details, ``{"unchanged": True}`` when it
    wrote nothing (no commit follows). ``allow_new`` are array-path prefixes
    the operation may create (a declared level's group).
    """
    from zagg import __version__

    if repo is None or block is None:
        repo, block = open_vetted(store_root, store_kwargs=store_kwargs)
    path = repo_path(store_root)
    session = repo.writable_session(BRANCH)
    # An init may have allocated rows since the vet: ``mutate`` and the check
    # work from the rows this session reads (§11.2).
    block = {**block, "rows": list(_session_block(session)["rows"])}
    before = _array_model(session)
    details = mutate(session, block)
    report = {"operation": name, "path": path, "snapshot": None, "message": None, **details}
    if details.get("unchanged"):
        logger.info(f"icechunk {name}: nothing to change at {path}")
        return report
    _validate(session, before, allow_new, block, path)
    message = message or name
    meta = {"operation": name, "zagg_version": __version__, **(metadata or {})}
    snapshot, rebases = _commit(
        session, message, local=_is_local(store_root), path=path, metadata=meta
    )
    logger.info(f"icechunk {name}: committed {snapshot} at {path}")
    return {**report, "snapshot": snapshot, "message": message, "rebases": rebases}


# ── set-attrs ───────────────────────────────────────────────────────────────


def set_attrs(
    store_root: str, path: str, updates: dict, *, store_kwargs: dict, message: str | None = None
) -> dict:
    """Merge ``updates`` into the attrs of the node at ``path``; one commit.

    A ``None`` value deletes the key. The root's reserved blocks are refused.
    Identical attrs commit nothing (``unchanged``). Returns the report with
    ``attrs`` (the node's attrs as committed) and ``keys`` (those touched).
    """
    import zarr

    node_path = _node_path(path)
    if not isinstance(updates, dict) or not updates:
        raise ValueError("set-attrs takes a non-empty JSON object")
    if not node_path and any(k in RESERVED_ROOT_KEYS for k in updates):
        raise ValueError(
            f"set-attrs refuses the root keys {RESERVED_ROOT_KEYS}: the block is the writer's "
            f"and the mirror is declare-pyramid's"
        )

    def mutate(session, _block):
        node = zarr.open(session.store, mode="r+", path=node_path)
        current = node.attrs.asdict()
        wanted = {**current, **{k: v for k, v in updates.items() if v is not None}}
        for key in (k for k, v in updates.items() if v is None):
            wanted.pop(key, None)
        details = {"node": f"/{node_path}", "keys": sorted(updates), "attrs": wanted}
        if wanted == current:
            return {**details, "unchanged": True}
        node.attrs.put(wanted)
        return details

    return _operation(
        store_root,
        "set-attrs",
        mutate,
        store_kwargs=store_kwargs,
        message=message or f"set-attrs /{node_path}",
        metadata={"node": f"/{node_path}", "keys": sorted(updates)},
    )


# ── declare-pyramid ─────────────────────────────────────────────────────────


def declare_pyramid(
    store_root: str, config, *, store_kwargs: dict, manifest: dict | None = None, grid=None
) -> dict:
    """Bring the repo's levels and ``multiscales`` mirror to the manifest's declaration.

    ``config`` is the store's pipeline config (the grid it defines — or
    ``grid``, when given — must be the repo's: the compatibility keys are
    vetted before anything is written).
    Levels are :func:`zagg.icechunk_refs.level_grids` of the manifest — the
    base plus the ``/2`` declaration's datasets. Returns the report with
    ``levels`` (as recorded), ``added``, ``dropped`` (left in the repo,
    delisted) and ``multiscales`` (whether the mirror is carried).
    """
    from zagg.grids import from_config
    from zagg.grids.base import vlen_dtype_warning_suppressed
    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.icechunk_refs import level_geometry

    if grid is None:
        grid = from_config(config)
    if manifest is None:
        manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root}: not a hive store root")
    want = {"shard_order": int(grid.parent_order), **level_geometry(grid)}
    repo, block = open_vetted(store_root, store_kwargs=store_kwargs, want=want)
    options = {k: block[k] for k in ("commit", "commit_order", "split_order")}
    # The row cut follows the repo as it stands, never this caller's config:
    # an absent key is a repo written before the §11.5 row split and keeps
    # its every-row-in-one cut (issue #584).
    options["rows_per_manifest"] = stored_rows_per_manifest(block)
    # A newly declared level's arrays are built at the repo's rows (§11.2).
    spec = repo_group_spec(grid, store_root, options, manifest, block["rows"])
    levels = spec.attributes[ICECHUNK_ATTR]["levels"]
    mirror = spec.attributes.get(MULTISCALES_ATTR)
    recorded = dict(block.get("levels") or {})
    for order in levels.keys() & recorded.keys():
        if levels[order] != recorded[order]:
            raise ValueError(
                f"level /{order} is recorded as {recorded[order]}, the manifest now declares "
                f"{levels[order]}: an array-model change is a new revision, not an operation"
            )
    added = sorted(levels.keys() - recorded.keys(), key=int)
    dropped = sorted(recorded.keys() - levels.keys(), key=int)
    # A delisted level's entry moves to ``retired`` (its group stays, and so
    # must its manifest split); relisting moves it back.
    retired = {**(block.get("retired") or {}), **{o: recorded[o] for o in dropped}}
    retired = {o: lvl for o, lvl in retired.items() if o not in levels}

    def mutate(session, session_block):
        import zarr

        # Built at the rows the session reads: an init may have grown them since the vet.
        members = spec.members
        if session_block["rows"] != block["rows"]:
            rows = session_block["rows"]
            members = repo_group_spec(grid, store_root, options, manifest, rows).members
        root = zarr.open_group(session.store, mode="r+")
        present = {name for name, _ in root.members()}
        for order in added:
            if order in present:
                # Delisted earlier and declared again: its group is still there,
                # and must still carry the declared model.
                if not _group_matches(session, order, members[order]):
                    raise ValueError(
                        f"level /{order} exists with another array model than the manifest "
                        f"declares: an array-model change is a new revision, not an operation"
                    )
                continue
            with vlen_dtype_warning_suppressed():
                members[order].to_zarr(session.store, order, overwrite=False)
        attrs = root.attrs.asdict()
        new_block = {**attrs[ICECHUNK_ATTR], "levels": levels, "retired": retired}
        if not retired:
            new_block.pop("retired")
        unchanged = (
            new_block == attrs[ICECHUNK_ATTR]
            and attrs.get(MULTISCALES_ATTR) == mirror
            and not added
        )
        details = {
            "levels": levels,
            "added": added,
            "dropped": dropped,
            "multiscales": mirror is not None,
        }
        if unchanged:
            return {**details, "unchanged": True}
        attrs[ICECHUNK_ATTR] = new_block
        if mirror is None:
            attrs.pop(MULTISCALES_ATTR, None)
        else:
            attrs[MULTISCALES_ATTR] = mirror
        root.attrs.put(attrs)
        return details

    report = _operation(
        store_root,
        "declare-pyramid",
        mutate,
        store_kwargs=store_kwargs,
        metadata={
            "semantic_hash": manifest.get("semantic_hash"),
            "added": added,
            "dropped": dropped,
        },
        allow_new=tuple(f"{o}/" for o in added),
        repo=repo,
        block=block,
    )
    if added and report["snapshot"]:
        # The new groups' manifest splits persist with the repo (§11.5) only
        # once the commit landed — a metadata commit writes no chunk, so they
        # are needed from the first ref commit on — and are cut from main's
        # block as it is NOW, so a concurrent init ratchet's split_order holds.
        # Residual: a ratchet saving between this read and this save is lost (last writer wins).
        current = _session_block(repo.readonly_session(BRANCH))
        _save_splits(repo, store_root, block_splits(current), store_kwargs)
    return report


def _group_matches(session, order: str, group_spec) -> bool:
    """Whether a delisted level's surviving group still carries the declared model."""
    import zarr
    from pydantic_zarr.experimental.v3 import GroupSpec

    def model(spec):
        return {
            n: {k: v for k, v in m.model_dump().items() if k != "attributes"}
            for n, m in spec.members.items()
        }

    have = GroupSpec.from_zarr(zarr.open_group(session.store, mode="r", path=order))
    return model(have) == model(group_spec)


# ── finalize ────────────────────────────────────────────────────────────────


class FinalizeRefusedError(ValueError):
    """A precondition of the operator ``finalize`` does not hold; nothing was written.

    Raised in-process by :func:`finalize_run`; the worker returns it as
    ``{"ok": false, "refused": reason}`` and :func:`finalize` re-raises it on
    the operator's host.
    """


#: The staged sweep's store-root run record (``zagg.sweep_stages._write_stage_record``).
_STAGE_RECORD_RE = re.compile(r"sweep_stats_(\d{8}T\d{6}Z)_stages\.json")


def _stage_record_incomplete(record: dict) -> str | None:
    """Why a staged-sweep run record may not stand for a completed ladder, or ``None``.

    The operator twin of :func:`zagg.runner._staged_sweep_incomplete`, read
    off the durable record instead of the dispatcher's summary: the finisher
    writes it as its last act, so its existence says the finisher landed;
    an expired barrier (propagated into the record, ``barrier_timed_out``)
    says a stage node may still have been committing when it did. The
    in-process sweep writes the record on failure too, with ``error`` — refused
    whatever else it carries. (``mode`` is not checked: :data:`_STAGE_RECORD_RE`
    already pins it, every ``_stages`` record is written with ``"stages"``.)
    """
    if record.get("error"):
        return f"the sweep failed ({record['error']})"
    if record.get("barrier_timed_out"):
        return "a barrier expired, so node commits may still have been in flight"
    if not isinstance(record.get("finisher"), dict):
        return "no finisher block"
    return None


def newest_stage_record(
    store_root: str,
    *,
    store_kwargs: dict,
    since: datetime | None = None,
    pipeline_run_id: str | None = None,
) -> tuple[str, dict] | None:
    """``(key, record)`` of the newest ``sweep_stats_*_stages.json`` at the root, or ``None``.

    One delimiter LIST of the store root (the timestamp-first naming sorts
    by write time). ``since`` drops records written before it — the
    ``written_at`` of a run's init commit — so a record from before the run
    can never stand for its ladder. The key's stamp is whole seconds, so
    ``since`` is floored to the second before the comparison.

    ``pipeline_run_id`` (issue #593) narrows it to the newest record that
    NAMES that run — the ``pipeline_run_id`` the dispatcher (or
    ``python -m zagg.sweep --stages --pipeline-run-id``) stamped into the
    sweep it chained, spec §4.7. Records are read newest first until one
    matches; a record naming another run, or none (``null``: the pass
    vouches for no run), is passed over, and so is one that is not a JSON
    object (it names no run either), with a warning. The newest match is
    returned even when an older one is complete: the caller refuses on it.
    Keys resolve to one second, so two passes finishing in the same second
    write one key and the later overwrites the earlier.
    """
    import obstore

    from zagg.store import open_object_store

    store = open_object_store(store_root, **store_kwargs)
    floor = since.replace(microsecond=0) if since is not None else None
    names = sorted(
        (
            name
            for o in obstore.list_with_delimiter(store)["objects"]
            if (m := _STAGE_RECORD_RE.fullmatch(name := o["path"].rsplit("/", 1)[-1]))
            and (
                floor is None
                or datetime.strptime(m[1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc) >= floor
            )
        ),
        reverse=True,
    )
    for name in names:
        try:
            record = json.loads(bytes(obstore.get(store, name).bytes()))
        except ValueError as e:
            record = e
        if not isinstance(record, dict):
            logger.warning(f"stage record {name} is not a JSON object ({record}) — passed over")
            continue
        if pipeline_run_id is None or record.get("pipeline_run_id") == pipeline_run_id:
            return name, record
    return None


def _run_dispatch_config(store_root: str, run_id: str, store_kwargs: dict):
    """The run's config off its dispatch manifest; raises when absent.

    The manifest at ``<store>.status/run-<run_id>/manifest.json`` (the setup
    invoke's worker-side write, issue #327) carries the very config the run
    dispatched — its ``retain_runs`` and the D19 hash the leaves were stamped
    with — which is why ``finalize`` takes no config and no ``--retain-runs``.
    A large hive run's manifest is slim (issue #588: its block did not fit
    the fire-and-forget setup ``Event``, so it rode without the ``shards``
    list) and still carries the config, which is all this reads. The write
    stays best-effort: a lost setup invoke or a failed PUT leaves no
    manifest, and so does a block that did not fit even slim (the run's
    summary then says ``dispatch_manifest: "dropped"``) — such a run cannot
    be finalized here.
    """
    from zagg.client_transport import MANIFEST_NAME, read_dispatch_manifest, run_status_prefix
    from zagg.config import load_config_from_dict

    prefix = run_status_prefix(store_root, run_id)
    manifest = read_dispatch_manifest(prefix, store_kwargs)
    if manifest is None or not manifest.get("config"):
        raise FinalizeRefusedError(
            f"no dispatch manifest with a config at {prefix}/{MANIFEST_NAME}: finalize reads "
            f"the run's retain_runs and semantic hash from it. A Lambda-dispatched run "
            f"has one, slim (no shard list) when the run is large; it is missing for a "
            f"wrong store/run_id, a dispatcher predating issue #327, a lost setup invoke "
            f"or write, or a block that did not fit the setup event even slim (the run "
            f'summary then says dispatch_manifest: "dropped"). '
            f"This operation cannot finalize such a run, and the next run's tag covers its "
            f"commits. A local-backend run finalizes in-process"
        )
    return load_config_from_dict(manifest["config"])


def _run_opened_at(store_root: str, run_id: str, store_kwargs: dict) -> datetime:
    """``written_at`` of the run's init commit on ``main``; raises when there is none.

    Every run opens with one (§11.4 **Init**: ``init {run_id}``, or the
    ``split ratchet … {run_id}`` commit when its init re-cuts —
    :func:`zagg.icechunk_finalize.run_marker`). Its stamp and the staged-sweep
    record's key are both worker/object-store-side clocks.
    """
    from zagg.icechunk_finalize import run_marker

    repo, _block = open_vetted(store_root, store_kwargs=store_kwargs)
    for info in repo.ancestry(branch=BRANCH):
        if run_marker(info.message) == ("init", run_id):
            return info.written_at
    raise FinalizeRefusedError(
        f"no init commit for run {run_id} on {repo_path(store_root)}: every run opens with "
        f"one (spec §11.4 Init): the run never initialized this repo, or a later run's "
        f"retention has squashed its commits (it is then not the newest run either)"
    )


def finalize_run(store_root: str, run_id: str, *, store_kwargs: dict) -> dict:
    """Tag a completed-but-untagged ladder run — the repo's newest — as its dispatcher would have.

    Every check and the write, in the calling process: the worker's
    ``mode="icechunk_finalize"`` under ``operator_checks``, or
    :func:`finalize` on a local store. Refuses (:class:`FinalizeRefusedError`)
    without the run's dispatch manifest, without the run's
    init commit on the repo, without a staged-sweep record that NAMES this
    run written since that commit (:func:`_run_opened_at` — the manifest
    supplies the config only), or when that record does not show a completed
    sweep. Otherwise
    :func:`zagg.icechunk_finalize.finalize_repo` with ``newest_only``: the
    report carries the finalize record plus ``operation``, ``run_id`` and
    ``stage_record`` (the record that vouched for the ladder); ``skipped``
    when the run is no longer the repo's newest or its tag already exists
    (nothing written either way).

    Two conditions tie the record to the run (issue #593). It must carry
    ``pipeline_run_id == run_id`` — stamped by the dispatcher that chained
    the sweep, or by ``python -m zagg.sweep --stages --pipeline-run-id`` for
    a pass completing a run whose dispatcher died — and it must be written
    since the run opened. Time alone let a sibling run's sweep, or an
    unrelated ``--stages`` pass, vouch for a ladder this run never built;
    a record naming another run, or none, now vouches for nothing here. The
    newest record naming the run decides: a later pass that names no run
    (or an unreadable record) neither vouches nor un-vouches, and a failed
    named retry refuses even over an earlier complete one — re-run the
    ``--pipeline-run-id`` pass to completion.
    """
    from zagg.icechunk_finalize import finalize_repo, resolve_retain_runs
    from zagg.semantics import semantic_hash

    config = _run_dispatch_config(store_root, run_id, store_kwargs)
    opened = _run_opened_at(store_root, run_id, store_kwargs)
    found = newest_stage_record(
        store_root, store_kwargs=store_kwargs, since=opened, pipeline_run_id=run_id
    )
    if found is None:
        remedy = (
            f"complete this run's ladder with `python -m zagg.sweep {store_root} --stages "
            f"--pipeline-run-id {run_id}`, then finalize"
        )
        other = newest_stage_record(store_root, store_kwargs=store_kwargs, since=opened)
        if other is None:
            raise FinalizeRefusedError(
                f"no staged-sweep record at {store_root} since run {run_id}'s init commit "
                f"({opened.isoformat(timespec='seconds')}): {remedy}"
            )
        raise FinalizeRefusedError(
            f"no staged-sweep record at {store_root} since run {run_id}'s init commit "
            f"({opened.isoformat(timespec='seconds')}) names this run: the newest, {other[0]}, "
            f"records pipeline_run_id {other[1].get('pipeline_run_id')!r} (null: the pass named "
            f"no run and vouches for none — issue #593), so it does not show this run's "
            f"ladder was built; {remedy}"
        )
    name, record = found
    reason = _stage_record_incomplete(record)
    if reason is not None:
        raise FinalizeRefusedError(
            f"staged-sweep record {name} does not show a completed sweep ({reason}); "
            f"re-run `python -m zagg.sweep {store_root} --stages --pipeline-run-id {run_id}`, "
            f"then finalize"
        )
    out = finalize_repo(
        store_root,
        run_id=run_id,
        semantic_hash=semantic_hash(config),
        retain_runs=resolve_retain_runs(config),
        store_kwargs=store_kwargs,
        newest_only=True,
    )
    report = {"operation": "finalize", "run_id": run_id, "stage_record": name, **out}
    if not out["tagged"] and "skipped" not in out:
        report["skipped"] = f"{out['tag']} already exists"
    logger.info(
        f"icechunk finalize {run_id}: "
        + (f"tagged {out['tag']}" if out["tagged"] else f"nothing written ({report['skipped']})")
    )
    return report


def _lambda_client(region: str | None):
    """A Lambda client for the one finalize invoke: no retry, a read past the 900 s ceiling."""
    import boto3
    from botocore.config import Config

    config = Config(read_timeout=960, connect_timeout=10, retries={"max_attempts": 0})
    return boto3.client("lambda", region_name=region, config=config)


def finalize(
    store_root: str,
    run_id: str,
    *,
    store_kwargs: dict,
    function_name: str | None = None,
    lambda_client=None,
) -> dict:
    """The operator's ``finalize``: :func:`finalize_run`, run where the writer is.

    A local store runs it in-process. Any other store gets ONE synchronous
    ``mode="icechunk_finalize"`` invoke with ``operator_checks`` (through
    :func:`zagg.runner._invoke_lambda_icechunk_finalize`): the worker reads
    the manifest and the repo, checks, commits, tags and applies retention
    under its execution role, and this returns its report (plus
    ``invoke_s``). The operator's host opens neither the store nor the repo
    and never falls back to finalizing in-process — it may hold no write
    credentials (a Source Cooperative store's only writer is the worker
    role), and a retention pass from outside the store's region would read
    the repo out of it.

    The function is ``function_name`` (``--function-name``), else the
    ``ZAGG_LAMBDA_FUNCTION_NAME`` environment variable, else
    :data:`zagg.runner.DEFAULT_FUNCTION_NAME` (``process-shard``), an empty
    value counting as unset — what :func:`zagg.runner._resolve_function_name`
    gives a config with no ``worker:`` block, except that the dispatchers
    take an empty value verbatim. The run config's ``worker:`` suffix is never applied:
    that config is in the run's dispatch manifest, which this host does not
    read. The report names the function under ``function_name``. The event
    carries no config, so a deployed worker that predates ``operator_checks``
    fails on the missing key before any write instead of tagging without the
    checks.

    Raises :class:`FinalizeRefusedError` with the worker's reason when a
    precondition does not hold, ``RuntimeError`` when the invoke failed or
    the worker errored.
    """
    if _is_local(store_root):
        return finalize_run(store_root, run_id, store_kwargs=store_kwargs)
    from zagg.runner import DEFAULT_FUNCTION_NAME, _invoke_lambda_icechunk_finalize

    function_name = (
        function_name or os.environ.get("ZAGG_LAMBDA_FUNCTION_NAME") or DEFAULT_FUNCTION_NAME
    )
    if lambda_client is None:
        lambda_client = _lambda_client(store_kwargs.get("region"))
    out = _invoke_lambda_icechunk_finalize(
        lambda_client, function_name, store_root, run_id=run_id, operator_checks=True
    )
    if "refused" in out:
        raise FinalizeRefusedError(out["refused"])
    if "error" in out:
        # Only a stale worker is known not to have finalized; a transport
        # failure after delivery leaves the worker's outcome unknown.
        stale = "'config'" in out["error"]
        hint = (
            "; a missing 'config' is a worker that predates the operator finalize, which "
            "fails on that key before any write: deploy a current worker"
            if stale
            else f"; if the request reached it the worker may have tagged: re-run finalize, "
            f"which reports an existing run-{run_id} tag and rewrites nothing"
        )
        outcome = "did not finalize" if stale else "may not have finalized"
        raise RuntimeError(
            f"the worker {function_name} {outcome} run {run_id}: {out['error']} "
            f"(nothing was written from this host{hint})"
        )
    return {**out, "function_name": function_name}


# ── CLI ─────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    sub = parser.add_subparsers(dest="operation", required=True)
    p = sub.add_parser("set-attrs", help="merge JSON into a node's attrs (null deletes a key)")
    p.add_argument("path", help="/ (root), /{cells} (a level) or /{cells}/{array}")
    p.add_argument("updates", help="a JSON object")
    p.add_argument("--message", default=None, help="commit message (default: the operation)")
    p = sub.add_parser("declare-pyramid", help="mirror the manifest's declaration into the repo")
    p.add_argument("config", help="the store's pipeline config YAML")
    p = sub.add_parser(
        "finalize", help="tag a completed, untagged ladder run (the repo's newest run only)"
    )
    p.add_argument("run_id", help="the run id (its dispatch manifest and run-<id> tag name it)")
    p.add_argument(
        "--function-name",
        default=None,
        help="the Lambda worker that finalizes an s3:// store (else env "
        "ZAGG_LAMBDA_FUNCTION_NAME, else process-shard; a local store finalizes in-process)",
    )
    args = parser.parse_args(argv)
    store_kwargs = {"region": args.region}
    if args.operation == "set-attrs":
        report = set_attrs(
            args.store_root,
            args.path,
            json.loads(args.updates),
            store_kwargs=store_kwargs,
            message=args.message,
        )
    elif args.operation == "declare-pyramid":
        from zagg.config import load_config

        report = declare_pyramid(
            args.store_root, load_config(args.config), store_kwargs=store_kwargs
        )
    else:
        report = finalize(
            args.store_root,
            args.run_id,
            store_kwargs=store_kwargs,
            function_name=args.function_name,
        )
    print(json.dumps(report, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
