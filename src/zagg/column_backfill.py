"""Leaf-column backfill for pre-column stores (issue #520): the ``/1 -> /2`` bridge.

Leaf columns (:mod:`zagg.column`, issues #383/#391 — the gen-1 partials the
staged sweep folds) are written only by pyramid-ON builds whose D24
classifiers admit the fields. Every store built before the 0.50 classifiers
is column-less, and a column-less store cannot take the ``/2`` staged sweep
at any transport.

This module is the missing bridge, and it owns both halves. The READ-BACK
path is :mod:`zagg.column`'s build-time fold recipe re-run against a leaf's
own stored arrays instead of the writer's resident #342 sink
(:func:`stored_leaf_slabs` -> :func:`column_from_leaf`), with the gates that
decide whether a column is owed at all (:func:`manifest_column_plan`) and
whether one already there is current (:func:`column_is_current`). The PASS
(:func:`backfill_columns`) drives it: one visit per committed leaf. Between
them, an existing published store is upgraded **without re-aggregation**, at
the cost of one leaf-reading pass, paid once.

It is a **sweep family** (``columns``, registered in :mod:`zagg.sweep`), not
a mode of its own: a registry entry needs no NEW transport and no new mode,
so the work-set normalization, partitioning (issue #377) and the discovery
seam are inherited whole rather than rebuilt. Three entry points, all
reaching the same :func:`backfill_columns`: the CLI
(``python -m zagg.sweep <root> --families columns``),
``zagg.sweep.run_sweep(..., families=["columns"])``, and — since the handler's
``mode: "sweep"`` arm began forwarding the event's ``families``/``partition``
blocks (issue #527, PR #528) — a fleet invoke naming the family.

One caveat on the fleet arm, and it is about PARTITIONING, not the family:
``--partitions`` is genuinely sequential in-process
(:func:`zagg.sweep_partition.sweep_partitions` runs one at a time), while the
runner fires fleet partitions as CONCURRENT ``Event`` invokes — and the
sweep-admission lease is store-granular by correctness, so it admits exactly
one of them. A fleet-scale PARTITIONED backfill therefore wants either a single
unpartitioned invoke or a scoped lease; that is an open espg design call, and
this module deliberately keeps the store-granular behavior until it is ruled.

It is deliberately NOT in :data:`zagg.sweep.DEFAULT_FAMILIES` — a backfill is
an explicit upgrade operation, never something a routine rollup sweep does
behind an operator's back. Spell it:
``python -m zagg.sweep <root> --families columns``.

**Declaration-driven, never guessed.** The gate is the manifest's own
``zagg-pyramid/2`` declaration (:func:`manifest_column_plan`) —
a ``/1`` schedule, a declared-off block, or a block whose every field is D24
``class: "none"`` refuses BY NAME and says re-declare first. Those are
declaration bugs, and backfilling one would publish a morton-only column (or
no artifact) as an upgrade.

**Who may write a column.** Spec §4.6's single-writer law is written for the
fleet: a column's writer is its leaf's worker. This pass is the one
sanctioned exception, and it inherits the law rather than repealing it —
:mod:`zagg.sweep_lease` serializes it against every other sweep, and it must
run against a store **no aggregation run is writing**. That second half is an
operator precondition, not something the lease can enforce: the lease's ruled
concurrency matrix allows fleet ∥ sweep precisely because their object sets
are disjoint, and this is the one pass for which they are not. Quiescence
cannot be PROVEN from inside a run — that needs a global run registry zagg
deliberately lacks (D8) — so the pass ships a best-effort smoke alarm instead:
:func:`warn_if_runs_in_flight` reads the issue #327 status channel and names
any recently active run in a WARNING before the first write. It never refuses,
and its silence is not evidence of quiescence (espg ruling 2026-08-25).
"""

from __future__ import annotations

import json
import logging
import time
from typing import NamedTuple

from zagg.column import (
    COLUMN_SPEC,
    _column_provenance,
    column_resolutions,
    composable_fields,
    fold_column,
)

logger = logging.getLogger(__name__)

#: Share of the lease TTL a beat must land inside — the throttle
#: :mod:`zagg.sweep_stages` uses, and for the same reason. The beat is thrown
#: by the WALL CLOCK, never by a leaf count: nothing bounds what one leaf
#: costs (:func:`zagg.column.write_leaf_column`'s node-order k-way merge is
#: measured in GB, and this pass reads the whole leaf on top of it), so a
#: count-based interval is an unenforced assumption about seconds per leaf and
#: a slow run silently outlives the TTL while still writing (review finding,
#: issue #520). A ``time.monotonic()`` per leaf costs nothing; a PUT per leaf
#: against the intent object would not.
HEARTBEAT_FRACTION = 3

#: How recent a §4.6-relevant status object must be to read as a run possibly
#: still in flight. The fleet's worker wall is the natural unit: a Lambda unit
#: is killed at 900 s, so a status object older than that cannot belong to a
#: unit still executing — its writer is gone either way. Deliberately the WHOLE
#: wall rather than a fraction: this probe's only job is to be loud before a
#: precondition is risked, and a false alarm costs a log line while a missed
#: one costs a chimera column.
IN_FLIGHT_WINDOW_S = 900


def warn_if_runs_in_flight(store_root: str, store_kwargs: dict, *, window_s=None) -> list[str]:
    """Warn — never refuse — when the status channel shows a recent run.

    The backfill's one enforceable-ish half of §4.6's operator precondition
    (espg ruling 2026-08-25 on the PR #524 Q1 thread). Quiescence cannot be
    PROVEN: doing so needs a global run registry zagg deliberately lacks (D8 —
    the dispatcher is not a control plane), and the failure mode is bounded and
    self-healing anyway (a column folded beside a concurrent leaf write is a
    regenerable cache, D9; the skip gate usually catches it, and the next
    backfill + sweep heals it). So this is a best-effort smoke alarm, not a
    gate.

    What it reads: the issue #327 status channel, a SIBLING of the store
    (``{store}.status/run-{run_id}/shard-*.json``,
    :func:`zagg.client_transport.run_status_prefix`) — the one place a fleet run
    leaves a per-unit trace this pass can see without a control plane. A run id
    with any object newer than ``window_s`` (:data:`IN_FLIGHT_WINDOW_S`) is
    named in a WARNING; the return is those run ids, for the summary and the
    tests.

    Three things it deliberately is NOT. It never refuses and never fails the
    pass — an operator who knows the run is finished (the channel is not pruned
    in band, espg ruling on #327: accumulation hygiene is the prefix delete
    policy) must be able to proceed. It is FAIL-OPEN on the listing itself: a
    status channel that is absent, unreadable, or credential-scoped away is not
    a reason to block an upgrade, so any exception is swallowed to a debug line.
    And it proves nothing when SILENT — a store written by a dispatcher that
    predates the channel, or one whose status prefix was lifecycled away, looks
    exactly like a quiesced one. Absence of a warning is not evidence of
    quiescence; the operator precondition stands either way.
    """
    from datetime import datetime, timedelta, timezone

    window_s = IN_FLIGHT_WINDOW_S if window_s is None else int(window_s)
    try:
        import obstore

        from zagg.store import open_object_store

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_s)
        store = open_object_store(f"{store_root.rstrip('/')}.status", **store_kwargs)
        recent: set = set()
        for batch in obstore.list(store):
            for meta in batch:
                modified = meta.get("last_modified")
                if modified is None or modified < cutoff:
                    continue
                head = str(meta["path"]).lstrip("/").split("/", 1)[0]
                if head.startswith("run-"):
                    recent.add(head.removeprefix("run-"))
    except Exception as e:
        logger.debug(f"backfill: status-channel in-flight probe skipped at {store_root}: {e}")
        return []
    if recent:
        logger.warning(
            f"backfill: the status channel at {store_root}.status shows {len(recent)} run(s) "
            f"with activity in the last {window_s}s — {sorted(recent)}. A backfill is the one "
            f"pass for which the fleet and a sweep are NOT write-disjoint (spec §4.6): if any "
            f"of these is still writing leaves, a column may be folded from cells that are "
            f"about to move, or interleave with the leaf worker's own column write. Proceeding "
            f"anyway (this is a warning, never a refusal) — confirm the runs are finished, or "
            f"re-run the backfill afterwards; it is idempotent."
        )
    return sorted(recent)


def backfill_columns(
    store_root: str,
    manifest: dict,
    by_shard: dict,
    store_kwargs: dict | None = None,
    min_order: int = 0,
    *,
    force: bool = False,
    run_id: str | None = None,
    lease: bool = True,
) -> dict:
    """Write the missing leaf column for every committed leaf in the work set.

    The ``columns`` family's whole-tree hook (:meth:`SweepFamily.sweep_store`'s
    signature, so :func:`zagg.sweep.run_sweep` dispatches straight to it).
    ``by_shard`` is the normalized ``{shard decimal: {window, ...}}`` work set
    the engine already filtered to this partition; every column lives at its
    own leaf's node prefix, at the shard order, which is at or below any
    partition split order — so partition disjointness holds by construction
    and ``min_order`` is accepted (the issue #377 contract is that a hook
    reads it, not swallows it) and recorded, never used to move a write.

    Per ``(leaf, window)``: an uncommitted leaf contributes nothing
    (``empty``); a column already current under
    :func:`column_is_current` is left alone (``current``, unless
    ``force``); anything else is recomputed from the leaf's stored bytes and
    written wholesale (``written``). A leaf that cannot be read or folded is
    logged and counted (``failed``), never fatal — the families sweep's
    posture, and a half-upgraded store is exactly as readable as an
    un-upgraded one, since readers never require a column (§4.6).

    **A store-wide fault is not N leaf faults.** One leaf that cannot be read
    or folded is counted and skipped, but a pass that ends having written
    nothing, skipped nothing as current, and FAILED at least one leaf raises,
    naming the last error. That is the shape an expired credential, a denied
    column prefix or a region-wide outage takes — every remaining leaf raising
    the same exception — and returning it as an ordinary
    ``{"written": 0, "failed": <every leaf>}`` summary would send the runbook
    straight on to the staged sweep over a store with no columns (review
    finding, issue #520). A PARTIAL failure still returns, with a
    ``logger.error``: the operator's gate there is the summary, and
    ``docs/pyramid_upgrade.md`` step 3 requires ``failed == 0`` before
    sweeping. Either way the repair is the same, because the pass is
    idempotent: fix the fault and re-run it.

    Before the first write (and before the lease, which cannot see a fleet) the
    pass runs :func:`warn_if_runs_in_flight` over the issue #327 status channel
    and logs a loud WARNING naming any run active within
    :data:`IN_FLIGHT_WINDOW_S`. It is advisory only — never a refusal, never a
    failure, fail-open on the listing — and the flagged ids ride the summary as
    ``runs_in_flight``.

    ``lease=False`` skips admission — for callers that already hold the store's
    sweep lease. Otherwise the pass takes it for its whole duration
    (:mod:`zagg.sweep_lease`), beating on the WALL CLOCK — once a leaf lands
    more than ``ttl_s`` / :data:`HEARTBEAT_FRACTION` after the last beat — and
    releasing in a ``finally``.

    Returns the family summary the sweep engine records:
    ``written``/``current``/``empty``/``failed`` counts plus the declaration
    it worked to.
    """
    import uuid

    from zagg.sweep_lease import (
        DEFAULT_TTL_S,
        acquire_lease,
        heartbeat_lease,
        release_lease,
    )

    store_kwargs = dict(store_kwargs or {})
    # BEFORE admission: a store that must be re-declared should say so without
    # first taking (and having to release) the store's sweep lease.
    plan = manifest_column_plan(manifest)
    # Loop-invariant by construction — its three arguments are the plan's, and
    # a plan does not move inside a pass — but ~35 pydantic dumps and JSON
    # round-trips per call, so it is derived ONCE here rather than per leaf
    # inside the gate (review finding, issue #520).
    template = column_structure(
        plan.fields, node_order=plan.node_order, resolutions=plan.resolutions
    )
    counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
    # Best-effort, before the lease: a run in flight is an operator problem to
    # know about, and the lease cannot see it (it serializes sweeps, not fleets).
    in_flight = warn_if_runs_in_flight(store_root, store_kwargs)
    run_id = run_id or f"backfill-{uuid.uuid4().hex[:12]}"
    held = None
    if lease:
        held = acquire_lease(store_root, run_id=run_id, store_kwargs=store_kwargs)
    beat_after = int((held or {}).get("ttl_s") or DEFAULT_TTL_S) / HEARTBEAT_FRACTION
    last_beat = time.monotonic()
    last_error = None
    try:
        for decimal in sorted(by_shard):
            for window in sorted(by_shard[decimal], key=lambda w: (w is not None, w or "")):
                failure = _backfill_leaf(
                    store_root,
                    decimal,
                    window,
                    plan,
                    counts,
                    store_kwargs,
                    force=force,
                    template=template,
                )
                last_error = failure or last_error
                if held is not None and time.monotonic() - last_beat >= beat_after:
                    held = heartbeat_lease(store_root, held, store_kwargs=store_kwargs)
                    last_beat = time.monotonic()
    finally:
        if held is not None:
            release_lease(store_root, run_id=run_id, store_kwargs=store_kwargs)
    if counts["failed"]:
        if not counts["written"] and not counts["current"]:
            # Raised OUTSIDE the try so the lease is already released: this is a
            # loud return, not a torn pass.
            raise RuntimeError(
                f"column backfill on {store_root} wrote nothing: all {counts['failed']} "
                f"leaf attempts failed, the last with {last_error}. A whole-store fault "
                f"(expired credentials, a denied column prefix, an outage) is not N leaf "
                f"faults — returning it as an ordinary summary would send the /1 -> /2 "
                f"runbook on to the staged sweep over a column-less store. The pass is "
                f"idempotent: fix the fault and re-run it"
            )
        logger.error(
            f"column backfill on {store_root}: {counts['failed']} of "
            f"{sum(len(w) for w in by_shard.values())} leaves FAILED (last: {last_error}). "
            f"`failed` must be 0 before the staged sweep — the pass is idempotent, re-run it"
        )
    return {
        **counts,
        "run_id": run_id,
        "min_order": int(min_order),
        "resolutions": list(plan.resolutions),
        "fields": sorted(plan.fields),
        # The run ids the status-channel probe flagged, recorded so the sweep's
        # own run record carries the warning an operator may have scrolled past.
        "runs_in_flight": in_flight,
    }


def _backfill_leaf(
    store_root, decimal, window, plan, counts, store_kwargs, *, force, template=None
) -> str | None:
    """One ``(leaf, window)``: gate, fold from stored bytes, write. Never raises.

    Returns the failure's text (``None`` on any other outcome) so the driver
    can name the LAST error when a whole-store fault turns every leaf into a
    ``failed`` count.

    Split out of the driver so a fold that blows up on one leaf costs that
    leaf and nothing else — the ``failed`` count the families sweep reports.
    The read order is cheapest-first: the leaf's commit stamp (a leaf that is
    not committed has no column to write, and unstamped debris is invisible
    exactly as the walk treats it), then the column's own stamp, attrs and
    member listing for the skip test, and only then the leaf's arrays.
    """
    from zagg.column import COLUMN_ATTR, write_column
    from zagg.grids.morton import morton_word

    try:
        shard_key = morton_word(decimal)
        leaf_stamp = _leaf_stamp(store_root, shard_key, window, store_kwargs)
        if leaf_stamp is None:
            counts["empty"] += 1
            return
        if not force:
            # Inside the gate, never above it: ``_column_state``'s member walk
            # is a LIST plus one ``zarr.json`` GET per member, and ``force``
            # rewrites the prefix wholesale — the read would buy nothing
            # (review finding, issue #520).
            stamp, attrs, structure = _column_state(store_root, shard_key, window, store_kwargs)
            current, reason = column_is_current(
                leaf_stamp,
                stamp,
                attrs.get(COLUMN_ATTR),
                structure,
                node_order=plan.node_order,
                cell_order=plan.cell_order,
                resolutions=plan.resolutions,
                fields=plan.fields,
                template=template,
            )
            if current:
                counts["current"] += 1
                return
            logger.debug(f"backfill: leaf {decimal} window {window!r} not current ({reason})")
        folded = column_from_leaf(
            store_root,
            shard_key,
            plan.fields,
            node_order=plan.node_order,
            cell_order=plan.cell_order,
            resolutions=plan.resolutions,
            window=window,
            store_kwargs=store_kwargs,
        )
        write_column(
            store_root,
            shard_key,
            folded,
            plan.fields,
            node_order=plan.node_order,
            cell_order=plan.cell_order,
            window=window,
            time_range=leaf_stamp.get("time_range"),
            granule_count=int(leaf_stamp.get("granule_count") or 0),
            store_kwargs=store_kwargs,
        )
        counts["written"] += 1
    except Exception as e:
        logger.warning(f"backfill: leaf {decimal} window {window!r} failed ({e})")
        counts["failed"] += 1
        return f"{type(e).__name__}: {e}"
    return None


def _leaf_stamp(store_root, shard_key, window, store_kwargs):
    """The leaf's D4 commit stamp, or ``None`` for absent/unstamped debris."""
    from zagg.hive import read_commit, shard_leaf_path
    from zagg.store import open_store

    leaf = shard_leaf_path(store_root, shard_key, window=window)
    return read_commit(open_store(leaf, read_only=True, **store_kwargs))


def _column_state(store_root, shard_key, window, store_kwargs) -> tuple:
    """The stored column as ``(commit stamp, root attrs, realized structure)``.

    The three things :func:`column_is_current` gates on. The
    structure — one member listing per resolution group
    (:func:`stored_column_structure`) — is read here rather than
    derived from the attrs because the attrs cannot carry it: a companion
    channel added at re-declaration leaves the recorded provenance identical
    and the member set a sibling short (review finding, issue #520).

    ``(None, {}, {})`` when nothing is there — the ordinary pre-column case
    this whole module exists for, and the same answer an unreadable prefix
    gives: the verdict either way is "not current", which rewrites.
    """
    import zarr

    from zagg.column import column_name
    from zagg.hive import COMMIT_ATTR, shard_leaf_path
    from zagg.store import open_store

    leaf = shard_leaf_path(store_root, shard_key, window=window)
    path = f"{leaf.rstrip('/').rsplit('/', 1)[0]}/{column_name(window)}"
    try:
        group = zarr.open_group(
            open_store(path, read_only=True, **store_kwargs), path="", mode="r", zarr_format=3
        )
        attrs = dict(group.attrs)
        structure = stored_column_structure(group)
    except Exception:
        return None, {}, {}
    stamp = attrs.get(COMMIT_ATTR)
    return (dict(stamp) if isinstance(stamp, dict) else None), attrs, structure


def stored_leaf_slabs(
    leaf_path: str, fields: dict, *, cell_order: int, n_cells: int, store_kwargs: dict | None = None
) -> dict:
    """``{field: cell slab}`` fold inputs read back from a COMMITTED leaf.

    The read-back twin of :func:`zagg.column.leaf_slabs` (issue #520): same filter
    (:func:`zagg.column.composable_fields`), same ``(n_cells,)`` extent refusal, same
    companion pickup — but the values come from the leaf's stored arrays
    instead of the writer's in-memory #342 sink. That equality is the whole
    bridge: a store built before the 0.50 classifiers has no column, and the
    only surviving record of what the leaf worker would have folded is the
    leaf itself. Feeding this map to :func:`zagg.column.fold_column` therefore
    reproduces the build-time column exactly — the byte-identity
    characterization pinned in ``tests/test_column_backfill.py``.

    The absent-key rule is the one place the twins part, and only because a
    sink cannot reach the case: an absent PAYLOAD folds as fill (schema
    evolution — a field the leaf predates), while an absent declared COMPANION
    refuses the leaf by name, since folding a payload apart from its channel is
    not defined.

    Two guards the staged sink cannot need but a read-back must, both
    borrowed from the sweep's own from-leaves fold
    (:func:`zagg.sweep_overview._fold_node`): the leaf's ``morton`` extent pins
    the geometry (a leaf at another cell order is not this declaration's leaf
    — mixed-order sources are unsupported, issue #347), and every digest
    field's stored §2.0 ``weights`` / §8.4 companion declaration is checked
    against the manifest's (:func:`zagg.sweep_overview.check_weights_match`,
    :func:`zagg.sweep_overview.check_companion_match`). All three raise: a
    backfill folding across a declaration mismatch would publish a column
    whose weight column means neither thing, and the caller turns the raise
    into one loudly skipped leaf.

    ``_fold_node`` carries a THIRD guard this does not: the D4 commit stamp,
    which is what separates a leaf from an interrupted writer's prefix. Here
    it is the CALLER's, not because it matters less but because the caller has
    already paid for it — :func:`_backfill_leaf` reads
    ``hive.read_commit`` first, cheapest-first, and an unstamped leaf is
    counted ``empty`` and never reaches this function
    (``test_uncommitted_leaf_contributes_nothing``). Hence the COMMITTED in
    the summary line: it is a precondition of calling this, and a caller that
    skips it folds whatever bytes are there (review finding, issue #520).
    """
    import zarr

    from zagg.store import open_store
    from zagg.sweep_overview import (
        _empty_slab,
        check_companion_match,
        check_weights_match,
        field_companions,
    )

    cell_order, n_cells = int(cell_order), int(n_cells)
    group = zarr.open_group(
        open_store(leaf_path, read_only=True, **dict(store_kwargs or {})),
        path=str(cell_order),
        mode="r",
        zarr_format=3,
    )
    morton = group["morton"]
    if morton.shape != (n_cells,):
        raise ValueError(
            f"leaf {leaf_path} carries {morton.shape} morton words, not the declared "
            f"cell_order {cell_order} subtree ({n_cells} cells) — mixed-order source "
            f"leaves are unsupported (issue #347)"
        )

    def _slab(key: str, meta: dict, *, channel: tuple | None = None):
        try:
            arr = group[key]
        except KeyError:
            if channel is not None:
                # NOT the fill arm: a synthesized all-empty sibling satisfies
                # ``fold_column``'s required-by-name pairing guard, so the pair
                # would be folded APART and surface from the kernel as a §1.1
                # row-alignment error that reads as data corruption. The true
                # diagnosis is a declaration the leaves predate — the expected
                # ``/1`` -> ``/2`` retrofit, since ruling 4 on issue #410 is
                # what made a located digest field ``approximate`` at all — so
                # refuse the leaf here, the way ``_fold_node`` does at the read
                # (review finding, issue #520).
                raise ValueError(
                    f"field {channel[0]!r} declares a {channel[1]} channel but leaf "
                    f"{leaf_path} carries no {key!r} array — these leaves predate the "
                    f"declaration. Re-declare without the channel, or rebuild the leaf; "
                    f"no fold of the payload alone is defined (spec §1.1)"
                ) from None
            # Schema evolution, and the ragged writer's all-empty skip: the
            # stored cells are the fill either way, which is exactly what the
            # staged sink synthesizes for an absent key.
            return _empty_slab(meta, n_cells)
        if channel is None:
            if meta["class"] == "approximate":
                check_weights_match(dict(arr.attrs), meta, key)
        else:
            check_companion_match(dict(arr.attrs), channel[0], channel[1])
        values = arr[:]
        if values.shape != (n_cells,):
            raise ValueError(
                f"stored slab for field {key!r} has shape {values.shape}, not the leaf's "
                f"({n_cells},) cell extent — refusing to fold a column from a leaf that "
                f"disagrees with the grid"
            )
        return values

    slabs: dict = {}
    for name, meta in composable_fields(fields).items():
        slabs[name] = _slab(name, meta)
        for kwarg, sibling in field_companions(name, meta):
            slabs[sibling] = _slab(sibling, meta, channel=(name, kwarg))
    return slabs


def column_from_leaf(
    store_root: str,
    shard_key,
    fields: dict,
    *,
    node_order: int,
    cell_order: int,
    resolutions: list,
    window: str | None = None,
    store_kwargs: dict | None = None,
) -> dict:
    """Recompute one leaf's column fold from its STORED bytes (issue #520).

    :func:`stored_leaf_slabs` -> :func:`zagg.column.fold_column`, the same two calls
    :func:`zagg.column.write_leaf_column` makes against the resident staged sink — so the
    result is the build-time column's ``folded`` map, byte for byte, for any
    leaf whose stored arrays are what that worker PUT. That equality is the
    issue #520 phase 1 characterization and the whole basis of the ``/1``
    -> ``/2`` upgrade: a pre-column store's leaves still carry every input
    the fold ever had.

    Pure compute: nothing is written and no gate is consulted. The caller
    (:func:`backfill_columns`) owns the D4 commit gate,
    the declaration gate, the skip-if-current test, and the write — this
    inherits :func:`stored_leaf_slabs`' COMMITTED-leaf precondition unchanged.
    """
    from zagg.hive import shard_leaf_path

    return fold_column(
        stored_leaf_slabs(
            shard_leaf_path(store_root, shard_key, window=window),
            fields,
            cell_order=cell_order,
            n_cells=4 ** (int(cell_order) - int(node_order)),
            store_kwargs=store_kwargs,
        ),
        fields,
        cell_order=int(cell_order),
        resolutions=resolutions,
    )


def _member_metadata(raw) -> dict:
    """One array's zarr metadata, normalized so the two projections compare.

    ``node_type`` is dropped because a ``pydantic_zarr`` ``ArraySpec`` dump
    carries it and ``ArrayMetadata.to_dict()`` does not; everything else
    round-trips key for key through the template write.
    """
    return {
        k: v for k, v in json.loads(json.dumps(dict(raw), default=str)).items() if k != "node_type"
    }


def column_structure(fields: dict, *, node_order: int, resolutions: list) -> dict:
    """``{group: {array: metadata}}`` a column of this declaration MUST have.

    Derived from the SAME template machinery :func:`zagg.column.write_column` writes with
    (:func:`zagg.sweep_overview._overview_config` -> ``HealpixGrid.shard_spec``),
    so this is not a second description of the column's shape that could drift
    from the writer's — it IS the writer's, projected member by member.

    :func:`column_is_current` compares it against the stored column because
    the recorded ``zagg_column`` attrs cannot say either half of it (review
    finding, issue #520):

    - the **member set** — :func:`zagg.column._column_provenance` records neither
      ``location`` nor ``temporal``, yet
      :func:`zagg.sweep_overview.field_companions` reads exactly those and each
      one adds a ``{field}_locations`` / ``{field}_times`` member to EVERY
      resolution group (spec §4.6, "plus every channel sibling that field's
      §4.5 entry declares"). A digest field re-declared with a ``location``
      channel — ruling 4 on issue #410, the expected ``/1`` -> ``/2`` retrofit
      — is invisible to the provenance compare;
    - each member's **dtype, fill value and array attrs** — an exact field's
      ``dtype`` and ``fill_value`` are absent from the recorded provenance by
      construction (:func:`zagg.sweep_overview._field_provenance` records
      ``class``/``method``/``nan_policy`` and nothing else for that class),
      yet ``fill_value`` is what :func:`zagg.sweep_overview.fold_dense`
      reduces to and ``dtype`` is the stored element type. Moving either moves
      the column's bytes.
    """
    from zagg.grids.healpix import HealpixGrid
    from zagg.sweep_overview import _overview_config

    cfg = _overview_config(composable_fields(fields))
    return {
        str(int(res)): {
            name: _member_metadata(member.model_dump())
            for name, member in HealpixGrid(int(node_order), int(res), config=cfg, sharded=True)
            .shard_spec()
            .members.items()
        }
        for res in resolutions
    }


def stored_column_structure(group) -> dict:
    """:func:`column_structure`'s twin, read off an OPEN column root group.

    One member listing per resolution group, and the metadata each listing
    already carries — the cheapest read that can see the structure, and the
    whole cost the term adds to a skip.
    """
    return {
        str(name): {
            member: _member_metadata(arr.metadata.to_dict()) for member, arr in sub.arrays()
        }
        for name, sub in group.groups()
    }


def column_is_current(
    leaf_stamp,
    column_stamp,
    column_attrs,
    structure,
    *,
    node_order,
    cell_order,
    resolutions,
    fields,
    template=None,
) -> tuple[bool, str]:
    """Is this ``(leaf, window)``'s stored column current? ``(verdict, reason)``.

    The backfill's skip-if-current gate (issue #520; the #397/#417 discipline
    read off the artifacts, since a column records no ``generation`` block of
    its own — it has exactly one source, its leaf). Five terms, in cost order:

    1. **Committed** — neither stamp may be missing. An unstamped COLUMN is
       an interrupted writer's prefix; an unstamped LEAF is D4 debris whose
       column can be current only by accident. ``hive.read_commit`` returns
       ``None`` for both, so both are taken by value and neither is
       dereferenced before the guard (review finding, issue #520).
    2. **Declaration** — the recorded ``zagg_column`` block's node/cell orders,
       group set, and per-field provenance (:func:`zagg.column._column_provenance`, which
       carries the fold law, the digest budget and the §3.3 linkage) must be
       the ones this run would write. A narrowed, widened or re-classed
       declaration is exactly the #383 case where the artifact must not
       outlive the declaration that made it.
    3. **Structure** — the column's REALIZED arrays
       (:func:`stored_column_structure`) must be the ones the template this
       run would write declares (:func:`column_structure`), member for member
       and metadata for metadata. ``template`` is that projection, passed in
       by a PASS that derived it ONCE for its whole work set (it is a function
       of the declaration alone, so it cannot move inside a pass); ``None``
       recomputes it, which is the direct-call form. This is the term that covers what the
       recorded grammar cannot say, and there are two such things: ``location``
       / ``temporal`` are not in :func:`zagg.column._column_provenance`, so a digest field
       re-declared with a companion channel passes term 2 while its stored
       column is a sibling short in every group; and an exact field's
       ``dtype`` / ``fill_value`` are not there either, so a re-declaration
       that moves the element type or what folds to missing passes it too
       (review finding, issue #520). ``structure`` is what the caller read off
       the store; an unreadable one (``None``) is drift, which rewrites.
    4. **Order** — the column's ``written_at`` may not PRECEDE its leaf's: a
       leaf re-run after its column leaves the column folded from cells that
       are gone.
    5. **Granules** — the column's stamp copies the LEAF's ``granule_count``
       (§4.6), so a re-run that changed the leaf's granule set fails the gate
       even inside the one-second stamp resolution that defeats term 4.

    Between them terms 2 and 3 cover every key the fold and the template
    consume. What NO term covers is the leaf's BYTES: nothing here reads a
    cell, so a leaf whose arrays moved without moving its stamp or its
    granule count reads as current — terms 4 and 5's business, and their
    residual below.

    Residual, disclosed rather than papered over: both stamps resolve to whole
    seconds (the issue #417 term), and a column records no ``run_id`` for its
    source leaf, so a same-second leaf rewrite at an unchanged granule count
    reads as current. That is a narrower window than #417's — a backfill runs
    against a store the fleet is not writing: spec §4.6 names this pass the
    ONE sanctioned second writer and makes "no aggregation run in flight" an
    operator precondition of it (:mod:`zagg.column_backfill`, "Who may write a
    column") — and ``force=True`` is the unconditional rewrite.
    """
    if not isinstance(leaf_stamp, dict) or not isinstance(column_stamp, dict):
        return False, "absent-or-unstamped"
    block = column_attrs if isinstance(column_attrs, dict) else {}
    expected = {
        "spec": COLUMN_SPEC,
        "order": int(node_order),
        "source_cell_order": int(cell_order),
        "groups": sorted(int(r) for r in resolutions),
        "fields": {n: _column_provenance(m) for n, m in composable_fields(fields).items()},
    }
    recorded = {
        "spec": block.get("spec"),
        "order": block.get("order"),
        "source_cell_order": block.get("source_cell_order"),
        "groups": sorted(int(r) for r in (block.get("groups") or {})),
        "fields": block.get("fields") or {},
    }
    # Round-tripped JSON on one side, freshly derived Python on the other
    # (``inner_shape`` is a list either way, but an int-keyed group map is not):
    # compare canonical JSON so a type that survives ``json.loads`` unchanged
    # cannot read as drift and re-fold the whole store for nothing.
    if json.dumps(recorded, sort_keys=True) != json.dumps(expected, sort_keys=True):
        return False, "declaration-drift"
    wanted = (
        column_structure(fields, node_order=node_order, resolutions=resolutions)
        if template is None
        else template
    )
    if json.dumps(structure or {}, sort_keys=True) != json.dumps(wanted, sort_keys=True):
        return False, "structure-drift"
    leaf_at, column_at = leaf_stamp.get("written_at"), column_stamp.get("written_at")
    if leaf_at is None or column_at is None or str(column_at) < str(leaf_at):
        return False, "stale"
    if int(column_stamp.get("granule_count") or 0) != int(leaf_stamp.get("granule_count") or 0):
        return False, "granule-drift"
    return True, "current"


#: The one instruction every :func:`manifest_column_plan` refusal ends on: a
#: backfill upgrades a store somebody else declared, so a malformed or absent
#: declaration is always the operator's to fix, never the pass's to guess at.
_RE_DECLARE = (
    "RE-DECLARE FIRST: `python -m zagg.sweep <root> --declare-pyramid <config.yaml> "
    "--overviews <chunk order>`, then re-run the backfill"
)


class ColumnPlan(NamedTuple):
    """What one store's manifest says every leaf column must be (issue #520)."""

    node_order: int
    cell_order: int
    resolutions: list
    fields: dict


def manifest_column_plan(manifest) -> ColumnPlan:
    """The column recipe read off a STORE, not a config (issue #520).

    :func:`zagg.column.leaf_column_plan` is the worker's gate: it reads the config the
    unit carries, because workers never open the manifest (spec §4.6). A
    backfill has no such config — it upgrades a store somebody else built —
    so its gate is the manifest's own ``zagg-pyramid/2`` declaration, which
    is the reader- and sweep-facing contract for exactly this reason. The two
    gates read the same grammar and must agree: ``overviews`` (already the
    fully expanded ``(node, cells)`` list, §4.5) through
    :func:`zagg.column.column_resolutions`, and ``overview.fields`` through
    :func:`zagg.column.composable_fields`.

    Every refusal is BY NAME and ends on :data:`_RE_DECLARE`, never guesses.
    The three the ``/1`` -> ``/2`` upgrade actually meets are the point of the
    issue: a ``/1`` block (an ``orders``/``spacing`` schedule, or the empty
    ``orders`` that spells declared-off), and a ``/2`` block whose fields are
    all D24 ``class: "none"`` — the CA ATL03 store's 0.48 build, whose every
    t-digest field classified ``none``. Under the collapsed grammar those are
    declaration bugs a backfill must not paper over: it would write a
    morton-only column, or none at all, and publish it as an upgrade.

    "By name" holds for a MALFORMED block too, which a backfill meets as
    readily: a manifest is a store's own object and this pass is the first
    thing to read one nobody validated (review finding, issue #520). So the
    block, each ``overviews`` level and ``overview.fields`` are shape-checked
    before they are used — a bare ``[5, 4]`` (the un-normalized
    ``output.pyramid.overviews`` knob, the likeliest paste into a hand-edited
    block) refuses here rather than as a ``TypeError`` from
    :func:`zagg.column.column_resolutions` with no store path in it.
    """
    from zagg.pyramid import PYRAMID_SPEC_V2

    if not isinstance(manifest, dict) or any(
        manifest.get(k) is None for k in ("shard_order", "cell_order")
    ):
        raise ValueError(
            "column backfill needs a hive manifest declaring shard_order/cell_order; "
            "this is not one"
        )
    node_order, cell_order = int(manifest["shard_order"]), int(manifest["cell_order"])
    block = manifest.get("pyramid")
    if not isinstance(block, dict) or block.get("spec") != PYRAMID_SPEC_V2:
        raise ValueError(
            f"this store declares {(block.get('spec') if isinstance(block, dict) else block)!r}, "
            f"not {PYRAMID_SPEC_V2!r} — leaf columns exist only under the /2 grammar (spec "
            f"§4.6). {_RE_DECLARE}"
        )
    levels = block.get("overviews")
    if not isinstance(levels, list) or not levels:
        raise ValueError(
            f"the /2 pyramid block carries no `overviews` list — nothing declares which "
            f"resolutions a leaf column holds. {_RE_DECLARE}"
        )
    for level in levels:
        if not (isinstance(level, dict) and level.get("node") is not None) or not isinstance(
            level.get("cells"), list
        ):
            raise ValueError(
                f"the /2 pyramid block's `overviews` carries {level!r}, not an expanded "
                f"`{{'node': ..., 'cells': [...]}}` level — a bare order is the "
                f"`output.pyramid.overviews` KNOB, which `normalize_overviews` expands "
                f"before it reaches a manifest (spec §4.5). {_RE_DECLARE}"
            )
    resolutions = column_resolutions(levels, node_order)
    if not resolutions:
        raise ValueError(
            f"the declared overviews {levels!r} place no member at the shard order "
            f"{node_order}, so this declaration asks for no leaf column at all. "
            f"{_RE_DECLARE}"
        )
    overview = block.get("overview")
    declared = overview.get("fields") if isinstance(overview, dict) else overview
    declared = {} if declared is None else declared
    if not isinstance(declared, dict):
        raise ValueError(
            f"the /2 pyramid block's `overview.fields` is {declared!r}, not a declaration "
            f"map — nothing here says which fields a leaf column carries. {_RE_DECLARE}"
        )
    fields = composable_fields(declared)
    if not fields:
        raise ValueError(
            f"every declared field is D24 class `none` ({sorted(declared) or 'no fields'}) — "
            f"a column of none of them is no artifact at all (spec §4.6), so this store's "
            f"declaration, not its data, is what needs fixing. RE-DECLARE FIRST with the "
            f"config whose classifiers admit the fields, then re-run the backfill"
        )
    return ColumnPlan(node_order, cell_order, resolutions, fields)
