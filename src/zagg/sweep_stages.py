"""The staged sweep RUN: pass driver, finisher, and orchestration (issue #384).

:mod:`zagg.sweep_stage` owns one stage worker's machinery (the planner, the
fold kernels, the writers); this module owns a whole run of them:

- :func:`sweep_stage_pass` — every tuple, finest first, over one dirty set
  (the in-process driver; the lease heartbeat rides its ``on_stage`` seam);
- :func:`run_finisher` — the designated finisher-worker (espg ruling): the
  root singletons, exactly once, lease release last;
- :func:`run_stage_sweep` — admission (:mod:`zagg.sweep_lease`), discovery
  (listing-based — the run records via :func:`zagg.sweep.discover_leaves`;
  the root ``coverage.moc`` is an accelerator, never truth), ``partitions=``
  composition, the run record, and the post-fleet chaining seam
  (``output.sweep: "stages"``, opt-in per the recorded lean).

The CLI backstop is ``python -m zagg.sweep <root> --stages``
(:mod:`zagg.sweep` routes here). Raster hive stores are column-less by
construction and refuse at the ``/2``-declaration gate — issue #399's
reducer-keyed fold family joins this orchestration later, one schema.
"""

from __future__ import annotations

import json
import logging
import time

import numpy as np

from zagg.sweep_stage import (
    DEFAULT_TUPLE_WIDTH,
    _node_at,
    aggregate_actuals,
    ladder_entries,
    stage_node,
    stage_tuples,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scope planning (pure): the dispatch-side spellings and their composition.
# Lives with the RUN (its only consumer) so the worker module stays inside
# the section 4 module cap; the functions are pure and import nothing of the
# run machinery.
# ---------------------------------------------------------------------------


def normalize_scope(scope) -> np.ndarray | None:
    """An optional sweep scope, canonicalized to a morton-word MOC.

    ``None`` means whole store. Accepted spellings: an iterable of morton
    words (ints) or D1 decimal strings — a node-prefix set at any mix of
    orders — or a mapping (a shardmap: its KEYS are the prefixes, already
    ancestor-coarsened by construction; values are ignored). Returns sorted
    unique ``uint64`` words. Empty scopes refuse by name: an empty MOC means
    "sweep nothing", which is never what an operator typed on purpose.
    """
    from zagg.grids.morton import morton_word

    if scope is None:
        return None
    if isinstance(scope, dict):
        scope = list(scope.keys())
    words = [morton_word(s) if isinstance(s, str) else int(s) for s in scope]
    if not words:
        raise ValueError("an empty sweep scope selects nothing — pass None for whole-store")
    return np.unique(np.asarray(words, dtype=np.uint64))


def partition_words(partitions: int, index: int) -> np.ndarray:
    """One ``2^n`` partition as a MOC: its 12 order-``k`` subtree roots.

    The :mod:`zagg.sweep_partition` ownership predicate spelled as words, so
    ``partitions=`` composes with a scope by plain MOC intersection
    (:func:`compose_scope`). ``partitions=1`` returns the 12 base cells —
    the identity scope.
    """
    from zagg.grids.morton import morton_word
    from zagg.hive import _rank_tail
    from zagg.sweep_partition import normalize_partition, partition_split_order

    normalize_partition({"index": index, "of": partitions})
    k = partition_split_order(partitions)
    tail = _rank_tail(int(index), k)
    bases = [str(b) for b in range(1, 7)] + [f"-{b}" for b in range(1, 7)]
    return np.unique(np.asarray([morton_word(b + tail) for b in bases], dtype=np.uint64))


def compose_scope(scope: np.ndarray | None, partition: np.ndarray | None) -> np.ndarray | None:
    """Scope ∩ partition (either may be ``None`` — the identity)."""
    from mortie import moc_and

    if scope is None:
        return partition
    if partition is None:
        return scope
    # TODO(espg/mortie#173, espg/mortie PR 174): swap the per-pair scalar
    # ``moc_and`` for the batch ``mocs_and`` (1xN broadcast with the hoisted
    # shared operand) once a mortie release ships it; never depend on an
    # unreleased mortie.
    return moc_and(scope, partition)


def scope_admits(decimal: str, scope: np.ndarray | None) -> bool:
    """Whether a node's subtree intersects the scope MOC (``None`` admits all).

    Containment resolves in either direction (mortie's ``moc_and`` semantics
    over mixed-order covers), so a scope spelled as shard prefixes admits
    every ancestor node above them — "only ancestor prefixes of the touched
    shardmap are invoked" (#381 point (11)).
    """
    from mortie import moc_and

    from zagg.grids.morton import morton_word

    if scope is None:
        return True
    # TODO(espg/mortie#173, espg/mortie PR 174): batch ``mocs_and`` replaces
    # this per-node scalar call once a mortie release ships it.
    return moc_and(np.asarray([morton_word(decimal)], dtype=np.uint64), scope).size > 0


def sweep_stage_pass(
    store_root: str,
    manifest: dict,
    by_shard: dict,
    *,
    scope: np.ndarray | None = None,
    tuple_width: int = DEFAULT_TUPLE_WIDTH,
    run_id: str,
    run_started: str | None = None,
    store_kwargs: dict | None = None,
    on_stage=None,
    on_node=None,
    level_actuals: dict | None = None,
    only_dispatch: int | None = None,
) -> dict:
    """One staged pass over the dirty set: every tuple, finest first.

    The in-process driver the orchestration (phase 4) wraps with admission,
    discovery, the finisher and the run record. ``by_shard`` is the
    normalized dirty set (``{shard_decimal: {window, ...}}``); candidate
    children per node come from it unioned with the root ``coverage.moc``
    (an ACCELERATOR — a stale root MOC degrades coverage of untouched
    siblings, recorded as ``root_moc_stale``, never discovery, which is
    listing-based upstream). ``scope`` (a normalized MOC) filters DISPATCH
    nodes; a dispatched worker folds all children on disk. ``on_stage`` is
    called after each tuple and ``on_node`` after each dispatch node — the
    lease heartbeat's seams: a tuple is unbounded work (the finest tuple of
    an o9 store is ~49k dispatch nodes), so a per-tuple-only beat would let
    the holder's own lease expire mid-tuple (review finding). Returns the
    pass summary with per-stage rows.

    ``only_dispatch`` restricts the pass to the ONE tuple dispatching at that
    order — what a fleet stage worker runs (issue #519). The in-process driver
    leaves it ``None`` and walks every tuple finest-first; the fleet transport
    walks the same tuples, one invoke fan-out per tuple, so the two differ in
    grouping alone (the merge-source law: grouping changes no bytes). An order
    that dispatches no tuple refuses BY NAME rather than sweeping nothing — a
    mistyped dispatch order must not read as a clean no-op.
    """
    from zagg.hive import _utcnow
    from zagg.store import open_object_store
    from zagg.sweep_overview import _candidate_decimals, _window_work

    store_kwargs = dict(store_kwargs or {})
    run_started = run_started or _utcnow()
    pyramid = manifest.get("pyramid") or {}
    shard_order = int(manifest["shard_order"])
    cell_order = int(manifest["cell_order"])
    levels = ladder_entries(pyramid, shard_order)
    # The schedule and the ``only_dispatch`` refusal come FIRST — before the
    # ``fields``/``candidates`` gates below, which both return a clean empty
    # summary. A mistyped dispatch order against a store with nothing to sweep
    # would otherwise read as a no-op and PUT a well-formed record with no
    # stages, which the dispatcher's barrier accepts (review finding). It is a
    # pure function of shard_order and tuple_width and needs nothing on disk.
    schedule = stage_tuples(shard_order, tuple_width=tuple_width)
    if only_dispatch is not None:
        picked = [t for t in schedule if int(t["dispatch"]) == int(only_dispatch)]
        if not picked:
            raise ValueError(
                f"no stage tuple dispatches at order {only_dispatch} (shard_order "
                f"{shard_order}, tuple_width {tuple_width}); the dispatch orders are "
                f"{[t['dispatch'] for t in schedule]}"
            )
        schedule = picked
    decl = pyramid.get("overview") if isinstance(pyramid.get("overview"), dict) else {}
    # THE shared admission predicate (:func:`zagg.column._is_composable`), not a
    # literal class list: this map goes to ``stage_node`` unfiltered and reaches
    # ``_merge_slabs``/``_gather_slabs`` directly, so a ``packed`` entry without
    # its ``of`` linkage would read ``group[None]`` inside ``_ColumnReader.read``
    # — a ``TypeError`` that guard does not catch, aborting the whole pass. Every
    # admission site routes through the one predicate (review finding).
    from zagg.column import _is_composable

    fields = {
        n: dict(m)
        for n, m in (decl.get("fields") or {}).items()
        if isinstance(m, dict) and _is_composable(m)
    }
    summary: dict = {"run_id": run_id, "tuple_width": int(tuple_width), "stages": []}
    if not fields:
        logger.info("stage sweep: no composable fields declared; nothing to generate")
        return summary
    windowed = manifest.get("temporal") is not None
    candidates, moc_stale = _candidate_decimals(store_root, shard_order, by_shard, store_kwargs)
    if moc_stale:
        summary["root_moc_stale"] = True
    if not candidates:
        return summary
    store = open_object_store(store_root, **store_kwargs)
    if level_actuals is None:
        level_actuals = {}  # callers may pass one to accumulate across passes
    from zagg.windows import SCHEDULE_NONE_TOKEN

    for stage in schedule:
        t0 = time.perf_counter()
        counts = {
            "written": 0,
            "current": 0,
            "empty": 0,
            "failed": 0,
            "under_covered": 0,
            "columns_written": 0,
            "columns_current": 0,
            "revalidated": 0,
        }
        nodes = sorted({_node_at(d, stage["dispatch"]) for d in candidates})
        nodes = [n for n in nodes if scope_admits(n, scope)]
        for node in nodes:
            dirty_windows: set = set()
            for d in by_shard:
                if d.startswith(node):
                    dirty_windows |= by_shard[d]
            from zagg.sweep_overview import _read_envelope

            envelope = _read_envelope(store, node)
            entries = dict((envelope or {}).get("windows") or {})
            for key, fold_windows in _window_work(decl, windowed, dirty_windows, entries):
                stage_node(
                    store,
                    store_root,
                    node,
                    stage,
                    levels,
                    fields,
                    key=key,
                    fold_windows=fold_windows,
                    all_time=bool(windowed and key == SCHEDULE_NONE_TOKEN),
                    windowed=windowed,
                    shard_order=shard_order,
                    cell_order=cell_order,
                    candidates=candidates,
                    run_id=run_id,
                    run_started=run_started,
                    counts=counts,
                    store_kwargs=store_kwargs,
                    level_actuals=level_actuals,
                )
            if on_node is not None:
                on_node(node)
        row = {
            "dispatch_order": stage["dispatch"],
            "orders": list(stage["orders"]),
            "nodes": len(nodes),
            **counts,
            "duration_s": time.perf_counter() - t0,
        }
        summary["stages"].append(row)
        if on_stage is not None:
            on_stage(row)
    summary["levels"] = {
        str(k): v for k, v in sorted(aggregate_actuals(level_actuals).items(), reverse=True)
    }
    return summary


# ---------------------------------------------------------------------------
# The designated finisher-worker — the root singletons, exactly once.
# ---------------------------------------------------------------------------


def run_finisher(
    store_root: str,
    manifest: dict,
    by_shard: dict,
    level_actuals: dict,  # the AGGREGATED per-level map (aggregate_actuals)
    *,
    run_id: str,
    store_kwargs: dict | None = None,
    release=None,
    touch_policy: str = "auto",
) -> dict:
    """The designated finisher-worker (espg ruling): root singletons, once.

    Fired by the orchestrator after the root tuple completes — never
    distributed across the 12 base cells (plural writers of singleton
    objects would breach the single-writer law). Owns, in order:

    1. the root ``coverage.moc`` refresh (GET-union-PUT, ``source:
       "sweep"``) — sequenced HERE, before any later scoped fan-out reads
       it: the #380 read-side obligation the partition machinery deferred;
    2. the manifest RMW nesting per-entry actuals inside the level entries
       of ``pyramid.overviews`` (#381 point (7); readers MUST tolerate the
       added key). The leaf entry records the ``leaf-column`` law
       (merges-from-raw 1); ladder entries record what this run observed —
       ``stage-gather`` at 1, ``stage-merge`` at 2, never 3 (gen 3 is
       append-later cascade territory only). This re-PUT also refreshes the
       manifest's ``LastModified``, satisfying the PR #397 lifecycle
       root-touch for ``morton_hive.json`` (and step 1 for the root MOC)
       WITHOUT duplicating it — only ``aggregation.yaml`` still needs the
       explicit touch, step 3. The family dict's order-keyed
       ``materialized`` is deliberately NOT written on ``/2`` stores: the
       per-entry actuals are the one source of truth (the flatten-ruling
       principle; any /1-era inventory is preserved verbatim);
    3. the ``aggregation.yaml`` lifecycle touch (issue #388's machinery);
    4. lease release via ``release()`` — the FINAL act, so a finisher that
       failed midway leaves the lease held and the run claimable/idempotent
       (re-invoking re-runs steps 1-3 harmlessly).

    Failures in steps 1-2 RAISE (the orchestrator records the incomplete
    finish and leaves the lease for recovery); step 3 is fail-open telemetry.
    """

    from zagg.grids.morton import morton_word
    from zagg.hive import (
        AGGREGATION_CORE_NAME,
        MANIFEST_NAME,
        _utcnow,
        build_root_coverage,
        read_manifest,
        write_root_coverage,
    )
    from zagg.lifecycle import touch_unit_footprint
    from zagg.store import open_object_store, put_object

    store_kwargs = dict(store_kwargs or {})
    out = {
        "root_moc": False,
        "manifest_updated": False,
        "objects_touched": 0,
        "touch_failures": 0,
        "lease_released": False,
    }
    shard_order = int(manifest["shard_order"])
    if by_shard:
        envelope = build_root_coverage(
            [morton_word(d) for d in by_shard], shard_order, source="sweep"
        )
        write_root_coverage(store_root, envelope, **store_kwargs)
        out["root_moc"] = True
    fresh = read_manifest(store_root, **store_kwargs)
    if fresh is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — cannot record actuals")
    now = _utcnow()
    changed = False
    for entry in (fresh.get("pyramid") or {}).get("overviews") or []:
        node = int(entry.get("node", -1))
        if node == shard_order and level_actuals:
            actuals = {"regime": "leaf-column", "merges_from_raw": 1, "generated_at": now}
        elif node in level_actuals:
            a = level_actuals[node]
            actuals = {
                "regime": a["regime"],
                "merges_from_raw": int(a["merges_from_raw"]),
                "source_children": dict(a["source_children"]),
                "run_id": run_id,
                "generated_at": now,
            }
        else:
            continue
        if entry.get("actuals") != actuals:
            entry["actuals"] = actuals
            changed = True
    if changed or level_actuals:
        put_object(
            open_object_store(store_root, **store_kwargs),
            MANIFEST_NAME,
            json.dumps(fresh, indent=1).encode(),
        )
        out["manifest_updated"] = True
    # ``touch_policy`` is the issue #501 declaration (``output.touch``), threaded
    # in from whichever caller holds the config: the post-run chaining path does
    # (``runner`` reads ``config`` on the line it decides to chain from), the
    # ``python -m zagg.sweep --stages`` CLI does not — a sweep invoked there is
    # driven by the store's own manifest, which carries no policy. Hence the
    # ``auto`` default: the issue #495 phase 4 inference, unchanged for the CLI.
    touch = touch_unit_footprint(
        [],
        [f"{str(store_root).rstrip('/')}/{AGGREGATION_CORE_NAME}"],
        store_kwargs=store_kwargs,
        policy=touch_policy,
    )
    out["objects_touched"] = touch["touched"]
    out["touch_failures"] = touch["failed"]
    # A published target is not applicable rather than touched or failed
    # (issue #495 phase 4); absent when zero, so an unpublished sweep record
    # keeps exactly the key set it had.
    if touch.get("skipped_paths"):
        out["touch_skipped_paths"] = touch["skipped_paths"]
    if release is not None:
        out["lease_released"] = bool(release())
    return out


# ---------------------------------------------------------------------------
# Phase 4: dispatch orchestration — admission, discovery, record, chaining.
# ---------------------------------------------------------------------------


def run_stage_sweep(
    store_root: str,
    leaves=None,
    *,
    scope=None,
    tuple_width: int = DEFAULT_TUPLE_WIDTH,
    partitions: int | None = None,
    store_kwargs: dict | None = None,
    record: bool = True,
    run_id: str | None = None,
    lease_ttl_s: int | None = None,
    touch_policy: str = "auto",
) -> dict:
    """One admitted staged sweep, end to end: lease -> stages -> finisher.

    ``leaves`` is the dirty ``(shard_key, window)`` work set; ``None`` means
    UNSCOPED DISCOVERY, which is listing-based by the espg ruling — the run
    records (:func:`zagg.sweep.discover_leaves`), never the root
    ``coverage.moc`` (a fleet append with no subsequent sweep leaves the
    root MOC stale, and the ratchet only heals nodes a sweep visits; the MOC
    stays an in-pass accelerator for sibling candidates only).

    ``scope`` is the optional node-prefix MOC (#381 point (11) — decimals,
    words, or a shardmap whose keys are the prefixes); ``partitions=``
    composes by intersection, each of the ``2^n`` partitions swept in turn
    UNDER THE SAME LEASE (the lease is store-granular by correctness: scope
    disjointness does not imply write disjointness). Admission is the
    :mod:`zagg.sweep_lease` conditional PUT — a live foreign intent raises
    :class:`zagg.sweep_lease.SweepRefusedError` naming the runner; an expired
    heartbeat is claimed and the claimant simply completes the partial prior
    run (the ratchet's ordinary posture). The lease heartbeats after every
    stage; the finisher releases it as its final act. On ANY failure the
    lease is deliberately left held — the run record says what happened, and
    the intent expires into claimability rather than admitting a sibling
    into a half-written store.

    The summary is PUT at the store root as the run record
    (``sweep_stats_{ts}_stages.json``) with the lease intent/completion, the
    per-stage rows, the per-level actuals, and the finisher counts
    (``objects_touched``/``touch_failures`` — the PR #397 shape).

    ``touch_policy`` is the caller's ``output.touch`` declaration (issue #501),
    forwarded to the finisher's ``aggregation.yaml`` touch — the one object a
    staged sweep refreshes. ``auto`` (the default) is the issue #495 phase 4
    inference, so the CLI entry point, which has no config to read it from, is
    unchanged.
    """
    import uuid
    from datetime import datetime, timezone

    from zagg.hive import MANIFEST_NAME, _utcnow, read_manifest
    from zagg.sweep import _normalize_leaves, discover_leaves
    from zagg.sweep_lease import (
        DEFAULT_TTL_S,
        acquire_lease,
        heartbeat_lease,
        release_lease,
    )

    t0 = time.perf_counter()
    store_kwargs = dict(store_kwargs or {})
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    shard_order = int(manifest["shard_order"])
    ladder_entries(manifest.get("pyramid") or {}, shard_order)  # loud /2 gate
    run_id = run_id or (
        f"stage-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    )
    if leaves is None:
        leaves = discover_leaves(store_root, store_kwargs=store_kwargs)
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    scope_words = normalize_scope(scope)
    lease = acquire_lease(
        store_root,
        run_id=run_id,
        scope=None if scope_words is None else [int(w) for w in scope_words],
        ttl_s=int(lease_ttl_s or DEFAULT_TTL_S),
        store_kwargs=store_kwargs,
    )
    run_started = _utcnow()
    # Heartbeat throttled by wall clock, fired per dispatch node AND per
    # tuple: a beat must land well inside the TTL however long a tuple (or
    # one node's fold) runs, without one PUT per node on a large store.
    ttl_s = int(lease_ttl_s or DEFAULT_TTL_S)
    last_beat = [time.monotonic()]

    def _maybe_beat(*_args):
        if time.monotonic() - last_beat[0] >= ttl_s / 3:
            heartbeat_lease(store_root, lease, store_kwargs=store_kwargs)
            last_beat[0] = time.monotonic()

    summary: dict = {
        "run_id": run_id,
        "store_root": store_root,
        "shard_order": shard_order,
        "tuple_width": int(tuple_width),
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "scope": None if scope_words is None else [str(int(w)) for w in scope_words],
        "partitions": partitions,
        "lease": {
            "acquired_at": lease.get("acquired_at"),
            "claimed_from": lease.get("claimed_from"),
            "ttl_s": lease.get("ttl_s"),
            "released": False,
        },
        "stages": [],
        "levels": {},
    }
    level_actuals: dict = {}
    try:
        indexes = [None] if not partitions or partitions == 1 else list(range(int(partitions)))
        for index in indexes:
            part_scope = (
                scope_words
                if index is None
                else compose_scope(scope_words, partition_words(int(partitions), index))
            )
            if part_scope is not None and part_scope.size == 0:
                continue  # scope ∩ partition selects nothing
            part = sweep_stage_pass(
                store_root,
                manifest,
                by_shard,
                scope=part_scope,
                tuple_width=tuple_width,
                run_id=run_id,
                run_started=run_started,
                store_kwargs=store_kwargs,
                on_stage=lambda row: heartbeat_lease(store_root, lease, store_kwargs=store_kwargs),
                on_node=_maybe_beat,
                level_actuals=level_actuals,
            )
            rows = part["stages"]
            if index is not None:
                for row in rows:
                    row["partition"] = {"index": index, "of": int(partitions)}
            summary["stages"].extend(rows)
            if part.get("root_moc_stale"):
                summary["root_moc_stale"] = True
        aggregated = aggregate_actuals(level_actuals)
        summary["levels"] = {str(k): v for k, v in sorted(aggregated.items(), reverse=True)}
        _maybe_beat()  # the finisher's RMW must not start on a stale beat
        summary["finisher"] = run_finisher(
            store_root,
            manifest,
            by_shard,
            aggregated,
            run_id=run_id,
            store_kwargs=store_kwargs,
            release=lambda: release_lease(store_root, run_id=run_id, store_kwargs=store_kwargs),
            touch_policy=touch_policy,
        )
        summary["lease"]["released"] = bool(summary["finisher"].get("lease_released"))
    except BaseException as e:
        # The lease stays HELD: it expires into claimability, and a sibling
        # admitted now would write into a half-swept store. Record and raise.
        summary["error"] = f"{type(e).__name__}: {e}"
        summary["duration_s"] = time.perf_counter() - t0
        if record:
            summary["record"] = _write_stage_record(store_root, summary, store_kwargs)
        raise
    summary["duration_s"] = time.perf_counter() - t0
    if record:
        summary["record"] = _write_stage_record(store_root, summary, store_kwargs)
    return summary


def _write_stage_record(store_root: str, summary: dict, store_kwargs: dict) -> str | None:
    """PUT the staged run record at the store root; its key, or ``None``.

    ``sweep_stats_{ts}_stages.json`` — the :func:`zagg.sweep._write_sweep_record`
    naming family (timestamp-first, outside the ``stats_*.parquet`` glob),
    with a ``_stages`` tag so a staged run never reads as a families pass.
    The rows extend the settled #397 record shape: the lease block is the
    intent/completion row, per-stage rows carry written/current/failed/
    under-coverage, and the finisher block carries
    ``objects_touched``/``touch_failures``. Fail-open (telemetry, D9).
    """
    from datetime import datetime, timezone

    from zagg.store import open_object_store, put_object
    from zagg.sweep import SWEEP_SPEC

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"sweep_stats_{ts}_stages.json"
    try:
        put_object(
            open_object_store(store_root, **store_kwargs),
            key,
            json.dumps({"spec": SWEEP_SPEC, "mode": "stages", **summary}, indent=1).encode(),
        )
    except Exception as e:
        logger.warning(f"stage sweep: run record write failed (fail-open, D9): {e}")
        return None
    return key


def stage_sweep_after_run(
    store_root: str, leaves, *, store_kwargs: dict | None = None, touch_policy: str = "auto"
):
    """Post-fleet chaining: the ``output.sweep: "stages"`` opt-in, fail-open.

    The dispatcher fires the staged sweep immediately after the fleet lands,
    auto-scoped to the fleet's own footprint (#381 point (11): the run's
    shard set IS the touched shardmap, and scope spelled as shard prefixes
    invokes exactly their ancestor chain). Fail-open like
    :func:`zagg.sweep.sweep_after_run` — every stage artifact is regenerable
    (D9), so a refused lease or a failed stage costs one later
    ``python -m zagg.sweep --stages`` pass, never a wrong answer. This is the
    LOCAL dispatcher's chaining: it is also the worker, so D8 does not
    constrain it. The Lambda dispatcher's twin is
    :func:`zagg.sweep_fleet.run_stage_sweep_fleet` (issue #519) — same tuple
    ordering, every write moved into ``mode="sweep"`` stage invokes.

    ``touch_policy`` is the run's ``output.touch`` (issue #501): the dispatcher
    reads the config to decide to chain at all, so it also passes the policy
    that governs the finisher's touch — an operator who declared ``never`` on an
    archival destination must not get one new root-core version per staged sweep.
    """
    from zagg.grids.morton import morton_decimal

    try:
        scope = sorted({morton_decimal(int(k)) for k, _w in (tuple(r) for r in leaves)})
        if not scope:
            return None
        summary = run_stage_sweep(
            store_root, leaves, scope=scope, store_kwargs=store_kwargs, touch_policy=touch_policy
        )
        logger.info(
            f"Post-run staged sweep: {[(s['dispatch_order'], s['written']) for s in summary['stages']]}"
        )
        return summary
    except Exception as e:
        logger.warning(f"post-run staged sweep failed (fail-open, D9 — regenerable): {e}")
        return None


# ---------------------------------------------------------------------------
# The FLEET worker arm (issue #519): one Lambda invoke's share of a staged
# sweep. :func:`run_stage_sweep` above is the same run done in one process;
# these two entry points are what a ``mode="sweep"`` event's ``stage`` block
# reaches, and the dispatcher that fans them out lives in
# :mod:`zagg.sweep_fleet` (D8: it invokes and polls, it never writes).
# ---------------------------------------------------------------------------

#: Envelope version of a stage invoke's record — the fleet's soft-barrier
#: signal (the dispatcher polls for it) AND its aggregation channel (the
#: finisher reads the run's records to rebuild the per-level actuals the
#: in-process driver accumulates in memory).
STAGE_RECORD_SPEC = "zagg-sweep-stage-record/1"

#: The finisher's own record basename — the last object of a fleet staged run.
FINISHER_RECORD_NAME = "finisher.json"


def stage_record_name(dispatch: int, batch: int) -> str:
    """One stage invoke's record basename: ``stage-<dispatch>-<batch>.json``.

    Zero-padded so a plain lexicographic listing reads finest-tuple-last in
    dispatch order, and deterministic from ``(dispatch, batch)`` alone — the
    dispatcher names every object it will poll for before it fires a single
    invoke, so the soft barrier needs no listing and no worker response.
    """
    return f"stage-{int(dispatch):02d}-{int(batch):04d}.json"


def _jsonable_actuals(level_actuals: dict) -> dict:
    """The raw per-artifact actuals map as JSON (int level keys -> strings)."""
    return {
        str(int(k)): {
            "cells": int(v["cells"]),
            "regime": v["regime"],
            "merges_from_raw": int(v["merges_from_raw"]),
            "children": {str(t): dict(row) for t, row in v["children"].items()},
        }
        for k, v in level_actuals.items()
    }


def merge_level_actuals(target: dict, incoming: dict) -> dict:
    """Fold one worker's raw actuals into the run's, in place; the target.

    The fleet's answer to the in-process ``level_actuals`` dict. Rows are
    keyed per ``(artifact node, window)`` and ASSIGNED, never added
    (:func:`zagg.sweep_stage._accumulate_actuals`), so merging across workers
    is a plain dict update: a coarse ancestor two batches both visited
    contributes its row ONCE, exactly as a partitioned in-process run
    re-visiting it does. ``cells``/``regime``/``merges_from_raw`` are
    first-wins per level, mirroring the in-process ``setdefault`` — they are
    derived per level (:func:`zagg.sweep_stage.classify_level`), so every
    worker computes the same values and the tie never has to be broken.
    """
    for k, entry in (incoming or {}).items():
        cur = target.setdefault(
            int(k),
            {
                "cells": int(entry["cells"]),
                "regime": entry["regime"],
                "merges_from_raw": int(entry["merges_from_raw"]),
                "children": {},
            },
        )
        for node_window, row in (entry.get("children") or {}).items():
            cur["children"][str(node_window)] = {
                name: int(row.get(name) or 0) for name in ("folded", "missing", "unreadable")
            }
    return target


def read_stage_records(records_from: str, *, run_id: str, store_kwargs: dict | None = None) -> list:
    """One run's stage records under its status prefix, sorted by object name.

    Immediate children only (the ``rows_from_status`` precedent — the layout
    is flat), and the finisher's own record is excluded: it is written after
    this read, by the caller. Unparsable objects are skipped with a warning
    rather than aborting the finish; the resulting actuals under-report, which
    is exactly the recorded-and-healed under-coverage posture (#381 point (6)).

    Records carrying another ``run_id`` are skipped. The prefix is run-scoped
    (``run_status_prefix``), but object NAMES are ``stage-<dispatch>-<batch>``
    and the batching is a dispatcher choice, not a store fact: a resumed run
    under the same id with fewer or differently-numbered batches leaves the
    dead attempt's higher-numbered records in place, and since
    :func:`merge_level_actuals` ASSIGNS in sorted-name order those stale rows
    would win the merge and land in the manifest permanently.
    """
    import obstore

    from zagg.store import open_object_store

    store = open_object_store(records_from, **dict(store_kwargs or {}))
    listing = obstore.list_with_delimiter(store)
    keys = sorted(
        meta["path"].rsplit("/", 1)[-1]
        for meta in listing["objects"]
        if meta["path"].rsplit("/", 1)[-1].startswith("stage-") and meta["path"].endswith(".json")
    )
    out = []
    for key in keys:
        try:
            record = json.loads(bytes(obstore.get(store, key).bytes()))
        except Exception as e:
            logger.warning(f"stage sweep: unreadable stage record {key} ({e}); skipping")
            continue
        if not (isinstance(record, dict) and record.get("spec") == STAGE_RECORD_SPEC):
            logger.warning(f"stage sweep: {key} is not a {STAGE_RECORD_SPEC} record; skipping")
            continue
        if record.get("run_id") != run_id:
            logger.warning(
                f"stage sweep: {key} belongs to run {record.get('run_id')!r}, not "
                f"{run_id!r} (a prior attempt's debris under this prefix); skipping"
            )
            continue
        out.append(record)
    return out


def _put_stage_record(records_from: str, name: str, record: dict, store_kwargs: dict) -> str:
    """PUT one stage record under the run's status prefix; its full URL.

    Deliberately NOT fail-open, unlike the store-root run record: this object
    is the transport, not telemetry about it. The dispatcher's soft barrier
    polls for it and the finisher rebuilds the manifest's per-level actuals
    from it, so swallowing a failed PUT would silently under-report coverage
    in the one place #381 point (7) exists to record it. The artifacts are
    already written and every stage is idempotent (skip-if-current), so the
    cost of raising is one re-invoke, not one re-fold.

    Routed through :func:`zagg.store.put_object`, never raw ``obstore.put``
    (issue #522): the record lands under the run's status prefix, which on a
    published store sits inside the same grant as the store itself, so a write
    that skipped the canned-ACL handle would publish an object Source
    Cooperative cannot manage — and say nothing about it.
    """
    from zagg.store import open_object_store, put_object

    put_object(
        open_object_store(records_from, **store_kwargs),
        name,
        json.dumps(record, indent=1).encode(),
    )
    return f"{records_from.rstrip('/')}/{name}"


def run_stage_worker(
    store_root: str,
    leaves,
    *,
    run_id: str,
    run_started: str,
    dispatch: int,
    nodes,
    batch: int = 0,
    tuple_width: int = DEFAULT_TUPLE_WIDTH,
    partition: dict | None = None,
    records_from: str,
    lease_ttl_s: int | None = None,
    store_kwargs: dict | None = None,
) -> dict:
    """One fleet stage worker: this invoke's dispatch nodes, one tuple.

    The worker half of the issue #519 transport. Everything the in-process
    driver does per tuple happens here — lease admission, the run-id skip
    keys, the :class:`zagg.sweep_stage.ForeignSweepError` backstop, the fold,
    every store write (D8 intact: the dispatcher only invokes and polls) —
    restricted to ``nodes`` (this invoke's share of the tuple's dispatch
    nodes, spelled as decimals) at the ``dispatch`` order.

    ``nodes`` reaches the pass as the ordinary ``scope`` MOC, so a worker
    folds exactly the dispatch nodes it was handed and no others. Dispatch
    nodes at one order own disjoint subtrees and a tuple's folds read only
    columns one tuple FINER, so the split across invokes is free of
    cross-worker dependencies — the same disjointness the in-process pass
    relies on, which is why the merge-source law makes the fleet build
    byte-identical to the CLI build.

    The node set is validated BY NAME before anything is read or written: it
    must be non-empty, and every entry must sit at exactly the ``dispatch``
    order. Neither check is decoration. An empty set would reach the pass as
    ``scope=None`` — whole store — and a node coarser or finer than
    ``dispatch`` over-claims its ancestor's subtree, because
    :func:`scope_admits` resolves containment in BOTH directions. Either way
    this invoke silently widens its share, and two same-run siblings then
    double-write the same objects — which neither the lease nor the
    foreign-fresh stamp catches, since both are same-run-permissive by design
    (that IS how a fan-out's siblings are admitted). A bad node set has to be
    loud here or it is never loud at all.

    Admission is the ordinary per-store lease. Every worker of a run calls
    :func:`zagg.sweep_lease.acquire_lease` with the SAME run id, so the first
    one creates the intent and the rest read their own back (the idempotent
    re-admission); a live FOREIGN intent refuses this invoke by name. The
    lease scope is left ``None`` deliberately: the lease is store-granular by
    correctness, and recording one batch's node list would record whichever
    worker happened to win the create race. Nobody releases it here — release
    is the finisher's final act (:func:`run_stage_finisher`), so a run that
    dies mid-fan-out leaves a claimable intent, not an open store.

    ``records_from`` is the run's status prefix and is REQUIRED: the record
    lands there under :func:`stage_record_name` and is both the dispatcher's
    soft-barrier signal and the finisher's aggregation input. A worker that
    wrote no record would fold correctly and then be indistinguishable from a
    lost invoke — its barrier waits out the full timeout and its coverage
    never reaches the manifest. That is a worse outcome than a loud refusal,
    so an absent prefix refuses BY NAME before anything is read or written
    (review finding). Returns the record.
    """
    from zagg.hive import MANIFEST_NAME, _decimal_order, read_manifest
    from zagg.sweep import _normalize_leaves
    from zagg.sweep_lease import DEFAULT_TTL_S, acquire_lease, heartbeat_lease

    t0 = time.perf_counter()
    store_kwargs = dict(store_kwargs or {})
    if not records_from:
        raise ValueError(
            f"stage invoke for run {run_id!r} (dispatch {dispatch}, batch {batch}) was "
            "handed no records_from — a worker whose record nobody can witness reads to "
            "the dispatcher exactly like a lost invoke, and its coverage never reaches "
            "the manifest; refusing rather than folding invisibly"
        )
    nodes = [str(n) for n in nodes]
    if not nodes:
        raise ValueError(
            f"stage invoke (run {run_id!r}, dispatch order {dispatch}, batch {batch}) was "
            "handed an empty node set — a stage worker folds ITS share of a tuple; an "
            "empty set would sweep the whole store as a same-run sibling of every other "
            "worker, which admission cannot catch"
        )
    misordered = sorted({n for n in nodes if _decimal_order(n) != int(dispatch)})
    if misordered:
        raise ValueError(
            f"stage invoke (run {run_id!r}, dispatch order {dispatch}, batch {batch}) was "
            f"handed nodes that are not at order {dispatch}: {misordered} (orders "
            f"{sorted({_decimal_order(n) for n in misordered})}) — containment resolves "
            "both ways, so an off-order node over-claims its ancestor's whole subtree"
        )
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    shard_order = int(manifest["shard_order"])
    ladder_entries(manifest.get("pyramid") or {}, shard_order)  # loud /2 gate
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    scope = normalize_scope(nodes)
    ttl_s = int(lease_ttl_s or DEFAULT_TTL_S)
    lease = acquire_lease(
        store_root, run_id=run_id, scope=None, ttl_s=ttl_s, store_kwargs=store_kwargs
    )
    last_beat = [time.monotonic()]

    def _maybe_beat(*_args):
        if time.monotonic() - last_beat[0] >= ttl_s / 3:
            heartbeat_lease(store_root, lease, store_kwargs=store_kwargs)
            last_beat[0] = time.monotonic()

    level_actuals: dict = {}
    summary = sweep_stage_pass(
        store_root,
        manifest,
        by_shard,
        scope=scope,
        tuple_width=tuple_width,
        run_id=run_id,
        run_started=run_started,
        store_kwargs=store_kwargs,
        on_stage=_maybe_beat,
        on_node=_maybe_beat,
        level_actuals=level_actuals,
        only_dispatch=int(dispatch),
    )
    rows = summary["stages"]
    if partition is not None:
        for row in rows:
            row["partition"] = dict(partition)
    record = {
        "spec": STAGE_RECORD_SPEC,
        "role": "stage",
        "run_id": run_id,
        "run_started": run_started,
        "dispatch": int(dispatch),
        "batch": int(batch),
        "tuple_width": int(tuple_width),
        "n_nodes": len(nodes),
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "partition": None if partition is None else dict(partition),
        "stages": rows,
        "level_actuals": _jsonable_actuals(level_actuals),
        "duration_s": time.perf_counter() - t0,
    }
    if summary.get("root_moc_stale"):
        record["root_moc_stale"] = True
    record["record"] = _put_stage_record(
        records_from, stage_record_name(dispatch, batch), record, store_kwargs
    )
    return record


def run_stage_finisher(
    store_root: str,
    leaves,
    *,
    run_id: str,
    records_from: str | None = None,
    touch_policy: str = "auto",
    barrier_timed_out: bool = False,
    lease_ttl_s: int | None = None,
    store_kwargs: dict | None = None,
    record: bool = True,
) -> dict:
    """The fleet's finisher invoke: aggregate the run's records, then finish.

    The counterpart to the tail of :func:`run_stage_sweep`. It rebuilds the
    per-level actuals the in-process driver carries in memory by reading the
    run's stage records (:func:`read_stage_records`) instead — which is why
    the dispatcher never has to ship them and never has to hold them — then
    runs the ordinary :func:`run_finisher` (root MOC, manifest actuals,
    ``aggregation.yaml`` touch, lease release LAST), writes the store-root
    run record, and finally its own :data:`FINISHER_RECORD_NAME` record: the
    one object that says a fleet staged run completed, and the last thing the
    dispatcher polls for.

    ``barrier_timed_out`` is the dispatcher's verdict on its own soft barrier,
    threaded in so the RUN RECORD says the per-level actuals may be short — see
    where it is recorded below.

    ``records_from`` is REQUIRED and the prefix must list at least one stage
    record; both refuse by name. A finisher only ever fires after a fan-out,
    so no prefix (or a prefix listing nothing — the wrong bucket, a role that
    cannot list it, every stage invoke lost) means the fan-out itself was
    lost, and stamping a manifest with NO actuals on that basis is precisely
    the silent under-reporting :func:`_put_stage_record` refuses to fail open
    on. A PARTIAL set stays fine: fewer records than batches under-reports
    coverage, which is recorded and self-heals on the next run (#381 point
    (6)); only zero is indistinguishable from "nothing ran".

    Admission is the ordinary per-store lease, acquired as this invoke's FIRST
    act — the finisher is the one invoke that touches the store-root
    singletons (the root ``coverage.moc`` and the manifest RMW), so it is the
    last place that may run unadmitted. A fan-out over thousands of dispatch
    nodes can outlive the TTL between the final stage worker's beat and this
    invoke, which is exactly the window a foreign sweep may claim; the call is
    idempotent re-admission when the run still holds its own intent, and
    refuses BY NAME (:class:`zagg.sweep_lease.SweepRefusedError`) when a live
    foreign one holds the store, rather than doing the RMW alongside the
    claimant's own finisher and reporting ``lease.released: False`` as a clean
    finish.

    Ordering is the local ordering. The lease is released inside
    :func:`run_finisher`, before the two records land, exactly as the CLI path
    releases it before ``_write_stage_record``: the records are the run's
    telemetry, not part of its admission. On ANY failure it is deliberately
    left HELD (:func:`run_stage_sweep`'s posture): a half-finished store
    expires into claimability rather than admitting a sibling now.
    """
    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.sweep import _normalize_leaves
    from zagg.sweep_lease import DEFAULT_TTL_S, acquire_lease, release_lease

    t0 = time.perf_counter()
    store_kwargs = dict(store_kwargs or {})
    if not records_from:
        raise ValueError(
            f"the finisher for run {run_id!r} was handed no records_from — the run's "
            "stage records ARE its per-level actuals; finishing without them would "
            "stamp the manifest with no coverage at all"
        )
    acquire_lease(
        store_root,
        run_id=run_id,
        scope=None,
        ttl_s=int(lease_ttl_s or DEFAULT_TTL_S),
        store_kwargs=store_kwargs,
    )
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    shard_order = int(manifest["shard_order"])
    ladder_entries(manifest.get("pyramid") or {}, shard_order)  # loud /2 gate
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    merged: dict = {}
    stage_rows: list = []
    records = read_stage_records(records_from, run_id=run_id, store_kwargs=store_kwargs)
    if not records:
        raise ValueError(
            f"the finisher for run {run_id!r} found no stage records under "
            f"{records_from} — a finisher fires only after a fan-out, so zero records "
            "means the fan-out was lost; refusing rather than recording an empty sweep "
            "(a PARTIAL set is fine: under-coverage is recorded and self-heals)"
        )
    for row in records:
        merge_level_actuals(merged, row.get("level_actuals") or {})
        stage_rows.extend(row.get("stages") or [])
    aggregated = aggregate_actuals(merged)
    summary: dict = {
        "run_id": run_id,
        "store_root": store_root,
        "shard_order": shard_order,
        "transport": "lambda",
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "stage_records": len(records),
        "stages": stage_rows,
        "levels": {str(k): v for k, v in sorted(aggregated.items(), reverse=True)},
    }
    if any(r.get("root_moc_stale") for r in records):
        summary["root_moc_stale"] = True
    # The dispatcher's soft-barrier verdict, made DURABLE (review finding). A
    # barrier that expired on a QUEUED rather than a lost invoke lets this
    # finisher aggregate before its late siblings have written: their artifacts
    # are still correct (a same-run sibling is not foreign, so nothing aborts),
    # but the per-level actuals recorded below can be short. Without this the
    # only witness is the dispatcher's log, which does not outlive the process
    # — and the manifest would claim coverage it did not observe.
    summary["barrier_timed_out"] = bool(barrier_timed_out)
    if barrier_timed_out:
        logger.warning(
            f"stage sweep {run_id}: the dispatcher reported an expired barrier — the "
            f"per-level actuals recorded from {len(records)} stage record(s) may "
            "under-report; the next pass heals them"
        )
    summary["finisher"] = run_finisher(
        store_root,
        manifest,
        by_shard,
        aggregated,
        run_id=run_id,
        store_kwargs=store_kwargs,
        release=lambda: release_lease(store_root, run_id=run_id, store_kwargs=store_kwargs),
        touch_policy=touch_policy,
    )
    summary["lease"] = {"released": bool(summary["finisher"].get("lease_released"))}
    summary["duration_s"] = time.perf_counter() - t0
    if record:
        summary["record"] = _write_stage_record(store_root, summary, store_kwargs)
    summary["finisher_record"] = _put_stage_record(
        records_from,
        FINISHER_RECORD_NAME,
        {"spec": STAGE_RECORD_SPEC, "role": "finisher", **summary},
        store_kwargs,
    )
    return summary
