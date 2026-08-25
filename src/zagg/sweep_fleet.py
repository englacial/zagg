"""The fleet dispatcher for the ``/2`` staged dense sweep (issue #519).

:mod:`zagg.sweep_stages` owns the staged sweep itself — the pass driver, the
finisher, and :func:`zagg.sweep_stages.run_stage_sweep`, which runs a whole
run in ONE process. This module runs the same run over the fleet: it mirrors
that function's tuple ordering exactly, but every unit of work is a
``mode="sweep"`` Lambda invoke carrying a ``stage`` block
(:func:`zagg.sweep_stages.run_stage_worker` is what receives it), and the
dispatcher itself **never writes to the store** — the D8 standing rule, which
is the whole reason this transport exists: a source.coop-published store names
the fleet execution role as its write identity (#495/#496), so a local stage
worker has no sanctioned write path at all.

The shape, per tuple, finest first:

1. **fan out** — the tuple's dispatch nodes, batched under the async payload
   cap, one ``InvocationType="Event"`` invoke per batch;
2. **soft-barrier** — poll the run's status prefix until every batch's stage
   record appears, or the barrier budget runs out. The barrier is a
   SCHEDULING preference, not a correctness device (#381 point (6)):
   under-coverage is recorded in the artifact's own ``source_children`` and
   heals on the next pass, so a timed-out barrier logs loudly and the run
   proceeds rather than stalling;
3. **next tuple**, then the **finisher** invoke last
   (:func:`zagg.sweep_stages.run_stage_finisher`) — root ``coverage.moc``,
   manifest actuals, ``aggregation.yaml`` touch, lease release.

Why the split across invokes is free: dispatch nodes at one order own disjoint
subtrees, and a tuple's folds read only columns one tuple FINER (a stage
worker reads its children once at ``child_order`` and folds every order in the
tuple from those same readers). Grouping is therefore a dispatch knob, never
grammar — the merge-source law (espg ruling 2026-08-09, issue #384) — and the
fleet-built ladder is byte-identical to the CLI-built one on the same store.

Dispatch nodes come from the WORK SET the dispatcher holds, not from the
store: an invoke-only dispatcher role cannot read the root ``coverage.moc``,
and D8 keeps it that way. The in-process pass derives its dispatch nodes from
work set ∪ root MOC, so a node whose ONLY leaves are untouched siblings
recorded in the MOC is not invoked here. That is the same scoped-sweep
posture ``stage_sweep_after_run`` already has (#381 point (11): the run's
shard set IS the touched shardmap); the worker still folds every child on
disk under each node it IS handed, so untouched siblings are folded in, never
dropped.
"""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)

#: Barrier budget per tuple, in seconds. A stage invoke is bounded by the
#: function timeout (900 s on the deployed fleet), and Event invokes queue, so
#: the budget has to cover a cold fan-out plus one worker's full run.
DEFAULT_BARRIER_TIMEOUT_S = 1200
#: Seconds between barrier polls. One LIST per poll, not one HEAD per record.
DEFAULT_POLL_INTERVAL_S = 5.0
#: The ``batch`` index a packed batch is MEASURED with. Larger than any batch
#: count a real fan-out reaches, so the measured payload is never smaller than
#: the one that ships (the index is assigned after packing).
_BATCH_INDEX_PROBE = 9_999_999


def dispatch_nodes(by_shard, dispatch: int, scope=None) -> list:
    """The tuple's dispatch nodes for a work set: sorted morton decimals.

    Every leaf's ancestor at the ``dispatch`` order, de-duplicated. ``scope``
    (a normalized MOC) filters them exactly as it filters the in-process
    pass's dispatch nodes.
    """
    from zagg.sweep_stage import _node_at
    from zagg.sweep_stages import scope_admits

    nodes = sorted({_node_at(d, int(dispatch)) for d in by_shard})
    return [n for n in nodes if scope_admits(n, scope)]


def _leaf_refs(by_shard, nodes=None) -> list:
    """``[[shard_key, window], ...]`` for the whole work set, or one node slice."""
    from zagg.grids.morton import morton_word

    prefixes = None if nodes is None else tuple(nodes)
    return [
        [int(morton_word(decimal)), window]
        for decimal in sorted(by_shard)
        if prefixes is None or decimal.startswith(prefixes)
        for window in sorted(by_shard[decimal], key=lambda w: (w is not None, w))
    ]


def _bucket_leaf_refs(by_shard, dispatch: int) -> dict:
    """``{dispatch node: [[shard_key, window], ...]}`` in ONE pass over the work set.

    :func:`pack_batches` needs every node's leaf slice, and asking
    :func:`_leaf_refs` for them one node at a time re-sorts and re-scans the
    WHOLE work set per node — quadratic, and the dominant dispatcher cost on
    the ~49k-node finest tuple of an o9 store (review finding). Bucketing by
    :func:`zagg.sweep_stage._node_at` once is linear and yields each node's
    slice in exactly the order ``_leaf_refs(by_shard, [node])`` would have.
    """
    from zagg.grids.morton import morton_word
    from zagg.sweep_stage import _node_at

    dispatch = int(dispatch)
    buckets: dict = {}
    for decimal in sorted(by_shard):
        key = int(morton_word(decimal))
        buckets.setdefault(_node_at(decimal, dispatch), []).extend(
            [key, window]
            for window in sorted(by_shard[decimal], key=lambda w: (w is not None, w))
        )
    return buckets


def _inline_event(store_path: str, block: dict, leaves, output_creds_event=None) -> dict:
    """The event build with NO cap fallback — the shape the packer measures.

    :func:`build_stage_event` is this plus the last-resort conversion to
    ``discover: true``; measuring through THAT would measure the stripped
    event and call every overflow a fit.
    """
    event: dict = {"mode": "sweep", "store_path": store_path, "stage": dict(block)}
    if output_creds_event is not None:
        event["output_credentials"] = output_creds_event
    if leaves is None:
        event["discover"] = True
    else:
        event["leaves"] = list(leaves)
    return event


def build_stage_event(store_path: str, block: dict, leaves, output_creds_event=None) -> dict:
    """One ``mode="sweep"`` + ``stage`` worker event; the single build site.

    Mirrors :func:`zagg.runner._build_sweep_event`: optional keys are added
    only when set, and an oversized inline work set falls back to
    ``discover: true`` — the worker re-derives it from the store's run records
    and each dispatch node prefix-filters it anyway, so the fallback stays
    correct per batch (it costs a LIST plus a parquet read per invoke, which
    is why :func:`pack_batches` spends its budget on keeping leaves inline).
    That fallback is a LAST resort, not a routine path: :func:`pack_batches`
    measures every batch it emits, so reaching it means the batch was built
    somewhere else — hence the WARNING naming the batch, since a silent switch
    to a store-wide LIST shows up only as unexplained worker latency.

    ``leaves=None`` asks for the discovery form outright — what
    :func:`pack_batches` returns for a batch whose own leaf slice cannot fit.
    An EMPTY list is not the same thing and never means discovery: it is a
    genuinely empty work set, and silently converting it would turn "nothing
    to do" into a full store-wide re-derivation.
    """
    from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES

    event = _inline_event(store_path, block, leaves, output_creds_event)
    if leaves is not None and len(json.dumps(event)) > _ASYNC_PAYLOAD_CAP_BYTES:
        logger.warning(
            f"stage fleet: run {block.get('run_id')!r} role {block.get('role', 'stage')} "
            f"batch {block.get('batch')} @{block.get('dispatch')} overflowed the "
            f"{_ASYNC_PAYLOAD_CAP_BYTES}-byte async payload cap with "
            f"{len(event['leaves'])} inline leaf ref(s) — falling back to discover: true, "
            "which costs the worker a store-wide LIST plus a parquet read"
        )
        del event["leaves"]
        event["discover"] = True
    return event


def _fit_batch(nodes, buckets, *, block: dict, store_path: str, output_creds_event, cap) -> list:
    """Split one greedily-packed batch until its REAL event fits under the cap.

    The incremental accounting in :func:`pack_batches` is an estimate; this is
    what makes it verifiable rather than merely careful. A batch that still
    measures over is halved and re-measured rather than handed to
    :func:`build_stage_event`, whose only recourse is to strip ``leaves`` and
    make the worker rediscover the whole work set. A single node that cannot
    fit alone IS the discover case, and is emitted as one.
    """
    leaves = [ref for node in nodes for ref in buckets.get(node, [])]
    probe = {**block, "nodes": list(nodes), "batch": _BATCH_INDEX_PROBE}
    if len(json.dumps(_inline_event(store_path, probe, leaves, output_creds_event))) <= cap:
        return [(list(nodes), leaves)]
    if len(nodes) == 1:
        return [(list(nodes), None)]  # its own leaves overflow: discover
    mid = len(nodes) // 2
    kw = dict(block=block, store_path=store_path, output_creds_event=output_creds_event, cap=cap)
    return _fit_batch(nodes[:mid], buckets, **kw) + _fit_batch(nodes[mid:], buckets, **kw)


def pack_batches(nodes, by_shard, *, block: dict, store_path: str, output_creds_event=None) -> list:
    """Split one tuple's dispatch nodes into invoke-sized batches.

    Returns ``[(nodes, leaves), ...]``, every node in exactly one batch and in
    order. Greedy in sorted node order over a work set bucketed by dispatch
    node ONCE (:func:`_bucket_leaf_refs`), with the projected payload measured
    incrementally — a full ``json.dumps`` per candidate node, or a full work-set
    scan per node, would be quadratic on the ~49k-node finest tuple of an o9
    store. A single node whose own leaf slice already overflows the cap is
    emitted alone with ``leaves=None`` — :func:`build_stage_event` then sends
    the ``discover: true`` form rather than truncating a work set, which would
    silently under-fold.

    The estimate is deliberately CONSERVATIVE (it charges the real ``", "``
    separators and a fixed envelope margin), and every batch it produces is
    then measured with one real ``json.dumps`` and split if it still exceeds
    the cap — so a batch this function calls inline ships inline, instead of
    being silently converted to ``discover: true`` at build time.
    """
    from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES

    # The fixed cost of the event minus its two variable-length lists, plus a
    # margin for the JSON punctuation the incremental accounting approximates.
    envelope = dict(block)
    envelope["nodes"] = []
    base = len(json.dumps(build_stage_event(store_path, envelope, [], output_creds_event))) + 64
    budget = _ASYNC_PAYLOAD_CAP_BYTES - base
    buckets = _bucket_leaf_refs(by_shard, int(block["dispatch"]))
    grouped: list = []
    cur_nodes: list = []
    cur_bytes = 0
    for node in nodes:
        # `", "` between elements, both lists: json.dumps' default separators.
        cost = len(node) + 4 + sum(len(json.dumps(r)) + 2 for r in buckets.get(node, []))
        if cur_nodes and cur_bytes + cost > budget:
            grouped.append(cur_nodes)
            cur_nodes, cur_bytes = [], 0
        if not cur_nodes and cost > budget:
            grouped.append([node])
            continue
        cur_nodes.append(node)
        cur_bytes += cost
    if cur_nodes:
        grouped.append(cur_nodes)
    batches: list = []
    for batch_nodes in grouped:
        batches.extend(
            _fit_batch(
                batch_nodes,
                buckets,
                block=block,
                store_path=store_path,
                output_creds_event=output_creds_event,
                cap=_ASYNC_PAYLOAD_CAP_BYTES,
            )
        )
    return batches


def _present(records_from: str, store_kwargs: dict) -> set:
    """Basenames currently under the run's status prefix (one LIST).

    A read, not a write: on ``s3://`` this is a single ``ListObjectsV2`` and
    the dispatcher stays write-free (D8). On a LOCAL path ``open_object_store``
    materializes the directory (it has no read-only mode) — a filesystem
    artifact of the test/CLI path, outside the store root, never an object.
    """
    import obstore

    from zagg.store import open_object_store

    try:
        listing = obstore.list_with_delimiter(open_object_store(records_from, **store_kwargs))
    except Exception as e:  # a prefix with no objects yet, or a transient fault
        logger.debug(f"stage fleet: cannot list {records_from} ({e})")
        return set()
    return {meta["path"].rsplit("/", 1)[-1] for meta in listing["objects"]}


def await_records(
    records_from: str,
    expected: set,
    *,
    store_kwargs: dict,
    timeout_s: float,
    interval_s: float,
) -> tuple:
    """Poll until every expected record lands, or the budget runs out.

    Returns ``(seen, timed_out)``. The SOFT barrier: a timeout is logged
    loudly and returned, never raised — the next tuple folding over a node
    whose child column has not landed records the miss in its own
    ``source_children`` and the ratchet heals it on the next pass (#381 point
    (6)). Blocking here is what keeps the fleet's tuple ordering identical to
    the in-process driver's, which is what the byte-identity acceptance rests
    on.
    """
    deadline = time.monotonic() + float(timeout_s)
    while True:
        seen = _present(records_from, store_kwargs) & expected
        if seen >= expected:
            return seen, False
        if time.monotonic() >= deadline:
            missing = sorted(expected - seen)
            logger.warning(
                f"stage fleet: barrier timed out after {timeout_s:.0f}s with "
                f"{len(missing)}/{len(expected)} stage record(s) missing ({missing[:5]}"
                f"{' ...' if len(missing) > 5 else ''}) — proceeding: under-coverage is "
                f"recorded per artifact and heals on the next pass (#381 point (6))"
            )
            return seen, True
        time.sleep(float(interval_s))


def run_stage_sweep_fleet(
    lambda_client,
    function_name: str,
    store_path: str,
    leaves,
    *,
    shard_order: int,
    scope=None,
    tuple_width: int | None = None,
    run_id: str | None = None,
    output_creds_event=None,
    store_kwargs: dict | None = None,
    touch_policy: str = "auto",
    lease_ttl_s: int | None = None,
    barrier_timeout_s: float = DEFAULT_BARRIER_TIMEOUT_S,
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
) -> dict:
    """One staged sweep run over the fleet: tuples, barriers, finisher last.

    The client-side mirror of :func:`zagg.sweep_stages.run_stage_sweep`. Same
    tuple ordering, same run identity, same lease — but the pass runs in
    workers, so this side only invokes and polls. Nothing here writes to the
    store (D8), including the lease: the FIRST stage worker of the run creates
    the intent and the finisher releases it.

    ``shard_order`` is supplied by the caller rather than read from the
    manifest for the same reason: a dispatcher role may hold nothing but
    ``lambda:InvokeFunction``. The runner has it from the config.

    ``run_id`` names the lease, the skip-key/foreign-stamp namespace AND the
    status prefix the stage records land under, so it is generated here (or
    supplied) and threaded verbatim into every invoke. ``run_started`` is
    pinned here too: every worker of the run must agree on when the run began,
    or a sibling's fresh stamp reads as a foreign sweep's.

    Returns the dispatcher's own summary — what it fired and what it saw. The
    RUN's record is the finisher's (``sweep_stats_{ts}_stages.json`` at the
    store root, worker-written); it is read back here when it lands.
    """
    import uuid
    from datetime import datetime, timezone

    from zagg.client_transport import run_status_prefix
    from zagg.hive import _utcnow
    from zagg.sweep import _normalize_leaves
    from zagg.sweep_stage import DEFAULT_TUPLE_WIDTH, stage_tuples
    from zagg.sweep_stages import FINISHER_RECORD_NAME, stage_record_name

    t0 = time.perf_counter()
    store_kwargs = dict(store_kwargs or {})
    tuple_width = int(DEFAULT_TUPLE_WIDTH if tuple_width is None else tuple_width)
    shard_order = int(shard_order)
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    run_id = run_id or (
        f"stage-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    )
    run_started = _utcnow()
    records_from = run_status_prefix(store_path, run_id)
    summary: dict = {
        "run_id": run_id,
        "run_started": run_started,
        "store_root": store_path,
        "shard_order": shard_order,
        "tuple_width": tuple_width,
        "transport": "lambda",
        "records_from": records_from,
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "invokes": 0,
        "stages": [],
    }

    def _fire(event: dict) -> None:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(event),
        )
        summary["invokes"] += 1

    for stage in stage_tuples(shard_order, tuple_width=tuple_width):
        dispatch = int(stage["dispatch"])
        nodes = dispatch_nodes(by_shard, dispatch, scope)
        if not nodes:
            continue
        block = {
            "role": "stage",
            "run_id": run_id,
            "run_started": run_started,
            "dispatch": dispatch,
            "tuple_width": tuple_width,
            "records_from": records_from,
        }
        if lease_ttl_s is not None:
            block["lease_ttl_s"] = int(lease_ttl_s)
        batches = pack_batches(
            nodes,
            by_shard,
            block=block,
            store_path=store_path,
            output_creds_event=output_creds_event,
        )
        expected = set()
        for batch, (batch_nodes, batch_leaves) in enumerate(batches):
            expected.add(stage_record_name(dispatch, batch))
            _fire(
                build_stage_event(
                    store_path,
                    {**block, "nodes": batch_nodes, "batch": batch},
                    batch_leaves,
                    output_creds_event,
                )
            )
        t_stage = time.perf_counter()
        seen, timed_out = await_records(
            records_from,
            expected,
            store_kwargs=store_kwargs,
            timeout_s=barrier_timeout_s,
            interval_s=poll_interval_s,
        )
        summary["stages"].append(
            {
                "dispatch_order": dispatch,
                "orders": list(stage["orders"]),
                "nodes": len(nodes),
                "batches": len(batches),
                "records_seen": len(seen),
                "barrier_timed_out": timed_out,
                "barrier_s": time.perf_counter() - t_stage,
            }
        )
        logger.info(
            f"stage fleet: tuple @{dispatch} — {len(nodes)} node(s) in {len(batches)} invoke(s), "
            f"{len(seen)}/{len(expected)} record(s) in {time.perf_counter() - t_stage:.1f}s"
        )
    _fire(
        build_stage_event(
            store_path,
            {
                "role": "finisher",
                "run_id": run_id,
                "records_from": records_from,
                "touch_policy": touch_policy,
            },
            _leaf_refs(by_shard),
            output_creds_event,
        )
    )
    seen, timed_out = await_records(
        records_from,
        {FINISHER_RECORD_NAME},
        store_kwargs=store_kwargs,
        timeout_s=barrier_timeout_s,
        interval_s=poll_interval_s,
    )
    summary["finisher"] = {"landed": not timed_out}
    if seen:
        summary["finisher"].update(_read_finisher_record(records_from, store_kwargs))
    summary["duration_s"] = time.perf_counter() - t0
    return summary


def _read_finisher_record(records_from: str, store_kwargs: dict) -> dict:
    """The finisher's record, reduced to what a dispatcher reports (fail-open)."""
    import obstore

    from zagg.store import open_object_store
    from zagg.sweep_stages import FINISHER_RECORD_NAME

    try:
        store = open_object_store(records_from, **store_kwargs)
        record = json.loads(bytes(obstore.get(store, FINISHER_RECORD_NAME).bytes()))
    except Exception as e:
        logger.warning(f"stage fleet: finisher record unreadable ({e})")
        return {}
    return {
        key: record[key]
        for key in ("stage_records", "levels", "lease", "record", "duration_s")
        if key in record
    }
