"""v2 Event-transport status channel (issue #327): per-shard status objects.

The fleet-scale alternative to holding one synchronous connection per in-flight
shard: workers record each shard's outcome as a tiny JSON object under a
per-run prefix SIBLING to the output store::

    <store>.status/run-<run_id>/shard-<decimal-key>.json

and the client resolves futures by polling that prefix instead of reading an
invoke response. Both halves of the channel live here so the schema has one
home:

- **Worker half** (:func:`write_shard_status`): called from the Lambda
  handler's top-level seam on every per-unit response — every status branch,
  including the caught-error 500 envelope. Always-on (espg ratification on
  issue #327: every run, regardless of dispatcher) and emphatically
  **fail-open**: a status PUT failure logs and never affects the shard result.
  Written through the same ``open_object_store`` factory + output credentials
  as every other worker write, so the dispatcher-never-writes rule (D8) is
  preserved by construction.
- **Client half** (later phases of issue #327): the polling resolver that
  turns one LIST of the run prefix per tick into future resolutions (one LIST
  *call*, which is O(N/1000) requests — see :class:`StatusPoller` for the real
  per-tick cost).

Naming: the shard component is the shard key's plain decimal rendering
(``str(int(shard_key))``) — derivable on every handler branch including the
caught-error path, where the grid (and so the issue #199 morton label) may not
be constructible. Windowed units (issue #246) suffix the window label,
mirroring the leaf naming, so two windows of one shard cannot clobber each
other's object. The prefix is a sibling of the store (never inside it): the
store stays vanilla zarr v3, the benchmark object model never sees telemetry,
and an S3 lifecycle/delete rule on the store's key prefix string catches the
status objects with it (espg ruling on issue #327 — accumulation hygiene is
the existing prefix delete policy, no in-band pruning).

Lambda Event-invoke semantics this channel is built for (issue #327
amendments): the fleet's ``EventInvokeConfig`` pins ``MaximumRetryAttempts: 0``
(no service retries — attempt-id dedupe is belt-and-braces, and it also
dedupes the client's own re-dispatches) and ``MaximumEventAgeInSeconds: 60``
(an invoke that cannot start within 60 s is silently dropped — the client
resolves a status-less shard as ``failed-unknown`` after the drop deadline).
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Callable

from zagg.dispatch import BENIGN_ERRORS

logger = logging.getLogger(__name__)

#: Poll cadence (issue #327, ratified fixed — not knobs): the tick interval
#: starts at the initial value, doubles on an idle tick up to the cap, and
#: resets on progress (a consumed status), so retries are picked up fast while
#: a long quiet fan-out costs ~4 LISTs a minute.
_POLL_INITIAL_INTERVAL_S = 2.0
_POLL_MAX_INTERVAL_S = 15.0

#: The fleet's ``MaximumEventAgeInSeconds`` (template.yaml EventInvokeConfig,
#: issue #151): an Event invoke that cannot start within this window is
#: silently discarded — the backpressure drop the failed-unknown outcome
#: exists to catch (issue #327 amendment (a')).
MAX_EVENT_AGE_S = 60.0

#: Margin on the drop deadline past function timeout + max event age: async
#: queue latency + the worker's status PUT. Mirrors the #151 channel's
#: ``_ASYNC_POLL_MARGIN_S``.
_DROP_MARGIN_S = 90.0

#: Class-(b) re-fire budget (see :class:`StatusPoller`): a status-less shard
#: re-fires at most this many times. ONE, so the worst case is 2 executions —
#: the bounded double-billing the queue-drop class trades for recovering a
#: silently discarded invoke (issue #151: ``MaximumRetryAttempts: 0`` exists to
#: stop the fleet re-billing 900 s executions, so the client must not
#: reintroduce an unbounded version of it from the outside).
_MAX_DROP_REFIRES = 1

#: Class-(c) backoff cap. A faulted invoke is re-armed with a NOT-BEFORE stamp
#: at ``2**attempts`` seconds out (2, 4, 8, ... capped here) — the sync
#: transport's ``time.sleep((2**attempt) + jitter)`` schedule expressed as a
#: timestamp, because this runs on the single poller thread: sleeping there
#: would stall every other shard's LIST. No jitter is needed since the poller
#: re-fires serially anyway, so a synchronized throttle can't produce a
#: simultaneous burst.
_RETRY_BACKOFF_CAP_S = 30.0


def drop_timeout_s(function_timeout_s: float) -> float:
    """Seconds after dispatch with NO status object => ``failed-unknown``.

    Function timeout (the worker may run to the ceiling and still write its
    status) + the 60 s max event age (the invoke may sit queued that long
    before starting — or be dropped) + margin for queue/PUT latency.
    """
    return float(function_timeout_s) + MAX_EVENT_AGE_S + _DROP_MARGIN_S


#: Version stamped into every status object; bump on any key change. Read back
#: in :meth:`StatusPoller._consume`, which warns on a mismatch so a future bump
#: is loud rather than subtly wrong (review finding, PR #343).
STATUS_SCHEMA_VERSION = 1

#: Dispatch-manifest object name under the run prefix (issue #327 phase 2).
MANIFEST_NAME = "manifest.json"

#: Tail-completion marker under the run prefix (issue #327 phase 5): written
#: by the mode="stats" worker when the post-run tail's record leg completes AND
#: the run's finalize succeeded, so a reattached handle re-runs the (idempotent
#: / fail-open) tail whenever it was never recorded or its finalize failed.
TAIL_NAME = "tail.json"


def run_status_prefix(store_path: str, run_id: str) -> str:
    """The run's status prefix: ``<store>.status/run-<run_id>`` (a store SIBLING)."""
    return f"{store_path.rstrip('/')}.status/run-{run_id}"


def shard_status_key(shard_key, window: str | None = None) -> str:
    """One shard's status object name under the run prefix.

    ``shard-<decimal-key>.json``, with the window label suffixed for windowed
    units (validated against the frozen window grammar — a label is a path
    component here, exactly as in the leaf/sidecar names).
    """
    base = f"shard-{int(shard_key)}"
    if window is None:
        return f"{base}.json"
    from zagg.windows import validate_label

    validate_label(str(window))
    return f"{base}_{window}.json"


def build_shard_status(event: dict, response: dict) -> tuple[str, dict] | None:
    """(object key, status object) for one per-unit response, or ``None``.

    ``None`` when the event carries no run identity (``run_id`` /
    ``store_path`` / ``shard_key``) — an old dispatcher, or a non-spatial
    unit — so the write is skipped and behavior is byte-identical to
    pre-#327 for those events.

    Status classification mirrors the dispatch accumulators: a 200 envelope
    with no error is ``ok``; a benign no-work error (:data:`BENIGN_ERRORS`)
    is ``no_data``; everything else — including the handler's caught-error
    500 — is ``failed``. ``timings`` is the same ``phase_timings`` dict the
    response body carries; ``body`` is the full envelope body so the poller
    can synthesize the exact per-shard result dict the sync transport returns
    (stats record, counters, container telemetry). ``attempt_id`` is a fresh
    uuid per EXECUTION: a duplicate execution (or a client re-dispatch)
    overwrites the object with a new id, which is what keeps the client's
    retry accounting straight.
    """
    run_id = event.get("run_id")
    store_path = event.get("store_path")
    shard_key = event.get("shard_key")
    if not run_id or not store_path or shard_key is None:
        return None
    try:
        body = json.loads(response.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        body = {}
    if not isinstance(body, dict):
        body = {}
    error = body.get("error")
    code = response.get("statusCode")
    if error in BENIGN_ERRORS:
        status = "no_data"
    elif code == 200 and not error:
        status = "ok"
    else:
        status = "failed"
    window = (event.get("window") or {}).get("label")
    obj = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "status": status,
        "attempt_id": uuid.uuid4().hex,
        "run_id": run_id,
        "shard": str(int(shard_key)),
        "window": window,
        "timings": body.get("phase_timings"),
        "error": str(error) if error else None,
        "status_code": code,
        "body": body,
    }
    return shard_status_key(shard_key, window=window), obj


def build_run_manifest_block(
    run_id: str, shard_keys, config, *, dataset: dict | None = None
) -> dict:
    """The dispatcher-side ``run_manifest`` block for the worker setup invoke.

    Rides the EXISTING setup invoke (the hive fire-and-forget manifest write /
    the flat synchronous template write) so the WORKER records the run's
    dispatch manifest — the dispatcher never writes (D8). ``shards`` is the
    dispatched shard-key set as decimal strings (deduped, order-preserving —
    windowed fan-outs repeat a shard per window), ``semantic_hash`` the run
    config's D19 identity, ``dispatched_at`` the dispatcher's clock at
    dispatch, and ``dataset`` the ShardMap identity block (``short_name`` /
    ``version``) a reattached tail needs for the hive finalize backstop. The
    worker folds the setup event's own ``config`` into the written manifest,
    so it is not duplicated here on the wire.
    """
    from datetime import datetime, timezone

    from zagg.semantics import semantic_hash

    return {
        "run_id": run_id,
        "shards": list(dict.fromkeys(str(int(k)) for k in shard_keys)),
        "semantic_hash": semantic_hash(config),
        "dispatched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": dataset,
    }


def write_dispatch_manifest(event: dict, store_kwargs: dict[str, Any]) -> None:
    """PUT the run's dispatch manifest from a setup event — fail-open, worker-side.

    Called from the Lambda handler's setup seam on a successful setup. The
    ``run_manifest`` block (see :func:`build_run_manifest_block`) plus the
    setup event's ``config`` land at ``<run prefix>/manifest.json`` — what
    ``Run.attach`` rebuilds a handle from. Absent block (an old dispatcher, or
    one dropped over the async payload cap) writes nothing, keeping the event
    byte-identical to pre-#327; any failure logs and never fails the setup.
    """
    try:
        block = event.get("run_manifest")
        if not block or not block.get("run_id") or not event.get("store_path"):
            return
        manifest = {
            "schema_version": STATUS_SCHEMA_VERSION,
            **block,
            "config": event.get("config"),
        }

        from zagg.store import open_object_store, put_object

        prefix = run_status_prefix(event["store_path"], block["run_id"])
        store = open_object_store(prefix, **store_kwargs)
        put_object(store, MANIFEST_NAME, json.dumps(manifest).encode())
        logger.info(f"Wrote dispatch manifest to {prefix}/{MANIFEST_NAME}")
    except Exception as e:
        logger.warning(f"dispatch manifest write failed (fail-open, issue #327): {e}")


def write_shard_status(event: dict, response: dict, store_kwargs: dict[str, Any]) -> None:
    """PUT the per-shard status object for one response — fail-open, always.

    The worker-half entry point, called from the Lambda handler's top-level
    seam on every per-unit response. ANY exception — building the object,
    opening the store, the PUT itself — is logged and swallowed (espg
    ratification on issue #327: the status PUT must never fail a shard whose
    result is otherwise in hand). ``store_kwargs`` is the handler's own
    output-store resolution, so the write uses exactly the credentials the
    shard's data writes used.
    """
    try:
        built = build_shard_status(event, response)
        if built is None:
            return
        key, obj = built

        from zagg.store import open_object_store, put_object

        prefix = run_status_prefix(event["store_path"], event["run_id"])
        put_object(open_object_store(prefix, **store_kwargs), key, json.dumps(obj).encode())
        logger.info(f"Wrote shard status ({obj['status']}) to {prefix}/{key}")
    except Exception as e:
        logger.warning(f"shard status write failed (fail-open, issue #327): {e}")


def write_tail_status(event: dict, store_kwargs: dict[str, Any]) -> None:
    """PUT the tail-completion marker named by ``event["tail_status_url"]``.

    Worker-side (the ``mode="stats"`` handler — the last recorded leg of the
    post-run tail), fail-open like every status write: absent key or any
    failure logs and never fails the stats invoke.

    NOT written when the stats event carries a ``finalize_error`` (issue #335):
    :meth:`zagg.client.Run.attach` reads this marker as "the tail ran" and
    skips the WHOLE tail, including the idempotent hive finalize /
    ``ensure_manifest`` backstop — and a run whose finalize failed is precisely
    the one that needs another pass. Leaving the marker unwritten is the safe
    direction: attach re-runs a tail that is idempotent by construction
    (review finding, PR #343). The run parquet still records the failure as its
    run-level column, so the postmortem signal is not lost.
    """
    try:
        url = event.get("tail_status_url")
        if not url:
            return
        if event.get("finalize_error"):
            logger.info(
                "tail marker withheld: this run's finalize failed, so a reattached "
                "handle must re-run the (idempotent) tail (issues #327/#335)"
            )
            return

        from zagg.store import open_object_store, put_object

        prefix, _, key = url.rpartition("/")
        marker = {
            "schema_version": STATUS_SCHEMA_VERSION,
            "status": "tail_done",
            "run_id": event.get("run_id"),
        }
        put_object(open_object_store(prefix, **store_kwargs), key, json.dumps(marker).encode())
        logger.info(f"Wrote tail status to {url}")
    except Exception as e:
        logger.warning(f"tail status write failed (fail-open, issue #327): {e}")


# ---------------------------------------------------------------------------
# Client half: the polling resolver (issue #327 phases 3-4) + reattach reads
# (phase 5). The reads below are the D8-safe half: LIST/GET only.
# ---------------------------------------------------------------------------


def read_dispatch_manifest(prefix: str, store_kwargs: dict[str, Any]) -> dict | None:
    """The run's dispatch manifest, or ``None`` when absent (read-only)."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        store = open_status_store(prefix, store_kwargs)
        return json.loads(bytes(obstore.get(store, MANIFEST_NAME).bytes()))
    except (FileNotFoundError, NotFoundError):
        return None


def tail_recorded(prefix: str, store_kwargs: dict[str, Any]) -> bool:
    """Whether the run's post-run tail marker exists (read-only).

    Any read failure counts as "not recorded": the reattached tail then
    re-runs, which is safe — finalize is idempotent and the rollup legs are
    fail-open duplicates at worst.
    """
    import obstore

    try:
        store = open_status_store(prefix, store_kwargs)
        obstore.get(store, TAIL_NAME)
        return True
    except Exception:
        return False


def dispatch_event_shards(
    run,
    client,
    cells,
    *,
    run_id,
    workers,
    config_dict,
    s3_creds,
    output_creds_event,
    aoi_by_shard,
    invoked_by,
    on_failed,
):
    """The v2 Event fan-out for :meth:`zagg.client.Run.dispatch` (issue #327).

    Split out of ``client.py`` for the §4 module ceiling (the split the PR
    plan pre-authorized); ``run`` is the :class:`~zagg.client.Run` instance.
    Builds each shard's event through the SAME construction the sync/agg
    paths use (``runner._build_cell_event`` — payloads byte-identical modulo
    InvocationType, asserted by the parity tests), registers it with a
    :class:`StatusPoller` that paces dispatch to the ``workers`` window, and
    returns ``(futures, poller)`` — the poller is the finisher's ``closer``.
    The payload-cap ValueError raises BEFORE anything fires, so an
    undispatchable run fails whole rather than mid-fan-out. D8 audit: this
    path's only store traffic is the poller's LIST/GETs (reads).
    """

    from zagg import runner

    prefix = run_status_prefix(run.store, run_id)
    store_kwargs = runner._output_store_kwargs(output_creds_event, run.region)
    # The drop deadline keys off the live function Timeout when readable; the
    # helper falls back to the template default (an injected stub client need
    # not answer get_function_configuration).
    timeout_s = runner._get_function_timeout_s(client, run.function_name)
    poller = StatusPoller(
        lambda: open_status_store(prefix, store_kwargs),
        drop_timeout_s=drop_timeout_s(timeout_s),
        max_retries=run.max_retries,
        max_in_flight=workers,
        on_failed=on_failed,
    )
    skip_hash = runner._fleet_skip_hash(run.config, run.overwrite)
    futures: dict[int, Future] = {}
    for key, records in cells:
        key = int(key)
        label = runner._safe_label(run.grid, key)
        granule_urls = runner._resolve_granule_entries(records, run.driver)
        ds = runner._clamped_data_source(dict(run.config.data_source), len(granule_urls))
        cell_config = {**config_dict, "data_source": ds} if ds is not None else config_dict
        event = runner._build_cell_event(
            run.grid.block_index(key),
            key,
            run._parent_order,
            run._child_order,
            granule_urls,
            run.store,
            s3_creds,
            config_dict=cell_config,
            output_creds_event=output_creds_event,
            handoff=run.handoff,
            profile=run.profile,
            aoi_payload=aoi_by_shard.get(key),
            invoked_by=invoked_by,
            run_id=run_id,
            semantic_hash=skip_hash,
        )
        submap = {
            "grid_signature": run.catalog_data["grid_signature"],
            "metadata": runner._submap_metadata(run.catalog_data.get("metadata")),
            "granules": records,
        }
        payload = runner._cell_payload(event, submap=submap, async_invoke=True, label=label)

        def _fire(p=payload):
            client.invoke(FunctionName=run.function_name, InvocationType="Event", Payload=p)

        futures[key] = poller.register(key, label, dispatch=_fire, granule_count=len(granule_urls))
    poller.start()
    return futures, poller


def attach_run(
    run_cls,
    store,
    run_id,
    *,
    function_name=None,
    region="us-west-2",
    lambda_client=None,
    output_credentials=None,
    output_endpoint_url=None,
):
    """The rebuild behind :meth:`zagg.client.Run.attach` (issue #327 phase 5).

    Split out of ``client.py`` for the §4 module ceiling; the public contract
    (and its full docstring) lives on ``Run.attach``. Reads the dispatch
    manifest, rebuilds a Run from its embedded config, registers every listed
    shard observe-only with a :class:`StatusPoller` (drop deadline anchored at
    the manifest's ``dispatched_at``), and starts the finisher with the
    read-only ``tail_recorded`` check so an already-recorded tail never
    re-runs.
    """
    import threading
    from dataclasses import asdict

    from zagg import runner
    from zagg.client import RunHandle, _fail_with_shard_error
    from zagg.config import (
        get_driver,
        get_handoff,
        get_output_endpoint_url,
        get_windowing,
        load_config_from_dict,
        validate_config,
    )
    from zagg.grids import from_config as grid_from_config

    output_creds_event = runner._build_output_creds_event(
        output_credentials, output_endpoint_url, region
    )
    store_kwargs = runner._output_store_kwargs(output_creds_event, region)
    prefix = run_status_prefix(store, run_id)
    manifest = read_dispatch_manifest(prefix, store_kwargs)
    if manifest is None:
        raise ValueError(
            f"no dispatch manifest at {prefix}/{MANIFEST_NAME} — wrong store/run_id, "
            f"a dispatcher predating issue #327, or a hive setup Event that was "
            f"dropped (its manifest write is best-effort)"
        )
    if not manifest.get("config"):
        raise ValueError(f"dispatch manifest for run {run_id} carries no config")
    config = load_config_from_dict(manifest["config"])
    validate_config(config)
    if get_windowing(config) is not None:
        raise NotImplementedError(
            "Run.attach covers unwindowed spatial runs; windowed units are "
            "(shard, window) pairs the manifest does not enumerate"
        )
    shards = [int(s) for s in manifest.get("shards") or []]
    if not shards:
        raise ValueError(f"dispatch manifest for run {run_id} lists no shards")

    grid = grid_from_config(config)
    catalog_data = {
        "shard_keys": shards,
        "granules": [[] for _ in shards],
        "grid_signature": grid.spatial_signature(),
        "metadata": manifest.get("dataset"),
    }
    run = run_cls(
        config,
        catalog_data,
        store=store,
        function_name=runner._resolve_function_name(config, function_name),
        region=region,
        lambda_client=lambda_client,
        driver=get_driver(config),
        handoff=get_handoff(config),
        output_credentials=output_credentials,
        output_endpoint_url=output_endpoint_url or get_output_endpoint_url(config),
    )
    client = lambda_client
    if client is None:
        import boto3
        from botocore.config import Config

        client = boto3.Session().client(
            "lambda",
            region_name=region,
            config=Config(read_timeout=960, connect_timeout=10, retries={"max_attempts": 0}),
        )

    # Drop deadline anchored at the manifest's dispatched_at: attaching long
    # after the fleet finished classifies status-less shards immediately
    # instead of waiting a fresh window.
    dispatched_at = None
    try:
        from datetime import datetime

        dispatched_at = datetime.fromisoformat(manifest["dispatched_at"]).timestamp()
    except (KeyError, TypeError, ValueError):
        pass
    timeout_s = runner._get_function_timeout_s(client, run.function_name)
    poller = StatusPoller(
        lambda: open_status_store(prefix, store_kwargs),
        drop_timeout_s=drop_timeout_s(timeout_s),
        on_failed=_fail_with_shard_error,
    )
    futures: dict[int, Future] = {}
    for key in shards:
        label = runner._safe_label(run.grid, key)
        futures[key] = poller.register(key, label, dispatch=None, dispatched_at=dispatched_at)
    poller.start()

    handle = RunHandle(futures, store_path=store)
    finisher = threading.Thread(
        target=run._post_run,
        args=(
            handle,
            client,
            poller,
            asdict(config),
            manifest.get("dataset"),
            output_creds_event,
            run_id,
        ),
        kwargs={
            "tail_done_check": lambda: tail_recorded(prefix, store_kwargs),
            # An attached run's rows are the same status objects, so an
            # oversized row set has the same pointer to fall back to.
            "result_prefix": prefix,
        },
        name="zagg-client-finish",
        daemon=True,
    )
    handle._finisher = finisher
    finisher.start()
    return handle


def open_status_store(prefix: str, store_kwargs: dict[str, Any]):
    """obstore store rooted at a run's status prefix (the poller's seam).

    Module-level so tests can monkeypatch it with an in-memory store; the
    production path is the same ``open_object_store`` factory the #151 result
    poller uses, with the runner's short per-request retry policy riding in
    ``store_kwargs``.
    """
    from zagg.store import open_object_store

    return open_object_store(prefix, **store_kwargs)


@dataclass
class _ShardEntry:
    """One shard's transport state, owned by the poller thread."""

    shard_key: int
    label: str
    key: str  # status object name under the run prefix
    future: Future
    dispatch: Callable[[], None] | None  # fires one Event invoke; None = observe-only
    granule_count: int = 0
    attempts: int = 0  # dispatches so far (the v1 attempt counter)
    queued: bool = True  # awaiting a dispatch slot
    drop_refires: int = 0  # class-(b) re-fires spent (see StatusPoller)
    retry_not_before: float = 0.0  # class-(c) backoff: don't re-fire before this
    dispatched_at: float | None = None  # this attempt's dispatch clock
    first_dispatched_at: float | None = None
    consumed: set = field(default_factory=set)  # attempt_ids already acted on


class StatusPoller:
    """Single background thread resolving ALL shard futures from status objects.

    One LIST of the run's status prefix per tick, then one GET per newly-landed
    (or re-attempted) object. Never writes (D8: the LIST/GETs are reads; every
    write stays worker-side).

    **What "one LIST per tick" actually costs** (review finding, PR #343 —
    issue #327's acceptance criterion says *O(1) LIST per tick regardless of
    shard count*, and that is not literally true): :meth:`_list_keys` re-lists
    the WHOLE run prefix and materializes every key, so a tick is O(N/1000)
    requests (``obstore.list``'s internal pagination) and O(N) key parsing —
    ~5 pages and 5k parsed paths per tick at 5k shards, in perpetuity. It is
    one *call site*, not one request. Trimming it would mean a lexicographic
    offset past the highest consumed key (``list_with_offset``); at the orders
    zagg dispatches today the LIST is not the cost that matters, but nobody
    should size a 50k-shard run off an O(1) claim. Two smaller notes on the
    same seam: a shard awaiting a re-fire keeps its stale object listed, so it
    is re-GET and re-parsed every tick until the new attempt lands (bounded by
    the failed-shard count); and :meth:`_dispatch_up_to_window` fires its whole
    batch SERIALLY on this thread, where v1 invoked from ``workers`` threads —
    so a ``max_in_flight``-wide opening burst is one thread's sequential invoke
    round-trips (order 10 ms each, i.e. tens of seconds at a 1000-wide window)
    during which no LIST runs. Fine for 900 s shards; it is a real ramp, not a
    mystery.

    The poller also owns Event dispatch: shards register with a zero-arg
    ``dispatch`` closure and at most ``max_in_flight`` are
    dispatched-unresolved at once (the concurrency preflight's clamp —
    load-bearing under the fleet's 60 s ``MaximumEventAgeInSeconds``, which
    silently drops an Event that cannot start in time).

    **Retry policy — three fault classes** (issue #327 amendment (b) as folded
    from the PR #343 review; espg's to veto). Function errors never return to
    an Event caller, so v1's single ``max_retries`` loop has to be split by
    what actually failed:

    (a) **Deterministic worker failure** — a ``failed`` STATUS object. By
        construction the worker RAN TO COMPLETION and reported an error, which
        is the case issue #119 already excludes from retry on the sync
        transport (*"Every FunctionError on a synchronous invoke is
        deterministic for a given shard ... so none are retried"*). It resolves
        immediately; no re-fire, no second billing.
    (b) **Queue drop** — no status object by the drop deadline
        (``failed-unknown``). Genuinely worth one more shot, since the invoke
        may simply have aged out of the async queue under backpressure. Bounded
        at :data:`_MAX_DROP_REFIRES` = 1 re-fire, so the exposure is at most
        **2 × function timeout** billed for a shard that may be unrecoverable
        (a drop and an OOM/timeout death are indistinguishable from the
        client). That bound is the point: issue #151 pinned
        ``MaximumRetryAttempts: 0`` precisely so async fan-out is never
        re-billed at 900 s per service retry, and an unbounded client-side
        re-dispatch would reintroduce it.
    (c) **Invoke-layer fault** — the ``client.invoke`` call itself raised
        (throttle, read timeout, connection reset). This is v1's ``max_retries``
        in its original meaning, and it retries up to that budget with
        **exponential backoff** — ``2**attempts`` seconds, capped at
        :data:`_RETRY_BACKOFF_CAP_S`, recorded as a not-before timestamp on the
        entry rather than a ``time.sleep``. Sleeping is not available here: one
        thread serves every shard, so a sleep would stall the LISTs. Without a
        stamp a throttled shard's whole budget burns in the same pass (the
        ``while True`` in :meth:`_dispatch_up_to_window` re-reads ``queued``
        immediately), turning a transient ``TooManyRequestsException`` — the
        canonical fleet-scale fault, and the exact condition the pacing window
        exists to manage — into a wholesale run failure in milliseconds.

    ``attempt_id`` keeps the accounting straight across all three: each status
    object is acted on at most once, so a duplicate execution's re-write (or a
    poll catching a stale pre-retry object) never double-counts.

    Resolution policy is the caller's: a terminal success/no-data sets the
    result dict (the exact shape the sync transport returns); a terminal
    failure goes through ``on_failed(entry, result)`` — the client raises
    :class:`zagg.client.ShardError` there, ``agg`` records the result dict —
    defaulting to ``set_result``.

    Observe-only entries (``dispatch=None`` — :meth:`zagg.client.Run.attach`)
    are never re-dispatched: a ``failed`` status resolves immediately and the
    drop deadline is anchored at the manifest's ``dispatched_at``.
    """

    def __init__(
        self,
        store_factory: Callable[[], Any],
        *,
        drop_timeout_s: float,
        max_retries: int = 3,
        max_in_flight: int | None = None,
        on_failed: Callable[[_ShardEntry, dict], None] | None = None,
        initial_interval_s: float | None = None,
        max_interval_s: float | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self._store_factory = store_factory
        self._store: Any = None
        self._drop_timeout_s = drop_timeout_s
        self._max_retries = max(1, int(max_retries))
        self._max_in_flight = max_in_flight
        self._on_failed = on_failed
        # None -> the module constants, read at construction so tests can
        # scale the cadence for every internally-built poller.
        self._initial_interval_s = (
            _POLL_INITIAL_INTERVAL_S if initial_interval_s is None else initial_interval_s
        )
        self._max_interval_s = _POLL_MAX_INTERVAL_S if max_interval_s is None else max_interval_s
        self._clock = clock
        self._entries: dict[str, _ShardEntry] = {}  # status key -> entry
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- registration / lifecycle -------------------------------------------

    def register(
        self,
        shard_key,
        label: str,
        *,
        dispatch: Callable[[], None] | None = None,
        window: str | None = None,
        granule_count: int = 0,
        dispatched_at: float | None = None,
    ) -> Future:
        """Track one shard; returns the Future the poller will resolve.

        ``dispatched_at`` seeds the drop deadline for observe-only entries
        (attach: the manifest's dispatch time); live entries get stamped at
        their own Event invoke.

        A registration that lands AFTER :meth:`shutdown` resolves immediately
        as a failure. :meth:`_run` is deliberately open-ended on the other side
        (registration may trail :meth:`start` — agg's pool registers shards as
        its threads reach them), so without this guard the loop is already gone
        and nothing would ever resolve the Future: ``runner``'s
        ``fut.result()`` has no timeout, so a pool thread would block forever
        and ``executor.shutdown()`` would never return. Today that ordering is
        safe only because ``_run_lambda``'s ``finally`` drains the executor
        before shutting the poller down — load-bearing and undocumented
        (review finding, PR #343).
        """
        entry = _ShardEntry(
            shard_key=int(shard_key),
            label=label,
            key=shard_status_key(shard_key, window=window),
            future=Future(),
            dispatch=dispatch,
            granule_count=granule_count,
        )
        if dispatch is None:
            entry.queued = False
            entry.attempts = 1
            entry.dispatched_at = dispatched_at if dispatched_at is not None else self._clock()
            entry.first_dispatched_at = entry.dispatched_at
        with self._lock:
            stopped = self._stop.is_set()
            if not stopped:
                self._entries[entry.key] = entry
        if stopped:
            entry.queued = False
            self._resolve_failure(
                entry,
                self._result(
                    entry,
                    status_code=None,
                    body={},
                    error=(
                        f"shard {entry.label} registered after the status poller shut "
                        f"down: no loop left to resolve it (issue #327)"
                    ),
                    outcome="not-dispatched",
                ),
            )
        return entry.future

    def start(self) -> "StatusPoller":
        self._thread = threading.Thread(target=self._run, name="zagg-status-poller", daemon=True)
        self._thread.start()
        return self

    def shutdown(self, wait: bool = False) -> None:
        """Stop the poller thread (pool-shaped so the finisher can close it).

        The flag is set under the entry lock so it cannot interleave with a
        :meth:`register` that is mid-insert: a registration either lands while
        the loop is live, or is refused and resolved (see ``register``).
        """
        with self._lock:
            self._stop.set()
        if wait and self._thread is not None:
            self._thread.join()

    # -- the loop -------------------------------------------------------------

    def _run(self) -> None:
        # Runs until shutdown(), never self-exits on "all done": registration
        # may TRAIL start() (agg's pool registers shards as its threads reach
        # them), so an empty/settled entry map is not proof the run is over.
        # An idle tick with nothing in flight returns before the LIST, so a
        # drained poller costs zero requests while it waits to be shut down.
        interval = self._initial_interval_s
        while not self._stop.is_set():
            try:
                progressed = self._tick()
            except Exception as e:
                # A transient LIST/GET fault must not kill the resolver; a
                # persistent one surfaces through the drop deadline instead.
                logger.warning(f"status poll tick failed (retrying): {e}")
                progressed = False
            interval = (
                self._initial_interval_s if progressed else min(interval * 2, self._max_interval_s)
            )
            # Never oversleep a class-(c) backoff: the idle-tick doubling would
            # otherwise add up to _POLL_MAX_INTERVAL_S to every re-fire.
            wait = self._next_retry_wait()
            if wait is not None:
                interval = min(interval, max(wait, 0.01))
            self._stop.wait(interval)

    def _next_retry_wait(self) -> float | None:
        """Seconds until the soonest backoff-armed re-fire, or ``None`` if none."""
        with self._lock:
            stamps = [
                e.retry_not_before
                for e in self._entries.values()
                if e.queued and e.retry_not_before and not e.future.done()
            ]
        return min(stamps) - self._clock() if stamps else None

    def _pending(self) -> list[_ShardEntry]:
        with self._lock:
            return [e for e in self._entries.values() if not e.future.done()]

    def _tick(self) -> bool:
        # Resolve BEFORE dispatching so a slot freed by a resolution refills in
        # the SAME tick (review finding, PR #343: dispatching first left the
        # slot idle until the next tick — up to _POLL_MAX_INTERVAL_S of dead
        # window). Harmless on the opening tick: every entry is still queued, so
        # _resolve_landed finds no pending entry and skips the LIST entirely.
        progressed = self._resolve_landed()
        return self._dispatch_up_to_window() or progressed

    def _resolve_landed(self) -> bool:
        """Consume newly-landed status objects; classify past-deadline shards."""
        pending = [e for e in self._pending() if not e.queued]
        if not pending:
            return False
        progressed = False
        listed = self._list_keys()
        now = self._clock()
        for entry in pending:
            if entry.future.done():
                continue
            if entry.key in listed and self._consume(entry):
                progressed = True
            elif (
                entry.dispatched_at is not None
                and now >= entry.dispatched_at + self._drop_timeout_s
            ):
                self._dropped(entry)
                progressed = True
        return progressed

    def _dispatch_up_to_window(self) -> bool:
        """Fire queued shards while the in-flight window has room (pacing).

        A queued entry still inside its class-(c) backoff is skipped (not
        counted against the window either — a parked shard holds no slot), so
        the loop terminates on a faulting dispatch instead of spinning the whole
        retry budget in one pass.
        """
        dispatched = False
        while True:
            now = self._clock()
            with self._lock:
                queued = [
                    e
                    for e in self._entries.values()
                    if e.queued and not e.future.done() and e.retry_not_before <= now
                ]
                in_flight = sum(
                    1 for e in self._entries.values() if not e.queued and not e.future.done()
                )
                room = (
                    len(queued) if self._max_in_flight is None else self._max_in_flight - in_flight
                )
                batch = queued[: max(room, 0)]
                for entry in batch:
                    entry.queued = False
            if not batch:
                return dispatched
            for entry in batch:
                self._fire(entry)
                dispatched = True

    def _fire(self, entry: _ShardEntry) -> None:
        dispatch = entry.dispatch
        if dispatch is None:  # observe-only entries are never queued (attach)
            return
        entry.attempts += 1
        entry.retry_not_before = 0.0  # this attempt cleared any armed backoff
        entry.dispatched_at = self._clock()
        if entry.first_dispatched_at is None:
            entry.first_dispatched_at = entry.dispatched_at
        try:
            dispatch()
        except Exception as e:
            # Fault class (c): the invoke CALL failed (throttle, read timeout).
            # Burns this attempt and re-fires within max_retries.
            logger.warning(f"shard {entry.label}: Event invoke failed ({e})")
            self._invoke_faulted(
                entry,
                self._result(entry, status_code=None, body={}, error=f"Event invoke failed: {e}"),
            )

    # -- resolution -----------------------------------------------------------

    def _list_keys(self) -> set:
        import obstore

        if self._store is None:
            self._store = self._store_factory()
        keys: set = set()
        for batch in obstore.list(self._store):
            for meta in batch:
                keys.add(str(meta["path"]))
        return keys

    def _consume(self, entry: _ShardEntry) -> bool:
        """GET one listed status object; act on it once per attempt_id.

        Two read-side guards on the fields the channel's forward-compat rests
        on (review findings, PR #343):

        - An unrecognized ``schema_version`` WARNS. A newer worker against an
          older client is the normal fleet state during a layer roll, so the
          version is only worth stamping if a bump is loud; the object is still
          consumed (a shard whose data landed must not fail on telemetry), but
          the log names the mismatch instead of reading v99 as v1 in silence.
        - A falsy/absent ``attempt_id`` is treated as always-new. Adding
          ``None`` to ``consumed`` used to poison the dedupe permanently: every
          later attempt's object read as ``None`` too and was ignored, so a
          shard that SUCCEEDED on its re-fire waited out the whole drop window
          and was reported ``failed-unknown``. ``build_shard_status`` always
          sets the id, so this needs a foreign or truncated object — but a
          silent-wrong-answer mode is worth one line to close.
        """
        import obstore

        try:
            obj = json.loads(bytes(obstore.get(self._store, entry.key).bytes()))
        except Exception as e:
            logger.warning(f"shard {entry.label}: status GET failed (retrying): {e}")
            return False
        version = obj.get("schema_version")
        if version != STATUS_SCHEMA_VERSION:
            logger.warning(
                f"shard {entry.label}: status object schema_version {version!r} != "
                f"{STATUS_SCHEMA_VERSION} (this client's); reading it as v"
                f"{STATUS_SCHEMA_VERSION} — a newer worker may carry keys this "
                f"client ignores (issue #327)"
            )
        attempt_id = obj.get("attempt_id")
        if not attempt_id:
            logger.warning(
                f"shard {entry.label}: status object carries no attempt_id; "
                f"treating it as a new attempt (issue #327)"
            )
        elif attempt_id in entry.consumed:
            return False  # already acted on (a not-yet-overwritten prior attempt)
        else:
            entry.consumed.add(attempt_id)
        result = self._result(
            entry,
            status_code=obj.get("status_code"),
            body=obj.get("body") or {},
            error=obj.get("error"),
        )
        if obj.get("status") in ("ok", "no_data"):
            entry.future.set_result(result)
        else:
            self._worker_failed(entry, result)
        return True

    def _result(self, entry: _ShardEntry, *, status_code, body, error, outcome=None) -> dict:
        """The v1-shaped per-shard result dict (see ``_poll_lambda_result``)."""
        start = entry.first_dispatched_at or self._clock()
        result = {
            "shard_key": entry.shard_key,
            "status_code": status_code,
            "body": body,
            "wall_time": self._clock() - start,
            "lambda_duration": (body or {}).get("duration_s", 0),
            "error": error,
            "retries": entry.attempts - 1,
            "timeout": False,
            "granule_count": entry.granule_count,
        }
        if outcome is not None:
            result["outcome"] = outcome
        return result

    # -- the three retry classes (see the class docstring) --------------------

    def _worker_failed(self, entry: _ShardEntry, result: dict) -> None:
        """Class (a): a ``failed`` status object is TERMINAL — never re-fired.

        The object exists, so the worker ran to completion and reported its own
        error: deterministic for this shard, exactly the case issue #119
        excludes from retry on the sync transport. Re-firing it would bill a
        second full execution for a guaranteed second failure.
        """
        self._resolve_failure(entry, result)

    def _drop_failed(self, entry: _ShardEntry, result: dict) -> None:
        """Class (b): a status-less shard re-fires at most :data:`_MAX_DROP_REFIRES`.

        The one class where a re-fire can actually succeed (an invoke aged out
        of the async queue never ran), bounded at one so the worst case is two
        billed executions rather than ``max_retries`` of them (issue #151).
        ``max_retries <= 1`` opts out entirely, keeping the knob's "no retries"
        meaning.
        """
        if (
            entry.dispatch is not None
            and entry.drop_refires < _MAX_DROP_REFIRES
            and entry.attempts < self._max_retries
        ):
            logger.warning(
                f"shard {entry.label}: no status object (attempt {entry.attempts}); "
                f"re-firing once — a dropped invoke can still succeed, a dead "
                f"worker will not (bounded at {_MAX_DROP_REFIRES} re-fire, issue #151)"
            )
            with self._lock:
                entry.drop_refires += 1
                entry.queued = True
            return
        self._resolve_failure(entry, result)

    def _invoke_faulted(self, entry: _ShardEntry, result: dict) -> None:
        """Class (c): the invoke call itself faulted — backed-off retry.

        Re-arms the entry with a not-before stamp ``2**attempts`` seconds out
        (capped) instead of sleeping: the poller thread has to stay free for
        LISTs, and an unstamped re-queue would spend the whole budget in the
        same ``_dispatch_up_to_window`` pass.
        """
        if entry.dispatch is not None and entry.attempts < self._max_retries:
            delay = min(2.0**entry.attempts, _RETRY_BACKOFF_CAP_S)
            logger.warning(
                f"shard {entry.label}: attempt {entry.attempts} failed "
                f"({result.get('error')}); re-dispatching in {delay:.0f}s "
                f"({self._max_retries - entry.attempts} left)"
            )
            with self._lock:
                entry.retry_not_before = self._clock() + delay
                entry.queued = True
            return
        self._resolve_failure(entry, result)

    def _resolve_failure(self, entry: _ShardEntry, result: dict) -> None:
        """Terminal failure: the caller's policy, defaulting to ``set_result``."""
        if self._on_failed is not None:
            self._on_failed(entry, result)
        else:
            entry.future.set_result(result)

    def _dropped(self, entry: _ShardEntry) -> None:
        """No status object by the drop deadline: the distinct failed-unknown outcome.

        The deadline is function timeout + the fleet's 60 s max event age +
        margin, so a shard reaching it either had its Event invoke silently
        dropped under backpressure (issue #327 amendment (a')) or died before
        its always-on status write — indistinguishable from here, hence the
        dedicated outcome. Fault class (b): one bounded re-fire.
        """
        result = self._result(
            entry,
            status_code=None,
            body={},
            error=(
                f"failed-unknown: no status object within {self._drop_timeout_s:.0f}s "
                f"(function timeout + 60s max event age + margin) — the Event invoke "
                f"was dropped under backpressure, or the worker died before its "
                f"status write (check CloudWatch)"
            ),
            outcome="failed-unknown",
        )
        self._drop_failed(entry, result)
