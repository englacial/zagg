"""
AWS Lambda handler for processing data by morton cell.

This is an AWS-specific wrapper around the cloud-agnostic processing module.

Event payload (default / process mode):
{
    "chunk_idx": int,
    "shard_key": int,           # grid-agnostic shard identifier
    "parent_order": int,        # HEALPix only (omit for other grids)
    "child_order": int,         # HEALPix only (omit for other grids)
    "granule_urls": [str, ...],
    "store_path": str,          # e.g. "s3://bucket/prefix.zarr"
    "s3_credentials": {         # creds for reading NSIDC source data
        "accessKeyId": str,
        "secretAccessKey": str,
        "sessionToken": str
    },
    "output_credentials": {     # OPTIONAL -- creds for writing the output store;
        "accessKeyId": str,     #   omit to use the execution role (in-account).
        "secretAccessKey": str, #   Supply to write an external/S3-compatible
        "sessionToken": str,    #   target (e.g. source.coop). sessionToken,
        "endpointUrl": str,     #   endpointUrl, and region are optional.
        "region": str
    },
    "config": dict (optional, pipeline config as dict),
    "aoi_payload": list (optional, issue #101) -- this shard's strict-AOI mask
        payload (a compact MOC for HEALPix / in-AOI cell ids for rectilinear).
        Forwarded to ``process_shard`` so the worker fills the ``aoi_mask``
        column; absent when ``output.aoi_mask`` is off (the column is not
        allocated), keeping the flag-off event and outputs byte-identical.
    "result_url": str (optional, issue #151) -- where to ALSO write this
        invocation's response envelope as JSON (e.g.
        "s3://bucket/out.zarr.status/<run_id>/<shard_label>.json", where the
        label is the decimal morton string for HEALPix -- issue #199). Set by
        the orchestrator's async dispatch (InvocationType="Event", which discards
        the return value); the orchestrator polls this object instead of
        holding a synchronous connection open while the shard runs. Written
        with the output-store credentials. Absent -> no write, and the event
        and behavior are byte-identical to the synchronous path.
}

Setup mode (creates the zarr template once before per-cell fan-out; for a
hive-layout config -- output.store_layout: hive, issue #199 -- it writes the
morton_hive.json manifest instead, and each process-mode worker emits its own
leaf template):
{
    "mode": "setup",
    "store_path": str,
    "parent_order": int,        # HEALPix fallback; config.output.grid wins
    "n_parent_cells": int,      # OPTIONAL -- dense layout only (populated count)
    "overwrite": bool,
    "config": dict,             # single source of truth: child_order, chunk_inner,
                                #   layout, store_layout, and grid type all come from here
    "dataset": dict (optional, hive only) -- {"short_name", "version"} identity
        block for the manifest, sourced from the ShardMap metadata by the
        orchestrator (matching the local dispatcher). Absent on flat runs.
    "output_credentials": dict (optional, same shape as process mode),
}

Finalize mode (consolidates zarr metadata after all cells complete):
{
    "mode": "finalize",
    "store_path": str,
    "output_credentials": dict (optional, same shape as process mode),
}

Extract mode (chunk-boundary geometry extraction, issue #148 — one parquet per
granule under an S3 prefix; a batch of granules per invocation for the fan-out):
{
    "mode": "extract",
    "granule_urls": [str, ...],
    "output_prefix": str,       # e.g. "s3://bucket/boundaries/" (execution role writes)
    "s3_credentials": dict,     # same shape as process mode (NSIDC read side)
    "driver": "s3" | "https" (optional, default "s3"),
    "block_chunks": int (optional, chunks per streamed read),
}

Process-event mode (the temporal/event pipeline worker -- issue #12, Phase 7b):
{
    "mode": "process_event",
    "event_key": str,           # identifier for this event row
    "event_mask_uri": str,      # s3:// (or local) URI of the event mask
                                #   DataArray (one variable, time x lat x lon)
    "collection_uris": {        # {collection_name: uri or [uris]} the specs
        "merra2_slv": "s3://.../merra2_slv.zarr", ...  #  read; a list (multi-
    },                          #  granule event) concats along time
    "static_uris": {            # {static_name: uri}, e.g. ais_mask / climatology
        "ais_mask": "s3://.../ais_mask.nc", ...
    },
    "store_path": str,          # s3:// (or local) tabular output, e.g. .parquet
    "config": dict,             # temporal pipeline config (specs etc.)
    "s3_credentials": dict (optional),     # read creds for the SOURCE collections
                                           #   only (issue #223)
    "input_credentials": dict | "unsigned" (optional),  # consumer-owned mask +
                                           #   statics channel: explicit creds,
                                           #   "unsigned" (public bucket), or
                                           #   absent -> execution role
    "output_credentials": dict (optional), # write creds for the tabular store
    "return_results": bool (optional),  # fan-out driver mode (issue #12 Phase
                                        #   8): skip the worker-side tabular
                                        #   write, return the flattened result
                                        #   values in the response body (and
                                        #   via "result_url" on Event invokes);
                                        #   "store_path" is then optional
}

This mirrors the local ``zagg.runner.TemporalStrategy``: load the event's
collections + static_data, run ``zagg.temporal.process_event`` for one event,
and write the single flattened result row to the tabular store. One event per
worker, fanned out the same way per-cell spatial work is.

Setup and finalize exist so callers without direct S3 write access to the
output bucket (e.g. cross-account JupyterHub orchestrators) can run the
full pipeline using only lambda:InvokeFunction.

Every per-unit response envelope (process / process_event, all status
branches, including the ``result_url`` mirror) additionally carries container
telemetry -- ``container_cold`` / ``container_generation`` / ``rss_start_mb``
/ ``sandbox_id`` / ``container_init_ts`` (issue #171; see
``_container_telemetry``) -- and after a successful async result mirror the
worker may self-recycle a bloated sandbox (``_maybe_self_recycle``, gated by
the ``ZAGG_RECYCLE_RSS_MB`` / ``ZAGG_RECYCLE_MAX_INVOCATIONS`` function env
vars).
"""

import ctypes
import gc
import json
import logging
import os
import resource
import threading
import time
from typing import Any, Dict, Optional

from zarr import open_group
from zarr.errors import GroupNotFoundError

# Import cloud-agnostic processing
from zagg.config import get_handoff, get_store_layout, load_config_from_dict
from zagg.processing import (
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
    write_shard_to_zarr,
)
from zagg.store import open_store

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Container-lifetime telemetry (issue #171, the detect-and-report half of the
# PR #172 plan). Module globals persist across warm invocations of the same
# sandbox: the import timestamp marks container init (imports run exactly once
# per sandbox), and the counter counts invocations served -- the sandbox's
# "generation". Together with per-invocation start RSS they make the #169
# warm-container RSS ratchet (959 -> 1650 -> 2029 -> OOM across four fleet
# runs on the same 9 sandboxes) visible in every result envelope instead of
# requiring CloudWatch forensics.
_CONTAINER_INIT_TS = time.time()
_INVOCATIONS_SERVED = 0
# Recycle budget (issue #177), counted separately from the true generation
# above: only recycle-eligible (async ``result_url``) invocations are billed
# against ``ZAGG_RECYCLE_MAX_INVOCATIONS``. A synchronous setup/
# finalize invoke still warms the sandbox (the generation keeps counting it,
# and telemetry keeps reporting it) but must not consume the worker budget --
# MAX_INVOCATIONS=1 means "one heavy async invocation per container", not
# "one invocation of any kind".
_ASYNC_INVOCATIONS_SERVED = 0


def _container_telemetry() -> Dict[str, Any]:
    """Per-invocation container-telemetry block (issue #171).

    Called exactly once per invocation, at handler entry: increments the
    sandbox's invocations-served counter and snapshots the *start* RSS --
    the ratchet signal (a fresh container starts near baseline; a dirty one
    starts near the previous invocation's retained RSS). ``container_cold``
    is ``generation == 1`` by construction. ``sandbox_id`` is the CloudWatch
    log-stream name, unique per sandbox, so the orchestrator can group
    per-shard results by physical container. Off Linux ``rss_start_mb`` is
    None (no ``/proc/self/status``), mirroring the #141 sampler fallback.
    """
    global _INVOCATIONS_SERVED
    _INVOCATIONS_SERVED += 1
    start_kib = _read_vmrss_kib()
    return {
        "container_cold": _INVOCATIONS_SERVED == 1,
        "container_generation": _INVOCATIONS_SERVED,
        "rss_start_mb": start_kib / 1024.0 if start_kib is not None else None,
        "sandbox_id": os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
        "container_init_ts": _CONTAINER_INIT_TS,
    }


def _attach_container_telemetry(
    response: Dict[str, Any], telemetry: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge the telemetry block into a per-unit response body (issue #171).

    The body is a JSON string (Lambda proxy shape); parse-merge-redump at the
    dispatcher gives one seam covering both per-unit handlers (spatial process,
    temporal process_event) and every status branch (200/400/500), so the
    orchestrator can stratify failures -- e.g. an OOM'd generation-4 shard --
    by container state, not just successes. A non-dict/undecodable body passes
    through untouched (never turn a valid error envelope into a crash).
    """
    try:
        body = json.loads(response.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return response
    if not isinstance(body, dict):
        return response
    body.update(telemetry)
    return {**response, "body": json.dumps(body)}


# Injectable exit seam (issue #171): module-level so tests can monkeypatch it.
# ``os._exit`` (not sys.exit) is deliberate -- the sandbox is being discarded,
# not shut down gracefully, and the exit must not be catchable en route.
_exit = os._exit


def _recycle_limit(name: str) -> float:
    """Read one self-recycle knob from the environment; 0.0 == disabled.

    Absent, empty, "0", or non-numeric (logged) all disable the check, so a
    stack deployed without the template.yaml defaults behaves exactly as
    before this feature existed.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        logger.warning(f"{name}={raw!r} is not numeric; recycle check disabled")
        return 0.0


def _maybe_self_recycle() -> None:
    """Destroy this sandbox when it is too bloated to trust (issue #171).

    Called ONLY after the invocation's result envelope was successfully
    mirrored to its ``result_url`` (the issue #151/#153 async channel): the
    orchestrator polls that S3 object, not the Lambda response, so once the
    mirror has landed the invocation is operationally complete and exiting
    loses nothing; ``MaximumRetryAttempts: 0`` (template.yaml) guarantees the
    cosmetically "failed" invocation is never re-driven. Never called on the
    synchronous path, where exiting would lose the response.

    Two independent knobs (function env vars with template.yaml defaults;
    absent/empty/0 disables that check):

    - ``ZAGG_RECYCLE_RSS_MB`` -- recycle when current RSS is at/over this
      many MB. The #169 ratchet retained ~700-1100 MB per heavy invocation
      against a 2047 MB cap, so the template's 1400 catches a dirty sandbox
      after roughly one heavy retention while leaving the triggering
      invocation ~650 MB of headroom to complete first.
    - ``ZAGG_RECYCLE_MAX_INVOCATIONS`` -- cap on recycle-eligible (async)
      invocations served, NOT the raw container generation (issue #177: the
      runner's synchronous setup invoke warms a sandbox first, and counting
      it made MAX_INVOCATIONS=1 deliver generation-2 workers while telemetry
      read as if recycling had failed). Template default 1: recycle after
      every async invocation, the cold-every-time posture; raise it for a
      belt-and-suspenders cap over retention modes the RSS read misses --
      and the only check that fires off-Linux, where RSS reads are None.

    Emits one CloudWatch-searchable line (``ZAGG_SELF_RECYCLE ...``) before
    exiting so dashboards can split intentional recycles from real crashes
    (metric-filter note in docs/deployment/lambda.md). The line carries both
    the async budget spent and the true container generation.

    Pure check: the async budget itself is billed at the dispatcher (every
    ``result_url`` invocation, mirror success or not), so a failed mirror --
    which skips this call -- still burns the budget (issue #177 review fold).
    """
    rss_limit = _recycle_limit("ZAGG_RECYCLE_RSS_MB")
    gen_limit = _recycle_limit("ZAGG_RECYCLE_MAX_INVOCATIONS")
    kib = _read_vmrss_kib()
    rss_mb = kib / 1024.0 if kib is not None else None
    async_served = _ASYNC_INVOCATIONS_SERVED
    generation = _INVOCATIONS_SERVED
    if rss_limit > 0 and rss_mb is not None and rss_mb >= rss_limit:
        threshold = rss_limit
    elif gen_limit > 0 and async_served >= gen_limit:
        threshold = gen_limit
    else:
        return
    rss_repr = f"{rss_mb:.0f}" if rss_mb is not None else "n/a"
    logger.info(
        f"ZAGG_SELF_RECYCLE rss_mb={rss_repr} async_served={async_served} "
        f"generation={generation} threshold={threshold:g}"
    )
    _exit(0)


def _max_memory_mb() -> float:
    """Peak resident set size of this worker in MB (issue #120).

    ``ru_maxrss`` is a high-water mark over the whole process, so reading it at
    the end of the invocation captures read+index+aggregate+write. On Linux
    (the Lambda runtime) the field is in kibibytes; tracks CloudWatch's "Max
    Memory Used" closely.
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _read_vmrss_kib() -> Optional[int]:
    """Current resident set size in KiB from ``/proc/self/status``, or None off Linux.

    ``VmRSS`` is the process's *current* RSS (not a high-water mark), reported in
    KiB. Returns None when ``/proc/self/status`` is absent/unreadable (macOS/dev),
    so callers fall back to ``ru_maxrss``.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])  # "VmRSS:\t   12345 kB"
    except (OSError, ValueError, IndexError):
        return None
    return None


class _PeakRSSSampler:
    """Sample THIS invocation's peak current RSS on a daemon thread (issue #141).

    ``ru_maxrss`` is a per-*process* high-water mark, so on a warm/reused Lambda
    container it reports the max over every prior invocation, not this one --
    making ``max_memory_mb`` untrustworthy on warm containers (it can only ever
    rise, so it also can't reflect #140's teardown reclaim). This polls the
    *current* RSS (``VmRSS``) at a fixed interval and records the max while it
    runs, so the reported peak reflects the current invocation. #140's teardown
    ``malloc_trim`` returns current RSS to ~baseline between invokes, so a warm
    container starts each invocation low and the sampled peak is clean.

    Off Linux (no ``/proc/self/status``) it degrades to a no-op and ``peak_mb`` is
    None, so the caller falls back to ``ru_maxrss``. Sampling overhead is one small
    file read per tick -- negligible next to read/aggregate/write.
    """

    def __init__(self, interval_s: float = 0.05):
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._peak_kib = 0
        self._available = _read_vmrss_kib() is not None

    def start(self) -> "_PeakRSSSampler":
        if self._available:
            self._thread = threading.Thread(target=self._run, name="peak-rss", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _run(self) -> None:
        # Sample immediately, then every interval until stopped.
        while True:
            cur = _read_vmrss_kib()
            if cur is not None and cur > self._peak_kib:
                self._peak_kib = cur
            if self._stop.wait(self._interval_s):
                return

    @property
    def peak_mb(self) -> Optional[float]:
        """Peak sampled RSS in MB, or None if unavailable (fall back to ru_maxrss)."""
        if not self._available or self._peak_kib == 0:
            return None
        return self._peak_kib / 1024.0


def _reclaim_memory() -> None:
    """Reclaim Python objects at invocation teardown (issues #139, #143).

    Lambda reuses warm containers, so a subsequent invocation on a warm
    container starts near the *previous* invocation's RSS and can OOM
    (``Max Memory Used`` is per-container-lifetime). The reliable fix for the
    glibc-arena retention that drives this is the allocator env vars set on the
    function itself (``MALLOC_ARENA_MAX``/``MALLOC_TRIM_THRESHOLD_`` in
    ``template.yaml``, issue #143) -- those take effect at libc init and flatten
    warm-container growth to ~0. The ``malloc_trim(0)`` below is retained as a
    harmless secondary: it only trims the top of the main arena, so on its own
    it does *not* reliably return the retained secondary-arena numpy blocks
    (hence the env-var fix supersedes it as the primary mechanism), but the
    ``gc.collect()`` still reclaims unreachable Python objects.

    Called once per invocation (O(heap) -- negligible next to read/aggregate/
    write) and behavior-neutral: it only frees memory the invocation is done
    with. Guarded so it is a no-op off glibc (macOS/dev has no ``libc.so.6``,
    non-glibc libcs may lack ``malloc_trim``) -- it never raises.
    """
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError):
        # No glibc (no libc.so.6) or no malloc_trim symbol -- nothing to trim.
        logger.debug("malloc_trim unavailable; skipping heap reclaim", exc_info=True)


def _output_store_kwargs(event: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve open_store kwargs for the output store from an event.

    Symmetric to the read side: an optional ``output_credentials`` block
    (camelCase ``accessKeyId``/``secretAccessKey``/``sessionToken``, plus
    optional ``endpointUrl``/``region``) injects explicit write credentials.
    When absent, falls back to the execution role and the AWS region env var.

    Returns
    -------
    dict
        Keyword arguments for ``open_store`` (always includes ``region``;
        ``credentials`` and ``endpoint_url`` only when supplied).

    Raises
    ------
    ValueError
        If ``output_credentials`` is present but missing required keys.
    """
    region = os.environ.get("AWS_REGION", "us-west-2")
    creds = event.get("output_credentials")
    if not creds:
        return {"region": region}
    missing = [k for k in ("accessKeyId", "secretAccessKey") if k not in creds]
    if missing:
        raise ValueError(f"output_credentials missing keys: {', '.join(missing)}")
    kwargs: Dict[str, Any] = {
        "region": creds.get("region", region),
        "credentials": creds,
    }
    if creds.get("endpointUrl"):
        kwargs["endpoint_url"] = creds["endpointUrl"]
    return kwargs


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Dispatch on event mode.

    Default ``mode`` (or no mode) runs per-cell processing. ``mode="setup"``
    creates the zarr template; ``mode="finalize"`` consolidates metadata;
    ``mode="coverage"`` writes the store-root ``coverage.moc`` (issue #200);
    ``mode="extract"`` extracts chunk-boundary geometry parquets (issue #148);
    ``mode="process_event"`` runs the temporal/event worker (issue #12).
    """
    # Count EVERY invocation toward the sandbox's generation (issue #171): a
    # setup/finalize/extract invoke warms the container just like a shard does,
    # so the next shard on this sandbox is genuinely generation N+1. Snapshot
    # start RSS here, before any per-unit work inflates it. (The recycle
    # budget is billed separately, per async invocation -- issue #177.)
    telemetry = _container_telemetry()
    mode = event.get("mode", "process")
    if mode == "setup":
        return _handle_setup(event)
    if mode == "finalize":
        return _handle_finalize(event)
    if mode == "coverage":
        return _handle_coverage(event)
    # Extract mode returns directly: the result_url mirror below is for the
    # per-unit fan-out handlers (spatial process, temporal process_event) only.
    if mode == "extract":
        return _handle_extract(event, context)
    if mode in ("process_event", "temporal", "event"):
        response = _handle_process_event(event)
    else:
        response = _handle_process(event, context)
    # Container telemetry rides in every per-unit envelope (issue #171) -- the
    # setup/finalize/extract bodies stay byte-identical (their consumers don't
    # aggregate container state).
    response = _attach_container_telemetry(response, telemetry)
    # Async result channel (issue #151): on an Event invoke the return value is
    # discarded, so mirror the response envelope to the orchestrator-supplied
    # result_url for it to poll. Covers every branch (200 / 400 / 500) of both
    # per-unit handlers (spatial process, temporal process_event -- #12 Phase 8).
    if event.get("result_url"):
        # Bill this async invocation against the recycle budget (issue #177)
        # BEFORE the mirror-success gate: the invocation was served either
        # way, so a failed mirror must not stretch the sandbox's budget (the
        # pre-#177 generation counter counted it too). Only the recycle
        # itself stays gated on the mirror landing.
        global _ASYNC_INVOCATIONS_SERVED
        _ASYNC_INVOCATIONS_SERVED += 1
        mirrored = _write_result(event["result_url"], response, event)
        # Self-recycle strictly AFTER a successful result mirror (issue #171):
        # the orchestrator polls the result object, not the Lambda response
        # (#151/#153), so at this point the invocation is complete from the
        # run's perspective and destroying a bloated sandbox loses nothing.
        # Sync invokes never reach here (no result_url) -- exiting would lose
        # their response -- and a failed mirror skips the recycle (the shard
        # is recorded failed at the poll deadline; don't also churn the
        # sandbox on what may be a transient S3 fault).
        if mirrored:
            _maybe_self_recycle()
    return response


def _write_result(result_url: str, response: Dict[str, Any], event: Dict[str, Any]) -> bool:
    """Write the response envelope to ``result_url`` as JSON (issue #151).

    Uses the same credentials/endpoint resolution as the output store. Never
    raises: on failure the orchestrator's poll times out and records the shard
    as failed, and the cause lands here in CloudWatch. Returns True only when
    the write landed -- the self-recycle gate (issue #171) keys on it.
    """
    import obstore

    from zagg.store import open_object_store

    try:
        prefix, key = result_url.rsplit("/", 1)
        store = open_object_store(prefix, **_output_store_kwargs(event))
        obstore.put(store, key, json.dumps(response).encode())
        logger.info(f"Wrote async result to {result_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to write async result to {result_url}: {e}")
        return False


def _handle_extract(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Chunk-boundary geometry extraction (issue #148).

    Runs both as a mode of the process function (incremental updates ride the
    existing deployment) and on the dedicated ``ExtractFn`` twin in
    ``template.yaml`` (full-archive runs get their own concurrency pool) —
    same code zip, layer, role, memory, and the shared ``Timeout``, so the
    fan-out is just many ``mode="extract"`` invocations over granule batches
    against either function. The
    body lives in :mod:`zagg.catalog.extract` (layer-safe: h5coro + pandas +
    fastparquet only); per-granule ``wall_s`` in the response feeds the
    full-catalog cost estimate the issue asks for.
    """
    from zagg.catalog.extract import run_extraction

    t0 = time.time()
    missing = [p for p in ("granule_urls", "output_prefix", "s3_credentials") if p not in event]
    if missing:
        error_msg = f"Missing required parameters: {', '.join(missing)}"
        logger.error(error_msg)
        return {"statusCode": 400, "body": json.dumps({"error": error_msg, "mode": "extract"})}

    driver = event.get("driver", "s3")
    s3_creds = event["s3_credentials"]
    if driver == "https":
        # Same fail-fast posture as the s3 branch: a missing token would pass
        # the whole creds dict downstream as the bearer token and burn the
        # batch on 401s instead of returning a 400 here.
        if "edl_token" not in s3_creds:
            error_msg = "Missing s3_credentials keys: edl_token (required for driver='https')"
            logger.error(error_msg)
            return {
                "statusCode": 400,
                "body": json.dumps({"error": error_msg, "mode": "extract"}),
            }
        credentials = s3_creds["edl_token"]
    else:
        # Mirror process mode's credential-shape gate: missing keys would map to
        # present-but-None kwargs, silently falling back to the execution role
        # and burning the whole batch on NSIDC 403s instead of failing fast.
        required_cred_keys = ["accessKeyId", "secretAccessKey", "sessionToken"]
        missing_cred_keys = [k for k in required_cred_keys if k not in s3_creds]
        if missing_cred_keys:
            error_msg = f"Missing s3_credentials keys: {', '.join(missing_cred_keys)}"
            logger.error(error_msg)
            return {
                "statusCode": 400,
                "body": json.dumps({"error": error_msg, "mode": "extract"}),
            }
        credentials = {
            "aws_access_key_id": s3_creds.get("accessKeyId"),
            "aws_secret_access_key": s3_creds.get("secretAccessKey"),
            "aws_session_token": s3_creds.get("sessionToken"),
        }

    try:
        kwargs = {}
        if "block_chunks" in event:
            kwargs["block_chunks"] = int(event["block_chunks"])
        results = run_extraction(
            event["granule_urls"],
            event["output_prefix"],
            driver=driver,
            credentials=credentials,
            **kwargs,
        )
        n_failed = sum(1 for r in results if not r["ok"])
        body = {
            "mode": "extract",
            "granules": results,
            "granule_count": len(results),
            "failed": n_failed,
            "duration_s": round(time.time() - t0, 3),
            "max_memory_mb": _max_memory_mb(),
        }
        return {"statusCode": 200 if n_failed == 0 else 500, "body": json.dumps(body)}
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "extract"})}


def _handle_setup(event: Dict[str, Any]) -> Dict[str, Any]:
    """Create the zarr template at ``event['store_path']``.

    For a hive-layout config (issue #199 phase 3) template time writes ONLY
    the ``morton_hive.json`` manifest — no global zarr template exists (zero
    metadata above the leaves, D5); each worker emits its own leaf template.
    The optional ``dataset`` event key carries the manifest's identity block
    (the orchestrator sources it from the ShardMap metadata, same as the local
    path). The flat path below is byte-identical to before, bar one addition:
    the success body now ECHOES the layout it acted on (``"layout"``) — a
    stale deployment without the hive branch returns the old echo-less body,
    which the dispatcher rejects for hive runs instead of silently letting old
    workers write a flat store at the hive root (review finding, PR #205).
    """
    from zagg.grids import from_config

    logger.info(f"Setup mode: creating template at {event.get('store_path')}")
    try:
        config = load_config_from_dict(event["config"])
        if get_store_layout(config) == "hive":
            from zagg.hive import build_manifest, ensure_manifest

            grid = from_config(config, parent_order=event.get("parent_order"))
            ensure_manifest(
                event["store_path"],
                build_manifest(grid, dataset=event.get("dataset")),
                overwrite=event.get("overwrite", False),
                **_output_store_kwargs(event),
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"ok": True, "mode": "setup", "layout": "hive"}),
            }
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        # Build the grid exactly as the worker does (from_config), so the
        # template's chunk structure can't drift from what workers write. The
        # old hand-built HEALPix branch dropped chunk_inner, under-chunking the
        # template at parent_order while workers wrote finer chunk_inner block
        # indices -> "block index out of bounds" (issue #99). from_config reads
        # chunk_inner + layout from the config. n_parent_cells is inert unless
        # the config selects layout: dense, where it threads through as
        # populated_shards (only its count matters for emit_template).
        populated = (
            list(range(event["n_parent_cells"]))
            if event.get("n_parent_cells") is not None
            else None
        )
        grid = from_config(
            config,
            parent_order=event.get("parent_order"),
            populated_shards=populated,
        )
        grid.emit_template(store, overwrite=event.get("overwrite", False))
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "mode": "setup", "layout": "flat"}),
        }
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "setup"})}


def _handle_finalize(event: Dict[str, Any]) -> Dict[str, Any]:
    """Consolidate zarr metadata for the store at ``event['store_path']``."""
    from zarr import consolidate_metadata

    logger.info(f"Finalize mode: consolidating metadata at {event.get('store_path')}")
    try:
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        consolidate_metadata(store, zarr_format=3)
        return {"statusCode": 200, "body": json.dumps({"ok": True, "mode": "finalize"})}
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "finalize"})}


def _handle_coverage(event: Dict[str, Any]) -> Dict[str, Any]:
    """Write/union the store-root ``coverage.moc`` (issue #200 phase 3).

    Posted fire-and-forget (``InvocationType="Event"``) by the dispatcher at
    end of run: the orchestrator can compute the shard-order MOC but cannot
    PUT to S3, so the SERIALIZED envelope rides in the event (bounded by
    construction — see the dispatch-site comment in ``zagg.runner``) and the
    worker GET-unions-PUTs one root object. Nobody reads this response on
    the Event invoke; errors are logged and fail open — the root MOC is a
    regenerable cache (D9): readers degrade to the sweep MOC or the walk,
    never to wrong answers.
    """
    from zagg.hive import write_root_coverage

    logger.info(f"Coverage mode: writing root coverage.moc at {event.get('store_path')}")
    try:
        merged = write_root_coverage(
            event["store_path"], event["coverage"], **_output_store_kwargs(event)
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"ok": True, "mode": "coverage", "ranges": len(merged.get("ranges", []))}
            ),
        }
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "coverage"})}


def _json_scalar(v: Any) -> Any:
    """Coerce one result value to a JSON-safe scalar (numpy float -> float)."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def _handle_process_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Temporal/event worker: one event -> one tabular row (issue #12, Phase 7b).

    Mirrors the local ``zagg.runner.TemporalStrategy``: build the specs from the
    config, load the event mask + collections + static_data from S3, run
    ``zagg.temporal.process_event`` for the single event, and write the
    flattened result row to the tabular ``store_path``.
    """
    from zagg import registry as zagg_registry
    from zagg.config import collection_options as _collection_options
    from zagg.output import write_tabular
    from zagg.temporal import _input_channel, open_dataset, process_event, specs_from_config

    event_key = event.get("event_key")
    logger.info(f"process_event mode: event {event_key!r}")
    start_time = time.time()
    try:
        # The fan-out driver (issue #12, Phase 8) sets return_results=True: the
        # flattened result values ride back in the response body (async: via
        # result_url) and the driver writes the single tabular object once, so
        # N workers never race a shared store_path -- which is then optional.
        return_results = bool(event.get("return_results"))
        required = ["event_key", "event_mask_uri", "config"]
        if not return_results:
            required.append("store_path")
        missing = [p for p in required if p not in event]
        if missing:
            error_msg = f"Missing required parameters: {', '.join(missing)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        config = load_config_from_dict(event["config"])
        specs = specs_from_config(config)

        # Two read channels (issue #223): s3_credentials covers the SOURCE
        # collections it was fetched for (e.g. GES DISC STS creds — scoped, so
        # signing other buckets with them is denied cross-account);
        # input_credentials covers the consumer-owned mask + statics (dict |
        # "unsigned" for public buckets | absent -> execution role).
        # output_credentials writes the tabular store.
        read_creds = event.get("s3_credentials") or None
        in_creds, in_unsigned = _input_channel(event.get("input_credentials"))
        region = os.environ.get("AWS_REGION", "us-west-2")

        event_mask = open_dataset(
            event["event_mask_uri"], credentials=in_creds, region=region, unsigned=in_unsigned
        )
        # The event mask is a single-variable file; index that variable so masks
        # operate on a DataArray (mirrors the local events= contract).
        mask_vars = list(getattr(event_mask, "data_vars", []))
        if mask_vars:
            event_mask = event_mask[mask_vars[0]]

        # The reader resolves by name (issue #213 Phase 3): only names present
        # in the layer's registry are reachable -- the payload stays pure data.
        reader = zagg_registry.get_reader((config.data_source or {}).get("reader") or "xarray_s3")
        collections, static_data = reader(
            event.get("collection_uris", {}),
            event.get("static_uris", {}),
            credentials=read_creds,
            region=region,
            collection_options=_collection_options(config),
            input_credentials=event.get("input_credentials"),
        )

        results, meta = process_event(event_key, event_mask, collections, specs, static_data)

        if return_results:
            output_path = None
        else:
            out_creds = event.get("output_credentials") or None
            out_endpoint = out_creds.get("endpointUrl") if out_creds else None
            out_region = (out_creds or {}).get("region", region)
            # ``config.output["format"]`` may be absent (None); ``write_tabular``
            # then infers parquet/csv from the store_path suffix -- the same effect
            # the runner gets by passing ``output_format(config)`` (which defaults to
            # ``zarr`` and is filtered out before this temporal write).
            output_path = write_tabular(
                [{"event_key": event_key, "results": results, "meta": meta}],
                event["store_path"],
                output_format=config.output.get("format"),
                credentials=out_creds,
                endpoint_url=out_endpoint,
                region=out_region,
            )

        body = {
            "ok": True,
            "mode": "process_event",
            "event_key": event_key,
            "timesteps_processed": meta.get("timesteps_processed"),
            "output_path": output_path,
            "duration_s": round(time.time() - start_time, 2),
            "max_memory_mb": _max_memory_mb(),
        }
        if return_results:
            # JSON-safe scalars (numpy floats don't json.dumps), mirroring the
            # antarctic_AR_dataset worker's float-cast return contract. A value
            # a registered custom reducer returns that isn't float-castable
            # (e.g. a label string) passes through unchanged, matching what the
            # direct-write path hands write_tabular.
            body["results"] = {k: _json_scalar(v) for k, v in results.items()}
            # Full per-event metadata (n_specs/collections/timesteps) so the
            # driver-side rows match the local backend's row shape exactly.
            body["meta"] = meta
        logger.info(json.dumps({"event_type": "process_event_complete", **body}))
        return {"statusCode": 200, "body": json.dumps(body)}
    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "mode": "process_event", "event_key": event_key}),
        }


def _handle_process(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Per-cell processing handler (the original lambda_handler body)."""
    # Log the event for debugging
    logger.info("=" * 70)
    logger.info("Lambda invocation started")
    logger.info(f"Request ID: {context.aws_request_id}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Memory: {context.memory_limit_in_mb} MB")
    logger.info(f"Timeout: {context.get_remaining_time_in_millis() / 1000:.0f}s")
    logger.info("=" * 70)

    # Log structured event data
    logger.info(
        json.dumps(
            {
                "event_type": "lambda_invocation",
                "shard_key": event.get("shard_key"),
                "granule_count": len(event.get("granule_urls", [])),
                "child_order": event.get("child_order"),
                "request_id": context.aws_request_id,
                "chunk_idx": event.get("chunk_idx"),
            }
        )
    )

    # Per-invocation peak-RSS sampler (issue #141): sample current VmRSS on a
    # daemon thread for the whole invocation so ``max_memory_mb`` reflects THIS
    # run, not the warm container's lifetime high-water. Stopped in ``finally``.
    rss_sampler = _PeakRSSSampler().start()

    # Per-invocation CPU baseline (issue #180 phase 3): ``os.times()`` is
    # process-cumulative (like ``ru_maxrss``), so snapshot at entry and diff at
    # the telemetry stamp below for THIS invocation's user+sys seconds.
    cpu_t0 = os.times()

    try:
        # Validate required parameters. ``child_order`` is HEALPix-specific and
        # only required once the grid is known to be HEALPix (checked below);
        # ``parent_order`` is forwarded by the orchestrator for every grid (None
        # for non-HEALPix), so its key is always present.
        required_params = [
            "shard_key",
            "parent_order",
            "granule_urls",
            "store_path",
            "s3_credentials",
        ]
        missing_params = [p for p in required_params if p not in event]

        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        # Validate s3_credentials structure
        s3_creds = event["s3_credentials"]
        required_cred_keys = ["accessKeyId", "secretAccessKey", "sessionToken"]
        missing_cred_keys = [k for k in required_cred_keys if k not in s3_creds]
        if missing_cred_keys:
            error_msg = f"Missing s3_credentials keys: {', '.join(missing_cred_keys)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        # Load pipeline config if provided, otherwise use default
        config = None
        if "config" in event:
            config = load_config_from_dict(event["config"])

        # Build grid (writer needs group_path + chunk_shape; no populated_shards
        # required because the orchestrator already computed chunk_idx).
        from zagg.grids import from_config

        if config is None:
            from zagg.config import default_config

            config = default_config("atl06")

        # child_order is required for HEALPix runs (drives the leaf order); it is
        # absent/unused for non-HEALPix grids.
        grid_type = config.output.get("grid", {}).get("type", "healpix")
        if grid_type == "healpix" and "child_order" not in event:
            error_msg = "Missing required parameters: child_order"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        grid = from_config(config, parent_order=event.get("parent_order"))

        # Process the shard using cloud-agnostic function. A K>1 grid needs a
        # multi-chunk sink (issue #82 phase 7): ``process_shard`` reads the granules
        # once and yields one ``(block_index, carrier, ragged)`` per finer Zarr chunk.
        # The non-sharded path streams each chunk write-then-free via a ``write_chunk``
        # callback (issue #91) so peak output memory holds ~1 chunk instead of all K;
        # the sharded path (#108) must bundle all K, so it still accumulates via
        # ``chunk_results``. At K==1 the lone chunk's ``block_index`` equals
        # ``event["chunk_idx"]`` and the write is byte-identical either way.
        from zagg.processing import process_shard

        # Opt-in per-phase timing (issue #100). When the orchestrator forwards
        # ``profile``, ``process_shard`` fills ``metadata["phase_timings"]`` with
        # read/index/aggregate deltas; the write phase runs in the callback below and
        # is accumulated into the same sub-dict. Default (no key) leaves it unchanged.
        profile = event.get("profile", False)
        # Strict-AOI mask payload (issue #101): when present, process_shard
        # expands it into the per-cell ``aoi_mask`` column. Absent (flag off) ->
        # not passed, so the worker call and outputs are byte-identical. Mirrors
        # the local runner threading aoi_payload through _process_and_write.
        aoi_payload = event.get("aoi_payload")
        # Per-cell carrier (issues #130/#132). Wire protocol (A): the orchestrator
        # injects the ``handoff`` event key only for an explicit non-default
        # override, so an absent key means "derive from the forwarded config" via
        # ``get_handoff(config)`` (``aggregation.handoff``, default ``"arrow"``).
        # This keeps existing event payloads byte-identical while making the config
        # the single source of truth. (Neither carrier imports pyarrow; pyarrow is
        # not in the layer.)
        handoff = event.get("handoff") or get_handoff(config)
        sharded = getattr(grid, "sharded", False)
        store_path = event["store_path"]
        shard_key = event["shard_key"]

        store_box: dict = {}
        write_error: dict = {}
        _write_elapsed = 0.0
        chunk_results: list | None = None
        _df_out = None

        if get_store_layout(config) == "hive":
            # Hive layout (issue #199 phase 3): the worker owns its WHOLE leaf
            # zarr — it derives the leaf path from shard_key + the event's
            # config orders, emits its own leaf template (lazily, on the first
            # chunk), writes its data, and stamps completion as its FINAL PUT
            # (D4), on error-free shards only. process_and_write_hive is the
            # same code path the local dispatcher runs, so leaf semantics
            # cannot drift between backends. A write failure raises out to the
            # handler's exception envelope: the leaf is then unstamped debris,
            # overwritten wholesale on retry — the same recovery model as the
            # local path (no per-chunk error recording needed).
            from zagg.hive import process_and_write_hive

            metadata = process_and_write_hive(
                shard_key,
                event["granule_urls"],
                grid,
                s3_creds,
                store_path,
                config,
                store_kwargs=_output_store_kwargs(event),
                handoff=handoff,
                aoi_payload=aoi_payload,
                profile=profile,
            )
        else:
            # Flat layout: lazy store + one-time template check, opened on the
            # FIRST chunk write so a no-data shard (zero chunks) never touches
            # the store, exactly as before. A missing template or a failed
            # write is RECORDED (not raised) so ``metadata`` from
            # ``process_shard`` survives — the buffered path returned its 500
            # with that metadata; folding the error in after the stream
            # preserves that body.
            def _get_store():
                """Open + template-check once; returns the store, or None if the template
                is missing (recording the error so the write is skipped)."""
                if "store" in store_box:
                    return store_box["store"]
                if write_error:
                    return None
                store = open_store(store_path, **_output_store_kwargs(event))
                # Validate the Zarr template exists before writing. ``store`` is a zarr v3
                # ``Store`` whose ``exists()`` is async, so open the group via the high-level
                # sync API and catch the missing-node error instead (issue #118), in the same
                # open-and-catch spirit as ``readers/tdigest_tensor.py``.
                # ``GroupNotFoundError`` is raised identically on LocalStore and obstore (S3);
                # a present-but-wrong-type node surfaces as a real error, not "missing".
                try:
                    open_group(store, path=grid.group_path, mode="r", zarr_format=3)
                except GroupNotFoundError:
                    msg = f"Zarr template not found at {store_path}/{grid.group_path}"
                    logger.error(msg)
                    write_error["msg"] = msg
                    return None
                logger.info(f"  Writing data to {store_path}...")
                store_box["store"] = store
                return store

            def _write_chunk(block_index, carrier, ragged):
                nonlocal _write_elapsed
                if write_error:
                    return  # a prior chunk failed (or template missing) — skip the rest
                store = _get_store()
                if store is None:
                    return  # template missing — recorded in write_error, skip the rest
                _t0 = time.time() if profile else None
                try:
                    # write_dataframe_to_zarr no-ops on an empty carrier, so no per-chunk
                    # emptiness check is needed. Use each chunk's own block_index.
                    write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
                    # Ragged fields land in their vlen-bytes arrays at the same
                    # block (issue #209). Mirrors runner._process_and_write.
                    write_ragged_to_zarr(ragged, store, grid=grid, chunk_idx=block_index)
                except Exception as e:
                    # Mirror the buffered path's ``except``: record the failure, stop
                    # writing, and let the run surface a 500 after process_shard returns.
                    logger.error(f"Failed to write zarr to {store_path}: {e}")
                    write_error["msg"] = f"Failed to write zarr: {e}"
                    return
                if profile:
                    _write_elapsed += time.time() - _t0

            chunk_results = [] if sharded else None
            _df_out, metadata = process_shard(
                grid,
                shard_key,
                event["granule_urls"],
                s3_credentials=s3_creds,
                config=config,
                chunk_results=chunk_results,
                write_chunk=None if sharded else _write_chunk,
                handoff=handoff,
                profile=profile,
                aoi_payload=aoi_payload,
            )

            # Sharded output (issue #108): bundle the shard's K inner chunks into one
            # ShardingCodec shard object — one block selection per dense array (a per-
            # inner-chunk loop would read-modify-write the same shard object). This path
            # accumulated all K, so it opens + validates + writes here (same recording).
            if sharded and chunk_results:
                store = _get_store()
                if store is not None:
                    _write_t0 = time.time() if profile else None
                    try:
                        write_shard_to_zarr(
                            chunk_results, store, grid=grid, shard_key=int(shard_key)
                        )
                        if profile:
                            _write_elapsed += time.time() - _write_t0
                    except Exception as e:
                        logger.error(f"Failed to write zarr to {store_path}: {e}")
                        write_error["msg"] = f"Failed to write zarr: {e}"

        # A recorded template-missing / write failure folds into ``metadata`` so the
        # response surfaces a 500 with the structured log, exactly as the buffered
        # ``except`` / early-return branches did (now carrying the worker metadata).
        if write_error:
            metadata["error"] = write_error["msg"]

        # Record the write-phase timing (issue #100): read/index/aggregate come from
        # ``process_shard``; ``write`` is the time spent in the streaming callback /
        # sharded write. Only attach it on a clean write (no ``error``) so a time-to-
        # failure is never folded in as a real write duration; the no-data path wrote
        # nothing (``_write_elapsed`` stays 0) but also has no chunks, so writing 0 is
        # harmless — gate on a populated ``phase_timings`` and no error to match the
        # old "write absent on failure / no-data" contract.
        if profile and not metadata.get("error") and "phase_timings" in metadata and store_box:
            metadata["phase_timings"]["write"] = _write_elapsed

        # Peak worker RSS (issues #120, #141): captured here, after the write phase,
        # so it covers the full invocation. ``max_memory_mb`` is the per-invocation
        # sampled peak (``VmRSS``), trustworthy on warm containers; ``ru_maxrss`` is
        # kept as ``container_hwm_mb`` (the container-lifetime high-water) so the
        # distinction is explicit. Off Linux the sampler is a no-op, so fall back to
        # ``ru_maxrss``. Threaded back via the result body so the orchestrator can
        # surface OOM-proximity without CloudWatch access.
        metadata["container_hwm_mb"] = _max_memory_mb()
        sampled_peak = rss_sampler.peak_mb
        metadata["max_memory_mb"] = (
            sampled_peak if sampled_peak is not None else metadata["container_hwm_mb"]
        )

        # Per-invocation CPU seconds (issue #180 phase 3): user+sys consumed by
        # this invocation across ALL threads (``os.times()`` aggregates the
        # process, so the granule/read pools' work is counted), diffed against
        # the handler-entry snapshot. utilization = cpu_seconds / duration_s is
        # the K-sweep A/B's vCPU-saturation signal, per invocation, without
        # CloudWatch access.
        cpu_t1 = os.times()
        metadata["cpu_seconds"] = round(
            (cpu_t1.user - cpu_t0.user) + (cpu_t1.system - cpu_t0.system), 3
        )

        # Log structured result
        logger.info(
            json.dumps(
                {
                    "event_type": "processing_complete",
                    "shard_key": metadata["shard_key"],
                    "cells_with_data": metadata["cells_with_data"],
                    "total_obs": metadata["total_obs"],
                    "duration_s": metadata["duration_s"],
                    "max_memory_mb": metadata["max_memory_mb"],
                    "error": metadata.get("error"),
                    "request_id": context.aws_request_id,
                }
            )
        )

        logger.info("=" * 70)
        logger.info("Lambda invocation completed successfully")
        logger.info("=" * 70)

        # Drop the invocation's large output buffers before the teardown reclaim
        # (issue #139): the response body is just ``metadata`` (small), so freeing
        # the shard's carrier / accumulated K chunk carriers here lets the
        # ``_reclaim_memory`` call in ``finally`` return their heap to the OS.
        del _df_out, chunk_results

        return {
            "statusCode": 200 if not metadata.get("error") else 500,
            "body": json.dumps(metadata),
        }

    except Exception as e:
        logger.error(f"Unhandled exception in Lambda handler: {e}")
        logger.exception(e)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": f"Unhandled exception: {str(e)}",
                    "shard_key": event.get("shard_key"),
                    "request_id": context.aws_request_id,
                }
            ),
        }
    finally:
        # Stop the per-invocation RSS sampler (issue #141) before the teardown
        # reclaim, so its thread isn't sampling while ``malloc_trim`` runs.
        rss_sampler.stop()
        # Warm-container memory reclaim (issue #139): once per invocation, after
        # the write completes and the buffers above are dropped, hand freed heap
        # back to the OS so the next invoke on this warm container starts near
        # baseline instead of the prior invocation's RSS high-water.
        _reclaim_memory()
