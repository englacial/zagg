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
        "accessKeyId": str,     #   omit to use the execution role, which reaches
        "secretAccessKey": str, #   the in-account bucket, sliderule-public-cors,
        "sessionToken": str,    #   AND source.coop (issue #495). Supply only for
        "endpointUrl": str,     #   an UN-NEGOTIATED target: a collaborator's
        "region": str           #   private bucket, or R2/MinIO. sessionToken,
    },                          #   endpointUrl, and region are optional.
    "config": dict (optional, pipeline config as dict),
    "aoi_payload": list (optional, issue #101) -- this shard's strict-AOI mask
        payload (a compact MOC for HEALPix / in-AOI cell ids for rectilinear).
        Forwarded to ``process_shard`` so the worker fills the ``aoi_mask``
        column; absent when ``output.aoi_mask`` is off (the column is not
        allocated), keeping the flag-off event and outputs byte-identical.
    "invoked_by": dict (optional, issue #297) -- {"arn", "userid"} caller
        identity the dispatcher resolved once per run via sts
        get-caller-identity; the worker copies it VERBATIM into the per-shard
        stats record (sidecar + envelope). Workers cannot see the invoker
        themselves (Event invokes carry no caller identity). Absent -> the
        stats record carries null.
    "run_id": str (optional, issue #297) -- the dispatcher's per-run identity,
        threaded like invoked_by: copied VERBATIM into the per-shard stats
        record so leaf sidecars join back to the run-level stats parquet
        (whose object name carries the same id). Absent -> the stats record
        carries null.
    "submap": dict (optional, issue #300, hive only) -- {"grid_signature",
        "metadata", "granules"} leaf sub-map fields: on success the worker
        writes the unit's full ShardMap JSON sub-map (D22) sibling to the
        stats sidecar via zagg.sweep.write_leaf_submap. The dispatcher
        threads the {id, s3, https} granule entries here because the event's
        bare granule_urls can't reconstruct them; size-gated dispatcher-side
        (dropped when it would push an async event over the 256 KB cap).
        Absent -> no write (old dispatchers keep working). The raster
        process_raster mode carries the same block minus "granules" (its
        events already ship the entries).
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
leaf template. Current dispatchers send hive setup as a fire-and-forget Event
invoke right after the ping -- the primary manifest write, issue #252 hybrid
-- and older synchronous dispatchers keep working against this function. For
a raster-pipeline config -- data_source.reader: raster, issue #264 -- it
writes the raster (time, cells) template instead, from a synchronous invoke):
{
    "mode": "setup",
    "store_path": str,
    "parent_order": int,        # HEALPix fallback; config.output.grid wins
    "n_parent_cells": None,     # OPTIONAL -- ignored (dense layout removed, issue #88)
    "overwrite": bool,
    "config": dict,             # single source of truth: child_order, chunk_inner,
                                #   layout, store_layout, and grid type all come from here
    "dataset": dict (optional, hive only) -- {"short_name", "version"} identity
        block for the manifest, sourced from the ShardMap metadata by the
        orchestrator (matching the local dispatcher). Absent on flat runs.
    "times_us": [int, ...] (raster only, issue #264) -- the catalog-derived
        time coordinate, in whatever encoding "config" declares: int64
        microseconds since the Unix epoch by default, or uint64 mortie toc
        words under output.time_encoding: toc (spec §8, issue #443). Plain
        ints either way; the worker re-derives the dtype from the config, so
        the key name is historical and NOT a width claim. The orchestrator
        owns the global timestep index and threads it here so the template
        write needs no S3 access from the dispatcher.
    "run_manifest": dict (optional, issue #327) -- {"run_id", "shards"
        (decimal shard-key strings), "semantic_hash", "dispatched_at",
        "dataset"} dispatch identity: on a successful setup the worker writes
        it (plus this event's "config") as
        "<store>.status/run-<run_id>/manifest.json" -- what Run.attach
        rebuilds a handle from (D8: the dispatcher never writes). Fail-open;
        absent -> no write, byte-identical to pre-#327 events.
    "output_credentials": dict (optional, same shape as process mode),
}

Finalize mode (after all cells complete: consolidates zarr metadata; for a
hive-layout config -- issue #252 hybrid -- it re-ensures the morton_hive.json
manifest instead, the idempotent backstop for the async init-time setup write):
{
    "mode": "finalize",
    "store_path": str,
    "config": dict (optional, hive only) -- same single-source config as setup;
        its presence + store_layout selects the manifest write. With it ride
        "parent_order", "overwrite", and the optional "dataset" identity
        block, mirroring the hive setup event. Absent on flat runs (their
        event is byte-identical to pre-#252).
    "output_credentials": dict (optional, same shape as process mode),
}

Ping mode (hive pre-fan-out preflight, issue #252 — writes nothing; kept while
flat exists, issue #251):
{
    "mode": "ping",
    "store_path": str,
    "config": dict,             # same manifest inputs as hive finalize; with it
    "parent_order": int,        #   ride "overwrite" and the optional "dataset"
    "overwrite": bool,          #   identity block
    "dataset": dict (optional),
    "output_credentials": dict (optional, same shape as process mode),
}
Answering 200 at all is the versioning half of the guard: a function deployed
before the issue #252 hive dispatch lifecycle doesn't know the mode, so
the event falls through to the process handler's 400 (zero writes) and the
dispatcher fails fast with a redeploy message before any worker runs. The body
echoes the deployed zagg version; the handler also runs the READ-ONLY
zagg.hive.validate_manifest against any existing root so an incompatible rerun
refuses up front (D2).

Sweep mode (unified rollup sweep, issue #300 — the D8 worker-invoke transport
for the D22 second pass; fire-and-forget Event invoke from the dispatcher at
end of run, like coverage mode; also invocable ad hoc):
{
    "mode": "sweep",
    "store_path": str,
    "leaves": [[shard_key, window-or-null], ...] (optional) -- the run's
        completed leaves, inline when they fit the async payload budget,
    "discover": bool (optional) -- re-derive the work set from the store's
        run-record parquets instead (zagg.sweep.discover_leaves; sent when
        the leaves list would overflow the 256 KB Event cap),
    "stage": dict (optional, issue #519) -- present => this invoke runs one
        share of the /2 STAGED dense sweep instead of the rollup families
        (zagg.sweep_stages; the dispatcher is zagg.sweep_fleet). Same event,
        same credential and work-set plumbing, one extra block:
        {
          "role": "stage" | "finisher" (default "stage"),
          "run_id": str,          # the sweep run's id: lease identity, the
                                  #   skip-key/foreign-stamp namespace, and
                                  #   the status prefix all key on it
          "run_started": str,     # dispatcher-pinned UTC ISO stamp, shared by
                                  #   every worker of the run (role="stage")
          "dispatch": int,        # the tuple's dispatch order (role="stage")
          "nodes": [str, ...],    # this invoke's dispatch nodes, as morton
                                  #   decimals (role="stage")
          "batch": int,           # which batch of that tuple this is; names
                                  #   the record object (role="stage")
          "tuple_width": int,     # optional; defaults to
                                  #   zagg.sweep_stage.DEFAULT_TUPLE_WIDTH, the one
                                  #   source the CLI path uses too -- a copied
                                  #   literal here would give the fleet a different
                                  #   tuple grouping, hence a different set of
                                  #   stage columns, on the same store
          "partition": {"index": int, "of": int},  # optional, recorded only
          "lease_ttl_s": int,     # optional
          "records_from": str,    # REQUIRED, both roles. The run's status prefix
                                  #   (a store SIBLING,
                                  #   zagg.client_transport.run_status_prefix):
                                  #   where this invoke PUTs its record, and
                                  #   where a finisher reads the run's records
                                  #   back to rebuild the per-level actuals. A
                                  #   worker that wrote no record reads to the
                                  #   dispatcher exactly like a lost invoke, so
                                  #   both roles refuse by name without it
          "touch_policy": str,    # optional, role="finisher" (issue #501)
          "barrier_timed_out": bool,  # optional, role="finisher": the
                                  #   dispatcher's verdict on its own soft
                                  #   barrier. Recorded in the store-root run
                                  #   record, so a run whose per-level actuals
                                  #   may be short says so durably
        }
        All store writes stay worker-side (D8). The work set rides in the
        SAME "leaves"/"discover" keys the families arm uses -- a stage invoke
        is sent only the slice under its own nodes, so the batching that keeps
        "nodes" under the async cap keeps "leaves" under it too.
    "output_credentials": dict (optional, same shape as process mode),
}

Stats mode (run-level stats parquet, issue #313 — the D8 worker-invoke
transport for the issue #297 run record; fire-and-forget Event invoke from
the dispatcher at end of run, like coverage mode):
{
    "mode": "stats",
    "store_path": str,
    "run_id": str,
    "timestamp": str (optional) -- pins the D20 key the dispatcher announced,
    "rows": [dict, ...] (optional) -- inline run-parquet rows (always carries
        the failure rows; small runs send everything inline),
    "rows_from": str (optional) -- the run's async status prefix; the worker
        assembles success rows from the mirrored result envelopes when the
        row set exceeds the async payload budget,
    "tail_status_url": str (optional, issue #327) -- where to ALSO write the
        run's tail-completion marker on success (the v2 status prefix's
        tail.json); a reattached handle (Run.attach) then skips a tail that
        already ran. Fail-open; absent -> no write, byte-identical.
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
import uuid
from typing import Any, Dict, Optional, Tuple

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


def _probe_output_write(
    event: Dict[str, Any], store_kwargs: Dict[str, Any]
) -> Optional[Tuple[str, bool]]:
    """PUT-then-DELETE a zero-byte object to prove ``s3:PutObject`` (issue #495).

    The ping's read-only manifest check proves the output credentials can REACH
    the store; it cannot prove they can write to it. Credentials that read but
    do not write are exactly how a fresh cross-account grant fails -- and Source
    Cooperative's in-region path vends no credentials of its own (our IAM role
    writes through THEIR bucket policy), so there is no interactive step where a
    human would notice a misconfigured grant. Without this probe the first real
    write is ``ensure_manifest`` in ``mode="setup"``, invoked
    ``InvocationType="Event"`` whose 500 the dispatcher never sees, and
    per-shard status writes are deliberately fail-open (issue #327): the denial
    would surface only after every worker had read and aggregated its shard
    (~$29-58 of compute on a CA-sized run).

    Probes the run's OWN async-result sibling -- ``<store_path>.status/`` (issue
    #151, ``zagg.client_transport.run_status_prefix``) -- as
    ``probe-<uuid>``, rather than the store root or a prefix of its own. Two
    properties, in order:

    * **Never inside the store root.** ``docs/specification.md`` §5.2 makes the
      leaf hash set discovery-based, so a stranded probe object under a leaf is
      a KEY-SET difference, not just untidy: a verifier would report an intact
      leaf as tampered. A denied DELETE must not be able to do that.
    * **Never a NEW grant surface.** The probe is fail-closed, so whatever
      prefix it writes becomes a precondition for the run to start. ``.status``
      is one the run already requires writable (the async invoke/poll transport
      writes every per-shard status object there), so a grant scoped to
      ``<store>.status/*`` + ``<store>/*`` passes the probe exactly when the
      run's real writes would succeed -- no fourth prefix for an operator to
      enumerate, and no false refusal of a correctly scoped grant.

    The key carries a uuid, so concurrent runs -- into different stores or the
    same one -- cannot collide on it, nor with the transport's ``run-<run_id>``
    objects. The prefix rides the same credentials, endpoint and external-target
    canned ACL as the real writes -- the PUT goes through
    ``zagg.store.put_object``, which is what lands a request on the ACL-bearing
    handle now that ``open_object_store`` returns the clean one (issues #495,
    #522) -- so a bucket that rejects the ACL fails here too. Two requests,
    added to the ping, for ``s3://`` stores only.

    Coverage is ``s3:PutObject`` plus ``s3:DeleteObject`` -- and, on an external
    target, ``s3:PutObjectAcl``, since the PUT carries the canned ACL -- and no
    more. One
    small PUT IS representative of the multipart path -- obstore's
    ``CreateMultipartUpload``/``UploadPart``/``CompleteMultipartUpload`` are all
    authorized by ``s3:PutObject``, so no multipart probe is needed -- but it
    cannot exercise ``s3:AbortMultipartUpload`` or
    ``s3:ListMultipartUploadParts``, which the phase 1 grant carries
    deliberately for aborted/retried uploads. A grant missing those still
    passes here.

    ``store_kwargs`` is resolved by the CALLER (``_output_store_kwargs``, in
    ``_handle_ping``'s read-side ``try``) rather than here: a malformed
    ``output_credentials`` block raises ``ValueError`` and must not be reported
    as a denied grant. That is genuinely reachable -- the read half calls
    ``_output_store_kwargs`` only on the hive branch, so on a raster or flat
    ping (issue #264) the event's credentials shape is first touched right
    here.

    Returns ``(probed URI, delete succeeded)``, or ``None`` for a non-``s3://``
    store (a local store has nothing to prove, and probing it would create the
    directory). The DELETE outcome rides back out so it can reach the
    dispatcher: a Put-but-no-Delete grant would otherwise pass this preflight
    silently and fail later at store-overwrite/manifest-cleanup time -- the
    exact "discovered after the compute" shape the probe exists to eliminate.
    """
    store_path = event.get("store_path") or ""
    if not store_path.startswith("s3://"):
        return None

    import obstore

    from zagg.store import open_object_store, put_object

    prefix = f"{store_path.rstrip('/')}.status"
    key = f"probe-{uuid.uuid4().hex}"
    store = open_object_store(prefix, **store_kwargs)
    # put_object, not obstore.put: open_object_store hands back the CLEAN
    # handle and the canned ACL rides its twin (issue #522), so a direct put
    # here would prove a permission the real writes do not use.
    put_object(store, key, b"")
    deleted = True
    try:
        obstore.delete(store, key)
    except Exception:
        deleted = False
        # The PUT is the load-bearing half -- write permission is proven. A
        # delete that fails leaves one zero-byte object OUTSIDE the store root
        # (so no leaf hash is perturbed): worth a warning, not a refused run.
        logger.warning(f"Write probe could not delete {prefix}/{key}", exc_info=True)
    return f"{prefix}/{key}", deleted


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Dispatch on event mode.

    Default ``mode`` (or no mode) runs per-cell processing. ``mode="setup"``
    creates the zarr template; ``mode="finalize"`` consolidates metadata;
    ``mode="ping"`` is the hive pre-fan-out preflight (issue #252);
    ``mode="coverage"`` writes the store-root ``coverage.moc`` (issue #200);
    ``mode="sweep"`` folds the D22 rollup families worker-side (issue #300);
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
        response = _handle_setup(event)
        # Dispatch manifest (issue #327 phase 2): the run's shard set +
        # identity, recorded by the WORKER off the same setup invoke (D8) so
        # a run is reattachable by run id. Fail-open; absent block -> no-op.
        if response.get("statusCode") == 200:
            _write_dispatch_manifest(event)
        return response
    if mode == "finalize":
        return _handle_finalize(event)
    if mode == "ping":
        return _handle_ping(event)
    if mode == "coverage":
        return _handle_coverage(event)
    if mode == "sweep":
        return _handle_sweep(event)
    if mode == "stats":
        return _handle_stats(event)
    # Extract mode returns directly: the result_url mirror below is for the
    # per-unit fan-out handlers (spatial process, temporal process_event) only.
    if mode == "extract":
        return _handle_extract(event, context)
    if mode in ("process_event", "temporal", "event"):
        response = _handle_process_event(event)
    elif mode == "process_raster":
        response = _handle_process_raster(event)
    else:
        response = _handle_process(event, context)
    # Container telemetry rides in every per-unit envelope (issue #171) -- the
    # setup/finalize/extract bodies stay byte-identical (their consumers don't
    # aggregate container state).
    response = _attach_container_telemetry(response, telemetry)
    # Per-shard status object (issue #327): always-on for every per-unit
    # response carrying a run identity, every status branch (200/400/500,
    # including the caught-error envelope) -- the v2 Event transport resolves
    # futures from these instead of the invoke response. Fail-open by
    # ratification: never affects the shard result.
    _write_shard_status(event, response)
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


def _write_shard_status(event: Dict[str, Any], response: Dict[str, Any]) -> None:
    """Always-on per-shard status object (issue #327), doubly fail-open.

    The body lives in ``zagg.client_transport.write_shard_status`` (itself
    fail-open); this wrapper additionally swallows an import/lookup failure so
    a function zip missing the module cannot fail a shard either. Uses the
    same output-store resolution as every other worker write.
    """
    try:
        from zagg.client_transport import write_shard_status

        write_shard_status(event, response, _output_store_kwargs(event))
    except Exception as e:
        logger.warning(f"shard status write failed (fail-open, issue #327): {e}")


def _write_dispatch_manifest(event: Dict[str, Any]) -> None:
    """Run dispatch manifest off the setup event (issue #327), doubly fail-open.

    Mirrors ``_write_shard_status``: the body lives in
    ``zagg.client_transport.write_dispatch_manifest`` (itself fail-open); this
    wrapper additionally swallows an import failure so a function zip missing
    the module cannot fail the setup either.
    """
    try:
        from zagg.client_transport import write_dispatch_manifest

        write_dispatch_manifest(event, _output_store_kwargs(event))
    except Exception as e:
        logger.warning(f"dispatch manifest write failed (fail-open, issue #327): {e}")


def _write_tail_status(event: Dict[str, Any]) -> None:
    """Tail-completion marker off the stats event (issue #327), doubly fail-open."""
    try:
        from zagg.client_transport import write_tail_status

        write_tail_status(event, _output_store_kwargs(event))
    except Exception as e:
        logger.warning(f"tail status write failed (fail-open, issue #327): {e}")


def _write_result(result_url: str, response: Dict[str, Any], event: Dict[str, Any]) -> bool:
    """Write the response envelope to ``result_url`` as JSON (issue #151).

    Uses the same credentials/endpoint resolution as the output store. Never
    raises: on failure the orchestrator's poll times out and records the shard
    as failed, and the cause lands here in CloudWatch. Returns True only when
    the write landed -- the self-recycle gate (issue #171) keys on it.
    """
    from zagg.store import open_object_store, put_object

    try:
        prefix, key = result_url.rsplit("/", 1)
        store = open_object_store(prefix, **_output_store_kwargs(event))
        # One envelope per shard on the published bucket -- through put_object
        # so every one of them carries the canned ACL (issue #522).
        put_object(store, key, json.dumps(response).encode())
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

    For a FLAT raster-pipeline config (``data_source.reader: raster``, issue
    #264; the hive layout wins the branch below regardless of pipeline kind —
    issue #247) this writes the ``(time, cells)`` raster template + its ``time``
    coordinate via ``emit_raster_template`` — the orchestrator dispatches it
    as a synchronous setup invoke before fan-out (the template is
    load-bearing: workers write slabs into its arrays), so an invoke-only
    dispatcher (the CI OIDC role) never needs S3 write access. ``times_us``
    (the catalog-derived int64 time coordinate) rides in the event; the
    success body echoes ``"pipeline": "raster"`` so the dispatcher can refuse
    a stale deployment that fell through to the point-path template below.

    For a hive-layout config (issue #199 phase 3) template time writes ONLY
    the ``morton_hive.json`` manifest — no global zarr template exists (zero
    metadata above the leaves, D5); each worker emits its own leaf template.
    Current dispatchers send hive setup as a fire-and-forget Event invoke
    right after the ping (issue #252 hybrid) — this branch is the PRIMARY
    manifest write, run off the critical path, with finalize as idempotent
    backstop; older synchronous dispatchers keep working against it
    unchanged. The optional ``dataset`` event key carries
    the manifest's identity block (the orchestrator sources it from the
    ShardMap metadata, same as the local path).
    The flat path below is byte-identical to before, bar one addition:
    the success body now ECHOES the layout it acted on (``"layout"``) — a
    stale deployment without the hive branch returns the old echo-less body,
    which the dispatcher rejects for hive runs instead of silently letting old
    workers write a flat store at the hive root (review finding, PR #205).
    """
    from zagg.grids import from_config

    logger.info(f"Setup mode: creating template at {event.get('store_path')}")
    try:
        config = load_config_from_dict(event["config"])
        from zagg.config import get_layout

        if get_layout(config) == "dense":
            # The dense layout was removed (issue #88): reject the stale deployed
            # event with a clean 400 (the PR #257 fold pattern) instead of letting
            # from_config raise into the generic 500 below — one guard for the
            # hive, raster, and flat branches alike.
            error_msg = (
                "setup requires output.grid.layout: fullsphere (dense was removed — issue #88)"
            )
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}
        # Layout splits BEFORE pipeline (issue #247): a hive store's template
        # time writes ONLY the manifest regardless of pipeline kind (D5 — the
        # raster hive worker emits its own per-leaf templates, exactly like
        # the point path), so the hive branch below owns raster + hive too;
        # the raster branch here is the FLAT (time, cells) template (issue
        # #264), byte-identical for flat raster runs.
        if get_store_layout(config) == "hive":
            from zagg.config import get_windowing
            from zagg.hive import build_manifest, ensure_manifest

            grid = from_config(config, parent_order=event.get("parent_order"))
            ensure_manifest(
                event["store_path"],
                # Windowed stores (issue #246) declare morton-hive/2 + the
                # temporal block, derived from the SAME forwarded config the
                # dispatcher fanned out on — no extra event key to drift.
                build_manifest(grid, dataset=event.get("dataset"), windowing=get_windowing(config)),
                overwrite=event.get("overwrite", False),
                **_output_store_kwargs(event),
            )
            return {
                "statusCode": 200,
                "body": json.dumps({"ok": True, "mode": "setup", "layout": "hive"}),
            }
        if (config.data_source or {}).get("reader") == "raster":
            import numpy as np

            from zagg.processing.raster import emit_raster_template
            from zagg.time_axis import time_axis_dtype, time_encoding

            store = open_store(event["store_path"], **_output_store_kwargs(event))
            grid = from_config(config)
            # The wire carries plain ints; the cast is the config's declared
            # time encoding (spec §8) — a toc word does not fit int64.
            times_us = np.asarray(event["times_us"], dtype=time_axis_dtype(time_encoding(config)))
            if times_us.size == 0:
                # A zero-timestep template is degenerate: the arrays get a
                # 0-length time axis no worker can slab-write into.
                # RasterStrategy.run refuses an empty catalog before it would
                # dispatch (runner.py); guard the load-bearing invoke-only
                # writer too, so a hand-rolled or drifted event can't write an
                # unusable success-shaped store (issue #264).
                raise ValueError("raster setup received empty times_us (no timesteps to template)")
            emit_raster_template(
                store, grid, config, times_us, overwrite=event.get("overwrite", False)
            )
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "ok": True,
                        "mode": "setup",
                        "pipeline": "raster",
                        "timesteps": int(times_us.size),
                    }
                ),
            }
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        # Build the grid exactly as the worker does (from_config), so the
        # template's chunk structure can't drift from what workers write. The
        # old hand-built HEALPix branch dropped chunk_inner, under-chunking the
        # template at parent_order while workers wrote finer chunk_inner block
        # indices -> "block index out of bounds" (issue #99). from_config reads
        # chunk_inner + layout from the config. The event's n_parent_cells is
        # ignored — the dense layout it selected was removed (issue #88).
        grid = from_config(config, parent_order=event.get("parent_order"))
        grid.emit_template(store, overwrite=event.get("overwrite", False))
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "mode": "setup", "layout": "flat"}),
        }
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "setup"})}


def _handle_finalize(event: Dict[str, Any]) -> Dict[str, Any]:
    """Finalize the store at ``event['store_path']``.

    Flat: consolidate zarr metadata (byte-identical to before). Hive (issue
    #252 hybrid): re-ensure the root ``morton_hive.json`` manifest — the
    idempotent BACKSTOP for the async init-time setup invoke (a frozen-key-
    matching manifest is accepted, no second PUT). Worker Event invokes run
    with retries 0 (template.yaml EventInvokeConfig), so a lost async init
    write is never redelivered — this backstop self-heals it. The manifest
    inputs mirror the hive setup event (``config``/``parent_order``/
    ``dataset``/``overwrite``); there is no zarr hierarchy above the leaves
    to consolidate (D5), so the hive branch returns without touching zarr.
    The success body echoes ``"layout": "hive"``, matching setup's echo.
    """
    logger.info(f"Finalize mode: finalizing store at {event.get('store_path')}")
    try:
        if "config" in event:
            config = load_config_from_dict(event["config"])
            if get_store_layout(config) == "hive":
                from zagg.config import get_windowing
                from zagg.grids import from_config
                from zagg.hive import build_manifest, ensure_manifest

                grid = from_config(config, parent_order=event.get("parent_order"))
                ensure_manifest(
                    event["store_path"],
                    build_manifest(
                        grid, dataset=event.get("dataset"), windowing=get_windowing(config)
                    ),
                    overwrite=event.get("overwrite", False),
                    **_output_store_kwargs(event),
                )
                return {
                    "statusCode": 200,
                    "body": json.dumps({"ok": True, "mode": "finalize", "layout": "hive"}),
                }
        from zarr import consolidate_metadata

        store = open_store(event["store_path"], **_output_store_kwargs(event))
        consolidate_metadata(store, zarr_format=3)
        return {"statusCode": 200, "body": json.dumps({"ok": True, "mode": "finalize"})}
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "finalize"})}


def _handle_ping(event: Dict[str, Any]) -> Dict[str, Any]:
    """Hive pre-fan-out preflight (issue #252).

    Answering 200 at all is the versioning half of the guard: a function that
    predates the issue #252 hive lifecycle doesn't know ``mode="ping"``,
    so the event falls through to its process handler's 400 (zero writes) and
    the dispatcher fails fast with a redeploy message before any worker is
    dispatched. The body echoes the deployed zagg version for observability.

    The store half mirrors setup/finalize's manifest inputs and runs the
    READ-ONLY :func:`zagg.hive.validate_manifest` against any existing root,
    so a run into a store templated for different orders/identity refuses up
    front (the D2 mixed-order footgun) instead of after the fan-out (PR #255
    review fold). This covers sequential reruns; two concurrent runs into
    the same fresh root both pass here, colliding within seconds of init
    once the async setup write lands (issue #252 hybrid). Kept while flat
    exists (issue #251): once flat is removed, a stale function simply
    errors and the ping can be dropped.

    The third half is the WRITE probe (issue #495): reachability is not
    permission, so :func:`_probe_output_write` PUT-then-DELETEs a zero-byte
    object under the run's own ``<store>.status/`` sibling -- still before any
    worker is dispatched, and under a prefix the async transport already
    requires writable, so it adds no grant surface of its own. Its failure is reported with ``"check": "write_probe"`` so the
    dispatcher names the right remedy -- narrowly, since the store kwargs are
    resolved in the read-side ``try`` above, so only the request itself can
    carry that tag -- and it is deliberately NOT fail-open:
    the point is to refuse the run while refusing is still free. The DELETE
    half stays fail-open but is REPORTED (``probe_delete``/``probe_key`` in the
    200 body), so a Put-but-no-Delete grant reaches the operator as a warning
    rather than as a silent pass plus a stranded object.
    """
    logger.info(f"Ping mode: hive preflight for {event.get('store_path')}")
    try:
        import zagg

        # Resolved HERE, inside the read-side try, and handed to the probe
        # below: a malformed output_credentials block is an event typo, not a
        # denied grant, and must keep the read-side tag. The hive branch is the
        # only other caller, so on a raster/flat ping (issue #264) this line is
        # the first thing to touch the credentials shape at all.
        store_kwargs = _output_store_kwargs(event)
        if "config" in event:
            config = load_config_from_dict(event["config"])
            if get_store_layout(config) == "hive":
                from zagg.config import get_windowing
                from zagg.grids import from_config
                from zagg.hive import build_manifest, validate_manifest

                grid = from_config(config, parent_order=event.get("parent_order"))
                validate_manifest(
                    event["store_path"],
                    build_manifest(
                        grid, dataset=event.get("dataset"), windowing=get_windowing(config)
                    ),
                    overwrite=event.get("overwrite", False),
                    **store_kwargs,
                )
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "ping"})}

    try:
        probed = _probe_output_write(event, store_kwargs)
    except Exception as e:
        # Tagged distinctly from the read-side refusal above: the dispatcher
        # turns "check": "write_probe" into a grant remedy instead of the
        # "clear the store root" one, which would be actively misleading here.
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "mode": "ping", "check": "write_probe"}),
        }
    body: Dict[str, Any] = {
        "ok": True,
        "mode": "ping",
        "zagg_version": zagg.__version__,
        "write_probe": probed is not None,
    }
    if probed is not None:
        body["probe_key"], body["probe_delete"] = probed
    return {"statusCode": 200, "body": json.dumps(body)}


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


def _handle_sweep(event: Dict[str, Any]) -> Dict[str, Any]:
    """Run the unified rollup sweep worker-side (issue #300, D8 transport).

    Posted fire-and-forget (``InvocationType="Event"``) by the Lambda-path
    dispatcher at end of run — the D8 orchestrator-no-write rule means the
    dispatcher cannot PUT rollups itself, so the worker role folds them,
    exactly like the root ``coverage.moc``. The work set rides inline as
    ``leaves`` (``[[shard_key, window], ...]``) when it fits the async
    budget; ``discover: true`` has the worker re-derive it from the store's
    run records (the D22 discovery path). Nobody reads this response on the
    Event invoke; errors log and fail open — every rollup is a regenerable
    cache (D9) and ``python -m zagg.sweep`` is the manual backstop.

    ``partition`` and ``families`` are forwarded verbatim (issue #527). The
    partition block is not decoration: per issue #377 it filters the work set
    worker-side, **stops the bottom-up walk at the split order**, and defers
    the ``finish()`` hook — the three things that make concurrent partitions
    disjoint. Dropping it made a ``discover``-transport partition sweep the
    WHOLE store in every worker (the CA 2,726-leaf sweep died at the 900 s
    wall in all 16), and made an inline-partitioned pass write coarse nodes
    above the split from partial data. ``families`` scopes the pass to a
    subset of :data:`zagg.sweep.DEFAULT_FAMILIES` (the issue #520 ``columns``
    backfill is the first caller that needs it).

    **Pick a width that fits the wall.** A partition does 1/N-th of the work
    but faces the same 900 s timeout, so a width that is merely *narrower* than
    the whole store still dies — N ways instead of one, each having done 1/N-th
    of the fold. The width must satisfy ``leaves x s_per_leaf / N < 900`` AND be
    a power of four (``partition_split_order`` splits on whole morton digits).
    For the CA ATL03 store that is 2,726 leaves x ~60 s/leaf (the SERC probe's
    measured rate) = 163,560 s of fold work, so ``ceil(163560 / 900) = 182``
    partitions minimum, rounded up to the first legal power of four: **256**
    (4^4, ~639 s/worker — fits, no headroom) or **1024** (4^5, ~160 s/worker —
    the width to actually use). The 16 that died was ~10,222 s/worker, 11x over
    the wall; forwarding the block does not change that, only the width does.

    **A partitioned pass writes NOTHING above the split order, and the root
    singletons are still owed.** Orders below the split are never walked
    (``range(shard_order - 1, min_order - 1, -1)`` in
    :func:`zagg.sweep._sweep_family`); ``MocFamily.finish`` — the only writer of
    the store-root ``coverage.moc`` and its sibling ``coverage.toc`` — is
    skipped and reported as ``finish_deferred``; and :mod:`zagg.sweep_overview`
    likewise defers the manifest ``pyramid.materialized`` RMW. That deferral is
    exactly what keeps concurrent partitions disjoint, but nothing on this
    transport picks it back up: a **subsequent partition-less pass** over the
    same work set is what writes the coarse levels, ``coverage.moc``,
    ``coverage.toc``, and the manifest update. It is cheap — every rollup the
    partitions already wrote is skip-if-current, so the follow-up only pays the
    orders above the split. Note there is no finisher arm in the mode table
    above: :func:`zagg.sweep_stages.run_finisher` is called from exactly one
    place, ``run_stage_sweep`` (the ``--stages`` CLI), so on the fleet **the
    partition-less follow-up invoke IS the finisher**. A partitioned "toc
    sweep" that stops after the fan-out produces no toc.
    """
    from zagg.sweep import discover_leaves, run_sweep
    from zagg.sweep_partition import normalize_partition

    logger.info(f"Sweep mode: folding rollups at {event.get('store_path')}")
    try:
        store_kwargs = _output_store_kwargs(event)
        # Validate the block BEFORE anything touches the store: on the
        # ``discover`` transport the next statement is a store-root LIST plus a
        # parquet read per run record, and a malformed partition should not
        # cost that. run_sweep re-validates (it is the authority); this is the
        # same guard ``python -m zagg.sweep`` carries for argv, issue #377.
        normalize_partition(event.get("partition"))
        t0 = time.perf_counter()
        if event.get("leaves") is not None:
            leaves = [(int(key), window) for key, window in event["leaves"]]
            discover_s = None  # the work set rode inline; nothing was derived
        else:
            leaves = discover_leaves(event["store_path"], store_kwargs=store_kwargs)
            discover_s = time.perf_counter() - t0
        # The STAGED arm (issue #519) rides the same event: same credential
        # resolution, same work-set transport, a "stage" block instead of the
        # rollup families. Routed BEFORE the no-work short circuit -- a finisher
        # invoke with an empty work set still has a manifest and a lease to
        # settle, and an empty stage tuple still owes its record.
        if event.get("stage") is not None:
            return _handle_stage_sweep(event, leaves, store_kwargs, t0, discover_s)
        if not leaves:
            # Same key set as the swept response: a benchmark driver reading
            # body["duration_s"] must not KeyError on a no-work invoke (#353).
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "ok": True,
                        "mode": "sweep",
                        "n_leaves": 0,
                        "duration_s": time.perf_counter() - t0,
                        "discover_s": discover_s,
                        "record": None,
                    }
                ),
            }
        summary = run_sweep(
            event["store_path"],
            leaves,
            store_kwargs=store_kwargs,
            families=event.get("families"),
            partition=event.get("partition"),
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "ok": True,
                    "mode": "sweep",
                    "n_leaves": summary["n_leaves"],
                    "families": summary["families"],
                    # issue #353: a synchronous benchmark invoke reads timings
                    # straight off the response; the store-root record is the
                    # durable copy (fail-open -> null). ``duration_s`` is the
                    # fold alone -- run_sweep's own span, which is exactly what
                    # the store-root record carries -- and ``discover_s`` the
                    # work-set derivation, null when the leaves rode inline.
                    # Invoke wall-clock is their sum, not duration_s.
                    "duration_s": summary["duration_s"],
                    "discover_s": discover_s,
                    "record": summary.get("record"),
                }
            ),
        }
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "sweep"})}


#: The per-stage counts the stage arm's envelope totals for a driver.
_STAGE_COUNTS = ("written", "current", "failed", "under_covered")


def _stage_body(
    role: str,
    block: Dict[str, Any],
    out: Optional[Dict[str, Any]],
    *,
    n_leaves: int,
    duration_s: float,
    discover_s: Optional[float],
    error: Optional[BaseException] = None,
) -> Dict[str, Any]:
    """The stage arm's response envelope: compact scalars, one fixed key set.

    The record itself is NOT echoed. A stage record carries one row per
    (artifact node, window) -- a finest-tuple batch is tens of thousands of
    rows, megabytes of JSON -- so embedding it would let a batch that folded
    correctly and PUT its record correctly come back to a RequestResponse
    driver as ``ResponseSizeTooLarge``, indistinguishable from the invoke
    failure the 500 exists to disambiguate. The record is already durable at
    the status prefix and the finisher reads it from there; the envelope
    carries only its identity and the totals.

    Two record keys, because they name two different objects: ``record`` is
    the store-root run record (``sweep_stats_*_stages.json``, the finisher's
    only -- ``None`` for a stage invoke, matching the families arm's use of
    that key), and ``stage_record`` is the status-prefix object this invoke
    PUT. The key SET is identical across both roles and both outcomes, with
    ``None`` where a role has no such value, so a driver can read any field
    off any stage response without a KeyError.
    """
    out = out or {}
    rows = out.get("stages") or []
    body: Dict[str, Any] = {
        "ok": error is None,
        "mode": "sweep",
        "stage": role,
        "run_id": block.get("run_id"),
        "dispatch": out.get("dispatch"),
        "batch": out.get("batch"),
        "n_nodes": out.get("n_nodes"),
        "n_leaves": n_leaves,
        "stage_records": out.get("stage_records"),
        "lease_released": (out.get("lease") or {}).get("released"),
        "duration_s": duration_s,
        "discover_s": discover_s,
        "record": out.get("record") if role == "finisher" else None,
        "stage_record": out.get("finisher_record") if role == "finisher" else out.get("record"),
        "error": None if error is None else str(error),
        "error_class": None if error is None else type(error).__name__,
    }
    body.update({name: sum(int(row.get(name) or 0) for row in rows) for name in _STAGE_COUNTS})
    return body


def _handle_stage_sweep(
    event: Dict[str, Any],
    leaves: Any,
    store_kwargs: Dict[str, Any],
    t0: float,
    discover_s: Optional[float],
) -> Dict[str, Any]:
    """Run one share of the ``/2`` staged dense sweep worker-side (issue #519).

    The D8 transport for the staged sweep, mirroring how ``mode="sweep"``
    already carries the rollup families: the dispatcher cannot PUT, so a
    ``stage`` block on the sweep event hands ONE invoke its share of the run
    and the worker role does every write. Two roles, keyed by
    ``stage.role``:

    * ``"stage"`` (the default) -- fold the dispatch nodes in ``stage.nodes``
      at ``stage.dispatch``, one tuple (``zagg.sweep_stages.run_stage_worker``).
      Lease admission, run-id skip keys and the foreign-fresh-stamp abort are
      the in-process pass's, unchanged.
    * ``"finisher"`` -- the run's last invoke
      (``zagg.sweep_stages.run_stage_finisher``): aggregate the run's stage
      records into the manifest's per-level actuals, refresh the root
      ``coverage.moc``, touch ``aggregation.yaml``, release the lease, write
      the run record. It takes the lease first, like every other role: it is
      the invoke that writes the store-root singletons, so a fan-out that
      outlived its TTL must refuse here rather than finish over a claimant.

    Unlike the families arm this one does NOT fail open in the handler: a
    staged run is a fan-out with a soft barrier, so a swallowed 500 would read
    to the dispatcher as a worker that simply never wrote its record -- the
    same signal as a lost invoke, with none of the cause. The envelope carries
    the error and CloudWatch carries the traceback; the run itself stays
    fail-open at the dispatcher's call site (every stage artifact is
    regenerable, D9, and ``python -m zagg.sweep --stages`` is the backstop).
    """
    from zagg.sweep_stage import DEFAULT_TUPLE_WIDTH
    from zagg.sweep_stages import run_stage_finisher, run_stage_worker

    block = event["stage"]
    role = block.get("role", "stage")
    store_path = event["store_path"]
    logger.info(
        f"Sweep mode [stage]: role={role} run={block.get('run_id')!r} "
        f"dispatch={block.get('dispatch')} at {store_path}"
    )
    try:
        if role == "finisher":
            out = run_stage_finisher(
                store_path,
                leaves,
                run_id=block["run_id"],
                records_from=block.get("records_from"),
                touch_policy=block.get("touch_policy", "auto"),
                lease_ttl_s=block.get("lease_ttl_s"),
                # The dispatcher's soft-barrier verdict (issue #519 review): it
                # is the only party that knows whether it stopped waiting, the
                # finisher cannot tell, and the run record is the only durable
                # place that can say the per-level actuals may be short.
                barrier_timed_out=bool(block.get("barrier_timed_out")),
                store_kwargs=store_kwargs,
            )
        elif role == "stage":
            out = run_stage_worker(
                store_path,
                leaves,
                run_id=block["run_id"],
                run_started=block["run_started"],
                dispatch=int(block["dispatch"]),
                nodes=block.get("nodes") or [],
                batch=int(block.get("batch", 0)),
                tuple_width=int(block.get("tuple_width", DEFAULT_TUPLE_WIDTH)),
                partition=block.get("partition"),
                records_from=block.get("records_from"),
                lease_ttl_s=block.get("lease_ttl_s"),
                store_kwargs=store_kwargs,
            )
        else:
            raise ValueError(f"unknown stage role {role!r} (expected 'stage' or 'finisher')")
        return {
            "statusCode": 200,
            "body": json.dumps(
                _stage_body(
                    role,
                    block,
                    out,
                    n_leaves=len(leaves),
                    duration_s=time.perf_counter() - t0,
                    discover_s=discover_s,
                )
            ),
        }
    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": json.dumps(
                _stage_body(
                    role,
                    block,
                    None,
                    n_leaves=len(leaves),
                    duration_s=time.perf_counter() - t0,
                    discover_s=discover_s,
                    error=e,
                )
            ),
        }


def _handle_stats(event: Dict[str, Any]) -> Dict[str, Any]:
    """Write the run-level stats parquet at the store root (issue #313).

    Posted fire-and-forget (``InvocationType="Event"``) by the dispatcher at
    end of run — the D8 orchestrator-no-write invariant means the dispatcher
    (e.g. the invoke-only CI OIDC role) cannot PUT the parquet itself, so the
    worker role performs the write, exactly like the root ``coverage.moc``
    (``mode="coverage"``). Two row transports, chosen dispatcher-side by
    payload size: ``rows`` inline (small runs; always carries the failure
    rows, which have no mirrored envelope to read back), and/or ``rows_from``
    — the run's async status prefix, from which the worker assembles the
    success rows (issue #151 envelopes; worker-role read). ``timestamp``
    pins the D20 key so the dispatcher knows the path it announced.
    ``finalize_error`` (issue #335) rides the same event and lands as the one
    run-level column — the durable record of a deferred finalize failure.
    Nobody reads this response on the Event invoke; errors log and fail open —
    the run record is best-effort telemetry, never load-bearing.
    """
    from zagg.telemetry import rows_from_status, write_run_parquet

    logger.info(f"Stats mode: writing run parquet at {event.get('store_path')}")
    try:
        rows = list(event.get("rows") or [])
        if event.get("rows_from"):
            rows += rows_from_status(event["rows_from"], store_kwargs=_output_store_kwargs(event))
        if not rows:
            # A pointer prefix that yielded nothing (and no inline rows):
            # nothing to persist — not an error (fail-open telemetry).
            _write_tail_status(event)
            return {
                "statusCode": 200,
                "body": json.dumps({"ok": True, "mode": "stats", "rows": 0}),
            }
        path = write_run_parquet(
            event["store_path"],
            rows,
            run_id=event["run_id"],
            timestamp=event.get("timestamp"),
            store_kwargs=_output_store_kwargs(event),
            # Run-level guarded-finalize outcome (issue #335): the dispatcher
            # defers a finalize failure through its tail, so this is where the
            # failure becomes durable. Absent on a pre-#335 dispatcher -> None.
            finalize_error=event.get("finalize_error"),
        )
        # Tail-completion marker (issue #327): the stats leg is the recorded
        # end of the post-run tail, so a reattached handle can skip a tail
        # that already ran. Fail-open; absent url -> no-op.
        _write_tail_status(event)
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True, "mode": "stats", "rows": len(rows), "path": path}),
        }
    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "mode": "stats"})}


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

        # The mask is open, so the event extent is known before any collection
        # read: the reader subsets + loads each granule to it and frees the
        # buffer, bounding peak memory to ~one granule (issue #225 -- a 4 GB
        # worker OOMs holding whole granules for every collection at once).
        extent = None
        coords = getattr(event_mask, "coords", {})
        if "lat" in coords and "lon" in coords:
            extent = (event_mask["lat"].values, event_mask["lon"].values)

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
            extent=extent,
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


def _handle_process_raster(event: Dict[str, Any]) -> Dict[str, Any]:
    """Raster pull-NN worker (issue #218): one shard's ``(time, cells)`` slabs.

    Event keys: ``shard_key`` (int), ``granules`` (the shard's ShardMap
    entries — per-band asset hrefs + datetime/time_key), ``config`` (raster
    pipeline config dict), ``store_path``, ``time_index`` (``{group_key:
    t_idx}`` for this shard's acquisition groups — the orchestrator owns the
    global index and the template, per the issue #218 single-writer append
    design). Source reads are anonymous by default (Earth Search COGs);
    ``data_source.source_region``/``anonymous`` override. No
    ``s3_credentials`` block: the raster source needs none, and the output
    store uses ``output_credentials`` or the execution role like every other
    mode.
    """
    start_time = time.time()
    # Per-invocation peak-RSS sampler (issue #141 convention; raster parity
    # for issue #250): the body reports THIS invocation's sampled peak, with
    # the container-lifetime ``ru_maxrss`` high-water as the off-Linux
    # fallback -- exactly the point-path split.
    rss_sampler = _PeakRSSSampler().start()
    try:
        # The hive path (issue #247) needs no ``time_index``: the worker
        # builds its own leaf-local index from the dispatched subset. Peek at
        # the raw config dict (no load yet) so a flat event — including one
        # with no config at all — reports the flat requirements byte-identical
        # to before.
        cfg_dict = event.get("config")
        output = cfg_dict.get("output") if isinstance(cfg_dict, dict) else None
        hive = isinstance(output, dict) and output.get("store_layout") == "hive"
        required = ["shard_key", "granules", "config", "store_path"]
        if not hive:
            required.append("time_index")
        missing = [p for p in required if p not in event]
        if missing:
            error_msg = f"Missing required parameters: {', '.join(missing)}"
            logger.error(error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        from zagg.grids import from_config
        from zagg.processing.raster import (
            new_stage_stats,
            process_raster_shard,
            write_raster_coords,
            write_raster_slab,
        )

        config = load_config_from_dict(event["config"])
        try:
            grid = from_config(config)
        except ValueError as e:
            # Fail fast with a clean 400 on an invalid grid config (e.g. a
            # stale layout: dense, removed in issue #88) instead of a retried
            # 500 (review fold, PR #257).
            logger.error(str(e))
            return {"statusCode": 400, "body": json.dumps({"error": str(e)})}

        shard_key = int(event["shard_key"])
        source = config.data_source or {}
        profile = bool(event.get("profile"))

        if hive:
            # Hive branch (issue #247), mirroring the aggregation one: the
            # worker owns its WHOLE leaf — process_and_write_raster_hive is
            # the same code path the local dispatcher runs (leaf template +
            # slabs + coverage + D4 commit stamp), so leaf semantics cannot
            # drift between backends. ``window`` ({"label", ...}) is the
            # dispatch unit's time window, absent on schedule-none stores; the
            # response mirrors the stamped ISO ``time_range`` back for the
            # dispatcher's root-summary union. A write failure raises into the
            # 500 envelope: the leaf is then unstamped debris, replaced
            # wholesale on retry (D13).
            from zagg.processing.raster import process_and_write_raster_hive

            meta = process_and_write_raster_hive(
                shard_key,
                event["granules"],
                grid,
                event["store_path"],
                config,
                store_kwargs=_output_store_kwargs(event),
                window=event.get("window"),
                profile=profile,
                region=source.get("source_region"),
                anonymous=source.get("anonymous", True),
            )
            body = {
                "shard_key": shard_key,
                "timesteps": meta["timesteps"],
                "granule_count": meta["granule_count"],
                "skipped": meta["skipped"],
                # Shared summary keys (see the flat branch below): a raster
                # unit's obs tally is its timestep count; cells_with_data
                # counts the leaf's occupied-cell union via the stamp input.
                "cells_with_data": meta.get("cells_with_data", 0),
                "total_obs": meta["timesteps"],
                "duration_s": time.time() - start_time,
                # Read-volume counters (issue #297) for the stats record.
                "raster_bytes_read": meta.get("raster_bytes_read"),
                "raster_px_decoded": meta.get("raster_px_decoded"),
                "raster_px_sampled": meta.get("raster_px_sampled"),
            }
            if meta.get("time_range") is not None:
                body["time_range"] = meta["time_range"]
            # Worker memory telemetry (issue #250), mirroring the flat branch:
            # sampled per-invocation peak, container high-water fallback.
            rss_sampler.stop()
            body["container_hwm_mb"] = _max_memory_mb()
            sampled_peak = rss_sampler.peak_mb
            body["max_memory_mb"] = (
                sampled_peak if sampled_peak is not None else body["container_hwm_mb"]
            )
            # Always-on collection (issue #297); the per-stage ``stages`` block
            # inside stays gated on ``profile`` (raster.py).
            if "phase_timings" in meta:
                body["phase_timings"] = meta["phase_timings"]
            # Per-shard stats record (issue #297): envelope ride + the leaf
            # sidecar (only when the unit wrote a leaf — ``leaf_written`` is the
            # accurate signal, set iff a slab streamed and the leaf was stamped;
            # a unit with acquisitions but no occupied cell writes no leaf, so
            # ``timesteps`` alone would orphan a sidecar). Fail-open on the
            # sidecar PUT; the record still rides the envelope.
            from zagg.telemetry import build_record, lambda_env, raster_granule_ids, write_sidecar

            record = build_record(
                shard_key=shard_key,
                metadata=body,
                granule_ids=raster_granule_ids(event["granules"]),
                invoked_by=event.get("invoked_by"),
                run_id=event.get("run_id"),
                window=(event.get("window") or {}).get("label"),
                lambda_config=lambda_env(),
            )
            body["stats"] = record
            if meta.get("leaf_written"):
                from zagg.hive import shard_leaf_path

                try:
                    window = event.get("window")
                    leaf = shard_leaf_path(
                        event["store_path"],
                        shard_key,
                        window=window["label"] if window else None,
                    )
                    write_sidecar(leaf, record, **_output_store_kwargs(event))
                except Exception as e:
                    logger.warning(f"stats sidecar write failed (fail-open, issue #297): {e}")
                # Leaf sub-map (issue #300, D22): full ShardMap JSON next to
                # the stats sidecar. Raster events already carry the unit's
                # ShardMap entries in ``granules``; the ``submap`` block adds
                # the catalog identity a worker cannot derive. Absent block
                # (old dispatcher) -> no write, fail-open like the sidecar.
                submap = event.get("submap")
                if submap:
                    try:
                        from zagg.sweep import submap_emittable, write_leaf_submap

                        window = event.get("window")
                        if submap_emittable(submap["grid_signature"], event["granules"]):
                            write_leaf_submap(
                                event["store_path"],
                                shard_key,
                                event["granules"],
                                grid_signature=submap["grid_signature"],
                                metadata=submap.get("metadata"),
                                window=window["label"] if window else None,
                                store_kwargs=_output_store_kwargs(event),
                            )
                        else:
                            logger.debug(
                                f"leaf sub-map skipped for shard {shard_key}: non-HEALPix "
                                f"grid or id-less entries (unmergeable, issue #300)"
                            )
                    except Exception as e:
                        logger.warning(f"leaf sub-map write failed (fail-open, issue #300): {e}")
            return {"statusCode": 200, "body": json.dumps(body)}

        time_index = {k: int(v) for k, v in event["time_index"].items()}

        # Stream the slabs: open the store up front and write + free each
        # timestep's slab as ``process_raster_shard`` completes its acquisition
        # group, so peak output memory holds ~1 slab instead of all T (issue
        # #231) — parity with the local RasterStrategy's per-shard streaming.
        store = open_store(event["store_path"], **_output_store_kwargs(event))
        wrote = False
        # Opt-in phase timing (the issue #100 convention, raster flavor):
        # ``write`` is the sink's accumulated wall and ``sample`` the
        # remainder — the split the PR #232 double-buffer decision needs
        # measured. Exact at write_buffer=1 (writes serialize in the loop);
        # at write_buffer>1 writes overlap sampling, so the remainder is
        # approximate — A/B on ``duration_s`` instead. ``stages`` (issue
        # #249) splits the sample bucket into per-stage work volumes +
        # counts — attribution, not a wall decomposition: concurrent samples
        # overlap, so stage sums can exceed ``sample`` (see
        # ``new_stage_stats``). Default (no ``profile`` key) emits nothing:
        # the body stays byte-identical and the sample path times nothing.
        write_s = 0.0
        stage_stats = new_stage_stats() if profile else None

        def _write_slab(t_idx: int, slab: dict) -> None:
            nonlocal wrote, write_s
            w0 = time.time()
            write_raster_slab(store, grid, shard_key, t_idx, slab)
            write_s += time.time() - w0
            wrote = True

        _slabs, meta = process_raster_shard(
            grid,
            shard_key,
            event["granules"],
            config,
            time_index,
            region=source.get("source_region"),
            anonymous=source.get("anonymous", True),
            on_slab=_write_slab,
            stage_stats=stage_stats,
        )
        if wrote:
            write_raster_coords(store, grid, shard_key)

        body = {
            "shard_key": shard_key,
            "timesteps": meta["timesteps"],
            "granule_count": meta["granule_count"],
            "skipped": meta["skipped"],
            # The shared summary accumulators key on these two names: a raster
            # shard's observation tally is its shard x timestep slab count.
            "cells_with_data": grid.cells_per_shard if wrote else 0,
            "total_obs": meta["timesteps"],
            "duration_s": time.time() - start_time,
            # Read-volume counters (issue #297) for the stats record.
            "raster_bytes_read": meta.get("raster_bytes_read"),
            "raster_px_decoded": meta.get("raster_px_decoded"),
            "raster_px_sampled": meta.get("raster_px_sampled"),
        }
        # Worker memory telemetry (issue #250): sampled per-invocation peak,
        # container high-water fallback (see the process-mode stamp).
        rss_sampler.stop()
        body["container_hwm_mb"] = _max_memory_mb()
        sampled_peak = rss_sampler.peak_mb
        body["max_memory_mb"] = (
            sampled_peak if sampled_peak is not None else body["container_hwm_mb"]
        )
        # Always-on sample/write split (issue #297); ``stages`` (issue #249)
        # stays profile-gated verbosity.
        body["phase_timings"] = {
            "sample": (time.time() - start_time) - write_s,
            "write": write_s,
        }
        if profile:
            body["phase_timings"]["stages"] = stage_stats
        # Stats record (issue #297): envelope ride only — a flat store has no
        # per-shard leaf for a sidecar sibling (see the PR #302 discussion).
        from zagg.telemetry import build_record, lambda_env, raster_granule_ids

        body["stats"] = build_record(
            shard_key=shard_key,
            metadata=body,
            granule_ids=raster_granule_ids(event["granules"]),
            invoked_by=event.get("invoked_by"),
            run_id=event.get("run_id"),
            lambda_config=lambda_env(),
        )
        return {"statusCode": 200, "body": json.dumps(body)}
    except Exception as e:
        logger.error(f"raster worker failed: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "shard_key": event.get("shard_key"),
                    "duration_s": time.time() - start_time,
                }
            ),
        }
    finally:
        rss_sampler.stop()  # idempotent; ends the daemon thread on error paths too


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
                # Temporal window unit (issue #246): {"label", "start", "end"}
                # in dataset units, absent on unwindowed runs. The shared
                # write path selects the windowed leaf, injects the
                # time_field filter, and stamps window + ISO time_range; the
                # response body (this metadata) then carries time_range back
                # for the dispatcher's root-summary union.
                window=event.get("window"),
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
                _t0 = time.time()
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
                    _write_t0 = time.time()
                    try:
                        write_shard_to_zarr(
                            chunk_results, store, grid=grid, shard_key=int(shard_key)
                        )
                        _write_elapsed += time.time() - _write_t0
                    except Exception as e:
                        logger.error(f"Failed to write zarr to {store_path}: {e}")
                        write_error["msg"] = f"Failed to write zarr: {e}"

        # A recorded template-missing / write failure folds into ``metadata`` so the
        # response surfaces a 500 with the structured log, exactly as the buffered
        # ``except`` / early-return branches did (now carrying the worker metadata).
        if write_error:
            metadata["error"] = write_error["msg"]

        # Record the write-phase timing (issue #100; always-on collection since
        # issue #297): read/index/aggregate come from ``process_shard``; ``write``
        # is the time spent in the streaming callback / sharded write. Only attach
        # it on a clean write (no ``error``) so a time-to-failure is never folded
        # in as a real write duration; the no-data path wrote nothing and has no
        # chunks — "write absent on failure / no-data" is unchanged.
        if not metadata.get("error") and "phase_timings" in metadata and store_box:
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

        # Per-shard stats record (issue #297): rides the response envelope
        # (``body["stats"]``) so the dispatcher builds the run parquet with no
        # second S3 listing; hive leaves additionally get the ``stats.json``
        # sidecar SIBLING to the leaf .zarr — success only (an error-free hive
        # shard is by construction written + stamped). ``invoked_by`` is copied
        # verbatim from the invoke payload (the dispatcher resolves it; the
        # worker cannot). Fail-open: a sidecar PUT failure never fails a shard
        # whose data landed — the record still rides the envelope.
        from zagg.telemetry import build_record, lambda_env, write_sidecar

        record = build_record(
            shard_key=int(shard_key),
            metadata=metadata,
            granule_ids=event.get("granule_urls"),
            invoked_by=event.get("invoked_by"),
            run_id=event.get("run_id"),
            window=(event.get("window") or {}).get("label"),
            lambda_config=lambda_env(),
        )
        metadata["stats"] = record
        if get_store_layout(config) == "hive" and not metadata.get("error"):
            from zagg.hive import shard_leaf_path

            try:
                window = event.get("window")
                leaf = shard_leaf_path(
                    store_path, int(shard_key), window=window["label"] if window else None
                )
                write_sidecar(leaf, record, **_output_store_kwargs(event))
            except Exception as e:
                logger.warning(f"stats sidecar write failed (fail-open, issue #297): {e}")
            # Leaf sub-map (issue #300, D22): full ShardMap JSON next to the
            # stats sidecar. The event's bare granule_urls can't reconstruct
            # the ShardMap entries, so the dispatcher threads them (plus the
            # catalog identity) in the size-gated ``submap`` block; absent
            # (old dispatcher, or dropped for the async cap) -> no write.
            submap = event.get("submap")
            if submap:
                try:
                    from zagg.sweep import submap_emittable, write_leaf_submap

                    window = event.get("window")
                    granules = submap.get("granules") or []
                    if submap_emittable(submap["grid_signature"], granules):
                        write_leaf_submap(
                            store_path,
                            int(shard_key),
                            granules,
                            grid_signature=submap["grid_signature"],
                            metadata=submap.get("metadata"),
                            window=window["label"] if window else None,
                            store_kwargs=_output_store_kwargs(event),
                        )
                    else:
                        logger.debug(
                            f"leaf sub-map skipped for shard {shard_key}: non-HEALPix grid "
                            f"or id-less entries (unmergeable, issue #300)"
                        )
                except Exception as e:
                    logger.warning(f"leaf sub-map write failed (fail-open, issue #300): {e}")

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
