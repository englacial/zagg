"""Notebook-first dispatch facade: per-shard futures over Lambda (issue #326).

:class:`Run` owns the composition that ``.github/scripts/run_benchmark.py``
and the dispatch scratch scripts do imperatively today — config-load →
shardmap → dispatch → harvest — as library API::

    from zagg.client import Run

    run = Run.from_config("atl03_tdigest_healpix_o9_hive.yaml",
                          shardmap="shardmap.json", store="s3://bucket/out.zarr")
    handle = run.dispatch()                    # returns after the setup handshake
    for fut in handle.progress():              # tqdm over as_completed
        r = fut.result()                       # worker envelope / raises ShardError
    handle.status()                            # {"pending", "ok", "failed"}

Draining the harvest iterator to exhaustion (``as_completed`` / ``progress`` /
``results`` / a clean ``raise_first``) also joins the post-run tail — the
finalize backstop and the coverage/stats/sweep rollup invokes — so the loop
above IS the whole run and :meth:`RunHandle.wait` is only needed for explicit
control (a timeout, or a handle harvested some other way). Abandoning a handle
without draining it is the one flow that truncates the tail: the finisher is a
daemon thread, so an undrained handle cannot wedge interpreter exit (review
finding, PR #333).

**v1 transport** (ratified on issue #265, the default): a
``ThreadPoolExecutor`` over the existing synchronous ``RequestResponse``
invokes — each shard is one :func:`zagg.runner._invoke_lambda_cell` call and
its pool future is the shard's :class:`concurrent.futures.Future`, wrapping
exactly what the worker returns today (timings, error payloads). Zero
worker-side change; the cost is one held connection per in-flight shard and
the client staying alive.

**v2 transport** (issue #327, ``dispatch(transport="event")``): fire-and-forget
``InvocationType="Event"`` invokes resolved by a single poller thread over the
run's status-object prefix (:mod:`zagg.client_transport`) — no held
connections, status-driven retries under the same ``max_retries``, a distinct
``failed-unknown`` outcome for silently-dropped invokes, and the run is
reattachable by run id (:meth:`Run.attach`). Same public surface.

The dispatcher-never-writes invariant (D8) is untouched: every store write —
template setup, shard output, finalize, coverage/stats/sweep rollups — rides a
worker invoke; this module only holds futures over those invokes.

Scope (v1): the spatial point path on the ``lambda`` backend. Temporal,
raster, and windowed configs are refused with a pointer to
:func:`zagg.runner.agg` / :func:`zagg.notebook.run`, which already run them.

tqdm is an *optional* import (``analysis`` extra, espg-ratified on issue
#265): only :meth:`RunHandle.progress` touches it, so importing this module —
and everything else in zagg — works without it.
"""

from __future__ import annotations

import logging
import threading
import uuid
import warnings
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import wait as futures_wait
from dataclasses import asdict
from typing import Any, Iterator

from zagg.config import (
    PipelineConfig,
    get_child_order,
    get_consolidate_metadata,
    get_coverage_moc,
    get_driver,
    get_handoff,
    get_icechunk,
    get_icechunk_options,
    get_output_endpoint_url,
    get_output_region,
    get_parent_order,
    get_pipeline_type,
    get_store_layout,
    get_store_path,
    get_sweep,
    get_touch_policy,
    get_windowing,
    load_config,
    load_config_from_dict,
    validate_config,
)
from zagg.dispatch import BENIGN_ERRORS, LAMBDA_ARCH, LAMBDA_PRICE_PER_GB_SEC, max_cost_usd
from zagg.grids import from_config as grid_from_config
from zagg.grids.morton import morton_word
from zagg.hive import effective_store_root

logger = logging.getLogger(__name__)

#: Fallback fan-out width: used when ``dispatch(max_workers=...)`` is not given
#: AND the account-concurrency preflight cannot run (a custom ``lambda_client``
#: is injected, or the probe was denied — see :meth:`Run._probe_workers`).
#: Deliberately modest, since the v1 sync transport holds one connection per
#: in-flight shard: fine at notebook/NEON scale, and ``max_workers`` is the
#: explicit override for bigger fan-outs.
_DEFAULT_MAX_WORKERS = 64


class ShardError(RuntimeError):
    """A shard's worker invoke failed; raised from that shard's ``future.result()``.

    Carries the full per-shard result dict (``payload``) the dispatch loop
    accumulates on the ``agg`` path — ``{shard_key, status_code, body,
    wall_time, lambda_duration, error, retries, ...}`` — so timings and the
    worker's error body survive into the exception.
    """

    def __init__(self, message: str, *, shard_key: int, label: str, payload: dict):
        super().__init__(message)
        self.shard_key = shard_key
        self.label = label
        self.payload = payload


def _fail_with_shard_error(entry, result: dict) -> None:
    """v2 poller ``on_failed``: a terminal failure raises :class:`ShardError`.

    Shared by :meth:`Run.dispatch` (event transport) and :meth:`Run.attach` so
    a failed status, a spent retry budget, and the ``failed-unknown`` drop
    outcome all surface from ``future.result()`` exactly like the v1 transport
    — the payload is the same per-shard result dict, with ``outcome`` marking
    a drop.
    """
    detail = result.get("error") or f"status {result.get('status_code')}"
    entry.future.set_exception(
        ShardError(
            f"shard {entry.label}: {detail}",
            shard_key=entry.shard_key,
            label=entry.label,
            payload=result,
        )
    )


class RunHandle:
    """Live handle over one dispatched fan-out: per-shard futures + rollup.

    ``futures`` maps the shardmap's shard key (int) to that shard's
    :class:`~concurrent.futures.Future`. A future resolves to the worker's
    result dict; a failed shard raises :class:`ShardError` from ``.result()``.
    Benign no-work outcomes (``"No granules found"`` / ``"No data after
    filtering"``) resolve normally and count as ``ok`` — matching how the
    ``agg`` path counts them (neither data nor error).

    A background finisher thread waits for every shard, then runs the same
    worker-invoke tail as ``agg``'s lambda path: the finalize backstop, plus
    the fail-open coverage/stats/sweep rollup invokes (D8: all worker-side).
    Draining any of the harvest iterators (:meth:`as_completed`,
    :meth:`progress`, :meth:`results`, a clean :meth:`raise_first`) joins that
    finisher before returning and surfaces a finalize failure, so a completed
    harvest loop means a completed run; :meth:`wait` is the explicit form (and
    the only one taking a timeout). Only a handle nobody drains truncates the
    tail — the finisher is a daemon so that case cannot hang exit.

    Under ``output.sweep: "stages"`` the tail also chains the staged sweep
    (issue #588), invoke-and-poll on the finisher thread, so the join can
    block up to the fleet's total barrier budget
    (:data:`zagg.sweep_fleet.DEFAULT_TOTAL_BARRIER_BUDGET_S`, 7,200 s) after
    the last shard settles. A process exit or kernel restart in that window
    cuts the sweep off while it holds the store lease; the lease's TTL expiry
    is the designed recovery (see ``runner._invoke_lambda_stage_sweep``, "If
    the dispatcher dies mid-barrier"), and the run is left untagged.
    """

    def __init__(
        self,
        futures: dict[int, Future],
        *,
        store_path: str,
        memory_gb: float | None = None,
        price_per_gb_sec: float = LAMBDA_PRICE_PER_GB_SEC,
    ):
        self.futures = futures
        self.store_path = store_path
        self._memory_gb = memory_gb
        self._price_per_gb_sec = price_per_gb_sec
        self._finisher: threading.Thread | None = None
        self._finalize_error: BaseException | None = None
        self._tail_error: BaseException | None = None
        #: The run's Icechunk finalize record (issue #582) once the tail has
        #: run — ``{path, tag, snapshot, ...}``, or ``{"error": ...}`` from the
        #: fail-open invoke; ``None`` while the tail is in flight and when
        #: the knob is off (or, on a dispatch, the run stood up no repo). A
        #: reattached handle finalizes off the knob: ``{"error": ...}`` when
        #: the store has no repo, ``{"skipped": ...}`` for a ``sweep:
        #: "stages"`` run (its dispatcher finalizes) or when a later run has committed since
        #: (newest-only); an already-tagged run is a no-op. Read it after
        #: :meth:`wait` or a drained harvest.
        self.icechunk_finalize: dict | None = None
        #: The tail's staged-sweep summary (issue #588) once the tail chained
        #: one — ``output.sweep: "stages"`` with leaves or dirt-only units —
        #: the same dict ``runner._run_lambda`` reports; ``None`` while the
        #: tail is in flight, when no staged sweep was chained, or when its
        #: dispatch failed (fail-open, D9 — ``icechunk_finalize`` then says
        #: ``{"skipped"}``).
        self.stage_sweep: dict | None = None

    def __len__(self) -> int:
        return len(self.futures)

    def __repr__(self) -> str:
        s = self.status()
        return (
            f"RunHandle({len(self)} shards: {s['pending']} pending, "
            f"{s['ok']} ok, {s['failed']} failed)"
        )

    def as_completed(self, timeout: float | None = None) -> Iterator[Future]:
        """Yield each shard future as it completes (``concurrent.futures`` order).

        Exhausting this iterator joins the post-run tail (see :meth:`wait`) and
        re-raises a finalize failure, so the documented harvest loop finishes a
        complete run without calling :meth:`wait` (review finding, PR #333).
        Breaking out early skips that join — call :meth:`wait` if you do.
        """
        yield from as_completed(self.futures.values(), timeout=timeout)
        self._join_tail()

    def cost_usd(self) -> float | None:
        """Metered Lambda cost of the settled shards so far (billed-duration rollup).

        Sums ``lambda_duration`` over every settled shard — worker envelopes
        and :class:`ShardError` payloads alike (a failed invoke still billed)
        — priced at the run's worker memory, the same rollup the CLI prints
        post-run (issue #298). Non-blocking: on a live run this is the
        running subtotal, so a status cell can show cost-so-far. ``None``
        when the handle does not know the worker memory (a reattached
        handle) — the run-stats parquet has the authoritative figures.
        """
        if self._memory_gb is None:
            return None
        total = 0.0
        for fut in self.futures.values():
            if not fut.done():
                continue
            exc = fut.exception()
            if exc is None:
                payload = fut.result()
            elif isinstance(exc, ShardError):
                payload = exc.payload
            else:
                continue
            total += float(payload.get("lambda_duration") or 0.0)
        return total * self._memory_gb * self._price_per_gb_sec

    def progress_async(self, **tqdm_kwargs) -> threading.Thread:
        """Drive the harvest on a daemon thread: live bar, the cell returns at once.

        The notebook-async form of :meth:`progress` (issue #328): the
        returned (already-started) thread drains :meth:`progress` — or the
        bar-less :meth:`as_completed` when tqdm is absent — so later cells
        execute while the bar ticks. Draining joins the post-run tail as
        usual; a tail failure is logged and re-raises from :meth:`wait`,
        never on the daemon thread. Join the run (:meth:`wait`, or the
        returned thread) before consuming results.
        """

        def _drain() -> None:
            try:
                iterator = self.progress(**tqdm_kwargs)
            except ImportError:
                iterator = self.as_completed()
            try:
                for _ in iterator:
                    pass
            except BaseException as e:  # noqa: BLE001 - surfaced via wait(), not the daemon
                logger.warning(f"async progress drain: {e} (re-raises from handle.wait())")

        thread = threading.Thread(target=_drain, name="zagg-client-progress", daemon=True)
        thread.start()
        return thread

    def status(self) -> dict[str, int]:
        """Non-blocking snapshot: ``{"pending": n, "ok": n, "failed": n}``."""
        counts = {"pending": 0, "ok": 0, "failed": 0}
        for fut in self.futures.values():
            if not fut.done():
                counts["pending"] += 1
            elif fut.exception() is not None:
                counts["failed"] += 1
            else:
                counts["ok"] += 1
        return counts

    def results(self, *, return_exceptions: bool = False) -> dict[int, Any]:
        """Block until every shard completes; return ``{shard_key: result}``.

        With ``return_exceptions=False`` (default) the first failed shard's
        :class:`ShardError` propagates (key order); with ``True`` failures
        appear as the exception object in the mapping instead — the
        ``asyncio.gather`` convention. Either way the post-run tail is joined
        first (see :meth:`as_completed`), so a shard failure does not truncate
        the rollups.
        """
        out: dict[int, Any] = {}
        first_exc: BaseException | None = None
        for key, fut in self.futures.items():
            exc = fut.exception()
            if exc is None:
                out[key] = fut.result()
            else:
                out[key] = exc
                if first_exc is None:
                    first_exc = exc
        self._join_tail()
        if first_exc is not None and not return_exceptions:
            raise first_exc
        return out

    def raise_first(self) -> None:
        """Block until the first failure (raising it) or all shards complete.

        A clean run drains :meth:`as_completed`, so it joins the post-run tail;
        raising on a failure leaves the tail in flight (:meth:`wait` joins it).
        """
        for fut in self.as_completed():
            exc = fut.exception()
            if exc is not None:
                raise exc

    def progress(self, **tqdm_kwargs) -> Iterator[Future]:
        """:meth:`as_completed` under a ``tqdm.auto`` bar (one tick per shard).

        tqdm is optional (the ``analysis`` extra, espg-ratified on issue
        #265); without it this raises ``ImportError`` with the install hint —
        :meth:`as_completed` is the bar-free equivalent.
        """
        try:
            from tqdm.auto import tqdm
        except ImportError as e:
            raise ImportError(
                "RunHandle.progress() needs tqdm (`pip install 'zagg[analysis]'`, "
                "issue #326); handle.as_completed() is the bar-free equivalent"
            ) from e
        kwargs: dict[str, Any] = {"total": len(self), "desc": "shards", "unit": "shard"}
        kwargs.update(tqdm_kwargs)
        return tqdm(self.as_completed(), **kwargs)

    def wait(self, timeout: float | None = None) -> None:
        """Block until every shard AND the post-run tail finish.

        Re-raises a failed finalize backstop (the manifest is required
        reader-facing schema, D6 — also warned at the moment it fails, so it is
        visible before this join); the coverage/stats/sweep rollups are
        fail-open by design and never raise here, but an exception in the
        tail's own plumbing does surface (it would otherwise vanish into the
        finisher thread). Raises ``TimeoutError`` when the tail is still
        running at ``timeout``.

        Draining a harvest iterator already joins the tail, so ``wait`` is for
        explicit control. Size ``timeout`` off the tail's own floor, not the
        shard work: the run-stats leg fires its Event invoke, then *verifies*
        the parquet landed over a read-only poll window
        (``runner._RUN_STATS_VERIFY_WINDOW_S``, 20 s as shipped) and re-fires
        once if it has not — so even a fully successful run can spend ~2x that
        window here. Anything under ~40 s risks a false ``TimeoutError``; pass
        ``None`` to just block (review finding, PR #333). Under ``output.sweep:
        "stages"`` the tail also runs the staged sweep (issue #588), bounded by
        :data:`zagg.sweep_fleet.DEFAULT_TOTAL_BARRIER_BUDGET_S` (7,200 s), so
        size ``timeout`` off that budget there, or pass ``None``.
        """
        if self._finisher is not None:
            self._finisher.join(timeout)
            if self._finisher.is_alive():
                raise TimeoutError(f"run tail still in flight after {timeout}s")
        self._raise_tail_error()

    def _join_tail(self) -> None:
        """Join the post-run finisher, then re-raise what it recorded.

        Idempotent (joining a finished thread is a no-op), so draining a
        harvest iterator and then calling :meth:`wait` is safe.
        """
        if self._finisher is not None:
            self._finisher.join()
        self._raise_tail_error()

    def _raise_tail_error(self) -> None:
        # Finalize first: it is the D6 manifest backstop, a strictly more
        # load-bearing failure than a crashed fail-open rollup leg.
        if self._finalize_error is not None:
            raise self._finalize_error
        if self._tail_error is not None:
            raise self._tail_error


class Run:
    """A configured-but-not-dispatched zagg Lambda run (issue #326).

    Build one with :meth:`from_config`; :meth:`dispatch` fans the shards out
    and returns a :class:`RunHandle` of per-shard futures. One ``Run`` may be
    dispatched more than once (each dispatch is an independent fan-out with
    its own run id and pool).
    """

    def __init__(
        self,
        config: PipelineConfig,
        catalog_data: dict,
        *,
        store: str,
        function_name: str,
        region: str,
        lambda_client=None,
        driver: str,
        handoff: str,
        profile: bool = False,
        max_retries: int = 3,
        overwrite: bool = False,
        output_credentials: dict | None = None,
        output_endpoint_url: str | None = None,
        source_credentials: dict | None = None,
    ):
        from zagg import runner

        self.config = config
        self.catalog_data = catalog_data
        self.store = store
        self.function_name = function_name
        self.region = region
        self.driver = driver
        self.handoff = handoff
        self.profile = profile
        self.max_retries = max_retries
        self.overwrite = overwrite
        self._lambda_client = lambda_client
        self._output_credentials = output_credentials
        self._output_endpoint_url = output_endpoint_url
        self._source_credentials = source_credentials

        grid_type = (config.output.get("grid") or {}).get("type", "healpix")
        self._grid_type = grid_type
        self._parent_order = get_parent_order(config) if grid_type == "healpix" else None
        self._child_order = get_child_order(config) if grid_type == "healpix" else None
        self.grid = grid_from_config(config)
        # The dispatch-time Icechunk init record (issue #580), read by the
        # tail's run-record write; None until dispatch() fires the invoke,
        # and None for good when the knob is off.
        self._icechunk_init: dict | None = None
        #: A reattached run (``Run.attach``) never held the init record: its
        #: tail finalizes the repo off the config's knob instead (issue #582).
        self._attached = False
        runner._check_signature(self.grid, catalog_data)

    def __repr__(self) -> str:
        return (
            f"Run({len(self.catalog_data['shard_keys'])} shards -> {self.store!r}, "
            f"function={self.function_name!r}, region={self.region!r})"
        )

    def __len__(self) -> int:
        return len(self.catalog_data["shard_keys"])

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig | dict | str,
        *,
        shardmap: dict | str | None = None,
        store: str | None = None,
        function_name: str | None = None,
        region: str = "us-west-2",
        lambda_client=None,
        driver: str | None = None,
        handoff: str | None = None,
        profile: bool = False,
        max_retries: int = 3,
        overwrite: bool = False,
        output_credentials: dict | None = None,
        output_endpoint_url: str | None = None,
        source_credentials: dict | None = None,
    ) -> "Run":
        """Build a :class:`Run` from a config plus a shard map.

        Parameters
        ----------
        config : PipelineConfig, dict, or str
            A loaded :class:`~zagg.config.PipelineConfig`, a plain config
            dict, or a path to a YAML config file. **Every** shape is
            cross-validated here (issue #472), including an already-built
            ``PipelineConfig`` that was mutated after ``default_config``.
        shardmap : dict or str, optional
            A loaded ShardMap manifest dict or a path to its JSON. Falls back
            to the config's ``catalog:`` key. The map's grid signature must
            match the config's grid (same guard as ``agg``).
        store : str, optional
            Output store; falls back to ``output.store:``. Must be ``s3://``
            (this facade is Lambda-only; the local backend stays on ``agg``).
        function_name : str, optional
            Lambda function name; default resolves ``ZAGG_LAMBDA_FUNCTION_NAME``
            plus the config ``worker:`` variant suffix, exactly like ``agg``.
        region : str
            AWS region (default ``us-west-2``; a config ``output.region``
            overrides the default, mirroring ``agg``).
        lambda_client : optional
            An injected boto3 Lambda client (testability / custom botocore
            config). Default ``None`` builds one per dispatch, sized to the
            fan-out (``read_timeout`` above the 900 s function ceiling). With
            an injected client neither the STS caller-identity probe nor the
            account-concurrency preflight runs, so worker stats records carry a
            null ``invoked_by`` (fail-open, issue #297) and the fan-out falls
            back to :data:`_DEFAULT_MAX_WORKERS` unless ``max_workers`` is
            given — a stub client must not have to answer either probe.
        driver, handoff, profile, max_retries, overwrite,
        output_credentials, output_endpoint_url
            Same semantics as the matching :func:`zagg.runner.agg` kwargs.
        source_credentials : dict, optional
            Explicit source-read S3 credentials (camelCase ``accessKeyId`` /
            ``secretAccessKey`` / ``sessionToken``). Default ``None`` resolves
            the config's credentials provider (NSIDC by default) at dispatch
            time, exactly like ``agg``.
        """
        from zagg import runner

        if isinstance(config, str):
            config = load_config(config)
        elif isinstance(config, dict):
            config = load_config_from_dict(config)

        # v1 scope gate: the spatial point path only. The other pipelines
        # already run through agg()/zagg.notebook.run; refusing here beats a
        # wrong fan-out (windowed units are (shard, window) pairs, not shards).
        kind = get_pipeline_type(config)
        if kind != "spatial":
            raise NotImplementedError(
                f"zagg.client v1 covers the spatial point path (got pipeline "
                f"type {kind!r}); temporal runs go through zagg.runner.agg(events=...)"
            )
        if (config.data_source or {}).get("reader") == "raster":
            raise NotImplementedError(
                "zagg.client v1 covers the spatial point path (got reader: "
                "raster); raster runs go through zagg.runner.agg / zagg.notebook.run"
            )
        if get_windowing(config) is not None:
            raise NotImplementedError(
                "zagg.client v1 dispatches one future per shard; windowed "
                "configs fan out (shard, window) units — use zagg.runner.agg / "
                "zagg.notebook.run until the v2 transport (issue #327)"
            )

        # Cross-validate EVERY input shape, not just the dict/path ones (issue
        # #472): a ``PipelineConfig`` from ``default_config`` validates once, at
        # build time, so a notebook that then grafts another template's
        # ``aggregation.variables`` onto it (the 02_write graft) dispatched a
        # config nothing had re-checked — and the error surfaced one invoke per
        # shard later, fleet-side. Runs after the scope gates above so an
        # out-of-scope pipeline still gets its "use agg" pointer rather than a
        # config error for a facade it was never going to run through, and
        # before the shard map / store resolution so nothing touches AWS first.
        validate_config(config)

        if isinstance(shardmap, dict):
            catalog_data = shardmap
            if not all(k in catalog_data for k in ("shard_keys", "granules", "grid_signature")):
                raise ValueError(
                    "shardmap dict is not a Phase-5 ShardMap (needs shard_keys "
                    "+ granules + grid_signature)"
                )
        else:
            catalog_path = shardmap or config.catalog
            if not catalog_path:
                raise ValueError("No shardmap specified (pass shardmap= or set catalog: in config)")
            catalog_data = runner._load_catalog(catalog_path)

        store_path = store or get_store_path(config)
        if not store_path:
            raise ValueError("No store path specified (pass store= or set output.store: in config)")
        store_path = effective_store_root(store_path, config)
        if not store_path.startswith("s3://"):
            raise ValueError(f"Lambda dispatch requires an s3:// store path, got: {store_path}")

        config_region = get_output_region(config)
        if config_region and region == "us-west-2":
            region = config_region

        return cls(
            config,
            catalog_data,
            store=store_path,
            function_name=runner._resolve_function_name(config, function_name),
            region=region,
            lambda_client=lambda_client,
            driver=driver or get_driver(config),
            handoff=handoff if handoff is not None else get_handoff(config),
            profile=profile,
            max_retries=max_retries,
            overwrite=overwrite,
            output_credentials=output_credentials,
            # Non-secret endpoint: runtime kwarg > config (mirrors agg).
            output_endpoint_url=output_endpoint_url or get_output_endpoint_url(config),
            source_credentials=source_credentials,
        )

    @classmethod
    def attach(
        cls,
        store: str,
        run_id: str,
        *,
        function_name: str | None = None,
        region: str = "us-west-2",
        lambda_client=None,
        output_credentials: dict | None = None,
        output_endpoint_url: str | None = None,
    ) -> RunHandle:
        """Reattach to a dispatched run by id (issue #327 phase 5).

        Rebuilds a :class:`RunHandle` from the run's dispatch manifest
        (``<store>.status/run-<run_id>/manifest.json`` — the config, shard
        set, and identity the setup invoke recorded worker-side) plus the
        current status objects: shards already settled resolve on the first
        poll tick, the rest resolve as their statuses land. A notebook kernel
        restart or a second machine can adopt a running fleet this way — the
        Event invokes are already in flight and owe the client nothing.

        Observe-only by design: the manifest carries no granule records, so a
        ``failed`` status resolves that shard's :class:`ShardError`
        immediately (no re-dispatch), and a shard with no status object by
        the drop deadline — anchored at the manifest's ``dispatched_at`` —
        resolves ``failed-unknown``. The post-run tail runs only if not
        already recorded (the run's ``tail.json`` marker, checked read-only)
        — except its Icechunk finalize (issue #582), which a reattached
        handle always fires when the config's knob is on, since the marker
        precedes it in the tail and the finalize is idempotent;
        when it does run it is the same worker-invoke tail as a live handle
        (D8 intact — this method's own store traffic is reads).

        Parameters
        ----------
        store : str
            The run's effective store root exactly as dispatched
            (``handle.store_path`` / the summary's ``store_path``).
        run_id : str
            The dispatch's run id (``manifest.json`` names it; it is also in
            every cell event and stats record).
        function_name, region, lambda_client, output_credentials,
        output_endpoint_url
            Same semantics as :meth:`from_config`; the function/client are
            only used for the tail invokes when the tail is not yet recorded.
        """
        from zagg.client_transport import attach_run

        return attach_run(
            cls,
            store,
            run_id,
            function_name=function_name,
            region=region,
            lambda_client=lambda_client,
            output_credentials=output_credentials,
            output_endpoint_url=output_endpoint_url,
        )

    # -- shard selection ----------------------------------------------------

    def _resolve_key(self, key) -> int:
        """One requested shard key -> the shardmap's raw int key.

        Ints pass through as raw shardmap keys (packed morton words for
        HEALPix); strings are the external form — the decimal morton string
        for HEALPix (issue #199), stringified ints otherwise.
        """
        if isinstance(key, str):
            if self._grid_type == "healpix":
                return morton_word(key)
            return int(key)
        return int(key)

    def _select(self, shard_keys) -> list[tuple]:
        """(shard_key, records) pairs to dispatch, biggest-work-first ordered."""
        from zagg import runner

        pairs = runner._select_cells(self.catalog_data)
        if shard_keys is not None:
            wanted = {self._resolve_key(k) for k in shard_keys}
            missing = wanted - {int(k) for k, _ in pairs}
            if missing:
                raise ValueError(f"shard key(s) not in shardmap: {sorted(missing)}")
            pairs = [(k, recs) for k, recs in pairs if int(k) in wanted]
        return runner._lambda_dispatch_order(pairs)

    # -- dispatch -----------------------------------------------------------

    def _probe_workers(self, session, n: int, *, fd_bound: bool = True) -> int:
        """Account-concurrency preflight, degrading to the default on denial.

        Runs ``agg``'s own :func:`~zagg.concurrency.compute_available_workers`
        (local FD ceiling ∩ account-wide Lambda headroom) against the work
        size, so a notebook fan-out is sized by what the account can actually
        take rather than by a hardcoded guess (espg ruling on PR #333: whoever
        can invoke the workers normally can read the concurrency too).

        ``fd_bound`` carries the transport's connection model through to the
        probe: the sync transport holds one socket per in-flight shard, so its
        window is FD-bounded; the event transport holds none and passes
        ``False`` (issue #375).

        Fail-open by design: ANY probe failure — ``AccessDenied`` under a
        narrow-permission deployment (the cryocloud case, where the worker role
        holds the perms and the caller only deploys), missing CloudWatch
        permissions, a throttle — falls back to :data:`_DEFAULT_MAX_WORKERS`
        with a ``warnings.warn`` naming the denial. A run the caller is allowed
        to dispatch must not fail on an optional preflight read. Skipped
        entirely when ``max_workers`` is explicit or a client was injected.
        """
        from zagg import runner

        try:
            workers, report = runner.compute_available_workers(
                n,
                session.client("lambda", region_name=self.region),
                session.client("cloudwatch", region_name=self.region),
                self.function_name,
                fd_bound=fd_bound,
            )
            runner._log_concurrency_report(report, workers)
            return workers
        except Exception as e:
            warnings.warn(
                f"account-concurrency preflight failed ({type(e).__name__}: {e}); "
                f"sizing the fan-out at the default max_workers="
                f"{_DEFAULT_MAX_WORKERS} instead. Pass max_workers= to set it "
                f"explicitly and skip the probe.",
                RuntimeWarning,
                stacklevel=3,
            )
            return _DEFAULT_MAX_WORKERS

    def dispatch(
        self,
        shard_keys=None,
        max_workers: int | None = None,
        transport: str = "sync",
    ) -> RunHandle:
        """Fan the shards out; return a :class:`RunHandle` of per-shard futures.

        Returns as soon as the setup handshake completes (worker-side template
        / manifest write — one short synchronous invoke; hive additionally
        fail-fast-pings first): the fan-out itself runs in the background and
        each shard's outcome arrives through its future. The config ships
        with its Icechunk ``commit`` mode pinned the way ``runner._run_lambda``
        pins it (issue #588): ``ladder`` when ``output.sweep: "stages"`` walks
        a ``/2`` ladder — the tail chains that staged sweep — else ``leaf``.
        Under ``sweep: "stages"`` the handle's tail chains the staged sweep
        whatever the mode, so draining it can block up to the fleet's total
        barrier budget (see :class:`RunHandle`).

        Parameters
        ----------
        shard_keys : iterable, optional
            Subset of shards to dispatch — raw int shardmap keys or external
            string labels (decimal morton for HEALPix). Default all shards.
        max_workers : int, optional
            Fan-out width, clamped to the shard count. An explicit value wins
            verbatim and skips the preflight probe; omitted, the account
            concurrency probe sizes it (see :meth:`_probe_workers`) and
            :data:`_DEFAULT_MAX_WORKERS` is the fallback when it cannot run.
            Under ``transport="sync"`` this is the invoke thread-pool width;
            under ``"event"`` it is the dispatch **pacing window** — how many
            shards may be dispatched-but-unresolved at once, load-bearing
            because the fleet's 60 s ``MaximumEventAgeInSeconds`` silently
            drops an Event invoke the account has no concurrency to start.
            The two transports therefore probe differently (issue #375): the
            sync pool is clamped by the local ``RLIMIT_NOFILE`` ceiling (one
            held socket per in-flight shard), while the event window — which
            holds no connections — is sized by account headroom alone, so a
            low ``ulimit -n`` no longer strands concurrency the account has
            already granted. The window is fired serially by the poller thread
            (:meth:`~zagg.client_transport.StatusPoller._dispatch_up_to_window`),
            so a wider window also lengthens the opening dispatch ramp
            proportionally — roughly 10 ms of invoke round-trip per shard, and
            no LIST runs during it. Deliberate: the ramp grows by seconds while
            the clamp cost whole 900 s waves (issue #375).
        transport : str
            ``"sync"`` (default, the v1 transport): a thread pool over
            synchronous ``RequestResponse`` invokes — one held connection per
            in-flight shard. ``"event"`` (the v2 transport, issue #327):
            fire-and-forget ``InvocationType="Event"`` invokes resolved by a
            single poller thread from one LIST of the run's status prefix per
            tick — no held connections, and the run survives client exit
            (reattachable via :meth:`attach`). Futures, harvest iterators,
            ``status()``, and the post-run tail behave identically; the retry
            policy splits by fault class (see
            :class:`~zagg.client_transport.StatusPoller`) — ``max_retries``
            still bounds invoke-layer faults, a worker-reported failure is
            terminal, and a shard whose status object never lands re-fires once
            then resolves ``failed-unknown``. Requires a deployed worker that
            writes status objects (issue #327) and read access to the status
            prefix for the poll.
        """
        from zagg import concurrency, runner

        if transport not in ("sync", "event"):
            raise ValueError(f'transport must be "sync" or "event", got {transport!r}')
        cells = self._select(shard_keys)
        if not cells:
            raise ValueError("no shards selected")
        n = len(cells)

        client = self._lambda_client
        invoked_by = None
        if client is not None:
            # Injected client (the test seam): no probe, no STS — a stub must
            # not need to answer GetAccountSettings or get-caller-identity.
            workers = min(max_workers or _DEFAULT_MAX_WORKERS, n)
        else:
            import boto3
            from botocore.config import Config

            session = boto3.Session()
            # The FD ceiling bounds the SYNC pool (one held socket per in-flight
            # shard); the event transport holds no connections, so its pacing
            # window comes from account headroom alone (issue #375).
            requested = (
                max_workers
                if max_workers is not None
                else self._probe_workers(session, n, fd_bound=transport == "sync")
            )
            workers = max(1, min(requested, n))
            # read_timeout must exceed the 900 s function ceiling (same
            # rationale as agg's preflight-built client, issue #148); the
            # connection pool tracks the fan-out width, but must never exceed
            # the fd ceiling regardless of pacing window — the event window
            # may clear RLIMIT_NOFILE (issue #375), a pool of sockets may not.
            # Through the module seam, so the fd ceiling has one patch point.
            client = session.client(
                "lambda",
                region_name=self.region,
                config=Config(
                    read_timeout=960,
                    connect_timeout=10,
                    retries={"max_attempts": 0},
                    max_pool_connections=min(workers, concurrency.fd_safe_max_workers()),
                ),
            )
            invoked_by = runner._resolve_invoked_by(session, self.region)

        # Pre-invoke cost ceiling (issue #298): displayed, never gated — the
        # notebook path never blocks on cost (ratified on issue #298).
        memory_gb = runner._worker_memory_gb(self.config)
        ceiling = max_cost_usd(n, memory_gb, timeout_s=runner._DEFAULT_FUNCTION_TIMEOUT_S)
        logger.info(
            f"Max cost ceiling: ~${ceiling:.2f} ({n} units x {memory_gb:g} GB x "
            f"{runner._DEFAULT_FUNCTION_TIMEOUT_S}s, {LAMBDA_ARCH})"
        )

        s3_creds = self._source_credentials or runner._resolve_source_credentials(self.config)
        # The Icechunk commit mode ships resolved (#580): the tail chains the
        # staged sweep under ``output.sweep: "stages"`` exactly as
        # ``runner._run_lambda`` does (issue #588), so the same pin applies —
        # ``ladder`` when the run walks it, ``leaf`` otherwise.
        config_dict = asdict(runner._pin_icechunk_commit(self.config, self.grid, stages=True))
        output_creds_event = runner._build_output_creds_event(
            self._output_credentials, self._output_endpoint_url, self.region
        )
        run_id = uuid.uuid4().hex

        # Dispatch manifest block (issue #327): rides the setup invoke so the
        # WORKER records the run's shard set + identity at the status prefix
        # (D8) — what :meth:`Run.attach` rebuilds a handle from.
        from zagg.client_transport import build_run_manifest_block

        md = self.catalog_data.get("metadata") or {}
        run_manifest = build_run_manifest_block(
            run_id,
            [k for k, _ in cells],
            self.config,
            dataset={"short_name": md.get("short_name"), "version": md.get("version")},
        )

        # Setup handshake, synchronous and load-bearing (worker-side writes
        # only — D8). Hive: fail-fast ping + fire-and-forget manifest write;
        # finalize below is the idempotent backstop (issue #252 hybrid). Flat:
        # the template write itself.
        dataset = None
        if get_store_layout(self.config) == "hive":
            dataset = {"short_name": md.get("short_name"), "version": md.get("version")}
            runner._invoke_lambda_ping(
                client,
                self.function_name,
                self.store,
                config_dict=config_dict,
                dataset=dataset,
                parent_order=self._parent_order,
                overwrite=self.overwrite,
                output_creds_event=output_creds_event,
            )
            runner._invoke_lambda_setup_async(
                client,
                self.function_name,
                self.store,
                config_dict=config_dict,
                dataset=dataset,
                parent_order=self._parent_order,
                overwrite=self.overwrite,
                output_creds_event=output_creds_event,
                run_manifest=run_manifest,
            )
            # Icechunk companion init (issue #580): synchronous, before the
            # fan-out, fail-open — the same seam ``runner._run_lambda`` takes.
            # The record is kept so the tail's run-record write carries the
            # repo and its init snapshot, exactly as the runner path does —
            # a failed init has to be recorded, not invisible (D9).
            if get_icechunk(self.config):
                self._icechunk_init = runner._invoke_lambda_icechunk_init(
                    client,
                    self.function_name,
                    self.store,
                    config_dict=config_dict,
                    parent_order=self._parent_order,
                    run_id=run_id,
                    output_creds_event=output_creds_event,
                )
        else:
            runner._invoke_lambda_setup(
                client,
                self.function_name,
                self.store,
                parent_order=self._parent_order,
                child_order=self._child_order,
                overwrite=self.overwrite,
                config_dict=config_dict,
                output_creds_event=output_creds_event,
                run_manifest=run_manifest,
            )

        aoi_by_shard = runner._aoi_payload_map(self.catalog_data)
        result_prefix = None
        if transport == "event":
            from zagg.client_transport import run_status_prefix

            # The tail's oversized-run-record pointer (issue #327): only the
            # event transport has a per-run prefix to point at.
            result_prefix = run_status_prefix(self.store, run_id)
            futures, closer = self._dispatch_event(
                client,
                cells,
                run_id=run_id,
                workers=workers,
                config_dict=config_dict,
                s3_creds=s3_creds,
                output_creds_event=output_creds_event,
                aoi_by_shard=aoi_by_shard,
                invoked_by=invoked_by,
            )
        else:
            pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="zagg-client")
            futures = {}
            for key, records in cells:
                futures[int(key)] = pool.submit(
                    self._shard_work,
                    client,
                    int(key),
                    records,
                    config_dict=config_dict,
                    s3_creds=s3_creds,
                    output_creds_event=output_creds_event,
                    aoi_payload=aoi_by_shard.get(int(key)),
                    invoked_by=invoked_by,
                    run_id=run_id,
                    max_workers=workers,
                )
            closer = pool

        handle = RunHandle(futures, store_path=self.store, memory_gb=memory_gb)
        finisher = threading.Thread(
            target=self._post_run,
            args=(handle, client, closer, config_dict, dataset, output_creds_event, run_id),
            kwargs={"result_prefix": result_prefix},
            name="zagg-client-finish",
            daemon=True,
        )
        handle._finisher = finisher
        finisher.start()
        return handle

    def _dispatch_event(
        self,
        client,
        cells,
        *,
        run_id: str,
        workers: int,
        config_dict: dict,
        s3_creds: dict,
        output_creds_event: dict | None,
        aoi_by_shard: dict,
        invoked_by: dict | None,
    ) -> tuple[dict[int, Future], Any]:
        """v2 Event transport (issue #327): fire-and-forget invokes + one poller.

        Builds each shard's event through the SAME construction the sync/agg
        paths use (``runner._build_cell_event`` — payloads are byte-identical
        modulo InvocationType), registers it with a
        :class:`~zagg.client_transport.StatusPoller` that paces dispatch to
        the ``workers`` window and resolves every future from one LIST of the
        run's status prefix per tick, and returns ``(futures, poller)`` — the
        poller is the finisher's ``closer``. D8 audit: this path's only store
        traffic is the poller's LIST/GETs (reads); every write stays behind a
        worker invoke. A terminal failure (a ``failed`` status, or the
        ``failed-unknown`` drop outcome past its one re-fire — see the
        three-class retry policy on
        :class:`~zagg.client_transport.StatusPoller`) raises
        :class:`ShardError` from the shard's future, exactly like v1.
        """
        from zagg.client_transport import dispatch_event_shards

        return dispatch_event_shards(
            self,
            client,
            cells,
            run_id=run_id,
            workers=workers,
            config_dict=config_dict,
            s3_creds=s3_creds,
            output_creds_event=output_creds_event,
            aoi_by_shard=aoi_by_shard,
            invoked_by=invoked_by,
            on_failed=_fail_with_shard_error,
        )

    def _shard_work(
        self,
        client,
        shard_key: int,
        records: list,
        *,
        config_dict: dict,
        s3_creds: dict,
        output_creds_event: dict | None,
        aoi_payload,
        invoked_by: dict | None,
        run_id: str,
        max_workers: int,
    ) -> dict:
        """One synchronous shard invoke; the pool future over this IS the API.

        Mirrors ``_run_lambda``'s ``_cell_work`` payload construction (labels,
        granule-worker clamp, leaf sub-map) minus the async result channel —
        v1 is the legacy sync ``RequestResponse`` path by design.

        The url selection honors ``data_source.driver`` — as does ``_cell_work``
        since the espg ruling on PR #333 (it hardcoded ``"s3"`` before), so the
        two paths build identical events for an ``https`` config too;
        ``TestRunnerParity`` asserts that field-by-field. It also resolves
        through ``_resolve_granule_entries`` (issue #425), so a paired-asset
        map's sibling hrefs reach the worker here as they do on the agg path —
        a plain URL string per granule for every single-asset map.
        """
        from zagg import runner

        label = runner._safe_label(self.grid, shard_key)
        granule_urls = runner._resolve_granule_entries(records, self.driver)
        ds = runner._clamped_data_source(dict(self.config.data_source), len(granule_urls))
        cell_config = {**config_dict, "data_source": ds} if ds is not None else config_dict
        submap = {
            "grid_signature": self.catalog_data["grid_signature"],
            "metadata": runner._submap_metadata(self.catalog_data.get("metadata")),
            "granules": records,
        }
        result = runner._invoke_lambda_cell(
            client,
            self.grid.block_index(shard_key),
            shard_key,
            self._parent_order,
            self._child_order,
            granule_urls,
            self.store,
            s3_creds,
            function_name=self.function_name,
            config_dict=cell_config,
            output_creds_event=output_creds_event,
            max_retries=self.max_retries,
            max_workers=max_workers,
            handoff=self.handoff,
            profile=self.profile,
            aoi_payload=aoi_payload,
            label=label,
            invoked_by=invoked_by,
            run_id=run_id,
            submap=submap,
            semantic_hash=runner._fleet_skip_hash(self.config, self.overwrite),
        )
        error = result.get("error")
        if result.get("status_code") == 200 and not error:
            return result
        if error in BENIGN_ERRORS:
            # "Nothing to do" is a normal outcome, not a failure — matching
            # _run_lambda's error counting (neither with_data nor error).
            return result
        detail = error or f"status {result.get('status_code')}"
        raise ShardError(
            f"shard {label}: {detail}",
            shard_key=shard_key,
            label=label,
            payload=result,
        )

    def _post_run(
        self,
        handle: RunHandle,
        client,
        closer,
        config_dict: dict,
        dataset: dict | None,
        output_creds_event: dict | None,
        run_id: str,
        tail_done_check=None,
        result_prefix=None,
    ) -> None:
        """Finisher-thread entry point: run the tail, never lose its failure.

        Anything the tail raises outside its own fail-open guards would
        otherwise die in ``threading.excepthook`` while ``wait()`` reported a
        clean run, so it is recorded on the handle as ``_tail_error`` (kept
        distinct from the D6-load-bearing ``_finalize_error``) and ``closer``
        — the v1 invoke pool or the v2 status poller, both
        ``shutdown(wait=False)``-shaped — is shut down either way (review
        finding, PR #333). ``tail_done_check`` (issue #327, the attach path):
        a zero-arg read-only predicate consulted AFTER every shard settles —
        True means the tail was already recorded (the run's ``tail.json``
        marker), so a reattached handle never re-runs it. ``result_prefix``
        (also #327) is the oversized-run-record pointer — see
        :meth:`_run_tail`.
        """
        try:
            if tail_done_check is not None:
                futures_wait(list(handle.futures.values()))
                if tail_done_check():
                    # The marker precedes the Icechunk finalize in the tail,
                    # so a client that died between the two left the run
                    # untagged: finalize anyway (idempotent — an existing tag
                    # returns at once), then skip the rest (issue #582).
                    logger.info(f"post-run tail already recorded for run {run_id}; skipping")
                    self._finalize_icechunk(handle, client, config_dict, output_creds_event, run_id)
                    return
            self._run_tail(
                handle,
                client,
                config_dict,
                dataset,
                output_creds_event,
                run_id,
                result_prefix=result_prefix,
            )
        except BaseException as e:
            handle._tail_error = e
            logger.warning(f"post-run tail failed (surfaced via handle.wait()): {e}")
        finally:
            closer.shutdown(wait=False)

    def _run_tail(
        self,
        handle: RunHandle,
        client,
        config_dict: dict,
        dataset: dict | None,
        output_creds_event: dict | None,
        run_id: str,
        result_prefix=None,
    ) -> None:
        """After every shard settles: the same worker-invoke tail as ``agg``.

        Finalize backstop (hive always — the idempotent manifest self-heal;
        flat only under ``consolidate_metadata``), then the fail-open rollups:
        root ``coverage.moc``, the run-stats record, and the sweep trigger —
        each a fire-and-forget worker invoke (D8), each swallowed on failure
        exactly like ``_run_lambda``'s tail — then, under ``output.sweep:
        "stages"``, the STAGED sweep (issue #588; the same
        :func:`runner._invoke_lambda_stage_sweep` seam, invoke-and-poll, never
        a write) and last the Icechunk finalize, gated like the CLI's on a
        completed staged sweep (:func:`runner._staged_sweep_incomplete`). The
        tail marker precedes the staged sweep, as on the CLI. A reattached
        handle chains no staged sweep (:meth:`Run.attach` is observe-only;
        the sweep is the dispatcher's). The staged sweep blocks this daemon
        finisher up to :data:`zagg.sweep_fleet.DEFAULT_TOTAL_BARRIER_BUDGET_S`
        (7,200 s); a process exit or kernel restart meanwhile cuts it off while
        it holds the store lease, whose TTL expiry is the designed recovery
        (``runner._invoke_lambda_stage_sweep``, "If the dispatcher dies
        mid-barrier") — the run is then left untagged. A finalize failure goes
        through the shared :func:`runner._finalize_with_retry` (issue #335 — one contract
        for the CLI and the facade): it warns immediately (``RuntimeWarning`` —
        notebook-visible while the tail is still running), retries once after a
        fixed 5 s backoff, and only a still-failing finalize is recorded on the
        handle to re-raise from :meth:`RunHandle.wait` / a drained harvest
        iterator. The backoff costs nothing interactively — the tail already
        runs on the finisher thread, so it delays the join, not the cell.

        Every shard contributes exactly one result dict, so every shard gets a
        run-stats row: a worker envelope, a :class:`ShardError` payload, or —
        for a transport-level exception that never produced an envelope (the
        payload-cap ``ValueError``, an fd-exhaustion re-raise, a botocore error
        escaping the retry loop) — a synthesized failure dict, which
        ``_lambda_result_rows`` turns into a ``failure_record`` row rather than
        dropping the shard from telemetry (review finding, PR #333).

        ``result_prefix`` is the pointer ``_dispatch_run_stats`` falls back to
        when the row set exceeds the inline payload cap. The v1 sync transport
        has none (no #151 envelope mirror), so an oversized sync run skips its
        record; the v2 event transport DOES — the run's status prefix, which
        ``telemetry.rows_from_status`` reads (issue #327) — and fleet-scale
        runs are exactly the ones that hit the cap, so passing it is what keeps
        ``Run.dispatch(transport="event")`` from silently dropping the run
        record that the byte-identical ``agg(invocation="event")`` keeps
        (``runner.py`` does the same fallback at its own stats dispatch).
        """
        from zagg import runner

        futures = list(handle.futures.values())
        futures_wait(futures)
        results = []
        for key, fut in handle.futures.items():
            exc = fut.exception()
            if exc is None:
                results.append(fut.result())
            elif isinstance(exc, ShardError):
                results.append(exc.payload)
            else:
                results.append(
                    {
                        "shard_key": key,
                        "status_code": None,
                        "body": {},
                        "error": f"{type(exc).__name__}: {exc}",
                        "retries": None,
                    }
                )
        layout = get_store_layout(self.config)
        # ONE warn/retry/record implementation for every dispatch path (issue
        # #335 phase 1): agg's _run_lambda, the raster driver and this finisher
        # all route through runner._finalize_with_retry, so the message contract
        # and the retry cannot drift between the CLI and the facade. It warns AT
        # the failure (espg ruling on PR #333, middle option) so a notebook sees
        # it in-stream rather than only at the join, retries once, and RETURNS
        # the error — the handle keeps this path's re-raise semantics.
        finalize_error = None
        finalize_kwargs: dict = {}
        if layout == "hive":
            finalize_kwargs = {
                "config_dict": config_dict,
                "dataset": dataset,
                "parent_order": self._parent_order,
                "overwrite": self.overwrite,
            }
        if layout == "hive" or get_consolidate_metadata(self.config):
            error = runner._finalize_with_retry(
                lambda: runner._invoke_lambda_finalize(
                    client,
                    self.function_name,
                    self.store,
                    output_creds_event=output_creds_event,
                    **finalize_kwargs,
                ),
                store_path=self.store,
                reraise_note=(
                    "This error also re-raises from handle.wait() / draining the harvest iterator."
                ),
            )
            if error is not None:
                finalize_error = str(error)
                handle._finalize_error = error
                logger.warning(f"finalize invoke failed (surfaced via handle.wait()): {error}")

        ok_results = [r for r in results if r.get("status_code") == 200 and not r.get("error")]
        if layout == "hive" and get_coverage_moc(self.config):
            try:
                from zagg.hive import build_root_coverage
                from zagg.windows import union_time_range

                done = [r["shard_key"] for r in ok_results]
                # hive is HEALPix-only (validated), so parent_order is set here.
                if done and self._parent_order is not None:
                    envelope = build_root_coverage(
                        done,
                        int(self._parent_order),
                        time_range=union_time_range(
                            *(r.get("body", {}).get("time_range") for r in ok_results)
                        ),
                    )
                    runner._invoke_lambda_coverage(
                        client,
                        self.function_name,
                        self.store,
                        envelope,
                        output_creds_event=output_creds_event,
                    )
            except Exception as e:
                logger.warning(f"root coverage.moc dispatch failed (fail-open, D9): {e}")

        # Run-record + sweep, both fail-open (issues #297/#313/#300). The
        # oversized-rows pointer is the run's v2 status prefix on an event
        # run (rows_from_status reads that object shape — issue #327) and
        # None on the sync transport, which has no #151 envelope mirror to
        # point at; the same fallback runner.py applies. The worker also
        # records the tail-completion marker (issue #327) so a reattached
        # handle knows this tail ran.
        from zagg.client_transport import TAIL_NAME, run_status_prefix

        stats_rows, stats_inline = runner._lambda_result_rows(results, run_id=run_id)
        runner._dispatch_run_stats(
            client,
            self.function_name,
            self.store,
            stats_rows,
            run_id=run_id,
            result_prefix=result_prefix,
            output_creds_event=output_creds_event,
            store_kwargs=runner._output_store_kwargs(output_creds_event, self.region),
            inline_rows=stats_inline,
            # Same wire contract as runner's tail (issue #335): always present,
            # None on success, so the parquet's column set does not vary — and
            # it is what makes the worker WITHHOLD the tail marker on a failed
            # finalize, so a reattached handle re-runs the idempotent backstop
            # instead of reading "recorded" as "succeeded" (review, PR #343).
            finalize_error=finalize_error,
            tail_status_url=f"{run_status_prefix(self.store, run_id)}/{TAIL_NAME}",
            # The init record dispatch() kept (issue #580): the run-level
            # icechunk columns broadcast over every row, so a run's leaves
            # join to the repo history they were committed into. None when
            # the knob is off — the same value runner._run_lambda threads.
            icechunk_init=self._icechunk_init,
        )
        # The finalize gate is decided from the config BEFORE the try (review,
        # PR #591): any exception inside the block — a malformed stats record,
        # a failed rollup invoke — then leaves ``stage_chained`` set with
        # ``handle.stage_sweep`` None, so the finalize records ``{skipped}``
        # rather than tagging a tip the staged sweep never reached. Cleared
        # only on the explicit no-work branch below.
        sweeps = layout == "hive" and get_sweep(self.config)
        stage_chained = (
            sweeps and self.config.output.get("sweep") == "stages" and not self._attached
        )
        if sweeps:
            try:
                from zagg.sweep import dirt_only_leaves, leaves_from_stats_records

                ok_bodies = [r.get("body") or {} for r in ok_results]
                leaves = leaves_from_stats_records([b.get("stats") for b in ok_bodies])
                if leaves:
                    runner._invoke_lambda_sweep(
                        client,
                        self.function_name,
                        self.store,
                        leaves,
                        output_creds_event=output_creds_event,
                    )
                # Post-fleet STAGED chaining (issues #384/#519, here #588) —
                # the same opt-in and the same seam as ``_run_lambda``'s
                # tail: invoke and poll the stage records, never write (D8);
                # fail-open (D9: ``python -m zagg.sweep --stages`` is the
                # backstop). Touched current units ride as dirt-only (#580).
                # Not on a reattached handle: attach is observe-only, and the
                # sweep (with its lease) is the dispatcher's. Hive is
                # HEALPix-only (validated), so parent_order is set here; were
                # it not, ``int(None)`` raises into the fail-open gate, as on
                # the CLI.
                dirt_only = dirt_only_leaves(ok_bodies)
                if stage_chained and not (leaves or dirt_only):
                    stage_chained = False
                if stage_chained:
                    logger.info(
                        f"chaining staged sweep over {len(leaves)} leaves "
                        f"({len(dirt_only)} dirt-only) for run {run_id}"
                    )
                    handle.stage_sweep = runner._invoke_lambda_stage_sweep(
                        client,
                        self.function_name,
                        self.store,
                        leaves,
                        shard_order=int(self._parent_order),
                        output_creds_event=output_creds_event,
                        store_kwargs=runner._output_store_kwargs(output_creds_event, self.region),
                        touch_policy=get_touch_policy(self.config),
                        dirt_only=dirt_only,
                    )
            except Exception as e:
                logger.warning(f"rollup sweep dispatch failed (fail-open, D9): {e}")
        self._finalize_icechunk(
            handle,
            client,
            config_dict,
            output_creds_event,
            run_id,
            skip=runner._staged_sweep_incomplete(handle.stage_sweep) if stage_chained else None,
        )

    def _finalize_icechunk(
        self,
        handle: RunHandle,
        client: Any,
        config_dict: dict,
        output_creds_event: dict | None,
        run_id: str,
        skip: str | None = None,
    ) -> None:
        """The tail's Icechunk run finalize (issue #582); the record on the handle.

        Fired AFTER every commit of the run landed — after the fan-out under
        ``commit: "leaf"``, after the staged sweep the tail chained under the
        ladder (issue #588) — synchronous, fail-open, the same seam
        ``runner._run_lambda`` takes and the same gate: ``skip`` is
        :func:`runner._staged_sweep_incomplete`'s reason when the chained
        sweep may still have node commits in flight, and the run is then left
        untagged with ``{"skipped": reason}`` on the handle (the next run's
        tag covers it, spec §11.4; ``python -m zagg.icechunk_ops <store>
        finalize <run_id>`` tags it once the ladder is complete).
        Gated on the dispatch's init record, or — on a reattached run, which
        never held one — on the config's knob (``get_icechunk``; the worker
        refuses a repo-less store and the invoke fails open). A reattached
        run finalizes only under the manifest config's pinned ``commit:
        "leaf"`` AND no ``sweep: "stages"``: a ``sweep: "stages"`` run's
        dispatcher chains the staged sweep after the tail marker whatever the
        commit mode (its nodes commit overview refs under ``"leaf"`` too), so
        its last commits land there, its finalize is that dispatcher's alone
        and attach never fires it (``{"skipped"}`` on the handle). An
        attached finalize is ``newest_only``: a later run on the repo leaves
        this one untagged (the next run's tag covers it, spec §11.4).
        """
        from zagg import runner

        if self._icechunk_init is None:
            if not (self._attached and get_icechunk(self.config)):
                return
            if (
                get_icechunk_options(self.config)["commit"] != "leaf"
                or self.config.output.get("sweep") == "stages"
            ):
                handle.icechunk_finalize = {
                    "skipped": 'attached sweep: "stages" run; its dispatcher finalizes '
                    "after the staged sweep"
                }
                return
        elif skip is not None:
            logger.warning(f"icechunk finalize skipped, run left untagged (issue #582): {skip}")
            handle.icechunk_finalize = {"skipped": skip}
            return
        handle.icechunk_finalize = runner._invoke_lambda_icechunk_finalize(
            client,
            self.function_name,
            self.store,
            config_dict=config_dict,
            run_id=run_id,
            icechunk_init=self._icechunk_init,
            output_creds_event=output_creds_event,
            newest_only=self._attached,
        )
