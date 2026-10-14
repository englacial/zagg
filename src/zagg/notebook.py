"""Notebook-facing dispatch wrapper: progress bar + rich run report (issue #298).

:func:`run` wraps :func:`zagg.runner.agg` with a per-shard progress bar driven
by the runner's ``on_progress`` callback (one tick per completed work unit,
with the running Lambda cost in the postfix) and returns a :class:`RunView`
whose ``_repr_html_`` renders the cost block, run counters, and failures in
Jupyter. The notebook path **never blocks on cost** (ratified on issue #298):
the pre-invoke ceiling is displayed, not confirmed. The CLI's yes/no gate
(:func:`confirm_max_cost`) lives here too and is wired in ``zagg.__main__``
behind ``--yes``/``-y``. The preview prices every leg of a run -- the leaf
fan-out, the staged sweep, the families pass and the tail's fixed invokes --
from the template and the shard map alone (issue #660).

tqdm is an *optional* import -- it already rides the dependency closure on
orchestrator installs (earthaccess -> pqdm -> tqdm) but is deliberately not a
zagg dependency: a bar when importable, a logging fallback otherwise. Workers
never import this module, so the ``lambda`` extra is untouched.
"""

from __future__ import annotations

import html
import logging

from zagg.config import (
    PipelineConfig,
    chains_stages,
    get_consolidate_metadata,
    get_coverage_moc,
    get_icechunk,
    get_parent_order,
    get_pipeline_type,
    get_pyramid,
    get_store_layout,
    get_sweep,
    get_windowing,
    validate_config,
)
from zagg.dispatch import BENIGN_ERRORS, LAMBDA_ARCH, max_cost_usd
from zagg.dispatch_guard import describe

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-invoke ceiling
# ---------------------------------------------------------------------------

#: Per-leg TYPICAL invoke durations (s) behind the preview's ``expected``
#: column (issue #660); ``max_cost_preview(typical_s=...)`` overrides any of
#: them. The ceiling column prices every invoke to the 900 s wall, which for
#: the stage leg is ~30x its real bill, so a ceiling alone would be useless
#: there. ``leaves``: the measured p50 of the 2026-10-12 v4 California
#: build's leaf successes. ``stage``: the same run's staged sweep, 26 s p50
#: over 2,093 invokes (``stage-20261012T164317Z-5e45d0``, issue #659),
#: rounded up. ``families`` and ``tail`` (the ping, setup, finalize, record
#: and finisher invokes): unmeasured; the stage figure stands in. Refresh
#: from a run's own status/stage records when they move.
TYPICAL_S = {"leaves": 700.0, "stage": 30.0, "families": 30.0, "tail": 30.0}

#: The legs in print order: ``(key, label)``.
_LEGS = (
    ("leaves", "leaves"),
    ("stage", "staged sweep"),
    ("families", "families pass"),
    ("tail", "setup/finalize"),
)


def _leaf_refs(units) -> list:
    """``(shard_key, window label | None)`` per leaf the dispatch units would write.

    What :func:`zagg.sweep.leaves_from_stats_records` reads off the run's
    records after the fan-out, read here off the planned units instead: a
    ``unit: shard`` unit carries ``[(payload, subset), ...]``, a per-window
    unit one payload, an unwindowed pair none (:func:`zagg.runner._unit_windows`).
    """
    from zagg import runner

    refs = []
    for unit in units:
        window, windows = runner._unit_windows(unit)
        if windows is not None:
            refs.extend((unit[0], payload["label"]) for payload, _subset in windows)
        else:
            refs.append((unit[0], None if window is None else window["label"]))
    return refs


def stage_invokes(leaves, shard_order: int, *, windowed: bool, all_time: bool) -> int:
    """The staged sweep's invoke count for a work set, from the dispatcher's own parts (issue #660).

    Walks the schedule :func:`zagg.sweep_fleet.run_stage_sweep_fleet` takes
    from the run tail -- :func:`zagg.sweep_partition.sized_stage_tuples`
    against the dense bound, since the tail hands in no coverage MOC -- and
    at each tuple's dispatch order counts what that dispatcher fires at the
    ruled one node per invoke: the dispatch nodes' units from
    :func:`zagg.sweep_units.stage_units`, one invoke per ``(node, window)``
    plus the node's close on a windowed store, one per node otherwise. The
    dispatch nodes are the work set's ancestors
    (:func:`zagg.sweep_fleet.dispatch_nodes`), so the count is the RUN's
    own: on an append the invoke count is unchanged, but what each stage
    worker folds is the union with the store's root ``coverage.moc``, which
    is not knowable offline (D8) -- and an operator-driven pass that hands in
    ``coverage=`` widens the node set and may narrow the tuples, neither of
    which this previews. The stage finisher is one more invoke, counted in
    the tail leg.
    """
    from zagg.sweep import _normalize_leaves
    from zagg.sweep_fleet import dispatch_nodes
    from zagg.sweep_partition import sized_stage_tuples
    from zagg.sweep_units import stage_units

    by_shard, _skipped = _normalize_leaves(leaves, shard_order)
    total = 0
    for stage in sized_stage_tuples(shard_order):
        dispatch = int(stage["dispatch"])
        nodes = dispatch_nodes(by_shard, dispatch)
        units = stage_units(
            by_shard, dispatch, windowed=windowed, all_time=all_time, candidates=nodes
        )
        total += sum(len(unit["windows"]) + int(unit["close"]) for unit in units)
    return total


def _leg(invokes: int, memory_gb: float, timeout_s: float, typical_s: float) -> dict:
    """One leg's row: its ceiling (every invoke to the wall) and expected bill."""
    return {
        "invokes": invokes,
        "memory_gb": memory_gb,
        "ceiling_usd": max_cost_usd(invokes, memory_gb, timeout_s=timeout_s),
        "expected_usd": max_cost_usd(invokes, memory_gb, timeout_s=min(typical_s, timeout_s)),
    }


def max_cost_preview(
    config: PipelineConfig,
    catalog: str | None = None,
    *,
    max_cells: int | None = None,
    morton_cell: str | None = None,
    typical_s: dict | None = None,
) -> dict:
    """Price every leg of a Lambda run from shardmap + config alone (issue #660).

    Mirrors the dispatching path's unit accounting -- cell selection
    (``max_cells``/``morton_cell``) and the unit expansion -- so the previewed
    ceiling equals the one the run logs before fan-out. The expansion is not
    mirrored but SHARED: :func:`~zagg.runner._expand_units` is the one the
    dispatch paths call, which is what keeps that equality true (issue #648).
    A temporal run has no shardmap ceiling -- its fan-out unit is the
    ``events=`` item resolved at call time, not a catalog shard -- so this
    raises ``ValueError``.

    Legs, each ``{invokes, memory_gb, ceiling_usd, expected_usd}`` under
    ``legs`` (issue #660; a leg the run does not fire has ``invokes`` 0):

    - ``leaves`` -- the dispatch units x ``worker.memory``. The pre-#660
      preview, and still what the top-level keys ``n_units``, ``memory_gb``,
      ``arch``, ``timeout_s`` and ``max_cost_usd`` describe, unchanged.
    - ``stage`` -- the staged sweep, when the tail chains it (a hive store
      whose resolved sweep mode is ``"stages"``; the raster tail never
      chains): :func:`stage_invokes` over the planned leaves at the config's
      shard order, on :data:`zagg.runner.STAGE_SWEEP_WORKER`'s tier.
    - ``families`` -- the separate families pass, when the tail fires one
      (a hive store with the sweep on whose rollups do NOT ride the staged
      sweep's node closes, :func:`zagg.runner._cascade_families`): one
      invoke when :func:`zagg.sweep_partition.families_partitions` is 1 (that
      pass is the finisher), else one per NON-EMPTY bucket of
      :func:`zagg.sweep_partition.partition_leaves` plus the finisher -- what
      :func:`zagg.sweep_fleet.run_families_sweep_fleet` fires.
    - ``tail`` -- the fixed invokes, per layout and path under the tails' own
      gates. Hive aggregation (:func:`zagg.runner._run_lambda`): ping, async
      setup, finalize backstop, run stats, the root ``coverage.moc`` write
      (``get_coverage_moc``), the Icechunk init and finalize
      (``get_icechunk``) and the stage finisher (staged sweep). Flat
      aggregation: sync setup, run stats, and finalize only under
      ``get_consolidate_metadata``. Hive raster (``RasterStrategy.run``):
      ping, async setup, finalize, run stats and the coverage write. Flat
      raster: ping, raster setup, run stats. An upper count, and on a run
      whose leaves land the only counted invoke that may not fire is the
      hive aggregation finalize backstop, skipped when the init-time
      manifest write landed.

    ``ceiling_usd`` prices every invoke to the wall and ``expected_usd`` at
    its leg's typical duration (:data:`TYPICAL_S`, overridden per leg by
    ``typical_s``); both are summed under the same keys at the top level,
    and the ceiling total is what the CLI gate blocks on. No AWS access and
    no grid-signature check: this is display math, not dispatch. The
    ceiling is NOT scaled by the write side: Lambda never bills past the
    function timeout, so it already bounds the billed wall
    (``duration_total_s``, issue #589).
    """
    from zagg import runner
    from zagg.grids import from_config
    from zagg.sweep_partition import families_partitions, partition_leaves

    if get_pipeline_type(config) == "temporal":
        raise ValueError(
            "temporal runs take events=; no shardmap ceiling (the fan-out unit "
            "is the event, resolved at call time, not a catalog shard)"
        )
    catalog_path = catalog or config.catalog
    if not catalog_path:
        raise ValueError("No catalog specified (pass catalog= or set catalog: in config)")
    catalog_data = runner._load_catalog(catalog_path)
    cells = runner._select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    # The dispatch paths' own expansion, not a copy of it (issue #648): this
    # branched per-window unconditionally, so a ``windowing.unit: shard`` run
    # previewed its window count as invokes while the dispatch logged the
    # true one (~9x on the yearly templates, $1,170 vs $131 at build scale).
    cells = runner._expand_units(cells, config)
    memory_gb = runner._worker_memory_gb(config)
    timeout_s = runner._DEFAULT_FUNCTION_TIMEOUT_S
    typical = {**TYPICAL_S, **(typical_s or {})}

    # The tail's own gates, in the tail's own spelling (runner._run_lambda).
    raster = (config.data_source or {}).get("reader") == "raster"
    sweeps = get_store_layout(config) == "hive" and get_sweep(config)
    grid = from_config(config) if sweeps else None
    staged = sweeps and not raster and chains_stages(config, grid)
    leaves = _leaf_refs(cells) if sweeps else []
    n_stage = (
        stage_invokes(
            leaves,
            get_parent_order(config),
            windowed=get_windowing(config) is not None,
            all_time=bool((get_pyramid(config) or {}).get("all_time")),
        )
        if staged and leaves
        else 0
    )
    rides = bool(runner._cascade_families(config, grid)) if staged else False
    n_families = 0
    if sweeps and leaves and not rides:
        n = families_partitions(leaves)
        n_families = 1 if n == 1 else len(partition_leaves(leaves, n)) + 1
    if get_store_layout(config) != "hive":
        n_tail = 3 if raster else 2 + int(get_consolidate_metadata(config))
    else:
        n_tail = 4 + int(get_coverage_moc(config))
        if not raster:
            n_tail += (2 if get_icechunk(config) else 0) + (1 if n_stage else 0)
    legs = {
        "leaves": _leg(len(cells), memory_gb, timeout_s, typical["leaves"]),
        "stage": _leg(
            n_stage, runner.STAGE_SWEEP_WORKER["memory"] / 1024.0, timeout_s, typical["stage"]
        ),
        "families": _leg(n_families, memory_gb, timeout_s, typical["families"]),
        "tail": _leg(n_tail, memory_gb, timeout_s, typical["tail"]),
    }
    return {
        "n_units": len(cells),
        "memory_gb": memory_gb,
        "arch": LAMBDA_ARCH,
        "timeout_s": timeout_s,
        "max_cost_usd": legs["leaves"]["ceiling_usd"],
        "legs": legs,
        "typical_s": typical,
        "ceiling_usd": sum(leg["ceiling_usd"] for leg in legs.values()),
        "expected_usd": sum(leg["expected_usd"] for leg in legs.values()),
    }


def format_max_cost(preview: dict) -> str:
    """One-line ceiling summary of the LEAF leg (the pre-#660 line, kept verbatim)."""
    return (
        f"Max cost ceiling: ~${preview['max_cost_usd']:.2f} "
        f"({preview['n_units']} units x {preview['memory_gb']:g} GB x "
        f"{preview['timeout_s']:g}s, {preview['arch']})"
    )


def format_cost_legs(preview: dict) -> str:
    """The per-leg table (issue #660): ceiling and expected per leg and in total.

    Falls back to :func:`format_max_cost` for a preview without ``legs``.
    """
    legs = preview.get("legs")
    if not legs:
        return format_max_cost(preview)
    typical = preview.get("typical_s", TYPICAL_S)
    head = f"  {'leg':<16}{'invokes':>8}{'GB':>5}{'ceiling':>11}{'expected':>11}  typical"
    lines = [f"Lambda cost preview ({preview['arch']}, {preview['timeout_s']:g}s wall):", head]
    for key, label in _LEGS:
        leg = legs[key]
        if not leg["invokes"]:
            lines.append(f"  {label:<16}{0:>8}{'-':>5}{'-':>11}{'-':>11}")
            continue
        lines.append(
            f"  {label:<16}{leg['invokes']:>8}{leg['memory_gb']:>5g}"
            f"{'$%.2f' % leg['ceiling_usd']:>11}{'$%.2f' % leg['expected_usd']:>11}"
            f"  {typical[key]:g}s"
        )
    lines.append(
        f"  {'total':<16}{sum(leg['invokes'] for leg in legs.values()):>8}{'':>5}"
        f"{'$%.2f' % preview['ceiling_usd']:>11}{'$%.2f' % preview['expected_usd']:>11}"
    )
    lines.append(
        f"Max cost ceiling: ~${preview['ceiling_usd']:.2f} (every invoke to the wall); "
        f"expected ~${preview['expected_usd']:.2f}"
    )
    return "\n".join(lines)


def confirm_max_cost(preview: dict, *, assume_yes: bool = False, prompt=None) -> bool:
    """CLI gate: print the per-leg table, ask yes/no. ``assume_yes`` (``--yes``) skips.

    Returns ``True`` to proceed. ``prompt`` defaults to ``input`` (resolved at
    call time, injectable for tests). Only the CLI blocks -- the notebook path
    calls :func:`max_cost_preview` for display and never this. The gate
    blocks on the ceiling total (issue #660): the expected column is a
    forecast, the ceiling is the bound.
    """
    print(format_cost_legs(preview))
    if assume_yes:
        return True
    ask = prompt if prompt is not None else input
    answer = ask("Proceed with Lambda fan-out? [y/N] ")
    return answer.strip().lower() in ("y", "yes")


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------


class _LogProgress:
    """Logging fallback when tqdm is not importable: one line per ~10%.

    ``update`` takes the runner's ``on_progress`` triple verbatim, so the
    sink IS the callback (``on_progress=progress.update``).
    """

    def __init__(self, total: int, desc: str):
        self._total = max(int(total), 1)
        self._desc = desc
        self._step = max(self._total // 10, 1)

    def update(self, done: int, total: int, cost_usd: float | None) -> None:
        if total != self._total:  # adopt the runner's authoritative total
            self._total = max(int(total), 1)
            self._step = max(self._total // 10, 1)
        if done % self._step == 0 or done == self._total:
            cost = f", ~${cost_usd:.2f}" if cost_usd is not None else ""
            logger.info(f"{self._desc}: {done}/{self._total}{cost}")

    def close(self) -> None:
        pass


class _TqdmProgress:
    """tqdm-backed bar; running Lambda cost rides the postfix."""

    def __init__(self, total: int, desc: str, tqdm_cls):
        self._bar = tqdm_cls(total=total, desc=desc, unit="unit")

    def update(self, done: int, total: int, cost_usd: float | None) -> None:
        if self._bar.total != total:  # adopt the runner's authoritative total
            self._bar.total = total
        # done is the absolute completion count (dispatch's 1-based index);
        # completion order is nondeterministic so set n rather than += 1.
        self._bar.n = done
        if cost_usd is not None:
            self._bar.set_postfix_str(f"~${cost_usd:.2f}", refresh=False)
        self._bar.refresh()

    def close(self) -> None:
        self._bar.close()


def _make_progress(total: int, desc: str = "shards"):
    """A progress sink: tqdm bar when importable, logging fallback otherwise.

    The sink's ``update(done, total, cost_usd)`` matches the runner's
    ``on_progress`` contract exactly; ``total`` here just pre-sizes the bar
    (the callback's total wins if they differ).
    """
    try:
        from tqdm.auto import tqdm
    except ImportError:
        return _LogProgress(total, desc)
    return _TqdmProgress(total, desc, tqdm)


# ---------------------------------------------------------------------------
# Run wrapper + report view
# ---------------------------------------------------------------------------

#: Row cap for the per-shard table in ``_repr_html_``; full detail stays in
#: ``summary["results"]``.
_HTML_MAX_ROWS = 20


class RunView:
    """A run summary with a rich Jupyter repr; plain-dict access delegates.

    Wraps the exact dict :func:`zagg.runner.agg` returns (``summary``);
    ``view["cells_with_data"]`` etc. keep working, so the wrapper adds
    display without changing the data contract.
    """

    def __init__(self, summary: dict):
        self.summary = summary

    def __getitem__(self, key):
        return self.summary[key]

    def get(self, key, default=None):
        return self.summary.get(key, default)

    def __repr__(self):
        s = self.summary
        done = s.get("cells_with_data", s.get("events_with_data"))
        errs = s.get("cells_error", s.get("events_error"))
        return f"RunView(backend={s.get('backend')!r}, with_data={done}, errors={errs})"

    def _failures(self) -> list[dict]:
        rows = []
        for r in self.summary.get("results") or []:
            error = r.get("error")
            if error and str(error) not in BENIGN_ERRORS:
                rows.append(r)
        return rows

    def _repr_html_(self) -> str:
        s = self.summary
        esc = html.escape
        out = ["<div><h3>zagg run</h3>"]
        out.append(
            f"<p><b>backend:</b> {esc(str(s.get('backend')))} &nbsp; "
            f"<b>store:</b> <code>{esc(str(s.get('store_path')))}</code></p>"
        )

        # Cost block (issue #298): lambda runs carry summary["cost"]; local
        # runs have no metered cost.
        cost = s.get("cost")
        out.append("<table><tr><th>cost</th><th>USD</th></tr>")
        if cost:
            for key, label in (
                ("max_cost_usd", "max (pre-invoke ceiling)"),
                ("estimated_cost_usd", "estimated (prior runs)"),
                ("actual_cost_usd", "actual (billed-duration rollup)"),
            ):
                val = cost.get(key)
                shown = f"${val:.4f}" if val is not None else "n/a"
                out.append(f"<tr><td>{label}</td><td>{shown}</td></tr>")
        else:
            out.append("<tr><td colspan=2>no metered cost (local run)</td></tr>")
        out.append("</table>")

        # Counters: spatial keys with the temporal spellings as fallback.
        rows = [
            ("units", s.get("total_cells", s.get("total_events"))),
            ("with data", s.get("cells_with_data", s.get("events_with_data"))),
            ("errors", s.get("cells_error", s.get("events_error"))),
            ("observations", s.get("total_obs", s.get("timesteps_processed"))),
            ("wall time (s)", s.get("wall_time_s")),
            ("lambda time (s)", s.get("lambda_time_s")),
        ]
        out.append("<table><tr><th>run</th><th></th></tr>")
        for label, val in rows:
            if val is None:
                continue
            shown = f"{val:,.1f}" if isinstance(val, float) else f"{val:,}"
            out.append(f"<tr><td>{label}</td><td>{shown}</td></tr>")
        out.append("</table>")

        # What watched the fan-out and any breaker trip (issue #652); the
        # pacing line says why when no guard ran.
        for line in describe(s.get("breaker"), s.get("dispatch_pacing")):
            out.append(f"<p>{esc(line)}</p>")

        failures = self._failures()
        if failures:
            out.append(f"<h4>failures ({len(failures)})</h4>")
            out.append("<table><tr><th>unit</th><th>class</th><th>error</th></tr>")
            for r in failures[:_HTML_MAX_ROWS]:
                key = r.get("shard_key", r.get("event_key"))
                # The store-error class (issue #651) rides the local meta and
                # the poller's result dict; the v1 sync result carries it in
                # ``body`` only.
                cls = str(
                    r.get("store_error_class")
                    or (r.get("body") or {}).get("store_error_class")
                    or ""
                )
                out.append(
                    f"<tr><td>{esc(str(key))}</td><td>{esc(cls)}</td>"
                    f"<td>{esc(str(r.get('error')))}</td></tr>"
                )
            if len(failures) > _HTML_MAX_ROWS:
                out.append(f"<tr><td colspan=3>... {len(failures) - _HTML_MAX_ROWS} more</td></tr>")
            out.append("</table>")
        out.append("</div>")
        return "".join(out)


def run(config: PipelineConfig, **kwargs) -> RunView:
    """Invoke :func:`zagg.runner.agg` with a progress bar; return a :class:`RunView`.

    Accepts every ``agg`` keyword unchanged -- ``dispatch=`` (the issue #652
    breaker block) included; the returned view renders the pacing line and
    any breaker trip. On ``backend="lambda"`` the
    pre-invoke cost ceiling is *displayed* first (informational only -- this
    path never prompts, per issue #298; the blocking gate is CLI-only). The
    bar total is the previewed unit count for catalog runs and ``len(events)``
    for temporal runs; when neither is resolvable the bar degrades to an
    unsized tqdm (or the logging fallback).
    """
    from zagg import runner

    # Refuse before the preview, not just before the invoke (issue #485).
    # ``agg`` stays the choke point for the dispatch itself; this call is the
    # wrapper's ordering guarantee -- ``max_cost_preview`` below reads the
    # shardmap (a local read or an S3 GET), so without it a bad config would
    # surface as a catalog error first, masking the real refusal. Same shape
    # as the facade's own pre-resolution call in ``client.Run.from_config``;
    # ``validate_config`` is pure, so the second call inside ``agg`` is free.
    validate_config(config)

    total = None
    # Honest default for the path that resolves no unit at all (no events=, no
    # catalog): the bar is sized 0 and the generic noun matches the ``unit=
    # "unit"`` the tqdm bar is built with -- "shards" would assert a unit this
    # path never resolved (issue #648 fold).
    desc = "units"
    if kwargs.get("events") is not None:
        # Materialize once and rebind: a one-shot iterable (generator) would be
        # exhausted by the count, then forwarded empty to agg -- TemporalStrategy's
        # ``list(events)`` would yield [] and the run would silently process zero
        # events. List/tuple inputs are unchanged by the round-trip.
        events = list(kwargs["events"])
        kwargs["events"] = events
        total = len(events)
        desc = "events"
    elif kwargs.get("catalog") or config.catalog:
        preview = max_cost_preview(
            config,
            kwargs.get("catalog"),
            max_cells=kwargs.get("max_cells"),
            morton_cell=kwargs.get("morton_cell"),
        )
        total = preview["n_units"]
        # The bar's unit IS the dispatch unit: ``_with_progress`` ticks once
        # per dispatched unit and ``total`` is the same count the preview
        # priced, so a bulk ``unit: shard`` run reads 0/N..N/N over its N
        # invokes. The label has to follow the unit too (issue #648) — a bar
        # saying "shards" must not tick once per (shard, window).
        desc = runner._unit_noun(config)
        if kwargs.get("backend") == "lambda":
            print(format_cost_legs(preview))

    progress = _make_progress(total if total is not None else 0, desc)
    try:
        summary = runner.agg(config, on_progress=progress.update, **kwargs)
    finally:
        progress.close()
    return RunView(summary)
