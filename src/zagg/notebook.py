"""Notebook-facing dispatch wrapper: progress bar + rich run report (issue #298).

:func:`run` wraps :func:`zagg.runner.agg` with a per-shard progress bar driven
by the runner's ``on_progress`` callback (one tick per completed work unit,
with the running Lambda cost in the postfix) and returns a :class:`RunView`
whose ``_repr_html_`` renders the cost block, run counters, and failures in
Jupyter. The notebook path **never blocks on cost** (ratified on issue #298):
the pre-invoke ceiling is displayed, not confirmed. The CLI's yes/no gate
(:func:`confirm_max_cost`) lives here too and is wired in ``zagg.__main__``
behind ``--yes``/``-y``.

tqdm is an *optional* import -- it already rides the dependency closure on
orchestrator installs (earthaccess -> pqdm -> tqdm) but is deliberately not a
zagg dependency: a bar when importable, a logging fallback otherwise. Workers
never import this module, so the ``lambda`` extra is untouched.
"""

from __future__ import annotations

import html
import logging

from zagg.config import PipelineConfig, get_pipeline_type, get_store_layout, get_windowing
from zagg.dispatch import LAMBDA_ARCH, max_cost_usd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-invoke ceiling
# ---------------------------------------------------------------------------


def max_cost_preview(
    config: PipelineConfig,
    catalog: str | None = None,
    *,
    max_cells: int | None = None,
    morton_cell: str | None = None,
) -> dict:
    """Resolve the pre-invoke cost ceiling from shardmap + config alone.

    Mirrors the dispatching path's unit accounting -- cell selection
    (``max_cells``/``morton_cell``) and the windowed-unit expansion -- so the
    previewed ceiling equals the one the run logs before fan-out. The unit
    accounting branches on pipeline kind exactly as :func:`~zagg.runner.agg`:
    the spatial path expands with :func:`~zagg.runner._windowed_units` (issue
    #246); the ``reader: raster`` re-route mirrors ``RasterStrategy.run``, whose
    windowed fan-out (:func:`~zagg.runner._raster_windowed_units`, issue #247)
    fires only on the hive layout and groups by acquisition, so its unit count
    differs from the spatial expansion for a windowed-raster config. A temporal
    run has no shardmap ceiling -- its fan-out unit is the ``events=`` item
    resolved at call time, not a catalog shard -- so this raises ``ValueError``.
    Returns ``{n_units, memory_gb, arch, timeout_s, max_cost_usd}``. No AWS
    access and no grid-signature check: this is display math, not dispatch.
    """
    from zagg import runner

    kind = get_pipeline_type(config)
    if kind == "temporal":
        raise ValueError(
            "temporal runs take events=; no shardmap ceiling (the fan-out unit "
            "is the event, resolved at call time, not a catalog shard)"
        )
    if kind == "spatial" and (config.data_source or {}).get("reader") == "raster":
        kind = "raster"

    catalog_path = catalog or config.catalog
    if not catalog_path:
        raise ValueError("No catalog specified (pass catalog= or set catalog: in config)")
    catalog_data = runner._load_catalog(catalog_path)
    cells = runner._select_cells(catalog_data, morton_cell=morton_cell, max_cells=max_cells)
    windowing = get_windowing(config)
    if kind == "raster":
        # Mirror RasterStrategy.run: windowed fan-out only on the hive layout;
        # otherwise one unit per selected cell (cells == _select_cells).
        if get_store_layout(config) == "hive" and windowing is not None:
            cells = runner._raster_windowed_units(cells, windowing)
    elif windowing is not None:
        cells = runner._windowed_units(cells, windowing, (config.bounds or {}).get("temporal"))
    memory_gb = runner._worker_memory_gb(config)
    timeout_s = runner._DEFAULT_FUNCTION_TIMEOUT_S
    return {
        "n_units": len(cells),
        "memory_gb": memory_gb,
        "arch": LAMBDA_ARCH,
        "timeout_s": timeout_s,
        "max_cost_usd": max_cost_usd(len(cells), memory_gb, timeout_s=timeout_s),
    }


def format_max_cost(preview: dict) -> str:
    """One-line ceiling summary shared by the CLI gate and the notebook display."""
    return (
        f"Max cost ceiling: ~${preview['max_cost_usd']:.2f} "
        f"({preview['n_units']} units x {preview['memory_gb']:g} GB x "
        f"{preview['timeout_s']:g}s, {preview['arch']})"
    )


def confirm_max_cost(preview: dict, *, assume_yes: bool = False, prompt=None) -> bool:
    """CLI gate: print the ceiling, ask yes/no. ``assume_yes`` (``--yes``) skips.

    Returns ``True`` to proceed. ``prompt`` defaults to ``input`` (resolved at
    call time, injectable for tests). Only the CLI blocks -- the notebook path
    calls :func:`max_cost_preview` for display and never this.
    """
    print(format_max_cost(preview))
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

#: Errors that mean "nothing to do", not "something broke" -- excluded from the
#: failures table, matching ``_run_lambda``'s error counting.
_BENIGN_ERRORS = ("No granules found", "No data after filtering")

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
            if error and str(error) not in _BENIGN_ERRORS:
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

        failures = self._failures()
        if failures:
            out.append(f"<h4>failures ({len(failures)})</h4>")
            out.append("<table><tr><th>unit</th><th>error</th></tr>")
            for r in failures[:_HTML_MAX_ROWS]:
                key = r.get("shard_key", r.get("event_key"))
                out.append(f"<tr><td>{esc(str(key))}</td><td>{esc(str(r.get('error')))}</td></tr>")
            if len(failures) > _HTML_MAX_ROWS:
                out.append(f"<tr><td colspan=2>... {len(failures) - _HTML_MAX_ROWS} more</td></tr>")
            out.append("</table>")
        out.append("</div>")
        return "".join(out)


def run(config: PipelineConfig, **kwargs) -> RunView:
    """Invoke :func:`zagg.runner.agg` with a progress bar; return a :class:`RunView`.

    Accepts every ``agg`` keyword unchanged. On ``backend="lambda"`` the
    pre-invoke cost ceiling is *displayed* first (informational only -- this
    path never prompts, per issue #298; the blocking gate is CLI-only). The
    bar total is the previewed unit count for catalog runs and ``len(events)``
    for temporal runs; when neither is resolvable the bar degrades to an
    unsized tqdm (or the logging fallback).
    """
    from zagg import runner

    total = None
    if kwargs.get("events") is not None:
        # Materialize once and rebind: a one-shot iterable (generator) would be
        # exhausted by the count, then forwarded empty to agg -- TemporalStrategy's
        # ``list(events)`` would yield [] and the run would silently process zero
        # events. List/tuple inputs are unchanged by the round-trip.
        events = list(kwargs["events"])
        kwargs["events"] = events
        total = len(events)
    elif kwargs.get("catalog") or config.catalog:
        preview = max_cost_preview(
            config,
            kwargs.get("catalog"),
            max_cells=kwargs.get("max_cells"),
            morton_cell=kwargs.get("morton_cell"),
        )
        total = preview["n_units"]
        if kwargs.get("backend") == "lambda":
            print(format_max_cost(preview))

    progress = _make_progress(total if total is not None else 0)
    try:
        summary = runner.agg(config, on_progress=progress.update, **kwargs)
    finally:
        progress.close()
    return RunView(summary)
