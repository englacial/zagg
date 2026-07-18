"""Summary + diagnostics figures for the restructured benchmark page (issue #250).

The renderers of the espg-approved layout (PR #256 rendering comment): each
cadence gets a TOP-LEVEL SUMMARY (total billed cost on a dual lambda-seconds /
USD axis, plus overall wall) and a DIAGNOSTICS figure (one panel per phase or
stage, never stacked). Lives in its own module so ``plot_series.py`` stays
under the repo's ~1000-line module ceiling; ``plot_series.main`` calls in, and
``write_index`` embeds the outputs by filename.

Display derivations (the series stay emission truth):
- point ``agg`` = ``phase_index_s + phase_aggregate_s`` (the approved mapping).
- summed billed total = ``cost_usd + setup_cost_usd + finalize_cost_usd``
  (per-release point; per-merge derives the sync-invoke dollars from
  ``setup_s``/``finalize_s`` at the fixed price). ``cost_usd`` itself keeps its
  worker-GB-seconds semantics.
- the USD axis is an exact relabeling of billed lambda-seconds (fixed 4 GB
  price), via a secondary axis.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from packaging.version import InvalidVersion, Version

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402

from zagg.dispatch import LAMBDA_MEMORY_GB, LAMBDA_PRICE_PER_GB_SEC  # noqa: E402

# The collapsed live per-merge target (issue #250): the single hive config.
LIVE_MERGE_TARGET = "tdigest_healpix_o9_hive"

# Series version floor (issue #272): only rows at/after this release are plotted.
# Pre-0.33.0 rows are wrong -- their cost estimates predate the ~100 s async
# setup term, and the last pre-floor point was a no-run recorded as 0 s / $0 that
# collapses the axis. Applied at RENDER time to every history; the retained
# parquet series is NOT pruned (a physical prune is an @espg action).
_VERSION_FLOOR = Version("0.33.0")


def _at_or_after_floor(hist: pd.DataFrame) -> pd.DataFrame:
    """Drop rows below the :data:`_VERSION_FLOOR` (issue #272).

    Keyed on ``zagg_version`` with a dev-suffix-tolerant parse (``0.33.1.dev5+g``
    parses and sorts after ``0.33.0``). A missing/unparseable version can't be
    proven at/after the floor, so it drops too (legacy rows predate it anyway).
    """
    if hist.empty or "zagg_version" not in hist.columns:
        return hist

    def _ok(v) -> bool:
        try:
            return Version(str(v)) >= _VERSION_FLOOR
        except (InvalidVersion, TypeError):
            return False

    return hist[hist["zagg_version"].map(_ok)]


# Point-pipeline diagnostics panels: (derived column, panel title). ``agg`` is
# the approved index+aggregate mapping; setup/finalize keep the issue #252
# semantics (hive: ping+dispatch / manifest backstop). Never stacked.
POINT_PHASE_PANELS = [
    ("phase_read_s", "read (max shard)"),
    ("phase_agg_s", "agg = index + aggregate (max shard)"),
    ("phase_write_s", "write (max shard)"),
    ("setup_s", "setup (sync path)"),
    ("finalize_s", "finalize (hive: manifest backstop)"),
]

# Raster diagnostics panels: the issue #249 stage set + the write bucket.
# Stage seconds are WORK VOLUME (overlapped async samples), not a wall
# decomposition -- the caveat rides the figure title.
RASTER_STAGE_PANELS = [
    ("stage_open_s", "open (max shard)"),
    ("stage_geometry_s", "geometry (max shard)"),
    ("stage_fetch_s", "fetch (max shard)"),
    ("stage_decode_s", "decode (max shard)"),
    ("stage_gather_s", "gather (max shard)"),
    ("stage_write_s", "write (max shard)"),
]

_USD_PER_LAMBDA_S = LAMBDA_MEMORY_GB * LAMBDA_PRICE_PER_GB_SEC


def _sum_min1(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Row-wise sum over the present columns, NaN only when ALL terms are NaN.

    ``min_count=1`` keeps a legacy row honest-but-present: e.g. a pre-#250 row's
    summed total degrades to its worker ``cost_usd`` alone instead of NaN.
    """
    present = [c for c in cols if c in df.columns]
    if not present:
        return pd.Series(float("nan"), index=df.index)
    return df[present].sum(axis=1, min_count=1)


def point_release_history(fa_df: pd.DataFrame) -> pd.DataFrame:
    """Release rows of the full-AOI point series with display derivations.

    ALL layouts (the collapsed leg is hive; legacy flat rows chart on the same
    axes), ordered by time, with ``phase_agg_s`` and the summed
    ``total_cost_usd`` / ``total_billed_s`` derived. Empty frame in, empty out.
    """
    if fa_df.empty or "target" not in fa_df.columns:
        return pd.DataFrame()
    hist = fa_df.copy()
    if "event" in hist.columns:
        hist = hist[hist["event"] == "release"]
    hist = hist.dropna(subset=["target"])
    hist = _at_or_after_floor(hist)
    if hist.empty:
        return hist
    hist = hist.sort_values("timestamp" if "timestamp" in hist.columns else "commit")
    hist["phase_agg_s"] = _sum_min1(hist, ["phase_index_s", "phase_aggregate_s"])
    hist["total_cost_usd"] = _sum_min1(hist, ["cost_usd", "setup_cost_usd", "finalize_cost_usd"])
    hist["total_billed_s"] = hist["total_cost_usd"] / _USD_PER_LAMBDA_S
    return hist.reset_index(drop=True)


def raster_release_history(r_df: pd.DataFrame) -> pd.DataFrame:
    """Release rows of the raster series, ordered, with the billed totals.

    The raster leg has no sync setup/finalize invokes (the runner emits the
    template, reported as ``template_s``), so its total is ``cost_usd`` directly.
    """
    if r_df.empty or "target" not in r_df.columns:
        return pd.DataFrame()
    hist = r_df.copy()
    if "event" in hist.columns:
        hist = hist[hist["event"] == "release"]
    hist = hist.dropna(subset=["target"])
    hist = _at_or_after_floor(hist)
    if hist.empty:
        return hist
    hist = hist.sort_values("timestamp" if "timestamp" in hist.columns else "commit")
    hist["total_cost_usd"] = hist["cost_usd"] if "cost_usd" in hist.columns else float("nan")
    hist["total_billed_s"] = hist["total_cost_usd"] / _USD_PER_LAMBDA_S
    return hist.reset_index(drop=True)


def merge_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained merge rows of the collapsed live per-merge target, derived.

    The sync-invoke dollars are derived from ``setup_s``/``finalize_s`` at the
    fixed price (the per-merge series carries no ``*_cost_usd`` columns).
    """
    if df.empty or "target" not in df.columns:
        return pd.DataFrame()
    hist = df[(df.get("event") == "merge") & (df["target"] == LIVE_MERGE_TARGET)].copy()
    hist = _at_or_after_floor(hist)
    if hist.empty:
        return hist
    hist = hist.sort_values("timestamp")
    hist["phase_agg_s"] = _sum_min1(hist, ["phase_index_s", "phase_aggregate_s"])
    sync_s = _sum_min1(hist, ["setup_s", "finalize_s"])
    hist["total_cost_usd"] = _sum_min1(
        hist.assign(_sync_usd=sync_s * _USD_PER_LAMBDA_S),
        ["cost_per_shard_usd", "_sync_usd"],
    )
    hist["total_billed_s"] = hist["total_cost_usd"] / _USD_PER_LAMBDA_S
    return hist.reset_index(drop=True)


def _mem_norm_and_fracs(hist: pd.DataFrame):
    """Observed-range memory Normalize + per-row fractions (plot_series's scale)."""
    from matplotlib.colors import Normalize

    cols = hist.columns
    fracs = [
        bench_metrics.memory_pct_of_cap(
            row["max_memory_mb"] if "max_memory_mb" in cols else None,
            row["memory_gb"] if "memory_gb" in cols else None,
        )
        for _, row in hist.iterrows()
    ]
    known = [f for f in fracs if f is not None]
    vmin, vmax = (min(known), max(known)) if known else (0.0, 1.0)
    if vmin == vmax:
        vmin, vmax = vmin - 0.01, vmax + 0.01
    return Normalize(vmin=vmin, vmax=vmax), fracs


def _draw_series(ax, xs, ys, *, fracs, norm, color="C0"):
    """One line + memory-coloured markers (uncoloured grey when unknown)."""
    line = [v if v == v else float("nan") for v in ys]  # NaN-safe passthrough
    ax.plot(xs, line, "-", color=color, zorder=1)
    colors = [f if f is not None else float("nan") for f in fracs]
    ax.scatter(
        xs,
        line,
        s=70,
        c=colors,
        cmap="RdYlGn_r",
        norm=norm,
        edgecolors=color,
        linewidths=0.6,
        zorder=2,
        plotnonfinite=True,
    )


def _finish_axis(ax, xs, labels):
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
    if xs:
        ax.set_xlim(xs[0] - 0.5, xs[-1] + 0.5)


def _add_colorbar(fig, axes, norm, cap_mb):
    """The unchanged memory colour bar: %-cap scale + absolute-MB twin axis."""
    from matplotlib.cm import ScalarMappable

    sm = ScalarMappable(norm=norm, cmap="RdYlGn_r")
    # pad clears room for the MB twin axis below the bar, so its label never
    # collides with the first panel row's titles.
    cbar = fig.colorbar(sm, ax=list(axes), location="top", fraction=0.04, pad=0.24, aspect=40)
    cbar.set_label("peak memory (% of cap)")
    cbar.ax.xaxis.set_major_formatter(lambda v, _pos: f"{v:.0%}")
    mb_ax = cbar.ax.secondary_xaxis(-1.0, functions=(lambda f: f * cap_mb, lambda mb: mb / cap_mb))
    mb_ax.set_xlabel("peak memory (MB)")


def _cap_mb(hist: pd.DataFrame) -> float:
    if "memory_gb" in hist.columns:
        caps = hist["memory_gb"].dropna()
        if not caps.empty:
            return float(caps.iloc[0]) * 1024.0
    return 4.0 * 1024.0


def _summary_ready(hist: pd.DataFrame) -> bool:
    """A hist charts only if it carries non-null billed + wall totals.

    Both panels are indexed unguarded below, so a frame missing either column
    (or with it all-null) is skipped like an empty hist rather than crashing --
    keeping the renderer's "return False, never raise" contract.
    """
    return not hist.empty and all(
        c in hist.columns and hist[c].notna().any() for c in ("total_billed_s", "total_wall_s")
    )


def make_summary_figure(rows: list[tuple[str, pd.DataFrame, str]], out_png: Path) -> bool:
    """The top-level summary: one row per pipeline, columns = cost | wall.

    ``rows`` is ``[(row_label, hist, x_col), ...]`` (empty hists, or hists
    lacking the billed/wall totals, are skipped). Left panels chart
    ``total_billed_s`` with a right-hand USD axis that is an EXACT relabeling
    (fixed 4 GB price); right panels chart ``total_wall_s``. Markers carry the
    unchanged memory colour scale where the series reports memory (both the
    point and raster handlers now report ``max_memory_mb``).
    """
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt

    rows = [(label, hist, x_col) for label, hist, x_col in rows if _summary_ready(hist)]
    if not rows:
        return False

    fig, axes = plt.subplots(
        len(rows), 2, figsize=(14, 3.4 * len(rows)), squeeze=False, gridspec_kw={"wspace": 0.35}
    )
    all_hist = pd.concat([h for _l, h, _x in rows], ignore_index=True)
    norm, _ = _mem_norm_and_fracs(all_hist)
    for r, (label, hist, x_col) in enumerate(rows):
        xs = list(range(len(hist)))
        labels = [str(v)[:7] if x_col == "commit" else str(v) for v in hist[x_col]]
        _, fracs = _mem_norm_and_fracs(hist)
        cost_ax, wall_ax = axes[r][0], axes[r][1]
        _draw_series(cost_ax, xs, hist["total_billed_s"].to_numpy(float), fracs=fracs, norm=norm)
        cost_ax.set_ylabel("billed lambda-seconds", color="C0")
        cost_ax.set_title(f"{label} — total billed cost", fontsize=10)
        # Exact USD relabeling of the same axis (fixed price), so the two
        # scales can never disagree.
        usd = cost_ax.secondary_yaxis(
            1.0,
            functions=(lambda s: s * _USD_PER_LAMBDA_S, lambda d: d / _USD_PER_LAMBDA_S),
        )
        usd.set_ylabel("USD", color="C0")
        _finish_axis(cost_ax, xs, labels)

        _draw_series(
            wall_ax, xs, hist["total_wall_s"].to_numpy(float), fracs=fracs, norm=norm, color="C1"
        )
        wall_ax.set_ylabel("overall wall (s)", color="C1")
        wall_ax.set_title(f"{label} — wall", fontsize=10)
        _finish_axis(wall_ax, xs, labels)
        if r != len(rows) - 1:  # x labels on the bottom row only
            cost_ax.tick_params(labelbottom=False)
            wall_ax.tick_params(labelbottom=False)

    _add_colorbar(fig, axes.ravel().tolist(), norm, _cap_mb(all_hist))
    fig.suptitle("zagg benchmark — total billed cost (lambda-s ⇔ USD) and wall", y=1.02)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


def make_diagnostics_figure(
    hist: pd.DataFrame,
    panels: list[tuple[str, str]],
    x_col: str,
    out_png: Path,
    suptitle: str,
    ylabel: str = "seconds",
) -> bool:
    """One panel PER metric (phase or stage), ``ylabel`` vs x — never stacked.

    Panels whose column is absent or all-null are skipped; no panels -> False
    (the Pages index omits the section instead of embedding a broken image).
    ``ylabel`` defaults to seconds (the phase/stage figures); the object-count
    figure passes ``"objects"`` so its count axis is not mislabelled.
    Markers carry the memory colour scale where available.
    """
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt

    if hist.empty:
        return False
    live = [(c, t) for c, t in panels if c in hist.columns and hist[c].notna().any()]
    if not live:
        return False

    ncols = min(3, len(live))
    nrows = -(-len(live) // ncols)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(5.2 * ncols, 3.0 * nrows), squeeze=False, gridspec_kw={"wspace": 0.3}
    )
    norm, fracs = _mem_norm_and_fracs(hist)
    xs = list(range(len(hist)))
    labels = [str(v)[:7] if x_col == "commit" else str(v) for v in hist[x_col]]
    for i, (col, title) in enumerate(live):
        ax = axes[i // ncols][i % ncols]
        _draw_series(ax, xs, hist[col].to_numpy(float), fracs=fracs, norm=norm)
        ax.set_title(title, fontsize=10)
        ax.set_ylabel(ylabel)
        _finish_axis(ax, xs, labels)
    for j in range(len(live), nrows * ncols):  # blank the unused grid slots
        axes[j // ncols][j % ncols].axis("off")
    # x labels only on each column's bottom-most populated panel.
    for c in range(ncols):
        occupied = [i // ncols for i in range(len(live)) if i % ncols == c]
        for r in occupied:
            if r != max(occupied):
                axes[r][c].tick_params(labelbottom=False)

    _add_colorbar(fig, axes.ravel().tolist(), norm, _cap_mb(hist))
    fig.suptitle(suptitle, y=1.03)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


def make_release_objects_figure(hist: pd.DataFrame, out_png: Path) -> bool:
    """Store object total vs release for the collapsed release leg (issue #240).

    Keeps the sharded-write-bypass tripwire visible after the 2x2 retirement:
    a bypass reads as a ~K-fold step here (record-only on the release leg).
    """
    if hist.empty or "objects_total" not in hist.columns or hist["objects_total"].dropna().empty:
        return False
    return make_diagnostics_figure(
        hist,
        [("objects_total", "store objects (total)"), ("objects_expected", "expected")],
        "ref",
        out_png,
        "zagg full-AOI point — store objects vs release (issue #240 tripwire)",
        ylabel="objects",
    )
