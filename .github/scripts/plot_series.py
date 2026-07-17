"""Render the GitHub Pages charts from the retained benchmark series (issue #110).

Two figures, x-axis = labelled merge points (the locked design): (1) cost per
shard + Lambda runtime; (2) cost per 100 km^2 + runtime. Each figure is a grid of
per-target panels with cost on the left axis (solid) and runtime on the right
(dashed), so a regression in either shows up against merge history.

Also writes a latest-merge snapshot of the most recent retained run:
``latest_table.png`` (embedded live in the docs by raw URL, like the charts) plus
its human/agent-readable companions ``latest.md`` and ``metrics.json`` -- the path
an agent should follow to reference current benchmark numbers. All artifacts land
in the output dir alongside a small ``index.html`` for Pages.

matplotlib lives in the ``benchmark`` (and ``analysis``) extra, not core, so this
is imported lazily and the plot test ``importorskip``s it -- the default test
suite never needs a plotting backend.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Allow ``import bench_metrics`` whether run as a script or imported by tests.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_metrics  # noqa: E402

# Cost metric -> (column, human label) for the two retained-series figures.
FIGURES = {
    "cost_per_shard": ("cost_per_shard_usd", "cost / shard (USD)"),
    "cost_per_100km2": ("cost_per_100km2_usd", "cost / 100 km² (USD)"),
}

# green->red colormap for memory headroom (issue #120): 0% of the cap reads
# green, the OOM wall reads red. ``_r`` flips the stock green->red ramp.
MEMORY_CMAP = "RdYlGn_r"


def memory_fractions(sub: pd.DataFrame) -> list[float | None]:
    """Per-row memory headroom as a fraction of the Lambda cap (issue #120).

    ``max_memory_mb / (memory_gb * 1024)`` via the shared ``bench_metrics``
    helper, so a row missing either column degrades to ``None`` (plotted as an
    uncoloured marker) instead of crashing the render.
    """
    cols = sub.columns
    return [
        bench_metrics.memory_pct_of_cap(
            row["max_memory_mb"] if "max_memory_mb" in cols else None,
            row["memory_gb"] if "memory_gb" in cols else None,
        )
        for _, row in sub.iterrows()
    ]


def _memory_cap_mb(sub: pd.DataFrame) -> float:
    """Lambda memory cap in MB, for the colorbar's fraction<->MB twin axis.

    Reads ``memory_gb`` (uniform across the series -- the benchmark pins 4 GB) and
    falls back to 2048 MB when the column is absent or empty.
    """
    if "memory_gb" in sub.columns:
        caps = sub["memory_gb"].dropna()
        if not caps.empty:
            return float(caps.iloc[0]) * 1024.0
    return 4.0 * 1024.0  # issue #193: benchmark pins 4 GB


def _merge_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained points only (merge runs), ordered by time."""
    hist = df[df["event"] == "merge"].copy()
    if hist.empty:
        return hist
    return hist.sort_values("timestamp").reset_index(drop=True)


def _codec_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of the forward sharded-vs-inner rows (issue #133): those that
    carry a non-null ``codec``. Absent column (a pre-#133 parquet) -> all False, so
    everything reads as frozen until the new matrix lands its first merge."""
    if "codec" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["codec"].notna()


def _codec_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained merge points of the forward matrix only (``codec.notna``)."""
    return _merge_history(df[_codec_mask(df)])


def _frozen_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained merge points of the frozen historical series only (``codec.isna``);
    the old rect/gain_bias matrix, unchanged by issue #133."""
    return _merge_history(df[~_codec_mask(df)])


def _aoi_axis(target: str) -> str:
    """The AOI-mask arm of a live target: ``"mask"`` or ``"nomask"`` (issue #202).

    The reset live matrix has no dedicated record column for the AOI-mask A/B
    (``bench_metrics``/``run_benchmark`` are stable plumbing, not touched by the
    reset), so the axis is read off the target-name suffix
    (``..._mask`` / ``..._nomask``). ``_nomask`` does not end with ``_mask``, so
    the plain ``endswith`` split is unambiguous."""
    return "mask" if str(target).endswith("_mask") else "nomask"


def _matrix_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of the live *reset* matrix rows (issue #202): o9 rows carrying
    a non-null ``index_backend`` (inline|sidecar) whose target names the AOI-mask
    arm (``..._mask`` / ``..._nomask``).

    The name/order gate is what resets the series to zero at the render layer: the
    pre-reset live points (the issue #193 o9/o10 inline-vs-sidecar rows, whose
    targets carry no ``_mask``/``_nomask`` suffix, and any o10 rows) fall outside
    this scheme and drop out, so the corrected 2x2 begins fresh at the first
    post-reset merge without needing to prune the benchmarks-branch series.
    Absent ``index_backend`` column (a pre-#193 parquet) -> all False.
    Flat rows only (issue #240 phase 4): the hive regression arm is excluded
    both by its suffix-free name and, defensively, by the ``store_layout``
    column, so it can never claim a 2x2 panel cell (its rows stay in the
    series). Legacy rows (null ``store_layout``) read as flat."""
    if "index_backend" not in df.columns:
        return pd.Series(False, index=df.index)
    has_backend = df["index_backend"].notna()
    grid_size = df["grid_size"].astype(str) if "grid_size" in df.columns else ""
    is_o9 = grid_size == "o9"
    if "target" in df.columns:
        aoi_suffix = df["target"].astype(str).str.endswith(("_mask", "_nomask"))
    else:
        aoi_suffix = pd.Series(False, index=df.index)
    if "store_layout" in df.columns:
        is_flat = df["store_layout"].fillna("flat") == "flat"
    else:
        is_flat = pd.Series(True, index=df.index)
    return has_backend & is_o9 & aoi_suffix.fillna(False) & is_flat


def _matrix_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained merge points of the live reset matrix (issue #202): inline/sidecar
    x AOI-mask on/off at o9."""
    return _merge_history(df[_matrix_mask(df)])


def _latest_of(hist: pd.DataFrame) -> pd.DataFrame:
    """Rows of the single most-recent run in an already-filtered/ordered history.

    The latest is the commit owning the newest timestamp; all of that commit's
    per-target rows come back together (including failed/zero rows, so the
    published snapshot shows the whole matrix, failures included).
    """
    if hist.empty:
        return hist
    latest_commit = hist.iloc[-1]["commit"]
    return hist[hist["commit"] == latest_commit].sort_values("target").reset_index(drop=True)


def _latest_merge(df: pd.DataFrame) -> pd.DataFrame:
    """Rows of the single most-recent retained (merge) run, ordered by target."""
    return _latest_of(_merge_history(df))


def latest_records(df: pd.DataFrame) -> list[dict]:
    """Latest-merge rows as plain records. Failed/legacy cells come back as the
    float NaN; the shared ``bench_metrics`` formatter renders those as ``n/a``
    (and ``write_latest_metrics`` serialises them to JSON ``null``)."""
    latest = _latest_merge(df)
    if latest.empty:
        return []
    return latest.to_dict(orient="records")


def write_latest_markdown(df: pd.DataFrame, out_md: Path) -> bool:
    """Write the latest-merge table as ``latest.md`` (issue #110). False if no
    retained run yet."""
    recs = latest_records(df)
    if not recs:
        return False
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(bench_metrics.latest_markdown(recs))
    return True


def write_latest_metrics(df: pd.DataFrame, out_json: Path) -> bool:
    """Write the latest-merge rows as machine-readable ``metrics.json`` (the path
    agents/scripts should follow for current numbers). ``to_json`` handles the
    numpy dtypes and NaN -> null. False if no retained run yet."""
    latest = _latest_merge(df)
    if latest.empty:
        return False
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(latest.to_json(orient="records", indent=2))
    return True


def _render_table(recs: list[dict], title: str, out_png: Path) -> bool:
    """Render a benchmark latest-merge table (one row per record) as a PNG.

    Shared by the frozen latest-table and the forward codec latest-table (issue
    #133) so both shade the ``% cap`` cell on the same green->red memory scale and
    use the shared ``bench_metrics`` headers/formatter. False on no records."""
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    if not recs:
        return False

    cmap = matplotlib.colormaps[MEMORY_CMAP]
    norm = Normalize(vmin=0.0, vmax=1.0)  # fixed: 1.0 (red) is the OOM wall
    cap_col = bench_metrics.TABLE_HEADERS.index("% cap")

    cell_text, cell_colours = [], []
    for r in recs:
        cells = bench_metrics.format_record_cells(r)
        cell_text.append([cells[h] for h in bench_metrics.TABLE_HEADERS])
        row_colours = ["white"] * len(bench_metrics.TABLE_HEADERS)
        if cells["mem_frac"] is not None:
            row_colours[cap_col] = cmap(norm(cells["mem_frac"]))
        cell_colours.append(row_colours)

    nrows = len(recs)
    fig, ax = plt.subplots(figsize=(11, 0.5 * nrows + 1.4))
    ax.axis("off")
    # Give the target column the room its long names need; split the rest evenly.
    ncol = len(bench_metrics.TABLE_HEADERS)
    col_widths = [0.26] + [(1.0 - 0.26) / (ncol - 1)] * (ncol - 1)
    table = ax.table(
        cellText=cell_text,
        colLabels=bench_metrics.TABLE_HEADERS,
        cellColours=cell_colours,
        colWidths=col_widths,
        loc="center",
        cellLoc="right",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
        if col == 0:  # left-align the target column
            cell.set_text_props(ha="left")

    ax.set_title(title, fontsize=10)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


def _table_title(prefix: str, recs: list[dict]) -> str:
    head = recs[0] if recs else {}
    commit = str(head.get("commit") or "")[:7]
    ts = str(head.get("timestamp") or "")
    gb = head.get("memory_gb")
    return (
        f"{prefix} — latest merge {commit} ({ts})\n"
        f"arm64 · {gb if gb is not None else 'n/a'} GB · one densest shard/target · "
        "% cap shaded green→red"
    )


def make_latest_table(df: pd.DataFrame, out_png: Path) -> bool:
    """Render the FROZEN latest-merge table as a PNG (the historical snapshot,
    issue #133's freeze). Embedded live in the docs by raw URL. False if no
    retained frozen run yet."""
    recs = _latest_of(_frozen_history(df)).to_dict(orient="records")
    return _render_table(recs, _table_title("zagg Lambda benchmark", recs), out_png)


def make_codec_latest_table(df: pd.DataFrame, out_png: Path) -> bool:
    """Render the forward sharded-vs-inner latest-merge table (issue #133): the
    ``codec.notna`` rows of the most recent merge. Rendered above the frozen table.
    False until the new matrix lands its first merge."""
    recs = _latest_of(_codec_history(df)).to_dict(orient="records")
    return _render_table(recs, _table_title("zagg sharded vs inner-chunk", recs), out_png)


def _panel_layout(hist: pd.DataFrame) -> tuple[list[list[str | None]], int, int]:
    """Place each FROZEN target in a ``(row, col)`` grid, data-driven (issue #121
    review). The forward sharded-vs-inner matrix uses the fixed ``_codec_layout``
    instead; this lays out the retained rect/gain_bias rows (issue #133 freeze).

    Convention: the LEFT column is the rectilinear grids, the RIGHT column is the
    HEALPix grids. A row pairs the two families at the same aggregator and the same
    *shard-size rank* within their family (so e.g. ``rect_6km`` lines up with the
    largest HEALPix shard). Rows are ordered largest-shard-first, top to bottom;
    same-size rows break ties on the aggregator name. The frozen targets slot in by
    the same rule -- nothing is hardcoded to a fixed target list.
    """
    meta = hist.dropna(subset=["target"]).drop_duplicates("target")
    rows = []
    for _, r in meta.iterrows():
        grid = str(r.get("grid_type", ""))
        col = 0 if grid.startswith("rect") else 1
        rows.append(
            {
                "target": r["target"],
                "col": col,
                "grid": grid,
                "size": str(r.get("grid_size", "")),
                "agg": str(r.get("aggregator", "")),
                "area": float(r.get("shard_area_km2") or 0.0),
            }
        )
    # Shard-size rank within each grid family (0 = largest), so the two families
    # align row-for-row even though their absolute areas differ. Rank on
    # ``(area, grid_size)`` so two resolutions that happen to share an exact area
    # still get distinct rows instead of colliding into one cell.
    for col in (0, 1):
        keys = sorted(
            {(d["area"], d["size"]) for d in rows if d["col"] == col},
            key=lambda k: (-k[0], k[1]),
        )
        rank = {k: i for i, k in enumerate(keys)}
        for d in rows:
            if d["col"] == col:
                d["rank"] = rank[(d["area"], d["size"])]
    # One row per (size-rank, aggregator); largest shard at the top.
    row_keys = sorted({(d["rank"], d["agg"]) for d in rows})
    row_of = {k: i for i, k in enumerate(row_keys)}
    nrows = len(row_keys)
    ncols = 1 + int(any(d["col"] == 1 for d in rows)) if rows else 1
    grid: list[list[str | None]] = [[None] * ncols for _ in range(nrows)]
    for d in rows:
        grid[row_of[(d["rank"], d["agg"])]][d["col"]] = d["target"]
    return grid, nrows, ncols


# Fixed 2x3 layout for the forward sharded-vs-inner matrix (issue #133): the two
# columns are the ShardingCodec A/B (sharded | inner), the three rows are orders
# o9 -> o11 top-to-bottom (largest shard on top, matching the frozen figure's
# largest-first convention). Unlike ``_panel_layout`` this is keyed to the codec
# axis, not the rect/healpix split the frozen rows use.
# "cached" is the issue #170 read-axis column: those targets carry codec
# "inner" (the codec key still drives output.grid.sharded) plus read="cached",
# and must not collide with the real inner panel in the slot map below.
CODEC_COLS = ["sharded", "inner", "cached"]
CODEC_ROWS = ["o9", "o10", "o11"]


def _codec_layout(hist: pd.DataFrame) -> tuple[list[list[str | None]], int, int]:
    """Place each codec target in the fixed 2x3 ``(order, codec)`` grid.

    Columns = ``CODEC_COLS`` (sharded, inner); rows = ``CODEC_ROWS`` (o9->o11).
    A cell holds the target whose ``grid_size``/``codec`` match, else ``None``
    (a not-yet-landed order, e.g. o9 before its shard map lands, plots blank).
    """
    by_slot: dict[tuple[str, str], str] = {}
    meta = hist.dropna(subset=["target"]).drop_duplicates("target")
    for _, r in meta.iterrows():
        # Read-aware slot label (issue #170): cached targets get their own
        # column; everything else keys on the codec exactly as before.
        label = "cached" if str(r.get("read", "")) == "cached" else str(r.get("codec", ""))
        by_slot[(str(r.get("grid_size", "")), label)] = r["target"]
    grid: list[list[str | None]] = [
        [by_slot.get((order, codec)) for codec in CODEC_COLS] for order in CODEC_ROWS
    ]
    return grid, len(CODEC_ROWS), len(CODEC_COLS)


# Live matrix (issue #202 reset): read-backend A/B (columns) x AOI-mask A/B (rows),
# o9 ONLY. Retired the o10 order axis (frozen) and the sharded/inner codec axis
# (frozen, issue #193). Columns are inline|sidecar; rows are nomask|mask (AOI-mask
# off on top, on below).
MATRIX_COLS = ["inline", "sidecar"]
MATRIX_ROWS = ["nomask", "mask"]


def _matrix_layout(hist: pd.DataFrame) -> tuple[list[list[str | None]], int, int]:
    """Place each live target in the fixed ``(aoi_mask, index_backend)`` grid
    (issue #202 reset). Columns = ``MATRIX_COLS`` (inline, sidecar); rows =
    ``MATRIX_ROWS`` (nomask, mask). Empty cell -> ``None`` (arm not landed yet).

    The AOI-mask row is derived from the target-name suffix via :func:`_aoi_axis`
    (there is no record column for it); all rows are o9 (the reset is o9-only)."""
    by_slot: dict[tuple[str, str], str] = {}
    meta = hist.dropna(subset=["target"]).drop_duplicates("target")
    for _, r in meta.iterrows():
        by_slot[(_aoi_axis(r["target"]), str(r.get("index_backend", "")))] = r["target"]
    grid: list[list[str | None]] = [
        [by_slot.get((aoi, backend)) for backend in MATRIX_COLS] for aoi in MATRIX_ROWS
    ]
    return grid, len(MATRIX_ROWS), len(MATRIX_COLS)


def _draw_panel(
    ax,
    sub: pd.DataFrame,
    cost_col: str,
    cost_label: str,
    norm,
    *,
    runtime_col: str = "runtime_s",
    runtime_label: str = "runtime (s)",
    label_col: str = "commit",
    label_len: int | None = 7,
) -> None:
    """Draw one cost+runtime panel for a single target's history.

    Shared by the frozen figure (``make_figure``), the codec figure
    (``make_codec_figure``), the live matrix, and the per-release full-AOI figure
    so they render points identically -- memory-coloured cost markers (left axis),
    hollow-circle runtime (right axis), failed runs (zero) broken out of the line
    and flagged with an 'x' near the floor. The per-release full-AOI figure varies
    the right-axis series (``total_wall_s``) and the x labels (release ``ref``, not
    a commit sha) via ``runtime_col`` / ``label_col`` / ``label_len``; the defaults
    keep the per-merge callers byte-identical.
    """
    xs = list(range(len(sub)))

    # Drop failed runs: a zero cost/runtime is a crashed shard, not a
    # measurement. ``line`` breaks the connecting segment at those x
    # (NaN), so the line never dips to 0; ``fail`` marks them with a
    # non-connected 'x' (drawn in axes-fraction y near the bottom, so it
    # doesn't drag the cost axis floor back down to 0) so the x-axis/commit
    # alignment is kept and the failure reads as a failure, not a real 0.
    cost = sub[cost_col].to_numpy(dtype=float)
    line = [v if v != 0 else float("nan") for v in cost]
    fail = [x for x, v in zip(xs, cost) if v == 0]

    # Connecting line stays cost-blue; the markers carry the memory signal
    # (colour = % of the Lambda memory cap, green->red). Rows missing
    # memory plot uncoloured (grey) rather than dropping out.
    fracs = memory_fractions(sub)
    colors = [f if (f is not None and v != 0) else float("nan") for f, v in zip(fracs, cost)]
    ax.plot(xs, line, "-", color="C0", zorder=1, label=cost_label)
    ax.scatter(
        xs,
        line,
        s=90,
        c=colors,
        cmap=MEMORY_CMAP,
        norm=norm,
        edgecolors="C0",
        linewidths=0.6,
        zorder=2,
        plotnonfinite=True,
    )
    if fail:  # failed-run markers, pinned near the axis floor, not joined
        ax.scatter(
            fail,
            [0.02] * len(fail),
            transform=ax.get_xaxis_transform(),
            marker="x",
            color="0.5",
            s=60,
            zorder=3,
        )
    ax.set_ylabel(cost_label, color="C0")
    ax.tick_params(axis="y", labelcolor="C0")
    ax.set_xticks(xs)
    if xs:  # pin the x-range to every merge so a failed last/first run
        ax.set_xlim(xs[0] - 0.5, xs[-1] + 0.5)  # (drawn in axes-y) stays visible

    # Label every panel with its own x identity (commit sha for the merge
    # figures, release tag for the full-AOI figure); the upper rows have the
    # labels hidden afterwards, so only the bottom row shows them.
    labels = [(str(cm)[:label_len] if label_len else str(cm)) for cm in sub[label_col]]
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)

    # Runtime on the right axis: hollow circles (not filled squares) so
    # the memory-coloured cost marker stays visible (issue #125). The twin
    # axis is drawn after ``ax``, so raise ``ax`` above it and drop
    # ``ax``'s opaque patch -- otherwise the runtime glyph sits on top of
    # the cost circles. Zero runtimes break the line the same way.
    runtime = sub[runtime_col].to_numpy(dtype=float)
    rt_line = [v if v != 0 else float("nan") for v in runtime]
    rt = ax.twinx()
    rt.plot(
        xs,
        rt_line,
        linestyle="--",
        marker="o",
        markerfacecolor="none",
        color="C1",
        label=runtime_label,
    )
    rt.set_ylabel(runtime_label, color="C1")
    rt.tick_params(axis="y", labelcolor="C1")
    ax.set_zorder(rt.get_zorder() + 1)
    ax.patch.set_visible(False)


def _render_panel_grid(
    hist: pd.DataFrame,
    layout: list[list[str | None]],
    nrows: int,
    ncols: int,
    cost_col: str,
    cost_label: str,
    suptitle: str,
    out_png: Path,
    *,
    runtime_col: str = "runtime_s",
    runtime_label: str = "runtime (s)",
    label_col: str = "commit",
    label_len: int | None = 7,
) -> bool:
    """Render a per-target grid of cost+runtime panels from ``layout`` + write it.

    The shared body of ``make_figure`` (frozen series), ``make_codec_figure`` (the
    2x3 sharded-vs-inner matrix), the live matrix, and the per-release full-AOI
    figure: same colour scale, colorbar (with the MB twin axis), per-panel draw,
    and bottom-row-only x labels. ``layout`` (and its ``nrows``/``ncols``) plus the
    right-axis / x-label overrides (forwarded to :func:`_draw_panel`) are the only
    things the callers vary.
    """
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize

    # Each panel keeps its OWN x-axis: targets can have different commit sets (one
    # may have skipped an early merge), so a single shared axis would stamp one
    # target's labels onto all panels. We instead hide the upper rows' commit
    # labels by hand below, keeping the "only the bottom row is labelled" look
    # without the misalignment. ``wspace`` opens a gap between the 2-wide columns
    # so a panel's right-axis (runtime) labels don't collide with the next
    # column's left-axis (cost) labels.
    fig, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(7 * ncols, 3.2 * nrows),
        squeeze=False,
        gridspec_kw={"wspace": 0.45},
    )

    # Clip the colour scale to the observed memory range (don't anchor at 0) so
    # the green->red gradient spans only real data; fall back to [0, 1] when no
    # row carries memory. ``cap_mb`` drives the colorbar's MB twin axis below.
    fracs_all = [f for f in memory_fractions(hist) if f is not None]
    vmin, vmax = (min(fracs_all), max(fracs_all)) if fracs_all else (0.0, 1.0)
    if vmin == vmax:  # single observed value -> give the bar a hair of width
        vmin, vmax = vmin - 0.01, vmax + 0.01
    norm = Normalize(vmin=vmin, vmax=vmax)
    cap_mb = _memory_cap_mb(hist)

    for r in range(nrows):
        for c in range(ncols):
            target = layout[r][c]
            ax = axes[r][c]
            if target is None:  # no target at this slot -> blank panel
                ax.axis("off")
                continue
            sub = hist[hist["target"] == target]
            _draw_panel(
                ax,
                sub,
                cost_col,
                cost_label,
                norm,
                runtime_col=runtime_col,
                runtime_label=runtime_label,
                label_col=label_col,
                label_len=label_len,
            )
            ax.set_title(target, fontsize=10)

    # Keep commit labels only on the bottom-most populated panel of each column;
    # hide the rows above so the labels aren't repeated up the grid.
    for c in range(ncols):
        last = max((r for r in range(nrows) if layout[r][c] is not None), default=None)
        for r in range(nrows):
            if layout[r][c] is not None and r != last:
                axes[r][c].tick_params(labelbottom=False)

    fig.suptitle(suptitle)
    # One shared colorbar for the memory scale (issue #120) -- horizontal and
    # tucked just below the title so it doesn't crowd the right-axis labels.
    sm = ScalarMappable(norm=norm, cmap=MEMORY_CMAP)
    cbar = fig.colorbar(
        sm,
        ax=axes.ravel().tolist(),
        location="top",
        fraction=0.04,
        pad=0.12,
        aspect=40,
    )
    cbar.set_label("peak memory (% of cap)")
    cbar.ax.xaxis.set_major_formatter(lambda v, _pos: f"{v:.0%}")
    # Twin axis (below the bar) reading the same scale in absolute MB: the bar's
    # data coords are fraction-of-cap, so MB = fraction * cap_mb (and back).
    mb_ax = cbar.ax.secondary_xaxis(-1.0, functions=(lambda f: f * cap_mb, lambda mb: mb / cap_mb))
    mb_ax.set_xlabel("peak memory (MB)")

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


def make_figure(df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path) -> bool:
    """Render the FROZEN figure: the historical rect/gain_bias rows (issue #133's
    freeze), laid out by the rect-left / HEALPix-right convention. Returns False
    (writing nothing) when there's no retained frozen data to plot."""
    hist = _frozen_history(df)
    if hist.empty:
        return False

    if hist["target"].dropna().empty:  # rows present but no labels -> nothing to panel
        return False
    # Meaningful layout: rect grids on the left, HEALPix on the right; rows pair
    # the families at matching aggregator + shard-size rank, largest shard on top.
    layout, nrows, ncols = _panel_layout(hist)
    return _render_panel_grid(
        hist,
        layout,
        nrows,
        ncols,
        cost_col,
        cost_label,
        f"zagg Lambda benchmark — {cost_label} vs merge history",
        out_png,
    )


def make_matrix_latest_table(df: pd.DataFrame, out_png: Path) -> bool:
    """Render the live reset-matrix latest-merge table (issue #202): the reset
    rows (o9, inline/sidecar x mask/nomask) of the most recent merge. False until
    the reset matrix lands its first merge."""
    recs = _latest_of(_matrix_history(df)).to_dict(orient="records")
    return _render_table(
        recs, _table_title("zagg inline/sidecar x AOI-mask (sharded, K=4, o9)", recs), out_png
    )


def make_matrix_figure(df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path) -> bool:
    """Render the live reset-matrix figure (issue #202): the reset rows in the
    fixed ``MATRIX_ROWS x MATRIX_COLS`` grid (rows = AOI-mask nomask/mask,
    columns = inline/sidecar, o9 only). Returns False when no reset rows are
    retained yet."""
    hist = _matrix_history(df)
    if hist.empty or hist["target"].dropna().empty:
        return False
    layout, nrows, ncols = _matrix_layout(hist)
    return _render_panel_grid(
        hist,
        layout,
        nrows,
        ncols,
        cost_col,
        cost_label,
        f"zagg inline/sidecar x AOI-mask (sharded, K=4, o9) \u2014 {cost_label} vs merge history",
        out_png,
    )


def make_codec_figure(df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path) -> bool:
    """Render the forward sharded-vs-inner figure (issue #133): the ``codec.notna``
    rows in a fixed 2x3 grid (cols = sharded/inner, rows = o9->o11). Returns False
    when no codec rows are retained yet (so the first merge of the new matrix is
    what brings it to life)."""
    hist = _codec_history(df)
    if hist.empty or hist["target"].dropna().empty:
        return False
    layout, nrows, ncols = _codec_layout(hist)
    return _render_panel_grid(
        hist,
        layout,
        nrows,
        ncols,
        cost_col,
        cost_label,
        f"zagg sharded vs inner-chunk — {cost_label} vs merge history",
        out_png,
    )


# --- per-release full-AOI NEON figure (issue #202, leg 1) -------------------
#
# The live 2x2 above is the PER-MERGE-TO-MAIN series (one densest shard/target,
# isolating code deltas from data drift). The full-AOI NEON run is the
# complementary PER-RELEASE series: the whole AOP_NEON box fanned out over every
# shard, recorded per release for cost-truth (all shards, real dollar total), not
# on every merge. The two are deliberately split (see docs/deployment/benchmark.md).
#
# This figure is rendered from a SEPARATE parquet (full_aoi_series.parquet, written
# by full_aoi_series.py) rather than the per-merge series, because the whole-AOI run
# record schema differs (n_shards, whole-AOI cost_usd, total_wall_s). Same 2x2 layout
# as the live matrix (inline/sidecar x AOI-mask), but each panel is the whole-AOI
# total (not a single shard) across RELEASES -- release tag on the x-axis.

# Full-AOI figure name -> (series column, human label). Mirrors FIGURES (per-merge)
# but at release cadence: the whole-AOI dollar total, and the AOI-AVERAGE cost per
# 100 km^2 (total cost / total AOI area) -- the honest "average shard" figure the
# single densest-shard matrix can't show (its cost/100 km^2 is the worst shard).
FULL_AOI_FIGURES = {
    "full_aoi_cost_total": ("cost_usd", "whole-AOI cost (USD)"),
    "full_aoi_cost_per_100km2": ("cost_per_100km2_usd", "AOI-avg cost / 100 km² (USD)"),
    # Store object total (issue #240, record-only on this leg): a sharded-write
    # bypass multiplies the store's object count ~K-fold (issue #215), so the
    # regression reads as a step in this panel even though the release run is
    # never failed on it. Null on releases recorded before the metric existed.
    "full_aoi_objects": ("objects_total", "store objects (total)"),
}


def _full_aoi_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained per-release rows of the full-AOI series, oldest release first.

    The series is release-only by construction (full_aoi_series retains
    ``event == "release"``), but we re-filter defensively and derive the
    AOI-average ``cost_per_100km2_usd`` = ``cost_usd`` * 100 / (n_shards *
    shard_area_km2) -- the whole-AOI cost spread over the whole AOI area, the
    average-shard figure. Rows without the area inputs get a NaN there (the panel
    then simply skips that metric).

    Flat rows only (issue #240 phase 4): the hive regression arm shares the
    ``index_backend`` axis with a flat target, so slotting it into the fixed
    inline/sidecar x AOI-mask grid would silently overwrite that panel cell.
    Hive rows stay in the parquet (object counts + ``parity_ok``); charting
    them is a follow-up once the layout axis gets its own panel row. Legacy
    rows (null ``store_layout``) read as flat."""
    if df.empty or "target" not in df.columns:
        return df
    hist = df.copy()
    if "event" in hist.columns:
        hist = hist[hist["event"] == "release"]
    if "store_layout" in hist.columns:
        hist = hist[hist["store_layout"].fillna("flat") == "flat"]
    hist = hist.dropna(subset=["target"])
    if hist.empty:
        return hist
    area = hist["n_shards"] * hist["shard_area_km2"]
    hist["cost_per_100km2_usd"] = (hist["cost_usd"] * 100.0 / area).where(area > 0)
    sort_col = "timestamp" if "timestamp" in hist.columns else "commit"
    return hist.sort_values(sort_col).reset_index(drop=True)


def make_full_aoi_release_figure(
    df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path
) -> bool:
    """Render the PER-RELEASE full-AOI NEON figure (issue #202 leg 1).

    Unlike the per-merge live matrix (one densest shard/target), this charts the
    whole-AOI cost/runtime truth once per release, x-axis = release tag. Same 2x2
    ``MATRIX_ROWS x MATRIX_COLS`` layout (inline/sidecar x AOI-mask) as the live
    matrix, wall time (``total_wall_s``) on the right axis. Returns False (writing
    nothing) when no full-AOI release rows are retained yet, so ``main`` / the Pages
    index simply omit the section (no broken image)."""
    hist = _full_aoi_history(df)
    # ``_full_aoi_history`` early-returns the frame unchanged when it lacks a
    # ``target`` column, so guard the column's presence too (keeps the two guards
    # consistent -- a non-empty frame without ``target`` returns False, not a
    # KeyError).
    if hist.empty or "target" not in hist.columns or hist["target"].dropna().empty:
        return False
    # A metric column appended after the series started (e.g. objects_total,
    # issue #240) is absent from a pre-append parquet and all-null until the
    # first release records it: skip the figure (no broken/empty panel) rather
    # than KeyError.
    if cost_col not in hist.columns or hist[cost_col].dropna().empty:
        return False
    layout, nrows, ncols = _matrix_layout(hist)
    return _render_panel_grid(
        hist,
        layout,
        nrows,
        ncols,
        cost_col,
        cost_label,
        f"zagg full-AOI NEON (all shards) — {cost_label} vs release",
        out_png,
        runtime_col="total_wall_s",
        runtime_label="wall (s)",
        label_col="ref",
        label_len=None,
    )


def write_index(
    outdir: Path,
    *,
    live_table_png: bool = False,
    has_md: bool = False,
    has_json: bool = False,
) -> None:
    """Emit the Pages index in the issue #250 layout (espg-approved on PR #256):
    per-release summary + diagnostics first, then the per-merge section, then an
    ARCHIVED section embedding every retained-but-retired PNG still on disk
    (the benchmarks branch keeps them; they are no longer regenerated).

    Sections are keyed on PNGs existing on disk, not on what THIS run rendered,
    so both workflows (per-merge and per-release) leave each other's sections
    intact when they re-render only their own figures."""
    blocks: list[str] = []
    links = []
    if has_md:
        links.append('<a href="latest.md">latest.md</a>')
    if has_json:
        links.append('<a href="metrics.json">metrics.json</a>')

    # --- per-release section (on top: full-AOI truth) ---
    release = [
        f'<h3>{title}</h3>\n<img src="{name}.png" alt="{name}">'
        for name, title in (
            ("full_aoi_summary", "Summary — total billed cost (lambda-s ⇔ USD) and wall"),
            ("full_aoi_point_phases", "Point pipeline — per-phase seconds (max shard)"),
            (
                "full_aoi_raster_phases",
                "Raster pipeline — per-stage seconds (work volume, never stacked)",
            ),
            ("full_aoi_objects", "Store objects (issue #240 tripwire)"),
        )
        if (outdir / f"{name}.png").exists()
    ]
    if release:
        blocks.append(
            "<h2>Per-release benchmarks (full-AOI NEON)</h2>\n"
            "<p>The whole AOP_NEON box, every shard, once per release tag — the "
            "point (tdigest, hive, AOI mask) and raster (Sentinel-2 2025) legs.</p>"
        )
        blocks += release

    # --- per-merge section: the collapsed single hive configuration ---
    merge_blocks: list[str] = []
    if live_table_png:
        block = (
            '<h3>Latest merge</h3>\n<img src="merge_table.png" alt="latest merge benchmark table">'
        )
        if links:
            block += f"\n<p>Machine-readable: {' \u00b7 '.join(links)}</p>"
        merge_blocks.append(block)
    merge_blocks += [
        f'<h3>{title}</h3>\n<img src="{name}.png" alt="{name}">'
        for name, title in (
            ("merge_summary", "Summary — total billed cost (lambda-s ⇔ USD) and wall"),
            ("merge_phases", "Diagnostics — per-phase seconds"),
        )
        if (outdir / f"{name}.png").exists()
    ]
    if merge_blocks:
        blocks.append(
            "<hr>\n<h2>Per commit to main (single densest shard)</h2>\n"
            "<p>One configuration — o9, hive, sharded, tdigest, no AOI mask — "
            "isolating code deltas from data drift (merge sha on the x-axis).</p>"
        )
        blocks += merge_blocks

    # --- archived section: retained frozen PNGs, embedded if still present ---
    archived: list[str] = []
    for png, title in (
        ("matrix_table.png", "inline/sidecar × AOI-mask table"),
        ("codec_table.png", "Sharded vs inner-chunk table"),
        ("latest_table.png", "Frozen historical table"),
    ):
        if (outdir / png).exists():
            archived.append(f'<h3>{title} (archived)</h3>\n<img src="{png}" alt="archived {png}">')
    for name in FIGURES:
        for suffix, tag in (
            ("_matrix", "inline/sidecar × AOI-mask"),
            ("_codec", "sharded vs inner"),
            ("", "frozen"),
        ):
            if (outdir / f"{name}{suffix}.png").exists():
                archived.append(
                    f"<h3>{name} ({tag}, archived)</h3>\n"
                    f'<img src="{name}{suffix}.png" alt="archived {name}{suffix}">'
                )
    for name, (_col, label) in FULL_AOI_FIGURES.items():
        # full_aoi_objects stays LIVE (re-rendered above); the cost pair retires.
        if name != "full_aoi_objects" and (outdir / f"{name}.png").exists():
            archived.append(
                f'<h3>{label} (archived)</h3>\n<img src="{name}.png" alt="archived {name}">'
            )
    if (outdir / "full_aoi_phases.png").exists():  # pre-restructure phase figure
        archived.append(
            "<h3>per-phase seconds, single-figure form (archived)</h3>\n"
            '<img src="full_aoi_phases.png" alt="archived full_aoi_phases">'
        )
    if archived:
        blocks.append(
            "<hr>\n<h2>Archived (frozen as of issues #193 / #202 / #250)</h2>\n"
            "<p>The pre-#193 codec + historical series, the pre-#250 "
            "inline/sidecar × AOI-mask 2×2, and the per-100 km² figures — "
            "retained but no longer updated.</p>"
        )
        blocks += archived

    if not blocks:
        blocks = [
            "<p>No retained benchmark runs yet. Charts appear after the first merge to main.</p>"
        ]
    imgs = "\n".join(blocks)
    html = (
        "<!doctype html>\n<html><head><meta charset='utf-8'>\n"
        "<title>zagg Lambda benchmark</title>\n"
        "<style>body{font-family:sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem}"
        "img{max-width:100%;height:auto;border:1px solid #ddd}</style></head>\n"
        "<body>\n<h1>zagg Lambda benchmark</h1>\n"
        "<p>arm64 \u00b7 4 GB \u00b7 o9 \u00b7 hive \u00b7 tdigest \u00b7 NEON SERC AOP box \u00b7 "
        "per release (full AOI) and per merge (single densest shard).</p>\n"
        f"{imgs}\n</body></html>\n"
    )
    (outdir / "index.html").write_text(html)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render benchmark Pages charts from the series.")
    parser.add_argument("--series", required=True, help="Path to the retained parquet series")
    parser.add_argument("--out", required=True, help="Output directory (Pages site)")
    parser.add_argument(
        "--full-aoi-series",
        default=None,
        help="Path to the per-release full-AOI parquet (full_aoi_series.parquet); "
        "renders the per-release point figures when present",
    )
    parser.add_argument(
        "--raster-series",
        default=None,
        help="Path to the per-release raster parquet (raster_series.parquet); "
        "renders the raster diagnostics + summary row when present",
    )
    args = parser.parse_args(argv)

    # The issue #250 renderers live in their own module (plot_series is at the
    # repo's ~1000-line module ceiling); imported here, after this module is
    # fully loaded, so plot_summary may import plot_series helpers if needed.
    import plot_summary

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(args.series) if Path(args.series).exists() else pd.DataFrame()
    fa_path = args.full_aoi_series
    fa_df = pd.read_parquet(fa_path) if fa_path and Path(fa_path).exists() else pd.DataFrame()
    r_path = args.raster_series
    r_df = pd.read_parquet(r_path) if r_path and Path(r_path).exists() else pd.DataFrame()

    rendered: list[str] = []

    # --- per-merge section (issue #250 collapse): the single hive target.
    # The retired matrix/codec/frozen figures are NO LONGER regenerated; their
    # PNGs persist on the benchmarks branch under the Archived section.
    merge_hist = plot_summary.merge_history(df)
    if not merge_hist.empty:
        if plot_summary.make_summary_figure(
            [("per-merge point (hive, densest shard)", merge_hist, "commit")],
            outdir / "merge_summary.png",
        ):
            rendered.append("merge_summary")
        if plot_summary.make_diagnostics_figure(
            merge_hist,
            plot_summary.POINT_PHASE_PANELS,
            "commit",
            outdir / "merge_phases.png",
            "zagg per-merge point — per-phase seconds vs merge",
        ):
            rendered.append("merge_phases")
    latest = _latest_of(merge_hist).to_dict(orient="records")
    live_table_png = _render_table(
        latest,
        _table_title("zagg per-merge benchmark (hive, o9)", latest),
        outdir / "merge_table.png",
    )
    # Machine-readable companions track the LIVE (collapsed) target's latest
    # merge; the per-100 km² column leaves the display (parquet keeps it).
    display_df = merge_hist.drop(columns=["cost_per_100km2_usd"], errors="ignore")
    has_md = not display_df.empty and write_latest_markdown(display_df, outdir / "latest.md")
    has_json = not display_df.empty and write_latest_metrics(display_df, outdir / "metrics.json")

    # --- per-release section: point + raster summary and diagnostics.
    point_hist = plot_summary.point_release_history(fa_df)
    raster_hist = plot_summary.raster_release_history(r_df)
    summary_rows = [
        ("full-AOI point (tdigest, hive, mask)", point_hist, "ref"),
        ("full-AOI raster (S2 2025)", raster_hist, "ref"),
    ]
    if plot_summary.make_summary_figure(summary_rows, outdir / "full_aoi_summary.png"):
        rendered.append("full_aoi_summary")
    if not point_hist.empty and plot_summary.make_diagnostics_figure(
        point_hist,
        plot_summary.POINT_PHASE_PANELS,
        "ref",
        outdir / "full_aoi_point_phases.png",
        "zagg full-AOI point — per-phase seconds vs release",
    ):
        rendered.append("full_aoi_point_phases")
    if not raster_hist.empty and plot_summary.make_diagnostics_figure(
        raster_hist,
        plot_summary.RASTER_STAGE_PANELS,
        "ref",
        outdir / "full_aoi_raster_phases.png",
        "zagg full-AOI raster — per-stage seconds vs release "
        "(work volume: stages overlap, sums can exceed wall — never stacked)",
    ):
        rendered.append("full_aoi_raster_phases")
    if plot_summary.make_release_objects_figure(point_hist, outdir / "full_aoi_objects.png"):
        rendered.append("full_aoi_objects")

    write_index(
        outdir,
        live_table_png=live_table_png,
        has_md=has_md,
        has_json=has_json,
    )
    extras = [
        n
        for n, ok in (
            ("merge_table", live_table_png),
            ("latest.md", has_md),
            ("metrics.json", has_json),
        )
        if ok
    ]
    print(
        f"rendered {len(rendered)} figure(s) [{', '.join(rendered)}]"
        f"{' + ' + ', '.join(extras) if extras else ''} -> {outdir} "
        "(matrix/codec/frozen tiers archived, not regenerated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
