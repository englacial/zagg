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
    Absent ``index_backend`` column (a pre-#193 parquet) -> all False."""
    if "index_backend" not in df.columns:
        return pd.Series(False, index=df.index)
    has_backend = df["index_backend"].notna()
    grid_size = df["grid_size"].astype(str) if "grid_size" in df.columns else ""
    is_o9 = grid_size == "o9"
    if "target" in df.columns:
        aoi_suffix = df["target"].astype(str).str.endswith(("_mask", "_nomask"))
    else:
        aoi_suffix = pd.Series(False, index=df.index)
    return has_backend & is_o9 & aoi_suffix.fillna(False)


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


def _draw_panel(ax, sub: pd.DataFrame, cost_col: str, cost_label: str, norm) -> None:
    """Draw one cost+runtime panel for a single target's merge history.

    Shared by the frozen figure (``make_figure``) and the codec figure
    (``make_codec_figure``) so both render points identically -- memory-coloured
    cost markers (left axis), hollow-circle runtime (right axis), failed runs
    (zero) broken out of the line and flagged with an 'x' near the floor.
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

    # Label every panel with its own commits; the upper rows have the
    # labels hidden afterwards, so only the bottom row shows them.
    labels = [str(cm)[:7] for cm in sub["commit"]]
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)

    # Runtime on the right axis: hollow circles (not filled squares) so
    # the memory-coloured cost marker stays visible (issue #125). The twin
    # axis is drawn after ``ax``, so raise ``ax`` above it and drop
    # ``ax``'s opaque patch -- otherwise the runtime glyph sits on top of
    # the cost circles. Zero runtimes break the line the same way.
    runtime = sub["runtime_s"].to_numpy(dtype=float)
    rt_line = [v if v != 0 else float("nan") for v in runtime]
    rt = ax.twinx()
    rt.plot(
        xs,
        rt_line,
        linestyle="--",
        marker="o",
        markerfacecolor="none",
        color="C1",
        label="runtime (s)",
    )
    rt.set_ylabel("runtime (s)", color="C1")
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
) -> bool:
    """Render a per-target grid of cost+runtime panels from ``layout`` + write it.

    The shared body of ``make_figure`` (frozen series) and ``make_codec_figure``
    (the 2x3 sharded-vs-inner matrix): same colour scale, colorbar (with the MB
    twin axis), per-panel draw, and bottom-row-only commit labels. ``layout`` (and
    its ``nrows``/``ncols``) is the only thing the two callers vary.
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
            _draw_panel(ax, sub, cost_col, cost_label, norm)
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
# The full-AOI run harness + its recorded schema are another agent's deliverable
# (issue #202 leg 1; the pinned full-AOI targets land in targets_full_aoi_neon.json,
# NOT owned here). This is the render side: a skeleton kept next to the per-merge
# renderers so the plot lands the moment leg 1's output schema is fixed.

# The columns the per-release full-AOI figure will chart once leg 1's schema lands
# (release tag on the x-axis, these on the y). Provisional -- reconcile with leg 1.
FULL_AOI_METRICS = {
    "cost_total": ("total cost (USD)", "whole-AOI dollar total across all shards"),
    "runtime_wall": ("wall time (s)", "end-to-end AOI dispatch wall"),
}


def make_full_aoi_release_figure(
    df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path
) -> bool:
    """Render the PER-RELEASE full-AOI NEON figure (issue #202 leg 1).

    Unlike the per-merge live matrix (one densest shard/target), this charts the
    whole-AOI cost/runtime truth once per release, x-axis = release tag.

    TODO(issue #202 leg 1): wire to the full-AOI harness output. Blocked on that
    agent's recorded schema (the per-release full-AOI records + their pinned
    targets in ``targets_full_aoi_neon.json``, owned by leg 1). Once the schema is
    fixed, this should: (1) select the per-release full-AOI rows from ``df`` (an
    ``event == "release"`` / ``run == "full_aoi"`` discriminator, TBD by leg 1),
    (2) lay out one panel per ``FULL_AOI_METRICS`` entry, and (3) reuse
    ``_render_panel_grid``'s cost+runtime panel style with the release tag on the
    x-axis instead of the commit. Returns False until then so ``main`` skips it
    and the Pages index simply omits the section (no broken image)."""
    return False


def write_index(
    outdir: Path,
    matrix_rendered: list[str],
    *,
    matrix_table_png: bool = False,
    has_md: bool = False,
    has_json: bool = False,
) -> None:
    """Emit a Pages index: the live inline-vs-sidecar matrix on top (issue
    #193), then an ARCHIVED section embedding the retained (frozen) codec +
    historical PNGs if they still exist in ``outdir`` (they persist on the
    benchmarks branch; this PR stopped regenerating them)."""
    blocks: list[str] = []
    links = []
    if has_md:
        links.append('<a href="latest.md">latest.md</a>')
    if has_json:
        links.append('<a href="metrics.json">metrics.json</a>')

    # --- live matrix section (on top) ---
    if matrix_table_png:
        block = (
            "<h2>inline/sidecar \u00d7 AOI-mask (sharded, K=4, o9) \u2014 latest merge</h2>\n"
            '<img src="matrix_table.png" alt="inline/sidecar x AOI-mask benchmark table">'
        )
        if links:
            block += f"\n<p>Machine-readable: {' \u00b7 '.join(links)}</p>"
        blocks.append(block)
    blocks += [
        f"<h2>{name} (inline/sidecar \u00d7 AOI-mask)</h2>\n"
        f'<img src="{name}_matrix.png" alt="{name}_matrix">'
        for name in matrix_rendered
    ]

    # --- archived section (below): retained frozen PNGs, embedded if present ---
    archived: list[str] = []
    if (outdir / "codec_table.png").exists():
        archived.append(
            "<h3>Sharded vs inner-chunk (archived)</h3>\n"
            '<img src="codec_table.png" alt="archived codec table">'
        )
    if (outdir / "latest_table.png").exists():
        archived.append(
            "<h3>Frozen historical (archived)</h3>\n"
            '<img src="latest_table.png" alt="archived historical table">'
        )
    for name in FIGURES:
        for suffix, tag in (("_codec", "sharded vs inner"), ("", "frozen")):
            if (outdir / f"{name}{suffix}.png").exists():
                archived.append(
                    f"<h3>{name} ({tag}, archived)</h3>\n"
                    f'<img src="{name}{suffix}.png" alt="archived {name}{suffix}">'
                )
    if archived:
        blocks.append(
            "<hr>\n<h2>Archived (frozen as of issues #193 / #202)</h2>\n"
            "<p>The pre-#193 codec (sharded/inner) + historical series and the "
            "pre-#202 o10 read-backend rows, retained but no longer updated.</p>"
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
        "<p>arm64 \u00b7 4 GB \u00b7 one shard/target \u00b7 densest cell over the NEON SERC "
        "AOP box. Each point is a merge to <code>main</code>.</p>\n"
        f"{imgs}\n</body></html>\n"
    )
    (outdir / "index.html").write_text(html)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render benchmark Pages charts from the series.")
    parser.add_argument("--series", required=True, help="Path to the retained parquet series")
    parser.add_argument("--out", required=True, help="Output directory (Pages site)")
    args = parser.parse_args(argv)

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(args.series) if Path(args.series).exists() else pd.DataFrame()

    # Live matrix (issue #193): inline-vs-sidecar, sharded, K=4. The retired
    # codec (sharded/inner) + frozen historical figures are NO LONGER
    # regenerated -- their PNGs are retained on the benchmarks branch as-is
    # (archived), and only the matrix figures below update going forward.
    matrix_rendered: list[str] = []
    for name, (col, label) in FIGURES.items():
        if not df.empty and make_matrix_figure(df, col, label, outdir / f"{name}_matrix.png"):
            matrix_rendered.append(name)

    matrix_table_png = not df.empty and make_matrix_latest_table(df, outdir / "matrix_table.png")
    # Machine-readable companions track the live matrix's latest merge.
    matrix_df = df[_matrix_mask(df)] if not df.empty else df
    has_md = not matrix_df.empty and write_latest_markdown(matrix_df, outdir / "latest.md")
    has_json = not matrix_df.empty and write_latest_metrics(matrix_df, outdir / "metrics.json")

    write_index(
        outdir,
        matrix_rendered,
        matrix_table_png=matrix_table_png,
        has_md=has_md,
        has_json=has_json,
    )
    extras = [
        n
        for n, ok in (
            ("matrix_table", matrix_table_png),
            ("latest.md", has_md),
            ("metrics.json", has_json),
        )
        if ok
    ]
    print(
        f"rendered {len(matrix_rendered)} matrix figure(s)"
        f"{' + ' + ', '.join(extras) if extras else ''} -> {outdir} "
        "(codec + frozen tiers archived, not regenerated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
