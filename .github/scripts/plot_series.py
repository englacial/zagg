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

    Reads ``memory_gb`` (uniform across the series -- the benchmark pins 2 GB) and
    falls back to 2048 MB when the column is absent or empty.
    """
    if "memory_gb" in sub.columns:
        caps = sub["memory_gb"].dropna()
        if not caps.empty:
            return float(caps.iloc[0]) * 1024.0
    return 2.0 * 1024.0


def _merge_history(df: pd.DataFrame) -> pd.DataFrame:
    """Retained points only (merge runs), ordered by time."""
    hist = df[df["event"] == "merge"].copy()
    if hist.empty:
        return hist
    return hist.sort_values("timestamp").reset_index(drop=True)


def _latest_merge(df: pd.DataFrame) -> pd.DataFrame:
    """Rows of the single most-recent retained (merge) run, ordered by target.

    The latest merge is the commit owning the newest timestamp; all of that
    commit's per-target rows come back together (including failed/zero rows, so
    the published snapshot shows the whole matrix, failures included).
    """
    hist = _merge_history(df)
    if hist.empty:
        return hist
    latest_commit = hist.iloc[-1]["commit"]
    return hist[hist["commit"] == latest_commit].sort_values("target").reset_index(drop=True)


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


def make_latest_table(df: pd.DataFrame, out_png: Path) -> bool:
    """Render the latest-merge table as a PNG (embedded live in the docs by raw
    URL, like the charts). The ``% cap`` cell is shaded on the same green->red
    memory scale as the chart markers. False if no retained run yet."""
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    recs = latest_records(df)
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

    head = recs[0]
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

    commit = str(head.get("commit") or "")[:7]
    ts = str(head.get("timestamp") or "")
    gb = head.get("memory_gb")
    ax.set_title(
        f"zagg Lambda benchmark — latest merge {commit} ({ts})\n"
        f"arm64 · {gb if gb is not None else 'n/a'} GB · one densest shard/target · "
        "% cap shaded green→red",
        fontsize=10,
    )
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return True


def _panel_layout(hist: pd.DataFrame) -> tuple[list[list[str | None]], int, int]:
    """Place each target in a ``(row, col)`` grid, data-driven (issue #121 review).

    Convention: the LEFT column is the rectilinear grids, the RIGHT column is the
    HEALPix grids. A row pairs the two families at the same aggregator and the same
    *shard-size rank* within their family (so e.g. ``rect_6km`` lines up with the
    largest HEALPix shard). Rows are ordered largest-shard-first, top to bottom;
    same-size rows break ties on the aggregator name. New targets slot in by the
    same rule -- nothing is hardcoded to today's eight.
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


def make_figure(df: pd.DataFrame, cost_col: str, cost_label: str, out_png: Path) -> bool:
    """Render one figure (a per-target grid of cost+runtime panels). Returns
    False (writing nothing) when there's no retained data to plot."""
    import matplotlib

    matplotlib.use("Agg")  # headless CI
    import matplotlib.pyplot as plt
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize

    hist = _merge_history(df)
    if hist.empty:
        return False

    if hist["target"].dropna().empty:  # rows present but no labels -> nothing to panel
        return False
    # Meaningful layout: rect grids on the left, HEALPix on the right; rows pair
    # the families at matching aggregator + shard-size rank, largest shard on top.
    layout, nrows, ncols = _panel_layout(hist)
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
            colors = [
                f if (f is not None and v != 0) else float("nan") for f, v in zip(fracs, cost)
            ]
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
            ax.set_title(target, fontsize=10)
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

    # Keep commit labels only on the bottom-most populated panel of each column;
    # hide the rows above so the labels aren't repeated up the grid.
    for c in range(ncols):
        last = max((r for r in range(nrows) if layout[r][c] is not None), default=None)
        for r in range(nrows):
            if layout[r][c] is not None and r != last:
                axes[r][c].tick_params(labelbottom=False)

    fig.suptitle(f"zagg Lambda benchmark — {cost_label} vs merge history")
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


def write_index(
    outdir: Path,
    rendered: list[str],
    *,
    latest_png: bool = False,
    has_md: bool = False,
    has_json: bool = False,
) -> None:
    """Emit a minimal Pages index embedding the rendered figures."""
    blocks: list[str] = []
    if latest_png:
        links = []
        if has_md:
            links.append('<a href="latest.md">latest.md</a>')
        if has_json:
            links.append('<a href="metrics.json">metrics.json</a>')
        block = '<h2>Latest merge</h2>\n<img src="latest_table.png" alt="latest benchmark table">'
        if links:
            block += f"\n<p>Machine-readable: {' · '.join(links)}</p>"
        blocks.append(block)
    blocks += [f'<h2>{name}</h2>\n<img src="{name}.png" alt="{name}">' for name in rendered]
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
        "<p>arm64 · 2 GB · one shard/target · densest cell over the NEON SERC AOP box. "
        "Each point is a merge to <code>main</code>.</p>\n"
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

    rendered = []
    for name, (col, label) in FIGURES.items():
        if not df.empty and make_figure(df, col, label, outdir / f"{name}.png"):
            rendered.append(name)

    # Latest-merge snapshot: a PNG (embedded live in the docs), plus its
    # human/agent-readable companions latest.md + metrics.json (issue #110).
    latest_png = not df.empty and make_latest_table(df, outdir / "latest_table.png")
    has_md = not df.empty and write_latest_markdown(df, outdir / "latest.md")
    has_json = not df.empty and write_latest_metrics(df, outdir / "metrics.json")

    write_index(outdir, rendered, latest_png=latest_png, has_md=has_md, has_json=has_json)
    extras = [
        n
        for n, ok in (("table", latest_png), ("latest.md", has_md), ("metrics.json", has_json))
        if ok
    ]
    print(
        f"rendered {len(rendered)} figure(s){' + ' + ', '.join(extras) if extras else ''} -> {outdir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
