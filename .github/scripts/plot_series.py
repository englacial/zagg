"""Render the GitHub Pages charts from the retained benchmark series (issue #110).

Two figures, x-axis = labelled merge points (the locked design): (1) cost per
shard + Lambda runtime; (2) cost per 100 km^2 + runtime. Each figure is a grid of
per-target panels with cost on the left axis (solid) and runtime on the right
(dashed), so a regression in either shows up against merge history. Writes the
PNGs plus a small ``index.html`` for Pages.

matplotlib lives in the ``benchmark`` (and ``analysis``) extra, not core, so this
is imported lazily and the plot test ``importorskip``s it -- the default test
suite never needs a plotting backend.
"""

from __future__ import annotations

import argparse
import math
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

    targets = sorted(hist["target"].dropna().unique())
    n = len(targets)
    if n == 0:  # rows present but no usable target labels -> nothing to panel
        return False
    ncols = min(2, n)
    nrows = math.ceil(n / ncols)
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

    for i, target in enumerate(targets):
        ax = axes[i // ncols][i % ncols]
        sub = hist[hist["target"] == target]
        xs = list(range(len(sub)))

        # Connecting line stays cost-blue; the markers carry the memory signal
        # (colour = % of the Lambda memory cap, green->red). Rows missing memory
        # plot uncoloured (grey) rather than dropping out.
        fracs = memory_fractions(sub)
        ax.plot(xs, sub[cost_col], "-", color="C0", zorder=1, label=cost_label)
        ax.scatter(
            xs,
            sub[cost_col],
            s=90,
            c=[f if f is not None else float("nan") for f in fracs],
            cmap=MEMORY_CMAP,
            norm=norm,
            edgecolors="C0",
            linewidths=0.6,
            zorder=2,
            plotnonfinite=True,
        )
        ax.set_ylabel(cost_label, color="C0")
        ax.tick_params(axis="y", labelcolor="C0")
        ax.set_title(target, fontsize=10)
        ax.set_xticks(xs)

        # Label every panel with its own commits; the upper rows have the labels
        # hidden afterwards, so only the bottom row of each column shows them.
        labels = [str(c)[:7] for c in sub["commit"]]
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)

        # Runtime on the right axis: hollow circles (not filled squares) so the
        # memory-coloured cost marker stays visible (issue #125). The twin axis is
        # drawn after ``ax``, so raise ``ax`` above it and drop ``ax``'s opaque
        # patch -- otherwise the runtime glyph sits on top of the cost circles.
        rt = ax.twinx()
        rt.plot(
            xs,
            sub["runtime_s"],
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
    for col in range(ncols):
        last = max((i for i in range(n) if i % ncols == col), default=None)
        for i in range(n):
            if i % ncols == col and i != last:
                axes[i // ncols][i % ncols].tick_params(labelbottom=False)

    # Blank any unused panels in the grid.
    for j in range(n, nrows * ncols):
        axes[j // ncols][j % ncols].axis("off")

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


def write_index(outdir: Path, rendered: list[str]) -> None:
    """Emit a minimal Pages index embedding the rendered figures."""
    if rendered:
        imgs = "\n".join(
            f'<h2>{name}</h2>\n<img src="{name}.png" alt="{name}">' for name in rendered
        )
    else:
        imgs = "<p>No retained benchmark runs yet. Charts appear after the first merge to main.</p>"
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
    write_index(outdir, rendered)
    print(f"rendered {len(rendered)} figure(s) -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
