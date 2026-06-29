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
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 3.2 * nrows), squeeze=False)

    # Fixed [0, 1] normalisation so colour means the same fraction-of-cap in
    # every panel and across both figures -- 1.0 (red) is the OOM wall.
    norm = Normalize(vmin=0.0, vmax=1.0)

    for i, target in enumerate(targets):
        ax = axes[i // ncols][i % ncols]
        sub = hist[hist["target"] == target]
        x = list(range(len(sub)))
        labels = [str(c)[:7] for c in sub["commit"]]

        # Connecting line stays cost-blue; the markers carry the memory signal
        # (colour = % of the Lambda memory cap, green->red). Rows missing memory
        # plot uncoloured (grey) rather than dropping out.
        fracs = memory_fractions(sub)
        ax.plot(x, sub[cost_col], "-", color="C0", zorder=1, label=cost_label)
        ax.scatter(
            x,
            sub[cost_col],
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
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)

        rt = ax.twinx()
        rt.plot(x, sub["runtime_s"], "s--", color="C1", label="runtime (s)")
        rt.set_ylabel("runtime (s)", color="C1")
        rt.tick_params(axis="y", labelcolor="C1")

    # Blank any unused panels in the grid.
    for j in range(n, nrows * ncols):
        axes[j // ncols][j % ncols].axis("off")

    # One shared colorbar for the memory scale (issue #120).
    sm = ScalarMappable(norm=norm, cmap=MEMORY_CMAP)
    cbar = fig.colorbar(sm, ax=axes.ravel().tolist(), fraction=0.025, pad=0.02)
    cbar.set_label("peak memory (% of cap)")

    fig.suptitle(f"zagg Lambda benchmark — {cost_label} vs merge history")
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
