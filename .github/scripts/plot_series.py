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
from pathlib import Path

import pandas as pd

# Cost metric -> (column, human label) for the two retained-series figures.
FIGURES = {
    "cost_per_shard": ("cost_per_shard_usd", "cost / shard (USD)"),
    "cost_per_100km2": ("cost_per_100km2_usd", "cost / 100 km² (USD)"),
}


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

    hist = _merge_history(df)
    if hist.empty:
        return False

    targets = sorted(hist["target"].dropna().unique())
    n = len(targets)
    ncols = min(2, n)
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 3.2 * nrows), squeeze=False)

    for i, target in enumerate(targets):
        ax = axes[i // ncols][i % ncols]
        sub = hist[hist["target"] == target]
        x = range(len(sub))
        labels = [str(c)[:7] for c in sub["commit"]]

        ax.plot(x, sub[cost_col], "o-", color="C0", label=cost_label)
        ax.set_ylabel(cost_label, color="C0")
        ax.tick_params(axis="y", labelcolor="C0")
        ax.set_title(target, fontsize=10)
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)

        rt = ax.twinx()
        rt.plot(x, sub["runtime_s"], "s--", color="C1", label="runtime (s)")
        rt.set_ylabel("runtime (s)", color="C1")
        rt.tick_params(axis="y", labelcolor="C1")

    # Blank any unused panels in the grid.
    for j in range(n, nrows * ncols):
        axes[j // ncols][j % ncols].axis("off")

    fig.suptitle(f"zagg Lambda benchmark — {cost_label} vs merge history")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120)
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
