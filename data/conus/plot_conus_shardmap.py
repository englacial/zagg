"""Render the CONUS HEALPix shard map (issue #202): shard outlines at o9 and o8,
each with a zoomed San Francisco Bay inset so the tiling is legible.

Shard polygons come from ``mortie.tools.mort2polygon`` (the same parent-cell
footprint ``HealpixGrid.shard_footprint`` uses). Outputs two PNGs under
``docs/deployment/`` embedded in ``conus_estimate.md``.

Run: ``uv run --with matplotlib python data/conus/plot_conus_shardmap.py``
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from mortie.tools import mort2polygon  # noqa: E402
from shapely.geometry import Polygon, box  # noqa: E402

HERE = Path(__file__).parent
REPO = HERE.parents[1]
DOCS = REPO / "docs" / "deployment"

# lon0, lat0, lon1, lat1 -- San Francisco Bay area
SFBAY = (-122.65, 37.2, -121.5, 38.35)
CONUS_XLIM = (-125.5, -66.0)
CONUS_YLIM = (24.0, 50.0)


def shard_polys(parquet: Path, step: int = 4) -> gpd.GeoDataFrame:
    keys = pq.read_table(str(parquet)).column("shard_key").to_pylist()
    polys = []
    for k in keys:
        v = mort2polygon(int(k), step=step)
        polys.append(Polygon((p[1], p[0]) for p in v))  # (lon, lat)
    gdf = gpd.GeoDataFrame(geometry=polys, crs="EPSG:4326")
    # drop any base-cell-wrap degenerate polygon (spuriously huge span)
    span = gdf.bounds
    ok = (span.maxx - span.minx < 20) & (span.maxy - span.miny < 20)
    return gdf[ok]


def make_figure(parquet: Path, order: int, n_shards: int, area_km2: float, out: Path) -> None:
    gdf = shard_polys(parquet)
    conus = gpd.read_file(str(HERE / "conus.geojson"))

    fig, ax = plt.subplots(figsize=(11, 6.6))
    gdf.boundary.plot(ax=ax, linewidth=0.05, color="#3a7ca5", alpha=0.6)
    conus.boundary.plot(ax=ax, linewidth=0.7, color="black")
    ax.set_xlim(*CONUS_XLIM)
    ax.set_ylim(*CONUS_YLIM)
    ax.set_aspect(1.28)
    ax.set_title(
        f"CONUS HEALPix shard map — order {order}  ({n_shards:,} shards, {area_km2:g} km² each)",
        fontsize=12,
    )
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # SF Bay inset
    axins = ax.inset_axes([0.60, 0.045, 0.385, 0.5])
    bay = gdf[gdf.intersects(box(*SFBAY))]
    bay.boundary.plot(ax=axins, linewidth=0.6, color="#3a7ca5")
    conus.boundary.plot(ax=axins, linewidth=1.0, color="black")
    axins.set_xlim(SFBAY[0], SFBAY[2])
    axins.set_ylim(SFBAY[1], SFBAY[3])
    axins.set_aspect(1.28)
    axins.set_xticklabels([])
    axins.set_yticklabels([])
    axins.set_title(f"San Francisco Bay ({len(bay)} shards)", fontsize=9)
    ax.indicate_inset_zoom(axins, edgecolor="red", linewidth=1.0)

    fig.tight_layout()
    fig.savefig(str(out), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}  ({len(gdf):,} shard polys, {len(bay)} in bay)")


def main() -> None:
    make_figure(
        HERE / "conus_shard_granule_counts.parquet",
        9,
        49285,
        162.15,
        DOCS / "conus_shardmap_o9.png",
    )
    make_figure(
        HERE / "conus_shard_granule_counts_o8.parquet",
        8,
        12596,
        648.58,
        DOCS / "conus_shardmap_o8.png",
    )


if __name__ == "__main__":
    main()
