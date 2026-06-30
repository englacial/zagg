"""Catalog construction: fetch granule metadata, build a shard map.

The two concerns of #24 live in submodules:

- :mod:`zagg.catalog.sources` -- fetch (``Query``, ``CMRSource``, ``Catalog``).
- :mod:`zagg.catalog.shardmap` -- build the shard map (``ShardMap``).

This package root keeps the small region/temporal helpers shared by both, the
:func:`make_shardmap` convenience that chains fetch -> build, and the CLI
(``python -m zagg.catalog --config ...``) which builds the output grid from the
*same* pipeline config the aggregator uses -- so the shard map can never be
built against a different grid than the run (enforced via ``grid.signature()``).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from importlib import resources

import numpy as np

from zagg.catalog.beams import beam_tracks_from_cmr_polygon

# ICESat-2 constants
_ICESAT2_LAUNCH = datetime(2018, 10, 13)
_ICESAT2_CYCLE_DAYS = 91


def cycle_to_dates(cycle: int) -> tuple[datetime, datetime]:
    """Convert an ICESat-2 repeat cycle number to a ``(start, end)`` date range.

    Parameters
    ----------
    cycle : int
        ICESat-2 cycle number (1-based).

    Returns
    -------
    tuple of (datetime, datetime)
    """
    start = _ICESAT2_LAUNCH + timedelta(days=(cycle - 1) * _ICESAT2_CYCLE_DAYS)
    return start, start + timedelta(days=_ICESAT2_CYCLE_DAYS)


def load_polygon(geojson_path: str) -> list[tuple]:
    """Load polygon(s) from a GeoJSON file as ``(lats, lons)`` parts.

    Supports Feature, FeatureCollection, Polygon, and MultiPolygon geometries.

    Parameters
    ----------
    geojson_path : str
        Path to a GeoJSON file.

    Returns
    -------
    list of (lats, lons)
        One coordinate-array pair per polygon ring (WGS84).
    """
    from shapely.geometry import shape

    with open(geojson_path) as f:
        geojson = json.load(f)

    if geojson.get("type") == "FeatureCollection":
        features = geojson["features"]
    elif geojson.get("type") == "Feature":
        features = [geojson]
    else:
        features = [{"geometry": geojson}]

    parts = []
    for feat in features:
        geom = shape(feat["geometry"])
        if geom.geom_type == "Polygon":
            coords = np.array(geom.exterior.coords)
            parts.append((coords[:, 1], coords[:, 0]))  # GeoJSON is (lon, lat)
        elif geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                coords = np.array(poly.exterior.coords)
                parts.append((coords[:, 1], coords[:, 0]))
    return parts


def polygon_to_bbox(parts: list[tuple]) -> tuple[float, float, float, float]:
    """Compute a ``(lon_min, lat_min, lon_max, lat_max)`` bbox from polygon parts.

    Parameters
    ----------
    parts : list of (lats, lons)

    Returns
    -------
    tuple of (lon_min, lat_min, lon_max, lat_max)
    """
    all_lats = np.concatenate([p[0] for p in parts])
    all_lons = np.concatenate([p[1] for p in parts])
    return (
        float(all_lons.min()),
        float(all_lats.min()),
        float(all_lons.max()),
        float(all_lats.max()),
    )


def load_antarctic_basins(filepath=None) -> list[tuple]:
    """Load Antarctic drainage basin polygons as ``(lats, lons)`` parts.

    Parameters
    ----------
    filepath : str, optional
        Path to the basin polygon file. Defaults to the file shipped with mortie.

    Returns
    -------
    list of (lats, lons)
        One pair per basin.
    """
    import pandas as pd

    if filepath is None:
        import mortie.tests

        filepath = str(resources.files(mortie.tests) / "Ant_Grounded_DrainageSystem_Polygons.txt")

    df = pd.read_csv(filepath, names=["Lat", "Lon", "basin"], sep=r"\s+")
    return [(g["Lat"].values, g["Lon"].values) for _, g in df.groupby("basin")]


def make_shardmap(
    query, grid, *, region=None, aoi=None, backend="auto", catalog_out=None, footprint="swath"
):
    """Fetch a Catalog and build a ShardMap in one call (concerns 1+2 chained).

    Parameters
    ----------
    query : zagg.catalog.sources.Query
        What/when/where to fetch.
    grid : OutputGrid
        Output grid (typically ``from_config(config)``).
    region : list of (lats, lons), optional
        Coverage mask. Defaults to the query bbox rectangle.
    aoi : AOIGeometry | bytes | str | list of (lats, lons), optional
        Strict-AOI polygon for the optional ``output.aoi_mask`` (issue #101) — WKB
        ``bytes``, WKT ``str``, an :class:`~zagg.grids.aoi.AOIGeometry`, or ring
        parts. ``None`` reuses ``region``. Forwarded to :meth:`ShardMap.build`.
    backend : str
        Geometry backend for the shard map.
    catalog_out : str, optional
        If given, persist the fetched Catalog to this geoparquet path.
    footprint : {"swath", "beams"}
        Granule footprint for intersection; ``"beams"`` tightens ICESat-2
        ATL03/06 assignment to per-beam-pair corridors (issue #65).

        .. deprecated::
            The ``"beams"`` corridor mechanism is a stopgap. Remove it once a
            better fix lands -- native per-beam CMR geometry, the memory-handling
            robustness in #66, or data virtualization tracked in #97.

    Returns
    -------
    zagg.catalog.shardmap.ShardMap
    """
    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import CMRSource

    cat = CMRSource().fetch(query)
    if catalog_out:
        cat.to_geoparquet(catalog_out)
    return ShardMap.build(cat, grid, region=region, aoi=aoi, backend=backend, footprint=footprint)


def main():
    """CLI: build a shard map from CMR-STAC for a pipeline config's grid."""
    import argparse
    import logging

    from zagg.catalog.shardmap import ShardMap
    from zagg.catalog.sources import CMRSource, Query
    from zagg.config import load_config
    from zagg.grids import from_config

    parser = argparse.ArgumentParser(
        description="Build a granule shard map from CMR-STAC for a config's grid.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # ICESat-2 cycle, HEALPix grid from atl06.yaml:
  python -m zagg.catalog --config atl06.yaml --short-name ATL06 --cycle 22 \\
      --polygon antarctica.geojson

  # Date range, rectilinear (UTM) grid from a config, over a bbox:
  python -m zagg.catalog --config serc_atl03.yaml --short-name ATL03 \\
      --start-date 2025-01-01 --end-date 2025-12-31 \\
      --bbox -76.62107,38.84504,-76.50583,38.93512
""",
    )
    parser.add_argument(
        "--config", required=True, help="Pipeline config YAML (defines the output grid)"
    )
    parser.add_argument("--short-name", required=True, help="CMR short name (e.g. ATL03)")
    parser.add_argument("--version", default="007", help="Product version (default: 007)")
    parser.add_argument("--provider", default="NSIDC_CPRD", help="CMR/STAC provider")

    temporal = parser.add_argument_group("temporal (choose one)")
    temporal.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    temporal.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    temporal.add_argument("--cycle", type=int, help="ICESat-2 cycle (computes dates)")

    spatial = parser.add_argument_group("spatial (choose one)")
    spatial.add_argument("--polygon", help="GeoJSON area of interest")
    spatial.add_argument("--bbox", help="lon_min,lat_min,lon_max,lat_max")
    parser.add_argument(
        "--aoi-wkt",
        help="Strict-AOI polygon as WKT (text or a path to a .wkt file) for "
        "output.aoi_mask (issue #101). Defaults to --polygon/--bbox coverage.",
    )
    parser.add_argument(
        "--aoi-wkb",
        help="Strict-AOI polygon as a path to a binary WKB file for output.aoi_mask "
        "(issue #101). Defaults to --polygon/--bbox coverage.",
    )

    parser.add_argument("--backend", default="auto", choices=["auto", "spherely", "mortie"])
    parser.add_argument(
        "--footprint",
        default="swath",
        choices=["swath", "beams"],
        help="ATL03/06: 'beams' decomposes the CMR swath into "
        "per-beam-pair corridors to tighten shard assignment (#65). "
        "DEPRECATED -- remove once native per-beam CMR geometry, "
        "#66, or #97 lands",
    )
    parser.add_argument(
        "--preserve-thumbnails",
        action="store_true",
        help="Keep browse/thumbnail assets in the Catalog",
    )
    parser.add_argument("--output", default=None, help="Output ShardMap JSON path")
    parser.add_argument(
        "--catalog-out", default=None, help="Optional: persist the fetched Catalog as geoparquet"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    config = load_config(args.config)
    grid = from_config(config)

    # Temporal
    if args.cycle:
        start_dt, end_dt = cycle_to_dates(args.cycle)
        start, end = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    elif args.start_date and args.end_date:
        start, end = args.start_date, args.end_date
    else:
        parser.error("provide --cycle or both --start-date and --end-date")

    # Spatial: parts drive coverage; bbox drives the CMR query.
    if args.polygon:
        parts = load_polygon(args.polygon)
        bbox = polygon_to_bbox(parts)
    elif args.bbox:
        bbox = tuple(float(x) for x in args.bbox.split(","))
        parts = None
    else:
        parser.error("provide --polygon or --bbox")

    # Strict-AOI geometry (issue #101), optional and independent of coverage:
    # WKB/WKT supplies the AOI polygon for output.aoi_mask while coverage still
    # uses --polygon/--bbox. Mutually exclusive; ``None`` -> mask reuses coverage.
    if args.aoi_wkt and args.aoi_wkb:
        parser.error("provide at most one of --aoi-wkt / --aoi-wkb")
    aoi = None
    if args.aoi_wkb:
        from pathlib import Path

        aoi = Path(args.aoi_wkb).read_bytes()
    elif args.aoi_wkt:
        from pathlib import Path

        p = Path(args.aoi_wkt)
        aoi = p.read_text() if p.exists() else args.aoi_wkt

    query = Query(args.short_name, args.version, start, end, region=bbox, provider=args.provider)
    cat = CMRSource().fetch(query, preserve_thumbnails=args.preserve_thumbnails)
    print(f"Fetched {len(cat)} granules ({query.collection})")
    if args.catalog_out:
        cat.to_geoparquet(args.catalog_out)
        print(f"Catalog -> {args.catalog_out}")

    sm = ShardMap.build(
        cat, grid, region=parts, aoi=aoi, backend=args.backend, footprint=args.footprint
    )
    out = args.output or f"shardmap_{args.short_name}_{start}_{end}.json"
    sm.to_json(out)
    print(
        f"ShardMap: {len(sm.shard_keys)} shards, {sm.metadata['total_pairs']} pairs "
        f"(backend={sm.metadata['backend']}) -> {out}"
    )


if __name__ == "__main__":
    main()


__all__ = [
    "cycle_to_dates",
    "load_polygon",
    "polygon_to_bbox",
    "load_antarctic_basins",
    "make_shardmap",
    "beam_tracks_from_cmr_polygon",
    "main",
]
