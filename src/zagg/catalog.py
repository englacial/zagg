#!/usr/bin/env python3
"""
Build a local granule catalog from CMR to avoid per-Lambda CMR queries.

This script:
1. Queries CMR for granules matching a date range, product, and spatial extent
2. Discovers parent morton cells via morton_coverage on an input polygon
3. Intersects cells with granule polygons via shapely STRtree
4. Builds a mapping: parent_morton -> [granule S3 URLs]

Usage:
    # General (date range + polygon):
    python -m zagg.catalog --start-date 2024-01-06 --end-date 2024-04-07 \\
        --polygon antarctica.geojson --parent-order 6

    # ICESat-2 convenience (cycle computes dates automatically):
    python -m zagg.catalog --cycle 22 --parent-order 6

    # Custom product:
    python -m zagg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \\
        --short-name ATL08 --polygon my_region.geojson --parent-order 6
"""

import argparse
import json
import logging
import time
import warnings
from datetime import datetime, timedelta
from importlib import resources
from typing import Dict, List

import numpy as np
import requests
from mortie import morton_coverage
from mortie.tools import mort2polygon

logger = logging.getLogger(__name__)

# ICESat-2 constants
_ICESAT2_LAUNCH = datetime(2018, 10, 13)
_ICESAT2_CYCLE_DAYS = 91


def cycle_to_dates(cycle: int) -> tuple[datetime, datetime]:
    """
    Convert an ICESat-2 repeat cycle number to a date range.

    Parameters
    ----------
    cycle : int
        ICESat-2 cycle number (1-based)

    Returns
    -------
    tuple of (start_date, end_date)
        Start and end datetimes for the cycle
    """
    start = _ICESAT2_LAUNCH + timedelta(days=(cycle - 1) * _ICESAT2_CYCLE_DAYS)
    end = start + timedelta(days=_ICESAT2_CYCLE_DAYS)
    return start, end


def load_polygon(geojson_path: str) -> list[tuple]:
    """
    Load polygon(s) from a GeoJSON file.

    Supports Feature, FeatureCollection, Polygon, and MultiPolygon geometries.

    Parameters
    ----------
    geojson_path : str
        Path to a GeoJSON file

    Returns
    -------
    list of (lats, lons)
        One (lats, lons) array pair per polygon ring, suitable for
        morton_coverage multipart input.
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
    """
    Compute a bounding box from polygon parts.

    Parameters
    ----------
    parts : list of (lats, lons)
        Polygon parts as returned by load_polygon

    Returns
    -------
    tuple of (lon_min, lat_min, lon_max, lat_max)
        Bounding box in CMR format
    """
    all_lats = np.concatenate([p[0] for p in parts])
    all_lons = np.concatenate([p[1] for p in parts])
    return (
        float(all_lons.min()),
        float(all_lats.min()),
        float(all_lons.max()),
        float(all_lats.max()),
    )


def load_antarctic_basins(filepath=None):
    """
    Load Antarctic drainage basin polygons.

    Parameters
    ----------
    filepath : str, optional
        Path to basin polygon file. Defaults to the file shipped with mortie.

    Returns
    -------
    list of (lats, lons)
        One (lats, lons) pair per basin, suitable for morton_coverage multipart input.
    """
    import pandas as pd

    if filepath is None:
        import mortie.tests

        filepath = str(
            resources.files(mortie.tests) / "Ant_Grounded_DrainageSystem_Polygons.txt"
        )

    df = pd.read_csv(filepath, names=["Lat", "Lon", "basin"], sep=r"\s+")
    basins = []
    for _, group in df.groupby("basin"):
        basins.append((group["Lat"].values, group["Lon"].values))
    return basins


def discover_cells(parent_order, polygon_parts=None):
    """
    Discover morton cells at parent_order covering a polygon.

    Parameters
    ----------
    parent_order : int
        Morton order for parent cells (e.g., 6)
    polygon_parts : list of (lats, lons), optional
        Polygon parts for coverage. Defaults to Antarctic drainage basins.

    Returns
    -------
    numpy.ndarray
        Sorted array of unique morton indices at parent_order
    """
    if polygon_parts is None:
        polygon_parts = load_antarctic_basins()

    lats_parts = [p[0] for p in polygon_parts]
    lons_parts = [p[1] for p in polygon_parts]
    cells = morton_coverage(lats_parts, lons_parts, order=parent_order)
    logger.info(
        f"Cell discovery: {len(cells)} cells at order {parent_order} from {len(polygon_parts)} parts"
    )
    return cells


def query_cmr(
    start_date: str,
    end_date: str,
    short_name: str = "ATL06",
    version: str = "007",
    provider: str = "NSIDC_CPRD",
    bbox: tuple = None,
    page_size: int = 2000,
) -> List[dict]:
    """
    Query CMR for granules matching temporal and spatial filters.

    Parameters
    ----------
    start_date : str
        Start date (YYYY-MM-DD)
    end_date : str
        End date (YYYY-MM-DD)
    short_name : str
        CMR short name (e.g., ATL06, ATL08)
    version : str
        Product version
    provider : str
        CMR provider
    bbox : tuple of (lon_min, lat_min, lon_max, lat_max), optional
        Bounding box filter
    page_size : int
        Results per page

    Returns
    -------
    list
        List of granule metadata dicts
    """
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    temporal = f"{start_date}T00:00:00Z,{end_date}T23:59:59Z"

    params: Dict[str, str | int] = {
        "provider": provider,
        "short_name": short_name,
        "version": version,
        "page_size": page_size,
        "sort_key": "start_date",
        "temporal": temporal,
        "offset": 0,
    }

    if bbox is not None:
        params["bounding_box"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

    headers = {"Accept": "application/vnd.nasa.cmr.umm_json+json"}

    logger.info(f"Querying CMR for {short_name} v{version}")
    logger.info(f"  Temporal: {temporal}")
    if bbox:
        logger.info(f"  Bounding box: {params['bounding_box']}")

    all_granules = []
    total_hits = None

    while True:
        response = requests.get(cmr_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        if total_hits is None:
            total_hits = int(response.headers.get("CMR-Hits", 0))
            logger.info(f"  Total matching granules: {total_hits}")

        data = response.json()
        items = data.get("items", [])

        if not items:
            break

        all_granules.extend(items)
        logger.info(f"  Retrieved {len(all_granules)}/{total_hits}...")

        if len(items) < page_size or len(all_granules) >= total_hits:
            break

        params["offset"] = int(params["offset"]) + len(items)
        time.sleep(0.1)  # Be nice to CMR

    logger.info(f"Retrieved {len(all_granules)} granules from CMR")
    return all_granules


def extract_granule_info(granule: dict) -> dict:
    """
    Extract S3 URL and geometry points from a CMR granule.

    Parameters
    ----------
    granule : dict
        UMM-JSON granule from CMR

    Returns
    -------
    dict
        Keys: granule_id, s3_url, points (list of (lat, lon) tuples)
    """
    umm = granule.get("umm", {})
    granule_id = umm.get("GranuleUR", "")

    related_urls = umm.get("RelatedUrls", [])
    s3_url = None
    https_url = None
    for url_obj in related_urls:
        url = url_obj.get("URL", "")
        if url.startswith("s3://") and url.endswith(".h5"):
            s3_url = url
        elif url.startswith("https://") and url.endswith(".h5") and url_obj.get("Type") == "GET DATA":
            https_url = url

    points = []
    spatial_extent = umm.get("SpatialExtent", {})
    horiz_spatial = spatial_extent.get("HorizontalSpatialDomain", {})
    geometry = horiz_spatial.get("Geometry", {})
    gpolygons = geometry.get("GPolygons", [])

    if gpolygons:
        boundary = gpolygons[0].get("Boundary", {})
        boundary_points = boundary.get("Points", [])
        for p in boundary_points:
            if "Latitude" in p and "Longitude" in p:
                points.append((p["Latitude"], p["Longitude"]))

    return {
        "granule_id": granule_id,
        "s3_url": s3_url,
        "https_url": https_url,
        "points": points,
    }


def _resolve_backend(backend: str) -> str:
    """Resolve ``"auto"`` to a concrete backend by checking what's importable.

    Prefers spherely (exact S2 intersection, fast) when available; falls back
    to mortie (HEALPix MOC cell-set intersection, no extra deps).
    """
    if backend != "auto":
        return backend
    try:
        import spherely  # noqa: F401

        return "spherely"
    except ImportError:
        return "mortie"


def build_catalog(
    granules: List[dict],
    parent_order: int = None,
    polygon_parts: list = None,
    *,
    grid=None,
    geometry_backend: str = "auto",
    mortie_order: int = 8,
) -> tuple:
    """Build a granule catalog mapping shard keys to granule URLs.

    The new (PR-C+) API takes a ``grid`` keyword and an optional
    ``geometry_backend``. The legacy ``parent_order``-only path stays as a
    deprecated back-compat shim that uses the EPSG:3031 reprojection trick
    appropriate for the existing Antarctic ATL06 workload.

    Parameters
    ----------
    granules : list
        Granule metadata from CMR (UMM-JSON dicts).
    parent_order : int, optional
        **Deprecated.** HEALPix parent order. If provided without ``grid``,
        the legacy EPSG:3031 path is used.
    polygon_parts : list of (lats, lons), optional
        Polygon parts for cell discovery. Defaults to Antarctic drainage
        basins.
    grid : OutputGrid, keyword-only
        Output grid (HealpixGrid, RectilinearGrid, ...). Required for the
        new path. The grid supplies ``coverage`` and ``shard_footprint``.
    geometry_backend : {"auto", "spherely", "mortie", "shapely", "shapely-3031"}
        Sphere-aware geometry backend.

        - ``"auto"`` (default): spherely if importable, else mortie.
        - ``"spherely"``: exact S2 polygon intersection. Requires
          ``pip install zagg[catalog]``.
        - ``"mortie"``: HEALPix MOC cell-set intersection at
          ``mortie_order``. No extra deps.
        - ``"shapely"``: shapely STRtree in WGS84. Antimeridian/pole
          correctness not guaranteed; kept for completeness.
        - ``"shapely-3031"``: legacy EPSG:3031 path (Antarctic only).
    mortie_order : int, default 8
        HEALPix MOC order used by the mortie backend. Orders 6-10 are
        safe (no false negatives in practice); higher orders trade more
        precision for occasional false negatives near polygon boundaries.

    Returns
    -------
    catalog : dict
        Mapping of shard_key (int) -> list of granule URLs.
    timings : dict
        Wall-clock seconds per pipeline step.
    """
    # Legacy back-compat: parent_order without grid → EPSG:3031 path
    if grid is None and parent_order is not None:
        warnings.warn(
            "build_catalog(parent_order=...) is deprecated; pass "
            "grid=HealpixGrid(...) and geometry_backend='auto' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _build_catalog_healpix(granules, parent_order, polygon_parts)
    if grid is None:
        raise ValueError(
            "build_catalog requires either grid=... (preferred) or "
            "parent_order=... (deprecated)"
        )

    chosen = _resolve_backend(geometry_backend)
    if chosen == "spherely":
        return _build_catalog_spherely(granules, grid, polygon_parts)
    if chosen == "mortie":
        return _build_catalog_mortie(granules, grid, polygon_parts, order=mortie_order)
    if chosen == "shapely":
        return _build_catalog_grid_driven(granules, grid, polygon_parts)
    if chosen == "shapely-3031":
        if not hasattr(grid, "parent_order"):
            raise ValueError("'shapely-3031' backend only supports HEALPix grids")
        return _build_catalog_healpix(granules, grid.parent_order, polygon_parts)
    raise ValueError(
        f"unknown geometry_backend: {geometry_backend!r} (resolved to {chosen!r})"
    )


def _build_catalog_healpix(granules, parent_order, polygon_parts):
    """**DEPRECATED — REMOVE ASAP after new-backend verification.**

    Legacy EPSG:3031 Antarctic-only fast path. Kept ONLY long enough to
    confirm the new ``_build_catalog_spherely`` path produces equivalent
    catalogs on a representative Antarctic cycle. Once that's verified,
    this function and the ``geometry_backend='shapely-3031'`` dispatch
    entry both go.

    Use ``build_catalog(..., grid=..., geometry_backend='auto')`` instead.
    """
    warnings.warn(
        "_build_catalog_healpix (EPSG:3031 path) is deprecated and will be "
        "removed in a future release. Use build_catalog(grid=..., "
        "geometry_backend='auto').",
        DeprecationWarning,
        stacklevel=2,
    )
    from pyproj import Transformer
    from shapely import STRtree, make_valid
    from shapely.geometry import Polygon

    timings = {}
    t_total = time.perf_counter()

    to_stereo = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)

    # --- Pass 1: Cell discovery via morton_coverage ---
    t0 = time.perf_counter()
    all_cells = discover_cells(parent_order, polygon_parts)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_cells)} cells")

    # --- Build granule polygons in EPSG:3031 ---
    t0 = time.perf_counter()
    granule_polys = []
    granule_urls = []

    for granule in granules:
        info = extract_granule_info(granule)
        if not info["s3_url"] or len(info["points"]) < 3:
            continue

        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])

        x, y = to_stereo.transform(lons, lats)
        coords = list(zip(x, y))
        try:
            poly = Polygon(coords)
            if not poly.is_valid:
                poly = make_valid(poly)
            if poly.is_empty:
                continue
        except Exception:
            continue

        granule_polys.append(poly)
        granule_urls.append(info["s3_url"])

    timings["granule_polygons"] = time.perf_counter() - t0
    logger.info(f"Built {len(granule_polys)} granule polygons")

    # --- STRtree construction ---
    t0 = time.perf_counter()
    tree = STRtree(granule_polys)
    timings["strtree_construction"] = time.perf_counter() - t0

    # --- Cell polygons via mort2polygon ---
    t0 = time.perf_counter()
    cell_shapely = []
    for cell_id in all_cells:
        verts = mort2polygon(int(cell_id), step=32)
        lats_c = np.array([v[0] for v in verts])
        lons_c = np.array([v[1] for v in verts])
        x, y = to_stereo.transform(lons_c, lats_c)
        poly = Polygon(zip(x, y))
        if not poly.is_valid:
            poly = make_valid(poly)
        cell_shapely.append(poly)
    timings["cell_polygons"] = time.perf_counter() - t0

    # --- STRtree queries ---
    t0 = time.perf_counter()
    catalog: Dict[int, List[str]] = {}
    for i, cell_id in enumerate(all_cells):
        hits = tree.query(cell_shapely[i], predicate="intersects")
        if len(hits) > 0:
            catalog[int(cell_id)] = [granule_urls[j] for j in hits]
    timings["strtree_queries"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total
    logger.info(f"Pass 2: {len(catalog)} cells with granule mappings")

    return catalog, timings


def _build_catalog_grid_driven(granules, grid, polygon_parts):
    """**DEPRECATED — REMOVE ASAP after new-backend verification.**

    Shapely-WGS84 grid-driven path. STRtree intersects in WGS84 without
    S2/MOC awareness; correctness near the antimeridian and poles is not
    guaranteed. Superseded by ``_build_catalog_spherely`` (S2) and
    ``_build_catalog_mortie`` (HEALPix MOC).

    Use ``build_catalog(..., grid=..., geometry_backend='auto')`` instead.
    """
    warnings.warn(
        "_build_catalog_grid_driven (shapely-WGS84 path) is deprecated and "
        "will be removed in a future release. Use build_catalog(grid=..., "
        "geometry_backend='auto').",
        DeprecationWarning,
        stacklevel=2,
    )
    from shapely import STRtree, make_valid
    from shapely.geometry import Polygon

    timings = {}
    t_total = time.perf_counter()

    # --- Pass 1: shard discovery via grid.coverage ---
    if polygon_parts is None:
        from zagg.catalog import load_antarctic_basins

        polygon_parts = load_antarctic_basins()
    t0 = time.perf_counter()
    all_shards = grid.coverage(polygon_parts)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_shards)} shards")

    # --- Build granule polygons in WGS84 ---
    t0 = time.perf_counter()
    granule_polys = []
    granule_urls = []
    for granule in granules:
        info = extract_granule_info(granule)
        if not info["s3_url"] or len(info["points"]) < 3:
            continue
        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])
        try:
            poly = Polygon(zip(lons, lats))
            if not poly.is_valid:
                poly = make_valid(poly)
            if poly.is_empty:
                continue
        except Exception:
            continue
        granule_polys.append(poly)
        granule_urls.append(info["s3_url"])
    timings["granule_polygons"] = time.perf_counter() - t0

    # --- STRtree + per-shard footprint via grid ---
    t0 = time.perf_counter()
    tree = STRtree(granule_polys)
    timings["strtree_construction"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    catalog: Dict[int, List[str]] = {}
    for shard_key in all_shards:
        footprint = grid.shard_footprint(shard_key)
        hits = tree.query(footprint, predicate="intersects")
        if len(hits) > 0:
            catalog[int(shard_key)] = [granule_urls[j] for j in hits]
    timings["strtree_queries"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total
    logger.info(f"Pass 2: {len(catalog)} shards with granule mappings")
    return catalog, timings


def _to_spherely_polygon(lats, lons):
    """Build a closed sphere-aware polygon. Returns None on validation failure.

    Tries the input vertex order first; if S2 rejects it (e.g., area >
    half-sphere because of orientation, or self-intersection from
    non-geodesic interpretation), retries the reverse. Picks whichever
    orientation has the smaller area (the bounded-region interpretation).
    """
    import spherely

    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    if lats[0] != lats[-1] or lons[0] != lons[-1]:
        lats = np.concatenate([lats, lats[:1]])
        lons = np.concatenate([lons, lons[:1]])

    def _try(la, lo):
        try:
            return spherely.create_polygon(spherely.points(la, lo))
        except (ValueError, RuntimeError):
            return None

    half_earth = 2.55e14
    fwd = _try(lats, lons)
    rev = _try(lats[::-1], lons[::-1])
    if fwd is None and rev is None:
        return None
    if fwd is None:
        return rev
    if rev is None:
        return fwd
    return fwd if spherely.area(fwd) < half_earth else rev


def _build_catalog_spherely(granules, grid, polygon_parts):
    """Catalog build via spherely (S2-backed) exact spherical intersection.

    Sphere-aware everywhere: no projection, no antimeridian / pole
    workarounds needed. Spherely 0.1.x has no STRtree, so per-shard query
    is O(N_granules) via vectorized C++ broadcast.
    """
    import spherely

    if polygon_parts is None:
        polygon_parts = load_antarctic_basins()

    timings = {}
    t_total = time.perf_counter()

    # Shard discovery
    t0 = time.perf_counter()
    all_shards = grid.coverage(polygon_parts)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_shards)} shards")

    # Build granule polygons in spherely's S2 space
    t0 = time.perf_counter()
    g_polys_raw = []
    granule_urls = []
    for granule in granules:
        info = extract_granule_info(granule)
        if not info["s3_url"] or len(info["points"]) < 3:
            continue
        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])
        poly = _to_spherely_polygon(lats, lons)
        if poly is None:
            continue
        g_polys_raw.append(poly)
        granule_urls.append(info["s3_url"])
    g_polys = np.array(g_polys_raw)
    timings["granule_polygons"] = time.perf_counter() - t0
    logger.info(f"Built {len(g_polys)} granule polygons")

    # Per-shard vectorized intersect
    t0 = time.perf_counter()
    catalog: Dict[int, List[str]] = {}
    for shard_key in all_shards:
        footprint = grid.shard_footprint(shard_key)
        # grid.shard_footprint returns a shapely Polygon in WGS84; we need
        # the same vertices as a spherely geography.
        sx, sy = footprint.exterior.coords.xy
        s_poly = _to_spherely_polygon(np.asarray(sy), np.asarray(sx))
        if s_poly is None or len(g_polys) == 0:
            continue
        mask = spherely.intersects(g_polys, s_poly)
        hits = np.where(mask)[0]
        if len(hits) > 0:
            catalog[int(shard_key)] = [granule_urls[i] for i in hits]
    timings["strtree_queries"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total
    logger.info(f"Pass 2: {len(catalog)} shards with granule mappings (spherely)")
    return catalog, timings


def _build_catalog_mortie(granules, grid, polygon_parts, *, order=8):
    """Catalog build via mortie HEALPix MOC cell-set intersection.

    Sphere-aware by construction (HEALPix tiles the sphere; no edges).
    Requires mortie >= 0.7.0 (earlier versions had a non-determinism bug,
    espg/mortie#28). Order 6-10 is the safe regime — order >= 12 starts
    dropping boundary-touching intersections due to mortie's polygon-edge
    interpretation.

    For HealpixGrid shards, the shard's MOC comes from
    ``generate_morton_children`` directly (no polygon round-trip). For
    non-HEALPix grids, the shard footprint is reduced to its WGS84
    polygon vertices and fed through ``morton_coverage``.
    """
    from mortie import generate_morton_children, morton_coverage

    if polygon_parts is None:
        polygon_parts = load_antarctic_basins()

    timings = {}
    t_total = time.perf_counter()

    # Shard discovery
    t0 = time.perf_counter()
    all_shards = grid.coverage(polygon_parts)
    timings["cell_discovery"] = time.perf_counter() - t0
    logger.info(f"Pass 1: {len(all_shards)} shards")

    # Build inverted cell→granule index in a vectorized pass (no per-cell
    # dict.add Python loop — see issue #28's prototype work).
    t0 = time.perf_counter()
    cell_arrays = []
    granule_urls = []
    for granule in granules:
        info = extract_granule_info(granule)
        if not info["s3_url"] or len(info["points"]) < 3:
            continue
        lats = np.array([p[0] for p in info["points"]])
        lons = np.array([p[1] for p in info["points"]])
        try:
            cells = morton_coverage(lats, lons, order=order)
        except Exception:
            continue
        if len(cells) == 0:
            continue
        cell_arrays.append(np.asarray(cells, dtype=np.int64))
        granule_urls.append(info["s3_url"])
    timings["granule_polygons"] = time.perf_counter() - t0

    if not cell_arrays:
        timings["total"] = time.perf_counter() - t_total
        return {}, timings

    all_cells = np.concatenate(cell_arrays)
    counts = np.fromiter((len(c) for c in cell_arrays), dtype=np.int64,
                         count=len(cell_arrays))
    all_idx = np.repeat(np.arange(len(cell_arrays), dtype=np.int64), counts)
    order_ = np.argsort(all_cells, kind="stable")
    sorted_cells = all_cells[order_]
    sorted_idx = all_idx[order_]
    timings["strtree_construction"] = time.perf_counter() - t0  # reused timing key

    # Per-shard MOC lookup
    t0 = time.perf_counter()
    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    catalog: Dict[int, List[str]] = {}
    for shard_key in all_shards:
        if is_healpix:
            # Fast path: shard IS a morton cell at parent_order; its MOC
            # at the target order is just its children.
            s_cells = generate_morton_children(int(shard_key), order)
        else:
            footprint = grid.shard_footprint(shard_key)
            sx, sy = footprint.exterior.coords.xy
            try:
                s_cells = morton_coverage(np.asarray(sy), np.asarray(sx),
                                          order=order)
            except Exception:
                continue
        if len(s_cells) == 0:
            continue
        lo = np.searchsorted(sorted_cells, s_cells, side="left")
        hi = np.searchsorted(sorted_cells, s_cells, side="right")
        nonempty = hi > lo
        if not nonempty.any():
            continue
        gathered = np.concatenate(
            [sorted_idx[lo_i:hi_i] for lo_i, hi_i in zip(lo[nonempty], hi[nonempty])]
        )
        hit_indices = np.unique(gathered)
        if hit_indices.size:
            catalog[int(shard_key)] = [granule_urls[int(i)] for i in hit_indices]
    timings["strtree_queries"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total
    logger.info(f"Pass 2: {len(catalog)} shards with granule mappings (mortie order={order})")
    return catalog, timings


def _extract_base_urls(granules: list) -> dict:
    """Extract S3 and HTTPS base URLs from the first granule with both.

    The base URLs are stored in catalog metadata so the runner can rewrite
    S3 URLs to HTTPS URLs at runtime without hardcoding provider paths.

    The S3 URL ``s3://bucket/path/file.h5`` maps to the HTTPS URL
    ``https://host/bucket/path/file.h5``. The file path (everything after
    the bucket name) is identical in both, so we extract the S3 bucket
    prefix and the HTTPS host+bucket prefix.

    Parameters
    ----------
    granules : list
        CMR granule list (only the first with both URLs is used).

    Returns
    -------
    dict
        ``{"s3_base": "s3://bucket", "https_base": "https://host/bucket"}``
        or empty.
    """
    for granule in granules:
        info = extract_granule_info(granule)
        s3_url = info.get("s3_url")
        https_url = info.get("https_url")
        if s3_url and https_url:
            # S3: s3://bucket/path/file.h5 → bucket
            s3_after = s3_url.split("//", 1)[1]  # bucket/path/file.h5
            s3_bucket = s3_after.split("/", 1)[0]  # bucket
            s3_base = f"s3://{s3_bucket}"

            # HTTPS: https://host/bucket/path/file.h5 → https://host/bucket
            https_after = https_url.split("//", 1)[1]  # host/bucket/path/file.h5
            # Find where the bucket name appears in the HTTPS path
            bucket_idx = https_after.find(f"/{s3_bucket}")
            if bucket_idx >= 0:
                https_base = "https://" + https_after[: bucket_idx + 1 + len(s3_bucket)]
            else:
                # Bucket name not in HTTPS path — can't derive mapping
                continue
            return {"s3_base": s3_base, "https_base": https_base}
    return {}


def main():
    parser = argparse.ArgumentParser(
        description="Build granule catalog from CMR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # ICESat-2 cycle (convenience):
  python -m zagg.catalog --cycle 22 --parent-order 6

  # Explicit date range:
  python -m zagg.catalog --start-date 2024-01-06 --end-date 2024-04-07 --parent-order 6

  # Custom region and product:
  python -m zagg.catalog --start-date 2024-01-01 --end-date 2024-06-01 \\
      --short-name ATL08 --polygon my_region.geojson --parent-order 6
""",
    )

    # Temporal
    temporal = parser.add_argument_group("temporal (choose one)")
    temporal.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    temporal.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    temporal.add_argument(
        "--cycle", type=int, help="ICESat-2 cycle number (computes dates automatically)"
    )

    # Product
    parser.add_argument("--short-name", default="ATL06", help="CMR short name (default: ATL06)")
    parser.add_argument("--version", default="007", help="Product version (default: 007)")
    parser.add_argument("--provider", default="NSIDC_CPRD", help="CMR provider")

    # Spatial
    spatial = parser.add_argument_group("spatial")
    spatial.add_argument(
        "--polygon",
        help="GeoJSON file for area of interest (used for cell discovery and CMR bbox)",
    )
    spatial.add_argument(
        "--bbox",
        help="Bounding box override: lon_min,lat_min,lon_max,lat_max",
    )

    # Output
    parser.add_argument("--parent-order", type=int, default=6, help="Parent morton order")
    parser.add_argument("--output", default=None, help="Output JSON file path")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # --- Resolve temporal ---
    if args.cycle:
        start_dt, end_dt = cycle_to_dates(args.cycle)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        parser.error("Provide either --cycle or both --start-date and --end-date")

    # --- Resolve spatial ---
    polygon_parts = None
    bbox = None

    if args.polygon:
        polygon_parts = load_polygon(args.polygon)
        bbox = polygon_to_bbox(polygon_parts)
        logger.info(f"Loaded polygon from {args.polygon}: {len(polygon_parts)} parts")
        logger.info(f"  Auto-computed bbox: {bbox}")

    if args.bbox:
        bbox = tuple(float(x) for x in args.bbox.split(","))

    # --- Query CMR ---
    start_time = time.time()

    granules = query_cmr(
        start_date=start_date,
        end_date=end_date,
        short_name=args.short_name,
        version=args.version,
        provider=args.provider,
        bbox=bbox,
    )

    if not granules:
        print("No granules found!")
        return

    # --- Build catalog ---
    catalog, timings = build_catalog(granules, args.parent_order, polygon_parts)

    print("\nTimings:")
    for step, sec in timings.items():
        print(f"  {step}: {sec:.3f}s")

    granule_counts = [len(urls) for urls in catalog.values()]
    print("\nCatalog statistics:")
    print(f"  Parent cells: {len(catalog)}")
    print(
        f"  Granules per cell: min={min(granule_counts)}, "
        f"max={max(granule_counts)}, avg={np.mean(granule_counts):.1f}"
    )

    # --- Save catalog ---
    if args.output:
        output_file = args.output
    elif args.cycle:
        output_file = f"catalog_{args.short_name}_cycle{args.cycle}_order{args.parent_order}.json"
    else:
        output_file = (
            f"catalog_{args.short_name}_{start_date}_{end_date}_order{args.parent_order}.json"
        )

    # Derive base URLs from first granule for driver URL rewriting
    access_urls = _extract_base_urls(granules)

    output_metadata = {
        "short_name": args.short_name,
        "version": args.version,
        "provider": args.provider,
        "start_date": start_date,
        "end_date": end_date,
        "parent_order": args.parent_order,
        "total_granules": len(granules),
        "total_cells": len(catalog),
        "created": datetime.now().isoformat(),
        **access_urls,
    }
    if args.cycle:
        output_metadata["cycle"] = args.cycle
    if bbox:
        output_metadata["bbox"] = list(bbox)
    if args.polygon:
        output_metadata["polygon"] = args.polygon

    # New catalog format (PR-C): two parallel lists keyed by index.
    output_metadata["grid_type"] = "healpix"
    shard_keys = sorted(int(k) for k in catalog.keys())
    output_data = {
        "metadata": output_metadata,
        "shard_keys": shard_keys,
        "granules": [catalog[k] for k in shard_keys],
    }

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    elapsed = time.time() - start_time
    print(f"\nCatalog saved to: {output_file}")
    print(f"Total time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
