"""Headless render core for the shard-map viewer (issue #38, phase 1).

Pure Python: turns a :class:`~zagg.catalog.shardmap.ShardMap` (and an optional
:class:`~zagg.catalog.sources.Catalog`) into GeoJSON ``FeatureCollection`` dicts
in WGS84. No browser, no ipyleaflet -- everything here is unit-testable with
just the core deps (``shapely`` + the grid backends).

Three layers are produced:

- :func:`shard_outlines` -- one polygon feature per shard, straight off
  ``grid.shard_footprint(key)``. The grid is reconstructed from the map's own
  ``grid_signature`` (:func:`grid_from_signature`) so no second grid spec is
  needed.
- :func:`granule_footprints` -- one polygon feature per granule footprint,
  decoded from a ``Catalog`` (``granule_records``).
- :func:`viewport_cells` -- shard-order cell outlines clipped to a viewport
  bbox, emitted **only** when ``<= max_shards`` shards intersect the viewport
  (the "grid-on-zoom" gate -- never a global graticule, issue #38).

Antimeridian handling
---------------------
HEALPix shard polygons near +-180 deg come back from mortie's ``mort2polygon``
with longitudes that, read as a flat ring, span more than a hemisphere (e.g.
180 -> -178) and would render as a band wrapping the whole globe. The mortie
path already normalizes vertices that merely *touch* the antimeridian; for the
ones that genuinely *cross* it, :func:`_split_antimeridian` cuts the ring at
+-180 into a ``MultiPolygon`` so GeoJSON consumers draw it correctly.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

# Longitude span (deg) above which a ring is treated as antimeridian-crossing.
_ANTIMERIDIAN_SPAN = 180.0


def grid_from_signature(signature: dict):
    """Reconstruct an output grid from a ``ShardMap.grid_signature``.

    The viewer only needs ``shard_footprint`` / ``children`` off the grid, both
    of which are fully determined by the signature -- so the map is
    self-describing and no separate config is required.

    Parameters
    ----------
    signature : dict
        A grid ``signature()`` dict (``type`` is ``"healpix"`` or
        ``"rectilinear"``).

    Returns
    -------
    OutputGrid

    Raises
    ------
    ValueError
        If ``signature['type']`` is unknown.
    """
    gtype = signature.get("type")
    if gtype == "healpix":
        from zagg.grids import HealpixGrid

        return HealpixGrid(
            parent_order=signature["parent_order"],
            child_order=signature["child_order"],
            layout=signature.get("layout", "fullsphere"),
        )
    if gtype == "rectilinear":
        from zagg.grids import RectilinearGrid

        a, _b, c, _d, e, f = signature["affine"]
        height, width = signature["shape"]
        res_x, res_y = a, -e
        xmin, ymax = c, f
        xmax = c + a * width
        ymin = f + e * height
        return RectilinearGrid(
            crs=signature["crs"],
            resolution=(res_x, res_y),
            bounds=[xmin, ymin, xmax, ymax],
            chunk_shape=tuple(signature["chunk_shape"]),
        )
    raise ValueError(f"unknown grid signature type: {gtype!r}")


# ── GeoJSON geometry helpers ─────────────────────────────────────────────────

def _ring_list(ring) -> list[list[float]]:
    """A shapely ring's ``[[lon, lat], ...]`` as plain floats."""
    x, y = ring.coords.xy
    return [[float(lon), float(lat)] for lon, lat in zip(x, y)]


def _ring_coords(geom) -> list[list[float]]:
    """Exterior-ring ``[[lon, lat], ...]`` for a shapely Polygon."""
    return _ring_list(geom.exterior)


def _crosses_antimeridian(geom) -> bool:
    """True if the polygon's exterior ring jumps the +-180 seam.

    A *jump* -- two consecutive vertices &gt; 180 deg apart in longitude -- means
    the ring crosses the antimeridian. This is distinct from a merely wide
    polygon (e.g. a swath from lon -170 to +170 across 0 deg), whose vertices
    step continuously and never jump, so it is left intact (review of #38
    phase 1).
    """
    lons = np.array([pt[0] for pt in _ring_coords(geom)])
    return bool(np.any(np.abs(np.diff(lons)) > _ANTIMERIDIAN_SPAN))


def _split_antimeridian(geom):
    """Split a Polygon that crosses +-180 deg into hemisphere-local parts.

    Returns a GeoJSON ``geometry`` dict -- a ``Polygon`` (interior rings kept)
    when the ring does not cross the seam, or a ``MultiPolygon`` cut at the
    antimeridian when it does. The cut unwraps the polygon (western vertices
    shifted +360), clips against the ``[-180, 180]`` and ``[180, 540]``
    half-planes, then rewraps the eastern part back into ``[-180, 180]`` --
    shapely-only, no extra deps. Holes are carried through the unwrap/clip.
    """
    from shapely.geometry import Polygon, box

    if not _crosses_antimeridian(geom):
        return {"type": "Polygon", "coordinates": _polygon_rings(geom)}

    # Unwrap: lift western-hemisphere vertices by +360 so the ring is monotone
    # across the seam (e.g. 180, -178 -> 180, 182). Interiors come along.
    def _unwrap(ring):
        return [[lon + 360.0 if lon < 0 else lon, lat] for lon, lat in _ring_list(ring)]

    poly = Polygon(_unwrap(geom.exterior), [_unwrap(r) for r in geom.interiors])
    if not poly.is_valid:
        poly = poly.buffer(0)

    west = poly.intersection(box(-180.0, -90.0, 180.0, 90.0))
    east = poly.intersection(box(180.0, -90.0, 540.0, 90.0))

    parts: list = []
    for part, shift in ((west, 0.0), (east, -360.0)):
        for sub in getattr(part, "geoms", [part]):
            if sub.is_empty or sub.geom_type != "Polygon":
                continue
            parts.append(_polygon_rings(sub, shift=shift))

    if not parts:
        return {"type": "Polygon", "coordinates": _polygon_rings(geom)}
    return {"type": "MultiPolygon", "coordinates": parts}


def _polygon_rings(geom, *, shift: float = 0.0) -> list[list[list[float]]]:
    """GeoJSON ring list ``[exterior, *interiors]`` for a Polygon, lon-shifted."""
    rings = [geom.exterior, *geom.interiors]
    return [[[lon + shift, lat] for lon, lat in _ring_list(r)] for r in rings]


def _plain_geometry(geom) -> dict:
    """GeoJSON geometry for a Polygon/MultiPolygon with no antimeridian split.

    Used under a polar-stereographic CRS, where the +-180 seam is a Mercator/
    WGS84 artifact that does not exist in the projected plane (the singularity is
    the opposite pole instead). Coordinates are plain ``list`` so the result is
    canonical, ``json``-stable GeoJSON.
    """
    if geom.geom_type == "MultiPolygon":
        return {
            "type": "MultiPolygon",
            "coordinates": [_polygon_rings(sub) for sub in geom.geoms],
        }
    if geom.geom_type == "Polygon":
        return {"type": "Polygon", "coordinates": _polygon_rings(geom)}
    raise ValueError(f"unsupported geometry type for GeoJSON: {geom.geom_type}")


def _polygon_geometry(geom, *, split_seam: bool = True) -> dict:
    """GeoJSON geometry for a shapely Polygon/MultiPolygon, antimeridian-safe.

    When ``split_seam`` is False (polar CRS), the +-180 split is skipped -- see
    :func:`_plain_geometry`. Coordinates are plain ``list`` (not shapely's
    tuples) so the result is canonical, ``json``-stable GeoJSON.
    """
    if not split_seam:
        return _plain_geometry(geom)
    if geom.geom_type == "MultiPolygon":
        coords: list = []
        for sub in geom.geoms:
            g = _split_antimeridian(sub)
            if g["type"] == "Polygon":
                coords.append(g["coordinates"])
            else:
                coords.extend(g["coordinates"])
        return {"type": "MultiPolygon", "coordinates": coords}
    if geom.geom_type == "Polygon":
        return _split_antimeridian(geom)
    raise ValueError(f"unsupported geometry type for GeoJSON: {geom.geom_type}")


def _polygonal(geom):
    """Reduce an intersection result to Polygon/MultiPolygon, or None.

    A clip can degenerate to a point/line on a shared edge or yield a
    ``GeometryCollection`` mixing dimensions; keep only the polygonal part.
    """
    if geom.is_empty:
        return None
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return geom
    if geom.geom_type == "GeometryCollection":
        from shapely.geometry import MultiPolygon

        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if not polys:
            return None
        merged = MultiPolygon(
            [p for g in polys for p in getattr(g, "geoms", [g])]
        )
        return merged
    return None


def _feature(geometry: dict, properties: dict) -> dict:
    return {"type": "Feature", "geometry": geometry, "properties": properties}


def _collection(features: list[dict]) -> dict:
    return {"type": "FeatureCollection", "features": features}


# ── layers ───────────────────────────────────────────────────────────────────

def shard_outlines(shardmap, *, split_seam: bool = True) -> dict:
    """Shard/chunk outlines as a GeoJSON ``FeatureCollection``.

    One feature per shard key in the map, with the shard's footprint polygon and
    its granule count under ``properties``. The grid is reconstructed from the
    map's ``grid_signature`` -- no separate grid spec needed.

    Parameters
    ----------
    shardmap : ShardMap
        A built or loaded shard map.
    split_seam : bool
        Split polygons crossing +-180 into a ``MultiPolygon``. Set False under a
        polar-stereographic CRS, where there is no antimeridian seam.

    Returns
    -------
    dict
        GeoJSON ``FeatureCollection`` (WGS84).
    """
    grid = grid_from_signature(shardmap.grid_signature)
    features = []
    for key, granules in zip(shardmap.shard_keys, shardmap.granules):
        geom = grid.shard_footprint(key)
        features.append(
            _feature(
                _polygon_geometry(geom, split_seam=split_seam),
                {"shard_key": _jsonable(key), "n_granules": len(granules)},
            )
        )
    return _collection(features)


def granule_footprints(catalog, *, split_seam: bool = True) -> dict:
    """Granule footprints as a GeoJSON ``FeatureCollection``.

    One polygon feature per granule in the catalog (its footprint
    exterior ring), with the granule id under ``properties``.

    Parameters
    ----------
    catalog : Catalog
        A loaded catalog (provides ``granule_records``).
    split_seam : bool
        Split polygons crossing +-180 into a ``MultiPolygon``. Set False under a
        polar-stereographic CRS, where there is no antimeridian seam.

    Returns
    -------
    dict
        GeoJSON ``FeatureCollection`` (WGS84).
    """
    from shapely.geometry import Polygon

    features = []
    for rec in catalog.granule_records():
        ring = list(zip(np.asarray(rec["lons"]), np.asarray(rec["lats"])))
        if len(ring) < 4:
            continue
        features.append(
            _feature(
                _polygon_geometry(Polygon(ring), split_seam=split_seam),
                {"id": rec["id"]},
            )
        )
    return _collection(features)


def viewport_cells(shardmap, bbox, *, max_shards: int = 4, split_seam: bool = True) -> dict:
    """Shard-order cell outlines clipped to a viewport, gated on visible shards.

    Implements the "grid-on-zoom" behavior (issue #38): cell outlines **at the
    shard order** are drawn only when ``<= max_shards`` shards intersect
    ``bbox``. When more shards are visible the viewport is too zoomed-out for a
    useful grid and an empty collection is returned -- never a global graticule.

    Parameters
    ----------
    shardmap : ShardMap
        A built or loaded shard map.
    bbox : tuple of float
        Viewport ``(lon_min, lat_min, lon_max, lat_max)`` in WGS84.
    max_shards : int
        Maximum number of intersecting shards for the grid to render.
    split_seam : bool
        Split polygons crossing +-180 into a ``MultiPolygon``. Set False under a
        polar-stereographic CRS, where there is no antimeridian seam.

    Returns
    -------
    dict
        GeoJSON ``FeatureCollection`` of shard-cell outlines clipped to the
        viewport, or an empty collection when the gate is not met.
    """
    from shapely.geometry import box

    grid = grid_from_signature(shardmap.grid_signature)
    view = box(bbox[0], bbox[1], bbox[2], bbox[3])

    # Build each footprint once (reproject + densify is not free), then gate.
    visible = [
        (key, fp)
        for key, fp in ((k, grid.shard_footprint(k)) for k in shardmap.shard_keys)
        if fp.intersects(view)
    ]
    if not visible or len(visible) > max_shards:
        return _collection([])

    features = []
    for key, fp in visible:
        clipped = _polygonal(fp.intersection(view))
        if clipped is None:
            continue
        features.append(
            _feature(
                _polygon_geometry(clipped, split_seam=split_seam),
                {"shard_key": _jsonable(key)},
            )
        )
    return _collection(features)


# ── top-level assembly ───────────────────────────────────────────────────────

def render_shardmap(shardmap, catalog=None, *, bbox=None, max_shards: int = 4) -> dict:
    """Assemble all viewer layers for a shard map into one dict of collections.

    Parameters
    ----------
    shardmap : ShardMap or str
        A ``ShardMap`` or a path to a ShardMap JSON file.
    catalog : Catalog or str, optional
        A ``Catalog`` or a geoparquet path. When given, the granule-footprint
        layer is included.
    bbox : tuple of float, optional
        Viewport ``(lon_min, lat_min, lon_max, lat_max)``. When given, the
        viewport-clipped cell-outline layer is included.
    max_shards : int
        Visible-shard gate for the viewport cell layer.

    Returns
    -------
    dict
        ``{"shards": FC, "granules": FC | None, "cells": FC | None}`` where each
        value is a GeoJSON ``FeatureCollection`` (or ``None`` when its input was
        not provided).
    """
    shardmap = _load_shardmap(shardmap)
    out = {"shards": shard_outlines(shardmap), "granules": None, "cells": None}
    if catalog is not None:
        out["granules"] = granule_footprints(_load_catalog(catalog))
    if bbox is not None:
        out["cells"] = viewport_cells(shardmap, bbox, max_shards=max_shards)
    return out


# ── small loaders / utils ────────────────────────────────────────────────────

def _load_shardmap(shardmap):
    """Accept a ShardMap or a JSON path; return a ShardMap."""
    if isinstance(shardmap, (str, Path)):
        from zagg.catalog.shardmap import ShardMap

        return ShardMap.from_json(str(shardmap))
    return shardmap


def _load_catalog(catalog):
    """Accept a Catalog or a geoparquet path; return a Catalog."""
    if isinstance(catalog, (str, Path)):
        from zagg.catalog.sources import Catalog

        return Catalog.from_geoparquet(str(catalog))
    return catalog


def _jsonable(key):
    """Render a shard key (int or tuple) as a JSON-safe value."""
    if isinstance(key, (list, tuple)):
        return [int(k) for k in key]
    try:
        return int(key)
    except (TypeError, ValueError):
        return key


def _is_geojson(obj) -> bool:
    """True if ``obj`` round-trips as JSON and is a FeatureCollection."""
    return (
        isinstance(obj, dict)
        and obj.get("type") == "FeatureCollection"
        and json.loads(json.dumps(obj)) == obj
    )


__all__ = [
    "grid_from_signature",
    "shard_outlines",
    "granule_footprints",
    "viewport_cells",
    "render_shardmap",
]
