"""ipyleaflet wrapper for the shard-map viewer (issue #38).

Builds an interactive map from a saved ShardMap: a basemap, the shard-outline
layer, and an optional (toggleable) granule-footprint layer.

The CRS is chosen from the map's extent (:mod:`zagg.viz.crs`): polar AOIs get a
NASA polar-stereographic projection (EPSG:3413/3031) with a matching **GIBS**
WMTS basemap, mid-latitude AOIs stay on Web Mercator + OpenStreetMap. Vector
layers stay WGS84 GeoJSON -- proj4leaflet reprojects them client-side -- so the
headless render core is unchanged; the only seam difference is that the +-180
antimeridian split is skipped under a polar CRS (there is no such seam there).

All ``ipyleaflet`` imports are local to the functions here so importing
:mod:`zagg.viz` (and the phase-1 render core / test suite) never requires the
widget stack. Install it with ``pip install zagg[viz]``.
"""

from __future__ import annotations

from zagg.viz.crs import crs_info, is_polar, pick_crs
from zagg.viz.shardmap import (
    _load_catalog,
    _load_shardmap,
    granule_footprints,
    shard_outlines,
)

# Layer styles (kept terse; tweakable by callers via the returned Map).
_SHARD_STYLE = {"color": "#1f78b4", "weight": 1, "fillOpacity": 0.05}
_FOOTPRINT_STYLE = {"color": "#e31a1c", "weight": 1, "fillOpacity": 0.10}


def _center_zoom(fc: dict):
    """Center ``(lat, lon)`` for a FeatureCollection's bbox (zoom is left default)."""
    lons: list[float] = []
    lats: list[float] = []
    for feat in fc["features"]:
        _walk_coords(feat["geometry"]["coordinates"], lons, lats)
    if not lons:
        return (0.0, 0.0)
    return ((min(lats) + max(lats)) / 2, (min(lons) + max(lons)) / 2)


def _walk_coords(coords, lons, lats):
    """Collect lon/lat from an arbitrarily nested GeoJSON coordinate array."""
    if coords and isinstance(coords[0], (int, float)):
        lons.append(coords[0])
        lats.append(coords[1])
        return
    for sub in coords:
        _walk_coords(sub, lons, lats)


def _leaflet_crs(projection: dict):
    """A proj4leaflet ``ipyleaflet.projections`` CRS dict from a projection def.

    ipyleaflet's ``Map.crs`` accepts a flat dict (``name``, ``custom``,
    ``proj4def``, ``origin``, ``bounds``, ``resolutions``) -- the same shape as
    its bundled ``projections.EPSG3413["NASAGIBS"]``. :mod:`zagg.viz.crs` carries
    those values per polar EPSG so they line up with the GIBS tile matrix set.
    """
    return {
        "name": projection["name"],
        "custom": True,
        "proj4def": projection["proj4def"],
        "origin": projection["origin"],
        "bounds": projection["bounds"],
        "resolutions": projection["resolutions"],
    }


def show_shardmap(
    shardmap_path,
    catalog=None,
    *,
    zoom: int = 3,
    basemap=None,
    crs=None,
):
    """Build an interactive ipyleaflet map for a saved ShardMap.

    The display CRS is auto-selected from the map's extent: a polar AOI gets a
    NASA polar-stereographic projection (EPSG:3413 Arctic / EPSG:3031 Antarctic)
    with a matching GIBS WMTS basemap; mid-latitude AOIs keep Web Mercator +
    OpenStreetMap. Pass ``crs=`` to force one of ``"EPSG:3031"``,
    ``"EPSG:3413"``, ``"EPSG:3857"``.

    Parameters
    ----------
    shardmap_path : str or ShardMap
        Path to a ``ShardMap`` JSON file (or an in-memory ``ShardMap``).
    catalog : str or Catalog, optional
        A geoparquet path or a loaded ``Catalog``. When given, a toggleable
        granule-footprint layer is added.
    zoom : int
        Initial map zoom.
    basemap : ipyleaflet basemap, optional
        Overrides the default basemap (OSM for Web Mercator, GIBS for polar).
    crs : str, optional
        Force the display CRS instead of auto-picking from the map extent.

    Returns
    -------
    ipyleaflet.Map
        Map with a shard layer, an optional footprint layer, and a
        ``LayersControl`` for toggling layers.
    """
    # Import first so a missing `viz` extra fails clearly, before any work.
    from ipyleaflet import GeoJSON, LayersControl, Map, TileLayer, basemaps

    shardmap = _load_shardmap(shardmap_path)

    selected_crs = pick_crs(shardmap, override=crs)
    polar = is_polar(selected_crs)
    info = crs_info(selected_crs)
    # Under a polar CRS the +-180 seam does not exist, so skip the split.
    split_seam = not polar

    shards_fc = shard_outlines(shardmap, split_seam=split_seam)

    map_kwargs = {"center": _center_zoom(shards_fc), "zoom": zoom}
    if info["projection"] is not None:
        map_kwargs["crs"] = _leaflet_crs(info["projection"])

    if basemap is not None:
        map_kwargs["basemap"] = basemap
    elif info["basemap"] is not None:
        gibs = info["basemap"]
        map_kwargs["basemap"] = TileLayer(
            url=gibs["url"], attribution=gibs["attribution"], name=gibs["name"]
        )
    else:
        map_kwargs["basemap"] = basemaps.OpenStreetMap.Mapnik

    m = Map(**map_kwargs)

    shard_layer = GeoJSON(data=shards_fc, style=_SHARD_STYLE, name="shards")
    m.add(shard_layer)

    if catalog is not None:
        footprint_fc = granule_footprints(_load_catalog(catalog), split_seam=split_seam)
        footprint_layer = GeoJSON(
            data=footprint_fc, style=_FOOTPRINT_STYLE, name="granule footprints"
        )
        m.add(footprint_layer)

    m.add(LayersControl(position="topright"))
    return m


__all__ = ["show_shardmap"]
