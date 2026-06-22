"""ipyleaflet wrapper for the shard-map viewer (issue #38, phases 2 & C).

Builds an interactive map from a saved ShardMap: a basemap, the shard-outline
layer, an optional (toggleable) granule-footprint layer, and a grid layer that
draws shard-order cell outlines only when the viewport is zoomed in far enough
to show ``<= max_shards`` shards (the "grid-on-zoom" gate -- never a global
graticule, issue #38).

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

import asyncio

from zagg.viz.crs import crs_info, is_polar, pick_crs
from zagg.viz.shardmap import (
    _load_catalog,
    _load_shardmap,
    granule_footprints,
    shard_index,
    shard_outlines,
    viewport_cells,
)

# Layer styles (kept terse; tweakable by callers via the returned Map).
_SHARD_STYLE = {"color": "#1f78b4", "weight": 1, "fillOpacity": 0.05}
_FOOTPRINT_STYLE = {"color": "#e31a1c", "weight": 1, "fillOpacity": 0.10}
_GRID_STYLE = {"color": "#333333", "weight": 1, "fillOpacity": 0.0}

# Coalesce a burst of pan/zoom ``bounds`` ticks into one grid refresh after the
# viewport settles (seconds). Without this the grid recomputes on every
# intermediate frame of a drag.
_GRID_DEBOUNCE_S = 0.2


def _debounce(wait, func):
    """Wrap ``func`` so rapid calls coalesce to one call ``wait`` s after the last.

    The coalesced call is scheduled on the **kernel's own asyncio event loop**
    (``loop.call_later``) -- the same main thread that owns the ipywidgets comm
    -- not on a background ``threading.Timer``. That distinction is the fix for
    the kernel-crash report (PR #44): the Jupyter widget comm channel is not
    thread-safe, so mutating a widget traitlet (here ``grid_layer.data``) from a
    timer thread can hang or corrupt the comm and crash the kernel. Scheduling on
    the running loop keeps every refresh on the main thread.

    Each call cancels the pending :class:`asyncio.TimerHandle` and reschedules,
    so a burst of events fires ``func`` once when they stop. Returns the wrapper
    with a ``cancel()`` to tear the pending call down.

    When no event loop is running (e.g. a plain script / headless test) the call
    is run synchronously -- there is no comm to protect and no loop to schedule
    on, so coalescing is moot.
    """
    handle: list = [None]

    def _fire(args, kwargs):
        handle[0] = None
        func(*args, **kwargs)

    def wrapper(*args, **kwargs):
        if handle[0] is not None:
            handle[0].cancel()
            handle[0] = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            func(*args, **kwargs)
            return
        handle[0] = loop.call_later(wait, _fire, args, kwargs)

    def cancel():
        if handle[0] is not None:
            handle[0].cancel()
            handle[0] = None

    wrapper.cancel = cancel
    return wrapper


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
    max_shards: int = 4,
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
        granule-footprint layer is added (off by default in the layer control).
    max_shards : int
        Visible-shard gate for the grid-on-zoom layer.
    zoom : int
        Initial map zoom.
    basemap : ipyleaflet basemap, optional
        Overrides the default basemap (OSM for Web Mercator, GIBS for polar).
    crs : str, optional
        Force the display CRS instead of auto-picking from the map extent.

    Returns
    -------
    ipyleaflet.Map
        Map with a shard layer, optional footprint layer, a zoom-thresholded
        grid layer, and a ``LayersControl`` for toggling layers.
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

    # Grid-on-zoom: an empty layer kept in sync with the viewport. The shard
    # footprints are indexed once (STRtree) so each refresh is a cheap query, not
    # a full footprint rebuild; the headless core's gate returns nothing when too
    # many shards are visible, so this never becomes a global graticule.
    grid_layer = GeoJSON(
        data={"type": "FeatureCollection", "features": []},
        style=_GRID_STYLE,
        name="grid (shard cells)",
    )
    m.add(grid_layer)

    index = shard_index(shardmap)

    def _refresh_grid(event=None):  # noqa: ARG001 (traitlets observe signature)
        bounds = m.bounds
        if not bounds:
            return
        (south, west), (north, east) = bounds
        grid_layer.data = viewport_cells(
            shardmap,
            (west, south, east, north),
            max_shards=max_shards,
            split_seam=split_seam,
            index=index,
        )

    # Debounce so a burst of intermediate pan/zoom ticks coalesces into one
    # refresh after movement settles, scheduled on the kernel's event loop (main
    # thread) -- never a background thread mutating the widget comm. Keep a handle
    # so the pending callback can be cancelled (``m.cancel_grid_refresh``).
    debounced_refresh = _debounce(_GRID_DEBOUNCE_S, _refresh_grid)
    m.observe(debounced_refresh, names="bounds")
    m.cancel_grid_refresh = debounced_refresh.cancel
    _refresh_grid()

    m.add(LayersControl(position="topright"))
    return m


__all__ = ["show_shardmap"]
