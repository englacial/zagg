"""ipyleaflet wrapper for the shard-map viewer (issue #38, phase 2).

Builds an interactive map from a saved ShardMap: a basemap, the shard-outline
layer, an optional (toggleable) granule-footprint layer, and a grid layer that
draws shard-order cell outlines only when the viewport is zoomed in far enough
to show ``<= max_shards`` shards (the "grid-on-zoom" gate -- never a global
graticule, issue #38).

All ``ipyleaflet`` imports are local to the functions here so importing
:mod:`zagg.viz` (and the phase-1 render core / test suite) never requires the
widget stack. Install it with ``pip install zagg[viz]``.

The geometry is produced entirely by the headless core in
:mod:`zagg.viz.shardmap` -- this module only wires those GeoJSON collections
onto ipyleaflet layers and keeps the grid layer in sync with the viewport.
"""
from __future__ import annotations

from zagg.viz.shardmap import (
    _load_catalog,
    _load_shardmap,
    granule_footprints,
    shard_outlines,
    viewport_cells,
)

# Layer styles (kept terse; tweakable by callers via the returned Map).
_SHARD_STYLE = {"color": "#1f78b4", "weight": 1, "fillOpacity": 0.05}
_FOOTPRINT_STYLE = {"color": "#e31a1c", "weight": 1, "fillOpacity": 0.10}
_GRID_STYLE = {"color": "#333333", "weight": 1, "fillOpacity": 0.0}


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


def show_shardmap(
    shardmap_path,
    catalog=None,
    *,
    max_shards: int = 4,
    zoom: int = 3,
    basemap=None,
):
    """Build an interactive ipyleaflet map for a saved ShardMap.

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
        Overrides the default OpenStreetMap basemap.

    Returns
    -------
    ipyleaflet.Map
        Map with a shard layer, optional footprint layer, a zoom-thresholded
        grid layer, and a ``LayersControl`` for toggling layers.
    """
    # Import first so a missing `viz` extra fails clearly, before any work.
    from ipyleaflet import GeoJSON, LayersControl, Map, basemaps

    shardmap = _load_shardmap(shardmap_path)
    shards_fc = shard_outlines(shardmap)

    center = _center_zoom(shards_fc)
    m = Map(basemap=basemap or basemaps.OpenStreetMap.Mapnik, center=center, zoom=zoom)

    shard_layer = GeoJSON(data=shards_fc, style=_SHARD_STYLE, name="shards")
    m.add(shard_layer)

    if catalog is not None:
        footprint_fc = granule_footprints(_load_catalog(catalog))
        footprint_layer = GeoJSON(
            data=footprint_fc, style=_FOOTPRINT_STYLE, name="granule footprints"
        )
        m.add(footprint_layer)

    # Grid-on-zoom: an empty layer kept in sync with the viewport. Recomputed on
    # every bounds change; the headless core's gate returns nothing when too
    # many shards are visible, so this never becomes a global graticule.
    grid_layer = GeoJSON(
        data={"type": "FeatureCollection", "features": []},
        style=_GRID_STYLE,
        name="grid (shard cells)",
    )
    m.add(grid_layer)

    def _refresh_grid(event=None):  # noqa: ARG001 (traitlets observe signature)
        bounds = m.bounds
        if not bounds:
            return
        (south, west), (north, east) = bounds
        grid_layer.data = viewport_cells(
            shardmap, (west, south, east, north), max_shards=max_shards
        )

    m.observe(_refresh_grid, names="bounds")
    _refresh_grid()

    m.add(LayersControl(position="topright"))
    return m


__all__ = ["show_shardmap"]
