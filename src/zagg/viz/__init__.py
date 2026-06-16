"""Shard-map viewer (issue #38).

Two layers:

- :mod:`zagg.viz.shardmap` -- the **headless render core** (phase 1). Pure
  Python, no browser or ipyleaflet import: it turns a :class:`ShardMap` (and an
  optional :class:`~zagg.catalog.sources.Catalog`) into GeoJSON
  ``FeatureCollection`` dicts -- shard/chunk outlines, granule footprints, and
  viewport-clipped cell outlines at the shard order. Fully unit-testable with
  no widget stack installed.
- :func:`show_shardmap` -- the **ipyleaflet wrapper** (phase 2). Builds an
  interactive map from a saved ShardMap (basemap + shard layer + a toggleable
  footprint layer + a zoom-thresholded grid layer). ``ipyleaflet`` is imported
  lazily inside the widget functions so phase-1 core and the test suite never
  require it; it lives in the optional ``viz`` extra (``pip install zagg[viz]``).

Both inputs are reused directly off the existing surface -- ``ShardMap`` /
``Catalog`` round-trips and per-grid ``shard_footprint`` / ``signature`` -- so
there is no viewer-specific file type or second tessellation (issue #38).
"""
from __future__ import annotations

from zagg.viz.shardmap import (
    granule_footprints,
    grid_from_signature,
    render_shardmap,
    shard_outlines,
    viewport_cells,
)


def show_shardmap(shardmap_path, catalog=None, **kwargs):
    """Build an interactive ipyleaflet map for a saved ShardMap (phase 2).

    Thin lazy passthrough to :func:`zagg.viz.leaflet.show_shardmap` so importing
    :mod:`zagg.viz` never pulls in ``ipyleaflet`` -- only calling this does.
    Install the widget stack with ``pip install zagg[viz]``.

    Parameters
    ----------
    shardmap_path : str
        Path to a ``ShardMap`` JSON file (``ShardMap.to_json``).
    catalog : str or Catalog, optional
        A geoparquet path or a loaded ``Catalog`` for the granule-footprint
        layer. When omitted, only the shard layer is drawn.
    **kwargs
        Forwarded to :func:`zagg.viz.leaflet.show_shardmap`.

    Returns
    -------
    ipyleaflet.Map
    """
    from zagg.viz.leaflet import show_shardmap as _show

    return _show(shardmap_path, catalog=catalog, **kwargs)


__all__ = [
    "render_shardmap",
    "shard_outlines",
    "granule_footprints",
    "viewport_cells",
    "grid_from_signature",
    "show_shardmap",
]
