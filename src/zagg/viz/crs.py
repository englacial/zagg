"""CRS selection for the shard-map viewer (issue #38, phase C).

The default ipyleaflet basemap is EPSG:3857 (Web Mercator), which is unusable
above ~85 deg and badly distorts polar regions -- exactly the cryosphere AOIs
zagg exists for. This module picks a projection from a shard map's geographic
extent: NASA polar-stereographic for high-latitude AOIs, Web Mercator otherwise.

- ``EPSG:3031`` -- Antarctic Polar Stereographic, when the map bbox is entirely
  poleward of ~-60 deg latitude.
- ``EPSG:3413`` -- NSIDC Sea Ice Polar Stereographic North (Arctic), when the
  bbox is entirely poleward of ~+60 deg.
- ``EPSG:3857`` -- Web Mercator, the mid-latitude / global fallback.

For the two polar cases the matching proj4leaflet projection definition (proj4
string, origin, resolutions, bounds) and a NASA **GIBS** WMTS basemap URL are
provided so :mod:`zagg.viz.leaflet` can wire them onto an ipyleaflet ``Map``.
The vector layers stay WGS84 GeoJSON -- proj4leaflet reprojects them
client-side -- so the headless render core is unchanged.

Everything here is pure Python (no ipyleaflet): the bbox helper and the picker
are unit-testable with just the core deps.
"""
from __future__ import annotations

from zagg.viz.shardmap import grid_from_signature

# Latitude (deg) past which an AOI is treated as polar. A bbox whose nearer
# edge to the pole is beyond this cutoff selects a polar-stereographic CRS.
_POLAR_LAT = 60.0

# proj4leaflet projection definitions for the two NASA polar-stereographic
# grids. ``resolutions`` and ``bounds`` follow the GIBS tile matrix sets so the
# WMTS basemaps below line up (the published EPSG:3413 / EPSG:3031 GIBS tile
# pyramids, 8192 m down to 256 m per pixel over a +-4194304 m extent).
_PROJ_3413 = {
    "name": "EPSG:3413",
    "proj4def": (
        "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 "
        "+a=6378137 +b=6356752.3142 +units=m +no_defs"
    ),
    "origin": [-4194304, 4194304],
    "bounds": [[-4194304, -4194304], [4194304, 4194304]],
    "resolutions": [8192.0, 4096.0, 2048.0, 1024.0, 512.0, 256.0],
}

_PROJ_3031 = {
    "name": "EPSG:3031",
    "proj4def": (
        "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 "
        "+a=6378137 +b=6356752.3142 +units=m +no_defs"
    ),
    "origin": [-4194304, 4194304],
    "bounds": [[-4194304, -4194304], [4194304, 4194304]],
    "resolutions": [8192.0, 4096.0, 2048.0, 1024.0, 512.0, 256.0],
}

# NASA GIBS WMTS basemap (BlueMarble shaded relief, a static all-time layer) per
# polar tile matrix set. ``{z}/{y}/{x}`` is filled in by the leaflet TileLayer.
_GIBS_BASE = (
    "https://gibs.earthdata.nasa.gov/wmts/epsg{epsg}/best/"
    "BlueMarble_ShadedRelief_Bathymetry/default/{matrix}/{{z}}/{{y}}/{{x}}.jpeg"
)

_GIBS_3413 = {
    "url": _GIBS_BASE.format(epsg="3413", matrix="500m"),
    "attribution": "NASA EOSDIS GIBS",
    "name": "GIBS BlueMarble (Arctic)",
}
_GIBS_3031 = {
    "url": _GIBS_BASE.format(epsg="3031", matrix="500m"),
    "attribution": "NASA EOSDIS GIBS",
    "name": "GIBS BlueMarble (Antarctic)",
}

# What each selectable CRS carries: the proj4leaflet definition (None for Web
# Mercator, which ipyleaflet ships natively) and the GIBS basemap (None -> the
# caller's Mercator default).
_CRS_INFO = {
    "EPSG:3413": {"projection": _PROJ_3413, "basemap": _GIBS_3413},
    "EPSG:3031": {"projection": _PROJ_3031, "basemap": _GIBS_3031},
    "EPSG:3857": {"projection": None, "basemap": None},
}


def shardmap_bbox(shardmap) -> tuple[float, float, float, float]:
    """WGS84 ``(lon_min, lat_min, lon_max, lat_max)`` of a shard map's footprints.

    Computed from the union of the shard footprints (rebuilt from the map's own
    ``grid_signature``), so it reflects exactly the area the viewer will draw.

    Raises
    ------
    ValueError
        If the map has no shards.
    """
    grid = grid_from_signature(shardmap.grid_signature)
    keys = list(shardmap.shard_keys)
    if not keys:
        raise ValueError("shard map has no shards")
    lon_min = lat_min = float("inf")
    lon_max = lat_max = float("-inf")
    for key in keys:
        x0, y0, x1, y1 = grid.shard_footprint(key).bounds
        lon_min, lat_min = min(lon_min, x0), min(lat_min, y0)
        lon_max, lat_max = max(lon_max, x1), max(lat_max, y1)
    return (lon_min, lat_min, lon_max, lat_max)


def pick_crs(shardmap, override=None) -> str:
    """Select an EPSG CRS for displaying ``shardmap``.

    Returns ``EPSG:3031`` when the map bbox is entirely poleward of ~-60 deg,
    ``EPSG:3413`` when entirely poleward of ~+60 deg, else ``EPSG:3857`` (Web
    Mercator). An explicit ``override`` (one of those three) is returned as-is.

    Parameters
    ----------
    shardmap : ShardMap
        A built or loaded shard map.
    override : str, optional
        Force a CRS. Must be one of ``"EPSG:3031"``, ``"EPSG:3413"``,
        ``"EPSG:3857"``.

    Returns
    -------
    str
        One of the three EPSG codes above.

    Raises
    ------
    ValueError
        If ``override`` is given but not a supported CRS.
    """
    if override is not None:
        if override not in _CRS_INFO:
            raise ValueError(
                f"unsupported crs override {override!r}; "
                f"expected one of {sorted(_CRS_INFO)}"
            )
        return override

    _lon_min, lat_min, _lon_max, lat_max = shardmap_bbox(shardmap)
    if lat_max <= -_POLAR_LAT:
        return "EPSG:3031"
    if lat_min >= _POLAR_LAT:
        return "EPSG:3413"
    return "EPSG:3857"


def crs_info(crs: str) -> dict:
    """proj4leaflet ``projection`` and GIBS ``basemap`` dicts for an EPSG ``crs``.

    Returns ``{"projection": dict | None, "basemap": dict | None}``. ``None``
    values mean "use ipyleaflet's native Web Mercator / the caller's default
    basemap" -- i.e. the EPSG:3857 case.

    Raises
    ------
    ValueError
        If ``crs`` is not a supported code.
    """
    if crs not in _CRS_INFO:
        raise ValueError(
            f"unsupported crs {crs!r}; expected one of {sorted(_CRS_INFO)}"
        )
    return _CRS_INFO[crs]


def is_polar(crs: str) -> bool:
    """True for a polar-stereographic CRS (no +-180 antimeridian seam)."""
    return crs in ("EPSG:3413", "EPSG:3031")


__all__ = ["shardmap_bbox", "pick_crs", "crs_info", "is_polar"]
