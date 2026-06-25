"""Decompose an ICESat-2 CMR swath polygon into per-beam-pair ground-track corridors.

The CMR/STAC footprint of an ATL03/ATL06 granule is a coarse ~12 km-wide
quarter-orbit *swath envelope*, while the data is six pencil-thin beams in three
pairs spanning only ~6.5 km, with ~3 km empty gaps between pairs. Intersecting a
shard map against the full envelope over-assigns granules to shards their beams
never touch (issue #65). ATLAS beams are not steerable -- off-pointing moves the
whole platform -- so the three pairs keep a fixed cross-track spacing. Combined
with the fact that the swath polygon's *centerline* tracks the real data center
to ~hundreds of metres, we can recover the centerline, place the three pair
corridors at fixed offsets, and intersect those instead.

``beam_tracks_from_cmr_polygon`` returns one thin closed corridor ring (as
``(lats, lons)`` arrays) per beam pair. The representation is deliberately the
same exterior-ring form the swath polygon already uses, so both shard-map
backends (spherely S2 polygons and mortie ``morton_coverage``) consume it
unchanged.

.. deprecated::
    This whole module is a stopgap geometric approximation of per-beam coverage.
    Remove it once a better fix lands -- native per-beam CMR geometry, the
    memory-handling robustness in #66 (so reading the full swath is affordable),
    or data virtualization tracked in #97. The ``footprint="beams"`` surface in
    :mod:`zagg.catalog.shardmap` carries the same marker.
"""

# DEPRECATED -- remove this module when a better fix lands: native per-beam CMR
# geometry, OR the memory-handling robustness in #66, OR data virtualization
# tracked in #97. Marked per the deprecation direction on PR #67.

from __future__ import annotations

import numpy as np

# ICESat-2 ATLAS beam geometry. The six beams form three pairs; the pairs are
# ~3.3 km apart cross-track (the strong/weak beams within a pair are only ~90 m
# apart, far below the corridor half-width, so one corridor per pair covers both).
# gt2 sits ~on the reference ground track (centerline); gt1/gt3 straddle it.
_ATL03_PAIR_OFFSETS_M: tuple[float, ...] = (-3300.0, 0.0, 3300.0)  # gt1, gt2, gt3

# Corridor half-width: envelopes the ~90 m within-pair separation plus the
# ~260 m centerline-recovery error, with margin -- while staying well inside the
# ~1.5 km half-gap between pairs so the inter-pair gaps remain unassigned.
_BEAM_CORRIDOR_HALF_WIDTH_M: float = 500.0

# Products this applies to (same ATLAS platform/beam geometry).
_BEAM_PRODUCTS: frozenset[str] = frozenset({"ATL03", "ATL06"})

# Minimum exterior-ring vertices to attempt a decomposition; below this we
# cannot reliably separate the two swath edges, so we fall back to the swath.
_MIN_VERTS: int = 6

# Expected ATL03/06 swath envelope half-width (the CMR polygon for a quarter
# orbit is ~12.6 km across, so ~6.3 km on either side of the centerline). When
# the recovered envelope is asymmetric or anomalously padded, the centerline of
# the envelope drifts from the true data center; we widen the corridor by half
# the excess so the beams remain covered (option (b) of the review on issue #65).
_EXPECTED_SWATH_HALF_WIDTH_M: float = 6300.0


def is_beam_product(product: str | None) -> bool:
    """True for ICESat-2 products whose CMR swath decomposes into beam corridors.

    .. deprecated::
        Part of the stopgap beam-corridor mechanism; remove with the rest of
        this module once native per-beam CMR geometry, #66, or #97 lands.
    """
    if not product:
        return False
    return product.upper() in _BEAM_PRODUCTS


def _wgs84_geod():
    from pyproj import Geod

    return Geod(ellps="WGS84")


def _swath_fallback(lats: np.ndarray, lons: np.ndarray) -> list[tuple[np.ndarray, np.ndarray]]:
    """Return the original ring unchanged (graceful degrade to swath behavior)."""
    return [(np.asarray(lats, dtype=float), np.asarray(lons, dtype=float))]


def _split_swath_edges(lats: np.ndarray, lons: np.ndarray):
    """Split a swath ring into its two long along-track edges.

    Projects the (unclosed) vertices onto their principal axis, then walks the
    ring between the two extreme along-track vertices to recover the two edges.
    Following the ring's connectivity (rather than a linear cross-track sign)
    keeps the two edges separated even when the swath curves over a long arc.
    Returns ``(edgeA, edgeB)`` as ``(t, lat, lon)`` tuples ordered by the
    along-track coordinate ``t``, or ``None`` if the ring cannot be split.
    """
    n = len(lats)
    if n < _MIN_VERTS:
        return None
    # Center and find the principal (along-track) axis. Scale longitude by
    # cos(lat) so the projection is distance-meaningful, not skewed by the
    # degree anisotropy at high latitude.
    lat0 = float(np.mean(lats))
    x = (lons - np.mean(lons)) * np.cos(np.radians(lat0))
    y = lats - lat0
    pts = np.column_stack([x, y])
    _, _, vh = np.linalg.svd(pts, full_matrices=False)
    t = pts @ vh[0]
    i_lo, i_hi = int(np.argmin(t)), int(np.argmax(t))
    if i_lo == i_hi:
        return None

    # The two arcs between the extreme-t vertices are the two long edges. Each
    # extreme vertex anchors both edges; neither edge re-includes the other's
    # anchor (that would drag a far-side vertex into the wrong edge -> a biased
    # centerline at the ring ends).
    idx = np.arange(n)
    fwd = np.concatenate([idx[i_lo:], idx[:i_lo]])  # ring order starting at i_lo
    pos_hi = int(np.where(fwd == i_hi)[0][0])
    a_idx = fwd[: pos_hi + 1]  # i_lo .. i_hi
    b_idx = fwd[pos_hi:]  # i_hi .. (wrap) .. just before i_lo
    if len(a_idx) < 2 or len(b_idx) < 2:
        return None

    def edge(ix):
        e_t, e_lat, e_lon = t[ix], lats[ix], lons[ix]
        order = np.argsort(e_t)
        return e_t[order], e_lat[order], e_lon[order]

    return edge(a_idx), edge(b_idx)


def _centerline(lats: np.ndarray, lons: np.ndarray, geod, n_samples: int = 200):
    """Recover the swath centerline + measured envelope half-width.

    Returns ``(clat, clon, half_width_m)`` ordered south->north, or ``None`` on
    a degenerate split. ``half_width_m`` is the larger of the median geodesic
    distances from the centerline to each envelope edge -- the asymmetric side
    is the one the corridor must widen toward downstream.

    The width threshold drops samples in the converging end caps where the two
    edges meet at a shared corner. Short interior gaps left by a mid-track
    pinch are bridged implicitly by the geod azimuth between adjacent kept
    samples -- a known limitation for severely-pinched CMR envelopes, which
    would yield an angled chord across the pinch rather than a faithful
    centerline.
    """
    split = _split_swath_edges(lats, lons)
    if split is None:
        return None
    (ta, la, na), (tb, lb, nb) = split
    t0 = max(ta[0], tb[0])
    t1 = min(ta[-1], tb[-1])
    if not (t1 > t0):
        return None
    ts = np.linspace(t0, t1, n_samples)
    lon_a, lon_b = np.interp(ts, ta, na), np.interp(ts, tb, nb)
    lat_a, lat_b = np.interp(ts, ta, la), np.interp(ts, tb, lb)
    clat = 0.5 * (lat_a + lat_b)
    clon = 0.5 * (lon_a + lon_b)
    # Near the end caps the two edges converge to a shared corner, which would
    # drag the centerline sideways; keep only where the swath has real width.
    width = np.abs((lon_a - lon_b) * np.cos(np.radians(clat)))
    keep = width >= 0.3 * np.median(width)
    if keep.sum() < 2:
        return None
    clat, clon = clat[keep], clon[keep]
    lon_a, lat_a, lon_b, lat_b = lon_a[keep], lat_a[keep], lon_b[keep], lat_b[keep]
    # Median geodesic distance from the centerline to each edge. Take the max
    # of the two so an asymmetric envelope (one edge padded farther than the
    # other) reports its true outer reach -- the side the centerline drifted
    # away from is the side we need to widen the corridor toward.
    _, _, d_a = geod.inv(clon, clat, lon_a, lat_a)
    _, _, d_b = geod.inv(clon, clat, lon_b, lat_b)
    half_width_m = float(max(np.median(d_a), np.median(d_b)))
    # Order south -> north so the three corridors land on the three true beam
    # positions as a *set* for both ascending and descending granules (the
    # backends union all per-granule rings, so per-pair west/east labelling is
    # not preserved across heading sign).
    if clat[0] > clat[-1]:
        clat, clon = clat[::-1], clon[::-1]
    return clat, clon, half_width_m


def _offset_point(geod, lon: float, lat: float, az_track: float, cross_m: float):
    """Point ``cross_m`` metres cross-track from (lon, lat); +right, -left of heading."""
    perp = (az_track + 90.0) if cross_m >= 0 else (az_track - 90.0)
    lon2, lat2, _ = geod.fwd(lon, lat, perp % 360.0, abs(cross_m))
    return lat2, lon2


def beam_tracks_from_cmr_polygon(
    lats,
    lons,
    product: str = "ATL03",
    *,
    pair_offsets_m: tuple[float, ...] = _ATL03_PAIR_OFFSETS_M,
    half_width_m: float = _BEAM_CORRIDOR_HALF_WIDTH_M,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Decompose a CMR swath polygon into per-beam-pair corridor rings.

    .. deprecated::
        This is a stopgap geometric approximation (see the module docstring).
        Remove it once a better fix lands -- native per-beam CMR geometry, the
        memory-handling robustness in #66, or data virtualization tracked in #97.

    Parameters
    ----------
    lats, lons : array-like
        Exterior-ring coordinates (WGS84) of the granule's CMR footprint swath.
    product : str
        Product short name. Only ICESat-2 beam products (``ATL03``/``ATL06``)
        are decomposed; anything else returns the swath ring unchanged.
    pair_offsets_m : tuple of float
        Cross-track offsets (metres) of the beam pairs from the centerline.
        Negative = west/left of the south->north heading, positive = east/right.
    half_width_m : float
        Half-width of each corridor (metres).

    Returns
    -------
    list of (lats, lons)
        One closed corridor ring per beam pair. Falls back to a single-element
        list holding the original swath ring for non-beam products or when the
        centerline cannot be recovered (a granule is never dropped).
    """
    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    if not is_beam_product(product) or len(lats) < _MIN_VERTS:
        return _swath_fallback(lats, lons)

    # Fall back on swaths whose ring has a >=180 deg consecutive-vertex
    # longitude jump. This catches the common antimeridian wrap (e.g.
    # 179.9 -> -179.9 = ~360 deg jump) and the rarer true polar-cap polygon
    # whose vertices straddle the pole at opposite longitudes (a near-180 deg
    # leap, no actual seam crossing). The plain raw-lon-span (np.ptp) form
    # used previously falsely tripped on wide-lon polar quarter-orbits with no
    # seam at all -- the consecutive-vertex form keeps those passing through.
    if len(lons) >= 2 and float(np.max(np.abs(np.diff(lons)))) >= 180.0:
        return _swath_fallback(lats, lons)

    # Drop the closing duplicate vertex, if any.
    if lats[0] == lats[-1] and lons[0] == lons[-1]:
        lats, lons = lats[:-1], lons[:-1]

    geod = _wgs84_geod()
    center = _centerline(lats, lons, geod)
    if center is None:
        return _swath_fallback(lats, lons)
    clat, clon, measured_half_width_m = center

    # Forward azimuth along the centerline; repeat the last for the final point.
    az12, _, _ = geod.inv(clon[:-1], clat[:-1], clon[1:], clat[1:])
    az = np.concatenate([az12, az12[-1:]])

    # Adaptive half-width. The CMR envelope of a quarter-orbit ATL03/06 swath
    # is ~12.6 km wide (half-width ~6.3 km); a wider-than-expected envelope
    # means CMR has padded one or both sides, and the recovered centerline
    # can drift by up to half the excess. Widen the corridor by exactly that
    # excess so the beams remain covered. Note this does NOT handle a
    # *placement-shifted* envelope of normal width (e.g. CMR padding shifted 4
    # km west / 8 km east while keeping the total ~12 km wide): from the
    # polygon shape alone the centerline-of-envelope is indistinguishable
    # from a symmetric centred envelope, so no widening fires and the beams
    # can fall outside their corridors. That case is documented in
    # ``_centerline``'s docstring and the PR description.
    extra = max(0.0, measured_half_width_m - _EXPECTED_SWATH_HALF_WIDTH_M)
    half_width_eff = half_width_m + extra

    rings: list[tuple[np.ndarray, np.ndarray]] = []
    for off in pair_offsets_m:
        lo, hi = off - half_width_eff, off + half_width_eff
        lo_lat, lo_lon, hi_lat, hi_lon = [], [], [], []
        for la, lo_, a in zip(clat, clon, az):
            p_lat, p_lon = _offset_point(geod, lo_, la, a, lo)
            lo_lat.append(p_lat)
            lo_lon.append(p_lon)
            p_lat, p_lon = _offset_point(geod, lo_, la, a, hi)
            hi_lat.append(p_lat)
            hi_lon.append(p_lon)
        ring_lat = np.array(lo_lat + hi_lat[::-1] + lo_lat[:1])
        ring_lon = np.array(lo_lon + hi_lon[::-1] + lo_lon[:1])
        rings.append((ring_lat, ring_lon))
    return rings
