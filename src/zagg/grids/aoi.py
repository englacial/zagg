"""Optional strict-AOI cell mask (issue #101).

zagg's shard universe is the ``parent_order`` cells that *overlap* the AOI
(``grid.coverage(region)``); every worker then aggregates every child of its
shard with no final clip, so the collected area overhangs the AOI by ~one
shard-cell (the #100 finding). This module computes an optional, default-OFF
per-cell boolean **aligned to the output cell grid** — ``True`` where the cell
falls inside the AOI — that a client uses to filter to a strict-AOI view. It is
"package, don't clip": no observation is dropped, and a flag-OFF run is
byte-identical to today.

The mask is computed at the **shard-map build stage** (``catalog/shardmap.py``)
and carried per shard in the manifest JSON:

- **HEALPix** — a compact MOC of the AOI at ``child_order`` via native morton
  (``morton_coverage_moc``); each worker expands it to a cell-order boolean over
  the shard's ``children()`` with ``moc_to_order`` + membership. No lat/lon-center
  decode.
- **Rectilinear** — a packed boolean per shard from a shapely cell-center
  ``contains`` test after reprojecting the AOI polygon to the grid CRS (the same
  ``to_crs`` path ``coverage`` uses).

The WKB/WKT polygon-input path is deferred (espg/mortie#71) — these helpers take
the same ``[(lats, lons), ...]`` parts contract as ``coverage``.
"""

from __future__ import annotations

import numpy as np

# mortie's MOC coverage API is 1..29 (espg/mortie#59 + #70, shipped in 0.8.2) and
# its public WKB/WKT/geometry cover entry points (``from_wkb`` / ``from_wkt`` /
# ``from_geometry``, espg/mortie#89) ship in 0.8.3. The AOI MOC is built at
# ``child_order`` (the cell resolution), which can exceed the legacy order-18 cap,
# and the WKB/WKT AOI path calls those public entry points — so we assert the
# resolved mortie is >= 0.8.3 rather than silently mis-sizing the mask against an
# 18-capped build or reaching for a WKB/WKT API that isn't there.
MIN_MORTIE_VERSION = "0.8.3"


def _assert_mortie_version() -> None:
    """Fail loudly if the resolved mortie predates the WKB/WKT cover API (0.8.3).

    Against an order-18-capped build ``morton_coverage_moc(..., order=child_order)``
    raises for any ``child_order > 18``, so the AOI mask would silently come back
    empty (the swallowing ``except`` paths elsewhere) or wrongly sized. Assert here
    so a stale environment is a clear error at use, not a quiet bad mask.

    The WKB/WKT AOI path additionally needs the public ``from_wkb`` / ``from_wkt``
    cover entry points (espg/mortie#89), which ship in the same 0.8.3 release, so a
    single ``>= 0.8.3`` gate covers both the MOC cap and the geometry-ingest API.

    Uses PEP 440 ordering (via ``packaging``) so a pre-release like ``0.8.3.devN``
    — which is *before* the 0.8.3 tag, hence missing the public entry points — is
    correctly rejected, not waved through by a digits-only parse.
    """
    import mortie
    from packaging.version import InvalidVersion, Version

    raw = str(getattr(mortie, "__version__", "0"))
    try:
        resolved = Version(raw)
    except InvalidVersion:
        # An unparseable version string can't be proven new enough — refuse rather
        # than silently run the MOC path against a possibly-capped build.
        raise RuntimeError(
            f"aoi_mask requires mortie >= {MIN_MORTIE_VERSION}; could not parse the "
            f"resolved mortie version {raw!r}. Upgrade mortie or disable output.aoi_mask."
        ) from None
    if resolved < Version(MIN_MORTIE_VERSION):
        raise RuntimeError(
            f"aoi_mask requires mortie >= {MIN_MORTIE_VERSION} (the order-29 MOC "
            f"coverage cap, espg/mortie#59 + #70, plus the public WKB/WKT cover "
            f"entry points, espg/mortie#89); resolved {raw}. Upgrade mortie "
            "or disable output.aoi_mask."
        )


# ── HEALPix: native-morton MOC mask ──────────────────────────────────────────


def healpix_aoi_moc(polygon_parts, order: int) -> np.ndarray:
    """Compact multi-order coverage (MOC) of the AOI at ``order``.

    ``polygon_parts`` is the ``[(lats, lons), ...]`` parts list ``coverage`` uses.
    Returns a 1-D ``uint64`` MOC (mixed-order; self-encoding their order) suitable
    for ``moc_to_order``/membership. ``order`` should be the grid's ``child_order``
    so the mask resolves at cell resolution.
    """
    _assert_mortie_version()
    from mortie import morton_coverage_moc

    lats_parts = [np.asarray(p[0], dtype=float) for p in polygon_parts]
    lons_parts = [np.asarray(p[1], dtype=float) for p in polygon_parts]
    moc = np.asarray(morton_coverage_moc(lats_parts, lons_parts, order=order), dtype=np.uint64)
    return moc


def healpix_shard_moc(aoi_moc: np.ndarray, shard_key: int) -> np.ndarray:
    """Sub-MOC of ``aoi_moc`` restricted to one shard (parent morton ``shard_key``).

    Intersects the AOI MOC with the shard's single coarse word (the
    ``parent_order`` morton ``shard_key`` as a one-cell MOC) via ``moc_and``. This
    is the compact per-shard MOC the manifest carries. A shard fully inside the AOI
    collapses to a single coord (the shard word itself — all-ones, expanded on
    read); an edge shard keeps its maximally-compact sub-MOC.

    ``moc_and`` (not ``clip2order`` equality) is required because the AOI MOC is
    *mixed-order*: a coarse interior word (order < ``parent_order``) spans many
    shards, so coarsening MOC words to ``parent_order`` and matching ``shard_key``
    would miss every shard under such a word. Intersecting against the shard word
    keeps coarse interior coverage and trims boundary detail to the shard.
    """
    from mortie import moc_and

    aoi_moc = np.asarray(aoi_moc, dtype=np.uint64)
    if aoi_moc.size == 0:
        return aoi_moc
    shard_word = np.array([int(shard_key)], dtype=np.uint64)
    return np.asarray(moc_and(aoi_moc, shard_word), dtype=np.uint64)


def healpix_mask_for_children(shard_moc: np.ndarray, children, child_order: int) -> np.ndarray:
    """Boolean over ``children`` — ``True`` where the cell is inside the AOI.

    ``shard_moc`` is the per-shard sub-MOC (from :func:`healpix_shard_moc`),
    ``children`` the shard/chunk cell morton ids in canonical (storage) order
    (``grid.children``), ``child_order`` the cell order. Expanding the sub-MOC to a
    flat cell-order set and testing membership of ``children`` IS the mask, already
    in cell order — no cell-center decode.
    """
    from mortie import moc_to_order

    children = np.asarray(children, dtype=np.uint64)
    shard_moc = np.asarray(shard_moc, dtype=np.uint64)
    if shard_moc.size == 0:
        return np.zeros(children.shape, dtype=bool)
    flat = np.asarray(moc_to_order(shard_moc, child_order), dtype=np.uint64)
    return np.isin(children, flat)


# ── Rectilinear: shapely cell-center contains mask ───────────────────────────


def rectilinear_aoi_polygon(polygon_parts, crs):
    """Reproject the AOI ``[(lats, lons), ...]`` parts to ``crs`` as a shapely geom.

    Reuses the WGS84 -> grid-CRS reprojection ``RectilinearGrid.coverage`` does
    (via odc.geo), returning a prepared-friendly shapely geometry in grid CRS for
    the per-cell ``contains`` test.

    The ring is **densified** before reprojection (odc.geo ``to_crs`` resolution
    densification — the same mechanism ``RectilinearGrid.shard_footprint`` uses,
    here with ``resolution="auto"`` for an extent-adaptive vertex spacing), so the
    AOI edges follow the geodesic instead of collapsing to straight chords in a
    polar / large-extent CRS. Since the mask is the *strict* deliverable, this
    keeps edge-cell membership from drifting by the chord-vs-arc deviation. This is
    a rect-only concern: the HEALPix path tessellates the native ``(lats, lons)``
    ring on the sphere (``morton_coverage_moc``) and never reprojects a polygon.
    """
    from odc.geo.geom import multipolygon, polygon

    rings = []
    for lats, lons in polygon_parts:
        rings.append([(float(x), float(y)) for x, y in zip(np.asarray(lons), np.asarray(lats))])
    if len(rings) == 1:
        geom = polygon(rings[0], crs="EPSG:4326")
    else:
        geom = multipolygon([[r] for r in rings], crs="EPSG:4326")
    return geom.to_crs(crs, resolution="auto").geom


def rectilinear_mask_for_centers(aoi_geom, xs, ys) -> np.ndarray:
    """Boolean — ``True`` where ``(xs[i], ys[i])`` (cell centers, grid CRS) is in the AOI.

    ``aoi_geom`` is the reprojected polygon (:func:`rectilinear_aoi_polygon`). Uses
    a prepared geometry so the per-cell point-in-polygon scan stays cheap.
    """
    from shapely import points
    from shapely.prepared import prep

    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    prepared = prep(aoi_geom)
    pts = points(xs, ys)
    return np.fromiter((prepared.contains(p) for p in pts), dtype=bool, count=len(pts))


__all__ = [
    "MIN_MORTIE_VERSION",
    "healpix_aoi_moc",
    "healpix_shard_moc",
    "healpix_mask_for_children",
    "rectilinear_aoi_polygon",
    "rectilinear_mask_for_centers",
]
