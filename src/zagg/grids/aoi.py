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

The AOI polygon may be supplied either as ``[(lats, lons), ...]`` ring parts (the
``coverage`` contract) or as a native **WKB/WKT geometry** (issue #101): the
HEALPix MOC rides mortie's public ``from_wkb`` / ``from_wkt`` cover entry points
(espg/mortie#89, mortie >= 0.8.3) and the rectilinear path reprojects the
shapely-loaded geometry, so a WKB/WKT AOI yields the *identical* mask to the
equivalent ring. :class:`AOIGeometry` normalizes either form.
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


# ── AOI geometry: (lats, lons) rings OR a WKB/WKT geometry ───────────────────
#
# The AOI may be supplied two ways (issue #101): the original
# ``[(lats, lons), ...]`` exterior-ring parts, or a native geometry as WKB bytes /
# WKT text. WKB/WKT ingest is routed to mortie's public ``from_wkb`` / ``from_wkt``
# cover entry points on the HEALPix side (espg/mortie#89, mortie >= 0.8.3) and to
# shapely's loaders on the rectilinear side, so a WKB/WKT AOI produces *exactly the
# same* cover/mask as passing the equivalent ``(lats, lons)`` ring. ``AOIGeometry``
# normalizes either input to a common carrier the shard-map builder consumes; a
# default-OFF run never constructs one, so flag-off byte-identity is preserved.


class AOIGeometry:
    """Normalized AOI carrier — either ``(lats, lons)`` parts or a WKB/WKT geometry.

    Construct with exactly one source:

    - :meth:`from_parts` — the legacy ``[(lats, lons), ...]`` exterior-ring parts.
    - :meth:`from_wkb` / :meth:`from_wkt` — a native geometry as WKB bytes / WKT
      text (Polygon / MultiPolygon, optionally with holes).

    Both engines read :attr:`parts` for the shard universe (``grid.coverage``); the
    HEALPix MOC and rectilinear-polygon builders additionally take the carrier so a
    WKB/WKT source rides mortie's / shapely's native loaders rather than a lossy
    round-trip through ``parts``.
    """

    __slots__ = ("parts", "wkb", "wkt")

    def __init__(self, *, parts=None, wkb=None, wkt=None):
        self.parts = parts
        self.wkb = wkb
        self.wkt = wkt

    @classmethod
    def from_parts(cls, polygon_parts) -> "AOIGeometry":
        return cls(
            parts=[
                (np.asarray(p[0], dtype=float), np.asarray(p[1], dtype=float))
                for p in polygon_parts
            ]
        )

    @classmethod
    def from_wkb(cls, data) -> "AOIGeometry":
        return cls(wkb=bytes(data), parts=_parts_from_shapely(_shapely_from_wkb(data)))

    @classmethod
    def from_wkt(cls, text) -> "AOIGeometry":
        return cls(wkt=str(text), parts=_parts_from_shapely(_shapely_from_wkt(text)))


def as_aoi_geometry(aoi) -> AOIGeometry:
    """Coerce a user-supplied AOI to an :class:`AOIGeometry`.

    Accepts an existing :class:`AOIGeometry`, WKB ``bytes``, a WKT ``str``, or the
    legacy ``[(lats, lons), ...]`` parts list (the default path). Bytes -> WKB, a
    ``str`` -> WKT; anything else is treated as parts.
    """
    if isinstance(aoi, AOIGeometry):
        return aoi
    if isinstance(aoi, (bytes, bytearray, memoryview)):
        return AOIGeometry.from_wkb(aoi)
    if isinstance(aoi, str):
        return AOIGeometry.from_wkt(aoi)
    return AOIGeometry.from_parts(aoi)


def _shapely_from_wkb(data):
    import shapely

    return shapely.from_wkb(bytes(data))


def _shapely_from_wkt(text):
    import shapely

    return shapely.from_wkt(str(text))


def _parts_from_shapely(geom):
    """Exterior rings of a (Multi)Polygon as ``[(lats, lons), ...]`` parts.

    Mirrors ``zagg.catalog.load_polygon``: one pair per polygon exterior in WGS84
    lat/lon order. Used so a WKB/WKT AOI still feeds ``grid.coverage`` (the shard
    universe) through the same ``(lats, lons)`` contract as a ring AOI.
    """
    gtype = geom.geom_type
    if gtype == "Polygon":
        polys = [geom]
    elif gtype == "MultiPolygon":
        polys = list(geom.geoms)
    else:
        raise ValueError(
            f"AOI geometry must be a Polygon or MultiPolygon (got {gtype!r}); "
            "supply WKB/WKT for a (multi)polygon area of interest."
        )
    parts = []
    for poly in polys:
        coords = np.asarray(poly.exterior.coords, dtype=float)
        parts.append((coords[:, 1], coords[:, 0]))  # shapely is (lon, lat)
    return parts


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


def healpix_aoi_moc_from_geometry(aoi: AOIGeometry, order: int) -> np.ndarray:
    """AOI MOC at ``order`` from an :class:`AOIGeometry` (WKB/WKT or ring parts).

    A WKB/WKT source rides mortie's public ``from_wkb`` / ``from_wkt`` cover entry
    points (espg/mortie#89) with ``moc=True`` — which decompose the geometry and
    route Polygon/MultiPolygon to ``morton_coverage_moc``, so the result is *exactly
    the same* compact MOC as calling :func:`healpix_aoi_moc` on the equivalent
    ``(lats, lons)`` ring (verified bit-for-bit in ``tests/test_aoi_mask.py``). A
    parts source falls back to :func:`healpix_aoi_moc`.
    """
    _assert_mortie_version()
    if aoi.wkb is not None:
        from mortie import from_wkb

        return np.asarray(from_wkb(aoi.wkb, order=order, moc=True), dtype=np.uint64)
    if aoi.wkt is not None:
        from mortie import from_wkt

        return np.asarray(from_wkt(aoi.wkt, order=order, moc=True), dtype=np.uint64)
    return healpix_aoi_moc(aoi.parts, order)


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


def rectilinear_aoi_polygon_from_geometry(aoi: AOIGeometry, crs):
    """Reprojected AOI polygon from an :class:`AOIGeometry` (WKB/WKT or ring parts).

    Routes through the exterior-ring ``parts`` :class:`AOIGeometry` already extracts
    (via shapely for WKB/WKT) into :func:`rectilinear_aoi_polygon`, so a WKB/WKT AOI
    reprojects through the *same* odc.geo densify + ``to_crs`` path — hence the same
    grid-CRS polygon and the same cell-center ``contains`` mask — as the equivalent
    ``(lats, lons)`` ring. Like the ring path, rectilinear is exterior-ring strict
    (holes in a WKB/WKT source are not subtracted, matching the legacy contract;
    the HEALPix engine honors holes natively via ``from_geometry``).
    """
    return rectilinear_aoi_polygon(aoi.parts, crs)


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
    "AOIGeometry",
    "as_aoi_geometry",
    "healpix_aoi_moc",
    "healpix_aoi_moc_from_geometry",
    "healpix_shard_moc",
    "healpix_mask_for_children",
    "rectilinear_aoi_polygon",
    "rectilinear_aoi_polygon_from_geometry",
    "rectilinear_mask_for_centers",
]
