"""Shard-map builder: ``Catalog`` + grid -> ``ShardMap`` manifest.

This is concern (2) of the #24 split -- take fetched granule metadata plus a
grid spec and produce the work-distribution manifest the runner dispatches.
It is independent of the fetch (concern 1): the same ``Catalog`` can build many
ShardMaps at different grids.

The ``ShardMap`` is a small, self-contained JSON plan (option C): each granule
is recorded with **both** its S3 and HTTPS hrefs so the runner can pick the
endpoint at dispatch time via ``data_source.driver`` -- the map itself stays
endpoint-neutral and never needs the Catalog at run time. It also records the
grid ``spatial_signature()`` (the spatial layout only, no aggregation fields;
#89) so a run can refuse a map built for a different *spatial* grid while still
reusing one map across configs that differ only in what they aggregate.

Geometry backends (all sphere-correct):

- ``spherely`` -- exact S2 intersection. Uses ``SpatialIndex`` (build once,
  query per shard) when the spatial-index build of spherely is present, else
  falls back to elementwise ``spherely.intersects`` -- a brute
  O(granules x shards) path that is still sphere-correct (no fork needed).
- ``mortie``   -- HEALPix MOC intersection (``morton_coverage_moc``); a tiny
  ~0.01% polar omission vs S2 (espg/mortie#32), no extra deps.

shapely is no longer an intersection backend -- its WGS84 STRtree path had
antimeridian/near-pole correctness bugs (#36). shapely remains a dependency for
WKB decode (``sources.py``) and footprint geometry (``grids/``).
"""

from __future__ import annotations

import importlib
import json
import time
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np

# Upper bound on the MOC order mortie's ``morton_coverage`` /
# ``morton_coverage_moc`` accept; a higher order raises inside mortie. The
# derived order is clamped to this so an exotic ``chunk_order`` can't push the MOC
# order past the cap and silently lose coverage (#92).
MORTIE_MOC_ORDER_CAP = 18

# ── granule footprint helpers ────────────────────────────────────────────────


def _granule_entry(rec: dict) -> dict:
    """Self-contained per-shard granule payload (option C).

    The canonical single-asset trio is always present; multi-asset records
    (raster sources, #218) additionally carry ``assets`` (per-band hrefs) and
    ``datetime`` (ISO acquisition time). ``time_start``/``time_end`` (issue
    #246) are the granule's ISO acquisition range on any record whose catalog
    carries STAC ``start_datetime``/``end_datetime`` — the metadata the
    dispatcher uses to subset granules per time window; absent on maps built
    from pre-#246 catalogs (the fan-out then degrades conservatively).
    """
    entry = {"id": rec["id"], "s3": rec["s3"], "https": rec["https"]}
    for key in ("assets", "datetime", "time_key", "time_start", "time_end"):
        if rec.get(key) is not None:
            entry[key] = rec[key]
    return entry


def _to_spherely_polygon(lats, lons):
    """Build a closed sphere-aware polygon, or None on validation failure.

    Uses spherely's ``oriented=False`` mode, which tries both vertex orderings
    and keeps the smaller-area interpretation -- the correct path for
    ICESat-2 polygons whose lat/lon vertices, read as geodesic edges, would
    otherwise self-intersect near the pole.
    """
    import spherely

    lats = np.asarray(lats, dtype=float)
    lons = np.asarray(lons, dtype=float)
    if lats[0] != lats[-1] or lons[0] != lons[-1]:
        lats = np.concatenate([lats, lats[:1]])
        lons = np.concatenate([lons, lons[:1]])
    try:
        return spherely.create_polygon(shell=list(zip(lons, lats)), oriented=False)
    except (ValueError, RuntimeError):
        return None


def _granule_footprints(rec, footprint, product):
    """Return ``[(lats, lons), ...]`` rings for one granule under ``footprint``.

    ``"swath"`` yields the single CMR footprint ring (current behavior).
    ``"beams"`` yields one thin corridor ring per beam pair via
    :func:`zagg.catalog.beams.beam_tracks_from_cmr_polygon` (issue #65). Both
    backends consume the rings identically -- spherely as polygons, mortie as
    ``morton_coverage`` point sequences -- so the per-beam path needs no
    backend-specific geometry.

    .. deprecated::
        The ``"beams"`` corridor path is a stopgap (see ``beams.py``); remove it
        once native per-beam CMR geometry, the memory-handling robustness in #66,
        or data virtualization (#97) lands.
    """
    if footprint == "beams":
        from zagg.catalog.beams import beam_tracks_from_cmr_polygon

        return beam_tracks_from_cmr_polygon(rec["lats"], rec["lons"], product=product)
    return [(rec["lats"], rec["lons"])]


def _resolve_mortie_order(mortie_order, grid) -> int:
    """Choose the MOC order for the mortie backend.

    The MOC order must be **>= the shard order** (``parent_order``). A coarser
    MOC upsamples in ``moc_to_order(moc, parent_order)``: every coarse cell
    becomes all ``4^(parent_order - order)`` order-``parent_order`` descendants,
    fattening a thin granule track to fill every shard under that cell. The old
    fixed default of 8 against ``parent_order=13`` expanded each cell to 1024
    shards, putting ~every granule in ~every shard and OOMing the workers (#92).

    ``None`` (the default) pins the order to the grid's **inner-chunk order**
    (``grid.chunk_order``) -- the Zarr-chunk order between the shard order
    (``parent_order``) and the leaf (``child_order``), set by ``chunk_inner`` and
    defaulting to ``parent_order`` when unset (so chunk == shard). The shipped
    ATL03 HEALPix configs use ``chunk_inner=13`` (parent 11, child 19), so the
    order resolves to 13. Keying the MOC to the chunk order matches the unit work
    is dispatched at: footprints resolve no finer than the chunk the worker reads,
    which is enough to keep ``moc_to_order`` from upsampling onto neighbor shards
    (#92) at near-minimal compute -- the order-sweep benchmark
    (``benchmarks/mortie_order_sweep.py``) shows granules/shard flat for every
    order >= ``parent_order`` while wall-time grows with order, so a finer MOC
    buys precision the order-``parent_order`` shard cells can't see.
    The order is still clamped to ``MORTIE_MOC_ORDER_CAP`` (mortie's order-18
    coverage cap) before the ``parent_order`` guard, so an exotic ``chunk_order``
    past the cap can't make mortie raise into the swallowing ``except`` (silent
    coverage loss). The clamp comes *before* the guard, so a ``parent_order``
    itself above the cap (the clamp then lands at 18 < ``parent_order``) still
    trips the raise rather than passing an order coarser than the shards. An
    explicit ``mortie_order`` is honored but still validated against
    ``parent_order``. Non-HEALPix grids (no ``parent_order`` / ``child_order``)
    keep the legacy default of 8.
    """
    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    if mortie_order is not None:
        order = int(mortie_order)
    elif is_healpix:
        # ``chunk_order`` is the inner-chunk order on HealpixGrid (always set;
        # == parent_order when chunk_inner is unset). The getattr default only
        # covers a duck-typed grid that exposes parent/child but not chunk_order.
        chunk_order = getattr(grid, "chunk_order", grid.parent_order)
        order = min(int(chunk_order), MORTIE_MOC_ORDER_CAP)
    else:
        order = 8
    if is_healpix and order < grid.parent_order:
        raise ValueError(
            f"mortie MOC order {order} is coarser than the grid's parent_order "
            f"{grid.parent_order}; this upsamples every granule footprint onto all "
            f"shards under each MOC cell (#92). Use order >= {grid.parent_order}."
        )
    return order


def _resolve_backend(backend: str, grid) -> str:
    """Resolve ``"auto"`` to a concrete, grid-appropriate backend.

    Prefers exact S2 via ``spherely`` whenever it imports -- using its
    ``SpatialIndex`` when present and elementwise ``spherely.intersects``
    (a brute path) otherwise, both sphere-correct. When spherely is absent,
    HEALPix grids use the native **mortie** MOC path (its order matches the
    grid); non-HEALPix grids have no spherely-free path, so ``build`` raises
    with an install pointer (#36).
    """
    if backend != "auto":
        return backend
    if _spherely_available():
        return "spherely"
    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    return "mortie" if is_healpix else "spherely"


def _spherely_available() -> bool:
    """True if ``import spherely`` succeeds (any build, fork or stock)."""
    try:
        importlib.import_module("spherely")
    except ImportError:
        return False
    return True


def _region_parts(region, metadata) -> list:
    """Resolve a coverage region to ``[(lats, lons), ...]`` polygon parts.

    ``region`` may be the parts list directly, or ``None`` to fall back to the
    catalog's bbox rectangle.
    """
    if region is not None:
        return region
    bbox = (metadata or {}).get("bbox")
    if not bbox:
        raise ValueError("no region given and catalog metadata has no bbox")
    x0, y0, x1, y1 = bbox
    return [(np.array([y0, y0, y1, y1, y0]), np.array([x0, x1, x1, x0, x0]))]


def _compute_aoi_mask(grid, aoi, shard_keys) -> list:
    """Per-shard strict-AOI mask payload (issue #101), parallel to ``shard_keys``.

    ``aoi`` is an :class:`~zagg.grids.aoi.AOIGeometry` (WKB/WKT geometry or ``(lats,
    lons)`` ring parts). HEALPix: each entry is the shard's compact sub-MOC of the
    AOI (``uint64`` words as ints). Rectilinear: each entry is the True-cell indices
    into the shard's ``children`` order (cell centers inside the reprojected AOI).
    The worker expands the entry to a per-cell bool over ``children(shard_key)`` at
    write time.

    Computed once here (the shard-map stage) so the local worker expands it with
    no region plumbing — the mask depends only on (grid, AOI), never on
    observations. Dispatches on the same HEALPix predicate the rest of this module
    uses (``parent_order`` + ``child_order``), then branches to the native morton
    ``aoi_moc`` path vs the rectilinear shapely-center ``aoi_polygon`` path (each
    consuming the same ``aoi`` carrier, so a WKB/WKT AOI yields the identical mask
    to the equivalent ring). A grid that is neither (no AOI API) with the flag on is
    a misconfiguration, raised here rather than left to a cryptic ``AttributeError``
    downstream.
    """
    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    if is_healpix:
        aoi_moc = grid.aoi_moc(aoi)
        return [[int(w) for w in grid.aoi_shard_moc(aoi_moc, int(k))] for k in shard_keys]
    if hasattr(grid, "aoi_polygon"):
        # Rectilinear (or any center-test grid): the in-AOI cell ids per shard.
        # Storing cell IDS (not positional indices) keeps the worker expansion
        # order-independent, so a K>1 chunk that enumerates a sub-tile still maps
        # correctly via membership.
        aoi_geom = grid.aoi_polygon(aoi)
        out = []
        for k in shard_keys:
            children = np.asarray(grid.children(int(k)))
            mask = grid.aoi_mask_for_children(aoi_geom, children)
            out.append([int(c) for c in children[mask]])
        return out
    raise ValueError(
        f"output.aoi_mask is on but grid {type(grid).__name__} provides no AOI mask "
        "API (aoi_moc / aoi_polygon); disable output.aoi_mask for this grid."
    )


# ── backends (operate on granule records) ────────────────────────────────────

_SPHERELY_INSTALL_HINT = (
    "spherely is required for the 'spherely' intersection backend. Install it "
    "(see the zagg README -- the exact-S2 SpatialIndex build is a fork not on "
    "PyPI; the stock build also works via a slower brute path), or use a "
    "HEALPix grid with backend='mortie'."
)


def _intersect_spherely(
    records, grid, all_shards, footprint="swath", product="ATL03"
) -> Dict[int, List[int]]:
    """Exact S2 intersection via spherely.

    Builds sphere-aware polygons for each granule footprint, then maps each
    shard to the granules it intersects. Uses ``spherely.SpatialIndex`` (build
    once, query per shard) when present; otherwise falls back to elementwise
    ``spherely.intersects`` -- still sphere-correct, but a brute
    O(granules x shards) scan with no tree prefilter (#36).

    ``footprint="beams"`` decomposes each granule into per-beam-pair corridor
    rings (issue #65); a granule is assigned to a shard if any of its rings
    intersect it (deduped, order preserved).
    """
    try:
        import spherely
    except ImportError as exc:
        raise ImportError(_SPHERELY_INSTALL_HINT) from exc

    polys, idx = [], []
    for i, rec in enumerate(records):
        for rlats, rlons in _granule_footprints(rec, footprint, product):
            poly = _to_spherely_polygon(rlats, rlons)
            if poly is not None:
                polys.append(poly)
                idx.append(i)
    if not polys:
        return {}
    poly_arr = np.asarray(polys)
    has_index = hasattr(spherely, "SpatialIndex")
    tree = spherely.SpatialIndex(poly_arr) if has_index else None

    out: Dict[int, List[int]] = {}
    for shard in all_shards:
        fp = grid.shard_footprint(shard)
        sx, sy = fp.exterior.coords.xy
        s_poly = _to_spherely_polygon(np.asarray(sy), np.asarray(sx))
        if s_poly is None:
            continue
        if tree is not None:
            hits = tree.query(s_poly, predicate="intersects")
        else:
            hits = np.flatnonzero(spherely.intersects(poly_arr, s_poly))
        if len(hits) > 0:
            # dict.fromkeys dedups multiple beam-ring hits per granule while
            # preserving order (a no-op for single-ring swath mode).
            out[int(shard)] = list(dict.fromkeys(idx[int(h)] for h in hits))
    return out


def _intersect_mortie(
    records, grid, all_shards, order=8, footprint="swath", product="ATL03"
) -> Dict[int, List[int]]:
    """HEALPix MOC intersection via mortie ``morton_coverage_moc``.

    ``footprint="beams"`` decomposes each granule into per-beam-pair corridor
    rings (issue #65); a granule maps to a shard if any of its rings cover it
    (deduped). Consumes the same ``(lats, lons)`` rings as the spherely path.
    """
    from mortie import moc_to_order, morton_coverage, morton_coverage_moc

    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    out: Dict[int, List[int]] = {}

    if is_healpix:
        parent_order = grid.parent_order
        for i, rec in enumerate(records):
            for rlats, rlons in _granule_footprints(rec, footprint, product):
                try:
                    moc = np.asarray(morton_coverage_moc(rlats, rlons, order=order))
                except Exception:
                    continue
                if moc.size == 0:
                    continue
                try:
                    shards = np.unique(moc_to_order(moc, parent_order))
                except Exception:
                    continue
                for s in shards.tolist():
                    s = int(s)
                    if s in all_shards:
                        out.setdefault(s, []).append(i)
        # Dedup a granule reached via multiple beam rings (no-op for swath).
        return {k: list(dict.fromkeys(v)) for k, v in out.items()}

    # Non-HEALPix: flat order-`order` granule cell index + per-shard lookup.
    cell_arrays, rec_idx = [], []
    for i, rec in enumerate(records):
        for rlats, rlons in _granule_footprints(rec, footprint, product):
            try:
                cells = morton_coverage(rlats, rlons, order=order)
            except Exception:
                continue
            if len(cells) == 0:
                continue
            cell_arrays.append(np.asarray(cells, dtype=np.int64))
            rec_idx.append(i)
    if not cell_arrays:
        return {}
    all_cells = np.concatenate(cell_arrays)
    counts = np.fromiter((len(c) for c in cell_arrays), dtype=np.int64, count=len(cell_arrays))
    flat_idx = np.repeat(np.asarray(rec_idx, dtype=np.int64), counts)
    srt = np.argsort(all_cells, kind="stable")
    sorted_cells, sorted_idx = all_cells[srt], flat_idx[srt]
    for shard in all_shards:
        fp = grid.shard_footprint(shard)
        sx, sy = fp.exterior.coords.xy
        try:
            s_cells = morton_coverage(np.asarray(sy), np.asarray(sx), order=order)
        except Exception:
            continue
        if len(s_cells) == 0:
            continue
        lo = np.searchsorted(sorted_cells, s_cells, side="left")
        hi = np.searchsorted(sorted_cells, s_cells, side="right")
        nz = hi > lo
        if not nz.any():
            continue
        gathered = np.concatenate([sorted_idx[a:b] for a, b in zip(lo[nz], hi[nz])])
        out[int(shard)] = [int(i) for i in np.unique(gathered)]
    return out


_BACKENDS = {
    "spherely": _intersect_spherely,
    "mortie": _intersect_mortie,
}


# ── ShardMap ─────────────────────────────────────────────────────────────────


@dataclass
class ShardMap:
    """Work-distribution manifest: shard key -> granules, tied to one grid.

    Parameters
    ----------
    grid_signature : dict
        ``grid.spatial_signature()`` at build time -- the spatial layout only
        (#89). The runner checks it against the run grid's spatial signature so
        a map can't be paired with a mismatched *spatial* grid, while staying
        reusable across configs that differ only in aggregation fields. (Kept as
        ``grid_signature`` for back-compat; old maps carry the full signature
        and still validate via a spatial-subset projection.)
    shard_keys : list of int
        Sorted shard keys with at least one granule.
    granules : list of list of dict
        Parallel to ``shard_keys``. Each granule is ``{"id", "s3", "https"}``
        (option C -- self-contained, endpoint-neutral).
    metadata : dict
        Provenance copied from the Catalog plus backend/timing info.
    """

    grid_signature: dict
    shard_keys: List[int]
    granules: List[List[dict]]
    metadata: dict = field(default_factory=dict)
    aoi_mask: List[List[int]] | None = None
    """Optional strict-AOI per-shard mask payload (issue #101), parallel to
    ``shard_keys``. ``None`` when ``output.aoi_mask`` is off (the default) — the
    manifest then carries no extra key and is byte-identical to a pre-feature map.
    Each entry is a JSON int list the grid expands to a per-cell bool over the
    shard's ``children()``: a compact MOC (HEALPix) or the True-cell indices into
    ``children`` order (rectilinear)."""

    @classmethod
    def build(
        cls,
        catalog,
        grid,
        *,
        region=None,
        aoi=None,
        backend: str = "auto",
        mortie_order: int | None = None,
        footprint: str = "swath",
    ) -> "ShardMap":
        """Build a ShardMap from a ``Catalog`` and an output grid.

        Parameters
        ----------
        catalog : Catalog
            Fetched granule metadata (provides ``granule_records()``).
        grid : OutputGrid
            Output grid (provides ``coverage``, ``shard_footprint``,
            ``spatial_signature``).
        region : list of (lats, lons), optional
            Coverage mask in WGS84. Defaults to the catalog bbox rectangle.
        aoi : AOIGeometry | bytes | str | list of (lats, lons), optional
            Strict-AOI polygon for the optional ``output.aoi_mask`` (issue #101),
            supplied as an :class:`~zagg.grids.aoi.AOIGeometry`, WKB ``bytes``, WKT
            ``str``, or ``(lats, lons)`` ring parts. ``None`` (default) reuses
            ``region`` (or the bbox rectangle), so a ring run is unchanged. Only
            consulted when ``output.aoi_mask`` is on — a flag-off run never builds
            it and stays byte-identical.
        backend : {"auto", "spherely", "mortie"}
            Geometry backend. ``"auto"`` -> spherely when importable, else
            mortie for HEALPix grids (non-HEALPix grids require spherely and
            raise an ``ImportError`` with an install pointer when it is absent).
        mortie_order : int, optional
            MOC order for the mortie backend. ``None`` (default) pins it to the
            grid's inner-chunk order ``grid.chunk_order`` (the ``chunk_inner``
            order, defaulting to ``parent_order`` when unset), clamped to mortie's
            order-18 coverage cap -- the dispatch chunk's own resolution, enough
            to keep ``moc_to_order`` from upsampling a footprint onto neighbor
            shards (#92) at near-minimal compute. Raises if the resolved order is
            coarser than ``parent_order``.
        footprint : {"swath", "beams"}
            Granule footprint used for intersection. ``"swath"`` (default) uses
            the raw CMR polygon. ``"beams"`` decomposes ICESat-2 ATL03/06 swaths
            into per-beam-pair corridors so granules stop being assigned to
            shards their beams never cross (issue #65); non-beam products fall
            back to the swath ring.

            .. deprecated::
                The ``"beams"`` corridor mechanism is a stopgap (see
                ``beams.py``); remove it once native per-beam CMR geometry, the
                memory-handling robustness in #66, or data virtualization (#97)
                lands.

        Returns
        -------
        ShardMap
        """
        if footprint not in ("swath", "beams"):
            raise ValueError(f"footprint must be 'swath' or 'beams' (got {footprint!r})")
        records = catalog.granule_records()
        # Product short-name drives beam decomposition (collection like "ATL03_007").
        product = ((catalog.metadata or {}).get("collection") or "").split("_")[0].upper()
        if footprint == "beams":
            from zagg.catalog.beams import is_beam_product

            if not is_beam_product(product):
                # ``beams`` is opt-in; silently degrading to swath here would
                # leave the metadata recording ``footprint="beams"`` while the
                # tightening did nothing. Make the mismatch loud.
                collection = (catalog.metadata or {}).get("collection")
                if collection is None:
                    detail = (
                        "catalog has no 'collection' metadata so the product can't be identified"
                    )
                else:
                    detail = f"catalog collection {collection!r} resolves to product {product!r}"
                raise ValueError(
                    f"footprint='beams' requires an ICESat-2 beam product (ATL03/ATL06); {detail}"
                )
        parts = _region_parts(region, catalog.metadata)
        all_shards = set(int(s) for s in grid.coverage(parts))

        chosen = _resolve_backend(backend, grid)
        if chosen not in _BACKENDS:
            raise ValueError(f"unknown backend: {backend!r} (resolved to {chosen!r})")

        t0 = time.perf_counter()
        if chosen == "mortie":
            mortie_order = _resolve_mortie_order(mortie_order, grid)
            shard_to_idx = _intersect_mortie(
                records,
                grid,
                all_shards,
                order=mortie_order,
                footprint=footprint,
                product=product,
            )
        else:
            shard_to_idx = _BACKENDS[chosen](
                records,
                grid,
                all_shards,
                footprint=footprint,
                product=product,
            )
        wall = time.perf_counter() - t0

        shard_keys = sorted(shard_to_idx)
        granules = [[_granule_entry(records[i]) for i in shard_to_idx[k]] for k in shard_keys]
        meta = {
            **(catalog.metadata or {}),
            "backend": chosen,
            "footprint": footprint,
            "total_granules": len(records),
            "total_shards": len(shard_keys),
            "total_pairs": sum(len(g) for g in granules),
            "build_wall_s": round(wall, 3),
        }
        if chosen == "mortie":
            meta["mortie_order"] = mortie_order

        # Strict-AOI mask (issue #101), default off: precompute a per-shard payload
        # so the worker can package the per-cell bool with no region plumbing. Only
        # when ``output.aoi_mask`` is on — otherwise the manifest is unchanged.
        from zagg.config import get_aoi_mask
        from zagg.grids.aoi import as_aoi_geometry

        grid_config = getattr(grid, "config", None)
        # The AOI defaults to the coverage ``region`` (ring parts) when no explicit
        # WKB/WKT/parts ``aoi`` is given, so a ring run is unchanged; an explicit
        # ``aoi`` (e.g. WKB/WKT) drives the mask while ``coverage`` still uses parts.
        aoi_mask = (
            _compute_aoi_mask(grid, as_aoi_geometry(aoi if aoi is not None else parts), shard_keys)
            if grid_config is not None and get_aoi_mask(grid_config)
            else None
        )
        if aoi_mask is not None:
            meta["aoi_mask"] = True
        return cls(grid.spatial_signature(), shard_keys, granules, meta, aoi_mask)

    def to_json(self, path: str) -> None:
        """Write the manifest as JSON."""
        from pathlib import Path

        payload = {
            "metadata": self.metadata,
            "grid_signature": self.grid_signature,
            "shard_keys": self.shard_keys,
            "granules": self.granules,
        }
        # Carry the strict-AOI per-shard mask only when present (issue #101): a map
        # built with the flag off writes no ``aoi_mask`` key, byte-identical to a
        # pre-feature manifest.
        if self.aoi_mask is not None:
            payload["aoi_mask"] = self.aoi_mask
        Path(path).write_text(json.dumps(payload, indent=2))

    @classmethod
    def from_json(cls, path: str) -> "ShardMap":
        """Load a manifest from JSON."""
        from pathlib import Path

        d = json.loads(Path(path).read_text())
        for key in ("grid_signature", "shard_keys", "granules"):
            if key not in d:
                raise ValueError(f"{path}: missing required key {key!r}")
        return cls(
            d["grid_signature"],
            d["shard_keys"],
            d["granules"],
            d.get("metadata", {}),
            d.get("aoi_mask"),
        )

    # Schema-metadata key for the manifest's non-columnar payload (parquet form).
    _PARQUET_META_KEY = b"zagg:shardmap_meta"

    def to_parquet(self, path: str) -> None:
        """Write the manifest as parquet with a TYPED morton ``shard_keys`` column.

        The Arrow-native sibling of :meth:`to_json` (issue #135): ``shard_keys``
        carries mortie's ``morton_index`` pyarrow extension type (registered by
        mortie on import), so any Arrow-aware consumer sees morton words, not
        anonymous ints. ``granules`` (and ``aoi_mask`` when present) ride as
        per-shard JSON strings — the same self-contained payloads the JSON form
        stores — and ``metadata``/``grid_signature`` live in the schema metadata,
        mirroring the ``Catalog`` geoparquet convention (``sources.py``).

        Requires pyarrow (the off-Lambda ``catalog`` extra); the worker path
        never calls this — the runner dispatches from the JSON manifest.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        from mortie.arrow import from_morton_index

        words = np.asarray(self.shard_keys, dtype=np.uint64)
        columns: dict = {
            "shard_keys": from_morton_index(words),
            "granules": pa.array([json.dumps(g) for g in self.granules]),
        }
        if self.aoi_mask is not None:
            columns["aoi_mask"] = pa.array([json.dumps(m) for m in self.aoi_mask])
        meta = json.dumps({"metadata": self.metadata, "grid_signature": self.grid_signature})
        table = pa.table(columns).replace_schema_metadata({self._PARQUET_META_KEY: meta.encode()})
        pq.write_table(table, path)

    @classmethod
    def from_parquet(cls, path: str) -> "ShardMap":
        """Load a manifest from the parquet form written by :meth:`to_parquet`.

        Importing :mod:`mortie.arrow` first registers the ``morton_index``
        extension type, so the ``shard_keys`` column rehydrates typed; the words
        are pulled over the C Data Interface (``import_c_array``) regardless.
        """
        import pyarrow.parquet as pq
        from mortie.arrow import import_c_array

        table = pq.read_table(path)
        raw = (table.schema.metadata or {}).get(cls._PARQUET_META_KEY)
        if raw is None or not {"shard_keys", "granules"}.issubset(table.column_names):
            raise ValueError(f"{path}: not a zagg ShardMap parquet manifest")
        d = json.loads(raw)
        if "grid_signature" not in d:
            raise ValueError(f"{path}: missing required key 'grid_signature'")
        shard_keys = [int(w) for w in import_c_array(table.column("shard_keys"))]
        granules = [json.loads(g) for g in table.column("granules").to_pylist()]
        aoi_mask = (
            [json.loads(m) for m in table.column("aoi_mask").to_pylist()]
            if "aoi_mask" in table.column_names
            else None
        )
        return cls(d["grid_signature"], shard_keys, granules, d.get("metadata", {}), aoi_mask)


__all__ = ["ShardMap"]
