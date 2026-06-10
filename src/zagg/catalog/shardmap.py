"""Shard-map builder: ``Catalog`` + grid -> ``ShardMap`` manifest.

This is concern (2) of the #24 split -- take fetched granule metadata plus a
grid spec and produce the work-distribution manifest the runner dispatches.
It is independent of the fetch (concern 1): the same ``Catalog`` can build many
ShardMaps at different grids.

The ``ShardMap`` is a small, self-contained JSON plan (option C): each granule
is recorded with **both** its S3 and HTTPS hrefs so the runner can pick the
endpoint at dispatch time via ``data_source.driver`` -- the map itself stays
endpoint-neutral and never needs the Catalog at run time. It also records the
grid ``signature()`` so a run can refuse a map built for a different grid.

Geometry backends (all sphere-aware where it matters):

- ``spherely`` -- exact S2 intersection via ``SpatialIndex`` (build once, query
  per shard). Requires the spatial-index build of spherely.
- ``mortie``   -- HEALPix MOC intersection (``morton_coverage_moc``); a tiny
  ~0.01% polar omission vs S2 (espg/mortie#32), no extra deps.
- ``shapely``  -- WGS84 STRtree fallback; antimeridian/pole correctness not
  guaranteed.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np


# ── granule footprint helpers ────────────────────────────────────────────────

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


def _resolve_backend(backend: str, grid) -> str:
    """Resolve ``"auto"`` to a concrete, grid-appropriate backend.

    Prefers exact S2 (``spherely.SpatialIndex``) when available. Without it,
    falls back per grid family: **mortie** for HEALPix (its native MOC order
    matches the grid), **shapely** for rectilinear (a global MOC order is far
    too coarse for fine projected tiles and over-commissions every granule to
    every shard).
    """
    if backend != "auto":
        return backend
    try:
        import spherely

        if hasattr(spherely, "SpatialIndex"):
            return "spherely"
    except ImportError:
        pass
    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    return "mortie" if is_healpix else "shapely"


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


# ── backends (operate on granule records) ────────────────────────────────────

def _intersect_spherely(records, grid, all_shards) -> Dict[int, List[int]]:
    """Exact S2 intersection via spherely ``SpatialIndex``.

    Builds the index once over granule footprints, then issues one
    ``query(..., predicate="intersects")`` per shard footprint.
    """
    import spherely

    polys, idx = [], []
    for i, rec in enumerate(records):
        poly = _to_spherely_polygon(rec["lats"], rec["lons"])
        if poly is not None:
            polys.append(poly)
            idx.append(i)
    if not polys:
        return {}
    tree = spherely.SpatialIndex(np.asarray(polys))

    out: Dict[int, List[int]] = {}
    for shard in all_shards:
        fp = grid.shard_footprint(shard)
        sx, sy = fp.exterior.coords.xy
        s_poly = _to_spherely_polygon(np.asarray(sy), np.asarray(sx))
        if s_poly is None:
            continue
        hits = tree.query(s_poly, predicate="intersects")
        if len(hits) > 0:
            out[int(shard)] = [idx[int(h)] for h in hits]
    return out


def _intersect_mortie(records, grid, all_shards, order=8) -> Dict[int, List[int]]:
    """HEALPix MOC intersection via mortie ``morton_coverage_moc``."""
    from mortie import moc_to_order, morton_coverage, morton_coverage_moc

    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    out: Dict[int, List[int]] = {}

    if is_healpix:
        parent_order = grid.parent_order
        for i, rec in enumerate(records):
            try:
                moc = np.asarray(morton_coverage_moc(rec["lats"], rec["lons"], order=order))
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
        return out

    # Non-HEALPix: flat order-`order` granule cell index + per-shard lookup.
    cell_arrays, rec_idx = [], []
    for i, rec in enumerate(records):
        try:
            cells = morton_coverage(rec["lats"], rec["lons"], order=order)
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


def _intersect_shapely(records, grid, all_shards) -> Dict[int, List[int]]:
    """WGS84 STRtree fallback (antimeridian/pole correctness not guaranteed)."""
    from shapely import STRtree, make_valid
    from shapely.geometry import Polygon

    polys, idx = [], []
    for i, rec in enumerate(records):
        try:
            poly = Polygon(zip(rec["lons"], rec["lats"]))
            if not poly.is_valid:
                poly = make_valid(poly)
            if poly.is_empty:
                continue
        except Exception:
            continue
        polys.append(poly)
        idx.append(i)
    if not polys:
        return {}
    tree = STRtree(polys)
    out: Dict[int, List[int]] = {}
    for shard in all_shards:
        fp = grid.shard_footprint(shard)
        hits = tree.query(fp, predicate="intersects")
        if len(hits) > 0:
            out[int(shard)] = [idx[int(h)] for h in hits]
    return out


_BACKENDS = {
    "spherely": _intersect_spherely,
    "mortie": _intersect_mortie,
    "shapely": _intersect_shapely,
}


# ── ShardMap ─────────────────────────────────────────────────────────────────

@dataclass
class ShardMap:
    """Work-distribution manifest: shard key -> granules, tied to one grid.

    Parameters
    ----------
    grid_signature : dict
        ``grid.signature()`` at build time. The runner checks it against the
        run grid so a map can't be silently paired with a mismatched grid.
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

    @classmethod
    def build(
        cls,
        catalog,
        grid,
        *,
        region=None,
        backend: str = "auto",
        mortie_order: int = 8,
    ) -> "ShardMap":
        """Build a ShardMap from a ``Catalog`` and an output grid.

        Parameters
        ----------
        catalog : Catalog
            Fetched granule metadata (provides ``granule_records()``).
        grid : OutputGrid
            Output grid (provides ``coverage``, ``shard_footprint``,
            ``signature``).
        region : list of (lats, lons), optional
            Coverage mask in WGS84. Defaults to the catalog bbox rectangle.
        backend : {"auto", "spherely", "mortie", "shapely"}
            Geometry backend. ``"auto"`` -> spherely if available, else mortie.
        mortie_order : int
            MOC order for the mortie backend.

        Returns
        -------
        ShardMap
        """
        records = catalog.granule_records()
        parts = _region_parts(region, catalog.metadata)
        all_shards = set(int(s) for s in grid.coverage(parts))

        chosen = _resolve_backend(backend, grid)
        if chosen not in _BACKENDS:
            raise ValueError(f"unknown backend: {backend!r} (resolved to {chosen!r})")

        t0 = time.perf_counter()
        if chosen == "mortie":
            shard_to_idx = _intersect_mortie(records, grid, all_shards, order=mortie_order)
        else:
            shard_to_idx = _BACKENDS[chosen](records, grid, all_shards)
        wall = time.perf_counter() - t0

        shard_keys = sorted(shard_to_idx)
        granules = [
            [
                {"id": records[i]["id"], "s3": records[i]["s3"], "https": records[i]["https"]}
                for i in shard_to_idx[k]
            ]
            for k in shard_keys
        ]
        meta = {
            **(catalog.metadata or {}),
            "backend": chosen,
            "total_granules": len(records),
            "total_shards": len(shard_keys),
            "total_pairs": sum(len(g) for g in granules),
            "build_wall_s": round(wall, 3),
        }
        if chosen == "mortie":
            meta["mortie_order"] = mortie_order
        return cls(grid.signature(), shard_keys, granules, meta)

    def to_json(self, path: str) -> None:
        """Write the manifest as JSON."""
        from pathlib import Path

        Path(path).write_text(json.dumps({
            "metadata": self.metadata,
            "grid_signature": self.grid_signature,
            "shard_keys": self.shard_keys,
            "granules": self.granules,
        }, indent=2))

    @classmethod
    def from_json(cls, path: str) -> "ShardMap":
        """Load a manifest from JSON."""
        from pathlib import Path

        d = json.loads(Path(path).read_text())
        for key in ("grid_signature", "shard_keys", "granules"):
            if key not in d:
                raise ValueError(f"{path}: missing required key {key!r}")
        return cls(d["grid_signature"], d["shard_keys"], d["granules"],
                   d.get("metadata", {}))


__all__ = ["ShardMap"]
