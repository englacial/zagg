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
import warnings
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np

# Upper bound on the MOC order mortie's ``morton_coverage`` /
# ``morton_coverage_moc`` accept; a higher order raises inside mortie. The
# derived order is clamped to this so an exotic ``chunk_order`` can't push the MOC
# order past the cap and silently lose coverage (#92).
MORTIE_MOC_ORDER_CAP = 18

# Rings per batch call into mortie's ``polygons_to_morton_mocs`` (issue #396),
# set from a sweep over **real** CMR catalogs
# (``bench/shardmap_batch_vs_serial.py --knee``) -- the two earlier values (1024,
# then 256) were both picked against synthetic footprints and were both too
# large. A real ATL03 quarter-orbit footprint covers a median 9,266 MOC words at
# order 13 (mean 8,659, max 10,881 over 1,000 granules sampled from the clone)
# against ~220 for a compact synthetic quadrilateral, so a synthetic sweep
# under-reads the block's memory by ~40x.
#
# Peak has **two** terms and blocking bounds only one of them. The block term is
# this constant x per-ring MOC words: California at order 13 peaks 57 MB at
# block 32, 251 at 64, 412 at 256, 616 at 2048. The catalog term is
# ``_flatten_rings``' up-front concatenate -- ~0.5 KB of vertices per granule,
# so 283 MB of lat/lon plus 9 MB of offsets/owners across the 555,867-granule
# clone -- which no block size removes, and which is why the same 190,625-pair
# build peaks well above California's 4,354-granule figure at the same block.
# Sizing a build's memory means adding both, not reading the block alone.
#
# Wall is where the block earns nothing above ~32. Quote it min-of-N only:
# ``polygons_to_morton_mocs`` is rayon-parallel, so single-shot walls scatter by
# tens of percent on a loaded machine -- wider than the effect. Min of 3,
# California at order 13: 12.62 s at block 8, 11.06 at 16, 9.89 at 32, 9.84 at
# 48, 9.67 at 64, then 9.5-9.8 flat out to 2048. So the batch's fixed cost is
# amortized by ~32 rings (within 4% of the asymptote), against the order of
# magnitude the synthetic sweep suggested. Order 9 is flatter still (0.73 s at
# 16, 0.66 at 32, 0.64 at 64, 0.58 at 1024).
#
# 32 is the size: it holds the block term to 57 MB on California -- 4.4x under
# 64's 251 MB and 7x under 256's 412 MB -- for 2% wall there. On the far denser
# 88S catalog the two are much closer on both axes (32: 57.37 s / 192 MB,
# 64: 55.02 s / 206 MB, min of 6 across two block orderings), so nothing there
# argues against it. Block ordering matters when measuring this: a single
# block-major sweep on a loaded machine put 88S's 32 at 63.13 s, and reversing
# the order moved it to 57.37 s against 64's 56.55 s -- drift, not a knee.
_MOC_BATCH_RINGS = 32

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


def _flatten_rings(records, footprint, product):
    """Flatten every granule's rings into mortie's ragged batch layout (#396).

    Returns ``(lats, lons, offsets, owners)`` or ``None`` when nothing survives.
    ``offsets`` are arrow list offsets satisfying mortie's strict batch contract
    (``offsets[0] == 0`` and ``offsets[-1] == len(lats) == len(lons)``: exact
    coverage of the vertex arrays), and ``owners[r]`` is the **record index ring
    ``r`` came from**. That ring -> granule map is what makes the flattening
    lossless: ``_granule_footprints`` yields one ring per granule in ``"swath"``
    mode but one per beam pair in ``"beams"`` mode (issue #65), while mortie's
    batch is one-ring-per-entry by design, so a granule's shard set is the union
    over its own rings, recovered through ``owners``.

    Rings mortie's coverage rejects outright -- fewer than 3 vertices, mismatched
    lat/lon lengths, or a non-finite coordinate -- are dropped here. The serial
    path swallowed those per granule; the batch call fails the *whole* call
    instead (naming the lowest-index offender), so screening them out up front is
    what keeps one malformed footprint from taking the build down with it.
    """
    lat_parts, lon_parts, counts, owners = [], [], [], []
    for i, rec in enumerate(records):
        for rlats, rlons in _granule_footprints(rec, footprint, product):
            rlats = np.asarray(rlats, dtype=float)
            rlons = np.asarray(rlons, dtype=float)
            if rlats.size != rlons.size or rlats.size < 3:
                continue
            if not (np.isfinite(rlats).all() and np.isfinite(rlons).all()):
                continue
            lat_parts.append(rlats)
            lon_parts.append(rlons)
            counts.append(rlats.size)
            owners.append(i)
    if not counts:
        return None
    offsets = np.zeros(len(counts) + 1, dtype=np.int64)
    np.cumsum(np.asarray(counts, dtype=np.int64), out=offsets[1:])
    return (
        np.concatenate(lat_parts),
        np.concatenate(lon_parts),
        offsets,
        np.asarray(owners, dtype=np.int64),
    )


def _first_of_run(values) -> np.ndarray:
    """Boolean mask marking the first element of every run of equal values.

    Precondition: ``values.size >= 1`` (``mask[0] = True`` raises ``IndexError``
    on an empty array). Both call sites in ``_intersect_mortie`` guarantee it --
    the first runs only after the ``if not hit_shards: return {}`` short-circuit,
    and the second slices a non-empty run of that same array -- so the empty case
    is left as a documented contract rather than an untested branch.
    """
    mask = np.empty(values.size, dtype=bool)
    mask[0] = True
    np.not_equal(values[1:], values[:-1], out=mask[1:])
    return mask


def _batch_ring_mocs(lats, lons, offsets, start, stop, order) -> tuple:
    """MOCs for rings ``[start, stop)`` -- one batch call into mortie (#396).

    Returns ``(mocs, batch_exc)``, where ``batch_exc`` is the exception that
    forced the serial fallback for this block, or ``None`` when the batch call
    succeeded. The caller surfaces it once per build rather than per block.

    ``polygons_to_morton_mocs`` (mortie >= 0.9.4, espg/mortie#153) covers the
    whole block in one crossing of the Python/Rust boundary with the GIL
    released and rayon spread across polygons, replacing one
    ``morton_coverage_moc`` call per granule. The offsets slice is re-based so
    the block satisfies the strict exact-coverage contract on its own vertex
    slice.

    The batch call is fail-fast for the whole block, where the serial path
    dropped just the offending granule; ``_flatten_rings`` screens the
    documented rejection causes, so a raise here means something undocumented
    (e.g. a captured kernel panic). Rather than lose the build, fall back to the
    per-ring scalar path for this block only, which restores the old
    swallow-one-granule behavior exactly.
    """
    from mortie import morton_coverage_moc, polygons_to_morton_mocs

    lo, hi = int(offsets[start]), int(offsets[stop])
    try:
        values, out_offsets = polygons_to_morton_mocs(
            lats[lo:hi], lons[lo:hi], offsets[start : stop + 1] - lo, order=order
        )
    except Exception as exc:
        mocs = []
        for r in range(start, stop):
            a, b = int(offsets[r]), int(offsets[r + 1])
            try:
                mocs.append(np.asarray(morton_coverage_moc(lats[a:b], lons[a:b], order=order)))
            except Exception:
                mocs.append(np.empty(0, dtype=np.uint64))
        return mocs, exc
    return [values[out_offsets[r] : out_offsets[r + 1]] for r in range(stop - start)], None


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
    (#92) at near-minimal compute -- the real-catalog order sweep
    (``bench/neon_order_sweep.py``) shows granules/shard flat for every
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
    """HEALPix MOC intersection via mortie's batch ``polygons_to_morton_mocs``.

    ``footprint="beams"`` decomposes each granule into per-beam-pair corridor
    rings (issue #65); a granule maps to a shard if any of its rings cover it
    (deduped). Consumes the same ``(lats, lons)`` rings as the spherely path.

    The HEALPix path is batched (issue #396): every granule's rings are flattened
    once into mortie's ragged layout (``_flatten_rings``) and covered a block at
    a time (``_batch_ring_mocs``) instead of one ``morton_coverage_moc`` call per
    granule, and shard membership is a ``searchsorted`` over the sorted shard
    array -- the same vectorized shape the non-HEALPix branch below already used
    -- instead of a scalar ``in all_shards`` test per cell. The result is
    identical: same shards, same per-shard granule order (records are visited in
    order, so the flattened owners are non-decreasing and a stable sort by shard
    leaves each shard's granules in record order, deduped).
    """
    from mortie import moc_to_order, morton_coverage

    is_healpix = hasattr(grid, "parent_order") and hasattr(grid, "child_order")
    out: Dict[int, List[int]] = {}

    if is_healpix:
        flat = _flatten_rings(records, footprint, product)
        if flat is None or not all_shards:
            return {}
        lats, lons, offsets, owners = flat
        parent_order = grid.parent_order
        shard_arr = np.fromiter(all_shards, dtype=np.uint64, count=len(all_shards))
        shard_arr.sort()

        hit_shards, hit_owners = [], []
        warned = False
        for start in range(0, owners.size, _MOC_BATCH_RINGS):
            stop = min(start + _MOC_BATCH_RINGS, owners.size)
            mocs, batch_exc = _batch_ring_mocs(lats, lons, offsets, start, stop, order)
            if batch_exc is not None and not warned:
                # Silence would hide a mortie-side bug behind a several-times
                # slower path (the serial loop is ~4x the batch on realistic
                # footprints), but one warning per block is noise on a
                # pathological catalog -- so say it once per build.
                warned = True
                warnings.warn(
                    f"mortie's batch coverage raised ({batch_exc!r}); this build fell back "
                    "to the per-ring path for the affected block(s). Results are unchanged, "
                    "but the build is several times slower. Reported once per build (#396).",
                    RuntimeWarning,
                    stacklevel=2,
                )
            for r, moc in enumerate(mocs):
                if moc.size == 0:
                    continue
                try:
                    # ``moc_to_order`` refines the MOC's coarse interior cells to
                    # the shard order; its cell budget can refuse a huge
                    # expansion, which drops that ring exactly as before.
                    shards = np.unique(np.asarray(moc_to_order(moc, parent_order)))
                except Exception:
                    continue
                # Filter to the AOI here, per ring, rather than accumulating the
                # block's densified cells and filtering once: a real quarter-orbit
                # footprint densifies to ~17k order-11 cells, so a block-level
                # buffer (plus its concatenate copies) dwarfs the AOI hits it
                # exists to produce. Per-ring keeps the accumulator bounded by
                # hits, as the pre-#396 loop was, and is just as vectorized --
                # ``owners`` is non-decreasing in ``r``, so appending in ring
                # order leaves the sort/dedup invariant below untouched.
                cand = shards.astype(np.uint64, copy=False)
                pos = np.searchsorted(shard_arr, cand)
                np.clip(pos, 0, shard_arr.size - 1, out=pos)
                kept = cand[shard_arr[pos] == cand]
                if kept.size:
                    hit_shards.append(kept)
                    hit_owners.append(np.full(kept.size, owners[start + r], dtype=np.int64))

        if not hit_shards:
            return {}
        cand = np.concatenate(hit_shards)
        own = np.concatenate(hit_owners)
        srt = np.argsort(cand, kind="stable")
        cand, own = cand[srt], own[srt]
        bounds = np.flatnonzero(_first_of_run(cand))
        for a, b in zip(bounds, np.append(bounds[1:], cand.size)):
            # Owners are non-decreasing within a shard (records visited in order,
            # a granule's beam rings adjacent), so dropping repeats of the run
            # dedups a granule reached via several rings -- the ``dict.fromkeys``
            # the serial path ended with.
            granules = own[a:b]
            out[int(cand[a])] = [int(g) for g in granules[_first_of_run(granules)]]
        return out

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
            # Distinct granules the exact intersection ASSIGNED to some shard —
            # ``total_granules`` above is the catalog records CONSIDERED (the
            # input, often a conservative bbox-prefilter superset), and reading
            # it as the assigned count is a recurring misread (demo, 2026-08-04).
            "granules_assigned": len({g["id"] for shard in granules for g in shard}),
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

    def reproject(self, target_grid, catalog=None) -> "ShardMap":
        """Derive a ShardMap at ``target_grid``'s ``parent_order`` (issue #294).

        HEALPix nesting means a shard map at one order is derivable from
        another **without touching the source catalog again** in the coarsen
        direction, and with only a scoped (per-shard) re-intersection in the
        refine direction -- either is far cheaper than a full
        :meth:`build` over the whole catalog.

        Parameters
        ----------
        target_grid : HealpixGrid
            Grid to reproject onto. Must share ``child_order`` (the leaf
            resolution) with the source grid -- reprojecting across different
            DGGS resolutions isn't meaningful, only the shard (dispatch) order
            changes.
        catalog : Catalog, optional
            Required only when refining (``target_grid.parent_order >`` this
            map's ``parent_order``): the shard map itself stores only
            ``{"id", "s3", "https"}`` per granule, not footprint geometry, so
            recovering which finer cell each granule falls in needs the
            granule records (``catalog.granule_records()``) back.

        Returns
        -------
        ShardMap
            New map at ``target_grid.parent_order``. ``metadata`` records the
            source order and ``reproject: {method: "coarsen"|"refine"|"noop"}``.

        Notes
        -----
        **Coarsen** (``target_order < source_order``): pure regroup, exact,
        no geometry. Each source shard's key coarsens via
        ``mortie.clip2order(target_order, shard_key)``; shards sharing a
        coarse parent are grouped and their granule lists unioned, deduplicated
        by granule ``id`` (a granule spanning multiple children counts once in
        the parent). Exact because footprint-to-cell assignment nests: a
        granule intersects a coarse cell iff it intersects one of its finer
        children.

        **Refine** (``target_order > source_order``): cannot be a pure
        regroup -- the coarse map never recorded which child cell a granule
        fell in. Instead, for each source shard, its own granules are
        re-intersected at ``target_order`` (the same ``morton_coverage_moc``
        machinery :meth:`build` uses), restricted to that shard's own
        descendant cells (``generate_morton_children(shard_key,
        target_order)``). In the interior this reproduces the direct
        :meth:`build` at the finer order; at a region/AOI boundary it may
        **over-include** child shards a region-restricted direct build would
        drop (the #101 whole-shard overhang class -- reproject applies no
        region clip), but it never drops a real intersection. Costs only this
        shard's granules, not the whole catalog. Refine always
        re-intersects via the mortie MOC path regardless of the source's
        ``backend`` (a spherely-built source is not reproduced by it), so the
        derived map records ``backend="mortie"``.
        """
        from mortie import clip2order, generate_morton_children

        target_order = getattr(target_grid, "parent_order", None)
        target_child_order = getattr(target_grid, "child_order", None)
        if target_order is None or target_child_order is None:
            raise ValueError(
                "reproject: target_grid must be a HEALPix grid (parent_order/child_order)"
            )
        source_order = self.grid_signature.get("parent_order")
        source_child_order = self.grid_signature.get("child_order")
        if source_order is None or self.grid_signature.get("type") != "healpix":
            raise ValueError("reproject: source ShardMap must be a HEALPix grid_signature")
        if source_child_order != target_child_order:
            raise ValueError(
                f"reproject: child_order must match (source={source_child_order}, "
                f"target={target_child_order}) -- reproject only changes the shard "
                "(parent_order), not the leaf DGGS resolution"
            )
        if not (0 <= target_order <= target_child_order):
            raise ValueError(
                f"reproject: target_order {target_order} outside [0, child_order="
                f"{target_child_order}]"
            )

        if target_order == source_order:
            meta = dict(self.metadata or {})
            meta["reproject"] = {
                "source_parent_order": int(source_order),
                "target_parent_order": int(target_order),
                "method": "noop",
            }
            return ShardMap(
                target_grid.spatial_signature(),
                list(self.shard_keys),
                [[_granule_entry(g) for g in shard] for shard in self.granules],
                meta,
                self.aoi_mask,
            )

        if target_order < source_order:
            # ── coarsen: pure regroup, exact, no geometry ────────────────
            keys = np.asarray([int(k) for k in self.shard_keys], dtype=np.uint64)
            parents = clip2order(target_order, keys)
            groups: Dict[int, List[int]] = {}
            for i, p in enumerate(parents.tolist()):
                groups.setdefault(int(p), []).append(i)

            new_keys = sorted(groups)
            new_granules = []
            for k in new_keys:
                seen: dict = {}
                for i in groups[k]:
                    for g in self.granules[i]:
                        seen[g["id"]] = _granule_entry(g)
                new_granules.append(list(seen.values()))
            method = "coarsen"
        else:
            # ── refine: scoped re-intersection (needs source footprints) ─
            if catalog is None:
                raise ValueError(
                    "reproject: refine (target_order > source_order) needs the source "
                    "Catalog for granule footprints -- the ShardMap itself only stores "
                    "{id, s3, https}, not geometry. Pass catalog=<Catalog>."
                )
            records_by_id = {r["id"]: r for r in catalog.granule_records()}
            footprint = self.metadata.get("footprint", "swath")
            product = (self.metadata.get("collection") or "").split("_")[0].upper()
            mortie_order = _resolve_mortie_order(None, target_grid)

            new_granules_map: Dict[int, dict] = {}
            for shard_key, gran_list in zip(self.shard_keys, self.granules):
                ids = [g["id"] for g in gran_list]
                missing = [gid for gid in ids if gid not in records_by_id]
                if missing:
                    extra = "..." if len(missing) > 5 else ""
                    raise ValueError(
                        f"reproject: refine needs footprints for all granules in the source "
                        f"map, but the catalog is missing {len(missing)} of them (e.g. "
                        f"{missing[:5]}{extra})"
                    )
                sub_records = [records_by_id[gid] for gid in ids]
                descendants = set(
                    int(s) for s in generate_morton_children(int(shard_key), target_order)
                )
                shard_to_idx = _intersect_mortie(
                    sub_records,
                    target_grid,
                    descendants,
                    order=mortie_order,
                    footprint=footprint,
                    product=product,
                )
                for k, idxs in shard_to_idx.items():
                    bucket = new_granules_map.setdefault(int(k), {})
                    for i in idxs:
                        entry = _granule_entry(sub_records[i])
                        bucket[entry["id"]] = entry

            new_keys = sorted(new_granules_map)
            new_granules = [list(new_granules_map[k].values()) for k in new_keys]
            method = "refine"

        meta = dict(self.metadata or {})
        meta["reproject"] = {
            "source_parent_order": int(source_order),
            "target_parent_order": int(target_order),
            "method": method,
        }
        meta["total_shards"] = len(new_keys)
        meta["total_pairs"] = sum(len(g) for g in new_granules)
        # Recomputed like total_shards/total_pairs: a derived map must not carry
        # the source's assigned count (refine can over-include at boundaries;
        # a pre-field source map simply gains the key).
        meta["granules_assigned"] = len({g["id"] for shard in new_granules for g in shard})
        # The dropped per-shard AOI mask (aoi_mask=None below) must not still be
        # advertised in the derived map's metadata.
        meta.pop("aoi_mask", None)
        # This reproject ran no timed build, so the source build's timing must
        # not describe the derived map.
        meta.pop("build_wall_s", None)
        if method == "refine":
            # Refine re-intersects via the mortie MOC path (``_intersect_mortie``)
            # regardless of the source's backend -- a spherely-built (exact-S2)
            # source is not reproduced by it -- so record what actually ran.
            meta["backend"] = "mortie"
            meta["mortie_order"] = mortie_order
        else:
            # Coarsen is a pure regroup: no geometry backend and no order-based
            # build ran, so the source's ``backend``/``mortie_order`` don't apply.
            meta.pop("backend", None)
            meta.pop("mortie_order", None)
        return ShardMap(target_grid.spatial_signature(), new_keys, new_granules, meta, None)

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
