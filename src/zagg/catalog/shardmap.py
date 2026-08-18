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
import logging
import re
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

# Records per ``mocs_and``/``mocs_to_orders`` call on the stored-index path
# (``_intersect_footprint_cells``, mortie 0.9.6's batch twins, espg/mortie#173).
# Same shape of argument as ``_MOC_BATCH_RINGS`` above: the whole-catalog single
# call holds *three* catalog-proportional arrays live at once -- the int64
# ``idx`` gather index, the ``values[idx]`` record-aligned copy it produces, and
# mortie's documented input copy -- measured 5,198 MB over the load plateau on
# the 555,867-granule clone against the pre-swap scalar loop's ~1.7 GB. Three is
# what makes that number add up: the clone's order-9 column is ~288 words per
# granule, so ~1.28 GB apiece, and two arrays over the floor would predict
# ~4.3 GB, three ~5.5 GB. Blocking bounds that term by the block, and it costs
# no wall: blocking is at worst free. Quote that like for like -- the two
# sessions that measured the same unblocked code on the same case disagree by
# more than the effect (3.83 s min-of-3 in the sweep below, 3.04 s min /
# 3.57 s median of 3 in the PR's measurement comment), against 2.53 s blocked.
# Blocked wins on both sessions' min and median, but by 0.5 s, not 1.3 s; this
# arm's walls scatter by tens of percent on a loaded machine. Clone sweep at
# order 9 (min-of-3 at the finalists): 2.53 s / 1,736.9 MB at 512, 2.50 s /
# 1,770 MB at 2048, 2.55 s / 1,828 MB at 4096, 2.49-3.10 s / 2,080-3,650 MB at
# 8192-65536, 3.27 s / 6,407 MB at 262144, 3.83 s / 5,198 MB unblocked. Peak is
# *not* monotone in block size: 262144 is 3 blocks on this catalog and peaks
# *above* the single call. Recorded as measured, not explained -- once a block is
# catalog-scale the peak is allocator scatter, not a term blocking controls.
# What the sweep supports is the small-block end, where the peak sits on the
# floor. That floor is the sweep's own minimum rather than a separate baseline
# run: 1,736.5 MB at 1024, 1,736.9 at 512 -- column materialization plus plan
# bookkeeping, which no block size removes. 512 over 2048 is bytes, not records:
# per-record MOC size grows ~40x from an order-9 column to an order-13 one, so
# a record-count block only bounds the worst case if it is sized against the
# fat-column end -- on 88S indexed at order 13 the peak is 2,627 MB at 512 vs
# 4,063 at 2048 (walls 4.62/4.59 s), and California o13 drops 1,261 -> 295 MB.
# The per-block cost of rebuilding the shared AOI operand's BMOC is measured,
# not assumed, negligible: 104 us/call at the 2,721-cell California operand,
# ~113 ms across the clone's 1,086 blocks, ~4% of wall.
#
# The ``mocs_intersect`` prefilter (the later phase) reuses this constant for
# both of its passes, and the blocking survives the prefilter for measured
# reasons on each:
# - the *predicate* pass walks every row, so what it hands mortie is
#   catalog-proportional exactly like the unblocked ``mocs_and`` was. The
#   mechanism is the binding's documented input copy; the figures below are
#   RSS over plateau -- that copy plus whatever the allocator keeps -- so
#   they are observations of the peak, not measurements of the copy. On the
#   clone's order-9 column (1,551 MB of values) the whole-column single call
#   measured 1,096 MB over plateau in-process against 184 MB blocked at 512,
#   the latter a high-water over the whole loop rather than one slice.
#   Neither reconciles 1:1 with the bytes handed over, and the decision only
#   needs the ordering they agree on: whole-column spikes ~1.1 GB, blocked
#   stays near the floor, for ~0.1 s of predicate wall (0.88 s whole-column
#   vs 0.98 s blocked, min-of-3 in-process). Same bytes-not-records argument
#   as above: 512 rows is ~1.2 MB of column per slice at order 9 and ~30 MB
#   at order 13.
# - the *survivor* pass is bounded by hits, ~0.4% of the clone against a
#   regional AOI (2,356 of 555,867 records) -- five blocks, where one call
#   would also be fine. But survivors are AOI-proportional, not
#   catalog-proportional: an AOI covering the catalog passes everything and
#   an unblocked survivor pass is then the ~5.2 GB whole-catalog call again.
#   Blocking is at worst free on wall (measured above), so it stays.
_CELLS_BATCH_RECORDS = 512

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


def _recorded_identity(entry: dict, canonicalize=None) -> tuple[str | None, tuple]:
    """``(canonical, distinguishing)`` for one shard entry.

    ``canonical`` is the granule id the leaf's D20 sidecar will carry — the
    basename of the href the runner resolves (:func:`zagg.telemetry.canonical_granule_id`),
    falling back to the id, then the datetime, for the raster entries that carry
    no href. ``None`` when there is nothing to canonicalize.

    ``distinguishing`` is what separates one granule from another. ``datetime``
    counts because :func:`zagg.telemetry.raster_granule_ids` records two
    acquisitions sharing an item id as one id; the sibling ``assets`` do not,
    because a record's identity is the primary alone (issue #425).

    ``canonicalize`` lets a hot loop hoist the import out of the per-entry path.
    """
    if canonicalize is None:
        from zagg.telemetry import canonical_granule_id as canonicalize

    href = entry.get("s3") or entry.get("https")
    named = href or entry.get("id") or entry.get("datetime")
    canonical = canonicalize(named) if named else None
    return canonical, (
        entry.get("id"),
        entry.get("s3"),
        entry.get("https"),
        entry.get("datetime"),
    )


def _collision_label(entry: dict) -> str:
    """Name one colliding entry by what tells it apart from its partner.

    The href when there is one — the prefix is what differs and what the remedy
    acts on. A raster pair has no href and both members carry the same id (that
    IS the collapse), so the datetime is the only thing left that separates them.
    """
    href = entry.get("s3") or entry.get("https")
    if href:
        return str(href)
    if entry.get("id") and entry.get("datetime"):
        return f"{entry['id']} @ {entry['datetime']}"
    return str(entry.get("id") or entry.get("datetime"))


def _refuse_basename_collisions(shard_keys, granules) -> None:
    """Refuse a map whose recorded granule identity is not per-shard unique (#468).

    Two granules of one shard whose recorded ids collapse onto one make the
    shard's catalog identity name fewer granules than it reads. PR #420 question
    (6) ruled the leaf-gate consequence acceptable *because* every catalog zagg
    reads names granules globally uniquely; this enforces that "because" where
    the invariant is owned rather than assuming it.

    Entries agreeing on every distinguishing field (:func:`_recorded_identity`)
    are one granule listed twice and pass — coarsen unions sibling shards, where
    a granule spanning several children legitimately arrives more than once.
    """
    n_collisions = 0
    shown: list = []
    # Imported once rather than per entry: this runs over every granule of every
    # shard, 555,867 of them at clone scale.
    from zagg.telemetry import canonical_granule_id

    for key, entries in zip(shard_keys, granules):
        by_canonical: dict = {}
        for entry in entries:
            canonical, distinguishing = _recorded_identity(entry, canonical_granule_id)
            # Empty as well as absent: an id of ``"/"`` canonicalizes to ``""``,
            # and reporting ``''`` as the collapsed id is exactly the
            # silently-wrong identity ``canonical_granule_id`` refuses to mint.
            if not canonical:
                continue
            by_canonical.setdefault(canonical, {})[distinguishing] = _collision_label(entry)
        # Filtered before sorting -- on every catalog zagg reads the filter
        # discards all of them, so sorting first is a per-shard sort of nothing.
        # Only the first few groups are retained: the message prints three, and a
        # wholly mis-scoped catalog has one group per granule.
        found = sorted((c, sorted(n.values())) for c, n in by_canonical.items() if len(n) > 1)
        n_collisions += len(found)
        shown += [(key, c, named) for c, named in found[: max(0, 4 - len(shown))]]
    if not n_collisions:
        return
    listed = "; ".join(f"shard {k} {c!r} <- {named}" for k, c, named in shown[:3])
    raise ValueError(
        f"ShardMap: {n_collisions} per-shard granule identity collision(s) — granules "
        f"assigned to one shard record as ONE granule id, so the shard's catalog identity "
        f"names fewer granules than it reads (issue #468). Usually one basename under two "
        f"key prefixes, in which case re-scope the catalog query so each granule appears "
        f"once, or de-collide the basenames; the entries below are named by whatever "
        f"separates them: {listed}{' ...' if n_collisions > 3 else ''}"
    )


#: Granule-id core for the catalog-time sibling join (issue #425): the
#: datetime / orbit / sub-orbit-granule / track fields shared between the
#: products of one acquisition (GEDI01_B_* and GEDI02_A_* of the same
#: sub-orbit), plus the trailing collection-generation suffix (``V002``) so
#: pairing is pinned within a product generation (v2<->v2, never across a
#: release boundary). The product prefix and the release/production fields
#: between track and generation differ per product and are ignored.
_SIBLING_ID_RE = re.compile(r"_(\d{13})_(O\d+)_(\d+)_(T\d+)_.*_(V\d+)(?:\.\w+)?$")

#: Unpaired fraction of the PRIMARY catalog above which the pairless warning
#: escalates (issue #425). An unpaired primary is excluded, so a sibling
#: catalog queried over a different AOI / time window (or a dropped page)
#: thins the product silently — per granule that is indistinguishable from a
#: genuinely missing sibling, in aggregate it is not.
_PAIRLESS_ALERT_FRACTION = 0.5


def sibling_join_key(granule_id: str) -> tuple | None:
    """The cross-product join key of a granule id, or ``None`` if unkeyed.

    ``(datetime, orbit, sub-orbit granule, track, generation)`` — identical
    between sibling products of one acquisition, distinct otherwise. ``None``
    (id does not carry the orbital core) means the granule cannot be paired.
    """
    m = _SIBLING_ID_RE.search(granule_id or "")
    return m.groups() if m is not None else None


def _pair_sibling_records(records, sibling_records, asset: str) -> tuple[list, list]:
    """Catalog-time sibling join (issue #425): one shard map, paired assets.

    Each primary record gaining a sibling match (on :func:`sibling_join_key`)
    carries the sibling's hrefs under ``assets[asset]`` — the STAC
    one-item-multiple-assets model, flowing into the shard-map entries via
    ``_granule_entry``'s existing ``assets`` passthrough. Pairless granules are
    **excluded and reported**, never silently kept: a primary without a
    sibling cannot be quality-filtered (the L2A join is core to the flux
    currency), and a sibling without a primary has no data to contribute.

    The join key deliberately ignores the release/production fields, so two
    records of one product can share it (a reprocessed sibling, a paginated
    query returning a granule twice). The join keeps the **first** record on a
    key — deterministic in catalog order — and reports every record it shadows
    as ``"duplicate-key"``: a shadowed sibling contributes nothing to the
    build, and nothing may leave the build silently. Duplicate keys on the
    PRIMARY side pair to one sibling and are kept (excluding a primary is
    destructive), but warn.

    Returns ``(paired_records, pairless)`` where ``pairless`` entries are
    ``{"id": <granule id>, "missing": <asset name | "primary" |
    "duplicate-key">}``.
    """
    sib_by_key: dict = {}
    pairless: list = []
    for rec in sibling_records:
        key = sibling_join_key(rec["id"])
        if key is None:
            continue
        if key in sib_by_key:
            pairless.append({"id": rec["id"], "missing": "duplicate-key"})
            continue
        sib_by_key[key] = rec
    paired: list = []
    matched: set = set()
    duplicate_primaries: list = []
    for rec in records:
        key = sibling_join_key(rec["id"])
        sib = sib_by_key.get(key) if key is not None else None
        if sib is None:
            pairless.append({"id": rec["id"], "missing": asset})
            continue
        if key in matched:
            duplicate_primaries.append(rec["id"])
        matched.add(key)
        rec = dict(rec)
        rec["assets"] = {
            **(rec.get("assets") or {}),
            asset: {"id": sib["id"], "s3": sib["s3"], "https": sib["https"]},
        }
        paired.append(rec)
    for key, sib in sib_by_key.items():
        if key not in matched:
            pairless.append({"id": sib["id"], "missing": "primary"})
    if duplicate_primaries:
        logging.warning(
            "ShardMap.build: %d primary granule(s) share a join key with another "
            "primary and pair to the SAME %s sibling (kept, not excluded — check "
            "the primary catalog for duplicates); e.g. %s",
            len(duplicate_primaries),
            asset,
            duplicate_primaries[:5],
        )
    return paired, pairless


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


def _regroup_hits(hit_shards, hit_owners) -> Dict[int, List[int]]:
    """Per-shard granule lists from parallel (shard, owner) hit arrays.

    Shared by both mortie paths (issue #396) so they cannot drift: a stable sort
    by shard, then run boundaries, then a consecutive-run dedup of owners inside
    each run. Owners are non-decreasing globally (records are visited in order,
    a granule's beam rings adjacent), so a stable sort leaves each shard's owners
    non-decreasing too and every repeat of a granule within a shard is adjacent
    -- making the run dedup exactly the ``dict.fromkeys`` the pre-#396 serial
    path ended with, same set and same order.
    """
    if not hit_shards:
        return {}
    cand = np.concatenate(hit_shards)
    own = np.concatenate(hit_owners)
    srt = np.argsort(cand, kind="stable")
    cand, own = cand[srt], own[srt]
    out: Dict[int, List[int]] = {}
    bounds = np.flatnonzero(_first_of_run(cand))
    for a, b in zip(bounds, np.append(bounds[1:], cand.size)):
        granules = own[a:b]
        out[int(cand[a])] = [int(g) for g in granules[_first_of_run(granules)]]
    return out


def _intersect_footprint_cells(
    rows, values, offsets, grid, all_shards, *, stored: bool = True
) -> Dict[int, List[int]]:
    """Shard assignment from row-aligned footprint MOCs -- no geometry (issue #396).

    The phase-3 fast path, and since issue #445 the *only* mortie ``swath``
    intersection: the cover it consumes is either the one the catalog carries
    (``Catalog.index_footprints``) or the one this build just made
    (``_live_cells_plan``), which is the whole remaining difference between an
    indexed build and an unindexed one. Where ``_intersect_mortie`` covers every
    footprint from its vertices, this does set algebra only: the AOI's
    shard keys are themselves a MOC at ``parent_order``, so a granule's shards
    are ``moc_to_order(moc_and(granule_moc, aoi_moc), parent_order)`` -- no
    ``searchsorted`` filter either, since intersecting with the AOI first is
    what restricts the result to shards in it.

    ``rows`` maps record index -> catalog table row (``rows[i]`` is the row
    record ``i`` came from); it is not the identity, because
    ``granule_records`` drops rows whose geometry is empty or not polygonal
    while the cover has one entry per row.

    ``stored`` says which of those two covers arrived, and is used for one
    thing: the cell-budget refusal's remediation clause. Re-indexing or dropping
    the ``footprint_cells`` column is only actionable when there *is* one
    (``plan is not None`` in :meth:`ShardMap.build`); an ephemeral cover has
    neither, so it gets the remedies that do apply to it. Nothing else branches
    on it -- the assignment is identical either way, which is the point of the
    shared intersection.

    mortie 0.9.6's batch twins (espg/mortie#173) remove the per-granule Python
    loop: one ``mocs_and(aoi_moc, ...)`` per block of records (empty results
    keep zero-width slots), chained into one ``mocs_to_orders`` (empty in,
    empty out) -- two boundary crossings per block instead of two calls per
    granule, and the AOI operand is normalized once per block instead of per
    granule. A ``mocs_intersect`` prefilter runs first: the predicate walks
    the row-aligned column in contiguous slices (no gather), and only the
    surviving records -- ~0.4% at clone scale against a regional AOI -- reach
    the materializing gather + ``mocs_and`` pass. The predicate is exact
    (``hits[i]`` iff ``mocs_and``'s slot ``i`` would be non-empty, mortie's
    documented contract), so survivors' results are byte-identical to running
    every record through. Both passes are blocked by ``_CELLS_BATCH_RECORDS``
    (see the constant): the survivor pass because its gather copy plus
    mortie's input copy are all-hit-proportional, the predicate because the
    whole-column clone call measured ~1.1 GB of RSS over plateau against
    ~184 MB blocked.
    """
    from mortie import compress_moc, mocs_and, mocs_intersect, mocs_to_orders

    if len(rows) == 0 or not all_shards:
        return {}
    parent_order = grid.parent_order
    shard_arr = np.fromiter(all_shards, dtype=np.uint64, count=len(all_shards))
    shard_arr.sort()
    # Uniform-order cells, so no cell contains another and the sorted array is
    # already a well-formed MOC; compressing it just folds complete sibling
    # quadruples into their parent, shrinking the shared ``mocs_and`` operand
    # without changing any result.
    aoi_moc = np.asarray(compress_moc(shard_arr))

    # A *malformed* stored word still panics in Rust (``PanicException``, a
    # ``BaseException``) and aborts the build, as before: only
    # ``index_footprints`` writes this column, so a bad word means a corrupted
    # catalog, which should not be built through. One semantic change rides on
    # the batch: the scalar loop's per-granule ``except Exception: continue``
    # around ``moc_to_order`` (cell-budget refusal -> silently drop that
    # granule) has no batch equivalent -- ``mocs_to_orders`` applies the same
    # per-MOC budget but refuses the *whole call* (``ValueError`` naming the
    # lowest-index offender), so a refusal now fails the build loudly instead
    # of quietly losing one granule. This is a real divergence from the
    # geometry path, which still swallows the same refusal per ring
    # (``_intersect_mortie``, below): the same catalog + grid can now raise
    # where it built. Since issue #445 the presence of the column is no longer
    # what decides which side you are on -- every mortie HEALPix ``swath`` build
    # reaches this raise, stored column or ephemeral cover alike, and only
    # ``footprint="beams"``, spherely, rectilinear and paired builds still
    # swallow it on the geometry path. (That is why the raise below takes
    # ``stored``: the remedy differs even though the refusal does not.)
    # Deliberate, and narrow -- refusal is not unreachable, but it is remote.
    # ``aoi_moc``
    # is compressed, so the intersection can keep cells *coarser* than
    # ``parent_order`` and ``mocs_to_orders`` can genuinely expand; the bound
    # is the AOI-clipped footprint densified at ``parent_order``, so tripping
    # the 1<<20 flat-cell budget takes a single granule covering >1e6 shard
    # cells inside the AOI. At that size the silent drop is the worse
    # outcome -- it removes a granule that covers a millionth-scale chunk of
    # the AOI without a word -- so loud-over-silent is the intended trade, and
    # the raise below says which records and what to do about it.
    rows = np.asarray(rows, dtype=np.int64)
    offsets = np.asarray(offsets, dtype=np.int64)
    values = np.asarray(values)

    # Prefilter (espg/mortie#173's range-walk predicate, this PR its first
    # production consumer): one bool per table row, materializing nothing.
    # It walks the *column* rather than the records because the column's own
    # arrow offsets make every block a contiguous zero-copy slice -- gathering
    # per record is exactly the copy the predicate exists to avoid. Rows
    # ``granule_records`` dropped are walked too; they cost nothing
    # (``index_footprints`` gives them zero-width runs) and ``hits[rows]``
    # discards them. Blocked, not whole-column: mortie's binding copies
    # ``values`` before releasing the GIL, and the whole-column call measured
    # ~1.1 GB of RSS over plateau at clone scale against ~184 MB blocked (see
    # ``_CELLS_BATCH_RECORDS`` for what those numbers do and do not say).
    n_rows = offsets.size - 1
    hits = np.empty(n_rows, dtype=bool)
    for a in range(0, n_rows, _CELLS_BATCH_RECORDS):
        b = min(a + _CELLS_BATCH_RECORDS, n_rows)
        base = offsets[a]
        hits[a:b] = mocs_intersect(aoi_moc, values[base : offsets[b]], offsets[a : b + 1] - base)
    # ``surv`` holds *original record indices* (``np.flatnonzero`` of a
    # record-aligned mask, so it is strictly increasing) -- the owner mapping
    # below must come from it, never from block-local positions.
    surv = np.flatnonzero(hits[rows])
    if surv.size == 0:
        return {}
    surv_rows = rows[surv]

    hit_shards, hit_owners = [], []
    for start in range(0, surv_rows.size, _CELLS_BATCH_RECORDS):
        blk = surv_rows[start : start + _CELLS_BATCH_RECORDS]
        own = surv[start : start + _CELLS_BATCH_RECORDS]
        # Gather the block's spans out of the row-aligned column (ragged fancy
        # index), so dropped rows never reach the batch call and slot ``i`` of
        # the block is record ``own[i]``.
        starts = offsets[blk]
        lens = offsets[blk + 1] - starts
        rec_off = np.zeros(blk.size + 1, dtype=np.int64)
        np.cumsum(lens, out=rec_off[1:])
        idx = np.repeat(starts - rec_off[:-1], lens) + np.arange(rec_off[-1], dtype=np.int64)
        try:
            hit_vals, hit_off = mocs_and(aoi_moc, values[idx], rec_off)
            flat, flat_off = mocs_to_orders(np.asarray(hit_vals), np.asarray(hit_off), parent_order)
        except ValueError as exc:
            # mortie names the offending MOC by its index *within the call*,
            # which is a slot in this block, not a record. Re-base it: an
            # un-rebased "MOC 400" out of a 512-record block is a plausible
            # record index, so it would send the operator to the wrong granule
            # rather than obviously failing. Re-raise, never swallow.
            # The remedy depends on where the cover came from (issue #445).
            # Telling the operator of an *unindexed* build to re-index or drop a
            # column names two things that do not exist, and re-indexing would
            # not move this anyway: the flattening target is ``parent_order``
            # whatever ``mortie_order`` was. So name what is actually actionable
            # on each path.
            remedy = (
                "Re-index the catalog at a coarser order, or drop the "
                "footprint_cells column to build from geometry."
                if stored
                else (
                    f"This build covered the catalog itself -- there is no "
                    f"footprint_cells column to re-cut or drop, and the cover is "
                    f"flattened to the grid's parent_order {parent_order} whatever "
                    f"order it ran at. The bound is one granule's AOI-clipped "
                    f"footprint densified at parent_order {parent_order}: build "
                    f"against a grid with a coarser parent_order, or narrow or "
                    f"split the region so that footprint clips smaller."
                )
            )
            raise ValueError(
                f"{'footprint_cells' if stored else 'footprint cover'} batch failed "
                f"within records {own[0]}-{own[-1]} (the MOC index in the message "
                f"counts this block's {own.size} prefilter survivors inside that "
                f"range, which are generally not contiguous records): {exc}. {remedy}"
            ) from exc
        # Every slot in this block is non-empty by the predicate's contract,
        # so ``flat`` is never empty here and each owner repeats >= 1 time.
        # The ``flat.size`` guard is belt-and-suspenders against that contract
        # drifting: appending all-empty blocks clears ``_regroup_hits``'s
        # ``if not hit_shards`` gate with an empty concatenation, which trips
        # ``_first_of_run``'s documented ``size >= 1`` precondition and raises
        # ``IndexError`` there -- a failure naming the wrong function. One
        # branch per block keeps the ``{}`` no-hits returns everywhere else.
        flat = np.asarray(flat)
        owners = np.repeat(own, np.diff(flat_off))
        # No per-granule ``np.unique``: ``_regroup_hits``'s stable sort keeps
        # any repeated (shard, owner) pair adjacent, and its run dedup
        # collapses it -- same dict, same order as the scalar loop this
        # replaces. Owners stay non-decreasing across blocks (``surv`` is
        # increasing and blocks walk it in order), so the regroup's sort/dedup
        # invariant holds on the concatenation exactly as it did on one array.
        if flat.size:
            hit_shards.append(flat.astype(np.uint64, copy=False))
            hit_owners.append(owners)
    return _regroup_hits(hit_shards, hit_owners)


def _footprint_cells_plan(catalog, grid, chosen, footprint, mortie_order):
    """Inputs for the :func:`_intersect_footprint_cells` fast path, or ``None``.

    Engages only on the shape the stored column can answer *exactly*, so a build
    that takes it is the build that would have run anyway, minus the geometry
    (with one disclosed exception: a cell-budget refusal raises here where the
    geometry path drops that granule silently -- see
    :func:`_intersect_footprint_cells`):

    - the resolved backend is ``mortie`` -- the column holds mortie MOCs, and
      silently substituting them for an exact-S2 spherely run would swap the
      backend's semantics (a ~0.01% polar omission, espg/mortie#32) behind the
      caller's back;
    - ``footprint="swath"`` -- the column covers the CMR footprint, not the
      per-beam corridors ``footprint="beams"`` decomposes it into (issue #65);
    - ``mortie_order`` was not pinned by the caller -- an explicit order asks for
      a cover at *that* order, which the column cannot restate. Since issue #445
      declining here does **not** mean falling back to geometry: an indexed
      catalog with a pinned order lands on :func:`_live_cells_plan`'s ephemeral
      cover and inherits its deltas (see :meth:`ShardMap.build`'s Notes);
    - the grid is HEALPix and the catalog carries the column.

    Takes no records and materializes none (issue #439): the plan is what lets
    ``build`` intersect first and decode granules only for the rows that survive,
    so it derives its own record alignment from the catalog's row screen.

    Returns
    -------
    tuple or None
        ``(values, offsets, order, rows, considered)``. ``rows`` maps record
        index -> table row (``np.flatnonzero`` of the screen, which is exactly
        the order :meth:`~zagg.catalog.Catalog.granule_records` emits) and
        ``considered`` is the record count that alignment implies -- the
        ``total_granules`` the eager path would have reported as ``len(records)``.

    Raises
    ------
    ValueError
        When the column is present and would otherwise be used but is **coarser
        than the grid's shard order**. Answering anyway would refine every cell
        onto all ``4^(parent_order - order)`` descendants and put ~every granule
        in ~every shard -- the #92 failure the order guard exists to stop -- and
        falling through to geometry would hide that the index the operator built
        is useless for this grid. Refuse and say which order to re-index at.

        Also when the catalog has **duplicate granule ids**. Alignment is
        positional (``np.flatnonzero`` of the row screen), so a repeat cannot
        misdirect a lookup -- there is none -- but it is evidence the table was
        concatenated or re-keyed, and a column stored beside rows that repeat an
        id cannot be shown to belong to them. Assigning from it anyway would put
        one granule's footprint on another under an id that names both, silently,
        since the shard count stays plausible. Refuse rather than misassign; the
        geometry path reads each record's own ring and is unaffected. The check
        stays **table-wide**, not hits-only: it is a statement about the catalog's
        integrity, so a duplicate among rows this build's AOI would have discarded
        must refuse the build just as it did before the inversion.
    """
    if chosen != "mortie" or footprint != "swath" or mortie_order is not None:
        return None
    parent_order = getattr(grid, "parent_order", None)
    cells = getattr(catalog, "footprint_cells", None)
    cells = cells() if callable(cells) else None
    if cells is None or parent_order is None:
        return None
    values, offsets, order = cells
    if order < parent_order:
        raise ValueError(
            f"catalog's footprint_cells column is at order {order}, coarser than the "
            f"grid's parent_order {parent_order}; answering from it would upsample every "
            f"footprint onto all shards under each cell (#92). Re-index the catalog with "
            f"Catalog.index_footprints(order>={parent_order}), or build against a grid "
            f"with parent_order <= {order}."
        )
    import pyarrow.compute as pc

    # ``mode="all"`` counts a null id as its own value, matching the id ->
    # row dict this replaces (which the pre-#439 plan built from the whole
    # column, unique-or-not, purely to align records back to rows).
    distinct = pc.count_distinct(catalog.table.column("id"), mode="all").as_py()
    if distinct != catalog.table.num_rows:
        raise ValueError(
            f"catalog has duplicate granule ids ({catalog.table.num_rows - distinct} "
            f"repeats over {catalog.table.num_rows} rows); the footprint_cells column is "
            f"matched to rows by position, so a repeated id means the table was concatenated "
            f"or re-keyed and the stored footprints can no longer be shown to belong to the "
            f"rows they sit beside -- building from them could put one granule's footprint on "
            f"another. De-duplicate the catalog, or drop the column to build from geometry."
        )
    # ``granule_records`` skips rows with empty or non-polygonal geometry and
    # keeps table order otherwise, so record ``i`` is the ``i``-th surviving
    # row: the screen's ``flatnonzero`` *is* the record -> row map, and its
    # popcount is the record count, neither needing a record. Ids no longer
    # carry the alignment (they cannot -- there are no records yet), but a
    # repeat still refuses above: it means the column cannot be trusted to
    # belong to the rows it sits beside.
    rows = np.flatnonzero(catalog.granule_row_mask()).astype(np.int64, copy=False)
    return values, offsets, order, rows, int(rows.size)


def _live_cells_plan(catalog, grid, chosen, footprint, mortie_order):
    """The same inputs as :func:`_footprint_cells_plan`, covered now (issue #445).

    An *unindexed* catalog is one whose cover was never persisted -- not one that
    needs a different intersection. So the live mortie build becomes
    build-an-ephemeral-index-and-query-it: cover the WKB column with
    :meth:`~zagg.catalog.Catalog.cover_footprints` (no records, no column), then
    hand :func:`_intersect_footprint_cells` exactly what the stored path hands
    it. Records follow for the hit rows only, via the shared :func:`_hit_records`.
    On the 555,867-granule ATL03 clone against California that is 86.9 s -> ~33 s,
    and the record decode that dominated it stops running over rows the AOI
    discards (99.6% of them).

    **The cover runs at the grid's ``parent_order``**, not at
    :func:`_resolve_mortie_order`'s chunk-order default. A shard map records one
    thing -- which ``parent_order`` cells a footprint touches -- and that is
    exactly what an order-``parent_order`` cover answers; the real-catalog order
    sweep quoted in :func:`_resolve_mortie_order` (``bench/neon_order_sweep.py``)
    measures granules/shard flat for every order >= ``parent_order``, so a finer
    MOC buys precision the shard cells cannot see while coverage words per
    granule roughly double per order (``index_footprints`` prices the clone at
    ~270-420 MB at order 9 against ~9-13 GB at order 13 -- the whole-catalog o13
    cover this path would otherwise need is not materializable). espg's ruling,
    2026-08-16.

    That default is **flat by measurement and superset-safe by construction**,
    not an identity. mortie's coverage is conservative per order, so
    ``cover(parent_order)`` is a superset of
    ``moc_to_order(cover(chunk_order), parent_order)``: refining a fine cover
    down does not reproduce a coarse one, and a boundary cell the fine cover
    misses can survive at the coarse one. The direction never inverts -- a true
    granule/shard pair is never dropped, only extra ones admitted -- and at the
    orders production runs there is no difference at all: a 200-polygon
    randomized sweep measured 0/200 rows differing at parent/chunk 9/13, 11/13,
    8/12 and 9/11, against 1-2 rows and up to 0.71% extra cells at the coarse
    pairs 6/10, 5/9 and 3/7. So the clone-scale digest match at
    ``parent_order=9`` is real, and a coarse-grid build inherits the same
    conservative posture the stored index already ships (the MultiPolygon
    superset below, the AOI overhang of #101). Pinned by
    ``TestLiveCover::test_coarse_shard_order_covers_a_superset``.

    An explicit ``mortie_order`` is still **honored literally**: it is a request
    for a cover at that order, so the cover runs there whatever it costs, and is
    validated against ``parent_order`` exactly as the geometry path validates it.
    (The stored plan cannot honor a pin at all -- a persisted column cannot
    restate its order -- which is why it declines instead.) Note what that
    means for an *indexed* catalog: a pinned build there declines the stored
    plan and lands **here**, not on the geometry path it took pre-#445, so it
    covers from WKB and inherits this path's deltas. Pinned by
    ``TestLiveCover::test_a_pinned_indexed_build_covers_live_too``, and raised
    for espg's ruling on PR #447 (the alternative is gating indexed catalogs out
    of this plan to keep the records-path assignment for pinned builds).

    Engages on the same shape :func:`_footprint_cells_plan` does, minus the
    column: the backend resolves to ``mortie`` (a spherely build stays exact-S2),
    ``footprint="swath"`` (the cover is the CMR footprint, not the per-beam
    corridors of #65 -- those keep :func:`_intersect_mortie`), and the grid is
    HEALPix. Paired builds (issue #425) never reach here -- ``build`` gates them
    out, as it does for the stored plan.

    Returns
    -------
    tuple or None
        ``(values, offsets, order, rows, considered)`` -- the same contract
        :func:`_footprint_cells_plan` returns, so both feed the same
        intersection: ``offsets`` are table-row-aligned, ``rows`` maps record
        index -> table row, and ``considered`` is the screen popcount, the
        ``total_granules`` the eager path reported as ``len(records)``.
    """
    if chosen != "mortie" or footprint != "swath":
        return None
    parent_order = getattr(grid, "parent_order", None)
    cover = getattr(catalog, "cover_footprints", None)
    if parent_order is None or not callable(cover):
        return None
    # An explicit pin is resolved (and validated -- coarser than the shards still
    # raises) before the cover, so a refusal costs nothing, as on the stored path.
    order = parent_order if mortie_order is None else _resolve_mortie_order(mortie_order, grid)
    values, offsets, rows = cover(order)
    return values, offsets, order, rows, int(rows.size)


def _hit_records(catalog, rows, shard_to_idx):
    """Decode granule records for the assigned rows only (issue #439).

    The fast path intersects the stored column before any record exists, so the
    indices in ``shard_to_idx`` are positions in the records the eager path
    *would* have built. Take just the rows those indices name, decode that slice,
    and renumber the assignment onto it -- at clone scale against a regional AOI
    that is ~2.4k rows decoded instead of ~556k, which is the whole point.

    Returns
    -------
    tuple
        ``(records, shard_to_idx)`` -- records in ascending original-record
        order, and the same dict with the same keys and per-shard ordering,
        its values renumbered into ``records``.
    """
    if not shard_to_idx:
        return [], shard_to_idx
    from zagg.catalog.sources import Catalog

    hits = np.unique(np.concatenate([np.asarray(v, dtype=np.int64) for v in shard_to_idx.values()]))
    hit_rows = np.asarray(rows, dtype=np.int64)[hits]
    records = Catalog(catalog.table.take(hit_rows), dict(catalog.metadata or {})).granule_records()
    if len(records) != hits.size:
        # Unreachable while the screen and ``granule_records`` share a
        # predicate; if they ever diverge the renumbering below silently shifts
        # every granule onto its neighbour's entry, so refuse instead.
        raise ValueError(
            f"footprint_cells row screen and granule_records disagree: {hits.size} rows taken "
            f"yielded {len(records)} records. Drop the footprint_cells column to build from "
            f"geometry, and report this (issue #439)."
        )
    renum = {int(h): i for i, h in enumerate(hits)}
    return records, {k: [renum[i] for i in v] for k, v in shard_to_idx.items()}


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

    This resolves the order for the paths that cover from decoded rings --
    ``footprint="beams"``, non-HEALPix grids, :meth:`ShardMap.reproject`. The
    HEALPix ``swath`` path covers from WKB instead and defaults to
    ``parent_order`` on the same sweep evidence quoted above, calling this only
    to honor (and validate) an explicit pin -- see :func:`_live_cells_plan`.
    The sweep makes those two defaults *measurably* equal at production orders,
    not equal by construction: a cover at ``parent_order`` is a superset of the
    same footprint covered here and refined down, never a subset, and the gap is
    reachable at coarse shard orders. :func:`_live_cells_plan` carries the
    numbers.
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

        return _regroup_hits(hit_shards, hit_owners)

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
        sibling_catalog=None,
        sibling_asset: str = "l2a",
    ) -> "ShardMap":
        """Build a ShardMap from a ``Catalog`` and an output grid.

        Parameters
        ----------
        catalog : Catalog
            Fetched granule metadata (provides ``granule_records()``). A catalog
            indexed by ``Catalog.index_footprints`` additionally carries every
            granule's morton MOC, which takes the geometry-free fast path
            described in the Notes (issue #396).
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
        sibling_catalog : Catalog, optional
            Paired-asset sibling product (issue #425), e.g. the GEDI02_A
            catalog beside a GEDI01_B primary. Granules are joined at build
            time on :func:`sibling_join_key` (the shared orbital id core,
            pinned within a product generation); each paired entry carries the
            sibling's hrefs under ``assets[sibling_asset]``, spatial
            intersection runs once on the primary footprints (the products
            share them). **Pairless granules are excluded and reported**:
            ``metadata["pairless"]`` lists ``{id, missing}`` for every primary
            without a sibling, sibling without a primary, and sibling shadowed
            by an earlier record on the same join key (``"duplicate-key"``),
            and a build-time warning fires when the list is non-empty. The
            sibling catalog is taken as authoritative, so it **must be queried
            over the same AOI and time window as the primary** — a narrower
            one reports genuinely-paired acquisitions as missing and thins the
            product; past ``_PAIRLESS_ALERT_FRACTION`` of the primary catalog
            the warning escalates to name that cause. A paired build always
            takes the eager/geometry path: pairing filters the record list,
            which the ``footprint_cells`` fast path cannot follow (its
            alignment is positional over the raw table -- issue #439), so the
            stored-index plan is skipped even on an indexed catalog.
        sibling_asset : str
            Asset key the sibling's hrefs are stored under (default ``"l2a"``);
            matches the ``data_source.assets`` name the reader joins on.

        Returns
        -------
        ShardMap

        Notes
        -----
        **Indexed catalogs skip the geometry entirely** (issue #396). When the
        catalog carries the ``footprint_cells`` column, the backend resolves to
        ``mortie``, ``footprint="swath"`` and ``mortie_order`` is left at its
        default, the intersection becomes ``moc_and`` of each stored granule MOC
        with the AOI's own shard MOC -- no WKB parse, no coverage walk. The
        result is the mortie backend's, unchanged on the single-part footprints
        every CMR ATL03/06 granule has; ``metadata["footprint_cells"]`` records
        that the index answered the build. The one intended divergence is a
        **MultiPolygon** footprint, where the column is a superset: it covers
        every part, while :meth:`~zagg.catalog.Catalog.granule_records` reads
        only the largest part's exterior ring, so the index can place such a
        granule in shards the geometry path misses. A column **coarser** than the
        grid's ``parent_order`` raises rather than answering (see
        :func:`_footprint_cells_plan`).

        That path also **materializes no granule records until after the
        intersection** (issue #439). Decoding every row's WKB and asset map ran
        ~25 s of a 29.5 s build over the 555,867-granule ATL03 clone against a
        California AOI, to feed an intersection that assigned 2,357 of them; the
        stored column is already table-row-ordered, so the build intersects on it
        and then decodes only the rows it kept. The manifest is unchanged --
        ``total_granules`` still counts the records *considered* (the whole
        post-screen catalog), which :meth:`~zagg.catalog.Catalog.granule_row_mask`
        supplies without decoding any.

        **An unindexed mortie build covers first too** (issue #445). "Indexed"
        now means only that the cover was persisted: a mortie ``swath`` build on
        a HEALPix grid with no usable column covers the WKB column itself
        (:meth:`~zagg.catalog.Catalog.cover_footprints`) and runs the *same*
        intersection, instead of decoding every record to cover from its rings.
        On the clone/California case that is 86.9 s -> ~33 s. Records still
        follow the intersection, and ``total_granules`` is still the screen
        popcount. The cover runs at the grid's ``parent_order`` -- the order the
        map's shard membership is stated in, and the order beyond which the
        real-catalog sweep in :func:`_resolve_mortie_order` measures granules per
        shard flat -- so an unpinned unindexed build records
        ``mortie_order = parent_order`` where it used to record the chunk order.
        Same assignment at the orders production runs (byte-identical over the
        555,867-granule clone, and 0/200 rows differing in a randomized sweep at
        parent/chunk 9/13, 11/13, 8/12 and 9/11), and **superset-safe** in
        general: mortie's coverage is conservative per order, so the coarser
        cover can admit a boundary shard that the finer cover refined down
        misses -- up to 0.71% extra cells at 3/7 -- and never drops one. See
        :func:`_live_cells_plan`. A caller-pinned ``mortie_order`` is still
        honored literally. Unchanged paths: ``footprint="beams"`` (the cover is
        the swath, not the corridors), the spherely backend, rectilinear grids,
        and paired builds (issue #425).

        Two deltas ride on it, both inherited from the cover this path now shares
        with the index. **MultiPolygon** footprints get the same superset as
        above -- the cover takes every part where
        :meth:`~zagg.catalog.Catalog.granule_records` reads only the largest
        part's exterior ring -- so a multi-part granule may assign to more shards
        than it did pre-#445; every CMR ATL03/06 granule is single-part, so no
        production build moves. And a **null** geometry is refused by name
        (:meth:`~zagg.catalog.Catalog.granule_row_mask`) where the record loop
        raised ``AttributeError`` on the same row: both refuse, one legibly.
        Memory is the index's documented posture, screen peak included (~1 GB
        over the parquet read at clone scale, issue #429).

        **A pinned ``mortie_order`` covers live whether or not the catalog is
        indexed.** A persisted column cannot restate an arbitrary order, so the
        stored plan declines a pin (:func:`_footprint_cells_plan`) -- and since
        issue #445 what catches it is the ephemeral cover, not the geometry
        path. So an *indexed* catalog built with an explicit ``mortie_order``
        is a third population whose assignment can move: it inherits the same
        two deltas above (measured on a two-part MultiPolygon: 8 shards on the
        records path, 13 on the cover). The pin is honored either way, and
        ``metadata["footprint_cells"]`` still records ``False`` there -- it
        means "the stored column did not answer this build", which stays the
        thing the operator can act on. Flagged for review on PR #447.
        """
        if footprint not in ("swath", "beams"):
            raise ValueError(f"footprint must be 'swath' or 'beams' (got {footprint!r})")
        # Catalog-time sibling join (issue #425): pair before any intersection
        # so the geometry backends see only paired records; pairless granules
        # are excluded and reported. Pairing FILTERS the record list, which the
        # stored-index fast path cannot follow -- its alignment is positional
        # over the raw catalog table (issue #439) -- so a paired build skips
        # the plan below and takes the eager/geometry path.
        pairless: list | None = None
        paired_records: list | None = None
        if sibling_catalog is not None:
            paired_records, pairless = _pair_sibling_records(
                catalog.granule_records(), sibling_catalog.granule_records(), sibling_asset
            )
            if pairless:
                logging.warning(
                    "ShardMap.build: %d pairless granule(s) excluded from the build "
                    "(no %s sibling / no primary); e.g. %s",
                    len(pairless),
                    sibling_asset,
                    [p["id"] for p in pairless[:5]],
                )
                unpaired = sum(1 for p in pairless if p["missing"] == sibling_asset)
                n_primary = len(paired_records) + unpaired
                if n_primary and unpaired > _PAIRLESS_ALERT_FRACTION * n_primary:
                    logging.warning(
                        "ShardMap.build: %d of %d primary granules (%.0f%%) have no %s "
                        "sibling and are EXCLUDED — check that the sibling catalog was "
                        "queried over the same AOI and time window as the primary "
                        "(a mis-scoped sibling query thins the product silently)",
                        unpaired,
                        n_primary,
                        100.0 * unpaired / n_primary,
                        sibling_asset,
                    )
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

        # Fast path (issue #396): an indexed catalog already carries every
        # granule's MOC, so the build is set algebra against the AOI with no
        # geometry work. Resolved before the geometry backends because it is the
        # same mortie intersection they would run, minus the cover.
        #
        # Inside the timer, as it was before the inversion: ``build_wall_s`` has
        # always spanned the plan as well as the intersection, and the plan is
        # where the record -> row alignment lives (an id -> row dict then, the
        # ``granule_row_mask`` screen now -- 0.47 s at clone scale). Timing only
        # the intersection would make this branch's number incomparable with the
        # ``build_wall_s`` already recorded in existing manifests. Two segments
        # rather than one span because the geometry path's ``granule_records``
        # sits between them and has always been outside (issue #439). The
        # ephemeral cover (issue #445) sits in the first segment for the same
        # reason the geometry path's cover sits in the second: covering is
        # intersection work, and ``build_wall_s`` has always counted it.
        t0 = time.perf_counter()
        # A paired build never plans: pairing filtered the records, and the
        # plan's positional table alignment cannot represent that (issue #425).
        plan = (
            None
            if paired_records is not None
            else _footprint_cells_plan(catalog, grid, chosen, footprint, mortie_order)
        )
        # No stored column: cover one now and query it, rather than decoding
        # every record to cover from its rings (issue #445). Same intersection,
        # same records step -- the only difference from ``plan`` is that nothing
        # was persisted, which is why the metadata below keys off ``plan`` alone.
        live = (
            _live_cells_plan(catalog, grid, chosen, footprint, mortie_order)
            if plan is None and paired_records is None
            else None
        )
        cells = plan if plan is not None else live
        plan_wall = time.perf_counter() - t0
        # Records are the fast path's whole remaining cost -- shapely-parsing
        # every row's WKB and ``to_pylist``-ing the nested assets column ran ~25 s
        # of a 29.5 s clone-scale build, to feed an intersection that discards
        # 99.6% of them (issue #439). So decode them AFTER the intersection
        # there, for its hit rows only; every other path needs them all up front.
        if paired_records is not None:
            records = paired_records
        elif cells is not None:
            records = None
        else:
            records = catalog.granule_records()
        considered = cells[4] if cells is not None else len(records)

        t0 = time.perf_counter()
        if cells is not None:
            values, offsets, mortie_order, rows, _ = cells
            # ``stored`` only steers the cell-budget refusal's remedy: the
            # stored column can be re-cut or dropped, an ephemeral cover cannot
            # (issue #445).
            shard_to_idx = _intersect_footprint_cells(
                rows, values, offsets, grid, all_shards, stored=plan is not None
            )
        elif chosen == "mortie":
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
        wall = plan_wall + (time.perf_counter() - t0)
        if cells is not None:
            # Outside the timer on purpose: ``build_wall_s`` covers the plan (the
            # ephemeral cover included) and the intersection, and record decode
            # has never been part of it.
            records, shard_to_idx = _hit_records(catalog, rows, shard_to_idx)

        from zagg.catalog.sources import FOOTPRINT_CELLS_ORDER

        shard_keys = sorted(shard_to_idx)
        granules = [[_granule_entry(records[i]) for i in shard_to_idx[k]] for k in shard_keys]
        # The per-shard identity invariant is owned here (issue #468): refuse a
        # build whose shards name two granules the same, rather than leaving it
        # to be discovered as duplicate drift at the leaf gate.
        _refuse_basename_collisions(shard_keys, granules)
        meta = {
            **(catalog.metadata or {}),
            "backend": chosen,
            "footprint": footprint,
            "total_granules": considered,
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
        if pairless is not None:
            # Present (possibly empty) exactly when a sibling join ran, so the
            # caller can distinguish "nothing dropped" from "never joined";
            # rides the manifest JSON as part of the build result.
            meta["pairless"] = pairless
            meta["sibling_asset"] = sibling_asset
        if plan is not None or FOOTPRINT_CELLS_ORDER in meta:
            # Says whether the stored index answered *this* build, which the
            # catalog's own ``footprint_cells_order`` (carried in via the
            # metadata spread above) does not: that key only says the column
            # exists. So an indexed catalog built through the geometry path
            # (spherely, pinned order, beams) records ``False`` rather than
            # leaving the order key beside no verdict, which reads as if the
            # index had answered. A catalog with no column has neither key.
            meta["footprint_cells"] = plan is not None

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
                # Keyed on what distinguishes one granule from another, not on
                # the id alone: coarsening merges sibling shards, so a basename
                # collision that was cross-shard at the source order lands
                # in-shard here, and an id-keyed dedup would drop one of the two
                # rather than let the check below name it (issue #468). A
                # granule spanning several children still counts once.
                seen: dict = {}
                for i in groups[k]:
                    for g in self.granules[i]:
                        entry = _granule_entry(g)
                        seen[_recorded_identity(entry)[1]] = entry
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
                # Sibling assets (issue #425) live on the MAP's entries, not
                # the catalog's records — carry them through the refine so a
                # paired map stays paired at the finer order.
                sub_records = [
                    {**records_by_id[gid], **({"assets": g["assets"]} if "assets" in g else {})}
                    for gid, g in zip(ids, gran_list)
                ]
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
                        bucket[_recorded_identity(entry)[1]] = entry

            new_keys = sorted(new_granules_map)
            new_granules = [list(new_granules_map[k].values()) for k in new_keys]
            method = "refine"

        # Reproject mints NEW shard membership, so it can mint a collision the
        # source map did not have -- coarsen by merging sibling shards, refine
        # by re-intersecting -- and owns the same invariant ``build`` does (#468).
        _refuse_basename_collisions(new_keys, new_granules)

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


__all__ = ["ShardMap", "sibling_join_key"]
