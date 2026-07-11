#!/usr/bin/env python
"""Shard-MOC sizing measurement -- issue #202 item (6), decides #200 O8.

For the per-shard cell-level coverage MOC that rides the D4 commit stamp (#200),
this measures whether a byte-budgeted coarsen-to-fit MOC (a) as JSON nested-range
pairs beats (b) a snap relative-depth bitmap, and picks the budget constant. It
reads real ATL03 coordinates for the pinned SERC/NEON shard and the 88S
worst-case shards, folds each point to its order-19 morton cell (the worker's own
``geo2mort`` path), and characterizes the coverage MOC at every relative depth.

Pure analysis lives in the top half (importable, unit-tested in
``tests/test_moc_sizing.py``); the coordinate I/O and orchestration in the bottom
half run only under ``__main__``.

Representations measured (both sized *as they sit in* a JSON ``zarr.json`` attr):
  (i)  JSON nested-range pairs with **string** endpoints -- the O1 convention
       (packed u64 > 2^53, so range endpoints must be decimal strings). A range
       is a maximal contiguous run of the coverage over the order-19 morton curve.
  (ii) the packed compressed-MOC word array, base64-encoded into the attr.

Tiers (per the #200 tiered-metadata addendum):
  tier 0  morton box   -- minimal <=4-member cover (the 32 B baseline the budget
                          tiers must beat).
  tier 1  budgeted MOC -- coarsen-to-fit ranges at a byte budget.
  bitmap  alternative  -- 4^d-bit occupancy raster, zstd-compressed.

Budgets swept (issue #202 correction comment): 1, 4, 16, 64, 256 KB, 1 MB, and
exact/uncapped. No anchor budget -- the box baseline and over-coverage curves
drive the recommendation.

Read caveat (surfaced, not hidden): ATL03 granules are global; extracting a tiny
shard means reading whole files over NSIDC HTTPS byte-range (EDL auth via
~/.netrc), which is ~minutes/granule from outside us-west-2. Occupancy is built
from the **segment reference photons** (``geolocation/reference_photon_lat/lon``,
~20 m sampling) rather than every ~0.7 m photon -- ~150x less data, so the full
SERC granule set is readable. This mildly *under*-counts the order-19 cell set
(hence under-states MOC size), reported honestly. The 88S worst-case granule
lists (5,620 / 4,605 granules) cannot be read whole in reasonable time; those are
a stratified **sample** with a convergence curve, flagged as a lower bound.

Usage:
    uv run python .github/scripts/moc_sizing.py --out metrics_moc_sizing.json
    uv run python .github/scripts/moc_sizing.py --shard serc
    uv run python .github/scripts/moc_sizing.py --shard 88s --sample-88s 120
    uv run python .github/scripts/moc_sizing.py --dry-run   # offline, synthetic
"""

from __future__ import annotations

import argparse
import base64
import json
import time
from dataclasses import dataclass, field

import numpy as np

# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

CHILD_ORDER = 19  # ICESat-2 ~10 m cell; the coverage domain (mortie 0.9.0 lifts
# the old MAX_DEPTH=18 ceiling, so order 19 is representable in a MOC).

# Packed-word increment between two morton-adjacent cells at order 19. Verified
# empirically: an ancestor's order-19 descendants form a contiguous arithmetic
# run with this step, so any (mixed-order) MOC member maps to one order-19
# interval [lo, lo + (4**(19-order)-1)*STEP19]. This is what makes the
# nested-range (interval) form exact and cheap to build.
STEP19 = 4_194_304  # == 2**22

# Byte budgets for the coarsen-to-fit sweep (issue #202 correction). ``None`` is
# the exact/uncapped point.
BUDGETS: list[int | None] = [
    1 * 1024,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    None,
]


def budget_label(b: int | None) -> str:
    if b is None:
        return "exact"
    if b >= 1024 * 1024:
        return f"{b // (1024 * 1024)}MB"
    return f"{b // 1024}KB"


# ---------------------------------------------------------------------------
# pure analysis (no network, unit-tested)
# ---------------------------------------------------------------------------


def coarsen(occ19: np.ndarray, order: int) -> np.ndarray:
    """Occupied order-19 cells -> unique occupied cells at ``order`` (<=19)."""
    from mortie import clip2order

    occ = np.asarray(occ19, dtype=np.uint64)
    if order == CHILD_ORDER:
        return np.unique(occ)
    return np.unique(clip2order(order, occ))


def moc_at_order(occ19: np.ndarray, order: int) -> np.ndarray:
    """Canonical compressed MOC of the occupancy coarsened to ``order``."""
    from mortie import compress_moc

    return np.asarray(compress_moc(coarsen(occ19, order)), dtype=np.uint64)


def _member_order(word: int) -> int:
    from mortie import infer_order_from_morton

    return int(infer_order_from_morton(int(word)))


def _first_leaf(word: int, order: int, target: int = CHILD_ORDER) -> int:
    """Smallest order-``target`` descendant of a cell at ``order`` (first-child)."""
    from mortie import generate_morton_children

    w = int(word)
    o = order
    while o < target:
        w = min(int(x) for x in generate_morton_children(w, o + 1))
        o += 1
    return w


def moc_intervals(moc_words: np.ndarray) -> list[tuple[int, int]]:
    """Compressed (mixed-order) MOC -> merged order-19 ``(lo, hi)`` intervals.

    Each member covers a contiguous order-19 run; morton-adjacent members merge,
    so the interval count can be well below the member count -- exactly the
    compaction the nested-range form buys over the packed member array.
    """
    words = np.asarray(moc_words, dtype=np.uint64)
    spans: list[tuple[int, int]] = []
    for w in words:
        order = _member_order(int(w))
        lo = _first_leaf(int(w), order)
        hi = lo + (4 ** (CHILD_ORDER - order) - 1) * STEP19
        spans.append((lo, hi))
    spans.sort()
    merged: list[tuple[int, int]] = []
    for lo, hi in spans:
        if merged and lo <= merged[-1][1] + STEP19:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def intervals_json(intervals: list[tuple[int, int]]) -> str:
    """O1 wire form: JSON array of ``["lo", "hi"]`` decimal-string pairs."""
    return json.dumps([[str(lo), str(hi)] for lo, hi in intervals], separators=(",", ":"))


def json_ranges_size(moc_words: np.ndarray) -> tuple[int, int]:
    """(serialized-byte size, range count) of the O1 nested-range form."""
    ivals = moc_intervals(moc_words)
    return len(intervals_json(ivals).encode()), len(ivals)


def packed_sizes(moc_words: np.ndarray) -> tuple[int, int]:
    """(raw packed bytes, base64-in-attr bytes) of the member array."""
    raw = np.asarray(moc_words, dtype=np.uint64).tobytes()
    b64 = base64.b64encode(raw)
    return len(raw), len(b64) + 2  # +2 for the JSON string quotes


def covered_leaf_count(moc_words: np.ndarray) -> int:
    """Order-19 cells the MOC covers (conservative superset area)."""
    total = 0
    for w in np.asarray(moc_words, dtype=np.uint64):
        total += 4 ** (CHILD_ORDER - _member_order(int(w)))
    return int(total)


def over_coverage(moc_words: np.ndarray, n_true: int) -> float:
    """Covered order-19 cells / truly-occupied order-19 cells (>= 1.0)."""
    return covered_leaf_count(moc_words) / n_true if n_true else float("nan")


def morton_box(occ19: np.ndarray, shard_order: int) -> tuple[np.ndarray, int]:
    """Tier-0 <=4-member cover: the deepest common ancestor's intersecting
    children (#200 addendum construction). Returns (box words, box order)."""
    from mortie import clip2order

    occ = np.asarray(occ19, dtype=np.uint64)
    a = shard_order
    while a < CHILD_ORDER:
        if len(np.unique(clip2order(a + 1, occ))) > 1:
            break
        a += 1
    box_order = min(a + 1, CHILD_ORDER)
    box = np.unique(coarsen(occ, box_order))
    return box, box_order


def edge_length_m(order: int) -> float:
    """Cell-edge length in metres at ``order`` (mortie ``order2res`` is km) --
    the STAC simplification-tolerance analog for the achieved depth."""
    from mortie import order2res

    return float(order2res(order)) * 1000.0


def _compress(buf: bytes) -> tuple[bytes, str]:
    """Compress with zstd (pyarrow's codec -- an existing extra, no new dep); fall
    back to stdlib zlib-9 where pyarrow is absent (e.g. the CI ``test`` env). zlib
    runs ~5-10% larger than zstd on these sparse rasters, so it is a conservative
    stand-in and the name used is recorded in the output."""
    try:
        import pyarrow as pa

        return pa.Codec("zstd", compression_level=19).compress(buf), "zstd-19"
    except Exception:  # noqa: BLE001 -- pyarrow optional; zlib is the fallback
        import zlib

        return zlib.compress(buf, 9), "zlib-9"


def bitmap_sizes(occ19: np.ndarray, shard_key: int, shard_order: int, order: int) -> dict:
    """Raw and compressed sizes of the 4^d occupancy raster at ``order``.

    Bit i is set where the shard's i-th order-``order`` cell (morton order) is
    occupied -- the snap relative-depth bitmap the ranges compete against.
    """
    d = order - shard_order
    n_cells = 4**d
    cells = coarsen(occ19, order)
    first = _first_leaf(int(shard_key), shard_order, order)
    step = STEP19 * 4 ** (CHILD_ORDER - order)
    idx = ((cells.astype(object) - first) // step).astype(np.int64)
    bits = np.zeros(n_cells, dtype=bool)
    bits[idx] = True
    packed = np.packbits(bits).tobytes()
    comp, codec = _compress(packed)
    return {
        "order": order,
        "depth": d,
        "n_set": int(bits.sum()),
        "raw_bytes": len(packed),
        "zstd_bytes": len(comp),
        "codec": codec,
    }


def sibling_union_us(occ19: np.ndarray, shard_order: int) -> float:
    """Time a union of the 4 order-(shard+1) sibling sub-MOCs (composition step)."""
    from mortie import clip2order, moc_or

    occ = np.asarray(occ19, dtype=np.uint64)
    labels = clip2order(shard_order + 1, occ)
    subs = [moc_at_order(occ[labels == key], CHILD_ORDER) for key in np.unique(labels)]
    if len(subs) < 2:
        return 0.0
    t0 = time.perf_counter()
    acc = subs[0]
    for s in subs[1:]:
        acc = moc_or(acc, s)
    return (time.perf_counter() - t0) * 1e6


# --- leaf zarr.json carrier accounting -------------------------------------

# A representative zagg leaf group ``zarr.json`` sans the MOC payload: zarr v3
# group metadata plus the commit stamp attrs every metadata open GETs. The MOC
# rides ``coverage`` in attrs; the size of that string is what the budget buys
# against the per-open GET cost.
_LEAF_BASE_ATTRS = {
    "zagg": {
        "spec": "morton-hive/1",
        "grid": {"type": "healpix", "indexing_scheme": "nested", "child_order": 19},
        "shard": {"parent_order": 9, "chunk_inner": 13, "sharded": True},
    },
    "commit_stamp": {
        "spec": "morton-moc/1",
        "generated_at": "2026-07-09T00:00:00Z",
        "source": "worker",
        "run_id": "00000000-0000-0000-0000-000000000000",
        "morton_box": ["", "", "", ""],
        "achieved_depth": 0,
    },
}


def leaf_zarr_json_size(coverage_attr: str | None) -> int:
    """Bytes of a leaf ``zarr.json`` carrying ``coverage_attr`` in the stamp.

    ``None`` models the carrier fork's sidecar case -- the MOC is its own object
    and attrs hold only the box + depth + a pointer.
    """
    attrs = json.loads(json.dumps(_LEAF_BASE_ATTRS))
    if coverage_attr is None:
        attrs["commit_stamp"]["coverage_ref"] = "coverage.moc.b64"
    else:
        attrs["commit_stamp"]["coverage"] = coverage_attr
    doc = {"zarr_format": 3, "node_type": "group", "attributes": attrs}
    return len(json.dumps(doc, separators=(",", ":")).encode())


def coarsen_to_fit(occ19: np.ndarray, shard_order: int, depths: list["DepthRow"]) -> list[dict]:
    """Deepest order whose O1 nested-range MOC fits each budget + its cost."""
    budgets: list[dict] = []
    for b in BUDGETS:
        chosen = None
        for row in reversed(depths):  # deepest first
            if b is None or row.ranges_bytes <= b:
                chosen = row
                break
        if chosen is None:
            budgets.append({"budget": budget_label(b), "budget_bytes": b, "fits": False})
            continue
        cov_attr = intervals_json(moc_intervals(moc_at_order(occ19, chosen.order)))
        budgets.append(
            {
                "budget": budget_label(b),
                "budget_bytes": b,
                "fits": True,
                "achieved_order": chosen.order,
                "achieved_depth": chosen.depth,
                "ranges_bytes": chosen.ranges_bytes,
                "ranges_count": chosen.ranges_count,
                "over_coverage": chosen.over_coverage,
                "edge_m": chosen.edge_m,
                "leaf_zarr_json_bytes": leaf_zarr_json_size(cov_attr),
            }
        )
    return budgets


# ---------------------------------------------------------------------------
# depth sweep for one occupancy
# ---------------------------------------------------------------------------


@dataclass
class DepthRow:
    order: int
    depth: int
    n_cells: int
    moc_members: int
    ranges_bytes: int
    ranges_count: int
    packed_raw: int
    packed_b64: int
    over_coverage: float
    edge_m: float
    bitmap_raw: int
    bitmap_zstd: int


@dataclass
class ShardResult:
    name: str
    shard_key: int
    shard_order: int
    n_granules_total: int
    n_granules_read: int
    sampled: bool
    n_points: int
    n_cells_19: int
    box_members: int
    box_order: int
    box_over_coverage: float
    box_bytes: int
    build_ms: float
    union_us: float
    convergence: list[dict] = field(default_factory=list)
    depths: list[DepthRow] = field(default_factory=list)
    budgets: list[dict] = field(default_factory=list)


def analyze_occupancy(
    name,
    shard_key,
    shard_order,
    occ19,
    *,
    n_granules_total,
    n_granules_read,
    sampled,
    n_points,
    convergence=None,
) -> ShardResult:
    """Full per-shard MOC characterization from an occupied order-19 cell set."""
    occ19 = np.unique(np.asarray(occ19, dtype=np.uint64))
    n_cells_19 = int(occ19.size)

    box, box_order = morton_box(occ19, shard_order)
    box_rng_bytes, _ = json_ranges_size(box)

    # worker-side build cost: in-memory morton array -> serialized payload
    t0 = time.perf_counter()
    exact_moc = moc_at_order(occ19, CHILD_ORDER)
    _ = intervals_json(moc_intervals(exact_moc))
    build_ms = (time.perf_counter() - t0) * 1000.0

    union_us = sibling_union_us(occ19, shard_order)

    depths: list[DepthRow] = []
    for order in range(shard_order + 3, CHILD_ORDER + 1):
        moc = moc_at_order(occ19, order)
        rng_bytes, rng_count = json_ranges_size(moc)
        praw, pb64 = packed_sizes(moc)
        bmp = bitmap_sizes(occ19, shard_key, shard_order, order)
        depths.append(
            DepthRow(
                order=order,
                depth=order - shard_order,
                n_cells=int(coarsen(occ19, order).size),
                moc_members=int(moc.size),
                ranges_bytes=rng_bytes,
                ranges_count=rng_count,
                packed_raw=praw,
                packed_b64=pb64,
                over_coverage=over_coverage(moc, n_cells_19),
                edge_m=edge_length_m(order),
                bitmap_raw=bmp["raw_bytes"],
                bitmap_zstd=bmp["zstd_bytes"],
            )
        )

    res = ShardResult(
        name=name,
        shard_key=shard_key,
        shard_order=shard_order,
        n_granules_total=n_granules_total,
        n_granules_read=n_granules_read,
        sampled=sampled,
        n_points=n_points,
        n_cells_19=n_cells_19,
        box_members=int(box.size),
        box_order=box_order,
        box_over_coverage=over_coverage(box, n_cells_19),
        box_bytes=box_rng_bytes,
        build_ms=build_ms,
        union_us=union_us,
        convergence=convergence or [],
        depths=depths,
    )
    res.budgets = coarsen_to_fit(occ19, shard_order, depths)
    return res


# ---------------------------------------------------------------------------
# coordinate I/O (network; __main__ only)
# ---------------------------------------------------------------------------

BEAMS = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]
_SOURCE_PATHS = {
    "reference": ("geolocation/reference_photon_lat", "geolocation/reference_photon_lon"),
    "photons": ("heights/lat_ph", "heights/lon_ph"),
}


def _edl_token() -> str:
    import earthaccess

    auth = earthaccess.login(strategy="netrc")
    if not auth.authenticated:
        raise SystemExit("earthdata login failed (need ~/.netrc)")
    return auth.token["access_token"]


def read_granule_cells(https_url, token, shard_key, shard_order, source="reference"):
    """Read one granule's coordinates, fold to order-19 cells, keep the shard's.

    Returns (unique in-shard order-19 cells, in-shard point count).
    """
    from h5coro import h5coro, webdriver
    from mortie import clip2order, geo2mort

    lat_ds, lon_ds = _SOURCE_PATHS[source]
    h5 = h5coro.H5Coro(https_url, webdriver.HTTPDriver, credentials=token, errorChecking=True)
    paths = []
    for g in BEAMS:
        paths += [f"{g}/{lat_ds}", f"{g}/{lon_ds}"]
    promise = h5.readDatasets(paths, block=True)
    keep: list[np.ndarray] = []
    n_pts = 0
    for g in BEAMS:
        try:
            lat = np.asarray(promise[f"{g}/{lat_ds}"])
            lon = np.asarray(promise[f"{g}/{lon_ds}"])
        except Exception:  # noqa: BLE001 -- a missing/short beam is skipped
            continue
        if lat.size == 0 or lat.size != lon.size:
            continue
        good = np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90.0)
        lat, lon = lat[good], lon[good]
        if lat.size == 0:
            continue
        cells = geo2mort(lat, lon, order=CHILD_ORDER)
        in_shard = clip2order(shard_order, cells) == np.uint64(shard_key)
        n_pts += int(in_shard.sum())
        keep.append(np.unique(cells[in_shard]))
    if not keep:
        return np.empty(0, dtype=np.uint64), 0
    return np.unique(np.concatenate(keep)), n_pts


def _stratified(granules, n):
    if not n or n >= len(granules):
        return granules
    idx = np.linspace(0, len(granules) - 1, n).round().astype(int)
    return [granules[i] for i in sorted(set(idx.tolist()))]


def collect_occupancy(
    granules,
    shard_key,
    shard_order,
    token,
    *,
    sample,
    workers,
    source="reference",
    per_read_timeout=240,
):
    """Read (a sample of) granules -> (occ19, n_read, n_points, convergence)."""
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FTimeout

    chosen = _stratified(granules, sample)
    occ: set[int] = set()
    convergence: list[dict] = []
    n_read = 0
    n_points = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(read_granule_cells, g["https"], token, shard_key, shard_order, source): g
            for g in chosen
        }
        for i, (fut, g) in enumerate(futs.items(), 1):
            try:
                cells, pts = fut.result(timeout=per_read_timeout)
            except (FTimeout, Exception) as exc:  # noqa: BLE001 -- log + continue
                print(f"  [{i}/{len(chosen)}] {g['id']}: FAILED ({type(exc).__name__})")
                continue
            n_read += 1
            n_points += pts
            occ.update(int(x) for x in cells)
            convergence.append({"granules_read": n_read, "cells_19": len(occ)})
            print(f"  [{i}/{len(chosen)}] {g['id']}: +{cells.size} cells -> {len(occ)} total")
    return np.array(sorted(occ), dtype=np.uint64), n_read, n_points, convergence


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

_O9_88S_KEY = 11530494877603201033
_O10_88S_KEY = 11530494877603201034


def _synthetic_occ(shard_key, shard_order):
    """A track-like occupancy inside the shard for --dry-run smoke tests."""
    from mortie import generate_morton_children

    leaves = np.sort(
        np.asarray(generate_morton_children(int(shard_key), CHILD_ORDER), dtype=np.uint64)
    )
    rng = np.random.RandomState(0)
    idx = np.unique(rng.randint(0, leaves.size, size=min(3000, leaves.size)))
    return leaves[idx]


def _result_to_dict(r: ShardResult) -> dict:
    return {
        "name": r.name,
        "shard_key": r.shard_key,
        "shard_order": r.shard_order,
        "n_granules_total": r.n_granules_total,
        "n_granules_read": r.n_granules_read,
        "sampled": r.sampled,
        "n_points": r.n_points,
        "n_cells_19": r.n_cells_19,
        "morton_box": {
            "members": r.box_members,
            "order": r.box_order,
            "over_coverage": r.box_over_coverage,
            "ranges_bytes": r.box_bytes,
        },
        "build_ms": r.build_ms,
        "sibling_union_us": r.union_us,
        "convergence": r.convergence,
        "depths": [vars(d) for d in r.depths],
        "budgets": r.budgets,
    }


def _print_shard(r: ShardResult) -> None:
    tag = (
        f"SAMPLED {r.n_granules_read}/{r.n_granules_total}"
        if r.sampled
        else f"full {r.n_granules_read}/{r.n_granules_total}"
    )
    print(f"  {r.n_cells_19} occupied order-19 cells, {r.n_points} in-shard points ({tag})")
    print(
        f"  morton box: {r.box_members} members @ order {r.box_order}, "
        f"over-coverage {r.box_over_coverage:.1f}x, {r.box_bytes} B"
    )
    print(f"  build {r.build_ms:.1f} ms | sibling-union {r.union_us:.1f} us")
    print("  d ord  cells   moc  rng(B) rng# pkb64 overcov edge(m) bmp_raw bmp_zstd")
    for d in r.depths:
        print(
            f"  {d.depth:2d} {d.order:3d} {d.n_cells:6d} {d.moc_members:5d} "
            f"{d.ranges_bytes:6d} {d.ranges_count:4d} {d.packed_b64:5d} "
            f"{d.over_coverage:6.2f} {d.edge_m:6.1f} {d.bitmap_raw:7d} {d.bitmap_zstd:7d}"
        )
    print("  budget fit_ord depth rng(B) rng# overcov edge(m) leaf_zarr.json(B)")
    for b in r.budgets:
        if not b.get("fits"):
            print(f"  {b['budget']:6s} (no depth fits)")
            continue
        print(
            f"  {b['budget']:6s} {b['achieved_order']:7d} {b['achieved_depth']:5d} "
            f"{b['ranges_bytes']:6d} {b['ranges_count']:4d} {b['over_coverage']:6.2f} "
            f"{b['edge_m']:7.1f} {b['leaf_zarr_json_bytes']:8d}"
        )


def run(args) -> None:
    bench = "tests/data/benchmark"
    targets = json.load(open(f"{bench}/targets.json"))
    token = None if args.dry_run else _edl_token()

    jobs = []
    if args.shard in ("serc", "all"):
        jobs.append(
            (
                "serc_o9",
                f"{bench}/shardmaps/sm_healpix_o9.json",
                targets["shardmaps"]["healpix_o9"]["shard_key"],
                9,
                None,
            )
        )
    if args.shard in ("88s", "all"):
        jobs.append(
            ("88s_o9", f"{bench}/shardmaps/sm_healpix_o9_88s.json", _O9_88S_KEY, 9, args.sample_88s)
        )
        jobs.append(
            (
                "88s_o10",
                f"{bench}/shardmaps/sm_healpix_o10_88s.json",
                _O10_88S_KEY,
                10,
                args.sample_88s,
            )
        )

    results: list[ShardResult] = []
    o9_occ_cache: dict[int, np.ndarray] = {}

    for name, sm_path, shard_key, shard_order, sample in jobs:
        print(f"\n=== {name} (shard {shard_key}, order {shard_order}) ===")
        with open(sm_path) as fh:
            sm = json.load(fh)
        # ``granules`` is a list-of-lists aligned to ``shard_keys`` (one granule
        # list per shard); pick this shard's list.
        granules = sm["granules"][sm["shard_keys"].index(shard_key)]
        n_total = len(granules)

        if name == "88s_o10" and _O9_88S_KEY in o9_occ_cache:
            from mortie import clip2order

            o9cells = o9_occ_cache[_O9_88S_KEY]
            occ = o9cells[clip2order(10, o9cells) == np.uint64(shard_key)]
            prev = results[-1]
            n_read, sampled, conv, n_pts = prev.n_granules_read, prev.sampled, [], 0
            print(f"  derived from o9 read pass: {occ.size} cells in o10 shard")
        elif args.dry_run:
            occ = _synthetic_occ(shard_key, shard_order)
            n_read, sampled, conv, n_pts = 0, False, [], int(occ.size)
        else:
            occ, n_read, n_pts, conv = collect_occupancy(
                granules,
                shard_key,
                shard_order,
                token,
                sample=sample,
                workers=args.workers,
                source=args.source,
            )
            sampled = bool(sample and sample < n_total)
            if name == "88s_o9":
                o9_occ_cache[_O9_88S_KEY] = occ

        if occ.size == 0:
            print("  no occupancy -- skipping")
            continue

        res = analyze_occupancy(
            name,
            shard_key,
            shard_order,
            occ,
            n_granules_total=n_total,
            n_granules_read=n_read,
            sampled=sampled,
            n_points=n_pts,
            convergence=conv,
        )
        results.append(res)
        _print_shard(res)

    from importlib.metadata import version as _pkg_version

    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mortie_version": _pkg_version("mortie"),
        "occupancy_source": args.source,
        "resolution_note": (
            "occupancy folded from segment reference photons (~20 m); a mild "
            "under-count of the full-photon order-19 cell set, so MOC sizes are a "
            "lower bound"
            if args.source == "reference"
            else "full photon occupancy"
        ),
        "compressor": _compress(b"\x00" * 64)[1],
        "budgets": [budget_label(b) for b in BUDGETS],
        "shards": [_result_to_dict(r) for r in results],
    }
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2, default=str)
    print(f"\nwrote {args.out} ({len(results)} shards)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shard", choices=["serc", "88s", "all"], default="all")
    ap.add_argument(
        "--sample-88s",
        type=int,
        default=None,
        help="cap 88S granule reads to N (stratified); omit to read all",
    )
    ap.add_argument("--workers", type=int, default=6, help="concurrent granule reads")
    ap.add_argument("--source", choices=["reference", "photons"], default="reference")
    ap.add_argument("--out", default="metrics_moc_sizing.json")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="no network; synthetic occupancy to exercise the pipeline",
    )
    run(ap.parse_args())


if __name__ == "__main__":
    main()
