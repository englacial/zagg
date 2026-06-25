"""Mortie MOC-order sweep for the shardmap builder (PR #93 / issue #92).

Benchmarks ``ShardMap.build(..., backend="mortie", mortie_order=K)`` across the
morton orders @espg requested on the PR -- [9, 12, 13, 15, 16, 17, 18] -- over
the NEON Maryland AOI (the AOI the PR evidence table used), reporting wall-time,
total granules selected, and mean granules-per-shard at each order.

OFFLINE NOTE -- this environment has no Earthdata/CMR credentials, so the real
catalog fetch is not reachable. The granule set here is a **synthetic but
representative** catalog of thin ICESat-2-style RGT tracks crossing the AOI: each
"granule" is a narrow (~90 m wide) near-polar diagonal swath, the geometry that
drove the #92 upsampling degeneracy. Absolute numbers are therefore
representative, not measured from production granules; the *relative* cost across
orders (the thing this isolates) is what the sweep compares. Run it in a
credentialed session against a real ``Catalog`` to get production absolutes.

Run::

    uv run python benchmarks/mortie_order_sweep.py
"""

from __future__ import annotations

import time

import numpy as np
import pyarrow as pa
import stac_geoparquet.arrow as sga

from zagg.catalog.shardmap import _intersect_mortie, _region_parts, _resolve_mortie_order
from zagg.catalog.sources import Catalog
from zagg.grids import HealpixGrid

# NEON Maryland AOI, the 10 km box from benchmarks/region_timing.py
# (center lon=-76.56, lat=38.89; half_lat 0.045, half_lon 0.045/cos(lat)).
_LAT, _LON = 38.89, -76.56
_HALF_LAT = 0.045
_HALF_LON = 0.045 / max(np.cos(np.radians(_LAT)), 1e-3)
AOI_BBOX = (_LON - _HALF_LON, _LAT - _HALF_LAT, _LON + _HALF_LON, _LAT + _HALF_LAT)

ORDERS = [9, 12, 13, 15, 16, 17, 18]

# Production HEALPix grid from the shipped atl03 healpix configs: parent_order 11
# shards, chunk_inner 13 inner chunks, child_order 19 leaves.
GRID = HealpixGrid(11, 19, layout="fullsphere", chunk_inner=13)


def _track_item(gid: str, frac: float, n: int = 40) -> dict:
    """A thin near-polar RGT-style swath crossing the AOI at horizontal offset ``frac``.

    The track runs roughly south->north (high inclination) and is ~90 m wide --
    the thin diagonal geometry that, at a too-coarse MOC order, upsamples onto
    every shard (#92). ``frac`` in [0, 1] places it across the AOI width.
    """
    x0, y0, x1, y1 = AOI_BBOX
    lon_c = x0 + frac * (x1 - x0)
    # ~0.02 deg inclination skew over the AOI height, ~0.0004 deg (~90 m) half-width.
    lats = np.linspace(y0, y1, n)
    skew = 0.02 * (lats - y0) / (y1 - y0)
    lon_l = lon_c - 0.0004 + skew
    lon_r = lon_c + 0.0004 + skew
    ring_lon = np.concatenate([lon_l, lon_r[::-1], lon_l[:1]])
    ring_lat = np.concatenate([lats, lats[::-1], lats[:1]])
    ring = [[float(lo), float(la)] for lo, la in zip(ring_lon, ring_lat)]
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [
            float(ring_lon.min()),
            float(ring_lat.min()),
            float(ring_lon.max()),
            float(ring_lat.max()),
        ],
        "properties": {"datetime": "2025-06-01T00:00:00Z"},
        "collection": "ATL03",
        "stac_extensions": [],
        "links": [],
        "assets": {
            "data": {"href": f"https://h/{gid}.h5", "roles": ["data"]},
            "data_s3": {"href": f"s3://b/{gid}.h5", "roles": ["data"]},
        },
    }


def _catalog(n_tracks: int = 12) -> Catalog:
    """``n_tracks`` thin swaths fanned across the AOI.

    A sparse fan (default 12) so footprints do not blanket every shard -- the
    regime where a finer MOC order can tighten footprint edges and a coarser one
    over-commits. (66, the PR evidence count, blankets this small AOI and hides
    the cross-order footprint difference; the sparse fan exposes it.)
    """
    items = [_track_item(f"G{i:03d}", i / max(n_tracks - 1, 1)) for i in range(n_tracks)]
    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "ATL03", "bbox": list(AOI_BBOX)},
    )


def main() -> None:
    catalog = _catalog()
    n_granules = len(catalog.granule_records())
    print(f"AOI bbox: {AOI_BBOX}")
    print(
        f"grid: parent_order={GRID.parent_order} chunk_order={GRID.chunk_order} "
        f"child_order={GRID.child_order}"
    )
    print(f"synthetic granules in catalog: {n_granules}\n")

    records = catalog.granule_records()
    parts = _region_parts(None, catalog.metadata)
    all_shards = set(int(s) for s in GRID.coverage(parts))
    print(f"AOI shards (order {GRID.parent_order}) covering the box: {len(all_shards)}\n")

    # Order at or below parent_order trips the #92 guard in the real builder; the
    # sweep still *measures* what those orders would do by calling the intersection
    # directly, flagging which orders the production guard rejects.
    rows = []
    for order in ORDERS:
        guard_ok = True
        try:
            _resolve_mortie_order(order, GRID)
        except ValueError:
            guard_ok = False
        t0 = time.perf_counter()
        shard_to_idx = _intersect_mortie(records, GRID, all_shards, order=order)
        wall = time.perf_counter() - t0
        n_shards = len(shard_to_idx)
        pairs = sum(len(v) for v in shard_to_idx.values())
        gps = pairs / n_shards if n_shards else 0.0
        sel = len({i for v in shard_to_idx.values() for i in v})
        rows.append((order, wall, sel, n_shards, gps, guard_ok))
        flag = "" if guard_ok else "  (REJECTED by #92 guard: order < parent_order)"
        print(
            f"order={order:>2}  wall={wall:7.3f}s  granules_selected={sel:>3}  "
            f"shards={n_shards:>5}  granules/shard={gps:7.2f}{flag}"
        )

    print("\n| morton order | run time (s) | granules selected | granules/shard (mean) | guard |")
    print("|---|---|---|---|---|")
    for order, wall, sel, _n, gps, guard_ok in rows:
        state = "ok" if guard_ok else "rejected (<parent_order)"
        print(f"| {order} | {wall:.3f} | {sel} | {gps:.2f} | {state} |")


if __name__ == "__main__":
    main()
