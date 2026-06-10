"""Catalog drift harness for the #24 refactor.

Tracks how the (shard, granule) mapping drifts across three points in time:

  - **old**    : a pre-refactor catalog built with buggy mortie (historical).
  - **recent** : current code, pre-hard-break (mortie 0.7.2 MOC / spherely).
  - **new**    : the post-refactor output.

This is *information, not an oracle* -- spherely (exact S2) is the geometric
reference, but mortie carries a known ~0.01% polar omission (gh espg/mortie#32),
so we expect drift to shrink, not vanish, and never expect byte identity.

Two subcommands::

    # Snapshot a baseline from the cached cycle-22 granule pull:
    python bench/drift_catalog.py build --backend mortie \
        --out bench/drift/recent_cycle22_atl06_order6_mortie.json

    # Compare any two catalogs (old-dict or new shard_keys/granules format):
    python bench/drift_catalog.py compare A.json B.json --label-a old --label-b recent
"""
from __future__ import annotations

import argparse
import json
import pickle
import time
from pathlib import Path

# Cached CMR pull produced by bench/verify_spherely_cycle22.py.
CACHE = Path("/tmp/cmr_cycle22_atl06.pkl")
PARENT_ORDER = 6


# ── catalog loading (format-agnostic) ────────────────────────────────────────

def _basename(url: str) -> str:
    """Granule identity, independent of s3://, https://, or bucket prefix."""
    return url.rstrip("/").rsplit("/", 1)[-1]


def _gran_basename(g) -> str:
    """Granule identity from either a URL string or a {id,s3,https} record."""
    if isinstance(g, dict):
        return _basename(g.get("s3") or g.get("https") or g.get("id"))
    return _basename(g)


def load_pairs(path: str) -> dict[int, set[str]]:
    """Load a catalog as ``{shard_key: {granule_basename, ...}}``.

    Accepts both the legacy ``{"catalog": {str_key: [urls]}}`` format and the
    current ``{"shard_keys": [...], "granules": [[urls], ...]}`` format.

    Parameters
    ----------
    path : str
        Path to a catalog JSON file.

    Returns
    -------
    dict[int, set[str]]
        Shard key -> set of granule basenames.
    """
    d = json.loads(Path(path).read_text())
    out: dict[int, set[str]] = {}
    if "shard_keys" in d and "granules" in d:
        for k, urls in zip(d["shard_keys"], d["granules"]):
            out[int(k)] = {_gran_basename(u) for u in urls}
    elif "catalog" in d:
        for k, urls in d["catalog"].items():
            out[int(k)] = {_gran_basename(u) for u in urls}
    else:
        raise ValueError(f"{path}: unrecognized catalog format")
    return out


def _pair_set(cat: dict[int, set[str]]) -> set[tuple[int, str]]:
    return {(s, g) for s, gs in cat.items() for g in gs}


# ── compare ──────────────────────────────────────────────────────────────────

def compare(path_a: str, path_b: str, label_a: str, label_b: str) -> dict:
    """Diff two catalogs and return a summary dict.

    ``label_b`` is treated as the reference: ``{a}_only`` pairs are commission
    (a has, b lacks) and ``{b}_only`` pairs are omission (a misses what b has).

    Parameters
    ----------
    path_a, path_b : str
        Catalog JSON paths.
    label_a, label_b : str
        Short labels for reporting.

    Returns
    -------
    dict
        Shard- and pair-level overlap/omission/commission counts.
    """
    a, b = load_pairs(path_a), load_pairs(path_b)
    ak, bk = set(a), set(b)
    ap, bp = _pair_set(a), _pair_set(b)
    common = ap & bp
    denom = max(len(ap), len(bp), 1)
    return {
        f"{label_a}_shards": len(ak),
        f"{label_b}_shards": len(bk),
        "shards_common": len(ak & bk),
        f"{label_a}_only_shards": len(ak - bk),
        f"{label_b}_only_shards": len(bk - ak),
        f"{label_a}_pairs": len(ap),
        f"{label_b}_pairs": len(bp),
        "pairs_common": len(common),
        f"{label_a}_only_pairs": len(ap - bp),
        f"{label_b}_only_pairs": len(bp - ap),
        "overlap_pct": round(100 * len(common) / denom, 4),
    }


def _print_compare(summary: dict, label_a: str, label_b: str) -> None:
    print(f"\n=== {label_a}  vs  {label_b} (reference) ===")
    width = max(len(k) for k in summary)
    for k, v in summary.items():
        print(f"  {k:<{width}} : {v}")
    a_only = summary[f"{label_a}_only_pairs"]
    b_only = summary[f"{label_b}_only_pairs"]
    print(
        f"  -> commission ({label_a} extra): {a_only:,}   "
        f"omission ({label_a} missing): {b_only:,}   "
        f"overlap: {summary['overlap_pct']}%"
    )


# ── build ─────────────────────────────────────────────────────────────────────

def _catalog_from_umm(granules):
    """Build a Catalog from cached raw UMM-JSON granules.

    Lets ``build`` run on the *same* granule set as the pre-refactor baselines
    (cached from the legacy UMM CMR pull), so new-vs-recent drift reflects the
    algorithm, not a different granule set.
    """
    import pyarrow as pa
    import stac_geoparquet.arrow as sga

    from zagg.catalog.sources import Catalog

    items = []
    for g in granules:
        umm = g.get("umm", {})
        s3 = next((u["URL"] for u in umm.get("RelatedUrls", [])
                   if u.get("URL", "").startswith("s3://") and u["URL"].endswith(".h5")), None)
        gpoly = (umm.get("SpatialExtent", {}).get("HorizontalSpatialDomain", {})
                 .get("Geometry", {}).get("GPolygons", []))
        if not s3 or not gpoly:
            continue
        pts = gpoly[0].get("Boundary", {}).get("Points", [])
        ring = [[p["Longitude"], p["Latitude"]] for p in pts if "Latitude" in p]
        if len(ring) < 3:
            continue
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        lons = [c[0] for c in ring]
        lats = [c[1] for c in ring]
        items.append({
            "type": "Feature", "stac_version": "1.0.0", "id": umm.get("GranuleUR", ""),
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "bbox": [min(lons), min(lats), max(lons), max(lats)],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "collection": "ATL06", "stac_extensions": [], "links": [],
            "assets": {"data_s3": {"href": s3, "roles": ["data"]}},
        })
    return Catalog(pa.table(sga.parse_stac_items_to_arrow(items)), {})


def build(backend: str, out: str, *, mortie_order: int = 8) -> None:
    """Build an order-6 fullsphere cycle-22 ShardMap from the cached granule pull."""
    import logging

    logging.disable(logging.INFO)
    from zagg.catalog import load_antarctic_basins
    from zagg.catalog.shardmap import ShardMap
    from zagg.grids import HealpixGrid

    if not CACHE.exists():
        raise SystemExit(
            f"No cached granule pull at {CACHE}. Run "
            f"bench/verify_spherely_cycle22.py first to populate it."
        )
    granules = pickle.loads(CACHE.read_bytes())
    print(f"[build] backend={backend} granules={len(granules)} ...")
    cat = _catalog_from_umm(granules)
    grid = HealpixGrid(PARENT_ORDER, PARENT_ORDER, layout="fullsphere")
    t0 = time.perf_counter()
    sm = ShardMap.build(cat, grid, region=load_antarctic_basins(),
                        backend=backend, mortie_order=mortie_order)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    sm.to_json(out)
    print(f"[build] {len(sm.shard_keys)} shards, {sm.metadata['total_pairs']:,} pairs, "
          f"{time.perf_counter() - t0:.1f}s -> {out}")


# ── cli ────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    pb = sub.add_parser("build", help="snapshot a baseline catalog from the cache")
    pb.add_argument("--backend", default="mortie",
                    choices=["mortie", "spherely", "shapely", "auto"])
    pb.add_argument("--out", required=True)
    pb.add_argument("--mortie-order", type=int, default=8)

    pc = sub.add_parser("compare", help="diff two catalogs")
    pc.add_argument("a")
    pc.add_argument("b")
    pc.add_argument("--label-a", default="a")
    pc.add_argument("--label-b", default="b")

    args = p.parse_args()
    if args.cmd == "build":
        build(args.backend, args.out, mortie_order=args.mortie_order)
    else:
        summary = compare(args.a, args.b, args.label_a, args.label_b)
        _print_compare(summary, args.label_a, args.label_b)


if __name__ == "__main__":
    main()
