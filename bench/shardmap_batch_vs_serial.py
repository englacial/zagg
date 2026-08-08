"""Batch-vs-serial mortie intersection on REAL catalogs (issue #396, PR #400).

Measures the phase-1 rewire of ``_intersect_mortie`` -- one batched
``mortie.polygons_to_morton_mocs`` call per block of rings, replacing one
``morton_coverage_moc`` call per granule -- against the per-granule loop it
replaced, reporting **wall and peak RSS** for both.

Why this file exists at all: the first cut of these numbers came from synthetic
footprints, and the synthetic fixture was badly miscalibrated. A synthetic
quadrilateral covers ~220 MOC cells at order 13. Real ATL03 CMR footprints, 1,000
sampled uniformly from the clone (seed 20260807) and covered at order 13, run::

    min 4,373   p05 6,540   p25 7,179   median 9,266   mean 8,659
    p75 10,238  p95 10,370  p99 10,618  max 10,881     (vertices/footprint: 27)

-- ~40x the synthetic figure, with a 2.5x spread that a single median
under-describes: the block's steady memory follows the **mean** (8,659) and its
worst case the **p99/max** (10,618 / 10,881). The synthetic figure is what
produced a block size tuned an order of magnitude too large; that the earlier
"realistic" 32,127 is 3.0x the largest footprint in the sample is what confirms
it was a 90-degree-of-latitude envelope rather than a CMR polygon. Real catalogs
are in the tree -- use them. (The synthetic ``benchmarks/mortie_order_sweep.py``
was retired with this change, espg's ruling: if we keep a benchmark at all it
runs on real data. The *order* question is answered by the real-catalog
``bench/neon_order_sweep.py``.)

Cases, smallest first. The first two are committed fixtures, so they run in any
checkout (every path below is resolved relative to *this file*, so the benchmark
reads its inputs from the same tree as the code under test); the last two need
the 305 MB full-mission ATL03 clone (``data/atl03_v007/``, not committed) and
skip cleanly when it is absent via ``_available()``. That guard is new here --
``bench/neon_order_sweep.py`` reads the clone with an unguarded ``pq.read_table``
and raises when it is missing, so this is an improvement on that file rather
than a convention copied from it.

  neon        2,089 granules   committed  cat_neon.parquet  x AOP_NEON
  88s        35,639 granules   committed  cat_88s.parquet   x antarctic_88s
  california  4,354 granules   clone      bbox cut of the clone x California
  full      555,867 granules   clone      the whole clone   x California

``full`` is the issue's headline baseline (the 310.7 s single-pass mortie row);
it is slow by construction and opt-in via ``--cases full``.

Peak RSS needs process isolation -- ``ru_maxrss`` is a monotone high-water mark,
so serial and batch cannot be measured in one process. Each measurement
therefore re-executes this file as a child (``--measure``) that prints one JSON
line.

Two baselines are reported, because they are not the same number and the
difference is 173 MB at operator scale. ``ru_maxrss`` before the intersection is
the *high-water* of the load, not its resident plateau: on the ``full`` case
``granule_records()`` leaves a 173 MB transient above the plateau it settles at
(resident 4,033.6 MB vs maxrss 4,206.8 MB), so a maxrss-vs-maxrss delta silently
absorbs the intersection's first 173 MB and floors at 0. The ``MB`` columns are
therefore ``maxrss_after - resident_before`` -- the intersection's real
increment -- and the ``hw`` columns are the old ``maxrss_after - maxrss_before``,
kept so the clamped floor stays visible. They coincide wherever the load has no
transient: on ``california`` ``rss_load == maxrss_load`` exactly, because
``filter_bbox`` cuts before ``granule_records`` runs, so the knee sweep and the
California rows read the same either way.

Run::

    uv run python bench/shardmap_batch_vs_serial.py
    uv run python bench/shardmap_batch_vs_serial.py --cases neon,88s
    uv run python bench/shardmap_batch_vs_serial.py --cases full   # slow, needs the clone

    # the order a default build actually resolves to, at operator scale: batch
    # only, because the serial arm of this one is ~1 h. It is the single row in
    # the table with no oracle assert and it prints ``no-assert`` to say so
    uv run python bench/shardmap_batch_vs_serial.py --cases full --orders 13 --arms batch

    # block-size knee: blocks, order and repeat count are all CLI-settable, so
    # every column of the PR body's sweep is reproducible from here
    uv run python bench/shardmap_batch_vs_serial.py --knee california --order 13 --reps 3
    uv run python bench/shardmap_batch_vs_serial.py --knee california --order 9 \
        --blocks 16,32,64,1024 --reps 3
    uv run python bench/shardmap_batch_vs_serial.py --knee 88s --order 13 \
        --blocks 32,64,256,1024 --reps 3
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

REPO = str(Path(__file__).resolve().parents[1])
BENCH = f"{REPO}/tests/data/benchmark"
FULL = f"{REPO}/data/atl03_v007/atl03_v007_full.parquet"
CFG9 = f"{BENCH}/configs/atl03_tdigest_healpix_o9.yaml"

# ``atl03_tdigest_healpix_o9.yaml`` is the shipped production grid: parent_order
# 9 shards, child_order 19 leaves, chunk_inner 13 -- so ``ShardMap.build``'s
# default MOC order resolves to 13 (#92). Both orders are swept: 13 is what a
# default build actually runs, 9 is the legal floor the issue's baselines used.
ORDERS = (9, 13)

# Default ``--blocks`` for the knee sweep; every block quoted in the PR body is
# reachable from the CLI (``--blocks``/``--order``/``--reps``), so no claim about
# the block size rests on a run this script cannot regenerate.
KNEE_BLOCKS = (8, 16, 24, 32, 48, 64, 96, 128, 256, 512, 1024, 2048)

CASES = {
    "neon": {"catalog": f"{BENCH}/catalogs/cat_neon.parquet", "aoi": f"{BENCH}/AOP_NEON.geojson"},
    "88s": {
        "catalog": f"{BENCH}/catalogs/cat_88s.parquet",
        "aoi": f"{BENCH}/antarctic_88s.geojson",
    },
    "california": {"catalog": FULL, "aoi": "california", "bbox_cut": True},
    "full": {"catalog": FULL, "aoi": "california", "orders": (9,)},
}


def _rss_mb() -> float:
    """Process high-water RSS in MB (``ru_maxrss`` is bytes on macOS, KB on Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def _resident_mb() -> float:
    """Process *resident* RSS in MB right now -- the plateau, not the high-water."""
    if sys.platform == "darwin":
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True
        )
        return int(out.stdout.strip()) / 1e3
    pages = int(Path("/proc/self/statm").read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / 1e6


def _intersect_serial(records, grid, all_shards, order):
    """The pre-#396 per-granule loop -- the thing the batch path replaced.

    Kept in step with ``tests/test_shardmap.py::_intersect_mortie_serial``, which
    is the authoritative copy: that one is asserted equal to the shipping path on
    every run, so a drift between them shows up as a test failure, not as a
    quietly wrong benchmark.
    """
    from mortie import moc_to_order, morton_coverage_moc

    out: dict = {}
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
    return {k: list(dict.fromkeys(v)) for k, v in out.items()}


def _load_case(name):
    """Return ``(records, grid, all_shards)`` for one case, as ``build`` would."""
    from zagg.catalog import load_polygon
    from zagg.catalog.shardmap import _region_parts
    from zagg.catalog.sources import Catalog
    from zagg.config import load_config
    from zagg.data import demo_aoi
    from zagg.grids import from_config

    spec = CASES[name]
    grid = from_config(load_config(CFG9))
    aoi = spec["aoi"]
    parts = load_polygon(demo_aoi(aoi) if not aoi.endswith(".geojson") else aoi)
    cat = Catalog.from_geoparquet(spec["catalog"])
    if spec.get("bbox_cut"):
        # The demo/01_query.ipynb path: cut the global clone to the AOI's
        # shard-complete bbox first, then let the exact intersection prune.
        cat = cat.filter_bbox([grid.coverage_bbox(parts)])
    # This line, not the intersection, is the memory story at operator scale, and
    # it is worth being precise about *which* part of it. On the full clone:
    # 137 MB interpreter -> 1,353 MB after from_geoparquet -> 4,038 MB resident
    # here, so granule_records() itself adds ~2,684 MB. The coordinate arrays it
    # returns are only 282.9 MB of that (17,681,679 vertices), 7%. The dominant
    # term is the seven whole-table ``to_pylist()`` calls at sources.py:525-552,
    # which are all live simultaneously (~1.8 GB): ``assets`` alone is 1,147 MB
    # (2,064 B/row, 28% of the plateau) and the loop reads exactly two hrefs out
    # of each dict; geometry WKB is 305 MB, the str values 304 MB, the dict
    # objects 151 MB, id 49 MB, the two datetimes 63 MB. So the large cheap lever
    # is not vectorizing shapely.from_wkb (<=15% of the plateau) but projecting
    # the two href fields in Arrow / batching the to_pylist() calls.
    records = cat.granule_records()
    all_shards = {int(s) for s in grid.coverage(_region_parts(parts, cat.metadata))}
    return records, grid, all_shards


def _measure_child(name, path, order, block):
    """Child mode: run one (case, path) measurement and print a JSON line."""
    from zagg.catalog import shardmap

    if block is not None:
        shardmap._MOC_BATCH_RINGS = block
    records, grid, all_shards = _load_case(name)
    rss_load = _rss_mb()
    res_load = _resident_mb()
    fn = _intersect_serial if path == "serial" else shardmap._intersect_mortie
    t0 = time.perf_counter()
    out = fn(records, grid, all_shards, order=order)
    wall = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "granules": len(records),
                "shards_aoi": len(all_shards),
                "shards_hit": len(out),
                "pairs": sum(len(v) for v in out.values()),
                "digest": hash(tuple(sorted((k, tuple(v)) for k, v in out.items()))),
                "wall_s": round(wall, 2),
                "rss_load_mb": round(rss_load, 1),
                "res_load_mb": round(res_load, 1),
                "rss_peak_mb": round(_rss_mb(), 1),
            }
        )
    )


def _measure(name, path, order, block=None):
    argv = [sys.executable, __file__, "--measure", name, "--path", path, "--order", str(order)]
    if block is not None:
        argv += ["--block", str(block)]
    proc = subprocess.run(argv, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{name}/{path}/o{order} failed:\n{proc.stderr[-2000:]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _available(name) -> bool:
    return Path(CASES[name]["catalog"]).exists()


def _peak_mb(m) -> float:
    """The intersection's own peak, above the load's *resident* plateau."""
    return m["rss_peak_mb"] - m["res_load_mb"]


def _peak_hw_mb(m) -> float:
    """The same peak above the load's high-water -- clamps at 0, kept for contrast."""
    return m["rss_peak_mb"] - m["rss_load_mb"]


def run_cases(names, orders=None, arms=("serial", "batch")):
    """Serial vs batch per (case, order), asserting batch == serial on every row.

    ``arms`` drops one side. ``--arms batch`` exists for exactly one row --
    ``full`` at order 13, where the serial arm is ~1 h -- and it is the only way
    to get a row **without** the oracle assert, so it prints ``no-assert`` to say
    so rather than letting a bare number imply a verified one. What backs that
    row instead is ``california`` at the same order and AOI, where serial and
    batch do run head to head on identical output (190,625 pairs / 2,721 shards),
    plus ``tests/test_shardmap.py::TestMortieBatch``'s ``dict ==`` identity pins.
    """
    print(
        f"{'case':>11} {'order':>5} {'granules':>9} {'shards':>7} {'pairs':>9} "
        f"{'serial_s':>9} {'batch_s':>8} {'x':>5} {'ser_MB':>7} {'bat_MB':>7} "
        f"{'ser_hw':>7} {'bat_hw':>7}",
        flush=True,
    )
    for name in names:
        if not _available(name):
            print(f"{name:>11}  -- skipped: {CASES[name]['catalog']} not present", flush=True)
            continue
        for order in orders or CASES[name].get("orders", ORDERS):
            ser = _measure(name, "serial", order) if "serial" in arms else None
            bat = _measure(name, "batch", order) if "batch" in arms else None
            if ser and bat:
                assert ser["digest"] == bat["digest"], f"{name}@o{order}: batch != serial"
            ref = ser or bat
            speedup = f"{ser['wall_s'] / max(bat['wall_s'], 1e-9):>4.1f}x" if ser and bat else "--"
            row = [
                f"{name:>11}",
                f"{order:>5}",
                f"{ref['granules']:>9,}",
                f"{ref['shards_hit']:>7,}",
                f"{ref['pairs']:>9,}",
                f"{ser['wall_s']:>9.2f}" if ser else f"{'--':>9}",
                f"{bat['wall_s']:>8.2f}" if bat else f"{'--':>8}",
                f"{speedup:>5}",
                f"{_peak_mb(ser):>7.0f}" if ser else f"{'--':>7}",
                f"{_peak_mb(bat):>7.0f}" if bat else f"{'--':>7}",
                f"{_peak_hw_mb(ser):>7.0f}" if ser else f"{'--':>7}",
                f"{_peak_hw_mb(bat):>7.0f}" if bat else f"{'--':>7}",
            ]
            if not (ser and bat):
                row.append(" no-assert (one arm only)")
            print(" ".join(row), flush=True)


def run_knee(name, order=13, blocks=KNEE_BLOCKS, reps=1):
    """Re-validate ``_MOC_BATCH_RINGS`` on real footprints, not synthetic ones.

    ``reps`` > 1 reports the **min** wall over N runs of each block.
    ``polygons_to_morton_mocs`` is rayon-parallel and so the most load-sensitive
    number here: single-shot walls scatter by tens of percent on a busy machine,
    which is wider than the block-to-block effect being measured. Peak is stable
    across reps and is reported from the first.
    """
    if not _available(name):
        print(f"knee: skipped, {CASES[name]['catalog']} not present", flush=True)
        return
    print(f"\nblock-size knee -- {name} @order{order}, min of {reps} (real footprints)", flush=True)
    print(f"{'block':>7} {'wall_s':>8} {'peak_MB':>8} {'peak_hw':>8}", flush=True)
    for block in blocks:
        runs = [_measure(name, "batch", order, block=block) for _ in range(reps)]
        wall = min(m["wall_s"] for m in runs)
        print(
            f"{block:>7} {wall:>8.2f} {_peak_mb(runs[0]):>8.0f} {_peak_hw_mb(runs[0]):>8.0f}",
            flush=True,
        )


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--cases",
        default=None,
        help="default neon,88s,california -- or nothing when --knee is given",
    )
    ap.add_argument("--orders", default=None, help="override the swept MOC orders, e.g. 13")
    ap.add_argument(
        "--arms",
        default="serial,batch",
        help="which arms to run; 'batch' alone skips the oracle assert and says so",
    )
    ap.add_argument("--knee", default=None, help="case to sweep _MOC_BATCH_RINGS on")
    ap.add_argument(
        "--blocks",
        default=",".join(str(b) for b in KNEE_BLOCKS),
        help="block sizes for --knee",
    )
    ap.add_argument("--reps", type=int, default=1, help="--knee repeats; wall is min-of-N")
    ap.add_argument("--measure", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--path", choices=("serial", "batch"), default="batch")
    ap.add_argument("--order", type=int, default=13, help="MOC order for --knee (and --measure)")
    ap.add_argument("--block", type=int, default=None)
    args = ap.parse_args(argv)

    if args.measure:
        _measure_child(args.measure, args.path, args.order, args.block)
        return 0
    orders = tuple(int(o) for o in args.orders.split(",")) if args.orders else None
    cases = args.cases if args.cases is not None else ("" if args.knee else "neon,88s,california")
    if cases:
        arms = tuple(a for a in args.arms.split(",") if a)
        run_cases([c for c in cases.split(",") if c], orders, arms)
    if args.knee:
        blocks = tuple(int(b) for b in args.blocks.split(",") if b)
        run_knee(args.knee, order=args.order, blocks=blocks, reps=args.reps)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
