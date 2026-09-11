"""Serial vs batch vs stored-index intersection on REAL catalogs (issue #396, PR #400).

Measures the phase-1 rewire of ``_intersect_mortie`` -- one batched
``mortie.polygons_to_morton_mocs`` call per block of rings, replacing one
``morton_coverage_moc`` call per granule -- against the per-granule loop it
replaced, and both against the phase-3 ``cells`` arm, which reads the MOC the
catalog already carries (``Catalog.index_footprints``) and does no geometry at
all. **Wall and peak RSS** for every arm; the phase-3 indexing pass is timed
separately as ``idx_s`` because it is one-time per catalog, not per build.

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

    # phase 3: the stored-index arm against the geometry it replaces, min-of-3
    uv run python bench/shardmap_batch_vs_serial.py --cases neon,88s \
        --arms serial,batch,cells --orders 9 --reps 3

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
    from mortie import moc_to_order, polygons_to_morton_mocs

    out: dict = {}
    parent_order = grid.parent_order
    for i, rec in enumerate(records):
        try:
            moc = np.asarray(
                polygons_to_morton_mocs(
                    rec["lats"], rec["lons"], np.array([0, len(rec["lats"])]), order=order
                )[0]
            )
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


def _load_case(name, index_order=None):
    """Return ``(records, grid, all_shards, cat, index_s)`` for one case, as ``build`` would.

    ``index_order`` precomputes the ``footprint_cells`` column (phase 3) before
    the records are decoded, and reports its wall separately: it is the
    **one-time** cost the ``cells`` arm amortizes, not part of a build, so
    counting it in the intersection wall would measure the wrong thing.
    """
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
    index_s = None
    if index_order is not None:
        t_idx = time.perf_counter()
        cat = cat.index_footprints(index_order)
        index_s = time.perf_counter() - t_idx
    records = cat.granule_records()
    all_shards = {int(s) for s in grid.coverage(_region_parts(parts, cat.metadata))}
    return records, grid, all_shards, cat, index_s


def _measure_child(name, path, order, block):
    """Child mode: run one (case, path) measurement and print a JSON line."""
    from zagg.catalog import shardmap

    if block is not None:
        # ``--block`` overrides the arm's own blocking constant: rings per
        # ``polygons_to_morton_mocs`` call on the batch arm, records per
        # ``moc_and``/``moc_to_order`` call on the cells arm (phase 4).
        if path == "cells":
            shardmap._CELLS_BATCH_RECORDS = block
        else:
            shardmap._MOC_BATCH_RINGS = block
    records, grid, all_shards, cat, index_s = _load_case(
        name, index_order=order if path == "cells" else None
    )
    rss_load = _rss_mb()
    res_load = _resident_mb()
    if path == "cells":
        # Both halves of the stored-index build path are timed: resolving the
        # plan (which since #439 carries the record -> table-row alignment as
        # ``granule_row_mask``'s whole-table shapely screen, the term ``build``
        # pays every time in place of the id lookup it used to) and the blocked
        # ``moc_and``/``moc_to_order`` batch itself. Only ``index_footprints``
        # above is outside, because only it is one-time.
        def fn(records, grid, all_shards, order):  # unused args: the shared arm signature
            plan = shardmap._footprint_cells_plan(cat, grid, "mortie", "swath", None)
            values, offsets, _order, rows, _considered = plan
            return shardmap._intersect_footprint_cells(rows, values, offsets, grid, all_shards)
    else:
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
                "index_s": None if index_s is None else round(index_s, 2),
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


ARMS = ("serial", "batch", "cells")


def run_cases(names, orders=None, arms=("serial", "batch"), reps=1):
    """One row per (case, order), asserting every arm present agrees exactly.

    Three arms, each the whole intersection for one implementation:

    ``serial``
        the pre-#396 per-granule ``morton_coverage_moc`` loop -- the oracle.
    ``batch``
        phase 1: ``polygons_to_morton_mocs`` a block of rings at a time.
    ``cells``
        phases 3-4: no geometry at all -- ``moc_and`` of the AOI's own shard
        MOC against a block of granules' **stored** MOCs
        (``Catalog.index_footprints``), chained into ``moc_to_order``,
        ``_CELLS_BATCH_RECORDS`` records per call. The indexing
        pass runs in the same child but is timed separately and
        reported as ``idx_s``, because it is one-time per catalog where the
        others are per build.

    ``arms`` drops sides. ``--arms batch`` exists for exactly one row --
    ``full`` at order 13, where the serial arm is ~1 h -- and it is the only way
    to get a row **without** the oracle assert, so it prints ``no-assert`` to say
    so rather than letting a bare number imply a verified one. What backs that
    row instead is ``california`` at the same order and AOI, where the arms do
    run head to head on identical output (190,625 pairs / 2,721 shards), plus
    ``tests/test_shardmap.py``'s ``dict ==`` identity pins.

    ``reps`` > 1 reports the **min** wall per arm over N processes, ``idx_s``
    included -- each ``cells`` rep re-runs ``index_footprints`` in its own child,
    so min-ing it keeps every number in the row measured the same way. Quote them
    min-of-N for the same reason ``--knee`` does: the batch arm is
    rayon-parallel, so a single-shot wall on a loaded machine scatters wider
    than some of the differences being read off this table. Peak and the
    correctness digest come from the first rep (peak is stable across them).

    Returns the list of requested cases whose catalog is **absent**, so the
    caller can exit non-zero rather than let a run over nothing read as clean.
    """
    print(
        f"{'case':>11} {'order':>5} {'granules':>9} {'shards':>7} {'pairs':>9} "
        f"{'serial_s':>9} {'batch_s':>8} {'cells_s':>8} {'idx_s':>7} {'b/c':>5} "
        f"{'ser_MB':>7} {'bat_MB':>7} {'cel_MB':>7} {'ser_hw':>7} {'bat_hw':>7} {'cel_hw':>7}",
        flush=True,
    )
    skipped = []
    for name in names:
        if not _available(name):
            print(f"{name:>11}  -- skipped: {CASES[name]['catalog']} not present", flush=True)
            skipped.append(name)
            continue
        for order in orders or CASES[name].get("orders", ORDERS):
            got = {}
            for arm in ARMS:
                if arm not in arms:
                    continue
                runs = [_measure(name, arm, order) for _ in range(reps)]
                m = dict(runs[0])
                m["wall_s"] = min(r["wall_s"] for r in runs)
                if m.get("index_s") is not None:
                    m["index_s"] = min(r["index_s"] for r in runs)
                got[arm] = m
            digests = {a: m["digest"] for a, m in got.items()}
            assert len(set(digests.values())) <= 1, f"{name}@o{order}: arms disagree {digests}"
            ref = next(iter(got.values()))
            bat, cel = got.get("batch"), got.get("cells")
            gain = f"{bat['wall_s'] / max(cel['wall_s'], 1e-9):>4.1f}x" if bat and cel else "--"
            row = [
                f"{name:>11}",
                f"{order:>5}",
                f"{ref['granules']:>9,}",
                f"{ref['shards_hit']:>7,}",
                f"{ref['pairs']:>9,}",
                *(
                    f"{got[a]['wall_s']:>{w}.2f}" if a in got else f"{'--':>{w}}"
                    for a, w in (("serial", 9), ("batch", 8), ("cells", 8))
                ),
                f"{cel['index_s']:>7.2f}" if cel else f"{'--':>7}",
                f"{gain:>5}",
                *(
                    f"{peak(got[a]):>7.0f}" if a in got else f"{'--':>7}"
                    for peak in (_peak_mb, _peak_hw_mb)
                    for a in ARMS
                ),
            ]
            if len(got) < 2:
                row.append(" no-assert (one arm only)")
            print(" ".join(row), flush=True)
    return skipped


def run_knee(name, order=13, blocks=KNEE_BLOCKS, reps=1, arm="batch"):
    """Re-validate a blocking constant on real footprints, not synthetic ones.

    ``arm="batch"`` sweeps ``_MOC_BATCH_RINGS`` (rings per
    ``polygons_to_morton_mocs`` call); ``arm="cells"`` sweeps
    ``_CELLS_BATCH_RECORDS`` (records per ``moc_and``/``moc_to_order`` call
    on the phase-4 stored-index path -- pass record-scaled ``--blocks``, the
    ring-scaled defaults are far too small for it).

    ``reps`` > 1 reports the **min** wall over N runs of each block.
    Both batch entry points are rayon-parallel and so the most load-sensitive
    number here: single-shot walls scatter by tens of percent on a busy machine,
    which is wider than the block-to-block effect being measured. Peak is stable
    across reps and is reported from the first.
    """
    if not _available(name):
        print(f"knee: skipped, {CASES[name]['catalog']} not present", flush=True)
        return [name]
    print(
        f"\nblock-size knee -- {name} @order{order}, arm {arm}, min of {reps} (real footprints)",
        flush=True,
    )
    print(f"{'block':>7} {'wall_s':>8} {'peak_MB':>8} {'peak_hw':>8}", flush=True)
    for block in blocks:
        runs = [_measure(name, arm, order, block=block) for _ in range(reps)]
        wall = min(m["wall_s"] for m in runs)
        print(
            f"{block:>7} {wall:>8.2f} {_peak_mb(runs[0]):>8.0f} {_peak_hw_mb(runs[0]):>8.0f}",
            flush=True,
        )
    return []


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
        help="any of serial,batch,cells; a single arm skips the oracle assert and says so",
    )
    ap.add_argument("--knee", default=None, help="case to sweep a blocking constant on")
    ap.add_argument(
        "--knee-arm",
        choices=("batch", "cells"),
        default="batch",
        help="which blocking constant --knee sweeps: batch=_MOC_BATCH_RINGS, "
        "cells=_CELLS_BATCH_RECORDS (phase 4)",
    )
    ap.add_argument(
        "--blocks",
        default=",".join(str(b) for b in KNEE_BLOCKS),
        help="block sizes for --knee",
    )
    ap.add_argument("--reps", type=int, default=1, help="repeats for --cases and --knee; min-of-N")
    ap.add_argument("--measure", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--path", choices=ARMS, default="batch")
    ap.add_argument("--order", type=int, default=13, help="MOC order for --knee (and --measure)")
    ap.add_argument("--block", type=int, default=None)
    args = ap.parse_args(argv)

    if args.measure:
        _measure_child(args.measure, args.path, args.order, args.block)
        return 0
    orders = tuple(int(o) for o in args.orders.split(",")) if args.orders else None
    cases = args.cases if args.cases is not None else ("" if args.knee else "neon,88s,california")
    skipped = []
    names = [c for c in cases.split(",") if c] if cases else []
    unknown = [c for c in ([*names, args.knee] if args.knee else names) if c not in CASES]
    if unknown:
        raise SystemExit(f"unknown case(s) {unknown}; known: {sorted(CASES)}")
    if names:
        arms = tuple(a for a in args.arms.split(",") if a)
        skipped = run_cases(names, orders, arms, reps=args.reps)
    if args.knee:
        blocks = tuple(int(b) for b in args.blocks.split(",") if b)
        skipped += run_knee(
            args.knee, order=args.order, blocks=blocks, reps=args.reps, arm=args.knee_arm
        )
    # A requested case whose catalog is absent produced no row. Exiting 0 would
    # read as a clean run over nothing, which is the wrong default for a harness
    # whose output gets quoted.
    if skipped:
        n = len(names) + bool(args.knee)
        raise SystemExit(f"skipped {len(skipped)}/{n} requested cases: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
