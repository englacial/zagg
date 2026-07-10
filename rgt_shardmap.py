"""Bin RGT 1336 repeat-track photons into o9 HEALPix parent shards (issue #209).

No CMR / network: reads each local granule's per-segment reference lat/lon +
``segment_ph_cnt`` via h5coro's FileDriver, maps segments to the grid's
``parent_order`` shard, and sums photons per shard to pick the densest o9 parent
(all 29 repeat granules cover the same track, so photons stack per cell)."""

from __future__ import annotations

import glob
import os
from collections import defaultdict

import numpy as np
from h5coro import H5Coro, filedriver

BEAMS = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]


def _shard_photons(grid, path):
    """Return {parent_shard: photon_count} for one granule."""
    h5 = H5Coro(path, filedriver.FileDriver, errorChecking=True, verbose=False)
    acc: dict[int, int] = defaultdict(int)
    for b in BEAMS:
        base = f"/{b}/geolocation"
        paths = [
            f"{base}/reference_photon_lat",
            f"{base}/reference_photon_lon",
            f"{base}/segment_ph_cnt",
        ]
        try:
            data = h5.readDatasets(paths)
        except Exception:  # noqa: BLE001
            continue
        lat = np.asarray(data[paths[0]])
        lon = np.asarray(data[paths[1]])
        cnt = np.asarray(data[paths[2]]).astype(np.int64)
        if lat.size == 0:
            continue
        good = np.isfinite(lat) & np.isfinite(lon) & (np.abs(lat) <= 90)
        lat, lon, cnt = lat[good], lon[good], cnt[good]
        if lat.size == 0:
            continue
        leaf = grid.assign(lat, lon)
        parents = grid.shards_of(leaf)
        for p, c in zip(np.asarray(parents), cnt):
            acc[int(p)] += int(c)
    return acc


def build_rgt(config, granule_dir, n_granules=None):
    """Pick the densest o9 parent shard over the RGT granules; return
    ``(shard_key, granule_paths)``. All granules are passed to the worker (it
    re-filters to the shard via the segment-indexed read plan)."""
    from zagg.grids import from_config

    paths = sorted(glob.glob(os.path.join(granule_dir, "ATL03_*.h5")))
    if n_granules:
        paths = paths[:n_granules]
    # bootstrap grid (fullsphere) just for assign/shards_of
    grid = from_config(config, populated_shards=[0])
    totals: dict[int, int] = defaultdict(int)
    for i, p in enumerate(paths):
        acc = _shard_photons(grid, p)
        for k, v in acc.items():
            totals[k] += v
        top = max(acc.items(), key=lambda kv: kv[1]) if acc else (None, 0)
        print(f"  [{i+1}/{len(paths)}] {os.path.basename(p)} shards={len(acc)} "
              f"top={top[0]}:{top[1]}", flush=True)
    best = max(totals.items(), key=lambda kv: kv[1])
    print(f"  densest o9 parent {best[0]} photons={best[1]} over {len(paths)} granules", flush=True)
    return best[0], paths


if __name__ == "__main__":
    import sys

    from zagg.config import load_config

    cfg = load_config(
        os.path.join(
            os.path.dirname(__file__),
            "tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml",
        )
    )
    n = int(sys.argv[1]) if len(sys.argv) > 1 else None
    build_rgt(cfg, os.path.expanduser("~/ignore/atl03_1336_r05"), n_granules=n)
