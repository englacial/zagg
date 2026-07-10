"""Issue #209 Option A measurement: fixed-length padded centroid array vs ragged CSR.

Standalone measurement (NOT production code). Builds per-cell t-digests (delta=512)
for the densest shard of a sparse (NEON) and a dense (RGT 1336) fixture, then
compares the current per-inner-chunk ragged CSR encoding against a fixed
``[delta, 2]`` padded dense array riding the Zarr v3 ShardingCodec.

Run:  uv run --with h5py python scripts_measure_option_a.py <fixture> <out.json>
      fixture in {neon, rgt1336}
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from collections import Counter, defaultdict

import h5py
import numpy as np
import zarr
from mortie import clip2order, geo2mort
from zarr.codecs import BytesCodec, ZstdCodec
from zarr.storage import LocalStore

from zagg.csr import read_csr, write_csr
from zagg.stats.tdigest import build_tdigest

REF_ORDER = 29
CHILD_ORDER = 19
CHUNK_INNER = 13
DELTA = 512
GROUPS = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]
SCRATCH = (
    "/private/tmp/claude-501/-Users-espg-software-zagg/"
    "2531f603-b5ed-4ebf-b97e-2461d627df9e/scratchpad"
)

CELLS_PER_CHUNK = 4 ** (CHILD_ORDER - CHUNK_INNER)  # 4096


def iter_beams(path, need_h):
    """Yield (lat, lon, h) per beam, applying signal_conf_ph[:,0] != -2 filter."""
    with h5py.File(path, "r") as f:
        for g in GROUPS:
            base = f.get(g + "/heights")
            if base is None:
                continue
            conf = base["signal_conf_ph"][:, 0]
            keep = conf != -2
            if not keep.any():
                continue
            lat = base["lat_ph"][:][keep]
            lon = base["lon_ph"][:][keep]
            h = base["h_ph"][:][keep] if need_h else None
            yield lat, lon, h


def densest_shard(granules, parent_order):
    cnt = Counter()
    for path in granules:
        for lat, lon, _ in iter_beams(path, need_h=False):
            leaf = geo2mort(lat, lon, order=REF_ORDER, points=True)
            sh = clip2order(parent_order, leaf)
            u, c = np.unique(sh, return_counts=True)
            for s, n in zip(u, c):
                cnt[int(s)] += int(n)
    best = max(cnt, key=cnt.get)
    return best, cnt[best], len(cnt)


def densest_shard_bbox(scan_granules, parent_order):
    """Densest shard + its padded lat/lon bbox from a subset of granules.

    For a repeat track (RGT 1336) every repeat overlaps the same ground track,
    so the densest shard found from one granule is the densest overall; its
    bbox lets pass 2 prefilter photons cheaply (numpy compare) before the
    expensive geo2mort, so only shard-adjacent photons are indexed.
    """
    cnt = Counter()
    lat_of = {}
    lon_of = {}
    for path in scan_granules:
        for lat, lon, _ in iter_beams(path, need_h=False):
            leaf = geo2mort(lat, lon, order=REF_ORDER, points=True)
            sh = clip2order(parent_order, leaf)
            for s in np.unique(sh):
                m = sh == s
                s = int(s)
                cnt[s] += int(m.sum())
                lat_of.setdefault(s, []).append((lat[m].min(), lat[m].max()))
                lon_of.setdefault(s, []).append((lon[m].min(), lon[m].max()))
    best = max(cnt, key=cnt.get)
    lats = np.array(lat_of[best])
    lons = np.array(lon_of[best])
    pad = 0.02
    bbox = (lats[:, 0].min() - pad, lats[:, 1].max() + pad,
            lons[:, 0].min() - pad, lons[:, 1].max() + pad)
    return best, cnt[best], len(cnt), bbox


def cell_values_for_shard(granules, parent_order, shard, bbox=None):
    """Return {cell_morton: 1-D h array} for photons in `shard`.

    ``bbox`` (lat_lo, lat_hi, lon_lo, lon_hi) prefilters photons cheaply before
    the expensive geo2mort (used for the repeat-track fixture).
    """
    acc = defaultdict(list)
    for path in granules:
        for lat, lon, h in iter_beams(path, need_h=True):
            if bbox is not None:
                pre = (lat >= bbox[0]) & (lat <= bbox[1]) & (lon >= bbox[2]) & (lon <= bbox[3])
                if not pre.any():
                    continue
                lat = lat[pre]
                lon = lon[pre]
                h = h[pre]
            leaf = geo2mort(lat, lon, order=REF_ORDER, points=True)
            sh = clip2order(parent_order, leaf)
            m = sh == shard
            if not m.any():
                continue
            leaf = leaf[m]
            hh = h[m]
            cell = clip2order(CHILD_ORDER, leaf)
            order = np.argsort(cell, kind="stable")
            cs = cell[order]
            hs = hh[order]
            bounds = np.flatnonzero(np.diff(cs)) + 1
            starts = np.concatenate([[0], bounds])
            ends = np.concatenate([bounds, [len(cs)]])
            for st, en in zip(starts, ends):
                acc[int(cs[st])].append(hs[st:en])
    return {k: np.concatenate(v) for k, v in acc.items()}


def build_digests(cell_vals):
    """{cell_morton: (k,2) float32 digest} for non-empty digests."""
    out = {}
    for cm, vals in cell_vals.items():
        d = build_tdigest(vals.astype(np.float64), delta=DELTA)
        if d.shape[0] > 0:
            out[cm] = d.astype(np.float32)
    return out


def store_bytes(path):
    tot = 0
    nf = 0
    for root, _, files in os.walk(path):
        for f in files:
            tot += os.path.getsize(os.path.join(root, f))
            nf += 1
    return nf, tot


# ── ragged CSR encoding (current path) ──────────────────────────────────────
def encode_csr(digests, chunk_cells):
    """One CSR subgroup per occupied inner chunk (the current write.py:279 layout)."""
    path = f"{SCRATCH}/opt_a_csr.zarr"
    shutil.rmtree(path, ignore_errors=True)
    store = LocalStore(path)
    for chunk, cells in chunk_cells.items():
        cells_sorted = sorted(cells)
        values_list = [digests[c] for c in cells_sorted]
        cell_ids = list(range(len(cells_sorted)))  # local position within chunk
        write_csr(store, f"tdig/{chunk}", values_list, cell_ids, dtype="float32")
    nf, tot = store_bytes(path)
    return path, nf, tot, len(chunk_cells)


def decode_csr_one(path, chunk_cells):
    store = LocalStore(path)
    chunk = next(iter(chunk_cells))
    t = time.perf_counter()
    d = read_csr(store, f"tdig/{chunk}")
    _ = d["values"][d["offsets"][0] : d["offsets"][1]]
    return time.perf_counter() - t


def decode_csr_all(path, chunk_cells):
    store = LocalStore(path)
    t = time.perf_counter()
    total = 0
    for chunk in chunk_cells:
        d = read_csr(store, f"tdig/{chunk}")
        off = d["offsets"]
        for k in range(len(off) - 1):
            total += off[k + 1] - off[k]
    return time.perf_counter() - t, int(total)


# ── padded [delta,2] dense sharded array ────────────────────────────────────
def _build_slab(cells, digests):
    slab = np.zeros((CELLS_PER_CHUNK, DELTA, 2), dtype=np.float32)
    for row, cm in enumerate(sorted(cells)):
        d = digests[cm]
        k = min(d.shape[0], DELTA)
        slab[row, :k, :] = d[:k, :]
    return slab


def measure_padded(digests, chunk_cells):
    """Build each occupied inner-chunk slab once and measure the ShardingCodec
    on-disk cost per inner codec.

    ShardingCodec compresses each inner chunk independently, so the shard object
    data size == sum over occupied inner chunks of len(inner_codec.encode(slab)).
    Measuring the codec output directly (numcodecs) is byte-faithful and avoids
    materializing the ~14.5 GB uncompressed array. ``bytes_only`` is the raw slab
    size (zagg's current dense on-disk policy) computed analytically.
    """
    from numcodecs import Blosc, Zstd

    codecs = {
        "zstd0": Zstd(level=0),
        "zstd5": Zstd(level=5),
        "blosc_zstd5": Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE),
    }
    sums = {k: 0 for k in codecs}
    raw_bytes = 0
    for chunk, cells in chunk_cells.items():
        slab = _build_slab(cells, digests)
        raw = slab.tobytes()
        raw_bytes += len(raw)
        for name, c in codecs.items():
            sums[name] += len(c.encode(slab))
    out = {"bytes_only": {"bytes": raw_bytes, "objects": len(chunk_cells) + 1}}
    for name in codecs:
        out[name] = {"bytes": int(sums[name]), "objects": 2}  # 1 shard + zarr.json
    return out


def write_real_zstd0_shard(digests, chunk_cells):
    """Write ONE real Zarr v3 ShardingCodec array (zstd L0) to confirm the
    ~1-object-per-shard collapse and validate the analytic size."""
    path = f"{SCRATCH}/opt_a_pad_zstd0.zarr"
    shutil.rmtree(path, ignore_errors=True)
    store = LocalStore(path)
    n_cells = len(chunk_cells) * CELLS_PER_CHUNK
    a = zarr.create_array(
        store, shape=(n_cells, DELTA, 2), shards=(n_cells, DELTA, 2),
        chunks=(CELLS_PER_CHUNK, DELTA, 2), dtype="float32", fill_value=0.0,
        serializer=BytesCodec(), compressors=ZstdCodec(level=0),
    )
    for slot, (chunk, cells) in enumerate(chunk_cells.items()):
        beg = slot * CELLS_PER_CHUNK
        a[beg : beg + CELLS_PER_CHUNK] = _build_slab(cells, digests)
    nf, tot = store_bytes(path)
    return path, nf, tot


def decode_padded_one(path):
    store = LocalStore(path)
    a = zarr.open_array(store, mode="r")
    t = time.perf_counter()
    cell = np.asarray(a[0])  # (delta,2)
    _ = cell[cell[:, 1] > 0]  # trim padding by weight
    return time.perf_counter() - t


def decode_padded_all(path, n_occ):
    store = LocalStore(path)
    a = zarr.open_array(store, mode="r")
    t = time.perf_counter()
    total = 0
    for slot in range(n_occ):
        beg = slot * CELLS_PER_CHUNK
        slab = np.asarray(a[beg : beg + CELLS_PER_CHUNK])
        w = slab[:, :, 1] > 0
        total += int(w.sum())
    return time.perf_counter() - t, total


def main():
    fixture = sys.argv[1]
    out_path = sys.argv[2]
    if fixture == "neon":
        gdir = "/Users/espg/ignore/zagg_neon_atl03_test_shard/granules"
        granules = sorted(os.path.join(gdir, f) for f in os.listdir(gdir) if f.endswith(".h5"))
        parent_order = 8
    elif fixture == "rgt1336":
        gdir = "/Users/espg/ignore/atl03_1336_r05"
        granules = sorted(os.path.join(gdir, f) for f in os.listdir(gdir) if f.endswith(".h5"))
        parent_order = 9
    else:
        raise SystemExit(f"unknown fixture {fixture}")

    print(f"[{fixture}] {len(granules)} granules, parent_order={parent_order}", flush=True)
    K = 4 ** (CHUNK_INNER - parent_order)
    cells_per_shard = 4 ** (CHILD_ORDER - parent_order)

    import pickle
    cache = f"{SCRATCH}/digests_{fixture}.pkl"
    if os.path.exists(cache):
        with open(cache, "rb") as f:
            blob = pickle.load(f)
        shard, obs, n_shards, digests = (
            blob["shard"], blob["obs"], blob["n_shards"], blob["digests"])
        print(f"  loaded cached digests: {len(digests)} cells", flush=True)
        _run_measurement(fixture, granules, parent_order, K, cells_per_shard,
                         shard, obs, n_shards, digests, out_path)
        return

    t0 = time.perf_counter()
    bbox = None
    if fixture == "rgt1336":
        # Repeat track: densest shard from one granule == densest overall; use
        # its bbox to prefilter pass 2 (avoids geo2mort over off-track photons).
        shard, obs, n_shards, bbox = densest_shard_bbox(granules[:1], parent_order)
        print(f"  (bbox prefilter {bbox})", flush=True)
    else:
        shard, obs, n_shards = densest_shard(granules, parent_order)
    print(f"  densest shard {shard}: {obs} obs across {n_shards} shards "
          f"({time.perf_counter()-t0:.0f}s)", flush=True)

    t1 = time.perf_counter()
    cell_vals = cell_values_for_shard(granules, parent_order, shard, bbox=bbox)
    obs = int(sum(v.size for v in cell_vals.values()))  # true per-shard obs (pass 2)
    print(f"  {len(cell_vals)} occupied cells, {obs} obs in shard "
          f"({time.perf_counter()-t1:.0f}s)", flush=True)

    t2 = time.perf_counter()
    digests = build_digests(cell_vals)
    print(f"  built digests ({time.perf_counter()-t2:.0f}s)", flush=True)

    with open(cache, "wb") as f:
        pickle.dump({"shard": shard, "obs": obs, "n_shards": n_shards, "digests": digests}, f)
    print(f"  cached digests -> {cache}", flush=True)

    _run_measurement(fixture, granules, parent_order, K, cells_per_shard,
                     shard, obs, n_shards, digests, out_path)


def _run_measurement(fixture, granules, parent_order, K, cells_per_shard,
                     shard, obs, n_shards, digests, out_path):
    centroids = [d.shape[0] for d in digests.values()]
    total_centroids = int(sum(centroids))
    mean_centroids = float(np.mean(centroids))
    print(f"  {len(digests)} digests, total centroids {total_centroids}, "
          f"mean {mean_centroids:.1f} (fill {mean_centroids/DELTA:.3f})", flush=True)

    # group occupied cells by inner chunk (order-13)
    chunk_cells = defaultdict(list)
    for cm in digests:
        ch = int(clip2order(CHUNK_INNER, np.array([cm], dtype=np.uint64))[0])
        chunk_cells[ch].append(cm)
    chunk_cells = dict(chunk_cells)
    print(f"  {len(chunk_cells)} occupied inner chunks (K={K}/shard)", flush=True)

    # CSR (current path): one subgroup per occupied inner chunk
    csr_path, csr_nf, csr_bytes, csr_groups = encode_csr(digests, chunk_cells)
    csr_one = min(decode_csr_one(csr_path, chunk_cells) for _ in range(3))
    csr_all_t, _ = decode_csr_all(csr_path, chunk_cells)
    print(f"  CSR: {csr_nf} objects, {csr_bytes} bytes", flush=True)

    # padded: sizes per inner codec (build-once), plus one real sharded zarr
    pad = measure_padded(digests, chunk_cells)
    for tag in pad:
        print(f"  padded[{tag}]: {pad[tag]['bytes']} bytes "
              f"(ratio vs CSR {pad[tag]['bytes']/csr_bytes:.3f})", flush=True)
    real_path, real_nf, real_bytes = write_real_zstd0_shard(digests, chunk_cells)
    print(f"  real zstd0 sharded array: {real_nf} objects, {real_bytes} bytes "
          f"(analytic sum {pad['zstd0']['bytes']})", flush=True)

    pad_one = min(decode_padded_one(real_path) for _ in range(3))
    pad_all_t, _ = decode_padded_all(real_path, len(chunk_cells))

    result = {
        "fixture": fixture,
        "n_granules": len(granules),
        "parent_order": parent_order,
        "child_order": CHILD_ORDER,
        "chunk_inner": CHUNK_INNER,
        "K_inner_chunks_per_shard": K,
        "cells_per_shard": cells_per_shard,
        "cells_per_chunk": CELLS_PER_CHUNK,
        "delta": DELTA,
        "shard_morton": shard,
        "shard_obs": obs,
        "n_shards_total": n_shards,
        "n_cells": len(digests),
        "n_occupied_inner_chunks": len(chunk_cells),
        "total_centroids": total_centroids,
        "mean_centroids_per_cell": mean_centroids,
        "centroid_fill_fraction": mean_centroids / DELTA,
        "csr": {
            "objects": csr_nf,
            "groups": csr_groups,
            "bytes": csr_bytes,
            "decode_one_cell_s": csr_one,
            "decode_whole_shard_s": csr_all_t,
        },
        "padded": pad,
        "padded_real_zstd0": {"objects": real_nf, "bytes": real_bytes},
        "padded_decode_one_cell_s": pad_one,
        "padded_decode_whole_shard_s": pad_all_t,
        "ratios_padded_over_csr": {t: pad[t]["bytes"] / csr_bytes for t in pad},
    }
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  wrote {out_path}", flush=True)


if __name__ == "__main__":
    main()
