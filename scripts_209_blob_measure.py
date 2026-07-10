"""Issue #209 Option B measurement: one CSR blob per shard vs per-inner-chunk CSR.

Standalone (NOT wired into ``src/zagg``). Builds the per-cell t-digest (delta=512)
for two shards via the real worker (``process_shard``), then compares the current
ragged per-inner-chunk CSR layout against a single-object-per-shard blob encoding.

Run: ``uv run python scripts_209_blob_measure.py [neon|rgt|both]``
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import time

import numpy as np
import zarr
from numcodecs import Blosc, GZip, Zstd

from h5coro import filedriver

from zagg.config import load_config
from zagg.csr import read_csr, write_csr
from zagg.grids import from_config
from zagg.processing import process_shard

SCRATCH = os.path.expanduser(
    "/private/tmp/claude-501/-Users-espg-software-zagg/"
    "2531f603-b5ed-4ebf-b97e-2461d627df9e/scratchpad"
)
os.makedirs(SCRATCH, exist_ok=True)

DELTA = 512
FIELD = "h_tdigest"


# --------------------------------------------------------------------------- #
# Collect per-cell digests from the real worker, grouped by inner chunk.
# --------------------------------------------------------------------------- #
def collect_digests(config, grid, shard_key, paths):
    """Return list of (inner_chunk_ordinal, local_cell_ids, values_list) per
    populated inner chunk, plus meta. Uses the actual process_shard worker (K>1)."""
    chunk_results: list = []
    _df, meta = process_shard(
        grid,
        int(shard_key),
        paths,
        s3_credentials={},
        config=config,
        driver="https",
        h5coro_driver=filedriver.FileDriver,
        handoff="pandas",
        chunk_results=chunk_results,
    )
    per_chunk = []
    for i, (_block_index, _carrier, ragged) in enumerate(chunk_results):
        if not ragged or FIELD not in ragged:
            continue
        entry = ragged[FIELD]
        values_list, cell_ids = entry[0], entry[1]
        if len(values_list) == 0:
            continue
        vlist = [np.asarray(v, dtype=np.float32).reshape(-1, 2) for v in values_list]
        keep = [(int(c), v) for c, v in zip(cell_ids, vlist) if v.size]
        if not keep:
            continue
        cids = np.array([c for c, _ in keep], dtype=np.int64)
        vals = [v for _, v in keep]
        per_chunk.append((i, cids, vals))
    return per_chunk, meta


# --------------------------------------------------------------------------- #
# Measurement helpers
# --------------------------------------------------------------------------- #
def _dir_bytes_and_objs(path):
    total = 0
    nobj = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
            nobj += 1
    return total, nobj


def measure_current_csr(per_chunk, workdir):
    """Write each populated inner chunk as its own CSR subgroup (production layout)
    and measure on-disk bytes + object (file) count."""
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    store = zarr.storage.LocalStore(workdir)
    chunk_prefixes = []
    for chunk_ord, cids, vals in per_chunk:
        prefix = f"{FIELD}/{chunk_ord}"
        write_csr(store, prefix, vals, list(cids), dtype="float32")
        chunk_prefixes.append(prefix)
    bytes_on_disk, nobj = _dir_bytes_and_objs(workdir)

    # whole-shard read: read every chunk subgroup + reconstruct
    t0 = time.perf_counter()
    for prefix in chunk_prefixes:
        _csr = read_csr(store, prefix)
    t_whole = time.perf_counter() - t0

    # one-cell read: read ONLY the chunk subgroup holding the densest cell
    best = None  # (nrows, list_idx, local_pos)
    for li, (_chunk_ord, _cids, vals) in enumerate(per_chunk):
        for pos, v in enumerate(vals):
            if best is None or v.shape[0] > best[0]:
                best = (v.shape[0], li, pos)
    _n, li, pos = best
    target_prefix = chunk_prefixes[li]
    reps = 100
    t0 = time.perf_counter()
    for _ in range(reps):
        csr = read_csr(store, target_prefix)
        off = csr["offsets"]
        _payload = csr["values"][off[pos] : off[pos + 1]]
    t_one = (time.perf_counter() - t0) / reps
    return {
        "bytes_on_disk": bytes_on_disk,
        "objects": nobj,
        "populated_chunks": len(chunk_prefixes),
        "read_whole_s": t_whole,
        "read_one_cell_s": t_one,
        "densest_cell_rows": int(best[0]),
    }


def _flatten_shard(per_chunk, k_cells_per_chunk):
    """Concatenate all cells across chunks into shard-wide CSR arrays."""
    all_vals = []
    cell_ids = []
    lengths = []
    for chunk_ord, cids, vals in per_chunk:
        for c, v in zip(cids, vals):
            all_vals.append(v)
            cell_ids.append(chunk_ord * k_cells_per_chunk + int(c))
            lengths.append(v.shape[0])
    values = np.concatenate(all_vals).astype(np.float32)  # (N,2)
    offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int64)
    cell_ids = np.array(cell_ids, dtype=np.uint32)
    return values, offsets, cell_ids


def _pack_raw(values, offsets, cell_ids):
    """Deterministic raw concatenation of the three arrays with a tiny header."""
    buf = io.BytesIO()
    header = {"values_shape": list(values.shape), "n_cells": int(cell_ids.shape[0])}
    hb = json.dumps(header).encode()
    buf.write(len(hb).to_bytes(4, "little"))
    buf.write(hb)
    buf.write(cell_ids.astype(np.uint32).tobytes())
    buf.write(offsets.astype(np.int64).tobytes())
    buf.write(np.ascontiguousarray(values, dtype=np.float32).tobytes())
    return buf.getvalue()


def _unpack_raw(raw):
    mv = memoryview(raw)
    hlen = int.from_bytes(mv[:4], "little")
    header = json.loads(bytes(mv[4 : 4 + hlen]))
    off = 4 + hlen
    n = header["n_cells"]
    cell_ids = np.frombuffer(mv[off : off + 4 * n], dtype=np.uint32)
    off += 4 * n
    offsets = np.frombuffer(mv[off : off + 8 * (n + 1)], dtype=np.int64)
    off += 8 * (n + 1)
    vshape = tuple(header["values_shape"])
    values = np.frombuffer(mv[off:], dtype=np.float32).reshape(vshape)
    return values, offsets, cell_ids


def measure_blob(per_chunk, k_cells_per_chunk, workdir):
    values, offsets, cell_ids = _flatten_shard(per_chunk, k_cells_per_chunk)
    n_cells = int(cell_ids.shape[0])
    total_centroids = int(values.shape[0])
    raw = _pack_raw(values, offsets, cell_ids)
    raw_bytes = len(raw)

    results = {
        "n_cells": n_cells,
        "total_centroids": total_centroids,
        "raw_uncompressed_bytes": raw_bytes,
        "compressors": {},
    }

    codecs = {
        "zstd-3": Zstd(level=3),
        "zstd-9": Zstd(level=9),
        "zstd-19": Zstd(level=19),
        "blosc-zstd-9-shuffle": Blosc(cname="zstd", clevel=9, shuffle=Blosc.SHUFFLE),
        "gzip-6": GZip(level=6),
    }
    for name, codec in codecs.items():
        comp = bytes(codec.encode(raw))
        cb = len(comp)
        reps = 20
        t0 = time.perf_counter()
        for _ in range(reps):
            dec = codec.decode(comp)
            _v, _o, _c = _unpack_raw(bytes(dec) if not isinstance(dec, bytes) else dec)
        t_whole = (time.perf_counter() - t0) / reps
        results["compressors"][name] = {
            "compressed_bytes": cb,
            "ratio": round(raw_bytes / cb, 3),
            "read_whole_s": t_whole,
        }

    # npz (zip + deflate)
    buf = io.BytesIO()
    np.savez_compressed(buf, values=values, offsets=offsets, cell_ids=cell_ids)
    npz_blob = buf.getvalue()
    reps = 20
    t0 = time.perf_counter()
    for _ in range(reps):
        z = np.load(io.BytesIO(npz_blob))
        _v, _o, _c = z["values"], z["offsets"], z["cell_ids"]
    t_npz = (time.perf_counter() - t0) / reps
    results["compressors"]["npz-deflate"] = {
        "compressed_bytes": len(npz_blob),
        "ratio": round(raw_bytes / len(npz_blob), 3),
        "read_whole_s": t_npz,
    }

    # Arrow IPC (pyarrow) with zstd, values as a list<float32> column per cell
    try:
        import pyarrow as pa

        flat = values.reshape(-1)
        list_arr = pa.ListArray.from_arrays(
            pa.array(offsets.astype(np.int32) * 2, type=pa.int32()), pa.array(flat)
        )
        tbl = pa.table({"cell_ids": pa.array(cell_ids), "digest": list_arr})
        sink = pa.BufferOutputStream()
        opts = pa.ipc.IpcWriteOptions(compression="zstd")
        with pa.ipc.new_file(sink, tbl.schema, options=opts) as w:
            w.write_table(tbl)
        arrow_bytes = sink.getvalue().to_pybytes()
        reps = 20
        t0 = time.perf_counter()
        for _ in range(reps):
            reader = pa.ipc.open_file(pa.py_buffer(arrow_bytes))
            _t = reader.read_all()
        t_arrow = (time.perf_counter() - t0) / reps
        results["compressors"]["arrow-ipc-zstd"] = {
            "compressed_bytes": len(arrow_bytes),
            "ratio": round(raw_bytes / len(arrow_bytes), 3),
            "read_whole_s": t_arrow,
        }
    except Exception as e:  # noqa: BLE001
        results["compressors"]["arrow-ipc-zstd"] = {"error": repr(e)}

    # ---- chosen format: raw + zstd-19 -> write ONE object to disk, measure ----
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)
    codec = Zstd(level=19)
    blob = bytes(codec.encode(raw))
    blob_path = os.path.join(workdir, f"{FIELD}.blob")
    with open(blob_path, "wb") as fh:
        fh.write(blob)
    on_disk, nobj = _dir_bytes_and_objs(workdir)

    # one-cell read: fetch+decode the WHOLE object, then index one cell (O(1)).
    dense_pos = int(np.argmax(np.diff(offsets)))
    reps = 20
    t0 = time.perf_counter()
    for _ in range(reps):
        with open(blob_path, "rb") as fh:
            data = fh.read()
        dec = bytes(codec.decode(data))
        v, off, _c = _unpack_raw(dec)
        _payload = v[off[dense_pos] : off[dense_pos + 1]]
    t_one = (time.perf_counter() - t0) / reps

    results["chosen"] = {
        "format": "raw-concat + zstd-19 (single object)",
        "objects": nobj,
        "bytes_on_disk": on_disk,
        "read_one_cell_s": t_one,
    }
    return results


def run_shard(name, config, grid, shard_key, paths):
    print(f"\n=== {name}: collect digests shard {shard_key} ({len(paths)} gran) ===", flush=True)
    t0 = time.perf_counter()
    per_chunk, meta = collect_digests(config, grid, shard_key, paths)
    print(
        f"  process_shard {time.perf_counter()-t0:.1f}s obs={meta.get('total_obs')} "
        f"cells={meta.get('cells_with_data')} populated_chunks={len(per_chunk)}",
        flush=True,
    )
    K = grid.chunks_per_shard
    k_cells_per_chunk = int(np.prod(grid.chunk_shape))
    cur = measure_current_csr(per_chunk, os.path.join(SCRATCH, f"cur_{name}"))
    blob = measure_blob(per_chunk, k_cells_per_chunk, os.path.join(SCRATCH, f"blob_{name}"))
    return {
        "shard_key": str(shard_key),
        "K_inner_chunks": int(K),
        "cells_per_chunk": k_cells_per_chunk,
        "n_granules": len(paths),
        "total_obs": int(meta.get("total_obs", 0)),
        "delta": DELTA,
        "current_csr": cur,
        "blob": blob,
    }


def neon():
    d = os.path.expanduser("~/ignore/zagg_neon_atl03_test_shard")
    config = load_config(os.path.join(d, "atl03_tdigest_healpix_o8.yaml"))
    config.aggregation["variables"][FIELD]["params"]["delta"] = DELTA
    sm = json.load(open(os.path.join(d, "sm_healpix_o8.json")))
    sk = [int(x) for x in sm["shard_keys"]]
    gr = sm["granules"]
    order = sorted(range(len(sk)), key=lambda i: -len(gr[i]))
    idx = order[0]
    shard_key = sk[idx]
    gdir = os.path.join(d, "granules")
    paths = [os.path.join(gdir, e["https"].split("/")[-1]) for e in gr[idx]]
    paths = [p for p in paths if os.path.exists(p)]
    grid = from_config(config, populated_shards=sk)
    return run_shard("neon_o8", config, grid, shard_key, paths)


def rgt():
    """Dense shard: RGT 1336 repeat-track granules -> densest o9 parent shard."""
    from rgt_shardmap import build_rgt

    d = os.path.expanduser("~/ignore/atl03_1336_r05")
    cfg_path = os.path.join(
        os.path.dirname(__file__),
        "tests/data/benchmark/configs/atl03_tdigest_healpix_o9.yaml",
    )
    config = load_config(cfg_path)
    config.aggregation["variables"][FIELD]["params"]["delta"] = DELTA
    shard_key, paths = build_rgt(config, d)
    grid = from_config(config, populated_shards=[shard_key])
    return run_shard("rgt_o9", config, grid, shard_key, paths)


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "neon"
    out = {}
    if which in ("neon", "both"):
        out["neon"] = neon()
    if which in ("rgt", "both"):
        out["rgt"] = rgt()
    with open(os.path.join(SCRATCH, f"metrics_209_{which}.json"), "w") as fh:
        json.dump(out, fh, indent=2)
    print(json.dumps(out, indent=2))
