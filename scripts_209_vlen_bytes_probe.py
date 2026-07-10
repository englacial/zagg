"""Issue #209 cheap-probe (step 2): ragged t-digest as a sharded vlen-``bytes`` array.

Standalone (NOT wired into ``src/zagg``). Builds the real per-cell t-digest
(delta=512) for two shards via the actual worker (``process_shard``), then writes
each shard's ragged field as ONE Zarr v3 ``bytes`` (vlen-bytes) array whose inner
codec chain ``[vlen-bytes, zstd]`` rides *inside* a ``ShardingCodec`` -- so the K
inner chunks bundle to one object per shard. Each cell's value is the raw f32 bytes
of its ``(n_centroids, 2)`` centroids; the per-cell shape is recovered from
``len(bytes) // 8`` (recorded in array attrs). This is option 3's storage layout
with an interim registered dtype (zero new extension code), per the #209 deep-dive.

Compares against the status-quo per-inner-chunk CSR (``zagg.csr``) on the same
fixtures/shards/delta as Options A/B, and verifies the four "lands cleanly" gates:
round-trip bit-exactness, 1 object/shard, real 2-GET single-cell reads, and
empty-inner-chunk omission.

Run: ``uv run python scripts_209_vlen_bytes_probe.py [neon|rgt|both]``
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import time
import warnings

import numpy as np
import zarr
from zarr.codecs import VLenBytesCodec, ZstdCodec
from zarr.core.buffer import default_buffer_prototype as _dbp
from zarr.storage import MemoryStore

from h5coro import filedriver

from zagg.config import load_config
from zagg.csr import read_csr, write_csr
from zagg.grids import from_config
from zagg.processing import process_shard

warnings.filterwarnings("ignore")  # silence per-cell numeric warnings; #209 warts captured explicitly

SCRATCH = os.path.expanduser(
    "/private/tmp/claude-501/-Users-espg-software-zagg/"
    "2531f603-b5ed-4ebf-b97e-2461d627df9e/scratchpad"
)
os.makedirs(SCRATCH, exist_ok=True)

DELTA = 512
FIELD = "h_tdigest"
SENTINEL = 2**64 - 1  # ShardingCodec empty-inner-chunk index marker


# --------------------------------------------------------------------------- #
# Instrumented store: count GETs (key, byte_range, bytes) and SETs.
# --------------------------------------------------------------------------- #
class CountingStore(MemoryStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gets: list = []
        self.sets: list = []

    def with_read_only(self, read_only=True):
        s = CountingStore(store_dict=self._store_dict, read_only=read_only)
        s.gets = self.gets
        s.sets = self.sets
        return s

    async def get(self, key, prototype, byte_range=None):
        r = await super().get(key, prototype, byte_range)
        if r is not None:
            self.gets.append((key, byte_range, len(r)))
        return r

    async def set(self, key, value):
        self.sets.append((key, len(value)))
        await super().set(key, value)


def _unique_objects(store):
    async def go():
        return [k async for k in store.list()]

    return asyncio.run(go())


def _raw_get(store, key):
    async def go():
        r = await store.get(key, prototype=_dbp())
        return r.to_bytes() if r is not None else None

    return asyncio.run(go())


# --------------------------------------------------------------------------- #
# Collect per-cell digests from the real worker, grouped by inner chunk.
# (Same recipe as the Options A/B measurement scripts, so numbers compare.)
# --------------------------------------------------------------------------- #
def collect_digests(config, grid, shard_key, paths):
    chunk_results: list = []
    _df, meta = process_shard(
        grid,
        int(shard_key),
        paths,
        s3_credentials={},
        config=config,
        driver="https",  # identity url-rewriter; FileDriver reads the local path
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
        vals = [np.ascontiguousarray(v, dtype=np.float32) for _, v in keep]
        per_chunk.append((i, cids, vals))
    return per_chunk, meta


# --------------------------------------------------------------------------- #
# Status-quo: per-inner-chunk CSR subgroups (production layout).
# --------------------------------------------------------------------------- #
def _dir_bytes_and_objs(path):
    total = nobj = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
            nobj += 1
    return total, nobj


def measure_current_csr(per_chunk, workdir):
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    store = zarr.storage.LocalStore(workdir)
    chunk_prefixes = []
    t0 = time.perf_counter()
    for chunk_ord, cids, vals in per_chunk:
        prefix = f"{FIELD}/{chunk_ord}"
        write_csr(store, prefix, vals, list(cids), dtype="float32")
        chunk_prefixes.append(prefix)
    t_write = time.perf_counter() - t0
    bytes_on_disk, nobj = _dir_bytes_and_objs(workdir)

    t0 = time.perf_counter()
    for prefix in chunk_prefixes:
        _csr = read_csr(store, prefix)
    t_whole = time.perf_counter() - t0

    # one-cell read: read ONLY the chunk subgroup holding the densest cell.
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
        "objects": nobj,
        "populated_chunks": len(chunk_prefixes),
        "bytes_on_disk": bytes_on_disk,
        "write_s": t_write,
        "read_whole_s": t_whole,
        "read_one_cell_s": t_one,
        "densest_cell_rows": int(best[0]),
    }


# --------------------------------------------------------------------------- #
# The prototype: ragged field as ONE sharded vlen-``bytes`` array.
# --------------------------------------------------------------------------- #
def measure_vlen_bytes(per_chunk, K, cells_per_chunk):
    N = K * cells_per_chunk

    # Build the shard-wide object array. Each populated cell holds the raw f32
    # bytes of its (n, 2) centroids; empty cells are b"". Cell placement mirrors
    # the dense-array layout: global position = inner_chunk_ord * cells_per_chunk
    # + local_cell_id, so inner chunk i maps 1:1 to Zarr inner chunk i.
    obj = np.empty(N, dtype=object)
    obj[:] = b""
    inputs: dict[int, np.ndarray] = {}
    for chunk_ord, cids, vals in per_chunk:
        base = chunk_ord * cells_per_chunk
        for c, v in zip(cids, vals):
            gpos = base + int(c)
            obj[gpos] = v.tobytes()
            inputs[gpos] = v
    n_cells = len(inputs)

    store = CountingStore()
    with warnings.catch_warnings(record=True) as wlog:
        warnings.simplefilter("always")
        d = zarr.create_array(
            store,
            name=FIELD,
            shape=(N,),
            chunks=(cells_per_chunk,),  # K inner chunks per shard
            shards=(N,),  # one ShardingCodec object spanning the whole shard
            dtype="bytes",
            serializer=VLenBytesCodec(),
            compressors=[ZstdCodec()],
            fill_value=b"",
        )
        # record shape convention in attrs: n_centroids = len(bytes) // (2 * 4)
        d.attrs["ragged_shape"] = "(len_bytes // 8, 2)"
        d.attrs["ragged_dtype"] = "float32"
        d.attrs["field"] = FIELD
        t0 = time.perf_counter()
        d[:] = obj
        t_write = time.perf_counter() - t0
    warn_names = sorted({w.category.__name__ for w in wlog})

    stored = sum(s for k, s in store.sets if not k.endswith("zarr.json"))
    data_objs = [k for k in _unique_objects(store) if not k.endswith("zarr.json")]

    codecs_meta = json.loads(_raw_get(store, f"{FIELD}/zarr.json"))["codecs"]

    # ---- whole-shard read ----
    t0 = time.perf_counter()
    alld = d[:]
    decoded = [np.frombuffer(bytes(b), "f4").reshape(-1, 2) for b in alld if len(b)]
    t_whole = time.perf_counter() - t0

    # ---- round-trip correctness: reopen fresh (exercises the dtype-name round
    # trip through metadata, #3517), then compare EVERY populated cell bit-exact.
    d2 = zarr.open_array(store, path=FIELD, mode="r")
    alld2 = d2[:]
    mismatches = 0
    for gpos, v in inputs.items():
        got = np.frombuffer(bytes(alld2[gpos]), "f4").reshape(-1, 2)
        if got.shape != v.shape or not np.array_equal(got, v):
            mismatches += 1
    roundtrip_ok = mismatches == 0

    # ---- single-cell partial read: densest cell, count GETs + bytes moved ----
    dense_gpos = max(inputs, key=lambda g: inputs[g].shape[0])
    store.gets.clear()
    t0 = time.perf_counter()
    cell = d[dense_gpos : dense_gpos + 1]
    t_one = time.perf_counter() - t0
    gets = list(store.gets)
    one_val = np.frombuffer(bytes(cell[0]), "f4").reshape(-1, 2)
    one_bitexact = np.array_equal(one_val, inputs[dense_gpos])
    one_get_bytes = sum(g[2] for g in gets)

    # ---- empty-inner-chunk omission: parse the shard footer index (K u64 pairs
    # + crc32c), count sentinel entries. ----
    shard = _raw_get(store, f"{FIELD}/c/0")
    idx = np.frombuffer(shard[-(16 * K + 4) : -4], dtype="<u8").reshape(K, 2)
    empty_inner = int((idx == SENTINEL).all(axis=1).sum())
    populated_inner = K - empty_inner

    return {
        "n_cells": n_cells,
        "total_centroids": int(sum(v.shape[0] for v in inputs.values())),
        "objects": len(data_objs),  # data objects/shard (excl. zarr.json)
        "data_object_keys": data_objs,
        "stored_bytes": int(stored),
        "write_s": t_write,
        "read_whole_s": t_whole,
        "single_cell": {
            "gets": len(gets),
            "get_bytes": int(one_get_bytes),
            "read_s": t_one,
            "get_detail": [(k, str(br), nb) for k, br, nb in gets],
            "bitexact": bool(one_bitexact),
            "densest_cell_rows": int(inputs[dense_gpos].shape[0]),
        },
        "roundtrip_ok": bool(roundtrip_ok),
        "roundtrip_mismatches": int(mismatches),
        "empty_inner_omission": {
            "K": int(K),
            "populated_inner_from_index": int(populated_inner),
            "empty_inner_from_index": int(empty_inner),
            "populated_chunks_input": len(per_chunk),
            "matches": populated_inner == len(per_chunk),
        },
        "warnings_on_create": warn_names,
        "codecs_meta": codecs_meta,
    }


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def run_shard(name, config, grid, shard_key, paths):
    print(f"\n=== {name}: collect digests shard {shard_key} ({len(paths)} gran) ===", flush=True)
    t0 = time.perf_counter()
    per_chunk, meta = collect_digests(config, grid, shard_key, paths)
    K = int(grid.chunks_per_shard)
    cells_per_chunk = int(np.prod(grid.chunk_shape))
    print(
        f"  process_shard {time.perf_counter()-t0:.1f}s obs={meta.get('total_obs')} "
        f"cells={meta.get('cells_with_data')} populated_chunks={len(per_chunk)} "
        f"K={K} cells_per_chunk={cells_per_chunk}",
        flush=True,
    )
    cur = measure_current_csr(per_chunk, os.path.join(SCRATCH, f"cur_{name}"))
    print(f"  current CSR: {cur['objects']} objs, {cur['bytes_on_disk']/1e6:.2f} MB", flush=True)
    vlen = measure_vlen_bytes(per_chunk, K, cells_per_chunk)
    print(
        f"  vlen-bytes: {vlen['objects']} obj, {vlen['stored_bytes']/1e6:.2f} MB, "
        f"roundtrip_ok={vlen['roundtrip_ok']}, single-cell {vlen['single_cell']['gets']} GETs / "
        f"{vlen['single_cell']['get_bytes']/1e3:.1f} KB, "
        f"empty-omit match={vlen['empty_inner_omission']['matches']}",
        flush=True,
    )
    return {
        "shard_key": str(shard_key),
        "K_inner_chunks": K,
        "cells_per_chunk": cells_per_chunk,
        "n_granules": len(paths),
        "total_obs": int(meta.get("total_obs", 0)),
        "delta": DELTA,
        "current_csr": cur,
        "vlen_bytes": vlen,
    }


def neon():
    d = os.path.expanduser("~/ignore/zagg_neon_atl03_test_shard")
    config = load_config(os.path.join(d, "atl03_tdigest_healpix_o8.yaml"))
    config.aggregation["variables"][FIELD]["params"]["delta"] = DELTA
    sm = json.load(open(os.path.join(d, "sm_healpix_o8.json")))
    sk = [int(x) for x in sm["shard_keys"]]
    gr = sm["granules"]
    idx = sorted(range(len(sk)), key=lambda i: -len(gr[i]))[0]  # densest shard
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
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    out = {
        "issue": 209,
        "probe": "step 2 - ragged t-digest as a sharded vlen-bytes array",
        "delta": DELTA,
        "zarr": zarr.__version__,
    }
    if which in ("neon", "both"):
        out["neon_sparse_o8"] = neon()
    if which in ("rgt", "both"):
        out["rgt_dense_o9"] = rgt()
    dst = os.path.join(os.path.dirname(__file__), f"metrics_209_vlen_bytes_{which}.json")
    with open(dst, "w") as fh:
        json.dump(out, fh, indent=2)
    print("\nwrote", dst)
    print(json.dumps(out, indent=2, default=str))
