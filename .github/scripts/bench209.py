import sys, time, asyncio
import numpy as np
import zarr
from zarr.codecs import VLenBytesCodec, ZstdCodec
from zarr.storage import MemoryStore
sys.path.insert(0, "/home/user/zagg/src")
from zagg.csr import write_csr

class CountingStore(MemoryStore):
    def __init__(self):
        super().__init__()
        self.gets = []; self.sets = []
    async def get(self, key, prototype, byte_range=None):
        r = await super().get(key, prototype, byte_range)
        if r is not None:
            self.gets.append((key, byte_range, len(r)))
        return r
    async def set(self, key, value):
        self.sets.append((key, len(value)))
        await super().set(key, value)
    async def set_if_not_exists(self, key, value):
        self.sets.append((key, len(value)))
        await super().set_if_not_exists(key, value)

def store_stats(store):
    async def go():
        n, total = 0, 0
        from zarr.core.buffer import default_buffer_prototype
        async for k in store.list():
            b = await store.get(k, prototype=default_buffer_prototype())
            n += 1; total += len(b)
        return n, total
    return asyncio.run(go())

rng = np.random.default_rng(42)
N_INNER = 256
CELLS_PER_INNER = 512
N_CELLS = N_INNER * CELLS_PER_INNER   # 131072
OCC_FRAC = 0.92                        # ~120k occupied
DELTA = 512

occupied = rng.random(N_CELLS) < OCC_FRAC
# lognormal centroid counts, median ~10, clipped to [1, 512]
nc = np.clip(np.round(rng.lognormal(np.log(10), 1.1, N_CELLS)).astype(int), 1, DELTA)
nc[~occupied] = 0
print(f"cells={N_CELLS} occupied={occupied.sum()} centroids: median={np.median(nc[occupied]):.0f} "
      f"p95={np.percentile(nc[occupied],95):.0f} max={nc.max()} total={nc.sum()}")
raw_bytes = nc.sum() * 2 * 4
print(f"raw payload (values only): {raw_bytes/1e6:.1f} MB")

# per-cell payloads (n,2) float32: col0 = sorted-ish means, col1 = weights
payloads = [None]*N_CELLS
for i in np.nonzero(occupied)[0]:
    n = nc[i]
    means = np.sort(rng.normal(1500, 300, n)).astype('f4')
    wts = rng.integers(1, 500, n).astype('f4')
    payloads[i] = np.stack([means, wts], axis=1)

results = {}

# ---- A) status quo: one CSR group per inner chunk ----
store = CountingStore()
t0 = time.perf_counter()
for c in range(N_INNER):
    lo, hi = c*CELLS_PER_INNER, (c+1)*CELLS_PER_INNER
    vlist, ids = [], []
    for i in range(lo, hi):
        if payloads[i] is not None:
            vlist.append(payloads[i]); ids.append(i - lo)
    write_csr(store, f"h_tdigest/{c}", vlist, ids, dtype="float32")
tA = time.perf_counter() - t0
nA, bA = store_stats(store)
results['A status-quo (CSR/inner-chunk)'] = (len(store.sets), bA, tA)

# ---- B) option 2: one CSR group per shard ----
store = CountingStore()
t0 = time.perf_counter()
vlist = [p for p in payloads if p is not None]
ids = [i for i,p in enumerate(payloads) if p is not None]
write_csr(store, "h_tdigest/shard0", vlist, ids, dtype="float32")
tB = time.perf_counter() - t0
nB, bB = store_stats(store)
results['B opt2 (CSR/shard)'] = (len(store.sets), bB, tB)
storeB = store

# ---- D) option-3 emulation: bytes dtype + vlen-bytes + zstd + sharding ----
store = CountingStore()
t0 = time.perf_counter()
a = zarr.create_array(store, name="h_tdigest", shape=(N_CELLS,), chunks=(CELLS_PER_INNER,),
                      shards=(N_CELLS,), dtype="bytes", serializer=VLenBytesCodec(),
                      compressors=[ZstdCodec()], fill_value=b"")
obj = np.array([p.tobytes() if p is not None else b"" for p in payloads], dtype=object)
a[:] = obj
tD = time.perf_counter() - t0
nD, bD = store_stats(store)
results['D opt3-emu (vlen-bytes+shard)'] = (len(store.sets), bD, tD)
storeD = store

print(f"\n{'variant':38s} {'PUTs':>6s} {'stored MB':>10s} {'write s':>8s}")
for k,(n,b,t) in results.items():
    print(f"{k:38s} {n:6d} {b/1e6:10.2f} {t:8.2f}")
