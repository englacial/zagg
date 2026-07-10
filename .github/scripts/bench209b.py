import sys, time, asyncio, warnings
import numpy as np
import zarr
from zarr.codecs import VLenBytesCodec, ZstdCodec
from zarr.storage import MemoryStore
sys.path.insert(0, "/home/user/zagg/src")
from zagg.csr import write_csr, read_csr, iter_csr_cells
warnings.filterwarnings("ignore")

class CountingStore(MemoryStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs); self.gets=[]; self.sets=[]
    def with_read_only(self, read_only=True):
        s = CountingStore(store_dict=self._store_dict, read_only=read_only)
        s.gets = self.gets; s.sets = self.sets
        return s
    async def get(self, key, prototype, byte_range=None):
        r = await super().get(key, prototype, byte_range)
        if r is not None: self.gets.append((key, byte_range, len(r)))
        return r
    async def set(self, key, value):
        self.sets.append((key, len(value))); await super().set(key, value)

def unique_objects(store):
    async def go():
        return [k async for k in store.list()]
    return asyncio.run(go())

rng = np.random.default_rng(42)
N_INNER, CPI, DELTA = 256, 512, 512
N_CELLS = N_INNER * CPI
occupied = rng.random(N_CELLS) < 0.92
nc = np.clip(np.round(rng.lognormal(np.log(10), 1.1, N_CELLS)).astype(int), 1, DELTA)
nc[~occupied] = 0
payloads = [None]*N_CELLS
for i in np.nonzero(occupied)[0]:
    n = nc[i]
    payloads[i] = np.stack([np.sort(rng.normal(1500,300,n)).astype('f4'),
                            rng.integers(1,500,n).astype('f4')], axis=1)

# ---- C) option 1: padded dense + sharding + zstd ----
storeC = CountingStore()
t0 = time.perf_counter()
slab = np.zeros((N_CELLS, DELTA, 2), dtype='f4')
for i, p in enumerate(payloads):
    if p is not None: slab[i, :len(p)] = p
c = zarr.create_array(storeC, name="h_tdigest", shape=(N_CELLS, DELTA, 2),
                      chunks=(CPI, DELTA, 2), shards=(N_CELLS, DELTA, 2),
                      dtype='f4', compressors=[ZstdCodec()])
c[:] = slab
tC = time.perf_counter()-t0
objsC = unique_objects(storeC)
szC = sum(s for _,s in storeC.sets if not _.endswith('zarr.json'))
print(f"C opt1 padded dense: PUTs={len(storeC.sets)} unique_objs={len(objsC)} "
      f"raw slab={slab.nbytes/1e6:.0f}MB stored={szC/1e6:.2f}MB write={tC:.2f}s")

# rebuild B and D for read tests
storeB = CountingStore()
vlist = [p for p in payloads if p is not None]
ids = [i for i,p in enumerate(payloads) if p is not None]
write_csr(storeB, "h_tdigest/shard0", vlist, ids, dtype="float32")

storeD = CountingStore()
d = zarr.create_array(storeD, name="h_tdigest", shape=(N_CELLS,), chunks=(CPI,),
                      shards=(N_CELLS,), dtype="bytes", serializer=VLenBytesCodec(),
                      compressors=[ZstdCodec()], fill_value=b"")
d[:] = np.array([p.tobytes() if p is not None else b"" for p in payloads], dtype=object)

# ---- whole-shard reads ----
t0=time.perf_counter(); csr = read_csr(storeB, "h_tdigest/shard0"); cells = iter_csr_cells(csr); tRB=time.perf_counter()-t0
t0=time.perf_counter(); allD = d[:]; dec=[np.frombuffer(b,'f4').reshape(-1,2) for b in allD if len(b)]; tRD=time.perf_counter()-t0
t0=time.perf_counter(); allC = c[:]; tRC0=time.perf_counter()-t0
# strip padding using counts (in real life from a companion count or trailing zeros)
t0=time.perf_counter(); stripped=[allC[i,:nc[i]] for i in np.nonzero(occupied)[0]]; tRC1=time.perf_counter()-t0
print(f"whole-shard read: B(csr)={tRB:.2f}s D(vlen)={tRD:.2f}s C(padded)={tRC0:.2f}+{tRC1:.2f}s")

# ---- single-cell random access ----
cell = 70000
storeD.gets.clear()
t0=time.perf_counter(); v = d[cell]; tD1=time.perf_counter()-t0
getsD = list(storeD.gets)
storeC.gets.clear()
t0=time.perf_counter(); vc = c[cell]; tC1=time.perf_counter()-t0
getsC = list(storeC.gets)
storeB.gets.clear()
t0=time.perf_counter()
off = zarr.open_array(storeB, path="h_tdigest/shard0/offsets", mode="r")[:]
cid = zarr.open_array(storeB, path="h_tdigest/shard0/cell_ids", mode="r")[:]
k = np.searchsorted(cid, cell)
vals = zarr.open_array(storeB, path="h_tdigest/shard0/values", mode="r")[off[k]:off[k+1]]
tB1=time.perf_counter()-t0
getsB = list(storeB.gets)
def s(g): return f"{len(g)} GETs, {sum(x[2] for x in g)/1e3:.1f} KB"
print(f"single cell: B(csr) {tB1*1e3:.1f}ms [{s(getsB)}] | C(padded) {tC1*1e3:.1f}ms [{s(getsC)}] | D(vlen) {tD1*1e3:.1f}ms [{s(getsD)}]")
for g in getsD: print("  D GET:", g[0], g[1], g[2])
