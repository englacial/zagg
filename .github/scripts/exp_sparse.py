import warnings, asyncio
import numpy as np, zarr
from zarr.codecs import VLenBytesCodec, ZstdCodec
from zarr.storage import MemoryStore
warnings.filterwarnings("ignore")

store = MemoryStore()
a = zarr.create_array(store, name="b", shape=(64,), chunks=(4,), shards=(64,),
                      dtype="bytes", serializer=VLenBytesCodec(), compressors=[ZstdCodec()],
                      fill_value=b"")
obj = np.array([b""]*64, dtype=object)
obj[0] = b"hello"; obj[1] = b"world"   # only inner chunk 0 populated
a[:] = obj

from zarr.core.buffer import default_buffer_prototype
async def get(k, br=None): 
    r = await store.get(k, prototype=default_buffer_prototype(), byte_range=br)
    return r.to_bytes() if r else None
shard = asyncio.run(get("b/c/0"))
print("shard size:", len(shard))
idx = np.frombuffer(shard[-(16*16+4):-4], dtype="<u8").reshape(16,2)
n_empty = (idx == 2**64-1).all(axis=1).sum()
print(f"inner chunks: 16, marked-empty in index: {n_empty}")
print("roundtrip:", a[0], a[1], repr(a[5]))
