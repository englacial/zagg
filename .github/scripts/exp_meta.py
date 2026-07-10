import asyncio, json
import numpy as np, zarr
from zarr.codecs import VLenBytesCodec, ZstdCodec
from zarr.storage import MemoryStore

# bytes dtype + vlen-bytes + zstd + sharding
store = MemoryStore()
a = zarr.create_array(store, name="b", shape=(64,), chunks=(4,), shards=(16,),
                      dtype="bytes", serializer=VLenBytesCodec(), compressors=[ZstdCodec()])
rng = np.random.default_rng(0)
data = np.array([rng.random(int(rng.integers(1,40))*2).astype('f4').tobytes() for _ in range(64)], dtype=object)
a[:] = data
back = a[:]
print("bytes+sharding roundtrip:", all(bytes(x)==bytes(y) for x,y in zip(back, data)))
async def get(k):
    from zarr.core.buffer import default_buffer_prototype
    return (await store.get(k, prototype=default_buffer_prototype())).to_bytes()
meta = json.loads(asyncio.run(get("b/zarr.json")))
print(json.dumps(meta, indent=1))
# single-element read
print("single elem:", np.frombuffer(a[3], dtype='f4')[:4])
