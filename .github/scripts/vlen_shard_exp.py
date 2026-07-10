import traceback
import numpy as np
import zarr
from zarr.codecs import ShardingCodec, VLenUTF8Codec, ZstdCodec, BytesCodec
from zarr.storage import MemoryStore

print("zarr", zarr.__version__)

# (i) variable-length string dtype + ShardingCodec(vlen-utf8 inner)
try:
    store = MemoryStore()
    a = zarr.create_array(
        store, name="s", shape=(64,), chunks=(4,), shards=(16,),
        dtype="str",
        serializer=VLenUTF8Codec(),
        compressors=[ZstdCodec()],
    )
    data = np.array([("x" * (i % 7)) for i in range(64)], dtype=object)
    a[:] = data
    back = a[:]
    print("(i) SUCCESS str+sharding roundtrip:", (back == data).all())

    import asyncio
    async def lk(): return [k async for k in store.list()]
    print("(i) store keys:", asyncio.run(lk()))
except Exception as e:
    print("(i) FAILED:", type(e).__name__, e)
    traceback.print_exc()

# (ii) explicit ShardingCodec object construction
try:
    store2 = MemoryStore()
    codec = ShardingCodec(chunk_shape=(4,), codecs=[VLenUTF8Codec(), ZstdCodec()])
    a2 = zarr.create_array(store2, name="s2", shape=(64,), chunks=(16,), dtype="str",
                           serializer=codec, compressors=None)
    a2[:] = data
    print("(ii) explicit ShardingCodec SUCCESS:", (a2[:] == data).all())
except Exception as e:
    print("(ii) FAILED:", type(e).__name__, e)

# (iii) object dtype of variable-length float arrays (VLenArray-style)
try:
    store3 = MemoryStore()
    a3 = zarr.create_array(store3, name="o", shape=(8,), chunks=(2,), dtype=object)
    print("(iii) object dtype create SUCCESS?", a3)
except Exception as e:
    print("(iii) object-dtype FAILED:", type(e).__name__, e)

# (iv) what ZDTypes exist
from zarr.core.dtype import data_type_registry
data_type_registry.lazy_load()
print("(iv) registered v3 dtypes:", sorted(data_type_registry.contents.keys()))
