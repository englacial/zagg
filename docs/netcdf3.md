# netCDF3 ingest: the catalog-time index route

zagg's worker reads chunked HDF5 through h5coro. A netCDF3 product (classic
`CDF\x01`, 64-bit offset `CDF\x02`) has no HDF5 metadata to walk -- but it
does not need any. A netCDF3 file is a small header (dimensions, attributes,
and one record per variable carrying its type, dimensions and file offset)
followed by uncompressed big-endian arrays: non-record variables are one
contiguous block each, record variables are interleaved along the unlimited
dimension, one slab per record. That layout *is* a chunk manifest, so the
format is handled as a **catalog-time indexing problem**
([issue #682](https://github.com/englacial/zagg/issues/682)): the header is
parsed once, off-Lambda, into the same granule-keyed sidecar manifest the
`inline` backend's write-back produces, and the existing `sidecar` index
backend fetches byte ranges and decodes them on the worker with no new
worker code or dependency.

```
catalog (CMR, .nc hrefs)  ──►  describe() / index_granules()  ──►  <store>/<granule_id>.parquet
                                     header bytes only                       │
                                                                             ▼
worker: reader h5coro + index {backend: sidecar, on_miss: error}  ◄──  ranged GETs of the
        opens the granule as a manifest-only ranged handle              manifest's byte ranges
```

## Introspection

`zagg.catalog.netcdf3.describe` reads header bytes only -- one ranged read of
64 KiB (`INITIAL_HEADER_BYTES`), doubling only when a field runs past the
window, capped at `MAX_HEADER_BYTES` -- and returns a JSON-ready description.
The source may be raw bytes, a local path, any object with an h5coro-style
`read(pos, size)`, or a URL opened through an h5coro driver chosen by scheme
(`s3://` → `S3Driver`, `http(s)://` → `HTTPDriver` with an EDL bearer token as
`credentials`):

```python
from zagg.auth import get_edl_token
from zagg.catalog.netcdf3 import describe

d = describe(
    "https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/"
    "ICEBRIDGE-Related/RDWES1B/1/2025/05/15/"
    "RDWES1B_CS_OFFL_SIR_SAR_1B_20250515T233107_20250515T233632_E001.nc",
    credentials=get_edl_token(),
)
d["format"], d["numrecs"], d["dims"]
# ('classic', 0, {'x': 3921})
d["variables"]["elev"]
# {'dtype': '>f4', 'nc_type': 'float', 'dims': ['x'], 'shape': [3921],
#  'attrs': {'units': 'Meters', 'long_name': 'Elevation'}, 'record': False,
#  'begin': 35316, 'vsize': 15684}
```

Attribute values come back as plain Python (`char` → `str` with trailing
NULs stripped, numeric → scalar or list), so `units`, `_FillValue`,
`scale_factor` and `add_offset` read directly. `numrecs` is `None` for a
file written in streaming mode. CDF-5 (`CDF\x05`) is refused with a clear
message; it is out of scope.

## Building the sidecar store

`build_manifest` turns the header into the manifest frame
(`zagg.index.inline.MANIFEST_DTYPES`, the columns
`h5coro_hidefix.manifest.MANIFEST_COLUMNS` consumes); `index_granule`
persists it through the `inline` backend's own `write_manifest`, so the store
layout is identical to a write-back run's; `index_granules` pools that over a
list of hrefs (a shard map's granules) and tolerates a bad granule:

```python
from zagg.auth import get_nsidc_s3_credentials
from zagg.catalog.netcdf3 import index_granules

keys = index_granules(
    [entry["url"] for entry in shardmap.granules()],   # s3://... hrefs
    "s3://your-bucket/zagg-index/RDWES1B/1/",
    credentials=get_nsidc_s3_credentials(),
)
```

Manifest rows, per variable:

| variable kind | rows | `chunk_shape` | `byte_offset` | `nbytes` |
|---|---|---|---|---|
| non-record | 1 (`chunk_idx` 0) | `shape` | `begin` | unpadded array bytes |
| the only record variable | 1 (`chunk_idx` 0) | `shape` | `begin` | `numrecs * slab` (packed, no gaps) |
| one of several record variables | one per record | `(1, *fixed dims)` | `begin + rec * recsize` | unpadded slab bytes |

`dtype` is the byte-order-explicit numpy string (`>i2`, `>f4`, `>f8`; `char`
→ `|u1`, decoded as code points), `gzip` / `shuffle` are false and
`filter_mask` is 0 -- netCDF3 is never compressed. `recsize` follows the
spec's padding rule (each slab padded to 4 bytes, except a lone record
variable, whose slabs are packed); the header's `vsize` is informational
only and never used as a stride. Skipped with a log line: scalar variables
(the compiled reader supports ranks 1-9) and record variables of a file with
zero records. A streaming-mode file needs `file_size=` to derive its record
count.

## Reading

The config is an ordinary `reader: h5coro` source whose dataset paths are
`/<variable>` and whose index is the `sidecar` backend with `on_miss: error`
(packaged: `cryosat2_rdwes1b_healpix.yaml`):

```yaml
data_source:
  reader: h5coro
  driver: s3
  groups: [root]
  coordinates: {latitude: "/lat", longitude: "/lon"}
  variables: {elev: "/elev"}
  index:
    backend: sidecar
    store: s3://your-bucket/zagg-index/RDWES1B/1/
    on_miss: error
```

The worker opens every granule with h5coro first; when h5coro rejects the
signature and the first four bytes are a netCDF3 magic, it opens a
manifest-only ranged handle instead (`zagg.processing.granule.open_granule`)
-- the subset of `H5Coro` the sidecar backend uses (`resource`, `ioRequest`,
`close`). No config key decides this; the file's own magic does, so one
shard map may mix HDF5 and netCDF3 granules. Any metadata-driven read on
that handle (the default `inline` backend, `on_miss: fallback`, `on_miss:
build`) fails loudly per group with a message naming the sidecar route, and
the shard counts it in `read_errors` rather than decoding garbage. That is
why the template pins `on_miss: error`.

The end-to-end proof is `tests/test_netcdf3_e2e.py`: a classic and a 64-bit
offset file with non-record coordinates and record variables aggregate
through `process_shard` to the same bytes, and the only reads after the
rejected superblock attempt and the magic peek are the manifest's byte
ranges.

## Verified collections

Queried CMR for cloud-hosted collections whose granule format is netCDF-3
(`granule_data_format=netCDF-3`, 143 collections on 2026-10-31) and read the
first four bytes of a recent granule over the public HTTPS endpoint:

| collection | provider | magic | layout |
|---|---|---|---|
| RDWES1B (CryoSat-2 L1B sea-ice elevation) | NSIDC_CPRD | `CDF\x01` | 22 non-record float variables over `x`; the packaged template |
| IR2HI1B (IceBridge HiCARS 2 L1B echo profiles) | NSIDC_CPRD | `CDF\x01` | record variables over `time` incl. `[time, 3200]` waveforms -- the per-record-slab form |
| TL2CO2LN (TES/Aura L2 CO2 Lite) | LARC_CLOUD | `\x89HDF` | labelled netCDF-3 in CMR but is HDF5: the magic, not the label, decides |

## Limits

- CDF-5, compressed netCDF (does not exist), HDF4 (the DMR++ route, separate
  issue).
- A scalar variable is not addressable (rank 0); `char` variables decode as
  `|u1` code points.
- When several record variables interleave, each costs one manifest row per
  record: a file with millions of records makes a large manifest (the backend
  has no stride). A lone record variable and every non-record variable are
  one row regardless of size.
