"""Raster (GeoTIFF/COG) read path: pull-NN sampling at grid cell centers.

Issue #218. The decode engine is **async-tiff** (espg-ratified on the issue):
byte-range tile reads through an obspec store, Rust-side decode, typed numpy
buffers. zagg owns the mapping — ``grid.sample()`` turns cell centers into
source-pixel indices, and this module fetches exactly the COG tiles those
indices touch and gathers per-cell values. The engine never touches the
output side (HEALPix emission stays pure mortie/pyproj), and GDAL is never
involved.

Sync facade over async-tiff's async API: worker call sites are synchronous
(one shard per Lambda invoke), so :func:`sample_asset` runs its own event
loop; the per-asset tile fan-out inside it is concurrent.

Georeferencing is read from the GeoTIFF IFD itself (geo keys + pixel scale +
tiepoint), so a granule entry needs only the asset href — no STAC ``proj:*``
carriage through the shardmap.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import re
from urllib.parse import urlparse

import numpy as np

# TIFF SampleFormat x BitsPerSample -> numpy dtype, for sizing the fill return
# when a shard has no valid cells (no tile fetched, so no decoded buffer to
# take a dtype from). 1=unsigned int, 2=signed int, 3=IEEE float.
_DTYPES = {
    (1, 8): np.uint8,
    (1, 16): np.uint16,
    (1, 32): np.uint32,
    (2, 8): np.int8,
    (2, 16): np.int16,
    (2, 32): np.int32,
    (3, 32): np.float32,
    (3, 64): np.float64,
}

_S3_VHOST = re.compile(r"(?P<bucket>.+)\.s3[.-](?P<region>[a-z0-9-]+)\.amazonaws\.com$")


def _geo_from_ifd(ifd) -> tuple[int, tuple[float, float, float, float, float, float]]:
    """``(epsg, affine)`` from a GeoTIFF IFD, STAC ``proj:transform`` order.

    Supports the ModelPixelScale + ModelTiepoint form (what COGs write); the
    full ModelTransformation matrix form raises rather than misgeoreference.
    """
    gkd = ifd.geo_key_directory
    if gkd is None:
        raise ValueError("not a GeoTIFF: no GeoKeyDirectory")
    epsg = gkd.projected_type or gkd.geographic_type
    if not epsg:
        raise ValueError("GeoKeyDirectory carries no projected or geographic EPSG code")
    scale, tie = ifd.model_pixel_scale, ifd.model_tiepoint
    if not scale or not tie:
        raise ValueError(
            "only ModelPixelScale+ModelTiepoint GeoTIFFs are supported "
            "(no pixel scale / tiepoint tags found)"
        )
    sx, sy = float(scale[0]), float(scale[1])
    i, j, _, x, y, _ = (float(v) for v in tie[:6])
    return int(epsg), (sx, 0.0, x - i * sx, 0.0, -sy, y + j * sy)


def _store_and_path(href: str, *, region: str | None = None, anonymous: bool = True):
    """obspec store + in-store path for an asset href.

    Handles ``s3://bucket/key``, virtual-hosted S3 HTTPS
    (``https://bucket.s3.region.amazonaws.com/key`` -- what Earth Search
    asset hrefs look like), plain HTTPS, and local paths.
    """
    from async_tiff.store import HTTPStore, LocalStore, S3Store

    u = urlparse(href)
    if u.scheme == "s3":
        kw: dict = {"skip_signature": True} if anonymous else {}
        if region:
            kw["region"] = region
        return S3Store(u.netloc, **kw), u.path.lstrip("/")
    if u.scheme in ("http", "https"):
        m = _S3_VHOST.match(u.netloc)
        if m:
            kw = {"region": region or m["region"]}
            if anonymous:
                kw["skip_signature"] = True
            return S3Store(m["bucket"], **kw), u.path.lstrip("/")
        return HTTPStore(f"{u.scheme}://{u.netloc}"), u.path.lstrip("/")
    import os

    d, name = os.path.split(href)
    return LocalStore(d or "."), name


async def sample_asset_async(
    grid,
    cells,
    href: str,
    *,
    region: str | None = None,
    anonymous: bool = True,
    fill=0,
):
    """Pull-NN sample one raster asset at the centers of ``cells``.

    Fetches only the tiles the sampled pixels touch (concurrently), decodes,
    and gathers one value per cell.

    Returns
    -------
    (values, valid)
        ``values`` in the asset's dtype (``fill`` where invalid); ``valid``
        True where the cell center lands on a source pixel. Nodata masking is
        the caller's concern (e.g. Sentinel-2 encodes nodata as DN 0).

    ``fill`` must be representable in the asset's dtype; a non-fitting sentinel
    (e.g. ``-1`` or ``NaN`` into a ``uint16`` band) raises ``ValueError`` up
    front rather than failing opaquely inside the gather.
    """
    values, valid, _center = await _sample_one(
        grid, cells, href, region=region, anonymous=anonymous, fill=fill
    )
    return values, valid


async def _sample_one(
    grid,
    cells,
    href: str,
    *,
    region: str | None = None,
    anonymous: bool = True,
    fill=0,
):
    """:func:`sample_asset_async` body, also returning the raster's center
    ``(lon, lat)`` — the ownership rule's tile-center input (#218)."""
    from async_tiff import TIFF

    store, path = _store_and_path(href, region=region, anonymous=anonymous)
    tiff = await TIFF.open(path, store=store)
    ifd = tiff.ifds[0]
    if len(ifd.bits_per_sample) != 1:
        raise ValueError(
            "single-band rasters only; Sentinel-2 distributes one COG per band "
            f"(found samples-per-pixel = {len(ifd.bits_per_sample)})"
        )
    dtype = _DTYPES[(int(ifd.sample_format[0]), int(ifd.bits_per_sample[0]))]
    if not np.can_cast(np.min_scalar_type(fill), dtype):
        raise ValueError(
            f"fill={fill!r} is not representable in the asset dtype {np.dtype(dtype).name}"
        )
    epsg, transform = _geo_from_ifd(ifd)
    shape = (ifd.image_height, ifd.image_width)
    center = _raster_center_lonlat(epsg, transform, shape)
    rows, cols, valid = grid.sample(cells, f"EPSG:{epsg}", transform, shape)

    th, tw = ifd.tile_height, ifd.tile_width
    vr, vc = rows[valid], cols[valid]
    tr, tc = vr // th, vc // tw

    if vr.size == 0:
        return np.full(rows.shape, fill, dtype=dtype), valid, center

    pairs = np.unique(np.stack([tr, tc], axis=1), axis=0)
    tiles = await asyncio.gather(*[tiff.fetch_tile(int(c), int(r), 0) for r, c in pairs])
    decoded = await asyncio.gather(*[t.decode() for t in tiles])

    gathered = np.full(vr.shape, fill, dtype=dtype)
    for (trow, tcol), dec in zip(pairs, decoded):
        arr = np.asarray(dec)[:, :, 0]
        m = (tr == trow) & (tc == tcol)
        gathered[m] = arr[vr[m] - trow * th, vc[m] - tcol * tw]

    values = np.full(rows.shape, fill, dtype=dtype)
    values[valid] = gathered
    return values, valid, center


def _raster_center_lonlat(epsg: int, transform, shape) -> tuple[float, float]:
    """The raster's center pixel as WGS84 ``(lon, lat)`` — its "tile center"."""
    from pyproj import CRS, Transformer

    a, b, c, d, e, f = (float(t) for t in transform[:6])
    col, row = shape[1] / 2.0, shape[0] / 2.0
    x, y = a * col + b * row + c, d * col + e * row + f
    tx = Transformer.from_crs(
        CRS.from_user_input(f"EPSG:{epsg}"), CRS.from_epsg(4326), always_xy=True
    )
    lon, lat = tx.transform(x, y)
    return float(lon), float(lat)


def sample_asset(grid, cells, href: str, **kwargs):
    """Sync facade over :func:`sample_asset_async` (worker call sites are sync).

    Safe to call under an already-running event loop (Jupyter/Binder): when one
    is detected the coroutine runs to completion on a one-shot worker thread and
    the result is returned synchronously. Async callers should prefer awaiting
    :func:`sample_asset_async` directly.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(sample_asset_async(grid, cells, href, **kwargs))

    def _run():
        return asyncio.run(sample_asset_async(grid, cells, href, **kwargs))

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(_run).result()


def _run_sync(coro):
    """Run a coroutine from sync code, safe under an already-running loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(asyncio.run, coro).result()


# ── item sampling + acquisition grouping (issue #218) ────────────────────────


async def sample_item_async(
    grid,
    cells,
    assets: dict,
    bands: dict,
    *,
    nodata=None,
    region: str | None = None,
    anonymous: bool = True,
):
    """Sample every configured band of one STAC item, concurrently.

    Parameters
    ----------
    assets : dict
        ``{asset_key: href}`` — the granule entry's per-band asset map.
    bands : dict
        Normalized band config (:func:`zagg.config.get_raster_bands`):
        field -> ``{asset, dtype, fill_value, attrs}``.
    nodata : scalar, optional
        Source nodata DN; a cell whose sampled pixel equals it in ANY band is
        marked invalid (Sentinel-2 encodes scene-footprint nodata as 0 across
        all bands together).

    Returns
    -------
    (values, valid, center)
        ``values`` ``{field: ndarray}`` (asset dtype, fill where invalid),
        ``valid`` the combined per-cell mask, ``center`` the item's raster
        center ``(lon, lat)`` for the nearest-tile-center ownership rule.
    """
    missing = [meta["asset"] for meta in bands.values() if meta["asset"] not in assets]
    if missing:
        raise ValueError(
            f"granule entry is missing configured asset(s) {sorted(missing)}; "
            f"available: {sorted(assets)}"
        )
    fields = list(bands)
    results = await asyncio.gather(
        *[
            _sample_one(
                grid,
                cells,
                assets[bands[f]["asset"]],
                region=region,
                anonymous=anonymous,
                fill=bands[f]["fill_value"],
            )
            for f in fields
        ]
    )
    values = {f: r[0] for f, r in zip(fields, results)}
    valid = results[0][1].copy()
    for _v, mask, _c in results[1:]:
        valid &= mask
    if nodata is not None:
        for f in fields:
            valid &= values[f] != nodata
    return values, valid, results[0][2]


def _iso_us(iso: str) -> int:
    """ISO datetime -> int microseconds since the epoch (UTC)."""
    from datetime import datetime, timezone

    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def raster_time_index(granules) -> tuple[dict, np.ndarray]:
    """Global timestep index from ShardMap granule lists.

    A timestep is an *acquisition group* — entries sharing a ``time_key``
    (e.g. the Sentinel-2 datatake id; adjacent MGRS tiles of one datatake are
    items seconds apart) — falling back to the entry datetime when no key was
    configured. The group's coordinate value is its earliest datetime.

    Parameters
    ----------
    granules : list of list of dict
        ``ShardMap.granules`` (raster entries carry ``assets`` + ``datetime``).

    Returns
    -------
    (time_index, times_us)
        ``{group_key: t_idx}`` and the int64 microseconds-since-epoch time
        coordinate, both in ascending time order.
    """
    earliest: dict = {}
    for shard_entries in granules:
        for e in shard_entries:
            if not e.get("assets"):
                continue
            if not e.get("datetime"):
                raise ValueError(f"raster granule entry {e.get('id')!r} carries no datetime")
            key = e.get("time_key") or e["datetime"]
            us = _iso_us(e["datetime"])
            if key not in earliest or us < earliest[key]:
                earliest[key] = us
    ordered = sorted(earliest, key=lambda k: (earliest[k], k))
    time_index = {k: i for i, k in enumerate(ordered)}
    times_us = np.array([earliest[k] for k in ordered], dtype=np.int64)
    return time_index, times_us


def _chord2(lons, lats, lon0: float, lat0: float) -> np.ndarray:
    """Squared unit-sphere chord distance from points to ``(lon0, lat0)``.

    Monotone in great-circle distance — all the ownership argmin needs — and
    comparable across UTM zones (unlike per-zone projected distances).
    """
    lam, phi = np.radians(np.asarray(lons, dtype=float)), np.radians(np.asarray(lats, dtype=float))
    lam0, phi0 = np.radians(lon0), np.radians(lat0)
    x = np.cos(phi) * np.cos(lam) - np.cos(phi0) * np.cos(lam0)
    y = np.cos(phi) * np.sin(lam) - np.cos(phi0) * np.sin(lam0)
    z = np.sin(phi) - np.sin(phi0)
    return x * x + y * y + z * z


def process_raster_shard(
    grid,
    shard_key: int,
    granules: list,
    config,
    time_index: dict,
    *,
    region: str | None = None,
    anonymous: bool = True,
):
    """Process one shard of a raster pipeline: every acquisition group -> slab.

    For each timestep, every covering item is sampled (pull-NN over the
    shard's cells); where several items cover a cell — the MGRS overlap — the
    cell takes the value from the item whose tile center is nearest
    (espg-ratified ownership rule on issue #218; subsumes same-zone dedupe).

    Invariant: ``time_index`` MUST be built from the same manifest the shards
    were dispatched from (see :func:`raster_time_index`). A granule whose
    acquisition-group key is absent raises :class:`ValueError` naming the key,
    rather than failing with an opaque ``KeyError`` deep in the gather.

    Returns
    -------
    (slabs, metadata)
        ``slabs``: ``{t_idx: {field: values}}`` — one dense per-cell array per
        band per timestep, fill where no valid source. ``metadata``: counts
        (``granule_count``, ``skipped``, ``timesteps``, ``shard_key``).
    """
    from zagg.config import get_raster_bands

    cells = grid.children(int(shard_key))
    bands = get_raster_bands(config)
    nodata = config.data_source.get("nodata")

    groups: dict = {}
    skipped = 0
    for e in granules:
        if not e.get("assets"):
            skipped += 1
            continue
        groups.setdefault(e.get("time_key") or e["datetime"], []).append(e)

    for key in groups:
        if key not in time_index:
            raise ValueError(
                f"acquisition group key {key!r} is absent from the passed "
                "time_index; time_index must be built from the same manifest "
                "the shards were dispatched from — see raster_time_index"
            )

    # One event loop per shard: fan out across every item of every timestep
    # concurrently (S3 fetches overlap), instead of one asyncio.run per item.
    async def _sample_all():
        return await asyncio.gather(
            *[
                asyncio.gather(
                    *[
                        sample_item_async(
                            grid,
                            cells,
                            e["assets"],
                            bands,
                            nodata=nodata,
                            region=region,
                            anonymous=anonymous,
                        )
                        for e in items
                    ]
                )
                for items in groups.values()
            ]
        )

    group_results = _run_sync(_sample_all())

    lonlat = None  # computed once, only if some timestep has overlapping items
    slabs: dict = {}
    for (key, _items), sampled in zip(groups.items(), group_results):
        t = time_index[key]
        if len(sampled) == 1:
            values, valid, _center = sampled[0]
        else:
            if lonlat is None:
                lonlat = grid.cell_lonlat(cells)
            values, valid = _combine_by_ownership(sampled, lonlat, bands)
        slab = {}
        for f, v in values.items():
            out = v.copy()  # keep the asset dtype (np.where would promote)
            out[~valid] = bands[f]["fill_value"]
            slab[f] = out
        slabs[t] = slab
    metadata = {
        "shard_key": int(shard_key),
        "granule_count": len(granules),
        "skipped": skipped,
        "timesteps": len(slabs),
    }
    return slabs, metadata


def _combine_by_ownership(sampled, lonlat, bands):
    """Nearest-tile-center combine across one timestep's overlapping items."""
    lons, lats = lonlat
    dists = np.stack(
        [_chord2(lons, lats, *center) for _v, _m, center in sampled]
    )  # (n_items, n_cells)
    valid_stack = np.stack([m for _v, m, _c in sampled])
    dists[~valid_stack] = np.inf
    owner = np.argmin(dists, axis=0)
    any_valid = valid_stack.any(axis=0)
    values = {}
    for f in bands:
        stack = np.stack([v[f] for v, _m, _c in sampled])
        values[f] = stack[owner, np.arange(stack.shape[1])]
    return values, any_valid


# ── lean (time, cell) template + slab writer (issue #218) ────────────────────
#
# Pull-NN emits one dense slab per (timestep x shard) by construction, so the
# raster output path is a plain Zarr region assignment — it BYPASSES the
# aggregation write machinery (carriers / reductions / ragged) rather than
# threading a time dimension through it. Appends are the standard Zarr
# resize-then-write-slab pattern and are single-writer (the runner owns the
# resize, as it owns template emission).

_TIME_ATTRS = {"units": "microseconds since 1970-01-01T00:00:00", "calendar": "proleptic_gregorian"}


def raster_group_spec(grid, config, n_time: int):
    """pydantic-zarr GroupSpec for the raster ``(time, cells)`` template.

    Per band: shape ``(n_time, n_pixels)``, chunks ``(1, cells_per_chunk)`` —
    one storage object per (timestep, chunk), so per-date rewrites are exact.
    Plus ``time`` (int64 microseconds, CF attrs) and ``cell_ids`` (uint64,
    written per shard by :func:`write_raster_coords`).
    """
    from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig

    from zagg.config import get_raster_bands

    if getattr(grid, "sharded", False):
        raise ValueError("raster templates do not support sharded output yet (issue #218)")
    n_pixels = int(np.prod(grid.array_shape))
    if len(grid.array_shape) != 1:
        raise ValueError(
            "raster templates currently require a 1-D cell axis (HEALPix); "
            "the rectilinear (time, y, x) variant is future work (issue #218)"
        )

    def _arr(shape, chunks, dims, dtype, fill, attrs=None):
        return ArraySpec(
            attributes=attrs or {},
            shape=shape,
            dimension_names=dims,
            data_type=dtype,
            chunk_grid=NamedConfig(name="regular", configuration={"chunk_shape": chunks}),
            chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
            codecs=(
                NamedConfig(name="bytes", configuration={"endian": "little"}),
                NamedConfig(name="zstd", configuration={"level": 3, "checksum": False}),
            ),
            storage_transformers=(),
            fill_value=fill,
        )

    members = {
        "time": _arr((n_time,), (max(n_time, 1),), ("time",), "int64", 0, dict(_TIME_ATTRS)),
        "cell_ids": _arr((n_pixels,), (grid.cells_per_chunk,), ("cells",), "uint64", 0),
    }
    for name, meta in get_raster_bands(config).items():
        members[name] = _arr(
            (n_time, n_pixels),
            (1, grid.cells_per_chunk),
            ("time", "cells"),
            meta["dtype"],
            meta["fill_value"],
            meta["attrs"] or {},
        )
    return GroupSpec(members=members, attributes={})


def emit_raster_template(store, grid, config, times_us: np.ndarray, *, overwrite: bool = False):
    """Write the raster template and its ``time`` coordinate values."""
    from zarr import config as zarr_config
    from zarr import open_array

    spec = raster_group_spec(grid, config, int(len(times_us)))
    with zarr_config.set({"async.concurrency": 128}):
        spec.to_zarr(store, grid.group_path, overwrite=overwrite)
        arr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        arr[:] = np.asarray(times_us, dtype=np.int64)
    return store


def _shard_cell_range(grid, shard_key: int) -> tuple[int, int]:
    """The shard's contiguous cell-axis extent ``[start, stop)``.

    Dense layout: ``block_index`` is the shard's position in
    ``populated_shards``; fullsphere: the parent's nested id. Both are the
    shard's block on a ``cells_per_shard``-strided axis.
    """
    start = int(grid.block_index(int(shard_key))[0]) * grid.cells_per_shard
    return start, start + grid.cells_per_shard


def write_raster_slab(store, grid, shard_key: int, t_idx: int, slab: dict):
    """Write one timestep x shard slab: ``array[t, start:stop] = values``.

    Chunk-aligned by construction (``start`` is a multiple of
    ``cells_per_chunk``), so no read-modify-write.
    """
    from zarr import config as zarr_config
    from zarr import open_array

    start, stop = _shard_cell_range(grid, shard_key)
    with zarr_config.set({"async.concurrency": 128}):
        for name, values in slab.items():
            arr = open_array(
                store, path=f"{grid.group_path}/{name}", zarr_format=3, consolidated=False
            )
            arr[int(t_idx), start:stop] = np.asarray(values, dtype=arr.dtype)
    return store


def write_raster_coords(store, grid, shard_key: int):
    """Write the shard's ``cell_ids`` coordinate block (once per shard)."""
    from zarr import config as zarr_config
    from zarr import open_array

    start, stop = _shard_cell_range(grid, shard_key)
    cell_ids = grid.encode_cell_ids(grid.children(int(shard_key)))
    with zarr_config.set({"async.concurrency": 128}):
        arr = open_array(
            store, path=f"{grid.group_path}/cell_ids", zarr_format=3, consolidated=False
        )
        arr[start:stop] = np.asarray(cell_ids, dtype=np.uint64)
    return store


__all__ = [
    "sample_asset",
    "sample_asset_async",
    "sample_item_async",
    "raster_time_index",
    "process_raster_shard",
    "raster_group_spec",
    "emit_raster_template",
    "write_raster_slab",
    "write_raster_coords",
]
