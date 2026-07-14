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
    """
    from async_tiff import TIFF

    store, path = _store_and_path(href, region=region, anonymous=anonymous)
    tiff = await TIFF.open(path, store=store)
    ifd = tiff.ifds[0]
    if len(ifd.bits_per_sample) != 1:
        raise ValueError(
            "single-band rasters only; Sentinel-2 distributes one COG per band "
            f"(found samples-per-pixel = {len(ifd.bits_per_sample)})"
        )
    epsg, transform = _geo_from_ifd(ifd)
    shape = (ifd.image_height, ifd.image_width)
    rows, cols, valid = grid.sample(cells, f"EPSG:{epsg}", transform, shape)

    th, tw = ifd.tile_height, ifd.tile_width
    vr, vc = rows[valid], cols[valid]
    tr, tc = vr // th, vc // tw

    if vr.size == 0:
        dtype = _DTYPES[(int(ifd.sample_format[0]), int(ifd.bits_per_sample[0]))]
        return np.full(rows.shape, fill, dtype=dtype), valid

    pairs = np.unique(np.stack([tr, tc], axis=1), axis=0)
    tiles = await asyncio.gather(*[tiff.fetch_tile(int(c), int(r), 0) for r, c in pairs])
    decoded = await asyncio.gather(*[t.decode() for t in tiles])

    gathered = None
    for (trow, tcol), dec in zip(pairs, decoded):
        arr = np.asarray(dec)[:, :, 0]
        if gathered is None:
            gathered = np.full(vr.shape, fill, dtype=arr.dtype)
        m = (tr == trow) & (tc == tcol)
        gathered[m] = arr[vr[m] - trow * th, vc[m] - tcol * tw]

    values = np.full(rows.shape, fill, dtype=gathered.dtype)
    values[valid] = gathered
    return values, valid


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


__all__ = ["sample_asset", "sample_asset_async"]
