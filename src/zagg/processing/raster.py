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
import os
import re
import threading
import time
import warnings
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


# Store cache (issue #244): one obspec store per (kind, bucket-or-host-or-dir,
# region, anonymous) per PROCESS. A fresh S3Store per asset-sample cost ~300 ms
# of client/TLS setup and made every tile GET ride a cold connection (425
# clients per full-year o9 invoke — the measured breakdown on the issue).
# Module lifetime == sandbox lifetime (espg-ratified): warm Lambda invocations
# keep their connection pools, matching the issue #171 sandbox-lifetime
# pattern. Lock-guarded because the running-loop fallback in ``sample_asset``
# and hand-rolled callers can construct from other threads; construction runs
# under the lock deliberately (single-flight per key).
#
# Credential lifetime (issue #244 review): every key deployed today is
# anonymous (``sentinel2_l2a.yaml`` sets ``anonymous: true``; runner.py:676
# and ``sample_asset*`` default it True), so the cached ``S3Store`` carries
# ``skip_signature=True`` and signs nothing — credentials never enter the
# picture and warm-caching a store cannot go stale. If a *signed* store
# (``anonymous=False``) were ever cached, sandbox-lifetime caching would only
# be safe because of how async-tiff's Rust ``object_store``-backed ``S3Store``
# resolves credentials, and that splits by environment:
#   - On AWS Lambda the execution-role credentials arrive as static env vars
#     valid for the whole sandbox lifetime and do NOT rotate mid-sandbox, so a
#     sandbox-lifetime store cannot outlive its creds — safe by construction.
#   - Off-Lambda (EC2 instance profile / IMDS, SSO), object_store resolves
#     through a caching credential *provider* that refreshes on expiry per
#     request rather than freezing a token at construction, so a warm store
#     keeps working across rotation.
# (The Lambda case is directly grounded; the off-Lambda refresh behavior
# reflects object_store's documented provider design — ``S3Store`` exposes a
# ``credential_provider`` slot — and was not source-verified against the
# installed binary wheel.) Caveat: no explicit-credential store exists here
# (the raster path is anonymous); do NOT add one to the cache without
# revisiting this, since a statically-supplied token would be frozen at
# construction and eventually go stale on a warm worker.
_STORE_CACHE: dict = {}
_STORE_LOCK = threading.Lock()


def _store_and_path(href: str, *, region: str | None = None, anonymous: bool = True):
    """obspec store + in-store path for an asset href.

    Handles ``s3://bucket/key``, virtual-hosted S3 HTTPS
    (``https://bucket.s3.region.amazonaws.com/key`` -- what Earth Search
    asset hrefs look like), plain HTTPS, and local paths. Stores are cached
    per ``(kind, location, region, anonymous)`` for the life of the process
    (issue #244) — the returned store is shared, never per-call.
    """
    u = urlparse(href)
    if u.scheme == "s3":
        key = ("s3", u.netloc, region, anonymous)
        path = u.path.lstrip("/")
    elif u.scheme in ("http", "https"):
        m = _S3_VHOST.match(u.netloc)
        if m:
            key = ("s3", m["bucket"], region or m["region"], anonymous)
            path = u.path.lstrip("/")
        else:
            key = ("http", f"{u.scheme}://{u.netloc}", None, None)
            path = u.path.lstrip("/")
    else:
        d, name = os.path.split(href)
        key = ("local", d or ".", None, None)
        path = name
    with _STORE_LOCK:
        store = _STORE_CACHE.get(key)
        if store is None:
            store = _build_store(key)
            _STORE_CACHE[key] = store
    return store, path


def _build_store(key):
    """Construct the obspec store for a cache key (see ``_STORE_CACHE``)."""
    from async_tiff.store import HTTPStore, LocalStore, S3Store

    kind, loc, region, anonymous = key
    if kind == "s3":
        kw: dict = {}
        if anonymous:
            kw["skip_signature"] = True
        if region:
            kw["region"] = region
        return S3Store(loc, **kw)
    if kind == "http":
        return HTTPStore(loc)
    return LocalStore(loc)


def new_stage_stats() -> dict:
    """Fresh per-invoke stage accumulator for the sample path (issue #249).

    The floats are wall-clock seconds (``time.time()`` deltas, the issue #100
    convention) of **stage work volume**: each asset-sample times its own
    stages independently and the K x bands concurrent samples of an invoke
    overlap on one event loop, so a stage total is the sum of per-sample
    elapsed walls *including* time suspended while sibling samples ran. The
    sums attribute where the samples' time went (which stage differs between
    a fast and a slow invoke) — they are NOT a wall decomposition and can
    exceed the invoke's wall clock. That is deliberate: the ``write_buffer >
    1`` sample/write remainder on PR #232 already showed wall splits go
    approximate under overlap.

    Keys — seconds: ``open`` (store lookup + TIFF header round trips + geo/
    dtype parse), ``geometry`` (pull-NN mapping; a ``geom_cache`` hit records
    ~0), ``fetch`` (tile GETs), ``decode``, ``gather`` (tile-index derivation
    + numpy scatter/gather).
    Counts: ``assets`` (asset-samples), ``tiles`` (tiles fetched),
    ``geom_hits`` (mappings served from ``geom_cache``).
    """
    return {
        "open": 0.0,
        "geometry": 0.0,
        "fetch": 0.0,
        "decode": 0.0,
        "gather": 0.0,
        "assets": 0,
        "tiles": 0,
        "geom_hits": 0,
    }


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
    geom_cache: dict | None = None,
    stage_stats: dict | None = None,
    io_stats: dict | None = None,
):
    """:func:`sample_asset_async` body, also returning the raster's center
    ``(lon, lat)`` — the ownership rule's tile-center input (#218).

    ``geom_cache`` (issue #244) memoizes the pull-NN mapping ``(rows, cols,
    valid)`` per ``(epsg, transform, shape)``: the mapping is invariant across
    every timestep and band that shares a source grid, so a shard invoke
    computes it once per distinct grid (~175 ms at o9) instead of once per
    asset-sample. ``None`` (the default, and the public ``sample_asset*``
    path) computes per call, unchanged.

    ``stage_stats`` (issue #249) accumulates per-stage seconds + counts in
    place — see :func:`new_stage_stats` for the keys and the work-volume (not
    wall-decomposition) semantics. ``None`` (the default, and the public
    ``sample_asset*`` path) makes no timing calls at all — the hot path is
    unchanged. Accumulation is plain ``+=`` on the event loop with no await
    between read and write, so it is atomic by the same argument as the
    ``geom_cache`` store below — no locks; the ``write_buffer`` sink threads
    never touch this dict.

    ``io_stats`` (issue #297) accumulates the read-volume counters for the
    per-shard stats record, in place and by the same on-loop ``+=`` argument:
    ``bytes_read`` (compressed tile bytes fetched), ``px_decoded`` (pixels in
    the decoded tiles — whole tiles are read to sample a few cells), and
    ``px_sampled`` (cell samples actually gathered). Unlike ``stage_stats``
    this is ALWAYS-ON in the shard workers (the counters are a ``len()`` and
    two multiplies per asset — no timing calls); ``None`` (the public
    ``sample_asset*`` path) counts nothing.
    """
    from async_tiff import TIFF

    prof = stage_stats is not None
    _t0 = time.time() if prof else None
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
    if prof:
        stage_stats["open"] += time.time() - _t0
        stage_stats["assets"] += 1
        _t0 = time.time()
    geom_key = (epsg, transform, shape)
    geom = geom_cache.get(geom_key) if geom_cache is not None else None
    if prof and geom is not None:
        stage_stats["geom_hits"] += 1
    if geom is None:
        # INVARIANT (issue #244 thread): no ``await`` between this check and
        # the store below. asyncio interleaves only at await points, so the
        # compute-and-store is atomic on the loop and each source grid is
        # computed exactly once per invoke — no locks needed. If this compute
        # ever moves off-loop (``to_thread``), add per-key async locks.
        # INVARIANT (issue #244 thread): the key ``(epsg, transform, shape)``
        # is complete only because ``cells`` and ``grid`` are constants of the
        # invoke — ``cells = grid.children(shard_key)`` is computed once (see
        # ``process_raster_shard``) and threaded unchanged into every
        # ``_sample_one``, and ``geom_cache`` is allocated fresh per
        # ``process_raster_shard`` call. A future refactor that varies
        # ``cells`` (or ``grid``) per item/group within one invoke MUST fold
        # them into the key or drop the cache, else it returns a stale mapping.
        geom = grid.sample(cells, f"EPSG:{epsg}", transform, shape)
        if geom_cache is not None:
            geom_cache[geom_key] = geom
    rows, cols, valid = geom
    if prof:
        stage_stats["geometry"] += time.time() - _t0
        _t0 = time.time()

    th, tw = ifd.tile_height, ifd.tile_width
    vr, vc = rows[valid], cols[valid]
    tr, tc = vr // th, vc // tw

    if io_stats is not None:
        io_stats["px_sampled"] += int(vr.size)

    if vr.size == 0:
        if prof:
            stage_stats["gather"] += time.time() - _t0
        return np.full(rows.shape, fill, dtype=dtype), valid, center

    pairs = np.unique(np.stack([tr, tc], axis=1), axis=0)
    if prof:
        stage_stats["gather"] += time.time() - _t0
        _t0 = time.time()
    tiles = await asyncio.gather(*[tiff.fetch_tile(int(c), int(r), 0) for r, c in pairs])
    if prof:
        stage_stats["fetch"] += time.time() - _t0
        stage_stats["tiles"] += len(pairs)
        _t0 = time.time()
    if io_stats is not None:
        # compressed_bytes is one Buffer (chunky) or a list of Buffers (planar);
        # normalize so bytes_read counts bytes, not buffers, if the single-band
        # guard above (bits_per_sample != 1) is ever relaxed for multi-band COGs.
        # A lone Buffer answers len() with its byte count; only the planar
        # list/tuple needs per-buffer summing.
        io_stats["bytes_read"] += sum(
            sum(len(x) for x in cb) if isinstance(cb, (list, tuple)) else len(cb)
            for cb in (t.compressed_bytes for t in tiles)
        )
        io_stats["px_decoded"] += len(pairs) * th * tw
    decoded = await asyncio.gather(*[t.decode() for t in tiles])
    if prof:
        stage_stats["decode"] += time.time() - _t0
        _t0 = time.time()

    gathered = np.full(vr.shape, fill, dtype=dtype)
    for (trow, tcol), dec in zip(pairs, decoded):
        arr = np.asarray(dec)[:, :, 0]
        m = (tr == trow) & (tc == tcol)
        gathered[m] = arr[vr[m] - trow * th, vc[m] - tcol * tw]

    values = np.full(rows.shape, fill, dtype=dtype)
    values[valid] = gathered
    if prof:
        stage_stats["gather"] += time.time() - _t0
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
    geom_cache: dict | None = None,
    stage_stats: dict | None = None,
    io_stats: dict | None = None,
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
        marked invalid. This is a single *scene-wide* sentinel, not per-band:
        for Sentinel-2 a DN of 0 means the pixel is outside the scene footprint
        in every band, so a cell that is valid in ``red`` but reads ``scl == 0``
        is dropped intentionally (footprint masking). Only co-declare bands that
        share this sentinel's "no data" meaning — a band that legitimately
        carries a 0 with different semantics would drop otherwise-valid cells.

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
                geom_cache=geom_cache,
                stage_stats=stage_stats,
                io_stats=io_stats,
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


def _us_iso(us: int) -> str:
    """Microseconds since the epoch -> canonical ISO-8601 UTC (seconds precision).

    The inverse of :func:`_iso_us` at the stamp's seconds precision — the D15
    ``time_range`` rendering (:func:`zagg.windows.iso_utc` convention).
    """
    from datetime import datetime, timezone

    return datetime.fromtimestamp(int(us) // 1_000_000, tz=timezone.utc).isoformat(
        timespec="seconds"
    )


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


# Default cap on how many acquisition groups sample concurrently per shard
# (issue #231). One knob per pipeline family (issue #232): ``shard_workers`` is
# "source units in flight per shard" — granules on the spatial path, acquisition
# groups (timesteps) here — mirroring the spatial default of 4: every
# in-flight group holds one timestep's decoded COG tiles + per-band gather
# buffers, so the cap bounds peak sampling memory to ~K timesteps instead of
# all T at once, while still overlapping S3 fetches at fine orders.
_DEFAULT_SHARD_WORKERS = 4


def _shard_workers(config) -> int:
    """``data_source.shard_workers``: acquisition groups in flight per shard.

    Bounds the :class:`asyncio.Semaphore` over timesteps in
    :func:`process_raster_shard` (issue #231). Default 4; ``1`` samples one
    timestep at a time. Re-checked here with the same int>=1 / bool-trap guard
    ``validate_config`` applies at submission, so a hand-rolled worker payload
    fails loudly rather than passing a bad width to ``Semaphore``.
    """
    k = (config.data_source or {}).get("shard_workers", _DEFAULT_SHARD_WORKERS)
    if isinstance(k, bool) or not isinstance(k, int) or k < 1:
        raise ValueError(f"data_source.shard_workers must be an integer >= 1 (got {k!r})")
    return k


# Streamed-write slab budget (PR #232 review): ``1`` is the strict serial
# bound — a completed slab is written and freed before the next group drains.
# ``N`` allows N-1 writes in flight on worker threads while the next slab
# builds, so peak output memory holds <= N slabs; write latency then overlaps
# sampling instead of serializing against it.
_DEFAULT_WRITE_BUFFER = 1


def _write_buffer(config) -> int:
    """``data_source.write_buffer``: max slabs alive under a streamed sink.

    Only meaningful when ``process_raster_shard`` runs with ``on_slab``;
    dict-mode accumulation ignores it. Same int>=1 / bool-trap guard as the
    sibling worker knobs.
    """
    n = (config.data_source or {}).get("write_buffer", _DEFAULT_WRITE_BUFFER)
    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise ValueError(f"data_source.write_buffer must be an integer >= 1 (got {n!r})")
    return n


def process_raster_shard(
    grid,
    shard_key: int,
    granules: list,
    config,
    time_index: dict,
    *,
    region: str | None = None,
    anonymous: bool = True,
    on_slab=None,
    stage_stats: dict | None = None,
    occupied_out: list | None = None,
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

    Parameters
    ----------
    on_slab : callable, optional
        ``on_slab(t_idx, slab)`` sink invoked once per timestep as its
        acquisition group completes (issue #231). When given, the slab is
        handed off and dropped immediately, so peak output memory holds
        ``data_source.write_buffer`` slabs (default 1: written + freed before
        the next group drains; ``N`` runs up to ``N-1`` sink calls on worker
        threads so write latency overlaps sampling — the PR #232
        double-buffer); the returned ``slabs`` is then empty. When ``None``
        (the default) every slab accumulates into ``slabs`` and is returned,
        as before, and ``write_buffer`` is ignored.
    stage_stats : dict, optional
        Per-invoke stage accumulator from :func:`new_stage_stats` (issue
        #249): the sample path adds per-stage seconds (``open`` / ``geometry``
        / ``fetch`` / ``decode`` / ``gather``) and counts (``assets`` /
        ``tiles`` / ``geom_hits``) in place. Stage seconds are work volume,
        not a wall decomposition — concurrent samples overlap, so their sum
        can exceed this call's wall (see :func:`new_stage_stats`). ``None``
        (the default) makes no timing calls — the sample path is unchanged.
    occupied_out : list, optional
        When given, receives one uint64 array of the shard's OCCUPIED cell
        words — cells valid in at least one timestep, i.e. the spatial union
        across the acquisitions sampled (the D14 coverage input; per-timestep
        validity stays data-plane nodata, D9). Mirrors ``process_shard``'s
        seam of the same name. ``None`` (the default) allocates nothing.

    Returns
    -------
    (slabs, metadata)
        ``slabs``: ``{t_idx: {field: values}}`` — one dense per-cell array per
        band per timestep, fill where no valid source (empty when ``on_slab``
        streamed them). ``metadata``: counts (``granule_count``, ``skipped``,
        ``timesteps``, ``shard_key``).
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

    # One event loop per shard, but bound how many acquisition groups sample
    # concurrently: an ``asyncio.Semaphore(K)`` over the timesteps caps peak
    # memory at ~K in-flight timesteps of decoded COG tiles + per-band gather
    # buffers instead of all T at once (issue #231). Within a bounded group
    # every item still fans out concurrently — adjacent MGRS tiles of one
    # datatake are seconds apart, so the wall-clock overlap survives at fine
    # orders.
    k = _shard_workers(config)
    wb = _write_buffer(config)
    # Pull-NN geometry memo (issue #244), scoped to THIS invoke: the (rows,
    # cols, valid) mapping embeds the shard's cells, so per-invoke scoping
    # makes cross-shard collisions impossible by construction. A full-year
    # Sentinel-2 shard has exactly two distinct source grids (10 m bands,
    # 20 m scl) — this turns 425 geometry computations into 2.
    geom_cache: dict = {}
    # Occupied-cell union (issue #247): OR of per-timestep validity across the
    # shard's acquisition groups — the D14 coverage input. Accumulation is an
    # in-place index-assign on the event loop (no await between read and
    # write, and no name rebinding into the coroutine scope), atomic by the
    # same argument as the geom_cache store; allocated only when a sink was
    # passed, so the default path is unchanged.
    occupied_acc = np.zeros(len(cells), dtype=bool) if occupied_out is not None else None
    # Read-volume counters (issue #297): always-on inputs for the stats
    # record — compressed bytes fetched, pixels decoded (whole tiles), and
    # cell samples gathered. Their decoded/sampled ratio reads as the extract's
    # read-time over-provision only when the output grid is coarser than the
    # source; a finer grid (more cells than source pixels) inverts it below 1.
    # Stored raw (associative sums), never as a ratio, per the
    # mergeable-by-construction schema rule.
    io_stats = {"bytes_read": 0, "px_decoded": 0, "px_sampled": 0}

    async def _run_all():
        sem = asyncio.Semaphore(k)

        async def _sample_group(key, items):
            async with sem:
                sampled = await asyncio.gather(
                    *[
                        sample_item_async(
                            grid,
                            cells,
                            e["assets"],
                            bands,
                            nodata=nodata,
                            region=region,
                            anonymous=anonymous,
                            geom_cache=geom_cache,
                            stage_stats=stage_stats,
                            io_stats=io_stats,
                        )
                        for e in items
                    ]
                )
            return time_index[key], sampled

        lonlat = None  # computed once, only if some timestep has overlapping items
        slabs: dict = {}
        # Streamed-sink hand-off. At the default ``write_buffer`` of 1 the
        # sink runs synchronously in the loop: a completed slab is written +
        # freed before the next group drains (the strict issue #231 bound).
        # At N>1 (the PR #232 double-buffer) up to N-1 sink calls run on
        # worker threads while the next slab builds — <= N slabs alive, write
        # latency overlapped with sampling. A sink error surfaces at most one
        # group late, at the next hand-off (or the final drain below).
        pending: list = []

        async def _emit(t, slab):
            if wb <= 1:
                on_slab(t, slab)
                return
            while len(pending) >= wb - 1:
                await pending.pop(0)
            pending.append(asyncio.create_task(asyncio.to_thread(on_slab, t, slab)))

        # Drain groups as they finish (as_completed): build each timestep's slab
        # and hand it to the sink — the output side stays ~write_buffer
        # timesteps (issues #231/#232).
        try:
            for fut in asyncio.as_completed(
                [_sample_group(key, items) for key, items in groups.items()]
            ):
                t, sampled = await fut
                if len(sampled) == 1:
                    values, valid, _center = sampled[0]
                else:
                    if lonlat is None:
                        lonlat = grid.cell_lonlat(cells)
                    values, valid = _combine_by_ownership(sampled, lonlat, bands)
                if occupied_acc is not None:
                    occupied_acc[valid] = True
                slab = {}
                for f, v in values.items():
                    out = v.copy()  # keep the asset dtype (np.where would promote)
                    out[~valid] = bands[f]["fill_value"]
                    slab[f] = out
                if on_slab is not None:
                    await _emit(t, slab)
                else:
                    slabs[t] = slab
        except BaseException:
            # Reap in-flight writes before propagating the primary error, so
            # no task is left un-awaited (their own errors are secondary here).
            await asyncio.gather(*pending, return_exceptions=True)
            raise
        if pending:
            await asyncio.gather(*pending)  # propagate any trailing write error
        return slabs

    slabs = _run_sync(_run_all())
    if occupied_out is not None:
        occupied_out.append(np.asarray(cells, dtype=np.uint64)[occupied_acc])
    metadata = {
        "shard_key": int(shard_key),
        "granule_count": len(granules),
        "skipped": skipped,
        "timesteps": len(groups),
        # Read-volume counters (issue #297) for the stats record.
        "raster_bytes_read": io_stats["bytes_read"],
        "raster_px_decoded": io_stats["px_decoded"],
        "raster_px_sampled": io_stats["px_sampled"],
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


def _raster_array_spec(shape, chunks, dims, dtype, fill, attrs=None):
    """ArraySpec shared by the flat template and the hive leaf spec."""
    from pydantic_zarr.experimental.v3 import ArraySpec, NamedConfig

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


def _check_raster_grid(grid) -> None:
    """Shared template guards: no sharded output, 1-D cell axis only."""
    if getattr(grid, "sharded", False):
        # Permanent exclusion (espg-ratified on issue #247), mirroring the
        # validate_config message: per-timestep slab streaming would
        # read-modify-write each ShardingCodec object.
        raise ValueError(
            "raster templates do not support sharded output (per-timestep slab "
            "streaming would read-modify-write each ShardingCodec object)"
        )
    if len(grid.array_shape) != 1:
        raise ValueError(
            "raster templates currently require a 1-D cell axis (HEALPix); "
            "the rectilinear (time, y, x) variant is future work (issue #218)"
        )


def _raster_members(grid, config, n_time: int, n_cells: int) -> dict:
    """The ``time``/``morton``/band ArraySpec members for one raster store.

    ``morton`` (packed u64 words) is the sole stored cell coordinate — the
    D16 flip applies to the raster path too (espg-ruled on the PR #314
    review: one default cell coordinate everywhere). The legacy NESTED
    ``cell_ids`` array rides only the same ``emit_cell_ids`` transition
    hatch as the spatial path — never a separate schedule.
    """
    from zagg.config import get_raster_bands

    members = {
        "time": _raster_array_spec(
            (n_time,), (max(n_time, 1),), ("time",), "int64", 0, dict(_TIME_ATTRS)
        ),
        "morton": _raster_array_spec((n_cells,), (grid.cells_per_chunk,), ("cells",), "uint64", 0),
    }
    if grid.emit_cell_ids:
        members["cell_ids"] = _raster_array_spec(
            (n_cells,), (grid.cells_per_chunk,), ("cells",), "uint64", 0
        )
    for name, meta in get_raster_bands(config).items():
        members[name] = _raster_array_spec(
            (n_time, n_cells),
            (1, grid.cells_per_chunk),
            ("time", "cells"),
            meta["dtype"],
            meta["fill_value"],
            meta["attrs"] or {},
        )
    return members


def raster_group_spec(grid, config, n_time: int):
    """pydantic-zarr GroupSpec for the raster ``(time, cells)`` template.

    Per band: shape ``(n_time, n_pixels)``, chunks ``(1, cells_per_chunk)`` —
    one storage object per (timestep, chunk), so per-date rewrites are exact.
    Plus ``time`` (int64 microseconds, CF attrs) and ``morton`` (packed u64
    words, written per shard by :func:`write_raster_coords`). The group
    carries the same morton-declared dggs attrs block as the spatial path
    (issues #304/#305): one reader contract for every store.
    """
    from pydantic_zarr.experimental.v3 import GroupSpec

    _check_raster_grid(grid)
    n_pixels = int(np.prod(grid.array_shape))
    return GroupSpec(
        members=_raster_members(grid, config, n_time, n_pixels),
        attributes=grid._dggs_attrs(),
    )


def emit_raster_template(store, grid, config, times_us: np.ndarray, *, overwrite: bool = False):
    """Write the raster template and its ``time`` coordinate values."""
    from zarr import config as zarr_config
    from zarr import open_array
    from zarr.errors import ArrayNotFoundError, ContainsGroupError

    times_us = np.asarray(times_us, dtype=np.int64)
    spec = raster_group_spec(grid, config, int(len(times_us)))
    time_path = f"{grid.group_path}/time"
    with zarr_config.set({"async.concurrency": 128}):
        if not overwrite:
            # ``to_zarr(overwrite=False)`` only refuses a template whose SPEC
            # differs (a changed timestep COUNT -> different ``time`` shape ->
            # ContainsGroupError). A store already holding a same-length but
            # different-valued time axis slips past it, and the unconditional
            # ``arr[:]`` below would silently rewrite the coordinate the
            # workers slab-write against. Refuse that too, so overwrite=False
            # uniformly won't clobber a differing template (issue #264).
            try:
                existing = open_array(store, path=time_path, zarr_format=3, consolidated=False)
            except ArrayNotFoundError:
                existing = None
            if existing is not None and not np.array_equal(existing[:], times_us):
                raise ContainsGroupError(store, grid.group_path)
        spec.to_zarr(store, grid.group_path, overwrite=overwrite)
        arr = open_array(store, path=time_path, zarr_format=3, consolidated=False)
        arr[:] = times_us
    return store


def raster_leaf_spec(grid, config, n_time: int):
    """GroupSpec for ONE shard's hive leaf zarr (issue #247, D3/D13).

    The raster analog of ``HealpixGrid.shard_spec``: the same member set as
    :func:`raster_group_spec` — ``time``/``morton`` plus one ``(time,
    cells)`` array per band, same dtypes/fills/chunking — with the cells axis
    sized to a single shard (``cells_per_shard``) and the time axis to the
    LEAF's own acquisitions (``n_time`` = the groups intersecting this shard
    × window, known at dispatch from the catalog). Wrapped in a ROOT group
    (members under ``grid.group_path``, mirroring ``emit_shard_template``) so
    the D4 commit stamp is one attrs update on an object that exists anyway.
    """
    from pydantic_zarr.experimental.v3 import GroupSpec

    _check_raster_grid(grid)
    inner = GroupSpec(
        members=_raster_members(grid, config, n_time, grid.cells_per_shard),
        # The same morton-declared dggs attrs as the spatial leaf (issue
        # #304 — one reader contract), on the inner group like
        # HealpixGrid._group_spec.
        attributes=grid._dggs_attrs(),
    )
    return GroupSpec(members={grid.group_path: inner}, attributes={})


def emit_raster_leaf_template(
    store, grid, config, shard_key: int, times_us: np.ndarray, *, overwrite: bool = False
):
    """Write one leaf's template plus its ``time`` and ``morton`` coords.

    Unlike the flat path (template at fan-out time, coords per shard after
    the slabs), a leaf's coordinates are fully known at template time — the
    time axis is the leaf's own acquisition groups and ``morton`` is the
    shard's children (packed words; the legacy ``cell_ids`` only under the
    ``emit_cell_ids`` hatch) — so both are written here, once. Called lazily on the
    first slab (mirroring ``process_and_write_hive``'s lazy ``_leaf``) with
    ``overwrite=True`` so a no-data shard never creates the ``.zarr/`` prefix
    and a retry replaces debris wholesale (D4).
    """
    from zarr import config as zarr_config
    from zarr import open_array

    spec = raster_leaf_spec(grid, config, int(len(times_us)))
    children = np.asarray(grid.children(int(shard_key)), dtype=np.uint64)
    with zarr_config.set({"async.concurrency": 128}):
        spec.to_zarr(store, "", overwrite=overwrite)
        arr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        arr[:] = np.asarray(times_us, dtype=np.int64)
        arr = open_array(store, path=f"{grid.group_path}/morton", zarr_format=3, consolidated=False)
        arr[:] = children
        if grid.emit_cell_ids:
            arr = open_array(
                store, path=f"{grid.group_path}/cell_ids", zarr_format=3, consolidated=False
            )
            arr[:] = np.asarray(grid.encode_cell_ids(children), dtype=np.uint64)
    return store


def write_raster_leaf_slab(store, grid, t_idx: int, slab: dict):
    """Write one timestep's slab at LEAF-LOCAL indices: ``array[t, :] = values``.

    The leaf's arrays span exactly one shard, so the cell axis needs no
    block offset (contrast :func:`write_raster_slab`); ``t_idx`` is the
    leaf-local timestep from the leaf's own time index. Chunk-aligned by
    construction (whole rows of ``(1, cells_per_chunk)`` chunks).
    """
    from zarr import config as zarr_config
    from zarr import open_array

    with zarr_config.set({"async.concurrency": 128}):
        for name, values in slab.items():
            arr = open_array(
                store, path=f"{grid.group_path}/{name}", zarr_format=3, consolidated=False
            )
            arr[int(t_idx), :] = np.asarray(values, dtype=arr.dtype)
    return store


def _shard_cell_range(grid, shard_key: int) -> tuple[int, int]:
    """The shard's contiguous cell-axis extent ``[start, stop)``.

    ``block_index`` is the parent's nested id — the shard's block on a
    ``cells_per_shard``-strided axis.
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
    """Write the shard's ``morton`` coordinate block (once per shard).

    Packed u64 words — the sole stored cell coordinate (D16, issue #304);
    the legacy NESTED ``cell_ids`` block rides only the ``emit_cell_ids``
    transition hatch, exactly like the spatial path.
    """
    from zarr import config as zarr_config
    from zarr import open_array

    start, stop = _shard_cell_range(grid, shard_key)
    children = np.asarray(grid.children(int(shard_key)), dtype=np.uint64)
    with zarr_config.set({"async.concurrency": 128}):
        arr = open_array(store, path=f"{grid.group_path}/morton", zarr_format=3, consolidated=False)
        arr[start:stop] = children
        if grid.emit_cell_ids:
            arr = open_array(
                store, path=f"{grid.group_path}/cell_ids", zarr_format=3, consolidated=False
            )
            arr[start:stop] = np.asarray(grid.encode_cell_ids(children), dtype=np.uint64)
    return store


def process_and_write_raster_hive(
    shard_key,
    granules,
    grid,
    store_root: str,
    config,
    *,
    store_kwargs: dict,
    window: dict | None = None,
    profile: bool = False,
    region: str | None = None,
    anonymous: bool = True,
    stage_stats: dict | None = None,
):
    """Process one raster shard into its own hive leaf store (issue #247).

    The raster analog of :func:`zagg.hive.process_and_write_hive` — the
    SHARED per-(shard, window) write path for both dispatchers, so leaf
    templating, slab placement, coverage, and stamp ordering cannot drift
    between backends. The unit's output is a self-describing leaf zarr at
    :func:`zagg.hive.shard_leaf_path` (windowed name when ``window`` is
    given, the bare schedule-``none`` leaf otherwise, D13), whose time axis
    is the unit's OWN acquisition groups (:func:`raster_time_index` over the
    dispatched subset — deterministic, so both dispatchers produce identical
    leaves). The leaf template is emitted lazily on the first slab
    (``overwrite=True``): a no-data unit never creates the ``.zarr/`` prefix,
    a torn worker leaves an UNSTAMPED prefix (debris, D4), and a re-run
    replaces the leaf wholesale (the D13 append/idempotency story).

    ``window`` is the dispatch unit's ``{"label", ...}`` payload. Membership
    was decided AT DISPATCH — the acquisition group's STAC ``datetime``, the
    ratified issue #247 rule — so unlike the aggregation path no
    observation-level filter is injected; the window selects the leaf name,
    arms the D14 popcount (``encoding: "full"`` — gated off on ``None``
    exactly as aggregation gates it for schedule-none stores), and adds the
    D15 stamp truth: the window label plus the ACTUAL ISO-UTC ``[min, max]``
    of the unit's acquisition datetimes (also returned as
    ``metadata["time_range"]`` for the dispatcher's root-summary union).

    The stamp is the leaf's FINAL write: dense slabs (streamed) -> coverage
    sidecar (edge shards only; interior shards stamp ``"full"`` with no
    sidecar PUT) -> stamp. ``cells_with_data`` counts the occupied-cell
    union; ``granule_count`` the unit's acquisitions (asset-carrying
    entries). Phase timings are always collected (issue #297):
    ``metadata["phase_timings"] = {"sample", "write"}`` with the leaf
    write-out (template + slabs + sidecar + stamp) as ``write``; the
    per-stage ``stages`` block (issue #249) stays gated on ``profile`` /
    a passed ``stage_stats`` (the local dispatcher's debug-logging flavor).
    """
    from zagg.hive import (
        COVERAGE_SIDECAR,
        build_coverage,
        encode_coverage_bitmap,
        shard_leaf_path,
        stamp_commit,
        write_coverage_sidecar,
    )
    from zagg.store import open_store

    t_start = time.time()
    if profile and stage_stats is None:
        stage_stats = new_stage_stats()
    label = window["label"] if window else None
    leaf_path = shard_leaf_path(store_root, int(shard_key), window=label)
    # The leaf's own time axis, from the dispatched subset. Every group key in
    # the subset is in this index by construction, so the worker never trips
    # the foreign-manifest guard.
    time_index, times_us = raster_time_index([granules])
    box: dict = {}
    write_s = 0.0

    def _leaf():
        if "store" not in box:
            store = open_store(leaf_path, **store_kwargs)
            # overwrite=True: an existing prefix is debris from a torn run
            # (D4) or a prior committed leaf being redone (D13 re-run) — both
            # replaced wholesale; per-leaf state never blocks a retry. The
            # overwrite enumeration warns about the prior attempt's coverage
            # sidecar — the one foreign key we put there ourselves — so that
            # specific warning is expected and suppressed (the
            # process_and_write_hive posture); anything else stays loud.
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=f"Object at {COVERAGE_SIDECAR}")
                emit_raster_leaf_template(
                    store, grid, config, int(shard_key), times_us, overwrite=True
                )
            box["store"] = store
        return box["store"]

    def _write_slab(t_idx, slab):
        nonlocal write_s
        _t0 = time.time()
        write_raster_leaf_slab(_leaf(), grid, t_idx, slab)
        write_s += time.time() - _t0

    occupied: list = []
    _slabs, meta = process_raster_shard(
        grid,
        int(shard_key),
        granules,
        config,
        time_index,
        region=region,
        anonymous=anonymous,
        on_slab=_write_slab,
        stage_stats=stage_stats,
        occupied_out=occupied,
    )
    # Stamp ONLY a leaf that wrote slabs: a unit that streamed nothing has no
    # prefix, and a worker error raised out above, leaving debris (D4). Write
    # order is pinned: dense slabs -> coverage sidecar -> stamp.
    meta["cells_with_data"] = 0
    # Accurate leaf-written signal for the stats-sidecar gate (issue #297): set
    # iff a slab streamed (``"store" in box``), so both dispatchers gate the
    # sidecar PUT on leaf existence rather than the ``timesteps`` proxy (a unit
    # with acquisitions but no occupied cell writes no leaf). ``phase_timings``
    # cannot serve as the gate — it rides only under ``profile``.
    meta["leaf_written"] = "store" in box
    if "store" in box:
        _t0 = time.time()
        words = occupied[0] if occupied and occupied[0].size else None
        # D14 popcount: a fully-occupied subtree stamps encoding "full" — no
        # sidecar PUT. Gated on a windowed unit (/2 stores) so schedule-none
        # output mirrors aggregation's gate (hive.process_and_write_hive).
        depth = int(grid.child_order) - int(grid.parent_order)
        full = window is not None and words is not None and np.unique(words).size == 4**depth
        bitmap = None
        if words is not None and not full and depth > 0:
            bitmap = encode_coverage_bitmap(int(shard_key), words, grid.child_order)
            write_coverage_sidecar(leaf_path, bitmap, **store_kwargs)
        # D15 truth: the actual acquisition extent written, as ISO-UTC — the
        # min/max STAC datetime over the unit's asset-carrying entries (item
        # instants, not group coordinates, so adjacent-tile spreads count).
        time_range = None
        if window is not None:
            instants = [_iso_us(e["datetime"]) for e in granules if e.get("assets")]
            if instants:
                time_range = [_us_iso(min(instants)), _us_iso(max(instants))]
                meta["time_range"] = time_range
        meta["cells_with_data"] = int(words.size) if words is not None else 0
        stamp_commit(
            box["store"],
            cells_with_data=meta["cells_with_data"],
            granule_count=meta["granule_count"] - meta["skipped"],
            coverage=build_coverage(
                int(shard_key), words, grid.child_order, bitmap=bitmap, full=full
            ),
            window=label,
            time_range=time_range,
        )
        write_s += time.time() - _t0
    # Phase split (issues #100/#249; always-on collection since issue #297 —
    # the stats sidecar needs complete timings by default): only a unit that
    # actually wrote carries it, so a no-data unit stays write-less and
    # sample/write always decompose this call's wall. The per-stage ``stages``
    # block stays verbosity, gated on profiling/debug (a passed stage_stats).
    if "store" in box:
        meta["phase_timings"] = {
            "sample": (time.time() - t_start) - write_s,
            "write": write_s,
        }
        if stage_stats is not None:
            meta["phase_timings"]["stages"] = stage_stats
    return meta


__all__ = [
    "new_stage_stats",
    "sample_asset",
    "sample_asset_async",
    "sample_item_async",
    "raster_time_index",
    "process_raster_shard",
    "process_and_write_raster_hive",
    "raster_group_spec",
    "raster_leaf_spec",
    "emit_raster_template",
    "emit_raster_leaf_template",
    "write_raster_slab",
    "write_raster_leaf_slab",
    "write_raster_coords",
]
