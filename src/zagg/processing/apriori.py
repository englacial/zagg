"""A-priori chunk-boundary read planning (issue #148, arm 2a).

Plans one granule's reads from its **chunk-boundary parquet** (written by
:mod:`zagg.catalog.extract`) instead of the geolocation-rate coordinate read:
each HDF5 chunk's first/last-photon lat/lon gives a per-beam polyline, chunks
whose along-track span touches the shard are selected by sampling that
"faked geometry" through the SAME mortie test the photon path applies
(``grid.shards_of(grid.assign(...)) == shard_key``), and the matched chunks
become a chunk-aligned :class:`~zagg.read_plan.ReadPlan`. h5coro still resolves
byte offsets (arm 2b — offsets — is deferred per the issue #148 decisions).

Correctness does not rest on the faked geometry being exact: the plan only
bounds IO, and the photon-exact shard mask in
:func:`zagg.processing.read._execute_plan_group` trims over-inclusion, so the
output is bit-identical to the production read paths as long as no touched
chunk is missed. The interpolated samples plus ``pad`` (whole chunks, default 1)
are the conservative margin for track curvature between boundary points.

Enabled via ``data_source.read_plan.chunk_boundaries``::

    read_plan:
      chunk_boundaries:
        prefix: s3://bucket/boundaries/   # or a local directory
        samples_per_chunk: 64             # optional
      pad: 1                              # shared with the planned path
      full_read_threshold: 0.9            # shared with the planned path

The plan trusts the parquet to describe the granule as read: a stale cache
(granule reprocessed since extraction) makes the slices misalign, which
surfaces as a loud h5coro read failure (``errorChecking``) — never a silently
wrong result from a *shifted* mask, since the exact photon filter re-tests
every row that is read.
"""

from __future__ import annotations

import os

import numpy as np

from zagg.read_plan import plan_read

#: Interpolated test points per chunk (including both boundary photons). At the
#: ATL03 100k-photon chunk size a chunk spans ~20-70 km along-track, so 64
#: samples test every ~0.3-1.1 km — well under the o10 (~6.4 km) cell width the
#: 88S benchmark plans against.
DEFAULT_SAMPLES_PER_CHUNK = 64


def _boundary_parquet_name(granule_url: str) -> str:
    """The per-granule parquet name :func:`zagg.catalog.extract.run_extraction` writes."""
    granule_id = os.path.basename(granule_url)
    return f"{granule_id.removesuffix('.h5')}.boundaries.parquet"


def _load_boundaries(prefix: str, granule_url: str):
    """Load one granule's boundary parquet from ``prefix`` (s3:// or local dir).

    Returns ``(df, meta)`` via :func:`zagg.catalog.extract.read_boundaries_parquet`.
    S3 objects are staged through a temp file (fastparquet reads paths); local
    prefixes are read in place. A missing parquet raises — for a benchmark arm a
    silent fallback to another read path would corrupt the comparison, and the
    worker's per-group isolation already contains the failure to this granule.
    """
    from zagg.catalog.extract import read_boundaries_parquet

    name = _boundary_parquet_name(granule_url)
    if prefix.startswith("s3://"):
        import tempfile

        import boto3

        bucket, _, key_prefix = prefix[5:].partition("/")
        key = f"{key_prefix.rstrip('/')}/{name}" if key_prefix else name
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            boto3.client("s3").download_file(bucket, key, tmp.name)
            return read_boundaries_parquet(tmp.name)
    path = os.path.join(prefix, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"chunk-boundary parquet not found for {granule_url}: {path}")
    return read_boundaries_parquet(path)


def _chunk_shard_mask(bdf, grid, shard_key: int, samples_per_chunk: int) -> np.ndarray:
    """Per-chunk boolean mask: does the chunk's along-track span touch the shard?

    Interpolates ``samples_per_chunk`` points between each chunk's boundary
    photons on the unit sphere (linear in 3-D then renormalized — pole- and
    antimeridian-safe, unlike lat/lon interpolation; boundary photons of one
    chunk are never antipodal so the normalization is well-conditioned) and
    applies the same mortie test the photon path uses. Endpoints are included,
    so a chunk is matched whenever either boundary photon or any interpolated
    point lands in the shard.
    """

    def _xyz(lat_deg, lon_deg):
        lat, lon = np.radians(lat_deg), np.radians(lon_deg)
        return np.stack(
            [np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)], axis=-1
        )

    p0 = _xyz(bdf["start_lat"].to_numpy(), bdf["start_lon"].to_numpy())
    p1 = _xyz(bdf["end_lat"].to_numpy(), bdf["end_lon"].to_numpy())
    t = np.linspace(0.0, 1.0, samples_per_chunk)[None, :, None]
    pts = p0[:, None, :] * (1.0 - t) + p1[:, None, :] * t
    pts /= np.linalg.norm(pts, axis=-1, keepdims=True)
    lats = np.degrees(np.arcsin(np.clip(pts[..., 2], -1.0, 1.0)))
    lons = np.degrees(np.arctan2(pts[..., 1], pts[..., 0]))
    leaf = grid.assign(lats.ravel(), lons.ravel())
    in_shard = np.asarray(grid.shards_of(leaf)) == shard_key
    return in_shard.reshape(len(bdf), samples_per_chunk).any(axis=1)


def _plan_from_boundaries(
    bdf,
    grid,
    shard_key: int,
    *,
    pad: int = 1,
    full_read_threshold: float = 0.9,
    samples_per_chunk: int = DEFAULT_SAMPLES_PER_CHUNK,
):
    """Build a chunk-aligned ReadPlan from one beam's boundary rows.

    Chunks are the "coarse parents" of :func:`zagg.read_plan.plan_read`:
    ``index_beg`` is the running photon offset (chunks tile the photon axis
    contiguously, which is re-derived from ``n_photons`` rather than trusted
    from ``chunk * chunk_size`` so a truncated final chunk is exact), the
    boundary-sampling mask is the ``coarse_mask``, and the returned slices are
    therefore chunk-aligned by construction — ``pad`` and run merging included.
    Returns ``(plan, n_base)``.
    """
    bdf = bdf.sort_values("chunk", ignore_index=True)
    if not (bdf["chunk"].to_numpy() == np.arange(len(bdf))).all():
        raise ValueError(
            f"boundary rows are not a contiguous chunk range 0..{len(bdf) - 1}: "
            f"{bdf['chunk'].tolist()[:8]}..."
        )
    cnt = bdf["n_photons"].to_numpy(dtype=np.int64)
    ibeg = np.concatenate(([0], np.cumsum(cnt)[:-1]))
    n_base = int(cnt.sum())
    mask = _chunk_shard_mask(bdf, grid, shard_key, samples_per_chunk)
    plan = plan_read(
        bdf["start_lat"].to_numpy(),
        bdf["start_lon"].to_numpy(),
        ibeg,
        cnt,
        n_base,
        coarse_mask=mask,
        pad=pad,
        full_read_threshold=full_read_threshold,
    )
    return plan, n_base


def _make_boundary_read_fn(h5obj, chunk_size: int):
    """Hyperslice reader that sidesteps h5coro's chunk-aligned-start bug.

    Every a-priori slice starts exactly on a chunk boundary — precisely the
    reads h5coro's B-tree start-edge off-by-one fails wholesale (a hyperslice
    beginning at ``k * chunk_size``, k > 0, spuriously matches the preceding
    chunk; see :mod:`zagg.catalog.extract`). Mirror the extractor's workaround:
    start one element early and drop it, costing one re-decompressed chunk per
    run. Valid for every heights dataset because the chunk grid is aligned
    (verified at extraction time by ``heights_chunk_grid``).
    """

    def _read_fn(path, hyperslice=None):
        if hyperslice is None:
            return h5obj.readDatasets([path])[path]
        start, end = hyperslice[0]
        r0 = start - 1 if start > 0 and start % chunk_size == 0 else start
        arr = h5obj.readDatasets([{"dataset": path, "hyperslice": [(r0, end)]}])[path]
        return arr[start - r0 :]

    return _read_fn


def _apriori_read_group(
    h5obj,
    group: str,
    data_source: dict,
    shard_key: int,
    grid,
    arrow: bool = False,
    granule_url: str | None = None,
):
    """A-priori (chunk-boundary) read of one HDF5 group — issue #148 arm 2a.

    Same ``DataFrame`` / ``arro3.core.Table`` / ``None`` contract as
    :func:`zagg.processing.read._read_group`. The plan comes from the granule's
    boundary parquet (no geolocation-rate coordinate read, no segment→shard
    mask); everything after the plan — exact photon shard mask, filters,
    segment broadcasts — is the shared
    :func:`~zagg.processing.read._execute_plan_group`, so the output is
    bit-identical to the production paths. Selectivity above
    ``full_read_threshold`` falls back to the full-coord read, mirroring
    :func:`~zagg.processing.read._planned_read_group`.
    """
    from zagg.processing.read import _execute_plan_group, _read_group_full

    rp = data_source["read_plan"]
    cfg = rp["chunk_boundaries"]
    prefix = cfg.get("prefix") if isinstance(cfg, dict) else None
    if not prefix:
        raise ValueError("read_plan.chunk_boundaries requires a 'prefix' (s3:// or local dir)")
    if granule_url is None:
        raise ValueError(
            "read_plan.chunk_boundaries requires the granule URL to locate its "
            "boundary parquet (the worker passes it when the feature is on)"
        )

    bdf, meta = _load_boundaries(prefix, granule_url)
    beam = group.strip("/").split("/")[0]
    bdf = bdf[bdf["beam"] == beam]
    if bdf.empty:
        return None  # beam absent or photon-less at extraction time -> no data

    plan, n_base = _plan_from_boundaries(
        bdf,
        grid,
        shard_key,
        pad=int(rp.get("pad", 1)),
        full_read_threshold=float(rp.get("full_read_threshold", 0.9)),
        samples_per_chunk=int(cfg.get("samples_per_chunk", DEFAULT_SAMPLES_PER_CHUNK)),
    )
    if not plan.parent_runs:
        return None  # no chunk's span touches this shard
    if plan.full_read:
        return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)

    chunk_size = int(meta.get("chunk_size") or bdf["n_photons"].max())
    read_fn = _make_boundary_read_fn(h5obj, chunk_size)
    return _execute_plan_group(
        h5obj, group, data_source, shard_key, grid, plan, n_base, arrow, read_fn=read_fn
    )
