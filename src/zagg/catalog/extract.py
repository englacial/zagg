"""Chunk-boundary geometry extraction for ATL03 ``heights`` groups (issue #148).

Approach (2) of the 88S stress benchmark: record the lat/lon of the first and
last photon of every HDF5 chunk, per beam, so *which chunks a shard needs*
becomes decidable a priori from geometry alone — no geolocation-rate read and
no segment→photon link walk in the worker. One parquet per granule accumulates
the per-beam boundary pairs (the start/end-pair prototype; the N+1 start-only
encoding is the follow-on refinement described in #148).

Verified on local ATL03 v007 granules (issue #148 phase 2):

- **Chunk grids are aligned.** Every dataset in a beam's ``heights`` group is
  chunked ``(100_000,)`` on the photon axis (``signal_conf_ph`` is
  ``(100_000, 5)``), so one boundary polyline per beam covers all heights
  datasets. :func:`heights_chunk_grid` re-verifies this per granule instead of
  trusting the sample.
- **Byte offsets are NOT inferable.** Deflate makes compressed chunk sizes
  variable (~3x spread) and chunks of different datasets interleave in the
  file, so ``(offset, nbytes)`` can only come from the chunk B-tree. Recording
  offsets in this parquet is deferred per the issue #148 discussion (pending
  the full-catalog cost estimate), which is why the schema is geometry-only.

Extraction must decompress every lat/lon chunk (deflate has no partial reads),
so it streams block-by-block and accumulates only the boundary pairs, keeping
memory flat regardless of granule size.
"""

from __future__ import annotations

import json
import time

import numpy as np
import pandas as pd

ATL03_BEAMS = ("gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r")

#: Chunks per streamed read. Each block read holds ``block_chunks`` chunks of
#: lat + lon float64 (~25 MB at the ATL03 100k chunk size) — flat regardless of
#: beam length — while amortizing the one-chunk re-decompression the
#: boundary-aligned-start workaround costs (see ``extract_beam_boundaries``).
DEFAULT_BLOCK_CHUNKS = 16

#: Parquet key under which the per-granule extraction metadata is stored.
EXTRACT_META_KEY = "zagg:extract_meta"


def heights_chunk_grid(h5obj, beam: str) -> tuple[int, int]:
    """Return ``(n_photons, chunk_size)`` for one beam's ``heights`` group.

    Reads only HDF5 metadata (``h5obj.list``). Verifies the aligned-chunk-grid
    assumption the a-priori read plan rests on: every photon-rate dataset in
    the group must share ``lat_ph``'s photon-axis chunk size. A granule that
    violates it would need per-dataset boundaries, so raise loudly rather than
    emit geometry that silently mis-plans reads for the other datasets.
    """
    variables, _, _ = h5obj.list(f"/{beam}/heights", w_attr=False)
    metas = {name: v["__metadata__"] for name, v in variables.items()}
    lat = metas["lat_ph"]
    n = int(lat.dimensions[0])
    chunk = int(lat.chunkDimensions[0])
    misaligned = [
        name
        for name, m in metas.items()
        if list(m.dimensions[:1]) == [n]
        and m.chunkDimensions
        and int(m.chunkDimensions[0]) != chunk
    ]
    if misaligned:
        raise ValueError(
            f"{beam}/heights chunk grid is not aligned with lat_ph ({chunk}): {misaligned}"
        )
    return n, chunk


def extract_beam_boundaries(
    h5obj, beam: str, *, block_chunks: int = DEFAULT_BLOCK_CHUNKS
) -> pd.DataFrame:
    """Stream one beam's lat/lon and return its per-chunk boundary photons.

    Returns a DataFrame with one row per HDF5 chunk: ``beam``, ``chunk``
    (index), ``start_lat``/``start_lon`` (first photon), ``end_lat``/``end_lon``
    (last photon), ``n_photons``. Empty (zero rows, full schema) when the beam
    has no photons.

    Reads ``block_chunks`` chunks per ``readDatasets`` call so memory stays
    flat. Block reads never start exactly on a chunk boundary (except 0):
    h5coro's B-tree intersection test is off by one at the *start* edge, so a
    hyperslice beginning at ``k * chunk_size`` (k > 0) spuriously matches the
    preceding chunk and the whole read fails with ``invalid location to read
    chunk``. Starting one element early (and skipping it) sidesteps the bug at
    the cost of re-decompressing one chunk per block.
    """
    n, chunk = heights_chunk_grid(h5obj, beam)
    cols = ("beam", "chunk", "start_lat", "start_lon", "end_lat", "end_lon", "n_photons")
    if n == 0:
        return pd.DataFrame({c: [] for c in cols})

    lat_path = f"/{beam}/heights/lat_ph"
    lon_path = f"/{beam}/heights/lon_ph"
    rows = []
    block = block_chunks * chunk
    for b0 in range(0, n, block):
        b1 = min(b0 + block, n)
        r0 = max(0, b0 - 1)  # chunk-aligned-start workaround (see docstring)
        data = h5obj.readDatasets(
            [
                {"dataset": lat_path, "hyperslice": [(r0, b1)]},
                {"dataset": lon_path, "hyperslice": [(r0, b1)]},
            ]
        )
        lats = np.asarray(data[lat_path])
        lons = np.asarray(data[lon_path])
        for c0 in range(b0, b1, chunk):
            c1 = min(c0 + chunk, n)
            rows.append(
                (
                    beam,
                    c0 // chunk,
                    lats[c0 - r0],
                    lons[c0 - r0],
                    lats[c1 - 1 - r0],
                    lons[c1 - 1 - r0],
                    c1 - c0,
                )
            )
    return pd.DataFrame(rows, columns=list(cols))


def extract_granule_boundaries(
    h5obj,
    granule_id: str,
    *,
    beams: tuple[str, ...] = ATL03_BEAMS,
    block_chunks: int = DEFAULT_BLOCK_CHUNKS,
) -> tuple[pd.DataFrame, dict]:
    """Extract chunk-boundary geometry for every beam of one granule.

    Returns ``(df, meta)``: the concatenated per-beam boundary rows plus a
    provenance/cost dict (``granule``, ``chunk_size``, ``n_chunks``,
    ``wall_s`` — the number feeding the per-granule extraction-cost estimate
    #148 asks for). A beam missing from the file is skipped and listed under
    ``meta["missing_beams"]``.
    """
    t0 = time.time()
    parts = []
    chunk_sizes = set()
    missing = []
    for beam in beams:
        try:
            n, chunk = heights_chunk_grid(h5obj, beam)
        except KeyError:
            missing.append(beam)
            continue
        chunk_sizes.add(chunk)
        parts.append(extract_beam_boundaries(h5obj, beam, block_chunks=block_chunks))
    cols = ("beam", "chunk", "start_lat", "start_lon", "end_lat", "end_lon", "n_photons")
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame({c: [] for c in cols})
    meta = {
        "granule": granule_id,
        "chunk_size": max(chunk_sizes) if chunk_sizes else None,
        "n_chunks": int(len(df)),
        "beams": [b for b in beams if b not in missing],
        "missing_beams": missing,
        "wall_s": round(time.time() - t0, 3),
        "schema_version": 1,
    }
    if len(chunk_sizes) > 1:
        raise ValueError(f"chunk size differs across beams: {sorted(chunk_sizes)}")
    return df, meta


def write_boundaries_parquet(df: pd.DataFrame, meta: dict, path: str) -> None:
    """Write one granule's boundary rows to parquet (fastparquet engine).

    The extraction metadata rides as file-level custom metadata under
    :data:`EXTRACT_META_KEY` so the emit side can recover provenance without a
    side channel. fastparquet is a zagg core dependency and is in the Lambda
    layer (pyarrow is not), so this works both locally and in the extraction
    Lambda.
    """
    df.to_parquet(
        path,
        engine="fastparquet",
        index=False,
        custom_metadata={EXTRACT_META_KEY: json.dumps(meta)},
    )


def read_boundaries_parquet(path) -> tuple[pd.DataFrame, dict]:
    """Read one granule's boundary parquet back as ``(df, meta)``."""
    from fastparquet import ParquetFile

    pf = ParquetFile(path)
    raw = (pf.key_value_metadata or {}).get(EXTRACT_META_KEY)
    return pf.to_pandas(), (json.loads(raw) if raw else {})


def _put_parquet(local_path: str, output_prefix: str, name: str) -> str:
    """Deliver one parquet to ``output_prefix`` (``s3://bucket/prefix`` or a local dir).

    Returns the destination URI. S3 uploads use the ambient (execution-role)
    credentials — the extraction Lambda writes to an in-account bucket, unlike
    the NSIDC *read* side which needs the event's temporary credentials.
    """
    if output_prefix.startswith("s3://"):
        import boto3

        bucket, _, prefix = output_prefix[5:].partition("/")
        key = f"{prefix.rstrip('/')}/{name}" if prefix else name
        boto3.client("s3").upload_file(local_path, bucket, key)
        return f"s3://{bucket}/{key}"
    import shutil
    from pathlib import Path

    dest = Path(output_prefix) / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(local_path, dest)
    return str(dest)


def _resolve_h5coro_driver(driver: str):
    """Map a ``data_source.driver`` string to the h5coro driver class."""
    if driver == "https":
        from h5coro import webdriver

        return webdriver.HTTPDriver
    if driver == "file":
        from h5coro import filedriver

        return filedriver.FileDriver
    from h5coro import s3driver

    return s3driver.S3Driver


def run_extraction(
    granule_urls: list[str],
    output_prefix: str,
    *,
    driver: str = "s3",
    credentials=None,
    block_chunks: int = DEFAULT_BLOCK_CHUNKS,
    scratch_dir: str = "/tmp",
) -> list[dict]:
    """Extract boundary parquets for many granules (the extract-Lambda body).

    One parquet per granule lands under ``output_prefix`` as
    ``<granule_id>.boundaries.parquet``. Returns one record per granule:
    ``{"granule", "ok", "wall_s", "n_chunks", "output" | "error"}`` — a failed
    granule is recorded and skipped, never fatal to the batch (same posture as
    the worker's per-granule read loop), and ``wall_s`` is the per-granule
    number behind the full-catalog cost extrapolation #148 asks for.
    """
    import os

    import h5coro

    from zagg.processing import _make_url_rewriter

    h5coro_driver = _resolve_h5coro_driver(driver)
    rewrite = _make_url_rewriter(driver)
    results = []
    for url in granule_urls:
        granule_id = os.path.basename(url)
        h5obj = None
        try:
            h5obj = h5coro.H5Coro(
                rewrite(url), h5coro_driver, credentials=credentials, errorChecking=True
            )
            df, meta = extract_granule_boundaries(h5obj, granule_id, block_chunks=block_chunks)
            name = f"{granule_id.removesuffix('.h5')}.boundaries.parquet"
            local = os.path.join(scratch_dir, name)
            write_boundaries_parquet(df, meta, local)
            dest = _put_parquet(local, output_prefix, name)
            os.remove(local)
            results.append(
                {
                    "granule": granule_id,
                    "ok": True,
                    "wall_s": meta["wall_s"],
                    "n_chunks": meta["n_chunks"],
                    "output": dest,
                }
            )
        except Exception as e:  # per-granule isolation, mirrors the worker loop
            results.append({"granule": granule_id, "ok": False, "error": str(e)})
        finally:
            if h5obj is not None and hasattr(h5obj, "close"):
                h5obj.close()
    return results
