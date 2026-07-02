"""Per-chunk byte-offset extraction for ATL03 granules (issue #158).

Emits one row per HDF5 chunk of every heights dataset (and the geolocation
link arrays the zagg read path uses) so a worker can issue pure ranged GETs
with zero HDF5 metadata I/O — arm (2b) of the #148 stress benchmark:

    (granule, beam, dataset, chunk_idx, elem_start, elem_end,
     byte_offset, nbytes, filter_mask)

``elem_start``/``elem_end`` are half-open element ranges along the photon
(first) axis; ``chunk_idx`` is the row-major linear chunk index (for the
ATL03 datasets here the chunk grid is 1-wide in every trailing dimension, so
it equals the photon-axis chunk index — ``signal_conf_ph``'s ``(100_000, 5)``
chunk spans the full second axis). ``filter_mask`` is the HDF5 per-chunk
filter mask (0 = all pipeline filters applied; recorded for validation).

Two independent extraction routes, cross-checked against each other and
against hidefix's index (see ``crosscheck_hidefix.py``):

- **h5py** (reference): ``dset.id.get_chunk_info(i)`` — libhdf5's own answer.
- **h5coro** (Lambda-deployable, pure Python): a chunk B-tree walk driven
  through h5coro's ``H5Dataset`` internals (``metaOnly`` metadata parse, then
  ``readField``/``readBTreeNodeV1`` over the v1 B-tree). This is the route
  the #152 ``mode="extract"`` Lambda would grow, since the deployed layer has
  no h5py. Note the walk enumerates *every* leaf entry — h5coro's
  hyperslice-intersection off-by-one (issue #148 thread) is not on this path.

Both routes read only metadata (superblock, object headers, B-tree nodes);
no chunk is ever decompressed, which is why offsets extraction is ~100x
cheaper than the #152 boundary-geometry scan.

Self-contained bench tooling (CLAUDE.md exemption from the src/ packaging
rules); parquet I/O conventions mirror ``src/zagg/catalog/extract.py`` on the
PR #152 branch (fastparquet engine + file-level custom metadata) without
importing that unmerged code.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import pandas as pd

ATL03_BEAMS = ("gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r")

#: Within-beam dataset paths to index. ``heights/*`` is what the worker bulk
#: reads; the ``geolocation/*`` link arrays are what today's read plan needs
#: before it can touch heights (segment->photon links + segment coordinates).
HEIGHTS_DATASETS = ("lat_ph", "lon_ph", "h_ph", "signal_conf_ph")
GEOLOCATION_DATASETS = (
    "ph_index_beg",
    "segment_ph_cnt",
    "reference_photon_lat",
    "reference_photon_lon",
)
DEFAULT_DATASETS = tuple(
    [f"heights/{d}" for d in HEIGHTS_DATASETS] + [f"geolocation/{d}" for d in GEOLOCATION_DATASETS]
)

#: Row schema, single source of truth (same empty-frame-drift rationale as
#: the #152 boundary extractor).
OFFSETS_DTYPES = {
    "granule": "object",
    "beam": "object",
    "dataset": "object",
    "chunk_idx": "int64",
    "elem_start": "int64",
    "elem_end": "int64",
    "byte_offset": "int64",
    "nbytes": "int64",
    "filter_mask": "int64",
}

#: Parquet key for the per-granule extraction metadata.
OFFSETS_META_KEY = "zagg:offsets_meta"


def _empty_offsets() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype=t) for c, t in OFFSETS_DTYPES.items()})


def _finalize(rows: list[tuple], granule_id: str) -> pd.DataFrame:
    cols = list(OFFSETS_DTYPES)
    df = pd.DataFrame(rows, columns=cols[1:]) if rows else _empty_offsets().drop(columns="granule")
    df.insert(0, "granule", granule_id)
    df = df.astype(OFFSETS_DTYPES)
    return df.sort_values(["beam", "dataset", "chunk_idx"], ignore_index=True)


def _chunk_rows(
    beam: str,
    dataset: str,
    dims: tuple[int, ...],
    chunk_dims: tuple[int, ...],
    entries: list[tuple[tuple[int, ...], int, int, int]],
) -> list[tuple]:
    """Turn raw per-chunk entries ``(offset_elems, filter_mask, addr, size)``
    into schema rows, computing the linear chunk index and photon-axis range."""
    grid = [math.ceil(d / c) for d, c in zip(dims, chunk_dims)]
    step = [1] * len(grid)
    for d in range(len(grid) - 2, -1, -1):
        step[d] = grid[d + 1] * step[d + 1]
    rows = []
    for offset_elems, filter_mask, addr, size in entries:
        idx = sum((o // c) * s for o, c, s in zip(offset_elems, chunk_dims, step))
        e0 = offset_elems[0]
        e1 = min(e0 + chunk_dims[0], dims[0])
        rows.append((beam, dataset, idx, e0, e1, addr, size, filter_mask))
    return rows


# ---------------------------------------------------------------------------
# route (a): h5py reference
# ---------------------------------------------------------------------------


def extract_offsets_h5py(
    path: str, granule_id: str, *, beams=ATL03_BEAMS, datasets=DEFAULT_DATASETS
) -> tuple[pd.DataFrame, dict]:
    """Extract the offsets table with h5py (libhdf5 chunk queries)."""
    import h5py

    t0 = time.time()
    rows: list[tuple] = []
    missing: list[str] = []
    with h5py.File(path, "r") as f:
        for beam in beams:
            if beam not in f:
                missing.append(beam)
                continue
            for dataset in datasets:
                d = f[f"/{beam}/{dataset}"]
                if d.chunks is None:  # contiguous: one pseudo-chunk, no filters
                    off = d.id.get_offset()
                    if off is None:  # no allocated storage (empty dataset)
                        continue
                    rows.append((beam, dataset, 0, 0, d.shape[0], off, d.id.get_storage_size(), 0))
                    continue
                entries: list[tuple] = []
                try:  # single-pass iteration (h5py>=3.8 + libhdf5>=1.12.3)
                    d.id.chunk_iter(
                        lambda ci, acc=entries: acc.append(
                            (tuple(ci.chunk_offset), ci.filter_mask, ci.byte_offset, ci.size)
                        )
                    )
                except AttributeError:  # pragma: no cover - old libhdf5 fallback
                    for i in range(d.id.get_num_chunks()):
                        ci = d.id.get_chunk_info(i)
                        entries.append(
                            (tuple(ci.chunk_offset), ci.filter_mask, ci.byte_offset, ci.size)
                        )
                rows.extend(_chunk_rows(beam, dataset, d.shape, d.chunks, entries))
    df = _finalize(rows, granule_id)
    meta = {
        "granule": granule_id,
        "route": "h5py",
        "n_chunks": int(len(df)),
        "missing_beams": missing,
        "datasets": list(datasets),
        "wall_s": round(time.time() - t0, 3),
        "schema_version": 1,
    }
    return df, meta


# ---------------------------------------------------------------------------
# route (b): h5coro B-tree walk (pure Python, Lambda-deployable)
# ---------------------------------------------------------------------------


def _walk_chunk_btree(ds) -> list[tuple[tuple[int, ...], int, int, int]]:
    """Enumerate every leaf entry of a chunked dataset's v1 B-tree.

    ``ds`` is a metadata-only ``h5coro.h5dataset.H5Dataset``; nodes are read
    through its ``readField``/``readBTreeNodeV1`` (same field parsing the data
    path uses in ``readBTreeV1``, minus the chunk reads and the hyperslice
    pruning). Returns ``[(offset_elems, filter_mask, byte_offset, nbytes)]``.
    """
    from h5coro.h5dataset import FatalError, H5Dataset

    ro = ds.resourceObject
    entries: list[tuple] = []

    def walk(addr: int) -> None:
        ds.pos = addr
        signature = ds.readField(4)
        node_type = ds.readField(1)
        if signature != H5Dataset.H5_TREE_SIGNATURE_LE:
            raise FatalError(f"invalid b-tree signature: 0x{signature:x}")
        if node_type != 1:
            raise FatalError(f"only raw data chunk b-trees supported: {node_type}")
        node_level = ds.readField(1)
        entries_used = ds.readField(2)
        ds.pos += ro.offsetSize * 2  # skip sibling addresses
        curr = ds.readBTreeNodeV1(ds.meta.ndims)
        for _ in range(entries_used):
            child_addr = ds.readField(ro.offsetSize)
            nxt = ds.readBTreeNodeV1(ds.meta.ndims)
            if node_level > 0:
                pos = ds.pos
                walk(child_addr)
                ds.pos = pos
            else:
                # leaf key: element offset per dim + compressed size + mask
                entries.append(
                    (tuple(curr["slice"]), curr["filter_mask"], child_addr, curr["chunk_size"])
                )
            curr = nxt

    walk(ds.meta.address)
    return entries


def extract_offsets_h5coro(
    h5obj, granule_id: str, *, beams=ATL03_BEAMS, datasets=DEFAULT_DATASETS
) -> tuple[pd.DataFrame, dict]:
    """Extract the offsets table by walking chunk B-trees through h5coro.

    ``h5obj`` is an open ``h5coro.H5Coro`` (any driver — FileDriver locally,
    HTTPDriver for EDL/HTTPS), so this is the route an extraction Lambda runs.
    """
    from h5coro.h5dataset import INVALID_VALUE, H5Dataset

    t0 = time.time()
    rows: list[tuple] = []
    missing: list[str] = []
    for beam in beams:
        for dataset in datasets:
            path = f"/{beam}/{dataset}"
            ds = H5Dataset(h5obj, path, earlyExit=True, metaOnly=True, enableAttributes=False)
            if ds.meta.typeSize == 0:
                # metaOnly never raises for an absent path -- the traversal
                # just leaves default metadata. Mirror h5py: a whole missing
                # beam is recorded and skipped; a missing dataset inside a
                # present beam is a schema violation and raises.
                if dataset == datasets[0]:
                    missing.append(beam)
                    break
                raise KeyError(path)
            dims = tuple(int(x) for x in ds.meta.dimensions or ())
            if not dims or 0 in dims:
                continue
            if ds.meta.address == INVALID_VALUE[h5obj.offsetSize]:
                continue  # no allocated storage (mirrors h5py get_offset() is None)
            if ds.meta.layout == H5Dataset.CHUNKED_LAYOUT:
                chunk_dims = tuple(int(x) for x in ds.meta.chunkDimensions)
                rows.extend(_chunk_rows(beam, dataset, dims, chunk_dims, _walk_chunk_btree(ds)))
            elif ds.meta.layout == H5Dataset.CONTIGUOUS_LAYOUT:
                # one pseudo-chunk, no filters; meta.size is the layout
                # message's storage size. COMPACT data lives inside the object
                # header, not at a file offset -- skip it (h5py's get_offset()
                # returns None there, so route (a) skips it too).
                rows.append((beam, dataset, 0, 0, dims[0], ds.meta.address, ds.meta.size, 0))
    df = _finalize(rows, granule_id)
    meta = {
        "granule": granule_id,
        "route": "h5coro",
        "n_chunks": int(len(df)),
        "missing_beams": missing,
        "datasets": list(datasets),
        "wall_s": round(time.time() - t0, 3),
        "schema_version": 1,
    }
    return df, meta


# ---------------------------------------------------------------------------
# parquet I/O (conventions per src/zagg/catalog/extract.py on PR #152)
# ---------------------------------------------------------------------------


def write_offsets_parquet(df: pd.DataFrame, meta: dict, path: str | Path) -> None:
    df.to_parquet(
        str(path),
        engine="fastparquet",
        index=False,
        custom_metadata={OFFSETS_META_KEY: json.dumps(meta)},
    )


def read_offsets_parquet(path: str | Path) -> tuple[pd.DataFrame, dict]:
    from fastparquet import ParquetFile

    pf = ParquetFile(str(path))
    raw = (pf.key_value_metadata or {}).get(OFFSETS_META_KEY)
    return pf.to_pandas(), (json.loads(raw) if raw else {})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _open_h5coro(path: str):
    import h5coro
    from h5coro import filedriver

    return h5coro.H5Coro(str(path), filedriver.FileDriver, errorChecking=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("granules", nargs="+", help="local .h5 granule paths (or a directory)")
    ap.add_argument("--route", choices=("h5py", "h5coro", "both"), default="both")
    ap.add_argument("--out-dir", default="bench/offsets/out", help="parquet output directory")
    ap.add_argument("--timings-out", default=None, help="optional JSON timings path")
    args = ap.parse_args(argv)

    paths: list[Path] = []
    for g in args.granules:
        p = Path(g).expanduser()
        paths.extend(sorted(p.glob("*.h5")) if p.is_dir() else [p])

    routes = ("h5py", "h5coro") if args.route == "both" else (args.route,)
    out_dir = Path(args.out_dir)
    timings: list[dict] = []
    for path in paths:
        gid = path.name
        for route in routes:
            if route == "h5py":
                df, meta = extract_offsets_h5py(str(path), gid)
            else:
                h5obj = _open_h5coro(str(path))
                try:
                    df, meta = extract_offsets_h5coro(h5obj, gid)
                finally:  # FileDriver holds the fd; don't leak one per granule
                    if hasattr(h5obj, "close"):
                        h5obj.close()
            dest = out_dir / route / f"{gid.removesuffix('.h5')}.offsets.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            write_offsets_parquet(df, meta, dest)
            timings.append({k: meta[k] for k in ("granule", "route", "n_chunks", "wall_s")})
            print(f"{gid} [{route}] {meta['n_chunks']} chunks in {meta['wall_s']}s -> {dest}")
    if args.timings_out:
        Path(args.timings_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.timings_out).write_text(json.dumps(timings, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
