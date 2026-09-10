"""Voxel exports for `hhdc_viewer.ipynb`: isotropic chips and co-registered pairs.

Both leave the `read_tensors` path and build from the stored digests, because each
needs something it cannot give -- an arbitrary output order for the first, and a
z axis shared between two sensors for the second.
"""

from __future__ import annotations

import json
import os
import time
import zipfile
from io import BytesIO

import moczarr as mz
import numpy as np
from moczarr.hhdc import (
    block_rank,
    chunk_z_range,
    rank_to_rowcol,
    rasterize_cell,
    rowcol_to_rank,
)
from mortie import generate_morton_children
from viewers import BLOCK_ORDER, SIDE, UNITS

__all__ = ["fit_window", "registered_pair", "voxel_chips"]


def human_bytes(n) -> str:
    """Byte count -> '320.0 MiB' / '1.25 GiB'. Exports print both sides of a write."""
    return f"{n / 2**30:.2f} GiB" if n >= 2**30 else f"{n / 2**20:.1f} MiB"


def _cell_words(word, order, depth):
    """Order-`order` cells under `word`, in the (2**depth,)**2 raster the cubes use.

    Children come back in z-order rank, the cubes are (row, col) rasters; this is
    the deinterleave between them, done once at WRITE time so the exports carry
    their own geometry and a reader georeferences with ``mort2geo(cells)`` alone.
    """
    rank = rowcol_to_rank(*np.divmod(np.arange(4**depth), 2**depth), depth=depth)
    return generate_morton_children(int(word), order)[rank].reshape(2**depth, 2**depth)


def _write_npy(zf, member, arr):
    buf = BytesIO()
    np.save(buf, arr)
    zf.writestr(member, buf.getvalue())


def _wq(zs, ws, q):
    """Weighted quantile of pre-sorted `zs` with weights `ws`.

    Positions are the MIDPOINT of each centroid's weight interval,
    ``(cumsum - w/2) / total``. Interpolating on the raw cumsum instead biases
    every quantile low by half a centroid -- the median of two points at 0 and
    100 comes back 0.
    """
    cum = np.cumsum(ws)
    if cum[-1] <= 0:
        return float(zs[0])
    return float(np.interp(q * cum[-1], cum - 0.5 * ws, zs))


def fit_window(z, wt, n_bins, dz, first=0.02, step=0.05, max_drop=0.5):
    """Fit `z` into a FIXED `n_bins * dz` window by trimming tails.

    Returns ``(z0, lo_drop, hi_drop, kept)`` -- window floor, the weight fraction
    trimmed off each end, and the fraction that survives.

    The window is fixed because the grid is isotropic: `dz` is the cell edge, so
    the cube really is a cube. When the data does not fit, something has to go,
    and the only honest choices are which end and how much.

    The first cut takes `first` off whichever tail lies further from the WEIGHTED
    median -- the mass sits near the median, so the far tail is the cheaper side
    to lose. Cuts then alternate in `step` increments so neither end is
    preferentially eaten. If `max_drop` is reached without fitting, the window is
    centred on the median and `kept` reports what actually lands inside it, which
    is worse but never silent.
    """
    order = np.argsort(z, kind="stable")
    zs, ws = np.asarray(z)[order], np.asarray(wt)[order]
    span = n_bins * dz
    total = float(ws.sum())
    z0 = float(np.floor(zs[0]))
    # Test the window we will ACTUALLY use. `z0` is floored, so it sits up to
    # 1 m below `zs[0]`; checking `zs[-1] - zs[0] <= span` passes data that then
    # falls off the top of the window and is dropped while this reports kept=1.
    if total <= 0 or zs[-1] < z0 + span:
        return z0, 0.0, 0.0, 1.0

    med = _wq(zs, ws, 0.5)
    drop_high = (zs[-1] - med) >= (med - zs[0])  # start on the far tail
    lo_d = hi_d = 0.0
    cut = first
    while lo_d + hi_d + cut <= max_drop:
        if drop_high:
            hi_d += cut
        else:
            lo_d += cut
        drop_high, cut = not drop_high, step
        lo, hi = _wq(zs, ws, lo_d), _wq(zs, ws, 1.0 - hi_d)
        if hi - lo <= span:
            # `kept` is what LANDS in the window, not the nominal quantile drop:
            # the window is wider than the trimmed range and recaptures some of it.
            z0 = float(np.floor(lo))
            return z0, lo_d, hi_d, float(ws[(zs >= z0) & (zs < z0 + span)].sum() / total)

    z0 = float(np.floor(med - span / 2))  # give up trimming; centre on the mass
    inside = float(ws[(zs >= z0) & (zs < z0 + span)].sum() / total)
    return z0, None, None, inside  # None, not NaN: `meta.json` has to parse


def voxel_chips(handles, block, sensor="atl03", order=22, side=128, path=None):
    """One o12 block -> isotropic `side`**3 count chips, empty chips skipped.

    Shifting a centroid's block-local rank right by ``2 * (29 - order)`` truncates
    its order-29 point word to `order` -- nested ranks are hierarchical -- which is
    how the cube gets finer than the stored cells. The z bin equals the cell edge
    and `n_bins` equals `side`, so every chip is a true cube; each keeps its own
    `z0` and trim record in `meta.json`, and its cell words in a `.cells` member.
    """
    store, field = handles[sensor]
    n_bins = side
    dz = SIDE / 2 ** (order - BLOCK_ORDER)  # isotropic: z bin == cell edge
    depth = order - (side.bit_length() - 1) - BLOCK_ORDER
    tiles = generate_morton_children(int(block), order - (side.bit_length() - 1))

    t0 = time.perf_counter()
    got = list(mz.read_ragged(store, field, locations=True, subtree=mz.morton_decimal(int(block))))
    v = np.concatenate([np.asarray(r[1]) for r in got])
    z, wt = v[:, 0], v[:, 1]
    rank = block_rank(np.concatenate([np.asarray(r[2], np.uint64) for r in got]), BLOCK_ORDER)[0]
    row, col = rank_to_rowcol(rank >> np.uint64(2 * (29 - order)), order - BLOCK_ORDER)
    read_s = time.perf_counter() - t0

    # Stream each chip out and drop it: 64 x 8 MiB held at once is 512 MiB, and
    # mybinder caps the container at 2 GB. An .npz is a zip of .npy members.
    path = path or f"{sensor}_o{order}_chips_{mz.morton_decimal(int(block))}.npz"
    t1 = time.perf_counter()
    meta, kept, filled, dense, trimmed = {}, 0, 0, 0, 0
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        for i in range(2**depth):
            for j in range(2**depth):
                m = np.flatnonzero((row // side == i) & (col // side == j))
                if not len(m):
                    continue
                z0, lo_d, hi_d, frac = fit_window(z[m], wt[m], n_bins, dz)
                # np.floor, not a bare cast: `astype` truncates TOWARD ZERO, so a
                # centroid in [z0 - dz, z0) gives -0.x -> 0, passes `iz >= 0`, and
                # lands in bin 0 at the wrong height. `fit_window` can put z0 above
                # the minimum, so this is reachable whenever a chip is trimmed.
                iz = np.floor((z[m] - z0) / dz).astype(np.int64)
                keep = (iz >= 0) & (iz < n_bins)
                trimmed += int(wt[m][~keep].sum())
                mk, iz = m[keep], iz[keep]
                acc = np.zeros((side, side, n_bins), dtype=np.float32)
                np.add.at(acc, (row[mk] % side, col[mk] % side, iz), wt[mk])
                chip = np.rint(acc).astype(np.uint32)  # merged centroids weigh fractionally
                tile = int(tiles[rowcol_to_rank(i, j, depth=depth)])
                name = mz.morton_decimal(tile)
                _write_npy(zf, f"{name}.npy", chip)
                _write_npy(zf, f"{name}.cells.npy", _cell_words(tile, order, side.bit_length() - 1))
                meta[name] = {
                    "z0": z0,
                    "dz": dz,
                    "written": int(chip.sum()),
                    "trim_low": lo_d,
                    "trim_high": hi_d,
                    "kept": frac,
                }
                kept += int(chip.sum())
                filled += int(np.count_nonzero(chip))
                dense += chip.nbytes
        zf.writestr("meta.json", json.dumps(meta))  # z0 + trim per chip

    n, on_disk = 2 ** (2 * depth), os.path.getsize(path)
    cut = [k for k, mm in meta.items() if mm["kept"] < 1.0]
    print(f"{sensor} o{order} — {dz:.3f} m isotropic voxels, {side}^3 = {dz * side:.0f} m cubes")
    print(f"  read   {len(z):,} centroids, {wt.sum():,.0f} {UNITS[sensor]}, {read_s:.1f}s")
    print(
        f"  wrote  {len(meta)} of {n} chips, {kept:,} of {wt.sum():,.0f} {UNITS[sensor]}"
        + (f" ({trimmed:,} trimmed to fit)" if trimmed else " (nothing trimmed)")
        + f", {time.perf_counter() - t1:.1f}s"
    )
    if cut:
        worst = min(meta[k]["kept"] for k in cut)
        print(
            f"         {len(cut)} chip(s) needed a trim; worst kept {100 * worst:.1f}% of its weight"
        )
    # Dense size is the tensors in MEMORY; the file is their deflate (mostly runs
    # of zeros) PLUS a cell-word lattice per chip, so the ratio is the whole
    # archive against the tensors, not the tensors' compressibility alone.
    print(
        f"         {human_bytes(dense)} of tensors in memory -> {human_bytes(on_disk)} on disk "
        f"with their cell words ({dense / on_disk:.0f}x — "
        f"{100 * filled / (dense // 4):.3f}% of voxels occupied)"
    )
    return path, meta


def registered_pair(handles, block, order=19, n_bins=128, resolution=0.5, path=None):
    """Both sensors as cubes of ONE shape: one xy lattice, one z axis.

    Each GEDI o18 cell is REPLICATED into its four o19 children rather than ATL03
    being merged up to o18 -- so a GEDI cube sums to four times its stored weight.
    The `cells` key is the shared lattice's cell words, in cube (row, col) order.
    `chunk_z_range` is handed both sensors' digests together; derived per sensor it
    puts them bins apart, and nothing downstream notices.
    """
    t0 = time.perf_counter()
    side = 2 ** (order - BLOCK_ORDER)
    got = {
        n: list(mz.read_ragged(store, field, subtree=mz.morton_decimal(int(block))))
        for n, (store, field) in handles.items()
    }
    read_s = time.perf_counter() - t0

    # What each sensor would get ALONE, against the window taken over both.
    window = dict(
        n_bins=n_bins, resolution=resolution, bottom=0.05, top=0.95, fit="degrade_resolution"
    )
    solo = {
        n: chunk_z_range([np.asarray(v) for _w, v in rows], **window) for n, rows in got.items()
    }
    z0, n_bins, dz = chunk_z_range(
        [np.asarray(v) for rows in got.values() for _w, v in rows], **window
    )

    t1 = time.perf_counter()
    cubes, lines = {}, []
    for name, (_store, field) in handles.items():
        cell_order = int(field.split("/", 1)[0])
        k = 2 ** (order - cell_order)  # children of one cell on the output grid
        words = np.array([w for w, _v in got[name]], dtype=np.uint64)
        r, c = rank_to_rowcol(block_rank(words, BLOCK_ORDER)[0], cell_order - BLOCK_ORDER)
        cube = np.zeros((side, side, n_bins), dtype=np.float32)
        for i, (_w, v) in enumerate(got[name]):
            cube[r[i] * k : (r[i] + 1) * k, c[i] * k : (c[i] + 1) * k] = rasterize_cell(
                np.asarray(v), z0, dz, n_bins
            )
        cubes[name] = cube
        lines.append(
            f"         {name}: {len(words):,} o{cell_order} cells -> {k}x{k} -> "
            f"{int((cube.sum(2) > 0).sum()):,}/{side * side:,} columns, "
            f"{cube.sum():,.0f} {UNITS[name]}" + (f" ({k**2}x replicated)" if k > 1 else "")
        )

    path = path or f"registered_o{order}_{mz.morton_decimal(int(block))}.npz"
    cells = _cell_words(block, order, order - BLOCK_ORDER)
    np.savez_compressed(path, **cubes, z0=z0, dz=dz, order=order, cells=cells)
    dense, on_disk = sum(c.nbytes for c in cubes.values()), os.path.getsize(path)
    fit = "as asked" if abs(dz - resolution) < 1e-9 else f"DEGRADED from {resolution:g} m"
    print(
        f"registered o{order} — {next(iter(cubes.values())).shape} float32 each, {read_s:.1f}s read"
    )
    print(
        f"  grid   shared z = {z0:.1f} m + bin * {dz:g} m ({fit}) — alone they would be "
        + " vs ".join(f"{n} {s0:.1f}/{g:g} m" for n, (s0, _b, g) in solo.items())
    )
    print("\n".join(lines))
    print(
        f"  wrote  {human_bytes(dense)} of cubes in memory -> {human_bytes(on_disk)} on disk "
        f"in {path} with the shared cell lattice, {time.perf_counter() - t1:.1f}s"
    )
    return path, cubes
