"""Shared viewers for the reader-only demo notebooks.

`hhdc_viewer.ipynb` and `waveform_viewer.ipynb` both open the same paired
stores and differ only in how they draw what they find, so the drawing lives
here and the notebooks stay about the READ path -- polygon, coverage, shards,
leaves, digests.

Everything here is reader-side: `mortie` for the geometry, `moczarr` for the
store. The one zagg import (`cdf_from_tdigest`) is lazy and confined to the
waveform view -- moczarr imports the t-digest algebra from zagg rather than
vendoring it (moczarr issue #19), which is what the `moczarr[zagg]` extra
carries and what `read_tensors` has always needed.
"""

from __future__ import annotations

import time

import matplotlib.pyplot as plt
import moczarr as mz
import numpy as np
from IPython.display import display
from ipywidgets import (
    Checkbox,
    Dropdown,
    FloatSlider,
    FloatText,
    HBox,
    IntSlider,
    Layout,
    VBox,
    interactive_output,
)
from matplotlib.colors import LogNorm
from moczarr.hhdc import block_rank, rank_to_rowcol
from mortie import generate_morton_children, toc2time

#: The o12 block the views frame everything in. Cells across a block edge are
#: ``2**(cell_order - BLOCK_ORDER)`` -- 128 for ATL03's o19, 64 for GEDI's o18.
BLOCK_ORDER = 12

#: o12 block side in metres (equal-area, square-equivalent).
SIDE = float(np.sqrt(4 * np.pi / (12 * 4**BLOCK_ORDER)) * 6_371_000)

#: Points drawn per sensor per block. A leaf holds millions; the 3-D view
#: subsamples deterministically so rotation stays smooth.
CAP = 20_000

#: What one unit of a sensor's stored WEIGHT is. ATL03 counts photons. GEDI's
#: flux is background-subtracted counts scaled by a named receiver gain, and
#: this store ships `gain: {name: "unit", value: 1.0}` -- a placeholder -- so
#: "pe" here is PROPORTIONAL to photoelectrons, not calibrated to them (the
#: published chain is ~0.4 pe/count; see `waveform_viewer.ipynb`). The two are
#: not commensurate and run orders of magnitude apart, so every count is
#: labelled and none are ever summed together.
UNITS = {"atl03": "ph", "gedi": "pe"}

__all__ = [
    "BLOCK_ORDER",
    "CAP",
    "SIDE",
    "UNITS",
    "View",
    "block_of",
    "grid_xy",
    "densest_shard",
    "joint_cells",
    "paired_blocks",
    "viewer_stats",
    "load",
    "view3d",
    "waveform_view",
]


def _si(n) -> str:
    """1266765 -> '1.27M'. Counts below a thousand stay exact."""
    n = float(n)
    for cut, suffix in ((1e9, "G"), (1e6, "M"), (1e3, "k")):
        if n >= cut:
            return f"{n / cut:.3g}{suffix}"
    return f"{n:,.0f}"


def paired_blocks(handles, block_order: int = BLOCK_ORDER, n_bins: int = 256, res: float = 1.0):
    """Read both sensors' block tensors and rank the blocks by coincident cells.

    Returns ``(gains, pairs)``: the per-sensor ``{block word: (offset, gain)}`` z
    window, and the sorted ``(word, joint mask, A2, G2)`` list the waveform view
    picks cells out of. `A2` is ATL03's column sums folded 2x2 onto GEDI's o18
    grid -- one GEDI footprint covers exactly four ATL03 cells -- so `joint` marks
    the o18 cells both sensors populate.

    Holds NO tensor. One ATL03 block at these settings is (128, 128, 256) uint32 =
    16 MiB and a shard has 64 per sensor, so keeping the `read_tensors` tuples was
    1.25 GiB against mybinder's 2 GB cap. Each tensor is reduced to its column sums
    (64 KiB) and dropped as it arrives, and the z gain is the only other thing the
    view ever needed from it -- so peak is one block, not sixty-four.

    Totals printed here come from the TENSORS, so they count only what fell inside
    each block's z window and run a few per cent under what the digests hold.
    """
    t0 = time.perf_counter()
    gains, cols = {}, {}
    for name, (store, field) in handles.items():
        gains[name], cols[name] = {}, {}
        for tensor, _mask, window, w in mz.read_tensors(
            store,
            field,
            n_bins=n_bins,
            resolution=res,
            block_order=block_order,
            fit="degrade_resolution",
        ):
            gains[name][w] = window
            cols[name][w] = tensor.sum(axis=2)  # the tensor is dropped here
    print(
        f"paired tensors in {time.perf_counter() - t0:.1f}s — "
        + ", ".join(f"{len(v)} {k} blocks" for k, v in cols.items())
    )
    pairs = []
    for w, g2 in cols["gedi"].items():
        if w not in cols["atl03"]:
            continue
        gside = g2.shape[0]
        a2 = cols["atl03"][w].reshape(gside, 2, gside, 2).sum(axis=(1, 3))
        joint = (a2 > 0) & (g2 > 0)
        if joint.any():
            pairs.append((w, joint, a2, g2))
    pairs.sort(key=lambda p: -int(p[1].sum()))
    for w, j, a2, g2 in pairs[:8]:
        print(
            f"  {mz.morton_decimal(w)}  {int(j.sum()):4,} joint o18 cells   "
            f"{int(a2[j].sum()):9,} atl03 ph*   {int(g2[j].sum()):9,} gedi pe*"
        )
    print("  * tensor totals: only what fell inside each block's z window")
    return gains, pairs


def viewer_stats(handles, block_order: int = BLOCK_ORDER) -> dict:
    """Sweep each field once and return what the block dropdown needs.

    One pass per sensor over ONE field's stored digests. Nothing is kept -- the
    viewer reads per block on demand -- but the per-block cell and weight counts,
    and the count of cells both sensors populate, fall out of the same pass for
    free. Also prints what that pass cost, which is the price of reading one
    array of the leaf end to end (this ATL03 leaf holds nine).

    Reading the dense `morton`/`count` sidecars instead is slower, not faster:
    they span the shard's whole cell space (1,048,576 entries at o19) where this
    touches only populated cells. Measured 124 s against 74 s.
    """
    tally, words = {}, {}
    for name, (store, field) in handles.items():
        t0 = time.perf_counter()
        _, element = mz.open_ragged(store, field)
        per, seen, centroids, nbytes, obs = {}, [], 0, 0, 0.0
        for word, value in mz.read_ragged(store, field):
            v = np.asarray(value)
            seen.append(word)
            weight = float(v[:, 1].sum())  # column 1 is the centroid weight
            block = int(block_of(word, block_order))
            had_cells, had_obs = per.get(block, (0, 0.0))
            per[block] = (had_cells + 1, had_obs + weight)
            centroids, nbytes, obs = centroids + len(v), nbytes + v.nbytes, obs + weight
        tally[name] = per
        words[name] = np.asarray(seen, dtype=np.uint64)
        print(
            f"{name:6s} swept {field} in {time.perf_counter() - t0:5.1f}s — "
            f"{len(seen):,} cells over {len(per)} blocks, {centroids:,} centroids, "
            f"{obs:,.0f} {UNITS.get(name, 'obs')}, {nbytes / 2**20:.1f} MiB decoded"
        )
        print(f"       element {element}")
    return {"tally": tally, "joint": joint_cells(words, block_order)}


def densest_shard(root, shards, **s3) -> str:
    """The shard with the most granules, by the store's own leaf telemetry.

    `read_stats` fetches one leaf's stats sidecar by arithmetic on the manifest
    -- no LIST, a few KB each -- so ranking a handful of candidates is cheap.
    This is the rule `demo/06_paired` used (`max` on the GEDI granule count),
    reproduced here off the published store instead of a local shardmap, so
    both demos land on the same shard without anyone hardcoding an id.
    """
    return max(shards, key=lambda s: (mz.read_stats(root, s, **s3) or {}).get("n_granules", 0))


def joint_cells(words, block_order: int = BLOCK_ORDER):
    """{block word: cells BOTH sensors populate}, from the sweep's words alone.

    The sensors are keyed at different orders -- ATL03 o19, GEDI o18 -- so each
    side is folded to the COARSER of the two before intersecting: one GEDI
    footprint covers 2x2 ATL03 cells, and it is joint when any of the four holds
    a digest. Exact, and free: the open-and-sweep pass already visited every one
    of these words, so this reads nothing and needs no tensor.

    (Checked against the tensor-derived mask `waveform_viewer` builds -- both
    give 27 joint cells on block 4331422233444 -- and against the occupancy
    channel `read_tensors` returns, which agrees to the cell.)
    """
    if len(words) < 2:
        return {}
    per, coarse = {}, None
    for name, w in words.items():
        w = np.asarray(w, dtype=np.uint64)
        if not len(w):
            return {}  # a sensor with nothing here has nothing joint anywhere
        order = int(np.asarray(block_rank(w[:1], block_order)[1]).ravel()[0])
        per[name] = w
        coarse = order if coarse is None else min(coarse, order)
    both = None
    for w in per.values():
        folded = np.unique(block_of(w, coarse))
        both = folded if both is None else np.intersect1d(both, folded, assume_unique=True)
    if not len(both):
        return {}
    blocks, counts = np.unique(block_of(both, block_order), return_counts=True)
    return {int(b): int(c) for b, c in zip(blocks, counts)}


def block_of(words, block_order: int = BLOCK_ORDER):
    """Cell word(s) -> their order-`block_order` ancestor word, by truncation.

    A packed area word is ``[prefix | body | 6-bit suffix]`` and the suffix
    carries the order, so an ancestor is the body truncated to the ancestor's
    depth with the suffix rewritten -- no decode, and it vectorizes over an
    array. Only valid for AREA words at or below `block_order`.
    """
    sh = np.uint64(6 + 2 * (27 - block_order))
    return ((np.asarray(words, dtype=np.uint64) >> sh) << sh) | np.uint64(block_order)


# --------------------------------------------------------------------------- #
# geometry
# --------------------------------------------------------------------------- #
def grid_xy(words, block_order: int = BLOCK_ORDER):
    """Word -> (x, y) metres inside its o12 block. Three library calls, no bits.

    ``block_rank`` recovers each word's block-local nested rank and its own
    order -- normalizing POINT words to their order-29 area twins on the way,
    which is the step whose absence used to make the level-28/29 digits decode
    out of range. ``rank_to_rowcol`` is the bit deinterleave (mortie spec
    section 8): it returns ``(row, col) = (y, x)`` with ``[0, 0]`` at the
    subtree's south corner, and this returns them as ``(x, y) = (col, row)``
    so the picture matches the tensors this notebook exports.

    Those axes are NOT east and north. zagg's ``readers/_layout.py`` pins the
    semantics: "``tensor[0, 0]`` is the subtree's south corner; rows advance
    toward the north-WEST edge, columns toward the north-EAST edge." A HEALPix
    face is a diamond, so its two local axes run along the face's edges, not
    along the compass. ``view3d`` labels them for what they are.

    Note the tensors ``read_tensors`` (and so ``hhdc_viewer``'s export cells)
    hand back are indexed ``(row, col, bin)``: the picture's x is the tensor's
    SECOND axis. :func:`_binned_pts` applies the same swap, so the binned and
    exact views agree with each other.

    A leaf mixes orders -- o29 located words beside coarser merged-centroid
    words, and GEDI's cell words are o18 -- so the ranks are grouped by depth
    and handed over one vectorized call per depth.
    """
    rank, order = block_rank(words, block_order)
    depth = order - block_order
    row = np.zeros(len(rank))
    col = np.zeros(len(rank))
    for d in np.unique(depth):
        m = np.flatnonzero(depth == d)
        r, c = rank_to_rowcol(rank[m], int(d))
        row[m] = np.asarray(r, dtype=float)
        col[m] = np.asarray(c, dtype=float)
    side = 2.0 ** depth.astype(float)  # cells along a block edge
    return (col + 0.5) / side * SIDE, (row + 0.5) / side * SIDE


def load(store, field, block_order: int = BLOCK_ORDER) -> dict:
    """One sensor's centroids: z, weight, xy, o12 block, acquisition days.

    xy comes from the located sibling's word where the field declares one
    (ATL03 -- point-exact) and from the cell word where it does not (GEDI
    flux, whose shots are unlocated by design, so a centroid renders at its
    cell's centre). Both land in the block's own lattice frame.
    """
    arr, _ = mz.open_ragged(store, field)
    attrs = dict(arr.attrs)
    locname = (attrs.get("ragged") or {}).get("locations")
    tname = attrs.get("times")
    zs, wts, cells, locw, seq = [], [], [], [], []
    for row in mz.read_ragged(store, field, locations=bool(locname)):
        seq.append(row[0])
        v = np.asarray(row[1])
        zs.append(v[:, 0])
        wts.append(v[:, 1])
        cells.append(np.full(len(v), row[0], dtype=np.uint64))
        if locname:
            locw.append(np.asarray(row[2], dtype=np.uint64))
    z, wt = np.concatenate(zs), np.concatenate(wts)
    cells = np.concatenate(cells)
    blocks = block_of(cells, block_order)
    x, y = grid_xy(np.concatenate(locw) if locname else cells, block_order)
    t = None
    if tname:
        tmap = {
            int(r[0]): np.asarray(r[1], dtype=np.uint64).ravel()
            for r in mz.read_ragged(store, field.rsplit("/", 1)[0] + "/" + tname)
        }
        tw = np.concatenate([tmap[int(c)] for c in seq])
        ns2018 = float(
            (np.datetime64("2018-01-01") - np.datetime64("1850-01-01")) // np.timedelta64(1, "ns")
        )
        t = (np.asarray(toc2time(tw)[0], dtype="float64") - ns2018) / 86.4e12
    return {
        "z": z,
        "wt": wt,
        "x": x,
        "y": y,
        "blocks": blocks,
        "t": t,
        "xy_note": "exact xy" if locname else "cell-center xy",
    }


# --------------------------------------------------------------------------- #
# the 3-D view
# --------------------------------------------------------------------------- #
def _binned_pts(tensor, offset, gain, cap: int = CAP):
    """Occupied voxels of one block tensor -> (x, y, z, weight), subsampled.

    ``x``/``y`` come back as FRACTIONS of the block edge (the tensor's own
    ``side`` is its xy shape), so the caller scales them by :data:`SIDE` the
    same way the exact path does, and ``x`` is the tensor's COL axis and ``y``
    its ROW axis, the same round as :func:`grid_xy`.

    ``np.nonzero`` gives cell INDICES, so the ``+ 0.5`` that moves a point off
    the cell corner onto its centre is the same one :func:`grid_xy` applies --
    without it, toggling **binned** shifts the whole cloud by half a cell, and
    by a DIFFERENT half-cell per sensor (6.2 m for ATL03's o19 cells, 12.4 m
    for GEDI's o18).
    """
    side = tensor.shape[0]
    rows, cols, zs = np.nonzero(tensor)  # the tensor is indexed (row, col, bin)
    counts = tensor[rows, cols, zs].astype("float64")
    if len(rows) > cap:
        keep = np.random.default_rng(0).choice(len(rows), cap, replace=False)
        rows, cols, zs, counts = rows[keep], cols[keep], zs[keep], counts[keep]
    return (cols + 0.5) / side, (rows + 0.5) / side, offset + zs * gain, counts


def _coverage(recs):
    """Coarse sensor -> ``(fine sensor, its cells over a fine cell, its cells)``.

    One GEDI o18 footprint tiles exactly 2x2 of ATL03's o19 cells, so "does
    this GEDI cell sit over a stored ATL03 SIGNAL digest" is a block-reduce of the
    finer occupancy channel ANDed with the coarser one: two boolean arrays of
    at most 128x128, and no extra I/O -- `read_tensors` hands the occupancy
    back beside every tensor it already read.

    Occupancy is `mask == 2` ("observed, digest stored"), never
    `mask.astype(bool)`: where the leaf carries an exact occupancy sidecar the
    mask is 3-state and `1` means observed-but-EMPTY, which is precisely the
    cell this is asking about. Counting it as populated would inflate both the
    cell counts and this overlap.
    """
    occ = {n: r["occ"] for n, r in recs.items() if r["occ"] is not None}
    if len(occ) < 2:  # a block one sensor missed entirely -- nothing to compare
        return {}
    fine = max(occ, key=lambda n: occ[n].shape[0])
    out = {}
    for name, o in occ.items():
        side = o.shape[0]
        k, rem = divmod(occ[fine].shape[0], side)
        if name == fine or rem:  # not a whole-cell nesting; say nothing
            continue
        over = occ[fine].reshape(side, k, side, k).any(axis=(1, 3))
        out[name] = (fine, int((over & o).sum()), int(o.sum()))
    return out


class View:
    """Holds what is on screen so `export` knows which block you mean."""

    shard = None
    block = None


def view3d(
    handles,
    shard,
    stats=None,
    block_order: int = BLOCK_ORDER,
    cap: int = CAP,
    n_bins: int = 256,
    resolution: float = 1.0,
) -> View:
    """Interactive paired 3-D view. Drag to rotate -- the two panes stay linked.

    ``handles`` is ``{sensor: (leaf_store, field)}``. Two ways to see the same
    block, switched by the **binned** box:

    * **binned** (default) -- the fixed ``read_tensors`` voxels, so xy AND z
      are on the store's own grid and the two sensors share a z window.
    * **exact** -- the digests read straight through, z the stored float32
      centroid elevation and xy decoded from the located sibling's word where
      the field carries one (ATL03, point-exact) or the cell word where it
      does not (GEDI flux, footprint scale).

    Nothing is read until it is drawn. The block list is arithmetic, each
    block's tensor is fetched the first time you select it and cached, and the
    exact centroids are swept lazily on the first unbinned draw -- so a reader
    who looks at one block pays for one block, and one who never unticks the
    box never pays for the sweep at all.

    `stats` is what :func:`viewer_stats` returns; it labels and orders the block
    dropdown, and costs nothing here because that pass has already been paid for.
    Those are STORED totals over the whole digest. The counts each read prints,
    and the ones in the pane titles, are what landed inside the tensor's finite
    z window and so run lower -- the tails are trimmed at `bottom`/`top` and
    anything past ``n_bins * gain`` metres of relief has nowhere to go.

    Returns the :class:`View` the widgets write to, so a later cell can export
    whatever is on screen.
    """
    view = View()
    view.shard = shard
    names = list(handles)
    tally = (stats or {}).get("tally")
    joint = (stats or {}).get("joint")

    # One block per read, cached. Sweeping the shard up front cost ~1.3 GiB
    # resident (Binder caps at 2 GB) and ~148 s before the first frame.
    voxels: dict = {n: {} for n in names}

    def _voxels(name, block):
        """This sensor's drawable voxels for one block, read once and cached."""
        hit = voxels[name].get(block)
        if hit is None:
            store, field = handles[name]
            t0 = time.perf_counter()
            got = next(
                iter(
                    mz.read_tensors(
                        store,
                        field,
                        n_bins=n_bins,
                        resolution=resolution,
                        block_order=block_order,
                        fit="degrade_resolution",
                        subtree=mz.morton_decimal(block),
                    )
                ),
                None,
            )
            if got is None:  # the block holds nothing in this store
                hit = {"pts": None, "cells": 0, "gain": resolution, "occ": None, "obs": 0.0}
            else:
                occ = got[1] == 2  # stored digest; see `_coverage` on why not `.astype(bool)`
                hit = {
                    "pts": _binned_pts(got[0], *got[2], cap),
                    "cells": int(occ.sum()),
                    "gain": got[2][1],
                    "occ": occ,
                    "obs": float(got[0].sum(dtype="float64")),
                    "bins": got[0].shape[2],
                }
            voxels[name][block] = hit
            # Report what the read actually MOVED, not a resident-memory delta:
            # cells, the sensor's own weight unit, and the z window they fell in.
            print(
                f"  {name} {mz.morton_decimal(block)}: "
                + (
                    "empty"
                    if hit["pts"] is None
                    else f"{hit['cells']:,} cells, {_si(hit['obs'])} "
                    f"{UNITS.get(name, 'obs')} in a {hit['bins']} x {hit['gain']:g} m z window"
                )
                + f", {time.perf_counter() - t0:.1f}s",
                flush=True,
            )
        return hit

    exact: dict = {}  # filled on the first unbinned draw

    def _exact():
        if not exact:
            # One whole-shard sweep per sensor, millions of centroids -- say so,
            # or the first untick reads as a frozen notebook.
            print("sweeping exact centroids (once, whole shard, both sensors)...", flush=True)
            t0 = time.perf_counter()
            for name, (store, field) in handles.items():
                exact[name] = load(store, field, block_order)
            print(
                "  "
                + ", ".join(f"{len(exact[n]['z']):,} {n}" for n in handles)
                + f" in {time.perf_counter() - t0:.0f}s",
                flush=True,
            )
        return exact

    # Blocks the sweep actually found, so they cannot disagree with the open
    # leaf; without a tally, arithmetic off the shard word, no I/O.
    within = {int(w) for w in generate_morton_children(int(mz.morton_word(shard)), block_order)}
    if tally:
        found = {w for per in tally.values() for w in per}
        if not found & within:
            raise ValueError(
                f"the stats hold no block beneath shard {shard} — `handles`/`stats` and "
                f"`shard` disagree. Open and view the SAME shard (one name, used twice)."
            )
        blocks = found & within
    else:
        blocks = within
    if not blocks:
        raise ValueError(f"shard {shard}: no order-{block_order} blocks beneath it")

    # Rank by coincident cells -- no scaling needed, unlike the weights.
    if joint:
        order = sorted(blocks, key=lambda w: (-joint.get(w, 0), -w))
    else:
        order = sorted(blocks, reverse=True)  # descending, so ...444 heads the list

    def _label(w):
        """Block label: decimal id, coincident cells, each sensor's stored size."""
        parts = []
        if joint is not None:
            parts.append(f"{joint.get(w, 0):,} joint")
        parts += [
            f"{n} {c:,} cells / {_si(o)} {UNITS.get(n, 'obs')}"
            for n in names
            for c, o in [(tally or {}).get(n, {}).get(w, (0, 0.0))]
        ]
        return mz.morton_decimal(w) + (" — " + " · ".join(parts) if tally or joint else "")

    dd = Dropdown(
        options=[(_label(w), w) for w in order],
        value=order[0],
        description="block",
        layout=Layout(width="700px" if (tally or joint) else "260px"),
    )
    zmode = Dropdown(
        options=[("independent z", "auto"), *[(f"pin z to {n}", n) for n in names]],
        value="auto",
        description="z extent",
    )
    elev_cb = Checkbox(value=False, description="color by elevation (shared)")
    time_cb = Checkbox(value=False, description="color by time (shared)")
    bin_cb = Checkbox(value=True, description="binned (fixed tensors)")
    # Flat by default: weight is already the colour, and letting it drive alpha
    # too put 76% of a block's voxels under 0.10 and hid the single-count ones.
    # `shade by weight` puts that cue back deliberately, spanning the slider's
    # value down to a twentieth of it.
    # `continuous_update=False` here, unlike the waveform's `nth`: this slider
    # reads nothing (block voxels are cached), so the only cost is the redraw --
    # ~585 ms for 2 panes of 20k 3-D points. Firing that per drag step queues
    # frames for a value nobody asked to stop on; on release it is one redraw.
    alpha_sl = FloatSlider(
        min=0.05,
        max=1.0,
        step=0.05,
        value=0.55,
        description="opacity",
        continuous_update=False,
        readout_format=".2f",
        layout=Layout(width="330px"),
    )
    shade_cb = Checkbox(value=False, description="shade by weight")

    said: set = set()  # blocks whose coarse-over-fine line has already printed

    def _panes(block, binned):
        if binned:
            recs = {n: _voxels(n, block) for n in names}
            cov = _coverage(recs)
            if block not in said:
                said.add(block)
                for n, (fine, over, total) in cov.items():
                    print(
                        f"  {n} over {fine}: {over:,} of {total:,} cells "
                        f"({100 * over / max(total, 1):.0f}%)",
                        flush=True,
                    )
            out = []
            for n in names:
                rec = recs[n]
                if rec["pts"] is None:  # nothing stored here for this sensor
                    empty = np.empty(0)
                    out.append(
                        {
                            "x": empty,
                            "y": empty,
                            "z": empty,
                            "wt": empty,
                            "t": None,
                            "label": n,
                            "xy_note": "no data in this block",
                        }
                    )
                    continue
                x, y, z, wt = rec["pts"]
                note = f"binned ({rec['gain']:g} m z, {rec['cells']:,} cells"
                if n in cov:
                    fine, over, total = cov[n]
                    note += f", {100 * over / max(total, 1):.0f}% over {fine}"
                out.append(
                    {
                        "x": x * SIDE,
                        "y": y * SIDE,
                        "z": z,
                        "wt": wt,
                        "t": None,
                        "label": n,
                        "xy_note": note + ")",
                    }
                )
            return out
        data = _exact()
        out = []
        rng = np.random.default_rng(0)
        for n in names:
            d = data[n]
            m = np.flatnonzero(d["blocks"] == np.uint64(block))
            if len(m) > cap:
                m = m[rng.choice(len(m), cap, replace=False)]
            out.append(
                {
                    **{k: d[k][m] for k in ("z", "wt", "x", "y")},
                    "t": None if d["t"] is None else d["t"][m],
                    "label": n,
                    "xy_note": d["xy_note"],
                }
            )
        return out

    live: dict = {}  # the one figure this view keeps open

    def draw(block, by_elev, by_time, binned, zmode, opacity, shade):
        view.block = block
        panes = _panes(block, binned)
        # A block can be empty in one store and not the other (and every pane is
        # empty if it is empty in both), so every range below is taken over the
        # NON-empty panes only -- `.min()` and `np.percentile` both raise on an
        # empty array, which would take the whole view down on a sparse block.
        filled = [p for p in panes if len(p["z"])]
        pinned = panes[names.index(zmode)] if zmode != "auto" else None
        zlim = (
            (pinned["z"].min(), pinned["z"].max())
            if pinned is not None and len(pinned["z"])
            else None
        )
        shared_elev = (
            plt.Normalize(min(p["z"].min() for p in filled), max(p["z"].max() for p in filled))
            if by_elev and not by_time and filled
            else None
        )
        tv = [p["t"] for p in panes if p["t"] is not None and len(p["t"])]
        shared_time = (
            plt.Normalize(min(t.min() for t in tv), max(t.max() for t in tv))
            if (by_time and tv)
            else None
        )
        ticks = [0.0, SIDE / 2, SIDE]
        labels = [f"{v:.0f} m" for v in ticks]

        # `draw` reruns on EVERY widget change, and `clear_output(wait=True)`
        # clears the Output widget's display, not matplotlib's figure registry.
        # An ipympl figure is a live canvas -- it holds its websocket comm and
        # the `_sync` handler connected below for the kernel's lifetime -- so
        # without this the 21st interaction warns and every orphan keeps
        # handling mouse events. Close the PREVIOUS figure, not this one: the
        # widget backend needs the current canvas alive to render it.
        if (prev := live.pop("fig", None)) is not None:
            plt.close(prev)
        fig = plt.figure(figsize=(11, 5.2))
        live["fig"] = fig
        axes = []
        for k, (p, cmap) in enumerate(zip(panes, ("viridis", "plasma"))):
            ax = fig.add_subplot(1, 2, k + 1, projection="3d")
            axes.append(ax)
            note = f" — {p['xy_note']}"
            if not len(p["z"]):  # empty here; label the pane and leave the axes bare
                ax.set_title(p["label"] + note, fontsize=9)
                ax.set_zlabel("elevation (m)")
                continue
            # Weight is ALREADY the colour, log-normed. Letting it drive opacity
            # linearly too erased the sparse returns: with a p98 in the hundreds
            # a single-count voxel landed on the 0.08 floor and vanished, and
            # single-count voxels are the interesting part of a lidar cloud --
            # canopy top, understory, anything off the ground return. Ramp on
            # log(weight) instead, over a narrow band with a high floor, so
            # density still reads as depth without hiding anything.
            if shade:
                # Ramp DOWN from the slider, not up to opaque: the slider stays the
                # ceiling for the densest voxels and weight pushes the sparse ones
                # toward invisible, which is the whole point of ticking the box.
                # Ticking it never makes anything more opaque than the flat setting.
                ref = max(np.percentile(p["wt"], 98), 1.0)
                lo = 0.05 * opacity
                alpha = np.clip(
                    lo + (opacity - lo) * np.log1p(p["wt"]) / np.log1p(ref), lo, opacity
                )
            else:
                alpha = opacity
            if by_time and p["t"] is not None:
                pts = ax.scatter(
                    p["x"],
                    p["y"],
                    p["z"],
                    c=p["t"],
                    s=1.5,
                    cmap="turbo",
                    norm=shared_time,
                    alpha=alpha,
                )
                fig.colorbar(pts, shrink=0.55, pad=0.10, label="days since 2018-01-01")
            elif by_time:
                ax.scatter(p["x"], p["y"], p["z"], color="#9498a0", s=1.5, alpha=opacity * 0.4)
                note += " — no temporal channel" + (" in binned tensors" if binned else "")
            elif by_elev:
                pts = ax.scatter(
                    p["x"],
                    p["y"],
                    p["z"],
                    c=p["z"],
                    s=1.5,
                    cmap="viridis",
                    norm=shared_elev,
                    alpha=alpha,
                )
                fig.colorbar(pts, shrink=0.55, pad=0.10, label="elevation (m)")
            else:
                pts = ax.scatter(
                    p["x"],
                    p["y"],
                    p["z"],
                    c=p["wt"],
                    s=1.5,
                    cmap=cmap,
                    norm=LogNorm(),
                    alpha=alpha,
                )
                fig.colorbar(pts, shrink=0.55, pad=0.10, label="weight")
            if zlim is not None:
                ax.set_zlim(*zlim)
            ax.set_title(p["label"] + note, fontsize=9)
            ax.set_zlabel("elevation (m)")
            ax.set_xticks(ticks, labels, fontsize=7)
            ax.set_yticks(ticks, labels, fontsize=7)
            # Face-local axes from the block's SOUTH corner: col runs toward the
            # north-EAST edge, row toward the north-WEST (zagg readers/_layout.py).
            # A HEALPix face is a diamond -- these are not compass east/north.
            ax.set_xlabel("→ NE edge", fontsize=8, labelpad=-2)
            ax.set_ylabel("→ NW edge", fontsize=8, labelpad=-2)

        # Linked rotation: only while DRAGGING, and only when the angles moved --
        # a bare hover must not trigger a redraw.
        def _sync(event):
            if event.button is None or event.inaxes not in axes:
                return
            src = event.inaxes
            for other in axes:
                if other is not src and (other.elev != src.elev or other.azim != src.azim):
                    other.view_init(elev=src.elev, azim=src.azim)
                    fig.canvas.draw_idle()

        fig.canvas.mpl_connect("motion_notify_event", _sync)
        fig.suptitle(
            f"shard {shard} — block {mz.morton_decimal(block)}"
            + ("" if binned else " — exact centroids"),
            fontsize=11,
        )
        plt.show()

    out = interactive_output(
        draw,
        {
            "block": dd,
            "by_elev": elev_cb,
            "by_time": time_cb,
            "binned": bin_cb,
            "zmode": zmode,
            "opacity": alpha_sl,
            "shade": shade_cb,
        },
    )
    # Three rows: an HBox does not wrap, and an overrun row squeezes the slider
    # into an undraggable stub.
    display(
        VBox(
            [
                HBox([dd, zmode]),
                HBox([elev_cb, time_cb, bin_cb]),
                HBox([alpha_sl, shade_cb]),
                out,
            ]
        )
    )
    return view


# --------------------------------------------------------------------------- #
# the waveform view
# --------------------------------------------------------------------------- #
def _mixture(digest, z, sigma):
    """Digest -> density on `z`: each centroid a Gaussian of width `sigma`."""
    mu, wt = digest[:, 0], digest[:, 1]
    pdf = (wt[None, :] * np.exp(-0.5 * ((z[:, None] - mu[None, :]) / sigma) ** 2)).sum(axis=1)
    return pdf / max(wt.sum(), 1e-9) / (sigma * np.sqrt(2 * np.pi))


def _block_digests(store, field, block, cell_order, block_order=BLOCK_ORDER):
    """Every stored digest in one block, keyed by its ``(row, col)`` in that block.

    ONE `read_ragged(subtree=...)` for the whole block instead of a `read_cell`
    per cell. Same bytes, one round trip: the per-cell path cost five round trips
    for every slider step (one GEDI cell plus its four ATL03 quarters), and a
    single `read_cell` measured a median of 8.9 s on a poor link -- 45 s a step,
    which is why the view read as frozen. Reading the block up front costs about
    what one step used to, and every step after it is a dict lookup.

    Keying on ``(row, col)`` also retires the chunk arithmetic the per-cell path
    needed: `read_cell` had to resolve an o19 cell to its o13 read chunk first,
    with a `chunk_side` that merely happened to equal GEDI's block side on this
    store pair. `read_ragged` hands back the cell word, and `block_rank` /
    `rank_to_rowcol` place it directly.
    """
    got = list(mz.read_ragged(store, field, subtree=mz.morton_decimal(int(block))))
    if not got:
        return {}
    words = np.array([w for w, _v in got], dtype=np.uint64)
    rows, cols = rank_to_rowcol(block_rank(words, block_order)[0], cell_order - block_order)
    return {(int(rows[i]), int(cols[i])): np.asarray(v) for i, (_w, v) in enumerate(got)}


def waveform_view(stores, fields, gains, pairs, shard):
    """Interactive coincident-waveform view.

    ``stores``/``fields`` are ``{sensor: ...}``; ``gains`` and ``pairs`` are what
    :func:`paired_blocks` returns.

    Digests are read a BLOCK at a time and cached, so moving the slider does no
    I/O at all -- see :func:`_block_digests`.
    """
    from zagg.stats.tdigest import cdf_from_tdigest  # moczarr[zagg]; see module docstring

    orders = {n: int(f.split("/", 1)[0]) for n, f in fields.items()}

    live: dict = {}  # the one figure this view keeps open

    # Cached per BLOCK, not per cell: only changing the block reads anything, so
    # `nth` and `binw` are pure rendering. Two blocks kept -- one is a few MB.
    cache: dict = {}

    def _block(w):
        if w not in cache:
            if len(cache) > 1:
                cache.clear()
            cache[w] = {
                n: _block_digests(stores[n], fields[n], w, orders[n]) for n in ("gedi", "atl03")
            }
        return cache[w]

    def _digests(pair, r, c, w):
        """One GEDI cell and the 2x2 ATL03 cells under it, from the block cache."""
        cells = _block(w)
        gdigest = cells["gedi"].get((r, c), np.empty((0, 2)))
        quarters = [cells["atl03"].get((2 * r + dr, 2 * c + dc)) for dr in (0, 1) for dc in (0, 1)]
        kept = [q for q in quarters if q is not None and len(q)]
        if not kept:
            return gdigest, np.empty((0, 2))
        # SORT BY MEAN. Each quarter is sorted on its own, so concatenating gives
        # four ascending runs, not one -- and `cdf_from_tdigest` reads the centroid
        # means as `np.interp`'s x-coordinates, which numpy documents as nonsense
        # unless they increase. Measured on a representative 2x2 the CDF drifted by
        # 32% of the cell's weight while still looking monotonic, so it fails
        # silently. The mixture is a weighted sum and does not care, which is why
        # only the CDF was wrong.
        merged = np.concatenate(kept)
        return gdigest, merged[np.argsort(merged[:, 0], kind="stable")]

    def paired_waveform(pair=0, nth=0, binw=1.0):
        w, joint, a2, g2 = pairs[pair]  # a2/g2: the notebook's A2/G2, lowercased for N806
        _aoff, ag = gains["atl03"][w]
        _goff, gg = gains["gedi"][w]
        # Rank the joint cells by the weaker side. a2 is ATL03 PHOTON counts and
        # g2 GEDI PHOTOELECTRONS -- incommensurate units that run two to three
        # orders of magnitude apart on this store pair, so a raw `np.minimum`
        # would collapse to a2 on essentially every cell and "the weaker member"
        # would really mean "the ATL03 count". Scale each side by its own
        # maximum over the JOINT cells first, so the min picks out whichever
        # sensor is relatively weaker. (What makes a pick *coincident* at all is
        # the `joint` mask itself -- both sensors populated -- not this min.)
        a = a2 / max(int(a2[joint].max()), 1)
        g = g2 / max(int(g2[joint].max()), 1)
        rank = np.minimum(a, g) * joint
        order = np.argsort(rank.ravel())[::-1]
        r, c = np.unravel_index(int(order[min(nth, int(joint.sum()) - 1)]), rank.shape)

        gdigest, adigest = _digests(pair, int(r), int(c), w)

        # The ATL03 side can legitimately come back empty: `joint` is built from
        # the TENSORS (a finite z window, possibly degraded) while this reads the
        # RAW digests, and the two are not guaranteed to agree cell for cell.
        # Draw the GEDI side alone and say so, rather than letting an empty
        # `.min()` turn "no photons here" into a traceback inside the widget.
        zmu = [gdigest[:, 0]] + ([adigest[:, 0]] if len(adigest) else [])
        lo = min(a.min() for a in zmu) - 5
        hi = max(a.max() for a in zmu) + 5
        z = np.linspace(lo, hi, 700)
        amu, awt = adigest[:, 0], adigest[:, 1]

        # Same figure-registry bound as `view3d.draw`: this is an
        # `interactive_output` callback too, so close the previous figure
        # rather than accumulating one per widget change.
        if (prev := live.pop("fig", None)) is not None:
            plt.close(prev)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.6), sharey=True)
        live["fig"] = fig
        ax1.plot(
            _mixture(gdigest, z, gg),
            z,
            color="#7b3294",
            lw=2,
            label=f"GEDI flux ({gdigest[:, 1].sum():.0f} pe)",
        )
        if len(adigest):
            ax1.plot(
                _mixture(adigest, z, ag),
                z,
                color="#008837",
                lw=2,
                label=f"ATL03 signal ({awt.sum():.0f} ph)",
            )
        ax1.set_xlabel("normalized density")
        ax1.set_ylabel("elevation (m)")

        # Top axis: the RAW ATL03 photons -- binned bars, or one dot per centroid
        # when the width is 0 (cells under the store's delta budget are loss-free,
        # so centroids ~ photons there; a merged centroid's weight sets its size).
        ax1t = ax1.twiny()
        if binw and binw > 0:
            edges = np.arange(lo, hi + binw, binw)
            counts, _ = np.histogram(amu, bins=edges, weights=awt)
            ax1t.barh(
                edges[:-1] + binw / 2,
                counts,
                height=binw * 0.9,
                color="#008837",
                alpha=0.25,
                zorder=0,
            )
            ax1t.set_xlabel(f"ATL03 photons / {binw:g} m bin", fontsize=9)
        else:
            ax1t.scatter(
                np.zeros(len(amu)),
                amu,
                s=np.clip(awt * 8, 8, 40),
                color="#008837",
                alpha=0.45,
                zorder=0,
            )
            ax1t.set_xlim(-0.05, 1.0)
            ax1t.set_xlabel("ATL03 photons (unbinned)", fontsize=9)
        ax1.set_zorder(ax1t.get_zorder() + 1)
        ax1.patch.set_visible(False)
        ax1.set_title(
            f"cell ({r},{c}) @o18 — GEDI {len(gdigest)} centroids "
            + (
                f"vs ATL03 {len(adigest)} (2×2 @o19)"
                if len(adigest)
                else "— no ATL03 digest under this cell"
            ),
            fontsize=9,
        )
        ax1.legend(fontsize=8)

        # cdf_from_tdigest returns CUMULATIVE WEIGHT (pe for GEDI, photons for
        # ATL03) -- normalize each by its own total so both share the axis honestly.
        ax2.plot(
            cdf_from_tdigest(gdigest, z) / max(gdigest[:, 1].sum(), 1e-9), z, color="#7b3294", lw=2
        )
        if len(adigest):
            ax2.plot(
                cdf_from_tdigest(adigest, z) / max(adigest[:, 1].sum(), 1e-9),
                z,
                color="#008837",
                lw=2,
            )
        ax2.set_xlim(0, 1)
        ax2.set_xlabel("CDF (probability)")
        ax2.set_title("cumulative", fontsize=10)
        for ax in (ax1, ax2):
            ax.spines[["top", "right"]].set_visible(False)
            ax.grid(alpha=0.25, lw=0.5)
        fig.suptitle(f"shard {shard} — block {mz.morton_decimal(w)}", fontsize=11)
        plt.tight_layout()
        plt.show()

    dd = Dropdown(
        options=[
            (f"{mz.morton_decimal(w)}  ({int(j.sum()):,} joint cells)", i)
            for i, (w, j, _, _) in enumerate(pairs)
        ],
        value=0,
        description="block",
        layout=Layout(width="360px"),
    )
    # Opens on the 7th coincident cell (0-indexed): on the SERC block this
    # notebook ships with, that one shows the canopy/ground structure clearly.
    # Any other AOI will rank differently -- it is a nice default, not a rule.
    #
    # `continuous_update` left at its default (True), unlike the opacity slider:
    # since `_block_digests` caches the whole block, moving this reads NOTHING --
    # a dict lookup and two 2-D line plots -- so there is nothing to suppress.
    # (It used to cost five S3 round trips a step, ~45 s on a poor link, which is
    # what made the control look dead.)
    #
    # `max` is re-set per block below. A fixed 0..40 was wrong: 30 of the 64 blocks
    # on this shard hold fewer than 41 coincident cells, and `paired_waveform`
    # clamps with `min(nth, joint.sum() - 1)`, so every position past the count
    # selected the SAME cell and the view stopped changing.
    nth = IntSlider(min=0, max=40, value=6, description="nth joint", layout=Layout(width="330px"))
    binw = FloatText(
        value=1.0,
        step=0.5,
        description="histogram bin size, ATL03 z (m)",
        style={"description_width": "initial"},
        layout=Layout(width="330px"),
    )

    # Two rows, every widget sized -- see the HBox note in `view3d`.
    def _fit_nth(_change=None):
        """Bound `nth` to the cells this block actually has."""
        nth.max = max(int(pairs[dd.value][1].sum()) - 1, 0)

    dd.observe(_fit_nth, names="value")
    _fit_nth()
    display(
        VBox(
            [
                HBox([dd, nth]),
                binw,
                interactive_output(paired_waveform, {"pair": dd, "nth": nth, "binw": binw}),
            ]
        )
    )
