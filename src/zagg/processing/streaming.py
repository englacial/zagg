"""Streaming buffered aggregation for mergeable reducers (issue #148, phase 4).

The pooled worker path holds every granule's filtered reads until the shard is
fully read (``_concat_and_group`` in ``worker.py``), then digests once. At 88S
scale (a 5,620-granule stress shard, tens of millions of photons) that pool
alone blows the 2 GB Lambda cap, so ``aggregation.streaming`` trades one-shot
pooling for **bounded buffers + incremental merges**: reads accumulate for
``buffer_granules`` granules, are grouped and reduced into per-cell running
state, and released — peak memory is one buffer plus the running digest state,
independent of the shard's granule count.

Only *mergeable* reducers can stream, so the mode is deliberately narrow and
validated up front (:func:`validate_streaming`):

- dense scalar fields with ``function: len`` — merged by summation (exact,
  byte-identical to pooled);
- ragged fields with ``function: zagg.stats.tdigest.build_tdigest`` — merged by
  :func:`zagg.stats.tdigest.merge_tdigests` under the field's own ``delta``
  (approximate within t-digest guarantees; **exactly** the pooled result when
  the shard fits in a single buffer, since one flush == one pooled build).

Everything else (expressions, vector fields, ``resolution: chunk`` companions,
``chunk_precompute``) has no incremental-merge story and raises. A fixed-size
buffer is used rather than pure granule-by-granule updates because each flush
costs one merge round over the touched cells (~10 µs/cell, see
``zagg/stats/tdigest.py``): near 88S a tangent-running granule touches most of
the shard's cells, so N/B rounds instead of N keeps merge CPU in seconds.
"""

from __future__ import annotations

import numpy as np

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_chunk_precompute,
    get_output_signature,
)
from zagg.stats.tdigest import _DEFAULT_DELTA, build_tdigest, merge_tdigests

#: The one ragged reducer with a merge law wired up.
_TDIGEST_FUNCTION = "zagg.stats.tdigest.build_tdigest"


#: Safe per-cell slot bound for staged tdigest builds/merges, as a multiple of
#: the field's delta. On heavy-tailed adversarial inputs (Cauchy/Pareto) the
#: k-1 compression law peaks near ~1.55*delta centroids (measured: 388 at
#: delta=256, 776 at delta=512, 1544 at delta=1024) — well above the ~1.13*delta
#: seen on well-behaved data — so keep the factor at 2: 2*delta clears that
#: worst case and never truncates; asserted at every staged write. Do not
#: tighten below the ~1.55*delta adversarial bound.
_K_SLOT_FACTOR = 2

#: Shared zero-size arena so releasing a field's old buffer needs no allocation.
_EMPTY_ARENA = np.empty((0, 2), dtype=np.float32)


def _ranges_to_indices(starts: np.ndarray, lengths: np.ndarray) -> np.ndarray:
    """Element indices for concatenated ``[start, start+length)`` ranges.

    Vectorized (no per-range Python loop): for the arena rebuild it turns the
    kept cells' old/new offset ranges into flat gather/scatter indices.
    """
    total = int(lengths.sum())
    if total == 0:
        return np.empty(0, dtype=np.int64)
    shifts = np.concatenate(([0], np.cumsum(lengths)[:-1]))
    return np.arange(total, dtype=np.int64) + np.repeat(starts - shifts, lengths)


def get_streaming(config: PipelineConfig) -> dict | None:
    """Return the ``aggregation.streaming`` block, or ``None`` (pooled path).

    The block is ``{"buffer_granules": int, "state_layout": "dict"|"arena",
    "arena_backing": "memory"|"tmp"}``; ``buffer_granules`` must be a positive
    int. ``state_layout`` (issue #217) picks the running-state container:
    ``"dict"`` (default, per-cell ndarrays) or ``"arena"`` (contiguous CSR
    buffers — same merge sequence, same bytes out, ~24 B/cell overhead instead
    of ~290). ``arena_backing: tmp`` (arena only) puts the centroid buffers in
    unlinked ``/tmp``-backed memmaps so flush transients page under memory
    pressure instead of OOM-killing the worker. Absent block -> ``None`` so
    existing configs are untouched.
    """
    block = config.aggregation.get("streaming")
    if block is None:
        return None
    if not isinstance(block, dict):
        raise ValueError("aggregation.streaming must be a mapping, e.g. {buffer_granules: 50}")
    buffer_granules = block.get("buffer_granules", 50)
    if not isinstance(buffer_granules, int) or buffer_granules < 1:
        raise ValueError(
            f"aggregation.streaming.buffer_granules must be a positive int "
            f"(got {buffer_granules!r})"
        )
    state_layout = block.get("state_layout", "dict")
    if state_layout not in ("dict", "arena"):
        raise ValueError(
            f"aggregation.streaming.state_layout must be 'dict' or 'arena' (got {state_layout!r})"
        )
    arena_backing = block.get("arena_backing", "memory")
    if arena_backing not in ("memory", "tmp"):
        raise ValueError(
            f"aggregation.streaming.arena_backing must be 'memory' or 'tmp' (got {arena_backing!r})"
        )
    if arena_backing == "tmp" and state_layout != "arena":
        raise ValueError(
            "aggregation.streaming.arena_backing: tmp requires state_layout: arena "
            "(the dict layout has no contiguous buffers to back with a file)"
        )
    return {
        "buffer_granules": buffer_granules,
        "state_layout": state_layout,
        "arena_backing": arena_backing,
    }


def validate_streaming(config: PipelineConfig) -> None:
    """Reject configs whose reducers have no incremental-merge law.

    Raises ``ValueError`` naming every offending field so a config error reads
    as one message, not a peel-the-onion loop.
    """
    problems: list[str] = []
    if get_chunk_precompute(config):
        problems.append("chunk_precompute is chunk-scoped and cannot stream")
    for name, meta in get_agg_fields(config).items():
        sig = get_output_signature(meta)
        if "expression" in meta:
            problems.append(f"field '{name}': expression fields cannot stream")
        elif sig["resolution"] != "cell":
            problems.append(f"field '{name}': resolution '{sig['resolution']}' cannot stream")
        elif sig["kind"] == "ragged":
            if sig["location"] is not None:
                # The located channel (issue #87) has a merge law (located
                # merge_tdigests), but the streaming state does not thread
                # per-cell locations yet — reject rather than silently dropping
                # the channel from the store.
                problems.append(
                    f"field '{name}': located ragged fields (location: "
                    f"{sig['location']!r}) cannot stream yet"
                )
            elif meta.get("function") != _TDIGEST_FUNCTION:
                problems.append(
                    f"field '{name}': ragged function {meta.get('function')!r} has no "
                    f"merge law (only {_TDIGEST_FUNCTION})"
                )
            elif tuple(sig["inner_shape"]) != (2,):
                # The pooled path validates payload shape per cell via
                # _coerce_ragged_value; the buffered path stores merged digests
                # directly, so a mis-declared inner_shape must fail HERE, not
                # silently disagree with the store schema readers key on.
                problems.append(
                    f"field '{name}': tdigest payloads are (k, 2) centroids; "
                    f"declared inner_shape {list(sig['inner_shape'])} cannot stream"
                )
        elif sig["kind"] == "scalar":
            # ``count`` is the pooled path's alias of ``len`` (aggregate.py);
            # both merge by summation.
            if meta.get("function") not in ("len", "count"):
                problems.append(
                    f"field '{name}': scalar function {meta.get('function')!r} is not "
                    "mergeable (only 'len'/'count')"
                )
        else:
            problems.append(f"field '{name}': kind '{sig['kind']}' cannot stream")
    if problems:
        raise ValueError(
            "aggregation.streaming is on but the config is not streamable: " + "; ".join(problems)
        )


class StreamingAggregator:
    """Bounded-buffer worker state: accumulate B granules, merge, release.

    Drives the same ``_concat_and_group`` grouping as the pooled path per
    flush, then folds each populated cell into running per-cell state:
    ``len`` fields by summation, tdigest fields via ``merge_tdigests``.
    ``chunk_outputs`` then emits the exact ``(stats_arrays, ragged_payloads,
    ragged_cell_indices, cells_with_data)`` shape ``_aggregate_chunk_cells``
    returns, so the worker's carrier/ragged construction is shared verbatim.
    """

    def __init__(
        self,
        config: PipelineConfig,
        grid,
        handoff: str,
        buffer_granules: int,
        state_layout: str = "dict",
        arena_backing: str = "memory",
    ):
        validate_streaming(config)
        self.grid = grid
        self.handoff = handoff
        self.buffer_granules = buffer_granules
        self.state_layout = state_layout
        self.arena_backing = arena_backing
        agg_fields = get_agg_fields(config)
        self._count_fields: list[str] = []
        self._digest_fields: dict[str, tuple[str, int]] = {}  # name -> (source, delta)
        for name, meta in agg_fields.items():
            if get_output_signature(meta)["kind"] == "ragged":
                delta = int((meta.get("params") or {}).get("delta", _DEFAULT_DELTA))
                # Mirror the pooled path's source default (aggregate.py:
                # ``meta.get("source") or value_col`` with the worker's
                # ``value_col="h_li"``) so a config that runs pooled doesn't die
                # here with a bare KeyError when streaming is turned on.
                self._digest_fields[name] = (meta.get("source") or "h_li", delta)
            else:
                self._count_fields.append(name)
        self.counts: dict[int, int] = {}
        self.digests: dict[str, dict[int, np.ndarray]] = {n: {} for n in self._digest_fields}
        # Arena layout (issue #217): the same running state held as contiguous
        # CSR buffers — sorted cell ids + parallel counts, and per digest field
        # one packed centroid buffer addressed by offsets. Identical merge
        # sequence to the dict layout (same build/merge calls in the same
        # order), so the emitted bytes match; only the container differs
        # (~24 B/cell instead of ~290 B of dict-slot + ndarray-header overhead).
        self._cells = np.empty(0, dtype=np.uint64)
        self._cell_counts = np.empty(0, dtype=np.int64)
        self._offsets: dict[str, np.ndarray] = {
            n: np.zeros(1, dtype=np.int64) for n in self._digest_fields
        }
        self._arenas: dict[str, np.ndarray] = {
            n: np.empty((0, 2), dtype=np.float32) for n in self._digest_fields
        }
        # Set to the field name while its arena is released for compaction and
        # cleared once compaction lands; a lingering value means a compaction
        # failed mid-rebuild and the running state is unusable (see update()).
        self._poisoned: str | None = None
        self.n_obs_total = 0
        self.flushes = 0
        self._buffer: list = []
        self._buffered_granules = 0

    def _alloc_arena(self, n_rows: int) -> np.ndarray:
        """Allocate one centroid buffer: RAM, or a ``/tmp``-backed memmap.

        Under ``arena_backing: tmp`` (issue #217, the spill/arena hybrid) the
        buffer is an **anonymous** file mapping: the file is unlinked the
        moment the mapping exists, so the mapping itself is the only
        reference — space frees on GC and nothing can accumulate in ``/tmp``
        across warm Lambda invokes. The payoff is failure mode: dirty pages
        are file-backed page cache the kernel can write back and reclaim
        under cgroup pressure, so an oversized flush transient degrades into
        paging instead of an OOM kill of anonymous heap.
        """
        if self.arena_backing != "tmp" or n_rows == 0:
            return np.empty((n_rows, 2), dtype=np.float32)
        import os
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".arena")
        try:
            mapped = np.memmap(path, dtype=np.float32, mode="w+", shape=(n_rows, 2))
        finally:
            os.close(fd)
            os.unlink(path)
        return mapped

    def add_read(self, chunk) -> None:
        """Buffer one group read (the carrier ``_read_group`` returned)."""
        self._buffer.append(chunk)

    def granule_done(self) -> None:
        """Mark one granule fully read; flush when the buffer is full."""
        self._buffered_granules += 1
        if self._buffered_granules >= self.buffer_granules:
            self.flush()

    def flush(self) -> None:
        """Group the buffered reads and merge them into the running state."""
        if not self._buffer:
            self._buffered_granules = 0
            return
        from zagg.processing.aggregate import _concat_and_group

        col_arrays, cell_to_slice, n_obs = _concat_and_group(self._buffer, self.grid, self.handoff)
        self.n_obs_total += n_obs
        if self.state_layout == "arena":
            self._fold_arena(col_arrays, cell_to_slice)
        else:
            for cell, (start, end) in cell_to_slice.items():
                self.counts[cell] = self.counts.get(cell, 0) + (end - start)
                for name, (source, delta) in self._digest_fields.items():
                    fresh = build_tdigest(col_arrays[source][start:end], delta=delta)
                    held = self.digests[name].get(cell)
                    self.digests[name][cell] = (
                        fresh if held is None else merge_tdigests(held, fresh, delta=delta)
                    )
        self.flushes += 1
        self._buffer = []
        self._buffered_granules = 0

    def _fold_arena(self, col_arrays, cell_to_slice) -> None:
        """Rebuild the CSR state with this flush's cells folded in — staged.

        Every intermediate is a bulk array, never a per-cell object list:
        fresh digests are built straight into a bounded staging arena (slot
        bound ``min(n, 2*delta)`` — group sizes are known before any build),
        old-only and fresh-only cells move by vectorized element gathers, and
        only genuine merges loop. A final one-gather compaction squeezes the
        merge-compression slack back out. Flush transient is therefore old +
        staging + bounded-new (all payload-sized, freed in stages) — the naive
        rebuild's list of ~1M small fresh-digest ndarrays measurably cost more
        than the state itself (issue #217 A/B replay).
        """
        fresh_cells = np.fromiter(cell_to_slice, dtype=np.uint64, count=len(cell_to_slice))
        order = np.argsort(fresh_cells)
        fresh_cells = fresh_cells[order]
        slices = list(cell_to_slice.values())
        starts = np.array([slices[i][0] for i in order], dtype=np.int64)
        ends = np.array([slices[i][1] for i in order], dtype=np.int64)

        union = np.union1d(self._cells, fresh_cells)
        pos_old = np.searchsorted(union, self._cells)
        pos_fresh = np.searchsorted(union, fresh_cells)
        overlap = np.isin(fresh_cells, self._cells, assume_unique=True)

        new_counts = np.zeros(len(union), dtype=np.int64)
        new_counts[pos_old] = self._cell_counts
        np.add.at(new_counts, pos_fresh, ends - starts)

        keep = ~np.isin(self._cells, fresh_cells, assume_unique=True)
        ovl_idx = np.nonzero(overlap)[0]
        held_pos = np.searchsorted(self._cells, fresh_cells[overlap])

        for name, (source, delta) in self._digest_fields.items():
            offsets, arena = self._offsets[name], self._arenas[name]
            k_old = np.diff(offsets)
            cap = _K_SLOT_FACTOR * delta

            # Stage fresh digests straight into a bounded arena — group sizes
            # are known before any build (k <= min(n, cap)), so no per-cell
            # object list ever exists (a 1M-cell flush as a list of small
            # ndarrays transiently costs more than the state itself).
            bound_fresh = np.minimum(ends - starts, cap)
            stage_off = np.concatenate(([0], np.cumsum(bound_fresh)))
            stage = self._alloc_arena(int(stage_off[-1]))
            k_fresh = np.empty(len(fresh_cells), dtype=np.int64)
            for j in range(len(fresh_cells)):
                d = build_tdigest(col_arrays[source][starts[j] : ends[j]], delta=delta)
                assert len(d) <= bound_fresh[j], (
                    f"tdigest returned {len(d)} centroids > slot bound {bound_fresh[j]} "
                    f"(delta={delta}) — the {_K_SLOT_FACTOR}*delta compression bound broke"
                )
                stage[stage_off[j] : stage_off[j] + len(d)] = d
                k_fresh[j] = len(d)

            # Bounded merged layout: exact slots for old-only / fresh-only
            # cells, min(k_old + k_fresh, cap) for merges.
            k_bound = np.zeros(len(union), dtype=np.int64)
            k_bound[pos_old] = k_old
            k_bound[pos_fresh] = k_fresh
            k_bound[pos_fresh[ovl_idx]] = np.minimum(k_old[held_pos] + k_fresh[ovl_idx], cap)
            bound_off = np.concatenate(([0], np.cumsum(k_bound)))
            bound_arena = self._alloc_arena(int(bound_off[-1]))
            k_exact = k_bound.copy()

            src = _ranges_to_indices(offsets[:-1][keep], k_old[keep])
            dst = _ranges_to_indices(bound_off[:-1][pos_old[keep]], k_old[keep])
            bound_arena[dst] = arena[src]
            solo = ~overlap
            src = _ranges_to_indices(stage_off[:-1][solo], k_fresh[solo])
            dst = _ranges_to_indices(bound_off[:-1][pos_fresh[solo]], k_fresh[solo])
            bound_arena[dst] = stage[src]
            for j, i_old in zip(ovl_idx, held_pos, strict=True):
                held = arena[offsets[i_old] : offsets[i_old + 1]]
                fresh = stage[stage_off[j] : stage_off[j] + k_fresh[j]]
                d = merge_tdigests(held, fresh, delta=delta)
                u = pos_fresh[j]
                assert len(d) <= k_bound[u], (
                    f"merged tdigest {len(d)} centroids > slot bound {k_bound[u]} "
                    f"(delta={delta}) — the {_K_SLOT_FACTOR}*delta compression bound broke"
                )
                bound_arena[bound_off[u] : bound_off[u] + len(d)] = d
                k_exact[u] = len(d)
            # Release the old arena and staging before compaction so the
            # compaction peak is bound + exact, not old + stage + bound + exact.
            # This leaves the field transiently inconsistent (arena emptied,
            # offsets still describing the pre-merge layout); poison it so a
            # failed gather below (e.g. OOM) can't be caught and then read as a
            # silent no-digest result. Cleared once compaction lands atomically.
            self._arenas[name] = _EMPTY_ARENA
            del arena, stage
            self._poisoned = name

            # Compact the bounded layout to exact CSR in one vectorized gather
            # (merge compression leaves slack only inside merged slots). The
            # gather lands directly in a fresh factory buffer (``out=``) so
            # the compacted state inherits the configured backing instead of
            # fancy-indexing into an anonymous RAM copy.
            new_offsets = np.concatenate(([0], np.cumsum(k_exact)))
            src = _ranges_to_indices(bound_off[:-1], k_exact)
            new_arena = self._alloc_arena(int(new_offsets[-1]))
            np.take(bound_arena, src, axis=0, out=new_arena)
            self._offsets[name], self._arenas[name] = new_offsets, new_arena
            self._poisoned = None

        self._cells, self._cell_counts = union, new_counts

    @property
    def empty(self) -> bool:
        """True when no observation ever survived filtering (mirror of no reads)."""
        if self.state_layout == "arena":
            return self._cells.size == 0 and not self._buffer
        return not self.counts and not self._buffer

    def occupied_cells(self) -> np.ndarray:
        """Populated cell words as a ``uint64`` array (issue #200 coverage sink).

        Layout-agnostic: the dict layout keys ``self.counts``, the arena layout
        holds the sorted ids in ``self._cells``. Read after the final flush, so
        both mirror the merged running state.
        """
        if self.state_layout == "arena":
            return self._cells
        return np.fromiter(self.counts.keys(), dtype=np.uint64, count=len(self.counts))

    def chunk_outputs(self, children, agg_fields: dict):
        """Emit one chunk's aggregation outputs from the running state.

        Same return contract as ``_aggregate_chunk_cells``: dense arrays are
        preallocated to the field's dtype/fill and filled per populated cell;
        ragged payloads are collected with their chunk-local cell indices.
        """
        children = np.asarray(children)
        n_cells = len(children)
        stats_arrays: dict[str, np.ndarray] = {}
        for name in self._count_fields:
            meta = agg_fields[name]
            dtype = np.dtype(meta.get("dtype", "float32"))
            if meta.get("fill_value", "NaN") == "NaN":
                stats_arrays[name] = np.full(n_cells, np.nan, dtype=dtype)
            else:
                stats_arrays[name] = np.zeros(n_cells, dtype=dtype)
        ragged_payloads: dict[str, list] = {n: [] for n in self._digest_fields}
        ragged_cell_indices: dict[str, list[int]] = {n: [] for n in self._digest_fields}

        if self.state_layout == "arena":
            return self._chunk_outputs_arena(
                children, stats_arrays, ragged_payloads, ragged_cell_indices
            )
        cells_with_data = 0
        for i, child in enumerate(children):
            cell = int(child)
            count = self.counts.get(cell)
            if count is None:
                # Empty cell: dense fields keep their fill; ``len`` over an empty
                # slice is 0 on the pooled path, so mirror it explicitly.
                for name in self._count_fields:
                    stats_arrays[name][i] = 0
                continue
            cells_with_data += 1
            for name in self._count_fields:
                stats_arrays[name][i] = count
            for name in self._digest_fields:
                digest = self.digests[name].get(cell)
                if digest is not None and digest.size > 0:
                    ragged_payloads[name].append(digest)
                    ragged_cell_indices[name].append(i)
        return stats_arrays, ragged_payloads, ragged_cell_indices, cells_with_data

    def _chunk_outputs_arena(self, children, stats_arrays, ragged_payloads, ragged_cell_indices):
        """Arena-layout ``chunk_outputs``: vectorized lookup instead of dict gets.

        Same emitted values as the dict branch — occupied cells carry their
        count, empty cells 0 (the pooled ``len`` over an empty slice), digests
        only where nonempty — via one ``searchsorted`` over the sorted cell ids.
        """
        if self._poisoned is not None:
            raise RuntimeError(
                f"streaming digest state for {self._poisoned!r} was left inconsistent "
                "by a failed compaction; the running state is unusable"
            )
        cells = children.astype(np.uint64)
        pos = np.searchsorted(self._cells, cells)
        pos_c = np.minimum(pos, max(self._cells.size - 1, 0))
        occupied = (self._cells[pos_c] == cells) if self._cells.size else np.zeros(len(cells), bool)
        counts = np.where(occupied, self._cell_counts[pos_c], 0)
        for name in self._count_fields:
            stats_arrays[name][:] = counts.astype(stats_arrays[name].dtype)
        for name in self._digest_fields:
            offsets, arena = self._offsets[name], self._arenas[name]
            for i in np.nonzero(occupied)[0]:
                lo, hi = offsets[pos[i]], offsets[pos[i] + 1]
                if hi > lo:
                    ragged_payloads[name].append(arena[lo:hi])
                    ragged_cell_indices[name].append(int(i))
        return stats_arrays, ragged_payloads, ragged_cell_indices, int(occupied.sum())
