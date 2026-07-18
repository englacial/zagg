"""Spill-partition aggregation for the streaming worker (issue #217).

Option (A) from the #217 plan: instead of folding each read buffer into
running merge state (``streaming.py``, the ~500 s merge-CPU term that kills
the heavy o8 shards), a flush **appends the buffer's grouped columns to
per-partition packed files in ``/tmp``** and aggregation happens once, after
the reads, from complete per-cell data — reproducing the pooled path's
results byte-for-byte in the single-block regime.

This module is the phase-1 surface: the block/partition writer + reader and
the standalone ``/tmp`` capacity guard. Key mechanics:

- One append file per (block, partition). The file is created with
  ``tempfile.mkstemp`` and **unlinked immediately**; the open file object is
  the only reference, so space frees when the partition is closed (or the
  process dies) and nothing can leak across warm Lambda invokes — there is no
  teardown pathway to miss. Fd count is K per block (K = the grid's inner
  chunks per shard, ≤ ~64), well under limits.
- Records are packed columnar segments: per append, the cell words
  (``uint64``) followed by each declared column's values in schema order,
  raw bytes, no framing — segment row counts live in memory on the writer
  (same process reads them back). Read-back is ``seek(0)`` + ``readinto``
  straight into preallocated arrays: exact bytes in, exact bytes out.
- The partition key is the observation's inner-chunk id (``clip2order`` at
  ``grid.chunk_order`` — :func:`partition_ids`); with ``chunk_inner`` unset
  (K == 1) everything lands in a single partition.
- Byte accounting is exact on write (``bytes_written`` sums each segment's
  ``nbytes``): it is both the block-threshold input and the ``spill_bytes``
  metric (the espg-approved /tmp throughput instrumentation).

``check_tmp_headroom`` is a **standalone** statvfs guard (deliberately not
the #260 arena guard, which is deleted with the arena paths): sizing ``/tmp``
below the spill working set would otherwise surface as ENOSPC mid-append, so
spill-enable checks free space up front and fails with a config-style error
naming the ``-disk`` function-variant fix.
"""

from __future__ import annotations

import os
import tempfile
import time

import numpy as np

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_data_vars,
    get_output_signature,
)
from zagg.stats.tdigest import _DEFAULT_DELTA, build_tdigest, merge_tdigests

#: Floor for the spill-enable /tmp check: below this, even a degraded
#: many-block run is pointless — fail at config time instead of thrashing.
_MIN_SPILL_BYTES = 64 * 2**20


class SpillOverflowError(RuntimeError):
    """A spill block hit its threshold under a config with no merge law.

    Raised the moment a second block would open (never on single-block
    shards, where every reducer is exact); the message names the remedies.
    Deliberately a distinct type so the worker's tolerated per-granule
    ``except`` can re-raise it instead of warn-and-continue.
    """


def check_tmp_headroom(need_bytes: int, tmp_dir: str | None = None) -> None:
    """Refuse to enable spill when ``/tmp`` cannot hold its working set.

    Standalone spill guard (issue #217 plan: written independently of the
    #260 arena SIGBUS guard so it survives the arena removal). Raises a loud
    config-style ``RuntimeError`` naming the deployment fix when the spill
    directory's free space is below ``need_bytes`` — typically the block
    threshold, the most a single spill block is allowed to grow.
    """
    tmp_dir = tmp_dir or tempfile.gettempdir()
    st = os.statvfs(tmp_dir)
    avail = st.f_bavail * st.f_frsize
    if avail < need_bytes:
        raise RuntimeError(
            f"aggregation.streaming.mode: spill needs {need_bytes:,} bytes of free "
            f"space in {tmp_dir!r} but only {avail:,} are available; deploy on a "
            f"function variant with larger ephemeral storage (the '-disk' variants, "
            f"e.g. process-shard-4096-disk) or fall back to mode: merge."
        )


def partition_ids(grid, cells: np.ndarray) -> np.ndarray:
    """Spill partition key per cell: the enclosing inner-chunk id.

    HEALPix grids with a finer ``chunk_inner`` (K > 1) coarsen each
    child-order cell word to ``grid.chunk_order`` via ``mortie.clip2order`` —
    the same words ``grid.iter_chunks`` enumerates, so a chunk's partition is
    found by clipping any of its children. Every other case (``chunk_inner``
    unset, rectilinear, minimal test stubs) is a single partition: key 0.
    """
    cells = np.asarray(cells)
    if int(getattr(grid, "chunks_per_shard", 1)) <= 1 or not hasattr(grid, "chunk_order"):
        return np.zeros(len(cells), dtype=np.uint64)
    from mortie import clip2order

    return np.asarray(clip2order(grid.chunk_order, cells.astype(np.uint64)))


def _readinto(f, arr: np.ndarray) -> None:
    """Fill a 1-D contiguous array from ``f``'s current position, exactly."""
    view = memoryview(arr).cast("B")
    got = 0
    while got < len(view):
        n = f.readinto(view[got:])
        if not n:
            raise OSError(f"short read from spill file: expected {len(view)} bytes, got {got}")
        got += n


class _Partition:
    """One partition's unlinked append file plus its in-memory segment map."""

    __slots__ = ("file", "segments", "nbytes")

    def __init__(self, tmp_dir: str):
        fd, path = tempfile.mkstemp(prefix="zagg-spill-", dir=tmp_dir)
        try:
            self.file = os.fdopen(fd, "w+b")
        except BaseException:
            os.close(fd)
            raise
        finally:
            # Unlink at birth: the open file object is the only reference, so
            # the space frees on close/GC and no warm-invoke cleanup exists to
            # forget. (If fdopen raised, the fd was closed above.)
            os.unlink(path)
        self.segments: list[int] = []
        self.nbytes = 0

    def write_segment(self, arrays) -> int:
        """Append one segment (cells + columns, raw bytes); return bytes written."""
        n_rows = len(arrays[0])
        written = 0
        for arr in arrays:
            a = np.ascontiguousarray(arr)
            self.file.write(memoryview(a).cast("B"))
            written += a.nbytes
        self.segments.append(n_rows)
        self.nbytes += written
        return written

    def read(self, cell_dtype, schema) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        """Read every segment back into fresh arrays (cells, {name: values})."""
        f = self.file
        f.flush()
        f.seek(0)
        total = sum(self.segments)
        cells = np.empty(total, dtype=cell_dtype)
        cols = {name: np.empty(total, dtype=dtype) for name, dtype in schema}
        off = 0
        for n in self.segments:
            _readinto(f, cells[off : off + n])
            for name, _ in schema:
                _readinto(f, cols[name][off : off + n])
            off += n
        f.seek(0, os.SEEK_END)
        return cells, cols

    def close(self) -> None:
        self.file.close()


class SpillBlock:
    """One block of K spill partitions: packed columnar appends, exact bytes.

    The writer half of the spill design: a flush routes its grouped rows to
    partitions by ``part_ids`` (contiguity is *not* assumed — each maximal run
    of one partition id becomes one segment, so any id layout is correct), and
    the reader half hands a partition back as fresh column arrays for the
    pooled aggregation machinery to group and reduce.

    The column schema (names, dtypes, order) is pinned by the first append;
    later appends must match exactly — a drift would silently corrupt the
    packed byte stream, so it raises instead.
    """

    def __init__(self, tmp_dir: str | None = None):
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        self._partitions: dict[int, _Partition] = {}
        self._schema: list[tuple[str, np.dtype]] | None = None
        self._cell_dtype: np.dtype | None = None
        self.bytes_written = 0

    @property
    def schema(self) -> list[tuple[str, np.dtype]] | None:
        return self._schema

    @property
    def cell_dtype(self) -> np.dtype | None:
        return self._cell_dtype

    def partition_keys(self) -> list[int]:
        """Keys of the partitions holding at least one row."""
        return list(self._partitions)

    def n_rows(self, part_key: int) -> int:
        return sum(self._partitions[part_key].segments)

    def append(
        self,
        part_ids: np.ndarray,
        cells: np.ndarray,
        col_dict: dict[str, np.ndarray],
    ) -> int:
        """Append rows to their partitions; returns exact bytes written.

        ``part_ids``, ``cells``, and every column are row-aligned 1-D arrays.
        """
        if self._schema is None:
            self._schema = [(name, np.dtype(arr.dtype)) for name, arr in col_dict.items()]
            self._cell_dtype = np.dtype(cells.dtype)
        else:
            got = [(name, np.dtype(arr.dtype)) for name, arr in col_dict.items()]
            if got != self._schema or np.dtype(cells.dtype) != self._cell_dtype:
                raise ValueError(
                    f"spill append schema drift: block was opened with "
                    f"{self._schema} (cells {self._cell_dtype}), got {got} "
                    f"(cells {np.dtype(cells.dtype)})"
                )
        if len(cells) == 0:
            return 0
        # Segment per maximal run of one partition id. No monotonicity is
        # assumed: a partition appearing in several runs simply gets several
        # segments, which read back in append order.
        bounds = np.flatnonzero(np.diff(part_ids)) + 1
        starts = np.concatenate(([0], bounds))
        ends = np.concatenate((bounds, [len(part_ids)]))
        written = 0
        for s, e in zip(starts, ends):
            key = int(part_ids[s])
            part = self._partitions.get(key)
            if part is None:
                part = self._partitions[key] = _Partition(self.tmp_dir)
            written += part.write_segment(
                [cells[s:e], *(col_dict[name][s:e] for name, _ in self._schema)]
            )
        self.bytes_written += written
        return written

    def read_partition(
        self, part_key: int, *, close: bool = False
    ) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        """Read one partition back as ``(cells, {name: values})``.

        Rows come back in exact append order (flush order, within-flush order
        preserved), so a stable sort by cell reproduces the pooled path's
        per-cell row order. ``close=True`` closes the partition's file after
        the read — its (already unlinked) bytes free immediately.
        """
        part = self._partitions[part_key]
        out = part.read(self._cell_dtype, self._schema)
        if close:
            part.close()
            del self._partitions[part_key]
        return out

    def close(self) -> None:
        """Close every partition file (space frees; files were never linked)."""
        for part in self._partitions.values():
            part.close()
        self._partitions.clear()


def _memory_budget_bytes() -> int:
    """Worker memory budget: Lambda env, else cgroup v2 limit, else RAM."""
    mb = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
    if mb is not None:
        try:
            return int(mb) * 2**20
        except ValueError:
            pass
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            return int(raw)
    except (OSError, ValueError):
        pass
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError):
        return 2 * 2**30


def _default_block_bytes(n_partitions: int, tmp_dir: str | None = None) -> int:
    """Default spill-block threshold (issue #217 design comment).

    The formula: a closing block's reduce working set is its **largest
    partition** (~block/K) plus the block's outputs, live alongside the read
    buffer — so block bytes ≲ ``0.8 x (memory - read buffer - outputs) x K``.
    The read buffer and output carriers aren't measurable up front; they are
    budgeted at 25% of memory combined, giving ``0.8 x 0.75 x memory x K =
    0.6 x memory x K``. ``/tmp`` must additionally hold the closing block
    beside the filling one, so the result is capped at 45% of the spill
    directory's current free space. Injectable for tests and ops via
    ``SpillAggregator(block_bytes=...)``.
    """
    mem = _memory_budget_bytes()
    st = os.statvfs(tmp_dir or tempfile.gettempdir())
    tmp_cap = int(0.45 * st.f_bavail * st.f_frsize)
    return max(1, min(int(0.6 * mem * n_partitions), tmp_cap))


class SpillAggregator:
    """Streaming worker state for ``aggregation.streaming.mode: spill``.

    Same read-side seams as :class:`~zagg.processing.streaming.StreamingAggregator`
    (``add_read`` / ``granule_done`` / ``flush`` / ``empty`` /
    ``occupied_cells``), but a flush **appends the buffer's grouped columns to
    the current block's partitions** instead of folding into running merge
    state — no per-flush ``build_tdigest``/``merge_tdigests`` CPU, which is
    the term that kills the heavy o8 shards (issue #217 fleet A/B).

    Aggregation happens after the reads, per partition:

    - **Single block** (no threshold crossing — all of o8 at the ``-disk``
      tiers): ``chunk_outputs`` reads a chunk's partition back, groups it with
      the same ``_group_columns`` the pooled path uses, and drives the
      **pooled aggregation machinery** (``_pool_chunk_columns`` →
      ``_eval_chunk_precompute`` → ``_aggregate_chunk_cells``) over it. Every
      reducer the pooled path supports — expressions, vectors, located
      ragged, ``chunk_precompute`` — works with zero reimplementation, and
      the output is byte-identical to pooled **by construction**: the
      partition holds exactly the chunk's rows in global read order, so the
      stable sort reproduces the pooled per-cell slices bit for bit.
    - **Multi block** (bytes hit the threshold — see
      :func:`_default_block_bytes`): each closing block is reduced
      partition-by-partition into running mergeable state (counts by
      summation, tdigests via ``merge_tdigests`` — the StreamingAggregator
      laws), collapsing merge rounds from N/buffer to ~spill/threshold. A
      config with any non-mergeable reducer raises
      :class:`SpillOverflowError` at the first crossing instead of silently
      approximating.

    ``chunk_outputs`` returns the 5-tuple ``_aggregate_chunk_cells`` contract
    (``stats_arrays, ragged_payloads, ragged_cell_indices, ragged_locations,
    cells_with_data``) — one element more than StreamingAggregator, since
    spill serves located fields.
    """

    def __init__(
        self,
        config: PipelineConfig,
        grid,
        handoff: str,
        buffer_granules: int,
        block_bytes: int | None = None,
        tmp_dir: str | None = None,
    ):
        self.config = config
        self.grid = grid
        self.handoff = handoff
        self.buffer_granules = buffer_granules
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        agg_fields = get_agg_fields(config)
        self._data_vars = get_data_vars(config)
        # Mergeable iff the merge-mode validator accepts the config: those are
        # exactly the reducers with a cross-block combine law. Non-mergeable
        # configs are still accepted — they are exact in the single-block
        # regime — but cannot survive a block close (SpillOverflowError).
        from zagg.processing.streaming import validate_streaming

        try:
            validate_streaming(config)
            self._mergeable = True
        except ValueError:
            self._mergeable = False
        self._count_fields: list[str] = []
        self._digest_fields: dict[str, tuple[str, int]] = {}  # name -> (source, delta)
        if self._mergeable:
            for name, meta in agg_fields.items():
                if get_output_signature(meta)["kind"] == "ragged":
                    delta = int((meta.get("params") or {}).get("delta", _DEFAULT_DELTA))
                    self._digest_fields[name] = (meta.get("source") or "h_li", delta)
                else:
                    self._count_fields.append(name)
        k = int(getattr(grid, "chunks_per_shard", 1)) if hasattr(grid, "chunk_order") else 1
        self._n_partitions = max(k, 1)
        if block_bytes is not None:
            self.block_bytes = int(block_bytes)
            check_tmp_headroom(max(_MIN_SPILL_BYTES, self.block_bytes), self.tmp_dir)
        else:
            check_tmp_headroom(_MIN_SPILL_BYTES, self.tmp_dir)
            self.block_bytes = _default_block_bytes(self._n_partitions, self.tmp_dir)
        self._block = SpillBlock(self.tmp_dir)
        self._closed_blocks = 0
        self._finalized = False
        # Cross-block mergeable running state (only ever fed on block close).
        self._counts: dict[int, int] = {}
        self._digests: dict[str, dict[int, np.ndarray]] = {n: {} for n in self._digest_fields}
        # Per-flush unique cell words; unioned lazily by occupied_cells().
        self._occupied: list[np.ndarray] = []
        # Single-block reduce cache: (part_key, col_arrays, cell_to_slice) for
        # the most recently loaded partition (chunks sharing a partition — the
        # K==1-partition case — reuse it; each partition is read once).
        self._loaded: tuple | None = None
        self.n_obs_total = 0
        self.flushes = 0
        self.spill_bytes = 0
        self.spill_write_s = 0.0
        self.spill_read_s = 0.0
        self._buffer: list = []
        self._buffered_granules = 0

    # -- read-side seams (StreamingAggregator contract) ----------------------

    def add_read(self, chunk) -> None:
        """Buffer one group read (the carrier ``_read_group`` returned)."""
        self._buffer.append(chunk)

    def granule_done(self) -> None:
        """Mark one granule fully read; flush when the buffer is full."""
        self._buffered_granules += 1
        if self._buffered_granules >= self.buffer_granules:
            self.flush()

    def flush(self) -> None:
        """Group the buffered reads and append them to the block's partitions."""
        if not self._buffer:
            self._buffered_granules = 0
            return
        from zagg.processing.aggregate import _concat_and_group

        col_arrays, cell_to_slice, n_obs = _concat_and_group(self._buffer, self.grid, self.handoff)
        self.n_obs_total += n_obs
        self.flushes += 1
        self._buffer = []
        self._buffered_granules = 0
        if not cell_to_slice:
            return
        keys = np.fromiter(cell_to_slice.keys(), dtype=np.uint64, count=len(cell_to_slice))
        lengths = np.fromiter(
            (e - s for s, e in cell_to_slice.values()), dtype=np.int64, count=len(cell_to_slice)
        )
        self._occupied.append(keys)
        # The sorted cell column reconstructed from the slice map (dict order
        # is ascending — _group_columns inserts along the sorted array), and
        # the per-row partition id from the per-cell one.
        cells_sorted = np.repeat(keys, lengths)
        part_rows = np.repeat(partition_ids(self.grid, keys), lengths)
        t0 = time.perf_counter()
        self.spill_bytes += self._block.append(part_rows, cells_sorted, col_arrays)
        self.spill_write_s += time.perf_counter() - t0
        if self._block.bytes_written >= self.block_bytes:
            self._close_block()

    @property
    def empty(self) -> bool:
        """True when no observation ever survived filtering."""
        return self.n_obs_total == 0 and not self._buffer

    def occupied_cells(self) -> np.ndarray:
        """Distinct populated cell words (issue #200 coverage sink), sorted."""
        if not self._occupied:
            return np.empty(0, dtype=np.uint64)
        return np.unique(np.concatenate(self._occupied))

    # -- block close / mergeable fold ----------------------------------------

    def _close_block(self) -> None:
        """Reduce the full block into mergeable state and open a fresh one."""
        if not self._mergeable:
            raise SpillOverflowError(
                f"spill block hit the {self.block_bytes:,}-byte threshold but the "
                f"config carries reducers with no merge law, so per-block results "
                f"cannot combine (single-block spill is exact for every reducer). "
                f"Remedies: a bigger memory tier, a '-disk' function variant with "
                f"more ephemeral storage, or a finer parent_order (smaller shards)."
            )
        self._fold_block(self._block)
        self._block.close()
        self._block = SpillBlock(self.tmp_dir)
        self._closed_blocks += 1

    def _fold_block(self, block: SpillBlock) -> None:
        """Fold one block into the running mergeable state, per partition.

        This is the StreamingAggregator merge sequence at block granularity:
        counts by summation (exact), tdigests built fresh per cell and merged
        under the field's delta — one merge round per block instead of per
        buffer, which is the ~6x merge-CPU collapse the design targets.
        """
        from zagg.processing.aggregate import _group_columns

        for key in block.partition_keys():
            t0 = time.perf_counter()
            cells, cols = block.read_partition(key, close=True)
            self.spill_read_s += time.perf_counter() - t0
            col_arrays, cell_to_slice = _group_columns(cols, cells)
            del cells, cols
            for cell, (start, end) in cell_to_slice.items():
                self._counts[cell] = self._counts.get(cell, 0) + (end - start)
                for name, (source, delta) in self._digest_fields.items():
                    fresh = build_tdigest(col_arrays[source][start:end], delta=delta)
                    held = self._digests[name].get(cell)
                    self._digests[name][cell] = (
                        fresh if held is None else merge_tdigests(held, fresh, delta=delta)
                    )

    # -- post-read emission ----------------------------------------------------

    def chunk_outputs(self, children, agg_fields: dict):
        """Emit one chunk's outputs; ``_aggregate_chunk_cells`` 5-tuple contract."""
        if self._closed_blocks:
            return self._chunk_outputs_merged(children, agg_fields)
        return self._chunk_outputs_exact(children, agg_fields)

    def _chunk_outputs_exact(self, children, agg_fields: dict):
        """Single-block regime: pooled machinery over the chunk's partition."""
        from zagg.processing.aggregate import (
            _aggregate_chunk_cells,
            _eval_chunk_precompute,
            _pool_chunk_columns,
        )

        children = np.asarray(children)
        key = int(partition_ids(self.grid, children[:1])[0]) if len(children) else 0
        if self._loaded is None or self._loaded[0] != key:
            self._load_partition(key)
        _, col_arrays, cell_to_slice = self._loaded
        chunk_pooled = _pool_chunk_columns(col_arrays, cell_to_slice, children)
        chunk_scalars = _eval_chunk_precompute(self.config, chunk_pooled)
        return _aggregate_chunk_cells(
            children,
            col_arrays,
            cell_to_slice,
            chunk_scalars,
            self.config,
            self._data_vars,
            agg_fields,
        )

    def _load_partition(self, key: int) -> None:
        """Read one partition back and group it (replacing the cached one)."""
        from zagg.processing.aggregate import _group_columns

        self._loaded = None  # free the previous partition before loading
        if key in self._block.partition_keys():
            t0 = time.perf_counter()
            cells, cols = self._block.read_partition(key, close=True)
            self.spill_read_s += time.perf_counter() - t0
            col_arrays, cell_to_slice = _group_columns(cols, cells)
        else:
            # Empty chunk: length-0 columns per the block schema — the same
            # shape the pooled path's _pool_chunk_columns hands an empty chunk.
            schema = self._block.schema or []
            col_arrays = {name: np.empty(0, dtype=dtype) for name, dtype in schema}
            cell_to_slice = {}
        self._loaded = (key, col_arrays, cell_to_slice)

    def _chunk_outputs_merged(self, children, agg_fields: dict):
        """Multi-block regime: emit from the cross-block mergeable state."""
        if not self._finalized:
            # The final (still-open) block was never threshold-closed; fold it
            # into the running state once, before the first emission.
            self._fold_block(self._block)
            self._block.close()
            self._finalized = True
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
        cells_with_data = 0
        for i, child in enumerate(children):
            cell = int(child)
            count = self._counts.get(cell)
            if count is None:
                for name in self._count_fields:
                    stats_arrays[name][i] = 0
                continue
            cells_with_data += 1
            for name in self._count_fields:
                stats_arrays[name][i] = count
            for name in self._digest_fields:
                digest = self._digests[name].get(cell)
                if digest is not None and digest.size > 0:
                    ragged_payloads[name].append(digest)
                    ragged_cell_indices[name].append(i)
        return stats_arrays, ragged_payloads, ragged_cell_indices, {}, cells_with_data

    def close(self) -> None:
        """Release every spill fd and the cached partition (idempotent)."""
        self._block.close()
        self._loaded = None
