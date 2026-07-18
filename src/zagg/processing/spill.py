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

import numpy as np


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
