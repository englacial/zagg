"""Spill-partition aggregation for the streaming worker (issue #217).

Option (A) from the #217 plan: instead of folding each read buffer into
running merge state (``streaming.py``, the ~500 s merge-CPU term that kills
the heavy o8 shards), a flush **appends the buffer's grouped columns to
per-partition packed files in ``/tmp``** and aggregation happens once, after
the reads, from complete per-cell data — reproducing the pooled path's
results byte-for-byte in the single-block regime.

The module has two layers: the block/partition writer + reader
(:class:`SpillBlock`) with the standalone ``/tmp`` capacity guard, and
:class:`SpillAggregator`, the worker-facing state machine that drives them.
Key mechanics:

- One append file per (block, partition). The file is created with
  ``tempfile.mkstemp`` and **unlinked immediately**; the open file object is
  the only reference, so space frees when the partition is closed (or the
  process dies) and nothing can leak across warm Lambda invokes — there is no
  teardown pathway to miss. Fd count is bounded at ``4**_GROUP_LEVELS`` (64)
  partition-group files per block, well under Lambda's ~1,024 nofile default
  even at production geometry (``chunk_inner: 13`` = 1,024 inner chunks per
  o8 shard).
- Records are packed columnar segments: per append, the cell words
  (``uint64``) followed by each declared column's values in schema order,
  raw bytes, no framing — segment row counts live in memory on the writer
  (same process reads them back). Read-back is ``seek(0)`` + ``readinto``
  straight into preallocated arrays: exact bytes in, exact bytes out.
- The partition key is the observation's partition-group id — the inner
  chunk when the shard owns ≤ 64, else a contiguous group of inner chunks
  (``clip2order`` at :func:`_group_order` — :func:`partition_ids`); with
  ``chunk_inner`` unset (K == 1) everything lands in a single partition.
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

import ast
import logging
import os
import tempfile
import threading
import time
from typing import NamedTuple

import numpy as np

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_data_vars,
    get_output_signature,
)
from zagg.stats.composition import merge_composition_kway, pack_composition_n
from zagg.stats.tdigest import (
    _DEFAULT_DELTA,
    build_tdigest,
    build_tdigest_where,
    merge_tdigests,
    merge_tdigests_kway,
)
from zagg.time_axis import TOC_SHAPE_PER_CENTROID, TOC_WORD_COLUMN

logger = logging.getLogger(__name__)

#: Floor for the spill-enable /tmp check: below this, even a degraded
#: many-block run is pointless — fail at config time instead of thrashing.
_MIN_SPILL_BYTES = 64 * 2**20


class SpillOverflowError(RuntimeError):
    """A spill block hit its threshold under a config with no merge law.

    Raised the moment a second block would open (never on single-block
    shards, where every reducer is exact); the message carries the probe's
    per-field verdict (``validate_spill_fold``) and the remedies.
    Deliberately a distinct type so the worker's tolerated per-granule
    ``except`` can re-raise it instead of warn-and-continue.
    """


class SpillReduceError(RuntimeError):
    """An overlap-thread block reduce failed; the merged state is incomplete.

    Raised by :meth:`SpillAggregator._join_reducer` when the parked reducer
    exception surfaces at the next block close — which happens inside the
    worker's per-granule read loop. Deliberately a distinct type (like
    :class:`SpillOverflowError`) so the worker's tolerated per-granule
    ``except`` re-raises it instead of downgrading it to warn-and-continue:
    a dropped block silently omitted from the emitted output must abort the
    shard, not be logged and swallowed.
    """


def check_tmp_headroom(
    need_bytes: int, tmp_dir: str | None = None, from_config: bool = False
) -> None:
    """Refuse to enable spill when ``/tmp`` cannot hold its working set.

    Standalone spill guard (issue #217 plan: written independently of the
    #260 arena SIGBUS guard so it survives the arena removal). Raises a loud
    config-style ``RuntimeError`` naming the deployment fix when the spill
    directory's free space is below ``need_bytes`` — typically the block
    threshold, the most a single spill block is allowed to grow.

    ``from_config`` says the requirement is derived from an operator-set
    ``aggregation.streaming.block_bytes`` (issue #474), which puts lowering
    that knob at the head of the remedies — otherwise the message quotes a
    number the operator controls without naming what controls it.
    """
    tmp_dir = tmp_dir or tempfile.gettempdir()
    st = os.statvfs(tmp_dir)
    avail = st.f_bavail * st.f_frsize
    if avail < need_bytes:
        knob = (
            "lower aggregation.streaming.block_bytes (this requirement is derived from it), "
            if from_config
            else ""
        )
        raise RuntimeError(
            f"aggregation.streaming.mode: spill needs {need_bytes:,} bytes of free "
            f"space in {tmp_dir!r} but only {avail:,} are available; {knob}deploy on a "
            f"function variant with larger ephemeral storage (the '-disk' variants, "
            f"e.g. process-shard-4096-disk) or fall back to mode: merge."
        )


#: Partition-group depth below the shard order: at most ``4**_GROUP_LEVELS``
#: (= 64) partition files per block. Production HEALPix configs pin
#: ``chunk_inner: 13``, which at an o8 dispatch shard is 4^5 = 1,024 inner
#: chunks — one file per inner chunk would blow Lambda's ~1,024 nofile
#: default. Grouping instead clips cells to ``parent_order + _GROUP_LEVELS``:
#: each group is a morton cell whose children at ``chunk_order`` are a
#: **contiguous** run of inner chunks, so a group's file still reads back as
#: one contiguous cell span for the pooled per-chunk build, and the reduce
#: working set divides by the group count (64 is ample: ~3 GB of spill reads
#: back in ~50 MB units) without approaching the fd limit.
_GROUP_LEVELS = 3


def _group_order(grid) -> int:
    """The morton order spill partitions are keyed at (grouped inner chunks)."""
    return min(int(grid.chunk_order), int(grid.parent_order) + _GROUP_LEVELS)


def partition_ids(grid, cells: np.ndarray) -> np.ndarray:
    """Spill partition key per cell: the enclosing partition-*group* id.

    HEALPix grids with a finer ``chunk_inner`` (K > 1) coarsen each
    child-order cell word to :func:`_group_order` via ``mortie.clip2order`` —
    ``grid.chunk_order`` itself when the shard owns ≤ ``4**_GROUP_LEVELS``
    inner chunks (a group == one inner chunk, the words ``grid.iter_chunks``
    enumerates), else a coarser prefix so at most 64 partition files exist per
    block. Either way a chunk's partition is found by clipping any of its
    children, and ``iter_chunks`` order visits each group as one contiguous
    run (morton children of a group cell are consecutive), so every group is
    read back exactly once. Every other case (``chunk_inner`` unset,
    rectilinear, minimal test stubs) is a single partition: key 0.
    """
    cells = np.asarray(cells)
    if int(getattr(grid, "chunks_per_shard", 1)) <= 1 or not hasattr(grid, "chunk_order"):
        return np.zeros(len(cells), dtype=np.uint64)
    from mortie import clip2order

    return np.asarray(clip2order(_group_order(grid), cells.astype(np.uint64)))


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
        n = len(cells)
        if len(part_ids) != n or any(len(arr) != n for arr in col_dict.values()):
            raise ValueError("spill append: part_ids/cells/columns must be row-aligned")
        if n == 0:
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


class _DigestField(NamedTuple):
    """One ragged tdigest field's fold-relevant declaration (issues #279/#370).

    ``pairwise`` selects the cross-block fold law: ``False`` — order-independent
    k-way collapse at finalize (``build_tdigest``/``build_tdigest_where``, the
    default); ``True`` — pairwise left-fold per block (``build_tdigest_pairwise``).
    ``location`` is the per-observation morton column (issue #87) or ``None``;
    ``where`` the field's raw ``where`` param (a column name or expression the
    fold resolves per cell, exactly like the pooled path) or ``None``.

    ``temporal`` records the spec §8.3 ``temporal: per-centroid`` declaration
    (issue #410): its words are the DERIVED toc column, not a read column, so
    unlike ``location`` there is no column name to carry.

    ``stratum`` records that the DECLARED function is ``build_tdigest_where``
    and is what selects the reducer in :meth:`SpillAggregator._fold_block` —
    never ``where``'s presence. Keying on the param would let a config drop
    ``where`` and silently get the unmasked population, or add one to a plain
    ``build_tdigest`` and silently get a masked digest, in either case
    disagreeing with the pooled replay (which raises ``TypeError``).
    ``validate_spill_fold`` rejects both mis-declarations up front, so
    ``stratum`` implies ``where is not None`` and vice versa.
    """

    source: str
    delta: int
    pairwise: bool
    location: str | None
    where: object | None
    stratum: bool
    temporal: bool


#: A digest field's companion channels in the kernel's FIXED return order
#: ``(digest, locations, temporal)`` — the same channels in the same order
#: ``sweep_overview.COMPANION_CHANNELS`` folds overviews by. Per entry: the
#: declaring :class:`_DigestField` attribute, the reducer kwarg, and the
#: ``chunk_outputs`` emission key (``_aggregate_chunk_cells``'s — ``times``, not
#: ``temporal``, for the §8.3 sibling). The ONLY place that order lives: every
#: fold site zips against this tuple, so none can drift on a dict's insertion
#: order (``strict=True`` checks a zip's length, never its order, and the two
#: word grammars accept each other's values, so a swap would be silent).
_CHANNELS = (("location", "locations", "locations"), ("temporal", "temporal", "times"))


def _channels(f: _DigestField) -> tuple[tuple[str, str], ...]:
    """``(reducer kwarg, emission key)`` per channel ``f`` declares, in kernel order."""
    return tuple((kwarg, emit) for attr, kwarg, emit in _CHANNELS if getattr(f, attr))


def _resolve_param(param, cell_data: dict[str, np.ndarray]):
    """Resolve one param over one cell's rows, the pooled path's way.

    ``cell_data`` is the cell's already-sliced column views — built once per
    cell by :meth:`SpillAggregator._fold_block`, exactly as the pooled path
    builds it once per cell before resolving every field's params
    (``_aggregate_chunk_cells`` -> ``calculate_cell_statistics``).

    Mirrors ``calculate_cell_statistics``'s params resolution exactly (bare
    column name -> that column's rows; a string naming columns -> eval'd over
    the cell's rows with the same cached code object; anything else passes
    through), so a stratum's per-block ``where`` membership and a composition
    field's conf columns are computed by the identical rule the pooled/
    single-block replay applies per cell — block boundaries cannot shift a
    photon between strata or lanes. The two namespaces are identical only
    because ``validate_spill_fold`` rejects ``chunk_precompute`` outright: the
    pooled namespace's one extra ingredient is its ``chunk_scalars``, which no
    config reaching this fold can carry.
    """
    from zagg.processing.aggregate import _compile_param_expr

    if isinstance(param, str) and param in cell_data:
        return cell_data[param]
    if isinstance(param, str) and any(c in param for c in cell_data):
        ns = {"__builtins__": {}, "np": np, "numpy": np, **cell_data}
        return eval(_compile_param_expr(param), ns)  # noqa: S307
    return param


def _unresolvable_names(param, available) -> tuple[str, ...]:
    """Identifiers ``param`` needs that :func:`_resolve_param` could not supply.

    The eval namespace is exactly the cell's columns plus ``np``/``numpy``
    (``__builtins__`` stripped), so an expression's free names are checkable
    against the block schema without running it — which is the only way to name
    the field AND the column for a param that today either falls through as a
    literal string (nothing in it matches a column: the failure surfaces as a
    shape mismatch deep inside the reducer) or reaches ``eval`` and raises a
    ``NameError`` naming the identifier but not the field. Bare column names
    parse as a single :class:`ast.Name`, so one rule covers both forms.

    Non-strings and strings that are not parseable expressions are genuine
    literals for their reducer and yield ``()`` — the check never widens
    :func:`_resolve_param`'s pass-through rule beyond what the caller declared
    must be a column.
    """
    if not isinstance(param, str):
        return ()
    try:
        tree = ast.parse(param, mode="eval")
    except SyntaxError:
        return ()
    allowed = set(available) | {"np", "numpy"}
    return tuple(sorted({n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} - allowed))


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


#: Peak in-memory working set of reducing one partition, as a multiple of its
#: spilled bytes. Measured on the issue #217 phase-3 replays (real 120/148 g
#: count slabs, K=1): 5,787 MB peak over a 1,652 MB partition and 8,338 MB
#: over 3,054 MB — the read-back columns, the stable-sort gather copies, and
#: the per-cell outputs coexist at ~2.6-3.5x the partition bytes. Budget the
#: worst case; do not lower without re-measuring the replay.
_BUILD_MULT = 3

#: Share of the spill directory's free space one worker may hold in open
#: blocks: the cap on :func:`_default_block_bytes`, and the cap the bulk
#: multi-window bins (:class:`zagg.processing.windowed.WindowBins`) enforce
#: over their N aggregators' open blocks together.
SPILL_TMP_FRACTION = 0.45


def _default_block_bytes(n_partitions: int, tmp_dir: str | None = None) -> int:
    """Default spill-block threshold (issue #217 design comment).

    The formula: a closing block's reduce working set is its **largest
    partition** (~block/K) at the measured :data:`_BUILD_MULT` build peak
    (read-back columns + sort copies + per-cell outputs — outputs are inside
    this multiple, charged here and nowhere else), live alongside the read
    buffer — so block bytes ≲ ``0.8 x (memory - read buffer) x K /
    _BUILD_MULT``. The read buffer (plus slack) isn't measurable up front; it
    is budgeted at 25% of memory, giving ``0.8 x 0.75 / 3 x memory x K = 0.2 x
    memory x K``. ``/tmp`` must additionally hold the
    closing block beside the filling one, so the result is capped at 45% of
    the spill directory's current free space. A finer ``chunk_inner`` raises
    K and with it the usable block (the build unit is one partition, not the
    block). Overridable from **config** —
    ``aggregation.streaming.block_bytes`` (issue #474), the route operators
    take — and from ``SpillAggregator(block_bytes=...)`` for tests and ops.
    An explicit value replaces this whole formula, 45% cap included, so the
    constructor headroom-checks the pair it will actually hold (2x under
    overlap); keep it at or below ~45% of the tier's ephemeral storage.
    """
    mem = _memory_budget_bytes()
    st = os.statvfs(tmp_dir or tempfile.gettempdir())
    tmp_cap = int(SPILL_TMP_FRACTION * st.f_bavail * st.f_frsize)
    return max(1, min(int(0.8 * 0.75 * mem * n_partitions / _BUILD_MULT), tmp_cap))


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
    - **Multi block** (bytes hit the threshold — ``aggregation.streaming.block_bytes``
      when the config sets it, else :func:`_default_block_bytes`): each closing block is reduced
      partition-by-partition into running mergeable state (counts by
      summation, tdigests via ``merge_tdigests``/``merge_tdigests_kway`` —
      including ``build_tdigest_where`` strata and, under the :data:`_CHANNELS`
      overloads, companion-carrying fields — and composition words via
      ``merge_composition_kway``), collapsing merge rounds from N/buffer to
      ~spill/threshold. A config with any reducer outside the
      ``validate_spill_fold`` surface raises :class:`SpillOverflowError` at the
      first crossing instead of silently approximating.

    ``chunk_outputs`` returns the 5-tuple ``_aggregate_chunk_cells`` contract
    (``stats_arrays, ragged_payloads, ragged_cell_indices, ragged_channels,
    cells_with_data``) — one element more than StreamingAggregator, since spill
    serves companion-carrying fields. Both :data:`_CHANNELS` ride BOTH regimes
    (issue #477): single-block through the pooled machinery unchanged,
    multi-block through the per-channel fold state below.
    """

    def __init__(
        self,
        config: PipelineConfig,
        grid,
        handoff: str,
        buffer_granules: int,
        block_bytes: int | None = None,
        tmp_dir: str | None = None,
        overlap: bool = True,
    ):
        self.config = config
        self.grid = grid
        self.handoff = handoff
        self.buffer_granules = buffer_granules
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        agg_fields = get_agg_fields(config)
        self._data_vars = get_data_vars(config)
        # Mergeable iff the spill fold probe accepts the config (issue #370):
        # wider than merge mode — located ragged fields and build_tdigest_where
        # strata fold on block closes. Non-mergeable configs are still accepted
        # — they are exact in the single-block regime — but cannot survive a
        # block close (SpillOverflowError).
        from zagg.processing.streaming import validate_spill_fold

        # The probe names every offending field; keep its text so the overflow
        # raised at the first block close can say WHICH reducer has no fold law
        # (this is its only call site — dropping the message left an operator
        # re-deriving it from a 900 s Lambda they cannot reproduce locally).
        self._fold_problems: str | None = None
        try:
            validate_spill_fold(config)
            self._mergeable = True
        except ValueError as e:
            self._mergeable = False
            self._fold_problems = str(e)
        self._count_fields: list[str] = []
        self._digest_fields: dict[str, _DigestField] = {}
        # name -> (source, params): scalar pack_composition fields (issue #370
        # option (a)); params carry the conf column names + threshold, resolved
        # per cell at fold time exactly like the pooled path.
        self._composition_fields: dict[str, tuple[str, dict]] = {}
        if self._mergeable:
            from zagg.processing.streaming import (
                _COMPOSITION_FUNCTION,
                _TDIGEST_PAIRWISE_FUNCTION,
                _TDIGEST_WHERE_FUNCTION,
            )

            for name, meta in agg_fields.items():
                sig = get_output_signature(meta)
                if sig["kind"] == "ragged":
                    params = meta.get("params") or {}
                    self._digest_fields[name] = _DigestField(
                        # Mirror the pooled path's source default (aggregate.py).
                        source=meta.get("source") or "h_li",
                        delta=int(params.get("delta", _DEFAULT_DELTA)),
                        pairwise=meta.get("function") == _TDIGEST_PAIRWISE_FUNCTION,
                        location=sig["location"],
                        where=params.get("where"),
                        stratum=meta.get("function") == _TDIGEST_WHERE_FUNCTION,
                        temporal=sig["temporal"] == TOC_SHAPE_PER_CENTROID,
                    )
                elif meta.get("function") == _COMPOSITION_FUNCTION:
                    self._composition_fields[name] = (
                        meta.get("source") or "h_li",
                        dict(meta.get("params") or {}),
                    )
                else:
                    self._count_fields.append(name)
        if hasattr(grid, "chunk_order") and int(getattr(grid, "chunks_per_shard", 1)) > 1:
            # Partition-group count: 4^levels below the shard order, capped at
            # 4**_GROUP_LEVELS files per block (see _GROUP_LEVELS).
            self._n_partitions = 4 ** (_group_order(grid) - int(grid.parent_order))
        else:
            self._n_partitions = 1
        if block_bytes is not None:
            # An explicit threshold skips _default_block_bytes' 45%-of-free-/tmp
            # cap, so the guard has to reserve what the run actually holds: under
            # overlap a CLOSING block is resident beside the FILLING one
            # (_close_block), i.e. 2x. Without the doubling any value between
            # ~50% and 100% of free /tmp passes here and then ENOSPCs mid-shard —
            # the failure check_tmp_headroom exists to pre-empt (issue #474).
            self.block_bytes = int(block_bytes)
            resident = self.block_bytes * (2 if overlap else 1)
            check_tmp_headroom(max(_MIN_SPILL_BYTES, resident), self.tmp_dir, from_config=True)
        else:
            check_tmp_headroom(_MIN_SPILL_BYTES, self.tmp_dir)
            self.block_bytes = _default_block_bytes(self._n_partitions, self.tmp_dir)
        # Whether the threshold is the operator's (aggregation.streaming.block_bytes)
        # or disk-derived — the overflow message only offers the knob when it is
        # actually the thing that set the number (issue #474).
        self._block_bytes_from_config = block_bytes is not None
        self._block = SpillBlock(self.tmp_dir)
        self._closed_blocks = 0
        self._finalized = False
        # Async read/reduce overlap (phase 5): at most one closed block is
        # being reduced on ``_reducer`` while reads fill the next block; its
        # failure is parked in ``_reduce_err`` and re-raised at the next join.
        self.overlap = bool(overlap)
        self._reducer: threading.Thread | None = None
        self._reduce_err: BaseException | None = None
        # Called before every block close: the bulk multi-window bins join all
        # N aggregators' reducers there, so one block reduces at a time.
        self.before_close = None
        # Cross-block mergeable running state (only ever fed on block close).
        self._counts: dict[int, int] = {}
        self._digests: dict[str, dict[int, np.ndarray]] = {n: {} for n in self._digest_fields}
        # Companion-channel running state (issues #370/#410), per field and per
        # DECLARED channel (:data:`_CHANNELS`): the per-cell uint64 word vector
        # row-aligned with the running digest. Every channel rides the SAME
        # merge_tdigests / merge_tdigests_kway call as its payload, so a field
        # declaring both never has one folded against a partition the other
        # did not see.
        self._digest_words: dict[str, dict[str, dict[int, np.ndarray]]] = {
            n: {kw: {} for kw, _ in _channels(f)} for n, f in self._digest_fields.items()
        }
        # Whether any field needs the derived toc word column at fold time.
        self._needs_toc = any(f.temporal for f in self._digest_fields.values())
        # Composition state (issue #370): per-cell (word, n_signal), written
        # once at finalize from the per-block parts below.
        self._compositions: dict[str, dict[int, tuple[int, int]]] = {
            n: {} for n in self._composition_fields
        }
        # Per-block (word, n_signal) parts, stashed like ``_digest_parts`` and
        # collapsed in ONE weighted-mean pass at finalize
        # (``merge_composition_kway``): quantizing once instead of once per
        # block close keeps the stored word independent of block-close order —
        # the property the k-way digest fold already has, and the one #280's
        # parallel reduce would otherwise silently break here — and holds the
        # count error at one quantization instead of compounding with the block
        # count. Still option (a): the word is not byte-stable against the
        # single-block result, presence is exact via the floor. Two ints per
        # cell per block, negligible beside the digest parts.
        self._composition_parts: dict[str, dict[int, list[tuple[int, int]]]] = {
            n: {} for n in self._composition_fields
        }
        # k-way fold accumulator: for order-independent fields, each block's
        # per-cell digest is stashed here and collapsed once at finalize
        # (merge_tdigests_kway), rather than pairwise-folded per block. Holding
        # all parts to finalize is by design: order-independence forbids
        # incremental collapse (folding subsets then combining is a tree
        # reduction of a non-associative op, which reintroduces the very
        # order-dependence k-way exists to remove). Cost is bounded: each part is
        # a saturated ≤δ-centroid digest (~4 KB at δ=512, δ·8 bytes), so peak is
        # ~B×C×(δ·8) over B closed blocks and C occupied cells — small relative
        # to the multi-GB per-partition build peak the block threshold already
        # reserves. Tighter memory-budget accounting against that reservation is
        # owned by the #280 parallel-reduce work.
        self._digest_parts: dict[str, dict[int, list[np.ndarray]]] = {
            n: {} for n, f in self._digest_fields.items() if not f.pairwise
        }
        # Companion words stashed alongside, index-aligned with ``_digest_parts``
        # so the finalize k-way collapse folds every channel in the same pass.
        self._digest_word_parts: dict[str, dict[str, dict[int, list[np.ndarray]]]] = {
            n: {kw: {} for kw, _ in _channels(f)}
            for n, f in self._digest_fields.items()
            if not f.pairwise
        }
        # Per-flush unique cell words; unioned lazily by occupied_cells().
        self._occupied: list[np.ndarray] = []
        # Single-block reduce cache: (part_key, col_arrays, cell_to_slice) for
        # the most recently loaded partition. Chunks sharing a partition group
        # reuse it, and iter_chunks visits each group as one contiguous run,
        # so every group is read back exactly once. ``_consumed`` tripwires that
        # invariant: a group re-requested after its read-and-close raises rather
        # than silently emitting the empty-columns else-branch (see
        # _load_partition).
        self._loaded: tuple | None = None
        self._consumed: set[int] = set()
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

    @property
    def open_block_bytes(self) -> int:
        """Bytes appended to the block still filling (the ``/tmp`` it holds)."""
        return self._block.bytes_written

    def close_block(self) -> None:
        """Close the filling block now — the bulk bins' shared ``/tmp`` cap
        (:class:`zagg.processing.windowed.WindowBins`); the threshold crossing
        in :meth:`flush` is the same fold."""
        self._close_block()

    def join_reducer(self) -> None:
        """Wait for this aggregator's in-flight block reduce (the bulk bins)."""
        self._join_reducer()

    @property
    def closed_blocks(self) -> int:
        """Blocks closed at the threshold; 0 = exact single-block regime.

        Ridden into the worker's ``phase_timings`` as ``spill_blocks_closed``
        (issue #370) so an overflow-folded leaf is queryable in the run
        telemetry, the same route ``spill_bytes`` takes.
        """
        return self._closed_blocks

    def occupied_cells(self) -> np.ndarray:
        """Distinct populated cell words (issue #200 coverage sink), sorted."""
        if not self._occupied:
            return np.empty(0, dtype=np.uint64)
        return np.unique(np.concatenate(self._occupied))

    # -- block close / mergeable fold ----------------------------------------

    def _close_block(self) -> None:
        """Hand the full block to the reducer and open a fresh one.

        Async read/reduce overlap (issue #217 phase 5): the closed block is
        reduced disk→memory on one worker thread while reads keep streaming
        network→disk into the next block. At most one closed block is in
        flight — the next close **joins** the previous reduce before starting
        its own — so the /tmp working set stays closing + filling (the
        threshold formula's reservation) and blocks fold in close order, which
        keeps the merge sequence (and therefore the bytes out) identical to
        the sequential path. ``overlap=False`` reduces inline.
        """
        if not self._mergeable:
            # The threshold is the operator's own number when it came from
            # config, so raising it — the one remedy that keeps this shard in
            # the exact single-block regime — leads the list (issue #474).
            knob = (
                "a larger aggregation.streaming.block_bytes (this threshold came from it), "
                if self._block_bytes_from_config
                else ""
            )
            raise SpillOverflowError(
                f"spill block hit the {self.block_bytes:,}-byte threshold but the "
                f"config carries reducers with no cross-block fold law, so per-block "
                f"results cannot combine (the fold covers 'len'/'count', tdigest "
                f"fields — located, temporal, where-strata, pairwise — and the "
                f"packed composition word; single-block spill is exact for every "
                f"reducer). "
                f"{self._fold_problems}. "
                f"Remedies: {knob}a bigger memory tier, a '-disk' function variant "
                f"with more ephemeral storage, or a finer parent_order (smaller shards)."
            )
        if self._closed_blocks == 0:
            # Once per shard, at the moment the exact regime is left (issue
            # #370): from here on outputs FOLD across blocks — an overflow
            # leaf is not byte-identical to its single-block result.
            logger.warning(
                f"spill block threshold ({self.block_bytes:,} bytes) crossed: this "
                f"shard leaves the exact single-block regime and its outputs now "
                f"fold across blocks — digests merge under t-digest semantics, "
                f"located centroids coarsen to common ancestors, temporal words "
                f"widen to their members' envelope, composition takes "
                f"one k-way re-quantization; counts stay exact."
            )
        if self.before_close is not None:
            self.before_close()
        block = self._block
        self._block = SpillBlock(self.tmp_dir)
        self._closed_blocks += 1
        if not self.overlap:
            try:
                self._fold_block(block)
            finally:
                block.close()
            return
        self._join_reducer()
        self._reducer = threading.Thread(
            target=self._reduce_one, args=(block,), name="zagg-spill-reduce", daemon=True
        )
        self._reducer.start()

    def _reduce_one(self, block: SpillBlock) -> None:
        """Reducer-thread body: fold one closed block, then release its fds."""
        try:
            self._fold_block(block)
        except BaseException as e:  # surfaced by _join_reducer on the main thread
            self._reduce_err = e
        finally:
            block.close()

    def _join_reducer(self) -> None:
        """Wait for the in-flight block reduce; re-raise its failure loudly."""
        if self._reducer is not None:
            self._reducer.join()
            self._reducer = None
        if self._reduce_err is not None:
            err, self._reduce_err = self._reduce_err, None
            raise SpillReduceError(
                "spill block reduce failed on the overlap thread; the merged "
                "running state is incomplete"
            ) from err

    def _fold_block(self, block: SpillBlock) -> None:
        """Fold one block into the running mergeable state, per partition.

        Counts fold by summation (exact). tdigests are built fresh per cell —
        a located field (issue #87) via the located ``build_tdigest`` (its
        ``leaf_id`` column is a spilled read column, so the block holds the
        exact order-29 point words), a ``where`` stratum field (issue #321)
        via ``build_tdigest_where`` with its mask resolved per cell over the
        block's spilled columns (:func:`_resolve_param` — row selection
        precedes the build, so block boundaries cannot shift a photon between
        strata, issue #370). Which of the two reducers runs is decided by the
        field's **declared function** (``_DigestField.stratum``), never by
        whether a ``where`` param happens to be present, so the fold can never
        substitute a reducer the config did not declare. A ``temporal:
        per-centroid`` field (spec §8.3) additionally rides the derived toc word
        column, encoded per cell below as the pooled path does and passed as the
        build's ``temporal=`` channel. Then, per the field's fold law: **k-way**
        fields stash the block digest (+ every declared channel's words) for the
        order-independent collapse at finalize (:meth:`_finalize_kway`);
        **pairwise** fields merge it into the running digest here, the companion
        overloads carrying the channels. Composition fields stash a per-block
        ``pack_composition_n`` ``(word, n_signal)`` pair — the same predicate and
        bytes as the pooled reducer — for the same finalize collapse
        (``merge_composition_kway``). Either way it is one build round per block
        instead of per buffer (the ~6x merge-CPU collapse of issue #279).
        """
        from zagg.processing.aggregate import _group_columns, _toc_word_column

        self._check_fold_columns(block.schema)
        for key in block.partition_keys():
            t0 = time.perf_counter()
            cells, cols = block.read_partition(key, close=True)
            self.spill_read_s += time.perf_counter() - t0
            col_arrays, cell_to_slice = _group_columns(cols, cells)
            del cells, cols
            for cell, (start, end) in cell_to_slice.items():
                self._counts[cell] = self._counts.get(cell, 0) + (end - start)
                # One namespace per cell, not per field — the pooled path's
                # shape (`calculate_cell_statistics` gets one `cell_data`),
                # and the strata config declares two complementary `where`
                # fields over the same source.
                cell_data = {col: arr[start:end] for col, arr in col_arrays.items()}
                if self._needs_toc:
                    # The derived toc word column (spec §8.3), encoded where the
                    # pooled path encodes it — right after the namespace is
                    # built (``calculate_cell_statistics``) — so the two
                    # ``cell_data`` dicts match and a ``where`` reading the
                    # column resolves identically. Per cell, not once per
                    # partition: this path exists because memory is scarce, and
                    # the encode's transient allocation is unbudgeted by
                    # ``_default_block_bytes`` / ``_BUILD_MULT``.
                    cell_data[TOC_WORD_COLUMN] = _toc_word_column(cell_data, self.config)
                for name, f in self._digest_fields.items():
                    values = cell_data[f.source]
                    declared = _channels(f)  # every zip below is against THIS tuple
                    chans = {
                        kw: cell_data[f.location if kw == "locations" else TOC_WORD_COLUMN]
                        for kw, _ in declared
                    }
                    if f.stratum:
                        where = _resolve_param(f.where, cell_data)
                        built = build_tdigest_where(values, delta=f.delta, where=where, **chans)
                    else:
                        built = build_tdigest(values, delta=f.delta, **chans)
                    # ``(digest, *words)`` with any channel declared, the bare
                    # digest without — the kernel's arity contract.
                    fresh, *words = built if declared else (built,)
                    if not f.pairwise:
                        self._digest_parts[name].setdefault(cell, []).append(fresh)
                        parts = self._digest_word_parts[name]
                        for (kw, _), vec in zip(declared, words, strict=True):
                            parts[kw].setdefault(cell, []).append(vec)
                        continue
                    running = self._digest_words[name]
                    held = self._digests[name].get(cell)
                    if held is None:
                        self._digests[name][cell] = fresh
                        for (kw, _), vec in zip(declared, words, strict=True):
                            running[kw][cell] = vec
                    elif declared:
                        # Both channels ride ONE merge (kwargs ``{kw}1``/``{kw}2``),
                        # so the words describe the digest this call produced.
                        pairs = {}
                        for (kw, _), vec in zip(declared, words, strict=True):
                            pairs[f"{kw}1"], pairs[f"{kw}2"] = running[kw][cell], vec
                        merged, *merged_words = merge_tdigests(held, fresh, delta=f.delta, **pairs)
                        self._digests[name][cell] = merged
                        for (kw, _), vec in zip(declared, merged_words, strict=True):
                            running[kw][cell] = vec
                    else:
                        self._digests[name][cell] = merge_tdigests(held, fresh, delta=f.delta)
                for name, (source, params) in self._composition_fields.items():
                    values = cell_data[source]
                    kwargs = {k: _resolve_param(v, cell_data) for k, v in params.items()}
                    self._composition_parts[name].setdefault(cell, []).append(
                        pack_composition_n(values, **kwargs)
                    )

    def _check_fold_columns(self, schema) -> None:
        """Name a missing source/location/param column, as the pooled path does.

        Covers every column the fold resolves out of the block: a field's
        ``source`` and ``location``, a stratum's ``where``, and a composition
        field's ``conf_*`` params (see :func:`_unresolvable_names`). A column the
        block never carried would otherwise surface as a bare ``KeyError`` on the
        overlap reducer thread, which :meth:`_join_reducer` re-wraps as
        :class:`SpillReduceError` with the real cause reachable only via
        ``__cause__`` — materially worse than ``calculate_cell_statistics``'s
        named ``ValueError`` for the same config error, and visible only on
        shards big enough to close a block. Checked once per block against the
        block schema, off the per-cell loop; an empty block has nothing to check.
        """
        available = sorted(name for name, _ in schema or [])
        if not available:
            return
        # The derived toc word column is not spilled — ``_fold_block`` encodes it
        # per cell — but it IS resolvable there, as on the pooled path, so a
        # ``where`` reading it is not a missing column. It stays OUT of
        # ``available``, which is the block's true contents: the source/location
        # messages must not advertise ``toc_word``, and a ``location: toc_word``
        # must still raise here rather than reach ``mortie.common_ancestor``. An
        # absent CLOCK column is named by ``_toc_word_column``, with the pooled
        # path's message.
        resolvable = sorted([*available, TOC_WORD_COLUMN]) if self._needs_toc else available
        for name, f in self._digest_fields.items():
            if f.source not in available:
                raise ValueError(
                    f"ragged field {name!r} declares source: {f.source!r} but that "
                    f"column is not in the spilled block (available: {available})"
                )
            if f.location is not None and f.location not in available:
                raise ValueError(
                    f"ragged field {name!r} declares location: {f.location!r} but that "
                    f"column is not in the spilled block (available: {available}); "
                    f"per-observation mortons require a HEALPix grid"
                )
            missing = _unresolvable_names(f.where, resolvable)
            if missing:
                raise ValueError(
                    f"ragged field {name!r} declares where: {f.where!r}, which names "
                    f"{list(missing)} — not in the spilled block (available: "
                    f"{available}); the stratum mask is resolved per cell over the "
                    f"block's columns"
                )
        for name, (source, params) in self._composition_fields.items():
            if source not in available:
                raise ValueError(
                    f"field {name!r} declares source: {source!r} but that column is "
                    f"not in the spilled block (available: {available})"
                )
            # The five conf_* params MUST resolve to row-aligned columns (they
            # are stacked in pack_composition_n): a typo falls through
            # _resolve_param as a literal string and dies inside
            # np.column_stack naming neither the field nor the column.
            for pname, pval in params.items():
                missing = _unresolvable_names(pval, resolvable) if pname.startswith("conf_") else ()
                if missing:
                    raise ValueError(
                        f"field {name!r} param {pname}: {pval!r} names {list(missing)}, "
                        f"not in the spilled block (available: {available}); the conf "
                        f"params must resolve to row-aligned columns"
                    )

    def _finalize_kway(self) -> None:
        """Collapse every k-way channel's per-block parts, once per cell.

        Runs once, after the final block is folded and before the first emission.
        The single-pass k-way merge is order-independent (t-digest merge is not
        associative), so the result does not depend on block reduce order — the
        property #280 relies on to parallelize the reducer. A companion-carrying
        field's word parts collapse in the SAME pass (the ``merge_tdigests_kway``
        channel overloads, in :data:`_CHANNELS` order), keeping every channel
        row-aligned with the payload — and order-independent too, since the merge
        breaks ``(mean, weight)`` ties on the words themselves, location tertiary
        and toc quaternary. Composition parts collapse the same way
        (``merge_composition_kway`` over the block ``(word, n_signal)`` pairs:
        one weighted lane mean, quantized once), so a reducer parallelized
        under #280 inherits the property on all three channels rather than
        finding composition silently excluded.
        """
        for name, cell_parts in self._digest_parts.items():
            f = self._digest_fields[name]
            dest = self._digests[name]
            word_parts = self._digest_word_parts[name]
            declared = _channels(f)  # ``_CHANNELS`` order, as in _fold_block
            if declared:
                dest_words = self._digest_words[name]
                for cell, parts in cell_parts.items():
                    folded = merge_tdigests_kway(
                        parts,
                        delta=f.delta,
                        **{kw: word_parts[kw][cell] for kw, _ in declared},
                    )
                    dest[cell] = folded[0]
                    for (kw, _), vec in zip(declared, folded[1:], strict=True):
                        dest_words[kw][cell] = vec
                for by_cell in word_parts.values():
                    by_cell.clear()
            else:
                for cell, parts in cell_parts.items():
                    dest[cell] = merge_tdigests_kway(parts, delta=f.delta)
            cell_parts.clear()
        for name, comp_parts in self._composition_parts.items():
            comp_dest = self._compositions[name]
            for cell, pairs in comp_parts.items():
                comp_dest[cell] = (
                    merge_composition_kway(pairs),
                    sum(n for _, n in pairs),
                )
            comp_parts.clear()

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
        assert self._loaded is not None  # _load_partition always sets it before returning
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
            chunk_pooled=chunk_pooled,
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
            self._consumed.add(key)  # read-and-closed: this group is now gone
        elif key in self._consumed:
            # The group was read once (close=True deleted its partition) and is
            # being requested again: iter_chunks did NOT visit its chunks
            # contiguously. The empty-columns branch below would silently emit
            # zeros for a genuinely-populated group, so abort the shard loudly.
            raise RuntimeError(
                f"spill group {key} re-requested after it was read-and-closed; "
                f"the single-block exact reduce relies on iter_chunks visiting "
                f"each partition group's chunks contiguously (every group read "
                f"back exactly once)"
            )
        else:
            # Empty chunk: length-0 columns per the block schema — the same
            # shape the pooled path's _pool_chunk_columns hands an empty chunk.
            schema = self._block.schema or []
            col_arrays = {name: np.empty(0, dtype=dtype) for name, dtype in schema}
            cell_to_slice = {}
        self._loaded = (key, col_arrays, cell_to_slice)

    def _chunk_outputs_merged(self, children, agg_fields: dict):
        """Multi-block regime: emit from the cross-block mergeable state."""
        from zagg.processing.aggregate import _field_sentinel, _integer_fill

        if not self._finalized:
            # Join the in-flight overlap reduce (its failure re-raises here),
            # then fold the final (still-open, never threshold-closed) block
            # into the running state once, before the first emission. The k-way
            # collapse runs after every block is folded so it sees all parts.
            self._join_reducer()
            try:
                self._fold_block(self._block)
            finally:
                self._block.close()
            self._finalize_kway()
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
        for name in self._composition_fields:
            # The packed word (issue #321). Same dtype default as every other
            # emission path (`_aggregate_chunk_cells`, the count loop above) and
            # the same fill derivation the pooled path uses for an empty cell
            # (`_integer_fill` for integer dtypes — it names a non-numeric
            # sentinel instead of letting it reach numpy — else
            # `_field_sentinel`): a dtype-omitted composition field must not
            # store float32 single-block and uint64 multi-block.
            meta = agg_fields[name]
            dtype = np.dtype(meta.get("dtype", "float32"))
            fill = (
                _integer_fill(meta, dtype)
                if np.issubdtype(dtype, np.integer)
                else _field_sentinel(meta)
            )
            stats_arrays[name] = np.full(n_cells, fill, dtype=dtype)
        ragged_payloads: dict[str, list] = {n: [] for n in self._digest_fields}
        ragged_cell_indices: dict[str, list[int]] = {n: [] for n in self._digest_fields}
        # Companion-carrying fields only: keyed presence tells the worker which
        # sibling slots to deliver, mirroring _aggregate_chunk_cells — the same
        # :data:`_CHANNELS` emission keys in the same order, so a folded chunk
        # and a pooled one are indistinguishable to the writer.
        ragged_channels: dict[str, dict[str, list]] = {
            n: {emit: [] for _, emit in _channels(f)}
            for n, f in self._digest_fields.items()
            if _channels(f)
        }
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
            for name in self._composition_fields:
                # Every occupied cell has an entry (the fold writes one per
                # cell per block); the word may legitimately be 0 (no signal).
                stats_arrays[name][i] = self._compositions[name][cell][0]
            for name in self._digest_fields:
                digest = self._digests[name].get(cell)
                if digest is not None and digest.size > 0:
                    ragged_payloads[name].append(digest)
                    ragged_cell_indices[name].append(i)
                    if name in ragged_channels:
                        running = self._digest_words[name]
                        for kw, emit in _channels(self._digest_fields[name]):
                            ragged_channels[name][emit].append(running[kw][cell])
        return stats_arrays, ragged_payloads, ragged_cell_indices, ragged_channels, cells_with_data

    def close(self) -> None:
        """Release every spill fd and the cached partition (idempotent).

        Joins a still-running overlap reduce first (without re-raising — close
        runs on cleanup paths and must not mask the original error; the parked
        failure stays in ``_reduce_err`` for a later ``_join_reducer``). The
        reducer thread always terminates after its one block, so no thread can
        outlive the shard even when the worker aborts mid-read.
        """
        if self._reducer is not None:
            self._reducer.join()
            self._reducer = None
        self._block.close()
        self._loaded = None
