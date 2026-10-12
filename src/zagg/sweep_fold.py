"""The chunk-streamed fold kernels (issue #586 phase 4; kernels from issue #384).

:mod:`zagg.sweep_stage` owns one stage worker — the planner and the
writers; :mod:`zagg.sweep_overview` the ``/1`` retrofit sweep. This module
owns the fold kernels BOTH run and the sources they fold from (the
stamp-validated column and artifact readers), in the form the 2026-09-26
amendment on issue #586 ruled: **block by block over the output range**.
Every output cell's fold is independent — a gather assigns a child's span, a
cascade k-way merges the children's cells that fold into it — so a level is
folded one block of output cells at a time: read only the child members
covering the block, fold, hand the block on, free. Nothing is spilled to
disk: the inputs already live in the store and are re-read per block, so a
spill would only add billed ephemeral storage.

:func:`cascade_fold` is THE per-node fold of a ladder level from the level
below it (issue #620): the ``/1`` cascade (:func:`zagg.sweep_overview._cascade_node`)
and every ``/2`` stage-merge level (:func:`zagg.sweep_stage._stage_fold`)
produce their outputs through it, and nothing else folds a node — any
future overview regime (raster included, issue #399) joins here.

What that bounds. Before this module a fold densified a whole level: one slab
per field for all ``4^(r - k)`` output cells, each child member read whole
(``group[name][:]``) before it was assigned or merged. The 0.55 fleet's
order-6 stage died at 4,094 MB on 64 unwindowed leaves that way. Resident
inputs are now one block's: :data:`STAGE_BLOCK_ORDER` output cells of a
gather, or the source cells of the output cells one block covers in a merge
(never fewer than one output cell's ``factor`` sources — a cell folds in ONE
k-way call, so that is a floor, not a choice).

The block is aligned to the **stage column's own zarr chunking**: a column
group wider than one block is written on regular inner chunks of exactly one
block (:func:`zagg.sweep_stage.write_stage_column`), so the block a writer
emits is one chunk object and the block a parent reads is one chunk object.
A ladder overview stays the single chunk it always was (its extent is the
ladder depth, 256 cells at the reference geometry; the Icechunk level arrays
of spec §11 reference it whole), so its fold assembles one output chunk from
streamed inputs.

Values are unchanged by the block size: a block is a set of whole output
cells, each folded from exactly the sources the whole-level fold gave it, in
the same order. ``content_hash``, ``generation`` and ``source_children``
accumulate across blocks and are computed once at the end.
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)

#: One fold block, as a HEALPix order difference: ``4 ** STAGE_BLOCK_ORDER``
#: cells (1,024). It is also the inner-chunk extent of a streamed stage
#: column group, so writer and reader blocks are whole chunk objects. A power
#: of four so blocks nest in every child span and merge factor.
STAGE_BLOCK_ORDER = 5


class ColumnMovedError(ValueError):
    """A source was rewritten after a fold had already read from it.

    A streamed fold reads a member block by block, so a rewrite landing
    between two blocks would hand it half of each write. The reader pins the
    stamp its first served read validated under
    (:class:`_ColumnReader`); a later read under another
    stamp, or under none (a rewrite in flight), raises this instead of
    serving data. The ARTIFACT being folded is then folded again from fresh
    readers (:func:`refold_on_move`), never patched.
    """


# ---------------------------------------------------------------------------
# The fold's sources: stamp-validated readers over child columns and ladder
# artifacts, and the levels a unit has just folded.
# ---------------------------------------------------------------------------


#: Row marker for a candidate child whose column could not be READ (open or
#: root-metadata fault) — distinct from ``None`` (no committed column at all,
#: i.e. missing): the two land in different ``source_children`` counters, and
#: a log line in an exited process cannot make that distinction for a reader.
UNREADABLE = object()


def _is_reader(entry) -> bool:
    return isinstance(entry, _ColumnReader)


class ForeignSweepError(RuntimeError):
    """A foreign run's FRESH stamp was seen mid-run: two sweeps are live.

    The lease (:mod:`zagg.sweep_lease`) makes this unreachable in normal
    operation; reaching it means a residual race (TTL-expiry clock skew, a
    zombie worker from a crashed run) — abort loudly, never fold through it.
    """


def _same_stamp(a: dict | None, b: dict | None) -> bool:
    """Whether two commit-stamp reads witness the same write."""
    return (a or None) == (b or None)


def _foreign_fresh(stamp: dict | None, run_id: str, run_started: str) -> bool:
    """A stage stamp from ANOTHER run, written since this run started.

    Fleet stamps carry no ``run_id`` and are never foreign (fleet ∥ sweep is
    allowed by the concurrency matrix); a foreign STAGE stamp older than this
    run is a completed prior sweep — ordinary input, not a conflict. The
    comparison is STRICT: stamps resolve to whole seconds, so a prior run
    that completed in the second this run started would otherwise read as
    live. A true same-second race escapes the backstop — admission is the
    LEASE's job; this predicate only has to catch the long-lived residuals
    (zombie workers, TTL clock skew), which keep writing well past our start.
    """
    if not isinstance(stamp, dict):
        return False
    rid = stamp.get("run_id")
    return rid is not None and rid != run_id and str(stamp.get("written_at") or "") > run_started


class _ColumnReader:
    """Stamp-validated reads over one child column (leaf or stage).

    One root-metadata GET serves the D4 stamp gate, the provenance attrs, and
    the generation basis together. Every group read is bracketed by the
    **optimistic stamp validation** the lease ruling requires: stamp before,
    read, stamp after — if the stamp moved (a fleet worker rewrote the leaf
    mid-read, the allowed fleet ∥ sweep regime), the read retries against the
    fresh stamp — but only until the reader has SERVED a read. The first
    served read pins its stamp: the fold reads a member block by block (issue
    #586 phase 4), so after that a moved stamp, or a vanished one (a rewrite
    in flight), raises :class:`zagg.sweep_fold.ColumnMovedError` rather than
    hand the fold half of each write, and a stamp that keeps moving raises it
    too. The caller folds the artifact again from fresh readers. A stage
    column stamped by a foreign run SINCE this run started raises
    :class:`ForeignSweepError` (two live sweeps — the lease backstop).

    Handed ``store`` (the invoke's obstore handle at the store root), ``path``
    is the column's key RELATIVE to it and no client is built (issue #610);
    without it, ``path`` is absolute and opened on its own.
    """

    def __init__(self, path: str, *, run_id: str, run_started: str, store_kwargs: dict, store=None):
        from zarr.storage import StorePath

        from zagg.store import open_store, zarr_view

        self.path = path
        self.revalidated = 0
        self._served = False  # whether a read was handed out (pins self.stamp)
        self._run_id, self._run_started = run_id, run_started
        if store is None:
            self._store = open_store(path, read_only=True, **store_kwargs)
        else:
            self._store = StorePath(zarr_view(store), path)
        self._arrays: dict = {}
        self.stamp, self.attrs = self._root()
        self._foreign_guard(self.stamp)

    def _foreign_guard(self, stamp: dict | None) -> None:
        if _foreign_fresh(stamp, self._run_id, self._run_started):
            raise ForeignSweepError(
                f"column {self.path} carries a fresh stamp from foreign sweep run "
                f"{stamp.get('run_id')!r} (written {stamp.get('written_at')}); "
                f"two sweeps are live on this store — aborting (lease backstop)"
            )

    def _root(self) -> tuple[dict | None, dict]:
        """Root attrs + stamp; absent reads ``(None, {})``, corrupt RAISES.

        The distinction feeds ``source_children``: a cleanly absent column is
        ``missing`` (never generated, or a fleet still in flight); a column
        whose root metadata exists but cannot be read is ``unreadable``
        (review finding — the two must not launder into one counter).
        """
        import zarr
        from zarr.errors import GroupNotFoundError

        from zagg.hive import COMMIT_ATTR

        try:
            attrs = dict(zarr.open_group(self._store, path="", mode="r", zarr_format=3).attrs)
        except (FileNotFoundError, GroupNotFoundError):
            return None, {}
        stamp = attrs.get(COMMIT_ATTR)
        return (dict(stamp) if isinstance(stamp, dict) else None), attrs

    @property
    def committed(self) -> bool:
        return self.stamp is not None

    def generation(self) -> tuple:
        """This column's generation basis: its own, or the leaf identity.

        :func:`zagg.column.stamped_generation_key`'s triple: a stage column's
        recorded ``generation`` block (the summed ratchet) or a leaf column's
        identity — one leaf at its stamp's timestamp — plus the run id THIS
        column's own stamp carries (fleet-written ones carry none, review
        finding). The parent's skip gate keys on the SUM of these.
        """
        from zagg.column import COLUMN_ATTR, stamped_generation_key

        block = (self.attrs.get(COLUMN_ATTR) or {}).get("generation")
        return stamped_generation_key(block, self.stamp)

    def _array(self, res: int, name: str):
        """The member's open array, or ``None`` when it is absent.

        Handles are kept for the reader's life so a member read block by block
        (issue #586 phase 4) pays its metadata opens once; absence is never
        cached, and a moved stamp drops every handle (:meth:`_read`).
        """
        import zarr
        from zarr.errors import GroupNotFoundError

        key = (int(res), name)
        if key not in self._arrays:
            try:
                group = zarr.open_group(self._store, path=str(res), mode="r", zarr_format=3)
                self._arrays[key] = group[name]
            except (KeyError, FileNotFoundError, GroupNotFoundError):
                return None  # the member postdates this column: fill
        return self._arrays[key]

    def has(self, res: int, name: str) -> bool:
        """Whether the column carries ``{res}/{name}`` (metadata only)."""
        return self._array(res, name) is not None

    def _read(self, res: int, name: str, cells: slice) -> np.ndarray | None:
        for _attempt in range(3):
            before = self.stamp
            array = self._array(res, name)
            values = None if array is None else array[cells]
            after, attrs = self._root()
            if after is not None and _same_stamp(before, after):
                self._served = True
                return values
            self._foreign_guard(after)
            if self._served:
                raise ColumnMovedError(
                    f"column {self.path} was rewritten after this fold read from it "
                    f"(stamp {'gone' if after is None else 'moved'})"
                )
            logger.info(f"stage sweep: column {self.path} moved mid-read; re-reading")
            self.stamp, self.attrs = after, attrs
            self._arrays.clear()
            self.revalidated += 1
        raise ColumnMovedError(f"column {self.path} stamp kept moving across re-reads")

    def read(self, res: int, name: str) -> np.ndarray | None:
        """One group array, stamp-validated; ``None`` for an absent member.

        An absent FIELD or an absent GROUP both read ``None`` — schema (or
        declaration) evolution: the member postdates this column, and its
        cells contribute fill until the leaf re-runs (review finding: a
        deepened ``overviews`` declaration over existing columns must
        under-cover, never abort the sweep). Every re-read re-runs the
        foreign-fresh guard: a column rewritten mid-read by a FOREIGN sweep
        is the exact race the backstop exists for (review finding).
        """
        return self._read(res, name, slice(None))

    def read_range(self, res: int, name: str, start: int, stop: int) -> np.ndarray | None:
        """Cells ``[start, stop)`` of one group array — :meth:`read`, for a block.

        What the chunk-streamed fold reads (:mod:`zagg.sweep_fold`): only the
        chunk objects covering the range are fetched, under the same stamp
        validation and foreign-fresh guard as a whole-member read.
        """
        return self._read(res, name, slice(int(start), int(stop)))


class _OverviewReader(_ColumnReader):
    """Stamp-validated reads over one ladder overview — a cascade's source.

    Everything :class:`_ColumnReader` gives a child column — one root GET for
    stamp + attrs, optimistic re-validation around every read, the
    foreign-fresh abort — over a ``zagg-overview/2`` artifact, whose
    generation block and fold depth live in its own attrs key. What a merge
    level folds (its children's artifacts at the next finer level) and what
    the all-time fold folds (the node's per-window overviews).
    """

    @property
    def provenance(self) -> dict:
        from zagg.sweep_overview import OVERVIEW_ATTR

        block = self.attrs.get(OVERVIEW_ATTR)
        return block if isinstance(block, dict) else {}

    def generation(self) -> tuple:
        from zagg.column import stamped_generation_key

        return stamped_generation_key(self.provenance.get("generation"), self.stamp)


class _FoldedSource(_OverviewReader):
    """A level this unit has just folded, served to the next coarser merge from memory.

    The same reader surface over the fold's slabs, so a ``[2,1,0]`` unit
    reads its order-3 children's artifacts once and folds order 1 from the
    order-2 slabs it just wrote, order 0 from order 1's — byte-what a reader
    opening those artifacts back off the store would serve, since the stamp
    and attrs are the ones the writer has just recorded.
    """

    def __init__(self, path: str, res: int, fold: dict, run_id: str):
        from zagg.sweep_overview import OVERVIEW_ATTR

        self.path, self.revalidated, self.res, self.slabs = path, 0, int(res), fold["slabs"]
        self.attrs = {
            OVERVIEW_ATTR: {
                "generation": fold["generation"],
                "merges_from_raw": fold["merges_from_raw"],
            }
        }
        self.stamp = {
            "granule_count": fold["granule_count"],
            "time_range": fold["time_range"],
            "run_id": run_id,
        }

    def _array(self, res: int, name: str):
        return self.slabs.get(name) if int(res) == self.res else None

    def _read(self, res: int, name: str, cells: slice):
        array = self._array(res, name)
        return None if array is None else array[cells]


def _readers_for(
    store,
    children: list,
    windows: list,
    *,
    run_id: str,
    run_started: str,
    store_kwargs: dict,
    counts: dict,
) -> dict:
    """``{child decimal: [reader-or-None per window]}``, stamp-gated.

    An absent or unstamped column reads ``None`` — under-coverage, recorded
    by the caller (the soft-barrier posture: fold what is on disk, loudly).
    An unreadable one counts ``failed`` and also reads ``None``. Every column
    is read through ``store``, the invoke's one handle (issue #610).
    """
    from zagg.column import column_name
    from zagg.sweep import _node_rel

    readers: dict = {}
    for child in children:
        row = []
        for window in windows:
            path = f"{_node_rel(child)}/{column_name(window)}"
            try:
                reader = _ColumnReader(
                    path,
                    run_id=run_id,
                    run_started=run_started,
                    store_kwargs=store_kwargs,
                    store=store,
                )
            except ForeignSweepError:
                raise
            except Exception as e:
                logger.warning(f"stage sweep: unreadable column {path} ({e})")
                counts["failed"] += 1
                row.append(UNREADABLE)
                continue
            row.append(reader if reader.committed else None)
        readers[child] = row
    return readers


def _summed_generation(rows: list) -> dict:
    """The ratchet key: child generations summed (skip-if-current basis).

    ``run_ids`` is the union over the contributing children — issue #417's
    term; see :func:`zagg.column.generation_key`.
    """
    n, stamps, runs = 0, [], set()
    for row in rows:
        for reader in row or ():
            if _is_reader(reader):
                count, timestamp, run_ids = reader.generation()
                n += count
                stamps.append(timestamp)
                runs.update(run_ids)
    stamps = [t for t in stamps if t is not None]
    return {
        "n_leaves": int(n),
        "max_leaf_timestamp": max(stamps) if stamps else None,
        "run_ids": sorted(runs),
    }


def _dense_rows(readers: dict, node: str, *, depth: int) -> list:
    """The full rank range of ``node``'s children: reader rows or ``None``."""
    from zagg.sweep_overview import _rel_rank

    rows: list = [None] * (4**depth)
    for child, row in readers.items():
        if child.startswith(node) and len(child) == len(node) + depth:
            rows[_rel_rank(child, node)] = row
    return rows


def _gather_depth(rows: list, res: int) -> int:
    """A gather's ``merges_from_raw``: the deepest its source columns record at ``res``.

    A gather copies the child columns' group at ``res`` untouched, so it is at
    their depth: 1 at or above the raw-fold boundary, 2 below it (issue #538,
    :func:`zagg.column.member_merges_from_raw`) — every artifact records the
    depth of what it consumed.
    """
    from zagg.column import COLUMN_ATTR

    depths = []
    for row in rows:
        for reader in row or ():
            if _is_reader(reader):
                groups = (reader.attrs.get(COLUMN_ATTR) or {}).get("groups") or {}
                depths.append(int((groups.get(str(res)) or {}).get("merges_from_raw") or 1))
    return max(depths, default=1)


def _source_depth(rows: list) -> int:
    """A merge's ``merges_from_raw``: one more than the deepest source folded."""
    depths = [
        int(reader.provenance.get("merges_from_raw") or 1)
        for row in rows
        if row is not None
        for reader in row
        if isinstance(reader, _OverviewReader)
    ]
    return 1 + max(depths, default=1)


def refold_on_move(fold, refresh, what: str, *, retry_on=(ColumnMovedError,)):
    """``fold()``, once more after ``refresh()`` if it raised one of ``retry_on``.

    ``refresh`` replaces the artifact's readers with fresh ones, so the second
    attempt reads every source from scratch, each under one stamp. A second
    failure propagates: the caller counts the artifact failed and moves on.
    :class:`ForeignSweepError` always propagates at once.
    """
    try:
        return fold()
    except ForeignSweepError:
        raise
    except retry_on as e:
        logger.info(f"stage sweep: {what} is folded again from fresh readers ({e})")
    refresh()
    return fold()


def block_cells() -> int:
    """Cells per fold block (read at call time — the tests narrow it)."""
    return 4 ** int(STAGE_BLOCK_ORDER)


class FoldMeter:
    """Resident-input accounting for one unit's folds (issue #586 phase 4).

    Counts the source cells a fold block has read and not yet released; the
    block's inputs are released together when its outputs are handed on, so
    :attr:`peak_cells` is the largest single block's inputs — the number the
    streamed fold exists to bound, and what the stage record reports
    (``fold_peak_cells``) so a fleet run can be read against it.
    """

    def __init__(self):
        self.blocks = 0
        self.cells_read = 0
        self.peak_cells = 0
        self._held = 0

    def hold(self, values) -> None:
        if values is None:
            return
        n = int(len(values))
        self._held += n
        self.cells_read += n
        self.peak_cells = max(self.peak_cells, self._held)

    def release(self) -> None:
        """A block's outputs were handed on: its inputs are no longer held."""
        self.blocks += 1
        self._held = 0


def _fetch(reader, res: int, name: str, rng, meter):
    """One member read: whole (``rng is None``) or the ``[a, b)`` cell range."""
    values = reader.read(res, name) if rng is None else reader.read_range(res, name, *rng)
    if meter is not None:
        meter.hold(values)
    return values


def _companion_group(reader, res: int, name: str, declared: list, rng=None, meter=None):
    """One contributor's ``{array: values}`` at ``res``, or ``None`` if refused.

    A field and EVERY companion it declares are read together and validated
    before any of them is used (issue #410). ``_ColumnReader.read`` returns
    ``None`` per ARRAY, not per member — its documented schema-evolution
    contract — so a column written before ``location:``/``temporal:`` joined the
    declaration reads its payload fine and its siblings as ``None``. That is not
    under-coverage: the words are exact only *given* the centroid partition the
    payload describes (spec §9.1/§8.3/§2.3), so a payload gathered or folded
    without its words is **corruption**, not a missing member. All absent
    together is the honest schema-evolution case and reads as fill.

    Returns ``None`` for a partial group; the caller counts the contributor
    unreadable and skips it, which is the posture
    ``sweep_overview._fold_node`` takes at a leaf missing a sibling (skip
    loudly, ``failed += 1``) rather than aborting the level — a single stale
    child column must not take a whole stage down.

    ``rng`` is the ``[a, b)`` cell range of a block read (``None`` reads the
    member whole). Presence is a property of the ARRAY, not of the range, so
    every block of one contributor reaches the same verdict.
    """
    arrays = {name: _fetch(reader, res, name, rng, meter)}
    for _kwarg, sibling in declared:
        arrays[sibling] = _fetch(reader, res, sibling, rng, meter)
    present = [k for k, v in arrays.items() if v is not None]
    if not present or len(present) == len(arrays):
        return arrays
    logger.warning(
        f"stage sweep: column {reader.path} carries {sorted(present)} at resolution {res} "
        f"but not {sorted(set(arrays) - set(present))}; counting the contributor "
        f"unreadable rather than writing a payload and channels that describe different "
        f"partitions (spec §9.1/§8.3, §1.1)"
    )
    return None


def _source_counts(rows: list, broken=()) -> tuple:
    """``(folded, missing, unreadable)`` over the dense child range.

    A child counts **folded** when at least one of its windows delivered a
    usable column, **missing** when every window's column is cleanly absent
    (never generated, or a fleet still in flight), and **unreadable**
    otherwise — the distinction the two counters must not launder into one.

    ``broken`` is the set of ``(child index, window index)`` contributors a
    fold refused MID-READ: a located payload present without its §9 channel
    (:func:`_companion_group`). Such a contributor is not usable, exactly as an
    unreadable column is not, so it is classified here rather than counted as
    a clean fold — the counters are what ``source_children`` records, and an
    artifact folded without a contributor must say so (spec §4.5).
    """
    folded = missing = unreadable = 0
    for i, row in enumerate(rows):
        if row is None:
            continue
        if any(_is_reader(r) and (i, w) not in broken for w, r in enumerate(row)):
            folded += 1
        elif all(r is None for r in row):
            missing += 1
        else:
            unreadable += 1
    return folded, missing, unreadable


def _companions(fields: dict) -> dict:
    """``{field: [(kernel kwarg, sibling array), ...]}`` for the fields that declare any."""
    from zagg.sweep_overview import field_companions

    return {
        name: field_companions(name, meta)
        for name, meta in fields.items()
        if field_companions(name, meta)
    }


def broken_groups(rows: list, fields: dict, *, res: int) -> set:
    """The gather contributors :func:`_companion_group` will refuse at ``res``.

    Presence only — one metadata open per array, which the block reads then
    reuse — so a writer can know a member's ``source_children`` BEFORE it
    streams a single block (:func:`zagg.sweep_stage.write_stage_column` must
    not clear an existing column for a member that folds nothing).
    """
    broken: set = set()
    for i, row in enumerate(rows):
        reader = row[0] if row is not None else None
        if not _is_reader(reader):
            continue
        for name, declared in _companions(fields).items():
            present = [reader.has(res, n) for n in (name, *(sib for _kw, sib in declared))]
            if any(present) and not all(present):
                broken.add((i, 0))
    return broken


# ---------------------------------------------------------------------------
# The gather: concatenation of child members, one block of output cells at a
# time.
# ---------------------------------------------------------------------------


def _gather_block(rows, fields, companions, *, res, span, lo, hi, broken, meter) -> dict:
    """Output cells ``[lo, hi)`` of a gather: each covering child's own range."""
    from zagg.sweep_overview import _empty_slab

    slabs = {name: _empty_slab(meta, hi - lo) for name, meta in fields.items()}
    for declared in companions.values():
        for _kwarg, sibling in declared:
            slabs[sibling] = np.full(hi - lo, b"", dtype=object)
    for i in range(lo // span, min(len(rows), -(-hi // span))):
        row = rows[i]
        if row is None:
            continue
        reader = row[0]
        if not _is_reader(reader):
            continue
        a, b = max(lo, i * span), min(hi, (i + 1) * span)
        # A block that covers the child whole reads it whole — the one read a
        # single-chunk member has, and the shape the pre-streaming fold used.
        rng = None if (a, b) == (i * span, (i + 1) * span) else (a - i * span, b - i * span)
        grouped: dict = {}
        skip: set = set()
        for name, declared in companions.items():
            group = _companion_group(reader, res, name, declared, rng, meter)
            if group is None:
                # EVERY half stays fill: the child's span for this field reads
                # as under-covered, never as a payload with an absent channel.
                broken.add((i, 0))
                skip |= {name, *(sib for _kw, sib in declared)}
                continue
            grouped.update(group)
        for name in list(slabs):
            if name in skip:
                continue
            values = grouped[name] if name in grouped else _fetch(reader, res, name, rng, meter)
            if values is not None:
                slabs[name][a - lo : b - lo] = values
    return slabs


def iter_gather(rows, fields, *, res, span, n_out, broken, block=None, meter=None):
    """Yield ``(lo, hi, slabs)`` for a gather, one block of output cells each.

    The streamed form the stage column writer drives: it writes each block as
    one chunk and drops it. ``broken`` is the caller's accumulator — the
    ``(child, window)`` contributors refused mid-read, which feed
    :func:`_source_counts` once the last block is in.
    """
    companions = _companions(fields)
    step = block_cells() if block is None else int(block)
    for lo in range(0, n_out, step):
        hi = min(lo + step, n_out)
        slabs = _gather_block(
            rows, fields, companions, res=res, span=span, lo=lo, hi=hi, broken=broken, meter=meter
        )
        yield lo, hi, slabs
        if meter is not None:
            meter.release()


def _joined(parts: list) -> dict:
    """Per-block slabs joined along the cells axis (one block: returned as is)."""
    if len(parts) == 1:
        return parts[0]
    return {name: np.concatenate([p[name] for p in parts]) for name in parts[0]}


def _gather_slabs(
    rows: list, fields: dict, *, res: int, span: int, n_out: int, block=None, meter=None
) -> tuple:
    """Concatenate child members at ``res`` — gen-1 content, untouched.

    ``rows`` is the DENSE rank-ordered child range: ``None`` for a child no
    candidate leaf inhabits (genuinely empty — fill, uncounted), else the
    child's reader row (gathers are per-window by construction, one reader,
    itself ``None`` when the candidate's column is missing — under-coverage,
    counted). Payloads are ASSIGNED, never decoded or re-folded — the
    acceptance contract that gather levels carry gen-1 bytes untouched.
    Returns ``(slabs, folded, missing, unreadable, demotions)`` — the last
    always empty here because a gather FOLDS nothing, so the packed rail has
    no site to fire in; NOT because a gather cannot relay the half-paired
    shape the rail exists for. It can, and the residual is the laundering
    case named at the return.

    A located field's sibling is gathered under the same rule as its payload
    (ruling 4 on issue #410): a gather ASSIGNS gen-1 bytes, so the pair stays
    row-aligned by construction — **given that both arrays are present**, which
    is the one thing the read does not establish. The pair is therefore read
    and validated (:func:`_companion_group`) BEFORE anything is assigned, so a
    child carrying one half contributes neither and is counted unreadable
    rather than half-applied into the span.

    The whole level, assembled from :func:`iter_gather`'s blocks: what a
    ladder overview (one zarr chunk) is written from. The inputs are held one
    block at a time; the output is the level.
    """
    broken: set = set()
    parts = [
        slabs
        for _lo, _hi, slabs in iter_gather(
            rows, fields, res=res, span=span, n_out=n_out, broken=broken, block=block, meter=meter
        )
    ]
    # A gather ASSIGNS gen-1 bytes, so the packed guard rail — a fold-site
    # check — has no site to fire in and the demotions slot is empty (issue
    # #518), kept so every fold arm returns one shape. Empty because nothing
    # folds here, NOT because a gather cannot produce the half-paired shape
    # the rail exists for: a source column carrying the ``of`` digest without
    # the word relays a PRESENT, all-fill word array beside a populated
    # divisor (the packed pair is not validated the way ``_companion_group``
    # validates a located one), which the next rung's :func:`cascade_fold`
    # then reads as legitimate ``(0, n)`` parts — diluting the lane fractions
    # instead of blanking them, with no rail fired and nothing in any attrs.
    # That relay laundering predates issue #518 and stands as a question on
    # its PR; the fix belongs beside ``_companion_group``'s
    # pairing logic, not in this observability path (review finding).
    return (_joined(parts), *_source_counts(rows, broken), [])


# ---------------------------------------------------------------------------
# The cascade: a k-way fold of the children's cells, one block of output
# cells at a time.
# ---------------------------------------------------------------------------


def _pieces(rows, *, src_per_child, s_lo, s_hi, step):
    """``(child, row, a, b)`` — child-local source ranges covering ``[s_lo, s_hi)``.

    In rank order, each at most ``step`` cells: the reads one merge block
    makes. ``step``, ``src_per_child`` and every merge factor are powers of
    four, so a piece starts on a chunk boundary of the child's own member and
    a chunk is never read twice.
    """
    for i in range(s_lo // src_per_child, min(len(rows), -(-s_hi // src_per_child))):
        row = rows[i]
        if row is None:
            continue
        c_lo = max(s_lo, i * src_per_child) - i * src_per_child
        c_hi = min(s_hi, (i + 1) * src_per_child) - i * src_per_child
        for a in range(c_lo, c_hi, step):
            yield i, row, a, min(a + step, c_hi)


def _range(a: int, b: int, src_per_child: int):
    """The read range of a piece: ``None`` (whole member) when it is the child."""
    return None if (a, b) == (0, src_per_child) else (a, b)


def _merge_block(
    rows, fields, *, res_src, src_per_child, factor, lo, hi, windows, step, state, meter
) -> dict:
    """Output cells ``[lo, hi)`` of a merge, from the sources that cover them."""
    from zagg.stats.composition import merge_composition_kway
    from zagg.sweep_overview import (
        DEMOTION_DIVISOR_MISSING,
        DEMOTION_WORD_MISSING,
        _empty_slab,
        combine_dense,
        decode_digest,
        field_companions,
        fold_dense,
        fold_digests,
        note_demotion,
        overview_fold_delta,
        payload_weight,
    )

    broken, demoted, noted = state["broken"], state["demoted"], state["noted"]
    s_lo, s_hi = lo * factor, hi * factor
    span = dict(src_per_child=src_per_child, s_lo=s_lo, s_hi=s_hi, step=step)
    slabs: dict = {}
    for name, meta in fields.items():
        if meta["class"] != "exact":
            continue
        law, fill = meta.get("method"), meta.get("fill_value", "NaN")
        out = None
        for w in range(windows):
            acc = _empty_slab(meta, s_hi - s_lo)
            for i, row, a, b in _pieces(rows, **span):
                reader = row[w]
                if not _is_reader(reader):
                    continue
                values = _fetch(reader, res_src, name, _range(a, b, src_per_child), meter)
                if values is not None:
                    base = i * src_per_child - s_lo
                    acc[base + a : base + b] = values
            part = fold_dense(acc, factor, law, fill)
            out = part if out is None else combine_dense(out, part, law, fill)
        slabs[name] = out
    for name, meta in fields.items():
        if meta["class"] != "approximate":
            continue
        dtype = meta.get("dtype") or "float32"
        inner = tuple(meta.get("inner_shape") or (2,))
        delta = overview_fold_delta(meta)
        # A located field folds its sibling in the SAME k-way call as its
        # payload (ruling 4 on issue #410): the merged words are keyed on the
        # centroid partition that merge produces (spec §9.1), so the two are
        # accumulated together per open cell and folded together. A contributor
        # carrying one half of the pair is SKIPPED loudly and counted
        # unreadable — the gather's posture, and ``_fold_node``'s at a leaf
        # missing the sibling — never folded payload-only, and never a raise
        # that takes the whole level down over one stale child column.
        declared = field_companions(name, meta)
        out = np.full(hi - lo, b"", dtype=object)
        sibling_slabs = {kwarg: np.full(hi - lo, b"", dtype=object) for kwarg, _ in declared}
        pending: dict[int, list] = {}
        # {kernel kwarg: {open cell: [word vectors]}} — accumulated in lockstep
        # with ``pending`` so a cell closes with its payload and every channel.
        words: dict[str, dict[int, list]] = {kwarg: {} for kwarg, _ in declared}

        def _close(
            j,
            out=out,
            delta=delta,
            dtype=dtype,
            declared=declared,
            pending=pending,
            words=words,
            sibling_slabs=sibling_slabs,
        ):
            cell = pending.pop(j)
            if not declared:
                out[j - lo] = fold_digests(cell, delta=delta, dtype=dtype)
                return
            payload, *encoded = fold_digests(
                cell,
                delta=delta,
                dtype=dtype,
                channels={kwarg: words[kwarg].pop(j) for kwarg, _ in declared},
            )
            out[j - lo] = payload
            for (kwarg, _), value in zip(declared, encoded, strict=True):
                sibling_slabs[kwarg][j - lo] = value

        for i, row, a, b in _pieces(rows, **span):
            base = i * src_per_child
            for w, reader in enumerate(row):
                if not _is_reader(reader):
                    continue
                group = _companion_group(
                    reader, res_src, name, declared, _range(a, b, src_per_child), meter
                )
                if group is None:
                    broken.add((i, w))
                    continue
                slab = group[name]
                if slab is None:
                    continue
                for pos, payload in enumerate(slab):
                    if payload is None or not len(payload):
                        continue
                    key = (base + a + pos) // factor
                    pending.setdefault(key, []).append(decode_digest(payload, dtype, inner))
                    for kwarg, sibling in declared:
                        words[kwarg].setdefault(key, []).append(
                            decode_digest(group[sibling][pos], "uint64", ())
                        )
            # Cells wholly covered by the sources read so far are complete:
            # fold and free them, so resident state never exceeds the open
            # boundary.
            done = (base + b) // factor
            for j in [j for j in pending if j < done]:
                _close(j)
        for j in list(pending):
            _close(j)
        slabs[name] = out
        for kwarg, sibling in declared:
            slabs[sibling] = sibling_slabs[kwarg]
    for name, meta in fields.items():
        if meta["class"] != "packed":
            continue
        # The packed composition fold (issue #515, spec §3.4): each source
        # cell contributes its ``(word, n)`` pair, ``n`` being the ``of``
        # digest's weight at the same cell, and every output cell collapses in
        # ONE k-way call (single quantization). A contributor carrying one
        # half of the pair is SKIPPED for this field and the demotion recorded
        # (issue #518, spec §4.3) — the word is uninterpretable without its
        # divisor digest, and a divisor without its word says nothing. Its
        # other fields still fold: the rail is per FIELD, not a verdict on the
        # contributor (the one rule of both sweeps since issue #620).
        #
        # Skipping alone does NOT keep the pair consistent in ONE of the two
        # directions, and that is the difference from the located pair above:
        # there both halves are the same field's, so refusing one refuses both.
        # Here the divisor is a DIFFERENT declared field, folded by the digest
        # loop above, which knows nothing about this one — so a contributor
        # carrying the DIVISOR but not the word lands in the level's
        # ``N_signal`` while its word is excluded, and a reader doing the §3.3
        # recovery divides by a denominator the word never covered (a ~29%
        # skew, reproduced on review). Absence over wrongness: every output
        # cell that contributor's span covers keeps the fill word ``0``, which
        # makes no §3.2 presence/fraction claim, while the digest itself stays
        # correct on its own.
        #
        # The REVERSE direction poisons nothing: when the divisor is the
        # missing half, the digest loop above reads that same array for the
        # ``of`` field itself and drops the contributor too (``slab is None``),
        # so word and ``N_signal`` already exclude the same rows. The record's
        # ``cells`` is keyed there only where the contributor OWNS its span —
        # one window and no output cell shared between children, the cascade's
        # shape — since a shared cell is still covered by its siblings.
        #
        # In the skew direction the blanking IS wider than the offending
        # contributor where output cells are shared (the all-time fold across
        # windows), so poisoning it drops SIBLING contributions as well. Left
        # by design — a shared cell whose folded ``N_signal`` counts rows no
        # surviving word describes cannot carry an honest word, and blanking
        # beats skewing.
        #
        # Blocks: a contributor's verdict is a property of its ARRAYS, so
        # every block its span reaches re-derives the same poisoned cells; the
        # demotion record counts it once (``noted``), however many blocks it
        # crossed.
        of_name = meta.get("of")
        of_dtype = (fields.get(of_name) or {}).get("dtype") or "float32"
        out = _empty_slab(meta, hi - lo)
        parts_by_cell: dict[int, list] = {}
        poisoned: set[int] = set()
        disjoint = windows == 1 and factor <= src_per_child
        for i, row, a, b in _pieces(rows, **span):
            base = i * src_per_child
            for w, reader in enumerate(row):
                if not _is_reader(reader):
                    continue
                rng = _range(a, b, src_per_child)
                word_slab = _fetch(reader, res_src, name, rng, meter)
                of_values = _fetch(reader, res_src, of_name, rng, meter)
                if word_slab is None or of_values is None:
                    if (word_slab is None) != (of_values is None):
                        first = (name, i, w) not in noted
                        noted.add((name, i, w))
                        if first:
                            logger.warning(
                                f"fold: source {reader.path} carries only one of "
                                f"{name!r}/{of_name!r} at resolution {res_src}; the field "
                                f"folds short of it (spec §3.3, §4.3)"
                            )
                        # Either direction is a demotion the artifact must
                        # record (issue #518, spec §4.3): the level's word
                        # coverage folded short, and the bytes alone cannot
                        # say so (the fill word makes no §3.2 claim).
                        owned = range(base // factor, (base + src_per_child + factor - 1) // factor)
                        if of_values is not None:
                            poisoned.update(owned)
                            if first:
                                note_demotion(
                                    demoted, name, DEMOTION_WORD_MISSING, of_name, cells=owned
                                )
                        elif first:
                            note_demotion(
                                demoted,
                                name,
                                DEMOTION_DIVISOR_MISSING,
                                of_name,
                                cells=owned if disjoint else None,
                            )
                    continue
                for pos in range(len(word_slab)):
                    n = payload_weight(of_values[pos], of_dtype)
                    if n > 0:
                        parts_by_cell.setdefault((base + a + pos) // factor, []).append(
                            (int(word_slab[pos]), n)
                        )
        # Poisoned cells drop out HERE, after accumulation: every surviving
        # cell still folds its parts in one k-way call (single quantization).
        for j, parts in parts_by_cell.items():
            if j not in poisoned:
                out[j - lo] = merge_composition_kway(parts)
        slabs[name] = out
    return slabs


def merge_level(
    rows: list,
    fields: dict,
    *,
    res_src: int,
    src_per_child: int,
    factor: int,
    n_out: int,
    block=None,
    meter=None,
) -> tuple:
    """One k-way level, block by block: ``(slabs, broken, demotions)``.

    The kernel under :func:`cascade_fold`, without the per-child roll-up —
    ``broken`` is the raw ``(child, window)`` set the fold refused, for a
    caller whose coverage counters are per WINDOW (the all-time fold, where
    the one "child" is the node itself and the windows fold across,
    ``factor == 1`` — :func:`zagg.sweep_units.close_node`).
    """
    from zagg.sweep_overview import demotion_records

    windows = next((len(row) for row in rows if row is not None), 1)
    step = block_cells() if block is None else int(block)
    out_step = max(1, step // int(factor))
    state: dict = {"broken": set(), "demoted": {}, "noted": set()}
    parts = []
    for lo in range(0, n_out, out_step):
        parts.append(
            _merge_block(
                rows,
                fields,
                res_src=res_src,
                src_per_child=src_per_child,
                factor=factor,
                lo=lo,
                hi=min(lo + out_step, n_out),
                windows=windows,
                step=step,
                state=state,
                meter=meter,
            )
        )
        if meter is not None:
            meter.release()
    return _joined(parts), state["broken"], demotion_records(state["demoted"])


def cascade_fold(
    rows: list,
    fields: dict,
    *,
    k: int,
    r: int,
    src_order: int,
    res_src: int,
    block=None,
    meter=None,
) -> tuple:
    """Fold level ``(k, r)`` of one node from its children's level — THE engine.

    The one fold-of-folds every ladder path runs (issue #620; the cascade
    espg ruled on issue #376 for ``/1`` and re-ruled for the staged ``/2``
    ladder): a node's level is folded from its ``4^(src_order - k)``
    children's artifacts at order ``src_order``, each carrying ``4^(res_src -
    src_order)`` cells at resolution ``res_src``, ``4^(res_src - r)``-to-one
    into the node's ``4^(r - k)`` output cells. ``rows`` is that DENSE
    rank-ordered child range (``None`` for an uninhabited child, else
    ``[reader-or-None]`` — a :class:`_ColumnReader` over the
    child's artifact, ``None`` when it is missing, ``UNREADABLE`` when it
    could not be read). Every output cell folds in ONE k-way call over the
    child cells it covers (``merge_tdigests_kway`` is order-independent by
    its sort, #370), so the values are a fixed function of the ladder — the
    same whether the children were just folded in memory or read back off
    the store, and at any tuple grouping. The input per node is the four
    children's slabs, constant in the subtree's leaf count.

    Memory bound: one block of output cells at a time (:func:`_merge_block`),
    sized so its sources are one fold block (:func:`block_cells`) or one
    output cell's sources, whichever is larger. Inside a block the exact
    classes hold one dense source vector (scalars); the digest classes stream
    piece by piece, holding one piece plus the open output cells' decoded
    digests. Missing candidates contribute fill and are counted
    (``source_children.missing``).

    A located field's pair is read together (:func:`_companion_group`) and a
    contributor carrying one half is **skipped for that field and counted
    unreadable** — the same posture as the gather, and as
    ``sweep_overview._fold_node``'s at a leaf missing the sibling. It is not a
    raise: one stale child must not take a whole level down, and the fold is
    a fold — dropping a contributor is under-coverage the artifact records,
    where writing a payload without its words would be corruption (spec
    §9.1). A packed field's half-pair is a per-FIELD demotion (issue #518):
    the contributor still folds for its other fields and the record says
    which field folded short of it.

    Returns ``(slabs, folded, missing, unreadable, demotions)`` — the last a
    :func:`zagg.sweep_overview.demotion_records` list naming every packed
    field the half-pair rail demoted here, per direction.
    """
    slabs, broken, demotions = merge_level(
        rows,
        fields,
        res_src=int(res_src),
        src_per_child=4 ** (int(res_src) - int(src_order)),
        factor=4 ** (int(res_src) - int(r)),
        n_out=4 ** (int(r) - int(k)),
        block=block,
        meter=meter,
    )
    # The coverage counters are computed at the END, from the same
    # ``(child, window)`` classification the gather uses: a contributor whose
    # located pair this fold refuses (:func:`_companion_group`) is unreadable,
    # not a clean fold, and that is only known once the sources have been read.
    return (slabs, *_source_counts(rows, broken), demotions)
