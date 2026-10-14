"""The windowed leaf's all-time column (issue #649): its window columns, folded.

A windowed ``/2`` store's all-time fold used to exist only above the shard:
the staged sweep's close (:func:`zagg.sweep_units.close_node`) folds each
ladder node's per-window overviews into ``all.zarr``, but the shard order is
never a stage order, so the declared LEAF-column levels — the finest the
store has, the ones issue #631 added for the viewer — had no all-time member
and the ``all`` row read empty there. This module is that close one level
down, written by the leaf worker itself (espg's ruling of 2026-10-13 on the
issue: option (a1), as a hybrid): beside a shard's window columns
(``{window}.pyramid.zarr``, :mod:`zagg.column`) it writes ``all.pyramid.zarr``,
every column resolution folded cell for cell across the windows under the
SAME k-way law the sweep applies above — :func:`zagg.sweep_fold.merge_level`
at ``factor == 1`` over ONE row of window sources, in ascending window order
— so the bytes are what the close would have written had the shard been a
node. There is no second merge path: exact classes reduce through
``fold_dense``/``combine_dense``, approximate ones through ``fold_digests``
(their companion channels in the same call), packed words through the §3.4
composition merge, all inside the one kernel.

The inputs are hybrid, per window: the slabs this invoke has just folded
(:class:`_ResidentColumn` — the bulk ``unit: shard`` build, where every
window is resident and nothing is read), else the window's stored column read
back (:class:`_StoredColumn` — a window the identity gate skipped as
current, one an append did not touch). :func:`window_row` builds the row from both kinds;
one fold, one writer, the same bytes either way, because a resident slab IS
the stored member's value. An invoke that wrote nothing (every window
current) re-folds only when the member is absent, short, or older than a
window column it folds (:func:`member_current`). The fan-out ``unit: window``
invoke does not close: its units of one shard run concurrently and would tear
the member, so it clears it instead (PR #666 review finding (1)) — a
``unit: window`` store carries no leaf all-time member until the maintainer
rules how it gets one.

Provenance (spec §4.6, the issue's carve-out): the member is a
``zagg-column/1`` artifact under the reserved ``all`` token, its groups keep
the ``leaf-column`` regime and record ``merges_from_raw`` **one more than
the deepest window member folded** at that group (2 at or above the raw-fold
boundary, 3 below it — the rule the stage close applies to its sources,
:func:`zagg.sweep_fold._gather_depth` + 1), and the artifact carries
``source_windows`` — ``{folded, missing, unreadable}`` over the window
columns — as the sweep's ``all.zarr`` does, a window refused at any group
counting ``unreadable`` (:func:`write_all_time_column`). The commit stamp
carries the token and the folded windows' time-range union, as the sweep's
does.
"""

from __future__ import annotations

import logging
import time

from zagg.sweep_fold import _ColumnReader, _gather_depth, _is_reader, merge_level

logger = logging.getLogger(__name__)


class _ResidentColumn(_ColumnReader):
    """A window column this invoke has just folded, served to the all-time fold from memory.

    The reader surface :func:`zagg.sweep_fold.merge_level` consumes
    (``read``/``read_range`` per member, ``stamp``, the ``zagg_column`` groups'
    depth in ``attrs``) over the fold :func:`zagg.column.write_leaf_column`
    has just written — the window's exact column values, with no read-back
    — mirroring :class:`zagg.sweep_fold._FoldedSource` one tier down: no
    store is opened and no stamp validated, so the base constructor is not
    run.
    """

    def __init__(
        self,
        path: str,
        folded: dict,
        *,
        node_order: int,
        cell_order: int,
        granule_count,
        time_range,
    ):
        from zagg.column import COLUMN_ATTR, member_merges_from_raw, raw_fold_boundary

        self.path, self.revalidated = path, 0
        self._folded = {int(res): slabs for res, slabs in folded.items()}
        boundary = raw_fold_boundary(node_order, cell_order, self._folded)
        self.attrs = {
            COLUMN_ATTR: {
                "groups": {
                    str(res): {"merges_from_raw": member_merges_from_raw(res, boundary)}
                    for res in self._folded
                }
            }
        }
        self.stamp = {"granule_count": int(granule_count or 0), "time_range": time_range}

    def _array(self, res: int, name: str):
        return self._folded.get(int(res), {}).get(name)

    def _read(self, res: int, name: str, cells: slice):
        array = self._array(res, name)
        return None if array is None else array[cells]


class _StoredColumn(_ColumnReader):
    """A window column read back from the store for the all-time fold.

    The sweep's stamp-validated reader over one ``{label}.pyramid.zarr`` —
    one root GET for stamp and attrs, optimistic re-validation around every
    member read — opened on its own path (the leaf worker holds no store
    handle at the root). Without the foreign-sweep abort: the worker is not
    a sweep, holds no lease and runs beside the fleet by design (§4.8), and
    a fleet column's stamp carries no ``run_id`` for the guard to read; a
    column rewritten under it mid-read still raises ``ColumnMovedError``
    through the base class, which fails the close (the shard's error, and
    the retry re-reads).
    """

    def __init__(self, path: str, *, store_kwargs: dict | None = None):
        super().__init__(path, run_id=None, run_started="", store_kwargs=dict(store_kwargs or {}))

    def _foreign_guard(self, stamp) -> None:
        return None


def window_row(store_root: str, shard_key, labels, resident: dict, *, store_kwargs=None) -> list:
    """One source per window label, ascending: resident when held, else the stored column.

    ``resident`` maps the labels this invoke wrote to their
    :class:`_ResidentColumn`; every other label is read back as a
    :class:`_StoredColumn` — a committed one is a source, an uncommitted or
    absent one reads ``None`` (``missing``: the window is known to the shard
    but its column is not there), one whose root cannot be read is
    :data:`zagg.sweep_fold.UNREADABLE` (the sweep's ``_readers_for`` posture,
    one tier down). Ascending label order is the close law's fold order.
    """
    from zagg.column import column_name
    from zagg.hive import shard_leaf_path
    from zagg.sweep_fold import UNREADABLE

    node_prefix = shard_leaf_path(store_root, shard_key).rstrip("/").rsplit("/", 1)[0]
    row: list = []
    for label in sorted(labels):
        if label in resident:
            row.append(resident[label])
            continue
        path = f"{node_prefix}/{column_name(label)}"
        try:
            reader = _StoredColumn(path, store_kwargs=store_kwargs)
        except Exception as e:
            logger.warning(f"all-time column: unreadable window column {path} ({e})")
            row.append(UNREADABLE)
            continue
        row.append(reader if reader.committed else None)
    return row


def member_current(store_root: str, shard_key, row: list, *, store_kwargs=None) -> bool:
    """Whether the standing ``all.pyramid.zarr`` already folds exactly ``row``.

    The skip gate of an invoke that wrote nothing (every window the identity
    gate found current): the member is current when it is committed, folded
    every window of ``row`` and nothing else (``source_windows`` says
    ``folded == len(row)`` with no ``missing`` or ``unreadable`` — a short
    fold is re-folded so the next run heals it), and is stamped no earlier
    than any column it folds. Stamps resolve to one second, so a column
    rewritten in the member's own second by a writer that does not clear the
    member reads as current (the issue #417 blind spot; every zagg writer of
    a window column clears or rewrites the member in the same invoke).
    Anything unreadable is not current.
    """
    from zagg.column import COLUMN_ATTR, column_name
    from zagg.hive import shard_leaf_path

    node_prefix = shard_leaf_path(store_root, shard_key).rstrip("/").rsplit("/", 1)[0]
    try:
        member = _StoredColumn(f"{node_prefix}/{column_name(None)}", store_kwargs=store_kwargs)
    except Exception as e:
        logger.warning(f"all-time column: standing member unreadable ({e}); re-folding")
        return False
    if not member.committed or not all(_is_reader(entry) for entry in row):
        return False
    block = member.attrs.get(COLUMN_ATTR)
    counters = (block.get("source_windows") or {}) if isinstance(block, dict) else {}
    if (counters.get("folded"), counters.get("missing"), counters.get("unreadable")) != (
        len(row),
        0,
        0,
    ):
        return False
    stamped = str(member.stamp.get("written_at") or "")
    return bool(stamped) and all(
        str(entry.stamp.get("written_at") or "") <= stamped for entry in row
    )


def fold_all_time(row: list, fields: dict, *, node_order: int, resolutions) -> tuple:
    """``(folded, depth, broken, demotions)`` over ``row``.

    ``folded`` is ``{res: {field: slab}}``, ``depth`` ``{res: merges_from_raw}``
    over the windows folded AT that group, ``broken`` ``{res: windows}`` the
    kernel refused there (review finding (8)), and ``demotions`` ``{res:
    records}`` — the packed rail's :func:`zagg.sweep_overview.demotion_records`
    at each group where it fired, and only there (review finding (7)).

    ``row`` is the shard's window sources in ascending window order — a
    :class:`_ResidentColumn` or a committed :class:`zagg.sweep_fold._ColumnReader`
    per window, ``None`` for a window with no committed column (missing),
    :data:`zagg.sweep_fold.UNREADABLE` for one that could not be opened. Each
    resolution is one :func:`zagg.sweep_fold.merge_level` call at ``factor
    1`` with the shard as the one child: output cell ``j`` is the k-way fold
    of the windows' cell ``j`` at that resolution, the sweep's close law. A
    window carrying one half of a companion pair at some group is refused
    THERE alone and returned in ``broken[res]`` (the kernel's posture — never
    folded payload-only); its other groups still fold it. A window whose column lacks a group (a declaration
    deepened since it was written) contributes fill there, the kernel's
    schema-evolution rule; a window carrying a packed field's divisor digest
    without its word (a column written before the field was declared)
    demotes the field at that group — the cells it shares keep the fill
    word — and the record says so.
    """
    folded: dict = {}
    depth: dict = {}
    broken: dict = {}
    demoted: dict = {}
    for res in sorted((int(r) for r in resolutions), reverse=True):
        n_out = 4 ** (res - int(node_order))
        slabs, bad, demotions = merge_level(
            [row], fields, res_src=res, src_per_child=n_out, factor=1, n_out=n_out
        )
        folded[res] = slabs
        broken[res] = {w for _child, w in bad}
        kept = [None if w in broken[res] else entry for w, entry in enumerate(row)]
        depth[res] = _gather_depth([kept], res) + 1
        if demotions:
            demoted[res] = demotions
        for record in demotions:
            logger.warning(
                f"all-time column at resolution {res}: packed rail fired ({record}); "
                f"the affected cells keep the fill word (spec §3.3)"
            )
    return folded, depth, broken, demoted


def write_all_time_column(
    store_root: str,
    shard_key,
    row: list,
    fields: dict,
    *,
    node_order: int,
    cell_order: int,
    resolutions,
    store_kwargs: dict | None = None,
) -> str | None:
    """Fold ``row`` and write the shard's ``all.pyramid.zarr``; returns its basename.

    A thin caller of :func:`zagg.column.write_column` under the reserved
    window token: the fold's groups, ``merges_from_raw`` one deeper than the
    window members' (and the packed rail's ``demotions`` where it fired), the
    ``source_windows`` counters, and a stamp. One stated derivation (review
    finding (8)): a window counts ``folded`` in ``source_windows`` only when
    no group refused it — one refused at ANY group counts ``unreadable``, as
    an unopenable one does, since the member under-covers it there — while
    the stamp sums the granule counts and unions the time ranges of every
    window folded at ANY group, the record whose content the bytes carry.
    (:func:`zagg.sweep_units.close_node` refuses per window over the stage's
    one group; a multi-group column needs the split.) ``None``, and nothing
    written, when no window is folded at any group — an all-time member
    folded from nothing would claim a record it does not hold.
    """
    from zagg.column import write_column
    from zagg.windows import SCHEDULE_NONE_TOKEN, union_time_range

    if not any(_is_reader(entry) for entry in row):
        return None
    folded, depth, broken, demoted = fold_all_time(
        row, fields, node_order=node_order, resolutions=resolutions
    )
    readers = [w for w, entry in enumerate(row) if _is_reader(entry)]
    used = [row[w] for w in readers if any(w not in bad for bad in broken.values())]
    if not used:
        logger.warning(f"shard {shard_key}: no window column could be folded; no all-time column")
        return None
    whole = sum(1 for w in readers if not any(w in bad for bad in broken.values()))
    missing = sum(1 for entry in row if entry is None)
    ranges = [s.stamp["time_range"] for s in used if s.stamp.get("time_range") is not None]
    return write_column(
        store_root,
        shard_key,
        folded,
        fields,
        node_order=node_order,
        cell_order=cell_order,
        window=SCHEDULE_NONE_TOKEN,
        time_range=union_time_range(*ranges) if ranges else None,
        granule_count=sum(int(s.stamp.get("granule_count") or 0) for s in used),
        store_kwargs=store_kwargs,
        # The rail's record sits in the group it fired at (spec §4.3),
        # keyed only there, so a clean fold's attrs are unchanged.
        groups={
            res: {"merges_from_raw": d, **({"demotions": demoted[res]} if res in demoted else {})}
            for res, d in depth.items()
        },
        provenance={
            "source_windows": {
                "folded": whole,
                "missing": missing,
                "unreadable": len(row) - whole - missing,
            }
        },
    )


def stored_window_labels(store_root: str, shard_key, store_kwargs: dict | None = None) -> set:
    """The window labels with a ``{label}.pyramid.zarr`` beside the shard: one delimiter LIST.

    The all-time member covers EVERY window the shard HAS, not the ones an
    invoke was dispatched — a run over new granules alone sends the windows
    those granules touch — and the store is the only place that knows
    (:func:`zagg.sweep_units.node_windows`' posture, one tier down: the column
    objects at the leaf's node prefix, the reserved ``all`` member excluded).
    Bounded and node-scoped, never a walk; a failed LIST raises, since folding
    the windows one invoke happened to hold and calling it all-time would
    publish a short fold under a name that claims the whole record.
    """
    import obstore

    from zagg.column import COLUMN_SUFFIX
    from zagg.hive import shard_leaf_path
    from zagg.store import open_object_store
    from zagg.windows import SCHEDULE_NONE_TOKEN, validate_label

    node_prefix = shard_leaf_path(store_root, shard_key).rstrip("/").rsplit("/", 1)[0]
    store = open_object_store(node_prefix, **dict(store_kwargs or {}))
    labels = set()
    for prefix in obstore.list_with_delimiter(store)["common_prefixes"]:
        name = str(prefix).rstrip("/").rsplit("/", 1)[-1]
        label = name.removesuffix(COLUMN_SUFFIX)
        if label == name or label == SCHEDULE_NONE_TOKEN:
            continue
        try:
            labels.add(validate_label(label))
        except ValueError:
            continue  # not a window's column (foreign debris)
    return labels


def close_shard(
    store_root: str,
    shard_key,
    grid,
    config,
    resident: dict,
    *,
    dispatched=(),
    fresh: bool = True,
    store_kwargs,
) -> str | None:
    """The worker seam: gate on the declaration, build the row, fold, write; the basename.

    The same gate the window columns take (:func:`zagg.column.leaf_column_plan`):
    a declaration with no leaf-node levels writes no all-time column and
    deletes any a previous declaration left (:func:`zagg.column._clear_column`
    under the token), so the member never outlives the declaration that
    made it. The windows folded are every label the shard HAS — the stored
    columns beside it (:func:`stored_window_labels`), the ``resident`` folds
    this invoke wrote, and ``dispatched``: the labels this invoke was sent
    but did not write (the identity gate skipped them), so a skipped window
    whose column is gone counts ``missing`` rather than vanishing from the
    record. ``fresh`` says the invoke wrote a window, so the member MUST be
    rewritten; an invoke that wrote nothing rewrites only when
    :func:`member_current` says the standing member is not this row's fold.
    Returns the basename when a member was written, ``None`` otherwise
    (nothing to fold, or current). Failures raise; the caller reports them
    as the unit's error.
    """
    from zagg.column import _clear_column, leaf_column_plan

    store_kwargs = dict(store_kwargs or {})
    plan = leaf_column_plan(config, grid)
    if plan is None:
        _clear_column(store_root, shard_key, None, store_kwargs)
        return None
    resolutions, fields = plan
    labels = stored_window_labels(store_root, shard_key, store_kwargs) | set(resident)
    labels |= set(dispatched)
    if not labels:
        return None
    row = window_row(store_root, shard_key, labels, resident, store_kwargs=store_kwargs)
    if not fresh and member_current(store_root, shard_key, row, store_kwargs=store_kwargs):
        return None
    return write_all_time_column(
        store_root,
        shard_key,
        row,
        fields,
        node_order=grid.parent_order,
        cell_order=grid.child_order,
        resolutions=resolutions,
        store_kwargs=store_kwargs,
    )


def _resident_sources(grid, written: dict) -> dict:
    """``{label: _ResidentColumn}`` over the windows this invoke committed a column for.

    ``written`` maps a label to its ``(_LeafUnit, metadata)``; a window with
    no column (none declared, or a benign no-data window with no leaf) has
    no source and is simply absent from the map.
    """
    from zagg.column import column_name

    return {
        label: _ResidentColumn(
            column_name(label),
            unit.column_fold,
            node_order=grid.parent_order,
            cell_order=grid.child_order,
            granule_count=m.get("granule_count", 0),
            time_range=m.get("time_range"),
        )
        for label, (unit, m) in written.items()
        if m.get("leaf_column")
    }


def _close_all_time(
    shard_key, grid, store_root, config, resident, skipped, *, fresh, sink, timed, store_kwargs
) -> None:
    """Run the shard's all-time close; its failure is ``sink``'s error, its time rides ``timed``.

    The bulk unit's close, called from :func:`zagg.hive._close_windows` (the
    fan-out unit clears the member instead): a failed close —
    the LIST, a stored column read, the fold or the write — lands in
    ``sink["error"]`` through the same channel a failed column uses, so the
    invoke fails and the dispatcher retries the unit, while the leaves and
    columns already committed keep their metadata and records; never raised
    past the telemetry envelope. The elapsed time goes to every meta in
    ``timed`` as ``phase_timings["all_time_column"]`` (review finding (4)):
    the records are built per WINDOW meta, so it rides every written window's
    phases as the invoke's ``read`` does, and ``telemetry.merge`` counts it
    once per invoke (``INVOKE_PHASES``).
    """
    _t0 = time.time()
    column = None
    try:
        column = close_shard(
            store_root,
            shard_key,
            grid,
            config,
            resident,
            dispatched=skipped,
            fresh=fresh,
            store_kwargs=store_kwargs,
        )
    except Exception as e:
        logger.error(f"all-time column write failed for shard {shard_key}: {e}")
        sink["error"] = f"all-time column: {e}"
        sink["all_time_column_error"] = str(e)
    finally:
        elapsed = time.time() - _t0
        for m in timed:
            if "phase_timings" in m:
                m["phase_timings"]["all_time_column"] = elapsed
    if column is not None:
        sink["all_time_column"] = column
