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
back (the non-resident path, phase 2 of the issue). One fold, one writer,
the same bytes either way, because a resident slab IS the stored member's
value.

Provenance (spec §4.6, the issue's carve-out): the member is a
``zagg-column/1`` artifact under the reserved ``all`` token, its groups keep
the ``leaf-column`` regime and record ``merges_from_raw`` **one more than
the deepest window member folded** at that group (2 at or above the raw-fold
boundary, 3 below it — the rule the stage close applies to its sources,
:func:`zagg.sweep_fold._gather_depth` + 1), and the artifact carries
``source_windows`` — ``{folded, missing, unreadable}`` over the window
columns — exactly as the sweep's ``all.zarr`` does. The commit stamp carries
the token and the folded windows' time-range union, as the sweep's does.
"""

from __future__ import annotations

import logging

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


def fold_all_time(row: list, fields: dict, *, node_order: int, resolutions) -> tuple:
    """``({res: {field: slab}}, {res: merges_from_raw}, broken windows)`` over ``row``.

    ``row`` is the shard's window sources in ascending window order — a
    :class:`_ResidentColumn` or a committed :class:`zagg.sweep_fold._ColumnReader`
    per window, ``None`` for a window with no committed column (missing),
    :data:`zagg.sweep_fold.UNREADABLE` for one that could not be opened. Each
    resolution is one :func:`zagg.sweep_fold.merge_level` call at ``factor
    1`` with the shard as the one child: output cell ``j`` is the k-way fold
    of the windows' cell ``j`` at that resolution, the sweep's close law. A
    window carrying one half of a companion pair at some group is refused
    there and returned in ``broken`` (the kernel's posture — never folded
    payload-only); it counts ``unreadable`` in the artifact's
    ``source_windows``. A window whose column lacks a group (a declaration
    deepened since it was written) contributes fill there, the kernel's
    schema-evolution rule.
    """
    folded: dict = {}
    depth: dict = {}
    broken: set = set()
    for res in sorted((int(r) for r in resolutions), reverse=True):
        n_out = 4 ** (res - int(node_order))
        slabs, bad, demotions = merge_level(
            [row], fields, res_src=res, src_per_child=n_out, factor=1, n_out=n_out
        )
        folded[res] = slabs
        depth[res] = _gather_depth([row], res) + 1
        broken |= {w for _child, w in bad}
        for record in demotions:
            logger.warning(
                f"all-time column at resolution {res}: packed rail fired ({record}); "
                f"the affected cells keep the fill word (spec §3.3)"
            )
    return folded, depth, broken


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
    window members', the ``source_windows`` counters, a stamp carrying the
    folded windows' granule counts summed and their time ranges unioned (the
    stage close's own derivation, :func:`zagg.sweep_stage._fold_result`).
    ``None``, and nothing written, when no window source is a committed
    column — there is nothing to fold, and an all-time member folded from
    nothing would claim a record it does not hold.
    """
    from zagg.column import write_column
    from zagg.windows import SCHEDULE_NONE_TOKEN, union_time_range

    if not any(_is_reader(entry) for entry in row):
        return None
    folded, depth, broken = fold_all_time(
        row, fields, node_order=node_order, resolutions=resolutions
    )
    used = [entry for w, entry in enumerate(row) if _is_reader(entry) and w not in broken]
    if not used:
        logger.warning(f"shard {shard_key}: no window column could be folded; no all-time column")
        return None
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
        groups={res: {"merges_from_raw": d} for res, d in depth.items()},
        provenance={
            "source_windows": {
                "folded": len(used),
                "missing": missing,
                "unreadable": len(row) - len(used) - missing,
            }
        },
    )


def close_shard(store_root: str, shard_key, grid, config, row: list, *, store_kwargs) -> str | None:
    """The worker seam: gate on the declaration, fold ``row``, write; returns the basename.

    The same gate the window columns take (:func:`zagg.column.leaf_column_plan`):
    a declaration with no leaf-node levels writes no all-time column and
    deletes any a previous declaration left (:func:`zagg.column._clear_column`
    under the token), so the member never outlives the declaration that
    made it. Failures raise; the caller reports them as the shard's error.
    """
    from zagg.column import _clear_column, leaf_column_plan

    store_kwargs = dict(store_kwargs or {})
    plan = leaf_column_plan(config, grid)
    if plan is None:
        _clear_column(store_root, shard_key, None, store_kwargs)
        return None
    resolutions, fields = plan
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
