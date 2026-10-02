"""The staged sweep's work units: ``(node, window)`` and the node close (issue #586 phase 4).

A staged sweep used to fold a dispatch node's every window in one worker,
serially. On the first windowed fleet run (issue #586, 2026-09-26) that put
448 leaves on the order-6 worker and it hit the 900 s wall twice; narrowing
the tuple only deferred the ceiling. The per-window fold is independent —
each window's column and overview come from that window's leaves alone — so
the unit of stage work is ``(node, window)``:

- **window units** — one per window per dispatch node, run concurrently on
  the fleet. A window unit reads and writes only its own window's objects
  (its overviews, its stage column) and shares no read-modify-write with its
  siblings: the skip gate reads each artifact's own attrs
  (:func:`zagg.sweep_stage._artifact_entry`). A unit that dies costs its own
  window's artifacts and nothing else.
- **the node close** — one per node, AFTER every window unit of the node has
  landed: the **all-time fold** (:func:`close_node`), which k-way merges the
  node's per-window overviews — small objects, never leaves — into
  ``all.zarr``. It exists only where the store declares
  ``pyramid.overview.all_time`` (:func:`closes_nodes`): a store without it
  has nothing to do per node once its windows are in, so it pays no unit and
  no barrier. This is also the seam a per-node commit belongs on (the
  Icechunk row model of issue #584: window units write ref sidecars, the
  node commits once after them).

An unwindowed store has one unit per node, as before — its single fold IS
all-time (spec §4.2) and closes inline.

:func:`stage_units` is THE enumeration: the in-process pass
(:func:`zagg.sweep_stages.sweep_stage_pass`, hence the local backend and the
``python -m zagg.sweep --stages`` backstop), the fleet worker (the same pass,
restricted to its unit) and the fleet dispatcher
(:func:`zagg.sweep_fleet.run_stage_sweep_fleet`) all call it, so what the
dispatcher waits for is exactly what the workers do.
"""

from __future__ import annotations

import logging

from zagg.sweep_stage import UNREADABLE, _ColumnReader, _is_reader

logger = logging.getLogger(__name__)

#: The two ``stage.unit`` values a fleet stage event names. Absent, a worker
#: runs a node whole — every window unit, then the close — which is what an
#: unwindowed store's one unit is, and what a pre-phase-4 dispatcher sends.
UNIT_WINDOW = "window"
UNIT_CLOSE = "close"
UNIT_KINDS = (UNIT_WINDOW, UNIT_CLOSE)


def check_unit(unit, window, what: str) -> None:
    """Refuse, by name, a stage invoke whose ``unit`` / ``window`` disagree.

    A window unit folds exactly one named window and no other unit takes
    one; anything else would silently run a different share than the one the
    dispatcher is waiting on.
    """
    if unit is not None and unit not in UNIT_KINDS:
        raise ValueError(f"unknown stage unit {unit!r} (expected one of {UNIT_KINDS}; {what})")
    if (unit == UNIT_WINDOW) != (window is not None):
        raise ValueError(
            f"stage invoke ({what}) names unit {unit!r} with window {window!r} — a window "
            "unit folds exactly one named window, and no other unit takes one"
        )


def closes_nodes(*, windowed: bool, all_time: bool) -> bool:
    """Whether a store's nodes take a close unit after their window units.

    Today exactly when a windowed store declares the all-time fold. The
    predicate is one function because the dispatcher and the workers must
    agree on it — a dispatcher waiting for close records no worker writes
    would sit out a whole barrier.
    """
    return bool(windowed and all_time)


def node_rows(node: str, by_shard: dict, dirt_only: dict | None, *, windowed: bool, close: bool):
    """The companion repo's rows one stage node writes THIS RUN (spec §11.2).

    The ladder plans and commits per row, so it must be told the run's rows
    and not read the repo's whole list: an append would otherwise re-plan
    and re-commit every window the store ever allocated (issue #584 review).
    An unwindowed store has the one ``all`` row. A windowed node's rows are
    the window labels dirty beneath it — the written set (``by_shard``) and
    the refs-only set (``dirt_only``, issue #580) alike, since the hook runs
    for a regathered node too — plus ``all`` when the node closes, which is
    the cross-window fold's own row at the overview levels.
    """
    from zagg.icechunk_rows import ALL_ROW

    if not windowed:
        return [ALL_ROW]
    labels = sorted(
        {
            window
            for source in (by_shard, dirt_only or {})
            for decimal, windows in source.items()
            if decimal.startswith(node)
            for window in windows
            if window is not None and window != ALL_ROW
        }
    )
    return [*labels, *([ALL_ROW] if close else [])]


def manifest_closes(manifest: dict) -> bool:
    """:func:`closes_nodes` as the store's manifest declares it — the source of truth.

    ``pyramid`` is not a frozen manifest key (it may be declared or changed
    after the store exists), so a run's config can disagree with the store.
    The workers decide from the manifest, and every stage record says what
    they decided (``closes``): the fleet dispatcher takes the answer from a
    landed window unit's record, never from its config
    (:func:`zagg.sweep_fleet.run_stage_sweep_fleet`).
    """
    decl = (manifest.get("pyramid") or {}).get("overview")
    return closes_nodes(
        windowed=manifest.get("temporal") is not None,
        all_time=bool(decl.get("all_time")) if isinstance(decl, dict) else False,
    )


def stage_units(
    work: dict,
    dispatch: int,
    *,
    windowed: bool,
    all_time: bool,
    scope=None,
    candidates=None,
    dirt_only: dict | None = None,
) -> list[dict]:
    """One tuple's stage units, per dispatch node: THE shared enumeration.

    ``work`` is the dirty set (``{shard decimal: {window, ...}}``),
    ``dirt_only`` the same shape for leaves whose data is current but whose
    refs moved (issue #580). Dispatch nodes are the ``dispatch``-order
    ancestors of ``candidates`` — the work set by default (what a dispatcher
    holds), or a wider set the caller read from the store (the in-process
    pass adds the root ``coverage.moc``'s shards, so untouched siblings are
    still visited and heal) — filtered by the ``scope`` MOC.

    Returns ``[{"node", "windows", "close"}, ...]`` sorted by node:

    - ``windows`` — the node's window units, one per entry. ``[None]`` on an
      unwindowed store (the one unit, which also closes the node); on a
      windowed store the sorted labels dirty beneath the node.
    - ``close`` — whether a close unit follows the window units
      (:func:`closes_nodes`).

    A windowed node with neither a dirty window nor a close has no unit and
    is not listed. A window labelled with the reserved ``all`` token gets no
    unit: its overview would be the all-time fold's own object (D23).
    """
    from zagg.sweep_stage import _node_at
    from zagg.sweep_stages import scope_admits
    from zagg.windows import SCHEDULE_NONE_TOKEN

    dispatch = int(dispatch)
    close = closes_nodes(windowed=windowed, all_time=all_time)
    pool = set(work) | set(dirt_only or ()) if candidates is None else set(candidates)
    dirty: dict = {}
    for decimal, windows in work.items():
        dirty.setdefault(_node_at(decimal, dispatch), set()).update(windows)
    units = []
    for node in sorted({_node_at(d, dispatch) for d in pool}):
        if not scope_admits(node, scope):
            continue
        if not windowed:
            units.append({"node": node, "windows": [None], "close": False})
            continue
        labels = sorted(w for w in dirty.get(node, ()) if w is not None)
        if SCHEDULE_NONE_TOKEN in labels:
            # Config validation rejects the label; a hand-edited or pre-guard
            # manifest can still carry one (review finding, #201).
            logger.warning(
                f"stage sweep: window label {SCHEDULE_NONE_TOKEN!r} at node {node} is the "
                f"reserved all-time token (D23) — no per-window overview is written for it"
            )
            labels.remove(SCHEDULE_NONE_TOKEN)
        if labels or close:
            units.append({"node": node, "windows": labels, "close": close})
    return units


class _OverviewReader(_ColumnReader):
    """Stamp-validated reads over one per-window ladder overview.

    The all-time fold's source. Everything :class:`_ColumnReader` gives a
    child column — one root GET for stamp + attrs, optimistic re-validation
    around every read, the foreign-fresh abort — over a ``zagg-overview/2``
    artifact, whose generation block lives in its own attrs key.
    """

    @property
    def provenance(self) -> dict:
        from zagg.sweep_overview import OVERVIEW_ATTR

        block = self.attrs.get(OVERVIEW_ATTR)
        return block if isinstance(block, dict) else {}

    def generation(self) -> tuple:
        from zagg.column import stamped_generation_key

        return stamped_generation_key(self.provenance.get("generation"), self.stamp)


def node_windows(store, node: str) -> set:
    """The window labels with an overview object at ``node`` (one delimiter LIST).

    The all-time fold covers EVERY window the node has, not the ones this
    run touched, and the store is the only place that knows: a dispatcher
    holds a run's work set, and the sweep-internal envelope is not maintained
    by concurrent window units. Bounded and node-scoped — one LIST of the
    node's own prefix, never a walk. A failed LIST raises: folding the
    windows this run happened to dirty and calling it all-time would publish
    a short fold under a name that claims the whole record.
    """
    import obstore

    from zagg.sweep_stage import _node_rel
    from zagg.windows import SCHEDULE_NONE_TOKEN, validate_label

    listing = obstore.list_with_delimiter(store, _node_rel(node) + "/")
    labels = set()
    for prefix in listing["common_prefixes"]:
        name = str(prefix).rstrip("/").rsplit("/", 1)[-1]
        if not name.endswith(".zarr") or name.endswith(".pyramid.zarr"):
            continue
        label = name[: -len(".zarr")]
        if label == SCHEDULE_NONE_TOKEN:
            continue
        try:
            labels.add(validate_label(label))
        except ValueError:
            continue  # not a window's object (a child digit, foreign debris)
    return labels


def _window_readers(store_root, node, labels, *, run_id, run_started, store_kwargs, counts) -> list:
    """One reader per window overview at ``node``: reader, ``None``, or unreadable."""
    from zagg.sweep_overview import _overview_basename
    from zagg.sweep_stage import ForeignSweepError, _node_rel

    row = []
    for label in labels:
        path = f"{store_root}/{_node_rel(node)}/{_overview_basename(label)}"
        try:
            reader = _OverviewReader(
                path, run_id=run_id, run_started=run_started, store_kwargs=store_kwargs
            )
        except ForeignSweepError:
            raise
        except Exception as e:
            logger.warning(f"stage sweep: unreadable overview {path} ({e})")
            counts["failed"] += 1
            row.append(UNREADABLE)
            continue
        row.append(reader if reader.committed and reader.provenance else None)
    return row


def close_node(
    store,
    store_root,
    node,
    stage,
    levels,
    fields,
    *,
    dirty_windows,
    shard_order,
    cell_order,
    candidates,
    run_id,
    run_started,
    counts,
    store_kwargs,
    meter=None,
) -> None:
    """The node close: the all-time fold of a windowed store, for one tuple.

    Per ladder order of the tuple and per artifact node beneath the dispatch
    node, k-way merge the node's per-window overviews (``{window}.zarr``,
    every window the node has — :func:`node_windows` — plus the windows this
    run dirtied, so one whose unit died is counted missing rather than
    silently left out) into ``all.zarr``, cell for cell at the level's own
    resolution. The sources are overview-sized objects at the SAME node, so
    the close costs the same at any tuple and any number of leaves, and
    scales with the window count alone.

    Provenance, recorded on the artifact:

    - ``regime`` is ``stage-merge``; ``merges_from_raw`` is one more than its
      sources' — **2** at a gather level, whose per-window overviews are
      gen-1 content (the same digests the previous all-time fold read off the
      child columns, so those levels are byte-identical to it), **3** at a
      merge level, whose per-window overviews are themselves gen-2 merges;
    - ``source_windows`` — ``{folded, missing, unreadable}`` over the window
      overviews: a window with a dirty leaf and no committed overview is
      ``missing``;
    - ``source_children`` — the folded windows' own counters, summed: a
      window that under-covered its subtree makes the all-time fold short by
      the same children.

    Skip-if-current keys on the summed generations of the window overviews
    and on the count of missing windows, read off the all-time artifact's own
    attrs: the close shares no object with a window unit either. It writes no stage column (a column relays
    gen-1 content; an all-time one would be merged).
    """
    from functools import partial

    from zagg.column import generation_key
    from zagg.sweep_fold import ColumnMovedError, merge_level, refold_on_move
    from zagg.sweep_overview import _overview_basename
    from zagg.sweep_stage import (
        STAGE_MERGE,
        _artifact_entry,
        _fold_result,
        _node_at,
        _summed_generation,
        _write_stage_overview,
        classify_level,
    )
    from zagg.windows import SCHEDULE_NONE_TOKEN

    level_by_order = {int(e["node"]): int(e["cells"][0]) for e in levels}
    basename = _overview_basename(SCHEDULE_NONE_TOKEN)
    for k in (k for k in stage["orders"] if k in level_by_order):
        r = level_by_order[k]
        n_out = 4 ** (r - k)
        base_gen = 1 if classify_level(r, shard_order=shard_order) != STAGE_MERGE else 2
        for target in sorted({_node_at(d, k) for d in candidates if d.startswith(node)}):
            try:
                labels = sorted(node_windows(store, target) | set(dirty_windows(target)))
            except Exception as e:
                logger.warning(f"stage sweep: cannot list the windows at node {target} ({e})")
                counts["failed"] += 1
                continue
            reader_args = dict(run_id=run_id, run_started=run_started, store_kwargs=store_kwargs)
            row = _window_readers(store_root, target, labels, counts=counts, **reader_args)
            sources = [reader for reader in row if _is_reader(reader)]
            if not sources:
                counts["empty"] += 1  # no window overview at this node yet
                continue
            rows = [row]
            missing = sum(1 for reader in row if reader is None)
            fresh_gen = _summed_generation(rows)
            entry = _artifact_entry(store_root, target, basename, run_id, run_started, store_kwargs)
            if (
                entry is not None
                and generation_key(entry.get("generation")) == generation_key(fresh_gen)
                and entry.get("regime") == STAGE_MERGE
                # A window that is newly missing moves no generation — nothing
                # was folded from it — but the artifact must say it is short.
                and (entry.get("source_windows") or {}).get("missing") == missing
            ):
                counts["current"] += 1
                continue

            def _fresh_readers(target=target, labels=labels, row=row, args=reader_args):
                # In place: ``rows`` holds this list (the unreadable were counted once).
                row[:] = _window_readers(store_root, target, labels, counts={"failed": 0}, **args)

            try:
                slabs, broken, demotions = refold_on_move(
                    partial(
                        merge_level,
                        rows,
                        fields,
                        res_src=r,
                        src_per_child=n_out,
                        factor=1,
                        n_out=n_out,
                        meter=meter,
                    ),
                    _fresh_readers,
                    f"the all-time fold at node {target}",
                )
            except ColumnMovedError as e:
                logger.warning(f"stage sweep: all-time fold failed at node {target} ({e})")
                counts["failed"] += 1
                continue
            # Recounted: a refold read the windows from fresh readers.
            missing = sum(1 for reader in row if reader is None)
            used = set(sources) | {reader for reader in row if _is_reader(reader)}
            counts["revalidated"] += sum(reader.revalidated for reader in used)
            folded = [
                reader
                for w, reader in enumerate(row)
                if _is_reader(reader) and (0, w) not in broken
            ]
            if not folded:
                counts["empty"] += 1
                continue
            children = {"folded": 0, "missing": 0, "unreadable": 0}
            for reader in folded:
                for name in children:
                    children[name] += int(
                        (reader.provenance.get("source_children") or {}).get(name) or 0
                    )
            fold = _fold_result(
                target,
                k,
                r,
                fields,
                rows,
                slabs,
                regime=STAGE_MERGE,
                merges_from_raw=1
                + max(int(rd.provenance.get("merges_from_raw") or base_gen) for rd in folded),
                source_children=(children["folded"], children["missing"], children["unreadable"]),
                demotions=demotions,
            )
            fold["source_windows"] = {
                "folded": len(folded),
                "missing": missing,
                "unreadable": len(row) - len(folded) - missing,
            }
            if missing or children["missing"]:
                counts["under_covered"] += 1
            try:
                _write_stage_overview(
                    store_root,
                    target,
                    k,
                    SCHEDULE_NONE_TOKEN,
                    r,
                    fold,
                    fields,
                    shard_order,
                    cell_order,
                    True,
                    run_id,
                    store_kwargs,
                )
            except Exception as e:
                logger.warning(f"stage sweep: all-time write failed at node {target} ({e})")
                counts["failed"] += 1
                continue
            counts["written"] += 1


def run_tuple(
    stage: dict,
    context: dict,
    *,
    window_unit,
    only_unit: str | None = None,
    only_window: str | None = None,
    on_node=None,
) -> dict:
    """Run one tuple's units, node by node — window units, then the close; its row.

    The execution half of :func:`stage_units`, shared by every executor: the
    in-process pass runs a tuple whole through it, a fleet stage invoke runs
    its own share (``only_unit`` — :data:`UNIT_WINDOW` with ``only_window``,
    or :data:`UNIT_CLOSE`). ``context`` is the pass's constants
    (:func:`zagg.sweep_stages.sweep_stage_pass` builds it) and
    ``window_unit`` the ``(node, window)`` fold
    (:func:`zagg.sweep_stage.stage_node`, handed in by the pass).

    **The node close** is everything that runs ONCE per node after its window
    units have landed: the all-time fold (:func:`close_node`) where the unit
    list says so, then the Icechunk ref hook
    (:func:`zagg.icechunk_ladder.stage_hook`). An unwindowed node's one unit
    closes inline, right after it; a window-only invoke closes nothing.

    **A unit's failure is its own**, on a windowed store: a window unit or a
    close that raises is counted (``failed``), named in ``unit_errors``, and
    the node's other units — and every other node — go on. A
    :class:`zagg.sweep_stage.ForeignSweepError` still aborts (the lease
    backstop), and an unwindowed store's one unit per node raises as it
    always has. Every unit is metered (:class:`zagg.sweep_fold.FoldMeter`)
    into the tuple's ``fold_*`` counters.

    Returns the tuple's stage row: the counters, ``nodes``, ``window_units``
    / ``close_units``, ``duration_s``, and — only when present —
    ``unit_errors`` and (under the Icechunk ladder) ``icechunk_nodes``.
    """
    import time

    from zagg.icechunk_ladder import STAGE_COUNTS, node_row, stage_hook
    from zagg.sweep_fold import FoldMeter
    from zagg.sweep_stage import ForeignSweepError
    from zagg.windows import SCHEDULE_NONE_TOKEN

    t0 = time.perf_counter()
    manifest, by_shard = context["manifest"], context["by_shard"]
    dirt_only, ladder = context["dirt_only"], context["ladder"]
    windowed = manifest.get("temporal") is not None
    counts = {
        "written": 0,
        "current": 0,
        "empty": 0,
        "failed": 0,
        "under_covered": 0,
        "columns_written": 0,
        "columns_current": 0,
        "revalidated": 0,
        # The streamed fold's own accounting: blocks folded, source cells
        # read, and the most any one block held.
        "fold_blocks": 0,
        "fold_cells_read": 0,
        "fold_peak_cells": 0,
        **({name: 0 for name in STAGE_COUNTS} if ladder is not None else {}),
    }
    units = stage_units(
        by_shard,
        stage["dispatch"],
        windowed=windowed,
        all_time=manifest_closes(manifest),
        scope=context["scope"],
        candidates=context["candidates"],
        dirt_only=dirt_only,
    )
    shared = {
        "shard_order": int(manifest["shard_order"]),
        "cell_order": int(manifest["cell_order"]),
        "candidates": context["candidates"],
        "run_id": context["run_id"],
        "run_started": context["run_started"],
        "counts": counts,
        "store_kwargs": context["store_kwargs"],
    }
    target = (context["store"], context["store_root"])
    ran: dict = {"window_units": 0, "close_units": 0, "unit_errors": [], "icechunk_nodes": []}

    def _run(unit_fn, node, label):
        meter = FoldMeter()
        try:
            unit_fn(meter)
        except ForeignSweepError:
            raise
        except Exception as e:
            if not windowed:
                raise
            logger.warning(f"stage sweep: unit ({node}, {label}) failed ({e})")
            counts["failed"] += 1
            ran["unit_errors"].append(
                {"node": node, "unit": label, "error": f"{type(e).__name__}: {e}"}
            )
        finally:
            counts["fold_blocks"] += meter.blocks
            counts["fold_cells_read"] += meter.cells_read
            counts["fold_peak_cells"] = max(counts["fold_peak_cells"], meter.peak_cells)

    for unit in units:
        node = unit["node"]
        dirty = any(d.startswith(node) for d in by_shard)
        regather = not dirty and any(d.startswith(node) for d in dirt_only)
        for window in () if regather or only_unit == UNIT_CLOSE else unit["windows"]:
            if only_unit == UNIT_WINDOW and window != only_window:
                continue
            ran["window_units"] += 1
            _run(
                lambda meter, node=node, window=window: window_unit(
                    *target,
                    node,
                    stage,
                    context["levels"],
                    context["fields"],
                    key=SCHEDULE_NONE_TOKEN if window is None else window,
                    window=window,
                    windowed=windowed,
                    relay=context["relay"],
                    level_actuals=context["level_actuals"],
                    # An unwindowed node has ONE writer, so its unit owns the
                    # node envelope; window units run concurrently on the
                    # fleet and share nothing.
                    envelope=not windowed,
                    meter=meter,
                    **shared,
                ),
                node,
                window,
            )
        if only_unit != UNIT_WINDOW:
            if unit["close"] and not regather:
                ran["close_units"] += 1
                _run(
                    lambda meter, node=node: close_node(
                        *target,
                        node,
                        stage,
                        context["levels"],
                        context["fields"],
                        dirty_windows=lambda at: {
                            w
                            for d, ws in by_shard.items()
                            if d.startswith(at)
                            for w in ws
                            if w is not None and w != SCHEDULE_NONE_TOKEN
                        },
                        meter=meter,
                        **shared,
                    ),
                    node,
                    UNIT_CLOSE,
                )
            hooked = stage_hook(
                context["store_root"],
                node,
                stage,
                dirty=dirty or regather,
                rows=node_rows(
                    node, by_shard, dirt_only, windowed=windowed, close=bool(unit["close"])
                ),
                manifest=manifest,
                levels=context["levels"],
                fields=context["fields"],
                candidates=context["candidates"],
                block=ladder,
                store_kwargs=context["store_kwargs"],
                counts=counts,
            )
            if hooked is not None:
                ran["icechunk_nodes"].append(node_row(hooked))
                if regather and "error" not in hooked:
                    counts["icechunk_regathered"] += 1
        if on_node is not None:
            on_node(node)
    row = {
        "dispatch_order": stage["dispatch"],
        "orders": list(stage["orders"]),
        "nodes": len(units),
        "window_units": ran["window_units"],
        "close_units": ran["close_units"],
        **counts,
        "duration_s": time.perf_counter() - t0,
    }
    if ran["unit_errors"]:
        row["unit_errors"] = ran["unit_errors"]
    if ladder is not None:
        row["icechunk_nodes"] = ran["icechunk_nodes"]
    return row
