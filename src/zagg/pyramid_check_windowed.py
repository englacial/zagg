"""The windowed arm of the E2E harness (issues #434, #586): one ladder per window.

Routed to by :func:`zagg.pyramid_check.validate_pyramid` when the manifest
declares a window schedule (``temporal``) under ``zagg-pyramid/2``. A windowed
store is the unwindowed one repeated per window, plus one artifact that has no
unwindowed counterpart, and the harness holds it to what spec §4.2–§4.6 state:

- **the per-window ladder** — a leaf ``{id}_{window}.zarr``, its column
  ``{window}.pyramid.zarr`` beside it (§4.6), and an overview ``{window}.zarr``
  at every ladder node above it (§4.2/§4.4). Each window is validated exactly
  as an unwindowed store is, by the same legs
  (:func:`zagg.pyramid_check_v2._tier_checks`) reading that window's objects:
  its ladder re-folded from ITS leaf columns, its columns from ITS leaves. An
  overview built from another window's leaves therefore fails by value, on
  the node and the window that hold it; one carrying another window's name
  in its attrs fails read-back on every node, sampled or not.
- **the all-time fold** — ``all.zarr`` at every ladder node, present exactly
  where the manifest declares ``pyramid.overview.all_time`` (§4.5). Its
  source is the node's OWN per-window overviews (§4.4 "The all-time fold of a
  windowed store"), so per sampled node the harness lists the windows the
  node has, and requires: every cell to equal the k-way fold across them
  under each field's law (:func:`zagg.pyramid_check_core._check_node`, the
  same value laws as every other level); ``regime: stage-merge`` with
  ``merges_from_raw`` one more than its sources' — 2 at a gather level, 3 at
  a merge level, derived from the geometry; ``source_windows`` equal to the
  windows actually there; ``source_children`` and ``generation`` equal to
  the consumed overviews' own blocks, summed. A fold that consumed fewer
  windows than the node now holds is STALE and fails by name. Undeclared, the
  check is reported ``skip`` — *not applicable* — never left out.

Nothing here calls the fold it checks: expectations come from the leaves
(the roster, the columns), from the per-window artifacts' own recorded
attrs, and from the geometry.

**What it reads.** The leaf roster — the run records at the store root
(one LIST, the ``stats_*.parquet`` objects), or ``--roster list`` / ``moc``;
one ``zarr.json`` GET per declared ``(node, window)`` overview, per ``(leaf,
window)`` column and per all-time node (the same per-artifact bound as the
unwindowed arm, times the window count); then array reads for
``sample_windows`` windows × ``sample_nodes`` nodes per order ×
``sample_cells`` cells, and for ``sample_nodes`` all-time nodes per order one
delimiter LIST of the node's prefix plus the same cells of each of its
windows' overviews. Read-only throughout.

**Not applicable, and said so.** The staged sweep's stage columns
(``{window}.pyramid.zarr`` above the shard) are orchestration, never contract
(§4.6), and are not validated on either arm. A windowed ``zagg-pyramid/1``
store (a legacy ``orders`` schedule, or a raster store) is not modelled: its
``declaration`` fails naming that, with ``coordinates`` reported beside it.
"""

from __future__ import annotations

import logging
from functools import partial

import numpy as np

from zagg.pyramid_check_core import (
    _check_node,
    _column_object_rel,
    _committed,
    _coordinates_check,
    _declared_nodes,
    _entry,
    _field_groups,
    _finish,
    _Harness,
    _leaf_roster,
    _node_object_rel,
    _probe_nodes,
    _rank,
    _settle_value_checks,
)

logger = logging.getLogger(__name__)

#: The windowed checklist: the ``/2`` arm's, plus the all-time fold.
CHECKS_WINDOWED = (
    "declaration",
    "coordinates",
    "materialization",
    "columns",
    "readback",
    "counts",
    "digests",
    "composition",
    "all_time",
    "idempotency",
)

_VALUE_CHECKS = ("readback", "counts", "digests", "composition")
#: The §4.4 counter keys of ``source_windows`` and ``source_children``.
_COUNTERS = ("folded", "missing", "unreadable")


def window_roster(store_root, manifest, store_kwargs, mode) -> tuple[list, str]:
    """``(shard decimal, window)`` leaf refs + the source used.

    ``auto`` reads the run records (:func:`zagg.sweep.discover_leaves` — the
    staged sweep's own discovery source: one LIST of the root and the
    ``stats_*.parquet`` objects), falling back to the flat list when they name
    no windowed leaf. ``list`` walks the store. ``moc`` takes the shards from
    the root ``coverage.moc`` and each shard's windows from one delimiter LIST
    of its node — the MOC is per shard and does not know the windows.
    """
    from zagg.grids.morton import morton_decimal

    if mode == "auto":
        from zagg.sweep import discover_leaves

        refs = {
            (morton_decimal(key), window)
            for key, window in discover_leaves(store_root, store_kwargs=store_kwargs)
            if window is not None
        }
        if refs:
            return sorted(refs), "run records"
    if mode == "moc":
        shards, _source = _leaf_roster(store_root, manifest, store_kwargs, "moc")
        return _listed_refs(store_root, shards, store_kwargs), "coverage.moc + shard lists"
    return _listed_refs(store_root, None, store_kwargs, int(manifest["shard_order"])), "list"


def _listed_refs(store_root, shards, store_kwargs, shard_order=None) -> list:
    """Windowed leaf refs by listing: the whole store, or each shard's node."""
    import concurrent.futures

    import obstore

    from zagg.grids.morton import morton_word
    from zagg.hive import shard_leaf_path
    from zagg.store import open_object_store
    from zagg.windows import split_leaf_name

    store = open_object_store(store_root, **store_kwargs)

    def ref(name, decimal):
        try:
            full_id, window = split_leaf_name(name)
        except ValueError:
            return None  # a column, an overview, or debris: not a leaf of this shard
        return (decimal, window) if full_id == decimal and window is not None else None

    if shards is None:
        refs = set()
        for batch in obstore.list(store):
            for meta in batch:
                parts = str(meta["path"]).split("/")
                if len(parts) == shard_order + 3 and parts[-1] == "zarr.json":
                    refs.add(ref(parts[-2], "".join(parts[:-2])))
        return sorted(refs - {None})

    def windows_of(decimal):
        prefix = shard_leaf_path("", morton_word(decimal)).lstrip("/").rsplit("/", 1)[0]
        listing = obstore.list_with_delimiter(store, prefix + "/")
        names = [str(p).rstrip("/").rsplit("/", 1)[-1] for p in listing["common_prefixes"]]
        return [r for r in (ref(name, decimal) for name in names) if r is not None]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        return sorted(r for refs in pool.map(windows_of, shards) for r in refs)


def validate_windowed(
    store_root: str,
    manifest: dict,
    report: dict,
    *,
    ladder: list,
    fields: dict,
    store_kwargs: dict,
    sample_nodes: int,
    sample_cells: int,
    sample_windows: int,
    seed: int,
    full: bool,
    resweep: bool,
    roster: str,
) -> dict:
    """The windowed ``/2`` checklist spine; called with the shared prologue done."""
    from zagg.pyramid_check_v2 import _declaration_grammar, _restage_check

    checks = report["checks"]

    def skip_rest(reason, *, after):
        for name in CHECKS_WINDOWED[CHECKS_WINDOWED.index(after) + 1 :]:
            if name == "idempotency" or name in checks:
                continue
            checks[name] = _entry("skip", reason)

    # -- [1] declaration: the /2 grammar (§4.5), and what the schedule adds.
    entries, leaf_cells, problems = _declaration_grammar(manifest)
    if problems:
        checks["declaration"] = _entry(
            "fail", f"malformed zagg-pyramid/2 declaration: {'; '.join(problems)}"
        )
        skip_rest("malformed /2 declaration", after="declaration")
        return _finish(report, CHECKS_WINDOWED)
    all_time = bool(((manifest.get("pyramid") or {}).get("overview") or {}).get("all_time"))
    schedule = (manifest.get("temporal") or {}).get("schedule")
    report["leaf_levels"] = leaf_cells
    report["all_time"] = all_time
    checks["declaration"] = _entry(
        "pass",
        f"zagg-pyramid/2 leaf resolutions {leaf_cells} + fixed every-order ladder to 0, one "
        f"ladder per window (schedule {schedule!r}); all-time fold "
        f"{'declared' if all_time else 'not declared'}; composable fields {sorted(fields)} "
        f"(classes {report['field_classes']})",
    )

    # -- roster: (shard, window) leaves — what every declared artifact derives from.
    try:
        refs, roster_source = window_roster(store_root, manifest, store_kwargs, roster)
    except Exception as exc:
        checks["materialization"] = _entry("fail", f"no leaf roster: {exc}")
        skip_rest("no roster", after="materialization")
        return _finish(report, CHECKS_WINDOWED)
    by_window: dict = {}
    for dec, window in refs:
        by_window.setdefault(window, []).append(dec)
    report["roster"] = {"source": roster_source, "leaves": len(refs), "windows": len(by_window)}
    report["windows"] = sorted(by_window)
    if not refs:
        checks["materialization"] = _entry("fail", f"empty leaf roster (source {roster_source})")
        skip_rest("empty roster", after="materialization")
        return _finish(report, CHECKS_WINDOWED)
    _coordinates_check(
        store_root,
        manifest,
        refs,
        store_kwargs,
        checks,
        report,
        seed=seed,
        sample_nodes=sample_nodes,
        sample_cells=sample_cells,
        full=full,
    )

    # -- [2] materialization: one overview per (declared node, window).
    declared = {w: {k: _declared_nodes(ds, k) for k, _ in ladder} for w, ds in by_window.items()}
    probes, state = _artifact_probes(
        store_root,
        {w: sorted({n for nodes in declared[w].values() for n in nodes}) for w in by_window},
        store_kwargs,
        rel=_node_object_rel,
        role="overview",
        what="(node, window) overview",
        check="materialization",
        baseline="pre-sweep baseline",
        checks=checks,
        report=report,
    )
    if state == "errors":
        skip_rest("node probes failed", after="materialization")
        return _finish(report, CHECKS_WINDOWED)

    # -- [3] the §4.6 leaf-column tier: one column per (leaf, window).
    from zagg.column import COLUMN_ROLE

    col_probes, col_state = _artifact_probes(
        store_root,
        by_window,
        store_kwargs,
        rel=_column_object_rel,
        role=COLUMN_ROLE,
        what="(leaf, window) column",
        check="columns",
        baseline="pre-backfill baseline",
        checks=checks,
        report=report,
    )
    if state == "baseline":
        skip_rest("no materialized ladder nodes (pre-sweep baseline)", after="columns")
        return _finish(report, CHECKS_WINDOWED)
    if col_state == "errors":
        skip_rest("column probes failed", after="columns")
        return _finish(report, CHECKS_WINDOWED)

    # -- [4..7] the per-window value checks, then the all-time fold.
    rng = np.random.default_rng(seed)
    harness = _Harness(
        store_root,
        manifest,
        store_kwargs,
        rng=rng,
        sample_nodes=sample_nodes,
        sample_cells=sample_cells,
    )
    groups = _field_groups(harness, report)
    errors: dict = {name: [] for name in _VALUE_CHECKS}
    counted = dict.fromkeys(_VALUE_CHECKS, 0)
    _window_checks(
        harness,
        ladder,
        declared,
        probes,
        col_probes,
        by_window,
        entries,
        groups,
        errors,
        counted,
        full=full,
        sample_windows=sample_windows,
        seed=seed,
    )
    count_meta, _exact, digest_fields, wide_fields, packed_fields = groups
    _settle_value_checks(
        checks,
        report,
        harness,
        errors,
        counted,
        count_meta=count_meta,
        digest_fields=digest_fields,
        wide_fields=wide_fields,
        packed_fields=packed_fields,
    )
    if roster_source != "list" and checks["counts"]["status"] == "fail":
        checks["counts"]["detail"] += (
            f"; NOTE the leaf roster came from the {roster_source} — a roster that "
            "understates the leaves reads exactly like a short fold; rerun with "
            "--roster list to tell a broken sweep from a stale record"
        )
    _all_time_check(
        harness, ladder, declared, probes, groups, checks, report, all_time=all_time, full=full
    )
    if harness.warnings:
        report["warnings"] = list(dict.fromkeys([*report.get("warnings", []), *harness.warnings]))

    # -- [9] idempotency (fixture mode): an immediate staged re-pass is a no-op.
    if resweep:
        by_shard: dict = {}
        for dec, window in refs:
            by_shard.setdefault(dec, set()).add(window)
        checks["idempotency"] = _restage_check(store_root, manifest, by_shard, store_kwargs)
    return _finish(report, CHECKS_WINDOWED)


def _artifact_probes(
    store_root, names_by_window, store_kwargs, *, rel, role, what, check, baseline, checks, report
) -> tuple[dict, str]:
    """Probe one artifact per ``(name, window)`` and settle ``check``.

    ``names_by_window`` maps each window to the node (or leaf) decimals that
    must carry its artifact; ``rel`` maps ``(decimal, window)`` to the
    artifact's zarr root. One ``zarr.json`` GET each. Returns ``({window:
    {decimal: committed attrs | None}}, state)`` with the unwindowed legs'
    three-way state: ``"errors"`` (a transport failure — UNKNOWN, never a
    verdict), ``"baseline"`` (none committed) or ``"ok"``. A missing or
    partial artifact is named ``decimal[window]``.
    """
    committed: dict = {}
    missing, torn, probe_errors = [], [], []
    n_declared = 0
    for window, names in sorted(names_by_window.items()):
        probed, errored = _probe_nodes(
            store_root, names, store_kwargs, rel=partial(rel, window=window)
        )
        probe_errors.extend(f"{n}[{window}]: {e}" for n, e in sorted(errored.items()))
        committed[window] = {
            n: attrs if _committed(attrs, role=role) else None for n, attrs in probed.items()
        }
        n_declared += len(names)
        for n in sorted(names):
            if committed[window].get(n) is None:
                (torn if probed.get(n) is not None else missing).append(f"{n}[{window}]")
    found = n_declared - len(missing) - len(torn)
    report[check] = {
        "declared": n_declared,
        "materialized": found,
        "missing": missing[:50],
        "partial": torn[:50],
        "probe_errors": probe_errors[:50],
    }
    debris = f"; {len(torn)} partial uncommitted: {torn[:8]}" if torn else ""
    if probe_errors:
        checks[check] = _entry(
            "fail",
            f"{len(probe_errors)} probe error(s) — {what} state is UNKNOWN, not a verdict: "
            f"{probe_errors[:3]}",
        )
        return committed, "errors"
    if not found:
        checks[check] = _entry(
            "fail",
            f"declared but none committed: 0/{n_declared} {what}s across "
            f"{len(names_by_window)} window(s) — {baseline}{debris}",
        )
        return committed, "baseline"
    absent = f"; missing: {missing[:8]}" if missing else ""
    checks[check] = _entry(
        "pass" if found == n_declared else "fail",
        f"{found}/{n_declared} {what}s committed across {len(names_by_window)} window(s)"
        f"{absent}{debris}",
    )
    return committed, "ok"


def _window_checks(
    harness,
    ladder,
    declared,
    probes,
    col_probes,
    by_window,
    entries,
    groups,
    errors,
    counted,
    *,
    full,
    sample_windows,
    seed,
):
    """The unwindowed ``/2`` legs, once per window, over that window's objects.

    The window key every artifact records is checked on ALL of them (the
    attrs are already in hand): an object under one window's name that says
    it is another's is misfiled whatever its values. The value legs then run
    for ``sample_windows`` windows (every one in full mode), each through a
    harness bound to the window, so the ladder is re-folded from that
    window's leaf columns and the columns from that window's leaves; every
    finding is prefixed with its window.
    """
    from zagg.column import COLUMN_ATTR
    from zagg.pyramid_check_v2 import _actuals_errors, _tier_checks
    from zagg.sweep_overview import OVERVIEW_ATTR

    for window in sorted(by_window):
        for attr, artifacts in ((OVERVIEW_ATTR, probes[window]), (COLUMN_ATTR, col_probes[window])):
            for name, attrs in sorted(artifacts.items()):
                block = (attrs or {}).get(attr)
                if isinstance(block, dict) and block.get("window") != window:
                    counted["readback"] += 1
                    errors["readback"].append(
                        f"{name}[{window}]: {attr!r} records window {block.get('window')!r} — "
                        f"the artifact is another window's, misfiled under this name"
                    )

    windows = sorted(by_window)
    if not full and len(windows) > sample_windows:
        picks = np.random.default_rng(seed).choice(len(windows), sample_windows, replace=False)
        windows = sorted(windows[int(p)] for p in picks)
        harness.warn(
            f"value checks ran on {len(windows)} of {len(by_window)} window(s) {windows} "
            f"(--sample-windows); the artifact rosters cover every window"
        )
    for window in windows:
        bound = _Harness(
            harness.store_root,
            harness.manifest,
            harness.store_kwargs,
            rng=harness.rng,
            sample_nodes=harness.sample_nodes,
            sample_cells=harness.sample_cells,
            window=window,
        )
        found: dict = {name: [] for name in _VALUE_CHECKS}
        by_order = {
            k: {n: probes[window].get(n) for n in nodes} for k, nodes in declared[window].items()
        }
        _tier_checks(
            bound,
            ladder,
            declared[window],
            by_order,
            col_probes[window],
            by_window[window],
            entries,
            groups,
            found,
            counted,
            full=full,
        )
        for name in _VALUE_CHECKS:
            errors[name].extend(f"window {window}: {e}" for e in found[name])
        for message in bound.warnings:
            harness.warn(f"window {window}: {message}")

    # The finisher's per-entry actuals describe the per-window ladder (§4.4).
    orders = [k for k, _ in ladder]
    declared_any = {k: sorted({n for w in by_window for n in declared[w][k]}) for k in orders}
    landed = {
        k: {
            n: all(probes[w].get(n) is not None for w in by_window if n in declared[w][k]) or None
            for n in declared_any[k]
        }
        for k in orders
    }
    raw_entries = (harness.manifest.get("pyramid") or {}).get("overviews") or []
    _actuals_errors(
        raw_entries, harness.shard_order, harness, errors, counted, landed, declared_any
    )


class _WindowSources(_Harness):
    """A harness whose fold source is ONE node's per-window overviews (§4.4).

    The all-time fold is cell-for-cell at the level's own resolution, so a
    source tier is ``(node, resolution, windows)`` and every window's
    overview contributes its own cell ``j`` to output cell ``j``.
    """

    def _cell(self, cell_dec, source):
        node, r, windows = source
        j = _rank(cell_dec[len(node) :])
        for window in windows:
            try:
                yield window, self._open(_node_object_rel(node, window), r), j
            except Exception as exc:
                self.warn(f"{node}[{window}]: overview unreadable ({exc}) — cells skipped")
                yield window, None, j

    def contributions(self, cell_dec, source, field):
        out, complete = [], True
        for window, group, j in self._cell(cell_dec, source):
            if group is None:
                complete = False
            elif field in group:
                out.append(np.asarray(group[field][j : j + 1]))
            else:
                self.warn(f"{source[0]}[{window}] lacks field {field!r} — contributes fill")
        return out, complete

    def paired_contributions(self, cell_dec, source, word_field, of_field):
        parts, poisoned, complete = [], False, True
        for _window, group, j in self._cell(cell_dec, source):
            if group is None:
                complete = False
                continue
            has_word, has_of = word_field in group, of_field in group
            if has_of and not has_word:
                poisoned = True
            elif has_of:
                parts.append(
                    (
                        np.asarray(group[word_field][j : j + 1]),
                        np.asarray(group[of_field][j : j + 1]),
                    )
                )
        return parts, poisoned, complete


def _node_window_labels(store, node: str) -> set:
    """The window labels with an overview object at ``node``: one delimiter LIST.

    The all-time fold covers every window the node HAS (§4.4), which the
    roster may understate — a leaf whose run record never landed is still
    swept — so the harness asks the store.
    """
    import obstore

    from zagg.sweep import _node_rel
    from zagg.windows import SCHEDULE_NONE_TOKEN, validate_label

    labels = set()
    for prefix in obstore.list_with_delimiter(store, _node_rel(node) + "/")["common_prefixes"]:
        name = str(prefix).rstrip("/").rsplit("/", 1)[-1]
        if not name.endswith(".zarr") or name.endswith(".pyramid.zarr"):
            continue
        label = name.removesuffix(".zarr")
        try:
            if label != SCHEDULE_NONE_TOKEN:
                labels.add(validate_label(label))
        except ValueError:
            continue  # a leaf, a child digit, or foreign debris
    return labels


def _all_time_check(
    harness, ladder, declared, probes, groups, checks, report, *, all_time, full
) -> None:
    """Settle ``all_time``: the all-time fold at every ladder node (§4.4).

    Declared, every ladder node that has a leaf beneath it in any window must
    carry a committed ``all.zarr`` (one GET each); ``sample_nodes`` of them
    per order (all in full mode) are then held to the fold's contract — see
    :func:`_all_time_node`. Undeclared, there is nothing to hold: reported
    *not applicable*.
    """
    from zagg.store import open_object_store

    if not all_time:
        checks["all_time"] = _entry(
            "skip",
            "not applicable — the manifest declares no pyramid.overview.all_time, so no node "
            "carries an all-time fold (§4.5); nothing was probed",
        )
        return
    orders = {k: sorted({n for w in declared for n in declared[w][k]}) for k, _ in ladder}
    probed, errored = _probe_nodes(
        harness.store_root, [n for k in orders for n in orders[k]], harness.store_kwargs
    )
    if errored:
        checks["all_time"] = _entry(
            "fail",
            f"{len(errored)} probe error(s) — all-time state is UNKNOWN, not a verdict: "
            f"{[f'{n}: {e}' for n, e in sorted(errored.items())][:3]}",
        )
        return
    committed = {n: attrs for n, attrs in probed.items() if _committed(attrs)}
    missing = sorted(set(probed) - set(committed))
    report["all_time_nodes"] = {
        "declared": len(probed),
        "materialized": len(committed),
        "missing": missing[:50],
    }
    if not committed:
        checks["all_time"] = _entry(
            "fail",
            f"declared but no committed all-time fold: 0/{len(probed)} nodes — the node close "
            f"has not run",
        )
        return
    sources = _WindowSources(
        harness.store_root,
        harness.manifest,
        harness.store_kwargs,
        rng=harness.rng,
        sample_nodes=harness.sample_nodes,
        sample_cells=harness.sample_cells,
    )
    store = open_object_store(harness.store_root, **harness.store_kwargs)
    errors: dict = {name: [] for name in _VALUE_CHECKS}
    counted = dict.fromkeys(_VALUE_CHECKS, 0)
    checked = 0
    for k, r in ladder:
        nodes = [n for n in orders[k] if n in committed]
        if not full and len(nodes) > harness.sample_nodes:
            picks = harness.rng.choice(len(nodes), harness.sample_nodes, replace=False)
            nodes = sorted(nodes[int(p)] for p in picks)
        for node in nodes:
            checked += 1
            _all_time_node(
                sources, store, node, k, r, committed[node], probes, groups, errors, counted, full
            )
    for message in sources.warnings:
        harness.warn(message)
    found = [e for name in _VALUE_CHECKS for e in errors[name]]
    if missing:
        found.insert(
            0, f"{len(missing)} declared node(s) carry no committed all.zarr: {missing[:8]}"
        )
    if found:
        checks["all_time"] = _entry(
            "fail", f"{len(found)} mismatch(es); first: {found[0]}", mismatches=found[:20]
        )
        return
    if groups[0] is not None and not counted["counts"]:
        # Zero comparisons is not a pass (the settlement rule of the value checks).
        checks["all_time"] = _entry(
            "fail", "0 all-time comparison(s) performed — NOTHING was validated"
        )
        return
    checks["all_time"] = _entry(
        "pass",
        f"{len(committed)}/{len(probed)} nodes carry the all-time fold; {checked} checked "
        f"against the k-way fold of their own per-window overviews — {counted['counts']} "
        f"count, {counted['digests']} digest, {counted['composition']} composition "
        f"comparison(s) — with the §4.4 provenance (regime, merges_from_raw, source_windows, "
        f"source_children, generation)",
    )


def _all_time_node(
    sources, store, node, k, r, attrs, probes, groups, errors, counted, full
) -> None:
    """One node's all-time fold against the windows the node holds (§4.4).

    The windows are the node's own ``{window}.zarr`` objects (one LIST); a
    listed window the roster did not name is probed here. The recorded
    provenance is held to the spec's statement of it, and the fold's
    ``source_windows`` to the windows committed NOW: a fold that consumed
    fewer than the node holds, or that stamped a window missing which has
    since landed, is stale. Only a fold whose sources are the ones on disk
    is compared by value — against a stale one every cell would differ, and
    the finding is the staleness.
    """
    from zagg.sweep_overview import OVERVIEW_ATTR

    count_meta, exact_fields, digest_fields, _wide, packed_fields = groups
    try:
        labels = _node_window_labels(store, node)
    except Exception as exc:
        errors["readback"].append(f"{node}[all]: cannot list the node's windows ({exc})")
        return
    # Every window the node is known to have: the roster's and the store's own.
    expected = labels | {w for w in probes if node in probes[w]}
    held: dict = {}
    for window in sorted(expected):
        known = probes.get(window, {})
        if node not in known:
            sources.warn(
                f"{node}[{window}]: an overview the leaf roster does not account for — the "
                f"roster understates this window; its ladder was not validated"
            )
            known, errored = _probe_nodes(
                sources.store_root,
                [node],
                sources.store_kwargs,
                rel=partial(_node_object_rel, window=window),
            )
            if errored:
                errors["readback"].append(f"{node}[{window}]: probe failed ({errored[node]})")
                return
        if _committed(known.get(node)):
            held[window] = known[node]
    prov = attrs.get(OVERVIEW_ATTR)
    stale = _all_time_provenance(sources, node, k, r, prov, held, len(expected), errors)
    _check_node(
        sources,
        node,
        k,
        r,
        (node, r, sorted(held)),
        attrs,
        count_meta,
        exact_fields,
        digest_fields,
        packed_fields,
        errors,
        counted,
        full=full,
        values=not stale,
    )


def _all_time_provenance(sources, node, k, r, prov, held, n_expected, errors) -> bool:
    """The all-time artifact's ``zagg-overview/2`` attrs vs §4.4; True when stale.

    ``held`` maps each window committed at the node to its overview's attrs —
    the fold's sources as they are on disk now — and ``n_expected`` counts
    the windows the node is known to have, committed or not. Appends
    read-back findings; returns whether the fold's recorded sources are NOT
    the ones on disk (so its values are not comparable to them).
    """
    from zagg.pyramid_check_v2 import _as_int
    from zagg.sweep_overview import OVERVIEW_ATTR
    from zagg.sweep_stage import OVERVIEW_SPEC_V2, STAGE_GATHER, STAGE_MERGE, classify_level
    from zagg.windows import SCHEDULE_NONE_TOKEN

    name = f"{node}[all]"
    if not isinstance(prov, dict):
        if prov is not None:
            errors["readback"].append(f"{name}: {OVERVIEW_ATTR!r} attrs are not a mapping")
        return False  # absent: _check_node's read-back leg names it
    out = errors["readback"]
    if prov.get("spec") != OVERVIEW_SPEC_V2:
        out.append(f"{name}: attrs spec {prov.get('spec')!r} != {OVERVIEW_SPEC_V2!r}")
    if _as_int(prov.get("order")) != int(k) or _as_int(prov.get("cell_order")) != int(r):
        out.append(
            f"{name}: attrs order/cell_order ({prov.get('order')}, {prov.get('cell_order')}) "
            f"!= level entry ({k}, {r})"
        )
    if prov.get("window") != SCHEDULE_NONE_TOKEN:
        out.append(f"{name}: attrs window {prov.get('window')!r} != {SCHEDULE_NONE_TOKEN!r}")
    if prov.get("regime") != STAGE_MERGE:
        out.append(
            f"{name}: regime {prov.get('regime')!r} != {STAGE_MERGE!r} — the all-time fold is a "
            f"merge across windows at every level (§4.4)"
        )
    gather = classify_level(r, shard_order=sources.shard_order) == STAGE_GATHER
    expected_mfr = 2 if gather else 3
    if _as_int(prov.get("merges_from_raw")) != expected_mfr:
        out.append(
            f"{name}: merges_from_raw {prov.get('merges_from_raw')} != {expected_mfr} — one more "
            f"than its per-window sources', which are "
            f"{'gathers (1)' if gather else 'merges (2)'} at cells {r} (§4.4)"
        )
    if not prov.get("run_id"):
        out.append(f"{name}: no run_id in the zagg-overview/2 attrs (§4.4)")
    sw = prov.get("source_windows")
    if not isinstance(sw, dict) or not set(_COUNTERS) <= set(sw):
        out.append(f"{name}: source_windows {sw!r} lacks the folded/missing/unreadable counters")
        return True
    folded, missing, unreadable = (_as_int(sw.get(c)) for c in _COUNTERS)
    if None in (folded, missing, unreadable):
        out.append(f"{name}: source_windows {sw!r} carries a non-integer counter (§4.4)")
        return True
    if folded != len(held):
        out.append(
            f"{name}: STALE all-time fold — it folded {sw.get('folded')} window overview(s) and "
            f"the node now holds {len(held)} committed {sorted(held)}; re-sweep"
        )
        return True
    if missing or unreadable:
        if len(held) == n_expected:
            out.append(
                f"{name}: STALE all-time fold — it records source_windows {sw} while all "
                f"{n_expected} of the node's window overviews are committed; re-sweep"
            )
        else:
            # Honestly short: the absent window already failed ``materialization``.
            sources.warn(
                f"{name}: the all-time fold under-covers ({sw}; {n_expected - len(held)} of the "
                f"node's {n_expected} window(s) uncommitted) — compared against the "
                f"{len(held)} it folded; the next sweep heals it"
            )
    parsed = {window: _source_block(a) for window, a in held.items()}
    bad = sorted(window for window, p in parsed.items() if p is None)
    if bad:
        out.extend(
            f"{node}[{window}]: malformed {OVERVIEW_ATTR!r} source_children/generation attrs — "
            f"the all-time fold's sources cannot be summed (§4.4)"
            for window in bad
        )
        return False
    children = {c: sum(p[0][c] for p in parsed.values()) for c in _COUNTERS}
    recorded = prov.get("source_children")
    if (
        not isinstance(recorded, dict)
        or {c: _as_int(recorded.get(c)) for c in children} != children
    ):
        out.append(
            f"{name}: source_children {recorded!r} != the folded windows' own counters summed "
            f"{children} (§4.4)"
        )
    # §4.5's skip key over the consumed overviews: their blocks' leaf counts
    # summed, the newest leaf stamp among them, and every run id they relay
    # or were stamped by.
    stamps = [p[2] for p in parsed.values() if p[2]]
    summed = {
        "n_leaves": sum(p[1] for p in parsed.values()),
        "max_leaf_timestamp": max(stamps) if stamps else None,
        "run_ids": sorted({run for p in parsed.values() for run in p[3]}),
    }
    block = prov.get("generation")
    block = block if isinstance(block, dict) else {}
    runs = block.get("run_ids") or []
    recorded_generation = {
        "n_leaves": _as_int(block.get("n_leaves")),
        "max_leaf_timestamp": block.get("max_leaf_timestamp"),
        "run_ids": sorted(set(runs)) if _str_list(runs) else runs,
    }
    if recorded_generation != summed:
        out.append(
            f"{name}: STALE all-time fold — its generation {recorded_generation} is not its "
            f"window overviews' blocks summed {summed} (§4.4/§4.5); re-sweep"
        )
        return True
    return False


def _str_list(value) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


def _source_block(attrs) -> tuple | None:
    """A consumed window overview's ``(children, n_leaves, stamp, run ids)``, or None.

    The writer's own reading of the block (absent counters are 0, an absent
    generation is empty), plus the run that stamped it; None when the attrs
    are malformed — untrusted JSON is named, never summed into a traceback.
    """
    from zagg.hive import COMMIT_ATTR
    from zagg.pyramid_check_v2 import _as_int
    from zagg.sweep_overview import OVERVIEW_ATTR

    block = attrs.get(OVERVIEW_ATTR)
    sc = (block.get("source_children") or {}) if isinstance(block, dict) else None
    gen = (block.get("generation") or {}) if isinstance(block, dict) else None
    if not isinstance(sc, dict) or not isinstance(gen, dict):
        return None
    children = {c: _as_int(sc.get(c) or 0) for c in _COUNTERS}
    n_leaves, stamp = _as_int(gen.get("n_leaves") or 0), gen.get("max_leaf_timestamp")
    runs, run = gen.get("run_ids") or [], attrs[COMMIT_ATTR].get("run_id")
    if (
        None in children.values()
        or n_leaves is None
        or not isinstance(stamp, str | None)
        or not _str_list(runs)
        or not isinstance(run, str | None)
    ):
        return None
    return children, n_leaves, stamp, {*runs, *([run] if run else [])}
