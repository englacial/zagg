"""The ``zagg-pyramid/2`` validation arm of the E2E harness (issue #434).

Routed to by :func:`zagg.pyramid_check.validate_pyramid` when the manifest
declares ``zagg-pyramid/2`` — the grammar the #547 re-declaration uses (espg
ruling, 2026-09-11). A ``/2`` store is written by the STAGED sweep
(:mod:`zagg.sweep_stage` / :mod:`zagg.sweep_stages`, fleet transport
:mod:`zagg.sweep_fleet`), whose fold model this arm validates against what
spec §4.4/§4.6 make normative:

- **declaration** — the block-level ``overviews`` list must be exactly the
  leaf entry plus the fixed every-order ladder (§4.5: the recorded list IS
  the contract, and it is expanded, never re-derived — checked against
  :func:`zagg.pyramid.expand_overviews` of the leaf entry's resolutions);
- **materialization** — the above-shard ladder's committed ``all.zarr``
  artifacts, exactly as the ``/1`` arm counts them (shared leg);
- **columns** — the §4.6 leaf-column tier: one committed
  ``all.pyramid.zarr`` per roster leaf. The tier is the ``/2`` leaf entry's
  artifact and the gen-1 source every ladder level folds from, so 0/N here
  is the PRE-BACKFILL baseline (issue #520), distinct from the pre-sweep
  one;
- **read-back** — per ladder artifact, the ``zagg-overview/2`` attrs:
  ``regime`` must equal the DERIVED classification
  (:func:`zagg.sweep_stage.classify_level` — gather at/below the shard
  resolution, merge above it), ``merges_from_raw`` 1/2 accordingly (never 3
  upfront), ``source_children`` with its three counters, and the writing
  ``run_id``; per column, the ``zagg-column/1`` attrs and the declared
  group roster. Manifest per-entry ``actuals`` (the finisher's RMW) are
  held to the same regime law when present;
- **counts / digests / composition** — the same value laws as the ``/1``
  arm (shared ``_check_node``), re-folded from the LEAF COLUMNS: the level
  at resolution ``r`` reads each contributing leaf's column group at
  ``max(r, shard_order)`` — its own cells for a gather, its node-order
  partial for a merge — which is byte-what the staged sweep consumed (the
  merge-source law makes tuple grouping irrelevant). A gather level's
  packed word is compared as ASSIGNED gen-1 content, not re-merged (§3.4
  quantization drift is per merge). The column tier itself is validated
  against the leaf's own cell arrays — the §4.6 from-leaves parity, whose
  per-cell fold factor is set by the geometry rather than by ``sample_cells``
  and is therefore bounded by :data:`COLUMN_PARITY_FOLD_MAX` outside full
  mode;
- **idempotency** (fixture mode) — an immediate
  :func:`zagg.sweep_stages.sweep_stage_pass` re-run is a no-op (the #417
  generation ratchet), refused for ``s3://`` roots like the ``/1`` arm's.

A level whose ``source_children`` records ``missing``/``unreadable``
children claims to under-cover its subtree (§4.3). The claim is the artifact
talking about itself, so it is CROSS-CHECKED against the committed leaf
columns of that subtree (:func:`_coverage_verdict`): genuinely short sources
DECLINE the level's per-cell comparisons and are named — the fill cells
there are not evidence, and the next sweep heals it — while a level stamped
short whose sources are all committed is a STALE artifact and fails by name.
The decline never covers the structural read-back legs.
"""

from __future__ import annotations

import logging

import numpy as np

from zagg.pyramid_check_core import (
    _check_node,
    _column_object_rel,
    _committed,
    _entry,
    _field_groups,
    _finish,
    _Harness,
    _ladder_materialization,
    _leaf_roster,
    _probe_nodes,
    _settle_value_checks,
)

logger = logging.getLogger(__name__)

#: Leaf-cell fold factor above which the §4.6 from-leaves parity leg declines
#: a sampled column cell's PAYLOAD comparisons (digests, packed ``of``
#: weights). A column group at resolution ``q`` folds ``4 ** (cell_order - q)``
#: leaf cells per cell, and the group at the node order has exactly ONE cell —
#: so ``sample_cells`` cannot shrink it, and the coarsest groups would re-fold
#: a whole leaf per sampled leaf (review finding: ~1.05M ragged payloads at the
#: ATL03 o9 target, which is the leaf worker's own workload, not a sample).
#: ``4**6`` keeps a sampled cell's ragged re-fold near the ~4k payloads the
#: module header's "sample nodes and cells" bound promises; exact count parity
#: is dense and stays exhaustive, and ``full=True`` (fixture mode) is never
#: bounded at all.
COLUMN_PARITY_FOLD_MAX = 4**6


def validate_v2(
    store_root: str,
    manifest: dict,
    report: dict,
    *,
    ladder: list,
    fields: dict,
    store_kwargs: dict,
    sample_nodes: int,
    sample_cells: int,
    seed: int,
    full: bool,
    resweep: bool,
    roster: str,
) -> dict:
    """The ``/2`` checklist spine; called with the shared prologue done.

    ``report``/``report["checks"]`` arrive with the manifest read, the
    windowed-store gate, and the ladder/fields presence checks already
    settled by :func:`zagg.pyramid_check.validate_pyramid`; ``ladder`` is the
    above-shard ``(node_order, cells)`` list from the expanded manifest.
    """
    from zagg.pyramid_check import CHECKS_V2

    checks = report["checks"]

    def skip_rest(reason, *, after):
        for name in CHECKS_V2[CHECKS_V2.index(after) + 1 :]:
            if name == "idempotency" or name in checks:
                continue
            checks[name] = _entry("skip", reason)

    # -- [1] declaration: the recorded /2 grammar (§4.5).
    entries, leaf_cells, problems = _declaration_grammar(manifest)
    if problems:
        checks["declaration"] = _entry(
            "fail", f"malformed zagg-pyramid/2 declaration: {'; '.join(problems)}"
        )
        skip_rest("malformed /2 declaration", after="declaration")
        return _finish(report, CHECKS_V2)
    report["leaf_levels"] = leaf_cells
    checks["declaration"] = _entry(
        "pass",
        f"zagg-pyramid/2 leaf resolutions {leaf_cells} + fixed every-order ladder to 0 "
        f"(d={leaf_cells[-1] - int(manifest['shard_order'])}); composable fields "
        f"{sorted(fields)} (classes {report['field_classes']})",
    )

    # -- roster: what both tiers' declared rosters derive from.
    leaves, roster_source = _leaf_roster(store_root, manifest, store_kwargs, roster)
    report["roster"] = {"source": roster_source, "leaves": len(leaves)}
    if not leaves:
        checks["materialization"] = _entry("fail", f"empty leaf roster (source {roster_source})")
        skip_rest("empty roster", after="materialization")
        return _finish(report, CHECKS_V2)

    # -- [2] materialization: the above-shard ladder (shared leg).
    probes, declared, state = _ladder_materialization(
        store_root, ladder, leaves, store_kwargs, checks, report
    )
    if state == "errors":
        skip_rest("node probes failed", after="materialization")
        return _finish(report, CHECKS_V2)

    # -- [3] the §4.6 leaf-column tier — probed even at the pre-sweep
    # baseline: the campaign sequence is declare -> backfill -> sweep, so
    # "ladder 0/N, columns N/N" is exactly the post-backfill gate reading.
    col_probes, col_state = _columns_check(store_root, leaves, store_kwargs, checks, report)
    if state == "baseline":
        skip_rest("no materialized ladder nodes (pre-sweep baseline)", after="columns")
        return _finish(report, CHECKS_V2)
    if col_state == "errors":
        skip_rest("column probes failed", after="columns")
        return _finish(report, CHECKS_V2)

    # -- [4..7] value checks on sampled (or, in full mode, all) artifacts.
    rng = np.random.default_rng(seed)
    harness = _Harness(
        store_root,
        manifest,
        store_kwargs,
        rng=rng,
        sample_nodes=sample_nodes,
        sample_cells=sample_cells,
    )
    _value_checks_v2(
        harness,
        ladder,
        declared,
        probes,
        col_probes,
        leaves,
        entries,
        checks,
        report,
        full=full,
    )

    # -- [8] idempotency (fixture mode): an immediate staged re-pass is a no-op.
    if resweep:
        checks["idempotency"] = _restage_check(store_root, manifest, leaves, store_kwargs)
    return _finish(report, CHECKS_V2)


def _declaration_grammar(manifest: dict) -> tuple[list, list, list]:
    """Normalized level entries + leaf resolutions + grammar problems (§4.5).

    The recorded ``overviews`` list must be the FULLY EXPANDED form: one leaf
    entry at the shard order (resolutions strictly inside the shard's
    resolution window, strictly descending —
    :func:`zagg.pyramid.validate_overviews`), then the fixed every-order
    ladder down to node 0 — byte-what :func:`zagg.pyramid.expand_overviews`
    emits. Additive keys on an entry (``actuals``) are tolerated per §4.5;
    the comparison strips them.
    """
    from zagg.pyramid import expand_overviews, validate_overviews

    s, c = int(manifest["shard_order"]), int(manifest["cell_order"])
    raw = (manifest.get("pyramid") or {}).get("overviews") or []
    problems: list = []
    try:
        entries: list[dict] = [
            {"node": int(e["node"]), "cells": [int(x) for x in e["cells"]]} for e in raw
        ]
    except Exception as exc:
        return [], [], [f"unreadable level entry ({type(exc).__name__}: {exc})"]
    leaf_entries = [e for e in entries if e["node"] == s]
    if len(leaf_entries) != 1:
        problems.append(
            f"expected exactly one leaf entry at node {s} (the shard order); "
            f"found {len(leaf_entries)} — the /2 list is always leaf entry + fixed ladder (§4.4)"
        )
        return entries, [], problems
    leaf_cells: list[int] = [int(x) for x in leaf_entries[0]["cells"]]
    try:
        validate_overviews(leaf_cells, parent_order=s, child_order=c)
    except ValueError as exc:
        return entries, leaf_cells, [str(exc)]
    expected = expand_overviews(leaf_cells, parent_order=s)
    if entries != expected:
        problems.append(
            f"recorded overviews list is not the leaf entry + fixed every-order ladder "
            f"of §4.4 (readers never re-derive it, so the recorded list must BE the "
            f"contract); expected {expected}"
        )
    return entries, leaf_cells, problems


def _columns_check(store_root, leaves, store_kwargs, checks, report) -> tuple[dict, str]:
    """The §4.6 leaf-column roster: one committed column per roster leaf.

    One ``zarr.json`` GET per leaf (the tier is part of the declared roster —
    the ``/2`` leaf entry's artifact — and its completeness is the backfill
    acceptance: ``docs/pyramid_upgrade.md`` requires ``failed == 0`` before
    the staged sweep). Returns ``({leaf: committed attrs | None}, state)``
    with the same three-way state as the ladder leg — and the same probe-error
    discipline: a transport failure is UNKNOWN state, never a baseline.
    """
    from zagg.column import COLUMN_ROLE

    probed, errored = _probe_nodes(store_root, leaves, store_kwargs, rel=_column_object_rel)
    committed = {
        d: attrs if _committed(attrs, role=COLUMN_ROLE) else None for d, attrs in probed.items()
    }
    found = [d for d, attrs in committed.items() if attrs is not None]
    partial = sorted(
        d for d, attrs in probed.items() if attrs is not None and not _committed(attrs, COLUMN_ROLE)
    )
    probe_errors = [f"{d}: {e}" for d, e in sorted(errored.items())]
    report["columns"] = {"declared": len(leaves), "materialized": len(found)}
    report["missing_columns"] = sorted(set(leaves) - set(found))[:50]
    report["partial_columns"] = partial[:50]
    report["column_probe_errors"] = probe_errors[:50]
    debris = f"; {len(partial)} partial uncommitted column(s) {partial[:8]}" if partial else ""
    if probe_errors:
        checks["columns"] = _entry(
            "fail",
            f"{len(probe_errors)} column probe error(s) — column state is UNKNOWN, "
            f"not a backfill verdict: {probe_errors[:3]}",
        )
        return committed, "errors"
    if not found:
        checks["columns"] = _entry(
            "fail",
            f"declared but no committed leaf columns: 0/{len(leaves)} — pre-backfill "
            f"baseline (the issue #520 /1 -> /2 column backfill has not run){debris}",
        )
        return committed, "baseline"
    status = "pass" if len(found) == len(leaves) and not partial else "fail"
    checks["columns"] = _entry(
        status, f"{len(found)}/{len(leaves)} leaf columns committed (§4.6){debris}"
    )
    return committed, "ok" if status == "pass" else "short"


def _value_checks_v2(
    harness, ladder, declared, probes, col_probes, leaves, entries, checks, report, *, full
):
    """Read-back + counts + digests + composition, both /2 tiers.

    Ladder levels re-fold from the LEAF COLUMNS (the gen-1 tier the staged
    sweep itself consumes — the merge-source law makes the tuple grouping
    between them irrelevant); the column tier re-folds from the leaf's own
    cell arrays (§4.6 from-leaves parity). The composition compare is always
    exact here: the /2 regime is DERIVED from the geometry
    (:func:`zagg.sweep_stage.classify_level`), never guessed.
    """
    from zagg.column import column_resolutions
    from zagg.sweep_overview import OVERVIEW_ATTR
    from zagg.sweep_stage import STAGE_GATHER, classify_level

    errors: dict = {"readback": [], "counts": [], "digests": [], "composition": []}
    counted = {"readback": 0, "counts": 0, "digests": 0, "composition": 0}
    count_meta, exact_fields, digest_fields, wide_fields, packed_fields = _field_groups(
        harness, report
    )
    s = harness.shard_order

    # -- the above-shard ladder, from the leaf-column tier.
    for k, r in ladder:
        gather = classify_level(r, shard_order=s) == STAGE_GATHER
        q = max(r, s)
        tier = (s, q, leaves, lambda dec, q=q: harness.column_group(dec, q))
        nodes = [n for n in declared[k] if probes[k].get(n) is not None]
        if not full and len(nodes) > harness.sample_nodes:
            picks = harness.rng.choice(len(nodes), harness.sample_nodes, replace=False)
            nodes = sorted(nodes[int(p)] for p in picks)
        for node in nodes:
            attrs = probes[k][node]
            prov = attrs.get(OVERVIEW_ATTR)
            errors["readback"].extend(_stage_provenance_errors(node, k, r, s, prov, gather))
            sc = (prov or {}).get("source_children") if isinstance(prov, dict) else None
            sc = sc if isinstance(sc, dict) else {}
            values = _coverage_verdict(node, sc, leaves, col_probes, harness, errors)
            _check_node(
                harness,
                node,
                k,
                r,
                tier,
                attrs,
                count_meta,
                exact_fields,
                digest_fields,
                packed_fields,
                errors,
                counted,
                full=full,
                compose_exact=True,
                gather=gather,
                values=values,
            )

    # -- the leaf-column tier, from the leaves' own cell arrays (§4.6 parity).
    resolutions = column_resolutions(entries, s)
    leaf_tier = (s, harness.cell_order, leaves, harness.leaf_group)
    col_nodes = [d for d in leaves if col_probes.get(d) is not None]
    if not full and len(col_nodes) > harness.sample_nodes:
        picks = harness.rng.choice(len(col_nodes), harness.sample_nodes, replace=False)
        col_nodes = sorted(col_nodes[int(p)] for p in picks)
    for dec in col_nodes:
        attrs = col_probes[dec]
        errors["readback"].extend(_column_attrs_errors(dec, s, attrs, resolutions))
        for q in resolutions:
            # Bound the fold: one cell of the group at ``q`` covers
            # ``4 ** (cell_order - q)`` leaf cells, whatever ``sample_cells``
            # says, so the payload legs are declined (and NAMED) above
            # :data:`COLUMN_PARITY_FOLD_MAX` outside full mode.
            fold = 4 ** (harness.cell_order - q)
            refold = full or fold <= COLUMN_PARITY_FOLD_MAX
            if not refold:
                harness.warn(
                    f"column group [{q}]: each cell folds {fold} leaf cells "
                    f"(> {COLUMN_PARITY_FOLD_MAX}) — §4.6 digest/composition parity "
                    f"declined for this group in sampled mode (counts still compared); "
                    f"rerun --full on a fixture-scale store to check it"
                )
            _check_node(
                harness,
                dec,
                s,
                q,
                leaf_tier,
                attrs,
                count_meta,
                exact_fields,
                digest_fields,
                packed_fields,
                errors,
                counted,
                full=full,
                compose_exact=True,
                role="column",
                provenance_attr="zagg_column",
                group_getter=harness.column_group,
                refold_payloads=refold,
            )

    # -- the finisher's manifest actuals (#381 point (7)), when recorded.
    # Read from the RAW manifest entries: the grammar-normalized ``entries``
    # deliberately strip additive keys, ``actuals`` included.
    raw_entries = (harness.manifest.get("pyramid") or {}).get("overviews") or []
    _actuals_errors(raw_entries, s, harness, errors, counted, probes, declared)

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


def _as_int(value) -> int | None:
    """``int(value)`` or None — attrs are untrusted JSON, and this is a reader.

    Absent and malformed both become None, which never equals an expected
    counter, so the caller's one comparison covers "no claim at all" and
    "an unreadable claim" alike without a traceback out of a read-only run.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coverage_verdict(node, sc, leaves, col_probes, harness, errors) -> bool:
    """Whether one ladder node's per-cell value comparisons may run (§4.3).

    A level's ``source_children`` counters are written by the ARTIFACT UNDER
    TEST about itself, so taking them on faith would let a corrupt ladder
    stamp ``missing: 1`` and decline its way past the gate (review finding:
    reproduced as a ``PASS`` on nine corrupted nodes). The claim is therefore
    CROSS-CHECKED against the roster the harness already holds: a node's
    sources are the committed leaf columns of its subtree (the §4.6 gen-1
    tier), which :func:`_columns_check` just probed.

    - stamped short **and** some source column is genuinely absent — the
      benign case: decline the per-cell comparisons (a fill cell there is not
      evidence) and name it; the ``columns`` check carries the failure;
    - stamped short while EVERY source column is committed — a STALE
      artifact whose sources have since healed: a read-back failure by name,
      and the value checks run, because the tier they compare against is
      complete;
    - not stamped short — the ordinary path.

    Either way the structural read-back legs run (``values=False`` keeps
    role/provenance/arrays/companions/morton/§3.3 attrs), so nothing is
    rubber-stamped and nothing is declined that coverage does not touch.
    """
    if not (int(sc.get("missing") or 0) or int(sc.get("unreadable") or 0)):
        return True
    subtree = [d for d in leaves if d.startswith(node)]
    uncommitted = [d for d in subtree if col_probes.get(d) is None]
    if not uncommitted:
        errors["readback"].append(
            f"{node}: records source_children {sc} — under-covers its subtree — while "
            f"all {len(subtree)} of its source column(s) are committed: a STALE level "
            f"whose sources have since healed; re-sweep"
        )
        return True
    harness.warn(
        f"{node}: level under-covers its subtree ({sc}; {len(uncommitted)}/{len(subtree)} "
        f"source column(s) uncommitted, e.g. {uncommitted[:3]}) — per-cell value checks "
        f"declined; a fill cell there is not evidence (§4.3), and the next sweep heals it"
    )
    return False


def _stage_provenance_errors(node, k, r, s, prov, gather) -> list:
    """One ladder artifact's ``zagg-overview/2`` attrs vs the §4.4 contract.

    An ABSENT block is left to :func:`_check_node`'s read-back leg (one
    report line, not two). A block that is present but not a MAPPING is an
    error here: that leg tests key presence only, so a ``zagg_overview`` set
    to a string satisfies it and would otherwise skip this whole contract
    silently (review finding). A present one must record the DERIVED regime —
    gather at/below the shard resolution, merge above — with its
    merges-from-raw at 1/2 (never 3 upfront: gen 3 belongs only to the
    append-later cascade regime), the ``source_children`` counters (present
    in BOTH stage regimes), and the writing ``run_id``. ``merges_from_raw``
    is REQUIRED like its two siblings — §4.4 makes it normative on a ``/2``
    stage artifact, and an absent key is the one way to make no claim at all
    (review finding: it used to pass, alone among the three).
    """
    from zagg.sweep_overview import OVERVIEW_ATTR
    from zagg.sweep_stage import OVERVIEW_SPEC_V2, STAGE_GATHER, STAGE_MERGE

    if prov is None:
        return []
    if not isinstance(prov, dict):
        return [
            f"{node}: {OVERVIEW_ATTR!r} attrs are a {type(prov).__name__}, not the §4.4 "
            f"provenance mapping — nothing in the block can be validated"
        ]
    errs = []
    if prov.get("spec") != OVERVIEW_SPEC_V2:
        errs.append(f"{node}: attrs spec {prov.get('spec')!r} != {OVERVIEW_SPEC_V2!r}")
    if int(prov.get("order", -1)) != int(k) or int(prov.get("cell_order", -1)) != int(r):
        errs.append(
            f"{node}: attrs order/cell_order ({prov.get('order')}, {prov.get('cell_order')}) "
            f"!= level entry ({k}, {r})"
        )
    expected_regime = STAGE_GATHER if gather else STAGE_MERGE
    if prov.get("regime") != expected_regime:
        errs.append(
            f"{node}: regime {prov.get('regime')!r} != derived {expected_regime!r} "
            f"(cells {r} vs shard order {s}, §4.4)"
        )
    expected_mfr = 1 if gather else 2
    if _as_int(prov.get("merges_from_raw")) != expected_mfr:
        errs.append(
            f"{node}: merges_from_raw {prov.get('merges_from_raw')} != {expected_mfr} "
            f"for a {expected_regime} level (never 3 for an upfront level, §4.4)"
        )
    sc = prov.get("source_children")
    if not isinstance(sc, dict) or not {"folded", "missing", "unreadable"} <= set(sc):
        errs.append(
            f"{node}: source_children {sc!r} does not carry the folded/missing/"
            f"unreadable counters (present in both stage regimes, §4.4)"
        )
    if not prov.get("run_id"):
        errs.append(f"{node}: no run_id in the zagg-overview/2 attrs (§4.4)")
    return errs


def _column_attrs_errors(dec, s, attrs, resolutions) -> list:
    """One leaf column's ``zagg_column`` attrs vs the §4.6 contract.

    Absent is :func:`_check_node`'s read-back leg (one line, not two);
    present-but-not-a-mapping is an error HERE, since that leg tests key
    presence only and cannot see it (review finding).
    """
    from zagg.column import COLUMN_ATTR, COLUMN_SPEC

    block = attrs.get(COLUMN_ATTR)
    if block is None:
        return []
    if not isinstance(block, dict):
        return [
            f"{dec}: {COLUMN_ATTR!r} attrs are a {type(block).__name__}, not the §4.6 "
            f"column mapping — nothing in the block can be validated"
        ]
    errs = []
    if block.get("spec") != COLUMN_SPEC:
        errs.append(f"{dec}: column attrs spec {block.get('spec')!r} != {COLUMN_SPEC!r}")
    if block.get("node") != dec or int(block.get("order", -1)) != int(s):
        errs.append(
            f"{dec}: column attrs node/order ({block.get('node')!r}, {block.get('order')}) "
            f"!= ({dec!r}, {s})"
        )
    raw_groups = block.get("groups")
    groups: dict = raw_groups if isinstance(raw_groups, dict) else {}
    got = sorted((int(g) for g in groups), reverse=True)
    if got != list(resolutions):
        errs.append(
            f"{dec}: column groups {got} != declared resolutions {list(resolutions)} (§4.6)"
        )
    for res, g in groups.items():
        if not isinstance(g, dict):
            continue
        if g.get("regime") != "leaf-column" or int(g.get("merges_from_raw", 0)) != 1:
            errs.append(
                f"{dec}[{res}]: group provenance ({g.get('regime')!r}, "
                f"{g.get('merges_from_raw')}) != ('leaf-column', 1) — every column group "
                f"folds directly from the leaf's resident cells (§4.6)"
            )
        expected_cells = 4 ** (int(res) - int(s))
        if int(g.get("n_cells", -1)) != expected_cells:
            errs.append(f"{dec}[{res}]: n_cells {g.get('n_cells')} != 4^({res}-{s})")
    if resolutions and int(block.get("cells_with_data_order", -1)) != int(resolutions[0]):
        errs.append(
            f"{dec}: cells_with_data_order {block.get('cells_with_data_order')} != finest "
            f"group {resolutions[0]}"
        )
    return errs


def _actuals_errors(entries, s, harness, errors, counted, probes, declared) -> None:
    """The finisher's per-entry ``actuals`` (§4.5), held to the regime law.

    ``actuals`` is written by the finisher's manifest RMW and is bookkeeping,
    not presence (declared-but-unmaterialized stays legal), so an ABSENT
    block is at most a warning — flagged when the entry's ladder tier is
    fully materialized, i.e. a sweep clearly ran but its finisher's record
    did not land. A PRESENT block must record the leaf-column law at the
    leaf entry and the derived stage regime at 1/2 merges-from-raw above it.

    §4.5 calls ``actuals`` an additive key a reader must TOLERATE, and this
    module's contract is a verdict on a malformed store, never a traceback
    (review finding): a block that is not a mapping, or whose counters do
    not read as integers, is a read-back error by name.
    """
    from zagg.sweep_stage import STAGE_GATHER, STAGE_MERGE, classify_level

    for e in entries:
        node, a = int(e["node"]), e.get("actuals")
        if a is None:
            if (
                node < s
                and declared.get(node)
                and all(probes[node].get(n) is not None for n in declared[node])
            ):
                harness.warn(
                    f"ladder order {node}: fully materialized but the manifest entry "
                    f"records no actuals — the finisher's manifest RMW may not have landed"
                )
            continue
        counted["readback"] += 1
        if not isinstance(a, dict):
            errors["readback"].append(
                f"manifest actuals for node {node}: a {type(a).__name__}, not the §4.5 "
                f"mapping — the finisher's record cannot be validated"
            )
            continue
        if node == s:
            if a.get("regime") != "leaf-column" or _as_int(a.get("merges_from_raw")) != 1:
                errors["readback"].append(
                    f"manifest actuals for the leaf entry (node {node}): "
                    f"({a.get('regime')!r}, {a.get('merges_from_raw')!r}) != ('leaf-column', 1)"
                )
            continue
        r = int(e["cells"][0])
        expected = classify_level(r, shard_order=s)
        mfr = 1 if expected == STAGE_GATHER else 2
        if a.get("regime") not in (STAGE_GATHER, STAGE_MERGE):
            errors["readback"].append(
                f"manifest actuals for node {node}: unknown regime {a.get('regime')!r}"
            )
        elif a.get("regime") != expected or _as_int(a.get("merges_from_raw")) != mfr:
            errors["readback"].append(
                f"manifest actuals for node {node}: ({a.get('regime')!r}, "
                f"{a.get('merges_from_raw')!r}) != derived ({expected!r}, {mfr}) (§4.5)"
            )


def _restage_check(store_root, manifest, leaves, store_kwargs) -> dict:
    """Fixture-mode skip gate (issue #417 ratchet): a staged re-pass is a no-op."""
    import uuid

    from zagg.hive import _utcnow
    from zagg.sweep_stages import sweep_stage_pass

    if str(store_root).startswith("s3://"):
        return _entry("fail", "resweep refused for s3:// roots — production sweeps are fleet-side")
    summary = sweep_stage_pass(
        store_root,
        manifest,
        {d: {None} for d in leaves},
        run_id=f"pyramid-check-{uuid.uuid4().hex[:6]}",
        run_started=_utcnow(),
        store_kwargs=store_kwargs,
    )
    moved = {
        name: sum(int(row.get(name) or 0) for row in summary["stages"])
        for name in ("written", "failed", "columns_written")
    }
    current = sum(int(row.get("current") or 0) for row in summary["stages"])
    if any(moved.values()):
        return _entry("fail", f"staged re-sweep was not a no-op: {moved}")
    return _entry("pass", f"immediate staged re-sweep is a no-op ({current} current)")
