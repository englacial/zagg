"""Overview-pyramid E2E validation harness (issue #434).

Read-only acceptance gate for a swept multiresolution store: given a hive
store root (local path or ``s3://``), check the declared pyramid ladder
against the materialized overview objects and validate fold-correctness
against the fold's own source tier — declaration ↔ materialized nodes, exact
count conservation, t-digest k-way fold-correctness (weight-exact, CDF
within tolerance), and the packed composition word (issue #515). Value
checks SAMPLE nodes and cells so a production store is read, never swept:
the only store-wide operations are the leaf roster (root ``coverage.moc``,
or one flat list), one small metadata GET per declared node, and — on a
``/2`` store — one per leaf column (the §4.6 tier is part of the declared
roster there).

Two modes share every check:

- **Fixture mode** (``full=True``): a local store the test suite built and
  swept (``tests/test_pyramid_e2e.py`` / ``tests/test_pyramid_e2e_v2.py``) —
  every node, every populated cell, ladder-total conservation, and
  ``resweep=True`` for the skip-gate idempotency check (an immediate
  re-sweep is a no-op, issues #417/#421).
- **Production mode** (CLI): anonymous read-only sampling against the
  published stores::

      python -m zagg.pyramid_check \\
          s3://us-west-2.opendata.source.coop/englacial/zagg/demo/atl03_tdigest_o9.zarr \\
          --anon

  Against a declared-but-unswept store this reports the pre-sweep baseline
  (0/N declared nodes materialized) cleanly and exits nonzero — it never
  raises for that. The sweep itself is fleet-side (issue #547); production
  idempotency is asserted there, and this harness only reads.

The printed checklist mirrors issue #434's phases: declaration (the
build-side contract), materialization (the sweep's output), read-back, then
the conservation checks. Digest comparisons re-fold contributors with the
k-way merge law at the manifest's overview δ (issue #424) and re-sort
centroids by mean before any CDF interpolation — concatenation does not
preserve the sort, and an unsorted digest interpolates garbage (the
sort-before-interp trap).

Both pyramid grammars are VALIDATED, each against its own fold model
(the [espg ruling on issue #547](https://github.com/englacial/zagg/issues/547),
2026-09-11: the re-declaration is ``/2`` and this harness is its acceptance
gate). This module owns the ``zagg-pyramid/1`` arm — the level-from-level
cascade written by :mod:`zagg.sweep_overview`, each node's fold source read
from its OWN recorded provenance (``zagg_overview.fold_source`` /
``fold_from_order``): ``fold_source: "leaves"``, ``exact_levels > 1`` and
the gap-wider-than-the-slab fallback all fold from the raw leaves instead
of the next finer level. A ``zagg-pyramid/2`` store routes to
:mod:`zagg.pyramid_check_v2` — the staged-sweep arm (:mod:`zagg.sweep_stage`),
which validates the ``/2`` declaration grammar, the §4.6 leaf-column tier,
and the ladder against the gen-1 columns under the derived
stage-gather/stage-merge regimes. The grammar-independent machinery both
arms share lives in :mod:`zagg.pyramid_check_core`.
"""

from __future__ import annotations

import json
import logging

import numpy as np

from zagg.pyramid_check_core import (
    CDF_TOL,  # noqa: F401  (re-exported: the harness's public tolerances)
    PROBE_QUANTILES,  # noqa: F401
    WEIGHT_RTOL,  # noqa: F401
    _check_node,
    _composable_fields,
    _entry,
    _field_groups,
    _finish,
    _Harness,
    _ladder_materialization,
    _leaf_roster,
    _missing_mask,
    _settle_value_checks,
)

logger = logging.getLogger(__name__)

#: Checklist keys, in print order (mirrors issue #434's phases) — the ``/1`` arm.
CHECKS = (
    "declaration",
    "materialization",
    "readback",
    "counts",
    "digests",
    "composition",
    "idempotency",
)

#: The ``/2`` arm's checklist: the same phases plus the §4.6 leaf-column tier
#: (``columns``), between the ladder materialization and the value checks.
CHECKS_V2 = (
    "declaration",
    "materialization",
    "columns",
    "readback",
    "counts",
    "digests",
    "composition",
    "idempotency",
)


def _ladder(manifest: dict) -> list[tuple[int, int]]:
    """Above-shard ``(node_order, overview_cell_order)`` pairs, finest first.

    ``zagg-pyramid/1``: constant depth at the declared ``orders`` (spec §4.4,
    ``t = c - (s - k)``). ``zagg-pyramid/2``: the manifest's fully expanded
    ``overviews`` list is the reader contract (readers never re-derive the
    ladder); every above-shard entry carries exactly one member.
    """
    pyramid = manifest.get("pyramid") or {}
    if not isinstance(pyramid, dict):
        return []
    s, c = int(manifest["shard_order"]), int(manifest["cell_order"])
    if pyramid.get("overviews"):
        pairs = [
            (int(e["node"]), int(max(e["cells"])))
            for e in pyramid["overviews"]
            if int(e["node"]) < s
        ]
        return sorted(pairs, reverse=True)
    orders = (pyramid.get("overview") or {}).get("orders") or []
    ks = sorted({int(k) for k in orders if 0 <= int(k) < s}, reverse=True)
    return [(k, c - (s - k)) for k in ks]


def validate_pyramid(
    store_root: str,
    *,
    store_kwargs: dict | None = None,
    sample_nodes: int = 3,
    sample_cells: int = 8,
    seed: int = 0,
    full: bool = False,
    resweep: bool = False,
    roster: str = "auto",
) -> dict:
    """Run the issue #434 checklist against a store; return the report dict.

    ``full=True`` (fixture mode) checks every node and every populated cell
    and adds ladder-total count conservation; otherwise ``sample_nodes``
    nodes per declared order and ``sample_cells`` populated cells per node
    (plus one empty cell), drawn with ``seed``. ``resweep=True`` re-runs the
    store's own sweep on a LOCAL store and requires it to be a no-op
    (refused for ``s3://`` roots: production sweeps are fleet-side, issue
    #547). The report's ``checks`` map carries one ``status``/``detail``
    entry per :data:`CHECKS` phase (:data:`CHECKS_V2` on a ``/2`` store);
    ``passed`` is True iff no check failed.

    ``full=True`` is REFUSED for ``s3://`` roots, like ``resweep``: it voids
    every bound in the module header — ``_ladder_totals`` alone reads every
    leaf's whole ``count`` array (~12 GB across the ATL03 o9 roster, whose
    leaves are ``4**10`` int32 each) before the per-cell arm walks every
    populated cell of every node (review finding).
    """
    from zagg.hive import read_manifest

    if full and str(store_root).startswith("s3://"):
        raise ValueError(
            "full=True is refused for s3:// roots — it reads every leaf's whole count "
            "array and every populated cell of every node, which is the bound sampling "
            "exists to keep; run the sampled default (optionally with larger "
            "--sample-nodes/--sample-cells), or --full against a local store"
        )
    store_kwargs = dict(store_kwargs or {})
    report: dict = {"store": store_root, "checks": {}}
    checks = report["checks"]

    def skip_rest(reason, *, after):
        for name in CHECKS[CHECKS.index(after) + 1 :]:
            if name == "idempotency":
                continue
            checks[name] = _entry("skip", reason)

    manifest = read_manifest(store_root, **store_kwargs)
    checks["idempotency"] = _entry(
        "skip", "production sweeps are fleet-side (issue #547); asserted in fixture mode"
    )
    if manifest is None:
        checks["declaration"] = _entry("fail", "no morton_hive.json manifest at the store root")
        skip_rest("no manifest", after="declaration")
        return _finish(report, CHECKS)

    # -- [1] declaration: the build-side contract.
    report["spec"] = manifest.get("spec")
    pyramid = manifest.get("pyramid") or {}
    report["pyramid_spec"] = pyramid.get("spec") if isinstance(pyramid, dict) else None
    if manifest.get("temporal") is not None:
        checks["declaration"] = _entry(
            "fail",
            "windowed store — this harness validates unwindowed stores only "
            "(both issue #547 targets are unwindowed)",
        )
        skip_rest("windowed store", after="declaration")
        return _finish(report, CHECKS)
    ladder = _ladder(manifest)
    fields = _composable_fields(manifest)
    classes: dict = {}
    decl_fields = ((pyramid.get("overview") or {}).get("fields") or {}) if pyramid else {}
    for name, meta in decl_fields.items():
        cls = meta.get("class") if isinstance(meta, dict) else None
        classes.setdefault(str(cls), []).append(name)
    report["ladder"] = [{"node": k, "cells": t} for k, t in ladder]
    report["field_classes"] = {c: sorted(n) for c, n in classes.items()}
    if not ladder:
        # The shared prologue runs before either arm, so the sentence must not
        # be /1-flavoured: a /2 list recorded with its leaf entry and no ladder
        # (a truncated or hand-edited block — exactly the class
        # ``_declaration_grammar`` exists to name) reaches here too, and used
        # to exit as a bare "nothing declared" (review finding).
        detail = "no pyramid overview declaration in the manifest (declared: False)"
        if report["pyramid_spec"] == "zagg-pyramid/2":
            detail += (
                f": the zagg-pyramid/2 block records {len(pyramid.get('overviews') or [])} "
                f"level entry(ies), none of them above the shard order — the /2 list is "
                f"always the leaf entry at node {int(manifest['shard_order'])} PLUS the "
                f"fixed every-order ladder down to 0 (§4.4/§4.5), so a list without one "
                f"is truncated, not a shallower pyramid"
            )
        checks["declaration"] = _entry("fail", detail)
        skip_rest("no declaration", after="declaration")
        return _finish(report, CHECKS)
    if not fields:
        checks["declaration"] = _entry(
            "fail",
            f"declared ladder carries no composable fields (classes: "
            f"{report['field_classes']}) — a v1-era declaration; redeclare per issue #547",
        )
        skip_rest("no composable fields", after="declaration")
        return _finish(report, CHECKS)
    if report["pyramid_spec"] == "zagg-pyramid/2":
        # The /2 arm (the espg ruling of 2026-09-11 on issue #547: the
        # re-declaration IS /2, and this harness is its acceptance gate). The
        # staged sweep's fold model differs from the /1 cascade — column
        # gathers and node-order-partial merges, ``regime``/``merges_from_raw``
        # /``source_children`` provenance — so the /2 spine owns its own
        # declaration grammar, source tiers, and the §4.6 leaf-column tier.
        from zagg.pyramid_check_v2 import validate_v2

        return validate_v2(
            store_root,
            manifest,
            report,
            ladder=ladder,
            fields=fields,
            store_kwargs=store_kwargs,
            sample_nodes=sample_nodes,
            sample_cells=sample_cells,
            seed=seed,
            full=full,
            resweep=resweep,
            roster=roster,
        )
    detail = (
        f"{report['pyramid_spec']} ladder {[k for k, _ in ladder]}; composable fields "
        f"{sorted(fields)} (classes {report['field_classes']})"
    )
    checks["declaration"] = _entry("pass", detail)

    # -- roster: what the declared-node enumeration derives from.
    leaves, roster_source = _leaf_roster(store_root, manifest, store_kwargs, roster)
    report["roster"] = {"source": roster_source, "leaves": len(leaves)}
    if not leaves:
        checks["materialization"] = _entry("fail", f"empty leaf roster (source {roster_source})")
        skip_rest("empty roster", after="materialization")
        return _finish(report, CHECKS)

    # -- [2] materialization: declared node roster vs stored overview objects.
    # "Materialized" means COMMITTED, in the sweep's own sense: ``role:
    # overview`` AND the D4 commit stamp (both in the one probed zarr.json). A
    # bare/attr-less node object or a role-without-stamp one is a partial
    # write (the aborted 2026-08-25 sweep's debris, issue #547 forensics):
    # unmaterialized, reported separately.
    probes, declared, state = _ladder_materialization(
        store_root, ladder, leaves, store_kwargs, checks, report
    )
    if state == "errors":
        skip_rest("node probes failed", after="materialization")
        return _finish(report, CHECKS)
    if state == "baseline":
        skip_rest("no materialized nodes (pre-sweep baseline)", after="materialization")
        return _finish(report, CHECKS)

    # -- [3..6] value checks on sampled (or, in full mode, all) nodes.
    rng = np.random.default_rng(seed)
    harness = _Harness(
        store_root,
        manifest,
        store_kwargs,
        rng=rng,
        sample_nodes=sample_nodes,
        sample_cells=sample_cells,
    )
    _value_checks(harness, ladder, declared, probes, leaves, checks, report, full=full)

    # -- [7] idempotency (fixture mode): an immediate re-sweep is a no-op.
    if resweep:
        checks["idempotency"] = _resweep_check(store_root, manifest, leaves, store_kwargs)
    return _finish(report, CHECKS)


def _value_checks(harness, ladder, declared, probes, leaves, checks, report, *, full):
    """Read-back + counts + digests + composition over the sampled nodes."""
    errors: dict = {"readback": [], "counts": [], "digests": [], "composition": []}
    counted = {"readback": 0, "counts": 0, "digests": 0, "composition": 0}
    count_meta, exact_fields, digest_fields, wide_fields, packed_fields = _field_groups(
        harness, report
    )

    for i, (k, t) in enumerate(ladder):
        nodes = [n for n in declared[k] if probes[k].get(n) is not None]
        if not full and len(nodes) > harness.sample_nodes:
            picks = harness.rng.choice(len(nodes), harness.sample_nodes, replace=False)
            nodes = sorted(nodes[int(p)] for p in picks)
        for node in nodes:
            attrs = probes[k][node]
            source, compose_exact, note = _source_for(
                harness, ladder, i, declared, probes, leaves, attrs
            )
            if note:
                harness.warn(f"{node}: {note}")
            if source is None:
                continue
            _check_node(
                harness,
                node,
                k,
                t,
                source,
                attrs,
                count_meta,
                exact_fields,
                digest_fields,
                packed_fields,
                errors,
                counted,
                full=full,
                compose_exact=compose_exact,
            )

    if full and count_meta is not None:
        materialized = {
            k: [n for n in declared[k] if probes[k].get(n) is not None] for k, _ in ladder
        }
        _ladder_totals(harness, ladder, materialized, leaves, count_meta, errors, counted)

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


def _source_for(harness, ladder, i, declared, probes, leaves, attrs):
    """One node's fold-source tier, from its OWN recorded provenance.

    Returns ``(source, exact_composition, note)``. The sweep records which
    regime made each level (``zagg_overview.fold_source`` /
    ``fold_from_order``, :func:`zagg.sweep_overview._fold_provenance`), and
    three reachable regimes are NOT "the next finer ladder level":
    ``fold_source: "leaves"`` (every level folds from the raw leaves),
    ``exact_levels > 1`` (the finest N do), and the gap-wider-than-the-slab
    fallback. Assuming the cascade would make the packed composition compare —
    exact, and re-quantized once per fold — a GUARANTEED false fail under any
    of them, so the tier is read, never inferred (review finding). With no
    usable provenance the next-finer heuristic still holds for counts and
    digests (an associative law and a tolerance-based one) but the exact
    composition compare is declined and said so. The finest level needs no
    provenance: it has no finer overview to cascade from at all.
    """
    from zagg.sweep_overview import OVERVIEW_ATTR

    leaf_tier = (harness.shard_order, harness.cell_order, leaves, harness.leaf_group)
    provenance = (attrs or {}).get(OVERVIEW_ATTR)
    provenance = provenance if isinstance(provenance, dict) else {}
    recorded = provenance.get("fold_source")
    if recorded == "leaves":
        return leaf_tier, True, None
    if recorded == "cascade":
        k_src = provenance.get("fold_from_order")
        tier = _node_tier(harness, ladder, declared, probes, k_src)
        if tier is None:
            return None, False, f"cascade from order {k_src!r}, which is not a declared level"
        return tier, True, None
    if i == 0:
        return leaf_tier, True, None
    k_src, t_src = ladder[i - 1]
    materialized = [n for n in declared[k_src] if probes[k_src].get(n) is not None]
    return (
        (k_src, t_src, materialized, lambda node: harness.node_group(node, t_src)),
        False,
        f"no recorded fold provenance ({recorded!r}); counts/digests checked against "
        f"order {k_src}, composition NOT validated",
    )


def _node_tier(harness, ladder, declared, probes, k_src):
    """The materialized-node tier at ladder order ``k_src``, or None."""
    for k, t in ladder:
        if k == k_src:
            materialized = [n for n in declared[k] if probes[k].get(n) is not None]
            return (k, t, materialized, lambda node, t=t: harness.node_group(node, t))
    return None


def _ladder_totals(harness, ladder, materialized, leaves, count_meta, errors, counted):
    """Full mode: whole-store count totals conserve at every ladder level.

    Walks the MATERIALIZED nodes only — a missing node already failed the
    materialization check, and its absence shows here as a short total.
    """
    fill = count_meta.get("fill_value", 0)

    def total(opener, names):
        out, complete = 0, True
        for name in names:
            try:
                values = np.asarray(opener(name)["count"][:])
            except Exception as exc:
                # A stale root MOC can name a leaf that is gone (D9 cache):
                # skipped and named, never a traceback out of a read-only run.
                harness.warn(f"ladder totals: {name} unreadable ({exc}) — totals not comparable")
                complete = False
                continue
            out += int(values[~_missing_mask(values, fill)].sum())
        return out, complete

    base, complete = total(harness.leaf_group, leaves)
    if not complete:
        return
    for k, t in ladder:
        level, complete = total(lambda n, t=t: harness.node_group(n, t), materialized[k])
        if not complete:
            continue
        counted["counts"] += 1
        if level != base:
            errors["counts"].append(f"ladder o{k}: total {level} != base {base}")


def _resweep_check(store_root, manifest, leaves, store_kwargs) -> dict:
    """Fixture-mode skip gate (issues #417/#421): re-sweep must be a no-op."""
    from zagg.sweep_overview import sweep_overviews

    if str(store_root).startswith("s3://"):
        return _entry("fail", "resweep refused for s3:// roots — production sweeps are fleet-side")
    counts = sweep_overviews(
        store_root, manifest, {d: {None} for d in leaves}, store_kwargs=store_kwargs
    )
    if counts.get("written") or counts.get("failed"):
        return _entry("fail", f"re-sweep was not a no-op: {counts}")
    return _entry("pass", f"immediate re-sweep is a no-op ({counts.get('current', 0)} current)")


def format_report(report: dict) -> str:
    """The printed checklist, mirroring issue #434's phases (either arm)."""
    lines = [f"overview-pyramid E2E validation — {report['store']}"]
    if report.get("spec"):
        ladder = ", ".join(f"o{e['node']}→cells {e['cells']}" for e in report.get("ladder", []))
        lines.append(
            f"  store {report['spec']} | pyramid {report.get('pyramid_spec')} | {ladder or 'no ladder'}"
        )
    if report.get("roster"):
        roster = report["roster"]
        lines.append(f"  leaf roster: {roster['leaves']} shards (source: {roster['source']})")
    for name in CHECKS_V2:  # the superset, in print order; /1 reports skip "columns"
        entry = report["checks"].get(name)
        if entry is not None:
            lines.append(f"  [{entry['status'].upper():4}] {name:15} {entry['detail']}")
    warnings = report.get("warnings") or []
    if warnings:
        lines.append(f"  {len(warnings)} check(s) declined — NOT validated:")
        lines.extend(f"    - {w}" for w in warnings[:10])
        if len(warnings) > 10:
            lines.append(f"    - ... {len(warnings) - 10} more")
    lines.append(f"VERDICT: {'PASS' if report['passed'] else 'FAIL'}")
    return "\n".join(lines)


def main(argv=None) -> int:
    """CLI: ``python -m zagg.pyramid_check <store_root> [--anon] ...``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Issue #434 overview-pyramid E2E validation: declaration vs "
        "materialized nodes, count conservation, digest fold-correctness, packed "
        "composition — read-only, sampled. Validates both pyramid grammars "
        "(zagg-pyramid/1 cascade, zagg-pyramid/2 staged + leaf columns)."
    )
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument(
        "--anon", action="store_true", help="Anonymous S3 reads (public stores, no credentials)"
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--sample-nodes", type=int, default=3, help="Nodes sampled per declared order (default: 3)"
    )
    parser.add_argument(
        "--sample-cells",
        type=int,
        default=8,
        help="Populated cells sampled per node (default: 8)",
    )
    parser.add_argument("--seed", type=int, default=0, help="Sampling seed (default: 0)")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Check every node and every populated cell + ladder totals "
        "(LOCAL fixture-scale stores only; refused for s3:// roots)",
    )
    parser.add_argument(
        "--roster",
        choices=("auto", "moc", "list"),
        default="auto",
        help="Leaf roster source: root coverage.moc, a flat store list, or auto (default)",
    )
    parser.add_argument("--json", default=None, metavar="PATH", help="Also write the report JSON")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    store_kwargs: dict = {}
    if args.store_root.startswith("s3://"):
        if args.full:
            # `--full` sits right beside `--anon`, which only a production
            # invocation passes: refuse the pairing here rather than let the
            # library ValueError out as a traceback (review finding).
            parser.error(
                "--full is refused for s3:// roots: it reads every leaf's whole count "
                "array and every populated cell of every node. Use the sampled default "
                "(--sample-nodes/--sample-cells raise the bound deliberately)."
            )
        store_kwargs["region"] = args.region
        if args.anon:
            store_kwargs["skip_signature"] = True
    report = validate_pyramid(
        args.store_root,
        store_kwargs=store_kwargs,
        sample_nodes=args.sample_nodes,
        sample_cells=args.sample_cells,
        seed=args.seed,
        full=args.full,
        roster=args.roster,
    )
    print(format_report(report))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=1)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
