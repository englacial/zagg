"""The all-time LEAF-COLUMN leg of the windowed harness (issues #649, #650).

A windowed ``/2`` store's leaf-column tier has an all-time member of its
own — ``all.pyramid.zarr`` beside each shard's window columns, written by
the leaf worker (:mod:`zagg.column_alltime`; spec §4.6 "The all-time
column") — which the ladder's node arm cannot see: ``_ladder`` drops the
leaf entry, whose node IS the shard order. :mod:`zagg.pyramid_check_windowed`
probes the member per roster leaf (one ``zarr.json`` GET each) and hands
the committed ones here, where each is held to what the sweep's ``all.zarr``
is held to one tier up:

- its ``zagg_column`` attrs under the §4.6 carve-out
  (:func:`zagg.pyramid_check_v2._column_attrs_errors` with ``all_time``):
  the full column resolution set, every group ``leaf-column`` at the window
  members' derived depth **plus one**, the ``source_windows`` counters;
- its ``source_windows`` against the window columns committed beside it
  NOW, and its stamp against theirs — a member that folded fewer windows
  than the shard holds, or that is stamped before a column it folds, is
  **stale**: it fails by name and its values are not compared (every cell
  would differ, and the finding is the staleness; the next bulk unit or
  all-current re-run rewrites it);
- every sampled cell of every group equal to the k-way fold across the
  window columns' same-resolution members under each field's law
  (:func:`zagg.pyramid_check_core._check_node`, the value laws every other
  level is held to), from the window columns' own bytes — nothing here
  calls the fold it checks, and the window columns were themselves held to
  the leaves by the ``columns`` leg.

Sampled like the node arm (``sample_nodes`` leaves, every one in full
mode). The base (the store's cell order) is exempt: an all-time leaf IS the
unwindowed store.
"""

from __future__ import annotations

import logging

from zagg.pyramid_check_core import _check_node, _column_object_rel, _committed, _rank
from zagg.pyramid_check_windowed import _WindowSources

logger = logging.getLogger(__name__)


def probe_members(harness, leaves, report) -> tuple[dict, list[str], str | None]:
    """One ``zarr.json`` GET per roster leaf: ``(committed members, findings, UNKNOWN detail)``.

    A probe error is UNKNOWN, never a verdict (the node probe's rule): the
    detail comes back for the caller to fail the leg with, and nothing else
    is said. Otherwise ``report["all_time_columns"]`` records the tier
    (``declared``, ``materialized``, ``missing``) and the by-leaf finding
    names every leaf without a committed member — gated on
    :func:`zagg.pyramid_check_core._committed` under the column role, the
    writers' own gate, so a torn write reads as absent.
    """
    from zagg.column import COLUMN_ROLE
    from zagg.pyramid_check_core import _probe_nodes

    probed, errored = _probe_nodes(
        harness.store_root,
        leaves,
        harness.store_kwargs,
        rel=_column_object_rel,
        workers=harness.workers,
    )
    if errored:
        return (
            {},
            [],
            f"{len(errored)} all.pyramid.zarr probe error(s) — the all-time state of the "
            f"leaf-column tier is UNKNOWN, not a verdict: "
            f"{[f'{n}: {e}' for n, e in sorted(errored.items())][:3]}",
        )
    levels, base = report["leaf_levels"], harness.cell_order
    missing = [n for n in leaves if not _committed(probed.get(n), COLUMN_ROLE)]
    report["all_time_columns"] = {
        "declared": len(leaves),
        "materialized": len(leaves) - len(missing),
        "missing": missing[:50],
    }
    findings = (
        [
            f"{len(missing)}/{len(leaves)} leaf/leaves carry no committed all.pyramid.zarr: "
            f"{missing[:8]} — the declared leaf-column levels {levels} have NO all-time "
            f"member, so the store's default view is missing them (issue #650; the base, cells "
            f"{base}, is exempt — an all-time leaf IS the unwindowed store)"
        ]
        if missing
        else []
    )
    return {n: probed[n] for n in leaves if n not in missing}, findings, None


def all_time_columns_check(
    harness, leaves, members, col_probes, entries, groups, report, errors, counted, *, full
) -> list[str]:
    """Hold every committed all-time column to §4.6; returns the by-name findings.

    ``leaves`` are the roster's shard decimals, ``members`` ``{decimal:
    attrs}`` of the committed ``all.pyramid.zarr`` probes, ``col_probes``
    ``{window: {decimal: committed attrs | None}}`` from the ``columns`` leg
    (the window columns as they stand), ``entries`` the manifest's ``/2``
    ladder. Value mismatches accumulate into ``errors``/``counted`` beside
    the node arm's; the returned list carries the attrs, staleness and
    coverage findings, and ``report["all_time_columns"]`` gains
    ``checked`` and ``stale``.
    """
    from zagg.column import COLUMN_ATTR, COLUMN_ROLE, column_resolutions, raw_fold_boundary
    from zagg.pyramid_check_v2 import _as_int, _column_attrs_errors

    s = harness.shard_order
    resolutions = column_resolutions(entries, s)
    boundary = raw_fold_boundary(s, harness.cell_order, resolutions)
    count_meta, exact_fields, digest_fields, _wide, packed_fields = groups
    sources = _ColumnWindowSources(
        harness.store_root,
        harness.manifest,
        harness.store_kwargs,
        rng=harness.rng,
        sample_nodes=harness.sample_nodes,
        sample_cells=harness.sample_cells,
        workers=harness.workers,
    )
    picked = [dec for dec in leaves if dec in members]
    if not full and len(picked) > harness.sample_nodes:
        picks = harness.rng.choice(len(picked), harness.sample_nodes, replace=False)
        picked = sorted(picked[int(p)] for p in picks)
    findings: list[str] = []
    stale: list[str] = []
    for dec in picked:
        attrs = members[dec]
        findings.extend(_column_attrs_errors(dec, s, attrs, resolutions, boundary, all_time=True))
        # The windows the shard is known to have: every window whose roster
        # names this leaf; held = those with a committed column now.
        known = sorted(w for w in col_probes if dec in col_probes[w])
        held = {w: col_probes[w][dec] for w in known if _committed(col_probes[w][dec], COLUMN_ROLE)}
        block = attrs.get(COLUMN_ATTR)
        counters = (block.get("source_windows") or {}) if isinstance(block, dict) else {}
        folded, missing, unreadable = (
            _as_int(counters.get(c)) for c in ("folded", "missing", "unreadable")
        )
        is_stale = False
        if None in (folded, missing, unreadable):
            pass  # the attrs leg named the malformed counters
        elif folded > len(held) or len(held) - folded > unreadable:
            is_stale = True
            findings.append(
                f"{dec}: STALE all.pyramid.zarr — it folded {folded} window column(s) "
                f"({unreadable} unreadable) and the shard now holds {len(held)} committed "
                f"{sorted(held)}; the next bulk unit or all-current re-run rewrites it"
            )
        elif missing > len(known) - len(held):
            is_stale = True
            findings.append(
                f"{dec}: STALE all.pyramid.zarr — it records {missing} missing window(s) and "
                f"only {len(known) - len(held)} of the shard's {len(known)} lack a column now"
            )
        stamped = str((attrs.get("morton_hive_commit") or {}).get("written_at") or "")
        newer = sorted(
            w
            for w, a in held.items()
            if str((a.get("morton_hive_commit") or {}).get("written_at") or "") > stamped
        )
        if newer and not is_stale:
            is_stale = True
            findings.append(
                f"{dec}: STALE all.pyramid.zarr — window column(s) {newer} were stamped after "
                f"it ({stamped or 'no stamp'}); re-run the shard"
            )
        if missing or unreadable:
            harness.warn(
                f"{dec}: all.pyramid.zarr under-covers ({counters}) — "
                + (
                    "its values are not compared against the windows it could not read"
                    if (unreadable or 0) and len(held) > (folded or 0)
                    else f"compared against the {len(held)} window column(s) it folded"
                )
            )
        if is_stale:
            stale.append(dec)
        compare = not is_stale and len(held) == folded
        for r in resolutions:
            _check_node(
                sources,
                dec,
                s,
                r,
                (dec, r, sorted(held)),
                attrs,
                count_meta,
                exact_fields,
                digest_fields,
                packed_fields,
                errors,
                counted,
                full=full,
                compose_exact=True,
                role=COLUMN_ROLE,
                provenance_attr=COLUMN_ATTR,
                group_getter=sources.member_group,
                values=compare,
            )
    for message in sources.warnings:
        harness.warn(message)
    report["all_time_columns"].update({"checked": len(picked), "stale": stale[:50]})
    return findings


class _ColumnWindowSources(_WindowSources):
    """A harness whose fold source is ONE leaf's per-window COLUMNS (§4.6).

    :class:`zagg.pyramid_check_windowed._WindowSources` over
    ``{window}.pyramid.zarr`` instead of ``{window}.zarr``: the member's group
    at ``r`` is the k-way fold of the window columns' groups at ``r``, cell for
    cell, so a source tier is ``(leaf decimal, resolution, windows)`` and every
    window contributes its own cell ``j`` to output cell ``j``.
    """

    def _cell(self, cell_dec, source):
        node, r, windows = source
        j = _rank(cell_dec[len(node) :])
        for window in windows:
            try:
                yield window, self._open(_column_object_rel(node, window), r), j
            except Exception as exc:
                self.warn(f"{node}[{window}]: window column unreadable ({exc}) — cells skipped")
                yield window, None, j

    def member_group(self, decimal: str, r: int):
        """The all-time column's resolution group — the artifact under test."""
        return self._open(_column_object_rel(decimal, None), r)
