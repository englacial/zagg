"""Leaf-worker pyramid column folds (issue #383; umbrella #381 points (1)-(3)).

A **column artifact** is the leaf worker's own pyramid contribution, computed
at aggregation time while the shard's cell data is resident: one zarr per
``(leaf, window)`` under the leaf's node prefix, holding a resolution group
for every leaf-node level the ``zagg-pyramid/2`` declaration carries, every
member a coarser declaration implies within the leaf's footprint, and the
node-order member (``cells == node`` — the leaf's whole-footprint aggregate,
its **universal partial** for every coarser cell; there is no ``partial/``
grammar, #381 point (2)).

Every group folds directly from the leaf's raw resident cell slabs —
merges-from-raw 1 for all leaf-written content, the #381 point (1) regime
law; a group is never folded from another group. Exact classes reduce via
:func:`zagg.sweep_overview.fold_dense`, approximate (t-digest) classes via
the order-independent k-way merge (:func:`zagg.sweep_overview.fold_digests`,
the issue #370 fold law) — the same kernels the sweep's from-leaves fold
runs over the same per-cell inputs in the same ascending order, so column
bytes are parity-equal with that fold by construction.

This module owns the fold core (pure functions over in-memory slabs), the
column writer (one artifact per ``(leaf, window)``, D4 write discipline: a
wholesale template, every resolution group, the role/provenance attrs, ONE
commit stamp last covering the whole column, and the D20 stats sidecar after
the stamp), and the worker seam (:func:`write_leaf_column` — gate on the
declaration, fold from the #342 staged sink, write), which
``hive.process_and_write_hive`` calls after the leaf's own commit.
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)

#: Envelope version of the column artifact's provenance attrs payload.
COLUMN_SPEC = "zagg-column/1"
#: Column basename suffix: the D23 window stem carries it in place of
#: ``.zarr`` (:func:`column_name`). The ONE definition of the name seam —
#: store walkers that must tell a column from a source leaf key off it
#: (:func:`zagg.coverage.refresh_root_coverage`), never off a second literal.
COLUMN_SUFFIX = ".pyramid.zarr"
#: Root-group attrs key carrying the column provenance payload (D11).
COLUMN_ATTR = "zagg_column"
#: ``role`` attrs value classifying a column zarr (D11: classification by
#: attrs, never tree position; source leaves carry no role — absence means
#: source — and the sweep's overview family keeps its own ``overview`` role).
COLUMN_ROLE = "column"
#: The #381 point (7) regime every leaf-written group records: folded from
#: the leaf's own resident cells. No ``source_children`` rides this regime
#: (PR #379 precedent: coverage counts ride ``cascade``) — a leaf column's
#: source is complete by construction, and its merges-from-raw is 1.
LEAF_REGIME = "leaf-column"


def generation_key(block) -> tuple:
    """The staged sweep's skip-gate key over a summed ``generation`` block.

    ``(n_leaves, max_leaf_timestamp, run ids, granule_count)`` — the block
    the stage worker records in these attrs and in the ladder entries it
    writes (§4.4/§4.6). The last two terms exist because stamps resolve to
    **one second**, so the count/timestamp pair alone reads a same-second
    rewrite of a child at an unchanged leaf count as *current* and serves
    stale content:

    - ``run_ids`` (issue #417) — every STAGE stamp carries its ``run_id``
      (PR #416 phase 2), so a foreign rewrite moves the id set, and the
      single-writer law forbids a run rewriting its own object mid-run;
    - ``granule_count`` (issue #433) — fleet-written leaf columns carry no
      run id (§4.6: absence means not-a-stage-artifact), so at the finest
      dispatch tuple the id term is empty and the run-id half cannot see a
      same-second leaf rewrite at all. Every stamp, fleet or stage, records
      the granules it folded, so the common case — a leaf re-run over more
      granules — moves this term instead.

    A block missing either term keys on that term's zero (empty tuple / 0),
    never on a wildcard: an upgraded store re-folds once rather than
    inheriting the blind spot. A non-block keys on ``()``, which matches no
    generation.

    ``run_ids`` is compared as a SET: the recorded list is read back off an
    artifact this process did not write, so its order is not a property to
    assume (review finding — an unsorted or duplicated list would otherwise
    re-fold a whole ladder for nothing).
    """
    if not isinstance(block, dict):
        return ()
    return (
        int(block.get("n_leaves") or 0),
        block.get("max_leaf_timestamp"),
        tuple(sorted(set(block.get("run_ids") or ()))),
        int(block.get("granule_count") or 0),
    )


def stamped_generation_key(block, stamp) -> tuple:
    """One child's contribution to its parent's skip key (issues #417/#433).

    :func:`generation_key` over the child's recorded ``generation`` block —
    or, for a leaf column (which records none), the leaf identity: one leaf
    at its stamp's timestamp — **unioned with the run id that stamped THIS
    child, and carrying that stamp's own granule count**. Both come off the
    child's stamp rather than its relayed block: the run that wrote the
    child and the granules it folded are what a same-second foreign rewrite
    changes, and reading the relayed block alone would see only its own
    children's ids (empty all the way down a fleet-built store, so the gate
    would fall back to the count/timestamp pair it is meant to strengthen).
    Fleet-written stamps carry no ``run_id`` and contribute none; they do
    carry ``granule_count``, which is the leaf arm's whole point (issue
    #433).

    The two are composed differently ON PURPOSE (review finding): the run id
    is UNIONED with the block's, the granule count REPLACES it. A stage
    column's stamp count and its recorded block's are the same sum over the
    same children (:func:`zagg.sweep_stage._summed_generation` and the relay
    column's stamp both reduce that run's readers), so replacing loses
    nothing and upgrades a pre-#433 block — which carries no count — off the
    stamp. Unioning or ADDING them would double-count every granule at every
    level of the ladder.
    """
    stamp = stamp if isinstance(stamp, dict) else {}
    if not isinstance(block, dict):
        block = {"n_leaves": 1, "max_leaf_timestamp": stamp.get("written_at")}
    runs = set(block.get("run_ids") or ())
    if stamp.get("run_id"):
        runs.add(stamp["run_id"])
    return generation_key(
        {**block, "run_ids": sorted(runs), "granule_count": stamp.get("granule_count")}
    )


def column_resolutions(levels: list, node_order: int) -> list[int]:
    """The resolutions a leaf-node column carries, finest first (issue #383).

    ``levels`` is the NORMALIZED ``zagg-pyramid/2`` grouped form — the
    manifest block's ``overviews`` list (:func:`zagg.pyramid.normalize_overviews`,
    the ``output.pyramid.overviews`` knob). The column holds every declared
    resolution that is complete within one leaf footprint (``cells >=
    node_order`` — the leaf-node groups plus the members coarser declarations
    imply, which makes their levels pure gathers, #381 point (3)) plus the
    node-order member unconditionally. Resolutions coarser than the node need
    no member of their own: the leaf's contribution to ANY coarser cell is
    its whole-footprint aggregate — the node-order member itself.

    Empty when ``levels`` declares no ``node == node_order`` entry — no
    leaf-written column, and the sweep owns whatever it materializes. Under
    the collapsed ``/2`` grammar that cannot happen: ``expand_overviews``
    emits the leaf entry unconditionally, first, so an expanded list always
    carries one. The guard is a robustness backstop for manifest-shaped
    ``levels`` built some other way (a hand-edited block, or a future
    declaration whose schedule starts coarser than the shard node) — this is
    a public function and ``expand_overviews`` is not its only conceivable
    caller.
    """
    node_order = int(node_order)
    if not any(int(e["node"]) == node_order for e in levels or []):
        return []
    within = {int(c) for e in levels for c in e["cells"] if int(c) >= node_order}
    return sorted(within | {node_order}, reverse=True)


def composable_fields(fields: dict) -> dict:
    """The declared fields a column fold may carry: the two composable classes.

    The D24 ``class: "none"`` entries — expressions, vector fields,
    chunk-resolution companions, temporal companions, and the derived
    statistics (:func:`zagg.semantics.field_composability`, recorded by
    :func:`zagg.pyramid.declared_fields`) — exist at native resolution ONLY,
    and no coarser fold of them is defined. A **located** ragged field is not
    among them: since ruling 4 on issue #410 it is ``approximate`` and folds
    through the pyramid with its ``{field}_locations`` channel, so it passes
    this filter and :func:`fold_column` carries the pair. The fold core filters them here,
    the same posture the sweep takes before ``_fold_node``
    (:func:`zagg.sweep_overview.sweep_overviews`), so handing a whole
    declaration's ``fields`` map straight through can neither refuse a leaf
    over a non-cell-extent vector slab nor materialize an all-empty ragged
    group for a companion the column has no business carrying.
    """
    return {
        n: m
        for n, m in (fields or {}).items()
        if isinstance(m, dict) and m.get("class") in ("exact", "approximate")
    }


def leaf_slabs(staged: dict, fields: dict, *, group_path: str, n_cells: int) -> dict:
    """``{field: cell slab}`` fold inputs from the leaf writer's staged sink.

    ``staged`` is the issue #342 staged-array record
    (``{f"{group_path}/{name}": slab}`` — the exact in-memory values the leaf
    write PUT), so the fold consumes what the leaf stores, byte-for-byte,
    with no read-back. A declared field absent from the sink contributes
    fill — the leaf writers skip an all-empty ragged array entirely
    (``write_ragged_leaf_to_zarr``), and its stored cells are the ``b""``
    fill regardless — so the synthesized slab is exactly what a read-back
    would return — synthesized by :func:`zagg.sweep_overview._empty_slab`, the
    same seed the sweep's own fold starts from, so the fill semantics the two
    machineries must agree on stay one definition.

    ``fields`` is filtered to the composable classes first
    (:func:`composable_fields`), which is what makes the ``(n_cells,)`` extent
    check sound: those two classes admit nothing but cell-resolution scalars
    and ragged payloads, both of which are one row per cell, so a staged slab
    of any other extent really is a sink that disagrees with the grid — and
    folding it would write a wrong column, so it raises.

    A field's companion siblings — ``{field}_locations`` (§9) and
    ``{field}_times`` (§8.3) — are picked up under the same rule and the same
    extent check (issue #410): each is one more cell-extent slab in the same
    sink, and :func:`fold_column` needs them in one place because the merge
    produces all of them in one call (spec §9.1/§8.3).
    """
    from zagg.sweep_overview import _empty_slab, field_companions

    def _slab(key: str, meta: dict):
        slab = staged.get(f"{group_path}/{key}")
        if slab is None:
            return _empty_slab(meta, n_cells)
        slab = np.asarray(slab)
        if slab.shape != (int(n_cells),):
            raise ValueError(
                f"staged slab for field {key!r} has shape {slab.shape}, not the "
                f"leaf's ({int(n_cells)},) cell extent — refusing to fold a column "
                f"from a sink that disagrees with the grid"
            )
        return slab

    slabs: dict = {}
    for name, meta in composable_fields(fields).items():
        slabs[name] = _slab(name, meta)
        for _kwarg, sibling in field_companions(name, meta):
            slabs[sibling] = _slab(sibling, meta)
    return slabs


def fold_column(slabs: dict, fields: dict, *, cell_order: int, resolutions: list) -> dict:
    """Fold the leaf's resident cell slabs into ``{resolution: {field: slab}}``.

    Each resolution group folds INDEPENDENTLY from the raw cell slabs (never
    group from group — merges-from-raw stays 1 for every group, #381 point
    (1)): ``4^(cell_order - resolution)`` consecutive leaf cells share one
    target cell (the ascending packed-word leaf invariant). Exact fields
    reduce under their declared merge law; approximate fields decode each
    child cell's payload and k-way merge the non-empty digests per target
    cell, in ascending cell order — input-identical to the sweep's
    from-leaves fold (:func:`zagg.sweep_overview._fold_node` +
    :func:`zagg.sweep_overview.fold_digests`) of the committed leaf, which is
    the issue #383 byte-parity contract. The node-order resolution is the
    degenerate 1-cell group: the leaf's whole-footprint aggregate.

    ``fields`` is filtered to the composable classes (:func:`composable_fields`)
    — a D24 ``none`` field has no coarser fold and never becomes a group. A
    resolution FINER than ``cell_order`` is refused by name: it would ask for
    a fractional fold factor, which no guard downstream can read as a divisor
    (both classes would surface it as an opaque numpy failure instead).

    A **located** field folds its ``{field}_locations`` sibling in the SAME
    k-way call as its payload and returns it as its own group member (ruling 4
    on issue #410, review finding): the §4.6 template
    (:func:`zagg.sweep_overview._overview_config`) emits the sibling array and
    the payload's §1.2 binding for every located field, so a fold returning
    payload slabs only would commit populated payload rows against ``b""``
    sibling rows under a §9 declaration — §1.1's row-alignment MUST broken, and
    hashed into the §5 sidecar as content. The sibling slab is required by
    name rather than defaulted: the words are keyed on the centroid partition
    the merge produces (spec §9.1), so the pair may never be folded apart.
    """
    from zagg.sweep_overview import (
        decode_digest,
        field_companions,
        fold_dense,
        fold_digests,
        overview_fold_delta,
    )

    cell_order = int(cell_order)
    fields = composable_fields(fields)
    out: dict = {}
    for res in resolutions:
        res = int(res)
        if res > cell_order:
            raise ValueError(
                f"cannot fold a column group at resolution {res}: it is FINER than the "
                f"leaf's cell order {cell_order}, so the fold factor "
                f"4^({cell_order} - {res}) is fractional — a column group is a fold of "
                f"the leaf's own cells, never an upsample of them"
            )
        factor = 4 ** (cell_order - res)
        groups: dict = {}
        for name, meta in fields.items():
            slab = slabs[name]
            if meta["class"] == "exact":
                groups[name] = fold_dense(
                    slab, factor, meta.get("method"), meta.get("fill_value", "NaN")
                )
                continue
            dtype = meta.get("dtype") or "float32"
            inner = tuple(meta.get("inner_shape") or (2,))
            delta = overview_fold_delta(meta)
            if slab.shape[0] % factor:
                raise ValueError(f"cannot fold {slab.shape[0]} cells {factor}-to-one for {name!r}")
            declared = field_companions(name, meta)
            for kwarg, sibling in declared:
                if slabs.get(sibling) is None:
                    raise ValueError(
                        f"field {name!r} declares a {kwarg} channel but no {sibling!r} slab "
                        f"was supplied — the words are keyed on the centroid partition the "
                        f"merge produces (spec §9.1/§8.3), so the pair cannot be folded apart"
                    )
            folded = np.full(slab.shape[0] // factor, b"", dtype=object)
            sibling_slabs = {
                kwarg: np.full(folded.shape[0], b"", dtype=object) for kwarg, _ in declared
            }
            for j in range(folded.shape[0]):
                rows = [
                    i
                    for i in range(j * factor, (j + 1) * factor)
                    if slab[i] is not None and len(slab[i])
                ]
                if not rows:
                    continue
                cell = [decode_digest(slab[i], dtype, inner) for i in rows]
                if not declared:
                    folded[j] = fold_digests(cell, delta=delta, dtype=dtype)
                    continue
                payload, *words = fold_digests(
                    cell,
                    delta=delta,
                    dtype=dtype,
                    channels={
                        kwarg: [decode_digest(slabs[sibling][i], "uint64", ()) for i in rows]
                        for kwarg, sibling in declared
                    },
                )
                folded[j] = payload
                for (kwarg, _), encoded in zip(declared, words, strict=True):
                    sibling_slabs[kwarg][j] = encoded
            groups[name] = folded
            for kwarg, sibling in declared:
                groups[sibling] = sibling_slabs[kwarg]
        out[res] = groups
    return out


def _column_provenance(meta: dict) -> dict:
    """One field's column-attrs entry: the sweep's, plus the digest budget.

    :func:`zagg.sweep_overview._field_provenance` records class + fold law
    (+ ``nan_policy`` for exact). An approximate group's stored centroids are
    also decided by the three values :func:`fold_column` reads — ``delta``,
    ``dtype``, ``inner_shape`` — and a reader gathering columns k-way (#370)
    needs them: they cannot be recovered from the leaf, since a column can
    outlive a declaration change. Recorded here rather than in the sweep's
    shared helper — the overview's identical gap is a spec call for the
    issue #383 phase 4 section, not a reason to leave this artifact short.
    """
    from zagg.sweep_overview import _field_provenance, overview_fold_delta

    entry = dict(_field_provenance(meta))
    if meta.get("class") == "approximate":
        entry["delta"] = int(meta.get("delta") or 512)
        # The budget the column fold actually compressed at (issue #424):
        # the split overview_delta, not the leaf δ.
        entry["overview_delta"] = overview_fold_delta(meta)
        entry["dtype"] = meta.get("dtype") or "float32"
        entry["inner_shape"] = list(meta.get("inner_shape") or (2,))
    return entry


def column_name(window: str | None) -> str:
    """The column basename: the D23 window stem + ``.pyramid.zarr``.

    ``{window}.pyramid.zarr``, with ``all.pyramid.zarr`` for the unwindowed /
    schedule-none leaf — the same window-only stem the overview writer uses
    unconditionally (:func:`zagg.windows.leaf_name_v3`), plus a ``.pyramid``
    stem marker so the name is disjoint from every leaf and overview basename
    (both end at ``{window}.zarr``). Proposed on the issue #383 PR, flagged
    there as a naming question.

    :data:`~zagg.windows.SCHEDULE_NONE_TOKEN` normalizes back to ``None``,
    mirroring :func:`zagg.sweep_overview._overview_basename`: the token is
    what :func:`write_column` records as the unwindowed column's
    ``zagg_column.window``, and ``leaf_name_v3`` RAISES on it (an explicit
    label may never be the reserved token), so the attrs value would
    otherwise be the one input this function cannot take. The alias is
    unambiguous precisely because no legitimate window carries that label.
    """
    from zagg.windows import SCHEDULE_NONE_TOKEN, leaf_name_v3

    stem = leaf_name_v3(None if window == SCHEDULE_NONE_TOKEN else window)
    return stem.removesuffix(".zarr") + COLUMN_SUFFIX


def write_column(
    store_root: str,
    shard_key,
    folded: dict,
    fields: dict,
    *,
    node_order: int,
    cell_order: int,
    window: str | None = None,
    time_range=None,
    granule_count: int = 0,
    store_kwargs: dict | None = None,
) -> str:
    """Write one leaf's column artifact under its node prefix; returns its basename.

    ``folded`` is :func:`fold_column`'s output; ``fields`` the declaration's
    composable map (the template and the provenance attrs are derived from
    it, exactly as the overview writer derives them). The write order is the
    leaf's own D4 discipline: template (wholesale — the prefix is DELETED
    first, the issue #341 semantics, so an idempotent re-run replaces the
    column entirely and a prior torn write never survives) -> every
    resolution group's ``morton`` + ``{order}/{field}`` arrays -> the
    role/provenance attrs -> ONE commit stamp LAST covering the whole
    column. The clear also removes any PRIOR sidecar (fail-open, sibling to
    the prefix it cannot reach): a rewrite whose fresh sidecar PUT then fails
    leaves the record ABSENT — unverifiable, §5.3 — never STALE, which would
    verify as a mismatch and read as a false tamper signal (D20).
    There is no partial-column failure state: an interrupted writer
    leaves an unstamped prefix — ignorable debris, and repair is re-invoking
    the idempotent leaf (never a sweep-side fallback to raw cells). The D20
    stats sidecar is a SIBLING object PUT after the stamp (fail-open,
    telemetry class), so the stamp stays the column's own final write.

    Object cost (the fleet's bill, not the byte cost the design bounded):
    each group contributes one ``zarr.json`` plus ``(1 morton + n_fields)``
    arrays of one ``zarr.json`` + one chunk object each, and the root pays
    three ``zarr.json`` PUTs (template, attrs, stamp) plus the sidecar. At
    the 19/13/9 reference geometry with 2 composable fields — groups
    {13, 12, 11, 10, 9} — that is ``5*(1 + 3*2) + 3 + 1 = 39`` objects per
    ``(leaf, window)``, against the leaf's own ~10. The array writes run
    inside the same ``async.concurrency: 128`` context as the template
    (the leaf writers' posture, issue #209): serial open+PUT round-trips at
    that count are a tail-latency term on every unit.

    The commit stamp's ``cells_with_data`` counts the populated mask of the
    column's FINEST group — the one denominator a leaf or an overview has
    implicitly, but a column (N grids) does not, so the group it counts is
    recorded as ``cells_with_data_order`` in the ``zagg_column`` attrs: the
    number is declaration-dependent (a coarser base declaration yields a
    different count for the same leaf) and a reader cannot infer it.

    A ``folded`` without the node-order member is refused by name: that
    member is the leaf's universal partial for every coarser cell (#381
    point (2)), the one group #384's gather may assume, so stamping a
    column without it would publish a complete-looking artifact that folds
    short. The same guard covers an empty ``folded`` (the
    ``column_resolutions() == []`` gate belongs to the caller).

    Single-writer law (#381 point (2)): the column lives only under its
    leaf's node prefix and has exactly one writer, ever — no locking.
    """
    import zarr
    from mortie import generate_morton_children
    from pydantic_zarr.experimental.v3 import GroupSpec
    from zarr import config as zarr_config
    from zarr import open_array
    from zarr.core.sync import sync

    from zagg.grids.base import vlen_dtype_warning_suppressed
    from zagg.grids.healpix import HealpixGrid
    from zagg.grids.morton import morton_decimal
    from zagg.hive import _utcnow, shard_leaf_path, stamp_commit
    from zagg.store import open_store
    from zagg.sweep_overview import ROLE_ATTR, _overview_config, _populated_mask
    from zagg.windows import SCHEDULE_NONE_TOKEN

    store_kwargs = dict(store_kwargs or {})
    node_order, cell_order = int(node_order), int(cell_order)
    fields = composable_fields(fields)
    resolutions = sorted((int(r) for r in folded), reverse=True)
    if node_order not in resolutions:
        raise ValueError(
            f"a column must carry the node-order member ({node_order}) — it is the leaf's "
            f"universal partial for every coarser cell (#381 point (2)), and no coarse level "
            f"ever rewrites a leaf; got resolutions {resolutions}"
        )
    leaf_path = shard_leaf_path(store_root, shard_key, window=window)
    node_prefix = leaf_path.rstrip("/").rsplit("/", 1)[0]
    basename = column_name(window)
    path = f"{node_prefix}/{basename}"
    cfg = _overview_config(fields)
    # `sharded=True` is INERT here (as in `_write_overview`): with no
    # `chunk_inner`, `HealpixGrid` computes chunks_per_shard == 1 and turns
    # sharding back off for every group — one chunk per group, no shard index.
    grids = {res: HealpixGrid(node_order, res, config=cfg, sharded=True) for res in resolutions}
    spec = GroupSpec(
        members={str(res): grids[res].shard_spec() for res in resolutions}, attributes={}
    )
    store = open_store(path, **store_kwargs)
    staged: dict = {}
    with zarr_config.set({"async.concurrency": 128}), vlen_dtype_warning_suppressed():
        sync(store.delete_dir(""))
        _delete_sidecar(node_prefix, _sidecar_name(basename), store_kwargs)
        spec.to_zarr(store, "", overwrite=True)
        for res in resolutions:
            words = np.asarray(generate_morton_children(int(shard_key), res), dtype=np.uint64)
            arr = open_array(store, path=f"{res}/morton", zarr_format=3, consolidated=False)
            arr[:] = words
            staged[f"{res}/morton"] = words
            for name, slab in folded[res].items():
                arr = open_array(store, path=f"{res}/{name}", zarr_format=3, consolidated=False)
                arr[:] = slab
                staged[f"{res}/{name}"] = slab
    root = zarr.open_group(store, path="", mode="r+", zarr_format=3)
    root.attrs.update(
        {
            ROLE_ATTR: COLUMN_ROLE,
            COLUMN_ATTR: {
                "spec": COLUMN_SPEC,
                "node": morton_decimal(int(shard_key)),
                "order": node_order,
                "source_cell_order": cell_order,
                "window": window if window is not None else SCHEDULE_NONE_TOKEN,
                "fields": {n: _column_provenance(m) for n, m in fields.items()},
                "groups": {
                    str(res): {
                        "regime": LEAF_REGIME,
                        "merges_from_raw": 1,
                        "n_cells": 4 ** (res - node_order),
                    }
                    for res in resolutions
                },
                "cells_with_data_order": resolutions[0],
                "generated_at": _utcnow(),
            },
        }
    )
    populated = _populated_mask(folded[resolutions[0]], fields)
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(granule_count),
        window=window,
        time_range=time_range if window is not None else None,
    )
    _write_sidecar(
        store, path, shard_key, staged, int(populated.sum()), granule_count, window, store_kwargs
    )
    return basename


def _sidecar_name(basename: str) -> str:
    """The column's D20 sidecar basename: its own stem + ``.stats.json``."""
    return basename.removesuffix(".zarr") + ".stats.json"


def _delete_sidecar(prefix: str, name: str, store_kwargs: dict) -> None:
    """Drop a prior run's column sidecar, fail-open (absent beats stale)."""
    try:
        import obstore

        from zagg.store import open_object_store

        obstore.delete(open_object_store(prefix, **store_kwargs), name)
    except Exception as e:
        logger.debug(f"column stats sidecar clear skipped at {prefix}/{name}: {e}")


def _clear_column(store_root: str, shard_key, window: str | None, store_kwargs: dict) -> None:
    """Delete this ``(leaf, window)``'s column and sidecar (the no-gate arm).

    ``hive.process_and_write_hive`` clears ``{node}/{window}.zarr`` wholesale
    on every write (issue #341), but nothing else owns the column beside it —
    so a declaration that was removed or narrowed between runs would leave a
    STAMPED column folded from cells that are gone. That is a third state
    beyond the §4.6 pair (a column exists exactly when the run declares one;
    absent-or-unstamped is a torn worker), and it is the same stale-beats-
    absent inversion the sidecar clear already rules against.

    NOT fail-open on the prefix: a stale stamped column is wrong data, so a
    delete that cannot be performed fails the unit like any other write
    failure. An absent prefix is not a failure (``delete_dir`` over a missing
    prefix lists empty); the sidecar drop keeps its own fail-open posture.
    Cost: one delete attempt per leaf write on stores that declare no column.
    """
    from zarr.core.sync import sync

    from zagg.hive import shard_leaf_path
    from zagg.store import open_store

    leaf_path = shard_leaf_path(store_root, shard_key, window=window)
    node_prefix = leaf_path.rstrip("/").rsplit("/", 1)[0]
    basename = column_name(window)
    sync(open_store(f"{node_prefix}/{basename}", **store_kwargs).delete_dir(""))
    _delete_sidecar(node_prefix, _sidecar_name(basename), store_kwargs)


def _write_sidecar(
    store, path, shard_key, staged, cells_with_data, granule_count, window, store_kwargs
) -> None:
    """The column's D20 stats sidecar: ``{stem}.stats.json``, after the stamp.

    The overview writer's O11 recipe (issue #342, spec §5): the content
    hashes computed from the staged arrays just written, in a
    :func:`zagg.telemetry.build_record` row keyed by the shard. The name is
    derived from the column's own stem — ``telemetry.sidecar_key``'s label
    grammar (rightly) rejects the dotted ``.pyramid`` stem, and the rule is
    the same ``{stem}.stats.json`` one. Fail-open (D9 telemetry posture):
    §5.3 reads absence as unverifiable, never tampered — so EVERY import a
    sidecar needs sits inside the ``try`` (``_write_overview``'s posture): an
    ImportError here is a telemetry-class failure and must not fail a column
    that is already committed. The caller's open ``store`` is reused rather
    than re-derived — one fewer store construction and root-metadata read.
    """
    try:
        import json

        import obstore
        import zarr

        from zagg.content_hash import content_hashes_record, hash_arrays
        from zagg.store import open_object_store
        from zagg.telemetry import build_record

        group = zarr.open_group(store, path="", mode="r", zarr_format=3)
        record = build_record(
            shard_key=int(shard_key),
            metadata={
                "cells_with_data": int(cells_with_data),
                "granule_count": int(granule_count),
                "content_hashes": content_hashes_record(hash_arrays(group, staged=staged)),
            },
            window=window,
        )
        prefix, _, name = path.rstrip("/").rpartition("/")
        obstore.put(
            open_object_store(prefix, **store_kwargs),
            _sidecar_name(name),
            json.dumps(record).encode(),
        )
    except Exception as e:
        logger.warning(f"column stats sidecar failed (fail-open, issue #383): {e}")


def leaf_column_plan(config, grid) -> tuple[list[int], dict] | None:
    """The leaf's column plan from its own config: ``(resolutions, fields)`` or None.

    The issue #383 gate, decided worker-side from the config both backends
    already carry: a column is written iff the declaration carries leaf-node
    levels — an explicit ``output.pyramid.overviews`` knob (the
    ``zagg-pyramid/2`` grammar; its expansion always places the declared
    resolutions at the shard node), or — the ruled issue #384 default flip —
    a DEFAULT declaration (no ``overviews``, no legacy ``orders``/``spacing``
    spelled), which now means ``/2`` at the grid's resolved chunk order
    whenever that order is strictly interior. This mirrors
    ``build_pyramid_block``'s manifest default EXACTLY: the two gates must
    agree, or a default-flipped store would declare leaf levels no worker
    writes. Explicit ``orders``/``spacing`` schedules stay ``/1`` (no
    column), as does a grid with no strictly-interior chunk order (K == 1).
    The declaration is re-validated against the grid here — cheap, and the
    Lambda worker builds its config without ``validate_config`` — with the
    same refusals the templating path raises. The D24 field map is the
    declaration's own (:func:`zagg.pyramid.declared_fields`) filtered to the
    composable classes; the template-time warning for excluded fields is NOT
    repeated per shard (``build_pyramid_block`` owns the loud warning).
    """
    from zagg.config import get_pyramid
    from zagg.pyramid import (
        declared_fields,
        expand_overviews,
        normalize_overviews,
        validate_overviews,
    )

    knob = get_pyramid(config)
    if knob is None:
        return None
    raw = knob.get("overviews")
    if raw is None:
        if knob.get("orders") is not None or knob.get("spacing") is not None:
            return None  # an explicit legacy /1 schedule declares no columns
        chunk = getattr(grid, "chunk_order", None)
        if not (isinstance(chunk, int) and int(grid.parent_order) < chunk < int(grid.child_order)):
            return None  # no strictly-interior default exists (K == 1)
        raw = chunk  # the ruled /2 default flip (issue #384)
    declared = normalize_overviews(raw)
    validate_overviews(
        declared, parent_order=int(grid.parent_order), child_order=int(grid.child_order)
    )
    levels = expand_overviews(declared, parent_order=int(grid.parent_order))
    resolutions = column_resolutions(levels, grid.parent_order)
    if not resolutions:
        return None
    fields = composable_fields(declared_fields(config)[0])
    if not fields:
        return None
    return resolutions, fields


def write_leaf_column(
    store_root: str,
    shard_key,
    grid,
    config,
    staged: dict,
    *,
    window: str | None = None,
    time_range=None,
    granule_count: int = 0,
    store_kwargs: dict | None = None,
) -> str | None:
    """Fold and write one leaf's column from its resident staged slabs.

    The worker seam ``hive.process_and_write_hive`` calls after the leaf's
    own commit stamp: gate (:func:`leaf_column_plan`) -> fold inputs from the
    issue #342 staged sink (:func:`leaf_slabs` — the exact in-memory values
    the leaf write PUT) -> per-resolution folds (:func:`fold_column`) ->
    :func:`write_column` (D4). Returns the column basename, or ``None`` when
    the declaration carries no leaf-node levels — in which case any column a
    PREVIOUS declaration left at this ``(leaf, window)``, and its sidecar,
    are deleted (:func:`_clear_column`), so the artifact never outlives the
    declaration that made it. Failures RAISE;
    ``hive.process_and_write_hive`` REPORTS them as the unit's
    ``metadata["error"]`` rather than propagating (the leaf is already
    committed there, and a propagated raise would take the unit's whole
    telemetry envelope with it) — the unit still fails, and the idempotent
    retry rewrites leaf and column wholesale (both writers clear their own
    prefix first).

    Memory note (the PR #391 phase 1 review measurement): the node-order
    member's k-way merge concatenates every resident digest once — at the
    ~17.6M-centroid scale ``hive.process_and_write_hive`` already cites for
    its ~200 MB ragged accumulation, the fold measured ~2.0 GB (float64
    copies + sort temporaries inside ``merge_tdigests_kway``). That is a
    DELTA over a bare build-the-slabs harness, and it rides ON TOP of what
    the worker is still holding at this hook: ``chunk_results`` (never
    cleared after the leaf write), ``ragged_chunks`` on the streaming path,
    ``_df_out``, and ``staged`` itself — on Lambda those release only once
    the handler returns from ``process_and_write_hive``. The overlap is
    smaller than that list of live names suggests (the per-cell payload
    ``bytes`` are shared references between ``chunk_results`` and
    ``staged``), and the bound is transient, single-threaded, and dies with
    the call — but it is 4 GB workers (issue #193) absorbing ~2.0 GB on top
    of a loaded heap, not beside an empty one. A digest load ~2x that scale
    does not fit and needs the kernel-side preallocation named on the PR
    thread before the column can carry it.
    """
    store_kwargs = dict(store_kwargs or {})
    # The gate re-validates a declaration the templating path already
    # accepted, so a divergence fires on EVERY shard at once — name the seam
    # and the unit, or N thousand CloudWatch lines read like a template-time
    # error rather than a refusal at the tail of a committed leaf write.
    try:
        plan = leaf_column_plan(config, grid)
    except Exception as e:
        raise ValueError(f"column gate refused shard {shard_key} window {window!r}: {e}") from e
    if plan is None:
        _clear_column(store_root, shard_key, window, store_kwargs)
        return None
    resolutions, fields = plan
    slabs = leaf_slabs(staged, fields, group_path=grid.group_path, n_cells=grid.cells_per_shard)
    folded = fold_column(slabs, fields, cell_order=grid.child_order, resolutions=resolutions)
    return write_column(
        store_root,
        shard_key,
        folded,
        fields,
        node_order=grid.parent_order,
        cell_order=grid.child_order,
        window=window,
        time_range=time_range,
        granule_count=granule_count,
        store_kwargs=store_kwargs,
    )
