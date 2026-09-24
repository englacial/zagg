"""Leaf-worker pyramid column folds (issue #383; umbrella #381 points (1)-(3)).

A **column artifact** is the leaf worker's own pyramid contribution, computed
at aggregation time while the shard's cell data is resident: one zarr per
``(leaf, window)`` under the leaf's node prefix, holding a resolution group
for every leaf-node level the ``zagg-pyramid/2`` declaration carries, every
member a coarser declaration implies within the leaf's footprint, and the
node-order member (``cells == node`` — the leaf's whole-footprint aggregate,
its **universal partial** for every coarser cell; there is no ``partial/``
grammar, #381 point (2)).

Groups at or finer than the **raw-fold boundary** (``node_order +``
:data:`RAW_MEMBER_DEPTH`) fold directly from the leaf's raw resident cell
slabs — merges-from-raw 1, the #381 point (1) regime law. The coarser
members fold FLAT from the boundary member (issue #538): one k-way call per
output cell over the boundary cells it contains, never chained group from
group, so they record merges-from-raw 2 and the peak memory of any single
merge is bounded by one boundary cell's rows rather than the whole shard.
Exact classes always reduce from raw via :func:`zagg.sweep_overview.fold_dense`
(dense and cheap — their values are unaffected); approximate (t-digest)
classes via the order-independent k-way merge
(:func:`zagg.sweep_overview.fold_digests`, the issue #370 fold law) — the
same kernels the sweep's from-leaves fold runs over the same per-cell inputs
in the same ascending order, so from-raw column bytes are parity-equal with
that fold by construction, and the coarse members are parity-equal with a
flat fold over the boundary member.

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
#: the leaf's own resident cells, directly or flat through the boundary
#: member. No ``source_children`` rides this regime (PR #379 precedent:
#: coverage counts ride ``cascade``) — a leaf column's source is complete by
#: construction; its merges-from-raw is :func:`member_merges_from_raw`.
LEAF_REGIME = "leaf-column"
#: The leaf tier's raw-fold boundary (issue #538): members at ``cells >=
#: node_order + RAW_MEMBER_DEPTH`` fold from the leaf's resident cells; the
#: coarser members fold FLAT from that boundary member — one k-way call per
#: output cell over its already-quantized boundary cells, never chained —
#: and record ``merges_from_raw`` 2. The largest single merge is then the
#: LARGEST BOUNDARY CELL's centroid rows — ``1 / 4**RAW_MEMBER_DEPTH`` of the
#: shard under uniform occupancy, more where photons concentrate (the bound
#: is the cell, not the fraction) — at ~100 B per row of peak memory, where
#: the node-order member merged the whole shard (2.2 GB at the CA store's
#: p90, 6.8 GB at its largest shard — the 0.52 fleet's OOMs). Measured on
#: the CA store's 39 fattest shards, the largest res-11 cell holds 6.0 M
#: rows (14% of its shard at most; ICESat-2 tracks cross every res-11
#: cell), so the worst boundary merge is ~0.6 GB against the 4 GB tier. A
#: constant, not a knob: polar zones ingest per year, so the density that
#: would motivate tuning it does not arise (espg ruling).
RAW_MEMBER_DEPTH = 2


def raw_fold_boundary(node_order: int, cell_order: int, resolutions) -> int | None:
    """The member the coarse groups fold flat from, or ``None`` (all from raw).

    ``node_order + RAW_MEMBER_DEPTH`` when the column carries that member;
    ``None`` when the boundary is not finer than the cells themselves (a
    fold from it IS a fold from raw) or when the column carries no group at
    it (nothing to fold flat from: every group keeps the from-raw law,
    merges-from-raw 1, and the #538 memory bound does not apply). The second
    case is MEMBERSHIP, not a floor on the finest declared resolution: a
    list whose rungs skip the boundary while including something coarser
    (``overviews: [12, 10]`` on the 19/13/9 geometry — members {12, 10, 9})
    hits it with a finest resolution well above it. Such a list is refused
    at declaration since the contiguity ruling (PR #567 thread, 2026-09-17;
    :func:`zagg.pyramid.validate_overviews`), so for a validated store the
    case reduces to a finest resolution of ``node_order + 1``; a hand-built
    manifest can still carry the gap, and membership stays the predicate.
    """
    boundary = int(node_order) + RAW_MEMBER_DEPTH
    if boundary < int(cell_order) and boundary in {int(r) for r in resolutions}:
        return boundary
    return None


def member_merges_from_raw(res: int, boundary: int | None) -> int:
    """A column group's ``merges_from_raw``: 2 below the boundary, else 1."""
    return 2 if boundary is not None and int(res) < boundary else 1


def leaf_entry_merges_from_raw(levels: list, shard_order: int, cell_order: int) -> int:
    """The §4.5 leaf-entry ``actuals`` value: the worst of its declared cells.

    The entry is one record over every declared leaf resolution, so it
    carries the MAXIMUM of :func:`member_merges_from_raw` over them — 1 when
    every declared resolution is at or above the raw-fold boundary, 2 when
    the entry declares one below it (the finisher's record and the
    pyramid-check expectation share this one definition).
    """
    shard_order = int(shard_order)
    boundary = raw_fold_boundary(shard_order, cell_order, column_resolutions(levels, shard_order))
    cells = [int(c) for e in levels if int(e["node"]) == shard_order for c in e["cells"]]
    return max(member_merges_from_raw(c, boundary) for c in cells)


def relay_resolution(levels: list, shard_order: int, cell_order: int) -> int:
    """The member every above-shard merge folds from (spec §4.4, issue #538).

    The leaf column's **coarsest member still folded from raw**, derived from
    :func:`raw_fold_boundary` itself so the two can never disagree (review
    finding): the boundary member when the column CARRIES one (every group
    below it is a flat second merge, so a ladder merge consuming one would
    sit at gen 3), else the node-order member — the one group every column
    carries, and from raw whenever no boundary group exists. Either way a
    stage merge consuming the relay is exactly 2 merges from raw.

    Membership, not the finest declared resolution, is the predicate: a
    list that straddles the boundary without carrying it (``overviews: [12,
    10]`` on the 19/13/9 geometry — members {12, 10, 9}, finest 12, no group
    11) is refused at declaration since the contiguity ruling (PR #567
    thread, 2026-09-17), but a hand-built manifest can still carry it, and
    relaying a member the leaf columns do not hold would leave every
    above-shard merge level at fill. Stage columns relay this member for
    their subtree
    (:func:`zagg.sweep_stage.column_members`). The leaf entry (``node ==
    shard_order``) places the members; a ``levels`` list without one (a
    hand-built manifest — ``expand_overviews`` always emits it) has no
    column resolutions at all and relays the node-order member.
    """
    shard_order = int(shard_order)
    boundary = raw_fold_boundary(shard_order, cell_order, column_resolutions(levels, shard_order))
    return shard_order if boundary is None else int(boundary)


def generation_key(block) -> tuple:
    """The staged sweep's skip-gate key over a summed ``generation`` block.

    ``(n_leaves, max_leaf_timestamp, run ids)`` — the block the stage worker
    records in these attrs and in the ladder entries it writes (§4.4/§4.6).
    The run ids are the issue #417 term: stamps resolve to **one second**, so
    the count/timestamp pair alone reads a same-second rewrite of a child at
    an unchanged leaf count as *current* and serves stale content. Every
    stage stamp carries its ``run_id`` (PR #416 phase 2), so a foreign
    rewrite moves the id set, and the single-writer law forbids a run
    rewriting its own object mid-run. A block without ``run_ids`` (pre-#417,
    or children that are all fleet-written leaf columns — those stamps carry
    no run id) keys on the empty tuple, never on a wildcard: an upgraded
    store re-folds once rather than inheriting the blind spot. A non-block
    keys on ``()``, which matches no generation.

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
    )


def stamped_generation_key(block, stamp) -> tuple:
    """One child's contribution to its parent's skip key (issue #417).

    :func:`generation_key` over the child's recorded ``generation`` block —
    or, for a leaf column (which records none), the leaf identity: one leaf
    at its stamp's timestamp — **unioned with the run id that stamped THIS
    child**. That id is the term the issue turns on: the run that wrote the
    child is what a same-second foreign rewrite changes, and reading the
    relayed block alone would see only the ids of the child's own children
    (empty all the way down a fleet-built store, so the gate would fall back
    to the count/timestamp pair it is meant to strengthen). Fleet-written
    stamps carry no ``run_id`` and contribute nothing.
    """
    stamp = stamp if isinstance(stamp, dict) else {}
    if not isinstance(block, dict):
        block = {"n_leaves": 1, "max_leaf_timestamp": stamp.get("written_at")}
    runs = set(block.get("run_ids") or ())
    if stamp.get("run_id"):
        runs.add(stamp["run_id"])
    return generation_key({**block, "run_ids": sorted(runs)})


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


def _is_composable(meta: dict) -> bool:
    """One declared entry's fold admission: a known class, fully linked.

    Shared with :func:`zagg.sweep_overview.sweep_overviews`' own filter so the
    two fold paths admit exactly the same entries.
    """
    cls = meta.get("class")
    if cls == "packed":
        return bool(meta.get("of"))
    return cls in ("exact", "approximate")


def composable_fields(fields: dict) -> dict:
    """The declared fields a column fold may carry: the composable classes.

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
    group for a companion the column has no business carrying. The ``packed``
    class (issue #515) passes: a composition word is a cell-resolution dense
    scalar whose fold (:func:`fold_column`'s packed branch) pairs it with its
    ``of`` digest's weights, both of which this filter keeps — but ONLY when
    the entry carries that ``of`` linkage: a packed entry without it declares
    a fold with no divisor, and a manifest carrying one (never written here,
    but manifests outlive their writer, spec §4.5) degrades to native
    resolution exactly as an unknown class does, rather than publishing an
    all-fill array under an invalid §3.3 declaration.
    """
    return {n: m for n, m in (fields or {}).items() if isinstance(m, dict) and _is_composable(m)}


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
    check sound: those classes admit nothing but cell-resolution scalars
    (including the §3 composition word) and ragged payloads, both of which are
    one row per cell, so a staged slab of any other extent really is a sink
    that disagrees with the grid — and folding it would write a wrong column,
    so it raises.

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


def fold_column(
    slabs: dict,
    fields: dict,
    *,
    cell_order: int,
    resolutions: list,
    node_order: int | None = None,
) -> dict:
    """Fold the leaf's resident cell slabs into ``{resolution: {field: slab}}``.

    ``4^(source - resolution)`` consecutive source cells share one target
    cell (the ascending packed-word leaf invariant). Groups at or finer than
    the raw-fold boundary (:func:`raw_fold_boundary` — ``node_order +``
    :data:`RAW_MEMBER_DEPTH`) fold from the raw cell slabs, merges-from-raw
    1 (#381 point (1)): approximate fields decode each child cell's payload
    and k-way merge the non-empty digests per target cell, in ascending cell
    order — input-identical to the sweep's from-leaves fold
    (:func:`zagg.sweep_overview._fold_node` +
    :func:`zagg.sweep_overview.fold_digests`) of the committed leaf, which is
    the issue #383 byte-parity contract. The coarser groups fold FLAT from
    the boundary group (issue #538): the same k-way call per target cell,
    over the ``4^(boundary - resolution)`` boundary cells it contains — never
    chained through an intermediate group, so every such group is exactly 2
    merges from raw and the largest single merge is one boundary cell's rows
    rather than the shard's. Exact fields always reduce from raw under their
    declared merge law (dense, cheap, and their values do not depend on it).
    The node-order resolution is the degenerate 1-cell group: the leaf's
    whole-footprint aggregate.

    ``node_order`` places the boundary; a column always carries its node
    member as the coarsest group, so it defaults to ``min(resolutions)`` and
    a whole-column caller may omit it — a partial fold that must place the
    boundary where the whole column would (the boundary member absent from
    ``resolutions`` means every requested group folds from raw) passes it.

    ``fields`` is filtered to the composable classes (:func:`composable_fields`)
    — a D24 ``none`` field has no coarser fold and never becomes a group. A
    resolution FINER than ``cell_order`` is refused by name: it would ask for
    a fractional fold factor, which no guard downstream can read as a divisor
    (every composable class would surface it as an opaque numpy failure
    instead).

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
    cell_order = int(cell_order)
    fields = composable_fields(fields)
    resolutions = [int(r) for r in resolutions]
    for res in resolutions:
        if res > cell_order:
            raise ValueError(
                f"cannot fold a column group at resolution {res}: it is FINER than the "
                f"leaf's cell order {cell_order}, so the fold factor "
                f"4^({cell_order} - {res}) is fractional — a column group is a fold of "
                f"the leaf's own cells, never an upsample of them"
            )
    node_order = min(resolutions) if node_order is None else int(node_order)
    boundary = raw_fold_boundary(node_order, cell_order, resolutions)
    out: dict = {}
    if boundary is not None:
        out[boundary] = {
            **_fold_exact(slabs, fields, 4 ** (cell_order - boundary)),
            **_fold_ragged(slabs, fields, 4 ** (cell_order - boundary)),
        }
    for res in resolutions:
        if res == boundary:
            continue
        source, order = (
            (slabs, cell_order)
            if boundary is None or res >= boundary
            else (out[boundary], boundary)
        )
        out[res] = {
            **_fold_exact(slabs, fields, 4 ** (cell_order - res)),
            **_fold_ragged(source, fields, 4 ** (order - res)),
        }
    return out


def _fold_exact(slabs: dict, fields: dict, factor: int) -> dict:
    """The exact-class groups, ``factor``-to-one under each declared law."""
    from zagg.sweep_overview import fold_dense

    return {
        name: fold_dense(slabs[name], factor, meta.get("method"), meta.get("fill_value", "NaN"))
        for name, meta in fields.items()
        if meta["class"] == "exact"
    }


def _fold_ragged(slabs: dict, fields: dict, factor: int) -> dict:
    """The approximate and packed groups, ``factor``-to-one from ``slabs``.

    ``slabs`` is either the leaf's raw cell slabs or a folded group (the
    boundary member) — both are ``{field: cell slab}`` plus every companion
    sibling, one row per source cell, so the one body serves both tiers.
    """
    from zagg.stats.composition import merge_composition_kway
    from zagg.sweep_overview import (
        _empty_slab,
        decode_digest,
        field_companions,
        fold_digests,
        overview_fold_delta,
        payload_weight,
    )

    groups: dict = {}
    for name, meta in fields.items():
        if meta["class"] == "exact":
            continue
        slab = slabs[name]
        if slab.shape[0] % factor:
            raise ValueError(f"cannot fold {slab.shape[0]} cells {factor}-to-one for {name!r}")
        if meta["class"] == "packed":
            # The packed composition fold (issue #515, spec §3.4): each
            # child cell contributes its ``(word, n)`` pair, ``n`` being
            # the ``of`` digest's weight at the SAME cell — required by
            # name, like a companion sibling: the word is uninterpretable
            # without its divisor, so the pair may never fold apart.
            of_name = meta.get("of")
            of_slab = slabs.get(of_name)
            if of_slab is None:
                raise ValueError(
                    f"field {name!r} declares the packed composition fold over "
                    f"{of_name!r} but no such slab was supplied — the fold's n "
                    f"inputs are that digest's per-cell weights (spec §3.3/§3.4)"
                )
            of_dtype = (fields.get(of_name) or {}).get("dtype") or "float32"
            folded = _empty_slab(meta, slab.shape[0] // factor)
            for j in range(folded.shape[0]):
                parts = [
                    (int(slab[i]), n)
                    for i in range(j * factor, (j + 1) * factor)
                    if (n := payload_weight(of_slab[i], of_dtype)) > 0
                ]
                if parts:
                    folded[j] = merge_composition_kway(parts)
            groups[name] = folded
            continue
        dtype = meta.get("dtype") or "float32"
        inner = tuple(meta.get("inner_shape") or (2,))
        delta = overview_fold_delta(meta)
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
    return groups


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
    if meta.get("class") == "packed":
        # The packed group's decode keys (issue #515): the word dtype and the
        # §3.3 ``of`` linkage a reader pairs it with — recoverable from the
        # manifest, but recorded here for the same outlive-a-declaration
        # reason as the digest budget below.
        entry["dtype"] = meta.get("dtype") or "uint64"
        if meta.get("of") is not None:
            entry["of"] = meta["of"]
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
    from zagg.sweep_overview import ROLE_ATTR, _overview_config, _populated_mask, _staged_hashes
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
    boundary = raw_fold_boundary(node_order, cell_order, resolutions)
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
                        "merges_from_raw": member_merges_from_raw(res, boundary),
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
    # §5 O11 record BEFORE the stamp so it rides it (issue #580), then the
    # sidecar carries the same record.
    hashes = _staged_hashes(store, staged, f"leaf column {basename}")
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(granule_count),
        window=window,
        time_range=time_range if window is not None else None,
        content_hashes=hashes,
    )
    # No record -> no sidecar: the column's sidecar carries nothing the stamp
    # does not, save the O11 record, and a hash-less sidecar on a rewrite would
    # read as a stale-or-absent ambiguity (test_sidecar_lands_after_the_stamp_
    # and_fails_open). The stamp above already stands without the key.
    if hashes is not None:
        _write_sidecar(
            store,
            path,
            shard_key,
            hashes,
            int(populated.sum()),
            granule_count,
            window,
            store_kwargs,
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
    store, path, shard_key, hashes, cells_with_data, granule_count, window, store_kwargs
) -> None:
    """The column's D20 stats sidecar: ``{stem}.stats.json``, after the stamp.

    The overview writer's O11 recipe (issue #342, spec §5): ``hashes`` is the
    content-hash record computed from the staged arrays just written (the
    same record the stamp carries, issue #580), in a
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

        from zagg.store import open_object_store, put_object
        from zagg.telemetry import build_record

        record = build_record(
            shard_key=int(shard_key),
            metadata={
                "cells_with_data": int(cells_with_data),
                "granule_count": int(granule_count),
                "content_hashes": hashes,
            },
            window=window,
        )
        prefix, _, name = path.rstrip("/").rpartition("/")
        put_object(
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

    Memory note (issue #538): the largest single k-way merge is the
    LARGEST raw-fold-boundary cell's resident centroid rows — a 16th of the
    shard under uniform occupancy on the o9/o19 reference geometry, up to
    the whole shard if every photon sat in one res-11 cell; on the CA store's
    fattest shards the largest res-11 cell is 14% of its shard — at ~100 B
    per row of peak (float64 compress temporaries, the lexsort, the
    Rust-side companion reducers), where the node-order member cost that per
    shard photon when it merged the whole shard at once (2.2 GB at the CA
    store's p90 of 21.8 M photons, 6.8 GB at its 67.8 M maximum: the 0.52
    fleet's 47/251 OOMs at 4 GB). The coarse members then merge δ-bounded
    boundary digests, which is small by construction. The transient still
    rides ON TOP of whatever the worker holds at this hook (``staged``
    itself, at least): on Lambda that releases only once the handler
    returns from ``process_and_write_hive``.
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
    folded = fold_column(
        slabs,
        fields,
        cell_order=grid.child_order,
        resolutions=resolutions,
        node_order=grid.parent_order,
    )
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
