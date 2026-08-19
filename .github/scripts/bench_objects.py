"""Store object-count metric for the benchmark harnesses (issue #240).

The regression class the hive/sharded write paths need a tripwire for — a
worker writing K per-inner-chunk objects instead of one sharded object (the
~250x object blow-up documented in issue #215) — is invisible in the recorded
cost/wall/RSS metrics except as second-order cost drift. This module LISTs the
output store after a run and compares the measured object count against a
model derived from the run's grid config, so a sharded-write bypass fails (or
is recorded) instead of drifting.

The expected-count model is derived from the grid's own template spec
(``grid.spec()`` / ``grid.shard_spec()``), never re-derived from config knobs,
so it cannot drift from what the template actually emits:

- **flat** (single shared zarr): fixed metadata objects (the root ``zarr.json``,
  the group ``zarr.json``, one ``zarr.json`` per array) plus, per populated
  shard, one data object per array per storage block the shard owns. A sharded
  array (ShardingCodec) gives the shard ONE block (``shard_objects_per_shard``
  at a finer ``shard_order``), so its count is deterministic; an unsharded
  array at K>1 gives 1..K objects (zarr's default ``write_empty_chunks=False``
  omits all-fill chunks, so empty inner chunks write nothing).
- **hive** (per-shard leaf zarrs): store-root objects (``morton_hive.json``,
  plus the optional ``coverage.moc``, plus the fail-open ``aggregation.yaml``
  semantic core, issue #299)
  plus, per populated leaf, the leaf metadata (root + group + per-array
  ``zarr.json``), the in-leaf ``coverage.moc`` sidecar (depth > 0), one
  whole-leaf ragged object per ragged field, and — since issue #236 — one
  whole-leaf ShardingCodec object per dense array when ``sharded`` (the hive
  default), making the sharded hive count EXACT like the flat sharded one; an
  unsharded K>1 leaf keeps the bounded 1..K dense estimate. (The commit stamp
  rewrites the leaf root ``zarr.json`` in place — no extra object.)
- **hive leaf pyramid column** (issue #418): since the issue #384 ``/2``
  default flip a default declaration also makes every leaf worker write its
  own column artifact — ``{node}/{window}.pyramid.zarr``, a SIBLING of the
  leaf (specification §4.6) — plus its D20 stats sidecar. It is modeled
  exactly, per populated leaf, off the store's own ``pyramid`` declaration
  (see :func:`_column_objects`): a column with no declared leaf levels costs
  nothing, and a ``/1`` (or declared-off) store keeps the pre-#384 count.

**Scope of the model: LEAF-ONLY — the fleet write path, never a swept
ladder** (issue #418, the decision that issue asks to be made explicitly).
What the model claims to predict exactly is what the *leaf workers* write:
the leaf zarr, its sidecars, and — new — the leaf's own pyramid column.
Everything the SWEEP writes stays outside the audited total, in the
unbounded ``objects_rollups`` / ``objects_sweep`` buckets: the D9
rollups (issue #300), the ladder overview zarrs above the shard node (issue
#201/#384), and **stage columns** — the ``{window}.pyramid.zarr`` artifacts
the staged sweep writes at ANCESTOR nodes (specification §4.6). Three
reasons, all structural rather than convenient: the sweep is fire-and-forget
on Lambda, so its artifacts may or may not have landed at measurement time;
its column *placement* is dispatch cadence, which §4.6 declares
"orchestration, never contract", so no fixed count could be right; and a
partial or scoped sweep is a legal store state. The leaf column is on the
other side of that line — one deterministic artifact per populated leaf,
written inside the same worker whose object count the issue #215 tripwire
guards — so it is audited exactly, and a column that goes missing or arrives
multiplied trips the per-shard assertion.

**The hive model assumes the UNWINDOWED leaf** (review finding), as the flat
model assumes fullsphere HEALPix (:func:`_require_fullsphere`). One populated
shard is one leaf and one column; a windowed store writes one of each per
``(leaf, window)``, which this model does not count — and would not reach in
any case, since the per-shard attribution builds its leaf prefixes from
``hive.shard_leaf_path("", key)`` with no ``window=``, so a windowed leaf
matches no prefix and surfaces as a loud ``other`` first. Widening the model
to windows is its own change, with its own test surface; it is not smuggled
in here.

Determinism caveats (documented, not modeled): a populated shard is assumed to
write every array — true when every field aggregates the same observations
(the benchmark matrix's ``count``/``h_tdigest`` both reduce ``h_ph``) and the
shard map only dispatches AOI-intersecting shards (so ``aoi_mask`` always has
a ``True`` cell). A dense array whose whole shard slab equals its fill value
would be omitted by zarr; that cannot happen for the coordinate/count arrays
of a shard with observations.

Kept separate from ``bench_metrics`` (pure arithmetic, no IO): the measured
side here LISTs the store via ``zagg.store.open_object_store``, which works
identically for a local path and ``s3://`` (the same factory the workers write
through).
"""

from __future__ import annotations

import re

from zagg.column import COLUMN_SUFFIX

# Per-shard attribution on the flat layout assumes the 1-D fullsphere HEALPix
# layout the benchmark manifests use (block index arithmetic on the cells
# axis); the hive layout attributes by leaf prefix and is layout-agnostic.

#: A leaf zarr basename's morton-id stem (D1 grammar: sign + base 1-6, then
#: digits 1-4). Sweep overview basenames (issue #201, D23 window naming:
#: ``{window}.zarr`` / ``all.zarr``) can never match it — window labels carry
#: 0/5-9 digits or letters and ``all`` is the reserved token — which is what
#: lets the hive walk tell an overview zarr from a stray source leaf.
_MORTON_ID_RE = re.compile(r"-?[1-6][1-4]*")


def _column_node(key: str) -> str | None:
    """The node dir holding ``key``'s pyramid COLUMN zarr, or ``None``.

    A column artifact is ``{node}/{window}.pyramid.zarr`` (specification
    §4.6): the leaf worker's own contribution at a leaf node, and the staged
    sweep's relay at an ancestor node — the SAME basename grammar at both, so
    the node the caller resolves it against is what tells them apart (a
    dispatched leaf's node makes it write-path; anything else makes it a
    sweep artifact). Keyed off ``COLUMN_SUFFIX`` rather than a second literal,
    and checked on the FIRST ``.zarr`` segment for the same reason
    :func:`_overview_node` is: an object nested under a real leaf's zarr is
    that shard's, never a sibling artifact's.
    """
    parts = key.split("/")
    for i, part in enumerate(parts):
        if part.endswith(".zarr"):
            return "/".join(parts[:i]) if part.endswith(COLUMN_SUFFIX) else None
    return None


def _overview_node(key: str) -> str | None:
    """The node dir holding ``key``'s sweep overview zarr, or ``None``.

    A key belongs to an overview (issue #201) when its FIRST ``.zarr`` segment
    has a stem that does not parse as a morton id — window labels carry 0/5-9
    digits or letters and ``all`` is the reserved token, so ``{window}.zarr`` /
    ``all.zarr`` can never collide with a source leaf's ``{id}[_{window}].zarr``
    (see :data:`_MORTON_ID_RE`). Taking the FIRST segment keeps the hive walk's
    leaf-membership-first ordering: anything nested under a real leaf's zarr is
    that shard's object, never an overview.

    A COLUMN zarr is excluded (issue #418): its stem does not parse as a
    morton id either, so it would otherwise read as an overview — but a
    column is a different artifact family with a different audit posture
    (:func:`_column_node`), and the two buckets must stay disjoint.
    """
    parts = key.split("/")
    for i, part in enumerate(parts):
        if not part.endswith(".zarr"):
            continue
        if part.endswith(COLUMN_SUFFIX):
            return None
        stem = part.removesuffix(".zarr").split("_", 1)[0]
        return None if _MORTON_ID_RE.fullmatch(stem) else "/".join(parts[:i])
    return None


def _require_fullsphere(grid) -> None:
    """Fence the flat model/attribution to its assumption (review, PR #242).

    The flat-layout block arithmetic assumes the 1-D fullsphere HEALPix layout
    (contiguous nested ids under a parent). A rect grid would die on a bare
    ``AttributeError`` and a dense-layout HEALPix grid would silently
    mis-attribute -- fail loudly instead. Every benchmark manifest target is
    fullsphere HEALPix today; extend the model when a real non-fullsphere
    target exists.
    """
    layout = getattr(grid, "layout", None)
    if layout != "fullsphere":
        raise NotImplementedError(
            "flat object-count model assumes fullsphere HEALPix; got "
            f"layout={layout!r} ({type(grid).__name__})"
        )


def _member_layouts(grid, *, leaf: bool = False) -> list[dict]:
    """Per-array storage layout facts from the grid's own template spec.

    One entry per array in the template group: the first-axis chunk extent
    (``chunk0`` — the ShardingCodec outer chunk for a sharded array), the
    first-axis span one dispatch shard owns (``span``, in the array's own
    axis units: cells for per-cell arrays, chunks for the ``resolution:
    chunk`` companions), the blocks-per-shard quotient, and whether the array
    is a ragged (vlen-bytes) field.
    """
    spec = grid.shard_spec() if leaf else grid.spec()
    members = []
    for name, member in spec.members.items():
        cg = member.chunk_grid
        cfg = cg["configuration"] if isinstance(cg, dict) else cg.configuration
        chunk0 = int(cfg["chunk_shape"][0])
        dims = tuple(member.dimension_names or ())
        span = grid.chunks_per_shard if dims and dims[0] == "chunks" else grid.cells_per_shard
        members.append(
            {
                "name": name,
                "chunk0": chunk0,
                "span": span,
                "blocks_per_shard": span // chunk0,
                "ragged": str(member.data_type) == "variable_length_bytes",
            }
        )
    return members


def _column_objects(pyramid, node_order: int) -> int:
    """Objects ONE leaf's pyramid column costs, or 0 when none is declared.

    The issue #384 ``/2`` default flip made this a term every default store
    pays (issue #418). Derived from the store's own ``pyramid`` declaration
    through the writer's own functions — :func:`zagg.column.column_resolutions`
    for the groups and :func:`zagg.column.composable_fields` for the arrays —
    so the model cannot drift from what ``write_column`` emits, the same
    posture :func:`_member_layouts` takes toward the template spec.

    Per :func:`zagg.column.write_column`'s D4 order, one column is: the root
    ``zarr.json`` (the template, the attrs and the commit stamp all REWRITE
    it — three PUTs, one object), then per resolution group a group
    ``zarr.json`` plus, per array (``morton`` + one per composable field),
    one ``zarr.json`` and one chunk object — the groups are one chunk each
    (``chunks_per_shard == 1`` turns the inert sharding back off), so the
    count is deterministic exactly as the #236 sharded leaf's is. The D20
    stats sidecar written after the stamp is the ``+ 1``, counted like the
    leaf's own ``stats.json`` sibling.

    Zero for a ``/1`` (or declared-off) block, and zero when the declaration
    carries no leaf-node entry — both are stores whose leaves write no column.
    """
    from zagg.column import column_resolutions, composable_fields
    from zagg.pyramid import PYRAMID_SPEC_V2

    if not isinstance(pyramid, dict) or pyramid.get("spec") != PYRAMID_SPEC_V2:
        return 0
    resolutions = column_resolutions(pyramid.get("overviews") or [], node_order)
    if not resolutions:
        return 0
    fields = (pyramid.get("overview") or {}).get("fields") or {}
    arrays = 1 + len(composable_fields(fields))
    return 1 + len(resolutions) * (1 + 2 * arrays) + 1


def expected_object_counts(
    grid,
    *,
    n_shards: int,
    store_layout: str = "flat",
    pyramid: dict | None = None,
) -> dict:
    """Expected store object counts for ``n_shards`` populated shards.

    Returns ``{"metadata", "per_shard_min", "per_shard_max", "total_min",
    "total_max", "exact"}``. ``metadata`` is the fixed (shard-independent)
    object count; ``exact`` is True when every per-shard count is
    deterministic (the flat sharded live matrix), in which case
    ``total_min == total_max`` is the asserted total.

    ``pyramid`` is the store manifest's ``pyramid`` block (hive only) — the
    declaration that decides whether each leaf also writes a column
    (:func:`_column_objects`). :func:`measure_objects` reads it off the store
    it is measuring; ``None`` models the pre-#384 shape, i.e. no column.
    """
    if store_layout == "flat":
        _require_fullsphere(grid)
        members = _member_layouts(grid)
        # Root zarr.json + group zarr.json + one zarr.json per array — EXACT.
        # Per-run root telemetry (the issue #297 stats parquet) is not modeled
        # here: it rides the unbounded ``objects_telemetry`` bucket instead
        # (see the hive comment below and :func:`_is_root_telemetry`).
        metadata_min = metadata_max = 2 + len(members)
        lo = hi = 0
        for m in members:
            blocks = m["blocks_per_shard"]
            hi += blocks
            # One block per shard is deterministic (a populated shard writes
            # it — the ragged case assumes shard data, see module docstring);
            # a multi-block dense array writes at least its one populated
            # chunk; a multi-block ragged array may write none.
            if blocks == 1 or not m["ragged"]:
                lo += 1
    elif store_layout == "hive":
        members = _member_layouts(grid, leaf=True)
        # Store root: the morton_hive.json manifest (always written) PLUS the
        # root coverage.moc. The MOC is a fail-open, regenerable D9 cache that
        # may legitimately be ABSENT (e.g. the orchestrator role can't PUT it),
        # so it is an OPTIONAL metadata object: the floor is the manifest
        # alone, the ceiling adds the MOC. The ceiling counts it
        # UNCONDITIONALLY — not off ``output.coverage_moc`` — because it has
        # TWO independent writers and only one honours that knob: the
        # end-of-run write in ``runner.agg`` (gated on ``get_coverage_moc``)
        # and ``zagg.sweep.MocFamily.finish`` -> ``hive.write_root_coverage``,
        # reached from ``sweep_after_run`` with ``DEFAULT_FAMILIES`` (which
        # includes ``"moc"``) and gated only on ``get_sweep``. A hive target
        # with ``coverage_moc: false`` and the default sweep still lands the
        # object, so a knob-gated ceiling would hard-fail a correct store. A
        # real sharded-write bypass lands in the per-shard DATA counts
        # (asserted exactly in object_count_mismatch), never in this window.
        # ... plus the OPTIONAL aggregation.yaml semantic core (issue #299, D19
        # — a fail-open derived convenience riding the manifest write), same
        # fail-open posture as the root MOC.
        # Per-run ROOT TELEMETRY is deliberately NOT in this window (issue
        # #362): the run stats parquet (``stats_{ts}_{run_id}.parquet``, issue
        # #297) and the sweep run record (``sweep_stats_{ts}.json``, issue #353)
        # are per-run UNIQUE names, so in a store that more than one run writes
        # they ACCUMULATE — a fixed ceiling can never be right for them (it held
        # only while every run got a fresh store, and went red on main the day
        # twelve runs shared one). They ride the unbounded ``objects_telemetry``
        # bucket instead (:func:`_is_root_telemetry`), counted and reported but
        # excluded from the audited write-path total — the same posture the D9
        # rollup/overview buckets already take. This window therefore stays
        # TIGHT (manifest + optional root MOC + optional aggregation.yaml), so
        # real root-metadata drift still trips it.
        metadata_min = 1
        metadata_max = 1 + 1 + 1
        # Leaf fixed objects: leaf root zarr.json + group zarr.json + one
        # zarr.json per array, plus the in-leaf coverage.moc sidecar (written
        # for any populated leaf when the leaf has depth, i.e. child_order >
        # parent_order), plus THREE node-dir siblings per successful shard:
        # the stats.json sidecar (issue #297), its granules.json recorded
        # id list (issue #388 — written by the leaf seam right after the
        # commit stamp, so it lands on both backends) and the shardmap.json
        # leaf sub-map (issue #300 — same success gate; on the Lambda path it
        # may be legitimately absent for a unit whose submap event block was
        # dropped over the async payload cap, which then surfaces here as a
        # mismatch worth seeing rather than a modeled window).
        # ... plus the leaf's own pyramid column and its sidecar (issue #418),
        # exact per populated leaf and zero when none is declared.
        sidecar = 1 if grid.child_order > grid.parent_order else 0
        lo = hi = 5 + len(members) + sidecar + _column_objects(pyramid, grid.parent_order)
        for m in members:
            blocks = m["blocks_per_shard"]
            hi += blocks
            if blocks == 1 or not m["ragged"]:
                lo += 1
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")
    return {
        # ``metadata`` is the CEILING (kept for back-compat / display); the floor
        # is ``metadata_min`` — equal on flat (exact), a [1, 3] window on hive.
        "metadata": metadata_max,
        "metadata_min": metadata_min,
        "per_shard_min": lo,
        "per_shard_max": hi,
        "total_min": metadata_min + n_shards * lo,
        "total_max": metadata_max + n_shards * hi,
        "exact": lo == hi,
    }


def _is_run_parquet(key: str) -> bool:
    """A store-root run-level stats parquet (issue #297): ``stats_*.parquet``."""
    return "/" not in key and key.startswith("stats_") and key.endswith(".parquet")


def _is_sweep_record(key: str) -> bool:
    """A store-root sweep run record (issue #353): ``sweep_stats_*.json``.

    One per sweep pass, and the local dispatcher sweeps in-process at end of
    run (``sweep_after_run``, default on for hive) — so a hive run lands
    exactly one of these at the root alongside the run parquet. Deliberately
    outside the :func:`_is_run_parquet` grammar (a sweep record must never read
    as a shard run record).
    """
    return "/" not in key and key.startswith("sweep_stats_") and key.endswith(".json")


def _is_root_telemetry(key: str) -> bool:
    """A store-root PER-RUN telemetry object (issue #362): parquet or sweep record.

    Both names embed the run's own timestamp/id, so a store that more than one
    run writes accumulates one of each PER RUN — never a fixed count. They are
    tallied in their own unbounded ``objects_telemetry`` bucket (like the D9
    rollups/overviews) and excluded from the audited write-path total, rather
    than widening the metadata window by a number that can only ever be wrong.
    """
    return _is_run_parquet(key) or _is_sweep_record(key)


def _is_status_object(key: str) -> bool:
    """Any key under a ``.status`` prefix segment: the per-run status channel.

    Generalizes the :func:`_is_run_parquet` telemetry filter (ratified on
    issue #327): ``<store>.status/run-<id>/...`` holds the always-on per-shard
    status objects, dispatch manifests, and the issue #151 result envelopes —
    run telemetry, never write-path store objects, so the #215/#240 tripwire
    must not count them. The prefix is a store SIBLING, which the
    "/"-delimited prefix join already keeps out of a store-rooted LIST; this
    explicit exclusion covers a listing taken from a parent prefix.
    """
    return any(part.endswith(".status") for part in key.split("/")[:-1])


def list_store_keys(store_path: str, **store_kwargs) -> list[str]:
    """All object keys under a store prefix — local path or ``s3://`` alike.

    Rides ``zagg.store.open_object_store`` (the harness's own store factory),
    whose prefix join is "/"-delimited, so sibling prefixes like the Lambda
    ``<store>.status/`` result channel are never swept into the count.
    """
    from pathlib import Path

    import obstore

    from zagg.store import open_object_store

    # ``open_object_store`` mkdir's an absent LOCAL path (load-bearing for its
    # side-channel-JSON writers), which here would count a mistyped store as 0
    # objects (review, PR #242). Fail as "not found" instead; s3 paths keep the
    # factory's behavior (a wrong prefix lists empty, and the count mismatch
    # still fails the run).
    if not store_path.startswith("s3://") and not Path(store_path).exists():
        raise FileNotFoundError(f"store not found: {store_path}")
    store = open_object_store(store_path, **store_kwargs)
    keys: list[str] = []
    for batch in obstore.list(store):
        keys.extend(str(meta["path"]) for meta in batch)
    return keys


def store_object_counts(
    store_path: str,
    *,
    grid,
    shard_keys,
    store_layout: str = "flat",
    **store_kwargs,
) -> dict:
    """LIST a run's output store and attribute its objects per shard.

    Returns ``{"objects_total", "objects_metadata", "objects_per_shard",
    "objects_rollups", "objects_sweep", "objects_telemetry", "objects_other",
    "other_keys"}``. ``objects_sweep`` is the SECOND-PASS SWEEP bucket, of
    which overview zarrs are one family and ancestor-node stage columns
    (issue #418) another — named for the pass, not for one of its families
    (issue #433). ``objects_per_shard`` keys are the dispatched shards'
    external labels (``grid.shard_label``); a data object whose block resolves
    to an undispatched shard is keyed ``"block:<n>"`` so a stray write is
    visible rather than silently pooled. ``other_keys`` is a capped sample of
    unclassifiable keys.
    """
    keys = [k for k in list_store_keys(store_path, **store_kwargs) if not _is_status_object(k)]
    per_shard: dict[str, int] = {}
    other: list[str] = []
    metadata = 0
    rollups = 0
    sweep = 0
    telemetry = 0

    if store_layout == "flat":
        _require_fullsphere(grid)
        members = {m["name"]: m for m in _member_layouts(grid)}
        label_of = {int(grid.block_index(int(k))[0]): grid.shard_label(int(k)) for k in shard_keys}
        group = grid.group_path
        for key in keys:
            if _is_root_telemetry(key):
                telemetry += 1
                continue
            if key == "zarr.json" or key.endswith("/zarr.json"):
                metadata += 1
                continue
            parts = key.split("/")
            member = (
                members.get(parts[1])
                if len(parts) >= 4 and parts[0] == group and parts[2] == "c"
                else None
            )
            if member is None or not parts[3].isdigit():
                other.append(key)
                continue
            # First-axis block -> owning parent (shard) nested id: nested ids
            # at a finer order tile contiguously under their parent, so the
            # block's cell offset divided by the shard span is the parent
            # block. Works for sharded/unsharded per-cell arrays and the
            # chunk-grid companions alike (span is in the array's axis unit).
            parent = int(parts[3]) * member["chunk0"] // member["span"]
            label = label_of.get(parent, f"block:{parent}")
            per_shard[label] = per_shard.get(label, 0) + 1
    elif store_layout == "hive":
        from zagg import hive

        leaf_of = {
            hive.shard_leaf_path("", int(k)).lstrip("/") + "/": grid.shard_label(int(k))
            for k in shard_keys
        }
        # The per-shard stats sidecar (issue #297), its recorded granule-id
        # list (issue #388) and the leaf sub-map (issue #300) are SIBLINGS of
        # the leaf .zarr — ``{node}/stats.json``, ``{node}/granules.json`` and
        # ``{node}/shardmap.json`` (``_{window}`` suffixed when windowed)
        # — so attribute them to the node's shard rather than pooling them in
        # ``other``.
        stats_of = {
            prefix.rstrip("/").rsplit("/", 1)[0] + "/": label for prefix, label in leaf_of.items()
        }
        # Pre-pass: the nodes that actually hold a sweep overview zarr. These
        # are the ONLY places a D23 ``{window}.stats.json`` overview sidecar
        # can legitimately sit (PR #356 writes one beside each overview leaf,
        # at the ancestor node the pyramid folds to), so they anchor the
        # sidecar branch below instead of it matching on the suffix alone —
        # a ``.stats.json`` at the store root, at an arbitrary deep path, or
        # at an UNDISPATCHED leaf's node stays a loud ``other``, which is the
        # "the model knows every object the run writes" contract.
        overview_nodes = {n for n in map(_overview_node, keys) if n is not None}
        # ... and the nodes holding a SWEEP-written stage column (issue #418):
        # its D20 sidecar sits beside it exactly as an overview's does, so it
        # anchors the same branch. A dispatched leaf's node is excluded — its
        # column sidecar is that shard's write-path object, caught by the
        # sibling rule below, and the two branches must stay disjoint.
        overview_nodes |= {
            n for n in map(_column_node, keys) if n is not None and n + "/" not in stats_of
        }
        for key in keys:
            if _is_root_telemetry(key):
                telemetry += 1
                continue
            if key in (
                hive.MANIFEST_NAME,
                hive.ROOT_COVERAGE_NAME,
                hive.AGGREGATION_CORE_NAME,
            ):
                metadata += 1
                continue
            # Leaf-prefix membership FIRST (issue #300 review): an object that
            # lands INSIDE a leaf `.zarr/` prefix is that shard's data object —
            # rollup-named or not — so it counts into ``per_shard`` and trips
            # the exact #215 per-shard guard. Only keys OUTSIDE every leaf
            # prefix are eligible for the rollup / sibling buckets below.
            for prefix, label in leaf_of.items():
                if key.startswith(prefix):
                    per_shard[label] = per_shard.get(label, 0) + 1
                    break
            else:
                # Sweep rollups (issue #300): `{family}.rollup.json` at a digit
                # node — second-pass D9 caches with their own bucket, never
                # write-path objects. Legit rollups sit at `{node}/` (siblings
                # of the leaf `.zarr/`), so classifying them here — after the
                # leaf-membership check has failed — leaves the per-shard guard
                # untouched while a MISPLACED in-leaf rollup falls to the
                # attribution above and trips #215 instead of vanishing.
                if key.endswith(".rollup.json"):
                    rollups += 1
                    continue
                # Pyramid columns (issue #418), before the overview branch —
                # they share the "stem is not a morton id" shape but not the
                # audit posture. A column under a DISPATCHED leaf's node is
                # that shard's write-path object (the leaf worker wrote it
                # while the shard's cells were resident), so it counts into
                # ``per_shard`` and rides the exact #215 guard; a column at
                # any other node is a STAGE column the sweep wrote, which
                # joins the second-pass sweep bucket outside the audited total
                # (the leaf-only scope stated in the module docstring).
                column_node = _column_node(key)
                if column_node is not None:
                    label = stats_of.get(column_node + "/")
                    if label is None:
                        sweep += 1
                    else:
                        per_shard[label] = per_shard.get(label, 0) + 1
                    continue
                # Sweep overview zarrs (issue #201): `{window}.zarr/...` at a
                # digit node. The window-token basename can never parse as a
                # morton id (see _MORTON_ID_RE), while a source leaf's stem
                # before the first `_` always does — so an overview classifies
                # into its own D9 second-pass bucket (like the rollups above,
                # excluded from the audited write-path total) while a stray
                # id-named `.zarr` outside the dispatched leaf set stays a
                # loud ``other`` finding.
                if _overview_node(key) is not None:
                    sweep += 1
                    continue
                node, _, name = key.rpartition("/")
                # Sidecar grammars, both naming generations
                # (``zagg.telemetry.sidecar_key``): the legacy
                # ``stats.json`` / ``stats_{window}.json`` every current writer
                # emits, and the D23 window-only ``{stem}.stats.json``
                # (``morton-hive/3``, PR #356). The two are disjoint by
                # construction ("stats.json" cannot end in ".stats.json").
                is_sibling = any(
                    name == f"{stem}.json"
                    or (name.startswith(f"{stem}_") and name.endswith(".json"))
                    or name.endswith(f".{stem}.json")
                    for stem in ("stats", "granules", "shardmap")
                )
                if is_sibling and node + "/" in stats_of:
                    per_shard[stats_of[node + "/"]] = per_shard.get(stats_of[node + "/"], 0) + 1
                elif name.endswith(".stats.json") and node in overview_nodes:
                    # A D23-named stats sidecar at a node that HOLDS an
                    # overview zarr is that overview's sidecar (PR #356 writes
                    # one per overview leaf, at the ANCESTOR node the pyramid
                    # folds to), so it rides the same second-pass D9 bucket as
                    # the overview it describes. Reached only after the
                    # leaf-prefix-membership check above has failed AND the
                    # node is not a dispatched leaf's, so it can neither
                    # swallow a misplaced in-leaf object (issue #215 tripwire)
                    # nor steal a real leaf sidecar's attribution; the
                    # ``overview_nodes`` anchor keeps every OTHER ``.stats.json``
                    # (store root, stray prefix, undispatched leaf) loud.
                    sweep += 1
                else:
                    other.append(key)
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")

    return {
        "objects_total": len(keys),
        "objects_metadata": metadata,
        "objects_per_shard": per_shard,
        "objects_rollups": rollups,
        "objects_sweep": sweep,
        "objects_telemetry": telemetry,
        "objects_other": len(other),
        "other_keys": other[:20],
    }


def measure_objects(
    store_path: str,
    *,
    grid,
    shard_keys,
    n_shards: int,
    store_layout: str = "flat",
    **store_kwargs,
) -> dict:
    """Measure a run's store objects and compare against the expected model.

    The shared harness entry point (issue #240): LISTs ``store_path``,
    attributes per shard, and returns the record payload both harnesses
    thread into their metrics -- measured total, the exact expectation (null
    when the layout's count is data-dependent), the per-shard attribution,
    and the mismatch description (null when clean). ``n_shards`` is the number
    of shards expected to have written (the dispatch count for the per-merge
    harness, the completed-with-data count for the full-AOI fan-out).

    On hive the pyramid DECLARATION is read from the store's own manifest and
    threaded into the model (issue #418), so the column term needs no new
    argument at the harness call sites: the block is the run's declaration,
    written by ``hive.build_manifest`` before any leaf runs, never a product
    of the write path being audited. A hive store with no readable manifest
    raises here rather than quietly modelling the pre-#384 shape.
    """
    pyramid = None
    if store_layout == "hive":
        from zagg.hive import read_manifest

        pyramid = (read_manifest(store_path, **store_kwargs) or {}).get("pyramid")
    expected = expected_object_counts(
        grid, n_shards=n_shards, store_layout=store_layout, pyramid=pyramid
    )
    measured = store_object_counts(
        store_path,
        grid=grid,
        shard_keys=shard_keys,
        store_layout=store_layout,
        **store_kwargs,
    )
    return {
        "objects_total": measured["objects_total"],
        # The netted, audited count (issue #362) — the one directly comparable
        # to ``objects_expected``; ``objects_total`` stays gross so a reused
        # store's real object footprint is still legible.
        "objects_write_path": write_path_total(measured),
        "objects_expected": expected["total_max"] if expected["exact"] else None,
        "objects_per_shard": measured["objects_per_shard"],
        # Per-run root telemetry (issue #362): reported so an accumulating
        # shared store is legible in metrics.json / the run log, never audited
        # (``object_count_mismatch`` nets it out of the total).
        "objects_telemetry": measured["objects_telemetry"],
        "objects_mismatch": object_count_mismatch(measured, expected),
    }


def write_path_total(measured: dict) -> int:
    """The audited WRITE-PATH object count: the gross total, netted.

    ``objects_total`` is every object under the store prefix. Three buckets are
    not write-path objects and are subtracted here:

    * ``objects_rollups`` (issue #300) and ``objects_sweep`` — the second-pass
      D9 caches the end-of-run sweep may or may not have landed by measurement
      time (fire-and-forget on Lambda: overview zarrs, issue #201, and stage
      columns, issue #418);
    * per-run root telemetry (issue #362) — additionally unbounded, since its
      names are per-run unique, so a reused store accumulates it and no fixed
      expectation could ever hold.

    This is the number :func:`object_count_mismatch` audits and the one
    ``objects_expected`` is comparable to; ``objects_total`` stays gross
    because storage/cost questions want every object. Single-sourced so the
    reported figure and the asserted figure cannot drift apart.

    Only ``objects_total`` is subscripted; the three subtrahends are read
    tolerantly ON PURPOSE (review finding) — :func:`object_count_mismatch`
    accepts a PARTIAL ``measured`` carrying just the terms an assertion
    needs, so a caller that never swept omits the sweep buckets rather than
    writing zeros. The cost of that tolerance is that a bucket RENAME would
    under-subtract silently instead of raising; the key set the measured side
    emits is pinned directly against this docstring so a half-done rename
    fails there first.
    """
    return (
        measured["objects_total"]
        - measured.get("objects_rollups", 0)
        - measured.get("objects_sweep", 0)
        - measured.get("objects_telemetry", 0)
    )


def object_count_mismatch(measured: dict, expected: dict) -> str | None:
    """Describe a measured-vs-expected object-count mismatch, or ``None``.

    The real sharded-write-bypass guard (issue #215: a leaf writing K
    per-inner-chunk objects instead of one sharded object) is the PER-SHARD DATA
    count, asserted exactly whenever the per-shard count is deterministic
    (``exact``). Metadata and total are checked as **windows**: on flat they
    collapse to an exact assertion (``min == max``); on hive they widen for the
    optional, fail-open D9 root objects (``coverage.moc``, ``aggregation.yaml``
    — present or absent are both valid). Unclassifiable keys are always a
    finding: the model claims to know every object the run writes.
    """
    problems = []
    total = write_path_total(measured)
    meta = measured["objects_metadata"]
    meta_lo, meta_hi = expected["metadata_min"], expected["metadata"]
    if not (meta_lo <= meta <= meta_hi):
        problems.append(
            f"metadata objects {meta} != expected {meta_hi}"
            if meta_lo == meta_hi
            else f"metadata objects {meta} outside [{meta_lo}, {meta_hi}]"
        )
    lo_t, hi_t = expected["total_min"], expected["total_max"]
    if not (lo_t <= total <= hi_t):
        problems.append(
            f"total objects {total} != expected {hi_t}"
            if lo_t == hi_t
            else f"total objects {total} outside [{lo_t}, {hi_t}]"
        )
    # Per-shard DATA exactness — the #215 bypass tripwire — regardless of the
    # metadata/total window (a bypass inflates a shard's data-object count).
    if expected["exact"]:
        per = expected["per_shard_max"]
        bad = {k: v for k, v in measured["objects_per_shard"].items() if v != per}
        if bad:
            problems.append(f"per-shard object counts != expected {per}: {bad}")
    if measured["objects_other"]:
        problems.append(
            f"{measured['objects_other']} unrecognized object key(s), "
            f"e.g. {measured['other_keys'][:3]}"
        )
    return "; ".join(problems) or None
