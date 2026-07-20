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
  plus ``coverage.moc`` when ``output.coverage_moc`` is on — the hive default)
  plus, per populated leaf, the leaf metadata (root + group + per-array
  ``zarr.json``), the in-leaf ``coverage.moc`` sidecar (depth > 0), one
  whole-leaf ragged object per ragged field, and — since issue #236 — one
  whole-leaf ShardingCodec object per dense array when ``sharded`` (the hive
  default), making the sharded hive count EXACT like the flat sharded one; an
  unsharded K>1 leaf keeps the bounded 1..K dense estimate. (The commit stamp
  rewrites the leaf root ``zarr.json`` in place — no extra object.)

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

# Per-shard attribution on the flat layout assumes the 1-D fullsphere HEALPix
# layout the benchmark manifests use (block index arithmetic on the cells
# axis); the hive layout attributes by leaf prefix and is layout-agnostic.


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


def expected_object_counts(
    grid,
    *,
    n_shards: int,
    store_layout: str = "flat",
    coverage_moc: bool = False,
) -> dict:
    """Expected store object counts for ``n_shards`` populated shards.

    Returns ``{"metadata", "per_shard_min", "per_shard_max", "total_min",
    "total_max", "exact"}``. ``metadata`` is the fixed (shard-independent)
    object count; ``exact`` is True when every per-shard count is
    deterministic (the flat sharded live matrix), in which case
    ``total_min == total_max`` is the asserted total.
    """
    if store_layout == "flat":
        _require_fullsphere(grid)
        members = _member_layouts(grid)
        # Root zarr.json + group zarr.json + one zarr.json per array (exact),
        # plus the OPTIONAL run-level stats parquet (issue #297) — fail-open,
        # absent when the dispatcher role cannot PUT (the CI OIDC role).
        metadata_min = 2 + len(members)
        metadata_max = metadata_min + 1
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
        # root coverage.moc when output.coverage_moc is on (the hive default).
        # The root MOC is a fail-open, regenerable D9 cache
        # (runner.write_root_coverage) — it may legitimately be ABSENT (e.g. the
        # orchestrator role can't PUT it), so it is an OPTIONAL metadata object:
        # the floor is the manifest alone, the ceiling adds the MOC. A real
        # sharded-write bypass lands in the per-shard DATA counts (asserted
        # exactly in object_count_mismatch), never in this metadata window.
        # ... plus the OPTIONAL run-level stats parquet (issue #297), same
        # fail-open posture as the root MOC.
        metadata_min = 1
        metadata_max = 1 + (1 if coverage_moc else 0) + 1
        # Leaf fixed objects: leaf root zarr.json + group zarr.json + one
        # zarr.json per array, plus the in-leaf coverage.moc sidecar (written
        # for any populated leaf when the leaf has depth, i.e. child_order >
        # parent_order), plus TWO node-dir siblings per successful shard: the
        # stats.json sidecar (issue #297) and the shardmap.json leaf sub-map
        # (issue #300 — same success gate; on the Lambda path it may be
        # legitimately absent for a unit whose submap event block was dropped
        # over the async payload cap, which then surfaces here as a mismatch
        # worth seeing rather than a modeled window).
        sidecar = 1 if grid.child_order > grid.parent_order else 0
        lo = hi = 4 + len(members) + sidecar
        for m in members:
            blocks = m["blocks_per_shard"]
            hi += blocks
            if blocks == 1 or not m["ragged"]:
                lo += 1
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")
    return {
        # ``metadata`` is the CEILING (kept for back-compat / display); the floor
        # is ``metadata_min`` — equal on flat (exact), a [1, 1+moc] window on hive.
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
    "objects_other", "other_keys"}``. ``objects_per_shard`` keys are the
    dispatched shards' external labels (``grid.shard_label``); a data object
    whose block resolves to an undispatched shard is keyed ``"block:<n>"`` so
    a stray write is visible rather than silently pooled. ``other_keys`` is a
    capped sample of unclassifiable keys.
    """
    keys = list_store_keys(store_path, **store_kwargs)
    per_shard: dict[str, int] = {}
    other: list[str] = []
    metadata = 0
    rollups = 0

    if store_layout == "flat":
        _require_fullsphere(grid)
        members = {m["name"]: m for m in _member_layouts(grid)}
        label_of = {int(grid.block_index(int(k))[0]): grid.shard_label(int(k)) for k in shard_keys}
        group = grid.group_path
        for key in keys:
            if key == "zarr.json" or key.endswith("/zarr.json") or _is_run_parquet(key):
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
        # The per-shard stats sidecar (issue #297) and the leaf sub-map
        # (issue #300) are SIBLINGS of the leaf .zarr — ``{node}/stats.json``
        # and ``{node}/shardmap.json`` (``_{window}`` suffixed when windowed)
        # — so attribute them to the node's shard rather than pooling them in
        # ``other``.
        stats_of = {
            prefix.rstrip("/").rsplit("/", 1)[0] + "/": label for prefix, label in leaf_of.items()
        }
        for key in keys:
            if key in (hive.MANIFEST_NAME, hive.ROOT_COVERAGE_NAME) or _is_run_parquet(key):
                metadata += 1
                continue
            # Sweep rollups (issue #300): `{family}.rollup.json` at any digit
            # node — second-pass D9 caches with their own bucket, never
            # write-path objects (and never inside a leaf prefix, so the #215
            # per-shard guard is untouched).
            if key.endswith(".rollup.json"):
                rollups += 1
                continue
            for prefix, label in leaf_of.items():
                if key.startswith(prefix):
                    per_shard[label] = per_shard.get(label, 0) + 1
                    break
            else:
                node, _, name = key.rpartition("/")
                is_sibling = any(
                    name == f"{stem}.json"
                    or (name.startswith(f"{stem}_") and name.endswith(".json"))
                    for stem in ("stats", "shardmap")
                )
                if is_sibling and node + "/" in stats_of:
                    per_shard[stats_of[node + "/"]] = per_shard.get(stats_of[node + "/"], 0) + 1
                else:
                    other.append(key)
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")

    return {
        "objects_total": len(keys),
        "objects_metadata": metadata,
        "objects_per_shard": per_shard,
        "objects_rollups": rollups,
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
    coverage_moc: bool = False,
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
    """
    expected = expected_object_counts(
        grid, n_shards=n_shards, store_layout=store_layout, coverage_moc=coverage_moc
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
        "objects_expected": expected["total_max"] if expected["exact"] else None,
        "objects_per_shard": measured["objects_per_shard"],
        "objects_mismatch": object_count_mismatch(measured, expected),
    }


def object_count_mismatch(measured: dict, expected: dict) -> str | None:
    """Describe a measured-vs-expected object-count mismatch, or ``None``.

    The real sharded-write-bypass guard (issue #215: a leaf writing K
    per-inner-chunk objects instead of one sharded object) is the PER-SHARD DATA
    count, asserted exactly whenever the per-shard count is deterministic
    (``exact``). Metadata and total are checked as **windows**: on flat they
    collapse to an exact assertion (``min == max``); on hive they widen by one
    for the optional, fail-open D9 root ``coverage.moc`` (present or absent are
    both valid). Unclassifiable keys are always a finding: the model claims to
    know every object the run writes.
    """
    problems = []
    # Sweep rollups (issue #300) are second-pass D9 caches, not write-path
    # objects: the end-of-run sweep may or may not have landed them by
    # measurement time (fire-and-forget on Lambda), so they are tallied in
    # their own bucket and excluded from the write-path total this model
    # audits (the #215 bypass guard below is untouched — rollups never live
    # inside a leaf prefix).
    total = measured["objects_total"] - measured.get("objects_rollups", 0)
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
