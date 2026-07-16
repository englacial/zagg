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
  plus ``coverage.moc`` when ``output.coverage_moc`` is on) plus, per populated
  leaf, the leaf metadata (root + group + per-array ``zarr.json``), the
  in-leaf ``coverage.moc`` sidecar (depth > 0), one leaf-sharded ragged object
  per ragged field, and 1..K objects per dense array.

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
        members = _member_layouts(grid)
        # Root zarr.json + group zarr.json + one zarr.json per array.
        metadata = 2 + len(members)
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
        # Store root: the morton_hive.json manifest, plus the root coverage
        # MOC when output.coverage_moc is on (hive default).
        metadata = 1 + (1 if coverage_moc else 0)
        # Leaf fixed objects: leaf root zarr.json + group zarr.json + one
        # zarr.json per array, plus the in-leaf coverage.moc sidecar (written
        # for any populated leaf when the leaf has depth, i.e. child_order >
        # parent_order).
        sidecar = 1 if grid.child_order > grid.parent_order else 0
        lo = hi = 2 + len(members) + sidecar
        for m in members:
            blocks = m["blocks_per_shard"]
            hi += blocks
            if blocks == 1 or not m["ragged"]:
                lo += 1
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")
    return {
        "metadata": metadata,
        "per_shard_min": lo,
        "per_shard_max": hi,
        "total_min": metadata + n_shards * lo,
        "total_max": metadata + n_shards * hi,
        "exact": lo == hi,
    }


def list_store_keys(store_path: str, **store_kwargs) -> list[str]:
    """All object keys under a store prefix — local path or ``s3://`` alike.

    Rides ``zagg.store.open_object_store`` (the harness's own store factory),
    whose prefix join is "/"-delimited, so sibling prefixes like the Lambda
    ``<store>.status/`` result channel are never swept into the count.
    """
    import obstore

    from zagg.store import open_object_store

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

    if store_layout == "flat":
        members = {m["name"]: m for m in _member_layouts(grid)}
        label_of = {int(grid.block_index(int(k))[0]): grid.shard_label(int(k)) for k in shard_keys}
        group = grid.group_path
        for key in keys:
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
        for key in keys:
            if key in (hive.MANIFEST_NAME, hive.ROOT_COVERAGE_NAME):
                metadata += 1
                continue
            for prefix, label in leaf_of.items():
                if key.startswith(prefix):
                    per_shard[label] = per_shard.get(label, 0) + 1
                    break
            else:
                other.append(key)
    else:
        raise ValueError(f"unknown store_layout: {store_layout!r} (expected 'flat' or 'hive')")

    return {
        "objects_total": len(keys),
        "objects_metadata": metadata,
        "objects_per_shard": per_shard,
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

    Exact expectations (the flat sharded matrix) assert the total AND each
    shard's count — a bypass regression writing K per-chunk objects (issue
    #215) fails both. Bounded expectations (unsharded / hive data-dependent
    counts) assert the total stays inside ``[total_min, total_max]``.
    Unclassifiable keys are always a finding: the model claims to know every
    object the run writes.
    """
    problems = []
    total = measured["objects_total"]
    if expected["exact"]:
        if total != expected["total_max"]:
            problems.append(f"total objects {total} != expected {expected['total_max']}")
        per = expected["per_shard_max"]
        bad = {k: v for k, v in measured["objects_per_shard"].items() if v != per}
        if bad:
            problems.append(f"per-shard object counts != expected {per}: {bad}")
    elif not (expected["total_min"] <= total <= expected["total_max"]):
        problems.append(
            f"total objects {total} outside [{expected['total_min']}, {expected['total_max']}]"
        )
    if measured["objects_other"]:
        problems.append(
            f"{measured['objects_other']} unrecognized object key(s), "
            f"e.g. {measured['other_keys'][:3]}"
        )
    return "; ".join(problems) or None
