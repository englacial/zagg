"""Unified second-pass rollup sweep (issue #300, design §7 / D22).

One idempotent bottom-up pass over a hive store's digit tree that folds leaf
artifacts into interior-node rollups, per registered **artifact family**
(D22): stats sidecars (the :func:`zagg.telemetry.merge` fold), MOC regen,
sub-shardmap rollups (leaf ShardMap JSON folded via the #294 exact coarsen
regroup), overview zarrs (issue #201; :mod:`zagg.sweep_overview`), and
optional debris collection (stubbed). Everything the sweep
writes is a **regenerable cache, never truth** (D9): deleting every rollup
leaves all leaf reads intact, and the leaf sidecars/stamps remain the durable
ground truth.

Discovery is from **run records, never a recursive LIST** (D22): callers pass
the leaves a run touched — ``(shard_key, window)`` pairs from the dispatcher's
run report (the end-of-run hook) or from the run-level stats parquets at the
store root (the manual CLI). The walk visits only the ancestor paths of those
leaves; untouched siblings contribute through their existing stored rollups
(read, never recomputed), so incremental runs accumulate exactly.

Every rollup is stamped with **generation info** — merged-leaf count + max
leaf timestamp (D22) — making staleness *detectable, not prevented*: a leaf
re-run bumps its artifact timestamp past every earlier stamp, so ancestors'
stored generations no longer match and the next sweep rewrites exactly that
chain. Skip-if-current is a **two-part test**: the generation stamp is the
fast path (a matching count + max timestamp), backstopped by a
**payload-equality** check for same-second rewrites. Both timestamp sources
resolve to whole seconds (``timespec="seconds"``), so a leaf re-run within one
wall-clock second carries an unchanged stamp; every node therefore recomputes
its merged payload BEFORE the skip decision and PUTs whenever the stored
payload differs, so a same-second content change still rewrites the whole
chain (shard nodes re-read their leaf sidecars each pass; interior nodes fold
the freshly computed child payloads, so the rewrite cascades up). A second
sweep over an unchanged tree recomputes but PUTs nothing.

Each pass also PUTs its own small run record at the store root
(``sweep_stats_{ts}.json`` — per-family durations + counts, issue #353); like
every other sweep artifact it is regenerable telemetry, never truth.

Rollup objects are JSON sidecars at digit nodes, named ``{family}.rollup.json``
— deliberately DISTINCT from the leaf sidecar names (``stats.json`` /
``coverage.moc``): under D24 mixed-order stores a node can be leaf and
interior at once, so sharing the leaf names would self-clobber; distinct names
also keep the walker's closed name set unambiguous (they list as objects,
never as digit-shaped prefixes, so the §5 discovery walk is unaffected).
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

#: Envelope version of every rollup object this module writes.
SWEEP_SPEC = "zagg-sweep/1"

#: Families swept when the caller does not choose (the four D22 families).
#: ``moc`` deliberately precedes ``overview``: the overview fold discovers
#: untouched sibling shards through the root ``coverage.moc``, which the MOC
#: family refreshes earlier in the same pass.
DEFAULT_FAMILIES = ("stats", "moc", "submap", "overview")

#: Grid types already warned about as unsupported leaf sub-maps (once-ish, so a
#: raster sweep does not warn-spam per shard). Never security-load-bearing.
_warned_unsupported_submap: set[str | None] = set()


def _warn_unsupported_submap(grid_type: str | None) -> None:
    """Warn once per grid type that a non-HEALPix leaf sub-map was skipped."""
    if grid_type in _warned_unsupported_submap:
        return
    _warned_unsupported_submap.add(grid_type)
    logger.warning(
        f"sweep[submap]: skipping non-HEALPix leaf sub-map (grid type {grid_type!r}); "
        f"the sub-shardmap fold is HEALPix-only (issue #300)"
    )


class SweepFamily:
    """One derived-artifact family (D22): how leaves read and payloads fold.

    Subclasses implement :meth:`read_leaf` (one leaf's contribution + its
    staleness timestamp) and :meth:`merge` (the associative payload fold);
    :meth:`finish` is an optional post-walk hook fed the top-level (base-node)
    artifacts — the seam the MOC family uses to refresh the store-root
    ``coverage.moc``. A family whose artifact is not a per-node JSON rollup
    (the overview family's zarrs, issue #201) instead defines ``sweep_store``
    — the whole-tree hook :func:`run_sweep` dispatches to with the manifest,
    the normalized dirty work set, and ``min_order``. That last one is
    CONTRACTUAL, not advisory (issue #377): it is the partition's split order,
    and a hook that swallows it into ``**kwargs`` writes above the split and
    breaks the disjointness invariant for every family in the pass.
    Families registered with
    ``available = False`` are visible slots that :func:`get_family` refuses
    with their ``reason``.
    """

    name = ""
    available = True
    reason = ""

    @property
    def rollup_name(self) -> str:
        """The family's per-node rollup object name."""
        return f"{self.name}.rollup.json"

    def read_leaf(self, store_root, decimal, window, spec, store_kwargs):
        """One leaf's ``(payload, timestamp)`` contribution, or ``None``.

        ``None`` means the leaf carries no artifact for this family (e.g. a
        fail-open sidecar PUT that never landed) — it is skipped, not fatal.
        ``spec`` is the manifest's store spec string, threaded so spec-keyed
        sidecar naming (the PR #307 D23 seam) resolves per store.
        """
        raise NotImplementedError

    def merge(self, payloads: list, *, node, order) -> dict:
        """Fold payloads into one (associative — rollup == direct, §8.3).

        ``node``/``order`` name the target rollup node (decimal prefix and its
        morton order) for families whose fold is order-aware (the sub-shardmap
        reproject); order-free families ignore them.
        """
        raise NotImplementedError

    def finish(self, store_root, tops, shard_order, store_kwargs) -> dict:
        """Post-walk hook over the base-node artifacts; extra summary keys."""
        return {}


class StatsFamily(SweepFamily):
    """Stats/cost rollups (D20 fold): leaf ``stats.json`` sidecars up-tree.

    The payload is a stats record as :func:`zagg.telemetry.merge` produces —
    mergeable by construction (counts/sums/min-max, never stored means), so
    interior payloads re-merge exactly and the rollup at any node equals the
    direct fold of every leaf record beneath it.
    """

    name = "stats"

    def read_leaf(self, store_root, decimal, window, spec, store_kwargs):
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path
        from zagg.telemetry import read_sidecar

        leaf = shard_leaf_path(store_root, morton_word(decimal), window=window)
        record = read_sidecar(leaf, spec, **store_kwargs)
        if record is None:
            return None
        return record, record.get("timestamp")

    def merge(self, payloads: list, *, node, order) -> dict:
        from zagg.telemetry import merge

        return merge(payloads)


class MocFamily(SweepFamily):
    """MOC regen (D8/D9): committed-leaf coverage folded up-tree + root refresh.

    A leaf contributes iff its D4 commit stamp is present (unstamped debris is
    invisible, exactly as the walk treats it); the staleness timestamp is the
    stamp's ``written_at``. Payloads are shard-order ranges bodies (the root
    ``coverage.moc``'s O1 encoding, minus its carrier fields) plus the D15
    time union, so the fold is a word union and the base-node artifacts
    compose directly into the store root. :meth:`finish` refreshes the root
    ``coverage.moc`` from those base folds via the GET-union-PUT writer —
    the sweep is the REGENERATOR; the runner's end-of-run fail-open write
    stays the fast path (O7/D9) — skipping the PUT when the existing root
    already covers the folded words and time range (sweep idempotence). The
    O8 in-leaf bitmap contract is untouched: this family reads only the
    stamp envelope, never the cell-order bitmap sidecar.
    """

    name = "moc"

    def read_leaf(self, store_root, decimal, window, spec, store_kwargs):
        # ``spec`` is unused here: leaf PATHS are the frozen /1-/2 grammar
        # (shard_leaf_path); the D23 /3 leaf naming has no writer yet, and
        # adopting it is the issue #299 flip, which lands in shard_leaf_path.
        from zagg.grids.morton import morton_word
        from zagg.hive import read_commit, shard_leaf_path
        from zagg.store import open_store

        leaf = shard_leaf_path(store_root, morton_word(decimal), window=window)
        stamp = read_commit(open_store(leaf, **store_kwargs))
        if stamp is None:
            return None  # absent leaf or unstamped debris (D4)
        payload = _moc_payload([morton_word(decimal)], stamp.get("time_range"))
        return payload, stamp.get("written_at")

    def merge(self, payloads: list, *, node, order) -> dict:
        import numpy as np

        from zagg.hive import root_coverage_words
        from zagg.windows import union_time_range

        words = np.unique(np.concatenate([root_coverage_words(p) for p in payloads]))
        return _moc_payload(words, union_time_range(*(p.get("time_range") for p in payloads)))

    def finish(self, store_root, tops, shard_order, store_kwargs) -> dict:
        """Refresh the store-root ``coverage.moc`` from the base-node folds.

        Unions with the existing root object (the sweep may cover only the
        dirty subtrees — untouched bases must keep their listing), via the
        same :func:`zagg.hive.write_root_coverage` transport the runner uses.
        No PUT when the existing root already lists every folded word and
        covers the folded time range, so an unchanged tree re-sweep is a
        no-op here too.
        """
        import numpy as np

        from zagg.hive import (
            build_root_coverage,
            read_root_coverage,
            root_coverage_words,
            write_root_coverage,
        )
        from zagg.windows import union_time_range

        if not tops:
            return {"root_moc_written": False}
        words = np.unique(np.concatenate([root_coverage_words(t["payload"]) for t in tops]))
        time_range = union_time_range(*(t["payload"].get("time_range") for t in tops))
        try:
            existing = read_root_coverage(store_root, **store_kwargs)
        except ValueError:
            existing = None  # unparsable root -> regenerate (D9)
        if isinstance(existing, dict):
            try:
                covered = bool(np.isin(words, root_coverage_words(existing)).all())
                covered = covered and (
                    union_time_range(existing.get("time_range"), time_range)
                    == existing.get("time_range")
                )
                if covered:
                    return {"root_moc_written": False}
            except (KeyError, TypeError, ValueError):
                pass  # malformed cache cannot vouch for coverage -> rewrite
        write_root_coverage(
            store_root,
            build_root_coverage(words, shard_order, source="sweep", time_range=time_range),
            **store_kwargs,
        )
        return {"root_moc_written": True}


def _moc_payload(words, time_range) -> dict:
    """A rollup's shard-order ranges body (deterministic — no carrier fields).

    Reuses :func:`zagg.hive.build_root_coverage` for the O1 range encoding and
    drops ``source``/``generated_at`` (they would defeat skip-if-current
    byte stability) and ``spec`` (the rollup rides the ``zagg-sweep/1``
    envelope; the root object written by :meth:`MocFamily.finish` carries the
    full ``morton-moc/1`` carrier as usual). ``time_range`` is normalized
    through the D15 union so leaf and merged payloads compare equal.
    """
    from zagg.grids.morton import morton_decimal
    from zagg.hive import _decimal_order, build_root_coverage
    from zagg.windows import union_time_range

    order = _decimal_order(morton_decimal(int(words[0])))
    envelope = build_root_coverage(words, order, time_range=union_time_range(time_range))
    payload = {"encoding": "ranges", "order": envelope["order"], "ranges": envelope["ranges"]}
    if "time_range" in envelope:
        payload["time_range"] = envelope["time_range"]
    return payload


#: Leaf sub-map object name (bare leaves); windowed and ``/3`` leaves derive
#: their names through :func:`submap_key`, single-sourced on the stats-sidecar
#: naming seam.
SUBMAP_NAME = "shardmap.json"


def submap_key(leaf_name: str, spec: str | None = None) -> str:
    """Sub-map object name for a leaf zarr basename, keyed by store spec.

    Single-sourced on the PR #307 sidecar-naming seam
    (:func:`zagg.telemetry.sidecar_key`) with the ``shardmap`` stem swapped in:
    legacy stores get ``shardmap.json`` / ``shardmap_{window}.json``,
    ``morton-hive/3`` stores get ``{window}.shardmap.json`` — so the issue
    #299 writer flip renames both sidecars through one seam. Raises on an
    unknown spec, exactly as the seam does.
    """
    from zagg.telemetry import sidecar_key

    key = sidecar_key(leaf_name, spec)
    if key.endswith(".stats.json"):  # D23 /3 grammar: {stem}.stats.json
        return key.removesuffix(".stats.json") + ".shardmap.json"
    return "shardmap" + key.removeprefix("stats")  # legacy: stats[_{window}].json


def write_leaf_submap(
    store_root: str,
    shard_key,
    granules,
    *,
    grid_signature: dict,
    metadata: dict | None = None,
    window: str | None = None,
    spec: str | None = None,
    store_kwargs: dict | None = None,
) -> None:
    """PUT one leaf's sub-map — full ShardMap JSON (D22, ratified) — at its prefix.

    The payload is a one-shard :class:`~zagg.catalog.shardmap.ShardMap` in the
    standard JSON schema (``metadata``/``grid_signature``/``shard_keys``/
    ``granules``) plus a top-level ``written_at`` staleness stamp — extra keys
    are ignored by ``ShardMap.from_json``, so the object stays loadable as-is.
    ``granules`` are the unit's ShardMap entries, copied verbatim (windowed
    units carry their window's subset, so the shard-node fold's window union
    reassembles the shard). ``metadata`` is the run catalog's, with the
    whole-catalog counts and build fields rewritten to describe this sub-map
    (the same fields ``reproject`` strips from derived maps). Written by the
    worker on success, sibling to the stats sidecar — call sites fail open.
    """
    import obstore

    from zagg.hive import _utcnow, shard_leaf_path
    from zagg.store import open_object_store

    entries = [dict(g) for g in granules]
    meta = dict(metadata or {})
    # Run-wide fields that must not be republished per leaf: build provenance
    # (rewritten below or meaningless here) and ``pairless`` — the sibling
    # join's per-granule exclusion report (issue #425), which is unbounded in
    # the catalog size and belongs to the RUN's map, not to one leaf's.
    for stale in ("aoi_mask", "build_wall_s", "reproject", "pairless"):
        meta.pop(stale, None)
    meta.update(
        total_shards=1,
        total_granules=len(entries),
        total_pairs=len(entries),
        # Whole-catalog count from the build metadata: rewritten per leaf like
        # its siblings above, or a leaf object would publish the run total.
        granules_assigned=len({g["id"] for g in entries}),
    )
    payload = {
        "metadata": meta,
        "grid_signature": dict(grid_signature),
        "shard_keys": [int(shard_key)],
        "granules": [entries],
        "written_at": _utcnow(),
    }
    leaf = shard_leaf_path(store_root, int(shard_key), window=window)
    prefix, _, name = leaf.rstrip("/").rpartition("/")
    obstore.put(
        open_object_store(prefix, **(store_kwargs or {})),
        submap_key(name, spec),
        json.dumps(payload, indent=1).encode(),
    )


def submap_emittable(grid_signature: dict | None, granules) -> bool:
    """Whether a unit's leaf sub-map can be folded by :class:`SubmapFamily`.

    The sub-shardmap fold is HEALPix-morton only — ``reproject`` coarsens by
    ``parent_order``/``child_order`` and the merge keys entries by granule
    ``id``. A rectilinear raster signature (grid ``type != "healpix"``, no
    ``parent_order``/``child_order``) or id-less granule entries would fold to
    a ``failed`` node the sweep can never consume, so the emission sites (issue
    #300) check this and skip the write instead of persisting an unmergeable
    payload. ``store_layout: hive`` is HEALPix-only by validation, so the
    realistic skip is id-less raster entries under a rectilinear signature.
    """
    sig = grid_signature or {}
    if sig.get("type") != "healpix" or "parent_order" not in sig or "child_order" not in sig:
        return False
    return all("id" in g for g in granules)


class SubmapFamily(SweepFamily):
    """Sub-shardmap rollups (D22): leaf ShardMap JSON folded via ``reproject``.

    Leaf artifact: the full-ShardMap-JSON sub-map the worker writes next to
    the leaf (:func:`write_leaf_submap`). Shard-node fold: the windows' entry
    lists union, deduplicated by granule ``id`` (a granule spanning two
    windows counts once). Interior fold: children sub-maps concatenate and
    coarsen to the node's order via :meth:`ShardMap.reproject` — the #294
    exact pure regroup (granule union deduped by id), now over stored
    artifacts — so the rollup at order N equals a direct reproject of the
    leaf-level map to N (§8.3). Every rollup's ``payload`` is a plain
    ShardMap JSON dict (the sweep envelope wraps it, as for every family).
    """

    name = "submap"

    def read_leaf(self, store_root, decimal, window, spec, store_kwargs):
        import obstore
        from obstore.exceptions import NotFoundError

        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path
        from zagg.store import open_object_store

        leaf = shard_leaf_path(store_root, morton_word(decimal), window=window)
        prefix, _, name = leaf.rstrip("/").rpartition("/")
        try:
            data = obstore.get(
                open_object_store(prefix, **store_kwargs), submap_key(name, spec)
            ).bytes()
        except (FileNotFoundError, NotFoundError):
            return None
        sub = json.loads(bytes(data))
        ok = isinstance(sub, dict) and all(
            k in sub for k in ("grid_signature", "shard_keys", "granules")
        )
        if not ok:
            # Corrupt (missing required keys): a present-but-malformed sub-map
            # means a broken writer; the engine catches per leaf and counts it
            # failed — the loud, not-absent signal.
            raise ValueError(f"malformed leaf sub-map next to {leaf}")
        sig = sub["grid_signature"] or {}
        if sig.get("type") != "healpix" or "parent_order" not in sig or "child_order" not in sig:
            # Unsupported (not corrupt): the sub-shardmap fold is HEALPix-only
            # (reproject coarsens by parent/child order). A non-HEALPix leaf —
            # e.g. a rectilinear raster sub-map slipped past the emission guard
            # (submap_emittable) — is a SKIP, not a broken writer: return None so
            # the engine counts it "empty", never "failed" (data-corruption).
            _warn_unsupported_submap(sig.get("type"))
            return None
        timestamp = sub.pop("written_at", None)
        return sub, timestamp

    def merge(self, payloads: list, *, node, order) -> dict:
        """Fold child sub-maps into this node's rollup (§8.3).

        Identity metadata (``collection``/``short_name``/``version``/
        ``footprint``) is inherited from the first child payload
        (``payloads[0]``); only ``total_granules`` is recomputed. The
        one-store-per-product invariant (D7/D19: one product tree per semantic
        core, one catalog family) makes those fields uniform across a node's
        children by construction, so the first child is representative. A store
        that accreted leaves from more than one catalog under one root would
        need identity reconciliation this fold does not attempt (the rollup
        would advertise one arbitrary child's identity). ``grid_signature`` is
        safe regardless — all children of a node share ``child_order``.
        """
        from zagg.catalog.shardmap import (
            ShardMap,
            _recorded_identity,
            _refuse_basename_collisions,
        )

        # Same-key union first (several windows of one shard), deduplicated by
        # granule id — the same rule reproject's coarsen applies across shards.
        # Keyed like coarsen's, on everything that distinguishes one granule from
        # another rather than on the id alone (issue #468): two granules whose
        # recorded identity collapses must both survive the union to be seen,
        # not have one silently overwrite the other here.
        buckets: dict[int, dict] = {}
        for p in payloads:
            for key, entries in zip(p["shard_keys"], p["granules"]):
                bucket = buckets.setdefault(int(key), {})
                for entry in entries:
                    # read_leaf already filters non-HEALPix/id-less leaves to the
                    # "empty" skip path, so an id-less entry reaching the fold is a
                    # genuinely broken artifact — name the field cleanly ("failed"),
                    # never a bare KeyError.
                    if "id" not in entry:
                        raise ValueError("leaf sub-map granule entry missing required 'id' field")
                    entry = dict(entry)
                    bucket[_recorded_identity(entry)[1]] = entry
        keys = sorted(buckets)
        granules = [list(buckets[k].values()) for k in keys]
        signature = dict(payloads[0]["grid_signature"])
        meta = dict(payloads[0].get("metadata") or {})
        meta.pop("reproject", None)  # never inherit a child fold's stamp
        meta["total_granules"] = len({e["id"] for g in granules for e in g})
        # On a folded map the distinct-union IS the assigned set; keep the two
        # keys agreeing rather than inheriting a stale build-side value.
        meta["granules_assigned"] = meta["total_granules"]
        folded = ShardMap(signature, keys, granules, meta)
        # The union above mints shard membership just as build and reproject do,
        # so it owns the same identity invariant (issue #468). Checked here for
        # BOTH arms: the interior arm inherits the check from reproject anyway,
        # and a shard-node fold that stayed silent while its parent refused
        # would be incoherent. Note this reaches ``_merged``'s fail-open handler
        # as a skipped rollup node -- see the PR's questions for review.
        _refuse_basename_collisions(keys, granules)
        if int(signature["parent_order"]) == int(order):
            # Shard-node fold: already at the node's order — a window union,
            # not a reprojection; keep counts honest without a noop stamp.
            meta.update(total_shards=len(keys), total_pairs=sum(len(g) for g in granules))
        else:
            # Interior fold: coarsen the children to this node's order. The
            # resulting metadata["reproject"]["source_parent_order"] records the
            # immediately-lower fold level (payloads[0]'s parent_order, one order
            # below this node) — the LAST hop, not the leaf/shard origin order.
            # Coarsen is transitive, so the folded data equals a direct reproject
            # of the leaf-level map straight to this order (§8.3,
            # test_rollup_equals_direct_reproject); the stamp naming the last hop
            # is by design, not a data discrepancy.
            folded = folded.reproject(_ReprojectTarget(signature, order))
        return {
            "metadata": folded.metadata,
            "grid_signature": folded.grid_signature,
            "shard_keys": [int(k) for k in folded.shard_keys],
            "granules": folded.granules,
        }


class _ReprojectTarget:
    """Minimal coarsen-target shim for :meth:`ShardMap.reproject`.

    ``reproject`` consumes exactly ``parent_order``/``child_order`` and
    ``spatial_signature()``. The sweep has no run config to build a full
    :class:`~zagg.grids.healpix.HealpixGrid` from, and the target signature IS
    the source's with the shard order swapped — reproject changes only the
    dispatch order, never the leaf DGGS resolution.
    """

    def __init__(self, signature: dict, parent_order: int):
        self._signature = {**signature, "parent_order": int(parent_order)}
        self.parent_order = int(parent_order)
        self.child_order = int(signature["child_order"])

    def spatial_signature(self) -> dict:
        return dict(self._signature)


class OverviewFamily(SweepFamily):
    """Overview zarrs at ancestor nodes (D11/D22/D24 — issue #201).

    Unlike the JSON-rollup families, the artifact is a ZARR at manifest-
    declared orders only, folded from leaf DATA (per-field D24 merge laws),
    so this family overrides :attr:`sweep_store` — the whole-tree hook the
    engine dispatches to instead of the generic bottom-up JSON fold. The
    implementation lives in :mod:`zagg.sweep_overview`.
    """

    name = "overview"

    def sweep_store(self, store_root, manifest, by_shard, store_kwargs, min_order=0) -> dict:
        from zagg.sweep_overview import sweep_overviews

        return sweep_overviews(
            store_root, manifest, by_shard, store_kwargs=store_kwargs, min_order=min_order
        )


class DebrisFamily(SweepFamily):
    """Debris collection (D22 optional audit-class fifth family) — stub."""

    name = "debris"
    available = False
    reason = (
        "debris collection (deleting unstamped .zarr/ prefixes past a "
        "declared horizon, D22's optional fifth family) is not implemented"
    )


#: Family registry (D22's plug-in point): name -> class. Unavailable entries
#: are visible slots that :func:`get_family` refuses with their ``reason``.
FAMILIES: dict[str, type[SweepFamily]] = {
    cls.name: cls for cls in (StatsFamily, MocFamily, SubmapFamily, OverviewFamily, DebrisFamily)
}


def get_family(name: str) -> SweepFamily:
    """Instantiate a registered family; loud on unknown or stubbed names."""
    cls = FAMILIES.get(name)
    if cls is None:
        raise ValueError(f"unknown sweep family {name!r}; registered: {sorted(FAMILIES)}")
    if not cls.available:
        raise NotImplementedError(f"sweep family {name!r} is registered but stubbed: {cls.reason}")
    return cls()


def run_sweep(
    store_root: str,
    leaves,
    *,
    families=None,
    store_kwargs: dict | None = None,
    record: bool = True,
    partition=None,
) -> dict:
    """One sweep pass: fold leaf artifacts up-tree for each family (D22).

    ``leaves`` is the run-record-derived work set — an iterable of
    ``(shard_key, window)`` pairs (or bare shard keys, meaning unwindowed):
    the leaves whose ancestors may be stale. The walk visits ONLY those
    ancestor paths; siblings contribute via their stored rollups. Leaves at a
    non-manifest order are skipped with a warning (mixed-order stores are
    unsupported this round, matching ``refresh_root_coverage``).

    Idempotent: a rollup whose stored generation (merged-leaf count + max
    leaf timestamp) AND stored payload both match the freshly computed ones is
    left untouched, so a second pass over an unchanged tree writes nothing; the
    payload compare is the same-second backstop the generation stamp cannot see
    (module docstring). Returns a summary with
    per-family ``written`` / ``current`` (skip-if-current) / ``empty`` (no
    artifact found) / ``failed`` (unmergeable, logged) counts, each carrying
    its wall-clock ``duration_s`` (issue #353) plus a pass total — the sweep's
    analogue of the workers' ``phase_timings``, so benchmark runs are not
    limited to billed-duration inference.

    ``partition`` (issue #377) restricts the pass to ONE ``2^n`` morton-subtree
    partition — the worker event's ``{"index", "of"}`` block. Three things
    follow, and together they are what makes concurrent partitions safe: the
    work set is filtered to the partition's subtrees HERE rather than trusted
    from the dispatcher (the ``discover`` transport derives the whole store's
    work set worker-side); the bottom-up walk stops at the split order; and the
    post-walk :meth:`SweepFamily.finish` hook is deferred. Nodes above the
    split and the store-root ``coverage.moc`` span partitions — they belong to
    the coarse-level finisher, so no two partitions can write the same
    ``(node, window)`` artifact. ``of=1`` is the identity partition, byte-
    identical to an unpartitioned pass. See :mod:`zagg.sweep_partition`.
    Every summary field is then partition-scoped EXCEPT ``skipped_leaves``,
    which is derived before the filter and stays store-wide.

    Unless ``record=False``, the summary is also PUT at the store root as the
    sweep's own run record (:func:`_write_sweep_record`, fail-open).
    """
    import time

    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.store import open_object_store
    from zagg.sweep_partition import normalize_partition, partition_split_order, select_partition

    t0 = time.perf_counter()
    store_kwargs = dict(store_kwargs or {})
    part = normalize_partition(partition)
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    shard_order = int(manifest["shard_order"])
    split = 0 if part is None else partition_split_order(part[1])
    if split > shard_order:
        raise ValueError(
            f"sweep partitions={part[1]} splits at order {split}, finer than the store's "
            f"shard_order {shard_order}; no leaf lies below the split (issue #377)"
        )
    fams = [get_family(n) for n in (DEFAULT_FAMILIES if families is None else families)]
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    foreign = 0
    if part is not None:
        by_shard, foreign = select_partition(by_shard, part[1], part[0])
    store = open_object_store(store_root, **store_kwargs)
    summary: dict = {
        "store_root": store_root,
        "shard_order": shard_order,
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "families": {},
    }
    if part is not None:
        summary["partition"] = {"index": part[0], "of": part[1], "split_order": split}
        summary["foreign_leaves"] = foreign
    for fam in fams:
        fam_t0 = time.perf_counter()
        runner = getattr(fam, "sweep_store", None)
        if runner is not None:
            result = runner(store_root, manifest, by_shard, store_kwargs, min_order=split)
        else:
            result = _sweep_family(
                store_root,
                store,
                fam,
                by_shard,
                shard_order,
                manifest.get("spec"),
                store_kwargs,
                split,
            )
        result["duration_s"] = time.perf_counter() - fam_t0
        summary["families"][fam.name] = result
    summary["duration_s"] = time.perf_counter() - t0
    if record:
        summary["record"] = _write_sweep_record(store, summary)
    return summary


def _write_sweep_record(store, summary: dict) -> str | None:
    """PUT the sweep's run record at the store root; its key, or ``None`` (#353).

    The return is deliberately the bare store-root-relative key, NOT the joined
    ``{store_root}/{key}`` that ``telemetry.write_run_parquet`` hands back: it
    is the same currency as every other key this sweep writes (the rollups), so
    it is what you hand straight back to ``obstore`` against this same store.
    Joining would mean plumbing ``store_root`` in here, where only the store
    handle is held. Callers holding the response body alone re-join it with the
    ``store_path`` they invoked with.

    One small JSON object per sweep pass — ``sweep_stats_{ts}.json``,
    timestamp-first like the run parquets (D20) so a lexicographic listing is
    chronological. A PARTITIONED pass (issue #377) appends ``_p{index}of{of}``:
    the ``2^n`` invokes of one fan-out fire in the same second and would
    otherwise clobber each other's record, and the per-partition rows are
    exactly what the issue #354 timing record is asked to grow — one object
    per partition, each carrying its own ``partition`` block and durations.
    Both forms are deliberately OUTSIDE the ``stats_*.parquet`` glob the
    sweep's own run-record discovery scans (a sweep record must never read as
    a shard run record). A second pass within the same wall-clock second
    overwrites the first — acceptable for telemetry, which this is: fail-open
    (one warning, ``None``), never truth, exactly like every other sweep
    artifact (D9).
    """
    from datetime import datetime, timezone

    import obstore

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    part = summary.get("partition")
    tag = "" if part is None else f"_p{part['index']}of{part['of']}"
    key = f"sweep_stats_{ts}{tag}.json"
    try:
        obstore.put(store, key, json.dumps({"spec": SWEEP_SPEC, **summary}, indent=1).encode())
    except Exception as e:
        logger.warning(f"sweep: run record write failed (fail-open, D9 — telemetry): {e}")
        return None
    return key


def _normalize_leaves(leaves, shard_order: int):
    """``{shard_decimal: {window, ...}}`` from run-record leaf refs."""
    from zagg.grids.morton import morton_decimal
    from zagg.hive import _decimal_order

    by_shard: dict[str, set] = {}
    skipped: list[str] = []
    for ref in leaves:
        key, window = ref if isinstance(ref, (tuple, list)) else (ref, None)
        decimal = morton_decimal(int(key))
        if _decimal_order(decimal) != shard_order:
            logger.warning(
                f"sweep: skipping leaf {decimal} at order {_decimal_order(decimal)} under a "
                f"shard_order-{shard_order} manifest (mixed-order stores are unsupported)"
            )
            skipped.append(decimal)
            continue
        by_shard.setdefault(decimal, set()).add(None if window is None else str(window))
    return by_shard, skipped


def _sweep_family(
    store_root, store, fam, by_shard, shard_order, spec, store_kwargs, min_order=0
) -> dict:
    """Bottom-up fold of one family over the dirty ancestor paths.

    ``min_order`` (issue #377) is the partition's split order: the walk writes
    no node above it and the post-walk ``finish`` hook is deferred, because
    both span partitions. Zero — an unpartitioned pass — is the whole tree.
    """
    counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
    computed: dict[str, dict | None] = {}
    for decimal in sorted(by_shard):
        computed[decimal] = _rollup_shard_node(
            store_root,
            store,
            fam,
            decimal,
            by_shard[decimal],
            shard_order,
            spec,
            store_kwargs,
            counts,
        )
    frontier = [d for d in sorted(by_shard) if computed[d] is not None]
    for _order in range(shard_order - 1, min_order - 1, -1):
        parents = sorted({a for d in frontier if (a := _ancestor(d)) is not None})
        frontier = []
        for node in parents:
            computed[node] = _rollup_interior(store, fam, node, computed, counts)
            if computed[node] is not None:
                frontier.append(node)
        if not frontier:
            break
    tops = [computed[d] for d in frontier]
    result = dict(counts)
    if min_order:
        # Deferred in ORDERS as well as by name, so the finisher's obligation
        # is machine-readable and sized: split == shard_order is permitted,
        # and then every node above the leaves is owed, not just the base ones.
        result["finish_deferred"] = True  # spans partitions -> the finisher's
        result["deferred_orders"] = list(range(min_order))
    else:
        result.update(fam.finish(store_root, tops, shard_order, store_kwargs))
    return result


def _rollup_shard_node(
    store_root, store, fam, decimal, windows, shard_order, spec, store_kwargs, counts
) -> dict | None:
    """Fold one shard node's window-leaf artifacts into its rollup.

    The window set is the union of the run's dirty windows and the windows the
    existing rollup already merged (recorded in its ``windows`` key), so an
    append run that touches one window never drops its siblings.
    """
    existing = _read_rollup(store, fam, decimal)
    known = set(windows)
    if existing is not None:
        known |= {None if w is None else str(w) for w in existing.get("windows") or []}
    parts = []
    for window in sorted(known, key=lambda w: (w is not None, w or "")):
        try:
            got = fam.read_leaf(store_root, decimal, window, spec, store_kwargs)
        except Exception as e:
            logger.warning(
                f"sweep[{fam.name}]: leaf read failed at {decimal} window {window!r}; "
                f"skipping that leaf ({e})"
            )
            counts["failed"] += 1
            got = None
        if got is not None:
            parts.append((window, *got))
    if not parts:
        counts["empty"] += 1
        return None
    generation = _generation(len(parts), [ts for _w, _p, ts in parts])
    payload = _merged(fam, [p for _w, p, _ts in parts], decimal, shard_order, counts)
    if payload is None:
        return None
    if (
        existing is not None
        and existing.get("generation") == generation
        and existing.get("payload") == payload
    ):
        counts["current"] += 1
        return existing
    envelope = {
        "spec": SWEEP_SPEC,
        "family": fam.name,
        "node": decimal,
        "order": shard_order,
        "generation": generation,
        "windows": [w for w, _p, _ts in parts],
        "payload": payload,
    }
    _put_rollup(store, fam, decimal, envelope)
    counts["written"] += 1
    return envelope


def _rollup_interior(store, fam, node, computed, counts) -> dict | None:
    """Fold a digit node's four candidate children rollups into its own.

    A child freshly computed this pass is used in memory; any other candidate
    is probed on the store (<= 4 GETs, no LIST) so prior runs' siblings keep
    contributing. Generation is the children's sum/max — fold-of-folds equals
    the direct leaf fold because every family's merge is associative (§8.3).

    The store is append-only at the leaf level (leaf deletion/GC is the
    registered debris family, deliberately stubbed), so a child that emptied
    this pass returns ``None`` from its shard rollup and this fallback picks up
    its prior on-store rollup — an emptied/vanished leaf keeps contributing to
    its parent until a deletion-aware family exists. Under append-only + D9
    (rollups are regenerable caches) that is intended, not stale-serving.
    """
    from zagg.hive import _decimal_order

    children = []
    for digit in "1234":
        art = computed.get(node + digit)
        if art is None:
            art = _read_rollup(store, fam, node + digit)
        if art is not None:
            children.append(art)
    if not children:
        counts["empty"] += 1
        return None
    generation = _generation(
        sum(int(a["generation"]["n_leaves"]) for a in children),
        [a["generation"].get("max_leaf_timestamp") for a in children],
    )
    existing = _read_rollup(store, fam, node)
    payload = _merged(fam, [a["payload"] for a in children], node, _decimal_order(node), counts)
    if payload is None:
        return None
    if (
        existing is not None
        and existing.get("generation") == generation
        and existing.get("payload") == payload
    ):
        counts["current"] += 1
        return existing
    envelope = {
        "spec": SWEEP_SPEC,
        "family": fam.name,
        "node": node,
        "order": _decimal_order(node),
        "generation": generation,
        "payload": payload,
    }
    _put_rollup(store, fam, node, envelope)
    counts["written"] += 1
    return envelope


def _merged(fam, payloads, node, order, counts) -> dict | None:
    """The family fold, fail-open per node: unmergeable -> logged + skipped."""
    try:
        return fam.merge(payloads, node=node, order=order)
    except Exception as e:
        logger.warning(f"sweep[{fam.name}]: merge failed at node {node}; skipping ({e})")
        counts["failed"] += 1
        return None


def _generation(n_leaves: int, timestamps) -> dict:
    """The D22 generation stamp: merged-leaf count + max leaf timestamp."""
    stamps = [t for t in timestamps if t is not None]
    return {"n_leaves": int(n_leaves), "max_leaf_timestamp": max(stamps) if stamps else None}


def _ancestor(decimal: str) -> str | None:
    """The parent prefix of a node decimal (``None`` at a base component)."""
    from zagg.hive import _decimal_base

    return None if decimal == _decimal_base(decimal) else decimal[:-1]


def _node_rel(decimal: str) -> str:
    """A node decimal's relative digit path (``-311`` -> ``-3/1/1``)."""
    from zagg.hive import _decimal_base

    base = _decimal_base(decimal)
    return "/".join([base, *decimal[len(base) :]])


def _rollup_key(fam, decimal: str) -> str:
    return f"{_node_rel(decimal)}/{fam.rollup_name}"


def _read_rollup(store, fam, decimal: str) -> dict | None:
    """A node's stored rollup envelope, or ``None`` — strict, cache posture.

    Missing, unparsable, wrong-spec/family, or stamp-less objects all read as
    absent (debug-logged): a corrupt rollup is a regenerable cache (D9) and is
    simply rebuilt, never half-trusted.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(store, _rollup_key(fam, decimal)).bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    try:
        envelope = json.loads(bytes(data))
    except ValueError:
        envelope = None
    generation = envelope.get("generation") if isinstance(envelope, dict) else None
    usable = (
        isinstance(envelope, dict)
        and envelope.get("spec") == SWEEP_SPEC
        and envelope.get("family") == fam.name
        and isinstance(generation, dict)
        and isinstance(generation.get("n_leaves"), int)
        and "payload" in envelope
    )
    if not usable:
        logger.debug(
            f"sweep[{fam.name}]: unusable rollup at node {decimal}; ignoring "
            f"(regenerable cache, D9)"
        )
        return None
    return envelope


def _put_rollup(store, fam, decimal: str, envelope: dict) -> None:
    import obstore

    obstore.put(store, _rollup_key(fam, decimal), json.dumps(envelope, indent=1).encode())


# ---------------------------------------------------------------------------
# Trigger surfaces (issue #300 phases 4-5): run-record discovery, the manual
# CLI, and the end-of-run hook wrapper.
# ---------------------------------------------------------------------------


def sweep_after_run(
    store_root: str, leaves, *, families=None, store_kwargs: dict | None = None
) -> dict | None:
    """End-of-run hook: fail-open wrapper around :func:`run_sweep` (D22).

    Off the critical path by contract: any failure — missing manifest, no
    store write access, a family blowing up — logs one warning and returns
    ``None``; the run result is untouched. Rollups are regenerable caches
    (D9), so a skipped sweep costs one later CLI pass, never a wrong answer.
    Called in-process by the LOCAL dispatchers only (they are the workers and
    hold the user's store credentials); the Lambda dispatchers never PUT (the
    D8 standing rule) and post a fire-and-forget ``mode="sweep"`` worker
    Event invoke instead, whose handler calls :func:`run_sweep` directly.
    """
    try:
        summary = run_sweep(store_root, leaves, families=families, store_kwargs=store_kwargs)
        logger.info(f"Post-run sweep: {summary['families']}")
        return summary
    except Exception as e:
        logger.warning(f"post-run sweep failed (fail-open, D9/D22 — rollups are caches): {e}")
        return None


def leaves_from_stats_records(records) -> list:
    """``(shard_key, window)`` work-set pairs from per-shard stats records.

    The dispatcher-side bridge from a run report to the sweep: every
    successful unit's record (envelope- or meta-ridden) names its leaf via
    ``shard_key`` + the issue #300 ``window`` field. Records without a window
    key (older workers) map to the unwindowed leaf name — on a windowed store
    that read simply misses (fail-open; the CLI backstops). Failure and
    ``None`` records are skipped; pairs are deduplicated and sorted.
    """
    refs = {
        (int(r["shard_key"]), r.get("window"))
        for r in records
        if r and r.get("success") and r.get("shard_key") is not None
    }
    return sorted(refs, key=lambda p: (p[0], p[1] is not None, p[1] or ""))


def discover_leaves(store_root: str, *, store_kwargs: dict | None = None) -> list:
    """Leaf refs from the run-record parquets at the product root (D22).

    One shallow delimiter LIST of the product root finds the
    ``stats_*.parquet`` run records (both the timestamp-first D20 names and
    the older ``stats_{run_id}_{ts}`` form); their success rows give the work
    set — discovery is from run records, never a tree enumeration. Windowed
    leaves resolve through the records' ``window`` column; every shard that
    contributed a pre-column (window-less) row on a windowed store falls back
    to one delimiter LIST of that shard's node — regardless of whether some
    other record already named one of its windows, since the LIST + set dedup
    is idempotent and a mixed-deployment store can hold a window only the
    sidecar knows about (bounded by the run-record shard set). Rows whose
    ``shard_key`` came back float-typed (pre-fix parquets mixing keys with
    failure-row nulls) are skipped past 2^53 with a warning — those keys are
    inexact by construction; :func:`zagg.telemetry.write_run_parquet` now
    writes the column nullable-UInt64 so new records round-trip exactly.
    Returns sorted, deduplicated ``(shard_key, window)`` pairs.
    """
    import re
    import tempfile

    import obstore

    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.store import open_object_store

    store_kwargs = dict(store_kwargs or {})
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    spec = manifest.get("spec")
    windowed = manifest.get("temporal") is not None
    store = open_object_store(store_root, **store_kwargs)
    listing = obstore.list_with_delimiter(store)
    names = sorted(
        o["path"].rsplit("/", 1)[-1]
        for o in listing["objects"]
        if re.fullmatch(r"stats_.+\.parquet", o["path"].rsplit("/", 1)[-1])
    )
    refs: set = set()
    fallback_keys: set[int] = set()
    warned_float = False
    for name in names:
        import pandas as pd

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(bytes(obstore.get(store, name).bytes()))
            tmp.flush()
            try:
                df = pd.read_parquet(tmp.name, engine="fastparquet")
            except Exception as e:
                logger.warning(f"sweep discovery: unreadable run record {name}; skipping ({e})")
                continue
        if "shard_key" not in df.columns:
            logger.warning(f"sweep discovery: run record {name} has no shard_key; skipping")
            continue
        ok = df[df["shard_key"].notna() & df.get("success", True)]
        has_window = "window" in df.columns
        for _idx, row in ok.iterrows():
            key = row["shard_key"]
            if isinstance(key, float):
                if key != int(key) or key >= 2**53:
                    if not warned_float:
                        warned_float = True
                        logger.warning(
                            f"sweep discovery: {name} stores shard_key as float64 (a "
                            f"pre-fix run record); keys past 2^53 are inexact and are "
                            f"skipped — re-run those shards or sweep them by hand"
                        )
                    continue
            key = int(key)
            if has_window:
                window = row["window"]
                refs.add((key, None if window is None or pd.isna(window) else str(window)))
            elif windowed:
                fallback_keys.add(key)  # pre-column record: resolve below
            else:
                refs.add((key, None))
    # Pre-``window``-column records on a windowed store: the row can't name
    # its leaf, so resolve each shard's windows with ONE delimiter LIST of its
    # node — scoped by the run-record shard set, never a tree walk. Every
    # pre-column shard is LISTed even if another record already named one of
    # its windows: the LIST + set dedup is idempotent, and on a mixed
    # deployment the sidecar can hold a window no post-column record names.
    from zagg.grids.morton import morton_decimal

    for key in sorted(fallback_keys):
        node_listing = obstore.list_with_delimiter(store, _node_rel(morton_decimal(key)) + "/")
        for obj in node_listing["objects"]:
            window = _sidecar_window(obj["path"].rsplit("/", 1)[-1], spec)
            if window is not _NO_SIDECAR:
                refs.add((key, window))
    return sorted(refs, key=lambda r: (r[0], r[1] is not None, r[1] or ""))


#: Sentinel: "this object is not a stats sidecar" (``None`` means unwindowed).
_NO_SIDECAR = object()


def _sidecar_window(name: str, spec: str | None):
    """The window label a stats-sidecar object name encodes, else the sentinel."""
    from zagg.telemetry import SIDECAR_NAME, SPEC_V3
    from zagg.windows import validate_label

    try:
        if spec == SPEC_V3:
            if not name.endswith(".stats.json"):
                return _NO_SIDECAR
            stem = name.removesuffix(".stats.json")
            validate_label(stem)
            return stem
        if name == SIDECAR_NAME:
            return None
        stem, ext = SIDECAR_NAME.rsplit(".", 1)
        if name.startswith(f"{stem}_") and name.endswith(f".{ext}"):
            window = name[len(stem) + 1 : -(len(ext) + 1)]
            validate_label(window)
            return window
    except ValueError:
        return _NO_SIDECAR
    return _NO_SIDECAR


def main(argv=None) -> int:
    """Manual CLI: ``python -m zagg.sweep <store_root>`` (issue #300, D22).

    ``--partitions 2^n`` (issue #377) folds one partition at a time instead of
    the whole tree — the single-process half of the parallel sweep: no
    speed-up, but peak memory is bounded by the partition, which is what lets
    a store no single walk fits still be swept from a laptop. Prints a list of
    per-partition summaries in that mode instead of the single summary.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="zagg unified rollup sweep: fold leaf artifacts into interior-node "
        "rollups (issue #300). Work is discovered from the store's run records."
    )
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument(
        "--families",
        default=None,
        help=f"Comma-separated families (default: {','.join(DEFAULT_FAMILIES)}; "
        f"registered: {', '.join(sorted(FAMILIES))})",
    )
    parser.add_argument(
        "--stages",
        action="store_true",
        help="Run the STAGED pyramid sweep for zagg-pyramid/2 stores (issue #384): "
        "tuple-grouped stage workers over the leaf columns, lease-admitted, with the "
        "designated finisher. Composes with --partitions (swept under one lease). "
        "The families sweep does not run in this mode.",
    )
    parser.add_argument(
        "--tuple-width",
        type=int,
        default=3,
        help="Stage dispatch cadence for --stages: orders per tuple (default: 3). "
        "Grouping is orchestration-only — it changes no bytes (issue #381 point (6))",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="Split the pass into 2^n disjoint morton-subtree partitions and fold them "
        "one at a time, bounding peak memory by the partition rather than the store "
        "(issue #377). Must be a power of FOUR (each morton digit is 2 bits); coarse "
        "levels above the split order are left to the finisher. Default: 1 (whole tree)",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--output-creds",
        default=None,
        metavar="PATH",
        help="Path to a JSON credentials file for the store (same format as python -m zagg)",
    )
    parser.add_argument(
        "--declare-pyramid",
        default=None,
        metavar="CONFIG_YAML",
        help="Retrofit the manifest pyramid declaration from this pipeline config "
        "(issue #358), print the summary, and exit — declaration-only: no sweep "
        "pass runs in the same invocation (--families and --partitions are ignored)",
    )
    args = parser.parse_args(argv)
    # Validate the fan-out width from argv alone, BEFORE anything touches the
    # store: discover_leaves is a LIST plus a parquet read per run record, and
    # a mistyped width should not cost that (review finding, issue #377).
    from zagg.sweep_partition import partition_split_order

    partition_split_order(args.partitions)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    store_kwargs: dict = {"region": args.region}
    if args.output_creds:
        from zagg.runner import normalize_output_credentials

        with open(args.output_creds) as f:
            credentials = normalize_output_credentials(json.load(f))
        store_kwargs["credentials"] = credentials
        store_kwargs["endpoint_url"] = credentials.get("endpointUrl")
    # ``is not None``, never truthiness: an empty value (an unset shell
    # variable expanding to --declare-pyramid="") must reach load_config and
    # fail loudly, not fall through to the sweep pass the flag advertises it
    # will not run — that path WRITES (review finding, issue #358).
    if args.declare_pyramid is not None:
        from zagg.config import load_config
        from zagg.sweep_overview import declare_pyramid

        summary = declare_pyramid(
            args.store_root, load_config(args.declare_pyramid), store_kwargs=store_kwargs
        )
        print(json.dumps(summary, indent=2))
        return 0
    families = [f.strip() for f in args.families.split(",")] if args.families else None
    leaves = discover_leaves(args.store_root, store_kwargs=store_kwargs)
    if not leaves:
        print("No completed leaves found in the store's run records; nothing to sweep.")
        return 0
    if args.stages:
        from zagg.sweep_stages import run_stage_sweep

        summary = run_stage_sweep(
            args.store_root,
            leaves,
            tuple_width=args.tuple_width,
            partitions=args.partitions if args.partitions != 1 else None,
            store_kwargs=store_kwargs,
        )
        print(json.dumps(summary, indent=2))
        return 0
    if args.partitions != 1:
        from zagg.sweep_partition import sweep_partitions

        summary = sweep_partitions(
            args.store_root,
            leaves,
            partitions=args.partitions,
            families=families,
            store_kwargs=store_kwargs,
        )
    else:
        summary = run_sweep(args.store_root, leaves, families=families, store_kwargs=store_kwargs)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
