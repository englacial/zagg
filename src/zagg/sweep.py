"""Unified second-pass rollup sweep (issue #300, design §7 / D22).

One idempotent bottom-up pass over a hive store's digit tree that folds leaf
artifacts into interior-node rollups, per registered **artifact family**
(D22): stats sidecars (the :func:`zagg.telemetry.merge` fold), MOC regen,
sub-shardmap rollups (stubbed on PR #295), overview zarrs (reserved for
issue #201), and optional debris collection (stubbed). Everything the sweep
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
chain (skip-if-current elsewhere keeps the pass idempotent: a second sweep
over an unchanged tree PUTs nothing).

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

#: Families swept when the caller does not choose (phase 1: stats only).
DEFAULT_FAMILIES = ("stats",)


class SweepFamily:
    """One derived-artifact family (D22): how leaves read and payloads fold.

    Subclasses implement :meth:`read_leaf` (one leaf's contribution + its
    staleness timestamp) and :meth:`merge` (the associative payload fold);
    :meth:`finish` is an optional post-walk hook fed the top-level (base-node)
    artifacts — the seam the MOC family uses to refresh the store-root
    ``coverage.moc``. Families registered with ``available = False`` are
    visible slots that :func:`get_family` refuses with their ``reason``.
    """

    name = ""
    available = True
    reason = ""

    @property
    def rollup_name(self) -> str:
        """The family's per-node rollup object name."""
        return f"{self.name}.rollup.json"

    def read_leaf(self, store_root, decimal, window, store_kwargs):
        """One leaf's ``(payload, timestamp)`` contribution, or ``None``.

        ``None`` means the leaf carries no artifact for this family (e.g. a
        fail-open sidecar PUT that never landed) — it is skipped, not fatal.
        """
        raise NotImplementedError

    def merge(self, payloads: list) -> dict:
        """Fold payloads into one (associative — rollup == direct, §8.3)."""
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

    def read_leaf(self, store_root, decimal, window, store_kwargs):
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path
        from zagg.telemetry import read_sidecar

        leaf = shard_leaf_path(store_root, morton_word(decimal), window=window)
        record = read_sidecar(leaf, **store_kwargs)
        if record is None:
            return None
        return record, record.get("timestamp")

    def merge(self, payloads: list) -> dict:
        from zagg.telemetry import merge

        return merge(payloads)


class SubmapFamily(SweepFamily):
    """Sub-shardmap rollups (D22) — registered, NOT implemented yet."""

    name = "submap"
    available = False
    reason = (
        "sub-shardmap rollups fold leaf ShardMap JSON up-tree via the exact "
        "coarsen regroup (ShardMap.reproject), still under review on PR #295"
    )


class OverviewFamily(SweepFamily):
    """Overview zarrs (D11/D22) — the reserved issue #201 slot."""

    name = "overview"
    available = False
    reason = (
        "overview zarr aggregation is issue #201's follow-on PR; this slot "
        "reserves the family registration (D22)"
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
    cls.name: cls for cls in (StatsFamily, SubmapFamily, OverviewFamily, DebrisFamily)
}


def get_family(name: str) -> SweepFamily:
    """Instantiate a registered family; loud on unknown or stubbed names."""
    cls = FAMILIES.get(name)
    if cls is None:
        raise ValueError(f"unknown sweep family {name!r}; registered: {sorted(FAMILIES)}")
    if not cls.available:
        raise NotImplementedError(f"sweep family {name!r} is registered but stubbed: {cls.reason}")
    return cls()


def run_sweep(store_root: str, leaves, *, families=None, store_kwargs: dict | None = None) -> dict:
    """One sweep pass: fold leaf artifacts up-tree for each family (D22).

    ``leaves`` is the run-record-derived work set — an iterable of
    ``(shard_key, window)`` pairs (or bare shard keys, meaning unwindowed):
    the leaves whose ancestors may be stale. The walk visits ONLY those
    ancestor paths; siblings contribute via their stored rollups. Leaves at a
    non-manifest order are skipped with a warning (mixed-order stores are
    unsupported this round, matching ``refresh_root_coverage``).

    Idempotent: a rollup whose stored generation (merged-leaf count + max
    leaf timestamp) matches the freshly computed one is left untouched, so a
    second pass over an unchanged tree writes nothing. Returns a summary with
    per-family ``written`` / ``current`` (skip-if-current) / ``empty`` (no
    artifact found) / ``failed`` (unmergeable, logged) counts.
    """
    from zagg.hive import MANIFEST_NAME, read_manifest
    from zagg.store import open_object_store

    store_kwargs = dict(store_kwargs or {})
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    shard_order = int(manifest["shard_order"])
    fams = [get_family(n) for n in (DEFAULT_FAMILIES if families is None else families)]
    by_shard, skipped = _normalize_leaves(leaves, shard_order)
    store = open_object_store(store_root, **store_kwargs)
    summary: dict = {
        "store_root": store_root,
        "shard_order": shard_order,
        "n_leaves": sum(len(w) for w in by_shard.values()),
        "skipped_leaves": skipped,
        "families": {},
    }
    for fam in fams:
        summary["families"][fam.name] = _sweep_family(
            store_root, store, fam, by_shard, shard_order, store_kwargs
        )
    return summary


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


def _sweep_family(store_root, store, fam, by_shard, shard_order, store_kwargs) -> dict:
    """Bottom-up fold of one family over the dirty ancestor paths."""
    counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
    computed: dict[str, dict | None] = {}
    for decimal in sorted(by_shard):
        computed[decimal] = _rollup_shard_node(
            store_root, store, fam, decimal, by_shard[decimal], shard_order, store_kwargs, counts
        )
    frontier = [d for d in sorted(by_shard) if computed[d] is not None]
    for _order in range(shard_order - 1, -1, -1):
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
    result.update(fam.finish(store_root, tops, shard_order, store_kwargs))
    return result


def _rollup_shard_node(
    store_root, store, fam, decimal, windows, shard_order, store_kwargs, counts
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
            got = fam.read_leaf(store_root, decimal, window, store_kwargs)
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
    if existing is not None and existing.get("generation") == generation:
        counts["current"] += 1
        return existing
    payload = _merged(fam, [p for _w, p, _ts in parts], decimal, counts)
    if payload is None:
        return None
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
    if existing is not None and existing.get("generation") == generation:
        counts["current"] += 1
        return existing
    payload = _merged(fam, [a["payload"] for a in children], node, counts)
    if payload is None:
        return None
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


def _merged(fam, payloads, node, counts) -> dict | None:
    """The family fold, fail-open per node: unmergeable -> logged + skipped."""
    try:
        return fam.merge(payloads)
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
