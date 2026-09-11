"""Rollup → GeoJSON choropleth exporter (issue #301, gridlook viewer track).

Reads a published hive store's per-node **stats rollups** (the issue #300
sweep artifacts) and emits a GeoJSON ``FeatureCollection`` — one polygon per
shard (or per order-``N`` rollup node) whose properties gridlook's existing
``vectorChoropleth`` layer colors by: ``n_obs``, ``cells_with_data``,
``n_granules``, ``est_cost_usd``, ``gb_seconds``, ``duration_s``. Zero viewer
code: the layer is property-driven, so this file IS the integration.

Reads are anonymous-capable object GETs (``skip_signature=True`` for
published buckets) and degrade gracefully on a partial or stale rollup tree —
the live demo stores make that ladder load-bearing, not theoretical: every
covered ATL03 shard carries its own shard-node rollup, but its *interior*
rollups lag well behind the leaves beneath them (node ``3``: a generation
stamp of 1,054 leaves against 2,819 covered shards), so the fold-from-below
arm runs for real on every one of them, while GEDI's tree is complete:

1. A node's own ``stats.rollup.json`` when present — at interior nodes when
   its generation stamp accounts for at least the covered shards beneath it
   (``n_leaves >= covered shards``), and at shard nodes when the windows the
   envelope records still match the sidecars the node holds. Both checks see
   only *new* leaves: a leaf re-run in place (same shard, same window) moves
   no counter either one reads, so a rollup that FAILS a check is definitely
   stale, while one that passes is not thereby proven fresh — on any store,
   windowed or not. A feature's ``timestamp`` is the latest leaf timestamp
   its numbers account for; that is the vintage signal.
2. Otherwise fold from below: child rollups where usable, per-leaf
   ``stats.json`` sidecars at the shards (windows discovered by one node
   LIST, merged via :func:`zagg.telemetry.merge`; a LIST-less store falls
   back to the unwindowed sidecar name).
3. A covered shard with no stats artifact at all still gets its polygon,
   with null metrics — coverage says the data leaf exists.

Each feature marks provenance: ``"source": "rollup"`` (numbers came from
stored rollups), ``"leaf_stats"`` (folded from leaf sidecars), ``"mixed"``
(both, under a stale interior node), ``"partial"`` (the fold reached a
covered shard with no readable stats, so the numbers are short by whatever
that subtree holds), or ``"missing"`` (nothing at all). Every feature also
carries ``n_covered``, the covered shards beneath its node, so a consumer
can size a ``partial`` cell's hole against ``n_leaves``.

Shard discovery is the store-root ``coverage.moc`` ranges MOC (D10: no tree
LIST on the discovery path). Geometry is ``mortie.mort2polygon`` per node —
note mortie returns ``[lat, lon]`` rings; GeoJSON (RFC 7946) is
``[lon, lat]``, swapped here — closed, CCW-oriented, and split into a
``MultiPolygon`` at the antimeridian when a cell crosses it. ``n_granules``
sums per-shard counts, so a granule spanning shards counts once per shard
(the deduplicated count is the submap rollup's job, not this layer's).

CLI: ``python -m zagg.choropleth s3://bucket/store.zarr --anon -o out.geojson
[--order N]`` — ``--order`` emits the coarser rollup-node polygons (the
zoomed-out layer).
"""

from __future__ import annotations

import json
import logging

import numpy as np

logger = logging.getLogger(__name__)

#: Metric properties every feature carries (``None`` where unrecorded) — the
#: choropleth's color-by menu. Names match the stats record fields (§5 D20).
PROPERTY_KEYS = (
    "n_obs",
    "cells_with_data",
    "n_granules",
    "est_cost_usd",
    "gb_seconds",
    "duration_s",
)


def export_choropleth(store_root: str, *, order=None, step=4, workers=8, store_kwargs=None) -> dict:
    """GeoJSON ``FeatureCollection`` of a store's per-node stats (issue #301).

    ``order`` selects the emission level: ``None`` (default) emits one
    feature per covered shard; an integer ``0 <= order <= shard_order`` emits
    one feature per covered rollup-tree node at that HEALPix order (the
    zoomed-out layer). ``step`` is points per polygon side
    (``mortie.mort2polygon``; > 1 traces curved cell edges — matters near the
    poles). ``store_kwargs`` reach :func:`zagg.store.open_object_store`
    (``skip_signature=True`` + ``region`` for anonymous published-bucket
    reads). ``workers`` bounds the thread pool resolving nodes concurrently —
    per-node work is independent small-object GETs, so a published store
    export is fetch-latency-bound (1 = sequential).

    Raises ``ValueError`` when the root manifest or a usable ``coverage.moc``
    is absent (discovery is the root MOC — regenerate it with
    ``python -m zagg.sweep``), or when the MOC's order disagrees with the
    manifest ``shard_order`` (mixed-order stores are unsupported, as in the
    sweep).
    """
    from collections import Counter

    from mortie import mort2polygon

    from zagg.grids.morton import morton_word
    from zagg.hive import _decimal_base
    from zagg.store import open_object_store
    from zagg.sweep import get_family

    store_kwargs = dict(store_kwargs or {})
    manifest, covered = _covered_shards(store_root, store_kwargs)
    shard_order = int(manifest["shard_order"])
    order = shard_order if order is None else int(order)
    if not 0 <= order <= shard_order:
        raise ValueError(f"order must be in [0, {shard_order}] (the shard order); got {order}")
    n_covered = Counter(d[: len(_decimal_base(d)) + order] for d in covered)
    nodes = sorted(n_covered)
    store = open_object_store(store_root, **store_kwargs)
    fam = get_family("stats")
    spec = manifest.get("spec")
    words = np.asarray([morton_word(n) for n in nodes], dtype=np.uint64)
    rings = mort2polygon(words, step=step)
    if len(nodes) == 1:
        rings = [rings]  # mortie unwraps a size-1 batch to the bare ring
    from concurrent.futures import ThreadPoolExecutor

    def resolve(node):
        return _resolve_node(store, node, covered, shard_order, spec, fam)

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        resolved = list(pool.map(resolve, nodes))
    features = []
    for node, ring, (record, source, n_leaves) in zip(nodes, rings, resolved):
        props = {
            "morton": node,
            "order": order,
            "source": source,
            "n_leaves": n_leaves,
            "n_covered": n_covered[node],
        }
        for key in PROPERTY_KEYS + ("success", "timestamp"):
            props[key] = None if record is None else record.get(key)
        features.append({"type": "Feature", "geometry": _geometry(ring), "properties": props})
    return {
        "type": "FeatureCollection",
        "features": features,
        # Foreign member (RFC 7946 §6.1): export provenance for humans/tools;
        # property-driven consumers (gridlook) ignore it.
        "zagg_choropleth": {"store_root": store_root, "order": order, "shard_order": shard_order},
    }


def _covered_shards(store_root: str, store_kwargs: dict):
    """The manifest + covered shard decimals from the root ``coverage.moc``."""
    from zagg.coverage import load_coverage
    from zagg.grids.morton import morton_decimal
    from zagg.hive import MANIFEST_NAME, ROOT_COVERAGE_NAME, read_manifest, root_coverage_words

    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    envelope = load_coverage(store_root, **store_kwargs)
    if envelope is None:
        raise ValueError(
            f"no usable {ROOT_COVERAGE_NAME} at {store_root} — the exporter discovers "
            f"shards from the root MOC (D10: no tree LIST); regenerate it with "
            f"'python -m zagg.sweep {store_root}'"
        )
    if int(envelope["order"]) != int(manifest["shard_order"]):
        raise ValueError(
            f"root MOC order {envelope['order']} != manifest shard_order "
            f"{manifest['shard_order']} — mixed-order stores are unsupported"
        )
    return manifest, sorted(morton_decimal(int(w)) for w in root_coverage_words(envelope))


def _resolve_node(store, node: str, covered, shard_order: int, spec, fam):
    """``(record | None, source, n_leaves)`` for one rollup-tree node.

    The fallback ladder from the module docstring: own rollup (generation-
    checked against the covered shards beneath at interior nodes) → fold of
    the four children → ``(None, "missing", 0)``. The fold is
    :func:`zagg.telemetry.merge`, the same associative law the sweep uses, so
    a folded-from-below value equals what a fresh rollup would store.

    A fold that loses a covered child subtree (nothing readable beneath it)
    reports ``"partial"``, not the child sources: the numbers are real but
    short, and a choropleth must not color that cell as a low reading.
    """
    from zagg.hive import _decimal_order
    from zagg.sweep import _read_rollup
    from zagg.telemetry import merge

    if _decimal_order(node) == shard_order:
        return _resolve_shard(store, node, spec, fam)
    envelope = _read_rollup(store, fam, node)
    beneath = sum(1 for d in covered if d.startswith(node))
    if envelope is not None:
        n_leaves = int(envelope["generation"]["n_leaves"])
        if n_leaves >= beneath:
            return envelope["payload"], "rollup", n_leaves
        logger.info(
            f"choropleth: stale/partial stats rollup at node {node} ({n_leaves} leaves "
            f"< {beneath} covered shards); folding from below"
        )
    parts, sources, n_leaves, partial = [], set(), 0, False
    for digit in "1234":
        child = node + digit
        if not any(d.startswith(child) for d in covered):
            continue
        record, source, n = _resolve_node(store, child, covered, shard_order, spec, fam)
        partial = partial or record is None or source == "partial"
        if record is None:
            continue
        parts.append(record)
        sources.add(source)
        n_leaves += n
    if not parts:
        return None, "missing", 0
    try:
        merged = merge(parts)
    except ValueError as e:
        logger.warning(f"choropleth: cannot fold node {node}'s children ({e}); marking missing")
        return None, "missing", 0
    if partial:
        logger.info(
            f"choropleth: node {node} folded from below over "
            f"{n_leaves} leaves with a covered subtree missing; marking partial"
        )
        return merged, "partial", n_leaves
    return merged, sources.pop() if len(sources) == 1 else "mixed", n_leaves


def _resolve_shard(store, decimal: str, spec, fam):
    """One shard node: its rollup, else its merged leaf sidecars, else missing.

    The rollup is taken on its face with one check, free because the envelope
    already carries the evidence: a windowed shard's rollup records the
    windows it merged (``_rollup_shard_node``), so a window that landed after
    the sweep is caught by the one node LIST the fallback would run anyway.
    An unwindowed shard skips even that — its single leaf is by construction
    the one the rollup merged — so the happy path stays one GET per shard.
    Neither arm sees a leaf re-run in place (same window, new content): that
    needs the leaf's own timestamp, i.e. exactly the fold the rollup exists
    to avoid. See the module docstring's ladder note.
    """
    from zagg.sweep import _read_rollup, _sidecar_window
    from zagg.telemetry import merge

    envelope = _read_rollup(store, fam, decimal)
    names = listed = None
    if envelope is not None:
        n_leaves = int(envelope["generation"]["n_leaves"])
        merged = {None if w is None else str(w) for w in envelope.get("windows") or [None]}
        if merged == {None}:  # unwindowed shard: the one leaf is in the rollup
            return envelope["payload"], "rollup", n_leaves
        names, listed = _sidecar_names(store, decimal, spec)
        # A failed LIST is no evidence about windows, so it never unseats a rollup.
        unmerged = {_sidecar_window(n, spec) for n in names} - merged if listed else set()
        if not unmerged:
            return envelope["payload"], "rollup", n_leaves
        logger.info(
            f"choropleth: shard {decimal}'s stats rollup predates window(s) "
            f"{sorted(str(w) for w in unmerged)}; folding its leaf sidecars"
        )
    if names is None:
        names, listed = _sidecar_names(store, decimal, spec)
    records = _read_sidecars(store, decimal, names)
    if not records:
        return None, "missing", 0
    try:
        return merge(records), "leaf_stats", len(records)
    except ValueError as e:
        logger.warning(f"choropleth: cannot merge shard {decimal}'s sidecars ({e})")
        return None, "missing", 0


def _sidecar_names(store, decimal: str, spec) -> tuple[list[str], bool]:
    """A shard node's stats-sidecar object names + whether the LIST succeeded.

    Windows are discovered by one delimiter LIST of the shard's node — the
    same bounded, run-scoped pattern as the sweep's pre-column fallback,
    never a tree walk. A store without LIST permission falls back to the one
    sidecar name that needs no discovery, the unwindowed leaf's, and reports
    ``False``: that guess is not evidence of which windows exist.
    """
    import obstore

    from zagg.sweep import _NO_SIDECAR, _node_rel, _sidecar_window
    from zagg.telemetry import SPEC_V3, sidecar_key
    from zagg.windows import SCHEDULE_NONE_TOKEN

    node = _node_rel(decimal)
    try:
        listing = obstore.list_with_delimiter(store, node + "/")
        names = [o["path"].rsplit("/", 1)[-1] for o in listing["objects"]]
        return [n for n in names if _sidecar_window(n, spec) is not _NO_SIDECAR], True
    except Exception as e:  # no LIST permission on the published bucket
        logger.debug(f"choropleth: node LIST failed at {node} ({e}); trying unwindowed sidecar")
        stem = SCHEDULE_NONE_TOKEN if spec == SPEC_V3 else decimal
        return [sidecar_key(f"{stem}.zarr", spec)], False


def _read_sidecars(store, decimal: str, names) -> list[dict]:
    """The readable stats-sidecar records among ``names`` at a shard node."""
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.sweep import _node_rel

    node = _node_rel(decimal)
    records = []
    for name in names:
        try:
            records.append(json.loads(bytes(obstore.get(store, f"{node}/{name}").bytes())))
        except (FileNotFoundError, NotFoundError):
            continue
    return records


def _geometry(ring_latlon) -> dict:
    """GeoJSON geometry from a mortie ``[lat, lon]`` ring.

    Swaps to RFC 7946 ``[lon, lat]`` order, orients the exterior CCW, and
    splits a cell that genuinely crosses the antimeridian (lon span > 180°
    after mortie's touching-cell normalization) into a two-part
    ``MultiPolygon`` so no consumer draws it the wrong way around the globe.
    """
    from shapely.affinity import translate
    from shapely.geometry import MultiPolygon, Polygon, box, mapping
    from shapely.geometry.polygon import orient

    ring = [(float(lon), float(lat)) for lat, lon in ring_latlon]
    lons = [x for x, _y in ring]
    if max(lons) - min(lons) <= 180.0:
        return json.loads(json.dumps(mapping(orient(Polygon(ring)))))
    shifted = Polygon([(x + 360.0 if x < 0.0 else x, y) for x, y in ring])
    halves = (
        shifted.intersection(box(0.0, -90.0, 180.0, 90.0)),
        translate(shifted.intersection(box(180.0, -90.0, 360.0, 90.0)), xoff=-360.0),
    )
    parts = []
    for half in halves:
        polys = getattr(half, "geoms", [half])
        parts.extend(orient(p) for p in polys if p.geom_type == "Polygon" and not p.is_empty)
    return json.loads(json.dumps(mapping(MultiPolygon(parts))))


def main(argv=None) -> int:
    """CLI: ``python -m zagg.choropleth <store_root> [-o out.geojson]``."""
    import argparse
    from collections import Counter

    parser = argparse.ArgumentParser(
        description="Export a hive store's per-node stats rollups as a GeoJSON "
        "FeatureCollection for gridlook's vectorChoropleth layer (issue #301)."
    )
    parser.add_argument("store_root", help="Hive store root (local path or s3://bucket/prefix)")
    parser.add_argument("-o", "--output", default=None, help="Output path (default: stdout)")
    parser.add_argument(
        "--order",
        type=int,
        default=None,
        help="Emit rollup-node polygons at this HEALPix order instead of leaf "
        "shards (the zoomed-out layer)",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=4,
        help="Points per polygon side (default: 4; raise near the poles)",
    )
    parser.add_argument(
        "--anon",
        action="store_true",
        help="Anonymous (unsigned) reads, for published/public buckets",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent node-resolution threads (default: 8; 1 = sequential)",
    )
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    parser.add_argument(
        "--output-creds",
        default=None,
        metavar="PATH",
        help="Path to a JSON credentials file for the store (same format as python -m zagg)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    store_kwargs: dict = {"region": args.region}
    if args.anon:
        store_kwargs["skip_signature"] = True
    if args.output_creds:
        from zagg.runner import normalize_output_credentials

        with open(args.output_creds) as f:
            credentials = normalize_output_credentials(json.load(f))
        store_kwargs["credentials"] = credentials
        store_kwargs["endpoint_url"] = credentials.get("endpointUrl")
    collection = export_choropleth(
        args.store_root,
        order=args.order,
        step=args.step,
        workers=args.workers,
        store_kwargs=store_kwargs,
    )
    text = json.dumps(collection)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
    else:
        print(text)
    counts = Counter(f["properties"]["source"] for f in collection["features"])
    logger.info(
        f"choropleth: {len(collection['features'])} features "
        f"({', '.join(f'{k}={v}' for k, v in sorted(counts.items()))})"
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
