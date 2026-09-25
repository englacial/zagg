"""Reader-side coverage primitives — issue #200 phase 4 (design §5-lite).

The minimal consumption layer over the write-side machinery in
:mod:`zagg.hive` (the full §5 reader architecture is its own effort): load
the store-root envelope, intersect an AOI against each coverage tier (root
ranges MOC → leaf tier-0 box → leaf bitmap sidecar → the leaf's ``morton``
coordinate as exact truth), detect staleness lazily per the O7 lean, and
rebuild the root MOC explicitly. No LISTs on any hot path (D10) — the one
sanctioned walk lives in :func:`refresh_root_coverage`.

Lives in its own module rather than ``zagg.hive`` to keep that file inside
the ~1000-line module guidance (review finding, PR #208 round 3): ``hive``
owns the write side and the raw accessors; this module owns consumption.
"""

from __future__ import annotations

import json
import logging
import warnings

import numpy as np

from zagg.column import COLUMN_SUFFIX
from zagg.hive import (
    COVERAGE_SPEC,
    MANIFEST_NAME,
    ROOT_COVERAGE_NAME,
    _decimal_base,
    _decimal_order,
    _decimal_rank,
    _is_base_component,
    build_root_coverage,
    read_commit,
    read_coverage_bitmap,
    read_manifest,
    read_root_coverage,
    root_coverage_words,
)
from zagg.store import open_object_store
from zagg.windows import split_leaf_name, union_time_range

logger = logging.getLogger(__name__)

#: Stores already warned about a stale root MOC in this process (O7: warn
#: once per stale episode, trust silently otherwise, never auto-walk).
#: Keyed on the slash-normalized root; reset by a successful
#: :func:`refresh_root_coverage` so a LATER episode warns again.
_stale_warned: set[str] = set()


def load_coverage(store_root: str, **store_kwargs) -> dict | None:
    """The store-root coverage envelope, or ``None`` when unusable.

    The tolerant reader-facing counterpart of
    :func:`zagg.hive.read_root_coverage` (which raises on garbage JSON):
    a missing object, unparsable JSON, or an unknown spec/encoding all read
    as absent — the reader degrades to the walk, never to a wrong answer
    (D9) — with a debug log so the degradation is discoverable. Strict spec
    gate, the same posture as :func:`zagg.hive.read_coverage`.
    """
    try:
        envelope = read_root_coverage(store_root, **store_kwargs)
    except ValueError as e:
        logger.debug(f"unparsable {ROOT_COVERAGE_NAME} at {store_root} ({e}); ignoring")
        return None
    if envelope is None:
        return None
    usable = (
        isinstance(envelope, dict)
        and envelope.get("spec") == COVERAGE_SPEC
        and envelope.get("encoding") == "ranges"
    )
    if not usable:
        logger.debug(f"{ROOT_COVERAGE_NAME} at {store_root} has an unknown spec/encoding; ignoring")
        return None
    return envelope


def root_coverage_and(envelope: dict, aoi) -> np.ndarray:
    """Intersection of the root MOC with an AOI morton cover.

    ``aoi`` is any morton cover (mixed order allowed — shard-order or
    cell-order words both work; mortie's ``moc_and`` resolves containment
    across orders). Returns the compacted intersection; an empty array means
    the AOI touches no covered shard. Expansion is O(covered shards) — the
    scale note on :func:`zagg.hive.root_coverage_words` applies.
    """
    from mortie import moc_and

    return moc_and(root_coverage_words(envelope), np.asarray(aoi, dtype=np.uint64))


def box_and(coverage: dict, aoi) -> np.ndarray:
    """Intersection of a LEAF envelope's tier-0 box with an AOI morton cover.

    ``coverage`` is the stamp payload from :func:`zagg.hive.read_coverage`.
    One in-memory op on <= 4 members — the cheap AOI reject readers run on
    the stamp GET they already make, before paying for the bitmap sidecar.
    An empty result rejects the leaf outright (the box is a conservative
    superset: false positives possible, false negatives impossible).
    """
    from mortie import moc_and

    from zagg.grids.morton import morton_word

    members = [morton_word(s) for s in coverage["box"] if s is not None]
    return moc_and(np.asarray(members, dtype=np.uint64), np.asarray(aoi, dtype=np.uint64))


def bitmap_and(leaf_root: str, aoi, **store_kwargs) -> np.ndarray | None:
    """Exact cell-level intersection via a leaf's coverage (bitmap or full).

    Reads the stamp envelope once. ``encoding: "full"`` (issue #246, D14 —
    whole-subtree coverage, no sidecar object exists) short-circuits to the
    shard's own MOC membership: ``moc_and`` against the shard id resolves
    containment exactly, with no bitmap GET and no cell expansion. The
    ``"bitmap"`` path pays the one opt-in sidecar GET
    (:func:`zagg.hive.read_coverage_bitmap`, envelope reused). ``None`` when
    the leaf carries neither (box-only phase-1 stamp, depth-0 config, debris,
    absent leaf) — the caller falls back to the box verdict; a
    present-but-corrupt sidecar raises (the decoder's posture). An empty
    array is a definitive miss: both encodings are exact, not conservative.
    """
    from zagg.hive import read_coverage
    from zagg.store import open_store

    coverage = read_coverage(open_store(leaf_root, **store_kwargs))
    if not coverage:
        return None
    from mortie import moc_and

    if coverage.get("encoding") == "full":
        from zagg.grids.morton import morton_word

        word = morton_word(split_leaf_name(leaf_root.rstrip("/").rsplit("/", 1)[-1])[0])
        return moc_and(np.asarray([word], dtype=np.uint64), np.asarray(aoi, dtype=np.uint64))
    occupied = read_coverage_bitmap(leaf_root, coverage=coverage, **store_kwargs)
    if occupied is None:
        return None
    return moc_and(occupied, np.asarray(aoi, dtype=np.uint64))


def _ranges_contain(envelope: dict, decimal: str) -> bool:
    """Whether the envelope's ranges list one shard id — rank space, O(ranges).

    No word expansion (the containment check runs on the reader's hot path,
    unlike the union's :func:`~zagg.hive.root_coverage_words`).
    """
    if _decimal_order(decimal) != int(envelope["order"]):
        return False
    base, rank = _decimal_base(decimal), _decimal_rank(decimal)
    return any(
        _decimal_base(lo) == base and _decimal_rank(lo) <= rank <= _decimal_rank(hi)
        for lo, hi in envelope["ranges"]
        if _decimal_base(hi) == _decimal_base(lo)
    )


def warn_if_stale(store_root: str, shard_key, envelope: dict | None) -> bool:
    """O7 lazy staleness detection for one opened, commit-stamped leaf.

    Call when a reader holds POSITIVE evidence of a committed shard — it
    opened the leaf and read the stamp — that the store's root MOC does not
    list. The most common cause is BENIGN: a run still in progress (the root
    MOC is written at end of run, while leaves stamp continuously — review
    finding, PR #208 round 4); the pathological causes are a crashed run,
    the concurrent-run union race, and out-of-band writes. Returns ``True``
    when stale, warning ONCE per store per stale episode with the regen
    suggestion (the latch is slash-normalized and reset by a successful
    :func:`refresh_root_coverage`); afterwards it stays silent — the hot
    path trusts silently and never auto-walks (D10). ``envelope`` may be
    ``None`` (no root MOC at all): that is absence, not staleness, and reads
    ``False``. Containment is checked in rank space (O(ranges), no word
    expansion); a malformed envelope counts as not-listing, i.e. stale.
    """
    if envelope is None:
        return False
    from zagg.grids.morton import morton_decimal

    decimal = morton_decimal(shard_key)
    try:
        if _ranges_contain(envelope, decimal):
            return False
    except (KeyError, TypeError, ValueError):
        pass  # malformed envelope cannot vouch for the shard — stale
    key = store_root.rstrip("/")
    if key not in _stale_warned:
        _stale_warned.add(key)
        warnings.warn(
            f"commit-stamped shard {decimal} is not listed by {store_root}/"
            f"{ROOT_COVERAGE_NAME} — the root MOC lags the leaves. Usually benign: a "
            f"run still in progress writes the root MOC only at end of run. If no run "
            f"is active, the causes are a crashed run, the concurrent-run union race, "
            f"or out-of-band writes; regenerate with "
            f"zagg.coverage.refresh_root_coverage({store_root!r})",
            stacklevel=2,
        )
    return True


def _is_decimal_id(decimal: str) -> bool:
    """Whether ``decimal`` is a well-formed D1 id: ``{sign+base}`` + ``[1-4]`` digits.

    The guard that keeps ``morton_words_from_decimals`` from raising out of the
    repair walk: a foreign basename can satisfy the leaf-name grammar AND
    :func:`~zagg.hive._decimal_order` without being a morton id at all
    (``all.zarr`` — ``_decimal_order("all") == 2``).
    """
    base = _decimal_base(decimal)
    return _is_base_component(base) and all(ch in "1234" for ch in decimal[len(base) :])


def _is_derived_zarr(zarr_root: str, store_kwargs: dict) -> bool:
    """Whether a ``.zarr``'s root attrs declare a D11 ``role`` (derived artifact).

    Overview pyramid zarrs (issue #201) sit at ANCESTOR digit nodes under
    ``{window}.zarr`` basenames, so the walker's leaf grammar can match them
    (``all.zarr``, or a digits-only window label that parses as a decimal); the
    ``role`` attr is the sanctioned discriminator — D11 forbids inferring the
    role from tree position. One small GET of the root group metadata, gated by
    the caller on a non-shard-order depth so source leaves never pay it.
    """
    from zagg.hive import _read_json
    from zagg.sweep_overview import ROLE_ATTR

    try:
        meta = _read_json(open_object_store(zarr_root, **store_kwargs), "zarr.json")
    except ValueError:
        return False
    attrs = (meta or {}).get("attributes")
    return isinstance(attrs, dict) and ROLE_ATTR in attrs


def refresh_root_coverage(
    store_root: str, *, materialize: bool = True, **store_kwargs
) -> dict | None:
    """Rebuild the root MOC from a full tree walk — the explicit escape hatch.

    THE SANCTIONED ROBUSTNESS PATH, not the hot path: D10 forbids walking
    per read, but the strongly-consistent delimiter-LIST walk is ground
    truth (§2), so an explicit ``refresh`` recovers from crashed runs, the
    union race, and garbage objects. One carve-out (review finding, PR #208
    round 4): a stamped leaf at a NON-manifest order — hand-copied data or
    an old-config survivor of a partial clear — cannot be represented in a
    fixed-order ranges MOC; it is SKIPPED with a logged warning (this round
    does not support mixed-order stores) and the root MOC is rebuilt from
    the conforming leaves, so the escape hatch itself never dies on it. One
    LIST per digit node (root: ``{sign+base}`` children; below: ``[1-4]``
    digits; a ``*.zarr`` entry is a leaf at that node), collecting the
    shards whose commit stamp is present — unstamped debris is excluded,
    exactly as the D4 model demands. The fresh envelope carries
    ``source: "refresh"`` and REPLACES the root object (no union: the walk
    supersedes it). DERIVED zarrs at ancestor nodes — the overview pyramid
    family, issue #201 — are stamped and can wear leaf-shaped basenames, so a
    ``.zarr`` off the shard-order depth is checked for the D11 ``role`` attr and
    skipped when it carries one (never classified by position) — as is a
    ``{stem}.pyramid.zarr`` column (issue #383), the one derived family that
    lives at the leaf's OWN node. A
    supersedes it). A temporal-declaring store (spec §10, issue #480) also has
    its ``zagg-coverage-toc/1`` section rebuilt from this same walk —
    fail-open per SHARD, so an unreadable companion costs the section that
    shard and never the refresh; and a walk that lost any shard COMPOSES its
    rebuild with the standing section (§10.4) instead of replacing it, so the
    escape hatch can never be the thing that deletes the section. That walk
    also WRITES: every leaf it had to read raw gets the §10.6 record
    materialized into its prefix (``source: "refresh"``, issue #575), so the
    repair is a read plus N leaf PUTs, not a read plus one root write. It is
    fail-open too — an unwritable leaf logs a warning and still contributes —
    but on a store the caller can only READ, that is one failed PUT per leaf:
    pass ``materialize=False`` to keep the escape hatch strictly read-only
    (the sweep keeps the backfill duty). A successful
    refresh also re-arms the
    :func:`warn_if_stale` once-per-episode latch for this store. Returns the
    envelope written, or ``None`` — deleting any existing root object — when
    no stamped leaf exists (absence is truthful, a stale cache is not, and
    the ranges envelope has no empty form). Windowed leaves (issue #246)
    classify as data via the frozen first-``_`` name split; several stamped
    windows of one shard are one covered shard (the MOC is spatial — the
    builder de-duplicates words), and a malformed window label on a STAMPED
    leaf is skipped with a warning, like the foreign-order carve-out —
    unstamped malformed debris stays silent, as ordinary debris does.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.coverage_toc import (
        COVER_KEY,
        TEMPORAL_COVERAGE_SPEC,
        build_cover_section,
        build_temporal_section,
        read_cover,
        temporal_cell_order,
        temporal_fields,
        write_cover,
    )
    from zagg.grids.morton import morton_words_from_decimals
    from zagg.leaf_temporal import leaf_contribution
    from zagg.store import open_store, put_object

    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(f"no {MANIFEST_NAME} at {store_root} — not a hive store root")
    order = int(manifest["shard_order"])
    # The §10 temporal section (issue #480) is rebuilt from the SAME walk, so
    # the escape hatch regenerates it rather than deleting it — and, because
    # this walk is whole-store by construction, its root counted cover is the
    # authoritative one (spec §10's whole-coverage rule). The §10.5 word-set
    # cover sibling (issue #489) rebuilds from the same contributions.
    toc_fields = temporal_fields(manifest)
    cell_order = temporal_cell_order(manifest)
    if toc_fields and cell_order is None:
        logger.warning(
            f"refresh: {store_root} declares temporal fields but carries no cell_order — "
            f"rebuilding no §10 section rather than guessing a group"
        )
        toc_fields = {}
    contributions: dict[str, list] = {}
    toc_failed: set[str] = set()
    store = open_object_store(store_root, **store_kwargs)
    root = store_root.rstrip("/")
    # Decimals accumulate through the walk and parse once at the end (issue
    # #322) — a full-store refresh visits every stamped leaf.
    decimals: list = []
    time_ranges: list = []
    stack = [""]
    while stack:
        prefix = stack.pop()
        listing = obstore.list_with_delimiter(store, prefix or None)
        for child in listing["common_prefixes"]:
            rel = child.rstrip("/")
            name = rel.split("/")[-1]
            if name.endswith(".zarr"):
                # Read the commit stamp FIRST — it opens the full rel path, not
                # the decimal, so it needs no name split. Unstamped debris drops
                # SILENTLY here, malformed name or not, exactly as valid-named
                # debris (D4) and the foreign-order carve-out below do: only real
                # data ever earns a warning.
                stamp = read_commit(open_store(f"{root}/{rel}", **store_kwargs))
                if stamp is None:
                    continue  # unstamped debris (D4)
                at_leaf_depth = rel.count("/") - 1 == order
                if (not at_leaf_depth or name.endswith(COLUMN_SUFFIX)) and _is_derived_zarr(
                    f"{root}/{rel}", store_kwargs
                ):
                    # A DERIVED artifact, not source data: overview pyramid zarrs
                    # are stamped and their basenames can collide with the leaf
                    # grammar (issue #201), and a leaf's own column (issue #383)
                    # is a derived zarr AT shard-order depth — so the depth gate
                    # is a cost optimization only, widened by the column suffix.
                    # Silent like all non-data children — role is authoritative
                    # (D11), position never is.
                    logger.debug(f"refresh: skipping derived zarr {rel!r} (D11 role attr)")
                    continue
                # Windowed leaves (issue #246, D13): `{full_id}_{window}.zarr`
                # names split on the first `_` (frozen parse rule); several
                # windows of one shard collapse to one coverage entry below.
                try:
                    decimal, _window = split_leaf_name(name)
                except ValueError:
                    # A STAMPED leaf (real data) whose name breaks the frozen
                    # grammar — warn only past the stamp gate, the same "only
                    # complain about real data" posture as the foreign-order
                    # branch. The escape hatch stays alive on it either way.
                    logger.warning(
                        f"refresh: skipping STAMPED leaf {name!r} with a malformed "
                        f"window label (frozen grammar, mortie#62) — it will NOT be "
                        f"listed in {ROOT_COVERAGE_NAME}"
                    )
                    continue
                if not _is_decimal_id(decimal):
                    # Same posture, one grammar down: the name split succeeded
                    # but the id is not a morton decimal, so no MOC word exists
                    # for it. Skipped HERE rather than left to raise out of the
                    # closing `morton_words_from_decimals` (issue #201 review).
                    logger.warning(
                        f"refresh: skipping STAMPED leaf {name!r} whose id {decimal!r} is not "
                        f"a D1 morton decimal (sign+base then [1-4] digits) — it will NOT be "
                        f"listed in {ROOT_COVERAGE_NAME}"
                    )
                    continue
                if _decimal_order(decimal) != order:
                    # A fixed-order ranges MOC cannot represent this leaf:
                    # either corruption (old-config data surviving a partial
                    # clear, a hand-copied leaf) or a mixed-order future this
                    # round does not support. Skip it — the escape hatch must
                    # not die on the store it exists to repair.
                    logger.warning(
                        f"refresh: skipping stamped leaf {decimal} at order "
                        f"{_decimal_order(decimal)} under a shard_order-{order} manifest "
                        f"(mixed-order stores are unsupported; clear or re-shard the "
                        f"foreign-order data) — it will NOT be listed in {ROOT_COVERAGE_NAME}"
                    )
                    continue
                decimals.append(decimal)
                if toc_fields and decimal not in toc_failed:
                    try:
                        got, _route = leaf_contribution(
                            f"{root}/{rel}",
                            cell_order,
                            toc_fields,
                            materialize=materialize,
                            # This walk's own provenance (§10.6): a record
                            # the refresh backfilled must not claim the
                            # sweep wrote it, like every other object here.
                            source="refresh",
                            **store_kwargs,
                        )
                    except Exception as e:  # fail-open: the section is a cache
                        # Shard-scoped, not leaf-scoped: §10.2's word must
                        # contain EVERY instant in a listed shard, which a
                        # word joined over the window leaves that happened to
                        # read cannot promise. Drop the shard — absent means
                        # "unknown", which stays a candidate.
                        logger.warning(
                            f"refresh: dropping shard {decimal} from the temporal section "
                            f"— leaf {rel} did not read ({e})"
                        )
                        toc_failed.add(decimal)
                        contributions.pop(decimal, None)
                        got = None
                    if got is not None:
                        contributions.setdefault(decimal, []).append(got)
                # D15: windowed stamps carry the leaf's actual time range;
                # the rebuilt root summary re-derives the union from this
                # walk's stamps (truth), superseding any cached value.
                time_ranges.append(stamp.get("time_range"))
                continue
            is_digit_node = (
                _is_base_component(name) if prefix == "" else len(name) == 1 and name in "1234"
            )
            if is_digit_node:
                stack.append(rel + "/")
    _stale_warned.discard(root)
    if not decimals:
        from zagg.coverage_toc import delete_cover

        try:
            obstore.delete(store, ROOT_COVERAGE_NAME)
        except (FileNotFoundError, NotFoundError):
            pass
        # The §10.5 sibling goes with the carrier it refines (wholesale
        # discard; a foreign-revision object survives under succession).
        delete_cover(store_root, **store_kwargs)
        return None
    section = build_temporal_section(contributions, toc_fields, source="refresh")
    cover_sec = build_cover_section(contributions, toc_fields, order, source="refresh")
    if toc_failed:
        # Fail-open per leaf is fail-DESTRUCTIVE in aggregate. This walk PUTs
        # its envelope outright (no union, by design), so a section rebuilt
        # from a partial read publishes the losses as fact, and an all-failed
        # walk deletes the section entirely — at exactly the moment an
        # operator reached for the escape hatch because something was already
        # wrong. Compose with the standing section instead, through the same
        # §10.4 seam the sweep writes across.
        from zagg.coverage_toc import TEMPORAL_KEY, merge_cover_sections, merge_temporal_sections
        from zagg.hive import read_root_coverage

        standing = read_root_coverage(store_root, **store_kwargs)
        section = merge_temporal_sections(
            standing.get(TEMPORAL_KEY) if isinstance(standing, dict) else None, section
        )
        # The §10.5 sibling composes across the identical seam, for the
        # identical reason: an all-failed walk must not delete it, a partial
        # one must not publish the losses as fact.
        try:
            standing_cover = read_cover(store_root, **store_kwargs)
        except ValueError:
            standing_cover = None  # garbage cannot vouch for anything (D9)
        try:
            cover_sec = merge_cover_sections(standing_cover, cover_sec)
        except (KeyError, TypeError, ValueError):
            pass  # malformed standing cover: this walk's replaces it
        logger.warning(
            f"refresh: the temporal section is PARTIAL — {len(toc_failed)} shard(s) did "
            f"not read; composing with the standing section rather than replacing it"
        )
    if cover_sec is not None:
        # Sibling before marker, exactly as the sweep orders it. `replace`:
        # this walk is whole-store by construction, so it is the §10.5
        # authoritative rebuild (the partial case merged above already).
        written_cover = write_cover(store_root, cover_sec, replace=True, **store_kwargs)
        # Only ever stamp into a section at THIS revision. The merge above can
        # hand back a standing section at an unknown revision, preserved
        # VERBATIM under §10.4's succession rule — that producer may define
        # `cover` differently, and "verbatim" is the whole point of the rule.
        if (
            isinstance(section, dict)
            and section.get("spec") == TEMPORAL_COVERAGE_SPEC
            and isinstance(written_cover, dict)
            and isinstance(written_cover.get("spec"), str)
        ):
            section[COVER_KEY] = written_cover["spec"]
    # No `else` arm. A whole-store walk that produced no cover input has
    # proved nothing about the leaves — `toc_fields` is empty on a manifest
    # that merely lost its `cell_order`, and a store whose companions are not
    # written yet reads the same way — so the standing sibling stays standing,
    # the posture §10.5 states ("a producer with no temporal contribution
    # leaves the standing object untouched") and the one the sweep takes on
    # the same evidence through `write_cover(None)`. §10.5's delete licence is
    # scoped to the arm above, where no stamped leaf remains at all.
    envelope = build_root_coverage(
        morton_words_from_decimals(decimals),
        order,
        source="refresh",
        time_range=union_time_range(*time_ranges),
        temporal=section,
    )
    put_object(store, ROOT_COVERAGE_NAME, json.dumps(envelope, indent=1).encode())
    return envelope


__all__ = [
    "bitmap_and",
    "box_and",
    "load_coverage",
    "refresh_root_coverage",
    "root_coverage_and",
    "warn_if_stale",
]
