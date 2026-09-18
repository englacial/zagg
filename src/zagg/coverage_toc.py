"""The root coverage sidecar's temporal section — ``zagg-coverage-toc/1`` (issue #480).

Two tiers, both derived from the §8.3 per-centroid toc siblings a store
already carries, both riding the ONE object a reader already GETs to
bootstrap discovery (``{store_root}/coverage.moc``):

1. **the per-shard envelope word map** — one toc word per populated shard
   (``mortie.toc_reduce`` over that shard's sibling words, unioned across the
   store's temporal-carrying fields). This is the spatiotemporal pruning
   tier: "which shards hold data DURING my window" resolves from metadata,
   before any leaf is opened;
2. **the optional root counted cover** — observation counts per aligned
   time bucket (:class:`zagg.leaf_temporal.CountedCover`, §10.3; espg ruling
   of 2026-09-17 on issue #575, replacing the root time-digest): exact
   counts on the §10.5 bucket grid, composed by plain addition and coarsened
   by ancestor sums under the cap, so "how MUCH data in this window" answers
   exactly to bucket resolution with no merge law and no mass placed where
   nothing was observed.

Both are composed from per-leaf contributions — the leaf's own
``temporal.toc`` record where the worker wrote one (:mod:`zagg.leaf_temporal`),
else the leaf's raw §8.3 companions read back chunk by chunk
(:func:`read_leaf_temporal`).

A third surface lives BESIDE the bootstrap object (issue #489): the
**word-set cover sibling** ``{store_root}/coverage.toc`` — per shard, the
``mortie.toc_normalize`` canonical cover of that shard's companion words,
quantized to a spec-pinned temporal order (§10.5). Tier 1's one word makes
gaps invisible below shard granularity; the cover preserves them (the
grammar's never-bridge law) at bucket resolution, so "is there data in
``[t0, t1)``" resolves per shard without opening a leaf. It is a sibling
object, not a section: at CA scale it is ~1.5 MB against the ~0.1 MB
tier-1-carrying bootstrap object it sits beside (~15×), and spatial-only
readers must not pay that. ``coverage.moc``'s temporal
section carries only a presence marker for it (:data:`COVER_KEY`).

Absence composes: a store with no temporal channel writes no section, and
every accessor here returns ``None`` for it. That is never a refusal — the
standing absence posture of the sidecar grammars.

The writer rides :class:`zagg.sweep.MocFamily`, the walk that already visits
every leaf to roll the spatial coverage up to the root; this module owns the
per-leaf read, the fold, the section grammar, and the reader-side accessors
zagg's own tests need. The normative grammar is ``docs/specification.md``
§10.
"""

from __future__ import annotations

import base64
import logging

import numpy as np

logger = logging.getLogger(__name__)

#: The versioned key discipline: the section carries its own spec marker, so
#: a reader gates on it exactly as it gates the enclosing ``morton-moc/1``
#: envelope and refuses an unknown revision rather than guessing.
TEMPORAL_COVERAGE_SPEC = "zagg-coverage-toc/1"

#: The section's key on the root coverage envelope.
TEMPORAL_KEY = "temporal"

#: The §8.3 shape this section is derived from. A ``"per-cell"`` or
#: ``"coordinate"`` declaration is a different array grammar and contributes
#: nothing here (see the §10 open question).
PER_CENTROID = "per-centroid"

#: The word-set cover sibling's own spec marker (§10.5, issue #489) — an
#: OBJECT-level marker: the cover is a root sibling, not a section, so it
#: gates itself the way the carrier envelope does.
COVER_SPEC = "zagg-coverage-toc-cover/1"

#: The sibling object's name under the store root, beside ``coverage.moc``.
COVER_NAME = "coverage.toc"

#: The presence-marker key on the §10.1 temporal section: its value is the
#: sibling object's spec string, so a temporal consumer knows one GET ahead
#: that (and at which revision) a cover stands beside the root. A hint under
#: the sidecar staleness posture, never a promise.
COVER_KEY = "cover"

#: The §10.5 temporal order — the ONE rung the leaf record's counted cover,
#: the root cover sibling and the §10.3 root tier share: temporal order
#: ``o`` partitions the toc scale into ``2**o`` aligned buckets of
#: ``2**(63 - o)`` ns, and the pin is order 24 (span 2^39 ns ≈ 9.2 min;
#: espg amendment on issue #575, 2026-09-17, retiring the order-18 pin of
#: issue #489). Correctness is order-independent (quantization only widens)
#: and the sources are spikes — a pass crosses a shard in ~1 s, so a shard
#: has the same ~50 words at order 24 as at 18 — so the rung is chosen for
#: the CONSUMER: a gap floor of 2 × 2^39 ns ≈ 18.3 min and a cover-midpoint
#: epoch error of ±4.6 min keep consecutive orbits distinct and make the
#: closest-observation argmin (issue #509) exact. The 512-word cap is the
#: only coarsening mechanism; coarser effective rungs are data-driven, never
#: pinned. Bucket bounds stay exactly representable on the grammar's own
#: encoding grids for every order ≤ 31 (§10.5).
TEMPORAL_COVER_ORDER = 24

#: The §10.5 overflow cap: a shard's cover holds at most this many words.
#: A cover that lands above it coarsens by order (each step halves the
#: bucket count) until it fits — widening only, loudly recorded.
COVER_CAP = 512


def temporal_fields(manifest: dict | None) -> dict[str, dict]:
    """The store's temporal-carrying digest fields, from the manifest declaration.

    Keyed by payload field name, valued with the fold metadata plus the
    resolved ``sibling`` name. Discovery is declaration-driven — the same
    ``pyramid.overview.fields`` block :func:`zagg.sweep_overview.field_companions`
    reads — never a member enumeration of the leaf (a foreign or orphaned
    array prefix must not be able to steer this), and never a naming
    convention reconstructed by the reader. A store that declares no
    ``temporal`` field returns ``{}``, which is what makes the whole section
    absent for it.
    """
    from zagg.grids.base import ragged_times_name

    if not isinstance(manifest, dict):
        return {}
    decl = (manifest.get("pyramid") or {}).get("overview")
    fields = (decl or {}).get("fields") if isinstance(decl, dict) else None
    out: dict[str, dict] = {}
    for name, meta in (fields or {}).items():
        if not isinstance(meta, dict) or meta.get(TEMPORAL_KEY) != PER_CENTROID:
            continue
        out[name] = {**meta, "sibling": ragged_times_name(name)}
    return out


def temporal_cell_order(manifest: dict | None) -> int | None:
    """The manifest's ``cell_order`` — the leaf group the arrays live under.

    ``None`` when the key is absent or unparsable, and a caller MUST read that
    as "this store publishes no temporal coverage" rather than substitute a
    default. ``cell_order`` is a required manifest key, and a silent ``0``
    either asks for a group that does not exist — surfacing as an ordinary
    "no temporal contribution" log line, which hides the manifest problem —
    or, on a store whose cell order really IS 0, reads the wrong group and
    publishes words for it.
    """
    if not isinstance(manifest, dict):
        return None
    try:
        return int(manifest["cell_order"])
    except (KeyError, TypeError, ValueError):
        return None


def read_leaf_temporal(leaf_root: str, cell_order: int, fields: dict, **store_kwargs):
    """One leaf's contribution from its raw companions: ``(word, counts)`` or ``None``.

    Reads each declared field's payload and its §8.3 sibling by NAME (never a
    member enumeration), row-aligned per §1.1, ONE ragged chunk at a time
    (issue #575): the arrays are chunked at 4,096 rows, and each chunk is
    decoded, folded into a :class:`zagg.leaf_temporal.LeafTemporalAccumulator`
    and released, so memory is bounded by the chunk rather than the leaf — a
    CA-scale leaf is a million rows per field, which read whole is what
    killed the families pass at the 4 GB tier. The envelope word is
    ``toc_reduce`` over every sibling word the leaf holds, unioned across the
    declared fields — coverage as "any data". ``counts`` is the leaf's §10.3
    counted cover: each centroid counted at its envelope's representative
    instant (exact for a weight-1 centroid, the midpoint for a merged one)
    with the payload's own weight, so its total is the leaf's temporal
    observation count — where ``fields`` names more than one field, once per
    field. ``None`` when the leaf holds no temporal row at all (an unpopulated
    or pre-companion leaf), which is absence, not failure.

    This is the sweep's RAW route, for leaves written before the worker
    record existed; a leaf carrying ``temporal.toc`` is read from that
    instead (one small GET), and what this computes is what that record
    holds under ``source: "sweep"``.
    """
    import zarr

    from zagg.leaf_temporal import LeafTemporalAccumulator
    from zagg.store import open_store
    from zagg.sweep_overview import decode_digest

    group = zarr.open_group(
        open_store(leaf_root, **store_kwargs), path=str(cell_order), mode="r", zarr_format=3
    )
    acc = LeafTemporalAccumulator()
    for name in sorted(fields):
        meta = fields[name]
        try:
            sibling, payload = group[meta["sibling"]], group[name]
        except KeyError:
            # Schema evolution: the field postdates this leaf. It contributes
            # nothing, exactly as the pyramid fold treats the same gap.
            logger.debug(f"coverage[toc]: leaf {leaf_root} lacks field {name!r}")
            continue
        dtype = meta.get("dtype") or "float32"
        n = int(payload.shape[0])
        if int(sibling.shape[0]) != n:
            # A SHORT companion aligns row for row over its own length, so the
            # per-cell check below never fires: the leaf would publish a word
            # joined over a PREFIX of its cells and be listed as complete. This
            # is the truncated-array shape the read path refuses everywhere
            # else (issue #452); refuse it here too, per shard (§10.2).
            raise ValueError(
                f"{meta['sibling']} has {int(sibling.shape[0])} rows for a "
                f"{n}-row {name} payload — the companion must be "
                f"row-aligned with its digest (spec §1.1)"
            )
        step = int(sibling.chunks[0]) or n
        for start in range(0, n, step):
            raw_words = sibling[start : start + step]
            raw_payload = payload[start : start + step]
            words_parts: list[np.ndarray] = []
            weight_parts: list[np.ndarray] = []
            for i, row in enumerate(raw_words):
                if row is None or not len(row):
                    continue
                words = decode_digest(row, "uint64", ())
                cell = decode_digest(raw_payload[i], dtype, (2,))
                if len(cell) != len(words):
                    raise ValueError(
                        f"{meta['sibling']} cell {start + i} has {len(words)} words for a "
                        f"{len(cell)}-centroid payload — the companion must be row-aligned "
                        f"with its digest (spec §1.1)"
                    )
                words_parts.append(words)
                weight_parts.append(cell[:, 1])
            if words_parts:
                acc.add_weighted(np.concatenate(words_parts), np.concatenate(weight_parts))
    return acc.finish()


def build_temporal_section(contributions: dict, fields, *, source: str = "sweep") -> dict | None:
    """The ``zagg-coverage-toc/1`` section from per-leaf contributions.

    ``contributions`` maps a shard's D1 decimal id to the LIST of
    ``(word, counts)`` tuples — :func:`read_leaf_temporal`'s return, or a
    leaf record's (:func:`zagg.leaf_temporal.leaf_temporal_contribution`) —
    one per window leaf, so a windowed shard's several leaves reduce to the
    one envelope word the shard-keyed map holds. Returns ``None`` for an
    empty map: a store with no temporal channel gets no section, and its root
    object stays byte-identical to a pre-#480 one.

    The root counted cover is the per-word SUM over every leaf's counts
    (:func:`zagg.leaf_temporal.merge_counts` — exact, order-independent, no
    merge law) coarsened by whole orders to the §10.5 cap
    (:func:`zagg.leaf_temporal.cap_counts`, ancestor sums — exact at the
    coarser rung). Every leaf of a shard contributes, so the total is the
    store's temporal observation count.
    """
    from mortie import toc_reduce

    from zagg.hive import _utcnow
    from zagg.leaf_temporal import cap_counts, encode_counts, merge_counts

    if not contributions:
        return None
    shards: dict[str, int] = {}
    parts = []
    for decimal in sorted(contributions):
        leaves = contributions[decimal]
        shards[decimal] = int(toc_reduce(np.asarray([p[0] for p in leaves], dtype=np.uint64)))
        parts.extend(p[1] for p in leaves)
    section = {
        "spec": TEMPORAL_COVERAGE_SPEC,
        "source": source,
        "generated_at": _utcnow(),
        "fields": sorted(fields),
        "shards": {d: str(w) for d, w in sorted(shards.items())},
    }
    counts = cap_counts(merge_counts(parts))
    if counts.words.size:
        section["counts"] = encode_counts(counts)
    return section


def merge_temporal_sections(existing, incoming) -> dict | None:
    """Compose two temporal sections for the root object's GET-union-PUT.

    Tier 1 unions elementwise under the grammar's ``toc_merge`` join —
    idempotent and exact, so a re-sweep of unchanged leaves reproduces the
    same words. Tier 2 is deliberately NOT summed at this seam: its values
    are observation counts, and adding two covers over overlapping shard
    sets would double-count them. It is REPLACED instead, and only by a
    producer that covered every shard the merged map lists; a partial
    producer drops it and leaves tier 1 standing (§10).

    ``fields`` unions with tier 1, because it is the provenance of the SHARD
    MAP and the map is the thing that unions. It is deliberately not narrowed
    to the surviving counts' own fields: doing so would describe the map with
    a list that no longer covers it. §10.1 says so, and makes the list an
    upper bound for the once-per-field count rule rather than an exact
    description of the installed block.

    An unknown-spec section on the INCOMING side contributes nothing — the
    same strict gate the enclosing envelope uses. On the EXISTING side it is
    kept **verbatim** instead: this function is the write composer, and a
    ``None`` return deletes the key. A section at a revision this zagg cannot
    read was written by a producer that knew more than this one, so it is
    neither dropped by a producer with no contribution nor downgraded by one
    carrying a ``zagg-coverage-toc/1`` section — the page's standing rule that
    readers add revisions and never drop them (§10.4).
    """
    from mortie import toc_merge

    a, b = _usable(existing), _usable(incoming)
    if b is None:
        return dict(a) if a is not None else _preserved(existing)
    if a is None:
        newer = _preserved(existing)
        if newer is not None:
            logger.warning(
                f"coverage[toc]: keeping the standing {newer.get('spec')!r} section — "
                f"{TEMPORAL_COVERAGE_SPEC} does not read it and MUST NOT downgrade it"
            )
            return newer
        return dict(b)
    shards = {d: int(w) for d, w in (a.get("shards") or {}).items()}
    for d, w in (b.get("shards") or {}).items():
        prior = shards.get(d)
        shards[d] = int(w) if prior is None else int(toc_merge(prior, int(w)))
    merged = {
        "spec": TEMPORAL_COVERAGE_SPEC,
        "source": b.get("source", a.get("source")),
        "generated_at": b.get("generated_at", a.get("generated_at")),
        "fields": sorted(set(a.get("fields") or []) | set(b.get("fields") or [])),
        "shards": {d: str(w) for d, w in sorted(shards.items())},
    }
    # Whole-coverage test, newest producer first: only a section whose own map
    # listed every shard in the union can vouch for store-wide counts.
    listed = set(merged["shards"])
    for side in (b, a):
        if side.get("counts") is not None and set(side.get("shards") or {}) >= listed:
            merged["counts"] = side["counts"]
            break
    # The §10.5 presence marker carries through the seam (§10.4): the sibling
    # object composes under its own merge, so a producer that wrote no cover
    # must not erase the standing pointer to one. Incoming wins, with one
    # exception — the succession rule applied to the marker itself: a standing
    # marker naming a cover revision this producer cannot read still describes
    # the object that is actually there, because `write_cover` preserves such
    # an object on BOTH of its paths. Downgrading the marker to `COVER_SPEC`
    # would make the carrier advertise a revision the sibling does not carry
    # (§10.1 defines the value as that object's own `spec`), and the sweep and
    # the refresh would then take turns re-PUTting the root object forever.
    marker = b.get(COVER_KEY, a.get(COVER_KEY))
    standing = a.get(COVER_KEY)
    if marker == COVER_SPEC and isinstance(standing, str) and standing not in ("", COVER_SPEC):
        marker = standing
    if marker is not None:
        merged[COVER_KEY] = marker
    return merged


def section_unchanged(existing, incoming) -> bool:
    """Whether composing ``incoming`` into ``existing`` would change nothing.

    The temporal half of the MOC family's skip-if-current test: an unchanged
    re-sweep of a temporal store must write no root object either. The test is
    on what would actually be **written** — :func:`merge_temporal_sections`'s
    own output — compared against the standing section on CONTENT (the shard
    words, the counts block, the field list), never on the whole section:
    ``source`` and ``generated_at`` churn per pass by construction.

    Testing the merge rather than the inputs is what makes it converge. A
    producer that walked only part of the store always builds counts, and
    §10.4 always drops that block at the seam; a test asking "does the
    standing section already carry everything this one holds" therefore
    answers *no* forever on any store with more than one shard, and every
    incremental sweep re-PUTs a byte-identical object.
    """
    merged = merge_temporal_sections(existing, incoming)
    return _content(merged) == _content(existing if isinstance(existing, dict) else None)


def _content(section) -> tuple | None:
    """The part of a section a no-op pass must reproduce exactly."""
    if section is None:
        return None
    return (
        {d: str(w) for d, w in (section.get("shards") or {}).items()},
        section.get("counts"),
        sorted(section.get("fields") or []),
        section.get(COVER_KEY),
    )


def _usable(section) -> dict | None:
    """A section dict at the spec revision this module implements, else ``None``."""
    if not isinstance(section, dict) or section.get("spec") != TEMPORAL_COVERAGE_SPEC:
        if section is not None:
            logger.debug("coverage[toc]: ignoring a section with an unknown spec")
        return None
    return section


def _preserved(section) -> dict | None:
    """A standing section carrying a spec MARKER this revision does not implement.

    Distinguished from plain malformation: a marked section is some future
    revision's, and §10.4 keeps it verbatim rather than clobber it. An
    unmarked carrier (no ``spec``, or a non-string one) claims no revision,
    so it is debris a producer may legitimately replace — otherwise one bad
    write would wedge the key shut forever.
    """
    if not isinstance(section, dict):
        return None
    spec = section.get("spec")
    if isinstance(spec, str) and spec and spec != TEMPORAL_COVERAGE_SPEC:
        return dict(section)
    return None


# ---------------------------------------------------------------------------
# The word-set cover sibling — `zagg-coverage-toc-cover/1` (§10.5, issue
# #489). A root object BESIDE the bootstrap sidecar, GET-on-demand by
# temporal consumers only: per shard, the canonical gap-preserving cover of
# its companion words, quantized to the pinned cover order.
# ---------------------------------------------------------------------------


def quantize_words(words, order: int = TEMPORAL_COVER_ORDER) -> np.ndarray:
    """The §10.5 quantization: toc words, widened to aligned order-``o`` buckets.

    Temporal order ``o`` partitions the toc scale (2^63 ns from the grammar's
    1850 epoch) into ``2**o`` aligned buckets of ``2**(63 - o)`` ns. Each
    input word's conservative envelope (``toc2time``) is widened to the
    bucket grid — start floored, end ceiled — re-encoded as a range word,
    and the set canonicalized with ``toc_normalize``. Bucket bounds are
    exactly representable on the grammar's own grids for every ``o <= 31``
    (a bucket start is a multiple of 2^31 ns and its end of 2^32 ns), so the
    encoding round-trip adds no rounding of its own.

    **Widening only.** The output's coverage contains the input's — a cover
    may over-claim, never false-negative — and the never-bridge law holds at
    bucket resolution: **a gap survives iff it contains a whole ALIGNED
    bucket.** Widening puts the instants either side of a gap into their own
    buckets, and ``toc_normalize`` coalesces ranges that merely abut, so a
    gap of one bucket span never survives (its two buckets abut) and only a
    gap that leaves an entire bucket uncovered does. The guaranteed floor is
    therefore TWO bucket spans — at the pinned cover order, 2 × 2^45 ns ≈ 19.5 h
    — with the ``[1, 2)``-span band decided by where the data falls on the
    grid, not by the gap's length. The one clamp is the scale ceiling:
    the top bucket's end exceeds the grammar's maximum encodable end
    (``mortie.TOC_MAX_NS``), so it is clamped there — still containing every
    encodable input word, because no encoder-produced envelope reaches past
    the ceiling either.

    Quantization commutes with union and with the envelope join
    (``toc_reduce``), which is what makes the per-leaf fold exact
    (:func:`read_leaf_temporal`) and the §10.5 parity invariant testable.
    Junk words carry the grammar's own posture: garbage in, garbage out,
    deterministically.
    """
    from mortie import TOC_MAX_NS, span2toc, toc2time, toc_normalize

    if not 0 <= int(order) <= 31:
        raise ValueError(
            f"temporal order {order} is outside [0, 31] — coarser than the scale, or a "
            f"bucket finer than the grammar's 2^32 ns end grid can encode (spec §10.5)"
        )
    words = np.asarray(words, dtype=np.uint64)
    if words.size == 0:
        return words
    k = np.uint64(63 - int(order))
    start, end = toc2time(words)
    start = np.atleast_1d(np.asarray(start, dtype=np.uint64))
    end = np.atleast_1d(np.asarray(end, dtype=np.uint64))
    lo = (start >> k) << k
    # A timestamp decodes to (t, t); a range's decoded end is exclusive. The
    # last covered instant is therefore max(end, start + 1) - 1, uniformly.
    last = np.maximum(end, start + np.uint64(1)) - np.uint64(1)
    hi = np.minimum(((last >> k) + np.uint64(1)) << k, np.uint64(TOC_MAX_NS))
    return toc_normalize(span2toc(lo, hi - np.uint64(1)))


def _cap_cover(cover: np.ndarray, order: int, cap: int = COVER_CAP) -> tuple[np.ndarray, int]:
    """Coarsen a cover by order until it fits the §10.5 cap.

    Each step halves the bucket count (order − 1 doubles the span), so the
    loop terminates: order 0 is a single bucket. The same widening law as
    the quantization itself — never a truncation of the word list, which
    would silently drop coverage.
    """
    while len(cover) > cap and order > 0:
        order -= 1
        cover = quantize_words(cover, order)
    return cover, order


def build_cover_section(contributions: dict, fields, shard_order: int, *, source="sweep"):
    """The ``zagg-coverage-toc-cover/1`` object body from per-leaf contributions.

    Same ``contributions`` mapping :func:`build_temporal_section` folds —
    this derives each shard's word set from its leaves' counted covers: the
    per-word sum over the window leaves, capped, then coarsened to the pinned
    cover order and normalized (:func:`zagg.leaf_temporal.cover_from_counts`
    — byte-equal to :func:`quantize_words` over the same instants, so the
    §10.5 bytes are what they were before the counts existed). ``None`` for
    an empty map — the standing absence rule, so a store with no temporal
    channel gets no sibling object at all.

    A shard that had to coarsen below :data:`TEMPORAL_COVER_ORDER` records the
    order it landed at in its own block (``temporal_order``), and the
    coarsening is logged — §10.5's "widening only, loudly recorded".
    """
    from zagg.hive import _utcnow
    from zagg.leaf_temporal import cap_counts, cover_from_counts, merge_counts

    if not contributions:
        return None
    shards: dict[str, dict] = {}
    for decimal in sorted(contributions):
        counts = cap_counts(merge_counts([p[1] for p in contributions[decimal]]))
        cover, order = cover_from_counts(counts)
        if order != TEMPORAL_COVER_ORDER:
            logger.warning(
                f"coverage[toc]: shard {decimal} cover coarsened to temporal order {order} "
                f"(span 2^{63 - order} ns) to fit the {COVER_CAP}-word cap (spec §10.5)"
            )
        shards[decimal] = _encode_cover_block(cover, order)
    return {
        "spec": COVER_SPEC,
        "source": source,
        "generated_at": _utcnow(),
        "order": int(shard_order),
        "temporal_order": TEMPORAL_COVER_ORDER,
        "cap": COVER_CAP,
        "fields": sorted(fields),
        "element": {"dtype": "uint64", "shape": [-1]},
        "encoding": "base64",
        "shards": shards,
    }


def _encode_cover_block(cover: np.ndarray, order: int, pinned: int = TEMPORAL_COVER_ORDER) -> dict:
    """One shard's block: base64 of the §1.4 element bytes, plus its k.

    ``count`` is the §10.5 MUST-check (the §10.3 ``centroids`` rule, one
    buffer instead of two); ``temporal_order`` appears only when the shard
    coarsened below ``pinned``, the ENCLOSING object's pinned order —
    absence means that pin, so the key and the object's declaration have to
    be written against the same number.
    """
    from zagg.sweep_overview import encode_digest

    block = {
        "words": base64.b64encode(encode_digest(np.asarray(cover, np.uint64), "uint64")).decode(
            "ascii"
        ),
        "count": int(len(cover)),
    }
    if int(order) != int(pinned):
        block["temporal_order"] = int(order)
    return block


def _object_pin(section) -> int:
    """A cover object's declared ``temporal_order``, or this revision's pin.

    §10.5 defines a block's missing ``temporal_order`` relative to the OBJECT
    ("absence means the pinned order"), not to whatever this build happens to
    pin, so every decode resolves through here.
    """
    pinned = (section or {}).get("temporal_order", TEMPORAL_COVER_ORDER)
    try:
        return int(pinned)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"cover declares a non-integer temporal_order {pinned!r} (spec §10.5)"
        ) from e


def _decode_cover_block(decimal: str, block, pinned: int = TEMPORAL_COVER_ORDER):
    """One shard's ``(words, temporal_order)``, MUST-checked against ``count``.

    ``pinned`` is the enclosing object's declared order: it supplies the
    default for a block that omits its own, and it is the ceiling §10.5's
    "Always ≤ the object's ``temporal_order``" makes a MUST.
    """
    from zagg.sweep_overview import decode_digest

    if not isinstance(block, dict):
        raise ValueError(f"cover shard {decimal} block is not an object (spec §10.5)")
    words = decode_digest(base64.b64decode(block["words"]), "uint64", ())
    if block.get("count") != len(words):
        raise ValueError(
            f"cover shard {decimal} declares {block.get('count')!r} words and decodes "
            f"{len(words)} — the block must agree with its own buffer (spec §10.5)"
        )
    order = int(block.get("temporal_order", pinned))
    if order > int(pinned):
        raise ValueError(
            f"cover shard {decimal} declares temporal order {order} above the object's "
            f"pinned {int(pinned)} — a block only ever coarsens BELOW the pin (spec §10.5)"
        )
    return words, order


def merge_cover_sections(existing, incoming):
    """Compose two cover bodies for the sibling object's GET-union-PUT.

    Per shard: the union of the two word sets, requantized at the coarser of
    the two recorded orders (union alone could interleave two orders' bucket
    bounds, and the §10.5 parity invariant is a claim at ONE order), then
    re-capped — which may coarsen further, under the same widening law. A
    shard on one side carries over unchanged. The unknown-revision rules are
    the §10.4 ones verbatim: an incoming unknown revision contributes
    nothing; a standing one is preserved and never downgraded.

    Shard ``order`` gates the union the way the carrier's does
    (:func:`zagg.hive.write_root_coverage`): D1 ids at two different orders
    are not comparable, so a standing object at another order is incompatible
    debris and the incoming side wins wholesale — the regenerable-cache
    posture, and the behavior a re-shard needs if the sibling is to stay in
    step with the ``coverage.moc`` that already replaces itself.
    """
    a, b = _cover_usable(existing), _cover_usable(incoming)
    if b is None:
        return dict(a) if a is not None else _cover_preserved(existing)
    if a is None:
        newer = _cover_preserved(existing)
        if newer is not None:
            logger.warning(
                f"coverage[toc]: keeping the standing {newer.get('spec')!r} cover — "
                f"{COVER_SPEC} does not read it and MUST NOT downgrade it"
            )
            return newer
        return dict(b)
    if a.get("order") != b.get("order"):
        logger.warning(
            f"coverage[toc]: standing cover is at shard order {a.get('order')!r} and the "
            f"incoming one at {b.get('order')!r}; overwriting (regenerable cache) — "
            f"D1 ids at two orders are not comparable"
        )
        return dict(b)
    # Each side's blocks decode against ITS OWN declared pin; the composed
    # object pins at the finer of the two, which is the only value every
    # surviving block's order is still ≤ (§10.5's "always ≤ the object's").
    pins = (_object_pin(a), _object_pin(b))
    pinned = max(pins)
    shards: dict[str, dict] = {}
    for decimal in sorted(set(a.get("shards") or {}) | set(b.get("shards") or {})):
        sides = [
            _decode_cover_block(decimal, side["shards"][decimal], pin)
            for side, pin in zip((a, b), pins, strict=True)
            if decimal in (side.get("shards") or {})
        ]
        if len(sides) == 1:
            ((words, order),) = sides
        else:
            order = min(o for _, o in sides)
            words = quantize_words(np.concatenate([w for w, _ in sides]), order)
        words, order = _cap_cover(words, order)
        shards[decimal] = _encode_cover_block(words, order, pinned)
    return {
        "spec": COVER_SPEC,
        "source": b.get("source", a.get("source")),
        "generated_at": b.get("generated_at", a.get("generated_at")),
        "order": b.get("order", a.get("order")),
        "temporal_order": pinned,
        "cap": COVER_CAP,
        "fields": sorted(set(a.get("fields") or []) | set(b.get("fields") or [])),
        "element": {"dtype": "uint64", "shape": [-1]},
        "encoding": "base64",
        "shards": shards,
    }


def cover_unchanged(existing, incoming) -> bool:
    """Whether composing ``incoming`` into ``existing`` would change nothing.

    The sibling object's half of the MOC family's skip-if-current test,
    built exactly as :func:`section_unchanged` is: on the MERGE's content
    (the per-shard blocks and the field list), never on the carrier fields
    that churn per pass. A standing object the merge cannot decode answers
    ``False`` — the write seam replaces it, so the test must not be the thing
    that raises first.
    """
    try:
        merged = merge_cover_sections(existing, incoming)
    except (KeyError, TypeError, ValueError):
        # A standing object that is JSON but not a decodable cover cannot be
        # "already current": the writer's own fail-open arm is about to
        # replace it, so report the change rather than raise here (§10.5's
        # regenerable-cache posture).
        return False
    return _cover_content(merged) == _cover_content(
        existing if isinstance(existing, dict) else None
    )


def _cover_content(section) -> tuple | None:
    if section is None:
        return None
    shards = {
        d: (b.get("words"), b.get("count"), b.get("temporal_order"))
        for d, b in (section.get("shards") or {}).items()
        if isinstance(b, dict)
    }
    # The two pins are content, not provenance: a block's absent
    # `temporal_order` means "the object's", so an object at another pin says
    # something different with the same bytes, and a cap change is a producer
    # policy change the next pass must re-express.
    return (
        shards,
        sorted(section.get("fields") or []),
        section.get("order"),
        section.get("temporal_order"),
        section.get("cap"),
    )


def _cover_usable(section) -> dict | None:
    if not isinstance(section, dict) or section.get("spec") != COVER_SPEC:
        if section is not None:
            logger.debug("coverage[toc]: ignoring a cover with an unknown spec")
        return None
    return section


def _cover_preserved(section) -> dict | None:
    """A standing cover at a spec marker this revision does not implement."""
    if not isinstance(section, dict):
        return None
    spec = section.get("spec")
    if isinstance(spec, str) and spec and spec != COVER_SPEC:
        return dict(section)
    return None


def write_cover(store_root: str, section: dict | None, *, replace: bool = False, **store_kwargs):
    """PUT the ``coverage.toc`` sibling, composing across the standing object.

    The default is the same GET-union-PUT seam ``coverage.moc`` rides
    (incremental sweeps accumulate; concurrent runs race benignly, last
    writer wins, D9). ``replace=True`` is the refresh escape hatch: the
    caller's walk is authoritative and the standing object is discarded —
    except a standing cover at an UNKNOWN revision, which §10.4's succession
    rule preserves on both paths (a producer never downgrades what it cannot
    read). An unparsable standing object is logged and overwritten, the
    regenerable-cache posture — "unparsable" covering a standing object that
    is valid JSON but not a decodable cover (a block disagreeing with its own
    ``count``, a missing or corrupt ``words`` buffer, a non-object block) as
    well as one that is not JSON at all: the truth is in the leaves, and this
    writer runs inside the sweep's spatial rollup, which one corrupt byte in
    an accelerator must not take down. Returns what was left standing.

    A ``None`` section — :func:`build_cover_section`'s empty-walk answer — is
    a no-op on BOTH arms: §10.5's "a producer with no temporal contribution
    leaves the standing object untouched", so the refresh escape hatch over a
    store with no temporal channel writes nothing rather than clobbering (or
    crashing on ``dict(None)``).
    """
    import json

    from zagg.hive import _read_json, open_object_store
    from zagg.store import put_object

    store = open_object_store(store_root, **store_kwargs)
    try:
        existing = _read_json(store, COVER_NAME)
    except ValueError:
        logger.warning(
            f"existing {COVER_NAME} at {store_root} is not JSON; overwriting "
            f"(regenerable cache — the sweep is the authoritative rebuilder)"
        )
        existing = None
    if section is None:
        # Nothing to say. The default arm reaches the same answer through
        # `merge_cover_sections(existing, None)`; short-circuiting keeps the
        # two arms in agreement and skips a PUT that would only rewrite the
        # standing bytes with themselves.
        return existing if isinstance(existing, dict) else None
    if replace:
        preserved = _cover_preserved(existing)
        if preserved is not None:
            logger.warning(
                f"coverage[toc]: keeping the standing {preserved.get('spec')!r} cover at "
                f"{store_root} — {COVER_SPEC} does not read it and MUST NOT downgrade it"
            )
        merged = preserved if preserved is not None else dict(section)
    else:
        try:
            merged = merge_cover_sections(existing, section)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(
                f"existing {COVER_NAME} at {store_root} failed to parse ({e}); "
                f"overwriting (regenerable cache — the sweep rebuilds authoritatively)"
            )
            merged = dict(section)
    if merged is None:
        return None
    put_object(store, COVER_NAME, json.dumps(merged, indent=1).encode())
    return merged


def read_cover(store_root: str, **store_kwargs):
    """Read the store-root ``coverage.toc``; ``None`` when absent.

    Raises ``ValueError`` on garbage JSON, exactly as the carrier's reader
    does — a corrupt cache must be loud, never a plausible partial answer.
    """
    from zagg.hive import _read_json, open_object_store

    return _read_json(open_object_store(store_root, **store_kwargs), COVER_NAME)


def delete_cover(store_root: str, **store_kwargs) -> bool:
    """Discard the sibling with an authoritative wholesale rebuild (§10.5).

    The refresh escape hatch's arm for a walk that left no stamped leaf: the
    stale object goes with the carrier it refined. A standing object at an
    UNKNOWN revision survives (the §10.4 succession rule — a producer never
    deletes what it cannot read); garbage JSON is debris and goes.

    An object the read found ABSENT is answered from the read: no DELETE is
    issued, so a refresh of a non-temporal store costs one GET rather than a
    GET and a pointless DELETE. The return is best-effort and
    backend-dependent under a race — read and delete are two requests, and
    S3's ``DeleteObject`` is idempotent, so an object that appears between
    them (or vanishes) is reported by whichever request saw it.
    """
    import obstore
    from obstore.exceptions import NotFoundError

    from zagg.hive import _read_json, open_object_store

    store = open_object_store(store_root, **store_kwargs)
    try:
        existing = _read_json(store, COVER_NAME)
        garbage = False
    except ValueError:
        existing, garbage = None, True  # debris — delete below
    if existing is None and not garbage:
        return False  # nothing there: the read already answered
    if _cover_preserved(existing) is not None:
        logger.warning(
            f"coverage[toc]: keeping the standing {existing.get('spec')!r} cover at "
            f"{store_root} — {COVER_SPEC} does not read it and MUST NOT delete it"
        )
        return False
    try:
        obstore.delete(store, COVER_NAME)
        return True
    except (FileNotFoundError, NotFoundError):
        return False


# ---------------------------------------------------------------------------
# Reader side — the minimum zagg's own tests (and demos) need. The external
# reader's `coverage_toc` / `when=` surface is espg/moczarr#45, decoded from
# the spec text and the §7 fixtures alone.
# ---------------------------------------------------------------------------


def load_temporal_coverage(envelope) -> dict | None:
    """The temporal section of a root coverage envelope, or ``None``.

    Absence — no section, an unknown spec revision, a malformed carrier — all
    read as ``None``. A store with no temporal channel is the common case and
    is never an error.
    """
    if not isinstance(envelope, dict):
        return None
    return _usable(envelope.get(TEMPORAL_KEY))


def coverage_toc(envelope) -> dict[str, int] | None:
    """The per-shard envelope word map, ``{shard decimal: toc word}``.

    ``None`` when the store carries no temporal section. Words come back as
    Python ints (the JSON carries decimal STRINGS — a uint64 exceeds 2^53 and
    a float-based parser would mangle a raw number, the same rule the spatial
    ranges follow).
    """
    section = load_temporal_coverage(envelope)
    if section is None:
        return None
    return {d: int(w) for d, w in (section.get("shards") or {}).items()}


def coverage_toc_counts(envelope):
    """The root counted cover as a :class:`zagg.leaf_temporal.CountedCover`, or ``None``.

    §10.3's MUST-checks ride :func:`zagg.leaf_temporal.decode_counts`: the
    two buffers against each other and against the block's ``count``, the
    counts against ``obs_total``, the order against the pin. A block that
    fails them is broken, not decorative — and a reference accessor that
    skipped the check would leave the external reader (moczarr) implementing
    one zagg does not.
    """
    section = load_temporal_coverage(envelope)
    block = (section or {}).get("counts")
    if not isinstance(block, dict):
        return None
    from zagg.leaf_temporal import decode_counts

    return decode_counts(block)


def load_cover(obj) -> dict | None:
    """A ``coverage.toc`` body at the revision this module implements, else ``None``.

    The strict-gate-then-degrade rule at object level: an unknown revision, a
    non-dict carrier, or plain absence all read as ``None`` — the cover is an
    accelerator whose truth is in the leaves, so degraded means "fall back to
    tier 1 (or open leaves)", never a refusal.
    """
    return _cover_usable(obj)


def cover_words(obj) -> dict[str, np.ndarray] | None:
    """The decoded per-shard word sets, ``{shard decimal: uint64 words}``.

    ``None`` when ``obj`` is not a readable cover; a readable cover holding a
    broken BLOCK raises, which makes this the conformance surface — the
    query path (:func:`shards_overlapping`) degrades that one shard to tier 1
    instead. Each block's ``count`` is
    MUST-checked against its own buffer (§10.5) — the same rule
    :func:`coverage_toc_counts` applies to the counts block's ``count``.
    A block that omits ``temporal_order`` decodes at the OBJECT's declared
    pin (not this build's), and one declaring an order above that pin is
    refused: §10.5's "always ≤ the object's ``temporal_order``".
    """
    section = load_cover(obj)
    if section is None:
        return None
    pinned = _object_pin(section)
    return {
        d: _decode_cover_block(d, block, pinned)[0]
        for d, block in (section.get("shards") or {}).items()
    }


def _query_word_set(section, decimal: str):
    """One shard's cover words for the QUERY path, or ``None`` if unreadable.

    The tolerant twin of :func:`cover_words`' strict decode: pruning must
    never fail on debris, so a block that breaks §10.5's rules degrades this
    one shard to tier 1 instead of raising the whole query.
    """
    try:
        return _decode_cover_block(decimal, section["shards"][decimal], _object_pin(section))[0]
    except (KeyError, TypeError, ValueError) as e:
        logger.debug(
            f"coverage[toc]: cover shard {decimal} does not decode ({e}) — tier 1 decides it"
        )
        return None


def shards_overlapping(envelope, q_start_ns: int, q_end_ns: int, *, cover=None) -> list[str] | None:
    """Shard ids whose temporal claim intersects ``[q_start_ns, q_end_ns)``.

    The pruning answer, on the grammar's own predicate
    (``mortie.toc_overlaps``): conservative — it never under-reports, and may
    over-report by up to one quantum at a window edge. ``None`` only when
    the store offers NEITHER a readable temporal section NOR a readable
    ``cover``, which a caller MUST read as "no temporal information", never
    as "no shards" — a section-less carrier with a standing cover still
    answers (from the cover, under the seam rules below), because there is
    temporal information to answer from.

    ``cover`` upgrades the answer to the §10.5 word SET (issue #489): pass
    the ``coverage.toc`` body (:func:`read_cover`'s return) and a shard the
    cover lists is tested against its whole word set — so a window that
    falls in a gap BETWEEN a shard's campaigns no longer selects it, which
    is the entire point of the sibling. Widening is the only cover direction
    (§10.5), so the upgraded answer is still conservative against the true
    observations: over-report bounded by the quantization bucket at a
    cluster edge, under-report never.

    The two maps meet at a staleness seam, and §10.5 rules it three ways by
    which object lists the shard:

    * **Both** — the cover's word set decides. The carrier's envelope word is
      the join of that set, so the set is the strictly tighter claim, and
      pruning a gap window here is the whole point of the sibling.
    * **Tier 1 only** — the envelope word decides ("a shard the cover does
      not list is unknown rather than empty": unknown stays a candidate at
      the resolution the carrier does have).
    * **The cover only** — an unconditional candidate. §10.5 composes such a
      shard "under the standing staleness posture — unknown, a candidate,
      never authoritative", because a cover-only listing is exactly the seam
      where the sibling may be arbitrarily older than the carrier (a refresh
      whose walk found no temporal input replaces ``coverage.moc`` and leaves
      the standing sibling untouched), and a stale word set MUST NOT prune.

    A ``cover`` at an unknown revision degrades to tier 1 wholesale — exactly
    ``cover=None`` — per §10.5's strict-gate-then-degrade rule, and so does a
    cover whose declared shard ``order`` differs from the carrier's: D1 ids
    at two orders are not comparable, so a foreign-order sibling is debris
    that must neither decide a shard nor contribute ids of its own (the gate
    :func:`merge_cover_sections` applies on the write side, logged the same
    way).

    Degradation is per shard below the object gate: a block that does not
    decode — §10.5's ``count`` MUST-check, a block above the object's pin, a
    buffer that is not a buffer — falls back to that shard's tier-1 word
    (a candidate if tier 1 does not list it), logged at debug. So does a
    both-listed block that decodes to an EMPTY word set: the widening law
    forbids an empty cover for a shard that has data, so that block is
    malformed and the shard falls back rather than under-reporting. This
    query surface never raises on a corrupt sibling — the cover is an
    accelerator whose truth is in the leaves, and §10.5 calls a broken one
    "debris". The strict reading of the same bytes is
    :func:`cover_words`, which is the conformance surface and DOES raise.
    """
    from mortie import toc_overlaps

    words = coverage_toc(envelope)
    section = load_cover(cover) if cover is not None else None
    e_order = envelope.get("order") if isinstance(envelope, dict) else None
    c_order = (section or {}).get("order")
    if section is not None and None not in (c_order, e_order) and c_order != e_order:
        logger.warning(
            f"coverage[toc]: ignoring the cover at shard order {c_order!r} — the carrier is "
            f"at order {e_order!r}; D1 ids at two orders are not comparable"
        )
        section = None
    blocks = (section or {}).get("shards") or {}
    if words is None and section is None:
        return None
    words = words or {}
    hits, tier, cov_keys, cov_sets = {}, [], [], []
    for key in sorted(words.keys() | blocks.keys()):
        if key not in words:
            # Cover-only: the fresher carrier has never listed it. Unknown,
            # never authoritative — a candidate for every window (§10.5).
            hits[key] = True
            continue
        cover_set = _query_word_set(section, key) if key in blocks else None
        if cover_set is not None and len(cover_set):
            cov_keys.append(key)
            cov_sets.append(np.asarray(cover_set, np.uint64))
        else:
            # A word each: one batched call, not one FFI hop per shard.
            tier.append(key)
    if cov_keys:
        # Word sets too: one call over the concatenation, then a segmented OR.
        # Every set here is non-empty (an empty one degraded to tier 1 above),
        # so no segment can swallow the next shard's verdict.
        offsets = np.cumsum([0] + [len(a) for a in cov_sets[:-1]])
        batch = np.atleast_1d(
            np.asarray(toc_overlaps(np.concatenate(cov_sets), q_start_ns, q_end_ns))
        )
        merged = np.add.reduceat(batch.astype(np.int64), offsets) > 0
        hits.update(zip(cov_keys, (bool(h) for h in merged), strict=True))
    if tier:
        batch = np.atleast_1d(
            np.asarray(
                toc_overlaps(np.asarray([words[k] for k in tier], np.uint64), q_start_ns, q_end_ns)
            )
        )
        hits.update(zip(tier, (bool(h) for h in batch), strict=True))
    return sorted(k for k, hit in hits.items() if hit)


__all__ = [
    "COVER_CAP",
    "COVER_KEY",
    "COVER_NAME",
    "COVER_SPEC",
    "PER_CENTROID",
    "TEMPORAL_COVER_ORDER",
    "TEMPORAL_COVERAGE_SPEC",
    "TEMPORAL_KEY",
    "build_cover_section",
    "build_temporal_section",
    "cover_unchanged",
    "cover_words",
    "coverage_toc",
    "coverage_toc_counts",
    "delete_cover",
    "load_cover",
    "load_temporal_coverage",
    "merge_cover_sections",
    "merge_temporal_sections",
    "quantize_words",
    "read_cover",
    "read_leaf_temporal",
    "section_unchanged",
    "shards_overlapping",
    "temporal_cell_order",
    "temporal_fields",
    "write_cover",
]
