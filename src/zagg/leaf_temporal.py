"""The per-leaf temporal record — ``zagg-leaf-temporal/1`` (issue #575).

One small JSON object per shard leaf, ``{leaf}/temporal.toc``, carrying what
the spec §10 root section needs from that leaf — its §10.2 envelope word,
its **counted cover** (observation counts per aligned time bucket, the
§10.3 tier-2 channel; espg ruling of 2026-09-17 on issue #575) and the
§10.5 cover derived from it — computed **where the observations are**: on
the worker, per chunk, from the toc words the aggregation already encodes
(:func:`zagg.processing.aggregate._chunk_toc_words`), and written at the
leaf write site beside the tier-1 spatial coverage sidecar. The families
sweep composes the root section from these records instead of reading each
leaf's raw companion column back, which at CA scale (a million-row ragged
array per field per leaf) could not finish inside one invoke at all.

The record is the twin of the leaf's ``coverage.moc`` occupancy sidecar: a
regenerable accelerator (D9) written BEFORE the commit stamp so the stamp
stays the leaf's final write (D4), additive (an old store simply lacks it;
the sweep's raw route materializes it once), and never truth — the truth is
the leaf's own arrays. The normative grammar is ``docs/specification.md``
§10.6; the counted cover's laws are §10.3.

**The counted cover** (:class:`CountedCover`) is ``{bucket word: n_obs}``
over the leaf's observation instants quantized at the one pinned order,
:data:`zagg.coverage_toc.TEMPORAL_COVER_ORDER` (24: aligned 2^39 ns ≈ 9.2
min buckets), kept UN-coalesced at that order so each occupied bucket keeps
its own count. Its algebra is exact and needs no merge law: two covers on
one grid **merge** by per-word addition (:func:`merge_counts`); a cover
**coarsens** to a lower order by summing into each bucket's ancestor
(:func:`coarsen_counts`), applied only to fit the §10.5 cap by whole
orders; and the §10.5 word-set cover is its keys, ``toc_normalize``d
(:func:`cover_from_counts`) — byte-equal to :func:`zagg.coverage_toc.quantize_words`
over the same instants. Our sources are spikes (a pass crosses a shard in
about a second), so the word count does not grow with finer order until
buckets approach pass duration: the pin costs nothing over a coarser one
and answers "within the hour?" from the record alone.

Two producers fill :class:`LeafTemporalAccumulator`, recorded in
``source``: **the worker** (``"worker"``) folds per-OBSERVATION words, one
exact timestamp each; **the sweep's backfill** (``"sweep"``) folds a leaf's
committed per-centroid companions with the centroid weights as counts
(:func:`zagg.coverage_toc.read_leaf_temporal`) — a weight-1 centroid is an
exact instant, a merged one is counted at its envelope's midpoint. The
envelope word is identical from either (the join is a semilattice) and the
count is the same; the worker's buckets are the exact ones.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import NamedTuple

import numpy as np

from zagg.coverage_toc import (
    COVER_CAP,
    TEMPORAL_COVER_ORDER,
    _decode_cover_block,
    _encode_cover_block,
    _object_pin,
)

logger = logging.getLogger(__name__)

#: The record's own spec marker (an OBJECT-level marker, like the §10.5
#: cover's): a reader strict-checks it and treats an unknown revision as
#: absent, never as a refusal.
LEAF_TEMPORAL_SPEC = "zagg-leaf-temporal/1"

#: The object's name under the leaf prefix, beside ``coverage.moc``.
LEAF_TEMPORAL_NAME = "temporal.toc"

#: Observation words buffered before a fold. The worker hands the
#: accumulator one array per chunk; a chunk on a fat shard can hold millions
#: of rows and a chunk on a sparse one a handful, so folds are keyed to ROWS
#: rather than calls — a large chunk folds at once, small ones batch — and
#: the fold's transient (the bucket index and its ``np.unique``) is bounded
#: by this many words plus the one chunk in flight, never by the shard.
FOLD_ROWS = 1 << 16


class CountedCover(NamedTuple):
    """``{word: n_obs}`` over aligned order-``order`` buckets, as parallel arrays.

    ``words`` are the buckets' range words (sorted, unique, un-coalesced —
    abutting occupied buckets stay distinct); ``obs`` the observation count
    in each, row-aligned. Exact under :func:`merge_counts` (same order ⇒ same
    grid ⇒ per-word addition) and :func:`coarsen_counts` (ancestor sums).
    """

    words: np.ndarray
    obs: np.ndarray
    order: int


def _bucket_words(index: np.ndarray, order: int) -> np.ndarray:
    """Range words for aligned order-``order`` buckets by index (§10.5's grid).

    The top bucket's end clamps to the grammar's ceiling (``TOC_MAX_NS``),
    exactly as :func:`zagg.coverage_toc.quantize_words` clamps it, so the
    two agree word for word.
    """
    from mortie import TOC_MAX_NS, span2toc

    k = np.uint64(63 - int(order))
    lo = np.asarray(index, dtype=np.uint64) << k
    hi = np.minimum(lo + (np.uint64(1) << k), np.uint64(TOC_MAX_NS)) - np.uint64(1)
    return np.atleast_1d(np.asarray(span2toc(lo, hi), dtype=np.uint64))


def _bucket_index(words: np.ndarray, order: int) -> np.ndarray:
    """Aligned bucket index at ``order`` of each word's decoded start."""
    from mortie import toc2time

    start, _end = toc2time(np.asarray(words, dtype=np.uint64))
    return np.atleast_1d(np.asarray(start, dtype=np.uint64)) >> np.uint64(63 - int(order))


def _instants(words: np.ndarray) -> np.ndarray:
    """One representative instant per word: itself for a timestamp, the midpoint for a range."""
    from mortie import toc2time

    start, end = toc2time(np.asarray(words, dtype=np.uint64))
    start = np.atleast_1d(np.asarray(start, dtype=np.uint64))
    end = np.atleast_1d(np.asarray(end, dtype=np.uint64))
    return start + (end - start) // np.uint64(2)


def _tally(index: np.ndarray, weights, order: int) -> CountedCover:
    uniq, inverse = np.unique(index, return_inverse=True)
    if weights is None:
        obs = np.bincount(inverse, minlength=len(uniq))
    else:
        obs = np.rint(np.bincount(inverse, weights=np.asarray(weights, dtype=np.float64)))
    return CountedCover(_bucket_words(uniq, order), obs.astype(np.uint64), int(order))


def count_words(words, weights=None, order: int = TEMPORAL_COVER_ORDER) -> CountedCover:
    """Count words into aligned order-``order`` buckets by their representative instant.

    ``weights`` (optional, row-aligned) are observation counts per word — the
    sweep's per-centroid feed; ``None`` counts each word once (the worker's
    per-observation feed). Counts are rounded to integers.
    """
    words = np.asarray(words, dtype=np.uint64).ravel()
    if words.size == 0:
        return CountedCover(np.empty(0, np.uint64), np.empty(0, np.uint64), int(order))
    return _tally(_instants(words) >> np.uint64(63 - int(order)), weights, order)


def coarsen_counts(counts: CountedCover, order: int) -> CountedCover:
    """Sum each bucket into its order-``order`` ancestor (exact at the coarser rung)."""
    if int(order) > counts.order:
        raise ValueError(f"cannot coarsen an order-{counts.order} counted cover to {order}")
    if int(order) == counts.order or counts.words.size == 0:
        return CountedCover(counts.words, counts.obs, int(order))
    shift = np.uint64(counts.order - int(order))
    return _tally(_bucket_index(counts.words, counts.order) >> shift, counts.obs, order)


def merge_counts(parts: list[CountedCover]) -> CountedCover:
    """Per-word sum over the union of keys, at the coarsest order among ``parts``."""
    parts = [p for p in parts if p.words.size]
    if not parts:
        return CountedCover(np.empty(0, np.uint64), np.empty(0, np.uint64), TEMPORAL_COVER_ORDER)
    order = min(p.order for p in parts)
    parts = [coarsen_counts(p, order) for p in parts]
    index = np.concatenate([_bucket_index(p.words, order) for p in parts])
    return _tally(index, np.concatenate([p.obs for p in parts]), order)


def cap_counts(counts: CountedCover, cap: int = COVER_CAP) -> CountedCover:
    """Coarsen by whole orders until at most ``cap`` words — the §10.5 cap, widening only."""
    while counts.words.size > cap and counts.order > 0:
        counts = coarsen_counts(counts, counts.order - 1)
    return counts


def cover_from_counts(counts: CountedCover) -> tuple[np.ndarray, int]:
    """The §10.5 word-set cover: the counts' keys, normalized, with their order.

    Byte-equal to :func:`zagg.coverage_toc.quantize_words` at ``counts.order``
    over the instants the counts were built from, so a cover is never
    computed separately from its counts; a cover capped below the pin stays
    at its own rung.
    """
    from mortie import toc_normalize

    if counts.words.size == 0:
        return np.empty(0, np.uint64), counts.order
    return np.asarray(toc_normalize(counts.words), dtype=np.uint64), counts.order


def encode_counts(counts: CountedCover) -> dict:
    """The counted-cover block: two row-aligned §1.4 uint64 buffers, base64'd."""
    from zagg.sweep_overview import encode_digest

    return {
        "temporal_order": int(counts.order),
        "cap": COVER_CAP,
        "element": {"dtype": "uint64", "shape": [-1]},
        "encoding": "base64",
        "words": base64.b64encode(encode_digest(counts.words, "uint64")).decode("ascii"),
        "obs": base64.b64encode(encode_digest(counts.obs, "uint64")).decode("ascii"),
        "count": int(counts.words.size),
        "obs_total": int(counts.obs.sum()) if counts.obs.size else 0,
    }


def _counted_int(block: dict, key: str) -> int:
    """One of §10.3's integer keys, refused (``ValueError``) if it is not one.

    Coercing first would leak a bare ``TypeError`` out of ``int()`` for a
    ``null`` or a string, where §10.3 states the check as a MUST on the block
    — and an external reader implementing the refusal from this module would
    then implement one zagg does not.
    """
    value = block.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"counted cover declares {key} {value!r}, which is not an integer (spec §10.3)"
        )
    return value


def decode_counts(block, pin: int = TEMPORAL_COVER_ORDER) -> CountedCover:
    """Decode a counted-cover block, MUST-checked.

    Refuses (``ValueError``) a non-integer ``temporal_order``/``count``/
    ``obs_total``, buffers disagreeing with ``count`` or with each other, an
    ``obs_total`` that is not the sum, and an order outside ``[0, pin]`` —
    this revision's producers write at most the §10.5 pin.
    """
    from zagg.sweep_overview import decode_digest

    if not isinstance(block, dict):
        raise ValueError("counted cover block is not an object (spec §10.3)")
    order = _counted_int(block, "temporal_order")
    count = _counted_int(block, "count")
    obs_total = _counted_int(block, "obs_total")
    if not 0 <= order <= int(pin):
        raise ValueError(f"counted cover declares temporal_order {order}, outside [0, {pin}]")
    words = decode_digest(base64.b64decode(block["words"]), "uint64", ())
    obs = decode_digest(base64.b64decode(block["obs"]), "uint64", ())
    if len(words) != len(obs) or count != len(words):
        raise ValueError(
            f"counted cover declares {count!r} buckets and decodes {len(words)} "
            f"words with {len(obs)} counts — the buffers must agree (spec §10.3)"
        )
    if obs_total != int(obs.sum()):
        raise ValueError(
            f"counted cover declares obs_total {obs_total!r} but its counts sum "
            f"to {int(obs.sum())} (spec §10.3)"
        )
    return CountedCover(np.asarray(words, np.uint64), np.asarray(obs, np.uint64), order)


def armed(config) -> bool:
    """Whether ``config`` declares a §8.3 ``per-centroid`` field — the record's gate.

    A store with no temporal channel writes no record (§10's absence rule);
    a ``per-cell`` or ``coordinate`` declaration alone is a different array
    grammar and contributes nothing to the §10 section, so it arms nothing.
    """
    return bool(temporal_field_names(config))


def temporal_field_names(config) -> list[str]:
    """The sorted ``per-centroid`` field names a config declares (the record's ``fields``)."""
    from zagg.config import get_agg_fields
    from zagg.time_axis import TOC_SHAPE_PER_CENTROID

    return sorted(
        name
        for name, meta in get_agg_fields(config).items()
        if meta.get("temporal") == TOC_SHAPE_PER_CENTROID
    )


class LeafTemporalAccumulator:
    """Fold one leaf's temporal contribution chunk by chunk, never whole.

    Two feeds, one state: :meth:`add_words` takes a chunk's per-observation
    toc words (the worker), :meth:`add_weighted` a chunk's per-centroid
    companions with their weights (the sweep's raw route). Each fold joins
    the envelope word (``toc_reduce``, associative) and adds the chunk's
    counted cover into the running one (:func:`merge_counts`, exact), so the
    held state is a few hundred rows regardless of the shard.
    :meth:`finish` returns ``(word, counts)``, or ``None`` when nothing was
    added.
    """

    def __init__(self, order: int = TEMPORAL_COVER_ORDER) -> None:
        self.order = int(order)
        self._pending: list[np.ndarray] = []
        self._pending_rows = 0
        self._word: int | None = None
        self._counts: CountedCover | None = None

    def add_words(self, words) -> None:
        """Fold a chunk's per-observation toc words (one exact timestamp each)."""
        words = np.ascontiguousarray(np.asarray(words, dtype=np.uint64).ravel())
        if words.size == 0:
            return
        self._pending.append(words)
        self._pending_rows += int(words.size)
        if self._pending_rows >= FOLD_ROWS:
            self._fold_pending()

    def add_weighted(self, words, weights) -> None:
        """Fold a chunk's per-centroid words, each counted ``weights`` times."""
        words = np.asarray(words, dtype=np.uint64).ravel()
        if words.size:
            self._fold(words, np.asarray(weights, dtype=np.float64).ravel())

    def _fold_pending(self) -> None:
        words = np.concatenate(self._pending) if len(self._pending) > 1 else self._pending[0]
        self._pending, self._pending_rows = [], 0
        self._fold(words, None)

    def _fold(self, words: np.ndarray, weights) -> None:
        from mortie import toc_reduce

        joined = int(toc_reduce(words))
        self._word = (
            joined
            if self._word is None
            else int(toc_reduce(np.asarray([self._word, joined], dtype=np.uint64)))
        )
        counts = count_words(words, weights, self.order)
        self._counts = counts if self._counts is None else merge_counts([self._counts, counts])

    def finish(self) -> tuple[int, CountedCover] | None:
        """``(word, counts)`` over everything added, or ``None``."""
        if self._pending:
            self._fold_pending()
        if self._word is None or self._counts is None:
            return None
        return self._word, self._counts


def build_leaf_temporal(word, counts: CountedCover, fields, *, source: str = "worker") -> dict:
    """The ``zagg-leaf-temporal/1`` record body from one leaf's ``(word, counts)``.

    The counted cover is coarsened to the §10.5 cap by whole orders (the
    block records the order it landed at); the §10.5 cover is derived from
    it (:func:`cover_from_counts`) and carried so a reader wanting only the
    word set needs no coarsening of its own.
    """
    from zagg.hive import _utcnow

    counts = cap_counts(counts)
    if counts.order != TEMPORAL_COVER_ORDER:
        logger.warning(
            f"leaf temporal: counted cover coarsened to temporal order {counts.order} to fit "
            f"the {COVER_CAP}-word cap (spec §10.5)"
        )
    cover, cover_order = cover_from_counts(counts)
    return {
        "spec": LEAF_TEMPORAL_SPEC,
        "source": source,
        "generated_at": _utcnow(),
        "fields": sorted(fields),
        "n_obs": int(counts.obs.sum()) if counts.obs.size else 0,
        "word": str(int(word)),
        "temporal_order": TEMPORAL_COVER_ORDER,
        "cap": COVER_CAP,
        "counts": encode_counts(counts),
        "cover": {
            **_encode_cover_block(cover, cover_order),
            "element": {"dtype": "uint64", "shape": [-1]},
            "encoding": "base64",
        },
    }


def load_leaf_temporal(obj) -> dict | None:
    """A record body at the revision this module implements, else ``None``.

    The strict-gate-then-degrade rule: a foreign revision, a non-dict body,
    or plain absence all read as ``None`` — the record is an accelerator
    whose truth is in the leaf, so degraded means "read the leaf".
    """
    if not isinstance(obj, dict) or obj.get("spec") != LEAF_TEMPORAL_SPEC:
        return None
    return obj


def leaf_temporal_contribution(record: dict) -> tuple[int, CountedCover]:
    """Decode a record to ``(word, counts)``, MUST-checked.

    Refuses (``ValueError``) a counts block failing :func:`decode_counts`'s
    checks, a cover block disagreeing with its ``count`` or declaring an
    order above the record's pin, a cover that is not the one derived from
    the counts, an ``n_obs`` that is not the counts' total, and a cover
    whose envelope escapes the quantized word (§10.5's containment, per
    leaf) — a record that fails its own consistency claims is debris, and
    the caller regenerates it from the leaf.
    """
    from mortie import toc2time, toc_reduce

    from zagg.coverage_toc import quantize_words

    word = int(record["word"])
    # Both blocks decode against the RECORD's own declared pin (§10.6 gives
    # the record one), not against whatever this build's default happens to
    # be — one rule for the two blocks, as §10.5's `_object_pin` intends.
    pin = _object_pin(record)
    counts = decode_counts(record["counts"], pin)
    cover, order = _decode_cover_block("leaf", record["cover"], pin)
    derived, derived_order = cover_from_counts(counts)
    if order != derived_order or not np.array_equal(cover, derived):
        raise ValueError(
            "leaf temporal record's cover is not the one its counts derive (spec §10.6)"
        )
    if int(record["n_obs"]) != int(counts.obs.sum()):
        raise ValueError(
            f"leaf temporal record declares n_obs {record['n_obs']!r} but its counts total "
            f"{int(counts.obs.sum())} (spec §10.6)"
        )
    if len(cover):
        lo_c, hi_c = (int(x) for x in toc2time(int(toc_reduce(cover))))
        lo_w, hi_w = (int(x) for x in toc2time(int(toc_reduce(quantize_words([word], order)))))
        if lo_c < lo_w or hi_c > hi_w:
            raise ValueError(
                "leaf temporal record's cover escapes its own word (spec §10.5 containment, "
                "per leaf) — regenerate it from the leaf"
            )
    return word, counts


def write_leaf_temporal(leaf_root: str, record: dict, **store_kwargs) -> None:
    """PUT the record at ``{leaf}/temporal.toc`` (before the stamp, like the bitmap)."""
    from zagg.store import open_object_store, put_object

    put_object(
        open_object_store(leaf_root, **store_kwargs),
        LEAF_TEMPORAL_NAME,
        json.dumps(record, indent=1).encode(),
    )


def read_leaf_temporal_record(leaf_root: str, **store_kwargs) -> dict | None:
    """The leaf's record as stored, or ``None`` when absent.

    Raw: the spec gate is :func:`load_leaf_temporal`'s, kept separate so a
    caller can tell a foreign revision (preserve it — the §10.4 succession
    rule) from absence (materialize one). A body that is not JSON raises,
    which the caller treats as debris.
    """
    from zagg.hive import _read_json
    from zagg.store import open_object_store

    return _read_json(open_object_store(leaf_root, **store_kwargs), LEAF_TEMPORAL_NAME)


def leaf_contribution(
    leaf_root: str, cell_order: int, fields: dict, *, materialize: bool = True, **store_kwargs
):
    """One leaf's ``(word, counts)`` — record first — and the route it took.

    The families sweep's per-leaf read (issue #575). Returns
    ``(contribution, route)``: the decoded record with ``"record"`` when the
    leaf carries a readable ``temporal.toc`` at this revision whose
    ``fields`` cover every declared field (one small GET, no array opened);
    otherwise the raw route — :func:`zagg.coverage_toc.read_leaf_temporal`,
    one ragged chunk at a time — with ``"raw"``, or ``"materialized"`` when
    ``materialize`` is set and the record it computed was written back
    (``source: "sweep"``), so a store written before the record existed
    backfills once and converges across partitioned re-fires.
    ``contribution`` is ``None`` for a leaf holding no temporal row.

    Succession and debris follow §10.4/§10.6: a record at a FOREIGN revision
    is preserved (raw route, never overwritten); an unparsable or
    inconsistent one is debris the materialized record replaces; one whose
    ``fields`` omit a declared field is stale (the field postdates it) and
    is re-derived over the union. A record whose GET itself fails is neither:
    the leaf is read (an unreadable accelerator is no more evidence about it
    than a missing one) and the object is left alone, since a body that did
    not read may be a foreign revision. Materialization is fail-open (D9): a
    write that fails is logged and the contribution still returns.
    """
    from zagg.coverage_toc import read_leaf_temporal

    foreign = False
    try:
        raw = read_leaf_temporal_record(leaf_root, **store_kwargs)
    except ValueError as e:
        logger.warning(f"leaf temporal: {leaf_root} record is not JSON ({e}) — re-deriving")
        raw = None
    except Exception as e:
        # The GET itself failed (a 403 on the key, a 5xx, a timeout): the
        # accelerator is not the truth, so read the leaf rather than costing
        # the shard — and never overwrite a body that could not be read (it
        # may be a foreign revision, which §10.4 says to preserve).
        logger.warning(f"leaf temporal: {leaf_root} record did not read ({e}) — re-deriving")
        raw, foreign = None, True
    record = load_leaf_temporal(raw)
    if record is not None:
        if set(record.get("fields") or []) >= set(fields):
            try:
                return leaf_temporal_contribution(record), "record"
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"leaf temporal: {leaf_root} record is debris ({e}) — re-deriving")
        else:
            logger.info(
                f"leaf temporal: {leaf_root} record predates a declared field — re-deriving"
            )
    elif isinstance(raw, dict) and isinstance(raw.get("spec"), str) and raw["spec"]:
        # An unknown revision: read the leaf, and leave the object alone.
        foreign = True
    got = read_leaf_temporal(leaf_root, cell_order, fields, **store_kwargs)
    if got is None or not materialize or foreign:
        return got, "raw"
    try:
        write_leaf_temporal(
            leaf_root, build_leaf_temporal(*got, fields, source="sweep"), **store_kwargs
        )
    except Exception as e:
        logger.warning(f"leaf temporal: could not materialize {leaf_root} (fail-open, D9): {e}")
        return got, "raw"
    return got, "materialized"


__all__ = [
    "FOLD_ROWS",
    "LEAF_TEMPORAL_NAME",
    "LEAF_TEMPORAL_SPEC",
    "CountedCover",
    "LeafTemporalAccumulator",
    "armed",
    "build_leaf_temporal",
    "cap_counts",
    "coarsen_counts",
    "count_words",
    "cover_from_counts",
    "decode_counts",
    "encode_counts",
    "leaf_contribution",
    "leaf_temporal_contribution",
    "load_leaf_temporal",
    "merge_counts",
    "read_leaf_temporal_record",
    "temporal_field_names",
    "write_leaf_temporal",
]
