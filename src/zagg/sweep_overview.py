"""Overview-zarr sweep family: pyramid generation at ancestor nodes (issue #201).

The fourth D22 derived-artifact family (``docs/design/sparse_coverage.md`` §7,
D11/D22/D24): for each manifest-declared overview order *k*, the sweep folds
committed source leaves into an **overview zarr at the ancestor digit node** —
the same leaf structure one order family up (dense per-field arrays + the
``morton`` coordinate + ragged t-digest vlen arrays), at cell order
``cell_order - (shard_order - k)`` (constant depth: cells coarsen 4x per
order of ascent, so the pyramid is the store's resolution axis, partially
materialized — D24).

Field inclusion is gated by the D24 composability class
(:func:`zagg.semantics.field_composability`): ``exact`` fields fold by their
merge law (count/sum by addition, min/max by extremum — byte-equal to a direct
**nan-skipping** aggregation at the coarser order; see
:data:`EXACT_NAN_POLICY`), ``approximate`` fields (t-digests) merge
via the order-independent k-way fold (``np.isclose`` equality class), and
``none`` fields are **excluded** — they exist only at native resolution, with
the absence declared in the manifest's pyramid block (the ruled D24 default;
declared derived summaries are the opt-in, deferred until a roster-kind
consumer exists — issue #265).

Naming and attrs are the ratified D23/D11 layout moczarr plans against:
overviews inherit window naming — ``{window}.zarr`` at the ancestor node,
``all.zarr`` for the unwindowed / all-time fold (the reserved token) — and
every overview carries ``role: overview`` plus source orders, per-field
aggregation methods, and the D22 generation stamp in its root attrs (never
inferred from tree position: a shallow zarr may equally be coarse source).

Like every sweep artifact, overviews are regenerable caches (D9): deleting
every one leaves all leaf reads intact. Discovery is from the run-record
work set plus the root coverage MOC — never a recursive LIST.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import overload

import numpy as np

from zagg.grids.base import ragged_locations_name, ragged_times_name

logger = logging.getLogger(__name__)

#: Envelope version of the per-node overview attrs payload this module writes.
OVERVIEW_SPEC = "zagg-overview/1"
#: Root-group attrs key carrying the overview provenance payload (D11).
OVERVIEW_ATTR = "zagg_overview"
#: Root-group attrs key classifying the zarr (D11/D24: never inferred from
#: position; source leaves carry no role — absence means source).
ROLE_ATTR = "role"
#: Version string of the manifest ``pyramid`` block this module declares.
PYRAMID_SPEC = "zagg-pyramid/1"
#: Default overview-order spacing (espg-ratified on the issue #201 thread:
#: display schedule is every 2 orders — 16x per step, ~1/15 extra storage).
DEFAULT_SPACING = 2

#: The two fold sources a declared level can have (issue #376). ``cascade``
#: folds a level from the next FINER declared level's already-materialized
#: overview — fold-of-folds, so per-node input is the 4^gap child overview
#: slabs (constant, independent of subtree size) and the leaves are read once
#: for the whole pyramid. ``leaves`` is the pre-#376 exact-from-leaves fold:
#: every level re-reads the whole subtree, and a single-base-cell AOI's
#: coarse nodes accumulate their entire subtree's centroids before one k-way
#: merge — the state-scale wall the issue names. It is kept as a DEPRECATED
#: opt-in only.
FOLD_SOURCES = ("cascade", "leaves")
#: The espg-ratified default (issue #376): under ``/1``, overviews are
#: display artifacts with no precision guarantee past the first skip level.
#: (For ``/2`` that doctrine is superseded — spec §4.4: exact fields exact at
#: every order, approximate fields analysis-grade at their recorded
#: generation.)
DEFAULT_FOLD_SOURCE = "cascade"
#: How many of the FINEST declared levels fold exactly from the leaves; every
#: coarser level cascades from the level below it in this list. 1 is the
#: ratified default — the finest declared level has no finer overview to
#: cascade from, so it is exact by construction. The open sub-decision on
#: issue #376 is whether the second level is exact too ("first, or possibly
#: second"), which is exactly ``exact_levels: 2``.
DEFAULT_EXACT_LEVELS = 1

#: NaN policy the exact fold actually implements, recorded per field in the
#: pyramid declaration and in every overview's attrs (review finding, issue
#: #201). A leaf's stored NaN is the same bytes whether it is the fill sentinel
#: or a NaN datum, so :func:`fold_dense` cannot distinguish them and skips
#: both: the exact-class fold is byte-equal to a direct ``nansum``/``nanmin``/
#: ``nanmax`` at the coarser order, NOT to a NaN-propagating ``sum``/``min``/
#: ``max`` (which the store encoding makes unrecoverable either way — the plain
#: and nan-aware aggregators share one law in
#: :data:`zagg.semantics.EXACT_MERGE_LAWS`).
EXACT_NAN_POLICY = "skip"

#: Fold law name recorded for approximate (t-digest) fields: the
#: order-independent k-way merge (issue #279 — deterministic under
#: permutation, unlike a pairwise left-fold).
TDIGEST_LAW = "tdigest_kway"

#: Fold law name recorded for the ``packed`` class (issue #515):
#: :func:`zagg.stats.composition.merge_composition_kway` over per-contributor
#: ``(word, n_signal)`` pairs, ``n`` sourced from the entry's ``of`` digest's
#: per-cell weights (spec §3.3/§3.4). One quantization per fold call, so a
#: permutation of the parts returns identical bytes; presence is exact
#: through arbitrary chains and counts stay within one lane quantization per
#: fold — deterministic, but NOT byte-equal to a direct aggregation, which is
#: why ``packed`` is its own class rather than an ``exact`` method.
COMPOSITION_LAW = "composition_kway"


# ---------------------------------------------------------------------------
# Phase A: per-field up-aggregation kernels (the D24 merge laws over arrays).
# ---------------------------------------------------------------------------


def _is_missing(values: np.ndarray, fill_value) -> np.ndarray:
    """Boolean missing-mask for a dense field slab (fill/NaN sentinel cells)."""
    if values.dtype.kind == "f":
        fill = _fill_scalar(fill_value, values.dtype)
        if np.isnan(fill):
            return np.isnan(values)
        return np.isnan(values) | (values == fill)
    return values == _fill_scalar(fill_value, values.dtype)


def _fill_scalar(fill_value, dtype):
    """The numeric fill sentinel (zarr's ``"NaN"`` JSON token included)."""
    if isinstance(fill_value, str):
        return np.array(fill_value, dtype=dtype)[()]  # "NaN"/"Infinity" tokens
    return np.array(fill_value, dtype=dtype)[()]


#: law -> (reduce ufunc, identity kind). The identity replaces missing cells
#: before the reduce; all-missing groups are restored to the fill afterwards.
_LAW_REDUCERS = {
    "sum": np.add,
    "min": np.minimum,
    "max": np.maximum,
}


def fold_dense(values: np.ndarray, factor: int, law: str, fill_value) -> np.ndarray:
    """Fold a dense per-cell slab ``factor``-to-one under an exact merge law.

    ``values`` is a leaf-ordered 1-D array (row i = the i-th subtree cell in
    ascending packed-word order — the leaf invariant), so ``factor``
    consecutive rows share one coarser parent: the fold is a pure
    ``reshape(-1, factor)`` reduce. Missing cells (the field's fill sentinel,
    NaN for float fields) are excluded from the reduce; an all-missing group
    folds back to the fill. Exact by construction for the D24 exact class:
    addition/extremum over the same values in the same ascending order a
    direct coarser aggregation would visit — with the one premise
    :data:`EXACT_NAN_POLICY` names: NaN is ALWAYS missing (fill and datum are
    the same bytes), so the match is against the nan-skipping reduction.
    """
    if law not in _LAW_REDUCERS:
        raise ValueError(f"unknown exact fold law {law!r}; known: {sorted(_LAW_REDUCERS)}")
    values = np.asarray(values)
    if factor <= 0 or values.shape[0] % factor:
        raise ValueError(f"cannot fold {values.shape[0]} cells {factor}-to-one")
    groups = values.reshape(-1, factor)
    missing = _is_missing(groups, fill_value)
    if law == "sum":
        identity = np.zeros((), dtype=groups.dtype)
    elif law == "min":
        ident = np.inf if groups.dtype.kind == "f" else np.iinfo(groups.dtype).max
        identity = np.array(ident, dtype=groups.dtype)
    else:  # max
        ident = -np.inf if groups.dtype.kind == "f" else np.iinfo(groups.dtype).min
        identity = np.array(ident, dtype=groups.dtype)
    out = _LAW_REDUCERS[law].reduce(np.where(missing, identity, groups), axis=1)
    fill = _fill_scalar(fill_value, groups.dtype)
    return np.where(missing.all(axis=1), fill, out).astype(groups.dtype, copy=False)


def combine_dense(a: np.ndarray, b: np.ndarray, law: str, fill_value) -> np.ndarray:
    """Element-wise combine of two partial folds under one exact law.

    The accumulation form of :func:`fold_dense` (interleave the operands and
    fold 2-to-one), for overview cells assembled from more than one source
    leaf: coarser-than-shard overview orders, and the all-time fold's
    cross-window accumulation.
    """
    a, b = np.asarray(a), np.asarray(b)
    stacked = np.stack([a, b], axis=1).reshape(-1)
    return fold_dense(stacked, 2, law, fill_value)


def decode_digest(raw, dtype, inner_shape=(2,)) -> np.ndarray:
    """One cell's ragged payload bytes -> its ``(k, *inner_shape)`` array.

    The D18 vlen framing (raw little-endian C-order values at the element
    dtype); an empty payload decodes to the zero-length array, matching the
    reader (``zagg.readers.tdigest_tensor._decode_cell``).
    """
    dt = np.dtype(dtype).newbyteorder("<")
    return np.frombuffer(bytes(raw), dtype=dt).reshape((-1, *inner_shape))


def encode_digest(digest: np.ndarray, dtype) -> bytes:
    """The inverse of :func:`decode_digest`: C-order little-endian bytes."""
    dt = np.dtype(dtype).newbyteorder("<")
    return np.ascontiguousarray(np.asarray(digest, dtype=dt)).tobytes()


#: Default cap on the pyramid-fold compression budget (issue #424). Overview
#: digests are governed by the ~1/δ quantile-accuracy bound (512 → ~0.2%),
#: not the leaf's loss-free bound — and the cap also bounds the sweep's
#: chunk-batched k-way fold buffers (4 children × δ × cells), which saturate
#: toward ~1 GB per slab at a δ=8,192 leaf budget vs ~33 MB here.
OVERVIEW_DELTA_CAP = 512


def payload_weight(raw, dtype, inner_shape=(2,)) -> int:
    """One stored digest payload's total weight, 0 for the empty cell.

    The ``n`` input of the packed composition fold (spec §3.3/§3.4): every
    fold site pairs a contributor's composition word with its ``of`` digest's
    weight at the same cell. Rounded to the count it stores — strata weights
    are exact stratum photon counts (spec §2), carried as the payload dtype's
    floats.
    """
    if raw is None or not len(raw):
        return 0
    return int(round(float(decode_digest(raw, dtype, tuple(inner_shape))[:, 1].sum())))


def overview_fold_delta(meta: dict) -> int:
    """The δ an overview/pyramid/column fold compresses at (issue #424).

    A declared ``overview_delta`` (manifest field entry / config field key)
    wins. Absent — every pre-#424 manifest — the leaf ``delta`` capped at
    :data:`OVERVIEW_DELTA_CAP`: identical to the historical fold-at-leaf-δ
    behavior for every manifest ever written (all carried δ ≤ 512), while a
    raised leaf δ no longer saturates the fold buffers. Leaf-side builds and
    streaming/spill folds are NOT overview folds and keep the leaf δ — only
    the first fold level ever sees leaf-δ inputs, and those are bounded by
    actual observations, not δ.
    """
    declared = meta.get("overview_delta")
    if declared is not None:
        return int(declared)
    return min(int(meta.get("delta") or OVERVIEW_DELTA_CAP), OVERVIEW_DELTA_CAP)


def check_weights_match(attrs, meta: dict, field: str) -> None:
    """Refuse a digest fold across mismatched §2.0 weights declarations (#424).

    Merges are legal only between payloads carrying the same declaration
    (spec §2.0): folding a counts payload under a flux declaration (or vice
    versa) would produce a weight column whose sum means neither thing. The
    manifest's declared value (absent ⇒ counts, like the attrs key itself) is
    the store-wide truth; a source array disagreeing with it raises here,
    which the per-leaf/per-child fold guards turn into a loud skip rather
    than a silent mixed merge.
    """
    from zagg.grids.base import weights_declaration

    stored = weights_declaration(dict(attrs or {}))
    declared = meta.get("weights") or "counts"
    if stored != declared:
        raise ValueError(
            f"field {field!r}: stored weights declaration {stored!r} does not match "
            f"the manifest's {declared!r} — merges are legal only between matching "
            f"declarations (spec §2.0, issue #424)"
        )


def check_companion_match(attrs, field: str, kwarg: str = "locations") -> None:
    """Refuse a digest fold over a companion declaration this zagg cannot join.

    The companion analogue of :func:`check_weights_match`, called on the
    SIBLING's attrs — where §8.3/§9 put the block. §9.2 imports §8.4 verbatim:
    declared companions compose only when they match on ``{shape, grammar}``,
    and a writer joining ones that differ MUST refuse. Each convention defines
    exactly one of each for the per-centroid shape, so strict-checking every
    contributor's declaration IS the match check: any two blocks that pass agree
    by construction, and one that does not pass is a shape or grammar this fold
    has no words for. An absent block is the convention's absent-key rule (§2.2
    verbatim for located; no companion for temporal) and joins cleanly, so it is
    not drift.

    The temporal arm additionally pins the SHAPE to ``per-centroid``, which the
    espg ruling of 2026-08-17 makes the only shape a digest pyramid folds: a
    ``per-cell`` block reaching a per-centroid fold would decode one word per
    cell as a per-centroid vector and produce envelopes whose containment claim
    is false. §8.4's shape-coarsening reduction stays licensed for producers
    that want it — zagg's digest pyramids simply do not use it, so meeting one
    here is a store this fold has no law for.

    Reading words under the wrong shape would not fail loudly, which is why the
    refusal is here and not left to the arithmetic. The per-leaf and per-child
    fold guards turn it into a loud skip; :func:`_field_drift` refuses the same
    store at retrofit time, so a disagreement is caught before the declaration
    lands.
    """
    from zagg.grids.base import located_declaration
    from zagg.time_axis import TOC_SHAPE_PER_CENTROID, temporal_declaration

    try:
        if kwarg == "temporal":
            temporal_declaration(dict(attrs or {}), shape=TOC_SHAPE_PER_CENTROID)
        else:
            located_declaration(dict(attrs or {}))
    except ValueError as e:
        raise ValueError(f"field {field!r}: {e}") from e


#: A digest field's companion channels, in the FIXED order the kernel returns
#: them (``(digest, locations, temporal)``): the field-meta key that declares
#: the channel, the reducer/kernel keyword it rides under, and the sibling array
#: namer. Both channels are per-centroid at every pyramid level — espg-ruled
#: 2026-08-17, amending ruling 3 on issue #410 — so a level above the leaf folds
#: them identically and this table is the single place the order lives.
COMPANION_CHANNELS = (
    ("location", "locations", ragged_locations_name),
    ("temporal", "temporal", ragged_times_name),
)


def field_companions(name: str, meta: dict) -> list[tuple[str, str]]:
    """``(kernel kwarg, sibling array name)`` per channel this field declares.

    In :data:`COMPANION_CHANNELS` order, so a caller can zip the kernel's extra
    tuple elements onto the siblings positionally. Empty for a field declaring
    neither, which is what keeps every pre-companion store's fold on the code
    path it always took.
    """
    return [
        (kwarg, namer(name))
        for key, kwarg, namer in COMPANION_CHANNELS
        if meta.get(key) is not None
    ]


@overload
def fold_digests(
    cell_digests: list, *, delta: int, dtype: str = ..., channels: None = ...
) -> bytes: ...
@overload
def fold_digests(
    cell_digests: list, *, delta: int, dtype: str = ..., channels: dict
) -> tuple[bytes, ...]: ...
def fold_digests(
    cell_digests: list, *, delta: int, dtype: str = "float32", channels: dict | None = None
) -> bytes | tuple[bytes, ...]:
    """Merge one overview cell's accumulated t-digests into its payload bytes.

    The approximate-class fold law (D24): the **order-independent k-way
    merge** (:func:`zagg.stats.tdigest.merge_tdigests_kway`, issue #279), so
    the overview digest is permutation-stable — a re-sweep over unchanged
    leaves reproduces identical bytes. An empty accumulation folds to the
    empty payload (the ragged fill).

    ``channels`` carries the companion channels (issue #410) as
    ``{kernel kwarg: [per-digest word vector]}`` — ``locations`` for the §9
    morton words, ``temporal`` for the §8.3 toc words. The returned slots are
    always in :data:`COMPANION_CHANNELS` order, whatever order the mapping was
    built in: every arm below normalizes through that table, because the k-way
    merge fixes its own return order by the same table and an arm that instead
    followed the caller's insertion order would hand back a toc word in the
    locations slot. The two word grammars are mutually accepting, so such a swap
    raises nowhere — it just writes false containment claims. An unrecognized
    kwarg is rejected rather than dropped. Each vector list is aligned with
    ``cell_digests``, and every declared channel is threaded through the SAME
    merge, so a merged centroid's location is the deepest common ancestor of its
    contributors' words and its toc word is their envelope. Given, the return is
    ``(payload_bytes, *channel_bytes)`` in that order; omitted, the bare payload
    bytes a field with no companion still gets.

    **The payload and its channels must come from one call.** The words are
    exact only *given* the centroid partition the merge produces (spec
    §9.1/§8.3), so folding a channel in a second pass would describe a different
    partition than the payload's. An empty accumulation folds to empty payloads
    throughout, keeping every sibling row-aligned with the digest at zero rows.

    Every vector is length-checked HERE rather than at the call sites, because
    this function is the documented seam they must come from together: the
    k-way merge validates the pairing itself, but the single-contributor arm
    below bypasses the merge — and it is the majority case at the finest
    overview level, where an output cell usually has exactly one populated
    contributor. A ``b""`` sibling row decodes to a length-0 vector with no
    complaint, so without this check that arm would encode a populated payload
    against an empty channel: §1.1's row-alignment MUST, broken and written.
    """
    from zagg.stats.tdigest import merge_tdigests_kway

    if channels is None:
        digests = [d for d in cell_digests if len(d)]
        if not digests:
            return b""
        if len(digests) == 1:
            return encode_digest(digests[0], dtype)
        return encode_digest(merge_tdigests_kway(digests, delta=int(delta)), dtype)
    keep = [i for i, d in enumerate(cell_digests) if len(d)]
    declared = [kwarg for _key, kwarg, _namer in COMPANION_CHANNELS if kwarg in channels]
    unknown = sorted(set(channels) - set(declared))
    if unknown:
        raise ValueError(
            f"unknown companion channel(s) {unknown} — the channels map is keyed by the "
            f"kernel kwarg of a COMPANION_CHANNELS entry"
        )
    for kwarg in declared:
        vectors = channels[kwarg]
        if len(vectors) != len(cell_digests):
            raise ValueError(
                f"{kwarg} channel has {len(vectors)} vectors for "
                f"{len(cell_digests)} accumulated digests"
            )
        for i in keep:
            if len(vectors[i]) != len(cell_digests[i]):
                raise ValueError(
                    f"{kwarg} sibling has {len(vectors[i])} words for a "
                    f"{len(cell_digests[i])}-centroid digest — the channel must be "
                    f"row-aligned with its payload (spec §1.1)"
                )
    if not keep:
        return (b"", *(b"" for _ in declared))
    digests = [cell_digests[i] for i in keep]
    kept = {kwarg: [channels[kwarg][i] for i in keep] for kwarg in declared}
    if len(digests) == 1:
        return (
            encode_digest(digests[0], dtype),
            *(encode_digest(kept[kwarg][0], "uint64") for kwarg in declared),
        )
    payload, *words = merge_tdigests_kway(digests, delta=int(delta), **kept)
    return (encode_digest(payload, dtype), *(encode_digest(w, "uint64") for w in words))


# ---------------------------------------------------------------------------
# The manifest pyramid block: template-time declaration (D22, per-family
# schedules), sweep-populated actuals.
# ---------------------------------------------------------------------------


def build_pyramid_block(config, shard_order: int, chunk_order: int | None = None) -> dict:
    """The manifest ``pyramid`` block: the template-time declaration (D22).

    Declares the overview family's order schedule (``output.pyramid`` knob),
    how each level is folded (:data:`DEFAULT_FOLD_SOURCE` /
    :data:`DEFAULT_EXACT_LEVELS`, issue #376), and each field's composability
    class + fold method (D24).
    ``none`` fields are declared with their class ONLY — the recorded absence
    that gives readers zero-open filtering (option A, the ruled default) —
    and warned about loudly at template time, per the D24 ruling. The sweep
    later adds actuals; it never rewrites the declaration.

    An explicit ``output.pyramid.overviews`` knob (issue #382) declares the
    ``zagg-pyramid/2`` grouped ``(node, cells)`` grammar — built in
    :mod:`zagg.pyramid` (:func:`zagg.pyramid.overview_block_v2`), replacing
    the ``orders``/``spacing`` schedule wholesale. Since the issue #384
    acceptance ruling (recorded on the #384 thread), the DEFAULT declaration
    is ``/2`` too: with no schedule spelled at all, a new store declares
    ``overviews: [chunk_order]`` whenever the caller passes the grid's
    resolved ``chunk_order`` (``build_manifest`` does) and it is strictly
    interior to the shard's resolution window. Raster configs are exempt
    (column-less by construction, issue #399), an explicit
    ``orders``/``spacing`` schedule stays ``/1`` deliberately, and the
    grid-less retrofit path (``declare_pyramid``) passes no ``chunk_order``
    and keeps its ``/1`` fallback. :func:`zagg.column.leaf_column_plan`
    mirrors this default from the grid, so declaration and worker gate can
    never disagree.
    """
    from zagg.config import get_pyramid
    from zagg.pyramid import (
        declared_fields,
        expand_overviews,
        normalize_overviews,
        overview_block_v2,
        validate_overviews,
        warn_excluded,
    )

    knob = get_pyramid(config)
    if knob is None:  # output.pyramid: false — declared off
        return {"spec": PYRAMID_SPEC, "overview": {"orders": []}}
    default_child = (config.output.get("grid") or {}).get("child_order")
    if (
        knob.get("overviews") is None
        and knob.get("orders") is None
        and knob.get("spacing") is None
        and isinstance(chunk_order, int)
        and int(shard_order) < int(chunk_order)
        and (default_child is None or int(chunk_order) < int(default_child))
        and (config.data_source or {}).get("reader") != "raster"
    ):
        # The ruled /2 default flip (issue #384): every new store declares the
        # multiresolution grammar. Strictly-interior only — a K == 1 grid, or
        # one whose chunks are the cells themselves, has no valid /2 default
        # and keeps the /1 fallback below.
        knob = {**knob, "overviews": int(chunk_order)}
    fields, excluded = declared_fields(config)
    if knob.get("overviews") is not None:
        resolutions = normalize_overviews(knob["overviews"])
        # Ordering/range live here, not in ``normalize_overviews`` (they need the
        # grid orders) — and this is the ONLY validation the templating path
        # gets: the Lambda worker builds its config with ``load_config_from_dict``,
        # which never calls ``validate_config``, then goes straight to
        # ``build_manifest``. A grid-less retrofit config (no ``output.grid``,
        # the ``declare_pyramid`` shape) is the one case skipped: there is no
        # child order to check against here, and ``declare_pyramid``
        # re-validates against the MANIFEST's own shard_order/cell_order before
        # anything is written.
        grid_child = (config.output.get("grid") or {}).get("child_order")
        if grid_child is not None:
            validate_overviews(
                resolutions, parent_order=int(shard_order), child_order=int(grid_child)
            )
        # The manifest records the FULLY EXPANDED list — the leaf entry plus
        # the fixed every-order ladder to 0 (espg ruling; readers never
        # re-derive).
        levels = expand_overviews(resolutions, parent_order=int(shard_order))
        fold = _fold_plan(knob, [e["node"] for e in levels])
        return overview_block_v2(knob, levels, fold, fields, excluded)
    spacing = int(knob.get("spacing") or DEFAULT_SPACING)
    if knob.get("orders") is not None:
        orders = sorted({int(k) for k in knob["orders"]}, reverse=True)
    else:
        orders = list(range(int(shard_order) - spacing, -1, -spacing))
    fold_source, exact_levels = _fold_plan(knob, orders)
    if excluded and orders:
        warn_excluded(excluded)
    overview: dict = {
        "spacing": spacing,
        "orders": orders,
        "all_time": bool(knob.get("all_time", False)),
        "fold_source": fold_source,
    }
    # ``exact_levels`` is the cascade boundary and nothing else: under the
    # deprecated ``leaves`` source every level is exact, so recording a
    # boundary there would declare a distinction the store does not have.
    if fold_source == "cascade":
        overview["exact_levels"] = exact_levels
    overview["fields"] = fields
    if knob.get("summarize"):
        overview["summarize"] = {str(k): dict(v) for k, v in knob["summarize"].items()}
    return {"spec": PYRAMID_SPEC, "overview": overview}


def _fold_plan(knob: dict, orders: list) -> tuple[str, int]:
    """The declared ``(fold_source, exact_levels)`` pair (issue #376).

    Defensive like the rest of the declaration path: :func:`declare_pyramid`
    reaches this without ``validate_config`` (issue #358), so an unusable
    value warns and falls back to the ratified default rather than writing a
    declaration the sweep would then have to second-guess. ``leaves`` is
    accepted but deprecated — it is the pre-#376 fold whose per-node input is
    the whole subtree.
    """
    raw = knob.get("fold_source")
    fold_source = DEFAULT_FOLD_SOURCE if raw is None else str(raw)
    if fold_source not in FOLD_SOURCES:
        logger.warning(
            f"pyramid: unknown fold_source {raw!r} (known: {list(FOLD_SOURCES)}); "
            f"declaring the default {DEFAULT_FOLD_SOURCE!r}"
        )
        fold_source = DEFAULT_FOLD_SOURCE
    raw = knob.get("exact_levels")
    try:
        exact_levels = DEFAULT_EXACT_LEVELS if raw is None else int(raw)
    except (TypeError, ValueError):
        exact_levels = 0
    if exact_levels < 1:
        logger.warning(
            f"pyramid: exact_levels must be an int >= 1 (got {raw!r}); declaring the "
            f"default {DEFAULT_EXACT_LEVELS}"
        )
        exact_levels = DEFAULT_EXACT_LEVELS
    if fold_source == "leaves" and orders:
        logger.warning(
            "pyramid: fold_source 'leaves' is DEPRECATED (issue #376) — every declared "
            "level re-folds the whole subtree from the raw leaves, so per-node memory "
            "grows with the subtree and the leaves are read once per level; the default "
            "'cascade' folds each coarse level from the level below it instead"
            + (
                " (the declared exact_levels is ignored: every level is exact here)"
                if knob.get("exact_levels") is not None
                else ""
            )
        )
    elif orders and exact_levels >= len(orders):
        # A boundary at or past the last declared level buys the DEPRECATED
        # regime wholesale — every level folds from the leaves — while the
        # block still declares 'cascade'. Say so here: this is the only place
        # that knows the derived schedule (validate_config has no shard_order).
        logger.warning(
            f"pyramid: exact_levels {exact_levels} covers all {len(orders)} declared levels "
            f"{orders}, so EVERY level folds from the raw leaves — the DEPRECATED 'leaves' "
            f"regime (issue #376) under a 'cascade' declaration; lower it to keep the cascade"
        )
    return fold_source, exact_levels


def _update_manifest_pyramid(store_root, folded: dict, store_kwargs) -> bool:
    """Record materialized overview orders in the manifest pyramid block.

    The one manifest key the sweep may touch (D11: the block is populated/
    updated by the §7 sweep; it is excluded from the frozen resume keys, so
    this RMW can never brick appends). Fail-open — the declaration readers
    key on is untouched; ``materialized`` is a convenience actual.

    ``folded`` is ``{order: fold_source}`` for the orders this pass touched,
    with ``None`` for an order that is materialized but wrote nothing this
    pass (current, or an empty fold): its recorded regime stays as it is,
    because the artifact on disk did not change. ``fold_sources`` carries the
    per-level provenance forward (issue #376) so the manifest answers "which
    regime is this level in" without opening an overview — a level's regime
    can differ from the declaration when a level was materialized under an
    earlier one, and the declaration is what the NEXT sweep applies, never a
    claim about what is on disk.
    """

    from zagg.hive import MANIFEST_NAME, _utcnow, read_manifest
    from zagg.store import open_object_store, put_object

    try:
        fresh = read_manifest(store_root, **store_kwargs)
        if fresh is None:
            return False
        block = fresh.setdefault("pyramid", {}).setdefault("overview", {})
        prior = block.get("materialized") or {}
        known = set(prior.get("orders") or [])
        sources = dict(prior.get("fold_sources") or {})
        sources.update({str(int(k)): v for k, v in folded.items() if v is not None})
        block["materialized"] = {
            "orders": sorted(known | {int(k) for k in folded}),
            "fold_sources": sources,
            "generated_at": _utcnow(),
        }
        put_object(
            open_object_store(store_root, **store_kwargs),
            MANIFEST_NAME,
            json.dumps(fresh, indent=1).encode(),
        )
        return True
    except Exception as e:
        logger.warning(f"sweep[overview]: manifest pyramid update failed (fail-open): {e}")
        return False


def declare_pyramid(
    store_root: str, config, *, overviews=None, chunk_order=None, store_kwargs=None
) -> dict:
    """Install/refresh the manifest ``pyramid`` declaration on an EXISTING store.

    The issue #358 retrofit tool: declaration is no longer birth-only. The
    block is re-derived from the supplied pipeline config (the authoritative
    source — the same :func:`build_pyramid_block` path template time uses) at
    the manifest's own ``shard_order``, validated against store truth, and
    RMW'd into the manifest. Unlike the sweep's fail-open ``materialized``
    update (:func:`_update_manifest_pyramid`), this is the user's explicit
    operation: every failure raises — a missing or malformed manifest, a config
    whose semantics the store's frozen hash denies, a declaration the store
    contradicts — and nothing is half-written (the one PUT is the whole write).

    Two validation sources, covering the two halves of the recipe:

    * **Typing** — ONE committed leaf, found through the store's run records
      (:func:`zagg.sweep.discover_leaves`, the sweep's own tree-enumeration-free
      discovery: one shallow root LIST of the run records, never a recursive
      LIST), read at the manifest ``cell_order``. Leaves are the layer the
      overview fold actually reads, so they are what can falsify the declared
      **presence, dtype, and D18 ragged element shape** of each field. They
      cannot falsify more than that: no leaf records which reducer produced it.
      A store with no committed leaf yet is still declarable — field-level
      checks are then skipped and the summary says which skip it was.
    * **Semantics** — the manifest's own frozen ``semantic_hash``
      (:func:`_semantic_guard`), which is what covers the ``method``/
      ``nan_policy`` half the leaf is silent on. The D19 ``aggregation.yaml``
      core is NOT used (it is itself config-derived and written fail-open).

    ``overviews=`` / ``chunk_order=`` (issue #520) are the ``/2`` retrofit
    levers, and they are what makes this tool the second step of the
    ``/1 -> /2`` upgrade recipe. Without them the grid-less retrofit path can
    only ever declare ``/1``: the #384 default flip fires off the GRID's
    resolved ``chunk_order``, which there is no grid here to ask, and the
    config a published store was built with has no reason to spell
    ``output.pyramid.overviews``. ``overviews=`` spells the leaf resolutions
    outright (overriding whatever the config's own pyramid knob says,
    ``false`` included — logged, and the point of the lever); ``chunk_order=``
    supplies the one number the default flip is missing. Both are validated
    against the MANIFEST's ``shard_order``/``cell_order`` — the store's truth,
    not the config's — before anything is written, and the two silent-no-op
    spellings refuse by name rather than falling back to ``/1``
    (:func:`zagg.pyramid.retrofit_declaration`). The summary records which
    lever declared the block in ``declared_via``.

    Idempotent RMW: an identical existing declaration is not re-PUT; a
    changed one is rewritten PRESERVING any ``materialized`` actuals (they
    inventory overview artifacts already on disk — overviews at
    now-undeclared orders stay as regenerable-cache debris, D24 option A).
    An ``output.pyramid: false`` config installs the declared-off block:
    recording absence is a valid retrofit.

    Returns a summary dict carrying ``fold_source`` (the declared fold
    regime, issue #376), ``fields`` (``{name: class}``), ``validated`` (what
    store truth was checked), ``previous`` (``absent``/``identical``/
    ``replaced``), and ``updated`` (whether a PUT happened), plus the
    revision's schedule key and NOT the other one's — mirroring the manifest
    block itself: ``orders`` under ``/1``, ``overviews`` (the normalized
    grouped form, issue #382) under ``/2``. An empty ``orders`` is ``/1``'s
    declared-off signal, so a ``/2`` summary must not carry the key at all.
    """

    from zagg.hive import MANIFEST_NAME, _frozen_matches, read_manifest
    from zagg.pyramid import retrofit_declaration
    from zagg.store import open_object_store, put_object

    store_kwargs = dict(store_kwargs or {})
    manifest = read_manifest(store_root, **store_kwargs)
    if manifest is None:
        raise ValueError(
            f"no {MANIFEST_NAME} at {store_root} — not a hive store root; "
            f"declare_pyramid retrofits existing hive stores only"
        )
    # Both order keys up front: ``cell_order`` is only consumed by the leaf probe,
    # and a manifest that parses without it should refuse HERE rather than
    # KeyError after the whole discovery cost has been paid.
    if not isinstance(manifest, dict) or any(
        manifest.get(k) is None for k in ("shard_order", "cell_order")
    ):
        raise ValueError(
            f"the {MANIFEST_NAME} at {store_root} declares no shard_order/cell_order — "
            f"not a hive store manifest; declare_pyramid retrofits existing hive stores only"
        )
    shard_order = int(manifest["shard_order"])
    config, default_chunk, declared_via = retrofit_declaration(
        config,
        overviews=overviews,
        chunk_order=chunk_order,
        parent_order=shard_order,
        child_order=int(manifest["cell_order"]),
    )
    block = build_pyramid_block(config, shard_order, default_chunk)
    if (overviews is not None or chunk_order is not None) and "overviews" not in block:
        # Both levers exist to produce a /2 block; anything that swallowed one must
        # say so rather than install the /1 fallback the caller explicitly reached
        # past. Raster is refused by name upstream now (``retrofit_declaration``,
        # both arms), so what survives to here is the ``chunk_order=`` arm on a
        # NON-raster config: the lever was validated against the MANIFEST's
        # ``cell_order`` while the #384 flip gates on the CONFIG's own
        # ``output.grid.child_order``, and orders are packaging — excluded from
        # the semantic core — so ``_semantic_guard`` cannot catch a config whose
        # grid disagrees with the store (review finding, issue #520).
        raise ValueError(
            f"declare_pyramid({declared_via}) did not produce a zagg-pyramid/2 block: the "
            f"issue #384 default flip needs the order strictly inside this CONFIG's own "
            f"`output.grid.child_order` "
            f"({(config.output.get('grid') or {}).get('child_order')!r}), which is not where "
            f"the store's manifest puts its cells (cell_order {int(manifest['cell_order'])}) "
            f"— the config's grid does not describe this store. Pass overviews= to spell the "
            f"schedule outright; nothing was written"
        )
    # Compare (and write) canonical JSON: ``prior`` came back through
    # ``json.loads``, so any non-JSON-primitive surviving the derivation (a tuple
    # in a ``summarize`` declaration, say) would differ from its round-tripped
    # self and re-PUT on every call — against the headline idempotency property.
    # It also surfaces an unserializable block HERE rather than at the PUT,
    # after the whole store-truth probe has been paid for.
    block = json.loads(json.dumps(block))
    if "overviews" in block:
        # The /2 declaration (issue #382; block-level expanded list per the
        # espg shape ruling): re-validate the LEAF resolutions against the
        # MANIFEST's own orders — config validation saw the config's grid
        # block, and the retrofit contract is that the store's truth wins.
        # The first entry is the leaf entry (its node is the shard order the
        # expansion was derived at); the ladder above it is fixed law, valid
        # by construction given a valid leaf list.
        from zagg.pyramid import validate_overviews

        validate_overviews(
            block["overviews"][0]["cells"],
            parent_order=shard_order,
            child_order=int(manifest["cell_order"]),
        )
    bad = [k for k in block["overview"].get("orders") or [] if not 0 <= int(k) < shard_order]
    if bad:
        raise ValueError(
            f"declared orders {bad} are not ancestor orders of the manifest "
            f"shard_order {shard_order} — the config does not match this store"
        )
    semantic = _semantic_guard(manifest, config)
    validated = _validate_block_against_store(store_root, manifest, block, store_kwargs)
    # Re-read immediately before the RMW, the same discipline
    # :func:`_update_manifest_pyramid` uses: validation above is slow (run-record
    # discovery + leaf GETs), this PUT rewrites the WHOLE manifest, and a
    # concurrent sweep's ``materialized`` update landing in that window would be
    # silently reverted by the pre-validation copy. Frozen keys are re-checked so
    # the block is never installed on a manifest validation never saw.
    fresh = read_manifest(store_root, **store_kwargs)
    if fresh is None or not _frozen_matches(fresh, manifest):
        raise ValueError(
            f"the {MANIFEST_NAME} at {store_root} changed under declare_pyramid's "
            f"validation window (frozen keys differ, or it vanished) — nothing was "
            f"written; re-run against the settled store"
        )
    prior = fresh.get("pyramid")
    # A non-dict prior (hand-edited ``"pyramid": "off"``) is not an error: it is
    # not this module's grammar, so it carries no actuals to preserve and is
    # simply replaced — but it must not AttributeError on the way there.
    prior_overview = prior.get("overview") if isinstance(prior, dict) else None
    materialized = prior_overview.get("materialized") if isinstance(prior_overview, dict) else None
    if materialized is not None:
        block["overview"]["materialized"] = materialized
    summary = {
        # The schedule key of the declared revision, and only that one: this
        # dict is what ``--declare-pyramid`` prints, and an empty ``orders``
        # is /1's wire signal for "pyramid declared OFF" (§4.5) — printing it
        # beside a /2 ``overviews`` list would read as a store with no pyramid.
        **(
            {"overviews": [dict(e) for e in block["overviews"]]}
            if "overviews" in block
            else {"orders": list(block["overview"].get("orders") or [])}
        ),
        # The retrofit's user sees which fold regime they just declared for
        # every future sweep of this store (issue #376) — it is printed by
        # ``python -m zagg.sweep --declare-pyramid`` and nowhere else.
        "fold_source": block["overview"].get("fold_source"),
        # Which lever declared this block (issue #520): the config as supplied,
        # or one of the two /2 retrofit overrides.
        "declared_via": declared_via,
        "fields": {n: m.get("class") for n, m in (block["overview"].get("fields") or {}).items()},
        "validated": f"{validated}; {semantic}",
        "previous": "absent" if prior is None else "identical" if prior == block else "replaced",
        "updated": prior != block,
    }
    if prior == block:
        logger.info("declare_pyramid: the manifest already carries this declaration; no write")
        return summary
    fresh["pyramid"] = block
    put_object(
        open_object_store(store_root, **store_kwargs),
        MANIFEST_NAME,
        json.dumps(fresh, indent=1).encode(),
    )
    return summary


def _semantic_guard(manifest: dict, config) -> str:
    """Refuse a config whose semantics the store's frozen ``semantic_hash`` denies.

    The leaf probe (:func:`_field_drift`) can falsify TYPING only — no leaf
    records which reducer produced a field — so nothing there contradicts a
    declaration of ``method: "max"`` over a store holding minima, and
    :func:`build_pyramid_block` puts exactly that fold law into the block. The
    store does record its reducers, in the one place the repo calls
    authoritative: the D19 frozen ``semantic_hash`` (issue #299), a digest over
    the whole ``aggregation`` block. Comparing it here is what makes the issue
    #358 contract ("a wrong config cannot install a fold recipe the store
    contradicts") true of the fold LAW and not just of dtypes.

    ``output.*`` is not in the semantic core, so the intended retrofit config —
    the original plus ``output.pyramid`` — hashes identically; the guard cannot
    false-refuse on the pyramid edit itself. It compares only when the manifest
    declares the key, the same both-sides-present exemption
    :func:`zagg.hive._frozen_matches` gives pre-#299 stores (a pre-#344 retrofit
    target may well be one). Returns the note recorded in the summary.
    """
    from zagg.semantics import semantic_fingerprint, semantic_hash

    stored = manifest.get("semantic_hash")
    if not stored:
        logger.warning(
            "declare_pyramid: the manifest carries no semantic_hash (pre-#299 store) — "
            "the config's aggregation semantics could NOT be verified against the store; "
            "the declared fold methods are taken on trust"
        )
        return "semantic_hash absent (pre-#299 store — fold methods unverified)"
    supplied = semantic_hash(config)
    if supplied != stored:
        raise ValueError(
            f"config semantics {semantic_fingerprint(supplied)} != the store's frozen "
            f"semantic_hash {semantic_fingerprint(stored)} — this config did not build this "
            f"store, so the fold methods it declares (which no leaf records, and which the "
            f"field checks cannot falsify) are not the store's; declare_pyramid refuses. "
            f"Retrofit with the ORIGINAL config: output.* is not in the semantic core, so "
            f"adding output.pyramid to it hashes identically"
        )
    return f"semantic_hash {semantic_fingerprint(stored)}"


def _validate_block_against_store(store_root, manifest, block, store_kwargs) -> str:
    """The pre-write store-truth check: refuse a recipe the store contradicts.

    Probes the run records for a committed leaf (the first hit wins — discovery
    has already parsed every record, so the scan costs one GET per ref until it
    lands) and checks every declared field against the
    leaf's stored array: presence, dense dtype for the exact class, the D18
    ragged element declaration (element dtype + ``inner_shape``) for the
    approximate class. Returns the "what was checked" summary string;
    raises ``ValueError`` naming every drifted field. No committed leaf is
    not an error — the store is declarable before its first commit — but
    the skip is loud and recorded in the summary.
    """
    import zarr

    from zagg.hive import read_commit, shard_leaf_path
    from zagg.store import open_store
    from zagg.sweep import discover_leaves

    fields = block["overview"].get("fields") or {}
    if not fields:
        # The declared-off block carries no ``fields`` key at all; an ON
        # declaration over an empty aggregation carries an empty one. Reporting
        # both as "declared off" would read as a lie in the summary.
        return (
            "nothing to check (declared off)"
            if "fields" not in block["overview"]
            else "nothing to check (no aggregation variables to declare)"
        )
    # Un-capped on purpose: ``discover_leaves`` has ALREADY paid the whole cost
    # (a root LIST plus a GET + parse of every run record), so truncating its
    # result bounds nothing — it only risks giving up while committed leaves
    # remain. Each extra ref costs one ``read_commit`` GET, and the loop stops at
    # the first hit; the read is read-only, which on S3 also picks the shorter
    # readonly retry policy (issue #186).
    refs = discover_leaves(store_root, store_kwargs=store_kwargs)
    leaf = None
    for key, window in refs:
        path = shard_leaf_path(store_root, key, window=window)
        if read_commit(open_store(path, read_only=True, **store_kwargs)) is not None:
            leaf = path
            break
    if leaf is None:
        # Two very different stores that must not report the same thing: no run
        # records at all is a genuinely pre-commit store, while refs that are all
        # torn/rolled-back debris is a store whose validation was skipped over
        # real work. Neither refuses — a store with no committed leaf stays
        # declarable, which is the point of the retrofit — but the skip is loud
        # and says which case it was.
        detail = (
            "no run records at the product root"
            if not refs
            else f"none of the {len(refs)} run-record refs is a committed leaf"
        )
        logger.warning(
            f"declare_pyramid: no committed leaf to probe ({detail}) — field-level "
            f"validation skipped; only the manifest root was checked"
        )
        return f"manifest only ({detail}; fields unvalidated)"
    group = zarr.open_group(
        open_store(leaf, **store_kwargs),
        path=str(int(manifest["cell_order"])),
        mode="r",
        zarr_format=3,
    )
    drift = [e for n, m in sorted(fields.items()) if (e := _field_drift(group, n, m))]
    if drift:
        raise ValueError(
            f"pyramid declaration contradicts the store (checked leaf {leaf}): " + "; ".join(drift)
        )
    return f"leaf {leaf}"


def _field_drift(group, name, meta) -> str | None:
    """One declared field's mismatch against its stored leaf array, or ``None``.

    The ragged branch also gates the D18 attrs' **spec revision** against
    :data:`zagg.grids.base.RAGGED_SPEC`, mirroring the readers' posture
    (``readers.tdigest_tensor._open_ragged`` raises on mismatch) rather than
    inventing a second policy. A store whose ragged layout this zagg cannot
    read is the purest instance of the tool's contract — a fold recipe the
    store contradicts: without the gate the retrofit installs a declaration
    promising overviews the sweep then fails to read at FOLD time, later and
    with a worse error. ``zagg-ragged/2`` is a live migration path (issue
    #210 moves the element declaration into the zarr data type), so this is
    not hypothetical (espg-ruled, issue #358).

    The same branch gates the §2.0 ``weights`` declaration for the same
    reason (issue #424): it is what :func:`check_weights_match` refuses a
    fold over, so a disagreement caught here is the retrofit refusing, and
    one missed here is every leaf warning at fold time. A stored value this
    zagg does not define RAISES out of the probe rather than reporting drift
    — that store is unreadable, not merely mis-declared.
    """
    from zagg.grids.base import (
        RAGGED_ELEMENT_ATTR,
        RAGGED_SPEC,
        TIMES_ATTR,
        weights_declaration,
    )

    try:
        arr = group[name]
    except KeyError:
        return f"field {name!r} is declared but absent from the leaf"
    if meta["class"] == "approximate":
        raw = arr.attrs.get(RAGGED_ELEMENT_ATTR)
        ragged = dict(raw) if isinstance(raw, dict) else {}
        element = ragged.get("element") or {}
        if not element:
            return (
                f"field {name!r}: declared approximate (ragged) but the stored "
                f"array carries no ragged element declaration"
            )
        if ragged.get("spec") != RAGGED_SPEC:
            return (
                f"field {name!r}: the stored array declares ragged spec "
                f"{ragged.get('spec')!r}; this zagg understands {RAGGED_SPEC!r} only — "
                f"a newer writer's layout must be adopted deliberately, not folded "
                f"blind (declaring overviews over it would promise a fold that fails "
                f"at sweep time)"
            )
        declared_dt = np.dtype(meta.get("dtype") or "float32")
        # Explicit, because ``np.dtype(None)`` is float64: an element block that
        # declares no dtype would otherwise silently VALIDATE a float64
        # declaration against a store that says nothing.
        stored_dt = element.get("dtype")
        if stored_dt is None:
            return f"field {name!r}: the stored ragged element declaration carries no dtype"
        if np.dtype(stored_dt) != declared_dt:
            return f"field {name!r}: ragged element dtype {stored_dt} != declared {declared_dt}"
        stored_inner = [int(s) for s in (element.get("shape") or [-1])[1:]]
        declared_inner = [int(s) for s in meta.get("inner_shape") or [2]]
        if stored_inner != declared_inner:
            return f"field {name!r}: ragged inner_shape {stored_inner} != declared {declared_inner}"
        # The §2.0 weights declaration is one more thing a leaf can falsify,
        # and it is stamped on the array this probe already opened: absent it
        # here, ``declare_pyramid`` installs a flux declaration over a counts
        # store cleanly, and every leaf then fails ``check_weights_match`` at
        # FOLD time as a per-leaf warning (review finding, issue #424).
        # Absent on either side is counts, exactly as the fold gate reads it.
        stored_weights = weights_declaration(dict(arr.attrs))
        declared_weights = meta.get("weights") or "counts"
        if stored_weights != declared_weights:
            return (
                f"field {name!r}: stored weights declaration {stored_weights!r} != "
                f"declared {declared_weights!r} — merges are legal only between "
                f"matching declarations (spec §2.0)"
            )
        # A declared companion channel (issue #410) is one more thing a leaf can
        # falsify, and the same argument applies: without the check
        # ``declare_pyramid`` installs a companion declaration over a store with
        # no sibling, and every fold then dies reading an array that is not
        # there. Each declaration is checked on its own SIBLING, which is where
        # §8.3/§9 put it.
        for kwarg, sibling in field_companions(name, meta):
            binding = ragged.get("locations") if kwarg == "locations" else arr.attrs.get(TIMES_ATTR)
            try:
                sib = group[sibling]
            except KeyError:
                return (
                    f"field {name!r}: declared a {kwarg} channel but the sibling "
                    f"{sibling!r} is absent from the leaf (spec §8.3/§9, §1.1)"
                )
            if binding != sibling:
                return (
                    f"field {name!r}: the payload binds {kwarg} {binding!r}, not the "
                    f"declared {sibling!r} — a reader binds the channel by that "
                    f"declaration (spec §1.2/§8.3)"
                )
            # The declaration itself, strict-checked: §9.2 imports §8.4, so
            # companions compose only on matching ``{shape, grammar}`` and a
            # writer joining ones that differ MUST refuse. The per-centroid shape
            # defines one of each, so the strict check IS the match check — and
            # it RAISES rather than reporting drift (docstring posture: a sibling
            # declaring an unimplemented shape is unreadable, not merely
            # mis-declared).
            check_companion_match(dict(sib.attrs), name, kwarg)
            sib_element = (dict(sib.attrs.get(RAGGED_ELEMENT_ATTR) or {})).get("element") or {}
            sib_dtype = sib_element.get("dtype")
            # Normalized like the payload's compare above: an equivalent
            # spelling (``"<u8"``) is the same dtype and must not read as drift.
            if sib_dtype is None or np.dtype(sib_dtype) != np.dtype("uint64"):
                return (
                    f"field {name!r}: the {kwarg} sibling declares element dtype "
                    f"{sib_dtype!r}, not 'uint64' (spec §6.1)"
                )
            # The words are decoded as a flat vector (``decode_digest(..., ())``),
            # so an inner shape would be silently reinterpreted by the fold —
            # exactly the store-contradicts-the-recipe class this gate exists for.
            if [int(x) for x in (sib_element.get("shape") or [-1])[1:]]:
                return (
                    f"field {name!r}: the {kwarg} sibling declares element shape "
                    f"{sib_element.get('shape')!r}; the channel is one flat uint64 "
                    f"word per centroid row (spec §8.3/§9, §1.1)"
                )
    elif meta["class"] == "packed":
        from zagg.stats.composition import COMPOSITION_ATTR, COMPOSITION_SPEC, LANES

        declared_dt = np.dtype(meta.get("dtype") or "uint64")
        if arr.dtype != declared_dt:
            return f"field {name!r}: dtype {arr.dtype} != declared {declared_dt}"
        stored = dict(arr.attrs.get(COMPOSITION_ATTR) or {})
        # A packed declaration over an UNSTAMPED array has no reader contract
        # at all — the same posture the approximate arm takes on a missing
        # ragged element block.
        if not stored:
            return (
                f"field {name!r}: declared packed but the stored array carries no "
                f"{COMPOSITION_ATTR!r} attrs block (spec §3.3)"
            )
        # The word's lanes are unpacked BY POSITION under this writer's
        # constants, so a store declaring another spec or another lane order
        # would fold into a well-formed and wrong word — the store-contradicts-
        # the-declaration class this gate exists for, guarded on the read side
        # exactly as ``config`` guards it on the write side.
        if stored.get("spec") != COMPOSITION_SPEC:
            return (
                f"field {name!r}: the stored array declares composition spec "
                f"{stored.get('spec')!r}; this zagg folds {COMPOSITION_SPEC!r} only "
                f"(lanes are merged by position)"
            )
        if [str(lane) for lane in stored.get("lanes") or ()] != list(LANES):
            return (
                f"field {name!r}: the stored composition lanes {stored.get('lanes')!r} "
                f"are not the {COMPOSITION_SPEC} order (spec §3.1)"
            )
        # The §3.3 linkage is what the fold's ``n`` inputs come from, so a
        # store whose stored block binds a DIFFERENT digest than the
        # declaration would fold every word against the wrong divisor —
        # checked on the same array this probe already opened.
        stored_of = stored.get("of")
        if stored_of is not None and meta.get("of") is not None and stored_of != meta["of"]:
            return (
                f"field {name!r}: the stored composition block binds of={stored_of!r}, "
                f"not the declared {meta['of']!r} — the fold's n inputs are that "
                f"digest's per-cell weights (spec §3.3/§3.4)"
            )
    elif meta["class"] == "exact":
        declared_dt = np.dtype(meta.get("dtype") or "float32")
        if arr.dtype != declared_dt:
            return f"field {name!r}: dtype {arr.dtype} != declared {declared_dt}"
    return None


# ---------------------------------------------------------------------------
# Phase B: the overview writer — the family's whole-tree sweep.
# ---------------------------------------------------------------------------

#: Per-node overview bookkeeping object (window inventory + skip-if-current
#: stamps), sibling to the other families' ``{family}.rollup.json`` — the same
#: closed name grammar, so the §5 walker's child classification is unaffected.
ENVELOPE_NAME = "overview.rollup.json"


def sweep_overviews(
    store_root: str, manifest: dict, by_shard: dict, *, store_kwargs=None, min_order: int = 0
) -> dict:
    """Generate/refresh overview zarrs at the manifest-declared orders (D22).

    ``by_shard`` is the engine's normalized dirty work set
    (``{shard_decimal: {window, ...}}``); the walk visits ONLY ancestor nodes
    of those shards at each declared order. The full descendant leaf set per
    node comes from the dirty set unioned with the root ``coverage.moc``
    (run record + MOC — never a LIST); with the default family order the MOC
    family has just refreshed that root in the same pass — except in a
    PARTITIONED pass, which defers that refresh, so the root MOC is this
    pass's input as well as the finisher's obligation and a degraded read is
    reported as ``root_moc_stale`` (:func:`_candidate_decimals`).

    Orders are walked **finest first**, which is what makes the default
    cascade (issue #376) possible: the finest ``exact_levels`` levels fold
    from the leaves, and each coarser level then folds from the level this
    same pass has just materialized (:func:`_fold_sources`,
    :func:`_cascade_node`) — bounded per-node input, and the leaves read once
    for the whole pyramid instead of once per level.

    Idempotent: a (node, window) whose stored generation stamp (merged-leaf
    count + max leaf stamp timestamp) AND content hash both match the freshly
    folded payload — and whose zarr is confirmed present and stamped, since the
    envelope and the artifact are two objects (D9) — is skipped; the hash is
    the same-second backstop the engine's payload compare provides for JSON
    families.

    Returns the standard ``written``/``current``/``empty``/``failed`` counts
    plus ``declared`` (whether the manifest carries a usable overview
    declaration at all) and ``sweepable`` (whether THIS zagg can fold the
    declared revision — ``False`` only for the ``/2`` grammar of issue #382,
    whose materialization arrives with issues #383/#384). Both keys are
    present on every path: this dict is serialized into the sweep run record
    (:func:`zagg.sweep._write_sweep_record`), an operator-facing artifact
    whose schema must not vary by revision.

    ``min_order`` (issue #377) is the sweep partition's split order. Two things
    span partitions and so are left to the coarse-level finisher: declared
    orders coarser than it (``deferred_orders``), and the manifest pyramid RMW
    at the store root (``manifest_deferred``, carrying the order set the RMW
    would have unioned as ``materialized_orders`` plus the regimes that wrote
    them as ``materialized_fold_sources`` — issue #376's §4.5 actuals). The
    ``/2`` gate above fires BEFORE the partition clamp: a partitioned pass
    over a ``/2``-declaring store refuses exactly like an unpartitioned one
    (nothing is folded, so nothing is deferred).
    """
    from zagg.pyramid import PYRAMID_SPEC_V2
    from zagg.store import open_object_store, put_object

    store_kwargs = dict(store_kwargs or {})
    counts: dict = {
        "written": 0,
        "current": 0,
        "empty": 0,
        "failed": 0,
        "declared": True,
        "sweepable": True,
    }
    pyramid = manifest.get("pyramid") or {}
    decl = pyramid.get("overview") if isinstance(pyramid, dict) else None
    spec = pyramid.get("spec") if isinstance(pyramid, dict) else None
    declared_v2 = isinstance(pyramid, dict) and pyramid.get("overviews") is not None
    if spec == PYRAMID_SPEC_V2 or declared_v2:
        # The /2 (node, cells) grammar is swept by the STAGED sweep (issue
        # #384: zagg.sweep_stage.run_stage_sweep, `python -m zagg.sweep
        # <root> --stages`, or the post-fleet `output.sweep: "stages"`
        # chaining) — the declared-not-yet-sweepable gate of PR #389 is
        # retired. This family generates nothing for /2: its /1 fold reads
        # raw leaves per level, which the column regime exists to end, and
        # the two regimes must never mix. /1 stores sweep exactly as before.
        counts["sweepable"] = True
        counts["regime"] = "stages"
        # The `spec`-only arm reaches here with no list checked: a /2 marker
        # carrying no overviews list declares nothing, and must report the
        # same `declared: False` the /1 branch below would give it.
        counts["declared"] = bool(declared_v2 and pyramid.get("overviews"))
        logger.info(
            f"sweep[overview]: the manifest pyramid declaration is {spec!r} — /2 "
            f"stores are swept by the staged sweep (issue #384: run_stage_sweep, "
            f"`python -m zagg.sweep --stages`, or output.sweep 'stages'); the "
            f"overview family generates nothing here"
        )
        return counts
    if not isinstance(decl, dict) or not decl.get("orders"):
        counts["declared"] = False
        logger.info(
            "sweep[overview]: no pyramid overview declaration in the manifest "
            "(template-time, D22); nothing to generate"
        )
        return counts
    if decl.get("summarize"):
        logger.warning(
            f"sweep[overview]: declared derived summaries {sorted(decl['summarize'])} are not "
            f"generated yet — no roster-kind ragged field ships (D24 opt-in, issue #265); skipping"
        )
    from zagg.column import _is_composable

    fields = {
        n: dict(m)
        for n, m in (decl.get("fields") or {}).items()
        if isinstance(m, dict) and _is_composable(m)
    }
    if not fields:
        logger.info("sweep[overview]: no composable fields declared; nothing to generate")
        return counts
    shard_order = int(manifest["shard_order"])
    cell_order = int(manifest["cell_order"])
    windowed = manifest.get("temporal") is not None
    orders = sorted({int(k) for k in decl["orders"]}, reverse=True)
    bad = [k for k in orders if not (0 <= k < shard_order)]
    if bad:
        logger.warning(
            f"sweep[overview]: declared orders {bad} are not ancestor orders of "
            f"shard_order {shard_order}; skipping them"
        )
        orders = [k for k in orders if k not in bad]
    # Plans derive from the FULL declared schedule, before the partition clamp:
    # a level's cascade source is the next FINER declared level (issue #376),
    # which the clamp never removes (it drops only the coarse tail), so the
    # surviving levels' plans are identical either way — but _fold_sources'
    # declaration-level warnings must describe the declaration, not one
    # partition's clamped view of it.
    plans = _fold_sources(decl, orders, cell_order, shard_order)
    if deferred := [k for k in orders if k < int(min_order)]:
        counts["deferred_orders"] = deferred
        orders = [k for k in orders if k not in deferred]
        logger.info(
            f"sweep[overview]: orders {deferred} are coarser than the partition split order "
            f"{min_order}; they span partitions and are the finisher's (issue #377)"
        )
    candidates, moc_stale = _candidate_decimals(store_root, shard_order, by_shard, store_kwargs)
    if moc_stale:
        counts["root_moc_stale"] = True
    store = open_object_store(store_root, **store_kwargs)
    materialized: dict[int, str | None] = {}
    for k in orders:
        nodes = sorted({_node_at(d, k) for d in by_shard})
        for node in nodes:
            node_shards = sorted(d for d in candidates if d.startswith(node))
            dirty_windows = set()
            for d in by_shard:
                if d.startswith(node):
                    dirty_windows |= by_shard[d]
            envelope = _read_envelope(store, node)
            entries = dict((envelope or {}).get("windows") or {})
            for key, fold_windows in _window_work(decl, windowed, dirty_windows, entries):
                before = counts["written"]
                entry = _roll_node(
                    store_root,
                    node,
                    k,
                    plans[k],
                    key,
                    fold_windows,
                    node_shards,
                    fields,
                    cell_order,
                    shard_order,
                    windowed,
                    entries.get(key),
                    counts,
                    store_kwargs,
                )
                if entry is not None:
                    entries[key] = entry
                    if counts["written"] > before:
                        # §4.5's fold_sources is an ACTUAL: record the regime
                        # that just WROTE, never the plan. A carried-forward
                        # entry (current, or an empty fold) keeps whatever
                        # regime made the artifact that is still on disk.
                        materialized[k] = entry["fold_source"]
            if entries:
                # The order IS materialized (an entry means an overview) even
                # when nothing was written this pass; its regime may not be
                # known here, and ``None`` leaves the recorded one standing.
                materialized.setdefault(k, None)
            fresh = {
                "spec": _sweep().SWEEP_SPEC,
                "family": "overview",
                "node": node,
                "order": k,
                "windows": entries,
            }
            if entries and fresh != envelope:
                put_object(
                    store,
                    f"{_node_rel(node)}/{ENVELOPE_NAME}",
                    json.dumps(fresh, indent=1).encode(),
                )
    if min_order:
        # The manifest pyramid RMW is a store-ROOT shared write: 2^n partitions
        # racing it would lose updates. It belongs to the finisher, the one
        # invoke that runs alone after the partitions land (issue #377).
        # Carry the payload the RMW would have written: the per-partition
        # record is the only durable trace, and "somebody owes an update"
        # without saying WHAT to write makes the finisher re-derive it. Since
        # issue #376 that payload is the fold-source actuals too — the regime
        # that WROTE each order this pass (§4.5's fold_sources shape), never
        # the plan.
        counts["manifest_deferred"] = True
        counts["materialized_orders"] = sorted(materialized)
        if sources := {str(int(k)): v for k, v in materialized.items() if v is not None}:
            counts["materialized_fold_sources"] = sources
    elif counts["written"]:
        counts["manifest_updated"] = _update_manifest_pyramid(
            store_root, materialized, store_kwargs
        )
    return counts


def _sweep():
    """The engine module (lazy: sweep.py registers this module's family)."""
    import zagg.sweep

    return zagg.sweep


def _node_rel(decimal: str) -> str:
    return _sweep()._node_rel(decimal)


def _node_at(decimal: str, order: int) -> str:
    """The ancestor prefix of a shard decimal at ``order`` (0 = base)."""
    from zagg.hive import _decimal_base

    return decimal[: len(_decimal_base(decimal)) + order]


def _rel_rank(decimal: str, node: str) -> int:
    """Base-4 rank of ``decimal``'s digit tail beyond the ``node`` prefix."""
    rank = 0
    for ch in decimal[len(node) :]:
        rank = rank * 4 + (int(ch) - 1)
    return rank


def _candidate_decimals(store_root, shard_order, by_shard, store_kwargs) -> tuple[set, bool]:
    """Descendant-leaf candidates and whether the root MOC was unusable.

    Discovery stays LIST-free (D22): untouched sibling shards contribute via
    the root ``coverage.moc`` (default-on for hive). An unpartitioned default
    pass has the MOC family refresh that root earlier in the same pass — but a
    PARTITIONED pass DEFERS that refresh (a store-root shared write, issue
    #377), so the root MOC is a partitioned pass's INPUT as well as its
    deferred output, and what it reads is whatever the last whole-tree sweep
    or ``mode="coverage"`` leg left. That ordering is the finisher's to own: a
    partition handed a RUN-scoped leaf set against a missing root MOC re-folds
    from the run's leaves alone, and :func:`_roll_node` overwrites on a
    generation mismatch, so a complete node overview can be rewritten with
    fewer contributing leaves until the next whole-tree pass repairs it.

    A missing/unusable root MOC therefore degrades to the dirty set with a
    loud warning AND a ``root_moc_stale`` count, so the per-partition record
    shows it rather than only the log (D9: regenerable).
    """
    from zagg.grids.morton import morton_decimal
    from zagg.hive import read_root_coverage, root_coverage_words

    decimals = set(by_shard)
    try:
        env = read_root_coverage(store_root, **store_kwargs)
    except ValueError:
        env = None
    if isinstance(env, dict) and env.get("order") == shard_order:
        try:
            decimals |= {morton_decimal(int(w)) for w in root_coverage_words(env)}
            return decimals, False
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"sweep[overview]: unusable root coverage.moc ({e})")
    if decimals:
        logger.warning(
            "sweep[overview]: no usable root coverage.moc — overviews will fold ONLY "
            "the run's own leaves; sweep the moc family (or the default set) to repair"
        )
    return decimals, True


def _window_work(decl, windowed, dirty_windows, entries) -> list:
    """``(envelope key, [windows to fold])`` work items for one node.

    Per-window overviews inherit window naming (D23); the reserved ``all``
    token is the unwindowed leaf AND the opt-in all-time fold on a windowed
    store (``all_time`` in the declaration; a preexisting all-time entry stays
    maintained even if the declaration later drops the flag). A window whose
    label IS that token can therefore never own a separate overview — same
    basename, same envelope key — so it yields the all-time item alone.
    """
    from zagg.windows import SCHEDULE_NONE_TOKEN

    if not windowed:
        return [(SCHEDULE_NONE_TOKEN, [None])]
    labels = sorted(
        {w for w in dirty_windows if w is not None}
        | {key for key in entries if key != SCHEDULE_NONE_TOKEN}
    )
    work = [(w, [w]) for w in labels]
    if decl.get("all_time") or SCHEDULE_NONE_TOKEN in entries:
        if SCHEDULE_NONE_TOKEN in labels:
            # A window literally labeled with the reserved token (config
            # validation now rejects it; a hand-edited or pre-guard manifest can
            # still carry one). Its per-window overview would resolve to the
            # SAME basename and envelope key as the all-time fold, be
            # overwritten, and never regenerate — so yield only the all-time
            # item, which folds this window in anyway (review finding, #201).
            logger.warning(
                f"sweep[overview]: window label {SCHEDULE_NONE_TOKEN!r} is the reserved "
                f"all-time token (D23) — no separate per-window overview is written for "
                f"it; its leaves fold into the all-time overview instead"
            )
            work = [item for item in work if item[0] != SCHEDULE_NONE_TOKEN]
        work.append((SCHEDULE_NONE_TOKEN, labels))
    return work


def _overview_basename(key: str) -> str:
    """The D23 basename of a (node, window) overview — ``all.zarr`` for the token."""
    from zagg.windows import SCHEDULE_NONE_TOKEN, leaf_name_v3

    return leaf_name_v3(None if key == SCHEDULE_NONE_TOKEN else key)


def _read_envelope(store, node: str) -> dict | None:
    """The node's stored overview envelope, or ``None`` (strict, D9 cache)."""
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        data = obstore.get(store, f"{_node_rel(node)}/{ENVELOPE_NAME}").bytes()
    except (FileNotFoundError, NotFoundError):
        return None
    try:
        envelope = json.loads(bytes(data))
    except ValueError:
        envelope = None
    usable = (
        isinstance(envelope, dict)
        and envelope.get("spec") == _sweep().SWEEP_SPEC
        and envelope.get("family") == "overview"
        and isinstance(envelope.get("windows"), dict)
    )
    if not usable:
        logger.debug(f"sweep[overview]: unusable envelope at node {node}; ignoring")
        return None
    return envelope


def _roll_node(
    store_root,
    node,
    k,
    plan,
    key,
    fold_windows,
    node_shards,
    fields,
    cell_order,
    shard_order,
    windowed,
    existing_entry,
    counts,
    store_kwargs,
):
    """Fold one (node, window) overview; write its zarr unless current.

    ``plan`` is this level's ``(fold_source, source_order)`` (:func:`_fold_sources`):
    a cascading level folds the level below it (:func:`_cascade_node`), an
    exact one folds the leaves (:func:`_fold_node`). Returns the fresh
    envelope entry, the existing one when current (or when nothing
    contributed — an emptied window keeps its prior overview, the same
    append-only posture as the engine's interior fallback), or ``None``.
    """
    fold_source, source_order = plan
    try:
        if fold_source == "cascade":
            fold = _cascade_node(
                store_root,
                node,
                k,
                source_order,
                key,
                node_shards,
                fields,
                cell_order,
                shard_order,
                counts,
                store_kwargs,
            )
        else:
            fold = _fold_node(
                store_root,
                node,
                k,
                fold_windows,
                node_shards,
                fields,
                cell_order,
                shard_order,
                counts,
                store_kwargs,
            )
    except Exception as e:
        logger.warning(f"sweep[overview]: fold failed at node {node} window {key!r}; ({e})")
        counts["failed"] += 1
        return None
    if fold is None:
        counts["empty"] += 1
        return existing_entry
    if (
        isinstance(existing_entry, dict)
        and existing_entry.get("generation") == fold["generation"]
        and existing_entry.get("content_hash") == fold["content_hash"]
        # A stored overview folded the other way is NOT current, even when the
        # bytes agree — the exact folds are byte-equal under either source, so
        # only the recorded regime distinguishes them (issue #376). Entries
        # predating #376 carry no key and read as the leaves fold they were.
        and existing_entry.get("fold_source", "leaves") == fold["fold_source"]
        # The regime is the pair: which LEVEL a cascade folded from is
        # spec-normative too (§4.3's fold_from_order), and dropping an
        # intermediate order changes it without moving n_leaves or the hash.
        # Both sides are None on the leaves path, so it is a no-op there.
        and existing_entry.get("fold_from_order") == fold.get("fold_from_order")
        and _overview_committed(store_root, node, existing_entry.get("object"), store_kwargs)
    ):
        counts["current"] += 1
        return existing_entry
    try:
        basename = _write_overview(
            store_root, node, k, key, fold, fields, cell_order, shard_order, windowed, store_kwargs
        )
    except Exception as e:
        logger.warning(f"sweep[overview]: write failed at node {node} window {key!r}; ({e})")
        counts["failed"] += 1
        return None
    counts["written"] += 1
    entry = {
        "object": basename,
        "generation": fold["generation"],
        "content_hash": fold["content_hash"],
        "fold_source": fold["fold_source"],
    }
    if fold.get("fold_from_order") is not None:
        entry["fold_from_order"] = int(fold["fold_from_order"])
    return entry


def _overview_committed(store_root, node, basename, store_kwargs) -> bool:
    """Whether the entry's overview zarr is really present AND stamped (D9).

    Unlike the JSON families — whose bookkeeping IS the artifact, so they
    self-heal for free — the overview's envelope entry and its zarr are two
    objects: a generation+hash match proves nothing about the zarr surviving.
    One commit-stamp GET makes skip-if-current self-healing (a deleted
    overview regenerates) and doubles as proof the D4 stamp landed, so a torn
    prior write no longer reads as ``current`` (review finding, issue #201).
    """
    from zagg.hive import read_commit
    from zagg.store import open_store

    if not basename:
        return False
    try:
        leaf = open_store(f"{store_root}/{_node_rel(node)}/{basename}", **store_kwargs)
        return read_commit(leaf) is not None
    except Exception as e:  # an unreadable object is not a current one
        logger.debug(f"sweep[overview]: cannot confirm {node}/{basename} ({e})")
        return False


def _fold_node(
    store_root,
    node,
    k,
    fold_windows,
    node_shards,
    fields,
    cell_order,
    shard_order,
    counts,
    store_kwargs,
):
    """Fold the node's committed descendant leaves into per-field slabs.

    The **exact** fold: single-quantization from the raw leaves, byte-equal to
    a direct aggregation at the coarser order for the exact class. It is the
    finest level's fold always, and every level's fold under the deprecated
    ``fold_source: "leaves"`` — deprecated because it re-reads the whole
    subtree per level AND accumulates that subtree's centroid lists per
    output cell before merging, so per-node memory grows with the subtree
    (issue #376; :func:`_cascade_node` is the bounded default).

    Reads leaf DATA by declared array name only — never a member enumeration
    — so orphan array prefixes from schema evolution (issue #341 Bug A) and
    foreign objects (status prefixes, #327) cannot crash the fold. A leaf
    missing one declared field contributes fill for it (schema evolution:
    the field postdates the leaf); a leaf at an unexpected cell order or with
    an unreadable group is skipped loudly (``failed``), never fatal.
    """
    import zarr

    from zagg.grids.morton import morton_word
    from zagg.hive import read_commit, shard_leaf_path
    from zagg.stats.composition import merge_composition_kway
    from zagg.store import open_store
    from zagg.windows import union_time_range

    target_order = cell_order - (shard_order - k)
    n_cells = 4 ** (target_order - k)
    leaf_cells = 4 ** (cell_order - shard_order)
    slabs: dict = {}
    digests: dict = {}
    # Per packed field, per output cell, the accumulated ``(word, n)`` parts —
    # merged ONCE at the end (single quantization, the k-way law of spec §3.4).
    packed: dict = {}
    # Per packed field, the output cells a HALF-PAIRED leaf covers. The fold's
    # divisor is a different declared field, folded by the digest arm below,
    # which knows nothing about this one: a leaf carrying the word without its
    # ``of`` digest (or the reverse) would leave the level's ``N_signal``
    # counting rows the word excludes, so a reader's §3.3 recovery divides by a
    # denominator the word never covered. Absence over wrongness — those cells
    # keep the fill word ``0``, which makes no §3.2 presence/fraction claim,
    # while the digest stays correct on its own. A leaf carrying NEITHER half
    # is the ordinary schema-evolution case (nothing folds from it either way)
    # and poisons nothing.
    packed_poison: dict = {}
    # Per field, per declared companion channel, the accumulated word vectors —
    # index-aligned with ``digests[name]`` cell by cell so the fold merges the
    # payload and every channel in ONE call (issue #410, spec §9.1/§8.3).
    # ``{field: {kernel kwarg: [per-cell list of vectors]}}``.
    channels: dict = {}
    for name, meta in fields.items():
        if meta["class"] == "approximate":
            digests[name] = [[] for _ in range(n_cells)]
            declared = field_companions(name, meta)
            if declared:
                channels[name] = {kw: [[] for _ in range(n_cells)] for kw, _ in declared}
        else:
            slabs[name] = _empty_slab(meta, n_cells)
            if meta["class"] == "packed":
                packed[name] = {}
                packed_poison[name] = set()
    if target_order >= shard_order:
        span = 4 ** (target_order - shard_order)
        fold_factor = 4 ** (shard_order - k)
    else:
        span = 1
        fold_factor = leaf_cells
    n_leaves, timestamps, granules, ranges = 0, [], 0, []
    for dec in node_shards:
        if target_order >= shard_order:
            start = _rel_rank(dec, node) * span
        else:
            start = _rel_rank(_node_at(dec, target_order), node)
        for window in fold_windows:
            leaf = shard_leaf_path(store_root, morton_word(dec), window=window)
            leaf_store = open_store(leaf, **store_kwargs)
            stamp = read_commit(leaf_store)
            if stamp is None:
                continue  # absent leaf or unstamped debris (D4)
            # Fold the whole leaf's contribution BEFORE touching the slabs, so
            # a corrupt leaf skips cleanly instead of half-applying.
            try:
                group = zarr.open_group(leaf_store, path=str(cell_order), mode="r", zarr_format=3)
                morton = group["morton"]
                if morton.shape != (leaf_cells,):
                    raise ValueError(
                        f"morton shape {morton.shape} is not the manifest cell_order "
                        f"{cell_order} subtree ({leaf_cells} cells); mixed-order source "
                        f"leaves are unsupported this round (issue #347: sweep fold "
                        f"semantics + the writer-side append guard)"
                    )
                partials: dict = {}
                cell_digests: dict = {}
                leaf_packed: dict = {}
                leaf_poison: set = set()
                for name, meta in fields.items():
                    try:
                        arr = group[name]
                    except KeyError:
                        # Schema evolution: the field postdates this leaf — it
                        # contributes fill, exactly what re-running would write.
                        logger.debug(f"sweep[overview]: leaf {leaf} lacks field {name!r}")
                        # Unless it is HALF a packed pair in the ONE skew
                        # direction — word absent, divisor PRESENT: the digest
                        # arm will fold that divisor, so the word must not be
                        # reconstructed over a denominator this leaf's rows are
                        # in but its word is not. (Divisor absent is the packed
                        # arm below: harmless, and never poisoned.)
                        if meta["class"] == "packed" and (meta.get("of") or "") in group:
                            # WARNING, not debug: the poisoned cells are
                            # byte-indistinguishable from genuine emptiness
                            # (the fill word 0 makes no §3.2 claim either way),
                            # and ``_fold_node`` returns no ``source_children``
                            # analog to record it — so this line is the only
                            # trace the under-coverage leaves. Naming the node,
                            # field, leaf and span makes it reconstructable
                            # (review finding).
                            logger.warning(
                                f"sweep[overview]: node {node} field {name!r}: leaf {leaf} "
                                f"carries {meta.get('of')!r} but not the word; output cells "
                                f"[{start}, {start + span}) keep the fill word (spec §3.3)"
                            )
                            leaf_poison.add(name)
                        continue
                    if meta["class"] == "exact":
                        partials[name] = fold_dense(
                            arr[:], fold_factor, meta.get("method"), meta.get("fill_value", "NaN")
                        )
                    elif meta["class"] == "packed":
                        # The fold's ``n`` inputs are the ``of`` digest's
                        # per-cell weights (spec §3.3/§3.4). A leaf carrying
                        # the word without its divisor digest contributes
                        # nothing — never a part with a guessed ``n``; that is
                        # the schema-evolution under-coverage posture above.
                        #
                        # Skipped WITHOUT poisoning, unlike the reverse
                        # direction above: the ``of`` field is itself a declared
                        # entry, and the digest arm reading it hits the same
                        # absence in the same leaf, so BOTH halves drop together
                        # and the level's ``N_signal`` already excludes this
                        # leaf's rows. Nothing to skew — and poisoning here
                        # would delete SIBLING leaves' correct words from the
                        # shared output cells (review finding).
                        of_name = meta.get("of")
                        try:
                            of_values = group[of_name][:]
                        except (KeyError, TypeError):
                            logger.debug(
                                f"sweep[overview]: leaf {leaf} lacks {of_name!r} for "
                                f"packed field {name!r}"
                            )
                            continue
                        of_dtype = (fields.get(of_name) or {}).get("dtype") or "float32"
                        words_arr = arr[:]
                        leaf_packed[name] = [
                            (i, int(words_arr[i]), n)
                            for i in range(leaf_cells)
                            if (n := payload_weight(of_values[i], of_dtype)) > 0
                        ]
                    else:
                        # Mismatched §2.0 weights declarations refuse to merge
                        # (issue #424); the enclosing guard skips the leaf loudly.
                        check_weights_match(dict(arr.attrs), meta, name)
                        values = arr[:]
                        dtype = meta.get("dtype") or "float32"
                        inner = tuple(meta.get("inner_shape") or (2,))
                        # Every declared companion is read in the SAME guarded
                        # block as its payload, so a leaf missing or failing on
                        # one contributes NEITHER — never a digest folded with a
                        # channel silently dropped (the words are keyed on the
                        # partition the payload describes, spec §9.1/§8.3). A
                        # declaration this fold cannot join refuses here, exactly
                        # as a mismatched §2.0 one does above.
                        raw: dict = {}
                        for kwarg, sibling_name in field_companions(name, meta):
                            sib = group[sibling_name]
                            check_companion_match(dict(sib.attrs), name, kwarg)
                            raw[kwarg] = sib[:]
                        cell_digests[name] = [
                            (
                                start + i // fold_factor,
                                decode_digest(payload, dtype, inner),
                                {
                                    kwarg: decode_digest(col[i], "uint64", ())
                                    for kwarg, col in raw.items()
                                },
                            )
                            for i, payload in enumerate(values)
                            if payload is not None and len(payload)
                        ]
            except Exception as e:
                logger.warning(f"sweep[overview]: skipping unreadable leaf {leaf} ({e})")
                counts["failed"] += 1
                continue
            seg = slice(start, start + span)
            for name, partial in partials.items():
                meta = fields[name]
                slabs[name][seg] = combine_dense(
                    slabs[name][seg], partial, meta.get("method"), meta.get("fill_value", "NaN")
                )
            for name, decoded in cell_digests.items():
                for j, digest, words in decoded:
                    digests[name][j].append(digest)
                    for kwarg, vector in words.items():
                        channels[name][kwarg][j].append(vector)
            for name, contributions in leaf_packed.items():
                for i, word, n in contributions:
                    packed[name].setdefault(start + i // fold_factor, []).append((word, n))
            # The leaf's whole span, which is exactly the output cells its
            # rows reach (``leaf_cells // fold_factor == span`` in both fold
            # geometries). Applied only once the leaf has read cleanly: an
            # UNREADABLE leaf is skipped whole, so the digest excludes it too
            # and the pair stays consistent without poisoning anything.
            for name in leaf_poison:
                packed_poison[name].update(range(start, start + span))
            n_leaves += 1
            timestamps.append(stamp.get("written_at"))
            granules += int(stamp.get("granule_count") or 0)
            if stamp.get("time_range") is not None:
                ranges.append(stamp["time_range"])
    if n_leaves == 0:
        return None
    for name, parts_by_cell in packed.items():
        # Poisoned cells drop out HERE, after accumulation: every surviving
        # cell still folds its parts in one k-way call (single quantization).
        for j, parts in parts_by_cell.items():
            if j not in packed_poison[name]:
                slabs[name][j] = merge_composition_kway(parts)
    for name, acc in digests.items():
        meta = fields[name]
        dtype = meta.get("dtype") or "float32"
        delta = overview_fold_delta(meta)
        slab = np.full(n_cells, b"", dtype=object)
        declared = field_companions(name, meta)
        acc_channels = channels.get(name)
        sibling_slabs = {kwarg: np.full(n_cells, b"", dtype=object) for kwarg, _ in declared}
        for j, cell in enumerate(acc):
            if not cell:
                continue
            if not declared:
                slab[j] = fold_digests(cell, delta=delta, dtype=dtype)
                continue
            payload, *words = fold_digests(
                cell,
                delta=delta,
                dtype=dtype,
                channels={kw: acc_channels[kw][j] for kw, _ in declared},
            )
            slab[j] = payload
            for (kwarg, _), encoded in zip(declared, words, strict=True):
                sibling_slabs[kwarg][j] = encoded
        slabs[name] = slab
        for kwarg, sibling_name in declared:
            slabs[sibling_name] = sibling_slabs[kwarg]
    stamps = [t for t in timestamps if t is not None]
    return {
        "slabs": slabs,
        "generation": {
            "n_leaves": int(n_leaves),
            "max_leaf_timestamp": max(stamps) if stamps else None,
        },
        "content_hash": _content_hash(node, k, target_order, fields, slabs),
        "granule_count": granules,
        "time_range": union_time_range(*ranges) if ranges else None,
        "fold_source": "leaves",
    }


def _empty_slab(meta: dict, n_cells: int) -> np.ndarray:
    """One field's empty output slab: ragged empty payloads for the digest
    class, the dense fill for the scalar classes (``exact`` and ``packed`` —
    a packed field's fill is the ``0`` word, spec §3)."""
    if meta["class"] == "approximate":
        return np.full(n_cells, b"", dtype=object)
    dtype = np.dtype(meta.get("dtype") or "float32")
    return np.full(n_cells, _fill_scalar(meta.get("fill_value", "NaN"), dtype), dtype)


def _cascade_node(
    store_root,
    node,
    k,
    source_order,
    key,
    node_shards,
    fields,
    cell_order,
    shard_order,
    counts,
    store_kwargs,
):
    """Fold one node's overview from the level below it — fold-of-folds (#376).

    The cascade path, and the ratified default: the input is the node's
    ``4^gap`` child overviews at ``source_order`` (``gap = source_order - k``),
    not its subtree's leaves. Every overview slab in the tree holds the same
    ``4^(cell_order - shard_order)`` cells (constant tree depth, §4.4), so a
    child's slab folds ``4^gap``-to-one into the ``4^(cell_order-shard_order)
    / 4^gap`` output cells the child owns — a **disjoint** span per child
    (assignment, never accumulation). Two consequences, both the point of the
    issue:

    * per-node resident memory is the output slab plus ONE child slab, whose
      cells each hold at most delta centroids — constant in the subtree size,
      where :func:`_fold_node` grows with it (it accumulates the whole
      subtree's centroid lists per output cell before merging);
    * each leaf is read once for the WHOLE pyramid, by the finest level only.

    The cost is accuracy: a cascaded digest is a merge of merges, so it
    inherits the documented order-dependence of the t-digest merge and drifts
    from the exact-from-leaves fold. That is in contract — overviews are
    display artifacts with no precision guarantee past the exact levels
    (espg, issue #376) — and it is why every level records which regime made
    it (§4.3/§4.5).

    Children are read exactly like leaves are: by declared array name only
    (never a member enumeration), stamp-gated (an unstamped child is debris,
    D4), and skipped loudly — never fatally — when unreadable. A child whose
    ``role``/``zagg_overview`` attrs do not classify it as an overview at
    ``source_order`` is skipped rather than folded blind: write order pins
    those attrs BEFORE the commit stamp, so a stamped overview always carries
    them, and anything else at that path is not this fold's input.
    """
    import zarr

    from zagg.hive import read_commit
    from zagg.store import open_store
    from zagg.windows import union_time_range

    target_order = cell_order - (shard_order - k)
    n_cells = 4 ** (target_order - k)
    factor = 4 ** (source_order - k)
    span = n_cells // factor
    source_cell_order = target_order + (source_order - k)
    slabs = {name: _empty_slab(meta, n_cells) for name, meta in fields.items()}
    # A located field's sibling gets its own output slab (ruling 4 on issue
    # #410): ``_fold_child`` returns it beside the payload, and children own
    # disjoint spans, so it assigns exactly as every other slab does.
    for name, meta in fields.items():
        if meta["class"] != "approximate":
            continue
        for _kwarg, sibling_name in field_companions(name, meta):
            slabs[sibling_name] = np.full(n_cells, b"", dtype=object)
    basename = _overview_basename(key)
    n_sources, n_leaves, timestamps, granules, ranges = 0, 0, [], 0, []
    missing, unreadable = 0, 0
    children = sorted({_node_at(d, source_order) for d in node_shards})
    for child in children:
        path = f"{store_root}/{_node_rel(child)}/{basename}"
        try:
            child_store = open_store(path, read_only=True, **store_kwargs)
            stamp = read_commit(child_store)
        except Exception as e:
            logger.warning(f"sweep[overview]: skipping unreadable overview {path} ({e})")
            counts["failed"] += 1
            unreadable += 1
            continue
        if stamp is None:
            missing += 1  # never generated, or unstamped debris (D4)
            continue
        # Fold the whole child BEFORE touching the slabs, so a corrupt child
        # skips cleanly instead of half-applying (the leaf path's discipline).
        try:
            root = zarr.open_group(child_store, path="", mode="r", zarr_format=3)
            provenance = root.attrs.get(OVERVIEW_ATTR)
            provenance = dict(provenance) if isinstance(provenance, dict) else {}
            if root.attrs.get(ROLE_ATTR) != "overview" or provenance.get("order") != source_order:
                raise ValueError(
                    f"role {root.attrs.get(ROLE_ATTR)!r} / declared order "
                    f"{provenance.get('order')!r} is not an overview at order {source_order}"
                )
            group = zarr.open_group(
                child_store, path=str(source_cell_order), mode="r", zarr_format=3
            )
            if group["morton"].shape != (n_cells,):
                raise ValueError(
                    f"morton shape {group['morton'].shape} is not the {n_cells}-cell "
                    f"overview slab of order {source_order}"
                )
            partials = _fold_child(group, fields, factor, span, path)
        except Exception as e:
            logger.warning(f"sweep[overview]: skipping unreadable overview {path} ({e})")
            counts["failed"] += 1
            unreadable += 1
            continue
        start = _rel_rank(child, node) * span
        for name, partial in partials.items():
            # Children own disjoint spans of the parent slab, so this is an
            # assignment — the accumulate-then-merge the leaf fold needs (and
            # pays for in memory) has no counterpart here.
            slabs[name][start : start + span] = partial
        n_sources += 1
        n_leaves += int((provenance.get("generation") or {}).get("n_leaves") or 0)
        timestamps.append((provenance.get("generation") or {}).get("max_leaf_timestamp"))
        granules += int(stamp.get("granule_count") or 0)
        if stamp.get("time_range") is not None:
            ranges.append(stamp["time_range"])
    if missing or unreadable:
        # The cascade folds what is ON DISK, where the leaf fold folds every
        # leaf the MOC knows about: a child the fold could not use leaves its
        # span at fill until a sweep covers it. Logged rather than warned — a
        # candidate child with no leaf in THIS window has no overview either,
        # which is ordinary on a windowed store — but ALSO recorded in the
        # overview's own attrs below, since a log line in an exited process
        # cannot tell a reader that a fill cell is under-coverage rather than
        # emptiness (§4.3 ``source_children``).
        logger.info(
            f"sweep[overview]: node {node} order {k} window {key!r}: {missing + unreadable} of "
            f"{len(children)} candidate child overviews at order {source_order} are not usable "
            f"({missing} not materialized, {unreadable} unreadable) — their cells stay fill "
            f"until a sweep regenerates them"
        )
    if n_sources == 0:
        return None
    stamps = [t for t in timestamps if t is not None]
    return {
        "slabs": slabs,
        "generation": {
            "n_leaves": int(n_leaves),
            "max_leaf_timestamp": max(stamps) if stamps else None,
        },
        "content_hash": _content_hash(node, k, target_order, fields, slabs),
        "granule_count": granules,
        "time_range": union_time_range(*ranges) if ranges else None,
        "fold_source": "cascade",
        "fold_from_order": int(source_order),
        "source_children": {
            "folded": int(n_sources),
            "missing": int(missing),
            "unreadable": int(unreadable),
        },
    }


def _fold_child(group, fields, factor, span, path) -> dict:
    """One child overview's slabs, folded ``factor``-to-one into ``span`` cells.

    Digest cells are merged group by group, so at most ``factor`` decoded
    digests are ever resident — the bound that makes the cascade fold's
    per-node memory independent of the subtree (issue #376). A field absent
    from the child contributes nothing (schema evolution, as at the leaves).

    A located field's ``{field}_locations`` sibling folds in the SAME group as
    its payload (ruling 4 on issue #410): the merged words are keyed on the
    centroid partition that merge produced, so the pair cannot be folded in two
    passes. The cascade therefore reads and writes both arrays here, and its
    words sit at heterogeneous orders exactly as the leaf fold's do (spec §9.1).
    """
    partials: dict = {}
    for name, meta in fields.items():
        try:
            arr = group[name]
        except KeyError:
            logger.debug(f"sweep[overview]: overview {path} lacks field {name!r}")
            continue
        if meta["class"] == "exact":
            partials[name] = fold_dense(
                arr[:], factor, meta.get("method"), meta.get("fill_value", "NaN")
            )
            continue
        if meta["class"] == "packed":
            # The cascade's ``n`` inputs are the child's ``of`` digest weights
            # at the same rows (spec §3.3/§3.4); a child carrying the word
            # without its divisor digest contributes nothing for the field —
            # the schema-evolution posture above, never a guessed ``n``. That
            # skip needs no poisoning counterpart (unlike ``_fold_node``'s and
            # ``_merge_slabs``'): children own DISJOINT spans of the parent
            # slab and ``_cascade_node`` ASSIGNS them, so a skipped child
            # leaves its whole span at the fill word — no output cell can mix
            # this child's absence with a sibling's contribution. Each
            # level re-quantizes once (the k-way law), which is in the class's
            # documented contract: presence exact, counts within one lane
            # quantization per fold.
            from zagg.stats.composition import merge_composition_kway

            of_name = meta.get("of")
            try:
                of_values = group[of_name][:]
            except (KeyError, TypeError):
                logger.debug(f"sweep[overview]: overview {path} lacks {of_name!r} for {name!r}")
                continue
            of_dtype = (fields.get(of_name) or {}).get("dtype") or "float32"
            words_arr = arr[:]
            folded = _empty_slab(meta, span)
            for j in range(span):
                parts = [
                    (int(words_arr[i]), n)
                    for i in range(j * factor, min((j + 1) * factor, len(words_arr)))
                    if (n := payload_weight(of_values[i], of_dtype)) > 0
                ]
                if parts:
                    folded[j] = merge_composition_kway(parts)
            partials[name] = folded
            continue
        # Mismatched §2.0 weights declarations refuse to merge (issue #424);
        # the enclosing per-child guard skips the child loudly.
        check_weights_match(dict(arr.attrs), meta, name)
        values = arr[:]
        dtype = meta.get("dtype") or "float32"
        inner = tuple(meta.get("inner_shape") or (2,))
        delta = overview_fold_delta(meta)
        declared = field_companions(name, meta)
        # A companion declaration this fold cannot join refuses beside the §2.0
        # one — read in the same guarded block, so a child failing on one
        # contributes neither.
        raw: dict = {}
        for kwarg, sibling_name in declared:
            sib = group[sibling_name]
            check_companion_match(dict(sib.attrs), name, kwarg)
            raw[kwarg] = sib[:]
        folded = np.full(span, b"", dtype=object)
        sibling_slabs = {kwarg: np.full(span, b"", dtype=object) for kwarg, _ in declared}
        for j in range(span):
            rows = [
                i
                for i in range(j * factor, min((j + 1) * factor, len(values)))
                if values[i] is not None and len(values[i])
            ]
            if not rows:
                continue
            cell = [decode_digest(values[i], dtype, inner) for i in rows]
            if not declared:
                folded[j] = fold_digests(cell, delta=delta, dtype=dtype)
                continue
            payload, *words = fold_digests(
                cell,
                delta=delta,
                dtype=dtype,
                channels={
                    kwarg: [decode_digest(raw[kwarg][i], "uint64", ()) for i in rows]
                    for kwarg, _ in declared
                },
            )
            folded[j] = payload
            for (kwarg, _), encoded in zip(declared, words, strict=True):
                sibling_slabs[kwarg][j] = encoded
        partials[name] = folded
        for kwarg, sibling_name in declared:
            partials[sibling_name] = sibling_slabs[kwarg]
    return partials


def _fold_sources(decl, orders, cell_order, shard_order) -> dict:
    """Per-order ``(fold_source, source_order)`` for one sweep (issue #376).

    ``orders`` is descending (finest first) and the sweep walks it in that
    order, so ``orders[i - 1]`` is the level a cascading level folds from —
    the one this same pass has just materialized. The finest
    ``exact_levels`` levels fold from the leaves: the first has no finer
    overview to cascade from at all, and the knob is what carries the open
    "first, or possibly second" sub-decision. A gap wider than the node slab
    can address (``4^gap`` children but only ``4^(cell_order - shard_order)``
    output cells) falls back to the leaves loudly rather than folding a slab
    it cannot divide.
    """
    declared = decl.get("fold_source") or DEFAULT_FOLD_SOURCE
    if declared not in FOLD_SOURCES:
        logger.warning(
            f"sweep[overview]: declared fold_source {declared!r} is unknown "
            f"(known: {list(FOLD_SOURCES)}); folding as {DEFAULT_FOLD_SOURCE!r}"
        )
        declared = DEFAULT_FOLD_SOURCE
    try:
        exact_levels = max(1, int(decl.get("exact_levels") or DEFAULT_EXACT_LEVELS))
    except (TypeError, ValueError):
        logger.warning(
            f"sweep[overview]: declared exact_levels {decl.get('exact_levels')!r} is unusable; "
            f"folding {DEFAULT_EXACT_LEVELS} level(s) from the leaves"
        )
        exact_levels = DEFAULT_EXACT_LEVELS
    if declared == "cascade" and orders and exact_levels >= len(orders):
        # Mirrored from _fold_plan: a hand-edited manifest reaches the sweep
        # without either declaration path, and this is the same deprecated
        # regime under a 'cascade' declaration (issue #376).
        logger.warning(
            f"sweep[overview]: declared exact_levels {exact_levels} covers all "
            f"{len(orders)} declared levels {orders}, so EVERY level folds from the raw "
            f"leaves — the DEPRECATED 'leaves' regime under a 'cascade' declaration"
        )
    depth = cell_order - shard_order
    plan: dict = {}
    for i, k in enumerate(orders):
        if declared == "leaves" or i < exact_levels:
            plan[k] = ("leaves", None)
            continue
        gap = orders[i - 1] - k
        if gap > depth:
            logger.warning(
                f"sweep[overview]: order {k} sits {gap} orders below {orders[i - 1]}, wider "
                f"than the {depth}-order node slab — a cascade would have 4^{gap} children "
                f"for {4**depth} output cells; folding it from the leaves instead"
            )
            plan[k] = ("leaves", None)
        else:
            plan[k] = ("cascade", orders[i - 1])
    return plan


def _content_hash(node, k, target_order, fields, slabs) -> str:
    """sha256 over the folded per-field values (decoded bytes, O11-style).

    The skip-if-current backstop: leaf stamps resolve to whole seconds, so a
    same-second leaf re-run carries an unchanged generation stamp; the hash
    catches the content change without re-reading the written overview.
    """
    h = hashlib.sha256(f"{OVERVIEW_SPEC}:{node}:{k}:{target_order}".encode())
    for name in sorted(slabs):
        h.update(name.encode())
        slab = slabs[name]
        if slab.dtype == object:
            for payload in slab:
                h.update(len(payload).to_bytes(4, "little"))
                h.update(payload)
        else:
            h.update(np.ascontiguousarray(slab).tobytes())
    return h.hexdigest()


def _overview_config(fields):
    """A minimal PipelineConfig whose leaf template matches the overview.

    The overview zarr reuses the leaf template machinery
    (``HealpixGrid.emit_shard_template``) so structure — dtypes, fills, the
    D18 ragged attrs, the D16 dggs attrs — cannot drift from source leaves.

    The §2.0 ``weights`` declaration (and the ``gain`` provenance §2.0 makes
    REQUIRED beside it) is carried through from the manifest field entry
    (:func:`zagg.pyramid.declared_fields`), because the overview's payload IS
    the fold of its sources' weights: a bare template would declare the fold
    of a flux store as counts, and :func:`check_weights_match` would then
    refuse every child of the cascade (review finding, issue #424). Both keys
    are absent on a counts field, so a counts store's template bytes are
    unchanged.
    """
    from zagg.config import PipelineConfig

    variables = {}
    for name, meta in fields.items():
        if meta["class"] == "exact":
            variables[name] = {
                "function": meta.get("method"),
                "dtype": meta.get("dtype", "float32"),
                "fill_value": meta.get("fill_value", "NaN"),
            }
        elif meta["class"] == "packed":
            # §3.3: an overview's composition array carries the same attrs
            # block a leaf does — ``spec``/``lanes`` are stamped from the
            # writer's constants (``grids.base.apply_field_attrs``), while the
            # per-product ``of``/``threshold`` halves ride the manifest entry,
            # which is the only description the overview writer has.
            block = {"of": meta.get("of")}
            if meta.get("threshold") is not None:
                block["threshold"] = meta["threshold"]
            variables[name] = {
                "function": "zagg.stats.composition.pack_composition",
                "dtype": meta.get("dtype", "uint64"),
                "fill_value": meta.get("fill_value", 0),
                "attrs": {"composition": block},
            }
        else:
            variables[name] = {
                "kind": "ragged",
                "function": "zagg.stats.tdigest.build_tdigest",
                "inner_shape": list(meta.get("inner_shape") or [2]),
                "dtype": meta.get("dtype", "float32"),
                "fill_value": 0,
            }
            # A located field folds through the pyramid (ruling 4, issue #410),
            # so its overview carries the same ``{field}_locations`` sibling its
            # source leaves do — declared here because the manifest entry is the
            # only description the overview writer has. The value is the source
            # column's NAME, which the overview never reads (it folds stored
            # words, not observations); what it buys is the sibling array and its
            # §9 declaration, so an overview is self-describing exactly as a leaf
            # is. Spec §9.1 makes the heterogeneous orders that fold produces
            # normative.
            if meta.get("location") is not None:
                variables[name]["location"] = str(meta["location"])
            # The §8.3 temporal companion rides the same way (espg-ruled
            # 2026-08-17): per-centroid at every level, so the overview template
            # emits the ``{field}_times`` sibling and its declaration exactly as
            # a leaf does. The SHAPE is what the template needs; the leaf's
            # ingest clock is never re-read by a fold.
            if meta.get("temporal") is not None:
                variables[name]["temporal"] = str(meta["temporal"])
            if meta.get("weights") not in (None, "counts"):
                variables[name]["weights"] = meta["weights"]
                if meta.get("gain") is not None:
                    variables[name]["attrs"] = {"gain": meta["gain"]}
    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": variables,
        }
    )


def _write_overview(
    store_root, node, k, key, fold, fields, cell_order, shard_order, windowed, store_kwargs
) -> str:
    """Write one overview zarr at its ancestor node; returns the basename.

    D23 naming: overviews inherit window naming — ``{window}.zarr``, with the
    reserved ``all`` token for the unwindowed / all-time fold. Write order is
    pinned like a leaf's: template (wholesale overwrite — a prior overview or
    torn debris is replaced, D4 retry semantics) -> arrays -> role/provenance
    attrs -> commit stamp LAST, so an interrupted writer leaves ignorable
    debris and presence certifies the ``role`` attr landed (D11). The D20
    stats sidecar (§5 ``content_hashes``, issue #342) is a SIBLING object PUT
    after the stamp — outside the leaf, so the stamp stays the leaf's own
    final write, exactly as source-leaf sidecars land post-stamp.
    """
    import zarr
    from mortie import generate_morton_children
    from zarr import open_array

    from zagg.grids.healpix import HealpixGrid
    from zagg.grids.morton import morton_word
    from zagg.hive import _utcnow, stamp_commit
    from zagg.store import open_store

    target_order = cell_order - (shard_order - k)
    basename = _overview_basename(key)
    path = f"{store_root}/{_node_rel(node)}/{basename}"
    grid = HealpixGrid(k, target_order, config=_overview_config(fields), sharded=True)
    store = open_store(path, **store_kwargs)
    grid.emit_shard_template(store, overwrite=True)
    words = np.asarray(generate_morton_children(morton_word(node), target_order), dtype=np.uint64)
    arr = open_array(store, path=f"{target_order}/morton", zarr_format=3, consolidated=False)
    arr[:] = words
    for name, slab in fold["slabs"].items():
        arr = open_array(store, path=f"{target_order}/{name}", zarr_format=3, consolidated=False)
        arr[:] = slab
    populated = _populated_mask(fold["slabs"], fields)
    root = zarr.open_group(store, path="", mode="r+", zarr_format=3)
    root.attrs.update(
        {
            ROLE_ATTR: "overview",
            OVERVIEW_ATTR: {
                "spec": OVERVIEW_SPEC,
                "node": node,
                "order": int(k),
                "cell_order": int(target_order),
                "source_shard_order": int(shard_order),
                "source_cell_order": int(cell_order),
                "window": key,
                "fields": {n: _field_provenance(m) for n, m in fields.items()},
                **_fold_provenance(fold),
                "generation": fold["generation"],
                "content_hash": fold["content_hash"],
                "generated_at": _utcnow(),
            },
        }
    )
    stamp_window = key if windowed else None
    stamp_commit(
        store,
        cells_with_data=int(populated.sum()),
        granule_count=int(fold["granule_count"]),
        window=stamp_window,
        time_range=fold["time_range"] if stamp_window is not None else None,
    )
    # O11 content hashes (issue #342 phase 4): an overview leaf gets the same
    # §5 D20 sidecar record as a source leaf, computed from the folded arrays
    # already in memory (the ratified overview-scope decision (1)); the
    # envelope's sweep-internal skip digest (``_content_hash`` above) is a
    # DIFFERENT recipe with a different job and stays untouched (decision
    # (2)). Sidecar naming follows the leaf basename's D23 window-only
    # grammar (``{stem}.stats.json``) regardless of the store's manifest
    # spec: overview basenames are v3-named unconditionally, and the legacy
    # grammar would key every window's sidecar to one ``stats.json`` at the
    # node. Fail-open (D9 telemetry posture; §5.3 reads absence as
    # unverifiable, never tampered).
    try:
        from zagg.content_hash import content_hashes_record, hash_arrays
        from zagg.telemetry import SPEC_V3, build_record, write_sidecar

        staged = {f"{target_order}/morton": words}
        staged.update({f"{target_order}/{name}": slab for name, slab in fold["slabs"].items()})
        group = zarr.open_group(store, path="", mode="r", zarr_format=3)
        record = build_record(
            shard_key=morton_word(node),
            metadata={
                "cells_with_data": int(populated.sum()),
                "granule_count": int(fold["granule_count"]),
                "content_hashes": content_hashes_record(hash_arrays(group, staged=staged)),
            },
            window=stamp_window,
        )
        write_sidecar(path, record, spec=SPEC_V3, **store_kwargs)
    except Exception as e:
        logger.warning(f"sweep[overview]: O11 sidecar failed at {node}/{basename} ({e})")
    return basename


def _fold_provenance(fold: dict) -> dict:
    """The overview's own fold provenance: which regime produced this level.

    ``fold_source`` is ``"leaves"`` (single-quantization from the raw leaves)
    or ``"cascade"``, and a cascaded overview also names the order it folded
    from (``fold_from_order``). A reader needs this because the two regimes
    are not interchangeable for the approximate class: a cascaded digest is a
    merge of merges, so it inherits the documented merge order-dependence and
    carries no precision guarantee, while an exact-from-leaves one is single
    quantization (issue #376, spec §4.3).

    A cascade also records ``source_children`` — how many candidate child
    overviews it folded, and how many it could not use. The cascade folds
    what is on disk, so a nonzero ``missing``/``unreadable`` means the level
    UNDER-COVERS its subtree and a fill cell there is not evidence of
    emptiness: the distinction lives in the artifact rather than in a sweep
    log, which is the only place a later reader can find it.
    """
    entry = {"fold_source": fold.get("fold_source", "leaves")}
    if fold.get("fold_from_order") is not None:
        entry["fold_from_order"] = int(fold["fold_from_order"])
    if fold.get("source_children") is not None:
        entry["source_children"] = dict(fold["source_children"])
    return entry


def _field_provenance(meta: dict) -> dict:
    """One field's per-field entry in the overview attrs: class + fold law.

    Exact fields also carry :data:`EXACT_NAN_POLICY`, so a reader of the
    overview knows the reduction it actually got — nan-skipping, never
    NaN-propagating (review finding, issue #201).

    A manifest entry with no ``method`` defaults by CLASS, not to the digest
    law: :func:`zagg.pyramid.declared_fields` always writes one, but this
    consumes a manifest, and manifests outlive their writer and may come from
    an external one (spec §4.5). Stamping ``tdigest_kway`` on a ``packed``
    entry would have the overview's own provenance claim a t-digest law over
    a dense composition word (review finding, issue #515).
    """
    default = COMPOSITION_LAW if meta["class"] == "packed" else TDIGEST_LAW
    entry = {"class": meta["class"], "method": meta.get("method", default)}
    if meta["class"] == "exact":
        entry["nan_policy"] = EXACT_NAN_POLICY
    return entry


def _populated_mask(slabs: dict, fields: dict) -> np.ndarray:
    """Cells carrying any non-fill value in any included field."""
    populated = None
    for name, slab in slabs.items():
        if slab.dtype == object:
            mask = np.fromiter((len(p) > 0 for p in slab), dtype=bool, count=len(slab))
        else:
            mask = ~_is_missing(slab, fields[name].get("fill_value", "NaN"))
        populated = mask if populated is None else (populated | mask)
    return populated if populated is not None else np.zeros(0, dtype=bool)
