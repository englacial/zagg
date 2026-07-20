"""Boundary adapter for mortie's ``morton_index`` extension type.

The ``morton`` output coordinate is carried in memory as a mortie
:class:`~mortie.morton_index.MortonIndexArray` (a pandas ExtensionArray over the
packed ``uint64`` Morton words), and crosses the Arrow carrier boundary as
mortie's ``morton_index`` Arrow **extension type** over the PyCapsule C Data
Interface (:func:`morton_to_arrow` / :func:`morton_from_arrow`; mortie >= 0.8.4,
issue #135) — typed on both the pandas and arro3 surfaces, no pyarrow on the
worker path. On disk it is stored as plain ``uint64`` — Zarr stores numpy
dtypes, and the extension metadata lives at the interchange layer only — and
reconstructed as a ``MortonIndexArray`` on read.

This is the contained #71 migration: only the ``morton`` coordinate adopts the
type — and since the D16 flip (issue #304) it is the only stored cell
coordinate by default (the legacy NESTED ``cell_ids`` array survives behind
the ``output.grid.emit_cell_ids: true`` transition hatch; the issue-#135
``cell_ids_encoding`` knob is retired). The internal leaf/cell/shard morton
arithmetic (``cells_of`` / ``shards_of`` / ``children``) stays on plain
``uint64`` ndarrays.

Storing the raw ``uint64`` words (rather than a reinterpreted ``int64``) is what
removes the sign hazard: the packed word's prefix is ``base+1``, so base cells
7–11 set bit 63 and read back negative under an ``int64`` coordinate. ``uint64``
keeps them non-negative and the Z-order intact (espg/zagg#71).
"""

from __future__ import annotations

import numpy as np

# Wire name of mortie's Arrow extension type (``mortie.arrow.EXTENSION_NAME``),
# carried as ``ARROW:extension:name`` field metadata over the PyCapsule C Data
# Interface (issue #135). Mirrored here so the hot-path metadata check needs no
# import; a test pins it against mortie's constant.
MORTON_EXTENSION_NAME = "mortie.morton_index"
_EXTENSION_NAME_KEY = "ARROW:extension:name"

#: Self-declared ``zarr_conventions`` entry for morton-declared stores
#: (issue #305, D16). The UUID is minted once and PERMANENT — readers key on
#: it; never regenerate it. The spec/schema URLs point at the mortie
#: specification page (``docs/specification.md`` — the normative home of the
#: convention; zagg's design doc only cites it). ``zarr_conventions`` is a
#: LIST: a future upstream dggs-registry entry (issue #72 ask 3) coexists
#: alongside this one rather than replacing it.
MORTON_CONVENTION = {
    "schema_url": "https://github.com/espg/mortie/blob/main/docs/specification.md#dggs-attrs",
    "spec_url": "https://github.com/espg/mortie/blob/main/docs/specification.md",
    "uuid": "3e22156d-ea9e-4e01-95fe-e3809a4b41e7",
    "name": "morton-dggs",
    "description": "Packed-u64 morton (HEALPix) DGGS convention",
}

#: O10 resolution discriminator (espg-ratified, issue #305): ``exact`` — ids
#: are true cells at their encoded order; grid-derived cell coordinates are
#: exact BY CONSTRUCTION, so every zagg aggregation output emits it.
RESOLUTION_EXACT = "exact"
#: ``point`` — locations cast to order 29 with no area claim (raw lat/lon
#: conversions: the temporal event path, future HHDC id fields). The mortie
#: spec page's 29->24 clip rule applies to ``point`` ONLY. Emission is per
#: data kind and the writer always knows which; no heuristic fallback.
RESOLUTION_POINT = "point"
#: ``mixed`` (espg-proposed on the mortie PR #118 review, 2026-07-21):
#: order-29 ids are points (unknown resolution), ids at any other order are
#: exact — per-id recovery via the RESERVED order 29. The 29->24 clip rule is
#: INAPPLICABLE to mixed arrays (clipping destroys the in-band signal;
#: Number-safe paths use the other D16 measures), and genuinely-exact
#: order-29 cells are unrepresentable under it (declare ``exact``). Accepted
#: as a declared value; no zagg writer emits it today (aggregation outputs
#: are ``exact`` — the HHDC exact-cells-plus-raw-locations direction is the
#: intended consumer).
RESOLUTION_MIXED = "mixed"


def is_morton_array(values) -> bool:
    """True if ``values`` is a mortie ``MortonIndexArray``."""
    try:
        from mortie import MortonIndexArray
    except ImportError:  # pragma: no cover - mortie is a hard dependency
        return False
    return isinstance(values, MortonIndexArray)


def morton_words(values) -> np.ndarray:
    """Return the packed ``uint64`` Morton words for ``values``.

    Accepts a :class:`~mortie.morton_index.MortonIndexArray` (its ``uint64``
    storage is returned) or any ``uint64``-coercible array-like (returned as a
    ``uint64`` ndarray). This is the on-disk / wire form of the ``morton``
    coordinate.
    """
    if is_morton_array(values):
        return np.asarray(values._data, dtype=np.uint64)
    return np.asarray(values, dtype=np.uint64)


def to_morton_array(words):
    """Reconstruct a ``MortonIndexArray`` from packed ``uint64`` words.

    The inverse of :func:`morton_words` for the storage round-trip: read the
    ``uint64`` coordinate back from Zarr and wrap it as the extension array.
    """
    from mortie import MortonIndexArray

    return MortonIndexArray.from_words(np.asarray(words, dtype=np.uint64))


def morton_decimal(word) -> str:
    """Decimal morton string for one packed shard-key word (issue #199).

    The external/path form of a shard id per the sparse-coverage design record
    (``docs/design/sparse_coverage.md`` D1): the packed ``uint64`` word stays
    the canonical in-memory/wire form, and every externally visible string —
    hive leaf ids, ``.status`` object keys, log lines — renders through
    mortie's decode-through-kernel decimal repr (e.g. ``-31123``). Raises
    ``ValueError`` on an empty, invalid, or negative word (a path component
    must never be silently wrong) — a NEGATIVE int here is usually a *legacy
    signed decimal id* handed in where the packed word belongs; parse it with
    ``morton_word(str(id))`` instead.
    """
    word = int(word)
    if word < 0:
        # np.uint64 coercion would raise an opaque OverflowError; normalize to
        # the documented ValueError with the likely cause spelled out.
        raise ValueError(
            f"packed morton word must be non-negative (got {word}); a signed "
            f"decimal id like '-4211322' is the external form — parse it with "
            f"morton_word(str(id))"
        )
    from mortie import MortonIndexArray

    return MortonIndexArray.from_words(np.asarray([word], dtype=np.uint64)).decimal_repr()[0]


def morton_word(label: str) -> int:
    """Parse a decimal morton string back to its packed word (issue #199).

    The inverse of :func:`morton_decimal` at the zagg boundary — used where an
    external decimal id re-enters (``--morton-cell``, hive leaf ids on the
    read path). Raises ``ValueError`` on a malformed id.

    Implementation note: this rides mortie's private-but-documented
    ``_decimal_to_word`` (the issue-104 parse-back) rather than the public
    ``MortonIndexArray.from_hive_path(label, suffix="")`` because the array
    classes are built lazily and require pandas — the private function is
    numpy-only, keeping the reader path light. The upstream ask (a public
    numpy-only export) stands. Same non-injectivity caveat mortie documents: an
    order-29 *point* id parses back to the *area* word (irrelevant for shard
    keys at order <= 11; noted since this is a general boundary helper).
    """
    from mortie.morton_index import _decimal_to_word

    return _decimal_to_word(str(label))


def morton_box(values) -> np.ndarray:
    """Tier-0 morton box: canonical <= 4-member MOC covering ``values`` (issue #200).

    The fixed-width tier of the coverage envelope (``docs/design/
    sparse_coverage.md`` §4): compact the occupied cells to their canonical MOC
    via mortie's ``compress_moc`` (mixed order allowed; an occupied ancestor
    absorbs its descendants, complete sibling quads merge), then — unless one
    member already covers everything — split at the deepest common ancestor
    (``common_ancestor``, the longest common decimal-string prefix) and
    **tighten** each of its 2-4 intersecting children to the common ancestor of
    the occupancy inside it (review finding, PR #208). The result is
    deterministic/canonical and a conservative superset of the input — no
    occupied cell escapes the box — but NOT always the globally minimal
    <= 4-member MOC (optimizing member areas under a 4-member cap is a harder
    search); this "DCA children, each tightened" construction is the definition
    that freezes with the mortie-side tier-0 spec.

    Accepts anything :func:`morton_words` does; returns sorted packed ``uint64``
    words. Raises ``ValueError`` on empty input (and, via mortie, on a set
    spanning HEALPix base cells — a shard's cells are one subtree by
    construction, so the hive path never triggers it).
    """
    from mortie import clip2order, common_ancestor, compress_moc

    words = morton_words(values)
    if words.size == 0:
        raise ValueError("morton_box requires at least one occupied cell")
    occ = compress_moc(words)
    if occ.size == 1:
        return occ
    # After compression every member is strictly deeper than the ancestor (a
    # member at the ancestor's own order would contain the rest and compress
    # to a single cell), so coarsening lands each on one of its 2-4 children;
    # the per-child common_ancestor then drops each member to the deepest
    # cell covering that child's share of the occupancy.
    anc = morton_decimal(common_ancestor(occ))
    anc_order = len(anc) - (2 if anc.startswith("-") else 1)
    children = clip2order(anc_order + 1, occ)
    box = [common_ancestor(occ[children == child]) for child in np.unique(children)]
    return np.sort(np.asarray(box, dtype=np.uint64))


def morton_to_arrow(values):
    """Export ``values`` as a typed ``arro3.core.Array`` (issue #135).

    The Arrow leg of the boundary: the returned array carries mortie's
    ``morton_index`` extension type in its field metadata
    (:data:`MORTON_EXTENSION_NAME`), pulled zero-copy over the PyCapsule C Data
    Interface (``MortonIndexArray.__arrow_c_array__``; mortie >= 0.8.4) — no
    pyarrow on the path. Accepts a ``MortonIndexArray`` or any
    ``uint64``-coercible array-like of packed words; the all-zero empty sentinel
    is exported as an Arrow null.
    """
    from arro3.core import Array

    if not is_morton_array(values):
        values = to_morton_array(values)
    return Array.from_arrow(values)


def morton_from_arrow(col):
    """Reconstruct a ``MortonIndexArray`` from a typed Arrow column.

    The inverse of :func:`morton_to_arrow`: ``col`` is any Arrow C-Data source
    (an ``arro3.core.Array``, a chunked ``ChunkedArray`` column, or a
    ``(schema, array)`` capsule pair). Arrow nulls come back as the all-zero
    empty sentinel word, so ``isna`` round-trips.
    """
    from mortie import MortonIndexArray

    return MortonIndexArray.from_arrow(col)


def is_morton_arrow(col) -> bool:
    """True if ``col`` is an Arrow array/column carrying the morton extension type.

    Reads the ``ARROW:extension:name`` field metadata (present on both an
    ``arro3.core.Array`` and a table column's ``ChunkedArray``); anything
    without field metadata is not a typed morton column.
    """
    field = getattr(col, "field", None)
    if field is None:
        return False
    # metadata_str is already a plain dict on arro3 — no copy on the write path.
    return field.metadata_str.get(_EXTENSION_NAME_KEY) == MORTON_EXTENSION_NAME


__all__ = [
    "MORTON_CONVENTION",
    "MORTON_EXTENSION_NAME",
    "RESOLUTION_EXACT",
    "RESOLUTION_MIXED",
    "RESOLUTION_POINT",
    "is_morton_array",
    "is_morton_arrow",
    "morton_box",
    "morton_decimal",
    "morton_from_arrow",
    "morton_to_arrow",
    "morton_word",
    "morton_words",
    "to_morton_array",
]
