"""Chunk-local nested rank ↔ 2-D ``(row, col)`` tensor layout (issue #336).

A depth-``d`` HEALPix subtree holds ``4**d`` cells whose ascending nested
(morton) order traces a Z-order curve over a ``2**d × 2**d`` block — so a
row-major reshape of the 1-D cells axis scrambles the block spatially beyond
2-cell runs. The faithful mapping is the pure bit deinterleave that mortie
0.9.3 ships as ``rank_to_xy``/``xy_to_rank`` (espg/mortie#150; normative
contract in mortie's ``docs/specification.md`` §8, frozen for mortie 1.x):
``x`` gathers the rank's even bits, ``y`` its odd bits, with ``(0, 0)`` at the
subtree's **south corner**, ``x`` increasing toward the north-east edge and
``y`` toward the north-west edge — exactly healpy's face-local
``pix2xyf``/``xyf2pix`` (nest) convention.

Row/col orientation (pinned here, once)
---------------------------------------
Tensor axis 0 (row) is ``y`` and axis 1 (col) is ``x``. This matches
gridlook's texture convention (``src/ui/grids/Healpix.vue``,
``getUnshuffleIndex``): ``texture[i*size + j] = data[bit_combine(j, i)]``,
i.e. the texture's slow axis ``i`` feeds ``bit_combine``'s second (odd-bit,
``y``) argument and the fast axis ``j`` its first (even-bit, ``x``) argument
— so a zagg tensor uploads to a gridlook texture with no further shuffle.
``tensor[0, 0]`` is the subtree's south corner; rows advance toward the
north-west edge, columns toward the north-east edge.

Subtree spans (issue #351)
--------------------------
The same nested ordering makes "everything below one morton node" a
contiguous cell-index span (normative: zagg ``docs/specification.md`` §1.5).
:func:`normalize_subtree` and :func:`subtree_cell_span` are the pure
arithmetic behind the readers' ``subtree=`` keyword — no store access.
"""

from __future__ import annotations

import warnings

from mortie import rank_to_xy, xy_to_rank

__all__ = ["rank_to_rowcol", "rowcol_to_rank", "normalize_subtree", "subtree_cell_span"]


def rank_to_rowcol(rank, depth: int):
    """``(row, col)`` tensor position of a chunk-local nested rank.

    ``rank`` is the cell's position ``0..4**depth - 1`` within its depth-
    ``depth`` subtree (scalar or array; the same position the ragged writers
    use on the cells axis). Returns ``(row, col) = (y, x)`` per the module
    orientation contract (mortie spec §8 / gridlook ``bit_combine(j, i)``).
    """
    x, y = rank_to_xy(rank, depth)
    return y, x


def rowcol_to_rank(row, col, depth: int):
    """Chunk-local nested rank at a ``(row, col)`` tensor position.

    Inverse of :func:`rank_to_rowcol`: ``row`` is ``y``, ``col`` is ``x``
    (scalars or arrays; values must be ``< 2**depth``).
    """
    return xy_to_rank(col, row, depth)


def normalize_subtree(subtree) -> tuple[int, str, int]:
    """Normalize a ``subtree=`` argument to ``(word, decimal, order)``.

    Both ratified currencies (issue #351): a packed morton AREA word
    (``int``) or a decimal morton string (``str``). A parsed string always
    yields the area word (mortie's spec §4 parse tie-break), so no kind
    flag is needed. Raises ``ValueError`` on a malformed member of either
    currency (an unparseable string, an invalid packed word).
    """
    from zagg.grids.morton import morton_decimal, morton_word

    word = morton_word(subtree) if isinstance(subtree, str) else int(subtree)
    decimal = morton_decimal(word)  # raises on an invalid/negative packed word
    return word, decimal, len(decimal) - (2 if decimal.startswith("-") else 1)


def subtree_cell_span(
    subtree,
    anchor: int,
    anchor_index: int,
    cell_order: int,
    n_cells: int,
    field: str,
    *,
    stacklevel: int = 2,
) -> tuple[int, int]:
    """Cell-index span ``[start, stop)`` of ``subtree`` on a nested cells axis.

    The spec §1.5 subtree-span identity, resolved by pure arithmetic: a
    cell's axis position is its HEALPix NESTED id (chunks are keyed by the
    parent's nested id — ``HealpixGrid.block_index`` — and the in-chunk
    position is the nested rank), so the order-``k`` subtree below a word
    with nested id ``h`` occupies ``[h·4^d, (h+1)·4^d)`` (``d = cell_order
    - k``) in fullsphere coordinates. A single-root power-of-four axis (a
    hive leaf's shard subtree) subtracts its root's own span start, with the
    root derived from ``anchor`` — any WRITTEN cell word (the morton anchor
    slice the caller already read); no store bytes are touched here.

    That identity is CHECKED, not assumed: ``anchor`` sits at ``anchor_index``
    on the axis, so ``nested(anchor) - root_start`` must equal ``anchor_index``
    or the axis is not in canonical nested placement, and this arithmetic
    would hand back plausible-but-wrong cells silently — the single-root
    geometry is INFERRED from ``4**depth == n_cells`` alone, and this is the
    first reader that derives absolute placement instead of searching for it
    (review, PR #357). A mismatch raises ``ValueError``.

    The span is returned intersected with the axis: a word at or above the
    axis root clips to the whole axis (every stored cell is its
    descendant), and a well-formed word DISJOINT from the axis warns — naming
    the word and the axis root — and returns the empty span ``(0, 0)``.
    Out-of-domain is a data answer (the ratified issue #351 fork (3)), so an
    empty read is ambiguous between "in-domain, nothing stored" and "outside
    the domain" unless the warning is observed — and DELIVERY of that warning
    is the caller's ``warnings`` filter's call: the default ``default`` action
    dedups on (message, category, module, lineno), so a second call with the
    same word is silent in the same process. The guarantee is "at most once
    per call" (never per cell or per chunk). ``stacklevel`` is the caller's
    depth from here — the readers pass the depth that attributes the warning
    to THEIR caller rather than to zagg's own frames.
    Raises ``ValueError`` for a malformed word, or one deeper than the
    cells-axis order (grammatically impossible — nothing stored could be
    its descendant).
    """
    from mortie import clip2order, mort2healpix

    from zagg.grids.morton import morton_decimal

    word, decimal, order = normalize_subtree(subtree)
    if order > cell_order:
        raise ValueError(
            f"subtree {decimal} is order {order} — deeper than {field!r}'s "
            f"order-{cell_order} cells axis, so nothing stored can be its descendant "
            f"(a subtree names an ancestor, at any order up to the cell order)"
        )
    d = cell_order - order
    h, _o = mort2healpix(word)  # scalar in → scalar out (mortie ≥1.0, espg/mortie#219)
    lo = h * 4**d
    n = int(n_cells)
    depth = (n.bit_length() - 1) // 2
    # A fullsphere axis (12·4^c cells) starts at 0 — the nested id IS the axis
    # position, and every well-formed word's span lies inside it. A single-root
    # power-of-four axis starts at its root's own span start.
    single_root = 4**depth == n
    root_order, root, root_start = cell_order - depth, 0, 0
    if single_root:
        # clip2order's scalar form is length-1-array-out (its documented 1.x
        # contract, unlike mort2healpix's scalar-out), so the [0] stays.
        root = int(clip2order(root_order, anchor)[0])
        rh, _ro = mort2healpix(root)
        root_start = rh * 4**depth
    ah, _ao = mort2healpix(anchor)
    if ah - root_start != int(anchor_index):
        raise ValueError(
            f"{field!r}'s cells axis is not in canonical nested placement: cell "
            f"{int(anchor_index)} carries morton {morton_decimal(int(anchor))}, whose "
            f"nested id puts it at axis position {ah - root_start}. A subtree "
            f"span is derived arithmetically (cell position == nested id minus the "
            f"axis root's start), so this axis would yield the wrong cells"
        )
    if not single_root:
        return lo, lo + 4**d
    lo -= root_start
    start, stop = max(lo, 0), min(lo + 4**d, n)
    if start >= stop:
        warnings.warn(
            f"subtree {decimal} is outside this axis' order-{root_order} root "
            f"{morton_decimal(root)} — yielding nothing",
            stacklevel=stacklevel,
        )
        return 0, 0
    return start, stop
