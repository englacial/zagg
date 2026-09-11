"""Per-cell temporal envelopes over mortie toc words (spec §8.2, issue #410).

The `zagg-toc/1` companion family has two shapes and this module owns the
coarser one. A **per-centroid** companion is folded inside the digest kernel
(:func:`zagg.stats.tdigest._centroid_envelopes`, spec §8.3) because its
partition is the digest's own; a **per-cell** companion is one word for a whole
cell, which is a plain reduction and belongs here:

- :func:`cell_envelope` is the reducer a config declares (``temporal:
  per-cell``): a cell's observations in, one ``uint64`` word out. It is what
  GEDI's shot pooling uses — every bin of a shot shares one instant, so the
  word is exact when a cell holds one shot and a conservative range when
  several pool (ruling 2 on issue #410).
- :func:`cell_envelopes` is its segmented sibling, the shape-coarsening
  reduction spec §8.4 licenses: many cells' words in, one word per cell out.
  Both the pyramid folds (per-centroid leaves -> per-cell overviews, ruling 3)
  and a dense per-cell fold up the ladder run through it.

Both are the grammar's semilattice join (``mortie.toc_merge``, reduced by
``mortie.toc_reduce``, scalar and segmented), so both are associative,
commutative and idempotent: the output word is **bit-identical whatever order
or tree** the contributors were reduced in — the §8.2 guarantee that lets an
overview be folded from leaves or cascaded from finer overviews and come out
the same. The join preserves an instant (a cell whose observations share one
nanosecond keeps its exact timestamp word rather than widening into a range),
and it never narrows: every contributor's coverage is inside the result.

The word grammar itself — bit layout, flag position, sort order, merge law —
is mortie's and is not restated here (spec §8's citation).
"""

from __future__ import annotations

import numpy as np

from zagg.time_axis import TOC_UNOBSERVED

__all__ = ["cell_envelope", "cell_envelopes"]


def _checked(words) -> np.ndarray:
    """Validate a toc word array: packed ``uint64``, no reserved ``0``.

    ``0`` is §8.2's unobserved-cell marker and no encoder produces it, so a
    zero reaching a fold is a fill value that leaked into real words — the
    reduction would happily join it and return an envelope reaching back to the
    grammar's epoch. Refusing is the honest failure.

    Deliberately separate from the digest kernel's
    :func:`zagg.stats.tdigest._check_words`, which makes the same refusal: that
    one's other job is aligning a channel against a digest's centroid count, a
    shape these per-cell reducers have no analogue for, and this module names the
    marker through :data:`zagg.time_axis.TOC_UNOBSERVED` rather than a literal.
    """
    arr = np.asarray(words)
    if arr.dtype != np.uint64:
        raise ValueError(f"toc words dtype {arr.dtype} is not uint64; pass packed toc words")
    if arr.size and not arr.all():
        raise ValueError(
            f"toc words contain the reserved {TOC_UNOBSERVED} word (spec §8.2's "
            f"unobserved-cell marker); pass encoded toc words, not a fill value"
        )
    return arr


def cell_envelope(words) -> np.uint64:
    """One cell's temporal envelope: the join over its observations' toc words.

    The ``temporal: per-cell`` reducer (spec §8.2). Returns a **timestamp** word
    when every contributor covers one instant and a conservative **range** word
    otherwise — instants never widened, intervals never narrowed. An empty cell
    raises: the join has no identity element, so "the envelope of nothing" has
    no answer, and the writer's own fill (:data:`zagg.time_axis.TOC_UNOBSERVED`)
    is what marks an unobserved cell instead.
    """
    import mortie

    arr = _checked(words)
    if arr.size == 0:
        raise ValueError(
            "cell_envelope of no observations: the toc join has no identity element, "
            f"so an unobserved cell is marked by the reserved {TOC_UNOBSERVED} fill "
            f"(spec §8.2), never reduced to a word"
        )
    return np.uint64(mortie.toc_reduce(arr))


def cell_envelopes(words, offsets) -> np.ndarray:
    """Per-group envelopes: the §8.4 shape-coarsening reduction, batched.

    ``offsets`` is the arrow list layout — group ``i`` spans ``[offsets[i],
    offsets[i + 1])``, exactly covering ``words`` — so the whole partition
    crosses into Rust once (``mortie.toc_reduce``, espg/mortie#177) instead of
    a Python loop per cell. Result ``i`` is bit-identical to
    ``cell_envelope(words[offsets[i]:offsets[i + 1]])``, and an empty group is
    an error for the same reason it is there.
    """
    import mortie

    arr = _checked(words)
    return np.asarray(
        mortie.toc_reduce(arr, offsets=np.asarray(offsets, dtype=np.int64)), dtype=np.uint64
    )
