"""Boundary adapter for mortie's ``morton_index`` extension type.

The ``morton`` output coordinate is carried in memory as a mortie
:class:`~mortie.morton_index.MortonIndexArray` (a pandas ExtensionArray over the
packed ``uint64`` Morton words). On disk it is stored as plain ``uint64`` — Zarr
stores numpy dtypes, not pandas extension dtypes — and reconstructed as a
``MortonIndexArray`` on read.

This is the contained #71 migration: only the ``morton`` coordinate adopts the
type. ``cell_ids`` stays NESTED ``uint64`` (the DGGS coordinate, unchanged), and
the internal leaf/cell/shard morton arithmetic (``cells_of`` / ``shards_of`` /
``children``) stays on plain ``uint64`` ndarrays.

Storing the raw ``uint64`` words (rather than a reinterpreted ``int64``) is what
removes the sign hazard: the packed word's prefix is ``base+1``, so base cells
7–11 set bit 63 and read back negative under an ``int64`` coordinate. ``uint64``
keeps them non-negative and the Z-order intact (espg/zagg#71).
"""

from __future__ import annotations

import numpy as np


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


__all__ = ["is_morton_array", "morton_words", "to_morton_array"]
