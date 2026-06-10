"""OutputGrid protocol — pluggable spatial grid interface.

Each implementation owns the grid-specific operations the pipeline needs:
shard enumeration, point-to-cell assignment, write-partition identity,
storage-block mapping, footprint geometry, and Zarr template emission.

Terminology
-----------
- **leaf id** — the high-precision spatial identifier returned by ``assign``.
  Grid-specific (HEALPix: order-18 morton; rectilinear: flat row-major cell
  index). Pipeline code treats it as an opaque integer.
- **cell id** — the aggregation-grid identifier returned by ``cells_of`` and
  ``children``. For some grids (rectilinear) leaf id and cell id coincide.
- **shard key** — the write-partition identifier returned by ``shard_of``.
  ``block_index`` maps a shard key to a storage block tuple.

The shard_of / block_index split (see bench/REPORT.md §2) keeps spatial
identity separate from storage position. For grids where chunks tile space
algorithmically (rectilinear, fullsphere DGGS) the two collapse; for dense-
packed sparse DGGS arrays they differ and block_index needs the populated-
shard list to translate.
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import numpy as np
from zarr.abc.store import Store

ShardKey = Any  # int for HEALPix, tuple[int,int] for rectilinear, etc.


class InconsistentShardError(ValueError):
    """Raised by shard_of when input cells don't all share a shard."""


@runtime_checkable
class OutputGrid(Protocol):
    """Pluggable output grid.

    Implementations may be stateless (fullsphere DGGS, rectilinear) or carry
    a populated-shard list (dense DGGS) so block_index can resolve.
    """

    @property
    def array_shape(self) -> tuple[int, ...]:
        """Full output-array shape (1D for DGGS, 2D for rectilinear)."""
        ...

    @property
    def chunk_shape(self) -> tuple[int, ...]:
        """Per-chunk shape (rank matches ``array_shape``)."""
        ...

    def coverage(
        self, polygon_parts: list[tuple[np.ndarray, np.ndarray]]
    ) -> np.ndarray:
        """Enumerate shard keys covering multipart polygons."""
        ...

    def assign(self, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
        """Map (lat, lon) points to leaf ids."""
        ...

    def cells_of(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Coarsen leaf ids to aggregation cell ids."""
        ...

    def shards_of(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Vectorized: shard key for each leaf id."""
        ...

    def shard_of(self, leaf_ids: np.ndarray) -> ShardKey:
        """All leaves must share a shard; return it.

        Raises
        ------
        InconsistentShardError
            If the input cells span more than one shard.
        """
        ...

    def block_index(self, shard_key: ShardKey) -> tuple[int, ...]:
        """Storage block index for this shard in the Zarr array.

        For stateless layouts this is pure arithmetic on shard_key. For dense
        layouts it consults the populated-shard list built at template time.
        """
        ...

    def shard_footprint(self, shard_key: ShardKey):
        """Shard polygon in WGS84 (shapely.Geometry)."""
        ...

    def children(self, shard_key: ShardKey) -> np.ndarray:
        """Enumerate cell ids under a shard, in canonical chunk order."""
        ...

    def encode_cell_ids(self, cell_ids: np.ndarray) -> np.ndarray:
        """Encode cell ids to the output coord array (e.g., morton → healpix)."""
        ...

    def chunk_coords(self, shard_key: ShardKey) -> dict:
        """Per-cell coord column values for a chunk (HEALPix: morton, cell_ids).

        Empty for grids that store coords as 1D dimensional arrays on the
        template (e.g., rectilinear's ``x``/``y``).
        """
        ...

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write a Zarr template (group + arrays) for this grid to ``store``."""
        ...

    def signature(self) -> dict:
        """Canonical fingerprint of the grid's defining parameters.

        Recorded in a ShardMap at build time and compared at run time so a
        shard map can never be silently paired with a mismatched grid.
        """
        ...

    def nests_with(self, other: "OutputGrid") -> bool:
        """Whether this grid and ``other`` tile compatibly (align + nest).

        The primitive for cross-aggregator compatibility validation: same
        family, aligned, whole-number resolution ratios. Cross-family
        (HEALPix vs rectilinear) always returns False.
        """
        ...


__all__ = ["OutputGrid", "ShardKey", "InconsistentShardError"]
