"""OutputGrid protocol — pluggable spatial grid interface.

Each implementation owns the grid-specific operations the pipeline needs:
shard enumeration, point-to-cell assignment, write-partition identity,
storage-block mapping, footprint geometry, and Zarr template emission.

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

    parent_order: int
    child_order: int

    def coverage(
        self, polygon_parts: list[tuple[np.ndarray, np.ndarray]]
    ) -> np.ndarray:
        """Enumerate shard keys covering multipart polygons."""
        ...

    def assign(self, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
        """Map (lat, lon) points to leaf cell IDs at child_order."""
        ...

    def shards_of(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Vectorized: shard key for each leaf cell."""
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
        """Enumerate leaf cell IDs under a shard, in the canonical chunk order."""
        ...

    def encode_cell_ids(self, leaf_ids: np.ndarray) -> np.ndarray:
        """Encode leaf IDs to the output coord array (e.g., morton → healpix)."""
        ...

    def emit_template(self, store: Store, *, overwrite: bool = False) -> Store:
        """Write a Zarr template (group + arrays) for this grid to ``store``."""
        ...


__all__ = ["OutputGrid", "ShardKey", "InconsistentShardError"]
