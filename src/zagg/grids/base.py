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


def vector_array_spec(base, sig, *, base_dims, base_chunk_shape):
    """Extend a scalar data-var ``ArraySpec`` with a trailing payload dim.

    Issue #29 phase 5: a ``kind: vector`` field is stored as a dense array of
    shape ``(*spatial_shape, *trailing_shape)`` — the per-cell vector rides on
    one or more trailing dimensions appended to the grid's spatial axes.

    **Single-trailing-chunk invariant.** The trailing payload dimension(s) are
    chunked *whole* (one chunk spans the full ``trailing_shape``). This is what
    lets :func:`zagg.processing.write_dataframe_to_zarr` address a shard's
    payload with ``block_idx = chunk_idx + (0,) * len(trailing_shape)`` — the
    trailing block index is always ``0``. Do not chunk the trailing dim; the
    writer assumes block 0.

    Parameters
    ----------
    base : ArraySpec
        The scalar data-var spec for this grid (already carries the field's
        ``dtype``/``fill_value``); its ``shape`` is the spatial shape and its
        chunk grid the spatial chunk shape.
    sig : dict
        Output signature from :func:`zagg.config.get_output_signature`
        (``kind``/``trailing_shape``/``dtype``). For a scalar field (empty
        ``trailing_shape``) ``base`` is returned unchanged.
    base_dims : tuple of str
        Spatial dimension names (e.g. ``("cells",)`` or ``("y", "x")``).
    base_chunk_shape : tuple of int
        Spatial chunk shape (the grid's ``chunk_shape``).

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    trailing = tuple(int(t) for t in sig["trailing_shape"])
    if not trailing:
        return base
    shape = (*base.shape, *trailing)
    dim_names = (*base_dims, *(f"{base_dims[-1]}_v{i}" for i in range(len(trailing))))
    chunk_shape = (*base_chunk_shape, *trailing)
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(chunk_shape)})
    # Set shape + dimension_names together: ArraySpec validates their ranks
    # match on construction, so a chained ``with_shape`` then
    # ``with_dimension_names`` would transiently mismatch and raise.
    return type(base)(
        **{
            **base.model_dump(),
            "shape": shape,
            "dimension_names": dim_names,
            "chunk_grid": chunk_grid,
        }
    )


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
        family, aligned, whole-number resolution ratios, and the same Option-B
        output-field set (issue #29 — same scalar/vector kinds, trailing
        shapes, dtypes). Cross-family (HEALPix vs rectilinear) always returns
        False.
        """
        ...


__all__ = [
    "OutputGrid",
    "ShardKey",
    "InconsistentShardError",
    "vector_array_spec",
]
