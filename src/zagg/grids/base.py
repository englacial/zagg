"""OutputGrid protocol — pluggable spatial grid interface.

Each implementation owns the grid-specific operations the pipeline needs:
shard enumeration, point-to-cell assignment, write-partition identity,
storage-block mapping, footprint geometry, and Zarr template emission.

Terminology
-----------
- **leaf id** — the high-precision spatial identifier returned by ``assign``.
  Grid-specific (HEALPix: morton at the grid's reference order; rectilinear:
  flat row-major cell index). Pipeline code treats it as an opaque integer.
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
    # Name trailing payload axes ``vector`` (one dim) or ``vector_0``/``vector_1``
    # (multi-dim, e.g. a t-digest ``(k, 2)``) — distinct from the spatial axes.
    if len(trailing) == 1:
        trailing_names: tuple[str, ...] = ("vector",)
    else:
        trailing_names = tuple(f"vector_{i}" for i in range(len(trailing)))
    dim_names = (*base_dims, *trailing_names)
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


def chunk_array_spec(base, *, chunk_grid_shape, chunk_dims):
    """Build a chunk-resolution companion ``ArraySpec`` from a scalar data-var spec.

    Issue #30 item 2: a ``resolution: chunk`` field stores ONE value per chunk
    rather than one per aggregation cell. Its array is shaped at the *chunk grid*
    (``grid.chunk_grid_shape`` = number of chunks along each axis), with one Zarr
    block per chunk so :func:`zagg.processing.write_dataframe_to_zarr` writes a
    chunk's single value at its chunk block index.

    At K==1 (``chunk_inner`` unset, one chunk per shard) that index is
    ``grid.block_index(shard_key)``. At K>1 (issue #30 item 3) the chunk grid is
    finer than the shard grid, so the per-chunk index is the block yielded by
    ``grid.iter_chunks(shard_key)``, not ``block_index``.

    The companion carries the field's ``dtype``/``fill_value`` (inherited from
    ``base``); only its shape, dimension names and chunk grid are re-set to the
    chunk grid. Each axis is chunked ``1`` (one chunk == one block).

    Parameters
    ----------
    base : ArraySpec
        The scalar data-var spec for this field (already carries
        ``dtype``/``fill_value``).
    chunk_grid_shape : tuple of int
        Number of chunks along each spatial axis (the companion array shape).
    chunk_dims : tuple of str
        Dimension names for the chunk-grid axes (e.g. ``("chunks",)`` for HEALPix,
        ``("chunk_y", "chunk_x")`` for rectilinear).

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    shape = tuple(int(s) for s in chunk_grid_shape)
    chunk_shape = tuple(1 for _ in shape)
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(chunk_shape)})
    return type(base)(
        **{
            **base.model_dump(),
            "shape": shape,
            "dimension_names": tuple(chunk_dims),
            "chunk_grid": chunk_grid,
        }
    )


def sharded_array_spec(base, *, shard_shape, inner_chunk_shape):
    """Wrap a dense data-var ``ArraySpec`` in a zarr ``ShardingCodec`` (issue #108).

    Decouples the **write/dispatch** granularity (one shard object per dispatch
    shard) from the **read** granularity (the inner 64×64 chunk). The K inner
    chunks of one shard become ONE shard object (empties omitted from the shard
    index, so sub-shard sparsity is preserved *inside* the object) instead of K
    independent regular chunk objects.

    The transform: the array's outer chunk grid is re-set to ``shard_shape`` (the
    whole dispatch shard) and a ``sharding_indexed`` codec replaces the array's
    top-level codecs, carrying the inner ``chunk_shape`` (the 64×64 read chunk)
    plus the array's existing (bytes-only) codecs as its INNER codecs. Building
    the codec config by hand — rather than ``ArraySpec.from_array`` (which drops
    the sharding codec in pydantic-zarr 0.10.0) or ``zarr.create_array`` (which
    silently injects a zstd level-0 compressor) — is what preserves zagg's
    bytes-only/uncompressed on-disk policy (the two prototype caveats on
    issue #108).

    Parameters
    ----------
    base : ArraySpec
        The dense data-var spec whose ``chunk_grid`` currently chunks at the
        inner chunk shape; its ``codecs`` are the bytes-only inner codecs.
    shard_shape : tuple of int
        Outer chunk (== shard) shape: the whole dispatch shard's cell extent.
    inner_chunk_shape : tuple of int
        Inner chunk shape (the 64×64 read chunk); ``prod(shard_shape) //
        prod(inner_chunk_shape) == K`` inner chunks per shard.

    Returns
    -------
    ArraySpec
    """
    from pydantic_zarr.experimental.v3 import NamedConfig

    inner_codecs = [c.model_dump() if hasattr(c, "model_dump") else dict(c) for c in base.codecs]
    # crc32c on the shard index matches zarr's create_array default; the index
    # payload is bytes-only (no compression), keeping the policy bytes-only.
    index_codecs = [
        {"name": "bytes", "configuration": {"endian": "little"}},
        {"name": "crc32c"},
    ]
    sharding = NamedConfig(
        name="sharding_indexed",
        configuration={
            "chunk_shape": list(inner_chunk_shape),
            "codecs": inner_codecs,
            "index_codecs": index_codecs,
            "index_location": "end",
        },
    )
    chunk_grid = NamedConfig(name="regular", configuration={"chunk_shape": list(shard_shape)})
    return type(base)(
        **{
            **base.model_dump(),
            "chunk_grid": chunk_grid,
            "codecs": (sharding,),
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

    def coverage(self, polygon_parts: list[tuple[np.ndarray, np.ndarray]]) -> np.ndarray:
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
    "chunk_array_spec",
    "sharded_array_spec",
]
