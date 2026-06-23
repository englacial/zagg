"""Write-stage helpers for :mod:`zagg.processing` (split out of the monolithic
``processing.py`` for the §4 size limit; pure relocation, no behavior change).

Assembles the per-shard output carrier and writes it to the Zarr template
(including the ``resolution: chunk`` companion path). Depends only on
``config``/``grids`` — never on the read or aggregate stages — so the import DAG
stays acyclic.
"""

import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    get_agg_fields,
    get_output_signature,
)
from zagg.grids.morton import is_morton_array, morton_words


def _arrow_column(block: np.ndarray, sig: dict):
    """Build the Arrow column for one agg field from its per-cell stats block.

    A scalar field's ``(n_cells,)`` block becomes a plain Arrow array (values
    byte-for-byte identical to the pandas carrier). A ``vector`` field's
    ``(n_cells, *trailing_shape)`` block becomes a ``FixedSizeList<C>`` column
    (``C = prod(trailing_shape)``), so every cell carries an identically-sized
    list. Keeping the vector path a list-carrier (rather than a bespoke 2-D
    column) is what lets the future ragged t-digest slot in as a variable-length
    ``List<FixedSizeList<2>>`` through the same seam (issue #29 Tier 2).
    """
    import pyarrow as pa

    if sig["kind"] != "vector":
        return pa.array(block)
    width = int(np.prod(sig["trailing_shape"]))
    flat = np.ascontiguousarray(block).reshape(-1)
    return pa.FixedSizeListArray.from_arrays(pa.array(flat), width)


def _build_output(stats_arrays, data_vars, agg_fields, grid, shard_key, use_arrow: bool):
    """Assemble the per-shard output carrier from the per-cell stats blocks.

    Returns a ``pandas.DataFrame`` for a pure-scalar config (unchanged) or a
    ``pyarrow.Table`` when any ``vector`` field is present, in both cases with the
    data-variable columns followed by the grid's per-cell coord columns.
    """
    if not use_arrow:
        df_out = pd.DataFrame({var: stats_arrays[var] for var in data_vars})
        for col_name, vals in grid.chunk_coords(shard_key).items():
            df_out[col_name] = vals
        return df_out

    import pyarrow as pa

    columns = {
        var: _arrow_column(stats_arrays[var], get_output_signature(agg_fields[var]))
        for var in data_vars
    }
    for col_name, vals in grid.chunk_coords(shard_key).items():
        # Route the morton coordinate through the same uint64 boundary as the
        # pandas carrier (#71), so both carriers share one on-disk dtype guarantee
        # rather than relying on MortonIndexArray.__array__ returning uint64.
        arr = morton_words(vals) if is_morton_array(vals) else np.asarray(vals)
        columns[col_name] = pa.array(arr)
    return pa.table(columns)


def _carrier_empty(carrier) -> bool:
    """Whether a process_shard output carrier (DataFrame or Arrow table) is empty."""
    if isinstance(carrier, pd.DataFrame):
        return carrier.empty
    return carrier.num_rows == 0


def write_dataframe_to_zarr(
    df_out,
    store: Store,
    *,
    grid,
    chunk_idx: tuple,
) -> Store:
    """Write a per-shard output carrier to an existing Zarr template.

    Parameters
    ----------
    df_out : pandas.DataFrame or pyarrow.Table
        Coordinate + data-variable columns. A ``pyarrow.Table`` is used when the
        config declares any ``vector`` field (issue #29): its ``FixedSizeList``
        columns carry the per-cell ``trailing_shape`` payload, written to a
        Zarr array with a trailing dimension. Cell count must equal
        ``prod(grid.chunk_shape)``; cells are in the grid's canonical chunk order
        (``grid.children(shard_key)``).
    store : Store
        Zarr-compatible store with the template already written.
    grid : OutputGrid
        Grid the data was aggregated against. Provides ``group_path`` and
        ``chunk_shape`` for routing the write.
    chunk_idx : tuple of int
        Storage block index for this shard, as returned by
        ``grid.block_index(shard_key)``.

    Returns
    -------
    Store
        The same store, with data written.
    """
    if _carrier_empty(df_out):
        return store

    expected_count = int(np.prod(grid.chunk_shape))
    n_cells = len(df_out) if isinstance(df_out, pd.DataFrame) else df_out.num_rows
    if n_cells != expected_count:
        raise ValueError(
            f"Expected {expected_count} rows for chunk_shape={grid.chunk_shape}, got {n_cells}"
        )

    # Fields declared ``resolution: chunk`` (issue #30 item 2) are written ONCE per
    # chunk into a companion array shaped at the chunk grid, not the per-cell array.
    # Read the set from the grid's config (every concrete grid carries ``config``).
    chunk_res_fields = _chunk_resolution_fields(getattr(grid, "config", None))

    chunk_idx = tuple(int(i) for i in chunk_idx)
    for name, values in _iter_carrier_columns(df_out):
        if name in chunk_res_fields:
            # resolution: chunk — the column must be chunk-uniform (every populated
            # cell carries the same chunk value), so collapse the CELL axis to the
            # single value and write it to the companion array's one-block-per-chunk
            # grid at ``chunk_idx`` (which IS ``grid.block_index(shard_key)``). One
            # value per chunk, no per-cell duplication on disk (issues #30 item 2, #82).
            # A vector companion keeps its ``trailing`` shape: the block is
            # (1, ..., 1, *trailing) and the trailing axis is one whole chunk, so the
            # block index appends ``0`` per trailing dim (issue #29's invariant).
            chunk_value = np.asarray(_chunk_uniform_value(name, values))
            trailing = chunk_value.shape
            block = chunk_value.reshape((1,) * len(chunk_idx) + trailing)
            block_idx = chunk_idx + (0,) * len(trailing)
            with config.set({"async.concurrency": 128}):
                array = open_array(
                    store,
                    path=f"{grid.group_path}/{name}",
                    zarr_format=3,
                    consolidated=False,
                )
                array.set_block_selection(block_idx, block)
            continue

        # Scalar columns reshape to the grid's chunk_shape; a vector column keeps
        # its trailing payload dim(s), so the block (and target array) is
        # (*chunk_shape, *trailing_shape). The cell count invariant is unchanged.
        #
        # Single-trailing-chunk invariant (issue #29): the template
        # (``grids.base.vector_array_spec``) chunks the trailing payload dim
        # *whole*, so the trailing block index is always 0 and a shard's payload
        # lands in one Zarr block via ``chunk_idx + (0,) * len(trailing)``.
        trailing = values.shape[1:]
        values = values.reshape((*grid.chunk_shape, *trailing))
        block_idx = chunk_idx + (0,) * len(trailing)
        with config.set({"async.concurrency": 128}):
            array = open_array(
                store,
                path=f"{grid.group_path}/{name}",
                zarr_format=3,
                consolidated=False,
            )
            if trailing:
                # Enforce the single-trailing-chunk invariant: the target array's
                # trailing chunk must span the whole payload, or set_block_selection
                # at block 0 would silently write only part of it (issue #29).
                target_trailing_chunks = array.chunks[len(grid.chunk_shape) :]
                if target_trailing_chunks != trailing:
                    raise ValueError(
                        f"vector field {name!r}: trailing chunk "
                        f"{target_trailing_chunks} must equal trailing shape "
                        f"{trailing} (the payload dim must be one whole chunk)"
                    )
            array.set_block_selection(block_idx, values)

    return store


def _chunk_resolution_fields(config: PipelineConfig | None) -> set[str]:
    """Names of agg fields declared ``resolution: chunk`` (issue #30 item 2).

    Empty for a config without any such field, so the writer's per-cell path is
    byte-for-byte unchanged for existing (cell-resolution-only) configs.
    """
    if config is None:
        return set()
    return {
        name
        for name, meta in get_agg_fields(config).items()
        if get_output_signature(meta)["resolution"] == "chunk"
    }


def _chunk_uniform_value(name: str, values: np.ndarray):
    """Collapse a ``resolution: chunk`` field's per-cell column to its single value.

    A chunk-resolution field stores ONE value per chunk (issue #30 item 2), so its
    per-cell column must be chunk-uniform: every *populated* cell carries the same
    chunk value. Empty cells (which carry the field's fill sentinel — ``NaN`` for a
    float field, or the bare-chunk-anchor when the expression is a bare precompute
    name) are ignored when selecting the representative value, so an empty cell 0 no
    longer poisons the companion write with ``NaN``.

    The column may be a scalar ``(n_cells,)`` array or a vector
    ``(n_cells, *trailing)`` block (issue #82): the CELL axis (axis 0) is collapsed,
    keeping the per-element ``trailing`` shape. Uniformity is checked per-element over
    the trailing axis — a populated cell is one whose vector is not all-NaN — and the
    returned value is a 0-d scalar (scalar field) or a ``trailing``-shaped array.

    Raises a clear error if the populated cells are NOT uniform — that means the
    field's ``expression`` genuinely varies per cell and ``resolution: chunk`` is a
    misconfiguration (the per-cell values would be silently dropped otherwise).
    """
    arr = np.asarray(values)
    # Reshape to (n_cells, *trailing): a scalar column is (n_cells,) -> trailing ().
    cells = arr.reshape(arr.shape[0], -1) if arr.ndim > 1 else arr.reshape(-1)
    trailing = arr.shape[1:]
    # Treat NaN as the empty/fill sentinel for float columns; integer columns have
    # no NaN, so every cell is "populated" and the uniformity check covers them all.
    # A vector cell is "empty" only when its whole vector is NaN (a partially-NaN
    # vector is a real chunk value, kept as-is).
    if np.issubdtype(arr.dtype, np.floating):
        if arr.ndim > 1:
            populated_mask = ~np.all(np.isnan(cells), axis=1)
        else:
            populated_mask = ~np.isnan(cells)
        populated = cells[populated_mask]
    else:
        populated = cells
    if populated.shape[0] == 0:
        # Whole chunk is fill (e.g. a vector-carrier shard with no chunk anchor):
        # nothing meaningful to record; fall back to the first cell's sentinel.
        return arr[0]
    first = populated[0]
    # Compare per-element over the trailing axis; for float columns NaN positions
    # must match too (so a NaN-bearing-but-uniform vector is accepted), which only
    # ``array_equal(equal_nan=True)`` allows — integer columns have no NaN.
    broadcast_first = np.broadcast_to(first, populated.shape)
    uniform = (
        np.array_equal(populated, broadcast_first, equal_nan=True)
        if np.issubdtype(arr.dtype, np.floating)
        else np.array_equal(populated, broadcast_first)
    )
    if not uniform:
        # Count distinct populated rows for the message. Map NaN to a sentinel
        # first so a uniform-but-NaN-bearing layout is not over-counted (NaN != NaN
        # would otherwise make every such row look distinct).
        keys = populated.reshape(populated.shape[0], -1)
        if np.issubdtype(arr.dtype, np.floating):
            keys = np.where(np.isnan(keys), np.float64(np.inf), keys.astype(np.float64))
        n_distinct = len({tuple(row) for row in keys})
        raise ValueError(
            f"resolution: chunk field {name!r} is not chunk-uniform: the populated "
            f"cells carry {n_distinct} distinct values; a chunk-resolution field must "
            f"reduce to one value per chunk (use resolution: cell for a per-cell field)"
        )
    return first.reshape(trailing) if trailing else first


def _iter_carrier_columns(carrier):
    """Yield ``(name, ndarray)`` for each column of a DataFrame or Arrow table.

    Scalar columns yield a 1-D array; a ``FixedSizeList<C>`` Arrow column yields a
    2-D ``(n_cells, C)`` array (the per-cell vector block), so the writer can map
    it onto the Zarr trailing payload dimension (issue #29).
    """
    if isinstance(carrier, pd.DataFrame):
        for name, series in carrier.items():
            values = series.values
            # The ``morton`` coordinate is carried as a mortie ``MortonIndexArray``
            # (#71); Zarr stores numpy dtypes, so extract its packed ``uint64``
            # words for the on-disk write (no .reshape on the extension array).
            if is_morton_array(values):
                values = morton_words(values)
            yield name, values
        return

    import pyarrow as pa

    n_rows = carrier.num_rows
    for name in carrier.column_names:
        col = carrier.column(name).combine_chunks()
        if pa.types.is_fixed_size_list(col.type):
            width = col.type.list_size
            flat = col.values.to_numpy(zero_copy_only=False)
            yield name, flat.reshape(n_rows, width)
        else:
            yield name, col.to_numpy(zero_copy_only=False)
