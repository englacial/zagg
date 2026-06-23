"""
Cloud-agnostic processing functions for aggregating HDF5 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.
"""

import logging
import warnings
from datetime import datetime
from typing import Any, List, Tuple

import h5coro
import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    default_config,
    evaluate_filter_expression,
    filters_from_data_source,
    get_agg_fields,
    get_chunk_precompute,
    get_data_vars,
    get_output_signature,
)
from zagg.grids.morton import is_morton_array, morton_words
from zagg.read_plan import execute_read_plan, plan_read
from zagg.schema import ProcessingMetadata

logger = logging.getLogger(__name__)


def _make_url_rewriter(driver: str | None):
    """Return a function that converts a granule URL for the active h5coro driver.

    The ShardMap carries the driver-appropriate href already (S3 vs HTTPS is
    chosen at dispatch), so this only strips the ``s3://`` scheme for the S3
    driver (h5coro's S3Driver expects ``bucket/key``); HTTPS is used as-is.
    """
    if driver == "https":
        return lambda url: url
    return lambda url: url.replace("s3://", "", 1)


def _field_sentinel(meta: dict) -> float:
    """Per-cell fill value for an agg field's empty/unused slots.

    Mirrors how ``process_shard`` / :func:`_kernel_aggregate` seed their output
    arrays: the schema-declared ``fill_value`` (default ``"NaN"`` -> ``np.nan``,
    else the literal numeric fill). Used both for scalar empty cells and for the
    padding of ``vector`` fields (issue #29 Option B).
    """
    fill_value = meta.get("fill_value", "NaN")
    return np.nan if fill_value == "NaN" else fill_value


def _group_columns(
    col_dict: dict[str, np.ndarray],
    cell_col: np.ndarray,
) -> tuple[dict[str, np.ndarray], dict[int, tuple[int, int]]]:
    """Sort column arrays by cell id; return reordered arrays and per-cell slice map.

    Carrier-agnostic core shared by the pandas and Arrow handoff paths. ``col_dict``
    is a plain ``name -> ndarray`` mapping (extracted from a DataFrame or an Arrow
    table); the math below is identical regardless of carrier, so both paths produce
    byte-for-byte identical groupings and aggregations.

    O(n log n) replacement for the O(n_children x n_obs) boolean-mask loop. The
    returned arrays are sorted (stably) by ascending cell id; each cell's
    observations form a contiguous slice, so ``col_arrays[col][start:end]`` is a
    view.
    """
    sort_idx = np.argsort(cell_col, kind="stable")
    sorted_cells = cell_col[sort_idx]
    col_arrays = {col: arr[sort_idx] for col, arr in col_dict.items()}
    if len(sorted_cells) == 0:
        return col_arrays, {}
    boundaries = np.flatnonzero(np.diff(sorted_cells)) + 1
    starts = np.concatenate([[0], boundaries])
    ends = np.concatenate([boundaries, [len(sorted_cells)]])
    cell_to_slice = {int(sorted_cells[s]): (int(s), int(e)) for s, e in zip(starts, ends)}
    return col_arrays, cell_to_slice


def _build_groups(
    df_all: pd.DataFrame,
    cell_col: np.ndarray,
) -> tuple[dict[str, np.ndarray], dict[int, tuple[int, int]]]:
    """Sort observations by cell id; return reordered column arrays and per-cell slice map.

    Pandas carrier wrapper over :func:`_group_columns` (extracts ``.values`` once).

    Parameters
    ----------
    df_all : pd.DataFrame
        Combined observation DataFrame (all beams / granules for this shard).
    cell_col : np.ndarray
        Cell id for each row in df_all (from ``grid.cells_of``).

    Returns
    -------
    col_arrays : dict[str, np.ndarray]
        Column arrays from df_all, sorted in ascending cell-id order.
    cell_to_slice : dict[int, tuple[int, int]]
        Maps each observed cell id to ``(start, end)`` indices into col_arrays.
    """
    col_dict = {col: df_all[col].values for col in df_all.columns}
    return _group_columns(col_dict, cell_col)


def _concat_and_group(all_reads, grid, handoff: str):
    """Concat the per-group reads and split observations by cell.

    Carrier-agnostic seam shared by :func:`process_shard` and its tests, so the
    Arrow path is exercised end-to-end (including multi-table ``concat_tables``
    ordering) rather than re-assembled inline. Both carriers feed identical numpy
    arrays into :func:`_group_columns`, so the groupings — and the aggregations
    computed from them — are byte-for-byte identical.

    Parameters
    ----------
    all_reads : list
        Per-group reads from ``_read_group``: ``pandas.DataFrame`` for the pandas
        carrier, ``pyarrow.Table`` for the arrow carrier.
    grid : OutputGrid
        Provides ``cells_of`` to map leaf ids to child cell ids.
    handoff : {"pandas", "arrow"}
        Which carrier ``all_reads`` holds.

    Returns
    -------
    col_arrays : dict[str, np.ndarray]
        Column arrays sorted in ascending cell-id order.
    cell_to_slice : dict[int, tuple[int, int]]
        Maps each observed cell id to ``(start, end)`` into ``col_arrays``.
    n_obs_total : int
        Total observation count across all reads.
    """
    if handoff == "arrow":
        import pyarrow as pa

        table = pa.concat_tables(all_reads).combine_chunks()
        # The arrow handoff requires dense, null-free columns: ``_read_group``
        # builds tables from raw h5coro reads (no null mask), so
        # ``to_numpy(zero_copy_only=False)`` is dtype-exact and matches ``.values``
        # on the pandas side. Guard the precondition so a future nullable source
        # can't silently diverge the two carriers instead of failing loudly.
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        cell_col = grid.cells_of(table.column("leaf_id").to_numpy(zero_copy_only=False))
        col_dict = {n: table.column(n).to_numpy(zero_copy_only=False) for n in table.column_names}
        col_arrays, cell_to_slice = _group_columns(col_dict, cell_col)
    else:
        df_all = pd.concat(all_reads, ignore_index=True)
        n_obs_total = len(df_all)
        cell_col = grid.cells_of(df_all["leaf_id"].values)
        col_arrays, cell_to_slice = _build_groups(df_all, cell_col)
    return col_arrays, cell_to_slice, n_obs_total


def _eval_chunk_precompute(config: PipelineConfig, pooled: dict[str, np.ndarray]) -> dict[str, Any]:
    """Evaluate the ``chunk_precompute`` entries ONCE over a shard's pooled columns.

    The per-chunk precompute hook (issue #30, items 1+2) is the "compute once per
    chunk, use per cell" primitive: each named entry is evaluated a single time
    over the shard's *pooled* column arrays (all beams/granules concatenated,
    before the per-cell split), yielding a chunk-level value. Those values are
    then injected into the per-cell expression namespace by :func:`process_shard`
    so a per-cell ``expression`` (e.g. a 128-bin waveform window) can reference a
    chunk-uniform anchor instead of recomputing a per-cell one.

    Evaluation follows :func:`calculate_cell_statistics`'s expression/function
    dispatch: an ``expression`` entry runs through ``_eval_expression_raw`` over
    the pooled columns; a ``function`` entry resolves via ``resolve_function`` and
    is applied to the entry's ``source`` column (with ``params`` resolved the same
    way as agg fields). The optional ``dtype`` casts the result.

    The result is **shape-agnostic**: a chunk value may be a scalar OR a
    non-scalar array (e.g. a covariance matrix), since the namespace-injection
    mechanism is shape-blind — a per-cell ``expression`` can reference a chunk
    array just like a chunk scalar (issue #30, @espg's 4773649308). Scalar-ness is
    only required when a chunk value is *written* to a ``kind: scalar`` output
    field; that is enforced in :func:`calculate_cell_statistics` (a non-scalar into
    a scalar field raises a clear error), not here.

    It deliberately diverges from the per-cell path in two ways: (1) there is no
    ``n_obs == 0`` short-circuit (these are shard-level reductions, evaluated once
    over the pooled columns), and no ``len``/``count`` short-circuit (a
    ``function: len`` precompute returns the pooled column length, not a cell
    ``n_obs``); (2) entries are evaluated independently over ``pooled`` only, with
    no defined order, so one entry cannot reference another's scalar (validation
    rejects inter-precompute references — see ``_validate_chunk_precompute``).

    Returns an empty dict when no ``chunk_precompute`` block is present, so the
    per-cell path is byte-for-byte unchanged for configs that do not use the hook.

    Parameters
    ----------
    config : PipelineConfig
        Drives the ``chunk_precompute`` entries.
    pooled : dict[str, np.ndarray]
        Pooled column arrays for the whole shard (e.g. ``col_arrays`` from
        :func:`_concat_and_group`). Order does not matter — these are chunk-level
        reductions over the full shard.

    Returns
    -------
    dict[str, object]
        ``{name: value}`` for each ``chunk_precompute`` entry (scalar or array).
    """
    from zagg.config import _eval_expression_raw, resolve_function

    entries = get_chunk_precompute(config)
    if not entries:
        return {}

    out: dict[str, Any] = {}
    for name, meta in entries.items():
        expression = meta.get("expression")
        if expression is not None:
            value = _eval_expression_raw(expression, pooled)
        else:
            source = meta["source"]
            if source not in pooled:
                # The pooled dict only carries columns that were actually read for
                # this shard; a validated config can still hit this if a read path
                # omits the source. Raise a clear error rather than a bare KeyError.
                raise ValueError(
                    f"chunk_precompute '{name}': source column {source!r} is not "
                    f"present in the shard's pooled data (available: {sorted(pooled)})"
                )
            values = pooled[source]
            params = dict(meta.get("params", {}))
            resolved_params = {}
            for pkey, pval in params.items():
                if isinstance(pval, str) and pval in pooled:
                    resolved_params[pkey] = pooled[pval]
                elif isinstance(pval, str) and any(c in pval for c in pooled):
                    ns = {"__builtins__": {}, "np": np, "numpy": np, **pooled}
                    resolved_params[pkey] = eval(pval, ns)  # noqa: S307
                else:
                    resolved_params[pkey] = pval
            value = resolve_function(meta["function"])(values, **resolved_params)
        # Shape-agnostic: a chunk value may be a scalar or a non-scalar array (e.g.
        # a covariance matrix) — both inject cleanly into the per-cell namespace and
        # can feed any per-cell ``expression`` (issue #30). Scalar-ness is required
        # only when a chunk value is written to a ``kind: scalar`` field, which is
        # enforced at that write point in ``calculate_cell_statistics``. The dtype
        # cast applies element-wise to either a scalar or an array.
        dtype = meta.get("dtype")
        if dtype is not None:
            np_dtype = np.dtype(dtype)
            value = (
                np_dtype.type(value) if np.ndim(value) == 0 else np.asarray(value, dtype=np_dtype)
            )
        out[name] = value
    return out


def _has_vector_fields(config: PipelineConfig) -> bool:
    """Whether any aggregation field declares a non-scalar (``vector``) output.

    A pure-scalar config keeps the unchanged pandas carrier; any ``vector`` field
    (issue #29) routes the whole cell->table handoff through Arrow (see
    :func:`_arrow_column`).
    """
    return any(
        get_output_signature(meta)["kind"] == "vector" for meta in get_agg_fields(config).values()
    )


def _has_ragged_fields(config: PipelineConfig) -> bool:
    """Whether any aggregation field declares a ``ragged`` (CSR) output.

    Ragged fields (issue #48) carry variable-length per-cell payloads and are
    collected separately from scalar/vector fields; they are written via the CSR
    writer rather than the dense Zarr path.
    """
    return any(
        get_output_signature(meta)["kind"] == "ragged" for meta in get_agg_fields(config).values()
    )


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


def calculate_cell_statistics(
    cell_data: dict[str, Any],
    value_col: str = "h_li",
    sigma_col: str = "s_li",
    config: PipelineConfig | None = None,
) -> dict:
    """
    Calculate summary statistics for a cell, driven by pipeline config metadata.

    User contract
    -------------
    The supported aggregation surface is *anything expressible in numpy*. Each
    agg field names a ``function`` that :func:`zagg.config.resolve_function`
    turns into a callable: a bare name (``"min"``, ``"nanmean"``) resolves to
    ``np.<name>`` via ``getattr(np, ...)``, an ``"np."``-prefixed name the same
    way, and a dotted path (``"numpy.quantile"``) via import. This means the full
    numpy **NaN-aware family** — ``np.nanmean``, ``np.nanvar``, ``np.nanmax``,
    ``np.nanmin``, ``np.nansum``, ``np.nanstd``, ``np.nanmedian``, … — is
    usable directly from the config template with no special-casing, and is
    reduced with numpy's own NaN semantics (see ``test_numpy_nan_aware_functions``).
    The experimental ``handoff="arrow-kernel"`` path is an *opt-in* acceleration
    for the kernel-able subset only; it does not change or narrow this contract
    (see the EXPERIMENTAL block below).

    Parameters
    ----------
    cell_data : dict[str, Any]
        Eval namespace for a single cell. Keys are column names; values are
        numpy arrays of equal length. May also carry chunk-level scalars injected
        by the per-chunk precompute hook (issue #30), which a per-cell expression
        can reference by name.
    value_col : str
        Column name for elevation values.
    sigma_col : str
        Column name for uncertainty values.
    config : PipelineConfig, optional
        Pipeline config to use for dispatch. Defaults to ``default_config()``.

    Returns
    -------
    dict
        Dictionary of statistics keyed by aggregation variable name.
    """
    from zagg.config import _eval_expression_raw, resolve_function

    if config is None:
        config = default_config()
    agg_fields = get_agg_fields(config)

    # ``n_obs`` must count a real (length-bearing) observation column, not a 0-d
    # chunk-precompute scalar injected into the namespace (issue #30). Scalars have
    # no ``len``; skip them so an empty cell whose namespace carries only scalars
    # still reports n_obs == 0 rather than crashing on ``len`` of a 0-d value.
    n_obs = next(
        (len(v) for v in cell_data.values() if np.ndim(v) != 0),
        0,
    )
    if n_obs == 0:
        # Empty cell: every agg field gets its sentinel EXCEPT a field whose
        # ``expression`` is a bare chunk-precompute name. Those resolve to the
        # chunk-uniform scalar (well-defined for an empty cell), so the dense
        # writer's empty rows still carry the shared chunk anchor instead of NaN
        # (issue #30 — every cell in a chunk shares one anchor).
        empty = {}
        for name, meta in agg_fields.items():
            expr = meta.get("expression")
            if expr is not None:
                key = expr.strip()
                if key.isidentifier() and key in cell_data and np.ndim(cell_data[key]) == 0:
                    sig = get_output_signature(meta)
                    if sig["kind"] == "scalar":
                        empty[name] = float(cell_data[key])
                        continue
            empty[name] = _empty_cell_value(meta)
        return empty

    result = {}
    for name, meta in agg_fields.items():
        func_name = meta.get("function")
        expression = meta.get("expression")
        source = meta.get("source") or value_col
        params = dict(meta.get("params", {}))
        sig = get_output_signature(meta)

        # Expression-based aggregation (e.g. h_sigma). A scalar expression casts
        # to a Python float; a ``kind: vector`` expression is coerced through the
        # same ``_coerce_field_value``/``trailing_shape``/dtype path as a vector
        # ``function`` field (issue #29). A ``kind: ragged`` expression (issue #48)
        # returns the raw result as a numpy array — the CSR writer receives it as
        # a variable-length per-cell payload.
        if expression:
            if sig["kind"] == "vector":
                out = _eval_expression_raw(expression, cell_data)
                result[name] = _coerce_field_value(out, sig)
            elif sig["kind"] == "ragged":
                out = _eval_expression_raw(expression, cell_data)
                result[name] = _coerce_ragged_value(out, sig)
            else:
                # kind: scalar — the expression must reduce to a single value. A
                # non-scalar chunk_precompute value (issue #30 allows arrays in the
                # namespace) written to a scalar field is a config error; raise a
                # clear message rather than letting ``float()`` emit a cryptic one.
                out = _eval_expression_raw(expression, cell_data)
                if np.ndim(out) != 0:
                    raise ValueError(
                        f"scalar field {name!r}: expression {expression!r} produced a "
                        f"non-scalar of shape {np.shape(out)}; a kind: scalar field "
                        f"requires a scalar result (declare 'kind: vector' to store an "
                        f"array per cell)"
                    )
                result[name] = float(out)
            continue

        values = cell_data[source]

        # Count via len
        if func_name in ("len", "count"):
            result[name] = n_obs
            continue

        # Resolve params: bare column name -> array, expression -> eval'd
        resolved_params = {}
        for pkey, pval in params.items():
            if isinstance(pval, str) and pval in cell_data:
                resolved_params[pkey] = cell_data[pval]
            elif isinstance(pval, str) and any(c in pval for c in cell_data):
                ns = {
                    "__builtins__": {},
                    "np": np,
                    "numpy": np,
                    **cell_data,
                }
                resolved_params[pkey] = eval(pval, ns)  # noqa: S307
            else:
                resolved_params[pkey] = pval

        func = resolve_function(func_name)
        out = func(values, **resolved_params)
        # Scalar fields stay byte-for-byte identical to the pre-#29 path; a
        # declared ``vector`` field coerces to its trailing_shape (issue #29); a
        # ``ragged`` field (issue #48) returns a variable-length numpy array that
        # the CSR writer later packs into flat + offsets + cell_ids arrays.
        if sig["kind"] == "vector":
            result[name] = _coerce_field_value(out, sig)
        elif sig["kind"] == "ragged":
            result[name] = _coerce_ragged_value(out, sig)
        else:
            result[name] = float(out)

    return result


def _empty_cell_value(meta: dict):
    """Value emitted for a single agg field when its cell has no observations.

    Scalar fields keep the pre-#29 contract: ``0`` for ``len``/``count``,
    ``np.nan`` otherwise. A ``vector`` field (issue #29) instead gets a full
    ``trailing_shape`` array filled with its schema-declared sentinel
    (:func:`_field_sentinel`), so empty and populated cells emit the same shape.
    A ``ragged`` field (issue #48) returns an empty list ``[]`` — the CSR writer
    handles absent cells by leaving them out of ``cell_ids``.
    """
    sig = get_output_signature(meta)
    if sig["kind"] == "ragged":
        return []
    if sig["kind"] == "vector":
        dtype = np.dtype(sig["dtype"]) if sig["dtype"] is not None else np.dtype("float32")
        return np.full(sig["trailing_shape"], _field_sentinel(meta), dtype=dtype)
    return 0 if meta.get("function") in ("len", "count") else np.nan


def _coerce_field_value(value, sig: dict) -> np.ndarray:
    """Coerce a ``vector`` field's aggregation output to its declared signature.

    The field's ``function`` or ``expression`` must yield exactly
    ``trailing_shape`` values (issue #29 Tier-1 fixed-width vectors; ragged/CSR
    is Tier 2). Returns a contiguous array of the declared dtype (default
    ``float32``), so every cell emits an identically-shaped slab the dense
    writer (phase 5) can stack.
    """
    dtype = np.dtype(sig["dtype"]) if sig["dtype"] is not None else np.dtype("float32")
    arr = np.asarray(value, dtype=dtype)
    if arr.shape != sig["trailing_shape"]:
        raise ValueError(
            f"vector field produced shape {arr.shape}, expected {sig['trailing_shape']}"
        )
    return arr


def _coerce_ragged_value(value, sig: dict) -> np.ndarray:
    """Coerce a ``ragged`` field's aggregation output to a 2-D numpy array.

    A ragged field (issue #48) emits a variable-length array of shape
    ``(n_elements, *inner_shape)`` per cell. This function verifies the inner
    dimensions match the declared ``inner_shape`` and returns a contiguous
    array of the declared dtype (default ``float32``), ready for the CSR writer.

    Parameters
    ----------
    value : array-like
        The raw result from the field's function or expression.
    sig : dict
        Output signature from :func:`zagg.config.get_output_signature`.

    Returns
    -------
    np.ndarray
        Shape ``(n_elements, *inner_shape)``, or ``(0, *inner_shape)`` when
        ``value`` is empty.
    """
    dtype = np.dtype(sig["dtype"]) if sig["dtype"] is not None else np.dtype("float32")
    inner = sig["inner_shape"]
    arr = np.asarray(value, dtype=dtype)
    if arr.size == 0:
        return np.empty((0, *inner), dtype=dtype)
    # Accept a 1-D array when inner_shape has one dimension: reshape to (n, d).
    if arr.ndim == 1 and len(inner) == 1:
        arr = arr.reshape(-1, *inner)
    if arr.ndim != len(inner) + 1 or arr.shape[1:] != inner:
        raise ValueError(f"ragged field produced inner shape {arr.shape[1:]}, expected {inner}")
    return np.ascontiguousarray(arr)


# EXPERIMENTAL (phase 2b of #30) -----------------------------------------------
# Dual aggregation contract
# -------------------------
# The DEFAULT, fully-supported contract is "any aggregation expressible in numpy",
# including the NaN-aware family (``np.nanmean``/``np.nanvar``/``np.nanmax``/…);
# the user picks the function in the agg template and it runs through
# ``calculate_cell_statistics`` with numpy's own semantics (see that docstring and
# ``test_numpy_nan_aware_functions``). Arrow kernels do NOT replace or narrow that
# contract — they are an OPT-IN acceleration for the kernel-able subset, and the
# user chooses numpy vs arrow per run via the ``handoff`` flag.
#
# Why arrow kernels aren't drop-in nan-operators: pyarrow compute has
# ``mean``/``min_max``/``variance`` with ``skip_nulls``, but an Arrow NULL is a
# distinct missing-value bit, NOT a float NaN — ``skip_nulls`` does not skip NaN.
# So there is no arrow "nanmean" kernel equivalent; the kernel path instead
# replicates numpy's NaN behaviour by hand (NaN-propagating min/max, see below)
# rather than pretending arrow nulls and float NaN are the same thing.
#
# Optional pyarrow.compute hash-aggregate ("kernel") reduction path. Unlike the
# pandas/arrow *carriers* — which feed identical numpy arrays into
# ``calculate_cell_statistics`` and are therefore byte-for-byte identical — the
# kernel path computes the kernel-able reductions in a single vectorised C++
# pass (Acero ``TableGroupBy.aggregate``). pyarrow's float summation differs from
# numpy's, so its ``mean``/``variance`` outputs are NOT byte-identical to the
# numpy path; they agree only within ``KERNEL_RTOL`` (validated in tests and in
# ``benchmarks/handoff_bench.py``). ``count``/``min``/``max`` ARE exact vs numpy,
# including on NaN input: pyarrow's ``min``/``max`` kernels skip NaN by default
# (numpy propagates it), so :func:`_kernel_aggregate` detects NaN per group and
# overwrites those groups' min/max with NaN to restore numpy parity (NaN is a
# value, not an Arrow null, so ``skip_nulls`` does not cover it). This lever is
# opt-in via ``handoff="arrow-kernel"`` and exists purely so phase 3 can benchmark
# it on real ATL03 data; it is kept gated and clearly experimental, and should be
# dropped if that benchmark shows no material speedup (see PR #33 discussion).

# Documented tolerance for kernel-vs-numpy float agreement. float32 means/variance
# over millions of obs diverge by ~1 ULP (~1e-6 relative); 1e-5 leaves headroom.
KERNEL_RTOL = 1e-5

# numpy/config ``function`` name -> pyarrow hash-aggregate function name. Only
# reductions that are mathematically a pure (unweighted) group reduction appear
# here; weighted ``average``, ``quantile`` (only approximate via tdigest) and any
# ``expression`` field fall back to the per-cell numpy path.
# NOTE: ``"average" -> "mean"`` is currently dead for the shipped atl06 config
# (its ``h_mean`` is a *weighted* average, which ``_kernel_able`` excludes). It is
# kept only so an unweighted ``average`` field — if a future config defines one —
# is kernel-able rather than silently falling back; remove it if that never lands.
_KERNEL_FUNCS = {
    "len": "count",
    "count": "count",
    "min": "min",
    "max": "max",
    "var": "variance",
    "average": "mean",
}


def _kernel_able(meta: dict) -> bool:
    """Whether an agg field can be computed by a pyarrow hash-aggregate kernel.

    EXPERIMENTAL. Excludes expression fields, weighted ``average`` (no weighted
    hash kernel), quantiles (only approximate tdigest), and anything whose
    function has no pure-reduction kernel equivalent.
    """
    if meta.get("expression"):
        return False
    func = meta.get("function")
    if func not in _KERNEL_FUNCS:
        return False
    # Weighted average is not a pure hash reduction.
    if func == "average" and "weights" in (meta.get("params") or {}):
        return False
    return True


def _kernel_aggregate(
    table,
    cell_col: np.ndarray,
    children,
    value_col: str,
    config: PipelineConfig,
    chunk_scalars: dict[str, Any] | None = None,
) -> dict:
    """EXPERIMENTAL pyarrow hash-aggregate reducer (phase 2b of #30).

    Computes the kernel-able stats (count/min/max/variance/unweighted-mean) for
    every child cell in one vectorised ``TableGroupBy.aggregate`` pass, then fills
    the remaining (weighted mean, expression, quantile) fields via the per-cell
    numpy path so output columns match the default reducer exactly in shape.

    ``chunk_scalars`` (issue #30) are the per-chunk precompute values, injected
    into each cell's namespace in the fallback per-cell loop exactly as the default
    handoff does, so an ``expression`` field referencing a chunk anchor resolves on
    the arrow path too. A precompute field is never kernel-able (it is an
    ``expression``), so it always lands in ``fallback_names`` and sees the scalars.

    ``count``/``min``/``max`` are EXACT vs the numpy reducer, including on NaN
    input — pyarrow's min/max kernels skip NaN, so this function detects NaN per
    group and propagates it (numpy semantics). The kernel-reduced float stats
    (``mean``/``variance``) are NOT byte-identical to numpy; they agree within
    :data:`KERNEL_RTOL` (and both yield NaN on a NaN-bearing group). Returns
    ``stats_arrays`` (``name -> ndarray`` over ``children``) plus
    ``cells_with_data``.

    Parameters
    ----------
    table : pyarrow.Table
        Concatenated, null-free observations (one row per observation). "Null-free"
        is the Arrow-null sense; float NaN values ARE allowed and are handled with
        numpy semantics (see above). Callers must enforce the null-free contract
        (``process_shard`` does); this function does not re-check it.
    cell_col : np.ndarray
        Child cell id for each row of ``table`` (already ``grid.cells_of`` mapped,
        so the group key is the destination cell, not the leaf id).
    children : sequence of int
        Child cell ids, in canonical chunk order.
    value_col : str
        Default value column for fields without an explicit ``source``.
    config : PipelineConfig
        Drives the agg-field metadata.
    """
    import pyarrow as pa

    agg_fields = get_agg_fields(config)
    data_vars = get_data_vars(config)
    n_cells = len(children)
    child_index = {int(c): i for i, c in enumerate(children)}

    stats_arrays: dict[str, np.ndarray] = {}
    for name in data_vars:
        meta = agg_fields[name]
        # Ragged fields (issue #48) cannot be dense-preallocated; skip them here.
        # The kernel path does not support ragged — they have no hash-aggregate
        # kernel equivalent — so a config mixing ragged + arrow-kernel uses the
        # fallback for ragged fields. But the dense fallback array assignment
        # (``stats_arrays[name][i] = value``) would crash for a list payload.
        # Exclude ragged from both the kernel and fallback lists; the caller is
        # responsible for collecting ragged payloads via process_shard's own loop.
        if get_output_signature(meta)["kind"] == "ragged":
            continue
        zarr_dtype = np.dtype(meta.get("dtype", "float32"))
        fill_value = meta.get("fill_value", "NaN")
        if fill_value == "NaN":
            stats_arrays[name] = np.full(n_cells, np.nan, dtype=zarr_dtype)
        else:
            stats_arrays[name] = np.zeros(n_cells, dtype=zarr_dtype)

    # Ragged fields are excluded from kernel and fallback (see above).
    dense_names = [n for n in data_vars if get_output_signature(agg_fields[n])["kind"] != "ragged"]
    kernel_names = [n for n in dense_names if _kernel_able(agg_fields[n])]
    fallback_names = [n for n in dense_names if n not in kernel_names]

    # Group by the destination cell id (not the raw leaf id): append cell_col and
    # run one vectorised group-by + reduction pass for all kernel-able fields.
    keyed = table.append_column("_cell", pa.array(np.asarray(cell_col)))
    aggregations = [
        (agg_fields[n].get("source") or value_col, _KERNEL_FUNCS[agg_fields[n]["function"]])
        for n in kernel_names
    ]
    # NaN semantics: pyarrow's ``min``/``max`` hash kernels SKIP NaN, whereas
    # ``np.min``/``np.max`` PROPAGATE it. To keep count/min/max bit-identical to the
    # numpy path on NaN-bearing input (ATL06 ``h_li`` can carry fill/invalid values
    # and ``quality_filter`` is a flag check, not a NaN filter), detect NaN per
    # group on each min/max source column and overwrite those groups' min/max with
    # NaN below. (``count`` already matches: NaN is a value, not a null, so it is
    # counted; ``mean``/``variance`` already propagate NaN like numpy.)
    extrema_srcs = {
        src for n, (src, kfunc) in zip(kernel_names, aggregations) if kfunc in ("min", "max")
    }
    for src in extrema_srcs:
        is_nan = np.isnan(table.column(src).to_numpy(zero_copy_only=False))
        keyed = keyed.append_column(f"_isnan_{src}", pa.array(is_nan))
    aggregations_nan = [(f"_isnan_{src}", "max") for src in extrema_srcs]
    gd = keyed.group_by("_cell").aggregate(aggregations + aggregations_nan).to_pydict()
    group_cells = gd["_cell"]
    group_has_nan = {src: gd[f"_isnan_{src}_max"] for src in extrema_srcs}
    # Map each grouped row back to its position in ``children``.
    row_to_idx = [child_index.get(int(c)) for c in group_cells]
    for n, (src, kfunc) in zip(kernel_names, aggregations):
        col = gd[f"{src}_{kfunc}"]
        nan_flags = group_has_nan.get(src) if kfunc in ("min", "max") else None
        out = stats_arrays[n]
        for row, idx in enumerate(row_to_idx):
            if idx is not None:
                # Propagate NaN for min/max to match numpy (pyarrow skips NaN).
                out[idx] = np.nan if (nan_flags is not None and nan_flags[row]) else col[row]

    cells_with_data = sum(1 for idx in row_to_idx if idx is not None)

    # Fallback fields (and only those) via the per-cell numpy reducer. Reuse the
    # carrier-agnostic grouping so the slices match the default path exactly.
    # NOTE: ``calculate_cell_statistics`` recomputes the *full* stats dict per cell,
    # so the kernel-able stats are computed a second time here and discarded — we
    # only read ``fallback_names`` out of it. Acceptable while experimental (the
    # fallback set is small: weighted mean, expression, quantiles); revisit if a
    # config makes the fallback set dominate.
    if fallback_names:
        col_dict = {
            name: table.column(name).to_numpy(zero_copy_only=False) for name in table.column_names
        }
        col_arrays, cell_to_slice = _group_columns(col_dict, np.asarray(cell_col))
        _empty = {col: arr[:0] for col, arr in col_arrays.items()}
        for i, child in enumerate(children):
            child = int(child)
            if child in cell_to_slice:
                s, e = cell_to_slice[child]
                cell_data = {col: arr[s:e] for col, arr in col_arrays.items()}
            else:
                cell_data = _empty
            # Inject the chunk-level precompute values (no-op when empty), so an
            # expression fallback field can reference a chunk anchor (issue #30).
            cell_namespace = {**cell_data, **chunk_scalars} if chunk_scalars else cell_data
            stats = calculate_cell_statistics(cell_namespace, value_col=value_col, config=config)
            for name in fallback_names:
                stats_arrays[name][i] = stats[name]

    return {"stats_arrays": stats_arrays, "cells_with_data": cells_with_data}


# -- end EXPERIMENTAL kernel path ---------------------------------------------


_COMPARE = {
    "eq": np.equal,
    "ne": np.not_equal,
    "ge": np.greater_equal,
    "le": np.less_equal,
    "lt": np.less,
    "gt": np.greater,
}


def _expand_mask_to_base(
    coarse_mask: np.ndarray,
    index_beg_arr: np.ndarray,
    count_arr: np.ndarray,
    index_base: int,
    total_base_size: int,
) -> np.ndarray:
    """Expand a coarse-rate boolean mask to a base-rate boolean mask (issue #43, Phase B).

    Each coarse parent ``p`` covers base-rate rows
    ``index_beg_arr[p] - index_base, ..., index_beg_arr[p] - index_base + count_arr[p] - 1``.
    The contiguity assumption: ranges do not overlap and together tile the full base array.

    Parameters
    ----------
    coarse_mask : np.ndarray
        1-D boolean array of length ``n_parents``.
    index_beg_arr : np.ndarray
        Per-parent start index into the base array (before ``index_base`` shift).
    count_arr : np.ndarray
        Per-parent child count (number of base-rate rows this parent covers).
    index_base : int
        Subtracted from ``index_beg_arr`` to get 0-based base indices.
    total_base_size : int
        Length of the output base-rate array.

    Returns
    -------
    np.ndarray
        1-D boolean array of length ``total_base_size``.
    """
    out = np.zeros(total_base_size, dtype=bool)
    for p, keep in enumerate(coarse_mask):
        if not keep:
            continue
        beg = int(index_beg_arr[p]) - index_base
        if beg < 0:
            raise ValueError(
                f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
            )
        cnt = int(count_arr[p])
        out[beg : beg + cnt] = True
    return out


def _broadcast_segment_to_base(
    seg_values: np.ndarray,
    index_beg_arr: np.ndarray,
    count_arr: np.ndarray,
    index_base: int,
    total_base_size: int,
) -> np.ndarray:
    """Broadcast a per-segment variable to a base-rate (per-photon) array (issue #30).

    Each coarse parent ``p`` covers base-rate rows ``index_beg_arr[p] - index_base``
    through ``... + count_arr[p] - 1`` (the same contiguous parent->child tiling
    :func:`_expand_mask_to_base` expands a mask over). Under #43's contiguity
    assumption (ranges do not overlap and together tile the full base array) this
    equals ``np.repeat(seg_values, count_arr)``; placing by ``index_beg`` keeps the
    per-parent value correctly positioned even when ``index_beg`` is shifted
    (``index_base``). Any base row left untiled (a gap, if the contiguity assumption
    is violated) is filled with ``NaN`` for float dtypes so it surfaces as a missing
    value rather than uninitialized garbage; non-float dtypes are zero-filled. The
    returned array carries each photon's segment value (e.g. ``dem_h``, one value per
    ~100 photons) so it can ride alongside the base-rate variables through the read
    plan's spatial/keep masks.

    Parameters
    ----------
    seg_values : np.ndarray
        1-D per-parent values of length ``n_parents``.
    index_beg_arr, count_arr, index_base, total_base_size
        As in :func:`_expand_mask_to_base`.

    Returns
    -------
    np.ndarray
        1-D array of length ``total_base_size`` and ``seg_values``' dtype.

    Raises
    ------
    ValueError
        If a parent's range starts before 0 or extends past ``total_base_size``
        (a tiling that does not fit the declared base size — e.g. a segment-level
        variable on a level whose link does not match the read's base extent).
    """
    # NaN-fill floats so an untiled gap reads as missing, not garbage (the mask path
    # is safe-by-construction with np.zeros; a value array has no such safe default).
    if np.issubdtype(seg_values.dtype, np.floating):
        out = np.full(total_base_size, np.nan, dtype=seg_values.dtype)
    else:
        out = np.zeros(total_base_size, dtype=seg_values.dtype)
    for p in range(len(seg_values)):
        beg = int(index_beg_arr[p]) - index_base
        if beg < 0:
            raise ValueError(
                f"index_beg_arr[{p}]={index_beg_arr[p]} is less than index_base={index_base}"
            )
        cnt = int(count_arr[p])
        if beg + cnt > total_base_size:
            raise ValueError(
                f"segment {p} range [{beg}:{beg + cnt}] exceeds base size {total_base_size}; "
                f"the segment-level variable's link does not tile the read's base extent"
            )
        out[beg : beg + cnt] = seg_values[p]
    return out


def _segment_level_variables(data_source: dict) -> dict[str, dict[str, str]]:
    """Collect declared segment-level (non-base) readable variables (issue #30).

    A non-base level may declare ``variables`` as a ``{name: path-template}``
    mapping (the readable form, distinct from the documentation-only ``list[str]``
    form). Each such variable is read at coarse rate and broadcast to the base
    (photon) rows via the level's ``link`` (``_broadcast_segment_to_base``), so a
    per-segment field like ``dem_h`` becomes a per-photon column the aggregation /
    ``chunk_precompute`` can reduce. Returns ``{level_key: {name: template}}`` for
    every non-base level carrying a dict ``variables``; empty when none do, so the
    read path is unchanged for configs without it.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or base_level is None:
        return {}
    out: dict[str, dict[str, str]] = {}
    for name, lvl in levels.items():
        if name == base_level or not isinstance(lvl, dict):
            continue
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict) and lvl_vars:
            out[name] = dict(lvl_vars)
    return out


def _read_segment_broadcasts(
    h5obj, group: str, data_source: dict, levels: dict, n_base: int
) -> dict[str, np.ndarray]:
    """Read each segment-level variable and broadcast it to a base-rate column (issue #30).

    For every non-base level carrying a ``{name: path}`` ``variables`` mapping, read
    the variable and the level's link arrays at coarse rate, then broadcast to the
    base (photon) rows via :func:`_broadcast_segment_to_base`. Returns
    ``{name: base_rate_array}`` (length ``n_base``), ready to be sliced through the
    same spatial / keep masks the base-rate variables are. A variable name colliding
    with a ``data_source.variables`` column is rejected (it would shadow the read).
    """
    seg_vars = _segment_level_variables(data_source)
    if not seg_vars:
        return {}
    base_cols = set(data_source.get("variables", {}))
    out: dict[str, np.ndarray] = {}
    for level_key, mapping in seg_vars.items():
        lvl = levels[level_key]
        link = lvl["link"]
        index_base = int(link.get("index_base", 0))
        ibeg_path = link["index_beg"].format(group=group)
        cnt_path = link["count"].format(group=group)
        link_data = h5obj.readDatasets([ibeg_path, cnt_path])
        ibeg_arr = link_data[ibeg_path]
        cnt_arr = link_data[cnt_path]
        for col_name, tmpl in mapping.items():
            if col_name in base_cols:
                raise ValueError(
                    f"segment-level variable '{col_name}' on level '{level_key}' "
                    f"collides with a data_source.variables column"
                )
            seg_path = tmpl.format(group=group)
            seg_values = np.asarray(h5obj.readDatasets([seg_path])[seg_path])
            out[col_name] = _broadcast_segment_to_base(
                seg_values, ibeg_arr, cnt_arr, index_base, n_base
            )
    return out


def _predicate_mask(arr: np.ndarray, f: dict) -> np.ndarray:
    """Build a 1-D boolean keep-mask for one structured predicate (issue #43).

    ``f`` is a normalized structured filter (see :func:`zagg.config.get_filters`):
    ``{op, column, value|values, keep}``. An integer ``column`` selects a column
    from a 2-D flag array before comparing; it is required for N-D arrays and
    rejected for 1-D arrays. ``keep: false`` inverts the result (drop matches).
    """
    column = f.get("column")
    if arr.ndim > 1:
        if column is None:
            raise ValueError(f"filter on '{f['dataset']}': N-D array requires an integer 'column'")
        arr = arr[:, column]
    elif column is not None:
        raise ValueError(f"filter on '{f['dataset']}': 'column' set but array is 1-D")

    op = f["op"]
    if op == "in":
        mask = np.isin(arr, f["values"])
    elif op == "not_in":
        mask = ~np.isin(arr, f["values"])
    else:
        mask = _COMPARE[op](arr, f["value"])
    if not f.get("keep", True):
        mask = ~mask
    return mask


def _level_coord_paths(level: dict, group: str) -> tuple[str, str]:
    """Resolve ``(latitude, longitude)`` HDF5 paths for a coarse-level spatial index.

    The level's ``coordinates`` field is a ``{latitude, longitude}`` dict of names
    relative to the level's ``path`` template (matching the schema in #43's issue
    body). Both halves are required for the ``read_plan`` to compute an AOI box.
    """
    coords = level.get("coordinates")
    if not isinstance(coords, dict) or "latitude" not in coords or "longitude" not in coords:
        raise ValueError(
            "read_plan.spatial_index level requires "
            "'coordinates: {latitude: <name>, longitude: <name>}'"
        )
    base = level["path"].format(group=group).rstrip("/")
    lat_name = coords["latitude"]
    lon_name = coords["longitude"]
    # Allow either a relative name (joined to the level path) or an absolute path
    # template (already group-substituted on .format above? no -- coords names
    # don't carry templates; keep them simple). Absolute paths win as-is.
    lat_path = lat_name if lat_name.startswith("/") else f"{base}/{lat_name}"
    lon_path = lon_name if lon_name.startswith("/") else f"{base}/{lon_name}"
    return lat_path, lon_path


def _planned_read_group(
    h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
):
    """Planned (AOI-bounded) read of one HDF5 group via the coarse spatial index.

    Issue #43 Phase C: when ``data_source.read_plan.spatial_index`` names a coarse
    level whose ``link`` points at the base level, we read the coarse coordinates
    + link arrays once (small), call :func:`zagg.read_plan.plan_read` to compute
    which base-rate slices the AOI bbox actually touches, and read base-rate
    coords + variables + filter datasets only over those slices via
    :func:`zagg.read_plan.execute_read_plan`. This avoids the
    ``lat_ph`` + ``lon_ph`` full-coord read (up to ~245 MB per ATL03 beam) that
    drives Lambda OOMs (issue #43 motivation).

    Falls back transparently to :func:`_read_group` when:
    - the empty-AOI short-circuit fires (no parents match) → return ``None``;
    - ``plan_read`` flags ``full_read=True`` (selectivity above threshold);
    - the cell ``signal_conf_ph``-style 2-D structured filter would be re-read
      via the planned slices either way (the helper handles that uniformly).

    Returns the same ``pandas.DataFrame`` / ``pyarrow.Table`` / ``None`` contract
    as :func:`_read_group`. Output rows are in plan-slice / spatial-mask /
    filter order — which matches the full-read path's row ordering because the
    plan's runs are emitted in increasing parent index.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    levels = data_source["levels"]
    base_level_key = data_source["base_level"]
    rp = data_source["read_plan"]
    spatial_index_level = rp["spatial_index"]
    pad = int(rp.get("pad", 1))
    full_read_threshold = float(rp.get("full_read_threshold", 0.9))

    si_lvl = levels[spatial_index_level]
    link = si_lvl.get("link")
    if not isinstance(link, dict):
        raise ValueError(f"read_plan.spatial_index level {spatial_index_level!r} requires a 'link'")
    if link["to"] != base_level_key:
        raise ValueError(
            f"read_plan.spatial_index level {spatial_index_level!r} must link "
            f"directly to base level {base_level_key!r} (got link.to={link['to']!r})"
        )
    index_base = int(link.get("index_base", 0))

    # Read coarse-level coordinates + link arrays in one go (small — geolocation
    # rate is ~30x lighter than photon rate on ATL03).
    si_lat_path, si_lon_path = _level_coord_paths(si_lvl, group)
    ibeg_path = link["index_beg"].format(group=group)
    cnt_path = link["count"].format(group=group)
    coarse_data = h5obj.readDatasets([si_lat_path, si_lon_path, ibeg_path, cnt_path])
    coarse_lats = coarse_data[si_lat_path]
    coarse_lons = coarse_data[si_lon_path]
    ibeg_arr = coarse_data[ibeg_path]
    cnt_arr = coarse_data[cnt_path]

    if len(coarse_lats) == 0:
        return None

    # ``n_base`` under #43's contiguity assumption ("ranges do not overlap and
    # together tile the full base array" -- :func:`_expand_mask_to_base`).
    # ``int(cnt_arr.sum())`` makes the assumption explicit and is identical to
    # ``ibeg_arr[-1] - index_base + cnt_arr[-1]`` when contiguity holds. If a
    # future granule format drops trailing photons or gaps between parents,
    # either form under- or over-estimates -- track via a follow-up to #43.
    n_base = int(np.asarray(cnt_arr).sum())
    if n_base <= 0:
        return None

    # Compute the shard's WGS84 bbox from the grid (every grid's
    # ``shard_footprint`` returns a shapely ``Polygon`` or ``MultiPolygon``).
    # An antimeridian-crossing HEALPix shard's footprint can come back as a
    # split ``MultiPolygon`` (see ``zagg.viz.shardmap._split_antimeridian``),
    # in which case ``.bounds`` spans ~360 deg in lon and would neutralize
    # the IO bound (the AOI would intersect every segment). Same for
    # globe-spanning polar caps. Detect the wide-bbox case up front and fall
    # back to ``_read_group_full`` so we don't pretend to optimize.
    poly = grid.shard_footprint(shard_key)
    min_lon, min_lat, max_lon, max_lat = poly.bounds
    if (max_lon - min_lon) >= 180.0:
        # Hand off to the full-read path; the planned-IO benefit is gone for
        # this shard and trying to plan would waste the coarse-coord read.
        return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)
    bbox = (float(min_lon), float(min_lat), float(max_lon), float(max_lat))

    plan = plan_read(
        np.asarray(coarse_lats),
        np.asarray(coarse_lons),
        np.asarray(ibeg_arr),
        np.asarray(cnt_arr),
        n_base,
        bbox,
        index_base=index_base,
        pad=pad,
        full_read_threshold=full_read_threshold,
    )

    if not plan.parent_runs:
        return None  # empty AOI -- no parent intersects, skip the group entirely

    if plan.full_read:
        # Selectivity above threshold: many small reads would still sum to most
        # of the file. Defer to the full-coord-read path; semantics identical.
        return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)

    # h5coro-compatible reader callback for execute_read_plan.
    def _read_fn(path, hyperslice=None):
        if hyperslice is None:
            return h5obj.readDatasets([path])[path]
        return h5obj.readDatasets([{"dataset": path, "hyperslice": hyperslice}])[path]

    # ---- Read base coords + variables + filter datasets over the planned slices.
    filters = filters_from_data_source(data_source)
    base_structured = [
        f
        for f in filters
        if "expression" not in f and (f.get("level") is None or f.get("level") == base_level_key)
    ]
    coarse_structured = [
        f
        for f in filters
        if "expression" not in f and f.get("level") is not None and f.get("level") != base_level_key
    ]
    expressions = [f for f in filters if "expression" in f]

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = execute_read_plan(plan, _read_fn, lat_path, np.float64)
    lons = execute_read_plan(plan, _read_fn, lon_path, np.float64)

    if len(lats) == 0:
        return None

    # Apply spatial / shard mask over the concatenated planned reads.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == shard_key
    if np.sum(mask_spatial) == 0:
        return None

    # Read the variables and base-level filter datasets via the same plan. Read
    # each distinct path once (the variable and filter dataset paths can coincide).
    var_paths = {col: tmpl.format(group=group) for col, tmpl in variables.items()}
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    paths_seen: set[str] = set()
    arrays_by_path: dict[str, np.ndarray] = {}
    for path in list(var_paths.values()) + list(filter_paths.values()):
        if path in paths_seen:
            continue
        paths_seen.add(path)
        # dtype hint isn't load-bearing -- execute_read_plan dtype-casts via
        # np.asarray, which is a no-op when the source dtype already matches.
        arrays_by_path[path] = execute_read_plan(plan, _read_fn, path, None)

    # Base-level structured filters: ANDed keep-masks over the concatenated reads.
    keep_mask: np.ndarray | None = None
    for f in base_structured:
        flag = arrays_by_path[filter_paths[id(f)]][mask_spatial]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    # Cross-level (Phase B) filters: read coarse flags fully, expand to base
    # rate (length n_base), then subset to the planned indices.
    if coarse_structured:
        # Build the global base-index array once: which original-base positions
        # are present in the concatenated planned read.
        global_idx = np.concatenate([np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices])
        cross_full: np.ndarray | None = None
        for f in coarse_structured:
            level_key = f["level"]
            cf_lvl = levels[level_key]
            cf_link = cf_lvl["link"]
            cf_index_base = int(cf_link.get("index_base", 0))
            cf_flag_path = f["dataset"].format(group=group)
            cf_ibeg_path = cf_link["index_beg"].format(group=group)
            cf_cnt_path = cf_link["count"].format(group=group)
            cf_data = h5obj.readDatasets([cf_flag_path, cf_ibeg_path, cf_cnt_path])
            cf_flag = cf_data[cf_flag_path]
            cf_ibeg = cf_data[cf_ibeg_path]
            cf_cnt = cf_data[cf_cnt_path]
            coarse_fmask = _predicate_mask(cf_flag, f)
            expanded = _expand_mask_to_base(coarse_fmask, cf_ibeg, cf_cnt, cf_index_base, n_base)
            cross_full = expanded if cross_full is None else (cross_full & expanded)
        # Subset the full-length mask to the concatenated planned indices, then
        # to the spatial keep window so it lines up with keep_mask above.
        cross_planned = cross_full[global_idx][mask_spatial]
        keep_mask = cross_planned if keep_mask is None else (keep_mask & cross_planned)

    if keep_mask is not None and np.sum(keep_mask) == 0:
        return None

    # Segment-level variables (issue #30): read each declared non-base-level
    # variable and broadcast it to a base-rate per-photon column (length n_base),
    # then subset to the concatenated planned indices so it lines up with the
    # base-rate variables before the spatial / keep masks below.
    seg_broadcasts = _read_segment_broadcasts(h5obj, group, data_source, levels, n_base)
    if seg_broadcasts:
        seg_global_idx = np.concatenate(
            [np.arange(s, e, dtype=np.int64) for s, e in plan.base_slices]
        )

    # Build the data dict (variables sliced to mask_spatial, then to keep_mask).
    leaf_after_spatial = leaf_ids[mask_spatial]
    data_dict: dict[str, np.ndarray] = {}
    for col_name, path in var_paths.items():
        values = arrays_by_path[path][mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, base_values in seg_broadcasts.items():
        values = base_values[seg_global_idx][mask_spatial]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    data_dict["leaf_id"] = (
        leaf_after_spatial[keep_mask] if keep_mask is not None else leaf_after_spatial
    )

    # Base-level expression filters (aggregation-time escape hatch, no pushdown).
    # The namespace carries both base-rate ``variables`` and any segment-level
    # broadcast columns (issue #30), which are already materialized into
    # ``data_dict`` above, so an expression filter may reference e.g. ``dem_h``.
    expr_names = list(variables) + list(seg_broadcasts)
    for f in expressions:
        cols = {c: data_dict[c] for c in expr_names if c in data_dict}
        try:
            emask = evaluate_filter_expression(f["expression"], cols)
        except NameError as e:
            raise NameError(
                f"expression filter {f['expression']!r} references an undefined name: {e}"
            ) from e
        if emask.shape != data_dict["leaf_id"].shape:
            raise ValueError(
                f"expression filter {f['expression']!r} must yield a per-row "
                f"boolean mask (got shape {emask.shape})"
            )
        if np.sum(emask) == 0:
            return None
        data_dict = {k: v[emask] for k, v in data_dict.items()}

    if arrow:
        import pyarrow as pa

        return pa.table(data_dict)
    return pd.DataFrame(data_dict)


def _read_group(h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False):
    """Read and spatially filter one HDF5 group.

    Returns a ``pandas.DataFrame`` (default) or, when ``arrow=True``, a
    ``pyarrow.Table`` carrying the identical columns. Returns ``None`` when the
    group has no observations in this shard.

    Supports three modes (issues #43 Phase A/B/C):

    *Flat* (no ``levels``/``base_level`` in ``data_source``): unchanged from Phase A —
    all structured filters are applied directly to base-rate data.

    *Hierarchical filtering* (``levels`` + ``base_level`` present): structured
    filters whose normalized ``level`` key names a non-base level are applied at
    coarse rate, then expanded to base-rate via the level's ``link`` arrays
    (``_expand_mask_to_base``). Base-level structured filters and expression
    filters are unchanged.

    *Hierarchical (planned) read* (``read_plan.spatial_index`` set, in addition
    to ``levels``/``base_level``): the AOI bbox is computed from the grid's
    shard footprint, the coarse-level spatial-index coordinates are read fully
    (cheap), and base-rate coords + variables + filter datasets are read only
    over the planned hyperslices via :func:`zagg.read_plan.execute_read_plan`.
    Empty-AOI groups short-circuit to ``None``. Selectivity above the configured
    threshold falls back to the full-read path; the planned and full paths
    produce row-for-row identical output (#43 Phase C parity).
    """
    rp = data_source.get("read_plan")
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    # Truthy-checking ``levels``/``base_level`` would route an empty ``{}`` (a
    # config typo, easy to do) back to the full-read path silently. Reject
    # incomplete configurations explicitly instead -- the planned path is
    # gated only when ``spatial_index`` is set, and *then* requires a real
    # multi-level structure to operate on.
    if isinstance(rp, dict) and rp.get("spatial_index"):
        if not isinstance(levels, dict) or not levels:
            raise ValueError(
                "data_source.read_plan.spatial_index requires a non-empty 'levels' mapping"
            )
        if not base_level:
            raise ValueError("data_source.read_plan.spatial_index requires 'base_level'")
        return _planned_read_group(h5obj, group, data_source, shard_key, grid, arrow=arrow)
    return _read_group_full(h5obj, group, data_source, shard_key, grid, arrow=arrow)


def _read_group_full(
    h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
):
    """Full-coord-read variant of :func:`_read_group` (the pre-#49-Phase-C path).

    Reads the base-rate coordinate arrays in full, computes the spatial mask,
    then hyperslices variables + base-level filter datasets to the matched
    ``[min_idx, max_idx]`` range. Cross-level structured filters are read fully
    at coarse rate and expanded to base-rate via ``_expand_mask_to_base``.
    Expression filters apply over already-read variable columns.

    Kept as the explicit fallback for: groups whose ``data_source`` declares no
    ``read_plan.spatial_index``; ``plan_read``'s selectivity fallback
    (``full_read=True``); and the legacy flat (no-levels) form.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    filters = filters_from_data_source(data_source)
    base_level_key = data_source.get("base_level")
    levels = data_source.get("levels")
    # Partition filters: base-level structured, coarse-level structured, expressions.
    base_structured = [
        f
        for f in filters
        if "expression" not in f and (f.get("level") is None or f.get("level") == base_level_key)
    ]
    coarse_structured = [
        f
        for f in filters
        if "expression" not in f and f.get("level") is not None and f.get("level") != base_level_key
    ]
    expressions = [f for f in filters if "expression" in f]

    # Resolve coordinate paths
    coord_paths = [path.format(group=group) for path in coordinates.values()]
    coord_data = h5obj.readDatasets(coord_paths)

    lat_path = coordinates["latitude"].format(group=group)
    lon_path = coordinates["longitude"].format(group=group)
    lats = coord_data[lat_path]
    lons = coord_data[lon_path]

    if len(lats) == 0:
        return None

    # Assign points to leaf cells, then filter to the current shard.
    leaf_ids = grid.assign(lats, lons)
    mask_spatial = grid.shards_of(leaf_ids) == shard_key

    if np.sum(mask_spatial) == 0:
        return None

    # Bounding indices for hyperslice read
    indices = np.where(mask_spatial)[0]
    min_idx = int(indices[0])
    max_idx = int(indices[-1]) + 1

    # --- Coarse-level filter expansion (Phase B) ---
    # For each filter whose level is not the base level, read the coarse-rate
    # flag array from the declared level path, build a coarse mask, then expand
    # to base-rate via the level link arrays.  AND the results into ``cross_mask``.
    cross_mask: np.ndarray | None = None
    if coarse_structured and levels is not None:
        for f in coarse_structured:
            level_key = f["level"]
            lvl = levels[level_key]
            flag_path = f["dataset"].format(group=group)
            # Read the coarse flag array (full level, no hyperslice — we need all parents
            # to align with link arrays which are also full-length).
            coarse_data = h5obj.readDatasets([{"dataset": flag_path}])
            coarse_arr = coarse_data[flag_path]
            coarse_fmask = _predicate_mask(coarse_arr, f)
            # Read the link arrays from this level.
            link = lvl["link"]
            index_base = int(link.get("index_base", 0))
            ibeg_path = link["index_beg"].format(group=group)
            cnt_path = link["count"].format(group=group)
            link_data = h5obj.readDatasets(
                [
                    {"dataset": ibeg_path},
                    {"dataset": cnt_path},
                ]
            )
            ibeg_arr = link_data[ibeg_path]
            cnt_arr = link_data[cnt_path]
            expanded = _expand_mask_to_base(coarse_fmask, ibeg_arr, cnt_arr, index_base, len(lats))
            cross_mask = expanded if cross_mask is None else (cross_mask & expanded)
        if cross_mask is not None and np.sum(cross_mask[min_idx:max_idx]) == 0:
            return None

    # Build hyperslice dataset list: variables + any base-level structured-filter arrays.
    # Read each distinct path once; flag datasets may coincide with a variable.
    datasets = []
    paths_seen = set()
    var_paths = {col: tmpl.format(group=group) for col, tmpl in variables.items()}
    for path in var_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)
    filter_paths = {id(f): f["dataset"].format(group=group) for f in base_structured}
    for path in filter_paths.values():
        if path not in paths_seen:
            datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})
            paths_seen.add(path)

    data = h5obj.readDatasets(datasets)

    # Apply spatial mask to sliced data
    mask_sliced = mask_spatial[min_idx:max_idx]

    # Combine base-level structured predicates as ANDed keep-masks (issue #43).
    keep_mask = None
    for f in base_structured:
        flag = data[filter_paths[id(f)]][mask_sliced]
        fmask = _predicate_mask(flag, f)
        keep_mask = fmask if keep_mask is None else (keep_mask & fmask)

    # AND in the cross-level expanded mask, aligned to the sliced window.
    if cross_mask is not None:
        cross_sliced = cross_mask[min_idx:max_idx][mask_sliced]
        keep_mask = cross_sliced if keep_mask is None else (keep_mask & cross_sliced)

    if keep_mask is not None and np.sum(keep_mask) == 0:
        return None

    # Segment-level variables (issue #30): read each declared non-base-level
    # variable and broadcast it to a base-rate per-photon column (length len(lats))
    # so it can be sliced through the same masks as the base-rate variables below.
    seg_broadcasts = _read_segment_broadcasts(h5obj, group, data_source, levels or {}, len(lats))

    # Build dataframe (variables sliced to spatial mask, then to the keep-mask)
    leaf_sliced = leaf_ids[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path in var_paths.items():
        values = data[path][mask_sliced]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values
    for col_name, base_values in seg_broadcasts.items():
        values = base_values[min_idx:max_idx][mask_sliced]
        if keep_mask is not None:
            values = values[keep_mask]
        data_dict[col_name] = values

    if keep_mask is not None:
        data_dict["leaf_id"] = leaf_sliced[keep_mask]
    else:
        data_dict["leaf_id"] = leaf_sliced

    # Base-level ``expression`` filters: aggregation-time escape hatch, evaluated
    # over the already-read variable columns (forfeits pushdown, issue #43). The
    # namespace also carries any segment-level broadcast columns (issue #30),
    # materialized into ``data_dict`` above, so a filter may reference e.g.
    # ``dem_h``.
    expr_names = list(variables) + list(seg_broadcasts)
    for f in expressions:
        cols = {c: data_dict[c] for c in expr_names if c in data_dict}
        try:
            emask = evaluate_filter_expression(f["expression"], cols)
        except NameError as e:
            raise NameError(
                f"expression filter {f['expression']!r} references an undefined name: {e}"
            ) from e
        if emask.shape != data_dict["leaf_id"].shape:
            raise ValueError(
                f"expression filter {f['expression']!r} must yield a per-row "
                f"boolean mask (got shape {emask.shape})"
            )
        if np.sum(emask) == 0:
            return None
        data_dict = {k: v[emask] for k, v in data_dict.items()}

    if arrow:
        import pyarrow as pa

        return pa.table(data_dict)
    return pd.DataFrame(data_dict)


def process_shard(
    grid,
    shard_key: int,
    granule_urls: List[str],
    *,
    s3_credentials: dict,
    h5coro_driver=None,
    config: PipelineConfig | None = None,
    driver: str | None = None,
    handoff: str = "pandas",
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """Process one shard: read granules, filter to this shard, aggregate, return df.

    Grid-agnostic. For HEALPix, ``shard_key`` is the parent morton ID; for
    rectilinear, the packed ``rb * n_col_blocks + cb`` chunk index.

    Parameters
    ----------
    grid : OutputGrid
        Output grid (provides ``assign``/``shards_of``/``children``/
        ``encode_cell_ids``/``chunk_coords``).
    shard_key : int
        Shard identifier (grid-specific encoding).
    granule_urls : list of str
        S3 URLs or file paths to read.
    s3_credentials : dict
        For S3: ``accessKeyId``/``secretAccessKey``/``sessionToken``.
        For HTTPS: ``{"edl_token": "..."}``.
    h5coro_driver : class, optional
        Overrides ``driver``.
    config : PipelineConfig, optional
        Defaults to ``default_config()``.
    driver : str, optional
        ``"s3"`` (default) or ``"https"``.
    handoff : str, optional
        Per-cell aggregation carrier: ``"pandas"`` (default), ``"arrow"``, or the
        EXPERIMENTAL ``"arrow-kernel"``. ``"pandas"`` and ``"arrow"`` share
        :func:`_group_columns` and the same numpy reductions, so scalar outputs
        are byte-for-byte identical; only the read→concat→extract representation
        differs. ``"arrow-kernel"`` (phase 2b of #30) instead reduces via
        ``pyarrow.compute`` hash-aggregate kernels: ``count``/``min``/``max`` stay
        exact vs numpy (NaN included — see :func:`_kernel_aggregate`), while its
        float ``mean``/``variance`` differ by ~1 ULP (agree within
        :data:`KERNEL_RTOL`, not byte identical). All three are opt-in while
        benchmarked (issue #30).

    Returns
    -------
    (DataFrame, metadata)
        DataFrame in canonical chunk order; metadata dict with ``shard_key``,
        ``cells_with_data``, ``total_obs``, ``granule_count``,
        ``files_processed``, ``duration_s``, ``error``.
    """
    if config is None:
        config = default_config()
    if handoff not in ("pandas", "arrow", "arrow-kernel"):
        raise ValueError(f"handoff must be 'pandas', 'arrow', or 'arrow-kernel', got {handoff!r}")
    data_source = config.data_source

    shard_key = int(shard_key)
    logger.info(f"Processing shard: {shard_key}")
    start_time = datetime.now()

    # Resolve driver
    if h5coro_driver is None:
        if driver is None:
            driver = config.data_source.get("driver", "s3")
        if driver == "https":
            from h5coro import webdriver

            h5coro_driver = webdriver.HTTPDriver
        else:
            from h5coro import s3driver

            h5coro_driver = s3driver.S3Driver

    # Prepare metadata
    metadata: ProcessingMetadata = {
        "shard_key": shard_key,
        "cells_with_data": 0,
        "total_obs": 0,
        "granule_count": len(granule_urls),
        "files_processed": 0,
        "duration_s": 0.0,
        "error": None,
    }

    # Check for granules
    if not granule_urls:
        logger.info(f"  No granules provided for shard {shard_key} - skipping")
        metadata["error"] = "No granules found"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    logger.info(f"  Processing {len(granule_urls)} granules from catalog")

    # Prepare credentials for h5coro
    if driver == "https":
        credentials = s3_credentials.get("edl_token", s3_credentials)
    else:
        credentials = {
            "aws_access_key_id": s3_credentials.get("accessKeyId")
            or s3_credentials.get("aws_access_key_id"),
            "aws_secret_access_key": s3_credentials.get("secretAccessKey")
            or s3_credentials.get("aws_secret_access_key"),
            "aws_session_token": s3_credentials.get("sessionToken")
            or s3_credentials.get("aws_session_token"),
        }

    # Build URL rewriter for the active driver
    _rewrite_url = _make_url_rewriter(driver)

    use_arrow = handoff in ("arrow", "arrow-kernel")
    all_reads = []
    files_processed = 0

    # Read files and filter spatially
    for s3_url in granule_urls:
        try:
            resource_path = _rewrite_url(s3_url)

            h5obj = h5coro.H5Coro(
                resource_path,
                h5coro_driver,
                credentials=credentials,
                errorChecking=True,
                verbose=False,
            )

            for g in data_source["groups"]:
                try:
                    chunk = _read_group(h5obj, g, data_source, shard_key, grid, arrow=use_arrow)
                    if chunk is not None:
                        all_reads.append(chunk)
                except Exception as e:
                    logger.debug(f"  Error reading track {g}: {e}")
                    continue

            files_processed += 1

        except Exception as e:
            logger.warning(f"  Error processing file {s3_url}: {e}")
            continue

    logger.info(f"  Processed {files_processed}/{len(granule_urls)} files")
    metadata["files_processed"] = files_processed

    if not all_reads:
        logger.info(f"  No data after filtering for shard {shard_key} - skipping")
        metadata["error"] = "No data after filtering"
        metadata["duration_s"] = (datetime.now() - start_time).total_seconds()
        return pd.DataFrame(), metadata

    children = grid.children(shard_key)
    data_vars = get_data_vars(config)

    # Ragged-field collectors (issue #48): populated only in the non-kernel path;
    # initialized here so the post-if/else _build_output call can reference them
    # regardless of which branch ran.
    ragged_payloads: dict[str, list] = {}
    ragged_cell_indices: dict[str, list[int]] = {}

    if handoff == "arrow-kernel":
        # EXPERIMENTAL (phase 2b of #30): reduce via pyarrow hash-aggregate kernels
        # instead of the per-cell numpy loop. Not byte-identical to the default
        # path (float mean/variance diverge by ~1 ULP — see KERNEL_RTOL).
        if _has_ragged_fields(config):
            raise NotImplementedError(
                "handoff='arrow-kernel' does not support ragged fields (issue #48); "
                "use handoff='pandas' or 'arrow' instead"
            )
        import pyarrow as pa

        table = pa.concat_tables(all_reads).combine_chunks()
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        cell_col = grid.cells_of(table.column("leaf_id").to_numpy(zero_copy_only=False))
        logger.info(f"  Read {n_obs_total:,} observations")
        # Per-chunk precompute hook (issue #30): reduce each entry ONCE over the
        # pooled arrow table. Columns are dense + null-free (guarded above), so the
        # ``to_numpy`` extraction is zero-copy where the buffer layout allows and
        # dtype-exact otherwise — the same numpy arrays the pandas/arrow carriers
        # feed in. The resulting scalars/arrays are threaded into the kernel
        # fallback per-cell loop (where expression fields resolve).
        pooled = {n: table.column(n).to_numpy(zero_copy_only=False) for n in table.column_names}
        chunk_scalars = _eval_chunk_precompute(config, pooled)
        logger.info(f"  Calculating statistics for {len(children)} cells (kernel)...")
        kernel = _kernel_aggregate(
            table, cell_col, children, "h_li", config, chunk_scalars=chunk_scalars
        )
        stats_arrays = kernel["stats_arrays"]
        cells_with_data = kernel["cells_with_data"]
        n_cells = len(children)
    else:
        # Concat the per-group reads and split observations by cell (carrier-
        # agnostic; both carriers feed identical numpy arrays into _group_columns).
        col_arrays, cell_to_slice, n_obs_total = _concat_and_group(all_reads, grid, handoff)
        logger.info(f"  Read {n_obs_total:,} observations")

        # Per-chunk precompute hook (issue #30, item 1): evaluate each
        # ``chunk_precompute`` entry ONCE over the shard's pooled columns, then
        # inject the resulting chunk-level scalars into every cell's namespace so a
        # per-cell expression can reference a chunk-uniform anchor (e.g. the 128-bin
        # waveform window). Empty when the block is absent, so the per-cell path is
        # byte-for-byte unchanged for configs that do not use the hook.
        chunk_scalars = _eval_chunk_precompute(config, col_arrays)
        logger.info(f"  Calculating statistics for {len(children)} cells...")

        n_cells = len(children)
        agg_fields = get_agg_fields(config)
        stats_arrays: dict = {}
        # Ragged fields (issue #48) are variable-length per-cell; they cannot be
        # preallocated as a dense block. ``ragged_payloads``/``ragged_cell_indices``
        # are pre-initialized before this branch (see above); fill them in the loop.
        for name in data_vars:
            meta = agg_fields[name]
            sig = get_output_signature(meta)
            if sig["kind"] == "ragged":
                ragged_payloads[name] = []
                ragged_cell_indices[name] = []
                continue
            # Vector fields (issue #29) get a per-cell (n_cells, *trailing_shape)
            # block; scalars keep the 1-D (n_cells,) layout, unchanged. Either way
            # ``stats_arrays[name][i] = value`` assigns the cell's result row.
            shape = (n_cells, *sig["trailing_shape"])
            zarr_dtype = np.dtype(meta.get("dtype", "float32"))
            fill_value = meta.get("fill_value", "NaN")
            if fill_value == "NaN":
                stats_arrays[name] = np.full(shape, np.nan, dtype=zarr_dtype)
            else:
                stats_arrays[name] = np.zeros(shape, dtype=zarr_dtype)

        # Per-cell observation slices (grouped above, carrier-agnostic).
        _empty: dict[str, np.ndarray] = {col: arr[:0] for col, arr in col_arrays.items()}

        cells_with_data = 0
        for i, child_morton in enumerate(children):
            if child_morton in cell_to_slice:
                start, end = cell_to_slice[child_morton]
                cell_data: dict[str, np.ndarray] = {
                    col: arr[start:end] for col, arr in col_arrays.items()
                }
                cells_with_data += 1
            else:
                cell_data = _empty
            # Inject the chunk-level scalars into this cell's namespace (no-op when
            # ``chunk_scalars`` is empty, so non-precompute configs are unchanged).
            cell_namespace: dict[str, Any] = (
                {**cell_data, **chunk_scalars} if chunk_scalars else cell_data
            )
            stats = calculate_cell_statistics(
                cell_namespace, value_col="h_li", sigma_col="s_li", config=config
            )
            for key, value in stats.items():
                if key in ragged_payloads:
                    # Ragged field: collect non-empty payloads with their cell index.
                    # Empty cells (from _empty_cell_value -> []) are skipped; the
                    # CSR writer represents absent cells via ``cell_ids``.
                    arr_val = np.asarray(value)
                    if arr_val.size > 0:
                        ragged_payloads[key].append(arr_val)
                        ragged_cell_indices[key].append(i)
                else:
                    stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Assemble the output carrier: a plain DataFrame for a pure-scalar config
    # (unchanged), or a pyarrow.Table with FixedSizeList vector columns when any
    # field declares a non-scalar output (issue #29). Scalars stay byte-identical.
    # Ragged fields (issue #48) are excluded from the dense carrier — they are
    # returned separately as (payloads, cell_indices) for the CSR writer.
    _agg_fields = get_agg_fields(config)
    dense_vars = [v for v in data_vars if get_output_signature(_agg_fields[v])["kind"] != "ragged"]
    df_out = _build_output(
        stats_arrays,
        dense_vars,
        _agg_fields,
        grid,
        shard_key,
        use_arrow=_has_vector_fields(config),
    )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {shard_key} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = n_obs_total
    metadata["duration_s"] = duration

    return df_out, metadata


def process_morton_cell(
    parent_morton: int,
    parent_order: int,
    child_order: int,
    granule_urls: List[str],
    s3_credentials: dict,
    h5coro_driver=None,
    config: PipelineConfig | None = None,
    driver: str | None = None,
    grid=None,
) -> Tuple[pd.DataFrame, ProcessingMetadata]:
    """Deprecated HEALPix-flavored alias for :func:`process_shard`.

    Constructs a stateless ``HealpixGrid`` and forwards to ``process_shard``.
    """
    warnings.warn(
        "process_morton_cell is deprecated; use process_shard(grid, shard_key, ...) directly.",
        DeprecationWarning,
        stacklevel=2,
    )
    if grid is None:
        from zagg.grids import HealpixGrid

        grid = HealpixGrid(
            parent_order=parent_order,
            child_order=child_order,
            layout="fullsphere",
            config=config or default_config(),
        )
    return process_shard(
        grid,
        parent_morton,
        granule_urls,
        s3_credentials=s3_credentials,
        h5coro_driver=h5coro_driver,
        config=config,
        driver=driver,
    )
