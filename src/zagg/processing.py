"""
Cloud-agnostic processing functions for aggregating HDF5 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.
"""

import logging
import warnings
from datetime import datetime
from typing import List, Tuple

import h5coro
import numpy as np
import pandas as pd
from zarr import config, open_array
from zarr.abc.store import Store

from zagg.config import (
    PipelineConfig,
    default_config,
    get_agg_fields,
    get_data_vars,
    get_output_signature,
)
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


def _has_vector_fields(config: PipelineConfig) -> bool:
    """Whether any aggregation field declares a non-scalar (``vector``) output.

    A pure-scalar config keeps the unchanged pandas carrier; any ``vector`` field
    (issue #29) routes the whole cell->table handoff through Arrow (see
    :func:`_arrow_column`).
    """
    return any(
        get_output_signature(meta)["kind"] == "vector"
        for meta in get_agg_fields(config).values()
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
        columns[col_name] = pa.array(np.asarray(vals))
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

    chunk_idx = tuple(int(i) for i in chunk_idx)
    for name, values in _iter_carrier_columns(df_out):
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
            array.set_block_selection(block_idx, values)

    return store


def _iter_carrier_columns(carrier):
    """Yield ``(name, ndarray)`` for each column of a DataFrame or Arrow table.

    Scalar columns yield a 1-D array; a ``FixedSizeList<C>`` Arrow column yields a
    2-D ``(n_cells, C)`` array (the per-cell vector block), so the writer can map
    it onto the Zarr trailing payload dimension (issue #29).
    """
    if isinstance(carrier, pd.DataFrame):
        for name, series in carrier.items():
            yield name, series.values
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
    cell_data: dict[str, np.ndarray],
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
    cell_data : dict[str, np.ndarray]
        Column arrays for a single cell. Keys are column names; values are
        numpy arrays of equal length.
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
    from zagg.config import _eval_expression_raw, evaluate_expression, resolve_function

    if config is None:
        config = default_config()
    agg_fields = get_agg_fields(config)

    n_obs = len(next(iter(cell_data.values()))) if cell_data else 0
    if n_obs == 0:
        return {name: _empty_cell_value(meta) for name, meta in agg_fields.items()}

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
        # ``function`` field (issue #29).
        if expression:
            if sig["kind"] == "vector":
                out = _eval_expression_raw(expression, cell_data)
                result[name] = _coerce_field_value(out, sig)
            else:
                result[name] = evaluate_expression(expression, cell_data)
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
        # Scalar fields stay byte-for-byte identical to the pre-#29 path; only a
        # declared ``vector`` field is allowed to return an ndarray (issue #29).
        result[name] = _coerce_field_value(out, sig) if sig["kind"] == "vector" else float(out)

    return result


def _empty_cell_value(meta: dict):
    """Value emitted for a single agg field when its cell has no observations.

    Scalar fields keep the pre-#29 contract: ``0`` for ``len``/``count``,
    ``np.nan`` otherwise. A ``vector`` field (issue #29) instead gets a full
    ``trailing_shape`` array filled with its schema-declared sentinel
    (:func:`_field_sentinel`), so empty and populated cells emit the same shape.
    """
    sig = get_output_signature(meta)
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
    table, cell_col: np.ndarray, children, value_col: str, config: PipelineConfig
) -> dict:
    """EXPERIMENTAL pyarrow hash-aggregate reducer (phase 2b of #30).

    Computes the kernel-able stats (count/min/max/variance/unweighted-mean) for
    every child cell in one vectorised ``TableGroupBy.aggregate`` pass, then fills
    the remaining (weighted mean, expression, quantile) fields via the per-cell
    numpy path so output columns match the default reducer exactly in shape.

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
        zarr_dtype = np.dtype(meta.get("dtype", "float32"))
        fill_value = meta.get("fill_value", "NaN")
        if fill_value == "NaN":
            stats_arrays[name] = np.full(n_cells, np.nan, dtype=zarr_dtype)
        else:
            stats_arrays[name] = np.zeros(n_cells, dtype=zarr_dtype)

    kernel_names = [n for n in data_vars if _kernel_able(agg_fields[n])]
    fallback_names = [n for n in data_vars if n not in kernel_names]

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
            stats = calculate_cell_statistics(cell_data, value_col=value_col, config=config)
            for name in fallback_names:
                stats_arrays[name][i] = stats[name]

    return {"stats_arrays": stats_arrays, "cells_with_data": cells_with_data}


# -- end EXPERIMENTAL kernel path ---------------------------------------------


def _read_group(
    h5obj, group: str, data_source: dict, shard_key: int, grid, arrow: bool = False
):
    """Read and spatially filter one HDF5 group.

    Returns a ``pandas.DataFrame`` (default) or, when ``arrow=True``, a
    ``pyarrow.Table`` carrying the identical columns. Returns ``None`` when the
    group has no observations in this shard.
    """
    coordinates = data_source["coordinates"]
    variables = data_source["variables"]
    quality_filter = data_source.get("quality_filter")

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

    # Build hyperslice dataset list: variables + optional quality filter
    datasets = []
    for path_template in variables.values():
        path = path_template.format(group=group)
        datasets.append({"dataset": path, "hyperslice": [(min_idx, max_idx)]})

    if quality_filter is not None:
        qf_path = quality_filter["dataset"].format(group=group)
        datasets.append({"dataset": qf_path, "hyperslice": [(min_idx, max_idx)]})

    data = h5obj.readDatasets(datasets)

    # Apply spatial mask to sliced data
    mask_sliced = mask_spatial[min_idx:max_idx]

    # Apply quality filter if configured
    if quality_filter is not None:
        qf_path = quality_filter["dataset"].format(group=group)
        q_flag = data[qf_path][mask_sliced]
        quality_mask = q_flag == quality_filter["value"]
        if np.sum(quality_mask) == 0:
            return None
    else:
        quality_mask = None

    # Build dataframe
    leaf_sliced = leaf_ids[min_idx:max_idx][mask_sliced]
    data_dict = {}
    for col_name, path_template in variables.items():
        path = path_template.format(group=group)
        values = data[path][mask_sliced]
        if quality_mask is not None:
            values = values[quality_mask]
        data_dict[col_name] = values

    if quality_mask is not None:
        data_dict["leaf_id"] = leaf_sliced[quality_mask]
    else:
        data_dict["leaf_id"] = leaf_sliced

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

    if handoff == "arrow-kernel":
        # EXPERIMENTAL (phase 2b of #30): reduce via pyarrow hash-aggregate kernels
        # instead of the per-cell numpy loop. Not byte-identical to the default
        # path (float mean/variance diverge by ~1 ULP — see KERNEL_RTOL).
        import pyarrow as pa

        table = pa.concat_tables(all_reads).combine_chunks()
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        cell_col = grid.cells_of(table.column("leaf_id").to_numpy(zero_copy_only=False))
        logger.info(f"  Read {n_obs_total:,} observations")
        logger.info(f"  Calculating statistics for {len(children)} cells (kernel)...")
        kernel = _kernel_aggregate(table, cell_col, children, "h_li", config)
        stats_arrays = kernel["stats_arrays"]
        cells_with_data = kernel["cells_with_data"]
        n_cells = len(children)
    else:
        # Concat the per-group reads and split observations by cell (carrier-
        # agnostic; both carriers feed identical numpy arrays into _group_columns).
        col_arrays, cell_to_slice, n_obs_total = _concat_and_group(all_reads, grid, handoff)
        logger.info(f"  Read {n_obs_total:,} observations")
        logger.info(f"  Calculating statistics for {len(children)} cells...")

        n_cells = len(children)
        agg_fields = get_agg_fields(config)
        stats_arrays = {}
        for name in data_vars:
            meta = agg_fields[name]
            # Vector fields (issue #29) get a per-cell (n_cells, *trailing_shape)
            # block; scalars keep the 1-D (n_cells,) layout, unchanged. Either way
            # ``stats_arrays[name][i] = value`` assigns the cell's result row.
            shape = (n_cells, *get_output_signature(meta)["trailing_shape"])
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
            stats = calculate_cell_statistics(
                cell_data, value_col="h_li", sigma_col="s_li", config=config
            )
            for key, value in stats.items():
                stats_arrays[key][i] = value

    logger.info(f"  Statistics: {cells_with_data}/{n_cells} cells with data")

    # Assemble the output carrier: a plain DataFrame for a pure-scalar config
    # (unchanged), or a pyarrow.Table with FixedSizeList vector columns when any
    # field declares a non-scalar output (issue #29). Scalars stay byte-identical.
    df_out = _build_output(
        stats_arrays,
        data_vars,
        get_agg_fields(config),
        grid,
        shard_key,
        use_arrow=_has_vector_fields(config),
    )

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"Completed shard {shard_key} in {duration:.1f}s")

    metadata["cells_with_data"] = cells_with_data
    metadata["total_obs"] = int(stats_arrays["count"].sum())
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
