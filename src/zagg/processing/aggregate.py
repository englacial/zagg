"""Aggregate-stage helpers for :mod:`zagg.processing` (split out of the
monolithic ``processing.py`` for the §4 size limit; pure relocation, no behavior
change).

Per-cell statistics, grouping, coercion, and the per-chunk precompute hook.
Depends only on ``config`` — never on the read or write stages — so the import
DAG stays acyclic.
"""

import logging
import os
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

from zagg.config import (
    PipelineConfig,
    default_config,
    get_agg_fields,
    get_chunk_precompute,
    get_output_signature,
)
from zagg.time_axis import TOC_SHAPE_PER_CENTROID, TOC_WORD_COLUMN

logger = logging.getLogger(__name__)


def _temporal_fields(agg_fields: dict) -> dict[str, str]:
    """``{field: temporal shape}`` for every field declaring one (spec §8, #410).

    Empty for every config written before #410, which is what keeps the derived
    toc word column off those runs' code path entirely.
    """
    return {
        name: str(meta["temporal"])
        for name, meta in agg_fields.items()
        if meta.get("temporal") is not None
    }


def _checked_toc_source(columns, config) -> dict:
    """The validated clock declaration for a temporal companion (spec §8.3, #410).

    The refusal seam both encode routes share: a declared companion with no
    ``output.time_source`` block is the §8.3 no-clock error (config validation is
    the front door, but a config built without it must still fail loudly rather
    than write an empty companion), and a declared clock whose column was not
    read names itself. ``columns`` only needs membership + iteration, so either
    a per-cell namespace or the pooled column dict can be checked without
    gathering anything.
    """
    from zagg.time_axis import TOC_NO_CLOCK_ERROR, toc_source

    source = toc_source(config)
    if source is None:
        # Defense in depth: validate_config refuses this at submission with the
        # same single-sourced message (issue #472).
        raise ValueError(TOC_NO_CLOCK_ERROR)
    if source["field"] not in columns:
        raise ValueError(
            f"output.time_source.field {source['field']!r} is not in the cell data "
            f"(available: {sorted(columns)}); a temporal companion needs the declared "
            f"time column read at base rate"
        )
    return source


def _toc_word_column(cell_data: dict, config) -> np.ndarray:
    """Encode this cell's observation times as toc words (spec §8.3, #410).

    The single conversion point: the declared ``output.time_source`` column
    through :func:`zagg.time_axis.observation_words`. Both companion shapes read
    the result — a ``per-centroid`` field as the reducer's ``temporal=`` channel,
    a ``per-cell`` field as its ``source`` column — so one store can never carry
    two clocks. The chunk path encodes through :func:`_chunk_toc_words` instead
    (one pass per chunk, issue #476); this per-cell route remains for direct
    :func:`calculate_cell_statistics` callers.
    """
    from zagg.time_axis import observation_words

    source = _checked_toc_source(cell_data, config)
    return observation_words(
        cell_data[source["field"]],
        epoch=source["epoch"],
        scale=source["scale"],
        units=source["units"],
    )


def _chunk_toc_words(
    col_arrays: dict[str, np.ndarray],
    cell_to_slice: dict[int, tuple[int, int]],
    children: np.ndarray,
    config,
    pooled: dict[str, np.ndarray] | None = None,
) -> dict[int, np.ndarray]:
    """Encode one chunk's toc words in ONE pass and split them per cell (#476).

    Per-chunk hoist of :func:`_toc_word_column`: gather the declared
    ``output.time_source`` column over the chunk's populated cells, encode it
    through one :func:`zagg.time_axis.observation_words` call, and slice the
    result back per cell. The encode is element-wise, so each cell's words are
    byte-identical to the per-cell encode this replaces — only the call count
    collapses (issue #476 measured 28,515 per-cell encodes at 0.21 ms against
    one pooled pass over the same rows).

    ``pooled`` is :func:`_pool_chunk_columns`' output for the same ``children``,
    when the caller already has it: both walk ``children`` in order and take the
    same ``cell_to_slice`` slices, so the pooled clock column IS this gather and
    reusing it skips a second index build and column copy per chunk (review
    finding, PR #478). Without it the index is built here — the direct-call route
    (tests, any caller without the pooled dict).

    Returns ``{cell_key: words}`` for exactly the chunk's populated cells. A
    chunk with no populated cells returns ``{}`` before touching the clock
    declaration — matching the per-cell path, where an empty cell never reaches
    the encode — while any populated cell runs the §8.3 refusal checks
    (:func:`_checked_toc_source`) exactly as the per-cell route did.
    """
    present: list[tuple[int, int, int]] = []
    for child in children:
        key = int(child)
        sl = cell_to_slice.get(key)
        if sl is not None:
            present.append((key, *sl))
    if not present:
        return {}
    from zagg.time_axis import observation_words

    source = _checked_toc_source(col_arrays, config)
    n_rows = sum(end - start for _, start, end in present)
    if pooled is not None:
        gathered = pooled[source["field"]]
        if len(gathered) != n_rows:
            raise ValueError(
                f"pooled chunk columns hold {len(gathered)} rows but this chunk's cells "
                f"span {n_rows}; the pooled columns must be _pool_chunk_columns' output "
                "for the same children"
            )
    elif len(present) == 1:
        _, start, end = present[0]
        gathered = col_arrays[source["field"]][start:end]
    else:
        idx = np.concatenate([np.arange(start, end) for _, start, end in present])
        gathered = col_arrays[source["field"]][idx]
    words = observation_words(
        gathered, epoch=source["epoch"], scale=source["scale"], units=source["units"]
    )
    out: dict[int, np.ndarray] = {}
    pos = 0
    for key, start, end in present:
        n = end - start
        out[key] = words[pos : pos + n]
        pos += n
    return out


def _rss_mb() -> float:
    """Best-effort process RSS in MB; never raises (diagnostic-only).

    Linux current RSS via ``/proc``; peak ``rusage`` fallback elsewhere; ``0.0`` if
    neither is available (e.g. Windows has no ``/proc`` and may lack ``resource``).
    """
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1e6
    except (FileNotFoundError, OSError, ValueError):
        try:
            import resource
            import sys

            m = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return m / 1e6 if sys.platform == "darwin" else m / 1024  # mac=bytes, linux=KB
        except Exception:
            return 0.0


def _rss_log(stage: str) -> None:
    """Opt-in per-stage RSS trace (set ``ZAGG_PROFILE_RSS=1``) for #130 diagnostics."""
    if os.environ.get("ZAGG_PROFILE_RSS"):
        logger.info(f"  [rss] {stage:34s} {_rss_mb():7.0f} MB")


@lru_cache(maxsize=256)
def _compile_param_expr(expression: str):
    """Cache the code object for a params expression (review finding, PR #334).

    A params value that names columns is evaluated **per cell**, so passing the
    source string to ``eval`` recompiles it once per cell per field. Measured on
    the issue #321 strata config's 5-term ``where`` predicate: 18.9 us per
    ``eval(str)`` vs 3.8 us per ``eval(code)`` — i.e. ~2.5 s of pure
    recompilation per o11 shard (65,536 cells x two ``where`` fields). Compiling
    in ``"eval"`` mode is semantically identical to ``eval`` on the string; the
    bounded cache keys on the expression source, of which a config has a handful.
    """
    return compile(expression, "<zagg-param>", "eval")


def _field_sentinel(meta: dict) -> float:
    """Per-cell fill value for an agg field's empty/unused slots.

    Mirrors how ``process_shard`` seeds its output arrays: the schema-declared
    ``fill_value`` (default ``"NaN"`` -> ``np.nan``,
    else the literal numeric fill). Used both for scalar empty cells and for the
    padding of ``vector`` fields (issue #29 Option B).
    """
    fill_value = meta.get("fill_value", "NaN")
    return np.nan if fill_value == "NaN" else fill_value


def _integer_fill(meta: dict, dtype) -> int:
    """:func:`_field_sentinel` for an INTEGER-dtype field: the sentinel must be numeric.

    The float default is the string ``"NaN"``, which no integer array can hold
    (issue #321's packed composition word declares ``fill_value: 0`` instead).
    Reject a non-numeric sentinel by name here rather than letting it surface
    mid-shard as a stray ``int('NaN')`` parse error or a numpy
    ``np.full(..., np.nan, dtype=uint64)`` failure — the two integer paths
    (empty scalar cells, ``vector`` padding) then agree with each other and with
    :func:`_field_sentinel` on where the value comes from (review finding).
    """
    fill = meta.get("fill_value", 0)
    if isinstance(fill, str):
        raise ValueError(
            f"integer field (dtype {np.dtype(dtype)}) declares fill_value {fill!r}: an "
            f"integer array cannot hold a non-numeric sentinel — declare a numeric "
            f"fill_value (e.g. 0) or a float dtype"
        )
    return int(fill)


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
    Arrow path is exercised end-to-end (including multi-table concat ordering)
    rather than re-assembled inline. Both carriers feed identical numpy arrays into
    :func:`_group_columns`, so the groupings — and the aggregations computed from
    them — are byte-for-byte identical.

    The arrow carrier is ``arro3-core`` (issue #130 path C): pyarrow is no longer a
    runtime dependency. ``arro3`` has no whole-table concat helper, so the per-group
    reads are concatenated by collecting their record batches into one table.

    Parameters
    ----------
    all_reads : list
        Per-group reads from ``_read_group``: ``pandas.DataFrame`` for the pandas
        carrier, ``arro3.core.Table`` for the arrow carrier.
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
        from arro3.core import Table

        # arro3 has no ``concat_tables``; collect every read's batches into one
        # table (preserving order), matching pyarrow's concat semantics.
        batches = [b for tbl in all_reads for b in tbl.to_batches()]
        table = Table.from_batches(batches, schema=all_reads[0].schema)
        _rss_log("arrow: after from_batches")
        # The arrow handoff requires dense, null-free columns: ``_read_group``
        # builds tables from raw h5coro reads (no null mask), so ``to_numpy`` is
        # dtype-exact and matches ``.values`` on the pandas side. Guard the
        # precondition so a future nullable source can't silently diverge the two
        # carriers instead of failing loudly.
        null_cols = [n for n in table.column_names if table.column(n).null_count]
        if null_cols:
            raise ValueError(f"arrow handoff requires null-free columns; got nulls in {null_cols}")
        n_obs_total = table.num_rows
        # ``combine_chunks().to_numpy()`` is the one forced copy: it concatenates
        # each column's per-read chunks into a fresh contiguous numpy array.
        cols = {n: table.column(n).combine_chunks().to_numpy() for n in table.column_names}
        _rss_log("arrow: after combine_chunks->numpy")
        # The pooled data now lives in ``cols`` (independent numpy copies that
        # ``combine_chunks().to_numpy()`` owns). Release the read-stage Arrow buffers
        # before grouping -- the chunked ``table``, its ``batches``, and the per-read
        # tables in ``all_reads`` -- so the worker doesn't hold the pooled data twice
        # through ``_group_columns``. Without this the peak RSS doubled and OOM'd the
        # densest shard at the 2 GB Lambda cap (issue #130). ``all_reads`` is unused
        # after this call (worker.py).
        del table, batches
        all_reads.clear()
        _rss_log("arrow: after free Arrow buffers")
        cell_col = grid.cells_of(cols["leaf_id"])
        col_arrays, cell_to_slice = _group_columns(cols, cell_col)
        _rss_log("arrow: after group")
    else:
        df_all = pd.concat(all_reads, ignore_index=True)
        _rss_log("pandas: after concat")
        n_obs_total = len(df_all)
        cell_col = grid.cells_of(df_all["leaf_id"].values)
        col_arrays, cell_to_slice = _build_groups(df_all, cell_col)
        _rss_log("pandas: after group")
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

    It deliberately diverges from the per-cell path in one way: entries are
    evaluated independently over ``pooled`` only, with no defined order, so one
    entry cannot reference another's scalar (validation rejects inter-precompute
    references — see ``_validate_chunk_precompute``).

    Empty input (``n_obs == 0``) is short-circuited: when ``pooled`` carries no
    observations — which happens for an empty inner chunk once the reduction moved
    into the per-chunk loop (issue #82 phase 6), since ``iter_chunks`` yields every
    chunk including the empty ones — each entry returns a NaN anchor (cast to the
    entry's ``dtype`` if declared) instead of evaluating its expression/function.
    Without this guard the canonical gain/offset anchor ``np.float32(np.min(h_li))``
    would raise ``ValueError: zero-size array to reduction`` on the first empty
    chunk (``np.min``/``np.nanmin`` over empty both raise). This mirrors the
    per-cell path's ``n_obs == 0`` short-circuit in
    :func:`calculate_cell_statistics`.

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

    # Empty-chunk short-circuit (issue #82 phase 6): an empty inner chunk leaves
    # length-0 pooled columns, and the canonical ``np.min``/``np.nanmin`` anchor
    # raises ``ValueError`` over a zero-size array. Mirror the per-cell ``n_obs ==
    # 0`` guard: count a real length-bearing column (skip any 0-d value) and, when
    # there are no observations, return a NaN anchor (cast to the entry's declared
    # ``dtype``) for every entry rather than evaluating its expression/function.
    n_obs = next((len(v) for v in pooled.values() if np.ndim(v) != 0), 0)
    if n_obs == 0:
        empty_out: dict[str, Any] = {}
        for name, meta in entries.items():
            dtype = meta.get("dtype")
            empty_out[name] = (
                np.dtype(dtype).type(np.nan) if dtype is not None else np.float64(np.nan)
            )
        return empty_out

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
                    resolved_params[pkey] = eval(_compile_param_expr(pval), ns)  # noqa: S307
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
    """Whether any aggregation field declares a ``ragged`` output.

    Ragged fields (issue #48) carry variable-length per-cell payloads and are
    collected separately from scalar/vector fields; they are written via the
    ragged vlen writer rather than the dense Zarr path.
    """
    return any(
        get_output_signature(meta)["kind"] == "ragged" for meta in get_agg_fields(config).values()
    )


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
    # The derived toc word column (spec §8.2/§8.3, issue #410): one word per
    # observation, from ONE conversion point whichever route reaches the
    # aggregation. The chunk path (``_aggregate_chunk_cells`` — pooled and spill
    # read-back alike) pre-encodes it once per chunk (``_chunk_toc_words``,
    # issue #476) and injects it at ``cell_data`` construction, so this per-cell
    # encode — and its dict copy — only runs for direct callers whose namespace
    # lacks the column. Materialized only when a field declares a companion, so
    # a config written before #410 takes the code path it always did.
    if n_obs and TOC_WORD_COLUMN not in cell_data and _temporal_fields(agg_fields):
        cell_data = {**cell_data, TOC_WORD_COLUMN: _toc_word_column(cell_data, config)}
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

    # Values are scalars, arrays, or (payload, locations) pairs for located
    # ragged fields (issue #87).
    result: dict[str, Any] = {}
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
        # returns the raw result as a numpy array — the ragged writer receives it
        # as a variable-length per-cell payload.
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
                result[name] = _coerce_scalar_value(out, sig, name)
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
                # Cached code object, not the source string: this runs per cell.
                resolved_params[pkey] = eval(_compile_param_expr(pval), ns)  # noqa: S307
            else:
                resolved_params[pkey] = pval

        func = resolve_function(func_name)

        # Companion-carrying ragged field: hand the reducer the named
        # per-observation morton column (``location:``, issue #87) and/or the
        # derived toc word column (``temporal: per-centroid``, spec §8.3, issue
        # #410), and accept one extra tuple element per declared channel — the
        # uint64 words ride beside the payload into the ``{field}_locations`` /
        # ``{field}_times`` sibling vlen arrays. Only the HEALPix read path
        # supplies ``leaf_id``, so a missing column is a grid/config mismatch,
        # reported clearly.
        per_centroid = sig["temporal"] == TOC_SHAPE_PER_CENTROID
        if sig["kind"] == "ragged" and (sig["location"] is not None or per_centroid):
            channels = {}
            if sig["location"] is not None:
                loc_col = sig["location"]
                if loc_col not in cell_data:
                    raise ValueError(
                        f"ragged field {name!r} declares location: {loc_col!r} but that "
                        f"column is not in the cell data (available: {sorted(cell_data)}); "
                        f"per-observation mortons require a HEALPix grid"
                    )
                channels["locations"] = cell_data[loc_col]
            if per_centroid:
                channels["temporal"] = cell_data[TOC_WORD_COLUMN]
            payload, *words = func(values, **channels, **resolved_params)
            payload = _coerce_ragged_value(payload, sig)
            out = []
            for label, channel in zip(channels, words, strict=True):
                # Must not copy: under ``batched_companion_folds`` (issue #476) the
                # reducer hands back a contiguous uint64 placeholder the batch fills
                # in place at the flush, so this normalization has to be identity —
                # which it is for the contiguous uint64 vectors the build_tdigest
                # family returns. A copy here would strand the flush on an orphan
                # (see ``_CompanionBatch``).
                channel = np.ascontiguousarray(np.asarray(channel))
                if channel.dtype != np.uint64:
                    # A silent uint64 cast would wrap negative/float garbage into
                    # plausible-looking morton or toc words; require the reducer to
                    # return uint64 outright (as build_tdigest does).
                    raise ValueError(
                        f"ragged field {name!r}: {label} dtype {channel.dtype} is not "
                        f"uint64; the reducer must return packed words"
                    )
                if channel.shape != (payload.shape[0],):
                    raise ValueError(
                        f"ragged field {name!r}: {label} shape {channel.shape} does not "
                        f"match the payload's {payload.shape[0]} elements"
                    )
                out.append(channel)
            result[name] = (payload, *out)
            continue

        out = func(values, **resolved_params)
        # Scalar fields stay byte-for-byte identical to the pre-#29 path; a
        # declared ``vector`` field coerces to its trailing_shape (issue #29); a
        # ``ragged`` field (issue #48) returns a variable-length numpy array that
        # the ragged writer later encodes as the cell's vlen-bytes payload.
        if sig["kind"] == "vector":
            result[name] = _coerce_field_value(out, sig)
        elif sig["kind"] == "ragged":
            result[name] = _coerce_ragged_value(out, sig)
        else:
            result[name] = _coerce_scalar_value(out, sig, name)

    return result


def _coerce_scalar_value(out, sig: dict, name: str = "<field>"):
    """Coerce one scalar field result per its declared dtype.

    Float dtypes keep the pre-#29 ``float()`` round-trip byte-for-byte. An
    integer dtype coerces via ``int()`` instead: a float64 round-trip corrupts
    exact integers above 2**53, which a packed 64-bit lane word (the issue
    #321 composition field) legitimately exceeds.

    An integer declaration is **verified, not trusted**: a reducer returning a
    fractional value into an integer field is a config/reducer mismatch, and
    bare ``int()`` would silently truncate it toward zero (``2.7 -> 2``). Python
    and numpy integers pass through; floats pass only when they carry no
    fractional part (``np.float64(3.0)`` is a legitimate reducer return); a
    fractional value raises naming the field, its dtype, and the value — the
    same fail-fast shape as :func:`_coerce_field_value`'s trailing-shape check.
    """
    dtype = sig.get("dtype")
    if dtype is not None and np.issubdtype(np.dtype(dtype), np.integer):
        if not isinstance(out, (int, np.integer)):
            # Anything else (float, np.floating, 0-d array) must be integral;
            # ``float()`` first so a 0-d array or np scalar answers uniformly.
            if not float(out).is_integer():
                raise ValueError(
                    f"scalar field {name!r}: declared dtype {np.dtype(dtype)} but the "
                    f"reducer returned {out!r}, which is not integral — an integer field "
                    f"cannot store a fractional value (declare a float dtype, or round "
                    f"in the reducer)"
                )
        return int(out)
    return float(out)


def _empty_cell_value(meta: dict):
    """Value emitted for a single agg field when its cell has no observations.

    Scalar fields keep the pre-#29 contract: ``0`` for ``len``/``count``,
    ``np.nan`` otherwise. A ``vector`` field (issue #29) instead gets a full
    ``trailing_shape`` array filled with its schema-declared sentinel
    (:func:`_field_sentinel`), so empty and populated cells emit the same shape.
    A ``ragged`` field (issue #48) returns an empty list ``[]`` — the ragged writer
    handles absent cells by leaving them out of ``cell_ids``. A companion-carrying
    ragged field returns an empty tuple of the same arity its reducer would —
    ``(payload, *channels)`` — so direct callers can always unpack the same shape
    (issue #87's located pair, extended by spec §8.3's temporal channel).
    """
    sig = get_output_signature(meta)
    if sig["kind"] == "ragged":
        channels = (sig["location"] is not None) + (sig["temporal"] == TOC_SHAPE_PER_CENTROID)
        if channels:
            dtype = np.dtype(sig["dtype"]) if sig["dtype"] is not None else np.dtype("float32")
            return (
                np.empty((0, *sig["inner_shape"]), dtype=dtype),
                *(np.empty(0, dtype=np.uint64) for _ in range(channels)),
            )
        return []
    if sig["kind"] == "vector":
        dtype = np.dtype(sig["dtype"]) if sig["dtype"] is not None else np.dtype("float32")
        sentinel = (
            _integer_fill(meta, dtype)
            if np.issubdtype(dtype, np.integer)
            else _field_sentinel(meta)
        )
        return np.full(sig["trailing_shape"], sentinel, dtype=dtype)
    if meta.get("function") in ("len", "count"):
        return 0
    # An integer-dtype scalar cannot hold NaN; empty cells take the field's
    # declared fill_value (issue #321 — e.g. the packed composition word's 0).
    dtype_str = sig["dtype"]
    if dtype_str is not None and np.issubdtype(np.dtype(dtype_str), np.integer):
        return _integer_fill(meta, dtype_str)
    return np.nan


def _coerce_field_value(value, sig: dict) -> np.ndarray:
    """Coerce a ``vector`` field's aggregation output to its declared signature.

    The field's ``function`` or ``expression`` must yield exactly
    ``trailing_shape`` values (issue #29 Tier-1 fixed-width vectors; ragged
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
    array of the declared dtype (default ``float32``), ready for the ragged writer.

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


def _pool_chunk_columns(
    col_arrays: dict[str, np.ndarray],
    cell_to_slice: dict[int, tuple[int, int]],
    chunk_children,
) -> dict[str, np.ndarray]:
    """Pool a single chunk's observations from the shard's sorted column arrays.

    The shard is read+grouped ONCE (``col_arrays`` sorted by cell id,
    ``cell_to_slice`` mapping each populated cell to its ``(start, end)`` slice);
    this gathers only the rows belonging to ``chunk_children`` so a per-chunk
    reduction (e.g. :func:`_eval_chunk_precompute`, issue #82 phase 6) sees just
    that Zarr chunk's observations rather than the whole shard's.

    Cells of ``chunk_children`` absent from ``cell_to_slice`` are empty and
    contribute no rows. The gather index is built once and reused across every
    column, so the cost is one fancy-index per column over the chunk's rows. An
    empty chunk (no populated cells) yields length-0 arrays of each column's dtype;
    :func:`_eval_chunk_precompute` short-circuits that ``n_obs == 0`` case to NaN
    anchors (``np.min``/``np.nanmin`` over an empty array would otherwise raise).

    Parameters
    ----------
    col_arrays : dict[str, np.ndarray]
        Shard column arrays, sorted in ascending cell-id order (from
        :func:`_concat_and_group` / :func:`_group_columns`).
    cell_to_slice : dict[int, tuple[int, int]]
        Maps each populated cell id to its ``(start, end)`` slice into
        ``col_arrays``.
    chunk_children : sequence of int
        The chunk's cell ids (canonical order).

    Returns
    -------
    dict[str, np.ndarray]
        ``{name: ndarray}`` holding only this chunk's rows, in the shard's sorted
        order (concatenated child-slice by child-slice).
    """
    slices = []
    for child in np.asarray(chunk_children):
        sl = cell_to_slice.get(int(child))
        if sl is not None:
            slices.append(sl)
    if not slices:
        # Empty chunk: length-0 view per column (dtype-preserving). The per-chunk
        # reduction (``_eval_chunk_precompute``) detects this n_obs==0 case and
        # returns NaN anchors rather than raising on ``np.min`` of an empty array.
        return {col: arr[:0] for col, arr in col_arrays.items()}
    if len(slices) == 1:
        start, end = slices[0]
        return {col: arr[start:end] for col, arr in col_arrays.items()}
    # Build the gather index once (the slices are disjoint and already in sorted
    # order) and reuse it across every column.
    idx = np.concatenate([np.arange(start, end) for start, end in slices])
    return {col: arr[idx] for col, arr in col_arrays.items()}


def _aggregate_chunk_cells(
    children,
    col_arrays: dict,
    cell_to_slice: dict,
    chunk_scalars: dict,
    config: PipelineConfig,
    data_vars,
    agg_fields: dict,
    chunk_pooled: dict | None = None,
):
    """Compute per-cell stats for one chunk's ``children`` (default numpy path).

    The per-cell aggregation loop, lifted out of ``process_shard`` so the
    multi-chunk-per-worker path (issue #30 item 3) can call it once per finer
    chunk. ``children`` are the chunk's cell ids in canonical order; the pooled
    ``col_arrays``/``cell_to_slice`` (grouped once over the whole shard) and the
    shard-level ``chunk_scalars`` are shared across chunks. At K==1 ``children`` is
    the whole shard's, so this is byte-for-byte the old single-chunk loop.

    Returns ``(stats_arrays, ragged_payloads, ragged_cell_indices,
    ragged_channels, cells_with_data)``: dense fields preallocated to
    ``(n_cells, *trailing_shape)`` and filled per cell; ragged fields collected
    as ``(payloads, cell_indices)`` keyed by the cell's position in ``children``
    (the chunk-local index the ragged writer expects). ``ragged_channels`` holds
    the per-cell uint64 companion vectors for the fields declaring one —
    ``{field: {channel: [per-cell words]}}``, the channels being ``locations``
    (issue #87) and ``times`` (spec §8.3, issue #410), each index-aligned with
    that field's payloads. A field declaring no companion has no entry, so a
    config written before either channel produces the same empty mapping.

    ``chunk_pooled`` is the caller's :func:`_pool_chunk_columns` output for these
    same ``children`` (both worker call sites build it for the chunk precompute
    anyway); it lets the toc hoist reuse that gather instead of rebuilding it.
    """
    children = np.asarray(children)
    n_cells = len(children)
    stats_arrays: dict = {}
    ragged_payloads: dict[str, list] = {}
    ragged_cell_indices: dict[str, list[int]] = {}
    ragged_channels: dict[str, dict[str, list]] = {}
    for name in data_vars:
        meta = agg_fields[name]
        sig = get_output_signature(meta)
        if sig["kind"] == "ragged":
            ragged_payloads[name] = []
            ragged_cell_indices[name] = []
            # Channel order is the kernel's fixed return order (locations, then
            # temporal), which is what lets the reducer's extra tuple elements be
            # zipped onto these keys positionally below.
            declared = {}
            if sig["location"] is not None:
                declared["locations"] = []
            if sig["temporal"] == TOC_SHAPE_PER_CENTROID:
                declared["times"] = []
            if declared:
                ragged_channels[name] = declared
            continue
        # Vector fields (issue #29) get a per-cell (n_cells, *trailing_shape) block;
        # scalars keep the 1-D (n_cells,) layout, unchanged.
        shape = (n_cells, *sig["trailing_shape"])
        zarr_dtype = np.dtype(meta.get("dtype", "float32"))
        fill_value = meta.get("fill_value", "NaN")
        if fill_value == "NaN":
            stats_arrays[name] = np.full(shape, np.nan, dtype=zarr_dtype)
        else:
            stats_arrays[name] = np.zeros(shape, dtype=zarr_dtype)

    _empty: dict[str, np.ndarray] = {col: arr[:0] for col, arr in col_arrays.items()}

    # Per-chunk toc encode (issue #476): when a field declares a temporal
    # companion, encode the whole chunk's words in one pass and hand each
    # populated cell its slice at namespace construction — the per-cell encode
    # (and its dict copy) in ``calculate_cell_statistics`` then never runs on
    # this path. Both callers (the worker's pooled loop and the spill
    # read-back, which is per chunk by construction) get the hoist here, so
    # chunk boundaries are respected at any chunks_per_shard.
    cell_toc = (
        _chunk_toc_words(col_arrays, cell_to_slice, children, config, pooled=chunk_pooled)
        if _temporal_fields(agg_fields)
        else None
    )

    # Batch the per-centroid companion folds across the loop (issue #476): each
    # ``build_tdigest`` inside defers its ``toc_reduce``/``common_ancestor``
    # partition and the context exit runs ONE fold per channel over the whole
    # chunk, filling the collected vectors in place — byte-identically, before
    # anything reads them (the ragged writer runs after this returns). Armed only
    # when a field actually declares a companion channel, so a config predating
    # issue #87/#410 never sees the ContextVar and an arbitrary config-resolved
    # reducer can only meet a placeholder on the configs that ask for one.
    from contextlib import nullcontext

    from zagg.stats.tdigest import batched_companion_folds

    cells_with_data = 0
    with batched_companion_folds() if ragged_channels else nullcontext():
        for i, child_morton in enumerate(children):
            child_key = int(child_morton)
            if child_key in cell_to_slice:
                start, end = cell_to_slice[child_key]
                cell_data: dict[str, np.ndarray] = {
                    col: arr[start:end] for col, arr in col_arrays.items()
                }
                if cell_toc is not None:
                    cell_data[TOC_WORD_COLUMN] = cell_toc[child_key]
                cells_with_data += 1
            else:
                cell_data = _empty
            # Inject the chunk-level scalars into this cell's namespace (no-op when
            # empty, so non-precompute configs are unchanged).
            cell_namespace: dict[str, Any] = (
                {**cell_data, **chunk_scalars} if chunk_scalars else cell_data
            )
            stats = calculate_cell_statistics(
                cell_namespace, value_col="h_li", sigma_col="s_li", config=config
            )
            for key, value in stats.items():
                if key in ragged_payloads:
                    # Ragged field: collect non-empty payloads with their chunk-local
                    # cell index. Empty cells (``_empty_cell_value`` -> []) are skipped.
                    # A companion-carrying field delivers ``(payload, *channel words)``
                    # in the kernel's fixed order; the words are collected
                    # index-aligned with the payloads.
                    if isinstance(value, tuple):
                        payload, *words = value
                    else:
                        payload, words = np.asarray(value), []
                    if payload.size == 0:
                        continue
                    # Fail fast if the value's arity disagrees with the declared
                    # signature (empty cells are exempt, having been skipped above) —
                    # a silent mismatch would surface much later as a length error in
                    # the ragged writer, or as a companion silently dropped.
                    channels = ragged_channels.get(key, {})
                    if len(channels) != len(words):
                        raise ValueError(
                            f"ragged field {key!r}: the reducer returned {len(words)} "
                            f"companion channel(s) but the declared signature has "
                            f"{len(channels)} ({sorted(channels) or 'none'})"
                        )
                    ragged_payloads[key].append(payload)
                    ragged_cell_indices[key].append(i)
                    for channel, word_vector in zip(channels.values(), words, strict=True):
                        channel.append(word_vector)
                else:
                    stats_arrays[key][i] = value

    return stats_arrays, ragged_payloads, ragged_cell_indices, ragged_channels, cells_with_data
