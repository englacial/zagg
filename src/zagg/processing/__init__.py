"""
Cloud-agnostic processing functions for aggregating HDF5 data.

This module contains the core processing logic that can be used across different
cloud platforms or local processing environments.

It is a package (split out of a single ~2000-line ``processing.py`` for the §4
size limit) whose stages are:

* :mod:`zagg.processing.read` — read + spatially filter HDF5 groups for a shard.
* :mod:`zagg.processing.aggregate` — per-cell statistics, grouping, the per-chunk
  precompute hook, and the EXPERIMENTAL pyarrow kernel reducer.
* :mod:`zagg.processing.write` — assemble + write the output carrier to Zarr.
* :mod:`zagg.processing.worker` — ``process_shard`` orchestration.

This ``__init__`` re-exports every public + previously-importable name, so
``zagg.processing.<name>`` (and ``monkeypatch.setattr("zagg.processing.<name>",
...)``) stays byte-for-byte stable across the relocation.
"""

# ``h5coro`` is referenced as ``zagg.processing.h5coro.H5Coro`` by the worker (and
# patched there in tests); keep it bound on the package so both resolve.
import h5coro

from zagg.processing.aggregate import (
    _KERNEL_FUNCS,
    KERNEL_RTOL,
    _build_groups,
    _coerce_field_value,
    _coerce_ragged_value,
    _concat_and_group,
    _empty_cell_value,
    _eval_chunk_precompute,
    _field_sentinel,
    _group_columns,
    _has_ragged_fields,
    _has_vector_fields,
    _kernel_able,
    _kernel_aggregate,
    calculate_cell_statistics,
)
from zagg.processing.read import (
    _COMPARE,
    _broadcast_segment_to_base,
    _expand_mask_to_base,
    _level_coord_paths,
    _make_url_rewriter,
    _planned_read_group,
    _predicate_mask,
    _read_group,
    _read_group_full,
    _read_segment_broadcasts,
    _segment_level_variables,
)

# ``worker`` imports this package (``import zagg.processing as _processing``) but
# only resolves ``_processing._read_group`` / ``_make_url_rewriter`` / ``h5coro``
# at call time, so the helper names above are already bound by the time
# ``process_shard`` runs — the import order here is immaterial to correctness.
from zagg.processing.worker import process_morton_cell, process_shard
from zagg.processing.write import (
    _arrow_column,
    _build_output,
    _carrier_empty,
    _chunk_resolution_fields,
    _chunk_uniform_value,
    _iter_carrier_columns,
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
)

# The four public entry points the package commits to, plus every previously
# importable private helper — listed so ``zagg.processing.<name>`` stays stable
# across the relocation (mirrors ``zagg.grids.__init__``'s re-export convention,
# and is what lets ruff accept these as intentional re-exports, not dead F401s).
__all__ = [
    # public surface
    "KERNEL_RTOL",
    # bound on the package so ``zagg.processing.h5coro.H5Coro`` resolves (and
    # stays monkeypatch-able) from the worker
    "h5coro",
    "calculate_cell_statistics",
    "process_morton_cell",
    "process_shard",
    "write_dataframe_to_zarr",
    "write_ragged_to_zarr",
    # aggregate-stage helpers
    "_KERNEL_FUNCS",
    "_build_groups",
    "_coerce_field_value",
    "_coerce_ragged_value",
    "_concat_and_group",
    "_empty_cell_value",
    "_eval_chunk_precompute",
    "_field_sentinel",
    "_group_columns",
    "_has_ragged_fields",
    "_has_vector_fields",
    "_kernel_able",
    "_kernel_aggregate",
    # read-stage helpers
    "_COMPARE",
    "_broadcast_segment_to_base",
    "_expand_mask_to_base",
    "_level_coord_paths",
    "_make_url_rewriter",
    "_planned_read_group",
    "_predicate_mask",
    "_read_group",
    "_read_group_full",
    "_read_segment_broadcasts",
    "_segment_level_variables",
    # write-stage helpers
    "_arrow_column",
    "_build_output",
    "_carrier_empty",
    "_chunk_resolution_fields",
    "_chunk_uniform_value",
    "_iter_carrier_columns",
]
