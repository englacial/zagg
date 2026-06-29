"""Compatibility wrappers for HEALPix Zarr template emission.

The actual implementation lives on ``zagg.grids.HealpixGrid``. These functions
preserve the pre-refactor public API: pass ``n_parent_cells`` for dense pack,
omit it for full sphere.
"""

from __future__ import annotations

import warnings

from pydantic_zarr.experimental.v3 import GroupSpec
from typing_extensions import NotRequired, TypedDict
from zarr.abc.store import Store

from zagg.config import PipelineConfig
from zagg.grids.healpix import HEALPIX_BASE_CELLS, HealpixGrid


class ProcessingMetadata(TypedDict):
    shard_key: int
    cells_with_data: int
    total_obs: int
    granule_count: int
    files_processed: int
    duration_s: float
    error: str | None
    # Peak resident memory (RSS) of the worker process in MB. Stamped by the
    # Lambda handler from ``resource.getrusage`` after the write phase (issue
    # #120); absent on the local runner path, hence ``NotRequired``.
    max_memory_mb: NotRequired[float]
    # Per-phase wall timings (read/index/aggregate/write), present only when the
    # worker is dispatched with ``profile=True`` (issue #100).
    phase_timings: NotRequired[dict[str, float]]
    # Count of per-group reads that raised during the read loop (issue #116).
    # Present (and non-zero) only when at least one group read failed; a raised
    # read is always a real error, so this surfaces a shard whose "no data"
    # result is actually a read failure rather than a legitimately-empty read.
    read_errors: NotRequired[int]


def xdggs_spec(
    parent_order: int,
    child_order: int,
    config: PipelineConfig | None = None,
) -> GroupSpec:
    """Return the full-sphere HEALPix GroupSpec (back-compat wrapper)."""
    return HealpixGrid(
        parent_order=parent_order,
        child_order=child_order,
        layout="fullsphere",
        config=config,
    ).spec()


def xdggs_zarr_template(
    store: Store,
    parent_order: int,
    child_order: int,
    n_parent_cells: int | None = None,
    overwrite: bool = False,
    config: PipelineConfig | None = None,
) -> Store:
    """Write a HEALPix Zarr template to ``store``.

    Layout is selected by ``n_parent_cells``: when ``None`` the store gets a
    full-sphere array of shape ``(12 · 4^child_order,)``; when set the store
    gets a dense-pack array of shape ``(4^Δ · n_parent_cells,)``.

    Parameters
    ----------
    store : Store
        Zarr-compatible store.
    parent_order : int
        Parent (shard) HEALPix order.
    child_order : int
        Leaf HEALPix order. Must be ``>= parent_order``.
    n_parent_cells : int, optional
        Number of populated shards. Selects dense layout when provided.
    overwrite : bool, optional
        Overwrite an existing array or group at the path.
    config : PipelineConfig, optional
        Pipeline configuration. Falls back to ``default_config("atl06")``.
    """
    if n_parent_cells is not None and n_parent_cells <= 0:
        raise ValueError(f"n_parent_cells must be positive, got {n_parent_cells}")
    if n_parent_cells is None:
        grid = HealpixGrid(
            parent_order=parent_order,
            child_order=child_order,
            layout="fullsphere",
            config=config,
        )
    else:
        warnings.warn(
            "xdggs_zarr_template(n_parent_cells=...) produces a deprecated "
            "dense-pack layout; omit n_parent_cells for fullsphere (the new "
            "default).",
            DeprecationWarning,
            stacklevel=2,
        )
        # Synthetic shard identities — emit_template only needs the count.
        grid = HealpixGrid(
            parent_order=parent_order,
            child_order=child_order,
            layout="dense",
            config=config,
            populated_shards=list(range(n_parent_cells)),
        )
    return grid.emit_template(store, overwrite=overwrite)


__all__ = [
    "HEALPIX_BASE_CELLS",
    "ProcessingMetadata",
    "xdggs_spec",
    "xdggs_zarr_template",
]
