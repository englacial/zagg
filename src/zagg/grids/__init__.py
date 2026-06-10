"""Output grid implementations."""
from __future__ import annotations

import warnings

from zagg.config import PipelineConfig, get_child_order
from zagg.grids.base import InconsistentShardError, OutputGrid, ShardKey
from zagg.grids.healpix import HEALPIX_BASE_CELLS, HealpixGrid
from zagg.grids.rectilinear import OOB_SENTINEL, RectilinearGrid


def from_config(
    config: PipelineConfig,
    *,
    parent_order: int | None = None,
    populated_shards: list | None = None,
) -> OutputGrid:
    """Construct an OutputGrid from a pipeline config.

    Parameters
    ----------
    config : PipelineConfig
        Pipeline config providing ``output.grid``.
    parent_order : int, optional
        Shard order for HEALPix (typically from the catalog metadata).
        Ignored for non-HEALPix grids.
    populated_shards : list, optional
        Required for dense-layout HEALPix; ignored otherwise.
    """
    grid_cfg = config.output.get("grid", {})
    grid_type = grid_cfg.get("type", "healpix")
    if grid_type == "healpix":
        explicit_layout = grid_cfg.get("layout")
        if explicit_layout == "dense":
            warnings.warn(
                "output.grid.layout: dense is deprecated; switch to fullsphere "
                "(the new default) or omit the field. Dense will be removed in "
                "a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
        layout = explicit_layout or "fullsphere"
        # Grid is fully defined by the config (single source of truth); the
        # parent_order kwarg is only a fallback for legacy callers.
        resolved_parent = grid_cfg.get("parent_order", parent_order)
        if resolved_parent is None:
            raise ValueError("output.grid.parent_order is required for HEALPix grids")
        return HealpixGrid(
            parent_order=resolved_parent,
            child_order=get_child_order(config),
            layout=layout,
            config=config,
            populated_shards=populated_shards,
        )
    if grid_type == "rectilinear":
        required = ("crs", "resolution", "bounds")
        missing = [k for k in required if k not in grid_cfg]
        if missing:
            raise ValueError(
                f"output.grid type 'rectilinear' missing required fields: {missing}"
            )
        return RectilinearGrid(
            crs=grid_cfg["crs"],
            resolution=grid_cfg["resolution"],
            bounds=grid_cfg["bounds"],
            chunk_shape=tuple(grid_cfg.get("chunk_shape", (256, 256))),
            config=config,
        )
    raise ValueError(f"Unknown output.grid.type: {grid_type!r}")


__all__ = [
    "OutputGrid",
    "ShardKey",
    "InconsistentShardError",
    "HealpixGrid",
    "HEALPIX_BASE_CELLS",
    "RectilinearGrid",
    "OOB_SENTINEL",
    "from_config",
]
