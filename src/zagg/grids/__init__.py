"""Output grid implementations."""
from __future__ import annotations

from zagg.config import PipelineConfig, get_child_order
from zagg.grids.base import InconsistentShardError, OutputGrid, ShardKey
from zagg.grids.healpix import HEALPIX_BASE_CELLS, HealpixGrid


def from_config(
    config: PipelineConfig,
    *,
    parent_order: int,
    populated_shards: list | None = None,
) -> OutputGrid:
    """Construct an OutputGrid from a pipeline config.

    Parameters
    ----------
    config : PipelineConfig
        Pipeline config providing ``output.grid.{type, child_order, layout}``.
    parent_order : int
        Shard order (typically from the catalog metadata).
    populated_shards : list, optional
        Required for dense-layout HEALPix; ignored for other layouts.
    """
    grid_cfg = config.output.get("grid", {})
    grid_type = grid_cfg.get("type", "healpix")
    if grid_type == "healpix":
        layout = grid_cfg.get("layout", "dense")
        return HealpixGrid(
            parent_order=parent_order,
            child_order=get_child_order(config),
            layout=layout,
            config=config,
            populated_shards=populated_shards,
        )
    raise ValueError(f"Unknown output.grid.type: {grid_type!r}")


__all__ = [
    "OutputGrid",
    "ShardKey",
    "InconsistentShardError",
    "HealpixGrid",
    "HEALPIX_BASE_CELLS",
    "from_config",
]
