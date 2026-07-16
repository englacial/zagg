"""Compatibility wrappers for HEALPix Zarr template emission.

The actual implementation lives on ``zagg.grids.HealpixGrid``. These functions
preserve the pre-refactor public API (fullsphere only; the deprecated
``n_parent_cells`` dense pack was removed — issue #88).
"""

from __future__ import annotations

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
    # Container telemetry (issue #171), stamped by the Lambda handler's
    # dispatcher into every per-unit envelope (all status branches) so the
    # runner can surface the warm-container RSS ratchet (#169). Absent on the
    # local runner path. ``container_cold``: first invocation on this sandbox.
    # ``container_generation``: invocations this sandbox has served (all
    # modes). ``rss_start_mb``: process RSS at handler entry -- the ratchet
    # signal (None off Linux). ``sandbox_id``: CloudWatch log-stream name,
    # unique per sandbox. ``container_init_ts``: module-import epoch seconds.
    # The per-invocation *peak* is the existing ``max_memory_mb`` (issue #141).
    container_cold: NotRequired[bool]
    container_generation: NotRequired[int]
    rss_start_mb: NotRequired[float | None]
    sandbox_id: NotRequired[str | None]
    container_init_ts: NotRequired[float]


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
    """Write a full-sphere HEALPix Zarr template to ``store``.

    The array has shape ``(12 · 4^child_order,)``.

    Parameters
    ----------
    store : Store
        Zarr-compatible store.
    parent_order : int
        Parent (shard) HEALPix order.
    child_order : int
        Leaf HEALPix order. Must be ``>= parent_order``.
    n_parent_cells : int, optional
        Removed. Passing a value raises — the dense-pack layout it selected
        was removed (issue #88).
    overwrite : bool, optional
        Overwrite an existing array or group at the path.
    config : PipelineConfig, optional
        Pipeline configuration. Falls back to ``default_config("atl06")``.
    """
    if n_parent_cells is not None:
        raise ValueError(
            "xdggs_zarr_template(n_parent_cells=...) selected the dense-pack "
            "layout, which was removed (issue #88); omit n_parent_cells for "
            "the fullsphere template"
        )
    grid = HealpixGrid(
        parent_order=parent_order,
        child_order=child_order,
        layout="fullsphere",
        config=config,
    )
    return grid.emit_template(store, overwrite=overwrite)


__all__ = [
    "HEALPIX_BASE_CELLS",
    "ProcessingMetadata",
    "xdggs_spec",
    "xdggs_zarr_template",
]
