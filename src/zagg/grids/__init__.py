"""Output grid implementations."""

from __future__ import annotations

import warnings

from zagg.config import (
    PipelineConfig,
    get_child_order,
    get_sharded,
)
from zagg.grids.base import InconsistentShardError, OutputGrid, ShardKey
from zagg.grids.healpix import HEALPIX_BASE_CELLS, HealpixGrid
from zagg.grids.rectilinear import OOB_SENTINEL, RectilinearGrid


def from_config(
    config: PipelineConfig,
    *,
    parent_order: int | None = None,
) -> OutputGrid:
    """Construct an OutputGrid from a pipeline config.

    Parameters
    ----------
    config : PipelineConfig
        Pipeline config providing ``output.grid``.
    parent_order : int, optional
        Shard order for HEALPix (typically from the catalog metadata).
        Ignored for non-HEALPix grids.
    """
    grid_cfg = config.output.get("grid", {})
    grid_type = grid_cfg.get("type", "healpix")
    if grid_type == "healpix":
        # Deprecation (issue #253, mirroring the retired dense warning): the
        # HEALPix flat/fullsphere store remains for interop/debug but hive is
        # the default — uniformly, raster included (issue #247 made raster +
        # hive the real write path); removal is gated on the sparse-DGGS read
        # path (#251 phase 3).
        explicit_layout = grid_cfg.get("layout")
        if config.output.get("store_layout") == "flat":
            warnings.warn(
                "output.store_layout: flat is deprecated for HEALPix; hive is "
                "the default. Flat remains for interop/debug and will be "
                "removed once the sparse-DGGS read path lands (issue #251 "
                "phase 3).",
                DeprecationWarning,
                stacklevel=2,
            )
        if explicit_layout is not None:
            warnings.warn(
                "output.grid.layout: fullsphere is deprecated for HEALPix; "
                "hive (the default store_layout) does not use it. Omit the "
                "key — the flat/fullsphere store it selects is removed once "
                "the sparse-DGGS read path lands (issue #251 phase 3).",
                DeprecationWarning,
                stacklevel=2,
            )
        layout = explicit_layout or "fullsphere"
        # Grid is fully defined by the config (single source of truth); the
        # parent_order kwarg is only a fallback for legacy callers.
        resolved_parent = grid_cfg.get("parent_order", parent_order)
        if resolved_parent is None:
            raise ValueError("output.grid.parent_order is required for HEALPix grids")
        # issue #215: HEALPix output defaults to sharded — a missing flag should
        # not silently cost the ~K-fold object blow-up (one object per inner
        # chunk instead of one per shard). Both layouts (issue #236): flat
        # bundles a shard's K inner chunks into one ShardingCodec object per
        # dispatch shard; a hive leaf bundles them into one object per array.
        # The grid no-ops sharding when K==1, so single-chunk grids stay
        # unaffected.
        child_order = get_child_order(config)
        sharded = get_sharded(config, default=True)
        chunk_inner = grid_cfg.get("chunk_inner")
        # Raster (per-timestep (time, cells) slab writes) never shards — a
        # permanent exclusion (issue #247, guarded in config._validate_raster):
        # a ShardingCodec object would be read-modify-written once per timestep.
        # So the #259 derivation below must not engage for raster: an omitted
        # chunk_inner has to stay K==1 / unsharded (its pre-#259 behavior), else
        # the derived inner chunk shards the grid and the raster template emit
        # rejects it (raster.py). The config's `sharded: true` guard fires only on
        # an *explicit* flag; the derivation is downstream of it, so gate here.
        is_raster = (config.data_source or {}).get("reader") == "raster"
        # issue #259: with sharding on (the default) but chunk_inner omitted, the
        # grid used to fall to chunk_order == parent_order — K==1 — and silently
        # disable sharding, so a config that only set `sharded: true` still paid a
        # whole-shard object fetch per single-cell read. Derive the 64x64
        # (= 4^6 = 4096-cell) inner chunk the flagship configs hand-set as
        # `chunk_inner: 13` — chunk_order = child_order - 6, engaged only when it
        # exceeds parent_order. When child_order - 6 <= parent_order the shard
        # already spans <= one inner chunk, so leave it None (the value would just
        # reproduce the K==1 no-op). (child_order - 6 <= child_order always, so no
        # upper bound is needed — the constructor's parent<=chunk<=child holds.)
        # Gate on fullsphere (the sole supported geometry since issue #88 removed
        # dense) so any other layout falls straight through to the constructor's
        # "Unknown layout" error rather than deriving first.
        if chunk_inner is None and sharded and layout == "fullsphere" and not is_raster:
            derived = child_order - 6
            if derived > resolved_parent:
                chunk_inner = derived
        return HealpixGrid(
            parent_order=resolved_parent,
            child_order=child_order,
            layout=layout,
            config=config,
            chunk_inner=chunk_inner,
            sharded=sharded,
        )
    if grid_type == "rectilinear":
        required = ("crs", "resolution", "bounds")
        missing = [k for k in required if k not in grid_cfg]
        if missing:
            raise ValueError(f"output.grid type 'rectilinear' missing required fields: {missing}")
        chunk_inner = grid_cfg.get("chunk_inner")
        return RectilinearGrid(
            crs=grid_cfg["crs"],
            resolution=grid_cfg["resolution"],
            bounds=grid_cfg["bounds"],
            chunk_shape=tuple(grid_cfg.get("chunk_shape", (256, 256))),
            config=config,
            chunk_inner=tuple(chunk_inner) if chunk_inner is not None else None,
            sharded=get_sharded(config),
        )
    raise ValueError(f"Unknown output.grid.type: {grid_type!r}")


def validate_compatible(grids: list) -> None:
    """Validate that output grids can nest into mutually aligned data cubes.

    This is the grid-compatibility core of the future multi-product validator
    (#24 function 3): every pair of grids must ``nests_with`` the other — same
    family, aligned origins, whole-number resolution ratios. Cross-family
    (HEALPix vs rectilinear) is rejected.

    Note
    ----
    Spatial-coverage overlap (do the products actually share a region) is not
    yet checked — that needs the Catalogs/regions and is deferred until the
    multi-product aggregation API lands.

    Parameters
    ----------
    grids : list of OutputGrid
        Grids that must be mutually compatible.

    Raises
    ------
    ValueError
        If any pair of grids does not nest.
    """
    for i, a in enumerate(grids):
        for b in grids[i + 1 :]:
            if not (a.nests_with(b) and b.nests_with(a)):
                raise ValueError(
                    f"incompatible grids (do not nest):\n  {a.signature()}\n  {b.signature()}"
                )


__all__ = [
    "OutputGrid",
    "ShardKey",
    "InconsistentShardError",
    "HealpixGrid",
    "HEALPIX_BASE_CELLS",
    "RectilinearGrid",
    "OOB_SENTINEL",
    "from_config",
    "validate_compatible",
]
