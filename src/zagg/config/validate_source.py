"""The raster pipeline's cross-block checks (issue #677 phase 1).

The ``data_source`` grammar itself lives in :mod:`zagg.config.models_source`;
what remains here is the raster kind's coupling to the other blocks.
"""

from zagg.config.core import PipelineConfig


def _validate_raster_config(config: PipelineConfig) -> None:
    """Validate a raster (pull-NN) pipeline config (issue #218).

    ``data_source.bands`` maps output field name -> ``{asset, dtype, ...}``:
    ``asset`` names the STAC asset key holding that band's GeoTIFF, ``dtype``
    is required (bands store exact source DNs — no float default), and
    optional ``fill_value`` (default 0), ``scale``/``offset`` (recorded as
    ``scale_factor``/``add_offset`` array attrs, never applied to the data).
    ``data_source.nodata`` (optional scalar) marks source nodata; a cell
    whose sampled pixel equals it in any band is left at fill.
    """
    for section in ("data_source", "output"):
        if not getattr(config, section):
            raise ValueError(f"Missing required section: {section}")
    # ``bands``/``nodata``/``shard_workers``/``write_buffer`` shape is the
    # raster model's (``models_source.RasterDataSource``, issue #677 phase 1).
    grid = config.output.get("grid")
    if not isinstance(grid, dict) or grid.get("type") != "healpix":
        raise ValueError(
            "raster pipelines currently require output.grid.type: healpix "
            "(the rectilinear (time, y, x) template is future work — issue #218)"
        )
    for key in ("parent_order", "child_order"):
        if key not in grid:
            raise ValueError(f"output.grid.{key} is required for healpix grid")
    if grid.get("sharded"):
        # Permanent exclusion, not a deferral (espg-ratified on issue #247):
        # per-timestep slab streaming over a ShardingCodec object would
        # read-modify-write it once per timestep, and raster object count is
        # time-axis-dominated anyway — sharding the cell axis buys little.
        raise ValueError(
            "raster pipelines do not support sharded: true (per-timestep slab "
            "streaming would read-modify-write each ShardingCodec object; "
            "chunks stay (1, cells_per_chunk) — one object per timestep-chunk)"
        )
