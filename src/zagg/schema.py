from __future__ import annotations

from pydantic_zarr.experimental.v3 import ArraySpec, GroupSpec, NamedConfig
from typing_extensions import TypedDict
from zarr import config as zarr_config
from zarr.abc.store import Store

from magg.config import PipelineConfig, default_config, get_agg_fields

HEALPIX_BASE_CELLS: int = 12  # Number of base cells in HEALPix tessellation


class ProcessingMetadata(TypedDict):
    parent_morton: int
    cells_with_data: int
    total_obs: int
    granule_count: int
    files_processed: int
    duration_s: float
    error: str | None


def xdggs_spec(
    parent_order: int,
    child_order: int,
    config: PipelineConfig | None = None,
) -> GroupSpec:
    """
    Create a [pydantic_zarr.experimental.v3.GroupSpec]() for aggregation data using HEALPix/Morton indexing.

    Parameters
    ----------
    parent_order : int
        HEALPix order of parent morton cells
    child_order : int
        HEALPix order of child morton cells (must be >= parent_order)
    config : PipelineConfig or None
        Pipeline configuration. Falls back to ``default_config("atl06")``.

    Returns
    -------
    GroupSpec
        Xdggs compatible group spec

    Raises
    ------
    ValueError
        If child_order < parent_order
    """
    if child_order < parent_order:
        raise ValueError(f"child_order ({child_order}) must be >= parent_order ({parent_order})")

    if config is None:
        config = default_config("atl06")

    level_diff = child_order - parent_order
    n_children = 4**level_diff
    n_pixels = HEALPIX_BASE_CELLS * 4**child_order

    base_array_spec = ArraySpec(
        attributes={},
        shape=(n_pixels,),
        dimension_names=("cells",),
        data_type="float32",
        chunk_grid=NamedConfig(name="regular", configuration={"chunk_shape": (n_children,)}),
        chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
        codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
        storage_transformers=(),
        fill_value="NaN",
    )

    members = {}
    for coord_name, coord_meta in config.aggregation.get("coordinates", {}).items():
        zarr_dtype = coord_meta.get("dtype", "float32")
        fill_value = coord_meta.get("fill_value", "NaN")
        members[coord_name] = base_array_spec.with_data_type(zarr_dtype).with_fill_value(fill_value)

    for var_name, var_meta in get_agg_fields(config).items():
        zarr_dtype = var_meta.get("dtype", "float32")
        fill_value = var_meta.get("fill_value", "NaN")
        members[var_name] = base_array_spec.with_data_type(zarr_dtype).with_fill_value(fill_value)

    dggs_attrs = {
        "zarr_conventions": [
            {
                "schema_url": "https://raw.githubusercontent.com/zarr-conventions/dggs/refs/tags/v1/schema.json",
                "spec_url": "https://github.com/zarr-conventions/dggs/blob/v1/README.md",
                "uuid": "7b255807-140c-42ca-97f6-7a1cfecdbc38",
                "name": "dggs",
                "description": "Discrete Global Grid Systems convention for zarr",
            }
        ],
        "dggs": {
            "name": "healpix",
            "refinement_level": child_order,
            "indexing_scheme": "nested",
            "spatial_dimension": "cells",
            "ellipsoid": {
                "name": "WGS84",
                "semimajor_axis": 6378137.0,
                "inverse_flattening": 298.257223563,
            },
            "coordinate": "cell_ids",
            "compression": "none",
        },
    }

    return GroupSpec(members=members, attributes=dggs_attrs)


def xdggs_zarr_template(
    store: Store,
    parent_order: int,
    child_order: int,
    n_parent_cells: int | None = None,
    overwrite: bool = False,
    config: PipelineConfig | None = None,
) -> Store:
    """
    Create a Zarr template for data aggregation data using HEALPix/Morton indexing.

    Parameters
    ----------
    store : Store
        Zarr-compatible store (from zarr.abc.store)
    parent_order : int
        HEALPix order of parent morton cells
    child_order : int
        HEALPix order of child morton cells (must be >= parent_order)
    n_parent_cells : int
        Number of parent cells containing data
    overwrite : bool
        Whether to overwrite an existing array or group at the path.
        Defaults to False.
    config : PipelineConfig or None
        Pipeline configuration. Falls back to ``default_config("atl06")``.

    Returns
    -------
    Store
        The same store, with template written to path '{child_order}/'
    """
    spec = xdggs_spec(parent_order=parent_order, child_order=child_order, config=config)
    if n_parent_cells is not None and n_parent_cells <= 0:
        raise ValueError(f"n_parent_cells must be positive, got {n_parent_cells}")
    if n_parent_cells:
        level_diff = child_order - parent_order
        n_pixels = 4**level_diff * n_parent_cells
        members = {
            var: m.with_shape((n_pixels,)) if isinstance(m, ArraySpec) else m
            for var, m in spec.members.items()
        }
        spec = spec.with_members(members)

    with zarr_config.set({"async.concurrency": 128}):
        spec.to_zarr(store, str(child_order), overwrite=overwrite)

    return store


__all__ = [
    "ProcessingMetadata",
    "xdggs_zarr_template",
    "xdggs_spec",
]
