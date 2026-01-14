import numpy as np
from pydantic_zarr.experimental.v3 import ArraySpec, BaseAttributes, GroupSpec, NamedConfig
from typing_extensions import TypedDict
from zarr import config
from zarr.abc.store import Store

# Constants
HEALPIX_BASE_CELLS: int = 12  # Number of base cells in HEALPix tessellation
COORDS: list[str] = ["cell_ids", "morton"]
DATA_VARS: list[str] = [
    "count",
    "min",
    "max",
    "mean_weighted",
    "sigma_mean",
    "variance",
    "q25",
    "q50",
    "q75",
]


class ATL06AggregationMembers(TypedDict):
    cell_ids: ArraySpec
    morton: ArraySpec
    count: ArraySpec
    min: ArraySpec
    max: ArraySpec
    mean_weighted: ArraySpec
    sigma_mean: ArraySpec
    variance: ArraySpec
    q25: ArraySpec
    q50: ArraySpec
    q75: ArraySpec


class ATL06AggregationGroup(GroupSpec):
    members: ATL06AggregationMembers  # type: ignore[assignment]
    attributes: BaseAttributes


def create_zarr_template(
    store: Store,
    parent_order: int,
    child_order: int,
    overwrite: bool = False,
) -> Store:
    """
    Create a Zarr template for ATL06 aggregation data using HEALPix/Morton indexing.

    Overwrites an existing Zarr store if it already exists.

    Parameters
    ----------
    store : Store
        Zarr-compatible store (from zarr.abc.store)
    parent_order : int
        HEALPix order of parent morton cells
    child_order : int
        HEALPix order of child morton cells (must be >= parent_order)
    overwrite: bool
        Whether to overwrite an existing array or group at the path. If overwrite is False and an array or group already exists at the path, an exception will be raised. Defaults to False.

    Returns
    -------
    Store
        The same store, with template written to path '{child_order}/'

    Raises
    ------
    ValueError
        If child_order < parent_order
    """
    if child_order < parent_order:
        raise ValueError(f"child_order ({child_order}) must be >= parent_order ({parent_order})")

    level_diff = child_order - parent_order
    n_children = 4**level_diff
    n_pixels = HEALPIX_BASE_CELLS * 4**child_order

    # Base configuration for all arrays
    base_array_spec = ArraySpec(
        attributes={},
        shape=(n_pixels,),
        dimension_names=("cell_ids",),
        data_type="float32",
        chunk_grid=NamedConfig(name="regular", configuration={"chunk_shape": (n_children,)}),
        chunk_key_encoding=NamedConfig(name="default", configuration={"separator": "/"}),
        codecs=(NamedConfig(name="bytes", configuration={"endian": "little"}),),
        storage_transformers=(),
        fill_value=np.nan,
    )

    # Create member specifications
    members = {
        "cell_ids": base_array_spec.with_fill_value(0).with_data_type("uint64"),
        "morton": base_array_spec.with_fill_value(0).with_data_type("int64"),
        "count": base_array_spec.with_fill_value(0).with_data_type("int32"),
    }

    # Add statistical data variables (all float32 with NaN fill)
    for var in DATA_VARS:
        if var != "count":  # count already added above with different dtype/fill
            members[var] = base_array_spec

    # Create and write group specification
    spec = ATL06AggregationGroup(members=members, attributes={})  # type: ignore[arg-type]
    with config.set({"async.concurrency": 128}):
        spec.to_zarr(store, str(child_order), overwrite=overwrite)

    return store


__all__ = ["DATA_VARS", "COORDS", "ATL06AggregationGroup", "create_zarr_template"]
