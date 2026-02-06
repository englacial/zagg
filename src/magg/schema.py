import numpy as np
import pandera.pandas as pa
from pandera.typing import Series
from pydantic_zarr.experimental.v3 import ArraySpec, BaseAttributes, GroupSpec, NamedConfig
from typing_extensions import TypedDict
from zarr import config
from zarr.abc.store import Store

HEALPIX_BASE_CELLS: int = 12  # Number of base cells in HEALPix tessellation


class CellStatsSchema(pa.DataFrameModel):
    """Pandera schema for cell-level aggregation output.

    Each field's metadata encodes its role (coord vs data_var), Zarr dtype/fill_value,
    and for data variables, the aggregation function and parameters.

    Metadata keys:
        role: "coord" | "data_var"
        zarr_dtype: str — Zarr array data type
        fill_value: int | str — Zarr fill value (0 or "NaN")
        agg: str — aggregation function name (data_var only)
        source: str | None — input column name for the aggregation
        params: dict — extra params (e.g. q for quantiles, weight_col for weighted stats)
    """

    # Coordinate columns
    cell_ids: Series[np.uint64] = pa.Field(
        metadata={"role": "coord", "zarr_dtype": "uint64", "fill_value": 0},
    )
    morton: Series[np.int64] = pa.Field(
        metadata={"role": "coord", "zarr_dtype": "int64", "fill_value": 0},
    )

    # Aggregation variables
    count: Series[np.int32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "int32",
            "fill_value": 0,
            "agg": "count",
            "source": None,
            "params": {},
        },
    )
    h_min: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "nanmin",
            "source": "h_li",
            "params": {},
        },
    )
    h_max: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "nanmax",
            "source": "h_li",
            "params": {},
        },
    )
    h_mean: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "weighted_mean",
            "source": "h_li",
            "params": {"weight_col": "s_li"},
        },
    )
    h_sigma: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "weighted_sigma",
            "source": "h_li",
            "params": {"weight_col": "s_li"},
        },
    )
    h_variance: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "nanvar",
            "source": "h_li",
            "params": {},
        },
    )
    h_q25: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "quantile",
            "source": "h_li",
            "params": {"q": 0.25},
        },
    )
    h_q50: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "quantile",
            "source": "h_li",
            "params": {"q": 0.50},
        },
    )
    h_q75: Series[np.float32] = pa.Field(
        metadata={
            "role": "data_var",
            "zarr_dtype": "float32",
            "fill_value": "NaN",
            "agg": "quantile",
            "source": "h_li",
            "params": {"q": 0.75},
        },
    )


# ---------------------------------------------------------------------------
# Schema metadata extraction helpers
# ---------------------------------------------------------------------------


def _get_schema_fields() -> dict[str, dict]:
    """Extract column metadata from CellStatsSchema."""
    schema = CellStatsSchema.to_schema()
    return {name: col.metadata or {} for name, col in schema.columns.items()}


def _fields_by_role(role: str) -> list[str]:
    """Return column names with the given role."""
    return [name for name, meta in _get_schema_fields().items() if meta.get("role") == role]


def _agg_fields() -> dict[str, dict]:
    """Return only fields that have an aggregation function defined."""
    return {name: meta for name, meta in _get_schema_fields().items() if meta.get("agg")}


# Derived from CellStatsSchema — same values as the old hardcoded lists
COORDS: list[str] = _fields_by_role("coord")
DATA_VARS: list[str] = _fields_by_role("data_var")


class ProcessingMetadata(TypedDict):
    parent_morton: int
    cells_with_data: int
    total_obs: int
    granule_count: int
    files_processed: int
    duration_s: float
    error: str | None


class ATL06AggregationMembers(TypedDict):
    cell_ids: ArraySpec
    morton: ArraySpec
    count: ArraySpec
    h_min: ArraySpec
    h_max: ArraySpec
    h_mean: ArraySpec
    h_sigma: ArraySpec
    h_variance: ArraySpec
    h_q25: ArraySpec
    h_q50: ArraySpec
    h_q75: ArraySpec


class ATL06AggregationGroup(GroupSpec):
    members: ATL06AggregationMembers  # type: ignore[assignment]
    attributes: BaseAttributes


def xdggs_spec(
    parent_order: int,
    child_order: int,
) -> ATL06AggregationGroup:
    """
    Create a [pydantic_zarr.experimental.v3.GroupSpec]() for ATL06 aggregation data using HEALPix/Morton indexing.

    Parameters
    ----------
    parent_order : int
        HEALPix order of parent morton cells
    child_order : int
        HEALPix order of child morton cells (must be >= parent_order)

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

    level_diff = child_order - parent_order
    n_children = 4**level_diff
    n_pixels = HEALPIX_BASE_CELLS * 4**child_order

    # Base configuration for all arrays
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

    # Build members from schema metadata
    schema_fields = _get_schema_fields()
    members = {}
    for col_name, meta in schema_fields.items():
        zarr_dtype = meta.get("zarr_dtype", "float32")
        fill_value = meta.get("fill_value", "NaN")
        members[col_name] = base_array_spec.with_data_type(zarr_dtype).with_fill_value(fill_value)

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

    # Create and write group specification
    return ATL06AggregationGroup(members=members, attributes=dggs_attrs)  # type: ignore[arg-type]


def xdggs_zarr_template(
    store: Store,
    parent_order: int,
    child_order: int,
    n_parent_cells: int | None = None,
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
        HEALPix order of parent morton cells (must be >= parent_order)
    child_order : int
        HEALPix order of child morton cells (must be >= parent_order)
    n_parent_cells: int
        Number of parent cells containing data
    overwrite: bool
        Whether to overwrite an existing array or group at the path. If overwrite is False and an array or group already exists at the path, an exception will be raised. Defaults to False.

    Returns
    -------
    Store
        The same store, with template written to path '{child_order}/'
    """
    spec = xdggs_spec(parent_order=parent_order, child_order=child_order)
    if n_parent_cells:
        assert n_parent_cells > 0
        level_diff = child_order - parent_order
        n_pixels = 4**level_diff * n_parent_cells
        members = {var: m.with_shape((n_pixels,)) for var, m in spec.members.items()}  # type: ignore[attr-defined]
        spec = spec.with_members(members)

    with config.set({"async.concurrency": 128}):
        spec.to_zarr(store, str(child_order), overwrite=overwrite)

    return store


__all__ = [
    "CellStatsSchema",
    "DATA_VARS",
    "COORDS",
    "ATL06AggregationGroup",
    "ProcessingMetadata",
    "xdggs_zarr_template",
    "xdggs_spec",
]
