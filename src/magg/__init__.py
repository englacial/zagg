"""
magg - Multi-resolution Aggregation

Multi-resolution aggregation using morton/healpix indexing.

This package provides cloud-agnostic processing functions that can be deployed
to various cloud platforms (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
or used for local processing.
"""

__version__ = "0.1.0"

# Export main processing functions
from .auth import get_edl_token, get_nsidc_s3_credentials, get_s3_credentials
from .config import (
    PipelineConfig,
    default_config,
    get_child_order,
    get_driver,
    get_pipeline_type,
    get_store_path,
    load_config,
)
from .processing import (
    calculate_cell_statistics,
    process_morton_cell,
    write_dataframe_to_zarr,
)
from .runner import agg
from .schema import xdggs_spec, xdggs_zarr_template
from .store import open_store, parse_s3_path

__all__ = [
    "PipelineConfig",
    "agg",
    "calculate_cell_statistics",
    "default_config",
    "get_child_order",
    "get_driver",
    "get_edl_token",
    "get_nsidc_s3_credentials",
    "get_pipeline_type",
    "get_s3_credentials",
    "get_store_path",
    "load_config",
    "open_store",
    "parse_s3_path",
    "process_morton_cell",
    "write_dataframe_to_zarr",
    "xdggs_spec",
    "xdggs_zarr_template",
]
