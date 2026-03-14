"""
magg - Multi-resolution Aggregation

Multi-resolution aggregation using morton/healpix indexing.

This package provides cloud-agnostic processing functions that can be deployed
to various cloud platforms (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
or used for local processing.
"""

__version__ = "0.1.0"

# Export main processing functions
from .auth import get_nsidc_s3_credentials
from .config import PipelineConfig, default_config, get_child_order, get_store_path, load_config
from .processing import (
    calculate_cell_statistics,
    process_morton_cell,
    write_dataframe_to_zarr,
)
from .schema import xdggs_spec, xdggs_zarr_template
from .store import open_store

__all__ = [
    "PipelineConfig",
    "calculate_cell_statistics",
    "default_config",
    "get_child_order",
    "get_nsidc_s3_credentials",
    "get_store_path",
    "load_config",
    "open_store",
    "process_morton_cell",
    "write_dataframe_to_zarr",
    "xdggs_spec",
    "xdggs_zarr_template",
]
