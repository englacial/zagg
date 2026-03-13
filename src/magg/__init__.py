"""
magg - Multi-resolution Aggregation

ICESat-2 ATL06 processing using morton/healpix indexing.

This package provides cloud-agnostic processing functions that can be deployed
to various cloud platforms (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
or used for local processing.
"""

__version__ = "0.1.0"

# Export main processing functions
from .auth import get_nsidc_s3_credentials
from .config import PipelineConfig, default_config, load_config
from .processing import (
    ATL06_CONFIG,
    DataSourceConfig,
    calculate_cell_statistics,
    process_morton_cell,
    write_dataframe_to_zarr,
)
from .schema import xdggs_spec, xdggs_zarr_template

__all__ = [
    "ATL06_CONFIG",
    "DataSourceConfig",
    "PipelineConfig",
    "calculate_cell_statistics",
    "default_config",
    "get_nsidc_s3_credentials",
    "load_config",
    "process_morton_cell",
    "write_dataframe_to_zarr",
    "xdggs_spec",
    "xdggs_zarr_template",
]
