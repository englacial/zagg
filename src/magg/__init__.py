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
from .processing import calculate_cell_statistics, process_morton_cell, write_dataframe_to_zarr
from .schema import CellStatsSchema, xdggs_spec, xdggs_zarr_template

__all__ = [
    "CellStatsSchema",
    "calculate_cell_statistics",
    "get_nsidc_s3_credentials",
    "process_morton_cell",
    "write_dataframe_to_zarr",
    "xdggs_spec",
    "xdggs_zarr_template",
]
