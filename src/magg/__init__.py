"""
magg - Multi-resolution Aggregation

ICESat-2 ATL06 processing using morton/healpix indexing.

This package provides cloud-agnostic processing functions that can be deployed
to various cloud platforms (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
or used for local processing.
"""

__version__ = "0.1.0"

# Export main processing functions
from .processing import calculate_cell_statistics, process_morton_cell
from .auth import get_nsidc_s3_credentials

__all__ = [
    "calculate_cell_statistics",
    "process_morton_cell",
    "get_nsidc_s3_credentials",
]
