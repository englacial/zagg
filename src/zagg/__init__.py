"""
zagg - Multi-resolution Aggregation

Multi-resolution aggregation using morton/healpix indexing.

This package provides cloud-agnostic processing functions that can be deployed
to various cloud platforms (AWS Lambda, GCP Cloud Functions, Azure Functions, etc.)
or used for local processing.
"""

try:
    from ._version import __version__
except ImportError:
    __version__ = "0.0.0+unknown"

import importlib

from .config import (
    PipelineConfig,
    default_config,
    get_child_order,
    get_driver,
    get_layout,
    get_store_path,
    load_config,
)
from .grids import HealpixGrid, OutputGrid
from .processing import (
    calculate_cell_statistics,
    process_morton_cell,
    process_shard,
    write_dataframe_to_zarr,
)
from .schema import xdggs_spec, xdggs_zarr_template
from .store import open_store, parse_s3_path

# Lazy orchestrator-only re-exports. ``auth`` pulls earthaccess and ``runner``
# pulls boto3 (and auth); importing them eagerly would force earthaccess into
# the Lambda layer, which the worker never uses (it receives credentials in the
# event). Accessing these attributes imports the backing module on demand.
_LAZY = {
    "agg": ".runner",
    "get_edl_token": ".auth",
    "get_nsidc_s3_credentials": ".auth",
}


def __getattr__(name):
    if name in _LAZY:
        mod = importlib.import_module(_LAZY[name], __name__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "HealpixGrid",
    "OutputGrid",
    "PipelineConfig",
    "calculate_cell_statistics",
    "default_config",
    "get_child_order",
    "get_driver",
    "get_layout",
    "get_edl_token",
    "get_nsidc_s3_credentials",
    "get_store_path",
    "load_config",
    "open_store",
    "parse_s3_path",
    "process_morton_cell",
    "process_shard",
    "agg",
    "write_dataframe_to_zarr",
    "xdggs_spec",
    "xdggs_zarr_template",
]
