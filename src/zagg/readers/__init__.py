"""Client-side read helpers for zagg products (issue #79).

These sit just outside the core write path: they reconstruct dense, fixed-size
arrays a downstream client can consume from the gridded Zarr products zagg
writes.  Pure-numpy + zarr (both already core deps); no new dependency.
"""

from __future__ import annotations

from zagg.readers.tdigest_tensor import (
    chunk_z_range,
    rasterize_cell,
    read_cell,
    read_locations,
    read_raw_values,
    read_tensors,
)

__all__ = [
    "chunk_z_range",
    "rasterize_cell",
    "read_cell",
    "read_locations",
    "read_raw_values",
    "read_tensors",
]
