"""Output writers for zagg's pipelines (issue #12, Phase 6).

The two pipeline cores produce structurally different output (challenge #5 in
issue #12): the spatial path writes a gridded, morton-chunked **Zarr** store; the
temporal/event path produces **tabular** output (one row per event x scalar
columns). This package gives both a single ``Writer`` seam so a run picks a
writer by output format rather than branching inline.

- :class:`ZarrGridWriter` wraps the existing spatial Zarr writes
  (:func:`zagg.processing.write_dataframe_to_zarr` /
  :func:`zagg.processing.write.write_ragged_to_zarr` + metadata consolidation)
  **byte-for-byte** -- it is a thin adapter over functions the spatial runner
  already calls, so routing through it changes nothing on disk.
- :class:`TabularWriter` serialises temporal/event result rows to Parquet
  (default) or CSV -- no new dependency, ``pyarrow``/``pandas`` are already core.

``get_writer(output_format(config))`` resolves the writer for a config's
``output.format`` so the dispatch stays declarative.
"""

from .base import Writer, get_writer, output_format, register_writer
from .tabular import TabularWriter, write_tabular
from .zarr_grid import ZarrGridWriter

__all__ = [
    "TabularWriter",
    "Writer",
    "ZarrGridWriter",
    "get_writer",
    "output_format",
    "register_writer",
    "write_tabular",
]
