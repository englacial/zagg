"""Output writers for zagg's pipelines (issue #12, Phase 6).

The two pipeline cores produce structurally different output (challenge #5 in
issue #12): the spatial path writes a gridded, morton-chunked **Zarr** store
(via the ``zagg.processing`` writers, called directly by the runner and Lambda
worker); the temporal/event path produces **tabular** output (one row per
event x scalar columns), serialised by :class:`TabularWriter` /
:func:`write_tabular` to Parquet (default) or CSV -- no new dependency,
``fastparquet``/``pandas`` are already core. :func:`output_format` resolves a
config's ``output.format`` (default ``"zarr"``).
"""

from .base import output_format
from .tabular import TabularWriter, write_tabular

__all__ = [
    "TabularWriter",
    "output_format",
    "write_tabular",
]
