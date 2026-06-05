"""Output writers, selected by ``output.format``.

Parallels :func:`zagg.grids.from_config`: a pipeline asks
:func:`from_output_config` for the writer matching its configured output format.
The spatial pipeline writes gridded zarr inline (worker-side, parallel chunk
writes); temporal/event pipelines produce one small row per event and use the
:class:`TabularWriter` (orchestrator-side — no concurrent-writer concern).
"""

from .tabular import TabularWriter

#: Output formats handled by :class:`TabularWriter`.
TABULAR_FORMATS = frozenset({"hdf5", "h5", "parquet", "pq", "table", "tabular"})


def from_output_config(config) -> TabularWriter:
    """Return the tabular output writer for a temporal/event config.

    Parameters
    ----------
    config : PipelineConfig
        Its ``output.format`` selects the on-disk format (default ``"hdf5"``).
    """
    fmt = (config.output or {}).get("format", "hdf5")
    return TabularWriter(fmt=fmt)


__all__ = ["TabularWriter", "from_output_config", "TABULAR_FORMATS"]
