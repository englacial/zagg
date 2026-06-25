"""The gridded-Zarr output writer for the spatial pipeline (issue #12, Phase 6).

:class:`ZarrGridWriter` is a thin adapter over the functions the spatial runner
already calls -- :func:`zagg.processing.write_dataframe_to_zarr`,
:func:`zagg.processing.write.write_ragged_to_zarr`, and Zarr metadata
consolidation. It introduces the ``Writer`` seam *without changing what reaches
disk*: the methods forward their arguments verbatim, so a run routed through the
writer produces a byte-for-byte identical store. The spatial ``runner`` path is
intentionally left calling the underlying functions directly for now -- wrapping
it would have to move code into ``runner.py`` (already at the size limit, §4) and
risk the byte-identical guarantee for no behavioural gain; the writer exists so
the *temporal* path and future callers share one polymorphic output seam.
"""

from __future__ import annotations

from zarr import consolidate_metadata
from zarr.abc.store import Store

from zagg.processing.write import write_dataframe_to_zarr, write_ragged_to_zarr

from .base import register_writer


@register_writer("zarr")
class ZarrGridWriter:
    """Writer for the gridded, morton-chunked Zarr output (``output.format: zarr``).

    A pure adapter: every method forwards to the existing spatial write function
    with the same arguments, so the on-disk result is identical to calling those
    functions directly.
    """

    def write(self, carrier, store: Store, *, grid, chunk_idx) -> Store:
        """Write one shard's dense carrier (forwards to ``write_dataframe_to_zarr``)."""
        return write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

    def write_ragged(self, ragged: dict, store: Store, *, grid, shard_key: int) -> Store:
        """Write one shard's ragged CSR fields (forwards to ``write_ragged_to_zarr``)."""
        return write_ragged_to_zarr(ragged, store, grid=grid, shard_key=shard_key)

    def finalize(self, store: Store) -> Store:
        """Consolidate Zarr v3 metadata once a run's shards are written."""
        consolidate_metadata(store, zarr_format=3)
        return store
