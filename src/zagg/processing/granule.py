"""Granule open for the worker: h5coro for HDF5, a bare ranged handle otherwise.

The worker opens every granule as an ``h5coro.H5Coro``, whose constructor
parses the HDF5 superblock before any index backend is consulted. A netCDF3
granule (issue #682) has no superblock, and the ``sidecar`` backend never
needs one: it addresses the file entirely from its manifest and fetches byte
ranges through the handle's ``ioRequest``. :func:`open_granule` keeps the
h5coro open as the first and only attempt for HDF5, and when h5coro rejects
the signature it peeks the first four bytes and, for a netCDF3 magic, hands
back a :class:`RangedGranule` instead -- the subset of ``H5Coro`` the index
backends touch (``resource``, ``driver``, ``ioRequest(pos, size,
caching=False)``, ``close()``). Any other non-HDF5 file re-raises h5coro's
error unchanged.

No config key decides this: the file's own magic does, so a shard map mixing
HDF5 and netCDF3 granules under one ``reader: h5coro`` source reads each
through the right handle. A ``RangedGranule`` refuses every metadata-driven
read (``readDatasets``) with a message naming the manifest route, so a
non-sidecar backend -- or ``on_miss: fallback`` -- fails loudly per group
rather than decoding garbage.
"""

from __future__ import annotations

#: netCDF3 magics: classic and 64-bit offset (CDF-5 is refused at indexing time
#: by ``zagg.catalog.netcdf3`` and would never reach a manifest).
NETCDF3_MAGICS = (b"CDF\x01", b"CDF\x02")

_SIGNATURE_ERROR = "invalid file signature"


class RangedGranule:
    """Open handle for a granule addressed only through a sidecar manifest."""

    def __init__(self, resource, driver):
        self.resource = resource
        self.driver = driver
        self.cache: dict = {}  # the worker's release path tolerates a bare dict

    def ioRequest(self, pos, size, caching=False, prefetch=False):  # noqa: N802 (h5coro API)
        """Direct ranged read; the HDF5 cache-line semantics do not apply."""
        return self.driver.read(pos, size)

    def readDatasets(self, *args, **kwargs):  # noqa: N802 (h5coro API)
        raise TypeError(
            f"{self.resource} is not an HDF5 file; it can only be read through a "
            "manifest-addressed index backend (data_source.index.backend: sidecar)"
        )

    def close(self):
        close = getattr(self.driver, "close", None)
        if close is not None:
            close()


def open_granule(resource, driver_class, credentials):
    """``H5Coro(resource, driver_class, credentials=...)`` for HDF5; a
    :class:`RangedGranule` for a file h5coro rejects whose magic is netCDF3.

    ``H5Coro`` is resolved through ``zagg.processing.h5coro`` at call time so
    tests that monkeypatch ``zagg.processing.h5coro.H5Coro`` keep intercepting
    the worker's open exactly as before.
    """
    import zagg.processing as _processing

    try:
        return _processing.h5coro.H5Coro(
            resource,
            driver_class,
            credentials=credentials,
            errorChecking=True,
            verbose=False,
        )
    except RuntimeError as e:  # h5coro's FatalError is a RuntimeError
        if _SIGNATURE_ERROR not in str(e):
            raise
        driver = driver_class(resource, credentials)
        try:
            magic = bytes(driver.read(0, 4))
        except Exception:
            magic = b""
        if magic not in NETCDF3_MAGICS:
            close = getattr(driver, "close", None)
            if close is not None:
                close()
            raise
        return RangedGranule(resource, driver)
