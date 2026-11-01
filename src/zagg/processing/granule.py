"""Granule open for the worker: h5coro for HDF5, a bare ranged handle otherwise.

The worker opens every granule as an ``h5coro.H5Coro``, whose constructor
parses the HDF5 superblock before any index backend is consulted. A netCDF3
granule (issue #682) has no superblock, and the ``sidecar`` backend never
needs one: it addresses the file entirely from its manifest and fetches byte
ranges through the handle's ``ioRequest``. :func:`open_granule` decides
between the two by the file's own magic, with the name as a cost hint only:

- a ``.nc`` / ``.nc4`` name (the netCDF suffixes, which also label netCDF4 =
  HDF5 files) peeks the first four bytes with ONE driver: a netCDF3 magic
  returns a :class:`RangedGranule` on that driver with no h5coro attempt at
  all; anything else closes it and opens with h5coro (one extra 4-byte GET);
- every other name opens with h5coro first, so an HDF5 granule costs nothing
  extra; when h5coro rejects the signature the first four bytes are peeked
  and a netCDF3 magic still gets a :class:`RangedGranule` -- at the price of
  the rejected attempt's 4 MiB cache-line read (see :func:`open_granule`).

Any other non-HDF5 file re-raises h5coro's error unchanged. The handle is the
subset of ``H5Coro`` the index backends touch (``resource``, ``driver``,
``ioRequest(pos, size, caching=False)``, ``close()``).

No config key decides this, so a shard map mixing HDF5 and netCDF3 granules
under one ``reader: h5coro`` source reads each through the right handle. A
``RangedGranule`` refuses every metadata-driven read (``readDatasets``, or any
h5coro-internal attribute) with :class:`NotHDF5Error` naming the manifest
route, so a non-sidecar backend -- or a sidecar miss under ``on_miss:
fallback`` / ``build`` -- fails loudly per group rather than decoding garbage.
"""

from __future__ import annotations

import os

#: netCDF3 magics: classic and 64-bit offset (CDF-5 is refused at indexing time
#: by ``zagg.catalog.netcdf3`` and would never reach a manifest).
NETCDF3_MAGICS = (b"CDF\x01", b"CDF\x02")

_SIGNATURE_ERROR = "invalid file signature"

#: Names that hint "netCDF": peek the magic before trying h5coro.
NETCDF_SUFFIXES = (".nc", ".nc4")


class NotHDF5Error(TypeError, AttributeError):
    """A metadata-driven read reached a :class:`RangedGranule`.

    A ``TypeError`` (the refusal), and also an ``AttributeError`` so that
    ``getattr(h, name, default)`` / ``hasattr`` probes on the handle keep their
    usual meaning while a direct h5coro-internal access (``rootAddress``,
    ``metadataTable``, ...) fails with the actionable message."""


class RangedGranule:
    """Open handle for a granule addressed only through a sidecar manifest."""

    def __init__(self, resource, driver):
        self.resource = resource
        self.driver = driver
        self.cache: dict = {}  # the worker's release path tolerates a bare dict

    def ioRequest(self, pos, size, caching=False, prefetch=False):  # noqa: N802 (h5coro API)
        """Direct ranged read; the HDF5 cache-line semantics do not apply."""
        return self.driver.read(pos, size)

    def _refuse(self, what):
        return NotHDF5Error(
            f"{self.resource} is not an HDF5 file ({what}): only the sidecar index "
            "backend can read it, from a manifest in its store; this read reached HDF5 "
            "metadata because the backend is not sidecar, or the sidecar store has no "
            "manifest for this granule (on_miss: fallback/build)"
        )

    def readDatasets(self, *args, **kwargs):  # noqa: N802 (h5coro API)
        raise self._refuse("readDatasets")

    def __getattr__(self, name):
        # Only reached for attributes the handle does not define: every
        # h5coro-internal metadata access (e.g. ``on_miss: build``'s walk
        # reading ``rootAddress``) gets the same refusal as ``readDatasets``.
        if name.startswith("__"):
            raise AttributeError(name)
        raise self._refuse(name)

    def close(self):
        _close(self.driver)


def _close(driver):
    close = getattr(driver, "close", None)
    if close is not None:
        close()


def _peek(resource, driver):
    """The first four bytes. A failed read is I/O, never a format verdict:
    ``None`` (h5coro's ``S3Driver.read`` swallows errors into it), a short
    read, or a driver exception (chained) raises ``OSError`` instead of
    falling through to "invalid file signature"."""
    try:
        got = driver.read(0, 4)
    except Exception as e:
        raise OSError(f"{resource}: ranged read failed at 0+4 ({e!r})") from e
    if got is None or len(got) < 4:
        what = "None" if got is None else f"{len(got)} bytes"
        raise OSError(f"{resource}: ranged read failed at 0+4 (driver returned {what})")
    return bytes(got)


def _open_ranged(resource, driver_class, credentials):
    """A :class:`RangedGranule` when the magic is netCDF3, else ``None``; the
    peek's driver is closed unless the handle keeps it."""
    driver = driver_class(resource, credentials)
    try:
        if _peek(resource, driver) in NETCDF3_MAGICS:
            return RangedGranule(resource, driver)
    except BaseException:
        _close(driver)
        raise
    _close(driver)
    return None


def open_granule(resource, driver_class, credentials):
    """``H5Coro(resource, driver_class, credentials=...)`` for HDF5; a
    :class:`RangedGranule` for a netCDF3 file.

    A ``.nc`` / ``.nc4`` resource is peeked first with one driver, reused by
    the ranged handle on a netCDF3 magic, so a netCDF3 granule never pays the
    h5coro attempt. Any other name tries h5coro first; a netCDF3 file under
    such a name is still recognized from the rejected signature, but that
    rejected attempt reads one full h5coro cache line (``(0, 4 MiB)``, the
    superblock read with caching on) before the peek, and its driver is left
    to GC (h5coro registers the finalizer only after the superblock parses).

    ``H5Coro`` is resolved through ``zagg.processing.h5coro`` at call time so
    tests that monkeypatch ``zagg.processing.h5coro.H5Coro`` keep intercepting
    the worker's open exactly as before.
    """
    import zagg.processing as _processing

    if os.fspath(resource).lower().endswith(NETCDF_SUFFIXES):
        ranged = _open_ranged(resource, driver_class, credentials)
        if ranged is not None:
            return ranged
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
        ranged = _open_ranged(resource, driver_class, credentials)
        if ranged is None:
            raise
        return ranged
