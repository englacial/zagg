"""netCDF3 header parser: the catalog-time index route for classic files (issue #682).

netCDF3 (``CDF\\x01`` classic, ``CDF\\x02`` 64-bit offset) is a small header
-- dimensions, attributes, and one record per variable carrying its type,
dimensions and file offset -- followed by uncompressed big-endian arrays.
Non-record variables are one contiguous block each; record variables are
interleaved along the unlimited dimension, one slab per record. That is a
chunk manifest, so the worker needs no new code: this module parses the
header (:func:`parse_header`, :func:`describe`) and, in the next phase,
emits the sidecar manifest the ``sidecar`` index backend already decodes.

The format reference is the netCDF classic format specification (the
``netcdf_file`` grammar in the netCDF-C docs, "File Format Specification").
Everything in the header is big-endian; the grammar this module walks::

    header   = magic numrecs dim_list gatt_list var_list
    magic    = 'C' 'D' 'F' VERSION          VERSION = \\x01 | \\x02 (| \\x05 CDF-5)
    numrecs  = NON_NEG | STREAMING          STREAMING = \\xFF\\xFF\\xFF\\xFF
    dim_list = ABSENT | NC_DIMENSION nelems [dim ...]     dim  = name dim_length
    att_list = ABSENT | NC_ATTRIBUTE nelems [attr ...]    attr = name nc_type nelems [values ...]
    var_list = ABSENT | NC_VARIABLE nelems [var ...]
    var      = name nelems [dimid ...] vatt_list nc_type vsize begin
    name     = nelems namestring (zero-padded to a 4-byte boundary)
    ABSENT   = ZERO ZERO (two 4-byte zeros); begin is 4 bytes (v1) or 8 (v2)

Header bytes are fetched on demand through a ``(offset, size) -> bytes``
ranged read: the parser starts with :data:`INITIAL_HEADER_BYTES` and grows
the window (doubling) only when a field runs past it, so ``describe`` on a
multi-GB cloud object costs one ranged GET for the common header size. The
data arrays are never touched. CDF-5 (``CDF\\x05``, 64-bit dimensions and
extra types) is detected and refused: its header grammar differs and no
archive product zagg targets uses it.
"""

from __future__ import annotations

import os
import struct
from dataclasses import dataclass, field

import numpy as np

__all__ = [
    "INITIAL_HEADER_BYTES",
    "STREAMING",
    "Header",
    "Variable",
    "describe",
    "parse_header",
]

#: First ranged read of the header. netCDF3 headers of archive products run
#: from a few hundred bytes to a few tens of KB (every variable's attributes
#: live in the header); 64 KiB covers them in one GET and costs nothing on a
#: local file.
INITIAL_HEADER_BYTES = 64 * 1024

#: ``numrecs`` sentinel: the record count is unknown (file written in
#: streaming mode); it must be derived from the file size.
STREAMING = 0xFFFFFFFF

_MAGIC = b"CDF"
_VERSIONS = {1: "classic", 2: "64-bit offset"}
_CDF5_VERSION = 5
_NC_DIMENSION = 0x0A
_NC_VARIABLE = 0x0B
_NC_ATTRIBUTE = 0x0C

#: ``nc_type`` code -> (netCDF name, numpy dtype). The file is big-endian, so
#: multi-byte types carry an explicit ``>``; ``char`` maps to ``|u1`` (raw
#: bytes) because the compiled chunk reader decodes numeric dtypes only -- a
#: character array reads as its code points and the caller decodes.
_NC_TYPES: dict[int, tuple[str, np.dtype]] = {
    1: ("byte", np.dtype("|i1")),
    2: ("char", np.dtype("|u1")),
    3: ("short", np.dtype(">i2")),
    4: ("int", np.dtype(">i4")),
    5: ("float", np.dtype(">f4")),
    6: ("double", np.dtype(">f8")),
}


def _pad4(n: int) -> int:
    """Round ``n`` up to the next multiple of 4 (the header's padding rule)."""
    return (n + 3) & ~3


class _HeaderBytes:
    """Header bytes fetched on demand through a ranged read.

    ``fetch(offset, size) -> bytes`` may return fewer bytes than asked at end
    of file. The window starts at ``initial`` bytes and doubles whenever a
    field runs past it; a field that runs past the end of the object is a
    truncated header.
    """

    def __init__(self, fetch, initial: int = INITIAL_HEADER_BYTES):
        self._fetch = fetch
        self._initial = max(int(initial), 16)
        self._buf = b""
        self._eof = False
        self.pos = 0

    def _ensure(self, end: int) -> None:
        while len(self._buf) < end and not self._eof:
            have = len(self._buf)
            want = max(end, 2 * have, self._initial) - have
            chunk = bytes(self._fetch(have, want))
            if len(chunk) < want:
                self._eof = True
            self._buf += chunk
        if len(self._buf) < end:
            raise ValueError(
                f"truncated netCDF3 header: needed {end} bytes, object has {len(self._buf)}"
            )

    def take(self, n: int) -> bytes:
        self._ensure(self.pos + n)
        out = self._buf[self.pos : self.pos + n]
        self.pos += n
        return out

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.take(8))[0]


@dataclass(frozen=True)
class Variable:
    """One ``var`` record of the header."""

    name: str
    dimids: tuple[int, ...]
    attrs: dict
    nc_type: int
    vsize: int
    begin: int

    @property
    def dtype(self) -> np.dtype:
        return _NC_TYPES[self.nc_type][1]

    @property
    def type_name(self) -> str:
        return _NC_TYPES[self.nc_type][0]


@dataclass(frozen=True)
class Header:
    """A parsed netCDF3 header.

    ``dims`` is ``(name, length)`` in dimid order, the record dimension with
    length 0 as written; ``numrecs`` is ``None`` when the file was written in
    streaming mode (:data:`STREAMING`), in which case the record count has to
    come from the file size (``(file_size - first record begin) // recsize``).
    ``header_bytes`` is the byte offset where the header ends.
    """

    version: int
    numrecs: int | None
    dims: tuple[tuple[str, int], ...]
    attrs: dict
    variables: tuple[Variable, ...]
    header_bytes: int = field(default=0)

    @property
    def format(self) -> str:
        return _VERSIONS[self.version]

    @property
    def record_dimid(self) -> int | None:
        for i, (_, length) in enumerate(self.dims):
            if length == 0:
                return i
        return None

    def is_record(self, var: Variable) -> bool:
        return bool(var.dimids) and var.dimids[0] == self.record_dimid

    def dim_names(self, var: Variable) -> list[str]:
        return [self.dims[d][0] for d in var.dimids]

    def shape(self, var: Variable) -> list[int | None]:
        """Array shape; the leading record extent is ``numrecs`` (``None`` when streaming)."""
        shape: list[int | None] = [self.dims[d][1] for d in var.dimids]
        if self.is_record(var):
            shape[0] = self.numrecs
        return shape

    def slab_bytes(self, var: Variable) -> int:
        """Unpadded bytes of one record (record variable) or of the whole array."""
        fixed = [self.dims[d][1] for d in var.dimids[1 if self.is_record(var) else 0 :]]
        return int(np.prod(fixed, dtype=np.int64)) * var.dtype.itemsize

    @property
    def record_variables(self) -> tuple[Variable, ...]:
        return tuple(v for v in self.variables if self.is_record(v))

    @property
    def recsize(self) -> int:
        """Bytes per record across all record variables, per the spec's padding
        rule: each slab is padded to 4 bytes, except when there is exactly one
        record variable, whose slabs are packed with no padding at all."""
        recs = self.record_variables
        if len(recs) == 1:
            return self.slab_bytes(recs[0])
        return sum(_pad4(self.slab_bytes(v)) for v in recs)

    def describe(self) -> dict:
        """The introspection surface: dims, attrs and per-variable metadata, as
        plain Python (JSON-ready) values. See :func:`describe`."""
        rec = self.record_dimid
        dims = {
            name: (self.numrecs if i == rec else length)
            for i, (name, length) in enumerate(self.dims)
        }
        variables = {}
        for v in self.variables:
            variables[v.name] = {
                "dtype": v.dtype.str,
                "nc_type": v.type_name,
                "dims": self.dim_names(v),
                "shape": self.shape(v),
                "attrs": v.attrs,
                "record": self.is_record(v),
                "begin": v.begin,
                "vsize": v.vsize,
            }
        return {
            "format": self.format,
            "version": self.version,
            "numrecs": self.numrecs,
            "record_dim": None if rec is None else self.dims[rec][0],
            "header_bytes": self.header_bytes,
            "dims": dims,
            "attrs": self.attrs,
            "variables": variables,
        }


def _read_name(hb: _HeaderBytes) -> str:
    n = hb.u32()
    raw = hb.take(_pad4(n))[:n]
    return raw.decode("utf-8")


def _attr_value(nc_type: int, raw: bytes, nelems: int):
    """Attribute values as plain Python: ``char`` -> ``str``, numeric -> a
    scalar when ``nelems == 1`` else a list."""
    if nc_type == 2:
        return raw.decode("utf-8", errors="replace")
    values = np.frombuffer(raw, dtype=_NC_TYPES[nc_type][1], count=nelems).tolist()
    return values[0] if nelems == 1 else values


def _read_att_list(hb: _HeaderBytes) -> dict:
    tag = hb.u32()
    count = hb.u32()
    if tag == 0:
        if count != 0:
            raise ValueError(f"malformed netCDF3 header: ABSENT att_list with count {count}")
        return {}
    if tag != _NC_ATTRIBUTE:
        raise ValueError(f"malformed netCDF3 header: expected NC_ATTRIBUTE tag, got 0x{tag:x}")
    attrs = {}
    for _ in range(count):
        name = _read_name(hb)
        nc_type = hb.u32()
        nelems = hb.u32()
        if nc_type not in _NC_TYPES:
            raise ValueError(f"netCDF3 attribute {name!r}: unsupported nc_type {nc_type}")
        nbytes = nelems * _NC_TYPES[nc_type][1].itemsize
        raw = hb.take(_pad4(nbytes))[:nbytes]
        attrs[name] = _attr_value(nc_type, raw, nelems)
    return attrs


def _read_tagged_count(hb: _HeaderBytes, tag_expected: int, what: str) -> int:
    tag = hb.u32()
    count = hb.u32()
    if tag == 0:
        if count != 0:
            raise ValueError(f"malformed netCDF3 header: ABSENT {what} with count {count}")
        return 0
    if tag != tag_expected:
        raise ValueError(
            f"malformed netCDF3 header: expected {what} tag 0x{tag_expected:x}, got 0x{tag:x}"
        )
    return count


def _parse(hb: _HeaderBytes) -> Header:
    magic = hb.take(4)
    if magic[:3] != _MAGIC:
        raise ValueError(
            f"not a netCDF3 file: magic {magic!r} (expected b'CDF\\x01' or b'CDF\\x02')"
        )
    version = magic[3]
    if version == _CDF5_VERSION:
        raise ValueError(
            "CDF-5 (b'CDF\\x05', 64-bit data) is not supported: its header grammar "
            "differs from classic / 64-bit offset netCDF3 (issue #682 scopes it out)"
        )
    if version not in _VERSIONS:
        raise ValueError(f"not a netCDF3 file: unknown version byte {version}")
    numrecs_raw = hb.u32()
    numrecs = None if numrecs_raw == STREAMING else numrecs_raw

    ndims = _read_tagged_count(hb, _NC_DIMENSION, "dim_list")
    dims = tuple((_read_name(hb), hb.u32()) for _ in range(ndims))
    if sum(1 for _, n in dims if n == 0) > 1:
        raise ValueError("malformed netCDF3 header: more than one record (length 0) dimension")

    attrs = _read_att_list(hb)

    nvars = _read_tagged_count(hb, _NC_VARIABLE, "var_list")
    variables = []
    for _ in range(nvars):
        name = _read_name(hb)
        nd = hb.u32()
        dimids = tuple(hb.u32() for _ in range(nd))
        for d in dimids:
            if d >= ndims:
                raise ValueError(
                    f"netCDF3 variable {name!r}: dimid {d} out of range ({ndims} dims)"
                )
        vattrs = _read_att_list(hb)
        nc_type = hb.u32()
        if nc_type not in _NC_TYPES:
            raise ValueError(f"netCDF3 variable {name!r}: unsupported nc_type {nc_type}")
        vsize = hb.u32()
        begin = hb.u32() if version == 1 else hb.i64()
        variables.append(Variable(name, dimids, vattrs, nc_type, vsize, begin))
    record_dim = next((i for i, (_, n) in enumerate(dims) if n == 0), None)
    for v in variables:
        if record_dim in v.dimids[1:]:
            raise ValueError(
                f"netCDF3 variable {v.name!r}: the record dimension may only be its first"
            )
    return Header(version, numrecs, dims, attrs, tuple(variables), hb.pos)


def _fetcher(source, driver=None, credentials=None):
    """``(offset, size) -> bytes`` over ``source``: raw bytes, a local path, an
    object with an h5coro-style ``read(pos, size)`` (any h5coro driver), or a
    URL opened through ``driver(source, credentials)`` -- defaulting to
    h5coro's own drivers by scheme (``s3://`` -> S3Driver, ``http(s)://`` ->
    HTTPDriver with an EDL bearer token as ``credentials``, else FileDriver),
    so the catalog's ranged GETs take the same credentialed path the worker's
    reads do."""
    if isinstance(source, (bytes, bytearray, memoryview)):
        buf = bytes(source)
        return lambda off, n: buf[off : off + n]
    if hasattr(source, "read") and not isinstance(source, (str, os.PathLike)):
        return source.read
    resource = os.fspath(source)
    if driver is None:
        if resource.startswith("s3://"):
            from h5coro import s3driver

            driver, resource = s3driver.S3Driver, resource[len("s3://") :]
        elif resource.startswith(("http://", "https://")):
            from h5coro import webdriver

            driver = webdriver.HTTPDriver
        else:
            from h5coro import filedriver

            driver = filedriver.FileDriver
    return driver(resource, credentials).read


def parse_header(source, driver=None, credentials=None, *, initial=INITIAL_HEADER_BYTES) -> Header:
    """Parse the header of ``source`` (see :func:`_fetcher` for the forms it
    takes) reading header bytes only. ``initial`` is the first ranged read's
    size (tests shrink it to exercise the on-demand growth)."""
    return _parse(_HeaderBytes(_fetcher(source, driver, credentials), initial))


def describe(source, driver=None, credentials=None) -> dict:
    """Describe a netCDF3 file from its header bytes alone.

    Returns a JSON-ready dict: ``format`` / ``version``, ``numrecs`` (``None``
    when streaming), ``record_dim``, ``header_bytes``, ``dims`` (name ->
    length, the record dimension reporting ``numrecs``), ``attrs`` (global),
    and ``variables`` (name -> ``dtype`` as the byte-order-explicit numpy
    string, ``nc_type``, ``dims``, ``shape``, ``attrs``, ``record``, ``begin``,
    ``vsize``). Attribute values are plain Python: ``char`` attributes decode
    to ``str``, numeric ones to a scalar or a list -- so ``units``,
    ``_FillValue``, ``scale_factor`` and ``add_offset`` read directly. The
    data arrays are never fetched.
    """
    return parse_header(source, driver, credentials).describe()
