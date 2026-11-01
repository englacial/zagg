"""netCDF3 header parser (issue #682, phase 2).

Hermetic: the fixture is written here from the netCDF classic format
specification (no netCDF library in the test env), so the parser is checked
against the spec, not against another implementation of it. ``_nc3_bytes``
lays out ``header | non-record arrays | record block`` exactly as the spec's
``vsize`` / ``begin`` / padding rules say; the on-demand header growth is
exercised by shrinking the first ranged read below the header size.
"""

import struct

import numpy as np
import pytest

from zagg.catalog import netcdf3
from zagg.catalog.netcdf3 import STREAMING, Header, describe, parse_header

# ---------------------------------------------------------------------------
# Spec-driven fixture writer
# ---------------------------------------------------------------------------

NC_BYTE, NC_CHAR, NC_SHORT, NC_INT, NC_FLOAT, NC_DOUBLE = 1, 2, 3, 4, 5, 6
_ITEMSIZE = {NC_BYTE: 1, NC_CHAR: 1, NC_SHORT: 2, NC_INT: 4, NC_FLOAT: 4, NC_DOUBLE: 8}
_DTYPE = {
    NC_BYTE: "|i1",
    NC_CHAR: "|u1",
    NC_SHORT: ">i2",
    NC_INT: ">i4",
    NC_FLOAT: ">f4",
    NC_DOUBLE: ">f8",
}


def _pad4(n):
    return (n + 3) & ~3


def _padded(b):
    return b + b"\0" * (_pad4(len(b)) - len(b))


def _name(s):
    b = s.encode()
    return struct.pack(">I", len(b)) + _padded(b)


def _att_list(attrs):
    """``attrs``: name -> (nc_type, value); value is str for char, else a
    scalar or list."""
    if not attrs:
        return b"\0" * 8
    out = struct.pack(">II", 0x0C, len(attrs))
    for name, (nc_type, value) in attrs.items():
        if nc_type == NC_CHAR:
            raw = value.encode()
            n = len(raw)
        else:
            arr = np.atleast_1d(np.asarray(value, dtype=_DTYPE[nc_type]))
            raw, n = arr.tobytes(), arr.size
        out += _name(name) + struct.pack(">II", nc_type, n) + _padded(raw)
    return out


def _nc3_bytes(version, dims, variables, gattrs=None, *, numrecs=None, streaming=False):
    """Serialize a netCDF3 file from the spec.

    ``dims``: list of ``(name, length)``, length 0 = the record dimension.
    ``variables``: list of dicts ``{name, dimids, nc_type, data, attrs?}``;
    ``data`` is a numpy array of the full shape (record variables include
    the record axis first). Returns ``(bytes, layout)`` where ``layout`` maps
    variable name -> ``(begin, vsize)`` as written, for assertions.
    """
    rec_dim = next((i for i, (_, n) in enumerate(dims) if n == 0), None)

    def is_rec(v):
        return bool(v["dimids"]) and v["dimids"][0] == rec_dim

    def slab(v):
        fixed = [dims[d][1] for d in v["dimids"][1 if is_rec(v) else 0 :]]
        return int(np.prod(fixed, dtype=np.int64)) * _ITEMSIZE[v["nc_type"]]

    recs = [v for v in variables if is_rec(v)]
    if numrecs is None:
        numrecs = int(recs[0]["data"].shape[0]) if recs else 0
    # vsize per the spec ("Note on vsize"): ALWAYS padded to 4, even for a
    # lone record variable -- whose slabs are nonetheless packed unpadded, so
    # there the record stride (recsize) is smaller than vsize (netCDF-C
    # ``ncgen`` writes ``short x(time, 3)`` alone as vsize 8, stride 6).
    vsize = {v["name"]: _pad4(slab(v)) for v in variables}
    stride = {v["name"]: slab(v) if len(recs) == 1 else vsize[v["name"]] for v in recs}
    recsize = sum(stride.values())

    off_size = 4 if version == 1 else 8
    # Header size is needed for ``begin``; it does not depend on begin values.
    header_len = 4 + 4
    header_len += 8 if not dims else 8 + sum(len(_name(n)) + 4 for n, _ in dims)
    header_len += len(_att_list(gattrs or {}))
    header_len += 8
    for v in variables:
        header_len += (
            len(_name(v["name"]))
            + 4
            + 4 * len(v["dimids"])
            + len(_att_list(v.get("attrs") or {}))
            + 4
            + 4
            + off_size
        )
    begin = {}
    pos = header_len
    for v in variables:
        if not is_rec(v):
            begin[v["name"]] = pos
            pos += vsize[v["name"]]
    rec_begin = pos
    for v in recs:
        begin[v["name"]] = pos
        pos += vsize[v["name"]]

    hdr = b"CDF" + bytes([version])
    hdr += struct.pack(">I", STREAMING if streaming else numrecs)
    if dims:
        hdr += struct.pack(">II", 0x0A, len(dims))
        for n, length in dims:
            hdr += _name(n) + struct.pack(">I", length)
    else:
        hdr += b"\0" * 8
    hdr += _att_list(gattrs or {})
    hdr += struct.pack(">II", 0x0B, len(variables)) if variables else b"\0" * 8
    for v in variables:
        hdr += _name(v["name"]) + struct.pack(">I", len(v["dimids"]))
        hdr += b"".join(struct.pack(">I", d) for d in v["dimids"])
        hdr += _att_list(v.get("attrs") or {})
        hdr += struct.pack(">II", v["nc_type"], vsize[v["name"]])
        hdr += struct.pack(">I" if version == 1 else ">q", begin[v["name"]])
    assert len(hdr) == header_len

    body = bytearray()
    for v in variables:
        if not is_rec(v):
            assert len(body) + header_len == begin[v["name"]]
            body += _padded(np.ascontiguousarray(v["data"], dtype=_DTYPE[v["nc_type"]]).tobytes())
    assert len(body) + header_len == rec_begin
    for r in range(numrecs):
        for v in recs:
            raw = np.ascontiguousarray(v["data"][r], dtype=_DTYPE[v["nc_type"]]).tobytes()
            body += raw + b"\0" * (stride[v["name"]] - len(raw))
    assert len(body) == (rec_begin - header_len) + numrecs * recsize
    layout = {v["name"]: (begin[v["name"]], vsize[v["name"]]) for v in variables}
    layout["__recsize__"] = recsize
    layout["__header__"] = header_len
    return bytes(hdr) + bytes(body), layout


# A GLOBAL-attribute, two non-record + two record variable fixture; the
# record variables have slabs that need padding (short x 3 = 6 bytes -> 8),
# and the char variable exercises the ``|u1`` mapping.
_DIMS = [("time", 0), ("x", 3), ("y", 2), ("strlen", 5)]
_TIME = np.array([[10.0], [20.0], [30.0], [40.0]], dtype=">f8").reshape(4)
_TEMP = (np.arange(12, dtype=">i2") * 100 - 300).reshape(4, 3)
_LAT = np.array([-1.5, 0.0, 1.5], dtype=">f4")
_GRID = np.arange(6, dtype=">i4").reshape(3, 2)
_LABEL = np.frombuffer(b"abcde", dtype="|u1")
_VARS = [
    {
        "name": "lat",
        "dimids": [1],
        "nc_type": NC_FLOAT,
        "data": _LAT,
        "attrs": {"units": (NC_CHAR, "degrees_north"), "_FillValue": (NC_FLOAT, -999.0)},
    },
    {"name": "grid", "dimids": [1, 2], "nc_type": NC_INT, "data": _GRID},
    {"name": "label", "dimids": [3], "nc_type": NC_CHAR, "data": _LABEL},
    {
        "name": "time",
        "dimids": [0],
        "nc_type": NC_DOUBLE,
        "data": _TIME,
        "attrs": {"units": (NC_CHAR, "seconds since 2000-01-01")},
    },
    {
        "name": "temp",
        "dimids": [0, 1],
        "nc_type": NC_SHORT,
        "data": _TEMP,
        "attrs": {
            "scale_factor": (NC_DOUBLE, 0.01),
            "add_offset": (NC_DOUBLE, 273.15),
            "valid_range": (NC_SHORT, [-400, 900]),
        },
    },
]
_GATTRS = {"title": (NC_CHAR, "netCDF3 fixture"), "Conventions": (NC_CHAR, "CF-1.6")}


def fixture(version=1, **kw):
    return _nc3_bytes(version, _DIMS, _VARS, _GATTRS, **kw)


# A wide, many-record-variable case: byte, char, short and a [time, 3200]
# float waveform interleaved with a double, so one record mixes 1-, 5-, 6-,
# 12800- and 8-byte slabs (padded to 4 each), plus a non-record byte.
_MDIMS = [("time", 0), ("n", 3200), ("s", 5), ("k", 3)]
_NREC = 7
_MULTI = {
    "b": (NC_BYTE, [0], np.array([1, -2, 3, -4, 5, -6, 7], dtype="|i1")),
    "name": (NC_CHAR, [0, 2], np.frombuffer(b"abcdefghijklmnopqrstuvwxyzABCDEFGHI", "|u1")),
    "sh": (NC_SHORT, [0, 3], np.arange(1, 22, dtype=">i2")),
    "wf": (NC_FLOAT, [0, 1], (np.arange(_NREC * 3200) % 1000).astype(">f4")),
    "t": (NC_DOUBLE, [0], np.arange(_NREC, dtype=">f8") * 0.5),
    "fixb": (NC_BYTE, [3], np.array([-128, 0, 127], dtype="|i1")),
}


def _multi_expected():
    out = {}
    for name, (_, dimids, flat) in _MULTI.items():
        shape = [_NREC if d == 0 else _MDIMS[d][1] for d in dimids]
        out[f"/{name}"] = flat.reshape(shape)
    return out


def _check_multi(data):
    """Full and partial reconstruction of the ``_MULTI`` layout from ``data``."""
    from zagg.catalog.netcdf3 import build_manifest

    exp = _multi_expected()
    idx, out = _reconstruct(build_manifest(data), data)
    assert set(out) == set(exp)
    for name, arr in exp.items():
        assert out[name].tobytes() == arr.astype(out[name].dtype).tobytes(), name
    assert out["/b"].dtype == np.int8 and out["/fixb"].dtype == np.int8
    for name in ("/wf", "/name", "/b", "/sh", "/t"):
        for lo, hi in ((2, 5), (6, 7), (0, 1)):
            addrs, sizes, _ = idx.read_plan(name, lo, hi)
            bufs = [data[int(a) : int(a) + int(n)] for a, n in zip(addrs, sizes)]
            got = idx.read_from_buffers(name, bufs, lo, hi)
            np.testing.assert_array_equal(got, exp[name][lo:hi], err_msg=f"{name}[{lo}:{hi}]")
    return idx


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDescribe:
    @pytest.mark.parametrize("version", [1, 2])
    def test_dims_attrs_variables(self, version):
        data, layout = fixture(version)
        d = describe(data)
        assert d["version"] == version
        assert d["format"] == {1: "classic", 2: "64-bit offset"}[version]
        assert d["numrecs"] == 4
        assert d["record_dim"] == "time"
        assert d["dims"] == {"time": 4, "x": 3, "y": 2, "strlen": 5}
        assert d["attrs"] == {"title": "netCDF3 fixture", "Conventions": "CF-1.6"}
        assert list(d["variables"]) == ["lat", "grid", "label", "time", "temp"]
        lat = d["variables"]["lat"]
        assert lat == {
            "dtype": ">f4",
            "nc_type": "float",
            "dims": ["x"],
            "shape": [3],
            "attrs": {"units": "degrees_north", "_FillValue": -999.0},
            "record": False,
            "begin": layout["lat"][0],
            "vsize": layout["lat"][1],
        }
        temp = d["variables"]["temp"]
        assert temp["dtype"] == ">i2" and temp["record"] is True
        assert temp["shape"] == [4, 3] and temp["dims"] == ["time", "x"]
        assert temp["attrs"]["scale_factor"] == 0.01
        assert temp["attrs"]["valid_range"] == [-400, 900]
        assert d["variables"]["label"]["dtype"] == "|u1"
        assert d["variables"]["grid"]["shape"] == [3, 2]
        assert d["header_bytes"] == layout["__header__"]

    def test_begin_offsets_match_the_layout(self):
        data, layout = fixture(2)
        d = describe(data)
        for name, (begin, vsize) in ((k, v) for k, v in layout.items() if not k.startswith("__")):
            assert d["variables"][name]["begin"] == begin
            assert d["variables"][name]["vsize"] == vsize

    def test_64bit_offset_reads_8_byte_begin(self):
        # The only grammar difference between v1 and v2: a v2 header is 4
        # bytes longer per variable, and parses to the same description.
        d1, d2 = describe(fixture(1)[0]), describe(fixture(2)[0])
        assert d2["header_bytes"] - d1["header_bytes"] == 4 * len(_VARS)
        strip = {k: v for k, v in d1.items() if k not in ("version", "format", "header_bytes")}
        for name in strip["variables"]:
            strip["variables"][name].pop("begin")
            d2["variables"][name].pop("begin")
        assert strip == {k: v for k, v in d2.items() if k in strip}

    def test_json_ready(self):
        import json

        json.dumps(describe(fixture()[0]))

    def test_streaming_numrecs(self):
        d = describe(fixture(streaming=True)[0])
        assert d["numrecs"] is None
        assert d["dims"]["time"] is None
        assert d["variables"]["temp"]["shape"] == [None, 3]

    def test_char_attribute_trailing_nuls_stripped(self):
        # nelems counts the NULs (a C writer passing strlen+1, or fixed-width
        # NUL padding); the value compares equal to the bare string.
        v = {"name": "lat", "dimids": [0], "nc_type": NC_FLOAT, "data": _LAT}
        v["attrs"] = {"units": (NC_CHAR, "degrees_north\x00"), "pad": (NC_CHAR, "ab\x00\x00\x00")}
        data, _ = _nc3_bytes(1, [("x", 3)], [v], {"title": (NC_CHAR, "t\x00")})
        assert data.count(b"degrees_north\x00") == 1
        d = describe(data)
        assert d["variables"]["lat"]["attrs"] == {"units": "degrees_north", "pad": "ab"}
        assert d["attrs"] == {"title": "t"}

    def test_scalar_variable(self):
        # The CF grid-mapping ``int crs`` every gridded product carries.
        crs = {"name": "crs", "dimids": [], "nc_type": NC_INT, "data": np.array(7)}
        crs["attrs"] = {"grid_mapping_name": (NC_CHAR, "polar_stereographic")}
        data, layout = _nc3_bytes(1, _DIMS, [crs, *_VARS])
        hdr = parse_header(data)
        assert hdr.slab_bytes(hdr.variables[0]) == 4
        v = describe(data)["variables"]["crs"]
        assert v["dims"] == [] and v["shape"] == [] and v["record"] is False
        assert (v["begin"], v["vsize"]) == layout["crs"] and v["vsize"] == 4
        assert v["attrs"] == {"grid_mapping_name": "polar_stereographic"}

    def test_empty_attributes(self):
        gattrs = {"none": (NC_INT, []), "blank": (NC_CHAR, ""), "after": (NC_SHORT, 3)}
        data, _ = _nc3_bytes(1, [("x", 3)], [], gattrs)
        assert describe(data)["attrs"] == {"none": [], "blank": "", "after": 3}

    def test_empty_lists(self):
        data, _ = _nc3_bytes(1, [], [], {})
        d = describe(data)
        assert d["dims"] == {} and d["attrs"] == {} and d["variables"] == {}
        assert d["record_dim"] is None and d["numrecs"] == 0


class TestHeaderBytesOnly:
    def test_reads_only_the_header_from_a_large_object(self):
        # Header bytes only: with the default initial window the parse costs
        # ONE ranged read of INITIAL_HEADER_BYTES, never the data arrays.
        data, layout = fixture()
        calls = []

        def fetch(off, n):
            calls.append((off, n))
            return data[off : off + n]

        hdr = netcdf3._parse(netcdf3._HeaderBytes(fetch))
        assert hdr.header_bytes == layout["__header__"]
        assert calls == [(0, netcdf3.INITIAL_HEADER_BYTES)]

    def test_grows_on_demand(self):
        data, layout = fixture()
        calls = []

        def fetch(off, n):
            calls.append((off, n))
            return data[off : off + n]

        hdr = netcdf3._parse(netcdf3._HeaderBytes(fetch, initial=16))
        assert hdr.header_bytes == layout["__header__"]
        assert len(calls) > 1
        # Contiguous, doubling windows: each read starts where the last ended.
        assert all(a[0] + a[1] == b[0] for a, b in zip(calls, calls[1:]))
        assert all(b[1] >= a[1] for a, b in zip(calls, calls[1:]))
        # Every read starts inside the header: nothing is fetched because the
        # data arrays were wanted, only because a header field ran past the window.
        assert calls[0] == (0, 16)
        assert all(off < layout["__header__"] for off, _ in calls)

    def test_parse_header_initial_kwarg(self):
        data, layout = fixture()
        assert parse_header(data, initial=16).header_bytes == layout["__header__"]

    def test_truncated_header_raises(self):
        data, layout = fixture()
        with pytest.raises(ValueError, match="truncated netCDF3 header"):
            describe(data[: layout["__header__"] - 3])

    def test_corrupt_length_never_reads_past_the_cap(self):
        # A bit-flipped name length must not become a multi-GB ranged GET:
        # the object here is "huge" (zeros past the real bytes), so only the
        # cap stops the read.
        data, _ = fixture()
        bad = bytearray(data)
        bad[16:20] = struct.pack(">I", 0xFFFFFF00)  # first dim's name length
        calls = []

        def fetch(off, n):
            calls.append((off, n))
            return (bytes(bad) + b"\0" * (off + n))[off : off + n]

        with pytest.raises(ValueError, match="corrupt netCDF3 header: field runs to byte"):
            netcdf3._parse(netcdf3._HeaderBytes(fetch))
        assert calls and max(off + n for off, n in calls) <= netcdf3.MAX_HEADER_BYTES

    def test_growth_is_clamped_to_the_cap(self):
        calls = []

        def fetch(off, n):
            calls.append((off, n))
            return b"\0" * n

        # Doubling from just past half the cap would overshoot it; it is clamped.
        half = netcdf3.MAX_HEADER_BYTES // 2
        hb = netcdf3._HeaderBytes(fetch)
        hb._ensure(half + 8)
        hb._ensure(half + 16)
        assert calls == [(0, half + 8), (half + 8, half - 8)]

    def test_failed_read_is_an_oserror(self):
        with pytest.raises(
            OSError, match=r"ranged read failed at 0\+65536 \(driver returned None\)"
        ):
            netcdf3._parse(netcdf3._HeaderBytes(lambda off, n: None))

    def test_local_path_source(self, tmp_path):
        data, _ = fixture()
        p = tmp_path / "f.nc"
        p.write_bytes(data)
        assert describe(str(p)) == describe(data)
        assert describe(p) == describe(data)

    def test_driver_object_source(self):
        # Anything with an h5coro-style read(pos, size) is a source.
        data, _ = fixture()

        class Drv:
            def read(self, pos, size):
                return data[pos : pos + size]

        assert describe(Drv()) == describe(data)

    def test_explicit_driver_class(self):
        data, _ = fixture()
        seen = {}

        class Drv:
            def __init__(self, resource, credentials):
                seen.update(resource=resource, credentials=credentials)

            def read(self, pos, size):
                return data[pos : pos + size]

        assert describe("https://x/y.nc", driver=Drv, credentials="tok") == describe(data)
        assert seen == {"resource": "https://x/y.nc", "credentials": "tok"}


class TestSchemeDrivers:
    """``_fetcher``'s default h5coro driver per URL scheme."""

    @pytest.fixture
    def drivers(self, monkeypatch):
        from h5coro import filedriver, s3driver, webdriver

        data, _ = fixture()
        seen = []

        def fake(kind):
            class Drv:
                def __init__(self, resource, credentials):
                    seen.append((kind, resource, credentials))

                def read(self, pos, size):
                    return data[pos : pos + size]

            return Drv

        monkeypatch.setattr(s3driver, "S3Driver", fake("s3"))
        monkeypatch.setattr(webdriver, "HTTPDriver", fake("http"))
        monkeypatch.setattr(filedriver, "FileDriver", fake("file"))
        return data, seen

    def test_s3_strips_scheme_and_defaults_to_ambient_session(self, drivers):
        data, seen = drivers
        assert describe("s3://bucket/key.nc") == describe(data)
        assert seen == [("s3", "bucket/key.nc", {})]
        describe("s3://bucket/key.nc", credentials={"role": True})
        assert seen[-1] == ("s3", "bucket/key.nc", {"role": True})

    def test_https_keeps_the_url(self, drivers):
        data, seen = drivers
        assert describe("https://host/a/b.nc", credentials="tok") == describe(data)
        assert seen == [("http", "https://host/a/b.nc", "tok")]

    def test_local_path_goes_to_file_driver(self, drivers, tmp_path):
        data, seen = drivers
        p = str(tmp_path / "f.nc")
        assert describe(p) == describe(data)
        assert seen == [("file", p, None)]


class TestRefusals:
    def test_not_netcdf(self):
        with pytest.raises(ValueError, match="not a netCDF3 file"):
            describe(b"\x89HDF\r\n\x1a\n" + b"\0" * 64)

    def test_cdf5_refused(self):
        with pytest.raises(ValueError, match="CDF-5"):
            describe(b"CDF\x05" + b"\0" * 64)

    def test_unknown_version(self):
        with pytest.raises(ValueError, match="unknown version byte 3"):
            describe(b"CDF\x03" + b"\0" * 64)

    def test_bad_tag(self):
        data, _ = fixture()
        bad = bytearray(data)
        bad[8:12] = struct.pack(">I", 0x0C)  # dim_list tagged as NC_ATTRIBUTE
        with pytest.raises(ValueError, match="expected dim_list tag"):
            describe(bytes(bad))

    def test_absent_with_count(self):
        bad = b"CDF\x01" + struct.pack(">I", 0) + struct.pack(">II", 0, 2) + b"\0" * 32
        with pytest.raises(ValueError, match="ABSENT dim_list with count 2"):
            describe(bad)

    def test_dimid_out_of_range(self):
        data, _ = _nc3_bytes(
            1, [("x", 2)], [{"name": "v", "dimids": [0], "nc_type": NC_INT, "data": np.zeros(2)}]
        )
        bad = bytearray(data)
        i = data.index(_name("v")) + len(_name("v")) + 4
        bad[i : i + 4] = struct.pack(">I", 7)
        with pytest.raises(ValueError, match="dimid 7 out of range"):
            describe(bytes(bad))

    def test_unsupported_nc_type(self):
        data, _ = _nc3_bytes(
            1, [("x", 2)], [{"name": "v", "dimids": [0], "nc_type": NC_INT, "data": np.zeros(2)}]
        )
        bad = bytearray(data)
        i = data.index(_name("v")) + len(_name("v")) + 4 + 4 + 8
        bad[i : i + 4] = struct.pack(">I", 10)  # NC_INT64 (CDF-5 only)
        with pytest.raises(ValueError, match="unsupported nc_type 10"):
            describe(bytes(bad))

    def test_record_dim_not_first(self):
        v = {"name": "v", "dimids": [1, 0], "nc_type": NC_INT, "data": np.zeros((2, 0))}
        data, _ = _nc3_bytes(1, [("time", 0), ("x", 2)], [v], numrecs=0)
        with pytest.raises(ValueError, match="'v': the record dimension may only be its first"):
            describe(data)

    @staticmethod
    def _gatt_at(gattrs=None):
        """A one-dim, no-variable file and the offset of its gatt_list tag."""
        data, _ = _nc3_bytes(1, [("x", 2)], [], gattrs or {})
        return bytearray(data), 8 + 8 + len(_name("x")) + 4

    def test_att_list_bad_tag(self):
        bad, i = self._gatt_at()
        bad[i : i + 4] = struct.pack(">I", 0x0B)
        with pytest.raises(ValueError, match="expected NC_ATTRIBUTE tag, got 0xb"):
            describe(bytes(bad))

    def test_att_list_absent_with_count(self):
        bad, i = self._gatt_at()
        assert bad[i : i + 8] == b"\0" * 8
        bad[i + 4 : i + 8] = struct.pack(">I", 3)
        with pytest.raises(ValueError, match="ABSENT att_list with count 3"):
            describe(bytes(bad))

    def test_attribute_unsupported_nc_type(self):
        bad, i = self._gatt_at({"a": (NC_INT, 1)})
        assert struct.unpack(">II", bad[i : i + 8]) == (0x0C, 1)
        j = i + 8 + len(_name("a"))
        bad[j : j + 4] = struct.pack(">I", 10)
        with pytest.raises(ValueError, match="attribute 'a': unsupported nc_type 10"):
            describe(bytes(bad))

    def test_two_record_dims(self):
        data, _ = _nc3_bytes(1, [("a", 0), ("b", 0)], [])
        with pytest.raises(ValueError, match="more than one record"):
            describe(data)


class TestHeaderLayout:
    """The size arithmetic the manifest emitter builds on (phase 3)."""

    def test_recsize_pads_each_slab_to_4(self):
        data, layout = fixture()
        hdr = parse_header(data)
        # time: 8 bytes; temp: 3 x int16 = 6 -> padded to 8.
        assert hdr.recsize == 16 == layout["__recsize__"]
        assert [hdr.slab_bytes(v) for v in hdr.record_variables] == [8, 6]

    def test_single_record_variable_is_unpadded(self):
        only = [v for v in _VARS if v["name"] == "temp"]
        data, layout = _nc3_bytes(1, _DIMS, only)
        hdr = parse_header(data)
        assert hdr.recsize == 6 == layout["__recsize__"]
        # vsize stays padded (the netCDF-C layout); the stride does not.
        assert hdr.variables[0].vsize == 8
        assert len(data) == layout["__header__"] + 4 * 6

    def test_non_record_slab_is_the_whole_array(self):
        hdr = parse_header(fixture()[0])
        by = {v.name: v for v in hdr.variables}
        assert hdr.slab_bytes(by["grid"]) == 6 * 4
        assert hdr.slab_bytes(by["label"]) == 5
        assert by["label"].vsize == 8  # padded on disk
        assert hdr.shape(by["grid"]) == [3, 2] and not hdr.is_record(by["grid"])

    def test_header_is_frozen_dataclass(self):
        hdr = parse_header(fixture()[0])
        assert isinstance(hdr, Header)
        with pytest.raises(AttributeError):
            hdr.numrecs = 1  # type: ignore[misc]


class TestOracle:
    """Cross-check the hand-written fixture against a netCDF library when one
    is installed (never in CI's env: scipy is not a zagg dependency)."""

    def test_scipy_reads_the_fixture(self, tmp_path):
        nc = pytest.importorskip("scipy.io")
        for version in (1, 2):
            data, _ = fixture(version)
            p = tmp_path / f"v{version}.nc"
            p.write_bytes(data)
            with nc.netcdf_file(str(p), "r", mmap=False) as f:
                assert f.title == b"netCDF3 fixture"
                assert dict(f.dimensions) == {"time": None, "x": 3, "y": 2, "strlen": 5}
                np.testing.assert_array_equal(f.variables["temp"][:], _TEMP)
                np.testing.assert_array_equal(f.variables["time"][:], _TIME)
                np.testing.assert_array_equal(f.variables["grid"][:], _GRID)
                np.testing.assert_array_equal(f.variables["lat"][:], _LAT)
                assert f.variables["label"][:].tobytes() == b"abcde"
                assert f.variables["temp"].scale_factor == 0.01

    @pytest.mark.parametrize("kind", ["classic", "64-bit offset"])
    def test_ncgen_lone_record_variable(self, tmp_path, kind):
        # netCDF-C (not scipy) is the layout authority for vsize: a lone
        # record variable carries a padded vsize but an unpadded stride.
        import shutil
        import subprocess

        if shutil.which("ncgen") is None:
            pytest.skip("ncgen (netCDF-C) not installed")
        cdl = tmp_path / "one.cdl"
        cdl.write_text(
            "netcdf one {\ndimensions:\n time = UNLIMITED ; x = 3 ;\nvariables:\n"
            " short temp(time, x) ;\n float lat(x) ;\n"
            "data:\n temp = 1,2,3,4,5,6 ;\n lat = 1,2,3 ;\n}\n"
        )
        nc = tmp_path / "one.nc"
        subprocess.run(["ncgen", "-k", kind, "-o", str(nc), str(cdl)], check=True)
        data = nc.read_bytes()
        hdr = parse_header(data)
        temp = next(v for v in hdr.variables if v.name == "temp")
        assert (temp.vsize, hdr.recsize) == (8, 6)
        assert len(data) == temp.begin + 2 * 6
        from zagg.catalog.netcdf3 import build_manifest

        _, out = _reconstruct(build_manifest(data), data)
        np.testing.assert_array_equal(out["/temp"], [[1, 2, 3], [4, 5, 6]])
        np.testing.assert_array_equal(out["/lat"], [1.0, 2.0, 3.0])

    @pytest.mark.parametrize("kind", ["classic", "64-bit offset"])
    def test_ncgen_multi_record_variables(self, tmp_path, kind):
        # netCDF-C as the layout authority for the PADDED case: five record
        # variables with 1-, 5-, 6-, 12800- and 8-byte slabs, plus a
        # non-record byte and a (skipped) scalar.
        import shutil
        import subprocess

        if shutil.which("ncgen") is None:
            pytest.skip("ncgen (netCDF-C) not installed")
        exp = _multi_expected()
        ctype = {NC_BYTE: "byte", NC_CHAR: "char", NC_SHORT: "short", NC_FLOAT: "float"}
        ctype[NC_DOUBLE] = "double"
        dnames = [n for n, _ in _MDIMS]
        decl, vals = [], []
        for name, (t, dimids, _) in _MULTI.items():
            decl.append(f" {ctype[t]} {name}({', '.join(dnames[d] for d in dimids)}) ;")
            arr = exp[f"/{name}"]
            if t == NC_CHAR:
                body = ", ".join(f'"{r.tobytes().decode()}"' for r in arr)
            else:
                body = ", ".join(str(x) for x in arr.ravel().tolist())
            vals.append(f" {name} = {body} ;")
        cdl = tmp_path / "multi.cdl"
        cdl.write_text(
            "netcdf multi {\ndimensions:\n time = UNLIMITED ; n = 3200 ; s = 5 ; k = 3 ;\n"
            "variables:\n"
            + "\n".join(decl)
            + "\n int sc ;\ndata:\n"
            + "\n".join(vals)
            + "\n sc = 42 ;\n}\n"
        )
        nc = tmp_path / "multi.nc"
        subprocess.run(["ncgen", "-k", kind, "-o", str(nc), str(cdl)], check=True)
        data = nc.read_bytes()
        hdr = parse_header(data)
        assert hdr.numrecs == _NREC and hdr.recsize == 4 + 8 + 8 + 12800 + 8
        _check_multi(data)


# ---------------------------------------------------------------------------
# Phase 3: manifest emitter
# ---------------------------------------------------------------------------


def _reconstruct(df, data):
    """``datasets_from_manifest`` + ``Index.from_chunks`` + ranged reads out of
    ``data`` -- exactly the sidecar backend's path, minus the driver."""
    from h5coro_hidefix import Index
    from h5coro_hidefix.manifest import datasets_from_manifest

    idx = Index.from_chunks("mem", datasets_from_manifest(df))
    out = {}
    for name in idx.datasets():
        addrs, sizes, _ = idx.read_plan(name, None, None)
        bufs = [data[int(a) : int(a) + int(n)] for a, n in zip(addrs, sizes)]
        out[name] = idx.read_from_buffers(name, bufs, None, None)
    return idx, out


_EXPECTED = {"/lat": _LAT, "/grid": _GRID, "/label": _LABEL, "/time": _TIME, "/temp": _TEMP}


class TestBuildManifest:
    @pytest.mark.parametrize("version", [1, 2])
    def test_rows(self, version):
        from zagg.catalog.netcdf3 import build_manifest
        from zagg.index.inline import MANIFEST_DTYPES

        data, layout = fixture(version)
        df = build_manifest(data)
        assert list(df.columns) == list(MANIFEST_DTYPES)
        assert dict(df.dtypes.astype(str)) == MANIFEST_DTYPES
        # One row per non-record variable; two interleaved record variables -> one per record.
        assert df["dataset"].value_counts().to_dict() == {
            "/lat": 1,
            "/grid": 1,
            "/label": 1,
            "/time": 4,
            "/temp": 4,
        }
        lat = df[df["dataset"] == "/lat"].iloc[0]
        assert (lat["byte_offset"], lat["nbytes"]) == (layout["lat"][0], 12)
        assert (lat["chunk_idx"], lat["elem_start"], lat["elem_end"]) == (0, 0, 3)
        assert (lat["dtype"], lat["shape"], lat["chunk_shape"]) == (">f4", "[3]", "[3]")
        assert lat["chunk_offset"] == "[0]" and lat["filter_mask"] == 0
        assert not lat["gzip"] and not lat["shuffle"]
        grid = df[df["dataset"] == "/grid"].iloc[0]
        assert (grid["dtype"], grid["shape"], grid["chunk_shape"]) == (">i4", "[3, 2]", "[3, 2]")
        assert grid["chunk_offset"] == "[0, 0]" and grid["nbytes"] == 24
        label = df[df["dataset"] == "/label"].iloc[0]
        assert label["dtype"] == "|u1" and label["nbytes"] == 5  # unpadded (vsize is 8)
        temp = df[df["dataset"] == "/temp"].sort_values("chunk_idx")
        recsize = layout["__recsize__"]
        assert list(temp["byte_offset"]) == [layout["temp"][0] + r * recsize for r in range(4)]
        assert list(temp["nbytes"]) == [6] * 4
        assert list(temp["chunk_idx"]) == [0, 1, 2, 3]
        assert list(temp["elem_start"]) == [0, 1, 2, 3] and list(temp["elem_end"]) == [1, 2, 3, 4]
        assert set(temp["shape"]) == {"[4, 3]"} and set(temp["chunk_shape"]) == {"[1, 3]"}
        assert list(temp["chunk_offset"]) == ["[0, 0]", "[1, 0]", "[2, 0]", "[3, 0]"]
        assert set(temp["dtype"]) == {">i2"}
        time = df[df["dataset"] == "/time"].sort_values("chunk_idx")
        assert list(time["byte_offset"]) == [layout["time"][0] + r * recsize for r in range(4)]
        assert set(time["chunk_shape"]) == {"[1]"} and set(time["shape"]) == {"[4]"}
        assert df["dtype"].str.match(r"^[<>|][a-z]\d+$").all()
        assert df.equals(df.sort_values(["dataset", "chunk_idx"], ignore_index=True))

    @pytest.mark.parametrize("version", [1, 2])
    def test_reconstructs_every_variable_byte_identically(self, version):
        # The acceptance bar: the sidecar consumer's reconstruction
        # (datasets_from_manifest -> Index.from_chunks -> read_plan ->
        # read_from_buffers) returns every variable's values from the raw
        # file bytes, record and non-record alike.
        from zagg.catalog.netcdf3 import build_manifest

        data, _ = fixture(version)
        idx, out = _reconstruct(build_manifest(data), data)
        assert set(out) == set(_EXPECTED)
        for name, exp in _EXPECTED.items():
            got = out[name]
            assert got.shape == exp.shape, name
            assert got.tobytes() == exp.astype(got.dtype).tobytes(), name
            np.testing.assert_array_equal(got, exp)
        # Decode is value-identical; the compiled reader hands back native
        # byte order, so the dtype KIND and width match, not the '>' flag.
        assert out["/temp"].dtype == np.int16 and out["/time"].dtype == np.float64
        assert out["/label"].tobytes() == b"abcde"

    def test_partial_record_range_reads_only_those_slabs(self):
        from h5coro_hidefix import Index
        from h5coro_hidefix.manifest import datasets_from_manifest

        from zagg.catalog.netcdf3 import build_manifest

        data, layout = fixture()
        idx = Index.from_chunks("mem", datasets_from_manifest(build_manifest(data)))
        addrs, sizes, _ = idx.read_plan("/temp", 1, 3)
        assert list(addrs) == [layout["temp"][0] + r * layout["__recsize__"] for r in (1, 2)]
        bufs = [data[int(a) : int(a) + int(n)] for a, n in zip(addrs, sizes)]
        np.testing.assert_array_equal(idx.read_from_buffers("/temp", bufs, 1, 3), _TEMP[1:3])

    @pytest.mark.parametrize("version", [1, 2])
    def test_byte_and_wide_record_variables(self, version):
        # NC_BYTE (|i1) record and non-record, and a wide [time, 3200] record
        # variable whose slab is one chunk across the trailing axis.
        exp = _multi_expected()
        variables = [
            {"name": n, "nc_type": t, "dimids": d, "data": exp[f"/{n}"]}
            for n, (t, d, _) in _MULTI.items()
        ]
        data, layout = _nc3_bytes(version, _MDIMS, variables)
        assert layout["__recsize__"] == 4 + 8 + 8 + 12800 + 8
        idx = _check_multi(data)
        addrs, sizes, _ = idx.read_plan("/wf", 2, 5)
        rec = layout["__recsize__"]
        assert list(addrs) == [layout["wf"][0] + r * rec for r in (2, 3, 4)]
        assert list(sizes) == [12800] * 3

    def test_single_record_variable_unpadded_layout(self):
        from zagg.catalog.netcdf3 import build_manifest

        only = [v for v in _VARS if v["name"] in ("lat", "temp")]
        data, layout = _nc3_bytes(2, _DIMS, only)
        df = build_manifest(data)
        # A lone record variable's slabs are packed with no gap: ONE row
        # spanning the whole record block, not one per record.
        temp = df[df["dataset"] == "/temp"]
        assert len(temp) == 1
        row = temp.iloc[0]
        assert (row["byte_offset"], row["nbytes"]) == (layout["temp"][0], 4 * 6)
        assert (row["chunk_idx"], row["elem_start"], row["elem_end"]) == (0, 0, 4)
        assert row["shape"] == row["chunk_shape"] == "[4, 3]" and row["chunk_offset"] == "[0, 0]"
        idx, out = _reconstruct(df, data)
        np.testing.assert_array_equal(out["/temp"], _TEMP)
        np.testing.assert_array_equal(out["/lat"], _LAT)
        # Record ranges still decode exactly out of the one chunk.
        for lo, hi in ((1, 3), (0, 1), (3, 4)):
            addrs, sizes, _ = idx.read_plan("/temp", lo, hi)
            bufs = [data[int(a) : int(a) + int(n)] for a, n in zip(addrs, sizes)]
            np.testing.assert_array_equal(
                idx.read_from_buffers("/temp", bufs, lo, hi), _TEMP[lo:hi]
            )

    def test_streaming_numrecs_from_file_size(self):
        from zagg.catalog.netcdf3 import build_manifest

        data, _ = fixture(streaming=True)
        df = build_manifest(data)  # bytes: file_size is len(data)
        assert (df["dataset"] == "/temp").sum() == 4
        _, out = _reconstruct(df, data)
        np.testing.assert_array_equal(out["/temp"], _TEMP)

        class Drv:
            def read(self, pos, size):
                return data[pos : pos + size]

        with pytest.raises(ValueError, match="STREAMING numrecs; pass file_size"):
            build_manifest(Drv())
        assert (build_manifest(Drv(), file_size=len(data))["dataset"] == "/temp").sum() == 4
        # A trailing partial record is not a record.
        assert (build_manifest(Drv(), file_size=len(data) - 1)["dataset"] == "/temp").sum() == 3

    def test_scalar_and_empty_record_variables_skipped(self, caplog):
        from zagg.catalog.netcdf3 import build_manifest

        vars_ = [
            {"name": "s", "dimids": [], "nc_type": NC_DOUBLE, "data": np.array(1.5)},
            {"name": "x", "dimids": [1], "nc_type": NC_INT, "data": np.arange(3)},
            {"name": "r", "dimids": [0], "nc_type": NC_INT, "data": np.zeros((0,))},
        ]
        data, _ = _nc3_bytes(1, _DIMS, vars_, numrecs=0)
        with caplog.at_level("INFO", logger="zagg.catalog.netcdf3"):
            df = build_manifest(data)
        assert list(df["dataset"]) == ["/x"]
        assert "skipping scalar variable 's'" in caplog.text
        assert "skipping record variable 'r' (0 records)" in caplog.text
        _, out = _reconstruct(df, data)
        np.testing.assert_array_equal(out["/x"], np.arange(3))

    def test_empty_file_gives_typed_empty_frame(self):
        from zagg.catalog.netcdf3 import build_manifest
        from zagg.index.inline import MANIFEST_DTYPES

        df = build_manifest(_nc3_bytes(1, [], [])[0])
        assert len(df) == 0 and list(df.columns) == list(MANIFEST_DTYPES)
        assert df["chunk_idx"].dtype == np.int64

    def test_cdf5_refused(self):
        from zagg.catalog.netcdf3 import build_manifest

        with pytest.raises(ValueError, match="CDF-5"):
            build_manifest(b"CDF\x05" + b"\0" * 64)

    def test_header_bytes_only(self):
        # The emitter never fetches past the header window.
        from zagg.catalog.netcdf3 import build_manifest

        data, layout = fixture()
        calls = []

        class Drv:
            def read(self, pos, size):
                calls.append((pos, size))
                return data[pos : pos + size]

        build_manifest(Drv(), file_size=len(data))
        assert calls == [(0, netcdf3.INITIAL_HEADER_BYTES)]


class TestIndexGranule:
    def test_writes_through_the_inline_writer(self, tmp_path):
        import pandas as pd

        from zagg.catalog.netcdf3 import build_manifest, index_granule
        from zagg.index.inline import write_manifest

        data, _ = fixture(2)
        src = tmp_path / "PRODUCT_v1_20200101.nc"
        src.write_bytes(data)
        store = tmp_path / "sidecar"
        key = index_granule(str(src), str(store))
        assert key == "PRODUCT_v1_20200101.parquet"
        direct = tmp_path / "direct"
        write_manifest(build_manifest(data), str(direct), "PRODUCT_v1_20200101")
        assert (store / key).read_bytes() == (direct / key).read_bytes()
        df = pd.read_parquet(store / key, engine="fastparquet")
        _, out = _reconstruct(df, data)
        np.testing.assert_array_equal(out["/temp"], _TEMP)

    def test_url_source_through_driver(self, tmp_path):
        from zagg.catalog.netcdf3 import index_granule

        data, _ = fixture()

        class Drv:
            def __init__(self, resource, credentials):
                self.resource = resource

            def read(self, pos, size):
                return data[pos : pos + size]

        key = index_granule("https://x/path/G.nc", str(tmp_path), driver=Drv, credentials="t")
        assert key == "G.parquet" and (tmp_path / "G.parquet").exists()

    def test_bytes_source_rejected(self, tmp_path):
        from zagg.catalog.netcdf3 import index_granule

        with pytest.raises(TypeError, match="path or URL"):
            index_granule(fixture()[0], str(tmp_path))


class TestIndexGranules:
    def test_pooled_build_records_each_key_and_tolerates_a_bad_granule(self, tmp_path, caplog):
        from zagg.catalog.netcdf3 import index_granules

        good = []
        for i in range(3):
            p = tmp_path / f"G{i}.nc"
            p.write_bytes(fixture(1 + i % 2)[0])
            good.append(str(p))
        bad = tmp_path / "junk.nc"
        bad.write_bytes(b"\x89HDF\r\n\x1a\n" + b"\0" * 64)
        store = tmp_path / "store"
        with caplog.at_level("WARNING", logger="zagg.catalog.netcdf3"):
            out = index_granules([*good, str(bad)], str(store), workers=4)
        assert out == {**{g: f"G{i}.parquet" for i, g in enumerate(good)}, str(bad): None}
        assert sorted(p.name for p in store.iterdir()) == ["G0.parquet", "G1.parquet", "G2.parquet"]
        assert "junk.nc failed: not a netCDF3 file" in caplog.text
        serial = index_granules(good, str(tmp_path / "serial"), workers=1)
        assert list(serial.values()) == ["G0.parquet", "G1.parquet", "G2.parquet"]
