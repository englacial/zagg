"""netCDF3 end to end (issue #682, phase 4): a classic file aggregates through
``reader: h5coro`` + ``index: {backend: sidecar, on_miss: error}`` with no
HDF5 metadata access -- the worker opens it as a manifest-only ranged handle
(``zagg.processing.granule.open_granule``) after one 4-byte magic peek, and the
sidecar backend fetches exactly the manifest's byte ranges.

The fixture is a point-cloud file written from the spec by
``test_netcdf3._nc3_bytes``: ``lat`` / ``lon`` are NON-record variables over
a fixed ``obs`` dimension and ``h`` / ``q`` are RECORD variables over the
unlimited ``time`` dimension (same length), so one aggregate exercises both
addressing forms, with ``q`` making the record block genuinely interleaved.
"""

from pathlib import Path

import numpy as np
import pytest
from h5coro import filedriver
from test_netcdf3 import NC_DOUBLE, NC_FLOAT, NC_SHORT, _nc3_bytes

import zagg.processing.granule as granule_mod
from zagg.catalog.netcdf3 import build_manifest, index_granule
from zagg.config import PipelineConfig
from zagg.processing import process_shard
from zagg.processing.granule import RangedGranule, open_granule

FIXTURE_H5 = Path(__file__).parent / "data" / "index" / "atl03_mini.h5"
#: h5coro's default cache line: the superblock read of an H5Coro open.
_CACHE_LINE = 4 * 1024 * 1024

_LAT = np.array([0.0, 0.1, 1.0, 0.9, 2.0, 2.1])
_LON = np.array([10.0, 11.0, 12.0, 13.0, 14.0, 15.0])
_H = np.array([5.0, 7.0, 1.0, 3.0, 100.0, 200.0], dtype=">f4")
_Q = np.array([1, 2, 3, 4, 5, 6], dtype=">i2")


def _point_cloud(version=1):
    dims = [("time", 0), ("obs", 6)]
    variables = [
        {"name": "lat", "dimids": [1], "nc_type": NC_DOUBLE, "data": _LAT},
        {"name": "lon", "dimids": [1], "nc_type": NC_DOUBLE, "data": _LON},
        {"name": "h", "dimids": [0], "nc_type": NC_FLOAT, "data": _H},
        {"name": "q", "dimids": [0], "nc_type": NC_SHORT, "data": _Q},
    ]
    return _nc3_bytes(version, dims, variables)


class _Grid:
    """leaf id == round(lat); shard 1 == leaves {0, 1} (4 of the 6 points)."""

    def assign(self, lats, lons):
        return np.round(np.asarray(lats)).astype(np.int64)

    def shards_of(self, leaf_ids):
        return np.isin(leaf_ids, [0, 1]).astype(np.int64)

    def children(self, shard_key):
        return np.array([0], dtype=np.int64)

    def cells_of(self, leaf_ids):
        return np.zeros(len(leaf_ids), dtype=np.int64)

    def chunk_coords(self, shard_key):
        return {"cell_lat": np.zeros(1), "cell_lon": np.zeros(1)}


def _config(index):
    return PipelineConfig(
        data_source={
            "reader": "h5coro",
            "groups": ["root"],
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h": "/h"},
            "index": index,
        },
        aggregation={
            "variables": {
                "count": {"function": "len", "source": "h", "dtype": "int32", "fill_value": 0},
                "h_min": {"function": "min", "source": "h", "dtype": "float32"},
                "h_mean": {"function": "mean", "source": "h", "dtype": "float32"},
            }
        },
        output={"store": "unused"},
    )


class _RecordingDriver(filedriver.FileDriver):
    reads: list = []

    def read(self, pos, size):
        type(self).reads.append((int(pos), int(size)))
        return super().read(pos, size)


@pytest.fixture
def granule(tmp_path):
    data, _ = _point_cloud(2)
    path = tmp_path / "PC_v2_0001.nc"
    path.write_bytes(data)
    store = tmp_path / "sidecar"
    assert index_granule(str(path), str(store)) == "PC_v2_0001.parquet"
    return path, store, data


class TestEndToEnd:
    def test_sidecar_aggregates_netcdf3_with_no_hdf5_metadata_access(self, granule, monkeypatch):
        path, store, data = granule
        _RecordingDriver.reads = []
        handles = []
        orig = granule_mod.RangedGranule

        class Spy(orig):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                handles.append(self)

        monkeypatch.setattr(granule_mod, "RangedGranule", Spy)

        df, meta = process_shard(
            _Grid(),
            1,
            [str(path)],
            s3_credentials={},
            h5coro_driver=_RecordingDriver,
            config=_config({"backend": "sidecar", "store": str(store), "on_miss": "error"}),
        )
        assert meta["error"] is None
        assert meta["files_processed"] == 1 and meta.get("read_errors", 0) == 0
        assert meta["total_obs"] == 4
        row = df.iloc[0]
        assert row["count"] == 4
        assert row["h_min"] == np.float32(1.0)
        assert row["h_mean"] == np.float32((5.0 + 7.0 + 1.0 + 3.0) / 4)

        # The granule was opened as the manifest-only handle, not H5Coro.
        assert len(handles) == 1
        # Reads: the .nc name's magic peek (0, 4) -- no h5coro superblock
        # attempt, no cache-line read -- then ONLY manifest byte ranges:
        # nothing in the header region after the peek, no HDF5 metadata walk.
        reads = _RecordingDriver.reads
        assert reads[0] == (0, 4)
        assert all(size < _CACHE_LINE for _, size in reads)
        manifest = build_manifest(data)
        lo = int(manifest["byte_offset"].min())
        hi = int((manifest["byte_offset"] + manifest["nbytes"]).max())
        for pos, size in reads[1:]:
            assert lo <= pos and pos + size <= hi, (pos, size)
        # Both addressing forms were exercised: the non-record coordinates
        # (one contiguous read each) and the record variable h (per-record
        # slabs, coalesced by the backend into spans inside the record block).
        rec_begin = int(manifest[manifest["dataset"] == "/h"]["byte_offset"].min())
        assert any(pos < rec_begin for pos, _ in reads[1:])
        assert any(pos >= rec_begin for pos, _ in reads[1:])

    def test_classic_and_64bit_offset_agree(self, tmp_path):
        outs = []
        for version in (1, 2):
            data, _ = _point_cloud(version)
            path = tmp_path / f"PC_v{version}.nc"
            path.write_bytes(data)
            store = tmp_path / f"store{version}"
            index_granule(str(path), str(store))
            df, meta = process_shard(
                _Grid(),
                1,
                [str(path)],
                s3_credentials={},
                h5coro_driver=filedriver.FileDriver,
                config=_config({"backend": "sidecar", "store": str(store), "on_miss": "error"}),
            )
            assert meta["error"] is None and meta["total_obs"] == 4
            outs.append(df)
        for col in outs[0].columns:
            assert outs[0][col].to_numpy().tobytes() == outs[1][col].to_numpy().tobytes()

    def test_on_miss_error_without_a_manifest_is_a_read_error(self, granule, tmp_path):
        path, _, _ = granule
        empty = tmp_path / "empty-store"
        df, meta = process_shard(
            _Grid(),
            1,
            [str(path)],
            s3_credentials={},
            h5coro_driver=filedriver.FileDriver,
            config=_config({"backend": "sidecar", "store": str(empty), "on_miss": "error"}),
        )
        assert meta["total_obs"] == 0 and meta["read_errors"] == 1

    # h5coro's H5Dataset.__del__ trips on a half-constructed instance when the
    # inline backend's chunk-map walk is handed the ranged handle -- an
    # upstream quirk of the refused path, not a leak.
    @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
    def test_metadata_backend_on_netcdf3_fails_loudly(self, granule, caplog):
        # Without a manifest route (the package default ``inline`` backend,
        # which builds chunk maps from HDF5 metadata), the ranged handle
        # refuses the metadata-driven read per group -- the shard reports the
        # error instead of decoding garbage.
        path, _, _ = granule
        with caplog.at_level("WARNING"):
            df, meta = process_shard(
                _Grid(),
                1,
                [str(path)],
                s3_credentials={},
                h5coro_driver=filedriver.FileDriver,
                config=_config(None),
            )
        assert meta["total_obs"] == 0 and meta["read_errors"] == 1
        assert "not an HDF5 file" in caplog.text

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
    @pytest.mark.parametrize("on_miss", ["build", "fallback"])
    def test_sidecar_miss_paths_name_the_real_cause(self, granule, tmp_path, caplog, on_miss):
        # A granule the store has no manifest for, under the metadata-reading
        # miss policies: ``build`` walks h5coro internals (``rootAddress``),
        # ``fallback`` calls ``readDatasets``; both get the same refusal, which
        # names the missing manifest rather than the backend choice.
        path, _, _ = granule
        empty = tmp_path / "empty-store"
        with caplog.at_level("WARNING"):
            df, meta = process_shard(
                _Grid(),
                1,
                [str(path)],
                s3_credentials={},
                h5coro_driver=filedriver.FileDriver,
                config=_config({"backend": "sidecar", "store": str(empty), "on_miss": on_miss}),
            )
        assert meta["total_obs"] == 0 and meta["read_errors"] == 1
        assert "not an HDF5 file" in caplog.text
        assert "sidecar store has no manifest for this granule" in caplog.text
        assert "AttributeError" not in caplog.text


class TestOpenGranule:
    def test_hdf5_opens_as_h5coro(self):
        import h5coro

        h5obj = open_granule(str(FIXTURE_H5), filedriver.FileDriver, {})
        try:
            assert isinstance(h5obj, h5coro.H5Coro)
        finally:
            h5obj.close()

    def test_netcdf3_opens_as_ranged_handle(self, granule):
        path, _, data = granule
        h5obj = open_granule(str(path), filedriver.FileDriver, {})
        try:
            assert isinstance(h5obj, RangedGranule)
            assert h5obj.resource == str(path)
            assert bytes(h5obj.ioRequest(0, 4, caching=False)) == b"CDF\x02"
            assert bytes(h5obj.ioRequest(10, 6)) == data[10:16]
            with pytest.raises(TypeError, match="not an HDF5 file"):
                h5obj.readDatasets(["/lat"])
            with pytest.raises(TypeError, match=r"not an HDF5 file \(rootAddress\)"):
                _ = h5obj.rootAddress
            # getattr/hasattr probes keep their meaning (the refusal is also
            # an AttributeError); dunder lookups are plain AttributeErrors.
            assert getattr(h5obj, "cache_locks", None) is None
            assert not hasattr(h5obj, "__deepcopy__")
        finally:
            h5obj.close()
        with pytest.raises(ValueError, match="closed"):
            h5obj.ioRequest(0, 4)

    def test_nc_name_peeks_once_and_skips_h5coro(self, granule):
        path, _, _ = granule
        _RecordingDriver.reads = []
        h5obj = open_granule(str(path), _RecordingDriver, {})
        try:
            assert isinstance(h5obj, RangedGranule)
            assert _RecordingDriver.reads == [(0, 4)]
        finally:
            h5obj.close()

    def test_hdf5_under_an_nc_name_still_opens_as_h5coro(self, tmp_path):
        # netCDF4 (and CMR-mislabelled HDF5, e.g. TES) carries .nc: the peek
        # costs one 4-byte read, then h5coro opens it as usual.
        import h5coro

        nc4 = tmp_path / "netcdf4.NC"
        nc4.write_bytes(FIXTURE_H5.read_bytes())
        _RecordingDriver.reads = []
        h5obj = open_granule(str(nc4), _RecordingDriver, {})
        try:
            assert isinstance(h5obj, h5coro.H5Coro)
            assert _RecordingDriver.reads[:2] == [(0, 4), (0, _CACHE_LINE)]
        finally:
            h5obj.close()

    def test_netcdf3_under_another_name_pays_the_rejected_h5coro_read(self, granule, tmp_path):
        # The magic, not the name, decides: a netCDF3 file named .bin is still
        # recognized, after h5coro's rejected superblock read of one full
        # cache line. Pinned so a change in h5coro's cache line is visible.
        _, _, data = granule
        other = tmp_path / "PC_v2_0001.bin"
        other.write_bytes(data)
        _RecordingDriver.reads = []
        h5obj = open_granule(str(other), _RecordingDriver, {})
        try:
            assert isinstance(h5obj, RangedGranule)
            assert _RecordingDriver.reads == [(0, _CACHE_LINE), (0, 4)]
        finally:
            h5obj.close()

    @pytest.mark.parametrize("name", ["G.nc", "G.bin"])
    @pytest.mark.parametrize(
        "result, match",
        [
            (None, r"ranged read failed at 0\+4 \(driver returned None\)"),
            (b"CD", r"ranged read failed at 0\+4 \(driver returned 2 bytes\)"),
            (ConnectionError("timed out"), r"ranged read failed at 0\+4 \(ConnectionError"),
        ],
    )
    def test_failed_magic_peek_is_an_io_error(self, granule, tmp_path, name, result, match):
        # A timed-out / swallowed peek is I/O, never "invalid file signature";
        # on both routes (the .nc pre-peek and the post-h5coro peek), and the
        # peek's driver is closed.
        _, _, data = granule
        path = tmp_path / name
        path.write_bytes(data)
        closed = []

        class Flaky(filedriver.FileDriver):
            def read(self, pos, size):
                if (pos, size) != (0, 4):
                    return super().read(pos, size)
                if isinstance(result, Exception):
                    raise result
                return result

            def close(self):
                closed.append(self)
                super().close()

        with pytest.raises(OSError, match=match) as info:
            open_granule(str(path), Flaky, {})
        assert "invalid file signature" not in str(info.value)
        if isinstance(result, Exception):
            assert info.value.__cause__ is result
        assert closed

    def test_other_files_reraise_h5coro_error(self, tmp_path):
        junk = tmp_path / "junk.bin"
        junk.write_bytes(b"\0" * 64)
        with pytest.raises(RuntimeError, match="invalid file signature"):
            open_granule(str(junk), filedriver.FileDriver, {})

    def test_monkeypatched_h5coro_still_intercepts(self, monkeypatch):
        sentinel = object()
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: sentinel)
        assert open_granule("anything", filedriver.FileDriver, {}) is sentinel


class TestShippedTemplate:
    def test_cryosat2_template_pins_the_sidecar_route(self):
        from zagg.config import default_config, validate_config

        cfg = default_config("cryosat2_rdwes1b_healpix")
        validate_config(cfg)
        ds = cfg.data_source
        assert ds["reader"] == "h5coro"
        # netCDF3 has no HDF5 metadata: only the manifest-addressed backend can
        # read it, and a miss must be an error, never a metadata fallback.
        assert ds["index"]["backend"] == "sidecar" and ds["index"]["on_miss"] == "error"
        # Paths are /<variable> -- the RDWES1B header's names (describe() on
        # 2026-10-31: lat, lon, elev, roughness, peakiness, all non-record).
        assert ds["coordinates"] == {"latitude": "/lat", "longitude": "/lon"}
        assert set(ds["variables"].values()) <= {"/elev", "/roughness", "/peakiness"}
