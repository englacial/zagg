"""Tests for chunk-boundary geometry extraction (issue #148, phase 2).

The stub h5obj emulates the h5coro surface the extractor uses (``list`` +
hyperslice ``readDatasets``) — including h5coro's chunk-aligned-start B-tree
bug: a hyperslice starting exactly at ``k * chunk_size`` (k > 0) spuriously
intersects the preceding chunk and the whole dataset read fails (returns
``None`` under ``errorChecking``). The stub *enforces* that failure mode so a
regression in the extractor's start-one-early workaround fails loudly here
instead of on real granules.
"""

import numpy as np
import pandas as pd
import pytest

from zagg.catalog.extract import (
    ATL03_BEAMS,
    EXTRACT_META_KEY,
    extract_beam_boundaries,
    extract_granule_boundaries,
    heights_chunk_grid,
    read_boundaries_parquet,
    run_extraction,
    write_boundaries_parquet,
)

CHUNK = 100


class _Meta:
    def __init__(self, n, chunk, width=None):
        self.dimensions = [n] if width is None else [n, width]
        self.chunkDimensions = [chunk] if width is None else [chunk, width]


class _StubH5:
    """h5coro stand-in: per-beam lat/lon arrays, chunked ``CHUNK`` per dataset."""

    def __init__(self, beams: dict, chunk=CHUNK, chunk_overrides: dict | None = None):
        self.beams = beams
        self.chunk = chunk
        self.chunk_overrides = chunk_overrides or {}

    def list(self, path, w_attr=False):
        beam = path.strip("/").split("/")[0]
        if beam not in self.beams:
            return {}, {}, {}
        n = len(self.beams[beam]["lat_ph"])
        variables = {}
        for name in ("lat_ph", "lon_ph", "h_ph"):
            chunk = self.chunk_overrides.get(name, self.chunk)
            variables[name] = {"__metadata__": _Meta(n, chunk)}
        variables["signal_conf_ph"] = {"__metadata__": _Meta(n, self.chunk, width=5)}
        return variables, {}, {}

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for spec in datasets:
            path = spec["dataset"]
            _, beam, _, name = path.split("/")
            arr = self.beams[beam][name]
            (start, end) = spec["hyperslice"][0]
            # h5coro chunk-aligned-start bug: reads beginning exactly on a
            # chunk boundary (except 0) fail wholesale.
            if start > 0 and start % self.chunk == 0:
                out[path] = None
                continue
            out[path] = arr[start:end]
        return out


def _beam(n, seed=0):
    rng = np.random.default_rng(seed)
    return {
        "lat_ph": rng.uniform(-88, -87, n),
        "lon_ph": rng.uniform(-180, 180, n),
    }


def _expected(arrs, beam, chunk=CHUNK):
    lat, lon = arrs["lat_ph"], arrs["lon_ph"]
    rows = []
    for i, c0 in enumerate(range(0, len(lat), chunk)):
        c1 = min(c0 + chunk, len(lat))
        rows.append((beam, i, lat[c0], lon[c0], lat[c1 - 1], lon[c1 - 1], c1 - c0))
    return rows


def test_beam_boundaries_partial_last_chunk():
    arrs = _beam(int(2.5 * CHUNK))
    df = extract_beam_boundaries(_StubH5({"gt1l": arrs}), "gt1l")
    exp = _expected(arrs, "gt1l")
    assert len(df) == 3
    assert df["n_photons"].tolist() == [CHUNK, CHUNK, CHUNK // 2]
    for row, (beam, i, sla, slo, ela, elo, n) in zip(df.itertuples(index=False), exp):
        assert (row.beam, row.chunk, row.n_photons) == (beam, i, n)
        assert (row.start_lat, row.start_lon) == (sla, slo)
        assert (row.end_lat, row.end_lon) == (ela, elo)


def test_beam_boundaries_block_smaller_than_beam():
    # Multiple block reads (block_chunks=2 over 5.5 chunks) match a single one,
    # and every non-initial block starts one element early (the stub fails any
    # chunk-aligned start, so passing at all proves the workaround).
    arrs = _beam(int(5.5 * CHUNK), seed=1)
    stub = _StubH5({"gt1l": arrs})
    df_blocked = extract_beam_boundaries(stub, "gt1l", block_chunks=2)
    df_single = extract_beam_boundaries(stub, "gt1l", block_chunks=100)
    pd.testing.assert_frame_equal(df_blocked, df_single)
    assert len(df_blocked) == 6


def test_beam_boundaries_empty_beam():
    df = extract_beam_boundaries(_StubH5({"gt1l": _beam(0)}), "gt1l")
    assert len(df) == 0
    assert list(df.columns) == [
        "beam",
        "chunk",
        "start_lat",
        "start_lon",
        "end_lat",
        "end_lon",
        "n_photons",
    ]


def test_misaligned_chunk_grid_raises():
    stub = _StubH5({"gt1l": _beam(3 * CHUNK)}, chunk_overrides={"h_ph": CHUNK // 2})
    with pytest.raises(ValueError, match="not aligned"):
        heights_chunk_grid(stub, "gt1l")


def test_granule_boundaries_meta_and_missing_beams():
    beams = {"gt1l": _beam(2 * CHUNK), "gt2r": _beam(3 * CHUNK, seed=2)}
    df, meta = extract_granule_boundaries(_StubH5(beams), "ATL03_x.h5")
    assert meta["granule"] == "ATL03_x.h5"
    assert meta["chunk_size"] == CHUNK
    assert meta["n_chunks"] == len(df) == 5
    assert meta["beams"] == ["gt1l", "gt2r"]
    assert set(meta["missing_beams"]) == set(ATL03_BEAMS) - {"gt1l", "gt2r"}
    assert meta["wall_s"] >= 0
    assert df.groupby("beam").size().to_dict() == {"gt1l": 2, "gt2r": 3}


def test_parquet_round_trip(tmp_path):
    df, meta = extract_granule_boundaries(_StubH5({"gt1l": _beam(2 * CHUNK)}), "g.h5")
    path = tmp_path / "g.boundaries.parquet"
    write_boundaries_parquet(df, meta, str(path))
    df2, meta2 = read_boundaries_parquet(str(path))
    assert meta2 == meta
    pd.testing.assert_frame_equal(df2, df)


def test_parquet_meta_key_present(tmp_path):
    from fastparquet import ParquetFile

    df, meta = extract_granule_boundaries(_StubH5({"gt1l": _beam(CHUNK)}), "g.h5")
    path = tmp_path / "g.parquet"
    write_boundaries_parquet(df, meta, str(path))
    assert EXTRACT_META_KEY in (ParquetFile(str(path)).key_value_metadata or {})


def test_run_extraction_local_prefix(tmp_path, monkeypatch):
    # Wire the stub through run_extraction: file driver, local output prefix.
    import zagg.catalog.extract as ext

    beams = {"gt1l": _beam(2 * CHUNK)}
    monkeypatch.setattr(
        ext, "_resolve_h5coro_driver", lambda driver: lambda *a, **k: None, raising=True
    )

    class _FakeH5Coro:
        def __init__(self, resource, driver, credentials=None, errorChecking=True):  # noqa: N803
            self._stub = _StubH5(beams)

        def list(self, *a, **k):
            return self._stub.list(*a, **k)

        def readDatasets(self, *a, **k):  # noqa: N802 (mirror real h5coro API)
            return self._stub.readDatasets(*a, **k)

        def close(self):
            pass

    import h5coro

    monkeypatch.setattr(h5coro, "H5Coro", _FakeH5Coro)
    results = run_extraction(
        ["/data/ATL03_a.h5", "/data/ATL03_b.h5"],
        str(tmp_path / "out"),
        driver="file",
        scratch_dir=str(tmp_path),
    )
    assert [r["ok"] for r in results] == [True, True]
    for r, gid in zip(results, ("ATL03_a", "ATL03_b")):
        assert r["granule"] == f"{gid}.h5"
        assert r["n_chunks"] == 2
        df, meta = read_boundaries_parquet(r["output"])
        assert meta["granule"] == f"{gid}.h5"
        assert len(df) == 2


def test_run_extraction_isolates_failures(tmp_path, monkeypatch):
    import h5coro

    def _boom(*a, **k):
        raise OSError("no such granule")

    monkeypatch.setattr(h5coro, "H5Coro", _boom)
    results = run_extraction(
        ["/data/ATL03_a.h5"], str(tmp_path / "out"), driver="file", scratch_dir=str(tmp_path)
    )
    assert results == [{"granule": "ATL03_a.h5", "ok": False, "error": "no such granule"}]


def test_empty_beam_schema_matches_populated(tmp_path):
    # An empty (or missing-beam) granule must not drift the parquet schema:
    # int columns stay int64 and beam stays a string column, so the phase-3
    # consumer reads one schema regardless of beam population.
    from fastparquet import ParquetFile

    populated, _ = extract_granule_boundaries(_StubH5({"gt1l": _beam(CHUNK)}), "a.h5")
    empty, _ = extract_granule_boundaries(_StubH5({"gt1l": _beam(0)}), "b.h5")
    assert list(empty.dtypes.astype(str)) == list(populated.dtypes.astype(str))
    for i, df in enumerate((populated, empty)):
        path = tmp_path / f"g{i}.parquet"
        write_boundaries_parquet(df, {"granule": "g"}, str(path))
    schema_a = {
        c.name: c.type for c in ParquetFile(str(tmp_path / "g0.parquet")).schema.schema_elements
    }
    schema_b = {
        c.name: c.type for c in ParquetFile(str(tmp_path / "g1.parquet")).schema.schema_elements
    }
    assert schema_a == schema_b


def test_run_extraction_cleans_scratch_on_upload_failure(tmp_path, monkeypatch):
    # A failed delivery must not leak the scratch parquet (Lambda /tmp is
    # 512 MB and warm containers revisit it across the fan-out).
    import zagg.catalog.extract as ext

    beams = {"gt1l": _beam(CHUNK)}
    monkeypatch.setattr(ext, "_resolve_h5coro_driver", lambda driver: object)

    class _FakeH5Coro:
        def __init__(self, *a, **k):
            self._stub = _StubH5(beams)

        def list(self, *a, **k):
            return self._stub.list(*a, **k)

        def readDatasets(self, *a, **k):  # noqa: N802 (mirror real h5coro API)
            return self._stub.readDatasets(*a, **k)

    import h5coro

    monkeypatch.setattr(h5coro, "H5Coro", _FakeH5Coro)

    def broken_put(local, prefix, name):
        raise OSError("upload failed")

    monkeypatch.setattr(ext, "_put_parquet", broken_put)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    results = run_extraction(
        ["/data/ATL03_a.h5"], str(tmp_path / "out"), driver="file", scratch_dir=str(scratch)
    )
    assert results[0]["ok"] is False and "upload failed" in results[0]["error"]
    assert list(scratch.iterdir()) == []  # no leaked parquet
