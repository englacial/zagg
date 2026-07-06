"""Tests for the a-priori chunk-boundary read plan (issue #148, arm 2a).

The granule stub emulates the h5coro surface for BOTH the extractor (``list``
+ hyperslice reads) and the read path (str/dict ``readDatasets``) — and
enforces h5coro's chunk-aligned-start B-tree bug (a hyperslice starting at
``k * chunk_size``, k > 0, fails wholesale), so the bit-identity test proves
the boundary read_fn's workaround, not just the plan. The headline test runs
the full local pipeline the phase-3 plan describes: extract boundaries from
the granule → parquet → per-beam polyline → chunk-aligned ReadPlan → read, and
asserts the output is bit-identical to the production planned and full read
paths on the same granule (real granules aren't in the tree; the stub mirrors
the NEON test-shard layout the same way the phase-2 extraction tests do).
"""

import numpy as np
import pandas as pd
import pytest

from zagg.catalog.extract import extract_granule_boundaries, write_boundaries_parquet
from zagg.config import PipelineConfig
from zagg.processing import _read_group, process_shard
from zagg.processing.apriori import (
    _chunk_shard_mask,
    _load_boundaries,
    _plan_from_boundaries,
)

CHUNK = 100
N = 1000  # 10 chunks
GRANULE = "ATL03_neonish.h5"
_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}

# Photon track: lon 0, lat -5 -> 5. The shard band below sits strictly inside
# chunk 6 (photons 600-699, lat 1.006-1.997): both of that chunk's boundary
# photons are OUTSIDE the band, so only the interpolated samples can match it.
LATS = np.linspace(-5.0, 5.0, N)
BAND = (1.31, 1.90)  # in-shard photons 631..689, all within chunk 6


class _Meta:
    def __init__(self, n, chunk):
        self.dimensions = [n]
        self.chunkDimensions = [chunk]


class _Granule:
    """One-beam h5coro stand-in with the chunk-aligned-start bug enforced."""

    def __init__(self, beam="gt1l"):
        self.beam = beam
        i = np.arange(N)
        seg_beg = np.arange(0, N, 25, dtype=np.int64)  # 40 segments x 25 photons
        self.arrays = {
            f"/{beam}/heights/lat_ph": LATS,
            f"/{beam}/heights/lon_ph": np.zeros(N),
            f"/{beam}/heights/h_ph": i * 0.5,
            f"/{beam}/heights/qs": (i % 7 == 0).astype(np.int8),
            f"/{beam}/geolocation/reference_photon_lat": LATS[seg_beg],
            f"/{beam}/geolocation/reference_photon_lon": np.zeros(len(seg_beg)),
            f"/{beam}/geolocation/ph_index_beg": seg_beg,
            f"/{beam}/geolocation/segment_ph_cnt": np.full(len(seg_beg), 25, dtype=np.int64),
        }
        self.calls: list[tuple[str, tuple | None]] = []

    def list(self, path, w_attr=False):
        if path != f"/{self.beam}/heights":
            raise KeyError(path)
        names = ("lat_ph", "lon_ph", "h_ph", "qs")
        return {n: {"__metadata__": _Meta(N, CHUNK)} for n in names}, {}, {}

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for spec in datasets:
            if isinstance(spec, str):
                spec = {"dataset": spec}
            path = spec["dataset"]
            arr = self.arrays[path]
            hs = spec.get("hyperslice")
            self.calls.append((path, tuple(hs[0]) if hs else None))
            if hs is not None:
                start, end = hs[0]
                # h5coro B-tree start-edge off-by-one: chunk-aligned starts
                # (except 0) fail the whole dataset read.
                if start > 0 and start % CHUNK == 0 and path.startswith(f"/{self.beam}/heights/"):
                    out[path] = None
                    continue
                arr = arr[start:end]
            out[path] = arr
        return out


class _BandGrid:
    """Grid stub: leaf id == latitude; a point is in the shard iff its lat is
    in the band (the ``_LatBboxGrid`` pattern from the planned-read tests)."""

    def __init__(self, lo, hi, shard_key=0):
        self.lo, self.hi, self.shard = lo, hi, shard_key

    def assign(self, lats, lons):
        return np.asarray(lats, dtype=np.float64)

    def shards_of(self, leaf_ids):
        leaf_ids = np.asarray(leaf_ids)
        out = np.full(leaf_ids.shape, -1, dtype=np.int64)
        out[(leaf_ids >= self.lo) & (leaf_ids <= self.hi)] = self.shard
        return out


def _data_source(*, chunk_boundaries=None, spatial_index=False, read_plan_extra=None):
    """Multi-level ATL03-shaped data source; both plan arms share the keys so
    ``chunk_boundaries`` alone selects the arm (precedence over spatial_index)."""
    ds = {
        "coordinates": {
            "latitude": "/{group}/heights/lat_ph",
            "longitude": "/{group}/heights/lon_ph",
        },
        "variables": {"h": "/{group}/heights/h_ph"},
        "filters": [{"dataset": "/{group}/heights/qs", "op": "eq", "value": 0}],
        "groups": ["gt1l"],
    }
    if spatial_index or chunk_boundaries:
        ds["base_level"] = "photons"
        ds["levels"] = {
            "photons": {
                "path": "/{group}/heights",
                "coordinates": {"latitude": "lat_ph", "longitude": "lon_ph"},
                "link": None,
            },
            "segments": {
                "path": "/{group}/geolocation",
                "coordinates": {
                    "latitude": "reference_photon_lat",
                    "longitude": "reference_photon_lon",
                },
                "link": {
                    "to": "photons",
                    "index_beg": "/{group}/geolocation/ph_index_beg",
                    "count": "/{group}/geolocation/segment_ph_cnt",
                    "index_base": 0,
                },
            },
        }
        rp = {"pad": 1}
        if spatial_index:
            rp["spatial_index"] = "segments"
        if chunk_boundaries:
            rp["chunk_boundaries"] = chunk_boundaries
        rp.update(read_plan_extra or {})
        ds["read_plan"] = rp
    return ds


def _boundaries_df(chunk_lats, n_photons=CHUNK):
    """Boundary rows from per-chunk (start_lat, end_lat) pairs, lon 0."""
    rows = [("gt1l", i, sla, 0.0, ela, 0.0, n_photons) for i, (sla, ela) in enumerate(chunk_lats)]
    cols = ["beam", "chunk", "start_lat", "start_lon", "end_lat", "end_lon", "n_photons"]
    return pd.DataFrame(rows, columns=cols)


def _write_parquet(tmp_path, granule=GRANULE):
    df, meta = extract_granule_boundaries(_Granule(), granule, beams=("gt1l",))
    out = tmp_path / f"{granule.removesuffix('.h5')}.boundaries.parquet"
    write_boundaries_parquet(df, meta, str(out))
    return df, meta


class TestChunkShardMask:
    def test_interior_crossing_with_endpoints_outside(self):
        # The band sits strictly between the chunk's boundary photons: an
        # endpoint-only test misses it; the interpolated samples must not.
        bdf = _boundaries_df([(1.0, 2.0)])
        assert _chunk_shard_mask(bdf, _BandGrid(*BAND), 0, 64).tolist() == [True]
        assert _chunk_shard_mask(bdf, _BandGrid(3.0, 4.0), 0, 64).tolist() == [False]

    def test_antimeridian_chunk_not_swept_through_lon_zero(self):
        # A chunk crossing the antimeridian must interpolate the short way
        # around (unit-sphere lerp); naive lon interpolation would sweep the
        # samples through lon 0 and spuriously match this shard.
        bdf = pd.DataFrame(
            [("gt1l", 0, -88.0, 179.5, -88.0, -179.5, CHUNK)],
            columns=["beam", "chunk", "start_lat", "start_lon", "end_lat", "end_lon", "n_photons"],
        )

        class _LonBandGrid(_BandGrid):
            def assign(self, lats, lons):
                return np.asarray(lons, dtype=np.float64)

        assert not _chunk_shard_mask(bdf, _LonBandGrid(-1.0, 1.0), 0, 64)[0]


class TestPlanFromBoundaries:
    def _bdf(self):
        # 10 chunks tiling lat -5..5 like the granule stub.
        edges = [(LATS[c * CHUNK], LATS[min(c * CHUNK + CHUNK, N) - 1]) for c in range(10)]
        return _boundaries_df(edges)

    def test_chunk_aligned_slices_with_pad(self):
        plan, n_base = _plan_from_boundaries(self._bdf(), _BandGrid(*BAND), 0, pad=1)
        assert n_base == N
        assert plan.parent_runs == [(5, 7)]  # chunk 6 matched, padded one chunk each side
        assert plan.base_slices == [(500, 800)]  # chunk-aligned by construction
        assert not plan.full_read

    def test_pad_zero_is_single_chunk(self):
        plan, _ = _plan_from_boundaries(self._bdf(), _BandGrid(*BAND), 0, pad=0)
        assert plan.base_slices == [(600, 700)]

    def test_adjacent_chunks_merge_into_one_run(self):
        plan, _ = _plan_from_boundaries(self._bdf(), _BandGrid(1.31, 2.5), 0, pad=0)
        assert plan.base_slices == [(600, 800)]  # chunks 6 and 7, one merged slice

    def test_partial_final_chunk_uses_true_counts(self):
        bdf = self._bdf()
        bdf.loc[9, "n_photons"] = 50  # truncated final chunk
        plan, n_base = _plan_from_boundaries(bdf, _BandGrid(4.0, 6.0), 0, pad=0)
        assert n_base == 950
        assert plan.base_slices[-1][1] == 950  # end clamps to the true extent

    def test_noncontiguous_chunk_range_raises(self):
        bdf = self._bdf().drop(index=3)
        with pytest.raises(ValueError, match="contiguous chunk range"):
            _plan_from_boundaries(bdf, _BandGrid(*BAND), 0)


class TestAprioriReadGroup:
    """The a-priori read against the local (stubbed-NEON) shard."""

    def _read(self, ds, granule_url=None, grid=None):
        return _read_group(
            _Granule(), "gt1l", ds, 0, grid or _BandGrid(*BAND), granule_url=granule_url
        )

    def test_bit_identical_to_planned_and_full(self, tmp_path):
        _write_parquet(tmp_path)
        url = f"/data/{GRANULE}"
        grid = _BandGrid(*BAND)

        g_apriori = _Granule()
        df_apriori = _read_group(
            g_apriori,
            "gt1l",
            _data_source(chunk_boundaries={"prefix": str(tmp_path)}, spatial_index=True),
            0,
            grid,
            granule_url=url,
        )
        df_planned = _read_group(_Granule(), "gt1l", _data_source(spatial_index=True), 0, grid)
        df_full = _read_group(_Granule(), "gt1l", _data_source(), 0, grid)

        # Photons 631..689 are in the band; qs drops the i % 7 == 0 rows.
        expected = [i * 0.5 for i in range(631, 690) if i % 7 != 0]
        assert df_apriori["h"].tolist() == expected
        pd.testing.assert_frame_equal(df_apriori, df_planned, check_exact=True)
        pd.testing.assert_frame_equal(df_apriori, df_full, check_exact=True)

        # The a-priori arm must plan without the geolocation read (that is the
        # point of arm 2a) and bound heights IO to the padded chunks 5..7 --
        # every hyperslice start is the chunk-aligned 500 shifted one element
        # early (h5coro start-edge bug workaround; the stub enforces the bug,
        # so correct output proves the workaround).
        assert not any("/geolocation/" in path for path, _ in g_apriori.calls)
        slices = [hs for _, hs in g_apriori.calls if hs is not None]
        assert slices and all(hs == (499, 800) for hs in slices)

    def test_full_read_fallback_matches_full_path(self, tmp_path):
        _write_parquet(tmp_path)
        ds = _data_source(
            chunk_boundaries={"prefix": str(tmp_path)},
            read_plan_extra={"full_read_threshold": 0.1},
        )
        df = self._read(ds, granule_url=f"/data/{GRANULE}")
        df_full = self._read(_data_source())
        pd.testing.assert_frame_equal(df, df_full, check_exact=True)

    def test_no_matching_chunk_returns_none(self, tmp_path):
        _write_parquet(tmp_path)
        ds = _data_source(chunk_boundaries={"prefix": str(tmp_path)})
        assert self._read(ds, granule_url=f"/data/{GRANULE}", grid=_BandGrid(80.0, 81.0)) is None

    def test_beam_missing_from_parquet_returns_none(self, tmp_path):
        _write_parquet(tmp_path)
        ds = _data_source(chunk_boundaries={"prefix": str(tmp_path)})
        result = _read_group(
            _Granule(), "gt2r", ds, 0, _BandGrid(*BAND), granule_url=f"/data/{GRANULE}"
        )
        assert result is None

    def test_missing_parquet_raises(self, tmp_path):
        ds = _data_source(chunk_boundaries={"prefix": str(tmp_path)})
        with pytest.raises(FileNotFoundError, match="boundary parquet"):
            self._read(ds, granule_url="/data/ATL03_other.h5")

    def test_missing_granule_url_raises(self, tmp_path):
        ds = _data_source(chunk_boundaries={"prefix": str(tmp_path)})
        with pytest.raises(ValueError, match="granule URL"):
            self._read(ds)

    def test_missing_prefix_raises(self):
        ds = _data_source(chunk_boundaries={"samples_per_chunk": 8})
        with pytest.raises(ValueError, match="prefix"):
            self._read(ds, granule_url=f"/data/{GRANULE}")

    @pytest.mark.parametrize("empty", [{}, None])
    def test_empty_block_fails_loudly_not_falls_back(self, empty):
        # The dispatch gate is a PRESENCE check: a present-but-empty (or null)
        # chunk_boundaries block must raise inside the a-priori path, never
        # silently run another arm and corrupt the benchmark comparison. The
        # config also carries spatial_index, so a truthiness gate would have
        # quietly taken the planned path instead.
        ds = _data_source(chunk_boundaries={"prefix": "unused"}, spatial_index=True)
        ds["read_plan"]["chunk_boundaries"] = empty
        with pytest.raises(ValueError, match="prefix"):
            self._read(ds, granule_url=f"/data/{GRANULE}")

    def test_samples_per_chunk_below_two_rejected(self, tmp_path):
        # < 2 samples cannot include both boundary photons (the documented
        # contract); np.linspace(0, 1, 1) would silently test only the start.
        _write_parquet(tmp_path)
        ds = _data_source(chunk_boundaries={"prefix": str(tmp_path), "samples_per_chunk": 1})
        with pytest.raises(ValueError, match="samples_per_chunk"):
            self._read(ds, granule_url=f"/data/{GRANULE}")


class TestLoadBoundaries:
    def test_s3_prefix_stages_through_boto3(self, tmp_path, monkeypatch):
        import boto3

        df, meta = _write_parquet(tmp_path)
        local = tmp_path / f"{GRANULE.removesuffix('.h5')}.boundaries.parquet"
        seen = {}

        class _FakeS3:
            def download_file(self, bucket, key, filename):
                import shutil

                seen.update(bucket=bucket, key=key)
                shutil.copyfile(local, filename)

        monkeypatch.setattr(boto3, "client", lambda service: _FakeS3())
        got_df, got_meta = _load_boundaries("s3://bkt/boundaries/", f"s3://data/{GRANULE}")
        assert seen == {
            "bucket": "bkt",
            "key": f"boundaries/{GRANULE.removesuffix('.h5')}.boundaries.parquet",
        }
        assert got_meta == meta
        pd.testing.assert_frame_equal(got_df, df)


class TestWorkerSeam:
    """The worker passes ``granule_url`` to ``_read_group`` only when the
    feature is on, so monkeypatched fakes (and the flag-off production call)
    keep their existing signature."""

    def _run(self, monkeypatch, data_source, fake_read_group):
        cfg = PipelineConfig(
            data_source=data_source,
            aggregation={"variables": {"n": {"function": "len", "source": "h"}}},
        )
        monkeypatch.setattr("zagg.processing._read_group", fake_read_group)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        return process_shard(object(), 0, ["s3://b/g0.h5"], s3_credentials=_CREDS, config=cfg)

    def test_granule_url_passed_when_enabled(self, monkeypatch):
        captured = {}

        def fake(h5obj, g, ds, sk, grid, arrow=False, granule_url=None):
            captured["granule_url"] = granule_url
            return None

        ds = _data_source(chunk_boundaries={"prefix": "/nowhere"})
        ds["reader"] = "h5coro"
        _, meta = self._run(monkeypatch, ds, fake)
        assert captured["granule_url"] == "s3://b/g0.h5"
        assert meta["error"] == "No data after filtering"

    def test_flag_off_call_signature_unchanged(self, monkeypatch):
        # A fake WITHOUT granule_url in its signature must keep working when
        # the feature is off (it would TypeError if the kwarg were passed).
        def fake(h5obj, g, ds, sk, grid, arrow=False):
            return None

        ds = _data_source()
        ds["reader"] = "h5coro"
        # The seam under test is the hierarchical backend's ``_read_group``
        # delegation; the inline default (issue #170) never calls it, so pin
        # the backend (the sibling test keeps hierarchical via its
        # ``chunk_boundaries`` carve-out in ``index_from_config``).
        ds["index"] = {"backend": "hierarchical"}
        _, meta = self._run(monkeypatch, ds, fake)
        assert meta["error"] == "No data after filtering"
