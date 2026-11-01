"""The packaged GLAH01 template over a synthetic GLAH01-shaped mini-granule (issue #678, phase 3).

Two canned-array stubs (the ``test_read_vlen`` convention, h5py-free): a
GLAH01 primary with ``r_rng_wf [n_shots, 544]``, ``i_waveformType``, per-shot
noise/gain/energy, a ``[n, 6]`` record-level table, 1 Hz-only predicted
lat/lon and the composite shot key, plus a GLAH05-shaped sibling carrying the
40 Hz lat/lon keyed on (``i_rec_ndx``, ``i_shot_count``). The template's whole
declared surface is driven through the public ``_read_group`` dispatch, the
cell statistics, and the real ``process_shard`` → ``process_and_write_hive``
leaf write, and the planned read is pinned to the shard's rows only.
"""

import numpy as np
import pytest

from zagg.config import default_config, validate_config
from zagg.processing.read import _read_group
from zagg.semantics import semantic_hash

# ── GLAH01-shaped mini-granule ───────────────────────────────────────────────
#
# Six shots over two 40 Hz records; the GLAS width is the product's 544.
#
#   shot  (rec, cnt)  type  lat (GLAH05)  sibling row
#   0     (1, 1)      1     -78.50        yes
#   1     (1, 2)      2     -78.50        yes   short: 200 valid gates
#   2     (1, 3)      1     -78.50        yes
#   3     (2, 1)      0     -78.50        yes   missing waveform
#   4     (2, 2)      1     -78.50        NO    no geolocation -> dropped
#   5     (2, 3)      2     -60.00        yes   another shard

WIDTH = 544
N_SHOTS = 6
G = "Data_40HZ"
REC_NDX = np.array([1, 1, 1, 2, 2, 2], dtype=np.int32)
SHOT_COUNT = np.array([1, 2, 3, 1, 2, 3], dtype=np.int8)
WF_TYPE = np.array([1, 2, 1, 0, 1, 2], dtype=np.int8)
UTC = np.array([1.0e8, 1.0e8 + 0.025, 1.0e8 + 0.05, 1.0e8 + 1.0, 1.0e8 + 1.025, 1.0e8 + 1.05])
BG_MEAN = np.array([0.05, 0.04, 0.06, 0.05, 0.05, 0.04], dtype=np.float32)
BG_SDEV = np.array([0.01, 0.01, 0.01, 0.01, 0.01, 0.01], dtype=np.float32)
GAIN = np.array([13, 13, 14, 13, 13, 12], dtype=np.int16)
ENERGY = np.array([1.5, 0.8, 1.7, 0.0, 1.2, 0.9], dtype=np.float32)
REF_RNG = np.array([600000.0, 600010.0, 600020.0, 600030.0, 600040.0, 600050.0])
GATES = np.arange(WIDTH, dtype=np.float64)
WT_FACT = np.arange(N_SHOTS * 6, dtype=np.int16).reshape(N_SHOTS, 6)
SIB_LAT = np.array([-78.5, -78.5, -78.5, -78.5, -60.0])
SIB_LON = np.array([-132.0, -132.0, -132.0, -132.0, 10.0])
#: Sibling rows: shot 4 ((2, 2)) is absent; the rest in a shuffled order.
SIB_REC = np.array([2, 1, 1, 1, 2], dtype=np.int32)
SIB_CNT = np.array([1, 3, 1, 2, 3], dtype=np.int8)


def _waveform():
    """Per-shot amplitude ramps: shot ``i`` reads ``1.0 + i`` volts in its
    valid gates and a flat 0.5 V tail beyond. The tail sits well ABOVE the
    clip threshold (``0.04 + 4.33 * 0.01 ~= 0.083`` for the short shot at the
    template's operating point) and below every valid amplitude, so only the
    ``sample_index`` padding filter can drop it -- the clip cannot."""
    wf = np.full((N_SHOTS, WIDTH), 0.5, dtype=np.float32)
    for i in range(N_SHOTS):
        valid = 200 if WF_TYPE[i] == 2 else WIDTH
        wf[i, :valid] = 1.0 + i
    return wf


WF = _waveform()


def _glah01_arrays():
    return {
        f"/{G}/Waveform/RecWaveform/r_rng_wf": WF,
        f"/{G}/Waveform/RecWaveform/d_refRng": REF_RNG,
        f"/{G}/Waveform/RecWaveform/DS_Gate_Index_544": GATES,
        f"/{G}/Waveform/Characteristics/i_waveformType": WF_TYPE,
        f"/{G}/Waveform/Characteristics/d_4nsBgMean": BG_MEAN,
        f"/{G}/Waveform/Characteristics/d_4nsBgSDEV": BG_SDEV,
        f"/{G}/Waveform/Characteristics/i_gainSet1064": GAIN,
        f"/{G}/Waveform/Characteristics/d_RecNrgAll_EU": ENERGY,
        f"/{G}/Waveform/Characteristics/i_wt_fact_filt": WT_FACT,
        f"/{G}/Time/i_rec_ndx": REC_NDX,
        f"/{G}/Time/i_shot_count": SHOT_COUNT,
        f"/{G}/Time/d_UTCTime_40": UTC,
        # 1 Hz predicted geolocation only -- never read by the template.
        "/Data_1HZ/Geolocation/d1_pred_lat": np.array([-78.4, -78.6]),
        "/Data_1HZ/Geolocation/d1_pred_lon": np.array([-132.1, -131.9]),
    }


def _glah05_arrays():
    return {
        f"/{G}/Time/i_rec_ndx": SIB_REC,
        f"/{G}/Time/i_shot_count": SIB_CNT,
        f"/{G}/Geolocation/d_lat": SIB_LAT,
        f"/{G}/Geolocation/d_lon": SIB_LON,
    }


class _Promise:
    """What ``readDatasets`` hands back on a real handle: h5coro's ``H5Promise``
    is a mapping with ``keys``/``__getitem__``/``__iter__`` and NO
    ``.values()``/``.items()`` — a ``dict`` stub hides that (phase 2 review)."""

    def __init__(self, data):
        self._data = data

    def keys(self):
        return self._data.keys()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)


class _FakeH5:
    """Stub h5coro object: hyperslices slice dim 0, trailing dims whole."""

    def __init__(self, arrays, resource=""):
        self._arrays = arrays
        self.resource = resource
        self.reads: list = []

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                self.reads.append((d, None))
                out[d] = self._arrays[d]
                continue
            path, hs = d["dataset"], d["hyperslice"]
            self.reads.append((path, hs))
            lo, hi = hs[0]
            out[path] = self._arrays[path][lo:hi]
        return _Promise(out)

    def close(self):
        pass


def _shard_key(order, lat=-78.5, lon=-132.0):
    from mortie import geo2mort

    return int(geo2mort(lat, lon, order=order)[0])


@pytest.fixture
def cfg():
    return default_config("glah01_waveform_healpix_hive")


@pytest.fixture
def grid(cfg):
    from zagg.grids import HealpixGrid

    g = cfg.output["grid"]
    return HealpixGrid(g["parent_order"], g["child_order"], layout="fullsphere", config=cfg)


class TestGlah01Template:
    def test_validates_and_declares_the_fixed_width_surface(self, cfg):
        validate_config(cfg)
        ds = cfg.data_source
        assert ds["levels"]["shots"]["link"] == {"to": "samples", "width": WIDTH}
        assert ds["variables"]["rx_wf"]["axes"] == ["record", "sample"]
        assert ds["variables"]["gate_range"]["synthesize"] == "offsets"
        assert ds["variables"]["sample_index"] == {"synthesize": "index", "level": "shots"}
        assert ds["coordinates"]["latitude"]["asset"] == "glah05"
        assert ds["assets"]["glah05"]["join"]["left"] == [
            "/{group}/Time/i_rec_ndx",
            "/{group}/Time/i_shot_count",
        ]
        assert ds["levels"]["shots"]["variables"]["wt_fact_filt_0"]["column"] == 0
        # No time_source: d_UTCTime_40 is nominal UTC (the template header).
        assert "time_source" not in cfg.output
        flux = cfg.aggregation["variables"]["rx_flux"]
        assert flux["weights"] == "flux" and flux["attrs"]["provenance"] == "proposed"
        assert flux["params"]["samples_per_record"] == WIDTH

    def test_hash_is_stable(self, cfg):
        # The GEDI/ATL03 pins live in test_live_store_templates; this template
        # is new, so pin that its hash is deterministic and distinct.
        assert semantic_hash(cfg) == semantic_hash(default_config("glah01_waveform_healpix_hive"))
        assert semantic_hash(cfg) != semantic_hash(default_config("gedi01b_waveform_healpix_hive"))

    def test_reads_the_fixture_through_the_sibling_join(self, cfg, grid):
        shard = _shard_key(cfg.output["grid"]["parent_order"])
        io_stats = {}
        df = _read_group(
            _FakeH5(_glah01_arrays()),
            G,
            cfg.data_source,
            shard,
            grid,
            io_stats=io_stats,
            siblings={"glah05": _FakeH5(_glah05_arrays())},
        )
        # Planned read: shots 0..3 are in the shard (pad 1 reaches shot 4,
        # which has no sibling row); shot 5 is another shard. Rows DECODED =
        # the padded run 0..4, 5 * 544.
        assert io_stats["obs_read"] == 5 * WIDTH
        # Kept: type-1 shots 0 and 2 at 544 gates, short shot 1 at 200; shot 3
        # is a missing waveform (type 0) and shot 4 has no position.
        assert len(df) == 2 * WIDTH + 200
        counts = df.groupby("utc_time").size()
        assert counts[UTC[0]] == WIDTH and counts[UTC[1]] == 200 and counts[UTC[2]] == WIDTH
        np.testing.assert_array_equal(
            df["rx_wf"].to_numpy(), np.concatenate([WF[0], WF[1][:200], WF[2]])
        )
        np.testing.assert_allclose(
            df["gate_range"].to_numpy(),
            np.concatenate(
                [
                    REF_RNG[0] + GATES * 0.15,
                    REF_RNG[1] + GATES[:200] * 0.15,
                    REF_RNG[2] + GATES * 0.15,
                ]
            ),
        )
        assert df["sample_index"].max() == WIDTH - 1
        assert df["wt_fact_filt_0"].tolist() == [0] * WIDTH + [6] * 200 + [12] * WIDTH
        assert df["i_waveformType"].tolist() == [1] * WIDTH + [2] * 200 + [1] * WIDTH

    def test_padding_filter_is_what_drops_the_short_tail(self, cfg, grid):
        # With the padding expression removed, the short shot's 344 tail gates
        # (0.5 V, above the clip threshold) come through: 3 full shots.
        cfg.data_source["filters"] = [
            f for f in cfg.data_source["filters"] if "expression" not in f
        ]
        df = _read_group(
            _FakeH5(_glah01_arrays()),
            G,
            cfg.data_source,
            _shard_key(9),
            grid,
            siblings={"glah05": _FakeH5(_glah05_arrays())},
        )
        assert len(df) == 3 * WIDTH
        assert (df.loc[df["utc_time"] == UTC[1], "rx_wf"].to_numpy()[200:] == 0.5).all()

    def test_planned_read_touches_only_the_shards_rows(self, cfg, grid):
        shard = _shard_key(cfg.output["grid"]["parent_order"])
        h5 = _FakeH5(_glah01_arrays())
        _read_group(
            h5, G, cfg.data_source, shard, grid, siblings={"glah05": _FakeH5(_glah05_arrays())}
        )
        wf_reads = [hs for p, hs in h5.reads if p.endswith("r_rng_wf")]
        # The 2-D waveform is sliced by RECORD rows -- shots 0..4 (the padded
        # run), never shot 5 and never a whole-dataset read.
        assert wf_reads == [[(0, 5)]]

    def test_missing_sibling_fails_the_group(self, cfg, grid):
        with pytest.raises(ValueError, match="no open sibling handle"):
            _read_group(_FakeH5(_glah01_arrays()), G, cfg.data_source, _shard_key(9), grid)

    def test_width_mismatch_raises_before_aggregation(self, cfg, grid):
        arrays = _glah01_arrays()
        arrays[f"/{G}/Waveform/RecWaveform/r_rng_wf"] = WF[:, :500]
        with pytest.raises(ValueError, match="has 500 entries but link.width is 544"):
            _read_group(
                _FakeH5(arrays),
                G,
                cfg.data_source,
                _shard_key(9),
                grid,
                siblings={"glah05": _FakeH5(_glah05_arrays())},
            )

    def test_cell_statistics(self, cfg, grid):
        from zagg.processing import calculate_cell_statistics

        df = _read_group(
            _FakeH5(_glah01_arrays()),
            G,
            cfg.data_source,
            _shard_key(9),
            grid,
            siblings={"glah05": _FakeH5(_glah05_arrays())},
        )
        one = df[df["utc_time"] == UTC[1]]  # the short waveform, alone
        cell = {c: one[c].to_numpy() for c in one.columns if c != "leaf_id"}
        stats = calculate_cell_statistics(cell, config=cfg)
        assert stats["count"] == 200
        assert stats["shot_count"] == 1
        assert stats["utc_time"] == pytest.approx(UTC[1])
        assert stats["noise_mean"] == pytest.approx(BG_MEAN[1])
        assert stats["gain_set"] == pytest.approx(GAIN[1])
        assert stats["rx_energy"] == pytest.approx(ENERGY[1])
        flux = stats["rx_flux"]
        # 2.0 V in every valid gate, far above the 0.04 noise floor: all 200
        # samples survive the clip with weight (count - noise_mean) * gain.
        assert flux.shape == (200, 2)
        np.testing.assert_allclose(flux[:, 1], np.float32(2.0 - BG_MEAN[1]), rtol=1e-6)
        assert flux[:, 0].min() == pytest.approx(REF_RNG[1])
        assert flux[:, 0].max() == pytest.approx(REF_RNG[1] + 199 * 0.15)

    def test_end_to_end_leaf_write(self, cfg, grid, tmp_path, monkeypatch):
        import zarr

        from zagg import hive
        from zagg.index.hierarchical import HierarchicalIndex
        from zagg.store import open_store

        handles = {
            "b/GLAH01_033.H5": _glah01_arrays(),
            "b/GLAH05_033.H5": _glah05_arrays(),
        }
        monkeypatch.setattr(
            "zagg.processing.h5coro.H5Coro",
            lambda resource, driver, **kw: _FakeH5(handles[resource], resource),
        )
        monkeypatch.setattr(
            "zagg.processing.worker.index_from_config", lambda cfg: HierarchicalIndex()
        )
        shard = _shard_key(cfg.output["grid"]["parent_order"])
        entry = {"url": "s3://b/GLAH01_033.H5", "assets": {"glah05": "s3://b/GLAH05_033.H5"}}
        root = str(tmp_path / "store")
        meta = hive.process_and_write_hive(shard, [entry], grid, {}, root, cfg, store_kwargs={})
        assert meta["error"] is None
        assert meta["total_obs"] == 2 * WIDTH + 200
        leaf_store = open_store(hive.shard_leaf_path(root, shard))
        grp = zarr.open_group(leaf_store, path=grid.group_path, mode="r", zarr_format=3)
        count = np.asarray(grp["count"][:])
        # Shots 0..2 share one sibling footprint, so they land in exactly one
        # cell, and every kept sample is counted there exactly once.
        assert count.sum() == 2 * WIDTH + 200
        occupied = np.flatnonzero(count)
        assert len(occupied) == 1
        cell = int(occupied[0])
        assert np.asarray(grp["shot_count"][:])[cell] == 3
        payload = grp["rx_flux"][cell : cell + 1][0]
        centroids = np.frombuffer(payload, "<f4").reshape(-1, 2)
        # Summed weight = sum((amp - noise_mean) * gain) over the kept gates.
        gain = cfg.aggregation["variables"]["rx_flux"]["params"]["gain"]
        valid = [WIDTH, 200, WIDTH]
        expected = sum(float(((WF[i, : valid[i]] - BG_MEAN[i]) * gain).sum()) for i in range(3))
        assert centroids[:, 1].sum() == pytest.approx(expected, rel=1e-5)
        assert centroids[:, 0].min() == pytest.approx(REF_RNG[:3].min())
        assert centroids[:, 0].max() == pytest.approx(REF_RNG[:3].max() + (WIDTH - 1) * 0.15)
        assert hive.read_commit(leaf_store) is not None
