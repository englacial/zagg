"""Tests for the raster pipeline core (issue #218 phase 3).

Config validation, acquisition-group time indexing, per-item multi-band
sampling, the nearest-tile-center ownership rule, and the lean ``(time,
cells)`` template + slab writer — all against synthetic GeoTIFFs from
``test_raster._write_tiff`` (no GDAL, no network) and in-memory Zarr stores.
"""

import numpy as np
import pytest
from pyproj import CRS, Transformer
from test_raster import ORIGIN, RES, TRANSFORM, UTM18, _index_raster, _write_tiff
from zarr import open_array
from zarr.errors import ContainsGroupError
from zarr.storage import MemoryStore

from zagg.config import get_raster_bands, load_config_from_dict, validate_config
from zagg.grids import HealpixGrid, RectilinearGrid
from zagg.processing.raster import (
    _run_sync,
    _shard_cell_range,
    _shard_workers,
    _write_buffer,
    emit_raster_leaf_template,
    emit_raster_template,
    new_stage_stats,
    process_raster_shard,
    raster_group_spec,
    raster_leaf_spec,
    raster_time_index,
    sample_item_async,
    write_raster_coords,
    write_raster_slab,
)
from zagg.time_axis import decode_time_axis, read_time_axis, time_axis_attrs

T0 = "2026-07-13T16:02:20+00:00"
T0B = "2026-07-13T16:02:24+00:00"  # same datatake, adjacent tile: seconds later
T1 = "2026-07-18T16:02:20+00:00"


def _raster_config(bands=None, nodata=0, grid=None, time_encoding=None):
    output = {
        "grid": grid or {"type": "healpix", "parent_order": 10, "child_order": 16},
        "store": "memory://",
    }
    if time_encoding:
        output["time_encoding"] = time_encoding
    return load_config_from_dict(
        {
            "data_source": {
                "reader": "raster",
                "bands": bands
                or {
                    "red": {
                        "asset": "red",
                        "dtype": "uint16",
                        "fill_value": 0,
                        "scale": 0.0001,
                        "offset": -0.1,
                    },
                    "scl": {"asset": "scl", "dtype": "uint16", "fill_value": 0},
                },
                "nodata": nodata,
            },
            "output": output,
        }
    )


def _entry(gid, assets, dt, time_key=None, time_start=None, time_end=None):
    e = {"id": gid, "s3": None, "https": None, "assets": assets, "datetime": dt}
    if time_key:
        e["time_key"] = time_key
    if time_start:
        e["time_start"] = time_start
    if time_end:
        e["time_end"] = time_end
    return e


class TestRasterConfigValidation:
    def test_valid_config_passes(self):
        validate_config(_raster_config())

    def test_aggregation_section_rejected(self):
        cfg = _raster_config()
        cfg.aggregation = {"variables": {"x": {"function": "mean"}}}
        with pytest.raises(ValueError, match="no aggregation section"):
            validate_config(cfg)

    def test_missing_bands_rejected(self):
        cfg = _raster_config()
        cfg.data_source.pop("bands")
        with pytest.raises(ValueError, match="data_source.bands"):
            validate_config(cfg)

    def test_band_requires_dtype(self):
        with pytest.raises(ValueError, match="requires a string 'dtype'"):
            validate_config(_raster_config(bands={"red": {"asset": "red"}}))

    def test_sharded_rejected_permanently(self):
        # Espg-ratified on issue #247: sharded raster output is a PERMANENT
        # exclusion (per-timestep slab streaming would read-modify-write the
        # ShardingCodec object), not a deferral — the message says why.
        grid = {"type": "healpix", "parent_order": 10, "child_order": 16, "sharded": True}
        with pytest.raises(ValueError, match="read-modify-write"):
            validate_config(_raster_config(grid=grid))

    def test_hive_store_layout_validates(self):
        # Issue #247: raster + hive is legal (the issue #239 stopgap is gone).
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        validate_config(cfg)

    def test_hive_plus_sharded_rejected(self):
        grid = {"type": "healpix", "parent_order": 10, "child_order": 16, "sharded": True}
        cfg = _raster_config(grid=grid)
        cfg.output["store_layout"] = "hive"
        with pytest.raises(ValueError, match="read-modify-write"):
            validate_config(cfg)

    def test_coverage_moc_validates_on_hive(self):
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["coverage_moc"] = True
        validate_config(cfg)

    def test_coverage_moc_rejected_on_flat(self):
        # The shared check (issue #200 posture): an explicit true without the
        # hive layout is a config mistake, not a no-op.
        cfg = _raster_config()
        cfg.output["store_layout"] = "flat"  # explicit: healpix defaults hive (issue #253)
        cfg.output["coverage_moc"] = True
        with pytest.raises(ValueError, match="store_layout: hive"):
            validate_config(cfg)

    def test_hive_consolidate_metadata_rejected(self):
        # Issue #247: raster now routes through the shared store-layout check,
        # so hive + consolidate_metadata: true hits the "no store-root zarr
        # hierarchy" rejection (D5/D12) just like the point path.
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["consolidate_metadata"] = True
        with pytest.raises(ValueError, match="no store-root zarr hierarchy"):
            validate_config(cfg)

    def test_coverage_moc_defaults_on_for_hive(self):
        # get_coverage_moc defaults ON for raster+hive (O9 root coverage) and
        # OFF for raster flat — the shared default resolution now covers raster.
        from zagg.config import get_coverage_moc

        hive = _raster_config()
        hive.output["store_layout"] = "hive"
        assert get_coverage_moc(hive) is True

        flat = _raster_config()
        flat.output["store_layout"] = "flat"  # explicit: healpix defaults hive (issue #253)
        assert get_coverage_moc(flat) is False

    def test_windowing_validates_and_normalizes(self):
        # Issue #247 (ratified): raster window membership is the acquisition's
        # STAC datetime — time_field is optional and the manifest records the
        # resolved field plus the fixed ISO-instant encoding.
        from zagg.config import get_windowing

        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "yearly"}
        validate_config(cfg)
        assert get_windowing(cfg) == {
            "schedule": "yearly",
            "time_field": "datetime",
            "epoch": "1970-01-01T00:00:00+00:00",
            "scale": "utc",
            "units": "seconds",
            "windows": None,
        }

    def test_windowing_explicit_time_field_datetime_accepted(self):
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "yearly", "time_field": "datetime"}
        validate_config(cfg)

    def test_windowing_other_time_field_rejected(self):
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "yearly", "time_field": "delta_time"}
        with pytest.raises(ValueError, match="STAC"):
            validate_config(cfg)

    def test_windowing_conversion_knobs_rejected(self):
        # epoch/scale/units describe a dataset-unit time_field conversion;
        # STAC datetimes are already ISO-8601 UTC — nothing to configure.
        for knob, value in (("epoch", "2018-01-01"), ("scale", "gps"), ("units", "days")):
            cfg = _raster_config()
            cfg.output["store_layout"] = "hive"
            cfg.output["windowing"] = {"schedule": "yearly", knob: value}
            with pytest.raises(ValueError, match=f"output.windowing.{knob}"):
                validate_config(cfg)

    def test_windowing_requires_hive_layout(self):
        # raster + flat + windowing lands on the SHARED hive-only check.
        cfg = _raster_config()
        cfg.output["store_layout"] = "flat"  # explicit: healpix defaults hive (issue #253)
        cfg.output["windowing"] = {"schedule": "yearly"}
        with pytest.raises(ValueError, match="store_layout: hive"):
            validate_config(cfg)

    def test_windowing_schedule_none_inert(self):
        from zagg.config import get_windowing

        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "none"}
        validate_config(cfg)
        assert get_windowing(cfg) is None

    def test_windowing_explicit_list_validated(self):
        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "windows": [
                {"label": "a", "start": "2019-02-01", "end": "2019-01-01"},
            ],
        }
        with pytest.raises(ValueError, match="half-open"):
            validate_config(cfg)

    def test_windowing_explicit_list_normalizes(self):
        # Issue #247: the happy path for schedule: explicit — a valid two-window
        # list normalizes through get_windowing with each boundary canonicalized
        # to ISO-8601 UTC, mirroring the generative case in
        # test_windowing_validates_and_normalizes so both normalizations stay locked.
        from zagg.config import get_windowing

        cfg = _raster_config()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "windows": [
                {"label": "q1", "start": "2019-01-01", "end": "2019-04-01"},
                {"label": "q2", "start": "2019-04-01", "end": "2019-07-01"},
            ],
        }
        validate_config(cfg)
        assert get_windowing(cfg) == {
            "schedule": "explicit",
            "time_field": "datetime",
            "epoch": "1970-01-01T00:00:00+00:00",
            "scale": "utc",
            "units": "seconds",
            "windows": [
                {
                    "label": "q1",
                    "start": "2019-01-01T00:00:00+00:00",
                    "end": "2019-04-01T00:00:00+00:00",
                },
                {
                    "label": "q2",
                    "start": "2019-04-01T00:00:00+00:00",
                    "end": "2019-07-01T00:00:00+00:00",
                },
            ],
        }

    def test_rectilinear_grid_rejected_for_now(self):
        grid = {"type": "rectilinear", "crs": UTM18, "resolution": 10, "bounds": [0, 0, 1, 1]}
        with pytest.raises(ValueError, match="healpix"):
            validate_config(_raster_config(grid=grid))

    def test_bool_fill_value_rejected(self):
        with pytest.raises(ValueError, match="fill_value must be a number"):
            validate_config(
                _raster_config(
                    bands={"red": {"asset": "red", "dtype": "uint16", "fill_value": True}}
                )
            )

    def test_get_raster_bands_normalizes(self):
        bands = get_raster_bands(_raster_config())
        assert bands["red"]["attrs"] == {"scale_factor": 0.0001, "add_offset": -0.1}
        assert bands["red"]["fill_value"] == 0
        assert bands["scl"]["attrs"] == {}

    def test_shard_workers_default_and_override(self):
        assert _shard_workers(_raster_config()) == 4  # issue #231 default
        cfg = _raster_config()
        cfg.data_source["shard_workers"] = 8
        assert _shard_workers(cfg) == 8

    def test_shard_workers_rejected(self):
        for bad in (0, -1, True, 2.0):
            cfg = _raster_config()
            cfg.data_source["shard_workers"] = bad
            with pytest.raises(ValueError, match="shard_workers"):
                validate_config(cfg)
            # The worker helper re-checks with the same guard (hand-rolled payload).
            with pytest.raises(ValueError, match="shard_workers"):
                _shard_workers(cfg)


class TestRasterTimeIndex:
    def test_time_key_groups_adjacent_tiles(self):
        granules = [
            [
                _entry("a", {"red": "x"}, T0, time_key="dt-1"),
                _entry("b", {"red": "y"}, T0B, time_key="dt-1"),
                _entry("c", {"red": "z"}, T1, time_key="dt-2"),
            ]
        ]
        index, times = raster_time_index(granules)
        assert index == {"dt-1": 0, "dt-2": 1}
        # Group time is the EARLIEST member datetime.
        assert times[0] == np.int64(1_783_958_540_000_000)
        assert times.dtype == np.int64 and times.shape == (2,)

    def test_datetime_fallback_without_time_key(self):
        granules = [[_entry("a", {"red": "x"}, T0), _entry("b", {"red": "y"}, T1)]]
        index, times = raster_time_index(granules)
        assert index[T0] == 0 and index[T1] == 1
        assert times[1] > times[0]

    def test_non_raster_entries_ignored(self):
        granules = [[{"id": "h5", "s3": "s3://b/g.h5", "https": None}]]
        index, times = raster_time_index(granules)
        assert index == {} and times.size == 0

    def test_missing_datetime_raises(self):
        with pytest.raises(ValueError, match="no datetime"):
            raster_time_index([[{"id": "bad", "assets": {"red": "x"}}]])


class TestTocTimeIndex:
    """The §8 toc encoding of the acquisition-group axis (issue #443)."""

    def _granules(self):
        return [
            [
                _entry("a", {"red": "x"}, T0, time_key="dt-1"),
                _entry("b", {"red": "y"}, T0B, time_key="dt-1"),
                _entry("c", {"red": "z"}, T1, time_key="dt-2"),
            ]
        ]

    def test_group_span_becomes_a_range_word(self):
        index, words = raster_time_index(self._granules(), encoding="toc")
        # Row assignment is the encoding-independent part: same order, same
        # index, so a leaf's slab rows cannot drift with the encoding.
        assert index == raster_time_index(self._granules())[0] == {"dt-1": 0, "dt-2": 1}
        assert words.dtype == np.uint64 and words.shape == (2,)
        lo, hi = decode_time_axis(words, time_axis_attrs("toc"))
        # dt-1 spans T0..T0B (4 s apart) — a conservative range containing both.
        assert lo[0] <= np.datetime64(T0[:-6], "ns") and hi[0] > np.datetime64(T0B[:-6], "ns")
        # dt-2 is a single item: an exact instant, not widened into a range.
        assert lo[1] == hi[1] == np.datetime64(T1[:-6], "ns")

    def test_stac_start_end_datetime_widens_the_envelope(self):
        granules = [
            [
                _entry(
                    "a",
                    {"red": "x"},
                    T0,
                    time_key="dt-1",
                    time_start="2026-07-13T16:02:18+00:00",
                    time_end="2026-07-13T16:02:29+00:00",
                )
            ]
        ]
        _index, words = raster_time_index(granules, encoding="toc")
        lo, hi = decode_time_axis(words, time_axis_attrs("toc"))
        assert lo[0] <= np.datetime64("2026-07-13T16:02:18", "ns")
        assert hi[0] > np.datetime64("2026-07-13T16:02:29", "ns")

    def test_legacy_encoding_ignores_the_span(self):
        # A pre-§8 axis is the earliest ITEM datetime, spans or not — the
        # legacy values must not move when a catalog gains start/end columns.
        granules = [
            [
                _entry(
                    "a",
                    {"red": "x"},
                    T0,
                    time_key="dt-1",
                    time_start="2026-07-13T16:02:18+00:00",
                    time_end="2026-07-13T16:02:29+00:00",
                )
            ]
        ]
        _index, times = raster_time_index(granules)
        assert times.dtype == np.int64 and times[0] == np.int64(1_783_958_540_000_000)

    def test_a_leading_span_puts_the_stored_words_out_of_row_order(self):
        # The §8.1 divergence, on the production function: row order keys on
        # the group's earliest ITEM datetime, but the word encodes the
        # ENVELOPE start. A later row whose declared start_datetime precedes
        # an earlier row's item datetime therefore encodes a SMALLER word --
        # so the stored axis is materially unsorted and MUST NOT be bisected,
        # while the row assignment stays identical to the legacy encoding.
        granules = [
            [
                _entry("a", {"red": "x"}, T0, time_key="dt-1"),
                _entry(
                    "b",
                    {"red": "y"},
                    "2026-07-13T16:02:30+00:00",
                    time_key="dt-2",
                    time_start="2026-07-13T16:00:59+00:00",
                    time_end="2026-07-13T16:02:40+00:00",
                ),
            ]
        ]
        index, words = raster_time_index(granules, encoding="toc")
        # Row assignment is stable across encodings — the load-bearing claim.
        assert index == raster_time_index(granules)[0] == {"dt-1": 0, "dt-2": 1}
        # ...but the stored words descend, by the span's ~81 s lead.
        assert words[1] < words[0]
        assert not np.array_equal(np.sort(words), words)
        lo, _hi = decode_time_axis(words, time_axis_attrs("toc"))
        assert lo[1] <= np.datetime64("2026-07-13T16:00:59", "ns") < lo[0]

    def test_empty_toc_axis(self):
        index, words = raster_time_index([[]], encoding="toc")
        assert index == {} and words.dtype == np.uint64 and words.size == 0

    def test_template_declares_and_round_trips(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path, time_encoding="toc")
        _index, words = raster_time_index(self._granules(), encoding="toc")
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, words)
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        assert tarr.dtype == np.uint64
        assert "units" not in tarr.attrs and "calendar" not in tarr.attrs
        assert dict(tarr.attrs)["temporal"]["spec"] == "zagg-toc/1"
        np.testing.assert_array_equal(tarr[:], words)
        # And the read path decodes the store without being told the encoding.
        lo, hi = read_time_axis(store, grid.group_path)
        assert lo.shape == (2,) and (hi >= lo).all()

    def test_leaf_template_declares_toc(self, tmp_path):
        cfg, grid, shard = _healpix_setup(tmp_path, time_encoding="toc")
        _index, words = raster_time_index(self._granules(), encoding="toc")
        store = MemoryStore()
        emit_raster_leaf_template(store, grid, cfg, shard, words)
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        assert tarr.dtype == np.uint64
        np.testing.assert_array_equal(tarr[:], words)

    def test_legacy_store_still_reads(self, tmp_path):
        # Schema evolution: a store written before §8 decodes through the same
        # reader, no declaration and no refusal.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([1_000_000, 2_000_000], dtype=np.int64))
        lo, hi = read_time_axis(store, grid.group_path)
        np.testing.assert_array_equal(lo, hi)
        assert lo[0] == np.datetime64("1970-01-01T00:00:01", "ns")


class _FakeGrid:
    """Minimal grid for the sampling-concurrency test: only ``children`` is
    touched on the single-item-per-group path (no ownership combine)."""

    def __init__(self, n_cells):
        self._n = n_cells

    def children(self, shard_key):
        return np.arange(self._n)


class TestSampleConcurrency:
    # k=1 serial (peak 1), an interior cap (peak k), and k>=n_groups (peak
    # n_groups): an off-by-one in the semaphore width passes k=3 but fails k=1.
    @pytest.mark.parametrize("k", [1, 3, 10, 12])
    def test_semaphore_bounds_in_flight_groups(self, monkeypatch, k):
        # N single-item acquisition groups sampled under Semaphore(K): each
        # group is one ``sample_item_async`` call, so concurrent calls track
        # concurrent timesteps. An instrumented fake records the peak, which
        # must be capped at min(K, N) yet actually reach it (issue #231: the cap
        # bounds memory without serializing the fan-out).
        import asyncio as _asyncio

        from zagg.processing import raster as raster_mod

        n_cells, n_groups = 8, 10
        state = {"cur": 0, "max": 0}
        lock = _asyncio.Lock()

        async def _fake_sample_item(
            grid, cells, assets, bands, *, nodata=None, region=None, anonymous=True, **_kw
        ):
            async with lock:
                state["cur"] += 1
                state["max"] = max(state["max"], state["cur"])
            await _asyncio.sleep(0.02)
            async with lock:
                state["cur"] -= 1
            n = len(cells)
            return {f: np.zeros(n, dtype=np.uint16) for f in bands}, np.ones(n, bool), (0.0, 0.0)

        monkeypatch.setattr(raster_mod, "sample_item_async", _fake_sample_item)

        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        cfg.data_source["shard_workers"] = k
        granules = [
            _entry(f"g{i}", {"red": f"r{i}.tif"}, T0, time_key=f"dt-{i}") for i in range(n_groups)
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(_FakeGrid(n_cells), 0, granules, cfg, index)
        assert meta["timesteps"] == n_groups
        assert set(slabs) == set(range(n_groups))
        # Bounded by min(K, N), and the fan-out reaches it (not serialized).
        assert state["max"] == min(k, n_groups)


def _rect_grid(bounds, chunk):
    from zagg.config import default_config

    return RectilinearGrid(UTM18, RES, bounds, chunk, config=default_config("atl06_polar"))


class TestSampleItem:
    def test_multi_band_values_and_nodata(self, tmp_path):
        data = _index_raster()
        data[:8, :8] = 0  # nodata corner (config nodata=0)
        _write_tiff(tmp_path / "red.tif", data)
        _write_tiff(tmp_path / "scl.tif", np.full((96, 96), 4, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cells = np.arange(96 * 96)
        bands = get_raster_bands(_raster_config())
        assets = {"red": str(tmp_path / "red.tif"), "scl": str(tmp_path / "scl.tif")}
        values, valid, center = _run_sync(sample_item_async(grid, cells, assets, bands, nodata=0))
        expect = _index_raster()
        expect[:8, :8] = 0
        np.testing.assert_array_equal(values["red"], expect.ravel())
        assert (values["scl"] == 4).all()
        # nodata corner is invalid; the rest valid.
        assert not valid.reshape(96, 96)[:8, :8].any()
        assert valid.reshape(96, 96)[8:, 8:].all()
        # tile center of the 960 m raster, back-projected.
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
        assert center == pytest.approx((lon, lat), abs=1e-9)

    def test_missing_configured_asset_raises(self, tmp_path):
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        bands = get_raster_bands(_raster_config())
        with pytest.raises(ValueError, match="missing configured asset"):
            _run_sync(sample_item_async(grid, np.arange(4), {"red": "x.tif"}, bands))


class TestOwnership:
    def test_nearest_tile_center_wins_in_overlap(self, tmp_path):
        # Tile A (constant 100) and tile B (constant 200) in one datatake,
        # offset 480 m: overlap x in [300480, 300960); centers 300480 / 300960,
        # midline 300720.
        _write_tiff(tmp_path / "a.tif", np.full((96, 96), 100, dtype=np.uint16))
        _write_tiff(
            tmp_path / "b.tif",
            np.full((96, 96), 200, dtype=np.uint16),
            origin=(ORIGIN[0] + 480.0, ORIGIN[1]),
        )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1440.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 144])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "b.tif")}, T0B, time_key="dt-1"),
        ]
        index, _times = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 1 and set(slabs) == {0}
        red = slabs[0]["red"].reshape(96, 144)
        xs = ORIGIN[0] + (np.arange(144) + 0.5) * RES
        assert (red[:, xs < 300700] == 100).all()  # A side (margin off the midline)
        assert (red[:, (xs > 300740) & (xs < 300960)] == 200).all()  # B side of overlap
        assert (red[:, xs > 300970] == 200).all()  # B-only region
        assert (red[:, xs < 300480] == 100).all()  # A-only region

    def test_missing_time_key_in_index_raises(self, tmp_path):
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [_entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-absent")]
        with pytest.raises(ValueError, match="dt-absent"):
            process_raster_shard(grid, 0, granules, cfg, {})

    def test_two_timesteps_two_items_concurrent(self, tmp_path):
        # 2 datatakes x 2 overlapping tiles sampled in one event loop: each
        # timestep's ownership combine must match the sequential golden.
        for name, const, ox in (
            ("a0", 100, 0.0),
            ("b0", 200, 480.0),
            ("a1", 50, 0.0),
            ("b1", 75, 480.0),
        ):
            _write_tiff(
                tmp_path / f"{name}.tif",
                np.full((96, 96), const, dtype=np.uint16),
                origin=(ORIGIN[0] + ox, ORIGIN[1]),
            )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1440.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 144])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A0", {"red": str(tmp_path / "a0.tif")}, T0, time_key="dt-1"),
            _entry("B0", {"red": str(tmp_path / "b0.tif")}, T0B, time_key="dt-1"),
            _entry("A1", {"red": str(tmp_path / "a1.tif")}, T1, time_key="dt-2"),
            _entry("B1", {"red": str(tmp_path / "b1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 2 and set(slabs) == {0, 1}
        xs = ORIGIN[0] + (np.arange(144) + 0.5) * RES
        for t, aval, bval in ((0, 100, 200), (1, 50, 75)):
            red = slabs[t]["red"].reshape(96, 144)
            assert (red[:, xs < 300480] == aval).all()  # A-only region
            assert (red[:, xs > 300970] == bval).all()  # B-only region
            assert (red[:, (xs > 300740) & (xs < 300960)] == bval).all()  # B side of overlap

    def test_three_item_ownership(self, tmp_path):
        # Three overlapping tiles offset 480 m apart, one datatake, distinct
        # constants: every cell must take the nearest tile center's value.
        for name, const, ox in (("a", 100, 0.0), ("b", 200, 480.0), ("c", 300, 960.0)):
            _write_tiff(
                tmp_path / f"{name}.tif",
                np.full((96, 96), const, dtype=np.uint16),
                origin=(ORIGIN[0] + ox, ORIGIN[1]),
            )
        bounds = [ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 1920.0, ORIGIN[1]]
        grid = _rect_grid(bounds, [96, 192])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "b.tif")}, T0B, time_key="dt-1"),
            _entry("C", {"red": str(tmp_path / "c.tif")}, T0B, time_key="dt-1"),
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["timesteps"] == 1
        red = slabs[0]["red"].reshape(96, 192)
        xs = ORIGIN[0] + (np.arange(192) + 0.5) * RES
        assert (red[:, xs < 300470] == 100).all()  # A-only region
        assert (red[:, (xs > 300740) & (xs < 300950)] == 200).all()  # A/B overlap, B nearer
        assert (red[:, (xs > 301000) & (xs < 301180)] == 200).all()  # B/C overlap, B nearer
        assert (red[:, (xs > 301220) & (xs < 301430)] == 300).all()  # B/C overlap, C nearer
        assert (red[:, xs > 301450] == 300).all()  # C-only region

    def test_single_item_timesteps_and_skips(self, tmp_path):
        _write_tiff(tmp_path / "a.tif", np.full((96, 96), 7, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}})
        granules = [
            _entry("A", {"red": str(tmp_path / "a.tif")}, T0),
            {"id": "h5-styled", "s3": "s3://b/g.h5", "https": None},
        ]
        index, _ = raster_time_index([granules])
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        assert meta["skipped"] == 1 and meta["granule_count"] == 2
        assert (slabs[0]["red"] == 7).all()

    @pytest.mark.parametrize("wb", [2, 3])
    def test_write_buffer_bounded_and_matches(self, tmp_path, wb):
        # PR #232 double-buffer: with write_buffer=N the sink runs on worker
        # threads, at most N slabs are alive at once, overlap actually occurs,
        # and the streamed output still matches dict mode exactly.
        import threading
        import time as _time

        vals = {"dt-1": 11, "dt-2": 22, "dt-3": 33, "dt-4": 44}
        granules = []
        for i, (tk, v) in enumerate(vals.items()):
            _write_tiff(tmp_path / f"s{i}.tif", np.full((96, 96), v, dtype=np.uint16))
            granules.append(
                _entry(
                    f"g{i}",
                    {"red": str(tmp_path / f"s{i}.tif")},
                    f"2026-07-{13 + i:02d}T16:02:20+00:00",
                    time_key=tk,
                )
            )
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        index, _ = raster_time_index([granules])
        golden, _gm = process_raster_shard(grid, 0, granules, cfg, index)

        cfg.data_source["write_buffer"] = wb
        lock = threading.Lock()
        live = {"now": 0, "max": 0}
        streamed = {}

        def _sink(t_idx, slab):
            with lock:
                live["now"] += 1
                live["max"] = max(live["max"], live["now"])
            _time.sleep(0.05)  # slow write: forces overlap under the buffer
            streamed[t_idx] = slab
            with lock:
                live["now"] -= 1

        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)
        assert slabs == {} and meta["timesteps"] == 4
        assert live["max"] <= wb - 1  # sink calls in flight (slabs alive <= wb)
        assert set(streamed) == set(golden)
        for t in golden:
            np.testing.assert_array_equal(streamed[t]["red"], golden[t]["red"])

    def test_write_buffer_validation(self):
        cfg = _raster_config()
        assert _write_buffer(cfg) == 1  # default: strict serial bound
        cfg.data_source["write_buffer"] = 2
        assert _write_buffer(cfg) == 2
        for bad in (0, -1, 1.5, True, "2"):
            cfg.data_source["write_buffer"] = bad
            with pytest.raises(ValueError, match="write_buffer"):
                _write_buffer(cfg)
            with pytest.raises(ValueError, match="write_buffer"):
                validate_config(cfg)

    def test_write_buffer_sink_error_propagates(self, tmp_path):
        _write_tiff(tmp_path / "t0.tif", np.full((96, 96), 11, dtype=np.uint16))
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 22, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        cfg.data_source["write_buffer"] = 2
        granules = [
            _entry("A", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])

        def _sink(t_idx, slab):
            raise OSError("s3 write failed")

        with pytest.raises(OSError, match="s3 write failed"):
            process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)

    def test_on_slab_streams_and_matches_dict(self, tmp_path):
        # The on_slab sink (issue #231): each timestep's slab is handed off as
        # its group completes and NOT accumulated (returned slabs is empty),
        # yet the streamed slabs match the buffered dict-mode output exactly.
        _write_tiff(tmp_path / "t0.tif", np.full((96, 96), 11, dtype=np.uint16))
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 22, dtype=np.uint16))
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}, nodata=None)
        granules = [
            _entry("A", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("B", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, _ = raster_time_index([granules])
        golden, _gm = process_raster_shard(grid, 0, granules, cfg, index)

        streamed = {}

        def _sink(t_idx, slab):
            streamed[t_idx] = slab

        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index, on_slab=_sink)
        assert slabs == {}  # streamed + freed, nothing accumulated
        assert meta["timesteps"] == 2
        assert set(streamed) == set(golden) == {0, 1}
        for t in golden:
            np.testing.assert_array_equal(streamed[t]["red"], golden[t]["red"])


def _healpix_setup(tmp_path, time_encoding=None):
    """Order-10 shard over the synthetic raster; order-16 cells (~97 m)."""
    from mortie import clip2order, geo2mort

    to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
    lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
    leaf = geo2mort(lat, lon, order=29, points=True)
    shard = int(clip2order(10, leaf)[0])
    cfg = _raster_config(
        bands={"red": {"asset": "red", "dtype": "uint16", "scale": 0.0001, "offset": -0.1}},
        grid={"type": "healpix", "parent_order": 10, "child_order": 16},
        time_encoding=time_encoding,
    )
    grid = HealpixGrid(10, 16, config=cfg)
    return cfg, grid, shard


class TestTemplateAndSlabs:
    def test_group_spec_shapes(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_group_spec(grid, cfg, 3)
        red = spec.members["red"]
        n_cells = 12 * 4**16  # fullsphere cell axis (shape is metadata; writes stay sparse)
        assert tuple(red.shape) == (3, n_cells)
        cg = red.chunk_grid
        cfg_block = cg["configuration"] if isinstance(cg, dict) else cg.configuration
        assert tuple(cfg_block["chunk_shape"]) == (1, grid.cells_per_chunk)
        assert red.attributes["scale_factor"] == 0.0001
        assert red.attributes["add_offset"] == -0.1
        assert tuple(spec.members["time"].shape) == (3,)
        assert tuple(spec.members["morton"].shape) == (n_cells,)

    def test_sharded_grid_rejected(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        grid.sharded = True
        with pytest.raises(ValueError, match="sharded"):
            raster_group_spec(grid, cfg, 1)

    def test_end_to_end_two_timesteps(self, tmp_path):
        cfg, grid, shard = _healpix_setup(tmp_path)
        data = _index_raster()
        _write_tiff(tmp_path / "t0.tif", data)
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 321, dtype=np.uint16))
        granules = [
            _entry("g0", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("g1", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, times = raster_time_index([granules])

        store = MemoryStore()
        emit_raster_template(store, grid, cfg, times)
        slabs, meta = process_raster_shard(grid, shard, granules, cfg, index)
        assert set(slabs) == {0, 1}
        for t, slab in slabs.items():
            write_raster_slab(store, grid, shard, t, slab)
        write_raster_coords(store, grid, shard)

        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (2, 12 * 4**16)
        start, stop = _shard_cell_range(grid, shard)
        assert stop - start == 4096  # 4^(child - parent)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        got_t0 = red[0, start:stop]
        np.testing.assert_array_equal(got_t0[valid], data[rows[valid], cols[valid]])
        assert (got_t0[~valid] == 0).all()  # fill outside the raster footprint
        got_t1 = red[1, start:stop]
        assert (got_t1[valid] == 321).all()
        # time coordinate round-trips as microseconds since epoch.
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], times)
        # morton written for the shard's block, in children order (D16 —
        # packed words are the sole stored cell coordinate, issue #304).
        ids = open_array(store, path=f"{grid.group_path}/morton", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(ids[start:stop], np.asarray(cells, dtype=np.uint64))
        assert valid.sum() > 50  # the 960 m raster covers many ~97 m cells

    def test_zero_time_template(self, tmp_path):
        # An empty-times template (no datatakes yet) must emit, not crash.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_group_spec(grid, cfg, 0)
        assert tuple(spec.members["time"].shape) == (0,)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([], dtype=np.int64))
        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (0, 12 * 4**16)

    def test_overwrite_false_refuses_same_count_different_values(self, tmp_path):
        # overwrite=False must refuse a rerun whose timestep COUNT is unchanged
        # but whose time values differ: to_zarr's shape guard doesn't catch it,
        # so without an explicit check the unconditional time write would
        # silently clobber the coordinate workers slab-write against (issue
        # #264). An identical rerun stays idempotent; overwrite=True rewrites.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([100, 200], dtype=np.int64))
        # Idempotent: same values, overwrite=False, no raise.
        emit_raster_template(store, grid, cfg, np.array([100, 200], dtype=np.int64))
        # Same count, shifted values: refused, and the store is left untouched.
        with pytest.raises(ContainsGroupError):
            emit_raster_template(store, grid, cfg, np.array([100, 999], dtype=np.int64))
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], np.array([100, 200], dtype=np.int64))
        # overwrite=True rewrites the shifted coordinate cleanly.
        emit_raster_template(store, grid, cfg, np.array([100, 999], dtype=np.int64), overwrite=True)
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], np.array([100, 999], dtype=np.int64))

    def test_time_attrs_round_trip(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([1_000_000, 2_000_000], dtype=np.int64))
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        assert tarr.attrs["units"] == "microseconds since 1970-01-01T00:00:00"
        assert tarr.attrs["calendar"] == "proleptic_gregorian"

    def test_fullsphere_end_to_end_slab(self, tmp_path):
        # Fullsphere layout: shape 12*4^child, one shard == cells_per_shard.
        from mortie import clip2order, geo2mort

        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
        lon, lat = to_wgs.transform(ORIGIN[0] + 480.0, ORIGIN[1] - 480.0)
        leaf = geo2mort(lat, lon, order=29, points=True)
        shard = int(clip2order(4, leaf)[0])
        cfg = _raster_config(
            bands={"red": {"asset": "red", "dtype": "uint16"}},
            grid={"type": "healpix", "parent_order": 4, "child_order": 8},
        )
        grid = HealpixGrid(4, 8, layout="fullsphere", config=cfg)
        data = _index_raster()
        _write_tiff(tmp_path / "r.tif", data)
        granules = [_entry("g", {"red": str(tmp_path / "r.tif")}, T0, time_key="dt-1")]
        index, times = raster_time_index([granules])

        store = MemoryStore()
        emit_raster_template(store, grid, cfg, times)
        slabs, meta = process_raster_shard(grid, shard, granules, cfg, index)
        for t, slab in slabs.items():
            write_raster_slab(store, grid, shard, t, slab)
        write_raster_coords(store, grid, shard)

        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, 12 * 4**8)  # 786432
        start, stop = _shard_cell_range(grid, shard)
        assert stop - start == 256  # 4^(child - parent)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        got = red[0, start:stop]
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
        assert (got[~valid] == 0).all()  # fill outside the raster footprint


class TestLeafTemplate:
    """Per-(shard, window) hive leaf templates (issue #247 phase 2)."""

    def test_leaf_spec_shapes_and_root_group(self, tmp_path):
        from zagg.processing.raster import raster_leaf_spec

        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_leaf_spec(grid, cfg, 3)
        inner = spec.members[grid.group_path]  # root group wraps the members (D4 stamp target)
        red = inner.members["red"]
        assert tuple(red.shape) == (3, grid.cells_per_shard)
        cg = red.chunk_grid
        cfg_block = cg["configuration"] if isinstance(cg, dict) else cg.configuration
        assert tuple(cfg_block["chunk_shape"]) == (1, grid.cells_per_chunk)
        assert red.attributes["scale_factor"] == 0.0001
        assert tuple(inner.members["time"].shape) == (3,)
        assert tuple(inner.members["morton"].shape) == (grid.cells_per_shard,)

    def test_leaf_spec_sharded_rejected(self, tmp_path):
        from zagg.processing.raster import raster_leaf_spec

        cfg, grid, _shard = _healpix_setup(tmp_path)
        grid.sharded = True
        with pytest.raises(ValueError, match="read-modify-write"):
            raster_leaf_spec(grid, cfg, 1)

    def test_leaf_template_round_trip(self, tmp_path):
        # Emit one leaf, stream the shard's slabs into it at leaf-local
        # indices, and read everything back: per-band dtype/fill, the leaf's
        # own time axis, and the shard's morton words — written at template time,
        # not per-slab.
        from zagg.processing.raster import emit_raster_leaf_template, write_raster_leaf_slab

        cfg, grid, shard = _healpix_setup(tmp_path)
        data = _index_raster()
        _write_tiff(tmp_path / "t0.tif", data)
        _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 321, dtype=np.uint16))
        granules = [
            _entry("g0", {"red": str(tmp_path / "t0.tif")}, T0, time_key="dt-1"),
            _entry("g1", {"red": str(tmp_path / "t1.tif")}, T1, time_key="dt-2"),
        ]
        index, times = raster_time_index([granules])

        store = MemoryStore()
        emit_raster_leaf_template(store, grid, cfg, shard, times)
        slabs, _meta = process_raster_shard(grid, shard, granules, cfg, index)
        for t, slab in slabs.items():
            write_raster_leaf_slab(store, grid, t, slab)

        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (2, grid.cells_per_shard)
        assert red.dtype == np.uint16 and red.fill_value == 0
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        got_t0 = red[0, :]
        np.testing.assert_array_equal(got_t0[valid], data[rows[valid], cols[valid]])
        assert (got_t0[~valid] == 0).all()
        assert (red[1, :][valid] == 321).all()
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], times)
        assert tarr.attrs["units"] == "microseconds since 1970-01-01T00:00:00"
        ids = open_array(store, path=f"{grid.group_path}/morton", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(ids[:], np.asarray(cells, dtype=np.uint64))

    def test_leaf_overwrite_replaces_wholesale(self, tmp_path):
        # Retry semantics (D4): re-emitting with overwrite=True replaces the
        # prior attempt's arrays — a NARROWER time axis must not leave stale
        # timesteps behind.
        from zagg.processing.raster import emit_raster_leaf_template

        cfg, grid, shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_leaf_template(store, grid, cfg, shard, np.array([1, 2, 3], dtype=np.int64))
        emit_raster_leaf_template(
            store, grid, cfg, shard, np.array([9], dtype=np.int64), overwrite=True
        )
        red = open_array(store, path=f"{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, grid.cells_per_shard)
        tarr = open_array(store, path=f"{grid.group_path}/time", zarr_format=3, consolidated=False)
        np.testing.assert_array_equal(tarr[:], [9])

    def test_flat_template_unchanged_by_refactor(self, tmp_path):
        # The shared-members refactor must leave the FLAT template spec
        # byte-identical: same member set, shapes, chunking, codecs.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        spec = raster_group_spec(grid, cfg, 2)
        assert set(spec.members) == {"time", "morton", "red"}
        # Fullsphere cell axis (issue #88: the dense pack is gone; shape is
        # metadata — writes stay sparse).
        assert tuple(spec.members["red"].shape) == (2, 12 * 4**16)
        codecs = spec.members["red"].codecs
        names = [c["name"] if isinstance(c, dict) else c.name for c in codecs]
        assert names == ["bytes", "zstd"]


class TestRasterMortonCoordinate:
    """Issue #304 raster extension (espg-ruled): morton is the sole stored
    cell coordinate on the raster path too, with the same dggs attrs block
    and the same emit_cell_ids transition hatch as the spatial path."""

    def test_default_has_no_cell_ids(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        assert "cell_ids" not in raster_group_spec(grid, cfg, 2).members
        inner = raster_leaf_spec(grid, cfg, 2).members[grid.group_path]
        assert "cell_ids" not in inner.members

    def test_hatch_restores_cell_ids(self, tmp_path):
        cfg, grid, _shard = _healpix_setup(tmp_path)
        cfg.output["grid"]["emit_cell_ids"] = True
        grid = HealpixGrid(10, 16, config=cfg)
        spec = raster_group_spec(grid, cfg, 2)
        assert str(spec.members["cell_ids"].data_type) == "uint64"
        # And the leaf writer fills BOTH coords under the hatch.
        from zagg.processing.raster import emit_raster_leaf_template

        store = MemoryStore()
        emit_raster_leaf_template(store, grid, cfg, _shard, np.array([0], dtype=np.int64))
        ids = open_array(
            store, path=f"{grid.group_path}/cell_ids", zarr_format=3, consolidated=False
        )
        cells = grid.children(_shard)
        np.testing.assert_array_equal(
            ids[:], np.asarray(grid.encode_cell_ids(cells), dtype=np.uint64)
        )

    def test_dggs_attrs_match_spatial_contract(self, tmp_path):
        # One reader contract everywhere: the raster group carries the same
        # morton-declared block HealpixGrid emits for aggregation stores.
        cfg, grid, _shard = _healpix_setup(tmp_path)
        for attrs in (
            raster_group_spec(grid, cfg, 2).attributes,
            raster_leaf_spec(grid, cfg, 2).members[grid.group_path].attributes,
        ):
            assert attrs == grid._dggs_attrs()
            assert attrs["dggs"]["name"] == "morton"
            assert attrs["dggs"]["coordinate"] == "morton"
            names = [c["name"] for c in attrs["zarr_conventions"]]
            assert names == ["dggs", "morton-dggs"]

    def test_written_leaf_attrs_round_trip(self, tmp_path):
        import zarr

        from zagg.processing.raster import emit_raster_leaf_template

        cfg, grid, shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_leaf_template(store, grid, cfg, shard, np.array([0], dtype=np.int64))
        grp = zarr.open_group(store, path=grid.group_path, mode="r", zarr_format=3)
        assert grp.attrs["dggs"]["coordinate"] == "morton"
        assert "cell_ids" not in grp

    def test_written_flat_attrs_round_trip(self, tmp_path):
        # F14: the FLAT write is the one that changed from attributes={} to
        # grid._dggs_attrs(); pin the written-store attrs there too, not just
        # on the leaf.
        import zarr

        cfg, grid, _shard = _healpix_setup(tmp_path)
        store = MemoryStore()
        emit_raster_template(store, grid, cfg, np.array([0], dtype=np.int64))
        grp = zarr.open_group(store, path=grid.group_path, mode="r", zarr_format=3)
        assert grp.attrs["dggs"]["coordinate"] == "morton"
        assert "cell_ids" not in grp


class TestGeometryCache:
    """Issue #244: the pull-NN mapping is memoized per (epsg, transform, shape)
    within a shard invoke — once per distinct source grid, not once per
    asset-sample — with output byte-identical to the uncached path."""

    def _run_counting(self, tmp_path, monkeypatch, n_timesteps=4, second_grid=False):
        vals = [11, 22, 33, 44][:n_timesteps]
        granules = []
        for i, v in enumerate(vals):
            _write_tiff(tmp_path / f"s{i}.tif", np.full((96, 96), v, dtype=np.uint16))
            assets = {"red": str(tmp_path / f"s{i}.tif")}
            if second_grid:
                # A second, differently-shaped source grid (48x48 @ 20 m).
                _write_tiff(tmp_path / f"c{i}.tif", np.full((48, 48), 4, dtype=np.uint16), res=20.0)
                assets["scl"] = str(tmp_path / f"c{i}.tif")
            granules.append(
                _entry(f"g{i}", assets, f"2026-07-{13 + i:02d}T16:02:20+00:00", time_key=f"dt-{i}")
            )
        bands = {"red": {"asset": "red", "dtype": "uint16"}}
        if second_grid:
            bands["scl"] = {"asset": "scl", "dtype": "uint16"}
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands=bands, nodata=None)
        index, _ = raster_time_index([granules])

        calls = {"n": 0}
        real = type(grid).sample

        def _counting(self_, *a, **k):
            calls["n"] += 1
            return real(self_, *a, **k)

        monkeypatch.setattr(type(grid), "sample", _counting)
        slabs, meta = process_raster_shard(grid, 0, granules, cfg, index)
        return slabs, calls["n"]

    def test_one_compute_per_source_grid(self, tmp_path, monkeypatch):
        _slabs, n = self._run_counting(tmp_path, monkeypatch, n_timesteps=4)
        assert n == 1  # 4 timesteps x 1 band, one shared source grid

    def test_two_computes_for_two_grids(self, tmp_path, monkeypatch):
        _slabs, n = self._run_counting(tmp_path, monkeypatch, n_timesteps=3, second_grid=True)
        assert n == 2  # 3 timesteps x 2 bands over exactly two source grids

    def test_cached_output_matches_uncached(self, tmp_path):
        _write_tiff(tmp_path / "t0.tif", _index_raster())
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        bands = get_raster_bands(_raster_config(bands={"red": {"asset": "red", "dtype": "uint16"}}))
        assets = {"red": str(tmp_path / "t0.tif")}
        cells = np.arange(96 * 96)
        uncached = _run_sync(sample_item_async(grid, cells, assets, bands))
        shared: dict = {}
        cached1 = _run_sync(sample_item_async(grid, cells, assets, bands, geom_cache=shared))
        cached2 = _run_sync(sample_item_async(grid, cells, assets, bands, geom_cache=shared))
        assert len(shared) == 1
        for got in (cached1, cached2):
            np.testing.assert_array_equal(got[0]["red"], uncached[0]["red"])
            np.testing.assert_array_equal(got[1], uncached[1])


class TestStageStats:
    """Issue #249: opt-in per-stage sample profiling via ``stage_stats``."""

    _STAGES = ("open", "geometry", "fetch", "decode", "gather")

    def _run(self, tmp_path, n_timesteps=3, second_grid=True, stage_stats=None):
        # Mirrors TestGeomCache._run_counting: n timesteps of a 96x96 10 m
        # 'red' plus (optionally) a 48x48 20 m 'scl' — two distinct source
        # grids exercising the geom_cache hit accounting.
        vals = [11, 22, 33, 44][:n_timesteps]
        granules = []
        for i, v in enumerate(vals):
            _write_tiff(tmp_path / f"s{i}.tif", np.full((96, 96), v, dtype=np.uint16))
            assets = {"red": str(tmp_path / f"s{i}.tif")}
            if second_grid:
                _write_tiff(tmp_path / f"c{i}.tif", np.full((48, 48), 4, dtype=np.uint16), res=20.0)
                assets["scl"] = str(tmp_path / f"c{i}.tif")
            granules.append(
                _entry(f"g{i}", assets, f"2026-07-{13 + i:02d}T16:02:20+00:00", time_key=f"dt-{i}")
            )
        bands = {"red": {"asset": "red", "dtype": "uint16"}}
        if second_grid:
            bands["scl"] = {"asset": "scl", "dtype": "uint16"}
        grid = _rect_grid([ORIGIN[0], ORIGIN[1] - 960.0, ORIGIN[0] + 960.0, ORIGIN[1]], [96, 96])
        cfg = _raster_config(bands=bands, nodata=None)
        index, _ = raster_time_index([granules])
        return process_raster_shard(grid, 0, granules, cfg, index, stage_stats=stage_stats)

    def test_counts_and_stage_seconds(self, tmp_path):
        stats = new_stage_stats()
        _slabs, meta = self._run(tmp_path, n_timesteps=3, second_grid=True, stage_stats=stats)
        assert meta["timesteps"] == 3
        assert stats["assets"] == 6  # 3 timesteps x 2 bands
        assert stats["geom_hits"] == 4  # assets - 2 distinct source grids
        assert stats["tiles"] >= stats["assets"]  # every sample fetches >= 1 tile
        assert all(stats[k] >= 0.0 for k in self._STAGES)
        assert sum(stats[k] for k in self._STAGES) > 0.0

    def test_profiled_output_matches_default(self, tmp_path):
        golden, _m = self._run(tmp_path)
        stats = new_stage_stats()
        profiled, _m2 = self._run(tmp_path, stage_stats=stats)
        assert stats["assets"] == 6
        assert set(golden) == set(profiled)
        for t in golden:
            assert set(golden[t]) == set(profiled[t])
            for f in golden[t]:
                np.testing.assert_array_equal(golden[t][f], profiled[t][f])

    def test_default_path_makes_no_timing_calls(self, tmp_path, monkeypatch):
        # The issue #249 zero-overhead gate: with stage_stats=None the sample
        # path must never call time.time(). Rebind raster.py's module-level
        # ``time`` name to a booby trap — only this module's calls are caught.
        import zagg.processing.raster as raster_mod

        class _Boom:
            @staticmethod
            def time():
                raise AssertionError("time.time() called on the unprofiled sample path")

        monkeypatch.setattr(raster_mod, "time", _Boom)
        _slabs, meta = self._run(tmp_path, n_timesteps=1, second_grid=False)
        assert meta["timesteps"] == 1


class TestRasterHiveWorker:
    """The shared per-(shard, window) hive leaf write path (issue #247 phase 3)."""

    def _setup(self, tmp_path, value=555):
        cfg, grid, shard = _healpix_setup(tmp_path)
        data = np.full((96, 96), value, dtype=np.uint16)
        _write_tiff(tmp_path / "h0.tif", data)
        _write_tiff(tmp_path / "h1.tif", np.full((96, 96), 777, dtype=np.uint16))
        granules = [
            _entry("g0", {"red": str(tmp_path / "h0.tif")}, T0, time_key="dt-1"),
            _entry("g1", {"red": str(tmp_path / "h1.tif")}, T1, time_key="dt-2"),
        ]
        return cfg, grid, shard, granules, str(tmp_path / "hivestore")

    def test_windowed_leaf_stamp_and_coverage(self, tmp_path):
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(
            shard,
            granules[:1],
            grid,
            root,
            cfg,
            store_kwargs={},
            window={"label": "20260713"},
        )
        leaf = hive.shard_leaf_path(root, shard, window="20260713")
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["complete"] and stamp["spec"] == "morton-hive/2"
        assert stamp["window"] == "20260713"
        # D15 truth: the unit's actual acquisition extent (one instant here).
        assert stamp["time_range"] == [T0, T0]
        assert meta["time_range"] == [T0, T0]
        assert stamp["granule_count"] == 1
        # Occupied union = cells whose center lands on the (nodata-free) raster.
        cells = grid.children(shard)
        _rows, _cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        assert stamp["cells_with_data"] == int(valid.sum())
        # Edge shard (raster covers a fraction of the shard): real bitmap
        # sidecar, decoding to exactly the occupied cells.
        assert stamp["coverage"]["encoding"] == "bitmap"
        got = hive.read_coverage_bitmap(leaf, coverage=stamp["coverage"])
        np.testing.assert_array_equal(got, np.sort(np.asarray(cells, dtype=np.uint64)[valid]))
        # Leaf data at leaf-local indices, leaf-local time axis of length 1.
        red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, grid.cells_per_shard)
        assert (red[0, :][valid] == 555).all()

    def test_schedule_none_bare_leaf(self, tmp_path):
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, window=None
        )
        assert meta["timesteps"] == 2 and "time_range" not in meta
        leaf = hive.shard_leaf_path(root, shard)  # bare {full_id}.zarr name
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["spec"] == "morton-hive/1"
        assert "window" not in stamp and "time_range" not in stamp
        # D14 "full" is gated off without a window — even full occupancy would
        # stamp a bitmap; here the shard is edge-covered anyway.
        assert stamp["coverage"]["encoding"] == "bitmap"
        red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (2, grid.cells_per_shard)

    def test_disjoint_timesteps_coverage_is_union(self, tmp_path):
        # D14 spatial union across acquisitions: two timesteps whose footprints
        # occupy DISJOINT cell subsets of one shard must stamp coverage = the
        # OR of both. A regression dropping the union (e.g. sampling only the
        # last group) would decode to one timestep's set, which — since neither
        # is a superset of the other — this test rejects.
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard = _healpix_setup(tmp_path)
        # Second raster shifted ~480 m west: overlapping but neither footprint
        # covers the other, so each timestep owns shard cells the other misses
        # (the shard's covered patch sits at the raster's east edge, so a west
        # shift keeps both timesteps' exclusive cells inside this shard).
        origin1 = (ORIGIN[0] - 480.0, ORIGIN[1])
        transform1 = (RES, 0.0, origin1[0], 0.0, -RES, origin1[1])
        _write_tiff(tmp_path / "d0.tif", np.full((96, 96), 555, dtype=np.uint16))
        _write_tiff(tmp_path / "d1.tif", np.full((96, 96), 777, dtype=np.uint16), origin=origin1)
        granules = [
            _entry("g0", {"red": str(tmp_path / "d0.tif")}, T0, time_key="dt-1"),
            _entry("g1", {"red": str(tmp_path / "d1.tif")}, T1, time_key="dt-2"),
        ]

        cells = grid.children(shard)
        _r0, _c0, valid0 = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        _r1, _c1, valid1 = grid.sample(cells, UTM18, transform1, (96, 96))
        # Premise: both timesteps land cells, and neither is a superset — some
        # cell is valid in exactly one, so the union is strictly larger.
        assert valid0.any() and valid1.any()
        assert (valid0 & ~valid1).any() and (valid1 & ~valid0).any()
        union = valid0 | valid1

        root = str(tmp_path / "hivestore")
        process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, window=None
        )
        leaf = hive.shard_leaf_path(root, shard)
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["complete"]
        assert stamp["cells_with_data"] == int(union.sum())
        got = hive.read_coverage_bitmap(leaf, coverage=stamp["coverage"])
        np.testing.assert_array_equal(got, np.sort(np.asarray(cells, dtype=np.uint64)[union]))

    def test_no_data_unit_creates_no_prefix(self, tmp_path):
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, _granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(
            shard, [], grid, root, cfg, store_kwargs={}, window={"label": "20260713"}
        )
        assert meta["timesteps"] == 0
        leaf = hive.shard_leaf_path(root, shard, window="20260713")
        import os as _os

        assert not _os.path.exists(leaf)

    def test_torn_worker_debris_and_rerun_replaces(self, tmp_path, monkeypatch):
        import zagg.processing.raster as raster_mod
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        real_write = raster_mod.write_raster_leaf_slab
        calls = {"n": 0}

        def _tear(store, g, t, slab, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("torn mid-shard")
            return real_write(store, g, t, slab, **kwargs)

        monkeypatch.setattr(raster_mod, "write_raster_leaf_slab", _tear)
        with pytest.raises(RuntimeError, match="torn"):
            process_and_write_raster_hive(
                shard, granules, grid, root, cfg, store_kwargs={}, window=None
            )
        leaf = hive.shard_leaf_path(root, shard)
        import os as _os

        assert _os.path.exists(leaf)  # prefix exists...
        assert hive.read_commit(leaf) is None  # ...but unstamped: debris (D4)

        # Re-run replaces the leaf WHOLESALE (D13): a narrower time axis must
        # not leave the torn attempt's arrays or extra timesteps behind.
        monkeypatch.setattr(raster_mod, "write_raster_leaf_slab", real_write)
        process_and_write_raster_hive(
            shard, granules[:1], grid, root, cfg, store_kwargs={}, window=None
        )
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["complete"]
        red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, grid.cells_per_shard)

    # ── leaf skip-if-current + contraction guard (issue #388 phase 2) ────────

    def _committed_leaf_with_sidecar(self, tmp_path, use=None):
        """First run: the real seam commits the leaf; then the sidecar a
        dispatcher would write (issue #297), carrying the #388 identity.
        ``use`` seals the record over a PREFIX of the granules, so a later run
        over the whole set reads as an expansion."""
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.telemetry import build_record, raster_granule_ids, write_sidecar

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        first = granules if use is None else granules[:use]
        meta = process_and_write_raster_hive(shard, first, grid, root, cfg, store_kwargs={})
        leaf = hive.shard_leaf_path(root, shard)
        record = build_record(
            shard_key=int(shard),
            metadata={**meta, "total_obs": meta["timesteps"]},
            granule_ids=raster_granule_ids(first),
            run_id="r1",
            semantic_hash=meta["semantic_hash"],
        )
        write_sidecar(leaf, record)
        return cfg, grid, shard, granules, root, leaf

    def _counting(self, monkeypatch):
        """Count real ``process_raster_shard`` calls — the did-it-fold pin."""
        import zagg.processing.raster as raster_mod

        real = raster_mod.process_raster_shard
        calls: list = []

        def counting(*a, **k):
            calls.append(1)
            return real(*a, **k)

        monkeypatch.setattr(raster_mod, "process_raster_shard", counting)
        return calls

    @staticmethod
    def _boom(monkeypatch):
        import zagg.processing.raster as raster_mod

        def boom(*_a, **_k):
            raise AssertionError("sampling ran on a gated unit")

        monkeypatch.setattr(raster_mod, "process_raster_shard", boom)

    @staticmethod
    def _tree(root):
        import os as _os

        out = {}
        for dirpath, _dirs, files in _os.walk(root):
            for name in files:
                p = _os.path.join(dirpath, name)
                out[_os.path.relpath(p, root)] = _os.stat(p).st_mtime_ns
        return out

    @staticmethod
    def _contents(root):
        import os as _os

        out = {}
        for dirpath, _dirs, files in _os.walk(root):
            for name in files:
                p = _os.path.join(dirpath, name)
                with open(p, "rb") as fh:
                    out[_os.path.relpath(p, root)] = fh.read()
        return out

    @staticmethod
    def _age(root, epoch=10_000):
        import os as _os

        for dirpath, _dirs, files in _os.walk(root):
            for name in files:
                _os.utime(_os.path.join(dirpath, name), (epoch, epoch))
        return epoch * 10**9

    def test_skip_if_current_no_ops_and_writes_nothing(self, tmp_path, monkeypatch):
        import zagg.processing.raster as raster_mod
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        before = self._contents(root)
        aged_ns = self._age(root)

        def boom(*_a, **_k):
            raise AssertionError("sampling ran on a current unit")

        monkeypatch.setattr(raster_mod, "process_raster_shard", boom)
        skipped = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert skipped["current"] is True and skipped["identity"] == "equal"
        assert skipped["timesteps"] == 0 and skipped["leaf_written"] is False
        # The unit wrote NOTHING: same object set, every byte identical...
        assert self._contents(root) == before
        # ...and the lifecycle touch refreshed every object under the unit
        # (issue #388 phase 3): the purge clock resets on a skip.
        after = self._tree(root)
        assert set(after) == set(before)
        assert all(mtime > aged_ns for mtime in after.values())
        assert skipped["touched_objects"] == len(after) and skipped["touch_failed"] == 0
        # Conditional key (issue #495 phase 4): a touch that RAN carries no
        # not-applicable count, so this record is what it was before.
        assert "touch_skipped_paths" not in skipped

    def test_declared_touch_policy_reaches_the_skip_seam(self, tmp_path, monkeypatch):
        # Issue #501 END TO END through the raster seam: the config's
        # `output.touch` must govern the touch, not just the lifecycle unit
        # tests that call touch_current_unit directly. Delete the
        # `policy=get_touch_policy(config)` kwarg in process_and_write_raster_hive
        # and this fails (review finding on PR #496).
        import zagg.processing.raster as raster_mod
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        aged_ns = self._age(root)

        def boom(*_a, **_k):
            raise AssertionError("sampling ran on a current unit")

        monkeypatch.setattr(raster_mod, "process_raster_shard", boom)
        cfg.output["touch"] = "never"
        skipped = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert skipped["current"] is True
        assert skipped["touched_objects"] == 0 and skipped["touch_failed"] == 0
        assert skipped["touch_skipped_paths"] > 0
        assert all(mtime == aged_ns for mtime in self._tree(root).values())

    def test_a_published_skip_is_recorded_not_silently_zero(self, tmp_path, monkeypatch):
        # Mirror of the aggregation seam (issue #495 phase 4, review finding
        # on PR #496): touched_objects: 0 / touch_failed: 0 cannot be told
        # from a touch that never ran -- the comment at the bottom of
        # zagg/processing/raster.py names that exact ambiguity -- so the
        # not-applicable PATH count is recorded too.
        import zagg.lifecycle as lifecycle_mod
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        monkeypatch.setattr(
            lifecycle_mod,
            "touch_current_unit",
            lambda *_a, **_k: {"touched": 0, "failed": 0, "skipped_paths": 3},
        )
        skipped = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert skipped["current"] is True
        assert skipped["touched_objects"] == 0 and skipped["touch_failed"] == 0
        assert skipped["touch_skipped_paths"] == 3

    def test_contraction_refuses_without_the_flag(self, tmp_path, monkeypatch):
        import zagg.processing.raster as raster_mod
        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.telemetry import raster_granule_ids

        cfg, grid, shard, granules, root, leaf = self._committed_leaf_with_sidecar(tmp_path)

        def boom(*_a, **_k):
            raise AssertionError("sampling ran on a refused unit")

        monkeypatch.setattr(raster_mod, "process_raster_shard", boom)
        refused = process_and_write_raster_hive(
            shard, granules[:1], grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert refused["refused"] is True and refused["identity"] == "contraction"
        assert refused["missing_granules"] == [raster_granule_ids(granules)[1]]
        # The committed leaf is protected: still stamped complete — and NOT
        # touched (only a certified-current unit resets the purge clock).
        assert hive.read_commit(leaf)["complete"] is True
        assert "touched_objects" not in refused

    def test_allow_contraction_rewrites(self, tmp_path):
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, leaf = self._committed_leaf_with_sidecar(tmp_path)
        redo = process_and_write_raster_hive(
            shard,
            granules[:1],
            grid,
            root,
            cfg,
            store_kwargs={},
            skip_if_current=True,
            allow_contraction=True,
        )
        # A flagged contraction is a normal wholesale rewrite (D13 semantics).
        assert "refused" not in redo and redo["identity"] == "contraction"
        assert redo["timesteps"] == 1 and redo["leaf_written"] is True
        red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (1, grid.cells_per_shard)

    def test_seam_stamps_semantic_hash(self, tmp_path):
        # The raster seam stamps the D19 hash for the sidecar fallback too.
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.semantics import semantic_hash as semhash

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(shard, granules, grid, root, cfg, store_kwargs={})
        assert meta["semantic_hash"] == semhash(cfg)

    # The rest of the vector gate matrix (tests/test_hive.py::
    # TestLeafSkipIfCurrent), mirrored: the raster seam has its own gate call
    # site and its own early return, so every branch is pinned on both.

    def test_gate_is_off_by_default(self, tmp_path, monkeypatch):
        # The byte-identity pin for the deployed handler: without
        # skip_if_current the seam rewrites unconditionally, exactly as today.
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(shard, granules, grid, root, cfg, store_kwargs={})
        assert len(calls) == 1
        assert "current" not in meta and "identity" not in meta

    def test_no_sidecar_rewrites(self, tmp_path, monkeypatch):
        # The raster sidecar is written only when ``leaf_written``, so this
        # seam reaches ``no-sidecar`` over a strictly wider set of states than
        # the vector one (a unit with acquisitions but no occupied cell writes
        # no leaf, hence no record).
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert len(calls) == 1 and meta["identity"] == "no-sidecar"

    def test_unrecorded_ids_rewrites_with_its_own_classification(self, tmp_path, monkeypatch):
        # A leaf written before issue #388 has no granule-id sibling: the
        # guard is INERT, and the classification is what run stats count apart.
        import os

        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.telemetry import granule_ids_path

        cfg, grid, shard, granules, root, leaf = self._committed_leaf_with_sidecar(tmp_path)
        os.remove(granule_ids_path(leaf))
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(
            shard, granules[:1], grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert len(calls) == 1
        assert meta["identity"] == "unrecorded-ids" and "refused" not in meta

    def test_expansion_rewrites(self, tmp_path, monkeypatch):
        # A new acquisition: planned ⊇ recorded never trips the guard.
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path, use=1)
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert len(calls) == 1 and meta["identity"] == "expansion"
        assert "refused" not in meta and "current" not in meta

    def test_semantic_mismatch_rewrites(self, tmp_path, monkeypatch):
        # Same id set under a different semantic hash: rewrite, never refuse.
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(
            shard,
            granules,
            grid,
            root,
            cfg,
            store_kwargs={},
            skip_if_current=True,
            semantic_hash="f" * 64,
        )
        assert len(calls) == 1 and meta["identity"] == "semantic-mismatch"
        assert meta["semantic_hash"] == "f" * 64

    def test_mixed_add_and_drop_refuses(self, tmp_path, monkeypatch):
        # The ruled predicate is ``recorded ∖ planned ≠ ∅``, NOT strict-subset.
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.telemetry import raster_granule_ids

        cfg, grid, shard, granules, root, _leaf = self._committed_leaf_with_sidecar(tmp_path)
        fresh = _entry("g2", {"red": str(tmp_path / "h0.tif")}, T1, time_key="dt-3")
        self._boom(monkeypatch)
        meta = process_and_write_raster_hive(
            shard, [granules[0], fresh], grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert meta["refused"] is True and meta["identity"] == "mixed"
        assert meta["missing_granules"] == [raster_granule_ids(granules)[1]]

    def test_destroyed_leaf_with_surviving_sidecar_rebuilds(self, tmp_path, monkeypatch):
        # The sidecar is a SIBLING of the leaf, so a prefix-scoped lifecycle
        # purge leaves the record over an absent leaf. The D4 stamp is the
        # skip precondition on both seams (hive._leaf_is_committed).
        import shutil

        from zagg import hive
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, granules, root, leaf = self._committed_leaf_with_sidecar(tmp_path)
        shutil.rmtree(leaf)
        assert hive.read_commit(leaf) is None
        calls = self._counting(monkeypatch)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, skip_if_current=True
        )
        assert len(calls) == 1 and meta["identity"] == "unstamped-leaf"
        assert "current" not in meta
        assert hive.read_commit(leaf)["complete"] is True


class TestRasterHiveContentHashes:
    """Issue #342 phase 5: O11 hashes accumulated incrementally as slabs stream.

    The raster leaf never holds a whole band array, so the §5 digest is fed
    row by row at write time. The acceptance gate is parity with the
    canonical recipe: what the accumulator recorded must equal
    ``content_hash.hash_arrays`` recomputed over the leaf read back from the
    store.
    """

    def _setup(self, tmp_path, n=2):
        cfg, grid, shard = _healpix_setup(tmp_path)
        granules = []
        for i in range(n):
            _write_tiff(tmp_path / f"h{i}.tif", np.full((96, 96), 500 + i, dtype=np.uint16))
            granules.append(
                _entry(f"g{i}", {"red": str(tmp_path / f"h{i}.tif")}, T0, time_key=f"dt-{i}")
            )
        return cfg, grid, shard, granules, str(tmp_path / "hivestore")

    def test_streamed_hashes_match_a_full_read_back(self, tmp_path):
        # THE acceptance gate: incremental path == canonical recipe.
        import zarr

        from zagg import hive
        from zagg.content_hash import combined_hash, hash_arrays
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.store import open_store

        cfg, grid, shard, granules, root = self._setup(tmp_path, n=3)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, window=None
        )
        record = meta["content_hashes"]
        leaf = hive.shard_leaf_path(root, shard)
        group = zarr.open_group(open_store(leaf), mode="r", zarr_format=3)
        read_back = hash_arrays(group)
        assert record["arrays"] == read_back
        assert record["combined"] == combined_hash(read_back)
        # Discovery-based scope: the band plus both coordinates, nothing else.
        assert set(read_back) == {
            f"{grid.group_path}/red",
            f"{grid.group_path}/time",
            f"{grid.group_path}/morton",
        }
        assert meta["phase_timings"]["hash"] >= 0.0

    def test_windowed_leaf_records_hashes_in_its_sidecar_record(self, tmp_path):
        from zagg.processing.raster import process_and_write_raster_hive
        from zagg.telemetry import build_record

        cfg, grid, shard, granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(
            shard, granules, grid, root, cfg, store_kwargs={}, window={"label": "20260713"}
        )
        # The D20 record both dispatchers build carries it verbatim (the
        # raster sidecar seam needs no change of its own).
        record = build_record(shard_key=shard, metadata=meta, window="20260713")
        assert record["content_hashes"] == meta["content_hashes"]

    def test_a_unit_that_writes_no_leaf_records_nothing(self, tmp_path):
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, grid, shard, _granules, root = self._setup(tmp_path)
        meta = process_and_write_raster_hive(
            shard, [], grid, root, cfg, store_kwargs={}, window=None
        )
        assert "content_hashes" not in meta  # unverifiable, not tampered (§5.3)

    def test_a_violated_precondition_records_no_hash_at_all(self, tmp_path, caplog):
        # Trap (2) at the integration level: a row written twice invalidates
        # that array's digest, and the WHOLE record drops rather than
        # recording a partial map a verifier would read as "array missing".
        import logging

        from zagg.processing.raster import (
            _arm_leaf_hashes,
            _finalize_leaf_hashes,
            emit_raster_leaf_template,
            write_raster_leaf_slab,
        )
        from zagg.store import open_store

        cfg, grid, shard, _granules, root = self._setup(tmp_path)
        store = open_store(f"{root}/leaf.zarr")
        staged: dict = {}
        streams: dict = {}
        emit_raster_leaf_template(
            store, grid, cfg, shard, np.array([0, 1], dtype=np.int64), staged_out=staged
        )
        _arm_leaf_hashes(store, staged, streams)
        row = np.zeros(grid.cells_per_shard, dtype=np.uint16)
        write_raster_leaf_slab(store, grid, 0, {"red": row}, streams=streams)
        write_raster_leaf_slab(store, grid, 0, {"red": row}, streams=streams)  # same row twice
        with caplog.at_level(logging.WARNING):
            assert _finalize_leaf_hashes(staged, streams) is None
        assert "no content hashes recorded" in caplog.text
        assert "written more than once" in caplog.text


class TestRasterHivePopcount:
    """D14 popcount: interior shard stamps "full" (no sidecar), edge shard
    writes the real bitmap (issue #247 phase 5)."""

    def _setup(self, tmp_path):
        # Coarse shards (order 14, ~400 m) with 16 order-16 children (~100 m):
        # the 960 m raster fully covers interior shards and clips edge ones.
        from mortie import clip2order, geo2mort

        cfg = _raster_config(
            bands={"red": {"asset": "red", "dtype": "uint16"}},
            grid={"type": "healpix", "parent_order": 14, "child_order": 16},
        )
        data = np.full((96, 96), 555, dtype=np.uint16)  # no nodata anywhere
        _write_tiff(tmp_path / "full.tif", data)
        granules = [_entry("g0", {"red": str(tmp_path / "full.tif")}, T0, time_key="dt-1")]
        to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)

        def shard_at(dx, dy):
            lon, lat = to_wgs.transform(ORIGIN[0] + dx, ORIGIN[1] - dy)
            leaf = geo2mort(lat, lon, order=29, points=True)
            return int(clip2order(14, leaf)[0])

        return cfg, granules, shard_at, str(tmp_path / "store")

    def _coverage_counts(self, cfg, grid, shard):
        cells = grid.children(shard)
        _r, _c, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        return int(valid.sum()), grid.cells_per_shard

    def test_interior_full_no_sidecar_edge_bitmap(self, tmp_path):
        import os as _os

        from zagg import hive
        from zagg.grids import HealpixGrid
        from zagg.processing.raster import process_and_write_raster_hive

        cfg, granules, shard_at, root = self._setup(tmp_path)
        interior = shard_at(480.0, 480.0)  # raster center
        grid = HealpixGrid(14, 16, config=cfg)
        n_valid, n_cells = self._coverage_counts(cfg, grid, interior)
        assert n_valid == n_cells  # the fixture premise: fully covered shard

        process_and_write_raster_hive(
            interior, granules, grid, root, cfg, store_kwargs={}, window={"label": "20260713"}
        )
        leaf = hive.shard_leaf_path(root, interior, window="20260713")
        stamp = hive.read_commit(leaf)
        assert stamp["coverage"]["encoding"] == "full"
        assert "sidecar" not in stamp["coverage"]
        # Asserted via object listing: NO coverage.moc object in the leaf.
        assert "coverage.moc" not in _os.listdir(leaf)
        # And the reader short-circuits: no bitmap to fetch.
        assert hive.read_coverage_bitmap(leaf, coverage=stamp["coverage"]) is None

        # An edge-of-scene shard: hunt along the raster's top edge for one the
        # scene only clips (the frozen fixture geometry guarantees several).
        edge = None
        for dx in range(0, 960, 60):
            cand = shard_at(float(dx), 20.0)
            g = HealpixGrid(14, 16, config=cfg)
            nv, nc = self._coverage_counts(cfg, g, cand)
            if 0 < nv < nc:
                edge, egrid, e_valid = cand, g, nv
                break
        assert edge is not None, "fixture drift: no partially-covered shard found"

        process_and_write_raster_hive(
            edge, granules, egrid, root, cfg, store_kwargs={}, window={"label": "20260713"}
        )
        eleaf = hive.shard_leaf_path(root, edge, window="20260713")
        estamp = hive.read_commit(eleaf)
        assert estamp["coverage"]["encoding"] == "bitmap"
        assert estamp["cells_with_data"] == e_valid
        # Asserted via object listing: the real bitmap sidecar object exists.
        assert "coverage.moc" in _os.listdir(eleaf)
        got = hive.read_coverage_bitmap(eleaf, coverage=estamp["coverage"])
        assert got is not None and got.size == e_valid
