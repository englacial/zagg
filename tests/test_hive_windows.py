"""Time-windowed morton-hive stores (morton-hive/2) — issue #246.

Covers the ``output.windowing`` config block (phase 2), the manifest temporal
block + spec bump, windowed leaf naming/walking (phase 3), the stamp/coverage
extensions (phase 4), and the dispatch fan-out (phase 5). The primitives
themselves are pinned in ``tests/test_windows.py``; the frozen grammar lives
on the mortie spec page (mortie#62).
"""

import pytest

from zagg import hive
from zagg.config import default_config, get_windowing, validate_config
from zagg.grids import HealpixGrid


@pytest.fixture
def cfg():
    c = default_config("atl06")
    # The membership timestamp column must be a declared read column.
    c.data_source["variables"]["delta_time"] = "/{group}/land_ice_segments/delta_time"
    return c


def _windowed(cfg, schedule="yearly", **over):
    cfg.output["store_layout"] = "hive"
    block = {
        "schedule": schedule,
        "time_field": "delta_time",
        # ICESat-2 ATLAS SDP epoch: GPS seconds since 2018-01-01T00:00:00Z.
        "epoch": "2018-01-01T00:00:00Z",
        "scale": "gps",
    }
    if schedule == "explicit":
        block["windows"] = [
            {"label": "melt-2019", "start": "2019-06-01", "end": "2019-09-01"},
            {"label": "melt-2020", "start": "2020-06-01", "end": "2020-09-01"},
        ]
    block.update(over)
    cfg.output["windowing"] = block
    return cfg


# ── config block (phase 2) ───────────────────────────────────────────────────


class TestWindowingConfig:
    def test_absent_is_none(self, cfg):
        assert get_windowing(cfg) is None
        validate_config(cfg)

    def test_null_block_is_none(self, cfg):
        cfg.output["windowing"] = None
        assert get_windowing(cfg) is None
        validate_config(cfg)

    def test_schedule_none_is_inert(self, cfg):
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "none"}
        validate_config(cfg)
        assert get_windowing(cfg) is None

    def test_yearly_normalized(self, cfg):
        _windowed(cfg)
        validate_config(cfg)
        got = get_windowing(cfg)
        assert got == {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00+00:00",
            "scale": "gps",
            "units": "seconds",
            "windows": None,
        }

    def test_explicit_normalized(self, cfg):
        _windowed(cfg, schedule="explicit")
        validate_config(cfg)
        got = get_windowing(cfg)
        assert got["windows"] == [
            {
                "label": "melt-2019",
                "start": "2019-06-01T00:00:00+00:00",
                "end": "2019-09-01T00:00:00+00:00",
            },
            {
                "label": "melt-2020",
                "start": "2020-06-01T00:00:00+00:00",
                "end": "2020-09-01T00:00:00+00:00",
            },
        ]

    def test_non_mapping_rejected(self, cfg):
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = "yearly"
        with pytest.raises(ValueError, match="mapping"):
            validate_config(cfg)

    def test_quarterly_reserved(self, cfg):
        _windowed(cfg, schedule="quarterly")
        with pytest.raises(ValueError, match="reserved"):
            validate_config(cfg)

    def test_unknown_schedule(self, cfg):
        _windowed(cfg, schedule="weekly")
        with pytest.raises(ValueError, match="unknown window schedule"):
            validate_config(cfg)

    def test_requires_hive_layout(self, cfg):
        _windowed(cfg)
        cfg.output["store_layout"] = "flat"
        with pytest.raises(ValueError, match="store_layout: hive"):
            validate_config(cfg)

    def test_requires_healpix(self, cfg):
        _windowed(cfg)
        cfg.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        cfg.output["store_layout"] = "hive"
        with pytest.raises(ValueError, match="healpix"):
            validate_config(cfg)

    def test_requires_time_field(self, cfg):
        _windowed(cfg, time_field=None)
        with pytest.raises(ValueError, match="time_field"):
            validate_config(cfg)

    def test_time_field_must_be_declared(self, cfg):
        _windowed(cfg, time_field="not_a_column")
        with pytest.raises(ValueError, match="declared data_source column"):
            validate_config(cfg)

    def test_requires_epoch(self, cfg):
        _windowed(cfg, epoch=None)
        with pytest.raises(ValueError, match="epoch"):
            validate_config(cfg)

    def test_bad_epoch_rejected(self, cfg):
        _windowed(cfg, epoch="the beginning")
        with pytest.raises(ValueError, match="ISO-8601"):
            validate_config(cfg)

    def test_bad_scale_and_units(self, cfg):
        _windowed(cfg, scale="tt")
        with pytest.raises(ValueError, match="scale"):
            validate_config(cfg)
        _windowed(cfg, scale="gps", units="fortnights")
        with pytest.raises(ValueError, match="units"):
            validate_config(cfg)

    def test_windows_only_for_explicit(self, cfg):
        _windowed(cfg, windows=[{"label": "x", "start": "2019-01-01", "end": "2020-01-01"}])
        with pytest.raises(ValueError, match="schedule: explicit"):
            validate_config(cfg)

    def test_explicit_requires_windows(self, cfg):
        _windowed(cfg, schedule="explicit", windows=None)
        with pytest.raises(ValueError, match="non-empty list"):
            validate_config(cfg)

    def test_explicit_bad_label(self, cfg):
        _windowed(
            cfg,
            schedule="explicit",
            windows=[{"label": "melt_2019", "start": "2019-06-01", "end": "2019-09-01"}],
        )
        with pytest.raises(ValueError, match="grammar"):
            validate_config(cfg)

    def test_explicit_reversed_range(self, cfg):
        _windowed(
            cfg,
            schedule="explicit",
            windows=[{"label": "w", "start": "2019-09-01", "end": "2019-06-01"}],
        )
        with pytest.raises(ValueError, match="half-open"):
            validate_config(cfg)

    def test_explicit_duplicate_label(self, cfg):
        _windowed(
            cfg,
            schedule="explicit",
            windows=[
                {"label": "w", "start": "2019-01-01", "end": "2019-02-01"},
                {"label": "w", "start": "2019-03-01", "end": "2019-04-01"},
            ],
        )
        with pytest.raises(ValueError, match="twice"):
            validate_config(cfg)

    def test_explicit_overlap(self, cfg):
        _windowed(
            cfg,
            schedule="explicit",
            windows=[
                {"label": "a", "start": "2019-01-01", "end": "2019-03-01"},
                {"label": "b", "start": "2019-02-01", "end": "2019-04-01"},
            ],
        )
        with pytest.raises(ValueError, match="overlap"):
            validate_config(cfg)

    def test_explicit_touching_ranges_allowed(self, cfg):
        # Half-open ranges may share a boundary instant without overlapping.
        _windowed(
            cfg,
            schedule="explicit",
            windows=[
                {"label": "a", "start": "2019-01-01", "end": "2019-02-01"},
                {"label": "b", "start": "2019-02-01", "end": "2019-03-01"},
            ],
        )
        validate_config(cfg)

    def test_raster_rejects_windowing_pointing_at_247(self):
        c = default_config("atl06")
        c.data_source = {
            "reader": "raster",
            "bands": {"red": {"asset": "red", "dtype": "uint16"}},
        }
        c.aggregation = {}
        c.output["grid"] = {"type": "healpix", "parent_order": 6, "child_order": 12}
        c.output["windowing"] = {"schedule": "yearly"}
        with pytest.raises(ValueError, match="issue #247"):
            validate_config(c)


# ── manifest temporal block + spec bump (phase 2) ────────────────────────────


class TestManifestTemporal:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_unwindowed_manifest_is_v1_and_unchanged(self, cfg):
        m = hive.build_manifest(self._grid(cfg), dataset={"short_name": "ATL06", "version": "007"})
        assert m["spec"] == "morton-hive/1"
        assert "temporal" not in m
        # The exact pre-#246 key set: no new keys leak into unwindowed stores.
        assert set(m) == {
            "spec",
            "dataset",
            "cell_order",
            "shard_order",
            "split_schedule",
            "pyramid",
            "generated_at",
        }

    def test_yearly_manifest_declares_v2_temporal(self, cfg):
        _windowed(cfg)
        m = hive.build_manifest(self._grid(cfg), windowing=get_windowing(cfg))
        assert m["spec"] == "morton-hive/2"
        assert m["temporal"] == {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00+00:00",
            "scale": "gps",
            "units": "seconds",
            "append_policy": "new-window",
        }

    def test_explicit_manifest_carries_windows_and_retemplate_policy(self, cfg):
        _windowed(cfg, schedule="explicit")
        m = hive.build_manifest(self._grid(cfg), windowing=get_windowing(cfg))
        assert m["spec"] == "morton-hive/2"
        assert m["temporal"]["append_policy"] == "re-template"
        assert [w["label"] for w in m["temporal"]["windows"]] == ["melt-2019", "melt-2020"]

    def test_windowed_rerun_resumes(self, cfg, tmp_path):
        _windowed(cfg)
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        again = hive.build_manifest(grid, windowing=get_windowing(cfg))
        assert hive.ensure_manifest(root, again)["spec"] == "morton-hive/2"

    def test_adding_windowing_to_v1_store_refuses(self, cfg, tmp_path):
        # A windowing change re-partitions leaf NAMES — it must refuse resume
        # exactly like an orders change (frozen-key posture).
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        hive.ensure_manifest(root, hive.build_manifest(grid))
        _windowed(cfg)
        with pytest.raises(ValueError, match="clear the store root"):
            hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))

    def test_changing_schedule_refuses(self, cfg, tmp_path):
        _windowed(cfg)
        root = str(tmp_path / "store")
        grid = self._grid(cfg)
        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        cfg.output["windowing"]["schedule"] = "monthly"
        with pytest.raises(ValueError, match="clear the store root"):
            hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))

    def test_both_specs_read_back(self, cfg, tmp_path):
        grid = self._grid(cfg)
        hive.ensure_manifest(str(tmp_path / "v1"), hive.build_manifest(grid))
        assert hive.read_manifest(str(tmp_path / "v1"))["spec"] == "morton-hive/1"
        _windowed(cfg)
        hive.ensure_manifest(
            str(tmp_path / "v2"), hive.build_manifest(grid, windowing=get_windowing(cfg))
        )
        m = hive.read_manifest(str(tmp_path / "v2"))
        assert m["spec"] == "morton-hive/2"
        assert m["temporal"]["schedule"] == "yearly"
