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

    def test_schedule_none_inert_on_flat_layout(self, cfg):
        # A ``schedule: none`` block is equivalent to an absent block, so it
        # must validate on a flat/non-healpix store — the layout guards run
        # only for a live (generative) schedule.
        cfg.output["windowing"] = {"schedule": "none"}
        validate_config(cfg)
        assert get_windowing(cfg) is None

    def test_schedule_none_rejects_stray_windows(self, cfg):
        # Validation symmetry: a stray ``windows`` list is rejected under
        # ``schedule: none`` just as it is under a generative schedule.
        cfg.output["windowing"] = {
            "schedule": "none",
            "windows": [{"label": "x", "start": "2019-01-01", "end": "2020-01-01"}],
        }
        with pytest.raises(ValueError, match="schedule: explicit"):
            validate_config(cfg)

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
        with pytest.raises(ValueError, match="base-rate data_source column"):
            validate_config(cfg)

    def test_time_field_rejected_when_no_columns_declared(self, cfg):
        # An empty read set is itself rejected: a store reading no columns
        # cannot filter on ``time_field`` (the check is unconditional).
        _windowed(cfg)
        cfg.data_source["coordinates"] = {}
        cfg.data_source["variables"] = {}
        with pytest.raises(ValueError, match="base-rate data_source column"):
            validate_config(cfg)

    def test_time_field_coordinate_rejected(self, cfg):
        # A coordinate ``time_field`` is rejected: the stamp time_range is
        # pooled from read VARIABLE columns, never coordinates (a lat/lon
        # coordinate is not a timestamp), so it would filter yet silently drop
        # the stamp. It must be declared as a variable.
        _windowed(cfg, time_field="latitude")
        with pytest.raises(ValueError, match="coordinate"):
            validate_config(cfg)

    def test_time_field_from_non_base_level_rejected(self, cfg):
        # A segment-rate (non-base) level column is rejected this round: window
        # membership would be decided per whole segment, not per observation,
        # so the per-observation split process_and_write_hive promises fails.
        _windowed(cfg, time_field="seg_time")
        cfg.data_source["base_level"] = "photons"
        cfg.data_source["levels"] = {
            "photons": {
                "path": "/{group}/heights",
                "coordinates": ["lat_ph", "lon_ph"],
                "variables": ["h_ph"],
            },
            "segments": {
                "path": "/{group}/geolocation",
                "coordinates": ["reference_photon_lat"],
                "variables": {"seg_time": "/{group}/geolocation/delta_time"},
                "link": {
                    "to": "photons",
                    "index_beg": "/{group}/geolocation/ph_index_beg",
                    "count": "/{group}/geolocation/segment_ph_cnt",
                },
            },
        }
        with pytest.raises(ValueError, match="segment-rate window membership"):
            validate_config(cfg)

    def test_time_field_base_rate_accepted_in_hierarchical_config(self, cfg):
        # In a hierarchical (levels) config the base level reads its columns
        # from ``data_source.variables`` (a base-level ``variables`` mapping is
        # forbidden). A base-rate ``time_field`` declared there filters at base
        # rate (level None) — per-observation membership — so it is accepted
        # even alongside a segment-level link.
        _windowed(cfg, time_field="delta_time")
        cfg.data_source["base_level"] = "photons"
        cfg.data_source["levels"] = {
            "photons": {
                "path": "/{group}/heights",
                "coordinates": ["lat_ph", "lon_ph"],
                "variables": ["h_ph"],
            },
            "segments": {
                "path": "/{group}/geolocation",
                "coordinates": ["reference_photon_lat"],
                "variables": {"seg_h": "/{group}/geolocation/h"},
                "link": {
                    "to": "photons",
                    "index_beg": "/{group}/geolocation/ph_index_beg",
                    "count": "/{group}/geolocation/segment_ph_cnt",
                },
            },
        }
        validate_config(cfg)
        assert get_windowing(cfg)["time_field"] == "delta_time"

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

    def test_raster_windowing_validates_on_hive(self):
        # Issue #247: raster + hive + windowing is legal; membership is the
        # acquisition's STAC datetime, so no time_field is required. The full
        # raster windowing matrix lives in test_raster_pipeline.py.
        c = default_config("atl06")
        c.data_source = {
            "reader": "raster",
            "bands": {"red": {"asset": "red", "dtype": "uint16"}},
        }
        c.aggregation = {}
        c.output["grid"] = {"type": "healpix", "parent_order": 6, "child_order": 12}
        c.output["store_layout"] = "hive"
        c.output["windowing"] = {"schedule": "yearly"}
        validate_config(c)
        assert get_windowing(c)["time_field"] == "datetime"


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
            "calendar": "proleptic_gregorian",
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


# ── windowed leaf paths + node invariant + walker (phase 3) ──────────────────


def _shard_word(order=6):
    """A real southern packed shard word (decimal form ``-5112333`` at order 6)."""
    import numpy as np
    from mortie import geo2mort

    return int(geo2mort(np.array([-78.5]), np.array([-132.0]), order=order)[0])


class TestWindowedLeafPath:
    def test_windowed_leaf_at_the_shard_node(self):
        # D13: the windowed leaf sits at the SAME digit node as the bare leaf,
        # basename `{full_id}_{window}.zarr` (frozen naming, mortie#62).
        word = _shard_word()
        assert (
            hive.shard_leaf_path("root", word, window="2025")
            == "root/-5/1/1/2/3/3/3/-5112333_2025.zarr"
        )
        assert (
            hive.shard_leaf_path("root", word, window="melt-2019")
            == "root/-5/1/1/2/3/3/3/-5112333_melt-2019.zarr"
        )

    def test_bare_path_byte_identical(self):
        word = _shard_word()
        assert hive.shard_leaf_path("root", word) == hive.shard_leaf_path("root", word, window=None)

    def test_bad_window_label_raises(self):
        with pytest.raises(ValueError, match="grammar"):
            hive.shard_leaf_path("root", _shard_word(), window="melt_2019")


class TestWindowedNodeInvariant:
    @pytest.mark.parametrize(
        "ok",
        [
            "-5/1/1/2/3/3/3/-5112333_2025.zarr",
            "-4/2/-42_melt-2019.zarr",
            "-4/2/-42_20251103.zarr",
            "-4/2/-42.zarr",  # bare stays legal
        ],
    )
    def test_accepts(self, ok):
        hive.check_node_invariant(ok)

    @pytest.mark.parametrize(
        "bad",
        [
            "-4/2/-43_2025.zarr",  # leaf id does not match the digit chain
            "-4/2/-42_mel_t.zarr",  # `_` inside the window label
            "-4/2/-42_.zarr",  # empty window label
            "-4/2/-42_" + "a" * 33 + ".zarr",  # label too long
        ],
    )
    def test_rejects(self, bad):
        with pytest.raises(ValueError, match="node invariant"):
            hive.check_node_invariant(bad)


class TestWindowedWalkerAndSidecar:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def _windowed_store(self, cfg, tmp_path, labels, debris=()):
        from zagg.store import open_store

        _windowed(cfg)
        grid = self._grid(cfg)
        root = str(tmp_path / "store")
        from zagg.config import get_windowing

        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        word = _shard_word()
        for label in (*labels, *debris):
            leaf = hive.shard_leaf_path(root, word, window=label)
            store = open_store(leaf)
            grid.emit_shard_template(store, overwrite=True)
            if label not in debris:
                hive.stamp_commit(store, cells_with_data=1, granule_count=1)
        return root, word, grid

    def test_refresh_walks_windowed_leaves_and_dedupes(self, cfg, tmp_path):
        import numpy as np

        from zagg.coverage import refresh_root_coverage

        # Two stamped windows + one debris window of the SAME shard: the root
        # MOC is spatial, so the shard is listed exactly once.
        root, word, _grid = self._windowed_store(
            cfg, tmp_path, labels=("2024", "2025"), debris=("2026",)
        )
        env = refresh_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(env), np.asarray([word], dtype=np.uint64)
        )

    def test_refresh_skips_malformed_window_label(self, cfg, tmp_path, caplog):
        import logging

        from zagg.coverage import refresh_root_coverage
        from zagg.store import open_store

        root, word, grid = self._windowed_store(cfg, tmp_path, labels=("2024",))
        node = tmp_path / "store" / "-5" / "1" / "1" / "2" / "3" / "3" / "3"
        # (a) An UNSTAMPED leaf whose name breaks the frozen charset is ordinary
        # D4 debris: read_commit returns None first, so it is dropped SILENTLY,
        # no noisier than a valid-named unstamped leaf or the foreign-order
        # carve-out — only real data earns a warning.
        grid.emit_shard_template(open_store(str(node / "-5112333_bad_label.zarr")), overwrite=True)
        with caplog.at_level(logging.WARNING, logger="zagg.coverage"):
            env = refresh_root_coverage(root)
        assert env is not None and len(env["ranges"]) == 1
        assert not any("malformed window label" in r.message for r in caplog.records)
        # (b) A STAMPED leaf with the same malformed name is real (misnamed)
        # data: the walk warns and skips it instead of dying (escape-hatch
        # posture), and the conforming leaf still carries the coverage.
        stamped = open_store(str(node / "-5112333_bad_label2.zarr"))
        grid.emit_shard_template(stamped, overwrite=True)
        hive.stamp_commit(stamped, cells_with_data=1, granule_count=1)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="zagg.coverage"):
            env = refresh_root_coverage(root)
        assert env is not None and len(env["ranges"]) == 1
        assert any("malformed window label" in r.message for r in caplog.records)

    def test_refresh_trusts_basename_id_over_path_node(self, cfg, tmp_path):
        # The walker keys off `split_leaf_name(name)[0]`, never re-checking
        # `check_node_invariant`, so a stamped windowed leaf parked at the WRONG
        # digit node is listed at its basename id, not the path's. This mirrors
        # pre-#246 behavior for bare leaves (the walk always trusted the basename
        # over the path); pinning it as the intended contract, not a regression.
        import numpy as np

        from zagg.config import get_windowing
        from zagg.coverage import refresh_root_coverage
        from zagg.store import open_store

        _windowed(cfg)
        grid = self._grid(cfg)
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        word = _shard_word()  # basename id -5112333, correct node .../3/3/3
        # Same basename, planted one digit off (.../3/3/4): the walker descends
        # any valid digit node and trusts the basename it finds.
        wrong = open_store(f"{root}/-5/1/1/2/3/3/4/-5112333_2025.zarr")
        grid.emit_shard_template(wrong, overwrite=True)
        hive.stamp_commit(wrong, cells_with_data=1, granule_count=1, window="2025")
        env = refresh_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(env), np.asarray([word], dtype=np.uint64)
        )

    def test_refresh_dedupes_bare_and_windowed_siblings(self, cfg, tmp_path):
        # The spec forbids mixing a bare and a windowed leaf of one id in a
        # single store (WRITE side), but the walk must survive it: both stamp to
        # the same shard word, so `build_root_coverage`'s np.unique dedupe lists
        # the shard exactly once (read-side dedupe across the two name shapes).
        import numpy as np

        from zagg.config import get_windowing
        from zagg.coverage import refresh_root_coverage
        from zagg.store import open_store

        _windowed(cfg)
        grid = self._grid(cfg)
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        word = _shard_word()
        for label in (None, "2025"):
            store = open_store(hive.shard_leaf_path(root, word, window=label))
            grid.emit_shard_template(store, overwrite=True)
            hive.stamp_commit(
                store, cells_with_data=1, granule_count=1, **({"window": label} if label else {})
            )
        env = refresh_root_coverage(root)
        np.testing.assert_array_equal(
            hive.root_coverage_words(env), np.asarray([word], dtype=np.uint64)
        )

    def test_bitmap_sidecar_round_trip_on_windowed_leaf(self, cfg, tmp_path):
        import numpy as np

        from zagg.store import open_store

        root, word, grid = self._windowed_store(cfg, tmp_path, labels=())
        leaf = hive.shard_leaf_path(root, word, window="2025")
        store = open_store(leaf)
        grid.emit_shard_template(store, overwrite=True)
        occupied = np.sort(np.asarray(grid.children(word)[:3], dtype=np.uint64))
        bitmap = hive.encode_coverage_bitmap(word, occupied, grid.child_order)
        hive.write_coverage_sidecar(leaf, bitmap)
        hive.stamp_commit(
            store,
            cells_with_data=3,
            granule_count=1,
            coverage=hive.build_coverage(word, occupied, grid.child_order, bitmap=bitmap),
        )
        # The shard id parses out of the WINDOWED basename (first-`_` split).
        np.testing.assert_array_equal(hive.read_coverage_bitmap(leaf), occupied)


# ── windowed stamps + coverage "full" + root time union (phase 4) ────────────


class TestWindowedStamp:
    def _leaf(self, cfg):
        from zarr.storage import MemoryStore

        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_shard_template(store, overwrite=True)
        return store

    def test_windowed_stamp_round_trip(self, cfg):
        store = self._leaf(cfg)
        hive.stamp_commit(
            store,
            cells_with_data=5,
            granule_count=2,
            window="2025",
            time_range=["2025-03-01T06:00:00+00:00", "2025-11-20T18:30:00+00:00"],
        )
        stamp = hive.read_commit(store)
        # D15 truth half: the stamp carries the window label + the ACTUAL
        # written range as ISO-8601 UTC strings (ratified #246 Q2), spec /2.
        assert stamp["spec"] == "morton-hive/2"
        assert stamp["window"] == "2025"
        assert stamp["time_range"] == ["2025-03-01T06:00:00+00:00", "2025-11-20T18:30:00+00:00"]

    def test_unwindowed_stamp_unchanged(self, cfg):
        store = self._leaf(cfg)
        hive.stamp_commit(store, cells_with_data=5, granule_count=2)
        stamp = hive.read_commit(store)
        assert stamp["spec"] == "morton-hive/1"
        # Pin the exact pre-#246 key set: no new keys leak into unwindowed stamps.
        assert set(stamp) == {
            "spec",
            "complete",
            "cells_with_data",
            "granule_count",
            "written_at",
        }

    def test_time_range_requires_window(self, cfg):
        store = self._leaf(cfg)
        with pytest.raises(ValueError, match="windowed stamps only"):
            hive.stamp_commit(
                store,
                cells_with_data=1,
                granule_count=1,
                time_range=["2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00"],
            )

    def test_reversed_time_range_rejected(self, cfg):
        # D15 truth half fails CLOSED: a [t_max, t_min] pair never becomes
        # durable truth (review finding, PR #248).
        store = self._leaf(cfg)
        with pytest.raises(ValueError, match="reversed"):
            hive.stamp_commit(
                store,
                cells_with_data=1,
                granule_count=1,
                window="2025",
                time_range=["2025-11-20T18:30:00+00:00", "2025-03-01T06:00:00+00:00"],
            )

    def test_wrong_length_time_range_rejected(self, cfg):
        store = self._leaf(cfg)
        for bad in (["2025-01-01T00:00:00+00:00"], ["a", "b", "c"]):
            with pytest.raises(ValueError, match="2-sequence"):
                hive.stamp_commit(
                    store, cells_with_data=1, granule_count=1, window="2025", time_range=bad
                )

    def test_garbage_time_range_rejected(self, cfg):
        store = self._leaf(cfg)
        with pytest.raises(ValueError, match="2-sequence"):
            hive.stamp_commit(
                store,
                cells_with_data=1,
                granule_count=1,
                window="2025",
                time_range=["not-a-date", "also-not"],
            )


class TestCoverageFull:
    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def test_full_envelope_shape(self, cfg):
        import numpy as np

        word = _shard_word()
        occupied = np.asarray(self._grid(cfg).children(word), dtype=np.uint64)
        cov = hive.build_coverage(word, occupied, 8, full=True)
        assert cov["encoding"] == "full"
        # No sidecar is written or pointed to (D14): the pointer keys are absent.
        assert "sidecar" not in cov and "nbytes" not in cov and "raw_nbytes" not in cov
        # The tier-0 box is carried unconditionally — full occupancy collapses
        # it to the shard's own id (the trivial 1-member cover).
        from zagg.grids.morton import morton_decimal

        assert cov["box"][0] == morton_decimal(word)

    def test_full_and_bitmap_mutually_exclusive(self, cfg):
        word = _shard_word()
        with pytest.raises(ValueError, match="mutually exclusive"):
            hive.build_coverage(word, None, 8, bitmap=b"x", full=True)

    def _stamped_leaf(self, cfg, tmp_path, *, full):
        import numpy as np

        from zagg.store import open_store

        grid = self._grid(cfg)
        word = _shard_word()
        root = str(tmp_path / "store")
        leaf = hive.shard_leaf_path(root, word, window="2025")
        store = open_store(leaf)
        grid.emit_shard_template(store, overwrite=True)
        if full:
            occupied = np.asarray(grid.children(word), dtype=np.uint64)
            cov = hive.build_coverage(word, occupied, grid.child_order, full=True)
        else:
            occupied = np.sort(np.asarray(grid.children(word)[:3], dtype=np.uint64))
            bitmap = hive.encode_coverage_bitmap(word, occupied, grid.child_order)
            hive.write_coverage_sidecar(leaf, bitmap)
            cov = hive.build_coverage(word, occupied, grid.child_order, bitmap=bitmap)
        hive.stamp_commit(
            store, cells_with_data=len(occupied), granule_count=1, window="2025", coverage=cov
        )
        return grid, word, leaf, occupied

    def test_full_leaf_writes_no_sidecar_and_short_circuits(self, cfg, tmp_path):
        import os

        import numpy as np

        from zagg.coverage import bitmap_and

        grid, word, leaf, _occ = self._stamped_leaf(cfg, tmp_path, full=True)
        # The whole point of "full": NO sidecar object exists in the leaf.
        assert not os.path.exists(os.path.join(leaf, hive.COVERAGE_SIDECAR))
        assert hive.read_coverage_bitmap(leaf) is None  # nothing to GET
        # bitmap_and short-circuits via the shard's own MOC membership: an AOI
        # of two child cells intersects exactly those cells.
        aoi = np.asarray(grid.children(word)[5:7], dtype=np.uint64)
        np.testing.assert_array_equal(np.sort(bitmap_and(leaf, aoi)), np.sort(aoi))

    def test_partial_leaf_keeps_the_bitmap_path(self, cfg, tmp_path):
        import numpy as np

        from zagg.coverage import bitmap_and

        grid, word, leaf, occupied = self._stamped_leaf(cfg, tmp_path, full=False)
        children = np.asarray(grid.children(word), dtype=np.uint64)
        aoi = children[1:5]  # overlaps occupied[1:3] only
        got = bitmap_and(leaf, aoi)
        np.testing.assert_array_equal(np.sort(got), np.sort(np.intersect1d(occupied, aoi)))
        # A miss is definitive (exact encoding).
        assert bitmap_and(leaf, children[5:7]).size == 0

    def _full_bitmap_leaf(self, cfg, tmp_path):
        """A leaf with ALL children occupied but encoded as a BITMAP (not
        "full") — the representation the "full" short-circuit claims
        moc_and-equivalence to."""
        import numpy as np

        from zagg.store import open_store

        grid = self._grid(cfg)
        word = _shard_word()
        leaf = hive.shard_leaf_path(str(tmp_path / "bmp"), word, window="2025")
        store = open_store(leaf)
        grid.emit_shard_template(store, overwrite=True)
        occupied = np.sort(np.asarray(grid.children(word), dtype=np.uint64))
        bitmap = hive.encode_coverage_bitmap(word, occupied, grid.child_order)
        hive.write_coverage_sidecar(leaf, bitmap)
        cov = hive.build_coverage(word, occupied, grid.child_order, bitmap=bitmap)
        hive.stamp_commit(
            store, cells_with_data=len(occupied), granule_count=1, window="2025", coverage=cov
        )
        return leaf

    def test_full_short_circuit_matches_bitmap_for_coarse_aoi(self, cfg, tmp_path):
        # The "full" short-circuit is `moc_and([shard], aoi)`; the bitmap path
        # is `moc_and(all_children_at_cell_order, aoi)`. Their equality rests on
        # moc_and COMPACTING fine children up to a coarser AOI cell. Every other
        # "full" test here uses child-order AOIs where both trivially agree and
        # no normalization runs; here the AOI is the shard's own order-6 word —
        # COARSER than cell_order 8 — so the bitmap path must compact its 4^2
        # children up to match the short-circuit. A future moc_and that stopped
        # compacting would break this (review finding, PR #248).
        import numpy as np

        from zagg.coverage import bitmap_and

        _grid, word, full_leaf, _occ = self._stamped_leaf(cfg, tmp_path, full=True)
        bmp_leaf = self._full_bitmap_leaf(cfg, tmp_path)
        aoi = np.asarray([word], dtype=np.uint64)  # coarser than cell_order
        full_res, bmp_res = bitmap_and(full_leaf, aoi), bitmap_and(bmp_leaf, aoi)
        assert full_res is not None and bmp_res is not None
        np.testing.assert_array_equal(np.sort(full_res), np.sort(bmp_res))

    def test_box_only_stamp_degrades_to_none(self, cfg, tmp_path):
        import numpy as np

        from zagg.coverage import bitmap_and
        from zagg.store import open_store

        grid = self._grid(cfg)
        word = _shard_word()
        leaf = hive.shard_leaf_path(str(tmp_path / "store"), word)
        store = open_store(leaf)
        grid.emit_shard_template(store, overwrite=True)
        occupied = np.asarray(grid.children(word)[:2], dtype=np.uint64)
        hive.stamp_commit(
            store,
            cells_with_data=2,
            granule_count=1,
            coverage=hive.build_coverage(word, occupied, grid.child_order),
        )
        assert bitmap_and(leaf, occupied) is None  # box-only: fall back to box


class TestRootTimeUnion:
    def test_union_time_range(self):
        assert hive.union_time_range(None, None) is None
        got = hive.union_time_range(
            ["2024-06-01T00:00:00+00:00", "2024-09-01T00:00:00+00:00"],
            None,
            ["2024-01-01T00:00:00+00:00", "2024-07-01T00:00:00+00:00"],
        )
        assert got == ["2024-01-01T00:00:00+00:00", "2024-09-01T00:00:00+00:00"]

    def test_union_drops_malformed_with_warning(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="zagg.windows"):
            got = hive.union_time_range(
                ["garbage", "2024-01-01"],
                ["2024-06-01T00:00:00+00:00", "2024-07-01T00:00:00+00:00"],
            )
        assert got == ["2024-06-01T00:00:00+00:00", "2024-07-01T00:00:00+00:00"]
        assert any("malformed time_range" in r.message for r in caplog.records)

    def test_envelope_carries_time_range_only_when_given(self):
        import numpy as np

        word = _shard_word()
        keys = np.asarray([word], dtype=np.uint64)
        assert "time_range" not in hive.build_root_coverage(keys, 6)
        env = hive.build_root_coverage(
            keys, 6, time_range=["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]
        )
        assert env["time_range"] == ["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]

    def test_write_root_coverage_unions_time_ranges(self, tmp_path):
        import numpy as np

        root = str(tmp_path / "store")
        word = _shard_word()
        keys = np.asarray([word], dtype=np.uint64)
        hive.write_root_coverage(
            root,
            hive.build_root_coverage(
                keys, 6, time_range=["2024-01-01T00:00:00+00:00", "2024-06-01T00:00:00+00:00"]
            ),
        )
        merged = hive.write_root_coverage(
            root,
            hive.build_root_coverage(
                keys, 6, time_range=["2024-03-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]
            ),
        )
        assert merged["time_range"] == ["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]
        # One-sided: a later run without a range keeps the accumulated union.
        merged = hive.write_root_coverage(root, hive.build_root_coverage(keys, 6))
        assert merged["time_range"] == ["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]

    def test_load_coverage_tolerates_time_range(self, tmp_path):
        import numpy as np

        from zagg.coverage import load_coverage

        root = str(tmp_path / "store")
        keys = np.asarray([_shard_word()], dtype=np.uint64)
        hive.write_root_coverage(
            root,
            hive.build_root_coverage(
                keys, 6, time_range=["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]
            ),
        )
        env = load_coverage(root)
        assert env is not None
        assert env["time_range"] == ["2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"]

    def test_refresh_rebuilds_time_union_from_stamps(self, cfg, tmp_path):
        from zagg.coverage import refresh_root_coverage
        from zagg.store import open_store

        _windowed(cfg)
        from zagg.config import get_windowing

        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        root = str(tmp_path / "store")
        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(cfg)))
        word = _shard_word()
        for label, lo, hi in [
            ("2024", "2024-02-01T00:00:00+00:00", "2024-11-01T00:00:00+00:00"),
            ("2025", "2025-01-15T00:00:00+00:00", "2025-03-01T00:00:00+00:00"),
        ]:
            leaf = hive.shard_leaf_path(root, word, window=label)
            store = open_store(leaf)
            grid.emit_shard_template(store, overwrite=True)
            hive.stamp_commit(
                store, cells_with_data=1, granule_count=1, window=label, time_range=[lo, hi]
            )
        env = refresh_root_coverage(root)
        assert env["time_range"] == ["2024-02-01T00:00:00+00:00", "2025-03-01T00:00:00+00:00"]


# ── dispatch fan-out + worker window wiring (phase 5) ────────────────────────


def _timed_rec(n, start, end):
    return {
        "id": f"g{n}",
        "s3": f"s3://bucket/granule{n}.h5",
        "https": f"https://h/g{n}.h5",
        "time_start": start,
        "time_end": end,
    }


class TestWindowedUnits:
    """``runner._windowed_units``: one work unit per (shard, window), granules
    subset by their shardmap time spans, bounds in dataset units."""

    def _windowing(self, cfg, **over):
        from zagg.config import get_windowing

        _windowed(cfg, **over)
        return get_windowing(cfg)

    def test_yearly_fanout_and_subsetting(self, cfg):
        from zagg.runner import _windowed_units

        g19 = _timed_rec(1, "2019-03-01T00:00:00Z", "2019-03-01T00:05:00Z")
        g20 = _timed_rec(2, "2020-07-01T00:00:00Z", "2020-07-01T00:05:00Z")
        straddle = _timed_rec(3, "2019-12-31T23:58:00Z", "2020-01-01T00:02:00Z")
        units = _windowed_units([(11, [g19, g20, straddle])], self._windowing(cfg), None)
        # Shard-major, chronological labels; the straddler rides BOTH windows.
        assert [(k, w["label"], [r["id"] for r in recs]) for k, recs, w in units] == [
            (11, "2019", ["g1", "g3"]),
            (11, "2020", ["g2", "g3"]),
        ]
        # Bounds are dataset units: GPS seconds since the ATLAS SDP epoch
        # (2018-01-01Z; post-2017 epoch -> the naive difference, exactly).
        w2019 = units[0][2]
        assert w2019["start"] == 365 * 86400.0
        assert w2019["end"] == (365 + 365) * 86400.0

    def test_untimed_granule_rides_every_window_with_bounds(self, cfg):
        from zagg.runner import _windowed_units

        legacy = {"id": "g0", "s3": "s3://b/g0.h5", "https": None}
        units = _windowed_units(
            [(11, [legacy])],
            self._windowing(cfg),
            {"start_date": "2019-01-01", "end_date": "2020-12-31"},
        )
        assert [w["label"] for _k, _r, w in units] == ["2019", "2020"]
        assert all(recs == [legacy] for _k, recs, _w in units)

    def test_bounds_full_iso_end_date_accepted(self, cfg):
        from zagg.runner import _windowed_units

        # A full ISO end_date must parse as-is (not get a T23:59:59 suffix
        # appended, which would corrupt it into a parse error).
        legacy = {"id": "g0", "s3": "s3://b/g0.h5", "https": None}
        units = _windowed_units(
            [(11, [legacy])],
            self._windowing(cfg),
            {"start_date": "2019-01-01", "end_date": "2020-12-31T00:00:00Z"},
        )
        assert [w["label"] for _k, _r, w in units] == ["2019", "2020"]

    def test_untimed_granule_without_bounds_is_a_pointed_error(self, cfg):
        from zagg.runner import _windowed_units

        legacy = {"id": "g0", "s3": "s3://b/g0.h5", "https": None}
        with pytest.raises(ValueError, match="bounds.temporal"):
            _windowed_units([(11, [legacy])], self._windowing(cfg), None)

    def test_explicit_schedule_uses_declared_windows(self, cfg):
        from zagg.runner import _windowed_units

        w = self._windowing(cfg, schedule="explicit")
        inside = _timed_rec(1, "2019-07-01T00:00:00Z", "2019-07-01T01:00:00Z")
        outside = _timed_rec(2, "2021-07-01T00:00:00Z", "2021-07-01T01:00:00Z")
        units = _windowed_units([(11, [inside, outside])], w, None)
        # Only the declared melt seasons dispatch; the 2021 granule matches
        # neither, and the empty melt-2020 subset is skipped entirely.
        assert [(w_["label"], [r["id"] for r in recs]) for _k, recs, w_ in units] == [
            ("melt-2019", ["g1"])
        ]

    def test_shard_with_no_matching_granules_dispatches_nothing(self, cfg):
        from zagg.runner import _windowed_units

        g = _timed_rec(1, "2019-03-01T00:00:00Z", "2019-03-01T01:00:00Z")
        units = _windowed_units([(11, [g]), (22, [])], self._windowing(cfg), None)
        assert [(k, w["label"]) for k, _r, w in units] == [(11, "2019")]

    def test_raster_instant_datetime_is_honored(self, cfg):
        from zagg.runner import _windowed_units

        rec = {"id": "r1", "s3": None, "https": None, "datetime": "2019-06-15T10:00:00Z"}
        units = _windowed_units([(11, [rec])], self._windowing(cfg), None)
        assert [w["label"] for _k, _r, w in units] == ["2019"]


class TestWindowedCellConfig:
    def test_injects_ge_lt_filters_on_the_time_field(self, cfg):
        from zagg.config import windowed_cell_config

        _windowed(cfg)
        unit_cfg, windowing = windowed_cell_config(cfg, {"label": "2019", "start": 1.5, "end": 9.5})
        got = unit_cfg.data_source["filters"]
        # The ATL06 quality_filter sugar is normalized FIRST, then the window
        # pair appends — declared filtering is preserved.
        assert got[0]["dataset"] == "/{group}/land_ice_segments/atl06_quality_summary"
        assert got[-2:] == [
            {
                "level": None,
                "dataset": "/{group}/land_ice_segments/delta_time",
                "op": "ge",
                "value": 1.5,
            },
            {
                "level": None,
                "dataset": "/{group}/land_ice_segments/delta_time",
                "op": "lt",
                "value": 9.5,
            },
        ]
        assert windowing["time_field"] == "delta_time"
        # The original config is untouched (per-unit copy).
        assert "filters" not in cfg.data_source

    def test_unwindowed_config_refuses(self, cfg):
        from zagg.config import windowed_cell_config

        with pytest.raises(ValueError, match="drift"):
            windowed_cell_config(cfg, {"label": "2019", "start": 0.0, "end": 1.0})


class TestProcessAndWriteHiveWindowed:
    """The shared write path threads the window end to end: leaf name, filter
    injection, ISO time_range stamp, and the D14 popcount."""

    def _grid(self, cfg):
        return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)

    def _carrier(self, grid, shard):
        import numpy as np
        import pandas as pd

        from zagg.config import get_agg_fields, get_data_vars, get_output_signature

        coords = grid.chunk_coords(shard)
        n = len(coords["cell_ids"])
        agg = get_agg_fields(grid.config)
        df = pd.DataFrame(
            {
                var: np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
                for var in get_data_vars(grid.config)
                if get_output_signature(agg[var])["kind"] != "ragged"
            }
        )
        for name, vals in coords.items():
            df[name] = vals
        return df

    def _run(self, monkeypatch, cfg, tmp_path, *, occupied, time_range=None, grid=None):
        import numpy as np
        import pandas as pd

        import zagg.processing as processing

        _windowed(cfg)
        grid = grid if grid is not None else self._grid(cfg)
        shard = _shard_word()
        root = str(tmp_path / "store")
        seen: dict = {}

        def fake(g, shard_key, urls, **kwargs):
            seen["config"] = kwargs["config"]
            seen["time_range_of"] = kwargs.get("time_range_of")
            kwargs["write_chunk"](
                grid.block_index(int(shard_key)), self._carrier(grid, shard_key), {}
            )
            if kwargs.get("occupied_out") is not None:
                kwargs["occupied_out"].append(np.asarray(occupied, dtype=np.uint64))
            meta = {
                "shard_key": int(shard_key),
                "cells_with_data": len(occupied),
                "total_obs": 7,
                "granule_count": 1,
                "files_processed": 1,
                "duration_s": 0.0,
                "error": None,
            }
            if time_range is not None:
                meta["time_range"] = time_range
            return pd.DataFrame(), meta

        monkeypatch.setattr(processing, "process_shard", fake)
        # One year window: [2019-01-01, 2020-01-01) as GPS seconds since the
        # ATLAS SDP epoch (2018-01-01Z).
        window = {"label": "2019", "start": 365 * 86400.0, "end": 730 * 86400.0}
        meta = hive.process_and_write_hive(
            shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={}, window=window
        )
        return grid, shard, root, meta, seen

    def test_windowed_leaf_filter_and_stamp(self, monkeypatch, cfg, tmp_path):
        import os

        from zagg.store import open_store

        grid, shard, root, meta, seen = self._run(
            monkeypatch,
            cfg,
            tmp_path,
            occupied=self._grid(cfg).children(_shard_word())[:3],
            # Dataset-unit extent from the worker: ~2019-03-02 .. ~2019-11-27.
            time_range=[425 * 86400.0, 695 * 86400.0],
        )
        leaf = hive.shard_leaf_path(root, shard, window="2019")
        assert os.path.exists(leaf)
        # The per-unit config the worker saw carries the injected window
        # filter pair and the time-extent sink on the declared time_field.
        assert seen["time_range_of"] == "delta_time"
        assert [f["op"] for f in seen["config"].data_source["filters"][-2:]] == ["ge", "lt"]
        # The stamp is the D15 truth: window label + ACTUAL ISO-UTC extent.
        stamp = hive.read_commit(open_store(leaf))
        assert stamp["spec"] == "morton-hive/2"
        assert stamp["window"] == "2019"
        assert stamp["time_range"] == ["2019-03-02T00:00:00+00:00", "2019-11-27T00:00:00+00:00"]
        # The dispatcher sees the SAME strings for the root-summary union.
        assert meta["time_range"] == stamp["time_range"]
        # Partial occupancy: the bitmap sidecar path is unchanged.
        assert stamp["coverage"]["encoding"] == "bitmap"
        assert os.path.exists(os.path.join(leaf, hive.COVERAGE_SIDECAR))

    def test_full_popcount_skips_the_sidecar(self, monkeypatch, cfg, tmp_path):
        import os

        from zagg.store import open_store

        grid, shard, root, _meta, _seen = self._run(
            monkeypatch, cfg, tmp_path, occupied=self._grid(cfg).children(_shard_word())
        )
        leaf = hive.shard_leaf_path(root, shard, window="2019")
        stamp = hive.read_commit(open_store(leaf))
        # D14: full subtree -> encoding "full", NO sidecar object written.
        assert stamp["coverage"]["encoding"] == "full"
        assert "sidecar" not in stamp["coverage"]
        assert not os.path.exists(os.path.join(leaf, hive.COVERAGE_SIDECAR))

    def test_depth0_full_popcount_short_circuits(self, monkeypatch, cfg, tmp_path):
        import os

        import numpy as np

        from zagg.coverage import bitmap_and
        from zagg.store import open_store

        # Depth-0 config: child_order == parent_order, so a shard IS one cell and
        # the popcount threshold is 4**0 == 1 (review finding, PR #248).
        grid0 = HealpixGrid(parent_order=6, child_order=6, layout="fullsphere", config=cfg)
        _grid, shard, root, _meta, _seen = self._run(
            monkeypatch,
            cfg,
            tmp_path,
            grid=grid0,
            occupied=grid0.children(_shard_word()),  # the single order-6 cell
        )
        leaf = hive.shard_leaf_path(root, shard, window="2019")
        stamp = hive.read_commit(open_store(leaf))
        # The one occupied cell is a FULL subtree: stamps "full", no sidecar, and
        # bitmap_and short-circuits — unlike an UNWINDOWED depth-0 leaf, which
        # stays box-only (full is gated on windowing).
        assert stamp["coverage"]["encoding"] == "full"
        assert "sidecar" not in stamp["coverage"]
        assert not os.path.exists(os.path.join(leaf, hive.COVERAGE_SIDECAR))
        aoi = np.asarray([shard], dtype=np.uint64)
        np.testing.assert_array_equal(np.sort(bitmap_and(leaf, aoi)), np.sort(aoi))

    def test_window_rerun_is_idempotent_replacement(self, monkeypatch, cfg, tmp_path):
        from zagg.store import open_store

        grid, shard, root, _m, _s = self._run(
            monkeypatch, cfg, tmp_path, occupied=self._grid(cfg).children(_shard_word())[:2]
        )
        # Re-dispatching the same window overwrites the leaf wholesale (D13).
        grid, shard, root, _m, _s = self._run(
            monkeypatch, cfg, tmp_path, occupied=self._grid(cfg).children(_shard_word())[:2]
        )
        leaf = hive.shard_leaf_path(root, shard, window="2019")
        assert hive.read_commit(open_store(leaf))["complete"] is True


class TestWindowedRunnerWiring:
    """Both dispatchers fan (shard, window) units into the shared write path."""

    def _catalog(self, tmp_path):
        import json as _json

        shard = _shard_word()
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "007"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 6,
                "child_order": 12,
                "layout": "fullsphere",
            },
            "shard_keys": [shard],
            "granules": [
                [
                    _timed_rec(1, "2019-03-01T00:00:00Z", "2019-03-01T00:05:00Z"),
                    _timed_rec(2, "2020-07-01T00:00:00Z", "2020-07-01T00:05:00Z"),
                    _timed_rec(3, "2019-12-31T23:58:00Z", "2020-01-01T00:02:00Z"),
                ]
            ],
        }
        p = tmp_path / "catalog.json"
        p.write_text(_json.dumps(catalog))
        return str(p), shard

    def test_local_windowed_fanout_manifest_and_root_union(self, monkeypatch, cfg, tmp_path):
        from zagg import runner
        from zagg.runner import agg

        _windowed(cfg)
        catalog_path, shard = self._catalog(tmp_path)
        root = str(tmp_path / "out")
        calls = []

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "a"})

        def fake_hive_write(shard_key, granule_urls, grid, s3_creds, store_root, config, **kw):
            w = kw["window"]
            calls.append((int(shard_key), w["label"], len(granule_urls)))
            return {
                "shard_key": int(shard_key),
                "error": None,
                "total_obs": 1,
                "time_range": [
                    f"{w['label']}-03-01T00:00:00+00:00",
                    f"{w['label']}-11-01T00:00:00+00:00",
                ],
            }

        monkeypatch.setattr(hive, "process_and_write_hive", fake_hive_write)
        agg(cfg, catalog=catalog_path, store=root, backend="local")

        # One unit per (shard, window); the straddler rides both years.
        assert sorted(calls) == [(shard, "2019", 2), (shard, "2020", 2)]
        # The manifest declares /2 + the temporal block.
        m = hive.read_manifest(root)
        assert m["spec"] == "morton-hive/2"
        assert m["temporal"]["schedule"] == "yearly"
        # The root summary carries the run's time-range union (D15 cache).
        env = hive.read_root_coverage(root)
        assert env["time_range"] == ["2019-03-01T00:00:00+00:00", "2020-11-01T00:00:00+00:00"]

    def test_lambda_cell_event_carries_window(self, cfg):
        import json as _json
        from unittest.mock import MagicMock

        from zagg.runner import _invoke_lambda_cell

        payload_box = MagicMock()
        payload_box.read.return_value = _json.dumps(
            {"statusCode": 200, "body": _json.dumps({"total_obs": 1})}
        ).encode()
        client = MagicMock()
        client.invoke.return_value = {"Payload": payload_box, "FunctionError": None}

        creds = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}
        window = {"label": "2019", "start": 365 * 86400.0, "end": 730 * 86400.0}
        _invoke_lambda_cell(
            client,
            (0,),
            _shard_word(),
            6,
            12,
            ["s3://b/g1.h5"],
            "s3://out/store",
            creds,
            function_name="process-shard",
            config_dict=None,
            window=window,
        )
        event = _json.loads(client.invoke.call_args.kwargs["Payload"])
        assert event["window"] == window

        # Unwindowed invoke: no "window" key — the event stays byte-identical
        # to the pre-#246 payload.
        _invoke_lambda_cell(
            client,
            (0,),
            _shard_word(),
            6,
            12,
            ["s3://b/g1.h5"],
            "s3://out/store",
            creds,
            function_name="process-shard",
            config_dict=None,
        )
        event = _json.loads(client.invoke.call_args.kwargs["Payload"])
        assert "window" not in event


class TestShardMapTimeMetadata:
    def test_granule_entry_carries_time_range(self):
        from zagg.catalog.shardmap import _granule_entry

        rec = _timed_rec(1, "2019-03-01T00:00:00+00:00", "2019-03-01T00:05:00+00:00")
        entry = _granule_entry(rec)
        assert entry["time_start"] == "2019-03-01T00:00:00+00:00"
        assert entry["time_end"] == "2019-03-01T00:05:00+00:00"
        # Legacy records without the keys stay byte-identical.
        legacy = {"id": "g", "s3": "s3://b/g.h5", "https": "https://h/g.h5"}
        assert _granule_entry(legacy) == legacy


# ── yearly end-to-end: real read path, observation-level split (phase 6) ─────


class _FakeH5:
    """Stub h5coro object (the ``test_processing`` filter-fixture pattern):
    ``readDatasets`` returns canned arrays by path, honoring hyperslices."""

    def __init__(self, arrays):
        self._arrays = arrays

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                out[d] = self._arrays[d]
                continue
            arr = self._arrays[d["dataset"]]
            hs = d["hyperslice"]
            if hs:
                lo, hi = hs[0]
                arr = arr[lo:hi]
            out[d["dataset"]] = arr
        return out


class TestYearlyEndToEnd:
    """The acceptance fixture (issue #246): a yearly windowed run over three
    granules — one per year plus a boundary straddler — driven through the
    REAL read path (``_read_group`` applies the injected window filters; only
    the h5coro transport is faked), asserting observation-level splitting,
    stamp truth, backfill, idempotent re-run, and the root time union."""

    DAY = 86400.0
    SHARD_DECIMAL = "-5112333"

    def _cfg(self):
        cfg = default_config("atl06")
        cfg.data_source = {
            "reader": "h5coro",
            "driver": "s3",
            "groups": ["g1"],
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h_li": "/h", "s_li": "/s", "delta_time": "/dt"},
        }
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00Z",  # ATLAS SDP epoch
            "scale": "gps",
        }
        validate_config(cfg)
        return cfg

    def _fakes(self):
        """Three granules in dataset units (GPS days since 2018-01-01):

        - gA: [300, 400, 401, 402] — one 2018 obs (backfill bait) + three 2019
        - gB: [800, 801, 802] — all 2020
        - gC: [729.5, 729.75, 730.0, 730.25, 730.5] — straddles the 2019/2020
          boundary (2020-01-01 = day 730); the 730.0 obs lands EXACTLY on the
          boundary instant and (half-open [start, end): ge start) belongs to 2020
        """
        import numpy as np

        def h5(days):
            n = len(days)
            return _FakeH5(
                {
                    "/lat": np.full(n, -78.5),
                    "/lon": np.full(n, -132.0),
                    "/h": np.arange(n, dtype=np.float32),
                    "/s": np.ones(n, dtype=np.float32),
                    "/dt": np.asarray(days, dtype=np.float64) * self.DAY,
                }
            )

        return {
            "s3://bucket/granuleA.h5": h5([300.0, 400.0, 401.0, 402.0]),
            "s3://bucket/granuleB.h5": h5([800.0, 801.0, 802.0]),
            "s3://bucket/granuleC.h5": h5([729.5, 729.75, 730.0, 730.25, 730.5]),
        }

    def _records(self):
        return [
            _timed_rec("A", "2018-10-28T00:00:00Z", "2019-02-07T00:00:00Z"),
            _timed_rec("B", "2020-03-10T00:00:00Z", "2020-03-13T00:00:00Z"),
            _timed_rec("C", "2019-12-31T12:00:00Z", "2020-01-01T12:00:00Z"),
        ]

    def _patch(self, monkeypatch, fakes):
        from zagg.index.hierarchical import HierarchicalIndex

        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda path, *a, **k: fakes[path])
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        monkeypatch.setattr(
            "zagg.processing.worker.index_from_config", lambda cfg: HierarchicalIndex()
        )

    def _run_units(self, monkeypatch, cfg, root, labels=None):
        from zagg.config import get_windowing
        from zagg.runner import _windowed_units

        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        shard = _shard_word()
        self._patch(monkeypatch, self._fakes())
        units = _windowed_units([(shard, self._records())], get_windowing(cfg), None)
        results = {}
        for shard_key, records, window in units:
            if labels is not None and window["label"] not in labels:
                continue
            urls = [r["s3"] for r in records]
            results[window["label"]] = hive.process_and_write_hive(
                shard_key, urls, grid, {}, root, cfg, store_kwargs={}, window=window
            )
        return grid, shard, results

    def _leaf_obs(self, root, shard, label, grid):
        import numpy as np
        import zarr

        from zagg.store import open_store

        leaf = hive.shard_leaf_path(root, shard, window=label)
        grp = zarr.open_group(open_store(leaf), path=grid.group_path, mode="r", zarr_format=3)
        return int(np.asarray(grp["count"][:]).sum())

    def test_boundary_straddling_observations_split_exactly(self, monkeypatch, tmp_path):
        from zagg.store import open_store

        root = str(tmp_path / "store")
        c = self._cfg()
        grid, shard, results = self._run_units(monkeypatch, c, root)
        # Enumerated from the granule spans: 2018 (gA's early obs), 2019, 2020.
        assert sorted(results) == ["2018", "2019", "2020"]

        # Observation-level split on delta_time (the injected ge/lt filters):
        # 2018 gets gA's single early obs; 2019 gets gA's three + gC's two
        # pre-boundary obs; 2020 gets gB's three + gC's three at-and-post-boundary
        # obs (the day-730.0 obs lands ON the boundary → 2020 by ge start).
        assert self._leaf_obs(root, shard, "2018", grid) == 1
        assert self._leaf_obs(root, shard, "2019", grid) == 5
        assert self._leaf_obs(root, shard, "2020", grid) == 6

        # Stamp truth (D15): window label + the ACTUAL ISO-UTC extent of what
        # was written, strictly inside the window's half-open range.
        stamp = hive.read_commit(open_store(hive.shard_leaf_path(root, shard, window="2019")))
        assert stamp["spec"] == "morton-hive/2"
        assert stamp["window"] == "2019"
        # day 400 = 2019-02-05; day 729.75 = 2019-12-31T18:00 (gC's last 2019 obs).
        assert stamp["time_range"] == ["2019-02-05T00:00:00+00:00", "2019-12-31T18:00:00+00:00"]
        # Boundary membership: gC's day-730.0 obs sits EXACTLY on the boundary
        # instant, so the 2020 leaf STARTS at 2020-01-01T00:00:00 — half-open
        # [start, end) puts the boundary tie in 2020, with no double-counting
        # (it is absent from 2019, whose lt end excludes it).
        stamp20 = hive.read_commit(open_store(hive.shard_leaf_path(root, shard, window="2020")))
        assert stamp20["time_range"][0] == "2020-01-01T00:00:00+00:00"
        # Per-unit granule subsetting reached the worker: 2019 read gA + gC.
        assert stamp["granule_count"] == 2

    def test_backfill_then_root_union_and_idempotent_rerun(self, monkeypatch, tmp_path):
        import os

        from zagg.coverage import refresh_root_coverage
        from zagg.store import open_store

        root = str(tmp_path / "store")
        c = self._cfg()
        # Later windows land first...
        grid, shard, _ = self._run_units(monkeypatch, c, root, labels={"2019", "2020"})
        leaf_2019 = hive.shard_leaf_path(root, shard, window="2019")
        stamped_at = hive.read_commit(open_store(leaf_2019))["written_at"]
        # ...then BACKFILL the earlier window: a new leaf appears next to the
        # committed ones, which are untouched (D13 — no resize, no rewrite).
        grid, shard, _ = self._run_units(monkeypatch, c, root, labels={"2018"})
        assert os.path.exists(hive.shard_leaf_path(root, shard, window="2018"))
        assert hive.read_commit(open_store(leaf_2019))["written_at"] == stamped_at

        # Idempotent window re-run (D13): same window again is wholesale
        # replacement, same content.
        grid, shard, _ = self._run_units(monkeypatch, c, root, labels={"2019"})
        assert self._leaf_obs(root, shard, "2019", grid) == 5

        # The rebuilt root summary unions the three stamps' time ranges:
        # day 300 = 2018-10-28 .. day 802 = 2020-03-13.
        from zagg.config import get_windowing

        hive.ensure_manifest(root, hive.build_manifest(grid, windowing=get_windowing(c)))
        env = refresh_root_coverage(root)
        assert env["time_range"] == ["2018-10-28T00:00:00+00:00", "2020-03-13T00:00:00+00:00"]
