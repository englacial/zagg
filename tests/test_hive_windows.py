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
        with pytest.raises(ValueError, match="declared data_source column"):
            validate_config(cfg)

    def test_time_field_rejected_when_no_columns_declared(self, cfg):
        # An empty read set is itself rejected: a store reading no columns
        # cannot filter on ``time_field`` (the check is unconditional).
        _windowed(cfg)
        cfg.data_source["coordinates"] = {}
        cfg.data_source["variables"] = {}
        with pytest.raises(ValueError, match="declared data_source column"):
            validate_config(cfg)

    def test_time_field_from_level_declared_variable_accepted(self, cfg):
        # A readable segment-level variable declared on a non-base level
        # (issue #30 mapping form) is a valid ``time_field``: it broadcasts to
        # a per-photon column the worker reads.
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
        validate_config(cfg)
        assert get_windowing(cfg)["time_field"] == "seg_time"

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

        root, word, _grid = self._windowed_store(cfg, tmp_path, labels=("2024",))
        # A leaf-shaped name whose window part breaks the frozen charset: the
        # walk warns and skips it instead of dying (escape-hatch posture).
        bad = tmp_path / "store" / "-5" / "1" / "1" / "2" / "3" / "3" / "3"
        (bad / "-5112333_bad_label.zarr").mkdir()
        (bad / "-5112333_bad_label.zarr" / "zarr.json").write_text("{}")
        with caplog.at_level(logging.WARNING, logger="zagg.coverage"):
            env = refresh_root_coverage(root)
        assert env is not None and len(env["ranges"]) == 1
        assert any("malformed window label" in r.message for r in caplog.records)

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

        with caplog.at_level(logging.WARNING, logger="zagg.hive"):
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
