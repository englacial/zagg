"""Tests for the fixed-width 2-D record-array modality (issue #678).

Phase 1: the grammar — ``link.width``, ``axes``, ``synthesize: offsets`` /
``index``, base-level ``{path, axes}`` coordinates, and list-form composite
join keys — through ``validate_config``.

Phase 2: the read route (``processing/read_fixed.py``) over a width-4 mini
granule behind the ``_FakeH5`` stub (the ``test_read_vlen`` convention):
synthesized link, ``axes`` layouts, offsets/index synthesis, the composite
join, asset-sourced record coordinates, and dispatch.
"""

import copy
import logging

import numpy as np
import pandas as pd
import pytest
from test_read_vlen import _Promise

from zagg.config import PipelineConfig, validate_config
from zagg.processing.read import _read_group, link_base_extent
from zagg.processing.read_fixed import (
    fixed_link_arrays,
    fixed_width_level,
    flatten_axes,
    synthesize_offsets,
)
from zagg.processing.read_vlen import _sibling_joined_values
from zagg.read_plan import plan_read

# ── GLAH01-shaped data_source (the issue #678 implementation spec) ───────────


def _fixed_ds(**extra):
    ds = {
        "coordinates": {
            "latitude": "/{group}/Geolocation/d_lat",
            "longitude": "/{group}/Geolocation/d_lon",
            "level": "shots",
        },
        "variables": {
            "rx_wf": {
                "path": "/{group}/Waveform/RecWaveform/r_rng_wf",
                "axes": ["record", "sample"],
            },
            "range": {
                "synthesize": "offsets",
                "level": "shots",
                "start": "/{group}/Waveform/RecWaveform/d_rng_start",
                "offsets": "/{group}/Waveform/RecWaveform/DS_Gate_Index_544",
                "scale": 0.15,
            },
            "sample_index": {"synthesize": "index", "level": "shots"},
        },
        "base_level": "samples",
        "levels": {
            "samples": {"path": "/{group}/Waveform", "link": None},
            "shots": {
                "path": "/{group}/Waveform",
                "coordinates": {
                    "latitude": "../Geolocation/d_lat",
                    "longitude": "../Geolocation/d_lon",
                },
                "link": {"to": "samples", "width": 544},
                "variables": {"i_waveformType": "/{group}/Waveform/Characteristics/i_waveformType"},
            },
        },
        "assets": {
            "glah05": {
                "join": {
                    "left": ["/{group}/Time/i_rec_ndx", "/{group}/Time/i_shot_count"],
                    "right": ["/{group}/Time/i_rec_ndx", "/{group}/Time/i_shot_count"],
                }
            }
        },
    }
    ds.update(extra)
    return ds


def _cfg(ds, source="rx_wf"):
    return PipelineConfig(
        data_source={"reader": "h5coro", "groups": ["Data_40HZ"], **ds},
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {"count": {"function": "len", "source": source}},
        },
        output={"grid": {"type": "healpix", "parent_order": 9, "child_order": 18}},
    )


def _rejects(ds, match):
    with pytest.raises(ValueError, match=match):
        validate_config(_cfg(ds))


# ── link.width ───────────────────────────────────────────────────────────────


class TestWidthLink:
    def test_the_spec_shape_passes(self):
        validate_config(_cfg(_fixed_ds()))

    def test_auto_passes(self):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"]["width"] = "auto"
        validate_config(_cfg(ds))

    @pytest.mark.parametrize("width", [0, -1, 1.5, True, "544", None])
    def test_width_must_be_a_positive_int_or_auto(self, width):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"]["width"] = width
        _rejects(ds, "width must be an integer >= 1 or 'auto'")

    @pytest.mark.parametrize("key", ["index_beg", "count", "index_base", "reference_index"])
    def test_width_excludes_the_dataset_link_keys(self, key):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"][key] = "/x" if key != "index_base" else 1
        _rejects(ds, f"'width' is mutually exclusive with \\['{key}'\\]")

    def test_without_width_the_dataset_keys_stay_required(self):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {"to": "samples", "index_beg": "/x"}
        _rejects(ds, "'count' is required")

    def test_to_is_still_required(self):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {"width": 544}
        _rejects(ds, "'to' is required")

    def test_two_width_levels_rejected(self):
        ds = _fixed_ds()
        ds["levels"]["frames"] = {"path": "/{group}", "link": {"to": "samples", "width": 10}}
        _rejects(ds, "only one fixed-width record level")

    def test_width_link_must_point_at_the_base_level(self):
        ds = _fixed_ds()
        ds["levels"]["frames"] = {"path": "/{group}", "link": {"to": "shots", "width": 10}}
        _rejects(ds, "a 'width' link must point at base_level 'samples'")


# ── axes ─────────────────────────────────────────────────────────────────────


class TestAxes:
    @pytest.mark.parametrize(
        "axes",
        [
            ["record", "sample"],
            ["sample", "record"],
            ["record", "sample", 0],
            [3, "record", "sample"],
        ],
    )
    def test_well_formed_axes_pass(self, axes):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["axes"] = axes
        validate_config(_cfg(ds))

    @pytest.mark.parametrize("axes", [[], "record,sample", None, ("record", "sample")])
    def test_axes_must_be_a_non_empty_list(self, axes):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["axes"] = axes
        _rejects(ds, "axes must be a non-empty list")

    @pytest.mark.parametrize(
        "axes", [["record"], ["sample"], ["record", "record", "sample"], [0, 1]]
    )
    def test_record_and_sample_exactly_once(self, axes):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["axes"] = axes
        _rejects(ds, "exactly once")

    @pytest.mark.parametrize("bad", [-1, True, 1.0, "band", None])
    def test_other_entries_are_non_negative_ints(self, bad):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["axes"] = ["record", "sample", bad]
        _rejects(ds, "entries must be 'record', 'sample' or an integer >= 0")

    def test_axes_and_column_are_mutually_exclusive(self):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["column"] = 0
        _rejects(ds, "exactly one of 'column' or 'axes'")

    def test_mapping_form_needs_one_of_them(self):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"] = {"path": "/x"}
        _rejects(ds, "exactly one of 'column' or 'axes'")

    def test_unknown_keys_rejected(self):
        ds = _fixed_ds()
        ds["variables"]["rx_wf"]["flatten"] = True
        _rejects(ds, r"unknown keys \['flatten'\]")

    def test_axes_requires_a_width_link_on_the_coordinates_level(self):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {
            "to": "samples",
            "index_beg": "/{group}/beg",
            "count": "/{group}/cnt",
        }
        del ds["variables"]["range"]  # the offsets dataset needs the width too
        _rejects(ds, "axes requires coordinates.level to name a level whose link declares 'width'")

    def test_axes_requires_the_vlen_route(self):
        ds = _fixed_ds()
        del ds["coordinates"]["level"]
        del ds["variables"]["range"]
        del ds["variables"]["sample_index"]
        _rejects(ds, "axes requires coordinates.level")

    def test_column_form_is_untouched(self):
        ds = _fixed_ds()
        ds["variables"]["conf"] = {"path": "/{group}/signal_conf", "column": 1}
        validate_config(_cfg(ds))
        ds["variables"]["conf"]["column"] = -1
        _rejects(ds, "column must be an integer >= 0")


# ── synthesize: offsets / index ──────────────────────────────────────────────


class TestSynthesizeKinds:
    def test_unknown_kind_lists_the_three(self):
        ds = _fixed_ds()
        ds["variables"]["range"]["synthesize"] = "arange"
        _rejects(ds, r"must be one of \['index', 'linspace', 'offsets'\]")

    def test_offsets_constant_step_passes(self):
        ds = _fixed_ds()
        ds["variables"]["range"]["offsets"] = -0.15
        del ds["variables"]["range"]["scale"]
        validate_config(_cfg(ds))

    @pytest.mark.parametrize(
        "name,step",
        [("range", None), ("range", -0.15), ("sample_index", None)],
        ids=["offsets-dataset", "offsets-step", "index"],
    )
    def test_fixed_width_kinds_reject_a_ragged_link(self, name, step):
        # The ragged (vlen) route synthesizes linspace only, so ``offsets``
        # in either form and ``index`` need the width link.
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {
            "to": "samples",
            "index_beg": "/{group}/beg",
            "count": "/{group}/cnt",
        }
        ds["variables"] = {"rx_wf": "/{group}/wf", name: ds["variables"][name]}
        if step is not None:
            ds["variables"]["range"]["offsets"] = step
        kind = ds["variables"][name]["synthesize"]
        _rejects(ds, f"synthesize '{kind}' is a fixed-width form")

    @pytest.mark.parametrize("offsets", ["", None, True, [1, 2]])
    def test_offsets_must_be_a_path_or_a_number(self, offsets):
        ds = _fixed_ds()
        ds["variables"]["range"]["offsets"] = offsets
        _rejects(ds, "offsets must be a 1-D dataset path or a numeric step")

    def test_offsets_requires_start(self):
        ds = _fixed_ds()
        del ds["variables"]["range"]["start"]
        _rejects(ds, r"\.start must be a non-empty string")

    @pytest.mark.parametrize("scale", ["x", True, None])
    def test_scale_must_be_numeric(self, scale):
        ds = _fixed_ds()
        ds["variables"]["range"]["scale"] = scale
        _rejects(ds, "scale must be a number")

    def test_offsets_rejects_linspace_keys(self):
        ds = _fixed_ds()
        ds["variables"]["range"]["stop"] = "/x"
        _rejects(ds, r"unknown keys \['stop'\] \(the 'offsets' form takes")

    def test_index_takes_only_level(self):
        ds = _fixed_ds()
        ds["variables"]["sample_index"]["start"] = "/x"
        _rejects(ds, r"unknown keys \['start'\] \(the 'index' form takes")

    def test_index_requires_level(self):
        ds = _fixed_ds()
        ds["variables"]["sample_index"] = {"synthesize": "index"}
        _rejects(ds, r"\.level must be a non-empty string")

    @pytest.mark.parametrize("name", ["range", "sample_index"])
    def test_level_must_be_the_coordinates_level(self, name):
        ds = _fixed_ds()
        ds["variables"][name]["level"] = "samples"
        _rejects(ds, "must equal coordinates.level")

    def test_index_requires_the_vlen_route(self):
        ds = _fixed_ds()
        del ds["coordinates"]["level"]
        ds["variables"] = {"sample_index": {"synthesize": "index", "level": "shots"}}
        _rejects(ds, "synthesize requires the vlen route")

    def test_linspace_is_unchanged(self):
        ds = _fixed_ds()
        ds["variables"]["range"] = {
            "synthesize": "linspace",
            "level": "shots",
            "start": "/{group}/a",
            "stop": "/{group}/b",
        }
        validate_config(_cfg(ds))
        del ds["variables"]["range"]["stop"]
        _rejects(ds, r"\.stop must be a non-empty string")


# ── composite join keys ──────────────────────────────────────────────────────


class TestCompositeJoin:
    def test_string_keys_still_pass(self):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"] = {"left": "/{group}/Time/d_UTCTime_40", "right": "/t"}
        validate_config(_cfg(ds))

    def test_unequal_lengths_rejected(self):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"]["right"] = ["/{group}/Time/i_rec_ndx"]
        _rejects(ds, r"equal-length lists \(left has 2, right has 1\)")

    def test_mixed_forms_rejected(self):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"]["right"] = "/{group}/Time/i_rec_ndx"
        _rejects(ds, "both be strings or both be lists")

    @pytest.mark.parametrize("bad", [[], ["/a", ""], ["/a", 1]])
    def test_list_entries_are_non_empty_paths(self, bad):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"]["left"] = bad
        _rejects(ds, "must be a non-empty list of dataset paths")

    @pytest.mark.parametrize("side", ["left", "right"])
    def test_repeated_key_paths_rejected(self, side):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"][side] = ["/{group}/Time/i_rec_ndx"] * 2
        _rejects(ds, f"join.{side}: composite key paths must be distinct")

    def test_unknown_join_keys_rejected(self):
        ds = _fixed_ds()
        ds["assets"]["glah05"]["join"]["on"] = "/x"
        _rejects(ds, r"unknown keys \['on'\]")


# ── base-level {path, axes} coordinates (the swath form, validation only) ────


def _swath_ds():
    ds = copy.deepcopy(_fixed_ds())
    del ds["coordinates"]["level"]
    ds["coordinates"] = {
        "latitude": {"path": "/{group}/Latitude", "axes": ["record", "sample"]},
        "longitude": {"path": "/{group}/Longitude", "axes": ["record", "sample"]},
    }
    ds["levels"]["scanlines"] = ds["levels"].pop("shots")
    ds["levels"]["scanlines"]["link"]["width"] = "auto"
    del ds["levels"]["scanlines"]["coordinates"]
    del ds["levels"]["scanlines"]["variables"]
    # ``axes`` on a variable rides coordinates.level; the swath form has none,
    # so its science variables stay plain reads until the swath read path
    # lands (issue #678 phase 5) — only the coordinates are pinned here.
    ds["variables"] = {"rx_wf": "/{group}/Radiance"}
    del ds["assets"]
    return ds


class TestBaseLevelCoordinateAxes:
    def test_the_swath_form_validates(self):
        ds = _swath_ds()
        validate_config(_cfg(ds))

    def test_both_coordinates_take_the_same_form(self):
        ds = _swath_ds()
        ds["coordinates"]["longitude"] = "/{group}/Longitude"
        _rejects(ds, "both be plain paths or both")

    def test_mapping_form_takes_path_and_axes_only(self):
        ds = _swath_ds()
        ds["coordinates"]["latitude"] = {"path": "/x", "axes": ["record", "sample"], "column": 0}
        _rejects(ds, "takes exactly 'path' and 'axes'")

    def test_axes_are_validated(self):
        ds = _swath_ds()
        ds["coordinates"]["latitude"]["axes"] = ["record"]
        _rejects(ds, "coordinates.latitude.axes must name 'sample' exactly once")

    def test_excludes_coordinates_level(self):
        ds = _swath_ds()
        ds["coordinates"]["level"] = "scanlines"
        _rejects(ds, "exclude coordinates.level")

    def test_requires_a_width_link(self):
        ds = _swath_ds()
        ds["levels"]["scanlines"]["link"] = {
            "to": "samples",
            "index_beg": "/b",
            "count": "/c",
        }
        _rejects(ds, "require a level whose link declares 'width'")

    def test_axes_variables_wait_for_the_swath_read_path(self):
        ds = _swath_ds()
        ds["variables"]["rx_wf"] = {"path": "/{group}/Radiance", "axes": ["record", "sample"]}
        _rejects(ds, "axes variables on the swath form .* arrive with the swath read path")


# ═════════════════════════════════════════════════════════════════════════════
# Phase 2 — the read route (``processing/read_fixed.py``)
# ═════════════════════════════════════════════════════════════════════════════
#
# A five-record, width-4 mini-granule. Record coordinates live on the primary
# (``_fx_arrays``) or come from a sibling through the composite join
# (``_fx_sibling_arrays``); the waveform is stored record-first, transposed,
# and as a two-band cube so every ``axes`` layout is pinned.
#
#   record  lat   flag  rng_start  key (a, b)
#   0       10.0  0     100.0      (1, 0)
#   1       11.0  0     200.0      (1, 1)
#   2       12.0  1     300.0      (2, 0)   <- no sibling row
#   3       13.0  0     400.0      (2, 1)
#   4       14.0  0     500.0      (3, 0)

WIDTH = 4
N_REC = 5
FX_LATS = np.array([10.0, 11.0, 12.0, 13.0, 14.0])
FX_LONS = np.array([-100.0, -100.1, -100.2, -100.3, -100.4])
FX_FLAG = np.array([0, 0, 1, 0, 0], dtype=np.uint8)
FX_START = np.array([100.0, 200.0, 300.0, 400.0, 500.0])
FX_GATE = np.array([0.0, 1.0, 3.0, 7.0])  # a piecewise offsets table, length WIDTH
FX_KEY_A = np.array([1, 1, 2, 2, 3], dtype=np.int32)
FX_KEY_B = np.array([0, 1, 0, 1, 0], dtype=np.int32)
FX_WF = (np.arange(N_REC * WIDTH, dtype=np.float32) + 1000.0).reshape(N_REC, WIDTH)
FX_TABLE = np.arange(N_REC * 6, dtype=np.float32).reshape(N_REC, 6)
FX_FLAT = np.arange(N_REC * WIDTH, dtype=np.int16)


class _FakeH5:
    """Stub h5coro object: hyperslices slice dim 0 (trailing dims whole)."""

    def __init__(self, arrays):
        self._arrays = arrays

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                out[d] = self._arrays[d]
                continue
            path = d["dataset"]
            arr = self._arrays[path]
            lo, hi = d["hyperslice"][0]
            out[path] = arr[lo:hi]
        return out


class _LatGrid:
    """Shard 1 for lat >= 11.5, else 0 (leaf id == shard id)."""

    @staticmethod
    def assign(lats, lons):
        return (np.asarray(lats) >= 11.5).astype(np.uint64)

    @staticmethod
    def shards_of(leaf_ids):
        return np.asarray(leaf_ids).astype(int)


class _OneShardGrid:
    @staticmethod
    def assign(lats, lons):
        return np.arange(len(lats), dtype=np.uint64)

    @staticmethod
    def shards_of(leaf_ids):
        return np.zeros(len(leaf_ids), dtype=int)


def _fx_arrays(group="G"):
    return {
        f"/{group}/wf": FX_WF,
        f"/{group}/wf_T": np.ascontiguousarray(FX_WF.T),
        f"/{group}/cube": np.stack([FX_WF, FX_WF + 100.0], axis=-1),
        f"/{group}/flat": FX_FLAT,
        f"/{group}/lat": FX_LATS,
        f"/{group}/lon": FX_LONS,
        f"/{group}/flag": FX_FLAG,
        f"/{group}/rng_start": FX_START,
        f"/{group}/gate": FX_GATE,
        f"/{group}/table": FX_TABLE,
        f"/{group}/key_a": FX_KEY_A,
        f"/{group}/key_b": FX_KEY_B,
    }


def _fx_sibling_arrays(group="G"):
    # Record 2 (key (2, 0)) has no sibling row; (9, 9) is a sibling-only row.
    return {
        f"/{group}/key_a": np.array([3, 1, 2, 1, 9], dtype=np.int32),
        f"/{group}/key_b": np.array([0, 1, 1, 0, 9], dtype=np.int32),
        f"/{group}/lat": np.array([14.0, 11.0, 13.0, 10.0, 99.0]),
        f"/{group}/lon": np.array([-100.4, -100.1, -100.3, -100.0, 0.0]),
        f"/{group}/qual": np.array([1, 1, 0, 1, 1], dtype=np.uint8),
    }


def _fx_ds(**extra):
    ds = {
        "coordinates": {"latitude": "/{group}/lat", "longitude": "/{group}/lon", "level": "recs"},
        "variables": {
            "wf": {"path": "/{group}/wf", "axes": ["record", "sample"]},
            "rng": {
                "synthesize": "offsets",
                "level": "recs",
                "start": "/{group}/rng_start",
                "offsets": "/{group}/gate",
                "scale": 0.5,
            },
            "sample_index": {"synthesize": "index", "level": "recs"},
        },
        "base_level": "samples",
        "levels": {
            "samples": {"path": "/{group}", "link": None},
            "recs": {
                "path": "/{group}",
                "link": {"to": "samples", "width": WIDTH},
                "variables": {"flag": "/{group}/flag"},
            },
        },
    }
    ds.update(extra)
    return ds


def _fx_planned_ds(**extra):
    return _fx_ds(read_plan={"spatial_index": "recs", "pad": 0}, **extra)


def _expected_rng(records):
    return np.concatenate([FX_START[r] + FX_GATE * 0.5 for r in records])


class TestFixedLinkArrays:
    def test_extent_is_records_times_width(self):
        ibeg, cnt = fixed_link_arrays(N_REC, WIDTH)
        assert ibeg.tolist() == [0, 4, 8, 12, 16]
        assert cnt.tolist() == [4] * 5
        assert link_base_extent(ibeg, cnt, 0) == N_REC * WIDTH

    def test_plan_read_slices_record_aligned(self):
        ibeg, cnt = fixed_link_arrays(N_REC, WIDTH)
        plan = plan_read(
            FX_LATS, FX_LONS, ibeg, cnt, N_REC * WIDTH, pad=0, coarse_mask=FX_LATS >= 12.5
        )
        assert plan.base_slices == [(12, 20)]


class TestFlattenAxes:
    def test_record_sample_is_row_major(self):
        np.testing.assert_array_equal(
            flatten_axes(FX_WF, ["record", "sample"], WIDTH, "wf"), FX_WF.reshape(-1)
        )

    def test_sample_record_transposes(self):
        np.testing.assert_array_equal(
            flatten_axes(FX_WF.T, ["sample", "record"], WIDTH, "wf"), FX_WF.reshape(-1)
        )

    def test_integer_picks(self):
        cube = np.stack([FX_WF, FX_WF + 100.0], axis=-1)
        np.testing.assert_array_equal(
            flatten_axes(cube, ["record", "sample", 1], WIDTH, "c"), FX_WF.reshape(-1) + 100.0
        )
        np.testing.assert_array_equal(
            flatten_axes(np.moveaxis(cube, -1, 0), [0, "record", "sample"], WIDTH, "c"),
            FX_WF.reshape(-1),
        )

    def test_rank_mismatch_raises(self):
        with pytest.raises(ValueError, match="2-D but axes .* declares 3"):
            flatten_axes(FX_WF, ["record", "sample", 0], WIDTH, "wf")

    def test_width_mismatch_raises(self):
        with pytest.raises(ValueError, match="'sample' axis has 4 entries but link.width is 5"):
            flatten_axes(FX_WF, ["record", "sample"], 5, "wf")


class TestSynthesizeOffsets:
    def test_table_form(self):
        parent = np.repeat(np.arange(2), WIDTH)
        within = np.tile(np.arange(WIDTH), 2)
        out = synthesize_offsets(FX_START, FX_GATE, 0.5, WIDTH, parent, within)
        np.testing.assert_allclose(out, _expected_rng([0, 1]))

    def test_step_form(self):
        parent = np.array([0, 0, 0, 0])
        within = np.arange(4)
        out = synthesize_offsets(FX_START, -0.15, 1.0, WIDTH, parent, within)
        np.testing.assert_allclose(out, 100.0 - 0.15 * np.arange(4))

    def test_gap_rows_nan(self):
        out = synthesize_offsets(FX_START, FX_GATE, 1.0, WIDTH, np.array([0, -1]), np.array([1, 0]))
        assert out[0] == 101.0 and np.isnan(out[1])

    def test_wrong_table_length_raises(self):
        with pytest.raises(ValueError, match=r"shape \(3,\); expected a 1-D table of length"):
            synthesize_offsets(FX_START, FX_GATE[:3], 1.0, WIDTH, np.array([0]), np.array([0]))


class TestFixedReadGroup:
    def test_full_read_gather_expand_synthesis(self):
        df = _read_group(_FakeH5(_fx_arrays()), "G", _fx_ds(), 0, _OneShardGrid())
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF.reshape(-1))
        np.testing.assert_allclose(df["rng"].to_numpy(), _expected_rng(range(N_REC)))
        assert df["sample_index"].tolist() == list(range(WIDTH)) * N_REC
        assert df["flag"].tolist() == np.repeat(FX_FLAG, WIDTH).tolist()
        assert df["leaf_id"].tolist() == list(range(N_REC * WIDTH))

    def test_planned_read_selects_shard_records(self):
        io_stats = {}
        df = _read_group(
            _FakeH5(_fx_arrays()), "G", _fx_planned_ds(), 1, _LatGrid(), io_stats=io_stats
        )
        # Shard 1 = records 2, 3, 4 (lat >= 11.5); pad 0 -> rows 8..20 only.
        assert io_stats["obs_read"] == 12
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[2:].reshape(-1))
        np.testing.assert_allclose(df["rng"].to_numpy(), _expected_rng([2, 3, 4]))

    def test_planned_read_touches_only_the_shards_rows(self):
        reads = []

        class _Spy(_FakeH5):
            def readDatasets(self, datasets):  # noqa: N802
                for d in datasets:
                    reads.append(
                        (d, None) if isinstance(d, str) else (d["dataset"], d["hyperslice"])
                    )
                return super().readDatasets(datasets)

        _read_group(_Spy(_fx_arrays()), "G", _fx_planned_ds(), 1, _LatGrid())
        # The 2-D waveform is sliced by RECORD rows (2..5) once, never read whole.
        assert [r for r in reads if r[0] == "/G/wf"] == [("/G/wf", [(2, 5)])]

    def test_planned_matches_full(self):
        for shard in (0, 1):
            full = _read_group(_FakeH5(_fx_arrays()), "G", _fx_ds(), shard, _LatGrid())
            planned = _read_group(_FakeH5(_fx_arrays()), "G", _fx_planned_ds(), shard, _LatGrid())
            pd.testing.assert_frame_equal(full, planned)

    def test_empty_shard_returns_none(self):
        io_stats = {}
        assert (
            _read_group(
                _FakeH5(_fx_arrays()), "G", _fx_planned_ds(), 7, _LatGrid(), io_stats=io_stats
            )
            is None
        )
        assert io_stats["obs_read"] == 0

    def test_transposed_variable_reads_whole_with_a_notice(self, caplog):
        ds = _fx_planned_ds()
        ds["variables"]["wf"] = {"path": "/{group}/wf_T", "axes": ["sample", "record"]}
        reads = []

        class _Spy(_FakeH5):
            def readDatasets(self, datasets):  # noqa: N802
                reads.extend(d if isinstance(d, str) else d["dataset"] for d in datasets)
                return super().readDatasets(datasets)

        with caplog.at_level(logging.INFO, logger="zagg.processing.read_fixed"):
            df = _read_group(_Spy(_fx_arrays()), "G", ds, 1, _LatGrid())
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[2:].reshape(-1))
        assert "/G/wf_T" in reads  # a whole read, not a hyperslice
        assert "is read whole" in caplog.text

    def test_band_pick_on_a_cube(self):
        ds = _fx_ds()
        ds["variables"]["wf"] = {"path": "/{group}/cube", "axes": ["record", "sample", 1]}
        ds["variables"]["band0"] = {"path": "/{group}/cube", "axes": ["record", "sample", 0]}
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF.reshape(-1) + 100.0)
        np.testing.assert_array_equal(df["band0"].to_numpy(), FX_WF.reshape(-1))

    @pytest.mark.parametrize("ds_fn", [_fx_ds, _fx_planned_ds])
    def test_width_mismatch_raises_before_aggregation(self, ds_fn):
        ds = ds_fn()
        ds["levels"]["recs"]["link"]["width"] = 5
        with pytest.raises(ValueError, match="but link.width is 5"):
            _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())

    @pytest.mark.parametrize("ds_fn", [_fx_ds, _fx_planned_ds])
    def test_rank_mismatch_raises(self, ds_fn):
        ds = ds_fn()
        ds["variables"]["wf"]["axes"] = ["record", "sample", 0]
        with pytest.raises(ValueError, match="2-D but axes"):
            _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())

    @pytest.mark.parametrize("ds_fn", [_fx_ds, _fx_planned_ds])
    def test_failed_axes_read_is_reported_as_a_read(self, ds_fn):
        # h5coro signals a failed read with None: report the read, not a rank
        # mismatch (PR #680 review).
        class _Failing(_FakeH5):
            def readDatasets(self, datasets):  # noqa: N802
                out = super().readDatasets(datasets)
                return {p: (None if p == "/G/wf" else a) for p, a in out.items()}

        with pytest.raises(ValueError, match="read returned None for /G/wf"):
            _read_group(_Failing(_fx_arrays()), "G", ds_fn(), 1, _LatGrid())

    def test_full_read_record_count_mismatch_raises(self):
        arrays = _fx_arrays()
        arrays["/G/wf"] = FX_WF[:3]
        with pytest.raises(ValueError, match="flattens to 12 rows but the coordinates level has 5"):
            _read_group(_FakeH5(arrays), "G", _fx_ds(), 0, _OneShardGrid())

    def test_flat_base_rate_variable_rides_the_synthesized_link(self):
        ds = _fx_planned_ds()
        ds["variables"]["flat"] = "/{group}/flat"
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 1, _LatGrid())
        assert df["flat"].tolist() == list(range(8, 20))

    @pytest.mark.parametrize("n_rows", [N_REC * WIDTH + 5, N_REC * WIDTH - 2])
    def test_full_arm_flat_length_mismatch_raises(self, n_rows):
        # A fixed-width link has no slack: a LONGER flat dataset means the
        # declared width is wrong and must raise, not trim (PR #680 review).
        arrays = _fx_arrays()
        arrays["/G/flat"] = np.arange(n_rows, dtype=np.int16)
        ds = _fx_ds()
        ds["variables"]["flat"] = "/{group}/flat"
        with pytest.raises(ValueError, match=f"'/G/flat' has {n_rows} rows but the fixed-width"):
            _read_group(_FakeH5(arrays), "G", ds, 0, _OneShardGrid())

    def test_planned_arm_flat_short_read_raises(self):
        # The planned arm cannot see the dataset's length, but a hyperslice off
        # the end of a truncated dataset comes back short and raises.
        arrays = _fx_arrays()
        arrays["/G/flat"] = FX_FLAT[:14]
        ds = _fx_planned_ds()
        ds["variables"]["flat"] = "/{group}/flat"
        with pytest.raises(ValueError, match="returned 6 of 12 planned rows"):
            _read_group(_FakeH5(arrays), "G", ds, 1, _LatGrid())

    def test_full_arm_flat_read_through_an_h5promise(self):
        # A real h5coro handle returns an ``H5Promise`` (no ``.items()``): the
        # full arm indexes it by path (PR #680 review).
        class _PromiseH5(_FakeH5):
            def readDatasets(self, datasets):  # noqa: N802
                return _Promise(super().readDatasets(datasets))

        ds = _fx_ds()
        ds["variables"]["flat"] = "/{group}/flat"
        df = _read_group(_PromiseH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        assert df["flat"].tolist() == FX_FLAT.tolist()

    def test_index_synthesis_drops_padding_via_expression(self):
        # Records with flag == 1 carry 2 valid samples of 4; the others all 4.
        ds = _fx_ds(filters=[{"expression": "sample_index < np.where(flag == 1, 2, 4)"}])
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        assert len(df) == 4 * WIDTH + 2
        assert df["sample_index"][df["flag"] == 1].tolist() == [0, 1]

    def test_record_level_filter_expands(self):
        ds = _fx_planned_ds(
            filters=[{"level": "recs", "dataset": "/{group}/flag", "op": "eq", "value": 0}]
        )
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 1, _LatGrid())
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[3:].reshape(-1))

    @pytest.mark.parametrize("ds_fn", [_fx_ds, _fx_planned_ds])
    def test_third_level_filter_link(self, ds_fn):
        # A cross-level filter on a link that is NOT the coordinates link:
        # block 0 = rows 0..9 (flag 0), block 1 = rows 10..19 (flag 1).
        arrays = _fx_arrays()
        arrays["/G/blk_beg"] = np.array([0, 10], dtype=np.int64)
        arrays["/G/blk_cnt"] = np.array([10, 10], dtype=np.int64)
        arrays["/G/blk_flag"] = np.array([0, 1], dtype=np.uint8)
        ds = ds_fn(
            filters=[{"level": "blocks", "dataset": "/{group}/blk_flag", "op": "eq", "value": 1}]
        )
        ds["levels"]["blocks"] = {
            "path": "/{group}",
            "link": {"to": "samples", "index_beg": "/{group}/blk_beg", "count": "/{group}/blk_cnt"},
        }
        df = _read_group(_FakeH5(arrays), "G", ds, 1, _LatGrid())
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF.reshape(-1)[10:20])
        # An overlapping third-level link raises, as on the vlen route (PR #680 review).
        arrays["/G/blk_beg"] = np.array([0, 8], dtype=np.int64)
        with pytest.raises(ValueError, match="link records on level 'blocks' overlap"):
            _read_group(_FakeH5(arrays), "G", ds, 1, _LatGrid())

    def test_record_level_table_column(self):
        ds = _fx_ds()
        ds["levels"]["recs"]["variables"]["wt5"] = {"path": "/{group}/table", "column": 5}
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        np.testing.assert_array_equal(df["wt5"].to_numpy(), np.repeat(FX_TABLE[:, 5], WIDTH))

    def test_linspace_still_works_under_a_width_link(self):
        ds = _fx_ds()
        ds["variables"]["rng"] = {
            "synthesize": "linspace",
            "level": "recs",
            "start": "/{group}/rng_start",
            "stop": "/{group}/rng_start",
        }
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        np.testing.assert_allclose(df["rng"].to_numpy(), np.repeat(FX_START, WIDTH))

    def test_offsets_step_form(self):
        ds = _fx_ds()
        ds["variables"]["rng"]["offsets"] = -0.15
        del ds["variables"]["rng"]["scale"]
        df = _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())
        np.testing.assert_allclose(
            df["rng"].to_numpy(), np.concatenate([s - 0.15 * np.arange(WIDTH) for s in FX_START])
        )

    def test_width_auto_is_refused_at_read_time(self):
        ds = _fx_ds()
        ds["levels"]["recs"]["link"]["width"] = "auto"
        with pytest.raises(ValueError, match="'auto' is not readable yet"):
            _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())

    def test_spatial_index_must_name_the_coordinates_level(self):
        ds = _fx_ds(read_plan={"spatial_index": "samples"})
        with pytest.raises(ValueError, match="must name the coordinates level"):
            _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())

    def test_arrow_output(self):
        table = _read_group(_FakeH5(_fx_arrays()), "G", _fx_ds(), 0, _OneShardGrid(), arrow=True)
        assert table.num_rows == N_REC * WIDTH


def _fx_join_ds(**extra):
    return _fx_ds(
        assets={
            "sib": {
                "join": {
                    "left": ["/{group}/key_a", "/{group}/key_b"],
                    "right": ["/{group}/key_a", "/{group}/key_b"],
                }
            }
        },
        **extra,
    )


class TestCompositeJoinRead:
    def test_asset_filter_joins_on_the_tuple(self):
        ds = _fx_join_ds(
            filters=[{"asset": "sib", "dataset": "/{group}/qual", "op": "eq", "value": 1}]
        )
        df = _read_group(
            _FakeH5(_fx_arrays()),
            "G",
            ds,
            0,
            _OneShardGrid(),
            siblings={"sib": _FakeH5(_fx_sibling_arrays())},
        )
        # Record 2 has no sibling row (fails closed); record 3's qual is 0.
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[[0, 1, 4]].reshape(-1))

    def test_a_single_key_would_mismatch(self):
        # key_a alone is NOT unique on the sibling: the composite is what makes
        # the join a join.
        ds = _fx_join_ds(
            filters=[{"asset": "sib", "dataset": "/{group}/qual", "op": "eq", "value": 1}]
        )
        ds["assets"]["sib"]["join"] = {"left": "/{group}/key_a", "right": "/{group}/key_a"}
        with pytest.raises(ValueError, match="duplicate key"):
            _read_group(
                _FakeH5(_fx_arrays()),
                "G",
                ds,
                0,
                _OneShardGrid(),
                siblings={"sib": _FakeH5(_fx_sibling_arrays())},
            )

    def test_asset_variable_through_the_composite_join(self):
        ds = _fx_join_ds()
        ds["levels"]["recs"]["variables"]["qual"] = {"asset": "sib", "path": "/{group}/qual"}
        df = _read_group(
            _FakeH5(_fx_arrays()),
            "G",
            ds,
            0,
            _OneShardGrid(),
            siblings={"sib": _FakeH5(_fx_sibling_arrays())},
        )
        q = df["qual"].to_numpy()
        np.testing.assert_array_equal(q[[0, 4, 12, 16]], [1, 1, 0, 1])
        assert np.isnan(q[8:12]).all()  # record 2: no sibling row

    def test_joined_values_unit(self):
        values, matched = _sibling_joined_values(
            _FakeH5(_fx_sibling_arrays()),
            "G",
            "sib",
            _fx_join_ds()["assets"]["sib"],
            N_REC,
            ["/G/qual"],
            lambda paths: {p: _fx_arrays()[p] for p in paths},
        )
        assert matched.tolist() == [True, True, False, True, True]
        assert values["/G/qual"][[0, 1, 3, 4]].tolist() == [1, 1, 0, 1]

    def test_duplicate_composite_keys_raise(self):
        sib = _fx_sibling_arrays()
        sib["/G/key_a"][4], sib["/G/key_b"][4] = 1, 0  # duplicates (1, 0)
        ds = _fx_join_ds(
            filters=[{"asset": "sib", "dataset": "/{group}/qual", "op": "eq", "value": 1}]
        )
        with pytest.raises(ValueError, match="duplicate key"):
            _read_group(
                _FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid(), siblings={"sib": _FakeH5(sib)}
            )


def _fx_sibling_coord_ds(**extra):
    ds = _fx_join_ds(**extra)
    ds["coordinates"] = {
        "latitude": {"asset": "sib", "path": "/{group}/lat"},
        "longitude": {"asset": "sib", "path": "/{group}/lon"},
        "level": "recs",
    }
    return ds


class TestAssetSourcedCoordinates:
    def test_validates(self):
        validate_config(_cfg(_fx_sibling_coord_ds(), source="wf"))

    def test_requires_a_declared_asset(self):
        ds = _fx_sibling_coord_ds()
        ds["coordinates"]["latitude"]["asset"] = "nope"
        _rejects(ds, "not declared in data_source.assets")

    def test_requires_the_vlen_route(self):
        ds = _fx_sibling_coord_ds()
        del ds["coordinates"]["level"]
        ds["variables"] = {"wf": "/{group}/wf"}
        _rejects(ds, "require coordinates.level")

    def test_both_coordinates_share_the_form(self):
        ds = _fx_sibling_coord_ds()
        ds["coordinates"]["longitude"] = "/{group}/lon"
        _rejects(ds, "both be plain paths or both")

    def test_full_read_places_records_by_the_sibling(self):
        df = _read_group(
            _FakeH5(_fx_arrays()),
            "G",
            _fx_sibling_coord_ds(),
            1,
            _LatGrid(),
            siblings={"sib": _FakeH5(_fx_sibling_arrays())},
        )
        # Shard 1 = lat >= 11.5 by the SIBLING's lat: records 3 and 4 (record 2
        # has no sibling row and so no position).
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[3:].reshape(-1))

    def test_planned_matches_full(self):
        kw = {"siblings": {"sib": _FakeH5(_fx_sibling_arrays())}}
        full = _read_group(_FakeH5(_fx_arrays()), "G", _fx_sibling_coord_ds(), 0, _LatGrid(), **kw)
        planned = _read_group(
            _FakeH5(_fx_arrays()),
            "G",
            _fx_sibling_coord_ds(read_plan={"spatial_index": "recs", "pad": 0}),
            0,
            _LatGrid(),
            **kw,
        )
        pd.testing.assert_frame_equal(full, planned)
        np.testing.assert_array_equal(full["wf"].to_numpy(), FX_WF[:2].reshape(-1))

    def test_missing_sibling_fails_the_group(self, caplog):
        # Raised before the join: no NaN-fill warning naming internal keys, and
        # no warn-once flag set on the handle (PR #680 review).
        h5obj = _FakeH5(_fx_arrays())
        with (
            caplog.at_level(logging.WARNING),
            pytest.raises(ValueError, match="asset 'sib', which sources coordinates.latitude"),
        ):
            _read_group(h5obj, "G", _fx_sibling_coord_ds(), 0, _OneShardGrid())
        assert not caplog.records
        assert not hasattr(h5obj, "_zagg_asset_nan_warned")

    def test_empty_primary_returns_none_before_the_join(self):
        # Zero records: no sibling read and no missing-handle raise, as the
        # vlen route returns before touching a sibling (PR #680 review).
        arrays = {p: a[:0] for p, a in _fx_arrays().items()}
        sib_reads = []

        class _Sib(_FakeH5):
            def readDatasets(self, datasets):  # noqa: N802
                sib_reads.append(datasets)
                return super().readDatasets(datasets)

        ds = _fx_sibling_coord_ds(
            filters=[{"asset": "sib", "dataset": "/{group}/qual", "op": "eq", "value": 1}]
        )
        for siblings in ({"sib": _Sib(_fx_sibling_arrays())}, None):
            io_stats = {}
            out = _read_group(
                _FakeH5(arrays), "G", ds, 0, _OneShardGrid(), io_stats=io_stats, siblings=siblings
            )
            assert out is None and io_stats["obs_read"] == 0
        assert sib_reads == []

    def test_the_join_is_shared_with_other_consumers(self, monkeypatch):
        import zagg.processing.read_vlen as rv

        calls = []
        real = rv._sibling_joined_values

        def _spy(*args, **kwargs):
            calls.append(args[2])
            return real(*args, **kwargs)

        monkeypatch.setattr(rv, "_sibling_joined_values", _spy)
        ds = _fx_sibling_coord_ds(
            filters=[{"asset": "sib", "dataset": "/{group}/qual", "op": "eq", "value": 1}]
        )
        ds["levels"]["recs"]["variables"]["qual"] = {"asset": "sib", "path": "/{group}/qual"}
        _read_group(
            _FakeH5(_fx_arrays()),
            "G",
            ds,
            0,
            _OneShardGrid(),
            siblings={"sib": _FakeH5(_fx_sibling_arrays())},
        )
        assert calls == ["sib"]


class TestDispatch:
    def test_gedi_template_never_enters_the_fixed_route(self, monkeypatch):
        import zagg.processing.read_fixed as rf
        from zagg.config import default_config

        monkeypatch.setattr(rf, "_fixed_read_group", lambda *a, **k: pytest.fail("fixed route"))
        cfg = default_config("gedi01b_waveform_healpix_hive")
        assert fixed_width_level(cfg.data_source) is None
        # The vlen route is reached: its first record-level read fails on the
        # empty stub, which is the KeyError the ragged route raises.
        with pytest.raises(KeyError, match="latitude_bin0"):
            _read_group(_FakeH5({}), "BEAM0000", cfg.data_source, 0, _OneShardGrid())

    def test_windowing_rejects_an_axes_time_field(self):
        cfg = _cfg(_fx_ds(), source="wf")
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "wf",
            "epoch": "2003-01-01T00:00:00Z",
        }
        with pytest.raises(ValueError, match="is an 'axes' or 'synthesize' variable"):
            validate_config(cfg)

    def test_the_swath_form_is_refused_at_read_time(self):
        ds = _swath_ds()
        with pytest.raises(ValueError, match="swath"):
            _read_group(_FakeH5(_fx_arrays()), "G", ds, 0, _OneShardGrid())

    @staticmethod
    def _stub_read_fn(monkeypatch, arrays, reads):
        """Swap InlineIndex's compiled seam for a plain-array ``read_fn``."""
        from zagg.index.inline import InlineIndex

        def _factory(self, h5obj, *, planned=True):
            def read_fn(path, hyperslice=None):
                reads.append((path, hyperslice))
                arr = arrays[path]
                return arr if hyperslice is None else arr[slice(*hyperslice[0])]

            read_fn.deferred_lines = set()
            return read_fn

        monkeypatch.setattr(InlineIndex, "_chunk_aligned_read_fn", _factory)

    @pytest.mark.parametrize("ds_fn", [_fx_ds, _fx_planned_ds])
    def test_inline_backend_reaches_the_fixed_route(self, monkeypatch, ds_fn):
        # PR #680 review: InlineIndex.read_group has its own coordinates.level
        # dispatch, which sent every record-level source to the vlen route
        # (KeyError 'index_beg' on a width link). It must mirror _read_group's.
        from zagg.index.inline import InlineIndex

        reads = []
        self._stub_read_fn(monkeypatch, _fx_arrays(), reads)
        df = InlineIndex().read_group(_FakeH5(_fx_arrays()), "G", ds_fn(), 1, _LatGrid())
        ref = _read_group(_FakeH5(_fx_arrays()), "G", ds_fn(), 1, _LatGrid())
        pd.testing.assert_frame_equal(df, ref)
        np.testing.assert_array_equal(df["wf"].to_numpy(), FX_WF[2:].reshape(-1))
        if ds_fn is _fx_planned_ds:
            # The 2-D waveform's record rows go through the compiled seam.
            assert ("/G/wf", [(2, 5)]) in reads

    def test_inline_backend_keeps_vlen_sources_on_the_vlen_route(self, monkeypatch):
        import zagg.processing.read_fixed as rf
        import zagg.processing.read_vlen as rv
        from zagg.index.inline import InlineIndex

        ds = _fx_ds()
        ds["levels"]["recs"]["link"] = {"to": "samples", "index_beg": "/x", "count": "/y"}
        sentinel = object()
        monkeypatch.setattr(rf, "_fixed_read_group", lambda *a, **k: pytest.fail("fixed route"))
        monkeypatch.setattr(rv, "_vlen_read_group", lambda *a, **k: sentinel)
        self._stub_read_fn(monkeypatch, {}, [])
        assert InlineIndex().read_group(_FakeH5({}), "G", ds, 0, _OneShardGrid()) is sentinel

    def test_inline_prebuild_covers_the_fixed_read_set(self, monkeypatch):
        import zagg.index.inline as inline_mod
        from zagg.index.inline import InlineIndex

        arrays = _fx_arrays()
        built = []

        def _spy(h5obj, path):
            arrays[path]  # a dataset this granule does not hold raises, as a real walk does
            built.append(path)
            return object()

        monkeypatch.setattr(inline_mod, "build_chunk_map", _spy)
        h5obj = _FakeH5(arrays)
        h5obj.resource = "s3://bucket/GLAH01_granule.H5"
        idx = InlineIndex(write_back=True, store="s3://bucket/manifests")
        ds = _fx_sibling_coord_ds(read_plan={"spatial_index": "recs"})
        idx._prebuild_group_maps(h5obj, "G", ds)
        assert {"/G/wf", "/G/rng_start", "/G/gate"} <= set(built)
        # The sibling coordinates and the RIGHT join keys live in the paired
        # granule, which this backend never opens. The LEFT keys are on the
        # primary, and the prebuild does not map them either: a known gap,
        # acceptable because the join key is a record-rate read the inline
        # backend serves from the handle's own B-tree without a chunk map.
        assert not any("key" in p or p.endswith(("/lat", "/lon")) for p in built)
