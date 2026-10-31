"""Tests for the fixed-width 2-D record-array modality (issue #678).

Phase 1: the grammar — ``link.width``, ``axes``, ``synthesize: offsets`` /
``index``, base-level ``{path, axes}`` coordinates, and list-form composite
join keys — through ``validate_config``.
"""

import copy

import pytest

from zagg.config import PipelineConfig, validate_config

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


def _cfg(ds):
    return PipelineConfig(
        data_source={"reader": "h5coro", "groups": ["Data_40HZ"], **ds},
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {"count": {"function": "len", "source": "rx_wf"}},
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

    def test_offsets_constant_step_works_on_a_ragged_link(self):
        # The step form needs no width: it is the old ``arange`` proposal.
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {
            "to": "samples",
            "index_beg": "/{group}/beg",
            "count": "/{group}/cnt",
        }
        ds["variables"]["rx_wf"] = "/{group}/wf"
        ds["variables"]["range"]["offsets"] = -0.15
        validate_config(_cfg(ds))

    def test_offsets_dataset_needs_the_width_link(self):
        ds = _fixed_ds()
        ds["levels"]["shots"]["link"] = {
            "to": "samples",
            "index_beg": "/{group}/beg",
            "count": "/{group}/cnt",
        }
        ds["variables"]["rx_wf"] = "/{group}/wf"
        _rejects(ds, "an 'offsets' dataset has one entry per sample of a fixed width")

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
