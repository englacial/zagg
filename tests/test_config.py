"""Tests for the YAML pipeline configuration system."""

import json
import re
from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest

from zagg.config import (
    PipelineConfig,
    _segment_variable_names,
    _validate_output_kind,
    default_config,
    evaluate_expression,
    get_agg_fields,
    get_base_level,
    get_child_order,
    get_chunk_precompute,
    get_coords,
    get_data_vars,
    get_filters,
    get_handoff,
    get_levels,
    get_output_signature,
    get_pipeline_type,
    get_sharded,
    get_store_path,
    load_config,
    load_config_from_dict,
    output_field_signature,
    resolve_function,
    validate_config,
)
from zagg.processing import calculate_cell_statistics
from zagg.time_axis import TOC_NO_CLOCK_ERROR

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def atl06_yaml(tmp_path):
    """Path to the built-in atl06.yaml (copied to tmp for load_config tests)."""
    from importlib import resources

    import zagg.configs

    ref = resources.files(zagg.configs).joinpath("atl06.yaml")
    text = ref.read_text(encoding="utf-8")
    p = tmp_path / "atl06.yaml"
    p.write_text(text)
    return str(p)


@pytest.fixture
def atl06_config():
    return default_config("atl06")


@pytest.fixture
def synthetic_df():
    return pd.DataFrame(
        {
            "h_li": np.array([120.5, 118.3, 122.1, 119.7, 121.0], dtype=np.float32),
            "s_li": np.array([0.05, 0.10, 0.03, 0.08, 0.06], dtype=np.float32),
        }
    )


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


class TestLoading:
    def test_load_yaml(self, atl06_yaml):
        cfg = load_config(atl06_yaml)
        assert isinstance(cfg, PipelineConfig)
        assert cfg.data_source
        assert cfg.aggregation
        assert cfg.output

    def test_all_sections_present(self, atl06_config):
        assert "groups" in atl06_config.data_source
        assert "variables" in atl06_config.aggregation
        assert "coordinates" in atl06_config.aggregation
        assert "grid" in atl06_config.output
        assert atl06_config.output["grid"]["type"] == "healpix"
        assert atl06_config.output["grid"]["child_order"] == 12


# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------


class TestDefaultConfig:
    def test_default_atl06(self):
        cfg = default_config("atl06")
        assert cfg.data_source["reader"] == "h5coro"
        assert len(cfg.data_source["groups"]) == 6

    def test_nonexistent_raises(self):
        with pytest.raises(FileNotFoundError):
            default_config("nonexistent")


# ---------------------------------------------------------------------------
# Pipeline type (issue #12 Phase 0)
# ---------------------------------------------------------------------------


class TestPipelineType:
    def test_default_is_spatial_no_key(self, atl06_config):
        # Existing configs (no ``pipeline`` key) keep defaulting to spatial,
        # preserving the back-compat contract for every shipped YAML.
        assert get_pipeline_type(atl06_config) == "spatial"

    def test_explicit_spatial(self):
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "spatial"},
                "data_source": {"reader": "h5coro"},
                "aggregation": {"variables": {"x": {"source": "h", "function": "np.mean"}}},
                "output": {"store": "."},
            }
        )
        assert get_pipeline_type(cfg) == "spatial"

    def test_unknown_type_raises(self):
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "bogus"},
                "data_source": {"reader": "h5coro"},
                "aggregation": {},
                "output": {},
            }
        )
        with pytest.raises(ValueError, match="pipeline.type must be one of"):
            get_pipeline_type(cfg)

    def test_validate_accepts_well_formed_temporal(self):
        # Phase 5: a temporal config with the four required per-variable spec
        # keys validates (the spatial grid cross-checks are skipped entirely).
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "max_t2m": {
                            "variable": "T2M",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                            "mask": "ais",
                        }
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )
        validate_config(cfg)  # no raise

    def test_validate_temporal_missing_spec_key_raises(self):
        # A temporal variable lacking a required spec key fails at load time
        # rather than silently shipping.
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3"},
                "aggregation": {"variables": {"x": {"variable": "T2M"}}},
                "output": {"store": "."},
            }
        )
        with pytest.raises(ValueError, match="missing required key"):
            validate_config(cfg)

    def test_validate_temporal_requires_variables(self):
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "event"},
                "data_source": {"reader": "xarray_s3"},
                "aggregation": {"coordinates": {}},
                "output": {"store": "."},
            }
        )
        with pytest.raises(ValueError, match="aggregation.variables"):
            validate_config(cfg)

    def test_pipeline_must_be_mapping(self):
        cfg = PipelineConfig(pipeline="spatial")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="pipeline must be a mapping"):
            get_pipeline_type(cfg)


class TestCollectionOptions:
    """``data_source.collections`` mapping form + reader options (issue #213 Phase 3)."""

    @staticmethod
    def _cfg(collections):
        return load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": collections},
                "aggregation": {
                    "variables": {
                        "max_t2m": {
                            "variable": "T2M",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                        }
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )

    def test_list_form_normalizes_to_empty_options(self):
        from zagg.config import collection_options

        cfg = self._cfg(["merra2_slv", "merra2_flx"])
        validate_config(cfg)
        assert collection_options(cfg) == {"merra2_slv": {}, "merra2_flx": {}}

    def test_mapping_form_carries_options_and_null_is_empty(self):
        from zagg.config import collection_options

        opts = {"time_offset": "-30min", "resample": {"freq": "3h", "how": "sum", "scale": 3600}}
        cfg = self._cfg({"merra2_slv": None, "merra2_flx": opts})
        validate_config(cfg)
        assert collection_options(cfg) == {"merra2_slv": {}, "merra2_flx": opts}

    def test_unknown_option_keys_pass_validation(self):
        # doi &c. are catalog metadata for downstream tooling.
        validate_config(self._cfg({"merra2": {"doi": "10.5067/EXAMPLE"}}))

    def test_resample_requires_freq(self):
        with pytest.raises(ValueError, match="resample.*freq"):
            validate_config(self._cfg({"merra2": {"resample": {"how": "sum"}}}))

    def test_resample_how_whitelisted(self):
        with pytest.raises(ValueError, match="resample.how"):
            validate_config(self._cfg({"merra2": {"resample": {"freq": "3h", "how": "median"}}}))

    def test_resample_scale_must_be_numeric(self):
        with pytest.raises(ValueError, match="resample.scale"):
            validate_config(self._cfg({"merra2": {"resample": {"freq": "3h", "scale": "3600"}}}))

    def test_time_offset_must_be_string(self):
        with pytest.raises(ValueError, match="time_offset"):
            validate_config(self._cfg({"merra2": {"time_offset": -30}}))

    def test_time_offset_must_parse(self):
        # A well-typed but unparseable offset fails at load, not on the worker.
        with pytest.raises(ValueError, match="time_offset is not a valid"):
            validate_config(self._cfg({"merra2": {"time_offset": "gibberish"}}))

    def test_resample_freq_must_be_string(self):
        with pytest.raises(ValueError, match="resample.freq"):
            validate_config(self._cfg({"merra2": {"resample": {"freq": 3}}}))

    def test_resample_freq_must_parse(self):
        with pytest.raises(ValueError, match="resample.freq is not a valid"):
            validate_config(self._cfg({"merra2": {"resample": {"freq": "notafreq"}}}))

    def test_derived_must_map_names_to_expressions(self):
        with pytest.raises(ValueError, match="derived"):
            validate_config(self._cfg({"merra2": {"derived": {"rainfall": 3}}}))

    def test_variables_must_be_name_list(self):
        with pytest.raises(ValueError, match="variables"):
            validate_config(self._cfg({"merra2": {"variables": "PRECLS"}}))

    def test_collection_entry_must_be_mapping_or_null(self):
        with pytest.raises(ValueError, match="mapping of options"):
            validate_config(self._cfg({"merra2": ["time_offset"]}))

    def test_coord_round_must_be_nonnegative_int(self):
        validate_config(self._cfg({"merra2": {"coord_round": 5}}))
        for bad in (True, -1, 5.0, "5"):
            with pytest.raises(ValueError, match="coord_round"):
                validate_config(self._cfg({"merra2": {"coord_round": bad}}))

    def test_credentials_provider_must_be_string(self):
        cfg = self._cfg(["merra2"])
        cfg.data_source["credentials_provider"] = ["gesdisc"]
        with pytest.raises(ValueError, match="credentials_provider"):
            validate_config(cfg)

    def test_spec_params_must_be_mapping(self):
        cfg = self._cfg(["merra2"])
        cfg.aggregation["variables"]["max_t2m"]["params"] = "knob=7"
        with pytest.raises(ValueError, match="params must be a mapping"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# ATL03 template
# ---------------------------------------------------------------------------


class TestATL03Template:
    @pytest.fixture
    def atl03_config(self):
        return default_config("atl03")

    def test_loads_and_validates(self, atl03_config):
        # default_config already runs validate_config; assert it round-trips.
        validate_config(atl03_config)
        assert atl03_config.data_source["reader"] == "h5coro"
        assert len(atl03_config.data_source["groups"]) == 6

    def test_scalar_variables(self, atl03_config):
        dvars = set(get_data_vars(atl03_config))
        assert dvars == {"count", "h_min", "h_max", "h_mean", "h_median", "h_variance"}

    def test_functions_resolve(self, atl03_config):
        for meta in get_agg_fields(atl03_config).values():
            assert "expression" not in meta  # scalar-only; non-scalar is #29
            resolve_function(meta["function"])  # raises on failure

    def test_confidence_filter_drops_tep(self, atl03_config):
        # The ATL03 template carries one structured TEP filter: keep photons where
        # signal_conf_ph[:, 0] (land surface type) != -2. TEP is uniform across
        # surface types per the ATL03 v3 data dictionary, so column 0 is
        # operationally equivalent to any other column for the TEP drop.
        filters = atl03_config.data_source["filters"]
        assert len(filters) == 1
        f = filters[0]
        assert f["value"] == -2
        assert f["op"] == "ne"  # keep signal_conf_ph != -2 (drop only TEP)
        assert f["column"] == 0
        assert f["dataset"].endswith("signal_conf_ph")

    def test_rectilinear_grid(self, atl03_config):
        grid = atl03_config.output["grid"]
        assert grid["type"] == "rectilinear"
        assert len(grid["bounds"]) == 4

    def test_multi_level_form_for_planned_reads(self, atl03_config):
        # Phase 6: the template declares the ``photons`` (base) + ``segments``
        # (coarse) levels and the link arrays so the #43 Phase C read_plan can
        # bound base-rate IO. Without this the ATL03 region runs OOM on Lambda
        # (245 MB-per-beam coord-read floor; see #43).
        ds = atl03_config.data_source
        assert ds["base_level"] == "photons"
        levels = ds["levels"]
        assert set(levels) == {"photons", "segments"}
        assert levels["photons"]["link"] is None
        seg_link = levels["segments"]["link"]
        assert seg_link["to"] == "photons"
        assert seg_link["index_beg"].endswith("ph_index_beg")
        assert seg_link["count"].endswith("segment_ph_cnt")
        assert seg_link["index_base"] == 1  # ATL03 ph_index_beg is 1-based

    def test_read_plan_targets_segments_level(self, atl03_config):
        rp = atl03_config.data_source["read_plan"]
        assert rp["spatial_index"] == "segments"
        assert rp["pad"] == 1


class TestWaveformChunkTemplate:
    """The worked chunk-precompute example (issue #30, item 1)."""

    @pytest.fixture
    def cfg(self):
        return default_config("atl03_waveform_chunk")

    def test_loads_and_validates(self, cfg):
        validate_config(cfg)
        assert cfg.data_source["reader"] == "h5coro"

    def test_declares_chunk_precompute(self, cfg):
        pc = get_chunk_precompute(cfg)
        assert set(pc) == {"chunk_offset", "chunk_gain"}
        for meta in pc.values():
            assert "expression" in meta
        # chunk_offset is the DEM anchor (dem_h, a segment-level variable);
        # chunk_gain is a spread over the photon heights (h_ph).
        assert pc["chunk_offset"]["source"] == "dem_h"
        assert pc["chunk_gain"]["source"] == "h_ph"

    def test_waveform_references_chunk_names(self, cfg):
        expr = cfg.aggregation["variables"]["waveform_counts"]["expression"]
        assert "chunk_offset" in expr
        assert "chunk_gain" in expr

    def test_records_offset_and_gain_scalar_fields(self, cfg):
        vars_ = cfg.aggregation["variables"]
        assert vars_["offset_h"]["expression"] == "chunk_offset"
        assert vars_["gain_h"]["expression"] == "chunk_gain"


# ---------------------------------------------------------------------------
# Function resolution
# ---------------------------------------------------------------------------


class TestResolveFunction:
    def test_min(self):
        assert resolve_function("min") is np.min

    def test_np_min(self):
        assert resolve_function("np.min") is np.min

    def test_np_quantile(self):
        assert resolve_function("np.quantile") is np.quantile

    def test_len(self):
        assert resolve_function("len") is len

    def test_count(self):
        assert resolve_function("count") is len

    def test_nonexistent_raises(self):
        with pytest.raises(ValueError):
            resolve_function("nonexistent_func")


# ---------------------------------------------------------------------------
# Expression evaluation
# ---------------------------------------------------------------------------


class TestEvaluateExpression:
    def test_simple_expression(self):
        cols = {"x": np.array([1.0, 2.0, 3.0])}
        result = evaluate_expression("np.mean(x)", cols)
        assert result == pytest.approx(2.0)

    def test_np_and_numpy(self):
        cols = {"x": np.array([4.0])}
        assert evaluate_expression("np.sqrt(x[0])", cols) == pytest.approx(2.0)
        assert evaluate_expression("numpy.sqrt(x[0])", cols) == pytest.approx(2.0)

    def test_len_available(self):
        cols = {"x": np.array([1.0, 2.0, 3.0])}
        assert evaluate_expression("float(len(x))", cols) == pytest.approx(3.0)

    def test_no_builtins(self):
        cols = {"x": np.array([1.0])}
        with pytest.raises(Exception):
            evaluate_expression("open('foo')", cols)
        with pytest.raises(Exception):
            evaluate_expression("__import__('os')", cols)

    def test_undefined_column(self):
        cols = {"x": np.array([1.0])}
        with pytest.raises(NameError):
            evaluate_expression("y + 1", cols)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_atl06_validates(self, atl06_config):
        # Should not raise
        validate_config(atl06_config)

    def _config_with_variables(self, variables):
        return PipelineConfig(
            data_source={"variables": variables},
            aggregation={
                "variables": {
                    "n": {"function": "len", "source": "h_li", "dtype": "int32"},
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_variable_column_form_accepted(self):
        cfg = self._config_with_variables(
            {"h_li": "/path", "conf_land": {"path": "/conf", "column": 0}}
        )
        validate_config(cfg)  # should not raise

    def test_variable_column_form_unknown_key_rejected(self):
        cfg = self._config_with_variables(
            {"h_li": "/path", "c": {"path": "/conf", "column": 0, "slice": 1}}
        )
        with pytest.raises(ValueError, match="unknown keys.*slice"):
            validate_config(cfg)

    def test_variable_column_form_bad_column_rejected(self):
        for column in (-1, "0", None, True):
            cfg = self._config_with_variables(
                {"h_li": "/path", "c": {"path": "/conf", "column": column}}
            )
            with pytest.raises(ValueError, match="column must be an integer"):
                validate_config(cfg)

    def test_variable_non_str_non_dict_rejected(self):
        cfg = self._config_with_variables({"h_li": "/path", "c": 7})
        with pytest.raises(ValueError, match="path string or a"):
            validate_config(cfg)

    def _config_with_field_attrs(self, attrs):
        return PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "n": {"function": "len", "source": "h_li", "dtype": "int32", "attrs": attrs},
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_field_attrs_accepted(self):
        validate_config(self._config_with_field_attrs({"stratum": "signal", "signal_threshold": 2}))

    def test_field_attrs_reserved_ragged_key_rejected(self):
        with pytest.raises(ValueError, match="reserved"):
            validate_config(self._config_with_field_attrs({"ragged": {}}))

    def test_field_attrs_non_str_key_rejected(self):
        with pytest.raises(ValueError, match="string keys"):
            validate_config(self._config_with_field_attrs({1: "x"}))

    def test_field_attrs_non_json_rejected(self):
        import numpy as np

        with pytest.raises(ValueError, match="JSON-serializable"):
            validate_config(self._config_with_field_attrs({"arr": np.zeros(2)}))

    def _config_with_composition(self, block, *, params=None, digest_kind="ragged", fill_value=0):
        """Composition field + a sibling digest field, for the attrs cross-checks."""
        return PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "h_tdigest_signal": {
                        "kind": digest_kind,
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "source": "h_li",
                        "inner_shape": [2],
                        "dtype": "float32",
                    },
                    "composition": {
                        "function": "zagg.stats.composition.pack_composition",
                        "source": "h_li",
                        "dtype": "uint64",
                        "fill_value": fill_value,
                        "params": {"threshold": 2} if params is None else params,
                        "attrs": {"composition": block},
                    },
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_composition_attrs_accepted(self):
        validate_config(
            self._config_with_composition({"of": "h_tdigest_signal", "threshold": 2})
        )  # should not raise

    def test_composition_fill_value_must_be_zero(self):
        # §3: a nonzero fill makes every unwritten cell report presence.
        with pytest.raises(ValueError, match="must declare fill_value: 0"):
            validate_config(
                self._config_with_composition(
                    {"of": "h_tdigest_signal", "threshold": 2}, fill_value=1
                )
            )
        with pytest.raises(ValueError, match="must declare fill_value: 0"):
            validate_config(
                self._config_with_composition(
                    {"of": "h_tdigest_signal", "threshold": 2}, fill_value=None
                )
            )

    def test_composition_spec_must_match_the_convention(self):
        # spec/lanes are writer-stamped (spec §3.3); a declaration that
        # disagrees with the module constants is rejected, not merged.
        with pytest.raises(ValueError, match="is not the convention this writer packs"):
            validate_config(
                self._config_with_composition(
                    {"of": "h_tdigest_signal", "threshold": 2, "spec": "zagg-composition/2"}
                )
            )

    def test_composition_lanes_must_match_lane_order(self):
        from zagg.stats.composition import LANES

        permuted = [LANES[1], LANES[0], *LANES[2:]]
        with pytest.raises(ValueError, match="lane order"):
            validate_config(
                self._config_with_composition(
                    {"of": "h_tdigest_signal", "threshold": 2, "lanes": permuted}
                )
            )
        with pytest.raises(ValueError, match="lane order"):
            validate_config(
                self._config_with_composition(
                    {"of": "h_tdigest_signal", "threshold": 2, "lanes": list(LANES[:5])}
                )
            )

    def test_composition_declared_spec_and_lanes_accepted_when_they_agree(self):
        from zagg.stats.composition import COMPOSITION_SPEC, LANES

        validate_config(
            self._config_with_composition(
                {
                    "of": "h_tdigest_signal",
                    "threshold": 2,
                    "spec": COMPOSITION_SPEC,
                    "lanes": list(LANES),
                }
            )
        )  # should not raise

    def test_composition_of_must_name_a_declared_field(self):
        with pytest.raises(ValueError, match="of 'h_tdigest_sginal' is not a declared"):
            validate_config(self._config_with_composition({"of": "h_tdigest_sginal"}))

    def test_composition_of_must_be_ragged(self):
        with pytest.raises(ValueError, match="must be a 'kind: ragged' digest field"):
            validate_config(
                self._config_with_composition({"of": "h_tdigest_signal"}, digest_kind="scalar")
            )

    def test_composition_of_cannot_be_itself(self):
        with pytest.raises(ValueError, match="names the composition field itself"):
            validate_config(self._config_with_composition({"of": "composition"}))

    def test_composition_threshold_must_match_params(self):
        with pytest.raises(ValueError, match="disagrees with params.threshold"):
            validate_config(
                self._config_with_composition({"of": "h_tdigest_signal", "threshold": 3})
            )

    def test_composition_attrs_without_block_is_inert(self):
        # A non-composition attrs block (or a non-mapping one) skips the checks.
        validate_config(self._config_with_field_attrs({"composition": "zagg-composition/1"}))

    def test_shipped_strata_config_validates(self):
        from importlib import resources

        from zagg.config import load_config

        cfg = load_config(
            str(resources.files("zagg.configs").joinpath("atl03_tdigest_strata_healpix.yaml"))
        )
        validate_config(cfg)

    def test_missing_source(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "bad": {"function": "min", "source": "nonexistent", "dtype": "float32"},
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="source.*nonexistent"):
            validate_config(cfg)

    def test_missing_weights_column(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "bad": {
                        "function": "average",
                        "source": "h_li",
                        "params": {"weights": "missing_col"},
                        "dtype": "float32",
                    },
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="missing_col"):
            validate_config(cfg)

    def test_expression_unknown_column(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "bad": {
                        "expression": "unknown_col + 1",
                        "dtype": "float32",
                    },
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="unknown_col"):
            validate_config(cfg)

    def test_function_and_expression_mutual_exclusion(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "bad": {
                        "function": "min",
                        "expression": "np.min(h_li)",
                        "source": "h_li",
                        "dtype": "float32",
                    },
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            validate_config(cfg)

    def test_neither_function_nor_expression(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {
                    "bad": {"source": "h_li", "dtype": "float32"},
                }
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="must specify"):
            validate_config(cfg)

    def test_missing_required_section(self):
        cfg = PipelineConfig(
            data_source={}, aggregation={"variables": {}}, output={"grid": {"type": "x"}}
        )
        with pytest.raises(ValueError, match="Missing required section"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Per-chunk precompute hook (issue #30, item 1)
# ---------------------------------------------------------------------------


def _cfg_with_precompute(precompute, variables=None):
    """Minimal valid config carrying an aggregation.chunk_precompute block."""
    return PipelineConfig(
        data_source={"variables": {"h_ph": "/{group}/h_ph"}},
        aggregation={
            "chunk_precompute": precompute,
            "variables": variables
            or {"h_min": {"function": "min", "source": "h_ph", "dtype": "float32"}},
        },
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestChunkPrecompute:
    def test_expression_entry_validates(self):
        cfg = _cfg_with_precompute(
            {"chunk_offset": {"expression": "np.floor(np.median(h_ph))", "source": "h_ph"}}
        )
        validate_config(cfg)  # should not raise
        assert list(get_chunk_precompute(cfg)) == ["chunk_offset"]

    def test_function_entry_validates(self):
        cfg = _cfg_with_precompute(
            {"chunk_median": {"function": "median", "source": "h_ph", "dtype": "float32"}}
        )
        validate_config(cfg)

    def test_get_chunk_precompute_empty_without_block(self, atl06_config):
        assert get_chunk_precompute(atl06_config) == {}

    def test_per_cell_expression_may_reference_precompute_name(self):
        # offset_h's expression is just the chunk_precompute name; it must validate.
        cfg = _cfg_with_precompute(
            {"chunk_offset": {"expression": "np.floor(np.median(h_ph))", "source": "h_ph"}},
            variables={"offset_h": {"expression": "chunk_offset", "dtype": "float32"}},
        )
        validate_config(cfg)

    def test_unknown_source_rejected(self):
        cfg = _cfg_with_precompute({"bad": {"function": "median", "source": "nonexistent"}})
        with pytest.raises(ValueError, match="chunk_precompute 'bad'.*nonexistent"):
            validate_config(cfg)

    def test_both_function_and_expression_rejected(self):
        cfg = _cfg_with_precompute(
            {
                "bad": {
                    "function": "median",
                    "expression": "np.median(h_ph)",
                    "source": "h_ph",
                }
            }
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            validate_config(cfg)

    def test_neither_function_nor_expression_rejected(self):
        cfg = _cfg_with_precompute({"bad": {"source": "h_ph"}})
        with pytest.raises(ValueError, match="must specify"):
            validate_config(cfg)

    def test_empty_name_rejected(self):
        cfg = _cfg_with_precompute({"   ": {"function": "median", "source": "h_ph"}})
        with pytest.raises(ValueError, match="non-empty strings"):
            validate_config(cfg)

    def test_expression_unknown_column_rejected(self):
        cfg = _cfg_with_precompute({"bad": {"expression": "np.median(missing_col)"}})
        with pytest.raises(ValueError, match="missing_col"):
            validate_config(cfg)

    def test_bad_dtype_rejected(self):
        cfg = _cfg_with_precompute(
            {"bad": {"function": "median", "source": "h_ph", "dtype": "not_a_dtype"}}
        )
        with pytest.raises(ValueError, match="not a valid.*numpy dtype"):
            validate_config(cfg)

    def test_non_mapping_block_rejected(self):
        cfg = _cfg_with_precompute(["not", "a", "mapping"])
        with pytest.raises(ValueError, match="must be a mapping"):
            validate_config(cfg)

    def test_name_colliding_with_column_rejected(self):
        # A precompute name equal to a data_source.variables column would shadow the
        # real column array with a 0-d scalar in the per-cell namespace merge.
        cfg = _cfg_with_precompute({"h_ph": {"expression": "np.median(h_ph)", "source": "h_ph"}})
        with pytest.raises(ValueError, match="chunk_precompute 'h_ph'.*collides"):
            validate_config(cfg)

    def test_name_colliding_with_leaf_id_rejected(self):
        cfg = _cfg_with_precompute({"leaf_id": {"expression": "np.median(h_ph)", "source": "h_ph"}})
        with pytest.raises(ValueError, match="chunk_precompute 'leaf_id'.*collides"):
            validate_config(cfg)

    def test_inter_precompute_reference_rejected_as_unknown_column(self):
        # Inter-precompute references are unsupported: chunk_gain referencing
        # chunk_offset is rejected as an unknown column (no chaining, single pass).
        cfg = _cfg_with_precompute(
            {
                "chunk_offset": {"expression": "np.median(h_ph)", "source": "h_ph"},
                "chunk_gain": {"expression": "chunk_offset + 1.0", "source": "h_ph"},
            }
        )
        with pytest.raises(ValueError, match="chunk_offset"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Structured filters (issue #43, Phase A)
# ---------------------------------------------------------------------------


def _cfg_with_filters(filters=None, quality_filter=None):
    """Minimal valid config with a custom data_source filter spec."""
    ds = {"variables": {"h_li": "/{group}/h_li"}}
    if filters is not None:
        ds["filters"] = filters
    if quality_filter is not None:
        ds["quality_filter"] = quality_filter
    return PipelineConfig(
        data_source=ds,
        aggregation={
            "variables": {
                "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            }
        },
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestFilters:
    def test_quality_filter_synthesizes_base_eq(self, atl06_config):
        filters = get_filters(atl06_config)
        assert len(filters) == 1
        f = filters[0]
        assert f["op"] == "eq"
        assert f["level"] is None
        assert f["column"] is None
        assert f["keep"] is True
        assert f["value"] == 0
        assert f["dataset"].endswith("atl06_quality_summary")

    def test_no_filters_returns_empty(self):
        cfg = _cfg_with_filters()
        assert get_filters(cfg) == []

    def test_explicit_filters_override_quality_filter(self):
        cfg = _cfg_with_filters(
            filters=[{"dataset": "/{group}/conf", "column": 0, "op": "ne", "value": 0}],
            quality_filter={"dataset": "/{group}/qs", "value": 0},
        )
        filters = get_filters(cfg)
        assert len(filters) == 1
        assert filters[0]["column"] == 0
        assert filters[0]["op"] == "ne"

    def test_normalize_set_op_keeps_values_list(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "in", "values": [2, 3, 4]}])
        f = get_filters(cfg)[0]
        assert f["values"] == [2, 3, 4]
        assert "value" not in f

    def test_normalize_keep_drop(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "eq", "value": 1, "keep": False}])
        assert get_filters(cfg)[0]["keep"] is False

    def test_expression_filter_normalized(self):
        cfg = _cfg_with_filters(filters=[{"expression": "h_li > 0"}])
        f = get_filters(cfg)[0]
        assert f["expression"] == "h_li > 0"
        assert f["level"] is None

    def test_unknown_op_rejected(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "between", "value": 1}])
        with pytest.raises(ValueError, match="unknown op"):
            validate_config(cfg)

    def test_column_must_be_int(self):
        cfg = _cfg_with_filters(
            filters=[{"dataset": "/d", "column": "land", "op": "ne", "value": 0}]
        )
        with pytest.raises(ValueError, match="must be an integer"):
            validate_config(cfg)

    def test_set_op_requires_values_list(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "in", "value": 3}])
        with pytest.raises(ValueError, match="requires a 'values' list"):
            validate_config(cfg)

    def test_scalar_op_requires_value(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "eq"}])
        with pytest.raises(ValueError, match="requires a scalar 'value'"):
            validate_config(cfg)

    def test_bad_value_type_rejected(self):
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "eq", "value": "x"}])
        with pytest.raises(ValueError, match="must be numeric"):
            validate_config(cfg)

    def test_missing_dataset_rejected(self):
        cfg = _cfg_with_filters(filters=[{"op": "eq", "value": 0}])
        with pytest.raises(ValueError, match="requires 'dataset'"):
            validate_config(cfg)

    def test_expression_with_level_rejected(self):
        cfg = _cfg_with_filters(filters=[{"expression": "h_li > 0", "level": "segment"}])
        with pytest.raises(ValueError, match="base-level only"):
            validate_config(cfg)

    def test_expression_with_op_rejected(self):
        cfg = _cfg_with_filters(filters=[{"expression": "h_li > 0", "op": "eq", "dataset": "/d"}])
        with pytest.raises(ValueError, match="take no 'op'"):
            validate_config(cfg)

    def test_bool_column_rejected(self):
        # bool is a subclass of int; filter column: true must be rejected.
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "column": True, "op": "eq", "value": 0}])
        with pytest.raises(ValueError, match="must be an integer"):
            validate_config(cfg)

    def test_bool_value_rejected(self):
        # bool is a subclass of int; filter value: true must be rejected.
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "eq", "value": True}])
        with pytest.raises(ValueError, match="must be numeric"):
            validate_config(cfg)

    def test_bool_in_values_rejected(self):
        # bool elements in a 'values' list must be rejected.
        cfg = _cfg_with_filters(filters=[{"dataset": "/d", "op": "in", "values": [0, True]}])
        with pytest.raises(ValueError, match="must be numeric"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Hierarchical levels and link validation (issue #43, Phase B)
# ---------------------------------------------------------------------------


def _minimal_two_level_ds(**overrides):
    """Return a minimal two-level data_source dict with one segment->photon link."""
    ds = {
        "reader": "h5coro",
        "groups": ["gt1l"],
        "coordinates": {"latitude": "/gt1l/ph_lat", "longitude": "/gt1l/ph_lon"},
        "variables": {"h": "/gt1l/h_ph"},
        "base_level": "photons",
        "levels": {
            "photons": {
                "path": "/{group}/heights",
                "coordinates": ["lat_ph", "lon_ph"],
                "variables": ["h_ph"],
                "link": None,
            },
            "segments": {
                "path": "/{group}/geolocation",
                "coordinates": ["reference_photon_lat", "reference_photon_lon"],
                "variables": ["signal_conf_ph"],
                "link": {
                    "to": "photons",
                    "index_beg": "/{group}/geolocation/ph_index_beg",
                    "count": "/{group}/geolocation/segment_ph_cnt",
                },
            },
        },
    }
    ds.update(overrides)
    return ds


def _cfg_with_levels(**overrides):
    ds = _minimal_two_level_ds(**overrides)
    return PipelineConfig(
        data_source=ds,
        aggregation={"variables": {"count": {"function": "len", "source": "h", "dtype": "int32"}}},
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestLevelsValidation:
    def test_valid_two_level_config(self):
        validate_config(_cfg_with_levels())

    def test_flat_form_still_valid(self, atl06_config):
        # Flat form (no levels/base_level) must still pass.
        assert get_levels(atl06_config) is None
        assert get_base_level(atl06_config) is None
        validate_config(atl06_config)

    def test_get_levels_and_base_level(self):
        cfg = _cfg_with_levels()
        levels = get_levels(cfg)
        assert levels is not None
        assert "photons" in levels
        assert "segments" in levels
        assert get_base_level(cfg) == "photons"

    def test_base_level_must_name_a_key(self):
        cfg = _cfg_with_levels(base_level="nonexistent")
        with pytest.raises(ValueError, match="not a key in levels"):
            validate_config(cfg)

    def test_link_to_must_name_a_key(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["to"] = "nonexistent"
        cfg = _cfg_with_levels(**ds)
        with pytest.raises(ValueError, match="not a key in levels"):
            validate_config(cfg)

    def test_link_to_must_name_a_key2(self):
        # Build config directly to avoid _cfg_with_levels merging issues
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["to"] = "nonexistent"
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="not a key in levels"):
            validate_config(cfg)

    def test_link_missing_required_field(self):
        ds = _minimal_two_level_ds()
        del ds["levels"]["segments"]["link"]["count"]
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="'count' is required"):
            validate_config(cfg)

    def test_link_unknown_field_rejected(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["bogus"] = "x"
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="unknown fields"):
            validate_config(cfg)

    def test_base_level_without_link_ok(self):
        # base_level is the only level allowed to have link: None.
        cfg = _cfg_with_levels()
        validate_config(cfg)
        assert cfg.data_source["levels"]["photons"]["link"] is None

    def test_non_base_level_without_link_rejected(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"] = None
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="must have a 'link'"):
            validate_config(cfg)

    def test_levels_missing_base_level_key_rejected(self):
        ds = _minimal_two_level_ds()
        del ds["base_level"]
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="base_level is required"):
            validate_config(cfg)

    def test_index_base_must_be_nonneg_int(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["index_base"] = -1
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="non-negative int"):
            validate_config(cfg)

    def test_reference_index_must_be_none(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["reference_index"] = "/some/path"
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="reserved"):
            validate_config(cfg)

    def test_self_link_rejected(self):
        # link.to == level name (self-reference) must raise ValueError
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["link"]["to"] = "segments"
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="cannot reference the level itself"):
            validate_config(cfg)

    def test_filter_level_not_in_levels_rejected(self):
        # A filter whose level names a nonexistent key must fail at validate time.
        ds = _minimal_two_level_ds()
        ds["filters"] = [
            {"level": "nonexistent", "dataset": "/{group}/flag", "op": "eq", "value": 0}
        ]
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="not a key in levels"):
            validate_config(cfg)


class TestSegmentLevelVariables:
    """Issue #30: a non-base level may declare ``variables`` as a ``{name: path}``
    mapping (a readable segment-level variable, e.g. ``dem_h``). It is validated
    like ``data_source.variables`` (string names -> non-empty path templates) and
    its names become valid column references for the aggregation."""

    def _cfg(self, seg_variables, **agg_overrides):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["variables"] = seg_variables
        agg = {"variables": {"c": {"function": "len", "dtype": "int32"}}}
        agg.update(agg_overrides)
        return PipelineConfig(
            data_source=ds,
            aggregation=agg,
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_mapping_form_valid(self):
        validate_config(self._cfg({"dem_h": "/{group}/geophys_corr/dem_h"}))

    def test_list_form_still_valid(self):
        # The documentation-only ``list[str]`` form is unchanged.
        validate_config(self._cfg(["signal_conf_ph"]))

    def test_segment_variable_usable_as_agg_source(self):
        # dem_h becomes a per-photon column, so an agg field may source it.
        validate_config(
            self._cfg(
                {"dem_h": "/{group}/geophys_corr/dem_h"},
                variables={"dem_mean": {"function": "mean", "source": "dem_h", "dtype": "float32"}},
            )
        )

    def test_segment_variable_usable_in_expression_filter(self):
        # A broadcast segment variable is a valid name in an ``expression`` filter,
        # consistent with agg/precompute expressions (issue #30).
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["variables"] = {"dem_h": "/{group}/geophys_corr/dem_h"}
        ds["filters"] = [{"expression": "dem_h > 100.0"}]
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        validate_config(cfg)

    def test_unknown_name_in_expression_filter_rejected(self):
        # A truly-undefined name in an expression filter is rejected at validate
        # time, like agg/precompute expressions.
        ds = _minimal_two_level_ds()
        ds["filters"] = [{"expression": "nope_col > 0"}]
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="nope_col"):
            validate_config(cfg)

    def test_segment_variable_usable_in_chunk_precompute(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["variables"] = {"dem_h": "/{group}/geophys_corr/dem_h"}
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={
                "chunk_precompute": {
                    "chunk_offset": {
                        "expression": "np.float32(np.median(dem_h))",
                        "source": "dem_h",
                    }
                },
                "variables": {"c": {"function": "len", "dtype": "int32"}},
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        validate_config(cfg)

    def test_empty_path_template_rejected(self):
        with pytest.raises(ValueError, match="path template must be a"):
            validate_config(self._cfg({"dem_h": ""}))

    def test_mapping_on_base_level_rejected(self):
        ds = _minimal_two_level_ds()
        ds["levels"]["photons"]["variables"] = {"h_ph": "/{group}/heights/h_ph"}
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="base level uses data_source.variables"):
            validate_config(cfg)

    def test_name_collision_with_base_variable_rejected(self):
        # ``h`` is already a data_source.variables column.
        with pytest.raises(ValueError, match="collides with a data_source.variables column"):
            validate_config(self._cfg({"h": "/{group}/geophys_corr/dem_h"}))

    def test_duplicate_segment_variable_across_levels_rejected(self):
        # Two non-base levels declaring the same name would silently overwrite each
        # other when broadcast into one per-photon column; reject the ambiguity.
        ds = _minimal_two_level_ds()
        ds["levels"]["segments"]["variables"] = {"dem_h": "/{group}/geophys_corr/dem_h"}
        ds["levels"]["coarse"] = {
            "path": "/{group}/other",
            "coordinates": [],
            "variables": {"dem_h": "/{group}/other/dem_h"},
            "link": {
                "to": "photons",
                "index_beg": "/{group}/other/ph_index_beg",
                "count": "/{group}/other/ph_cnt",
            },
        }
        cfg = PipelineConfig(
            data_source=ds,
            aggregation={"variables": {"c": {"function": "len", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match="already declared on another level"):
            validate_config(cfg)

    def test_worked_template_declares_dem_h_segment_variable(self):
        cfg = load_config("src/zagg/configs/atl03_waveform_chunk.yaml")
        validate_config(cfg)
        seg = cfg.data_source["levels"]["segments"]["variables"]
        assert isinstance(seg, dict)
        assert seg["dem_h"] == "/{group}/geophys_corr/dem_h"
        # chunk_offset is DEM-anchored.
        offset = cfg.aggregation["chunk_precompute"]["chunk_offset"]
        assert offset["source"] == "dem_h"
        assert "dem_h" in offset["expression"]


# ---------------------------------------------------------------------------
# Helper accessors
# ---------------------------------------------------------------------------


class TestAccessors:
    def test_get_agg_fields(self, atl06_config):
        fields = get_agg_fields(atl06_config)
        assert "count" in fields
        assert "h_mean" in fields
        assert fields["h_q50"]["params"]["q"] == 0.50

    def test_get_coords(self, atl06_config):
        # The shipped configs declare morton only (issue #304 phase 3 — the
        # legacy cell_ids declaration was removed with the D16 flip).
        coords = get_coords(atl06_config)
        assert "cell_ids" not in coords
        assert "morton" in coords

    def test_get_data_vars(self, atl06_config):
        dvars = get_data_vars(atl06_config)
        assert "count" in dvars
        assert "h_sigma" in dvars


# ---------------------------------------------------------------------------
# Equivalence with calculate_cell_statistics
# ---------------------------------------------------------------------------


def _dispatch_config_stat(name, meta, df):
    """Compute a single statistic using config metadata, mirroring calculate_cell_statistics."""
    if "function" in meta:
        func_name = meta["function"]
        source = meta.get("source", "h_li")
        values = df[source].values
        params = dict(meta.get("params", {}))

        if func_name in ("len", "count"):
            return len(values)

        func = resolve_function(func_name)

        # Resolve params: bare column name -> array, expression -> eval'd
        resolved = {}
        for k, v in params.items():
            if isinstance(v, str) and v in df.columns:
                resolved[k] = df[v].values
            elif isinstance(v, str) and any(c in v for c in df.columns):
                ns = {
                    "__builtins__": {},
                    "np": np,
                    "numpy": np,
                    **{c: df[c].values for c in df.columns},
                }
                resolved[k] = eval(v, ns)  # noqa: S307
            else:
                resolved[k] = v

        return float(func(values, **resolved))

    elif "expression" in meta:
        columns = {col: df[col].values for col in df.columns}
        return evaluate_expression(meta["expression"], columns)


class TestEquivalence:
    def test_expression_in_params(self, synthetic_df):
        """Param value '1.0 / s_li**2' is evaluated as an expression, not a column name."""
        meta = {
            "function": "average",
            "source": "h_li",
            "params": {"weights": "1.0 / s_li**2"},
        }
        result = _dispatch_config_stat("h_weighted", meta, synthetic_df)
        expected = np.average(
            synthetic_df["h_li"].values,
            weights=1.0 / synthetic_df["s_li"].values ** 2,
        )
        assert result == pytest.approx(expected, rel=1e-5)

    def test_config_matches_calculate_cell_statistics(self, atl06_config, synthetic_df):
        cell_data = {col: synthetic_df[col].values for col in synthetic_df.columns}
        expected = calculate_cell_statistics(cell_data)
        agg_fields = get_agg_fields(atl06_config)

        for name, meta in agg_fields.items():
            config_val = _dispatch_config_stat(name, meta, synthetic_df)
            exp_val = expected[name]

            assert config_val == pytest.approx(exp_val, rel=1e-5), (
                f"Mismatch for '{name}': config={config_val}, expected={exp_val}"
            )


# ---------------------------------------------------------------------------
# Roundtrip: YAML -> PipelineConfig -> dict -> PipelineConfig
# ---------------------------------------------------------------------------


class TestRoundtrip:
    def test_dict_roundtrip(self, atl06_config):
        d = asdict(atl06_config)
        restored = load_config_from_dict(d)
        assert restored.data_source == atl06_config.data_source
        assert restored.aggregation == atl06_config.aggregation
        assert restored.output == atl06_config.output

    def test_catalog_and_bounds_roundtrip(self):
        d = {
            "data_source": {
                "variables": {"h_li": "/path"},
                "reader": "h5coro",
                "groups": ["gt1l"],
                "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            },
            "aggregation": {
                "variables": {"count": {"function": "len", "source": "h_li", "dtype": "int32"}},
                "coordinates": {"cell_ids": {"dtype": "uint64"}},
            },
            "output": {"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
            "catalog": "my_catalog.json",
            "bounds": {"temporal": {"start_date": "2024-01-01", "end_date": "2024-06-01"}},
        }
        cfg = load_config_from_dict(d)
        assert cfg.catalog == "my_catalog.json"
        assert cfg.bounds["temporal"]["start_date"] == "2024-01-01"


# ---------------------------------------------------------------------------
# Output config helpers
# ---------------------------------------------------------------------------


class TestOutputHelpers:
    def test_get_child_order(self, atl06_config):
        assert get_child_order(atl06_config) == 12

    def test_get_child_order_missing(self):
        cfg = PipelineConfig(output={"grid": {"type": "healpix"}})
        with pytest.raises(ValueError, match="child_order"):
            get_child_order(cfg)

    def test_get_sharded_default_false(self, atl06_config):
        assert get_sharded(atl06_config) is False

    def test_get_sharded_true(self):
        cfg = PipelineConfig(
            output={
                "grid": {
                    "type": "healpix",
                    "parent_order": 6,
                    "child_order": 12,
                    "chunk_inner": 8,
                    "sharded": True,
                }
            }
        )
        assert get_sharded(cfg) is True

    def test_get_store_path(self):
        cfg = PipelineConfig(
            output={
                "store": "./test.zarr",
                "grid": {"type": "healpix", "parent_order": 6, "child_order": 12},
            }
        )
        assert get_store_path(cfg) == "./test.zarr"

    def test_get_store_path_none(self, atl06_config):
        assert get_store_path(atl06_config) is None

    def test_get_store_path_s3(self):
        cfg = PipelineConfig(
            output={
                "store": "s3://bucket/prefix.zarr",
                "grid": {"type": "healpix", "parent_order": 6, "child_order": 12},
            }
        )
        assert get_store_path(cfg) == "s3://bucket/prefix.zarr"


# ---------------------------------------------------------------------------
# Carrier handoff config helper + validation (issue #132)
# ---------------------------------------------------------------------------


def _cfg_with_handoff(handoff=None) -> PipelineConfig:
    """Minimal valid config; sets aggregation.handoff only when given."""
    aggregation = {"variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}}
    if handoff is not None:
        aggregation["handoff"] = handoff
    return PipelineConfig(
        data_source={"variables": {"h_li": "/path"}},
        aggregation=aggregation,
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestHandoff:
    def test_default_is_arrow(self, atl06_config):
        assert get_handoff(atl06_config) == "arrow"

    def test_explicit_pandas(self):
        assert get_handoff(_cfg_with_handoff("pandas")) == "pandas"

    def test_explicit_arrow(self):
        assert get_handoff(_cfg_with_handoff("arrow")) == "arrow"

    def test_valid_handoff_validates(self):
        validate_config(_cfg_with_handoff("pandas"))  # should not raise
        validate_config(_cfg_with_handoff("arrow"))

    def test_absent_handoff_validates(self):
        validate_config(_cfg_with_handoff())  # should not raise

    def test_invalid_handoff_rejected_at_load(self):
        with pytest.raises(ValueError, match=r"aggregation\.handoff must be 'pandas' or 'arrow'"):
            validate_config(_cfg_with_handoff("bogus"))

    def test_shipped_atl06_declares_arrow(self):
        # The shipped atl06 config now declares its carrier explicitly (issue #132).
        assert get_handoff(default_config("atl06")) == "arrow"

    def test_shipped_nullable_example_selects_pandas(self):
        # The nullable-source example ships with the pandas carrier and validates.
        cfg = default_config("atl06_nullable")  # default_config runs validate_config
        assert get_handoff(cfg) == "pandas"


class TestGranuleWorkersValidation:
    """``data_source.granule_workers`` (issue #180) is validated at submission,
    mirroring the worker's ``_granule_workers`` guard (same int>=1/bool-trap
    pattern as ``read_workers``)."""

    def _cfg(self, **ds_extra):
        cfg = _cfg_with_handoff()
        cfg.data_source.update(ds_extra)
        return cfg

    def test_absent_validates(self):
        validate_config(self._cfg())  # should not raise

    def test_valid_value_validates(self):
        validate_config(self._cfg(granule_workers=4))  # should not raise

    def test_bad_values_rejected_at_submission(self):
        for bad in (0, -1, 1.5, "2", True, False):
            with pytest.raises(ValueError, match=r"data_source\.granule_workers"):
                validate_config(self._cfg(granule_workers=bad))

    def test_sidecar_combos_pool_freely(self, tmp_path):
        # Every sidecar on_miss policy pools: the on_miss: build delegate
        # lazy-init race was fixed in h5coro-hidefix 0.3.1 (issue #180 review
        # finding 2), enforced by the pyproject pin — no config-level clamp.
        for on_miss in ("build", "fallback", "error"):
            index = {"backend": "sidecar", "store": str(tmp_path), "on_miss": on_miss}
            validate_config(self._cfg(granule_workers=2, index=index))
            validate_config(self._cfg(index=index))


# ---------------------------------------------------------------------------
# Output grid validation
# ---------------------------------------------------------------------------


class TestOutputGridValidation:
    def test_grid_must_be_dict(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}
            },
            output={"grid": "healpix"},
        )
        with pytest.raises(ValueError, match="output.grid must be a mapping"):
            validate_config(cfg)

    def test_grid_missing_type(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}
            },
            output={"grid": {"child_order": 12}},
        )
        with pytest.raises(ValueError, match="output.grid.type"):
            validate_config(cfg)

    def test_healpix_missing_child_order(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}
            },
            output={"grid": {"type": "healpix"}},
        )
        with pytest.raises(ValueError, match="child_order"):
            validate_config(cfg)

    def test_healpix_missing_parent_order(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={
                "variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}
            },
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="parent_order"):
            validate_config(cfg)


def _cfg_with_grid_knobs(grid_type="healpix", **grid_extra) -> PipelineConfig:
    """Minimal valid config; extra output.grid keys applied when given."""
    grid: dict = {"type": grid_type, "parent_order": 6, "child_order": 12}
    if grid_type == "rectilinear":
        grid = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
    grid.update(grid_extra)
    return PipelineConfig(
        data_source={
            "reader": "h5coro",
            "groups": ["gt1l"],
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h_li": "/path"},
        },
        aggregation={"variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}},
        output={"grid": grid},
    )


class TestCellIdsKnobs:
    """Issue #304 phase 3: the issue #135 ``cell_ids_encoding`` knob is
    retired (morton stored, NESTED derived); ``emit_cell_ids`` is the one
    remaining transition hatch."""

    def test_retired_knob_gets_pointed_error(self):
        # Any value — including the formerly-valid ones — errors with the
        # migration pointer, never a silent ignore.
        for value in ("nested", "morton", "bogus"):
            with pytest.raises(ValueError, match="cell_ids_encoding was retired"):
                validate_config(_cfg_with_grid_knobs(cell_ids_encoding=value))

    def test_absent_and_null_knob_validate(self):
        validate_config(_cfg_with_grid_knobs())
        # YAML `cell_ids_encoding:` (explicit null) behaves like absent.
        cfg = _cfg_with_grid_knobs()
        cfg.output["grid"]["cell_ids_encoding"] = None
        validate_config(cfg)

    def test_emit_cell_ids_accessor_and_validation(self):
        from zagg.config import get_emit_cell_ids

        assert get_emit_cell_ids(_cfg_with_grid_knobs()) is False
        cfg = _cfg_with_grid_knobs(emit_cell_ids=True)
        validate_config(cfg)
        assert get_emit_cell_ids(cfg) is True

    def test_emit_cell_ids_rejects_non_boolean(self):
        with pytest.raises(ValueError, match="emit_cell_ids must be a boolean"):
            validate_config(_cfg_with_grid_knobs(emit_cell_ids="yes"))

    def test_emit_cell_ids_rejected_on_rectilinear(self):
        # Rectilinear grids have no cell_ids coordinate; the flag would
        # silently do nothing, so a true value is rejected at load.
        with pytest.raises(ValueError, match="only applies to healpix"):
            validate_config(_cfg_with_grid_knobs(emit_cell_ids=True, grid_type="rectilinear"))

    def test_rectilinear_without_knobs_still_validates(self):
        validate_config(_cfg_with_grid_knobs(grid_type="rectilinear"))

    def test_legacy_indexing_scheme_key_is_descriptive_only(self):
        # The shipped configs carry output.grid.indexing_scheme: nested, which
        # no code reads; any other value must fail loudly.
        cfg = _cfg_with_grid_knobs()
        cfg.output["grid"]["indexing_scheme"] = "nested"
        validate_config(cfg)  # the shipped value stays valid
        cfg.output["grid"]["indexing_scheme"] = "morton"
        with pytest.raises(ValueError, match="declared coordinate is morton"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Bounds validation
# ---------------------------------------------------------------------------


class TestBoundsValidation:
    def test_valid_bounds(self, atl06_config):
        atl06_config.bounds = {
            "temporal": {"start_date": "2024-01-06", "end_date": "2024-04-07"},
            "spatial": {"bbox": [-180, -90, 180, -60]},
        }
        validate_config(atl06_config)

    def test_temporal_only(self, atl06_config):
        atl06_config.bounds = {"temporal": {"start_date": "2024-01-01", "end_date": "2024-06-01"}}
        validate_config(atl06_config)

    def test_spatial_only(self, atl06_config):
        atl06_config.bounds = {"spatial": {"bbox": [-180, -90, 180, -60]}}
        validate_config(atl06_config)

    def test_unknown_bounds_key(self, atl06_config):
        atl06_config.bounds = {
            "temporal": {"start_date": "2024-01-01", "end_date": "2024-06-01"},
            "foo": "bar",
        }
        with pytest.raises(ValueError, match="Unknown bounds keys"):
            validate_config(atl06_config)

    def test_temporal_missing_dates(self, atl06_config):
        atl06_config.bounds = {"temporal": {"start_date": "2024-01-01"}}
        with pytest.raises(ValueError, match="start_date and end_date"):
            validate_config(atl06_config)

    def test_none_bounds_ok(self, atl06_config):
        atl06_config.bounds = None
        validate_config(atl06_config)


# ---------------------------------------------------------------------------
# Non-scalar output kind declaration (issue #29, phase 1)
# ---------------------------------------------------------------------------


def _vector_config(var_meta: dict) -> PipelineConfig:
    """Build a minimal config whose single agg variable carries ``var_meta``."""
    cfg = PipelineConfig(
        data_source={
            "reader": "h5coro",
            "groups": ["gt1l"],
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h_li": "/path"},
        },
        aggregation={"variables": {"hist": {"source": "h_li", **var_meta}}},
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )
    return cfg


class TestOutputKind:
    def test_scalar_default_backward_compatible(self, atl06_config):
        """Existing scalar configs declare no kind and still validate."""
        validate_config(atl06_config)
        for meta in get_agg_fields(atl06_config).values():
            assert "kind" not in meta

    def test_vector_int_trailing_shape(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": 64})
        validate_config(cfg)

    def test_vector_list_trailing_shape(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": [16, 2]})
        validate_config(cfg)

    def test_unknown_kind_rejected(self):
        cfg = _vector_config({"function": "min", "kind": "matrix"})
        with pytest.raises(ValueError, match="output kind 'matrix' is not supported"):
            validate_config(cfg)

    def test_ragged_requires_inner_shape(self):
        cfg = _vector_config({"function": "min", "kind": "ragged"})
        with pytest.raises(ValueError, match="requires 'inner_shape'"):
            validate_config(cfg)

    def test_vector_requires_trailing_shape(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector"})
        with pytest.raises(ValueError, match="requires 'trailing_shape'"):
            validate_config(cfg)

    def test_scalar_rejects_trailing_shape(self):
        cfg = _vector_config({"function": "min", "trailing_shape": 8})
        with pytest.raises(ValueError, match="only valid for kind 'vector'"):
            validate_config(cfg)

    def test_trailing_shape_must_be_positive(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": 0})
        with pytest.raises(ValueError, match="positive"):
            validate_config(cfg)

    def test_trailing_shape_rejects_bool(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": True})
        with pytest.raises(ValueError, match="positive"):
            validate_config(cfg)

    def test_trailing_shape_rejects_empty(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": []})
        with pytest.raises(ValueError, match="at least one dimension"):
            validate_config(cfg)

    def test_trailing_shape_bad_type(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": "64"})
        with pytest.raises(ValueError, match="int or a sequence of ints"):
            validate_config(cfg)

    def test_trailing_shape_list_bad_element_type(self):
        cfg = _vector_config(
            {"function": "histogram", "kind": "vector", "trailing_shape": [16, "x"]}
        )
        with pytest.raises(ValueError, match="positive"):
            validate_config(cfg)

    def test_trailing_shape_list_zero_element(self):
        cfg = _vector_config({"function": "histogram", "kind": "vector", "trailing_shape": [16, 0]})
        with pytest.raises(ValueError, match="positive"):
            validate_config(cfg)

    def test_trailing_shape_list_bool_element(self):
        cfg = _vector_config(
            {"function": "histogram", "kind": "vector", "trailing_shape": [16, True]}
        )
        with pytest.raises(ValueError, match="positive"):
            validate_config(cfg)

    def test_vector_expression_allowed(self):
        """A vector field may be driven by an expression (issue #29)."""
        cfg = _vector_config(
            {
                "expression": "np.array([np.min(h_li), np.max(h_li)])",
                "kind": "vector",
                "trailing_shape": 2,
            }
        )
        validate_config(cfg)

    def test_vector_len_rejected(self):
        """``len`` short-circuits to a scalar count; kind 'vector' is nonsensical."""
        cfg = _vector_config({"function": "len", "kind": "vector", "trailing_shape": 4})
        with pytest.raises(ValueError, match="cannot be combined with kind 'vector'"):
            validate_config(cfg)

    def test_vector_count_rejected(self):
        cfg = _vector_config({"function": "count", "kind": "vector", "trailing_shape": 4})
        with pytest.raises(ValueError, match="cannot be combined with kind 'vector'"):
            validate_config(cfg)

    def test_invalid_dtype_rejected(self):
        cfg = _vector_config({"function": "min", "dtype": "not_a_dtype"})
        with pytest.raises(ValueError, match="not a valid"):
            validate_config(cfg)

    def test_valid_dtypes_accepted(self):
        for dt in ("float32", "int32", "uint64", "float64"):
            cfg = _vector_config({"function": "min", "dtype": dt})
            validate_config(cfg)


class TestGetOutputSignature:
    def test_scalar_signature(self):
        sig = get_output_signature({"function": "min", "dtype": "float32"})
        assert sig == {
            "kind": "scalar",
            "trailing_shape": (),
            "inner_shape": (),
            "dtype": "float32",
            "resolution": "cell",
            "location": None,
            "weights": None,
            "temporal": None,
        }

    def test_scalar_default_dtype_none(self):
        sig = get_output_signature({"function": "min"})
        assert sig == {
            "kind": "scalar",
            "trailing_shape": (),
            "inner_shape": (),
            "dtype": None,
            "resolution": "cell",
            "location": None,
            "weights": None,
            "temporal": None,
        }

    def test_vector_int_signature(self):
        sig = get_output_signature({"kind": "vector", "trailing_shape": 64, "dtype": "float32"})
        assert sig == {
            "kind": "vector",
            "trailing_shape": (64,),
            "inner_shape": (),
            "dtype": "float32",
            "resolution": "cell",
            "location": None,
            "weights": None,
            "temporal": None,
        }

    def test_vector_list_signature(self):
        sig = get_output_signature({"kind": "vector", "trailing_shape": [16, 2]})
        assert sig["kind"] == "vector"
        assert sig["trailing_shape"] == (16, 2)
        assert sig["inner_shape"] == ()


# ---------------------------------------------------------------------------
# resolution attribute (issue #30 item 2)
# ---------------------------------------------------------------------------


def _cfg_with_field(meta):
    """Minimal valid config carrying a single agg field ``f`` with ``meta``."""
    return PipelineConfig(
        data_source={"variables": {"h_ph": "/{group}/h_ph"}},
        aggregation={"variables": {"f": {"source": "h_ph", **meta}}},
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestResolutionAttribute:
    def test_resolution_defaults_to_cell(self):
        sig = get_output_signature({"function": "min"})
        assert sig["resolution"] == "cell"

    def test_resolution_cell_explicit_validates(self):
        cfg = _cfg_with_field({"function": "min", "resolution": "cell"})
        validate_config(cfg)  # should not raise
        assert get_output_signature(get_agg_fields(cfg)["f"])["resolution"] == "cell"

    def test_scalar_field_may_be_resolution_chunk(self):
        cfg = _cfg_with_field({"expression": "chunk_anchor", "resolution": "chunk"})
        # add the referenced chunk_precompute so the per-cell expression validates.
        cfg.aggregation["chunk_precompute"] = {
            "chunk_anchor": {"expression": "np.median(h_ph)", "source": "h_ph"}
        }
        validate_config(cfg)
        assert get_output_signature(get_agg_fields(cfg)["f"])["resolution"] == "chunk"

    def test_bad_resolution_rejected(self):
        cfg = _cfg_with_field({"function": "min", "resolution": "block"})
        with pytest.raises(ValueError, match="resolution 'block' is not supported"):
            validate_config(cfg)

    def test_vector_field_may_be_resolution_chunk(self):
        # issue #82: a kind: vector resolution: chunk companion is now accepted;
        # trailing_shape is validated exactly as for a cell-resolution vector.
        cfg = _cfg_with_field(
            {
                "kind": "vector",
                "trailing_shape": 4,
                "expression": "chunk_profile",
                "resolution": "chunk",
            }
        )
        cfg.aggregation["chunk_precompute"] = {
            "chunk_profile": {"expression": "np.zeros(4)", "source": "h_ph"}
        }
        validate_config(cfg)
        sig = get_output_signature(get_agg_fields(cfg)["f"])
        assert sig["resolution"] == "chunk"
        assert sig["kind"] == "vector"
        assert sig["trailing_shape"] == (4,)

    def test_vector_chunk_still_requires_trailing_shape(self):
        # The kind-specific validation still applies at chunk resolution.
        cfg = _cfg_with_field(
            {"kind": "vector", "expression": "np.zeros(4)", "resolution": "chunk"}
        )
        with pytest.raises(ValueError, match="kind 'vector' requires 'trailing_shape'"):
            validate_config(cfg)

    def test_ragged_chunk_companion_accepted(self):
        # issue #82 phase 4c: ragged chunk companions (CSR at chunk resolution) are
        # now wired — a chunk-resolution ragged field stores one payload per chunk
        # via write_ragged_to_zarr. Validation accepts it like any other ragged
        # field (inner_shape required; trailing_shape rejected).
        cfg = _cfg_with_field(
            {
                "kind": "ragged",
                "inner_shape": 2,
                "expression": "chunk_pairs",
                "resolution": "chunk",
            }
        )
        cfg.aggregation["chunk_precompute"] = {
            "chunk_pairs": {"expression": "np.zeros((3, 2))", "source": "h_ph"}
        }
        validate_config(cfg)  # no raise
        assert get_output_signature(get_agg_fields(cfg)["f"])["resolution"] == "chunk"

    def test_worked_template_offset_gain_are_chunk_resolution(self):
        cfg = default_config("atl03_waveform_chunk")
        fields = get_agg_fields(cfg)
        assert get_output_signature(fields["offset_h"])["resolution"] == "chunk"
        assert get_output_signature(fields["gain_h"])["resolution"] == "chunk"
        # waveform_counts stays cell-resolution (per-cell vector).
        assert get_output_signature(fields["waveform_counts"])["resolution"] == "cell"


# ---------------------------------------------------------------------------
# atl03_waveform_counts template (issue #30, phase 3)
# ---------------------------------------------------------------------------


class TestATL03WaveformCountsTemplate:
    @pytest.fixture
    def cfg(self):
        return default_config("atl03_waveform_counts")

    def test_loads_and_validates(self, cfg):
        # default_config already calls validate_config; just confirm round-trip.
        validate_config(cfg)
        assert cfg.data_source["reader"] == "h5coro"
        assert len(cfg.data_source["groups"]) == 6

    def test_variables_include_h_ph_only(self, cfg):
        # Option A: the histogram is centered on np.median(h_ph), so dem_h is
        # not needed (and the segment-level ``geophys_corr/dem_h`` path was the
        # wrong group anyway -- see #30 thread).
        ds_vars = cfg.data_source["variables"]
        assert "h_ph" in ds_vars
        assert "dem_h" not in ds_vars
        assert ds_vars["h_ph"].endswith("h_ph")

    def test_waveform_counts_field_is_vector(self, cfg):
        fields = get_agg_fields(cfg)
        meta = fields["waveform_counts"]
        sig = get_output_signature(meta)
        assert sig["kind"] == "vector"
        assert sig["trailing_shape"] == (128,)
        assert sig["dtype"] == "uint32"

    def test_bin_start_field_is_scalar(self, cfg):
        fields = get_agg_fields(cfg)
        meta = fields["bin_start"]
        sig = get_output_signature(meta)
        assert sig["kind"] == "scalar"
        assert sig["trailing_shape"] == ()

    def test_waveform_counts_expression_with_synthetic_data(self, cfg):
        # Photons all within ±128 m of their own median; all should be counted.
        from zagg.processing import calculate_cell_statistics

        np.random.seed(0)
        h_ph = np.random.uniform(-100.0, 100.0, 50).astype("float32")
        result = calculate_cell_statistics({"h_ph": h_ph, "leaf_id": np.arange(50)}, config=cfg)
        wc = result["waveform_counts"]
        assert wc.shape == (128,)
        assert wc.dtype == np.dtype("uint32")
        assert int(wc.sum()) == 50, "all in-range photons must be counted"

    def test_out_of_range_photons_dropped(self, cfg):
        from zagg.processing import calculate_cell_statistics

        # Two photons clustered near 0, one far outlier at 500 m. The cell median
        # is ~5 m, so the outlier sits beyond ±128 m and falls outside the hist.
        h_ph = np.array([0.0, 10.0, 500.0], dtype="float32")
        result = calculate_cell_statistics({"h_ph": h_ph, "leaf_id": np.arange(3)}, config=cfg)
        wc = result["waveform_counts"]
        assert int(wc.sum()) == 2, "out-of-range photon must not appear in any bin"

    def test_empty_cell_returns_zero_filled_vector(self, cfg):
        from zagg.processing import calculate_cell_statistics

        result = calculate_cell_statistics(
            {"h_ph": np.array([]), "leaf_id": np.array([])}, config=cfg
        )
        wc = result["waveform_counts"]
        assert wc.shape == (128,)
        assert np.all(wc == 0), "empty cell sentinel must be all-zero for uint32/fill_value:0"

    def test_confidence_filter_same_as_atl03(self, cfg):
        # Both templates carry the same TEP filter expressed in the structured
        # ``filters:`` list form (op: ne, value: -2, column: 0 -- land surface
        # type; TEP is uniform across columns per the v3 data dictionary).
        filters = cfg.data_source["filters"]
        assert len(filters) == 1
        f = filters[0]
        assert f["op"] == "ne"
        assert f["value"] == -2
        assert f["column"] == 0
        assert f["dataset"].endswith("signal_conf_ph")

    def test_rectilinear_grid(self, cfg):
        grid = cfg.output["grid"]
        assert grid["type"] == "rectilinear"
        assert len(grid["bounds"]) == 4

    def test_multi_level_form_matches_atl03(self, cfg):
        # Phase 6: waveform template carries the same multi-level form +
        # read_plan as atl03.yaml so the planned-IO benefits apply equally.
        ds = cfg.data_source
        assert ds["base_level"] == "photons"
        assert set(ds["levels"]) == {"photons", "segments"}
        rp = ds["read_plan"]
        assert rp["spatial_index"] == "segments"
        assert rp["pad"] == 1
        # Cross-check only the fields that drive ``plan_read`` parity: the
        # link's source/target arrays + index_base. Other level fields
        # (documentation-only ``variables``, coord names, formatting) can
        # legitimately diverge across templates without affecting the plan.
        atl03_link = default_config("atl03").data_source["levels"]["segments"]["link"]
        wf_link = ds["levels"]["segments"]["link"]
        for key in ("to", "index_beg", "count", "index_base"):
            assert wf_link[key] == atl03_link[key], f"link.{key} diverges from atl03.yaml"


# ---------------------------------------------------------------------------
# Ragged output kind (issue #48, phase 1)
# ---------------------------------------------------------------------------


def _ragged_cfg(inner_shape=None, **overrides):
    """Build a minimal config with a single ragged agg variable."""
    meta = {
        "function": "mean",
        "source": "h_ph",
        "kind": "ragged",
        **({"inner_shape": inner_shape} if inner_shape is not None else {}),
        **overrides,
    }
    return PipelineConfig(
        data_source={
            "reader": "h5coro",
            "groups": ["gt1l/heights"],
            "coordinates": {
                "latitude": "{group}/lat_ph",
                "longitude": "{group}/lon_ph",
            },
            "variables": {"h_ph": "{group}/h_ph"},
        },
        aggregation={"coordinates": {}, "variables": {"h_ph_tdigest": meta}},
        output={
            "grid": {
                "type": "healpix",
                "child_order": 12,
                "parent_order": 6,
            }
        },
    )


def _clocked(cfg, field="delta_time", **overrides):
    """Add the store's per-observation clock to a config (spec §8.3, #410).

    ``output.time_source`` is what a temporal companion encodes its words from,
    so every valid ``temporal:`` config carries it; the declared column has to
    be a base-rate ``data_source.variables`` entry, hence the pair.
    """
    cfg.data_source["variables"][field] = f"{{group}}/{field}"
    cfg.output["time_source"] = {
        "field": field,
        "epoch": "2018-01-01T00:00:00",
        "scale": "gps",
        "units": "seconds",
        **overrides,
    }
    return cfg


class TestRaggedKind:
    def test_valid_ragged_validates(self):
        """A ragged field with inner_shape declared validates without error."""
        cfg = _ragged_cfg(inner_shape=[2])
        validate_config(cfg)

    def test_get_output_signature_ragged(self):
        sig = get_output_signature({"kind": "ragged", "inner_shape": [2], "dtype": "float32"})
        assert sig == {
            "kind": "ragged",
            "trailing_shape": (),
            "inner_shape": (2,),
            "dtype": "float32",
            "resolution": "cell",
            "location": None,
            "weights": None,
            "temporal": None,
        }

    def test_ragged_inner_shape_int_normalized(self):
        sig = get_output_signature({"kind": "ragged", "inner_shape": 3})
        assert sig["inner_shape"] == (3,)

    def test_inner_shape_required(self):
        cfg = _ragged_cfg()  # no inner_shape
        with pytest.raises(ValueError, match="requires 'inner_shape'"):
            validate_config(cfg)

    def test_inner_shape_must_be_positive(self):
        cfg = _ragged_cfg(inner_shape=0)
        with pytest.raises(ValueError, match="'inner_shape' entries must be positive"):
            validate_config(cfg)

    def test_inner_shape_rejects_empty(self):
        cfg = _ragged_cfg(inner_shape=[])
        with pytest.raises(ValueError, match="'inner_shape' must have at least one dimension"):
            validate_config(cfg)

    def test_inner_shape_rejects_non_int(self):
        cfg = _ragged_cfg(inner_shape="2")
        with pytest.raises(ValueError, match="'inner_shape' must be an int or a sequence of ints"):
            validate_config(cfg)

    def test_inner_shape_list_rejects_non_int_element(self):
        cfg = _ragged_cfg(inner_shape=[2, "x"])
        with pytest.raises(ValueError, match="'inner_shape' entries must be positive"):
            validate_config(cfg)

    def test_ragged_with_expression_validates(self):
        """A ragged field driven by an expression (not function) validates."""
        cfg = PipelineConfig(
            data_source={
                "reader": "h5coro",
                "groups": ["gt1l/heights"],
                "coordinates": {
                    "latitude": "{group}/lat_ph",
                    "longitude": "{group}/lon_ph",
                },
                "variables": {"h_ph": "{group}/h_ph"},
            },
            aggregation={
                "coordinates": {},
                "variables": {
                    "h_ph_tdigest": {
                        "expression": "np.array([np.mean(h_ph), np.var(h_ph)])",
                        "kind": "ragged",
                        "inner_shape": [2],
                    }
                },
            },
            output={"grid": {"type": "healpix", "child_order": 12, "parent_order": 6}},
        )
        validate_config(cfg)

    def test_trailing_shape_rejected_for_ragged(self):
        cfg = _ragged_cfg(inner_shape=[2], trailing_shape=4)
        with pytest.raises(
            ValueError, match="'trailing_shape' is only valid for 'vector', not 'ragged'"
        ):
            validate_config(cfg)

    def test_len_rejected_for_ragged(self):
        cfg = _ragged_cfg(inner_shape=[2], function="len")
        with pytest.raises(ValueError, match="cannot be combined with kind 'ragged'"):
            validate_config(cfg)

    def test_count_rejected_for_ragged(self):
        cfg = _ragged_cfg(inner_shape=[2], function="count")
        with pytest.raises(ValueError, match="cannot be combined with kind 'ragged'"):
            validate_config(cfg)

    def test_scalar_inner_shape_empty(self):
        """Scalar fields still return inner_shape=() from get_output_signature."""
        sig = get_output_signature({"function": "min", "dtype": "float32"})
        assert sig["inner_shape"] == ()

    def test_vector_inner_shape_empty(self):
        """Vector fields still return inner_shape=() from get_output_signature."""
        sig = get_output_signature({"kind": "vector", "trailing_shape": 4, "dtype": "float32"})
        assert sig["inner_shape"] == ()

    def test_output_field_signature_ragged_includes_inner_shape(self):
        cfg = _ragged_cfg(inner_shape=[2], function="mean")
        entries = output_field_signature(cfg)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "h_ph_tdigest"
        assert entry["kind"] == "ragged"
        assert entry["inner_shape"] == [2]
        assert entry["trailing_shape"] == []

    def test_output_field_signature_scalar_inner_shape_empty(self, atl06_config):
        """Scalar fields get inner_shape: [] in output_field_signature (backward compat)."""
        entries = output_field_signature(atl06_config)
        for e in entries:
            assert e["inner_shape"] == [], f"{e['name']!r} has non-empty inner_shape"


# ---------------------------------------------------------------------------
# Packaged δ = 8,192 raise (issues #414/#424)
# ---------------------------------------------------------------------------


class TestPackagedDeltaRaise:
    """The four shipped ATL03 t-digest configs carry the split budgets.

    Leaf δ = 8,192 is the measured loss-free bound (issue #422's statewide CA
    scan); overview_delta = 512 is the pyramid-fold accuracy budget. The
    packaged configs are EXPLICIT so ``_DEFAULT_DELTA`` never decides an
    output value for them (the silent-change-under-stable-hash trap).
    """

    @pytest.mark.parametrize(
        "name",
        [
            "atl03_tdigest_healpix",
            "atl03_tdigest_healpix_hive",
            "atl03_tdigest_located_healpix",
            "atl03_tdigest_strata_healpix",
        ],
    )
    def test_digest_fields_declare_the_split_budgets(self, name):
        cfg = default_config(name)
        ragged = {
            f: meta
            for f, meta in cfg.aggregation["variables"].items()
            if meta.get("kind") == "ragged"
        }
        assert ragged  # every one of these templates carries digest fields
        for meta in ragged.values():
            assert meta["params"]["delta"] == 8192
            assert meta["overview_delta"] == 512


# ---------------------------------------------------------------------------
# Ragged weights declaration + overview_delta (spec §2.0, issue #424)
# ---------------------------------------------------------------------------


_FLUX_GAIN = {"gain": {"name": "test_gain", "version": "1"}}


class TestWeightsDeclaration:
    def test_counts_validates(self):
        validate_config(_ragged_cfg(inner_shape=[2], weights="counts"))

    def test_flux_with_gain_provenance_validates(self):
        validate_config(_ragged_cfg(inner_shape=[2], weights="flux", attrs=dict(_FLUX_GAIN)))

    def test_flux_requires_gain_provenance(self):
        with pytest.raises(ValueError, match="requires calibration provenance"):
            validate_config(_ragged_cfg(inner_shape=[2], weights="flux"))

    def test_flux_gain_requires_name_and_version(self):
        with pytest.raises(ValueError, match="requires calibration provenance"):
            validate_config(
                _ragged_cfg(inner_shape=[2], weights="flux", attrs={"gain": {"name": "g"}})
            )

    def test_unknown_value_rejected(self):
        with pytest.raises(ValueError, match="is not one of"):
            validate_config(_ragged_cfg(inner_shape=[2], weights="photons"))

    def test_weights_rejected_on_non_ragged_kinds(self):
        with pytest.raises(ValueError, match="'weights' is only valid for kind 'ragged'"):
            _validate_output_kind("f", {"function": "min", "weights": "counts"})
        with pytest.raises(ValueError, match="'weights' is only valid for kind 'ragged'"):
            _validate_output_kind("f", {"kind": "vector", "trailing_shape": 4, "weights": "counts"})

    def test_attrs_weights_key_is_spec_owned_on_ragged(self):
        # The template stamps the §2.0 key from the field declaration; an
        # attrs transcription could silently disagree, so it is reserved.
        with pytest.raises(ValueError, match="spec-owned"):
            validate_config(_ragged_cfg(inner_shape=[2], attrs={"weights": "flux"}))

    def test_shipped_gedi_template_refuses_the_pre_424_flat_gain(self):
        # The seam at TEMPLATE level, which the synthetic cases above do not
        # reach: take the SHIPPED gedi01b template and revert only its attrs
        # to the flat pre-#424 keys (gain_name / gain_version / scalar gain)
        # while `weights: flux` stays declared. validate_config must refuse
        # it. That mismatch — a declaration whose validator demands a shape
        # the template does not ship — is the bug this PR's flip had to fix,
        # and the packaged config is otherwise only ever exercised in its
        # already-correct form.
        cfg = default_config("gedi01b_waveform_healpix_hive", validate=False)
        rx = cfg.aggregation["variables"]["rx_flux"]
        assert rx["weights"] == "flux"
        del rx["attrs"]["gain"]
        rx["attrs"].update(
            {"gain_name": "unit", "gain_version": "gedi01b-v002-placeholder", "gain": 1.0}
        )
        with pytest.raises(ValueError, match="requires calibration provenance"):
            validate_config(cfg)

    def test_signature_carries_weights_only_when_set(self):
        entries = output_field_signature(
            _ragged_cfg(inner_shape=[2], weights="flux", attrs=dict(_FLUX_GAIN))
        )
        assert entries[0]["weights"] == "flux"
        # Undeclared: keyed-only-when-set, so existing signatures are stable.
        assert "weights" not in output_field_signature(_ragged_cfg(inner_shape=[2]))[0]


class TestTemporalShapeDeclaration:
    """The field-level ``temporal:`` shape (spec §8.2/§8.3, issue #410)."""

    def _cell_cfg(self, **overrides):
        meta = {
            "function": "zagg.stats.toc.cell_envelope",
            "source": "toc_word",
            "dtype": "uint64",
            "fill_value": 0,
            "temporal": "per-cell",
            **overrides,
        }
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.aggregation["variables"] = {"observed": meta}
        return cfg

    def _centroid_cfg(self, **overrides):
        return _clocked(
            _ragged_cfg(
                inner_shape=[2],
                temporal="per-centroid",
                function="zagg.stats.tdigest.build_tdigest",
                **overrides,
            )
        )

    def test_per_centroid_on_a_ragged_field_validates(self):
        validate_config(self._centroid_cfg())

    def test_per_cell_on_a_dense_uint64_field_validates(self):
        validate_config(self._cell_cfg())

    def test_a_non_producing_reducer_is_refused(self):
        # The gate keys on the FIELD's reducer: only the allowlisted ones emit
        # toc words, and anything else would stamp a zagg-toc/1 declaration over
        # its own output cast to uint64 (spec §8.2's envelope claim).
        with pytest.raises(ValueError, match="does not produce"):
            validate_config(self._cell_cfg(function="nanmax"))
        with pytest.raises(ValueError, match="does not produce"):
            validate_config(_clocked(_ragged_cfg(inner_shape=[2], temporal="per-centroid")))

    def test_the_refusal_names_the_issue(self):
        with pytest.raises(ValueError, match=r"issue #410"):
            validate_config(self._cell_cfg(function="nanmax"))

    def test_shape_and_reducer_must_agree(self):
        # A whole-cell reducer cannot fill a per-centroid sibling, and a digest
        # kernel's channel is not a dense per-cell array. The two allowlist
        # halves partition, so a crossed declaration is named as such.
        cfg = self._cell_cfg(function="zagg.stats.tdigest.build_tdigest")
        with pytest.raises(ValueError, match="produces the 'per-centroid' temporal shape"):
            validate_config(cfg)

    def test_temporal_requires_the_stores_clock(self):
        # Without output.time_source (or a continuous-scale windowing block to
        # fall back to) there is no column to encode words from. The refusal is
        # the worker's own text (issue #472) prefixed with the variable name.
        cfg = self._centroid_cfg()
        del cfg.output["time_source"]
        with pytest.raises(ValueError, match=re.escape(TOC_NO_CLOCK_ERROR)):
            validate_config(cfg)

    def test_windowing_satisfies_the_clock(self):
        # One declaration for window routing AND toc ingest, so the two cannot
        # disagree at a window boundary.
        cfg = self._centroid_cfg()
        del cfg.output["time_source"]
        cfg.data_source["variables"]["delta_time"] = "{group}/delta_time"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "gps",
        }
        validate_config(cfg)

    def test_params_may_not_shadow_the_temporal_kwarg(self):
        with pytest.raises(ValueError, match="reserved for the temporal channel"):
            validate_config(self._centroid_cfg(params={"temporal": "h_ph"}))

    def test_per_centroid_requires_a_temporal_capable_reducer(self):
        # A reducer with no ``temporal`` keyword would raise per cell; reject at
        # load, exactly as the located channel does for ``locations``. Note the
        # producer allowlist is checked too, so a reducer must fail BOTH gates to
        # be a clean case — ``np.sort`` takes no such keyword and produces no
        # words. (``build_waveform_digest`` carries the channel as of phase 4, so
        # it is no longer the example here.)
        cfg = self._centroid_cfg()
        cfg.aggregation["variables"]["h_ph_tdigest"]["function"] = "np.sort"
        with pytest.raises(
            ValueError, match="does not accept a 'temporal' keyword|does not produce"
        ):
            validate_config(cfg)

    def test_the_waveform_reducer_carries_the_channel(self):
        # Phase 4 (issue #410): GEDI's flux reducer accepts ``temporal=`` and is
        # on the producer allowlist, so a waveform field may declare a companion.
        import inspect

        from zagg.stats.waveform import build_waveform_digest
        from zagg.time_axis import TOC_PRODUCING_FUNCTIONS

        assert "temporal" in inspect.signature(build_waveform_digest).parameters
        assert "zagg.stats.waveform.build_waveform_digest" in TOC_PRODUCING_FUNCTIONS

    def test_undeclared_fields_are_untouched_by_the_gate(self):
        # The gate is scoped to `temporal:` — every other config still loads.
        validate_config(_ragged_cfg(inner_shape=[2]))

    def test_unknown_shape_rejected(self):
        with pytest.raises(ValueError, match="is not one of"):
            validate_config(_ragged_cfg(inner_shape=[2], temporal="per-photon"))

    def test_coordinate_shape_points_at_the_axis_knob(self):
        # §8.1 is declared by output.time_encoding, never by a variable.
        with pytest.raises(ValueError, match="output.time_encoding"):
            validate_config(_ragged_cfg(inner_shape=[2], temporal="coordinate"))

    def test_per_centroid_requires_a_ragged_field(self):
        with pytest.raises(ValueError, match="kind must be 'ragged'"):
            validate_config(self._cell_cfg(temporal="per-centroid"))

    def test_per_cell_requires_a_scalar_field(self):
        with pytest.raises(ValueError, match="kind must be 'scalar'"):
            validate_config(_ragged_cfg(inner_shape=[2], temporal="per-cell"))

    def test_per_cell_requires_uint64(self):
        with pytest.raises(ValueError, match="requires dtype 'uint64'"):
            validate_config(self._cell_cfg(dtype="int64"))

    def test_per_cell_requires_the_reserved_fill(self):
        # §8.2 reserves 0 as the unobserved-cell marker.
        with pytest.raises(ValueError, match="reserves it as the"):
            validate_config(self._cell_cfg(fill_value=1))

    def test_per_cell_requires_the_reserved_fill_explicitly(self):
        # An ABSENT key is refused here rather than defaulted: the dense
        # template's default is "NaN", so assuming 0 would only move the
        # failure to a bare zarr TypeError (the template half of this case
        # is test_processing.TestTemporalCompanionSeams).
        cfg = self._cell_cfg()
        del cfg.aggregation["variables"]["observed"]["fill_value"]
        with pytest.raises(ValueError, match="requires an explicit fill_value 0"):
            validate_config(cfg)

    def test_chunk_resolution_rejected(self):
        with pytest.raises(ValueError, match="not supported with 'resolution: chunk'"):
            validate_config(
                _ragged_cfg(inner_shape=[2], temporal="per-centroid", resolution="chunk")
            )

    def test_sibling_name_collision_rejected(self):
        cfg = self._centroid_cfg()
        cfg.aggregation["variables"]["h_ph_tdigest_times"] = {
            "function": "mean",
            "source": "h_ph",
        }
        with pytest.raises(ValueError, match="temporal channel is stored in a sibling"):
            validate_config(cfg)

    @pytest.mark.parametrize("key", ["temporal", "located", "times"])
    def test_spec_owned_attrs_keys_rejected(self, key):
        with pytest.raises(ValueError, match="spec-owned"):
            validate_config(_ragged_cfg(inner_shape=[2], attrs={key: "anything"}))

    def test_signature_carries_temporal_only_when_set(self):
        entries = output_field_signature(_ragged_cfg(inner_shape=[2], temporal="per-centroid"))
        assert entries[0]["temporal"] == "per-centroid"
        # Undeclared: keyed-only-when-set, so existing signatures are stable.
        assert "temporal" not in output_field_signature(_ragged_cfg(inner_shape=[2]))[0]


class TestTimeSource:
    """``output.time_source`` — the per-observation clock (spec §8.3, #410)."""

    def test_valid_block_validates(self):
        validate_config(_clocked(_ragged_cfg(inner_shape=[2])))

    def test_absent_block_resolves_to_none(self):
        from zagg.time_axis import toc_source

        assert toc_source(_ragged_cfg(inner_shape=[2])) is None

    def test_resolved_shape(self):
        from zagg.time_axis import toc_source

        assert toc_source(_clocked(_ragged_cfg(inner_shape=[2]))) == {
            "field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "gps",
            "units": "seconds",
        }

    def test_units_default_to_seconds(self):
        from zagg.time_axis import toc_source

        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        del cfg.output["time_source"]["units"]
        assert toc_source(cfg)["units"] == "seconds"

    def test_utc_scale_refused_by_name(self):
        # §8.3 wants an instant exact to the nanosecond; a nominal-UTC offset
        # column is only good to the leap seconds since its epoch.
        with pytest.raises(ValueError, match="continuous timescale"):
            validate_config(_clocked(_ragged_cfg(inner_shape=[2]), scale="utc"))

    def test_unknown_units_refused(self):
        with pytest.raises(ValueError, match="time_source.units must be one of"):
            validate_config(_clocked(_ragged_cfg(inner_shape=[2]), units="ticks"))

    def test_field_must_be_a_declared_variable(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        del cfg.data_source["variables"]["delta_time"]
        with pytest.raises(ValueError, match="not a declared data_source variable"):
            validate_config(cfg)

    def test_field_may_not_be_the_derived_word_column(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.output["time_source"]["field"] = "toc_word"
        with pytest.raises(ValueError, match="is the DERIVED toc word column"):
            validate_config(cfg)

    def test_epoch_required_and_parsed(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        del cfg.output["time_source"]["epoch"]
        with pytest.raises(ValueError, match="time_source.epoch is required"):
            validate_config(cfg)
        cfg = _clocked(_ragged_cfg(inner_shape=[2]), epoch="not-a-date")
        with pytest.raises(ValueError, match="is not an ISO-8601 datetime"):
            validate_config(cfg)

    def test_unknown_keys_refused(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.output["time_source"]["quantum"] = 1
        with pytest.raises(ValueError, match=r"unknown key\(s\) \['quantum'\]"):
            validate_config(cfg)

    def test_non_mapping_refused(self):
        cfg = _ragged_cfg(inner_shape=[2])
        cfg.output["time_source"] = "delta_time"
        with pytest.raises(ValueError, match="time_source must be a mapping"):
            validate_config(cfg)

    def test_utc_windowing_is_not_a_fallback(self):
        # The fallback exists to keep ONE clock in a windowed store; a utc-scale
        # windowing block cannot serve as one, so a companion must declare its
        # own continuous column rather than silently inherit a coarser one.
        from zagg.time_axis import toc_source

        cfg = _ragged_cfg(inner_shape=[2])
        cfg.data_source["variables"]["delta_time"] = "{group}/delta_time"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "utc",
        }
        assert toc_source(cfg) is None

    def test_derived_column_is_a_valid_source_reference(self):
        # ``toc_word`` becomes a real base-rate column once the clock is
        # declared, so a per-cell companion may name it — and only then.
        cfg = _ragged_cfg(inner_shape=[2])
        cfg.aggregation["variables"]["observed"] = {
            "function": "zagg.stats.toc.cell_envelope",
            "source": "toc_word",
            "dtype": "uint64",
            "fill_value": 0,
            "temporal": "per-cell",
        }
        with pytest.raises(ValueError, match="source 'toc_word' not in data_source.variables"):
            validate_config(cfg)
        validate_config(_clocked(cfg))

    def test_derived_column_is_only_readable_where_it_is_materialized(self):
        # Validation and materialization must gate on the SAME condition — a
        # declared ``temporal:`` companion. Widening on the clock alone is true of
        # every windowed gps/tai store via the fallback, so these two configs
        # validated clean and then raised KeyError/NameError in the worker
        # (issue #410 review).
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.aggregation["variables"]["t_max"] = {
            "function": "numpy.max",
            "source": "toc_word",
            "dtype": "uint64",
            "fill_value": 0,
        }
        with pytest.raises(ValueError, match="source 'toc_word' not in data_source.variables"):
            validate_config(cfg)
        # Declaring a companion materializes the column, and both readers resolve.
        cfg.aggregation["variables"]["observed"] = {
            "function": "zagg.stats.toc.cell_envelope",
            "source": "toc_word",
            "dtype": "uint64",
            "fill_value": 0,
            "temporal": "per-cell",
        }
        validate_config(cfg)

    def test_chunk_precompute_cannot_read_the_unmaterialized_derived_column(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.aggregation["chunk_precompute"] = {"tmax": {"expression": "toc_word.max()"}}
        with pytest.raises(ValueError, match="expression references 'toc_word'"):
            validate_config(cfg)

    def test_declared_column_may_not_shadow_the_derived_name(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.data_source["variables"]["toc_word"] = "{group}/toc_word"
        with pytest.raises(ValueError, match="reserved name of the derived toc word column"):
            validate_config(cfg)
        # The NAME is reserved unconditionally — the reservation does not depend
        # on whether a clock (or a companion) happens to be declared.
        bare = _ragged_cfg(inner_shape=[2])
        bare.data_source["variables"]["toc_word"] = "{group}/toc_word"
        with pytest.raises(ValueError, match="reserved name of the derived toc word column"):
            validate_config(bare)

    def test_declared_coordinate_may_not_shadow_the_derived_name(self):
        # Coordinates are read into the same cell_data mapping the derived words
        # are merged into, so an unreserved name there would be overwritten
        # silently, and only for the cells with observations (issue #410 review).
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.data_source["coordinates"] = {
            **(cfg.data_source.get("coordinates") or {}),
            "toc_word": "{group}/toc_word",
        }
        with pytest.raises(ValueError, match=r"data_source.coordinates declares 'toc_word'"):
            validate_config(cfg)

    def test_both_clock_declarations_must_agree(self):
        # The fallback single-sources the clock only when time_source is ABSENT.
        # Unchecked, a store declaring both routes windows on one clock and
        # encodes its §8.3 words from another (issue #410 review).
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "gps",
            "units": "seconds",
        }
        validate_config(cfg)  # an agreeing pair is the normal case
        cfg.data_source["variables"]["other_time"] = "{group}/other_time"
        cfg.output["time_source"].update(
            {
                "field": "other_time",
                "epoch": "2020-06-01T00:00:00",
                "scale": "tai",
                "units": "days",
            }
        )
        with pytest.raises(ValueError, match="disagrees with output.windowing"):
            validate_config(cfg)

    def test_an_agreeing_sub_second_pair_gets_the_sub_second_refusal(self):
        # This validator runs ahead of _validate_windowing's #390 guard, so it
        # is the first to see a sub-second windowing epoch. Comparing the
        # declared time_source epoch against get_windowing's ALREADY-RENDERED
        # one made two byte-identical declarations look like two clocks, and
        # the author got a factually untrue "disagrees" message instead of the
        # accurate sub-second refusal.
        cfg = _clocked(_ragged_cfg(inner_shape=[2]), epoch="2018-01-01T00:00:00.5Z")
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00.5Z",
            "scale": "gps",
            "units": "seconds",
        }
        with pytest.raises(ValueError, match="carries sub-second precision"):
            validate_config(cfg)
        # A real epoch disagreement still reports both DECLARED spellings — the
        # cross-check's purpose is untouched, only the value it quotes.
        cfg.output["time_source"]["epoch"] = "2020-06-01T00:00:00"
        with pytest.raises(
            ValueError, match=r"disagrees with output\.windowing .*2018-01-01T00:00:00\.5Z"
        ):
            validate_config(cfg)

    def test_a_utc_windowing_block_is_exempt_from_the_cross_check(self):
        # The one path that deliberately carries two declarations of the same
        # column on two scales: a utc windowing block gets no fallback, so the
        # explicit continuous declaration must be allowed to differ from it.
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "utc",
        }
        validate_config(cfg)

    def test_chunk_precompute_may_not_shadow_the_derived_name(self):
        cfg = _clocked(_ragged_cfg(inner_shape=[2]))
        cfg.aggregation["chunk_precompute"] = {"toc_word": {"function": "mean", "source": "h_ph"}}
        with pytest.raises(ValueError, match="reserved derived column"):
            validate_config(cfg)


class TestTemporalClockAtSubmission:
    """``validate_config`` refuses a ``temporal:`` companion whose clock does
    not resolve, in the worker's own words (issue #472).

    The config shape is the observed one: the ``02_write`` demo grafted
    ``aggregation["variables"]`` from ``atl03_tdigest_located_healpix`` (whose
    variables declare ``temporal: per-centroid``, PR #463) onto the hive base
    template without also grafting ``output.time_source`` and the
    ``delta_time`` source column.

    These are **validator-level** pins: the checks themselves predate this PR
    (``a67be395``/``ef68ef74``, issue #410) and pass on ``main`` — what is new
    here is the single-sourced message text (``test_both_seams_raise_the_same_text``).
    The regression for the reported symptom is one seam up, where the graft
    actually dispatched unvalidated:
    ``tests/test_client.py::TestSubmissionValidation`` (fold review).
    """

    def _graft(self):
        import copy

        base = default_config("atl03_tdigest_healpix_hive", validate=False)
        located = default_config("atl03_tdigest_located_healpix", validate=False)
        base.aggregation["variables"] = copy.deepcopy(located.aggregation["variables"])
        return base, located

    def test_graft_without_clock_refused_at_submission(self):
        # The exact observed shape, pinned to the worker's message text so the
        # two seams read identically.
        cfg, _ = self._graft()
        assert (cfg.output or {}).get("time_source") is None
        with pytest.raises(ValueError, match=re.escape(TOC_NO_CLOCK_ERROR)):
            validate_config(cfg)

    def test_graft_clock_without_column_refused_at_submission(self):
        # The graft's second missing piece: time_source present but its field
        # not a declared data_source variable — refused here, not as a worker
        # KeyError one seam later.
        cfg, located = self._graft()
        cfg.output["time_source"] = dict(located.output["time_source"])
        # Guard the precondition against the SAME set the validator checks —
        # declared variables plus broadcast level variables — so it tracks
        # ``_validate_time_source`` rather than agreeing with it by luck of
        # this template (fold review).
        declared = set(cfg.data_source["variables"]) | _segment_variable_names(cfg.data_source)
        assert cfg.output["time_source"]["field"] not in declared
        with pytest.raises(ValueError, match="not a declared data_source variable"):
            validate_config(cfg)

    def test_graft_with_full_clock_validates(self):
        # Grafting BOTH missing pieces (the clock block and its column) is the
        # correct form of the demo's config, and it validates.
        cfg, located = self._graft()
        cfg.output["time_source"] = dict(located.output["time_source"])
        field = cfg.output["time_source"]["field"]
        cfg.data_source["variables"][field] = located.data_source["variables"][field]
        validate_config(cfg)

    def test_windowing_fallback_satisfies_the_graft(self):
        # The continuous-scale windowing fallback (PR #463) resolves the clock
        # through the same resolver the worker uses (toc_source), so the graft
        # with a windowing block and its column — but no time_source — is valid.
        cfg, _ = self._graft()
        cfg.data_source["variables"]["delta_time"] = "{group}/heights/delta_time"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00",
            "scale": "gps",
        }
        validate_config(cfg)

    def test_both_seams_raise_the_same_text(self):
        # Parity pin: the worker's defense-in-depth refusal is the exact string
        # the validator embeds (single-sourced in zagg.time_axis, issue #472).
        from zagg.processing.aggregate import _toc_word_column

        cfg, _ = self._graft()
        with pytest.raises(ValueError) as worker_exc:
            _toc_word_column({}, cfg)
        with pytest.raises(ValueError) as submit_exc:
            validate_config(cfg)
        assert str(worker_exc.value) == TOC_NO_CLOCK_ERROR
        assert TOC_NO_CLOCK_ERROR in str(submit_exc.value)

    def test_every_shipped_temporal_template_validates(self):
        # Sweep the packaged configs for aggregation variables carrying the
        # ``temporal:`` key; each such template must pass validate_config, and
        # the sweep must actually find the known carriers (a guard against the
        # discovery matching nothing).
        from importlib import resources

        import zagg.configs

        names = sorted(
            p.name[: -len(".yaml")]
            for p in resources.files(zagg.configs).iterdir()
            if p.name.endswith(".yaml")
        )
        carriers = set()
        for name in names:
            cfg = default_config(name, validate=False)
            agg_vars = (cfg.aggregation or {}).get("variables") or {}
            if any(isinstance(m, dict) and m.get("temporal") for m in agg_vars.values()):
                carriers.add(name)
                validate_config(cfg)
        assert carriers >= {"atl03_tdigest_located_healpix", "gedi01b_waveform_healpix_hive"}


class TestOverviewDelta:
    def test_valid_overview_delta_validates(self):
        validate_config(_ragged_cfg(inner_shape=[2], overview_delta=512))

    @pytest.mark.parametrize("bad", [0, -1, 2.5, "512", True])
    def test_invalid_overview_delta_rejected(self, bad):
        with pytest.raises(ValueError, match="overview_delta must be a positive int"):
            validate_config(_ragged_cfg(inner_shape=[2], overview_delta=bad))

    def test_overview_delta_rejected_on_non_ragged_kinds(self):
        with pytest.raises(ValueError, match="'overview_delta' is only valid for kind 'ragged'"):
            _validate_output_kind("f", {"function": "min", "overview_delta": 512})


# ---------------------------------------------------------------------------
# Ragged location channel (issue #87)
# ---------------------------------------------------------------------------


class TestLocationChannel:
    def test_located_ragged_validates(self):
        cfg = _ragged_cfg(
            inner_shape=[2],
            location="leaf_id",
            function="zagg.stats.tdigest.build_tdigest",
        )
        validate_config(cfg)

    def test_location_requires_locations_capable_function(self):
        # np.mean has no ``locations`` kwarg — reject at load, not per cell.
        cfg = _ragged_cfg(inner_shape=[2], location="leaf_id", function="mean")
        with pytest.raises(ValueError, match="does not accept a 'locations' keyword"):
            validate_config(cfg)

    def test_location_params_collision_rejected(self):
        cfg = _ragged_cfg(
            inner_shape=[2],
            location="leaf_id",
            function="zagg.stats.tdigest.build_tdigest",
            params={"locations": "h_ph"},
        )
        with pytest.raises(ValueError, match="reserved for the location channel"):
            validate_config(cfg)

    def test_signature_carries_location(self):
        sig = get_output_signature({"kind": "ragged", "inner_shape": [2], "location": "leaf_id"})
        assert sig["location"] == "leaf_id"

    def test_location_rejected_for_scalar_and_vector(self):
        with pytest.raises(ValueError, match="only valid for kind 'ragged'"):
            _validate_output_kind("f", {"function": "min", "location": "leaf_id"})
        with pytest.raises(ValueError, match="only valid for kind 'ragged'"):
            _validate_output_kind(
                "f",
                {"function": "min", "kind": "vector", "trailing_shape": 4, "location": "leaf_id"},
            )

    def test_location_requires_function(self):
        cfg = _ragged_cfg(inner_shape=[2], location="leaf_id", expression="np.sort(h_ph)")
        del cfg.aggregation["variables"]["h_ph_tdigest"]["function"]
        with pytest.raises(ValueError, match="requires a 'function' reducer"):
            validate_config(cfg)

    def test_location_rejected_at_chunk_resolution(self):
        cfg = _ragged_cfg(inner_shape=[2], location="leaf_id", resolution="chunk")
        with pytest.raises(ValueError, match="not supported with 'resolution: chunk'"):
            validate_config(cfg)

    def test_location_must_be_string(self):
        cfg = _ragged_cfg(inner_shape=[2], location=29)
        with pytest.raises(ValueError, match="must be a column name string"):
            validate_config(cfg)

    def test_location_unknown_column_rejected(self):
        cfg = _ragged_cfg(inner_shape=[2], location="not_a_column")
        with pytest.raises(ValueError, match="is not 'leaf_id' or a data_source variable"):
            validate_config(cfg)

    def test_sibling_locations_field_collision_rejected(self):
        # Issue #209 (review, PR #211): the located channel lands in a SIBLING
        # array `{field}_locations` — a name the user never wrote. A declared
        # field claiming it would silently lose in the template members dict
        # and interleave into the same object slab at write time.
        cfg = _ragged_cfg(
            inner_shape=[2],
            location="leaf_id",
            function="zagg.stats.tdigest.build_tdigest",
        )
        cfg.aggregation["variables"]["h_ph_tdigest_locations"] = {
            "function": "min",
            "source": "h_ph",
            "dtype": "float32",
        }
        with pytest.raises(ValueError, match="sibling array named 'h_ph_tdigest_locations'"):
            validate_config(cfg)

    def test_sibling_locations_coordinate_collision_rejected(self):
        # Same guard for coordinates — they share the template members dict.
        cfg = _ragged_cfg(
            inner_shape=[2],
            location="leaf_id",
            function="zagg.stats.tdigest.build_tdigest",
        )
        cfg.aggregation["coordinates"]["h_ph_tdigest_locations"] = {
            "dtype": "uint64",
            "fill_value": 0,
        }
        with pytest.raises(ValueError, match="sibling array named 'h_ph_tdigest_locations'"):
            validate_config(cfg)

    def test_locations_suffix_free_without_location(self):
        # The sibling name is reserved only for LOCATED fields: an unlocated
        # ragged field coexists with a `{field}_locations`-named scalar.
        cfg = _ragged_cfg(inner_shape=[2])
        cfg.aggregation["variables"]["h_ph_tdigest_locations"] = {
            "function": "min",
            "source": "h_ph",
            "dtype": "float32",
        }
        validate_config(cfg)

    def test_location_defaults_to_healpix_when_grid_absent(self):
        # No output.grid block defaults to healpix everywhere else (the grid
        # factory), so a located field must validate the same way (review fold).
        cfg = _ragged_cfg(
            inner_shape=[2],
            location="leaf_id",
            function="zagg.stats.tdigest.build_tdigest",
        )
        cfg.output = {"store_path": "s3://x"}
        validate_config(cfg)

    def test_location_requires_healpix_grid(self):
        cfg = _ragged_cfg(inner_shape=[2], location="leaf_id")
        cfg.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3857",
            "resolution": 100,
            "bounds": [0, 0, 1, 1],
        }
        with pytest.raises(ValueError, match="requires a healpix output grid"):
            validate_config(cfg)

    def test_output_field_signature_includes_location_only_when_set(self):
        located = output_field_signature(_ragged_cfg(inner_shape=[2], location="leaf_id"))[0]
        assert located["location"] == "leaf_id"
        # Unlocated fields keep the pre-#87 signature entries byte-identical
        # (no new key), so existing shard-map signatures still match.
        plain = output_field_signature(_ragged_cfg(inner_shape=[2]))[0]
        assert "location" not in plain

    def test_located_builtin_config_loads_and_validates(self):
        """The shipped companion t-digest template loads, validates, and differs
        from the value-only template only by its two COMPANION channels.

        Issue #87 added the location channel; issue #410 added the §8.3 temporal
        one, which brings its clock with it — the ``delta_time`` read column and
        the ``output.time_source`` block that says how to convert it. Everything
        else must still be the plain template's, which is what this pins.
        """
        located = default_config("atl03_tdigest_located_healpix")
        plain = default_config("atl03_tdigest_healpix")
        sig = get_output_signature(get_agg_fields(located)["h_tdigest"])
        assert sig["location"] == "leaf_id"
        assert sig["temporal"] == "per-centroid"
        assert sig["kind"] == "ragged" and sig["inner_shape"] == (2,)
        # Same read plan and grid, modulo the clock the temporal channel needs.
        ds = {k: dict(v) if k == "variables" else v for k, v in located.data_source.items()}
        assert ds["variables"].pop("delta_time") == "/{group}/heights/delta_time"
        assert ds == plain.data_source
        out = dict(located.output)
        assert out.pop("time_source")["scale"] == "gps"
        assert out == plain.output
        located_meta = dict(get_agg_fields(located)["h_tdigest"])
        located_meta.pop("location")
        located_meta.pop("temporal")
        assert located_meta == get_agg_fields(plain)["h_tdigest"]


class TestMerra2StormTemplate:
    """The packaged temporal-pipeline example (issue #12, Phase 7)."""

    @pytest.fixture
    def merra2_config(self):
        from importlib import resources

        import zagg.configs

        ref = resources.files(zagg.configs).joinpath("merra2_storm.yaml")
        with resources.as_file(ref) as p:
            return load_config(str(p))

    def test_is_temporal_pipeline(self, merra2_config):
        assert get_pipeline_type(merra2_config) == "temporal"

    def test_validates(self, merra2_config):
        validate_config(merra2_config)  # no raise

    def test_output_is_parquet(self, merra2_config):
        from zagg.output import output_format

        assert output_format(merra2_config) == "parquet"
        assert merra2_config.output["store"].endswith(".parquet")

    def test_specs_resolve_registered_capabilities(self, merra2_config):
        from zagg import registry
        from zagg.temporal import specs_from_config

        specs = {s["output_name"]: s for s in specs_from_config(merra2_config)}
        assert set(specs) == {"max_t2m_ais", "anom_tqv_full", "total_precip_ocean"}
        # `anomaly: true` desugars to `transform: monthly_anomaly` (issue #12).
        assert specs["anom_tqv_full"]["transform"] == "monthly_anomaly"
        for spec in specs.values():
            assert spec["spatial_func"] in registry.list_spatial_funcs()
            assert spec["temporal_reducer"] in registry.list_reducers()
            assert spec["mask"] in registry.list_mask_providers()


class TestConsolidateMetadataFlag:
    """Issue #191: metadata consolidation is opt-in via output.consolidate_metadata
    (default False) — no zagg reader depends on the consolidated blob and building
    it is a ~70 s serial-GET finalize tax."""

    def test_default_off(self):
        from zagg.config import get_consolidate_metadata

        assert get_consolidate_metadata(default_config("atl06")) is False

    def test_accessor_true(self):
        from zagg.config import get_consolidate_metadata

        cfg = default_config("atl06")
        cfg.output = {**cfg.output, "consolidate_metadata": True}
        assert get_consolidate_metadata(cfg) is True

    def test_validate_rejects_non_bool(self):
        from zagg.config import validate_config

        cfg = default_config("atl06")
        cfg.output = {**cfg.output, "consolidate_metadata": "yes"}
        with pytest.raises(ValueError, match="output.consolidate_metadata must be a boolean"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# Worker-size selection block (issue #235)
# ---------------------------------------------------------------------------


class TestWorkerBlock:
    """Issue #235: the optional top-level ``worker:`` block selects one of the
    pre-provisioned Lambda size variants (memory in {2048, 4096, 8192},
    ``extra_disk`` for the ``-disk`` /tmp twin). Invalid values fail at config
    load — a typo would otherwise surface as a per-shard ResourceNotFound
    after the Lambda fan-out starts."""

    def test_absent_block_is_none_and_valid(self, atl06_config):
        # Back-compat: every existing config has no worker key.
        assert atl06_config.worker is None
        validate_config(atl06_config)

    def test_all_provisioned_memories_accepted(self, atl06_config):
        for memory in (2048, 4096, 8192):
            for extra_disk in (True, False):
                atl06_config.worker = {"memory": memory, "extra_disk": extra_disk}
                validate_config(atl06_config)

    def test_extra_disk_optional(self, atl06_config):
        atl06_config.worker = {"memory": 2048}
        validate_config(atl06_config)

    def test_loads_from_yaml_top_level(self, atl06_yaml, tmp_path):
        # The block is a top-level section alongside pipeline:, not nested
        # under output: (thread decision).
        text = open(atl06_yaml).read() + "\nworker:\n  memory: 2048\n  extra_disk: true\n"
        p = tmp_path / "atl06_worker.yaml"
        p.write_text(text)
        cfg = load_config(str(p))
        assert cfg.worker == {"memory": 2048, "extra_disk": True}

    def test_dict_roundtrip_carries_worker(self, atl06_config):
        atl06_config.worker = {"memory": 8192, "extra_disk": False}
        restored = load_config_from_dict(asdict(atl06_config))
        assert restored.worker == atl06_config.worker

    def test_unprovisioned_memory_rejected_naming_allowed_set(self, atl06_config):
        atl06_config.worker = {"memory": 1024}
        with pytest.raises(ValueError, match=r"worker\.memory must be one of \[2048, 4096, 8192\]"):
            validate_config(atl06_config)

    def test_missing_memory_rejected(self, atl06_config):
        atl06_config.worker = {"extra_disk": True}
        with pytest.raises(ValueError, match=r"worker\.memory must be one of"):
            validate_config(atl06_config)

    def test_string_memory_rejected(self, atl06_config):
        # "4096" (a string) is not the provisioned int — the suffix must be
        # built from a validated int, not whatever str() yields.
        atl06_config.worker = {"memory": "4096"}
        with pytest.raises(ValueError, match=r"worker\.memory must be one of"):
            validate_config(atl06_config)

    def test_bool_memory_rejected(self, atl06_config):
        atl06_config.worker = {"memory": True}
        with pytest.raises(ValueError, match=r"worker\.memory must be one of"):
            validate_config(atl06_config)

    def test_non_mapping_block_rejected(self, atl06_config):
        atl06_config.worker = 4096
        with pytest.raises(ValueError, match="worker must be a mapping"):
            validate_config(atl06_config)

    def test_unknown_key_rejected(self, atl06_config):
        # The plan's earlier extra_tmp spelling must not be silently ignored.
        atl06_config.worker = {"memory": 4096, "extra_tmp": True}
        with pytest.raises(ValueError, match=r"Unknown worker keys: \['extra_tmp'\]"):
            validate_config(atl06_config)

    def test_non_bool_extra_disk_rejected(self, atl06_config):
        atl06_config.worker = {"memory": 4096, "extra_disk": "yes"}
        with pytest.raises(ValueError, match=r"worker\.extra_disk must be a boolean"):
            validate_config(atl06_config)

    def test_validated_on_temporal_pipeline_too(self):
        # The block is validated before the pipeline-kind branch, so a bad
        # value fails on temporal configs as well (they fan out on Lambda).
        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "v": {
                        "variable": "PRECT",
                        "collection": "c",
                        "spatial_func": "mean",
                        "temporal_reducer": "sum",
                    }
                }
            },
            pipeline={"type": "temporal"},
            worker={"memory": 512},
        )
        with pytest.raises(ValueError, match=r"worker\.memory must be one of"):
            validate_config(cfg)


class TestNanAmbiguousReductionWarning:
    """Plain min/max/sum + NaN fill warns at validation (espg ruling, issue #201).

    Warning, never error: NaN data is the same bytes as the fill at read, so
    the reduction behaves as its nan-skipping form downstream (the pyramid
    block declares ``nan_policy: "skip"``); declaring the ``nan*`` form states
    that explicitly and silences the warning. The shipped atl06 config trips
    it by design and must stay valid.
    """

    def _cfg(self, function, **meta):
        return {
            "pipeline": {"type": "spatial"},
            "data_source": {
                "reader": "h5coro",
                "coordinates": {"latitude": "/lat", "longitude": "/lon"},
                "variables": {"h": "/h"},
            },
            "aggregation": {
                "variables": {
                    "x": {"source": "h", "function": function, "dtype": "float32", **meta}
                }
            },
            "output": {
                "store": ".",
                "grid": {"type": "healpix", "parent_order": 6, "child_order": 12},
            },
        }

    def test_min_with_explicit_nan_fill_warns(self, caplog):
        validate_config(load_config_from_dict(self._cfg("min", fill_value="NaN")))
        assert "indistinguishable from fill" in caplog.text
        assert "nanmin" in caplog.text and "issue #201" in caplog.text

    def test_float_default_fill_warns_too(self, caplog):
        # No fill_value declared: floats default to the NaN sentinel — the
        # exact posture of the shipped atl06 h_min/h_max declarations.
        validate_config(load_config_from_dict(self._cfg("max")))
        assert "nanmax" in caplog.text and "indistinguishable from fill" in caplog.text

    def test_nan_form_is_silent(self, caplog):
        validate_config(load_config_from_dict(self._cfg("nanmin", fill_value="NaN")))
        assert "indistinguishable from fill" not in caplog.text

    def test_non_nan_fill_is_silent(self, caplog):
        validate_config(load_config_from_dict(self._cfg("min", fill_value=0.0)))
        assert "indistinguishable from fill" not in caplog.text

    def test_shipped_atl06_warns_and_stays_valid(self, caplog):
        cfg = default_config("atl06")  # h_min/h_max: min/max, float32, NaN default
        validate_config(cfg)  # must NOT raise
        assert "h_min" in caplog.text and "h_max" in caplog.text


class TestTimeEncoding:
    """``output.time_encoding`` — the spec §8 declaration (issue #443)."""

    def _raster_cfg(self, encoding=None):
        output = {"grid": {"type": "healpix", "parent_order": 10, "child_order": 16}}
        if encoding is not None:
            output["time_encoding"] = encoding
        return load_config_from_dict(
            {
                "data_source": {
                    "reader": "raster",
                    "bands": {"red": {"asset": "red", "dtype": "uint16"}},
                },
                "output": output,
            }
        )

    def test_absent_and_both_values_validate(self):
        for encoding in (None, "microseconds", "toc"):
            validate_config(self._raster_cfg(encoding))

    def test_unknown_value_rejected(self):
        with pytest.raises(ValueError, match="output.time_encoding must be one of"):
            validate_config(self._raster_cfg("datetime64"))

    def test_toc_rejected_on_a_non_raster_pipeline(self):
        cfg = default_config("atl06")
        cfg.output["time_encoding"] = "toc"
        with pytest.raises(ValueError, match="applies to raster"):
            validate_config(cfg)

    def test_default_still_validates_on_a_non_raster_pipeline(self):
        cfg = default_config("atl06")
        cfg.output["time_encoding"] = "microseconds"
        validate_config(cfg)

    def test_packaged_sentinel2_config_declares_toc(self):
        cfg = default_config("sentinel2_l2a")
        validate_config(cfg)
        assert cfg.output["time_encoding"] == "toc"


class TestNanFillCanonicalization:
    """Float-NaN ``fill_value`` normalizes to the string ``"NaN"`` at load
    (issue #448).

    YAML ``.nan`` parses to a float NaN; ``json.dumps`` emits the non-standard
    token ``NaN``, and Lambda's strict parser refuses the whole dispatch
    payload (``InvalidRequestContentException``). The string form is the
    grammar's native one, so the fix is a canonicalization at the single load
    funnel rather than a new spelling rule for config authors.
    """

    def _cfg_dict(self, fill):
        return {
            "data_source": {
                "reader": "h5coro",
                "coordinates": {"latitude": "/lat", "longitude": "/lon"},
                "variables": {"h": "/h"},
            },
            "aggregation": {
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {
                    "h_mean": {
                        "function": "mean",
                        "source": "h",
                        "dtype": "float32",
                        "fill_value": fill,
                    }
                },
            },
            "output": {
                "store": ".",
                "grid": {"type": "healpix", "parent_order": 6, "child_order": 12},
            },
        }

    def test_yaml_nan_loads_as_the_string(self, tmp_path):
        import yaml

        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump(self._cfg_dict(float("nan"))))
        assert "fill_value: .nan" in path.read_text()  # the YAML spelling under test
        cfg = load_config(str(path))
        assert cfg.aggregation["variables"]["h_mean"]["fill_value"] == "NaN"

    def test_dict_nan_normalizes(self):
        cfg = load_config_from_dict(self._cfg_dict(float("nan")))
        assert cfg.aggregation["variables"]["h_mean"]["fill_value"] == "NaN"

    def test_nan_free_config_is_not_copied(self):
        # No NaN anywhere -> the input sub-dicts are passed through untouched,
        # so callers that hold a reference (and the identity assumptions in
        # the runner's per-cell config splicing) are unaffected.
        d = self._cfg_dict(0.0)
        cfg = load_config_from_dict(d)
        assert cfg.aggregation is d["aggregation"]
        assert cfg.data_source is d["data_source"]

    def test_normalized_config_serializes_strictly(self):
        cfg = load_config_from_dict(self._cfg_dict(float("nan")))
        json.dumps(asdict(cfg), allow_nan=False)  # must not raise

    def test_both_spellings_hash_identically(self):
        from zagg.semantics import semantic_hash

        from_nan = load_config_from_dict(self._cfg_dict(float("nan")))
        from_str = load_config_from_dict(self._cfg_dict("NaN"))
        assert semantic_hash(from_nan) == semantic_hash(from_str)

    def test_string_form_is_a_nan_fill_and_reaches_the_template(self, tmp_path):
        # The two consumers the normalization hands the string to: the
        # #201 warning's NaN test, and the healpix template's fill_value.
        import zarr
        from zarr.storage import MemoryStore

        from zagg.config import _is_nan_fill
        from zagg.grids import from_config

        cfg = load_config_from_dict(self._cfg_dict(float("nan")))
        assert _is_nan_fill(cfg.aggregation["variables"]["h_mean"])
        grid = from_config(cfg)
        store = grid.emit_template(MemoryStore())
        group = zarr.open_group(store, path=grid.group_path, mode="r")
        assert np.isnan(group["h_mean"].fill_value)

    def test_nan_in_a_tuple_normalizes(self):
        # Fold review: the walk recursed into dict/list only, so a
        # Python-built config holding a tuple of dicts slipped past it.
        d = self._cfg_dict(0.0)
        d["data_source"]["filters"] = ({"dataset": "/h", "op": "le", "fill_value": float("nan")},)
        cfg = load_config_from_dict(d)
        assert cfg.data_source["filters"][0]["fill_value"] == "NaN"
        assert isinstance(cfg.data_source["filters"], tuple)  # container type preserved

    def test_np_float32_nan_normalizes(self):
        # Fold review: np.float64 is a float subclass (caught either way);
        # np.float32 is not, and survived as a float NaN.
        cfg = load_config_from_dict(self._cfg_dict(np.float32("nan")))
        assert cfg.aggregation["variables"]["h_mean"]["fill_value"] == "NaN"


class TestNonFiniteFloatsAreRefusedAtValidation:
    """Any non-finite float OUTSIDE ``fill_value`` is a config error (issue
    #448 fold review).

    Canonicalization is scoped to ``fill_value`` on purpose — rewriting every
    float NaN in the tree would silently mangle authored values — so the
    guarantee "no config reaches a dispatch payload strict JSON refuses" is
    made real at validation instead: one ``json.dumps(asdict(cfg),
    allow_nan=False)``-equivalent check, with the offending path named, so the
    failure lands at load time rather than as an opaque
    ``InvalidRequestContentException`` one dispatch later.
    """

    def _cfg_dict(self):
        return {
            "data_source": {
                "reader": "h5coro",
                "coordinates": {"latitude": "/lat", "longitude": "/lon"},
                "variables": {"h": "/h"},
            },
            "aggregation": {
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {
                    "h_mean": {
                        "function": "mean",
                        "source": "h",
                        "dtype": "float32",
                        "fill_value": "NaN",
                    }
                },
            },
            "output": {
                "store": ".",
                "grid": {"type": "healpix", "parent_order": 6, "child_order": 12},
            },
        }

    def test_baseline_config_validates(self):
        validate_config(load_config_from_dict(self._cfg_dict()))

    def test_filter_value_nan_is_refused_with_its_path(self):
        d = self._cfg_dict()
        d["data_source"]["filters"] = [{"dataset": "/h", "op": "le", "value": float("nan")}]
        with pytest.raises(ValueError, match=r"data_source\.filters\[0\]\.value"):
            validate_config(load_config_from_dict(d))

    def test_attrs_nan_is_refused_with_its_path(self):
        d = self._cfg_dict()
        d["aggregation"]["variables"]["h_mean"]["attrs"] = {"scale": float("nan")}
        with pytest.raises(ValueError, match=r"aggregation\.variables\.h_mean\.attrs\.scale"):
            validate_config(load_config_from_dict(d))

    def test_infinity_is_refused_too(self):
        # allow_nan=False rejects Infinity on the same line as NaN.
        d = self._cfg_dict()
        d["bounds"] = {"max_h": float("inf")}
        with pytest.raises(ValueError, match=r"bounds\.max_h"):
            validate_config(load_config_from_dict(d))

    def test_np_float32_nan_is_refused(self):
        # json.dumps calls this an unserializable TYPE, not an out-of-range
        # float, so the walk (not the serializer) is what catches it.
        d = self._cfg_dict()
        d["bounds"] = {"max_h": np.float32("nan")}
        with pytest.raises(ValueError, match=r"bounds\.max_h"):
            validate_config(load_config_from_dict(d))

    def test_error_names_the_remedy(self):
        d = self._cfg_dict()
        d["bounds"] = {"max_h": float("nan")}
        with pytest.raises(ValueError) as e:
            validate_config(load_config_from_dict(d))
        assert "issue #448" in str(e.value) and "strict" in str(e.value)

    def test_canonicalized_fill_value_still_validates(self):
        # The load-time canonicalization is the semantic normalization; this
        # check is the backstop, not a replacement -- a YAML ``.nan``
        # fill_value must still sail through.
        d = self._cfg_dict()
        d["aggregation"]["variables"]["h_mean"]["fill_value"] = float("nan")
        validate_config(load_config_from_dict(d))

    def test_temporal_pipeline_is_checked_too(self):
        # The check runs before validate_config's pipeline-kind branch, so a
        # temporal config (which returns early) is covered as well.
        d = self._cfg_dict()
        d["pipeline"] = {"type": "temporal"}
        d["bounds"] = {"max_h": float("nan")}
        with pytest.raises(ValueError, match=r"bounds\.max_h"):
            validate_config(load_config_from_dict(d))

    def test_non_json_types_are_left_alone(self):
        # A non-JSON *type* is a different fault; validation must not newly
        # reject configs over it.
        from datetime import datetime as _dt

        from zagg.config import _validate_json_floats

        d = self._cfg_dict()
        d["bounds"] = {"temporal": {"start": _dt(2020, 1, 1)}}
        _validate_json_floats(load_config_from_dict(d))  # must not raise


class TestTouchPolicy:
    """``output.touch`` (issue #501): declare the lifecycle-touch behaviour
    instead of inferring it from the bucket name."""

    def test_defaults_to_auto(self):
        from zagg.config import default_config, get_touch_policy

        assert get_touch_policy(default_config()) == "auto"

    def test_reads_the_declared_value(self):
        from zagg.config import default_config, get_touch_policy

        cfg = default_config()
        for value in ("auto", "always", "never"):
            cfg.output["touch"] = value
            assert get_touch_policy(cfg) == value

    def test_rejects_anything_outside_the_three_values(self):
        # Validated at LOAD so a typo fails at submission rather than
        # resolving to the default deep inside a worker's skip path, where the
        # wrong answer is either version churn on a published store or a
        # collaborator's data expiring.
        import pytest

        from zagg.config import _validate_store_layout_keys, default_config

        cfg = default_config()
        cfg.output["touch"] = "sometimes"
        with pytest.raises(ValueError, match="output.touch must be one of"):
            _validate_store_layout_keys(cfg)

    def test_absent_is_legal(self):
        from zagg.config import _validate_store_layout_keys, default_config

        cfg = default_config()
        cfg.output.pop("touch", None)
        _validate_store_layout_keys(cfg)

    def test_unvalidated_typo_warns_before_falling_through_to_auto(self, caplog):
        # validate_config catches a typo at submission, but the worker's own
        # funnel (load_config_from_dict) does not validate -- so a hand-built
        # invoke event carrying `touch: "nevr"` reaches here. Fall-through to
        # `auto` is deliberate (fail-open), but it must be GREPPABLE rather
        # than silent (review finding on PR #496).
        import logging

        from zagg.config import default_config, get_touch_policy

        cfg = default_config()
        cfg.output["touch"] = "nevr"
        with caplog.at_level(logging.WARNING, logger="zagg.config"):
            assert get_touch_policy(cfg) == "nevr"  # fall-through is `auto` in _touch_applies
        assert any("'nevr'" in r.message and "'auto'" in r.message for r in caplog.records)


class TestPackagedConfigsAreDispatchable:
    """Every packaged config must survive the strict JSON the Lambda dispatch
    payload is built with (issue #448).

    The dispatch event's ``config`` block is ``dataclasses.asdict(config)``
    (``runner._dispatch_lambda`` -> ``_invoke_lambda_ping`` /
    ``_invoke_lambda``), serialized by ``json.dumps``. ``allow_nan=False`` is
    exactly what Lambda's parser enforces, so a config that fails here is a
    config that cannot be dispatched.
    """

    def _packaged_names(self):
        from importlib import resources

        import zagg.configs

        return sorted(
            p.name[: -len(".yaml")]
            for p in resources.files(zagg.configs).iterdir()
            if p.name.endswith(".yaml")
        )

    def test_names_found(self):
        names = self._packaged_names()
        assert "atl06" in names and "gedi01b_waveform_healpix_hive" in names

    def test_every_packaged_config_serializes_strictly(self):
        for name in self._packaged_names():
            cfg = default_config(name)
            json.dumps(asdict(cfg), allow_nan=False)  # must not raise

    def test_gedi_companions_declare_the_string_form(self):
        cfg = default_config("gedi01b_waveform_healpix_hive")
        companions = [
            "noise_mean",
            "noise_stddev",
            "rx_energy",
            "elevation_bin0",
            "elevation_lastbin",
        ]
        for name in companions:
            assert cfg.aggregation["variables"][name]["fill_value"] == "NaN"
