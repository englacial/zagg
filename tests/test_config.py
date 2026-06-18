"""Tests for the YAML pipeline configuration system."""

from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest

from zagg.config import (
    PipelineConfig,
    default_config,
    evaluate_expression,
    get_agg_fields,
    get_base_level,
    get_child_order,
    get_coords,
    get_data_vars,
    get_filters,
    get_levels,
    get_output_signature,
    get_store_path,
    load_config,
    load_config_from_dict,
    output_field_signature,
    resolve_function,
    validate_config,
)
from zagg.processing import calculate_cell_statistics

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
        coords = get_coords(atl06_config)
        assert "cell_ids" in coords
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
        assert sig == {"kind": "scalar", "trailing_shape": (), "inner_shape": (), "dtype": "float32"}

    def test_scalar_default_dtype_none(self):
        sig = get_output_signature({"function": "min"})
        assert sig == {"kind": "scalar", "trailing_shape": (), "inner_shape": (), "dtype": None}

    def test_vector_int_signature(self):
        sig = get_output_signature({"kind": "vector", "trailing_shape": 64, "dtype": "float32"})
        assert sig == {
            "kind": "vector",
            "trailing_shape": (64,),
            "inner_shape": (),
            "dtype": "float32",
        }

    def test_vector_list_signature(self):
        sig = get_output_signature({"kind": "vector", "trailing_shape": [16, 2]})
        assert sig["kind"] == "vector"
        assert sig["trailing_shape"] == (16, 2)
        assert sig["inner_shape"] == ()


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
        result = calculate_cell_statistics(
            {"h_ph": h_ph, "leaf_id": np.arange(50)}, config=cfg
        )
        wc = result["waveform_counts"]
        assert wc.shape == (128,)
        assert wc.dtype == np.dtype("uint32")
        assert int(wc.sum()) == 50, "all in-range photons must be counted"

    def test_out_of_range_photons_dropped(self, cfg):
        from zagg.processing import calculate_cell_statistics

        # Two photons clustered near 0, one far outlier at 500 m. The cell median
        # is ~5 m, so the outlier sits beyond ±128 m and falls outside the hist.
        h_ph = np.array([0.0, 10.0, 500.0], dtype="float32")
        result = calculate_cell_statistics(
            {"h_ph": h_ph, "leaf_id": np.arange(3)}, config=cfg
        )
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
        with pytest.raises(ValueError, match="'trailing_shape' is only valid for 'vector', not 'ragged'"):
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
