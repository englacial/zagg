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
    get_child_order,
    get_coords,
    get_data_vars,
    get_store_path,
    load_config,
    load_config_from_dict,
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
    return pd.DataFrame({
        "h_li": np.array([120.5, 118.3, 122.1, 119.7, 121.0], dtype=np.float32),
        "s_li": np.array([0.05, 0.10, 0.03, 0.08, 0.06], dtype=np.float32),
    })


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
            aggregation={"variables": {
                "bad": {"function": "min", "source": "nonexistent", "dtype": "float32"},
            }},
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="source.*nonexistent"):
            validate_config(cfg)

    def test_missing_weights_column(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {
                "bad": {
                    "function": "average",
                    "source": "h_li",
                    "params": {"weights": "missing_col"},
                    "dtype": "float32",
                },
            }},
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="missing_col"):
            validate_config(cfg)

    def test_expression_unknown_column(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {
                "bad": {
                    "expression": "unknown_col + 1",
                    "dtype": "float32",
                },
            }},
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="unknown_col"):
            validate_config(cfg)

    def test_function_and_expression_mutual_exclusion(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {
                "bad": {
                    "function": "min",
                    "expression": "np.min(h_li)",
                    "source": "h_li",
                    "dtype": "float32",
                },
            }},
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            validate_config(cfg)

    def test_neither_function_nor_expression(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {
                "bad": {"source": "h_li", "dtype": "float32"},
            }},
            output={"grid": {"type": "healpix", "child_order": 12}},
        )
        with pytest.raises(ValueError, match="must specify"):
            validate_config(cfg)

    def test_missing_required_section(self):
        cfg = PipelineConfig(data_source={}, aggregation={"variables": {}}, output={"grid": {"type": "x"}})
        with pytest.raises(ValueError, match="Missing required section"):
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
                ns = {"__builtins__": {}, "np": np, "numpy": np,
                      **{c: df[c].values for c in df.columns}}
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
        expected = calculate_cell_statistics(synthetic_df)
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
            "data_source": {"variables": {"h_li": "/path"}, "reader": "h5coro",
                            "groups": ["gt1l"], "coordinates": {"latitude": "/lat", "longitude": "/lon"}},
            "aggregation": {"variables": {"count": {"function": "len", "source": "h_li", "dtype": "int32"}},
                            "coordinates": {"cell_ids": {"dtype": "uint64"}}},
            "output": {"grid": {"type": "healpix", "child_order": 12}},
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
        cfg = PipelineConfig(output={"store": "./test.zarr", "grid": {"type": "healpix", "child_order": 12}})
        assert get_store_path(cfg) == "./test.zarr"

    def test_get_store_path_none(self, atl06_config):
        assert get_store_path(atl06_config) is None

    def test_get_store_path_s3(self):
        cfg = PipelineConfig(output={"store": "s3://bucket/prefix.zarr", "grid": {"type": "healpix", "child_order": 12}})
        assert get_store_path(cfg) == "s3://bucket/prefix.zarr"


# ---------------------------------------------------------------------------
# Output grid validation
# ---------------------------------------------------------------------------

class TestOutputGridValidation:
    def test_grid_must_be_dict(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}},
            output={"grid": "healpix"},
        )
        with pytest.raises(ValueError, match="output.grid must be a mapping"):
            validate_config(cfg)

    def test_grid_missing_type(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}},
            output={"grid": {"child_order": 12}},
        )
        with pytest.raises(ValueError, match="output.grid.type"):
            validate_config(cfg)

    def test_healpix_missing_child_order(self):
        cfg = PipelineConfig(
            data_source={"variables": {"h_li": "/path"}},
            aggregation={"variables": {"c": {"function": "len", "source": "h_li", "dtype": "int32"}}},
            output={"grid": {"type": "healpix"}},
        )
        with pytest.raises(ValueError, match="child_order"):
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
        atl06_config.bounds = {"temporal": {"start_date": "2024-01-01", "end_date": "2024-06-01"}, "foo": "bar"}
        with pytest.raises(ValueError, match="Unknown bounds keys"):
            validate_config(atl06_config)

    def test_temporal_missing_dates(self, atl06_config):
        atl06_config.bounds = {"temporal": {"start_date": "2024-01-01"}}
        with pytest.raises(ValueError, match="start_date and end_date"):
            validate_config(atl06_config)

    def test_none_bounds_ok(self, atl06_config):
        atl06_config.bounds = None
        validate_config(atl06_config)
