"""YAML-driven pipeline configuration for magg."""

import importlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import resources

import numpy as np
import yaml

import magg.configs


@dataclass
class PipelineConfig:
    """Full pipeline configuration.

    Parameters
    ----------
    data_source : dict
        Reader, groups, coordinates, variables, quality filter.
    aggregation : dict
        Coordinate and variable aggregation definitions.
    output : dict
        Grid type and indexing scheme.
    """

    data_source: dict = field(default_factory=dict)
    aggregation: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)


def load_config(path: str) -> PipelineConfig:
    """Load a YAML config file and return a validated PipelineConfig.

    Parameters
    ----------
    path : str
        Path to YAML file.

    Returns
    -------
    PipelineConfig
    """
    with open(path) as f:
        d = yaml.safe_load(f)
    cfg = load_config_from_dict(d)
    validate_config(cfg)
    return cfg


def load_config_from_dict(d: dict) -> PipelineConfig:
    """Build a PipelineConfig from a plain dict (e.g. Lambda JSON payload).

    Parameters
    ----------
    d : dict
        Dictionary with keys ``data_source``, ``aggregation``, ``output``.

    Returns
    -------
    PipelineConfig
    """
    return PipelineConfig(
        data_source=d.get("data_source", {}),
        aggregation=d.get("aggregation", {}),
        output=d.get("output", {}),
    )


def default_config(name: str = "atl06") -> PipelineConfig:
    """Load a built-in YAML config shipped with the package.

    Parameters
    ----------
    name : str
        Config name (without ``.yaml`` extension). Default ``"atl06"``.

    Returns
    -------
    PipelineConfig

    Raises
    ------
    FileNotFoundError
        If the named config does not exist.
    """
    ref = resources.files(magg.configs).joinpath(f"{name}.yaml")
    if not ref.is_file():
        raise FileNotFoundError(f"No built-in config named '{name}'")
    text = ref.read_text(encoding="utf-8")
    d = yaml.safe_load(text)
    cfg = load_config_from_dict(d)
    validate_config(cfg)
    return cfg


def validate_config(config: PipelineConfig) -> None:
    """Cross-validate a PipelineConfig.

    Parameters
    ----------
    config : PipelineConfig

    Raises
    ------
    ValueError
        On any validation failure.
    """
    # Required sections
    for section in ("data_source", "aggregation", "output"):
        val = getattr(config, section)
        if not val:
            raise ValueError(f"Missing required section: {section}")

    ds_vars = set(config.data_source.get("variables", {}).keys())
    agg_vars = config.aggregation.get("variables", {})

    for name, meta in agg_vars.items():
        has_func = "function" in meta
        has_expr = "expression" in meta

        # Mutual exclusivity
        if has_func and has_expr:
            raise ValueError(
                f"Variable '{name}': 'function' and 'expression' are mutually exclusive"
            )

        # Must have one (count via function:len is allowed)
        if not has_func and not has_expr:
            raise ValueError(
                f"Variable '{name}': must specify 'function' or 'expression'"
            )

        # Validate source references
        source = meta.get("source")
        if source is not None and source not in ds_vars:
            raise ValueError(
                f"Variable '{name}': source '{source}' not in data_source.variables"
            )

        # Validate function resolves
        if has_func:
            resolve_function(meta["function"])  # raises ValueError on failure

        # Validate params column references (e.g. weights: s_li)
        for pval in meta.get("params", {}).values():
            if isinstance(pval, str) and pval in config.data_source.get("variables", {}):
                pass  # valid column reference
            elif isinstance(pval, str) and pval not in config.data_source.get("variables", {}) and not _is_numeric(pval):
                # Check if it looks like a column reference (not a number)
                raise ValueError(
                    f"Variable '{name}': param value '{pval}' references "
                    f"unknown column (available: {ds_vars})"
                )

        # Validate expression column references
        if has_expr:
            _validate_expression_columns(name, meta["expression"], ds_vars)


def _is_numeric(s: str) -> bool:
    """Check if a string is a numeric literal."""
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _validate_expression_columns(var_name: str, expr: str, ds_vars: set[str]) -> None:
    """Check that identifiers in an expression that look like column names exist."""
    # Extract bare identifiers
    tokens = set(re.findall(r"\b([a-zA-Z_]\w*)\b", expr))
    # Remove known safe names
    safe = {"np", "numpy", "len", "sum", "sqrt", "abs", "log", "exp", "float", "int"}
    for tok in tokens - safe:
        if tok in ds_vars:
            continue
        # If it's an attribute (e.g. np.sqrt) the parent object handles it
        # Only flag tokens that could plausibly be columns but aren't
        if tok not in dir(np) and not hasattr(np, tok):
            raise ValueError(
                f"Variable '{var_name}': expression references '{tok}' "
                f"which is not in data_source.variables or numpy namespace"
            )


def resolve_function(name: str) -> Callable:
    """Resolve a function name to a callable.

    Resolution rules:
    - ``"len"`` or ``"count"`` -> builtin ``len``
    - No dot (e.g. ``"min"``) -> ``numpy.<name>``
    - Dotted path (e.g. ``"numpy.quantile"``) -> importlib resolution

    Parameters
    ----------
    name : str
        Function name or dotted path.

    Returns
    -------
    Callable

    Raises
    ------
    ValueError
        If the name cannot be resolved to a callable.
    """
    if name in ("len", "count"):
        return len

    if "." not in name:
        # numpy shorthand
        func = getattr(np, name, None)
        if func is not None and callable(func):
            return func
        raise ValueError(f"Cannot resolve '{name}' as numpy function")

    # Dotted path
    parts = name.rsplit(".", 1)
    try:
        mod = importlib.import_module(parts[0])
        func = getattr(mod, parts[1])
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Cannot resolve '{name}': {e}") from e

    if not callable(func):
        raise ValueError(f"'{name}' is not callable")
    return func


def get_agg_fields(config: PipelineConfig) -> dict:
    """Return aggregation variable metadata keyed by variable name.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict
        ``{name: {function/expression, source, params, dtype, fill_value, ...}}``
    """
    return dict(config.aggregation.get("variables", {}))


def get_coords(config: PipelineConfig) -> list[str]:
    """Return coordinate column names from the aggregation config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[str]
    """
    return list(config.aggregation.get("coordinates", {}).keys())


def get_data_vars(config: PipelineConfig) -> list[str]:
    """Return data variable column names from the aggregation config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[str]
    """
    return list(config.aggregation.get("variables", {}).keys())


def evaluate_expression(expression: str, columns: dict[str, np.ndarray]) -> float:
    """Evaluate an expression string in a restricted namespace.

    Parameters
    ----------
    expression : str
        Python expression using numpy and column variables.
    columns : dict[str, np.ndarray]
        Mapping of column names to arrays.

    Returns
    -------
    float
    """
    ns = {
        "__builtins__": {},
        "np": np,
        "numpy": np,
        "len": len,
        "float": float,
        "int": int,
        "abs": abs,
        "sum": sum,
        **columns,
    }
    return float(eval(expression, ns))  # noqa: S307
