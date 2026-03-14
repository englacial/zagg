"""YAML-driven pipeline configuration for magg."""

import importlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import resources
from typing import NotRequired, TypedDict

import numpy as np
import yaml

import magg.configs


class DataSourceDict(TypedDict):
    """Type hints for the ``data_source`` section of a pipeline config."""

    reader: str
    groups: list[str]
    coordinates: dict[str, str]
    variables: dict[str, str]
    quality_filter: NotRequired[dict]


@dataclass
class PipelineConfig:
    """Full pipeline configuration.

    Parameters
    ----------
    data_source : DataSourceDict
        Reader, groups, coordinates, variables, quality filter.
    aggregation : dict
        Coordinate and variable aggregation definitions.
    output : dict
        Grid spec, store path, and indexing details.
    catalog : str or None
        Optional path to granule catalog JSON.
    bounds : dict or None
        Optional temporal/spatial bounds for filtering.
    """

    data_source: DataSourceDict = field(default_factory=dict)
    aggregation: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    catalog: str | None = None
    bounds: dict | None = None


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
        catalog=d.get("catalog"),
        bounds=d.get("bounds"),
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

    # Validate output.grid structure
    grid = config.output.get("grid")
    if grid is not None:
        if not isinstance(grid, dict):
            raise ValueError("output.grid must be a mapping (e.g. type: healpix, child_order: 12)")
        if "type" not in grid:
            raise ValueError("output.grid.type is required")
        if grid["type"] == "healpix" and "child_order" not in grid:
            raise ValueError("output.grid.child_order is required for healpix grid")

    # Validate bounds structure (optional)
    if config.bounds is not None:
        allowed_keys = {"temporal", "spatial"}
        unknown = set(config.bounds.keys()) - allowed_keys
        if unknown:
            raise ValueError(f"Unknown bounds keys: {unknown} (allowed: {allowed_keys})")
        temporal = config.bounds.get("temporal")
        if temporal is not None:
            if "start_date" not in temporal or "end_date" not in temporal:
                raise ValueError("bounds.temporal requires start_date and end_date")

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

        # Validate params: bare column names, numeric literals, or expressions
        for pval in meta.get("params", {}).values():
            if not isinstance(pval, str):
                continue  # numeric literal
            if pval in ds_vars or _is_numeric(pval):
                continue  # column reference or number
            # Expression containing column names (e.g. "1.0 / s_li**2")
            if any(v in pval for v in ds_vars):
                continue
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
    - No dot (e.g. ``"min"``) -> ``np.<name>``
    - Dotted path (e.g. ``"np.quantile"``) -> importlib resolution

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

    # Normalize np. prefix to numpy lookup
    if name.startswith("np."):
        name = name[3:]

    if "." not in name:
        # numpy shorthand
        func = getattr(np, name, None)
        if func is not None and callable(func):
            return func
        raise ValueError(f"Cannot resolve '{name}' as numpy function")

    # Dotted path (e.g. numpy.quantile)
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


def get_child_order(config: PipelineConfig) -> int:
    """Return child_order from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    int

    Raises
    ------
    ValueError
        If child_order is not set in the config.
    """
    grid = config.output.get("grid", {})
    child_order = grid.get("child_order")
    if child_order is None:
        raise ValueError("output.grid.child_order is required")
    return int(child_order)


def get_store_path(config: PipelineConfig) -> str | None:
    """Return the store path from the output config, or None.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("store")


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
