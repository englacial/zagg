"""YAML-driven pipeline configuration for zagg."""

import importlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import resources
from typing import Any, NotRequired, TypedDict

import numpy as np
import yaml

import zagg.configs


class DataSourceDict(TypedDict):
    """Type hints for the ``data_source`` section of a pipeline config."""

    reader: str
    groups: list[str]
    coordinates: dict[str, str]
    variables: dict[str, str]
    quality_filter: NotRequired[dict]
    filters: NotRequired[list[dict]]


# Structured-predicate comparison operators (issue #43). ``in``/``not_in`` take a
# ``values`` list; the rest take a scalar ``value``. These are the only
# pushdown-eligible filter language; an ``expression`` filter is a base-level-only,
# aggregation-time escape hatch that forfeits pushdown.
_SCALAR_OPS = frozenset({"eq", "ne", "ge", "le", "lt", "gt"})
_SET_OPS = frozenset({"in", "not_in"})
FILTER_OPS = _SCALAR_OPS | _SET_OPS


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
    ref = resources.files(zagg.configs).joinpath(f"{name}.yaml")
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
        if grid["type"] == "healpix" and "parent_order" not in grid:
            raise ValueError("output.grid.parent_order is required for healpix grid")
        if grid["type"] == "rectilinear":
            for field in ("crs", "resolution", "bounds"):
                if field not in grid:
                    raise ValueError(f"output.grid.{field} is required for rectilinear grid")
            if len(grid["bounds"]) != 4:
                raise ValueError("output.grid.bounds must be [xmin, ymin, xmax, ymax]")
        layout = grid.get("layout")
        if layout is not None and layout not in ("dense", "fullsphere"):
            raise ValueError(f"output.grid.layout must be 'dense' or 'fullsphere' (got {layout!r})")

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

    # Validate the structured filter list (issue #43, Phase A)
    _validate_filters(config.data_source)

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
            raise ValueError(f"Variable '{name}': must specify 'function' or 'expression'")

        # Validate source references
        source = meta.get("source")
        if source is not None and source not in ds_vars:
            raise ValueError(f"Variable '{name}': source '{source}' not in data_source.variables")

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

        # Validate the output-kind declaration (kind + trailing_shape + dtype)
        _validate_output_kind(name, meta)


# Recognized per-field output kinds. ``ragged`` (CSR) is Tier 2 and not yet
# accepted; see issue #29.
OUTPUT_KINDS = ("scalar", "vector")


def _validate_output_kind(name: str, meta: dict) -> None:
    """Validate a variable's non-scalar output declaration.

    A field may declare ``kind`` (``scalar`` default, or ``vector``) and
    ``trailing_shape`` (required for ``vector``). ``scalar`` fields need
    neither and stay the default path. A ``vector`` field may be driven by
    either ``function`` or ``expression``; ``len``/``count`` are rejected for
    ``vector`` (they short-circuit to a scalar count). See issue #29.

    Parameters
    ----------
    name : str
        Variable name (for error messages).
    meta : dict
        The variable's aggregation metadata.

    Raises
    ------
    ValueError
        On any invalid output-kind declaration.
    """
    kind = meta.get("kind", "scalar")
    if kind not in OUTPUT_KINDS:
        allowed = ", ".join(OUTPUT_KINDS)
        raise ValueError(
            f"Variable '{name}': output kind '{kind}' is not supported "
            f"(allowed: {allowed}; 'ragged' is planned but not yet implemented)"
        )

    # dtype, when declared, must name a real numpy dtype (applies to all kinds).
    if "dtype" in meta:
        try:
            np.dtype(meta["dtype"])
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Variable '{name}': dtype {meta['dtype']!r} is not a valid numpy dtype ({e})"
            ) from e

    has_trailing = "trailing_shape" in meta

    if kind == "scalar":
        if has_trailing:
            raise ValueError(
                f"Variable '{name}': 'trailing_shape' is only valid for kind 'vector', not 'scalar'"
            )
        return

    # kind == "vector": trailing_shape is required and must be positive ints.
    if not has_trailing:
        raise ValueError(f"Variable '{name}': kind 'vector' requires 'trailing_shape'")
    _validate_trailing_shape(name, meta["trailing_shape"])

    # ``len``/``count`` short-circuit to a scalar obs count in
    # ``calculate_cell_statistics``; pairing them with kind 'vector' would
    # silently emit a scalar, so reject the nonsensical combination.
    if meta.get("function") in ("len", "count"):
        raise ValueError(
            f"Variable '{name}': function {meta['function']!r} produces a scalar "
            f"count and cannot be combined with kind 'vector'"
        )


def _validate_trailing_shape(name: str, trailing_shape) -> None:
    """Check a vector field's trailing_shape is a tuple of positive ints."""
    if isinstance(trailing_shape, int):
        dims: tuple = (trailing_shape,)
    elif isinstance(trailing_shape, (list, tuple)):
        dims = tuple(trailing_shape)
    else:
        raise ValueError(
            f"Variable '{name}': 'trailing_shape' must be an int or a "
            f"sequence of ints (got {trailing_shape!r})"
        )
    if not dims:
        raise ValueError(f"Variable '{name}': 'trailing_shape' must have at least one dimension")
    for dim in dims:
        if not isinstance(dim, int) or isinstance(dim, bool) or dim < 1:
            raise ValueError(
                f"Variable '{name}': 'trailing_shape' entries must be positive "
                f"integers (got {dim!r})"
            )


def get_filters(config: PipelineConfig) -> list[dict]:
    """Return the ordered list of normalized data-source filters (issue #43).

    Two filter languages coexist:

    - **Structured predicates** ``{level?, dataset, column?, op, value|values,
      keep?}`` are machine-inspectable and are the only kind eligible for read
      pushdown (Phase C). ``op`` is one of :data:`FILTER_OPS`; ``in``/``not_in``
      take ``values`` (a list), the rest take a scalar ``value``. ``column`` is an
      integer selector into an N-D flag array (e.g. ATL03 ``signal_conf_ph``).
      ``keep`` (default ``True``) keeps matching rows; ``keep: false`` drops them.
    - **Expression** filters ``{expression: "<py expr>"}`` are a base-level-only,
      aggregation-time escape hatch that forfeits pushdown (opaque to the planner).

    The flat ``quality_filter: {dataset, value}`` is sugar synthesizing one
    base-level ``op: eq`` structured filter, so the ATL06 path is unchanged. An
    explicit ``filters:`` list, when present, is used as-is (the flat
    ``quality_filter`` is then ignored).

    Each returned filter carries a normalized ``level`` (``None`` means the base
    level) and, for structured predicates, an explicit ``keep`` bool.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list[dict]
    """
    return filters_from_data_source(config.data_source)


def filters_from_data_source(data_source: dict) -> list[dict]:
    """Normalize the filter list from a raw ``data_source`` dict.

    Shared by :func:`get_filters` and the read path (which only holds the
    ``data_source`` mapping). See :func:`get_filters` for the schema.
    """
    explicit = data_source.get("filters")
    if explicit is not None:
        return [_normalize_filter(f) for f in explicit]
    qf = data_source.get("quality_filter")
    if qf is not None:
        return [
            {
                "level": None,
                "dataset": qf["dataset"],
                "column": None,
                "op": "eq",
                "value": qf["value"],
                "keep": True,
            }
        ]
    return []


def _normalize_filter(f: dict) -> dict:
    """Normalize one raw filter dict into canonical form (see :func:`get_filters`)."""
    if "expression" in f:
        return {"level": f.get("level"), "expression": f["expression"]}
    op = f["op"]
    out = {
        "level": f.get("level"),
        "dataset": f["dataset"],
        "column": f.get("column"),
        "op": op,
        "keep": bool(f.get("keep", True)),
    }
    if op in _SET_OPS:
        out["values"] = list(f["values"])
    else:
        out["value"] = f["value"]
    return out


def _validate_filters(data_source: dict) -> None:
    """Validate the ``filters`` list (and that a flat ``quality_filter`` is sane).

    Raises ``ValueError`` on: unknown op, missing ``dataset``, ``in``/``not_in``
    without a list ``values``, scalar ops without ``value``, non-int ``column``,
    a non-base-level ``expression`` filter, or wrong ``value`` type. ``column`` is
    required for the N-D flag case but cannot be checked against array rank here
    (no data); rank checks happen at read time.
    """
    filters = data_source.get("filters")
    if filters is None:
        return
    if not isinstance(filters, list):
        raise ValueError("data_source.filters must be a list")
    for i, f in enumerate(filters):
        if not isinstance(f, dict):
            raise ValueError(f"filter[{i}] must be a mapping")
        if "expression" in f:
            if "op" in f or "dataset" in f:
                raise ValueError(
                    f"filter[{i}]: 'expression' filters take no 'op'/'dataset' "
                    "(base-level aggregation-time escape hatch, no pushdown)"
                )
            if f.get("level") is not None:
                raise ValueError(
                    f"filter[{i}]: 'expression' filters are base-level only "
                    "(level must be omitted)"
                )
            if not isinstance(f["expression"], str):
                raise ValueError(f"filter[{i}]: 'expression' must be a string")
            continue
        if "dataset" not in f:
            raise ValueError(f"filter[{i}]: structured filter requires 'dataset'")
        op = f.get("op")
        if op not in FILTER_OPS:
            raise ValueError(
                f"filter[{i}]: unknown op {op!r} (allowed: {sorted(FILTER_OPS)})"
            )
        col = f.get("column")
        if col is not None and not isinstance(col, int):
            raise ValueError(
                f"filter[{i}]: 'column' must be an integer index (got {col!r})"
            )
        if op in _SET_OPS:
            if not isinstance(f.get("values"), list):
                raise ValueError(f"filter[{i}]: op {op!r} requires a 'values' list")
            for v in f["values"]:
                if not isinstance(v, (int, float)):
                    raise ValueError(
                        f"filter[{i}]: 'values' must be numeric (got {v!r})"
                    )
        else:
            if "value" not in f:
                raise ValueError(f"filter[{i}]: op {op!r} requires a scalar 'value'")
            if not isinstance(f["value"], (int, float)):
                raise ValueError(
                    f"filter[{i}]: 'value' must be numeric (got {f['value']!r})"
                )


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
        ``{name: {function/expression, source, params, dtype, fill_value, ...}}``.
        A field may also declare a non-scalar output (issue #29) via ``kind``
        (``scalar`` default, or ``vector``) and ``trailing_shape``; use
        :func:`get_output_signature` to read the normalized declaration.
    """
    return dict(config.aggregation.get("variables", {}))


def get_output_signature(meta: dict) -> dict:
    """Return the normalized non-scalar output signature for one agg field.

    This is the single read point for a field's Option B declaration (issue
    #29): its output ``kind``, the per-cell ``trailing_shape``, and ``dtype``.
    Later phases (statistic eval, the per-shard container, and the grid
    ``signature()``) consume this rather than re-parsing the raw metadata.

    Parameters
    ----------
    meta : dict
        A single variable's aggregation metadata (a value of
        :func:`get_agg_fields`).

    Returns
    -------
    dict
        ``{"kind": str, "trailing_shape": tuple[int, ...], "dtype": str}``.
        ``trailing_shape`` is ``()`` for scalar fields. ``dtype`` is the
        declared dtype string, or ``None`` if unset.
    """
    kind = meta.get("kind", "scalar")
    if kind == "vector":
        ts = meta["trailing_shape"]
        trailing_shape = (ts,) if isinstance(ts, int) else tuple(ts)
    else:
        trailing_shape = ()
    return {
        "kind": kind,
        "trailing_shape": trailing_shape,
        "dtype": meta.get("dtype"),
    }


def output_field_signature(config: PipelineConfig) -> list[dict]:
    """Return the Option-B output-field signature for a config (issue #29).

    A canonical, JSON-serializable list of ``{"name", "kind", "trailing_shape",
    "dtype"}`` for every aggregation variable, sorted by ``name``. Recorded in a
    grid's :meth:`signature` so a shard map can never be silently paired with a
    grid whose output schema (scalar vs vector, trailing shape, dtype) differs,
    and compared in ``nests_with`` so co-aggregated grids must share a field set.

    ``trailing_shape`` is rendered as a ``list`` (``()`` for scalar fields) so
    the structure round-trips through JSON unchanged.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    list of dict
    """
    fields = []
    for name, meta in get_agg_fields(config).items():
        sig = get_output_signature(meta)
        fields.append(
            {
                "name": name,
                "kind": sig["kind"],
                "trailing_shape": list(sig["trailing_shape"]),
                "dtype": sig["dtype"],
            }
        )
    return sorted(fields, key=lambda f: f["name"])


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


def get_driver(config: PipelineConfig) -> str:
    """Return the data access driver from the config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"s3"`` or ``"https"``. Defaults to ``"s3"``.
    """
    return config.data_source.get("driver", "s3")


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


def get_parent_order(config: PipelineConfig) -> int:
    """Return parent_order (shard order) from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    int

    Raises
    ------
    ValueError
        If parent_order is not set in the config.
    """
    grid = config.output.get("grid", {})
    parent_order = grid.get("parent_order")
    if parent_order is None:
        raise ValueError("output.grid.parent_order is required")
    return int(parent_order)


def get_layout(config: PipelineConfig) -> str:
    """Return the HEALPix storage layout from the output grid config.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"fullsphere"`` (default) or ``"dense"`` (deprecated).
    """
    return config.output.get("grid", {}).get("layout", "fullsphere")


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


def get_output_endpoint_url(config: PipelineConfig) -> str | None:
    """Return the output S3 endpoint URL from the output config, or None.

    Non-secret S3-compatible endpoint (e.g. R2, MinIO). Credentials are never
    stored in config; they are supplied at runtime.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("endpoint_url")


def get_output_region(config: PipelineConfig) -> str | None:
    """Return the output S3 region from the output config, or None.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.output.get("region")


def _eval_expression_raw(expression: str, columns: dict[str, np.ndarray]) -> Any:
    """Evaluate an expression string in a restricted namespace, uncoerced.

    Returns the expression's native value (a scalar, an ndarray, ...). Used by
    vector ``expression`` fields (issue #29), which coerce the result through
    ``_coerce_field_value`` rather than casting to ``float``.

    Parameters
    ----------
    expression : str
        Python expression using numpy and column variables.
    columns : dict[str, np.ndarray]
        Mapping of column names to arrays.

    Returns
    -------
    Any
        Whatever the expression evaluates to.
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
    return eval(expression, ns)  # noqa: S307


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
    return float(_eval_expression_raw(expression, columns))


def evaluate_filter_expression(
    expression: str, columns: dict[str, np.ndarray]
) -> np.ndarray:
    """Evaluate a boolean filter expression to a per-row mask (issue #43).

    Like :func:`evaluate_expression` but returns the raw boolean array rather than
    a scalar float — the base-level ``expression`` filter escape hatch (e.g.
    ``"(h_li > 0) & (s_li < 1)"``). Uses the same restricted namespace.

    Parameters
    ----------
    expression : str
        Python boolean expression over numpy and column variables.
    columns : dict[str, np.ndarray]
        Mapping of column names to arrays.

    Returns
    -------
    numpy.ndarray
        Boolean mask.
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
    return np.asarray(eval(expression, ns), dtype=bool)  # noqa: S307
