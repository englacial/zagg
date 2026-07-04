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


class LinkDict(TypedDict):
    """Per-level link to the next coarser level (issue #43, Phase B).

    A *link* describes a contiguous-range parent->child tiling: each parent segment
    ``p`` covers base-rate indices ``[index_beg[p] - index_base, ...`` for
    ``count[p]`` children.  ``index_base`` shifts the raw ``index_beg`` values so
    that Python 0-based indexing into the base array is straightforward.

    ``reference_index`` is a reserved slot for a future explicit-index-array variant
    (non-contiguous children per parent); leave it ``None`` for the contiguous case.
    """

    to: str  # key of the coarser level in ``levels``
    index_beg: str  # HDF5 path for the per-parent start index array
    count: str  # HDF5 path for the per-parent child count array
    index_base: NotRequired[int]  # subtracted from index_beg values (default 0)
    reference_index: NotRequired[str | None]  # reserved; must be None


class LevelDict(TypedDict):
    """One hierarchical level in a multi-rate HDF5 source (issue #43, Phase B).

    A source may have several rates (e.g. ATL03 ``photons`` and ``segments``).
    Each level declares its own ``path``, ``coordinates``, and ``variables``,
    plus an optional ``link`` to a coarser parent level.  The flat single-level
    form (no ``levels``/``base_level`` keys in ``data_source``) stays first-class.
    """

    path: str  # HDF5 group path template (may contain ``{group}``)
    coordinates: list[str]  # coordinate dataset names within ``path``
    # ``variables`` has two forms: a documentation-only ``list[str]`` of names, or
    # (non-base levels, issue #30) a ``{name: path-template}`` mapping declaring a
    # *readable* segment-level variable. The mapping form is read at coarse rate and
    # broadcast to the base (photon) rows via ``link`` so e.g. ``dem_h`` (one value
    # per ~100 photons) becomes a per-photon column the aggregation can reduce.
    variables: list[str] | dict[str, str]
    link: NotRequired[LinkDict | None]


class DataSourceDict(TypedDict):
    """Type hints for the ``data_source`` section of a pipeline config."""

    reader: str
    groups: list[str]
    coordinates: dict[str, str]
    variables: dict[str, str]
    quality_filter: NotRequired[dict]
    filters: NotRequired[list[dict]]
    # Hierarchical multi-level form (issue #43, Phase B). When present, the flat
    # ``coordinates``/``variables`` keys are still accepted for the base level but
    # ``levels`` + ``base_level`` take precedence for the read path.
    levels: NotRequired[dict[str, LevelDict]]
    base_level: NotRequired[str]


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
        # Optional cell_ids encoding (issue #135): "nested" (default, the DGGS
        # standard) or "morton" (emit the packed morton words as cell_ids — a
        # test/prototype capability). HEALPix-only: rectilinear grids have no
        # cell_ids coordinate, so the knob would silently do nothing there.
        encoding = grid.get("cell_ids_encoding")
        if encoding is not None:
            if encoding not in ("nested", "morton"):
                raise ValueError(
                    f"output.grid.cell_ids_encoding must be 'nested' or 'morton' (got {encoding!r})"
                )
            if grid["type"] != "healpix":
                raise ValueError(
                    "output.grid.cell_ids_encoding only applies to healpix grids "
                    f"(grid type is {grid['type']!r})"
                )
        # The legacy output.grid.indexing_scheme key is descriptive only (the
        # shipped configs carry "nested"); it does NOT select the cell_ids
        # encoding. Reject any other value so a user reaching for it lands on the
        # real knob instead of a silently-NESTED store.
        legacy_scheme = grid.get("indexing_scheme")
        if legacy_scheme is not None and legacy_scheme != "nested":
            raise ValueError(
                f"output.grid.indexing_scheme is descriptive and must be 'nested' "
                f"(got {legacy_scheme!r}); to emit morton words as cell_ids set "
                f"output.grid.cell_ids_encoding: morton"
            )

    # Validate the optional per-cell carrier (issue #132). Mirrors the worker's
    # ``{"pandas", "arrow"}`` guard (worker.py) so a typo in the aggregation YAML
    # fails at load, not deep in a worker.
    handoff = config.aggregation.get("handoff")
    if handoff is not None and handoff not in ("pandas", "arrow"):
        raise ValueError(f"aggregation.handoff must be 'pandas' or 'arrow' (got {handoff!r})")

    # Optional strict-AOI cell mask (issue #101), default off. Must be a bool.
    aoi_mask = config.output.get("aoi_mask")
    if aoi_mask is not None and not isinstance(aoi_mask, bool):
        raise ValueError(f"output.aoi_mask must be a boolean (got {aoi_mask!r})")

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

    # Validate hierarchical multi-level form (issue #43, Phase B)
    _validate_levels(config.data_source)

    # Cross-check: each filter's level field must name a key in levels (issue #43)
    _validate_filter_levels(config.data_source)

    # Segment-level (non-base) ``variables`` mappings (issue #30) become real
    # per-photon columns in the pooled shard data once broadcast, so they are valid
    # column references everywhere a ``data_source.variables`` column is (agg
    # sources/expressions, chunk_precompute sources). Fold their names into ds_vars.
    ds_vars = set(config.data_source.get("variables", {}).keys()) | _segment_variable_names(
        config.data_source
    )
    agg_vars = config.aggregation.get("variables", {})

    # Validate the per-chunk precompute hook (issue #30, item 1). Each entry is
    # evaluated ONCE per chunk over the shard's pooled column data, before the
    # per-cell loop; its name becomes available in the per-cell expression
    # namespace. Validation mirrors ``aggregation.variables`` (exactly one of
    # function/expression, sources exist) but the entries are chunk-level scalars.
    _validate_chunk_precompute(config.aggregation, ds_vars)

    # Base-level ``expression`` filters evaluate over the read columns at read time
    # (before chunk_precompute), so their valid names are exactly ``ds_vars`` —
    # ``data_source.variables`` plus any broadcast segment-level variable (issue
    # #30). Validate their column references the same way agg/precompute
    # expressions are, so e.g. an ``{expression: "dem_h > ..."}`` filter is accepted.
    for f in filters_from_data_source(config.data_source):
        if "expression" in f:
            _validate_expression_columns(f"filter {f['expression']!r}", f["expression"], ds_vars)

    # Chunk-precompute names are injected into the per-cell expression namespace
    # (issue #30), so a per-cell ``expression`` (or its params) may reference them
    # like a column. Treat them as valid identifiers in the per-cell validation.
    precompute_names = set(config.aggregation.get("chunk_precompute", {}).keys())
    expr_vars = ds_vars | precompute_names

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
            if pval in expr_vars or _is_numeric(pval):
                continue  # column / chunk-precompute reference or number
            # Expression containing column names (e.g. "1.0 / s_li**2")
            if any(v in pval for v in expr_vars):
                continue
            raise ValueError(
                f"Variable '{name}': param value '{pval}' references "
                f"unknown column (available: {expr_vars})"
            )

        # Validate expression column references (chunk-precompute names included)
        if has_expr:
            _validate_expression_columns(name, meta["expression"], expr_vars)

        # Validate the output-kind declaration (kind + trailing_shape + dtype)
        _validate_output_kind(name, meta)


def _validate_chunk_precompute(aggregation: dict, ds_vars: set[str]) -> None:
    """Validate the ``aggregation.chunk_precompute`` block (issue #30, item 1).

    Each named entry is a chunk-level scalar computed ONCE per chunk (shard) over
    the shard's pooled column data, before the per-cell loop; its name then enters
    the per-cell expression namespace. Validation mirrors ``aggregation.variables``:
    each entry must declare exactly one of ``function``/``expression``, and any
    ``source`` / expression / param column references must exist in
    ``data_source.variables``. ``dtype`` (optional) must be a valid numpy dtype.

    A precompute name must not collide with a ``data_source.variables`` column or
    a reserved namespace name (``leaf_id``): the per-cell namespace is built as
    ``{**cell_data, **chunk_scalars}`` in :func:`zagg.processing.process_shard`, so
    a colliding name would shadow the real column *array* with a 0-d *scalar* and
    corrupt every cell. Such names are rejected here.

    Inter-precompute references are NOT supported: an entry's expression is
    evaluated only over the pooled columns (:func:`zagg.processing._eval_chunk_precompute`
    iterates the entries independently, with no defined order), so one entry cannot
    reference another (e.g. ``chunk_gain`` cannot read ``chunk_offset``). A name
    that references another precompute entry is rejected as an unknown column.

    The block is optional; a config without it is unchanged.

    Parameters
    ----------
    aggregation : dict
        The config's ``aggregation`` mapping.
    ds_vars : set[str]
        Available ``data_source.variables`` column names.

    Raises
    ------
    ValueError
        On any invalid ``chunk_precompute`` declaration.
    """
    precompute = aggregation.get("chunk_precompute")
    if precompute is None:
        return
    if not isinstance(precompute, dict):
        raise ValueError("aggregation.chunk_precompute must be a mapping of name -> entry")
    reserved = ds_vars | {"leaf_id"}
    for name, meta in precompute.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("chunk_precompute entry names must be non-empty strings")
        if name in reserved:
            raise ValueError(
                f"chunk_precompute '{name}': name collides with a "
                f"data_source.variables column or the reserved 'leaf_id'; the "
                f"per-cell namespace merge would shadow the real column with a "
                f"chunk scalar. Rename the precompute entry."
            )
        if not isinstance(meta, dict):
            raise ValueError(f"chunk_precompute '{name}': entry must be a mapping")

        has_func = "function" in meta
        has_expr = "expression" in meta
        if has_func and has_expr:
            raise ValueError(
                f"chunk_precompute '{name}': 'function' and 'expression' are mutually exclusive"
            )
        if not has_func and not has_expr:
            raise ValueError(f"chunk_precompute '{name}': must specify 'function' or 'expression'")

        source = meta.get("source")
        if source is not None and source not in ds_vars:
            raise ValueError(
                f"chunk_precompute '{name}': source '{source}' not in data_source.variables"
            )

        if has_func:
            resolve_function(meta["function"])  # raises ValueError on failure

        for pval in meta.get("params", {}).values():
            if not isinstance(pval, str):
                continue  # numeric literal
            if pval in ds_vars or _is_numeric(pval):
                continue
            if any(v in pval for v in ds_vars):
                continue
            raise ValueError(
                f"chunk_precompute '{name}': param value '{pval}' references "
                f"unknown column (available: {ds_vars})"
            )

        if has_expr:
            _validate_expression_columns(name, meta["expression"], ds_vars)

        if "dtype" in meta:
            try:
                np.dtype(meta["dtype"])
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"chunk_precompute '{name}': dtype {meta['dtype']!r} is not a valid "
                    f"numpy dtype ({e})"
                ) from e


# Recognized per-field output kinds. ``ragged`` (CSR) is the Tier-2 carrier
# for variable-length per-cell outputs; see issue #48.
OUTPUT_KINDS = ("scalar", "vector", "ragged")

# Recognized per-field output resolutions (issue #30 item 2). ``cell`` (default)
# stores one value per aggregation cell; ``chunk`` stores one value per chunk in a
# companion array shaped at the chunk grid, indexed by ``grid.block_index``.
OUTPUT_RESOLUTIONS = ("cell", "chunk")


def _validate_output_kind(name: str, meta: dict) -> None:
    """Validate a variable's non-scalar output declaration.

    A field may declare ``kind`` (``scalar`` default, ``vector``, or ``ragged``)
    and a shape key (``trailing_shape`` for ``vector``, ``inner_shape`` for
    ``ragged``). ``scalar`` fields need neither and stay the default path.
    ``vector`` and ``ragged`` fields may be driven by either ``function`` or
    ``expression``; ``len``/``count`` are rejected for both (they short-circuit
    to a scalar count). See issue #29 (vector) and issue #48 (ragged/CSR).

    A field may also declare ``resolution`` (``cell`` default, or ``chunk``).
    A ``resolution: chunk`` field (issue #30 item 2) is written ONCE per chunk
    into a companion array shaped at the chunk grid (``main.shape //
    chunk_shape``), indexed by ``grid.block_index(shard_key)`` — the compact
    storage for a chunk-uniform value (e.g. a ``chunk_precompute`` anchor).
    ``scalar``, ``vector``, and ``ragged`` kinds may all be ``resolution: chunk``
    (issue #82): a ``scalar`` companion is a plain chunk-grid array, a ``vector``
    companion appends the field's ``trailing_shape`` to the chunk grid (chunked
    whole), and a ``ragged`` companion is CSR at chunk granularity — one
    variable-length payload per chunk, written by ``write_ragged_to_zarr`` (phase
    4c). The shape keys are validated below exactly as for cell resolution — the
    chunk axis just replaces the cell axis.

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
            f"Variable '{name}': output kind '{kind}' is not supported (allowed: {allowed})"
        )

    # resolution (cell default, or chunk). A chunk-resolution field stores one
    # value per chunk in a companion array (issue #30 item 2). ``scalar`` and
    # ``vector`` chunk companions are wired (issue #82): a scalar companion is a
    # plain chunk-grid array, a vector companion appends the field's
    # ``trailing_shape`` to the chunk grid (chunked whole). The kind-specific shape
    # keys are validated by the per-kind branches below regardless of resolution.
    resolution = meta.get("resolution", "cell")
    if resolution not in OUTPUT_RESOLUTIONS:
        allowed = ", ".join(OUTPUT_RESOLUTIONS)
        raise ValueError(
            f"Variable '{name}': resolution '{resolution}' is not supported (allowed: {allowed})"
        )
    # ``ragged`` at chunk resolution (issue #82) is CSR at chunk granularity: one
    # variable-length payload per chunk instead of per cell. It rides the same CSR
    # writer as cell-resolution ragged (``write_ragged_to_zarr``), which collapses
    # the populated cells to the single chunk payload under the same chunk-uniform
    # contract as scalar/vector chunk companions (raise if populated cells
    # disagree). No special shape key is needed beyond ``inner_shape``.

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

    if kind == "vector":
        # trailing_shape is required and must be positive ints.
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
        return

    # kind == "ragged": inner_shape is required; trailing_shape is rejected.
    if has_trailing:
        raise ValueError(
            f"Variable '{name}': 'trailing_shape' is only valid for 'vector', not 'ragged'"
        )
    if "inner_shape" not in meta:
        raise ValueError(f"Variable '{name}': kind 'ragged' requires 'inner_shape'")
    _validate_trailing_shape(name, meta["inner_shape"], key_name="inner_shape")

    # Same restriction as vector: ``len``/``count`` produce a scalar count.
    if meta.get("function") in ("len", "count"):
        raise ValueError(
            f"Variable '{name}': function {meta['function']!r} produces a scalar "
            f"count and cannot be combined with kind 'ragged'"
        )


def _validate_trailing_shape(name: str, trailing_shape, key_name: str = "trailing_shape") -> None:
    """Check a shape field (trailing_shape or inner_shape) is a tuple of positive ints."""
    if isinstance(trailing_shape, int):
        dims: tuple = (trailing_shape,)
    elif isinstance(trailing_shape, (list, tuple)):
        dims = tuple(trailing_shape)
    else:
        raise ValueError(
            f"Variable '{name}': '{key_name}' must be an int or a "
            f"sequence of ints (got {trailing_shape!r})"
        )
    if not dims:
        raise ValueError(f"Variable '{name}': '{key_name}' must have at least one dimension")
    for dim in dims:
        if not isinstance(dim, int) or isinstance(dim, bool) or dim < 1:
            raise ValueError(
                f"Variable '{name}': '{key_name}' entries must be positive integers (got {dim!r})"
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
    """Validate the ``filters`` list and the flat ``quality_filter`` sugar.

    For the structured ``filters:`` list, raises ``ValueError`` on: unknown op,
    missing ``dataset``, ``in``/``not_in`` without a list ``values``, scalar ops
    without ``value``, non-int ``column``, a non-base-level ``expression`` filter,
    or wrong ``value`` type. ``column`` is required for the N-D flag case but
    cannot be checked against array rank here (no data); rank checks happen at
    read time.

    For the flat ``quality_filter`` sugar, only ``dataset`` and ``value`` are
    honored (``filters_from_data_source`` synthesizes ``op: eq, column: null``).
    Reject any extra keys at load time so a user-typoed ``op: gt`` or stray
    ``column:`` is not silently dropped on the floor — the structured ``filters:``
    list is the right form when those knobs are wanted.
    """
    qf = data_source.get("quality_filter")
    if qf is not None:
        allowed = {"dataset", "value"}
        unknown = set(qf) - allowed
        if unknown:
            raise ValueError(
                f"data_source.quality_filter only honors {sorted(allowed)} "
                f"(got extra keys {sorted(unknown)}); use the structured "
                "'filters:' list to set 'op', 'column', 'keep', etc."
            )
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
                    f"filter[{i}]: 'expression' filters are base-level only (level must be omitted)"
                )
            if not isinstance(f["expression"], str):
                raise ValueError(f"filter[{i}]: 'expression' must be a string")
            continue
        if "dataset" not in f:
            raise ValueError(f"filter[{i}]: structured filter requires 'dataset'")
        op = f.get("op")
        if op not in FILTER_OPS:
            raise ValueError(f"filter[{i}]: unknown op {op!r} (allowed: {sorted(FILTER_OPS)})")
        col = f.get("column")
        if col is not None and (not isinstance(col, int) or isinstance(col, bool)):
            raise ValueError(f"filter[{i}]: 'column' must be an integer index (got {col!r})")
        if op in _SET_OPS:
            if not isinstance(f.get("values"), list):
                raise ValueError(f"filter[{i}]: op {op!r} requires a 'values' list")
            for v in f["values"]:
                if not isinstance(v, (int, float)) or isinstance(v, bool):
                    raise ValueError(f"filter[{i}]: 'values' must be numeric (got {v!r})")
        else:
            if "value" not in f:
                raise ValueError(f"filter[{i}]: op {op!r} requires a scalar 'value'")
            if not isinstance(f["value"], (int, float)) or isinstance(f["value"], bool):
                raise ValueError(f"filter[{i}]: 'value' must be numeric (got {f['value']!r})")


def _segment_variable_names(data_source: dict) -> set[str]:
    """Names of readable segment-level (non-base) variables (issue #30).

    A non-base level may declare ``variables`` as a ``{name: path-template}``
    mapping; each name becomes a per-photon column once broadcast at read time
    (:func:`zagg.processing._read_segment_broadcasts`). The documentation-only
    ``list[str]`` form contributes nothing. Empty when no level declares the
    mapping form, so plain configs are unaffected.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict) or base_level is None:
        return set()
    names: set[str] = set()
    for name, lvl in levels.items():
        if name == base_level or not isinstance(lvl, dict):
            continue
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict):
            names |= set(lvl_vars)
    return names


def _validate_levels(data_source: dict) -> None:
    """Validate the hierarchical ``levels``/``base_level`` form (issue #43, Phase B).

    Rules:
    - ``base_level`` must name a key in ``levels``.
    - ``link.to`` in each level must name another key in ``levels``.
    - ``link.index_base`` must be a non-negative int when present.
    - ``link.reference_index`` must be ``None`` when present (reserved slot).
    - Only ``base_level`` may omit ``link`` (it has no coarser parent).
    - Flat single-level form (no ``levels`` key) is always valid.
    """
    levels = data_source.get("levels")
    if levels is None:
        return
    if not isinstance(levels, dict) or not levels:
        raise ValueError("data_source.levels must be a non-empty mapping")
    base_level = data_source.get("base_level")
    if base_level is None:
        raise ValueError("data_source.base_level is required when levels is present")
    if base_level not in levels:
        raise ValueError(
            f"data_source.base_level {base_level!r} is not a key in levels "
            f"(available: {sorted(levels)})"
        )
    base_vars = set(data_source.get("variables", {}))
    seg_var_names: set[str] = set()  # segment-variable names seen across non-base levels
    level_keys = set(levels)
    for name, lvl in levels.items():
        if not isinstance(lvl, dict):
            raise ValueError(f"levels.{name} must be a mapping")
        if "path" not in lvl:
            raise ValueError(f"levels.{name}: 'path' is required")
        # A non-base level may declare ``variables`` as a ``{name: path-template}``
        # mapping (issue #30): a readable segment-level variable broadcast to the
        # base rows at read time. Validate it like ``data_source.variables`` (string
        # names -> non-empty string path templates) and forbid the mapping form on
        # the base level (the base level uses ``data_source.variables``). The
        # documentation-only ``list[str]`` form stays valid on any level.
        lvl_vars = lvl.get("variables")
        if isinstance(lvl_vars, dict):
            if name == base_level:
                raise ValueError(
                    f"levels.{base_level}: the base level uses data_source.variables; "
                    f"a non-base level uses the 'variables' mapping for segment-level reads"
                )
            for var_name, tmpl in lvl_vars.items():
                if not isinstance(var_name, str) or not var_name:
                    raise ValueError(
                        f"levels.{name}.variables: variable names must be non-empty strings"
                    )
                if not isinstance(tmpl, str) or not tmpl:
                    raise ValueError(
                        f"levels.{name}.variables.{var_name}: path template must be a "
                        f"non-empty string (got {tmpl!r})"
                    )
                if var_name in base_vars:
                    raise ValueError(
                        f"levels.{name}.variables.{var_name}: collides with a "
                        f"data_source.variables column"
                    )
                # Two non-base levels declaring the same name would silently
                # overwrite each other when broadcast into one per-photon column
                # (the read keys by name); reject the ambiguity.
                if var_name in seg_var_names:
                    raise ValueError(
                        f"levels.{name}.variables.{var_name}: a segment-level "
                        f"variable named {var_name!r} is already declared on another level"
                    )
                seg_var_names.add(var_name)
        link = lvl.get("link")
        if link is None:
            if name != base_level:
                raise ValueError(
                    f"levels.{name}: non-base levels must have a 'link' "
                    f"(only {base_level!r} may omit it)"
                )
            continue
        if not isinstance(link, dict):
            raise ValueError(f"levels.{name}.link must be a mapping")
        for field_name in ("to", "index_beg", "count"):
            if field_name not in link:
                raise ValueError(f"levels.{name}.link: '{field_name}' is required")
        unknown = set(link) - {"to", "index_beg", "count", "index_base", "reference_index"}
        if unknown:
            raise ValueError(
                f"levels.{name}.link: unknown fields {sorted(unknown)} "
                f"(allowed: to, index_beg, count, index_base, reference_index)"
            )
        if link.get("to") == name:
            raise ValueError(f"level '{name}': link.to cannot reference the level itself")
        if link["to"] not in level_keys:
            raise ValueError(
                f"levels.{name}.link.to {link['to']!r} is not a key in levels "
                f"(available: {sorted(level_keys)})"
            )
        index_base = link.get("index_base", 0)
        if not isinstance(index_base, int) or isinstance(index_base, bool) or index_base < 0:
            raise ValueError(
                f"levels.{name}.link.index_base must be a non-negative int (got {index_base!r})"
            )
        ref = link.get("reference_index")
        if ref is not None:
            raise ValueError(
                f"levels.{name}.link.reference_index is reserved and must be null/omitted "
                f"(explicit index-array variant not yet implemented)"
            )


def _validate_filter_levels(data_source: dict) -> None:
    """Cross-check each filter's level field against the levels keys (issue #43).

    A filter with ``level: "nonexistent"`` would otherwise only fail at read time
    with an opaque ``KeyError``. Raises ``ValueError`` with a clear message when a
    filter's ``level`` names a key not present in ``levels``.
    """
    levels = data_source.get("levels")
    if levels is None:
        return
    level_keys = set(levels)
    filters = data_source.get("filters") or []
    for i, f in enumerate(filters):
        lvl = f.get("level")
        if lvl is not None and lvl not in level_keys:
            raise ValueError(
                f"filter[{i}]: level {lvl!r} is not a key in levels "
                f"(available: {sorted(level_keys)})"
            )


def get_levels(config: "PipelineConfig") -> dict | None:
    """Return the ``levels`` mapping from the data source, or ``None`` if flat.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict or None
    """
    return config.data_source.get("levels")


def get_base_level(config: "PipelineConfig") -> str | None:
    """Return the ``base_level`` key from the data source, or ``None`` if flat.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str or None
    """
    return config.data_source.get("base_level")


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


def get_chunk_precompute(config: PipelineConfig) -> dict:
    """Return the ``aggregation.chunk_precompute`` entries keyed by name (issue #30).

    Each entry is a chunk-level scalar evaluated ONCE per chunk (shard) over the
    shard's pooled column data, before the per-cell loop; the resulting scalar is
    injected into the per-cell expression namespace. Returns an empty dict when no
    ``chunk_precompute`` block is present, so the existing per-cell path is a no-op.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    dict
        ``{name: {function/expression, source, params, dtype}}``.
    """
    return dict(config.aggregation.get("chunk_precompute", {}))


def get_output_signature(meta: dict) -> dict:
    """Return the normalized non-scalar output signature for one agg field.

    This is the single read point for a field's output declaration (issues
    #29 and #48): its output ``kind``, the per-cell ``trailing_shape``,
    ``inner_shape``, and ``dtype``. Later phases (statistic eval, the
    per-shard container, and the grid ``signature()``) consume this rather
    than re-parsing the raw metadata.

    Parameters
    ----------
    meta : dict
        A single variable's aggregation metadata (a value of
        :func:`get_agg_fields`).

    Returns
    -------
    dict
        ``{"kind": str, "trailing_shape": tuple, "inner_shape": tuple, "dtype":
        str, "resolution": str}``.
        ``trailing_shape`` is ``()`` for scalar and ragged fields.
        ``inner_shape`` is ``()`` for scalar and vector fields; for ragged it
        holds the per-element shape (e.g. ``(2,)`` for a centroid pair).
        ``dtype`` is the declared dtype string, or ``None`` if unset.
        ``resolution`` is ``"cell"`` (default — one value per aggregation cell) or
        ``"chunk"`` (issue #30 item 2 — one value per chunk, stored in a companion
        array shaped at the chunk grid and indexed by ``grid.block_index``).
    """
    kind = meta.get("kind", "scalar")
    if kind == "vector":
        ts = meta["trailing_shape"]
        trailing_shape = (ts,) if isinstance(ts, int) else tuple(ts)
        inner_shape: tuple = ()
    elif kind == "ragged":
        trailing_shape = ()
        rs = meta["inner_shape"]
        inner_shape = (rs,) if isinstance(rs, int) else tuple(rs)
    else:
        trailing_shape = ()
        inner_shape = ()
    return {
        "kind": kind,
        "trailing_shape": trailing_shape,
        "inner_shape": inner_shape,
        "dtype": meta.get("dtype"),
        "resolution": meta.get("resolution", "cell"),
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
                "inner_shape": list(sig["inner_shape"]),
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


def get_handoff(config: PipelineConfig) -> str:
    """Return the per-cell aggregation carrier from the aggregation config (issue #132).

    The ``handoff`` knob lives on the ``aggregation`` block, default ``"arrow"``,
    and selects the per-cell read->concat->extract carrier: ``"arrow"`` (an
    ``arro3.core`` Table, faster + lighter on dense shards) or ``"pandas"`` (a
    DataFrame, which tolerates nullable columns natively). Both feed identical
    numpy arrays into the same reductions, so scalar outputs are byte-for-byte
    identical (issues #130/#131). A pipeline declares its carrier next to the rest
    of its aggregation settings rather than relying on a global default; the
    explicit ``handoff=`` kwarg still overrides this value.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"arrow"`` (default) or ``"pandas"``.
    """
    return config.aggregation.get("handoff", "arrow")


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


def get_sharded(config: PipelineConfig) -> bool:
    """Return whether the output grid uses ShardingCodec storage (issue #108).

    The ``sharded`` knob lives on the grid/chunk block next to ``chunk_inner``
    (mirroring its accessor), default ``False``. When ``True`` the grid bundles a
    dispatch shard's K inner chunks into one zarr shard object instead of K
    independent regular chunk objects; it is only valid when ``chunk_inner`` gives
    K>1 (the grid raises otherwise, validated before deployment).
    """
    return bool(config.output.get("grid", {}).get("sharded", False))


def get_shard_order(config: PipelineConfig) -> int | None:
    """Return the sharding-OBJECT order from the output grid config (issue #133 phase 8).

    ``shard_order`` decouples the ShardingCodec object from the dispatch shard: an
    order in ``[parent_order, chunk_inner]`` sizes each sharding object — at
    ``parent_order`` (or ``None``) one object spans the whole dispatch shard (today's
    byte-identical write), and a finer order (``> parent_order``) makes each object
    smaller so the worker writes its region in per-object passes (bounding peak memory
    under the 2 GB cap on large/dense shards).
    ``None`` (default) keeps one object per dispatch shard — today's behavior, a
    byte-identical write. Only meaningful when ``sharded`` is True (the grid raises
    otherwise, validated before deployment).
    """
    val = config.output.get("grid", {}).get("shard_order")
    return None if val is None else int(val)


def get_cell_ids_encoding(config: PipelineConfig) -> str:
    """Return the HEALPix ``cell_ids`` coordinate encoding (issue #135).

    ``"nested"`` (default) stores the standardized NESTED HEALPix cell IDs.
    ``"morton"`` stores the packed morton words instead — the same ``uint64``
    words the ``morton`` coordinate carries — opening test/prototype flows that
    index by morton directly. Default behavior (key absent, explicit ``null``,
    or ``"nested"``) is byte-identical to a pre-flag run.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    str
        ``"nested"`` (default) or ``"morton"``.
    """
    # A present-but-null key (YAML ``cell_ids_encoding:``) must fall back to the
    # default too — the same treatment ``from_config`` gives a null ``layout``.
    return config.output.get("grid", {}).get("cell_ids_encoding") or "nested"


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


def get_aoi_mask(config: PipelineConfig) -> bool:
    """Whether the optional strict-AOI cell mask is enabled (issue #101).

    ``output.aoi_mask: true`` packages a per-cell boolean ``aoi_mask`` array
    aligned to the output cell grid, ``True`` where the cell is inside the AOI.
    Defaults to ``False`` — when off, no array is emitted and outputs are
    byte-identical to a run without the feature.

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    bool
    """
    return bool(config.output.get("aoi_mask", False))


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


def evaluate_filter_expression(expression: str, columns: dict[str, np.ndarray]) -> np.ndarray:
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
