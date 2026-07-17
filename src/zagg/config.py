"""YAML-driven pipeline configuration for zagg."""

import importlib
import inspect
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
    # Virtual chunk-index backend block (issue #160). Absent → the default
    # ``hierarchical`` path, byte-identical. ``backend`` names a registered
    # backend (builtin or ``zagg.index_backends`` entry point); the remaining
    # keys are backend-specific and validated against the backend's declared
    # ``config_keys`` — irrelevant keys are config errors, not ignored.
    index: NotRequired[dict]
    # Credential-provider registry name for source-data S3 reads (issue #213
    # Phase 4/6): built-ins ``nsidc``/``gesdisc``; plugins may register others.
    # Absent → the spatial default (NSIDC); temporal events may also carry
    # per-event ``s3_credentials``, which win.
    credentials_provider: NotRequired[str]


# Structured-predicate comparison operators (issue #43). ``in``/``not_in`` take a
# ``values`` list; the rest take a scalar ``value``. These are the only
# pushdown-eligible filter language; an ``expression`` filter is a base-level-only,
# aggregation-time escape hatch that forfeits pushdown.
_SCALAR_OPS = frozenset({"eq", "ne", "ge", "le", "lt", "gt"})
_SET_OPS = frozenset({"in", "not_in"})
FILTER_OPS = _SCALAR_OPS | _SET_OPS


_PIPELINE_TYPES = frozenset({"spatial", "temporal", "event"})

# Memory sizes (MB) of the pre-provisioned Lambda worker-size variants
# (issue #235). Must match template.yaml's WorkerMemorySizes parameter — the
# runner resolves ``worker:`` to a ``<base>-<memory>[-disk]`` function name,
# so an unlisted size would dispatch to a function that does not exist.
WORKER_MEMORIES = frozenset({2048, 4096, 8192})


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
    pipeline : dict
        Pipeline kind selector (issue #12). ``{"type": "spatial"}`` (default)
        runs the point-cloud->grid aggregation path; ``"temporal"`` /
        ``"event"`` route to the event-streaming engines added in later
        phases. Absent ``pipeline`` key in YAML defaults to ``spatial`` for
        backward compatibility with every existing config.
    worker : dict or None
        Optional Lambda worker-size selector (issue #235):
        ``{"memory": 2048|4096|8192, "extra_disk": bool}``. The runner
        resolves it to a pre-provisioned function-name suffix
        (``-<memory>``, plus ``-disk`` when ``extra_disk`` is true) on the
        lambda backend; an explicit ``function_name`` kwarg wins over it.
        Absent block -> the unsuffixed default function, byte-identical
        prior behavior. Ignored by the local backend.
    """

    data_source: DataSourceDict = field(default_factory=dict)
    aggregation: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    catalog: str | None = None
    bounds: dict | None = None
    pipeline: dict = field(default_factory=lambda: {"type": "spatial"})
    worker: dict | None = None


def get_pipeline_type(config: PipelineConfig) -> str:
    """Return the pipeline kind, defaulting to ``"spatial"``.

    Centralised so dispatch / strategy selection has a single source of truth.
    """
    if not isinstance(config.pipeline, dict):
        raise ValueError("pipeline must be a mapping with a 'type' key")
    t = config.pipeline.get("type", "spatial")
    if t not in _PIPELINE_TYPES:
        raise ValueError(f"pipeline.type must be one of {sorted(_PIPELINE_TYPES)} (got {t!r})")
    return t


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
        pipeline=d.get("pipeline", {"type": "spatial"}),
        worker=d.get("worker"),
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
    # Pipeline kind drives which validation branch runs; spatial keeps the
    # full grid + aggregation cross-checks below. Temporal/event pipelines
    # validate their own (much smaller) spec shape and return early -- they
    # carry no output grid and run through the event-streaming engine
    # (``zagg.temporal.process_event``) rather than the point-cloud path.
    # Both pipeline kinds honor a provider-selected source-credential fetch
    # (issue #213 Phase 6), so the key's shape is checked before the branch.
    provider = (config.data_source or {}).get("credentials_provider")
    if provider is not None and not isinstance(provider, str):
        raise ValueError(
            "data_source.credentials_provider must be a credential-provider "
            f"registry name string, e.g. 'nsidc' or 'gesdisc' (got {provider!r})"
        )

    # The optional top-level worker block (issue #235) selects a
    # pre-provisioned Lambda size variant on any pipeline kind, so it is
    # validated before the kind branch, like credentials_provider above.
    _validate_worker(config)

    ptype = get_pipeline_type(config)
    if ptype != "spatial":
        _validate_temporal_config(config)
        return

    # Raster pipelines (issue #218) share the spatial grid checks but replace
    # the HDF5 read-side schema (groups/coordinates/variables) and the
    # aggregation section with a declarative band map: pull-NN yields exactly
    # one value per cell per timestep, so there is nothing to reduce.
    if (config.data_source or {}).get("reader") == "raster":
        _validate_raster_config(config)
        return

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

    # Validate the optional read fan-out width (issue #170). Mirrors the read
    # module's guard (_read_workers) so a config typo is rejected at
    # submission -- inside the worker the same error would be swallowed into
    # per-group read_errors and surface as a silently-empty shard.
    read_workers = (config.data_source or {}).get("read_workers")
    if read_workers is not None and (
        isinstance(read_workers, bool) or not isinstance(read_workers, int) or read_workers < 1
    ):
        raise ValueError(f"data_source.read_workers must be an integer >= 1 (got {read_workers!r})")

    # Validate the optional granule fan-out width (issue #180). Mirrors the
    # worker's guard (_granule_workers) with the same rejection rationale as
    # read_workers above. ``shard_workers`` is the canonical cross-pipeline
    # name (issue #232 — "source units in flight per shard"); the legacy
    # ``granule_workers`` key stays honored (canonical wins when both set).
    for _key in ("shard_workers", "granule_workers"):
        _w = (config.data_source or {}).get(_key)
        if _w is not None and (isinstance(_w, bool) or not isinstance(_w, int) or _w < 1):
            raise ValueError(f"data_source.{_key} must be an integer >= 1 (got {_w!r})")
    # Optional strict-AOI cell mask (issue #101), default off. Must be a bool.
    aoi_mask = config.output.get("aoi_mask")
    if aoi_mask is not None and not isinstance(aoi_mask, bool):
        raise ValueError(f"output.aoi_mask must be a boolean (got {aoi_mask!r})")

    # Optional zarr metadata consolidation (issue #191), default off. Must be a
    # bool. No zagg reader depends on the consolidated blob, and building it is a
    # ~70 s serial-GET finalize tax, so it is opt-in.
    consolidate_metadata = config.output.get("consolidate_metadata")
    if consolidate_metadata is not None and not isinstance(consolidate_metadata, bool):
        raise ValueError(
            f"output.consolidate_metadata must be a boolean (got {consolidate_metadata!r})"
        )

    # Store layout + root coverage MOC — shared with the raster branch
    # (issue #247: raster + hive is legal, so the checks live in one place).
    _validate_store_layout_keys(config)

    # Temporal windowing block (issue #246, morton-hive/2): nested
    # output.windowing declares the window schedule + time encoding. Absent =
    # schedule none = today's behavior; the block itself is hive-only (windowed
    # leaves are a hive-layout convention), mirroring coverage_moc's posture.
    _validate_windowing(config)

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

    # Virtual chunk-index backend block (issue #160)
    _validate_index(config.data_source)

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

        # Located ragged fields (issue #87): the location column is the
        # per-observation morton the HEALPix read path supplies as ``leaf_id``
        # (or a declared data column). Rect grids have no per-obs morton, so a
        # located field on any non-HEALPix grid is a config error — raise here
        # rather than emit garbage downstream.
        location = meta.get("location")
        if location is not None:
            if location != "leaf_id" and location not in ds_vars:
                raise ValueError(
                    f"Variable '{name}': location '{location}' is not 'leaf_id' or a "
                    f"data_source variable (available: {sorted(ds_vars)})"
                )
            # An absent output.grid defaults to healpix everywhere else (the
            # grid factory's ``grid_cfg.get("type", "healpix")``), so mirror
            # that default here rather than falsely rejecting a valid config.
            grid_type = grid.get("type") if grid is not None else "healpix"
            if grid_type != "healpix":
                raise ValueError(
                    f"Variable '{name}': 'location' requires a healpix output grid "
                    f"(the per-observation morton column); got grid type {grid_type!r}"
                )
            # The location channel is injected as the reducer's ``locations=``
            # kwarg (aggregate.py), so a params entry of that name would collide
            # (TypeError deep in the per-cell loop) and a reducer that cannot
            # accept the kwarg would crash on every populated cell — reject both
            # at load time instead.
            if "locations" in meta.get("params", {}):
                raise ValueError(
                    f"Variable '{name}': params may not name 'locations' — it is "
                    f"reserved for the location channel (issue #87)"
                )
            if has_func:
                func = resolve_function(meta["function"])
                try:
                    func_params = inspect.signature(func).parameters
                except (TypeError, ValueError):
                    func_params = None  # ufuncs/builtins: not introspectable
                if func_params is not None and "locations" not in func_params:
                    has_var_kw = any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in func_params.values()
                    )
                    if not has_var_kw:
                        raise ValueError(
                            f"Variable '{name}': function {meta['function']!r} does not "
                            f"accept a 'locations' keyword, which 'location' requires "
                            f"(use a located reducer, e.g. zagg.stats.tdigest.build_tdigest)"
                        )
            # The location channel is stored as a SIBLING vlen array named
            # ``{name}_locations`` (issue #209) — a name the user never wrote.
            # A field or coordinate already claiming it would silently lose in
            # the template members dict and interleave into the same object
            # slab at write time (data corruption), so reject at load
            # (review, PR #211).
            from zagg.grids.base import ragged_locations_name

            sibling = ragged_locations_name(name)
            if sibling in agg_vars or sibling in config.aggregation.get("coordinates", {}):
                raise ValueError(
                    f"Variable '{name}': its location channel is stored in a sibling "
                    f"array named '{sibling}', which collides with the declared "
                    f"field/coordinate of that name — rename one of them (issue #209)"
                )


# Required per-variable keys for a temporal/event aggregation spec. ``mask``
# is optional (``specs_from_config`` defaults it to ``"ais"``); capability
# *names* (spatial_func / temporal_reducer / mask) are resolved by the registry
# at run time, not here -- a typo surfaces as a clean ``UnknownCapability`` from
# ``process_event`` rather than a load-time guess about which plugins are
# installed.
_TEMPORAL_SPEC_KEYS = ("variable", "collection", "spatial_func", "temporal_reducer")


def _validate_worker(config: PipelineConfig) -> None:
    """Validate the optional top-level ``worker:`` block (issue #235).

    ``{memory, extra_disk?}`` selects one of the pre-provisioned Lambda
    worker-size variants: ``memory`` (required, one of
    :data:`WORKER_MEMORIES`) picks the ``-<memory>`` function, and
    ``extra_disk: true`` (optional, default false) the ``-disk`` twin with
    ``/tmp`` sized memory + 2048 MB. Fails at load — a typo here would
    otherwise surface as a per-shard ResourceNotFound after the fan-out
    starts. Absent block is today's behavior (unsuffixed function).
    """
    worker = config.worker
    if worker is None:
        return
    if not isinstance(worker, dict):
        raise ValueError(f"worker must be a mapping {{memory, extra_disk?}} (got {worker!r})")
    unknown = set(worker) - {"memory", "extra_disk"}
    if unknown:
        raise ValueError(
            f"Unknown worker keys: {sorted(unknown)} (allowed: 'memory', 'extra_disk')"
        )
    memory = worker.get("memory")
    if isinstance(memory, bool) or memory not in WORKER_MEMORIES:
        raise ValueError(
            f"worker.memory must be one of {sorted(WORKER_MEMORIES)} MB — the "
            f"pre-provisioned variant sizes (issue #235) — (got {memory!r})"
        )
    extra_disk = worker.get("extra_disk")
    if extra_disk is not None and not isinstance(extra_disk, bool):
        raise ValueError(f"worker.extra_disk must be a boolean (got {extra_disk!r})")


def _validate_temporal_config(config: PipelineConfig) -> None:
    """Validate a temporal/event pipeline config (issue #12, Phase 5).

    Temporal pipelines carry no output grid; the only cross-check is that the
    ``aggregation.variables`` block names, per variable, the four keys
    :data:`_TEMPORAL_SPEC_KEYS` that :func:`zagg.temporal.specs_from_config`
    requires (the rest are optional flags with defaults). Raises ``ValueError``
    on a missing section or key.
    """
    if not config.aggregation:
        raise ValueError("Missing required section: aggregation")
    variables = config.aggregation.get("variables")
    if not variables:
        raise ValueError("temporal pipeline requires aggregation.variables")
    for name, meta in variables.items():
        missing = [k for k in _TEMPORAL_SPEC_KEYS if k not in meta]
        if missing:
            raise ValueError(
                f"temporal variable '{name}' is missing required key(s): "
                f"{', '.join(missing)} (need {', '.join(_TEMPORAL_SPEC_KEYS)})"
            )
        params = meta.get("params")
        if params is not None and not isinstance(params, dict):
            raise ValueError(
                f"temporal variable '{name}' params must be a mapping (got {params!r})"
            )
    _validate_collection_options(config)


def _validate_raster_config(config: PipelineConfig) -> None:
    """Validate a raster (pull-NN) pipeline config (issue #218).

    ``data_source.bands`` maps output field name -> ``{asset, dtype, ...}``:
    ``asset`` names the STAC asset key holding that band's GeoTIFF, ``dtype``
    is required (bands store exact source DNs — no float default), and
    optional ``fill_value`` (default 0), ``scale``/``offset`` (recorded as
    ``scale_factor``/``add_offset`` array attrs, never applied to the data).
    ``data_source.nodata`` (optional scalar) marks source nodata; a cell
    whose sampled pixel equals it in any band is left at fill.
    """
    for section in ("data_source", "output"):
        if not getattr(config, section):
            raise ValueError(f"Missing required section: {section}")
    if config.aggregation:
        raise ValueError(
            "raster pipelines take no aggregation section: pull-NN yields one "
            "value per cell per timestep (declare bands under data_source.bands)"
        )
    bands = config.data_source.get("bands")
    if not bands or not isinstance(bands, dict):
        raise ValueError("raster pipeline requires data_source.bands (field -> {asset, dtype})")
    for name, meta in bands.items():
        if not isinstance(meta, dict):
            raise ValueError(f"band '{name}' must be a mapping (got {meta!r})")
        for key in ("asset", "dtype"):
            if not isinstance(meta.get(key), str) or not meta.get(key):
                raise ValueError(f"band '{name}' requires a string '{key}'")
        for key in ("fill_value", "scale", "offset"):
            if key in meta and (
                not isinstance(meta[key], (int, float)) or isinstance(meta[key], bool)
            ):
                raise ValueError(f"band '{name}' {key} must be a number (got {meta[key]!r})")
    nodata = config.data_source.get("nodata")
    if nodata is not None and (not isinstance(nodata, (int, float)) or isinstance(nodata, bool)):
        raise ValueError(f"data_source.nodata must be a number (got {nodata!r})")
    # Optional per-shard fan-out cap (issues #231/#232): ``shard_workers`` is
    # the ONE cross-pipeline knob for "source units in flight per shard" —
    # here, acquisition groups (timesteps) sampling concurrently. Guarded like
    # read_workers so a config typo is rejected at submission, not swallowed
    # inside the worker's event loop. Default 4
    # (``processing.raster._shard_workers``); ``1`` is serial. NOTE: the
    # memory cost per unit is pipeline-dependent (a granule read buffer on the
    # spatial path vs a timestep of decoded COG tiles here) — see issue #228.
    shard_workers = config.data_source.get("shard_workers")
    if shard_workers is not None and (
        isinstance(shard_workers, bool) or not isinstance(shard_workers, int) or shard_workers < 1
    ):
        raise ValueError(
            f"data_source.shard_workers must be an integer >= 1 (got {shard_workers!r})"
        )
    # Streamed-write slab budget (PR #232 double-buffer): ``1`` (default) is
    # the strict serial bound; ``N`` overlaps up to N-1 slab writes with
    # sampling at a peak of N slabs alive (``processing.raster._write_buffer``).
    write_buffer = config.data_source.get("write_buffer")
    if write_buffer is not None and (
        isinstance(write_buffer, bool) or not isinstance(write_buffer, int) or write_buffer < 1
    ):
        raise ValueError(f"data_source.write_buffer must be an integer >= 1 (got {write_buffer!r})")
    grid = config.output.get("grid")
    if not isinstance(grid, dict) or grid.get("type") != "healpix":
        raise ValueError(
            "raster pipelines currently require output.grid.type: healpix "
            "(the rectilinear (time, y, x) template is future work — issue #218)"
        )
    for key in ("parent_order", "child_order"):
        if key not in grid:
            raise ValueError(f"output.grid.{key} is required for healpix grid")
    if grid.get("sharded"):
        # Permanent exclusion, not a deferral (espg-ratified on issue #247):
        # per-timestep slab streaming over a ShardingCodec object would
        # read-modify-write it once per timestep, and raster object count is
        # time-axis-dominated anyway — sharding the cell axis buys little.
        raise ValueError(
            "raster pipelines do not support sharded: true (per-timestep slab "
            "streaming would read-modify-write each ShardingCodec object; "
            "chunks stay (1, cells_per_chunk) — one object per timestep-chunk)"
        )
    # Store layout, root coverage MOC, and temporal windowing ride the SHARED
    # checks (issue #247: raster + hive is legal — the issue #239 stopgap
    # rejections are gone). _validate_windowing resolves the raster
    # time_field/encoding rules (membership is the acquisition's STAC
    # datetime; the conversion knobs do not apply).
    _validate_store_layout_keys(config)
    _validate_windowing(config)


def _validate_store_layout_keys(config: PipelineConfig) -> None:
    """Shared ``output.store_layout`` / ``coverage_moc`` cross-checks.

    Optional store layout (issue #199 phase 2): "flat" (default, today's single
    shared store) or "hive" (one leaf zarr per shard under a morton digit tree —
    ``docs/design/sparse_coverage.md`` D1-D6). Hive ids are morton decimal
    strings, so the layout is HEALPix-only; metadata consolidation assumes the
    single shared store, so it is rejected with hive rather than silently
    mis-writing. Called from both the point-pipeline and raster validation
    branches (issue #247: raster + hive is legal, so the checks live in one
    place; the issue #239 stopgap rejections are gone).

    The end-of-run root coverage MOC (issue #200 phase 3) is boolean when
    present, default ON for the hive layout (O9); it writes
    ``{store}/coverage.moc``, a hive-root object, so an EXPLICIT true on a
    non-healpix grid or a flat-layout store is a config mistake, not a no-op —
    rejected pointedly. Absent simply means off there (``get_coverage_moc``
    resolves the default).
    """
    grid = config.output.get("grid")
    store_layout = config.output.get("store_layout")
    if store_layout is not None and store_layout not in ("flat", "hive"):
        raise ValueError(f"output.store_layout must be 'flat' or 'hive' (got {store_layout!r})")
    if store_layout == "hive":
        if (grid or {}).get("type", "healpix") != "healpix":
            raise ValueError(
                "output.store_layout: hive requires a healpix grid (hive node names "
                f"are morton decimal digits; grid type is {(grid or {}).get('type')!r})"
            )
        if config.output.get("consolidate_metadata"):
            raise ValueError(
                "output.store_layout: hive has no store-root zarr hierarchy to "
                "consolidate (D5/D12) — drop output.consolidate_metadata"
            )
    coverage_moc = config.output.get("coverage_moc")
    if coverage_moc is not None and not isinstance(coverage_moc, bool):
        raise ValueError(f"output.coverage_moc must be a boolean (got {coverage_moc!r})")
    if coverage_moc:
        if (grid or {}).get("type", "healpix") != "healpix":
            raise ValueError(
                "output.coverage_moc requires a healpix grid (the root coverage.moc "
                "is a morton MOC; drop the flag for rectilinear output)"
            )
        if store_layout != "hive":
            raise ValueError(
                "output.coverage_moc requires output.store_layout: hive (the root "
                "coverage.moc lives at the hive store root; flat stores have no "
                "hive root to bootstrap from)"
            )


def _validate_windowing(config: PipelineConfig) -> None:
    """Validate the ``output.windowing`` block (issue #246, morton-hive/2).

    ``{schedule, time_field, epoch, scale?, units?, windows?}`` — the nested
    form ratified on the #246 thread. Absent/null is schedule ``none``
    (today's behavior). A present block requires the hive store layout on a
    healpix grid (windowed leaf names are a morton-hive convention, like
    ``coverage_moc``). ``quarterly`` is grammar-reserved on the mortie spec
    page but NOT implemented — rejected with a pointed message. The explicit
    windows list must be well-formed: frozen label grammar, half-open
    ``start < end``, unique labels, non-overlapping ranges. ``time_field``
    must name a declared BASE-RATE ``data_source`` variable (a flat
    ``variables`` entry or the base level's ``variables``) — a coordinate or
    a non-base (segment-rate) level column is rejected, since the stamp
    ``time_range`` is pooled from read variable columns and window membership
    is decided per observation. On the RASTER path (issue #247) membership is
    the acquisition's STAC ``datetime`` instead: ``time_field`` is optional
    (fixed to ``datetime``) and the ``epoch``/``scale``/``units`` conversion
    knobs are rejected.
    """
    from zagg import windows as _windows

    block = config.output.get("windowing")
    if block is None:
        return
    if not isinstance(block, dict):
        raise ValueError(
            f"output.windowing must be a mapping "
            f"{{schedule, time_field, epoch, ...}} (got {block!r})"
        )
    try:
        schedule = _windows.check_schedule(block.get("schedule", "none"))
    except ValueError as e:
        raise ValueError(f"output.windowing.schedule: {e}") from e
    if schedule == "none":
        # An inert ``schedule: none`` block is equivalent to an absent block
        # (``get_windowing`` returns ``None``, no windowed output), so it must
        # NOT require the hive/healpix layout — this early return precedes the
        # layout guards. A stray ``windows`` key is still rejected, mirroring
        # the generative-schedule check below (validation symmetry).
        if block.get("windows") is not None:
            raise ValueError(
                "output.windowing.windows only applies to schedule: explicit "
                "(schedule: none produces no windowed output)"
            )
        return
    grid = config.output.get("grid") or {}
    if (grid.get("type", "healpix")) != "healpix":
        raise ValueError(
            "output.windowing requires a healpix grid (windowed leaves are a "
            f"morton-hive convention; grid type is {grid.get('type')!r})"
        )
    if (config.output.get("store_layout") or "flat") != "hive":
        raise ValueError(
            "output.windowing requires output.store_layout: hive (window leaves "
            "are hive leaf zarrs; the flat shared store has no leaves to window)"
        )
    time_field = block.get("time_field")
    if (config.data_source or {}).get("reader") == "raster":
        # Raster window membership is the acquisition's STAC ``datetime``,
        # decided at dispatch (issue #247, ratified): there is no
        # per-observation timestamp column, so ``time_field`` is optional and
        # fixed — the manifest records the resolved field, and a property-name
        # knob (e.g. ``start_datetime`` for interval-typed items) can be added
        # later without breaking any existing manifest.
        if time_field is not None and time_field != "datetime":
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is not configurable "
                "on the raster path: window membership is the acquisition's STAC "
                "datetime (drop the key, or set it to 'datetime')"
            )
        # The epoch/scale/units knobs describe a dataset-unit time_field
        # conversion; STAC datetimes are already ISO-8601 UTC instants, so
        # there is nothing to configure — reject rather than record a
        # misleading manifest temporal block (get_windowing resolves the
        # fixed encoding).
        knobs = [k for k in ("epoch", "scale", "units") if block.get(k) is not None]
        if knobs:
            raise ValueError(
                f"output.windowing.{knobs[0]} does not apply to raster "
                "pipelines: STAC datetimes are ISO-8601 UTC instants (drop the "
                "key)"
            )
        _validate_windowing_windows(block, schedule)
        return
    if not isinstance(time_field, str) or not time_field:
        raise ValueError(
            "output.windowing.time_field is required: the per-observation "
            "timestamp column that decides window membership"
        )
    # ``time_field`` must be a BASE-RATE variable column, i.e. a
    # ``data_source.variables`` entry (the base level reads its columns from
    # there too — ``_validate_levels`` forbids a base-level ``variables``
    # mapping). The stamp ``time_range`` (a windowed leaf's headline truth) is
    # pooled from the read VARIABLE columns (``col_arrays`` in the worker),
    # never from ``coordinates`` — so a coordinate ``time_field`` would filter
    # yet silently drop the stamp, and a lat/lon coordinate is not a timestamp
    # anyway. A non-base (segment-rate) level column is rejected too: window
    # membership would be decided per whole segment, not per observation,
    # which is not supported this round (see ``window_time_filters``).
    ds = config.data_source or {}
    declared = set(ds.get("variables") or {})
    if time_field not in declared:
        if time_field in set(ds.get("coordinates") or {}):
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is a data_source "
                f"coordinate; the stamp time_range is pooled from read variable "
                f"columns and a coordinate is not read as a timestamp. Declare it "
                f"under data_source.variables (or the base level's variables)."
            )
        if time_field in _segment_variable_names(ds):
            raise ValueError(
                f"output.windowing.time_field {time_field!r} is declared on a "
                f"non-base (segment-rate) level; segment-rate window membership is "
                f"not supported yet (whole segments would be kept or dropped). "
                f"Declare it on the base level or data_source.variables."
            )
        raise ValueError(
            f"output.windowing.time_field {time_field!r} is not a declared "
            f"base-rate data_source column (one of {sorted(declared)}); the "
            f"worker filters per observation on variable columns it reads"
        )
    epoch = block.get("epoch")
    if epoch is None:
        raise ValueError(
            "output.windowing.epoch is required: the dataset's zero time as an "
            "ISO-8601 UTC instant (e.g. '2018-01-01T00:00:00Z' for ICESat-2 "
            "delta_time)"
        )
    try:
        _windows.parse_utc(epoch)
    except ValueError as e:
        raise ValueError(f"output.windowing.epoch: {e}") from e
    scale = block.get("scale") or "utc"
    if scale not in _windows.EPOCH_SCALES:
        raise ValueError(
            f"output.windowing.scale must be one of {_windows.EPOCH_SCALES} (got {scale!r})"
        )
    units = block.get("units") or "seconds"
    if units not in _windows.UNIT_SECONDS:
        raise ValueError(
            f"output.windowing.units must be one of {tuple(_windows.UNIT_SECONDS)} (got {units!r})"
        )
    _validate_windowing_windows(block, schedule)


def _validate_windowing_windows(block: dict, schedule: str) -> None:
    """Validate the ``windows`` list half of ``output.windowing`` (issue #246).

    Shared by the point-pipeline and raster branches of
    :func:`_validate_windowing`: frozen label grammar, half-open
    ``start < end``, unique labels, non-overlapping ranges — required for
    ``schedule: explicit``, rejected otherwise.
    """
    from zagg import windows as _windows

    declared_windows = block.get("windows")
    if schedule != "explicit":
        if declared_windows is not None:
            raise ValueError(
                f"output.windowing.windows only applies to schedule: explicit "
                f"(the {schedule} schedule derives its windows from labels)"
            )
        return
    if not isinstance(declared_windows, list) or not declared_windows:
        raise ValueError(
            "output.windowing.windows is required for schedule: explicit — a "
            "non-empty list of {label, start, end} entries"
        )
    seen: dict = {}
    for entry in declared_windows:
        if not isinstance(entry, dict) or not {"label", "start", "end"} <= set(entry):
            raise ValueError(
                f"each explicit window must be a {{label, start, end}} mapping (got {entry!r})"
            )
        try:
            label = _windows.validate_label(entry["label"], "explicit")
            start, end = _windows.parse_utc(entry["start"]), _windows.parse_utc(entry["end"])
        except ValueError as e:
            raise ValueError(f"output.windowing.windows: {e}") from e
        if not start < end:
            raise ValueError(
                f"explicit window {label!r} is not half-open: start "
                f"{entry['start']!r} must precede end {entry['end']!r}"
            )
        if label in seen:
            raise ValueError(f"explicit window label {label!r} is declared twice")
        seen[label] = (start, end)
    ordered = sorted(seen.items(), key=lambda kv: kv[1][0])
    for (a, (_sa, ea)), (b, (sb, _eb)) in zip(ordered, ordered[1:]):
        if ea > sb:
            raise ValueError(
                f"explicit windows {a!r} and {b!r} overlap; windows must be "
                f"disjoint half-open ranges"
            )


def get_raster_bands(config: PipelineConfig) -> dict:
    """Normalized ``data_source.bands``: field -> {asset, dtype, fill_value, attrs}.

    ``attrs`` carries CF ``scale_factor``/``add_offset`` when the band
    declares ``scale``/``offset`` — recorded on the array for readers, never
    applied to the stored DNs (issue #218: exact source values).
    """
    out = {}
    for name, meta in (config.data_source.get("bands") or {}).items():
        attrs = {}
        if "scale" in meta:
            attrs["scale_factor"] = meta["scale"]
        if "offset" in meta:
            attrs["add_offset"] = meta["offset"]
        out[name] = {
            "asset": meta["asset"],
            "dtype": meta["dtype"],
            "fill_value": meta.get("fill_value", 0),
            "attrs": attrs,
        }
    return out


_RESAMPLE_HOWS = ("sum", "mean")


def _validate_collection_options(config: PipelineConfig) -> None:
    """Validate ``data_source.collections`` (issue #213, Phase 3).

    The block accepts a list of collection names (no options) or a mapping of
    name -> per-collection reader options consumed by
    :func:`zagg.temporal.prepare_collection`: ``coord_round`` (non-negative
    int, decimals to round lat/lon coords to -- for source grids whose own
    coordinate arrays carry float dirt), ``variables`` (list of names),
    ``time_offset`` (a pandas-parseable offset string), ``resample``
    (``{freq, how: sum|mean, scale}``), and ``derived`` (name -> numpy
    expression string). ``time_offset`` and ``resample.freq`` are parsed with
    pandas here so an unparseable value fails at load rather than surfacing as
    a per-event worker error after the Lambda spend. Unknown option keys (e.g.
    ``doi``) pass through by design for catalog tooling metadata.
    """
    colls = (config.data_source or {}).get("collections")
    if colls is None or isinstance(colls, list):
        return
    if not isinstance(colls, dict):
        raise ValueError(
            "data_source.collections must be a list of names or a mapping of "
            f"name -> options (got {type(colls).__name__})"
        )
    for cname, opts in colls.items():
        if opts is None:
            continue
        if not isinstance(opts, dict):
            raise ValueError(
                f"data_source.collections[{cname!r}] must be a mapping of options (or null)"
            )
        variables = opts.get("variables")
        if variables is not None and (
            not isinstance(variables, list) or not all(isinstance(v, str) for v in variables)
        ):
            raise ValueError(
                f"data_source.collections[{cname!r}].variables must be a list of names"
            )
        coord_round = opts.get("coord_round")
        if coord_round is not None and (
            isinstance(coord_round, bool) or not isinstance(coord_round, int) or coord_round < 0
        ):
            raise ValueError(
                f"data_source.collections[{cname!r}].coord_round must be a "
                f"non-negative integer (got {coord_round!r})"
            )
        offset = opts.get("time_offset")
        if offset is not None:
            if not isinstance(offset, str):
                raise ValueError(
                    f"data_source.collections[{cname!r}].time_offset must be an offset "
                    f"string like '-30min' (got {offset!r})"
                )
            import pandas as pd

            try:
                pd.to_timedelta(offset)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"data_source.collections[{cname!r}].time_offset is not a valid "
                    f"pandas offset string (got {offset!r})"
                ) from exc
        resample = opts.get("resample")
        if resample is not None:
            if not isinstance(resample, dict) or "freq" not in resample:
                raise ValueError(
                    f"data_source.collections[{cname!r}].resample must be a mapping "
                    "with at least 'freq' (optional: how, scale)"
                )
            freq = resample["freq"]
            if not isinstance(freq, str):
                raise ValueError(
                    f"data_source.collections[{cname!r}].resample.freq must be a "
                    f"frequency string like '3h' (got {freq!r})"
                )
            import pandas as pd

            try:
                pd.tseries.frequencies.to_offset(freq)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"data_source.collections[{cname!r}].resample.freq is not a valid "
                    f"pandas frequency string (got {freq!r})"
                ) from exc
            how = resample.get("how", "sum")
            if how not in _RESAMPLE_HOWS:
                raise ValueError(
                    f"data_source.collections[{cname!r}].resample.how must be one of "
                    f"{_RESAMPLE_HOWS} (got {how!r})"
                )
            scale = resample.get("scale", 1)
            if isinstance(scale, bool) or not isinstance(scale, (int, float)):
                raise ValueError(
                    f"data_source.collections[{cname!r}].resample.scale must be numeric "
                    f"(got {scale!r})"
                )
        derived = opts.get("derived")
        if derived is not None and (
            not isinstance(derived, dict)
            or not all(isinstance(k, str) and isinstance(v, str) for k, v in derived.items())
        ):
            raise ValueError(
                f"data_source.collections[{cname!r}].derived must map variable names "
                "to expression strings"
            )


def collection_options(config: PipelineConfig) -> dict[str, dict]:
    """Normalize ``data_source.collections`` to ``{name: options}``.

    Both config forms collapse to one shape: the list form (names only) yields
    empty option dicts; the mapping form yields each collection's options
    verbatim (``null`` becomes ``{}``). Consumed by the temporal reader /
    :func:`zagg.temporal.prepare_collection`.
    """
    colls = (config.data_source or {}).get("collections") or []
    if isinstance(colls, dict):
        return {name: dict(opts or {}) for name, opts in colls.items()}
    return {name: {} for name in colls}


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


# Recognized per-field output kinds. ``ragged`` is the Tier-2 carrier
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
    to a scalar count). See issue #29 (vector) and issue #48 (ragged).

    A field may also declare ``resolution`` (``cell`` default, or ``chunk``).
    A ``resolution: chunk`` field (issue #30 item 2) is written ONCE per chunk
    into a companion array shaped at the chunk grid (``main.shape //
    chunk_shape``), indexed by ``grid.block_index(shard_key)`` — the compact
    storage for a chunk-uniform value (e.g. a ``chunk_precompute`` anchor).
    ``scalar``, ``vector``, and ``ragged`` kinds may all be ``resolution: chunk``
    (issue #82): a ``scalar`` companion is a plain chunk-grid array, a ``vector``
    companion appends the field's ``trailing_shape`` to the chunk grid (chunked
    whole), and a ``ragged`` companion holds one
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

    # ``location`` (issue #87) is the ragged location channel; other kinds have
    # no companion ragged array to carry it.
    if "location" in meta and kind != "ragged":
        raise ValueError(
            f"Variable '{name}': 'location' is only valid for kind 'ragged', not '{kind}'"
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
    # ``ragged`` at chunk resolution (issue #82) stores one variable-length
    # payload per chunk instead of per cell. It rides the same vlen writer as
    # cell-resolution ragged (``write_ragged_to_zarr``), which collapses
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

    # Optional location channel (issue #87): the reducer also receives the named
    # per-observation morton column (``locations=`` kwarg) and returns a
    # ``(payload, locations)`` pair, stored as a uint64 companion vlen array. Only
    # a ``function`` reducer can accept the kwarg, and the chunk-uniform collapse
    # of ``resolution: chunk`` has no location fold — reject both combinations.
    location = meta.get("location")
    if location is not None:
        if not isinstance(location, str):
            raise ValueError(
                f"Variable '{name}': 'location' must be a column name string (got {location!r})"
            )
        if "function" not in meta:
            raise ValueError(
                f"Variable '{name}': 'location' requires a 'function' reducer "
                f"(an expression cannot receive the locations column)"
            )
        if resolution == "chunk":
            raise ValueError(
                f"Variable '{name}': 'location' is not supported with "
                f"'resolution: chunk' (locations are folded per cell)"
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


def _validate_index(data_source: dict) -> None:
    """Validate the optional ``data_source.index`` block (issue #160).

    Delegates to :func:`zagg.index.validate_index_config` (lazy import — the
    backend registry knows each backend's accepted keys, including entry-point
    backends this module cannot enumerate). Absent block → the default
    hierarchical path, nothing to check.
    """
    index_cfg = data_source.get("index")
    if index_cfg is None:
        return
    from zagg.index import validate_index_config

    validate_index_config(index_cfg, data_source)


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
        ``location`` is the ragged location channel's column name (issue #87), or
        ``None`` when the field carries no location.
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
        # Ragged location channel (issue #87): the per-observation morton column
        # the reducer folds per centroid; ``None`` for unlocated fields.
        "location": meta.get("location"),
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
        entry = {
            "name": name,
            "kind": sig["kind"],
            "trailing_shape": list(sig["trailing_shape"]),
            "inner_shape": list(sig["inner_shape"]),
            "dtype": sig["dtype"],
        }
        # A location channel changes the store schema (a uint64 companion vlen
        # array — issue #87), so it belongs in the signature; keyed only when set
        # so existing shard-map signatures are byte-identical.
        if sig["location"] is not None:
            entry["location"] = sig["location"]
        fields.append(entry)
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


def get_sharded(config: PipelineConfig, default: bool = False) -> bool:
    """Return whether the output grid uses ShardingCodec storage (issue #108).

    The ``sharded`` knob lives on the grid/chunk block next to ``chunk_inner``
    (mirroring its accessor). When ``True`` the grid bundles a dispatch shard's K
    inner chunks into one zarr shard object instead of K independent regular chunk
    objects; a K==1 grid has nothing to bundle, so the grid silently no-ops it
    (issue #215). ``default`` is the value returned when the flag is omitted —
    ``False`` here, but ``from_config`` passes ``True`` for HEALPix output on
    both store layouts (issue #215 flat, issue #236 hive: a missing flag should
    not cost the ~K-fold object blow-up).
    """
    return bool(config.output.get("grid", {}).get("sharded", default))


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


def get_store_layout(config: PipelineConfig) -> str:
    """Return the STORE layout — flat single store vs morton hive (issue #199).

    Distinct from ``output.grid.layout`` (the HEALPix *array* layout inside one
    store): ``output.store_layout`` selects how shard output is arranged under
    ``output.store``. ``"flat"`` (default) is today's single shared zarr store.
    ``"hive"`` writes one self-describing leaf zarr per shard at
    ``{store}/{sign+base}/{d1}/.../{full_id}.zarr`` with a root
    ``morton_hive.json`` manifest and a per-leaf commit stamp — see
    ``docs/design/sparse_coverage.md`` (D1-D6) and :mod:`zagg.hive`. A
    present-but-null key falls back to the default, like ``layout``.

    Returns
    -------
    str
        ``"flat"`` (default) or ``"hive"``.
    """
    return config.output.get("store_layout") or "flat"


def get_coverage_moc(config: PipelineConfig) -> bool:
    """Whether the end-of-run root ``coverage.moc`` write is on (issue #200).

    Default ON for hive-layout stores (O9/espg: coverage MOCs are the default
    for healpix templates — and hive is healpix-only by validation);
    ``output.coverage_moc: false`` opts out. Flat-layout / non-healpix
    configs default off, and an explicit ``true`` there is rejected by
    ``validate_config``. A present-but-null key falls back to the default,
    like ``store_layout``.
    """
    flag = config.output.get("coverage_moc")
    if flag is None:
        return get_store_layout(config) == "hive"
    return bool(flag)


def get_windowing(config: PipelineConfig) -> dict | None:
    """The normalized temporal windowing declaration, or ``None`` (issue #246).

    ``None`` — absent block, null block, or an explicit ``schedule: none`` —
    is today's unwindowed behavior (bare leaf names, ``morton-hive/1``).
    Otherwise a normalized dict with defaults resolved::

        {"schedule", "time_field", "epoch", "scale", "units", "windows"}

    ``epoch`` and explicit-window boundaries are canonicalized to ISO-8601
    UTC strings; ``windows`` is ``None`` except for ``schedule: explicit``.
    The same dict feeds the manifest temporal block
    (:func:`zagg.hive.build_manifest`) and the dispatch fan-out, so the two
    can never disagree.
    """
    from zagg import windows as _windows

    block = config.output.get("windowing")
    if not block or block.get("schedule", "none") == "none":
        return None
    declared = None
    if block["schedule"] == "explicit":
        declared = [
            {
                "label": w["label"],
                "start": _windows.iso_utc(_windows.parse_utc(w["start"])),
                "end": _windows.iso_utc(_windows.parse_utc(w["end"])),
            }
            for w in block["windows"]
        ]
    if (config.data_source or {}).get("reader") == "raster":
        # Raster membership is the acquisition's STAC ``datetime`` (issue
        # #247, ratified): the manifest records the resolved field plus the
        # fixed encoding any ISO-8601 UTC instant normalizes to (UTC seconds
        # since the Unix epoch). _validate_windowing rejects the conversion
        # knobs on raster configs, so nothing here can disagree with it.
        return {
            "schedule": block["schedule"],
            "time_field": "datetime",
            "epoch": "1970-01-01T00:00:00+00:00",
            "scale": "utc",
            "units": "seconds",
            "windows": declared,
        }
    return {
        "schedule": block["schedule"],
        "time_field": block["time_field"],
        "epoch": _windows.iso_utc(_windows.parse_utc(block["epoch"])),
        "scale": block.get("scale") or "utc",
        "units": block.get("units") or "seconds",
        "windows": declared,
    }


def window_time_filters(config: PipelineConfig, start: float, end: float) -> list[dict]:
    """Structured filters implementing one window's ``[start, end)`` (issue #246).

    The observation-level window filter is exactly a pair of structured
    predicates on the declared ``time_field`` — ``ge start`` / ``lt end`` in
    DATASET units (converted once at dispatch) — so it rides the existing,
    pushdown-eligible filter machinery (issue #43) on every backend instead
    of a bespoke row filter. ``_validate_windowing`` restricts ``time_field``
    to a base-rate ``data_source.variables`` entry (the base level reads its
    columns from there too), so the predicate always filters at base rate
    (``level=None``, per observation); a coordinate or non-base/segment-rate
    ``time_field`` is rejected at validation and never reaches here. Appended
    to :func:`filters_from_data_source`'s normalized output by the hive write
    path, preserving any declared filters.
    """
    windowing = get_windowing(config)
    if windowing is None:
        raise ValueError("window_time_filters requires a windowed config (output.windowing)")
    field = windowing["time_field"]
    path = ((config.data_source or {}).get("variables") or {}).get(field)
    if not (isinstance(path, str) and path):
        raise ValueError(
            f"windowing time_field {field!r} has no base-rate dataset path in "
            f"data_source.variables — validate_config should have rejected "
            f"this configuration"
        )
    return [
        {"level": None, "dataset": path, "op": "ge", "value": float(start)},
        {"level": None, "dataset": path, "op": "lt", "value": float(end)},
    ]


def windowed_cell_config(config: PipelineConfig, window: dict) -> tuple[PipelineConfig, dict]:
    """One work unit's config with its window filter injected (issue #246).

    Returns ``(config, windowing)``: a per-unit config copy whose normalized
    filter list gains the :func:`window_time_filters` pair for ``window``'s
    dataset-unit ``[start, end)`` (declared filters preserved — the explicit
    list wins over ``quality_filter`` sugar, so normalize-then-append), plus
    the normalized windowing declaration. Raises if the config declares no
    ``output.windowing`` — a dispatched window without one is dispatcher/
    config drift, never guessed around.
    """
    from dataclasses import replace

    windowing = get_windowing(config)
    if windowing is None:
        raise ValueError(
            "a window was dispatched but the config declares no output.windowing "
            "block — dispatcher/config drift, refusing to guess the time_field"
        )
    ds = dict(config.data_source)
    ds["filters"] = filters_from_data_source(ds) + window_time_filters(
        config, window["start"], window["end"]
    )
    return replace(config, data_source=ds), windowing


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


def get_consolidate_metadata(config: PipelineConfig) -> bool:
    """Whether the finalize step consolidates zarr metadata (issue #191).

    ``output.consolidate_metadata: true`` runs ``zarr.consolidate_metadata`` as a
    finalize step after every cell completes, producing a consolidated-metadata
    blob. Defaults to ``False``: no zagg reader opens with ``use_consolidated`` —
    readers navigate to specific paths in a few GETs — and consolidation is an
    optional zarr-v3 extension that costs ~70 s of serial metadata GETs per run.
    When off, the finalize invoke is skipped entirely and stores read fine (v3
    readers do lazy metadata reads natively).

    Parameters
    ----------
    config : PipelineConfig

    Returns
    -------
    bool
    """
    return bool(config.output.get("consolidate_metadata", False))


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
