"""``validate_config``: the cross-block entry point (issue #677 phase 0)."""

import inspect

from zagg.config.core import (
    WORKER_MEMORIES,
    PipelineConfig,
    _validate_json_floats,
    get_pipeline_type,
)
from zagg.config.expressions import _is_numeric, _validate_expression_columns, resolve_function
from zagg.config.models import validate_model
from zagg.config.source import _segment_variable_names, filters_from_data_source
from zagg.config.validate_fields import _validate_chunk_precompute, _validate_temporal_producer
from zagg.config.validate_output import (
    _validate_pyramid,
    _validate_time_source,
    _warn_nan_ambiguous_reductions,
)
from zagg.config.windowing import _validate_windowing


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
    # Dispatchability first (issue #448 fold review): every pipeline kind ships
    # its config through the same strict-JSON Lambda payload, so the check runs
    # before the kind branch and its early returns.
    _validate_json_floats(config)

    # The grammar (issue #677): shape, types, unknown keys and the per-block
    # cross-field rules the models carry, with every refusal path-addressed.
    # Runs before the kind branch because the root union picks the grammar the
    # way this function branches (``pipeline.type``, then ``reader: raster``).
    # The checks below are the genuinely cross-block ones the models cannot
    # express. The pipeline kind is resolved first so a mistyped
    # ``pipeline.type`` is named as such, not misread as another kind's grammar.
    ptype = get_pipeline_type(config)
    validate_model(config)

    # The optional top-level worker block (issue #235) selects a
    # pre-provisioned Lambda size variant on any pipeline kind, so it is
    # validated before the kind branch, like the grammar pass above.
    _validate_worker(config)

    # The time coordinate's encoding (spec §8, issue #443) is checked on every
    # pipeline kind, like the two blocks above: the value is only meaningful
    # where a time coordinate exists, so declaring it elsewhere is a typo
    # worth catching at submission rather than a knob that silently does
    # nothing.
    # The per-observation clock the §8.2/§8.3 companions encode from (issue
    # #410) — checked here for the same reason as the block above: the value is
    # only meaningful where a companion is declared, so declaring it on a
    # pipeline kind that has none is a typo worth catching at submission.
    _validate_time_source(config)

    if ptype != "spatial":
        _validate_temporal_config(config)
        return

    # Raster pipelines (issue #218) share the spatial grid checks but replace
    # the HDF5 read-side schema (groups/coordinates/variables) and the
    # aggregation section with a declarative band map: pull-NN yields exactly
    # one value per cell per timestep, so there is nothing to reduce.
    if (config.data_source or {}).get("reader") == "raster":
        # The raster kind's shape and couplings are the models' (``RasterConfig``);
        # the shared store-layout family is ``Output``'s. Only the windowing
        # time_field/epoch rules (data_source-dependent) and the NaN warning run.
        _validate_windowing(config)
        return

    # Required sections
    for section in ("data_source", "aggregation", "output"):
        val = getattr(config, section)
        if not val:
            raise ValueError(f"Missing required section: {section}")

    grid = config.output.get("grid")

    # The output block's shape and its intra-block couplings (store layout,
    # hive-only companions, pyramid grammar + orders, windowing layout) are the
    # model's (``models_output``, issue #677 phase 3); the NaN-fill reduction
    # warning is per config validation (espg ruling on the PR #344 finding).
    _warn_nan_ambiguous_reductions(config)
    _validate_pyramid(config)

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

    # The data_source grammar — variable forms, filters, levels/links, the vlen
    # surface, the chunk-index block — is the model's (``models_source``, issue
    # #677 phase 1); only the column cross-references below remain here.
    # Segment-level (non-base) ``variables`` mappings (issue #30) become real
    # per-photon columns in the pooled shard data once broadcast, so they are valid
    # column references everywhere a ``data_source.variables`` column is (agg
    # sources/expressions, chunk_precompute sources). Fold their names into ds_vars.
    ds_vars = set(config.data_source.get("variables", {}).keys()) | _segment_variable_names(
        config.data_source
    )
    agg_vars = config.aggregation.get("variables", {})
    # The derived toc word column (spec §8.2/§8.3, issue #410): materialized per
    # observation from ``output.time_source``, so it is a real base-rate column —
    # but READABLE only under exactly the condition
    # ``aggregate.calculate_cell_statistics`` materializes it on, which is a field
    # declaring a ``temporal:`` companion. Widening on the clock alone is true of
    # every windowed gps/tai store via the ``toc_source`` fallback, so a config
    # naming ``toc_word`` there validated clean and then died in the worker with
    # ``KeyError``/``NameError`` (issue #410 review). The NAME is reserved
    # unconditionally, like ``leaf_id`` — that side belongs to the reservation,
    # not to whether the column happens to exist.
    from zagg.time_axis import TOC_WORD_COLUMN, toc_source

    # ``coordinates`` is in the reservation too (issue #410 review): coordinate
    # columns are read into the same ``cell_data`` mapping, where the derived
    # words would overwrite the read column — silently, and only for cells with
    # observations.
    coord_names = set((config.data_source or {}).get("coordinates") or {})
    if TOC_WORD_COLUMN in ds_vars | coord_names:
        surface = "variables" if TOC_WORD_COLUMN in ds_vars else "coordinates"
        raise ValueError(
            f"data_source.{surface} declares {TOC_WORD_COLUMN!r}, which is the "
            f"reserved name of the derived toc word column output.time_source "
            f"materializes (spec §8.2/§8.3) — rename the column"
        )
    if toc_source(config) is not None and any(
        isinstance(m, dict) and m.get("temporal") for m in agg_vars.values()
    ):
        ds_vars = ds_vars | {TOC_WORD_COLUMN}

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

    # The per-field grammar — exactly one of function/expression, the output
    # kind and its shape keys, resolution, the temporal shape, weights,
    # overview_delta, attrs and the composition cross-check — is the model's
    # (``models_aggregation``, issue #677 phase 2). What remains needs the read
    # columns, the grid, or an importable reducer.
    for name, meta in agg_vars.items():
        has_func = "function" in meta
        has_expr = "expression" in meta

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

        # A temporal companion needs a producing reducer and the store's clock
        if meta.get("temporal") is not None:
            _validate_temporal_producer(name, meta, config)

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
            # kwarg (aggregate.py): a reducer that cannot accept the kwarg would
            # crash on every populated cell — reject at load time instead (the
            # params-name collision is the model's).
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
        # The §8.3 per-centroid temporal channel rides the same way: as a
        # reserved ``temporal=`` kwarg injected by aggregate.py, so the reducer
        # surface gets the same check (the sibling-name reservation and the
        # params-name collision are the model's).
        if meta.get("temporal") == "per-centroid":
            if has_func:
                func = resolve_function(meta["function"])
                try:
                    func_params = inspect.signature(func).parameters
                except (TypeError, ValueError):
                    func_params = None  # ufuncs/builtins: not introspectable
                if func_params is not None and "temporal" not in func_params:
                    has_var_kw = any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in func_params.values()
                    )
                    if not has_var_kw:
                        raise ValueError(
                            f"Variable '{name}': function {meta['function']!r} does not "
                            f"accept a 'temporal' keyword, which 'temporal: per-centroid' "
                            f"requires (use a reducer carrying the channel, e.g. "
                            f"zagg.stats.tdigest.build_tdigest)"
                        )


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

    Temporal pipelines carry no output grid. The per-variable keys are
    :class:`~zagg.config.models_aggregation.TemporalFieldSpec`'s (checked by
    :func:`~zagg.config.models.validate_model`, which runs first); this
    refuses an empty ``variables`` mapping with ``ValueError``.
    """
    variables = config.aggregation.get("variables")
    if not variables:
        raise ValueError("temporal pipeline requires aggregation.variables")
