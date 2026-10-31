"""``validate_config``: the cross-block entry point (issue #677 phase 0)."""

import inspect

from zagg.config.core import (
    WORKER_MEMORIES,
    PipelineConfig,
    _validate_json_floats,
    get_pipeline_type,
)
from zagg.config.expressions import _is_numeric, _validate_expression_columns, resolve_function
from zagg.config.source import _segment_variable_names, filters_from_data_source
from zagg.config.validate_fields import (
    _TEMPORAL_SPEC_KEYS,
    _validate_chunk_precompute,
    _validate_composition_attrs,
    _validate_output_kind,
)
from zagg.config.validate_output import (
    _validate_store_layout_keys,
    _validate_time_encoding,
    _validate_time_source,
)
from zagg.config.validate_source import (
    _validate_collection_options,
    _validate_ds_variables,
    _validate_filter_levels,
    _validate_filters,
    _validate_index,
    _validate_overviews,
    _validate_raster_config,
    _validate_vlen_source,
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

    # The time coordinate's encoding (spec §8, issue #443) is checked on every
    # pipeline kind, like the two blocks above: the value is only meaningful
    # where a time coordinate exists, so declaring it elsewhere is a typo
    # worth catching at submission rather than a knob that silently does
    # nothing.
    _validate_time_encoding(config)
    # The per-observation clock the §8.2/§8.3 companions encode from (issue
    # #410) — checked here for the same reason as the block above: the value is
    # only meaningful where a companion is declared, so declaring it on a
    # pipeline kind that has none is a typo worth catching at submission.
    _validate_time_source(config)

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
        if layout is not None and layout != "fullsphere":
            raise ValueError(
                f"output.grid.layout must be 'fullsphere' (got {layout!r}; "
                "the deprecated 'dense' layout was removed — issue #88)"
            )
        # The issue #135 knob is retired (issue #304 phase 3, D16 — #135
        # inverts): morton is the stored cell coordinate and NESTED ids are
        # DERIVED at read (moczarr fabrication). A config still carrying the
        # key gets a pointed error, never a silent ignore.
        if grid.get("cell_ids_encoding") is not None:
            raise ValueError(
                "output.grid.cell_ids_encoding was retired (issue #304): morton is "
                "the stored cell coordinate and NESTED ids are derived at read; "
                "to keep writing the legacy NESTED cell_ids array for transition "
                "stores set output.grid.emit_cell_ids: true"
            )
        # Transition escape hatch (issue #304 phase 2): emit the legacy NESTED
        # cell_ids array ALONGSIDE the morton coordinate. Default off — morton
        # is the only stored cell coordinate (D16); the hatch exists for
        # transition stores (browser-direct demos) until the gridlook morton
        # path lands. HEALPix-only: rectilinear grids have no cell_ids
        # coordinate, so the flag would silently do nothing there.
        emit = grid.get("emit_cell_ids")
        if emit is not None:
            if not isinstance(emit, bool):
                raise ValueError(f"output.grid.emit_cell_ids must be a boolean (got {emit!r})")
            if emit and grid["type"] != "healpix":
                raise ValueError(
                    "output.grid.emit_cell_ids only applies to healpix grids "
                    f"(grid type is {grid['type']!r})"
                )
        # The legacy output.grid.indexing_scheme key is descriptive only (the
        # shipped configs carry "nested"); it never selected an encoding.
        # Reject any other value so a user reaching for it fails loudly.
        legacy_scheme = grid.get("indexing_scheme")
        if legacy_scheme is not None and legacy_scheme != "nested":
            raise ValueError(
                f"output.grid.indexing_scheme is descriptive and must be 'nested' "
                f"(got {legacy_scheme!r}); the store's declared coordinate is morton "
                f"(D16 — issue #304)"
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

    # Validate base-level variable entries: plain path string, or the
    # column-selector mapping form (issue #321)
    _validate_ds_variables(config.data_source)

    # Validate the structured filter list (issue #43, Phase A)
    _validate_filters(config.data_source)

    # Validate hierarchical multi-level form (issue #43, Phase B)
    _validate_overviews(config.data_source)

    # Cross-check: each filter's level field must name a key in levels (issue #43)
    _validate_filter_levels(config.data_source)

    # Vlen grammar surface (issue #425): coordinates.level, synthesize
    # variables, sibling-asset declarations + asset filters.
    _validate_vlen_source(config.data_source)

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
        _validate_output_kind(name, meta, config)

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

        # The §8.3 per-centroid temporal channel takes the same sibling-name
        # reservation, for the same corruption reason (issue #410) — plus the
        # same two reducer-surface checks the location channel gets, because it
        # rides in the same way: as a reserved ``temporal=`` kwarg injected by
        # aggregate.py.
        if meta.get("temporal") == "per-centroid":
            from zagg.grids.base import ragged_times_name

            if "temporal" in meta.get("params", {}):
                raise ValueError(
                    f"Variable '{name}': params may not name 'temporal' — it is "
                    f"reserved for the temporal channel (spec §8.3, issue #410)"
                )
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
            sibling = ragged_times_name(name)
            if sibling in agg_vars or sibling in config.aggregation.get("coordinates", {}):
                raise ValueError(
                    f"Variable '{name}': its temporal channel is stored in a sibling "
                    f"array named '{sibling}', which collides with the declared "
                    f"field/coordinate of that name — rename one of them (spec §8.3)"
                )

        # Field-level store attrs (issue #321): free-form provenance the grid
        # template merges onto the field's array spec (e.g. the composition
        # field's lane order and signal threshold), so readers bind to
        # metadata instead of reconstructing config conventions.
        attrs = meta.get("attrs")
        if attrs is not None:
            if not isinstance(attrs, dict) or not all(isinstance(k, str) for k in attrs):
                raise ValueError(f"Variable '{name}': attrs must be a mapping with string keys")
            from zagg.grids.base import (
                LOCATED_ATTR,
                RAGGED_ELEMENT_ATTR,
                TIMES_ATTR,
                WEIGHTS_ATTR,
            )
            from zagg.time_axis import TEMPORAL_ATTR

            if RAGGED_ELEMENT_ATTR in attrs:
                raise ValueError(
                    f"Variable '{name}': attrs key {RAGGED_ELEMENT_ATTR!r} is reserved "
                    f"for the ragged layout block (issue #209)"
                )
            # The §2.0 weights key is spec-owned on ragged payload arrays:
            # the template stamps it from the field-level ``weights:``
            # declaration, so an attrs transcription could silently disagree.
            if meta.get("kind") == "ragged" and WEIGHTS_ATTR in attrs:
                raise ValueError(
                    f"Variable '{name}': attrs key {WEIGHTS_ATTR!r} is spec-owned on a "
                    f"ragged payload array — declare the field-level 'weights:' key "
                    f"instead (spec §2.0, issue #424)"
                )
            # Same discipline for the §8/§9 declaration blocks and the §8.3
            # sibling binding (issue #410): the template stamps all three
            # from the field-level ``temporal:``/``location:`` declarations,
            # so an author transcription could disagree with the words the
            # writer actually stored.
            for key in (TEMPORAL_ATTR, LOCATED_ATTR, TIMES_ATTR):
                if key in attrs:
                    raise ValueError(
                        f"Variable '{name}': attrs key {key!r} is spec-owned (spec §8/§9) — "
                        f"declare the field-level 'temporal:'/'location:' key instead "
                        f"(issue #410)"
                    )
            import json

            try:
                json.dumps(attrs)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Variable '{name}': attrs must be JSON-serializable ({exc})"
                ) from exc

            _validate_composition_attrs(name, meta, attrs, agg_vars)


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
