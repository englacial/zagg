"""``data_source`` block validators (issue #677 phase 0)."""

from zagg.config.core import _SET_OPS, FILTER_OPS, PipelineConfig
from zagg.config.validate_output import _validate_store_layout_keys
from zagg.config.windowing import _validate_windowing


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


def _validate_ds_variables(data_source: dict) -> None:
    """Validate ``data_source.variables`` entries (issue #321).

    Each value is either a path-template string (reads the whole dataset) or a
    mapping ``{path: <str>, column: <int >= 0>}`` selecting one column of a 2-D
    dataset — the variable analogue of the ``column`` key on structured
    filters, so several scalar columns can be declared against one shared
    dataset path (deduped to a single read).
    """
    variables = (data_source or {}).get("variables", {})
    if not isinstance(variables, dict):
        raise ValueError("data_source.variables must be a mapping of name -> path")
    for name, entry in variables.items():
        if isinstance(entry, str):
            continue
        if not isinstance(entry, dict):
            raise ValueError(
                f"data_source.variables['{name}'] must be a path string or a "
                f"{{path, column}} mapping (got {type(entry).__name__})"
            )
        # Synthesized-column form (issue #425, vlen grammar): the column is
        # computed per record — ``linspace(start, stop, count)`` over each
        # record's samples — never read. Cross-checks against ``levels`` and
        # ``coordinates.level`` live in ``_validate_vlen_source``.
        if "synthesize" in entry:
            if entry["synthesize"] != "linspace":
                raise ValueError(
                    f"data_source.variables['{name}'].synthesize must be 'linspace' "
                    f"(got {entry['synthesize']!r})"
                )
            extra = set(entry) - {"synthesize", "level", "start", "stop"}
            if extra:
                raise ValueError(
                    f"data_source.variables['{name}']: unknown keys {sorted(extra)} "
                    "(the synthesize form takes 'synthesize', 'level', 'start', 'stop')"
                )
            for key in ("level", "start", "stop"):
                if not isinstance(entry.get(key), str) or not entry[key]:
                    raise ValueError(
                        f"data_source.variables['{name}'].{key} must be a non-empty string"
                    )
            continue
        extra = set(entry) - {"path", "column"}
        if extra:
            raise ValueError(
                f"data_source.variables['{name}']: unknown keys {sorted(extra)} "
                "(the mapping form takes exactly 'path' and 'column')"
            )
        if not isinstance(entry.get("path"), str):
            raise ValueError(f"data_source.variables['{name}'].path must be a string")
        column = entry.get("column")
        if not isinstance(column, int) or isinstance(column, bool) or column < 0:
            raise ValueError(
                f"data_source.variables['{name}'].column must be an integer >= 0 (got {column!r})"
            )


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
        asset = f.get("asset")
        if asset is not None:
            # Sibling-asset predicate (issue #425): evaluated on the paired
            # asset's per-record rows after the declared key join, so it is
            # record-level by construction — a 'level' would be ambiguous.
            if not isinstance(asset, str) or not asset:
                raise ValueError(f"filter[{i}]: 'asset' must be a non-empty string")
            if f.get("level") is not None:
                raise ValueError(
                    f"filter[{i}]: 'asset' filters are record-level by construction "
                    "(the join key aligns records); omit 'level'"
                )
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


def _validate_vlen_source(data_source: dict) -> None:
    """Cross-validate the vlen grammar surface (issue #425).

    The vlen route is selected by ``coordinates.level`` (the base level has no
    native coordinates; they expand from that record level). Checks:

    - ``coordinates.level`` names a non-base level whose ``link`` points
      directly at ``base_level`` (the ragged-gather link);
    - ``synthesize:`` variables require the vlen route and their ``level``
      must equal ``coordinates.level`` (the within-record index used by the
      linspace comes from that level's link);
    - ``data_source.assets`` entries carry a ``join`` block with string
      ``left``/``right`` key paths; every filter's ``asset`` names a declared
      entry, and asset filters require the vlen route;
    - asset-sourced level variables (issue #464) require the vlen route, live
      on ``coordinates.level`` only (the record-key join aligns that level's
      records), and name a declared asset.
    """
    ds = data_source or {}
    coords = ds.get("coordinates")
    coord_level = coords.get("level") if isinstance(coords, dict) else None
    levels = ds.get("levels")
    base_level = ds.get("base_level")
    if coord_level is not None:
        if not isinstance(levels, dict) or base_level is None:
            raise ValueError("data_source.coordinates.level requires 'levels' and 'base_level'")
        if coord_level not in levels:
            raise ValueError(
                f"data_source.coordinates.level {coord_level!r} is not a key in levels "
                f"(available: {sorted(levels)})"
            )
        link = (levels[coord_level] or {}).get("link")
        if not isinstance(link, dict) or link.get("to") != base_level:
            raise ValueError(
                f"data_source.coordinates.level {coord_level!r} must link directly to "
                f"base level {base_level!r}"
            )
    for name, entry in (ds.get("variables") or {}).items():
        if not isinstance(entry, dict) or "synthesize" not in entry:
            continue
        if coord_level is None:
            raise ValueError(
                f"data_source.variables['{name}']: synthesize requires the vlen route "
                "(set data_source.coordinates.level)"
            )
        if entry["level"] != coord_level:
            raise ValueError(
                f"data_source.variables['{name}'].level {entry['level']!r} must equal "
                f"coordinates.level {coord_level!r} (the within-record index comes "
                "from that level's link)"
            )
    assets = ds.get("assets")
    if assets is not None:
        if not isinstance(assets, dict) or not assets:
            raise ValueError("data_source.assets must be a non-empty mapping of name -> {join}")
        for name, cfg in assets.items():
            if not isinstance(cfg, dict) or not isinstance(cfg.get("join"), dict):
                raise ValueError(f"data_source.assets['{name}'] requires a 'join' mapping")
            join = cfg["join"]
            unknown = set(join) - {"left", "right"}
            if unknown:
                raise ValueError(
                    f"data_source.assets['{name}'].join: unknown keys {sorted(unknown)} "
                    "(takes 'left' and 'right' record-key dataset paths)"
                )
            for side in ("left", "right"):
                if not isinstance(join.get(side), str) or not join[side]:
                    raise ValueError(
                        f"data_source.assets['{name}'].join.{side} must be a non-empty string"
                    )
    for i, f in enumerate(ds.get("filters") or []):
        asset = f.get("asset") if isinstance(f, dict) else None
        if asset is None:
            continue
        if not isinstance(assets, dict) or asset not in assets:
            raise ValueError(
                f"filter[{i}]: asset {asset!r} is not declared in data_source.assets "
                f"(available: {sorted(assets) if isinstance(assets, dict) else []})"
            )
        if coord_level is None:
            raise ValueError(
                f"filter[{i}]: 'asset' filters require the vlen route "
                "(set data_source.coordinates.level)"
            )
    # Asset-sourced level variables (issue #464): shape is validated in
    # ``_validate_overviews``; the cross-checks live here because they need
    # ``coordinates.level`` and ``assets``.
    if isinstance(levels, dict):
        for lvl_name, lvl in levels.items():
            lvl_vars = lvl.get("variables") if isinstance(lvl, dict) else None
            if not isinstance(lvl_vars, dict):
                continue
            for var_name, entry in lvl_vars.items():
                if not isinstance(entry, dict):
                    continue
                where = f"levels.{lvl_name}.variables['{var_name}']"
                if coord_level is None:
                    raise ValueError(
                        f"{where}: asset variables require the vlen route "
                        "(set data_source.coordinates.level)"
                    )
                if lvl_name != coord_level:
                    raise ValueError(
                        f"{where}: asset variables are record-level by construction "
                        f"(the join key aligns records); declare them on "
                        f"coordinates.level {coord_level!r}"
                    )
                asset = entry.get("asset")
                if not isinstance(assets, dict) or asset not in assets:
                    raise ValueError(
                        f"{where}: asset {asset!r} is not declared in data_source.assets "
                        f"(available: {sorted(assets) if isinstance(assets, dict) else []})"
                    )


def _validate_overviews(data_source: dict) -> None:
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
        # base rows at read time. An entry may instead be an ``{asset, path}``
        # mapping (issue #464): the same broadcast, sourced from a sibling asset
        # via the record-key join. Validate names and entry shapes, and forbid the
        # mapping form on the base level (the base level uses
        # ``data_source.variables``). The documentation-only ``list[str]`` form
        # stays valid on any level.
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
                if isinstance(tmpl, dict):
                    # Asset-sourced record-level variable (issue #464): the
                    # dataset is read from the named sibling asset's handle and
                    # joined per record on the declared ``assets.<name>.join``
                    # key. Shape only here; cross-checks against ``assets`` and
                    # ``coordinates.level`` live in ``_validate_vlen_source``.
                    unknown = set(tmpl) - {"asset", "path"}
                    if unknown:
                        # 'column' is the near-miss worth naming: the plain
                        # variable form takes one, while the asset form is
                        # scoped to record-rate scalars and does not (issue
                        # #464 review; richer L2A companions ride issue #465).
                        hint = (
                            "; the asset form reads 1-D datasets only, with no "
                            "'column' selector yet (refs issue #465)"
                            if "column" in unknown
                            else ""
                        )
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}: unknown keys "
                            f"{sorted(unknown)} (the asset form takes 'asset' and 'path'){hint}"
                        )
                    for key in ("asset", "path"):
                        if not isinstance(tmpl.get(key), str) or not tmpl[key]:
                            raise ValueError(
                                f"levels.{name}.variables.{var_name}.{key} must be a "
                                f"non-empty string"
                            )
                elif not isinstance(tmpl, str) or not tmpl:
                    raise ValueError(
                        f"levels.{name}.variables.{var_name}: path template must be a "
                        f"non-empty string or an {{asset, path}} mapping (got {tmpl!r})"
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
