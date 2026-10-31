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


#: ``synthesize:`` kinds and the keys each form takes beside ``synthesize`` and
#: ``level`` (issues #425, #678). ``linspace`` interpolates per record between
#: two per-record endpoints; ``offsets`` is ``start[record] + offsets[sample] *
#: scale`` with ``offsets`` a 1-D path of length ``width`` or a constant step;
#: ``index`` is the within-record sample index itself, so the expression filter
#: can drop a fixed-width row's padding.
_SYNTHESIZE_KEYS = {
    "linspace": {"start", "stop"},
    "offsets": {"start", "offsets", "scale"},
    "index": set(),
}


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_axes(where: str, axes) -> None:
    """Validate an ``axes`` declaration (issue #678).

    One entry per dataset dimension: ``record`` exactly once (the record
    level's axis), ``sample`` exactly once (the fixed-width axis, length
    ``link.width``), and otherwise a non-negative integer pick along that
    dimension (a band). Rank against the dataset is a read-time check.
    """
    if not isinstance(axes, list) or not axes:
        raise ValueError(f"{where}.axes must be a non-empty list (got {axes!r})")
    for entry in axes:
        if entry in ("record", "sample"):
            continue
        if not isinstance(entry, int) or isinstance(entry, bool) or entry < 0:
            raise ValueError(
                f"{where}.axes entries must be 'record', 'sample' or an integer >= 0 "
                f"(got {entry!r})"
            )
    for role in ("record", "sample"):
        if axes.count(role) != 1:
            raise ValueError(f"{where}.axes must name {role!r} exactly once (got {axes!r})")


def _validate_synthesize_entry(where: str, entry: dict) -> None:
    """Shape-check one ``synthesize:`` variable entry; cross-checks live in
    :func:`_validate_vlen_source`."""
    kind = entry["synthesize"]
    if kind not in _SYNTHESIZE_KEYS:
        raise ValueError(
            f"{where}.synthesize must be one of {sorted(_SYNTHESIZE_KEYS)} (got {kind!r})"
        )
    allowed = {"synthesize", "level"} | _SYNTHESIZE_KEYS[kind]
    extra = set(entry) - allowed
    if extra:
        raise ValueError(
            f"{where}: unknown keys {sorted(extra)} (the {kind!r} form takes {sorted(allowed)})"
        )
    required = {"linspace": ("level", "start", "stop"), "offsets": ("level", "start")}
    for key in required.get(kind, ("level",)):
        if not isinstance(entry.get(key), str) or not entry[key]:
            raise ValueError(f"{where}.{key} must be a non-empty string")
    if kind == "offsets":
        offsets = entry.get("offsets")
        if not ((isinstance(offsets, str) and offsets) or _is_number(offsets)):
            raise ValueError(
                f"{where}.offsets must be a 1-D dataset path or a numeric step (got {offsets!r})"
            )
        if "scale" in entry and not _is_number(entry["scale"]):
            raise ValueError(f"{where}.scale must be a number (got {entry['scale']!r})")


def _validate_ds_variables(data_source: dict) -> None:
    """Validate ``data_source.variables`` entries (issue #321).

    Each value is either a path-template string (reads the whole dataset) or a
    mapping ``{path: <str>, column: <int >= 0>}`` selecting one column of a 2-D
    dataset — the variable analogue of the ``column`` key on structured
    filters, so several scalar columns can be declared against one shared
    dataset path (deduped to a single read). The mapping may instead carry
    ``axes`` (issue #678): an explicit per-dimension declaration flattening a
    fixed-width N-D dataset to the base rate (see :func:`_validate_axes`);
    ``axes`` and ``column`` are mutually exclusive.
    """
    variables = (data_source or {}).get("variables", {})
    if not isinstance(variables, dict):
        raise ValueError("data_source.variables must be a mapping of name -> path")
    for name, entry in variables.items():
        if isinstance(entry, str):
            continue
        where = f"data_source.variables['{name}']"
        if not isinstance(entry, dict):
            raise ValueError(
                f"{where} must be a path string or a {{path, column}} / {{path, axes}} "
                f"mapping (got {type(entry).__name__})"
            )
        # Synthesized-column form (issue #425, vlen grammar): the column is
        # computed per record — never read. Cross-checks against ``levels``
        # and ``coordinates.level`` live in ``_validate_vlen_source``.
        if "synthesize" in entry:
            _validate_synthesize_entry(where, entry)
            continue
        extra = set(entry) - {"path", "column", "axes"}
        if extra:
            raise ValueError(
                f"{where}: unknown keys {sorted(extra)} "
                "(the mapping form takes 'path' plus exactly one of 'column' or 'axes')"
            )
        if not isinstance(entry.get("path"), str):
            raise ValueError(f"{where}.path must be a string")
        if ("column" in entry) == ("axes" in entry):
            raise ValueError(f"{where}: the mapping form takes exactly one of 'column' or 'axes'")
        if "axes" in entry:
            _validate_axes(where, entry["axes"])
            continue
        column = entry.get("column")
        if not isinstance(column, int) or isinstance(column, bool) or column < 0:
            raise ValueError(f"{where}.column must be an integer >= 0 (got {column!r})")


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


def _width_level(data_source: dict) -> str | None:
    """The level whose ``link`` is the fixed-width form (issue #678), or ``None``.

    At most one level may declare ``link.width`` against the base level: it is
    the record level every ``axes`` declaration flattens along, and the read
    route synthesizes exactly one link from it.
    """
    levels = data_source.get("levels")
    base_level = data_source.get("base_level")
    if not isinstance(levels, dict):
        return None
    found = [
        name
        for name, lvl in levels.items()
        if isinstance(lvl, dict)
        and isinstance(lvl.get("link"), dict)
        and "width" in lvl["link"]
        and lvl["link"].get("to") == base_level
    ]
    if len(found) > 1:
        raise ValueError(
            f"levels {found} all declare a 'width' link to base level {base_level!r}; "
            "only one fixed-width record level is allowed"
        )
    return found[0] if found else None


def _validate_base_coordinates(data_source: dict, coord_level, width_level) -> None:
    """``coordinates.latitude``/``longitude`` mapping forms (issue #678).

    Two mapping forms, both coordinates always in the same one:

    - ``{path, axes}`` — the swath form: the base level's coordinates are N-D
      datasets flattened by the ``axes`` rule. Needs a width link and excludes
      ``coordinates.level`` (coordinates either live at the base level or
      expand from a record level — never both). Science variables on this
      form stay plain reads for now: an ``axes`` variable here is rejected
      until the swath read path (the issue #678 follow-up) lands.
    - ``{asset, path}`` — record coordinates joined from a sibling asset
      (GLAH01 carries no 40 Hz geolocation; GLAH05/06 do). Requires
      ``coordinates.level`` (the join is keyed to its records) and a declared
      asset.
    """
    coords = data_source.get("coordinates")
    if not isinstance(coords, dict):
        return
    mapped: dict[str, str] = {}
    for key in ("latitude", "longitude"):
        entry = coords.get(key)
        if not isinstance(entry, dict):
            continue
        where = f"data_source.coordinates.{key}"
        if "asset" in entry:
            if set(entry) != {"asset", "path"} or not all(
                isinstance(entry[k], str) and entry[k] for k in ("asset", "path")
            ):
                raise ValueError(f"{where}: the asset form takes exactly 'asset' and 'path'")
            assets = data_source.get("assets")
            if not isinstance(assets, dict) or entry["asset"] not in assets:
                raise ValueError(
                    f"{where}: asset {entry['asset']!r} is not declared in data_source.assets "
                    f"(available: {sorted(assets) if isinstance(assets, dict) else []})"
                )
            mapped[key] = "asset"
            continue
        extra = set(entry) - {"path", "axes"}
        if extra or not isinstance(entry.get("path"), str) or "axes" not in entry:
            raise ValueError(f"{where}: the mapping form takes exactly 'path' and 'axes'")
        _validate_axes(where, entry["axes"])
        mapped[key] = "axes"
    if not mapped:
        return
    if len(mapped) != 2 or len(set(mapped.values())) != 1:
        raise ValueError(
            "data_source.coordinates: latitude and longitude must both be plain paths "
            "or both the same mapping form ({path, axes} or {asset, path})"
        )
    if set(mapped.values()) == {"asset"}:
        if coord_level is None:
            raise ValueError(
                "data_source.coordinates: {asset, path} coordinates require coordinates.level "
                "(the sibling join is keyed to that level's records)"
            )
        return
    if coord_level is not None:
        raise ValueError(
            "data_source.coordinates: {path, axes} coordinates are base-level and "
            "exclude coordinates.level (record-level expansion)"
        )
    if width_level is None:
        raise ValueError(
            "data_source.coordinates: {path, axes} coordinates require a level whose "
            "link declares 'width' to the base level"
        )


def _validate_join_keys(where: str, join: dict) -> None:
    """``join.left``/``right``: a key path each, or equal-length lists (issue #678)."""
    unknown = set(join) - {"left", "right"}
    if unknown:
        raise ValueError(
            f"{where}: unknown keys {sorted(unknown)} "
            "(takes 'left' and 'right' record-key dataset paths)"
        )
    sides = {}
    for side in ("left", "right"):
        value = join.get(side)
        if isinstance(value, list):
            if not value or not all(isinstance(v, str) and v for v in value):
                raise ValueError(f"{where}.{side} must be a non-empty list of dataset paths")
        elif not isinstance(value, str) or not value:
            raise ValueError(f"{where}.{side} must be a non-empty string")
        sides[side] = value
    if isinstance(sides["left"], list) != isinstance(sides["right"], list):
        raise ValueError(f"{where}: left and right must both be strings or both be lists")
    if isinstance(sides["left"], list) and len(sides["left"]) != len(sides["right"]):
        raise ValueError(
            f"{where}: composite keys need equal-length lists "
            f"(left has {len(sides['left'])}, right has {len(sides['right'])})"
        )


def _validate_vlen_source(data_source: dict) -> None:
    """Cross-validate the vlen grammar surface (issue #425).

    The vlen route is selected by ``coordinates.level`` (the base level has no
    native coordinates; they expand from that record level). Checks:

    - ``coordinates.level`` names a non-base level whose ``link`` points
      directly at ``base_level`` (the ragged-gather link);
    - ``synthesize:`` variables require the vlen route and their ``level``
      must equal ``coordinates.level`` (the within-record index used by the
      synthesis comes from that level's link); ``offsets`` (either form) and
      ``index`` need that link to be the fixed-width form (issue #678);
    - ``axes`` variables (issue #678) require ``coordinates.level`` to be the
      fixed-width (``link.width``) level; base-level ``{path, axes}``
      coordinates (the swath form) need a width link and exclude
      ``coordinates.level``;
    - ``data_source.assets`` entries carry a ``join`` block with
      ``left``/``right`` key paths (a string each, or equal-length lists for
      a composite key); every filter's ``asset`` names a declared entry, and
      asset filters require the vlen route;
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
    width_level = _width_level(ds)
    _validate_base_coordinates(ds, coord_level, width_level)
    for name, entry in (ds.get("variables") or {}).items():
        if not isinstance(entry, dict):
            continue
        where = f"data_source.variables['{name}']"
        if "axes" in entry:
            # Fixed-width flatten (issue #678): the record axis is the
            # coordinates level's, so ``axes`` needs that level's link to be
            # the width form — a ragged link has no constant sample axis.
            lat = coords.get("latitude") if isinstance(coords, dict) else None
            if isinstance(lat, dict) and "axes" in lat:
                raise ValueError(
                    f"{where}: axes variables on the swath form ({{path, axes}} base-level "
                    "coordinates) arrive with the swath read path, the issue #678 "
                    "follow-up; declare a plain path for now"
                )
            if coord_level is None or width_level != coord_level:
                raise ValueError(
                    f"{where}: axes requires coordinates.level to name a level whose "
                    "link declares 'width' (the fixed-width record link)"
                )
            continue
        if "synthesize" not in entry:
            continue
        if coord_level is None:
            raise ValueError(
                f"{where}: synthesize requires the vlen route (set data_source.coordinates.level)"
            )
        if entry["level"] != coord_level:
            raise ValueError(
                f"{where}.level {entry['level']!r} must equal "
                f"coordinates.level {coord_level!r} (the within-record index comes "
                "from that level's link)"
            )
        if entry["synthesize"] in ("offsets", "index") and width_level != coord_level:
            raise ValueError(
                f"{where}: synthesize {entry['synthesize']!r} is a fixed-width form, so "
                f"coordinates.level {coord_level!r} must link with 'width' (the ragged "
                "route synthesizes linspace only)"
            )
    assets = ds.get("assets")
    if assets is not None:
        if not isinstance(assets, dict) or not assets:
            raise ValueError("data_source.assets must be a non-empty mapping of name -> {join}")
        for name, cfg in assets.items():
            if not isinstance(cfg, dict) or not isinstance(cfg.get("join"), dict):
                raise ValueError(f"data_source.assets['{name}'] requires a 'join' mapping")
            _validate_join_keys(f"data_source.assets['{name}'].join", cfg["join"])
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
                if not isinstance(entry, dict) or "asset" not in entry:
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
                if isinstance(tmpl, dict) and "asset" not in tmpl:
                    # Record-level table column (issue #678): ``{path, column}``
                    # selects one column of an ``[n_records, k]`` dataset
                    # (GLAH01 ``i_wt_fact_filt [n, 6]``) — the record-level
                    # twin of the base-level column form, never ``axes``.
                    unknown = set(tmpl) - {"path", "column"}
                    if unknown or "column" not in tmpl:
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}: a mapping is either "
                            f"{{asset, path}} or {{path, column}} (got keys {sorted(tmpl)})"
                        )
                    if not isinstance(tmpl.get("path"), str) or not tmpl["path"]:
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}.path must be a non-empty string"
                        )
                    column = tmpl["column"]
                    if not isinstance(column, int) or isinstance(column, bool) or column < 0:
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}.column must be an integer "
                            f">= 0 (got {column!r})"
                        )
                elif isinstance(tmpl, dict):
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
        if "to" not in link:
            raise ValueError(f"levels.{name}.link: 'to' is required")
        unknown = set(link) - {
            "to",
            "index_beg",
            "count",
            "index_base",
            "reference_index",
            "width",
        }
        if unknown:
            raise ValueError(
                f"levels.{name}.link: unknown fields {sorted(unknown)} "
                f"(allowed: to, index_beg, count, index_base, reference_index, width)"
            )
        if "width" in link:
            # Fixed-width link (issue #678): every record owns exactly ``width``
            # consecutive base rows, so the reader synthesizes ``index_beg =
            # i * width``, ``count = width`` and no link datasets exist to name.
            clash = sorted(set(link) & {"index_beg", "count", "index_base", "reference_index"})
            if clash:
                raise ValueError(
                    f"levels.{name}.link: 'width' is mutually exclusive with {clash} "
                    "(a fixed-width link synthesizes its index_beg/count)"
                )
            if link["to"] != base_level:
                raise ValueError(
                    f"levels.{name}.link: a 'width' link must point at base_level "
                    f"{base_level!r} (got to: {link['to']!r}); its index_beg = i * width "
                    "indexes base rows"
                )
            width = link["width"]
            if width != "auto" and (
                not isinstance(width, int) or isinstance(width, bool) or width < 1
            ):
                raise ValueError(
                    f"levels.{name}.link.width must be an integer >= 1 or 'auto' (got {width!r})"
                )
        else:
            for field_name in ("index_beg", "count"):
                if field_name not in link:
                    raise ValueError(f"levels.{name}.link: '{field_name}' is required")
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
