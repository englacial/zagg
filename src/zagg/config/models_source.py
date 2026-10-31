"""The ``data_source`` grammar (issue #677 phase 1).

Three sources, one per reader family, each a strict model: the HDF5 point
source (:class:`H5DataSource`, the default — ``reader: h5coro``), the STAC
raster source (:class:`RasterDataSource`, ``reader: raster``, issue #218) and
the gridded-collection source of the temporal engine
(:class:`CollectionDataSource`, ``reader: xarray_s3`` or a registered plugin
reader, issue #213). The root union in :mod:`zagg.config.models` picks one
the way ``validate_config`` branches.

Shape is pydantic's job (types, required keys, ``extra="forbid"``); the
cross-field rules the old ``_validate_*`` helpers carried — levels/links,
the vlen route, asset joins, filter levels — live on the models as
validators with their messages intact, so a template author sees the same
refusal with the key's path in front of it.

Every ``Field(description=...)`` is written for a model reader: it is the
documentation the MCP ``build_template`` tool ships, migrated from the
template comments' grammar explanations (product rulings stay in the design
cards).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)

from zagg.config.models_base import (
    Bool,
    NonEmptyStr,
    NonNegInt,
    Number,
    PosInt,
    StrictModel,
    _tag_of,
)

__all__ = [
    "AssetJoin",
    "AssetVariable",
    "Band",
    "CollectionDataSource",
    "CollectionOptions",
    "ExpressionFilter",
    "H5DataSource",
    "Level",
    "Link",
    "PathColumn",
    "QualityFilter",
    "RasterDataSource",
    "ReadPlan",
    "Resample",
    "StructuredFilter",
    "Synthesize",
]

# Structured-predicate comparison operators (issue #43). ``in``/``not_in``
# take a ``values`` list; the rest take a scalar ``value``.
_SCALAR_OPS = ("eq", "ne", "ge", "le", "lt", "gt")
_SET_OPS = ("in", "not_in")
FilterOp = Literal["eq", "ne", "ge", "le", "lt", "gt", "in", "not_in"]


# ---------------------------------------------------------------------------
# data_source.variables entries
# ---------------------------------------------------------------------------


def _check_column(v: Any) -> Any:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise ValueError(f"must be an integer >= 0 (got {v!r})")
    return v


class PathColumn(StrictModel):
    """``{path, column}``: one column of a 2-D dataset (issue #321)."""

    path: str = Field(description="HDF5 dataset path template (may contain ``{group}``).")
    column: Annotated[int, Field(json_schema_extra={"minimum": 0})] = Field(
        description="0-based column selector into the N-D dataset — the variable analogue "
        "of a structured filter's ``column``, so several scalar columns can be declared "
        "against one shared dataset path (deduped to a single read)."
    )

    @field_validator("column", mode="before")
    @classmethod
    def _column(cls, v: Any) -> Any:
        return _check_column(v)


class Synthesize(StrictModel):
    """``{synthesize: linspace, level, start, stop}``: a per-record computed column (issue #425).

    The column is never read: it is ``linspace(start, stop, count)`` over each
    record's samples, where ``start``/``stop`` are record-level datasets on
    ``level`` (which must be ``coordinates.level``, the vlen record level).
    """

    synthesize: Literal["linspace"] = Field(
        description="The synthesis kind; ``linspace`` is the only one."
    )
    level: NonEmptyStr = Field(
        description="The record level the bounds are read at; must equal ``coordinates.level``."
    )
    start: NonEmptyStr = Field(description="Dataset path template of the per-record start value.")
    stop: NonEmptyStr = Field(description="Dataset path template of the per-record stop value.")


def _variable_tag(v: Any) -> str:
    def by_shape(raw: Any) -> str:
        if isinstance(raw, str):
            return "<path-form>"
        if isinstance(raw, dict) and "synthesize" in raw:
            return "<synthesize-form>"
        return "<column-form>"

    return _tag_of(v, {PathColumn: "<column-form>", Synthesize: "<synthesize-form>"}, by_shape)


#: A base-level variable: a path-template string (reads the whole dataset),
#: the ``{path, column}`` column-selector mapping, or the ``synthesize`` form.
VariableEntry = Annotated[
    Union[
        Annotated[str, Tag("<path-form>")],
        Annotated[PathColumn, Tag("<column-form>")],
        Annotated[Synthesize, Tag("<synthesize-form>")],
    ],
    Discriminator(_variable_tag),
]


# ---------------------------------------------------------------------------
# Filters (issue #43): structured predicates and the expression escape hatch
# ---------------------------------------------------------------------------


class QualityFilter(StrictModel):
    """The flat ``quality_filter: {dataset, value}`` sugar: one base-level ``op: eq`` filter.

    Only ``dataset`` and ``value`` are honored; use the structured ``filters:``
    list to set ``op``, ``column``, ``keep`` and the rest.
    """

    dataset: str = Field(description="Dataset path template the equality is read from.")
    value: Any = Field(description="The value rows must equal to be kept.")


class StructuredFilter(StrictModel):
    """``{level?, asset?, dataset, column?, op, value|values, keep?}`` — a pushdown-eligible predicate.

    The only filter language the planner can inspect (read pushdown, issue #43
    phase C). ``in``/``not_in`` take a ``values`` list; the other ops a scalar
    ``value``. ``column`` selects one column of an N-D flag array (its rank is
    checked at read time, not here).
    """

    dataset: str = Field(description="Dataset path template the predicate reads.")
    op: FilterOp = Field(
        description="Comparison operator: ``eq``/``ne``/``ge``/``le``/``lt``/``gt`` take "
        "``value``; ``in``/``not_in`` take ``values``."
    )
    value: Number | None = Field(default=None, description="Scalar operand for the scalar ops.")
    values: list[Number] | None = Field(
        default=None, description="Operand list for ``in``/``not_in``."
    )
    column: Annotated[int, Field(json_schema_extra={"minimum": 0})] | None = Field(
        default=None,
        description="Column selector into an N-D flag dataset (e.g. ATL03 "
        "``signal_conf_ph``); omit for a 1-D dataset.",
    )
    level: str | None = Field(
        default=None,
        description="The ``levels`` key the predicate is evaluated at; absent/null is the base "
        "level. A non-base predicate keeps or drops whole parent records.",
    )
    asset: str | None = Field(
        default=None,
        description="Sibling-asset predicate (issue #425): evaluated on the named "
        "``data_source.assets`` entry's rows after the record-key join; record-level by "
        "construction, so ``level`` must be omitted.",
    )
    keep: Bool = Field(
        default=True,
        description="``true`` (default) keeps matching rows; ``false`` drops them.",
    )

    @field_validator("column", mode="before")
    @classmethod
    def _column(cls, v: Any) -> Any:
        if v is None:
            return v
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError(f"'column' must be an integer index (got {v!r})")
        return v

    @model_validator(mode="after")
    def _operands(self) -> StructuredFilter:
        if self.asset is not None:
            if not self.asset:
                raise ValueError("'asset' must be a non-empty string")
            if self.level is not None:
                raise ValueError(
                    "'asset' filters are record-level by construction "
                    "(the join key aligns records); omit 'level'"
                )
        if self.op in _SET_OPS:
            if "values" not in self.model_fields_set or self.values is None:
                raise ValueError(f"op {self.op!r} requires a 'values' list")
        elif "value" not in self.model_fields_set:
            raise ValueError(f"op {self.op!r} requires a scalar 'value'")
        elif self.value is None:
            raise ValueError("'value' must be numeric (got None)")
        return self


class ExpressionFilter(StrictModel):
    """``{expression}`` — a base-level, aggregation-time row predicate that forfeits pushdown.

    Evaluated over the read columns (``data_source.variables`` plus broadcast
    segment-level variables) with the restricted numpy namespace; opaque to the
    planner, so it never reduces bytes read.
    """

    expression: str = Field(description="Python boolean expression over column names and ``np``.")
    level: None = Field(
        default=None, description="Must be omitted or null: expression filters are base-level only."
    )

    @model_validator(mode="before")
    @classmethod
    def _shape(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "op" in data or "dataset" in data:
                raise ValueError(
                    "'expression' filters take no 'op'/'dataset' "
                    "(base-level aggregation-time escape hatch, no pushdown)"
                )
            if data.get("level") is not None:
                raise ValueError("'expression' filters are base-level only (level must be omitted)")
        return data


def _filter_tag(v: Any) -> str:
    def by_shape(raw: Any) -> str:
        return (
            "<expression-filter>"
            if isinstance(raw, dict) and "expression" in raw
            else "<structured-filter>"
        )

    return _tag_of(
        v,
        {ExpressionFilter: "<expression-filter>", StructuredFilter: "<structured-filter>"},
        by_shape,
    )


Filter = Annotated[
    Union[
        Annotated[StructuredFilter, Tag("<structured-filter>")],
        Annotated[ExpressionFilter, Tag("<expression-filter>")],
    ],
    Discriminator(_filter_tag),
]


# ---------------------------------------------------------------------------
# Hierarchical levels (issue #43 phase B), segment variables (#30), assets (#464)
# ---------------------------------------------------------------------------


class Link(StrictModel):
    """Per-level link to the next coarser level: a contiguous-range parent→child tiling.

    Parent segment ``p`` covers base-rate indices ``[index_beg[p] - index_base, ...)``
    for ``count[p]`` children. ``reference_index`` is a reserved slot for a
    future explicit-index-array variant and must stay null.
    """

    to: str = Field(description="Key of the coarser level in ``levels``.")
    index_beg: str = Field(description="HDF5 path template of the per-parent start-index array.")
    count: str = Field(description="HDF5 path template of the per-parent child-count array.")
    index_base: NonNegInt = Field(
        default=0,
        description="Subtracted from ``index_beg`` so 0-based indexing into the base array is "
        "direct (ATL03 ``ph_index_beg`` is 1-based: ``index_base: 1``).",
    )
    reference_index: None = Field(
        default=None,
        description="Reserved; must be null (explicit index arrays are not implemented).",
    )

    @field_validator("reference_index", mode="before")
    @classmethod
    def _reserved(cls, v: Any) -> Any:
        if v is not None:
            raise ValueError(
                "is reserved and must be null/omitted (explicit index-array variant not yet implemented)"
            )
        return v


class AssetVariable(StrictModel):
    """``{asset, path}``: a record-level variable read from a sibling asset (issue #464).

    The dataset is read from the named ``data_source.assets`` entry's handle
    and joined per record on that asset's declared ``join`` key; 1-D datasets
    only (no ``column`` selector yet — richer companions ride issue #465).
    """

    asset: NonEmptyStr = Field(description="Name of the ``data_source.assets`` entry to read from.")
    path: NonEmptyStr = Field(description="Dataset path template inside the sibling asset.")

    @model_validator(mode="before")
    @classmethod
    def _keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            unknown = set(data) - {"asset", "path"}
            if unknown:
                hint = (
                    "; the asset form reads 1-D datasets only, with no 'column' selector yet "
                    "(refs issue #465)"
                    if "column" in unknown
                    else ""
                )
                raise ValueError(
                    f"unknown keys {sorted(unknown)} (the asset form takes 'asset' and 'path'){hint}"
                )
        return data


def _level_variable_tag(v: Any) -> str:
    return _tag_of(
        v,
        {AssetVariable: "<asset-form>"},
        lambda r: "<path-form>" if isinstance(r, str) else "<asset-form>",
    )


LevelVariable = Annotated[
    Union[
        Annotated[NonEmptyStr, Tag("<path-form>")],
        Annotated[AssetVariable, Tag("<asset-form>")],
    ],
    Discriminator(_level_variable_tag),
]


def _list_or_mapping_tag(v: Any) -> str:
    return "<list-form>" if isinstance(v, list) else "<mapping-form>"


LevelCoordinates = Annotated[
    Union[
        Annotated[list[str], Tag("<list-form>")],
        Annotated[dict[str, str], Tag("<mapping-form>")],
    ],
    Discriminator(_list_or_mapping_tag),
]

LevelVariables = Annotated[
    Union[
        Annotated[list[str], Tag("<list-form>")],
        Annotated[dict[NonEmptyStr, LevelVariable], Tag("<mapping-form>")],
    ],
    Discriminator(_list_or_mapping_tag),
]


class Level(StrictModel):
    """One rate of a multi-rate HDF5 source (e.g. ATL03 ``photons`` and ``segments``).

    Each level declares its own group ``path``, optional ``coordinates``, and
    ``variables``, plus a ``link`` to its coarser parent (the base level has
    none). ``variables`` is either a documentation-only list of names or, on a
    non-base level, a ``{name: path-template}`` mapping declaring a readable
    segment-level variable broadcast to the base rows at read time (issue #30)
    — or an ``{asset, path}`` entry sourced from a sibling asset (issue #464).
    """

    path: str = Field(description="HDF5 group path template (may contain ``{group}``).")
    coordinates: LevelCoordinates | None = Field(
        default=None,
        description="Coordinate dataset names within ``path``: a list, or a ``{latitude, "
        "longitude}`` mapping of dataset names.",
    )
    variables: LevelVariables | None = Field(
        default=None,
        description="A documentation-only list of names, or (non-base levels) a mapping of "
        "readable segment-level variables: ``name: path-template`` or ``name: {asset, path}``.",
    )
    link: Link | None = Field(
        default=None,
        description="Link to the coarser level; required on every non-base level, null on the base.",
    )


class AssetJoin(StrictModel):
    """``{left, right}``: the record-key datasets a sibling asset is joined on."""

    left: NonEmptyStr = Field(description="Record-key dataset path template on the primary source.")
    right: NonEmptyStr = Field(description="Record-key dataset path template on the sibling asset.")


class Asset(StrictModel):
    """A sibling asset (e.g. GEDI L2A beside L1B) paired by record key (issue #425)."""

    join: AssetJoin = Field(description="The record-key join aligning the asset's rows.")


class ChunkBoundaries(StrictModel):
    """The a-priori chunk-boundary arm (issue #148 arm 2a): boundary parquet per granule."""

    prefix: str = Field(description="s3:// or local directory prefix holding the boundary parquet.")
    samples_per_chunk: PosInt | None = Field(
        default=None, description="Override of the samples-per-chunk the boundaries were cut at."
    )


class ReadPlan(StrictModel):
    """How a shard's byte-range reads are planned over the levels (issue #43 phase C)."""

    spatial_index: str | None = Field(
        default=None,
        description="The ``levels`` key whose coordinates select which parent records a shard "
        "needs (e.g. ``segments``); absent is the full-read route.",
    )
    pad: NonNegInt = Field(
        default=1,
        description="Parent records of padding on each side of the spatial selection.",
    )
    full_read_threshold: Number = Field(
        default=0.9,
        description="Fraction of a group's records above which the planner reads it whole.",
    )
    chunk_boundaries: ChunkBoundaries | None = Field(
        default=None,
        description="The a-priori chunk-boundary arm; mutually exclusive with the ``inline`` index.",
    )


# ---------------------------------------------------------------------------
# The HDF5 point source
# ---------------------------------------------------------------------------


class H5DataSource(StrictModel):
    """The HDF5 point-cloud source read through h5coro (``reader: h5coro``, the default).

    Flat form: ``groups`` × ``coordinates``/``variables`` path templates plus
    filters. Hierarchical form (issue #43 phase B): ``levels`` + ``base_level``
    declare the rates and links; the flat keys still describe the base level.
    Vlen form (issue #425): ``coordinates.level`` names the record level the
    per-sample coordinates expand from.
    """

    reader: str | None = Field(
        default=None,
        description="Reader name; ``h5coro`` (default). ``raster`` selects the raster source "
        "grammar; the collection grammar is selected by ``pipeline.type: temporal|event``, "
        "not by this key.",
    )
    driver: str | None = Field(
        default=None, description="Granule access driver, ``s3`` (default) or ``https``."
    )
    groups: list[str] | None = Field(
        default=None,
        description="HDF5 group names substituted for ``{group}`` in every path template "
        "(ICESat-2 beams: ``gt1l`` … ``gt3r``).",
    )
    coordinates: dict[str, str] | None = Field(
        default=None,
        description="``{latitude, longitude}`` dataset path templates, plus the reserved "
        "``level`` key naming the vlen record level the coordinates expand from (issue #425).",
    )
    variables: dict[str, VariableEntry] | None = Field(
        default=None,
        description="Base-level columns: ``name: path-template``, ``name: {path, column}``, or "
        "``name: {synthesize: linspace, level, start, stop}``.",
    )
    quality_filter: QualityFilter | None = Field(
        default=None,
        description="Legacy ``{dataset, value}`` sugar for one base-level ``eq`` filter.",
    )
    filters: list[Filter] | None = Field(
        default=None,
        description="Ordered row predicates: structured ``{dataset, op, value|values, ...}`` "
        "entries (pushdown-eligible) or ``{expression}`` escape hatches. When present the "
        "flat ``quality_filter`` is ignored.",
    )
    levels: dict[str, Level] | None = Field(
        default=None,
        description="The hierarchical rates of the source, keyed by level name (issue #43 phase B).",
    )
    base_level: str | None = Field(
        default=None, description="The ``levels`` key of the finest rate; required with ``levels``."
    )
    index: dict[str, Any] | None = Field(
        default=None,
        description="Virtual chunk-index backend block (issue #160): ``backend`` names a "
        "registered backend (``hierarchical``, ``inline``, or an entry-point backend such as "
        "``sidecar``); the remaining keys are backend-specific. Absent resolves to ``inline`` "
        "(the compiled decode, issue #170), except under ``read_plan.chunk_boundaries``, "
        "where it is ``hierarchical``; ``{backend: hierarchical}`` pins the pure-h5coro walk.",
    )
    read_plan: ReadPlan | None = Field(
        default=None, description="Planned-read knobs: the spatial index level and its padding."
    )
    credentials_provider: str | None = Field(
        default=None,
        description="Credential-provider registry name for source S3 reads (``nsidc``, "
        "``gesdisc``, ``lpdaac``, or a plugin); absent is the spatial default (NSIDC).",
    )
    shard_workers: PosInt | None = Field(
        default=None,
        description="Source units (granules) in flight per shard (issue #232); default 4.",
    )
    granule_workers: PosInt | None = Field(
        default=None,
        description="Legacy spelling of ``shard_workers`` (issue #180); canonical wins.",
    )
    read_workers: PosInt | None = Field(
        default=None, description="Per-granule read fan-out width (issue #170); default 8."
    )
    assets: dict[str, Asset] | None = Field(
        default=None,
        description="Sibling assets paired by record key (issue #425), e.g. ``l2a: {join: "
        "{left, right}}``.",
    )

    @field_validator("levels")
    @classmethod
    def _levels_nonempty(cls, v: dict[str, Level] | None) -> dict[str, Level] | None:
        if v is not None and not v:
            raise ValueError("must be a non-empty mapping")
        return v

    @field_validator("assets")
    @classmethod
    def _assets_nonempty(cls, v: dict[str, Asset] | None) -> dict[str, Asset] | None:
        if v is not None and not v:
            raise ValueError("must be a non-empty mapping of name -> {join}")
        return v

    @model_validator(mode="after")
    def _cross_checks(self) -> H5DataSource:
        self._check_levels()
        self._check_filter_levels()
        self._check_vlen()
        self._check_index()
        return self

    # -- levels / links (was ``_validate_overviews``) --------------------------

    def _check_levels(self) -> None:
        levels = self.levels
        if levels is None:
            return
        base_level = self.base_level
        if base_level is None:
            raise ValueError("data_source.base_level is required when levels is present")
        if base_level not in levels:
            raise ValueError(
                f"data_source.base_level {base_level!r} is not a key in levels "
                f"(available: {sorted(levels)})"
            )
        base_vars = set(self.variables or {})
        seen: set[str] = set()
        for name, lvl in levels.items():
            if isinstance(lvl.variables, dict):
                if name == base_level:
                    raise ValueError(
                        f"levels.{base_level}: the base level uses data_source.variables; "
                        f"a non-base level uses the 'variables' mapping for segment-level reads"
                    )
                for var_name in lvl.variables:
                    if var_name in base_vars:
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}: collides with a "
                            f"data_source.variables column"
                        )
                    # Two non-base levels declaring the same name would silently
                    # overwrite each other when broadcast into one per-photon
                    # column (the read keys by name); reject the ambiguity.
                    if var_name in seen:
                        raise ValueError(
                            f"levels.{name}.variables.{var_name}: a segment-level "
                            f"variable named {var_name!r} is already declared on another level"
                        )
                    seen.add(var_name)
            link = lvl.link
            if link is None:
                if name != base_level:
                    raise ValueError(
                        f"levels.{name}: non-base levels must have a 'link' "
                        f"(only {base_level!r} may omit it)"
                    )
                continue
            if link.to == name:
                raise ValueError(f"level '{name}': link.to cannot reference the level itself")
            if link.to not in levels:
                raise ValueError(
                    f"levels.{name}.link.to {link.to!r} is not a key in levels "
                    f"(available: {sorted(levels)})"
                )

    # -- filter levels (was ``_validate_filter_levels``) ----------------------

    def _check_filter_levels(self) -> None:
        if self.levels is None:
            return
        for i, f in enumerate(self.filters or []):
            lvl = f.level
            if lvl is not None and lvl not in self.levels:
                raise ValueError(
                    f"filter[{i}]: level {lvl!r} is not a key in levels "
                    f"(available: {sorted(self.levels)})"
                )

    # -- the vlen grammar surface (was ``_validate_vlen_source``) -------------

    def _check_vlen(self) -> None:
        coord_level = (self.coordinates or {}).get("level")
        levels = self.levels
        base_level = self.base_level
        if coord_level is not None:
            if levels is None or base_level is None:
                raise ValueError("data_source.coordinates.level requires 'levels' and 'base_level'")
            if coord_level not in levels:
                raise ValueError(
                    f"data_source.coordinates.level {coord_level!r} is not a key in levels "
                    f"(available: {sorted(levels)})"
                )
            link = levels[coord_level].link
            if link is None or link.to != base_level:
                raise ValueError(
                    f"data_source.coordinates.level {coord_level!r} must link directly to "
                    f"base level {base_level!r}"
                )
        for name, entry in (self.variables or {}).items():
            if not isinstance(entry, Synthesize):
                continue
            if coord_level is None:
                raise ValueError(
                    f"data_source.variables['{name}']: synthesize requires the vlen route "
                    "(set data_source.coordinates.level)"
                )
            if entry.level != coord_level:
                raise ValueError(
                    f"data_source.variables['{name}'].level {entry.level!r} must equal "
                    f"coordinates.level {coord_level!r} (the within-record index comes "
                    "from that level's link)"
                )
        assets = self.assets
        for i, f in enumerate(self.filters or []):
            asset = getattr(f, "asset", None)
            if asset is None:
                continue
            if assets is None or asset not in assets:
                raise ValueError(
                    f"filter[{i}]: asset {asset!r} is not declared in data_source.assets "
                    f"(available: {sorted(assets) if assets else []})"
                )
            if coord_level is None:
                raise ValueError(
                    f"filter[{i}]: 'asset' filters require the vlen route "
                    "(set data_source.coordinates.level)"
                )
        # Asset-sourced level variables (issue #464): record-level by
        # construction, so they live on ``coordinates.level`` only.
        for lvl_name, lvl in (levels or {}).items():
            if not isinstance(lvl.variables, dict):
                continue
            for var_name, lvl_entry in lvl.variables.items():
                if not isinstance(lvl_entry, AssetVariable):
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
                if assets is None or lvl_entry.asset not in assets:
                    raise ValueError(
                        f"{where}: asset {lvl_entry.asset!r} is not declared in data_source.assets "
                        f"(available: {sorted(assets) if assets else []})"
                    )

    # -- the chunk-index block (was ``_validate_index``) ----------------------

    def _check_index(self) -> None:
        """Delegate to :func:`zagg.index.validate_index_config` (lazy import — the
        backend registry knows each backend's accepted keys, entry points included)."""
        if self.index is None:
            return
        from zagg.index import validate_index_config

        validate_index_config(self.index, self.model_dump(exclude_unset=True))

    @property
    def segment_variable_names(self) -> set[str]:
        """Names of readable segment-level (non-base) variables (issue #30)."""
        names: set[str] = set()
        if self.levels is None or self.base_level is None:
            return names
        for name, lvl in self.levels.items():
            if name != self.base_level and isinstance(lvl.variables, dict):
                names |= set(lvl.variables)
        return names


# ---------------------------------------------------------------------------
# The raster source (issue #218)
# ---------------------------------------------------------------------------


class Band(StrictModel):
    """One output field sampled pull-NN from a STAC asset's GeoTIFF."""

    asset: NonEmptyStr = Field(description="STAC asset key holding this band's GeoTIFF/COG.")
    dtype: NonEmptyStr = Field(
        description="Stored dtype — bands keep exact source DNs, so there is no float default."
    )
    fill_value: Number | None = Field(
        default=None, description="Fill for unsampled cells; default 0."
    )
    scale: Number | None = Field(
        default=None,
        description="Recorded as the ``scale_factor`` array attr for readers; never applied.",
    )
    offset: Number | None = Field(
        default=None,
        description="Recorded as the ``add_offset`` array attr for readers; never applied.",
    )

    @model_validator(mode="before")
    @classmethod
    def _no_null_numbers(cls, data: Any) -> Any:
        # Optional by absence only: an explicit null would be forwarded as a
        # None fill or a None scale_factor/add_offset attr.
        if isinstance(data, dict):
            for key in ("fill_value", "scale", "offset"):
                if key in data and data[key] is None:
                    raise ValueError(f"{key} must be a number (got None)")
        return data


class RasterDataSource(StrictModel):
    """The STAC raster source: ``reader: raster`` (issue #218).

    Pull-NN yields exactly one value per cell per timestep, so the aggregation
    block is replaced by a declarative band map.
    """

    reader: Literal["raster"] = Field(description="Selects the raster (pull-NN) pipeline.")
    bands: dict[str, Band] = Field(
        description="Output field name → ``{asset, dtype, fill_value?, scale?, offset?}``."
    )
    nodata: Number | None = Field(
        default=None,
        description="Source nodata scalar: a cell whose sampled pixel equals it in any band "
        "is left at fill.",
    )
    anonymous: Bool | None = Field(
        default=None, description="Open the source store without credentials (default true)."
    )
    source_region: str | None = Field(default=None, description="AWS region of the source store.")
    credentials_provider: str | None = Field(
        default=None, description="Credential-provider registry name for source reads."
    )
    shard_workers: PosInt | None = Field(
        default=None,
        description="Acquisition groups (timesteps) sampling concurrently per shard; default 4, "
        "1 is serial (issues #231/#232).",
    )
    write_buffer: PosInt | None = Field(
        default=None,
        description="Streamed-write slab budget: 1 (default) is the strict serial bound; N "
        "overlaps up to N-1 slab writes with sampling.",
    )

    @field_validator("bands")
    @classmethod
    def _bands_nonempty(cls, v: dict[str, Band]) -> dict[str, Band]:
        if not v:
            raise ValueError("raster pipeline requires data_source.bands (field -> {asset, dtype})")
        return v


# ---------------------------------------------------------------------------
# The gridded-collection source of the temporal engine (issue #213)
# ---------------------------------------------------------------------------


class Resample(StrictModel):
    """``{freq, how?, scale?}``: resample a collection onto a coarser cadence."""

    freq: str = Field(description="pandas frequency string, e.g. ``3h``.")
    how: Literal["sum", "mean"] = Field(default="sum", description="The resampling reducer.")
    scale: Number = Field(default=1, description="Multiplier applied after resampling.")

    @field_validator("freq")
    @classmethod
    def _freq(cls, v: str) -> str:
        import pandas as pd

        try:
            pd.tseries.frequencies.to_offset(v)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"resample.freq is not a valid pandas frequency string (got {v!r})"
            ) from exc
        return v


class CollectionOptions(BaseModel):
    """Per-collection reader options consumed by :func:`zagg.temporal.prepare_collection`.

    Unknown keys pass through by design (e.g. ``doi``): catalog tooling
    metadata rides beside the reader options.
    """

    model_config = ConfigDict(extra="allow")

    coord_round: NonNegInt | None = Field(
        default=None,
        description="Decimals to round lat/lon coordinates to, for source grids whose "
        "coordinate arrays carry float dirt.",
    )
    variables: list[str] | None = Field(
        default=None, description="Variables to load from the collection (default: all)."
    )
    time_offset: str | None = Field(
        default=None, description="pandas offset string shifting the time axis, e.g. ``-30min``."
    )
    resample: Resample | None = Field(default=None, description="Resample the collection.")
    derived: dict[str, str] | None = Field(
        default=None, description="Derived variables: name → numpy expression string."
    )

    @field_validator("time_offset")
    @classmethod
    def _offset(cls, v: str | None) -> str | None:
        if v is None:
            return v
        import pandas as pd

        try:
            pd.to_timedelta(v)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"time_offset is not a valid pandas offset string (got {v!r})"
            ) from exc
        return v


def _collections_tag(v: Any) -> str:
    return "<list-form>" if isinstance(v, list) else "<mapping-form>"


Collections = Annotated[
    Union[
        Annotated[list[str], Tag("<list-form>")],
        Annotated[dict[str, CollectionOptions | None], Tag("<mapping-form>")],
    ],
    Discriminator(_collections_tag),
]


class CollectionDataSource(StrictModel):
    """The gridded-collection source of the temporal/event engine (issue #12, #213).

    ``reader`` names a registered reader (``xarray_s3`` built in; plugins
    register others) resolved at run time, never here.
    """

    reader: str | None = Field(
        default=None, description="Registered reader name; absent is ``xarray_s3``."
    )
    collections: Collections | None = Field(
        default=None,
        description="A list of collection names, or a mapping name → per-collection options "
        "(``null`` for none).",
    )
    static_data: dict[str, str] | None = Field(
        default=None, description="Static inputs by name → URI (masks, climatologies)."
    )
    credentials_provider: str | None = Field(
        default=None,
        description="Credential-provider registry name for source reads (``gesdisc`` for "
        "MERRA-2); per-event ``s3_credentials`` win.",
    )
