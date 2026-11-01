"""The ``output`` grammar (issue #677 phase 3).

:class:`Output` is the store side of a spatial or raster pipeline — the grid
(a ``type``-tagged union), the store layout and its hive-only companions
(coverage MOC, sweep, leaf versions, Icechunk), the overview pyramid, the
temporal windowing and the per-observation clock; :class:`TemporalOutput`
the much smaller tabular sink of the temporal engine.

Shape is pydantic's; the rules that span keys INSIDE the block (hive requires
HEALPix, the hive-only knobs, pyramid orders against the grid, windowing
against the layout) are validators here with their messages intact. Rules
that reach into another block — a clock column against the read variables,
``summarize`` against the aggregation fields, the raster kind's couplings —
stay in :func:`zagg.config.validate_config` and the per-kind root models.
"""

from __future__ import annotations

import importlib
import logging
from datetime import date
from typing import Annotated, Any, Literal, Union

from pydantic import (
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    PlainValidator,
    Tag,
    WithJsonSchema,
    field_validator,
    model_validator,
)

from zagg.config.accessors import SWEEP_MODES, TOUCH_POLICIES
from zagg.config.models_base import (
    Bool,
    NonNegInt,
    Number,
    PosInt,
    StrictModel,
    _Block,
    _tag_of,
)

logger = logging.getLogger(__name__)

__all__ = [
    "Grid",
    "HealpixGrid",
    "Icechunk",
    "Output",
    "Pyramid",
    "RectilinearGrid",
    "TemporalOutput",
    "TimeSource",
    "Windowing",
]

#: ``output.windowing.unit`` (``windowing.WINDOWING_UNITS``).
WindowingUnit = Literal["shard", "window"]


class _Enum:
    """The closed vocabulary a ``str`` knob exports, read off its owning constant.

    Resolved at schema time, so this package keeps importing no other zagg
    module at import time (``sweep_overview`` imports :mod:`zagg.grids`, which
    imports this package); the knob's validator still words the refusal.
    """

    def __init__(self, module: str, name: str):
        self.module, self.name = module, name

    def __get_pydantic_json_schema__(self, core_schema: Any, handler: Any) -> dict:
        return {
            "type": "string",
            "enum": list(getattr(importlib.import_module(self.module), self.name)),
        }


def _check_instant(v: Any) -> Any:
    # Checked, never converted: the dump must stay the author's value (a YAML
    # ``date`` is not rewritten as a midnight ``datetime``), and pydantic's lax
    # ``datetime`` would admit an int as a Unix timestamp. ``parse_utc`` runs
    # in the ``after`` validators.
    if not isinstance(v, (str, date)):  # a ``datetime`` is a ``date``
        raise ValueError(f"must be an ISO-8601 instant string (got {v!r})")
    return v


#: An ISO-8601 instant as written, or the ``date``/``datetime`` YAML already typed it as.
Instant = Annotated[
    Any, PlainValidator(_check_instant), WithJsonSchema({"type": "string", "format": "date-time"})
]


# ---------------------------------------------------------------------------
# output.grid
# ---------------------------------------------------------------------------


class HealpixGrid(_Block):
    """A HEALPix NESTED grid: cells at ``child_order``, shards at ``parent_order``."""

    #: ``null`` is the documented spelling of both (the one layout; retired).
    _NULLABLE = frozenset({"layout", "cell_ids_encoding"})

    type: Literal["healpix"] = Field(description="Grid family.")
    parent_order: NonNegInt = Field(
        description="Shard / dispatch order: one worker per order-``parent_order`` cell."
    )
    child_order: NonNegInt = Field(description="Leaf cell resolution (order of the output cells).")
    chunk_inner: NonNegInt | None = Field(
        default=None,
        description="Inner zarr chunk order between ``parent_order`` and ``child_order`` "
        "(K = 4^(chunk_inner - parent_order) chunks per shard); absent derives "
        "``child_order - 6`` (64×64-cell inner chunks, issue #259) when sharded and that lies "
        "above ``parent_order``, else one chunk per shard (always so for raster).",
    )
    sharded: Bool | None = Field(
        default=None,
        description="Bundle a shard's K inner chunks into one ShardingCodec object (issue "
        "#108); HEALPix output defaults to true (issue #215).",
    )
    layout: Literal["fullsphere"] | None = Field(
        default=None,
        description="HEALPix array layout; ``fullsphere`` is the only one (the deprecated "
        "``dense`` pack was removed, issue #88).",
    )
    emit_cell_ids: Bool | None = Field(
        default=None,
        description="Transition hatch (issue #304): also write the legacy NESTED ``cell_ids`` "
        "array beside the ``morton`` coordinate. Default off.",
    )
    indexing_scheme: Literal["nested"] | None = Field(
        default=None,
        description="Descriptive only (the shipped templates spell ``nested``); the stored "
        "coordinate is morton (D16, issue #304).",
    )
    cell_ids_encoding: None = Field(
        default=None,
        description="Retired (issue #304): morton is the stored cell coordinate and NESTED ids "
        "are derived at read. Must be absent or null.",
    )

    @field_validator("layout", mode="before")
    @classmethod
    def _layout(cls, v: Any) -> Any:
        if v is not None and v != "fullsphere":
            raise ValueError(
                f"must be 'fullsphere' (got {v!r}; the deprecated 'dense' layout was removed "
                f"— issue #88)"
            )
        return v

    @field_validator("emit_cell_ids", mode="before")
    @classmethod
    def _emit(cls, v: Any) -> Any:
        if v is not None and not isinstance(v, bool):
            raise ValueError(f"must be a boolean (got {v!r})")
        return v

    @field_validator("indexing_scheme", mode="before")
    @classmethod
    def _scheme(cls, v: Any) -> Any:
        if v is not None and v != "nested":
            raise ValueError(
                f"is descriptive and must be 'nested' (got {v!r}); "
                f"the store's declared coordinate is morton (D16 — issue #304)"
            )
        return v

    @field_validator("cell_ids_encoding", mode="before")
    @classmethod
    def _retired(cls, v: Any) -> Any:
        if v is not None:
            raise ValueError(
                "retired (issue #304): morton is the stored "
                "cell coordinate and NESTED ids are derived at read; to keep writing the legacy "
                "NESTED cell_ids array for transition stores set output.grid.emit_cell_ids: true"
            )
        return v


class RectilinearGrid(_Block):
    """A projected rectilinear grid: ``crs`` + ``resolution`` + ``bounds`` fix the cell geometry."""

    type: Literal["rectilinear"] = Field(description="Grid family.")
    crs: str = Field(description="Coordinate reference system, e.g. ``EPSG:3031``.")
    resolution: Number = Field(description="Cell size in CRS units.")
    bounds: list[Number] = Field(description="``[xmin, ymin, xmax, ymax]`` in CRS units.")
    chunk_shape: list[PosInt] | None = Field(
        default=None, description="Shard tile ``[rows, cols]`` in cells (default ``[256, 256]``)."
    )
    chunk_inner: list[PosInt] | None = Field(
        default=None,
        description="Inner zarr chunk ``[rows, cols]`` within the shard tile (issue #30 item 3); "
        "default the shard tile.",
    )
    sharded: Bool | None = Field(
        default=None, description="ShardingCodec bundling of the inner chunks; rect defaults off."
    )
    emit_cell_ids: Bool | None = Field(
        default=None, description="Not applicable: rectilinear grids have no cell_ids coordinate."
    )

    @field_validator("bounds")
    @classmethod
    def _bounds(cls, v: list) -> list:
        if len(v) != 4:
            raise ValueError("must be [xmin, ymin, xmax, ymax]")
        return v

    @field_validator("emit_cell_ids", mode="before")
    @classmethod
    def _emit(cls, v: Any) -> Any:
        if v is not None and not isinstance(v, bool):
            raise ValueError(f"must be a boolean (got {v!r})")
        if v:
            raise ValueError("only applies to healpix grids (grid type is 'rectilinear')")
        return v


def _grid_tag(v: Any) -> str:
    def by_shape(raw: Any) -> str:
        t = raw.get("type") if isinstance(raw, dict) else None
        return "<rectilinear-grid>" if t == "rectilinear" else "<healpix-grid>"

    return _tag_of(
        v, {HealpixGrid: "<healpix-grid>", RectilinearGrid: "<rectilinear-grid>"}, by_shape
    )


#: ``output.grid``, tagged by ``type``; a block without ``type`` is refused by
#: the HEALPix member (``output.grid.type: Field required``).
Grid = Annotated[
    Union[
        Annotated[HealpixGrid, Tag("<healpix-grid>")],
        Annotated[RectilinearGrid, Tag("<rectilinear-grid>")],
    ],
    Discriminator(_grid_tag),
]


# ---------------------------------------------------------------------------
# output.windowing (issue #246) — explicit window entries (issue #355)
# ---------------------------------------------------------------------------


class WindowRange(StrictModel):
    """``{label, start, end}``: a half-open ``[start, end)`` window."""

    label: Any = Field(description="Window label in the explicit grammar (``[A-Za-z0-9-]+``).")
    start: Instant = Field(description="ISO-8601 UTC start (inclusive).")
    end: Instant = Field(description="ISO-8601 UTC end (exclusive).")

    @model_validator(mode="before")
    @classmethod
    def _keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # repr, not str: YAML 1.1 resolves bare `on`/`no`/numerals to
            # bool/int keys, which are neither sortable against strings nor
            # joinable (PR #367).
            extra = sorted(set(data) - {"label", "start", "end"}, key=repr)
            if extra:
                raise ValueError(
                    f"explicit range window entry carries unsupported key(s) "
                    f"{', '.join(map(repr, extra))}; the range form is "
                    f"{{label, start, end}} (got {data!r})"
                )
        return data


class WindowPoint(StrictModel):
    """``{label, timestamp}``: sugar for the one-second window ``[t, t + 1s)``."""

    label: Any = Field(description="Window label in the explicit grammar.")
    timestamp: Instant = Field(description="ISO-8601 UTC instant the window starts at.")

    @model_validator(mode="before")
    @classmethod
    def _keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            ranged = sorted({"start", "end"} & set(data))
            if ranged:
                raise ValueError(
                    f"explicit window entry carries both timestamp and {', '.join(ranged)}; "
                    f"each entry declares exactly one of {{timestamp}} or {{start, end}} "
                    f"(got {data!r})"
                )
            # `width` is documented as the future escape hatch but does not
            # exist yet: accepting it silently would hand back a 1 s window.
            extra = sorted(set(data) - {"label", "timestamp"}, key=repr)
            if extra:
                raise ValueError(
                    f"explicit point window entry carries unsupported key(s) "
                    f"{', '.join(map(repr, extra))}; the point form is "
                    f"{{label, timestamp}} (got {data!r})"
                )
        return data


def _entry_shape(v: Any) -> Any:
    if isinstance(v, (WindowRange, WindowPoint)):
        return v
    if not isinstance(v, dict) or not (
        "label" in v and ({"start", "end"} <= set(v) or "timestamp" in v)
    ):
        raise ValueError(
            f"each explicit window must be a {{label, start, end}} or "
            f"{{label, timestamp}} mapping (got {v!r})"
        )
    return v


def _entry_tag(v: Any) -> str:
    return _tag_of(
        v,
        {WindowPoint: "<point-window>", WindowRange: "<range-window>"},
        lambda r: (
            "<point-window>" if isinstance(r, dict) and "timestamp" in r else "<range-window>"
        ),
    )


WindowEntry = Annotated[
    Annotated[
        Union[
            Annotated[WindowRange, Tag("<range-window>")],
            Annotated[WindowPoint, Tag("<point-window>")],
        ],
        Discriminator(_entry_tag),
    ],
    BeforeValidator(_entry_shape),
]


class Windowing(_Block):
    """Temporal windowing (issue #246): windowed hive leaves, one per schedule label.

    Absent, or ``schedule: none``, is the unwindowed store. A present schedule
    requires the hive layout on a HEALPix grid. ``time_field`` must be a
    base-rate ``data_source.variables`` column (checked in ``validate_config``);
    on the raster path membership is the acquisition's STAC ``datetime``, so
    ``time_field`` is fixed and the conversion knobs are refused.
    """

    #: ``windows: null`` reads as absent beside a non-explicit schedule (and
    #: ``explicit`` refuses it, requiring a non-empty list); ``schedule`` is
    #: refused by its own grammar check with the sharper message.
    _NULLABLE = frozenset({"windows"})
    _NULL_CHECKED = frozenset({"schedule"})

    schedule: str = Field(
        default="none",
        description="``none`` (default), ``yearly``, ``monthly``, ``daily``, or ``explicit`` "
        "(with ``windows``); ``quarterly`` is reserved and not implemented.",
    )
    time_field: str | None = Field(
        default=None,
        description="Per-observation timestamp column deciding window membership (a base-rate "
        "``data_source.variables`` entry).",
    )
    epoch: Instant | None = Field(
        default=None,
        description="The dataset's zero time as an ISO-8601 UTC instant at whole-second "
        "precision (ICESat-2 delta_time: ``2018-01-01T00:00:00Z``).",
    )
    scale: Annotated[str, _Enum("zagg.windows", "EPOCH_SCALES")] | None = Field(
        default=None,
        description="Timescale of ``time_field``: ``utc`` (default), ``gps``, ``tai``.",
    )
    units: Annotated[str, _Enum("zagg.windows", "UNIT_SECONDS")] | None = Field(
        default=None, description="Units of ``time_field``: ``seconds`` (default) or ``days``."
    )
    unit: WindowingUnit | None = Field(
        default=None,
        description="Dispatch unit (issue #586): ``shard`` (default; one invoke per shard emits "
        "every window its granules span) or ``window`` (one invoke per (shard, window)).",
    )
    windows: list[WindowEntry] | None = Field(
        default=None,
        description="The explicit windows (``schedule: explicit`` only): disjoint half-open "
        "``{label, start, end}`` ranges or ``{label, timestamp}`` one-second points.",
    )

    @field_validator("schedule", mode="before")
    @classmethod
    def _schedule(cls, v: Any) -> Any:
        from zagg import windows as _windows

        return _windows.check_schedule(v)

    @field_validator("unit", mode="before")
    @classmethod
    def _unit(cls, v: Any) -> Any:
        from zagg.config.windowing import WINDOWING_UNITS

        if v is not None and v not in WINDOWING_UNITS:
            raise ValueError(
                f"must be one of {WINDOWING_UNITS} (got {v!r}): "
                f"'shard' emits every window from one invoke per shard, 'window' "
                f"dispatches one invoke per (shard, window)"
            )
        return v

    @model_validator(mode="after")
    def _windows_rules(self) -> Windowing:
        from zagg import windows as _windows
        from zagg.config.windowing import _validate_windowing_windows

        if self.schedule == "none":
            # An inert ``schedule: none`` block is equivalent to an absent one,
            # so it needs no layout; a stray ``windows`` key is still rejected.
            if self.windows is not None:
                raise ValueError(
                    "windows only applies to schedule: explicit "
                    "(schedule: none produces no windowed output)"
                )
            return self
        if self.scale is not None and self.scale not in _windows.EPOCH_SCALES:
            raise ValueError(f"scale must be one of {_windows.EPOCH_SCALES} (got {self.scale!r})")
        if self.units is not None and self.units not in _windows.UNIT_SECONDS:
            raise ValueError(
                f"units must be one of {tuple(_windows.UNIT_SECONDS)} (got {self.units!r})"
            )
        if self.epoch is not None:
            try:
                parsed_epoch = _windows.parse_utc(self.epoch)
            except ValueError as e:
                raise ValueError(f"epoch: {e}") from e
            # Validate the RENDERED epoch (issue #390): get_windowing renders at
            # timespec="seconds", so a sub-second epoch would be silently
            # truncated at the one place it is consumed.
            rendered_epoch = _windows.iso_utc(parsed_epoch)
            if _windows.parse_utc(rendered_epoch) != parsed_epoch:
                raise ValueError(
                    f"epoch {parsed_epoch.isoformat()!r} carries sub-second "
                    f"precision that the canonical whole-second rendering drops: it is "
                    f"recorded as {rendered_epoch!r}, which would shift every window "
                    f"conversion by {parsed_epoch.microsecond} µs — declare the epoch at "
                    f"second precision"
                )
        # The windows list half: label grammar, half-open, unique, disjoint
        # (shared with the point/raster branches of the old validator).
        _validate_windowing_windows(self.model_dump(exclude_unset=True), self.schedule)
        return self

    @property
    def declares_schedule(self) -> bool:
        """Whether this block windows the store (any schedule but ``none``)."""
        return self.schedule != "none"


# ---------------------------------------------------------------------------
# output.time_source (issue #410)
# ---------------------------------------------------------------------------


class TimeSource(_Block):
    """The per-observation clock a temporal companion's words are encoded from (spec §8.3).

    ``field`` must be a base-rate read column (checked against the data source
    in ``validate_config``); ``scale`` must be continuous — a nominal-UTC
    column is only good to the leap seconds since its epoch.
    """

    field: str = Field(description="Per-observation time column (a read variable, base rate).")
    epoch: Instant = Field(description="The UTC instant the column counts from.")
    scale: Annotated[str, _Enum("zagg.time_axis", "TOC_SOURCE_SCALES")] | None = Field(
        default=None, description="``gps`` (default) or ``tai``."
    )
    units: Annotated[str, _Enum("zagg.windows", "UNIT_SECONDS")] | None = Field(
        default=None, description="``seconds`` (default) or ``days``."
    )

    @model_validator(mode="before")
    @classmethod
    def _shape(cls, data: Any) -> Any:
        if isinstance(data, dict):
            unknown = set(data) - {"field", "epoch", "scale", "units"}
            if unknown:
                raise ValueError(
                    f"unknown key(s) {sorted(unknown)} (known: epoch, field, scale, units)"
                )
            field = data.get("field")
            if not isinstance(field, str) or not field:
                raise ValueError(
                    "field is required: the per-observation time column "
                    "the temporal companion's words are encoded from (issue #410)"
                )
            if "epoch" not in data:
                raise ValueError("epoch is required: the UTC instant the column counts from")
        return data

    @model_validator(mode="after")
    def _rules(self) -> TimeSource:
        from zagg.time_axis import TOC_SOURCE_SCALES, TOC_WORD_COLUMN
        from zagg.windows import UNIT_SECONDS, parse_utc

        if self.field == TOC_WORD_COLUMN:
            raise ValueError(
                f"field {self.field!r} is the DERIVED toc word column, not a "
                f"source — name the dataset-time column it is encoded from (issue #410)"
            )
        parse_utc(self.epoch)  # raises with the offending value named
        scale = self.scale or TOC_SOURCE_SCALES[0]
        if scale not in TOC_SOURCE_SCALES:
            raise ValueError(
                f"scale must be a continuous timescale {TOC_SOURCE_SCALES} "
                f"(got {scale!r}); a nominal-UTC column cannot carry the nanosecond-exact "
                f"instant spec §8.3 requires of a timestamp word"
            )
        units = self.units or "seconds"
        if units not in UNIT_SECONDS:
            raise ValueError(f"units must be one of {sorted(UNIT_SECONDS)} (got {units!r})")
        return self


# ---------------------------------------------------------------------------
# output.icechunk (issue #580) and output.pyramid (issue #201)
# ---------------------------------------------------------------------------


class Icechunk(_Block):
    """Options for the Icechunk companion repo (spec §11); ``true`` is this block with defaults.

    The shard-order-relative checks run at init (``icechunk_refs.resolve_options``);
    the shape and the one geometry-free invariant — a commit writes whole
    manifests — are here.
    """

    commit: Literal["ladder", "leaf"] | None = Field(
        default=None,
        description="Commit regime: ``ladder`` (refs committed up the staged ladder) or "
        "``leaf`` (per leaf).",
    )
    commit_order: int | None = Field(
        default=None, description="Order a commit covers (default derived from the shard order)."
    )
    split_order: int | None = Field(
        default=None, description="Order one manifest covers; never finer than ``commit_order``."
    )
    retain_runs: int | None = Field(
        default=None, description="Run tags finalize keeps; 0 (default) keeps every run."
    )

    @model_validator(mode="before")
    @classmethod
    def _shape(cls, data: Any) -> Any:
        if isinstance(data, dict):
            unknown = set(data) - {"commit", "commit_order", "split_order", "retain_runs"}
            if unknown:
                raise ValueError(
                    f"unknown key(s) {sorted(unknown)} (accepts commit, "
                    f"commit_order, split_order, retain_runs)"
                )
            retain = data.get("retain_runs")
            if retain is not None and (
                isinstance(retain, bool) or not isinstance(retain, int) or retain < 0
            ):
                raise ValueError(f"retain_runs must be a non-negative integer (got {retain!r})")
            commit = data.get("commit")
            if commit is not None and commit not in ("ladder", "leaf"):
                raise ValueError(f"commit must be 'ladder' or 'leaf' (got {commit!r})")
            for key in ("commit_order", "split_order"):
                value = data.get(key)
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                ):
                    raise ValueError(f"{key} must be a non-negative integer (got {value!r})")
        return data

    @model_validator(mode="after")
    def _orders(self) -> Icechunk:
        if (
            self.commit_order is not None
            and self.split_order is not None
            and self.split_order < self.commit_order
        ):
            raise ValueError(
                f"split_order {self.split_order} is finer than commit_order "
                f"{self.commit_order}: a manifest (one per split_order cell) would be written "
                f"by more than one commit (spec §11.5)"
            )
        return self


class Summarize(StrictModel):
    """One D24 derived-summary declaration: ``{as: <new field name>, ...}``."""

    # No declared fields, so the dump is the author's keys verbatim (an aliased
    # ``as_`` field would dump as ``as_``); ``Pyramid._shape`` enforces ``as``.
    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "properties": {
                "as": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Name of the derived overview field (must be new).",
                }
            },
            "required": ["as"],
        },
    )


class Pyramid(_Block):
    """The overview pyramid declaration (issue #201); ``false`` declares the family off.

    Absent applies the defaults (an every-2-orders schedule below the shard
    order; the all-time fold on a windowed store). ``overviews`` is the
    ``zagg-pyramid/2`` grammar (issue #382) and replaces ``orders``/``spacing``.
    """

    spacing: int | None = Field(
        default=None, description="Every-N-orders overview schedule below the shard order (>= 1)."
    )
    orders: list[NonNegInt] | None = Field(
        default=None,
        description="Explicit ancestor orders in ``[0, parent_order)``; wins over ``spacing``.",
    )
    overviews: Annotated[
        Any,
        WithJsonSchema(
            {"anyOf": [{"type": "integer"}, {"type": "array", "items": {"type": "integer"}}]}
        ),
    ] = Field(
        default=None,
        description="Leaf cell resolutions of the column tier (``zagg-pyramid/2``): an int or a "
        "non-empty list of ints, strictly descending and strictly inside the shard's "
        "resolution window.",
    )

    @field_validator("overviews")
    @classmethod
    def _overviews(cls, v: Any) -> Any:
        if v is not None:
            from zagg.pyramid import normalize_overviews

            normalize_overviews(v)  # zagg.pyramid owns the grammar messages
        return v

    all_time: bool | None = Field(
        default=None,
        description="Maintain the ``all.zarr`` cross-window fold on a windowed store; resolves "
        "true there when omitted, and ``false`` is refused.",
    )
    fold_source: Annotated[str, _Enum("zagg.sweep_overview", "FOLD_SOURCES")] | None = Field(
        default=None,
        description="``cascade`` (default: each coarse level folds from the level below) or the "
        "deprecated ``leaves``.",
    )
    exact_levels: int | None = Field(
        default=None,
        description="Under ``cascade``: how many of the finest declared levels fold exactly from "
        "the leaves (>= 1).",
    )
    summarize: dict[str, Summarize] | None = Field(
        default=None,
        description="D24 derived summaries: ``{source_field: {as: new_name, ...}}``; recorded in "
        "the pyramid block, never the semantic core.",
    )

    @model_validator(mode="before")
    @classmethod
    def _shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        unknown = set(data) - {
            "spacing",
            "orders",
            "overviews",
            "all_time",
            "fold_source",
            "exact_levels",
            "summarize",
        }
        if unknown:
            raise ValueError(f"unknown keys {sorted(unknown)}")
        from zagg.sweep_overview import FOLD_SOURCES

        fold_source = data.get("fold_source")
        if fold_source is not None and fold_source not in FOLD_SOURCES:
            raise ValueError(
                f"fold_source must be one of {list(FOLD_SOURCES)} (got {fold_source!r})"
            )
        exact_levels = data.get("exact_levels")
        if exact_levels is not None and (
            isinstance(exact_levels, bool) or not isinstance(exact_levels, int) or exact_levels < 1
        ):
            raise ValueError(
                f"exact_levels must be an int >= 1 — the finest declared "
                f"level has no finer overview to cascade from, so it is always folded from "
                f"the leaves (got {exact_levels!r})"
            )
        if fold_source == "leaves":
            if exact_levels is not None:
                raise ValueError(
                    "exact_levels is meaningless under fold_source: leaves "
                    "(every level folds from the leaves there); drop one of the two"
                )
            # Legal but deprecated (issue #376): warn at config validation, the
            # one place a misspelled knob still refuses before any store exists.
            logger.warning(
                "output.pyramid.fold_source: leaves is DEPRECATED (issue #376) — it re-folds "
                "every declared level from the raw leaves, so per-node memory grows with the "
                "subtree; the default 'cascade' folds each coarse level from the level below it"
            )
        spacing = data.get("spacing")
        if spacing is not None and (
            isinstance(spacing, bool) or not isinstance(spacing, int) or spacing < 1
        ):
            raise ValueError(f"spacing must be an int >= 1 (got {spacing!r})")
        if data.get("overviews") is not None and (
            data.get("orders") is not None or spacing is not None
        ):
            raise ValueError(
                "overviews is the zagg-pyramid/2 grammar and replaces "
                "orders/spacing wholesale — declare overviews OR orders/spacing, never both"
            )
        all_time = data.get("all_time")
        if all_time is not None and not isinstance(all_time, bool):
            raise ValueError(f"all_time must be a boolean (got {all_time!r})")
        summarize = data.get("summarize")
        if summarize is not None:
            if not isinstance(summarize, dict):
                raise ValueError(f"summarize must be a mapping (got {summarize!r})")
            for source, decl in summarize.items():
                target = (decl or {}).get("as") if isinstance(decl, dict) else None
                if not isinstance(target, str) or not target:
                    raise ValueError(f"summarize.{source} requires 'as': <new field name>")
        return data


def _pyramid_knob(v: Any) -> Any:
    if v is None or v is False or isinstance(v, (dict, Pyramid)):
        return v
    raise ValueError(f"must be a mapping or false (got {v!r})")


def _icechunk_knob(v: Any) -> Any:
    if v is None or isinstance(v, (bool, dict, Icechunk)):
        return v
    raise ValueError(f"must be a boolean or an options block (got {v!r})")


def _sweep_knob(v: Any) -> Any:
    if v is None or isinstance(v, bool) or v in SWEEP_MODES:
        return v
    raise ValueError(f"must be a boolean or one of {list(SWEEP_MODES)} (got {v!r})")


def _windowing_knob(v: Any) -> Any:
    if v is None or isinstance(v, (dict, Windowing)):
        return v
    raise ValueError(f"must be a mapping {{schedule, time_field, epoch, ...}} (got {v!r})")


def _time_source_knob(v: Any) -> Any:
    if v is None or isinstance(v, (dict, TimeSource)):
        return v
    raise ValueError(
        f"must be a mapping {{field, epoch, scale, units}} (got {v!r}) — spec §8.3, issue #410"
    )


def _grid_knob(v: Any) -> Any:
    # A present ``null`` is refused too: no consumer reads it as absent.
    if isinstance(v, (dict, HealpixGrid, RectilinearGrid)):
        return v
    raise ValueError("must be a mapping (e.g. type: healpix, child_order: 12)")


def _flag_or_block_tag(block_cls: type, flag_tag: str, block_tag: str):
    def tag(v: Any) -> str:
        return block_tag if isinstance(v, (dict, block_cls)) else flag_tag

    return tag


#: ``true``/``false`` or an options block, tagged so an error names the key, not a member.
IcechunkKnob = Annotated[
    Union[Annotated[Bool, Tag("<icechunk-flag>")], Annotated[Icechunk, Tag("<icechunk-block>")]],
    Discriminator(_flag_or_block_tag(Icechunk, "<icechunk-flag>", "<icechunk-block>")),
]
PyramidKnob = Annotated[
    Union[
        Annotated[Literal[False], Tag("<pyramid-off>")],
        Annotated[Pyramid, Tag("<pyramid-block>")],
    ],
    Discriminator(_flag_or_block_tag(Pyramid, "<pyramid-off>", "<pyramid-block>")),
]


# ---------------------------------------------------------------------------
# The output block
# ---------------------------------------------------------------------------


class Output(_Block):
    """The store side of a spatial or raster pipeline."""

    #: The knobs whose accessor documents a present-but-null key as the default
    #: (``get_store_layout``, ``get_coverage_moc``, ``get_sweep``,
    #: ``get_leaf_versions``, ``get_icechunk``, ``get_pyramid``,
    #: ``get_windowing``); ``grid`` is refused by its own validator.
    _NULLABLE = frozenset(
        {
            "store_layout",
            "coverage_moc",
            "sweep",
            "leaf_versions",
            "icechunk",
            "pyramid",
            "windowing",
        }
    )
    _NULL_CHECKED = frozenset({"grid"})

    store: str | None = Field(
        default=None, description="Store root: a local path or ``s3://bucket/prefix``."
    )
    format: str | None = Field(default=None, description="Store format; ``zarr`` (default).")
    endpoint_url: str | None = Field(
        default=None,
        description="Non-secret S3-compatible endpoint (R2, MinIO); never credentials.",
    )
    region: str | None = Field(default=None, description="AWS region of the output store.")
    product_name: str | None = Field(
        default=None,
        description="D19 product name (issue #299): the run writes under "
        "``{store}/{product_name}/``; absent is the bare single-product layout.",
    )
    grid: Annotated[Grid | None, BeforeValidator(_grid_knob)] = Field(
        default=None, description="The output grid: ``type: healpix`` or ``type: rectilinear``."
    )
    store_layout: Literal["flat", "hive"] | None = Field(
        default=None,
        description="``hive`` (one leaf zarr per shard under a morton digit tree; the HEALPix "
        "default) or ``flat`` (one shared store; the rectilinear default).",
    )
    touch: Annotated[str, _Enum("zagg.config.accessors", "TOUCH_POLICIES")] | None = Field(
        default=None,
        description="Skip-run lifecycle touch policy (issue #501): ``auto`` (default), "
        "``always``, ``never``.",
    )
    aoi_mask: Bool | None = Field(
        default=None,
        description="Package a per-cell boolean ``aoi_mask`` array (issue #101); default off.",
    )
    consolidate_metadata: Bool | None = Field(
        default=None,
        description="Run zarr metadata consolidation at finalize (flat stores only); default off.",
    )
    coverage_moc: Bool | None = Field(
        default=None,
        description="Write the root ``coverage.moc`` at the end of the run (hive + HEALPix only); "
        "defaults on for hive.",
    )
    sweep: Annotated[
        Any,
        BeforeValidator(_sweep_knob),
        WithJsonSchema(
            {"anyOf": [{"type": "boolean"}, {"enum": list(SWEEP_MODES)}, {"type": "null"}]}
        ),
    ] = Field(
        default=None,
        description="End-of-run sweep (hive only): ``true``/``families`` (the families pass), "
        "``stages`` (plus the staged ladder), ``false``/``none``. Absent resolves to ``stages`` "
        "on a hive store declaring a ladder, else ``families``; ``none`` on a flat store.",
    )
    leaf_versions: Bool | None = Field(
        default=None,
        description="Versioned hive leaves (spec §1.5, issue #582); defaults on for hive.",
    )
    icechunk: Annotated[IcechunkKnob, BeforeValidator(_icechunk_knob)] | None = Field(
        default=None,
        description="Icechunk companion repo (spec §11): a boolean, or an options block (an "
        "opt-in with knobs). Defaults on for non-raster hive stores.",
    )
    pyramid: Annotated[PyramidKnob, BeforeValidator(_pyramid_knob)] | None = Field(
        default=None,
        description="Overview pyramid: absent (defaults), ``false`` (off), or an options "
        "block (hive only).",
    )
    windowing: Annotated[Windowing, BeforeValidator(_windowing_knob)] | None = Field(
        default=None, description="Temporal windowing of the hive leaves (issue #246)."
    )
    time_source: Annotated[TimeSource, BeforeValidator(_time_source_knob)] | None = Field(
        default=None, description="The per-observation clock for temporal companions (spec §8.3)."
    )
    time_encoding: Annotated[str, _Enum("zagg.time_axis", "TIME_ENCODINGS")] | None = Field(
        default=None,
        description="Time-coordinate encoding of a raster ``(time, cells)`` product (spec §8): "
        "``microseconds`` (default) or ``toc``.",
    )

    @field_validator("touch", mode="before")
    @classmethod
    def _touch(cls, v: Any) -> Any:
        if v is not None and v not in TOUCH_POLICIES:
            raise ValueError(
                f"must be one of {', '.join(repr(t) for t in TOUCH_POLICIES)} (got {v!r})"
            )
        return v

    @field_validator("time_encoding", mode="before")
    @classmethod
    def _encoding(cls, v: Any) -> Any:
        from zagg.time_axis import TIME_ENCODINGS

        if v is not None and v not in TIME_ENCODINGS:
            raise ValueError(f"must be one of {TIME_ENCODINGS} (got {v!r})")
        return v

    @field_validator("product_name")
    @classmethod
    def _product(cls, v: str | None) -> str | None:
        if v is not None:
            from zagg.hive import validate_product_name

            validate_product_name(v)
        return v

    # -- resolvers mirrored from the accessors (the raw knob + its default) ---

    @property
    def grid_type(self) -> str:
        return self.grid.type if self.grid is not None else "healpix"

    @property
    def resolved_store_layout(self) -> str:
        """``store_layout`` with the grid-aware default (HEALPix → hive, rect → flat)."""
        if self.store_layout is not None:
            return self.store_layout
        return "hive" if self.grid_type == "healpix" else "flat"

    @property
    def sweep_enabled(self) -> bool | None:
        """``True``/``False`` for an explicit knob (``"none"`` is OFF), ``None`` when absent."""
        if self.sweep is None:
            return None
        if isinstance(self.sweep, bool):
            return self.sweep
        return self.sweep != "none"

    @property
    def icechunk_enabled(self) -> bool:
        """The RESOLVED opt-in: an options block (even ``{}``) is an opt-in whatever it holds."""
        return self.icechunk is True or isinstance(self.icechunk, Icechunk)

    @property
    def declares_windowing(self) -> bool:
        return self.windowing is not None and self.windowing.declares_schedule

    @model_validator(mode="after")
    def _layout_rules(self) -> Output:
        """The intra-output couplings (was ``_validate_store_layout_keys``,
        ``_validate_pyramid``'s layout/order half and ``_validate_windowing``'s)."""
        hive = self.resolved_store_layout == "hive"
        healpix = self.grid_type == "healpix"
        if hive:
            if not healpix:
                raise ValueError(
                    "output.store_layout: hive requires a healpix grid (hive node names "
                    f"are morton decimal digits; grid type is {self.grid_type!r})"
                )
            if self.consolidate_metadata:
                raise ValueError(
                    "output.store_layout: hive has no store-root zarr hierarchy to "
                    "consolidate (D5/D12) — drop output.consolidate_metadata"
                )
        if self.coverage_moc:
            if not healpix:
                raise ValueError(
                    "output.coverage_moc requires a healpix grid (the root coverage.moc "
                    "is a morton MOC; drop the flag for rectilinear output)"
                )
            if not hive:
                raise ValueError(
                    "output.coverage_moc requires output.store_layout: hive (the root "
                    "coverage.moc lives at the hive store root; flat stores have no "
                    "hive root to bootstrap from)"
                )
        # Resolved, not truthy: ``"none"`` is a truthy string that means OFF.
        if self.sweep_enabled and not hive:
            raise ValueError(
                "output.sweep requires output.store_layout: hive (the rollup sweep "
                "folds hive-tree leaf artifacts; flat stores have no digit tree)"
            )
        if self.leaf_versions and not hive:
            raise ValueError(
                "output.leaf_versions requires output.store_layout: hive (a version subgroup "
                "lives under a hive leaf's stable root)"
            )
        if self.icechunk_enabled and not hive:
            raise ValueError(
                "output.icechunk requires output.store_layout: hive (the companion repo "
                "references hive leaves by shard rank; flat stores have no leaves)"
            )
        self._pyramid_rules(hive)
        if self.declares_windowing:
            if not healpix:
                raise ValueError(
                    "output.windowing requires a healpix grid (windowed leaves are a "
                    f"morton-hive convention; grid type is {self.grid_type!r})"
                )
            if not hive:
                raise ValueError(
                    "output.windowing requires output.store_layout: hive (window leaves "
                    "are hive leaf zarrs; the flat shared store has no leaves to window)"
                )
        return self

    def _pyramid_rules(self, hive: bool) -> None:
        knob = self.pyramid
        if knob is None or knob is False:
            return
        if not hive:
            raise ValueError(
                "output.pyramid requires output.store_layout: hive (overviews live at "
                "hive-tree ancestor nodes; flat stores have no digit tree)"
            )
        grid = self.grid if isinstance(self.grid, HealpixGrid) else None
        parent_order = grid.parent_order if grid is not None else 0
        if knob.overviews is not None:
            from zagg.pyramid import normalize_overviews, validate_overviews

            # A missing child_order must refuse by NAME, not through a
            # level-range error that points at neither the problem nor the key.
            if grid is None:
                raise ValueError("output.grid.child_order is required")
            validate_overviews(
                normalize_overviews(knob.overviews),
                parent_order=parent_order,
                child_order=grid.child_order,
            )
        if knob.orders is not None and any(not (0 <= k < parent_order) for k in knob.orders):
            raise ValueError(
                f"output.pyramid.orders must be a list of ancestor orders in "
                f"[0, parent_order) = [0, {parent_order}) (got {knob.orders!r})"
            )
        if knob.all_time is False and self.declares_windowing:
            # espg's ruling on PR #623 question (16): a windowed store must
            # declare the all-time fold; the absent key resolves to true.
            raise ValueError(
                "output.windowing requires the all-time fold: output.pyramid.all_time: false "
                "is refused on a windowed store (the rollup families and the multiscales "
                "companion fold in each node's all-time close, issue #610 phase 4) — omit the "
                "key, which resolves to true on a windowed store, or set it true"
            )


class TemporalOutput(StrictModel):
    """The tabular sink of the temporal/event engine."""

    format: str | None = Field(
        default=None, description="Sink format (``parquet``); ``zarr`` is the spatial default."
    )
    store: str | None = Field(default=None, description="Output path or ``s3://`` URI.")
    endpoint_url: str | None = Field(default=None, description="Non-secret S3-compatible endpoint.")
    region: str | None = Field(default=None, description="AWS region of the output store.")
    time_encoding: str | None = Field(
        default=None,
        description="Accepted for symmetry; a non-default value is refused (raster only).",
    )
