"""The ``aggregation`` grammar (issue #677 phase 2).

Two shapes, one per engine: the spatial point-cloud aggregation
(:class:`Aggregation` — per-field reducers over read columns, chunk-level
precomputes, the carrier and streaming knobs) and the temporal/event
aggregation (:class:`TemporalAggregation` — per-variable capability names
the registry resolves at run time, issue #12).

``function``/``spatial_func``/``temporal_reducer``/``mask`` stay STRINGS
here (strings-not-callables, issue #64): the models check shape only.
Whether a ``function:`` dotted path is allowed to import is issue #281's
call and is not decided by the grammar. Column cross-references (``source``
in the read columns, expression identifiers, the location column) need the
``data_source`` block and stay in :func:`zagg.config.validate_config`.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Union

import numpy as np
from pydantic import Field, ValidationInfo, field_validator, model_validator

from zagg.config.models_base import Bool, NonEmptyStr, PosInt, _Block

__all__ = [
    "Aggregation",
    "CoordinateSpec",
    "FieldSpec",
    "PrecomputeSpec",
    "Streaming",
    "TemporalAggregation",
    "TemporalFieldSpec",
]

#: Recognized per-field output kinds. ``ragged`` is the Tier-2 carrier for
#: variable-length per-cell outputs (issue #48).
OutputKind = Literal["scalar", "vector", "ragged"]
#: ``cell`` (default) stores one value per aggregation cell; ``chunk`` one
#: value per chunk in a companion array at the chunk grid (issue #30 item 2).
OutputResolution = Literal["cell", "chunk"]
#: The §8 field shapes a temporal companion may declare (issue #410); the
#: ``coordinate`` shape is the time axis's own and is refused by name.
TemporalShape = Literal["per-cell", "per-centroid"]
#: Spec §2.0 ragged payload weight semantics (issue #424).
WeightsKind = Literal["counts", "flux"]

#: A positive-int shape: ``128`` or ``[16, 2]``.
Shape = Union[PosInt, Annotated[list[PosInt], Field(min_length=1)]]


def _check_shape(v: Any, key: str) -> Any:
    if isinstance(v, int) and not isinstance(v, bool):
        dims: tuple = (v,)
    elif isinstance(v, (list, tuple)):
        dims = tuple(v)
    else:
        raise ValueError(f"'{key}' must be an int or a sequence of ints (got {v!r})")
    if not dims:
        raise ValueError(f"'{key}' must have at least one dimension")
    for dim in dims:
        if not isinstance(dim, int) or isinstance(dim, bool) or dim < 1:
            raise ValueError(f"'{key}' entries must be positive integers (got {dim!r})")
    return v


def _check_dtype(v: Any) -> Any:
    if v is None:
        return v
    try:
        np.dtype(v)
    except (TypeError, ValueError) as e:
        raise ValueError(f"dtype {v!r} is not a valid numpy dtype ({e})") from e
    return v


class _Reducer(_Block):
    """What a field and a chunk precompute share: exactly one of ``function``/``expression``."""

    function: str | None = Field(
        default=None,
        description="Reducer name: ``len``/``count``, a numpy name (``mean``, ``nanmax``, "
        "``quantile``), or a dotted path (``zagg.stats.tdigest.build_tdigest``) resolved by "
        "``zagg.config.resolve_function`` at validation.",
    )
    expression: str | None = Field(
        default=None,
        description="Python expression over column names and ``np`` evaluated per cell "
        "(mutually exclusive with ``function``).",
    )
    source: str | None = Field(
        default=None,
        description="The read column (a ``data_source.variables`` name or a broadcast "
        "segment-level variable) the reducer takes as its positional argument.",
    )
    params: dict[str, Any] | None = Field(
        default=None,
        description="Keyword arguments forwarded to the reducer. A string value naming a "
        "column (or an expression over columns) is evaluated per cell first; numbers pass "
        "through. ``locations``/``temporal`` are reserved for the injected channels.",
    )
    dtype: str | None = Field(
        default=None, description="Stored numpy dtype of the result (``float32``, ``uint64``…)."
    )

    @field_validator("dtype")
    @classmethod
    def _dtype(cls, v: str | None) -> str | None:
        return _check_dtype(v)

    @model_validator(mode="after")
    def _one_of(self) -> _Reducer:
        has_func = "function" in self.model_fields_set
        has_expr = "expression" in self.model_fields_set
        if has_func and has_expr:
            raise ValueError("'function' and 'expression' are mutually exclusive")
        if not has_func and not has_expr:
            raise ValueError("must specify 'function' or 'expression'")
        return self


class FieldSpec(_Reducer):
    """One output field of the spatial aggregation (``aggregation.variables.<name>``).

    A scalar field by default; ``kind: vector`` (+ ``trailing_shape``) and
    ``kind: ragged`` (+ ``inner_shape``) declare the non-scalar carriers
    (issues #29, #48). A ragged field may carry a ``location`` channel
    (issue #87), a ``temporal`` companion (spec §8, issue #410), a ``weights``
    declaration and an ``overview_delta`` fold budget (issue #424).
    """

    _NULLABLE = frozenset({"fill_value", "trailing_shape", "inner_shape"})

    fill_value: Any = Field(
        default=None,
        description='Fill for unobserved cells: a number, or the string ``"NaN"`` '
        '(YAML ``.nan`` is canonicalized to it at load). Dense default ``"NaN"``.',
    )
    kind: OutputKind = Field(
        default="scalar",
        description="Output carrier: ``scalar`` (default), ``vector``, ``ragged``.",
    )
    trailing_shape: Shape | None = Field(
        default=None,
        description="Per-cell trailing shape of a ``vector`` field (an int or a list of ints).",
    )
    inner_shape: Shape | None = Field(
        default=None,
        description="Per-element shape of a ``ragged`` field's payload rows, e.g. ``[2]`` for "
        "``(mean, weight)`` centroid pairs.",
    )
    resolution: OutputResolution = Field(
        default="cell",
        description="``cell`` (default) or ``chunk``: one value per chunk, written to a companion "
        "array at the chunk grid (the compact store for a chunk-uniform value).",
    )
    location: str | None = Field(
        default=None,
        description="Ragged location channel (issue #87): the per-observation morton column — "
        "``leaf_id`` (the HEALPix read path's) or a data column — folded per centroid into a "
        "uint64 sibling array; requires a ``function`` reducer that accepts ``locations=``.",
    )
    temporal: TemporalShape | None = Field(
        default=None,
        description="Temporal companion shape (spec §8.2/§8.3): ``per-cell`` (the field's own "
        "dense uint64 array holds the words; ``fill_value: 0``) or ``per-centroid`` (a uint64 "
        "sibling row-aligned with a ragged payload). Requires a toc-producing reducer and the "
        "store's clock (``output.time_source``).",
    )
    weights: WeightsKind | None = Field(
        default=None,
        description="Ragged payload weight semantics (spec §2.0): ``counts`` (the absent-key "
        "default, integer weights) or ``flux`` (positive reals; requires ``attrs.gain`` with "
        "``name`` and ``version``).",
    )
    overview_delta: PosInt | None = Field(
        default=None,
        description="Pyramid-fold centroid budget for a ragged digest (issue #424), split from "
        "the leaf ``params.delta``; packaging, never hashed.",
    )
    attrs: dict[str, Any] | None = Field(
        default=None,
        description="Free-form JSON provenance merged onto the field's array attrs. Spec-owned "
        "keys (the ragged layout block, ``weights`` on ragged, the temporal/located "
        "bindings) are stamped by the writer and refused here; a ``composition`` block is "
        "cross-checked against the field it describes (issue #321).",
    )

    @field_validator("trailing_shape", "inner_shape", mode="before")
    @classmethod
    def _shape(cls, v: Any, info: ValidationInfo) -> Any:
        # No ``None`` short-circuit: pydantic runs no validator for an absent
        # key, so only an explicit ``null`` reaches here, and it is refused.
        return _check_shape(v, str(info.field_name))

    @field_validator("attrs", mode="before")
    @classmethod
    def _attrs_keys(cls, v: Any) -> Any:
        if v is not None and (not isinstance(v, dict) or not all(isinstance(k, str) for k in v)):
            raise ValueError("attrs must be a mapping with string keys")
        return v

    @field_validator("overview_delta", mode="before")
    @classmethod
    def _overview_delta(cls, v: Any) -> Any:
        if v is None:
            return v
        if not isinstance(v, int) or isinstance(v, bool) or v < 1:
            raise ValueError(f"overview_delta must be a positive int (got {v!r})")
        return v

    @field_validator("temporal", mode="before")
    @classmethod
    def _temporal(cls, v: Any) -> Any:
        from zagg.time_axis import TOC_SHAPE_COORDINATE

        if v == TOC_SHAPE_COORDINATE:
            raise ValueError(
                f"temporal shape {TOC_SHAPE_COORDINATE!r} is the time axis' own declaration — "
                f"set 'output.time_encoding: toc' instead (spec §8.1)"
            )
        return v

    @model_validator(mode="after")
    def _kind_rules(self) -> FieldSpec:
        """The output-kind declaration (was ``_validate_output_kind``) and the
        temporal companion's shape rules (was ``_validate_temporal_shape``)."""
        kind = self.kind
        fields = self.model_fields_set
        if "location" in fields and kind != "ragged":
            raise ValueError(f"'location' is only valid for kind 'ragged', not '{kind}'")
        for key in ("weights", "overview_delta"):
            if key in fields and kind != "ragged":
                raise ValueError(f"'{key}' is only valid for kind 'ragged', not '{kind}'")
        self._temporal_shape_rules()
        has_trailing = "trailing_shape" in fields
        if kind == "scalar":
            if has_trailing:
                raise ValueError("'trailing_shape' is only valid for kind 'vector', not 'scalar'")
        elif kind == "vector":
            if not has_trailing:
                raise ValueError("kind 'vector' requires 'trailing_shape'")
            # ``len``/``count`` short-circuit to a scalar obs count; pairing them
            # with a non-scalar kind would silently emit a scalar.
            if self.function in ("len", "count"):
                raise ValueError(
                    f"function {self.function!r} produces a scalar count and cannot be "
                    f"combined with kind 'vector'"
                )
        else:
            if has_trailing:
                raise ValueError("'trailing_shape' is only valid for 'vector', not 'ragged'")
            if "inner_shape" not in fields:
                raise ValueError("kind 'ragged' requires 'inner_shape'")
            if self.weights == "flux":
                gain = (self.attrs or {}).get("gain")
                if not isinstance(gain, dict) or not {"name", "version"} <= set(gain):
                    raise ValueError(
                        "weights 'flux' requires calibration provenance in attrs — a 'gain' "
                        "mapping with at least 'name' and 'version' (spec §2.0, issue #424)"
                    )
            if self.function in ("len", "count"):
                raise ValueError(
                    f"function {self.function!r} produces a scalar count and cannot be "
                    f"combined with kind 'ragged'"
                )
            if self.location is not None:
                if "function" not in fields:
                    raise ValueError(
                        "'location' requires a 'function' reducer "
                        "(an expression cannot receive the locations column)"
                    )
                if self.resolution == "chunk":
                    raise ValueError(
                        "'location' is not supported with 'resolution: chunk' "
                        "(locations are folded per cell)"
                    )
                if "locations" in (self.params or {}):
                    raise ValueError(
                        "params may not name 'locations' — it is reserved for the location "
                        "channel (issue #87)"
                    )
        if self.temporal == "per-centroid" and "temporal" in (self.params or {}):
            raise ValueError(
                "params may not name 'temporal' — it is reserved for the temporal channel "
                "(spec §8.3, issue #410)"
            )
        self._attrs_rules()
        return self

    def _temporal_shape_rules(self) -> None:
        from zagg.time_axis import TOC_SHAPE_PER_CELL, TOC_SHAPE_PER_CENTROID, TOC_UNOBSERVED

        shape = self.temporal
        if shape is None:
            return
        if self.resolution != "cell":
            raise ValueError(
                "'temporal' is not supported with 'resolution: chunk' — a temporal companion "
                "is per cell or per centroid (spec §8.2/§8.3)"
            )
        if shape == TOC_SHAPE_PER_CELL:
            if self.kind != "scalar":
                raise ValueError(
                    f"temporal {TOC_SHAPE_PER_CELL!r} is a dense per-cell companion, so kind "
                    f"must be 'scalar', not {self.kind!r} (spec §8.2)"
                )
            if str(self.dtype) != "uint64":
                raise ValueError(
                    f"temporal {TOC_SHAPE_PER_CELL!r} requires dtype 'uint64' (got "
                    f"{self.dtype!r}) — the words are packed uint64 (spec §8.2)"
                )
            # A spec-owned reservation is declared, never inferred: the dense
            # template default is "NaN", which is not a uint64 word.
            if "fill_value" not in self.model_fields_set:
                raise ValueError(
                    f"temporal {TOC_SHAPE_PER_CELL!r} requires an explicit fill_value "
                    f"{TOC_UNOBSERVED} — §8.2 reserves it as the unobserved-cell marker, and "
                    f"the dense default ('NaN') is not a uint64 word"
                )
            fill = self.fill_value
            if isinstance(fill, bool) or fill != TOC_UNOBSERVED:
                raise ValueError(
                    f"temporal {TOC_SHAPE_PER_CELL!r} requires fill_value {TOC_UNOBSERVED} "
                    f"(got {fill!r}) — §8.2 reserves it as the unobserved-cell marker"
                )
        elif self.kind != "ragged":
            raise ValueError(
                f"temporal {TOC_SHAPE_PER_CENTROID!r} is a sibling of a ragged payload, so "
                f"kind must be 'ragged', not {self.kind!r} (spec §8.3)"
            )

    def _attrs_rules(self) -> None:
        attrs = self.attrs
        if attrs is None:
            return
        from zagg.grids.base import LOCATED_ATTR, RAGGED_ELEMENT_ATTR, TIMES_ATTR, WEIGHTS_ATTR
        from zagg.time_axis import TEMPORAL_ATTR

        if RAGGED_ELEMENT_ATTR in attrs:
            raise ValueError(
                f"attrs key {RAGGED_ELEMENT_ATTR!r} is reserved for the ragged layout block "
                f"(issue #209)"
            )
        if self.kind == "ragged" and WEIGHTS_ATTR in attrs:
            raise ValueError(
                f"attrs key {WEIGHTS_ATTR!r} is spec-owned on a ragged payload array — declare "
                f"the field-level 'weights:' key instead (spec §2.0, issue #424)"
            )
        for key in (TEMPORAL_ATTR, LOCATED_ATTR, TIMES_ATTR):
            if key in attrs:
                raise ValueError(
                    f"attrs key {key!r} is spec-owned (spec §8/§9) — declare the field-level "
                    f"'temporal:'/'location:' key instead (issue #410)"
                )
        try:
            json.dumps(attrs)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"attrs must be JSON-serializable ({exc})") from exc


class PrecomputeSpec(_Reducer):
    """One ``aggregation.chunk_precompute`` entry: a chunk-level scalar (issue #30).

    Evaluated ONCE per chunk over the shard's pooled columns, before the
    per-cell loop; its name then enters the per-cell expression namespace.
    Entries are independent (one cannot reference another).
    """


class CoordinateSpec(_Block):
    """A declared store coordinate (``aggregation.coordinates.<name>``), e.g. ``morton``."""

    _NULLABLE = frozenset({"fill_value"})

    dtype: NonEmptyStr | None = Field(
        default=None,
        description="Stored numpy dtype (default ``float32``), e.g. ``uint64`` for ``morton``.",
    )
    fill_value: Any = Field(
        default=None,
        description='Fill for unobserved cells (default ``"NaN"``; integer dtypes need an '
        "explicit integer fill, e.g. ``0`` for ``morton``).",
    )

    @field_validator("dtype")
    @classmethod
    def _dtype(cls, v: str | None) -> str | None:
        return _check_dtype(v)


class Streaming(_Block):
    """Bounded-memory read/aggregate regime (issues #370, #474); absent is the pooled path.

    Packaging: every streaming regime shares one product identity under the
    law-equivalence contract (``zagg.semantics``).
    """

    mode: Literal["merge", "spill"] | None = Field(
        default=None,
        description="``merge`` (default): fold flushed buffers in memory; ``spill``: write "
        "buffers to /tmp partitions and aggregate once after the reads.",
    )
    buffer_granules: PosInt | None = Field(
        default=None, description="Granules of post-filter rows held before a flush (default 20)."
    )
    block_bytes: PosInt | None = Field(
        default=None, description="Spill-only cap on a block before it closes and folds."
    )

    @model_validator(mode="after")
    def _spill_only(self) -> Streaming:
        if self.block_bytes is not None and self.mode != "spill":
            raise ValueError(
                f"block_bytes only applies to mode: spill (got mode: {self.mode or 'merge'!r})"
            )
        return self


class Aggregation(_Block):
    """The spatial point-cloud aggregation block."""

    handoff: Literal["pandas", "arrow"] | None = Field(
        default=None,
        description="Per-cell carrier (issue #132): ``arrow`` (default; an arro3 Table) or "
        "``pandas`` (tolerates nullable columns). Packaging, never hashed.",
    )
    streaming: Streaming | None = Field(
        default=None, description="Bounded-memory regime knobs; absent is the pooled path."
    )
    coordinates: dict[str, CoordinateSpec] | None = Field(
        default=None,
        description="Declared store coordinates by name (the HEALPix templates declare "
        "``morton: {dtype: uint64, fill_value: 0}``).",
    )
    variables: dict[str, FieldSpec] | None = Field(
        default=None, description="Output fields by name."
    )
    chunk_precompute: dict[NonEmptyStr, PrecomputeSpec] | None = Field(
        default=None,
        description="Chunk-level scalars by name, computed once per chunk and exposed to the "
        "per-cell expressions (issue #30).",
    )

    @field_validator("chunk_precompute", mode="before")
    @classmethod
    def _precompute_names(cls, v: Any) -> Any:
        if isinstance(v, dict) and any(not isinstance(k, str) or not k.strip() for k in v):
            raise ValueError("chunk_precompute entry names must be non-empty strings")
        return v

    @model_validator(mode="after")
    def _cross_field(self) -> Aggregation:
        """Rules spanning fields: the sibling-array name reservations (issues #209,
        #410) and the ``composition`` attrs cross-check (was ``_validate_composition_attrs``)."""
        variables = self.variables or {}
        coordinates = self.coordinates or {}
        from zagg.grids.base import ragged_locations_name, ragged_times_name

        for name, meta in variables.items():
            if meta.location is not None:
                sibling = ragged_locations_name(name)
                if sibling in variables or sibling in coordinates:
                    raise ValueError(
                        f"Variable '{name}': its location channel is stored in a sibling array "
                        f"named '{sibling}', which collides with the declared field/coordinate "
                        f"of that name — rename one of them (issue #209)"
                    )
            if meta.temporal == "per-centroid":
                sibling = ragged_times_name(name)
                if sibling in variables or sibling in coordinates:
                    raise ValueError(
                        f"Variable '{name}': its temporal channel is stored in a sibling array "
                        f"named '{sibling}', which collides with the declared field/coordinate "
                        f"of that name — rename one of them (spec §8.3)"
                    )
            if meta.attrs is not None:
                _composition_rules(name, meta, variables)
        return self


def _composition_rules(name: str, meta: FieldSpec, variables: dict[str, FieldSpec]) -> None:
    """Cross-check a ``composition`` attrs block against the field it describes.

    The whole field rests on one invariant: the ``N`` a reader divides the
    lanes by is the total weight of the digest named by ``of``, and the lanes
    were packed at the ``threshold`` recorded here (issue #321). ``spec`` and
    ``lanes`` are spec-owned (stamped from ``zagg.stats.composition``) and a
    declaration must agree with the constant; ``fill_value`` must be ``0``
    (readers key presence off ``lane > 0``). The digest fields' ``where``
    predicates committing the same cut is NOT checked (an arbitrary
    expression) — see ``pack_composition``'s docstring.
    """
    block = (meta.attrs or {}).get("composition")
    if not isinstance(block, dict):
        return
    from zagg.stats.composition import COMPOSITION_SPEC, LANES

    spec = block.get("spec")
    if spec is not None and spec != COMPOSITION_SPEC:
        raise ValueError(
            f"Variable '{name}': attrs.composition.spec {spec!r} is not the convention this "
            f"writer packs ({COMPOSITION_SPEC!r}) — the marker is stamped from "
            f"zagg.stats.composition, not declared (issue #340)"
        )
    lanes = block.get("lanes")
    if lanes is not None and (not isinstance(lanes, (list, tuple)) or list(lanes) != list(LANES)):
        raise ValueError(
            f"Variable '{name}': attrs.composition.lanes must be the {COMPOSITION_SPEC} lane "
            f"order {list(LANES)} — pack_composition packs lane i at bits 8i..8i+7 in that "
            f"fixed order, so a permuted declaration would mislabel every lane"
        )
    if meta.fill_value != 0:
        raise ValueError(
            f"Variable '{name}': a composition field must declare fill_value: 0 (got "
            f"{meta.fill_value!r}) — an empty signal stratum packs to 0, and readers key "
            f"presence off 'lane > 0', so a nonzero fill makes every unwritten cell report "
            f"spurious flag presence (spec §3)"
        )
    of = block.get("of")
    if of is not None:
        if of == name:
            raise ValueError(
                f"Variable '{name}': attrs.composition.of names the composition field itself; "
                f"it must name the signal digest whose total weight is N_signal"
            )
        target = variables.get(of)
        if target is None:
            raise ValueError(
                f"Variable '{name}': attrs.composition.of '{of}' is not a declared "
                f"aggregation.variables field (readers resolve N_signal through it)"
            )
        if target.kind != "ragged":
            raise ValueError(
                f"Variable '{name}': attrs.composition.of '{of}' must be a 'kind: ragged' "
                f"digest field (got kind {target.kind!r}) — N_signal is its total weight"
            )
    threshold = block.get("threshold")
    if threshold is not None:
        packed = (meta.params or {}).get("threshold")
        if packed is not None and packed != threshold:
            raise ValueError(
                f"Variable '{name}': attrs.composition.threshold {threshold!r} disagrees with "
                f"params.threshold {packed!r} — the recorded threshold must be the one the "
                f"reducer packs at (issue #321)"
            )


# ---------------------------------------------------------------------------
# The temporal/event engine's aggregation (issue #12)
# ---------------------------------------------------------------------------


class TemporalFieldSpec(_Block):
    """One temporal/event output variable: capability names resolved by the registry at run time.

    A typo in a capability name surfaces as ``UnknownCapability`` from
    ``process_event``, never as a load-time guess about installed plugins.
    """

    variable: str = Field(description="Source variable name within the collection.")
    collection: str = Field(description="The ``data_source.collections`` entry to read it from.")
    spatial_func: str = Field(
        description="Registered spatial reducer over the masked grid (``max``, ``weighted_mean``…)."
    )
    temporal_reducer: str = Field(
        description="Registered reducer over the event's time window (``max``, ``sum``…)."
    )
    mask: str | None = Field(
        default=None, description="Registered spatial mask name (default ``ais``)."
    )
    negate: Bool | None = Field(
        default=None,
        description="Negate the field values (after any transform, before the spatial "
        "reduction), e.g. for southward-positive fluxes.",
    )
    anomaly: Bool | None = Field(
        default=None,
        description="Sugar for ``transform: monthly_anomaly`` (subtract the climatology).",
    )
    transform: str | None = Field(
        default=None,
        description="Registered transform applied before reduction; wins over ``anomaly``.",
    )
    trigger: str | None = Field(default=None, description="Registered event-trigger predicate.")
    trigger_mask: str | None = Field(
        default=None,
        description="Static-data mask the trigger tests against (default ``ais_mask``).",
    )
    params: dict[str, Any] | None = Field(
        default=None,
        description="Free-form mapping passed to the mask provider, field transform and event "
        "trigger (not to spatial funcs or reducers).",
    )


class TemporalAggregation(_Block):
    """The temporal/event aggregation block: output variables by name."""

    variables: dict[str, TemporalFieldSpec] = Field(description="Output variables by name.")
