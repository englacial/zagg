"""The template grammar as pydantic models — root union and shared plumbing (issue #677).

One model tree *is* the grammar: it loads a template dict into typed objects,
rejects unknown keys (``extra="forbid"`` everywhere) and wrong shapes with
path-addressed errors, and exports the JSON schema the MCP ``build_template``
tool hands a model. This module holds what every block module shares — the
strict base, the boolean-safe scalar types, the tagged-union helpers — plus
the root config union and the ``ValidationError`` → ``ValueError``
translation every caller sees.

Three design rules, all hash-stability obligations (issue #677):

- the models NEVER feed the stored config: :class:`~zagg.config.PipelineConfig`
  keeps the raw dicts the author wrote (NaN fills canonicalized, nothing
  else), so the accessors and :mod:`zagg.semantics` see exactly those keys.
  The typed tree is a validated VIEW (``config.model``), and
  ``model_dump(exclude_unset=True)`` reproduces the raw blocks key for key —
  ``tests/test_config_models.py`` proves it on every packaged template;
- defaults declared on a field document the consumer's default and are
  never injected into a dump (``exclude_unset``);
- every ``function:``/capability name stays a string the registry resolves
  later (strings-not-callables, issue #64) — the models check shape only.

The scalar types and the strict base live in :mod:`zagg.config.models_base`;
the block grammars in ``models_source.py`` (and, per phase, their siblings).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import (
    BaseModel,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    ValidationError,
    field_validator,
)

from zagg.config.models_aggregation import Aggregation, TemporalAggregation
from zagg.config.models_base import TAGS, StrictModel
from zagg.config.models_source import CollectionDataSource, H5DataSource, RasterDataSource

__all__ = [
    "ConfigModel",
    "RasterConfig",
    "SpatialConfig",
    "TemporalConfig",
    "format_validation_error",
    "validate_model",
]


# ---------------------------------------------------------------------------
# Root config: one of three grammars, picked exactly as ``validate_config``
# branches — pipeline kind first, then the raster reader
# ---------------------------------------------------------------------------


class _RootBase(StrictModel):
    """The four blocks phases 1–3 leave untyped (phase 4 types them)."""

    output: dict[str, Any] = Field(
        default_factory=dict, description="The output block (typed in issue #677 phase 3)."
    )
    pipeline: Any = Field(
        default=None,
        description="The pipeline kind selector, ``{type: spatial|temporal|event}``; absent "
        "means ``spatial`` (typed in issue #677 phase 4).",
    )
    worker: Any = Field(
        default=None,
        description="Optional Lambda worker-size selector ``{memory, extra_disk?}`` "
        "(typed in issue #677 phase 4).",
    )
    catalog: Any = Field(default=None, description="Optional path to the granule catalog.")
    bounds: Any = Field(
        default=None,
        description="Optional ``{temporal: {start_date, end_date}, spatial}`` run bounds "
        "(typed in issue #677 phase 4).",
    )


class SpatialConfig(_RootBase):
    """A point-cloud → grid pipeline reading HDF5 through h5coro (the default kind)."""

    aggregation: Aggregation = Field(
        default_factory=Aggregation,
        description="Per-field reducers over the read columns, chunk precomputes, carrier "
        "and streaming knobs.",
    )
    data_source: H5DataSource = Field(
        default_factory=H5DataSource,
        description="The HDF5 point source: groups, coordinates, variables, filters, levels.",
    )


class RasterConfig(_RootBase):
    """A raster (pull-NN) pipeline: ``data_source.reader: raster`` (issue #218)."""

    aggregation: dict[str, Any] = Field(
        default_factory=dict,
        description="Must be empty: pull-NN yields exactly one value per cell per timestep, so "
        "there is nothing to reduce — the band map under ``data_source.bands`` replaces it.",
    )

    @field_validator("aggregation")
    @classmethod
    def _no_aggregation(cls, v: dict[str, Any]) -> dict[str, Any]:
        if v:
            raise ValueError(
                "raster pipelines take no aggregation section: pull-NN yields one "
                "value per cell per timestep (declare bands under data_source.bands)"
            )
        return v

    data_source: RasterDataSource = Field(
        description="The STAC-asset raster source: a band map plus source-store knobs."
    )


class TemporalConfig(_RootBase):
    """A temporal/event pipeline over gridded collections (issue #12, #213)."""

    aggregation: TemporalAggregation = Field(
        description="Output variables as registry capability names (spatial reducer, "
        "temporal reducer, mask, transform)."
    )
    data_source: CollectionDataSource = Field(
        default_factory=CollectionDataSource,
        description="The collection reader source: named collections with per-collection "
        "reader options, plus static data.",
    )


def _config_tag(v: Any) -> str:
    if isinstance(v, BaseModel):
        return {
            SpatialConfig: "<spatial-config>",
            RasterConfig: "<raster-config>",
            TemporalConfig: "<temporal-config>",
        }[type(v)]
    if not isinstance(v, dict):
        return "<spatial-config>"
    pipeline = v.get("pipeline")
    if isinstance(pipeline, dict) and pipeline.get("type") in ("temporal", "event"):
        return "<temporal-config>"
    ds = v.get("data_source")
    if isinstance(ds, dict) and ds.get("reader") == "raster":
        return "<raster-config>"
    return "<spatial-config>"


#: The template grammar: exactly one of the three config kinds, discriminated
#: the way :func:`zagg.config.validate_config` branches (``pipeline.type``
#: first, then ``data_source.reader == "raster"``), so a template is checked
#: against the grammar the engine will actually run it under.
ConfigModel = Annotated[
    Union[
        Annotated[SpatialConfig, Tag("<spatial-config>")],
        Annotated[RasterConfig, Tag("<raster-config>")],
        Annotated[TemporalConfig, Tag("<temporal-config>")],
    ],
    Discriminator(_config_tag),
]

_ROOT: TypeAdapter = TypeAdapter(ConfigModel)


# ---------------------------------------------------------------------------
# Errors: pydantic's list → one ``ValueError`` with ``<path>: <msg>`` lines
# ---------------------------------------------------------------------------


def _render_loc(loc: tuple) -> str:
    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] += f"[{item}]"
            else:
                parts.append(f"[{item}]")
        elif item in TAGS:
            continue
        else:
            parts.append(str(item))
    return ".".join(parts)


def format_validation_error(exc: ValidationError) -> str:
    """``"<loc path>: <msg>"`` per error, ``"; "``-joined, tags stripped.

    A ``ValueError`` raised inside a validator is reported verbatim (pydantic's
    ``"Value error, "`` prefix dropped), so a rule's message reads the same
    whether the model or the remaining imperative checks refuse it.
    """
    lines = []
    for err in exc.errors():
        path = _render_loc(tuple(err["loc"]))
        msg = err["msg"]
        if err["type"] == "value_error" and "error" in (err.get("ctx") or {}):
            msg = str(err["ctx"]["error"])
        lines.append(f"{path}: {msg}" if path else msg)
    return "; ".join(lines)


def validate_model(config: Any) -> SpatialConfig | RasterConfig | TemporalConfig:
    """Validate a :class:`~zagg.config.PipelineConfig` through the grammar.

    Returns the typed tree. Raises ``ValueError`` (never
    ``pydantic.ValidationError``) with every offending key's path in the
    message, so existing ``except ValueError`` paths and the Lambda error
    envelope are unchanged. Pure: the config's dicts are read, never written.
    """
    raw = {
        "data_source": config.data_source,
        "aggregation": config.aggregation,
        "output": config.output,
        "pipeline": config.pipeline,
        "worker": config.worker,
        "catalog": config.catalog,
        "bounds": config.bounds,
    }
    try:
        return _ROOT.validate_python(raw)
    except ValidationError as exc:
        raise ValueError(format_validation_error(exc)) from exc


#: The pipeline-type literal the phase 4 model will carry; exported here so
#: the discriminator and the typed block agree on one spelling.
PipelineType = Literal["spatial", "temporal", "event"]
