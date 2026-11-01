"""The run-level blocks: ``pipeline``, ``worker``, ``catalog``, ``bounds`` (issue #677 phase 4).

Small grammars, typed per root kind so the exported union stops overlapping:
a spatial or raster config carries ``pipeline.type: spatial`` (or no block),
a temporal config ``temporal``/``event``.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field, WithJsonSchema, field_validator, model_validator

from zagg.config.core import WORKER_MEMORIES
from zagg.config.models_base import Bool, StrictModel, _Block

__all__ = ["Bounds", "SpatialPipeline", "TemporalBounds", "TemporalPipeline", "Worker"]

_MEMORIES = sorted(WORKER_MEMORIES)


class SpatialPipeline(StrictModel):
    """``pipeline`` of the point-cloud and raster kinds (absent means ``spatial``)."""

    type: Literal["spatial"] = Field(
        default="spatial", description="The point-cloud → grid engine (the default kind)."
    )


class TemporalPipeline(StrictModel):
    """``pipeline`` of the event-streaming engine (issue #12)."""

    type: Literal["temporal", "event"] = Field(
        description="``temporal`` or ``event``: the event-streaming engine over gridded collections."
    )


class Worker(_Block):
    """Lambda worker-size selector (issue #235): a pre-provisioned ``-<memory>[-disk]`` variant.

    Absent is the unsuffixed default function; ignored by the local backend.
    """

    memory: Annotated[int, WithJsonSchema({"type": "integer", "enum": _MEMORIES})] = Field(
        description=f"Memory in MB, one of {', '.join(map(str, _MEMORIES))} — the "
        "pre-provisioned variant sizes."
    )
    extra_disk: Bool = Field(
        default=False,
        description="Select the ``-disk`` twin with ``/tmp`` at Lambda's 10240 MB ceiling "
        "(issue #536; default false).",
    )

    @model_validator(mode="before")
    @classmethod
    def _shape(cls, data: Any) -> Any:
        if isinstance(data, dict):
            unknown = set(data) - {"memory", "extra_disk"}
            if unknown:
                raise ValueError(
                    f"Unknown worker keys: {sorted(unknown)} (allowed: 'memory', 'extra_disk')"
                )
        return data

    @field_validator("memory", mode="before")
    @classmethod
    def _memory(cls, v: Any) -> Any:
        # ``type(v) is int``: a bool or a whole float (``2048.0``) would reach
        # the function-name suffix unchanged (``process-shard-2048.0``).
        if type(v) is not int or v not in WORKER_MEMORIES:
            raise ValueError(
                f"must be one of {_MEMORIES} MB — the "
                f"pre-provisioned variant sizes (issue #235) — (got {v!r})"
            )
        return v


class TemporalBounds(StrictModel):
    """``bounds.temporal``: the run's granule time window."""

    start_date: Any = Field(description="ISO-8601 start of the granule window (inclusive).")
    end_date: Any = Field(
        description="ISO-8601 end of the granule window; a bare ``YYYY-MM-DD`` string is "
        "expanded to the end of that day."
    )

    @model_validator(mode="before")
    @classmethod
    def _both(cls, data: Any) -> Any:
        if isinstance(data, dict) and ("start_date" not in data or "end_date" not in data):
            raise ValueError("bounds.temporal requires start_date and end_date")
        return data


class Bounds(_Block):
    """Optional run bounds, recorded per run (never part of the product identity)."""

    _NULLABLE: ClassVar[frozenset[str]] = frozenset({"spatial"})

    temporal: TemporalBounds | None = Field(default=None, description="The granule time window.")
    spatial: Any = Field(default=None, description="Reserved: a spatial bound (unused today).")

    @model_validator(mode="before")
    @classmethod
    def _keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            unknown = set(data) - {"temporal", "spatial"}
            if unknown:
                raise ValueError(
                    f"Unknown bounds keys: {unknown} (allowed: {{'temporal', 'spatial'}})"
                )
        return data
