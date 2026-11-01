"""JSON-schema export of the template grammar (issue #677 phase 5).

:func:`json_schema` is the MCP seam (issue #59): the ``build_template`` tool
hands a model this schema, and every ``Field(description=...)`` in the
grammar modules is the documentation it carries. Capability-named fields
(``credentials_provider``, ``spatial_func``, ``temporal_reducer``, ``mask``,
…) are annotated with the names the registry knows at export time
(:func:`zagg.registry.describe_all`) as **hints**, never as a closed
``enum``: plugins register more, and a typo surfaces as ``UnknownCapability``
at run time rather than a load-time guess about installed plugins (issue #64).
"""

from __future__ import annotations

import json
from typing import Any, Union

from pydantic import TypeAdapter

from zagg.config.models import ConfigModel
from zagg.config.models_aggregation import Aggregation, TemporalAggregation
from zagg.config.models_output import Output, TemporalOutput
from zagg.config.models_run import Bounds, SpatialPipeline, TemporalPipeline, Worker
from zagg.config.models_source import CollectionDataSource, H5DataSource, RasterDataSource

__all__ = ["BLOCKS", "CAPABILITY_FIELDS", "json_schema", "schema_json"]

#: Block name → the type whose schema ``--block <name>`` exports. A block that
#: differs by pipeline kind exports as the union of its kinds.
BLOCKS: dict[str, Any] = {
    "data_source": Union[H5DataSource, RasterDataSource, CollectionDataSource],
    "aggregation": Union[Aggregation, TemporalAggregation],
    "output": Union[Output, TemporalOutput],
    "pipeline": Union[SpatialPipeline, TemporalPipeline],
    "worker": Worker,
    "bounds": Bounds,
}

#: ``(model, field)`` → registry kind whose entries are the hint list
#: (:data:`zagg.registry._REGISTRIES` keys). Keyed by the schema's ``$defs``
#: name, not the field name alone: ``reader`` resolves through the registry on
#: the collection source only (h5 reads ``h5coro``, raster reads ``raster``).
CAPABILITY_FIELDS: dict[tuple[str, str], str] = {
    **{
        (model, "credentials_provider"): "credential_provider"
        for model in ("H5DataSource", "RasterDataSource", "CollectionDataSource")
    },
    ("CollectionDataSource", "reader"): "reader",
    ("TemporalFieldSpec", "spatial_func"): "spatial_func",
    ("TemporalFieldSpec", "temporal_reducer"): "reducer",
    ("TemporalFieldSpec", "mask"): "mask_provider",
    ("TemporalFieldSpec", "transform"): "field_transform",
    ("TemporalFieldSpec", "trigger"): "event_trigger",
}

#: The vendor-prefixed key the hints ride under (JSON Schema ignores unknown
#: ``x-`` keywords; a reader that wants the list finds it by name).
HINT_KEY = "x-zagg-registered"


def _registry_hints() -> dict[str, list[dict[str, Any]]]:
    """The registry snapshot with the built-ins loaded.

    Built-in capabilities register on import of their modules (``zagg.auth``
    for the credential providers, ``zagg.temporal`` for the event engine's
    reducers/masks/transforms/readers), which the schema export need not have
    imported yet; a module whose optional dependencies are missing simply
    contributes no hints.
    """
    import importlib

    from zagg.registry import describe_all

    for module in ("zagg.auth", "zagg.temporal"):
        try:
            importlib.import_module(module)
        except ImportError:  # pragma: no cover - optional engine deps absent
            continue
    return describe_all()


def _annotate(schema: dict[str, Any], hints: dict[str, list[dict[str, Any]]]) -> None:
    """Attach the registered names to every capability field, model by model.

    Models are found by name: the ``$defs`` entries, plus the root when the
    export is a single model (pydantic titles it with the class name).
    """
    models = dict(schema.get("$defs", {}))
    if "title" in schema:
        models.setdefault(schema["title"], schema)
    for (model, field), kind in CAPABILITY_FIELDS.items():
        prop = models.get(model, {}).get("properties", {}).get(field)
        if not isinstance(prop, dict) or kind not in hints:
            continue
        entries = hints[kind]
        prop[HINT_KEY] = [
            {"name": e["name"], "description": e.get("description", "")} for e in entries
        ]
        names = ", ".join(e["name"] for e in entries)
        if names:
            prop["description"] = (
                f"{prop.get('description', '')} Registered {kind} names: {names}."
            ).strip()


def json_schema(block: str | None = None, *, registry_hints: bool = True) -> dict[str, Any]:
    """The template grammar as a JSON schema (draft 2020-12, pydantic's dialect).

    Parameters
    ----------
    block : str, optional
        Export one block (``data_source``, ``aggregation``, ``output``, …)
        instead of the whole config; a block whose grammar differs by pipeline
        kind exports as the union of its kinds.
    registry_hints : bool
        Annotate capability-named fields with the registered names
        (:data:`HINT_KEY`, plus a sentence in the description). Off for a
        hint-free schema (e.g. a fixture that must not depend on installed
        plugins).
    """
    if block is None:
        target = ConfigModel
    elif block in BLOCKS:
        target = BLOCKS[block]
    else:
        raise ValueError(f"unknown block {block!r} (one of {sorted(BLOCKS)})")
    schema = TypeAdapter(target).json_schema(mode="validation")
    if registry_hints:
        _annotate(schema, _registry_hints())
    return schema


def schema_json(block: str | None = None, *, registry_hints: bool = True, indent: int = 2) -> str:
    """:func:`json_schema` serialized with sorted keys (stable for diffs and docs)."""
    return json.dumps(
        json_schema(block, registry_hints=registry_hints), indent=indent, sort_keys=True
    )
