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

#: Grammar field name → registry kind whose entries are the hint list
#: (:data:`zagg.registry._REGISTRIES` keys).
CAPABILITY_FIELDS: dict[str, str] = {
    "credentials_provider": "credential_provider",
    "spatial_func": "spatial_func",
    "temporal_reducer": "reducer",
    "mask": "mask_provider",
    "transform": "field_transform",
    "trigger": "event_trigger",
    "reader": "reader",
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


def _annotate(node: Any, hints: dict[str, list[dict[str, Any]]]) -> None:
    """Walk a schema and attach the registered names to every capability field."""
    if isinstance(node, dict):
        props = node.get("properties")
        if isinstance(props, dict):
            for name, prop in props.items():
                kind = CAPABILITY_FIELDS.get(name)
                if kind is None or not isinstance(prop, dict) or kind not in hints:
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
        for value in node.values():
            _annotate(value, hints)
    elif isinstance(node, list):
        for value in node:
            _annotate(value, hints)


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
