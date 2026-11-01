"""Render ``docs/template_grammar.md`` from the template grammar's JSON schema.

The page is generated, never hand-edited: ``tests/test_config_schema.py``
regenerates it and fails when the committed copy is stale, so the docs stay
in step with the models (issue #677 phase 5). Run from the repo root::

    uv run python tools/render_schema_docs.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from zagg.config.schema import json_schema

OUT = Path(__file__).resolve().parents[1] / "docs" / "template_grammar.md"

HEADER = """# Template grammar

*Generated from the pydantic models in `zagg.config` by
`tools/render_schema_docs.py` (issue #677) — do not edit by hand. Export the
machine-readable form with `python -m zagg.config --schema [--block <name>]`,
which also annotates the capability-named fields with the names registered in
the running environment (`x-zagg-registered`); this page is rendered without
those hints so it does not depend on which plugins are installed.*

A template is one of three config kinds, picked the way `validate_config`
branches: `pipeline.type: temporal|event` selects the temporal grammar,
`data_source.reader: raster` the raster grammar, anything else the spatial
point-cloud grammar. Every block refuses unknown keys. Capability-named
fields (`credentials_provider`, `spatial_func`, `temporal_reducer`, `mask`, …)
take registry names — hints in the exported schema, never a closed set.

The models (`validate_config`) are the authority where JSON Schema is looser:
it counts a whole float as an `integer` (`9.0` passes), which the models
refuse, so write integer knobs without a decimal point; and
`data_source.index` is an open mapping its backend validates.

"""


def _type_of(prop: dict[str, Any], defs: dict[str, Any]) -> str:
    if "$ref" in prop:
        return f"[`{prop['$ref'].rsplit('/', 1)[-1]}`](#{prop['$ref'].rsplit('/', 1)[-1].lower()})"
    for key in ("anyOf", "oneOf"):
        if key in prop:
            parts = [_type_of(p, defs) for p in prop[key]]
            return " \\| ".join(dict.fromkeys(parts))
    if "enum" in prop:
        return " \\| ".join(f"`{v!r}`" for v in prop["enum"])
    if "const" in prop:
        return f"`{prop['const']!r}`"
    t = prop.get("type")
    if t == "array":
        items = prop.get("items", {})
        return f"list of {_type_of(items, defs)}" if items else "list"
    if t == "object":
        extra = prop.get("additionalProperties")
        if isinstance(extra, dict):
            return f"mapping of name → {_type_of(extra, defs)}"
        return "mapping"
    if t is None:
        return "any"
    return f"`{t}`"


def _render_model(name: str, model: dict[str, Any], defs: dict[str, Any]) -> list[str]:
    lines = [f"### {name}", ""]
    if model.get("description"):
        lines += [model["description"].strip(), ""]
    props = model.get("properties", {})
    if not props:
        return lines + ["(no keys)", ""]
    required = set(model.get("required", []))
    lines += ["| key | type | description |", "|---|---|---|"]
    for key, prop in props.items():
        desc = (prop.get("description") or "").replace("\n", " ").replace("|", "\\|")
        req = " *(required)*" if key in required else ""
        lines.append(f"| `{key}`{req} | {_type_of(prop, defs)} | {desc} |")
    return lines + [""]


def render() -> str:
    schema = json_schema(registry_hints=False)
    defs = schema["$defs"]
    out = [HEADER, "## Config kinds", ""]
    for member in schema["oneOf"]:
        out.append(
            f"- [`{member['$ref'].rsplit('/', 1)[-1]}`](#{member['$ref'].rsplit('/', 1)[-1].lower()})"
        )
    out += ["", "## Models", ""]
    for name in sorted(defs):
        out += _render_model(name, defs[name], defs)
    return "\n".join(out).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    text = render()
    if argv == ["--check"]:
        current = OUT.read_text() if OUT.exists() else ""
        if current != text:
            sys.stderr.write(f"{OUT} is stale: run tools/render_schema_docs.py\n")
            return 1
        return 0
    OUT.write_text(text)
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
