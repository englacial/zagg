"""Shared pydantic plumbing for the template grammar (issue #677).

The strict base model, the boolean-safe scalar types, and the tagged-union
helper every block module builds on. Kept apart from :mod:`zagg.config.models`
(the root union) so the block modules import downward only.

Numbers are typed through :data:`Number` / :data:`Int` and friends rather
than bare ``int``/``float``: pydantic's lax mode accepts ``True`` for an int
and ``5.0`` for an int, and converts an int into a float, any of which would
either admit a value the old validators refused or change the type (and so
the canonical JSON the semantic hash reads) of a value the author wrote.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Union

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    WithJsonSchema,
    model_validator,
)


class StrictModel(BaseModel):
    """Base for every grammar model: unknown keys are errors, never ignored."""

    model_config = ConfigDict(extra="forbid")


def _no_explicit_null(data: Any, keys: Any) -> Any:
    """Refuse an explicit ``null`` on ``keys``: consumers read ``.get(key, default)``,
    which returns ``None`` for an explicit null instead of the default."""
    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key] is None:
                raise ValueError(f"{key} must not be null (omit the key for the default)")
    return data


class _Block(StrictModel):
    """A grammar block whose optional keys may be omitted but never set to ``null``."""

    #: Optional keys for which an explicit ``null`` is meaningful; the exported
    #: schema keeps their ``null`` branch.
    _NULLABLE: ClassVar[frozenset[str]] = frozenset()
    #: Optional keys whose own validator refuses ``null`` with a sharper
    #: message: exempt from :meth:`_no_null`, but not nullable in the schema.
    _NULL_CHECKED: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="before")
    @classmethod
    def _no_null(cls, data: Any) -> Any:
        skip = cls._NULLABLE | cls._NULL_CHECKED
        keys = (k for k, f in cls.model_fields.items() if not f.is_required() and k not in skip)
        return _no_explicit_null(data, keys)

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: Any, handler: Any) -> Any:
        """Export what :meth:`_no_null` enforces: an optional key outside
        :attr:`_NULLABLE` loses its ``{"type": "null"}`` branch (a one-member
        ``anyOf`` collapses), so the schema and the model agree on ``null``."""
        json_schema = handler(core_schema)
        target = handler.resolve_ref_schema(json_schema)
        for key, prop in target.get("properties", {}).items():
            field = cls.model_fields.get(key)
            if field is None or field.is_required() or key in cls._NULLABLE:
                continue
            members = [m for m in prop.get("anyOf", ()) if m != {"type": "null"}]
            if len(members) == len(prop.get("anyOf", ())):
                continue
            del prop["anyOf"]
            if len(members) == 1:
                prop.update(members[0])
            else:
                prop["anyOf"] = members
            if "default" in prop and prop["default"] is None:
                del prop["default"]
        return json_schema


def _check_number(v: Any) -> Any:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError(f"must be numeric (got {v!r})")
    return v


def _check_int(v: Any) -> Any:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"must be an integer (got {v!r})")
    return v


def _check_pos_int(v: Any) -> Any:
    if isinstance(v, bool) or not isinstance(v, int) or v < 1:
        raise ValueError(f"must be an integer >= 1 (got {v!r})")
    return v


def _check_nonneg_int(v: Any) -> Any:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise ValueError(f"must be a non-negative int (got {v!r})")
    return v


def _check_bool(v: Any) -> Any:
    if not isinstance(v, bool):
        raise ValueError(f"must be a boolean (got {v!r})")
    return v


#: An int or a float, never a bool, with the author's type preserved (an int
#: stays an int so the canonical JSON the hash reads is unchanged).
Number = Annotated[
    Union[int, float], BeforeValidator(_check_number), WithJsonSchema({"type": "number"})
]
#: An int that is not a bool and not a whole float.
Int = Annotated[int, BeforeValidator(_check_int)]
#: An int >= 1 (the fan-out widths and buffer bounds).
PosInt = Annotated[
    int, BeforeValidator(_check_pos_int), WithJsonSchema({"type": "integer", "minimum": 1})
]
#: An int >= 0.
NonNegInt = Annotated[
    int, BeforeValidator(_check_nonneg_int), WithJsonSchema({"type": "integer", "minimum": 0})
]
#: A real boolean: pydantic's lax ``bool`` would accept ``"true"`` and ``1``.
Bool = Annotated[bool, BeforeValidator(_check_bool), WithJsonSchema({"type": "boolean"})]
#: A string with at least one character.
NonEmptyStr = Annotated[str, Field(min_length=1)]

#: The :class:`pydantic.Tag` names the grammar's tagged unions use. A tag is
#: a member label, not a key the author wrote, so the error renderer drops a
#: location element only on exact membership; the angle brackets keep a tag
#: from colliding with an author's mapping key (e.g. a variable ``x-config``).
TAGS = frozenset(
    {
        "<path-form>",
        "<column-form>",
        "<synthesize-form>",
        "<asset-form>",
        "<list-form>",
        "<mapping-form>",
        "<structured-filter>",
        "<expression-filter>",
        "<spatial-config>",
        "<raster-config>",
        "<temporal-config>",
        "<healpix-grid>",
        "<rectilinear-grid>",
        "<point-window>",
        "<range-window>",
        "<icechunk-flag>",
        "<icechunk-block>",
        "<pyramid-off>",
        "<pyramid-block>",
    }
)


def _tag_of(v: Any, model_tags: dict[type, str], dict_tag: Any) -> str:
    """Tag ``v`` for a callable discriminator: by class for a built instance
    (serialization), by shape for a raw input (validation)."""
    for cls, tag in model_tags.items():
        if isinstance(v, cls):
            return tag
    return dict_tag(v)
