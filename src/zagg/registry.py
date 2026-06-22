"""Capability registry — the shared name→object vocabulary (issue #64).

zagg resolves *capabilities* — spatial functions, reducers, mask providers,
etc. — by **name** rather than by passing callables through a config. Names
round-trip through YAML, Lambda JSON payloads, and the MCP `describe_products`
tool (#59); callables do not. This module owns the eight capability namespaces
named in the June 2026 plan
(https://github.com/englacial/zagg/issues/12#issuecomment-4635480666):

==================  ===================================================
Registry            What it holds
==================  ===================================================
spatial_func        Per-timestep reductions over a 2-D field (``max``)
reducer             Streaming accumulators across timesteps (``Max``)
mask_provider       Producers of boolean masks (``ais``, ``ocean``)
field_transform     Per-timestep transforms (``monthly_anomaly``)
event_trigger       Predicates that fire on a specific timestep
reader              Multi-collection data readers
catalog_source      Catalog adapters (CMR, earthaccess, …)
credential_provider Credential adapters (NSIDC, GES-DISC, EDL, …)
==================  ===================================================

Design (issue #64, locked with @espg):

- **Strings, never callables.** The registry maps a public *name* to a private
  *object*; configs/payloads carry the name and re-resolve the object at the
  execution site. The name is the stable identifier, the object is versioned.
- **`Registry[T]` class core** with `register` / `get` / `list` / `describe`.
- **Optional per-entry schema.** Each registration may carry a one-line
  ``description`` and an optional ``schema`` (a pydantic model / JSON schema for
  the entry's YAML args). Entries without a schema pay nothing; ``describe``
  surfaces it when present. This is the additive "(B) now, (C) where it earns
  its keep" path to the full MCP parameter surface.
- **Lazy entry-point discovery.** External packages contribute by declaring a
  ``zagg.plugins`` entry point pointing at a zero-arg ``register()`` callable;
  the first ``get`` / ``list`` triggers discovery once.
- **`UnknownCapability`** carries the kind + the sorted ``available`` names so a
  miss is a clean, propagatable error (good "did you mean…" surface for MCP).

Built-in capabilities register when the later phases land their owning modules;
this module ships the seam alone — every registry starts empty.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import metadata
from typing import Any, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Entry-point group external plugins declare in their ``pyproject.toml``::
#
#     [project.entry-points."zagg.plugins"]
#     antarctic_ar = "zagg_ar:register"
_ENTRY_POINT_GROUP = "zagg.plugins"


class UnknownCapability(KeyError):
    """Raised when a capability name is not registered for its kind.

    Subclasses ``KeyError`` so existing ``except KeyError`` paths still catch
    it, while carrying ``kind`` and the sorted ``available`` names for a
    diagnosable message (and a clean error type for the MCP server to relay).
    """

    def __init__(self, name: str, kind: str, available: Iterable[str]):
        self.name = name
        self.kind = kind
        self.available = list(available)
        super().__init__(f"Unknown {kind} {name!r}; registered: {self.available}")

    def __str__(self) -> str:  # KeyError stringifies via repr(); override for readability
        return f"Unknown {self.kind} {self.name!r}; registered: {self.available}"


@dataclass(frozen=True)
class Entry(Generic[T]):
    """One registered capability: its name, object, and optional metadata."""

    name: str
    obj: T
    description: str = ""
    schema: Any | None = None


class Registry(Generic[T]):
    """A single named capability namespace (one of the eight ``kind``\\ s).

    Use as a decorator (``@SPATIAL_FUNCS.register("max")``) or a direct call
    (``SPATIAL_FUNCS.register("max", my_func)``). ``get`` / ``list`` /
    ``describe`` trigger lazy entry-point discovery on first use.
    """

    def __init__(self, kind: str):
        self.kind = kind
        self._entries: dict[str, Entry[T]] = {}

    def register(
        self,
        name: str,
        obj: T | None = None,
        *,
        description: str = "",
        schema: Any | None = None,
        replace: bool = False,
    ):
        """Register ``obj`` under ``name`` (or return a decorator if omitted)."""
        if obj is None:
            def _decorate(target: T) -> T:
                self._set(name, target, description, schema, replace)
                return target

            return _decorate
        self._set(name, obj, description, schema, replace)
        return obj

    def _set(self, name: str, obj: T, description: str, schema: Any | None, replace: bool) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"{self.kind} name must be a non-empty string, got {name!r}")
        if not replace and name in self._entries:
            raise ValueError(
                f"{self.kind} {name!r} is already registered; pass replace=True to override"
            )
        self._entries[name] = Entry(name, obj, description, schema)

    def get(self, name: str) -> T:
        _ensure_discovered()
        try:
            return self._entries[name].obj
        except KeyError:
            raise UnknownCapability(name, self.kind, sorted(self._entries)) from None

    def list(self) -> list[str]:
        _ensure_discovered()
        return sorted(self._entries)

    def describe(self, name: str) -> dict[str, Any]:
        """Return ``{name, kind, description, schema?}`` for one entry."""
        _ensure_discovered()
        try:
            entry = self._entries[name]
        except KeyError:
            raise UnknownCapability(name, self.kind, sorted(self._entries)) from None
        return _entry_dict(entry, self.kind)

    def describe_all(self) -> list[dict[str, Any]]:
        """Return one ``describe`` dict per entry, name-sorted."""
        _ensure_discovered()
        return [_entry_dict(self._entries[name], self.kind) for name in sorted(self._entries)]

    def __contains__(self, name: object) -> bool:
        _ensure_discovered()
        return name in self._entries


def _entry_dict(entry: Entry, kind: str) -> dict[str, Any]:
    out: dict[str, Any] = {"name": entry.name, "kind": kind, "description": entry.description}
    if entry.schema is not None:
        out["schema"] = entry.schema
    return out


# ---------------------------------------------------------------------------
# The eight registries
# ---------------------------------------------------------------------------

SPATIAL_FUNCS: Registry[Callable] = Registry("spatial_func")
REDUCERS: Registry[Any] = Registry("reducer")
MASK_PROVIDERS: Registry[Any] = Registry("mask_provider")
FIELD_TRANSFORMS: Registry[Callable] = Registry("field_transform")
EVENT_TRIGGERS: Registry[Callable] = Registry("event_trigger")
READERS: Registry[Any] = Registry("reader")
CATALOG_SOURCES: Registry[Any] = Registry("catalog_source")
CREDENTIAL_PROVIDERS: Registry[Any] = Registry("credential_provider")

_REGISTRIES: dict[str, Registry] = {
    reg.kind: reg
    for reg in (
        SPATIAL_FUNCS,
        REDUCERS,
        MASK_PROVIDERS,
        FIELD_TRANSFORMS,
        EVENT_TRIGGERS,
        READERS,
        CATALOG_SOURCES,
        CREDENTIAL_PROVIDERS,
    )
}


# ---------------------------------------------------------------------------
# Lazy entry-point discovery
# ---------------------------------------------------------------------------

# ``_DISCOVERED`` flips True after a successful entry-point sweep so repeated
# ``get`` / ``list`` calls don't re-iterate ``entry_points`` every time.
# ``_DISCOVERING`` is a re-entrancy guard: if a plugin's ``register()`` reaches
# a ``get`` during loading, we return early without re-running the sweep.
_DISCOVERED = False
_DISCOVERING = False


def _ensure_discovered() -> None:
    """Discover ``zagg.plugins`` entry points exactly once per interpreter.

    Each entry point must resolve to a zero-arg callable that registers its
    capabilities. A failure in one plugin's ``ep.load()`` / ``register()`` is
    logged at ERROR and skipped — the rest still load. If
    ``entry_points()`` itself raises, ``_DISCOVERED`` stays False so the next
    ``get`` / ``list`` retries (transient backport failures don't degrade the
    seam permanently).
    """
    global _DISCOVERED, _DISCOVERING
    if _DISCOVERED or _DISCOVERING:
        return
    _DISCOVERING = True
    try:
        try:
            eps: Iterable[metadata.EntryPoint] = metadata.entry_points(group=_ENTRY_POINT_GROUP)
        except Exception:
            logger.exception(
                "zagg.plugins entry-point lookup failed; will retry on next get/list"
            )
            return  # Leave _DISCOVERED=False so subsequent calls retry.
        # Flip the persistent flag before invoking plugins so a plugin's
        # ``register()`` calling back through ``get`` short-circuits on
        # ``_DISCOVERED`` and never re-enters the sweep.
        _DISCOVERED = True
        for ep in eps:
            try:
                register_fn = ep.load()
            except Exception:
                logger.exception("Failed to load zagg.plugins entry point %r", ep.name)
                continue
            try:
                register_fn()
            except Exception:
                logger.exception("zagg.plugins entry point %r raised on register()", ep.name)
    finally:
        _DISCOVERING = False


def discover_plugins(*, force: bool = False) -> None:
    """Trigger entry-point discovery explicitly.

    Discovery is normally lazy (deferred to the first ``get`` / ``list``). The
    Lambda handler calls this at import so plugin-load failures surface in
    cold-start logs rather than mid-invocation. ``force=True`` re-runs the
    sweep (intended for tests that patch ``entry_points`` before first
    discovery); re-running against already-registered plugins trips the
    duplicate-name guard — use ``register(..., replace=True)`` to overwrite.
    """
    global _DISCOVERED
    if force:
        _DISCOVERED = False
    _ensure_discovered()


# ---------------------------------------------------------------------------
# Diagnostics + MCP surface
# ---------------------------------------------------------------------------


def registry_snapshot() -> dict[str, list[str]]:
    """Name-only snapshot of every registry (diagnostics; coarse MCP view)."""
    _ensure_discovered()
    return {kind: reg.list() for kind, reg in _REGISTRIES.items()}


def describe_all() -> dict[str, list[dict[str, Any]]]:
    """Structured snapshot the MCP ``describe_products`` tool (#59) consumes.

    ``{kind: [{name, kind, description, schema?}, …]}`` — a superset of
    ``registry_snapshot`` that carries each entry's description and, where the
    registrant supplied one, its argument ``schema``.
    """
    _ensure_discovered()
    return {kind: reg.describe_all() for kind, reg in _REGISTRIES.items()}


# ---------------------------------------------------------------------------
# Per-kind functional wrappers
#
# Thin sugar over the ``Registry`` instances so call sites read as
# ``register_spatial_func("max")`` and miss messages name the kind. Each pair
# threads ``description`` / ``schema`` straight through to ``Registry.register``.
# ---------------------------------------------------------------------------


def register_spatial_func(name, func=None, *, description="", schema=None, replace=False):
    """Register/decorate a spatial function (per-timestep 2-D reduction)."""
    return SPATIAL_FUNCS.register(
        name, func, description=description, schema=schema, replace=replace
    )


def get_spatial_func(name: str) -> Callable:
    return SPATIAL_FUNCS.get(name)


def list_spatial_funcs() -> list[str]:
    return SPATIAL_FUNCS.list()


def register_reducer(name, factory=None, *, description="", schema=None, replace=False):
    """Register a streaming reducer/accumulator (class or zero-arg factory)."""
    return REDUCERS.register(name, factory, description=description, schema=schema, replace=replace)


def get_reducer(name: str) -> Any:
    return REDUCERS.get(name)


def list_reducers() -> list[str]:
    return REDUCERS.list()


def register_mask_provider(name, provider=None, *, description="", schema=None, replace=False):
    """Register a mask provider (``ais``, ``ocean``, ``full``)."""
    return MASK_PROVIDERS.register(
        name, provider, description=description, schema=schema, replace=replace
    )


def get_mask_provider(name: str) -> Any:
    return MASK_PROVIDERS.get(name)


def list_mask_providers() -> list[str]:
    return MASK_PROVIDERS.list()


def register_field_transform(name, transform=None, *, description="", schema=None, replace=False):
    """Register a per-timestep field transform (``monthly_anomaly``)."""
    return FIELD_TRANSFORMS.register(
        name, transform, description=description, schema=schema, replace=replace
    )


def get_field_transform(name: str) -> Callable:
    return FIELD_TRANSFORMS.get(name)


def list_field_transforms() -> list[str]:
    return FIELD_TRANSFORMS.list()


def register_event_trigger(name, predicate=None, *, description="", schema=None, replace=False):
    """Register an event-trigger predicate (``first_landfall``)."""
    return EVENT_TRIGGERS.register(
        name, predicate, description=description, schema=schema, replace=replace
    )


def get_event_trigger(name: str) -> Callable:
    return EVENT_TRIGGERS.get(name)


def list_event_triggers() -> list[str]:
    return EVENT_TRIGGERS.list()


def register_reader(name, reader=None, *, description="", schema=None, replace=False):
    """Register a data reader (multi-collection xarray loader)."""
    return READERS.register(name, reader, description=description, schema=schema, replace=replace)


def get_reader(name: str) -> Any:
    return READERS.get(name)


def list_readers() -> list[str]:
    return READERS.list()


def register_catalog_source(name, source=None, *, description="", schema=None, replace=False):
    """Register a catalog source (``cmr``, ``earthaccess``)."""
    return CATALOG_SOURCES.register(
        name, source, description=description, schema=schema, replace=replace
    )


def get_catalog_source(name: str) -> Any:
    return CATALOG_SOURCES.get(name)


def list_catalog_sources() -> list[str]:
    return CATALOG_SOURCES.list()


def register_credential_provider(name, provider=None, *, description="", schema=None, replace=False):
    """Register a credential provider (``nsidc``, ``gesdisc``, ``edl``)."""
    return CREDENTIAL_PROVIDERS.register(
        name, provider, description=description, schema=schema, replace=replace
    )


def get_credential_provider(name: str) -> Any:
    return CREDENTIAL_PROVIDERS.get(name)


def list_credential_providers() -> list[str]:
    return CREDENTIAL_PROVIDERS.list()


__all__ = [
    "Entry",
    "Registry",
    "UnknownCapability",
    "SPATIAL_FUNCS",
    "REDUCERS",
    "MASK_PROVIDERS",
    "FIELD_TRANSFORMS",
    "EVENT_TRIGGERS",
    "READERS",
    "CATALOG_SOURCES",
    "CREDENTIAL_PROVIDERS",
    "describe_all",
    "registry_snapshot",
    "discover_plugins",
    "register_spatial_func",
    "get_spatial_func",
    "list_spatial_funcs",
    "register_reducer",
    "get_reducer",
    "list_reducers",
    "register_mask_provider",
    "get_mask_provider",
    "list_mask_providers",
    "register_field_transform",
    "get_field_transform",
    "list_field_transforms",
    "register_event_trigger",
    "get_event_trigger",
    "list_event_triggers",
    "register_reader",
    "get_reader",
    "list_readers",
    "register_catalog_source",
    "get_catalog_source",
    "list_catalog_sources",
    "register_credential_provider",
    "get_credential_provider",
    "list_credential_providers",
]
