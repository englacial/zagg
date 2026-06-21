"""Plugin registries for the temporal/event engine (issue #12, Phase 2).

zagg's temporal pipeline (``pipeline.type: temporal`` / ``"event"``) resolves
*capabilities* — spatial functions, accumulators, mask providers, etc. — by
*name* rather than by passing callables through the config. Names round-trip
through YAML and Lambda JSON payloads; callables do not. This module owns the
8 capability namespaces named in the June 2026 plan
(https://github.com/englacial/zagg/issues/12#issuecomment-4635480666):

================  =====================================================
Registry          What it holds
================  =====================================================
spatial_func      Per-timestep reductions over a 2-D field (e.g. ``max``)
reducer           Streaming accumulators across timesteps (e.g. ``Max``)
mask_provider     Producers of boolean masks (e.g. ``ais``, ``ocean``)
field_transform   Per-timestep transforms (e.g. ``monthly_anomaly``)
event_trigger     Predicates that fire on a specific timestep
reader            Multi-collection data readers
catalog_source    Catalog adapters (CMR, earthaccess, …)
credential_provider  Credential adapters (NSIDC, GES-DISC, EDL, …)
================  =====================================================

External packages contribute by declaring a ``zagg.plugins`` entry point that
points at a ``register()`` callable; the first call to any ``get_*`` /
``list_*`` helper triggers entry-point discovery once, lazily.

Built-in capabilities (the ``ais``/``ocean``/``full`` mask providers, the
``monthly_anomaly`` field transform, etc.) are registered when the later
phases land their owning modules (see PR #70 phases 4–7). Phase 2 ships the
seam alone — every registry starts empty until something registers into it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from importlib import metadata
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

# The eight registries from the issue #12 plan. Each maps name -> capability
# (a callable for *_func / *_provider / reducer / trigger / transform; the
# *_source / *_provider categories may hold classes — kept loosely typed as
# ``Any`` so the registry stays plugin-agnostic).
_SPATIAL_FUNCS: dict[str, Callable] = {}
_REDUCERS: dict[str, Any] = {}
_MASK_PROVIDERS: dict[str, Any] = {}
_FIELD_TRANSFORMS: dict[str, Callable] = {}
_EVENT_TRIGGERS: dict[str, Callable] = {}
_READERS: dict[str, Any] = {}
_CATALOG_SOURCES: dict[str, Any] = {}
_CREDENTIAL_PROVIDERS: dict[str, Any] = {}

_REGISTRIES: dict[str, dict[str, Any]] = {
    "spatial_func": _SPATIAL_FUNCS,
    "reducer": _REDUCERS,
    "mask_provider": _MASK_PROVIDERS,
    "field_transform": _FIELD_TRANSFORMS,
    "event_trigger": _EVENT_TRIGGERS,
    "reader": _READERS,
    "catalog_source": _CATALOG_SOURCES,
    "credential_provider": _CREDENTIAL_PROVIDERS,
}

# Entry-point group external plugins declare in their ``pyproject.toml``::
#
#     [project.entry-points."zagg.plugins"]
#     antarctic_ar = "zagg_ar:register"
_ENTRY_POINT_GROUP = "zagg.plugins"

# Discovery is idempotent across processes within a single interpreter; we
# track it so repeated ``get_*`` / ``list_*`` calls don't re-iterate
# ``importlib.metadata.entry_points`` every time.
_DISCOVERED = False


# ---------------------------------------------------------------------------
# Core register / get / list helpers (private; the public surface wraps these
# per-registry below so error messages and type hints stay specific).
# ---------------------------------------------------------------------------


def _register(registry: dict[str, Any], kind: str, name: str, obj: Any, *, replace: bool) -> Any:
    if not isinstance(name, str) or not name:
        raise ValueError(f"{kind} name must be a non-empty string, got {name!r}")
    if not replace and name in registry:
        raise ValueError(f"{kind} {name!r} is already registered; pass replace=True to override")
    registry[name] = obj
    return obj


def _get(registry: dict[str, Any], kind: str, name: str) -> Any:
    _ensure_discovered()
    try:
        return registry[name]
    except KeyError:
        known = sorted(registry)
        raise KeyError(f"Unknown {kind} {name!r}; registered: {known}") from None


def _list(registry: dict[str, Any]) -> list[str]:
    _ensure_discovered()
    return sorted(registry)


def _decorator(registry: dict[str, Any], kind: str, name: str, *, replace: bool) -> Callable:
    """Return a decorator that registers the decorated object under ``name``."""

    def _decorate(obj: Callable) -> Callable:
        _register(registry, kind, name, obj, replace=replace)
        return obj

    return _decorate


# ---------------------------------------------------------------------------
# Lazy entry-point discovery
# ---------------------------------------------------------------------------


def _ensure_discovered() -> None:
    """Discover ``zagg.plugins`` entry points exactly once per interpreter.

    Each entry point must resolve to a callable that, when invoked with no
    arguments, registers its capabilities through the helpers below. A
    failure in any one plugin is logged at WARNING but does not crash the
    discovery pass — the rest still load.
    """
    global _DISCOVERED
    if _DISCOVERED:
        return
    # Mark discovered *before* loading so a plugin that calls back into a
    # ``get_*`` helper during its ``register()`` doesn't recurse.
    _DISCOVERED = True
    try:
        eps: Iterable[metadata.EntryPoint] = metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # pragma: no cover - importlib.metadata API surface is narrow
        logger.exception("zagg.plugins entry-point lookup failed; no plugins loaded")
        return
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


def discover_plugins(*, force: bool = False) -> None:
    """Trigger entry-point discovery explicitly.

    Discovery is normally lazy (deferred until the first ``get_*`` / ``list_*``
    call). Tests and the Lambda handler call this at a deterministic point so
    failures surface early.

    Parameters
    ----------
    force : bool
        Re-run discovery even if it has already happened. Useful in tests
        that install a plugin after import.
    """
    global _DISCOVERED
    if force:
        _DISCOVERED = False
    _ensure_discovered()


# ---------------------------------------------------------------------------
# Public per-registry surface. The repetition is deliberate: each kind gets
# its own decorator + register + get + list trio so call sites read clearly
# (``register_spatial_func("max")`` not ``register("spatial_func", "max")``)
# and ``KeyError`` messages name the kind that was missed.
# ---------------------------------------------------------------------------


def register_spatial_func(name: str, func: Callable | None = None, *, replace: bool = False):
    """Register or decorate a spatial function (e.g. ``max`` over a 2-D field).

    Usable as a decorator (``@register_spatial_func("max")``) or as a direct
    call (``register_spatial_func("max", my_func)``).
    """
    if func is None:
        return _decorator(_SPATIAL_FUNCS, "spatial_func", name, replace=replace)
    return _register(_SPATIAL_FUNCS, "spatial_func", name, func, replace=replace)


def get_spatial_func(name: str) -> Callable:
    return _get(_SPATIAL_FUNCS, "spatial_func", name)


def list_spatial_funcs() -> list[str]:
    return _list(_SPATIAL_FUNCS)


def register_reducer(name: str, factory: Any = None, *, replace: bool = False):
    """Register a streaming reducer/accumulator (e.g. ``Max``, ``WeightedMean``).

    The registered value is typically a class or zero-arg factory that
    produces a fresh accumulator per spec.
    """
    if factory is None:
        return _decorator(_REDUCERS, "reducer", name, replace=replace)
    return _register(_REDUCERS, "reducer", name, factory, replace=replace)


def get_reducer(name: str) -> Any:
    return _get(_REDUCERS, "reducer", name)


def list_reducers() -> list[str]:
    return _list(_REDUCERS)


def register_mask_provider(name: str, provider: Any = None, *, replace: bool = False):
    """Register a mask provider (e.g. ``ais``, ``ocean``, ``full``).

    A mask provider is a callable that takes the static-data dict + an
    event/spec context and returns a boolean mask aligned with the spatial
    grid being reduced.
    """
    if provider is None:
        return _decorator(_MASK_PROVIDERS, "mask_provider", name, replace=replace)
    return _register(_MASK_PROVIDERS, "mask_provider", name, provider, replace=replace)


def get_mask_provider(name: str) -> Any:
    return _get(_MASK_PROVIDERS, "mask_provider", name)


def list_mask_providers() -> list[str]:
    return _list(_MASK_PROVIDERS)


def register_field_transform(
    name: str, transform: Callable | None = None, *, replace: bool = False
):
    """Register a per-timestep field transform (e.g. ``monthly_anomaly``,
    ``negate``, custom derivations).
    """
    if transform is None:
        return _decorator(_FIELD_TRANSFORMS, "field_transform", name, replace=replace)
    return _register(_FIELD_TRANSFORMS, "field_transform", name, transform, replace=replace)


def get_field_transform(name: str) -> Callable:
    return _get(_FIELD_TRANSFORMS, "field_transform", name)


def list_field_transforms() -> list[str]:
    return _list(_FIELD_TRANSFORMS)


def register_event_trigger(name: str, predicate: Callable | None = None, *, replace: bool = False):
    """Register an event trigger predicate (e.g. ``first_landfall``).

    A trigger inspects the current timestep + accumulator state and returns
    True the moment its condition fires; the engine then promotes the spec
    from "warming up" to "capturing".
    """
    if predicate is None:
        return _decorator(_EVENT_TRIGGERS, "event_trigger", name, replace=replace)
    return _register(_EVENT_TRIGGERS, "event_trigger", name, predicate, replace=replace)


def get_event_trigger(name: str) -> Callable:
    return _get(_EVENT_TRIGGERS, "event_trigger", name)


def list_event_triggers() -> list[str]:
    return _list(_EVENT_TRIGGERS)


def register_reader(name: str, reader: Any = None, *, replace: bool = False):
    """Register a data reader (e.g. a MERRA-2 multi-collection xarray loader).

    Readers are the orchestrator/worker-side IO surface; the engine asks them
    to open an event + its timesteps + collections.
    """
    if reader is None:
        return _decorator(_READERS, "reader", name, replace=replace)
    return _register(_READERS, "reader", name, reader, replace=replace)


def get_reader(name: str) -> Any:
    return _get(_READERS, "reader", name)


def list_readers() -> list[str]:
    return _list(_READERS)


def register_catalog_source(name: str, source: Any = None, *, replace: bool = False):
    """Register a catalog source (e.g. ``cmr``, ``earthaccess``)."""
    if source is None:
        return _decorator(_CATALOG_SOURCES, "catalog_source", name, replace=replace)
    return _register(_CATALOG_SOURCES, "catalog_source", name, source, replace=replace)


def get_catalog_source(name: str) -> Any:
    return _get(_CATALOG_SOURCES, "catalog_source", name)


def list_catalog_sources() -> list[str]:
    return _list(_CATALOG_SOURCES)


def register_credential_provider(name: str, provider: Any = None, *, replace: bool = False):
    """Register a credential provider (e.g. ``nsidc``, ``gesdisc``, ``edl``)."""
    if provider is None:
        return _decorator(_CREDENTIAL_PROVIDERS, "credential_provider", name, replace=replace)
    return _register(_CREDENTIAL_PROVIDERS, "credential_provider", name, provider, replace=replace)


def get_credential_provider(name: str) -> Any:
    return _get(_CREDENTIAL_PROVIDERS, "credential_provider", name)


def list_credential_providers() -> list[str]:
    return _list(_CREDENTIAL_PROVIDERS)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def registry_snapshot() -> dict[str, list[str]]:
    """Return a name-only snapshot of every registry, useful for diagnostics
    and the eventual MCP ``describe_products`` tool (issue #59).
    """
    _ensure_discovered()
    return {kind: sorted(reg) for kind, reg in _REGISTRIES.items()}
