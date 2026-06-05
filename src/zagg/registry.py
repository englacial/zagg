"""Extensible plugin registries for zagg pipelines.

zagg's aggregation cores refer to capabilities *by name* — config says
``spatial_func: max``, ``temporal_reducer: first_landfall``, ``mask: ais`` —
and those names resolve to callables/classes through the registries here.
zagg seeds the registries with its own built-ins (see :mod:`zagg.temporal`);
external packages contribute domain-specific behaviour by calling the
``register_*`` helpers, typically from a ``register()`` hook advertised under
the ``zagg.plugins`` entry-point group and discovered lazily by
:func:`load_plugins`.

Because config (and Lambda JSON payloads) carry *names*, never callables, the
same name must resolve identically on the orchestrator and inside the worker —
so any package that registers names must be importable in both places, and
:func:`load_plugins` is invoked on both sides.

Extension points
----------------
- ``spatial_func``   : ``fn(var_t, combined_mask, cell_areas) -> float | tuple``
- ``reducer``        : streaming accumulator class with ``update()/finalize()``
- ``mask_provider``  : ``fn(event_mask_t, static_data, spec) -> combined_mask``
- ``field_transform``: ``fn(var, spec, static_data, context) -> var`` (anomaly,
  derivations such as ``_rainfall``)
- ``event_trigger``  : ``fn(event_mask, static_data, spec) -> trigger key(s)``
- ``reader``         : event/data reader object (see :mod:`zagg.temporal`)
- ``catalog_source`` : catalog adapter (build/load work units)
- ``credential_provider`` : ``fetch(region) -> dict`` credentials
"""

import importlib.metadata
import logging

logger = logging.getLogger(__name__)

# --- the registries ---------------------------------------------------------
SPATIAL_FUNCTIONS: dict = {}
TEMPORAL_REDUCERS: dict = {}
MASK_PROVIDERS: dict = {}
FIELD_TRANSFORMS: dict = {}
EVENT_TRIGGERS: dict = {}
READERS: dict = {}
CATALOG_SOURCES: dict = {}
CREDENTIAL_PROVIDERS: dict = {}

# kind -> (registry dict, human label)
_REGISTRIES: dict[str, tuple[dict, str]] = {
    "spatial_func": (SPATIAL_FUNCTIONS, "spatial function"),
    "reducer": (TEMPORAL_REDUCERS, "temporal reducer"),
    "mask_provider": (MASK_PROVIDERS, "mask provider"),
    "field_transform": (FIELD_TRANSFORMS, "field transform"),
    "event_trigger": (EVENT_TRIGGERS, "event trigger"),
    "reader": (READERS, "reader"),
    "catalog_source": (CATALOG_SOURCES, "catalog source"),
    "credential_provider": (CREDENTIAL_PROVIDERS, "credential provider"),
}


def _register(kind: str, name: str, obj, *, overwrite: bool):
    registry, label = _REGISTRIES[kind]
    if name in registry and not overwrite:
        raise ValueError(
            f"{label} '{name}' is already registered; pass overwrite=True to replace it"
        )
    registry[name] = obj
    return obj


def _make_register(kind: str):
    """Build a ``register_<kind>`` helper usable as a decorator or direct call."""

    def register(name: str, obj=None, *, overwrite: bool = False):
        if obj is not None:
            return _register(kind, name, obj, overwrite=overwrite)

        def decorator(func):
            return _register(kind, name, func, overwrite=overwrite)

        return decorator

    register.__name__ = f"register_{kind}"
    register.__qualname__ = f"register_{kind}"
    register.__doc__ = (
        f"Register a {_REGISTRIES[kind][1]} under ``name``.\n\n"
        "Usable directly, ``register_x('foo', obj)``, or as a decorator, "
        "``@register_x('foo')``. Pass ``overwrite=True`` to replace an existing entry."
    )
    return register


def _make_get(kind: str):
    def get(name: str):
        registry, label = _REGISTRIES[kind]
        try:
            return registry[name]
        except KeyError:
            raise ValueError(
                f"Unknown {label} '{name}' "
                f"(registered: {sorted(registry)}). "
                "If it is provided by a plugin, ensure the package is installed "
                "and exposes a 'zagg.plugins' entry point."
            ) from None

    get.__name__ = f"get_{kind}"
    get.__qualname__ = f"get_{kind}"
    get.__doc__ = f"Return the registered {_REGISTRIES[kind][1]} for ``name`` (raises ValueError if absent)."
    return get


# Generate the public register_*/get_* helpers for every registry kind.
register_spatial_func = _make_register("spatial_func")
register_reducer = _make_register("reducer")
register_mask_provider = _make_register("mask_provider")
register_field_transform = _make_register("field_transform")
register_event_trigger = _make_register("event_trigger")
register_reader = _make_register("reader")
register_catalog_source = _make_register("catalog_source")
register_credential_provider = _make_register("credential_provider")

get_spatial_func = _make_get("spatial_func")
get_reducer = _make_get("reducer")
get_mask_provider = _make_get("mask_provider")
get_field_transform = _make_get("field_transform")
get_event_trigger = _make_get("event_trigger")
get_reader = _make_get("reader")
get_catalog_source = _make_get("catalog_source")
get_credential_provider = _make_get("credential_provider")


# --- lazy entry-point discovery ---------------------------------------------
ENTRY_POINT_GROUP = "zagg.plugins"
_loaded = False
_loaded_plugins: set[str] = set()


def load_plugins(*, force: bool = False) -> set[str]:
    """Discover and register external plugins via the ``zagg.plugins`` entry point.

    Each entry point must resolve to a zero-argument ``register()`` callable that
    performs ``register_*`` calls. Discovery runs at most once per process (unless
    ``force``); a failing plugin is logged and skipped so it cannot abort a run.

    Returns
    -------
    set[str]
        Names of plugins successfully loaded so far.
    """
    global _loaded
    if _loaded and not force:
        return set(_loaded_plugins)
    _loaded = True

    try:
        eps = importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("Could not enumerate '%s' entry points: %s", ENTRY_POINT_GROUP, e)
        return set(_loaded_plugins)

    for ep in eps:
        if ep.name in _loaded_plugins:
            continue
        try:
            register = ep.load()
            register()
            _loaded_plugins.add(ep.name)
            logger.info("Loaded zagg plugin '%s'", ep.name)
        except Exception as e:
            logger.warning("Failed to load zagg plugin '%s': %s", ep.name, e)

    return set(_loaded_plugins)
