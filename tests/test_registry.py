"""Tests for the zagg plugin registry."""

import pytest

from zagg import registry


@pytest.fixture(autouse=True)
def _restore_registries():
    """Snapshot every registry and restore it after each test.

    The registries are process-global module state; without this, names
    injected by these tests would leak into other suites (e.g. the
    built-in completeness checks in test_temporal).
    """
    snapshots = {kind: dict(reg) for kind, (reg, _) in registry._REGISTRIES.items()}
    try:
        yield
    finally:
        for kind, (reg, _) in registry._REGISTRIES.items():
            reg.clear()
            reg.update(snapshots[kind])


class TestRegisterAndGet:
    def test_direct_registration(self):
        sentinel = object()
        registry.register_spatial_func("test_direct_sf", sentinel)
        assert registry.get_spatial_func("test_direct_sf") is sentinel

    def test_decorator_registration(self):
        @registry.register_reducer("test_deco_reducer")
        class MyReducer:
            pass

        assert registry.get_reducer("test_deco_reducer") is MyReducer

    def test_duplicate_raises(self):
        registry.register_mask_provider("test_dup", object())
        with pytest.raises(ValueError, match="already registered"):
            registry.register_mask_provider("test_dup", object())

    def test_overwrite_allowed(self):
        first, second = object(), object()
        registry.register_event_trigger("test_ow", first)
        registry.register_event_trigger("test_ow", second, overwrite=True)
        assert registry.get_event_trigger("test_ow") is second

    def test_unknown_name_raises_helpful(self):
        with pytest.raises(ValueError, match="Unknown reader 'nope'"):
            registry.get_reader("nope")


class TestBuiltinsRegistered:
    def test_builtin_spatial_funcs_present(self):
        # Importing zagg.temporal seeds the registry with built-ins.
        import zagg.temporal  # noqa: F401

        for name in ("max", "min", "weighted_sum", "weighted_mean",
                     "max_gradient", "min_over_levels"):
            assert callable(registry.get_spatial_func(name))

    def test_builtin_reducers_present(self):
        import zagg.temporal  # noqa: F401

        for name in ("max", "min", "sum", "weighted_mean", "first_landfall"):
            assert registry.get_reducer(name) is not None

    def test_temporal_aliases_are_registry(self):
        import zagg.temporal as t

        assert t.SPATIAL_FUNCTIONS is registry.SPATIAL_FUNCTIONS
        assert t.TEMPORAL_REDUCERS is registry.TEMPORAL_REDUCERS


class TestLoadPlugins:
    def test_load_plugins_idempotent_and_safe(self):
        # No 'zagg.plugins' entry points are installed in the test env; this
        # should simply return without error and be safe to call twice.
        first = registry.load_plugins(force=True)
        second = registry.load_plugins()
        assert isinstance(first, set)
        assert isinstance(second, set)
