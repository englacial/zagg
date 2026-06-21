"""Tests for the capability registries (issue #12, Phase 2)."""

from __future__ import annotations

import pytest

import zagg.registry as registry
from zagg.registry import (
    discover_plugins,
    get_catalog_source,
    get_credential_provider,
    get_event_trigger,
    get_field_transform,
    get_mask_provider,
    get_reader,
    get_reducer,
    get_spatial_func,
    list_catalog_sources,
    list_credential_providers,
    list_event_triggers,
    list_field_transforms,
    list_mask_providers,
    list_readers,
    list_reducers,
    list_spatial_funcs,
    register_catalog_source,
    register_credential_provider,
    register_event_trigger,
    register_field_transform,
    register_mask_provider,
    register_reader,
    register_reducer,
    register_spatial_func,
    registry_snapshot,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_registries():
    """Snapshot every registry + the discovered flag, restore on teardown.

    Phase 2 ships empty registries, but later phases add built-ins; this
    keeps the test suite future-proof and isolates each test from the rest.
    """
    saved = {kind: dict(reg) for kind, reg in registry._REGISTRIES.items()}
    saved_discovered = registry._DISCOVERED
    yield
    for kind, reg in registry._REGISTRIES.items():
        reg.clear()
        reg.update(saved[kind])
    registry._DISCOVERED = saved_discovered


# ---------------------------------------------------------------------------
# All eight registries are present
# ---------------------------------------------------------------------------


# The June plan (https://github.com/englacial/zagg/issues/12#issuecomment-4635480666)
# names exactly these eight registries. The set is treated as an invariant.
_EXPECTED_REGISTRIES = {
    "spatial_func",
    "reducer",
    "mask_provider",
    "field_transform",
    "event_trigger",
    "reader",
    "catalog_source",
    "credential_provider",
}


class TestRegistrySet:
    def test_exactly_eight_registries(self):
        assert set(registry._REGISTRIES) == _EXPECTED_REGISTRIES

    def test_snapshot_lists_every_registry(self):
        snap = registry_snapshot()
        assert set(snap) == _EXPECTED_REGISTRIES
        # Each entry is a sorted name list
        for kind, names in snap.items():
            assert isinstance(names, list)
            assert names == sorted(names)


# ---------------------------------------------------------------------------
# Direct register / get / list round-trip for each registry. Parametrising
# keeps the eight surfaces in lockstep — a future registry that grows a typo
# in one of its three helpers fails here loudly.
# ---------------------------------------------------------------------------


_REGISTRATIONS = [
    ("spatial_func", register_spatial_func, get_spatial_func, list_spatial_funcs),
    ("reducer", register_reducer, get_reducer, list_reducers),
    ("mask_provider", register_mask_provider, get_mask_provider, list_mask_providers),
    ("field_transform", register_field_transform, get_field_transform, list_field_transforms),
    ("event_trigger", register_event_trigger, get_event_trigger, list_event_triggers),
    ("reader", register_reader, get_reader, list_readers),
    ("catalog_source", register_catalog_source, get_catalog_source, list_catalog_sources),
    (
        "credential_provider",
        register_credential_provider,
        get_credential_provider,
        list_credential_providers,
    ),
]


@pytest.mark.parametrize("kind,reg,get,lst", _REGISTRATIONS)
class TestRegistrySurface:
    def test_register_then_get_direct(self, kind, reg, get, lst):
        sentinel = object()
        reg("demo", sentinel)
        assert get("demo") is sentinel
        assert "demo" in lst()

    def test_register_then_get_decorator(self, kind, reg, get, lst):
        @reg("demo")
        def f():
            return 42

        assert get("demo") is f
        assert f() == 42
        assert "demo" in lst()

    def test_unknown_raises_keyerror_naming_kind(self, kind, reg, get, lst):
        with pytest.raises(KeyError, match=kind):
            get("nope")

    def test_duplicate_registration_raises(self, kind, reg, get, lst):
        reg("demo", object())
        with pytest.raises(ValueError, match="already registered"):
            reg("demo", object())

    def test_replace_overrides_existing(self, kind, reg, get, lst):
        first = object()
        second = object()
        reg("demo", first)
        reg("demo", second, replace=True)
        assert get("demo") is second

    def test_empty_name_rejected(self, kind, reg, get, lst):
        with pytest.raises(ValueError, match="non-empty string"):
            reg("", object())

    def test_non_string_name_rejected(self, kind, reg, get, lst):
        with pytest.raises(ValueError, match="non-empty string"):
            reg(123, object())


# ---------------------------------------------------------------------------
# Lazy entry-point discovery
# ---------------------------------------------------------------------------


class _FakeEntryPoint:
    """Lightweight stand-in for importlib.metadata.EntryPoint."""

    def __init__(self, name: str, register_fn):
        self.name = name
        self._register = register_fn

    def load(self):
        return self._register


def _install_entry_points(monkeypatch, eps):
    """Patch importlib.metadata.entry_points to return ``eps`` for our group."""

    def fake_entry_points(*, group):
        assert group == registry._ENTRY_POINT_GROUP
        return eps

    monkeypatch.setattr(registry.metadata, "entry_points", fake_entry_points)


class TestDiscovery:
    def test_discovery_runs_once_on_get(self, monkeypatch):
        calls = []

        def register_fn():
            calls.append(1)
            register_spatial_func("from_plugin", lambda: None)

        _install_entry_points(monkeypatch, [_FakeEntryPoint("demo", register_fn)])
        registry._DISCOVERED = False

        # First lookup triggers discovery and finds the plugin.
        assert get_spatial_func("from_plugin") is not None
        # Subsequent lookups do NOT re-discover.
        assert get_spatial_func("from_plugin") is not None
        assert calls == [1]

    def test_discover_plugins_force_reruns(self, monkeypatch):
        calls = []

        def register_fn():
            calls.append(1)

        _install_entry_points(monkeypatch, [_FakeEntryPoint("demo", register_fn)])
        registry._DISCOVERED = False
        discover_plugins()
        discover_plugins()  # idempotent
        discover_plugins(force=True)
        assert calls == [1, 1]

    def test_failing_plugin_does_not_crash_discovery(self, monkeypatch, caplog):
        good_calls = []

        def good():
            good_calls.append(1)
            register_reducer("good", object())

        def bad():
            raise RuntimeError("plugin exploded")

        _install_entry_points(
            monkeypatch,
            [
                _FakeEntryPoint("bad", bad),
                _FakeEntryPoint("good", good),
            ],
        )
        registry._DISCOVERED = False

        with caplog.at_level("ERROR"):
            discover_plugins()

        # Good plugin still registered; the bad one logged but did not abort.
        assert "good" in list_reducers()
        assert good_calls == [1]
        assert any("bad" in rec.message for rec in caplog.records)

    def test_failing_entry_point_load_does_not_crash(self, monkeypatch, caplog):
        class BadEntryPoint:
            name = "bad"

            def load(self):
                raise ImportError("module not found")

        def good():
            register_reader("good", object())

        _install_entry_points(
            monkeypatch,
            [BadEntryPoint(), _FakeEntryPoint("good", good)],
        )
        registry._DISCOVERED = False

        with caplog.at_level("ERROR"):
            discover_plugins()

        assert "good" in list_readers()
        assert any("bad" in rec.message for rec in caplog.records)

    def test_list_also_triggers_discovery(self, monkeypatch):
        def register_fn():
            register_event_trigger("from_plugin", lambda: True)

        _install_entry_points(monkeypatch, [_FakeEntryPoint("demo", register_fn)])
        registry._DISCOVERED = False

        assert "from_plugin" in list_event_triggers()

    def test_snapshot_triggers_discovery(self, monkeypatch):
        def register_fn():
            register_mask_provider("from_plugin", object())

        _install_entry_points(monkeypatch, [_FakeEntryPoint("demo", register_fn)])
        registry._DISCOVERED = False

        snap = registry_snapshot()
        assert "from_plugin" in snap["mask_provider"]
