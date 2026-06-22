"""Tests for the capability registry (issue #64)."""

from __future__ import annotations

import pytest

import zagg.registry as registry
from zagg.registry import (
    UnknownCapability,
    describe_all,
    discover_plugins,
    get_spatial_func,
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

# The June plan names exactly these eight registries; treated as an invariant.
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


@pytest.fixture(autouse=True)
def _clean_registries():
    """Snapshot every registry + the discovery flags; restore on teardown."""
    saved = {kind: dict(reg._entries) for kind, reg in registry._REGISTRIES.items()}
    saved_discovered = registry._DISCOVERED
    saved_discovering = registry._DISCOVERING
    yield
    for kind, reg in registry._REGISTRIES.items():
        reg._entries.clear()
        reg._entries.update(saved[kind])
    registry._DISCOVERED = saved_discovered
    registry._DISCOVERING = saved_discovering


# ---------------------------------------------------------------------------
# Registry set
# ---------------------------------------------------------------------------


class TestRegistrySet:
    def test_exactly_eight_registries(self):
        assert set(registry._REGISTRIES) == _EXPECTED_REGISTRIES

    def test_each_registry_knows_its_kind(self):
        for kind, reg in registry._REGISTRIES.items():
            assert reg.kind == kind

    def test_snapshot_lists_every_registry_name_sorted(self):
        snap = registry_snapshot()
        assert set(snap) == _EXPECTED_REGISTRIES
        for names in snap.values():
            assert isinstance(names, list)
            assert names == sorted(names)


# ---------------------------------------------------------------------------
# Register / get / list round-trip across all eight, both call forms
# ---------------------------------------------------------------------------

_REGISTER = [
    ("spatial_func", register_spatial_func),
    ("reducer", register_reducer),
    ("mask_provider", register_mask_provider),
    ("field_transform", register_field_transform),
    ("event_trigger", register_event_trigger),
    ("reader", register_reader),
    ("catalog_source", register_catalog_source),
    ("credential_provider", register_credential_provider),
]


@pytest.mark.parametrize("kind,register_fn", _REGISTER)
class TestSurface:
    def test_register_direct_then_get(self, kind, register_fn):
        sentinel = object()
        ret = register_fn("demo", sentinel)
        assert ret is sentinel  # direct call returns the object
        reg = registry._REGISTRIES[kind]
        assert reg.get("demo") is sentinel
        assert "demo" in reg.list()
        assert "demo" in reg

    def test_register_decorator_form(self, kind, register_fn):
        @register_fn("decorated")
        def capability():  # pragma: no cover - never called
            return 1

        assert registry._REGISTRIES[kind].get("decorated") is capability

    def test_duplicate_raises_unless_replace(self, kind, register_fn):
        register_fn("dup", object())
        with pytest.raises(ValueError, match="already registered"):
            register_fn("dup", object())
        replacement = object()
        register_fn("dup", replacement, replace=True)
        assert registry._REGISTRIES[kind].get("dup") is replacement

    def test_empty_or_nonstring_name_raises(self, kind, register_fn):
        with pytest.raises(ValueError, match="non-empty string"):
            register_fn("", object())
        with pytest.raises(ValueError, match="non-empty string"):
            register_fn(None, object())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Unknown-name error
# ---------------------------------------------------------------------------


class TestUnknownCapability:
    def test_get_unknown_raises_with_kind_and_available(self):
        register_spatial_func("alpha", object())
        register_spatial_func("beta", object())
        with pytest.raises(UnknownCapability) as exc:
            get_spatial_func("gamma")
        err = exc.value
        assert err.kind == "spatial_func"
        assert err.name == "gamma"
        assert err.available == ["alpha", "beta"]
        assert "spatial_func" in str(err)
        assert "alpha" in str(err)

    def test_is_a_keyerror_subclass(self):
        # Existing ``except KeyError`` paths keep catching it.
        with pytest.raises(KeyError):
            get_spatial_func("missing")

    def test_describe_unknown_raises(self):
        with pytest.raises(UnknownCapability):
            registry.SPATIAL_FUNCS.describe("nope")


# ---------------------------------------------------------------------------
# Optional description + schema
# ---------------------------------------------------------------------------


class TestDescribeAndSchema:
    def test_describe_without_schema_omits_key(self):
        register_spatial_func("plain", object(), description="a plain one")
        d = registry.SPATIAL_FUNCS.describe("plain")
        assert d == {"name": "plain", "kind": "spatial_func", "description": "a plain one"}
        assert "schema" not in d

    def test_describe_with_schema_includes_it(self):
        schema = {"type": "object", "properties": {"window": {"type": "integer"}}}
        register_spatial_func("with_schema", object(), description="d", schema=schema)
        d = registry.SPATIAL_FUNCS.describe("with_schema")
        assert d["schema"] is schema
        assert d["description"] == "d"

    def test_describe_all_is_structured_and_sorted(self):
        register_reducer("zeta", object())
        register_reducer("alpha", object(), description="first")
        out = describe_all()
        assert set(out) == _EXPECTED_REGISTRIES
        reducers = out["reducer"]
        assert [e["name"] for e in reducers] == ["alpha", "zeta"]
        assert reducers[0] == {"name": "alpha", "kind": "reducer", "description": "first"}

    def test_schema_default_none_pays_nothing(self):
        register_mask_provider("m", object())
        entry = registry.MASK_PROVIDERS._entries["m"]
        assert entry.schema is None
        assert entry.description == ""


# ---------------------------------------------------------------------------
# Lazy entry-point discovery
# ---------------------------------------------------------------------------


class _FakeEntryPoint:
    def __init__(self, name, register_fn, *, load_error=None):
        self.name = name
        self._register_fn = register_fn
        self._load_error = load_error

    def load(self):
        if self._load_error is not None:
            raise self._load_error
        return self._register_fn


def _patch_entry_points(monkeypatch, eps):
    monkeypatch.setattr(registry.metadata, "entry_points", lambda group: list(eps))
    registry._DISCOVERED = False
    registry._DISCOVERING = False


class TestDiscovery:
    def test_entry_point_register_runs_on_first_get(self, monkeypatch):
        def plugin_register():
            register_spatial_func("from_plugin", object())

        _patch_entry_points(monkeypatch, [_FakeEntryPoint("p", plugin_register)])
        # Not discovered yet until a get/list touches the registry.
        assert "from_plugin" not in registry.SPATIAL_FUNCS._entries
        assert "from_plugin" in list_spatial_funcs()

    def test_broken_load_is_skipped(self, monkeypatch):
        def good_register():
            register_reducer("good", object())

        eps = [
            _FakeEntryPoint("broken", None, load_error=ImportError("boom")),
            _FakeEntryPoint("good", good_register),
        ]
        _patch_entry_points(monkeypatch, eps)
        assert "good" in registry.REDUCERS.list()  # the broken one didn't abort discovery

    def test_broken_register_is_skipped(self, monkeypatch):
        def bad_register():
            raise RuntimeError("plugin blew up")

        def good_register():
            register_reader("ok", object())

        eps = [_FakeEntryPoint("bad", bad_register), _FakeEntryPoint("ok", good_register)]
        _patch_entry_points(monkeypatch, eps)
        assert "ok" in registry.READERS.list()

    def test_entry_points_lookup_failure_retries(self, monkeypatch):
        calls = {"n": 0}

        def flaky(group):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient metadata failure")
            return []

        monkeypatch.setattr(registry.metadata, "entry_points", flaky)
        registry._DISCOVERED = False
        registry._DISCOVERING = False
        # First call swallows the failure and leaves the seam retryable.
        assert list_spatial_funcs() == []
        assert registry._DISCOVERED is False
        # Second call succeeds and flips the flag.
        assert list_spatial_funcs() == []
        assert registry._DISCOVERED is True
        assert calls["n"] == 2

    def test_register_reentrancy_during_discovery(self, monkeypatch):
        # A plugin whose register() itself calls get_* must not re-enter the sweep.
        def plugin_register():
            register_field_transform("a", object())
            # Touch a get during discovery — should short-circuit, not recurse.
            assert "a" in registry.FIELD_TRANSFORMS._entries

        _patch_entry_points(monkeypatch, [_FakeEntryPoint("p", plugin_register)])
        discover_plugins()
        assert "a" in registry.FIELD_TRANSFORMS.list()

    def test_discover_plugins_force_reruns(self, monkeypatch):
        seen = {"n": 0}

        def plugin_register():
            seen["n"] += 1
            # replace=True so the second (forced) sweep doesn't trip the dup guard.
            register_credential_provider("edl", object(), replace=True)

        _patch_entry_points(monkeypatch, [_FakeEntryPoint("p", plugin_register)])
        discover_plugins()
        assert seen["n"] == 1
        discover_plugins(force=True)
        assert seen["n"] == 2


# ---------------------------------------------------------------------------
# Strings-not-callables invariant
# ---------------------------------------------------------------------------


def test_name_resolves_back_to_same_object():
    def my_max(arr):  # pragma: no cover - never called
        return arr

    register_spatial_func("max", my_max)
    # A config would carry the string "max"; resolution returns the callable.
    assert get_spatial_func("max") is my_max
