"""Tests for catalog-source and credential-provider adapters."""

import json

import pytest

import zagg  # noqa: F401  (ensures default adapters are registered)
from zagg import adapters, registry


class TestDefaultsRegistered:
    def test_credential_providers_present(self):
        assert "nsidc" in registry.CREDENTIAL_PROVIDERS
        assert "edl" in registry.CREDENTIAL_PROVIDERS

    def test_catalog_source_present(self):
        assert "cmr" in registry.CATALOG_SOURCES

    def test_register_defaults_idempotent(self):
        # Calling again must not raise (guards against duplicate registration).
        adapters.register_defaults()
        adapters.register_defaults()


class TestCmrCatalogSource:
    def test_load_valid(self, tmp_path):
        cat = {"shard_keys": [1, 2], "granules": [["u1"], ["u2"]], "metadata": {}}
        p = tmp_path / "cat.json"
        p.write_text(json.dumps(cat))
        loaded = registry.get_catalog_source("cmr").load(str(p))
        assert loaded["shard_keys"] == [1, 2]

    def test_load_invalid_raises(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text(json.dumps({"catalog": {}}))
        with pytest.raises(ValueError, match="shard_keys"):
            registry.get_catalog_source("cmr").load(str(p))


class TestProtocols:
    def test_credential_provider_interface(self):
        prov = registry.get_credential_provider("edl")
        assert isinstance(prov, adapters.CredentialProvider)

    def test_catalog_source_interface(self):
        src = registry.get_catalog_source("cmr")
        assert isinstance(src, adapters.CatalogSource)
