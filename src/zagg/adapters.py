"""Catalog-source and credential-provider adapters.

These decouple *where work units come from* and *how data access is
authenticated* from the runner, so neither the spatial CMR/morton catalog nor
NSIDC/EDL auth is hard-coded. Each is a small object registered by name; config
selects it (``catalog.source`` / ``data_source.credentials``), defaulting to the
spatial implementations so existing configs are unaffected. Domain adapters —
e.g. the MERRA-2 event catalog and GES-DISC credentials for the
antarctic_AR_dataset plugin — register alongside these defaults.
"""

import json
import logging
from typing import Protocol, runtime_checkable

from . import registry

logger = logging.getLogger(__name__)


@runtime_checkable
class CredentialProvider(Protocol):
    """Fetches data-access credentials once, to be fanned out to workers."""

    def fetch(self, region: str) -> dict:
        ...


@runtime_checkable
class CatalogSource(Protocol):
    """Builds or loads the work-unit catalog for a pipeline run."""

    def load(self, path: str) -> dict:
        ...


class NsidcCredentials:
    """Temporary NSIDC S3 credentials via earthaccess (the spatial default)."""

    def fetch(self, region: str = "us-west-2") -> dict:
        from .auth import get_nsidc_s3_credentials

        return get_nsidc_s3_credentials()


class EdlCredentials:
    """An Earthdata Login bearer token for HTTPS reads."""

    def fetch(self, region: str = "us-west-2") -> dict:
        from .auth import get_edl_token

        return {"edl_token": get_edl_token()}


class CmrCatalogSource:
    """Load a CMR/morton catalog JSON (the spatial default)."""

    def load(self, path: str) -> dict:
        with open(path) as f:
            data = json.load(f)
        if "shard_keys" in data and "granules" in data:
            return data
        raise ValueError(
            f"Catalog at {path} is missing 'shard_keys'/'granules' "
            "(regenerate with `python -m zagg.catalog`)."
        )


def register_defaults() -> None:
    """Register the built-in adapters (idempotent)."""
    for name, obj in (("nsidc", NsidcCredentials()), ("edl", EdlCredentials())):
        if name not in registry.CREDENTIAL_PROVIDERS:
            registry.register_credential_provider(name, obj)
    if "cmr" not in registry.CATALOG_SOURCES:
        registry.register_catalog_source("cmr", CmrCatalogSource())


register_defaults()
