"""Tests for the runner module (Python API)."""

import json

import pytest

from magg.config import default_config
from magg.runner import _load_catalog, _select_cells, agg


@pytest.fixture
def atl06_config():
    return default_config("atl06")


@pytest.fixture
def catalog_file(tmp_path):
    """Create a minimal catalog JSON for testing."""
    catalog = {
        "metadata": {
            "short_name": "ATL06",
            "parent_order": 6,
            "total_cells": 3,
            "total_granules": 10,
        },
        "catalog": {
            "-4211322": ["s3://bucket/granule1.h5", "s3://bucket/granule2.h5"],
            "-4211323": ["s3://bucket/granule3.h5"],
            "-4211324": ["s3://bucket/granule4.h5", "s3://bucket/granule5.h5", "s3://bucket/granule6.h5"],
        },
    }
    p = tmp_path / "catalog.json"
    p.write_text(json.dumps(catalog))
    return str(p)


class TestRunValidation:
    def test_missing_catalog_raises(self, atl06_config):
        with pytest.raises(ValueError, match="No catalog"):
            agg(atl06_config, store="./out.zarr")

    def test_missing_store_raises(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="No store path"):
            agg(atl06_config, catalog=catalog_file)

    def test_unknown_backend_raises(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="Unknown backend"):
            agg(atl06_config, catalog=catalog_file, store="./out.zarr", backend="magic")

    def test_lambda_requires_s3_store(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="s3://"):
            agg(atl06_config, catalog=catalog_file, store="./local.zarr", backend="lambda")


class TestDryRun:
    def test_dry_run_returns_summary(self, atl06_config, catalog_file):
        result = agg(atl06_config, catalog=catalog_file, store="./out.zarr", dry_run=True)
        assert result["dry_run"] is True
        assert result["total_cells"] == 3
        assert result["store_path"] == "./out.zarr"

    def test_dry_run_max_cells(self, atl06_config, catalog_file):
        result = agg(atl06_config, catalog=catalog_file, store="./out.zarr",
                     dry_run=True, max_cells=2)
        assert result["total_cells"] == 2

    def test_dry_run_morton_cell(self, atl06_config, catalog_file):
        result = agg(atl06_config, catalog=catalog_file, store="./out.zarr",
                     dry_run=True, morton_cell="-4211322")
        assert result["total_cells"] == 1

    def test_dry_run_invalid_morton_cell(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="not in catalog"):
            agg(atl06_config, catalog=catalog_file, store="./out.zarr",
                dry_run=True, morton_cell="99999")


class TestSelectCells:
    def test_all_cells(self):
        catalog = {"a": [], "b": [], "c": []}
        assert _select_cells(catalog) == ["a", "b", "c"]

    def test_max_cells(self):
        catalog = {"a": [], "b": [], "c": []}
        assert _select_cells(catalog, max_cells=2) == ["a", "b"]

    def test_morton_cell(self):
        catalog = {"a": [], "b": [], "c": []}
        assert _select_cells(catalog, morton_cell="b") == ["b"]

    def test_invalid_morton_cell(self):
        catalog = {"a": [], "b": []}
        with pytest.raises(ValueError, match="not in catalog"):
            _select_cells(catalog, morton_cell="z")


class TestLoadCatalog:
    def test_load(self, catalog_file):
        data = _load_catalog(catalog_file)
        assert "metadata" in data
        assert "catalog" in data
        assert len(data["catalog"]) == 3


class TestConfigFallbacks:
    def test_catalog_from_config(self, catalog_file, tmp_path):
        """Config.catalog is used when catalog= is not passed."""
        cfg = default_config("atl06")
        cfg.catalog = catalog_file
        result = agg(cfg, store="./out.zarr", dry_run=True)
        assert result["total_cells"] == 3

    def test_store_from_config(self, catalog_file):
        """Config output.store is used when store= is not passed."""
        cfg = default_config("atl06")
        cfg.output["store"] = "./configured.zarr"
        result = agg(cfg, catalog=catalog_file, dry_run=True)
        assert result["store_path"] == "./configured.zarr"
