"""Tests for the runner module (Python API)."""

import json

import pytest

from zagg.config import default_config
from zagg.runner import _load_catalog, _select_cells, agg


@pytest.fixture
def atl06_config():
    return default_config("atl06")


@pytest.fixture
def catalog_file(tmp_path):
    """Create a minimal catalog JSON for testing (PR-C shard_keys format)."""
    catalog = {
        "metadata": {
            "short_name": "ATL06",
            "parent_order": 6,
            "total_cells": 3,
            "total_granules": 10,
            "grid_type": "healpix",
        },
        "shard_keys": [-4211324, -4211323, -4211322],
        "granules": [
            ["s3://bucket/granule4.h5", "s3://bucket/granule5.h5", "s3://bucket/granule6.h5"],
            ["s3://bucket/granule3.h5"],
            ["s3://bucket/granule1.h5", "s3://bucket/granule2.h5"],
        ],
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
                dry_run=True, morton_cell="99999999")


class TestSelectCells:
    def _data(self, n=3):
        return {
            "metadata": {},
            "shard_keys": list(range(n)),
            "granules": [[f"u{i}"] for i in range(n)],
        }

    def test_all_cells(self):
        pairs = _select_cells(self._data(3))
        assert [k for k, _ in pairs] == [0, 1, 2]

    def test_max_cells(self):
        pairs = _select_cells(self._data(3), max_cells=2)
        assert [k for k, _ in pairs] == [0, 1]

    def test_morton_cell(self):
        pairs = _select_cells(self._data(3), morton_cell="1")
        assert [k for k, _ in pairs] == [1]

    def test_invalid_morton_cell(self):
        with pytest.raises(ValueError, match="not in catalog"):
            _select_cells(self._data(2), morton_cell="99")


class TestLoadCatalog:
    def test_load(self, catalog_file):
        data = _load_catalog(catalog_file)
        assert "metadata" in data
        assert "shard_keys" in data
        assert "granules" in data
        assert len(data["shard_keys"]) == 3

    def test_old_format_rejected(self, tmp_path):
        old = {"metadata": {}, "catalog": {"0": []}}
        p = tmp_path / "old.json"
        p.write_text(json.dumps(old))
        with pytest.raises(ValueError, match="pre-PR-C format"):
            _load_catalog(str(p))


class TestDenseDeprecation:
    def test_dense_layout_emits_warning(self, atl06_config, catalog_file):
        atl06_config.output["grid"]["layout"] = "dense"
        atl06_config.catalog = catalog_file
        with pytest.warns(DeprecationWarning, match="dense.*deprecated"):
            agg(atl06_config, store="./out.zarr", dry_run=True)

    def test_fullsphere_layout_does_not_warn(self, atl06_config, catalog_file):
        atl06_config.output["grid"]["layout"] = "fullsphere"
        atl06_config.catalog = catalog_file
        import warnings as _w
        with _w.catch_warnings():
            _w.simplefilter("error", DeprecationWarning)
            agg(atl06_config, store="./out.zarr", dry_run=True)


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
