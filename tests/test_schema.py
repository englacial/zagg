import numpy as np
import pytest
from zarr import open_array, open_group
from zarr.storage import MemoryStore

from magg.config import default_config, get_agg_fields, get_coords, get_data_vars
from magg.schema import (
    HEALPIX_BASE_CELLS,
    xdggs_zarr_template,
)


@pytest.fixture
def cfg():
    return default_config("atl06")


@pytest.fixture
def coords(cfg):
    return get_coords(cfg)


@pytest.fixture
def data_vars(cfg):
    return get_data_vars(cfg)


@pytest.fixture
def all_vars(coords, data_vars):
    return coords + data_vars


class TestCreateZarrTemplate:
    def test_creates_all_arrays(self, all_vars):
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        assert set(group.keys()) == set(all_vars)

    def test_array_shape(self, all_vars):
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_shape = (HEALPIX_BASE_CELLS * 4**child_order,)

        for name in all_vars:
            assert group[name].shape == expected_shape

    def test_array_shape_n_parent_cells(self, all_vars):
        parent_order = 6
        child_order = 8
        n_parent_cells = 1

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order, n_parent_cells=n_parent_cells)

        group = open_group(store, path=str(child_order), mode="r")
        expected_shape = (16,)

        for name in all_vars:
            assert group[name].shape == expected_shape

    def test_chunk_shape(self, all_vars):
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_chunks = (4 ** (child_order - parent_order),)

        for name in all_vars:
            assert group[name].chunks == expected_chunks

    def test_coordinate_dtypes(self):
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        assert group["cell_ids"].dtype == np.uint64
        assert group["morton"].dtype == np.int64

    def test_count_dtype(self):
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        assert group["count"].dtype == np.int32

    def test_statistical_vars_dtype(self, data_vars):
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        stat_vars = [v for v in data_vars if v != "count"]

        for name in stat_vars:
            assert group[name].dtype == np.float32

    def test_fill_values(self, data_vars):
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")

        # Coordinates have fill_value 0
        assert group["cell_ids"].fill_value == 0
        assert group["morton"].fill_value == 0

        # Count has fill_value 0
        assert group["count"].fill_value == 0

        # Statistical vars have fill_value NaN
        stat_vars = [v for v in data_vars if v != "count"]
        for name in stat_vars:
            assert np.isnan(group[name].fill_value)

    def test_dimension_names(self, all_vars):
        store = MemoryStore()
        child_order = 8
        xdggs_zarr_template(store, parent_order=6, child_order=child_order)

        for name in all_vars:
            array = open_array(store, path=f"{child_order}/{name}", mode="r")
            assert array.metadata.dimension_names == ("cells",)

    def test_child_order_less_than_parent_raises(self):
        store = MemoryStore()

        with pytest.raises(ValueError, match="child_order.*must be >= parent_order"):
            xdggs_zarr_template(store, parent_order=8, child_order=6)

    def test_equal_orders(self):
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=6)

        group = open_group(store, path="6", mode="r")
        assert group["count"].chunks == (1,)


class TestConfigDrivenFields:
    def test_expected_data_vars(self, data_vars):
        assert data_vars == [
            "count",
            "h_min",
            "h_max",
            "h_mean",
            "h_sigma",
            "h_variance",
            "h_q25",
            "h_q50",
            "h_q75",
        ]

    def test_expected_coords(self, coords):
        assert coords == ["cell_ids", "morton"]

    def test_all_agg_fields_have_required_metadata(self, cfg):
        for name, meta in get_agg_fields(cfg).items():
            has_func = "function" in meta
            has_expr = "expression" in meta
            assert has_func or has_expr, f"{name} missing 'function' or 'expression'"
            assert "dtype" in meta, f"{name} missing 'dtype'"
