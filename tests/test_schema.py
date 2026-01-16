import numpy as np
import pytest
from zarr import open_array, open_group
from zarr.storage import MemoryStore

from magg.schema import COORDS, DATA_VARS, HEALPIX_BASE_CELLS, xdggs_zarr_template


class TestCreateZarrTemplate:
    def test_creates_all_arrays(self):
        """Test that all COORDS and DATA_VARS arrays are created."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        assert set(group.keys()) == set(COORDS + DATA_VARS)

    def test_array_shape(self):
        """Test that array shape equals HEALPIX_BASE_CELLS * 4^child_order."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_shape = (HEALPIX_BASE_CELLS * 4**child_order,)

        for name in COORDS + DATA_VARS:
            assert group[name].shape == expected_shape

    def test_chunk_shape(self):
        """Test that chunk shape equals 4^(child_order - parent_order)."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_chunks = (4 ** (child_order - parent_order),)

        for name in COORDS + DATA_VARS:
            assert group[name].chunks == expected_chunks

    def test_coordinate_dtypes(self):
        """Test that cell_ids and morton are uint64."""
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        assert group["cell_ids"].dtype == np.uint64
        assert group["morton"].dtype == np.int64

    def test_count_dtype(self):
        """Test that count is int32."""
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        assert group["count"].dtype == np.int32

    def test_statistical_vars_dtype(self):
        """Test that statistical variables are float32."""
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")
        stat_vars = [v for v in DATA_VARS if v != "count"]

        for name in stat_vars:
            assert group[name].dtype == np.float32

    def test_fill_values(self):
        """Test that fill values are correct for each type."""
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=8)

        group = open_group(store, path="8", mode="r")

        # Coordinates have fill_value 0
        assert group["cell_ids"].fill_value == 0
        assert group["morton"].fill_value == 0

        # Count has fill_value 0
        assert group["count"].fill_value == 0

        # Statistical vars have fill_value NaN
        stat_vars = [v for v in DATA_VARS if v != "count"]
        for name in stat_vars:
            assert np.isnan(group[name].fill_value)

    def test_dimension_names(self):
        """Test that dimension_names is set to cell_ids."""
        store = MemoryStore()
        child_order = 8
        xdggs_zarr_template(store, parent_order=6, child_order=child_order)

        for name in COORDS + DATA_VARS:
            array = open_array(store, path=f"{child_order}/{name}", mode="r")
            print(array.metadata)
            assert array.metadata.dimension_names == ("cells",)

    def test_child_order_less_than_parent_raises(self):
        """Test that child_order < parent_order raises ValueError."""
        store = MemoryStore()

        with pytest.raises(ValueError, match="child_order.*must be >= parent_order"):
            xdggs_zarr_template(store, parent_order=8, child_order=6)

    def test_equal_orders(self):
        """Test that child_order == parent_order is valid (chunks of 1)."""
        store = MemoryStore()
        xdggs_zarr_template(store, parent_order=6, child_order=6)

        group = open_group(store, path="6", mode="r")
        assert group["count"].chunks == (1,)

    def test_template_creation_s3(self, s3_store_factory):
        """Test xdggs_zarr_template function on S3-compatible storage (MinIO)."""
        parent_order = 6
        child_order = 8

        store = s3_store_factory()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store=store, path=str(child_order), mode="r")
        assert set(group.keys()) == set(COORDS + DATA_VARS)
        assert group["count"].shape == (HEALPIX_BASE_CELLS * 4**child_order,)
        assert group["count"].chunks == (4 ** (child_order - parent_order),)
