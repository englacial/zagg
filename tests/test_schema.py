import numpy as np
import pytest
from zarr import open_array, open_group
from zarr.storage import MemoryStore

from magg.schema import (
    _COORDS,
    _DATA_VARS,
    HEALPIX_BASE_CELLS,
    CellStatsSchema,
    _agg_fields,
    _fields_by_role,
    xdggs_zarr_template,
)


class TestCreateZarrTemplate:
    def test_creates_all_arrays(self):
        """Test that all _COORDS and _DATA_VARS arrays are created."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        assert set(group.keys()) == set(_COORDS + _DATA_VARS)

    def test_array_shape(self):
        """Test that array shape equals HEALPIX_BASE_CELLS * 4^child_order."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_shape = (HEALPIX_BASE_CELLS * 4**child_order,)

        for name in _COORDS + _DATA_VARS:
            assert group[name].shape == expected_shape

    def test_array_shape_n_parent_cells(self):
        """Test that array shape equals HEALPIX_BASE_CELLS * 4^child_order."""
        parent_order = 6
        child_order = 8
        n_parent_cells = 1

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order, n_parent_cells=n_parent_cells)

        group = open_group(store, path=str(child_order), mode="r")
        expected_shape = (16,)

        for name in _COORDS + _DATA_VARS:
            assert group[name].shape == expected_shape

    def test_chunk_shape(self):
        """Test that chunk shape equals 4^(child_order - parent_order)."""
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        group = open_group(store, path=str(child_order), mode="r")
        expected_chunks = (4 ** (child_order - parent_order),)

        for name in _COORDS + _DATA_VARS:
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
        stat_vars = [v for v in _DATA_VARS if v != "count"]

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
        stat_vars = [v for v in _DATA_VARS if v != "count"]
        for name in stat_vars:
            assert np.isnan(group[name].fill_value)

    def test_dimension_names(self):
        """Test that dimension_names is set to cell_ids."""
        store = MemoryStore()
        child_order = 8
        xdggs_zarr_template(store, parent_order=6, child_order=child_order)

        for name in _COORDS + _DATA_VARS:
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
        assert set(group.keys()) == set(_COORDS + _DATA_VARS)
        assert group["count"].shape == (HEALPIX_BASE_CELLS * 4**child_order,)
        assert group["count"].chunks == (4 ** (child_order - parent_order),)


class TestCellStatsSchema:
    def test_data_vars_derived_from_schema(self):
        """_DATA_VARS should match fields with role='data_var'."""
        assert _DATA_VARS == _fields_by_role("data_var")

    def test_coords_derived_from_schema(self):
        """_COORDS should match fields with role='coord'."""
        assert _COORDS == _fields_by_role("coord")

    def test_expected_data_vars(self):
        """_DATA_VARS should contain the expected 9 entries."""
        assert _DATA_VARS == [
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

    def test_expected_coords(self):
        """_COORDS should contain cell_ids and morton."""
        assert _COORDS == ["cell_ids", "morton"]

    def test_all_agg_fields_have_required_metadata(self):
        """Every agg field must have agg, source, fill_value, zarr_dtype."""
        for name, meta in _agg_fields().items():
            assert "agg" in meta, f"{name} missing 'agg'"
            assert "source" in meta, f"{name} missing 'source'"
            assert "fill_value" in meta, f"{name} missing 'fill_value'"
            assert "zarr_dtype" in meta, f"{name} missing 'zarr_dtype'"

    def test_schema_validates_conforming_dataframe(self):
        """A DataFrame matching the schema should validate."""
        import pandas as pd

        n = 10
        df = pd.DataFrame(
            {
                "cell_ids": np.arange(n, dtype=np.uint64),
                "morton": np.arange(n, dtype=np.int64),
                "count": np.ones(n, dtype=np.int32),
                "h_min": np.zeros(n, dtype=np.float32),
                "h_max": np.ones(n, dtype=np.float32),
                "h_mean": np.full(n, 0.5, dtype=np.float32),
                "h_sigma": np.full(n, 0.1, dtype=np.float32),
                "h_variance": np.full(n, 0.01, dtype=np.float32),
                "h_q25": np.full(n, 0.25, dtype=np.float32),
                "h_q50": np.full(n, 0.50, dtype=np.float32),
                "h_q75": np.full(n, 0.75, dtype=np.float32),
            }
        )
        validated = CellStatsSchema.validate(df)
        assert len(validated) == n
