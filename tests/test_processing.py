import numpy as np
import pandas as pd
import pytest
from zarr import open_group

from magg.processing import write_dataframe_to_zarr
from magg.schema import COORDS, DATA_VARS, create_zarr_template


class TestWriteDataframeToZarr:
    def test_write_dataframe_to_zarr(self, s3_store_factory, mock_dataframe_factory):
        """Test the write_dataframe_to_zarr function from magg.processing."""
        parent_order = 6
        child_order = 8

        store = s3_store_factory()
        create_zarr_template(store, parent_order, child_order)

        # Antarctic coordinate
        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

        assert write_dataframe_to_zarr(df_out, store, child_order, parent_order)

        # Verify data was written correctly
        group = open_group(store=store, mode="r", path=str(child_order))
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in COORDS + DATA_VARS:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(actual, expected, err_msg=f"Mismatch in {col}")

    def test_write_empty_dataframe(self, s3_store_factory):
        store = s3_store_factory()
        """Test that writing an empty DataFrame returns False without error."""
        assert write_dataframe_to_zarr(pd.DataFrame(), store, 8, 6)

    def test_write_index_range_mismatch(self, s3_store_factory, mock_dataframe_factory):
        """Test that index range mismatch returns an error."""
        parent_order = 6
        child_order = 8

        store = s3_store_factory()
        create_zarr_template(store, parent_order, child_order)

        # Create DataFrame with only half the children
        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)
        df_out = df_out.iloc[: len(df_out) // 2]
        with pytest.raises(
            ValueError, match="Expected index range to match range between min and max cell_ids"
        ):
            write_dataframe_to_zarr(df_out, store, child_order, parent_order)
