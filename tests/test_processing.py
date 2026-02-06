import numpy as np
import pandas as pd
import pytest
from zarr import open_group

from magg.processing import AGG_FUNCTIONS, calculate_cell_statistics, write_dataframe_to_zarr
from magg.schema import _COORDS, _DATA_VARS, _agg_fields, xdggs_zarr_template


class TestWriteDataframeToZarr:
    def test_write_dataframe_to_zarr(self, s3_store_factory, mock_dataframe_factory):
        """Test the write_dataframe_to_zarr function from magg.processing."""
        parent_order = 6
        child_order = 8

        store = s3_store_factory()
        xdggs_zarr_template(store, parent_order, child_order)

        # Antarctic coordinate
        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

        n_children = 4 ** (child_order - parent_order)
        chunk_idx = int(df_out["cell_ids"].min()) // n_children
        assert write_dataframe_to_zarr(
            df_out, store, chunk_idx=chunk_idx, child_order=child_order, parent_order=parent_order
        )

        # Verify data was written correctly
        group = open_group(store=store, mode="r", path=str(child_order))
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in _COORDS + _DATA_VARS:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(actual, expected, err_msg=f"Mismatch in {col}")

    def test_write_empty_dataframe(self, s3_store_factory):
        store = s3_store_factory()
        """Test that writing an empty DataFrame returns False without error."""
        assert write_dataframe_to_zarr(
            pd.DataFrame(), store, chunk_idx=0, child_order=8, parent_order=6
        )

    def test_write_index_range_mismatch(self, s3_store_factory, mock_dataframe_factory):
        """Test that index range mismatch returns an error."""
        parent_order = 6
        child_order = 8

        store = s3_store_factory()
        xdggs_zarr_template(store, parent_order, child_order)

        # Create DataFrame with only half the children
        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)
        df_out = df_out.iloc[: len(df_out) // 2]
        n_children = 4 ** (child_order - parent_order)
        chunk_idx = int(df_out["cell_ids"].min()) // n_children
        with pytest.raises(
            ValueError, match="Expected index range to match range between min and max cell_ids"
        ):
            write_dataframe_to_zarr(
                df_out,
                store,
                chunk_idx=chunk_idx,
                child_order=child_order,
                parent_order=parent_order,
            )


class TestCalculateCellStatistics:
    def test_all_agg_functions_registered(self):
        """Every agg name in the schema must exist in AGG_FUNCTIONS."""
        for name, meta in _agg_fields().items():
            assert meta["agg"] in AGG_FUNCTIONS, f"Missing AGG_FUNCTIONS entry for '{meta['agg']}'"

    def test_empty_df_returns_zeros_and_nans(self):
        """Empty DataFrame should return count=0 and NaN for all stats."""
        result = calculate_cell_statistics(pd.DataFrame(columns=["h_li", "s_li"]))
        assert result["count"] == 0
        for name in _agg_fields():
            if name != "count":
                assert np.isnan(result[name]), f"{name} should be NaN for empty input"

    def test_result_keys_match_data_vars(self):
        """calculate_cell_statistics keys should exactly match _DATA_VARS."""
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert list(result.keys()) == _DATA_VARS

    def test_basic_statistics(self):
        """Verify basic statistics for a simple input."""
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert result["count"] == 3
        assert result["h_min"] == 1.0
        assert result["h_max"] == 3.0
        np.testing.assert_almost_equal(result["h_q50"], 2.0)
