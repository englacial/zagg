import numpy as np
import pandas as pd
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from magg.config import default_config, get_agg_fields, get_coords, get_data_vars
from magg.processing import (
    calculate_cell_statistics,
    write_dataframe_to_zarr,
)
from magg.schema import xdggs_zarr_template


class TestWriteDataframeToZarr:
    def test_write_dataframe_to_zarr(self, mock_dataframe_factory):
        parent_order = 6
        child_order = 8

        cfg = default_config()
        coords = get_coords(cfg)
        data_vars = get_data_vars(cfg)

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

        n_children = 4 ** (child_order - parent_order)
        chunk_idx = int(df_out["cell_ids"].min()) // n_children
        assert write_dataframe_to_zarr(
            df_out, store, chunk_idx=chunk_idx, child_order=child_order, parent_order=parent_order
        )

        group = open_group(store=store, mode="r", path=str(child_order))
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in coords + data_vars:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(actual, expected, err_msg=f"Mismatch in {col}")

    def test_write_empty_dataframe(self):
        store = MemoryStore()
        assert write_dataframe_to_zarr(
            pd.DataFrame(), store, chunk_idx=0, child_order=8, parent_order=6
        )

    def test_write_index_range_mismatch(self, mock_dataframe_factory):
        parent_order = 6
        child_order = 8

        store = MemoryStore()
        xdggs_zarr_template(store, parent_order, child_order)

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
    def test_empty_df_returns_zeros_and_nans(self):
        result = calculate_cell_statistics(pd.DataFrame(columns=["h_li", "s_li"]))
        assert result["count"] == 0
        for name in get_agg_fields(default_config()):
            if name != "count":
                assert np.isnan(result[name]), f"{name} should be NaN for empty input"

    def test_result_keys_match_data_vars(self):
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert list(result.keys()) == get_data_vars(default_config())

    def test_basic_statistics(self):
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert result["count"] == 3
        assert result["h_min"] == 1.0
        assert result["h_max"] == 3.0
        np.testing.assert_almost_equal(result["h_q50"], 2.0)

    def test_with_explicit_config(self):
        cfg = default_config()
        df = pd.DataFrame({"h_li": [10.0, 20.0, 30.0], "s_li": [0.1, 0.2, 0.1]})
        result = calculate_cell_statistics(df, config=cfg)
        assert result["count"] == 3
        assert result["h_min"] == 10.0
        assert result["h_max"] == 30.0
        np.testing.assert_almost_equal(
            result["h_mean"],
            np.average([10, 20, 30], weights=1.0 / np.array([0.1, 0.2, 0.1]) ** 2),
        )


class TestDataSource:
    """Test data_source section of default config (replaces old DataSourceConfig tests)."""

    def test_atl06_has_six_groups(self):
        ds = default_config().data_source
        assert len(ds["groups"]) == 6
        assert ds["groups"][0] == "gt1l"

    def test_atl06_has_coordinates(self):
        ds = default_config().data_source
        assert "latitude" in ds["coordinates"]
        assert "longitude" in ds["coordinates"]
        assert "{group}" in ds["coordinates"]["latitude"]

    def test_atl06_has_variables(self):
        ds = default_config().data_source
        assert "h_li" in ds["variables"]
        assert "s_li" in ds["variables"]

    def test_atl06_has_quality_filter(self):
        ds = default_config().data_source
        assert ds.get("quality_filter") is not None
        assert "dataset" in ds["quality_filter"]
        assert ds["quality_filter"]["value"] == 0

    def test_group_template_substitution(self):
        ds = default_config().data_source
        path = ds["coordinates"]["latitude"].format(group="gt2r")
        assert path == "/gt2r/land_ice_segments/latitude"
