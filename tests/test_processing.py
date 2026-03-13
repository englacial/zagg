import numpy as np
import pandas as pd
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from magg.processing import (
    AGG_FUNCTIONS,
    ATL06_CONFIG,
    DataSourceConfig,
    calculate_cell_statistics,
    write_dataframe_to_zarr,
)
from magg.schema import _COORDS, _DATA_VARS, _agg_fields, xdggs_zarr_template


class TestWriteDataframeToZarr:
    def test_write_dataframe_to_zarr(self, mock_dataframe_factory):
        parent_order = 6
        child_order = 8

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

        for col in _COORDS + _DATA_VARS:
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
    def test_all_agg_functions_registered(self):
        for name, meta in _agg_fields().items():
            assert meta["agg"] in AGG_FUNCTIONS, f"Missing AGG_FUNCTIONS entry for '{meta['agg']}'"

    def test_empty_df_returns_zeros_and_nans(self):
        result = calculate_cell_statistics(pd.DataFrame(columns=["h_li", "s_li"]))
        assert result["count"] == 0
        for name in _agg_fields():
            if name != "count":
                assert np.isnan(result[name]), f"{name} should be NaN for empty input"

    def test_result_keys_match_data_vars(self):
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert list(result.keys()) == _DATA_VARS

    def test_basic_statistics(self):
        df = pd.DataFrame({"h_li": [1.0, 2.0, 3.0], "s_li": [0.1, 0.1, 0.1]})
        result = calculate_cell_statistics(df)
        assert result["count"] == 3
        assert result["h_min"] == 1.0
        assert result["h_max"] == 3.0
        np.testing.assert_almost_equal(result["h_q50"], 2.0)


class TestDataSourceConfig:
    def test_atl06_config_has_six_groups(self):
        assert len(ATL06_CONFIG.groups) == 6
        assert ATL06_CONFIG.groups[0] == "gt1l"

    def test_atl06_config_has_coordinates(self):
        assert "latitude" in ATL06_CONFIG.coordinates
        assert "longitude" in ATL06_CONFIG.coordinates
        assert "{group}" in ATL06_CONFIG.coordinates["latitude"]

    def test_atl06_config_has_variables(self):
        assert "h_li" in ATL06_CONFIG.variables
        assert "s_li" in ATL06_CONFIG.variables

    def test_atl06_config_has_quality_filter(self):
        assert ATL06_CONFIG.quality_filter is not None
        assert "dataset" in ATL06_CONFIG.quality_filter
        assert ATL06_CONFIG.quality_filter["value"] == 0

    def test_roundtrip_serialization(self):
        d = ATL06_CONFIG.to_dict()
        restored = DataSourceConfig.from_dict(d)
        assert restored.groups == ATL06_CONFIG.groups
        assert restored.coordinates == ATL06_CONFIG.coordinates
        assert restored.variables == ATL06_CONFIG.variables
        assert restored.quality_filter == ATL06_CONFIG.quality_filter

    def test_no_quality_filter(self):
        cfg = DataSourceConfig(
            groups=["g1"],
            coordinates={"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
            variables={"val": "/{group}/value"},
        )
        assert cfg.quality_filter is None
        d = cfg.to_dict()
        assert d["quality_filter"] is None
        restored = DataSourceConfig.from_dict(d)
        assert restored.quality_filter is None

    def test_validate_schema_passes_for_atl06(self):
        ATL06_CONFIG.validate_schema()

    def test_validate_schema_catches_missing_source(self):
        cfg = DataSourceConfig(
            groups=["g1"],
            coordinates={"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
            variables={"h_li": "/{group}/h_li"},  # missing s_li
        )
        with pytest.raises(ValueError, match="s_li"):
            cfg.validate_schema()

    def test_validate_schema_passes_with_custom_fields(self):
        cfg = DataSourceConfig(
            groups=["g1"],
            coordinates={"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
            variables={"slope": "/{group}/slope"},
        )
        custom_fields = {
            "slope_min": {"agg": "nanmin", "source": "slope", "params": {}},
        }
        cfg.validate_schema(custom_fields)

    def test_validate_schema_catches_missing_with_custom_fields(self):
        cfg = DataSourceConfig(
            groups=["g1"],
            coordinates={"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
            variables={"slope": "/{group}/slope"},
        )
        custom_fields = {
            "x_mean": {"agg": "nanmin", "source": "missing_col", "params": {}},
        }
        with pytest.raises(ValueError, match="missing_col"):
            cfg.validate_schema(custom_fields)

    def test_group_template_substitution(self):
        path = ATL06_CONFIG.coordinates["latitude"].format(group="gt2r")
        assert path == "/gt2r/land_ice_segments/latitude"
