import numpy as np
import zarr

from magg.processing import write_dataframe_to_zarr
from magg.schema import COORDS, DATA_VARS, xdggs_zarr_template


def test_full_integration(s3_store_factory, mock_dataframe_factory):
    """Test complete flow: template creation + data writing.

    This tests both functions from the magg package:
    1. xdggs_zarr_template() - from magg.schema
    2. write_dataframe_to_zarr() - from magg.processing
    """
    parent_order = 6
    child_order = 8

    store = s3_store_factory()
    xdggs_zarr_template(store, parent_order, child_order)

    # Antarctic coordinate
    df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

    write_dataframe_to_zarr(df_out, store, child_order, parent_order)

    # Verify data integrity
    group = zarr.open_group(store=store, path=str(child_order), mode="r")
    min_idx = int(df_out["cell_ids"].min())
    max_idx = int(df_out["cell_ids"].max())

    for col in COORDS + DATA_VARS:
        actual = group[col][min_idx : max_idx + 1]
        expected = df_out[col].values
        np.testing.assert_array_almost_equal(actual, expected)


def test_multiple_parent_cells(s3_store_factory, mock_dataframe_factory):
    """Test writing data from multiple parent cells to the same store.

    This simulates how invoke_lambda.py processes multiple cells in parallel.
    """
    parent_order = 6
    child_order = 8

    store = s3_store_factory()
    xdggs_zarr_template(store, parent_order, child_order)

    # Different Antarctic locations to get different parent cells
    coordinates = [
        (-78.5, -132.0),  # West Antarctica
        (-75.0, 0.0),  # Near prime meridian
    ]
    all_data = {}

    for lat, lon in coordinates:
        df_out = mock_dataframe_factory(lat, lon, parent_order, child_order)

        write_dataframe_to_zarr(df_out, store, child_order, parent_order)

        all_data[(lat, lon)] = df_out

    # Verify both writes
    group = zarr.open_group(store=store, path=str(child_order), mode="r")

    for (lat, lon), df_out in all_data.items():
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in COORDS + DATA_VARS:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(
                actual, expected, err_msg=f"Mismatch in {col} for ({lat}, {lon})"
            )
