import numpy as np
import zarr

from magg.config import default_config, get_coords, get_data_vars
from magg.processing import write_dataframe_to_zarr
from magg.schema import xdggs_zarr_template


def test_full_integration(zarr_store, mock_dataframe_factory):
    """Test complete flow: template creation + data writing."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    coords = get_coords(cfg)
    data_vars = get_data_vars(cfg)

    store = zarr_store
    xdggs_zarr_template(store, parent_order, child_order)

    df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

    n_children = 4 ** (child_order - parent_order)
    chunk_idx = int(df_out["cell_ids"].min()) // n_children
    write_dataframe_to_zarr(
        df_out, store, chunk_idx=chunk_idx, child_order=child_order, parent_order=parent_order
    )

    group = zarr.open_group(store=store, path=str(child_order), mode="r")
    min_idx = int(df_out["cell_ids"].min())
    max_idx = int(df_out["cell_ids"].max())

    for col in coords + data_vars:
        actual = group[col][min_idx : max_idx + 1]
        expected = df_out[col].values
        np.testing.assert_array_almost_equal(actual, expected)


def test_multiple_parent_cells(zarr_store, mock_dataframe_factory):
    """Test writing data from multiple parent cells to the same store."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    coords = get_coords(cfg)
    data_vars = get_data_vars(cfg)

    store = zarr_store
    xdggs_zarr_template(store, parent_order, child_order)

    coordinates = [
        (-78.5, -132.0),
        (-75.0, 0.0),
    ]
    all_data = {}

    for lat, lon in coordinates:
        df_out = mock_dataframe_factory(lat, lon, parent_order, child_order)

        n_children = 4 ** (child_order - parent_order)
        chunk_idx = int(df_out["cell_ids"].min()) // n_children
        write_dataframe_to_zarr(
            df_out, store, chunk_idx=chunk_idx, child_order=child_order, parent_order=parent_order
        )

        all_data[(lat, lon)] = df_out

    group = zarr.open_group(store=store, path=str(child_order), mode="r")

    for (lat, lon), df_out in all_data.items():
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in coords + data_vars:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(
                actual, expected, err_msg=f"Mismatch in {col} for ({lat}, {lon})"
            )
