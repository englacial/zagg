import numpy as np
import zarr
from conftest import nested_ids
from zarr.storage import MemoryStore

from zagg.config import default_config, get_coords, get_data_vars
from zagg.grids import HealpixGrid
from zagg.processing import write_dataframe_to_zarr


def test_full_integration(zarr_store, mock_dataframe_factory):
    """Test complete flow: template creation + data writing."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    # D16 flip (issue #304): the stored coordinate set excludes cell_ids.
    coords = [c for c in get_coords(cfg) if c != "cell_ids"]
    data_vars = get_data_vars(cfg)

    grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
    store = zarr_store
    grid.emit_template(store)

    df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

    n_children = 4 ** (child_order - parent_order)
    chunk_idx = (int(nested_ids(df_out).min()) // n_children,)
    write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)

    group = zarr.open_group(store=store, path=str(child_order), mode="r")
    min_idx = int(nested_ids(df_out).min())
    max_idx = int(nested_ids(df_out).max())

    for col in coords + data_vars:
        actual = group[col][min_idx : max_idx + 1]
        expected = df_out[col].values
        np.testing.assert_array_almost_equal(actual, expected)


def test_rectilinear_end_to_end():
    """Write a synthetic per-cell dataframe to a rectilinear store; read
    back and verify cell positions."""
    import pandas as pd

    from zagg.config import default_config
    from zagg.grids import RectilinearGrid

    cfg = default_config("atl06_polar")
    # Small grid for test speed: 32 x 32 cells, 8x8 chunks → 4x4 chunk grid.
    grid = RectilinearGrid(
        crs="EPSG:3031",
        resolution=200_000,  # large resolution → small grid
        bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
        chunk_shape=(8, 8),
        config=cfg,
    )
    assert grid.array_shape == (32, 32)
    assert grid.n_row_blocks == 4 and grid.n_col_blocks == 4

    store = MemoryStore()
    grid.emit_template(store)

    # Write known values to one chunk.
    shard = grid._pack(1, 2)
    n_cells = grid.chunk_h * grid.chunk_w
    df = pd.DataFrame(
        {
            "count": np.arange(n_cells, dtype=np.int32),
            "h_min": np.linspace(-100, 100, n_cells, dtype=np.float32),
            "h_max": np.linspace(0, 200, n_cells, dtype=np.float32),
            "h_mean": np.linspace(-50, 150, n_cells, dtype=np.float32),
            "h_sigma": np.full(n_cells, 0.5, dtype=np.float32),
            "h_variance": np.full(n_cells, 1.0, dtype=np.float32),
        }
    )
    from zagg.processing import write_dataframe_to_zarr

    write_dataframe_to_zarr(df, store, grid=grid, chunk_idx=grid.block_index(shard))

    # Read back. Block (1, 2) covers rows [8, 16) and cols [16, 24).
    group = zarr.open_group(store, path=grid.group_path, mode="r")
    block = group["count"][8:16, 16:24]
    assert block.shape == (8, 8)
    np.testing.assert_array_equal(block, np.arange(n_cells, dtype=np.int32).reshape(8, 8))

    # Confirm other chunks are still fill-value (NaN for float arrays).
    assert np.all(np.isnan(group["h_mean"][0:8, 0:8]))


def test_multiple_parent_cells(zarr_store, mock_dataframe_factory):
    """Test writing data from multiple parent cells to the same store."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    # D16 flip (issue #304): the stored coordinate set excludes cell_ids.
    coords = [c for c in get_coords(cfg) if c != "cell_ids"]
    data_vars = get_data_vars(cfg)

    grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
    store = zarr_store
    grid.emit_template(store)

    coordinates = [
        (-78.5, -132.0),
        (-75.0, 0.0),
    ]
    all_data = {}

    for lat, lon in coordinates:
        df_out = mock_dataframe_factory(lat, lon, parent_order, child_order)

        n_children = 4 ** (child_order - parent_order)
        chunk_idx = (int(nested_ids(df_out).min()) // n_children,)
        write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)

        all_data[(lat, lon)] = df_out

    group = zarr.open_group(store=store, path=str(child_order), mode="r")

    for (lat, lon), df_out in all_data.items():
        min_idx = int(nested_ids(df_out).min())
        max_idx = int(nested_ids(df_out).max())

        for col in coords + data_vars:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(
                actual, expected, err_msg=f"Mismatch in {col} for ({lat}, {lon})"
            )
