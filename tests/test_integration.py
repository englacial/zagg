import numpy as np
import zarr
from zarr.storage import MemoryStore

from zagg.config import default_config, get_coords, get_data_vars
from zagg.grids import HealpixGrid
from zagg.processing import write_dataframe_to_zarr


def test_full_integration(zarr_store, mock_dataframe_factory):
    """Test complete flow: template creation + data writing."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    coords = get_coords(cfg)
    data_vars = get_data_vars(cfg)

    grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
    store = zarr_store
    grid.emit_template(store)

    df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

    n_children = 4 ** (child_order - parent_order)
    chunk_idx = (int(df_out["cell_ids"].min()) // n_children,)
    write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)

    group = zarr.open_group(store=store, path=str(child_order), mode="r")
    min_idx = int(df_out["cell_ids"].min())
    max_idx = int(df_out["cell_ids"].max())

    for col in coords + data_vars:
        actual = group[col][min_idx : max_idx + 1]
        expected = df_out[col].values
        np.testing.assert_array_almost_equal(actual, expected)


def test_dense_fullsphere_equivalence(mock_dataframe_factory):
    """Writing the same per-cell data through both layouts yields identical
    cell_id → value mappings. Validates that fullsphere is a correctness-
    preserving storage swap before flipping it on for production users."""
    from mortie import clip2order, geo2mort

    parent_order = 6
    child_order = 8
    cfg = default_config()
    coords = get_coords(cfg)
    data_vars = get_data_vars(cfg)

    # Pick a handful of distinct parents from real lat/lon samples.
    points = [(-78.5, -132.0), (-72.1, 25.4), (-65.0, -45.0)]
    frames = [mock_dataframe_factory(lat, lon, parent_order, child_order) for lat, lon in points]
    parents = [
        int(clip2order(parent_order, geo2mort(np.array([lat]), np.array([lon]), order=18))[0])
        for lat, lon in points
    ]
    # Sanity: distinct parents
    assert len(set(parents)) == len(parents)

    # Dense store
    dense_grid = HealpixGrid(
        parent_order=parent_order, child_order=child_order, layout="dense",
        config=cfg, populated_shards=parents,
    )
    dense_store = MemoryStore()
    dense_grid.emit_template(dense_store)
    for parent, df in zip(parents, frames):
        write_dataframe_to_zarr(
            df, dense_store,
            grid=dense_grid,
            chunk_idx=dense_grid.block_index(parent),
        )

    # Fullsphere store
    full_grid = HealpixGrid(
        parent_order=parent_order, child_order=child_order, layout="fullsphere", config=cfg,
    )
    full_store = MemoryStore()
    full_grid.emit_template(full_store)
    for parent, df in zip(parents, frames):
        write_dataframe_to_zarr(
            df, full_store,
            grid=full_grid,
            chunk_idx=full_grid.block_index(parent),
        )

    dense_group = zarr.open_group(dense_store, path=str(child_order), mode="r")
    full_group = zarr.open_group(full_store, path=str(child_order), mode="r")

    # Per-frame: the cell_ids in each frame are in fullsphere coord space.
    # Lookup the dense position via grid.block_index; read both, compare.
    n_children = 4 ** (child_order - parent_order)
    for parent, df in zip(parents, frames):
        full_min = int(df["cell_ids"].min())
        full_max = int(df["cell_ids"].max())
        dense_pos = dense_grid.block_index(parent)[0]
        dense_min = dense_pos * n_children
        dense_max = dense_min + n_children - 1
        for col in coords + data_vars:
            dense_vals = dense_group[col][dense_min : dense_max + 1]
            full_vals = full_group[col][full_min : full_max + 1]
            np.testing.assert_array_equal(
                dense_vals, full_vals,
                err_msg=f"layout mismatch in {col} for parent {parent}",
            )


def test_rectilinear_end_to_end():
    """Write a synthetic per-cell dataframe to a rectilinear store; read
    back and verify cell positions."""
    import pandas as pd

    from zagg.config import default_config
    from zagg.grids import RectilinearGrid

    cfg = default_config("atl06_polar")
    # Small grid for test speed: 32 x 32 cells, 8x8 chunks → 4x4 chunk grid.
    grid = RectilinearGrid(
        crs="EPSG:3031", resolution=200_000,  # large resolution → small grid
        bounds=(-3_200_000, -3_200_000, 3_200_000, 3_200_000),
        chunk_shape=(8, 8), config=cfg,
    )
    assert grid.array_shape == (32, 32)
    assert grid.n_row_blocks == 4 and grid.n_col_blocks == 4

    store = MemoryStore()
    grid.emit_template(store)

    # Write known values to one chunk.
    shard = grid._pack(1, 2)
    n_cells = grid.chunk_h * grid.chunk_w
    df = pd.DataFrame({
        "count": np.arange(n_cells, dtype=np.int32),
        "h_min": np.linspace(-100, 100, n_cells, dtype=np.float32),
        "h_max": np.linspace(0, 200, n_cells, dtype=np.float32),
        "h_mean": np.linspace(-50, 150, n_cells, dtype=np.float32),
        "h_sigma": np.full(n_cells, 0.5, dtype=np.float32),
        "h_variance": np.full(n_cells, 1.0, dtype=np.float32),
    })
    from zagg.processing import write_dataframe_to_zarr
    write_dataframe_to_zarr(df, store, grid=grid, chunk_idx=grid.block_index(shard))

    # Read back. Block (1, 2) covers rows [8, 16) and cols [16, 24).
    group = zarr.open_group(store, path=grid.group_path, mode="r")
    block = group["count"][8:16, 16:24]
    assert block.shape == (8, 8)
    np.testing.assert_array_equal(
        block, np.arange(n_cells, dtype=np.int32).reshape(8, 8)
    )

    # Confirm other chunks are still fill-value (NaN for float arrays).
    assert np.all(np.isnan(group["h_mean"][0:8, 0:8]))


def test_multiple_parent_cells(zarr_store, mock_dataframe_factory):
    """Test writing data from multiple parent cells to the same store."""
    parent_order = 6
    child_order = 8

    cfg = default_config()
    coords = get_coords(cfg)
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
        chunk_idx = (int(df_out["cell_ids"].min()) // n_children,)
        write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)

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
