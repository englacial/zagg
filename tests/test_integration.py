import numpy as np
import zarr
from zarr.storage import MemoryStore

from zagg.config import default_config, get_coords, get_data_vars
from zagg.grids import HealpixGrid
from zagg.processing import write_dataframe_to_zarr
from zagg.schema import xdggs_zarr_template


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
            chunk_idx=dense_grid.block_index(parent)[0],
            child_order=child_order, parent_order=parent_order,
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
            chunk_idx=full_grid.block_index(parent)[0],
            child_order=child_order, parent_order=parent_order,
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
