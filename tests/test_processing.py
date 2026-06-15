import numpy as np
import pandas as pd
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from zagg.config import default_config, get_agg_fields, get_coords, get_data_vars
from zagg.grids import HealpixGrid
from zagg.processing import (
    _build_groups,
    _concat_and_group,
    _group_columns,
    calculate_cell_statistics,
    write_dataframe_to_zarr,
)


class TestWriteDataframeToZarr:
    def test_write_dataframe_to_zarr(self, mock_dataframe_factory):
        parent_order = 6
        child_order = 8

        cfg = default_config()
        coords = get_coords(cfg)
        data_vars = get_data_vars(cfg)

        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)

        n_children = 4 ** (child_order - parent_order)
        chunk_idx = (int(df_out["cell_ids"].min()) // n_children,)
        assert write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)

        group = open_group(store=store, mode="r", path=str(child_order))
        min_idx = int(df_out["cell_ids"].min())
        max_idx = int(df_out["cell_ids"].max())

        for col in coords + data_vars:
            actual = group[col][min_idx : max_idx + 1]
            expected = df_out[col].values
            np.testing.assert_array_almost_equal(actual, expected, err_msg=f"Mismatch in {col}")

    def test_write_empty_dataframe(self):
        grid = HealpixGrid(6, 8, layout="fullsphere")
        store = MemoryStore()
        assert write_dataframe_to_zarr(pd.DataFrame(), store, grid=grid, chunk_idx=(0,))

    def test_write_row_count_mismatch(self, mock_dataframe_factory):
        parent_order = 6
        child_order = 8

        cfg = default_config()
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        df_out = mock_dataframe_factory(-78.5, -132.0, parent_order, child_order)
        df_out = df_out.iloc[: len(df_out) // 2]
        n_children = 4 ** (child_order - parent_order)
        chunk_idx = (int(df_out["cell_ids"].min()) // n_children,)
        with pytest.raises(ValueError, match="Expected.*rows for chunk_shape"):
            write_dataframe_to_zarr(df_out, store, grid=grid, chunk_idx=chunk_idx)


class TestCalculateCellStatistics:
    def test_empty_data_returns_zeros_and_nans(self):
        result = calculate_cell_statistics({"h_li": np.array([]), "s_li": np.array([])})
        assert result["count"] == 0
        for name in get_agg_fields(default_config()):
            if name != "count":
                assert np.isnan(result[name]), f"{name} should be NaN for empty input"

    def test_result_keys_match_data_vars(self):
        data = {"h_li": np.array([1.0, 2.0, 3.0]), "s_li": np.array([0.1, 0.1, 0.1])}
        result = calculate_cell_statistics(data)
        assert list(result.keys()) == get_data_vars(default_config())

    def test_basic_statistics(self):
        data = {"h_li": np.array([1.0, 2.0, 3.0]), "s_li": np.array([0.1, 0.1, 0.1])}
        result = calculate_cell_statistics(data)
        assert result["count"] == 3
        assert result["h_min"] == 1.0
        assert result["h_max"] == 3.0
        np.testing.assert_almost_equal(result["h_q50"], 2.0)

    def test_with_explicit_config(self):
        cfg = default_config()
        data = {"h_li": np.array([10.0, 20.0, 30.0]), "s_li": np.array([0.1, 0.2, 0.1])}
        result = calculate_cell_statistics(data, config=cfg)
        assert result["count"] == 3
        assert result["h_min"] == 10.0
        assert result["h_max"] == 30.0
        np.testing.assert_almost_equal(
            result["h_mean"],
            np.average([10, 20, 30], weights=1.0 / np.array([0.1, 0.2, 0.1]) ** 2),
        )


class TestBuildGroups:
    def test_slice_counts_match_per_cell_mask(self):
        """_build_groups produces identical cell populations as the old boolean-mask loop."""
        rng = np.random.default_rng(42)
        cells = np.array([10, 10, 20, 10, 30, 20, 30], dtype=np.int64)
        h_vals = rng.standard_normal(len(cells))
        s_vals = np.abs(rng.standard_normal(len(cells))) + 0.01
        df = pd.DataFrame({"h_li": h_vals, "s_li": s_vals, "leaf_id": cells})

        col_arrays, cell_to_slice = _build_groups(df, cells)

        for cell_id in [10, 20, 30]:
            start, end = cell_to_slice[cell_id]
            new_vals = col_arrays["h_li"][start:end]
            old_vals = h_vals[cells == cell_id]
            np.testing.assert_array_equal(new_vals, old_vals)

    def test_boundary_positions(self):
        cells = np.array([1, 1, 2, 3, 3, 3], dtype=np.int64)
        df = pd.DataFrame({"h_li": np.zeros(6), "s_li": np.ones(6), "leaf_id": cells})
        col_arrays, cell_to_slice = _build_groups(df, cells)
        start_1, end_1 = cell_to_slice[1]
        start_2, end_2 = cell_to_slice[2]
        start_3, end_3 = cell_to_slice[3]
        assert end_1 - start_1 == 2
        assert end_2 - start_2 == 1
        assert end_3 - start_3 == 3

    def test_absent_cell_not_in_map(self):
        cells = np.array([1, 2], dtype=np.int64)
        df = pd.DataFrame({"h_li": np.zeros(2), "s_li": np.ones(2), "leaf_id": cells})
        _, cell_to_slice = _build_groups(df, cells)
        assert 99 not in cell_to_slice

    def test_statistics_match_old_approach(self):
        """Sort-group statistics are identical to boolean-mask statistics."""
        rng = np.random.default_rng(7)
        n = 200
        child_ids = np.array([100, 200, 300, 400], dtype=np.int64)
        cells = rng.choice(child_ids, size=n)
        h_vals = rng.standard_normal(n).astype(np.float32)
        s_vals = np.abs(rng.standard_normal(n)).astype(np.float32) + 0.01
        df = pd.DataFrame({"h_li": h_vals, "s_li": s_vals, "leaf_id": cells})

        cfg = default_config()
        col_arrays, cell_to_slice = _build_groups(df, cells)
        _empty = {col: arr[:0] for col, arr in col_arrays.items()}

        for child_id in child_ids:
            # New sort/hash approach
            if child_id in cell_to_slice:
                s, e = cell_to_slice[child_id]
                new_data = {col: arr[s:e] for col, arr in col_arrays.items()}
            else:
                new_data = _empty
            new_stats = calculate_cell_statistics(new_data, config=cfg)

            # Reference: boolean-mask approach
            mask = cells == child_id
            old_data = {"h_li": h_vals[mask], "s_li": s_vals[mask], "leaf_id": cells[mask]}
            old_stats = calculate_cell_statistics(old_data, config=cfg)

            for key in new_stats:
                if np.isnan(new_stats[key]) and np.isnan(old_stats[key]):
                    continue
                np.testing.assert_array_equal(
                    new_stats[key], old_stats[key], err_msg=f"{key} mismatch for cell {child_id}"
                )


class TestArrowHandoff:
    """Phase 2 of #30: the Arrow carrier must match the pandas carrier exactly."""

    def test_group_columns_matches_build_groups(self):
        """_group_columns (carrier-agnostic core) == _build_groups (pandas wrapper)."""
        cells = np.array([5, 1, 5, 1, 9], dtype=np.int64)
        col_dict = {
            "h_li": np.arange(5.0, dtype=np.float32),
            "s_li": np.ones(5, dtype=np.float32),
            "leaf_id": cells,
        }
        df = pd.DataFrame(col_dict)
        arrays_a, slices_a = _build_groups(df, cells)
        arrays_b, slices_b = _group_columns(col_dict, cells)
        assert slices_a == slices_b
        for key in arrays_a:
            np.testing.assert_array_equal(arrays_a[key], arrays_b[key])

    def test_arrow_grouping_matches_pandas(self):
        """Arrow-carrier grouping yields byte-for-byte identical stats to pandas."""
        pa = pytest.importorskip("pyarrow")
        rng = np.random.default_rng(11)
        n = 500
        child_ids = np.array([100, 200, 300, 400, 500], dtype=np.int64)
        cells = rng.choice(child_ids, size=n)
        h_vals = (rng.standard_normal(n) * 30.0).astype(np.float32)
        s_vals = (np.abs(rng.standard_normal(n)) + 0.01).astype(np.float32)
        col_dict = {"h_li": h_vals, "s_li": s_vals, "leaf_id": cells}
        cfg = default_config()

        # pandas carrier
        df = pd.DataFrame(col_dict)
        p_arrays, p_slices = _build_groups(df, cells)

        # arrow carrier: read the columns back as numpy and group identically
        table = pa.table(col_dict).combine_chunks()
        a_carrier = {
            name: table.column(name).to_numpy(zero_copy_only=False) for name in table.column_names
        }
        a_leaf = table.column("leaf_id").to_numpy(zero_copy_only=False)
        a_arrays, a_slices = _group_columns(a_carrier, a_leaf)

        assert p_slices == a_slices
        for child in child_ids:
            child = int(child)
            ps, pe = p_slices[child]
            as_, ae = a_slices[child]
            p_stats = calculate_cell_statistics(
                {k: v[ps:pe] for k, v in p_arrays.items()}, config=cfg
            )
            a_stats = calculate_cell_statistics(
                {k: v[as_:ae] for k, v in a_arrays.items()}, config=cfg
            )
            for key in p_stats:
                if np.isnan(p_stats[key]) and np.isnan(a_stats[key]):
                    continue
                np.testing.assert_array_equal(
                    p_stats[key], a_stats[key], err_msg=f"{key} mismatch for cell {child}"
                )

    def test_concat_and_group_arrow_matches_pandas(self):
        """_concat_and_group drives the real carrier path (incl. multi-table concat)."""
        pa = pytest.importorskip("pyarrow")

        class _IdentityGrid:
            # leaf_id already is the cell id for this test; isolate the carrier
            # path from grid semantics.
            @staticmethod
            def cells_of(leaf_ids):
                return np.asarray(leaf_ids)

        grid = _IdentityGrid()
        cfg = default_config()
        rng = np.random.default_rng(7)
        child_ids = np.array([100, 200, 300, 400, 500], dtype=np.int64)

        # Three reads of differing length -> exercises concat ordering / offsets.
        reads = []
        for n in (40, 7, 53):
            cells = rng.choice(child_ids, size=n)
            reads.append(
                {
                    "h_li": (rng.standard_normal(n) * 30.0).astype(np.float32),
                    "s_li": (np.abs(rng.standard_normal(n)) + 0.01).astype(np.float32),
                    "leaf_id": cells,
                }
            )
        pandas_reads = [pd.DataFrame(r) for r in reads]
        arrow_reads = [pa.table(r) for r in reads]

        p_arrays, p_slices, p_n = _concat_and_group(pandas_reads, grid, "pandas")
        a_arrays, a_slices, a_n = _concat_and_group(arrow_reads, grid, "arrow")

        assert p_n == a_n == sum(len(r["leaf_id"]) for r in reads)
        assert p_slices == a_slices
        for child in child_ids:
            child = int(child)
            if child not in p_slices:
                continue
            ps, pe = p_slices[child]
            as_, ae = a_slices[child]
            p_stats = calculate_cell_statistics(
                {k: v[ps:pe] for k, v in p_arrays.items()}, config=cfg
            )
            a_stats = calculate_cell_statistics(
                {k: v[as_:ae] for k, v in a_arrays.items()}, config=cfg
            )
            for key in p_stats:
                if np.isnan(p_stats[key]) and np.isnan(a_stats[key]):
                    continue
                np.testing.assert_array_equal(
                    p_stats[key], a_stats[key], err_msg=f"{key} mismatch for cell {child}"
                )

    def test_concat_and_group_arrow_rejects_nulls(self):
        """The arrow carrier must fail loudly on null columns, not silently diverge."""
        pa = pytest.importorskip("pyarrow")

        class _IdentityGrid:
            @staticmethod
            def cells_of(leaf_ids):
                return np.asarray(leaf_ids)

        table = pa.table(
            {
                "h_li": pa.array([1.0, None, 3.0], type=pa.float32()),
                "s_li": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
                "leaf_id": pa.array([100, 200, 100], type=pa.int64()),
            }
        )
        with pytest.raises(ValueError, match="null-free"):
            _concat_and_group([table], _IdentityGrid(), "arrow")


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
