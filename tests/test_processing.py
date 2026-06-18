import numpy as np
import pandas as pd
import pytest
from zarr import open_group
from zarr.storage import MemoryStore

from zagg.config import default_config, get_agg_fields, get_coords, get_data_vars
from zagg.grids import HEALPIX_BASE_CELLS, HealpixGrid
from zagg.processing import (
    KERNEL_RTOL,
    _arrow_column,
    _build_groups,
    _build_output,
    _coerce_ragged_value,
    _concat_and_group,
    _empty_cell_value,
    _expand_mask_to_base,
    _group_columns,
    _has_ragged_fields,
    _has_vector_fields,
    _iter_carrier_columns,
    _kernel_able,
    _kernel_aggregate,
    _predicate_mask,
    _read_group,
    calculate_cell_statistics,
    process_shard,
    write_dataframe_to_zarr,
)


class _IdentityGrid:
    """Grid stub whose cell id is the leaf id, isolating the carrier path in
    ``_concat_and_group`` tests from real grid semantics."""

    @staticmethod
    def cells_of(leaf_ids):
        return np.asarray(leaf_ids)


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

    def test_numpy_nan_aware_functions(self):
        """The user contract — any aggregation expressible in numpy, including the
        ``nan*`` family — resolves and runs end-to-end through the default numpy
        path (``resolve_function`` does ``getattr(np, name)``). NaN-bearing input
        must be reduced NaN-skipping, matching the bare ``np.nan*`` operators."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_nanmean": {"function": "np.nanmean", "source": "h_li", "dtype": "float32"},
                    "h_nanmax": {"function": "nanmax", "source": "h_li", "dtype": "float32"},
                    "h_nanmin": {"function": "np.nanmin", "source": "h_li", "dtype": "float32"},
                    "h_nanvar": {"function": "nanvar", "source": "h_li", "dtype": "float32"},
                    "h_nansum": {"function": "np.nansum", "source": "h_li", "dtype": "float32"},
                    "h_nanstd": {"function": "np.nanstd", "source": "h_li", "dtype": "float32"},
                }
            }
        )
        vals = np.array([1.0, np.nan, 3.0, 5.0], dtype=np.float32)
        result = calculate_cell_statistics({"h_li": vals}, config=cfg)

        np.testing.assert_allclose(result["h_nanmean"], np.nanmean(vals))
        np.testing.assert_allclose(result["h_nanmax"], np.nanmax(vals))
        np.testing.assert_allclose(result["h_nanmin"], np.nanmin(vals))
        np.testing.assert_allclose(result["h_nanvar"], np.nanvar(vals))
        np.testing.assert_allclose(result["h_nansum"], np.nansum(vals))
        np.testing.assert_allclose(result["h_nanstd"], np.nanstd(vals))
        # NaN-skipping really happened: a plain np.mean/np.max would propagate NaN.
        assert not np.isnan(result["h_nanmean"])
        assert result["h_nanmax"] == 5.0
        assert result["h_nanmin"] == 1.0


class TestVectorOutputs:
    """Issue #29 phase 2: a ``kind: vector`` field yields a per-cell ndarray of
    its declared ``trailing_shape``/``dtype``; scalar fields are unchanged."""

    @staticmethod
    def _hist_config(bins=4, dtype="int64", fill_value=None):
        from zagg.config import PipelineConfig

        meta = {
            "function": "np.bincount",
            "source": "b",
            "kind": "vector",
            "trailing_shape": bins,
            "dtype": dtype,
            "params": {"minlength": bins},
        }
        if fill_value is not None:
            meta["fill_value"] = fill_value
        return PipelineConfig(aggregation={"variables": {"hist": meta}})

    def test_vector_field_returns_declared_shape(self):
        cfg = self._hist_config(bins=4)
        data = {"b": np.array([0, 1, 1, 3])}
        result = calculate_cell_statistics(data, config=cfg)
        hist = result["hist"]
        assert isinstance(hist, np.ndarray)
        assert hist.shape == (4,)
        assert hist.dtype == np.dtype("int64")
        np.testing.assert_array_equal(hist, [1, 2, 0, 1])

    def test_vector_empty_cell_gets_sentinel(self):
        cfg = self._hist_config(bins=4, dtype="float32")
        result = calculate_cell_statistics({"b": np.array([])}, config=cfg)
        hist = result["hist"]
        assert hist.shape == (4,)
        assert np.all(np.isnan(hist))  # default fill_value "NaN"

    def test_vector_empty_cell_numeric_sentinel(self):
        cfg = self._hist_config(bins=3, dtype="int64", fill_value=0)
        result = calculate_cell_statistics({"b": np.array([])}, config=cfg)
        np.testing.assert_array_equal(result["hist"], [0, 0, 0])

    def test_vector_wrong_width_raises(self):
        cfg = self._hist_config(bins=2)  # but bincount yields width 4 below
        with pytest.raises(ValueError, match="expected"):
            calculate_cell_statistics({"b": np.array([0, 3])}, config=cfg)

    @staticmethod
    def _edges_config(fill_value=None):
        """A ``kind: vector`` field driven by an ``expression`` (issue #29)."""
        from zagg.config import PipelineConfig

        meta = {
            "expression": "np.array([np.min(h), np.max(h)])",
            "source": "h",
            "kind": "vector",
            "trailing_shape": 2,
            "dtype": "float32",
        }
        if fill_value is not None:
            meta["fill_value"] = fill_value
        return PipelineConfig(aggregation={"variables": {"edges": meta}})

    def test_vector_expression_returns_declared_shape(self):
        """A vector ``expression`` field coerces to its declared shape/dtype, the
        same path a vector ``function`` field uses (issue #29)."""
        cfg = self._edges_config()
        result = calculate_cell_statistics({"h": np.array([1.0, 5.0, 3.0])}, config=cfg)
        edges = result["edges"]
        assert isinstance(edges, np.ndarray)
        assert edges.shape == (2,)
        assert edges.dtype == np.dtype("float32")
        np.testing.assert_array_equal(edges, [1.0, 5.0])

    def test_vector_expression_empty_cell_gets_sentinel(self):
        """An empty cell short-circuits to the fill_value sentinel WITHOUT
        evaluating the expression. ``_edges_config``'s expression is
        ``np.array([np.min(h), np.max(h)])`` and ``np.min([])`` raises, so this
        passing proves the empty-cell path never reaches the eval (issue #29)."""
        cfg = self._edges_config()
        result = calculate_cell_statistics({"h": np.array([])}, config=cfg)
        edges = result["edges"]
        assert edges.shape == (2,)
        assert np.all(np.isnan(edges))  # default fill_value "NaN"

    def test_vector_expression_empty_cell_numeric_sentinel(self):
        """Empty cell short-circuits to a numeric sentinel (no expression eval)."""
        cfg = self._edges_config(fill_value=0)
        result = calculate_cell_statistics({"h": np.array([])}, config=cfg)
        np.testing.assert_array_equal(result["edges"], [0, 0])

    def test_vector_expression_wrong_width_raises(self):
        """An expression yielding the wrong width fails loudly, like the function case."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "edges": {
                        "expression": "np.array([np.min(h), np.max(h), np.mean(h)])",
                        "source": "h",
                        "kind": "vector",
                        "trailing_shape": 2,
                        "dtype": "float32",
                    }
                }
            }
        )
        with pytest.raises(ValueError, match="expected"):
            calculate_cell_statistics({"h": np.array([1.0, 5.0, 3.0])}, config=cfg)

    def test_scalar_fields_unchanged_alongside_vector(self):
        """Adding a vector field must not perturb scalar outputs in the same dict."""
        scalar = calculate_cell_statistics(
            {"h_li": np.array([1.0, 2.0, 3.0]), "s_li": np.array([0.1, 0.1, 0.1])}
        )
        assert isinstance(scalar["h_min"], float)
        assert scalar["h_min"] == 1.0
        assert scalar["count"] == 3


class TestRaggedPayloads:
    """Issue #48 phase 2: ragged per-cell payloads collect correctly through the
    Arrow seam (``calculate_cell_statistics`` + ``_empty_cell_value``).
    """

    @staticmethod
    def _ragged_config(inner_shape=(2,), function="mean", source="h_li"):
        """Minimal config with one ragged field and one scalar field."""
        from zagg.config import PipelineConfig

        return PipelineConfig(
            aggregation={
                "variables": {
                    "h_ragged": {
                        "function": function,
                        "source": source,
                        "kind": "ragged",
                        "inner_shape": list(inner_shape),
                        "dtype": "float32",
                    },
                    "h_count": {
                        "function": "len",
                        "source": "h_li",
                    },
                }
            }
        )

    def test_has_ragged_fields_detects_ragged(self):
        cfg = self._ragged_config()
        assert _has_ragged_fields(cfg)

    def test_has_ragged_fields_false_for_scalar_only(self):
        from zagg.config import default_config

        assert not _has_ragged_fields(default_config())

    def test_empty_cell_value_ragged_returns_empty_list(self):
        meta = {"function": "mean", "source": "h", "kind": "ragged", "inner_shape": [2]}
        val = _empty_cell_value(meta)
        assert val == []

    def test_empty_cell_value_scalar_unchanged(self):
        meta = {"function": "len", "source": "h"}
        assert _empty_cell_value(meta) == 0

    def test_coerce_ragged_value_2d(self):
        sig = {"kind": "ragged", "inner_shape": (2,), "dtype": "float32", "trailing_shape": ()}
        arr = np.array([[1.0, 2.0], [3.0, 4.0]])
        out = _coerce_ragged_value(arr, sig)
        assert out.shape == (2, 2)
        assert out.dtype == np.dtype("float32")

    def test_coerce_ragged_value_empty(self):
        sig = {"kind": "ragged", "inner_shape": (2,), "dtype": "float32", "trailing_shape": ()}
        out = _coerce_ragged_value(np.array([]), sig)
        assert out.shape == (0, 2)

    def test_coerce_ragged_value_wrong_inner_raises(self):
        sig = {"kind": "ragged", "inner_shape": (3,), "dtype": "float32", "trailing_shape": ()}
        arr = np.array([[1.0, 2.0], [3.0, 4.0]])  # inner_shape (2,) != declared (3,)
        with pytest.raises(ValueError, match="inner shape"):
            _coerce_ragged_value(arr, sig)

    def test_calculate_cell_statistics_ragged_function(self):
        """A ragged field driven by a function returns a per-cell numpy array."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_raw": {
                        "function": "np.sort",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "dtype": "float32",
                    }
                }
            }
        )
        vals = np.array([3.0, 1.0, 2.0], dtype=np.float32)
        result = calculate_cell_statistics({"h_li": vals}, config=cfg)
        assert "h_raw" in result
        assert isinstance(result["h_raw"], np.ndarray)
        # np.sort returns a 1-D array; _coerce_ragged_value wraps to (n, 1).
        assert result["h_raw"].shape == (3, 1)
        np.testing.assert_array_equal(result["h_raw"].flatten(), [1.0, 2.0, 3.0])

    def test_calculate_cell_statistics_ragged_expression(self):
        """A ragged field driven by an expression returns a per-cell numpy array."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_pairs": {
                        "expression": "np.column_stack([h_li, h_li * 2])",
                        "kind": "ragged",
                        "inner_shape": [2],
                        "dtype": "float32",
                    }
                }
            }
        )
        vals = np.array([1.0, 2.0], dtype=np.float32)
        result = calculate_cell_statistics({"h_li": vals}, config=cfg)
        out = result["h_pairs"]
        assert out.shape == (2, 2)
        np.testing.assert_array_almost_equal(out[:, 0], [1.0, 2.0])
        np.testing.assert_array_almost_equal(out[:, 1], [2.0, 4.0])

    def test_empty_cell_ragged_returns_empty_list(self):
        """An empty cell for a ragged field returns [] (no observations)."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_raw": {
                        "function": "np.sort",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "dtype": "float32",
                    }
                }
            }
        )
        result = calculate_cell_statistics({"h_li": np.array([])}, config=cfg)
        assert result["h_raw"] == []

    def test_ragged_scalar_vector_coexist(self):
        """Ragged, scalar, and vector fields can coexist in one config."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_min": {"function": "min", "source": "h_li"},
                    "h_edges": {
                        "expression": "np.array([np.min(h_li), np.max(h_li)])",
                        "kind": "vector",
                        "trailing_shape": 2,
                        "dtype": "float32",
                    },
                    "h_raw": {
                        "function": "np.sort",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "dtype": "float32",
                    },
                }
            }
        )
        vals = np.array([3.0, 1.0, 2.0], dtype=np.float32)
        result = calculate_cell_statistics({"h_li": vals}, config=cfg)
        assert result["h_min"] == 1.0
        assert result["h_edges"].shape == (2,)
        assert result["h_raw"].shape == (3, 1)


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
        """The arrow carrier must fail loudly on null columns, not silently diverge.

        The null is in ``leaf_id`` — the grouping key — which is the case the guard
        exists to catch: a null there would corrupt the cell assignment under
        ``to_numpy``, not just a single stat.
        """
        pa = pytest.importorskip("pyarrow")

        table = pa.table(
            {
                "h_li": pa.array([1.0, 2.0, 3.0], type=pa.float32()),
                "s_li": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
                "leaf_id": pa.array([100, None, 100], type=pa.int64()),
            }
        )
        with pytest.raises(ValueError, match="null-free"):
            _concat_and_group([table], _IdentityGrid(), "arrow")


class TestKernelHandoff:
    """Phase 2b of #30 (EXPERIMENTAL): the pyarrow hash-aggregate kernel reducer.

    Unlike the pandas<->arrow *carrier* equivalence (byte-for-byte identical), the
    kernel path's float mean/variance diverge from numpy by ~1 ULP, so it is
    validated within :data:`KERNEL_RTOL`, not by exact equality.
    """

    def _numpy_reference(self, col_dict, cell_col, children, cfg):
        """Default per-cell numpy stats, as ``name -> ndarray`` over ``children``."""
        col_arrays, cell_to_slice = _group_columns(col_dict, cell_col)
        empty = {c: a[:0] for c, a in col_arrays.items()}
        out = {v: np.full(len(children), np.nan, dtype=np.float64) for v in get_data_vars(cfg)}
        for i, child in enumerate(children):
            child = int(child)
            if child in cell_to_slice:
                s, e = cell_to_slice[child]
                cell_data = {c: a[s:e] for c, a in col_arrays.items()}
            else:
                cell_data = empty
            stats = calculate_cell_statistics(cell_data, config=cfg)
            for k, v in stats.items():
                out[k][i] = v
        return out

    def test_kernel_able_classification(self):
        """count/min/max/var and unweighted average are kernel-able; the rest fall back."""
        cfg = default_config()
        fields = get_agg_fields(cfg)
        # Default atl06 config: count/h_min/h_max/h_variance are pure reductions;
        # h_mean is weighted, h_sigma is an expression, the quantiles are tdigest.
        assert _kernel_able(fields["count"])
        assert _kernel_able(fields["h_min"])
        assert _kernel_able(fields["h_max"])
        assert _kernel_able(fields["h_variance"])
        assert not _kernel_able(fields["h_mean"])  # weighted average
        assert not _kernel_able(fields["h_sigma"])  # expression
        assert not _kernel_able(fields["h_q50"])  # quantile
        # Unweighted average would be kernel-able.
        assert _kernel_able({"function": "average", "source": "h_li"})

    def test_kernel_matches_numpy_within_tolerance(self):
        """Kernel stats match the numpy reducer within KERNEL_RTOL (exact where integral)."""
        pa = pytest.importorskip("pyarrow")
        cfg = default_config()
        rng = np.random.default_rng(3)
        children = np.array([100, 200, 300, 400, 500], dtype=np.int64)
        n = 2000
        cells = rng.choice(children, size=n)
        h = (rng.standard_normal(n) * 30.0).astype(np.float32)
        s = (np.abs(rng.standard_normal(n)) + 0.01).astype(np.float32)
        col_dict = {"h_li": h, "s_li": s, "leaf_id": cells}

        ref = self._numpy_reference(col_dict, cells, children, cfg)
        table = pa.table(col_dict)
        kernel = _kernel_aggregate(table, cells, children, "h_li", cfg)
        ks = kernel["stats_arrays"]

        assert kernel["cells_with_data"] == len(children)
        # count/min/max are integral or order-independent extrema: exact.
        for name in ("count", "h_min", "h_max"):
            np.testing.assert_array_equal(
                np.asarray(ks[name], dtype=np.float64), ref[name], err_msg=name
            )
        # variance is the kernel-reduced float stat: close, not identical.
        np.testing.assert_allclose(
            np.asarray(ks["h_variance"], dtype=np.float64),
            ref["h_variance"],
            rtol=KERNEL_RTOL,
            equal_nan=True,
        )
        # Fallback fields (weighted mean, expression, quantiles) stay byte-identical
        # to numpy because the kernel path routes them through the same reducer.
        for name in ("h_mean", "h_sigma", "h_q25", "h_q50", "h_q75"):
            np.testing.assert_array_equal(
                np.asarray(ks[name], dtype=np.float64), ref[name], err_msg=name
            )

    def test_kernel_empty_cells_get_fill_values(self):
        """Cells with no observations get count=0 and NaN floats, like the default path."""
        pa = pytest.importorskip("pyarrow")
        cfg = default_config()
        children = np.array([1, 2, 3], dtype=np.int64)
        # Only cell 2 has data.
        cells = np.array([2, 2, 2], dtype=np.int64)
        col_dict = {
            "h_li": np.array([1.0, 2.0, 3.0], dtype=np.float32),
            "s_li": np.array([0.1, 0.1, 0.1], dtype=np.float32),
            "leaf_id": cells,
        }
        kernel = _kernel_aggregate(pa.table(col_dict), cells, children, "h_li", cfg)
        ks = kernel["stats_arrays"]
        assert kernel["cells_with_data"] == 1
        assert list(ks["count"]) == [0, 3, 0]
        assert np.isnan(ks["h_min"][0]) and np.isnan(ks["h_min"][2])
        assert ks["h_min"][1] == 1.0

    def test_kernel_nan_matches_numpy_semantics(self):
        """NaN-bearing cells: count/min/max stay EXACT vs numpy (NaN-propagating).

        pyarrow's min/max kernels skip NaN; numpy's propagate it. _kernel_aggregate
        must restore numpy semantics so the "count/min/max exact" contract holds on
        the NaN-bearing ``h_li`` values ATL06 can carry (the quality_filter is a
        flag check, not a NaN/fill filter). count is unaffected (NaN is a value, not
        a null) and mean/variance already propagate NaN like numpy.
        """
        pa = pytest.importorskip("pyarrow")
        cfg = default_config()
        children = np.array([10, 20, 30], dtype=np.int64)
        # cell 10: clean; cell 20: one NaN; cell 30: all NaN.
        cells = np.array([10, 10, 10, 20, 20, 20, 30, 30], dtype=np.int64)
        h = np.array([1.0, 2.0, 4.0, 1.0, np.nan, 3.0, np.nan, np.nan], dtype=np.float32)
        s = np.full(len(cells), 0.1, dtype=np.float32)
        col_dict = {"h_li": h, "s_li": s, "leaf_id": cells}

        ref = self._numpy_reference(col_dict, cells, children, cfg)
        ks = _kernel_aggregate(pa.table(col_dict), cells, children, "h_li", cfg)["stats_arrays"]

        # count: exact everywhere (NaN counts as a value).
        np.testing.assert_array_equal(np.asarray(ks["count"], dtype=np.float64), ref["count"])
        # min/max: bit-identical to numpy, including the NaN cells (10 clean, 20/30
        # propagate NaN). assert_array_equal treats NaN==NaN here.
        for name in ("h_min", "h_max"):
            np.testing.assert_array_equal(
                np.asarray(ks[name], dtype=np.float64), ref[name], err_msg=name
            )
        # Clean cell 10 is finite; NaN cells 20/30 propagate to NaN.
        assert ks["h_min"][0] == 1.0 and ks["h_max"][0] == 4.0
        assert np.isnan(ks["h_min"][1]) and np.isnan(ks["h_max"][1])
        assert np.isnan(ks["h_min"][2]) and np.isnan(ks["h_max"][2])
        # mean/variance already propagate NaN in both paths -> NaN on cells 20/30.
        for name in ("h_variance",):
            assert np.isnan(ks[name][1]) and np.isnan(ref[name][1])
            assert np.isnan(ks[name][2]) and np.isnan(ref[name][2])


class _KernelShardGrid:
    """Minimal grid stub driving the ``process_shard`` kernel branch.

    Exposes only what the ``handoff="arrow-kernel"`` path post-read needs:
    ``children``/``cells_of``/``chunk_coords`` (and ``chunk_shape`` is unused by
    process_shard itself). Spatial read methods are bypassed because the test
    monkeypatches ``_read_group`` to return canned tables.
    """

    def __init__(self, children, leaf_to_cell):
        self._children = np.asarray(children, dtype=np.int64)
        self._leaf_to_cell = leaf_to_cell

    def children(self, shard_key):
        return self._children

    def cells_of(self, leaf_ids):
        return np.array([self._leaf_to_cell[int(x)] for x in leaf_ids], dtype=np.int64)

    def chunk_coords(self, shard_key):
        return {
            "cell_lat": np.zeros(len(self._children)),
            "cell_lon": np.zeros(len(self._children)),
        }


class TestProcessShardKernelBranch:
    """HIGH-2 of PR #33 review: exercise the production ``process_shard`` kernel
    branch (null guard, ``cells_of``, ``concat_tables().combine_chunks()``, and the
    ``handoff`` validation), including NaN-bearing input so the NaN-semantics fix is
    covered end-to-end, not only in ``_kernel_aggregate``."""

    def _patch_reads(self, monkeypatch, tables):
        """Make ``_read_group`` yield the canned tables once, then None.

        Also stubs ``h5coro.H5Coro`` so the read loop never touches the network;
        the canned tables stand in for the spatially filtered group reads.
        """
        it = iter(tables)

        def fake_read_group(*args, **kwargs):
            return next(it, None)

        monkeypatch.setattr("zagg.processing._read_group", fake_read_group)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        # Avoid resolving a real h5coro driver (s3driver import / creds plumbing).
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_kernel_branch_matches_default_path(self, monkeypatch):
        """process_shard(handoff="arrow-kernel") agrees with the default path on the
        kernel-able stats (count/min/max exact, variance within KERNEL_RTOL),
        running the real concat + null guard + cells_of."""
        pa = pytest.importorskip("pyarrow")

        cfg = default_config()
        leaf_to_cell = {1: 10, 2: 10, 3: 20, 4: 30}
        children = [10, 20, 30]
        grid = _KernelShardGrid(children, leaf_to_cell)

        rng = np.random.default_rng(5)

        def make_table(n):
            leaf = rng.choice([1, 2, 3, 4], size=n).astype(np.int64)
            h = (rng.standard_normal(n) * 10.0).astype(np.float32)
            s = (np.abs(rng.standard_normal(n)) + 0.01).astype(np.float32)
            return pa.table({"h_li": h, "s_li": s, "leaf_id": leaf})

        # Two reads -> exercises pa.concat_tables(...).combine_chunks().
        tables = [make_table(60), make_table(25)]
        # Reuse the same data for the default path via a copy of the iterator.
        kernel_tables = [t for t in tables]
        default_tables = [t for t in tables]

        self._patch_reads(monkeypatch, kernel_tables)
        df_k, meta_k = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=cfg, handoff="arrow-kernel"
        )

        self._patch_reads(monkeypatch, default_tables)
        df_d, meta_d = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=cfg, handoff="arrow"
        )

        assert meta_k["cells_with_data"] == meta_d["cells_with_data"]
        assert meta_k["total_obs"] == meta_d["total_obs"] == 85
        for name in ("count", "h_min", "h_max"):
            np.testing.assert_array_equal(
                df_k[name].to_numpy(), df_d[name].to_numpy(), err_msg=name
            )
        np.testing.assert_allclose(
            df_k["h_variance"].to_numpy(),
            df_d["h_variance"].to_numpy(),
            rtol=KERNEL_RTOL,
            equal_nan=True,
        )

    def test_kernel_branch_nan_input(self, monkeypatch):
        """End-to-end NaN handling through process_shard's kernel branch: a NaN in
        ``h_li`` propagates to that cell's min/max (numpy semantics), count is
        unaffected, and the null guard does NOT trip (NaN is not an Arrow null)."""
        pa = pytest.importorskip("pyarrow")

        cfg = default_config()
        leaf_to_cell = {1: 10, 2: 20}
        children = [10, 20]
        grid = _KernelShardGrid(children, leaf_to_cell)

        # cell 10 clean, cell 20 has a NaN.
        table = pa.table(
            {
                "h_li": pa.array([1.0, 2.0, 4.0, 5.0, np.nan], type=pa.float32()),
                "s_li": pa.array([0.1, 0.1, 0.1, 0.1, 0.1], type=pa.float32()),
                "leaf_id": pa.array([1, 1, 1, 2, 2], type=pa.int64()),
            }
        )
        self._patch_reads(monkeypatch, [table])
        df, meta = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=cfg, handoff="arrow-kernel"
        )

        idx = {c: i for i, c in enumerate(children)}
        # Clean cell 10: finite extrema.
        assert df["h_min"].to_numpy()[idx[10]] == 1.0
        assert df["h_max"].to_numpy()[idx[10]] == 4.0
        # NaN cell 20: min/max propagate NaN (numpy semantics), count still 2.
        assert np.isnan(df["h_min"].to_numpy()[idx[20]])
        assert np.isnan(df["h_max"].to_numpy()[idx[20]])
        assert df["count"].to_numpy()[idx[20]] == 2
        assert meta["total_obs"] == 5

    def test_invalid_handoff_rejected(self):
        """The ``handoff`` validation rejects unknown carriers before any read."""

        grid = _KernelShardGrid([10], {1: 10})
        with pytest.raises(ValueError, match="handoff must be"):
            process_shard(grid, 0, ["s3://x"], s3_credentials={}, handoff="bogus")


class TestVectorCarrier:
    """Issue #29 phase 3: a config with any ``vector`` field routes the
    cell->table handoff through Arrow (FixedSizeList vector columns), while a
    pure-scalar config keeps the unchanged pandas carrier with byte-identical
    scalar outputs."""

    @staticmethod
    def _scalar_cfg():
        from zagg.config import PipelineConfig

        return PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                "variables": {
                    "count": {"function": "len"},
                    "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                }
            },
        )

    @staticmethod
    def _vector_cfg():
        """``_scalar_cfg`` plus a vector ``hist`` field (FixedSizeList<3>)."""
        from zagg.config import PipelineConfig

        return PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                "variables": {
                    "count": {"function": "len"},
                    "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                    "hist": {
                        "function": "np.bincount",
                        "source": "b",
                        "kind": "vector",
                        "trailing_shape": 3,
                        "dtype": "int64",
                        "fill_value": 0,
                        "params": {"minlength": 3},
                    },
                }
            },
        )

    def test_has_vector_fields(self):
        assert not _has_vector_fields(self._scalar_cfg())
        assert _has_vector_fields(self._vector_cfg())

    def _run(self, monkeypatch, cfg):
        """Drive process_shard on a canned read via the default (pandas) handoff;
        the output carrier (pandas vs Arrow) is chosen by the config's field kinds,
        independent of the input handoff."""
        pytest.importorskip("pyarrow")
        leaf_to_cell = {1: 10, 2: 10, 3: 20}
        children = [10, 20]
        grid = _KernelShardGrid(children, leaf_to_cell)
        df = pd.DataFrame(
            {
                "h_li": np.array([1.0, 2.0, 5.0], dtype=np.float32),
                "b": np.array([0, 2, 1], dtype=np.int64),
                "leaf_id": np.array([1, 1, 3], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        return process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg), children

    def test_scalar_config_returns_dataframe(self, monkeypatch):
        (df, _meta), _children = self._run(monkeypatch, self._scalar_cfg())
        assert isinstance(df, pd.DataFrame)

    def test_vector_config_returns_arrow_table(self, monkeypatch):
        pa = pytest.importorskip("pyarrow")
        (tbl, _meta), _children = self._run(monkeypatch, self._vector_cfg())
        assert isinstance(tbl, pa.Table)
        assert pa.types.is_fixed_size_list(tbl.column("hist").type)
        assert tbl.column("hist").type.list_size == 3

    def test_scalar_columns_byte_identical_with_and_without_vector(self, monkeypatch):
        """The hard #29/#30 criterion: adding a vector field must not perturb the
        scalar columns. Run the same canned input through both configs and assert
        the shared scalar columns match exactly."""
        (df, _m1), _c = self._run(monkeypatch, self._scalar_cfg())
        (tbl, _m2), _c = self._run(monkeypatch, self._vector_cfg())
        for name in ("count", "h_min"):
            np.testing.assert_array_equal(
                df[name].to_numpy(),
                tbl.column(name).to_numpy(zero_copy_only=False),
                err_msg=name,
            )

    def test_vector_column_values(self, monkeypatch):
        """The FixedSizeList payload holds each cell's per-cell vector. cell 10 has
        b=[0,2] -> bincount(minlength=3)=[1,0,1]; cell 20 has b=[1] -> [0,1,0]."""
        (tbl, _meta), children = self._run(monkeypatch, self._vector_cfg())
        hist = tbl.column("hist").combine_chunks()
        block = hist.values.to_numpy(zero_copy_only=False).reshape(len(children), 3)
        idx = {c: i for i, c in enumerate(children)}
        np.testing.assert_array_equal(block[idx[10]], [1, 0, 1])
        np.testing.assert_array_equal(block[idx[20]], [0, 1, 0])

    def test_arrow_column_roundtrips_through_iter(self):
        """_arrow_column -> _iter_carrier_columns recovers the (n_cells, C) block,
        the seam the dense vector writer consumes (phase 5)."""
        pa = pytest.importorskip("pyarrow")
        sig = {"kind": "vector", "trailing_shape": (3,), "dtype": "int64"}
        block = np.array([[1, 0, 1], [0, 1, 0]], dtype=np.int64)
        col = _arrow_column(block, sig)
        assert pa.types.is_fixed_size_list(col.type)
        tbl = pa.table({"hist": col})
        recovered = dict(_iter_carrier_columns(tbl))["hist"]
        np.testing.assert_array_equal(recovered, block)

    def test_build_output_scalar_is_plain_dataframe(self):
        """_build_output(use_arrow=False) is the unchanged pandas assembly."""
        grid = _KernelShardGrid([10, 20], {1: 10})
        stats = {"count": np.array([2, 1]), "h_min": np.array([1.0, 5.0], dtype=np.float32)}
        cfg = self._scalar_cfg()
        out = _build_output(
            stats, ["count", "h_min"], get_agg_fields(cfg), grid, 0, use_arrow=False
        )
        assert isinstance(out, pd.DataFrame)
        np.testing.assert_array_equal(out["count"].to_numpy(), [2, 1])


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


class TestVectorRoundTrip:
    """Issue #29 phase 6: a vector field written to a real Zarr template reads
    back through the trailing-dim block, and NaN-padded empty cells are skipped
    by a NaN-aware reducer."""

    @staticmethod
    def _vector_cfg():
        cfg = default_config("atl06")
        agg = {
            "coordinates": cfg.aggregation.get("coordinates", {}),
            "variables": {
                "count": {"function": "len", "source": "h_li"},
                "edges": {
                    "expression": "np.array([np.min(h), np.max(h)])",
                    "source": "h",
                    "kind": "vector",
                    "trailing_shape": 2,
                    "dtype": "float32",
                },
            },
        }
        from zagg.config import PipelineConfig

        return PipelineConfig(data_source=cfg.data_source, aggregation=agg, output=cfg.output)

    def test_vector_leaf_to_zarr_to_read(self):
        pytest.importorskip("pyarrow")
        from mortie import geo2mort

        cfg = self._vector_cfg()
        parent_order, child_order = 2, 4
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        parent = int(geo2mort(-78.5, -132.0, order=parent_order)[0])
        children = grid.children(parent)
        n = len(children)  # 4 ** (child_order - parent_order)
        assert n == 4 ** (child_order - parent_order)

        # This test isolates the carrier->writer->Zarr->reader half of #29, so it
        # fabricates the per-cell stats blocks directly rather than running the
        # ``edges`` expression (the stat-eval path is covered by TestVectorOutputs).
        # Two populated cells; the rest stay NaN-padded (the empty-cell sentinel).
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "edges": np.full((n, 2), np.nan, dtype="float32"),
        }
        stats["count"][0] = 5
        stats["edges"][0] = [1.0, 9.0]
        stats["count"][3] = 2
        stats["edges"][3] = [-2.0, 4.0]

        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=True
        )
        # The vector column is carried as a FixedSizeList (issue #29 B').
        assert carrier.column_names[:2] == ["count", "edges"]

        chunk_idx = grid.block_index(parent)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        group = open_group(store=store, mode="r", path=str(child_order))
        assert group["edges"].shape == (HEALPIX_BASE_CELLS * 4**child_order, 2)
        block_start = chunk_idx[0] * n
        got = group["edges"][block_start : block_start + n]

        # Populated cells round-trip exactly through the trailing-dim selection.
        np.testing.assert_array_equal(got[0], [1.0, 9.0])
        np.testing.assert_array_equal(got[3], [-2.0, 4.0])
        # Empty cells carry the NaN padding sentinel.
        assert np.all(np.isnan(got[1]))
        assert np.all(np.isnan(got[2]))

        # A NaN-aware reducer skips the padding: the per-edge mean over cells is
        # taken only over the two populated rows.
        reduced = np.nanmean(got, axis=0)
        np.testing.assert_allclose(reduced, [(1.0 - 2.0) / 2, (9.0 + 4.0) / 2])

    def test_split_trailing_chunk_rejected(self):
        """The writer enforces the single-trailing-chunk invariant: if the target
        array chunks the trailing payload dim, ``set_block_selection`` at block 0
        would drop the rest, so the write must raise instead (issue #29)."""
        pa = pytest.importorskip("pyarrow")
        from zarr import create_array

        class _OneChunkGrid:
            group_path = "g"
            chunk_shape = (2,)

        store = MemoryStore()
        # Trailing dim of width 4 deliberately split into two chunks of 2.
        create_array(
            store,
            name="g/edges",
            shape=(2, 4),
            chunks=(2, 2),
            dtype="float32",
            fill_value=np.float32("nan"),
        )
        edges = pa.FixedSizeListArray.from_arrays(pa.array(np.arange(8.0, dtype="float32")), 4)
        table = pa.table({"edges": edges})
        with pytest.raises(ValueError, match="one whole chunk"):
            write_dataframe_to_zarr(table, store, grid=_OneChunkGrid(), chunk_idx=(0,))


# ---------------------------------------------------------------------------
# Structured filters in the read path (issue #43, Phase A)
# ---------------------------------------------------------------------------


class _FakeH5:
    """Stub h5coro object: ``readDatasets`` returns canned arrays by path.

    Honors the ``hyperslice`` bound so sliced reads mirror the real driver.
    """

    def __init__(self, arrays):
        self._arrays = arrays

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        out = {}
        for d in datasets:
            if isinstance(d, str):
                out[d] = self._arrays[d]
                continue
            path = d["dataset"]
            arr = self._arrays[path]
            hs = d.get("hyperslice")
            if hs is not None:
                lo, hi = hs[0]
                arr = arr[lo:hi]
            out[path] = arr
        return out


class _ShardGrid:
    """Grid stub: leaf id == row index; every row maps to ``shard_key`` 0,
    so the spatial filter keeps all rows and the structured filters are
    exercised in isolation."""

    @staticmethod
    def assign(lats, lons):
        return np.arange(len(lats))

    @staticmethod
    def shards_of(leaf_ids):
        return np.zeros(len(leaf_ids), dtype=int)


class TestPredicateMask:
    def test_scalar_ops_1d(self):
        arr = np.array([0, 1, 2, 3, 0])
        assert _predicate_mask(
            arr, {"dataset": "/d", "op": "eq", "value": 0, "column": None}
        ).tolist() == [True, False, False, False, True]
        assert _predicate_mask(
            arr, {"dataset": "/d", "op": "ge", "value": 2, "column": None}
        ).tolist() == [False, False, True, True, False]

    def test_set_ops(self):
        arr = np.array([2, 3, 4, 5])
        assert _predicate_mask(arr, {"dataset": "/d", "op": "in", "values": [2, 4]}).tolist() == [
            True,
            False,
            True,
            False,
        ]
        assert _predicate_mask(
            arr, {"dataset": "/d", "op": "not_in", "values": [2, 4]}
        ).tolist() == [False, True, False, True]

    def test_keep_false_inverts(self):
        arr = np.array([0, 1, 0])
        assert _predicate_mask(
            arr, {"dataset": "/d", "op": "eq", "value": 0, "keep": False}
        ).tolist() == [False, True, False]

    def test_nd_column_slicing(self):
        # 2-D flag array (5 rows x 3 surface-type columns)
        arr = np.array([[0, 9, 9], [-2, 9, 9], [1, 9, 9], [-2, 9, 9], [3, 9, 9]])
        # signal_conf_ph-style: column 0, != -2
        mask = _predicate_mask(arr, {"dataset": "/d", "column": 0, "op": "ne", "value": -2})
        assert mask.tolist() == [True, False, True, False, True]

    def test_nd_requires_column(self):
        arr = np.zeros((3, 2))
        with pytest.raises(ValueError, match="requires an integer 'column'"):
            _predicate_mask(arr, {"dataset": "/d", "op": "eq", "value": 0})

    def test_column_on_1d_rejected(self):
        arr = np.zeros(3)
        with pytest.raises(ValueError, match="array is 1-D"):
            _predicate_mask(arr, {"dataset": "/d", "column": 0, "op": "eq", "value": 0})


class TestReadGroupFilters:
    def _data_source(self, **extra):
        ds = {
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h": "/h"},
        }
        ds.update(extra)
        return ds

    def test_quality_filter_eq_path(self):
        h5 = _FakeH5(
            {
                "/lat": np.array([1.0, 2.0, 3.0, 4.0]),
                "/lon": np.array([1.0, 2.0, 3.0, 4.0]),
                "/h": np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float32),
                "/qs": np.array([0, 1, 0, 1]),
            }
        )
        ds = self._data_source(quality_filter={"dataset": "/qs", "value": 0})
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [10.0, 30.0]

    def test_quality_filter_byte_identical_to_manual_eq(self):
        # The synthesized base eq filter must reproduce the legacy mask exactly.
        h = np.array([10.0, 20.0, 30.0, 40.0, 50.0], dtype=np.float32)
        qs = np.array([0, 1, 0, 0, 1])
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": h,
                "/qs": qs,
            }
        )
        ds = self._data_source(quality_filter={"dataset": "/qs", "value": 0})
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        expected = h[qs == 0]
        assert df["h"].to_numpy().tobytes() == expected.tobytes()

    def test_2d_signal_conf_filter(self):
        conf = np.array([[0], [-2], [4], [-2], [3]])  # column 0, surface type
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.arange(5.0, dtype=np.float32),
                "/conf": conf,
            }
        )
        ds = self._data_source(filters=[{"dataset": "/conf", "column": 0, "op": "ne", "value": -2}])
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [0.0, 2.0, 4.0]

    def test_multiple_anded_filters(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.arange(5.0, dtype=np.float32),
                "/conf": np.array([[5], [5], [0], [5], [5]]),
                "/pod": np.array([0, 0, 0, 1, 0]),
            }
        )
        ds = self._data_source(
            filters=[
                {"dataset": "/conf", "column": 0, "op": "ne", "value": 0},
                {"dataset": "/pod", "op": "eq", "value": 0},
            ]
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # row2 dropped by conf==0, row3 dropped by pod==1
        assert df["h"].tolist() == [0.0, 1.0, 4.0]

    def test_in_op_integer_column(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.arange(5.0, dtype=np.float32),
                "/conf": np.array([[2], [0], [3], [1], [4]]),
            }
        )
        ds = self._data_source(
            filters=[{"dataset": "/conf", "column": 0, "op": "in", "values": [2, 3, 4]}]
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [0.0, 2.0, 4.0]

    def test_atl03_shipped_template_2d_signal_conf(self):
        # Drives the shipped atl03.yaml structured filter through the read path
        # against a realistic (n_photons, 5) signal_conf_ph: one TEP photon
        # (-2 across every surface type) plus four non-TEP photons of varying
        # confidence. Only the TEP row is dropped (column 0, op: ne, value: -2).
        from zagg.config import default_config

        atl03_filters = default_config("atl03").data_source["filters"]
        conf = np.array(
            [
                [4, 4, 4, 4, 4],     # all-high-confidence (kept)
                [-2, -2, -2, -2, -2],  # TEP across all surface types (dropped)
                [0, 0, 0, 0, 0],     # noise across all (kept; -2 is the TEP flag)
                [3, 2, 1, 0, 4],     # mixed confidence (kept)
                [-2, 4, 4, 4, 4],    # column 0 is TEP but others aren't -- with
                                     # column: 0 this row is dropped, even though
                                     # the photon is a valid land-ice return on
                                     # surface type 3. See PR #47 review thread:
                                     # this is the operational tradeoff of moving
                                     # from .any(axis=1) to a single-column key.
            ]
        )
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.arange(5.0, dtype=np.float32),
                "/conf": conf,
            }
        )
        # Rewrite the shipped template's filter dataset path to match the fake h5.
        f = dict(atl03_filters[0])
        f["dataset"] = "/conf"
        ds = self._data_source(filters=[f])
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # Rows 1 (true TEP) and 4 (column-0 TEP only) are dropped.
        assert df["h"].tolist() == [0.0, 2.0, 3.0]

    def test_expression_filter_base_level(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.array([-1.0, 2.0, -3.0, 4.0, 5.0], dtype=np.float32),
            }
        )
        ds = self._data_source(filters=[{"expression": "h > 0"}])
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [2.0, 4.0, 5.0]

    def test_no_filter_keeps_all(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(3.0),
                "/lon": np.arange(3.0),
                "/h": np.arange(3.0, dtype=np.float32),
            }
        )
        df = _read_group(h5, "gt1l", self._data_source(), 0, _ShardGrid())
        assert df["h"].tolist() == [0.0, 1.0, 2.0]

    def test_all_filtered_returns_none(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(3.0),
                "/lon": np.arange(3.0),
                "/h": np.arange(3.0, dtype=np.float32),
                "/qs": np.array([1, 1, 1]),
            }
        )
        ds = self._data_source(quality_filter={"dataset": "/qs", "value": 0})
        assert _read_group(h5, "gt1l", ds, 0, _ShardGrid()) is None

    def test_filter_dataset_coincides_with_variable_path(self):
        # Filter dataset path == variable path exercises the dedup branch
        # (path must appear exactly once in the h5coro read list).
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.array([0.0, 1.0, 2.0, 3.0, 4.0], dtype=np.float32),
            }
        )
        ds = self._data_source(filters=[{"dataset": "/h", "op": "ge", "value": 2.0}])
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [2.0, 3.0, 4.0]

    def test_expression_filter_after_structured(self):
        # Expression filter ANDed after a structured predicate.
        h5 = _FakeH5(
            {
                "/lat": np.arange(5.0),
                "/lon": np.arange(5.0),
                "/h": np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32),
                "/qs": np.array([0, 0, 1, 0, 0]),
            }
        )
        ds = self._data_source(
            filters=[
                {"dataset": "/qs", "op": "eq", "value": 0},
                {"expression": "h > 2"},
            ]
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # structured drops row2 (qs==1); expression keeps h>2 from remainder
        assert df["h"].tolist() == [4.0, 5.0]

    def test_two_sequential_expression_filters(self):
        # Two sequential expression filters both applied.
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32),
            }
        )
        ds = self._data_source(
            filters=[
                {"expression": "h > 2"},
                {"expression": "h < 6"},
            ]
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [3.0, 4.0, 5.0]

    def test_expression_filter_undefined_name_raises(self):
        # Expression referencing an undefined name re-raises as NameError.
        h5 = _FakeH5(
            {
                "/lat": np.arange(3.0),
                "/lon": np.arange(3.0),
                "/h": np.arange(3.0, dtype=np.float32),
            }
        )
        ds = self._data_source(filters=[{"expression": "undefined_col > 0"}])
        with pytest.raises(NameError, match="undefined name"):
            _read_group(h5, "gt1l", ds, 0, _ShardGrid())


# ---------------------------------------------------------------------------
# _expand_mask_to_base and cross-level filter path (issue #43, Phase B)
# ---------------------------------------------------------------------------


class TestExpandMaskToBase:
    def test_single_parent_kept(self):
        # 1 parent keeps base rows 0-2 (3 photons).
        coarse = np.array([True])
        ibeg = np.array([0])
        cnt = np.array([3])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=0, total_base_size=3)
        np.testing.assert_array_equal(out, [True, True, True])

    def test_single_parent_dropped(self):
        coarse = np.array([False])
        ibeg = np.array([0])
        cnt = np.array([3])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=0, total_base_size=3)
        np.testing.assert_array_equal(out, [False, False, False])

    def test_two_parents_alternating(self):
        # parent 0 -> base rows 0-1 (kept); parent 1 -> base rows 2-4 (dropped).
        coarse = np.array([True, False])
        ibeg = np.array([0, 2])
        cnt = np.array([2, 3])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=0, total_base_size=5)
        np.testing.assert_array_equal(out, [True, True, False, False, False])

    def test_index_base_shift(self):
        # HDF5 1-based indexing: index_beg values start at 1.
        coarse = np.array([False, True])
        ibeg = np.array([1, 4])  # 1-based
        cnt = np.array([3, 2])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=1, total_base_size=5)
        # parent1 covers base rows 3 and 4 (ibeg=4-1=3, cnt=2).
        np.testing.assert_array_equal(out, [False, False, False, True, True])

    def test_empty_coarse_mask(self):
        coarse = np.array([False, False, False])
        ibeg = np.array([0, 2, 5])
        cnt = np.array([2, 3, 1])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=0, total_base_size=6)
        assert not np.any(out)

    def test_full_coarse_mask(self):
        coarse = np.array([True, True])
        ibeg = np.array([0, 3])
        cnt = np.array([3, 2])
        out = _expand_mask_to_base(coarse, ibeg, cnt, index_base=0, total_base_size=5)
        assert np.all(out)

    def test_negative_beg_raises(self):
        # index_beg_arr[0]=0 < index_base=1 -> beg=-1 -> must raise ValueError
        coarse = np.array([True])
        ibeg = np.array([0])
        cnt = np.array([3])
        with pytest.raises(ValueError, match="less than index_base"):
            _expand_mask_to_base(coarse, ibeg, cnt, index_base=1, total_base_size=3)


class TestReadGroupCrossLevel:
    """Phase B: cross-level filters expand coarse verdicts to base-rate rows."""

    def _data_source_with_levels(self, coarse_filter_value=None):
        """Two-level data source: 'segments' -> 'photons' via link arrays."""
        ds = {
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h": "/h"},
            "base_level": "photons",
            "levels": {
                "photons": {
                    "path": "/heights",
                    "coordinates": ["lat", "lon"],
                    "variables": ["h"],
                    "link": None,
                },
                "segments": {
                    "path": "/geolocation",
                    "coordinates": [],
                    "variables": ["signal_conf_ph"],
                    "link": {
                        "to": "photons",
                        "index_beg": "/ph_index_beg",
                        "count": "/segment_ph_cnt",
                    },
                },
            },
        }
        if coarse_filter_value is not None:
            ds["filters"] = [
                {
                    "dataset": "/conf",
                    "op": "ne",
                    "value": coarse_filter_value,
                    "level": "segments",
                }
            ]
        return ds

    def test_coarse_filter_expands_to_base(self):
        # 3 segments, each covering 2 photons (6 total).
        # segment1 (conf=-2) -> drop; segment0 and segment2 -> keep.
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.arange(6.0, dtype=np.float32),
                # link arrays: segment0->ph[0:2], segment1->ph[2:4], segment2->ph[4:6]
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
                # coarse flag: segment1 has conf=-2, others conf=4
                "/conf": np.array([4, -2, 4]),
            }
        )
        ds = self._data_source_with_levels(coarse_filter_value=-2)
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # Segments 0 and 2 survive; their photons are h[0:2] and h[4:6].
        assert df["h"].tolist() == [0.0, 1.0, 4.0, 5.0]

    def test_all_segments_filtered_returns_none(self):
        h5 = _FakeH5(
            {
                "/lat": np.arange(4.0),
                "/lon": np.arange(4.0),
                "/h": np.arange(4.0, dtype=np.float32),
                "/ph_index_beg": np.array([0, 2]),
                "/segment_ph_cnt": np.array([2, 2]),
                "/conf": np.array([-2, -2]),  # both segments dropped
            }
        )
        ds = self._data_source_with_levels(coarse_filter_value=-2)
        assert _read_group(h5, "gt1l", ds, 0, _ShardGrid()) is None

    def test_cross_level_and_base_level_filters_anded(self):
        # Cross-level keeps segments 0 and 2 (photons 0-1 and 4-5);
        # base-level h>1 further drops photon 0 and photon 4.
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.array([0.0, 1.5, 2.0, 2.5, 3.0, 4.0], dtype=np.float32),
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
                "/conf": np.array([4, -2, 4]),
                "/qs": np.array([0, 0, 1, 0, 0, 0]),  # base-level flag
            }
        )
        ds = self._data_source_with_levels(coarse_filter_value=-2)
        # Add a base-level structured filter alongside the coarse one.
        ds["filters"].append({"dataset": "/qs", "op": "eq", "value": 0})
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # Cross-level: keep ph0,1,4,5; base-level drops ph2 (qs==1 after reindex).
        # Expected survivors among ph0,1,4,5: qs[0]=0,qs[1]=0,qs[4]=0,qs[5]=0 -> all 4
        assert df["h"].tolist() == [0.0, 1.5, 3.0, 4.0]

    def test_flat_form_unchanged(self):
        # No levels/base_level -> flat path still works.
        h5 = _FakeH5(
            {
                "/lat": np.arange(3.0),
                "/lon": np.arange(3.0),
                "/h": np.array([1.0, 2.0, 3.0], dtype=np.float32),
                "/qs": np.array([0, 1, 0]),
            }
        )
        ds = {
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
            "variables": {"h": "/h"},
            "quality_filter": {"dataset": "/qs", "value": 0},
        }
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert df["h"].tolist() == [1.0, 3.0]
