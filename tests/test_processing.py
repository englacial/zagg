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
    _broadcast_segment_to_base,
    _build_groups,
    _build_output,
    _coerce_ragged_value,
    _concat_and_group,
    _empty_cell_value,
    _eval_chunk_precompute,
    _expand_mask_to_base,
    _group_columns,
    _has_ragged_fields,
    _has_vector_fields,
    _iter_carrier_columns,
    _kernel_able,
    _kernel_aggregate,
    _predicate_mask,
    _read_group,
    _read_segment_broadcasts,
    _segment_level_variables,
    calculate_cell_statistics,
    process_shard,
    write_dataframe_to_zarr,
    write_ragged_to_zarr,
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


class TestWriteShardToZarr:
    """Issue #108 phase 2: the sharded worker writes a whole shard in ONE block
    selection per dense array, byte-identical to the per-inner-chunk regular path."""

    # parent 6, child 8, chunk_inner 7 -> K = 4 inner chunks/shard, cells_per_chunk
    # 4, cells_per_shard 16. Small enough to enumerate, K>1 so sharding is valid.
    @staticmethod
    def _grids(cfg):
        from mortie import geo2mort

        kw = dict(layout="fullsphere", config=cfg, chunk_inner=7)
        sharded = HealpixGrid(6, 8, sharded=True, **kw)
        regular = HealpixGrid(6, 8, **kw)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        return sharded, regular, shard_key

    @staticmethod
    def _patch_reads(monkeypatch, df):
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def _read_df(self, grid, shard_key):
        # Two distinct cells in two distinct inner chunks of the shard, so the
        # written shard spans more than one inner read-chunk.
        children = grid.children(shard_key)
        c_first = int(children[0])  # inner chunk 0
        c_last = int(children[-1])  # inner chunk K-1
        return pd.DataFrame(
            {
                "h_li": np.array([3.0, 1.0, 7.0], dtype=np.float32),
                "s_li": np.array([0.1, 0.1, 0.1], dtype=np.float32),
                "leaf_id": np.array([c_first, c_first, c_last], dtype=np.uint64),
            }
        )

    def _run(self, grid, shard_key, df, monkeypatch):
        from zagg.processing import write_shard_to_zarr

        store = MemoryStore()
        grid.emit_template(store)
        self._patch_reads(monkeypatch, df)
        chunk_results: list = []
        process_shard(
            grid,
            shard_key,
            ["s3://x"],
            s3_credentials={},
            config=grid.config,
            chunk_results=chunk_results,
        )
        if getattr(grid, "sharded", False):
            write_shard_to_zarr(chunk_results, store, grid=grid, shard_key=shard_key)
        else:
            for block_index, carrier, _ragged in chunk_results:
                write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
        return store

    def test_sharded_matches_regular_byte_for_byte(self, monkeypatch):
        cfg = default_config()
        sharded, regular, shard_key = self._grids(cfg)
        df = self._read_df(regular, shard_key)

        s_store = self._run(sharded, shard_key, df, monkeypatch)
        r_store = self._run(regular, shard_key, df.copy(), monkeypatch)

        s_grp = open_group(store=s_store, mode="r", path="8")
        r_grp = open_group(store=r_store, mode="r", path="8")
        # The whole-shard slab contents must equal the per-inner-chunk writes.
        for name in r_grp.array_keys():
            np.testing.assert_array_equal(
                s_grp[name][:], r_grp[name][:], err_msg=f"sharded vs regular differ in {name}"
            )

    def test_one_shard_object_per_dispatch_shard(self, monkeypatch):
        cfg = default_config()
        sharded, _regular, shard_key = self._grids(cfg)
        df = self._read_df(sharded, shard_key)
        store = self._run(sharded, shard_key, df, monkeypatch)

        # Exactly one shard object per populated dense array (h_mean), not K.
        (shard_block,) = sharded.block_index(shard_key)
        h_keys = [k for k in store._store_dict if k.startswith("8/h_mean/c/")]
        assert h_keys == [f"8/h_mean/c/{shard_block}"]

    def test_readback_places_cells_at_correct_positions(self, monkeypatch):
        cfg = default_config()
        sharded, _regular, shard_key = self._grids(cfg)
        df = self._read_df(sharded, shard_key)
        store = self._run(sharded, shard_key, df, monkeypatch)

        grp = open_group(store=store, mode="r", path="8")
        children = sharded.children(shard_key)
        cell_ids = sharded.encode_cell_ids(children)
        first, last = int(cell_ids[0]), int(cell_ids[-1])
        # Populated cells carry data; an interior empty cell stays NaN fill.
        assert grp["count"][first] == 2  # two photons in the first cell
        assert grp["count"][last] == 1
        assert np.isnan(grp["h_mean"][int(cell_ids[1])])


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


class TestRaggedCsrWrite:
    """Issue #48 phase 4b: cell-resolution ragged (CSR) fields are threaded out of
    ``process_shard`` via ``ragged_out`` and persisted by ``write_ragged_to_zarr``,
    then read back through the standard ``read_csr`` layout the tensor reader
    consumes (``{group_path}/{field}/{shard_key}/values|offsets|cell_ids``)."""

    @staticmethod
    def _ragged_cfg():
        """One ragged field (sorted per-cell h_li) plus a scalar, on a 'g' group."""
        from zagg.config import PipelineConfig

        return PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                "variables": {
                    "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                    "h_raw": {
                        "function": "np.sort",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "dtype": "float32",
                    },
                }
            },
        )

    def _patch_reads(self, monkeypatch, df):
        """Return ``df`` for the first group read, then ``None`` (one granule)."""
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    @staticmethod
    def _shard_grid(cfg):
        """A fullsphere HEALPix grid + a valid (nonzero-morton) shard key."""
        from mortie import geo2mort

        grid = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        return grid, shard_key

    def test_ragged_out_collects_payloads_and_indices(self, monkeypatch):
        """``ragged_out`` is filled with ``(values_list, cell_ids)`` for each ragged
        field — one entry per *populated* cell, at the cell's chunk position."""
        cfg = self._ragged_cfg()
        grid, shard_key = self._shard_grid(cfg)
        # Build a read whose photons fall into two distinct child cells of the shard.
        children = grid.children(shard_key)
        c0, c1 = int(children[0]), int(children[5])
        df = pd.DataFrame(
            {
                "h_li": np.array([3.0, 1.0, 2.0, 9.0], dtype=np.float32),
                "leaf_id": np.array([c0, c0, c1, c1], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        ragged: dict = {}
        df_out, meta = process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, ragged_out=ragged
        )
        # 2-tuple return preserved; ragged delivered out-of-band.
        assert isinstance(df_out, pd.DataFrame)
        assert "h_raw" in ragged
        values_list, cell_ids = ragged["h_raw"]
        # Two populated cells; their payloads are the per-cell sorted h_li.
        assert len(values_list) == 2 and len(cell_ids) == 2
        assert cell_ids == [0, 5]
        np.testing.assert_array_equal(values_list[0].reshape(-1), [1.0, 3.0])
        np.testing.assert_array_equal(values_list[1].reshape(-1), [2.0, 9.0])

    def test_ragged_out_none_is_byte_identical(self, monkeypatch):
        """Passing no ``ragged_out`` (the default) is byte-for-byte the old path:
        the dense return is unchanged and no CSR collection escapes."""
        cfg = self._ragged_cfg()
        grid, shard_key = self._shard_grid(cfg)
        children = grid.children(shard_key)
        df = pd.DataFrame(
            {
                "h_li": np.array([3.0, 1.0], dtype=np.float32),
                "leaf_id": np.array([int(children[0])] * 2, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)
        result = process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg)
        # Still a 2-tuple; the dense carrier is a DataFrame (ragged excluded).
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)

    def test_end_to_end_write_then_read_csr(self, monkeypatch):
        """Full path: process_shard → write_ragged_to_zarr → read_csr returns the
        per-cell payloads at the ``{group_path}/{field}/{shard_key}`` CSR layout."""
        from zarr.storage import MemoryStore

        from zagg.csr import iter_csr_cells, read_csr

        cfg = self._ragged_cfg()
        grid, shard_key = self._shard_grid(cfg)
        children = grid.children(shard_key)
        c0, c1 = int(children[1]), int(children[3])
        df = pd.DataFrame(
            {
                "h_li": np.array([5.0, 4.0, 7.0], dtype=np.float32),
                "leaf_id": np.array([c0, c0, c1], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        # Emit the real product template first: a ragged field must NOT get a
        # dense array at ``{group_path}/h_raw`` (which would make the CSR per-shard
        # child groups collide with an array node). The dense scalar h_min still
        # gets its array.
        store = MemoryStore()
        grid.emit_template(store)
        import zarr

        product = zarr.open_group(store, path=grid.group_path, mode="r")
        assert "h_min" in product.array_keys()
        assert "h_raw" not in product.array_keys()

        ragged: dict = {}
        process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, ragged_out=ragged)
        write_ragged_to_zarr(ragged, store, grid=grid, shard_key=shard_key)

        csr = read_csr(store, f"{grid.group_path}/h_raw/{shard_key}")
        cells = dict(iter_csr_cells(csr))
        # Cell positions 1 and 3 are populated; their payloads are the sorted h_li.
        assert sorted(cells) == [1, 3]
        np.testing.assert_array_equal(cells[1].reshape(-1), [4.0, 5.0])
        np.testing.assert_array_equal(cells[3].reshape(-1), [7.0])
        # The values array carries the declared dtype.
        assert csr["values"].dtype == np.dtype("float32")

    def test_write_ragged_empty_is_noop(self):
        """An empty ``ragged`` dict writes nothing and returns the store."""
        from zarr.storage import MemoryStore

        cfg = self._ragged_cfg()
        grid, _shard_key = self._shard_grid(cfg)
        store = MemoryStore()
        out = write_ragged_to_zarr({}, store, grid=grid, shard_key=0)
        assert out is store


class TestRaggedChunkCompanion:
    """Issue #82 phase 4c: a ``kind: ragged`` + ``resolution: chunk`` field stores
    ONE variable-length payload per chunk (collapsed from the populated cells under
    the same chunk-uniform contract as scalar/vector companions), written as a
    single-entry CSR."""

    @staticmethod
    def _chunk_ragged_cfg():
        """A chunk-resolution ragged field anchored on a chunk_precompute value, so
        every populated cell carries the identical (chunk-uniform) payload."""
        from zagg.config import PipelineConfig

        return PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                # The chunk anchor is a fixed 3-vector reduced once over the shard.
                "chunk_precompute": {
                    "chunk_edges": {
                        "expression": "np.array([0.0, 5.0, 10.0], dtype=np.float32)",
                        "source": "h_li",
                    }
                },
                "variables": {
                    "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                    # bare chunk-anchor name -> every cell gets the same payload.
                    "h_chunk_edges": {
                        "expression": "chunk_edges",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "resolution": "chunk",
                        "dtype": "float32",
                    },
                },
            },
        )

    def _patch_reads(self, monkeypatch, df):
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_healpix_chunk_ragged_collapses_to_one_payload(self, monkeypatch):
        """HEALPix: a chunk-resolution ragged field writes ONE chunk payload, even
        with several populated cells, as a single-entry CSR (cell_ids == [0])."""
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.csr import iter_csr_cells, read_csr

        cfg = self._chunk_ragged_cfg()
        grid = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        children = grid.children(shard_key)
        # Three populated cells; the chunk anchor is shared across all of them.
        c0, c1, c2 = int(children[0]), int(children[2]), int(children[7])
        df = pd.DataFrame(
            {
                "h_li": np.array([1.0, 2.0, 3.0], dtype=np.float32),
                "leaf_id": np.array([c0, c1, c2], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        ragged: dict = {}
        process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, ragged_out=ragged)
        write_ragged_to_zarr(ragged, store, grid=grid, shard_key=shard_key)

        cells = dict(
            iter_csr_cells(read_csr(store, f"{grid.group_path}/h_chunk_edges/{shard_key}"))
        )
        # Exactly one chunk payload, keyed at the lone chunk position 0.
        assert list(cells) == [0]
        np.testing.assert_array_equal(cells[0].reshape(-1), [0.0, 5.0, 10.0])

    def test_chunk_ragged_non_uniform_raises(self):
        """A chunk-resolution ragged field whose populated cells disagree raises —
        it genuinely varies per cell, so resolution: chunk is a misconfiguration."""
        from zarr.storage import MemoryStore

        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                "variables": {
                    "h_raw": {
                        "function": "np.sort",
                        "source": "h_li",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "resolution": "chunk",
                        "dtype": "float32",
                    }
                }
            },
        )
        grid = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        # Two populated cells carry DIFFERENT sorted payloads -> not chunk-uniform.
        ragged = {"h_raw": ([np.array([[1.0]]), np.array([[2.0], [3.0]])], [0, 1])}
        store = MemoryStore()
        with pytest.raises(ValueError, match="not chunk-uniform"):
            write_ragged_to_zarr(ragged, store, grid=grid, shard_key=1)

    def test_chunk_ragged_template_has_no_dense_array(self):
        """The chunk-resolution ragged field gets NO dense companion array in the
        template (it is CSR), so its name stays a free group prefix."""
        import zarr
        from zarr.storage import MemoryStore

        cfg = self._chunk_ragged_cfg()
        grid = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)
        product = zarr.open_group(store, path=grid.group_path, mode="r")
        assert "h_min" in product.array_keys()
        assert "h_chunk_edges" not in product.array_keys()

    def test_rectilinear_chunk_ragged_roundtrip(self, monkeypatch):
        """Rectilinear: a chunk-resolution ragged field collapses + round-trips the
        same as HEALPix (grid-agnostic CSR seam)."""
        from zarr.storage import MemoryStore

        from zagg.config import PipelineConfig
        from zagg.csr import iter_csr_cells, read_csr
        from zagg.grids import from_config

        cfg = PipelineConfig(
            data_source={"groups": ["g"]},
            aggregation={
                "chunk_precompute": {
                    "chunk_edges": {
                        "expression": "np.array([1.0, 2.0], dtype=np.float32)",
                        "source": "h_li",
                    }
                },
                "variables": {
                    "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
                    "h_chunk_edges": {
                        "expression": "chunk_edges",
                        "kind": "ragged",
                        "inner_shape": [1],
                        "resolution": "chunk",
                        "dtype": "float32",
                    },
                },
            },
            output={
                "grid": {
                    "type": "rectilinear",
                    "crs": "EPSG:4326",
                    "resolution": [1.0, 1.0],
                    "bounds": [-4.0, -4.0, 4.0, 4.0],
                    "chunk_shape": [4, 4],
                }
            },
        )
        grid = from_config(cfg)
        # Pick a shard and two child cells within it.
        shard_key = 0
        children = grid.children(shard_key)
        c0, c1 = int(children[0]), int(children[3])
        df = pd.DataFrame(
            {
                "h_li": np.array([1.0, 2.0], dtype=np.float32),
                "leaf_id": np.array([c0, c1], dtype=np.int64),
            }
        )
        self._patch_reads(monkeypatch, df)

        import zarr

        store = MemoryStore()
        grid.emit_template(store)
        # Rect: the chunk-ragged field gets NO dense array either (CSR group prefix).
        product = zarr.open_group(store, path=grid.group_path, mode="r")
        assert "h_min" in product.array_keys()
        assert "h_chunk_edges" not in product.array_keys()

        ragged: dict = {}
        process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, ragged_out=ragged)
        write_ragged_to_zarr(ragged, store, grid=grid, shard_key=shard_key)

        cells = dict(
            iter_csr_cells(read_csr(store, f"{grid.group_path}/h_chunk_edges/{shard_key}"))
        )
        assert list(cells) == [0]
        np.testing.assert_array_equal(cells[0].reshape(-1), [1.0, 2.0])


class TestMultiChunkWorker:
    """Issue #30 item 3: one worker (one shard) owns K = grid.chunks_per_shard finer
    Zarr chunks. process_shard reads granules once and returns one carrier + ragged
    per chunk via ``chunk_results``; the runner writes K regions + K companions.
    K==1 (chunk_inner unset) is byte-identical to the single-chunk path."""

    @staticmethod
    def _scalar_cfg(chunk_inner=None):
        """atl06-style coords + a scalar config, optionally with a finer chunk_inner."""
        from zagg.config import default_config

        cfg = default_config("atl06")
        grid = {"type": "healpix", "parent_order": 6, "child_order": 8}
        if chunk_inner is not None:
            grid["chunk_inner"] = chunk_inner
        cfg.output["grid"] = grid
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            "count": {"function": "len", "source": "h_li"},
        }
        return cfg

    def _patch_reads(self, monkeypatch, df):
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_k_gt_1_yields_one_carrier_per_chunk(self, monkeypatch):
        """K=4: process_shard returns 4 chunk_results, each a carrier at its own
        block index; one photon per chunk lands in the right chunk region."""
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=7)  # parent 6, chunk 7, child 8 -> K=4
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        # One photon in the first cell of each finer chunk, distinct heights.
        leaf = [int(cc[0]) for _b, cc in chunks]
        df = pd.DataFrame(
            {
                "h_li": np.array([10.0, 11.0, 12.0, 13.0], dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        _df, meta = process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 4
        from zagg.processing import write_dataframe_to_zarr

        for block_index, carrier, _ragged in results:
            assert len(carrier) == grid.cells_per_chunk
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)

        h_min = zarr.open_array(store, path="8/h_min", mode="r")[:]
        populated = sorted(h_min[~np.isnan(h_min)].tolist())
        assert populated == [10.0, 11.0, 12.0, 13.0]
        assert meta["cells_with_data"] == 4

    def test_k_gt_1_without_chunk_results_raises(self, monkeypatch):
        """A K>1 grid called without a chunk_results sink raises (the K carriers
        cannot be returned through the single df_out)."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=7)
        grid = from_config(cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        df = pd.DataFrame(
            {
                "h_li": np.array([1.0], dtype=np.float32),
                "leaf_id": np.array([int(grid.children(shard_key)[0])], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)
        with pytest.raises(ValueError, match="chunks_per_shard"):
            process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg)

    def test_k1_byte_identical_to_chunk_results_path(self, monkeypatch):
        """K==1: the carrier from the default 2-tuple return equals the lone
        chunk_results carrier — the chunk_results plumbing changes nothing at K==1."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=None)  # K == 1
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 1
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        children = grid.children(shard_key)
        df = pd.DataFrame(
            {
                "h_li": np.array([5.0, 6.0], dtype=np.float32),
                "leaf_id": np.array([int(children[0]), int(children[1])], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)
        df_default, _ = process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg)

        self._patch_reads(monkeypatch, df.copy())
        results: list = []
        process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 1
        _block, carrier, _ragged = results[0]
        pd.testing.assert_frame_equal(
            df_default.reset_index(drop=True), carrier.reset_index(drop=True)
        )

    def test_k_gt_1_with_chunk_companion_and_ragged(self, monkeypatch):
        """K>1 with a resolution: chunk scalar companion AND a cell-resolution ragged
        field: each chunk writes its own companion slice + CSR group."""
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.config import default_config
        from zagg.csr import read_csr
        from zagg.grids import from_config
        from zagg.processing import write_dataframe_to_zarr, write_ragged_to_zarr

        cfg = default_config("atl06")
        cfg.output["grid"] = {
            "type": "healpix",
            "parent_order": 6,
            "chunk_inner": 7,
            "child_order": 8,
        }
        cfg.aggregation["chunk_precompute"] = {
            "anchor": {"expression": "np.float32(np.min(h_li))", "source": "h_li"}
        }
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            # chunk-resolution scalar companion (bare anchor name).
            "offset_h": {
                "expression": "anchor",
                "resolution": "chunk",
                "dtype": "float32",
            },
            # cell-resolution ragged.
            "h_raw": {
                "function": "np.sort",
                "source": "h_li",
                "kind": "ragged",
                "inner_shape": [1],
                "dtype": "float32",
            },
        }
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        leaf = [int(cc[0]) for _b, cc in chunks]
        df = pd.DataFrame(
            {
                "h_li": np.array([20.0, 21.0, 22.0, 23.0], dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 4
        n_csr_groups = 0
        for block_index, carrier, ragged in results:
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
            # Each chunk's ragged keyed by its own block index (K>1).
            key = int(block_index[0])
            write_ragged_to_zarr(ragged, store, grid=grid, shard_key=key)
            if ragged.get("h_raw") and ragged["h_raw"][0]:
                csr = read_csr(store, f"{grid.group_path}/h_raw/{key}")
                assert csr["values"].size > 0
                n_csr_groups += 1
        # offset_h companion: 4 distinct chunk slices populated.
        offset = zarr.open_array(store, path="8/offset_h", mode="r")[:]
        assert int(np.count_nonzero(~np.isnan(offset))) == 4
        # Each chunk had one populated cell -> one CSR group per chunk.
        assert n_csr_groups == 4

    def test_chunk_precompute_is_per_chunk_not_shard_pooled(self, monkeypatch):
        """Issue #82 phase 6: a ``chunk_precompute`` anchor is reduced over EACH
        finer Zarr chunk's own observations, not the whole pooled shard. Build a
        shard whose K=4 inner chunks hold disjoint value ranges and assert the
        stored per-chunk gain/offset companions DIFFER (each == its chunk's own
        min), where a shard-pooled anchor would have stored one shared value."""
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.config import default_config
        from zagg.grids import from_config
        from zagg.processing import write_dataframe_to_zarr

        cfg = default_config("atl06")
        cfg.output["grid"] = {
            "type": "healpix",
            "parent_order": 6,
            "chunk_inner": 7,
            "child_order": 8,
        }
        # gain/offset basis case: the anchor is min(h_li) over the chunk.
        cfg.aggregation["chunk_precompute"] = {
            "anchor": {"expression": "np.float32(np.min(h_li))", "source": "h_li"}
        }
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            "offset_h": {"expression": "anchor", "resolution": "chunk", "dtype": "float32"},
        }
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        # Two photons in the first cell of each chunk; each chunk's range is offset
        # by +100, so per-chunk mins are 100, 200, 300, 400 (shard min would be 100).
        leaf, h = [], []
        per_chunk_min = []
        for k, (_b, cc) in enumerate(chunks):
            base = 100.0 * (k + 1)
            leaf += [int(cc[0]), int(cc[0])]
            h += [base + 5.0, base]  # min is ``base``
            per_chunk_min.append(base)
        df = pd.DataFrame(
            {
                "h_li": np.array(h, dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 4
        for block_index, carrier, _ragged in results:
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
        # offset_h is a resolution: chunk companion: one value per chunk, indexed by
        # the chunk's block index. Read each chunk's stored anchor.
        offset = zarr.open_array(store, path="8/offset_h", mode="r")
        got = [float(offset[block_index]) for block_index, _c, _r in results]
        # Per-chunk anchors DIFFER and equal each chunk's own min — not the single
        # shard-pooled min (100.0) that the old shard-level reduction would store.
        assert got == per_chunk_min
        assert len(set(got)) == 4

    def test_chunk_precompute_empty_inner_chunk_gets_nan_anchor(self, monkeypatch):
        """Issue #82 phase 6 (review fold): an EMPTY inner chunk must not raise.
        ``iter_chunks`` yields all K chunks including those with zero observations,
        and the canonical ``np.float32(np.min(h_li))`` anchor raises ``ValueError``
        over an empty array. Populate only some chunks, leaving ≥1 empty, and assert
        the empty chunk's stored anchor is NaN (and the run does NOT raise)."""
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.config import default_config
        from zagg.grids import from_config
        from zagg.processing import write_dataframe_to_zarr

        cfg = default_config("atl06")
        cfg.output["grid"] = {
            "type": "healpix",
            "parent_order": 6,
            "chunk_inner": 7,
            "child_order": 8,
        }
        cfg.aggregation["chunk_precompute"] = {
            "anchor": {"expression": "np.float32(np.min(h_li))", "source": "h_li"}
        }
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            "offset_h": {"expression": "anchor", "resolution": "chunk", "dtype": "float32"},
        }
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        # Populate ONLY the first two chunks; the last two stay empty (zero obs).
        populated = {0, 1}
        leaf, h = [], []
        for k, (_b, cc) in enumerate(chunks):
            if k not in populated:
                continue
            base = 100.0 * (k + 1)
            leaf += [int(cc[0]), int(cc[0])]
            h += [base + 5.0, base]
        df = pd.DataFrame(
            {
                "h_li": np.array(h, dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        # The empty-chunk anchor would raise ValueError without the n_obs==0 guard.
        process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 4
        for block_index, carrier, _ragged in results:
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
        offset = zarr.open_array(store, path="8/offset_h", mode="r")
        for k, (block_index, _c, _r) in enumerate(results):
            anchor = float(offset[block_index])
            if k in populated:
                assert anchor == 100.0 * (k + 1)
            else:
                # Empty chunk -> NaN anchor (the n_obs==0 short-circuit), not a raise.
                assert np.isnan(anchor)

    def test_chunk_precompute_empty_inner_chunk_arrow_kernel(self, monkeypatch):
        """Issue #82 phase 6 (review fold): the arrow-kernel path also short-circuits
        an empty inner chunk to a NaN anchor instead of raising on ``np.min`` of an
        empty subset. Mirrors the default-path empty-chunk test on
        ``handoff='arrow-kernel'`` (the once-grouped O(N) per-chunk subset path)."""
        pa = pytest.importorskip("pyarrow")
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.config import default_config
        from zagg.grids import from_config
        from zagg.processing import write_dataframe_to_zarr

        cfg = default_config("atl06")
        cfg.output["grid"] = {
            "type": "healpix",
            "parent_order": 6,
            "chunk_inner": 7,
            "child_order": 8,
        }
        cfg.aggregation["chunk_precompute"] = {
            "anchor": {"expression": "np.float32(np.min(h_li))", "source": "h_li"}
        }
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            "offset_h": {"expression": "anchor", "resolution": "chunk", "dtype": "float32"},
        }
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        populated = {0, 1}
        leaf, h = [], []
        for k, (_b, cc) in enumerate(chunks):
            if k not in populated:
                continue
            base = 100.0 * (k + 1)
            leaf += [int(cc[0]), int(cc[0])]
            h += [base + 5.0, base]
        table = pa.table(
            {
                "h_li": pa.array(np.array(h, dtype=np.float32)),
                "s_li": pa.array(np.full(len(h), 0.1, dtype=np.float32)),
                "leaf_id": pa.array(np.array(leaf, dtype=np.uint64)),
            }
        )
        it = iter([table])
        monkeypatch.setattr("zagg.processing._read_group", lambda *a, **k: next(it, None))
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        process_shard(
            grid,
            shard_key,
            ["s3://x"],
            s3_credentials={},
            config=cfg,
            chunk_results=results,
            handoff="arrow-kernel",
        )
        assert len(results) == 4
        for block_index, carrier, _ragged in results:
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
        offset = zarr.open_array(store, path="8/offset_h", mode="r")
        for k, (block_index, _c, _r) in enumerate(results):
            anchor = float(offset[block_index])
            if k in populated:
                assert anchor == 100.0 * (k + 1)
            else:
                assert np.isnan(anchor)


class TestStreamAndFreeChunkWrites:
    """Issue #91: a ``write_chunk`` callback streams each chunk write-then-free so
    the worker holds ~1 chunk instead of all K. Output must stay byte-identical to
    the accumulated ``chunk_results`` path, and K==1 is a true no-op."""

    _scalar_cfg = staticmethod(TestMultiChunkWorker._scalar_cfg)
    _patch_reads = TestMultiChunkWorker._patch_reads

    def test_streaming_output_byte_identical_to_accumulated(self, monkeypatch):
        """K=4: the carriers handed to ``write_chunk`` equal, per block index, the
        carriers the accumulated ``chunk_results`` path produced — byte-identical."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=7)  # K=4
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 4
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        leaf = [int(cc[0]) for _b, cc in chunks]
        df = pd.DataFrame(
            {
                "h_li": np.array([10.0, 11.0, 12.0, 13.0], dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )

        # Accumulated reference.
        self._patch_reads(monkeypatch, df.copy())
        acc: list = []
        process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=acc)

        # Streamed: collect what the callback received.
        self._patch_reads(monkeypatch, df.copy())
        streamed: list = []

        def _wc(block_index, carrier, ragged):
            streamed.append((block_index, carrier, ragged))

        _df, _meta = process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, write_chunk=_wc
        )

        assert len(streamed) == len(acc) == 4
        acc_by_block = {tuple(b): (c, r) for b, c, r in acc}
        for block_index, carrier, ragged in streamed:
            ref_carrier, ref_ragged = acc_by_block[tuple(block_index)]
            pd.testing.assert_frame_equal(
                carrier.reset_index(drop=True), ref_carrier.reset_index(drop=True)
            )
            assert ragged == ref_ragged

    def test_callback_fires_once_per_chunk_and_chunk_results_untouched(self, monkeypatch):
        """The callback is invoked exactly K times and the accumulating sink is
        never populated when streaming (peak output memory is ~1 chunk)."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=7)  # K=4
        grid = from_config(cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        chunks = list(grid.iter_chunks(shard_key))
        leaf = [int(cc[0]) for _b, cc in chunks]
        df = pd.DataFrame(
            {
                "h_li": np.array([10.0, 11.0, 12.0, 13.0], dtype=np.float32),
                "leaf_id": np.array(leaf, dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)

        # ``live`` tracks carriers still referenced by the callback's caller. The
        # worker drops its own refs right after the call, so if the callback also
        # drops, the carrier is collectible before the next chunk is built.
        seen_blocks: list = []
        max_live = {"n": 0}
        live: list = []

        def _wc(block_index, carrier, ragged):
            seen_blocks.append(tuple(block_index))
            live.append(carrier)
            max_live["n"] = max(max_live["n"], len(live))
            live.clear()  # consumer frees as it goes

        _df, _meta = process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, write_chunk=_wc
        )
        assert len(seen_blocks) == 4
        assert len(set(seen_blocks)) == 4  # one call per distinct chunk
        assert max_live["n"] == 1  # never more than one chunk held at a time

    def test_streaming_and_chunk_results_together_raises(self, monkeypatch):
        """Passing both sinks is ambiguous and rejected."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=7)
        grid = from_config(cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        df = pd.DataFrame(
            {
                "h_li": np.array([1.0], dtype=np.float32),
                "leaf_id": np.array([int(grid.children(shard_key)[0])], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)
        with pytest.raises(ValueError, match="either chunk_results"):
            process_shard(
                grid,
                shard_key,
                ["s3://x"],
                s3_credentials={},
                config=cfg,
                chunk_results=[],
                write_chunk=lambda *a: None,
            )

    def test_k1_streaming_is_noop_byte_identical(self, monkeypatch):
        """K==1: the lone carrier streamed through ``write_chunk`` equals the carrier
        the default 2-tuple return produces — streaming changes nothing at K==1."""
        from mortie import geo2mort

        from zagg.grids import from_config

        cfg = self._scalar_cfg(chunk_inner=None)  # K==1
        grid = from_config(cfg)
        assert grid.chunks_per_shard == 1
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        children = grid.children(shard_key)
        df = pd.DataFrame(
            {
                "h_li": np.array([5.0, 6.0], dtype=np.float32),
                "leaf_id": np.array([int(children[0]), int(children[1])], dtype=np.uint64),
            }
        )
        self._patch_reads(monkeypatch, df)
        df_default, _ = process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg)

        self._patch_reads(monkeypatch, df.copy())
        streamed: list = []
        df_out, _ = process_shard(
            grid,
            shard_key,
            ["s3://x"],
            s3_credentials={},
            config=cfg,
            write_chunk=lambda b, c, r: streamed.append((b, c, r)),
        )
        assert len(streamed) == 1
        assert df_out.empty  # streamed path returns an empty carrier
        _block, carrier, _ragged = streamed[0]
        pd.testing.assert_frame_equal(
            df_default.reset_index(drop=True), carrier.reset_index(drop=True)
        )


class TestChunkCompanionWorkedExample:
    """Issue #82 phase 5: a worked example exercising a chunk_precompute value
    stored as all three chunk-companion kinds — scalar, vector, AND ragged — and
    read back. This is the end-to-end shape the issue asked for: one
    ``chunk_precompute`` anchor surfaced into a per-chunk scalar companion, a
    per-chunk vector companion, and a per-chunk ragged (CSR) companion."""

    def _patch_reads(self, monkeypatch, df):
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_scalar_vector_ragged_chunk_companions_roundtrip(self, monkeypatch):
        import zarr
        from mortie import geo2mort
        from zarr.storage import MemoryStore

        from zagg.config import default_config
        from zagg.csr import iter_csr_cells, read_csr
        from zagg.grids import from_config
        from zagg.processing import write_dataframe_to_zarr, write_ragged_to_zarr

        cfg = default_config("atl06")
        cfg.output["grid"] = {"type": "healpix", "parent_order": 6, "child_order": 8}
        # One chunk anchor (a fixed 3-vector + a scalar derived from it), surfaced
        # into a scalar, a vector, and a ragged chunk companion.
        cfg.aggregation["chunk_precompute"] = {
            "edges": {
                "expression": "np.array([0.0, 5.0, 10.0], dtype=np.float32)",
                "source": "h_li",
            },
            "anchor": {"expression": "np.float32(np.min(h_li))", "source": "h_li"},
        }
        cfg.aggregation["variables"] = {
            "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            # scalar chunk companion (one value per chunk).
            "offset_h": {"expression": "anchor", "resolution": "chunk", "dtype": "float32"},
            # vector chunk companion (one 3-vector per chunk). Default NaN fill so
            # empty cells are NaN-ignored by the chunk-uniform collapse (a 0-fill
            # would make empty cells [0,0,0], spuriously non-uniform vs [0,5,10]).
            "edges_h": {
                "expression": "edges",
                "kind": "vector",
                "trailing_shape": 3,
                "resolution": "chunk",
                "dtype": "float32",
            },
            # ragged chunk companion (one variable-length payload per chunk).
            "edges_ragged": {
                "expression": "edges",
                "kind": "ragged",
                "inner_shape": [1],
                "resolution": "chunk",
                "dtype": "float32",
            },
        }
        grid = from_config(cfg)
        shard_key = int(geo2mort(-78.5, -132.0, order=6)[0])
        children = grid.children(shard_key)
        df = pd.DataFrame(
            {
                "h_li": np.array([3.0, 7.0, 4.0], dtype=np.float32),
                "leaf_id": np.array(
                    [int(children[0]), int(children[0]), int(children[1])], dtype=np.uint64
                ),
            }
        )
        self._patch_reads(monkeypatch, df)

        store = MemoryStore()
        grid.emit_template(store)
        results: list = []
        process_shard(
            grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg, chunk_results=results
        )
        assert len(results) == 1  # K == 1 (no chunk_inner)
        block_index, carrier, ragged = results[0]
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=block_index)
        write_ragged_to_zarr(ragged, store, grid=grid, shard_key=shard_key)

        chunk_idx = grid.block_index(shard_key)
        # scalar companion: one value at this chunk == min(h_li) == 3.0.
        offset = zarr.open_array(store, path="8/offset_h", mode="r")
        assert offset[chunk_idx] == np.float32(3.0)
        # vector companion: the 3-vector edges at this chunk.
        edges = zarr.open_array(store, path="8/edges_h", mode="r")
        np.testing.assert_array_equal(edges[chunk_idx], [0.0, 5.0, 10.0])
        # ragged companion: one CSR payload per chunk == edges.
        cells = dict(iter_csr_cells(read_csr(store, f"8/edges_ragged/{shard_key}")))
        assert list(cells) == [0]
        np.testing.assert_array_equal(cells[0].reshape(-1), [0.0, 5.0, 10.0])


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


class TestChunkResolutionCompanion:
    """Issue #30 item 2: a ``resolution: chunk`` field is written ONCE per chunk to
    a companion array shaped at the chunk grid (main.shape // chunk_shape), indexed
    by ``grid.block_index``. Works identically on HEALPix and rectilinear."""

    @staticmethod
    def _chunk_cfg(base_name):
        """atl06-derived config with one cell-resolution count and one chunk field."""
        from zagg.config import PipelineConfig

        cfg = default_config(base_name)
        agg = {
            "coordinates": cfg.aggregation.get("coordinates", {}),
            "chunk_precompute": {
                "chunk_anchor": {"expression": "np.float32(np.median(h_li))", "source": "h_li"}
            },
            "variables": {
                "count": {"function": "len", "source": "h_li"},
                "anchor_h": {"expression": "chunk_anchor", "source": "h_li", "resolution": "chunk"},
            },
        }
        return PipelineConfig(data_source=cfg.data_source, aggregation=agg, output=cfg.output)

    def test_healpix_companion_shape_and_index(self):
        """The HEALPix companion array is shaped at the chunk grid (12·4^parent),
        and a shard's single value lands at block_index = the parent nested id."""
        from mortie import geo2mort

        cfg = self._chunk_cfg("atl06")
        parent_order, child_order = 2, 4
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        # Companion shape == number of chunks (12·4^parent_order), NOT the cell grid.
        group = open_group(store=store, mode="r", path=str(child_order))
        n_chunks = HEALPIX_BASE_CELLS * 4**parent_order
        assert grid.chunk_grid_shape == (n_chunks,)
        assert group["anchor_h"].shape == (n_chunks,)
        # The cell-resolution count keeps the full cell-grid shape.
        assert group["count"].shape == (HEALPIX_BASE_CELLS * 4**child_order,)

        parent = int(geo2mort(-78.5, -132.0, order=parent_order)[0])
        children = grid.children(parent)
        n = len(children)
        # Every cell carries the chunk-uniform anchor (chunk-uniform column).
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, 42.5, dtype="float32"),
        }
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=False
        )
        chunk_idx = grid.block_index(parent)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        # Exactly ONE value per chunk, at block_index; the rest stay at fill (NaN).
        rgroup = open_group(store=store, mode="r", path=str(child_order))
        companion = rgroup["anchor_h"][:]
        assert companion[chunk_idx[0]] == np.float32(42.5)
        # All other chunks untouched (NaN fill).
        other = np.delete(companion, chunk_idx[0])
        assert np.all(np.isnan(other))
        # A reader reconstructs the chunk value without any per-cell array.
        assert rgroup["anchor_h"][chunk_idx[0]] == np.float32(42.5)

    def test_rectilinear_companion_shape_and_index(self):
        """The rectilinear companion array is shaped at the chunk grid
        (n_row_blocks, n_col_blocks) and indexed by block_index = (rb, cb)."""
        from zagg.grids import RectilinearGrid

        cfg = self._chunk_cfg("atl06")
        grid = RectilinearGrid(
            crs="EPSG:3031",
            resolution=100000.0,
            bounds=[-400000, -400000, 400000, 400000],
            chunk_shape=(4, 4),
            config=cfg,
        )
        store = MemoryStore()
        grid.emit_template(store)

        group = open_group(store=store, mode="r", path="rectilinear")
        assert group["anchor_h"].shape == grid.chunk_grid_shape
        assert grid.chunk_grid_shape == (grid.n_row_blocks, grid.n_col_blocks)
        # Cell-resolution count keeps the full 2-D cell grid.
        assert group["count"].shape == grid.array_shape

        # Pick an interior chunk and write its uniform value.
        shard_key = grid._pack(1, 1)
        children = grid.children(shard_key)
        n = len(children)
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, 7.0, dtype="float32"),
        }
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, shard_key, use_arrow=False
        )
        chunk_idx = grid.block_index(shard_key)
        assert chunk_idx == (1, 1)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        rgroup = open_group(store=store, mode="r", path="rectilinear")
        companion = rgroup["anchor_h"][:]
        assert companion[1, 1] == np.float32(7.0)
        # Exactly one written cell; the rest are NaN fill.
        assert np.count_nonzero(~np.isnan(companion)) == 1

    def test_empty_cell_in_populated_chunk_needs_no_per_cell_value(self):
        """With resolution: chunk, an empty cell in a populated chunk needs NO
        per-cell value — the chunk anchor is stored once and read back regardless of
        which cells are empty (issue #30 item 2 retires the per-cell band-aid)."""
        from mortie import geo2mort

        cfg = self._chunk_cfg("atl06")
        parent_order, child_order = 2, 4
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        parent = int(geo2mort(-78.5, -132.0, order=parent_order)[0])
        children = grid.children(parent)
        n = len(children)
        # Only cell 0 has photons; every other cell is empty. The anchor column is
        # still chunk-uniform (empty cells carry the anchor too — phase-4 behavior).
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, 13.0, dtype="float32"),
        }
        stats["count"][0] = 4
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=False
        )
        chunk_idx = grid.block_index(parent)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        rgroup = open_group(store=store, mode="r", path=str(child_order))
        # One companion value for the whole chunk; reading it does not depend on any
        # per-cell anchor array — there is no cell-resolution anchor_h array at all.
        assert rgroup["anchor_h"].shape == (HEALPIX_BASE_CELLS * 4**parent_order,)
        assert rgroup["anchor_h"][chunk_idx[0]] == np.float32(13.0)

    def test_worked_template_emits_chunk_companions(self, monkeypatch):
        """The shipped atl03_waveform_chunk template, run end-to-end through the
        worker and written to a real Zarr store, stores offset_h/gain_h as
        chunk-resolution companions (one value per chunk), while waveform_counts
        stays a per-cell vector array (issue #30 items 1+2)."""
        pytest.importorskip("pyarrow")
        from zagg.grids import RectilinearGrid

        cfg = default_config("atl03_waveform_chunk")
        # Small rect grid so a real template fits in memory; one 2x2 chunk holds the
        # two populated cells (the worker fabricates the per-cell loop via canned
        # reads as in TestChunkPrecompute).
        grid = RectilinearGrid(
            crs="EPSG:4326",
            resolution=1.0,
            bounds=[0, 0, 2, 2],
            chunk_shape=(2, 2),
            config=cfg,
        )
        store = MemoryStore()
        grid.emit_template(store)

        # Companion arrays are at the chunk grid; waveform_counts keeps cell+trailing.
        group = open_group(store=store, mode="r", path="rectilinear")
        assert group["offset_h"].shape == grid.chunk_grid_shape
        assert group["gain_h"].shape == grid.chunk_grid_shape
        assert group["waveform_counts"].shape == (*grid.array_shape, 128)

        # Drive process_shard over chunk (0,0): children are the 4 cells of the
        # top-left 2x2 block; place photons in two of them.
        shard_key = grid._pack(0, 0)
        children = grid.children(shard_key)
        # vector field present -> default pandas carrier path; feed a DataFrame read.
        # dem_h (the DEM anchor) rides alongside h_ph as a pooled column (issue #30).
        dem = np.array([50.0, 50.0, 60.0, 60.0], dtype=np.float32)
        df = pd.DataFrame(
            {
                "h_ph": np.array([10.0, 12.0, 200.0, 202.0], dtype=np.float32),
                "dem_h": dem,
                "leaf_id": np.array(
                    [children[0], children[0], children[1], children[1]], dtype=np.int64
                ),
            }
        )

        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

        carrier, _meta = process_shard(grid, shard_key, ["s3://x"], s3_credentials={}, config=cfg)
        chunk_idx = grid.block_index(shard_key)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        rgroup = open_group(store=store, mode="r", path="rectilinear")
        # offset_h companion: one value at this chunk = floor(min(pooled dem_h)).
        expected_offset = np.float32(np.floor(np.min(dem)))
        assert rgroup["offset_h"][chunk_idx] == expected_offset
        assert not np.isnan(rgroup["gain_h"][chunk_idx])
        # Only one chunk written for each companion.
        assert np.count_nonzero(~np.isnan(rgroup["offset_h"][:])) == 1
        assert np.count_nonzero(~np.isnan(rgroup["gain_h"][:])) == 1

    def test_dense_healpix_companion_at_populated_shard_position(self):
        """Dense HEALPix layout: the companion is shaped (n_shards,) and a shard's
        value lands at its position in populated_shards (block_index), not at the
        parent nested id (issue #30 item 2; fold of review finding)."""
        from mortie import geo2mort

        cfg = self._chunk_cfg("atl06")
        shards = [
            int(geo2mort(la, lo, order=6)[0])
            for la, lo in [(-78.5, -132.0), (-72.1, 25.4), (-65.0, -45.0)]
        ]
        grid = HealpixGrid(6, 8, layout="dense", config=cfg, populated_shards=shards)
        store = MemoryStore()
        grid.emit_template(store)
        group = open_group(store=store, mode="r", path="8")
        assert grid.chunk_grid_shape == (len(shards),)
        assert group["anchor_h"].shape == (len(shards),)

        parent = shards[1]
        n = len(grid.children(parent))
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, 99.0, dtype="float32"),
        }
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=False
        )
        chunk_idx = grid.block_index(parent)
        assert chunk_idx == (1,)  # position in populated_shards, not nested id
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)
        companion = open_group(store=store, mode="r", path="8")["anchor_h"][:]
        assert companion[1] == np.float32(99.0)
        assert np.count_nonzero(~np.isnan(companion)) == 1

    def test_empty_cell0_compound_expr_writes_chunk_value_not_nan(self):
        """Fold of review [MED]: with a COMPOUND resolution: chunk expression (not a
        bare precompute name), an empty cell 0 used to poison the companion with NaN
        because the writer took flat[0]. The writer now selects a populated cell's
        value, so the companion records the real chunk value."""
        from mortie import geo2mort

        from zagg.config import PipelineConfig

        base = default_config("atl06")
        agg = {
            "coordinates": base.aggregation.get("coordinates", {}),
            "chunk_precompute": {
                "chunk_anchor": {"expression": "np.float32(np.median(h_li))", "source": "h_li"}
            },
            "variables": {
                "count": {"function": "len", "source": "h_li"},
                # compound expression (NOT a bare identifier) -> empty cells get NaN,
                # not the anchor, so the writer must skip them.
                "anchor_h": {
                    "expression": "chunk_anchor + np.float32(1.0)",
                    "source": "h_li",
                    "resolution": "chunk",
                },
            },
        }
        cfg = PipelineConfig(data_source=base.data_source, aggregation=agg, output=base.output)
        parent_order, child_order = 2, 4
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        parent = int(geo2mort(-78.5, -132.0, order=parent_order)[0])
        n = len(grid.children(parent))
        # cell 0 empty (NaN), only cell 3 populated with the chunk value 50.0.
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, np.nan, dtype="float32"),
        }
        stats["count"][3] = 5
        stats["anchor_h"][3] = 50.0
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=False
        )
        chunk_idx = grid.block_index(parent)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)
        companion = open_group(store=store, mode="r", path=str(child_order))["anchor_h"][:]
        assert companion[chunk_idx[0]] == np.float32(50.0)  # the populated value, not NaN
        assert not np.isnan(companion[chunk_idx[0]])

    def test_non_uniform_chunk_resolution_column_raises(self):
        """Fold of review [MED]: a resolution: chunk field whose per-cell values are
        NOT uniform (a misconfiguration) is rejected with a clear error instead of
        silently dropping every cell but the first."""
        from mortie import geo2mort

        cfg = self._chunk_cfg("atl06")
        grid = HealpixGrid(2, 4, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)
        parent = int(geo2mort(-78.5, -132.0, order=2)[0])
        n = len(grid.children(parent))
        # Two populated cells with DIFFERENT values -> not chunk-uniform.
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "anchor_h": np.full(n, np.nan, dtype="float32"),
        }
        stats["anchor_h"][0] = 1.0
        stats["anchor_h"][1] = 2.0
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=False
        )
        chunk_idx = grid.block_index(parent)
        with pytest.raises(ValueError, match="not chunk-uniform"):
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)


class TestVectorChunkResolutionCompanion:
    """Issue #82: a ``kind: vector`` ``resolution: chunk`` field stores ONE
    trailing-shaped vector per chunk (companion shape = chunk grid + trailing,
    chunked whole on trailing), indexed by ``grid.block_index``."""

    @staticmethod
    def _vec_chunk_cfg(base_name, width=8):
        from zagg.config import PipelineConfig

        cfg = default_config(base_name)
        agg = {
            "coordinates": cfg.aggregation.get("coordinates", {}),
            "chunk_precompute": {
                "chunk_profile": {
                    "expression": f"np.arange({width}).astype('float32')",
                    "source": "h_li",
                }
            },
            "variables": {
                "count": {"function": "len", "source": "h_li"},
                "profile_h": {
                    "kind": "vector",
                    "trailing_shape": width,
                    "expression": "chunk_profile",
                    "source": "h_li",
                    "resolution": "chunk",
                },
            },
        }
        return PipelineConfig(data_source=cfg.data_source, aggregation=agg, output=cfg.output)

    def test_healpix_vector_companion_shape_and_index(self):
        from mortie import geo2mort

        width = 8
        cfg = self._vec_chunk_cfg("atl06", width=width)
        parent_order, child_order = 2, 4
        grid = HealpixGrid(parent_order, child_order, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)

        group = open_group(store=store, mode="r", path=str(child_order))
        n_chunks = HEALPIX_BASE_CELLS * 4**parent_order
        # Companion shape = (n_chunks, width); trailing chunked whole.
        assert group["profile_h"].shape == (n_chunks, width)
        assert group["profile_h"].chunks == (1, width)
        assert grid.spec().members["profile_h"].dimension_names == ("chunks", "vector")

        parent = int(geo2mort(-78.5, -132.0, order=parent_order)[0])
        n = len(grid.children(parent))
        profile = np.arange(width, dtype="float32")
        # Every populated cell carries the same chunk vector (chunk-uniform).
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "profile_h": np.tile(profile, (n, 1)).astype("float32"),
        }
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=True
        )
        chunk_idx = grid.block_index(parent)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        rgroup = open_group(store=store, mode="r", path=str(child_order))
        companion = rgroup["profile_h"][:]
        # Exactly one chunk row written with the profile; the rest NaN.
        np.testing.assert_array_equal(companion[chunk_idx[0]], profile)
        other = np.delete(companion, chunk_idx[0], axis=0)
        assert np.all(np.isnan(other))

    def test_rectilinear_vector_companion_shape_and_index(self):
        from zagg.grids import RectilinearGrid

        width = 5
        cfg = self._vec_chunk_cfg("atl06", width=width)
        grid = RectilinearGrid(
            crs="EPSG:3031",
            resolution=100000.0,
            bounds=[-400000, -400000, 400000, 400000],
            chunk_shape=(4, 4),
            config=cfg,
        )
        store = MemoryStore()
        grid.emit_template(store)
        group = open_group(store=store, mode="r", path="rectilinear")
        assert group["profile_h"].shape == (grid.n_row_blocks, grid.n_col_blocks, width)
        assert group["profile_h"].chunks == (1, 1, width)

        shard_key = grid._pack(1, 1)
        n = len(grid.children(shard_key))
        profile = (np.arange(width) + 0.5).astype("float32")
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "profile_h": np.tile(profile, (n, 1)).astype("float32"),
        }
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, shard_key, use_arrow=True
        )
        chunk_idx = grid.block_index(shard_key)
        write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

        rgroup = open_group(store=store, mode="r", path="rectilinear")
        companion = rgroup["profile_h"][:]
        np.testing.assert_array_equal(companion[1, 1], profile)
        # Only one (rb, cb) row written.
        written = ~np.all(np.isnan(companion), axis=2)
        assert np.count_nonzero(written) == 1

    def test_per_cell_varying_vector_raises(self):
        """A vector chunk field whose populated cells carry DIFFERENT vectors is a
        misconfiguration -> clear non-uniform error (per-element over trailing)."""
        from mortie import geo2mort

        width = 4
        cfg = self._vec_chunk_cfg("atl06", width=width)
        grid = HealpixGrid(2, 4, layout="fullsphere", config=cfg)
        store = MemoryStore()
        grid.emit_template(store)
        parent = int(geo2mort(-78.5, -132.0, order=2)[0])
        n = len(grid.children(parent))
        stats = {
            "count": np.zeros(n, dtype="float32"),
            "profile_h": np.full((n, width), np.nan, dtype="float32"),
        }
        stats["profile_h"][0] = np.arange(width)
        stats["profile_h"][1] = np.arange(width) + 1.0  # differs in the trailing axis
        carrier = _build_output(
            stats, get_data_vars(cfg), get_agg_fields(cfg), grid, parent, use_arrow=True
        )
        chunk_idx = grid.block_index(parent)
        with pytest.raises(ValueError, match="not chunk-uniform"):
            write_dataframe_to_zarr(carrier, store, grid=grid, chunk_idx=chunk_idx)

    def test_partially_nan_but_uniform_vector_accepted(self):
        """A vector whose populated cells all carry the SAME partially-NaN vector is
        chunk-uniform (the NaN positions match) — the reason _chunk_uniform_value
        compares with equal_nan=True. Returns that vector, NaNs preserved."""
        from zagg.processing import _chunk_uniform_value

        vec = np.array([1.0, np.nan, 3.0, np.nan], dtype="float32")
        # 5 cells: cells 0,2 populated with the same partially-NaN vec; rest all-NaN.
        col = np.full((5, 4), np.nan, dtype="float32")
        col[0] = vec
        col[2] = vec
        out = _chunk_uniform_value("profile_h", col)
        np.testing.assert_array_equal(out, vec)  # array_equal treats NaN==NaN

    def test_all_nan_vector_chunk_falls_back_to_first(self):
        """When every cell's vector is all-NaN (empty chunk), _chunk_uniform_value
        returns the first cell's (all-NaN) sentinel rather than raising."""
        from zagg.processing import _chunk_uniform_value

        col = np.full((3, 4), np.nan, dtype="float32")
        out = np.asarray(_chunk_uniform_value("profile_h", col))
        assert out.shape == (4,)
        assert np.all(np.isnan(out))


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
                [4, 4, 4, 4, 4],  # all-high-confidence (kept)
                [-2, -2, -2, -2, -2],  # TEP across all surface types (dropped)
                [0, 0, 0, 0, 0],  # noise across all (kept; -2 is the TEP flag)
                [3, 2, 1, 0, 4],  # mixed confidence (kept)
                [-2, 4, 4, 4, 4],  # column 0 is TEP but others aren't -- with
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


# ---------------------------------------------------------------------------
# Planned-read path (issue #43, Phase C — read_plan wiring into _read_group)
# ---------------------------------------------------------------------------


class _BboxGrid:
    """Permissive grid stub: ``shard_footprint`` returns the bbox polygon,
    every photon read is in shard 0. Keeps tests focused on what the planned
    read returns (the IO-bounded slice + filters), not on a spatial-mask
    re-filter we'd need to model separately.
    """

    def __init__(self, bbox, shard_key=0):
        from shapely.geometry import box as _box

        self.bbox = tuple(float(v) for v in bbox)
        self._poly = _box(*self.bbox)
        self._shard_key = shard_key

    def shard_footprint(self, shard_key):
        return self._poly

    def assign(self, lats, lons):
        return np.arange(len(lats))

    def shards_of(self, leaf_ids):
        return np.full(len(leaf_ids), self._shard_key, dtype=int)


class _LatBboxGrid(_BboxGrid):
    """Strict variant: ``shards_of`` keeps a photon only when its lat (carried
    via ``assign``'s returned leaf id) falls inside the bbox lat range. Used
    for the planned-vs-full parity test, where the spatial mask must agree
    between paths."""

    def assign(self, lats, lons):
        # Stash lat as the leaf id; `shards_of` decodes it. Works because the
        # test fixture has distinct lats. Real grids use cell ids.
        return np.asarray(lats, dtype=np.float64)

    def shards_of(self, leaf_ids):
        min_lon, min_lat, max_lon, max_lat = self.bbox
        in_shard = (leaf_ids >= min_lat) & (leaf_ids <= max_lat)
        out = np.full(len(leaf_ids), -1, dtype=int)
        out[in_shard] = self._shard_key
        return out


def _planned_read_data_source(*, with_base_filter=False, with_coarse_filter=False):
    """Multi-level data source for the planned-read tests.

    Six segments × 2 photons/segment = 12 photons. The segment-level
    rep-point coordinates live at /seg/lat,/seg/lon; the base-level photon
    coords at /heights/lat_ph,/heights/lon_ph; the link arrays at
    /seg/ph_index_beg + /seg/segment_ph_cnt (0-based contiguous).
    """
    ds = {
        "coordinates": {
            "latitude": "/heights/lat_ph",
            "longitude": "/heights/lon_ph",
        },
        "variables": {"h": "/heights/h"},
        "base_level": "photons",
        "levels": {
            "photons": {
                "path": "/heights",
                "coordinates": {"latitude": "lat_ph", "longitude": "lon_ph"},
                "variables": {"h": "h"},
                "link": None,
            },
            "segments": {
                "path": "/seg",
                "coordinates": {"latitude": "lat", "longitude": "lon"},
                "variables": {},
                "link": {
                    "to": "photons",
                    "index_beg": "/seg/ph_index_beg",
                    "count": "/seg/segment_ph_cnt",
                    "index_base": 0,
                },
            },
        },
        "read_plan": {"spatial_index": "segments", "pad": 0},
    }
    filters = []
    if with_base_filter:
        filters.append({"dataset": "/heights/qs", "op": "eq", "value": 0})
    if with_coarse_filter:
        filters.append({"dataset": "/seg/podppd", "op": "eq", "value": 0, "level": "segments"})
    if filters:
        ds["filters"] = filters
    return ds


def _planned_read_h5(*, qs=None, podppd=None):
    """Six-segment / 12-photon HDF5 stub with optional base/coarse flag arrays.

    Segments live at lats 0,100,200,300,400,500 (lon 0); photons at lats
    0,50,100,150,200,250,...,550 (lon 0). The wide segment spacing keeps the
    ``plan_read`` linestring-crossing check from sweeping unrelated segments
    into the matched range -- a narrow bbox between two rep-points stays
    bounded by the immediate neighbours.
    """
    seg_lats = np.array([0.0, 100.0, 200.0, 300.0, 400.0, 500.0])
    seg_lons = np.zeros(6)
    ibeg = np.arange(0, 12, 2, dtype=np.int64)
    cnt = np.full(6, 2, dtype=np.int64)
    ph_lats = np.array(
        [0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0, 550.0]
    )
    ph_lons = np.zeros(12)
    h = np.arange(12.0, dtype=np.float32) * 10.0
    arrays = {
        "/seg/lat": seg_lats,
        "/seg/lon": seg_lons,
        "/seg/ph_index_beg": ibeg,
        "/seg/segment_ph_cnt": cnt,
        "/heights/lat_ph": ph_lats,
        "/heights/lon_ph": ph_lons,
        "/heights/h": h,
    }
    if qs is not None:
        arrays["/heights/qs"] = np.asarray(qs)
    if podppd is not None:
        arrays["/seg/podppd"] = np.asarray(podppd)
    return _FakeH5(arrays)


class TestPlannedReadGroup:
    """Phase C: ``_read_group`` dispatches to ``_planned_read_group`` when
    ``data_source.read_plan.spatial_index`` is set, bounding the base-rate IO
    via the coarse-level rep-point coords + link arrays.

    The shared fixture lays out 6 segments at lats ``[0, 100, 200, 300, 400,
    500]`` covering 12 photons (2 each). The wide spacing keeps ``plan_read``'s
    linestring-crossing sweep bounded: a bbox between two adjacent rep-points
    pulls in exactly its two neighbours."""

    def test_planned_path_bounds_io_to_matched_segments(self):
        # Mortie segment->shard mask (issue #95): lat band [100, 250] selects
        # segments 1 (lat 100) and 2 (lat 200) by rep-point -> one contiguous run
        # -> photons 2..5; the photon-level mask keeps all four (lat 100..250).
        ds = _planned_read_data_source()
        h5 = _planned_read_h5()
        grid = _LatBboxGrid((-0.1, 100.0, 0.1, 250.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        assert df["h"].tolist() == [20.0, 30.0, 40.0, 50.0]

    def test_empty_aoi_returns_none(self):
        # No segment rep-point maps to this shard -> empty coarse mask ->
        # short-circuit return None before any base-rate read.
        ds = _planned_read_data_source()
        h5 = _planned_read_h5()
        grid = _LatBboxGrid((-0.1, 10000.0, 0.1, 10001.0))
        assert _read_group(h5, "gt1l", ds, 0, grid) is None

    def test_full_read_fallback_on_high_selectivity(self):
        # full_read_threshold lowered so any plan covering >=10% of n_base
        # (>=2/12 photons) triggers the fallback. Same bbox as the basic test
        # selects 4/12 = 33% -> falls through to _read_group_full and reads
        # everything; the permissive grid keeps all 12.
        ds = _planned_read_data_source()
        ds["read_plan"]["full_read_threshold"] = 0.1
        h5 = _planned_read_h5()
        grid = _BboxGrid((-0.1, 175.0, 0.1, 225.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        assert df["h"].tolist() == [float(i * 10) for i in range(12)]

    def test_parity_with_full_read(self):
        # Both paths produce the same row set when the spatial mask is keyed
        # on lat: the planned read narrows IO to photons 2..5 (via plan_read);
        # _LatBboxGrid.shards_of further restricts to photons with lat in
        # bbox range (photon 4, lat=200). qs drops nothing in-shard.
        qs = np.array([0] * 12, dtype=np.int8)
        h5 = _planned_read_h5(qs=qs)
        grid = _LatBboxGrid((-0.1, 175.0, 0.1, 225.0))

        ds_planned = _planned_read_data_source(with_base_filter=True)
        ds_full = {
            "coordinates": {
                "latitude": "/heights/lat_ph",
                "longitude": "/heights/lon_ph",
            },
            "variables": {"h": "/heights/h"},
            "filters": [{"dataset": "/heights/qs", "op": "eq", "value": 0}],
        }

        df_planned = _read_group(h5, "gt1l", ds_planned, 0, grid)
        df_full = _read_group(h5, "gt1l", ds_full, 0, grid)
        # Photon 4 (lat=200, h=40) is the only one in the bbox lat range.
        assert df_planned["h"].tolist() == [40.0]
        assert df_full["h"].tolist() == [40.0]

    def test_parity_with_empty_segment(self):
        # ATL03 empty-segment sentinel (#68): seg 1 is empty (ph_index_beg==0,
        # count==0) and is pulled into the matched run as the pad boundary. Pre-fix
        # the run collapsed (base_end = 0-1+0 <= base_start) and the planned path
        # dropped seg 0's real photons -> planned (None) != full. The guard bounds
        # the run by its non-empty segment, restoring planned == full parity.
        h5 = _FakeH5(
            {
                "/seg/lat": np.array([200.0, 300.0, 1000.0, 2000.0]),
                "/seg/lon": np.zeros(4),
                "/seg/ph_index_beg": np.array([1, 0, 3, 5], dtype=np.int64),  # seg 1 empty
                "/seg/segment_ph_cnt": np.array([2, 0, 2, 2], dtype=np.int64),
                "/heights/lat_ph": np.array([200.0, 210.0, 1000.0, 1010.0, 2000.0, 2010.0]),
                "/heights/lon_ph": np.zeros(6),
                "/heights/h": np.arange(6.0, dtype=np.float32) * 10.0,
                "/heights/qs": np.zeros(6, dtype=np.int8),
            }
        )
        grid = _LatBboxGrid((-0.1, 195.0, 0.1, 215.0))  # keeps photons 0,1 (lat 200,210)

        ds_planned = _planned_read_data_source(with_base_filter=True)
        ds_planned["levels"]["segments"]["link"]["index_base"] = 1  # ATL03 1-based
        ds_planned["read_plan"]["pad"] = 1  # pull the empty seg 1 into the run
        ds_full = {
            "coordinates": {"latitude": "/heights/lat_ph", "longitude": "/heights/lon_ph"},
            "variables": {"h": "/heights/h"},
            "filters": [{"dataset": "/heights/qs", "op": "eq", "value": 0}],
        }

        df_planned = _read_group(h5, "gt1l", ds_planned, 0, grid)
        df_full = _read_group(h5, "gt1l", ds_full, 0, grid)
        assert df_planned["h"].tolist() == [0.0, 10.0]
        assert df_full["h"].tolist() == [0.0, 10.0]

    def test_coarse_filter_via_planned_path(self):
        # Cross-level (Phase B) filter ANDs with the planned path: lat band
        # [100, 250] selects segments 1,2 (mortie mask); podppd drops segment 1.
        # Photons 2,3 dropped; 4,5 kept.
        ds = _planned_read_data_source(with_coarse_filter=True)
        podppd = np.array([0, 1, 0, 0, 0, 0], dtype=np.int8)
        h5 = _planned_read_h5(podppd=podppd)
        grid = _LatBboxGrid((-0.1, 100.0, 0.1, 250.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        assert df["h"].tolist() == [40.0, 50.0]

    def test_pad_recovers_boundary_segment_and_matches_full(self):
        # Omission guard for the rep-point mask (issue #95): lat band [150, 310].
        # The mortie mask selects segments 2 (lat 200) and 3 (lat 300) by
        # rep-point, but photon 3 (lat 150) belongs to segment 1, whose rep-point
        # (100) is OUTSIDE the band. With pad=0 it would be omitted; pad=1 pulls
        # segment 1 into the run, recovering photon 3 -- so the planned read then
        # matches the full read exactly (no omission at the tested pad).
        h5 = _planned_read_h5()
        grid = _LatBboxGrid((-0.1, 150.0, 0.1, 310.0))

        ds_pad0 = _planned_read_data_source()  # pad=0 default
        df_pad0 = _read_group(h5, "gt1l", ds_pad0, 0, grid)
        assert df_pad0["h"].tolist() == [40.0, 50.0, 60.0]  # photon 3 (h=30) omitted

        ds_pad1 = _planned_read_data_source()
        ds_pad1["read_plan"]["pad"] = 1
        df_pad1 = _read_group(h5, "gt1l", ds_pad1, 0, grid)
        # Ground truth: a full read keeps every photon whose lat is in band.
        ds_full = {
            "coordinates": {"latitude": "/heights/lat_ph", "longitude": "/heights/lon_ph"},
            "variables": {"h": "/heights/h"},
        }
        df_full = _read_group(h5, "gt1l", ds_full, 0, grid)
        assert df_pad1["h"].tolist() == [30.0, 40.0, 50.0, 60.0]
        assert df_pad1["h"].tolist() == df_full["h"].tolist()

    def test_pad_does_not_recover_segment_two_runs_away(self):
        # Pins the omission bound from the PR's rep-point argument (issue #95):
        # pad recovers an in-band photon only when its OWNING segment is within
        # ``pad`` of a matched rep-point segment. Here segment 0 has a stray
        # photon (lat 255) inside the band [190, 260], but seg 0's rep-point (0)
        # is two segments away from the only matched segment (seg 2, rep 200),
        # so pad=1 does NOT pull seg 0's run in and the stray photon stays
        # omitted. The full read keeps it -> the planned read intentionally
        # diverges, confirming the bound is exactly ``pad`` segments, not "a few
        # edge photons" unconditionally.
        seg_lats = np.array([0.0, 100.0, 200.0, 300.0, 400.0])
        seg_lons = np.zeros(5)
        # seg 0 owns photons {0,1}, seg 1 {2}, seg 2 {3}, seg 3 {4}, seg 4 {5}.
        ibeg = np.array([0, 2, 3, 4, 5], dtype=np.int64)
        cnt = np.array([2, 1, 1, 1, 1], dtype=np.int64)
        # photon 1 is seg 0's stray, parked inside the band at lat 255.
        ph_lats = np.array([0.0, 255.0, 100.0, 200.0, 300.0, 400.0])
        ph_lons = np.zeros(6)
        h = np.arange(6.0, dtype=np.float32) * 10.0
        h5 = _FakeH5(
            {
                "/seg/lat": seg_lats,
                "/seg/lon": seg_lons,
                "/seg/ph_index_beg": ibeg,
                "/seg/segment_ph_cnt": cnt,
                "/heights/lat_ph": ph_lats,
                "/heights/lon_ph": ph_lons,
                "/heights/h": h,
            }
        )
        grid = _LatBboxGrid((-0.1, 190.0, 0.1, 260.0))  # rep-point band: seg 2 only

        ds_pad1 = _planned_read_data_source()
        ds_pad1["read_plan"]["pad"] = 1
        df_pad1 = _read_group(h5, "gt1l", ds_pad1, 0, grid)
        # Run [2,2] padded to [1,3] -> photons 2..4 -> only seg 2's photon (lat
        # 200, h=30) is in band; seg 0's stray (h=10) is two runs away, omitted.
        assert df_pad1["h"].tolist() == [30.0]

        ds_full = {
            "coordinates": {"latitude": "/heights/lat_ph", "longitude": "/heights/lon_ph"},
            "variables": {"h": "/heights/h"},
        }
        df_full = _read_group(h5, "gt1l", ds_full, 0, grid)
        # The full read recovers the stray (h=10) the planned read omits at pad=1.
        assert df_full["h"].tolist() == [10.0, 30.0]

    def test_invalid_link_target_raises(self):
        # The spatial_index level's link must point at the base level.
        ds = _planned_read_data_source()
        ds["levels"]["segments"]["link"]["to"] = "not_a_level"
        h5 = _planned_read_h5()
        grid = _BboxGrid((-0.1, 175.0, 0.1, 225.0))
        with pytest.raises(ValueError, match="must link directly to base level"):
            _read_group(h5, "gt1l", ds, 0, grid)

    def test_multi_slice_plan_global_idx_alignment(self):
        # Force a plan with two disjoint base-slices (one ATL03 track that
        # crosses the shard lat band twice). Fixture: 10 segments × 1 photon
        # each, lats wave from 0 -> 100 -> 0 -> 100 -> 0. The mortie mask (lat
        # band [45, 105]) picks up segments {2,3,4} and {7,8} -> two runs ->
        # ``global_idx = [2,3,4,7,8]``. A cross-level podppd filter that drops
        # segment 3 must align correctly through that global_idx (otherwise
        # photon 3's drop hits the wrong row).
        seg_lats = np.array([0.0, 0.0, 50.0, 100.0, 100.0, 0.0, 0.0, 100.0, 100.0, 0.0])
        seg_lons = np.zeros(10)
        ibeg = np.arange(10, dtype=np.int64)
        cnt = np.ones(10, dtype=np.int64)
        ph_lats = seg_lats.copy()
        ph_lons = np.zeros(10)
        h = np.arange(10.0, dtype=np.float32) * 10.0
        podppd = np.array([0, 0, 0, 1, 0, 0, 0, 0, 0, 0], dtype=np.int8)
        h5 = _FakeH5(
            {
                "/seg/lat": seg_lats,
                "/seg/lon": seg_lons,
                "/seg/ph_index_beg": ibeg,
                "/seg/segment_ph_cnt": cnt,
                "/seg/podppd": podppd,
                "/heights/lat_ph": ph_lats,
                "/heights/lon_ph": ph_lons,
                "/heights/h": h,
            }
        )
        ds = _planned_read_data_source(with_coarse_filter=True)
        grid = _LatBboxGrid((-0.1, 45.0, 0.1, 105.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        # mask selects segments {2,3,4} and {7,8} -> base_slices [(2,5),(7,9)]
        # -> global_idx [2,3,4,7,8] -> in-band photons h = [20,30,40,70,80];
        # cross-level drops segment 3 -> drop photon 3 (h=30) only.
        assert df["h"].tolist() == [20.0, 40.0, 70.0, 80.0]

    def test_full_read_fallback_carries_filters(self):
        # The selectivity-fallback path must produce the same row set as the
        # planned path would, including base-level structured filters: drop
        # photons 5,6 via qs=1 below. Bbox + low threshold -> fallback to
        # _read_group_full, which still applies the qs filter.
        ds = _planned_read_data_source(with_base_filter=True)
        ds["read_plan"]["full_read_threshold"] = 0.1
        qs = np.array([0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0], dtype=np.int8)
        h5 = _planned_read_h5(qs=qs)
        grid = _BboxGrid((-0.1, 175.0, 0.1, 225.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        # Full-coord path keeps all 12 photons (permissive grid); qs drops 5,6.
        assert df["h"].tolist() == [0.0, 10.0, 20.0, 30.0, 40.0, 70.0, 80.0, 90.0, 100.0, 110.0]

    def test_low_selectivity_falls_back_to_full_read(self):
        # The mortie mask needs no antimeridian/polar special-case (issue #95):
        # ``grid.assign`` is globally correct, so the old wide-bbox bail is gone.
        # A shard that genuinely owns (nearly) every segment -- here the
        # permissive grid maps all of them in -- still falls back to the full
        # read via the selectivity threshold rather than issuing many slices that
        # sum to the whole file.
        ds = _planned_read_data_source()
        h5 = _planned_read_h5()
        grid = _BboxGrid((-180.0, -10.0, 180.0, 10.0))  # permissive: all segments in shard
        df = _read_group(h5, "gt1l", ds, 0, grid)
        assert df["h"].tolist() == [float(i * 10) for i in range(12)]

    def test_dispatch_rejects_empty_levels(self):
        # An incomplete config -- ``read_plan.spatial_index`` set but
        # ``levels`` empty -- raises rather than silently routing to the
        # full-read path (which would pretend nothing was wrong).
        ds = _planned_read_data_source()
        ds["levels"] = {}
        h5 = _planned_read_h5()
        grid = _BboxGrid((-0.1, 175.0, 0.1, 225.0))
        with pytest.raises(ValueError, match="non-empty 'levels' mapping"):
            _read_group(h5, "gt1l", ds, 0, grid)

    def test_parity_with_full_read_includes_leaf_id(self):
        # Strengthen the parity check: row ORDER (via leaf_id) must agree
        # between paths, not just the value column.
        qs = np.array([0] * 12, dtype=np.int8)
        h5 = _planned_read_h5(qs=qs)
        grid = _LatBboxGrid((-0.1, 175.0, 0.1, 225.0))

        ds_planned = _planned_read_data_source(with_base_filter=True)
        ds_full = {
            "coordinates": {
                "latitude": "/heights/lat_ph",
                "longitude": "/heights/lon_ph",
            },
            "variables": {"h": "/heights/h"},
            "filters": [{"dataset": "/heights/qs", "op": "eq", "value": 0}],
        }

        df_planned = _read_group(h5, "gt1l", ds_planned, 0, grid)
        df_full = _read_group(h5, "gt1l", ds_full, 0, grid)
        # Same row -- leaf_id == lat under _LatBboxGrid.assign.
        assert df_planned["leaf_id"].tolist() == df_full["leaf_id"].tolist()
        assert df_planned["h"].tolist() == df_full["h"].tolist()

    def test_shipped_atl03_template_through_planned_read(self):
        # End-to-end coverage of the shipped ``atl03`` template against an
        # ATL03-shaped ``_FakeH5`` stub: real ``{group}`` path templates,
        # ``index_base: 1`` arithmetic, ``pad: 1``, and the 2-D
        # ``signal_conf_ph`` TEP filter all run through ``_planned_read_group``
        # together. This is the integration test the phase 6 review flagged
        # was missing -- without it a future YAML edit dropping ``index_base``
        # (defaulting to 0) lands green on the synthetic ``_planned_read_h5``
        # fixture even though it'd be wrong on real ATL03.
        from zagg.config import default_config

        cfg = default_config("atl03")
        ds = cfg.data_source
        # 4 segments × 2 photons = 8 photons, 1-based ph_index_beg.
        seg_lats = np.array([0.0, 100.0, 200.0, 300.0])
        seg_lons = np.zeros(4)
        ibeg = np.array([1, 3, 5, 7], dtype=np.int64)  # 1-based per ATL03 v3 dict
        cnt = np.array([2, 2, 2, 2], dtype=np.int64)
        ph_lats = np.array([0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0])
        ph_lons = np.zeros(8)
        h_ph = np.arange(8.0, dtype=np.float32) * 10.0
        # 2-D signal_conf_ph (n_photons × 5 surface types). One TEP photon
        # (uniform -2) at index 4 -- filter must drop it.
        signal_conf = np.full((8, 5), 4, dtype=np.int8)
        signal_conf[4, :] = -2
        h5 = _FakeH5(
            {
                "/gt1l/heights/lat_ph": ph_lats,
                "/gt1l/heights/lon_ph": ph_lons,
                "/gt1l/heights/h_ph": h_ph,
                "/gt1l/heights/signal_conf_ph": signal_conf,
                "/gt1l/geolocation/reference_photon_lat": seg_lats,
                "/gt1l/geolocation/reference_photon_lon": seg_lons,
                "/gt1l/geolocation/ph_index_beg": ibeg,
                "/gt1l/geolocation/segment_ph_cnt": cnt,
            }
        )
        # Permissive grid: every segment maps to the shard, so with pad=1 the
        # run spans all 4 segments and the planned read falls back to the full
        # read via the selectivity threshold (still index_base=1 arithmetic).
        # Plan covers photons 0..7; the 2-D signal_conf_ph filter drops photon 4
        # (uniform TEP -2 across all 5 surface types). Survivors: 0,1,2,3,5,6,7.
        grid = _BboxGrid((-0.1, 175.0, 0.1, 225.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        assert df is not None
        assert df["h_ph"].tolist() == [0.0, 10.0, 20.0, 30.0, 50.0, 60.0, 70.0]


class TestSegmentLevelVariables:
    """Issue #30: a non-base level declaring ``variables: {name: path}`` is read at
    coarse rate and broadcast to the base (photon) rows via the level's link, so a
    per-segment field (e.g. ``dem_h``, one value per ~100 photons) lands as a
    per-photon column the aggregation / chunk_precompute reduces."""

    def test_broadcast_segment_to_base_repeats_by_count(self):
        # 3 segments covering 2 photons each; each photon carries its segment value.
        seg = np.array([100.0, 200.0, 300.0], dtype=np.float32)
        ibeg = np.array([0, 2, 4])
        cnt = np.array([2, 2, 2])
        out = _broadcast_segment_to_base(seg, ibeg, cnt, index_base=0, total_base_size=6)
        assert out.tolist() == [100.0, 100.0, 200.0, 200.0, 300.0, 300.0]
        # Equals np.repeat under contiguity.
        assert out.tolist() == np.repeat(seg, cnt).tolist()
        assert out.dtype == seg.dtype

    def test_broadcast_honors_index_base(self):
        seg = np.array([10.0, 20.0])
        ibeg = np.array([1, 3])  # 1-based (ATL03-style)
        cnt = np.array([2, 2])
        out = _broadcast_segment_to_base(seg, ibeg, cnt, index_base=1, total_base_size=4)
        assert out.tolist() == [10.0, 10.0, 20.0, 20.0]

    def _data_source(self):
        return {
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
                    "variables": {"dem_h": "/dem_h"},
                    "link": {
                        "to": "photons",
                        "index_beg": "/ph_index_beg",
                        "count": "/segment_ph_cnt",
                    },
                },
            },
        }

    def test_full_path_broadcasts_dem_h_to_photons(self):
        # 3 segments x 2 photons; each photon carries its segment's dem_h.
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.arange(6.0, dtype=np.float32),
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
                "/dem_h": np.array([100.0, 200.0, 300.0], dtype=np.float32),
            }
        )
        df = _read_group(h5, "gt1l", self._data_source(), 0, _ShardGrid())
        assert df["h"].tolist() == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        assert df["dem_h"].tolist() == [100.0, 100.0, 200.0, 200.0, 300.0, 300.0]

    def test_full_path_alignment_under_base_filter(self):
        # A base-level filter drops some photons; the broadcast dem_h must follow
        # the SAME mask so each surviving photon keeps its own segment's value.
        ds = self._data_source()
        ds["filters"] = [{"dataset": "/qs", "op": "eq", "value": 0}]
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.arange(6.0, dtype=np.float32),
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
                "/dem_h": np.array([100.0, 200.0, 300.0], dtype=np.float32),
                "/qs": np.array([0, 1, 0, 0, 1, 0]),  # drop photons 1 and 4
            }
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # Survivors: photons 0,2,3,5 -> dem_h 100,200,200,300.
        assert df["h"].tolist() == [0.0, 2.0, 3.0, 5.0]
        assert df["dem_h"].tolist() == [100.0, 200.0, 200.0, 300.0]

    def test_expression_filter_references_broadcast_dem_h(self):
        # An ``{expression: "dem_h > ..."}`` filter references the broadcast
        # segment variable, which is materialized before the filter runs (issue
        # #30): photons are kept/dropped by their own segment's dem_h, even though
        # ``dem_h`` is not a ``data_source.variables`` column.
        ds = self._data_source()
        ds["filters"] = [{"expression": "dem_h > 150.0"}]
        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.arange(6.0, dtype=np.float32),
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
                "/dem_h": np.array([100.0, 200.0, 300.0], dtype=np.float32),
            }
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        # Seg 0 (dem_h 100) is dropped; segs 1,2 (dem_h 200,300) are kept.
        assert df["h"].tolist() == [2.0, 3.0, 4.0, 5.0]
        assert df["dem_h"].tolist() == [200.0, 200.0, 300.0, 300.0]

    def test_planned_partial_read_aligns_dem_h(self):
        # Partial read plan (NOT a full read): the bbox selects only segments
        # 1-2 (photons 2..5). Each selected photon must carry its own segment's
        # dem_h, broadcast over the read-plan-selected photons only.
        ds = _planned_read_data_source()
        ds["levels"]["segments"]["variables"] = {"dem_h": "/seg/dem_h"}
        h5 = _planned_read_h5()
        # 6 segments; dem_h one value per segment.
        h5._arrays["/seg/dem_h"] = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0], dtype=np.float32)
        grid = _LatBboxGrid((-0.1, 100.0, 0.1, 250.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        # Planned path selects photons 2,3 (seg 1) and 4,5 (seg 2).
        assert df["h"].tolist() == [20.0, 30.0, 40.0, 50.0]
        assert df["dem_h"].tolist() == [20.0, 20.0, 30.0, 30.0]

    def test_planned_partial_read_aligns_dem_h_under_base_filter(self):
        # Planned (partial) read with a base-level filter stacked on top of the
        # seg-variable broadcast: each surviving photon must still carry its own
        # segment's dem_h after the keep mask.
        ds = _planned_read_data_source(with_base_filter=True)
        ds["levels"]["segments"]["variables"] = {"dem_h": "/seg/dem_h"}
        # qs drops photon 3 (within the selected seg 1) so the keep mask is exercised.
        qs = np.zeros(12, dtype=np.int8)
        qs[3] = 1
        h5 = _planned_read_h5(qs=qs)
        h5._arrays["/seg/dem_h"] = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0], dtype=np.float32)
        grid = _LatBboxGrid((-0.1, 100.0, 0.1, 250.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        # Selected photons 2,3 (seg 1) + 4,5 (seg 2); qs drops photon 3.
        assert df["h"].tolist() == [20.0, 40.0, 50.0]
        assert df["dem_h"].tolist() == [20.0, 30.0, 30.0]

    def test_planned_path_expression_filter_references_broadcast_dem_h(self):
        # Planned (partial) read: an ``{expression: "dem_h > ..."}`` filter
        # references the broadcast segment variable, mirroring the full-path test.
        # Locks in parity of the namespace fix across both read paths (issue #30).
        ds = _planned_read_data_source()
        ds["levels"]["segments"]["variables"] = {"dem_h": "/seg/dem_h"}
        ds["filters"] = [{"expression": "dem_h > 25.0"}]
        h5 = _planned_read_h5()
        h5._arrays["/seg/dem_h"] = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0], dtype=np.float32)
        grid = _LatBboxGrid((-0.1, 100.0, 0.1, 250.0))
        df = _read_group(h5, "gt1l", ds, 0, grid)
        # Selected photons 2,3 (seg 1, dem_h 20) + 4,5 (seg 2, dem_h 30); the
        # filter drops seg 1 (20 <= 25) and keeps seg 2 (30 > 25).
        assert df["h"].tolist() == [40.0, 50.0]
        assert df["dem_h"].tolist() == [30.0, 30.0]

    def test_broadcast_out_of_bounds_raises(self):
        # A segment range extending past the base size (e.g. a seg-variable level
        # whose link does not tile the read's base extent) is rejected, not silently
        # written out of bounds.
        seg = np.array([1.0, 2.0])
        ibeg = np.array([0, 2])
        cnt = np.array([2, 5])  # second range [2:7] exceeds base size 4
        with pytest.raises(ValueError, match="exceeds base size"):
            _broadcast_segment_to_base(seg, ibeg, cnt, index_base=0, total_base_size=4)

    def test_broadcast_gap_fills_nan_for_float(self):
        # An untiled gap (contiguity violated) surfaces as NaN for float dtypes,
        # not uninitialized garbage.
        seg = np.array([1.0, 2.0], dtype=np.float32)
        ibeg = np.array([0, 3])  # base index 2 left untiled
        cnt = np.array([2, 1])
        out = _broadcast_segment_to_base(seg, ibeg, cnt, index_base=0, total_base_size=4)
        assert out[0] == 1.0 and out[1] == 1.0 and out[3] == 2.0
        assert np.isnan(out[2])

    def test_no_segment_variables_is_inert(self):
        # A hierarchical config whose non-base level uses the documentation-only
        # ``list[str]`` variables form (NOT the mapping) triggers no broadcast: the
        # helpers return empty and the read path produces the same columns as before.
        ds = self._data_source()
        ds["levels"]["segments"]["variables"] = ["signal_conf_ph"]  # list form
        assert _segment_level_variables(ds) == {}

        class _NoH5:
            def readDatasets(self, datasets):  # noqa: N802
                raise AssertionError("no segment-variable read should occur")

        # No segment-variable read is attempted (the link arrays are never read).
        assert _read_segment_broadcasts(_NoH5(), "gt1l", ds, ds["levels"], 6) == {}

        h5 = _FakeH5(
            {
                "/lat": np.arange(6.0),
                "/lon": np.arange(6.0),
                "/h": np.arange(6.0, dtype=np.float32),
                "/ph_index_beg": np.array([0, 2, 4]),
                "/segment_ph_cnt": np.array([2, 2, 2]),
            }
        )
        df = _read_group(h5, "gt1l", ds, 0, _ShardGrid())
        assert list(df.columns) == ["h", "leaf_id"]  # no dem_h column injected

    def test_worked_template_chunk_offset_is_floor_min_dem_h(self):
        # End-to-end through process_shard: dem_h broadcast feeds chunk_offset,
        # whose value is floor(min(pooled dem_h)) and is uniform across cells.
        from zagg.config import load_config

        cfg = load_config("src/zagg/configs/atl03_waveform_chunk.yaml")
        # Two cells' worth of photons pooled in one shard; dem_h per photon
        # (already broadcast) with a known min.
        dem = np.array([100.0, 100.0, 102.0, 108.0, 110.0], dtype=np.float32)
        chunk_scalars = _eval_chunk_precompute(
            cfg, {"h_ph": np.arange(5.0, dtype=np.float32), "dem_h": dem}
        )
        assert chunk_scalars["chunk_offset"] == np.float32(np.floor(np.min(dem)))
        assert chunk_scalars["chunk_offset"] == np.float32(100.0)


class TestChunkPrecompute:
    """Per-chunk precompute hook (issue #30, item 1): compute once per chunk over
    the shard's pooled data, inject into the per-cell expression namespace."""

    def _cfg(self, *, with_precompute):
        """Config whose per-cell ``offset`` records a chunk- or cell-level median.

        With ``with_precompute`` the offset is the chunk-precompute ``chunk_offset``
        (pooled over the whole shard); without it the offset is each cell's own
        ``np.median(h_ph)``. The contrast is the test's whole point: the former is
        uniform across a chunk, the latter varies cell to cell.
        """
        from zagg.config import PipelineConfig

        agg: dict = {
            "variables": {
                "offset": {"dtype": "float32"},
                "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
            }
        }
        if with_precompute:
            agg["chunk_precompute"] = {
                "chunk_offset": {"expression": "np.median(h_ph)", "source": "h_ph"}
            }
            agg["variables"]["offset"]["expression"] = "chunk_offset"
        else:
            agg["variables"]["offset"]["expression"] = "np.median(h_ph)"
        return PipelineConfig(
            data_source={"groups": ["gt1l"], "variables": {"h_ph": "/{group}/h_ph"}},
            aggregation=agg,
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_eval_chunk_precompute_pools_whole_shard(self):
        """The scalar is computed ONCE over the pooled columns, not per cell."""
        cfg = self._cfg(with_precompute=True)
        pooled = {"h_ph": np.array([1.0, 2.0, 3.0, 100.0], dtype=np.float32)}
        scalars = _eval_chunk_precompute(cfg, pooled)
        assert set(scalars) == {"chunk_offset"}
        assert scalars["chunk_offset"] == np.median(pooled["h_ph"])

    def test_eval_chunk_precompute_empty_without_block(self):
        cfg = self._cfg(with_precompute=False)
        assert _eval_chunk_precompute(cfg, {"h_ph": np.array([1.0, 2.0])}) == {}

    def test_eval_chunk_precompute_function_entry_with_dtype(self):
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"variables": {"h_ph": "/p"}},
            aggregation={
                "chunk_precompute": {
                    "m": {"function": "median", "source": "h_ph", "dtype": "float32"}
                },
                "variables": {"count": {"function": "len", "source": "h_ph"}},
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        out = _eval_chunk_precompute(cfg, {"h_ph": np.array([1.0, 2.0, 9.0])})
        assert out["m"] == np.float32(2.0)
        assert isinstance(out["m"], np.float32)

    def _run_shard(self, monkeypatch, cfg):
        """Drive process_shard over two cells via a canned multi-beam read.

        cell 10 holds low photons, cell 20 holds high photons, so a per-cell median
        differs between the two cells while the pooled (chunk) median is shared.
        """
        leaf_to_cell = {1: 10, 2: 20}
        children = [10, 20]
        grid = _KernelShardGrid(children, leaf_to_cell)
        df = pd.DataFrame(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        df_out, meta = process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg)
        return df_out, children

    def test_chunk_scalar_uniform_across_cells(self, monkeypatch):
        """The chunk-precompute offset is identical for every cell in the chunk —
        the pooled median — whereas a per-cell median would differ between the
        two cells. This is the keystone behavior of the hook."""
        df_chunk, _ = self._run_shard(monkeypatch, self._cfg(with_precompute=True))
        offsets = df_chunk["offset"].to_numpy()
        pooled_median = np.median([0.0, 2.0, 100.0, 102.0])
        np.testing.assert_array_equal(offsets, np.full(2, pooled_median, dtype=np.float32))
        # the two cells' offsets are equal (uniform), not the per-cell medians.
        assert offsets[0] == offsets[1]

    def test_per_cell_median_varies_without_precompute(self, monkeypatch):
        """Control: the same field as a per-cell median DOES vary cell to cell, so
        the uniformity above is a property of the chunk hook, not of the data."""
        df_cell, _ = self._run_shard(monkeypatch, self._cfg(with_precompute=False))
        offsets = df_cell["offset"].to_numpy()
        # cell 10 -> median(0, 2) = 1; cell 20 -> median(100, 102) = 101.
        np.testing.assert_array_equal(offsets, np.array([1.0, 101.0], dtype=np.float32))
        assert offsets[0] != offsets[1]

    def test_arrow_kernel_runs_chunk_precompute(self, monkeypatch):
        """The kernel path now evaluates chunk_precompute over the pooled arrow
        table and threads the scalar through the fallback per-cell loop (issue #30,
        item ii): the per-cell ``offset`` resolves to the pooled chunk anchor,
        uniform across cells — same result the pandas/arrow carriers produce."""
        pa = pytest.importorskip("pyarrow")
        cfg = self._cfg(with_precompute=True)
        grid = _KernelShardGrid([10, 20], {1: 10, 2: 20})
        table = pa.table(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [table])
        df_out, _ = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=cfg, handoff="arrow-kernel"
        )
        offsets = df_out["offset"].to_numpy()
        pooled_median = np.median([0.0, 2.0, 100.0, 102.0])
        np.testing.assert_array_equal(offsets, np.full(2, pooled_median, dtype=np.float32))
        assert offsets[0] == offsets[1]

    def test_worked_template_uniform_offset_and_gain(self, monkeypatch):
        """The shipped atl03_waveform_chunk template runs end-to-end through the
        worker and emits a chunk-uniform offset_h/gain_h across two cells whose own
        photon distributions differ (low vs high), proving the bin-28 window is set
        once over the pooled shard, not per cell."""
        pytest.importorskip("pyarrow")
        cfg = default_config("atl03_waveform_chunk")
        leaf_to_cell = {1: 10, 2: 20}
        children = [10, 20]
        grid = _KernelShardGrid(children, leaf_to_cell)
        # dem_h (the DEM anchor) is the per-photon broadcast of each segment's
        # reference DEM; it rides alongside h_ph as a pooled column (issue #30).
        dem = np.array([50.0, 50.0, 60.0, 60.0], dtype=np.float32)
        df = pd.DataFrame(
            {
                "h_ph": np.array([10.0, 12.0, 200.0, 202.0], dtype=np.float32),
                "dem_h": dem,
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        tbl, _meta = process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg)
        # vector waveform field -> arrow table carrier.
        offset = tbl.column("offset_h").to_numpy(zero_copy_only=False)
        gain = tbl.column("gain_h").to_numpy(zero_copy_only=False)
        assert offset[0] == offset[1]
        assert gain[0] == gain[1]
        # chunk_offset is now the DEM anchor: floor(min(pooled dem_h)).
        assert offset[0] == np.float32(np.floor(np.min(dem)))

    def test_empty_cell_gets_chunk_anchor_not_nan(self, monkeypatch):
        """An empty cell in a POPULATED chunk records the chunk-uniform anchor, not
        NaN. The dense writer still emits a row for the empty cell; the field whose
        expression is a bare chunk-precompute name must resolve to the shared scalar
        so readers see a uniform anchor across the whole chunk (issue #30)."""
        # Three children, but only cells 10 and 20 carry photons; cell 30 is empty.
        leaf_to_cell = {1: 10, 2: 20}
        children = [10, 20, 30]
        grid = _KernelShardGrid(children, leaf_to_cell)
        df = pd.DataFrame(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        df_out, _meta = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=self._cfg(with_precompute=True)
        )
        offsets = df_out["offset"].to_numpy()
        pooled_median = np.float32(np.median([0.0, 2.0, 100.0, 102.0]))
        # Every cell — including the empty third one — carries the chunk anchor.
        np.testing.assert_array_equal(offsets, np.full(3, pooled_median, dtype=np.float32))
        assert not np.isnan(offsets[2])
        # The empty cell still reports a zero obs count (the anchor is not data).
        assert df_out["count"].to_numpy()[2] == 0

    def test_non_scalar_chunk_value_allowed_in_namespace(self):
        """A non-scalar chunk_precompute result (e.g. a matrix) is now ALLOWED into
        the namespace (issue #30 / @espg 4773649308) — only a kind: scalar *write*
        requires scalar-ness. The dtype cast applies element-wise to the array."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"variables": {"h_ph": "/p"}},
            aggregation={
                # A covariance matrix: shape (2, 2), non-scalar.
                "chunk_precompute": {
                    "chunk_cov": {
                        "expression": "np.cov(np.vstack([h_ph, h_ph * 2.0]))",
                        "source": "h_ph",
                        "dtype": "float64",
                    }
                },
                "variables": {"count": {"function": "len", "source": "h_ph"}},
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        pooled = {"h_ph": np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float64)}
        out = _eval_chunk_precompute(cfg, pooled)
        assert out["chunk_cov"].shape == (2, 2)
        assert out["chunk_cov"].dtype == np.float64

    def test_non_scalar_chunk_value_usable_in_per_cell_expression(self, monkeypatch):
        """A non-scalar chunk value feeds a per-cell ``expression``: a per-cell
        vector field references a chunk-level array (here a 3-vector), proving the
        namespace injection is shape-agnostic end-to-end (issue #30)."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"groups": ["gt1l"], "variables": {"h_ph": "/{group}/h_ph"}},
            aggregation={
                # chunk_vec is a length-3 array (pooled per-quantile anchor).
                "chunk_precompute": {
                    "chunk_vec": {
                        "expression": "np.percentile(h_ph, [25, 50, 75])",
                        "source": "h_ph",
                        "dtype": "float32",
                    }
                },
                "variables": {
                    # references the chunk array in a per-cell vector expression.
                    "anchored": {
                        "kind": "vector",
                        "trailing_shape": 3,
                        "expression": "chunk_vec + np.float32(np.mean(h_ph))",
                        "source": "h_ph",
                        "dtype": "float32",
                    },
                    "count": {
                        "function": "len",
                        "source": "h_ph",
                        "dtype": "int32",
                        "fill_value": 0,
                    },
                },
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        grid = _KernelShardGrid([10, 20], {1: 10, 2: 20})
        df = pd.DataFrame(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        # vector field -> arrow table carrier.
        tbl, _ = process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg)
        anchored = tbl.column("anchored").combine_chunks()
        block = anchored.values.to_numpy(zero_copy_only=False).reshape(2, 3)
        chunk_vec = np.percentile([0.0, 2.0, 100.0, 102.0], [25, 50, 75]).astype(np.float32)
        # cell 10 mean = 1.0, cell 20 mean = 101.0.
        np.testing.assert_allclose(block[0], chunk_vec + np.float32(1.0), rtol=1e-5)
        np.testing.assert_allclose(block[1], chunk_vec + np.float32(101.0), rtol=1e-5)

    def test_non_scalar_chunk_value_into_scalar_field_clear_error(self, monkeypatch):
        """Writing a non-scalar chunk value to a kind: scalar field raises a clear
        error (the scalar-ness guard now lives at the WRITE point, not the
        precompute reduction — issue #30)."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"groups": ["gt1l"], "variables": {"h_ph": "/{group}/h_ph"}},
            aggregation={
                "chunk_precompute": {
                    "chunk_vec": {
                        "expression": "np.percentile(h_ph, [25, 50, 75])",
                        "source": "h_ph",
                    }
                },
                "variables": {
                    # scalar field assigned a length-3 chunk array -> clear error.
                    "bad": {"expression": "chunk_vec", "dtype": "float32"},
                    "count": {
                        "function": "len",
                        "source": "h_ph",
                        "dtype": "int32",
                        "fill_value": 0,
                    },
                },
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        grid = _KernelShardGrid([10, 20], {1: 10, 2: 20})
        df = pd.DataFrame(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        with pytest.raises(ValueError, match="scalar field 'bad'.*non-scalar"):
            process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg)

    def test_missing_source_column_clear_error(self):
        """A function precompute whose source is absent from the pooled dict raises
        a clear config/data error, not a bare KeyError."""
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"variables": {"h_ph": "/p", "other": "/o"}},
            aggregation={
                "chunk_precompute": {"m": {"function": "median", "source": "other"}},
                "variables": {"count": {"function": "len", "source": "h_ph"}},
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        # Pooled dict only carries h_ph (e.g. 'other' was not read).
        with pytest.raises(ValueError, match="source column 'other' is not present"):
            _eval_chunk_precompute(cfg, {"h_ph": np.array([1.0, 2.0])})

    def test_arrow_kernel_runs_non_scalar_chunk_precompute(self, monkeypatch):
        """A NON-scalar chunk_precompute result also works on the kernel path: the
        pooled arrow table is reduced once to a 3-vector, injected into the kernel
        fallback namespace, and a per-cell scalar expression reduces over it (issue
        #30, item ii). (A kind: vector OUTPUT is a separate kernel-path gap — the
        experimental kernel reducer dense-preallocates scalars only — so the
        per-cell field reduces the chunk array rather than emitting it whole.)"""
        pa = pytest.importorskip("pyarrow")
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            data_source={"groups": ["gt1l"], "variables": {"h_ph": "/{group}/h_ph"}},
            aggregation={
                "chunk_precompute": {
                    "chunk_vec": {
                        "expression": "np.percentile(h_ph, [25, 50, 75])",
                        "source": "h_ph",
                        "dtype": "float32",
                    }
                },
                "variables": {
                    # per-cell scalar reduces over the injected chunk 3-vector.
                    "anchored": {
                        "expression": "float(np.sum(chunk_vec)) + float(np.mean(h_ph))",
                        "source": "h_ph",
                        "dtype": "float32",
                    },
                    "count": {
                        "function": "len",
                        "source": "h_ph",
                        "dtype": "int32",
                        "fill_value": 0,
                    },
                },
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        grid = _KernelShardGrid([10, 20], {1: 10, 2: 20})
        table = pa.table(
            {
                "h_ph": np.array([0.0, 2.0, 100.0, 102.0], dtype=np.float32),
                "leaf_id": np.array([1, 1, 2, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [table])
        df_out, _ = process_shard(
            grid, 0, ["s3://x"], s3_credentials={}, config=cfg, handoff="arrow-kernel"
        )
        anchored = df_out["anchored"].to_numpy()
        chunk_sum = float(
            np.sum(np.percentile([0.0, 2.0, 100.0, 102.0], [25, 50, 75]).astype(np.float32))
        )
        # cell 10 mean = 1.0, cell 20 mean = 101.0.
        np.testing.assert_allclose(anchored[0], chunk_sum + 1.0, rtol=1e-5)
        np.testing.assert_allclose(anchored[1], chunk_sum + 101.0, rtol=1e-5)

    def test_degenerate_single_photon_chunk_gain_floor(self, monkeypatch):
        """The worked template's chunk_gain floors to 0.5 on a degenerate pooled
        range (single photon / all-equal heights) WITHOUT a log2(0) divide-by-zero
        warning — the range is clamped before log2 and the 0.5 m floor applies."""
        pytest.importorskip("pyarrow")
        cfg = default_config("atl03_waveform_chunk")
        children = [10, 20]
        grid = _KernelShardGrid(children, {1: 10, 2: 20})
        # All photons share one height -> pooled range == 0 (degenerate). dem_h
        # (the DEM anchor) rides alongside; chunk_gain is the spread over h_ph.
        df = pd.DataFrame(
            {
                "h_ph": np.array([42.0, 42.0], dtype=np.float32),
                "dem_h": np.array([42.0, 42.0], dtype=np.float32),
                "leaf_id": np.array([1, 2], dtype=np.int64),
            }
        )
        TestProcessShardKernelBranch()._patch_reads(monkeypatch, [df])
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any RuntimeWarning would fail the test
            tbl, _meta = process_shard(grid, 0, ["s3://x"], s3_credentials={}, config=cfg)
        gain = tbl.column("gain_h").to_numpy(zero_copy_only=False)
        np.testing.assert_array_equal(gain, np.full(2, np.float32(0.5)))

    def test_absent_block_matches_plain_path_values(self, monkeypatch):
        """A config WITHOUT chunk_precompute produces the SAME per-cell values the
        plain (pre-hook) path would — proving the hook is a true no-op when absent,
        not merely deterministic across two runs of the same new code. The expected
        per-cell medians/counts are known independently (cell 10 -> median(0,2)=1,
        cell 20 -> median(100,102)=101; two obs each)."""
        df_out, _ = self._run_shard(monkeypatch, self._cfg(with_precompute=False))
        np.testing.assert_array_equal(
            df_out["offset"].to_numpy(), np.array([1.0, 101.0], dtype=np.float32)
        )
        np.testing.assert_array_equal(df_out["count"].to_numpy(), np.array([2, 2], dtype=np.int32))
        # And the chunk-precompute config does NOT reproduce these per-cell values
        # (it injects the pooled anchor instead), so the no-op claim is non-vacuous.
        df_chunk, _ = self._run_shard(monkeypatch, self._cfg(with_precompute=True))
        assert not np.array_equal(
            df_chunk["offset"].to_numpy(), np.array([1.0, 101.0], dtype=np.float32)
        )


# ---------------------------------------------------------------------------
# Per-granule h5coro cache release in process_shard (issue #66)
# ---------------------------------------------------------------------------


class _ReleaseGrid:
    """Grid stub for the #66 release tests: leaf id == row index, every row maps
    to ``shard_key`` 0 (so the flat read keeps all rows), and the post-read
    methods (``cells_of``/``children``/``chunk_coords``) collapse every row onto a
    single cell. Drives the real ``_read_group`` → aggregate → ``_build_output``
    path so the per-granule ``close()`` in the loop actually runs."""

    @staticmethod
    def assign(lats, lons):
        return np.arange(len(lats), dtype=np.int64)

    @staticmethod
    def shards_of(leaf_ids):
        return np.zeros(len(leaf_ids), dtype=np.int64)

    def children(self, shard_key):
        return np.array([0], dtype=np.int64)

    def cells_of(self, leaf_ids):
        return np.zeros(len(leaf_ids), dtype=np.int64)

    def chunk_coords(self, shard_key):
        return {"cell_lat": np.zeros(1), "cell_lon": np.zeros(1)}


def _release_cfg():
    """Minimal flat config: one group, lat/lon coords + one variable ``h_li``,
    aggregated to count/min so ``process_shard`` runs end-to-end on canned reads."""
    from zagg.config import PipelineConfig

    return PipelineConfig(
        data_source={
            "groups": ["gt1l"],
            "coordinates": {"latitude": "/{group}/lat", "longitude": "/{group}/lon"},
            "variables": {"h_li": "/{group}/h_li"},
        },
        aggregation={
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_min": {"function": "min", "source": "h_li", "dtype": "float32"},
            }
        },
    )


def _serve_datasets(arrays, datasets):
    """Shared ``readDatasets`` body: honor the same path/hyperslice contract as
    :class:`_FakeH5` (mirrors the real h5coro driver)."""
    out = {}
    for d in datasets:
        if isinstance(d, str):
            out[d] = arrays[d]
            continue
        path = d["dataset"]
        arr = arrays[path]
        hs = d.get("hyperslice")
        if hs is not None:
            lo, hi = hs[0]
            arr = arr[lo:hi]
        out[path] = arr
    return out


class _CloseRecordingH5:
    """h5coro-1.0.5-shaped stub: serves canned arrays and records ``close()`` calls
    on a shared ``log`` (``("close", id)``), one entry per release."""

    def __init__(self, arrays, log):
        self._arrays = arrays
        self._log = log

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        return _serve_datasets(self._arrays, datasets)

    def close(self):
        self._log.append(("close", id(self)))


class _RecordingCache(dict):
    """A cache whose ``clear()`` records the release (so the 1.0.4 fallback —
    ``h5obj.cache.clear()`` — is observable)."""

    def __init__(self, log):
        super().__init__()
        self._log = log
        self["line0"] = b"x"  # non-empty so clear() actually frees something.

    def clear(self):
        self._log.append(("clear", id(self)))
        super().clear()


class _ClearOnlyH5:
    """h5coro-1.0.4-shaped stub: NO ``close()`` (so ``hasattr(h5obj, "close")`` is
    False), only a ``cache`` whose ``clear()`` records the release."""

    def __init__(self, arrays, log):
        self._arrays = arrays
        self.cache = _RecordingCache(log)

    def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
        return _serve_datasets(self._arrays, datasets)


def _canned_arrays():
    """Two photons in shard 0; lat/lon keep both rows under ``_ReleaseGrid``."""
    return {
        "/gt1l/lat": np.array([10.0, 11.0]),
        "/gt1l/lon": np.array([20.0, 21.0]),
        "/gt1l/h_li": np.array([100.0, 200.0], dtype=np.float32),
    }


class TestProcessShardCacheRelease:
    """Issue #66: ``process_shard`` must release each granule's h5coro cache once
    per granule (not zero, not once at the end), and the release must not corrupt
    the data already extracted into ``all_reads`` (copy-before-clear)."""

    def _patch_h5(self, monkeypatch, factory):
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", factory)
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)

    def test_close_called_once_per_granule(self, monkeypatch):
        """A close-bearing stub (h5coro 1.0.5 shape) is closed exactly once for each
        of three granules — the per-granule release fires inside the loop."""
        log: list = []

        def factory(*a, **k):
            return _CloseRecordingH5(_canned_arrays(), log)

        self._patch_h5(monkeypatch, factory)
        df_out, meta = process_shard(
            _ReleaseGrid(),
            0,
            ["s3://a", "s3://b", "s3://c"],
            s3_credentials={},
            config=_release_cfg(),
        )
        closes = [e for e in log if e[0] == "close"]
        # One release per granule, inside the loop — not zero, not once at the end.
        assert len(closes) == 3
        assert meta["files_processed"] == 3

    def test_cache_clear_fallback_on_1_0_4(self, monkeypatch):
        """When the object has no ``close()`` (h5coro 1.0.4), the loop falls back to
        ``cache.clear()`` — also once per granule."""
        log: list = []

        def factory(*a, **k):
            return _ClearOnlyH5(_canned_arrays(), log)

        self._patch_h5(monkeypatch, factory)
        process_shard(
            _ReleaseGrid(), 0, ["s3://a", "s3://b"], s3_credentials={}, config=_release_cfg()
        )
        clears = [e for e in log if e[0] == "clear"]
        assert len(clears) == 2

    def test_retained_data_survives_cache_clear(self, monkeypatch):
        """End-to-end copy-before-clear: drive the full ``process_shard`` loop with a
        stub that hands out buffer-backed views (as real h5coro does — memoryviews
        into 4 MB cache lines) and ZEROES every buffer on ``close()`` (the
        per-granule release simulating cache-line eviction). Assert the aggregation
        output reflects the ORIGINAL bytes — the worker's retained data is detached
        from the cache before the release fires, so per-granule release is safe.

        Note this guards the SYSTEM-LEVEL invariant (the worker's output survives the
        release), which is what the fix relies on. It is a two-layer guarantee:
        ``_read_group`` builds columns by boolean-mask indexing (a numpy copy) AND
        ``pd.DataFrame``/``pa.table`` copy their numpy inputs at construction. A test
        on a DataFrame-returning helper cannot isolate the read-site layer (pandas
        copies regardless), so this asserts the property that actually matters: no
        retained array references the evicted cache."""

        class _ViewBackedH5:
            """Returns memoryview-backed arrays into a private buffer per dataset;
            ``close()`` zeroes every buffer (simulating cache-line eviction)."""

            def __init__(self, arrays):
                # keep a writable bytearray-backed copy per path; hand out views.
                self._buffers = {k: bytearray(v.tobytes()) for k, v in arrays.items()}
                self._dtypes = {k: v.dtype for k, v in arrays.items()}

            def readDatasets(self, datasets):  # noqa: N802
                out = {}
                for d in datasets:
                    path = d if isinstance(d, str) else d["dataset"]
                    buf = self._buffers[path]
                    arr = np.frombuffer(buf, dtype=self._dtypes[path])  # view into buffer
                    if not isinstance(d, str) and d.get("hyperslice") is not None:
                        lo, hi = d["hyperslice"][0]
                        arr = arr[lo:hi]
                    out[path] = arr
                return out

            def close(self):
                for buf in self._buffers.values():
                    for i in range(len(buf)):
                        buf[i] = 0  # corrupt any surviving view

        def factory(*a, **k):
            return _ViewBackedH5(_canned_arrays())

        self._patch_h5(monkeypatch, factory)
        df_out, meta = process_shard(
            _ReleaseGrid(), 0, ["s3://a"], s3_credentials={}, config=_release_cfg()
        )
        # The single cell pooled both photons (h_li = 100, 200) BEFORE the buffer
        # was zeroed; h_min must be the original 100.0, not 0.0 (corrupted) and the
        # count must be 2.
        assert meta["total_obs"] == 2
        assert df_out["count"].to_numpy()[0] == 2
        assert df_out["h_min"].to_numpy()[0] == np.float32(100.0)

    def test_constructor_failure_releases_nothing_and_others_proceed(self, monkeypatch):
        """If ``H5Coro(...)`` raises for one granule, the loop-top ``h5obj = None``
        guard means the ``finally`` has nothing to release (no spurious ``close()`` on
        the failed granule), the granule is skipped (caught by the outer ``except`` →
        ``continue``), and the remaining granules still process and release normally."""
        log: list = []
        calls = {"n": 0}

        def factory(*a, **k):
            calls["n"] += 1
            if calls["n"] == 2:  # second granule's constructor blows up
                raise RuntimeError("h5coro open failed")
            return _CloseRecordingH5(_canned_arrays(), log)

        self._patch_h5(monkeypatch, factory)
        df_out, meta = process_shard(
            _ReleaseGrid(),
            0,
            ["s3://a", "s3://b", "s3://c"],
            s3_credentials={},
            config=_release_cfg(),
        )
        closes = [e for e in log if e[0] == "close"]
        # Only the two granules whose constructor SUCCEEDED are closed; the failed one
        # left ``h5obj is None`` so the ``finally`` released nothing for it.
        assert len(closes) == 2
        # The two good granules were still read end-to-end.
        assert meta["files_processed"] == 2
        assert meta["total_obs"] == 4  # 2 photons × 2 surviving granules

    def test_read_exception_still_releases_in_finally(self, monkeypatch):
        """The whole point of ``try/finally`` (vs. a post-read ``close()``): if a read
        raises AFTER the H5Coro is constructed, the ``finally`` still releases that
        granule's cache exactly once."""
        log: list = []

        class _RaisingReadH5:
            """Constructs fine, records ``close()``, but its read raises."""

            def __init__(self, log):
                self._log = log

            def readDatasets(self, datasets):  # noqa: N802 (mirror real h5coro API)
                raise RuntimeError("byte-range read failed")

            def close(self):
                self._log.append(("close", id(self)))

        def factory(*a, **k):
            return _RaisingReadH5(log)

        self._patch_h5(monkeypatch, factory)
        df_out, meta = process_shard(
            _ReleaseGrid(), 0, ["s3://a"], s3_credentials={}, config=_release_cfg()
        )
        closes = [e for e in log if e[0] == "close"]
        # close() fired exactly once despite the read raising — the finally ran.
        assert len(closes) == 1
        # No data survived the failed read, so the shard reports the empty path.
        assert meta["error"] == "No data after filtering"
