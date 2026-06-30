"""Tests for the output writer abstraction (issue #12 Phase 6)."""

import numpy as np
import pandas as pd
import pytest

from zagg.output import (
    TabularWriter,
    Writer,
    ZarrGridWriter,
    get_writer,
    output_format,
    register_writer,
)
from zagg.output import base as output_base

# ---------------------------------------------------------------------------
# Registry / format resolution
# ---------------------------------------------------------------------------


class TestWriterRegistry:
    def test_zarr_resolves_to_grid_writer(self):
        assert isinstance(get_writer("zarr"), ZarrGridWriter)

    @pytest.mark.parametrize("fmt", ["tabular", "parquet", "csv"])
    def test_tabular_aliases_resolve_to_tabular_writer(self, fmt):
        assert isinstance(get_writer(fmt), TabularWriter)

    def test_unknown_format_raises_listing_known(self):
        with pytest.raises(ValueError, match="no writer for output.format='bogus'"):
            get_writer("bogus")

    def test_writers_satisfy_protocol(self):
        assert isinstance(ZarrGridWriter(), Writer)
        assert isinstance(TabularWriter(), Writer)

    def test_register_writer_rejects_empty_name(self):
        with pytest.raises(ValueError, match="non-empty"):
            register_writer("", TabularWriter)

    def test_register_writer_duplicate_raises_without_replace(self):
        with pytest.raises(ValueError, match="already registered"):
            register_writer("zarr", ZarrGridWriter)

    def test_register_writer_replace_overrides(self):
        sentinel = type("Sentinel", (), {})
        original = output_base._WRITERS["zarr"]
        try:
            register_writer("zarr", sentinel, replace=True)
            assert isinstance(get_writer("zarr"), sentinel)
        finally:
            # Restore the canonical mapping so other tests see the real writer.
            register_writer("zarr", original, replace=True)

    def test_register_writer_decorator_form(self):
        @register_writer("decotest")
        class _DecoWriter:
            def write(self, payload, **kwargs):
                return payload

        try:
            assert isinstance(get_writer("decotest"), _DecoWriter)
        finally:
            output_base._WRITERS.pop("decotest", None)


class TestOutputFormat:
    def test_defaults_to_zarr_for_spatial_config(self):
        from zagg.config import load_config_from_dict

        cfg = load_config_from_dict(
            {
                "data_source": {"reader": "h5coro"},
                "aggregation": {"variables": {"x": {"source": "h", "function": "np.mean"}}},
                "output": {"store": "./out.zarr"},
            }
        )
        assert output_format(cfg) == "zarr"

    def test_reads_explicit_format(self):
        from zagg.config import load_config_from_dict

        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["m"]},
                "aggregation": {
                    "variables": {
                        "x": {
                            "variable": "T",
                            "collection": "m",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                        }
                    }
                },
                "output": {"format": "parquet", "store": "out.parquet"},
            }
        )
        assert output_format(cfg) == "parquet"


# ---------------------------------------------------------------------------
# ZarrGridWriter: byte-identical forwarding to the spatial write functions
# ---------------------------------------------------------------------------


class TestZarrGridWriter:
    def test_write_forwards_to_write_dataframe_to_zarr(self, monkeypatch):
        from zagg.output import zarr_grid

        captured = {}

        def fake_write(carrier, store, *, grid, chunk_idx):
            captured["args"] = (carrier, store, grid, chunk_idx)
            return store

        monkeypatch.setattr(zarr_grid, "write_dataframe_to_zarr", fake_write)
        store = object()
        out = ZarrGridWriter().write("CARRIER", store, grid="GRID", chunk_idx=(1, 2))
        assert out is store
        assert captured["args"] == ("CARRIER", store, "GRID", (1, 2))

    def test_write_ragged_forwards(self, monkeypatch):
        from zagg.output import zarr_grid

        captured = {}

        def fake_ragged(ragged, store, *, grid, shard_key):
            captured["args"] = (ragged, store, grid, shard_key)
            return store

        monkeypatch.setattr(zarr_grid, "write_ragged_to_zarr", fake_ragged)
        store = object()
        out = ZarrGridWriter().write_ragged({"f": 1}, store, grid="GRID", shard_key=7)
        assert out is store
        assert captured["args"] == ({"f": 1}, store, "GRID", 7)

    def test_finalize_consolidates_v3(self, monkeypatch):
        from zagg.output import zarr_grid

        captured = {}

        def fake_consolidate(store, *, zarr_format):
            captured["zarr_format"] = zarr_format
            return store

        monkeypatch.setattr(zarr_grid, "consolidate_metadata", fake_consolidate)
        store = object()
        out = ZarrGridWriter().finalize(store)
        assert out is store
        assert captured["zarr_format"] == 3


# ---------------------------------------------------------------------------
# TabularWriter
# ---------------------------------------------------------------------------


def _result_rows():
    """Temporal result rows in the ``summary['results']`` shape."""
    return [
        {
            "event_key": "storm1",
            "results": {"max_t2m": 5.0, "min_t2m": 1.0},
            "meta": {"timesteps_processed": 2, "n_specs": 2},
        },
        {
            "event_key": "storm2",
            "results": {"max_t2m": 9.0, "min_t2m": 0.5},
            "meta": {"timesteps_processed": 3, "n_specs": 2},
        },
    ]


class TestTabularWriterFrame:
    def test_to_frame_one_row_per_event(self):
        frame = TabularWriter().to_frame(_result_rows())
        assert list(frame["event_key"]) == ["storm1", "storm2"]
        assert list(frame.columns)[:2] == ["event_key", "timesteps_processed"]
        assert frame.set_index("event_key").loc["storm2", "max_t2m"] == pytest.approx(9.0)

    def test_to_frame_aligns_missing_outputs_as_nan(self):
        rows = [
            {"event_key": "a", "results": {"x": 1.0}, "meta": {}},
            {"event_key": "b", "results": {"y": 2.0}, "meta": {}},
        ]
        frame = TabularWriter().to_frame(rows).set_index("event_key")
        assert np.isnan(frame.loc["a", "y"])
        assert np.isnan(frame.loc["b", "x"])

    def test_to_frame_empty_rows(self):
        frame = TabularWriter().to_frame([])
        assert frame.empty


class TestTabularWriterSerialise:
    def test_parquet_round_trip(self, tmp_path):
        path = tmp_path / "events.parquet"
        out = TabularWriter().write(_result_rows(), path)
        assert out == path
        back = pd.read_parquet(path)
        assert list(back["event_key"]) == ["storm1", "storm2"]
        assert back.set_index("event_key").loc["storm1", "max_t2m"] == pytest.approx(5.0)

    def test_csv_round_trip(self, tmp_path):
        path = tmp_path / "events.csv"
        TabularWriter().write(_result_rows(), path)
        back = pd.read_csv(path)
        assert list(back["event_key"]) == ["storm1", "storm2"]

    def test_format_override_beats_extension(self, tmp_path):
        # A .dat path with explicit csv format writes CSV.
        path = tmp_path / "events.dat"
        TabularWriter().write(_result_rows(), path, output_format="csv")
        back = pd.read_csv(path)
        assert list(back["event_key"]) == ["storm1", "storm2"]

    def test_unknown_extension_defaults_to_parquet(self, tmp_path):
        path = tmp_path / "events.unknown"
        TabularWriter().write(_result_rows(), path)
        back = pd.read_parquet(path)
        assert len(back) == 2

    def test_bad_format_raises(self, tmp_path):
        with pytest.raises(ValueError, match="unknown tabular output format"):
            TabularWriter().write(_result_rows(), tmp_path / "x", output_format="xml")


class TestTabularWriterToBytes:
    def test_parquet_bytes_round_trip(self):
        import io

        payload = TabularWriter().to_bytes(_result_rows(), output_format="parquet")
        assert payload[:4] == b"PAR1"
        back = pd.read_parquet(io.BytesIO(payload)).set_index("event_key")
        assert back.loc["storm1", "max_t2m"] == pytest.approx(5.0)

    def test_csv_bytes_round_trip(self):
        payload = TabularWriter().to_bytes(_result_rows(), output_format="csv")
        assert b"event_key" in payload

    def test_parquet_bytes_round_trip_without_pyarrow(self, monkeypatch):
        # The deployed Lambda layer ships fastparquet but NOT pyarrow (issue
        # #130). Pin that the in-memory Parquet path (to_bytes -> BytesIO ->
        # read_parquet) still works with pyarrow hidden, i.e. on fastparquet --
        # this is the engine the s3 put runs on the real worker.
        import builtins
        import io
        import sys

        real_import = builtins.__import__

        def _no_pyarrow(name, *a, **k):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError("pyarrow hidden for test")
            return real_import(name, *a, **k)

        for mod in [m for m in sys.modules if m == "pyarrow" or m.startswith("pyarrow.")]:
            monkeypatch.delitem(sys.modules, mod, raising=False)
        monkeypatch.setattr(builtins, "__import__", _no_pyarrow)

        payload = TabularWriter().to_bytes(_result_rows(), output_format="parquet")
        assert payload[:4] == b"PAR1"
        back = pd.read_parquet(io.BytesIO(payload)).set_index("event_key")
        assert back.loc["storm2", "max_t2m"] == pytest.approx(9.0)

    def test_bad_format_raises(self):
        with pytest.raises(ValueError, match="unknown tabular output format"):
            TabularWriter().to_bytes(_result_rows(), output_format="xml")


class TestWriteTabular:
    def test_local_path_round_trip(self, tmp_path):
        from zagg.output import write_tabular

        path = tmp_path / "events.parquet"
        out = write_tabular(_result_rows(), str(path))
        assert out == str(path)
        back = pd.read_parquet(path).set_index("event_key")
        assert back.loc["storm2", "max_t2m"] == pytest.approx(9.0)

    def test_local_tabular_alias_infers_from_suffix(self, tmp_path):
        # output_format="tabular" is the generic alias -> infer csv from .csv suffix.
        from zagg.output import write_tabular

        path = tmp_path / "events.csv"
        write_tabular(_result_rows(), str(path), output_format="tabular")
        back = pd.read_csv(path)
        assert list(back["event_key"]) == ["storm1", "storm2"]

    def test_s3_path_puts_single_object(self, monkeypatch):
        import io

        import obstore
        import obstore.store

        from zagg.output import write_tabular

        captured = {}

        def _fake_s3store(bucket, **opts):
            captured["bucket"] = bucket
            captured["opts"] = opts
            return object()

        def _fake_put(store, key, payload):
            captured["key"] = key
            captured["payload"] = payload

        monkeypatch.setattr(obstore.store, "S3Store", _fake_s3store)
        monkeypatch.setattr(obstore, "put", _fake_put)

        creds = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}
        out = write_tabular(_result_rows(), "s3://bucket/dir/events.parquet", credentials=creds)
        assert out == "s3://bucket/dir/events.parquet"
        assert captured["bucket"] == "bucket"
        assert captured["key"] == "dir/events.parquet"
        assert captured["opts"]["access_key_id"] == "a"
        back = pd.read_parquet(io.BytesIO(captured["payload"])).set_index("event_key")
        assert back.loc["storm1", "max_t2m"] == pytest.approx(5.0)

    def test_s3_path_without_key_raises(self):
        from zagg.output import write_tabular

        with pytest.raises(ValueError, match="object key"):
            write_tabular(_result_rows(), "s3://bucket")
