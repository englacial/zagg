"""Tests for the tabular output writer and the output-config factory."""

import pytest

from zagg.config import PipelineConfig
from zagg.output import TabularWriter, from_output_config

ROWS = {
    "storm_a": {"max_T2M": 14.0, "min_SLP": 980.0},
    "storm_b": {"max_T2M": 114.0, "min_SLP": 1001.0},
}


class TestTabularWriter:
    def test_to_frame(self):
        df = TabularWriter().to_frame(ROWS)
        assert list(df.index) == ["storm_a", "storm_b"]
        assert df.index.name == "event_key"
        assert df.loc["storm_b", "max_T2M"] == 114.0

    def test_write_parquet_by_extension(self, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        store = str(tmp_path / "out.parquet")
        TabularWriter(fmt="hdf5").write(ROWS, store)  # extension wins over fmt
        df = pd.read_parquet(store)
        assert df.loc["storm_a", "min_SLP"] == 980.0

    def test_write_parquet_by_fmt(self, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        store = str(tmp_path / "out.data")
        TabularWriter(fmt="parquet").write(ROWS, store)
        assert pd.read_parquet(store).shape == (2, 2)


class TestFromOutputConfig:
    def test_default_hdf5(self):
        cfg = PipelineConfig(output={})
        assert from_output_config(cfg).fmt == "hdf5"

    def test_format_selected(self):
        cfg = PipelineConfig(output={"format": "parquet"})
        assert from_output_config(cfg).fmt == "parquet"
