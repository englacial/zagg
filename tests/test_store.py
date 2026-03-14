"""Tests for the store factory."""

from pathlib import Path

import pytest
from zarr.storage import LocalStore

from magg.store import open_store, parse_s3_path


class TestOpenStore:
    def test_local_absolute_path(self, tmp_path):
        store = open_store(str(tmp_path / "test.zarr"))
        assert isinstance(store, LocalStore)

    def test_local_relative_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        store = open_store("./output.zarr")
        assert isinstance(store, LocalStore)
        assert Path(str(store.root)).is_absolute()

    def test_local_read_only(self, tmp_path):
        p = tmp_path / "test.zarr"
        p.mkdir()
        store = open_store(str(p), read_only=True)
        assert isinstance(store, LocalStore)


class TestParseS3Path:
    def test_bucket_and_prefix(self):
        assert parse_s3_path("s3://mybucket/some/prefix.zarr") == ("mybucket", "some/prefix.zarr")

    def test_bucket_only(self):
        assert parse_s3_path("s3://mybucket") == ("mybucket", "")

    def test_not_s3_raises(self):
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("./local/path.zarr")
