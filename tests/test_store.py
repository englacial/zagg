"""Tests for the store factory."""

from pathlib import Path

from zarr.storage import LocalStore

from magg.store import open_store


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
