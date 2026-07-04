"""Tests for the store factory."""

from pathlib import Path
from unittest import mock

import pytest
from zarr.storage import LocalStore

from zagg.store import open_object_store, open_store, parse_s3_path


@pytest.fixture
def mock_s3(monkeypatch):
    """Patch obstore S3Store / Boto3CredentialProvider / zarr ObjectStore.

    Returns the (S3Store, Boto3CredentialProvider) mocks so tests can assert
    on the kwargs ``_open_s3_store`` passes to ``S3Store``.
    """
    s3_cls = mock.MagicMock(name="S3Store")
    prov_cls = mock.MagicMock(name="Boto3CredentialProvider")
    obj_cls = mock.MagicMock(name="ObjectStore")
    monkeypatch.setattr("obstore.store.S3Store", s3_cls)
    monkeypatch.setattr("obstore.auth.boto3.Boto3CredentialProvider", prov_cls)
    monkeypatch.setattr("zarr.storage.ObjectStore", obj_cls)
    return s3_cls, prov_cls


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


class TestOpenS3Store:
    """Assert the credential/endpoint branching in ``_open_s3_store``."""

    def test_no_creds_uses_credential_provider(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        open_store("s3://bucket/prefix.zarr")
        _, kwargs = s3_cls.call_args
        # Ambient path: credential_provider set, no explicit keys.
        assert kwargs["credential_provider"] is prov_cls.return_value
        assert "access_key_id" not in kwargs
        assert "secret_access_key" not in kwargs
        assert "session_token" not in kwargs
        assert "endpoint" not in kwargs
        # Default addressing unchanged (no path-style forced).
        assert "virtual_hosted_style_request" not in kwargs

    def test_explicit_creds(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        creds = {
            "accessKeyId": "AKIA",
            "secretAccessKey": "secret",
            "sessionToken": "tok",
        }
        open_store("s3://us-west-2.opendata.source.coop/foo.zarr", credentials=creds)
        _, kwargs = s3_cls.call_args
        assert kwargs["access_key_id"] == "AKIA"
        assert kwargs["secret_access_key"] == "secret"
        assert kwargs["session_token"] == "tok"
        assert "credential_provider" not in kwargs
        # Path-style addressing for dotted bucket names / external targets.
        assert kwargs["virtual_hosted_style_request"] is False
        prov_cls.assert_not_called()

    def test_explicit_creds_no_session_token(self, mock_s3):
        s3_cls, _ = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret"}
        open_store("s3://bucket/foo.zarr", credentials=creds)
        _, kwargs = s3_cls.call_args
        assert "session_token" not in kwargs

    def test_custom_endpoint(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret"}
        open_store(
            "s3://bucket/foo.zarr",
            credentials=creds,
            endpoint_url="https://acct.r2.cloudflarestorage.com",
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["endpoint"] == "https://acct.r2.cloudflarestorage.com"
        assert kwargs["virtual_hosted_style_request"] is False

    def test_endpoint_only_no_creds(self, mock_s3):
        # endpoint_url alone (no creds) still takes the explicit branch.
        s3_cls, prov_cls = mock_s3
        open_store("s3://bucket/foo.zarr", endpoint_url="https://minio.local")
        _, kwargs = s3_cls.call_args
        assert kwargs["endpoint"] == "https://minio.local"
        assert kwargs["virtual_hosted_style_request"] is False
        assert "credential_provider" not in kwargs


class TestOpenObjectStore:
    """Raw obstore store for side-channel objects (issue #151): same path and
    credential handling as ``open_store``, but no Zarr wrapper."""

    def test_local_roundtrip_creates_dir(self, tmp_path):
        import obstore

        store = open_object_store(str(tmp_path / "x.zarr.status" / "run1"))
        obstore.put(store, "12345.json", b'{"statusCode": 200}')
        got = bytes(obstore.get(store, "12345.json").bytes())
        assert got == b'{"statusCode": 200}'

    def test_local_missing_object_raises_not_found(self, tmp_path):
        import obstore
        from obstore.exceptions import NotFoundError

        store = open_object_store(str(tmp_path / "status"))
        with pytest.raises((FileNotFoundError, NotFoundError)):
            obstore.get(store, "nope.json")

    def test_s3_returns_bare_store_with_ambient_creds(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        store = open_object_store("s3://bucket/prefix.zarr.status/run1")
        # The raw S3Store, not wrapped in zarr's ObjectStore.
        assert store is s3_cls.return_value
        _, kwargs = s3_cls.call_args
        assert kwargs["credential_provider"] is prov_cls.return_value

    def test_s3_explicit_creds_and_endpoint(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret", "sessionToken": "tok"}
        open_object_store(
            "s3://bucket/foo.zarr.status/run1",
            credentials=creds,
            endpoint_url="https://minio.local",
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["access_key_id"] == "AKIA"
        assert kwargs["endpoint"] == "https://minio.local"
        assert kwargs["virtual_hosted_style_request"] is False
        prov_cls.assert_not_called()


class TestParseS3Path:
    def test_bucket_and_prefix(self):
        assert parse_s3_path("s3://mybucket/some/prefix.zarr") == ("mybucket", "some/prefix.zarr")

    def test_bucket_only(self):
        assert parse_s3_path("s3://mybucket") == ("mybucket", "")

    def test_not_s3_raises(self):
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("./local/path.zarr")
