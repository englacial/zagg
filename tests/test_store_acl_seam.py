"""The issue #522 dual-handle split, end to end through the zagg seam.

``tests/test_store_acl_signing.py`` pins obstore's behaviour; this module pins
zagg's use of it, by driving real ``zagg.store`` handles against the same
in-memory S3 stand-in and reading the captured wire bytes. Nothing here mocks
obstore, so an assertion about ``SignedHeaders`` is an assertion about the bytes
a fleet worker would send.

The stand-in is reached through ``endpoint_url``, which
:func:`zagg.store._external_target` deliberately excludes (R2/MinIO do not
implement canned ACLs), so each test forces the external-target verdict on and
lets the rest of the seam run for real.
"""

import pytest
from test_store_acl_signing import ACL, FakeS3

STORE_PATH = "s3://us-west-2.opendata.source.coop/englacial/zagg/demo.zarr"
CREDS = {"accessKeyId": "AKIAEXAMPLE", "secretAccessKey": "secret"}


@pytest.fixture
def fake_s3():
    server = FakeS3()
    try:
        yield server
    finally:
        server.close()


@pytest.fixture
def published(monkeypatch, fake_s3):
    """Open zagg stores against the stand-in as if it were a published bucket.

    Returns a callable taking ``zagg.store`` opener kwargs.
    """
    import zagg.store as store_mod

    monkeypatch.setattr(store_mod, "_external_target", lambda *a, **k: True)
    store_mod._OBJECT_STORE_CACHE.clear()

    def _open(opener, path=STORE_PATH, **kwargs):
        kwargs.setdefault("client_options", {"allow_http": True})
        return opener(path, credentials=CREDS, endpoint_url=fake_s3.endpoint, **kwargs)

    yield _open
    store_mod._OBJECT_STORE_CACHE.clear()


@pytest.fixture
def ambient_credentials(monkeypatch):
    """Let the AMBIENT branch build without walking the botocore chain.

    ``_s3_store_pair`` constructs a real ``Boto3CredentialProvider`` when no
    ``credentials``/``endpoint_url`` are passed, and that raises
    ``ValueError: Received None from session.get_credentials`` wherever there
    is no AWS identity to find -- which is CI. obstore accepts any zero-arg
    callable as a credential provider, so a static one keeps the construction
    assertions exactly as they are while making the test hermetic (it also
    drops the ~300 ms chain walk these tests otherwise pay locally).
    """
    import obstore.auth.boto3

    def static_provider():
        return {"access_key_id": "A", "secret_access_key": "B", "expires_at": None}

    monkeypatch.setattr(
        obstore.auth.boto3, "Boto3CredentialProvider", lambda *a, **k: static_provider
    )


def _puts(fake_s3):
    return [r for r in fake_s3.requests if r.method == "PUT"]


def _lists(fake_s3):
    return [r for r in fake_s3.requests if r.method == "GET" and "list-type" in r.path]


class TestOwnershipSemanticsUnchanged:
    """Phase 3 of issue #522: the split must not cost the canned ACL.

    Source Cooperative's direct-IAM upload path requires
    ``bucket-owner-full-control`` on every object we publish, so the fix is only
    a fix if the ACL still rides every object-creating request -- and rides it
    inside the signature, which is where obstore already puts a default header
    on that request shape.
    """

    def test_a_real_zarr_write_signs_the_acl_on_every_put(self, published, fake_s3):
        # Driven through zarr, not through obstore directly: the write path
        # that publishes shard bytes is zarr's obstore adapter, and it is the
        # path issue #495's per-PUT alternative could not reach.
        import numpy as np
        import zarr

        from zagg.store import _AclWriteObjectStore, open_store

        zstore = published(open_store)
        assert isinstance(zstore, _AclWriteObjectStore)
        array = zarr.create_array(
            store=zstore, name="temp", shape=(4,), chunks=(2,), dtype="i4", zarr_format=3
        )
        array[:] = np.arange(4, dtype="i4")

        puts = _puts(fake_s3)
        assert puts, "the zarr write issued no PUT"
        for put in puts:
            assert put.acl == ACL, f"{put.path} published with no canned ACL"
            assert put.acl_signed, f"{put.path} carries an unsigned ACL: {put.signed_headers}"

    def test_an_oversize_chunk_is_written_as_one_signed_put(self, published, fake_s3):
        # Issue #534: the chunk objects that cross obstore's 5 MiB multipart
        # threshold are exactly the ones that failed, because ``UploadPart``
        # refuses ``x-amz-acl``. The test above cannot see them -- ``_puts()``
        # filters on ``method == "PUT"`` and a multipart flow opens with a
        # ``POST ?uploads`` -- so a regression to multipart would leave it green
        # while every ragged chunk 400'd in the fleet. So assert the framing and
        # not merely the header: one PUT for the chunk, no multipart request of
        # any shape.
        import numpy as np
        import zarr

        from zagg.store import open_store

        # 12 MiB in one uncompressed chunk: over obstore's multipart threshold,
        # small enough to stay fast.
        n = 3 * 1024 * 1024
        array = zarr.create_array(
            store=published(open_store),
            name="big",
            shape=(n,),
            chunks=(n,),
            dtype="i4",
            compressors=None,
            zarr_format=3,
        )
        array[:] = np.arange(n, dtype="i4")

        assert not [r for r in fake_s3.requests if r.method == "POST" and "uploads" in r.query]
        assert not [r for r in _puts(fake_s3) if "partNumber" in r.query]
        (chunk,) = [r for r in _puts(fake_s3) if r.path.endswith("/big/c/0")]
        assert chunk.acl == ACL, "the oversize chunk was published with no canned ACL"
        assert chunk.acl_signed, f"the chunk carries an unsigned ACL: {chunk.signed_headers}"
        # The bytes really landed: a single PUT that never happened would
        # satisfy every assertion above.
        assert len(fake_s3.objects["englacial/zagg/demo.zarr/big/c/0"]) == n * 4

    def test_an_oversize_side_channel_object_is_written_as_one_put(self, published, fake_s3):
        # The raw-obstore seam has the same bug and takes the same fix. Most of
        # what goes through ``put_object`` is small JSON, but the inline index
        # buffer, the temporal tabular object and a column backfill payload are
        # bounded by nothing that keeps them under 5 MiB.
        from zagg.store import open_object_store, put_object

        payload = b"x" * (8 * 1024 * 1024)
        put_object(published(open_object_store), "big.parquet", payload)

        assert not [r for r in fake_s3.requests if r.method == "POST"]
        (put,) = _puts(fake_s3)
        assert "partNumber" not in put.query
        assert put.acl == ACL
        assert put.acl_signed
        assert fake_s3.objects["englacial/zagg/demo.zarr/big.parquet"] == payload

    def test_a_conditional_write_is_a_single_put_too(self, published, fake_s3, monkeypatch):
        # ``set_if_not_exists`` and ``put_object(mode="create")`` -- the sweep
        # lease's claim -- are conditional puts, and obstore already declines
        # multipart for any mode other than "overwrite". So the WIRE cannot
        # tell the two apart: drop ``use_multipart=False`` from
        # ``set_if_not_exists`` and the framing below is unchanged. The
        # observable difference is the argument, so spy on it at the seam --
        # the way ``tests/test_output.py`` pins ``put_object`` -- and keep the
        # framing assertions alongside, so the twin's one-operation property
        # rests on our own request rather than on an obstore detail.
        import obstore
        from zarr.core.buffer import cpu
        from zarr.core.sync import sync

        from zagg.store import open_object_store, open_store, put_object

        real_put_async = obstore.put_async
        seen = []

        async def _spy(store, key, value, **kwargs):
            seen.append((key, kwargs))
            return await real_put_async(store, key, value, **kwargs)

        monkeypatch.setattr(obstore, "put_async", _spy)

        zstore = published(open_store)
        sync(zstore.set_if_not_exists("cond", cpu.Buffer.from_bytes(b"y" * (8 * 1024 * 1024))))
        put_object(published(open_object_store), "lease.json", b"{}", mode="create")

        (cond,) = [(key, kwargs) for key, kwargs in seen if key == "cond"]
        assert cond[1].get("mode") == "create"
        assert cond[1].get("use_multipart") is False, (
            f"set_if_not_exists left the framing to obstore: {cond[1]}"
        )

        assert not [r for r in fake_s3.requests if r.method == "POST"]
        assert len(_puts(fake_s3)) == 2
        for put in _puts(fake_s3):
            assert "partNumber" not in put.query
            assert put.acl == ACL and put.acl_signed

    def test_an_in_account_write_still_multiparts(self, fake_s3, monkeypatch):
        # The fix must stay invisible off the published path: declining
        # multipart is the price of carrying the ACL, and an in-account target
        # carries no ACL, so it keeps obstore's own framing and its parallelism.
        import obstore

        import zagg.store as store_mod

        monkeypatch.setattr(store_mod, "_external_target", lambda *a, **k: False)
        payload = b"x" * (8 * 1024 * 1024)
        store = store_mod.open_object_store(
            STORE_PATH,
            credentials=CREDS,
            endpoint_url=fake_s3.endpoint,
            client_options={"allow_http": True},
        )
        assert store_mod.acl_write_store(store) is store
        store_mod.put_object(store, "big", payload)

        assert [r for r in fake_s3.requests if r.method == "POST" and "uploads" in r.query]
        assert [r for r in _puts(fake_s3) if "partNumber" in r.query]
        assert obstore.get(store, "big").bytes() == payload

    def test_a_payload_past_the_single_put_ceiling_is_refused_by_key(self):
        # The tripwire for the day a chunk object outgrows ``PutObject``.
        # Driven on the check itself rather than through a 5 GiB buffer, and
        # asserted to name the key, because saying WHICH object is its job.
        from types import SimpleNamespace

        from zagg.store import _SINGLE_PUT_MAX_BYTES, _check_single_put

        _check_single_put("small", b"x" * 1024)
        _check_single_put("unmeasurable", object())
        with pytest.raises(ValueError, match=r"19/h_tdigest_signal/c/0.*multipart"):
            _check_single_put(
                "19/h_tdigest_signal/c/0",
                SimpleNamespace(nbytes=_SINGLE_PUT_MAX_BYTES + 1),
            )

    def test_put_object_signs_the_acl_on_a_side_channel_write(self, published, fake_s3):
        # Status envelopes, hive manifests and stats sidecars are real objects
        # in the published bucket, so they hand over ownership too.
        from zagg.store import open_object_store, put_object

        put_object(published(open_object_store), "run1.json", b"{}")

        (put,) = _puts(fake_s3)
        assert put.acl == ACL
        assert put.acl_signed

    def test_the_handle_the_caller_holds_never_sends_an_acl(self, published, fake_s3):
        # The other half of the contract: reads and lists carry no ACL at all,
        # signed or otherwise, so there is nothing for S3 to reject.
        import obstore

        from zagg.store import open_object_store, put_object

        store = published(open_object_store)
        put_object(store, "run1.json", b"{}")
        fake_s3.clear()
        obstore.get(store, "run1.json").bytes()
        list(obstore.list(store))

        assert fake_s3.requests
        for request in fake_s3.requests:
            assert request.acl is None, f"{request.method} {request.path} still carries the ACL"


class TestTheRequestsThatDied:
    """Phase 4 of issue #522: the exact shapes the fleet failed on, pinned."""

    def test_a_published_bucket_handle_lists_the_digit_tree(self, published, fake_s3):
        # The per-leaf template guard's ``list_with_delimiter`` of the hive
        # digit tree -- the request 2,726/2,726 workers 500'd on. Against real
        # S3 the pre-fix handle answers 403 AccessDenied / HeadersNotSigned;
        # here the pin is that the request goes out clean and signed, which is
        # the same statement one layer down.
        import obstore

        from zagg.store import open_object_store, put_object

        store = published(open_object_store)
        put_object(store, "0/1/leaf.zarr/zarr.json", b"{}")
        fake_s3.clear()

        listing = obstore.list_with_delimiter(store)
        assert [str(p) for p in listing["common_prefixes"]] == ["0"]

        (request,) = _lists(fake_s3)
        assert request.acl is None
        # Signed, not merely header-free: an unsigned LIST would fail for a
        # different reason and this test would still be green.
        assert "x-amz-content-sha256" in request.signed_headers

    def test_the_status_poller_lists_its_channel(self, published, fake_s3):
        # The client-side half of the same failure: the event transport's
        # poller listing a run's ``.status/`` channel on a published bucket.
        from zagg.client_transport import StatusPoller
        from zagg.store import open_object_store, put_object

        prefix = f"{STORE_PATH}.status/run-e1ebd1c0"
        put_object(published(open_object_store, path=prefix), "shard-0.json", b"{}")
        fake_s3.clear()

        poller = StatusPoller(
            store_factory=lambda: published(open_object_store, path=prefix),
            drop_timeout_s=1.0,
        )
        assert poller._list_keys() == {"shard-0.json"}
        assert all(request.acl is None for request in fake_s3.requests)

    def test_an_in_account_handle_is_unchanged(self, ambient_credentials):
        # The fix must be invisible to every target that never needed the ACL:
        # one handle, no twin, no client_options, and the plain zarr adapter.
        from zarr.storage import ObjectStore

        from zagg.store import (
            _ACL_WRITE_STORE_ATTR,
            _s3_object_store,
            acl_write_store,
            open_store,
        )

        raw = _s3_object_store("s3://our-bucket/out.zarr")
        assert raw.client_options is None
        assert not hasattr(raw, _ACL_WRITE_STORE_ATTR)
        assert acl_write_store(raw) is raw

        zstore = open_store("s3://our-bucket/out.zarr")
        assert type(zstore) is ObjectStore

    def test_a_read_only_published_handle_is_unchanged(self, ambient_credentials):
        # ``read_only=True`` was already excluded from the ACL (issue #223), so
        # it must not grow a twin now either.
        from zagg.store import _ACL_WRITE_STORE_ATTR, _s3_object_store

        raw = _s3_object_store(STORE_PATH, read_only=True)
        assert raw.client_options is None
        assert not hasattr(raw, _ACL_WRITE_STORE_ATTR)
