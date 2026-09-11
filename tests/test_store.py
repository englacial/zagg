"""Tests for the store factory."""

from pathlib import Path
from unittest import mock

import pytest
from zarr.storage import LocalStore

from zagg.store import open_object_store, open_store, parse_s3_path


@pytest.fixture
def mock_s3(monkeypatch):
    """Patch obstore S3Store / Boto3CredentialProvider / the zarr adapters.

    Returns the (S3Store, Boto3CredentialProvider) mocks so tests can assert
    on the kwargs ``_open_s3_store`` passes to ``S3Store``. An external write
    target builds TWO stores (issue #522: a clean handle and its ACL-bearing
    twin), so ``call_args`` there is the twin's and ``call_args_list`` holds
    both, clean first.

    ``_AclWriteObjectStore`` is mocked out too, so a ``mock_s3`` test pins how
    the handles are CONSTRUCTED and can no longer catch a write-routing
    regression; routing is covered by ``tests/test_store_acl_seam.py``.
    """
    s3_cls = mock.MagicMock(name="S3Store")
    prov_cls = mock.MagicMock(name="Boto3CredentialProvider")
    obj_cls = mock.MagicMock(name="ObjectStore")
    monkeypatch.setattr("obstore.store.S3Store", s3_cls)
    monkeypatch.setattr("obstore.auth.boto3.Boto3CredentialProvider", prov_cls)
    # Both zarr adapters are bound as module globals in zagg.store, so they are
    # patched there rather than on zarr.storage.
    monkeypatch.setattr("zagg.store.ObjectStore", obj_cls)
    monkeypatch.setattr("zagg.store._AclWriteObjectStore", mock.MagicMock(name="AclObjectStore"))
    return s3_cls, prov_cls


@pytest.fixture(autouse=True)
def _clear_object_store_cache():
    """Reset the process-global ambient store cache (issue #287) around every
    test so one test's cached store can't leak into another's assertions."""
    from zagg import store

    store._OBJECT_STORE_CACHE.clear()
    yield
    store._OBJECT_STORE_CACHE.clear()


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


class TestBucketOwnerAcl:
    """Issue #495: writes to a target we do not own must hand ownership to the
    bucket owner. S3 object ownership follows the WRITING account, so a
    cross-account PUT without ``x-amz-acl: bucket-owner-full-control`` leaves
    objects the bucket owner cannot manage -- Source Cooperative's in-region
    upload grant requires the canned ACL for exactly that reason. obstore has no
    ACL config key, so it rides as a default request header."""

    HEADER = {"x-amz-acl": "bucket-owner-full-control"}
    CREDS = {"accessKeyId": "ASIA", "secretAccessKey": "secret", "sessionToken": "tok"}

    def test_external_target_sets_canned_acl(self, mock_s3):
        s3_cls, _ = mock_s3
        open_store(
            "s3://us-west-2.opendata.source.coop/englacial/zagg/d.zarr", credentials=self.CREDS
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == self.HEADER

    def test_side_channel_objects_get_the_header_too(self, mock_s3):
        # Status envelopes, hive manifests, stats sidecars and the temporal
        # tabular object are real writes to the same external store, so the
        # raw-obstore route must carry the ACL as well.
        s3_cls, _ = mock_s3
        open_object_store(
            "s3://us-west-2.opendata.source.coop/englacial/zagg/d.zarr.status/run1",
            credentials=self.CREDS,
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == self.HEADER

    def test_ambient_write_to_a_published_bucket_sends_the_header(self, mock_s3):
        # Phase 3 (issue #495) made publishing AMBIENT: the execution role
        # reaches source.coop with no injected credentials. Keying the ACL on
        # "did the caller pass credentials?" would therefore stop sending it on
        # exactly the path it exists for, and the fleet would publish objects
        # Source Cooperative cannot manage -- silently, since the PUT succeeds.
        # The destination decides instead (review finding on PR #496).
        s3_cls, _ = mock_s3
        open_store("s3://us-west-2.opendata.source.coop/englacial/zagg/demo/d.zarr")
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == self.HEADER
        # ...and through the raw-obstore route too (status envelopes, hive
        # manifests, stats sidecars), including its ambient per-process cache.
        open_object_store(
            "s3://us-west-2.opendata.source.coop/englacial/zagg/demo/d.zarr.status/run1"
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == self.HEADER

    def test_the_ambient_pair_shares_one_credential_provider(self, mock_s3):
        # The fleet's real branch: no injected credentials, no endpoint_url, so
        # ``_s3_store_pair`` builds a ``Boto3CredentialProvider`` -- once, and
        # hands the SAME one to both handles. Its ``__init__`` eagerly walks
        # the botocore credential chain (~300 ms), so moving the construction
        # inside ``build()`` (the obvious tidy-up for anyone reading ``build``
        # as self-contained) would pay it twice on every published open
        # (issue #287). ``TestObjectStoreCache`` counts providers too, but for
        # an in-account bucket -- one handle, no twin -- so the pair is pinned
        # only here. ``tests/test_store_acl_seam.py`` cannot cover it: its
        # stand-in is reached through ``endpoint_url``, which is the other arm.
        s3_cls, prov_cls = mock_s3
        open_store("s3://us-west-2.opendata.source.coop/englacial/zagg/demo/d.zarr")
        assert s3_cls.call_count == 2, "expected a clean handle and its ACL twin"
        assert prov_cls.call_count == 1
        clean, acl = s3_cls.call_args_list
        assert clean.kwargs["credential_provider"] is prov_cls.return_value
        assert acl.kwargs["credential_provider"] is prov_cls.return_value

    def test_in_account_write_sends_no_acl_header(self, mock_s3):
        # Ambient writes to a bucket we OWN: nothing to hand over, and no
        # client_options are introduced at all. Scoped to ownership, not to
        # ambience -- the header requires s3:PutObjectAcl on the target, which
        # the execution role holds on the published prefix only, so sending it
        # to every AWS endpoint would 403 self-hosters' own output buckets and
        # sliderule-public-cors (whose bucket policy is not ours to change).
        s3_cls, _ = mock_s3
        open_store("s3://our-bucket/out.zarr")
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs
        open_object_store("s3://our-bucket/out.zarr.status/run1")
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs
        open_store("s3://sliderule-public-cors/zagg-examples/d.zarr")
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs

    def test_anonymous_read_sends_no_acl_header(self, mock_s3):
        s3_cls, _ = mock_s3
        open_store("s3://public-bucket/demo.zarr", read_only=True, skip_signature=True)
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs

    def test_read_only_published_store_sends_no_acl_header(self, mock_s3):
        # The read_only exclusion is unchanged by the destination-keyed
        # predicate: reading the published store back is not a write.
        s3_cls, _ = mock_s3
        open_store("s3://us-west-2.opendata.source.coop/englacial/zagg/demo/d.zarr", read_only=True)
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs

    def test_custom_endpoint_sends_no_acl_header(self, mock_s3):
        # Canned ACLs are an AWS-S3 concept; the S3-compatible stores behind
        # endpoint_url (R2, MinIO) do not implement them.
        s3_cls, _ = mock_s3
        open_store(
            "s3://bucket/foo.zarr",
            credentials=self.CREDS,
            endpoint_url="https://acct.r2.cloudflarestorage.com",
        )
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs
        # ...and the exclusion still wins over the destination, which is what
        # keeps the retired data.source.coop proxy hop (an endpoint-routed AWS
        # target) out of the header path.
        open_store(
            "s3://us-west-2.opendata.source.coop/englacial/zagg/demo/d.zarr",
            endpoint_url="https://data.source.coop",
        )
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs

    def test_read_with_input_credentials_sends_no_acl_header(self, mock_s3):
        # The issue #223 consumer-INPUT channel: explicit credentials that read
        # somebody else's bucket (temporal.open_dataset's .zarr branch), not a
        # write target of ours. read_only is consumed by _s3_object_store and
        # never reaches S3Store.
        s3_cls, _ = mock_s3
        open_store("s3://someones-input/mask.zarr", read_only=True, credentials=self.CREDS)
        _, kwargs = s3_cls.call_args
        assert "client_options" not in kwargs
        assert "read_only" not in kwargs

    def test_object_store_reads_are_the_documented_exception(self, mock_s3):
        # open_object_store has no read_only concept, so temporal's NetCDF
        # branch (a pure GET of an input bucket) still carries the header. Inert
        # -- S3 reads x-amz-acl only on object-creating requests -- and pinned
        # here so the exception stays visible.
        s3_cls, _ = mock_s3
        open_object_store("s3://someones-input", credentials=self.CREDS, region="us-west-2")
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == self.HEADER

    def test_caller_client_options_are_preserved(self, mock_s3):
        # Additive merge: unrelated client options survive, and a caller who
        # sets x-amz-acl explicitly wins over the default.
        s3_cls, _ = mock_s3
        open_store(
            "s3://external/foo.zarr",
            credentials=self.CREDS,
            client_options={
                "timeout": "30s",
                "default_headers": {"x-amz-acl": "private", "x-custom": "1"},
            },
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["timeout"] == "30s"
        assert kwargs["client_options"]["default_headers"] == {
            "x-amz-acl": "private",
            "x-custom": "1",
        }

    def test_caller_mixed_case_acl_header_still_wins(self, mock_s3):
        # obstore lowercases header keys, so a raw setdefault cannot see
        # ``X-Amz-Acl`` and the collision resolves inside obstore in OUR
        # favour -- a caller who deliberately asked for ``private`` would be
        # silently overridden (review finding). Lowercasing the merge fixes it.
        s3_cls, _ = mock_s3
        open_store(
            "s3://external/foo.zarr",
            credentials=self.CREDS,
            client_options={"default_headers": {"X-Amz-Acl": "private", "X-Custom": "1"}},
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == {
            "x-amz-acl": "private",
            "x-custom": "1",
        }

    def test_caller_can_strip_the_header_with_an_explicit_none(self, mock_s3):
        # The escape hatch for an external AWS target that must send no ACL:
        # neither obstore-legal value expresses absence (None is rejected, ""
        # is a live empty header S3 rejects), so a None value means REMOVE.
        s3_cls, _ = mock_s3
        open_store(
            "s3://external/foo.zarr",
            credentials=self.CREDS,
            client_options={"default_headers": {"x-amz-acl": None, "x-custom": "1"}},
        )
        _, kwargs = s3_cls.call_args
        assert kwargs["client_options"]["default_headers"] == {"x-custom": "1"}

    def test_the_escape_hatch_leaves_the_read_handle_option_free(self):
        # The branch the split has to get right: with no ACL to send there is
        # no twin, and the surviving handle must be built byte-identically to
        # an in-account one -- ``None``, not ``{"default_headers": {}}``. The
        # difference is invisible to obstore today, and byte-identity is the
        # property the whole issue #522 fix rests on.
        from zagg.store import _acl_client_options

        assert _acl_client_options({"default_headers": {"x-amz-acl": None}}) == (None, None)
        # A caller's unrelated options still ride the read handle.
        assert _acl_client_options(
            {"default_headers": {"x-amz-acl": None}, "allow_http": True}
        ) == ({"allow_http": True}, None)

    def test_the_escape_hatch_builds_one_clean_store(self):
        # Same branch through _s3_store_pair, deliberately UNMOCKED (no
        # network: construction only) -- a MagicMock would auto-create the twin
        # attribute and the assertion would be vacuous. One handle, no
        # client_options, no twin.
        from zagg.store import _ACL_WRITE_STORE_ATTR, _s3_object_store, acl_write_store

        s3 = _s3_object_store(
            "s3://external/foo.zarr",
            credentials=self.CREDS,
            client_options={"default_headers": {"x-amz-acl": None}},
        )
        assert s3.client_options is None
        assert not hasattr(s3, _ACL_WRITE_STORE_ATTR)
        assert acl_write_store(s3) is s3

    def test_a_real_obstore_store_accepts_and_normalizes_the_header(self):
        # Deliberately UNMOCKED (no network: construction only). Every other
        # test here mocks S3Store, so they pin what zagg passes and never what
        # obstore accepts -- client_options/default_headers validate only at
        # real construction (a typo raises ValueError: Invalid key), so an
        # obstore rename would leave this class green while the fleet wrote
        # owner-less objects into someone else's bucket. Note the bytes:
        # obstore normalizes header values.
        from zagg.store import _s3_object_store, acl_write_store

        s3 = _s3_object_store("s3://external/foo.zarr", credentials=self.CREDS)
        # The handle the caller holds is clean -- it is the one that lists
        # (issue #522) -- and the ACL rides its twin.
        assert s3.client_options is None
        twin = acl_write_store(s3)
        assert twin is not s3
        assert twin.client_options["default_headers"]["x-amz-acl"] == b"bucket-owner-full-control"

    def test_a_real_obstore_read_only_store_carries_no_header(self):
        # The issue #223 consumer-input shape, unmocked: no client_options at
        # all reach obstore, so nothing rides the GET.
        from zagg.store import _s3_object_store

        s3 = _s3_object_store(
            "s3://someones-input/mask.zarr", credentials=self.CREDS, read_only=True
        )
        assert s3.client_options is None

    def test_caller_client_options_are_not_mutated(self, mock_s3):
        s3_cls, _ = mock_s3
        options = {"default_headers": {"x-custom": "1"}}
        open_store("s3://external/foo.zarr", credentials=self.CREDS, client_options=options)
        assert options == {"default_headers": {"x-custom": "1"}}


class TestRetryConfigIsNotShared:
    def test_the_two_handles_do_not_alias_one_retry_config(self, mock_s3):
        # Issue #522 split: ``dict(kwargs)`` per handle is shallow, so without
        # a nested copy the clean handle and its ACL twin hold the SAME
        # retry_config dict -- the aliasing the unconditional deepcopy in
        # _s3_store_pair exists to prevent, one level down.
        from zagg.store import _S3_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store(
            "s3://us-west-2.opendata.source.coop/englacial/zagg/d.zarr",
            credentials=TestBucketOwnerAcl.CREDS,
        )
        clean, twin = (c.kwargs["retry_config"] for c in s3_cls.call_args_list)
        assert clean == twin == _S3_RETRY_CONFIG
        assert clean is not twin
        assert clean is not _S3_RETRY_CONFIG


class TestAclWriteObjectStoreEquality:
    """Issue #522: the twin-bearing store must not be substitutable by equality.

    The inherited ``ObjectStore.__eq__`` looks only at ``read_only`` and
    ``self.store``, which is the CLEAN handle both kinds can hold -- so without
    an override a plain store compares equal to this one and an
    equality-keyed dedupe/cache can drop the ACL twin with no error and no log.
    """

    CREDS = TestBucketOwnerAcl.CREDS

    def _store(self):
        # Unmocked: real obstore handles, construction only, no network.
        from zagg.store import _AclWriteObjectStore, _open_s3_store

        zstore = _open_s3_store("s3://external/foo.zarr", credentials=self.CREDS)
        assert isinstance(zstore, _AclWriteObjectStore)
        return zstore

    def test_a_plain_store_over_the_clean_handle_is_not_equal(self):
        import zarr.storage

        zstore = self._store()
        plain = zarr.storage.ObjectStore(zstore.store)
        assert zstore != plain
        assert plain != zstore  # reflected: the subclass override runs first

    def test_it_still_equals_itself_and_its_read_only_view(self):
        zstore = self._store()
        assert zstore == zstore
        assert zstore == zstore.with_read_only(False)
        assert zstore != zstore.with_read_only(True)

    def test_an_in_account_store_is_a_plain_object_store(self):
        # No twin, no subclass, and the inherited equality is untouched.
        import zarr.storage

        from zagg.store import _AclWriteObjectStore, _open_s3_store

        zstore = _open_s3_store("s3://our-bucket/foo.zarr", skip_signature=True)
        assert not isinstance(zstore, _AclWriteObjectStore)
        assert zstore == zarr.storage.ObjectStore(zstore.store)


class TestPutObjectRouting:
    """Issue #522: object-creating requests, and only those, take the ACL twin."""

    def test_put_object_routes_to_the_twin(self):
        from zagg.store import _ACL_WRITE_STORE_ATTR, acl_write_store, put_object

        seen = {}

        class FakeStore:
            pass

        store = FakeStore()
        twin = FakeStore()
        setattr(store, _ACL_WRITE_STORE_ATTR, twin)
        assert acl_write_store(store) is twin

        with mock.patch("obstore.put", lambda s, k, v, **kw: seen.update(store=s, key=k)):
            put_object(store, "k", b"v")
        assert seen["store"] is twin

    def test_put_object_is_a_no_op_without_a_twin(self):
        # In-account and local stores have no twin, so the wrapper is exactly
        # ``obstore.put`` -- callers never have to know which kind they hold.
        from zagg.store import put_object

        seen = {}

        class FakeStore:
            pass

        store = FakeStore()
        with mock.patch("obstore.put", lambda s, k, v, **kw: seen.update(store=s, kw=kw)):
            put_object(store, "k", b"v", mode="create")
        assert seen["store"] is store
        assert seen["kw"] == {"mode": "create"}

    # Every obstore call that CREATES an object, so a write cannot slip past
    # put_object by picking a different API: the async twin of ``put``, the
    # multipart writer (``CreateMultipartUpload`` is where a canned ACL applies
    # on a multipart object), and the two CopyObject spellings.
    _OBSTORE_WRITE_APIS = frozenset({"put", "put_async", "open_writer", "copy", "rename"})

    @classmethod
    def _obstore_write_calls(cls, path):
        """Line numbers in ``path`` calling an object-creating obstore API.

        AST rather than substring: it follows ``import obstore as obs`` and
        ``from obstore import put`` (both of which reach the same wire request
        while spelling nothing like ``obstore.put(``), and it does not fire on
        the word appearing in a comment or docstring.
        """
        import ast

        tree = ast.parse(path.read_text())
        modules = set()  # names bound to the obstore module itself
        direct = set()  # names bound to a write API imported from obstore
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules |= {a.asname or a.name for a in node.names if a.name == "obstore"}
            elif isinstance(node, ast.ImportFrom) and node.module == "obstore":
                direct |= {
                    a.asname or a.name for a in node.names if a.name in cls._OBSTORE_WRITE_APIS
                }
        lines = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            hit = (
                isinstance(fn, ast.Attribute)
                and isinstance(fn.value, ast.Name)
                and fn.value.id in modules
                and fn.attr in cls._OBSTORE_WRITE_APIS
            ) or (isinstance(fn, ast.Name) and fn.id in direct)
            if hit:
                lines.append(node.lineno)
        return lines

    def test_no_module_calls_an_obstore_write_api_directly(self):
        # The routing only holds if every write goes through put_object. A
        # direct ``obstore.put`` on a published-bucket handle succeeds and
        # publishes an object Source Cooperative cannot manage -- silently --
        # so the seam is enforced here rather than by review.
        #
        # ``deployment/aws/lambda_handler.py`` is in the sweep because it ships
        # in the same zip as the package and is where the fleet's writes come
        # from, but it is not under ``src/zagg`` (issue #522 fold).
        import zagg

        pkg = Path(zagg.__file__).parent
        repo = Path(__file__).parent.parent
        files = sorted(pkg.rglob("*.py")) + sorted((repo / "deployment" / "aws").glob("*.py"))
        seam = pkg / "store.py"  # by path: any other store.py is not the seam
        offenders = [
            f"{path}:{n}" for path in files if path != seam for n in self._obstore_write_calls(path)
        ]
        assert offenders == [], f"use zagg.store.put_object instead: {offenders}"

    # The same question asked of boto3 (issue #534 fold). The obstore scan
    # above is what makes "every raw write goes through put_object" true, but
    # it only knows obstore NAMES -- a boto3 client writes the same objects and
    # is invisible to it. That gap matters in the direction that stays quiet:
    # boto3's transfer manager puts a canned ACL on ``CreateMultipartUpload``
    # and not on ``UploadPart``, so it never hits the 400 of issue #534, and a
    # boto3 write to a published bucket with NO ``ACL`` ExtraArg simply creates
    # objects our account owns and Source Cooperative cannot manage -- issue
    # #495's failure, with nothing to notice.
    #
    # Attribute calls only, and no attempt to prove the receiver is a boto3
    # client: these names are distinctive enough, tracking client bindings
    # through ``boto3.client(...)`` / ``session.client(...)`` / an injected
    # handle is not tractable, and a false positive here is a loud test failure
    # rather than a silent publish. ``copy`` is deliberately absent -- numpy's
    # ``.copy()`` is everywhere and ``copy_object`` is the unambiguous
    # spelling. zagg's own ``put_object`` is always a bare-Name call, so the
    # attribute-only rule does not collide with it.
    _BOTO3_WRITE_APIS = frozenset(
        {
            "upload_file",
            "upload_fileobj",
            "put_object",
            "copy_object",
            "create_multipart_upload",
            "upload_part",
        }
    )

    # The boto3 writes that exist today, each with the reason it is allowed to
    # stand outside the put_object seam. Keyed by (module suffix, API) rather
    # than by line, so an unrelated edit above them does not churn this list --
    # while a NEW boto3 write, or a new API in one of these files, still fails.
    _KNOWN_BOTO3_WRITES = {
        # Carries the canned ACL itself, via ``_copy_acl`` on the same
        # ``_external_target`` predicate the store seam uses: the skip-run touch
        # re-creates an object and must not strip the ownership an earlier PUT
        # handed over (issue #495).
        ("zagg/lifecycle.py", "copy_object"),
        # No ACL, and none needed: the extraction Lambda delivers boundary
        # parquets to an IN-ACCOUNT bucket. Held by its docstring rather than
        # by a check -- ``output_prefix`` reaches ``run_extraction`` straight
        # from the invoke event -- so if that ever points at a published
        # bucket, this entry is the thing to revisit (issue #534 review).
        ("zagg/catalog/extract.py", "upload_file"),
    }

    @classmethod
    def _boto3_write_calls(cls, path):
        """``(attr, lineno)`` for each object-creating boto3 call in ``path``."""
        import ast

        return [
            (node.func.attr, node.lineno)
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in cls._BOTO3_WRITE_APIS
        ]

    def test_no_new_module_writes_an_object_through_boto3(self):
        # Fails closed: a boto3 write added anywhere under the package (or in
        # the handler that ships beside it) has to be added to
        # ``_KNOWN_BOTO3_WRITES`` with a reason, which is the moment to decide
        # whether its destination needs the canned ACL.
        import zagg

        pkg = Path(zagg.__file__).parent
        repo = Path(__file__).parent.parent
        files = sorted(pkg.rglob("*.py")) + sorted((repo / "deployment" / "aws").glob("*.py"))

        found, offenders = set(), []
        for path in files:
            module = path.relative_to(pkg.parent if path.is_relative_to(pkg) else repo).as_posix()
            for attr, lineno in self._boto3_write_calls(path):
                found.add((module, attr))
                if (module, attr) not in self._KNOWN_BOTO3_WRITES:
                    offenders.append(f"{module}:{lineno} {attr}")
        assert offenders == [], (
            "a boto3 write outside the put_object seam -- route it through "
            f"zagg.store.put_object, or allow it in _KNOWN_BOTO3_WRITES with a reason: {offenders}"
        )
        # And the list stays honest in the other direction: an entry whose call
        # is gone (routed through put_object, say) must not linger as cover for
        # a future one.
        assert found == self._KNOWN_BOTO3_WRITES, (
            f"stale _KNOWN_BOTO3_WRITES entries: {self._KNOWN_BOTO3_WRITES - found}"
        )

    def test_the_boto3_guard_catches_a_new_upload(self, tmp_path):
        # Same treatment as the obstore scan below: a guard is only worth its
        # docstring if it fires. Pins that the receiver's spelling does not
        # matter and that ``.copy()`` is not swept up with ``copy_object``.
        sneaky = tmp_path / "sneaky.py"
        sneaky.write_text(
            "import boto3\n"
            "def f(client, arr):\n"
            "    boto3.client('s3').upload_file('a', 'b', 'c')\n"
            "    client.put_object(Bucket='b', Key='k', Body=b'v')\n"
            "    return arr.copy()  # numpy, not S3\n"
        )
        assert self._boto3_write_calls(sneaky) == [("upload_file", 3), ("put_object", 4)]

    def test_the_guard_catches_an_aliased_write(self, tmp_path):
        # The scan is only worth its docstring if it fires; pin the two shapes
        # a substring match used to miss.
        sneaky = tmp_path / "sneaky.py"
        sneaky.write_text(
            "import obstore as obs\n"
            "from obstore import open_writer\n"
            "def f(store):\n"
            "    obs.put_async(store, 'k', b'v')\n"
            "    open_writer(store, 'k')\n"
            "    # obstore.put(store, 'k', b'v') in a comment is not a call\n"
        )
        assert self._obstore_write_calls(sneaky) == [4, 5]


class TestS3RetryConfig:
    """Issue #186: S3 stores get a paced retry policy by default. obstore's
    default (10 retries, 100 ms init backoff, uniform jitter) exhausts its whole
    budget in ~2-4 s under a sustained 503 SlowDown burst — near-immediate
    retries that amplify the throttle instead of riding it out."""

    def test_default_retry_config_ambient(self, mock_s3):
        from zagg.store import _S3_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store("s3://bucket/prefix.zarr")
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == _S3_RETRY_CONFIG

    def test_default_retry_config_explicit_creds(self, mock_s3):
        from zagg.store import _S3_RETRY_CONFIG

        s3_cls, _ = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret"}
        open_object_store("s3://bucket/foo.zarr.status/run1", credentials=creds)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == _S3_RETRY_CONFIG

    def test_caller_override_wins(self, mock_s3):
        s3_cls, _ = mock_s3
        custom = {"max_retries": 2}
        open_store("s3://bucket/prefix.zarr", retry_config=custom)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == custom

    def test_explicit_none_gets_default(self, mock_s3):
        """``retry_config=None`` means the zagg default, not obstore's
        fast-burn default (a bare ``setdefault`` would let None through)."""
        from zagg.store import _S3_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store("s3://bucket/prefix.zarr", retry_config=None)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == _S3_RETRY_CONFIG

    def test_read_only_gets_short_policy(self, mock_s3):
        """``read_only=True`` (the interactive population) fails fast on a
        dead endpoint: the shorter read-only policy, not the write one."""
        from zagg.store import _S3_READONLY_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store("s3://bucket/prefix.zarr", read_only=True)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == _S3_READONLY_RETRY_CONFIG
        # No aliasing, nested backoff included (mirrors test_default_not_aliased).
        assert kwargs["retry_config"] is not _S3_READONLY_RETRY_CONFIG
        assert kwargs["retry_config"]["backoff"] is not _S3_READONLY_RETRY_CONFIG["backoff"]

    def test_read_only_explicit_none_gets_short_policy(self, mock_s3):
        """``retry_config=None`` on a read-only store means the read-only
        default (mirrors ``test_explicit_none_gets_default`` on the write
        path — the guard is ``is None``, not key-absence)."""
        from zagg.store import _S3_READONLY_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store("s3://bucket/prefix.zarr", read_only=True, retry_config=None)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == _S3_READONLY_RETRY_CONFIG

    def test_read_only_override_wins(self, mock_s3):
        s3_cls, _ = mock_s3
        custom = {"max_retries": 7}
        open_store("s3://bucket/prefix.zarr", read_only=True, retry_config=custom)
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] == custom

    def test_anonymous_read_skips_credential_provider(self, mock_s3):
        """``skip_signature=True`` (public bucket, e.g. the example notebooks
        on binder) must not construct ``Boto3CredentialProvider`` — it raises
        without ambient AWS credentials, which anonymous environments lack."""
        s3_cls, prov_cls = mock_s3
        open_store("s3://bucket/public.zarr", read_only=True, skip_signature=True)
        _, kwargs = s3_cls.call_args
        assert kwargs["skip_signature"] is True
        assert "credential_provider" not in kwargs
        prov_cls.assert_not_called()
        # The anonymous branch still inherits the read-only retry policy.
        from zagg.store import _S3_READONLY_RETRY_CONFIG

        assert kwargs["retry_config"] == _S3_READONLY_RETRY_CONFIG

    def test_skip_signature_with_credentials_takes_credentialed_branch(self, mock_s3):
        """Explicit credentials win over ``skip_signature``: the credentialed
        branch is taken (signed client config), ``skip_signature`` rides
        through kwargs for obstore to honor."""
        s3_cls, prov_cls = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret"}
        open_store("s3://bucket/x.zarr", credentials=creds, skip_signature=True)
        _, kwargs = s3_cls.call_args
        assert kwargs["access_key_id"] == "AKIA"
        assert kwargs["skip_signature"] is True
        prov_cls.assert_not_called()

    def test_default_not_aliased(self, mock_s3):
        """Each store gets its own copy — mutating one store's config must
        not edit the module-level default (nested ``backoff`` included)."""
        from zagg.store import _S3_RETRY_CONFIG

        s3_cls, _ = mock_s3
        open_store("s3://bucket/prefix.zarr")
        _, kwargs = s3_cls.call_args
        assert kwargs["retry_config"] is not _S3_RETRY_CONFIG
        assert kwargs["retry_config"]["backoff"] is not _S3_RETRY_CONFIG["backoff"]

    def test_default_paces_retries(self, tmp_path):
        """End-to-end through ``open_object_store`` against a local always-503
        endpoint: retries are paced by real exponential backoff (a small
        override keeps the test fast; the default values are pinned above).
        With obstore's default policy the same request burns 10 retries with
        sub-second gaps."""
        import http.server
        import threading
        import time
        from datetime import timedelta

        import obstore

        times: list[float] = []

        class SlowDown(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                times.append(time.monotonic())
                body = (
                    b"<Error><Code>SlowDown</Code>"
                    b"<Message>Please reduce your request rate.</Message></Error>"
                )
                self.send_response(503)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SlowDown)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            store = open_object_store(
                "s3://bkt/prefix",
                credentials={"accessKeyId": "x", "secretAccessKey": "y"},
                endpoint_url=f"http://127.0.0.1:{srv.server_address[1]}",
                client_options={"allow_http": True},
                retry_config={
                    "max_retries": 2,
                    "retry_timeout": timedelta(seconds=30),
                    "backoff": {
                        "init_backoff": timedelta(milliseconds=400),
                        "max_backoff": timedelta(seconds=2),
                        "base": 2,
                    },
                },
            )
            with pytest.raises(Exception, match="SlowDown|503|Server"):
                obstore.put(store, "zarr.json", b"{}")
        finally:
            srv.shutdown()
            srv.server_close()
        # 1 initial request + exactly max_retries retries: the override reached
        # the client, which is the plumbing under test (backoff jitter is
        # uniform from zero, so per-gap timing floors would flake).
        assert len(times) == 3
        assert times[-1] - times[0] < 10


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


class TestObjectStoreCache:
    """Ambient ``s3://`` stores are memoized per process (issue #287): the
    sidecar manifest fetch calls ``open_object_store(self.store)`` once per
    granule, and each call previously rebuilt a ``Boto3CredentialProvider``
    (~300 ms credential-chain walk) on the read hot path."""

    def test_ambient_store_built_once_and_reused(self, mock_s3):
        s3_cls, prov_cls = mock_s3
        p = "s3://sliderule-public-cors/zagg-index/ATL03/007"
        stores = [open_object_store(p) for _ in range(5)]
        # One construction total; the credential chain is walked once, not 5x.
        assert s3_cls.call_count == 1
        assert prov_cls.call_count == 1
        # Every caller gets the same shared store.
        assert all(s is stores[0] for s in stores)

    def test_distinct_paths_get_distinct_stores(self, mock_s3):
        s3_cls, _ = mock_s3
        open_object_store("s3://bucket/a")
        open_object_store("s3://bucket/b")
        assert s3_cls.call_count == 2

    def test_explicit_credentials_bypass_cache(self, mock_s3):
        s3_cls, _ = mock_s3
        creds = {"accessKeyId": "AKIA", "secretAccessKey": "secret"}
        open_object_store("s3://bucket/a", credentials=creds)
        open_object_store("s3://bucket/a", credentials=creds)
        # A statically-supplied token must never be cached (it would freeze on a
        # warm worker) -- each call builds fresh. Two S3Store constructions per
        # call: explicit credentials against the AWS endpoint are an external
        # write target, so each store comes with its ACL twin (issue #522).
        assert s3_cls.call_count == 4

    def test_endpoint_bypasses_cache(self, mock_s3):
        s3_cls, _ = mock_s3
        open_object_store("s3://bucket/a", endpoint_url="https://minio.local")
        open_object_store("s3://bucket/a", endpoint_url="https://minio.local")
        assert s3_cls.call_count == 2

    def test_kwargs_bypass_cache(self, mock_s3):
        """A caller passing any kwargs (e.g. the read-only retry config on the
        temporal path) falls through to a fresh build -- the cache is scoped to
        the sidecar's bare ``open_object_store(path)`` call only."""
        s3_cls, _ = mock_s3
        from zagg.store import _S3_READONLY_RETRY_CONFIG

        open_object_store("s3://bucket/a", retry_config=_S3_READONLY_RETRY_CONFIG)
        open_object_store("s3://bucket/a", retry_config=_S3_READONLY_RETRY_CONFIG)
        assert s3_cls.call_count == 2

    def test_local_paths_not_cached(self, tmp_path):
        s1 = open_object_store(str(tmp_path / "s"))
        s2 = open_object_store(str(tmp_path / "s"))
        # Local stores are cheap and unaffected -- fresh each call, not cached.
        assert s1 is not s2


class TestParseS3Path:
    def test_bucket_and_prefix(self):
        assert parse_s3_path("s3://mybucket/some/prefix.zarr") == ("mybucket", "some/prefix.zarr")

    def test_bucket_only(self):
        assert parse_s3_path("s3://mybucket") == ("mybucket", "")

    def test_not_s3_raises(self):
        with pytest.raises(ValueError, match="Not an S3 path"):
            parse_s3_path("./local/path.zarr")
