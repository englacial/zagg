"""Lifecycle touch for skip-if-current units — issue #388 phase 3.

Pins ``zagg.lifecycle``: the unit-footprint assembly (leaf tree + stats
sidecar + sub-map siblings + declared column tree/sidecar), the local
``os.utime`` mechanism, the S3 self-copy (``CopyObject`` onto itself with
``MetadataDirective="REPLACE"``) against a mocked client, and the fail-open
counting contract (a failed or absent object never raises, never un-skips).
"""

import os

import pytest
from botocore.exceptions import ClientError

from zagg import lifecycle

LEAF = "-5112333.zarr"


@pytest.fixture(autouse=True)
def _fresh_client_cache(monkeypatch):
    """``lifecycle._CLIENTS`` is process-wide by design (one client per store
    kwargs, not per unit); give each test its own so a patched ``_s3_client``
    cannot leak a mock into the next one."""
    monkeypatch.setattr(lifecycle, "_CLIENTS", {})


def _make_unit(tmp_path, *, submap=True, column=False, granule_ids=True):
    """A committed-looking local unit: leaf tree + sidecar siblings."""
    node = tmp_path / "store" / "-5" / "1"
    leaf = node / LEAF
    (leaf / "m" / "c").mkdir(parents=True)
    (leaf / "zarr.json").write_bytes(b"{}")
    (leaf / "m" / "c" / "0").write_bytes(b"\x00\x01")
    (leaf / "coverage.moc").write_bytes(b"moc")
    (node / "stats.json").write_bytes(b"{}")
    if granule_ids:
        (node / "granules.json").write_bytes(b"{}")
    if submap:
        (node / "shardmap.json").write_bytes(b"{}")
    column_path = None
    if column:
        column_path = node / "all.pyramid.zarr"
        (column_path / "h").mkdir(parents=True)
        (column_path / "zarr.json").write_bytes(b"{}")
        (column_path / "h" / "c").write_bytes(b"\x02")
        (node / "all.pyramid.stats.json").write_bytes(b"{}")
    return node, leaf, column_path


def _age(root, epoch=10_000):
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            os.utime(os.path.join(dirpath, name), (epoch, epoch))
    return epoch * 10**9


def _mtimes(root):
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            p = os.path.join(dirpath, name)
            out[os.path.relpath(p, root)] = os.stat(p).st_mtime_ns
    return out


class TestTouchLocal:
    def test_touches_the_whole_unit_footprint(self, tmp_path):
        node, leaf, _column = _make_unit(tmp_path)
        aged = _age(node)
        counts = lifecycle.touch_current_unit(str(leaf))
        after = _mtimes(node)
        # Leaf tree (coverage.moc included) + stats.json + granules.json +
        # shardmap.json.
        assert counts == {"touched": 6, "failed": 0}
        assert all(mtime > aged for mtime in after.values())

    def test_column_tree_and_sidecar_ride_the_footprint(self, tmp_path):
        node, leaf, column = _make_unit(tmp_path, column=True)
        aged = _age(node)
        counts = lifecycle.touch_current_unit(str(leaf), column_path=str(column))
        after = _mtimes(node)
        # + the column's two objects and its own stats sidecar.
        assert counts == {"touched": 9, "failed": 0}
        assert all(mtime > aged for mtime in after.values())

    def test_absent_column_is_left_out(self, tmp_path):
        # column_path=None (nothing declared): the column family is not part
        # of the footprint — nothing to touch, nothing to fail.
        node, leaf, _column = _make_unit(tmp_path)
        counts = lifecycle.touch_current_unit(str(leaf), column_path=None)
        assert counts["failed"] == 0 and counts["touched"] == 6

    def test_missing_submap_is_neither_touched_nor_failed(self, tmp_path):
        # A unit legitimately has no sub-map (non-HEALPix, id-less entries).
        _node, leaf, _column = _make_unit(tmp_path, submap=False)
        counts = lifecycle.touch_current_unit(str(leaf))
        assert counts == {"touched": 5, "failed": 0}

    def test_missing_granule_ids_sibling_is_neither_touched_nor_failed(self, tmp_path):
        # A leaf written before issue #388 (or one whose fail-open sibling PUT
        # was lost) has no granules.json: absent, not a failure.
        _node, leaf, _column = _make_unit(tmp_path, granule_ids=False)
        counts = lifecycle.touch_current_unit(str(leaf))
        assert counts == {"touched": 5, "failed": 0}

    def test_a_failing_utime_counts_and_never_raises(self, tmp_path, monkeypatch):
        node, leaf, _column = _make_unit(tmp_path)
        real = os.utime
        victim = str(leaf / "zarr.json")

        def flaky(path, *a, **k):
            if str(path) == victim:
                raise OSError("EACCES")
            return real(path, *a, **k)

        monkeypatch.setattr(lifecycle.os, "utime", flaky)
        counts = lifecycle.touch_current_unit(str(leaf))
        assert counts == {"touched": 5, "failed": 1}

    def test_footprint_assembly_failure_is_fail_open(self):
        # A leaf name the spec-keyed naming seam refuses (not a .zarr name)
        # cannot raise out of the touch: one counted failure, nothing touched.
        counts = lifecycle.touch_current_unit("/nowhere/not-a-leaf")
        assert counts == {"touched": 0, "failed": 1}

    def test_the_footprint_does_not_spill_onto_node_neighbours(self, tmp_path):
        # The OVER-touch direction (review finding): every other pin here is
        # "all of the walked root moved", which cannot catch a footprint that
        # is too WIDE. A hive node holds every window of a shard side by side
        # plus their sidecars, so touching unit 2019 must leave 2020 alone —
        # the property the tree walk's trailing slash carries.
        node = tmp_path / "store" / "-5" / "1"
        leaf = node / "123_2019.zarr"
        (leaf / "m").mkdir(parents=True)
        (leaf / "zarr.json").write_bytes(b"{}")
        (node / "stats_2019.json").write_bytes(b"{}")
        outsiders = [
            node / "123_2020.zarr" / "zarr.json",  # the sibling window's leaf
            node / "stats_2020.json",  # ...and its sidecar
            node / "granules_2020.json",  # ...and its recorded id list
            node / "shardmap_2020.json",  # ...and its sub-map
            # A key that is a STRING prefix extension of the leaf's: matched
            # by a prefix-less LIST, excluded by the trailing slash.
            node / "123_2019.zarr.bak",
        ]
        for path in outsiders:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x")
        aged = _age(node)

        counts = lifecycle.touch_current_unit(str(leaf), sidecar_spec=None)
        after = _mtimes(node)
        assert counts == {"touched": 2, "failed": 0}  # leaf tree + its sidecar
        moved = {name for name, mtime in after.items() if mtime > aged}
        assert moved == {os.path.join("123_2019.zarr", "zarr.json"), "stats_2019.json"}


class TestTouchStoreRoot:
    """The root objects no unit footprint covers (review finding, phase 3)."""

    def _root(self, tmp_path, *, moc=True):
        from zagg.hive import AGGREGATION_CORE_NAME, MANIFEST_NAME, ROOT_COVERAGE_NAME

        root = tmp_path / "store"
        root.mkdir()
        (root / MANIFEST_NAME).write_bytes(b"{}")
        (root / AGGREGATION_CORE_NAME).write_bytes(b"a: 1\n")
        if moc:
            (root / ROOT_COVERAGE_NAME).write_bytes(b"{}")
        return root

    def test_touches_the_manifest_semantic_core_and_root_moc(self, tmp_path):
        # ensure_manifest early-returns on a frozen-key match and every
        # skip-capable run is overwrite=False, so nothing else moves these.
        root = self._root(tmp_path)
        aged = _age(root)
        counts = lifecycle.touch_store_root(str(root))
        assert counts == {"touched": 3, "failed": 0}
        assert all(mtime > aged for mtime in _mtimes(root).values())

    def test_an_absent_root_moc_is_neither_touched_nor_failed(self, tmp_path):
        # output.coverage_moc off: the root MOC was never written.
        root = self._root(tmp_path, moc=False)
        assert lifecycle.touch_store_root(str(root)) == {"touched": 2, "failed": 0}

    def test_a_missing_store_root_is_fail_open(self, tmp_path):
        assert lifecycle.touch_store_root(str(tmp_path / "nope")) == {"touched": 0, "failed": 0}

    def test_s3_root_objects_are_exact_keys(self, monkeypatch):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.head_object.return_value = {}
        monkeypatch.setattr(lifecycle, "_s3_client", lambda kw: client)
        counts = lifecycle.touch_store_root("s3://bkt/store/")
        assert {c.kwargs["Key"] for c in client.copy_object.call_args_list} == {
            "store/morton_hive.json",
            "store/aggregation.yaml",
            "store/coverage.moc",
        }
        # No LIST: the root is three named objects, not a tree walk (a walk
        # would re-touch every leaf in the store).
        client.get_paginator.assert_not_called()
        assert counts == {"touched": 3, "failed": 0}


class TestTouchS3:
    BUCKET = "bkt"
    TREE_KEYS = [
        f"store/-5/1/{LEAF}/zarr.json",
        f"store/-5/1/{LEAF}/m/c/0",
        f"store/-5/1/{LEAF}/coverage.moc",
    ]

    def _client(self, monkeypatch, *, copy_error=None, listed=None, head=None):
        from unittest.mock import MagicMock

        client = MagicMock()
        paginator = MagicMock()
        contents = listed if listed is not None else [{"Key": k} for k in self.TREE_KEYS]
        paginator.paginate.return_value = [{"Contents": contents}]
        client.get_paginator.return_value = paginator
        # S3 omits StorageClass for STANDARD in both LIST and HEAD.
        client.head_object.return_value = head if head is not None else {}
        if copy_error is not None:
            client.copy_object.side_effect = copy_error
        monkeypatch.setattr(lifecycle, "_s3_client", lambda kw: client)
        return client

    def test_self_copy_with_metadata_replace_over_the_footprint(self, monkeypatch):
        client = self._client(monkeypatch)
        counts = lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={"region": "us-west-2", "credentials": None, "endpoint_url": None},
        )
        # The tree is enumerated with one delimiter-less LIST under the leaf
        # prefix; the siblings are direct copies.
        client.get_paginator.assert_called_once_with("list_objects_v2")
        assert client.get_paginator.return_value.paginate.call_args.kwargs == {
            "Bucket": self.BUCKET,
            "Prefix": f"store/-5/1/{LEAF}/",
        }
        calls = client.copy_object.call_args_list
        assert {c.kwargs["Key"] for c in calls} == set(self.TREE_KEYS) | {
            "store/-5/1/stats.json",
            "store/-5/1/granules.json",
            "store/-5/1/icechunk_refs.json",
            "store/-5/1/shardmap.json",
        }
        for c in calls:
            # The identity self-copy: S3 rejects it without REPLACE.
            assert c.kwargs["CopySource"] == {"Bucket": self.BUCKET, "Key": c.kwargs["Key"]}
            assert c.kwargs["MetadataDirective"] == "REPLACE"
            # LIST/HEAD omit the class for STANDARD; the copy must still name
            # one, or REPLACE resets whatever the object had.
            assert c.kwargs["StorageClass"] == "STANDARD"
        assert counts == {"touched": 7, "failed": 0}

    def test_the_storage_class_is_preserved_not_reset_to_standard(self, monkeypatch):
        # MetadataDirective=REPLACE covers SYSTEM metadata, so a copy with no
        # StorageClass silently promotes a STANDARD_IA/GLACIER_IR object back
        # to STANDARD — defeating a lifecycle TRANSITION policy and re-paying
        # the transition on every skip run (review finding).
        client = self._client(
            monkeypatch,
            listed=[
                {"Key": self.TREE_KEYS[0], "StorageClass": "GLACIER_IR"},
                {"Key": self.TREE_KEYS[1], "StorageClass": "STANDARD_IA"},
            ],
            head={"StorageClass": "STANDARD_IA"},
        )
        counts = lifecycle.touch_current_unit(f"s3://{self.BUCKET}/store/-5/1/{LEAF}")
        classes = {
            c.kwargs["Key"]: c.kwargs["StorageClass"] for c in client.copy_object.call_args_list
        }
        assert classes == {
            self.TREE_KEYS[0]: "GLACIER_IR",
            self.TREE_KEYS[1]: "STANDARD_IA",
            # The siblings have no LIST entry: one HEAD each buys the class.
            "store/-5/1/stats.json": "STANDARD_IA",
            "store/-5/1/granules.json": "STANDARD_IA",
            "store/-5/1/icechunk_refs.json": "STANDARD_IA",
            "store/-5/1/shardmap.json": "STANDARD_IA",
        }
        assert client.head_object.call_count == 4  # named objects only
        assert counts == {"touched": 6, "failed": 0}

    def test_external_target_copies_carry_the_bucket_owner_acl(self, monkeypatch):
        # CopyObject CREATES the object, so on a cross-account target a touch
        # without x-amz-acl re-creates it owned by US, stripping the ownership
        # the writing PUT handed over (issue #495 review finding). Same
        # predicate and value as the store seam, which the touch bypasses.
        from zagg.store import _BUCKET_OWNER_ACL

        client = self._client(monkeypatch)
        lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={
                "region": "us-west-2",
                "credentials": {"accessKeyId": "ASIA", "secretAccessKey": "s"},
                "endpoint_url": None,
            },
        )
        calls = client.copy_object.call_args_list
        assert calls
        assert {c.kwargs["ACL"] for c in calls} == {_BUCKET_OWNER_ACL}

    def test_published_target_is_not_touched_at_all(self, monkeypatch):
        # Issue #495 phase 4. The touch exists to defeat an expiration rule;
        # an archival published bucket has none, and it IS versioned, so a
        # self-copy would not refresh a timestamp in place -- it writes a new
        # full-size version and demotes the old one to noncurrent, where it
        # keeps consuming storage on a bucket AWS pays for as an Open Data
        # sponsor. One full-skip run over the CA store would add ~332 GB that
        # ListObjectsV2 does not even show. So: no LIST, no HEAD, no copy.
        client = self._client(monkeypatch)
        counts = lifecycle.touch_current_unit(
            f"s3://us-west-2.opendata.source.coop/englacial/zagg/demo/store/-5/1/{LEAF}",
            store_kwargs={"region": "us-west-2", "credentials": None, "endpoint_url": None},
        )
        assert client.copy_object.call_args_list == []
        assert client.head_object.call_args_list == []
        assert client.get_paginator.call_args_list == []
        # Not applicable, NOT failed: a published run must not read as an error
        # in the run parquet or the status objects.
        assert counts["failed"] == 0
        assert counts["touched"] == 0
        # PATHS, not objects (review finding on PR #496): the four inputs are
        # the leaf tree plus the three named siblings. A successful touch of
        # the same footprint reports touched: 7, because the tree contributes
        # one per listed key -- the keys are not summable, hence the name.
        assert counts["skipped_paths"] == 5

    PUBLISHED = "us-west-2.opendata.source.coop"

    def _leaf(self, bucket):
        return f"s3://{bucket}/englacial/zagg/demo/store/-5/1/{LEAF}"

    def _kw(self):
        return {"region": "us-west-2", "credentials": None, "endpoint_url": None}

    def test_policy_auto_matches_the_inference_on_both_destinations(self, monkeypatch):
        # issue #501: `auto` IS issue #495 phase 4, verbatim -- the default, so
        # every config that predates the knob is byte-identical.
        client = self._client(monkeypatch)
        pub = lifecycle.touch_current_unit(
            self._leaf(self.PUBLISHED), store_kwargs=self._kw(), policy="auto"
        )
        assert client.copy_object.call_args_list == []
        assert pub["skipped_paths"] > 0 and pub["failed"] == 0

        # _CLIENTS is process-wide and the autouse reset runs once per test, so
        # a second install inside one test needs the cache cleared or the first
        # client is handed back and the new mock records nothing.
        lifecycle._CLIENTS.clear()
        client = self._client(monkeypatch)
        ours = lifecycle.touch_current_unit(
            self._leaf(self.BUCKET), store_kwargs=self._kw(), policy="auto"
        )
        assert client.copy_object.call_args_list
        assert ours["touched"] > 0 and "skipped_paths" not in ours

    def test_policy_always_touches_the_published_destination_too(self, monkeypatch):
        # The override the inference cannot express: an un-negotiated external
        # target whose expiry rule we know about but whose name says nothing.
        # It must reach the COPY, not just the call-site guard -- the seam
        # carries a second check, and a policy-blind seam would silently make
        # `always` a no-op on exactly the destination it was set for.
        from zagg.store import _BUCKET_OWNER_ACL

        client = self._client(monkeypatch)
        pub = lifecycle.touch_current_unit(
            self._leaf(self.PUBLISHED), store_kwargs=self._kw(), policy="always"
        )
        assert client.copy_object.call_args_list, "always must reach the copy seam"
        assert pub["touched"] > 0 and "skipped_paths" not in pub
        # The copy must CARRY the bucket-owner ACL. `always` is the only route
        # that reaches _copy_acl with a published bucket, and under the ambient
        # execution role the bucket arm of _external_target is the only arm that
        # can fire -- so this pins the `bucket` argument that keeps an `always`
        # run from silently stripping ownership on every object it self-copies
        # to Source Cooperative (review finding on PR #496).
        assert {c.kwargs["ACL"] for c in client.copy_object.call_args_list} == {_BUCKET_OWNER_ACL}

        # _CLIENTS is process-wide and the autouse reset runs once per test, so
        # a second install inside one test needs the cache cleared or the first
        # client is handed back and the new mock records nothing.
        lifecycle._CLIENTS.clear()
        client = self._client(monkeypatch)
        ours = lifecycle.touch_current_unit(
            self._leaf(self.BUCKET), store_kwargs=self._kw(), policy="always"
        )
        assert client.copy_object.call_args_list
        assert ours["touched"] > 0

    def test_policy_never_touches_neither_destination(self, monkeypatch):
        client = self._client(monkeypatch)
        pub = lifecycle.touch_current_unit(
            self._leaf(self.PUBLISHED), store_kwargs=self._kw(), policy="never"
        )
        assert client.copy_object.call_args_list == []
        assert pub["skipped_paths"] > 0 and pub["failed"] == 0

        # _CLIENTS is process-wide and the autouse reset runs once per test, so
        # a second install inside one test needs the cache cleared or the first
        # client is handed back and the new mock records nothing.
        lifecycle._CLIENTS.clear()
        client = self._client(monkeypatch)
        ours = lifecycle.touch_current_unit(
            self._leaf(self.BUCKET), store_kwargs=self._kw(), policy="never"
        )
        assert client.copy_object.call_args_list == []
        assert ours["skipped_paths"] > 0 and ours["failed"] == 0 and ours["touched"] == 0

    def test_policy_never_covers_local_paths_too(self, tmp_path):
        # "never touch" is a statement about the RUN, not about S3. A local
        # store under `never` must not have its mtimes rewritten either.
        root = tmp_path / "store" / "-5" / "1" / LEAF
        root.mkdir(parents=True)
        target = root / "zarr.json"
        target.write_text("{}")
        os.utime(target, (1, 1))
        counts = lifecycle.touch_current_unit(str(root), policy="never")
        assert target.stat().st_mtime == 1
        assert counts["touched"] == 0 and counts["failed"] == 0
        assert counts["skipped_paths"] > 0

    def test_not_applicable_log_names_the_policy_not_the_inference(self, tmp_path, caplog):
        # The line an operator reads at 3 a.m. A policy skip is not a
        # publication skip: under `never` on a LOCAL store there is no bucket at
        # all, and the old wording reported an empty published-bucket list and
        # pointed at a predicate that was never consulted (review finding on
        # PR #496).
        import logging

        root = tmp_path / "store" / "-5" / "1" / LEAF
        root.mkdir(parents=True)
        (root / "zarr.json").write_text("{}")
        with caplog.at_level(logging.INFO, logger="zagg.lifecycle"):
            lifecycle.touch_current_unit(str(root), policy="never")
        line = next(r.message for r in caplog.records if "not applicable" in r.message)
        assert "policy='never'" in line
        assert "bucket(s)" not in line  # no bucket to name on a local path
        assert "_touch_applies" in line

    def test_published_skip_is_keyed_on_the_bucket_not_the_credentials(self, monkeypatch):
        # The guard is `bucket in _PUBLISHED_BUCKETS`, never
        # `_external_target(...)`. The two differ, and the difference is
        # load-bearing: _external_target is also true for injected-credential
        # targets, whose lifecycle and versioning config we do not know.
        # Skipping those could let a collaborator's data EXPIRE -- the exact
        # failure the touch exists to prevent. Only the published set is known
        # not to expire.
        from zagg.store import _external_target

        creds = {"accessKeyId": "ASIA", "secretAccessKey": "s"}
        assert _external_target(creds, None, self.BUCKET), "premise: external but not published"
        client = self._client(monkeypatch)
        counts = lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={"region": "us-west-2", "credentials": creds, "endpoint_url": None},
        )
        assert client.copy_object.call_args_list, "an un-negotiated target must still be touched"
        assert counts["touched"] > 0
        assert "skipped_paths" not in counts

    def test_the_copy_seam_itself_refuses_a_published_bucket(self, monkeypatch):
        # Belt and braces (review finding on PR #496): the counting guard is
        # in touch_unit_footprint, but the invariant belongs to the one
        # function that issues CopyObject, so no future entry point can
        # bypass it. Unreachable through the public surface today -- this
        # calls the private seam directly, which is the point.
        client = self._client(monkeypatch)
        counts = {"touched": 0, "failed": 0}
        lifecycle._touch_s3_object(
            client, "us-west-2.opendata.source.coop", "englacial/zagg/demo/store/x.json", counts
        )
        assert client.copy_object.call_args_list == []
        assert client.head_object.call_args_list == []
        # Counts NOTHING: the path-based accounting lives at the call site,
        # and counting here as well would double it.
        assert counts == {"touched": 0, "failed": 0}

    def test_in_account_copies_carry_no_acl(self, monkeypatch):
        # Ambient execution-role touch of our own bucket: nothing to hand over,
        # and an ACL would be a change the touch has no business making.
        client = self._client(monkeypatch)
        lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={"region": "us-west-2", "credentials": None, "endpoint_url": None},
        )
        calls = client.copy_object.call_args_list
        assert calls
        assert all("ACL" not in c.kwargs for c in calls)

    def test_custom_endpoint_copies_carry_no_acl(self, monkeypatch):
        # Excluded exactly as in the store seam: canned ACLs are an AWS-S3
        # concept the stores behind endpoint_url do not implement.
        client = self._client(monkeypatch)
        lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={
                "credentials": {"accessKeyId": "ASIA", "secretAccessKey": "s"},
                "endpoint_url": "https://acct.r2.cloudflarestorage.com",
            },
        )
        assert all("ACL" not in c.kwargs for c in client.copy_object.call_args_list)

    def test_a_failing_copy_on_an_external_target_stays_fail_open(self, monkeypatch):
        # The ACL rides a best-effort request: a rejection still only counts.
        err = ClientError({"Error": {"Code": "AccessDenied"}}, "CopyObject")
        self._client(monkeypatch, copy_error=err)
        counts = lifecycle.touch_current_unit(
            f"s3://{self.BUCKET}/store/-5/1/{LEAF}",
            store_kwargs={"credentials": {"accessKeyId": "ASIA", "secretAccessKey": "s"}},
        )
        assert counts == {"touched": 0, "failed": 7}

    def test_absent_sibling_is_neither_touched_nor_failed(self, monkeypatch):
        # copy of a missing key (e.g. no sub-map was ever written) NoSuchKey-s;
        # absence is not a failure.
        err = ClientError({"Error": {"Code": "NoSuchKey"}}, "CopyObject")
        self._client(monkeypatch, copy_error=err)
        counts = lifecycle.touch_current_unit(f"s3://{self.BUCKET}/store/-5/1/{LEAF}")
        assert counts == {"touched": 0, "failed": 0}

    def test_a_failing_copy_counts_and_never_raises(self, monkeypatch):
        err = ClientError({"Error": {"Code": "AccessDenied"}}, "CopyObject")
        self._client(monkeypatch, copy_error=err)
        counts = lifecycle.touch_current_unit(f"s3://{self.BUCKET}/store/-5/1/{LEAF}")
        assert counts == {"touched": 0, "failed": 7}

    def test_a_list_fault_aborts_fail_open(self, monkeypatch):
        client = self._client(monkeypatch)
        client.get_paginator.return_value.paginate.side_effect = RuntimeError("boom")
        counts = lifecycle.touch_current_unit(f"s3://{self.BUCKET}/store/-5/1/{LEAF}")
        assert counts["failed"] == 1 and counts["touched"] == 0

    KWARGS = {
        "region": "eu-west-1",
        "endpoint_url": "https://r2.example",
        "credentials": {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "ST"},
    }

    def _session_spy(self, monkeypatch):
        """Patch ``boto3.session.Session`` and record every client built."""
        from unittest.mock import MagicMock

        built = []

        class FakeSession:
            def client(self, service, **kwargs):
                built.append({"service": service, **kwargs})
                client = MagicMock()
                client.get_paginator.return_value.paginate.return_value = []
                client.head_object.return_value = {}
                return client

        monkeypatch.setattr("boto3.session.Session", FakeSession)
        return built

    def test_client_is_keyed_by_the_store_kwargs(self, monkeypatch):
        # The store kwargs reach boto3 in their translated (snake_case) form,
        # and they are the cache key: same kwargs -> the SAME client across
        # units, different kwargs -> a second one.
        built = self._session_spy(monkeypatch)
        leaf = f"s3://{self.BUCKET}/store/-5/1/{LEAF}"
        lifecycle.touch_current_unit(leaf, store_kwargs=dict(self.KWARGS))
        assert built == [
            {
                "service": "s3",
                "region_name": "eu-west-1",
                "endpoint_url": "https://r2.example",
                "aws_access_key_id": "AK",
                "aws_secret_access_key": "SK",
                "aws_session_token": "ST",
            }
        ]
        # A second unit under the same run: no second construction. (A
        # per-call cache meant one ~0.1-0.3 s client build per unit — minutes
        # of it on a few-thousand-shard all-skip rerun.)
        lifecycle.touch_current_unit(leaf, store_kwargs=dict(self.KWARGS))
        assert len(built) == 1
        lifecycle.touch_current_unit(leaf, store_kwargs={"region": "us-west-2"})
        assert len(built) == 2 and built[1] == {"service": "s3", "region_name": "us-west-2"}

    def test_concurrent_units_share_one_client(self, monkeypatch):
        # The local dispatcher runs units in a ThreadPoolExecutor; building a
        # client per thread off the shared default session is the documented
        # botocore hazard, and fail-open would swallow it into silently
        # unrefreshed objects. One client, built under the lock, shared.
        from concurrent.futures import ThreadPoolExecutor

        built = self._session_spy(monkeypatch)
        leaf = f"s3://{self.BUCKET}/store/-5/1/{LEAF}"
        with ThreadPoolExecutor(max_workers=8) as pool:
            counts = list(
                pool.map(
                    lambda _i: lifecycle.touch_current_unit(leaf, store_kwargs=dict(self.KWARGS)),
                    range(16),
                )
            )
        assert len(built) == 1
        assert all(c["failed"] == 0 for c in counts)

    def test_local_unit_builds_no_client(self, tmp_path, monkeypatch):
        # A local store must never construct a boto3 client (no AWS reach).
        def explode(kw):
            raise AssertionError("S3 client built for a local store")

        monkeypatch.setattr(lifecycle, "_s3_client", explode)
        _node, leaf, _column = _make_unit(tmp_path)
        counts = lifecycle.touch_current_unit(str(leaf))
        assert counts["failed"] == 0


@pytest.mark.parametrize(
    "path,expected",
    [
        ("s3://b/k/x.zarr", ("b", "k/x.zarr")),
        ("s3://bucket-only", ("bucket-only", "")),
    ],
)
def test_split_s3(path, expected):
    assert lifecycle._split_s3(path) == expected
