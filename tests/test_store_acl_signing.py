"""Signing physics of the issue #495 canned ACL (issue #522).

The production blocker: the ``x-amz-acl: bucket-owner-full-control`` header
that issue #495 attached to published-bucket store handles rides obstore's
``client_options.default_headers``, and obstore puts default headers into the
SigV4 ``SignedHeaders`` set on object-CREATING requests only. On GET/LIST the
header is present but unsigned, and S3 rejects any request carrying an unsigned
``x-amz-*`` header outright::

    403 AccessDenied: There were headers present in the request which were not
    signed <HeadersNotSigned>x-amz-acl</HeadersNotSigned>

So a handle that both carries the ACL and reads -- every fleet worker's store
and the client status poller's -- 403s on its first LIST.

These tests capture what obstore actually puts on the wire, against a local
in-memory S3 stand-in, so the asymmetry is pinned deterministically and offline
(no AWS, no network). The live half of the harness -- the same treatment
against real S3, which is what turns "unsigned" into a 403 -- lives in
``tests/test_store_acl_live.py``.
"""

import re
import threading
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

_S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

ACL = "bucket-owner-full-control"
ACL_HEADERS = {"x-amz-acl": ACL}


class _Recorded:
    """One captured request: verb, path, headers, and the signed-header set."""

    def __init__(self, method: str, path: str, headers: dict):
        self.method = method
        self.path = path
        self.headers = {k.lower(): v for k, v in headers.items()}

    @property
    def signed_headers(self) -> frozenset[str]:
        match = re.search(r"SignedHeaders=([^,\s]+)", self.headers.get("authorization", ""))
        return frozenset(match.group(1).split(";")) if match else frozenset()

    @property
    def query(self) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.path).query, keep_blank_values=True)

    @property
    def acl(self) -> str | None:
        return self.headers.get("x-amz-acl")

    @property
    def acl_signed(self) -> bool:
        return "x-amz-acl" in self.signed_headers

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{self.method} {self.path} acl={self.acl!r} signed={self.acl_signed}>"


class FakeS3:
    """A path-style S3 stand-in that records every request it serves.

    It models exactly what the tests in this file drive through BARE obstore
    (no zarr in the loop): single PUT, the multipart flow
    (CreateMultipartUpload / UploadPart / CompleteMultipartUpload), GET, HEAD,
    ListObjectsV2 (flat, and with a delimiter for the digit-tree listing
    ``tests/test_store_acl_seam.py`` drives), and the bucket-level bulk delete.
    Anything else gets a fast 501 rather than the stdlib error page obstore
    retries ten times over.

    Storage fidelity is incidental -- the object under test is the WIRE, i.e.
    the header set each request carries, captured in :class:`_Recorded`.
    """

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.uploads: dict[str, dict[int, bytes]] = {}
        self._upload_seq = 0
        self.requests: list[_Recorded] = []
        server = self
        self._lock = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _record(self):
                with server._lock:
                    server.requests.append(_Recorded(self.command, self.path, dict(self.headers)))

            def _key(self) -> str:
                # Path-style: /{bucket}/{key...}
                return urlparse(self.path).path.lstrip("/").split("/", 1)[-1]

            def _send(self, code: int, body: bytes = b"", ctype: str = "application/xml"):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body and self.command != "HEAD":
                    self.wfile.write(body)

            def _not_found(self):
                self._send(404, b"<Error><Code>NoSuchKey</Code></Error>")

            def do_GET(self):
                self._record()
                query = parse_qs(urlparse(self.path).query)
                if query.get("list-type") == ["2"]:
                    return self._send(200, server._list_xml(query))
                body = server.objects.get(self._key())
                return (
                    self._send(200, body, "application/octet-stream")
                    if body is not None
                    else self._not_found()
                )

            def do_HEAD(self):
                self._record()
                body = server.objects.get(self._key())
                return self._send(200, body or b"") if body is not None else self._not_found()

            def do_PUT(self):
                length = int(self.headers.get("Content-Length") or 0)
                payload = self.rfile.read(length)
                self._record()
                query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
                if "uploadId" in query and "x-amz-acl" in self.headers:
                    # What real S3 answers, verbatim (issue #534). ``x-amz-acl``
                    # is legal on PutObject and CreateMultipartUpload and not on
                    # UploadPart, and the stand-in has to model the difference
                    # or the fleet's failure mode passes silently here.
                    return self._send(400, server._invalid_acl_argument_xml())
                with server._lock:
                    if "uploadId" in query:  # UploadPart
                        upload = server.uploads.setdefault(query["uploadId"][0], {})
                        upload[int(query["partNumber"][0])] = payload
                    else:
                        server.objects[self._key()] = payload
                self.send_response(200)
                self.send_header("ETag", '"fake"')
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self):
                # The three sub-resources obstore POSTs. Dispatching them here
                # (rather than letting BaseHTTPRequestHandler answer a stdlib
                # 501, which obstore retries ten times over ~4s) keeps the
                # stand-in fast and legible about what it does not model.
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                self._record()
                query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
                if "uploads" in query:  # CreateMultipartUpload
                    return self._send(200, server._initiate_upload_xml(self._key()))
                if "uploadId" in query:  # CompleteMultipartUpload
                    return self._send(
                        200, server._complete_upload_xml(self._key(), query["uploadId"][0])
                    )
                if "delete" in query:  # DeleteObjects (bucket-level bulk delete)
                    return self._send(200, server._bulk_delete_xml(body))
                return self._send(501, b"<Error><Code>NotImplemented</Code></Error>")

            def log_message(self, *args):  # silence the stdlib access log
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        self.endpoint = f"http://127.0.0.1:{self._httpd.server_address[1]}"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def _list_xml(self, query: dict) -> bytes:
        prefix = (query.get("prefix") or [""])[0]
        delimiter = (query.get("delimiter") or [""])[0]
        # Sizes are captured with the key set, under one lock: this is a
        # ThreadingHTTPServer with daemon_threads, so a concurrent PUT or bulk
        # delete between the two reads would be a KeyError inside a handler.
        with self._lock:
            sizes = {k: len(v) for k, v in self.objects.items() if k.startswith(prefix)}
        contents, common = [], set()
        for key in sorted(sizes):
            rest = key[len(prefix) :]
            if delimiter and delimiter in rest:
                common.add(prefix + rest.split(delimiter, 1)[0] + delimiter)
            else:
                contents.append(key)
        root = ET.Element("ListBucketResult", xmlns=_S3_XMLNS)
        ET.SubElement(root, "IsTruncated").text = "false"
        ET.SubElement(root, "KeyCount").text = str(len(contents) + len(common))
        for key in contents:
            node = ET.SubElement(root, "Contents")
            ET.SubElement(node, "Key").text = key
            ET.SubElement(node, "Size").text = str(sizes[key])
            ET.SubElement(node, "LastModified").text = "2026-08-25T00:00:00.000Z"
            ET.SubElement(node, "ETag").text = '"fake"'
        for pfx in sorted(common):
            ET.SubElement(ET.SubElement(root, "CommonPrefixes"), "Prefix").text = pfx
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def _invalid_acl_argument_xml(self) -> bytes:
        """The ``400`` S3 returns for ``x-amz-acl`` on ``UploadPart``."""
        root = ET.Element("Error")
        ET.SubElement(root, "Code").text = "InvalidArgument"
        ET.SubElement(root, "Message").text = "The specified header is not valid in this context"
        ET.SubElement(root, "ArgumentName").text = "x-amz-acl"
        ET.SubElement(root, "ArgumentValue").text = ACL
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def _initiate_upload_xml(self, key: str) -> bytes:
        """Serve ``POST /{bucket}/{key}?uploads`` -- CreateMultipartUpload.

        The ``xmlns`` is load-bearing: without it obstore rejects the body with
        ``missing field UploadId`` rather than reading the id back out.
        """
        with self._lock:
            self._upload_seq += 1
            upload_id = f"upload-{self._upload_seq}"
        root = ET.Element("InitiateMultipartUploadResult", xmlns=_S3_XMLNS)
        ET.SubElement(root, "Bucket").text = "bkt"
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "UploadId").text = upload_id
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def _complete_upload_xml(self, key: str, upload_id: str) -> bytes:
        """Serve ``POST /{bucket}/{key}?uploadId=`` -- CompleteMultipartUpload."""
        with self._lock:
            parts = self.uploads.pop(upload_id, {})
            self.objects[key] = b"".join(parts[n] for n in sorted(parts))
        root = ET.Element("CompleteMultipartUploadResult", xmlns=_S3_XMLNS)
        ET.SubElement(root, "Bucket").text = "bkt"
        ET.SubElement(root, "Key").text = key
        ET.SubElement(root, "ETag").text = '"fake"'
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def _bulk_delete_xml(self, body: bytes) -> bytes:
        """Serve ``POST /{bucket}?delete`` -- what ``obstore.delete`` sends."""
        # obstore namespaces the request body, so match on the local name.
        keys = [
            node.text or ""
            for node in ET.fromstring(body).iter()
            if node.tag.rpartition("}")[2] == "Key"
        ]
        with self._lock:
            for key in keys:
                self.objects.pop(key, None)
        root = ET.Element("DeleteResult", xmlns=_S3_XMLNS)
        for key in keys:
            ET.SubElement(ET.SubElement(root, "Deleted"), "Key").text = key
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def of(self, method: str) -> list[_Recorded]:
        return [r for r in self.requests if r.method == method]

    def clear(self):
        with self._lock:
            self.requests.clear()

    def close(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


@pytest.fixture
def fake_s3():
    server = FakeS3()
    try:
        yield server
    finally:
        server.close()


def _raw_store(fake_s3, **client_options):
    """A bare obstore ``S3Store`` against the stand-in -- no zagg in the loop."""
    from obstore.store import S3Store

    return S3Store(
        "bkt",
        prefix="p",
        region="us-west-2",
        endpoint=fake_s3.endpoint,
        virtual_hosted_style_request=False,
        access_key_id="AKIAEXAMPLE",
        secret_access_key="secret",
        client_options={"allow_http": True, **client_options},
    )


class TestObstoreDefaultHeaderSigning:
    """Why the #495 header cannot ride a handle that also reads.

    obstore/object_store applies ``default_headers`` at two different points:
    object-creating requests pick them up before SigV4 runs (so they land in
    ``SignedHeaders``), while GET/LIST pick them up on the reqwest client,
    after signing. This class pins both halves -- the fix in issue #522 is
    built on the asymmetry being real, and an obstore bump that changed it
    should fail loudly here rather than silently in a fleet run.
    """

    def test_acl_default_header_is_signed_on_put(self, fake_s3):
        import obstore

        obstore.put(_raw_store(fake_s3, default_headers=ACL_HEADERS), "k", b"hello")
        (put,) = fake_s3.of("PUT")
        assert put.acl == ACL
        assert put.acl_signed, f"expected x-amz-acl in SignedHeaders, got {put.signed_headers}"

    def test_a_multipart_write_carrying_the_acl_dies_on_uploadpart(self, fake_s3):
        """Issue #534, in one request pair.

        obstore signs the default header on ``CreateMultipartUpload`` -- the
        request that would actually apply the ACL -- and then sends it again on
        ``UploadPart``, where S3 refuses it: ``400 InvalidArgument``, "The
        specified header is not valid in this context",
        ``<ArgumentName>x-amz-acl</ArgumentName>``. Both halves are asserted
        here, because the fix depends on both: the signing is why a canned ACL
        can be carried at all, and the rejection is why it cannot be carried
        through a multipart upload.

        This is what 52 of 60 sampled shards hit on the CA GEDI build, and what
        a deliberate single-shard test hit at ``partNumber=1`` -- its store on
        source.coop is missing exactly its two ragged chunks, the only objects
        it wrote over obstore's 5 MiB multipart threshold.
        """
        import obstore

        store = _raw_store(fake_s3, default_headers=ACL_HEADERS)
        with pytest.raises(obstore.exceptions.BaseError, match="InvalidArgument"):
            obstore.put(store, "big", b"x" * (8 * 1024 * 1024))

        (create,) = [r for r in fake_s3.of("POST") if "uploads" in r.query]
        assert create.acl == ACL
        assert create.acl_signed, (
            f"expected x-amz-acl in SignedHeaders, got {create.signed_headers}"
        )
        parts = [r for r in fake_s3.of("PUT") if "partNumber" in r.query]
        assert parts, "no UploadPart went out -- the flow did not reach the rejection"
        assert all(part.acl == ACL for part in parts)
        assert "p/big" not in fake_s3.objects

    def test_the_same_payload_lands_as_a_single_put(self, fake_s3):
        # The other half of the fix's premise, one layer below zagg: the ACL is
        # legal on ``PutObject`` at any size, so declining multipart is all it
        # takes. Same bytes, same handle, same header -- only the framing
        # differs.
        import obstore

        store = _raw_store(fake_s3, default_headers=ACL_HEADERS)
        payload = b"x" * (8 * 1024 * 1024)
        obstore.put(store, "big", payload, use_multipart=False)

        (put,) = fake_s3.of("PUT")
        assert put.acl == ACL
        assert put.acl_signed
        assert not fake_s3.of("POST")
        assert fake_s3.objects["p/big"] == payload

    def test_acl_default_header_is_unsigned_on_list(self, fake_s3):
        import obstore

        list(obstore.list(_raw_store(fake_s3, default_headers=ACL_HEADERS)))
        (listing,) = fake_s3.of("GET")
        # The bug in one assertion: header on the wire, absent from the
        # signature. Real S3 answers this shape with
        # 403 AccessDenied / HeadersNotSigned: x-amz-acl.
        assert listing.acl == ACL
        # The LIST is otherwise a fully signed request -- assert that too, or
        # `not acl_signed` would also hold for an obstore that stopped signing
        # LIST altogether (`signed_headers` would just be empty), which is the
        # opposite failure and 403s in the fleet just the same.
        assert "x-amz-content-sha256" in listing.signed_headers
        assert not listing.acl_signed, f"unexpectedly signed: {listing.signed_headers}"

    def test_only_the_list_path_leaves_the_acl_unsigned(self, fake_s3):
        # Narrowing the asymmetry, because the fix depends on where the line
        # falls. Keyed GET and HEAD sign the default header, and so does the
        # delete -- which obstore sends not as a keyed DELETE but as a
        # BUCKET-level bulk `POST /{bucket}?delete` (DeleteObjects). So the
        # line is not "keyed signs, bucket-level does not": ListObjectsV2 is
        # the single request that misses. That is exactly the one the fleet
        # died on -- the per-leaf template guard's LIST of the digit tree, and
        # the status poller's `.status/` listing.
        import obstore

        store = _raw_store(fake_s3, default_headers=ACL_HEADERS)
        obstore.put(store, "k", b"hello")
        fake_s3.clear()
        obstore.get(store, "k").bytes()
        obstore.head(store, "k")
        list(obstore.list(store))
        obstore.delete(store, "k")

        # Select by unpacking rather than keying a dict: a dict keeps only the
        # last request per verb, so a retry or a stray extra HEAD would vanish
        # from a harness whose whole job is recording what went on the wire.
        assert len(fake_s3.requests) == 4, fake_s3.requests
        (keyed_get,) = [r for r in fake_s3.of("GET") if "list-type" not in r.query]
        (listing,) = [r for r in fake_s3.of("GET") if "list-type" in r.query]
        (head,) = fake_s3.of("HEAD")
        (bulk_delete,) = fake_s3.of("POST")
        assert keyed_get.acl_signed
        assert head.acl_signed
        assert "delete" in bulk_delete.query and not fake_s3.of("DELETE")
        assert bulk_delete.acl_signed
        assert fake_s3.objects == {}  # the bulk delete really deleted
        assert listing.acl == ACL
        assert "x-amz-content-sha256" in listing.signed_headers
        assert not listing.acl_signed

    def test_a_handle_without_the_header_signs_a_clean_list(self, fake_s3):
        # The other half of the fix's premise: strip the header and the LIST is
        # an ordinary signed request S3 accepts.
        import obstore

        list(obstore.list(_raw_store(fake_s3)))
        (listing,) = fake_s3.of("GET")
        assert listing.acl is None
        assert "x-amz-content-sha256" in listing.signed_headers


class TestPerPutAttributesCannotCarryTheAcl:
    """Issue #522 fix direction (b), ruled out by evidence.

    obstore's per-request ``attributes=`` surface maps any key it does not
    recognise to S3 USER METADATA, so ``{"x-amz-acl": ...}`` arrives as
    ``x-amz-meta-x-amz-acl`` -- a metadata field, not the canned ACL. Even if
    it did work, both write seams of zarr's obstore adapter
    (``ObjectStore.set`` and ``ObjectStore.set_if_not_exists``) call
    ``put_async(store, key, buf)`` with no attributes, so the zarr write path
    could not carry it.
    """

    def test_attributes_become_user_metadata_not_a_canned_acl(self, fake_s3):
        import obstore

        obstore.put(_raw_store(fake_s3), "k", b"hello", attributes={"x-amz-acl": ACL})
        (put,) = fake_s3.of("PUT")
        assert put.acl is None
        assert put.headers["x-amz-meta-x-amz-acl"] == ACL

    @pytest.mark.parametrize("method", ["set", "set_if_not_exists"])
    def test_zarr_adapter_writes_carry_no_attributes(self, method):
        # The trap issue #495 hit and the reason direction (b) is closed at the
        # zarr seam too: BOTH adapter writes are bare three-argument puts.
        # Checked structurally, on the call's own arguments, rather than by
        # matching zarr's current formatting of the line -- an upstream reflow
        # must not read here as "the issue #522 fix regressed".
        import ast
        import inspect
        import textwrap

        import zarr.storage._obstore as adapter

        tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(adapter.ObjectStore, method))))
        (put,) = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"put", "put_async"}
        ]
        assert len(put.args) == 3  # (store, key, buf)
        assert "attributes" not in {keyword.arg for keyword in put.keywords}
