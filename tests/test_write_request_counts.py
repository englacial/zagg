"""Request counts and byte identity of the leaf + column writers (issue #657).

The windowed template build (``test_template_windowed_build``) runs through a
COUNTING ``LocalStore``: every store method call is recorded with the zagg
caller that issued it (carried across zarr's ``sync`` boundary) and bucketed
to the (shard, window) whose leaf or column prefix it touched. Two pins:

1. **Request ceilings per (shard, window)** — metadata GETs, metadata PUTs,
   data PUTs and the total. Issue #657 lands these as today's numbers first
   and tightens them phase by phase; a writer that regresses toward the
   post-mortem storm (issue #651) fails here by name.
2. **Byte identity** — a sha256 per object under every leaf and column
   prefix, against the committed manifest. The writers are being restructured
   (one ``create_hierarchy`` pass, kept array handles, hashes from memory)
   under the contract that NOT ONE BYTE on the store changes: every
   ``zarr.json``, chunk/shard object, stamp and O11 record must match. Set
   ``ZAGG_WRITE_MANIFEST_REGEN=1`` to regenerate the manifest after a
   deliberate format change (which also updates the spec fixtures, §4).

The ladder sweep writers (stage overview / stage column) share the store and
show the same pattern; they are a follow-on and are reported, not pinned.
"""

from __future__ import annotations

import collections
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest
import test_template_windowed_build as windowed
import zarr.core.sync as zsync
from zarr.storage import LocalStore

import zagg
from zagg import hive
from zagg.column import column_name
from zagg.store import open_store

ZAGG_SRC = os.path.dirname(zagg.__file__) + "/"

TEMPLATE = windowed.TEMPLATES[0]
MANIFEST = Path(__file__).parent / "data" / "write_manifest" / f"{TEMPLATE}.json"
FROZEN_NOW = "2026-01-01T00:00:00+00:00"

#: Store coroutines counted (``get_partial_values`` is counted once per key).
METHODS = ("get", "exists", "set", "set_if_not_exists", "delete", "delete_dir")
#: Async-generator listings: wrapped as generators, or zarr's member listing breaks.
LISTINGS = ("list", "list_dir", "list_prefix")
#: zarr / pydantic-zarr entry points worth naming in the attribution.
ZARR_ENTRY = (
    "open_array",
    "to_zarr",
    "create_hierarchy",
    "create_array",
    "open_group",
    "open",
    "set_block_selection",
)

#: Per-(shard, window) ceilings over the leaf + column prefixes. Issue #657
#: phase 1 pins today's writer; later phases lower them (acceptance: metadata
#: GETs < 10, metadata PUTs one per ``zarr.json``, total <= 250). A kind with
#: no entry (data GET, HEAD, ``list``, ``list_prefix``) is pinned at zero.
CEILINGS = {
    "metadata GET": 200,
    "metadata PUT": 99,
    "data PUT": 82,
    "data DELETE": 28,
    "delete_dir": 5,
    "list_dir": 12,
    "total": 395,
}


def _attribute() -> str:
    """``module:function via zarr_entry`` for the innermost zagg frame on the stack."""
    frame = sys._getframe(1)
    zagg_caller = entry = None
    while frame is not None:
        filename, name = frame.f_code.co_filename, frame.f_code.co_name
        if entry is None and ("/zarr/" in filename or "pydantic_zarr" in filename):
            if name in ZARR_ENTRY:
                entry = name
        if zagg_caller is None and filename.startswith(ZAGG_SRC):
            zagg_caller = filename[len(ZAGG_SRC) :] + ":" + name
        frame = frame.f_back
    return f"{zagg_caller or '?'} via {entry or '-'}"


class Counter:
    """Records ``(store root, key, method, caller)`` for every LocalStore call."""

    def __init__(self):
        self.ops: list[tuple[str, str, str, str]] = []
        self.caller = "-"

    def install(self, monkeypatch) -> None:
        original_sync = zsync.sync

        def counted_sync(coro, *args, **kwargs):
            self.caller = _attribute()
            try:
                return original_sync(coro, *args, **kwargs)
            finally:
                self.caller = "-"  # an op outside any sync reads as unattributed

        # The attribution is taken where the sync boundary is crossed, so the
        # async store methods below see the zagg frame that scheduled them.
        for module in list(sys.modules.values()):
            if getattr(module, "sync", None) is original_sync:
                monkeypatch.setattr(module, "sync", counted_sync)
        for method in METHODS:
            monkeypatch.setattr(LocalStore, method, self._wrap(method))
        for method in LISTINGS:
            monkeypatch.setattr(LocalStore, method, self._wrap_listing(method))
        monkeypatch.setattr(LocalStore, "get_partial_values", self._wrap_partial())

    def _wrap(self, method):
        original = getattr(LocalStore, method)
        counter = self

        async def counted(self, key, *args, **kwargs):
            counter.ops.append((str(self.root), str(key), method, counter.caller))
            return await original(self, key, *args, **kwargs)

        return counted

    def _wrap_listing(self, method):
        original = getattr(LocalStore, method)
        counter = self

        async def counted(self, *args, **kwargs):
            prefix = args[0] if args else kwargs.get("prefix", "")
            counter.ops.append((str(self.root), str(prefix), method, counter.caller))
            async for item in original(self, *args, **kwargs):
                yield item

        return counted

    def _wrap_partial(self):
        original = LocalStore.get_partial_values
        counter = self

        async def counted(self, prototype, key_ranges):
            key_ranges = list(key_ranges)
            for key, _byte_range in key_ranges:
                counter.ops.append((str(self.root), str(key), "get", counter.caller))
            return await original(self, prototype, key_ranges)

        return counted


def _kind(key: str, method: str) -> str:
    if method in ("delete_dir", *LISTINGS):
        return method
    doc = "metadata" if key.endswith("zarr.json") else "data"
    verb = {
        "get": "GET",
        "exists": "HEAD",
        "set": "PUT",
        "set_if_not_exists": "PUT",
        "delete": "DELETE",
    }[method]
    return f"{doc} {verb}"


def _prefixes(root: Path, shards) -> dict[tuple[int, str], tuple[str, str]]:
    """``(shard, window) -> (leaf prefix, column prefix)``, absolute paths."""
    out = {}
    for shard in shards:
        for window in windowed.WINDOWS:
            leaf = hive.shard_leaf_path(str(root), shard, window=window)
            node = leaf.rsplit("/", 1)[0]
            out[(shard, window)] = (leaf, f"{node}/{column_name(window)}")
    return out


def _bucket(ops, prefixes):
    """Per-(shard, window) ``Counter[(kind, caller)]`` plus an ``other`` bucket."""
    buckets: dict = {key: collections.Counter() for key in prefixes}
    other: collections.Counter = collections.Counter()
    for root, key, method, caller in ops:
        full = f"{root.rstrip('/')}/{key}" if key else root
        kind = _kind(key, method)
        for unit, (leaf, column) in prefixes.items():
            if full.startswith(leaf) or full.startswith(column):
                buckets[unit][(kind, caller)] += 1
                break
        else:
            other[(kind, caller)] += 1
    return buckets, other


def _totals(counter) -> dict[str, int]:
    totals: dict[str, int] = collections.Counter()
    for (kind, _caller), n in counter.items():
        totals[kind] += n
        totals["total"] += n
    return dict(totals)


def _manifest(root: Path, prefixes) -> dict[str, str]:
    """sha256 per object under every leaf and column prefix.

    The leaf's version segment (``run-<id>``) is normalized to ``<version>``
    and the pointer stamp's ``current`` value (which must name that run
    directory) to ``<version>`` in its raw bytes; everything else is the raw
    bytes. Telemetry sidecars beside the prefixes (``*.stats .json``, with
    wall-clock timestamps) are not the writers' store bytes.
    """
    out = {}
    for leaf, column in prefixes.values():
        runs = [d for d in os.listdir(leaf) if d.startswith("run-")]
        assert len(runs) == 1, (leaf, runs)  # a second run would collide on <version>
        for prefix in (leaf, column):
            for dirpath, _dirs, files in os.walk(prefix):
                for name in files:
                    path = Path(dirpath) / name
                    rel = str(path.relative_to(root))
                    parts = rel.split("/")
                    version = [i for i, p in enumerate(parts) if p.startswith("run-")]
                    data = path.read_bytes()
                    if version:
                        parts[version[0]] = "<version>"
                    elif str(path) == f"{leaf}/zarr.json":
                        current = json.loads(data)["attributes"][hive.COMMIT_ATTR]["current"]
                        assert current in runs, (leaf, current, runs)
                        field = f'"current": "{current}"'.encode()
                        assert data.count(field) == 1, (leaf, current)
                        data = data.replace(field, b'"current": "<version>"')
                    out["/".join(parts)] = hashlib.sha256(data).hexdigest()
    return out


@pytest.fixture(scope="module")
def counted_build(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("counted-store")
    counter = Counter()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(hive, "_utcnow", lambda: FROZEN_NOW)
        counter.install(monkeypatch)
        _cfg, root, shards = windowed._build(monkeypatch, tmp_path, TEMPLATE)
    prefixes = _prefixes(root, shards)
    buckets, other = _bucket(counter.ops, prefixes)
    return root, prefixes, buckets, other


def _report(buckets, other) -> str:
    n = len(buckets)
    merged: collections.Counter = collections.Counter()
    for counter in buckets.values():
        merged.update(counter)
    lines = [f"per (shard, window) over {n} units, leaf + column prefixes:"]
    for (kind, caller), count in sorted(merged.items(), key=lambda kv: -kv[1]):
        lines.append(f"  {count / n:7.1f}  {kind:14s} {caller}")
    lines.append("  mean: " + json.dumps({k: round(v / n, 1) for k, v in _totals(merged).items()}))
    for unit, counter in buckets.items():
        lines.append(f"  {unit}: " + json.dumps(_totals(counter)))
    lines.append("other (ladder sweep, root, refs), whole build: " + json.dumps(_totals(other)))
    return "\n".join(lines)


def test_requests_per_shard_window_stay_under_the_ceilings(counted_build):
    _root, _prefixes, buckets, other = counted_build
    report = _report(buckets, other)
    print("\n" + report)
    for unit, counter in buckets.items():
        totals = _totals(counter)
        for kind, count in totals.items():
            assert count <= CEILINGS.get(kind, 0), (unit, kind, totals, report)


def test_every_unit_lands_its_objects(counted_build):
    """Each (shard, window) wrote its leaf AND its column (the buckets are live)."""
    root, prefixes, buckets, _other = counted_build
    for unit, (leaf, column) in prefixes.items():
        assert os.path.isdir(leaf) and os.path.isdir(column), unit
        totals = _totals(buckets[unit])
        assert totals["metadata PUT"] > 0 and totals["data PUT"] > 0, (unit, totals)


def test_counting_store_keeps_the_content_hashes(counted_build):
    """The counting wrappers do not break the O11 hasher: every stamp carries its record."""
    _root, prefixes, _buckets, _other = counted_build
    for unit, (leaf, column) in prefixes.items():
        data_path, _pointer = hive.resolve_leaf(leaf)
        for path in (data_path, column):
            stamp = hive.read_commit(open_store(path, read_only=True))
            arrays = (stamp.get("content_hashes") or {}).get("arrays", {})
            assert arrays and set(arrays) == _array_paths(path), (unit, path, sorted(arrays))


def _array_paths(path: str) -> set[str]:
    """Every array under a group root, as paths relative to it."""
    out = set()
    for dirpath, _dirs, files in os.walk(path):
        if "zarr.json" in files:
            doc = json.loads((Path(dirpath) / "zarr.json").read_text())
            if doc.get("node_type") == "array":
                out.add(os.path.relpath(dirpath, path))
    return out


def test_store_bytes_match_the_manifest(counted_build):
    root, prefixes, _buckets, _other = counted_build
    manifest = _manifest(root, prefixes)
    assert len(manifest) > 100
    if os.environ.get("ZAGG_WRITE_MANIFEST_REGEN"):
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
        pytest.fail("manifest regenerated; re-run without ZAGG_WRITE_MANIFEST_REGEN")
    pinned = json.loads(MANIFEST.read_text())
    missing = sorted(set(pinned) - set(manifest))
    extra = sorted(set(manifest) - set(pinned))
    changed = sorted(k for k in pinned if k in manifest and pinned[k] != manifest[k])
    assert not (missing or extra or changed), {
        "missing": missing,
        "extra": extra,
        "changed": changed,
    }
