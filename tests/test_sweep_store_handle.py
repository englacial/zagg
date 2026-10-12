"""One store handle per sweep invoke (issue #610).

The families pass read every leaf by opening a store at the leaf's own path,
so a walk over N distinct leaf prefixes built N fresh clients (credential
resolution included: ~7 ``Found credentials`` a second on the v3 California
finisher). Every leaf object now reads through the pass's ONE handle at the
store root by relative key. Two claims are pinned here: the number of store
opens a pass makes does not grow with the leaf count, and the bytes the pass
writes are the ones the per-leaf-open code wrote (the committed spec fixture's
root objects, produced by that code, read back identical).
"""

import json
import shutil
from pathlib import Path

import pytest

from zagg.grids.morton import morton_word
from zagg.hive import shard_leaf_path
from zagg.sweep import run_sweep, write_leaf_submap
from zagg.telemetry import build_record, write_sidecar

FIXTURE = Path(__file__).parent / "data" / "spec" / "temporal"
SHARD = "11213"  # the fixture's one order-4 leaf
SHARD_ORDER = 4
SUBMAP_SIG = {
    "type": "healpix",
    "indexing_scheme": "nested",
    "parent_order": SHARD_ORDER,
    "child_order": SHARD_ORDER + 2,
    "layout": "flat",
}


def _decimals(n: int, base: str = "1") -> list[str]:
    """``n`` distinct order-4 shard ids under ``base`` (digits ``1..4``)."""
    out = []
    for i in range(n):
        digits, k = "", i
        for _ in range(SHARD_ORDER):
            digits, k = "1234"[k % 4] + digits, k // 4
        out.append(base + digits)
    return out


def _store(tmp_path, n: int, bases: tuple = ("1",)) -> tuple[str, list]:
    """The temporal fixture with its leaf cloned to ``n`` shards, each with every family's artifact.

    Every other leaf loses its ``temporal.toc`` record, so the temporal pass
    takes both routes: the record route and the raw route. ``bases`` clones
    ``n`` shards under each named base cell.
    """
    root = tmp_path / "store"
    shutil.copytree(FIXTURE, root)
    for name in ("coverage.moc", "coverage.toc"):
        (root / name).unlink()
    source = Path(shard_leaf_path(str(root), morton_word(SHARD)))
    leaves = []
    for i, decimal in enumerate(d for base in bases for d in _decimals(n, base)):
        word = morton_word(decimal)
        leaf = shard_leaf_path(str(root), word)
        if Path(leaf) != source:
            shutil.copytree(source, leaf)
        if i % 2:
            (Path(leaf) / "temporal.toc").unlink()  # the raw route
        write_sidecar(
            leaf,
            build_record(
                shard_key=word,
                metadata={"total_obs": 3, "cells_with_data": 1, "duration_s": 0.5},
                granule_ids=[f"g-{decimal}"],
            ),
        )
        write_leaf_submap(
            str(root),
            word,
            [{"id": f"g-{decimal}", "s3": f"s3://b/{decimal}", "https": f"https://h/{decimal}"}],
            grid_signature=SUBMAP_SIG,
            metadata={"collection": "TEST_001"},
        )
        leaves.append((word, None))
    if Path(source) != Path(shard_leaf_path(str(root), morton_word(_decimals(1)[0]))):
        shutil.rmtree(source)  # the clone source is not one of the n leaves
    return str(root), leaves


def _counting(monkeypatch) -> list:
    """Every store construction the pass makes, by path, whichever factory built it.

    Counted at the factories and at the constructors under them, so a client
    built directly — a ``LocalStore(...)``, or ``zarr.open_group`` on a
    string path — is seen too (the local stand-in for one S3 client).
    """
    import obstore.store
    import zarr.storage

    import zagg.hive as hive
    import zagg.store as store_mod

    opened = []
    real_init = zarr.storage.LocalStore.__init__

    def zarr_local(self, root, *a, **k):
        opened.append(("zarr-local", str(root)))
        real_init(self, root, *a, **k)

    def object_local(self, prefix=None, *a, **k):
        # obstore builds in ``__new__``; ``__init__`` only observes.
        opened.append(("object-local", str(prefix)))

    monkeypatch.setattr(zarr.storage.LocalStore, "__init__", zarr_local)
    monkeypatch.setattr(obstore.store.LocalStore, "__init__", object_local)
    real_object, real_store = store_mod.open_object_store, store_mod.open_store

    def object_store(path, *a, **k):
        opened.append(("object", path))
        return real_object(path, *a, **k)

    def zarr_store(path, *a, **k):
        opened.append(("zarr", path))
        return real_store(path, *a, **k)

    monkeypatch.setattr(store_mod, "open_object_store", object_store)
    monkeypatch.setattr(hive, "open_object_store", object_store)  # hive binds it at import
    monkeypatch.setattr(store_mod, "open_store", zarr_store)
    return opened


class TestOneHandlePerPass:
    FAMILIES = ("stats", "moc", "submap")

    def _opens(self, tmp_path, monkeypatch, n: int) -> list:
        root, leaves = _store(tmp_path / str(n), n)
        opened = _counting(monkeypatch)
        summary = run_sweep(root, leaves, families=self.FAMILIES)
        for family in self.FAMILIES:
            assert summary["families"][family]["failed"] == 0
            assert summary["families"][family]["empty"] == 0
        assert summary["families"]["moc"]["temporal_shards"] == n
        assert summary["families"]["moc"]["temporal_routes"] == {"records": n // 2, "raw": n // 2}
        monkeypatch.undo()
        return opened

    def test_the_open_count_does_not_grow_with_the_leaf_count(self, tmp_path, monkeypatch):
        """O(1) store opens per pass: 4 leaves and 16 leaves open the same stores.

        Before issue #610 every family opened a store per leaf (the stamp,
        the sidecar, the sub-map, the temporal record — four per leaf), so
        the two walks differed by 48 opens. Every open the pass still makes
        is at the store root — the handle itself plus the finisher's
        root-object reads and writes — never at a leaf.
        """
        few, many = self._opens(tmp_path, monkeypatch, 4), self._opens(tmp_path, monkeypatch, 16)
        assert len(few) == len(many)
        assert {Path(p).name for _k, p in many} == {"store"}  # the root, never a leaf
        # Not by a leaf's path, and not by a leaf's zarr store either.
        assert not [p for k, p in many if k.startswith("zarr")]
        assert ("object-local", str(tmp_path / "16" / "store")) in many  # the counter is live

    def test_the_readers_agree_with_their_path_form(self, tmp_path):
        """The relative-key reads return what the absolute-path wrappers return."""
        from zarr.storage import StorePath

        from zagg.hive import read_commit
        from zagg.leaf_temporal import read_leaf_temporal_record
        from zagg.store import open_object_store, open_store, zarr_view
        from zagg.sweep import _leaf_rel
        from zagg.telemetry import read_sidecar

        root, _leaves = _store(tmp_path, 2)
        store = open_object_store(root)
        for decimal in _decimals(2):
            rel, leaf = _leaf_rel(decimal, None), shard_leaf_path(root, morton_word(decimal))
            assert leaf == f"{root}/{rel}"
            assert read_commit(StorePath(zarr_view(store), rel)) == read_commit(open_store(leaf))
            assert read_sidecar(rel, store=store) == read_sidecar(leaf)
            assert read_leaf_temporal_record(rel, store=store) == read_leaf_temporal_record(leaf)
        assert read_commit(StorePath(zarr_view(store), "1/4/4/4/4/x.zarr")) is None
        assert read_sidecar("1/4/4/4/4/x.zarr", store=store) is None


class TestByteIdentity:
    """The pass writes the bytes the per-leaf-open code wrote.

    ``tests/data/spec/temporal``'s root ``coverage.moc`` and ``coverage.toc``
    were produced by ``MocFamily``'s leaf read and finisher before this
    change (``tools/generate_spec_fixtures.py``); a sweep over the fixture's
    leaf must write them back identically, ``generated_at`` aside.
    """

    @staticmethod
    def _stable(obj):
        return json.loads(json.dumps(obj, sort_keys=True).replace('"generated_at"', '"_"'))

    @pytest.mark.parametrize("name", ["coverage.moc", "coverage.toc"])
    def test_the_root_objects_match_the_committed_fixture(self, tmp_path, name):
        root = tmp_path / "store"
        shutil.copytree(FIXTURE, root)
        for stale in ("coverage.moc", "coverage.toc"):
            (root / stale).unlink()
        summary = run_sweep(str(root), [(morton_word(SHARD), None)], families=["moc"])
        assert summary["families"]["moc"]["root_moc_written"] is True
        got, expected = (
            json.loads((root / name).read_text()),
            json.loads((FIXTURE / name).read_text()),
        )
        for obj in (got, expected):
            obj.pop("generated_at")
            (obj.get("temporal") or {}).pop("generated_at", None)
        assert got == expected
