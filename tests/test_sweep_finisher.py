"""The families finisher composes from the partitions' accumulators (issue #610, phase 3).

A ``4^k`` fan-out leaves every rollup at and below its split order and, in
each partition's run record, the §10 contributions that partition read. The
finisher is handed the records' keys: it folds the blocks, starts its walk
from the split-order rollups, and writes the coarse levels, the root
``coverage.moc`` and its ``coverage.toc`` sibling without reading a leaf —
byte-identical to the single pass over the same leaves. A record it cannot
use sends it back to the leaf walk, and its record says so.
"""

import json
import shutil
from pathlib import Path

import pytest
from test_sweep_store_handle import _store

from zagg.client_transport import run_status_prefix
from zagg.sweep import run_sweep
from zagg.sweep_fleet import families_record_name
from zagg.sweep_partition import partition_leaves

FAMILIES = ("stats", "moc", "submap")
OF = 64  # the 16 cloned leaves sit under four order-3 nodes -> four partitions


def _finisher(root: str, names: list) -> dict:
    """The block the dispatcher hands the finisher once every record stood."""
    return {"of": OF, "records_from": run_status_prefix(root, "r610"), "accumulators": names}


# Anything below the node directories at the shard order: a leaf's zarr
# group (its sidecar, ``temporal.toc`` and bitmap included) or a node's
# per-leaf JSON. Rollups, run records and the manifest stay readable.
_LEAF_SUFFIXES = (".zarr", "stats.json", "shardmap.json", "temporal.toc", "granules.json")


def _leaf_key(key) -> bool:
    key = str(key).rstrip("/")
    return ".zarr/" in key or key.endswith(_LEAF_SUFFIXES)


def _no_leaf_reads(monkeypatch) -> list:
    """Every leaf-reading seam raises, by name AND at the store layer; the hits.

    The named seams are the three families' current ones. The store-layer
    guard wraps every ``obstore`` read entry point and the ``zagg.store``
    open factories, so a leaf read through any other path fails too. The
    hits are also returned, since a fail-open caller may swallow the raise.
    """
    import obstore

    import zagg.hive as hive
    import zagg.leaf_temporal as leaf_temporal
    import zagg.store as zstore
    import zagg.sweep as sweep

    hits: list = []

    def boom(*_a, **_k):
        hits.append(_a[1:2])
        raise AssertionError("the finisher must not read a leaf (issue #610)")

    for family in ("StatsFamily", "MocFamily", "SubmapFamily"):
        monkeypatch.setattr(getattr(sweep, family), "read_leaf", boom)
    monkeypatch.setattr(sweep, "_rollup_shard_node", boom)
    monkeypatch.setattr(leaf_temporal, "leaf_contribution", boom)

    def guarded(real, at):
        def read(*a, **k):
            if len(a) > at and _leaf_key(a[at]):
                hits.append(a[at])
                raise AssertionError(f"the finisher read leaf key {a[at]} (issue #610)")
            return real(*a, **k)

        return read

    for name in ("get", "get_range", "get_ranges", "head"):
        for fn in (name, f"{name}_async"):
            monkeypatch.setattr(obstore, fn, guarded(getattr(obstore, fn), 1))
    for module in (zstore, hive):
        for fn in ("open_store", "open_object_store"):
            if hasattr(module, fn):
                monkeypatch.setattr(module, fn, guarded(getattr(module, fn), 0))
    return hits


def _copy(root: str, to: Path) -> str:
    """A byte-identical copy of a built store: the single-pass reference.

    Copied rather than rebuilt, because the fixture stamps each leaf's
    sidecar with the wall clock — two builds straddling a second differ.
    """
    shutil.copytree(root, to / "store")
    return str(to / "store")


def _empty(root: str, leaves) -> None:
    """Remove every artifact of ``leaves``: nothing to fold there."""
    from zagg.hive import shard_leaf_path

    for word, _window in leaves:
        shutil.rmtree(Path(shard_leaf_path(root, word)).parent)


def _objects(root: str) -> dict:
    """Every rollup and root coverage object, ``generated_at`` stripped."""
    out = {}
    for path in sorted(Path(root).rglob("*")):
        # The ROOT objects only: a leaf's ``coverage.moc`` is its binary bitmap.
        root_object = path.parent == Path(root) and path.name in ("coverage.moc", "coverage.toc")
        if path.name.endswith(".rollup.json") or root_object:
            obj = json.loads(path.read_text())
            obj.pop("generated_at", None)
            (obj.get("temporal") or {}).pop("generated_at", None)
            out[str(path.relative_to(root))] = obj
    return out


def _fan_out(root: str, leaves) -> list:
    """Run every non-empty partition in-process; the status-prefix record names, in index order."""
    from zagg.grids.morton import morton_decimal

    prefix, names = run_status_prefix(root, "r610"), []
    for index, mine in partition_leaves(leaves, OF).items():
        partition = {"index": index, "of": OF}
        summary = run_sweep(
            root,
            leaves,
            families=FAMILIES,
            partition=partition,
            status_record=(prefix, families_record_name(partition)),
        )
        assert summary["families"]["moc"]["finish_deferred"] is True
        visited = sorted({morton_decimal(int(k)) for k, _w in mine})
        for path in (Path(root) / summary["record"], Path(summary["status_record"])):
            record = json.loads(path.read_text())
            block = record["families"]["moc"]["accumulator"]
            assert sorted(block) == [
                "cell_order",
                "fields",
                "routes",
                "shards",
                "uncounted",
                "visited",
            ]
            # Every shard the partition walked, whether or not it held a row.
            assert block["visited"] == visited and set(block["shards"]) <= set(visited)
            assert block["fields"] == (["h_tdigest"] if block["shards"] else [])
            assert "accumulator" not in record["families"]["stats"]
        # The returned summary carries the block's size; both records, the block.
        assert summary["families"]["moc"]["accumulator"] == {"shards": len(block["shards"])}
        names.append(families_record_name(partition))
    return names


class TestComposedFinisher:
    def test_the_finisher_reads_no_leaf_and_matches_the_single_pass(self, tmp_path, monkeypatch):
        root, leaves = _store(tmp_path / "fan", 16)
        single = _copy(root, tmp_path / "single")
        names = _fan_out(root, leaves)
        assert len(names) == 4
        hits = _no_leaf_reads(monkeypatch)
        summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        monkeypatch.undo()
        assert hits == []
        assert summary["finisher"] == {"of": OF, "accumulators": 4}
        moc = summary["families"]["moc"]
        assert moc["root_moc_written"] is True
        assert moc["temporal_shards"] == 16 and moc["cover_shards"] == 16
        assert moc["uncounted_shards"] == 8  # the record-less half, carried by the blocks
        assert moc["temporal_routes"] == {"records": 8, "raw": 8}
        assert moc["pass_uncounted"]["count"] == 8
        # Only the coarse levels were folded here: no shard node was written.
        assert summary["families"]["stats"]["written"] == 3  # orders 2, 1, 0 above the split
        reference = run_sweep(single, leaves, families=FAMILIES)
        assert reference["families"]["moc"]["temporal_shards"] == 16
        assert _objects(root) == _objects(single)

    def test_the_guard_sees_a_leaf_read_by_any_path(self, tmp_path, monkeypatch):
        import obstore

        from zagg.hive import shard_leaf_path
        from zagg.store import open_object_store

        root, leaves = _store(tmp_path / "fan", 1)
        store = open_object_store(root)
        rel = Path(shard_leaf_path(root, leaves[0][0])).relative_to(root)
        hits = _no_leaf_reads(monkeypatch)
        for key in (f"{rel}/zarr.json", f"{rel.parent}/stats.json", f"{rel}/temporal.toc"):
            with pytest.raises(AssertionError, match="leaf key"):
                obstore.get(store, key)
        assert obstore.get(store, "morton_hive.json").bytes()  # the manifest stays readable
        assert len(hits) == 3

    def test_two_base_cells_and_an_empty_partition_match_the_single_pass(
        self, tmp_path, monkeypatch
    ):
        root, leaves = _store(tmp_path / "fan", 16, bases=("1", "2"))
        # One partition's leaves hold nothing, in both copies (emptied before
        # the copy): it writes no split-order rollup, and the single pass
        # finds nothing there either.
        _empty(root, partition_leaves(leaves, OF)[0])
        single = _copy(root, tmp_path / "single")
        names = _fan_out(root, leaves)
        assert len(names) == 4  # a partition is a subtree index, spanning both bases
        hits = _no_leaf_reads(monkeypatch)
        summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        monkeypatch.undo()
        assert hits == [] and summary["finisher"] == {"of": OF, "accumulators": 4}
        assert summary["families"]["stats"]["empty"] == 2  # its missing rollup, under each base
        reference = run_sweep(single, leaves, families=FAMILIES)
        assert summary["families"]["moc"]["temporal_shards"] == 24
        assert reference["families"]["moc"]["temporal_shards"] == 24
        assert _objects(root) == _objects(single)

    def test_a_missing_record_sends_the_finisher_to_the_leaves(self, tmp_path, caplog):
        import logging

        root, leaves = _store(tmp_path / "fan", 16)
        single = _copy(root, tmp_path / "single")
        names = _fan_out(root, leaves)
        (Path(run_status_prefix(root, "r610")) / names[1]).unlink()
        with caplog.at_level(logging.WARNING, logger="zagg.sweep"):
            summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        assert (
            summary["finisher"]["accumulators"] == 4 and "absent" in summary["finisher"]["fallback"]
        )
        assert "reading the leaves instead" in caplog.text
        # The leaf walk: every shard node re-read and found current, as are
        # the four split-order rollups the partitions wrote; nothing folded twice.
        assert summary["families"]["stats"]["current"] == 20
        assert summary["families"]["moc"]["temporal_shards"] == 16
        run_sweep(single, leaves, families=FAMILIES)
        assert _objects(root) == _objects(single)

    def test_a_block_a_family_cannot_use_falls_back_too(self, tmp_path):
        root, leaves = _store(tmp_path / "fan", 16)
        names = _fan_out(root, leaves)
        path = Path(run_status_prefix(root, "r610")) / names[0]
        record = json.loads(path.read_text())
        record["families"]["moc"]["accumulator"]["shards"]["11111"][0]["counts"]["obs_total"] = (
            10**9
        )
        path.write_text(json.dumps(record))
        summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        assert "obs_total" in summary["finisher"]["fallback"]
        assert summary["families"]["moc"]["temporal_shards"] == 16

    def test_a_failed_record_get_falls_back(self, tmp_path, monkeypatch, caplog):
        import logging

        import obstore.exceptions

        import zagg.hive as hive

        root, leaves = _store(tmp_path / "fan", 16)
        names = _fan_out(root, leaves)

        read_json = hive._read_json

        def throttled(store, key, *a, **k):
            if key in names:  # the status-prefix records only, not the manifest
                raise obstore.exceptions.GenericError("503 slow down")  # not a ValueError
            return read_json(store, key, *a, **k)

        monkeypatch.setattr(hive, "_read_json", throttled)
        with caplog.at_level(logging.WARNING, logger="zagg.sweep"):
            summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        monkeypatch.undo()
        assert summary["finisher"]["fallback"] == "503 slow down"
        assert "reading the leaves instead" in caplog.text
        assert summary["families"]["moc"]["temporal_shards"] == 16

    def test_a_shard_no_partition_visited_falls_back(self, tmp_path):
        from zagg.grids.morton import morton_word

        root, leaves = _store(tmp_path / "fan", 16)
        names = _fan_out(root, leaves)
        # A leaf outside every partition's work set (the store's ``discover``
        # set is wider than this run's): the blocks do not cover it.
        wider = [*leaves, (morton_word("12111"), None)]
        summary = run_sweep(root, wider, families=FAMILIES, finisher=_finisher(root, names))
        assert summary["finisher"]["fallback"] == "1 shard(s) in the work set no partition visited"
        assert summary["families"]["moc"]["temporal_shards"] == 16

    def test_the_handler_forwards_the_finisher_block(self, tmp_path, monkeypatch):
        from test_sweep import _handler_module

        root, leaves = _store(tmp_path / "fan", 16)
        names = _fan_out(root, leaves)
        _no_leaf_reads(monkeypatch)
        response = _handler_module()._handle_sweep(
            {
                "mode": "sweep",
                "store_path": root,
                "leaves": [[int(k), w] for k, w in leaves],
                "families": list(FAMILIES),
                "records_from": run_status_prefix(root, "r610"),
                "finisher": _finisher(root, names),
            }
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["families"]["moc"]["temporal_shards"] == 16
        assert body["families"]["moc"]["root_moc_written"] is True


class TestEventBuilder:
    def test_the_finisher_block_rides_the_event_validated(self):
        from zagg.runner import _build_sweep_event

        block = {"of": 16, "records_from": "s3://b/s.status/run-x", "accumulators": ["a.json"]}
        event = _build_sweep_event("s3://b/s", [(7, None)], finisher=block)
        assert event["finisher"] == block
        with pytest.raises(ValueError, match="power"):
            _build_sweep_event("s3://b/s", [(7, None)], finisher={**block, "of": 3})
        assert "finisher" not in _build_sweep_event("s3://b/s", [(7, None)])
        # Over the payload cap the worker discovers the STORE's work set, which
        # the partitions' accumulators do not cover: the block is dropped too.
        big = _build_sweep_event("s3://b/s", [(k, None) for k in range(20000)], finisher=block)
        assert big["discover"] is True and "leaves" not in big and "finisher" not in big
