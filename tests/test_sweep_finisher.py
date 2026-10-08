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


def _no_leaf_reads(monkeypatch):
    """Every leaf-reading seam the three families have raises."""
    import zagg.leaf_temporal as leaf_temporal
    import zagg.sweep as sweep

    def boom(*_a, **_k):
        raise AssertionError("the finisher must not read a leaf (issue #610)")

    for family in ("StatsFamily", "MocFamily", "SubmapFamily"):
        monkeypatch.setattr(getattr(sweep, family), "read_leaf", boom)
    monkeypatch.setattr(sweep, "_rollup_shard_node", boom)
    monkeypatch.setattr(leaf_temporal, "leaf_contribution", boom)


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
    prefix, names = run_status_prefix(root, "r610"), []
    for index in partition_leaves(leaves, OF):
        partition = {"index": index, "of": OF}
        summary = run_sweep(
            root,
            leaves,
            families=FAMILIES,
            partition=partition,
            status_record=(prefix, families_record_name(partition)),
        )
        assert summary["families"]["moc"]["finish_deferred"] is True
        # The returned summary carries the block's size; both records, the block.
        assert summary["families"]["moc"]["accumulator"] == {"shards": 4}
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
            assert block["visited"] == sorted(block["shards"])  # every leaf here holds a row
            assert block["fields"] == ["h_tdigest"] and len(block["shards"]) == 4
            assert "accumulator" not in record["families"]["stats"]
        names.append(families_record_name(partition))
    assert len(names) == 4
    return names


class TestComposedFinisher:
    def test_the_finisher_reads_no_leaf_and_matches_the_single_pass(self, tmp_path, monkeypatch):
        root, leaves = _store(tmp_path / "fan", 16)
        single, _same = _store(tmp_path / "single", 16)
        names = _fan_out(root, leaves)
        _no_leaf_reads(monkeypatch)
        summary = run_sweep(root, leaves, families=FAMILIES, finisher=_finisher(root, names))
        monkeypatch.undo()
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

    def test_a_missing_record_sends_the_finisher_to_the_leaves(self, tmp_path, caplog):
        import logging

        root, leaves = _store(tmp_path / "fan", 16)
        single, _same = _store(tmp_path / "single", 16)
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
