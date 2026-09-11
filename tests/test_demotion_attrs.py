"""Issue #518: artifact-visible attrs when the packed guard rail demotes.

When an overview fold demotes ``composition`` at a node — a contributor
carrying half of the ``(word, of-digest)`` pair, in either direction — the
bytes that land are correct (the fill word makes no §3.2 claim) but the only
record of the demotion was a log line in an exited worker. These tests pin
the fix: the fold returns :func:`zagg.sweep_overview.demotion_records` and
the writer keys them into the node's ``zagg_overview`` attrs as
``demotions`` — present exactly when the rail fired, so a clean store's
attrs are byte-identical to a pre-#518 writer's.

The fold-level verdicts (poison direction, drop direction, clean) are pinned
beside the rail itself in ``test_strata_composability.py``; this module pins
the ARTIFACT: the attrs a reader sees, on both sweep revisions.
"""

from __future__ import annotations

import numpy as np
import test_strata_composability as tsc

from zagg.sweep_overview import (
    DEMOTION_DIVISOR_MISSING,
    DEMOTION_WORD_MISSING,
    OVERVIEW_ATTR,
    demotion_records,
    note_demotion,
)

#: The stage-column geometry of ``tsc.TestStageMergeHalfPair``: order-3
#: columns with a node-order relay member, folded by a (node 1, cells 2)
#: stage-merge level whose one populated output cell mixes both children.
NODE_ORDER, CELL_ORDER, RES = 3, 5, 4


def _write_column(root, dec, per_cell):
    """One committed strata column at ``dec`` (the tsc harness recipe)."""
    from zagg.column import column_name, fold_column, write_column
    from zagg.grids.morton import morton_word
    from zagg.sweep import _node_rel

    slabs = {
        "h_sig": np.array([c["sig"] for c in per_cell], dtype=object),
        "h_noise": np.array([c["noise"] for c in per_cell], dtype=object),
        "composition": np.array([c["word"] for c in per_cell], dtype=np.uint64),
    }
    folded = fold_column(
        slabs, tsc._STRATA_FIELDS, cell_order=CELL_ORDER, resolutions=[RES, NODE_ORDER]
    )
    write_column(
        str(root),
        morton_word(dec),
        folded,
        tsc._STRATA_FIELDS,
        node_order=NODE_ORDER,
        cell_order=CELL_ORDER,
        granule_count=1,
    )
    return f"{root}/{_node_rel(dec)}/{column_name(None)}"


class TestDemotionRecords:
    """The record grammar itself: deterministic, minimal, per (field, reason)."""

    def test_records_aggregate_per_field_and_reason(self):
        acc: dict = {}
        note_demotion(acc, "composition", DEMOTION_WORD_MISSING, "h_sig", cells=[0, 1])
        note_demotion(acc, "composition", DEMOTION_WORD_MISSING, "h_sig", cells=[1, 2])
        note_demotion(acc, "composition", DEMOTION_DIVISOR_MISSING, "h_sig")
        assert demotion_records(acc) == [
            {
                "field": "composition",
                "class": "packed",
                "reason": "divisor-missing",
                "contributors": 1,
                "of": "h_sig",
            },
            {
                "field": "composition",
                "class": "packed",
                "reason": "word-missing",
                "contributors": 2,
                "of": "h_sig",
                "cells": 3,
            },
        ]

    def test_empty_accumulator_yields_no_records(self):
        assert demotion_records({}) == []


class TestStageArtifactDemotions:
    """The ``/2`` stage writer: ``zagg_overview.demotions`` at the fired node."""

    def _columns(self, tmp_path):
        a, _ = tsc._strata_cells(k=16, n=40, seed=610)
        b, _ = tsc._strata_cells(k=16, n=40, seed=611)
        return [_write_column(tmp_path, "1111", a), _write_column(tmp_path, "1112", b)]

    def _reader(self, path):
        from zagg.hive import _utcnow
        from zagg.sweep_stage import _ColumnReader

        return _ColumnReader(path, run_id="A", run_started=_utcnow(), store_kwargs={})

    def _fold_and_write(self, root, paths):
        """A (node 1, cells 2) stage merge over the two columns, written."""
        from zagg.sweep_stage import _stage_fold, _write_stage_overview

        readers = {"1111": [self._reader(paths[0])], "1112": [self._reader(paths[1])]}
        fold = _stage_fold(
            "11",
            1,
            2,
            readers,
            tsc._STRATA_FIELDS,
            shard_order=NODE_ORDER,
            child_order=NODE_ORDER,
            all_time=False,
        )
        assert fold is not None and fold["regime"] == "stage-merge"
        _write_stage_overview(
            str(root),
            "11",
            1,
            None,
            2,
            fold,
            tsc._STRATA_FIELDS,
            NODE_ORDER,
            CELL_ORDER,
            False,
            "run-518",
            {},
        )
        return fold

    def _attrs(self, root):
        import zarr

        from zagg.store import open_store

        group = zarr.open_group(
            open_store(f"{root}/1/1/all.zarr", read_only=True), path="", mode="r", zarr_format=3
        )
        return dict(dict(group.attrs)[OVERVIEW_ATTR])

    def test_a_clean_fold_writes_no_demotions_key(self, tmp_path):
        """Byte-identity for clean stores: the key is absent, not empty."""
        paths = self._columns(tmp_path)
        fold = self._fold_and_write(tmp_path, paths)
        assert "demotions" not in fold
        assert "demotions" not in self._attrs(tmp_path)

    def test_a_half_paired_contributor_lands_in_the_artifact(self, tmp_path):
        """The poison direction: word missing, divisor folded — cells blanked."""
        import shutil

        paths = self._columns(tmp_path)
        shutil.rmtree(f"{paths[1]}/{NODE_ORDER}/composition")
        fold = self._fold_and_write(tmp_path, paths)
        attrs = self._attrs(tmp_path)
        assert attrs["demotions"] == fold["demotions"]
        assert attrs["demotions"] == [
            {
                "field": "composition",
                "class": "packed",
                "reason": "word-missing",
                "contributors": 1,
                "of": "h_sig",
                "cells": 1,
            }
        ]
        # The record sits with the coverage counters it refines.
        assert attrs["source_children"]["unreadable"] == 1

    def test_a_missing_divisor_lands_in_the_artifact(self, tmp_path):
        """The drop direction — the issue's mis-declared-divisor shape.

        A column carrying the word but not its ``of`` digest — the shape a
        mis-declared divisor leaves behind — contributes nothing for the
        field, and the artifact now says so. (One contributor only: the
        packed rail counts a fired contributor ``unreadable``, and a level
        where NO contributor folds cleanly returns no fold at all.)
        """
        import shutil

        paths = self._columns(tmp_path)
        shutil.rmtree(f"{paths[1]}/{NODE_ORDER}/h_sig")
        fold = self._fold_and_write(tmp_path, paths)
        attrs = self._attrs(tmp_path)
        assert attrs["demotions"] == fold["demotions"]
        assert attrs["demotions"] == [
            {
                "field": "composition",
                "class": "packed",
                "reason": "divisor-missing",
                "contributors": 1,
                "of": "h_sig",
            }
        ]
