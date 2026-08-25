"""The fleet transport for the /2 staged dense sweep (issue #519).

Standing claims:

- one ``mode="sweep"`` event with a ``stage`` block IS the worker arm: lease
  admission, run-id skip keys and the foreign-fresh-stamp abort are the
  in-process pass's, unchanged, and every store write stays worker-side (D8);
- a tuple splits across invokes freely — dispatch nodes at one order own
  disjoint subtrees and read only columns one tuple finer;
- the dispatcher mirrors ``run_stage_sweep``'s tuple ordering: fan out a
  tuple, soft-barrier on the stage records, then the next tuple, finisher
  last (#381 point (6): under-coverage is loud and self-healing);
- **the acceptance**: the fleet-built ladder is byte-identical to the
  CLI-built ladder on the same store (the merge-source law, espg ruling
  2026-08-09, issue #384) — its ``TestByteIdentityOracle`` lands with phase 3
  of this PR and is NOT in this file yet; phases 1-2 pin the transport.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

# The staged-sweep fixtures live with the in-process suite; the fleet arm is
# the SAME store and the SAME expectations, reached over the wire.
from test_sweep_stage import LEAVES, _artifact, _stage_store, _write_leaf  # noqa: E402

from zagg.grids.morton import morton_word
from zagg.sweep_stages import (
    FINISHER_RECORD_NAME,
    STAGE_RECORD_SPEC,
    merge_level_actuals,
    run_stage_finisher,
    run_stage_worker,
    stage_record_name,
)


def _handler_module():
    handler_path = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_stage", handler_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _leaf_refs(leaves=LEAVES):
    return [[morton_word(d), None] for d in leaves]


def _event(root, block, leaves=LEAVES):
    return {
        "mode": "sweep",
        "store_path": str(root),
        "leaves": _leaf_refs(leaves),
        "stage": block,
    }


RUN_STARTED = "2026-08-25T00:00:00+00:00"


def _stage_block(dispatch, nodes, *, run_id="F", batch=0, records_from=None, **extra):
    block = {
        "role": "stage",
        "run_id": run_id,
        "run_started": RUN_STARTED,
        "dispatch": int(dispatch),
        "nodes": list(nodes),
        "batch": int(batch),
        "tuple_width": 3,
    }
    if records_from is not None:
        block["records_from"] = str(records_from)
    block.update(extra)
    return block


# ---------------------------------------------------------------------------
# Phase 1: the worker arm.
# ---------------------------------------------------------------------------


class TestStageWorkerArm:
    def test_one_invoke_folds_only_its_nodes(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        record = run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
        )
        (row,) = record["stages"]
        assert row["dispatch_order"] == 0 and row["nodes"] == 1 and row["written"] > 0
        assert (root / "1" / "all.zarr").exists()
        # The base cell it was NOT handed is untouched — the fan-out's whole point.
        assert not (root / "-2" / "all.zarr").exists()

    def test_the_tuple_restriction_is_the_only_difference(self, tmp_path):
        # Without ``only_dispatch`` a scope naming an order-0 node also admits
        # its own subtree at every finer dispatch order (containment resolves
        # both ways), which would have one worker sweep the whole ladder.
        from zagg.sweep_stages import normalize_scope, sweep_stage_pass

        root = tmp_path / "s"
        manifest = _stage_store(root)
        whole = sweep_stage_pass(
            str(root),
            manifest,
            {d: {None} for d in LEAVES},
            run_id="A",
            scope=normalize_scope(["1"]),
        )
        assert [r["dispatch_order"] for r in whole["stages"]] == [0]  # o3/width 3: one tuple
        with pytest.raises(ValueError, match="no stage tuple dispatches at order 1"):
            sweep_stage_pass(
                str(root), manifest, {d: {None} for d in LEAVES}, run_id="A", only_dispatch=1
            )

    def test_a_mistyped_dispatch_refuses_even_with_nothing_to_sweep(self, tmp_path):
        # The refusal must not sit behind the pass's empty-summary gates: a
        # store with no composable fields would otherwise let ANY dispatch
        # order return ``stages: []`` and PUT a well-formed record on it.
        import copy

        from zagg.sweep_stages import sweep_stage_pass

        root = tmp_path / "s"
        manifest = _stage_store(root)
        bare = copy.deepcopy(manifest)
        bare["pyramid"]["overview"]["fields"] = {}
        assert sweep_stage_pass(str(root), bare, {}, run_id="A")["stages"] == []
        with pytest.raises(ValueError, match="no stage tuple dispatches at order 7"):
            sweep_stage_pass(str(root), bare, {}, run_id="A", only_dispatch=7)
        # Same over the worker entry point, with an order-7 node set to match.
        with pytest.raises(ValueError, match="no stage tuple dispatches at order 7"):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                run_started=RUN_STARTED,
                dispatch=7,
                nodes=["11111111"],
            )

    def test_the_worker_writes_and_holds_the_lease(self, tmp_path):
        from zagg.sweep_lease import read_lease

        root = tmp_path / "s"
        _stage_store(root)
        run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
        )
        held = read_lease(str(root))
        # Held, ours, and NOT released: release is the finisher's final act.
        assert held is not None and held["run_id"] == "F" and held["scope"] is None

    def test_an_empty_node_set_refuses_instead_of_sweeping_the_store(self, tmp_path):
        # ``scope=None`` is whole-store; a dropped/empty ``nodes`` key must not
        # decay into it, or every worker that loses the key double-writes every
        # object as a same-run sibling nothing refuses.
        from zagg.sweep_lease import read_lease

        root = tmp_path / "s"
        _stage_store(root)
        for nodes in ([], ()):
            with pytest.raises(ValueError, match="empty node set"):
                run_stage_worker(
                    str(root),
                    [(morton_word(d), None) for d in LEAVES],
                    run_id="F",
                    run_started=RUN_STARTED,
                    dispatch=0,
                    nodes=nodes,
                )
        # Refused before admission and before any fold: no lease, no artifacts.
        assert read_lease(str(root)) is None
        assert not (root / "1" / "all.zarr").exists()
        assert not (root / "-2" / "all.zarr").exists()

    def test_a_node_off_the_dispatch_order_refuses_the_whole_invoke(self, tmp_path):
        from zagg.sweep_lease import read_lease

        root = tmp_path / "s"
        _stage_store(root)
        # "111" is an order-3 node; handed at dispatch 0 it admits its ancestor
        # base cell "1" whole (containment resolves both ways), so this invoke
        # would quietly fold four times its share.
        with pytest.raises(ValueError, match=r"not at order 0: \['111'\]"):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                run_started=RUN_STARTED,
                dispatch=0,
                nodes=["1", "111"],
            )
        # The whole invoke refuses — the good node in the same set is not swept.
        assert read_lease(str(root)) is None
        assert not (root / "1" / "all.zarr").exists()

    def test_a_sibling_worker_of_the_same_run_is_admitted(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        for node in ("1", "-2"):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                run_started=RUN_STARTED,
                dispatch=0,
                nodes=[node],
            )
        assert (root / "1" / "all.zarr").exists() and (root / "-2" / "all.zarr").exists()

    def test_a_foreign_live_lease_refuses_the_invoke(self, tmp_path):
        from zagg.sweep_lease import SweepRefusedError, acquire_lease

        root = tmp_path / "s"
        _stage_store(root)
        acquire_lease(str(root), run_id="live-runner")
        with pytest.raises(SweepRefusedError, match="live-runner"):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                run_started=RUN_STARTED,
                dispatch=0,
                nodes=["1"],
            )

    def test_a_foreign_fresh_stamp_aborts_the_invoke(self, tmp_path):
        import zarr

        from zagg.store import open_store
        from zagg.sweep_stage import ForeignSweepError

        root = tmp_path / "s"
        _stage_store(root)
        run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
        )
        g = zarr.open_group(
            open_store(str(root / "1" / "1" / "1" / "all.zarr")), path="", mode="r+", zarr_format=3
        )
        stamp = dict(g.attrs["morton_hive_commit"])
        stamp["run_id"] = "zombie"
        stamp["written_at"] = "2999-01-01T00:00:00+00:00"
        g.attrs["morton_hive_commit"] = stamp
        # Same run id: this is a live sibling of the run that already holds the
        # lease, so admission passes and the STAMP is what has to catch it.
        with pytest.raises(ForeignSweepError, match="zombie"):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                run_started=RUN_STARTED,
                dispatch=0,
                nodes=["1"],
            )

    def test_the_run_id_skip_key_makes_a_reinvoke_current(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        first = run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
        )
        again = run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
        )
        assert first["stages"][0]["written"] > 0
        assert again["stages"][0]["written"] == 0 and again["stages"][0]["current"] > 0

    def test_the_record_lands_at_the_status_prefix(self, tmp_path):
        root = tmp_path / "s"
        _stage_store(root)
        prefix = tmp_path / "status"
        record = run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
            batch=2,
            records_from=str(prefix),
        )
        name = stage_record_name(0, 2)
        assert name == "stage-00-0002.json"
        on_disk = json.loads((prefix / name).read_text())
        assert on_disk["spec"] == STAGE_RECORD_SPEC and on_disk["role"] == "stage"
        assert on_disk["level_actuals"] and record["record"].endswith(name)

    def test_a_stage_worker_never_touches_store_root_singletons(self, tmp_path):
        from zagg.hive import MANIFEST_NAME

        root = tmp_path / "s"
        _stage_store(root)
        before = {
            name: (root / name).read_bytes()
            for name in (MANIFEST_NAME, "coverage.moc")
            if (root / name).exists()
        }
        assert set(before) == {MANIFEST_NAME, "coverage.moc"}
        run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1", "-2"],
        )
        # The finisher owns the manifest RMW and the root MOC refresh; a stage
        # worker writing either would breach the singleton single-writer law.
        # (The lease IS a store-root object, but it is control plane, not data.)
        assert {name: (root / name).read_bytes() for name in before} == before


class TestFinisherArm:
    def _sweep_all(self, root, prefix, *, run_id="F"):
        for batch, node in enumerate(("1", "-2")):
            run_stage_worker(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id=run_id,
                run_started=RUN_STARTED,
                dispatch=0,
                nodes=[node],
                batch=batch,
                records_from=str(prefix),
            )

    def test_the_finisher_aggregates_the_records_and_releases(self, tmp_path):
        from zagg.hive import read_manifest, read_root_coverage
        from zagg.sweep_lease import read_lease

        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["stage_records"] == 2 and summary["transport"] == "lambda"
        assert summary["lease"]["released"] and read_lease(str(root)) is None
        assert read_root_coverage(str(root))["source"] == "sweep"
        entries = {e["node"]: e for e in read_manifest(str(root))["pyramid"]["overviews"]}
        # Node 0's two base-cell artifacts, summed ONCE across the two invokes.
        assert entries[0]["actuals"]["source_children"] == {
            "folded": 4,
            "missing": 0,
            "unreadable": 0,
        }
        assert (prefix / FINISHER_RECORD_NAME).exists()
        assert json.loads((root / summary["record"]).read_text())["mode"] == "stages"

    def test_the_finisher_refuses_under_a_foreign_lease(self, tmp_path):
        # The fan-out outlived the TTL and a foreign sweep claimed the store.
        # The finisher writes the root singletons, so it must refuse rather
        # than do the manifest RMW alongside the claimant's own finisher.
        from zagg.hive import read_manifest
        from zagg.sweep_lease import SweepRefusedError, acquire_lease, release_lease

        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        assert release_lease(str(root), run_id="F")
        acquire_lease(str(root), run_id="foreign-runner")
        before = read_manifest(str(root))
        with pytest.raises(SweepRefusedError, match="foreign-runner"):
            run_stage_finisher(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                records_from=str(prefix),
            )
        assert read_manifest(str(root)) == before
        assert not (prefix / FINISHER_RECORD_NAME).exists()

    def test_the_finisher_re_admits_its_own_run(self, tmp_path):
        # The idempotent half: the run's own live intent is re-read, not refused.
        from zagg.sweep_lease import read_lease

        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        assert read_lease(str(root))["run_id"] == "F"
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["lease"]["released"] and read_lease(str(root)) is None

    def test_a_finisher_without_records_from_refuses(self, tmp_path):
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        with pytest.raises(ValueError, match="no records_from"):
            run_stage_finisher(str(root), [(morton_word(d), None) for d in LEAVES], run_id="F")

    def test_a_finisher_over_zero_records_refuses(self, tmp_path):
        # The fan-out was lost (or the prefix is wrong): stamping the manifest
        # with no actuals at all is the silent under-report this channel exists
        # to prevent, so it refuses instead of reporting a clean finish.
        from zagg.hive import read_manifest
        from zagg.sweep_lease import read_lease

        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        empty = tmp_path / "elsewhere"
        empty.mkdir()
        before = read_manifest(str(root))
        with pytest.raises(ValueError, match="no stage records"):
            run_stage_finisher(
                str(root),
                [(morton_word(d), None) for d in LEAVES],
                run_id="F",
                records_from=str(empty),
            )
        assert read_manifest(str(root)) == before
        assert not (empty / FINISHER_RECORD_NAME).exists()
        assert read_lease(str(root))["run_id"] == "F"  # failure leaves it HELD

    def test_a_partial_record_set_still_finishes(self, tmp_path):
        # Only ZERO refuses: under-coverage from a lost batch is recorded and
        # heals on the next run (#381 point (6)).
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        (prefix / stage_record_name(0, 1)).unlink()
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["stage_records"] == 1 and summary["lease"]["released"]
        # 4 with both records (see above); the lost batch under-reports, loudly.
        assert summary["levels"]["0"]["source_children"]["folded"] == 3

    def test_a_prior_attempts_record_cannot_win_the_merge(self, tmp_path):
        # Same prefix, same id, a dead attempt's higher-numbered batch: the
        # merge ASSIGNS in sorted-name order, so an unfiltered read would let
        # ``stage-00-0009`` overwrite the fresh rows with stale counts.
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        stale = json.loads((prefix / stage_record_name(0, 0)).read_text())
        stale["run_id"] = "E"  # the earlier attempt
        for level in stale["level_actuals"].values():
            for row in level["children"].values():
                row["folded"] = 99
        (prefix / stage_record_name(0, 9)).write_text(json.dumps(stale))
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["stage_records"] == 2  # the stale one is not one of ours
        assert summary["levels"]["0"]["source_children"]["folded"] == 4

    def test_read_stage_records_filters_on_run_id(self, tmp_path):
        from zagg.sweep_stages import read_stage_records

        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        stale = json.loads((prefix / stage_record_name(0, 0)).read_text())
        stale["run_id"] = "E"
        (prefix / stage_record_name(0, 9)).write_text(json.dumps(stale))
        assert [r["batch"] for r in read_stage_records(str(prefix), run_id="F")] == [0, 1]
        assert [r["run_id"] for r in read_stage_records(str(prefix), run_id="E")] == ["E"]

    def test_merge_is_idempotent_over_reinvoked_batches(self, tmp_path):
        # A re-fired batch (Event retry) rewrites its own record; the merge is
        # keyed per (artifact node, window) and ASSIGNED, so coverage counts
        # cannot inflate.
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        self._sweep_all(root, prefix)
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["levels"]["0"]["source_children"]["folded"] == 4

    def test_unreadable_record_is_skipped_not_fatal(self, tmp_path):
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        self._sweep_all(root, prefix)
        (prefix / stage_record_name(0, 1)).write_text("{not json")
        summary = run_stage_finisher(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            records_from=str(prefix),
        )
        assert summary["stage_records"] == 1  # the survivor; under-report, not abort
        assert summary["lease"]["released"]

    def test_merge_level_actuals_first_wins_on_level_metadata(self):
        # The merge runs in sorted record-NAME order, which is a dispatcher
        # batching artifact, so the level metadata must not be last-wins: the
        # second worker here disagrees on all three values and loses. (They
        # cannot legitimately disagree — every worker derives them per level
        # via ``classify_level`` — which is exactly why the tie is pinned.)
        target: dict = {}
        merge_level_actuals(
            target,
            {"2": {"cells": 3, "regime": "stage-gather", "merges_from_raw": 1, "children": {}}},
        )
        merge_level_actuals(
            target,
            {
                "2": {
                    "cells": 999,
                    "regime": "stage-merge",
                    "merges_from_raw": 7,
                    "children": {"111|all": {"folded": 2, "missing": 0, "unreadable": 0}},
                }
            },
        )
        assert target[2]["cells"] == 3
        assert target[2]["regime"] == "stage-gather"
        assert target[2]["merges_from_raw"] == 1
        # The per-(node, window) rows still merge in from both.
        assert target[2]["children"] == {"111|all": {"folded": 2, "missing": 0, "unreadable": 0}}


class TestHandlerStageArm:
    def test_stage_role_round_trips_over_the_event(self, tmp_path):
        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        response = mod.lambda_handler(
            _event(root, _stage_block(0, ["1"], records_from=prefix)), None
        )
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["ok"] and body["stage"] == "stage" and body["run_id"] == "F"
        assert body["n_leaves"] == len(LEAVES) and body["duration_s"] >= 0.0
        assert body["dispatch"] == 0 and body["batch"] == 0 and body["n_nodes"] == 1
        assert body["written"] > 0 and body["failed"] == 0
        # The record is durable at the status prefix; the envelope names it and
        # carries no rows (``record`` is the store-root run record — finisher only).
        assert body["record"] is None
        assert body["stage_record"].endswith(stage_record_name(0, 0))
        assert (prefix / stage_record_name(0, 0)).exists()

    def test_finisher_role_round_trips_over_the_event(self, tmp_path):
        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        mod.lambda_handler(_event(root, _stage_block(0, ["1", "-2"], records_from=prefix)), None)
        response = mod.lambda_handler(
            _event(
                root,
                {
                    "role": "finisher",
                    "run_id": "F",
                    "records_from": str(prefix),
                    "touch_policy": "auto",
                },
            ),
            None,
        )
        body = json.loads(response["body"])
        assert response["statusCode"] == 200 and body["stage"] == "finisher"
        assert body["lease_released"] and body["stage_records"] == 1
        assert body["record"].startswith("sweep_stats_")
        assert body["stage_record"].endswith(FINISHER_RECORD_NAME)

    def test_discovery_transport_is_shared_with_the_families_arm(self, tmp_path):
        import pandas as pd

        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        pd.DataFrame(
            {
                "shard_key": pd.array([morton_word(d) for d in LEAVES], dtype="UInt64"),
                "success": [True] * len(LEAVES),
                "window": [None] * len(LEAVES),
            }
        ).to_parquet(root / "stats_20260825T000000Z_test.parquet", engine="fastparquet")
        event = {
            "mode": "sweep",
            "store_path": str(root),
            "stage": _stage_block(0, ["1"], records_from=prefix),
        }
        body = json.loads(mod.lambda_handler(event, None)["body"])
        assert body["ok"] and body["n_leaves"] == len(LEAVES)
        assert body["discover_s"] >= 0.0  # the work set was derived, not shipped

    def test_a_failed_stage_invoke_reports_500(self, tmp_path):
        from zagg.sweep_lease import acquire_lease

        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        acquire_lease(str(root), run_id="live-runner")
        response = mod.lambda_handler(
            _event(root, _stage_block(0, ["1"], records_from=prefix)), None
        )
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error_class"] == "SweepRefusedError" and "live-runner" in body["error"]
        assert not (prefix / stage_record_name(0, 0)).exists()

    def test_a_stage_event_without_nodes_refuses_by_name(self, tmp_path):
        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        block = _stage_block(0, ["1"], records_from=prefix)
        del block["nodes"]
        response = mod.lambda_handler(_event(root, block), None)
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error_class"] == "ValueError" and "empty node set" in body["error"]
        assert not (prefix / stage_record_name(0, 0)).exists()

    def test_an_unknown_role_refuses_by_name(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _stage_store(root)
        response = mod.lambda_handler(_event(root, {"role": "nope", "run_id": "F"}), None)
        assert response["statusCode"] == 500
        assert "unknown stage role" in json.loads(response["body"])["error"]

    def test_the_envelope_is_scalars_and_one_fixed_key_set(self, tmp_path):
        # A stage record is one row per (artifact node, window); a finest-tuple
        # batch is megabytes of them, so echoing it would fail a SUCCEEDED
        # invoke on the 6 MB response cap. Only scalars cross the wire, and the
        # key set does not move with the role or the outcome.
        mod = _handler_module()
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root)
        stage = json.loads(
            mod.lambda_handler(
                _event(root, _stage_block(0, ["1", "-2"], records_from=prefix)), None
            )["body"]
        )
        finisher = json.loads(
            mod.lambda_handler(
                _event(root, {"role": "finisher", "run_id": "F", "records_from": str(prefix)}),
                None,
            )["body"]
        )
        failed = json.loads(
            mod.lambda_handler(_event(root, {"role": "nope", "run_id": "F"}), None)["body"]
        )
        assert set(stage) == set(finisher) == set(failed)
        assert all(not isinstance(v, (dict, list)) for v in stage.values()), (
            "the envelope carries scalars only"
        )
        assert "level_actuals" not in stage and "result" not in stage
        assert stage["error"] is None and failed["ok"] is False

    def test_the_wire_default_tuple_width_is_the_single_source(self, tmp_path, monkeypatch):
        # ``stage_tuples``' grouping decides which orders dispatch, hence which
        # nodes get a stage column at all; a literal here would drift from the
        # CLI path the moment the constant moves.
        import zagg.sweep_stage as sweep_stage
        import zagg.sweep_stages as sweep_stages

        mod = _handler_module()
        root = tmp_path / "s"
        _stage_store(root)
        seen = {}
        real = sweep_stages.run_stage_worker

        def _spy(*args, **kwargs):
            seen.update(kwargs)
            return real(*args, **kwargs)

        monkeypatch.setattr(sweep_stages, "run_stage_worker", _spy)
        block = _stage_block(0, ["1"])
        del block["tuple_width"]
        assert mod.lambda_handler(_event(root, block), None)["statusCode"] == 200
        assert seen["tuple_width"] == sweep_stage.DEFAULT_TUPLE_WIDTH

    def test_the_families_arm_is_untouched_without_a_stage_block(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _stage_store(root)
        body = json.loads(
            mod.lambda_handler(
                {"mode": "sweep", "store_path": str(root), "leaves": _leaf_refs()}, None
            )["body"]
        )
        assert "families" in body and "stage" not in body


class TestUnderCoverageHeals(object):
    def test_a_missing_child_under_covers_then_heals_over_the_wire(self, tmp_path):
        root, prefix = tmp_path / "s", tmp_path / "status"
        _stage_store(root, skip_columns={"1121"})
        record = run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="F",
            run_started=RUN_STARTED,
            dispatch=0,
            nodes=["1"],
            records_from=str(prefix),
        )
        assert record["stages"][0]["under_covered"] > 0
        attrs = dict(_artifact(root, "1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 2, "missing": 1, "unreadable": 0}
        _write_leaf(root, "1121", 2)
        # A second RUN: run F's finisher released the lease as its final act,
        # and the dispatcher pins the new run's start stamp at dispatch time —
        # run F's artifacts are then a COMPLETED prior sweep's, not a live
        # sibling's, which is what keeps the foreign-fresh backstop quiet.
        from zagg.hive import _utcnow
        from zagg.sweep_lease import release_lease

        assert release_lease(str(root), run_id="F")
        run_stage_worker(
            str(root),
            [(morton_word(d), None) for d in LEAVES],
            run_id="G",
            run_started=_utcnow(),
            dispatch=0,
            nodes=["1"],
            records_from=str(prefix),
        )
        attrs = dict(_artifact(root, "1/all.zarr").attrs)["zagg_overview"]
        assert attrs["source_children"] == {"folded": 3, "missing": 0, "unreadable": 0}
