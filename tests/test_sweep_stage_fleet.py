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
  2026-08-09, issue #384) — see ``TestByteIdentityOracle``.
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
        target: dict = {}
        merge_level_actuals(
            target,
            {"2": {"cells": 3, "regime": "stage-gather", "merges_from_raw": 1, "children": {}}},
        )
        merge_level_actuals(
            target,
            {
                "2": {
                    "cells": 3,
                    "regime": "stage-gather",
                    "merges_from_raw": 1,
                    "children": {"111|all": {"folded": 2, "missing": 0, "unreadable": 0}},
                }
            },
        )
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
        assert body["result"]["stages"][0]["written"] > 0
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
        assert body["result"]["lease"]["released"]
        assert body["record"].startswith("sweep_stats_")

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

    def test_an_unknown_role_refuses_by_name(self, tmp_path):
        mod = _handler_module()
        root = tmp_path / "s"
        _stage_store(root)
        response = mod.lambda_handler(_event(root, {"role": "nope", "run_id": "F"}), None)
        assert response["statusCode"] == 500
        assert "unknown stage role" in json.loads(response["body"])["error"]

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
