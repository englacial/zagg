"""Issue #434: the ``zagg-pyramid/2`` arm of the E2E validation harness.

Builds a small local ``/2`` hive store the way the #547 campaign will (the
espg ruling of 2026-09-11: declare ``/2`` -> issue #520 column backfill ->
staged sweep), using the REAL writers end to end — leaves shaped like the
production ATL03 target (count + two strata t-digests + the packed
composition word, ``tests/test_pyramid_e2e.py``'s declaration), then
``backfill_columns`` and ``run_stage_sweep`` — and drives
``zagg.pyramid_check`` through the ``/2`` checklist: declaration grammar,
ladder materialization, the §4.6 leaf-column tier, ``zagg-overview/2``
provenance (derived regime / merges-from-raw / source_children), count
conservation and digest/composition fold-correctness re-folded from the
gen-1 columns, column-vs-leaf parity, and the staged-sweep idempotency
ratchet. The negative tests corrupt the swept store and require the harness
to CATCH each break — the acceptance gate must be able to fail.

Geometry: shard order 3, cell order 6, declared leaf resolution [5]
(``d = 2``), so the fixed ladder exercises all three level shapes —
``(2, [4])`` a stage-GATHER above the shard order, ``(1, [3])`` a gather of
the node-order members, ``(0, [2])`` a stage-MERGE of the relayed partials.
"""

import json

import numpy as np
import obstore
import zarr
from mortie import generate_morton_children

# The leaf shape (fields + synthetic strata cells) is the /1 suite's — the
# production ATL03 declaration at test scale; only the geometry differs here.
from test_pyramid_e2e import FIELDS, _cells, _leaf_cfg

from zagg.column_backfill import backfill_columns
from zagg.grids.healpix import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.hive import (
    MANIFEST_NAME,
    build_root_coverage,
    shard_leaf_path,
    stamp_commit,
    write_root_coverage,
)
from zagg.pyramid import expand_overviews
from zagg.pyramid_check import CHECKS_V2, format_report, main, validate_pyramid
from zagg.store import open_object_store, open_store
from zagg.sweep_overview import encode_digest
from zagg.sweep_stages import run_stage_sweep

SHARD_ORDER = 3
CELL_ORDER = 6
LEAF_CELLS = 4 ** (CELL_ORDER - SHARD_ORDER)

#: Two bases; the ladder materializes 4 nodes at o2, 3 at o1, 2 at o0.
LEAVES = ["-3111", "-3112", "-3121", "-3211", "-4111"]


def _write_leaf(root, dec, per_cell):
    grid = HealpixGrid(SHARD_ORDER, CELL_ORDER, config=_leaf_cfg())
    word = morton_word(dec)
    store = open_store(shard_leaf_path(str(root), word))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(CELL_ORDER), mode="r+", zarr_format=3)
    assert len(per_cell) == LEAF_CELLS
    group["morton"][:] = np.asarray(generate_morton_children(word, CELL_ORDER), dtype=np.uint64)
    group["count"][:] = np.array([c["n_signal"] + c["n_noise"] for c in per_cell], dtype=np.int32)
    for field, key in (("h_sig", "sig"), ("h_noise", "noise")):
        slab = np.full(LEAF_CELLS, b"", dtype=object)
        for i, c in enumerate(per_cell):
            slab[i] = c[key]
        group[field][:] = slab
    group["composition"][:] = np.array([c["word"] for c in per_cell], dtype=np.uint64)
    stamp_commit(store, cells_with_data=sum(1 for c in per_cell if c["n_signal"]), granule_count=1)


def _build_store(root, *, leaves=LEAVES, backfill=True, sweep=True, backfill_only=()):
    """The #547 campaign sequence on a fixture store; returns the manifest.

    ``backfill_only`` restricts the column backfill to a subset of leaves —
    the under-coverage arm (a sweep over a store whose backfill has not
    reached every leaf must record it, never guess through it).
    """
    for i, dec in enumerate(leaves):
        _write_leaf(root, dec, _cells(LEAF_CELLS, 40, seed=100 + i))
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": CELL_ORDER,
        "shard_order": SHARD_ORDER,
        "split_schedule": [1] * SHARD_ORDER,
        "pyramid": {
            "spec": "zagg-pyramid/2",
            "overviews": expand_overviews([5], parent_order=SHARD_ORDER),
            "overview": {
                "all_time": False,
                "fold_source": "cascade",
                "exact_levels": 1,
                "fields": {k: dict(v) for k, v in FIELDS.items()},
            },
        },
        "generated_at": "2026-01-01T00:00:00+00:00",
    }
    obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())
    write_root_coverage(
        str(root),
        build_root_coverage([morton_word(d) for d in leaves], SHARD_ORDER, source="dispatcher"),
    )
    if backfill:
        subset = list(backfill_only) or list(leaves)
        counts = backfill_columns(str(root), manifest, {d: {None} for d in subset})
        assert counts["failed"] == 0 and counts["written"] == len(subset), counts
    if sweep:
        summary = run_stage_sweep(str(root), [[morton_word(d), None] for d in leaves])
        written = sum(row["written"] for row in summary["stages"])
        failed = sum(row["failed"] for row in summary["stages"])
        assert failed == 0 and written > 0, summary
        assert summary["lease"]["released"] is True
    return manifest


def _node_group(root, node_rel, order, mode="r+"):
    store = open_store(f"{root}/{node_rel}/all.zarr")
    return zarr.open_group(store, path=str(order), mode=mode, zarr_format=3)


def _column_group(root, leaf_rel_dir, order, mode="r+"):
    store = open_store(f"{root}/{leaf_rel_dir}/all.pyramid.zarr")
    return zarr.open_group(store, path=str(order), mode=mode, zarr_format=3)


class TestV2FixtureE2E:
    """Declare /2 → backfill → staged sweep → the full checklist passes."""

    def test_full_pipeline_passes_every_check(self, tmp_path):
        _build_store(tmp_path)
        report = validate_pyramid(str(tmp_path), full=True, resweep=True)
        assert [report["checks"][c]["status"] for c in CHECKS_V2] == ["pass"] * len(CHECKS_V2), (
            format_report(report)
        )
        assert report["passed"] is True
        assert report["pyramid_spec"] == "zagg-pyramid/2"
        assert report["leaf_levels"] == [5]
        # Declaration ↔ materialized: the fixed ladder is fully populated ...
        assert report["nodes"] == {
            "2": {"declared": 4, "materialized": 4},
            "1": {"declared": 3, "materialized": 3},
            "0": {"declared": 2, "materialized": 2},
        }
        # ... and so is the §4.6 leaf-column tier.
        assert report["columns"] == {"declared": 5, "materialized": 5}
        assert report["roster"] == {"source": "coverage.moc", "leaves": len(LEAVES)}
        assert report["missing_nodes"] == [] and report["missing_columns"] == []
        printed = format_report(report)
        for name in CHECKS_V2:
            assert name in printed
        assert printed.endswith("VERDICT: PASS")

    def test_sampled_mode_matches_full(self, tmp_path):
        _build_store(tmp_path)
        report = validate_pyramid(
            str(tmp_path), sample_nodes=2, sample_cells=3, seed=7, roster="list"
        )
        full = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is True, format_report(report)
        assert [report["checks"][c]["status"] for c in CHECKS_V2] == [
            full["checks"][c]["status"] for c in CHECKS_V2
        ], (format_report(report), format_report(full))
        assert report["nodes"] == full["nodes"]
        assert report["columns"] == full["columns"]
        for name in ("readback", "counts", "digests", "composition"):
            assert 0 < report["sampled"][name] <= full["sampled"][name], name
        assert report["roster"]["source"] == "list"

    def test_cli_exit_codes(self, tmp_path, capsys):
        _build_store(tmp_path)
        out_json = tmp_path / "report.json"
        assert main([str(tmp_path), "--full", "--json", str(out_json)]) == 0
        out = capsys.readouterr().out
        assert "VERDICT: PASS" in out and "columns" in out
        assert json.loads(out_json.read_text())["passed"] is True

    def test_wide_column_group_declines_payload_parity_in_sampled_mode(self, tmp_path, monkeypatch):
        # The §4.6 parity leg's span is the GEOMETRY's (one cell of the group
        # at q covers 4**(cell_order - q) leaf cells), so sample_cells cannot
        # bound it — a coarse group re-folds a whole leaf per sampled leaf.
        # Above the bound the payload legs are declined and NAMED; counts
        # (dense) still run, and full mode is never bounded.
        from zagg import pyramid_check_v2

        _build_store(tmp_path)
        monkeypatch.setattr(pyramid_check_v2, "COLUMN_PARITY_FOLD_MAX", 4)
        report = validate_pyramid(str(tmp_path), sample_nodes=2, sample_cells=3, seed=7)
        assert report["passed"] is True, format_report(report)
        declined = [w for w in report.get("warnings") or [] if "column group [3]" in w]
        assert declined, format_report(report)
        assert "64 leaf cells" in declined[0] and "counts still compared" in declined[0]
        assert report["sampled"]["counts"] > 0
        # ... and the bound does not apply in full (fixture) mode.
        full = validate_pyramid(str(tmp_path), full=True)
        assert full["passed"] is True, format_report(full)
        assert not any("column group [" in w for w in full.get("warnings") or [])

    def test_gather_levels_carry_gen1_bytes(self, tmp_path):
        # The acceptance contract the harness leans on: a gather level's cell
        # IS the leaf column's cell, assigned — pin it directly so the
        # gather-vs-merge expectation split rests on a demonstrated fact.
        _build_store(tmp_path)
        node = _node_group(tmp_path, "-3/1/1", 4, mode="r")
        column = _column_group(tmp_path, "-3/1/1/1", 4, mode="r")
        assert np.array_equal(node["count"][0:4], column["count"][0:4])
        assert bytes(node["h_sig"][0:1][0] or b"") == bytes(column["h_sig"][0:1][0] or b"")
        assert int(node["composition"][0]) == int(column["composition"][0])


class TestV2Baselines:
    """The campaign's intermediate states report cleanly, never raise."""

    def test_declared_and_backfilled_but_unswept(self, tmp_path):
        # The post-backfill gate reading: ladder 0/N (pre-sweep baseline),
        # columns N/N — exactly the state the #547 runbook checks between
        # steps. The /1 arm's baseline sentence is preserved verbatim.
        _build_store(tmp_path, sweep=False)
        report = validate_pyramid(str(tmp_path))
        assert report["checks"]["declaration"]["status"] == "pass"
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "declared but unmaterialized: 0/9 nodes" in entry["detail"]
        assert "pre-sweep baseline" in entry["detail"]
        assert report["checks"]["columns"]["status"] == "pass"
        assert report["columns"] == {"declared": 5, "materialized": 5}
        for name in ("readback", "counts", "digests", "composition"):
            assert report["checks"][name]["status"] == "skip"
        assert report["passed"] is False

    def test_declared_but_not_backfilled(self, tmp_path):
        # Pre-backfill: no columns at all — a DISTINCT baseline sentence, so
        # the runbook can tell "backfill has not run" from "sweep has not run".
        _build_store(tmp_path, backfill=False, sweep=False)
        report = validate_pyramid(str(tmp_path))
        assert report["checks"]["materialization"]["status"] == "fail"
        entry = report["checks"]["columns"]
        assert entry["status"] == "fail"
        assert "0/5 — pre-backfill baseline" in entry["detail"]
        assert report["passed"] is False

    def test_malformed_declaration_fails_by_name(self, tmp_path):
        # A /2 list that is not leaf entry + fixed every-order ladder (§4.4)
        # fails DECLARATION loudly — never widened into a plausible schedule.
        _build_store(tmp_path, backfill=False, sweep=False)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["pyramid"]["overviews"] = [{"node": 1, "cells": [3]}, {"node": 0, "cells": [2]}]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        report = validate_pyramid(str(tmp_path))
        entry = report["checks"]["declaration"]
        assert entry["status"] == "fail"
        assert "malformed zagg-pyramid/2 declaration" in entry["detail"]
        assert "leaf entry" in entry["detail"]
        assert report["passed"] is False

    def test_non_expanded_ladder_fails_declaration(self, tmp_path):
        # Every order from shard_order - 1 to 0 must be recorded: a reader
        # never re-derives the ladder, so a gap is a broken contract.
        _build_store(tmp_path, backfill=False, sweep=False)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["pyramid"]["overviews"] = [
            e for e in manifest["pyramid"]["overviews"] if e["node"] != 1
        ]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        report = validate_pyramid(str(tmp_path))
        entry = report["checks"]["declaration"]
        assert entry["status"] == "fail"
        assert "fixed every-order ladder" in entry["detail"]


class TestV2Provenance:
    """zagg-overview/2 attrs are held to §4.4, not taken on faith."""

    def _edit_node_attrs(self, tmp_path, node_rel, mutate):
        node_meta = tmp_path / node_rel / "all.zarr" / "zarr.json"
        meta = json.loads(node_meta.read_text())
        mutate(meta["attributes"])
        node_meta.write_text(json.dumps(meta))

    def test_wrong_regime_is_caught(self, tmp_path):
        # A gather level stamped as a merge: the regime is DERIVED from the
        # geometry (classify_level), so a disagreeing stamp is a broken write.
        _build_store(tmp_path)
        self._edit_node_attrs(
            tmp_path, "-3/1/1", lambda a: a["zagg_overview"].update(regime="stage-merge")
        )
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("regime" in m and "stage-merge" in m for m in entry["mismatches"])

    def test_gen3_merges_from_raw_is_caught(self, tmp_path):
        # Never 3 for an upfront level (§4.4): gen 3 belongs only to the
        # append-later cascade regime.
        _build_store(tmp_path)
        self._edit_node_attrs(
            tmp_path, "-3", lambda a: a["zagg_overview"].update(merges_from_raw=3)
        )
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("merges_from_raw 3" in m for m in entry["mismatches"])

    def test_missing_source_children_is_caught(self, tmp_path):
        _build_store(tmp_path)
        self._edit_node_attrs(tmp_path, "-3", lambda a: a["zagg_overview"].pop("source_children"))
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("source_children" in m for m in entry["mismatches"])

    def test_under_covered_level_is_declined_not_false_failed(self, tmp_path):
        # One leaf's column never backfilled: the sweep folds short and says
        # so (source_children.missing). The harness must DECLINE those nodes'
        # value checks — a fill cell there is not evidence (§4.3) — while the
        # columns check carries the actual failure. Comparing values against
        # the now-fuller tier would false-fail a sweep that wrote correctly.
        _build_store(tmp_path, backfill_only=LEAVES[1:])
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False
        assert report["checks"]["columns"]["status"] == "fail"
        assert "4/5" in report["checks"]["columns"]["detail"]
        assert any("under-covers" in w for w in report["warnings"]), report.get("warnings")
        for name in ("counts", "digests", "composition"):
            assert report["checks"][name]["status"] == "pass", (
                name,
                format_report(report),
            )
        # The decline is scoped to the per-cell comparisons: the declined
        # nodes are still READ BACK (role/provenance/arrays/morton/§3.3).
        assert report["checks"]["readback"]["status"] == "pass", format_report(report)

    def test_stamped_under_coverage_cannot_hide_a_corruption(self, tmp_path):
        # The adversarial direction of the test above: source_children is
        # written by the artifact about ITSELF, so a corrupt ladder that
        # stamps missing:1 on every node must not decline its way to PASS.
        # Every source column here IS committed, so the stamp is stale — a
        # read-back failure — and the value checks run and catch the counts.
        _build_store(tmp_path)
        for rel, order in (("-3/1/1", 4), ("-3/1", 3), ("-3", 2)):
            group = _node_group(tmp_path, rel, order)
            counts = group["count"][:]
            counts[int(np.flatnonzero(counts > 0)[0])] += 7
            group["count"][:] = counts
            self._stamp_under_coverage(tmp_path, rel)
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False, format_report(report)
        assert report["checks"]["counts"]["status"] == "fail", format_report(report)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("sources have since healed" in m for m in entry["mismatches"]), entry
        assert not any("under-covers its subtree (" in w for w in report.get("warnings") or [])

    def _stamp_under_coverage(self, tmp_path, node_rel):
        self._edit_node_attrs(
            tmp_path,
            node_rel,
            lambda a: a["zagg_overview"].update(
                source_children={"folded": 1, "missing": 1, "unreadable": 0}
            ),
        )

    def test_manifest_actuals_mismatch_is_caught(self, tmp_path):
        # The finisher's per-entry actuals (#381 point (7)) are bookkeeping a
        # reader may bind: a recorded regime that disagrees with the derived
        # law is a broken finisher, not tolerated additive keys.
        _build_store(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        for e in manifest["pyramid"]["overviews"]:
            if e["node"] == 0:
                e["actuals"]["regime"] = "stage-gather"
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("manifest actuals for node 0" in m for m in entry["mismatches"])

    def test_column_attrs_are_validated(self, tmp_path):
        # The §4.6 groups map: a column claiming a stage regime for a group
        # it folded from its own leaf is a broken write.
        _build_store(tmp_path)
        col_meta = tmp_path / "-3" / "1" / "1" / "1" / "all.pyramid.zarr" / "zarr.json"
        meta = json.loads(col_meta.read_text())
        meta["attributes"]["zagg_column"]["groups"]["3"]["regime"] = "stage-gather"
        col_meta.write_text(json.dumps(meta))
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["readback"]
        assert entry["status"] == "fail"
        assert any("leaf-column" in m for m in entry["mismatches"])


class TestV2Corruption:
    """Break the swept /2 store, see FAIL — at every level shape and tier."""

    def test_broken_count_at_a_gather_level(self, tmp_path):
        _build_store(tmp_path)
        group = _node_group(tmp_path, "-3/1/1", 4)  # o2, cells 4: gather (r > s)
        counts = group["count"][:]
        j = int(np.flatnonzero(counts > 0)[0])
        counts[j] += 1
        group["count"][:] = counts
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["counts"]["status"] == "fail"
        assert report["passed"] is False

    def test_broken_count_at_the_merge_level(self, tmp_path):
        _build_store(tmp_path)
        group = _node_group(tmp_path, "-3", 2)  # o0, cells 2: stage-merge
        counts = group["count"][:]
        j = int(np.flatnonzero(counts > 0)[0])
        counts[j] += 1
        group["count"][:] = counts
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["counts"]["status"] == "fail"
        assert all(m.startswith("-3[") for m in report["checks"]["counts"]["mismatches"]), report[
            "checks"
        ]["counts"]

    def test_broken_composition_word_is_caught_in_both_regimes(self, tmp_path):
        _build_store(tmp_path)
        for rel, order in (("-3/1/1", 4), ("-4", 2)):  # a gather and a merge
            group = _node_group(tmp_path, rel, order)
            words = group["composition"][:]
            j = int(np.flatnonzero(words > 0)[0])
            words[j] = int(words[j]) ^ (1 << 8)  # flip one packed lane bit
            group["composition"][:] = words
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["composition"]
        assert entry["status"] == "fail"
        assert {m.split("[")[0] for m in entry["mismatches"]} == {"-311", "-4"}

    def test_broken_digest_weight_is_caught(self, tmp_path):
        from zagg.sweep_overview import decode_digest

        _build_store(tmp_path)
        group = _node_group(tmp_path, "-3", 2)
        slab = group["h_sig"][:]
        j = next(i for i in range(len(slab)) if slab[i] is not None and len(slab[i]))
        digest = decode_digest(bytes(slab[j]), "float32").copy()
        digest[:, 1] *= 2  # double every centroid weight: conservation broken
        slab[j] = encode_digest(digest, "float32")
        group["h_sig"][:] = slab
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["digests"]["status"] == "fail"
        assert "weight" in report["checks"]["digests"]["detail"]

    def test_missing_node_object_is_caught(self, tmp_path):
        import shutil

        _build_store(tmp_path)
        shutil.rmtree(tmp_path / "-4" / "1" / "all.zarr")
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "8/9 declared nodes materialized" in entry["detail"]
        assert "-41" in report["missing_nodes"]

    def test_uncommitted_node_is_not_materialized(self, tmp_path):
        # The 08-25 debris shape: a bare node root group with empty attrs
        # counts as UNmaterialized, reported as partial (issue #547 forensics).
        _build_store(tmp_path)
        node_meta = tmp_path / "-4" / "1" / "all.zarr" / "zarr.json"
        meta = json.loads(node_meta.read_text())
        meta["attributes"] = {}
        node_meta.write_text(json.dumps(meta))
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "partial uncommitted" in entry["detail"]
        assert report["partial_nodes"] == ["-41"]

    def test_missing_column_after_sweep_is_caught(self, tmp_path):
        # A column deleted AFTER the sweep: the columns check fails, and the
        # ladder cells it fed are DECLINED (contributor unreadable), never
        # silently passed or false-failed.
        import shutil

        _build_store(tmp_path)
        shutil.rmtree(tmp_path / "-3" / "1" / "1" / "1" / "all.pyramid.zarr")
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["columns"]["status"] == "fail"
        assert "4/5" in report["checks"]["columns"]["detail"]
        assert "-3111" in report["missing_columns"]
        assert any("-3111 unreadable" in w for w in report["warnings"])
        assert report["passed"] is False

    def test_uncommitted_column_is_partial(self, tmp_path):
        from zagg.hive import COMMIT_ATTR

        _build_store(tmp_path)
        col_meta = tmp_path / "-3" / "1" / "1" / "1" / "all.pyramid.zarr" / "zarr.json"
        meta = json.loads(col_meta.read_text())
        meta["attributes"].pop(COMMIT_ATTR)
        col_meta.write_text(json.dumps(meta))
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["columns"]
        assert entry["status"] == "fail"
        assert "partial uncommitted column" in entry["detail"]
        assert report["partial_columns"] == ["-3111"]

    def test_broken_column_group_fails_leaf_parity(self, tmp_path):
        # Corrupt a column's node-order partial: the §4.6 from-leaves parity
        # catches it, and so does the merge level that consumed the original.
        _build_store(tmp_path)
        group = _column_group(tmp_path, "-3/1/1/1", 3)
        counts = group["count"][:]
        counts[0] += 5
        group["count"][:] = counts
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["counts"]["status"] == "fail"
        mismatches = report["checks"]["counts"]["mismatches"]
        assert any(m.startswith("-3111[") for m in mismatches), mismatches  # column vs leaf
        assert any(m.startswith("-3[") for m in mismatches), mismatches  # merge level vs column

    def test_stale_column_after_leaf_rewrite(self, tmp_path):
        # Base data moved, the column did not (the repair is the idempotent
        # backfill, §4.6): the column-vs-leaf parity leg must catch it.
        _build_store(tmp_path)
        _write_leaf(tmp_path, "-3111", _cells(LEAF_CELLS, 40, seed=999))
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False
        failing = {
            n
            for n in ("counts", "digests", "composition")
            if report["checks"][n]["status"] == "fail"
        }
        assert failing, format_report(report)

    def test_stale_ladder_after_rebackfill_without_resweep(self, tmp_path):
        # The E2E failure mode the gate exists for, /2 shape: leaf AND column
        # regenerated, ladder not re-swept — the ladder-vs-columns legs fail.
        _build_store(tmp_path)
        _write_leaf(tmp_path, "-3111", _cells(LEAF_CELLS, 40, seed=999))
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        counts = backfill_columns(str(tmp_path), manifest, {"-3111": {None}}, force=True)
        assert counts["written"] == 1, counts
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False
        failing = {
            n
            for n in ("counts", "digests", "composition")
            if report["checks"][n]["status"] == "fail"
        }
        assert failing, format_report(report)

    def test_blanked_node_does_not_pass(self, tmp_path):
        # A correctly-SHAPED but empty node must fail via the fill-side
        # presence law, in sampled mode too — never "0 checks, pass".
        _build_store(tmp_path)
        group = _node_group(tmp_path, "-3/1/1", 4)
        group["count"][:] = np.zeros_like(group["count"][:])
        group["composition"][:] = np.zeros_like(group["composition"][:])
        for name in ("h_sig", "h_noise"):
            slab = group[name][:]
            slab[:] = b""
            group[name][:] = slab
        for kwargs in ({"full": True}, {"sample_nodes": 99, "sample_cells": 8}):
            report = validate_pyramid(str(tmp_path), **kwargs)
            assert report["passed"] is not True, (kwargs, format_report(report))
            assert report["checks"]["counts"]["status"] == "fail", format_report(report)
