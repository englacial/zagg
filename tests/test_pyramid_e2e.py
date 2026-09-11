"""Issue #434: overview-pipeline E2E validation harness, fixture mode.

Builds a small local hive store shaped like the production ATL03 target of
issue #547 — count + two strata t-digests + the packed composition word
(issue #515) — with a DENSE ``zagg-pyramid/1`` ladder, runs the real
``sweep_overviews`` pass, then drives ``zagg.pyramid_check`` end to end:
declaration ↔ materialized nodes, exact count conservation, digest k-way
fold-correctness, composition, and the skip-gate idempotency re-sweep
(issues #417/#421). The negative tests corrupt the swept store and require
the harness to CATCH it — an acceptance gate that cannot fail is a rubber
stamp. Leaf-writing mirrors ``tests/test_strata_composability.py`` (the CA
shape at test scale) at the sweep-harness geometry (shard order 2, cell
order 4).
"""

import json

import numpy as np
import obstore
import zarr
from mortie import generate_morton_children

from zagg.config import PipelineConfig
from zagg.grids.healpix import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.hive import (
    MANIFEST_NAME,
    build_root_coverage,
    shard_leaf_path,
    stamp_commit,
    write_root_coverage,
)
from zagg.pyramid_check import CHECKS, format_report, main, validate_pyramid
from zagg.stats.composition import pack_composition_n
from zagg.stats.tdigest import build_tdigest_where
from zagg.store import open_object_store, open_store
from zagg.sweep_overview import encode_digest, sweep_overviews

SHARD_ORDER = 2
CELL_ORDER = 4
LEAF_CELLS = 4 ** (CELL_ORDER - SHARD_ORDER)

#: Two bases, three parents — the dense ladder [1, 0] materializes 5 nodes.
LEAVES = ["-311", "-312", "-313", "-321", "-411"]

#: The manifest field declaration: the production ATL03 shape at test scale.
FIELDS = {
    "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
    "h_sig": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 64,
    },
    "h_noise": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 64,
    },
    "composition": {
        "class": "packed",
        "method": "composition_kway",
        "dtype": "uint64",
        "fill_value": 0,
        "of": "h_sig",
        "threshold": 2,
    },
}


def _leaf_cfg():
    where = (
        "((conf_land >= 2) | (conf_ocean >= 2) | (conf_sea_ice >= 2) "
        "| (conf_land_ice >= 2) | (conf_inland_water >= 2))"
    )
    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_sig": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest_where",
                    "inner_shape": [2],
                    "params": {"delta": 64, "where": where},
                    "dtype": "float32",
                    "fill_value": 0,
                },
                "h_noise": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest_where",
                    "inner_shape": [2],
                    "params": {"delta": 64, "where": f"~{where}"},
                    "dtype": "float32",
                    "fill_value": 0,
                },
                "composition": {
                    "function": "zagg.stats.composition.pack_composition",
                    "dtype": "uint64",
                    "fill_value": 0,
                    "params": {"threshold": 2},
                    "attrs": {"composition": {"of": "h_sig", "threshold": 2}},
                },
            },
        }
    )


def _cells(k, n, seed):
    """``k`` synthetic strata cells (a couple left empty), as leaf slabs."""
    rng = np.random.default_rng(seed)
    per_cell = []
    for i in range(k):
        if i % 7 == 5:  # empty cells exercise the fill path at every level
            per_cell.append({"sig": b"", "noise": b"", "word": 0, "n_signal": 0, "n_noise": 0})
            continue
        values = rng.normal(30.0, 5.0, n)
        conf = np.full((n, 5), -1, dtype=np.int64)
        n_sig = int(rng.integers(1, n - 1))
        for j in range(n_sig):
            conf[j, rng.integers(0, 5)] = rng.integers(2, 5)
        signal = (conf >= 2).any(axis=1)
        kwargs = {
            f"conf_{name}": conf[:, c]
            for c, name in enumerate(("land", "ocean", "sea_ice", "land_ice", "inland_water"))
        }
        word, n_signal = pack_composition_n(values, **kwargs, threshold=2)
        d_sig = build_tdigest_where(values, delta=64, where=signal)
        d_noise = build_tdigest_where(values, delta=64, where=~signal)
        per_cell.append(
            {
                "sig": encode_digest(d_sig, "float32"),
                "noise": encode_digest(d_noise, "float32"),
                "word": word,
                "n_signal": n_signal,
                "n_noise": int(d_noise[:, 1].sum()),
            }
        )
    return per_cell


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


def _build_store(root, *, orders=(1, 0), leaves=LEAVES):
    """Leaves + manifest (dense /1 ladder) + root coverage; returns the manifest."""
    for i, dec in enumerate(leaves):
        _write_leaf(root, dec, _cells(LEAF_CELLS, 40, seed=100 + i))
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": CELL_ORDER,
        "shard_order": SHARD_ORDER,
        "split_schedule": [1] * SHARD_ORDER,
        "pyramid": {
            "spec": "zagg-pyramid/1",
            "overview": {
                "spacing": 1,
                "orders": list(orders),
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
    return manifest


def _sweep(root, manifest, leaves=LEAVES):
    counts = sweep_overviews(str(root), manifest, {d: {None} for d in leaves})
    assert counts["failed"] == 0 and counts["written"] > 0, counts
    return counts


class TestFixtureE2E:
    """Build → sweep → validate: the full checklist passes on a clean store."""

    def test_full_pipeline_passes_every_check(self, tmp_path):
        manifest = _build_store(tmp_path)
        _sweep(tmp_path, manifest)
        report = validate_pyramid(str(tmp_path), full=True, resweep=True)
        assert [report["checks"][c]["status"] for c in CHECKS] == ["pass"] * len(CHECKS), (
            format_report(report)
        )
        assert report["passed"] is True
        # Declaration ↔ materialized: the dense ladder is fully populated.
        assert report["nodes"] == {
            "1": {"declared": 3, "materialized": 3},
            "0": {"declared": 2, "materialized": 2},
        }
        assert report["roster"] == {"source": "coverage.moc", "leaves": len(LEAVES)}
        assert report["missing_nodes"] == []
        # The checklist mirrors the issue #434 phases, one line per check.
        printed = format_report(report)
        for name in CHECKS:
            assert name in printed
        assert printed.endswith("VERDICT: PASS")

    def test_cli_exit_codes(self, tmp_path, capsys):
        manifest = _build_store(tmp_path)
        _sweep(tmp_path, manifest)
        out_json = tmp_path / "report.json"
        assert main([str(tmp_path), "--full", "--json", str(out_json)]) == 0
        assert "VERDICT: PASS" in capsys.readouterr().out
        assert json.loads(out_json.read_text())["passed"] is True

    def test_sampled_mode_matches_full(self, tmp_path):
        # Production posture on the fixture: bounded samples, roster listing.
        manifest = _build_store(tmp_path)
        _sweep(tmp_path, manifest)
        report = validate_pyramid(
            str(tmp_path), sample_nodes=2, sample_cells=3, seed=7, roster="list"
        )
        assert report["passed"] is True, format_report(report)
        assert report["roster"]["source"] == "list"
        assert report["roster"]["leaves"] == len(LEAVES)


class TestLadderGrammars:
    """The read-side follows either pyramid grammar's ladder."""

    def test_v1_constant_depth(self):
        from zagg.pyramid_check import _ladder

        manifest = {
            "shard_order": 9,
            "cell_order": 19,
            "pyramid": {"spec": "zagg-pyramid/1", "overview": {"orders": [7, 5, 3, 1]}},
        }
        assert _ladder(manifest) == [(7, 17), (5, 15), (3, 13), (1, 11)]

    def test_v2_expanded_list_from_the_ca_fixture(self):
        # The vendored live CA manifest (issue #515) declares the /2 dense
        # 9..0 ladder (fixed d = 13 - 9 = 4, the chunk-order default leaf
        # resolution); the above-shard read is the manifest list, never a
        # re-derivation — pinned against the published record.
        from pathlib import Path

        from zagg.pyramid_check import _ladder

        manifest = json.loads(
            (Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json").read_text()
        )
        assert _ladder(manifest) == [(k, k + 4) for k in range(8, -1, -1)]


class TestPreSweepBaseline:
    """Declared-but-unswept: the production baseline output, no exception."""

    def test_unmaterialized_reports_cleanly(self, tmp_path):
        _build_store(tmp_path)  # no sweep
        report = validate_pyramid(str(tmp_path))
        assert report["checks"]["declaration"]["status"] == "pass"
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "declared but unmaterialized: 0/5 nodes" in entry["detail"]
        assert "pre-sweep baseline" in entry["detail"]
        for name in ("readback", "counts", "digests", "composition"):
            assert report["checks"][name]["status"] == "skip"
        assert report["passed"] is False

    def test_no_declaration_reports_cleanly(self, tmp_path):
        # The GEDI shape today: a manifest with an EMPTY overview block.
        manifest = _build_store(tmp_path)
        manifest["pyramid"] = {"spec": "zagg-pyramid/1", "overview": {"orders": []}}
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        report = validate_pyramid(str(tmp_path))
        assert report["checks"]["declaration"]["status"] == "fail"
        assert "no pyramid overview declaration" in report["checks"]["declaration"]["detail"]
        assert report["passed"] is False

    def test_v1_era_none_classes_fail_declaration(self, tmp_path):
        # The pre-#515 CA declaration: ladder present, every field 'none'
        # except count... with ONLY count composable the ladder still folds;
        # strata declared 'none' means the store's substance never rolls up —
        # flagged via the recorded class map rather than a hard failure.
        manifest = _build_store(tmp_path)
        fields = manifest["pyramid"]["overview"]["fields"]
        for name in ("h_sig", "h_noise", "composition"):
            fields[name] = {"class": "none"}
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        report = validate_pyramid(str(tmp_path))
        assert report["field_classes"]["none"] == ["composition", "h_noise", "h_sig"]
        assert report["checks"]["digests"]["status"] == "skip"
        assert report["checks"]["composition"]["status"] == "skip"


class TestHarnessCatchesCorruption:
    """A gate that cannot fail is a rubber stamp: break the store, see FAIL."""

    def _swept(self, tmp_path):
        manifest = _build_store(tmp_path)
        _sweep(tmp_path, manifest)
        return manifest

    def _overview_group(self, root, node_rel, order, mode="r+"):
        store = open_store(f"{root}/{node_rel}/all.zarr")
        return zarr.open_group(store, path=str(order), mode=mode, zarr_format=3)

    def test_broken_count_fold_is_caught(self, tmp_path):
        self._swept(tmp_path)
        group = self._overview_group(tmp_path, "-3/1", 3)
        counts = group["count"][:]
        j = int(np.flatnonzero(counts > 0)[0])
        counts[j] += 1
        group["count"][:] = counts
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["counts"]["status"] == "fail"
        assert report["passed"] is False

    def test_broken_composition_word_is_caught(self, tmp_path):
        self._swept(tmp_path)
        group = self._overview_group(tmp_path, "-3/1", 3)
        words = group["composition"][:]
        j = int(np.flatnonzero(words > 0)[0])
        words[j] = int(words[j]) ^ (1 << 8)  # flip one packed lane bit
        group["composition"][:] = words
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["checks"]["composition"]["status"] == "fail"

    def test_broken_digest_weight_is_caught(self, tmp_path):
        from zagg.sweep_overview import decode_digest

        self._swept(tmp_path)
        group = self._overview_group(tmp_path, "-3/1", 3)
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

        self._swept(tmp_path)
        shutil.rmtree(tmp_path / "-4" / "1" / "all.zarr")
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "4/5 declared nodes materialized" in entry["detail"]
        assert "-41" in report["missing_nodes"]

    def test_partial_uncommitted_node_is_not_materialized(self, tmp_path):
        # The live-store shape found on 2026-09-10: a bare node root group
        # with empty attrs (debris of the aborted 08-25 sweep, issue #547
        # forensics) must count as UNmaterialized, reported as partial.
        self._swept(tmp_path)
        node_meta = tmp_path / "-4" / "1" / "all.zarr" / "zarr.json"
        meta = json.loads(node_meta.read_text())
        meta["attributes"] = {}
        node_meta.write_text(json.dumps(meta))
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "4/5 declared nodes materialized" in entry["detail"]
        assert "partial uncommitted" in entry["detail"]
        assert report["partial_nodes"] == ["-41"]

    def test_probe_error_is_not_the_pre_sweep_baseline(self, tmp_path):
        # A probe that fails for any reason other than not-found (here an
        # unparsable node zarr.json; in production a credential/throttle
        # error) must NOT read as "declared but unmaterialized ... pre-sweep
        # baseline" — that sentence is the report's most consequential claim.
        self._swept(tmp_path)
        (tmp_path / "-4" / "1" / "all.zarr" / "zarr.json").write_text("{not json")
        report = validate_pyramid(str(tmp_path), full=True)
        entry = report["checks"]["materialization"]
        assert entry["status"] == "fail"
        assert "probe error" in entry["detail"]
        assert "pre-sweep baseline" not in entry["detail"]
        assert any("-41" in e for e in report["probe_errors"])
        assert report["passed"] is False

    def test_stale_overview_after_leaf_change_is_caught(self, tmp_path):
        # The E2E failure mode the gate exists for: base data moved, ladder
        # did not. Rewrite one leaf with different observations, no re-sweep.
        self._swept(tmp_path)
        _write_leaf(tmp_path, "-311", _cells(LEAF_CELLS, 40, seed=999))
        report = validate_pyramid(str(tmp_path), full=True)
        assert report["passed"] is False
        failing = {
            n
            for n in ("counts", "digests", "composition")
            if report["checks"][n]["status"] == "fail"
        }
        assert failing, format_report(report)
