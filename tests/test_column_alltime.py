"""The windowed leaf's all-time column (issue #649).

Phase 1, the resident path: the bulk ``unit: shard`` invoke folds the window
columns it has just written into ``all.pyramid.zarr`` beside them, under the
sweep's close law — every member the k-way fold of the window members, cell
for cell, in ascending window order — and the member carries the §4.6
carve-out's provenance (``merges_from_raw`` one deeper than the window
members', ``source_windows``).
"""

import json
from pathlib import Path

import numpy as np
import pytest
import test_column as tc
import test_windowed_emit as emit
import zarr

from zagg import hive
from zagg.column import COLUMN_ATTR, fold_column
from zagg.column_alltime import _ResidentColumn, fold_all_time, write_all_time_column
from zagg.hive import COMMIT_ATTR
from zagg.store import open_store
from zagg.sweep_overview import decode_digest, fold_dense, fold_digests, overview_fold_delta
from zagg.windows import SCHEDULE_NONE_TOKEN

WINDOWS = ("2018", "2019", "2020")


def _window_folds(windows=WINDOWS, *, shard_order=tc.SHARD_ORDER, cell_order=tc.CELL_ORDER):
    """``{label: fold_column output}`` over three windows of distinct resident cells."""
    rng = np.random.default_rng(649)
    n_cells = 4 ** (cell_order - shard_order)
    out = {}
    for k, label in enumerate(windows):
        # Every window populates a different subset, with cells 0 and 1 shared
        # by all so the cross-window merge has real work at the finest group.
        cells = {0: rng.normal(k, 1, 7), 1: rng.normal(10 + k, 1, 5), 2 + k: rng.normal(0, 1, 3)}
        slabs = tc._cell_slabs(cells, n_cells)
        resolutions = list(range(cell_order, shard_order - 1, -1))
        out[label] = fold_column(
            slabs, tc.FIELDS, cell_order=cell_order, resolutions=resolutions, node_order=shard_order
        )
    return out


def _resident(folds, *, shard_order=tc.SHARD_ORDER, cell_order=tc.CELL_ORDER):
    return [
        _ResidentColumn(
            f"{label}.pyramid.zarr",
            folded,
            node_order=shard_order,
            cell_order=cell_order,
            granule_count=1,
            time_range=[f"{label}-01-01T00:00:00+00:00", f"{label}-12-31T00:00:00+00:00"],
        )
        for label, folded in sorted(folds.items())
    ]


def _oracle(folds, res):
    """The per-cell close law over the window members at ``res``, by hand."""
    members = [folds[label][res] for label in sorted(folds)]
    n = len(members[0]["count"])
    expected = {}
    for name, meta in tc.FIELDS.items():
        if meta["class"] == "exact":
            stacked = np.stack([m[name] for m in members], axis=1).reshape(-1)
            expected[name] = fold_dense(stacked, len(members), meta["method"], meta["fill_value"])
            continue
        out = np.full(n, b"", dtype=object)
        for j in range(n):
            cell = [decode_digest(m[name][j], "float32") for m in members if len(m[name][j])]
            if cell:
                out[j] = fold_digests(cell, delta=overview_fold_delta(meta), dtype="float32")
        expected[name] = out
    return expected


class TestFoldAllTime:
    def test_every_member_is_the_kway_fold_of_the_window_members(self):
        folds = _window_folds()
        resolutions = [tc.CELL_ORDER, tc.CELL_ORDER - 1, tc.SHARD_ORDER]
        folded, depth, broken = fold_all_time(
            _resident(folds), tc.FIELDS, node_order=tc.SHARD_ORDER, resolutions=resolutions
        )
        assert broken == set() and sorted(folded) == sorted(resolutions)
        for res in resolutions:
            expected = _oracle(folds, res)
            assert set(folded[res]) == set(expected)
            np.testing.assert_array_equal(folded[res]["count"], expected["count"])
            np.testing.assert_array_equal(folded[res]["h_min"], expected["h_min"])
            assert [bytes(p) for p in folded[res]["h_tdigest"]] == [
                bytes(p) for p in expected["h_tdigest"]
            ]
        # Something was actually merged: the shared cells carry more weight
        # than any one window's member does.
        finest = folded[tc.CELL_ORDER]
        assert finest["count"][0] == 3 * 7 and finest["count"][1] == 3 * 5
        assert decode_digest(finest["h_tdigest"][0], "float32")[:, 1].sum() == 21

    def test_a_lone_window_folds_to_itself(self):
        folds = _window_folds(("2019",))
        folded, depth, _broken = fold_all_time(
            _resident(folds), tc.FIELDS, node_order=tc.SHARD_ORDER, resolutions=[tc.CELL_ORDER]
        )
        member = folds["2019"][tc.CELL_ORDER]
        np.testing.assert_array_equal(folded[tc.CELL_ORDER]["count"], member["count"])
        assert [bytes(p) for p in folded[tc.CELL_ORDER]["h_tdigest"]] == [
            bytes(p) for p in member["h_tdigest"]
        ]
        assert depth == {tc.CELL_ORDER: 2}

    def test_depth_is_one_more_than_the_window_members_at_every_group(self):
        # Node 2 / cells 6: the raw-fold boundary is 4, so the window members
        # record 1 at {6, 5, 4} and 2 at {3, 2}; the all-time member one more.
        folds = _window_folds(shard_order=2, cell_order=6)
        resolutions = [6, 5, 4, 3, 2]
        _folded, depth, _broken = fold_all_time(
            _resident(folds, shard_order=2, cell_order=6),
            tc.FIELDS,
            node_order=2,
            resolutions=resolutions,
        )
        assert depth == {6: 2, 5: 2, 4: 2, 3: 3, 2: 3}

    def test_a_missing_window_contributes_nothing_and_counts_missing(self, tmp_path):
        folds = _window_folds()
        row = _resident(folds)
        row[1] = None  # 2019: no committed column
        folded, _depth, broken = fold_all_time(
            row, tc.FIELDS, node_order=tc.SHARD_ORDER, resolutions=[tc.CELL_ORDER]
        )
        two = _oracle({k: v for k, v in folds.items() if k != "2019"}, tc.CELL_ORDER)
        np.testing.assert_array_equal(folded[tc.CELL_ORDER]["count"], two["count"])
        assert broken == set()
        basename = write_all_time_column(
            str(tmp_path),
            tc.morton_word("-311"),
            row,
            tc.FIELDS,
            node_order=tc.SHARD_ORDER,
            cell_order=tc.CELL_ORDER,
            resolutions=[tc.CELL_ORDER, tc.CELL_ORDER - 1, tc.SHARD_ORDER],
        )
        attrs = _attrs(tmp_path, basename)
        assert attrs[COLUMN_ATTR]["source_windows"] == {"folded": 2, "missing": 1, "unreadable": 0}

    def test_nothing_to_fold_writes_nothing(self, tmp_path):
        assert (
            write_all_time_column(
                str(tmp_path),
                tc.morton_word("-311"),
                [None, None],
                tc.FIELDS,
                node_order=tc.SHARD_ORDER,
                cell_order=tc.CELL_ORDER,
                resolutions=[tc.CELL_ORDER, tc.SHARD_ORDER],
            )
            is None
        )
        assert not list(tmp_path.rglob("all.pyramid.zarr"))


def _attrs(root: Path, basename: str) -> dict:
    node = Path(hive.shard_leaf_path(str(root), tc.morton_word("-311"))).parent
    return json.loads((node / basename / "zarr.json").read_text())["attributes"]


class TestWriteAllTimeColumn:
    def test_the_member_is_the_all_token_with_the_carve_out_provenance(self, tmp_path):
        folds = _window_folds()
        basename = write_all_time_column(
            str(tmp_path),
            tc.morton_word("-311"),
            _resident(folds),
            tc.FIELDS,
            node_order=tc.SHARD_ORDER,
            cell_order=tc.CELL_ORDER,
            resolutions=[tc.CELL_ORDER, tc.CELL_ORDER - 1, tc.SHARD_ORDER],
        )
        assert basename == "all.pyramid.zarr"
        attrs = _attrs(tmp_path, basename)
        block = attrs[COLUMN_ATTR]
        assert block["window"] == SCHEDULE_NONE_TOKEN
        assert {r: g["merges_from_raw"] for r, g in block["groups"].items()} == {
            "4": 2,
            "3": 2,
            "2": 2,
        }
        assert all(g["regime"] == "leaf-column" for g in block["groups"].values())
        assert block["source_windows"] == {"folded": 3, "missing": 0, "unreadable": 0}
        # The stamp is the sweep's all-time shape: the token, the folded
        # windows' granule counts summed and their time ranges unioned.
        stamp = attrs[COMMIT_ATTR]
        assert stamp["spec"] == "morton-hive/2" and stamp["window"] == SCHEDULE_NONE_TOKEN
        assert stamp["granule_count"] == 3
        assert stamp["time_range"] == ["2018-01-01T00:00:00+00:00", "2020-12-31T00:00:00+00:00"]
        node = Path(hive.shard_leaf_path(str(tmp_path), tc.morton_word("-311"))).parent
        assert (node / "all.pyramid.stats.json").exists()
        group = zarr.open_group(open_store(str(node / basename)), path="4", mode="r", zarr_format=3)
        np.testing.assert_array_equal(group["count"][:], _oracle(folds, 4)["count"])


def _node(root, label="2019") -> Path:
    return Path(hive.shard_leaf_path(root, emit._shard_word(), window=label)).parent


def _column_groups(path: Path) -> dict:
    store = open_store(str(path))
    attrs = dict(zarr.open_group(store, mode="r", zarr_format=3).attrs)
    groups = {}
    for res in attrs[COLUMN_ATTR]["groups"]:
        g = zarr.open_group(store, path=res, mode="r", zarr_format=3)
        groups[int(res)] = {name: np.asarray(g[name][:]) for name in g.array_keys()}
    return groups


class TestBulkUnit:
    """The resident path on the real bulk emit (``test_windowed_emit``'s harness)."""

    def test_the_shard_closes_its_windows_into_the_all_time_column(self, monkeypatch, tmp_path):
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        assert meta["phase_timings"]["all_time_column"] >= 0
        node = _node(root)
        members = {w: _column_groups(node / f"{w}.pyramid.zarr") for w in WINDOWS}
        all_time = _column_groups(node / "all.pyramid.zarr")
        assert sorted(all_time) == sorted(members["2019"]) == [6, 7]
        meta_digest = cfg.aggregation["variables"]["h_tdigest"]
        delta = overview_fold_delta({"delta": meta_digest["params"]["delta"]})
        for res, group in all_time.items():
            np.testing.assert_array_equal(group["morton"], members["2019"][res]["morton"])
            # Counts: the sum of the window members; digests: their k-way fold.
            np.testing.assert_array_equal(
                group["count"], sum(members[w][res]["count"] for w in WINDOWS)
            )
            for j in range(len(group["count"])):
                cell = [
                    decode_digest(members[w][res]["h_tdigest"][j], "float32")
                    for w in WINDOWS
                    if len(members[w][res]["h_tdigest"][j])
                ]
                expected = fold_digests(cell, delta=delta, dtype="float32") if cell else b""
                assert bytes(group["h_tdigest"][j]) == bytes(expected), (res, j)
        attrs = json.loads((node / "all.pyramid.zarr" / "zarr.json").read_text())["attributes"]
        assert attrs[COLUMN_ATTR]["source_windows"] == {"folded": 3, "missing": 0, "unreadable": 0}
        assert attrs[COMMIT_ATTR]["time_range"] == meta["time_range"]
        assert attrs[COMMIT_ATTR]["granule_count"] == sum(
            m["granule_count"] for m in meta["windows"]
        )

    def test_a_rerun_writes_the_same_bytes(self, monkeypatch, tmp_path):
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        before = emit._tree(root)
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert emit._tree(root) == before
        assert any(
            k.startswith(str(_node(root).relative_to(root) / "all.pyramid.zarr")) for k in before
        )

    def test_a_benign_empty_window_is_not_a_source(self, monkeypatch, tmp_path):
        # A's span covers 2018 but its rows are all 2019: the 2018 window has
        # no leaf and no column, so the all-time member folds the two that do.
        fakes = {
            **emit._dense_fakes(),
            "s3://bucket/granuleA.h5": emit._dense_h5([400.0, 401.0], 1),
        }
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, emit._digest_cfg(), root, fakes=fakes)
        assert [m["error"] for m in meta["windows"]] == ["No data after filtering", None, None]
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        attrs = json.loads((_node(root) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert attrs["attributes"][COLUMN_ATTR]["source_windows"] == {
            "folded": 2,
            "missing": 0,
            "unreadable": 0,
        }

    def test_a_gate_skipped_window_leaves_the_member_to_the_stored_path(
        self, monkeypatch, tmp_path
    ):
        # Phase 1 folds from memory only: a window the identity gate skipped is
        # not resident, so the member is not written short — the stored-column
        # path (phase 2 of issue #649) owns that case.
        gate = hive._LeafUnit.gate

        def _current(self, *, allow_contraction):
            if self.label == "2018":
                return {"shard_key": int(self.shard_key), "window": "2018", "current": True}
            return gate(self, allow_contraction=allow_contraction)

        monkeypatch.setattr(hive._LeafUnit, "gate", _current)
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(
            monkeypatch, emit._digest_cfg(), root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["error"] is None and "all_time_column" not in meta
        assert not (_node(root) / "all.pyramid.zarr").exists()
        assert (_node(root) / "2019.pyramid.zarr").exists()

    def test_a_failed_close_is_the_shards_error_and_the_windows_keep_theirs(
        self, monkeypatch, tmp_path
    ):
        import zagg.column_alltime as column_alltime

        def _boom(*a, **k):
            raise RuntimeError("PUT failed")

        monkeypatch.setattr(column_alltime, "write_all_time_column", _boom)
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, emit._digest_cfg(), root, fakes=emit._dense_fakes())
        assert meta["error"] == "all-time column: PUT failed"
        assert meta["all_time_column_error"] == "PUT failed"
        assert [m["error"] for m in meta["windows"]] == [None, None, None]
        assert all(m["leaf_column"] for m in meta["windows"])
        assert not (_node(root) / "all.pyramid.zarr").exists()

    def test_no_column_declaration_means_no_member_and_clears_a_stale_one(
        self, monkeypatch, tmp_path
    ):
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, emit._digest_cfg(), root, fakes=emit._dense_fakes())
        node = _node(root)
        assert (node / "all.pyramid.zarr").exists() and (node / "all.pyramid.stats.json").exists()
        # The same shard under a declaration with no composable field: the
        # window columns go (the #383 clear) and so does the all-time member.
        meta = emit._run_bulk(monkeypatch, emit._cfg(), root)
        assert meta["error"] is None and "all_time_column" not in meta
        assert not list(node.glob("*.pyramid.*"))


@pytest.mark.parametrize("label", ["2019", SCHEDULE_NONE_TOKEN])
def test_the_token_names_the_column_beside_the_node_not_a_leaf(tmp_path, label):
    # ``write_column`` under the token derives the node prefix from the
    # unwindowed leaf path: no ``{id}_all.zarr`` leaf is implied anywhere.
    folds = _window_folds(("2019",))
    from zagg.column import write_column

    basename = write_column(
        str(tmp_path),
        tc.morton_word("-311"),
        folds["2019"],
        tc.FIELDS,
        node_order=tc.SHARD_ORDER,
        cell_order=tc.CELL_ORDER,
        window=label,
        time_range=["2019-01-01T00:00:00+00:00", "2019-06-01T00:00:00+00:00"],
    )
    node = Path(hive.shard_leaf_path(str(tmp_path), tc.morton_word("-311"))).parent
    assert basename == f"{label}.pyramid.zarr" and (node / basename).exists()
    assert _attrs(tmp_path, basename)[COLUMN_ATTR]["window"] == label
    assert [p.name for p in node.iterdir() if p.name.endswith(".zarr")] == [basename]
