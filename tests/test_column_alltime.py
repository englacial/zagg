"""The windowed leaf's all-time column (issue #649).

The leaf worker folds a shard's window columns into ``all.pyramid.zarr``
beside them under the sweep's close law — every member the k-way fold of the
window members, cell for cell, in ascending window order — from a hybrid
row: the folds this invoke holds resident (the bulk ``unit: shard`` build),
the stored columns of the windows it did not write (a gate-skipped window,
an append). The fan-out ``unit: window`` invoke clears the member instead
(its concurrent units would tear it; PR #666 review finding (1)). The member carries the
§4.6 carve-out's provenance (``merges_from_raw`` one deeper than the window
members', ``source_windows``), and the two input kinds give the same bytes.
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


def _window_folds(
    windows=WINDOWS, *, shard_order=tc.SHARD_ORDER, cell_order=tc.CELL_ORDER, fields=tc.FIELDS
):
    """``{label: fold_column output}`` over three windows of distinct resident cells.

    ``fields`` may locate the digest (its ``_locations`` sibling built in the
    same call) or add a packed ``composition`` word ``of`` it, one distinct
    lane word per window so the merge is a genuine blend.
    """
    rng = np.random.default_rng(649)
    n_cells = 4 ** (cell_order - shard_order)
    out = {}
    for k, label in enumerate(windows):
        # Every window populates a different subset, with cells 0 and 1 shared
        # by all so the cross-window merge has real work at the finest group.
        cells = {0: rng.normal(k, 1, 7), 1: rng.normal(10 + k, 1, 5), 2 + k: rng.normal(0, 1, 3)}
        if "location" in fields["h_tdigest"]:
            slabs, _truth = tc._located_cell_slabs(cells, fields, n_cells)
        else:
            slabs = tc._cell_slabs(cells, n_cells)
        if "composition" in fields:
            slabs["composition"] = np.zeros(n_cells, np.uint64)
            slabs["composition"][list(cells)] = (0xFF000000FF0000FF, 0x00FF00FF00FF0000)[k % 2]
        resolutions = list(range(cell_order, shard_order - 1, -1))
        out[label] = fold_column(
            slabs, fields, cell_order=cell_order, resolutions=resolutions, node_order=shard_order
        )
    return out


#: The three field classes the hybrid row must carry byte-for-byte (review
#: finding (10) of PR #666): exact + approximate, a located digest (payload
#: plus its ``_locations`` sibling), and a packed word with its ``of`` digest.
CLASSES = {"plain": tc.FIELDS, "located": tc.LOCATED_FIELDS, "packed": tc.PACKED_FIELDS}


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
        folded, depth, broken, demotions = fold_all_time(
            _resident(folds), tc.FIELDS, node_order=tc.SHARD_ORDER, resolutions=resolutions
        )
        assert broken == {r: set() for r in resolutions} and demotions == {}
        assert sorted(folded) == sorted(resolutions)
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
        folded, depth, _broken, _demotions = fold_all_time(
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
        _folded, depth, _broken, _demotions = fold_all_time(
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
        folded, _depth, broken, _demotions = fold_all_time(
            row, tc.FIELDS, node_order=tc.SHARD_ORDER, resolutions=[tc.CELL_ORDER]
        )
        two = _oracle({k: v for k, v in folds.items() if k != "2019"}, tc.CELL_ORDER)
        np.testing.assert_array_equal(folded[tc.CELL_ORDER]["count"], two["count"])
        assert broken == {tc.CELL_ORDER: set()}
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

    def test_a_window_without_the_packed_word_lands_in_the_groups(self, tmp_path):
        # Review finding (7): 2019's column carries the divisor digest but not
        # the composition word (written before the field was declared), so
        # the rail blanks the shared cells; the artifact must say so.
        n = 4 ** (tc.CELL_ORDER - tc.SHARD_ORDER)
        resolutions = [tc.CELL_ORDER, tc.CELL_ORDER - 1, tc.SHARD_ORDER]
        folds = {}
        for k, label in enumerate(("2018", "2019")):
            slabs = tc._cell_slabs({0: np.arange(5.0) + k, 1: np.arange(3.0)}, n)
            slabs["composition"] = np.zeros(n, np.uint64)
            slabs["composition"][:2] = (0xFF000000FF0000FF, 0x00FF00FF00FF0000)
            folds[label] = fold_column(
                slabs,
                tc.PACKED_FIELDS,
                cell_order=tc.CELL_ORDER,
                resolutions=resolutions,
                node_order=tc.SHARD_ORDER,
            )
        for group in folds["2019"].values():
            del group["composition"]
        args = dict(node_order=tc.SHARD_ORDER, cell_order=tc.CELL_ORDER, resolutions=resolutions)
        basename = write_all_time_column(
            str(tmp_path), tc.morton_word("-311"), _resident(folds), tc.PACKED_FIELDS, **args
        )
        groups = _attrs(tmp_path, basename)[COLUMN_ATTR]["groups"]
        for res in map(str, resolutions):
            (record,) = groups[res]["demotions"]
            assert record["field"] == "composition" and record["reason"] == "word-missing"
            assert record["contributors"] == 1 and record["of"] == "h_tdigest"
        # A clean fold of the same field keys no ``demotions`` anywhere.
        clean = {label: folds["2018"] for label in ("2018", "2019")}
        basename = write_all_time_column(
            str(tmp_path / "clean"),
            tc.morton_word("-311"),
            _resident(clean),
            tc.PACKED_FIELDS,
            **args,
        )
        groups = _attrs(tmp_path / "clean", basename)[COLUMN_ATTR]["groups"]
        assert not any("demotions" in g for g in groups.values())

    def test_a_refusal_is_per_group_and_the_counters_say_so(self, tmp_path):
        # Review finding (8): 2019 carries its digest's location sibling at
        # every group but the coarsest, so the kernel refuses it THERE alone.
        from zagg.sweep_fold import _gather_depth

        n = 4 ** (tc.CELL_ORDER - tc.SHARD_ORDER)
        resolutions = [tc.CELL_ORDER, tc.CELL_ORDER - 1, tc.SHARD_ORDER]
        folds = {}
        for k, label in enumerate(("2018", "2019")):
            slabs, _truth = tc._located_cell_slabs(
                {0: np.arange(5.0) + k, 1: np.arange(3.0)}, tc.LOCATED_FIELDS, n
            )
            folds[label] = fold_column(
                slabs,
                tc.LOCATED_FIELDS,
                cell_order=tc.CELL_ORDER,
                resolutions=resolutions,
                node_order=tc.SHARD_ORDER,
            )
        del folds["2019"][tc.SHARD_ORDER]["h_tdigest_locations"]
        row = _resident(folds)
        # A refused window's depth must not reach the group that refused it.
        groups = row[1].attrs[COLUMN_ATTR]["groups"]
        for res in resolutions:
            groups[str(res)]["merges_from_raw"] = 5
        folded, depth, broken, _demotions = fold_all_time(
            row, tc.LOCATED_FIELDS, node_order=tc.SHARD_ORDER, resolutions=resolutions
        )
        assert broken == {tc.CELL_ORDER: set(), tc.CELL_ORDER - 1: set(), tc.SHARD_ORDER: {1}}
        assert depth[tc.SHARD_ORDER] == _gather_depth([row[:1]], tc.SHARD_ORDER) + 1 == 2
        assert depth[tc.CELL_ORDER] == depth[tc.CELL_ORDER - 1] == 6
        # 2019's digest still folds at the groups that did not refuse it.
        finest = decode_digest(folded[tc.CELL_ORDER]["h_tdigest"][0], "float32")
        assert finest[:, 1].sum() == 10
        basename = write_all_time_column(
            str(tmp_path),
            tc.morton_word("-311"),
            row,
            tc.LOCATED_FIELDS,
            node_order=tc.SHARD_ORDER,
            cell_order=tc.CELL_ORDER,
            resolutions=resolutions,
        )
        attrs = _attrs(tmp_path, basename)
        assert attrs[COLUMN_ATTR]["source_windows"] == {"folded": 1, "missing": 0, "unreadable": 1}
        # The stamp covers every window folded at some group: both.
        assert attrs[COMMIT_ATTR]["granule_count"] == 2
        assert attrs[COMMIT_ATTR]["time_range"][1].startswith("2019-12-31")

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


def _stored_columns(
    root, folds, *, shard_order=tc.SHARD_ORDER, cell_order=tc.CELL_ORDER, fields=tc.FIELDS
):
    """Write ``folds`` as window columns beside the shard; returns their labels."""
    from zagg.column import write_column

    for label, folded in folds.items():
        write_column(
            str(root),
            tc.morton_word("-311"),
            folded,
            fields,
            node_order=shard_order,
            cell_order=cell_order,
            window=label,
            time_range=[f"{label}-01-01T00:00:00+00:00", f"{label}-12-31T00:00:00+00:00"],
            granule_count=1,
        )
    return set(folds)


class TestHybridRow:
    """Resident and stored inputs are one fold: same bytes, same provenance."""

    def _bytes(self, folded):
        return {
            res: {
                name: [bytes(p) for p in slab] if slab.dtype == object else slab.tobytes()
                for name, slab in group.items()
            }
            for res, group in folded.items()
        }

    @pytest.mark.parametrize("kind", sorted(CLASSES))
    def test_resident_stored_and_mixed_rows_fold_to_the_same_bytes(self, tmp_path, kind):
        from zagg.column_alltime import _StoredColumn, window_row

        fields = CLASSES[kind]
        folds = _window_folds(fields=fields)
        labels = _stored_columns(tmp_path, folds, fields=fields)
        resident = {s.path.removesuffix(".pyramid.zarr"): s for s in _resident(folds)}
        rows = {
            "resident": window_row(str(tmp_path), tc.morton_word("-311"), labels, resident),
            "stored": window_row(str(tmp_path), tc.morton_word("-311"), labels, {}),
            "mixed": window_row(
                str(tmp_path), tc.morton_word("-311"), labels, {"2019": resident["2019"]}
            ),
        }
        assert all(isinstance(e, _StoredColumn) for e in rows["stored"])
        assert [type(e).__name__ for e in rows["mixed"]] == [
            "_StoredColumn",
            "_ResidentColumn",
            "_StoredColumn",
        ]
        out = {}
        for name, row in rows.items():
            folded, depth, broken, demoted = fold_all_time(
                row, fields, node_order=tc.SHARD_ORDER, resolutions=[4, 3, 2]
            )
            out[name] = (self._bytes(folded), depth, broken, demoted)
            assert set(folded[4]) == set(folds["2019"][4])  # every array, siblings too
        assert out["resident"] == out["stored"] == out["mixed"]
        assert out["mixed"][1] == {4: 2, 3: 2, 2: 2}
        assert out["mixed"][2] == {4: set(), 3: set(), 2: set()} and out["mixed"][3] == {}

    @pytest.mark.parametrize("kind", sorted(CLASSES))
    def test_the_members_written_from_either_row_are_byte_identical(self, tmp_path, kind):
        from zagg.column_alltime import window_row

        fields = CLASSES[kind]
        folds = _window_folds(fields=fields)
        trees = {}
        for source, held in (("resident", None), ("stored", {})):
            root = tmp_path / source
            labels = _stored_columns(root, folds, fields=fields)
            resident = (
                {s.path.removesuffix(".pyramid.zarr"): s for s in _resident(folds)}
                if held is None
                else held
            )
            row = window_row(str(root), tc.morton_word("-311"), labels, resident)
            assert (
                write_all_time_column(
                    str(root),
                    tc.morton_word("-311"),
                    row,
                    fields,
                    node_order=tc.SHARD_ORDER,
                    cell_order=tc.CELL_ORDER,
                    resolutions=[4, 3, 2],
                )
                == "all.pyramid.zarr"
            )
            trees[source] = {k: v for k, v in emit._tree(root).items() if "/all.pyramid." in k}
        assert trees["resident"] and trees["resident"] == trees["stored"]
        extra = {"plain": None, "located": "h_tdigest_locations", "packed": "composition"}[kind]
        assert extra is None or any(f"/{extra}/" in k for k in trees["stored"])

    def test_a_window_refused_at_one_group_is_the_same_either_way(self, tmp_path):
        # 2019's column lacks its digest's location sibling at the coarsest
        # group alone, resident and stored alike: the kernel refuses it THERE
        # in both rows, and the two members are byte-identical.
        import shutil

        from zagg.column_alltime import _StoredColumn, window_row

        fields = tc.LOCATED_FIELDS
        built = _window_folds(fields=fields)
        trees, broken = {}, {}
        for source in ("resident", "stored"):
            folds = {w: {r: dict(g) for r, g in f.items()} for w, f in built.items()}
            root = tmp_path / source
            labels = _stored_columns(root, folds, fields=fields)
            node = Path(hive.shard_leaf_path(str(root), tc.morton_word("-311"))).parent
            shutil.rmtree(node / "2019.pyramid.zarr" / str(tc.SHARD_ORDER) / "h_tdigest_locations")
            if source == "resident":
                held = {s.path.removesuffix(".pyramid.zarr"): s for s in _resident(folds)}
                del held["2019"]._folded[tc.SHARD_ORDER]["h_tdigest_locations"]
            else:
                held = {}
            row = window_row(str(root), tc.morton_word("-311"), labels, held)
            assert isinstance(row[1], _StoredColumn) is (source == "stored")
            broken[source] = fold_all_time(
                row, fields, node_order=tc.SHARD_ORDER, resolutions=[4, 3, 2]
            )[2]
            write_all_time_column(
                str(root),
                tc.morton_word("-311"),
                row,
                fields,
                node_order=tc.SHARD_ORDER,
                cell_order=tc.CELL_ORDER,
                resolutions=[4, 3, 2],
            )
            trees[source] = {k: v for k, v in emit._tree(root).items() if "/all.pyramid." in k}
            attrs = _attrs(root, "all.pyramid.zarr")
            assert attrs[COLUMN_ATTR]["source_windows"] == {
                "folded": 2,
                "missing": 0,
                "unreadable": 1,
            }
        assert broken["resident"] == broken["stored"] == {4: set(), 3: set(), 2: {1}}
        assert trees["resident"] and trees["resident"] == trees["stored"]

    def test_a_stored_row_names_missing_and_unreadable_windows(self, tmp_path):
        from zagg.column_alltime import window_row
        from zagg.sweep_fold import UNREADABLE

        folds = _window_folds()
        _stored_columns(tmp_path, {k: v for k, v in folds.items() if k != "2018"})
        node = Path(hive.shard_leaf_path(str(tmp_path), tc.morton_word("-311"))).parent
        (node / "2020.pyramid.zarr" / "zarr.json").write_text("{not json")
        row = window_row(str(tmp_path), tc.morton_word("-311"), set(WINDOWS), {})
        assert row[0] is None and row[2] is UNREADABLE and row[1].committed
        basename = write_all_time_column(
            str(tmp_path),
            tc.morton_word("-311"),
            row,
            tc.FIELDS,
            node_order=tc.SHARD_ORDER,
            cell_order=tc.CELL_ORDER,
            resolutions=[4, 3, 2],
        )
        attrs = _attrs(tmp_path, basename)
        assert attrs[COLUMN_ATTR]["source_windows"] == {"folded": 1, "missing": 1, "unreadable": 1}

    def test_a_stage_stamped_column_is_read_without_the_foreign_abort(self, tmp_path):
        # The leaf worker is no sweep: a column stamped by some run is a
        # source, never the two-live-sweeps abort the stage reader raises.
        from zagg.column_alltime import _StoredColumn
        from zagg.sweep_fold import _ColumnReader

        _stored_columns(tmp_path, _window_folds(("2019",)))
        node = Path(hive.shard_leaf_path(str(tmp_path), tc.morton_word("-311"))).parent
        meta = json.loads((node / "2019.pyramid.zarr" / "zarr.json").read_text())
        meta["attributes"][COMMIT_ATTR].update(run_id="sweep-x", written_at="2999-01-01T00:00:00")
        (node / "2019.pyramid.zarr" / "zarr.json").write_text(json.dumps(meta))
        path = str(node / "2019.pyramid.zarr")
        with pytest.raises(Exception, match="foreign sweep"):
            _ColumnReader(path, run_id="me", run_started="2000-01-01", store_kwargs={})
        assert _StoredColumn(path).committed


class TestMemberCurrent:
    def _write(self, root, folds):
        from zagg.column_alltime import window_row

        labels = _stored_columns(root, folds)
        row = window_row(str(root), tc.morton_word("-311"), labels, {})
        write_all_time_column(
            str(root),
            tc.morton_word("-311"),
            row,
            tc.FIELDS,
            node_order=tc.SHARD_ORDER,
            cell_order=tc.CELL_ORDER,
            resolutions=[4, 3, 2],
        )
        return row

    def test_a_member_folding_exactly_the_row_is_current(self, tmp_path):
        from zagg.column_alltime import member_current

        row = self._write(tmp_path, _window_folds())
        assert member_current(str(tmp_path), tc.morton_word("-311"), row)

    def test_an_absent_short_or_older_member_is_not(self, tmp_path):
        from zagg.column_alltime import member_current, window_row

        folds = _window_folds()
        self._write(tmp_path, folds)
        shard = tc.morton_word("-311")
        node = Path(hive.shard_leaf_path(str(tmp_path), shard)).parent
        # A window the member did not fold (it landed since): short.
        _stored_columns(tmp_path, _window_folds(("2021",)))
        assert not member_current(
            str(tmp_path), shard, window_row(str(tmp_path), shard, {*WINDOWS, "2021"}, {})
        )
        # A window column stamped after the member: the member is older.
        meta = json.loads((node / "2019.pyramid.zarr" / "zarr.json").read_text())
        meta["attributes"][COMMIT_ATTR]["written_at"] = "2999-01-01T00:00:00+00:00"
        (node / "2019.pyramid.zarr" / "zarr.json").write_text(json.dumps(meta))
        assert not member_current(
            str(tmp_path), shard, window_row(str(tmp_path), shard, set(WINDOWS), {})
        )
        # A missing window in the row, and no member at all.
        (node / "2019.pyramid.zarr" / "zarr.json").unlink()
        assert not member_current(
            str(tmp_path), shard, window_row(str(tmp_path), shard, set(WINDOWS), {})
        )
        import shutil

        shutil.rmtree(node / "all.pyramid.zarr")
        assert not member_current(
            str(tmp_path), shard, window_row(str(tmp_path), shard, set(WINDOWS), {})
        )


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


def _assert_member_folds_the_stored_columns(root, cfg) -> None:
    """The member at ``root``'s shard node is the close law over its stored window columns."""
    node = _node(root)
    labels = sorted(
        p.name.removesuffix(".pyramid.zarr")
        for p in node.glob("*.pyramid.zarr")
        if p.name != "all.pyramid.zarr"
    )
    members = {w: _column_groups(node / f"{w}.pyramid.zarr") for w in labels}
    all_time = _column_groups(node / "all.pyramid.zarr")
    assert sorted(all_time) == sorted(members[labels[0]]) == [6, 7]
    delta = overview_fold_delta(
        {"delta": cfg.aggregation["variables"]["h_tdigest"]["params"]["delta"]}
    )
    for res, group in all_time.items():
        np.testing.assert_array_equal(group["morton"], members[labels[0]][res]["morton"])
        # Counts: the sum of the window members; digests: their k-way fold.
        np.testing.assert_array_equal(group["count"], sum(members[w][res]["count"] for w in labels))
        for j in range(len(group["count"])):
            cell = [
                decode_digest(members[w][res]["h_tdigest"][j], "float32")
                for w in labels
                if len(members[w][res]["h_tdigest"][j])
            ]
            expected = fold_digests(cell, delta=delta, dtype="float32") if cell else b""
            assert bytes(group["h_tdigest"][j]) == bytes(expected), (res, j)
    attrs = json.loads((node / "all.pyramid.zarr" / "zarr.json").read_text())["attributes"]
    assert attrs[COLUMN_ATTR]["source_windows"] == {
        "folded": len(labels),
        "missing": 0,
        "unreadable": 0,
    }


class TestBulkUnit:
    """The hybrid row on the real emit (``test_windowed_emit``'s harness): the bulk
    unit from its resident folds, the gate-skipped and appended windows from
    their stored columns; the fan-out unit clears it."""

    def _member(self, root):
        return {k: v for k, v in emit._tree(root).items() if "/all.pyramid." in k}

    def _current_for(self, monkeypatch, labels):
        gate = hive._LeafUnit.gate

        def _current(self, *, allow_contraction):
            if self.label in labels:
                return {"shard_key": int(self.shard_key), "window": self.label, "current": True}
            return gate(self, allow_contraction=allow_contraction)

        monkeypatch.setattr(hive._LeafUnit, "gate", _current)

    def test_the_shard_closes_its_windows_into_the_all_time_column(self, monkeypatch, tmp_path):
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        assert meta["phase_timings"]["all_time_column"] >= 0
        node = _node(root)
        assert sorted(p.name for p in node.glob("*.pyramid.zarr")) == [
            f"{w}.pyramid.zarr" for w in (*WINDOWS, "all")
        ]
        _assert_member_folds_the_stored_columns(root, cfg)
        attrs = json.loads((node / "all.pyramid.zarr" / "zarr.json").read_text())["attributes"]
        assert attrs[COMMIT_ATTR]["time_range"] == meta["time_range"]
        assert attrs[COMMIT_ATTR]["granule_count"] == sum(
            m["granule_count"] for m in meta["windows"]
        )

    def test_the_close_cost_reaches_every_window_record_once_per_invoke(
        self, monkeypatch, tmp_path
    ):
        # Review finding (4): records are built per window meta (runner and
        # the Lambda handler alike), so the shard meta's phase is invisible.
        from zagg.telemetry import build_record, merge

        meta = emit._run_bulk(
            monkeypatch, emit._digest_cfg(), str(tmp_path / "bulk"), fakes=emit._dense_fakes()
        )
        close = meta["phase_timings"]["all_time_column"]
        records = [
            build_record(shard_key=meta["shard_key"], metadata=m, run_id="r", window=m["window"])
            for m in meta["windows"]
        ]
        assert [r["phase_timings"]["all_time_column"] for r in records] == [close] * 3
        assert merge(records)["phase_timings"]["all_time_column"] == pytest.approx(close)

    def test_the_row_folds_in_label_order_whatever_the_dispatch_order(self, monkeypatch, tmp_path):
        import zagg.column_alltime as column_alltime

        seen = []
        build = column_alltime.window_row

        def _spy(*a, **kw):
            row = build(*a, **kw)
            seen.append([r.path for r in row])
            return row

        monkeypatch.setattr(column_alltime, "window_row", _spy)
        cfg = emit._digest_cfg()
        emit._patch(monkeypatch, emit._dense_fakes())
        shard, urls, windows = emit._bulk_unit(cfg)
        trees = []
        for order, name in ((windows, "sorted"), (windows[::-1], "reversed")):
            root = str(tmp_path / name)
            hive.process_and_write_hive(
                shard, urls, emit._grid(cfg), {}, root, cfg, store_kwargs={}, windows=order
            )
            trees.append(self._member(root))
        assert seen == [[f"{w}.pyramid.zarr" for w in WINDOWS]] * 2
        assert trees[0] and trees[0] == trees[1]

    def test_only_the_bulk_unit_keeps_its_column_fold(self, monkeypatch, tmp_path):
        # Review finding (6): the fold is retained only where a close consumes
        # it — the bulk unit; the fan-out unit clears the member (PR #666
        # review finding (1)) and an unwindowed leaf has no member at all.
        import zagg.column as column

        kept = []
        write = column.write_leaf_column

        def _spy(*a, folded_out=None, **kw):
            kept.append(folded_out)
            return write(*a, folded_out=folded_out, **kw)

        monkeypatch.setattr(column, "write_leaf_column", _spy)
        cfg = emit._digest_cfg()
        emit._run_fanout(monkeypatch, cfg, str(tmp_path / "fan"), fakes=emit._dense_fakes())
        assert kept == [None] * 3
        kept.clear()
        emit._run_bulk(monkeypatch, cfg, str(tmp_path / "bulk"), fakes=emit._dense_fakes())
        assert len(kept) == 3 and all(sorted(k) == [6, 7] for k in kept)
        kept.clear()
        del cfg.output["windowing"]
        shard, urls, _windows = emit._bulk_unit(emit._digest_cfg())
        meta = hive.process_and_write_hive(
            shard, urls, emit._grid(cfg), {}, str(tmp_path / "flat"), cfg, store_kwargs={}
        )
        assert meta["error"] is None and meta["leaf_column"] and kept == [None]
        assert "all_time_column" not in meta

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

    def test_a_gate_skipped_window_is_read_back_not_re_aggregated(self, monkeypatch, tmp_path):
        # 2018 is current: its stored column is a source of the member, while
        # the shard reads and aggregates the two windows left to write only.
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        fresh = self._member(root)
        stamp_2018 = hive.read_commit(
            open_store(hive.shard_leaf_path(root, emit._shard_word(), window="2018"))
        )
        self._current_for(monkeypatch, {"2018"})
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        assert [m.get("current") for m in meta["windows"]] == [True, None, None]
        assert [m.get("unit_windows") for m in meta["windows"]] == [None, 2, 2]
        assert (
            hive.read_commit(
                open_store(hive.shard_leaf_path(root, emit._shard_word(), window="2018"))
            )
            == stamp_2018
        )
        assert self._member(root) == fresh
        attrs = json.loads((_node(root) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert attrs["attributes"][COLUMN_ATTR]["source_windows"]["folded"] == 3

    def test_an_append_leaves_the_member_a_fresh_full_build_writes(self, monkeypatch, tmp_path):
        # Run 1 sees granule A alone (2018, 2019); the append sees A, B and C
        # with 2018 current. Its member must equal a one-run build's.
        cfg = emit._digest_cfg()
        appended, fresh = str(tmp_path / "appended"), str(tmp_path / "fresh")
        first = emit._run_bulk(
            monkeypatch, cfg, appended, fakes=emit._dense_fakes(), records=[emit._records()[0]]
        )
        assert [m["window"] for m in first["windows"]] == ["2018", "2019"]
        two = json.loads((_node(appended) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert two["attributes"][COLUMN_ATTR]["source_windows"]["folded"] == 2
        self._current_for(monkeypatch, {"2018"})
        meta = emit._run_bulk(
            monkeypatch, cfg, appended, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        monkeypatch.undo()
        emit._run_bulk(monkeypatch, cfg, fresh, fakes=emit._dense_fakes())
        assert self._member(appended) == self._member(fresh)

    def test_a_narrower_dispatch_folds_the_stored_windows_in(self, monkeypatch, tmp_path):
        # Review finding (1): a run over new 2020 granules alone dispatches
        # ``windows=[2020]``; the store still holds 2018 and 2019, which the
        # member folds from their stored columns — never one window under a
        # name claiming all three.
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        before = self._member(root)
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), records=[emit._records()[1]]
        )
        assert [m["window"] for m in meta["windows"]] == ["2020"]
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        # 2020's column changed (granule B alone, no C), so the member did too
        # — and it is the fold of all three stored columns as they stand now.
        assert before and self._member(root) != before
        _assert_member_folds_the_stored_columns(root, cfg)

    def test_an_all_current_rerun_leaves_a_current_member_and_rebuilds_a_missing_one(
        self, monkeypatch, tmp_path
    ):
        import shutil

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        before = emit._tree(root)
        member = _node(root) / "all.pyramid.zarr"
        stamped = json.loads((member / "zarr.json").read_text())["attributes"][COMMIT_ATTR]
        self._current_for(monkeypatch, set(WINDOWS))
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        # Nothing changed and the member folds exactly the shard's windows: no
        # rewrite (the stamp is the very same object).
        assert meta["current"] is True and "all_time_column" not in meta
        assert emit._tree(root) == before
        assert json.loads((member / "zarr.json").read_text())["attributes"][COMMIT_ATTR] == stamped
        # The member gone (a close that once failed, review finding (3)): the
        # all-current re-run rebuilds it from the stored columns, same bytes.
        shutil.rmtree(member)
        (_node(root) / "all.pyramid.stats.json").unlink()
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["current"] is True and meta["all_time_column"] == "all.pyramid.zarr"
        assert emit._tree(root) == before

    def test_a_skipped_window_whose_column_is_gone_counts_missing(self, monkeypatch, tmp_path):
        import shutil

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        shutil.rmtree(_node(root) / "2018.pyramid.zarr")
        self._current_for(monkeypatch, set(WINDOWS))
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["all_time_column"] == "all.pyramid.zarr"
        attrs = json.loads((_node(root) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert attrs["attributes"][COLUMN_ATTR]["source_windows"] == {
            "folded": 2,
            "missing": 1,
            "unreadable": 0,
        }
        # PR #666 review finding (6): the row can never be whole, and a
        # re-fold would write the same counters — so the next all-current run
        # leaves the member standing instead of rewriting it every time.
        before = emit._tree(root)
        stamped = attrs["attributes"][COMMIT_ATTR]
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert meta["current"] is True and "all_time_column" not in meta
        assert emit._tree(root) == before
        attrs = json.loads((_node(root) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert attrs["attributes"][COMMIT_ATTR] == stamped

    def test_a_gate_refused_window_is_read_back(self, monkeypatch, tmp_path):
        # PR #666 review finding (10): a window the gate REFUSES (a
        # contraction without ``allow_contraction``) is a stored source of
        # the close, as a current one is — not a deferral that clears.
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        fresh = self._member(root)
        gate = hive._LeafUnit.gate

        def _refuse(self, *, allow_contraction):
            if self.label == "2018":
                return {"shard_key": int(self.shard_key), "window": "2018", "refused": True}
            return gate(self, allow_contraction=allow_contraction)

        monkeypatch.setattr(hive._LeafUnit, "gate", _refuse)
        meta = emit._run_bulk(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
        )
        assert [m.get("refused") for m in meta["windows"]] == [True, None, None]
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        assert self._member(root) == fresh
        _assert_member_folds_the_stored_columns(root, cfg)

    def test_a_fanout_window_unit_clears_the_member(self, monkeypatch, tmp_path):
        # PR #666 review finding (1), the interim: concurrent ``(shard,
        # window)`` units cannot share a writer for the member, so a clean
        # fan-out unit clears it and a ``unit: window`` store carries none
        # until the maintainer rules how it gets one.
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert (_node(root) / "all.pyramid.zarr").exists()
        out = emit._run_fanout(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), records=[emit._records()[1]]
        )
        assert list(out) == ["2020"] and out["2020"]["error"] is None
        assert "all_time_column" not in out["2020"]
        assert not (_node(root) / "all.pyramid.zarr").exists()
        assert not (_node(root) / "all.pyramid.stats.json").exists()
        assert (_node(root) / "2020.pyramid.zarr").exists()

    def test_a_gate_skipped_fanout_unit_writes_nothing(self, monkeypatch, tmp_path):
        # PR #666 review finding (2): the gate's "writes NOTHING" contract
        # holds on the fan-out — a skipped unit neither closes nor clears, so
        # a bulk-written member stands and a fan-out store stays without one.
        cfg = emit._digest_cfg()
        bulk, fan = str(tmp_path / "bulk"), str(tmp_path / "fan")
        emit._run_bulk(monkeypatch, cfg, bulk, fakes=emit._dense_fakes())
        emit._run_fanout(monkeypatch, cfg, fan, fakes=emit._dense_fakes())
        before = {root: emit._tree(root) for root in (bulk, fan)}
        self._current_for(monkeypatch, set(WINDOWS))
        for root in (bulk, fan):
            out = emit._run_fanout(
                monkeypatch, cfg, root, fakes=emit._dense_fakes(), skip_if_current=True
            )
            assert all(m.get("current") for m in out.values())
            assert not any("all_time_column" in m for m in out.values())
            assert emit._tree(root) == before[root]
        assert self._member(bulk) and not self._member(fan)

    def test_a_failed_window_defers_the_close_and_clears_the_stale_member(
        self, monkeypatch, tmp_path
    ):
        # Review finding (2): a window that failed leaves the member folding
        # that window's previous column, so it goes until the retry.
        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert (_node(root) / "all.pyramid.zarr").exists()
        finish = hive._LeafUnit.finish

        def _flaky(self, metadata):
            if self.label == "2019":
                raise RuntimeError("PUT failed")
            return finish(self, metadata)

        monkeypatch.setattr(hive._LeafUnit, "finish", _flaky)
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["error"] == "window 2019: RuntimeError: PUT failed"
        assert "all_time_column" not in meta
        assert not (_node(root) / "all.pyramid.zarr").exists()
        assert not (_node(root) / "all.pyramid.stats.json").exists()

    @pytest.mark.parametrize("fatal", ["2018", "2019"])
    def test_a_store_fatal_abort_ends_the_invoke_before_the_close(
        self, monkeypatch, tmp_path, fatal
    ):
        # Issue #651's fail-fast: a refusal the store would repeat ends the
        # invoke at the window that hit it — the windows after it are never
        # aggregated and NO close runs (nothing to fold them from, and the
        # store is refusing writes). A window that landed first superseded a
        # column the member folds, so the member is cleared (absent beats
        # stale, PR #666 review (9)); with none landed it is left for the
        # retry, which rewrites the windows and the member alike.
        from obstore.exceptions import GenericError

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        before = self._member(root)
        finish = hive._LeafUnit.finish

        def _refused(self, metadata):
            if self.label == fatal:
                raise GenericError("Generic S3 error: Got invalid DeleteObjects response: EOF")
            return finish(self, metadata)

        monkeypatch.setattr(hive._LeafUnit, "finish", _refused)
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["store_error_class"] == "store-fatal"
        assert meta["windows_unattempted"] == [w for w in WINDOWS if w > fatal]
        assert "all_time_column" not in meta and "all_time_column_error" not in meta
        assert all("all_time_column" not in m.get("phase_timings", {}) for m in meta["windows"])
        assert before
        assert self._member(root) == ({} if fatal == "2019" else before)

    def test_the_stored_labels_are_the_window_columns_beside_the_shard(self, monkeypatch, tmp_path):
        from zagg.column_alltime import stored_window_labels

        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, emit._digest_cfg(), root, fakes=emit._dense_fakes())
        (_node(root) / "not_a.label.pyramid.zarr").mkdir()  # outside the label charset
        (_node(root) / "notes.txt").mkdir()  # neither a leaf nor a column
        assert (_node(root) / "all.pyramid.zarr").exists()
        assert stored_window_labels(root, emit._shard_word(), {}) == set(WINDOWS)

    def test_a_leaf_without_its_column_counts_missing(self, monkeypatch, tmp_path):
        # PR #666 review finding (7): the shard's windows are its LEAVES too.
        # 2018's column is gone but its leaf stands, and the run dispatches
        # 2020 alone (current): 2018 must count ``missing``, not vanish.
        import shutil

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        shutil.rmtree(_node(root) / "2018.pyramid.zarr")
        assert Path(hive.shard_leaf_path(root, emit._shard_word(), window="2018")).exists()
        self._current_for(monkeypatch, {"2020"})
        meta = emit._run_bulk(
            monkeypatch,
            cfg,
            root,
            fakes=emit._dense_fakes(),
            records=[emit._records()[1]],
            skip_if_current=True,
        )
        assert [m["window"] for m in meta["windows"]] == ["2020"]
        assert meta["current"] is True and meta["all_time_column"] == "all.pyramid.zarr"
        attrs = json.loads((_node(root) / "all.pyramid.zarr" / "zarr.json").read_text())
        assert attrs["attributes"][COLUMN_ATTR]["source_windows"] == {
            "folded": 2,
            "missing": 1,
            "unreadable": 0,
        }

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
        assert (
            meta["store_error_class"] == "transient"
        )  # issue #651: the shard error carries its class
        assert [m["error"] for m in meta["windows"]] == [None, None, None]
        assert all(m["leaf_column"] for m in meta["windows"])
        assert not (_node(root) / "all.pyramid.zarr").exists()

    def test_a_failed_close_behind_benign_windows_keeps_its_error(self, monkeypatch, tmp_path):
        # PR #666 review (10): every dispatched window found no data, but the
        # store holds columns, so the close still runs; its failure is the
        # shard's error, never masked as the benign no-data string.
        import zagg.column_alltime as column_alltime

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())

        def _empty(self, metadata):
            return {
                "shard_key": int(self.shard_key),
                "window": self.label,
                "cells_with_data": 0,
                "total_obs": 0,
                "error": "No data after filtering",
            }

        def _boom(*a, **k):
            raise RuntimeError("LIST failed")

        monkeypatch.setattr(hive._LeafUnit, "finish", _empty)
        monkeypatch.setattr(column_alltime, "close_shard", _boom)
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert all(m["error"] == "No data after filtering" for m in meta["windows"])
        assert meta["error"] == "all-time column: LIST failed"
        assert meta["store_error_class"] == "transient"

    def test_a_failed_fanout_clear_carries_its_store_error_class(self, monkeypatch, tmp_path):
        # PR #666 review (10): the fan-out unit's member clear is a store
        # write like any other; a refusal the store would repeat is fatal.
        from obstore.exceptions import GenericError

        import zagg.column as column

        clear = column._clear_column

        def _refused(store_root, shard_key, window, store_kwargs):
            if window is None:
                raise GenericError("Generic S3 error: Got invalid DeleteObjects response: EOF")
            return clear(store_root, shard_key, window, store_kwargs)

        monkeypatch.setattr(column, "_clear_column", _refused)
        out = emit._run_fanout(
            monkeypatch,
            emit._digest_cfg(),
            str(tmp_path / "fan"),
            fakes=emit._dense_fakes(),
            records=[emit._records()[1]],
        )
        assert out["2020"]["error"].startswith("all-time column: Generic S3 error")
        assert out["2020"]["store_error_class"] == "store-fatal"

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


class TestIcechunkAllRow:
    """Phase 4 (issue #649): the repo's ``all`` row at the column levels references the member."""

    @pytest.fixture(autouse=True)
    def _icechunk(self):
        pytest.importorskip("icechunk")

    def _init(self, cfg, root):
        from zagg import icechunk_refs
        from zagg.config import get_windowing

        grid = emit._grid(cfg)
        manifest = hive.build_manifest(grid, windowing=get_windowing(cfg))
        hive.ensure_manifest(root, manifest)
        icechunk_refs.init_repo(
            root,
            grid,
            cfg,
            run_id="r1",
            store_kwargs={},
            manifest=manifest,
            rows=[*WINDOWS, SCHEDULE_NONE_TOKEN],
        )
        return grid

    def _repo(self, root):
        from zagg import icechunk_refs

        repo = icechunk_refs.open_repo(root, store_kwargs={})
        rows = icechunk_refs.read_block(root, store_kwargs={})["rows"]
        return zarr.open_group(repo.readonly_session("main").store, mode="r"), rows

    def test_member_units_name_the_all_row_at_the_declared_column_levels(
        self, monkeypatch, tmp_path
    ):
        from zagg.icechunk_refs import member_units

        cfg = emit._digest_cfg()
        root = str(tmp_path / "bulk")
        emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        units = member_units(emit._grid(cfg), cfg, emit._shard_word(), root, store_kwargs={})
        # The declared leaf level (7) at the reserved row; no base unit (there
        # is no all-time leaf), none for the node-order partial (6).
        assert [(u["level"], u["row"]) for u in units] == [(7, SCHEDULE_NONE_TOKEN)]
        entries = {e["path"]: e for e in units[0]["entries"]}
        assert set(entries) == {"morton", "count", "h_tdigest"}
        locations = {
            loc
            for e in entries.values()
            for loc in (e["locations"] if e["sharded"] else [c[1] for c in e["chunks"]])
            if loc
        }
        assert locations and all("/all.pyramid.zarr/7/" in loc for loc in locations)

    def test_the_leaf_commit_lands_the_member_at_the_all_row(self, monkeypatch, tmp_path):
        cfg = emit._digest_cfg()
        cfg.output["icechunk"] = {"commit": "leaf"}
        root = str(tmp_path / "bulk")
        grid = self._init(cfg, root)
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["error"] is None and meta["all_time_column"] == "all.pyramid.zarr"
        assert meta["icechunk_all_time"].get("error") is None, meta["icechunk_all_time"]
        assert meta["icechunk_all_time"]["refs"] > 0
        group, rows = self._repo(root)
        (rank,) = grid.block_index(emit._shard_word())
        span = slice(rank * 4, (rank + 1) * 4)
        member = _column_groups(_node(root) / "all.pyramid.zarr")[7]
        np.testing.assert_array_equal(
            group["7"]["count"][rows.index(SCHEDULE_NONE_TOKEN), span], member["count"]
        )
        for w in WINDOWS:
            column = _column_groups(_node(root) / f"{w}.pyramid.zarr")[7]
            np.testing.assert_array_equal(group["7"]["count"][rows.index(w), span], column["count"])
        # The base has no all-time leaf: its ``all`` row reads fill.
        assert not np.asarray(group["8"]["count"][rows.index(SCHEDULE_NONE_TOKEN), :]).any()
        # The member's refs point into the member, the windows' into theirs.
        from test_icechunk_rows import _refs

        from zagg import icechunk_refs

        refs = _refs(
            icechunk_refs.open_repo(root, store_kwargs={}).readonly_session("main"), "7/count"
        )
        by_row = {}
        for (row, _cell), (_kind, location, _offset, _length) in refs.items():
            by_row.setdefault(row, set()).add(location.rsplit("/7/", 1)[0].rsplit("/", 1)[-1])
        assert by_row[rows.index(SCHEDULE_NONE_TOKEN)] == {"all.pyramid.zarr"}
        assert by_row[rows.index("2019")] == {"2019.pyramid.zarr"}

    def test_the_ladder_carrier_is_written_read_and_cleared_with_the_member(
        self, monkeypatch, tmp_path
    ):
        from zagg import icechunk_ladder, icechunk_refs
        from zagg.grids.morton import morton_decimal

        cfg = emit._digest_cfg()
        cfg.output["icechunk"] = {"commit": "ladder"}
        root = str(tmp_path / "bulk")
        grid = self._init(cfg, root)
        meta = emit._run_bulk(monkeypatch, cfg, root, fakes=emit._dense_fakes())
        assert meta["icechunk_all_time"]["sidecar"] == "all.pyramid.icechunk_refs.json"
        assert (_node(root) / "all.pyramid.icechunk_refs.json").exists()
        got = icechunk_ladder.read_member_refs(root, emit._shard_word(), store_kwargs={})
        assert got is not None
        units, carrier_meta = got
        assert [(u["level"], u["row"]) for u in units] == [(7, SCHEDULE_NONE_TOKEN)]
        assert carrier_meta["geometry"] == {
            "shard_order": grid.parent_order,
            "chunk_order": grid.chunk_order,
            "cell_order": grid.child_order,
        }
        # The committing node gathers it with the leaves' carriers when it
        # closes, and not otherwise.
        block = icechunk_refs.read_block(root, store_kwargs={})
        dec = morton_decimal(emit._shard_word())
        node = dec[:-1]
        args = (root, node, 6, 6, [dec], block, "morton-hive/2", {}, list(WINDOWS))
        rows_closed = [row for u, _m in icechunk_ladder._child_units(*args, True) for row in u]
        rows_open = [row for u, _m in icechunk_ladder._child_units(*args, False) for row in u]
        assert [(u["level"], u["row"]) for u in rows_closed if u["row"] == SCHEDULE_NONE_TOKEN] == [
            (7, SCHEDULE_NONE_TOKEN)
        ]
        assert all(u["row"] != SCHEDULE_NONE_TOKEN for u in rows_open)
        # Committed, the ``all`` row at level 7 reads the member's values.
        icechunk_refs.commit_units(root, units, f"node {node}", store_kwargs={})
        group, rows = self._repo(root)
        (rank,) = grid.block_index(emit._shard_word())
        member = _column_groups(_node(root) / "all.pyramid.zarr")[7]
        np.testing.assert_array_equal(
            group["7"]["count"][rows.index(SCHEDULE_NONE_TOKEN), rank * 4 : (rank + 1) * 4],
            member["count"],
        )
        # A fan-out unit clears the member — and its carrier with it.
        out = emit._run_fanout(
            monkeypatch, cfg, root, fakes=emit._dense_fakes(), records=[emit._records()[1]]
        )
        assert out["2020"]["error"] is None
        assert not (_node(root) / "all.pyramid.zarr").exists()
        assert not (_node(root) / "all.pyramid.icechunk_refs.json").exists()
        assert icechunk_ladder.read_member_refs(root, emit._shard_word(), store_kwargs={}) is None

    def test_a_shard_without_a_carrier_is_not_missing(self, monkeypatch, tmp_path):
        from zagg import icechunk_ladder, icechunk_refs
        from zagg.grids.morton import morton_decimal

        cfg = emit._digest_cfg()
        cfg.output["icechunk"] = {"commit": "ladder"}
        root = str(tmp_path / "bulk")
        self._init(cfg, root)
        emit._run_fanout(monkeypatch, cfg, root, fakes=emit._dense_fakes())  # no member
        block = icechunk_refs.read_block(root, store_kwargs={})
        dec = morton_decimal(emit._shard_word())
        args = (root, dec[:-1], 6, 6, [dec], block, "morton-hive/2", {}, list(WINDOWS))
        yielded = list(icechunk_ladder._child_units(*args, True))
        assert sum(miss for _u, miss in yielded) == 0
        assert all(u["row"] != SCHEDULE_NONE_TOKEN for units, _m in yielded for u in units)


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


def test_an_all_current_rebuild_is_reported_not_called_untouched(monkeypatch, tmp_path, caplog):
    # PR #666 review finding (4): when every window skipped but the close
    # rebuilt the member, the runner says so beside ``current``.
    import logging
    import shutil

    import test_pyramid_check_windowed as pcw

    root = tmp_path / "store"
    pcw._run(monkeypatch, tmp_path, root, first_run=True)
    for member in root.rglob("all.pyramid.zarr"):
        shutil.rmtree(member)
    with caplog.at_level(logging.INFO, logger="zagg.runner"):
        pcw._run(monkeypatch, tmp_path, root, first_run=True)
    lines = [r.message for r in caplog.records if "inputs unchanged" in r.message]
    assert len(lines) == len(pcw.SHARDS)
    assert all(line.endswith("leaf untouched; all-time member rewritten") for line in lines)
    assert len(list(root.rglob("all.pyramid.zarr"))) == len(pcw.SHARDS)
