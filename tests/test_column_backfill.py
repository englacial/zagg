"""Column backfill for pre-column stores (issue #520): the /1 -> /2 bridge.

Standing claims:

- **byte identity** — a column recomputed from a leaf's STORED bytes is
  byte-identical to the one the leaf worker wrote at build time, per leaf,
  per array (phase 1); and the whole ``/1 -> /2`` upgrade of a pyramid-OFF
  store lands a ladder byte-equal to a twin built pyramid-ON from identical
  inputs (phase 4);
- the backfill is a **sweep family**, so every entry point — the CLI's
  ``--families columns``, ``run_sweep(families=["columns"])``, and a fleet
  ``mode: "sweep"`` invoke since the handler began forwarding the event's
  ``families``/``partition`` blocks (#527/#528) — inherits the work-set
  normalization, ``--partitions`` and the lease for free;
- it is **declaration-driven**: a store still declaring ``/1``, declared-off,
  or ``class: none`` on every field refuses loudly and says re-declare first;
- **idempotent**: a second pass writes nothing, and a moved declaration or a
  re-run leaf is not current. Every one of the gate's verdicts is pinned by a
  direct call as well as through a pass, because its failure mode is a FALSE
  SKIP — nothing downstream would notice a store that silently never gets
  re-columned.
"""

from __future__ import annotations

import json
import pathlib
import shutil

import numpy as np
import pytest
import zarr

from zagg.grids.morton import morton_word
from zagg.store import open_store

GENERATOR = pathlib.Path(__file__).parent.parent / "tools" / "generate_spec_fixtures.py"

#: Four leaves under one order-4 shard tree, spread over two base cells so the
#: ladder above them has something to k-way merge rather than relay — which is
#: what phase 4 needs. They are NOT four independent parity witnesses:
#: ``_build_cells`` re-seeds ``default_rng(340)`` on every call and its plan is
#: shard-independent, so in the ``kitchen_sink=False`` arm — whose declared
#: fields are functions of ``h`` alone — all four leaves fold to identical
#: bytes and differ only in the ``morton`` words ``write_column`` regenerates
#: from ``shard_key`` rather than folds. The ``kitchen_sink=True`` arm does
#: vary, through ``_point_words`` in the ``_locations`` channel.
SHARDS = ("11213", "11214", "11223", "21213")


def _generator():
    """The spec fixture generator, loaded as a module (test_column precedent)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("zagg_backfill_fixture_generator", GENERATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_store(
    root,
    monkeypatch,
    *,
    pyramid=None,
    shards=SHARDS,
    kitchen_sink=False,
    window=None,
    windowing=None,
    time_range=None,
):
    """A real hive store: every shard through ``process_and_write_hive``.

    ``pyramid=None`` takes the issue #384 default flip (``/2`` at the grid's
    chunk order, columns written at birth); ``pyramid=False`` is the
    pre-column store this issue exists to upgrade. The leaf ARRAYS are
    identical either way — ``output.pyramid`` changes only which sibling
    artifacts the unit writes — which is what makes the phase 4 twins
    comparable.

    ``window``/``windowing``/``time_range`` drive the D23 windowed arm (the
    ``tests/test_column.py::_run_unit`` shape): the unit writes
    ``{shard}_{label}.zarr`` beside a ``{label}.pyramid.zarr`` column, and
    ``time_range`` is the D15 observed extent in DATASET units the fake
    reports — which the leaf's stamp carries as an ISO pair and the backfill
    recovers from there rather than computing.
    """
    from dataclasses import replace

    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid

    gen = _generator()
    cfg = gen._config(kitchen_sink=kitchen_sink, pyramid=pyramid)
    # The generator's config declares no ``data_source.variables``; the fake
    # reader never needs them, but ``load_config`` (the CLI retrofit path)
    # validates every ``source:`` against them, and data_source IS in the
    # semantic core — so the store must be BUILT with them for a retrofit
    # config to hash identically.
    cfg = replace(cfg, data_source={**cfg.data_source, "variables": {"h": "g/h"}})
    if windowing is not None:
        # The window filter injection (windowed_cell_config) needs the
        # time_field's base-rate dataset path; the fake reader ignores it.
        variables = {**cfg.data_source["variables"], windowing["time_field"]: "g/t"}
        cfg = replace(
            cfg,
            data_source={**cfg.data_source, "variables": variables},
            output={**cfg.output, "windowing": windowing},
        )
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=True)
    root.mkdir(parents=True, exist_ok=True)
    hive.ensure_manifest(
        str(root), hive.build_manifest(grid, dataset={"short_name": "COL_TEST", "version": "1"})
    )
    for decimal in shards:
        shard = morton_word(decimal)
        by_chunk, _cells = gen._build_cells(grid, shard, kitchen_sink=kitchen_sink)
        inner = gen._fake_process_shard(grid, by_chunk, kitchen_sink=kitchen_sink)

        def fake(*args, _inner=inner, **kwargs):
            if kwargs.get("chunk_results") is None:
                kwargs["chunk_results"] = []
            df, meta = _inner(*args, **kwargs)
            meta["phase_timings"] = {"read": 0.0, "index": 0.0, "aggregate": 0.0}
            if time_range is not None:
                meta["time_range"] = list(time_range)
            return df, meta

        monkeypatch.setattr(processing, "process_shard", fake)
        meta = hive.process_and_write_hive(
            shard, ["s3://fixture/a.h5"], grid, {}, str(root), cfg, store_kwargs={}, window=window
        )
        assert meta.get("error") is None, meta.get("error")
    return cfg, grid


def _column_arrays(path) -> dict:
    """``{(group, array): bytes}`` for every array under a column or overview."""
    store = open_store(str(path), read_only=True)
    root = zarr.open_group(store, path="", mode="r", zarr_format=3)
    out = {}
    for res, group in root.groups():
        for name, arr in group.arrays():
            values = arr[:]
            out[(res, name)] = (
                [bytes(p or b"") for p in values] if values.dtype == object else values.tobytes()
            )
    return out


def _objects(prefix: pathlib.Path) -> dict:
    """``{relative key: bytes}`` for every object under a store prefix."""
    return {
        str(f.relative_to(prefix)): f.read_bytes() for f in sorted(prefix.rglob("*")) if f.is_file()
    }


def _sans_timestamps(raw: bytes) -> dict:
    """A column root ``zarr.json``, minus the two keys a rewrite always moves."""
    meta = json.loads(raw)
    attrs = meta.get("attributes", {})
    attrs.get("zagg_column", {}).pop("generated_at", None)
    attrs.get("morton_hive_commit", {}).pop("written_at", None)
    return meta


def _column_path(root, decimal, window=None):
    from zagg.column import column_name
    from zagg.hive import shard_leaf_path

    leaf = shard_leaf_path(str(root), morton_word(decimal), window=window)
    return pathlib.Path(leaf).parent / column_name(window)


def _plan(root):
    from zagg.column_backfill import manifest_column_plan
    from zagg.hive import read_manifest

    return manifest_column_plan(read_manifest(str(root)))


def _verdict(root, decimal, window=None, **over):
    """``column_is_current`` against a store's own state, with overrides.

    The four artifact-side arguments come from the backfill's own readers, so
    a verdict here is the one the pass would reach; ``over`` moves whichever
    declaration term the test is exercising.
    """
    from zagg.column import COLUMN_ATTR
    from zagg.column_backfill import _column_state, _leaf_stamp, column_is_current

    shard, plan = morton_word(decimal), _plan(root)
    stamp, attrs, structure = _column_state(str(root), shard, window, {})
    args = {
        "leaf_stamp": _leaf_stamp(str(root), shard, window, {}),
        "column_stamp": stamp,
        "column_attrs": attrs.get(COLUMN_ATTR),
        "structure": structure,
        "node_order": plan.node_order,
        "cell_order": plan.cell_order,
        "resolutions": plan.resolutions,
        "fields": plan.fields,
    }
    return column_is_current(**{**args, **over})


# ---------------------------------------------------------------------------
# Phase 1: the recipe recomputed from stored bytes IS the build-time column.
# ---------------------------------------------------------------------------


class TestStoredLeafParity:
    @pytest.mark.parametrize("kitchen_sink", [False, True])
    def test_recomputed_column_is_byte_identical_per_leaf(
        self, tmp_path, monkeypatch, kitchen_sink
    ):
        from zagg.column import write_column
        from zagg.column_backfill import column_from_leaf
        from zagg.hive import read_commit, shard_leaf_path

        root = tmp_path / "on"
        _build_store(root, monkeypatch, kitchen_sink=kitchen_sink)
        plan = _plan(root)
        assert plan.resolutions == [5, 4]
        for decimal in SHARDS:
            built = _column_arrays(_column_path(root, decimal))
            assert built, f"the pyramid-ON build wrote no column at {decimal}"
            folded = column_from_leaf(
                str(root),
                morton_word(decimal),
                plan.fields,
                node_order=plan.node_order,
                cell_order=plan.cell_order,
                resolutions=plan.resolutions,
            )
            # Re-write it into a scratch store rather than comparing slabs in
            # memory: the pin is the stored BYTES, which is what a /2 reader
            # and the staged sweep consume.
            scratch = tmp_path / f"scratch-{decimal}"
            leaf = shard_leaf_path(str(root), morton_word(decimal))
            stamp = read_commit(open_store(leaf, read_only=True))
            write_column(
                str(scratch),
                morton_word(decimal),
                folded,
                plan.fields,
                node_order=plan.node_order,
                cell_order=plan.cell_order,
                granule_count=stamp["granule_count"],
            )
            recomputed = _column_path(scratch, decimal)
            assert _column_arrays(recomputed) == built
            # Object-level identity: every object under the prefix is byte-equal
            # except the root ``zarr.json``, which carries the two provenance
            # timestamps a rewrite always moves (spec §4.6).
            a, b = _objects(_column_path(root, decimal)), _objects(recomputed)
            assert set(a) == set(b)
            for key in a:
                if key == "zarr.json":
                    assert _sans_timestamps(a[key]) == _sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], key

    def test_stored_slabs_match_the_staged_sink(self, tmp_path, monkeypatch):
        """The read-back adapter returns the writer's own in-memory values."""
        from zagg.column_backfill import stored_leaf_slabs
        from zagg.hive import shard_leaf_path

        captured: dict = {}
        import zagg.column as column_mod

        real = column_mod.leaf_slabs

        def spy(staged, fields, **kwargs):
            out = real(staged, fields, **kwargs)
            captured.update(out)
            return out

        monkeypatch.setattr(column_mod, "leaf_slabs", spy)
        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1], kitchen_sink=True)
        assert captured, "the worker seam never folded a column"
        plan = _plan(root)
        stored = stored_leaf_slabs(
            shard_leaf_path(str(root), morton_word(SHARDS[0])),
            plan.fields,
            cell_order=plan.cell_order,
            n_cells=4 ** (plan.cell_order - plan.node_order),
        )
        assert set(stored) == set(captured)
        for name, slab in stored.items():
            want = captured[name]
            if slab.dtype == object:
                assert [bytes(p or b"") for p in slab] == [bytes(p or b"") for p in want]
            else:
                assert np.array_equal(slab, want, equal_nan=slab.dtype.kind == "f")

    def test_mixed_order_leaf_refuses(self, tmp_path, monkeypatch):
        from zagg.column_backfill import stored_leaf_slabs
        from zagg.hive import shard_leaf_path

        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1])
        plan = _plan(root)
        with pytest.raises(ValueError, match="mixed-order source leaves"):
            stored_leaf_slabs(
                shard_leaf_path(str(root), morton_word(SHARDS[0])),
                plan.fields,
                cell_order=plan.cell_order,
                n_cells=17,
            )

    def test_absent_declared_field_reads_as_fill(self, tmp_path, monkeypatch):
        """Schema evolution: a field the leaf predates folds as the sink's fill."""
        from zagg.column_backfill import stored_leaf_slabs
        from zagg.hive import shard_leaf_path

        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1])
        plan = _plan(root)
        fields = {
            **plan.fields,
            "later": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
        }
        stored = stored_leaf_slabs(
            shard_leaf_path(str(root), morton_word(SHARDS[0])),
            fields,
            cell_order=plan.cell_order,
            n_cells=4 ** (plan.cell_order - plan.node_order),
        )
        assert stored["later"].tolist() == [0] * 4 ** (plan.cell_order - plan.node_order)

    def test_a_leaf_missing_a_declared_companion_refuses(self, tmp_path, monkeypatch):
        """A channel the leaves predate is a DECLARATION fault, named as one."""
        from zagg.column_backfill import column_from_leaf, stored_leaf_slabs
        from zagg.hive import shard_leaf_path
        from zagg.sweep_overview import field_companions

        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1], kitchen_sink=True)
        plan = _plan(root)
        name = next(n for n, m in plan.fields.items() if field_companions(n, m))
        sibling = field_companions(name, plan.fields[name])[0][1]
        leaf = shard_leaf_path(str(root), morton_word(SHARDS[0]))
        shutil.rmtree(pathlib.Path(leaf) / str(plan.cell_order) / sibling)
        n_cells = 4 ** (plan.cell_order - plan.node_order)
        with pytest.raises(ValueError, match=f"carries no '{sibling}' array"):
            stored_leaf_slabs(leaf, plan.fields, cell_order=plan.cell_order, n_cells=n_cells)
        # And it lands at the READ, not downstream as a §1.1 row-alignment
        # error from the kernel folding the pair apart.
        with pytest.raises(ValueError, match="predate the declaration"):
            column_from_leaf(
                str(root),
                morton_word(SHARDS[0]),
                plan.fields,
                node_order=plan.node_order,
                cell_order=plan.cell_order,
                resolutions=plan.resolutions,
            )

    def test_an_absent_declared_field_folds_as_the_staged_fill(self, tmp_path, monkeypatch):
        """The retrofit's own arm, FOLDED — a slab assertion pins only half of it."""
        from zagg.column import fold_column, leaf_slabs
        from zagg.column_backfill import stored_leaf_slabs
        from zagg.hive import shard_leaf_path

        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1])
        plan = _plan(root)
        n_cells = 4 ** (plan.cell_order - plan.node_order)
        fields = {
            **plan.fields,
            "later": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
        }
        leaf = shard_leaf_path(str(root), morton_word(SHARDS[0]))
        stored = stored_leaf_slabs(leaf, fields, cell_order=plan.cell_order, n_cells=n_cells)
        # The staged sink's answer to the same declaration, over a sink that
        # never heard of the field — the build-time half of the same claim.
        staged = leaf_slabs(
            {f"{plan.cell_order}/{n}": v for n, v in stored.items() if n != "later"},
            fields,
            group_path=str(plan.cell_order),
            n_cells=n_cells,
        )
        a, b = (
            fold_column(m, fields, cell_order=plan.cell_order, resolutions=plan.resolutions)
            for m in (stored, staged)
        )
        assert set(a) == set(b)
        for res in a:
            assert set(a[res]) == set(b[res]) and "later" in a[res]
            for name, x in a[res].items():
                y = b[res][name]
                if x.dtype == object:
                    assert [bytes(p or b"") for p in x] == [bytes(p or b"") for p in y], name
                else:
                    assert np.array_equal(x, y, equal_nan=x.dtype.kind == "f"), name
            # A field nothing ever wrote folds to its declared fill, everywhere.
            assert a[res]["later"].tolist() == [0] * 4 ** (res - plan.node_order)

    def test_mismatched_weights_declaration_refuses(self, tmp_path, monkeypatch):
        from zagg.column_backfill import stored_leaf_slabs
        from zagg.hive import shard_leaf_path

        root = tmp_path / "on"
        _build_store(root, monkeypatch, shards=SHARDS[:1])
        plan = _plan(root)
        fields = {n: dict(m) for n, m in plan.fields.items()}
        fields["h_tdigest"]["weights"] = "flux"
        with pytest.raises(ValueError, match="weights declaration"):
            stored_leaf_slabs(
                shard_leaf_path(str(root), morton_word(SHARDS[0])),
                fields,
                cell_order=plan.cell_order,
                n_cells=4 ** (plan.cell_order - plan.node_order),
            )


# ---------------------------------------------------------------------------
# Phase 2: the backfill arm — a sweep family, declaration-driven, idempotent.
# ---------------------------------------------------------------------------


def _install_pyramid(root, block):
    """Hand-install a manifest pyramid block (phase 3 does this properly)."""
    from zagg.hive import MANIFEST_NAME

    path = root / MANIFEST_NAME
    manifest = json.loads(path.read_text())
    if block is None:
        manifest.pop("pyramid", None)
    else:
        manifest["pyramid"] = block
    path.write_text(json.dumps(manifest, indent=1))
    return manifest


def _twin_block(on_root):
    from zagg.hive import read_manifest

    return read_manifest(str(on_root))["pyramid"]


def _backfill(root, *, shards=SHARDS, windows=(None,), **kwargs):
    from zagg.column_backfill import backfill_columns
    from zagg.hive import read_manifest

    return backfill_columns(
        str(root), read_manifest(str(root)), {d: set(windows) for d in shards}, **kwargs
    )


class TestDeclarationGate:
    def test_v1_schedule_refuses_and_says_re_declare(self, tmp_path, monkeypatch):
        root = tmp_path / "off"
        _build_store(root, monkeypatch, pyramid={"orders": [3, 2]}, shards=SHARDS[:1])
        with pytest.raises(ValueError, match="RE-DECLARE FIRST"):
            _backfill(root)

    def test_declared_off_refuses(self, tmp_path, monkeypatch):
        root = tmp_path / "off"
        _build_store(root, monkeypatch, pyramid=False, shards=SHARDS[:1])
        with pytest.raises(ValueError, match="RE-DECLARE FIRST"):
            _backfill(root)

    def test_all_none_class_fields_refuse(self, tmp_path, monkeypatch):
        """The CA ATL03 case: a /2 block whose every field classified `none`."""
        root = tmp_path / "on"
        on = tmp_path / "twin"
        _build_store(root, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _build_store(on, monkeypatch, shards=SHARDS[:1])
        block = _twin_block(on)
        block["overview"]["fields"] = {n: {"class": "none"} for n in block["overview"]["fields"]}
        _install_pyramid(root, block)
        with pytest.raises(ValueError, match="class `none`"):
            _backfill(root)

    def test_absent_block_refuses(self, tmp_path, monkeypatch):
        root = tmp_path / "off"
        _build_store(root, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _install_pyramid(root, None)
        with pytest.raises(ValueError, match="RE-DECLARE FIRST"):
            _backfill(root)

    def _malformed(self, tmp_path, monkeypatch, block):
        """A pyramid-ON store whose manifest block is then hand-mangled."""
        root, on = tmp_path / "off", tmp_path / "twin"
        _build_store(root, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _build_store(on, monkeypatch, shards=SHARDS[:1])
        good = _twin_block(on)
        _install_pyramid(root, block(good) if callable(block) else block)
        return root

    def test_a_knob_shaped_overviews_list_refuses(self, tmp_path, monkeypatch):
        """`[5, 4]` is the un-normalized knob, not an expanded manifest ladder."""
        root = self._malformed(
            tmp_path, monkeypatch, lambda b: {**b, "overviews": [{"node": 4, "cells": [5]}, 4]}
        )
        with pytest.raises(ValueError, match="RE-DECLARE FIRST") as e:
            _backfill(root)
        assert "KNOB" in str(e.value) and "4" in str(e.value)

    def test_a_level_without_cells_refuses(self, tmp_path, monkeypatch):
        root = self._malformed(tmp_path, monkeypatch, lambda b: {**b, "overviews": [{"node": 4}]})
        with pytest.raises(ValueError, match="not an expanded"):
            _backfill(root)

    def test_a_non_dict_block_refuses(self, tmp_path, monkeypatch):
        root = self._malformed(tmp_path, monkeypatch, "junk")
        with pytest.raises(ValueError, match="this store declares 'junk'"):
            _backfill(root)

    def test_a_non_dict_overview_refuses(self, tmp_path, monkeypatch):
        root = self._malformed(tmp_path, monkeypatch, lambda b: {**b, "overview": "junk"})
        with pytest.raises(ValueError, match="`overview.fields` is 'junk'"):
            _backfill(root)

    def test_a_ladder_that_misses_the_shard_order_names_every_rung(self, tmp_path, monkeypatch):
        """The refusal quotes the whole schedule, not just its first rung."""
        rungs = [{"node": 3, "cells": [5]}, {"node": 2, "cells": [4]}]
        root = self._malformed(tmp_path, monkeypatch, lambda b: {**b, "overviews": rungs})
        with pytest.raises(ValueError, match="no member at the shard order 4") as e:
            _backfill(root)
        assert "'node': 3" in str(e.value) and "'node': 2" in str(e.value)

    def test_gate_refuses_before_taking_the_lease(self, tmp_path, monkeypatch):
        from zagg.sweep_lease import read_lease

        root = tmp_path / "off"
        _build_store(root, monkeypatch, pyramid=False, shards=SHARDS[:1])
        with pytest.raises(ValueError):
            _backfill(root)
        assert read_lease(str(root)) is None


class TestBackfill:
    def _upgraded(self, tmp_path, monkeypatch, *, kitchen_sink=False):
        """A pyramid-OFF store, re-declared from its pyramid-ON twin."""
        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, kitchen_sink=kitchen_sink)
        _build_store(on, monkeypatch, kitchen_sink=kitchen_sink)
        _install_pyramid(off, _twin_block(on))
        return off, on

    @pytest.mark.parametrize("kitchen_sink", [False, True])
    def test_backfilled_columns_match_the_pyramid_on_twin(
        self, tmp_path, monkeypatch, kitchen_sink
    ):
        off, on = self._upgraded(tmp_path, monkeypatch, kitchen_sink=kitchen_sink)
        for decimal in SHARDS:
            assert not _column_path(off, decimal).exists()
        summary = _backfill(off)
        assert summary["written"] == len(SHARDS)
        assert summary["current"] == summary["empty"] == summary["failed"] == 0
        assert summary["resolutions"] == [5, 4]
        for decimal in SHARDS:
            a = _objects(_column_path(on, decimal))
            b = _objects(_column_path(off, decimal))
            assert set(a) == set(b)
            for key in a:
                if key == "zarr.json":
                    assert _sans_timestamps(a[key]) == _sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], (decimal, key)

    def test_second_pass_writes_nothing(self, tmp_path, monkeypatch):
        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        before = {d: _objects(_column_path(off, d)) for d in SHARDS}
        summary = _backfill(off)
        assert summary["current"] == len(SHARDS) and summary["written"] == 0
        assert {d: _objects(_column_path(off, d)) for d in SHARDS} == before

    def test_force_rewrites_a_current_column(self, tmp_path, monkeypatch):
        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        summary = _backfill(off, force=True)
        assert summary["written"] == len(SHARDS) and summary["current"] == 0

    def test_the_gate_template_is_derived_once_per_pass(self, tmp_path, monkeypatch):
        """`column_structure` is a function of the DECLARATION, not the leaf.

        ~35 pydantic dumps and JSON round-trips a call, against 2,721 leaves
        on the CA o9 target — so the pass derives it once and threads it
        (review finding, issue #520).
        """
        import zagg.column_backfill as backfill_mod

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        calls = []
        real = backfill_mod.column_structure

        def counting(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr(backfill_mod, "column_structure", counting)
        assert _backfill(off)["current"] == len(SHARDS)
        assert len(calls) == 1

    def test_force_skips_the_stored_column_read(self, tmp_path, monkeypatch):
        """`force` rewrites the prefix wholesale, so the skip gate's read is waste.

        ``_column_state`` walks the column's groups and arrays — a LIST plus a
        ``zarr.json`` GET per member on a store with no consolidated metadata
        — and every value it returns is consumed only by the gate ``force``
        discards (review finding, issue #520).
        """
        import zagg.column_backfill as backfill_mod

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        reads = []
        real = backfill_mod._column_state

        def counting(*args, **kwargs):
            reads.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr(backfill_mod, "_column_state", counting)
        assert _backfill(off, force=True)["written"] == len(SHARDS)
        assert reads == []

    def test_declaration_drift_is_not_current(self, tmp_path, monkeypatch):
        """A narrowed declaration must not leave the wider column standing."""
        off, on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        block = _twin_block(on)
        block["overview"]["fields"] = {
            n: m for n, m in block["overview"]["fields"].items() if n != "h_tdigest"
        }
        _install_pyramid(off, block)
        summary = _backfill(off)
        assert summary["written"] == len(SHARDS)
        column = zarr.open_group(
            open_store(str(_column_path(off, SHARDS[0])), read_only=True),
            path="5",
            mode="r",
            zarr_format=3,
        )
        assert "h_tdigest" not in dict(column.arrays())

    def test_an_unstamped_leaf_or_column_is_not_current(self, tmp_path, monkeypatch):
        """`read_commit` returns None for D4 debris — a verdict, never a crash."""
        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        for absent in ({"leaf_stamp": None}, {"column_stamp": None}, {"leaf_stamp": "junk"}):
            assert _verdict(off, SHARDS[0], **absent) == (False, "absent-or-unstamped"), absent

    def test_an_added_companion_channel_is_not_current(self, tmp_path, monkeypatch):
        """The retrofit the recorded provenance cannot see (ruling 4 on issue #410)."""
        from zagg.column import _column_provenance

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        assert _verdict(off, SHARDS[0]) == (True, "current")
        fields = {n: dict(m) for n, m in _plan(off).fields.items()}
        fields["h_tdigest"]["location"] = "leaf_id"
        # Term 2 is blind to it — the stored `zagg_column` grammar records
        # neither `location` nor `temporal` — so term 3 is the only one that
        # can see the column is a `h_tdigest_locations` short in every group.
        assert _column_provenance(fields["h_tdigest"]) == _column_provenance(
            _plan(off).fields["h_tdigest"]
        )
        assert _verdict(off, SHARDS[0], fields=fields) == (False, "structure-drift")

    def test_a_dropped_companion_channel_is_not_current(self, tmp_path, monkeypatch):
        """And the other direction: a located store re-declared without the channel."""
        off, _on = self._upgraded(tmp_path, monkeypatch, kitchen_sink=True)
        _backfill(off)
        fields = {n: dict(m) for n, m in _plan(off).fields.items()}
        assert fields["h_tdigest_signal"].pop("location", None) == "leaf_id"
        assert _verdict(off, SHARDS[0], fields=fields) == (False, "structure-drift")

    def test_a_moved_exact_dtype_or_fill_is_not_current(self, tmp_path, monkeypatch):
        """The second blind spot: `dtype`/`fill_value` are not in the provenance."""
        from zagg.column import _column_provenance

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        assert _verdict(off, SHARDS[0]) == (True, "current")
        for moved in ({"dtype": "float64", "fill_value": "NaN"}, {"fill_value": -1}):
            fields = {n: dict(m) for n, m in _plan(off).fields.items()}
            assert fields["count"]["class"] == "exact"
            assert fields["count"] | moved != fields["count"]
            fields["count"].update(moved)
            assert _column_provenance(fields["count"]) == _column_provenance(
                _plan(off).fields["count"]
            )
            assert _verdict(off, SHARDS[0], fields=fields) == (False, "structure-drift"), moved

    def test_every_verdict_is_reachable_by_name(self, tmp_path, monkeypatch):
        """One direct call per verdict string, against a real current column."""
        from zagg.column_backfill import _leaf_stamp

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        stamp = _leaf_stamp(str(off), morton_word(SHARDS[0]), None, {})
        located = {n: dict(m) for n, m in _plan(off).fields.items()}
        located["h_tdigest"]["location"] = "leaf_id"
        cases = {
            "current": {},
            "absent-or-unstamped": {"column_stamp": None},
            "declaration-drift": {"resolutions": [5]},
            "structure-drift": {"fields": located},
            "stale": {"leaf_stamp": {**stamp, "written_at": "2099-01-01T00:00:00+00:00"}},
            "granule-drift": {"leaf_stamp": {**stamp, "granule_count": stamp["granule_count"] + 1}},
        }
        for reason, over in cases.items():
            assert _verdict(off, SHARDS[0], **over) == (reason == "current", reason), reason

    def test_a_re_run_leaf_is_not_current(self, tmp_path, monkeypatch):
        from zagg.hive import COMMIT_ATTR, shard_leaf_path

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        leaf = shard_leaf_path(str(off), morton_word(SHARDS[0]))
        group = zarr.open_group(open_store(leaf), path="", mode="r+", zarr_format=3)
        stamp = dict(group.attrs[COMMIT_ATTR])
        stamp["written_at"] = "2099-01-01T00:00:00+00:00"
        group.attrs[COMMIT_ATTR] = stamp
        summary = _backfill(off)
        assert summary["written"] == 1 and summary["current"] == len(SHARDS) - 1

    def test_a_changed_granule_count_is_not_current(self, tmp_path, monkeypatch):
        """The same-second backstop: the column copies the LEAF's count."""
        from zagg.hive import COMMIT_ATTR, shard_leaf_path

        off, _on = self._upgraded(tmp_path, monkeypatch)
        _backfill(off)
        leaf = shard_leaf_path(str(off), morton_word(SHARDS[0]))
        group = zarr.open_group(open_store(leaf), path="", mode="r+", zarr_format=3)
        group.attrs[COMMIT_ATTR] = {**dict(group.attrs[COMMIT_ATTR]), "granule_count": 7}
        summary = _backfill(off)
        assert summary["written"] == 1 and summary["current"] == len(SHARDS) - 1

    def test_uncommitted_leaf_contributes_nothing(self, tmp_path, monkeypatch):
        from zagg.hive import COMMIT_ATTR, shard_leaf_path

        off, _on = self._upgraded(tmp_path, monkeypatch)
        leaf = shard_leaf_path(str(off), morton_word(SHARDS[0]))
        group = zarr.open_group(open_store(leaf), path="", mode="r+", zarr_format=3)
        del group.attrs[COMMIT_ATTR]
        summary = _backfill(off)
        assert summary["empty"] == 1 and summary["written"] == len(SHARDS) - 1
        assert not _column_path(off, SHARDS[0]).exists()

    def test_an_unreadable_leaf_fails_only_itself(self, tmp_path, monkeypatch):
        from zagg.hive import shard_leaf_path

        off, _on = self._upgraded(tmp_path, monkeypatch)
        leaf = pathlib.Path(shard_leaf_path(str(off), morton_word(SHARDS[0])))
        (leaf / "6" / "count" / "zarr.json").write_text("{ not json")
        summary = _backfill(off)
        assert summary["failed"] == 1 and summary["written"] == len(SHARDS) - 1


class TestWindowedBackfill:
    """The `(leaf, window)` arm: the ONE place `time_range` is RECOVERED.

    Every other column writer computes the D15 extent from the run it is part
    of; here it comes back out of the leaf's own commit stamp
    (``leaf_stamp.get("time_range")``), where ``stamp_commit`` put it as an
    ISO pair — and ``stamp_commit`` fails closed around it (a ``time_range``
    without a ``window`` raises, a reversed pair raises), so a backfill that
    dropped or mistyped it would fail the leaf silently into ``failed``.
    Untested with it before this: ``column_name(window)``, the
    ``shard_leaf_path(window=)`` legs in ``_leaf_stamp`` / ``_column_state``,
    and the driver's multi-window ordering (review finding, issue #520).
    """

    WINDOWING = {"schedule": "yearly", "time_field": "t", "epoch": "2018-01-01T00:00:00Z"}
    WINDOW = {"label": "2019", "start": 0.0, "end": 1.0}
    #: Dataset-unit seconds the fake reports; the ISO pair below is what the
    #: leaf stamp carries and therefore what the backfill must hand on.
    TIME_RANGE = [31536000.0, 31536060.0]
    ISO_RANGE = ["2019-01-01T00:00:00+00:00", "2019-01-01T00:01:00+00:00"]

    def _twins(self, tmp_path, monkeypatch, windows=(("2019", 0.0, 1.0),)):
        off, on = tmp_path / "off", tmp_path / "on"
        for label, start, end in windows:
            common = dict(
                shards=SHARDS[:2],
                window={"label": label, "start": start, "end": end},
                windowing=self.WINDOWING,
                time_range=self.TIME_RANGE,
            )
            _build_store(off, monkeypatch, pyramid=False, **common)
            _build_store(on, monkeypatch, **common)
        _install_pyramid(off, _twin_block(on))
        return off, on

    def test_a_windowed_column_is_backfilled_byte_for_byte(self, tmp_path, monkeypatch):
        off, on = self._twins(tmp_path, monkeypatch)
        for decimal in SHARDS[:2]:
            assert _column_path(on, decimal, "2019").name == "2019.pyramid.zarr"
            assert not _column_path(off, decimal, "2019").exists()
        summary = _backfill(off, shards=SHARDS[:2], windows=("2019",))
        assert summary["written"] == 2 and summary["failed"] == 0
        for decimal in SHARDS[:2]:
            a = _objects(_column_path(on, decimal, "2019"))
            b = _objects(_column_path(off, decimal, "2019"))
            assert set(a) == set(b)
            for key in a:
                if key == "zarr.json":
                    assert _sans_timestamps(a[key]) == _sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], (decimal, key)

    def test_the_window_and_the_recovered_time_range_reach_the_stamp(self, tmp_path, monkeypatch):
        """`morton-hive/2` + the label + the leaf's OWN observed extent."""
        from zagg.column import COLUMN_ATTR
        from zagg.hive import HIVE_SPEC_V2, read_commit, shard_leaf_path

        off, on = self._twins(tmp_path, monkeypatch)
        _backfill(off, shards=SHARDS[:2], windows=("2019",))
        for decimal in SHARDS[:2]:
            column = _column_path(off, decimal, "2019")
            stamp = read_commit(open_store(str(column), read_only=True))
            assert stamp["spec"] == HIVE_SPEC_V2 == "morton-hive/2"
            assert stamp["window"] == "2019"
            assert stamp["time_range"] == self.ISO_RANGE
            # RECOVERED, not recomputed: it is the leaf's own stamped pair.
            leaf = shard_leaf_path(str(off), morton_word(decimal), window="2019")
            assert (
                stamp["time_range"] == read_commit(open_store(leaf, read_only=True))["time_range"]
            )
            # ...and the same value the pyramid-ON twin's worker wrote.
            twin = read_commit(open_store(str(_column_path(on, decimal, "2019")), read_only=True))
            assert (stamp["window"], stamp["time_range"]) == (twin["window"], twin["time_range"])
            root = zarr.open_group(
                open_store(str(column), read_only=True), path="", mode="r", zarr_format=3
            )
            assert dict(root.attrs)[COLUMN_ATTR]["window"] == "2019"

    def test_a_leaf_with_several_windows_gets_one_column_each(self, tmp_path, monkeypatch):
        """The driver's window ordering — the only multi-window path there is."""
        off, on = self._twins(
            tmp_path, monkeypatch, windows=(("2019", 0.0, 1.0), ("2020", 1.0, 2.0))
        )
        summary = _backfill(off, shards=SHARDS[:2], windows=("2020", "2019"))
        assert summary["written"] == 4
        for decimal in SHARDS[:2]:
            for label in ("2019", "2020"):
                assert _objects(_column_path(off, decimal, label))
        # Idempotent per window, not just per leaf.
        assert _backfill(off, shards=SHARDS[:2], windows=("2019", "2020"))["current"] == 4


class TestFamilyRegistration:
    def test_registered_but_not_default(self):
        from zagg.sweep import DEFAULT_FAMILIES, FAMILIES, get_family

        assert "columns" in FAMILIES and "columns" not in DEFAULT_FAMILIES
        assert get_family("columns").name == "columns"

    def test_rides_run_sweep(self, tmp_path, monkeypatch):
        from zagg.sweep import run_sweep

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False)
        _build_store(on, monkeypatch)
        _install_pyramid(off, _twin_block(on))
        summary = run_sweep(
            str(off), [(morton_word(d), None) for d in SHARDS], families=["columns"], record=False
        )
        assert summary["families"]["columns"]["written"] == len(SHARDS)
        for decimal in SHARDS:
            assert _column_path(off, decimal).exists()

    def test_partitions_split_the_work_disjointly(self, tmp_path, monkeypatch):
        from zagg.sweep import run_sweep

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False)
        _build_store(on, monkeypatch)
        _install_pyramid(off, _twin_block(on))
        written = 0
        for index in range(4):
            summary = run_sweep(
                str(off),
                [(morton_word(d), None) for d in SHARDS],
                families=["columns"],
                record=False,
                partition={"index": index, "of": 4},
            )
            written += summary["families"]["columns"]["written"]
        assert written == len(SHARDS)
        for decimal in SHARDS:
            assert _column_path(off, decimal).exists()

    def test_a_live_foreign_lease_refuses_the_pass(self, tmp_path, monkeypatch):
        from zagg.sweep_lease import SweepRefusedError, acquire_lease

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _build_store(on, monkeypatch, shards=SHARDS[:1])
        _install_pyramid(off, _twin_block(on))
        acquire_lease(str(off), run_id="live-sweep")
        with pytest.raises(SweepRefusedError, match="live-sweep"):
            _backfill(off)

    def test_a_store_wide_fault_raises_instead_of_counting_every_leaf_failed(
        self, tmp_path, monkeypatch
    ):
        """`{"written": 0, "failed": 2721}` is a fault, not a summary.

        Every leaf raising the same exception is what an expired credential
        or a denied column prefix looks like; returning it as an ordinary
        summary let the runbook march on to the staged sweep over a
        column-less store (review finding, issue #520).
        """
        import zagg.column as column_mod
        from zagg.sweep_lease import read_lease

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:2])
        _build_store(on, monkeypatch, shards=SHARDS[:2])
        _install_pyramid(off, _twin_block(on))

        def denied(*_a, **_k):
            raise PermissionError("403 on the column prefix")

        monkeypatch.setattr(column_mod, "write_column", denied)
        with pytest.raises(RuntimeError, match="wrote nothing") as e:
            _backfill(off, shards=SHARDS[:2])
        assert "PermissionError: 403 on the column prefix" in str(e.value)
        # Loud, but not torn: the lease is released before the raise.
        assert read_lease(str(off)) is None

    def test_a_single_failed_leaf_is_counted_loudly_and_still_returns(
        self, tmp_path, monkeypatch, caplog
    ):
        """One bad leaf is a leaf fault: counted, logged at ERROR, not fatal."""
        import zagg.column_backfill as backfill_mod

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:2])
        _build_store(on, monkeypatch, shards=SHARDS[:2])
        _install_pyramid(off, _twin_block(on))
        first = morton_word(SHARDS[0])
        real = backfill_mod.column_from_leaf

        def one_bad(store_root, shard_key, *a, **k):
            if shard_key == first:
                raise ValueError("digest is corrupt")
            return real(store_root, shard_key, *a, **k)

        monkeypatch.setattr(backfill_mod, "column_from_leaf", one_bad)
        with caplog.at_level("ERROR"):
            summary = _backfill(off, shards=SHARDS[:2])
        assert summary["written"] == 1 and summary["failed"] == 1
        assert "must be 0 before the staged sweep" in caplog.text

    def test_the_heartbeat_is_thrown_by_the_clock_not_a_leaf_count(self, tmp_path, monkeypatch):
        """A slow pass beats inside the TTL however FEW leaves it has run.

        The count-based interval this replaced (``seen % 64``) is an
        unenforced assumption about seconds per leaf: a two-leaf pass whose
        leaves each outlast the TTL never beat at all, and kept writing after
        the intent became claimable (review finding, issue #520). Here the
        fake clock jumps a full TTL per leaf, so every leaf must beat.
        """
        import types

        import zagg.column_backfill as backfill_mod
        import zagg.sweep_lease as sweep_lease

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:2])
        _build_store(on, monkeypatch, shards=SHARDS[:2])
        _install_pyramid(off, _twin_block(on))

        beats = []
        real_beat = sweep_lease.heartbeat_lease

        def counting_beat(*args, **kwargs):
            beats.append(1)
            return real_beat(*args, **kwargs)

        monkeypatch.setattr(sweep_lease, "heartbeat_lease", counting_beat)
        ticks = iter(range(0, 10_000 * sweep_lease.DEFAULT_TTL_S, sweep_lease.DEFAULT_TTL_S))
        monkeypatch.setattr(
            backfill_mod, "time", types.SimpleNamespace(monotonic=lambda: float(next(ticks)))
        )
        summary = _backfill(off, shards=SHARDS[:2])
        assert summary["written"] == 2
        assert len(beats) == 2

    def test_the_lease_is_released(self, tmp_path, monkeypatch):
        from zagg.sweep_lease import read_lease

        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _build_store(on, monkeypatch, shards=SHARDS[:1])
        _install_pyramid(off, _twin_block(on))
        _backfill(off)
        assert read_lease(str(off)) is None


# ---------------------------------------------------------------------------
# Phase 3: the /2 retrofit declaration (declare_pyramid's explicit levers).
# ---------------------------------------------------------------------------


def _write_run_record(root, shards=SHARDS):
    """A minimal stats parquet the listing-based discovery reads."""
    import pandas as pd

    pd.DataFrame(
        {
            "shard_key": pd.array([morton_word(d) for d in shards], dtype="UInt64"),
            "success": [True] * len(shards),
            "window": [None] * len(shards),
        }
    ).to_parquet(root / "stats_20260825T000000Z_test.parquet", engine="fastparquet")


def _declare(root, cfg, **kwargs):
    from zagg.sweep_overview import declare_pyramid

    return declare_pyramid(str(root), cfg, **kwargs)


class TestRetrofitDeclaration:
    def _off(self, tmp_path, monkeypatch, *, pyramid=False):
        root = tmp_path / "off"
        cfg, _grid = _build_store(root, monkeypatch, pyramid=pyramid, shards=SHARDS[:2])
        _write_run_record(root, SHARDS[:2])
        return root, cfg

    def test_overviews_declares_v2_on_a_pyramid_off_store(self, tmp_path, monkeypatch):
        from zagg.hive import read_manifest
        from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews

        root, cfg = self._off(tmp_path, monkeypatch)
        assert read_manifest(str(root))["pyramid"]["spec"] != PYRAMID_SPEC_V2
        summary = _declare(root, cfg, overviews=5)
        assert summary["declared_via"] == "overviews=[5]"
        assert summary["updated"] and summary["previous"] == "replaced"
        assert "orders" not in summary
        block = read_manifest(str(root))["pyramid"]
        assert block["spec"] == PYRAMID_SPEC_V2
        assert block["overviews"] == expand_overviews([5], parent_order=4)
        assert set(block["overview"]["fields"]) == {"count", "h_tdigest"}

    def test_the_declaration_makes_the_store_backfillable(self, tmp_path, monkeypatch):
        from zagg.column_backfill import manifest_column_plan
        from zagg.hive import read_manifest

        root, cfg = self._off(tmp_path, monkeypatch)
        with pytest.raises(ValueError, match="RE-DECLARE FIRST"):
            manifest_column_plan(read_manifest(str(root)))
        _declare(root, cfg, overviews=5)
        plan = manifest_column_plan(read_manifest(str(root)))
        assert plan.resolutions == [5, 4] and set(plan.fields) == {"count", "h_tdigest"}

    def test_overviews_is_validated_against_the_manifest_orders(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch)
        for bad in (4, 6, 9):
            with pytest.raises(ValueError, match="strictly between"):
                _declare(root, cfg, overviews=bad)

    def test_overviews_overrides_pyramid_false_loudly(self, tmp_path, monkeypatch, caplog):
        root, cfg = self._off(tmp_path, monkeypatch)
        with caplog.at_level("WARNING"):
            _declare(root, cfg, overviews=5)
        assert "OVERRIDES" in caplog.text

    def test_chunk_order_fires_the_default_flip(self, tmp_path, monkeypatch):
        from zagg.hive import read_manifest
        from zagg.pyramid import PYRAMID_SPEC_V2

        # A config with a pyramid knob that spells NO schedule: the shape the
        # #384 flip completes, and the one the grid-less retrofit cannot.
        root, cfg = self._off(tmp_path, monkeypatch, pyramid={})
        summary = _declare(root, cfg, chunk_order=5)
        assert summary["declared_via"] == "chunk_order=5"
        block = read_manifest(str(root))["pyramid"]
        assert block["spec"] == PYRAMID_SPEC_V2
        assert block["overviews"][0] == {"node": 4, "cells": [5]}

    def test_chunk_order_is_validated_against_the_manifest_orders(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch, pyramid={})
        for bad in (4, 6, 9):
            with pytest.raises(ValueError, match="not strictly between"):
                _declare(root, cfg, chunk_order=bad)

    def test_chunk_order_refuses_a_declared_off_config(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch)
        with pytest.raises(ValueError, match="no pyramid to default"):
            _declare(root, cfg, chunk_order=5)

    def test_chunk_order_refuses_a_spelled_schedule(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch, pyramid={"orders": [3, 2]})
        with pytest.raises(ValueError, match="is inert"):
            _declare(root, cfg, chunk_order=5)

    def test_both_levers_refuse(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch)
        with pytest.raises(ValueError, match="not both"):
            _declare(root, cfg, overviews=5, chunk_order=5)

    def test_a_raster_config_is_refused_by_name_on_both_levers(self, tmp_path, monkeypatch):
        """§4.6 does not apply to raster hive stores — so no lever may declare /2.

        The #384 default flip already exempts them, but the ``overviews=`` arm
        never goes through the flip: it writes the knob and takes
        ``build_pyramid_block``'s explicit branch, which has no reader check,
        so before this it installed a ``/2`` block on a raster store and the
        backfill would chase leaf columns that cannot exist (review finding,
        issue #520).
        """
        from dataclasses import replace

        from zagg.hive import read_manifest

        root, cfg = self._off(tmp_path, monkeypatch, pyramid={})
        raster = replace(cfg, data_source={**cfg.data_source, "reader": "raster"})
        before = read_manifest(str(root))
        for lever in ({"overviews": 5}, {"chunk_order": 5}):
            with pytest.raises(ValueError, match="`reader: raster`"):
                _declare(root, raster, **lever)
        assert read_manifest(str(root)) == before

    def test_chunk_order_refuses_a_config_grid_the_store_contradicts(self, tmp_path, monkeypatch):
        """The one raise the /2 guard still catches, and what it must now say.

        ``retrofit_declaration`` validates the lever against the MANIFEST's
        ``cell_order`` (6 here); the #384 flip gates on the CONFIG's
        ``output.grid.child_order``. Orders are packaging, outside the
        semantic core, so ``_semantic_guard`` cannot catch a config whose grid
        disagrees with the store — this guard is the only thing that does.
        """
        from dataclasses import replace

        from zagg.hive import read_manifest

        root, cfg = self._off(tmp_path, monkeypatch, pyramid={})
        narrowed = replace(
            cfg, output={**cfg.output, "grid": {**cfg.output["grid"], "child_order": 5}}
        )
        before = read_manifest(str(root))
        with pytest.raises(ValueError, match="does not describe this store"):
            _declare(root, narrowed, chunk_order=5)
        assert read_manifest(str(root)) == before

    def test_no_lever_keeps_todays_behaviour(self, tmp_path, monkeypatch):
        root, cfg = self._off(tmp_path, monkeypatch, pyramid={"orders": [3, 2]})
        summary = _declare(root, cfg)
        assert summary["declared_via"] == "config"
        assert summary["orders"] == [3, 2] and "overviews" not in summary

    def test_nothing_is_written_when_a_lever_refuses(self, tmp_path, monkeypatch):
        from zagg.hive import read_manifest

        root, cfg = self._off(tmp_path, monkeypatch)
        before = read_manifest(str(root))
        with pytest.raises(ValueError):
            _declare(root, cfg, overviews=6)
        assert read_manifest(str(root)) == before

    def test_cli_overviews_requires_declare_pyramid(self):
        from zagg.sweep import main

        with pytest.raises(SystemExit) as e:
            main(["/nonexistent", "--overviews", "5"])
        assert e.value.code == 2

    def test_cli_declares_v2(self, tmp_path, monkeypatch, capsys):
        import yaml

        from zagg.hive import read_manifest
        from zagg.pyramid import PYRAMID_SPEC_V2
        from zagg.sweep import main

        root, cfg = self._off(tmp_path, monkeypatch)
        config_yaml = tmp_path / "retrofit.yaml"
        config_yaml.write_text(
            yaml.safe_dump(
                {
                    "data_source": cfg.data_source,
                    "aggregation": cfg.aggregation,
                    "output": {**cfg.output, "pyramid": False},
                }
            )
        )
        assert main([str(root), "--declare-pyramid", str(config_yaml), "--overviews", "5"]) == 0
        assert json.loads(capsys.readouterr().out)["declared_via"] == "overviews=[5]"
        assert read_manifest(str(root))["pyramid"]["spec"] == PYRAMID_SPEC_V2


# ---------------------------------------------------------------------------
# Phase 4: the /1 -> /2 upgrade, end to end and offline (the acceptance).
# ---------------------------------------------------------------------------


#: The artifact roles the acceptance compares, and the attrs block each one
#: records its provenance in. Overviews alone left SEVEN of the sixteen
#: artifacts this recipe produces compared by nothing — the four backfilled
#: LEAF columns (this PR's whole output) and the three §4.6 stage columns the
#: staged sweep gathers, each with its own ``regime``, ``generation``,
#: ``source_children`` and ``run_id`` (review finding, issue #520).
COMPARED_ROLES = {"overview": "zagg_overview", "column": "zagg_column"}


def _compared(root):
    """``(relative path, role, provenance block, open group)`` per artifact."""
    for path in sorted(pathlib.Path(root).rglob("*.zarr"), key=str):
        group = zarr.open_group(
            open_store(str(path), read_only=True), path="", mode="r", zarr_format=3
        )
        attrs = dict(group.attrs)
        block = COMPARED_ROLES.get(attrs.get("role"))
        if block is None:
            continue
        yield str(path.relative_to(root)), attrs["role"], attrs[block], group


def _resolution_groups(role, block, group) -> list:
    """Which resolution groups this artifact's arrays live under.

    Keyed off the ROLE, not assumed: an overview carries exactly one group
    (its own ``cell_order``), a column one per declared resolution.
    """
    return [str(block["cell_order"])] if role == "overview" else sorted(dict(group.groups()))


def _ladder(root) -> dict:
    """Every compared artifact's arrays, keyed by store-relative path."""
    out = {}
    for path, role, block, group in _compared(root):
        out[path] = {
            (res, name): (
                [bytes(p or b"") for p in arr[:]] if arr.dtype == object else arr[:].tobytes()
            )
            for res in _resolution_groups(role, block, group)
            for name, arr in group[res].arrays()
        }
    return out


#: Overview provenance keys that are run-local by construction: the wall clock
#: and the sweep run's own id (and, inside ``generation``, the leaf clock and
#: the run-id set the #417 skip key carries). Two runs of the same sweep over
#: the same bytes differ in exactly these and nothing else — which is the
#: phase 4 claim, so they are named here rather than compared away wholesale.
RUN_LOCAL = ("generated_at", "run_id")
RUN_LOCAL_GENERATION = ("max_leaf_timestamp", "run_ids")


def _overview_attrs(root) -> dict:
    """Every compared artifact's provenance block, minus the run-local terms.

    The same terms for a column as for an overview: two runs over the same
    bytes differ in the wall clock and the run id, and in nothing else.
    """
    out = {}
    for path, _role, raw, _group in _compared(root):
        block = {k: v for k, v in raw.items() if k not in RUN_LOCAL}
        generation = block.get("generation")
        if isinstance(generation, dict):
            block["generation"] = {
                k: v for k, v in generation.items() if k not in RUN_LOCAL_GENERATION
            }
        out[path] = block
    return out


def _manifest_overviews(root) -> list:
    """The manifest's ``overviews`` list, actuals minus their run-local terms."""
    from zagg.hive import read_manifest

    out = []
    for entry in read_manifest(str(root))["pyramid"]["overviews"]:
        entry = dict(entry)
        if isinstance(entry.get("actuals"), dict):
            entry["actuals"] = {k: v for k, v in entry["actuals"].items() if k not in RUN_LOCAL}
        out.append(entry)
    return out


class TestUpgradeEndToEnd:
    """pyramid-OFF -> declare -> backfill -> CLI staged sweep == pyramid-ON twin.

    The whole ``/1 -> /2`` recipe, on local-backend stores, with no fleet: the
    transport is the only thing a fleet arm changes, and the ladder the staged
    sweep builds is what this pins — every artifact of it, the
    backfilled leaf columns and the stage columns included, not the overviews
    alone (``COMPARED_ROLES``).
    """

    def _twins(self, tmp_path, monkeypatch, *, kitchen_sink=False):
        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, kitchen_sink=kitchen_sink)
        cfg, _grid = _build_store(on, monkeypatch, kitchen_sink=kitchen_sink)
        for root in (off, on):
            _write_run_record(root)
        return off, on, cfg

    @staticmethod
    def _staged_sweep(root):
        """The EXISTING CLI staged sweep — `python -m zagg.sweep <root> --stages`."""
        from zagg.sweep import main

        assert main([str(root), "--stages"]) == 0

    @pytest.mark.parametrize("kitchen_sink", [False, True])
    def test_upgraded_store_matches_the_pyramid_on_twin(self, tmp_path, monkeypatch, kitchen_sink):
        from zagg.hive import read_manifest
        from zagg.sweep import run_sweep

        off, on, cfg = self._twins(tmp_path, monkeypatch, kitchen_sink=kitchen_sink)

        # 1. re-declare the /2 grammar (phase 3's lever)
        summary = _declare(off, cfg, overviews=5)
        assert summary["declared_via"] == "overviews=[5]"
        assert (
            read_manifest(str(off))["pyramid"]["overviews"]
            == read_manifest(str(on))["pyramid"]["overviews"]
        )

        # 2. backfill the columns the pre-column build never wrote
        backfill = run_sweep(
            str(off), [(morton_word(d), None) for d in SHARDS], families=["columns"], record=False
        )
        assert backfill["families"]["columns"]["written"] == len(SHARDS)

        # 3. the SAME staged sweep over both stores
        self._staged_sweep(off)
        self._staged_sweep(on)

        # 4. the ladders agree, byte for byte
        upgraded, native = _ladder(off), _ladder(on)
        assert set(upgraded) == set(native) and upgraded
        # Every artifact the recipe produces, not just the ladder: 9 overviews
        # (one per above-shard node the leaves reach — the fixed every-order
        # ladder from order 3 down to 0 over two base cells), the 4 backfilled
        # LEAF columns, and the 3 §4.6 stage columns the staged sweep gathers.
        assert len(upgraded) == 16, sorted(upgraded)
        roles = [role for _p, role, _b, _g in _compared(off)]
        assert (roles.count("overview"), roles.count("column")) == (9, 7)
        assert upgraded == native
        assert _overview_attrs(off) == _overview_attrs(on)
        # ...and so does the declaration's materialization inventory.
        assert _manifest_overviews(off) == _manifest_overviews(on)

    def test_the_upgrade_is_a_no_op_on_a_store_that_never_needed_it(self, tmp_path, monkeypatch):
        """A pyramid-ON store re-declared + backfilled writes no new column."""
        from zagg.sweep import run_sweep

        on = tmp_path / "on"
        cfg, _grid = _build_store(on, monkeypatch)
        _write_run_record(on)
        before = {d: _objects(_column_path(on, d)) for d in SHARDS}
        summary = _declare(on, cfg, overviews=5)
        assert summary["previous"] == "identical" and not summary["updated"]
        backfill = run_sweep(
            str(on), [(morton_word(d), None) for d in SHARDS], families=["columns"], record=False
        )
        assert backfill["families"]["columns"]["current"] == len(SHARDS)
        assert {d: _objects(_column_path(on, d)) for d in SHARDS} == before


class TestInFlightWarning:
    """The status-channel smoke alarm (espg ruling 2026-08-25 on PR #524 Q1).

    Advisory only: it warns, it never refuses, and it is fail-open — a status
    channel that cannot be listed is not a reason to block an upgrade.
    """

    def _status_object(self, root, run_id, *, age_s=0.0):
        """One ``{store}.status/run-<id>/shard-*.json`` object, aged in place."""
        import os
        import time

        prefix = pathlib.Path(f"{str(root).rstrip('/')}.status") / f"run-{run_id}"
        prefix.mkdir(parents=True, exist_ok=True)
        obj = prefix / "shard-1.json"
        obj.write_text(json.dumps({"status": "ok"}))
        if age_s:
            stamped = time.time() - age_s
            os.utime(obj, (stamped, stamped))
        return obj

    def _upgraded(self, tmp_path, monkeypatch):
        off, on = tmp_path / "off", tmp_path / "on"
        _build_store(off, monkeypatch, pyramid=False, shards=SHARDS[:1])
        _build_store(on, monkeypatch, shards=SHARDS[:1])
        _install_pyramid(off, _twin_block(on))
        return off

    def test_a_recent_status_object_warns_and_the_pass_still_completes(
        self, tmp_path, monkeypatch, caplog
    ):
        off = self._upgraded(tmp_path, monkeypatch)
        self._status_object(off, "live-fleet-run")
        with caplog.at_level("WARNING", logger="zagg.column_backfill"):
            summary = _backfill(off, shards=SHARDS[:1])
        assert "live-fleet-run" in caplog.text
        assert "never a refusal" in caplog.text
        # Advisory ONLY: the upgrade ran to completion and wrote the column.
        assert summary["written"] == 1 and summary["failed"] == 0
        assert summary["runs_in_flight"] == ["live-fleet-run"]
        assert _column_path(off, SHARDS[0]).exists()

    def test_an_old_status_object_is_silent(self, tmp_path, monkeypatch, caplog):
        """Older than one worker wall: its writer is gone either way."""
        from zagg.column_backfill import IN_FLIGHT_WINDOW_S

        off = self._upgraded(tmp_path, monkeypatch)
        self._status_object(off, "finished-run", age_s=IN_FLIGHT_WINDOW_S + 60)
        with caplog.at_level("WARNING", logger="zagg.column_backfill"):
            summary = _backfill(off, shards=SHARDS[:1])
        assert "finished-run" not in caplog.text
        assert summary["runs_in_flight"] == [] and summary["written"] == 1

    def test_no_status_channel_is_silent(self, tmp_path, monkeypatch, caplog):
        off = self._upgraded(tmp_path, monkeypatch)
        assert not pathlib.Path(f"{off}.status").exists()
        with caplog.at_level("WARNING", logger="zagg.column_backfill"):
            summary = _backfill(off, shards=SHARDS[:1])
        assert "status channel" not in caplog.text
        assert summary["runs_in_flight"] == [] and summary["written"] == 1

    def test_a_listing_that_raises_is_silent_and_the_pass_completes(
        self, tmp_path, monkeypatch, caplog
    ):
        """Fail-open: an unreadable channel must not block an upgrade."""
        import obstore

        off = self._upgraded(tmp_path, monkeypatch)
        self._status_object(off, "unreadable-run")

        def boom(*args, **kwargs):
            raise PermissionError("status channel is credential-scoped away")

        monkeypatch.setattr(obstore, "list", boom)
        with caplog.at_level("WARNING", logger="zagg.column_backfill"):
            summary = _backfill(off, shards=SHARDS[:1])
        assert "unreadable-run" not in caplog.text
        assert summary["runs_in_flight"] == [] and summary["written"] == 1
        assert _column_path(off, SHARDS[0]).exists()

    def test_the_probe_never_raises_on_a_nonsense_root(self):
        from zagg.column_backfill import warn_if_runs_in_flight

        assert warn_if_runs_in_flight("s3://no-such-bucket-zagg-520/x", {}) == []
