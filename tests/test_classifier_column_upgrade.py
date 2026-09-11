"""Issue #548: the classifier-0.50 field upgrade for count-only pyramid columns.

The live ATL03 store's leaf columns split by the classifier era that wrote
them: ~2,713 shards carry 08-19-era ``all.pyramid.zarr`` columns holding
``count`` only (the 0.48 classifiers rated every digest field ``none``),
while ~204 shards touched by 0.52-era code carry the full 4-field set
(``count`` + both strata digests + ``composition``). GEDI has no columns at
all. The backfill machinery is PR #524 (issue #520); these tests pin the
three #548 claims against it, on fixture stores:

1. **current classifiers produce the 4-field set** — the shipped strata
   template's column schema is exactly the 204 exemplars' (the class map
   itself is pinned in ``test_strata_composability.py``);
2. **a count-only column upgrades in place** — the skip gate keys on the
   RECORDED field set (``column_is_current``'s declaration term), never on
   column existence, so the 2,713 stale shards rewrite while the 204
   current ones skip — and the keying is bidirectional, which is why the
   runbook re-declares BEFORE backfilling (the vendored live CA manifest
   yields a count-only plan that would downgrade a 4-field column);
3. **the field list is derived from the classifiers at run time** —
   ``declare_pyramid`` replaces a 0.48-era all-``none`` map wholesale
   (``build_pyramid_block`` -> ``declared_fields`` -> the D24 classifiers),
   so the re-declaration inherits the upgrade with no frozen field list;

plus the GEDI-shaped from-scratch case: a ``weights: flux`` store with a
``/2`` declaration and no columns backfills to the pyramid-ON twin's bytes
(the one §2.0 surface ``test_column_backfill.py`` does not cover).
"""

from __future__ import annotations

import json
import pathlib

import pytest
import test_column_backfill as tcb

from zagg.grids.morton import morton_word

CA_MANIFEST = pathlib.Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json"

#: The 4-field composable set the ≥0.50 classifiers admit for the ATL03
#: strata shape, plus the located siblings each digest's declaration adds to
#: every materialized group (ruling 4 on issue #410).
ATL03_COLUMN_ARRAYS = {
    "morton",
    "count",
    "h_tdigest_signal",
    "h_tdigest_signal_locations",
    "h_tdigest_noise",
    "h_tdigest_noise_locations",
    "composition",
}


def _count_only(plan) -> dict:
    """The 0.48-era composable set: ``count`` and nothing else."""
    return {"count": plan.fields["count"]}


def _rewrite_count_only(root, decimal, plan):
    """Rewrite one leaf's column as an 08-19-era build would have written it.

    The 0.48 classifiers rated every digest field ``none``, so the worker's
    ``composable_fields`` filter passed ``count`` alone — the same writer,
    the same leaf bytes, a one-entry field map. ``column_from_leaf`` +
    ``write_column`` is exactly that recipe re-run under the era's filter.
    """
    from zagg.column import write_column
    from zagg.column_backfill import _leaf_stamp, column_from_leaf

    shard = morton_word(decimal)
    fields = _count_only(plan)
    folded = column_from_leaf(
        str(root),
        shard,
        fields,
        node_order=plan.node_order,
        cell_order=plan.cell_order,
        resolutions=plan.resolutions,
    )
    stamp = _leaf_stamp(str(root), shard, None, {})
    write_column(
        str(root),
        shard,
        folded,
        fields,
        node_order=plan.node_order,
        cell_order=plan.cell_order,
        granule_count=int(stamp["granule_count"]),
    )


def _era_048_fields(block) -> dict:
    """A ``/2`` block's field map as the 0.48 classifiers declared it."""
    return {
        name: (entry if entry.get("class") == "exact" else {"class": "none"})
        for name, entry in block["overview"]["fields"].items()
    }


class TestFourFieldSchema:
    """#548 (1): a column materialized today carries the 4-field schema."""

    def test_strata_template_column_structure_is_the_exemplar_schema(self):
        """The shipped ATL03 strata template's column template, member by member.

        ``column_structure`` is the projection the backfill's skip gate
        compares stored columns against, derived from the same machinery
        ``write_column`` writes with — so this is the 204 exemplar shards'
        schema, pinned from the CURRENT classifiers alone (the class map is
        pinned in ``test_strata_composability.py::TestD24Classification``).
        """
        from zagg.column import composable_fields
        from zagg.column_backfill import column_structure
        from zagg.config import default_config
        from zagg.pyramid import declared_fields

        fields, excluded = declared_fields(default_config("atl03_tdigest_strata_healpix"))
        assert excluded == []
        composable = composable_fields(fields)
        assert sorted(composable) == [
            "composition",
            "count",
            "h_tdigest_noise",
            "h_tdigest_signal",
        ]
        structure = column_structure(composable, node_order=11, resolutions=[13, 11])
        assert set(structure) == {"13", "11"}
        for group in structure.values():
            assert set(group) == ATL03_COLUMN_ARRAYS

    def test_a_built_column_carries_the_four_field_set(self, tmp_path, monkeypatch):
        """The kitchen-sink fixture (the ATL03 shape at test scale), on disk."""
        import zarr

        from zagg.store import open_store

        root = tmp_path / "on"
        tcb._build_store(root, monkeypatch, shards=tcb.SHARDS[:1], kitchen_sink=True)
        column = zarr.open_group(
            open_store(str(tcb._column_path(root, tcb.SHARDS[0])), read_only=True),
            path="",
            mode="r",
            zarr_format=3,
        )
        for _res, group in column.groups():
            assert set(dict(group.arrays())) == ATL03_COLUMN_ARRAYS


class TestCountOnlyUpgradeInPlace:
    """#548 (2): a count-only column is upgrade-in-place, never already-done."""

    def _era_048_store(self, tmp_path, monkeypatch):
        """The live ATL03 state at test scale: full leaves, 0.48 artifacts.

        Both stores build pyramid-ON from identical inputs (the leaf arrays
        are classifier-independent); ``era`` is then demoted to what an
        08-19 build left behind — every column rewritten count-only, and the
        manifest's field map re-declared with the digests ``none``.
        """
        era, on = tmp_path / "era", tmp_path / "on"
        cfg, _grid = tcb._build_store(era, monkeypatch, kitchen_sink=True)
        tcb._build_store(on, monkeypatch, kitchen_sink=True)
        plan = tcb._plan(era)
        for decimal in tcb.SHARDS:
            _rewrite_count_only(era, decimal, plan)
        block = tcb._twin_block(on)
        block["overview"]["fields"] = _era_048_fields(block)
        tcb._install_pyramid(era, block)
        return era, on, cfg

    def test_a_count_only_column_reads_declaration_drift(self, tmp_path, monkeypatch):
        """The verdict itself: presence keys on the FIELD SET, not existence."""
        era, _on, _cfg = self._era_048_store(tmp_path, monkeypatch)
        # Under its own era's declaration the count-only column is current —
        # which is exactly why the 08-19 store was internally consistent.
        assert tcb._verdict(era, tcb.SHARDS[0]) == (True, "current")
        # Under the current 4-field declaration it is declaration drift.
        on_plan = tcb._plan(tmp_path / "on")
        assert tcb._verdict(era, tcb.SHARDS[0], fields=on_plan.fields) == (
            False,
            "declaration-drift",
        )

    def test_redeclaration_derives_the_field_map_from_current_classifiers(
        self, tmp_path, monkeypatch
    ):
        """#548 (3): the all-``none`` map is replaced at run time, not inherited."""
        from zagg.hive import read_manifest

        era, _on, cfg = self._era_048_store(tmp_path, monkeypatch)
        before = read_manifest(str(era))["pyramid"]["overview"]["fields"]
        assert {n: e["class"] for n, e in before.items()} == {
            "count": "exact",
            "h_tdigest_signal": "none",
            "h_tdigest_noise": "none",
            "composition": "none",
        }
        summary = tcb._declare(era, cfg, overviews=5)
        assert summary["fields"] == {
            "count": "exact",
            "h_tdigest_signal": "approximate",
            "h_tdigest_noise": "approximate",
            "composition": "packed",
        }
        after = read_manifest(str(era))["pyramid"]["overview"]["fields"]
        assert after["composition"]["of"] == "h_tdigest_signal"
        assert after["h_tdigest_signal"]["location"] == "leaf_id"

    def test_count_only_columns_backfill_to_the_four_field_set(self, tmp_path, monkeypatch):
        """The whole #548 recipe: re-declare, backfill upgrades in place, idempotent."""
        era, on, cfg = self._era_048_store(tmp_path, monkeypatch)
        tcb._declare(era, cfg, overviews=5)
        summary = tcb._backfill(era)
        assert summary["written"] == len(tcb.SHARDS)
        assert summary["current"] == summary["empty"] == summary["failed"] == 0
        for decimal in tcb.SHARDS:
            a = tcb._objects(tcb._column_path(on, decimal))
            b = tcb._objects(tcb._column_path(era, decimal))
            assert set(a) == set(b), decimal
            for key in a:
                if key == "zarr.json":
                    assert tcb._sans_timestamps(a[key]) == tcb._sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], (decimal, key)
        # Idempotent: the upgraded columns are already-done on the next pass.
        again = tcb._backfill(era)
        assert again["current"] == len(tcb.SHARDS) and again["written"] == 0

    def test_a_mixed_store_rewrites_the_stale_shards_only(self, tmp_path, monkeypatch):
        """The live mix — ~204 four-field + ~2,713 count-only — at test scale."""
        root = tmp_path / "mixed"
        tcb._build_store(root, monkeypatch, kitchen_sink=True)
        plan = tcb._plan(root)
        pristine = {d: tcb._objects(tcb._column_path(root, d)) for d in tcb.SHARDS}
        stale = tcb.SHARDS[:2]
        for decimal in stale:
            _rewrite_count_only(root, decimal, plan)
        summary = tcb._backfill(root)
        assert summary["written"] == len(stale)
        assert summary["current"] == len(tcb.SHARDS) - len(stale)
        for decimal in tcb.SHARDS:
            a, b = pristine[decimal], tcb._objects(tcb._column_path(root, decimal))
            assert set(a) == set(b)
            for key in a:
                if key == "zarr.json":
                    assert tcb._sans_timestamps(a[key]) == tcb._sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], (decimal, key)


class TestLiveCaManifestPlan:
    """The vendored live CA manifest against the backfill's declaration gate."""

    def test_the_live_manifest_yields_a_count_only_plan(self):
        """NOT refused: ``count`` is composable, so the gate passes it through.

        The refusal arm fires only when EVERY field is ``none`` — a
        count-composable declaration is legal, so a backfill against the live
        manifest as published would write count-only columns. That is why
        the #547 runbook's step 1 (re-declare) precedes step 2 uncondition-
        ally: the plan is the manifest's, and the manifest is 0.48's.
        """
        from zagg.column_backfill import manifest_column_plan

        plan = manifest_column_plan(json.loads(CA_MANIFEST.read_text()))
        assert sorted(plan.fields) == ["count"]
        assert (plan.node_order, plan.cell_order) == (9, 19)

    def test_a_four_field_column_under_the_count_only_plan_is_drift(self, tmp_path, monkeypatch):
        """The keying is bidirectional — which protects nothing by itself.

        A 4-field exemplar column reads ``declaration-drift`` under a
        count-only plan exactly as the reverse does, so a backfill run
        against the un-re-declared manifest would DOWNGRADE the ~204
        0.52-era shards. The field-set keying makes both directions loud in
        the summary (``written``, never ``current``); the ordering guarantee
        lives in the runbook, and this pin is what makes skipping step 1
        a visible rewrite rather than a silent no-op.
        """
        root = tmp_path / "on"
        tcb._build_store(root, monkeypatch, shards=tcb.SHARDS[:1], kitchen_sink=True)
        plan = tcb._plan(root)
        assert tcb._verdict(root, tcb.SHARDS[0]) == (True, "current")
        assert tcb._verdict(root, tcb.SHARDS[0], fields=_count_only(plan)) == (
            False,
            "declaration-drift",
        )


def _build_flux_store(root, monkeypatch, *, pyramid=None, shards=tcb.SHARDS):
    """A GEDI-shaped hive store: ``count`` + a ``weights: flux`` digest.

    ``tcb._build_store``'s recipe over the fixture generator's flux arm —
    the §2.0 surface the #524 suite does not drive through the backfill.
    ``pyramid=False`` is the live ``gedi_flux_o9.zarr`` state: built
    pyramid-off, no columns, no declaration.
    """
    from dataclasses import replace

    import zagg.processing as processing
    from zagg import hive
    from zagg.grids import HealpixGrid

    gen = tcb._generator()
    cfg = gen._config(False, pyramid=pyramid, flux=True)
    cfg = replace(cfg, data_source={**cfg.data_source, "variables": {"h": "g/h"}})
    grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=True)
    root.mkdir(parents=True, exist_ok=True)
    hive.ensure_manifest(
        str(root), hive.build_manifest(grid, dataset={"short_name": "FLUX_TEST", "version": "1"})
    )
    for decimal in shards:
        shard = morton_word(decimal)
        by_chunk, _cells = gen._build_cells(grid, shard, kitchen_sink=False, flux=True)
        inner = gen._fake_process_shard(grid, by_chunk, kitchen_sink=False, ragged_field="rx_flux")

        def fake(*args, _inner=inner, **kwargs):
            if kwargs.get("chunk_results") is None:
                kwargs["chunk_results"] = []
            df, meta = _inner(*args, **kwargs)
            meta["phase_timings"] = {"read": 0.0, "index": 0.0, "aggregate": 0.0}
            return df, meta

        monkeypatch.setattr(processing, "process_shard", fake)
        meta = hive.process_and_write_hive(
            shard, ["s3://fixture/a.h5"], grid, {}, str(root), cfg, store_kwargs={}
        )
        assert meta.get("error") is None, meta.get("error")
    return cfg, grid


class TestGediFromScratch:
    """#548's GEDI arm: a flux store with a declaration and no columns."""

    def test_gedi_template_classifies_count_and_flux_composable(self):
        """The shipped GEDI template under today's classifiers.

        ``rx_flux`` is ``approximate`` (the waveform digest joined the digest
        family in issue #508; ``temporal: per-centroid`` is the §8.3 shape,
        not the ``per-cell`` exclusion), so a GEDI ``declare_pyramid`` today
        admits ``{count, rx_flux}`` and a backfill materializes columns
        carrying both plus the ``rx_flux_times`` sibling. Every per-shot
        companion stays ``none``.
        """
        from zagg.config import default_config
        from zagg.pyramid import declared_fields
        from zagg.semantics import composability_classes

        cfg = default_config("gedi01b_waveform_healpix_hive")
        classes = composability_classes(cfg)
        assert classes["count"] == "exact" and classes["rx_flux"] == "approximate"
        assert {n for n, c in classes.items() if c != "none"} == {"count", "rx_flux"}
        fields, excluded = declared_fields(cfg)
        assert fields["rx_flux"]["temporal"] == "per-centroid"
        assert sorted(excluded) == [
            "elevation_bin0",
            "elevation_lastbin",
            "noise_mean",
            "noise_stddev",
            "rx_energy",
            "shot_count",
            "shot_number",
        ]

    def test_a_flux_store_backfills_from_scratch_to_the_twin(self, tmp_path, monkeypatch):
        """pyramid-off flux build -> declare -> backfill == the pyramid-ON twin."""
        from zagg.hive import read_manifest

        off, on = tmp_path / "off", tmp_path / "on"
        cfg, _grid = _build_flux_store(off, monkeypatch, pyramid=False)
        _build_flux_store(on, monkeypatch)
        for decimal in tcb.SHARDS:
            assert not tcb._column_path(off, decimal).exists()
        summary = tcb._declare(off, cfg, overviews=5)
        assert summary["fields"] == {"count": "exact", "rx_flux": "approximate"}
        # The §2.0 declaration rides the manifest entry with its calibration
        # (issue #424): the backfill's stored-weights gate compares against it.
        entry = read_manifest(str(off))["pyramid"]["overview"]["fields"]["rx_flux"]
        assert entry["weights"] == "flux" and entry["gain"]["name"] == "spec-fixture-gain"
        result = tcb._backfill(off)
        assert result["written"] == len(tcb.SHARDS) and result["failed"] == 0
        for decimal in tcb.SHARDS:
            a = tcb._objects(tcb._column_path(on, decimal))
            b = tcb._objects(tcb._column_path(off, decimal))
            assert set(a) == set(b), decimal
            for key in a:
                if key == "zarr.json":
                    assert tcb._sans_timestamps(a[key]) == tcb._sans_timestamps(b[key])
                else:
                    assert a[key] == b[key], (decimal, key)
        assert tcb._backfill(off)["current"] == len(tcb.SHARDS)

    def test_a_mismatched_stored_weights_declaration_fails_the_leaf(self, tmp_path, monkeypatch):
        """The §2.0 gate through the flux arm: counts-declared over flux bytes."""
        off, on = tmp_path / "off", tmp_path / "on"
        _build_flux_store(off, monkeypatch, pyramid=False, shards=tcb.SHARDS[:1])
        _build_flux_store(on, monkeypatch, shards=tcb.SHARDS[:1])
        block = tcb._twin_block(on)
        # A hand-edited declaration dropping the flux key: absent means
        # counts (spec §2.0), which the stored arrays falsify.
        block["overview"]["fields"]["rx_flux"] = {
            k: v for k, v in block["overview"]["fields"]["rx_flux"].items() if k != "weights"
        }
        tcb._install_pyramid(off, block)
        with pytest.raises(RuntimeError, match="weights declaration"):
            tcb._backfill(off, shards=tcb.SHARDS[:1])
