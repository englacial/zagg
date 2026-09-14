"""Issue #515: strata + composition through the D24 composability gate.

Phase 1 — characterization. ``build_tdigest_where`` (the strata builder) and
the packed composition word are both admitted by the SPILL fold gate
(``_TDIGEST_SPILL_FUNCTIONS`` / the ``validate_spill_fold`` scalar branch,
issues #370/#321) — the published CA store's 2,726 shards were built by k-way
merging per-block strata partials under exactly those laws — yet D24
classifies every one of their fields ``none``, so the live manifest promises
no overview fold for the very payloads whose bytes rest on one. These tests
pin that drift before the admission changes it:

- the LIVE CA-manifest declaration, vendored byte-for-byte as an in-tree
  fixture (``tests/data/ca_atl03_tdigest_o9_morton_hive.json``, fetched from
  ``atl03_tdigest_o9.zarr/morton_hive.json`` on 2026-08-24) — the permanent
  BEFORE record no later phase edits;
- today's classifier verdicts and the spill-gate admissions — the pins later
  phases flip deliberately, one per admission.

Phase 2 — the ``build_tdigest_where`` admission (espg-ruled admit,
2026-08-24): the strata builder joins ``_DIGEST_FAMILY_FUNCTIONS``, so strata
fields classify ``approximate`` and fold through the pyramid by the k-way law
with both companion channels; parity is asserted through the sweep's own fold
seam (``fold_digests``).
"""

import json
import logging
from pathlib import Path

import numpy as np
import pytest

from zagg.config import default_config, get_agg_fields
from zagg.pyramid import declared_fields
from zagg.semantics import composability_classes, field_composability

CA_MANIFEST = Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json"

#: The live store's per-field declaration, exactly as published (issue #515):
#: count folds, everything the store is FOR does not.
CA_FIELDS_BEFORE = {
    "count": {
        "class": "exact",
        "method": "sum",
        "nan_policy": "skip",
        "dtype": "int32",
        "fill_value": 0,
    },
    "h_tdigest_signal": {"class": "none"},
    "h_tdigest_noise": {"class": "none"},
    "composition": {"class": "none"},
}


class TestLiveCaManifestFixture:
    """The vendored CA manifest is the historical BEFORE — never edited."""

    def test_fixture_pins_the_live_declaration(self):
        # The known-answer half of the issue #515 before/after: the published
        # store declares its strata and composition non-composable while its
        # own bytes were built by their merge laws (spill fold, issue #370).
        manifest = json.loads(CA_MANIFEST.read_text())
        assert manifest["spec"] == "morton-hive/1"
        assert manifest["shard_order"] == 9
        assert manifest["cell_order"] == 19
        # Identity of the RECORD, not of a config: this hash names the store
        # the issue is about, and no config in the tree reproduces it — the
        # shipped template hashes 0ac7d33b..., the benchmark strata config
        # 5ebf740f.... So the before/after below is pinned against reality
        # for the per-field DECLARATION only; the store's semantic core is
        # not rebuildable from here. It is also a FROZEN manifest key
        # (``hive._FROZEN_MANIFEST_KEYS``), so the retrofit write the PR body
        # plans against this root needs ``overwrite=True`` or ``ensure_manifest``
        # refuses it.
        assert (
            manifest["semantic_hash"]
            == "b9b15fdde78f147c15c929da8ca93de21930ad5c552ae082c5d8998fb83ada21"
        )
        assert manifest["pyramid"]["overview"]["fields"] == CA_FIELDS_BEFORE

    def test_fixture_declares_a_full_cascade_ladder(self):
        # The retrofit context: the ladder is declared top to bottom (nodes
        # 9..0), fold_source cascade — so the ONLY thing standing between this
        # store and digest pyramids is the per-field class map above.
        manifest = json.loads(CA_MANIFEST.read_text())
        pyramid = manifest["pyramid"]
        assert pyramid["spec"] == "zagg-pyramid/2"
        assert [e["node"] for e in pyramid["overviews"]] == list(range(9, -1, -1))
        assert pyramid["overview"]["fold_source"] == "cascade"
        assert pyramid["overview"]["exact_levels"] == 1

    def test_declaration_writer_rebuilds_the_published_block(self):
        # The known-answer proper: today's declaration writer, handed the CA
        # store's grid (parent 9 / chunk_inner 13 / cell 19), rebuilds the
        # published block — so the fixture is checked against CODE, not only
        # against itself, and a ``build_pyramid_block`` regression fails here
        # as loudly as a ``declared_fields`` one.
        from zagg.sweep_overview import build_pyramid_block

        config = default_config("atl03_tdigest_strata_healpix")
        config.output["pyramid"] = {}  # the template ships ``pyramid: false``
        config.output["grid"]["parent_order"] = 9  # the CA store's shard order
        block = build_pyramid_block(config, 9, chunk_order=13)
        published = json.loads(CA_MANIFEST.read_text())["pyramid"]
        assert block["spec"] == published["spec"]
        assert block["overviews"] == published["overviews"]  # the 9..0 ladder + cells
        assert {k: v for k, v in block["overview"].items() if k != "fields"} == {
            k: v for k, v in published["overview"].items() if k != "fields"
        }
        # The per-field map is the ONLY divergence, and only in the ruled
        # entries: count byte-identical, the two strata fields the phase-2
        # admission, composition the phase-3 packed declaration.
        fields = block["overview"]["fields"]
        assert set(fields) == set(CA_FIELDS_BEFORE)
        assert [n for n in CA_FIELDS_BEFORE if fields[n] != CA_FIELDS_BEFORE[n]] == [
            "h_tdigest_signal",
            "h_tdigest_noise",
            "composition",
        ]
        assert fields["h_tdigest_signal"]["class"] == "approximate"
        assert fields["composition"]["class"] == "packed"


class TestSpillGateAdmissions:
    """The spill fold already carries both laws the D24 gate declines to state."""

    def test_where_builder_is_spill_admitted(self):
        from zagg.processing.streaming import (
            _TDIGEST_SPILL_FUNCTIONS,
            _TDIGEST_WHERE_FUNCTION,
        )

        assert _TDIGEST_WHERE_FUNCTION == "zagg.stats.tdigest.build_tdigest_where"
        assert _TDIGEST_WHERE_FUNCTION in _TDIGEST_SPILL_FUNCTIONS

    def test_composition_is_spill_admitted(self):
        # The composition spill law is ``merge_composition_kway`` over packed
        # ``(word, n_signal)`` pairs (issues #321/#370, option (a)); the gate
        # admits the reducer BY NAME in its scalar branch. Asserted as a
        # pass/raise pair on one config so the option (b) flip — the one-line
        # removal of the name from that branch (``_COMPOSITION_FUNCTION``
        # docstring) — fails this test instead of leaving it green.
        from zagg.processing.streaming import _COMPOSITION_FUNCTION, validate_spill_fold

        assert _COMPOSITION_FUNCTION == "zagg.stats.composition.pack_composition"
        config = default_config("atl03_tdigest_strata_healpix")
        fields = get_agg_fields(config)
        assert fields["composition"]["function"] == _COMPOSITION_FUNCTION
        validate_spill_fold(config)
        # Negative control: the same scalar field under any other function is
        # refused by name, so the pass above is the admission itself and not a
        # branch that waves every scalar through.
        fields["composition"]["function"] = "zagg.stats.composition.pack_composition_v2"
        with pytest.raises(ValueError, match="scalar function .* has no cross-block fold"):
            validate_spill_fold(config)

    def test_strata_template_passes_the_spill_gate(self):
        # The CA config shape survives a block close: both strata AND the
        # composition word have cross-block fold laws (issue #370) — this is
        # the admission the D24 gate drifted from.
        from zagg.processing.streaming import validate_spill_fold

        validate_spill_fold(default_config("atl03_tdigest_strata_healpix"))


class TestD24Classification:
    """The classifier verdicts, phase by phase: phase 1 pinned ``none`` for
    all three; phase 2 admitted the strata builder (espg ruling 2026-08-24);
    phase 3 gives composition its own class."""

    def test_composition_field_classifies_packed(self):
        # Phase 3: ``pack_composition``'s fold law (``merge_composition_kway``,
        # issues #321/#370) gets its own honest arm — ``packed``, neither
        # byte-equal (``exact``) nor digest-shaped (``approximate``). Driven
        # off the SHIPPED field rather than a hand-built meta, so it follows
        # the template; what it adds over ``test_strata_template_classes`` is
        # the reducer NAME the arm turns on, and the ``of`` linkage the class
        # is conditional on: a word with no recorded divisor has no fold.
        meta = get_agg_fields(default_config("atl03_tdigest_strata_healpix"))["composition"]
        assert meta["function"] == "zagg.stats.composition.pack_composition"
        assert field_composability(meta) == "packed"
        # No ``of`` ⇒ no divisor ⇒ ``none`` (spec §3.3/§3.4).
        stripped = {**meta, "attrs": {"composition": {"threshold": 2}}}
        assert field_composability(stripped) == "none"

    def test_strata_template_classes(self):
        classes = composability_classes(default_config("atl03_tdigest_strata_healpix"))
        assert classes["count"] == "exact"
        # Phase 2: the strata builder is in the digest family (issue #515).
        assert classes["h_tdigest_signal"] == "approximate"
        assert classes["h_tdigest_noise"] == "approximate"
        # Phase 3: composition's own honest arm.
        assert classes["composition"] == "packed"

    def test_declared_fields_no_longer_reproduce_the_live_ca_declaration(self):
        # The before/after known-answer: phase 1 asserted ``fields ==
        # CA_FIELDS_BEFORE`` (today's classifier reproduced the live store's
        # declaration exactly); the admissions change ONLY the ruled entries —
        # count is untouched, the strata declare the digest fold (phase 2),
        # composition the packed one (phase 3).
        fields, excluded = declared_fields(default_config("atl03_tdigest_strata_healpix"))
        assert fields != CA_FIELDS_BEFORE
        assert fields["count"] == CA_FIELDS_BEFORE["count"]
        assert fields["composition"] == {
            "class": "packed",
            "method": "composition_kway",
            "dtype": "uint64",
            "fill_value": 0,
            # The §3.3 linkage the fold's ``n`` inputs come from, plus the
            # committed cut for the overview array's attrs block.
            "of": "h_tdigest_signal",
            "threshold": 2,
        }
        assert excluded == []
        for name in ("h_tdigest_signal", "h_tdigest_noise"):
            assert fields[name] == {
                "class": "approximate",
                "method": "tdigest_kway",
                "dtype": "float32",
                "inner_shape": [2],
                # The template's leaf budget and its split pyramid-fold budget
                # (issue #424), recorded RESOLVED.
                "delta": 8192,
                "overview_delta": 512,
                # Located strata IS the default (espg ruling on PR #334); the
                # manifest entry is the only description the overview writer
                # has, so the channel must ride it (issue #410).
                "location": "leaf_id",
            }

    def test_class_map_gates_the_leaf_column_write_path(self):
        # The class map is not just manifest text: ``column.composable_fields``
        # filters this same declaration to the composable classes, and
        # ``leaf_column_plan``/``fold_column`` carry only what survives. That
        # is why the live CA store has no strata leaf COLUMNS to fold (class
        # ``none`` at build time) and the retrofit must re-read leaves — and
        # it is the half phase 2 also flips: the admission turns worker-side
        # strata column writes ON for new runs.
        from zagg.column import composable_fields

        assert composable_fields(CA_FIELDS_BEFORE) == {"count": CA_FIELDS_BEFORE["count"]}
        fields, _ = declared_fields(default_config("atl03_tdigest_strata_healpix"))
        assert sorted(composable_fields(fields)) == [
            "composition",
            "count",
            "h_tdigest_noise",
            "h_tdigest_signal",
        ]

    def test_a_packed_entry_without_its_of_linkage_is_not_composable(self):
        # A packed entry whose §3.3 linkage is missing declares a fold with no
        # divisor. ``declared_fields`` never writes one, but a manifest can
        # (they outlive their writer, spec §4.5) — and admitting it publishes
        # an all-fill composition array under an invalid declaration, where
        # fill 0 reads as "empty signal stratum" rather than absence. It
        # degrades to native resolution exactly as an unknown class does, on
        # BOTH fold paths (the column write path and the sweep's own filter).
        from zagg.column import _is_composable, composable_fields

        fields, _ = declared_fields(default_config("atl03_tdigest_strata_healpix"))
        broken = {
            **fields,
            "composition": {k: v for k, v in fields["composition"].items() if k != "of"},
        }
        assert broken["composition"]["class"] == "packed"
        assert "composition" not in composable_fields(broken)
        assert _is_composable(broken["composition"]) is False
        assert _is_composable(fields["composition"]) is True

    def test_the_stage_pass_drops_a_packed_entry_without_its_of_linkage(self, tmp_path):
        # The THIRD admission site: ``sweep_stage_pass`` builds its own fields
        # map and hands it to ``stage_node`` unfiltered, so an of-less packed
        # entry would reach ``_merge_slabs`` and read ``group[None]`` (a
        # ``TypeError`` ``_ColumnReader.read`` does not catch, aborting the
        # pass). Behavioral: with no composable field left the pass returns at
        # its own gate, BEFORE the candidate discovery that stamps
        # ``root_moc_stale`` — so that key's presence is the "got past the
        # gate" witness (review finding).
        from zagg.pyramid import PYRAMID_SPEC_V2
        from zagg.sweep_stages import sweep_stage_pass

        fields, _ = declared_fields(default_config("atl03_tdigest_strata_healpix"))
        packed = {"composition": dict(fields["composition"])}
        broken = {"composition": {k: v for k, v in packed["composition"].items() if k != "of"}}

        def run(entries):
            return sweep_stage_pass(
                str(tmp_path),
                {
                    "shard_order": 2,
                    "cell_order": 4,
                    "pyramid": {
                        "spec": PYRAMID_SPEC_V2,
                        "overviews": [{"node": 1, "cells": [2]}, {"node": 0, "cells": [1]}],
                        "overview": {"fields": entries},
                    },
                },
                {},
                run_id="A",
            )

        assert run(broken)["stages"] == [] and "root_moc_stale" not in run(broken)
        assert run(packed)["stages"] == [] and run(packed)["root_moc_stale"] is True


class TestWhereBuilderFoldParity:
    """Phase 2: the admission's substance — a stratum payload built by
    ``build_tdigest_where`` folds by the digest family's k-way law with BOTH
    companion channels, matching the pooled build within the documented
    bounds (weights exact, quantiles close, located words to the common
    ancestor, temporal words to the toc envelope)."""

    def _blocks(self, k=4, n=400, seed=515):
        import mortie
        from conftest import toc_words

        from zagg.stats.tdigest import build_tdigest_where

        rng = np.random.default_rng(seed)
        values = rng.normal(30.0, 5.0, k * n)
        signal = rng.random(k * n) < 0.6
        lats = np.clip(37.0 + rng.uniform(-1e-4, 1e-4, k * n), -89.9, 89.9)
        lons = -119.0 + rng.uniform(-1e-4, 1e-4, k * n)
        locs = np.asarray(
            mortie.MortonIndexArray.from_latlon(lats, lons, points=True), dtype=np.uint64
        )
        times = toc_words(k * n)
        digests, words, tocs = [], [], []
        for i in range(k):
            sl = slice(i * n, (i + 1) * n)
            d, w, t = build_tdigest_where(
                values[sl],
                delta=64,
                where=signal[sl],
                locations=locs[sl],
                temporal=times[sl],
            )
            digests.append(d)
            words.append(w)
            tocs.append(t)
        return values, signal, locs, times, digests, words, tocs

    def test_payload_and_companions_through_the_fold(self):
        import mortie
        from mortie import common_ancestor

        from zagg.stats.tdigest import build_tdigest_where, quantile_from_tdigest
        from zagg.sweep_overview import decode_digest, encode_digest, fold_digests

        values, signal, locs, times, digests, words, tocs = self._blocks()
        # Through the SWEEP's fold seam, exactly as an overview cell folds its
        # contributors (payload and channels in one call, spec §9.1/§8.3).
        payload, loc_bytes, toc_bytes = fold_digests(
            digests,
            delta=64,
            dtype="float32",
            channels={"locations": list(words), "temporal": list(tocs)},
        )
        folded = decode_digest(payload, "float32", (2,))
        out_locs = decode_digest(loc_bytes, "uint64", ())
        out_tocs = decode_digest(toc_bytes, "uint64", ())
        assert len(out_locs) == len(out_tocs) == len(folded)
        # Weights are exact stratum counts, additive through the fold.
        assert int(folded[:, 1].sum()) == int(signal.sum())
        # Quantiles within the k-way law's documented closeness vs pooled.
        pooled, _, _ = build_tdigest_where(
            values, delta=64, where=signal, locations=locs, temporal=times
        )
        for q in (0.1, 0.5, 0.9):
            assert abs(quantile_from_tdigest(folded, q) - quantile_from_tdigest(pooled, q)) < 1.0
        # Located companion: every merged word contains the whole stratum's
        # common ancestor's refinement — cheap strong form: each output word
        # is an ancestor-or-equal of at least the stratum hull, so the join
        # of output words equals the join of input words (spec §9.1).
        assert int(common_ancestor(out_locs)) == int(common_ancestor(locs[signal]))
        # Temporal companion: the outputs' envelope is the inputs' envelope
        # (spec §8.3) — selected rows only, the WHERE mask masks the channel.
        starts, ends = (np.asarray(b, dtype="int64") for b in mortie.toc2time(out_tocs))
        m_starts, m_ends = (np.asarray(b, dtype="int64") for b in mortie.toc2time(times[signal]))
        assert int(starts.min()) <= int(m_starts.min())
        assert int(ends.max()) >= int(m_ends.max())
        # Round-trip stability: encode/decode is the identity on the payload.
        assert np.array_equal(
            decode_digest(encode_digest(folded, "float32"), "float32", (2,)), folded
        )

    def test_fold_is_order_independent(self):
        # The k-way law's defining property, on where-built payloads: a
        # permutation of the contributors returns identical bytes.
        from zagg.sweep_overview import fold_digests

        _, _, _, _, digests, words, tocs = self._blocks(k=5)
        a = fold_digests(
            digests,
            delta=64,
            dtype="float32",
            channels={"locations": list(words), "temporal": list(tocs)},
        )
        perm = [3, 0, 4, 2, 1]
        b = fold_digests(
            [digests[i] for i in perm],
            delta=64,
            dtype="float32",
            channels={"locations": [words[i] for i in perm], "temporal": [tocs[i] for i in perm]},
        )
        assert a == b


def _strata_cells(k=4, n=120, seed=340):
    """``k`` synthetic leaf cells: heights + conf columns, strata + word each.

    Returns ``(per_cell, truth)`` where ``per_cell`` carries each cell's
    encoded signal/noise payloads, packed word and stratum sizes, and
    ``truth`` the pooled rows for direct whole-group aggregation.
    """
    from zagg.stats.composition import pack_composition_n
    from zagg.stats.tdigest import build_tdigest_where
    from zagg.sweep_overview import encode_digest

    rng = np.random.default_rng(seed)
    per_cell, all_values, all_conf = [], [], []
    for _ in range(k):
        values = rng.normal(30.0, 5.0, n)
        conf = np.full((n, 5), -1, dtype=np.int64)
        n_sig = int(rng.integers(1, n - 1))
        for i in range(n_sig):
            conf[i, rng.integers(0, 5)] = rng.integers(2, 5)
        signal = (conf >= 2).any(axis=1)
        kwargs = dict(
            conf_land=conf[:, 0],
            conf_ocean=conf[:, 1],
            conf_sea_ice=conf[:, 2],
            conf_land_ice=conf[:, 3],
            conf_inland_water=conf[:, 4],
        )
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
        all_values.append(values)
        all_conf.append(conf)
    return per_cell, (np.concatenate(all_values), np.vstack(all_conf))


#: Manifest-shaped field entries for the synthetic strata group above — what
#: ``declared_fields`` writes, reduced to the keys the folds read.
_STRATA_FIELDS = {
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
    },
}


class TestCompositionFoldParity:
    """Phase 3: the packed class's substance. A folded cell's composition
    equals the k-way merge of its contributors' ``(word, n)`` pairs with
    ``n`` sourced from the ``of`` digest — and the signal fraction derived
    from the folded strata matches leaf truth exactly (digest weights fold
    exactly, spec §2)."""

    def _fold(self, per_cell, fields=_STRATA_FIELDS, cell_order=5, resolution=4):
        from zagg.column import fold_column

        k = len(per_cell)
        slabs = {
            "h_sig": np.array([c["sig"] for c in per_cell], dtype=object),
            "h_noise": np.array([c["noise"] for c in per_cell], dtype=object),
            "composition": np.array([c["word"] for c in per_cell], dtype=np.uint64),
        }
        assert 4 ** (cell_order - resolution) == k
        return fold_column(slabs, fields, cell_order=cell_order, resolutions=[resolution])[
            resolution
        ]

    def test_folded_composition_is_the_kway_merge_of_contributors(self):
        from zagg.stats.composition import (
            counts_from_composition,
            merge_composition_kway,
            unpack_composition,
        )
        from zagg.sweep_overview import decode_digest

        per_cell, (values, conf) = _strata_cells()
        group = self._fold(per_cell)
        # The law, verbatim: the fold's output word IS merge_composition_kway
        # over the contributors' (word, n-from-the-of-digest) pairs.
        expected = merge_composition_kway([(c["word"], c["n_signal"]) for c in per_cell])
        assert int(group["composition"][0]) == expected
        # N_signal at the folded level is the folded of-digest's total weight
        # — exact, so a reader pairs word and divisor at every level the same
        # way it does at the leaves (spec §3.3).
        folded_sig = decode_digest(group["h_sig"][0], "float32", (2,))
        n_total = sum(c["n_signal"] for c in per_cell)
        assert int(folded_sig[:, 1].sum()) == n_total
        # Presence exact through the fold; counts within one lane
        # quantization of the direct whole-group pack (spec §3.4).
        from zagg.stats.composition import pack_composition_n

        kwargs = dict(
            conf_land=conf[:, 0],
            conf_ocean=conf[:, 1],
            conf_sea_ice=conf[:, 2],
            conf_land_ice=conf[:, 3],
            conf_inland_water=conf[:, 4],
        )
        pooled, pooled_n = pack_composition_n(values, **kwargs, threshold=2)
        assert pooled_n == n_total
        merged_word = int(group["composition"][0])
        assert np.array_equal(unpack_composition(merged_word) > 0, unpack_composition(pooled) > 0)
        assert (
            np.max(
                np.abs(
                    counts_from_composition(merged_word, n_total)
                    - counts_from_composition(pooled, n_total)
                )
            )
            <= 1
        )

    def test_signal_fraction_matches_leaf_truth(self):
        from zagg.sweep_overview import decode_digest

        per_cell, (values, conf) = _strata_cells(k=4, n=200, seed=341)
        group = self._fold(per_cell)
        n_sig = int(decode_digest(group["h_sig"][0], "float32", (2,))[:, 1].sum())
        n_noise = int(decode_digest(group["h_noise"][0], "float32", (2,))[:, 1].sum())
        true_signal = int(((conf >= 2).any(axis=1)).sum())
        # Stratum weights are exact counts and fold exactly, so the derived
        # browse quantity — the signal fraction — is leaf truth at every
        # level, not an estimate.
        assert n_sig == true_signal
        assert n_sig + n_noise == len(values)

    def test_empty_divisor_cells_contribute_nothing(self):
        # A cell whose signal stratum is empty is the (0, 0) identity: its
        # word (0) must not drag lanes, and an all-empty group folds to 0.
        per_cell, _ = _strata_cells(k=4)

        empty = {
            "sig": b"",
            "noise": per_cell[0]["noise"],
            "word": 0,
            "n_signal": 0,
            "n_noise": per_cell[0]["n_noise"],
        }
        from zagg.stats.composition import merge_composition_kway

        group = self._fold([per_cell[0], empty, per_cell[2], empty])
        expected = merge_composition_kway(
            [
                (per_cell[0]["word"], per_cell[0]["n_signal"]),
                (per_cell[2]["word"], per_cell[2]["n_signal"]),
            ]
        )
        assert int(group["composition"][0]) == expected
        all_empty = [dict(empty) for _ in range(4)]
        group = self._fold(all_empty)
        assert int(group["composition"][0]) == 0

    def test_fold_refuses_a_word_without_its_divisor(self):
        # The pair may never fold apart (spec §3.3): a fields map that admits
        # the word but not its of digest is refused by name.
        from zagg.column import fold_column

        per_cell, _ = _strata_cells(k=4)
        slabs = {"composition": np.array([c["word"] for c in per_cell], dtype=np.uint64)}
        fields = {"composition": dict(_STRATA_FIELDS["composition"])}
        with pytest.raises(ValueError, match="packed composition fold"):
            fold_column(slabs, fields, cell_order=5, resolutions=[4])

    def test_declaration_demotes_packed_without_an_approximate_divisor(self):
        # ``declared_fields`` writes ``packed`` only when the ``of`` digest is
        # itself declared ``approximate`` — the law divides by that digest's
        # weight at every level, so a composition whose divisor is excluded
        # from the pyramid is the recorded absence, not a promised fold.
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_vec": {
                        "kind": "ragged",
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "inner_shape": [3],  # non-(2,) — classifies none
                        "dtype": "float32",
                    },
                    "composition": {
                        "function": "zagg.stats.composition.pack_composition",
                        "dtype": "uint64",
                        "fill_value": 0,
                        "params": {"threshold": 2},
                        "attrs": {"composition": {"of": "h_vec", "threshold": 2}},
                    },
                }
            }
        )
        fields, excluded = declared_fields(cfg)
        assert fields["composition"] == {"class": "none"}
        assert sorted(excluded) == ["composition", "h_vec"]

    def test_declaration_demotes_packed_over_a_flux_weighted_divisor(self, caplog):
        # Same demotion for a divisor that IS approximate but declares §2.0
        # ``weights: flux``: the law divides by that digest's summed weights
        # and ``N_signal`` is a photon count, so a flux sum is the wrong n —
        # the packed fold arms never reach ``check_weights_match``, so the
        # declaration is the only place this pairing can be refused.
        from zagg.config import PipelineConfig

        def cfg(**digest):
            return PipelineConfig(
                aggregation={
                    "variables": {
                        "h_sig": {
                            "kind": "ragged",
                            "function": "zagg.stats.tdigest.build_tdigest",
                            "inner_shape": [2],
                            "dtype": "float32",
                            **digest,
                        },
                        "composition": {
                            "function": "zagg.stats.composition.pack_composition",
                            "dtype": "uint64",
                            "fill_value": 0,
                            "params": {"threshold": 2},
                            "attrs": {"composition": {"of": "h_sig", "threshold": 2}},
                        },
                    }
                }
            )

        # The counts control: the same pairing folds.
        fields, excluded = declared_fields(cfg())
        assert fields["composition"]["class"] == "packed"
        assert fields["composition"]["of"] == "h_sig"
        assert excluded == []
        # And the flux divisor demotes, while the digest itself still folds.
        flux = cfg(weights="flux", attrs={"gain": {"name": "g", "version": "1"}})
        with caplog.at_level(logging.WARNING, logger="zagg.pyramid"):
            fields, excluded = declared_fields(flux)
        # The demotion states its own cause: ``warn_excluded``'s generic line
        # would send an operator to the composition reducer, not to ``of``.
        [line] = [r.message for r in caplog.records if "composition" in r.message]
        assert "attrs.composition.of 'h_sig'" in line
        assert "weights='flux'" in line
        assert fields["h_sig"]["class"] == "approximate"
        assert fields["h_sig"]["weights"] == "flux"
        assert fields["composition"] == {"class": "none"}
        assert excluded == ["composition"]


def _strata_leaf_cfg():
    """The synthetic strata leaf config: the CA shape at test scale."""
    from zagg.config import PipelineConfig

    # The SAME cut the word packs: ``_strata_cells`` (and
    # ``pack_composition_n``) call a row signal when ANY of the five §3.1
    # surfaces clears ``threshold: 2``, so the divisor digest's predicate must
    # be that five-column union — a land-only ``where`` would give
    # ``weight(h_sig) < N_signal`` at every cell and break §3.3's recovery.
    # Nothing here executes it (``_build_store`` writes the slabs by hand, so
    # the reducers never run) — but the fixture is what gets copied into the
    # next product template, and ``zagg.config`` deliberately does not validate
    # this coupling (``src/zagg/config.py``, the third coupling).
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


def _write_strata_leaf(root, dec, per_cell, *, shard_order, cell_order):
    """Write one committed strata leaf at ``dec``; return its cell-order group path.

    The whole leaf-writing half of both fixtures below (the half-pair fold and
    the end-to-end pyramid): template, morton, ``count``, the two ragged strata
    slabs, the packed word, commit stamp. Shared so a geometry fix — the
    ``where`` cut on ``_strata_leaf_cfg``, say — cannot land on one copy and
    miss the other; the e2e fixture keeps only its manifest half.
    """
    import zarr
    from mortie import generate_morton_children

    from zagg.grids.healpix import HealpixGrid
    from zagg.grids.morton import morton_word
    from zagg.hive import shard_leaf_path, stamp_commit
    from zagg.store import open_store

    grid = HealpixGrid(shard_order, cell_order, config=_strata_leaf_cfg())
    word = morton_word(dec)
    store = open_store(shard_leaf_path(str(root), word))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(cell_order), mode="r+", zarr_format=3)
    n = 4 ** (cell_order - shard_order)
    assert len(per_cell) == n
    group["morton"][:] = np.asarray(generate_morton_children(word, cell_order), dtype=np.uint64)
    group["count"][:] = np.array([c["n_signal"] + c["n_noise"] for c in per_cell], dtype=np.int32)
    for field, key in (("h_sig", "sig"), ("h_noise", "noise")):
        slab = np.full(n, b"", dtype=object)
        for i, c in enumerate(per_cell):
            slab[i] = c[key]
        group[field][:] = slab
    group["composition"][:] = np.array([c["word"] for c in per_cell], dtype=np.uint64)
    stamp_commit(store, cells_with_data=n, granule_count=1)
    return f"{shard_leaf_path(str(root), word)}/{cell_order}"


class TestLeafFoldHalfPair:
    """``_fold_node``'s packed arm when ONE leaf of an output cell is half-paired.

    The leaf fold's counterpart to ``_merge_slabs``' skew: at a geometry where
    several leaves land in ONE output cell (``target_order < shard_order``), a
    leaf carrying the divisor digest but not the word would leave the cell's
    ``N_signal`` counting rows the published word never covered. The whole cell
    stays at the fill word instead, and the digest is untouched.
    """

    SHARD_ORDER, CELL_ORDER, K = 3, 4, 1

    def _leaf(self, root, dec, per_cell):
        return _write_strata_leaf(
            root, dec, per_cell, shard_order=self.SHARD_ORDER, cell_order=self.CELL_ORDER
        )

    def _fold(self, root):
        from zagg.sweep_overview import _fold_node

        fields = {
            "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
            **_STRATA_FIELDS,
        }
        counts = {"failed": 0}
        result = _fold_node(
            str(root),
            "-31",
            self.K,
            [None],
            ["-3111", "-3112"],
            fields,
            self.CELL_ORDER,
            self.SHARD_ORDER,
            counts,
            {},
        )
        return result["slabs"], counts

    @staticmethod
    def _weight(slab, j):
        from zagg.sweep_overview import decode_digest

        payload = slab[j]
        if payload is None or not len(payload):
            return 0
        return int(decode_digest(bytes(payload), "float32", (2,))[:, 1].sum())

    def test_a_half_paired_leaf_poisons_its_output_cells(self, tmp_path):
        import shutil

        from zagg.stats.composition import merge_composition_kway

        a, _ = _strata_cells(k=4, n=40, seed=620)
        b, _ = _strata_cells(k=4, n=40, seed=621)
        self._leaf(tmp_path, "-3111", a)
        path_b = self._leaf(tmp_path, "-3112", b)

        # Both leaves land in output cell 0 (target order 2 < shard order 3),
        # so the cell has two contributors — the geometry where a per-field
        # skip can mix.
        slabs, counts = self._fold(tmp_path)
        assert counts["failed"] == 0
        expected = merge_composition_kway(
            [(c["word"], c["n_signal"]) for c in (*a, *b) if c["n_signal"] > 0]
        )
        assert int(slabs["composition"][0]) == expected
        pooled = self._weight(slabs["h_sig"], 0)
        assert pooled == sum(c["n_signal"] for c in (*a, *b))

        # Drop ONE leaf's word: its divisor digest still folds into the same
        # cell, so publishing the sibling's word alone would be a fraction over
        # a denominator it never covered.
        shutil.rmtree(f"{path_b}/composition")
        slabs, counts = self._fold(tmp_path)
        assert counts["failed"] == 0
        assert int(slabs["composition"][0]) == 0, "the covered cell keeps the fill word"
        assert self._weight(slabs["h_sig"], 0) == pooled, "the divisor still folds both"

    def test_a_leaf_missing_the_divisor_drops_without_poisoning(self, tmp_path):
        import shutil

        from zagg.stats.composition import merge_composition_kway

        a, _ = _strata_cells(k=4, n=40, seed=620)
        b, _ = _strata_cells(k=4, n=40, seed=621)
        self._leaf(tmp_path, "-3111", a)
        path_b = self._leaf(tmp_path, "-3112", b)

        # The REVERSE direction: leaf -3112 keeps its word but loses the ``of``
        # digest. The digest arm reads that same array for ``h_sig`` itself and
        # hits the same absence, so BOTH halves drop — the cell's N_signal is
        # -3111's alone and -3111's word describes exactly those rows. Nothing
        # to skew, so poisoning here would only destroy a correct word.
        shutil.rmtree(f"{path_b}/h_sig")
        slabs, counts = self._fold(tmp_path)
        assert counts["failed"] == 0
        expected = merge_composition_kway(
            [(c["word"], c["n_signal"]) for c in a if c["n_signal"] > 0]
        )
        assert int(slabs["composition"][0]) == expected, "the surviving leaf's word stands"
        assert self._weight(slabs["h_sig"], 0) == sum(c["n_signal"] for c in a)


class TestStageMergeHalfPair:
    """``_merge_slabs``' packed arm when a contributor carries half the pair.

    The stage site's own behavior, which no other fold site has: the word and
    its ``of`` divisor are DIFFERENT declared fields folded by different loops,
    so skipping a half-paired contributor for the word alone would publish a
    word whose lanes are over a strict subset of the level's ``N_signal``
    (review finding: a ~29% skew). The covered output cells stay at the fill
    word instead — absence over wrongness — and the contributor counts
    unreadable.
    """

    NODE_ORDER, CELL_ORDER, RES = 3, 5, 4

    def _write_column(self, root, dec, per_cell):
        from zagg.column import column_name, fold_column, write_column
        from zagg.grids.morton import morton_word
        from zagg.sweep import _node_rel

        slabs = {
            "h_sig": np.array([c["sig"] for c in per_cell], dtype=object),
            "h_noise": np.array([c["noise"] for c in per_cell], dtype=object),
            "composition": np.array([c["word"] for c in per_cell], dtype=np.uint64),
        }
        folded = fold_column(
            slabs,
            _STRATA_FIELDS,
            cell_order=self.CELL_ORDER,
            resolutions=[self.RES, self.NODE_ORDER],
        )
        write_column(
            str(root),
            morton_word(dec),
            folded,
            _STRATA_FIELDS,
            node_order=self.NODE_ORDER,
            cell_order=self.CELL_ORDER,
            granule_count=1,
        )
        return f"{root}/{_node_rel(dec)}/{column_name(None)}"

    def _reader(self, path):
        from zagg.hive import _utcnow
        from zagg.sweep_stage import _ColumnReader

        return _ColumnReader(path, run_id="A", run_started=_utcnow(), store_kwargs={})

    def _merge(self, paths):
        from zagg.sweep_stage import _merge_slabs

        rows = [[self._reader(p)] for p in paths]
        # One output cell over BOTH children: factor 8 == the two children's
        # four source cells each, which is where a half-pair can mix.
        return _merge_slabs(
            rows,
            _STRATA_FIELDS,
            res_src=self.RES,
            src_per_child=4,
            factor=8,
            n_out=1,
        )

    @staticmethod
    def _weight(slab, j):
        from zagg.sweep_overview import decode_digest

        payload = slab[j]
        if payload is None or not len(payload):
            return 0
        return int(decode_digest(bytes(payload), "float32", (2,))[:, 1].sum())

    def test_a_half_paired_contributor_poisons_its_output_cells(self, tmp_path):
        import shutil

        import zarr

        from zagg.stats.composition import merge_composition_kway
        from zagg.store import open_store
        from zagg.sweep_overview import decode_digest

        a, _ = _strata_cells(k=16, n=40, seed=610)
        b, _ = _strata_cells(k=16, n=40, seed=611)
        paths = [self._write_column(tmp_path, "1111", a), self._write_column(tmp_path, "1112", b)]

        # The control: both pairs whole, the word is the k-way merge of every
        # source cell's (word, n) and the cell counts as folded.
        slabs, folded, missing, unreadable, demotions = self._merge(paths)
        assert demotions == [], "a clean fold records no demotion (issue #518)"
        expected = []
        for path in paths:
            group = zarr.open_group(
                open_store(path, read_only=True), path=str(self.RES), mode="r", zarr_format=3
            )
            words, sig = group["composition"][:], group["h_sig"][:]
            expected += [
                (int(words[i]), n)
                for i in range(len(words))
                if (n := int(decode_digest(bytes(sig[i] or b""), "float32", (2,))[:, 1].sum())) > 0
            ]
        assert int(slabs["composition"][0]) == merge_composition_kway(expected)
        assert (folded, missing, unreadable) == (2, 0, 0)
        pooled = self._weight(slabs["h_sig"], 0)
        assert pooled == sum(n for _w, n in expected)

        # Now child 1112 loses its word but keeps the divisor digest (schema
        # evolution on ONE half). The digest loop folds it either way, so the
        # level's N_signal is unchanged — which is exactly why the word may
        # not be published over the surviving subset.
        shutil.rmtree(f"{paths[1]}/{self.RES}/composition")
        slabs, folded, missing, unreadable, demotions = self._merge(paths)
        assert (folded, missing, unreadable) == (1, 0, 1)
        assert int(slabs["composition"][0]) == 0, "the covered cell keeps the fill word"
        assert self._weight(slabs["h_sig"], 0) == pooled, "the divisor still folds both"
        # The rail's firing is returned for the artifact record (issue #518).
        assert demotions == [
            {
                "field": "composition",
                "class": "packed",
                "reason": "word-missing",
                "contributors": 1,
                "of": "h_sig",
                "cells": 1,
            }
        ]

    def test_a_contributor_missing_the_divisor_drops_without_poisoning(self, tmp_path):
        import shutil

        import zarr

        from zagg.stats.composition import merge_composition_kway
        from zagg.store import open_store
        from zagg.sweep_overview import decode_digest

        a, _ = _strata_cells(k=16, n=40, seed=610)
        b, _ = _strata_cells(k=16, n=40, seed=611)
        paths = [self._write_column(tmp_path, "1111", a), self._write_column(tmp_path, "1112", b)]

        # The REVERSE direction: child 1112 keeps its word but loses the ``of``
        # digest. The digest loop above reads that same array for ``h_sig``
        # itself and drops the child too, so the level's N_signal is 1111's
        # alone and 1111's word describes exactly those rows — consistent, so
        # nothing is poisoned and the shared cell keeps the surviving word.
        # Still counted unreadable: the level did fold short.
        shutil.rmtree(f"{paths[1]}/{self.RES}/h_sig")
        slabs, folded, missing, unreadable, demotions = self._merge(paths)
        assert (folded, missing, unreadable) == (1, 0, 1)
        # Recorded without ``cells``: nothing was blanked, the contributor
        # simply folded short for the field (issue #518).
        assert demotions == [
            {
                "field": "composition",
                "class": "packed",
                "reason": "divisor-missing",
                "contributors": 1,
                "of": "h_sig",
            }
        ]
        group = zarr.open_group(
            open_store(paths[0], read_only=True), path=str(self.RES), mode="r", zarr_format=3
        )
        words, sig = group["composition"][:], group["h_sig"][:]
        survivors = [
            (int(words[i]), n)
            for i in range(len(words))
            if (n := int(decode_digest(bytes(sig[i] or b""), "float32", (2,))[:, 1].sum())) > 0
        ]
        assert int(slabs["composition"][0]) == merge_composition_kway(survivors)
        assert self._weight(slabs["h_sig"], 0) == sum(n for _w, n in survivors)


class TestEndToEndStrataPyramid:
    """Phase 4: template → build → overview fold on a synthetic strata store.

    The ``/1`` cascade regime end to end (finest level exact-from-leaves via
    ``_fold_node``, the coarser level a cascade via ``_cascade_node``), with
    the shard geometry of the sweep test harness (shard order 2, cell order
    4, one committed leaf at ``-311``). At EVERY level: both strata's
    ``weight_total`` folds exactly, and the composition word is the k-way
    merge of its contributors' ``(word, n)`` pairs — recomputed independently
    here from the arrays one level finer."""

    SHARD_ORDER, CELL_ORDER = 2, 4

    def _build_store(self, root, per_cell, second=None):
        import json as _json

        import obstore

        from zagg.hive import MANIFEST_NAME
        from zagg.store import open_object_store

        for dec, cells in (("-311", per_cell), ("-312", second)):
            if cells is not None:
                _write_strata_leaf(
                    root, dec, cells, shard_order=self.SHARD_ORDER, cell_order=self.CELL_ORDER
                )
        fields = {
            "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
            **{k: dict(v) for k, v in _STRATA_FIELDS.items()},
        }
        fields["composition"]["threshold"] = 2
        manifest = {
            "spec": "morton-hive/1",
            "dataset": {"short_name": "TEST", "version": "1"},
            "cell_order": self.CELL_ORDER,
            "shard_order": self.SHARD_ORDER,
            "split_schedule": [1] * self.SHARD_ORDER,
            "pyramid": {
                "spec": "zagg-pyramid/1",
                "overview": {
                    "spacing": 2,
                    "orders": [1, 0],
                    "all_time": False,
                    "fold_source": "cascade",
                    "exact_levels": 1,
                    "fields": fields,
                },
            },
            "generated_at": "2026-01-01T00:00:00+00:00",
        }
        obstore.put(open_object_store(str(root)), MANIFEST_NAME, _json.dumps(manifest).encode())
        return manifest

    @staticmethod
    def _group(root, node_rel, order):
        import zarr

        from zagg.store import open_store

        store = open_store(f"{root}/{node_rel}/all.zarr")
        return zarr.open_group(store, path=str(order), mode="r", zarr_format=3)

    def test_strata_and_composition_fold_at_every_level(self, tmp_path):
        from zagg.stats.composition import (
            merge_composition_kway,
            pack_composition_n,
            unpack_composition,
        )
        from zagg.sweep_overview import decode_digest, sweep_overviews

        # TWO committed leaves under node -31, at ranks 0 and 1. One leaf would
        # leave every populated level-1 cell inside a SINGLE level-0 fold
        # window, so a cascade window over-reaching by one cell would only ever
        # pick up an empty cell (dropped by the ``n > 0`` guard) and no
        # assertion here could see it (review finding). With two, level 1 is
        # populated at cells 0..7 and level 0 at cells 0 AND 1, so the window
        # boundary at ``j * factor`` separates two POPULATED spans.
        leaves = [_strata_cells(k=16, n=60, seed=515), _strata_cells(k=16, n=60, seed=518)]
        manifest = self._build_store(tmp_path, leaves[0][0], second=leaves[1][0])
        counts = sweep_overviews(str(tmp_path), manifest, {"-311": {None}, "-312": {None}})
        assert counts["failed"] == 0 and counts["written"] == 2

        def weights(group, field, j):
            payload = group[field][:][j]
            if payload is None or not len(payload):
                return 0
            return int(decode_digest(payload, "float32", (2,))[:, 1].sum())

        # Level 1 (node -31, cell order 3): exact-from-leaves. Leaf -311 is the
        # node's rank-0 child (digits are 1-based) and -312 its rank-1, so they
        # own the spans starting at 0 and 4; each output cell folds 4 leaf cells.
        g1 = self._group(tmp_path, "-3/1", 3)
        for rank, (per_cell, _truth) in enumerate(leaves):
            for j in range(4):
                rows = per_cell[4 * j : 4 * (j + 1)]
                cell = 4 * rank + j
                assert weights(g1, "h_sig", cell) == sum(c["n_signal"] for c in rows)
                assert weights(g1, "h_noise", cell) == sum(c["n_noise"] for c in rows)
                expected = merge_composition_kway(
                    [(c["word"], c["n_signal"]) for c in rows if c["n_signal"] > 0]
                )
                assert int(g1["composition"][cell]) == expected
                assert int(g1["count"][cell]) == sum(c["n_signal"] + c["n_noise"] for c in rows)
        # Cells outside both leaves' spans hold the packed fill word 0.
        assert int(g1["composition"][15]) == 0 and weights(g1, "h_sig", 15) == 0
        # The overview's composition array is self-describing exactly as a
        # leaf's: §3.3 block with the writer-stamped halves + the manifest's.
        block = dict(g1["composition"].attrs["composition"])
        assert block["spec"] == "zagg-composition/1"
        assert block["of"] == "h_sig" and block["threshold"] == 2

        # Level 0 (node -3, cell order 2): a cascade — its words must be the
        # k-way merge of the LEVEL-1 arrays' (word, n) pairs, recomputed here
        # independently, and its weights still leaf-exact. Each leaf collapses
        # whole into ONE level-0 cell, at its own rank, so a fold window that
        # crossed the boundary would mix the two.
        g0 = self._group(tmp_path, "-3", 2)
        for rank, (per_cell, (values, conf)) in enumerate(leaves):
            assert weights(g0, "h_sig", rank) == sum(c["n_signal"] for c in per_cell)
            assert weights(g0, "h_noise", rank) == sum(c["n_noise"] for c in per_cell)
            parts = [
                (int(g1["composition"][i]), weights(g1, "h_sig", i))
                for i in range(4 * rank, 4 * (rank + 1))
                if weights(g1, "h_sig", i) > 0
            ]
            assert int(g0["composition"][rank]) == merge_composition_kway(parts)
            # Presence is exact against DIRECT truth through both folds (§3.4);
            # this leaf collapses into one level-0 cell, so compare against one
            # pooled pack of its every raw row.
            pooled, pooled_n = pack_composition_n(
                values,
                conf_land=conf[:, 0],
                conf_ocean=conf[:, 1],
                conf_sea_ice=conf[:, 2],
                conf_land_ice=conf[:, 3],
                conf_inland_water=conf[:, 4],
                threshold=2,
            )
            assert pooled_n == weights(g0, "h_sig", rank)
            assert np.array_equal(
                unpack_composition(int(g0["composition"][rank])) > 0,
                unpack_composition(pooled) > 0,
            )

    def test_overview_provenance_records_the_packed_law(self, tmp_path):
        import zarr

        from zagg.store import open_store
        from zagg.sweep_overview import OVERVIEW_ATTR, sweep_overviews

        per_cell, _ = _strata_cells(k=16, n=30, seed=516)
        manifest = self._build_store(tmp_path, per_cell)
        sweep_overviews(str(tmp_path), manifest, {"-311": {None}})
        for node_rel, source in (("-3/1", "leaves"), ("-3", "cascade")):
            root = zarr.open_group(
                open_store(f"{tmp_path}/{node_rel}/all.zarr"), mode="r", zarr_format=3
            )
            block = dict(root.attrs[OVERVIEW_ATTR])
            assert block["fold_source"] == source
            entry = dict(block["fields"]["composition"])
            assert entry == {"class": "packed", "method": "composition_kway"}

    def test_packed_drift_checks_the_stored_composition_block(self, tmp_path):
        # The retrofit gate's packed arm: a leaf whose §3.3 block declares
        # another spec, another lane order, or nothing at all is a store this
        # declaration does not describe — the words are merged lane-wise BY
        # POSITION under this writer's constants, so folding through it would
        # produce a well-formed wrong word.
        import zarr

        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path
        from zagg.store import open_store
        from zagg.sweep_overview import _field_drift

        per_cell, _ = _strata_cells(k=16, n=20, seed=517)
        manifest = self._build_store(tmp_path, per_cell)
        meta = manifest["pyramid"]["overview"]["fields"]["composition"]
        store = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        group = zarr.open_group(store, path=str(self.CELL_ORDER), mode="r+", zarr_format=3)
        arr = group["composition"]
        assert _field_drift(group, "composition", meta) is None

        block = dict(arr.attrs["composition"])
        arr.attrs["composition"] = {**block, "spec": "zagg-composition/2"}
        assert "composition spec" in _field_drift(group, "composition", meta)
        arr.attrs["composition"] = {**block, "lanes": list(reversed(block["lanes"]))}
        assert "lanes" in _field_drift(group, "composition", meta)
        arr.attrs["composition"] = {**block, "of": "h_noise"}
        assert "binds of='h_noise'" in _field_drift(group, "composition", meta)
        del arr.attrs["composition"]
        assert "attrs block" in _field_drift(group, "composition", meta)

    def test_provenance_defaults_the_method_by_class(self):
        # A manifest entry with no ``method`` — never written by
        # ``declared_fields``, but manifests outlive their writer and may come
        # from an external one (spec §4.5) — must not have a packed field's
        # provenance claim the digest law over a dense composition word.
        from zagg.sweep_overview import COMPOSITION_LAW, TDIGEST_LAW, _field_provenance

        assert _field_provenance({"class": "packed"}) == {
            "class": "packed",
            "method": COMPOSITION_LAW,
        }
        assert _field_provenance({"class": "approximate"})["method"] == TDIGEST_LAW
        # An explicit method still wins over the class default.
        assert _field_provenance({"class": "packed", "method": "x"})["method"] == "x"
