"""Overview sweep family (issue #201): D24 kernels, writer, pyramid block.

Phase A covers the composability-class helpers (the D24 derivation from the
aggregator merge-law set) and the per-field up-aggregation kernels: exact
folds byte-equal by construction, approximate (t-digest) folds via the
order-independent k-way merge. Phase B covers the overview writer — real
leaf zarrs folded into ``{window}.zarr``/``all.zarr`` overviews at declared
ancestor orders, with the D11 role/provenance attrs.
"""

import json

import numpy as np
import obstore
import pytest
import zarr

from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, read_commit, read_manifest, shard_leaf_path, stamp_commit
from zagg.semantics import (
    EXACT_MERGE_LAWS,
    composability_classes,
    field_composability,
)
from zagg.store import open_object_store, open_store
from zagg.sweep import run_sweep
from zagg.sweep_overview import (
    OVERVIEW_ATTR,
    PYRAMID_SPEC,
    ROLE_ATTR,
    check_companion_match,
    combine_dense,
    declare_pyramid,
    decode_digest,
    encode_digest,
    fold_dense,
    fold_digests,
)

SHARD_ORDER = 2
CELL_ORDER = 4
LEAF_CELLS = 4 ** (CELL_ORDER - SHARD_ORDER)

#: The manifest pyramid declaration the writer consumes (template-time, D22).
FIELDS_DECL = {
    "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
    "h_min": {"class": "exact", "method": "min", "dtype": "float32", "fill_value": "NaN"},
    "h_tdigest": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 64,
    },
    "h_mean": {"class": "none"},  # option A: absence declared in the block
}


def _write_manifest(
    root,
    *,
    orders=(1, 0),
    all_time=False,
    windowed=False,
    shard_order=SHARD_ORDER,
    cell_order=CELL_ORDER,
    fields=None,
    fold_source=None,
    exact_levels=None,
):
    manifest = {
        "spec": "morton-hive/2" if windowed else "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": cell_order,
        "shard_order": shard_order,
        "split_schedule": [1] * shard_order,
        "pyramid": {
            "spec": PYRAMID_SPEC,
            "overview": {
                "spacing": 2,
                "orders": list(orders),
                "all_time": all_time,
                "fields": dict(FIELDS_DECL if fields is None else fields),
            },
        },
        "generated_at": "2026-01-01T00:00:00+00:00",
    }
    if fold_source is not None:
        manifest["pyramid"]["overview"]["fold_source"] = fold_source
    if exact_levels is not None:
        manifest["pyramid"]["overview"]["exact_levels"] = exact_levels
    if windowed:
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
    obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())
    return manifest


def _leaf_cfg(*, with_h_min=True):
    from zagg.config import PipelineConfig

    variables = {
        "count": {"function": "len", "dtype": "int32", "fill_value": 0},
        "h_mean": {"function": "mean", "dtype": "float32"},
        "h_tdigest": {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "inner_shape": [2],
            "dtype": "float32",
            "fill_value": 0,
        },
    }
    if with_h_min:
        variables["h_min"] = {"function": "min", "dtype": "float32"}
    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": variables,
        }
    )


def _make_leaf(
    root,
    decimal,
    cells,
    *,
    window=None,
    time_range=None,
    with_h_min=True,
    shard_order=SHARD_ORDER,
    cell_order=CELL_ORDER,
):
    """Write one committed leaf: ``cells`` maps leaf row -> observation values."""
    from mortie import generate_morton_children

    from zagg.grids.healpix import HealpixGrid
    from zagg.stats.tdigest import build_tdigest

    grid = HealpixGrid(shard_order, cell_order, config=_leaf_cfg(with_h_min=with_h_min))
    word = morton_word(decimal)
    store = open_store(shard_leaf_path(str(root), word, window=window))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(cell_order), mode="r+", zarr_format=3)
    n = 4 ** (cell_order - shard_order)
    group["morton"][:] = np.asarray(generate_morton_children(word, cell_order), dtype=np.uint64)
    count = np.zeros(n, np.int32)
    h_min = np.full(n, np.nan, np.float32)
    h_mean = np.full(n, np.nan, np.float32)
    digest = np.full(n, b"", dtype=object)
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        count[i] = len(obs)
        h_min[i] = obs.min()
        h_mean[i] = obs.mean()
        digest[i] = encode_digest(build_tdigest(obs, delta=64), "float32")
    group["count"][:] = count
    if with_h_min:
        group["h_min"][:] = h_min
    group["h_mean"][:] = h_mean
    group["h_tdigest"][:] = digest
    stamp_commit(
        store,
        cells_with_data=len(cells),
        granule_count=1,
        window=window,
        time_range=time_range,
    )


def _overview_group(root, node_rel, basename, order):
    store = open_store(f"{root}/{node_rel}/{basename}")
    return zarr.open_group(store, path=str(order), mode="r", zarr_format=3)


def _overview_root(root, node_rel, basename):
    return zarr.open_group(open_store(f"{root}/{node_rel}/{basename}"), mode="r", zarr_format=3)


class TestComposabilityClasses:
    def test_default_atl06_config_classes(self):
        from zagg.config import default_config

        classes = composability_classes(default_config("atl06"))
        assert classes["count"] == "exact"
        assert classes["h_min"] == "exact"
        assert classes["h_max"] == "exact"
        # average / var / quantile have no exact fold law; the expression
        # field is never composable.
        for name in ("h_mean", "h_sigma", "h_variance", "h_q25", "h_q50", "h_q75"):
            assert classes[name] == "none"

    def test_tdigest_field_is_approximate(self):
        from zagg.config import default_config

        classes = composability_classes(default_config("atl03_tdigest_healpix"))
        assert classes["h_tdigest"] == "approximate"

    def test_located_tdigest_is_approximate(self):
        # Ruling 4 on issue #410: a located field folds THROUGH the pyramid --
        # the located k-way merge is its channel's fold law (spec §9.1), and the
        # old `none` classification was the bug (a `location:` declaration used
        # to remove the digest from every overview level).
        meta = {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "inner_shape": [2],
            "location": "leaf_id",
            "dtype": "float32",
        }
        assert field_composability(meta) == "approximate"

    def test_temporal_per_centroid_is_approximate(self):
        # espg-ruled 2026-08-17, amending ruling 3: the temporal channel is
        # per-centroid at EVERY level, symmetric with located, so a temporal
        # digest field folds through the pyramid like any other.
        meta = {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "inner_shape": [2],
            "location": "leaf_id",
            "temporal": "per-centroid",
            "dtype": "float32",
        }
        assert field_composability(meta) == "approximate"

    def test_temporal_per_cell_stays_none(self):
        # The §8.2 DENSE shape (GEDI's leaf companion, ruling 2) is a different
        # object: its fold law is the grammar's join over a cell group, not its
        # own reducer, so classifying it by `function` would fold nanmax over toc
        # words and emit a word whose envelope claim is false.
        meta = {
            "function": "nanmax",
            "source": "delta_time",
            "dtype": "uint64",
            "fill_value": 0,
            "temporal": "per-cell",
        }
        assert field_composability(meta) == "none"

    def test_a_temporal_waveform_field_is_approximate(self):
        # Issue #508 (superseding the #422-era exclusion this test used to
        # pin): ``build_waveform_digest`` is a member of the shared digest
        # family — its stored payload is a §2 centroid array and the k-way law
        # is weight-agnostic (flux weights fold like counts, issue #431 §2) —
        # so a waveform field classifies ``approximate`` and folds through the
        # pyramid, per-centroid companion at every level.
        meta = {
            "kind": "ragged",
            "function": "zagg.stats.waveform.build_waveform_digest",
            "inner_shape": [2],
            "temporal": "per-centroid",
            "dtype": "float32",
        }
        assert field_composability(meta) == "approximate"
        # The inner-shape guard still applies to the family's new member.
        assert field_composability({**meta, "inner_shape": [3]}) == "none"

    def test_gedi_waveform_template_classifies_approximate(self):
        # Issue #508: the SHIPPED template's rx_flux — build_waveform_digest
        # with a per-centroid clock — classifies ``approximate`` via the
        # shared digest-family registry, which is what lets the SERC probe's
        # ``pyramid = {}`` declare a ladder instead of ``{"class": "none"}``.
        # The meta-level pin above records the mechanism; this one records
        # that the template hits it.
        from zagg.config import default_config

        classes = composability_classes(default_config("gedi01b_waveform_healpix_hive"))
        assert classes["rx_flux"] == "approximate"

    def test_digest_family_registry_members_pinned_by_value(self):
        # The issue #508 contract: ONE registry, existing tuples keep their
        # exact members. Pinned by value so a drive-by addition to any of the
        # three is a loud diff, not a silent gate widening (the mortie #194
        # lesson). ``build_tdigest_where`` joined the family by espg ruling
        # (admit, 2026-08-24, issue #515) — the one deliberate widening; the
        # streaming/spill gate tuples are unchanged by it.
        from zagg.processing.streaming import (
            _DIGEST_FAMILY_FUNCTIONS,
            _TDIGEST_FUNCTIONS,
            _TDIGEST_SPILL_FUNCTIONS,
        )

        assert _TDIGEST_FUNCTIONS == (
            "zagg.stats.tdigest.build_tdigest",
            "zagg.stats.tdigest.build_tdigest_pairwise",
        )
        assert _TDIGEST_SPILL_FUNCTIONS == (
            *_TDIGEST_FUNCTIONS,
            "zagg.stats.tdigest.build_tdigest_where",
        )
        assert _DIGEST_FAMILY_FUNCTIONS == (
            *_TDIGEST_FUNCTIONS,
            "zagg.stats.waveform.build_waveform_digest",
            "zagg.stats.tdigest.build_tdigest_where",
        )

    def test_where_strata_template_classifies_approximate(self):
        # The issue #508 gate drift, resolved by espg's ruling on issue #515
        # (admit, 2026-08-24): ``build_tdigest_where`` is in the digest-family
        # registry, so the shipped strata template's fields classify
        # ``approximate`` — the same k-way law the spill gate had admitted
        # since issue #370 (its stored payload is an ordinary (k, 2) centroid
        # array; row selection precedes the build).
        from zagg.config import default_config

        classes = composability_classes(default_config("atl03_tdigest_strata_healpix"))
        assert classes["h_tdigest_signal"] == "approximate"
        assert classes["h_tdigest_noise"] == "approximate"
        # The meta-level mechanism behind that template result.
        assert (
            field_composability(
                {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest_where",
                    "inner_shape": [2],
                    "params": {"where": "h_ph > 0"},
                    "dtype": "float32",
                }
            )
            == "approximate"
        )
        # The inner-shape guard applies to the strata builder like every
        # family member.
        assert (
            field_composability(
                {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest_where",
                    "inner_shape": [3],
                    "params": {"where": "h_ph > 0"},
                    "dtype": "float32",
                }
            )
            == "none"
        )

    def test_located_declaration_rides_the_manifest_entry(self):
        # The manifest is the only description the overview WRITER has of a
        # field (``_overview_config``), so the channel has to be recorded there
        # or the overview template emits no sibling for the fold to write.
        from zagg.config import PipelineConfig
        from zagg.pyramid import declared_fields

        cfg = PipelineConfig(
            aggregation={
                "variables": {
                    "h_tdigest": {
                        "kind": "ragged",
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "inner_shape": [2],
                        "location": "leaf_id",
                        "dtype": "float32",
                    },
                    "h_plain": {
                        "kind": "ragged",
                        "function": "zagg.stats.tdigest.build_tdigest",
                        "inner_shape": [2],
                        "dtype": "float32",
                    },
                }
            }
        )
        fields, excluded = declared_fields(cfg)
        assert excluded == []
        assert fields["h_tdigest"]["location"] == "leaf_id"
        # Keyed only when set: an unlocated field's entry is byte-identical to
        # what pre-#410 wrote.
        assert "location" not in fields["h_plain"]

    @pytest.mark.parametrize("name", ["min", "np.min", "numpy.min", "nanmax", "np.nansum"])
    def test_prefix_normalization(self, name):
        assert field_composability({"function": name, "dtype": "float32"}) == "exact"

    def test_every_exact_law_is_known(self):
        assert set(EXACT_MERGE_LAWS.values()) == {"sum", "min", "max"}

    @pytest.mark.parametrize(
        "meta",
        [
            {"function": "mean"},
            {"function": "median"},
            {"expression": "np.sum(x)"},
            {"function": "min", "kind": "vector", "trailing_shape": [3]},
            {"function": "len", "resolution": "chunk"},
            {"function": "zagg.stats.tdigest.build_tdigest", "kind": "ragged", "inner_shape": [3]},
            {"function": "somepkg.custom", "kind": "ragged", "inner_shape": [2]},
        ],
    )
    def test_non_composable_metas(self, meta):
        assert field_composability(meta) == "none"

    def test_temporal_companion_is_not_composable(self):
        # A per-cell toc companion's fold law is the word grammar's join
        # (spec §8.2), which no zagg fold implements yet — folding it through
        # its own reducer would emit a false envelope (issue #410).
        meta = {"function": "nanmax", "dtype": "uint64", "temporal": "per-cell"}
        assert field_composability(meta) == "none"
        assert field_composability({k: v for k, v in meta.items() if k != "temporal"}) == "exact"

    def test_pairwise_tdigest_builder_is_approximate(self):
        meta = {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest_pairwise",
            "inner_shape": [2],
            "dtype": "float32",
        }
        assert field_composability(meta) == "approximate"


class TestFoldDense:
    def test_sum_int_count(self):
        counts = np.array([1, 2, 0, 0, 3, 0, 4, 5], dtype=np.int32)
        out = fold_dense(counts, 4, "sum", 0)
        assert out.dtype == np.int32
        np.testing.assert_array_equal(out, [3, 12])

    def test_sum_float_nan_fill_skips_missing(self):
        vals = np.array([1.5, np.nan, 2.5, np.nan], dtype=np.float32)
        out = fold_dense(vals, 2, "sum", "NaN")
        np.testing.assert_array_equal(out, np.array([1.5, 2.5], dtype=np.float32))

    def test_all_missing_group_folds_to_fill(self):
        vals = np.array([np.nan, np.nan, 1.0, 2.0], dtype=np.float32)
        out = fold_dense(vals, 2, "min", "NaN")
        assert np.isnan(out[0]) and out[1] == 1.0

    def test_min_max_extrema(self):
        vals = np.array([3.0, np.nan, -1.0, 7.0], dtype=np.float32)
        np.testing.assert_array_equal(fold_dense(vals, 4, "min", "NaN"), [-1.0])
        np.testing.assert_array_equal(fold_dense(vals, 4, "max", "NaN"), [7.0])

    def test_exact_equals_direct(self):
        # The fold at factor 4 equals a direct order-coarser aggregation over
        # the same values (the §8.3 exactness claim, in kernel form). Exact
        # binary floats so equality is byte-for-byte.
        rng = np.random.default_rng(7)
        vals = (rng.integers(-64, 64, size=64) / 4.0).astype(np.float32)
        for law, direct in (("sum", np.sum), ("min", np.min), ("max", np.max)):
            out = fold_dense(vals, 4, law, "NaN")
            expect = np.array([direct(vals[i : i + 4]) for i in range(0, 64, 4)], np.float32)
            np.testing.assert_array_equal(out, expect)

    def test_nan_datum_is_skipped_not_propagated(self):
        # The DECLARED premise behind the §8.3 exactness claim (review finding,
        # issue #201, EXACT_NAN_POLICY): a stored NaN is the fill sentinel and a
        # NaN datum at once — same bytes — so the fold is nanmin/nansum, never
        # the NaN-propagating min/sum a direct coarse aggregation would return.
        from zagg.sweep_overview import EXACT_NAN_POLICY

        assert EXACT_NAN_POLICY == "skip"
        vals = np.array([1.0, np.nan, 3.0, 4.0], dtype=np.float32)
        np.testing.assert_array_equal(fold_dense(vals, 4, "min", "NaN"), [np.nanmin(vals)])
        np.testing.assert_array_equal(fold_dense(vals, 4, "sum", "NaN"), [np.nansum(vals)])
        assert np.isnan(np.min(vals)) and np.isnan(np.sum(vals))  # the divergence

    def test_nan_datum_skipped_even_under_a_numeric_fill(self):
        # Uniform policy: even where the fill is NOT NaN — so a NaN datum could
        # in principle be told apart — NaN still counts as missing.
        vals = np.array([2.0, np.nan], dtype=np.float32)
        np.testing.assert_array_equal(fold_dense(vals, 2, "min", -9999.0), [2.0])

    def test_fold_is_composable_two_hops(self):
        # fold(16x) == fold(4x) then fold(4x): the associativity that lets a
        # spacing-2 schedule read leaves directly at every declared order.
        vals = np.arange(32, dtype=np.float32)
        one_hop = fold_dense(vals, 16, "sum", "NaN")
        two_hop = fold_dense(fold_dense(vals, 4, "sum", "NaN"), 4, "sum", "NaN")
        np.testing.assert_array_equal(one_hop, two_hop)

    def test_int_extrema_identity(self):
        vals = np.array([5, 0, 0, 2], dtype=np.int32)  # fill 0 = missing
        np.testing.assert_array_equal(fold_dense(vals, 2, "max", 0), [5, 2])
        np.testing.assert_array_equal(fold_dense(vals, 2, "min", 0), [5, 2])

    def test_bad_factor_and_unknown_law_raise(self):
        with pytest.raises(ValueError, match="cannot fold"):
            fold_dense(np.zeros(3), 2, "sum", 0)
        with pytest.raises(ValueError, match="unknown exact fold law"):
            fold_dense(np.zeros(4), 2, "mean", 0)

    def test_combine_accumulates(self):
        a = np.array([1.0, np.nan], dtype=np.float32)
        b = np.array([2.0, np.nan], dtype=np.float32)
        out = combine_dense(a, b, "sum", "NaN")
        assert out[0] == 3.0 and np.isnan(out[1])


class TestFoldDigests:
    def _digest(self, values):
        from zagg.stats.tdigest import build_tdigest

        return build_tdigest(np.asarray(values, dtype=np.float64), delta=64)

    def test_roundtrip_encode_decode(self):
        d = self._digest([1.0, 2.0, 3.0])
        raw = encode_digest(d, "float32")
        np.testing.assert_array_equal(decode_digest(raw, "float32"), d.astype(np.float32))

    def test_empty_accumulation_is_empty_payload(self):
        assert fold_digests([], delta=64) == b""
        assert decode_digest(b"", "float32").shape == (0, 2)

    def test_single_digest_passes_through(self):
        d = self._digest([1.0, 5.0])
        assert fold_digests([d], delta=64) == encode_digest(d, "float32")

    def test_merge_matches_kway_oracle(self):
        from zagg.stats.tdigest import merge_tdigests_kway, quantile_from_tdigest

        a, b = self._digest(np.arange(100.0)), self._digest(np.arange(100.0, 200.0))
        raw = fold_digests([a, b], delta=64)
        merged = decode_digest(raw, "float32")
        oracle = merge_tdigests_kway([a, b], delta=64)
        np.testing.assert_array_equal(merged, oracle.astype(np.float32))
        # And the merged digest is the subtree's distribution (np.isclose class).
        assert np.isclose(quantile_from_tdigest(merged, 0.5), 99.5, rtol=0.05)

    def test_merge_is_permutation_stable(self):
        digests = [self._digest(np.arange(i, i + 50.0)) for i in (0, 30, 60)]
        forward = fold_digests(list(digests), delta=64)
        backward = fold_digests(list(reversed(digests)), delta=64)
        assert forward == backward  # the issue #279 order-independent law

    def test_a_short_located_sibling_is_refused_on_the_single_arm(self):
        # The single-contributor arm bypasses the k-way merge, and therefore its
        # pairing guard — a ``b""`` sibling row decodes to a length-0 vector, so
        # without the check here a populated payload would be written against an
        # empty channel (spec §1.1's row-alignment MUST).
        d = self._digest(np.arange(10.0))
        with pytest.raises(ValueError, match="row-aligned with its payload"):
            fold_digests([d], delta=64, channels={"locations": [np.array([], dtype=np.uint64)]})

    def test_a_short_located_sibling_is_refused_on_the_merge_arm(self):
        from conftest import point_words

        d = self._digest(np.arange(10.0))
        words = point_words(10, seed=7)
        with pytest.raises(ValueError, match="row-aligned with its payload"):
            fold_digests([d, d], delta=64, channels={"locations": [words[:3], words]})

    def test_the_aligned_pair_folds(self):
        from conftest import point_words

        d = self._digest(np.arange(10.0))
        payload, sib = fold_digests(
            [d], delta=64, channels={"locations": [point_words(len(d), seed=7)]}
        )
        n_rows = decode_digest(payload, "float32").shape[0]
        assert decode_digest(sib, "uint64", ()).shape == (n_rows,)

    @pytest.mark.parametrize("n_contributors", [0, 1, 3])
    def test_slot_order_is_the_table_not_the_callers_dict(self, n_contributors):
        # Every arm must return (payload, locations, temporal) in
        # COMPANION_CHANNELS order, because the k-way merge fixes ITS order by
        # that table. An arm following the caller's insertion order instead
        # would hand back a toc word in the locations slot, and since the two
        # grammars are mutually accepting nothing downstream would raise.
        from conftest import point_words, toc_words

        digests = [self._digest(np.arange(i, i + 10.0)) for i in range(n_contributors)]
        locs = [point_words(len(d), seed=7 + i) for i, d in enumerate(digests)]
        times = [toc_words(len(d)) for d in digests]
        forward = fold_digests(digests, delta=64, channels={"locations": locs, "temporal": times})
        reverse = fold_digests(digests, delta=64, channels={"temporal": times, "locations": locs})
        assert len(forward) == 3
        assert forward == reverse

    def test_an_unknown_channel_is_refused_not_dropped(self):
        # Silently dropping it would return fewer slots than the caller zips.
        d = self._digest(np.arange(10.0))
        with pytest.raises(ValueError, match="unknown companion channel"):
            fold_digests([d], delta=64, channels={"altitude": [np.zeros(len(d), np.uint64)]})


class TestOverviewFoldDelta:
    """Issue #424: the split leaf-δ / overview-δ fold budget."""

    def test_declared_budget_wins(self):
        from zagg.sweep_overview import overview_fold_delta

        assert overview_fold_delta({"delta": 8192, "overview_delta": 512}) == 512
        assert overview_fold_delta({"delta": 64, "overview_delta": 1024}) == 1024

    def test_absent_reproduces_every_historical_manifest(self):
        # Every manifest ever written carried δ ≤ 512, so the capped fallback
        # is byte-identical to the old fold-at-leaf-δ behavior for all of them.
        from zagg.sweep_overview import overview_fold_delta

        assert overview_fold_delta({"delta": 256}) == 256
        assert overview_fold_delta({"delta": 16}) == 16
        assert overview_fold_delta({}) == 512

    def test_absent_caps_a_raised_leaf_delta(self):
        # A δ=8,192 leaf must not saturate the sweep's k-way fold buffers
        # through an old-style manifest: the cap bounds it.
        from zagg.sweep_overview import OVERVIEW_DELTA_CAP, overview_fold_delta

        assert overview_fold_delta({"delta": 8192}) == OVERVIEW_DELTA_CAP == 512

    def test_declared_fields_records_the_resolved_budget(self):
        from zagg.config import PipelineConfig
        from zagg.pyramid import declared_fields

        def cfg(**extra):
            return PipelineConfig(
                data_source={
                    "variables": {"h": "/p"},
                    "coordinates": {"latitude": "/lat", "longitude": "/lon"},
                },
                aggregation={
                    "variables": {
                        "d": {
                            "kind": "ragged",
                            "function": "zagg.stats.tdigest.build_tdigest",
                            "source": "h",
                            "inner_shape": [2],
                            "params": {"delta": 8192},
                            **extra,
                        }
                    }
                },
                output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
            )

        declared = declared_fields(cfg(overview_delta=256))[0]["d"]
        assert declared["delta"] == 8192 and declared["overview_delta"] == 256
        # Undeclared resolves to the capped fallback, recorded explicitly so
        # the manifest is self-describing.
        resolved = declared_fields(cfg())[0]["d"]
        assert resolved["delta"] == 8192 and resolved["overview_delta"] == 512


class TestWeightsGate:
    """Spec §2.0 (issue #424): merges refuse across mismatched declarations."""

    def test_matching_defaults_pass(self):
        from zagg.sweep_overview import check_weights_match

        # Absent on both sides reads as counts on both sides.
        check_weights_match({}, {"class": "approximate"}, "d")
        check_weights_match(None, {}, "d")
        check_weights_match({"weights": "flux"}, {"weights": "flux"}, "d")

    def test_mismatch_refuses_both_ways(self):
        from zagg.sweep_overview import check_weights_match

        with pytest.raises(ValueError, match="matching\\s+declarations"):
            check_weights_match({"weights": "flux"}, {"class": "approximate"}, "d")
        with pytest.raises(ValueError, match="matching\\s+declarations"):
            check_weights_match({}, {"weights": "flux"}, "d")

    def test_unknown_stored_declaration_refuses(self):
        from zagg.sweep_overview import check_weights_match

        with pytest.raises(ValueError, match="unknown weights declaration"):
            check_weights_match({"weights": "photons"}, {}, "d")

    def test_declared_fields_records_flux_only(self):
        from zagg.config import PipelineConfig
        from zagg.pyramid import declared_fields

        def cfg(**extra):
            return PipelineConfig(
                data_source={
                    "variables": {"h": "/p"},
                    "coordinates": {"latitude": "/lat", "longitude": "/lon"},
                },
                aggregation={
                    "variables": {
                        "d": {
                            "kind": "ragged",
                            "function": "zagg.stats.tdigest.build_tdigest",
                            "source": "h",
                            "inner_shape": [2],
                            "params": {"delta": 64},
                            **extra,
                        }
                    }
                },
                output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
            )

        # counts (explicit or absent) records nothing — existing manifests
        # stay byte-identical; flux is recorded for the sweep's fold gate.
        assert "weights" not in declared_fields(cfg())[0]["d"]
        assert "weights" not in declared_fields(cfg(weights="counts"))[0]["d"]
        flux = cfg(weights="flux", attrs={"gain": {"name": "g", "version": "1"}})
        assert declared_fields(flux)[0]["d"]["weights"] == "flux"
        # §2.0 makes gain REQUIRED beside a flux declaration, and the manifest
        # entry is all the overview writer reconstructs a field from.
        assert declared_fields(flux)[0]["d"]["gain"] == {"name": "g", "version": "1"}

    def test_overview_template_stamps_the_flux_declaration(self, tmp_path):
        """The reconstructed overview template carries §2.0 through (#424).

        Without it a flux store's overview declares counts, and the cascade's
        per-child gate then refuses every child (review finding).
        """
        from zagg.grids.healpix import HealpixGrid
        from zagg.sweep_overview import _overview_config, check_weights_match

        gain = {"name": "rx_gain", "version": "3"}
        base = {
            "class": "approximate",
            "method": "tdigest_kway",
            "dtype": "float32",
            "inner_shape": [2],
            "delta": 64,
            "overview_delta": 64,
        }
        fields = {
            "flux_d": {**base, "weights": "flux", "gain": gain},
            "counts_d": dict(base),
        }
        grid = HealpixGrid(2, 4, config=_overview_config(fields), sharded=True)
        grid.emit_shard_template(open_store(str(tmp_path / "ov.zarr")), overwrite=True)
        group = zarr.open_group(
            open_store(str(tmp_path / "ov.zarr")), path="4", mode="r", zarr_format=3
        )
        assert group["flux_d"].attrs["weights"] == "flux"
        assert dict(group["flux_d"].attrs["gain"]) == gain
        # A counts field declares nothing — a counts store's template bytes
        # are unchanged by this path.
        assert "weights" not in dict(group["counts_d"].attrs)
        assert "gain" not in dict(group["counts_d"].attrs)
        # And the fold gate now accepts the overview as a cascade source.
        check_weights_match(dict(group["flux_d"].attrs), fields["flux_d"], "flux_d")
        check_weights_match(dict(group["counts_d"].attrs), fields["counts_d"], "counts_d")


class TestWaveformPyramidDeclaration:
    """Template-time classification for the waveform digest (issue #508).

    The SERC probe shape: ``gedi01b_waveform_healpix_hive`` + ``output.pyramid
    = {}`` + ``rx_flux.overview_delta = 512`` under the probe's uniform-δ4096
    override (the packaged template ships δ8192). Classification runs
    worker-side at template time, so this IS the deployed surface the 0.50
    fleet re-runs the probe against.
    """

    def _probe_cfg(self):
        from zagg.config import default_config

        cfg = default_config("gedi01b_waveform_healpix_hive")
        cfg.output["pyramid"] = {}
        rx = cfg.aggregation["variables"]["rx_flux"]
        # 512 is the SERC probe's literal declaration, and it coincides with
        # the OVERVIEW_DELTA_CAP fallback under δ4096 — so this value alone
        # cannot prove the declaration is read. The δ128 variant below does.
        rx["overview_delta"] = 512
        rx["params"]["delta"] = 4096  # the probe's uniform-δ override
        return cfg

    def test_probe_config_declares_the_ladder(self):
        from zagg.pyramid import declared_fields

        fields, excluded = declared_fields(self._probe_cfg())
        # The exact manifest entry the 0.49 probe SHOULD have produced instead
        # of {"class": "none"} — delta as the test config declares it (#424
        # records the split budget resolved), companion shape included.
        assert fields["rx_flux"] == {
            "class": "approximate",
            "method": "tdigest_kway",
            "dtype": "float32",
            "inner_shape": [2],
            "delta": 4096,
            "overview_delta": 512,
            "temporal": "per-centroid",
        }
        assert "rx_flux" not in excluded
        # Non-vacuous pin on the SAME probe config: the capped fallback can
        # only ever yield min(4096, 512) = 512, so a budget the cap cannot
        # produce shows the DECLARED value wins here too (the generic law is
        # test_declared_fields_records_the_resolved_budget).
        capped = self._probe_cfg()
        capped.aggregation["variables"]["rx_flux"]["overview_delta"] = 128
        assert declared_fields(capped)[0]["rx_flux"]["overview_delta"] == 128
        assert fields["count"]["class"] == "exact"
        # The per-record companions keep the ruled option-A absence: scalar
        # reducers without an exact law declare class only, at native
        # resolution only (issue #201).
        for name in (
            "shot_count",
            "shot_number",
            "noise_mean",
            "noise_stddev",
            "rx_energy",
            "elevation_bin0",
            "elevation_lastbin",
        ):
            assert fields[name] == {"class": "none"}
            assert name in excluded

    def test_manifest_block_covers_every_level(self):
        # pyramid = {} on the gedi grid (9 -> chunk 12 -> 18) takes the ruled
        # /2 default: the leaf entry plus the fixed ladder to 0, ONE fields map
        # covering every level — rx_flux is declared at each of them.
        from zagg.sweep_overview import build_pyramid_block

        block = build_pyramid_block(self._probe_cfg(), 9, 12)
        assert block["spec"] == "zagg-pyramid/2"
        assert [e["node"] for e in block["overviews"]] == list(range(9, -1, -1))
        assert block["overviews"][0]["cells"] == [12]
        # The manifest entry is what the /2 writers reconstruct the field
        # from, so the block must carry the whole declaration — not just the
        # class: the §8.3 companion shape and the resolved fold budget travel
        # in it or the overview template loses the sibling.
        rx = block["overview"]["fields"]["rx_flux"]
        assert rx["class"] == "approximate"
        assert rx["temporal"] == "per-centroid"
        assert rx["overview_delta"] == 512

    def test_overview_template_emits_the_times_sibling(self, tmp_path):
        # The companion path at overview levels (issue #410, per-centroid at
        # EVERY level): the manifest entry is the only description the
        # overview writer has, and _overview_config must turn its temporal key
        # into the ``rx_flux_times`` sibling + §8.3 declaration, exactly as a
        # leaf template does.
        from zagg.column import composable_fields
        from zagg.grids.healpix import HealpixGrid
        from zagg.sweep_overview import _overview_config, build_pyramid_block
        from zagg.time_axis import temporal_declaration

        # Sourced from the MANIFEST block, not from declared_fields directly,
        # because that is the seam production walks: the staged sweep lifts
        # pyramid.overview.fields out of the manifest and hands it on. A
        # regression that dropped the temporal key on the way into the block
        # fails here.
        fields = build_pyramid_block(self._probe_cfg(), 9, 12)["overview"]["fields"]
        # The production writers template only the composable classes — on the
        # /2 path, which is the one this store declares: run_stage_sweep
        # filters the manifest map before _write_stage_overview, and the
        # worker column filters in leaf_column_plan. The class-none companions
        # never reach the template, which is what the last assertion pins.
        grid = HealpixGrid(2, 4, config=_overview_config(composable_fields(fields)), sharded=True)
        grid.emit_shard_template(open_store(str(tmp_path / "ov.zarr")), overwrite=True)
        group = zarr.open_group(
            open_store(str(tmp_path / "ov.zarr")), path="4", mode="r", zarr_format=3
        )
        assert "rx_flux_times" in group
        assert temporal_declaration(dict(group["rx_flux_times"].attrs)) == {
            "spec": "zagg-toc/1",
            "shape": "per-centroid",
            "grammar": "mortie-toc/1",
        }
        # The payload binds the sibling by name (§1.2).
        assert dict(group["rx_flux"].attrs)["times"] == "rx_flux_times"
        # And the excluded per-record companions are absent from the template.
        assert "shot_number" not in group


class TestPyramidBlock:
    """The manifest declaration (Phase C): template time + config grammar."""

    def _cfg(self, **output):
        from zagg.config import default_config

        cfg = default_config("atl06")
        cfg.output.update(output)
        return cfg

    def test_default_declaration_from_atl06(self, caplog):
        from zagg.sweep_overview import build_pyramid_block

        block = build_pyramid_block(self._cfg(), shard_order=6)
        overview = block["overview"]
        assert block["spec"] == PYRAMID_SPEC
        assert overview["spacing"] == 2 and overview["orders"] == [4, 2, 0]
        assert overview["all_time"] is False
        assert overview["fields"]["count"] == {
            "class": "exact",
            "method": "sum",
            "nan_policy": "skip",
            "dtype": "int32",
            "fill_value": 0,
        }
        assert overview["fields"]["h_min"] == {
            "class": "exact",
            "method": "min",
            # The declared premise (issue #201 review): a stored NaN is the fill
            # sentinel and a NaN datum at once, so the fold skips both — this is
            # nanmin, not min. h_min's fill IS NaN in the shipped atl06 config.
            "nan_policy": "skip",
            "dtype": "float32",
            "fill_value": "NaN",  # JSON-safe token, not float nan
        }
        assert overview["fields"]["h_mean"] == {"class": "none"}
        # The D24 loud template-time warning names the excluded fields.
        assert "non-composable" in caplog.text and "h_mean" in caplog.text
        json.dumps(block)  # JSON-safe by construction

    def test_tdigest_field_declaration_carries_delta(self):
        from zagg.config import default_config
        from zagg.sweep_overview import build_pyramid_block

        cfg = default_config("atl03_tdigest_healpix")
        block = build_pyramid_block(cfg, shard_order=9)
        entry = block["overview"]["fields"]["h_tdigest"]
        assert entry["class"] == "approximate" and entry["method"] == "tdigest_kway"
        # The packaged split budgets (issues #414/#424): leaf δ 8,192
        # (loss-free bound), overview folds at 512 (accuracy bound).
        assert entry["inner_shape"] == [2] and entry["delta"] == 8192
        assert entry["overview_delta"] == 512

    def test_explicit_orders_and_all_time(self):
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"orders": [5, 1], "all_time": True})
        overview = build_pyramid_block(cfg, shard_order=6)["overview"]
        assert overview["orders"] == [5, 1] and overview["all_time"] is True

    def test_spacing_knob(self):
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"spacing": 3})
        assert build_pyramid_block(cfg, shard_order=9)["overview"]["orders"] == [6, 3, 0]

    def test_disabled_declares_off(self):
        from zagg.sweep_overview import build_pyramid_block

        block = build_pyramid_block(self._cfg(pyramid=False), shard_order=6)
        assert block["overview"] == {"orders": []}

    def test_build_manifest_carries_the_block(self):
        from zagg.grids.healpix import HealpixGrid
        from zagg.hive import build_manifest

        grid = HealpixGrid(6, 8, config=self._cfg())
        manifest = build_manifest(grid, dataset={"short_name": "ATL06", "version": "007"})
        assert manifest["pyramid"]["overview"]["orders"] == [4, 2, 0]
        json.dumps(manifest)

    def test_levels_knob_declares_v2(self):
        from zagg.pyramid import PYRAMID_SPEC_V2
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"overviews": [9, 7]})
        block = build_pyramid_block(cfg, shard_order=6)
        assert block["spec"] == PYRAMID_SPEC_V2
        overview = block["overview"]
        # The FULLY EXPANDED list at BLOCK level (the espg shape + collapse
        # rulings): the leaf entry carries every declared resolution, then
        # the fixed every-order ladder (d = 7 - 6 = 1) runs down to node 0.
        assert block["overviews"] == [{"node": 6, "cells": [9, 7]}] + [
            {"node": k, "cells": [k + 1]} for k in range(5, -1, -1)
        ]
        # The list REPLACES the /1 schedule keys wholesale — they must not
        # exist, and the family dict does not carry the declaration.
        assert "orders" not in overview and "spacing" not in overview
        assert "overviews" not in overview
        # The issue #376 fold declaration rides along exactly as under /1.
        assert overview["fold_source"] == "cascade" and overview["exact_levels"] == 1
        # The fields map is revision-independent (same D24 declarations).
        assert overview["fields"]["count"]["class"] == "exact"
        assert overview["fields"]["h_mean"] == {"class": "none"}
        assert overview["all_time"] is False
        json.dumps(block)  # JSON-safe by construction

    def test_levels_via_build_manifest(self):
        from zagg.grids.healpix import HealpixGrid
        from zagg.hive import build_manifest
        from zagg.pyramid import PYRAMID_SPEC_V2

        grid = HealpixGrid(6, 12, config=self._cfg(pyramid={"overviews": 7}))
        manifest = build_manifest(grid, dataset={"short_name": "ATL06", "version": "007"})
        assert manifest["pyramid"]["spec"] == PYRAMID_SPEC_V2
        # Scalar sugar expanded, ladder recorded in full, rooted at order 0.
        assert manifest["pyramid"]["overviews"][0] == {"node": 6, "cells": [7]}
        assert manifest["pyramid"]["overviews"][-1] == {"node": 0, "cells": [1]}
        assert len(manifest["pyramid"]["overviews"]) == 7
        json.dumps(manifest)

    def test_levels_carry_all_time_and_summarize(self):
        # The declaration keys the /2 block carries forward from the knob:
        # all_time (read by the windowed all.zarr fold) and the D24 opt-in
        # summarize map, dict()-copied for JSON safety exactly as under /1.
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(
            pyramid={
                "overviews": [7],
                "all_time": True,
                "summarize": {"h_mean": {"as": "h_mean_digest"}},
            }
        )
        overview = build_pyramid_block(cfg, shard_order=6)["overview"]
        assert overview["all_time"] is True
        assert overview["summarize"] == {"h_mean": {"as": "h_mean_digest"}}
        assert overview["summarize"]["h_mean"] is not cfg.output["pyramid"]["summarize"]["h_mean"]
        json.dumps(overview)

    def test_levels_validated_against_the_grid_block(self):
        """The templating path validates too — it is the Lambda worker's path.

        ``deployment/aws/lambda_handler.py`` builds the worker config with
        ``load_config_from_dict``, which never runs ``validate_config``, and
        then calls ``build_manifest``; without the check here an out-of-contract
        schedule in a dispatch payload would reach ``morton_hive.json``.
        """
        from zagg.sweep_overview import build_pyramid_block

        # Ascending resolutions.
        cfg = self._cfg(pyramid={"overviews": [7, 9]})
        with pytest.raises(ValueError, match="must strictly descend"):
            build_pyramid_block(cfg, shard_order=6)
        # A resolution at the shard's own order — writer-side, never declared.
        cfg = self._cfg(pyramid={"overviews": [6]})
        with pytest.raises(ValueError, match="not strictly between"):
            build_pyramid_block(cfg, shard_order=6)
        # A resolution at the base data's own cell order.
        cfg = self._cfg(pyramid={"overviews": [12]})
        with pytest.raises(ValueError, match="not strictly between"):
            build_pyramid_block(cfg, shard_order=6)

    def test_levels_skip_validation_without_a_grid_block(self):
        """A grid-less retrofit config templates; ``declare_pyramid`` re-validates.

        There is no child order to check against here, so the check is skipped
        rather than guessed at — the store's own manifest orders win at
        declare time (``TestDeclarePyramid``).
        """
        from zagg.pyramid import PYRAMID_SPEC_V2
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"overviews": [8]})
        cfg.output.pop("grid")
        block = build_pyramid_block(cfg, shard_order=6)
        assert block["spec"] == PYRAMID_SPEC_V2
        assert block["overviews"][0] == {"node": 6, "cells": [8]}
        assert block["overviews"][-1] == {"node": 0, "cells": [2]}

    def test_validate_rejects_bad_grammar(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="spacing must be an int >= 1"):
            validate_config(self._cfg(store_layout="hive", pyramid={"spacing": 0}))
        with pytest.raises(ValueError, match="orders must be a list of ancestor orders"):
            validate_config(self._cfg(store_layout="hive", pyramid={"orders": [6]}))
        with pytest.raises(ValueError, match="all_time must be a boolean"):
            validate_config(self._cfg(store_layout="hive", pyramid={"all_time": "yes"}))
        with pytest.raises(ValueError, match="unknown keys"):
            validate_config(self._cfg(store_layout="hive", pyramid={"depths": [1]}))
        # `overviews` is a KNOWN key since issue #382 — its grammar errors
        # are zagg.pyramid's (tests/test_pyramid.py), surfaced through here.
        with pytest.raises(ValueError, match="not strictly between"):
            validate_config(self._cfg(store_layout="hive", pyramid={"overviews": [1]}))
        with pytest.raises(ValueError, match="must be a mapping or false"):
            validate_config(self._cfg(store_layout="hive", pyramid=True))

    def test_validate_rejects_pyramid_on_flat(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="output.pyramid requires"):
            validate_config(
                self._cfg(
                    store_layout="flat", coverage_moc=False, sweep=False, pyramid={"spacing": 2}
                )
            )

    def test_validate_summarize_grammar(self):
        from zagg.config import validate_config

        ok = self._cfg(store_layout="hive", pyramid={"summarize": {"h_mean": {"as": "h_mean_d"}}})
        validate_config(ok)
        with pytest.raises(ValueError, match="unknown field"):
            validate_config(
                self._cfg(store_layout="hive", pyramid={"summarize": {"nope": {"as": "x"}}})
            )
        with pytest.raises(ValueError, match="collides with a declared field"):
            validate_config(
                self._cfg(store_layout="hive", pyramid={"summarize": {"h_mean": {"as": "count"}}})
            )
        with pytest.raises(ValueError, match="requires 'as'"):
            validate_config(self._cfg(store_layout="hive", pyramid={"summarize": {"h_mean": {}}}))

    def test_summarize_recorded_in_block(self):
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"summarize": {"h_mean": {"as": "h_mean_digest"}}})
        overview = build_pyramid_block(cfg, shard_order=6)["overview"]
        assert overview["summarize"] == {"h_mean": {"as": "h_mean_digest"}}

    def test_default_pyramid_validates_clean(self):
        from zagg.config import validate_config

        validate_config(self._cfg(store_layout="hive"))  # absent knob is legal

    def test_default_declaration_is_cascade_with_one_exact_level(self):
        from zagg.sweep_overview import build_pyramid_block

        overview = build_pyramid_block(self._cfg(), shard_order=6)["overview"]
        assert overview["fold_source"] == "cascade" and overview["exact_levels"] == 1

    def test_exact_levels_knob_recorded(self):
        from zagg.sweep_overview import build_pyramid_block

        cfg = self._cfg(pyramid={"exact_levels": 2})
        assert build_pyramid_block(cfg, shard_order=6)["overview"]["exact_levels"] == 2

    def test_leaves_source_is_deprecated_and_carries_no_boundary(self, caplog):
        from zagg.sweep_overview import build_pyramid_block

        overview = build_pyramid_block(self._cfg(pyramid={"fold_source": "leaves"}), shard_order=6)[
            "overview"
        ]
        assert overview["fold_source"] == "leaves"
        # The boundary is a cascade-only concept: recording one under 'leaves'
        # would declare a distinction the store does not have (issue #376).
        assert "exact_levels" not in overview
        assert "DEPRECATED" in caplog.text

    def test_declared_off_block_carries_no_fold_keys(self):
        from zagg.sweep_overview import build_pyramid_block

        # Spec §4.5: the declared-off form is exactly ``{"orders": []}``.
        block = build_pyramid_block(self._cfg(pyramid=False), shard_order=6)
        assert block["overview"] == {"orders": []}

    def test_unusable_fold_knobs_fall_back_to_the_default(self, caplog):
        from zagg.sweep_overview import build_pyramid_block

        # The issue #358 retrofit path reaches build_pyramid_block without
        # validate_config, so an unusable value declares the default loudly.
        cfg = self._cfg(pyramid={"fold_source": "sideways", "exact_levels": 0})
        overview = build_pyramid_block(cfg, shard_order=6)["overview"]
        assert overview["fold_source"] == "cascade" and overview["exact_levels"] == 1
        assert "unknown fold_source" in caplog.text and "exact_levels must be" in caplog.text

    def test_the_deprecation_stays_silent_on_the_default_path(self, caplog):
        from zagg.sweep_overview import _fold_sources, build_pyramid_block

        # build_pyramid_block runs at every manifest bootstrap (hive.py) and
        # _fold_sources at every sweep, so the #376 deprecation must never fire
        # on the shipped default — a warning that cries wolf stops being read.
        build_pyramid_block(self._cfg(), shard_order=6)
        _fold_sources({}, [4, 2, 0], 8, 6)
        assert "DEPRECATED" not in caplog.text

    def test_exact_levels_past_the_last_level_warns_it_is_the_leaves_regime(self, caplog):
        from zagg.sweep_overview import build_pyramid_block

        # validate_config accepts exact_levels: 3 (it has no shard_order, so it
        # cannot know the schedule); here the derived orders are [4, 2, 0], and
        # a boundary of 3 silently buys the deprecated all-leaves regime under
        # a block that declares "cascade".
        overview = build_pyramid_block(self._cfg(pyramid={"exact_levels": 3}), shard_order=6)[
            "overview"
        ]
        assert overview["fold_source"] == "cascade" and overview["exact_levels"] == 3
        assert "covers all 3 declared levels" in caplog.text and "DEPRECATED" in caplog.text

    def test_validate_fold_grammar(self, caplog):
        from zagg.config import validate_config

        validate_config(self._cfg(store_layout="hive", pyramid={"exact_levels": 3}))
        with pytest.raises(ValueError, match="fold_source must be one of"):
            validate_config(self._cfg(store_layout="hive", pyramid={"fold_source": "sideways"}))
        with pytest.raises(ValueError, match="exact_levels must be an int >= 1"):
            validate_config(self._cfg(store_layout="hive", pyramid={"exact_levels": 0}))
        with pytest.raises(ValueError, match="exact_levels is meaningless"):
            validate_config(
                self._cfg(store_layout="hive", pyramid={"fold_source": "leaves", "exact_levels": 2})
            )
        validate_config(self._cfg(store_layout="hive", pyramid={"fold_source": "leaves"}))
        assert "DEPRECATED" in caplog.text


class TestPyramidV2Gate:
    """The /1 overview family generates nothing for /2 stores (issue #384).

    The declared-not-yet-sweepable REFUSAL is retired: /2 stores are fully
    sweepable via the STAGED sweep (``zagg.sweep_stages.run_stage_sweep``).
    This family still writes nothing for them — the /1 fold reads raw leaves
    per level, which the column regime exists to end — so these tests keep
    pinning the no-write posture, now under ``sweepable: True`` and the
    ``regime: "stages"`` pointer."""

    def _v2_manifest(self, root, *, windowed=False, all_time=False):
        manifest = _write_manifest(root, orders=(1, 0), windowed=windowed)
        manifest["pyramid"] = {
            "spec": "zagg-pyramid/2",
            "overviews": [
                {"node": 2, "cells": [3]},
                {"node": 1, "cells": [2]},
                {"node": 0, "cells": [1]},
            ],
            "overview": {
                "all_time": all_time,
                "fold_source": "cascade",
                "exact_levels": 1,
                "fields": dict(FIELDS_DECL),
            },
        }
        obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())
        return manifest

    def test_sweep_refuses_v2_loudly_and_writes_nothing(self, tmp_path, caplog):
        from zagg.sweep_overview import sweep_overviews

        manifest = self._v2_manifest(tmp_path)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        caplog.set_level("INFO")  # the /2 pointer is informational, not a refusal
        counts = sweep_overviews(str(tmp_path), manifest, {"-311": {None}})
        assert counts["sweepable"] is True and counts["regime"] == "stages"
        assert counts["declared"] is True
        assert counts["written"] == 0 and counts["failed"] == 0
        assert "staged sweep" in caplog.text
        # Declared-unswept is a recorded state, not a partial write: no
        # overview artifacts, no envelopes, the declaration untouched.
        assert not list(tmp_path.rglob("overview.rollup.json"))
        assert not (tmp_path / "-3" / "1" / "all.zarr").exists()
        assert json.loads((tmp_path / MANIFEST_NAME).read_text()) == manifest

    def test_levels_key_gates_even_under_a_v1_marker(self, tmp_path, caplog):
        from zagg.sweep_overview import sweep_overviews

        # Defense in depth: a hand-edited manifest carrying levels under a /1
        # marker is still the (node, cells) grammar — never fold it as orders.
        manifest = self._v2_manifest(tmp_path)
        manifest["pyramid"]["spec"] = PYRAMID_SPEC
        caplog.set_level("INFO")
        counts = sweep_overviews(str(tmp_path), manifest, {"-311": {None}})
        assert counts["regime"] == "stages" and counts["written"] == 0
        assert "staged sweep" in caplog.text

    def test_a_windowed_v2_store_writes_no_all_time_fold(self, tmp_path, caplog):
        from zagg.sweep_overview import sweep_overviews

        # The gate returns BEFORE `windowed = manifest.get("temporal") is not
        # None` is ever computed, so the windowed path is untested by the
        # unwindowed fixtures above: an all_time /2 store must produce zero
        # all.zarr writes too, not a partial cross-window fold.
        manifest = self._v2_manifest(tmp_path, windowed=True, all_time=True)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        caplog.set_level("INFO")
        counts = sweep_overviews(str(tmp_path), manifest, {"-311": {"2019"}})
        assert counts["sweepable"] is True and counts["declared"] is True
        assert counts["written"] == 0 and counts["failed"] == 0
        assert "staged sweep" in caplog.text
        for node in ("-3", "-3/1"):
            assert not (tmp_path / node / "all.zarr").exists()
            assert not (tmp_path / node / "2019.zarr").exists()
        assert not list(tmp_path.rglob("overview.rollup.json"))
        assert json.loads((tmp_path / MANIFEST_NAME).read_text())["pyramid"] == manifest["pyramid"]

    def test_a_bare_v2_marker_declares_nothing(self, tmp_path):
        from zagg.sweep_overview import sweep_overviews

        # The `spec`-only arm: a /2 marker with no overview block is a
        # declaration of nothing, and must report `declared: False` exactly as
        # the /1 branch does for an absent/empty `orders`.
        manifest = self._v2_manifest(tmp_path)
        manifest["pyramid"] = {"spec": "zagg-pyramid/2"}
        counts = sweep_overviews(str(tmp_path), manifest, {"-311": {None}})
        assert counts["declared"] is False and counts["regime"] == "stages"


#: A located declaration for the harness above: the same ``h_tdigest`` field,
#: plus the §9 channel ruling 4 makes foldable (issue #410).
LOCATED_FIELDS_DECL = {
    **{k: v for k, v in FIELDS_DECL.items() if k != "h_tdigest"},
    "h_tdigest": {**FIELDS_DECL["h_tdigest"], "location": "leaf_id"},
}


#: The same field carrying BOTH companion channels (espg ruling of 2026-08-17:
#: temporal is per-centroid at every level, symmetric with located). This is the
#: arity the ``channels=`` plumbing exists for — one field, two siblings folded
#: through one k-way call.
BOTH_CHANNEL_FIELDS_DECL = {
    **LOCATED_FIELDS_DECL,
    "h_tdigest": {**LOCATED_FIELDS_DECL["h_tdigest"], "temporal": "per-centroid"},
}

#: And the temporal channel ALONE, which is what a ``build_tdigest`` field with
#: a clock and no ``location:`` declares.
TIMED_FIELDS_DECL = {
    **{k: v for k, v in FIELDS_DECL.items() if k != "h_tdigest"},
    "h_tdigest": {**FIELDS_DECL["h_tdigest"], "temporal": "per-centroid"},
}


def _located_leaf_cfg(*, location=True, temporal=False):
    from zagg.config import PipelineConfig

    digest = {
        "kind": "ragged",
        "function": "zagg.stats.tdigest.build_tdigest",
        "inner_shape": [2],
        "dtype": "float32",
        "fill_value": 0,
    }
    if location:
        digest["location"] = "leaf_id"
    if temporal:
        digest["temporal"] = "per-centroid"
    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_min": {"function": "min", "dtype": "float32"},
                "h_mean": {"function": "mean", "dtype": "float32"},
                "h_tdigest": digest,
            },
        }
    )


def _make_located_leaf(root, decimal, cells, *, window=None, location=True, temporal=False):
    """A committed leaf carrying the payload AND its declared companions.

    ``cells`` maps leaf row -> observation values; each cell's per-observation
    morton point words come from ``conftest.point_words``, seeded on the row so
    two cells never share a word, and its toc words from ``conftest.toc_words``
    offset by the row so two cells never share an instant either.

    Returns the per-row truth: the location words alone in the located-only
    default (what every pre-#410 caller reads), and ``(locations, temporal)``
    dicts whenever more than one channel is written.
    """
    from conftest import TOC_BASE, point_words, toc_words
    from mortie import generate_morton_children

    from zagg.grids.healpix import HealpixGrid
    from zagg.stats.tdigest import build_tdigest

    grid = HealpixGrid(
        SHARD_ORDER, CELL_ORDER, config=_located_leaf_cfg(location=location, temporal=temporal)
    )
    word = morton_word(decimal)
    store = open_store(shard_leaf_path(str(root), word, window=window))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(CELL_ORDER), mode="r+", zarr_format=3)
    n = 4 ** (CELL_ORDER - SHARD_ORDER)
    group["morton"][:] = np.asarray(generate_morton_children(word, CELL_ORDER), dtype=np.uint64)
    count = np.zeros(n, np.int32)
    h_min = np.full(n, np.nan, np.float32)
    h_mean = np.full(n, np.nan, np.float32)
    digest = np.full(n, b"", dtype=object)
    locs = np.full(n, b"", dtype=object)
    times = np.full(n, b"", dtype=object)
    truth: dict = {}
    time_truth: dict = {}
    for i, obs in cells.items():
        obs = np.asarray(obs, dtype=np.float64)
        kw = {}
        if location:
            kw["locations"] = point_words(len(obs), seed=1000 + i)
        if temporal:
            # Distinct base per row so no two cells' envelopes overlap.
            base = str(np.datetime64(TOC_BASE, "ns") + np.timedelta64(3600 * (i + 1), "s"))
            kw["temporal"] = toc_words(len(obs), base=base)
        d, *words = build_tdigest(obs, 64, **kw)
        count[i] = len(obs)
        h_min[i] = obs.min()
        h_mean[i] = obs.mean()
        digest[i] = encode_digest(d, "float32")
        emitted = dict(zip(kw, words, strict=True))
        if location:
            locs[i] = encode_digest(emitted["locations"], "uint64")
            truth[i] = kw["locations"]
        if temporal:
            times[i] = encode_digest(emitted["temporal"], "uint64")
            time_truth[i] = kw["temporal"]
    group["count"][:] = count
    group["h_min"][:] = h_min
    group["h_mean"][:] = h_mean
    group["h_tdigest"][:] = digest
    if location:
        group["h_tdigest_locations"][:] = locs
    if temporal:
        group["h_tdigest_times"][:] = times
    stamp_commit(store, cells_with_data=len(cells), granule_count=1, window=window)
    if location and temporal:
        return truth, time_truth
    return time_truth if temporal else truth


def _contains_all(ancestor, members):
    """True when ``ancestor``'s cell contains every member word (spec §9.1).

    Compared against ``common_ancestor([ancestor, ancestor])`` rather than
    ``ancestor`` itself, because the fold is kind-aware: an order-29 POINT word
    carries no area claim (spec §2.2), so folding one with itself yields that
    cell's AREA word, not the point word back. The invariant that matters is
    that adding the members coarsens no further than the ancestor's own cell.
    """
    from mortie import common_ancestor

    a = np.uint64(ancestor)
    own = int(common_ancestor(np.asarray([a, a], dtype=np.uint64)))
    return int(common_ancestor(np.asarray([a, *members], dtype=np.uint64))) == own


class TestLocatedOverviewFold:
    """Ruling 4 on issue #410: a located field folds THROUGH the pyramid.

    Before this, `location:` put the field in class ``none`` and the digest
    vanished from every overview level. These pin the channel end to end: the
    sibling is emitted by the overview template, folded in the SAME k-way call
    as its payload, and its words satisfy §9.1's containment claim.
    """

    def test_leaf_fold_writes_the_sibling_row_aligned(self, tmp_path):
        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        truth = _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0, 3.0], 1: [7.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 0
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert "h_tdigest_locations" in g
        # Overview cell 0 folds leaf rows 0..3, so it merges both populated
        # cells' digests -- and its words must contain every contributor.
        payload = decode_digest(bytes(g["h_tdigest"][:][0]), "float32")
        words = decode_digest(bytes(g["h_tdigest_locations"][:][0]), "uint64", ())
        assert words.dtype == np.uint64
        assert words.shape == (payload.shape[0],), "the sibling stays row-aligned (§1.1)"
        # Every word against ITS OWN members, partitioned by the digest's weights
        # -- the same per-centroid check ``test_overview_words_contain_their_
        # contributors`` makes, so a fold that got every word but the zeroth
        # wrong cannot pass (review finding).
        members = np.concatenate([truth[0], truth[1]])
        bounds = np.concatenate([[0], np.cumsum(payload[:, 1]).astype(np.int64)])
        for i, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
            assert _contains_all(int(np.uint64(words[i])), members[lo:hi])
        # Every stored word contains at least itself and is a real morton word.
        from mortie import validate_morton

        validate_morton(words)

    def test_overview_words_sit_at_heterogeneous_orders(self, tmp_path):
        # §9.1's normative property, and the one this phase newly creates: "A
        # fold coarsens only as far as its contributors force, so one overview
        # array's words routinely carry different orders", and a reader "MUST
        # NOT assume a uniform order per array, per level, or per store". At
        # delta=64 a 3-observation cell merges nothing, so its centroids keep
        # their order-29 POINT words while cell 1's single observation folded
        # with nothing else also stays a point word -- to get a MERGED (coarse
        # area) word beside them the cell needs more observations than delta
        # allows to stay singletons.
        from mortie import is_point, orders_of

        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        _make_located_leaf(tmp_path, "-311", {0: list(np.arange(400.0)), 1: [7.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        payload = decode_digest(bytes(g["h_tdigest"][:][0]), "float32")
        words = decode_digest(bytes(g["h_tdigest_locations"][:][0]), "uint64", ())
        orders = np.asarray(orders_of(words))
        singletons = payload[:, 1] == 1.0
        assert singletons.any() and (~singletons).any(), "need both merged and unmerged centroids"
        assert len(set(orders.tolist())) > 1, "§9.1: one array, heterogeneous orders"
        # The unmerged centroids keep their order-29 POINT words; the merged ones
        # are area words at strictly coarser orders -- kind and order both
        # decoded from the word, never from the level (§9.1, mortie §1/§4).
        points = np.asarray(is_point(words))
        assert points[singletons].all() and not points[~singletons].any()
        assert set(orders[singletons].tolist()) == {29}
        assert orders[~singletons].max() < 29

    def test_overview_words_contain_their_contributors(self, tmp_path):
        # §9.1's whole claim: a merged centroid's word is a cell containing
        # every observation merged into it. Values rise with the observation
        # index here, so a centroid's members are a contiguous run of the
        # value-sorted words -- which is what makes the claim checkable.
        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        truth = _make_located_leaf(tmp_path, "-311", {0: list(np.arange(40.0))})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        payload = decode_digest(bytes(g["h_tdigest"][:][0]), "float32")
        words = decode_digest(bytes(g["h_tdigest_locations"][:][0]), "uint64", ())
        bounds = np.concatenate([[0], np.cumsum(payload[:, 1]).astype(np.int64)])
        src = truth[0]  # already in observation order == value order
        for i, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
            assert _contains_all(int(np.uint64(words[i])), src[lo:hi])

    def test_the_overview_sibling_carries_the_section_9_declaration(self, tmp_path):
        from zagg.grids.base import located_declaration

        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert located_declaration(dict(g["h_tdigest_locations"].attrs)) == {
            "spec": "zagg-located/1",
            "shape": "per-centroid",
            "grammar": "mortie-morton/1",
        }
        # The payload binds the sibling by NAME (§1.2), never by convention.
        assert dict(g["h_tdigest"].attrs)["ragged"]["locations"] == "h_tdigest_locations"

    def test_cascade_folds_the_sibling_too(self, tmp_path):
        # The fold-of-folds path (issue #376) reads an overview's own sibling
        # and re-merges it; a cascade that folded only the payload would emit an
        # overview whose channel described a stale partition.
        _write_manifest(
            tmp_path,
            orders=(1, 0),
            fields=LOCATED_FIELDS_DECL,
            fold_source="cascade",
            exact_levels=1,
        )
        truth = _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0], 5: [9.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 0
        coarse = _overview_group(tmp_path, "-3", "all.zarr", 2)
        assert "h_tdigest_locations" in coarse
        payload = decode_digest(bytes(coarse["h_tdigest"][:][0]), "float32")
        words = decode_digest(bytes(coarse["h_tdigest_locations"][:][0]), "uint64", ())
        assert words.shape == (payload.shape[0],)
        assert _contains_all(int(np.uint64(words[0])), np.concatenate([truth[0], truth[5]])[:1])

    def test_a_leaf_without_the_sibling_is_skipped_loudly(self, tmp_path):
        # The channel is read in the payload's guarded block, so a leaf that
        # carries the payload but not the sibling contributes NEITHER -- never a
        # digest folded with its channel silently dropped.
        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]})  # unlocated leaf writer
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 1
        assert result["families"]["overview"]["written"] == 0

    def test_unlocated_fields_are_byte_identical(self, tmp_path):
        # The channel is opt-in: an undeclared field's overview must be exactly
        # what it was before ruling 4.
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0, 3.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert "h_tdigest_locations" not in g
        assert "locations" not in dict(g["h_tdigest"].attrs)["ragged"]


def _toc_contains(envelope, members):
    """True when ``envelope`` covers every member instant (spec §8.3).

    The temporal analogue of :func:`_contains_all`: the grammar's join is
    idempotent and monotone, so adding the members widens nothing exactly when
    the envelope already covers them. Compared against the envelope joined with
    itself for the same reason the located helper is — a singleton timestamp
    word is not the same bit pattern as a range covering that instant.
    """
    from zagg.stats.toc import cell_envelope

    e = np.uint64(envelope)
    own = int(cell_envelope(np.asarray([e, e], dtype=np.uint64)))
    return int(cell_envelope(np.asarray([e, *members], dtype=np.uint64))) == own


class TestBothChannelsOverviewFold:
    """Two channels on ONE field, folded through a single k-way call (#410).

    The espg ruling of 2026-08-17 makes temporal per-centroid at every level,
    symmetric with located, and the fold plumbing generalized from one
    ``locations=`` kwarg to a ``channels=`` map to carry it. With a single
    channel the return is a 2-tuple and every ordering question degenerates, so
    nothing pinned the arity the map exists for. These fold BOTH siblings and
    check each against its OWN grammar, which is what fails when a channel is
    dropped, folded in a second pass, or swapped with its sibling — and the two
    word grammars are mutually accepting, so a swap raises nowhere on its own
    (review finding).
    """

    def _fold(self, tmp_path, cells, *, fields=None, decl=None):
        _write_manifest(tmp_path, orders=(1,), fields=fields or BOTH_CHANNEL_FIELDS_DECL)
        truth = _make_located_leaf(tmp_path, "-311", cells, **(decl or {"temporal": True}))
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 0
        return truth, _overview_group(tmp_path, "-3/1", "all.zarr", 3)

    @staticmethod
    def _row(g, j=0):
        payload = decode_digest(bytes(g["h_tdigest"][:][j]), "float32")
        locs = decode_digest(bytes(g["h_tdigest_locations"][:][j]), "uint64", ())
        times = decode_digest(bytes(g["h_tdigest_times"][:][j]), "uint64", ())
        return payload, locs, times

    def test_both_siblings_are_emitted_row_aligned_and_declared(self, tmp_path):
        from zagg.grids.base import located_declaration
        from zagg.time_axis import temporal_declaration

        _, g = self._fold(tmp_path, {0: [1.0, 2.0, 3.0], 1: [7.0]})
        assert "h_tdigest_locations" in g and "h_tdigest_times" in g
        payload, locs, times = self._row(g)
        assert locs.dtype == times.dtype == np.uint64
        assert locs.shape == times.shape == (payload.shape[0],), "§1.1 row alignment, both"
        # Each sibling declares its OWN convention, and the payload binds both
        # by name (§1.2) — never one block covering two channels.
        assert located_declaration(dict(g["h_tdigest_locations"].attrs)) == {
            "spec": "zagg-located/1",
            "shape": "per-centroid",
            "grammar": "mortie-morton/1",
        }
        assert temporal_declaration(dict(g["h_tdigest_times"].attrs)) == {
            "spec": "zagg-toc/1",
            "shape": "per-centroid",
            "grammar": "mortie-toc/1",
        }
        attrs = dict(g["h_tdigest"].attrs)
        assert attrs["ragged"]["locations"] == "h_tdigest_locations"
        assert attrs["times"] == "h_tdigest_times"

    # Leaf rows 0..3 all fold into overview cell 0, so ONE populated row takes
    # ``fold_digests``' single-contributor arm — which bypasses the k-way merge,
    # and is the majority case at the finest level — and two take the merge arm.
    # Both must put the same word in the same slot.
    @pytest.mark.parametrize("rows", [[0], [0, 1]], ids=["single-arm", "merge-arm"])
    def test_singleton_centroids_round_trip_both_words_unswapped(self, tmp_path, rows):
        # At delta=64 these observations merge nothing, so every centroid is a
        # singleton and BOTH channels must reproduce their contributor's exact
        # word. This is the decisive anti-swap pin: the toc slot holding the
        # morton word (or vice versa) is a well-formed word in either grammar
        # and raises nowhere, but it is not THIS word.
        cells = {r: [1.0 + 8 * r, 2.0 + 8 * r] for r in rows}
        (locs_truth, times_truth), g = self._fold(tmp_path, cells)
        payload, locs, times = self._row(g)
        assert (payload[:, 1] == 1.0).all(), "need singleton centroids"
        # Values ascend across the rows, so the value-sorted centroids are row
        # 0's, then row 1's.
        np.testing.assert_array_equal(locs, np.concatenate([locs_truth[r] for r in rows]))
        np.testing.assert_array_equal(times, np.concatenate([times_truth[r] for r in rows]))
        assert not np.array_equal(locs, times)

    def test_merged_centroids_contain_their_members_in_both_grammars(self, tmp_path):
        # The real fold: 400 observations at delta=64 forces genuine merges, and
        # each centroid's location must ENCLOSE its members (§9.1) while its toc
        # word must COVER their instants (§8.3) — one partition, two claims.
        (locs_truth, times_truth), g = self._fold(tmp_path, {0: list(np.arange(400.0))})
        payload, locs, times = self._row(g)
        assert (payload[:, 1] > 1.0).any(), "need merged centroids"
        bounds = np.concatenate([[0], np.cumsum(payload[:, 1]).astype(np.int64)])
        for i, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
            assert _contains_all(int(np.uint64(locs[i])), locs_truth[0][lo:hi])
            assert _toc_contains(times[i], times_truth[0][lo:hi])
        # And neither slot satisfies the OTHER channel's claim, which is what a
        # swap would look like on bytes that pass every validator.
        assert not _toc_contains(locs[0], times_truth[0][: int(payload[0, 1])])

    def test_cascade_folds_both_siblings(self, tmp_path):
        # The fold-of-folds path re-reads an overview's own companions; one that
        # carried only the located channel forward would leave the temporal
        # sibling describing the finer level's partition.
        _write_manifest(
            tmp_path,
            orders=(1, 0),
            fields=BOTH_CHANNEL_FIELDS_DECL,
            fold_source="cascade",
            exact_levels=1,
        )
        locs_truth, times_truth = _make_located_leaf(
            tmp_path, "-311", {0: [1.0, 2.0], 5: [9.0]}, temporal=True
        )
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 0
        coarse = _overview_group(tmp_path, "-3", "all.zarr", 2)
        payload, locs, times = self._row(coarse)
        assert locs.shape == times.shape == (payload.shape[0],)
        # Cell-level identity: the coarse envelope over the column equals the
        # envelope over every leaf instant that reached it, whatever partition
        # the two folds passed through (spec §8.3).
        from mortie import common_ancestor

        from zagg.stats.toc import cell_envelope

        members_t = np.concatenate([times_truth[0], times_truth[5]])
        members_l = np.concatenate([locs_truth[0], locs_truth[5]])
        assert int(cell_envelope(times)) == int(cell_envelope(members_t))
        assert int(common_ancestor(np.asarray(locs, dtype=np.uint64))) == int(
            common_ancestor(np.asarray(members_l, dtype=np.uint64))
        )

    def test_temporal_alone_folds_through_the_pyramid(self, tmp_path):
        # A ``build_tdigest`` field with a clock and no ``location:``. Every
        # other temporal test carries a location too, so nothing pinned the
        # one-channel temporal shape on its own (review finding).
        times_truth, g = self._fold(
            tmp_path,
            {0: list(np.arange(400.0))},
            fields=TIMED_FIELDS_DECL,
            decl={"location": False, "temporal": True},
        )
        assert "h_tdigest_locations" not in g
        payload = decode_digest(bytes(g["h_tdigest"][:][0]), "float32")
        times = decode_digest(bytes(g["h_tdigest_times"][:][0]), "uint64", ())
        assert times.shape == (payload.shape[0],)
        bounds = np.concatenate([[0], np.cumsum(payload[:, 1]).astype(np.int64)])
        for i, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
            assert _toc_contains(times[i], times_truth[0][lo:hi])


#: The waveform-store declaration for the harness above (issue #508): the
#: digest family's new member, per-centroid companion, no located channel —
#: the gedi01b shape reduced to the fold-relevant keys.
WAVEFORM_FIELDS_DECL = {
    "count": {"class": "exact", "method": "sum", "dtype": "int32", "fill_value": 0},
    "rx_flux": {
        "class": "approximate",
        "method": "tdigest_kway",
        "dtype": "float32",
        "inner_shape": [2],
        "delta": 64,
        "overview_delta": 64,
        "temporal": "per-centroid",
    },
}


def _waveform_leaf_cfg():
    from zagg.config import PipelineConfig

    return PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "rx_flux": {
                    "kind": "ragged",
                    "function": "zagg.stats.waveform.build_waveform_digest",
                    "inner_shape": [2],
                    "temporal": "per-centroid",
                    "dtype": "float32",
                    "fill_value": 0,
                },
            },
        }
    )


def _make_waveform_leaf(root, decimal, cells, *, seed=11):
    """A committed leaf whose payloads come from ``build_waveform_digest``.

    ``cells`` maps leaf row -> sample count; every sample carries an INTEGER
    photoelectron count over a zero noise floor, so each cell's flux (the sum
    of its centroid weights) is exactly its integer count total — sums of
    small ints are exact in float32, which is what lets the per-level parity
    assertions below demand equality, not closeness. Returns ``(flux_total,
    {row: (digest, words)})``.
    """
    from conftest import TOC_BASE, toc_words
    from mortie import generate_morton_children

    from zagg.grids.healpix import HealpixGrid
    from zagg.stats.waveform import build_waveform_digest

    grid = HealpixGrid(SHARD_ORDER, CELL_ORDER, config=_waveform_leaf_cfg())
    word = morton_word(decimal)
    store = open_store(shard_leaf_path(str(root), word))
    grid.emit_shard_template(store, overwrite=True)
    group = zarr.open_group(store, path=str(CELL_ORDER), mode="r+", zarr_format=3)
    n_cells = 4 ** (CELL_ORDER - SHARD_ORDER)
    group["morton"][:] = np.asarray(generate_morton_children(word, CELL_ORDER), dtype=np.uint64)
    rng = np.random.default_rng(seed)
    count = np.zeros(n_cells, np.int32)
    digest = np.full(n_cells, b"", dtype=object)
    times = np.full(n_cells, b"", dtype=object)
    flux_total = 0.0
    truth = {}
    for i, n in cells.items():
        values = rng.normal(0.0, 10.0, n)
        counts = rng.integers(2, 30, n).astype(np.float64)
        base = str(np.datetime64(TOC_BASE, "ns") + np.timedelta64(3600 * (i + 1), "s"))
        d, w = build_waveform_digest(
            values,
            64,
            counts=counts,
            noise_mean=np.zeros(n),
            noise_stddev=np.full(n, 0.25),
            temporal=toc_words(n, base=base),
        )
        # Zero noise floor + counts >= 2 clear the clip threshold (0.77 at the
        # reducer's default operating point, n_σ = 3.09 against σ = 0.25; the
        # margin holds at the shipped gedi01b point too, n_σ = 4.49 → 1.12), so
        # nothing is dropped and the digest mass IS the count total.
        assert float(d[:, 1].sum()) == float(counts.sum())
        count[i] = n
        digest[i] = encode_digest(d, "float32")
        times[i] = encode_digest(w, "uint64")
        flux_total += float(counts.sum())
        truth[i] = (d, w)
    group["count"][:] = count
    group["rx_flux"][:] = digest
    group["rx_flux_times"][:] = times
    stamp_commit(store, cells_with_data=len(cells), granule_count=1)
    return flux_total, truth


class TestWaveformOverviewFold:
    """The runbook step-2 fold gates, in-repo (issue #508 phase 4).

    A synthetic waveform store — leaves built by ``build_waveform_digest``
    itself — swept through the production overview fold: per-level flux
    ``weight_total`` equals the leaf total through the k-way law at EVERY
    declared level (the weight-agnostic licensing fact, issue #431 §2), the
    ``rx_flux_times`` sibling rides every level row-aligned, and the ragged
    ``(2,)`` element round-trips.
    """

    def _sweep(self, tmp_path, cells, orders=(1, 0)):
        _write_manifest(tmp_path, orders=orders, fields=WAVEFORM_FIELDS_DECL)
        leaf_total, truth = _make_waveform_leaf(tmp_path, "-311", cells)
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 0
        return leaf_total, truth

    def test_weight_total_parity_and_times_at_every_level(self, tmp_path):
        # 300 samples at δ64 force genuine k-way merges at the fine level and
        # a fold-of-merges at the coarse one; the mass must survive both.
        from zagg.stats.toc import cell_envelope

        leaf_total, truth = self._sweep(tmp_path, {0: 300, 3: 80, 7: 41})
        for node_rel, order, partitioned in (("-3/1", 3, True), ("-3", 2, False)):
            g = _overview_group(tmp_path, node_rel, "all.zarr", order)
            rows = [decode_digest(bytes(b), "float32") for b in g["rx_flux"][:]]
            level_total = sum(float(d[:, 1].sum()) for d in rows if d.size)
            assert level_total == leaf_total, f"mass lost/invented at {node_rel}"
            words = [decode_digest(bytes(b), "uint64", ()) for b in g["rx_flux_times"][:]]
            # Leaf rows fold into contiguous runs of the coarser level, so its
            # row ``j`` is fed exactly by leaf rows ``j * per_row ...`` (the
            # slots past the level's own cell count stay empty). Every
            # ``_make_waveform_leaf`` row sits an hour off its neighbour, so the
            # words below are pinned to the CONTRIBUTORS' instants rather than
            # to some merely well-formed word: a column carried forward from
            # another row widens the envelope out of the hour and fails here.
            per_row = LEAF_CELLS // 4 ** (order - SHARD_ORDER)
            populated = 0
            for j, (d, w) in enumerate(zip(rows, words, strict=True)):
                assert w.shape == (d.shape[0],), "§1.1 row alignment"
                members = [truth[i] for i in range(j * per_row, (j + 1) * per_row) if i in truth]
                if not members:
                    assert w.size == 0, f"words in an unfed row of {node_rel}"
                    continue
                take = np.argsort(np.concatenate([t[0][:, 0] for t in members]), kind="stable")
                weights = np.concatenate([t[0][:, 1] for t in members])[take]
                instants = np.concatenate([t[1] for t in members])[take]
                if partitioned:
                    # The fine level merges the leaf centroids themselves, whole
                    # and value-ordered, so the leaf's own words index it: cut
                    # by cumulative FLUX (these weights are photoelectron
                    # counts, not observation counts) and each merged word must
                    # cover its contributors' instants — §8.3 over the §9.1
                    # partition, as in ``TestBothChannelsOverviewFold``.
                    fed = np.cumsum(weights)
                    got = np.concatenate([[0.0], np.cumsum(d[:, 1].astype(np.float64))])
                    for k, (lo, hi) in enumerate(zip(got[:-1], got[1:], strict=True)):
                        lo_i = int(np.searchsorted(fed, lo, side="right"))
                        hi_i = int(np.searchsorted(fed, hi, side="right"))
                        assert hi_i > lo_i, "a centroid folded no contributor whole"
                        assert _toc_contains(w[k], instants[lo_i:hi_i])
                # The coarse level folds the FINE level's centroids, whose
                # values are weighted means and so no longer interleave in leaf
                # order — the claim that survives the re-partition is the
                # cell-level identity, exactly as ``test_cascade_folds_both_
                # siblings`` states it: nothing invented, nothing dropped.
                assert int(cell_envelope(w)) == int(cell_envelope(instants))
                populated += len(w)
            assert populated > 0

    def test_single_contributor_row_round_trips_byte_identical(self, tmp_path):
        # One populated leaf row under the coarse cell: whichever arm serves it
        # (the k-way merge is byte-idempotent on one digest, so the two are
        # indistinguishable here — ``TestBothChannelsOverviewFold`` parametrizes
        # them apart), the overview element must be the leaf's EXACT bytes and
        # words. The ragged (2,) element of a builder-origin payload survives
        # the fold and re-encodes byte-identically.
        _, truth = self._sweep(tmp_path, {0: 25}, orders=(1,))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        raw = bytes(g["rx_flux"][:][0])
        d = decode_digest(raw, "float32")
        assert d.ndim == 2 and d.shape[1] == 2
        np.testing.assert_array_equal(d, truth[0][0].astype(np.float32))
        assert encode_digest(d, "float32") == raw
        np.testing.assert_array_equal(
            decode_digest(bytes(g["rx_flux_times"][:][0]), "uint64", ()), truth[0][1]
        )


class TestLocatedDeclarationGate:
    """The §9 declaration is checked at retrofit time AND at fold time.

    §9.2 imports §8.4: companions compose only on matching ``{shape, grammar}``
    and a writer joining ones that differ MUST refuse. The pair of checks is
    the §2.0 ``weights`` precedent (issue #424): a disagreement caught by
    ``_field_drift`` is the retrofit refusing, and one missed there is every
    leaf refusing at fold time.
    """

    def _leaf_group(self, root, mode="r+"):
        store = open_store(shard_leaf_path(str(root), morton_word("-311")))
        return zarr.open_group(store, path=str(CELL_ORDER), mode=mode, zarr_format=3)

    def _drift(self, root):
        from zagg.sweep_overview import _field_drift

        return _field_drift(
            self._leaf_group(root, mode="r"), "h_tdigest", LOCATED_FIELDS_DECL["h_tdigest"]
        )

    def test_the_written_sibling_passes_the_gate(self, tmp_path):
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        assert self._drift(tmp_path) is None

    def test_an_equivalent_dtype_spelling_is_not_drift(self, tmp_path):
        # ``"<u8"`` IS uint64; a raw string compare would report spurious drift.
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        sib = self._leaf_group(tmp_path)["h_tdigest_locations"]
        block = dict(sib.attrs["ragged"])
        block["element"] = {**block["element"], "dtype": "<u8"}
        sib.attrs["ragged"] = block
        assert self._drift(tmp_path) is None

    def test_a_sibling_element_shape_is_drift(self, tmp_path):
        # The fold decodes the words as a FLAT vector, so an inner shape would
        # be silently reinterpreted rather than refused.
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        sib = self._leaf_group(tmp_path)["h_tdigest_locations"]
        block = dict(sib.attrs["ragged"])
        block["element"] = {**block["element"], "shape": [-1, 2]}
        sib.attrs["ragged"] = block
        assert "element shape" in self._drift(tmp_path)

    def test_an_unimplemented_shape_raises_out_of_the_gate(self, tmp_path):
        # Not drift but UNREADABLE — the posture ``_field_drift``'s docstring
        # already reserves for a store this zagg cannot decode.
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        sib = self._leaf_group(tmp_path)["h_tdigest_locations"]
        sib.attrs["located"] = {**dict(sib.attrs["located"]), "shape": "per-cell"}
        with pytest.raises(ValueError, match="is not implemented"):
            self._drift(tmp_path)

    def test_an_absent_declaration_is_not_drift(self, tmp_path):
        # §9's absent-key rule: every located store written before the
        # declaration stays conformant, no byte rewritten.
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        sib = self._leaf_group(tmp_path)["h_tdigest_locations"]
        del sib.attrs["located"]
        assert self._drift(tmp_path) is None
        assert check_companion_match(dict(sib.attrs), "h_tdigest") is None

    def test_the_leaf_fold_refuses_an_unimplemented_shape(self, tmp_path):
        # The fold-time counterpart: the leaf is skipped LOUDLY, exactly as a
        # mismatched §2.0 weights declaration skips it (issue #424).
        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        sib = self._leaf_group(tmp_path)["h_tdigest_locations"]
        sib.attrs["located"] = {**dict(sib.attrs["located"]), "grammar": "not-mortie/9"}
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["failed"] == 1
        assert result["families"]["overview"]["written"] == 0

    def test_the_cascade_refuses_an_unimplemented_shape(self, tmp_path):
        # Same check on the fold-of-folds path, which reads the OVERVIEW's own
        # sibling. ``_cascade_node`` wraps this call in its per-child guard, so
        # the raise becomes a loud skip there (``failed`` + ``unreadable``).
        from zagg.sweep_overview import _fold_child

        _write_manifest(tmp_path, orders=(1,), fields=LOCATED_FIELDS_DECL)
        _make_located_leaf(tmp_path, "-311", {0: [1.0, 2.0], 5: [9.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        store = open_store(f"{tmp_path}/-3/1/all.zarr")
        fine = zarr.open_group(store, path="3", mode="r+", zarr_format=3)
        assert _fold_child(fine, LOCATED_FIELDS_DECL, 4, 4, "fine") is not None
        fine["h_tdigest_locations"].attrs["located"] = {
            "spec": "zagg-located/1",
            "shape": "per-cell",
            "grammar": "mortie-morton/1",
        }
        with pytest.raises(ValueError, match="is not implemented"):
            _fold_child(fine, LOCATED_FIELDS_DECL, 4, 4, "fine")


class TestOverviewWriter:
    def test_folds_leaves_at_every_declared_order(self, tmp_path):
        from zagg.stats.tdigest import quantile_from_tdigest

        _write_manifest(tmp_path, orders=(1, 0))
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0], 5: [10.0]})
        _make_leaf(tmp_path, "-312", {0: [3.0]})
        refs = [(morton_word("-311"), None), (morton_word("-312"), None)]
        result = run_sweep(str(tmp_path), refs, families=("overview",))
        counts = result["families"]["overview"]
        # order 1: node -31; order 0: node -3 -> two overview zarrs.
        assert counts["written"] == 2 and counts["failed"] == 0
        # The run record's counts schema does not vary by revision: a /1 store
        # carries `sweepable` too (the /2 gate is the only False).
        assert counts["sweepable"] is True and counts["declared"] is True

        # Order 1 (cells at order 3, 4 source cells per overview cell):
        # leaf -311 occupies overview rows 0-3, leaf -312 rows 4-7.
        g1 = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        np.testing.assert_array_equal(g1["count"][:8], [2, 1, 0, 0, 1, 0, 0, 0])
        assert g1["h_min"][0] == 1.0 and g1["h_min"][4] == 3.0
        merged = decode_digest(bytes(g1["h_tdigest"][:][0]), "float32")
        assert np.isclose(quantile_from_tdigest(merged, 0.5), 2.0, atol=0.6)
        # morton is the order-3 subtree of -31, ascending.
        assert g1["morton"].shape == (16,) and g1["morton"].dtype == np.uint64

        # Order 0 (cells at order 2 == the source shard order): each leaf
        # folds whole into its shard's cell — count 3 for -311, 1 for -312.
        g0 = _overview_group(tmp_path, "-3", "all.zarr", 2)
        np.testing.assert_array_equal(g0["count"][:2], [3, 1])
        assert g0["h_min"][0] == 1.0 and g0["h_min"][1] == 3.0

    def test_role_and_provenance_attrs(self, tmp_path):
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        root = _overview_root(tmp_path, "-3/1", "all.zarr")
        assert root.attrs[ROLE_ATTR] == "overview"
        info = root.attrs[OVERVIEW_ATTR]
        assert info["order"] == 1 and info["cell_order"] == 3
        assert info["source_shard_order"] == SHARD_ORDER
        assert info["source_cell_order"] == CELL_ORDER
        assert info["fields"]["count"] == {
            "class": "exact",
            "method": "sum",
            "nan_policy": "skip",
        }
        assert info["fields"]["h_tdigest"]["method"] == "tdigest_kway"
        assert "nan_policy" not in info["fields"]["h_tdigest"]
        assert info["generation"]["n_leaves"] == 1
        # The overview is a stamped, committed zarr (D4 semantics apply).
        stamp = read_commit(open_store(str(tmp_path / "-3" / "1" / "all.zarr")))
        assert stamp is not None and stamp["granule_count"] == 1

    def test_none_field_is_absent_from_the_overview(self, tmp_path):
        # Option A (the ruled D24 default): h_mean exists at the leaves but is
        # excluded from every overview order; the pyramid block declares it.
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0, 5.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert "h_mean" not in g
        assert "count" in g and "h_tdigest" in g

    def test_no_declaration_is_a_noop(self, tmp_path):
        manifest = _write_manifest(tmp_path, orders=(1,))
        manifest["pyramid"] = {"orders": [], "aggregation": {}}  # the legacy block
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["declared"] is False
        assert result["families"]["overview"]["written"] == 0
        assert not (tmp_path / "-3" / "1" / "all.zarr").exists()

    def test_unstamped_debris_leaf_is_invisible(self, tmp_path):
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        # A torn worker's leaf: template only, no commit stamp (D4).
        from zagg.grids.healpix import HealpixGrid

        grid = HealpixGrid(SHARD_ORDER, CELL_ORDER, config=_leaf_cfg())
        debris = open_store(shard_leaf_path(str(tmp_path), morton_word("-312")))
        grid.emit_shard_template(debris, overwrite=True)
        refs = [(morton_word("-311"), None), (morton_word("-312"), None)]
        run_sweep(str(tmp_path), refs, families=("overview",))
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        info = _overview_root(tmp_path, "-3/1", "all.zarr").attrs[OVERVIEW_ATTR]
        assert info["generation"]["n_leaves"] == 1
        np.testing.assert_array_equal(g["count"][4:8], [0, 0, 0, 0])

    def test_envelope_records_window_inventory(self, tmp_path):
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        envelope = json.loads((tmp_path / "-3" / "1" / "overview.rollup.json").read_text())
        assert envelope["family"] == "overview" and envelope["node"] == "-31"
        entry = envelope["windows"]["all"]
        assert entry["object"] == "all.zarr"
        assert entry["generation"]["n_leaves"] == 1
        assert entry["content_hash"]

    def test_windowed_per_window_and_all_time(self, tmp_path):
        _write_manifest(tmp_path, orders=(1,), windowed=True, all_time=True)
        _make_leaf(
            tmp_path,
            "-311",
            {0: [1.0, 2.0]},
            window="2019",
            time_range=["2019-02-01T00:00:00+00:00", "2019-11-01T00:00:00+00:00"],
        )
        _make_leaf(
            tmp_path,
            "-311",
            {0: [10.0]},
            window="2020",
            time_range=["2020-01-01T00:00:00+00:00", "2020-06-01T00:00:00+00:00"],
        )
        word = morton_word("-311")
        result = run_sweep(str(tmp_path), [(word, "2019"), (word, "2020")], families=("overview",))
        assert result["families"]["overview"]["written"] == 3  # 2019, 2020, all
        g2019 = _overview_group(tmp_path, "-3/1", "2019.zarr", 3)
        g2020 = _overview_group(tmp_path, "-3/1", "2020.zarr", 3)
        gall = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert g2019["count"][0] == 2 and g2020["count"][0] == 1
        assert gall["count"][0] == 3  # the all-time fold accumulates windows
        assert gall["h_min"][0] == 1.0
        # Stamps: per-window overviews carry their window; the all-time fold
        # carries the reserved token and the unioned extent (D15/D23).
        stamp = read_commit(open_store(str(tmp_path / "-3" / "1" / "all.zarr")))
        assert stamp["window"] == "all"
        assert stamp["time_range"] == [
            "2019-02-01T00:00:00+00:00",
            "2020-06-01T00:00:00+00:00",
        ]
        assert read_commit(open_store(str(tmp_path / "-3" / "1" / "2019.zarr")))["window"] == "2019"

    def test_window_named_all_folds_into_the_all_time_overview(self, tmp_path, caplog):
        # Review finding, issue #201: a window labeled with the reserved token
        # resolves to the same basename AND envelope key as the all-time fold,
        # which used to overwrite it and then never regenerate it. Config
        # validation now rejects the label; a pre-guard manifest folds the
        # window into the all-time overview with a warning instead.
        _write_manifest(tmp_path, orders=(1,), windowed=True, all_time=True)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        _make_leaf(tmp_path, "-311", {0: [10.0]}, window="all")
        word = morton_word("-311")
        result = run_sweep(str(tmp_path), [(word, "2019"), (word, "all")], families=("overview",))
        assert result["families"]["overview"]["written"] == 2  # 2019 + all-time
        assert "reserved all-time token" in caplog.text
        gall = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert gall["count"][0] == 3  # both windows' leaves are folded in
        envelope = json.loads((tmp_path / "-3" / "1" / "overview.rollup.json").read_text())
        assert sorted(envelope["windows"]) == ["2019", "all"]

    def test_sibling_contribution_via_root_moc(self, tmp_path):
        # Incremental run: the second sweep names ONLY the new leaf; the
        # untouched sibling still contributes through the root coverage.moc
        # the MOC family refreshed (run record + MOC, never a LIST).
        _write_manifest(tmp_path, orders=(0,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("moc", "overview"))
        _make_leaf(tmp_path, "-312", {0: [5.0]})
        run_sweep(str(tmp_path), [(morton_word("-312"), None)], families=("moc", "overview"))
        g = _overview_group(tmp_path, "-3", "all.zarr", 2)
        np.testing.assert_array_equal(g["count"][:2], [1, 1])
        assert g["h_min"][0] == 1.0 and g["h_min"][1] == 5.0

    def test_missing_field_reads_as_fill(self, tmp_path):
        # Schema evolution: a leaf written before h_min existed contributes
        # fill for it — the walk never dies on the absent array (issue #341).
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]}, with_h_min=False)
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["written"] == 1
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert g["count"][0] == 1
        assert np.isnan(g["h_min"][0])

    def test_orphan_objects_in_leaf_are_tolerated(self, tmp_path):
        # Issue #341 Bug A: schema evolution can leave orphan array prefixes
        # inside a leaf; the fold opens arrays BY NAME (no member walk), so
        # junk prefixes and foreign objects cannot crash it.
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [2.0]})
        leaf = tmp_path / "-3" / "1" / "1" / "-311.zarr"
        orphan = leaf / str(CELL_ORDER) / "orphan_field" / "c"
        orphan.mkdir(parents=True)
        (orphan / "0").write_bytes(b"\x00garbage")
        (leaf / "stray.status").write_text("worker status debris")
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        counts = result["families"]["overview"]
        assert counts["written"] == 1 and counts["failed"] == 0

    def test_unreadable_leaf_skips_loudly_not_fatally(self, tmp_path, caplog):
        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        # A committed leaf whose inner group vanished (torn schema surgery):
        # stamp present, arrays gone -> skip + log, the node still folds.
        _make_leaf(tmp_path, "-312", {0: [9.0]})
        import shutil

        shutil.rmtree(tmp_path / "-3" / "1" / "2" / "-312.zarr" / str(CELL_ORDER))
        refs = [(morton_word("-311"), None), (morton_word("-312"), None)]
        result = run_sweep(str(tmp_path), refs, families=("overview",))
        counts = result["families"]["overview"]
        assert counts["written"] == 1 and counts["failed"] == 1
        assert "skipping unreadable leaf" in caplog.text
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert g["count"][0] == 1

    def test_summarize_declaration_warns_and_skips(self, tmp_path, caplog):
        _write_manifest(tmp_path, orders=(1,))
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["pyramid"]["overview"]["summarize"] = {"h_mean": {"as": "h_mean_digest"}}
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["written"] == 1
        assert "derived summaries" in caplog.text and "issue #265" in caplog.text
        g = _overview_group(tmp_path, "-3/1", "all.zarr", 3)
        assert "h_mean_digest" not in g

    def test_sweep_populates_manifest_materialized(self, tmp_path):
        from zagg.hive import read_manifest

        _write_manifest(tmp_path, orders=(1, 0))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["manifest_updated"] is True
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block["materialized"]["orders"] == [0, 1]
        assert block["materialized"]["generated_at"]
        # Per-level fold provenance (issue #376): the finest level is exact,
        # the one below it cascaded from it.
        assert block["materialized"]["fold_sources"] == {"1": "leaves", "0": "cascade"}
        # The declaration itself is untouched (the sweep populates, never
        # rewrites — D11/D22 write-once discipline).
        assert block["orders"] == [1, 0] and block["fields"] == FIELDS_DECL

    def test_manifest_update_keeps_frozen_keys_resumable(self, tmp_path):
        # The pyramid block is excluded from the frozen resume keys, so the
        # sweep's materialized update can never brick an append precheck.
        from zagg.hive import _frozen_matches, read_manifest

        before = _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        after = read_manifest(str(tmp_path))
        assert after["pyramid"] != before["pyramid"]
        assert _frozen_matches(after, before)

    def test_coarser_than_shard_order_accumulates(self, tmp_path):
        # Overview cells COARSER than the shard order (deep schedule, shallow
        # depth): whole shards fold into one overview cell, accumulated
        # across leaves.
        _write_manifest(tmp_path, orders=(0,), shard_order=3)
        _make_leaf(tmp_path, "-3111", {0: [1.0]}, shard_order=3)
        _make_leaf(tmp_path, "-3112", {1: [4.0, 5.0]}, shard_order=3)
        refs = [(morton_word("-3111"), None), (morton_word("-3112"), None)]
        result = run_sweep(str(tmp_path), refs, families=("overview",))
        assert result["families"]["overview"]["written"] == 1
        # node -3, overview cells at order 1 (= 4 - 3 + 0): both shards live
        # under -31 -> row 0 accumulates 1 + 2 observations.
        g = _overview_group(tmp_path, "-3", "all.zarr", 1)
        np.testing.assert_array_equal(g["count"][:], [3, 0, 0, 0])
        assert g["h_min"][0] == 1.0


#: The composable subset of the declaration — what the sweep hands the folds.
COMPOSABLE = {n: m for n, m in FIELDS_DECL.items() if m["class"] != "none"}


class TestCascadeFold:
    """Fold-of-folds: coarse levels fold the level below them (issue #376)."""

    def _leaf_orders(self, monkeypatch):
        """Record which declared orders read the raw leaves (``_fold_node``)."""
        import zagg.sweep_overview as so

        seen = []
        real = so._fold_node

        def spy(store_root, node, k, *args, **kwargs):
            seen.append(k)
            return real(store_root, node, k, *args, **kwargs)

        monkeypatch.setattr(so, "_fold_node", spy)
        return seen

    def _two_leaves(self, root):
        _make_leaf(root, "-311", {0: [1.0, 2.0], 5: [10.0]})
        _make_leaf(root, "-312", {0: [3.0]})
        return [(morton_word("-311"), None), (morton_word("-312"), None)]

    def _entry(self, root, node_rel):
        envelope = json.loads((root / node_rel / "overview.rollup.json").read_text())
        return envelope["windows"]["all"]

    def test_coarse_level_folds_from_the_finer_overview(self, tmp_path, monkeypatch):
        from zagg.stats.tdigest import quantile_from_tdigest

        seen = self._leaf_orders(monkeypatch)
        _write_manifest(tmp_path, orders=(1, 0))  # cascade is the default
        refs = self._two_leaves(tmp_path)
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 2 and counts["failed"] == 0
        # The headline property: the leaves are read ONCE for the whole
        # pyramid, by the finest declared level only.
        assert set(seen) == {1}
        entry = self._entry(tmp_path, "-3")
        assert entry["fold_source"] == "cascade" and entry["fold_from_order"] == 1
        assert self._entry(tmp_path, "-3/1")["fold_source"] == "leaves"
        # Same answer as the leaves fold for the exact class, and the digest
        # of the whole -311 leaf still carries all three observations.
        g0 = _overview_group(tmp_path, "-3", "all.zarr", 2)
        np.testing.assert_array_equal(g0["count"][:2], [3, 1])
        assert g0["h_min"][0] == 1.0 and g0["h_min"][1] == 3.0
        merged = decode_digest(bytes(g0["h_tdigest"][:][0]), "float32")
        assert merged[:, 1].sum() == 3
        assert np.isclose(quantile_from_tdigest(merged, 0.5), 2.0, atol=1.0)

    def test_exact_fields_are_byte_equal_under_either_source(self, tmp_path):
        # The exact merge laws are associative, so folding folds is the same
        # arithmetic: only the approximate class pays for the cascade.
        slabs = {}
        for source in ("cascade", "leaves"):
            root = tmp_path / source
            root.mkdir()
            _write_manifest(root, orders=(1, 0), fold_source=source)
            refs = self._two_leaves(root)
            run_sweep(str(root), refs, families=("overview",))
            group = _overview_group(root, "-3", "all.zarr", 2)
            slabs[source] = (group["count"][:], group["h_min"][:])
        np.testing.assert_array_equal(slabs["cascade"][0], slabs["leaves"][0])
        np.testing.assert_array_equal(slabs["cascade"][1], slabs["leaves"][1])

    def test_leaves_source_reads_the_leaves_at_every_level(self, tmp_path, monkeypatch):
        seen = self._leaf_orders(monkeypatch)
        _write_manifest(tmp_path, orders=(1, 0), fold_source="leaves")
        refs = self._two_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        assert sorted(seen) == [0, 1]
        entry = self._entry(tmp_path, "-3")
        assert entry["fold_source"] == "leaves" and "fold_from_order" not in entry

    def test_exact_levels_two_keeps_the_second_level_on_the_leaves(self, tmp_path, monkeypatch):
        seen = self._leaf_orders(monkeypatch)
        _write_manifest(tmp_path, orders=(1, 0), exact_levels=2)
        run_sweep(str(tmp_path), self._two_leaves(tmp_path), families=("overview",))
        assert sorted(seen) == [0, 1]
        assert self._entry(tmp_path, "-3")["fold_source"] == "leaves"

    def test_switching_the_source_regenerates_the_coarse_overview(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0), fold_source="leaves")
        refs = self._two_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        _write_manifest(tmp_path, orders=(1, 0))  # same leaves, now cascading
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        # The exact slabs are byte-equal under either source, so only the
        # recorded regime can tell the stored overview is the other fold.
        assert counts["written"] == 1 and counts["current"] == 1
        assert self._entry(tmp_path, "-3")["fold_source"] == "cascade"

    def test_dropping_an_intermediate_order_regenerates_the_coarse_overview(self, tmp_path):
        # Exact-only fields, so the coarse slabs — and therefore the content
        # hash — are byte-equal whichever level they cascaded from, and the
        # summed n_leaves is the same either way: only fold_from_order moves.
        exact = {n: m for n, m in FIELDS_DECL.items() if m["class"] == "exact"}
        leaves = {"-3111": {0: [1.0, 2.0]}, "-3231": {5: [7.0]}}
        geometry = dict(shard_order=3, cell_order=6, fields=exact)
        _write_manifest(tmp_path, orders=(2, 1, 0), **geometry)
        for decimal, cells in leaves.items():
            _make_leaf(tmp_path, decimal, cells, shard_order=3, cell_order=6)
        refs = [(morton_word(d), None) for d in leaves]
        run_sweep(str(tmp_path), refs, families=("overview",))
        assert self._entry(tmp_path, "-3")["fold_from_order"] == 1
        # Re-declare without the intermediate level: order 0 now folds from
        # order 2, so the stored overview names a level that is gone.
        _write_manifest(tmp_path, orders=(2, 0), **geometry)
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 1 and counts["current"] == 2
        assert self._entry(tmp_path, "-3")["fold_from_order"] == 2
        assert (
            _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]["fold_from_order"] == 2
        )

    def test_pre_376_envelope_entry_reads_as_the_leaves_fold(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0), fold_source="leaves")
        refs = self._two_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        for node_rel in ("-3", "-3/1"):
            path = tmp_path / node_rel / "overview.rollup.json"
            envelope = json.loads(path.read_text())
            envelope["windows"]["all"].pop("fold_source")
            path.write_text(json.dumps(envelope))
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 0 and counts["current"] == 2

    def test_windowed_levels_cascade_per_window(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0), windowed=True, all_time=True)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        _make_leaf(tmp_path, "-311", {0: [10.0]}, window="2020")
        word = morton_word("-311")
        refs = [(word, "2019"), (word, "2020")]
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 6 and counts["failed"] == 0  # 3 windows x 2 orders
        # Each coarse window cascades from the SAME window's child overview
        # (D23 naming), and the all-time one from the child's all.zarr.
        assert _overview_group(tmp_path, "-3", "2019.zarr", 2)["count"][0] == 2
        assert _overview_group(tmp_path, "-3", "2020.zarr", 2)["count"][0] == 1
        assert _overview_group(tmp_path, "-3", "all.zarr", 2)["count"][0] == 3
        envelope = json.loads((tmp_path / "-3" / "overview.rollup.json").read_text())
        assert {e["fold_source"] for e in envelope["windows"].values()} == {"cascade"}

    def test_plan_names_the_source_of_every_level(self, caplog):
        from zagg.sweep_overview import _fold_sources

        # Depth 2 (cell_order 8 over shard_order 6): a 2-order gap is the
        # widest a 4^2-cell node slab can address.
        assert _fold_sources({}, [4, 2, 0], 8, 6) == {
            4: ("leaves", None),
            2: ("cascade", 4),
            0: ("cascade", 2),
        }
        assert _fold_sources({"exact_levels": 2}, [4, 2, 0], 8, 6)[2] == ("leaves", None)
        assert _fold_sources({"fold_source": "leaves"}, [4, 2, 0], 8, 6) == {
            4: ("leaves", None),
            2: ("leaves", None),
            0: ("leaves", None),
        }
        wide = _fold_sources({}, [5, 0], 8, 6)
        assert wide[0] == ("leaves", None) and "wider than the 2-order node slab" in caplog.text

    def test_a_boundary_past_the_last_level_folds_everything_from_the_leaves_loudly(self, caplog):
        from zagg.sweep_overview import _fold_sources

        # The manifest reaches the sweep hand-edited too, so the check is
        # mirrored here: a cascade declaration whose boundary covers every
        # level IS the deprecated regime, and must not be silent about it.
        assert _fold_sources({"exact_levels": 3}, [4, 2, 0], 8, 6) == {
            4: ("leaves", None),
            2: ("leaves", None),
            0: ("leaves", None),
        }
        assert "covers all 3 declared levels" in caplog.text and "DEPRECATED" in caplog.text

    def test_unusable_fold_declaration_falls_back_loudly(self, caplog):
        from zagg.sweep_overview import _fold_sources

        decl = {"fold_source": "sideways", "exact_levels": "two"}
        assert _fold_sources(decl, [4, 2], 8, 6) == {4: ("leaves", None), 2: ("cascade", 4)}
        assert "fold_source 'sideways' is unknown" in caplog.text
        assert "exact_levels 'two' is unusable" in caplog.text

    def test_child_that_is_not_an_overview_is_skipped_loudly(self, tmp_path, caplog):
        from zagg.sweep_overview import _cascade_node

        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        # A stamped object at a node position that does not classify as an
        # overview at the source order is not this fold's input (D11: role is
        # never inferred from tree position).
        store = open_store(str(tmp_path / "-3" / "1" / "all.zarr"))
        zarr.open_group(store, mode="r+", zarr_format=3).attrs[ROLE_ATTR] = "something-else"
        counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
        fold = _cascade_node(
            str(tmp_path),
            "-3",
            0,
            1,
            "all",
            ["-311"],
            COMPOSABLE,
            CELL_ORDER,
            SHARD_ORDER,
            counts,
            {},
        )
        assert fold is None and counts["failed"] == 1
        assert "is not an overview at order 1" in caplog.text

    def test_cascade_without_a_materialized_child_is_empty(self, tmp_path, caplog):
        import logging

        from zagg.sweep_overview import _cascade_node

        caplog.set_level(logging.INFO)
        _write_manifest(tmp_path, orders=(1, 0))
        _make_leaf(tmp_path, "-311", {0: [1.0]})  # leaves only: no child overview yet
        counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
        fold = _cascade_node(
            str(tmp_path),
            "-3",
            0,
            1,
            "all",
            ["-311"],
            COMPOSABLE,
            CELL_ORDER,
            SHARD_ORDER,
            counts,
            {},
        )
        assert fold is None and counts["failed"] == 0
        # An unmaterialized child is not an error, but it IS under-coverage:
        # the cascade folds what is on disk, so it is recorded.
        assert "1 of 1 candidate child overviews at order 1 are not usable" in caplog.text
        assert "(1 not materialized, 0 unreadable)" in caplog.text

    def _sibling_leaves(self, root):
        """Two leaves in DIFFERENT order-1 subtrees, both in the coverage MOC."""
        _make_leaf(root, "-311", {0: [1.0, 2.0]})
        _make_leaf(root, "-321", {0: [7.0]})
        refs = [(morton_word(d), None) for d in ("-311", "-321")]
        run_sweep(str(root), refs, families=("moc",))
        return refs

    def test_a_partially_swept_store_self_heals_on_the_next_sweep(self, tmp_path):
        # The end-to-end shape of the PR's "a cascade folds what is on disk":
        # sweep A only, then sweep B, through run_sweep both times.
        _write_manifest(tmp_path, orders=(1, 0))
        refs = self._sibling_leaves(tmp_path)
        run_sweep(str(tmp_path), refs[:1], families=("overview",))
        # -32 has a committed leaf but no overview yet, so its span in the
        # order-0 parent is fill — indistinguishable from empty without the
        # recorded coverage.
        coarse = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert coarse["source_children"] == {"folded": 1, "missing": 1, "unreadable": 0}
        assert coarse["generation"]["n_leaves"] == 1
        counts0 = _overview_group(tmp_path, "-3", "all.zarr", 2)["count"][:]
        assert counts0[0] == 2 and counts0[4] == 0  # -311's span; -321's still fill
        # Sweep B: the parent must be REWRITTEN, not read as current — the
        # repair rests on its summed n_leaves moving once the child exists.
        counts = run_sweep(str(tmp_path), refs[1:], families=("overview",))["families"]["overview"]
        assert counts["written"] == 2 and counts["current"] == 0
        coarse = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert coarse["source_children"] == {"folded": 2, "missing": 0, "unreadable": 0}
        assert coarse["generation"]["n_leaves"] == 2
        counts0 = _overview_group(tmp_path, "-3", "all.zarr", 2)["count"][:]
        assert counts0[0] == 2 and counts0[4] == 1  # A unchanged, B now populated
        assert counts0.sum() == 3

    def test_an_unusable_child_still_leaves_the_parent_covering_the_rest(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0))
        refs = self._sibling_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        # Break ONE child's role attr. Its own level reads as current (the
        # envelope and the D4 stamp are untouched), so it is not regenerated
        # and the order-0 cascade meets a child it must skip.
        store = open_store(str(tmp_path / "-3" / "2" / "all.zarr"))
        zarr.open_group(store, mode="r+", zarr_format=3).attrs[ROLE_ATTR] = "something-else"
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 1 and counts["current"] == 2 and counts["failed"] == 1
        # The parent is still written, covering the child it COULD read, and
        # records that it under-covers the other.
        coarse = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert coarse["source_children"] == {"folded": 1, "missing": 0, "unreadable": 1}
        counts0 = _overview_group(tmp_path, "-3", "all.zarr", 2)["count"][:]
        assert counts0[0] == 2 and counts0[4] == 0

    def test_under_coverage_is_recorded_in_the_overview_attrs(self, tmp_path, caplog):
        import logging

        caplog.set_level(logging.INFO)
        _write_manifest(tmp_path, orders=(1, 0))
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]})
        _make_leaf(tmp_path, "-321", {0: [7.0]})  # committed, but not swept yet
        refs = [(morton_word(d), None) for d in ("-311", "-321")]
        run_sweep(str(tmp_path), refs, families=("moc",))  # both leaves in the coverage MOC
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        # -32 is a candidate child of -3 (its leaf is in the coverage MOC) with
        # no overview on disk, so the order-0 cascade under-covers its subtree
        # — and says so IN THE ARTIFACT, not only in this process's log.
        info = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert info["source_children"] == {"folded": 1, "missing": 1, "unreadable": 0}
        assert "1 of 2 candidate child overviews at order 1 are not usable" in caplog.text
        # The exact-from-leaves level carries no such key: fill there means
        # empty, full stop.
        assert (
            "source_children"
            not in _overview_root(tmp_path, "-3/1", "all.zarr").attrs[OVERVIEW_ATTR]
        )

    def test_an_unreadable_child_is_counted_in_both_terms(self, tmp_path, caplog):
        import logging

        from zagg.sweep_overview import _cascade_node

        caplog.set_level(logging.INFO)
        _write_manifest(tmp_path, orders=(1, 0))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        _make_leaf(tmp_path, "-321", {0: [7.0]})
        refs = [(morton_word(d), None) for d in ("-311", "-321")]
        run_sweep(str(tmp_path), refs, families=("overview",))
        # One child unreadable as an overview, one never materialized: both
        # leave their span at fill, so both are under-coverage and both have
        # to reach the count (the denominator is the candidate children).
        store = open_store(str(tmp_path / "-3" / "2" / "all.zarr"))
        zarr.open_group(store, mode="r+", zarr_format=3).attrs[ROLE_ATTR] = "something-else"
        counts = {"written": 0, "current": 0, "empty": 0, "failed": 0}
        fold = _cascade_node(
            str(tmp_path),
            "-3",
            0,
            1,
            "all",
            ["-311", "-321", "-331"],
            COMPOSABLE,
            CELL_ORDER,
            SHARD_ORDER,
            counts,
            {},
        )
        assert fold["source_children"] == {"folded": 1, "missing": 1, "unreadable": 1}
        assert "2 of 3 candidate child overviews at order 1 are not usable" in caplog.text
        assert "(1 not materialized, 1 unreadable)" in caplog.text

    def test_two_order_gap_places_each_of_the_16_children(self, tmp_path):
        # The DEFAULT schedule is every 2 orders, so the ordinary cascade gap
        # is 2: 16 children, each owning a 1/16 span of the parent slab. A
        # geometry one order deeper than the module default (shard 3, cell 6)
        # is the smallest one that can hold it.
        _write_manifest(tmp_path, orders=(2, 0), shard_order=3, cell_order=6)
        leaves = {"-3111": {0: [1.0, 2.0]}, "-3231": {5: [7.0], 9: [8.0, 9.0]}}
        for decimal, cells in leaves.items():
            _make_leaf(tmp_path, decimal, cells, shard_order=3, cell_order=6)
        refs = [(morton_word(d), None) for d in leaves]
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 3 and counts["failed"] == 0  # 2 nodes + the root
        assert self._entry(tmp_path, "-3")["fold_from_order"] == 2
        # The order-0 overview holds the 64 order-3 cells of -3: one per leaf
        # shard, so each leaf's whole count lands in its own cell — cell 0 for
        # -3111 and cell 24 for -3231 (digits 2,3,1 in base 4).
        counts0 = _overview_group(tmp_path, "-3", "all.zarr", 3)["count"][:]
        assert counts0[0] == 2 and counts0[24] == 3
        assert counts0.sum() == 5

    def test_every_level_records_its_fold_in_its_attrs(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0))
        run_sweep(str(tmp_path), self._two_leaves(tmp_path), families=("overview",))
        fine = _overview_root(tmp_path, "-3/1", "all.zarr").attrs[OVERVIEW_ATTR]
        coarse = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert fine["fold_source"] == "leaves" and "fold_from_order" not in fine
        assert coarse["fold_source"] == "cascade" and coarse["fold_from_order"] == 1

    def test_materialized_fold_sources_carry_forward(self, tmp_path):
        from zagg.hive import read_manifest

        _write_manifest(tmp_path, orders=(1, 0), fold_source="leaves")
        refs = self._two_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        # Narrow the declaration to the finest level only, in place — the
        # sweep's own actuals must survive an edit of the declaration.
        manifest = read_manifest(str(tmp_path))
        manifest["pyramid"]["overview"]["orders"] = [1]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0, 4.0], 5: [10.0]})  # a changed leaf
        run_sweep(str(tmp_path), refs, families=("overview",))
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]["materialized"]
        # Order 0 was not swept this pass: its recorded regime survives, so
        # the manifest still describes what is actually on disk.
        assert block["orders"] == [0, 1]
        assert block["fold_sources"] == {"1": "leaves", "0": "leaves"}

    def test_materialized_fold_sources_record_only_a_level_that_wrote(self, tmp_path, monkeypatch):
        import zagg.sweep_overview as so
        from zagg.hive import read_manifest

        def block(name):
            return read_manifest(str(tmp_path))["pyramid"]["overview"]["materialized"][name]

        _write_manifest(tmp_path, orders=(1, 0), fold_source="leaves")
        refs = self._two_leaves(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        assert block("fold_sources") == {"1": "leaves", "0": "leaves"}

        def boom(*args, **kwargs):
            raise RuntimeError("cascade unavailable")

        # Now cascade, but the order-0 fold fails: order 0 keeps the entries —
        # and the artifacts — an earlier sweep left, so nothing at that level
        # was written this pass. The declaration is edited in place so the
        # sweep's own prior actuals are still there to be contradicted.
        manifest = read_manifest(str(tmp_path))
        manifest["pyramid"]["overview"].pop("fold_source")  # back to the cascade default
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        monkeypatch.setattr(so, "_cascade_node", boom)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0, 4.0], 5: [10.0]})  # a changed leaf
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 1 and counts["failed"] == 1
        # An actual, not the plan (§4.5): order 0 still carries the leaves fold
        # that is really on disk, and the order stays materialized.
        assert block("orders") == [0, 1]
        assert block("fold_sources") == {"1": "leaves", "0": "leaves"}
        assert (
            _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]["fold_source"]
            == "leaves"
        )

    def test_cascade_generation_stamp_counts_the_underlying_leaves(self, tmp_path):
        _write_manifest(tmp_path, orders=(1, 0))
        run_sweep(str(tmp_path), self._two_leaves(tmp_path), families=("overview",))
        # Both leaves sit under -31, so the order-0 node inherits their count
        # through the child's own generation stamp — staleness stays keyed to
        # leaf commits, not to overview writes.
        info = _overview_root(tmp_path, "-3", "all.zarr").attrs[OVERVIEW_ATTR]
        assert info["generation"]["n_leaves"] == 2
        stamp = read_commit(open_store(str(tmp_path / "-3" / "all.zarr")))
        assert stamp is not None and stamp["granule_count"] == 2


class TestOverviewContentHashes:
    """Issue #342 phase 4: overview leaves get §5 ``content_hashes`` sidecars,
    computed from the folded arrays in memory; the envelope's sweep-internal
    skip digest is a different recipe and stays untouched."""

    def test_sidecar_records_section5_hashes_with_parity(self, tmp_path):
        from zagg.content_hash import combined_hash, hash_arrays
        from zagg.telemetry import SPEC_V3, read_sidecar

        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0], 5: [10.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        leaf = str(tmp_path / "-3" / "1" / "all.zarr")
        record = read_sidecar(leaf, spec=SPEC_V3)
        assert record is not None and record["window"] is None
        # Write-read parity, overview edition: the staged-source hashes must
        # equal a full read-back of the written overview zarr.
        group = zarr.open_group(open_store(leaf), mode="r", zarr_format=3)
        read_back = hash_arrays(group)
        assert record["content_hashes"]["arrays"] == read_back
        assert record["content_hashes"]["combined"] == combined_hash(read_back)
        # Discovery-based scope: morton + every composable field, none extra.
        assert set(read_back) == {"3/morton", "3/count", "3/h_min", "3/h_tdigest"}

    def test_stamp_skip_digest_stays_separate_and_untouched(self, tmp_path):
        from zagg.telemetry import SPEC_V3, read_sidecar

        _write_manifest(tmp_path, orders=(1,))
        _make_leaf(tmp_path, "-311", {0: [1.0]})
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        envelope = json.loads((tmp_path / "-3" / "1" / "overview.rollup.json").read_text())
        entry = envelope["windows"]["all"]
        root = _overview_root(tmp_path, "-3/1", "all.zarr")
        # The sweep-internal skip digest still rides envelope + attrs...
        assert entry["content_hash"] == root.attrs[OVERVIEW_ATTR]["content_hash"]
        # ...and is NOT the §5 combined hash (different recipe, different job).
        record = read_sidecar(str(tmp_path / "-3" / "1" / "all.zarr"), spec=SPEC_V3)
        assert entry["content_hash"] != record["content_hashes"]["combined"]

    def test_windowed_overviews_get_per_window_sidecars(self, tmp_path):
        from zagg.telemetry import SPEC_V3, read_sidecar

        _write_manifest(tmp_path, orders=(1,), windowed=True, all_time=True)
        _make_leaf(tmp_path, "-311", {0: [1.0, 2.0]}, window="2019")
        _make_leaf(tmp_path, "-311", {0: [10.0]}, window="2020")
        word = morton_word("-311")
        run_sweep(str(tmp_path), [(word, "2019"), (word, "2020")], families=("overview",))
        node = tmp_path / "-3" / "1"
        # D23 window-only naming keys one sidecar per window basename — the
        # legacy grammar would have keyed them all to one stats.json.
        assert (node / "2019.stats.json").exists()
        assert (node / "2020.stats.json").exists()
        assert (node / "all.stats.json").exists()
        r2019 = read_sidecar(str(node / "2019.zarr"), spec=SPEC_V3)
        rall = read_sidecar(str(node / "all.zarr"), spec=SPEC_V3)
        assert r2019["window"] == "2019"
        assert rall["window"] == "all"
        assert r2019["content_hashes"]["combined"] != rall["content_hashes"]["combined"]


class TestSection83Obligations:
    """The standing D22 test claims for the overview family (§8.3)."""

    #: Two shards' observations, keyed (leaf decimal, leaf row). Exact binary
    #: floats so exact-class equality asserts are byte-for-byte.
    OBS = {
        ("-311", 0): [1.5, 2.5, -3.0],
        ("-311", 3): [8.0],
        ("-311", 5): [0.25, 0.75],
        ("-311", 15): [4.0, 6.0],
        ("-312", 0): [3.0, -1.0],
        ("-312", 9): [2.0],
    }

    def _populate(self, root, orders=(1, 0)):
        _write_manifest(root, orders=orders)
        cells: dict = {}
        for (dec, row), obs in self.OBS.items():
            cells.setdefault(dec, {})[row] = obs
        for dec, rows in cells.items():
            _make_leaf(root, dec, rows)
        return [(morton_word(d), None) for d in sorted(cells)]

    def _direct(self, k):
        """Direct aggregation at overview order ``k``: pool raw observations
        per overview cell (the oracle the fold must match)."""
        span = 4 ** (CELL_ORDER - SHARD_ORDER - (SHARD_ORDER - k))
        pooled: dict[int, list] = {}
        for (dec, row), obs in self.OBS.items():
            shard_rank = {"-311": 0, "-312": 1}[dec]
            cell = shard_rank * span + row // 4 ** (SHARD_ORDER - k)
            pooled.setdefault(cell, []).extend(obs)
        return pooled

    @pytest.mark.parametrize("k", [1, 0])
    def test_rollup_equals_direct_at_every_declared_order(self, tmp_path, k):
        from zagg.stats.tdigest import build_tdigest, quantile_from_tdigest

        refs = self._populate(tmp_path)
        run_sweep(str(tmp_path), refs, families=("overview",))
        rel = {1: "-3/1", 0: "-3"}[k]
        g = _overview_group(tmp_path, rel, "all.zarr", CELL_ORDER - (SHARD_ORDER - k))
        count, h_min = g["count"][:], g["h_min"][:]
        digests = g["h_tdigest"][:]
        direct = self._direct(k)
        for cell in range(len(count)):
            if cell not in direct:
                assert count[cell] == 0 and np.isnan(h_min[cell])
                assert len(bytes(digests[cell])) == 0
                continue
            obs = np.asarray(direct[cell], dtype=np.float64)
            # Exact class: byte-equal to the direct aggregation (§8.3).
            assert count[cell] == len(obs)
            assert h_min[cell] == np.float32(obs.min())
            # Approximate class: np.isclose against the direct digest (D24).
            merged = decode_digest(bytes(digests[cell]), "float32")
            oracle = build_tdigest(obs, delta=64)
            for q in (0.25, 0.5, 0.75):
                assert np.isclose(
                    quantile_from_tdigest(merged, q),
                    quantile_from_tdigest(oracle, q),
                    rtol=0.05,
                    atol=0.25,
                )

    @pytest.mark.parametrize("k", [1, 0])
    def test_morton_labels_address_the_folded_cells(self, tmp_path, k):
        # Review finding, issue #201: the writer places slabs by base-4 rank
        # arithmetic but LABELS them with generate_morton_children(), two
        # orderings that must agree — and `_direct()` above cannot catch a
        # disagreement because it recomputes the writer's own formula. Resolve
        # every slot from the overview's OWN morton value instead: a leaf cell
        # belongs to the overview cell its morton decimal PREFIXES.
        from mortie import generate_morton_children

        from zagg.grids.morton import morton_decimal

        refs = self._populate(tmp_path, orders=(k,))
        run_sweep(str(tmp_path), refs, families=("overview",))
        target_order = CELL_ORDER - (SHARD_ORDER - k)
        node = {1: "-31", 0: "-3"}[k]
        base = node[: len(node) - k]
        g = _overview_group(tmp_path, {1: "-3/1", 0: "-3"}[k], "all.zarr", target_order)

        pooled: dict[str, list] = {}
        for (dec, row), obs in self.OBS.items():
            word = generate_morton_children(morton_word(dec), CELL_ORDER)[row]
            cell = morton_decimal(int(word))  # the order-4 leaf cell's id
            pooled.setdefault(cell[: len(base) + target_order], []).extend(obs)
        assert pooled  # every OBS cell resolved to an overview cell

        morton, count, h_min = g["morton"][:], g["count"][:], g["h_min"][:]
        assert len(set(morton.tolist())) == len(morton)  # labels are distinct
        seen = set()
        for j, word in enumerate(morton.tolist()):
            label = morton_decimal(int(word))
            assert label.startswith(node) and len(label) == len(base) + target_order
            if label in pooled:
                obs = np.asarray(pooled[label], dtype=np.float64)
                assert count[j] == len(obs)
                assert h_min[j] == np.float32(obs.min())
                seen.add(label)
            else:  # unclaimed slots stay at the fill
                assert count[j] == 0 and np.isnan(h_min[j])
        assert seen == set(pooled)  # no populated cell went unlabelled

    def test_second_pass_over_unchanged_tree_writes_nothing(self, tmp_path):
        refs = self._populate(tmp_path)
        first = run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        assert first["families"]["overview"]["written"] == 2
        env_before = (tmp_path / "-3" / "1" / "overview.rollup.json").read_text()
        second = run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        counts = second["families"]["overview"]
        assert counts["written"] == 0 and counts["current"] == 2
        assert "manifest_updated" not in counts  # no write, no manifest touch
        assert (tmp_path / "-3" / "1" / "overview.rollup.json").read_text() == env_before

    def test_same_second_leaf_rerun_rewrites_via_content_hash(self, tmp_path, monkeypatch):
        # Leaf stamps resolve to whole seconds, so a back-to-back re-run
        # carries an UNCHANGED generation stamp; freeze the clock to force the
        # collision and prove the content hash is the backstop that rewrites.
        import zagg.hive as hive_mod

        monkeypatch.setattr(hive_mod, "_utcnow", lambda: "2026-05-01T12:00:00+00:00")
        refs = self._populate(tmp_path, orders=(0,))
        run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        stale = json.loads((tmp_path / "-3" / "overview.rollup.json").read_text())
        # Same wall-clock second, different content: leaf -311 changes.
        _make_leaf(tmp_path, "-311", {0: [100.0]})
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["written"] == 1
        fresh = json.loads((tmp_path / "-3" / "overview.rollup.json").read_text())
        assert fresh["windows"]["all"]["generation"] == stale["windows"]["all"]["generation"]
        assert fresh["windows"]["all"]["content_hash"] != stale["windows"]["all"]["content_hash"]
        g = _overview_group(tmp_path, "-3", "all.zarr", 2)
        # The re-run leaf's new fold, with the untouched sibling retained
        # (candidates come from the root MOC, not just the dirty set).
        assert g["count"][0] == 1 and g["count"][1] == 3
        assert g["h_min"][0] == 100.0

    def test_deleting_every_overview_leaves_leaf_reads_green(self, tmp_path):
        import shutil

        from zagg.hive import read_commit as read_stamp

        refs = self._populate(tmp_path)
        run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        hashes = {
            p: json.loads(p.read_text())["windows"]["all"]["content_hash"]
            for p in tmp_path.rglob("overview.rollup.json")
        }
        assert hashes
        for p in tmp_path.rglob("all.zarr"):
            shutil.rmtree(p)
        for p in list(tmp_path.rglob("overview.rollup.json")):
            p.unlink()
        # Leaf truth reads back untouched (nothing-load-bearing, D9)...
        for dec in ("-311", "-312"):
            leaf = open_store(shard_leaf_path(str(tmp_path), morton_word(dec)))
            assert read_stamp(leaf) is not None
            g = zarr.open_group(leaf, path=str(CELL_ORDER), mode="r", zarr_format=3)
            assert g["count"][0] >= 1
        # ...and one sweep regenerates byte-identical folds (same hashes).
        run_sweep(str(tmp_path), refs, families=("overview",))
        for p, digest in hashes.items():
            assert json.loads(p.read_text())["windows"]["all"]["content_hash"] == digest

    def test_deleting_only_the_zarrs_regenerates_them(self, tmp_path):
        # Review finding, issue #201: the envelope and the zarr are two objects,
        # so skip-if-current must probe the ARTIFACT — deleting only the zarrs
        # used to read back as `current` with nothing on disk (no D9 self-heal).
        import shutil

        refs = self._populate(tmp_path)
        run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        hashes = {
            p: json.loads(p.read_text())["windows"]["all"]["content_hash"]
            for p in tmp_path.rglob("overview.rollup.json")
        }
        zarrs = sorted(tmp_path.rglob("all.zarr"))
        assert len(zarrs) == 2 and len(hashes) == 2
        for p in zarrs:
            shutil.rmtree(p)
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 2 and counts["current"] == 0
        for p in zarrs:
            assert read_commit(open_store(str(p))) is not None
        # ...and the regenerated folds are byte-identical (same content hashes).
        for p, digest in hashes.items():
            assert json.loads(p.read_text())["windows"]["all"]["content_hash"] == digest

    def test_torn_overview_write_is_not_current(self, tmp_path):
        # The same probe covers a torn prior write: the zarr is there but the
        # commit stamp never landed, so the entry must not read as `current`.
        refs = self._populate(tmp_path, orders=(0,))
        run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        (tmp_path / "-3" / "all.zarr" / "zarr.json").unlink()
        counts = run_sweep(str(tmp_path), refs, families=("overview",))["families"]["overview"]
        assert counts["written"] == 1 and counts["current"] == 0
        assert read_commit(open_store(str(tmp_path / "-3" / "all.zarr"))) is not None

    def test_repair_walk_ignores_overviews(self, tmp_path):
        # Review finding, issue #201: overview basenames collide with the leaf
        # grammar, so the D9 repair walk must classify them out by the D11 role
        # attr. `all.zarr` used to die outright on a shard_order-2 store
        # (_decimal_order("all") == 2 -> "malformed decimal Morton id 'all'").
        from zagg.coverage import refresh_root_coverage
        from zagg.grids.morton import morton_decimal
        from zagg.hive import root_coverage_words

        refs = self._populate(tmp_path, orders=(1, 0))
        run_sweep(str(tmp_path), refs, families=("moc", "overview"))
        assert sorted(p.name for p in tmp_path.rglob("all.zarr")) == ["all.zarr"] * 2
        env = refresh_root_coverage(str(tmp_path))
        assert [morton_decimal(int(w)) for w in root_coverage_words(env)] == ["-311", "-312"]

    def test_repair_walk_ignores_digit_labeled_overviews(self, tmp_path):
        # The windowed half of the same finding: a window label made of [1-4]
        # digits at length shard_order+1 parses as a VALID decimal, so the
        # rebuilt root MOC used to gain a phantom positive-base shard ("2411")
        # that then poisons every root-MOC consumer.
        from zagg.coverage import refresh_root_coverage
        from zagg.grids.morton import morton_decimal
        from zagg.hive import root_coverage_words

        _write_manifest(tmp_path, orders=(1,), windowed=True, shard_order=3)
        _make_leaf(tmp_path, "-3111", {0: [1.0]}, window="2411", shard_order=3)
        run_sweep(str(tmp_path), [(morton_word("-3111"), "2411")], families=("moc", "overview"))
        assert (tmp_path / "-3" / "1" / "2411.zarr").is_dir()
        env = refresh_root_coverage(str(tmp_path))
        assert [morton_decimal(int(w)) for w in root_coverage_words(env)] == ["-3111"]

    def test_role_never_inferred_from_position(self, tmp_path):
        # D11/D24: an overview is classified by its role attr, never by tree
        # depth — the leaf carries NO role, the overview always does.
        refs = self._populate(tmp_path, orders=(1,))
        run_sweep(str(tmp_path), refs, families=("overview",))
        leaf = zarr.open_group(
            open_store(shard_leaf_path(str(tmp_path), morton_word("-311"))), mode="r"
        )
        assert ROLE_ATTR not in leaf.attrs
        assert _overview_root(tmp_path, "-3/1", "all.zarr").attrs[ROLE_ATTR] == "overview"


def _run_record(root, decimals, run_id="r1", window=None):
    """One run-record parquet naming ``decimals`` as completed shards (D20/D22)."""
    from zagg.telemetry import build_record, flatten_record, write_run_parquet

    rows = [
        flatten_record(
            build_record(
                shard_key=morton_word(d),
                metadata={"total_obs": 1, "duration_s": 1.0},
                window=window,
            )
        )
        for d in decimals
    ]
    write_run_parquet(str(root), rows, run_id=run_id)


class TestDeclarePyramid:
    """The issue #358 retrofit: install/update the declaration on an existing store."""

    #: Per-leaf observations the retrofit fixtures write.
    CELLS = {"-311": {0: [1.0, 2.0]}, "-312": {0: [3.0]}}

    def _pre_declaration_store(self, root, decimals=("-311",), semantic_config=None):
        """A pre-#344-style store: manifest WITHOUT a pyramid key + committed leaves.

        ``semantic_config`` stamps the D19 frozen ``semantic_hash`` the config
        guard compares against; omitted leaves the pre-#299 (hash-less) shape.
        """
        _write_manifest(root)
        manifest = json.loads((root / MANIFEST_NAME).read_text())
        del manifest["pyramid"]
        if semantic_config is not None:
            from zagg.semantics import semantic_hash

            manifest["semantic_hash"] = semantic_hash(semantic_config)
        obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())
        for d in decimals:
            _make_leaf(root, d, self.CELLS[d])
        _run_record(root, decimals)

    def test_fresh_install_on_pre_declaration_store(self, tmp_path):
        from zagg.hive import _frozen_matches
        from zagg.sweep_overview import build_pyramid_block

        self._pre_declaration_store(tmp_path)
        before = read_manifest(str(tmp_path))
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["updated"] is True and summary["previous"] == "absent"
        assert summary["orders"] == [0]  # shard_order 2, default spacing 2
        assert summary["fold_source"] == "cascade"  # the regime future sweeps apply
        assert summary["fields"] == {
            "count": "exact",
            "h_min": "exact",
            "h_mean": "none",
            "h_tdigest": "approximate",
        }
        assert summary["validated"].startswith("leaf ")
        after = read_manifest(str(tmp_path))
        # The installed block IS the template-time derivation at the
        # manifest's own shard order — no retrofit-specific grammar.
        assert after["pyramid"] == build_pyramid_block(_leaf_cfg(), SHARD_ORDER)
        assert _frozen_matches(after, before)  # only the pyramid key moved

    def test_second_call_is_idempotent_no_put(self, tmp_path, monkeypatch):
        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())
        puts = []
        real_put = obstore.put
        monkeypatch.setattr(obstore, "put", lambda *a, **k: (puts.append(a), real_put(*a, **k))[1])
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["updated"] is False and summary["previous"] == "identical"
        assert puts == []  # identical declaration -> no PUT at all

    def test_non_json_primitive_declaration_stays_idempotent(self, tmp_path, monkeypatch):
        # The block is normalized to canonical JSON before the compare, so a
        # config value that is not a JSON primitive (here a tuple, which comes
        # back out of the manifest as a list) cannot re-PUT on every call.
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"summarize": {"h_tdigest": {"as": "p50", "quantiles": (0.5,)}}}
        declare_pyramid(str(tmp_path), cfg)
        assert read_manifest(str(tmp_path))["pyramid"]["overview"]["summarize"] == {
            "h_tdigest": {"as": "p50", "quantiles": [0.5]}
        }
        puts = []
        real_put = obstore.put
        monkeypatch.setattr(obstore, "put", lambda *a, **k: (puts.append(a), real_put(*a, **k))[1])
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "identical" and puts == []

    def test_redeclaration_preserves_materialized(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        materialized = read_manifest(str(tmp_path))["pyramid"]["overview"]["materialized"]
        assert materialized["orders"] == [0]
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"orders": [1, 0]}  # the schedule changes
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True and summary["previous"] == "replaced"
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block["orders"] == [1, 0]
        assert block["materialized"] == materialized  # actuals survive the rewrite

    def test_concurrent_materialized_survives_the_validation_window(self, tmp_path, monkeypatch):
        # The RMW re-reads immediately before its PUT: a sweep landing
        # ``materialized`` while the (slow) validation runs must not be reverted
        # by a manifest copy read before it.
        from zagg import sweep_overview

        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())
        real = sweep_overview._validate_block_against_store

        def concurrent_sweep(*args, **kwargs):
            out = real(*args, **kwargs)
            sweep_overview._update_manifest_pyramid(str(tmp_path), {0: "leaves"}, {})
            return out

        monkeypatch.setattr(sweep_overview, "_validate_block_against_store", concurrent_sweep)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"orders": [1, 0]}
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block["orders"] == [1, 0]
        assert block["materialized"]["orders"] == [0]  # the concurrent update survives

    def test_manifest_changed_under_validation_refuses(self, tmp_path, monkeypatch):
        # Frozen keys re-checked at the PUT: the block is never installed on a
        # manifest the validation never saw.
        from zagg import sweep_overview

        self._pre_declaration_store(tmp_path)
        real = sweep_overview._validate_block_against_store

        def clobber(*args, **kwargs):
            out = real(*args, **kwargs)
            m = read_manifest(str(tmp_path))
            m["shard_order"] = SHARD_ORDER + 1
            obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(m).encode())
            return out

        monkeypatch.setattr(sweep_overview, "_validate_block_against_store", clobber)
        with pytest.raises(ValueError, match="changed under declare_pyramid"):
            declare_pyramid(str(tmp_path), _leaf_cfg())
        assert "pyramid" not in read_manifest(str(tmp_path))

    def test_field_dtype_drift_refuses_and_writes_nothing(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_min"]["dtype"] = "float64"  # store holds float32
        with pytest.raises(ValueError, match="h_min.*declared float64"):
            declare_pyramid(str(tmp_path), cfg)
        assert "pyramid" not in read_manifest(str(tmp_path))  # never half-written

    def test_absent_declared_field_refuses(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["extra"] = {"function": "max", "dtype": "float32"}
        with pytest.raises(ValueError, match="'extra' is declared but absent"):
            declare_pyramid(str(tmp_path), cfg)

    def test_ragged_element_dtype_drift_refuses(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_tdigest"]["dtype"] = "float64"  # store: float32
        with pytest.raises(ValueError, match="h_tdigest.*ragged element dtype"):
            declare_pyramid(str(tmp_path), cfg)

    def test_ragged_element_without_dtype_refuses(self, tmp_path):
        # np.dtype(None) is float64, so a dtype-less element block would
        # otherwise silently validate a float64 declaration.
        from zarr import open_array

        self._pre_declaration_store(tmp_path)
        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        arr = open_array(leaf, path=f"{CELL_ORDER}/h_tdigest", mode="r+", zarr_format=3)
        rag = dict(arr.attrs["ragged"])
        rag["element"] = {"shape": [-1, 2]}  # a block that declares no dtype
        arr.attrs["ragged"] = rag
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_tdigest"]["dtype"] = "float64"
        with pytest.raises(ValueError, match="h_tdigest.*carries no dtype"):
            declare_pyramid(str(tmp_path), cfg)

    def test_ragged_inner_shape_drift_refuses(self, tmp_path):
        # The declared inner shape is pinned by composability (build_tdigest is
        # approximate only at [2]), so the drift that can happen is STORE-side:
        # a leaf written under a different element convention.
        from zarr import open_array

        self._pre_declaration_store(tmp_path)
        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        arr = open_array(leaf, path=f"{CELL_ORDER}/h_tdigest", mode="r+", zarr_format=3)
        rag = dict(arr.attrs["ragged"])
        rag["element"] = {"dtype": "float32", "shape": [-1, 3]}
        arr.attrs["ragged"] = rag
        with pytest.raises(ValueError, match="h_tdigest.*inner_shape"):
            declare_pyramid(str(tmp_path), _leaf_cfg())

    def test_flux_declaration_over_a_counts_store_refuses(self, tmp_path):
        # The operator-facing case the gate exists for (review, issue #424):
        # without it the retrofit PUTs the flux declaration clean, and the
        # next sweep logs "skipping unreadable leaf" for every leaf.
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_tdigest"].update(
            weights="flux", attrs={"gain": {"name": "g", "version": "1"}}
        )
        with pytest.raises(ValueError, match="h_tdigest.*weights declaration"):
            declare_pyramid(str(tmp_path), cfg)
        assert "pyramid" not in read_manifest(str(tmp_path))  # nothing written

    def test_store_side_weights_drift_refuses(self, tmp_path):
        # The other direction: a flux-stamped leaf under a counts config.
        from zarr import open_array

        self._pre_declaration_store(tmp_path)
        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        arr = open_array(leaf, path=f"{CELL_ORDER}/h_tdigest", mode="r+", zarr_format=3)
        arr.attrs["weights"] = "flux"
        with pytest.raises(ValueError, match="h_tdigest.*weights declaration"):
            declare_pyramid(str(tmp_path), _leaf_cfg())
        assert "pyramid" not in read_manifest(str(tmp_path))

    def test_undefined_stored_weights_raises(self, tmp_path):
        # A value §2.0 does not define: unreadable, not merely mis-declared.
        from zarr import open_array

        self._pre_declaration_store(tmp_path)
        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        arr = open_array(leaf, path=f"{CELL_ORDER}/h_tdigest", mode="r+", zarr_format=3)
        arr.attrs["weights"] = "photons"
        with pytest.raises(ValueError, match="unknown weights declaration"):
            declare_pyramid(str(tmp_path), _leaf_cfg())

    def test_unreadable_ragged_spec_revision_refuses(self, tmp_path):
        # espg-ruled (issue #358): a store whose ragged layout THIS zagg cannot
        # read must be refused at declaration, not discovered at fold time.
        # Same posture as the reader (readers.tdigest_tensor._open_ragged).
        from zarr import open_array

        from zagg.grids.base import RAGGED_SPEC

        self._pre_declaration_store(tmp_path)
        leaf = open_store(shard_leaf_path(str(tmp_path), morton_word("-311")))
        arr = open_array(leaf, path=f"{CELL_ORDER}/h_tdigest", mode="r+", zarr_format=3)
        rag = dict(arr.attrs["ragged"])
        assert rag["spec"] == RAGGED_SPEC  # the fixture is a current-spec store
        rag["spec"] = "zagg-ragged/2"  # the live migration path (issue #210)
        arr.attrs["ragged"] = rag
        with pytest.raises(ValueError, match="h_tdigest.*zagg-ragged/2"):
            declare_pyramid(str(tmp_path), _leaf_cfg())
        assert "pyramid" not in read_manifest(str(tmp_path))  # nothing written

    def test_declared_off_retrofit(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = False
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True and summary["orders"] == []
        assert summary["fields"] == {}
        assert read_manifest(str(tmp_path))["pyramid"] == {
            "spec": PYRAMID_SPEC,
            "overview": {"orders": []},
        }
        # Recorded absence: the sweep sees the declared-off block and no-ops.
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["declared"] is False

    def test_wrong_reducer_refuses_via_semantic_hash(self, tmp_path):
        # The failure the leaf probe structurally cannot catch: no leaf records
        # which reducer produced a field, so a config declaring h_min as a MAX
        # over a store of minima passes every dtype check. The store's frozen
        # semantic_hash is what refuses it.
        self._pre_declaration_store(tmp_path, semantic_config=_leaf_cfg())
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_min"]["function"] = "max"
        with pytest.raises(ValueError, match="config semantics .* != the store's frozen"):
            declare_pyramid(str(tmp_path), cfg)
        assert "pyramid" not in read_manifest(str(tmp_path))

    def test_pyramid_only_edit_hashes_identically(self, tmp_path):
        # The intended retrofit — the ORIGINAL config plus output.pyramid —
        # must not false-refuse: output.* is not in the semantic core.
        from zagg.semantics import semantic_fingerprint, semantic_hash

        self._pre_declaration_store(tmp_path, semantic_config=_leaf_cfg())
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"orders": [1, 0]}
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True and summary["orders"] == [1, 0]
        fp = semantic_fingerprint(semantic_hash(_leaf_cfg()))
        assert summary["validated"].endswith(f"semantic_hash {fp}")

    def test_hashless_manifest_skips_the_semantic_check(self, tmp_path, caplog):
        # Pre-#299 stores carry no hash; the check is skipped (the _frozen_matches
        # both-sides-present exemption) but loudly, and the summary says so.
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_min"]["function"] = "max"  # unverifiable here
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True
        assert summary["validated"].endswith(
            "semantic_hash absent (pre-#299 store — fold methods unverified)"
        )
        assert "could NOT be verified" in caplog.text

    def test_declared_off_preserves_prior_materialized(self, tmp_path):
        # The intended shape (D24 option A): overviews already on disk are real
        # regenerable-cache debris, so a declared-OFF block still inventories
        # them. The sweep gates on ``orders``, so it no-ops regardless.
        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())
        run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        materialized = read_manifest(str(tmp_path))["pyramid"]["overview"]["materialized"]
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = False
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True and summary["orders"] == []
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block == {"orders": [], "materialized": materialized}
        result = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert result["families"]["overview"]["declared"] is False

    def test_drift_leaves_an_existing_declaration_untouched(self, tmp_path):
        # The dangerous half of never-half-written: a store that already carries
        # a GOOD declaration must come out of a refused call byte-identical.
        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())
        before = json.dumps(read_manifest(str(tmp_path)), sort_keys=True)
        cfg = _leaf_cfg()
        cfg.aggregation["variables"]["h_min"]["dtype"] = "float64"
        with pytest.raises(ValueError, match="h_min.*declared float64"):
            declare_pyramid(str(tmp_path), cfg)
        assert json.dumps(read_manifest(str(tmp_path)), sort_keys=True) == before

    def test_windowed_store_probe_resolves_the_window(self, tmp_path):
        # The probe threads the run record's window into shard_leaf_path; on a
        # windowed store the leaf exists ONLY under ``{window}.zarr``, so a
        # dropped window would silently degrade to the no-committed-leaf skip.
        _write_manifest(tmp_path, windowed=True)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        del manifest["pyramid"]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        _make_leaf(tmp_path, "-311", self.CELLS["-311"], window="2411")
        _run_record(tmp_path, ("-311",), window="2411")
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["validated"].startswith("leaf ") and "2411.zarr" in summary["validated"]

    def test_missing_manifest_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not a hive store root"):
            declare_pyramid(str(tmp_path), _leaf_cfg())

    @pytest.mark.parametrize("drop", ["shard_order", "cell_order"])
    def test_malformed_manifest_raises_pointedly(self, tmp_path, drop):
        # A JSON blob that parses but is not a hive manifest gets the pointed
        # refusal, not a KeyError — and cell_order refuses up front rather than
        # after the whole leaf-discovery cost.
        self._pre_declaration_store(tmp_path)
        manifest = read_manifest(str(tmp_path))
        del manifest[drop]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        with pytest.raises(ValueError, match="declares no shard_order/cell_order"):
            declare_pyramid(str(tmp_path), _leaf_cfg())

    def test_non_dict_prior_pyramid_is_replaced(self, tmp_path):
        # A hand-edited ``"pyramid": "off"`` is not this module's grammar: it
        # carries no actuals to preserve and is replaced, never AttributeErrors.
        self._pre_declaration_store(tmp_path)
        manifest = read_manifest(str(tmp_path))
        manifest["pyramid"] = "off"
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["previous"] == "replaced" and summary["updated"] is True
        assert read_manifest(str(tmp_path))["pyramid"]["overview"]["orders"] == [0]

    def test_no_committed_leaf_still_declarable(self, tmp_path, caplog):
        _write_manifest(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        del manifest["pyramid"]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["updated"] is True
        assert summary["validated"].startswith("manifest only (no run records")
        assert "field-level validation skipped" in caplog.text
        assert read_manifest(str(tmp_path))["pyramid"]["overview"]["orders"] == [0]

    def test_uncommitted_refs_are_not_reported_as_no_records(self, tmp_path, caplog):
        # A store whose refs are all debris is a different animal from one that
        # has never run: both stay declarable (the probe never refuses), but the
        # operator must be able to tell which skip they got.
        _write_manifest(tmp_path)
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        del manifest["pyramid"]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        _run_record(tmp_path, ("-311", "-312"))  # refs whose leaves never committed
        summary = declare_pyramid(str(tmp_path), _leaf_cfg())
        assert summary["updated"] is True
        assert summary["validated"].startswith("manifest only (none of the 2 run-record refs")
        assert "field-level validation skipped" in caplog.text

    def test_orders_outside_the_store_refuse(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"orders": [4]}  # not an ancestor of shard_order 2
        with pytest.raises(ValueError, match="not ancestor orders"):
            declare_pyramid(str(tmp_path), cfg)

    def test_retrofit_then_sweep_materializes_overviews(self, tmp_path):
        # End-to-end (the issue #358 consumer): the sweep no-ops on the
        # pre-#344 store, the retrofit installs the declaration, and one
        # overview sweep then materializes real overviews.
        from zagg.sweep import discover_leaves

        self._pre_declaration_store(tmp_path, decimals=("-311", "-312"))
        noop = run_sweep(str(tmp_path), [(morton_word("-311"), None)], families=("overview",))
        assert noop["families"]["overview"]["declared"] is False  # the #358 gap
        declare_pyramid(str(tmp_path), _leaf_cfg())
        refs = discover_leaves(str(tmp_path))
        result = run_sweep(str(tmp_path), refs, families=("overview",))
        assert result["families"]["overview"]["written"] == 1  # node -3, order 0
        g = _overview_group(tmp_path, "-3", "all.zarr", 2)
        np.testing.assert_array_equal(g["count"][:2], [2, 1])  # per-leaf obs fold whole
        assert g["h_min"][0] == 1.0 and g["h_min"][1] == 3.0
        block = read_manifest(str(tmp_path))["pyramid"]["overview"]
        assert block["materialized"]["orders"] == [0]

    def test_declare_v2_levels_on_existing_store(self, tmp_path):
        from zagg.pyramid import PYRAMID_SPEC_V2

        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": 3}  # scalar sugar; window is (2, 4)
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is True and summary["previous"] == "absent"
        # /2 declares the expanded overviews list, not orders — the summary
        # omits `orders` ENTIRELY, mirroring the manifest block: an empty
        # `orders` is /1's declared-off signal (specification §4.5).
        assert "orders" not in summary
        assert summary["overviews"] == [
            {"node": 2, "cells": [3]},
            {"node": 1, "cells": [2]},
            {"node": 0, "cells": [1]},
        ]
        assert summary["validated"].startswith("leaf ")  # store truth still probed
        after = read_manifest(str(tmp_path))
        assert after["pyramid"]["spec"] == PYRAMID_SPEC_V2
        assert after["pyramid"]["overviews"] == summary["overviews"]

    def test_declare_v2_is_idempotent(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": [3]}
        declare_pyramid(str(tmp_path), cfg)
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is False and summary["previous"] == "identical"

    def test_declare_v2_levels_outside_the_store_refuse(self, tmp_path):
        self._pre_declaration_store(tmp_path)
        cfg = _leaf_cfg()
        # Valid grammar in the abstract (the grid-less config skips the
        # template-time check), but resolution 4 IS this store's cell order —
        # the manifest's own orders win (issue #358 posture).
        cfg.output["pyramid"] = {"overviews": [4]}
        with pytest.raises(ValueError, match="not strictly between"):
            declare_pyramid(str(tmp_path), cfg)

    def test_declare_v2_replaces_v1_preserving_materialized(self, tmp_path):
        from zagg.pyramid import PYRAMID_SPEC_V2

        self._pre_declaration_store(tmp_path)
        declare_pyramid(str(tmp_path), _leaf_cfg())  # installs the /1 default
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        actuals = {
            "orders": [0],
            "fold_sources": {"0": "leaves"},
            "generated_at": "2026-01-01T00:00:00+00:00",
        }
        manifest["pyramid"]["overview"]["materialized"] = actuals
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": [3]}
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "replaced" and summary["updated"] is True
        after = read_manifest(str(tmp_path))
        # The /1 actuals survive the revision bump: they inventory overview
        # artifacts already on disk (regenerable-cache debris, D24 option A).
        assert after["pyramid"]["spec"] == PYRAMID_SPEC_V2
        assert after["pyramid"]["overview"]["materialized"] == actuals

    def test_declare_v2_replaces_v1_declared_but_never_materialized(self, tmp_path):
        # The live atl03_tdigest_o9 shape (issue #520 audit): a /1 declaration
        # retrofit-installed but NEVER swept — no ``materialized`` key at all.
        # Replacing it with the /2 dense ladder must be a clean single-PUT
        # replace that invents no actuals inventory out of nothing.
        from zagg.pyramid import PYRAMID_SPEC_V2

        _write_manifest(tmp_path)  # /1 orders [1, 0], no materialized
        _make_leaf(tmp_path, "-311", self.CELLS["-311"])
        _run_record(tmp_path, ("-311",))
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": [3]}
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "replaced" and summary["updated"] is True
        assert "orders" not in summary  # /2 summaries carry no /1 schedule key
        after = read_manifest(str(tmp_path))["pyramid"]
        assert after["spec"] == PYRAMID_SPEC_V2
        # The dense every-order ladder: leaf entry + one member per order to 0.
        assert after["overviews"] == [
            {"node": 2, "cells": [3]},
            {"node": 1, "cells": [2]},
            {"node": 0, "cells": [1]},
        ]
        assert "materialized" not in after["overview"]
        assert "orders" not in after["overview"] and "spacing" not in after["overview"]

    def test_declare_v2_replaces_v1_declared_off(self, tmp_path):
        # The live gedi_flux_o9 shape: pyramids declared OFF (``orders: []``,
        # the /1 declared-off signal) — the /2 retrofit replaces it wholesale.
        from zagg.pyramid import PYRAMID_SPEC_V2

        _write_manifest(tmp_path, orders=())
        _make_leaf(tmp_path, "-311", self.CELLS["-311"])
        _run_record(tmp_path, ("-311",))
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": [3]}
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "replaced" and summary["updated"] is True
        after = read_manifest(str(tmp_path))["pyramid"]
        assert after["spec"] == PYRAMID_SPEC_V2
        assert [e["node"] for e in after["overviews"]] == [2, 1, 0]
        assert "materialized" not in after["overview"]

    def test_declared_v2_store_sweeps_to_a_loud_noop(self, tmp_path, caplog):
        # End-to-end (#381 point (11)): declaring is free, sweeping is the
        # operational decision — a /2 retrofit leaves the store sweep-safe.
        from zagg.sweep import discover_leaves

        self._pre_declaration_store(tmp_path, decimals=("-311", "-312"))
        cfg = _leaf_cfg()
        cfg.output["pyramid"] = {"overviews": [3]}
        declare_pyramid(str(tmp_path), cfg)
        caplog.set_level("INFO")
        result = run_sweep(str(tmp_path), discover_leaves(str(tmp_path)), families=("overview",))
        counts = result["families"]["overview"]
        assert counts["regime"] == "stages" and counts["written"] == 0
        assert "staged sweep" in caplog.text
        assert not list(tmp_path.rglob("overview.rollup.json"))
