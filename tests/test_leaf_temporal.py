"""The per-leaf temporal record and its counted cover — spec §10.3/§10.6, issue #575.

What is pinned here that the §7 conformance suite cannot: the counted
cover's laws (merge by per-word sum, coarsen by ancestor sum, the cover
derived from the keys — each exact, order-independent, and byte-equal to
the §10.5 quantization), the chunked accumulator's fold laws from either
feed, the record's own consistency checks, and the arming rule. The
committed ``temporal/`` fixture is the real-store end of it
(``test_spec_conformance.py``); the worker-path end (pooled and both spill
regimes) is in ``test_spill.py``.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from mortie import time2toc, toc2time, toc_normalize, toc_overlaps, toc_reduce

from zagg import leaf_temporal
from zagg.config import PipelineConfig
from zagg.coverage_toc import COVER_CAP, TEMPORAL_COVER_ORDER, quantize_words
from zagg.leaf_temporal import (
    LEAF_TEMPORAL_NAME,
    LEAF_TEMPORAL_SPEC,
    CountedCover,
    LeafTemporalAccumulator,
    armed,
    build_leaf_temporal,
    cap_counts,
    coarsen_counts,
    count_words,
    cover_from_counts,
    decode_counts,
    encode_counts,
    leaf_temporal_contribution,
    load_leaf_temporal,
    merge_counts,
    read_leaf_temporal_record,
    temporal_field_names,
    write_leaf_temporal,
)

#: A day on the toc scale, in internal ns.
DAY_NS = 86_400 * 10**9
#: An arbitrary but realistic base instant on the §8 internal-ns scale.
BASE_NS = 5_344_000_000_000_000_000
#: One bucket at the pinned order — the one rung counts and cover share.
COUNT_ORDER = TEMPORAL_COVER_ORDER
COUNT_SPAN = COVER_SPAN = 1 << (63 - TEMPORAL_COVER_ORDER)


def _stamps(t) -> np.ndarray:
    return np.asarray([int(time2toc(int(x))) for x in np.atleast_1d(t)], dtype=np.uint64)


def _instants(n: int, seed: int, clusters: int = 3, gap_days: int = 5) -> np.ndarray:
    """``n`` timestamp words in ``clusters`` campaign clusters ``gap_days`` apart.

    Each cluster is a few hours wide, the gaps are whole cover buckets wide,
    so the cover is a multi-word set with real holes and the counted cover
    has many occupied ~9-minute buckets per cluster.
    """
    rng = np.random.default_rng(seed)
    which = rng.integers(0, clusters, n)
    t = BASE_NS + which * gap_days * DAY_NS + rng.integers(0, 4 * 3600 * 10**9, n)
    return _stamps(np.sort(t))


def _whole(words: np.ndarray):
    acc = LeafTemporalAccumulator()
    acc.add_words(words)
    return acc.finish()


def _same(a: CountedCover, b: CountedCover) -> None:
    assert a.order == b.order
    np.testing.assert_array_equal(a.words, b.words)
    np.testing.assert_array_equal(a.obs, b.obs)


def _covers(cover: np.ndarray, words: np.ndarray) -> bool:
    """Whether ``cover`` overlaps every instant the timestamp ``words`` encode."""
    start, end = (np.atleast_1d(x) for x in toc2time(words))
    return all(
        bool(np.any(np.atleast_1d(toc_overlaps(cover, int(lo), max(int(hi), int(lo) + 1)))))
        for lo, hi in zip(start, end, strict=True)
    )


class TestCountedCoverLaws:
    """§10.3 — the algebra: exact, order-independent, un-coalesced."""

    def test_counts_are_exact_and_stay_un_coalesced(self):
        # Two abutting buckets: toc_normalize would fuse them into one range;
        # the counted cover keeps both, each with its own count.
        t = np.array([BASE_NS + 5, BASE_NS + 9, BASE_NS + COUNT_SPAN + 1], dtype=np.uint64)
        t -= np.uint64(BASE_NS % COUNT_SPAN)  # align the first bucket
        counts = count_words(_stamps(t))
        assert counts.order == COUNT_ORDER
        assert len(counts.words) == 2 and counts.obs.tolist() == [2, 1]
        assert len(toc_normalize(counts.words)) == 1
        assert int(counts.obs.sum()) == 3

    def test_weights_count_each_word_that_many_times(self):
        words = _instants(50, seed=1)
        weighted = count_words(words[:10], weights=np.arange(1, 11))
        assert int(weighted.obs.sum()) == 55
        _same(count_words(words), count_words(words, weights=np.ones(len(words))))

    def test_merge_is_a_per_word_sum_and_order_independent(self):
        words = _instants(900, seed=2)
        whole = count_words(words)
        parts = [count_words(words[i : i + 137]) for i in range(0, len(words), 137)]
        _same(merge_counts(parts), whole)
        _same(merge_counts(parts[::-1]), whole)
        assert int(whole.obs.sum()) == 900

    def test_coarsen_sums_into_ancestors_exactly(self):
        words = _instants(600, seed=3)
        fine = count_words(words)
        coarse = coarsen_counts(fine, COUNT_ORDER - 3)
        assert coarse.order == COUNT_ORDER - 3 and len(coarse.words) <= len(fine.words)
        assert int(coarse.obs.sum()) == int(fine.obs.sum()) == 600
        _same(coarse, count_words(words, order=COUNT_ORDER - 3))
        _same(coarsen_counts(fine, COUNT_ORDER), fine)
        with pytest.raises(ValueError, match="cannot coarsen"):
            coarsen_counts(fine, COUNT_ORDER + 1)

    def test_merging_two_orders_coarsens_the_finer_first(self):
        words = _instants(300, seed=4)
        a = count_words(words[:150])
        b = coarsen_counts(count_words(words[150:]), COUNT_ORDER - 2)
        merged = merge_counts([a, b])
        assert merged.order == COUNT_ORDER - 2
        _same(merged, count_words(words, order=COUNT_ORDER - 2))

    def test_the_cap_coarsens_by_whole_orders_and_conserves_the_total(self):
        # One instant every third bucket, well past the cap.
        n = COVER_CAP + 100
        t = BASE_NS + np.arange(n, dtype=np.uint64) * np.uint64(3 * COUNT_SPAN)
        fine = count_words(_stamps(t))
        assert len(fine.words) == n
        capped = cap_counts(fine)
        assert len(capped.words) <= COVER_CAP and capped.order < COUNT_ORDER
        assert int(capped.obs.sum()) == n
        _same(capped, count_words(_stamps(t), order=capped.order))

    def test_the_cover_is_the_10_5_quantization_byte_for_byte(self):
        words = _instants(700, seed=5)
        cover, order = cover_from_counts(count_words(words))
        assert order == TEMPORAL_COVER_ORDER
        np.testing.assert_array_equal(cover, quantize_words(words))
        # Normalizing coalesces abutting buckets the counts keep apart.
        assert len(cover) < len(count_words(words).words)
        # A counted cover capped BELOW the pin derives its cover at its own rung.
        coarse = coarsen_counts(count_words(words), TEMPORAL_COVER_ORDER - 2)
        cover, order = cover_from_counts(coarse)
        assert order == TEMPORAL_COVER_ORDER - 2
        np.testing.assert_array_equal(cover, quantize_words(words, order))

    def test_empty_in_empty_out(self):
        empty = count_words(np.empty(0, np.uint64))
        assert empty.words.size == 0 and empty.order == COUNT_ORDER
        assert merge_counts([]).words.size == 0
        assert cover_from_counts(empty)[0].size == 0
        _same(coarsen_counts(empty, 3), CountedCover(empty.words, empty.obs, 3))

    def test_the_block_round_trips_and_is_must_checked(self):
        counts = count_words(_instants(200, seed=6))
        block = json.loads(json.dumps(encode_counts(counts)))
        assert block["temporal_order"] == COUNT_ORDER and block["cap"] == COVER_CAP
        assert block["element"] == {"dtype": "uint64", "shape": [-1]}
        assert block["count"] == len(counts.words) and block["obs_total"] == 200
        _same(decode_counts(block), counts)
        for key, value, match in (
            ("count", block["count"] + 1, "buckets"),
            ("obs_total", 199, "obs_total"),
            ("temporal_order", COUNT_ORDER + 1, "outside"),
        ):
            broken = {**block, key: value}
            with pytest.raises(ValueError, match=match):
                decode_counts(broken)
        with pytest.raises(ValueError, match="not an object"):
            decode_counts("nope")


class TestAccumulatorLaws:
    """The chunked fold equals the whole-leaf fold, from either feed."""

    def test_a_chunked_fold_equals_the_whole_fold(self, monkeypatch):
        # Fold on a tiny row budget so the accumulator actually folds mid-leaf
        # (several times), rather than once at finish.
        monkeypatch.setattr(leaf_temporal, "FOLD_ROWS", 37)
        words = _instants(1_000, seed=575)
        acc = LeafTemporalAccumulator()
        for start in range(0, len(words), 61):
            acc.add_words(words[start : start + 61])
        word, counts = acc.finish()
        w_word, w_counts = _whole(words)
        assert word == w_word == int(toc_reduce(words))
        _same(counts, w_counts)
        _same(counts, count_words(words))

    def test_chunk_order_does_not_matter(self, monkeypatch):
        monkeypatch.setattr(leaf_temporal, "FOLD_ROWS", 50)
        words = _instants(600, seed=7)
        chunks = [words[i : i + 45] for i in range(0, len(words), 45)]
        rng = np.random.default_rng(1)
        results = []
        for _ in range(3):
            acc = LeafTemporalAccumulator()
            for j in rng.permutation(len(chunks)):
                acc.add_words(chunks[j])
            results.append(acc.finish())
        for word, counts in results[1:]:
            assert word == results[0][0]
            _same(counts, results[0][1])

    def test_the_weighted_feed_agrees_with_the_word_feed_on_exact_centroids(self):
        # The sweep's raw route hands per-centroid words with their weights;
        # weight-1 centroids are exact instants, so the fold equals the
        # worker's over the same observations.
        words = _instants(300, seed=3)
        acc = LeafTemporalAccumulator()
        for start in range(0, len(words), 100):
            acc.add_weighted(words[start : start + 100], np.ones(100, np.float32))
        word, counts = acc.finish()
        w_word, w_counts = _whole(words)
        assert word == w_word
        _same(counts, w_counts)

    def test_a_merged_centroid_counts_at_its_midpoint(self):
        # A range word carrying weight w lands whole in the bucket of its
        # envelope midpoint (§10.3); the envelope word still joins in full.
        from mortie import span2toc

        lo = BASE_NS - BASE_NS % COUNT_SPAN
        hi = lo + 6 * COUNT_SPAN  # spans seven buckets; midpoint in the fourth
        merged = np.asarray([int(span2toc(lo, hi))], dtype=np.uint64)
        acc = LeafTemporalAccumulator()
        acc.add_weighted(merged, np.array([17.0]))
        word, counts = acc.finish()
        assert word == int(merged[0])
        assert len(counts.words) == 1 and int(counts.obs[0]) == 17
        mid = lo + (hi - lo) // 2
        assert bool(np.any(np.atleast_1d(toc_overlaps(counts.words, mid, mid + 1))))
        # The cover derived from it sits INSIDE the quantized envelope word.
        cover, order = cover_from_counts(counts)
        lo_c, hi_c = (int(x) for x in toc2time(int(toc_reduce(cover))))
        lo_w, hi_w = (int(x) for x in toc2time(int(toc_reduce(quantize_words(merged, order)))))
        assert lo_w <= lo_c and hi_c <= hi_w

    def test_the_cover_contains_every_instant_and_keeps_the_gaps(self):
        words = _instants(500, seed=11)
        _word, counts = _whole(words)
        cover, _order = cover_from_counts(counts)
        assert len(cover) >= 2
        assert _covers(cover, words)
        # The clusters are 5 days apart: a whole aligned bucket between them
        # stays uncovered (§10.5's never-bridge law at bucket resolution).
        mid = BASE_NS + 2 * DAY_NS + DAY_NS // 2
        lo = (mid // COVER_SPAN) * COVER_SPAN
        assert not bool(np.any(np.atleast_1d(toc_overlaps(cover, lo, lo + COVER_SPAN))))

    def test_nothing_added_finishes_as_none(self):
        acc = LeafTemporalAccumulator()
        acc.add_words(np.empty(0, dtype=np.uint64))
        acc.add_weighted(np.empty(0, dtype=np.uint64), np.empty(0))
        assert acc.finish() is None


class TestRecordGrammar:
    """§10.6 — what a built record carries, and what a reader refuses."""

    def _record(self, n=400, seed=575, **kw):
        word, counts = _whole(_instants(n, seed=seed))
        return build_leaf_temporal(word, counts, ["h_tdigest"], **kw)

    def test_round_trip_through_json(self):
        record = self._record()
        body = json.loads(json.dumps(record))
        assert set(body) == {
            "spec",
            "source",
            "generated_at",
            "fields",
            "n_obs",
            "word",
            "temporal_order",
            "cap",
            "counts",
            "cover",
        }
        assert body["spec"] == LEAF_TEMPORAL_SPEC
        assert body["source"] == "worker"
        assert body["fields"] == ["h_tdigest"]
        assert body["n_obs"] == 400 == body["counts"]["obs_total"]
        assert body["temporal_order"] == TEMPORAL_COVER_ORDER and body["cap"] == COVER_CAP
        assert body["counts"]["temporal_order"] == COUNT_ORDER
        assert isinstance(body["word"], str) and int(body["word"]) > 2**53
        assert body["cover"]["element"] == {"dtype": "uint64", "shape": [-1]}
        assert body["cover"]["encoding"] == "base64"
        assert "temporal_order" not in body["cover"]  # absence means the pin
        assert load_leaf_temporal(body) is body
        word, counts = leaf_temporal_contribution(body)
        assert word == int(record["word"]) and int(counts.obs.sum()) == 400
        words = _instants(400, seed=575)
        _same(counts, count_words(words))
        # The carried cover IS the §10.5 quantization, and the per-leaf
        # parity holds with equality on a worker record.
        cover, _order = cover_from_counts(counts)
        np.testing.assert_array_equal(cover, quantize_words(words))
        assert int(toc_reduce(cover)) == int(toc_reduce(quantize_words([word])))

    def test_the_sweep_provenance_is_recorded(self):
        assert self._record(source="sweep")["source"] == "sweep"

    def test_counts_over_the_cap_coarsen_and_the_cover_follows(self):
        n = COVER_CAP + 100
        t = BASE_NS + np.arange(n, dtype=np.uint64) * np.uint64(3 * COUNT_SPAN)
        word, counts = _whole(_stamps(t))
        record = build_leaf_temporal(word, counts, ["h_tdigest"])
        assert record["counts"]["count"] <= COVER_CAP
        # Un-coalesced buckets two apart are still distinct one rung down and
        # pair up only at the second: the cap lands at the pin minus two, and
        # the derived cover block records the same order (§10.5's block rule).
        assert record["counts"]["temporal_order"] == COUNT_ORDER - 2
        assert record["cover"]["temporal_order"] == record["counts"]["temporal_order"]
        got_word, got_counts = leaf_temporal_contribution(record)
        assert got_word == word and int(got_counts.obs.sum()) == n
        cover, _order = cover_from_counts(got_counts)
        assert _covers(cover, _stamps(t))

    @pytest.mark.parametrize("obj", [None, [], {"spec": "zagg-leaf-temporal/9"}, {"word": "1"}])
    def test_a_foreign_or_unmarked_body_reads_as_absent(self, obj):
        assert load_leaf_temporal(obj) is None

    def test_a_cover_block_disagreeing_with_its_count_is_refused(self):
        record = self._record()
        record["cover"]["count"] += 1
        with pytest.raises(ValueError, match="declares"):
            leaf_temporal_contribution(record)

    def test_a_cover_that_is_not_derived_from_the_counts_is_refused(self):
        record = self._record()
        other = self._record(seed=11)
        record["cover"] = other["cover"]
        with pytest.raises(ValueError, match="derive"):
            leaf_temporal_contribution(record)

    def test_a_wrong_n_obs_is_refused(self):
        record = self._record()
        record["n_obs"] += 1
        with pytest.raises(ValueError, match="n_obs"):
            leaf_temporal_contribution(record)

    def test_a_word_that_does_not_envelope_the_cover_is_refused(self):
        record = self._record()
        record["word"] = str(int(time2toc(BASE_NS + 400 * DAY_NS)))
        with pytest.raises(ValueError, match="containment"):
            leaf_temporal_contribution(record)

    def test_write_and_read_on_a_leaf(self, tmp_path):
        leaf = str(tmp_path / "11213.zarr")
        Path(leaf).mkdir()
        assert read_leaf_temporal_record(leaf) is None
        record = self._record()
        write_leaf_temporal(leaf, record)
        assert (Path(leaf) / LEAF_TEMPORAL_NAME).exists()
        assert read_leaf_temporal_record(leaf) == record
        # Debris (not JSON) raises for the caller to regenerate; a foreign
        # revision reads back raw so the caller can preserve it.
        (Path(leaf) / LEAF_TEMPORAL_NAME).write_bytes(b"not json {")
        with pytest.raises(ValueError):
            read_leaf_temporal_record(leaf)
        future = {"spec": "zagg-leaf-temporal/9", "word": "1"}
        (Path(leaf) / LEAF_TEMPORAL_NAME).write_text(json.dumps(future))
        assert read_leaf_temporal_record(leaf) == future
        assert load_leaf_temporal(read_leaf_temporal_record(leaf)) is None


class TestArming:
    """The record is armed by a §8.3 per-centroid declaration, nothing else."""

    def _cfg(self, **fields):
        variables = {"count": {"function": "len", "source": "h"}}
        for name, temporal in fields.items():
            variables[name] = {
                "kind": "ragged",
                "function": "zagg.stats.tdigest.build_tdigest",
                "source": "h",
                "inner_shape": [2],
                "dtype": "float32",
                "fill_value": 0,
                **({"temporal": temporal} if temporal else {}),
            }
        return PipelineConfig(data_source={"groups": ["g"]}, aggregation={"variables": variables})

    def test_per_centroid_arms_and_names_the_fields(self):
        cfg = self._cfg(z_tdigest="per-centroid", h_tdigest="per-centroid", plain=None)
        assert armed(cfg) is True
        assert temporal_field_names(cfg) == ["h_tdigest", "z_tdigest"]

    def test_no_temporal_field_arms_nothing(self):
        assert armed(self._cfg(h_tdigest=None)) is False
        assert temporal_field_names(self._cfg()) == []

    def test_a_per_cell_declaration_alone_arms_nothing(self):
        cfg = self._cfg()
        cfg.aggregation["variables"]["observed"] = {
            "function": "nanmax",
            "source": "h",
            "dtype": "uint64",
            "temporal": "per-cell",
        }
        assert armed(cfg) is False
