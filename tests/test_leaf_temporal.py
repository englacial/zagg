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
import shutil
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
    leaf_contribution,
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

    @pytest.mark.parametrize("key", ["temporal_order", "count", "obs_total"])
    @pytest.mark.parametrize("junk", [None, "5", 1.5, True])
    def test_a_non_integer_counts_key_is_refused_as_a_value_error(self, key, junk):
        """§10.3's MUST-checks refuse, they do not coerce.

        ``int(None)`` would leak a bare ``TypeError`` where the spec states a
        refusal — and the message is what moczarr implements the check from.
        """
        record = self._record()
        record["counts"][key] = junk
        with pytest.raises(ValueError, match="counted cover declares"):
            leaf_temporal_contribution(record)

    def test_both_blocks_decode_against_the_records_own_pin(self):
        """§10.6: the record declares one pin, and BOTH blocks answer to it."""
        record = self._record()
        record["temporal_order"] = COUNT_ORDER - 1
        with pytest.raises(ValueError, match="outside"):
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


# ── the sweep's route: record first, chunked raw fallback, materialize once ──

SPEC_DATA = Path(__file__).parent / "data" / "spec"
SHARD = "11213"


def _fixture_copy(tmp_path) -> str:
    dst = tmp_path / "temporal"
    shutil.copytree(SPEC_DATA / "temporal", dst)
    return str(dst)


def _leaf_of(root: str, decimal: str = SHARD) -> str:
    from zagg.grids.morton import morton_word
    from zagg.hive import shard_leaf_path

    return shard_leaf_path(root, int(morton_word(decimal)))


def _declared(root: str):
    from zagg.coverage_toc import temporal_cell_order, temporal_fields

    manifest = json.loads((Path(root) / "morton_hive.json").read_text())
    return temporal_cell_order(manifest), temporal_fields(manifest)


def _boom(*_a, **_k):
    raise AssertionError("the raw route must not run here")


class TestSweepRoute:
    """§10.6 as the sweep sees it: one GET per leaf, the leaf only when it must."""

    def test_the_record_is_read_first_and_no_array_is_opened(self, tmp_path, monkeypatch):
        import zagg.coverage_toc as toc

        root = _fixture_copy(tmp_path)
        cell_order, fields = _declared(root)
        monkeypatch.setattr(toc, "read_leaf_temporal", _boom)
        got, route = leaf_contribution(_leaf_of(root), cell_order, fields)
        assert route == "record"
        word, counts = leaf_temporal_contribution(read_leaf_temporal_record(_leaf_of(root)))
        assert got[0] == word
        _same(got[1], counts)

    def test_the_section_composes_from_the_record_alone(self, tmp_path, monkeypatch):
        import zagg.coverage_toc as toc
        from zagg.coverage_toc import cover_words, coverage_toc, coverage_toc_counts, read_cover
        from zagg.grids.morton import morton_word
        from zagg.hive import read_root_coverage
        from zagg.sweep import run_sweep

        root = _fixture_copy(tmp_path)
        (Path(root) / "coverage.moc").unlink()
        (Path(root) / "coverage.toc").unlink()
        monkeypatch.setattr(toc, "read_leaf_temporal", _boom)
        summary = run_sweep(root, [(int(morton_word(SHARD)), None)], families=["moc"], record=False)
        assert summary["families"]["moc"]["temporal_shards"] == 1
        record = read_leaf_temporal_record(_leaf_of(root))
        word, counts = leaf_temporal_contribution(record)
        envelope = read_root_coverage(root)
        assert coverage_toc(envelope) == {SHARD: word}
        _same(coverage_toc_counts(envelope), counts)
        cover, _order = cover_from_counts(counts)
        np.testing.assert_array_equal(cover_words(read_cover(root))[SHARD], cover)

    def test_a_missing_record_is_materialized_once(self, tmp_path, monkeypatch):
        import zagg.coverage_toc as toc

        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        (Path(leaf) / LEAF_TEMPORAL_NAME).unlink()
        raw = toc.read_leaf_temporal(leaf, cell_order, fields)
        got, route = leaf_contribution(leaf, cell_order, fields)
        assert route == "materialized"
        assert got[0] == raw[0]
        _same(got[1], raw[1])
        record = read_leaf_temporal_record(leaf)
        assert record["source"] == "sweep" and record["fields"] == sorted(fields)
        assert record["n_obs"] == int(raw[1].obs.sum())
        # The next pass finds it and never opens an array.
        monkeypatch.setattr(toc, "read_leaf_temporal", _boom)
        again, route = leaf_contribution(leaf, cell_order, fields)
        assert route == "record" and again[0] == got[0]
        _same(again[1], got[1])

    def test_materialize_off_reads_raw_and_writes_nothing(self, tmp_path):
        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        (Path(leaf) / LEAF_TEMPORAL_NAME).unlink()
        got, route = leaf_contribution(leaf, cell_order, fields, materialize=False)
        assert route == "raw" and got is not None
        assert not (Path(leaf) / LEAF_TEMPORAL_NAME).exists()

    def test_a_foreign_revision_record_is_preserved_and_bypassed(self, tmp_path):
        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        future = {"spec": "zagg-leaf-temporal/9", "word": "1"}
        (Path(leaf) / LEAF_TEMPORAL_NAME).write_text(json.dumps(future))
        got, route = leaf_contribution(leaf, cell_order, fields)
        assert route == "raw" and got is not None
        assert read_leaf_temporal_record(leaf) == future

    @pytest.mark.parametrize("damage", ["not json {", "n_obs", "fields", "unmarked"])
    def test_debris_and_stale_records_are_replaced(self, tmp_path, damage, caplog):
        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        path = Path(leaf) / LEAF_TEMPORAL_NAME
        if damage == "not json {":
            path.write_bytes(damage.encode())
        else:
            record = json.loads(path.read_text())
            if damage == "n_obs":
                record["n_obs"] += 1  # inconsistent with its own counts
            elif damage == "fields":
                record["fields"] = []  # predates the declared field
            else:
                record.pop("spec")  # claims no revision at all
            path.write_text(json.dumps(record))
        got, route = leaf_contribution(leaf, cell_order, fields)
        assert route == "materialized" and got is not None
        fresh = read_leaf_temporal_record(leaf)
        assert fresh["source"] == "sweep" and fresh["fields"] == sorted(fields)
        assert leaf_temporal_contribution(fresh)[0] == got[0]

    def test_a_materialization_failure_is_fail_open(self, tmp_path, monkeypatch, caplog):
        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        (Path(leaf) / LEAF_TEMPORAL_NAME).unlink()

        def refuse(*_a, **_k):
            raise OSError("access denied")

        monkeypatch.setattr(leaf_temporal, "write_leaf_temporal", refuse)
        with caplog.at_level("WARNING"):
            got, route = leaf_contribution(leaf, cell_order, fields)
        assert route == "raw" and got is not None
        assert "fail-open" in caplog.text
        assert not (Path(leaf) / LEAF_TEMPORAL_NAME).exists()

    def test_the_raw_route_reads_one_chunk_at_a_time(self, tmp_path, monkeypatch):
        """The memory shape issue #575 fixes: never a whole column, one chunk."""
        import zarr

        from zagg.coverage_toc import read_leaf_temporal

        root = _fixture_copy(tmp_path)
        leaf = _leaf_of(root)
        cell_order, fields = _declared(root)
        fed: list[int] = []
        add = LeafTemporalAccumulator.add_weighted
        monkeypatch.setattr(
            LeafTemporalAccumulator,
            "add_weighted",
            lambda self, w, x: (fed.append(len(w)), add(self, w, x)),
        )
        slices: list[int] = []
        getitem = zarr.Array.__getitem__

        def spy(self, key):
            if isinstance(key, slice):
                slices.append((key.stop or self.shape[0]) - (key.start or 0))
            return getitem(self, key)

        monkeypatch.setattr(zarr.Array, "__getitem__", spy)
        word, counts = read_leaf_temporal(leaf, cell_order, fields)
        expected = json.loads((SPEC_DATA / "temporal.expected.json").read_text())
        chunk_rows = expected["cells_per_chunk"]
        populated_chunks = {c["index"] // chunk_rows for c in expected["cells"]}
        # One feed per populated chunk, every slice at most one chunk long.
        assert len(fed) == len(populated_chunks) and slices
        assert max(slices) <= chunk_rows
        assert sum(fed) == sum(len(c["h_tdigest_times"]) for c in expected["cells"])
        assert int(counts.obs.sum()) == expected["root_coverage"]["obs_total"]

    def test_a_partitioned_pass_backfills_and_the_unpartitioned_pass_composes(
        self, tmp_path, monkeypatch
    ):
        """The operator path for a store written before the record (issue #575).

        A partitioned families pass cannot write the section (its finish is
        deferred) but it CAN materialize every leaf's record; the
        unpartitioned pass the runner tail fires then composes the section
        from records alone — no raw column read at all.
        """
        import zagg.coverage_toc as toc
        from zagg.coverage_toc import coverage_toc_counts
        from zagg.grids.morton import morton_word
        from zagg.hive import read_root_coverage
        from zagg.sweep import run_sweep

        root = _fixture_copy(tmp_path)
        other = "11214"
        shutil.copytree(_leaf_of(root), _leaf_of(root, other))
        for decimal in (SHARD, other):
            (Path(_leaf_of(root, decimal)) / LEAF_TEMPORAL_NAME).unlink()
        (Path(root) / "coverage.moc").unlink()
        (Path(root) / "coverage.toc").unlink()
        leaves = [(int(morton_word(SHARD)), None), (int(morton_word(other)), None)]
        for index in range(4):
            summary = run_sweep(
                root, leaves, families=["moc"], record=False, partition={"index": index, "of": 4}
            )
            assert summary["families"]["moc"].get("finish_deferred", True) is True
            assert "root_moc_written" not in summary["families"]["moc"]
        assert not (Path(root) / "coverage.moc").exists()
        for decimal in (SHARD, other):
            record = read_leaf_temporal_record(_leaf_of(root, decimal))
            assert record is not None and record["source"] == "sweep"
        monkeypatch.setattr(toc, "read_leaf_temporal", _boom)
        summary = run_sweep(root, leaves, families=["moc"], record=False)
        assert summary["families"]["moc"]["root_moc_written"] is True
        assert summary["families"]["moc"]["temporal_shards"] == 2
        envelope = read_root_coverage(root)
        assert set(envelope["temporal"]["shards"]) == {SHARD, other}
        expected = json.loads((SPEC_DATA / "temporal.expected.json").read_text())
        assert (
            int(coverage_toc_counts(envelope).obs.sum())
            == 2 * expected["root_coverage"]["obs_total"]
        )

    def test_refresh_reads_records_too(self, tmp_path, monkeypatch):
        import zagg.coverage_toc as toc
        from zagg.coverage import refresh_root_coverage

        root = _fixture_copy(tmp_path)
        monkeypatch.setattr(toc, "read_leaf_temporal", _boom)
        envelope = refresh_root_coverage(root)
        assert envelope["temporal"]["source"] == "refresh"
        assert set(envelope["temporal"]["shards"]) == {SHARD}
