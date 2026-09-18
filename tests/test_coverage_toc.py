"""The root coverage sidecar's temporal section — spec §10, issue #480.

Three things are asserted here that the §7 conformance suite cannot: the
order-independence of the root fold, the composition rules the GET-union-PUT
seam applies, and the byte-identity of a NON-temporal store's root object —
the promise that a store with no temporal channel is untouched by this
revision. The committed ``temporal/`` fixture is the real-store end of it;
``minimal/`` is the absence end.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

import numpy as np
import pytest
from mortie import span2toc, time2toc, toc_merge, toc_overlaps, toc_reduce

from zagg.coverage import refresh_root_coverage
from zagg.coverage_toc import (
    COVER_CAP,
    COVER_KEY,
    COVER_NAME,
    COVER_SPEC,
    TEMPORAL_COVER_ORDER,
    TEMPORAL_COVERAGE_SPEC,
    build_cover_section,
    build_temporal_section,
    cover_unchanged,
    cover_words,
    coverage_toc,
    coverage_toc_counts,
    load_cover,
    load_temporal_coverage,
    merge_cover_sections,
    merge_temporal_sections,
    quantize_words,
    read_cover,
    section_unchanged,
    shards_overlapping,
    temporal_fields,
    write_cover,
)
from zagg.hive import build_root_coverage, read_root_coverage, write_root_coverage
from zagg.leaf_temporal import count_words, cover_from_counts, merge_counts

SPEC_DATA = Path(__file__).parent / "data" / "spec"
#: A day on the toc scale, in internal ns — enough to keep the synthetic
#: leaves below in visibly distinct campaign clusters.
DAY_NS = 86_400 * 10**9
#: An arbitrary but realistic base instant on the §8 internal-ns scale.
BASE_NS = 5_344_000_000_000_000_000


def _leaf(seed: int, n: int = 12):
    """A synthetic per-leaf contribution: ``(word, counts)``.

    Shaped exactly like :func:`zagg.coverage_toc.read_leaf_temporal`'s return
    — the join over the leaf's observation words and its §10.3 counted cover,
    each instant weighted like a centroid (the worker's shape, where every
    word is an exact timestamp; the midpoint-counted range words of a sweep
    backfill are ``test_leaf_temporal``'s business).
    """
    rng = np.random.default_rng(seed)
    starts = np.sort(BASE_NS + seed * 40 * DAY_NS + rng.integers(0, 30 * DAY_NS, n)).astype(
        np.uint64
    )
    words = np.asarray(time2toc(starts), dtype=np.uint64)
    return int(toc_reduce(words)), count_words(words, rng.integers(1, 20, n))


def _total(contributions) -> int:
    return sum(int(part[1].obs.sum()) for parts in contributions.values() for part in parts)


def _contributions(seeds):
    return {f"1121{i}": [_leaf(s)] for i, s in enumerate(seeds)}


class TestSectionGrammar:
    """§10.1 — what a built section carries, and what it refuses to carry."""

    def test_required_keys_and_string_words(self):
        section = build_temporal_section(_contributions([1, 2, 3]), ["h_tdigest"])
        assert section["spec"] == TEMPORAL_COVERAGE_SPEC
        assert set(section) == {"spec", "source", "generated_at", "fields", "shards", "counts"}
        assert section["fields"] == ["h_tdigest"]
        assert all(isinstance(w, str) and w.isdigit() for w in section["shards"].values())
        assert section["counts"]["temporal_order"] == TEMPORAL_COVER_ORDER
        assert section["counts"]["cap"] == COVER_CAP
        assert section["counts"]["element"] == {"dtype": "uint64", "shape": [-1]}

    def test_an_empty_walk_builds_no_section(self):
        assert build_temporal_section({}, []) is None
        assert build_temporal_section({}, ["h_tdigest"]) is None

    def test_several_window_leaves_reduce_to_one_shard_word(self):
        a, b = _leaf(1), _leaf(2)
        section = build_temporal_section({"11213": [a, b]}, ["h_tdigest"])
        assert set(section["shards"]) == {"11213"}
        assert int(section["shards"]["11213"]) == int(toc_merge(a[0], b[0]))

    def test_count_conservation(self):
        contributions = _contributions([1, 2, 3])
        section = build_temporal_section(contributions, ["h_tdigest"])
        counts = coverage_toc_counts({"temporal": section})
        assert int(counts.obs.sum()) == section["counts"]["obs_total"] == _total(contributions)

    def test_the_root_cover_lies_inside_the_join_of_every_shard_word(self):
        # §10.5's parity relation at the root: the counts' cover sits inside
        # the quantized join of the shard words (equality only when every
        # word is an instant — these leaves carry midpoint-counted ranges).
        from mortie import toc2time

        section = build_temporal_section(_contributions([1, 2, 3]), ["h_tdigest"])
        counts = coverage_toc_counts({"temporal": section})
        cover, order = cover_from_counts(counts)
        join = toc_reduce(np.array([int(w) for w in section["shards"].values()], dtype=np.uint64))
        lo_c, hi_c = (int(x) for x in toc2time(int(toc_reduce(cover))))
        lo_w, hi_w = (int(x) for x in toc2time(int(toc_reduce(quantize_words([join], order)))))
        assert lo_w <= lo_c and hi_c <= hi_w


class TestOrderIndependence:
    """§10.3 — the fold is a per-word sum, so leaf order cannot matter."""

    def test_permuting_the_leaves_reproduces_the_section(self):
        contributions = _contributions([4, 7, 11, 13, 17])
        forward = build_temporal_section(contributions, ["h_tdigest"])
        keys = list(contributions)
        orders = [list(reversed(keys)), [keys[i] for i in (2, 0, 4, 1, 3)]]
        for order in orders:
            other = build_temporal_section({k: contributions[k] for k in order}, ["h_tdigest"])
            assert other["shards"] == forward["shards"]
            # Byte-for-byte: both buffers of the counts block.
            assert other["counts"] == forward["counts"]

    def test_the_root_counts_are_the_exact_sum_of_the_leaves(self):
        contributions = _contributions(range(1, 12))
        section = build_temporal_section(contributions, ["h_tdigest"])
        counts = coverage_toc_counts({"temporal": section})
        expect = merge_counts([part[1] for parts in contributions.values() for part in parts])
        np.testing.assert_array_equal(counts.words, expect.words)
        np.testing.assert_array_equal(counts.obs, expect.obs)
        assert int(counts.obs.sum()) == _total(contributions)
        assert section["counts"]["count"] == len(expect.words) <= COVER_CAP


class TestComposition:
    """§10.4 — how two sections meet at the GET-union-PUT seam."""

    def test_tier_one_unions_elementwise(self):
        a = build_temporal_section({"11211": [_leaf(1)], "11212": [_leaf(2)]}, ["h_tdigest"])
        b = build_temporal_section({"11212": [_leaf(3)], "11213": [_leaf(4)]}, ["h_tdigest"])
        merged = merge_temporal_sections(a, b)
        assert set(merged["shards"]) == {"11211", "11212", "11213"}
        assert int(merged["shards"]["11212"]) == int(
            toc_merge(int(a["shards"]["11212"]), int(b["shards"]["11212"]))
        )
        # The join is idempotent: re-merging changes nothing.
        assert merge_temporal_sections(merged, merged)["shards"] == merged["shards"]

    def test_a_partial_producer_drops_the_counts(self):
        whole = build_temporal_section({"11211": [_leaf(1)], "11212": [_leaf(2)]}, ["h_tdigest"])
        partial = build_temporal_section({"11213": [_leaf(3)]}, ["h_tdigest"])
        merged = merge_temporal_sections(whole, partial)
        # Neither side's map covers the union, so neither block can vouch for
        # the store — tier 1 stands, tier 2 goes (summing would double-count).
        assert set(merged["shards"]) == {"11211", "11212", "11213"}
        assert "counts" not in merged

    def test_a_whole_covering_producer_replaces_the_counts(self):
        old = build_temporal_section({"11211": [_leaf(1)]}, ["h_tdigest"])
        new = build_temporal_section({"11211": [_leaf(9)]}, ["h_tdigest"])
        merged = merge_temporal_sections(old, new)
        assert merged["counts"] == new["counts"] != old["counts"]

    def test_a_producer_with_no_section_leaves_the_standing_one_alone(self):
        standing = build_temporal_section(_contributions([1, 2]), ["h_tdigest"])
        assert merge_temporal_sections(standing, None) == standing
        assert merge_temporal_sections(None, standing) == standing
        assert merge_temporal_sections(None, None) is None

    def test_an_unknown_revision_on_the_standing_side_is_preserved(self):
        """§10.4: readers add revisions, they never drop them.

        The merge is the WRITE composer — a ``None`` return deletes the key —
        so an unreadable standing section must survive both a producer with
        nothing to say and one carrying this revision's section. Otherwise the
        older zagg in a mixed fleet is the one that wins.
        """
        good = build_temporal_section(_contributions([1]), ["h_tdigest"])
        future = {**good, "spec": "zagg-coverage-toc/2", "shards": {"99999": "1"}}
        assert merge_temporal_sections(future, None) == future
        assert merge_temporal_sections(future, good) == future
        # Incoming side: this revision cannot read it, so it contributes
        # nothing and the standing section stands.
        assert merge_temporal_sections(good, future) == good
        # Unmarked debris claims no revision and does not wedge the key shut.
        assert merge_temporal_sections({}, good) == good
        assert merge_temporal_sections({"shards": {"1": "2"}}, good) == good

    def test_section_unchanged(self):
        a = build_temporal_section({"11211": [_leaf(1)], "11212": [_leaf(2)]}, ["h_tdigest"])
        assert section_unchanged(a, None)
        assert section_unchanged(a, a)
        assert not section_unchanged(None, a)
        assert not section_unchanged(
            build_temporal_section({"11211": [_leaf(1)]}, ["h_tdigest"]), a
        )
        # A standing section this revision cannot read is preserved verbatim
        # by the merge, so composing over it changes nothing either — the
        # skip test must not churn the object on a mixed-version store.
        assert section_unchanged({"spec": "zagg-coverage-toc/2"}, a)

    def test_a_partial_producer_converges_instead_of_re_putting_forever(self):
        """The composed counts, not the built ones, are what the skip test sees.

        A producer that walked one shard of a two-shard store always builds a
        counts block, and §10.4 always drops it at the seam. Comparing the
        built section against the standing one therefore never converges;
        comparing the MERGE against it does, on the very next pass.
        """
        first = build_temporal_section({"11211": [_leaf(1)]}, ["h_tdigest"])
        second = build_temporal_section({"11212": [_leaf(2)]}, ["h_tdigest"])
        standing = merge_temporal_sections(first, second)
        assert "counts" not in standing  # neither producer covered the store
        assert second.get("counts") is not None  # ... yet the producer built one
        assert section_unchanged(standing, second)
        assert section_unchanged(standing, first)


class TestAbsence:
    """§10's standing posture: absence composes, and is never a refusal."""

    @pytest.mark.parametrize(
        "envelope",
        [
            None,
            {},
            {"spec": "morton-moc/1", "encoding": "ranges"},
            {"temporal": None},
            {"temporal": {"spec": "zagg-coverage-toc/2"}},
            "not a dict",
        ],
    )
    def test_readers_return_none_cleanly(self, envelope):
        assert load_temporal_coverage(envelope) is None
        assert coverage_toc(envelope) is None
        assert coverage_toc_counts(envelope) is None
        assert shards_overlapping(envelope, 0, 10**18) is None

    def test_a_block_whose_buffers_disagree_is_refused(self):
        """§10.3's MUST-checks: the two buffers, `count`, `obs_total`, the pin."""
        section = build_temporal_section(_contributions([1, 2]), ["h_tdigest"])
        block = section["counts"]
        for bad in (
            {"count": block["count"] + 1},
            {"count": None},
            {"obs": build_temporal_section(_contributions([3]), ["h"])["counts"]["obs"]},
            {"obs_total": block["obs_total"] + 1},
            {"temporal_order": TEMPORAL_COVER_ORDER + 1},
        ):
            envelope = {"temporal": {**section, "counts": {**block, **bad}}}
            # ValueError with the spec's own wording, never a bare TypeError
            # out of a coercion: §10.3 states each of these as a MUST on the
            # block, and the message is what an external reader implements.
            with pytest.raises(ValueError, match="counted cover declares"):
                coverage_toc_counts(envelope)

    def test_a_section_without_counts_still_prunes(self):
        section = build_temporal_section(_contributions([1, 2]), ["h_tdigest"])
        section.pop("counts")
        envelope = {"temporal": section}
        assert coverage_toc_counts(envelope) is None
        assert set(coverage_toc(envelope)) == set(section["shards"])

    def test_a_store_declaring_no_temporal_field_has_no_fields(self):
        manifest = json.loads((SPEC_DATA / "minimal" / "morton_hive.json").read_text())
        assert temporal_fields(manifest) == {}
        assert temporal_fields(None) == {}
        assert temporal_fields({}) == {}

    def test_a_temporal_store_declares_its_sibling(self):
        manifest = json.loads((SPEC_DATA / "temporal" / "morton_hive.json").read_text())
        fields = temporal_fields(manifest)
        assert set(fields) == {"h_tdigest"}
        assert fields["h_tdigest"]["sibling"] == "h_tdigest_times"


class TestPartialReadsDropTheShard:
    """§10.2 — a LISTED shard's word contains every instant in that shard.

    A word joined over whichever window leaves happened to read does not, so
    a failed read costs the shard its map entry. Absent reads as *unknown*
    (still a candidate); listed-but-partial reads as a promise the section
    cannot keep.
    """

    def test_a_failed_window_leaf_drops_its_whole_shard(self, monkeypatch):
        import zagg.coverage_toc as toc_module
        from zagg.sweep import MocFamily

        def reader(leaf, *args, **kwargs):
            if leaf.endswith("_2020.zarr"):
                raise OSError("truncated companion")
            return _leaf(1)

        monkeypatch.setattr(toc_module, "read_leaf_temporal", reader)
        family = MocFamily()
        family._temporal_fields = {"h_tdigest": {"sibling": "h_tdigest_times"}}
        family._accumulate_temporal("root", "11213", "root/11213_2019.zarr", {})
        assert "11213" in family._temporal
        family._accumulate_temporal("root", "11213", "root/11213_2020.zarr", {})
        assert "11213" not in family._temporal
        # A later window that DOES read cannot resurrect a half-read shard.
        family._accumulate_temporal("root", "11213", "root/11213_2021.zarr", {})
        assert "11213" not in family._temporal
        # ... and the failure is scoped to its own shard.
        family._accumulate_temporal("root", "11214", "root/11214_2019.zarr", {})
        assert "11214" in family._temporal


class TestPruning:
    """§10.2 — the tier-1 predicate, conservative by the grammar's own law."""

    def test_windows_select_the_right_shards(self):
        contributions = _contributions([1, 5])
        section = build_temporal_section(contributions, ["h_tdigest"])
        envelope = {"temporal": section}
        from mortie import toc2time

        for shard, parts in contributions.items():
            word = np.array([parts[0][0]], dtype=np.uint64)
            start, end = toc2time(parts[0][1].words)
            lo = int(np.min(np.asarray(start, np.uint64))) - DAY_NS
            hi = int(np.max(np.asarray(end, np.uint64))) + DAY_NS
            assert bool(np.asarray(toc_overlaps(word, lo, hi))[0])
            assert shard in shards_overlapping(envelope, lo, hi)

    def test_a_window_past_every_shard_selects_none(self):
        section = build_temporal_section(_contributions([1, 5]), ["h_tdigest"])
        far = BASE_NS + 10_000 * DAY_NS
        assert shards_overlapping({"temporal": section}, far, far + DAY_NS) == []


class TestCoverPruning:
    """§10.5 as a query surface: the word SET makes gap windows prune."""

    def _two_campaign_shard(self):
        """One shard, two pass clusters years apart, and its section pair."""
        rng = np.random.default_rng(3)
        a = BASE_NS + rng.integers(0, 6 * 3600 * 10**9, 40).astype(np.uint64)
        b = BASE_NS + 900 * DAY_NS + rng.integers(0, 6 * 3600 * 10**9, 40).astype(np.uint64)
        words = np.asarray(time2toc(np.concatenate([a, b])), dtype=np.uint64)
        contributions = {"11213": [(int(toc_reduce(words)), count_words(words))]}
        section = build_temporal_section(contributions, ["h"])
        cover = build_cover_section(contributions, ["h"], 4)
        return {"temporal": section}, cover

    def test_a_gap_window_prunes_under_the_cover_but_not_under_tier_one(self):
        envelope, cover = self._two_campaign_shard()
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        # Tier 1's single envelope word bridges the campaigns: candidate.
        assert shards_overlapping(envelope, gap0, gap1) == ["11213"]
        # The word set preserves the gap: pruned, no leaf opened.
        assert shards_overlapping(envelope, gap0, gap1, cover=cover) == []

    def test_a_campaign_window_still_selects_under_the_cover(self):
        envelope, cover = self._two_campaign_shard()
        for day in (0, 900):
            lo = BASE_NS + day * DAY_NS - DAY_NS
            hi = BASE_NS + day * DAY_NS + 2 * DAY_NS
            assert shards_overlapping(envelope, lo, hi, cover=cover) == ["11213"]

    def test_a_shard_the_cover_does_not_list_falls_back_to_tier_one(self):
        envelope, cover = self._two_campaign_shard()
        # A second shard in the section only: unknown at cover tier.
        other = _leaf(5)
        envelope["temporal"]["shards"]["11219"] = str(other[0])
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        got = shards_overlapping(envelope, gap0, gap1, cover=cover)
        assert "11213" not in got  # cover-listed: the gap prunes it
        # 11219's tier-1 word decides for itself (its data is elsewhere).
        expect = bool(toc_overlaps(np.asarray([other[0]], np.uint64), gap0, gap1)[0])
        assert ("11219" in got) == expect

    def test_a_shard_only_the_cover_lists_is_still_a_candidate(self):
        """§10.5: cover-only is "unknown, a candidate, never authoritative".

        The discriminating window is the GAP one — a campaign window is green
        whether the cover-only shard is an unconditional candidate or merely
        authoritative-and-hitting. The sibling may be arbitrarily older than
        the carrier here (a refresh with no temporal input replaces
        ``coverage.moc`` and leaves the standing cover alone), so its word
        set MUST NOT prune a shard tier 1 has never listed.
        """
        envelope, cover = self._two_campaign_shard()
        # Simulate the seam where the two maps disagree about the shard.
        del envelope["temporal"]["shards"]["11213"]
        lo, hi = BASE_NS - DAY_NS, BASE_NS + 2 * DAY_NS
        assert shards_overlapping(envelope, lo, hi, cover=cover) == ["11213"]
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        # Cover-listed AND tier-1-listed, this window prunes (the test above);
        # cover-only, the same window keeps it — that is the whole rule.
        assert shards_overlapping(envelope, gap0, gap1, cover=cover) == ["11213"]

    def test_an_empty_word_set_falls_back_to_the_tier_one_word(self):
        """A structurally legal ``count: 0`` block never prunes a real shard.

        Widening is the only cover direction (§10.5), so an empty set for a
        shard the carrier lists cannot be a true claim — it is malformed, and
        the shard degrades to its envelope word instead of vanishing from
        every window.
        """
        from zagg.coverage_toc import _encode_cover_block

        envelope, cover = self._two_campaign_shard()
        block = _encode_cover_block(np.asarray([], np.uint64), TEMPORAL_COVER_ORDER)
        empty = {**cover, "shards": {"11213": block}}
        assert empty["shards"]["11213"]["count"] == 0
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        assert shards_overlapping(envelope, gap0, gap1, cover=empty) == ["11213"]
        assert shards_overlapping(envelope, gap0, gap1) == ["11213"]

    def test_an_unknown_revision_cover_degrades_to_tier_one(self):
        envelope, cover = self._two_campaign_shard()
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        foreign = {**cover, "spec": "zagg-coverage-toc-cover/9"}
        assert shards_overlapping(envelope, gap0, gap1, cover=foreign) == ["11213"]
        assert shards_overlapping(envelope, gap0, gap1, cover=None) == ["11213"]

    @pytest.mark.parametrize("damage", ["count", "above_pin", "object_pin", "not_a_block"])
    def test_a_broken_block_degrades_that_shard_instead_of_raising(self, damage):
        """§10.5's MUST-refuse is scoped to the BLOCK, not to the query.

        The cover is an accelerator whose truth is in the leaves, so debris
        makes a shard unknown — it does not kill the caller's prune. The
        strict reading of the same bytes (``cover_words``) still raises;
        that is the conformance surface, asserted below.
        """
        from zagg.coverage_toc import _encode_cover_block

        envelope, cover = self._two_campaign_shard()
        block = dict(cover["shards"]["11213"])
        if damage == "count":
            block["count"] = 99
        elif damage == "above_pin":
            block = _encode_cover_block(
                cover_words(cover)["11213"], TEMPORAL_COVER_ORDER, TEMPORAL_COVER_ORDER
            )
            block["temporal_order"] = TEMPORAL_COVER_ORDER + 1
        elif damage == "not_a_block":
            block = "words"
        broken = {**cover, "shards": {"11213": block}}
        if damage == "object_pin":
            broken["temporal_order"] = "sixteen"
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        # Tier 1 decides this shard now, so the gap no longer prunes it.
        assert shards_overlapping(envelope, gap0, gap1, cover=broken) == ["11213"]
        assert shards_overlapping(envelope, gap0, gap1) == ["11213"]
        lo, hi = BASE_NS - DAY_NS, BASE_NS + 2 * DAY_NS
        assert shards_overlapping(envelope, lo, hi, cover=broken) == ["11213"]
        with pytest.raises(ValueError):
            cover_words(broken)

    def test_a_cover_at_another_shard_order_is_ignored_wholesale(self, caplog):
        """§10.4/§10.5's order gate, on the read side (:func:`merge_cover_sections`).

        A re-shard moves the carrier while a producer with no temporal
        contribution leaves the sibling standing. D1 ids at two orders are
        not comparable, so the foreign-order cover must neither decide a
        shard nor contribute ids of its own — a leaf at ``1121344`` does not
        exist in an order-4 store.
        """
        envelope = json.loads((SPEC_DATA / "temporal" / "coverage.moc").read_text())
        cover = json.loads((SPEC_DATA / "temporal" / COVER_NAME).read_text())
        assert envelope["order"] == cover["order"] == 4
        lo, hi = 0, (1 << 63) - 1
        bad = {**cover, "order": 6, "shards": {"1121344": cover["shards"]["11213"]}}
        with caplog.at_level(logging.WARNING, logger="zagg.coverage_toc"):
            got = shards_overlapping(envelope, lo, hi, cover=bad)
        assert got == shards_overlapping(envelope, lo, hi, cover=None) == ["11213"]
        assert "order 6" in caplog.text and "order 4" in caplog.text

    def test_the_cover_never_under_reports_the_true_instants(self):
        envelope, cover = self._two_campaign_shard()
        # The helper's own instants (same seed): windows straddling any real
        # instant must always select the shard.
        rng = np.random.default_rng(3)
        a = BASE_NS + rng.integers(0, 6 * 3600 * 10**9, 40).astype(np.uint64)
        b = BASE_NS + 900 * DAY_NS + rng.integers(0, 6 * 3600 * 10**9, 40).astype(np.uint64)
        for t in (*a[::7], *b[::7]):
            assert shards_overlapping(envelope, int(t) - 1, int(t) + 1, cover=cover) == ["11213"]

    def _mixed_shape(self):
        """One store touching every arm: word set, empty, debris, tier 1, cover-only."""
        from zagg.coverage_toc import _encode_cover_block

        rng = np.random.default_rng(11)
        contributions = {}
        for i, key in enumerate(("11213", "11214", "11215", "11216")):
            t = np.concatenate(
                [
                    BASE_NS + off * DAY_NS + rng.integers(0, 6 * 3600 * 10**9, 30)
                    for off in (30 * i, 700 + 40 * i)
                ]
            ).astype(np.uint64)
            words = np.asarray(time2toc(t), dtype=np.uint64)
            contributions[key] = [(int(toc_reduce(words)), count_words(words))]
        envelope = {"temporal": build_temporal_section(contributions, ["h"])}
        cover = build_cover_section(contributions, ["h"], 4)
        cover["shards"] = dict(cover["shards"])
        # 11213/11214 keep their real word sets; the rest are the seams.
        empty = _encode_cover_block(np.asarray([], np.uint64), TEMPORAL_COVER_ORDER)
        cover["shards"]["11215"] = empty
        cover["shards"]["11216"] = {**cover["shards"]["11216"], "count": 99}
        envelope["temporal"]["shards"]["11217"] = str(_leaf(5)[0])
        cover["shards"]["11218"] = cover["shards"]["11213"]
        return envelope, cover

    @staticmethod
    def _per_shard(envelope, lo, hi, cover):
        """The pre-batching formulation: one ``toc_overlaps`` call per shard."""
        from zagg.coverage_toc import _query_word_set

        words = coverage_toc(envelope) or {}
        section = load_cover(cover)
        blocks = (section or {}).get("shards") or {}
        hits = {}
        for key in sorted(words.keys() | blocks.keys()):
            if key not in words:
                hits[key] = True
                continue
            cover_set = _query_word_set(section, key) if key in blocks else None
            if cover_set is not None and len(cover_set):
                hits[key] = bool(np.any(np.atleast_1d(toc_overlaps(cover_set, lo, hi))))
            else:
                hits[key] = bool(toc_overlaps(np.asarray([words[key]], np.uint64), lo, hi)[0])
        return sorted(k for k, hit in hits.items() if hit)

    def test_the_batched_query_answers_exactly_as_the_per_shard_loop(self):
        """One concatenated call plus a segmented OR == one call per shard.

        The cover arm answers every word-set shard in a single FFI hop, which
        is only sound if no segment can absorb its neighbour's verdict. The
        empty and undecodable blocks never enter the concatenation (they
        degrade to tier 1 first), so the shape below carries both alongside
        two real word sets, a tier-1-only shard and a cover-only one.
        """
        envelope, cover = self._mixed_shape()
        assert set(cover["shards"]) == {"11213", "11214", "11215", "11216", "11218"}
        tier = coverage_toc(envelope)
        assert "11217" in tier and "11218" not in tier
        answers = set()
        for i in range(60):
            lo = BASE_NS + i * 20 * DAY_NS
            hi = lo + DAY_NS
            got = shards_overlapping(envelope, lo, hi, cover=cover)
            assert got == self._per_shard(envelope, lo, hi, cover)
            answers.add(tuple(got))
        # The sweep discriminates — this is not 60 copies of one answer.
        assert len(answers) > 3
        # ...only the cover-only shard is unconditional, and the two degraded
        # blocks still answer (from tier 1) rather than vanishing everywhere.
        assert all("11218" in a for a in answers)
        assert any("11215" in a for a in answers) and any("11216" in a for a in answers)

    def test_no_information_stays_none(self):
        assert shards_overlapping({}, 0, 1, cover=None) is None
        assert shards_overlapping({}, 0, 1, cover={"spec": "junk"}) is None

    def test_a_section_less_carrier_answers_from_the_cover(self):
        """``None`` means no information — a standing cover IS information.

        This is the state the refresh's "no ``else`` arm" leaves behind: a
        walk with no temporal input rewrites ``coverage.moc`` without a
        section and leaves the sibling alone. Every shard is cover-only
        there, so every shard is a candidate (§10.5) — not ``None``, and not
        pruned by a word set that may be arbitrarily old.
        """
        envelope, cover = self._two_campaign_shard()
        section_less = {k: v for k, v in envelope.items() if k != "temporal"}
        assert shards_overlapping(section_less, BASE_NS, BASE_NS + DAY_NS) is None
        lo, hi = BASE_NS - DAY_NS, BASE_NS + 2 * DAY_NS
        assert shards_overlapping(section_less, lo, hi, cover=cover) == ["11213"]
        gap0, gap1 = BASE_NS + 400 * DAY_NS, BASE_NS + 401 * DAY_NS
        assert shards_overlapping(section_less, gap0, gap1, cover=cover) == ["11213"]


class TestOnCommittedStores:
    """End to end, on the §7 fixtures: the writer, and the absence pin."""

    def _copy(self, tmp_path, name):
        dst = tmp_path / name
        shutil.copytree(SPEC_DATA / name, dst)
        return str(dst)

    def _clone_shard(self, root, src="11213", dst="11214"):
        """Give ``root`` a second shard, cloned from its committed leaf.

        The §7 ``temporal/`` fixture ships ONE shard, which is exactly the
        shape that hides the composition seam: a single-shard producer is
        always whole-covering, so its counts survive the merge and the skip
        test converges by accident.
        """
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path

        src_leaf = Path(shard_leaf_path(root, int(morton_word(src))))
        dst_leaf = Path(shard_leaf_path(root, int(morton_word(dst))))
        dst_leaf.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src_leaf, dst_leaf)
        return [(int(morton_word(src)), None)], [(int(morton_word(dst)), None)]

    def test_refresh_rebuilds_the_section_from_its_own_walk(self, tmp_path):
        root = self._copy(tmp_path, "temporal")
        committed = json.loads((SPEC_DATA / "temporal" / "coverage.moc").read_text())
        envelope = refresh_root_coverage(root)
        assert envelope["temporal"]["source"] == "refresh"
        # Same walk, same words: only the carrier's provenance differs.
        assert envelope["temporal"]["shards"] == committed["temporal"]["shards"]
        assert envelope["temporal"]["counts"] == committed["temporal"]["counts"]
        # The §10.5 sibling rebuilds from the same walk: same word sets, its
        # own provenance, and the carrier's marker points at it.
        committed_cover = json.loads((SPEC_DATA / "temporal" / COVER_NAME).read_text())
        rebuilt = read_cover(root)
        assert rebuilt["source"] == "refresh"
        assert rebuilt["shards"] == committed_cover["shards"]
        assert envelope["temporal"][COVER_KEY] == rebuilt["spec"] == COVER_SPEC

    def test_refresh_preserves_a_future_sibling_and_points_at_it(self, tmp_path):
        """§10.4's succession rule at object level, on the refresh's replace path.

        The escape hatch is authoritative about the leaves, never about a
        revision it cannot read: the standing object stays, and the marker
        the carrier publishes is that object's own ``spec`` — the truth about
        what the store actually carries.
        """
        root = self._copy(tmp_path, "temporal")
        future = {"spec": "zagg-coverage-toc-cover/9", "shards": {}}
        (Path(root) / COVER_NAME).write_text(json.dumps(future, indent=1))
        envelope = refresh_root_coverage(root)
        assert read_cover(root) == future
        assert envelope["temporal"][COVER_KEY] == future["spec"]

    def test_refresh_over_an_unstamped_store_discards_both_objects(self, tmp_path):
        """The one arm §10.5's delete licence names by hand.

        No stamped leaf remains, so the carrier goes — and the sibling, whose
        ids are keyed to a carrier that no longer exists, goes with it.
        """
        root = self._copy(tmp_path, "temporal")
        shutil.rmtree(Path(root) / "1")
        assert refresh_root_coverage(root) is None
        assert not (Path(root) / "coverage.moc").exists()
        assert not (Path(root) / COVER_NAME).exists()

    def test_a_sweep_writes_the_section_the_fixture_committed(self, tmp_path):
        from zagg.grids.morton import morton_word
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "temporal")
        (Path(root) / "coverage.moc").unlink()
        (Path(root) / COVER_NAME).unlink()
        leaves = [(int(morton_word("11213")), None)]
        summary = run_sweep(root, leaves, families=["moc"], record=False)
        assert summary["families"]["moc"]["temporal_shards"] == 1
        assert summary["families"]["moc"]["cover_shards"] == 1
        committed = json.loads((SPEC_DATA / "temporal" / "coverage.moc").read_text())
        written = read_root_coverage(root)
        assert written["temporal"]["shards"] == committed["temporal"]["shards"]
        assert written["temporal"]["counts"] == committed["temporal"]["counts"]
        # The §10.5 sibling lands beside it, byte-equal in content to the
        # committed fixture's, and the section's marker names its revision.
        committed_cover = json.loads((SPEC_DATA / "temporal" / COVER_NAME).read_text())
        written_cover = read_cover(root)
        assert written_cover["shards"] == committed_cover["shards"]
        assert written["temporal"][COVER_KEY] == written_cover["spec"] == COVER_SPEC
        # Idempotence: a second pass over an unchanged tree writes nothing.
        again = run_sweep(root, leaves, families=["moc"], record=False)
        assert again["families"]["moc"]["root_moc_written"] is False
        # Self-heal — the reason `cover_unchanged` is in the skip test at all.
        # The carrier is current and the sibling is gone (or debris): a sweep
        # that tested only the carrier would skip and leave the store with a
        # `cover` marker pointing at nothing.
        for damage in (None, b"not json {"):
            path = Path(root) / COVER_NAME
            path.unlink() if damage is None else path.write_bytes(damage)
            healed = run_sweep(root, leaves, families=["moc"], record=False)
            assert healed["families"]["moc"]["root_moc_written"] is True
            assert read_cover(root)["shards"] == committed_cover["shards"]

    def test_a_sweep_over_a_future_sibling_converges_instead_of_lying(self, tmp_path):
        """§10.4's succession rule, applied to the §10.5 marker.

        ``write_cover`` never replaces a standing cover at a revision it
        cannot read, so on such a store the standing marker is the truth
        about what the sibling carries. A ``zagg-coverage-toc-cover/1``
        producer that downgraded the marker would advertise a revision the
        object does not have, and would never converge: the sweep would
        stamp ``/1``, the refresh would stamp back the object's own ``/9``,
        and the pair would re-PUT ``coverage.moc`` on every pass.
        """
        from zagg.grids.morton import morton_word
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "temporal")
        future = "zagg-coverage-toc-cover/9"
        (Path(root) / COVER_NAME).write_text(json.dumps({"spec": future, "shards": {}}, indent=1))
        moc = Path(root) / "coverage.moc"
        envelope = json.loads(moc.read_text())
        envelope["temporal"][COVER_KEY] = future
        moc.write_text(json.dumps(envelope, indent=1))
        leaves = [(int(morton_word("11213")), None)]
        for _ in range(2):
            summary = run_sweep(root, leaves, families=["moc"], record=False)
            assert summary["families"]["moc"]["root_moc_written"] is False
        assert read_root_coverage(root)["temporal"][COVER_KEY] == future
        assert read_cover(root)["spec"] == future

    def test_a_truncated_companion_is_refused(self, tmp_path):
        """§1.1 row alignment, at ARRAY level (issue #452's failure shape).

        A companion with fewer rows than its payload aligns row for row over
        its own length, so the per-cell check never fires — the leaf would
        join a prefix of its cells and be published as whole.
        """
        import zarr

        from zagg.coverage_toc import read_leaf_temporal
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path

        root = self._copy(tmp_path, "temporal")
        manifest = json.loads((Path(root) / "morton_hive.json").read_text())
        fields = temporal_fields(manifest)
        leaf = shard_leaf_path(root, int(morton_word("11213")))
        group = zarr.open_group(leaf, path=str(manifest["cell_order"]), mode="a", zarr_format=3)
        rows = group["h_tdigest_times"].shape[0]
        group["h_tdigest_times"].resize((rows - 1,))
        with pytest.raises(ValueError, match="row-aligned"):
            read_leaf_temporal(leaf, int(manifest["cell_order"]), fields)

    def test_refresh_drops_only_the_shard_whose_leaf_failed(self, tmp_path, monkeypatch):
        import zagg.coverage_toc as toc_module

        root = self._copy(tmp_path, "temporal")
        self._clone_shard(root)
        real = toc_module.read_leaf_temporal

        def reader(leaf, *args, **kwargs):
            if "11214" in leaf:
                raise OSError("truncated companion")
            return real(leaf, *args, **kwargs)

        monkeypatch.setattr(toc_module, "read_leaf_temporal", reader)
        envelope = refresh_root_coverage(root)
        # The spatial walk still lists both shards; the temporal map lists
        # only the one it could read whole (§10.2's unknown-not-empty rule).
        assert set(envelope["temporal"]["shards"]) == {"11213"}

    def test_the_shard_word_unions_across_every_temporal_field(self, tmp_path):
        """§10.2's headline rule: coverage is "any data", not "data in field X".

        The committed fixture declares ONE temporal field, so the union is
        invisible on it. A second field is grafted onto a copy of the leaf —
        the same payload rows under a companion whose words sit in a different
        campaign — and the shard word must be the join across both.
        """
        import zarr
        from mortie import time2toc

        from zagg.coverage_toc import read_leaf_temporal
        from zagg.grids.morton import morton_word
        from zagg.hive import shard_leaf_path

        root = self._copy(tmp_path, "temporal")
        manifest = json.loads((Path(root) / "morton_hive.json").read_text())
        order = int(manifest["cell_order"])
        leaf = shard_leaf_path(root, int(morton_word("11213")))
        group = zarr.open_group(leaf, path=str(order), mode="a", zarr_format=3)
        payload, sibling = group["h_tdigest"], group["h_tdigest_times"]
        # A companion of the same per-row width, so §1.1 alignment holds, but
        # carrying instants a whole campaign away from the committed ones.
        far = np.empty(sibling.shape[0], dtype=object)
        for i, row in enumerate(sibling[:]):
            width = 0 if row is None else len(row) // 8
            far[i] = np.array(
                [int(time2toc(BASE_NS + 20_000 * DAY_NS + j * DAY_NS)) for j in range(width)],
                dtype="<u8",
            ).tobytes()
        for name, values in (("g_tdigest", payload[:]), ("g_tdigest_times", far)):
            group.create_array(
                name,
                shape=payload.shape,
                chunks=payload.chunks,
                dtype=payload.metadata.data_type,
                overwrite=True,
            )[:] = values

        fields = temporal_fields(manifest)
        second = {"g_tdigest": {**fields["h_tdigest"], "sibling": "g_tdigest_times"}}
        one = read_leaf_temporal(leaf, order, fields)
        other = read_leaf_temporal(leaf, order, second)
        both = read_leaf_temporal(leaf, order, {**fields, **second})
        assert both[0] == int(toc_reduce(np.array([one[0], other[0]], dtype=np.uint64)))
        assert both[0] not in (one[0], other[0])  # neither field alone covers it
        section = build_temporal_section({"11213": [both]}, ["g_tdigest", "h_tdigest"])
        assert int(section["shards"]["11213"]) == both[0]
        # §10.3's once-per-field counting rule, seen from the count side.
        assert section["counts"]["obs_total"] == int(one[1].obs.sum()) + int(other[1].obs.sum())

    def test_a_manifest_without_a_cell_order_publishes_no_section(self, tmp_path, caplog):
        """A required key missing is a broken manifest, not group ``"0"``.

        Defaulting either asks for a group that does not exist — logged as an
        ordinary missing contribution, which hides the real problem — or, on a
        store whose cell order really is 0, reads the wrong group.
        """
        from zagg.grids.morton import morton_word
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "temporal")
        path = Path(root) / "morton_hive.json"
        manifest = json.loads(path.read_text())
        del manifest["cell_order"]
        path.write_text(json.dumps(manifest, indent=1))
        (Path(root) / "coverage.moc").unlink()
        standing_cover = (Path(root) / COVER_NAME).read_bytes()
        with caplog.at_level("WARNING"):
            summary = run_sweep(
                root, [(int(morton_word("11213")), None)], families=["moc"], record=False
            )
            assert "temporal_shards" not in summary["families"]["moc"]
            assert "temporal" not in read_root_coverage(root)
            assert refresh_root_coverage(root).get("temporal") is None
        assert caplog.text.count("cell_order") >= 2
        # Neither producer touches the §10.5 sibling on this evidence: a
        # manifest defect proves nothing about the leaves, so "no temporal
        # contribution leaves the standing object untouched" governs — and
        # both producers have to answer it the same way, or an operator's
        # refresh destroys a perfectly good cover the sweep kept.
        assert (Path(root) / COVER_NAME).read_bytes() == standing_cover

    def test_refresh_never_deletes_the_section_when_every_leaf_fails(self, tmp_path, monkeypatch):
        """The escape hatch must not be the thing that destroys the section.

        Fail-open per leaf is fail-DESTRUCTIVE in aggregate: refresh PUTs its
        envelope outright, so an all-failed walk would publish a root object
        with the ``temporal`` key gone — during exactly the incident an
        operator reached for refresh to repair.
        """
        import zagg.coverage_toc as toc_module

        root = self._copy(tmp_path, "temporal")
        standing = json.loads((Path(root) / "coverage.moc").read_text())["temporal"]
        standing_cover = read_cover(root)

        def reader(*args, **kwargs):
            raise OSError("credentials expired mid-walk")

        monkeypatch.setattr(toc_module, "read_leaf_temporal", reader)
        envelope = refresh_root_coverage(root)
        assert envelope["ranges"]  # the spatial refresh still succeeded
        assert envelope["temporal"] == standing
        assert coverage_toc(envelope) == coverage_toc({"temporal": standing})
        # The §10.5 sibling rides the same seam: an all-failed walk composes
        # `None` into the standing object and re-publishes it verbatim.
        assert read_cover(root) == standing_cover

    def test_refresh_preserves_a_future_section_without_stamping_it(self, tmp_path, monkeypatch):
        """§10.4's "verbatim" is verbatim — the refresh writes no key into it.

        The merge hands back a standing section at a revision this producer
        cannot read; that producer may define ``cover`` differently, or use
        it for something else entirely. Stamping the §10.5 marker into it
        would be this revision editing another's bytes at the one seam a
        mixed-version fleet actually meets.
        """
        import zagg.coverage_toc as toc_module

        root = self._copy(tmp_path, "temporal")
        moc = Path(root) / "coverage.moc"
        envelope = json.loads(moc.read_text())
        envelope["temporal"]["spec"] = "zagg-coverage-toc/9"
        envelope["temporal"].pop(COVER_KEY, None)
        moc.write_text(json.dumps(envelope, indent=1))
        standing = envelope["temporal"]

        def reader(*args, **kwargs):
            raise OSError("credentials expired mid-walk")

        monkeypatch.setattr(toc_module, "read_leaf_temporal", reader)
        rebuilt = refresh_root_coverage(root)["temporal"]
        assert rebuilt == standing  # byte for byte, marker included
        assert COVER_KEY not in rebuilt

    def test_refresh_composes_a_partial_rebuild_with_the_standing_section(
        self, tmp_path, monkeypatch
    ):
        import zagg.coverage_toc as toc_module

        root = self._copy(tmp_path, "temporal")
        self._clone_shard(root)
        refresh_root_coverage(root)  # both shards land in the standing section
        standing_cover = read_cover(root)
        real = toc_module.read_leaf_temporal

        def reader(leaf, *args, **kwargs):
            if "11214" in leaf:
                raise OSError("truncated companion")
            return real(leaf, *args, **kwargs)

        monkeypatch.setattr(toc_module, "read_leaf_temporal", reader)
        envelope = refresh_root_coverage(root)
        # The shard the walk could not read keeps the word the last whole walk
        # published: a partial rebuild composes, it does not overwrite.
        assert set(envelope["temporal"]["shards"]) == {"11213", "11214"}
        # The §10.5 sibling composes across the identical seam — the failed
        # shard's BLOCK survives, so the cover does not shrink to the half of
        # the store this walk happened to read.
        composed = read_cover(root)
        assert set(composed["shards"]) == {"11213", "11214"}
        np.testing.assert_array_equal(
            cover_words(composed)["11214"], cover_words(standing_cover)["11214"]
        )

    def test_a_second_pass_over_a_multi_shard_store_writes_nothing(self, tmp_path):
        """Sweep idempotence where the seam actually bites (§10.4).

        Two shards, two incremental sweeps: neither producer covers the store,
        so the composed section carries no counts while every pass keeps
        building a block. The skip test has to converge on what was WRITTEN,
        or the fleet re-PUTs a byte-identical root object forever.
        """
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "temporal")
        (Path(root) / "coverage.moc").unlink()
        a, b = self._clone_shard(root)
        for leaves in (a, b):
            summary = run_sweep(root, leaves, families=["moc"], record=False)
            assert summary["families"]["moc"]["root_moc_written"] is True
        written = read_root_coverage(root)
        assert set(written["temporal"]["shards"]) == {"11213", "11214"}
        assert "counts" not in written["temporal"]
        # The §10.5 sibling accumulated BOTH incremental producers' blocks —
        # the convergence `cover_unchanged` is tested on below.
        assert set(read_cover(root)["shards"]) == {"11213", "11214"}
        for leaves in (b, a):
            again = run_sweep(root, leaves, families=["moc"], record=False)
            assert again["families"]["moc"]["root_moc_written"] is False
        assert read_root_coverage(root)["temporal"] == written["temporal"]

    def test_parity_holds_on_every_shard_the_sweep_writes(self, tmp_path):
        """§10.5's standing invariant, on the production writer's own output.

        For every shard listed by BOTH root objects:
        ``toc_reduce(cover words) == toc_reduce(quantize(tier-1 word, o))``
        at the shard's effective order — the cross-object consistency claim
        the spec tells readers they MAY check cheaply. Asserted over the
        multi-shard incremental sweep, so the §10.4/§10.5 seams (not just a
        single-producer PUT) are inside the claim.

        The passes OVERLAP deliberately: two disjoint producers only ever
        exercise ``merge_cover_sections``' carry-over arm ("a shard on one
        side carries over unchanged"). The third pass re-covers both shards,
        so every block here has been through the per-shard
        union-then-requantize arm on the production writer's own output —
        the arm where parity could actually break.
        """
        from zagg.coverage_toc import _object_pin
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "temporal")
        (Path(root) / "coverage.moc").unlink()
        (Path(root) / COVER_NAME).unlink()
        a, b = self._clone_shard(root)
        for leaves in (a, b, a + b):
            run_sweep(root, leaves, families=["moc"], record=False)
        tier1 = coverage_toc(read_root_coverage(root))
        cover = read_cover(root)
        decoded = cover_words(cover)
        assert set(decoded) == set(tier1) == {"11213", "11214"}
        for shard, words in decoded.items():
            order = cover["shards"][shard].get("temporal_order", _object_pin(cover))
            lhs = int(toc_reduce(words))
            rhs = int(toc_reduce(quantize_words([tier1[shard]], order)))
            assert lhs == rhs, shard

    def test_a_non_temporal_store_writes_byte_identical_bytes(self, tmp_path):
        """The §10 absence promise, as bytes.

        A store declaring no temporal field must produce EXACTLY the root
        object a pre-#480 zagg produced: no key added, no key reordered.
        """
        from zagg.grids.morton import morton_word
        from zagg.sweep import run_sweep

        root = self._copy(tmp_path, "minimal")
        summary = run_sweep(
            root, [(int(morton_word("11213")), None)], families=["moc"], record=False
        )
        assert "temporal_shards" not in summary["families"]["moc"]
        raw = (Path(root) / "coverage.moc").read_bytes()
        envelope = json.loads(raw)
        assert "temporal" not in envelope
        assert coverage_toc(envelope) is None
        # The reference: the same carrier built with the §10 parameter omitted
        # entirely — the pre-#480 call — serialized the pre-#480 way. Equal
        # bytes is the whole promise.
        reference = build_root_coverage([morton_word("11213")], 4, source="sweep")
        reference["generated_at"] = envelope["generated_at"]
        assert json.dumps(reference, indent=1).encode() == raw
        # And a re-write through the GET-union-PUT seam stays temporal-free.
        assert "temporal" not in write_root_coverage(root, reference)
        assert "temporal" not in json.loads((Path(root) / "coverage.moc").read_bytes())
        # And the §10.5 sibling was never created: absence composes.
        assert not (Path(root) / COVER_NAME).exists()


#: One §10.5 cover-order bucket, in internal ns (order 24 -> span 2^39).
BUCKET_NS = 1 << (63 - TEMPORAL_COVER_ORDER)


class TestQuantization:
    """§10.5's quantization law: widening only, gap-preserving, commuting."""

    def _instants(self, n_days=49, per_day=200, seed=489):
        # The source shape §10.5's pin is chosen for: a PASS is ~1 s wide
        # (issue #575), so each day's instants fall inside one second.
        rng = np.random.default_rng(seed)
        days = np.sort(rng.choice(2_700, n_days, replace=False))
        ts = np.concatenate(
            [BASE_NS + int(d) * DAY_NS + rng.integers(0, 10**9, per_day) for d in days]
        ).astype(np.uint64)
        return days, ts

    def test_the_cover_contains_every_instant(self):
        from mortie import toc2time

        _days, ts = self._instants()
        cover = quantize_words(time2toc(ts))
        start, end = toc2time(cover)
        assert all(np.any((start <= t) & (t < end)) for t in ts[::97])

    def test_buckets_are_aligned_at_the_pinned_order(self):
        from mortie import toc2time

        _days, ts = self._instants()
        start, end = toc2time(quantize_words(time2toc(ts)))
        assert np.all(np.asarray(start, np.uint64) % BUCKET_NS == 0)
        assert np.all(np.asarray(end, np.uint64) % BUCKET_NS == 0)

    def test_a_range_word_widens_to_its_buckets(self):
        word = span2toc(BASE_NS + 10, BASE_NS + BUCKET_NS)
        from mortie import toc2time

        start, end = toc2time(quantize_words([word]))
        assert int(start[0]) == (BASE_NS // BUCKET_NS) * BUCKET_NS
        assert int(end[0]) - int(start[0]) >= 2 * BUCKET_NS  # widened, never shrunk

    def test_the_pass_day_shape_compresses_to_the_day_clusters(self):
        # The CA store's shard 3231242244 shape, scaled: millions of exact
        # timestamps clustering into ~49 distinct pass-days must land at
        # ~one word per cluster, not one per instant.
        days, ts = self._instants()
        cover = quantize_words(time2toc(ts))
        assert len(cover) <= len(days)

    def test_days_far_from_any_pass_stay_uncovered(self):
        # Named for what it checks: days at least 3 clear of any pass, i.e.
        # well past the two-span floor the law above pins exactly.
        days, ts = self._instants()
        cover = quantize_words(time2toc(ts))
        far = [int(d) for d in range(2_700) if np.abs(days - d).min() >= 2][:60]
        assert far
        for d in far:
            q0, q1 = BASE_NS + d * DAY_NS, BASE_NS + (d + 1) * DAY_NS
            assert not np.any(toc_overlaps(cover, q0, q1))

    @pytest.mark.parametrize(
        ("frac", "spans", "survives"),
        [
            (0.9, 1.0, False),  # abutting buckets coalesce: one span never survives
            (0.0, 1.0, False),
            (0.0, 1.99, False),  # the [1, 2) band is alignment-decided, not length-decided
            (0.5, 1.5, True),
            (0.0, 2.0, True),  # two whole spans always leave a bucket free
            (0.9, 2.0, True),
        ],
    )
    def test_a_gap_survives_iff_it_holds_a_whole_aligned_bucket(self, frac, spans, survives):
        # §10.5's only resolution promise, pinned as bytes in both directions:
        # the guaranteed floor is TWO bucket spans (2 * 2^39 ns ~ 18.3 min), not one.
        t0 = (BASE_NS // BUCKET_NS) * BUCKET_NS + int(frac * BUCKET_NS)
        t1 = t0 + int(spans * BUCKET_NS)
        cover = quantize_words(time2toc(np.array([t0, t1], dtype=np.uint64)))
        assert (len(cover) == 2) is survives
        # Either way the law never false-negatives on the instants themselves.
        assert np.any(toc_overlaps(cover, t0, t0 + 1))
        assert np.any(toc_overlaps(cover, t1, t1 + 1))

    def test_quantization_commutes_with_union(self):
        _days, ts = self._instants()
        words = time2toc(ts)
        a, b = words[: len(words) // 2], words[len(words) // 2 :]
        joint = quantize_words(words)
        parts = quantize_words(np.concatenate([quantize_words(a), quantize_words(b)]))
        assert np.array_equal(joint, parts)

    def test_quantization_commutes_with_the_envelope_join(self):
        _days, ts = self._instants()
        words = time2toc(ts)
        lhs = int(toc_reduce(quantize_words(words)))
        rhs = int(toc_reduce(quantize_words([toc_reduce(words)])))
        assert lhs == rhs

    def test_idempotent_at_the_same_order(self):
        _days, ts = self._instants()
        once = quantize_words(time2toc(ts))
        assert np.array_equal(once, quantize_words(once))

    def test_the_scale_ceiling_clamps_without_losing_containment(self):
        from mortie import TOC_MAX_NS, toc2time

        word = span2toc(TOC_MAX_NS - 2 * BUCKET_NS, TOC_MAX_NS - 10**9)
        start, end = toc2time(quantize_words([word]))
        w_start, w_end = toc2time(np.asarray([word], np.uint64))
        assert int(start[0]) <= int(w_start[0]) and int(w_end[0]) <= int(end[0])

    def test_orders_outside_the_grammar_are_refused(self):
        with pytest.raises(ValueError, match="temporal order"):
            quantize_words([time2toc(BASE_NS)], 32)
        with pytest.raises(ValueError, match="temporal order"):
            quantize_words([time2toc(BASE_NS)], -1)

    def test_empty_in_empty_out(self):
        assert len(quantize_words(np.array([], dtype=np.uint64))) == 0


class TestCoverSection:
    """§10.5's object grammar, from the same contributions the section folds."""

    def test_grammar_and_decode_round_trip(self):
        contributions = _contributions([1, 2, 3])
        section = build_cover_section(contributions, ["h_tdigest"], 4)
        assert section["spec"] == COVER_SPEC
        assert section["order"] == 4
        assert section["temporal_order"] == TEMPORAL_COVER_ORDER
        assert section["cap"] == COVER_CAP
        assert section["element"] == {"dtype": "uint64", "shape": [-1]}
        decoded = cover_words(section)
        assert set(decoded) == set(contributions)
        for decimal, parts in contributions.items():
            expect, _order = cover_from_counts(merge_counts([p[1] for p in parts]))
            assert np.array_equal(decoded[decimal], expect)

    def test_an_empty_walk_builds_no_object(self):
        assert build_cover_section({}, [], 4) is None

    def test_window_leaves_union_into_one_shard_block(self):
        parts = [_leaf(3), _leaf(9)]
        section = build_cover_section({"11213": parts}, ["h"], 4)
        expect, _order = cover_from_counts(merge_counts([p[1] for p in parts]))
        assert np.array_equal(cover_words(section)["11213"], expect)

    def test_the_cap_coarsens_by_order_and_records_it(self, caplog):
        # 600 instants two buckets apart: the buckets never abut, so the
        # NORMALIZED cover is 600 words at the pinned order, over the 512 cap.
        # §10.5's cap is on those words, so ONE rung down suffices — the
        # doubled buckets abut and `toc_normalize` coalesces the whole run
        # into a single word — and the block lands one order below the pin.
        ts = (BASE_NS + np.arange(600, dtype=np.uint64) * np.uint64(2 * BUCKET_NS)).astype(
            np.uint64
        )
        stamps = np.asarray(time2toc(ts), dtype=np.uint64)
        cover = quantize_words(stamps)
        assert len(cover) == 600
        contributions = {"11213": [(int(toc_reduce(stamps)), count_words(stamps))]}
        with caplog.at_level("WARNING"):
            section = build_cover_section(contributions, ["h"], 4)
        block = section["shards"]["11213"]
        assert block["temporal_order"] == TEMPORAL_COVER_ORDER - 1
        assert block["count"] == 1
        assert block["count"] <= COVER_CAP
        assert "coarsened" in caplog.text
        effective = block["temporal_order"]
        words = cover_words(section)["11213"]
        # Widening only, and EXACTLY: the capped block is the input cover
        # re-quantized one order coarser, never a truncation of its word list
        # (a whole-span overlap check would pass for a truncation too).
        assert np.array_equal(words, quantize_words(cover, effective))
        # Per instant, so a dropped cluster fails here.
        assert all(np.any(toc_overlaps(words, int(t), int(t) + 1)) for t in ts[::7])
        # §10.5's parity invariant at the shard's EFFECTIVE order — the arm
        # `test_parity_with_the_tier_one_map` never reaches.
        tier1 = int(toc_reduce(cover))
        assert int(toc_reduce(words)) == int(toc_reduce(quantize_words([tier1], effective)))

    def test_an_abutting_run_over_the_cap_stays_at_the_pin(self, caplog):
        """§10.5's cap counts the COVER's words, not the counts' buckets.

        600 consecutive occupied buckets are 600 counted-cover keys (§10.3,
        un-coalesced) but exactly ONE §10.5 word, because abutting buckets
        coalesce. Capping the buckets instead would coarsen this shard below
        the pin for a cover that was never near the cap — and would disagree
        with :func:`merge_cover_sections`, which caps the normalized words.
        """
        ts = (BASE_NS + np.arange(600, dtype=np.uint64) * np.uint64(BUCKET_NS)).astype(np.uint64)
        stamps = np.asarray(time2toc(ts), dtype=np.uint64)
        counts = count_words(stamps)
        assert counts.words.size == 600 > COVER_CAP
        assert len(quantize_words(stamps)) == 1
        contributions = {"11213": [(int(toc_reduce(stamps)), counts)]}
        with caplog.at_level("WARNING"):
            section = build_cover_section(contributions, ["h"], 4)
        block = section["shards"]["11213"]
        assert "temporal_order" not in block and block["count"] == 1
        assert "coarsened" not in caplog.text
        # And the same shard through the other §10.5 seam lands identically.
        merged = merge_cover_sections(section, section)
        assert merged["shards"]["11213"] == block

    def test_parity_with_the_tier_one_map(self):
        contributions = _contributions([1, 5, 11])
        section = build_temporal_section(contributions, ["h"], source="sweep")
        cover = build_cover_section(contributions, ["h"], 4)
        for decimal, block in cover["shards"].items():
            order = block.get("temporal_order", TEMPORAL_COVER_ORDER)
            words = cover_words(cover)[decimal]
            tier1 = int(section["shards"][decimal])
            assert int(toc_reduce(words)) == int(toc_reduce(quantize_words([tier1], order)))

    def test_a_block_whose_count_disagrees_is_refused(self):
        section = build_cover_section(_contributions([1]), ["h"], 4)
        (decimal,) = section["shards"]
        section["shards"][decimal]["count"] += 1
        with pytest.raises(ValueError, match="declares"):
            cover_words(section)


class TestCoverComposition:
    """§10.5's seam: per-shard union, requantize at the coarser order, re-cap."""

    def test_merge_unions_per_shard_and_carries_singletons(self):
        a = build_cover_section(_contributions([1, 2]), ["h"], 4)
        b = build_cover_section({"11211": [_leaf(7)], "11219": [_leaf(4)]}, ["g"], 4)
        merged = merge_cover_sections(a, b)
        assert set(merged["shards"]) == {"11210", "11211", "11219"}
        assert merged["fields"] == ["g", "h"]
        union, _order = cover_from_counts(merge_counts([_leaf(2)[1], _leaf(7)[1]]))
        assert np.array_equal(cover_words(merged)["11211"], union)
        assert np.array_equal(cover_words(merged)["11210"], cover_words(a)["11210"])

    def test_mixed_orders_requantize_at_the_coarser(self):
        fine = build_cover_section({"11213": [_leaf(1)]}, ["h"], 4)
        coarse = build_cover_section({"11213": [_leaf(2)]}, ["h"], 4)
        coarse["shards"]["11213"] = dict(coarse["shards"]["11213"])
        # Simulate a capped producer: re-encode the block at order 14.
        words14 = quantize_words(cover_words(coarse)["11213"], 14)
        from zagg.coverage_toc import _encode_cover_block

        coarse["shards"]["11213"] = _encode_cover_block(words14, 14)
        merged = merge_cover_sections(fine, coarse)
        block = merged["shards"]["11213"]
        assert block["temporal_order"] == 14
        expect = quantize_words(np.concatenate([cover_words(fine)["11213"], words14]), 14)
        assert np.array_equal(cover_words(merged)["11213"], expect)
        # And the parity invariant survives the mixed-order requantize, at the
        # merged shard's effective order.
        tier1 = int(toc_reduce(np.array([_leaf(1)[0], _leaf(2)[0]], dtype=np.uint64)))
        assert int(toc_reduce(cover_words(merged)["11213"])) == int(
            toc_reduce(quantize_words([tier1], 14))
        )

    def test_a_standing_cover_at_another_shard_order_is_replaced(self, caplog):
        # D1 ids at two orders are not comparable, so the seam behaves like
        # the carrier's incompatible-envelope arm: the incoming side wins.
        a = build_cover_section(_contributions([1, 2]), ["h"], 4)
        b = build_cover_section({"11219": [_leaf(4)]}, ["g"], 5)
        with caplog.at_level("WARNING"):
            merged = merge_cover_sections(a, b)
        assert merged == b
        assert set(merged["shards"]) == {"11219"}
        assert merged["fields"] == ["g"]
        assert "not comparable" in caplog.text
        # And the same shard order still unions, so the gate is the only change.
        same = build_cover_section({"11219": [_leaf(4)]}, ["g"], 4)
        assert set(merge_cover_sections(a, same)["shards"]) == {"11210", "11211", "11219"}

    def test_a_block_decodes_at_the_objects_pin_not_the_modules(self):
        # §10.5 defines an absent block `temporal_order` against the OBJECT's
        # declaration. A conforming order-14 object leaves its at-the-pin
        # blocks unmarked, and a reader must not read them at this build's pin.
        from zagg.coverage_toc import _encode_cover_block

        section = build_cover_section({"11213": [_leaf(1)]}, ["h"], 4)
        words14 = quantize_words(cover_words(section)["11213"], 14)
        section["temporal_order"] = 14
        section["shards"]["11213"] = _encode_cover_block(words14, 14, 14)
        assert "temporal_order" not in section["shards"]["11213"]
        assert np.array_equal(cover_words(section)["11213"], words14)

    def test_merging_an_object_at_a_coarser_pin_requantizes_there(self):
        from zagg.coverage_toc import _encode_cover_block

        coarse = build_cover_section({"11213": [_leaf(1)]}, ["h"], 4)
        words14 = quantize_words(cover_words(coarse)["11213"], 14)
        coarse["temporal_order"] = 14
        coarse["shards"]["11213"] = _encode_cover_block(words14, 14, 14)
        fine = build_cover_section({"11213": [_leaf(2)]}, ["h"], 4)
        merged = merge_cover_sections(coarse, fine)
        # The composed object pins at the finer of the two, so every block's
        # own order is still <= it; this shard coarsened to 14 and says so.
        assert merged["temporal_order"] == TEMPORAL_COVER_ORDER
        assert merged["shards"]["11213"]["temporal_order"] == 14
        expect = quantize_words(np.concatenate([words14, cover_words(fine)["11213"]]), 14)
        assert np.array_equal(cover_words(merged)["11213"], expect)
        tier1 = int(toc_reduce(np.array([_leaf(1)[0], _leaf(2)[0]], dtype=np.uint64)))
        assert int(toc_reduce(cover_words(merged)["11213"])) == int(
            toc_reduce(quantize_words([tier1], 14))
        )

    def test_a_block_above_the_objects_pin_is_refused(self):
        from zagg.coverage_toc import _encode_cover_block

        section = build_cover_section({"11213": [_leaf(1)]}, ["h"], 4)
        section["temporal_order"] = 14
        section["shards"]["11213"] = _encode_cover_block(
            cover_words(section)["11213"], 20, TEMPORAL_COVER_ORDER
        )
        with pytest.raises(ValueError, match="above the object's pinned"):
            cover_words(section)

    def test_a_pin_change_is_a_content_change(self):
        # The declared pin and cap are what the blocks MEAN, so a standing
        # object at another pin is not "already current" (the skip-if-current
        # test would otherwise never rewrite it).
        a = build_cover_section({"11213": [_leaf(1)]}, ["h"], 4)
        merged = merge_cover_sections(None, a)
        assert cover_unchanged(merged, a)
        repinned = dict(merged, temporal_order=14)
        assert not cover_unchanged(repinned, a)
        recapped = dict(merged, cap=COVER_CAP // 2)
        assert not cover_unchanged(recapped, a)

    def test_an_unknown_incoming_revision_contributes_nothing(self):
        a = build_cover_section(_contributions([1]), ["h"], 4)
        assert merge_cover_sections(a, {"spec": "zagg-coverage-toc-cover/9"}) == a

    def test_an_unknown_standing_revision_is_preserved(self):
        b = build_cover_section(_contributions([1]), ["h"], 4)
        future = {"spec": "zagg-coverage-toc-cover/9", "shards": {}}
        assert merge_cover_sections(future, b) == future
        assert merge_cover_sections(future, None) == future

    def test_unmarked_debris_is_replaced(self):
        b = build_cover_section(_contributions([1]), ["h"], 4)
        assert merge_cover_sections({"shards": "junk"}, b) == b

    def test_cover_unchanged_converges(self):
        a = build_cover_section(_contributions([1, 2]), ["h"], 4)
        assert not cover_unchanged(None, a)
        merged = merge_cover_sections(None, a)
        assert cover_unchanged(merged, a)
        b = build_cover_section({"11219": [_leaf(4)]}, ["h"], 4)
        assert not cover_unchanged(merged, b)
        assert cover_unchanged(merge_cover_sections(merged, b), b)


class TestCoverObject:
    """The sibling object's transport: GET-union-PUT, replace, preservation."""

    def test_write_read_round_trip_accumulates(self, tmp_path):
        root = str(tmp_path)
        a = build_cover_section(_contributions([1]), ["h"], 4)
        b = build_cover_section({"11219": [_leaf(4)]}, ["h"], 4)
        write_cover(root, a)
        write_cover(root, b)
        standing = read_cover(root)
        assert set(standing["shards"]) == {"11210", "11219"}
        assert (tmp_path / COVER_NAME).exists()

    def test_replace_discards_the_standing_object(self, tmp_path):
        root = str(tmp_path)
        write_cover(root, build_cover_section(_contributions([1]), ["h"], 4))
        b = build_cover_section({"11219": [_leaf(4)]}, ["h"], 4)
        write_cover(root, b, replace=True)
        assert set(read_cover(root)["shards"]) == {"11219"}

    def test_replace_never_downgrades_a_future_revision(self, tmp_path):
        root = str(tmp_path)
        future = {"spec": "zagg-coverage-toc-cover/9", "shards": {}}
        (tmp_path / COVER_NAME).write_text(json.dumps(future))
        b = build_cover_section(_contributions([1]), ["h"], 4)
        write_cover(root, b, replace=True)
        assert read_cover(root) == future
        write_cover(root, b)
        assert read_cover(root) == future

    def test_no_contribution_leaves_the_standing_object_untouched(self, tmp_path):
        # The refresh escape hatch over a store with NO temporal channel:
        # `build_cover_section` answers None and `replace=True` must be a
        # no-op on that arm too, not a crash and not an overwrite (§10.5).
        root = str(tmp_path)
        a = build_cover_section(_contributions([1]), ["h"], 4)
        write_cover(root, a)
        raw = (tmp_path / COVER_NAME).read_bytes()
        assert write_cover(root, None, replace=True) == read_cover(root)
        assert (tmp_path / COVER_NAME).read_bytes() == raw
        assert write_cover(root, None) == read_cover(root)
        assert (tmp_path / COVER_NAME).read_bytes() == raw

    def test_no_contribution_on_an_empty_root_writes_nothing(self, tmp_path):
        assert write_cover(str(tmp_path), None, replace=True) is None
        assert write_cover(str(tmp_path), None) is None
        assert not (tmp_path / COVER_NAME).exists()

    def test_garbage_is_overwritten(self, tmp_path):
        root = str(tmp_path)
        (tmp_path / COVER_NAME).write_text("not json {")
        b = build_cover_section(_contributions([1]), ["h"], 4)
        write_cover(root, b)
        assert set(read_cover(root)["shards"]) == {"11210"}

    @pytest.mark.parametrize("damage", ["count", "no-words", "bad-words", "not-an-object"])
    def test_a_corrupt_standing_cover_is_replaced(self, tmp_path, caplog, damage):
        # JSON-valid but not a decodable cover. The regenerable-cache posture
        # is the same as for garbage bytes: log and overwrite, never take the
        # sweep's spatial rollup down with an accelerator (§10.5).
        root = str(tmp_path)
        standing = build_cover_section(_contributions([1]), ["h"], 4)
        (decimal,) = standing["shards"]
        if damage == "count":
            standing["shards"][decimal]["count"] += 1
        elif damage == "no-words":
            del standing["shards"][decimal]["words"]
        elif damage == "bad-words":
            standing["shards"][decimal]["words"] = "junk"
        else:
            standing["shards"][decimal] = "junk"
        (tmp_path / COVER_NAME).write_text(json.dumps(standing))
        incoming = build_cover_section({"11219": [_leaf(4)]}, ["h"], 4)
        # The skip-if-current test runs BEFORE the writer, so it must fail
        # open too rather than raise on the way in.
        assert not cover_unchanged(read_cover(root), incoming)
        with caplog.at_level("WARNING"):
            write_cover(root, incoming)
        assert "failed to parse" in caplog.text
        assert set(read_cover(root)["shards"]) == {"11219"}

    def test_the_read_accessor_stays_loud_on_a_corrupt_block(self):
        # Fail-open is the WRITE seam's posture only: a reader handed a block
        # that disagrees with its own buffer still gets the §10.5 MUST-check.
        section = build_cover_section(_contributions([1]), ["h"], 4)
        (decimal,) = section["shards"]
        del section["shards"][decimal]["words"]
        with pytest.raises(KeyError):
            cover_words(section)

    def test_absent_reads_none(self, tmp_path):
        assert read_cover(str(tmp_path)) is None
        assert load_cover(None) is None
        assert load_cover({"spec": "zagg-coverage-toc-cover/9"}) is None
        assert cover_words(None) is None

    def test_delete_discards_ours_and_debris_but_never_a_future_revision(
        self, tmp_path, monkeypatch
    ):
        import obstore

        from zagg.coverage_toc import delete_cover

        issued: list[str] = []
        real = obstore.delete

        def counting(store, path, *args, **kwargs):
            issued.append(path)
            return real(store, path, *args, **kwargs)

        monkeypatch.setattr(obstore, "delete", counting)
        root = str(tmp_path)
        # Absent: answered from the read, with no DELETE issued at all — which
        # is also what makes the answer the same on S3, whose DeleteObject is
        # idempotent and would report a removal that never happened.
        assert delete_cover(root) is False
        assert issued == []
        write_cover(root, build_cover_section(_contributions([1]), ["h"], 4))
        assert delete_cover(root) is True
        assert read_cover(root) is None
        (tmp_path / COVER_NAME).write_text("not json {")
        assert delete_cover(root) is True  # garbage is debris
        future = {"spec": "zagg-coverage-toc-cover/9", "shards": {}}
        (tmp_path / COVER_NAME).write_text(json.dumps(future))
        assert delete_cover(root) is False  # succession: never deleted
        assert read_cover(root) == future


class TestCoverMarker:
    """§10.1's `cover` key: carried through the section merge, never invented."""

    def test_the_marker_survives_the_seam(self):
        a = build_temporal_section(_contributions([1]), ["h"], source="sweep")
        a[COVER_KEY] = COVER_SPEC
        b = build_temporal_section(_contributions([2]), ["h"], source="sweep")
        assert merge_temporal_sections(a, b)[COVER_KEY] == COVER_SPEC
        assert merge_temporal_sections(b, a)[COVER_KEY] == COVER_SPEC
        assert merge_temporal_sections(None, a)[COVER_KEY] == COVER_SPEC
        assert merge_temporal_sections(a, None)[COVER_KEY] == COVER_SPEC

    def test_no_marker_no_key(self):
        a = build_temporal_section(_contributions([1]), ["h"], source="sweep")
        b = build_temporal_section(_contributions([2]), ["h"], source="sweep")
        assert COVER_KEY not in merge_temporal_sections(a, b)

    def test_a_marker_change_is_a_content_change(self):
        a = build_temporal_section(_contributions([1]), ["h"], source="sweep")
        merged = merge_temporal_sections(None, a)
        assert section_unchanged(merged, a)
        marked = dict(a)
        marked[COVER_KEY] = COVER_SPEC
        assert not section_unchanged(merged, marked)


class TestCaliforniaShape:
    """The issue's real-world shape, scaled (issue #489 acceptance).

    The published CA store's shard 3231242244 holds 2,699,113 exact-timestamp
    companion words that cluster into 49 distinct pass-days (the plan
    comment's decode). Scaled to 49 days × 200 instants, the production fold
    must reproduce that pass-day structure — every observed day answers, a
    day far from every pass prunes, and the whole day scan agrees with the
    quantization law — through the same query surface a reader uses.
    """

    N_DAYS, PER_DAY, SPAN_DAYS = 49, 200, 2_700

    def _shard(self):
        # A pass crosses the shard in ~1 s (issue #575): each day's 200
        # instants fall inside one second.
        rng = np.random.default_rng(3231242244 % 2**31)
        days = np.sort(rng.choice(self.SPAN_DAYS, self.N_DAYS, replace=False))
        ts = np.concatenate(
            [BASE_NS + int(d) * DAY_NS + rng.integers(0, 10**9, self.PER_DAY) for d in days]
        ).astype(np.uint64)
        words = np.asarray(time2toc(ts), dtype=np.uint64)
        contributions = {"11213": [(int(toc_reduce(words)), count_words(words))]}
        envelope = {"temporal": build_temporal_section(contributions, ["h"])}
        cover = build_cover_section(contributions, ["h"], 4)
        return days, words, envelope, cover

    def _day_window(self, d: int) -> tuple[int, int]:
        return BASE_NS + d * DAY_NS, BASE_NS + (d + 1) * DAY_NS

    def test_the_pass_day_clusters_compress_to_about_one_word_each(self):
        _days, words, _envelope, cover = self._shard()
        block = cover["shards"]["11213"]
        # 9,800 exact timestamps -> at most one word per pass-day cluster,
        # and nowhere near the cap (no coarsening recorded).
        assert 1 <= block["count"] <= self.N_DAYS
        assert "temporal_order" not in block
        assert np.array_equal(cover_words(cover)["11213"], quantize_words(words))

    def test_every_pass_day_answers_and_far_days_prune(self):
        days, _words, envelope, cover = self._shard()
        day_set = set(int(d) for d in days)
        for d in day_set:
            assert shards_overlapping(envelope, *self._day_window(d), cover=cover) == ["11213"]
        # INTERIOR gaps only: tier 1's one envelope word spans first..last
        # pass, so it prunes nothing between them — the cover must. At the
        # pinned order a bucket reaches minutes past an instant, so day
        # distance >= 2 from every pass is guaranteed gap.
        far = [
            d for d in range(min(day_set), max(day_set)) if min(abs(p - d) for p in day_set) >= 2
        ]
        assert len(far) > 2_000  # the store is overwhelmingly gap
        for d in far[:: max(1, len(far) // 200)]:
            assert shards_overlapping(envelope, *self._day_window(d), cover=cover) == []
            # ... where tier 1 alone cannot prune a single one of them:
            assert shards_overlapping(envelope, *self._day_window(d)) == ["11213"]

    def test_the_full_day_scan_reproduces_the_pass_day_set(self):
        days, words, envelope, cover = self._shard()
        expect = quantize_words(words)
        selected, law = [], []
        for d in range(self.SPAN_DAYS):
            lo, hi = self._day_window(d)
            if shards_overlapping(envelope, lo, hi, cover=cover):
                selected.append(d)
            if bool(np.any(np.atleast_1d(toc_overlaps(expect, lo, hi)))):
                law.append(d)
        # The query surface IS the quantization law, day for day...
        assert selected == law
        day_set = set(int(d) for d in days)
        # ...it never misses an observed day...
        assert day_set <= set(selected)
        # ...and it over-claims by at most the bucket geometry: every
        # selected day is within 1 day of a real pass (span 2^45 ns ≈ 0.41
        # days, so a bucket touching an instant reaches at most 1 day out).
        assert all(min(abs(p - d) for p in day_set) <= 1 for d in selected)

    def test_parity_holds_at_ca_shape(self):
        _days, words, envelope, cover = self._shard()
        cw = cover_words(cover)["11213"]
        tier1 = int(envelope["temporal"]["shards"]["11213"])
        assert int(toc_reduce(cw)) == int(toc_reduce(quantize_words([tier1])))
