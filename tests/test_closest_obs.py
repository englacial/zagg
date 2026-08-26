"""Tests for the closest-observation ingest builder (issue #509).

Phase 1 — cover-driven epoch extraction: per-shard epochs from one or more
store roots' ``coverage.toc`` word-set covers (spec §10.5), union across
stores, AOI intersect, loud refusal when a store carries no readable cover.
Synthetic covers ride the same :func:`zagg.coverage_toc.build_cover_section`
producer the sweep uses; the committed golden ``coverage.toc`` fixture pins
the extraction against the frozen §10.5 grammar bytes.
"""

import json
import logging
from pathlib import Path

import numpy as np
import pytest
from mortie import from_datetime64, time2toc, to_datetime64

from zagg.catalog.closest_obs import (
    ReferenceEpochs,
    _word_midpoints,
    closest_obs_shardmap,
    nearest_acquisitions,
    reference_epochs,
)
from zagg.coverage_toc import (
    COVER_CAP,
    COVER_NAME,
    COVER_SPEC,
    TEMPORAL_COVER_ORDER,
    _encode_cover_block,
    build_cover_section,
    cover_words,
    quantize_words,
    read_cover,
    write_cover,
)
from zagg.grids.morton import morton_word

SPEC_DATA = Path(__file__).parent / "data" / "spec"
DAY_NS = 86_400 * 10**9
#: An arbitrary but realistic base instant on the §8 internal-ns scale
#: (mirrors ``test_coverage_toc``'s convention).
BASE_NS = 5_344_000_000_000_000_000
#: One order-18 cover bucket (2^45 ns) and half of it — the epoch-midpoint
#: error bound is half a BUCKET, not half a word (a word is a run of buckets).
BUCKET_NS = 2 ** (63 - TEMPORAL_COVER_ORDER)
HALF_BUCKET = np.timedelta64(BUCKET_NS // 2, "ns")

#: The golden fixture's one shard, and the cell centre / far-away rings the
#: AOI tests use (order 4; centre from ``mortie.mort2geo``).
SHARD = "11213"
SHARD_KEY = morton_word(SHARD)
#: The shard next door (centre lat 14.54, lon 59.06) — the cross-shard join
#: tests need a second shard the single-shard fixture never exercised.
SHARD_B = "11212"
SHARD_B_KEY = morton_word(SHARD_B)


def _ring(lat, lon, half=2.0):
    lats = np.array([lat - half, lat - half, lat + half, lat + half, lat - half])
    lons = np.array([lon - half, lon + half, lon + half, lon - half, lon - half])
    return [(lats, lons)]


AOI_AT_SHARD = _ring(14.54, 53.44)
AOI_ELSEWHERE = _ring(-40.0, -120.0)


def _write_store(root, shards: dict[str, np.ndarray], order: int = 4) -> str:
    """A store root carrying a §10.5 cover claiming ``shards``' instants."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    contributions = {
        decimal: [(None, None, None, quantize_words(time2toc(np.asarray(inst, dtype=np.uint64))))]
        for decimal, inst in shards.items()
    }
    write_cover(str(root), build_cover_section(contributions, ["h"], order))
    return str(root)


def _instants(*day_offsets) -> np.ndarray:
    return np.array([BASE_NS + d * DAY_NS for d in day_offsets], dtype=np.uint64)


def _utc(instants) -> np.ndarray:
    return np.sort(np.asarray(to_datetime64(np.asarray(instants, np.uint64)), "datetime64[ns]"))


def _nearest_gap(mids: np.ndarray, true: np.ndarray) -> np.timedelta64:
    """Largest distance from a true instant to its nearest epoch."""
    return np.abs(true[:, None] - mids[None, :]).min(axis=1).max()


class TestWordMidpoints:
    def test_empty_words_decode_to_no_epochs(self):
        out = _word_midpoints(np.empty(0, dtype=np.uint64))
        assert out.dtype == np.dtype("datetime64[ns]") and out.size == 0

    def test_a_quantized_word_midpoint_stays_within_half_a_bucket(self):
        inst = _instants(0, 5, 11)
        words = quantize_words(time2toc(inst))
        mids = np.sort(_word_midpoints(words))
        true = np.sort(np.asarray(to_datetime64(inst), dtype="datetime64[ns]"))
        assert mids.size == true.size
        assert (np.abs(mids - true) <= HALF_BUCKET).all()

    def test_an_exact_timestamp_word_decodes_to_its_bucket_midpoint(self):
        """An unquantized word still resolves to the bucket it falls in."""
        inst = _instants(3)
        mids = _word_midpoints(np.asarray(time2toc(inst), dtype=np.uint64))
        assert mids.size == 1
        assert abs(mids[0] - _utc(inst)[0]) <= HALF_BUCKET

    def test_two_passes_one_bucket_apart_yield_two_epochs(self):
        """``toc_normalize`` coalesces the abutting buckets into ONE word."""
        inst = np.array([BASE_NS, BASE_NS + BUCKET_NS], dtype=np.uint64)
        words = quantize_words(time2toc(inst))
        assert len(words) == 1  # the coalescing that motivates the expansion
        mids = np.sort(_word_midpoints(words))
        assert mids.size == 2
        assert _nearest_gap(mids, _utc(inst)) <= HALF_BUCKET

    def test_a_contiguous_campaign_yields_one_epoch_per_covered_bucket(self):
        """A 10-day, 6-hourly campaign is one word — and 25 covered buckets."""
        inst = np.array([BASE_NS + i * 6 * 3600 * 10**9 for i in range(40)], dtype=np.uint64)
        words = quantize_words(time2toc(inst))
        assert len(words) == 1
        covered = {int(t) >> (63 - TEMPORAL_COVER_ORDER) for t in inst}
        mids = np.sort(_word_midpoints(words))
        assert mids.size == len(covered) == 25
        assert _nearest_gap(mids, _utc(inst)) <= HALF_BUCKET

    def test_a_pass_straddling_a_bucket_edge_yields_two_epochs(self):
        """Benign over-selection: both epochs pick the same nearest granule."""
        edge = ((BASE_NS >> (63 - TEMPORAL_COVER_ORDER)) + 1) << (63 - TEMPORAL_COVER_ORDER)
        inst = np.array([edge - 60 * 10**9, edge + 60 * 10**9], dtype=np.uint64)
        words = quantize_words(time2toc(inst))
        assert len(words) == 1
        mids = np.sort(_word_midpoints(words))
        assert mids.size == 2
        assert _nearest_gap(mids, _utc(inst)) <= HALF_BUCKET

    def test_a_coarser_order_widens_the_buckets(self):
        """The block's effective order drives the expansion, not the pin."""
        inst = np.array([BASE_NS, BASE_NS + BUCKET_NS], dtype=np.uint64)
        coarse = quantize_words(time2toc(inst), TEMPORAL_COVER_ORDER - 2)
        mids = _word_midpoints(coarse, TEMPORAL_COVER_ORDER - 2)
        assert mids.size == 1  # both passes now share one 39 h bucket
        assert _nearest_gap(mids, _utc(inst)) <= 4 * HALF_BUCKET


class TestReferenceEpochs:
    def test_one_store_one_shard(self, tmp_path):
        root = _write_store(tmp_path, {SHARD: _instants(0, 5, 11)})
        out = reference_epochs(root)
        assert isinstance(out, ReferenceEpochs)
        assert out.order == 4
        assert list(out.epochs) == [SHARD_KEY]
        assert out.epochs[SHARD_KEY].size == 3 == out.total
        assert out.stores == [root]
        # Sorted unique datetime64[ns], the contract phase 2 searchsorted rides.
        e = out.epochs[SHARD_KEY]
        assert e.dtype == np.dtype("datetime64[ns]")
        assert (np.diff(e.astype("int64")) > 0).all()

    def test_union_across_stores_is_deduplicated(self, tmp_path):
        """Two stores quantized on the same grid: shared passes count once."""
        a = _write_store(tmp_path / "a", {SHARD: _instants(0, 5)})
        b = _write_store(tmp_path / "b", {SHARD: _instants(5, 11)})
        out = reference_epochs([a, b])
        assert out.epochs[SHARD_KEY].size == 3
        # Parity: the union equals each store's own epochs united.
        ea = reference_epochs(a).epochs[SHARD_KEY]
        eb = reference_epochs(b).epochs[SHARD_KEY]
        assert np.array_equal(out.epochs[SHARD_KEY], np.union1d(ea, eb))

    def test_partially_overlapping_covers_union_at_the_bucket_grid(self, tmp_path):
        """Unequal words for a shared pass must not double it (bucket union).

        Three passes one bucket apart; store A saw passes 1+2 and store B
        saw 2+3, so each store's cover carries a *different* two-bucket range
        word for the shared pass. Word-level ``np.unique`` keeps both and
        yields two displaced epochs for three passes; bucket-level union
        yields exactly one epoch per covered bucket.
        """
        passes = np.array([BASE_NS, BASE_NS + BUCKET_NS, BASE_NS + 2 * BUCKET_NS], np.uint64)
        a = _write_store(tmp_path / "a", {SHARD: passes[:2]})
        b = _write_store(tmp_path / "b", {SHARD: passes[1:]})
        # Each store really does emit one coalesced (and unequal) word.
        wa = cover_words(read_cover(a))[SHARD]
        wb = cover_words(read_cover(b))[SHARD]
        assert len(wa) == len(wb) == 1 and wa[0] != wb[0]
        out = reference_epochs([a, b])
        e = out.epochs[SHARD_KEY]
        assert e.size == 3
        assert _nearest_gap(e, _utc(passes)) <= HALF_BUCKET

    def test_a_shard_only_one_store_covers_still_contributes(self, tmp_path):
        a = _write_store(tmp_path / "a", {SHARD: _instants(0)})
        b = _write_store(tmp_path / "b", {SHARD: _instants(40), "11212": _instants(7)})
        out = reference_epochs([a, b])
        assert set(out.epochs) == {SHARD_KEY, morton_word("11212")}
        assert out.epochs[SHARD_KEY].size == 2

    def test_a_store_without_a_cover_refuses_loudly(self, tmp_path):
        (tmp_path / "empty").mkdir()
        with pytest.raises(ValueError, match="cover-driven"):
            reference_epochs(str(tmp_path / "empty"))

    def test_an_unknown_revision_cover_refuses_loudly(self, tmp_path):
        root = tmp_path / "future"
        root.mkdir()
        (root / COVER_NAME).write_text(
            json.dumps({"spec": "zagg-coverage-toc-cover/9", "shards": {}})
        )
        with pytest.raises(ValueError, match="unreadable"):
            reference_epochs(str(root))

    def test_a_shard_order_mismatch_between_stores_refuses(self, tmp_path):
        a = _write_store(tmp_path / "a", {SHARD: _instants(0)}, order=4)
        b = _write_store(tmp_path / "b", {SHARD: _instants(5)}, order=5)
        with pytest.raises(ValueError, match="not.*comparable|shard order"):
            reference_epochs([a, b])

    def test_a_cover_without_a_shard_order_refuses_by_name(self, tmp_path):
        """A missing ``order`` used to surface as an opaque ``TypeError``."""
        root = tmp_path / "orderless"
        root.mkdir()
        (root / COVER_NAME).write_text(
            json.dumps({"spec": "zagg-coverage-toc-cover/1", "shards": {}})
        )
        with pytest.raises(ValueError, match="non-integer cover shard order"):
            reference_epochs(str(root))

    def test_no_stores_refuses(self):
        with pytest.raises(ValueError, match="at least one"):
            reference_epochs([])

    def test_aoi_parts_restrict_the_shard_set(self, tmp_path):
        root = _write_store(tmp_path, {SHARD: _instants(0, 5)})
        assert set(reference_epochs(root, aoi=AOI_AT_SHARD).epochs) == {SHARD_KEY}
        assert reference_epochs(root, aoi=AOI_ELSEWHERE).epochs == {}

    def test_aoi_moc_restricts_the_shard_set(self, tmp_path):
        from mortie import Moc

        root = _write_store(tmp_path, {SHARD: _instants(0, 5)})
        keep = Moc.from_polygon(*AOI_AT_SHARD[0])
        drop = Moc.from_polygon(*AOI_ELSEWHERE[0])
        assert set(reference_epochs(root, aoi=keep).epochs) == {SHARD_KEY}
        assert reference_epochs(root, aoi=drop).epochs == {}

    def test_aoi_geojson_path_restricts_the_shard_set(self, tmp_path):
        root = _write_store(tmp_path, {SHARD: _instants(0)})
        lats, lons = AOI_AT_SHARD[0]
        geojson = {
            "type": "Polygon",
            "coordinates": [[[float(x), float(y)] for x, y in zip(lons, lats)]],
        }
        path = tmp_path / "aoi.geojson"
        path.write_text(json.dumps(geojson))
        assert set(reference_epochs(root, aoi=str(path)).epochs) == {SHARD_KEY}


class TestCoarsenedBlock:
    """§10.5 lets a block coarsen below the pin — the read half must be loud."""

    #: Enough single-bucket claims (2 buckets apart, so no gap coalesces) to
    #: blow the 512-word cap and force ``_cap_cover`` down an order.
    COARSE_INSTANTS = np.array(
        [BASE_NS + i * 2 * BUCKET_NS for i in range(COVER_CAP + 88)], dtype=np.uint64
    )

    def _root(self, tmp_path):
        return _write_store(tmp_path, {SHARD: self.COARSE_INSTANTS})

    def test_the_block_really_coarsened(self, tmp_path):
        root = self._root(tmp_path)
        block = read_cover(root)["shards"][SHARD]
        assert block["temporal_order"] < TEMPORAL_COVER_ORDER

    def test_a_coarsened_block_warns_and_reports_its_order(self, tmp_path, caplog):
        root = self._root(tmp_path)
        landed = read_cover(root)["shards"][SHARD]["temporal_order"]
        with caplog.at_level("WARNING", logger="zagg.catalog.closest_obs"):
            out = reference_epochs(root)
        assert f"temporal order {landed}" in caplog.text
        assert "below the pinned" in caplog.text
        assert out.orders == {SHARD_KEY: landed}
        assert out.tolerance(SHARD_KEY) == np.timedelta64(2 ** (62 - landed), "ns")

    def test_the_epochs_are_the_coarse_buckets_midpoints(self, tmp_path):
        root = self._root(tmp_path)
        landed = read_cover(root)["shards"][SHARD]["temporal_order"]
        k = 63 - landed
        internal = np.asarray(from_datetime64(reference_epochs(root).epochs[SHARD_KEY]), np.uint64)
        # Every epoch sits at an aligned coarse-bucket midpoint, and at the
        # coarse grid's buckets — not the pinned order's.
        assert set(int(t) % 2**k for t in internal) == {2 ** (k - 1) - 1}
        covered = {int(t) >> k for t in self.COARSE_INSTANTS}
        assert set(int(t) >> k for t in internal) == covered
        assert _nearest_gap(
            reference_epochs(root).epochs[SHARD_KEY], _utc(self.COARSE_INSTANTS)
        ) <= np.timedelta64(2 ** (k - 1), "ns")

    def test_an_uncoarsened_block_neither_warns_nor_hides_its_order(self, tmp_path, caplog):
        root = _write_store(tmp_path, {SHARD: _instants(0, 5)})
        with caplog.at_level("WARNING", logger="zagg.catalog.closest_obs"):
            out = reference_epochs(root)
        assert "below the pinned" not in caplog.text
        assert out.orders == {SHARD_KEY: TEMPORAL_COVER_ORDER}
        assert out.tolerance(SHARD_KEY) == HALF_BUCKET


class TestGoldenFixture:
    """The committed §7 ``temporal/`` fixture pins the frozen grammar bytes."""

    #: The two epochs the committed fixture decodes to, pinned as literals
    #: rather than recomputed with the function under test. Verified by hand:
    #: each is the midpoint of an order-18 bucket (internal ns congruent to
    #: 2^44 - 1 mod 2^45, buckets 151903 and 151915 — twelve apart, the
    #: fixture's five-day gap), and each sits within half a bucket of the
    #: generator's two campaign clusters (``TEMPORAL_BASE`` and +5 days).
    GOLDEN = np.array(
        ["2019-05-14T03:14:07.595891711", "2019-05-19T00:31:00.060957695"],
        dtype="datetime64[ns]",
    )

    def test_the_golden_cover_decodes_to_the_pinned_epochs(self):
        out = reference_epochs(str(SPEC_DATA / "temporal"))
        assert out.order == 4
        assert list(out.epochs) == [SHARD_KEY]
        assert np.array_equal(out.epochs[SHARD_KEY], self.GOLDEN)
        assert out.orders == {SHARD_KEY: TEMPORAL_COVER_ORDER}

    def test_the_golden_epochs_are_order_18_bucket_midpoints(self):
        """The arithmetic the ±4.9 h claim rests on, checked independently."""
        k = 63 - TEMPORAL_COVER_ORDER
        internal = np.asarray(from_datetime64(self.GOLDEN), dtype=np.uint64)
        assert [int(t) % 2**k for t in internal] == [2 ** (k - 1) - 1] * 2
        assert [int(t) >> k for t in internal] == [151903, 151915]

    def test_every_fixture_observation_has_an_epoch_within_half_a_bucket(self):
        """The property the ruling actually claims, against the generator's
        own recorded instants (``temporal.expected.json``), not against
        anything :mod:`zagg.catalog.closest_obs` computed."""
        expected = json.loads((SPEC_DATA / "temporal.expected.json").read_text())
        true = np.array(
            sorted({int(ns) for c in expected["cells"] for ns in c["obs_span_ns"]}),
            dtype="datetime64[ns]",
        )
        epochs = reference_epochs(str(SPEC_DATA / "temporal")).epochs[SHARD_KEY]
        assert _nearest_gap(epochs, true) <= HALF_BUCKET

    def test_the_golden_epochs_sit_inside_the_fixture_campaign(self):
        """Midpoints land inside the leaf's synthetic campaign window."""
        out = reference_epochs(str(SPEC_DATA / "temporal"))
        lo = np.datetime64("2019-01-01")
        hi = np.datetime64("2020-01-01")
        e = out.epochs[SHARD_KEY]
        assert ((e > lo) & (e < hi)).all()


class TestNearestAcquisitions:
    """Phase 2 — the vectorized closest-1 selection core."""

    def _dt(self, *hours):
        return np.array(
            [np.datetime64("2025-06-01T00:00") + np.timedelta64(h, "h") for h in hours]
        ).astype("datetime64[ns]")

    def test_each_epoch_selects_its_nearest_acquisition(self):
        times = self._dt(0, 10, 24)
        epochs = self._dt(1, 9, 23)
        sel, off = nearest_acquisitions(epochs, times)
        assert sel.tolist() == [0, 1, 2]
        assert off.astype("timedelta64[h]").astype(int).tolist() == [-1, 1, 1]

    def test_offsets_are_signed_acquisition_minus_epoch(self):
        times = self._dt(12)
        epochs = self._dt(10, 14)
        sel, off = nearest_acquisitions(epochs, times)
        assert sel.tolist() == [0, 0]
        assert off[0] == np.timedelta64(2, "h") and off[1] == -np.timedelta64(2, "h")

    def test_an_exact_match_has_zero_offset(self):
        times = self._dt(5, 7)
        sel, off = nearest_acquisitions(self._dt(7), times)
        assert sel.tolist() == [1] and off[0] == np.timedelta64(0, "ns")

    def test_a_tie_selects_the_earlier_acquisition(self):
        times = self._dt(0, 10)
        sel, off = nearest_acquisitions(self._dt(5), times)
        assert sel.tolist() == [0]
        assert off[0] == -np.timedelta64(5, "h")

    def test_selection_indices_refer_to_the_input_order(self):
        times = self._dt(24, 0, 10)  # unsorted catalog order
        sel, _ = nearest_acquisitions(self._dt(23, 1), times)
        assert sel.tolist() == [0, 1]

    def test_max_time_offset_boundary_exactly_at_selects(self):
        times = self._dt(0)
        sel, off = nearest_acquisitions(self._dt(6), times, max_time_offset=np.timedelta64(6, "h"))
        assert sel.tolist() == [0]

    def test_max_time_offset_one_ns_past_drops_but_still_reports(self):
        times = self._dt(0)
        cap = np.timedelta64(6, "h") - np.timedelta64(1, "ns")
        sel, off = nearest_acquisitions(self._dt(6), times, max_time_offset=cap)
        assert sel.tolist() == [-1]
        # The nearest offset is still reported — the loud record the drop rides.
        assert off[0] == -np.timedelta64(6, "h")

    def test_no_acquisitions_selects_nothing_and_reports_nat(self):
        sel, off = nearest_acquisitions(self._dt(1, 2), np.array([], dtype="datetime64[ns]"))
        assert sel.tolist() == [-1, -1]
        assert np.isnat(off).all()

    def test_no_epochs_is_empty(self):
        sel, off = nearest_acquisitions(np.array([], dtype="datetime64[ns]"), self._dt(0))
        assert sel.size == 0 and off.size == 0

    def test_a_negative_max_time_offset_refuses(self):
        with pytest.raises(ValueError, match="non-negative"):
            nearest_acquisitions(self._dt(1), self._dt(0), max_time_offset=-np.timedelta64(1, "h"))

    def test_duplicate_acquisition_times_select_the_first_record(self):
        # One datatake, three granules at the same instant, plus a far one.
        times = self._dt(0, 0, 0, 96)
        for epoch in (self._dt(-1), self._dt(0), self._dt(1)):
            sel, _ = nearest_acquisitions(epoch, times)
            assert sel.tolist() == [0]

    def _check_oracle(self, epochs, times, cap):
        """Brute force: nearest, ties to the earlier, then the lowest index."""
        sel, off = nearest_acquisitions(epochs, times, max_time_offset=cap)
        t = times.astype("int64")
        for k, e in enumerate(epochs.astype("int64")):
            d = np.abs(t - e)
            best = np.flatnonzero(d == d.min())
            # tie -> earlier acquisition; equal times -> lowest catalog index
            want = best[np.argmin(t[best])] if best.size > 1 else best[0]
            assert off[k] == np.timedelta64(int(t[want] - e), "ns")
            if d.min() <= cap.astype("timedelta64[ns]").astype("int64"):
                assert sel[k] == want
            else:
                assert sel[k] == -1

    def test_an_unconvertible_max_time_offset_refuses_by_name(self):
        with pytest.raises(ValueError, match="does not convert exactly to nanoseconds"):
            nearest_acquisitions(
                self._dt(1), self._dt(0), max_time_offset=np.timedelta64(1000, "Y")
            )

    def test_a_nat_max_time_offset_refuses(self):
        with pytest.raises(ValueError, match="must be a real duration"):
            nearest_acquisitions(self._dt(1), self._dt(0), max_time_offset=np.timedelta64("NaT"))

    def test_a_gap_past_int64_nanoseconds_never_pairs_under_a_cap(self):
        # 584 years apart — a real distance that timedelta64[ns] cannot carry.
        times = np.array(["1677-09-22T00:12:44"], dtype="datetime64[ns]")
        epochs = np.array(["2262-04-11T23:47:16"], dtype="datetime64[ns]")
        for cap in (np.timedelta64(1, "D"), np.timedelta64(120, "D")):
            sel, off = nearest_acquisitions(epochs, times, max_time_offset=cap)
            assert sel.tolist() == [-1]
            assert np.isnat(off).all()
        # Uncapped, the nearest is still the nearest; the offset saturates to NaT.
        sel, off = nearest_acquisitions(epochs, times)
        assert sel.tolist() == [0]
        assert np.isnat(off).all()

    def test_a_two_century_gap_still_reports_an_exact_offset(self):
        times = np.array(["1800-01-01", "2150-01-01"], dtype="datetime64[ns]")
        epochs = np.array(["2000-01-01"], dtype="datetime64[ns]")
        sel, off = nearest_acquisitions(epochs, times)
        assert sel.tolist() == [1]
        assert off[0] == times[1] - epochs[0]

    def test_a_nat_acquisition_time_refuses(self):
        times = np.array(["2025-06-01T00", "NaT", "2025-06-01T10"], dtype="datetime64[ns]")
        with pytest.raises(ValueError, match="times carries NaT"):
            nearest_acquisitions(self._dt(1, 9), times)

    def test_a_nat_epoch_refuses(self):
        epochs = np.array(["2025-06-01T01", "NaT"], dtype="datetime64[ns]")
        with pytest.raises(ValueError, match="epochs carries NaT"):
            nearest_acquisitions(epochs, self._dt(0, 10))

    def test_matches_a_brute_force_oracle(self):
        rng = np.random.default_rng(7)
        base = np.datetime64("2025-01-01").astype("datetime64[ns]").astype("int64")
        times = (base + rng.integers(0, 400 * 86_400, 60) * 10**9).astype("datetime64[ns]")
        epochs = (base + rng.integers(0, 400 * 86_400, 45) * 10**9).astype("datetime64[ns]")
        self._check_oracle(epochs, times, np.timedelta64(2, "D"))

    def test_matches_the_oracle_with_every_acquisition_time_duplicated(self):
        rng = np.random.default_rng(7)
        base = np.datetime64("2025-01-01").astype("datetime64[ns]").astype("int64")
        raw = base + rng.integers(0, 400 * 86_400, 20) * 10**9
        # Every instant carried by two catalog records, interleaved out of order.
        times = np.concatenate([raw, raw]).astype("datetime64[ns]")
        epochs = (base + rng.integers(0, 400 * 86_400, 30) * 10**9).astype("datetime64[ns]")
        self._check_oracle(epochs, times, np.timedelta64(2, "D"))


# ── phase 3: the builder ─────────────────────────────────────────────────────
#
# Geometry: everything happens in shard 11213 (order 4; centre lat 14.54,
# lon 53.44 from mortie.mort2geo) — the same shard the golden fixture covers —
# with S2-like STAC items (multi-band assets, NO canonical data asset, a
# per-item datetime) footprinted around the centre so the spatial build
# assigns them there.


def _s2_item(gid, iso, lat=14.54, lon=53.44, half=0.4):
    ring = [
        [lon - half, lat - half],
        [lon + half, lat - half],
        [lon + half, lat + half],
        [lon - half, lat + half],
        [lon - half, lat - half],
    ]
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": gid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "bbox": [lon - half, lat - half, lon + half, lat + half],
        "properties": {"datetime": iso},
        "collection": "sentinel-2-l2a",
        "stac_extensions": [],
        "links": [],
        "assets": {
            "red": {"href": f"https://h/{gid}/B04.tif", "roles": ["data"]},
            "nir": {"href": f"https://h/{gid}/B08.tif", "roles": ["data"]},
        },
    }


def _s2_catalog(items, bbox=(52.0, 13.0, 55.0, 16.0)):
    import pyarrow as pa
    import stac_geoparquet.arrow as sga

    from zagg.catalog.sources import Catalog

    return Catalog(
        pa.table(sga.parse_stac_items_to_arrow(items)),
        {"collection": "sentinel-2-l2a", "bbox": list(bbox)},
    )


def _grid(parent_order=4):
    from zagg.grids import HealpixGrid

    return HealpixGrid(parent_order, 6)


def _epoch_iso(day):
    """An ISO instant near BASE_NS + day*DAY (the synthetic covers' passes)."""
    ns = np.datetime64(to_datetime64(np.array([BASE_NS + day * DAY_NS], dtype=np.uint64))[0], "ns")
    return np.datetime_as_string(ns.astype("datetime64[s]")) + "Z"


class TestClosestObsShardmap:
    def _setup(self, tmp_path, days=(0, 5, 11), s2_days=(0.1, 5.2, 20.0)):
        store = _write_store(tmp_path / "ref", {SHARD: _instants(*days)})
        items = [_s2_item(f"S2_{i}", _epoch_iso(d)) for i, d in enumerate(s2_days)]
        return store, _s2_catalog(items)

    def test_builds_a_standard_shardmap(self, tmp_path):
        store, cat = self._setup(tmp_path)
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        assert sm.shard_keys == [SHARD_KEY]
        ids = {g["id"] for g in sm.granules[0]}
        # Epochs at days 0/5/11 pick S2_0 (0.1), S2_1 (5.2), S2_2 (20 vs 5.2:
        # day 11 is 5.8 days from S2_1 and 9 days from S2_2 -> S2_1), deduped.
        assert ids == {"S2_0", "S2_1"}
        assert sm.metadata["total_pairs"] == 2
        assert sm.metadata["granules_assigned"] == 2
        assert sm.metadata["closest_obs"]["epochs_paired"] == 3
        assert sm.metadata["closest_obs"]["epochs_dropped"] == 0

    def test_provenance_rows_reconstruct_the_pairing(self, tmp_path):
        store, cat = self._setup(tmp_path)
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        by_id = {g["id"]: g for g in sm.granules[0]}
        # S2_1 was selected by two epochs (days 5 and 11): dedupe keeps ONE
        # entry carrying BOTH provenance rows, row-aligned.
        assert len(by_id["S2_1"]["paired_epochs"]) == 2
        assert len(by_id["S2_1"]["epoch_offsets_ns"]) == 2
        # Signed offsets: acquisition - epoch. Day-5 epoch precedes the day-5.2
        # acquisition -> positive; day-11 epoch follows it -> negative.
        offs = sorted(by_id["S2_1"]["epoch_offsets_ns"])
        assert offs[0] < 0 < offs[1]
        # And each is within half a bucket + the true separation.
        assert len(by_id["S2_0"]["paired_epochs"]) == 1

    def test_the_map_json_round_trips_with_provenance(self, tmp_path):
        store, cat = self._setup(tmp_path)
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        path = tmp_path / "map.json"
        sm.to_json(str(path))
        from zagg.catalog.shardmap import ShardMap

        back = ShardMap.from_json(str(path))
        assert back.shard_keys == sm.shard_keys
        assert back.granules == sm.granules
        assert back.metadata["closest_obs"] == sm.metadata["closest_obs"]

    def test_max_time_offset_drops_are_recorded_loudly(self, tmp_path, caplog):
        # Day-11 epoch's nearest acquisition is 5.8 days away: beyond a 2-day
        # cap it selects nothing, and the drop carries the near-miss offset.
        store, cat = self._setup(tmp_path)
        with caplog.at_level(logging.WARNING, logger="zagg.catalog.closest_obs"):
            sm = closest_obs_shardmap(
                cat,
                store,
                grid=_grid(),
                backend="mortie",
                max_time_offset=np.timedelta64(2, "D"),
            )
        rec = sm.metadata["closest_obs"]
        assert rec["epochs_dropped"] == 1 and len(rec["dropped"]) == 1
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]
        assert rec["dropped"][0]["shard"] == SHARD
        assert rec["dropped"][0]["nearest_offset_ns"] is not None
        assert any("selected nothing" in m for m in caplog.messages)
        ids = {g["id"] for g in sm.granules[0]}
        assert ids == {"S2_0", "S2_1"}

    def test_filtered_map_is_a_subset_of_the_spatial_map(self, tmp_path):
        from zagg.catalog.shardmap import ShardMap

        store, cat = self._setup(tmp_path)
        spatial = ShardMap.build(cat, _grid(), backend="mortie")
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        spatial_pairs = {
            (k, g["id"]) for k, gr in zip(spatial.shard_keys, spatial.granules) for g in gr
        }
        paired = {(k, g["id"]) for k, gr in zip(sm.shard_keys, sm.granules) for g in gr}
        assert paired <= spatial_pairs

    def test_grid_order_mismatch_refuses(self, tmp_path):
        store, cat = self._setup(tmp_path)
        with pytest.raises(ValueError, match="parent_order"):
            closest_obs_shardmap(cat, store, grid=_grid(parent_order=5), backend="mortie")

    def test_max_granules_per_shard_refuses_loudly(self, tmp_path):
        store, cat = self._setup(tmp_path)
        with pytest.raises(ValueError, match="max_granules_per_shard"):
            closest_obs_shardmap(
                cat, store, grid=_grid(), backend="mortie", max_granules_per_shard=1
            )

    def test_estimate_reports_without_building(self, tmp_path):
        store, cat = self._setup(tmp_path)
        est = closest_obs_shardmap(
            cat,
            store,
            grid=_grid(),
            backend="mortie",
            estimate=True,
            max_granules_per_shard=1,
            bytes_per_granule=10**6,
        )
        assert isinstance(est, dict)
        assert est["shards"] == 1 and est["granules"] == 2 and est["pairs"] == 2
        assert est["per_shard"] == {SHARD: 2}
        assert est["histogram"] == {2: 1}
        assert est["est_bytes"] == 2 * 10**6
        assert est["max_cost_usd"] > 0
        # The cost gate REPORTS violations in a dry run instead of raising.
        assert est["violations"] == [(SHARD, 2)]

    def test_a_granule_without_acquisition_time_refuses(self, tmp_path):
        store, _ = self._setup(tmp_path)
        item = _s2_item("S2_bare", _epoch_iso(0))
        del item["properties"]["datetime"]  # -> null datetime column
        # stac-geoparquet requires a datetime key; set None explicitly instead
        item["properties"]["datetime"] = None
        cat = _s2_catalog([item])
        with pytest.raises(ValueError, match="acquisition time"):
            closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")

    def test_the_spatial_aoi_mask_is_neither_carried_nor_claimed(self, tmp_path):
        # The strict-AOI payload (#101) belongs to the spatial map; the derived
        # map carries none, so its metadata must not advertise one either.
        from zagg.catalog.shardmap import ShardMap
        from zagg.config import default_config
        from zagg.grids import HealpixGrid

        cfg = default_config("atl06")
        cfg.output = {**cfg.output, "aoi_mask": True}
        grid = HealpixGrid(4, 6, config=cfg)
        store, cat = self._setup(tmp_path)
        spatial = ShardMap.build(cat, grid, backend="mortie")
        assert spatial.aoi_mask is not None and spatial.metadata["aoi_mask"] is True
        sm = closest_obs_shardmap(cat, store, grid=grid, backend="mortie")
        assert sm.aoi_mask is None
        assert "aoi_mask" not in sm.metadata

    def test_a_shard_the_catalog_never_reaches_ledgers_its_epochs(self, tmp_path, caplog):
        # Two-shard cover, one-shard catalog: the unreached shard's epochs are
        # dropped ROWS, not silence — epochs_total reconciles by construction.
        store = _write_store(
            tmp_path / "ref", {SHARD: _instants(0, 5, 11), SHARD_B: _instants(0, 5, 11, 17)}
        )
        items = [_s2_item(f"S2_{i}", _epoch_iso(d)) for i, d in enumerate((0.1, 5.2, 20.0))]
        with caplog.at_level(logging.WARNING, logger="zagg.catalog.closest_obs"):
            sm = closest_obs_shardmap(_s2_catalog(items), store, grid=_grid(), backend="mortie")
        rec = sm.metadata["closest_obs"]
        assert sm.shard_keys == [SHARD_KEY]
        assert rec["shards_without_acquisitions"] == [SHARD_B]
        assert rec["epochs_paired"] == 3
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]
        rows = [d for d in rec["dropped"] if d["shard"] == SHARD_B]
        assert len(rows) == rec["epochs_dropped"] > 0
        assert all(d["nearest_offset_ns"] is None for d in rows)
        assert any("NO spatially-assigned acquisitions" in m for m in caplog.messages)

    def test_a_start_datetime_only_record_still_emits_a_datetime(self, tmp_path):
        # STAC's null-datetime + start/end_datetime form pairs on ``time_start``;
        # the emitted entry must carry the ``datetime`` raster dispatch reads.
        store, _ = self._setup(tmp_path)
        item = _s2_item("S2_range", _epoch_iso(0))
        item["properties"]["datetime"] = None
        item["properties"]["start_datetime"] = _epoch_iso(0)
        item["properties"]["end_datetime"] = _epoch_iso(1)
        cat = _s2_catalog([item])
        assert "datetime" not in cat.granule_records()[0]  # the fixture really lacks it
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        entry = sm.granules[0][0]
        assert entry["datetime"] == entry["time_start"]

    def _two_shard(self, tmp_path):
        """Cover + catalog spanning 11213 and its lon neighbour 11212."""
        store = _write_store(
            tmp_path / "ref", {SHARD: _instants(0, 5, 11), SHARD_B: _instants(0, 5, 11)}
        )
        items = [_s2_item(f"A{i}", _epoch_iso(d)) for i, d in enumerate((0.1, 5.2))]
        items += [_s2_item(f"B{i}", _epoch_iso(d), lon=59.06) for i, d in enumerate((0.1, 5.2))]
        return store, _s2_catalog(items, bbox=(51.0, 12.0, 61.0, 18.0))

    def _spy_build(self, monkeypatch):
        """Record the kwargs the builder hands ``ShardMap.build``."""
        from zagg.catalog.shardmap import ShardMap

        calls = []
        real = ShardMap.build.__func__

        def spy(cls, catalog, grid, **kw):
            calls.append(kw)
            return real(cls, catalog, grid, **kw)

        monkeypatch.setattr(ShardMap, "build", classmethod(spy))
        return calls

    def _pairs(self, sm):
        return {(k, g["id"]) for k, gr in zip(sm.shard_keys, sm.granules) for g in gr}

    def test_an_aoi_excluded_shard_is_not_coverage_disagreement(self, tmp_path, monkeypatch):
        # A Moc aoi has no ring-parts form, so the spatial build stays unscoped
        # and still assigns 11212 — whose epochs the aoi clipped away. That is
        # self-inflicted, not "the reference stores never observed this ground".
        from mortie import Moc

        calls = self._spy_build(monkeypatch)
        store, cat = self._two_shard(tmp_path)
        sm = closest_obs_shardmap(
            cat, store, grid=_grid(), backend="mortie", aoi=Moc.from_polygon(*AOI_AT_SHARD[0])
        )
        assert calls[0]["region"] is None
        assert sm.shard_keys == [SHARD_KEY]
        assert sm.metadata["closest_obs"]["spatial_shards_without_epochs"] == 0

    def test_ring_parts_aoi_scopes_the_spatial_build(self, tmp_path, monkeypatch):
        calls = self._spy_build(monkeypatch)
        store, cat = self._two_shard(tmp_path)
        full = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        scoped = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie", aoi=AOI_AT_SHARD)
        assert calls[0]["region"] is None  # aoi=None is unchanged
        assert calls[1]["region"] is AOI_AT_SHARD
        # Scoping the intersection changes the cost, never the answer.
        assert SHARD_B_KEY in full.shard_keys
        assert self._pairs(scoped) == {p for p in self._pairs(full) if p[0] == SHARD_KEY}

    def test_a_geojson_aoi_scopes_the_spatial_build_as_parts(self, tmp_path, monkeypatch):
        calls = self._spy_build(monkeypatch)
        store, cat = self._two_shard(tmp_path)
        lats, lons = AOI_AT_SHARD[0]
        path = tmp_path / "aoi.geojson"
        path.write_text(
            json.dumps(
                {
                    "type": "Polygon",
                    "coordinates": [[[float(x), float(y)] for x, y in zip(lons, lats)]],
                }
            )
        )
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie", aoi=str(path))
        # Resolved to ring parts before it reaches the build, never the path.
        assert isinstance(calls[0]["region"], list)
        assert sm.shard_keys == [SHARD_KEY]

    def test_reprojecting_a_paired_map_keeps_the_provenance(self, tmp_path):
        # PR #511's pin, flipped by issue #517: ShardMap._granule_entry's key set
        # is closed, so every reproject arm — the same-order noop branch included
        # — used to strip the two provenance keys, leaving a map that is
        # spatially valid but can no longer say why any granule is in it. The
        # generic auxiliary-key passthrough now carries them through verbatim.
        # metadata["closest_obs"] still rides through describing the SOURCE map;
        # that half of the trap is unchanged and stays pinned below.
        store, cat = self._setup(tmp_path)
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        assert "paired_epochs" in sm.granules[0][0]
        noop = sm.reproject(_grid())
        assert noop.granules == sm.granules
        assert noop.granules[0][0]["paired_epochs"] == sm.granules[0][0]["paired_epochs"]
        assert noop.granules[0][0]["epoch_offsets_ns"] == sm.granules[0][0]["epoch_offsets_ns"]
        assert noop.metadata["closest_obs"] == sm.metadata["closest_obs"]

    def test_coarsening_a_paired_map_keeps_the_provenance(self, tmp_path):
        # The coarsen arm regroups the map's own entries rather than rebuilding
        # from a catalog, but it too rebuilt each entry through _granule_entry
        # and dropped the keys with it (issue #517).
        store, cat = self._setup(tmp_path)
        sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        source = sm.granules[0][0]
        coarse = sm.reproject(_grid(parent_order=3))
        carried = [g for shard in coarse.granules for g in shard if g["id"] == source["id"]]
        assert carried, "the granule must survive the coarsen to say anything about its keys"
        assert all(g["paired_epochs"] == source["paired_epochs"] for g in carried)
        assert all(g["epoch_offsets_ns"] == source["epoch_offsets_ns"] for g in carried)


# ── phase 4: two-store scenarios ─────────────────────────────────────────────
#
# An ATL03-like sparse cover beside a GEDI-like denser cover with an interior
# gap, both over shard 11213; the GEDI-like store also claims 11212, where the
# S2 catalog has no acquisitions at all. S2 revisit ~4.3 days across the span.


class TestTwoStoreScenarios:
    # Sparse, nothing near the middle. Day 13 is what makes A's selection
    # NOT a subset of B's (it reaches S2_3, which no B epoch does), so the
    # union-parity test below can actually fail on a dropped store; it is
    # still clear of the gap set (its S2_3 sits at day 12.9, and the first
    # gap acquisition at 17.2 d is 4.2 d away, past the 3 d cap).
    A_DAYS = (0, 13, 55)
    B_DAYS = (0, 2, 4, 6, 8, 10, 50, 52, 54, 56, 58, 60)  # dense, gap 10..50

    def _stores(self, tmp_path):
        a = _write_store(tmp_path / "atl03", {SHARD: _instants(*self.A_DAYS)})
        b = _write_store(
            tmp_path / "gedi",
            {SHARD: _instants(*self.B_DAYS), "11212": _instants(1, 3)},
        )
        return a, b

    def _catalog(self, days=None):
        days = days if days is not None else [d / 10 for d in range(0, 600, 43)]
        return _s2_catalog([_s2_item(f"S2_{i}", _epoch_iso(d)) for i, d in enumerate(days)])

    def test_an_acquisition_in_the_cover_gap_is_never_selected(self, tmp_path):
        """Days 10..50 are a gap in BOTH stores: no epoch reaches into it."""
        a, b = self._stores(tmp_path)
        cat = self._catalog()
        sm = closest_obs_shardmap(
            cat,
            [a, b],
            grid=_grid(),
            backend="mortie",
            max_time_offset=np.timedelta64(3, "D"),
        )
        ids = {g["id"] for g in sm.granules[sm.shard_keys.index(SHARD_KEY)]}
        # Acquisitions land every 4.3 d; those in (13, 47) days sit >3 d from
        # every epoch (epochs live at passes 0..10 and 50..60) so none of them
        # may appear -- the cover gap prunes them even though they are
        # spatially assigned.
        gap = {f"S2_{i}" for i, d in enumerate(d / 10 for d in range(0, 600, 43)) if 13 < d < 47}
        assert gap and not (ids & gap)
        # And the near-gap acquisitions ARE selected -- pinned by name, since
        # a bare `assert ids` passes on any non-empty selection and so cannot
        # fail when the shoulders stop reaching. S2_2 (day 8.6) is reached by
        # the epochs at days 8.098/10.134, S2_12 (day 51.6) by 50.042/52.078.
        assert {"S2_2", "S2_12"} <= ids  # the gap's shoulder epochs reach across

    def test_union_parity_across_stores(self, tmp_path):
        """map(A ∪ B) selects exactly union(map(A), map(B)) per shard.

        Closest-1 is per-epoch independent and epochs are the union across
        stores, so the selection commutes with the union.
        """
        a, b = self._stores(tmp_path)
        cat = self._catalog()
        kw = dict(grid=_grid(), backend="mortie", max_time_offset=np.timedelta64(3, "D"))

        def _pairs(sm):
            return {(k, g["id"]) for k, gr in zip(sm.shard_keys, sm.granules) for g in gr}

        both = _pairs(closest_obs_shardmap(cat, [a, b], **kw))
        only_a = _pairs(closest_obs_shardmap(cat, a, **kw))
        only_b = _pairs(closest_obs_shardmap(cat, b, **kw))
        assert both == only_a | only_b
        # Both stores must CONTRIBUTE, or parity is satisfied by a builder that
        # keeps only one of them: pin each side as a proper subset of the union
        # so neither store's epochs can be silently dropped.
        assert only_a < both and only_b < both

    def test_the_filtered_map_is_a_subset_of_the_spatial_map(self, tmp_path):
        from zagg.catalog.shardmap import ShardMap

        a, b = self._stores(tmp_path)
        cat = self._catalog()
        spatial = ShardMap.build(cat, _grid(), backend="mortie")
        sm = closest_obs_shardmap(cat, [a, b], grid=_grid(), backend="mortie")
        spatial_pairs = {
            (k, g["id"]) for k, gr in zip(spatial.shard_keys, spatial.granules) for g in gr
        }
        got = {(k, g["id"]) for k, gr in zip(sm.shard_keys, sm.granules) for g in gr}
        assert got and got <= spatial_pairs

    def test_a_covered_shard_with_no_acquisitions_is_recorded(self, tmp_path):
        a, b = self._stores(tmp_path)
        cat = self._catalog()  # items only around shard 11213's centre
        sm = closest_obs_shardmap(cat, [a, b], grid=_grid(), backend="mortie")
        rec = sm.metadata["closest_obs"]
        assert "11212" in rec["shards_without_acquisitions"]
        # Membership alone would pass while the epochs went unledgered; the
        # shard's two cover epochs (days 1 and 3) must each show up in
        # ``dropped`` with no offset -- there is no acquisition to measure.
        rows = [d for d in rec["dropped"] if d["shard"] == "11212"]
        assert len(rows) == 2
        assert all(d["nearest_offset_ns"] is None for d in rows)
        got = np.sort(np.array([d["epoch"] for d in rows], dtype="datetime64[ns]"))
        assert _nearest_gap(got, _utc(_instants(1, 3))) <= HALF_BUCKET
        assert morton_word("11212") not in sm.shard_keys

    def test_offset_boundary_exactly_at_selects_one_ns_past_drops(self, tmp_path):
        """The cap boundary at BUILDER level, against a cover-derived epoch."""
        store = _write_store(tmp_path / "ref", {SHARD: _instants(0)})
        epochs = reference_epochs(store).epochs[SHARD_KEY]
        assert epochs.size == 1
        acq_iso = _epoch_iso(2.0)
        cat = _s2_catalog([_s2_item("S2_only", acq_iso)])
        acq = np.datetime64(acq_iso.rstrip("Z")).astype("datetime64[ns]")
        exact = acq - epochs[0]  # signed timedelta64[ns], acquisition - epoch
        assert exact > np.timedelta64(0, "ns")
        kw = dict(grid=_grid(), backend="mortie")
        at = closest_obs_shardmap(cat, store, max_time_offset=exact, **kw)
        assert [g["id"] for g in at.granules[0]] == ["S2_only"]
        assert at.granules[0][0]["epoch_offsets_ns"] == [int(exact.astype("int64"))]
        past = closest_obs_shardmap(
            cat, store, max_time_offset=exact - np.timedelta64(1, "ns"), **kw
        )
        assert past.shard_keys == []
        rec = past.metadata["closest_obs"]
        assert rec["epochs_dropped"] == 1
        assert rec["dropped"][0]["nearest_offset_ns"] == int(exact.astype("int64"))


# ── espg tolerance ruling (2026-08-24, thread r3845481805) ───────────────────


def _write_store_at_order(root, decimal, instants, block_order, order=4):
    """A store whose cover block sits EXPLICITLY at ``block_order`` < the pin."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    words = quantize_words(time2toc(np.asarray(instants, dtype=np.uint64)), block_order)
    section = {
        "spec": COVER_SPEC,
        "source": "test",
        "order": order,
        "temporal_order": TEMPORAL_COVER_ORDER,
        "cap": COVER_CAP,
        "fields": ["h"],
        "element": {"dtype": "uint64", "shape": [-1]},
        "encoding": "base64",
        "shards": {decimal: _encode_cover_block(np.asarray(words, np.uint64), block_order)},
    }
    (root / COVER_NAME).write_text(json.dumps(section))
    return str(root)


class TestCoarsenedCoverTolerance:
    """max_time_offset is a precision bar on the epochs, not just the offsets.

    The ruling of record: with a cap set, an epoch whose bucket half-span
    exceeds it cannot be paired to the stated precision and drops loudly as
    its own ledger category; with no cap, one warning names the effective
    resolution and pairing proceeds (widening is lawful, §10.5). Flat-warn
    risks silently arbitrary pairings from a cap-degraded store; flat-refuse
    fails whole builds over blocks that may not even intersect the AOI.
    """

    #: An order-12 bucket's half-span: 2^50 ns ≈ 13.03 days.
    ORDER12_HALF = np.timedelta64(2**50, "ns")

    def _coarse_store(self, tmp_path, days=(40.0,), name="coarse"):
        return _write_store_at_order(tmp_path / name, SHARD, _instants(*days), 12)

    def test_low_resolution_epochs_drop_under_a_cap(self, tmp_path):
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))])
        sm = closest_obs_shardmap(
            cat, store, grid=_grid(), backend="mortie", max_time_offset=np.timedelta64(3, "D")
        )
        rec = sm.metadata["closest_obs"]
        # Every epoch in the order-12 block is unresolvable at ±3 d.
        assert sm.shard_keys == []
        assert rec["epochs_total"] > 0
        assert rec["epochs_dropped_low_resolution"] == rec["epochs_dropped"] == rec["epochs_total"]
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]
        row = rec["dropped"][0]
        assert row["temporal_order"] == 12
        assert row["cover_half_span_ns"] == 2**50
        # Its own category: no near-miss offset — the selection never ran.
        assert "nearest_offset_ns" not in row

    def test_the_estimate_reports_the_category(self, tmp_path):
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))])
        est = closest_obs_shardmap(
            cat,
            store,
            grid=_grid(),
            backend="mortie",
            estimate=True,
            max_time_offset=np.timedelta64(3, "D"),
        )
        assert est["epochs_dropped_low_resolution"] == est["epochs_total"] > 0
        assert est["dropped"][0]["temporal_order"] == 12

    def test_no_cap_warns_once_and_pairs_everything(self, tmp_path, caplog):
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))])
        with caplog.at_level(logging.WARNING, logger="zagg.catalog.closest_obs"):
            sm = closest_obs_shardmap(cat, store, grid=_grid(), backend="mortie")
        build_warnings = [m for m in caplog.messages if "no max_time_offset" in m]
        assert len(build_warnings) == 1
        assert "temporal order 12" in build_warnings[0]
        rec = sm.metadata["closest_obs"]
        assert rec["epochs_dropped_low_resolution"] == 0
        assert rec["epochs_paired"] == rec["epochs_total"] > 0
        assert {g["id"] for g in sm.granules[0]} == {"S2_0"}

    def test_the_cap_arm_warns_that_a_coarsened_cover_dropped_them(self, tmp_path, caplog):
        """Symmetric to the no-cap arm: the arm that DISCARDS is at least as loud.

        The no-cap arm names the effective resolution and proceeds; the
        with-cap arm throws epochs away, so it gets its own line naming the
        count and the cause. The generic summary must stop mis-attributing
        these rows to distance or a catalog gap — neither ran for them.
        """
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))])
        with caplog.at_level(logging.WARNING, logger="zagg.catalog.closest_obs"):
            sm = closest_obs_shardmap(
                cat, store, grid=_grid(), backend="mortie", max_time_offset=np.timedelta64(3, "D")
            )
        n = sm.metadata["closest_obs"]["epochs_dropped_low_resolution"]
        assert n > 0
        drops = [m for m in caplog.messages if "UNRESOLVABLE" in m]
        assert len(drops) == 1
        assert f"{n} epoch(s) dropped as UNRESOLVABLE" in drops[0]
        assert "a coarsened cover, NOT distance" in drops[0]
        assert "temporal order 12" in drops[0]
        # ...and the generic summary now names the third cause.
        summary = [m for m in caplog.messages if "selected nothing" in m]
        assert len(summary) == 1
        assert "cover block too coarse for the offset" in summary[0]
        # The no-cap line is the OTHER arm's; it must not fire under a cap.
        assert not [m for m in caplog.messages if "no max_time_offset" in m]

    def test_half_span_exactly_at_the_cap_still_pairs(self, tmp_path):
        """The pinned boundary side: half-span == max_time_offset is pairable.

        Strictly-greater drops, matching the selection gate where an offset
        exactly at the cap SELECTS — both boundaries sit on the permissive
        side. (The acquisition inside the epoch's own bucket is within
        half-span of its midpoint, so the selection also passes.)
        """
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))])
        sm = closest_obs_shardmap(
            cat, store, grid=_grid(), backend="mortie", max_time_offset=self.ORDER12_HALF
        )
        rec = sm.metadata["closest_obs"]
        assert rec["epochs_dropped_low_resolution"] == 0
        assert rec["epochs_paired"] == rec["epochs_total"] > 0
        one_ns_under = self.ORDER12_HALF - np.timedelta64(1, "ns")
        dropped = closest_obs_shardmap(
            cat, store, grid=_grid(), backend="mortie", max_time_offset=one_ns_under
        )
        assert (
            dropped.metadata["closest_obs"]["epochs_dropped_low_resolution"] == rec["epochs_total"]
        )

    def test_mixed_orders_drop_only_the_coarse_epochs(self, tmp_path):
        """One shard, a pinned store beside a coarsened one: per-epoch gating."""
        fine = _write_store(tmp_path / "fine", {SHARD: _instants(0, 5)})
        coarse = self._coarse_store(tmp_path)
        ref = reference_epochs([fine, coarse])
        assert set(ref.epoch_orders[SHARD_KEY].tolist()) == {TEMPORAL_COVER_ORDER, 12}
        n_coarse = int((ref.epoch_orders[SHARD_KEY] == 12).sum())
        assert n_coarse > 0
        cat = _s2_catalog(
            [_s2_item(f"S2_{i}", _epoch_iso(d)) for i, d in enumerate((0.1, 5.2, 40.0))]
        )
        sm = closest_obs_shardmap(
            cat,
            [fine, coarse],
            grid=_grid(),
            backend="mortie",
            max_time_offset=np.timedelta64(3, "D"),
        )
        rec = sm.metadata["closest_obs"]
        # Only the coarse-block epochs fail the precision bar; the pinned
        # store's epochs pair through the same shard.
        assert rec["epochs_dropped_low_resolution"] == n_coarse
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]
        ids = {g["id"] for g in sm.granules[0]}
        assert ids == {"S2_0", "S2_1"}

    def test_an_unreached_shard_still_counts_its_resolution_drops(self, tmp_path):
        """The category is a property of the epoch, not of the catalog's reach.

        Unresolvability follows from the epoch's own cover block, so it must
        not flip to a no-acquisition row just because the raster catalog
        never reaches the shard — an operator sizing ``max_time_offset`` off
        ``epochs_dropped_low_resolution == 0`` would fix the catalog gap and
        watch the same epochs reappear as resolution drops.
        """
        store = self._coarse_store(tmp_path)
        cat = _s2_catalog(
            [_s2_item("S2_far", _epoch_iso(40.0), lat=-40.0, lon=-120.0)],
            bbox=(-121.0, -41.0, -119.0, -39.0),
        )
        sm = closest_obs_shardmap(
            cat, store, grid=_grid(), backend="mortie", max_time_offset=np.timedelta64(3, "D")
        )
        rec = sm.metadata["closest_obs"]
        assert rec["epochs_dropped_low_resolution"] == rec["epochs_total"] > 0
        assert all("temporal_order" in d for d in rec["dropped"])
        # The shard is still named: that row is about the SHARD, not its epochs.
        assert rec["shards_without_acquisitions"] == [SHARD]
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]

    def test_an_unreached_shard_splits_coarse_from_surviving_epochs(self, tmp_path):
        """Mixed orders + no acquisitions: each epoch lands in exactly one class."""
        fine = _write_store(tmp_path / "fine", {SHARD: _instants(0, 5)})
        coarse = self._coarse_store(tmp_path)
        ref = reference_epochs([fine, coarse])
        n_coarse = int((ref.epoch_orders[SHARD_KEY] == 12).sum())
        n_fine = int(ref.epoch_orders[SHARD_KEY].size - n_coarse)
        assert n_coarse > 0 and n_fine > 0
        cat = _s2_catalog(
            [_s2_item("S2_far", _epoch_iso(0.1), lat=-40.0, lon=-120.0)],
            bbox=(-121.0, -41.0, -119.0, -39.0),
        )
        sm = closest_obs_shardmap(
            cat,
            [fine, coarse],
            grid=_grid(),
            backend="mortie",
            max_time_offset=np.timedelta64(3, "D"),
        )
        rec = sm.metadata["closest_obs"]
        assert rec["epochs_dropped_low_resolution"] == n_coarse
        # The survivors of the precision bar become no-acquisition rows.
        gap_rows = [d for d in rec["dropped"] if "nearest_offset_ns" in d]
        assert len(gap_rows) == n_fine
        assert all(d["nearest_offset_ns"] is None for d in gap_rows)
        assert rec["shards_without_acquisitions"] == [SHARD]
        assert rec["epochs_total"] == rec["epochs_paired"] + rec["epochs_dropped"]

    def test_a_negative_block_order_refuses_by_name(self, tmp_path):
        """Fail-CLOSED on a corrupt block order, rather than through the bar.

        ``coverage_toc._decode_cover_block``'s §10.5 order check is one-sided
        (ceiling only), so ``temporal_order: -1`` decodes; the half-span shift
        then overflows int64 to a huge NEGATIVE, which passes any cap, on a
        midpoint ``_word_midpoints`` invents (2142-04-11 for this block). The
        durable lower bound belongs beside that ceiling check; this is the
        read-boundary refusal on the builder's side.
        """
        root = Path(tmp_path / "corrupt")
        root.mkdir(parents=True, exist_ok=True)
        words = quantize_words(time2toc(_instants(40.0)), 12)
        block = _encode_cover_block(np.asarray(words, np.uint64), 12)
        block["temporal_order"] = -1
        (root / COVER_NAME).write_text(
            json.dumps(
                {
                    "spec": COVER_SPEC,
                    "source": "test",
                    "order": 4,
                    "temporal_order": TEMPORAL_COVER_ORDER,
                    "cap": COVER_CAP,
                    "fields": ["h"],
                    "element": {"dtype": "uint64", "shape": [-1]},
                    "encoding": "base64",
                    "shards": {SHARD: block},
                }
            )
        )
        for call in (
            lambda: reference_epochs(str(root)),
            lambda: closest_obs_shardmap(
                _s2_catalog([_s2_item("S2_0", _epoch_iso(40.0))]),
                str(root),
                grid=_grid(),
                backend="mortie",
                max_time_offset=np.timedelta64(3, "D"),
            ),
        ):
            with pytest.raises(ValueError, match="temporal_order -1"):
                call()
