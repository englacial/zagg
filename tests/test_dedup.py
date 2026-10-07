"""has_run dedup statuses (issue #299 phase 4, D19).

The acceptance criteria from the issue thread: "hit" only when identity AND
catalog match; a catalog-grown shard reports "stale", never "hit"; debris and
absent leaves are plain misses.
"""

import copy
import hashlib
import pathlib

import numpy as np
import pytest
import zarr

from zagg import hive
from zagg.config import default_config
from zagg.dedup import classify_leaf_identity, has_run, leaf_recorded_ids, shard_status
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_word
from zagg.semantics import semantic_hash
from zagg.store import open_store
from zagg.telemetry import (
    build_record,
    granule_ids_key,
    granule_ids_path,
    granules_sha256,
    sidecar_key,
    write_granule_ids,
    write_sidecar,
)

WORD = morton_word("1121121")  # order-6 shard key
GRANULES = ["s3://b/g1.h5", "s3://b/g2.h5"]


def _cfg():
    return default_config("atl06")


def _grid(cfg):
    return HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)


def _write_leaf(
    root,
    cfg,
    *,
    stamp=True,
    sidecar=True,
    sidecar_hash="match",
    granules=GRANULES,
    stamp_hashes=None,
):
    """Emit + optionally stamp a leaf, optionally with a stats sidecar."""
    leaf = hive.shard_leaf_path(root, WORD)
    store = open_store(leaf)
    _grid(cfg).emit_shard_template(store, overwrite=True)
    if stamp:
        group = zarr.open_group(store, path="", mode="a", zarr_format=3)
        recorded_stamp = {"cells_with_data": 1}
        if stamp_hashes is not None:
            recorded_stamp["content_hashes"] = stamp_hashes
        group.attrs[hive.COMMIT_ATTR] = recorded_stamp
    if sidecar:
        recorded = {
            "match": semantic_hash(cfg),
            "other": "0" * 64,
            "absent": None,
        }[sidecar_hash]
        record = build_record(
            shard_key=WORD,
            metadata={"total_obs": 2, "cells_with_data": 1, "duration_s": 0.1},
            granule_ids=granules,
            semantic_hash=recorded,
        )
        write_sidecar(leaf, record)
    return leaf


class TestShardStatus:
    def test_absent_leaf_is_miss(self, tmp_path):
        cfg = _cfg()
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status == {"status": "miss"}

    def test_unstamped_leaf_is_miss(self, tmp_path):
        # Debris (D4): a template without the commit stamp is invisible.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, stamp=False, sidecar=False)
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status["status"] == "miss"

    def test_stamped_without_sidecar_is_stale(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, sidecar=False)
        status = shard_status(str(tmp_path), WORD, semantic_hash=semantic_hash(cfg))
        assert status["status"] == "stale"
        assert "sidecar" in status["reason"]

    def test_full_match_is_hit(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(cfg), granule_ids=GRANULES
        )
        assert status["status"] == "hit"
        assert status["semantic_hash_match"] is True
        assert status["catalog_match"] is True

    def test_content_hashes_come_from_the_stamp_when_it_carries_them(self, tmp_path):
        # Issue #580: the stamp is the §5.3 preferred copy — sealed with the
        # bytes it certifies — so a leaf whose sidecar predates the key (or
        # lost it to the fail-open PUT) still surfaces the verifier.
        cfg = _cfg()
        record = {"arrays": {"8/morton": "a" * 64}, "combined": "b" * 64}
        _write_leaf(str(tmp_path), cfg, stamp_hashes=record)
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(cfg), granule_ids=GRANULES
        )
        assert status["status"] == "hit"
        assert status["content_hashes"] == record

    def test_catalog_growth_is_stale_never_hit(self, tmp_path):
        # The headline acceptance criterion: ATL03 is a living collection —
        # a grown catalog must recompute, not skip.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        status = shard_status(
            str(tmp_path),
            WORD,
            semantic_hash=semantic_hash(cfg),
            granule_ids=[*GRANULES, "s3://b/g3.h5"],
        )
        assert status["status"] == "stale"
        assert status["catalog_match"] is False
        assert status["semantic_hash_match"] is True
        # F8: the recorded vs current digests are surfaced so a systematic
        # id-space mismatch is distinguishable from a genuine catalog change.
        assert status["granules_sha256_recorded"] != status["granules_sha256_current"]

    def test_semantic_mismatch_is_stale(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        other = copy.deepcopy(cfg)
        other.aggregation["variables"]["count"]["dtype"] = "int64"
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(other), granule_ids=GRANULES
        )
        assert status["status"] == "stale"
        assert status["semantic_hash_match"] is False

    def test_pre299_sidecar_is_stale(self, tmp_path):
        # A pre-#299 sidecar records no semantic_hash: unverifiable identity
        # degrades to recompute, never to a false hit.
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg, sidecar_hash="absent")
        status = shard_status(
            str(tmp_path), WORD, semantic_hash=semantic_hash(cfg), granule_ids=GRANULES
        )
        assert status["status"] == "stale"


class TestHasRun:
    def test_mapping_input_checks_catalog(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        out = has_run(str(tmp_path), cfg, {WORD: GRANULES})
        assert out[WORD]["status"] == "hit"
        grown = has_run(str(tmp_path), cfg, {WORD: [*GRANULES, "s3://b/new.h5"]})
        assert grown[WORD]["status"] == "stale"

    def test_iterable_input_skips_catalog_check(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        out = has_run(str(tmp_path), cfg, [WORD])
        assert out[WORD]["status"] == "hit"
        assert out[WORD]["catalog_match"] is None

    def test_spec_defaults_from_manifest(self, tmp_path):
        # The sidecar key grammar is spec-keyed (#307); has_run reads the
        # manifest once for it. A manifest-less root uses the legacy names.
        cfg = _cfg()
        root = str(tmp_path)
        hive.ensure_manifest(root, hive.build_manifest(_grid(cfg)))
        _write_leaf(root, cfg)
        assert has_run(root, cfg, [WORD])[WORD]["status"] == "hit"

    def test_missing_shards_reported(self, tmp_path):
        cfg = _cfg()
        _write_leaf(str(tmp_path), cfg)
        other = morton_word("2431123")
        out = has_run(str(tmp_path), cfg, [WORD, other])
        assert out[WORD]["status"] == "hit"
        assert out[other]["status"] == "miss"

    def test_windowed_leaf_status(self, tmp_path):
        # A windowed store (issue #246): the (shard, window) leaf carries its
        # own legacy `stats_{window}.json` sidecar (spec=None), and has_run must
        # key on the window so a hit for one window is a miss for another.
        cfg = _cfg()
        root = str(tmp_path)
        leaf = hive.shard_leaf_path(root, WORD, window="2025")
        assert sidecar_key(leaf.rstrip("/").rsplit("/", 1)[-1]) == "stats_2025.json"
        store = open_store(leaf)
        _grid(cfg).emit_shard_template(store, overwrite=True)
        group = zarr.open_group(store, path="", mode="a", zarr_format=3)
        group.attrs[hive.COMMIT_ATTR] = {"cells_with_data": 1}
        write_sidecar(
            leaf,
            build_record(
                shard_key=WORD,
                metadata={"total_obs": 2, "cells_with_data": 1, "duration_s": 0.1},
                granule_ids=GRANULES,
                semantic_hash=semantic_hash(cfg),
            ),
        )
        hit = has_run(root, cfg, {WORD: GRANULES}, window="2025")
        assert hit[WORD]["status"] == "hit"
        # A different window has no leaf: a plain miss (windows are disjoint).
        miss = has_run(root, cfg, {WORD: GRANULES}, window="2024")
        assert miss[WORD]["status"] == "miss"


class TestClassifyLeafIdentity:
    """Worker-side identity readings (issue #388): equal / expansion /
    contraction / mixed, with every ambiguity degrading to rewrite.

    The recorded id LIST is not in the sidecar (the ruling on question (6)):
    it comes from a sibling object through a lazy loader, so these fix the
    sidecar's hash and hand the classifier a loader whose call count is
    itself asserted."""

    SEM = "a" * 64
    IDS = ["s3://b/g1.h5", "s3://b/g2.h5", "s3://b/g3.h5"]

    def _classify(self, planned_ids, *, ids=None, semantic=SEM, want=SEM, sibling=True):
        """Classify ``planned_ids`` against a leaf recording ``ids``.

        ``sibling=False`` models a leaf with no recorded id list at all — a
        pre-#388 leaf, a lost PUT, or a sibling that failed to pair. The
        loader's call count lands on ``self.loads``."""
        recorded_ids = self.IDS if ids is None else ids
        recorded = {
            "semantic_hash": semantic,
            "granules_sha256": granules_sha256(recorded_ids),
        }
        self.loads = 0

        def _load():
            self.loads += 1
            return sorted(recorded_ids) if sibling else None

        return classify_leaf_identity(
            recorded,
            semantic_hash=want,
            planned_ids=planned_ids,
            load_recorded_ids=_load,
        )

    def test_equal_skips_on_the_hash_fast_path(self):
        # Order must not matter: the hash is over the sorted ids. And the
        # sibling is NEVER read on this path — the whole point of the split.
        got = self._classify(self.IDS[::-1])
        assert got == {"action": "skip", "classification": "equal", "missing": []}
        assert self.loads == 0

    def test_equal_without_a_recorded_sibling_still_skips(self):
        # The fast path never needs the id list, so a leaf whose sibling is
        # absent (pre-#388, or a lost PUT) still skips when the hash matches.
        got = self._classify(self.IDS, sibling=False)
        assert got["action"] == "skip"
        assert self.loads == 0

    def test_the_sibling_is_read_once_on_mismatch(self):
        # The one read the split pays for, and only once per classification.
        self._classify(self.IDS[:2])
        assert self.loads == 1

    def test_missing_loader_reads_as_unrecorded(self):
        # A caller that supplies none (or a pure caller with no store in
        # scope) has no recorded set to diff: today's rewrite, counted apart.
        got = classify_leaf_identity(
            {"semantic_hash": self.SEM, "granules_sha256": granules_sha256(self.IDS)},
            semantic_hash=self.SEM,
            planned_ids=self.IDS[:2],
        )
        assert got == {"action": "rewrite", "classification": "unrecorded-ids", "missing": []}

    def test_expansion_rewrites(self):
        got = self._classify(self.IDS + ["s3://b/g4.h5"])
        assert got == {"action": "rewrite", "classification": "expansion", "missing": []}

    def test_pure_contraction_refuses_and_names_ids(self):
        # ``missing`` is named in the CANONICAL id space (espg-ruled
        # 2026-08-17): the driver-stripped bare granule id, which is also the
        # only name that means the same thing whatever driver the run read with.
        got = self._classify(self.IDS[:2])
        assert got["action"] == "refuse"
        assert got["classification"] == "contraction"
        assert got["missing"] == ["g3.h5"]

    def test_mixed_add_and_drop_refuses(self):
        # The ruled predicate is recorded - planned != {} — NOT strict subset:
        # the planned set can even be LARGER while data drops (the upstream
        # purge behind a fresh catalog query, the espg contraction ruling).
        got = self._classify(self.IDS[:2] + ["s3://b/new1.h5", "s3://b/new2.h5"])
        assert got["action"] == "refuse"
        assert got["classification"] == "mixed"
        assert got["missing"] == ["g3.h5"]

    def test_contraction_beats_semantic_mismatch(self):
        # Dropping inputs refuses even when the semantic hash also changed —
        # the guard is about data loss, not intent drift.
        got = self._classify(self.IDS[:1], semantic="b" * 64)
        assert got["action"] == "refuse"
        assert got["missing"] == ["g2.h5", "g3.h5"]

    def test_no_sidecar_rewrites(self):
        got = classify_leaf_identity(None, semantic_hash=self.SEM, planned_ids=self.IDS)
        assert got == {"action": "rewrite", "classification": "no-sidecar", "missing": []}

    def test_absent_sibling_on_mismatch_rewrites(self):
        # Hash mismatch + no recorded id list: undecidable, today's rewrite.
        got = self._classify(self.IDS[:2], sibling=False)
        assert got == {"action": "rewrite", "classification": "unrecorded-ids", "missing": []}

    def test_semantic_mismatch_with_equal_sets_rewrites_not_refuses(self):
        got = self._classify(self.IDS, semantic="b" * 64)
        assert got == {"action": "rewrite", "classification": "semantic-mismatch", "missing": []}
        # And it costs NO sibling read: equal hashes mean an identical id
        # multiset, so the diff cannot change the verdict. This is the routine
        # config-only rerun — one needless ~550 KB GET per shard otherwise.
        assert self.loads == 0

    def test_semantic_mismatch_without_a_sibling_is_not_unrecorded(self):
        # Same quadrant over a pre-#388 leaf: the guard is NOT inert here (the
        # hashes prove no contraction), so it must not report as unrecorded —
        # a config-only rerun over a pre-#388 store would otherwise come back
        # 100% cells_unrecorded.
        got = self._classify(self.IDS, semantic="b" * 64, sibling=False)
        assert got == {"action": "rewrite", "classification": "semantic-mismatch", "missing": []}
        assert self.loads == 0

    def test_null_recorded_semantic_never_skips(self):
        # Fleet-written pre-#388 vector sidecars record semantic_hash null
        # (the Lambda handler passes none): never provably current.
        got = self._classify(self.IDS, semantic=None)
        assert got["action"] == "rewrite"

    def test_caller_without_semantic_hash_never_skips(self):
        got = self._classify(self.IDS, want=None)
        assert got["action"] == "rewrite"

    def test_empty_planned_set_is_pure_contraction(self):
        got = self._classify([])
        assert got["action"] == "refuse"
        assert got["classification"] == "contraction"
        assert got["missing"] == ["g1.h5", "g2.h5", "g3.h5"]

    def test_unknown_planned_set_rewrites_rather_than_refusing(self):
        # None is UNKNOWN, not empty: it must not diff to "every recorded id
        # was dropped" and refuse — the maximally ambiguous input takes the
        # conservative rewrite, the same direction as every other ambiguity.
        got = self._classify(None)
        assert got == {"action": "rewrite", "classification": "unknown-planned", "missing": []}
        assert self.loads == 0

    def test_planned_ids_are_iterated_not_truth_tested(self):
        # A numpy id array raises ValueError on a bare truthiness test; the
        # planned set is iterated instead, so array-shaped callers work.
        assert self._classify(np.array(self.IDS))["action"] == "skip"
        empty = self._classify(np.array([], dtype=object))
        assert empty["action"] == "refuse"  # an empty ARRAY is still empty, not unknown

    def test_duplicate_drift_with_equal_sets_rewrites(self):
        # granules_sha256 keeps duplicates; the set diff dedups. Same set,
        # different multiset -> not current, not a contraction.
        got = self._classify(self.IDS, ids=self.IDS + [self.IDS[0]])
        assert got == {"action": "rewrite", "classification": "id-multiset-drift", "missing": []}

    @pytest.mark.parametrize(
        "planned",
        [
            ["https://h/g1.h5", "https://h/g2.h5", "https://h/g3.h5"],  # driver: https
            ["b/g1.h5", "b/g2.h5", "b/g3.h5"],  # the s3 driver's stripped form
            ["g1.h5", "g2.h5", "g3.h5"],  # bare catalog ids
            [{"url": "https://h/g1.h5", "assets": {"l2a": "https://h/g1b.h5"}}, *IDS[1:]],
        ],
    )
    def test_a_driver_switch_still_reads_current(self, planned):
        # The epoch's canonical-identity ruling at the gate (espg-ruled
        # 2026-08-17, PR #420 question (1)(b)): the driver is packaging in the
        # D19 core, so rerunning the SAME granules through a different driver --
        # or against a leaf whose sibling recorded the other href form -- must
        # read `equal`. Pre-epoch every one of these renamed all three ids and
        # the gate went down the expansion arm, rewriting the whole store.
        got = self._classify(planned)
        assert got == {"action": "skip", "classification": "equal", "missing": []}
        assert self.loads == 0  # the hash fast path, not the diff, decides

    def test_collapsed_recorded_ids_degrade_the_guard_loudly(self, caplog):
        # The accepted cost of the canonical-identity ruling, pinned where it
        # actually lands: two recorded hrefs differing only in prefix are ONE
        # canonical id, so a genuine drop of one of them leaves the set diff
        # unchanged and the leaf REWRITES where the pre-epoch href-space diff
        # refused and named the dropped href. Resolving that the other way is a
        # ruling on the contraction predicate (a multiset diff would refuse the
        # duplicate-drift case `test_duplicate_drift_with_equal_sets_rewrites`
        # pins as a rewrite), so this pins the state as-shipped -- and pins that
        # it is never silent.
        recorded_ids = ["s3://v006/ATL06_X.h5", "s3://v007/ATL06_X.h5", "s3://v007/ATL06_Y.h5"]
        with caplog.at_level("WARNING", logger="zagg.dedup"):
            got = self._classify(
                ["s3://v007/ATL06_X.h5", "s3://v007/ATL06_Y.h5"],  # one granule dropped
                ids=recorded_ids,
            )
        assert got == {"action": "rewrite", "classification": "id-multiset-drift", "missing": []}
        assert "collapsed distinct recorded granule ids" in caplog.text
        assert "ATL06_X.h5" in caplog.text  # the colliding canonical id is named

    @pytest.mark.parametrize(
        "planned",
        [
            ["g1.h5", "g2.h5", "g3.h5"],  # the post-epoch plan, canonical
            IDS,  # ...and the pre-epoch spelling: same verdict, same reason
        ],
    )
    def test_a_pre_epoch_leaf_rewrites_rather_than_refusing(self, planned):
        # The PRE-epoch half of the driver-switch story, which no other pin
        # covers: a leaf written before the epoch recorded its `granules_sha256`
        # over RESOLVED HREFS, so the canonical-space fast path mismatches by
        # construction and `equal` is unreachable no matter how unchanged the
        # granules are -- the "every pre-epoch granules_sha256 invalidated"
        # headline of the migration note, and why such a leaf rewrites once.
        # What canonicalizing the RECORDED side buys is the DIRECTION of that
        # rewrite: the sibling's hrefs reduce to the same bare ids as the plan,
        # so the leaf reads `id-multiset-drift` and rewrites instead of REFUSING
        # as a spurious contraction -- which is what a driver switch over a
        # pre-epoch store did before, and what would otherwise need
        # `--allow-contraction` to get past.
        href_space = hashlib.sha256("\n".join(sorted(self.IDS)).encode()).hexdigest()
        assert href_space != granules_sha256(self.IDS)  # the epoch, in one line
        self.loads = 0

        def _load():
            self.loads += 1
            return sorted(self.IDS)  # the sibling is href-space too

        got = classify_leaf_identity(
            {"semantic_hash": self.SEM, "granules_sha256": href_space},
            semantic_hash=self.SEM,
            planned_ids=planned,
            load_recorded_ids=_load,
        )
        assert got == {"action": "rewrite", "classification": "id-multiset-drift", "missing": []}
        assert self.loads == 1  # the fast path cannot decide it: one sibling GET


class TestLeafRecordedIds:
    """The sibling read (issue #388): pairs with its sidecar or reads absent."""

    IDS = ["s3://b/g2.h5", "s3://b/g1.h5"]
    #: What the sibling records for :data:`IDS` — the canonical driver-stripped
    #: bare ids, sorted (espg-ruled 2026-08-17).
    CANON = ["g1.h5", "g2.h5"]

    def _leaf(self, tmp_path):
        leaf = hive.shard_leaf_path(str(tmp_path), WORD)
        (pathlib.Path(leaf).parent).mkdir(parents=True, exist_ok=True)
        return leaf

    def _sidecar(self, ids):
        return {"granules_sha256": granules_sha256(ids)}

    def test_round_trips_the_recorded_set_sorted(self, tmp_path):
        # Recorded in the CANONICAL id space (espg-ruled 2026-08-17): the
        # sibling holds bare granule ids whatever href form the caller passed,
        # which is what makes it pair with the sidecar hash beside it.
        leaf = self._leaf(tmp_path)
        assert write_granule_ids(leaf, self.IDS) is True
        got = leaf_recorded_ids(leaf, self._sidecar(self.IDS))
        assert got == self.CANON

    def test_absent_sibling_reads_none(self, tmp_path):
        assert leaf_recorded_ids(self._leaf(tmp_path), self._sidecar(self.IDS)) is None

    def test_unpaired_sibling_is_rejected(self, tmp_path):
        # A torn rewrite (sibling from one run, sidecar from another) must
        # read as unrecorded — a stale set would name the wrong granules as
        # dropped, or hide real ones.
        leaf = self._leaf(tmp_path)
        write_granule_ids(leaf, self.IDS)
        assert leaf_recorded_ids(leaf, self._sidecar(self.IDS + ["s3://b/g3.h5"])) is None

    def test_malformed_sibling_reads_none(self, tmp_path):
        leaf = self._leaf(tmp_path)
        write_granule_ids(leaf, self.IDS)
        path = pathlib.Path(granule_ids_path(leaf))
        for body in ("[]", '"nope"', '{"granule_ids": "not-a-list"}', "{oh no"):
            path.write_text(body)
            assert leaf_recorded_ids(leaf, self._sidecar(self.IDS)) is None

    def test_the_spec_marker_is_read_leniently(self, tmp_path):
        # The hash pairing decides, not the marker: a sibling with an unknown
        # or absent spec must still read (a future zagg-granule-ids/N that
        # keeps these keys), and never raise — the ruled fallback posture is
        # unrecorded-ids, not an error.
        import json

        leaf = self._leaf(tmp_path)
        write_granule_ids(leaf, self.IDS)
        path = pathlib.Path(granule_ids_path(leaf))
        body = json.loads(path.read_text())
        for spec in ("zagg-granule-ids/99", None):
            body["spec"] = spec
            path.write_text(json.dumps(body))
            assert leaf_recorded_ids(leaf, self._sidecar(self.IDS)) == self.CANON

    def test_record_borne_ids_are_read_when_no_sibling_exists(self, tmp_path):
        # The 3f13af2..this-PR window: PR #397 put the id list ON the record
        # and there is no sibling beside those leaves. Falling back to it
        # keeps the guard armed over them instead of classifying every one
        # unrecorded-ids forever.
        recorded = dict(self._sidecar(self.IDS), granule_ids=sorted(self.IDS))
        assert leaf_recorded_ids(self._leaf(tmp_path), recorded) == sorted(self.IDS)
        # A malformed record-borne value is still no recorded set.
        assert leaf_recorded_ids(self._leaf(tmp_path), {"granule_ids": "nope"}) is None

    def test_the_sibling_wins_over_a_record_borne_list(self, tmp_path):
        # A store rewritten under this release has both for one run; the
        # sibling is the current writer, so it is what the diff must use.
        leaf = self._leaf(tmp_path)
        write_granule_ids(leaf, self.IDS)
        recorded = dict(self._sidecar(self.IDS), granule_ids=["s3://b/stale.h5"])
        assert leaf_recorded_ids(leaf, recorded) == self.CANON

    def test_windowed_sibling_is_per_window(self, tmp_path):
        # Same grammar as the sidecar: two windows of one shard cannot
        # clobber each other's recorded set.
        assert granule_ids_key("1121121.zarr") == "granules.json"
        assert granule_ids_key("1121121_2025.zarr") == "granules_2025.json"
        assert granule_ids_key("2025.zarr", "morton-hive/3") == "2025.granules.json"
