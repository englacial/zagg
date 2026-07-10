"""Tier-0 coverage: the morton box on the hive commit stamp — issue #200 phase 1.

Design contract: ``docs/design/sparse_coverage.md`` §4 (tiered coverage, as
amended on PR #206). The box is the canonical <= 4-member cover of a shard's
occupied cells (DCA children, each tightened — PR #208 finding 1), serialized
as decimal strings padded to exactly four JSON-null-sentinel slots, riding the
D4 commit stamp — zero extra store operations, debris semantics inherited.
Flat-layout stores are untouched (coverage is a hive-leaf concept).
"""

import importlib.util
import json
import os.path
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from zarr.storage import MemoryStore

from zagg import hive
from zagg.config import default_config, get_data_vars
from zagg.grids import HealpixGrid
from zagg.grids.morton import morton_box, morton_decimal, morton_word

# Order-6 southern shard used across the hive tests (decimal form -5112333).
SHARD = "-5112333"
# Its northern (positive-base) mirror: the ancestor-order arithmetic and the
# subtree prefix guard are string-form-dependent (no leading "-"), so the box
# matrix runs on both hemispheres (PR #208 finding 2).
NORTH = SHARD[1:]


def _words(*decimals):
    return np.asarray([morton_word(d) for d in decimals], dtype=np.uint64)


def _decimals(words):
    return [morton_decimal(w) for w in np.asarray(words, dtype=np.uint64)]


def _brute_force_box(decimals):
    """Spec-literal oracle (issue #200 plan + PR #208 finding 1): the deepest
    common ancestor is the longest common decimal-string prefix (D1: one digit
    per level); the box is its intersecting children, each TIGHTENED to the
    common prefix of the occupancy inside it — where an occupied cell EQUAL to
    the ancestor occupies all of it. Pure string arithmetic, independent of
    mortie's MOC kernels."""
    unique = sorted(set(decimals))
    if len(unique) == 1:
        return unique
    prefix = os.path.commonprefix(unique)
    if prefix in unique:
        return [prefix]
    groups: dict = {}
    for s in unique:
        groups.setdefault(s[: len(prefix) + 1], []).append(s)
    return sorted(os.path.commonprefix(g) for g in groups.values())


def _canonical(words):
    """Canonical compact form for area-equality comparison: two MOCs cover the
    same area iff their compressed forms are identical (complete sibling quads
    and their parent are the same area)."""
    from mortie import compress_moc

    return compress_moc(np.asarray(words, dtype=np.uint64))


def _random_occupancy(rng, shard, max_cells=24, max_depth=4):
    """Random occupied cells at mixed depths within the ``shard`` subtree."""
    n = int(rng.integers(1, max_cells))
    return [
        shard + "".join(rng.choice(list("1234"), size=int(rng.integers(1, max_depth + 1))))
        for _ in range(n)
    ]


# ── the box function ─────────────────────────────────────────────────────────


@pytest.fixture(params=[SHARD, NORTH], ids=["south", "north"])
def shard(request):
    return request.param


class TestMortonBox:
    def test_single_cell_is_the_box(self, shard):
        assert _decimals(morton_box(_words(shard + "1"))) == [shard + "1"]

    def test_duplicates_collapse(self, shard):
        assert _decimals(morton_box(_words(shard + "12", shard + "12"))) == [shard + "12"]

    def test_two_cells_in_different_children(self, shard):
        # DCA is the shard; each intersecting child is tightened to the lone
        # occupied cell inside it (PR #208 finding 1).
        box = morton_box(_words(shard + "12", shard + "43"))
        assert _decimals(box) == [shard + "12", shard + "43"]

    def test_members_tightened_within_children(self, shard):
        # The review counterexample: {S+111, S+112, S+2} must yield
        # [S+11, S+2] — not the looser DCA-child form [S+1, S+2].
        box = morton_box(_words(shard + "111", shard + "112", shard + "2"))
        assert _decimals(box) == [shard + "11", shard + "2"]

    def test_cells_spanning_all_four_children(self, shard):
        box = morton_box(_words(*(shard + d + "1" for d in "1234")))
        assert _decimals(box) == [shard + d + "1" for d in "1234"]

    def test_mixed_depth_ancestor_absorbs_descendants(self, shard):
        # An occupied ancestor covers its whole subtree: {parent, child} is
        # the parent alone. (A naive "children of the DCA holding an occupied
        # cell" would keep only the child and DROP the parent's remaining
        # area — the superset test below is what pins this.)
        box = morton_box(_words(shard + "1", shard + "14"))
        assert _decimals(box) == [shard + "1"]

    def test_mixed_depth_split(self, shard):
        box = morton_box(_words(shard + "11", shard + "422"))
        assert _decimals(box) == [shard + "11", shard + "422"]

    def test_complete_quad_collapses_to_parent(self, shard):
        # Four complete siblings tile their parent exactly; the canonical box
        # is the parent — one member, area-identical to the four children.
        box = morton_box(_words(*(shard + "1" + d for d in "1234")))
        assert _decimals(box) == [shard + "1"]

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            morton_box(np.asarray([], dtype=np.uint64))

    @pytest.mark.parametrize("seed", range(8))
    def test_matches_brute_force_on_random_occupancy(self, shard, seed):
        # Area-equal to the spec-literal tightened oracle, never > 4 members.
        decs = _random_occupancy(np.random.default_rng(seed), shard)
        box = morton_box(_words(*decs))
        assert 1 <= box.size <= hive.COVERAGE_BOX_SLOTS
        np.testing.assert_array_equal(_canonical(box), _canonical(_words(*_brute_force_box(decs))))

    @pytest.mark.parametrize("seed", range(8))
    def test_superset_invariant_randomized(self, shard, seed):
        # Every occupied cell has a box member as ancestor (decimal prefix):
        # false positives cost a wasted read, false negatives are impossible.
        decs = _random_occupancy(np.random.default_rng(seed), shard)
        box = _decimals(morton_box(_words(*decs)))
        for d in decs:
            assert any(d.startswith(b) for b in box), (d, box)
        # And the box never escapes the shard subtree (the shard id is always
        # a valid trivial ancestor).
        assert all(b.startswith(shard) for b in box)


# ── the stamp payload ────────────────────────────────────────────────────────


class TestBuildCoverage:
    def _cov(self, *decimals, cell_order=8):
        occupied = _words(*decimals) if decimals else None
        return hive.build_coverage(morton_word(SHARD), occupied, cell_order)

    def test_exactly_four_slots_nulls_trail(self):
        for occ, members in [
            ((SHARD + "11",), 1),
            ((SHARD + "12", SHARD + "43"), 2),
            ((SHARD + "11", SHARD + "21", SHARD + "31"), 3),
            (tuple(SHARD + d + "1" for d in "1234"), 4),
        ]:
            cov = self._cov(*occ)
            assert len(cov["box"]) == hive.COVERAGE_BOX_SLOTS
            assert all(isinstance(s, str) for s in cov["box"][:members])
            assert cov["box"][members:] == [None] * (hive.COVERAGE_BOX_SLOTS - members)

    def test_payload_fields(self):
        cov = self._cov(SHARD + "12", SHARD + "43", cell_order=8)
        assert cov == {
            "spec": "morton-moc/1",
            "box": [SHARD + "12", SHARD + "43", None, None],
            "cell_order": 8,
            "source": "worker",
        }
        # JSON-safe as-is: the null sentinel is the recorded pad lean.
        assert json.loads(json.dumps(cov)) == cov

    def test_box_members_round_trip_decimal(self):
        cov = self._cov(SHARD + "12", SHARD + "43")
        for label in cov["box"]:
            if label is not None:
                assert morton_decimal(morton_word(label)) == label

    def test_empty_occupancy_falls_back_to_trivial_shard_cover(self):
        # The shard id is always a valid (trivial) 1-member cover.
        assert self._cov()["box"] == [SHARD, None, None, None]

    def test_whole_shard_occupancy_is_the_shard_itself(self):
        cov = self._cov(*(SHARD + d for d in "1234"))
        assert cov["box"] == [SHARD, None, None, None]

    def test_occupancy_outside_shard_subtree_rejected(self):
        with pytest.raises(ValueError, match="subtree"):
            self._cov(SHARD[:-1] + "41")  # sibling shard's cell

    def test_northern_shard_subtree_check(self):
        # The prefix guard has no sign character on positive bases (PR #208
        # finding 2): both the accept and the reject arms run northern.
        cov = hive.build_coverage(morton_word(NORTH), _words(NORTH + "12"), 8)
        assert cov["box"] == [NORTH + "12", None, None, None]
        with pytest.raises(ValueError, match="subtree"):
            hive.build_coverage(morton_word(NORTH), _words(NORTH[:-1] + "41"), 8)


class TestStampCoverage:
    def _stamped_store(self, coverage):
        cfg = default_config("atl06")
        store = MemoryStore()
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        grid.emit_shard_template(store, overwrite=True)
        hive.stamp_commit(store, cells_with_data=2, granule_count=1, coverage=coverage)
        return store

    def test_stamp_carries_coverage_and_accessor_reads_it(self):
        cov = hive.build_coverage(morton_word(SHARD), _words(SHARD + "12", SHARD + "43"), 8)
        store = self._stamped_store(cov)
        stamp = hive.read_commit(store)
        assert stamp["coverage"] == cov
        assert hive.read_coverage(store) == cov
        # No timestamp of its own: the payload reuses the stamp's written_at.
        assert "written_at" not in cov and "generated_at" not in cov
        assert stamp["written_at"]

    def test_stamp_payload_byte_cost(self):
        # The byte-cost pin promised in the plan (issue #200) and PR #208
        # finding 3: the tier-0 payload is fixed-width by construction — keep
        # it (and future envelope creep) bounded on the leaf zarr.json every
        # reader GETs.
        cov = hive.build_coverage(morton_word(SHARD), _words(*(SHARD + d + "1" for d in "1234")), 8)
        assert len(json.dumps(cov)) < 256

    def test_pre_coverage_stamp_reads_none(self):
        # Forward compat: an issue-#199 stamp (no coverage key) keeps reading
        # fine — commit still visible, coverage reads None.
        store = self._stamped_store(None)
        assert hive.read_commit(store)["complete"] is True
        assert hive.read_coverage(store) is None

    def test_unknown_spec_reads_none(self):
        # Strict spec posture (PR #208 finding 4): an unknown/future envelope
        # version reads as absent, never half-parsed. The raw stamp keeps the
        # payload for whoever understands it.
        cov = {"spec": "morton-moc/2", "box": [SHARD, None, None, None], "cell_order": 8}
        store = self._stamped_store(cov)
        assert hive.read_coverage(store) is None
        assert hive.read_commit(store)["coverage"] == cov

    def test_missing_spec_reads_none(self):
        store = self._stamped_store({"box": [SHARD, None, None, None]})
        assert hive.read_coverage(store) is None

    def test_debris_and_absent_leaves_read_none(self):
        assert hive.read_coverage(MemoryStore()) is None  # no leaf at all
        cfg = default_config("atl06")
        store = MemoryStore()
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        grid.emit_shard_template(store, overwrite=True)  # unstamped = debris
        assert hive.read_coverage(store) is None


# ── the worker seam ──────────────────────────────────────────────────────────


class TestOccupiedOutSink:
    """The real ``process_shard`` seam: ``occupied_out`` receives exactly the
    distinct cell words holding observations (the cells ``cells_with_data``
    counts), on the same read stub the write-path tests use."""

    def test_occupied_out_gets_cells_with_data(self, monkeypatch):
        from zagg.index.hierarchical import HierarchicalIndex
        from zagg.processing import process_shard

        cfg = default_config()
        grid = HealpixGrid(6, 8, layout="fullsphere", config=cfg)
        shard = morton_word(SHARD)
        children = grid.children(shard)
        c1, c2 = int(children[0]), int(children[5])
        df = pd.DataFrame(
            {
                "h_li": np.array([3.0, 1.0, 7.0], dtype=np.float32),
                "s_li": np.array([0.1, 0.1, 0.1], dtype=np.float32),
                "leaf_id": np.array([c1, c1, c2], dtype=np.uint64),
            }
        )
        calls = {"n": 0}

        def one_shot(*args, **kwargs):
            calls["n"] += 1
            return df if calls["n"] == 1 else None

        monkeypatch.setattr("zagg.processing._read_group", one_shot)
        monkeypatch.setattr("zagg.processing.h5coro.H5Coro", lambda *a, **k: object())
        monkeypatch.setattr("zagg.processing._make_url_rewriter", lambda driver: lambda u: u)
        monkeypatch.setattr(
            "zagg.processing.worker.index_from_config", lambda cfg: HierarchicalIndex()
        )

        occupied: list = []
        _df, meta = process_shard(
            grid,
            shard,
            ["s3://x"],
            s3_credentials={},
            config=cfg,
            chunk_results=[],
            occupied_out=occupied,
        )
        (words,) = occupied
        assert words.dtype == np.uint64
        assert sorted(int(w) for w in words) == sorted({c1, c2})
        assert meta["cells_with_data"] == 2


# ── the hive write path (both backends) ──────────────────────────────────────


def _rec_meta(shard):
    return {
        "shard_key": int(shard),
        "cells_with_data": 2,
        "total_obs": 7,
        "granule_count": 1,
        "files_processed": 1,
        "duration_s": 0.0,
        "error": None,
    }


def _carrier(grid, shard):
    coords = grid.chunk_coords(shard)
    n = len(coords["cell_ids"])
    df = pd.DataFrame(
        {
            var: np.zeros(n, dtype=np.int32 if var == "count" else np.float32)
            for var in get_data_vars(grid.config)
        }
    )
    for name, vals in coords.items():
        df[name] = vals
    return df


def _occupancy_fake(grid, occupied_words):
    """A process_shard stand-in that streams one real carrier and reports
    ``occupied_words`` through the occupied_out sink, as the real worker does."""

    def fake(g, shard_key, urls, **kwargs):
        kwargs["write_chunk"](grid.block_index(int(shard_key)), _carrier(grid, shard_key), {})
        if kwargs.get("occupied_out") is not None and occupied_words is not None:
            kwargs["occupied_out"].append(np.asarray(occupied_words, dtype=np.uint64))
        return pd.DataFrame(), _rec_meta(shard_key)

    return fake


class TestProcessAndWriteHiveCoverage:
    def _run(self, monkeypatch, tmp_path, occupied_words):
        import zagg.processing as processing
        from zagg.store import open_store

        cfg = default_config("atl06")
        grid = HealpixGrid(parent_order=6, child_order=8, layout="fullsphere", config=cfg)
        shard = morton_word(SHARD)
        monkeypatch.setattr(processing, "process_shard", _occupancy_fake(grid, occupied_words))
        root = str(tmp_path / "store")
        hive.process_and_write_hive(shard, ["s3://b/g1.h5"], grid, {}, root, cfg, store_kwargs={})
        return grid, open_store(hive.shard_leaf_path(root, shard))

    def test_stamp_gains_coverage_with_zero_extra_objects(self, monkeypatch, tmp_path):
        grid, leaf_store = self._run(monkeypatch, tmp_path, _words(SHARD + "12", SHARD + "43"))
        cov = hive.read_coverage(leaf_store)
        assert cov == {
            "spec": hive.COVERAGE_SPEC,
            "box": [SHARD + "12", SHARD + "43", None, None],
            "cell_order": int(grid.child_order),
            "source": "worker",
        }
        # Same stamp object, no companion coverage object anywhere in the leaf
        # (the tier-0 payload rides the existing final attrs PUT).
        stamp = hive.read_commit(leaf_store)
        assert stamp["coverage"] == cov

    def test_worker_without_occupancy_stamps_trivial_cover(self, monkeypatch, tmp_path):
        # No occupied_out delivery (e.g. a legacy caller): the shard id is the
        # trivial 1-member cover — conservative, never a false negative.
        _grid, leaf_store = self._run(monkeypatch, tmp_path, None)
        assert hive.read_coverage(leaf_store)["box"] == [SHARD, None, None, None]


# ── backend identity via the Lambda handler path ─────────────────────────────

HANDLER_PATH = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"


@pytest.fixture(scope="module")
def handler_mod():
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_coverage", HANDLER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestBothBackendsIdenticalPayload:
    """The shared-code-path pin: the local dispatcher and the Lambda handler
    both run ``hive.process_and_write_hive``, so identical input produces the
    byte-identical coverage payload — one test through the REAL handler path,
    like the existing stamp tests."""

    # Order-6 southern shard the handler tests use (decimal -4211322).
    _WORD = 11827859996358475782

    def test_local_and_lambda_coverage_payloads_identical(self, handler_mod, monkeypatch, tmp_path):
        import zagg.processing as processing
        from zagg.config import load_config_from_dict
        from zagg.grids import from_config
        from zagg.store import open_store

        # Self-recycle hygiene (issue #171): never let this module-scoped
        # handler instance reach a real os._exit under dev-shell env knobs.
        monkeypatch.delenv("ZAGG_RECYCLE_RSS_MB", raising=False)
        monkeypatch.delenv("ZAGG_RECYCLE_MAX_INVOCATIONS", raising=False)
        monkeypatch.setattr(
            handler_mod, "_exit", lambda code: (_ for _ in ()).throw(AssertionError(code))
        )

        cfg = default_config("atl06")
        cfg.output["store_layout"] = "hive"
        config_dict = asdict(cfg)
        grid = from_config(load_config_from_dict(config_dict))
        shard_dec = morton_decimal(self._WORD)
        occupied = _words(shard_dec + "12", shard_dec + "43")
        monkeypatch.setattr(processing, "process_shard", _occupancy_fake(grid, occupied))

        # Local backend leg.
        local_root = str(tmp_path / "local")
        hive.process_and_write_hive(
            self._WORD, ["s3://b/g.h5"], grid, {}, local_root, cfg, store_kwargs={}
        )

        # Lambda backend leg: the real process-mode handler.
        lambda_root = str(tmp_path / "lambda")
        ctx = MagicMock()
        ctx.aws_request_id = "req-1"
        ctx.function_name = "process-shard"
        ctx.memory_limit_in_mb = 2048
        ctx.get_remaining_time_in_millis.return_value = 900_000
        event = {
            "shard_key": self._WORD,
            "parent_order": 6,
            "child_order": int(grid.child_order),
            "granule_urls": ["s3://b/g.h5"],
            "store_path": lambda_root,
            "s3_credentials": {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
            "config": config_dict,
        }
        resp = handler_mod._handle_process(event, ctx)
        assert resp["statusCode"] == 200, resp["body"]

        local_cov = hive.read_coverage(open_store(hive.shard_leaf_path(local_root, self._WORD)))
        lambda_cov = hive.read_coverage(open_store(hive.shard_leaf_path(lambda_root, self._WORD)))
        assert local_cov is not None
        assert local_cov == lambda_cov
        assert local_cov["box"] == [shard_dec + "12", shard_dec + "43", None, None]
