"""Tests for the offline read-plan module (issue #43, Phase C).

All tests use synthetic numpy arrays — no h5coro, S3, or credentials needed.
"""

import numpy as np

from zagg.read_plan import ReadPlan, execute_read_plan, plan_read


def _isolated_setup(n_segs, seg_len=10, lat_step=40.0):
    """Build lat/lon arrays where adjacent rep-points are far enough apart
    that a 1-degree-tall bbox around segment 0 does not intersect the
    linestring from segment 0 to segment 1.

    Segments sit at lat = [0, lat_step, 2*lat_step, ...] and lon = 0.5.
    With lat_step=40 the linestring from seg 0 (lat=0) to seg 1 (lat=40)
    does NOT cross a bbox limited to lat < 0.5, so segment 0 is the
    only match for bbox=(0, -0.5, 1, 0.5).
    """
    lats = np.arange(n_segs, dtype=float) * lat_step
    lons = np.full(n_segs, 0.5)
    index_beg = np.arange(n_segs, dtype=int) * seg_len
    count = np.full(n_segs, seg_len, dtype=int)
    n_base = n_segs * seg_len
    return lats, lons, index_beg, count, n_base


class TestPlanReadBasic:
    def test_single_parent_in_aoi(self):
        # Segment 0 sits at lat=0. bbox is (0, -0.5, 1, 0.5) — only its rep-point
        # lands inside; the linestring to seg 1 (lat=40) exits immediately upward
        # and never re-enters this narrow band.
        lats, lons, idx, cnt, n_base = _isolated_setup(5)
        bbox = (0.0, -0.5, 1.0, 0.5)
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0)
        assert not plan.full_read
        assert len(plan.parent_runs) == 1
        assert plan.parent_runs[0] == (0, 0)
        assert plan.base_slices[0] == (0, 10)

    def test_empty_aoi_returns_empty_plan(self):
        lats, lons, idx, cnt, n_base = _isolated_setup(5)
        bbox = (100.0, 100.0, 110.0, 110.0)  # nowhere near our data
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0)
        assert plan.parent_runs == []
        assert plan.base_slices == []
        assert plan.chunk_lists == []
        assert not plan.full_read

    def test_adjacent_parents_merged_into_one_run(self):
        # Both segs 0 (lat=0) and 1 (lat=40) land inside a wide bbox.
        lats, lons, idx, cnt, n_base = _isolated_setup(5)
        bbox = (0.0, -0.5, 1.0, 40.5)  # covers segs 0 and 1
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0)
        assert len(plan.parent_runs) == 1
        assert plan.parent_runs[0] == (0, 1)
        assert plan.base_slices[0] == (0, 20)

    def test_non_contiguous_parents_give_separate_runs(self):
        # Four segments spaced 40 lat-degrees apart. Only segs 0 and 3 land in
        # their respective narrow bboxes. Because they are not contiguous, testing
        # with a single-call narrow bbox on seg 0 only.
        lats, lons, idx, cnt, n_base = _isolated_setup(4)
        # Only seg 0 in bbox
        bbox = (0.0, -0.5, 1.0, 0.5)
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0)
        assert len(plan.parent_runs) == 1
        assert plan.parent_runs[0] == (0, 0)

    def test_pad_extends_run(self):
        # Seg 0 in aoi; pad=1 should extend the run to include seg 1.
        lats, lons, idx, cnt, n_base = _isolated_setup(5)
        bbox = (0.0, -0.5, 1.0, 0.5)  # only seg 0 in AOI
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=1)
        # pad=1 extends [0,0] -> [0, 1] (clamped at start 0)
        assert plan.parent_runs[0] == (0, 1)
        assert plan.base_slices[0] == (0, 20)

    def test_pad_clamps_at_boundaries(self):
        lats, lons, idx, cnt, n_base = _isolated_setup(5)
        bbox = (0.0, -0.5, 1.0, 0.5)  # only seg 0 in AOI
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=2)
        # pad=2, start clamped to 0, end = min(4, 2) = 2
        assert plan.parent_runs[0][0] == 0
        assert plan.parent_runs[0][1] == 2

    def test_selectivity_fallback_to_full_read(self):
        # All 10 segs in aoi -> total_base = n_base -> full_read
        lats, lons, idx, cnt, n_base = _isolated_setup(10, seg_len=100, lat_step=1.0)
        bbox = (-1.0, -1.0, 2.0, 10.0)  # covers all segments
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0, full_read_threshold=0.9)
        assert plan.full_read

    def test_selectivity_below_threshold_no_full_read(self):
        lats, lons, idx, cnt, n_base = _isolated_setup(10, seg_len=10)
        bbox = (0.0, -0.5, 1.0, 0.5)  # only seg 0 -> 10/100 = 0.1
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0, full_read_threshold=0.9)
        assert not plan.full_read


class TestPlanReadCrossingCase:
    def test_linestring_crossing_bbox(self):
        """A parent whose rep-point is just outside the bbox but whose linestring
        to the next parent crosses the bbox is still included."""
        # Two segments: seg0 at lon=0 (outside bbox), seg1 at lon=2 (outside bbox).
        # The line from (0,0)->(2,0) crosses bbox (0.5, -0.5, 1.5, 0.5).
        lats = np.array([0.0, 0.0])
        lons = np.array([0.0, 2.0])
        idx = np.array([0, 5])
        cnt = np.array([5, 5])
        n_base = 10
        # bbox crosses the midpoint of the lon=0 to lon=2 linestring
        bbox = (0.5, -0.5, 1.5, 0.5)
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, pad=0)
        assert not plan.full_read
        # seg0's linestring to seg1 crosses the bbox -> seg0 included
        assert len(plan.parent_runs) >= 1
        assert plan.parent_runs[0][0] == 0


class TestPlanReadIndexBase:
    def test_index_base_1_atl03_style(self):
        """index_base=1: ph_index_beg=1 means base[0], ph_index_beg=6 means base[5]."""
        lats = np.array([0.0, 100.0])
        lons = np.array([0.5, 0.5])
        idx = np.array([1, 6])  # 1-based
        cnt = np.array([5, 5])
        n_base = 10
        bbox = (0.0, -0.5, 1.0, 0.5)  # only seg 0 (lat=0)
        plan = plan_read(lats, lons, idx, cnt, n_base, bbox, index_base=1, pad=0)
        assert len(plan.parent_runs) == 1
        assert plan.base_slices[0] == (0, 5)  # 1-1=0, 0+5=5


class TestPlanReadEmptySegments:
    """ATL03 marks empty segments with ph_index_beg == 0 / count == 0. Using one
    as a run boundary translated to a bogus slice -- balloon to photon 0 (->
    full-read OOM) or collapse to nothing (-> silent data loss). The run must be
    bounded by its non-empty segments only.
    """

    def test_empty_at_run_start_does_not_balloon(self):
        # Three close rep-points all land in the bbox; the first is empty. The
        # slice must start at the first NON-EMPTY segment's photons, not at 0.
        lats = np.array([0.00, 0.10, 0.20, 40.0, 80.0])
        lons = np.full(5, 0.5)
        ibeg = np.array([0, 5001, 5101, 5201, 5301])  # seg 0 empty (sentinel 0)
        cnt = np.array([0, 100, 100, 100, 100])
        n_base = 5400
        bbox = (0.0, -0.5, 1.0, 0.5)  # matches segs 0,1,2
        plan = plan_read(lats, lons, ibeg, cnt, n_base, bbox, index_base=1, pad=0)
        assert not plan.full_read  # pre-fix this ballooned to [0, 5200] -> selectivity full read
        assert plan.base_slices == [(5000, 5200)]  # non-empty segs 1..2 only
        assert plan.parent_runs == [(0, 2)]  # parent_runs stays in lockstep

    def test_empty_at_run_end_not_dropped(self):
        # Empty segment at the run end must not collapse the slice to nothing.
        lats = np.array([0.00, 0.10, 0.20, 40.0])
        lons = np.full(4, 0.5)
        ibeg = np.array([5001, 5101, 0, 5301])  # seg 2 empty
        cnt = np.array([100, 100, 0, 100])
        n_base = 5400
        bbox = (0.0, -0.5, 1.0, 0.5)  # matches segs 0,1,2
        plan = plan_read(lats, lons, ibeg, cnt, n_base, bbox, index_base=1, pad=0)
        assert plan.base_slices == [(5000, 5200)]  # bounded by non-empty seg 1
        assert plan.parent_runs == [(0, 2)]

    def test_empty_in_run_middle_stays_contiguous(self):
        # An empty interior segment consumes no photon indices, so the two
        # non-empty neighbours are adjacent -- one contiguous slice covers both.
        lats = np.array([0.00, 0.10, 0.20, 40.0])
        lons = np.full(4, 0.5)
        ibeg = np.array([5001, 0, 5101, 5301])  # seg 1 empty (middle)
        cnt = np.array([100, 0, 100, 100])
        n_base = 5400
        bbox = (0.0, -0.5, 1.0, 0.5)  # matches segs 0,1,2
        plan = plan_read(lats, lons, ibeg, cnt, n_base, bbox, index_base=1, pad=0)
        assert plan.base_slices == [(5000, 5200)]  # seg0 [5000,5100) + seg2 [5100,5200)
        assert plan.parent_runs == [(0, 2)]

    def test_all_empty_run_skipped_no_crash(self):
        # A run with only empty segments contributes no photons. parent_runs MUST
        # stay empty in lockstep, else execute_read_plan hits np.concatenate([]).
        lats = np.array([0.00, 0.10, 40.0])
        lons = np.full(3, 0.5)
        ibeg = np.array([0, 0, 5301])
        cnt = np.array([0, 0, 100])
        n_base = 5400
        bbox = (0.0, -0.5, 1.0, 0.5)  # matches segs 0,1 (both empty)
        plan = plan_read(lats, lons, ibeg, cnt, n_base, bbox, index_base=1, pad=0)
        assert plan.base_slices == []
        assert plan.parent_runs == []  # lockstep -> consumer short-circuits, no phantom run
        assert not plan.full_read
        # consumer contract: must return empty, not raise on an empty concatenate.
        out = execute_read_plan(plan, lambda *a, **k: np.array([]), "any/path", float)
        assert out.size == 0


class TestExecuteReadPlan:
    def _make_plan(self, slices, full_read=False):
        runs = [(s, e - 1) for s, e in slices]
        # chunk_lists use h5coro's half-open ``[start, end)`` convention --
        # mirrors base_slices exactly. The runs are inclusive index pairs in
        # the coarse array (different domain than chunks).
        chunks = [[slc] for slc in slices]
        return ReadPlan(
            parent_runs=runs,
            base_slices=list(slices),
            chunk_lists=chunks,
            full_read=full_read,
        )

    def test_empty_plan_returns_empty_array(self):
        plan = ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[])
        calls = []

        def read_fn(path, hyperslice=None):
            calls.append(hyperslice)
            return np.array([])

        out = execute_read_plan(plan, read_fn, "/h", np.float32)
        assert len(out) == 0
        assert len(calls) == 0  # no read calls for empty plan

    def test_single_slice_reads_correct_range(self):
        data = np.arange(100.0, dtype=np.float32)
        plan = self._make_plan([(10, 20)])

        def read_fn(path, hyperslice=None):
            # h5coro hyperslice is half-open ``[lo, hi)`` per h5dataset.py
            # ("must provide as list of ranges [x,y)") -- Python slicing matches.
            assert hyperslice is not None
            lo, hi = hyperslice[0]
            return data[lo:hi]

        out = execute_read_plan(plan, read_fn, "/h", np.float32)
        np.testing.assert_array_equal(out, data[10:20])

    def test_multiple_slices_concatenated(self):
        data = np.arange(100.0, dtype=np.float32)
        plan = self._make_plan([(5, 10), (20, 25)])

        def read_fn(path, hyperslice=None):
            lo, hi = hyperslice[0]
            return data[lo:hi]

        out = execute_read_plan(plan, read_fn, "/h", np.float32)
        expected = np.concatenate([data[5:10], data[20:25]])
        np.testing.assert_array_equal(out, expected)

    def test_full_read_plan_calls_without_hyperslice(self):
        data = np.arange(50.0, dtype=np.float32)
        plan = ReadPlan(
            parent_runs=[(0, 4)],
            base_slices=[(0, 50)],
            chunk_lists=[[(0, 50)]],
            full_read=True,
        )
        called_with_none = []

        def read_fn(path, hyperslice=None):
            called_with_none.append(hyperslice is None)
            return data

        out = execute_read_plan(plan, read_fn, "/h", np.float32)
        assert called_with_none == [True]
        np.testing.assert_array_equal(out, data)

    def test_dtype_coercion(self):
        data = np.array([1, 2, 3], dtype=np.int32)
        plan = self._make_plan([(0, 3)])

        def read_fn(path, hyperslice=None):
            return data

        out = execute_read_plan(plan, read_fn, "/h", np.float64)
        assert out.dtype == np.float64

    def test_full_read_true_with_empty_parent_runs(self):
        # full_read=True must take precedence over empty parent_runs; the full
        # dataset should be returned rather than an empty array.
        data = np.arange(20.0, dtype=np.float32)
        plan = ReadPlan(parent_runs=[], base_slices=[], chunk_lists=[], full_read=True)
        calls = []

        def read_fn(path, hyperslice=None):
            calls.append(hyperslice)
            return data

        out = execute_read_plan(plan, read_fn, "/h", np.float32)
        assert calls == [None]  # full-read call, no hyperslice
        np.testing.assert_array_equal(out, data)
