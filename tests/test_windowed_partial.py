"""A bulk invoke with one failed window (issue #586 review finding (8), option (1)).

The shard is a failed cell on every backend — its 500, its ``failed`` status
object, an error naming the window — but the leaves that landed are in the
store, so the post-run tail reads LEAVES, not invokes:

- the sweep work set (the families sweep, the staged sweep's ``(node,
  window)`` units and the node close) is every result's stats records, each
  on its own ``success``;
- a shard enters the root ``coverage.moc`` when at least one of its leaves
  stands, and the time-range union takes those leaves' ranges alone;
- the failed window is in none of them, and a re-run redoes it while the gate
  skips the ones that landed.

One rule on the local backend, on ``agg``'s Lambda path over each of its
three transports, and on the ``zagg.client`` tail — pinned here by running
the same failure through the real pipeline on both backends (the fleet's
invokes executed in-process by the real handler) and comparing what each
swept and covered.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import test_windowed_emit as emit
from obstore.store import MemoryStore
from test_client_transport import (
    _STORE,
    EventStubLambdaClient,
    _put_manifest,
    _put_status,
    _run,
)
from test_sweep_stage import _artifact
from test_sweep_stage_fleet import _handler_module

import zagg.client_transport as ct
from zagg import hive, runner
from zagg.client import Run
from zagg.config import validate_config
from zagg.grids.morton import morton_decimal
from zagg.runner import _landed_coverage, _reported_units
from zagg.sweep import discover_leaves

#: The failed window is the LAST one, so the landed leaves' time-range union
#: stops short of it and a coverage that counted it would show.
FAILED = "2020"
LANDED = ("2018", "2019")
#: Granule A's one 2018 observation .. granule C's last 2019 one.
LANDED_RANGE = ["2018-10-28T00:00:00+00:00", "2019-12-31T18:00:00+00:00"]
FULL_RANGE = ["2018-10-28T00:00:00+00:00", "2020-03-13T00:00:00+00:00"]
ERROR = f"window {FAILED}: RuntimeError: PUT failed"
_CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}


def _cfg():
    cfg = emit._digest_cfg()
    cfg.output["sweep"] = "stages"
    cfg.output["pyramid"] = {"overviews": 7, "all_time": True}
    validate_config(cfg)
    return cfg


def _fail_one_window(monkeypatch, label=FAILED):
    """The ``label`` window's leaf write raises; every other window lands."""
    finish = hive._LeafUnit.finish

    def _flaky(self, metadata):
        if self.label == label:
            raise RuntimeError("PUT failed")
        return finish(self, metadata)

    monkeypatch.setattr(hive._LeafUnit, "finish", _flaky)
    return lambda: monkeypatch.setattr(hive._LeafUnit, "finish", finish)


class _Payload:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class _HandlerLambda:
    """A Lambda client whose every invoke runs the REAL handler in-process.

    Cell, setup, coverage, stats, sweep and stage events all execute against
    the local store root, so what the dispatcher's tail asks for is what
    lands — the fleet path with nothing but the network removed.
    """

    def __init__(self, mod):
        self.mod = mod
        self.events: list = []

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803 (boto3 spelling)
        event = json.loads(Payload)
        self.events.append((InvocationType, event))
        ctx = MagicMock()
        ctx.aws_request_id, ctx.function_name, ctx.memory_limit_in_mb = "req", FunctionName, 2048
        ctx.get_remaining_time_in_millis.return_value = 900_000
        response = self.mod.lambda_handler(event, ctx)
        if InvocationType == "Event":
            return {"StatusCode": 202}
        return {"Payload": _Payload(json.dumps(response).encode()), "FunctionError": None}

    def modes(self, mode):
        return [e for _t, e in self.events if e.get("mode") == mode]


def _run_local(monkeypatch, tmp_path, cfg, root=None):
    """``agg`` on the local backend; the tail's sweep inputs, as it passed them."""
    import zagg.sweep as sweep_mod
    import zagg.sweep_stages as stages_mod

    catalog_path, shard = emit._catalog(tmp_path)
    root = root or tmp_path / "local"
    seen: dict = {}
    monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: dict(_CREDS))
    real_sweep, real_stage = sweep_mod.sweep_after_run, stages_mod.stage_sweep_after_run

    def sweep(store, leaves, **kw):
        seen["sweep"] = list(leaves)
        return real_sweep(store, leaves, **kw)

    def stage(store, leaves, **kw):
        seen["stage"] = list(leaves)
        return real_stage(store, leaves, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(sweep_mod, "sweep_after_run", sweep)
        patch.setattr(stages_mod, "stage_sweep_after_run", stage)
        summary = runner.agg(cfg, catalog=catalog_path, store=str(root), backend="local")
    return root, shard, summary, seen


def _run_fleet(monkeypatch, tmp_path, cfg, invocation, root=None):
    """``runner._run_lambda`` with every invoke executed by the real handler."""
    import boto3

    import zagg.sweep_fleet as fleet_mod
    from zagg.concurrency import ConcurrencyReport

    catalog_path, shard = emit._catalog(tmp_path)
    root = root or tmp_path / f"fleet-{invocation}"
    stub = _HandlerLambda(_handler_module())
    session = MagicMock()
    session.client.side_effect = lambda service, **k: stub if service == "lambda" else MagicMock()
    report = ConcurrencyReport(
        account_limit=1000, current_concurrent=0, padding=100, available=900, function_reserved=None
    )
    seen: dict = {}
    real_stage, real_fleet = runner._invoke_lambda_stage_sweep, fleet_mod.run_stage_sweep_fleet

    def stage(client, fn, store, leaves, **kw):
        seen["stage"] = list(leaves)
        return real_stage(client, fn, store, leaves, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(boto3, "Session", lambda *a, **k: session)
        patch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 900)
        patch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", 0)
        patch.setattr(runner, "get_nsidc_s3_credentials", lambda: dict(_CREDS))
        patch.setattr(runner, "compute_available_workers", lambda n, *a, **k: (1, report))
        patch.setattr(runner, "_invoke_lambda_stage_sweep", stage)
        patch.setattr(
            fleet_mod,
            "run_stage_sweep_fleet",
            lambda *a, **k: real_fleet(*a, **{**k, "poll_interval_s": 0.01}),
        )
        patch.setattr(ct, "_POLL_INITIAL_INTERVAL_S", 0.01)
        patch.setattr(ct, "_POLL_MAX_INTERVAL_S", 0.02)
        summary = runner._run_lambda(
            cfg,
            json.loads(Path(catalog_path).read_text()),
            str(root),
            8,
            max_cells=None,
            morton_cell=None,
            max_workers=1,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
            function_name="process-shard",
            invocation=invocation,
        )
    families = [e for e in stub.modes("sweep") if "stage" not in e]
    seen["sweep"] = [tuple(leaf) for e in families for leaf in e["leaves"]]
    return root, shard, summary, seen, stub


def _artifacts(root):
    """Every zarr artifact under the store: leaves, columns, overviews."""
    out = []
    for p in Path(root).rglob("*.zarr"):
        rel = str(p.relative_to(root))
        if p.is_dir() and ".zarr/" not in rel:
            out.append(rel)
    return sorted(out)


def _coverage(root):
    env = hive.read_root_coverage(str(root))
    return None if env is None else {k: env.get(k) for k in ("order", "ranges", "time_range")}


def _rows(summary):
    rows = pd.read_parquet(summary["run_stats_path"], engine="fastparquet")
    return sorted(zip(rows["window"], rows["success"].astype(bool), strict=True))


def _overviews(root, name):
    return [a for a in _artifacts(root) if a.endswith(f"/{name}.zarr") and "_" not in a]


# ---------------------------------------------------------------------------
# The two backends, the same failure, the real pipeline on each.
# ---------------------------------------------------------------------------


class TestBackendsAgree:
    @pytest.mark.parametrize("invocation", ["sync", "async", "event"])
    def test_the_fleet_sweeps_and_covers_what_the_local_backend_does(
        self, monkeypatch, tmp_path, invocation
    ):
        cfg = _cfg()
        emit._patch(monkeypatch)
        _fail_one_window(monkeypatch)
        local_root, shard, local, local_seen = _run_local(monkeypatch, tmp_path, cfg)
        fleet_root, _shard, fleet, fleet_seen, stub = _run_fleet(
            monkeypatch, tmp_path, cfg, invocation
        )
        landed = [(shard, w) for w in LANDED]

        # The shard is a failed cell on the fleet: its 500, its error naming
        # the window, its records — the failed window's unsuccessful.
        (result,) = fleet["results"]
        assert fleet["cells_error"] == 1 and fleet["cells_with_data"] == 0
        assert result["status_code"] == 500 and result["error"] == ERROR
        assert [(r["window"], r["success"]) for r in result["body"]["stats"]] == [
            ("2018", True),
            ("2019", True),
            ("2020", False),
        ]
        if invocation == "event":
            # ... and on the v2 transport its ``failed`` status object.
            key = ct.shard_status_key(shard)
            (path,) = Path(f"{fleet_root}.status").glob(f"run-*/{key}")
            status = json.loads(path.read_text())
            assert (status["status"], status["error"], status["status_code"]) == (
                "failed",
                ERROR,
                500,
            )

        # The sweep work set: the landed windows, on both backends, in the
        # families sweep and the staged one alike; the failed window in none.
        assert local_seen == fleet_seen == {"sweep": landed, "stage": landed}

        # Root coverage: the shard is covered through its landed windows, and
        # the union stops where they do.
        expected = {
            "order": 6,
            "ranges": [[morton_decimal(shard)] * 2],
            "time_range": LANDED_RANGE,
        }
        assert _coverage(local_root) == _coverage(fleet_root) == expected

        # The run parquet: one row per leaf on both, the failed one unsuccessful.
        assert _rows(local) == _rows(fleet) == [("2018", True), ("2019", True), ("2020", False)]
        # ... so the CLI backstop's run-record discovery is the same set too.
        assert discover_leaves(str(local_root)) == discover_leaves(str(fleet_root)) == landed

        # What the sweeps built: the same artifacts on both backends — every
        # landed window's overview and the all-time fold at each of the six
        # ladder nodes, and nothing for the failed window.
        assert _artifacts(local_root) == _artifacts(fleet_root)
        for root in (local_root, fleet_root):
            assert [len(_overviews(root, w)) for w in ("2018", "2019", "2020", "all")] == [
                6,
                6,
                0,
                6,
            ]
            block = dict(_artifact(root, "-5/all.zarr").attrs)["zagg_overview"]
            assert block["source_windows"] == {"folded": 2, "missing": 0, "unreadable": 0}
        # The staged sweep ran its (node, window) units for the landed windows.
        windows = {
            e["stage"]["window"]
            for e in stub.modes("sweep")
            if e.get("stage", {}).get("unit") == "window"
        }
        assert windows == set(LANDED)

    @pytest.mark.parametrize("backend", ["local", "sync"])
    def test_a_rerun_redoes_the_failed_window_and_skips_the_landed_ones(
        self, monkeypatch, tmp_path, backend
    ):
        cfg = _cfg()
        emit._patch(monkeypatch)
        heal = _fail_one_window(monkeypatch)
        root = tmp_path / "store"

        def go():
            if backend == "local":
                _root, shard, summary, seen = _run_local(monkeypatch, tmp_path, cfg, root)
                return shard, summary, seen
            _root, shard, summary, seen, _stub = _run_fleet(
                monkeypatch, tmp_path, cfg, backend, root
            )
            return shard, summary, seen

        shard, _first, seen = go()
        assert seen["stage"] == [(shard, w) for w in LANDED]
        heal()
        _shard, second, seen = go()
        # The gate skipped the landed windows; only the failed one is redone,
        # swept, and unioned into the root summary.
        assert (second["cells_current"], second["cells_error"]) == (2, 0)
        assert seen == {"sweep": [(shard, FAILED)], "stage": [(shard, FAILED)]}
        assert _coverage(root)["time_range"] == FULL_RANGE
        assert [len(_overviews(root, w)) for w in ("2018", "2019", "2020", "all")] == [6, 6, 6, 6]
        block = dict(_artifact(root, "-5/all.zarr").attrs)["zagg_overview"]
        assert block["source_windows"] == {"folded": 3, "missing": 0, "unreadable": 0}


# ---------------------------------------------------------------------------
# The tail's two readers.
# ---------------------------------------------------------------------------


def _bulk(key, *windows, error=None):
    return {"shard_key": key, "error": error, "windows": [dict(w, shard_key=key) for w in windows]}


class TestTailReaders:
    def test_reported_units_keep_a_bulk_body_whatever_its_status(self):
        failed_bulk = _bulk(2, {"window": "2019", "error": None}, error="window 2020: x")
        results = [
            {"shard_key": 1, "status_code": 200, "body": {"total_obs": 1}, "error": None},
            {"shard_key": 2, "status_code": 500, "body": failed_bulk, "error": "window 2020: x"},
            # an unwindowed (or per-window) unit that failed reports no leaf
            {"shard_key": 3, "status_code": 500, "body": {"error": "boom"}, "error": "boom"},
            # a killed invoke has no body at all
            {"shard_key": 4, "status_code": None, "body": {}, "error": "Lambda timeout: x"},
            {"shard_key": 5, "status_code": 200, "body": {}, "error": "No granules found"},
        ]
        assert _reported_units(results) == [(1, {"total_obs": 1}), (2, failed_bulk)]

    def test_coverage_is_the_leaves_that_stand(self):
        r19 = ["2019-02-01T00:00:00+00:00", "2019-03-01T00:00:00+00:00"]
        r20 = ["2020-02-01T00:00:00+00:00", "2020-03-01T00:00:00+00:00"]
        r21 = ["2021-02-01T00:00:00+00:00", "2021-03-01T00:00:00+00:00"]
        units = [
            # one landed, one failed (its range, were it to carry one, is not unioned)
            (
                1,
                _bulk(
                    1,
                    {"window": "2019", "error": None, "time_range": r19},
                    {"window": "2021", "error": "RuntimeError: x", "time_range": r21},
                    error="window 2021: RuntimeError: x",
                ),
            ),
            # every window failed: not covered
            (2, _bulk(2, {"window": "2019", "error": "RuntimeError: x"}, error="window 2019: x")),
            # a failed window beside one the gate found current: the leaf stands
            (
                3,
                _bulk(
                    3,
                    {"window": "2019", "error": None, "current": True},
                    {"window": "2020", "error": "RuntimeError: x"},
                    error="window 2020: x",
                ),
            ),
            # a benign no-data window beside a landed one (a 200 shard)
            (
                4,
                _bulk(
                    4,
                    {"window": "2019", "error": "No data after filtering"},
                    {"window": "2020", "error": None, "time_range": r20},
                ),
            ),
            # per-window and unwindowed units: covered iff the unit has no error
            (5, {"shard_key": 5, "error": None}),
            (6, {"shard_key": 6, "error": "boom", "time_range": r21}),
        ]
        done, time_range = _landed_coverage(units)
        assert done == [1, 3, 4, 5]
        assert time_range == [r19[0], r20[1]]
        assert _landed_coverage([]) == ([], None)

    def test_the_shard_metadata_unions_the_landed_windows(self):
        r19 = ["2019-02-01T00:00:00+00:00", "2019-03-01T00:00:00+00:00"]
        r21 = ["2021-02-01T00:00:00+00:00", "2021-03-01T00:00:00+00:00"]
        meta = hive._shard_meta(
            {"shard_key": 1, "duration_s": 1.0, "error": None},
            [
                {"window": "2019", "error": None, "time_range": r19, "total_obs": 2},
                {"window": "2021", "error": "RuntimeError: x", "time_range": r21, "total_obs": 9},
            ],
        )
        assert meta["time_range"] == r19 and meta["total_obs"] == 2
        assert meta["error"] == "window 2021: RuntimeError: x"


# ---------------------------------------------------------------------------
# The zagg.client tail (the facade's finisher, and Run.attach's).
# ---------------------------------------------------------------------------


@pytest.fixture
def failed_body(monkeypatch, tmp_path):
    """The REAL handler's response body for the failed bulk invoke, and its shard."""
    emit._patch(monkeypatch)
    _fail_one_window(monkeypatch)
    event = emit._handler_event(emit._cfg(), tmp_path)
    response = emit._handle(_handler_module(), event)
    assert response["statusCode"] == 500
    return event["shard_key"], json.loads(response["body"])


@pytest.fixture
def status_store(monkeypatch):
    store = MemoryStore()
    monkeypatch.setattr(ct, "open_status_store", lambda prefix, kwargs: store)
    monkeypatch.setattr(ct, "_POLL_INITIAL_INTERVAL_S", 0.01)
    monkeypatch.setattr(ct, "_POLL_MAX_INTERVAL_S", 0.02)
    monkeypatch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", 0)
    return store


class _PartialStub(EventStubLambdaClient):
    """The v2 stub, with one shard's worker answering a canned failed bulk body."""

    def __init__(self, status_store, shard, body):
        super().__init__(status_store)
        self._partial = (shard, body)

    def invoke(self, **kwargs):
        event = json.loads(kwargs["Payload"])
        shard, body = self._partial
        if event.get("mode") is None and event["shard_key"] == shard:
            self.events.append((kwargs["FunctionName"], kwargs["InvocationType"], event))
            self._write_status(event, {"statusCode": 500, "body": json.dumps(body)})
            return {"StatusCode": 202}
        return super().invoke(**kwargs)


def _tail_events(stub):
    by_mode = {e["mode"]: e for _n, _t, e in stub.events if e.get("mode")}
    return by_mode.get("coverage"), by_mode.get("sweep")


class TestClientTail:
    """The facade refuses a windowed config at construction and at attach, so
    no dispatch of its own produces this body today; its tail reads through
    the same two readers regardless, and the body is planted on the channel
    the tail reads it from (the status object)."""

    def _assert_tail(self, stub, shard, others=()):
        coverage, sweep = _tail_events(stub)
        assert coverage["coverage"]["time_range"] == LANDED_RANGE
        covered = {d for lo, hi in coverage["coverage"]["ranges"] for d in (lo, hi)}
        assert morton_decimal(shard) in covered
        landed = [[shard, w] for w in LANDED]
        assert [leaf for leaf in sweep["leaves"] if leaf[0] == shard] == landed
        assert all(leaf[1] != FAILED for leaf in sweep["leaves"])
        assert {leaf[0] for leaf in sweep["leaves"]} == {shard, *others}

    def test_the_event_tail_sweeps_and_covers_the_landed_windows(
        self, failed_body, status_store, monkeypatch
    ):
        shard, body = failed_body
        catalog = {
            "metadata": {"short_name": "ATL06", "version": "006"},
            "grid_signature": {
                "type": "healpix",
                "indexing_scheme": "nested",
                "parent_order": 6,
                "child_order": 12,
                "layout": "fullsphere",
            },
            "shard_keys": [shard],
            "granules": [emit._records()],
        }
        stub = _PartialStub(status_store, shard, body)
        handle = _run(catalog, client=stub).dispatch(transport="event")
        with pytest.raises(sys.modules["zagg.client"].ShardError, match=f"window {FAILED}"):
            handle.futures[shard].result(timeout=10)
        handle.wait(timeout=10)
        self._assert_tail(stub, shard)

    def test_attach_reads_the_same_set_off_the_status_object(self, failed_body, status_store):
        shard, body = failed_body
        _put_manifest(status_store, "partial", [shard])
        _put_status(status_store, shard, status="failed", error=ERROR, status_code=500, body=body)
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "partial", lambda_client=stub)
        handle.results(return_exceptions=True)
        assert handle.status() == {"pending": 0, "ok": 0, "failed": 1}
        handle.wait(timeout=10)
        self._assert_tail(stub, shard)
        assert stub.cell_events() == []  # observe-only, as ever

    def test_a_plain_failed_shard_is_still_out_of_both(self, status_store):
        # An unwindowed unit's failure reports no leaf: neither covered nor swept.
        words = [11828422946311897094, 11828141471335186438]
        _put_manifest(status_store, "plain", words)
        ok = {"total_obs": 7, "duration_s": 1.0}
        from zagg.telemetry import build_record

        ok["stats"] = build_record(shard_key=words[0], metadata=dict(ok))
        bad = {"error": "boom", "stats": build_record(shard_key=words[1], metadata={"error": "x"})}
        _put_status(status_store, words[0], body=ok)
        _put_status(
            status_store, words[1], status="failed", error="boom", status_code=500, body=bad
        )
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "plain", lambda_client=stub)
        handle.results(return_exceptions=True)
        handle.wait(timeout=10)
        coverage, sweep = _tail_events(stub)
        assert coverage["coverage"]["ranges"] == [[morton_decimal(words[0])] * 2]
        assert sweep["leaves"] == [[words[0], None]]


# ---------------------------------------------------------------------------
# Records, the roll-up and the readout on a partly failed invoke.
# ---------------------------------------------------------------------------


class TestRecords:
    def test_result_rows_are_one_per_leaf_and_ride_the_envelope(self, failed_body):
        shard, body = failed_body
        result = {"shard_key": shard, "status_code": 500, "body": body, "error": ERROR}
        rows, inline = runner._lambda_result_rows([result], run_id="rid")
        assert [(r["window"], r["success"], r["unit_windows"]) for r in rows] == [
            ("2018", True, 3),
            ("2019", True, 3),
            ("2020", False, 3),
        ]
        assert rows[2]["error"] == "RuntimeError: PUT failed"
        assert inline == []  # every row is re-derivable from the status object

    def test_the_rollup_counts_the_invoke_once_and_the_rerun_beside_it(self, failed_body):
        # A shard node's rollup merges the SIDECARS, and only landed windows
        # have one: two records of a three-window invoke still carry one
        # invoke's bill, and the re-run's single record adds its own.
        from zagg.telemetry import build_record, merge

        _shard, body = failed_body
        landed = [r for r in body["stats"] if r["success"]]
        assert [r["unit_windows"] for r in landed] == [3, 3]
        rolled = merge(landed)
        assert rolled["duration_total_s"] == pytest.approx(body["duration_total_s"])
        assert rolled["n_obs"] == sum(r["n_obs"] for r in landed) == 6
        rerun = build_record(
            shard_key=landed[0]["shard_key"],
            metadata={"unit_windows": 1, "duration_s": 2.0, "duration_total_s": 5.0},
            window=FAILED,
            run_id="rerun",
        )
        both = merge([*landed, rerun])
        assert both["duration_total_s"] == pytest.approx(body["duration_total_s"] + 5.0)

    def test_the_readout_counts_one_unit_and_one_error(self, monkeypatch, tmp_path):
        sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
        import windowed_emit_measure as tool

        import zagg.telemetry as telemetry

        monkeypatch.setattr(telemetry, "lambda_env", lambda: {"memory_mb": 2048, "arch": "arm64"})
        emit._patch(monkeypatch)
        _fail_one_window(monkeypatch)
        root, _shard, summary, _seen, _stub = _run_fleet(monkeypatch, tmp_path, _cfg(), "sync")
        fleet = tool.measure(str(root), store_kwargs={}, max_shards=4)["fleet"]
        (result,) = summary["results"]
        assert (fleet["units"], fleet["shards"], fleet["errors"]) == (1, 1, 1)
        assert fleet["windows_per_shard"]["p100"] == 2  # the landed leaves
        assert fleet["n_obs"] == 6
        # The invoke's bill, once: not once per landed row.
        assert fleet["duration_total_s"]["p100"] == pytest.approx(
            result["body"]["duration_total_s"]
        )
        wall = result["body"]["duration_total_s"]
        assert fleet["gb_seconds"] == pytest.approx(wall * 2.0)
