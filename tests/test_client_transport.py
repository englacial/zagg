"""Tests for the v2 Event transport (issue #327): status objects + poller.

No AWS, no network: Lambda traffic goes through a stub client whose Event
"workers" write status objects into an ``obstore`` MemoryStore — through the
REAL ``build_shard_status`` (the worker half), so the two halves of the
channel cannot drift — and the poller reads them back through the same
obstore functions the production path uses.
"""

import json
import re
import threading
import time
from concurrent.futures import Future

import obstore
import pytest
from obstore.store import MemoryStore

import zagg.client_transport as ct
from zagg import hive
from zagg.client import Run
from zagg.config import default_config
from zagg.dispatch_guard import guard_pacing
from zagg.pyramid import PYRAMID_SPEC_V2, expand_overviews


def _shard_error():
    """The CURRENT ShardError class, resolved at call time.

    tests/test_client.py's tqdm test importlib.reload()s zagg.client, which
    rebinds the class object mid-suite; an import-time reference here would
    stop matching the exceptions the (reloaded) transport raises.
    """
    import zagg.client

    return zagg.client.ShardError


# Mirrors tests/test_client.py: order-6 shardmap over three packed morton words.
_ATL06_SIG = {
    "type": "healpix",
    "indexing_scheme": "nested",
    "parent_order": 6,
    "child_order": 12,
    "layout": "fullsphere",
}
_WORDS = [11828422946311897094, 11828141471335186438, 11827859996358475782]
_LABELS = ["-4211324", "-4211323", "-4211322"]
_CREDS = {"accessKeyId": "AK", "secretAccessKey": "SK", "sessionToken": "TK"}
_STORE = "s3://test-bucket/out.zarr"
#: The shard order the staged-sweep audit's store declares — the config's own
#: ``parent_order`` above, because the fleet dispatcher's ladder gate (issue
#: #635) declines a manifest that denies the order it was handed.
_D8_SHARD_ORDER = _ATL06_SIG["parent_order"]


def _rec(n):
    return {"id": f"g{n}", "s3": f"s3://bucket/granule{n}.h5", "https": f"https://h/granule{n}.h5"}


@pytest.fixture
def catalog():
    return {
        "metadata": {"short_name": "ATL06", "version": "006"},
        "grid_signature": dict(_ATL06_SIG),
        "shard_keys": list(_WORDS),
        "granules": [[_rec(4), _rec(5), _rec(6)], [_rec(3)], [_rec(1), _rec(2)]],
    }


@pytest.fixture(autouse=True)
def _fast_poll(monkeypatch):
    monkeypatch.setattr(ct, "_POLL_INITIAL_INTERVAL_S", 0.01)
    monkeypatch.setattr(ct, "_POLL_MAX_INTERVAL_S", 0.02)


@pytest.fixture(autouse=True)
def _no_stats_verify(monkeypatch):
    from zagg import runner

    monkeypatch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", 0)


@pytest.fixture
def status_store(monkeypatch):
    """One in-memory status store shared by stub workers and the poller."""
    store = MemoryStore()
    monkeypatch.setattr(ct, "open_status_store", lambda prefix, kwargs: store)
    return store


def _envelope(body: dict, status=200) -> dict:
    return {"statusCode": status, "body": json.dumps(body)}


class EventStubLambdaClient:
    """Stub Lambda whose Event 'workers' write status objects (issue #327).

    A per-unit Event invoke computes the worker's canned response envelope and
    records it as a status object via the REAL worker half
    (``build_shard_status``), exactly as the deployed handler seam does. Mode
    invokes (ping/setup/finalize/...) answer synchronously like the v1 stub.
    """

    def __init__(
        self,
        status_store,
        *,
        fail=(),
        benign=(),
        drop=(),
        fail_times=None,
        drop_times=None,
        record=False,
        fail_modes=(),
    ):
        self._fail_modes = set(fail_modes)  # mode invokes that raise (e.g. finalize)
        self.status_store = status_store
        self._record = record  # deployed workers ride a stats record; stale ones don't
        self.events: list[tuple[str, str, dict]] = []
        self._lock = threading.Lock()
        self._fail = set(fail)  # deterministic failures (every attempt)
        self._benign = set(benign)
        self._drop = set(drop)  # invoke accepted, no status ever lands
        self._fail_times = dict(fail_times or {})  # key -> failures before success
        self._drop_times = dict(drop_times or {})  # key -> silent drops before success
        self.status_writes = 0

    def invoke(self, **kwargs):
        event = json.loads(kwargs["Payload"])
        with self._lock:
            self.events.append((kwargs["FunctionName"], kwargs["InvocationType"], event))
        if event.get("mode") is not None:
            if event["mode"] in self._fail_modes:
                raise RuntimeError(f"{event['mode']} down")
            # Simulate the deployed worker's status-prefix writes off the mode
            # invokes (issue #327): the setup event's dispatch manifest and
            # the stats event's tail-completion marker.
            if event["mode"] == "setup" and event.get("run_manifest"):
                manifest = {
                    "schema_version": 1,
                    **event["run_manifest"],
                    "config": event.get("config"),
                }
                obstore.put(self.status_store, ct.MANIFEST_NAME, json.dumps(manifest).encode())
            if (
                event["mode"] == "stats"
                and event.get("tail_status_url")
                # Mirrors write_tail_status: a failed finalize withholds the
                # marker so a reattached handle re-runs the idempotent tail.
                and not event.get("finalize_error")
            ):
                marker = {"status": "tail_done", "run_id": event.get("run_id")}
                obstore.put(self.status_store, ct.TAIL_NAME, json.dumps(marker).encode())
            return {
                "Payload": type(
                    "P", (), {"read": lambda self: b'{"statusCode": 200, "body": "{}"}'}
                )(),
                "FunctionError": None,
            }
        assert kwargs["InvocationType"] == "Event", "v2 cell invokes are fire-and-forget"
        key = event["shard_key"]
        if key in self._drop:
            return {"StatusCode": 202}
        if self._drop_times.get(key, 0) > 0:
            # A queue drop: the invoke is accepted (202) and no status ever
            # lands for this attempt — fault class (b).
            with self._lock:
                self._drop_times[key] -= 1
            return {"StatusCode": 202}
        if key in self._fail:
            response = _envelope({"error": "boom"}, status=500)
        elif self._fail_times.get(key, 0) > 0:
            with self._lock:
                self._fail_times[key] -= 1
            response = _envelope({"error": "boom"}, status=500)
        elif key in self._benign:
            response = _envelope({"error": "No granules found"})
        else:
            # Same canned ok body as tests/test_client.py's v1 stub, so agg
            # sync/event parity compares identical worker outputs.
            body = {"total_obs": 7, "duration_s": 1.5}
            if self._record:
                from zagg.telemetry import build_record

                body["stats"] = build_record(shard_key=int(key), metadata=dict(body))
            response = _envelope(body)
        self._write_status(event, response)
        return {"StatusCode": 202}

    def _write_status(self, event, response):
        obj_key, obj = ct.build_shard_status(event, response)
        with self._lock:
            self.status_writes += 1
        obstore.put(self.status_store, obj_key, json.dumps(obj).encode())

    def cell_events(self):
        return [(n, t, e) for n, t, e in self.events if e.get("mode") is None]

    def modes(self):
        return [e.get("mode") for _, _, e in self.events]


class DropNthStub(EventStubLambdaClient):
    """An Event stub that silently drops the Nth cell invoke, whatever its key.

    The dispatch order is the runner's (biggest-first buckets over the #197
    shuffle), so a breaker test that needs "one landed, then one timed out"
    keys the drop on invoke ORDER, not on a shard key.
    """

    def __init__(self, status_store, *, drop_nth, **kwargs):
        super().__init__(status_store, **kwargs)
        self._drop_nth = drop_nth
        self.cell_invokes = 0

    def invoke(self, **kwargs):
        event = json.loads(kwargs["Payload"])
        if event.get("mode") is None:
            with self._lock:
                self.cell_invokes += 1
                nth = self.cell_invokes
            if nth == self._drop_nth:
                self.events.append((kwargs["FunctionName"], kwargs["InvocationType"], event))
                return {"StatusCode": 202}
        return super().invoke(**kwargs)


#: A trip on the first timeout: ``window: 1`` makes the breaker judge each
#: result alone, so on a width-1 (serial) run "one landed, the next timed out"
#: trips ``timeout-ratio`` with a leaf on the store — the tail-skip case.
_TRIP_KNOBS = {"breaker": {"window": 1, "timeout_ratio": 0.5}}

#: A trip on the first store-fatal result.
_STORE_FATAL_KNOBS = {"breaker": {"store_fatal": 1, "store_fatal_first": 20}}


def _store_fatal_stub():
    """A sync stub whose every cell invoke answers PR #658's store-fatal 500 envelope."""
    from test_client import StubLambdaClient, _envelope

    class StoreFatalStub(StubLambdaClient):
        def invoke(self, **kwargs):
            answer = super().invoke(**kwargs)  # records the event
            if json.loads(kwargs["Payload"]).get("mode") is not None:
                return answer
            error = "window 2018 failed: HTTP status client error (403 Forbidden)"
            return _envelope({"error": error, "store_error_class": "store-fatal"}, status=500)

    return StoreFatalStub()


def _run(catalog, *, client, config=None, **kwargs):
    return Run.from_config(
        config if config is not None else default_config("atl06"),
        shardmap=catalog,
        store=_STORE,
        function_name="process-shard-test",
        lambda_client=client,
        source_credentials=_CREDS,
        **kwargs,
    )


def _put_status(store, shard_key, status="ok", attempt_id=None, **fields):
    """Hand-craft one status object (the worker-half schema)."""
    obj = {
        "schema_version": 1,
        "status": status,
        "attempt_id": attempt_id or f"aid-{time.time_ns()}",
        "shard": str(shard_key),
        "timings": None,
        "error": fields.pop("error", None),
        "status_code": fields.pop("status_code", 200),
        "body": fields.pop("body", {}),
    }
    obstore.put(store, ct.shard_status_key(shard_key), json.dumps(obj).encode())


def _pinned_config(commit, **output):
    """The dispatched config as a manifest records it: ``output.icechunk.commit`` pinned."""
    from dataclasses import asdict

    cfg = default_config("atl06")
    cfg.output["icechunk"] = {"commit": commit}
    cfg.output.update(output)
    return asdict(cfg)


def _put_manifest(store, run_id, shards, dispatched_at=None, config=None, slim=False):
    """Hand-craft one dispatch manifest (what the worker writes off setup).

    ``slim``: a large hive run's (issue #588) — the dispatcher's real slim
    block, so the list is ``null`` and ``shards_omitted`` counts ``shards``.
    """
    from dataclasses import asdict
    from datetime import datetime, timezone

    block = {
        "run_id": run_id,
        "shards": [str(int(w)) for w in shards],
        "semantic_hash": "ab" * 32,
        "dispatched_at": dispatched_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": {"short_name": "ATL06", "version": "006"},
    }
    manifest = {
        "schema_version": 1,
        **(ct.slim_run_manifest_block(block) if slim else block),
        "config": config if config is not None else asdict(default_config("atl06")),
    }
    obstore.put(store, ct.MANIFEST_NAME, json.dumps(manifest).encode())


# -- Run.dispatch(transport="event") -------------------------------------------


class TestEventDispatch:
    def test_all_shards_resolve_from_status_objects(self, catalog, status_store):
        stub = EventStubLambdaClient(status_store)
        handle = _run(catalog, client=stub).dispatch(transport="event")
        results = handle.results()
        assert set(results) == set(_WORDS)
        assert all(r["body"]["total_obs"] == 7 for r in results.values())
        assert all(r["status_code"] == 200 for r in results.values())
        assert handle.status() == {"pending": 0, "ok": 3, "failed": 0}
        cells = stub.cell_events()
        assert len(cells) == 3
        assert all(t == "Event" for _, t, _ in cells)
        # v2 needs no per-shard mirror target: statuses are always-on.
        assert all("result_url" not in e for _, _, e in cells)

    def test_event_payloads_match_sync_modulo_invocation_type(self, catalog, status_store):
        from test_client import StubLambdaClient

        sync_stub = StubLambdaClient()
        _run(catalog, client=sync_stub).dispatch(transport="sync").results()
        sync_events = {e["shard_key"]: e for _, _, e in sync_stub.cell_events()}

        stub = EventStubLambdaClient(status_store)
        _run(catalog, client=stub).dispatch(transport="event").results()
        event_events = {e["shard_key"]: e for _, _, e in stub.cell_events()}

        assert set(event_events) == set(sync_events) == set(_WORDS)
        for key in _WORDS:
            mine, theirs = dict(event_events[key]), dict(sync_events[key])
            assert mine.pop("run_id") and theirs.pop("run_id")  # fresh per dispatch
            assert mine == theirs  # byte-identical construction (issue #327)

    def test_paired_asset_entries_ride_the_event_fanout(self, catalog, status_store):
        # The async fan-out builds its own granule entries; it resolved bare
        # urls, so a paired-asset map (issue #425) reached the worker with no
        # sibling hrefs and every read raised (review finding, PR #432).
        sib = {"id": "s4", "s3": "s3://bucket/s4.h5", "https": "https://h/s4.h5"}
        catalog["granules"][0] = [{**_rec(4), "assets": {"l2a": sib}}]
        catalog["metadata"]["pairless"] = [{"id": "gX", "missing": "l2a"}]
        stub = EventStubLambdaClient(status_store)
        _run(catalog, client=stub).dispatch(transport="event").results()
        events = {e["shard_key"]: e for _, _, e in stub.cell_events()}
        assert events[_WORDS[0]]["granule_urls"] == [
            {"url": _rec(4)["s3"], "assets": {"l2a": sib["s3"]}}
        ]
        assert events[_WORDS[1]]["granule_urls"] == [_rec(3)["s3"]]
        # The unbounded pairless report never rides the per-cell submap block.
        assert "pairless" not in events[_WORDS[0]]["submap"]["metadata"]

    def test_benign_no_data_counts_ok(self, catalog, status_store):
        stub = EventStubLambdaClient(status_store, benign={_WORDS[2]})
        handle = _run(catalog, client=stub).dispatch(transport="event")
        assert handle.results()[_WORDS[2]]["error"] == "No granules found"
        assert handle.status() == {"pending": 0, "ok": 3, "failed": 0}

    def test_failed_status_raises_shard_error_without_redispatch(self, catalog, status_store):
        # Fault class (a): the worker RAN and reported an error, so the status
        # object is terminal — one execution billed, never max_retries of them
        # (issue #119's sync-transport rule, folded from the PR #343 review).
        stub = EventStubLambdaClient(status_store, fail={_WORDS[1]})
        handle = _run(catalog, client=stub, max_retries=3).dispatch(transport="event")
        with pytest.raises(_shard_error(), match="boom") as excinfo:
            handle.futures[_WORDS[1]].result(timeout=10)
        err = excinfo.value
        assert err.shard_key == _WORDS[1]
        assert err.label == _LABELS[1]
        assert err.payload["retries"] == 0  # the first and only attempt
        handle.results(return_exceptions=True)
        attempts = [e for _, _, e in stub.cell_events() if e["shard_key"] == _WORDS[1]]
        assert len(attempts) == 1

    def test_dropped_invoke_refires_once_and_recovers(self, catalog, status_store, monkeypatch):
        # Fault class (b): the first invoke is accepted but aged out of the
        # queue (no status object), so the one bounded re-fire recovers it.
        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        stub = EventStubLambdaClient(status_store, drop_times={_WORDS[0]: 1})
        handle = _run(catalog, client=stub, max_retries=3).dispatch(transport="event")
        result = handle.futures[_WORDS[0]].result(timeout=10)
        assert result["body"]["total_obs"] == 7
        assert result["retries"] == 1  # succeeded on the second attempt
        handle.results()
        assert handle.status() == {"pending": 0, "ok": 3, "failed": 0}

    def test_post_run_tail_matches_v1(self, catalog, status_store):
        stub = EventStubLambdaClient(status_store)
        handle = _run(catalog, client=stub).dispatch(transport="event")
        handle.results()  # drains + joins the tail
        modes = stub.modes()
        first_cell = modes.index(None)
        assert modes[:first_cell] == ["ping", "setup", "icechunk_init"]
        assert "finalize" in modes and "coverage" in modes and "stats" in modes
        assert modes[-1] == "icechunk_finalize"  # the tail's last invoke (issue #582)

    def test_a_tripped_breaker_skips_the_facade_tail(
        self, catalog, status_store, monkeypatch, caplog
    ):
        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        stub = DropNthStub(status_store, drop_nth=2, record=True)
        with caplog.at_level("INFO", logger="zagg.client"):
            handle = _run(catalog, client=stub, max_retries=1).dispatch(
                transport="event", dispatch=_TRIP_KNOBS, max_workers=1
            )
            handle.wait()
        # The trip is printed on the facade too, as on the CLI (review, PR #665).
        assert (
            "circuit breaker tripped (timeout-ratio) at result 2, width 1: "
            "1 units not dispatched, tail legs skipped"
        ) in caplog.text
        assert "Dispatch ramp" not in caplog.text
        assert "Circuit breaker TRIPPED, dispatch stopped: rule timeout-ratio" in caplog.text
        assert handle.breaker["rule"] == "timeout-ratio" and handle.breaker["tripped"]
        assert not hasattr(handle, "ramp")
        assert handle.status() == {"pending": 0, "ok": 1, "failed": 2}
        results = handle.results(return_exceptions=True)
        errors = sorted(
            e.payload["outcome"] for e in results.values() if isinstance(e, _shard_error())
        )
        assert errors == ["failed-unknown", "not-dispatched"]
        assert stub.cell_invokes == 2
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "coverage" in modes
        assert not any(m in modes for m in ("sweep", "stages", "icechunk_finalize"))
        assert handle.icechunk_finalize == {"skipped": "circuit breaker tripped (timeout-ratio)"}
        assert handle.families_sweep is None and handle.stage_sweep is None
        assert any("circuit breaker tripped (timeout-ratio)" in r for r in handle.tail_sweep_short)
        assert handle.dispatch_pacing == guard_pacing("transport='event'")

    def test_each_dispatch_tail_reads_its_own_guard(self, catalog, monkeypatch):
        # Two dispatches on one Run: A trips, B (clean) is dispatched before
        # A's tail runs. The guard rides each handle, so B's cannot stand in
        # for A's (review finding, PR #665). One fresh status store per run id.
        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        fresh = [MemoryStore(), MemoryStore()]
        stub_a = DropNthStub(fresh[0], drop_nth=2, record=True)

        def _store(prefix, kwargs):  # A's run id rode its (synchronous) setup invoke
            ids = {e["run_manifest"]["run_id"] for _, _, e in stub_a.events if "run_manifest" in e}
            return fresh[0] if any(i in prefix for i in ids) else fresh[1]

        monkeypatch.setattr(ct, "open_status_store", _store)
        gate = threading.Event()
        tail = Run._run_tail
        monkeypatch.setattr(Run, "_run_tail", lambda *a, **k: gate.wait(10) and tail(*a, **k))
        run = _run(catalog, client=stub_a, max_retries=1)
        a = run.dispatch(transport="event", dispatch=_TRIP_KNOBS, max_workers=1)
        run._lambda_client = stub_b = EventStubLambdaClient(fresh[1], record=True)
        b = run.dispatch(transport="event")
        gate.set()
        a.wait()
        b.wait()
        assert a.breaker["rule"] == "timeout-ratio" and a.breaker["tripped"]
        assert a.icechunk_finalize == {"skipped": "circuit breaker tripped (timeout-ratio)"}
        assert b.breaker is None and b.status() == {"pending": 0, "ok": 3, "failed": 0}
        assert "icechunk_finalize" not in stub_a.modes()
        assert "icechunk_finalize" in stub_b.modes()

    def test_sync_pool_timeouts_trip_and_stop_invoking(self, catalog, status_store):
        # Run.dispatch's DEFAULT transport (issue #652 phase 6): the first
        # shard times out, the breaker trips on its pool thread, and the next
        # shards raise the not-dispatched ShardError without invoking.
        from test_client import StubLambdaClient

        stub = StubLambdaClient(timeout=set(_WORDS))
        handle = _run(catalog, client=stub, max_retries=1).dispatch(
            transport="sync", dispatch=_TRIP_KNOBS, max_workers=1
        )
        handle.wait()
        assert len(stub.cell_events()) == 1
        assert handle.breaker == {
            "tripped": True,
            "rule": "timeout-ratio",
            "at_result": 1,
            "width": 1,
        }
        errors = handle.results(return_exceptions=True).values()
        outcomes = sorted(str(e.payload.get("outcome")) for e in errors)
        assert outcomes == ["None", "not-dispatched", "not-dispatched"]
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and not any(m in modes for m in ("sweep", "icechunk_finalize"))
        assert handle.icechunk_finalize == {"skipped": "circuit breaker tripped (timeout-ratio)"}
        assert handle.dispatch_pacing == guard_pacing("transport='sync'")

    def test_sync_pool_store_fatal_trips_through_the_raised_payload(self, catalog, status_store):
        # The facade's sync work raises ShardError: the guard reads the 500
        # envelope off its ``payload`` (``gate``'s except branch), end to end.
        stub = _store_fatal_stub()
        handle = _run(catalog, client=stub, max_retries=1).dispatch(
            transport="sync", dispatch=_STORE_FATAL_KNOBS, max_workers=1
        )
        handle.wait()
        assert len(stub.cell_events()) == 1
        assert handle.breaker["rule"] == "store-fatal" and handle.breaker["at_result"] == 1
        errors = handle.results(return_exceptions=True).values()
        outcomes = sorted(str(e.payload.get("outcome")) for e in errors)
        assert outcomes == ["None", "not-dispatched", "not-dispatched"]
        assert handle.icechunk_finalize == {"skipped": "circuit breaker tripped (store-fatal)"}

    def test_handle_carries_no_verdict_without_a_trip(self, catalog, status_store):
        handle = _run(catalog, client=EventStubLambdaClient(status_store)).dispatch(
            transport="event", max_workers=2
        )
        handle.results()
        assert handle.breaker is None and handle._guard.width == 2  # the full width
        assert handle.tail_sweep_short == []  # no trip: the verdict is the pre-#652 one
        # The sync transport has no poller, so no guard: the handle says why.
        sync = _run(catalog, client=__import__("test_client").StubLambdaClient()).dispatch()
        sync.results()
        assert sync.breaker is None
        assert sync.dispatch_pacing == guard_pacing("transport='sync'")

    def test_oversized_rows_point_at_the_status_prefix(self, catalog, status_store, monkeypatch):
        # An oversized row set on an EVENT run keeps its run record: the tail
        # points rows_from at the run's status prefix (which rows_from_status
        # reads — issue #327). The facade hardcoded result_prefix=None, so a
        # fleet-scale event run — the only kind that hits the cap — dropped
        # the record its byte-identical agg(invocation="event") twin keeps.
        from zagg import runner

        monkeypatch.setattr(runner, "_RUN_STATS_INLINE_CAP_BYTES", 10)
        stub = EventStubLambdaClient(status_store, record=True)
        handle = _run(catalog, client=stub).dispatch(transport="event")
        handle.results()
        handle.wait(timeout=10)
        run_id = stub.cell_events()[0][2]["run_id"]
        (stats,) = [e for _, _, e in stub.events if e.get("mode") == "stats"]
        assert stats["rows_from"] == ct.run_status_prefix(_STORE, run_id)
        assert stats["rows"] == []  # every row is re-derivable from the prefix

    def test_unknown_transport_refused(self, catalog, status_store):
        run = _run(catalog, client=EventStubLambdaClient(status_store))
        with pytest.raises(ValueError, match="transport"):
            run.dispatch(transport="carrier-pigeon")

    @pytest.mark.parametrize("transport", ["sync", "event"])
    def test_a_bad_dispatch_block_is_refused_before_any_invoke(
        self, catalog, status_store, transport
    ):
        # The Run.dispatch twin of the agg refusal (review finding, PR #665):
        # before the ping, the setup or any cell invoke, on both transports.
        stub = EventStubLambdaClient(status_store)
        with pytest.raises(ValueError, match=re.escape("dispatch: unknown key(s) ['canary']")):
            _run(catalog, client=stub).dispatch(transport=transport, dispatch={"canary": 8})
        assert stub.events == []


# -- StatusPoller unit behavior --------------------------------------------------


class TestStatusPoller:
    @staticmethod
    def _poller(store, **kwargs):
        kwargs.setdefault("drop_timeout_s", 1050.0)
        return ct.StatusPoller(lambda: store, **kwargs)

    def test_a_freed_slot_refills_in_the_same_tick(self):
        # _tick consumed AFTER dispatching, so a slot freed by a resolution sat
        # idle until the NEXT tick — up to _POLL_MAX_INTERVAL_S of dead window
        # (review, PR #343). Driven tick by tick, no threads.
        store = MemoryStore()
        fired: list[int] = []
        poller = self._poller(store, max_in_flight=1)
        f1 = poller.register(1, "1", dispatch=lambda: fired.append(1))
        poller.register(2, "2", dispatch=lambda: fired.append(2))
        poller._tick()
        assert fired == [1]  # the window holds shard 2 back
        _put_status(store, 1)
        poller._tick()  # ONE tick: resolve shard 1 AND fire shard 2
        assert f1.done()
        assert fired == [1, 2]

    def test_unknown_schema_version_warns_and_still_resolves(self, caplog):
        # A newer worker against an older client is the normal fleet state
        # during a layer roll: the object is honored (a shard whose data landed
        # must not fail on telemetry) but the mismatch is LOUD, so a future
        # bump can't read as v1 in silence (review, PR #343).
        store = MemoryStore()
        poller = self._poller(store)
        poller.register(1, "1", dispatch=lambda: None)
        entry = poller._entries[ct.shard_status_key(1)]
        poller._store = store
        obj = {
            "schema_version": 99,
            "status": "ok",
            "attempt_id": "a1",
            "status_code": 200,
            "body": {},
        }
        obstore.put(store, entry.key, json.dumps(obj).encode())
        with caplog.at_level("WARNING"):
            assert poller._consume(entry) is True
        assert "schema_version 99" in caplog.text
        assert entry.future.result(timeout=1)["status_code"] == 200

    def test_missing_attempt_id_does_not_poison_the_dedupe(self):
        # A falsy attempt_id used to land None in `consumed`, after which EVERY
        # later object read as None too and was ignored — so a shard that
        # succeeded on its re-fire was reported failed-unknown. Absent ids are
        # always-new instead (review, PR #343).
        store = MemoryStore()
        poller = self._poller(store, max_retries=3)
        poller.register(1, "1", dispatch=lambda: None)
        entry = poller._entries[ct.shard_status_key(1)]
        poller._store = store

        def _put_idless(status):
            obj = {"schema_version": 1, "status": status, "status_code": 200, "body": {}}
            obstore.put(store, entry.key, json.dumps(obj).encode())

        _put_idless("failed")
        assert poller._consume(entry) is True
        assert entry.consumed == set()  # nothing poisoned
        # A second id-less object is consumed again rather than ignored.
        entry.future = Future()
        _put_idless("ok")
        assert poller._consume(entry) is True
        assert entry.future.result(timeout=1)["status_code"] == 200

    def test_pacing_window_bounds_in_flight_dispatch(self):
        # max_in_flight=1: the second shard's Event must not fire until the
        # first resolves — load-bearing under the fleet's 60 s max event age.
        store = MemoryStore()
        fired: list[int] = []
        poller = self._poller(store, max_in_flight=1)
        f1 = poller.register(1, "1", dispatch=lambda: fired.append(1))
        f2 = poller.register(2, "2", dispatch=lambda: fired.append(2))
        poller.start()
        deadline = time.time() + 5
        while not fired and time.time() < deadline:
            time.sleep(0.005)
        time.sleep(0.05)  # a few idle ticks: the window must still hold
        assert fired == [1]
        _put_status(store, 1)
        assert f1.result(timeout=5)["status_code"] == 200
        deadline = time.time() + 5
        while len(fired) < 2 and time.time() < deadline:
            time.sleep(0.005)
        assert fired == [1, 2]
        _put_status(store, 2)
        assert f2.result(timeout=5)["status_code"] == 200
        poller.shutdown(wait=True)

    def test_attempt_id_dedupe_acts_once_per_execution(self):
        # A duplicate poll of the SAME attempt's object is acted on once; only
        # a NEW attempt_id is consumed. Driven straight through _consume (the
        # loop skips a settled entry anyway, so the property lives here).
        store = MemoryStore()
        poller = self._poller(store, max_retries=3)
        poller.register(1, "1", dispatch=lambda: None)
        entry = poller._entries[ct.shard_status_key(1)]
        poller._store = store
        _put_status(store, 1, status="failed", attempt_id="a1", error="boom")
        assert poller._consume(entry) is True
        assert poller._consume(entry) is False  # same attempt_id: not re-acted
        assert entry.consumed == {"a1"}

    def test_drop_refire_consumes_a_late_status_object(self):
        # The re-fire of a dropped invoke (class (b)) is the one place a
        # queued entry can still see an object land; it resolves normally.
        store = MemoryStore()
        now = {"t": 0.0}
        fired: list[float] = []
        poller = ct.StatusPoller(
            lambda: store,
            drop_timeout_s=10.0,
            max_retries=3,
            clock=lambda: now["t"],
        )
        fut = poller.register(1, "1", dispatch=lambda: fired.append(now["t"]))
        poller.start()
        deadline = time.time() + 5
        while len(fired) < 1 and time.time() < deadline:
            time.sleep(0.005)
        now["t"] = 11.0  # the first attempt drops -> the single re-fire
        deadline = time.time() + 5
        while len(fired) < 2 and time.time() < deadline:
            time.sleep(0.005)
        _put_status(store, 1, status="ok", attempt_id="a2")
        assert fut.result(timeout=5)["retries"] == 1
        poller.shutdown(wait=True)

    def test_invoke_fault_burns_an_attempt_and_retries(self):
        store = MemoryStore()
        now = {"t": 1000.0}
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("TooManyRequestsException")

        poller = ct.StatusPoller(
            lambda: store, drop_timeout_s=1e6, max_retries=3, clock=lambda: now["t"]
        )
        fut = poller.register(1, "1", dispatch=flaky)
        poller.start()
        deadline = time.time() + 5
        while calls["n"] < 1 and time.time() < deadline:
            time.sleep(0.005)
        now["t"] = 1002.0  # the 2 s class-(c) backoff expires
        deadline = time.time() + 5
        while calls["n"] < 2 and time.time() < deadline:
            time.sleep(0.005)
        assert calls["n"] == 2
        _put_status(store, 1)
        assert fut.result(timeout=5)["retries"] == 1
        poller.shutdown(wait=True)

    def test_throttled_invoke_backs_off_instead_of_bursting(self):
        # The medium-high from the PR #343 review: re-queueing without a
        # not-before stamp spent the WHOLE retry budget in ~0.3 ms, because
        # _dispatch_up_to_window's `while True` re-reads `queued` in the same
        # pass. Each re-fire now waits 2**attempts seconds — enforced by the
        # clock, not by sleeping on the poller thread.
        store = MemoryStore()
        now = {"t": 1000.0}
        fired: list[float] = []

        def always_throttled():
            fired.append(now["t"])
            raise RuntimeError("TooManyRequestsException: Rate exceeded")

        poller = ct.StatusPoller(
            lambda: store, drop_timeout_s=1e6, max_retries=4, clock=lambda: now["t"]
        )
        fut = poller.register(1, "1", dispatch=always_throttled)
        poller.start()
        # 2 s, then 4 s, then 8 s: no re-fire until the clock passes the stamp.
        for expected, t in ((1, 1002.0), (2, 1006.0), (3, 1014.0)):
            deadline = time.time() + 5
            while len(fired) < expected and time.time() < deadline:
                time.sleep(0.005)
            time.sleep(0.05)  # several ticks INSIDE the backoff
            assert len(fired) == expected, fired
            now["t"] = t
        result = fut.result(timeout=5)  # attempt 4 == max_retries: terminal
        assert "TooManyRequestsException" in result["error"]
        assert result["retries"] == 3
        assert fired == [1000.0, 1002.0, 1006.0, 1014.0]
        poller.shutdown(wait=True)

    def test_a_store_fatal_status_is_terminal_and_names_its_class(self):
        # Issue #651: the worker ended its invoke on a refusal the store would
        # repeat; the status object is ``failed`` with the class explicit, the
        # result dict carries it, and the shard is never re-dispatched
        # (class (a) above, said by name for #652's breaker and the readouts).
        store = MemoryStore()
        fired: list[int] = []
        poller = self._poller(store, max_retries=3)
        fut = poller.register(7, "7", dispatch=lambda: fired.append(1))
        error = "window 2019: GenericError: Generic S3 error: Got invalid DeleteObjects response"
        _put_status(
            store,
            7,
            status="failed",
            error=error,
            status_code=500,
            body={
                "error": error,
                "store_error_class": "store-fatal",
                "windows_unattempted": ["2020"],
            },
        )
        poller.start()
        result = fut.result(timeout=5)
        poller.shutdown(wait=True)
        assert result["error"] == error and result["store_error_class"] == "store-fatal"
        assert result["retries"] == 0 and fired == [1]

    def test_default_failure_resolution_is_a_result_dict(self):
        # agg's mode: no on_failed -> the terminal failure resolves as the
        # v1-shaped result dict (the accumulator records it), never an exception.
        store = MemoryStore()
        poller = self._poller(store, max_retries=1)
        fut = poller.register(7, "7", dispatch=lambda: None)
        _put_status(store, 7, status="failed", error="boom", status_code=500)
        poller.start()
        result = fut.result(timeout=5)
        assert result["error"] == "boom"
        assert result["status_code"] == 500
        assert result["shard_key"] == 7
        poller.shutdown(wait=True)

    def test_transient_list_fault_does_not_kill_the_poller(self):
        store = MemoryStore()
        calls = {"n": 0}
        real_list = obstore.list

        def flaky_list(s, *a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("503 slow down")
            return real_list(s, *a, **k)

        poller = self._poller(store)
        fut = poller.register(1, "1", dispatch=lambda: None)
        _put_status(store, 1)
        try:
            obstore.list = flaky_list
            poller.start()
            assert fut.result(timeout=5)["status_code"] == 200
        finally:
            obstore.list = real_list
        poller.shutdown(wait=True)

    def test_observe_only_entry_never_redispatches(self):
        # attach's mode (issue #327 phase 5): dispatch=None -> a failed status
        # resolves immediately through on_failed, no retry budget to spend.
        store = MemoryStore()
        failures: list = []
        poller = self._poller(
            store,
            max_retries=3,
            on_failed=lambda e, r: (failures.append(r), e.future.set_result(r)),
        )
        fut = poller.register(1, "1", dispatch=None)
        _put_status(store, 1, status="failed", error="boom")
        poller.start()
        assert fut.result(timeout=5)["error"] == "boom"
        assert len(failures) == 1
        poller.shutdown(wait=True)


# -- drop detection (issue #327 phase 4) ------------------------------------------


class TestDropDetection:
    """A shard with no status object after (function timeout + 60 s max event
    age + margin) resolves ``failed-unknown`` — the silent-drop outcome the
    fleet's ``MaximumEventAgeInSeconds: 60`` makes possible under
    backpressure — instead of hanging. Fault class (b): exactly ONE bounded
    re-fire, so the worst case is 2 billed executions (issue #151)."""

    def test_drop_resolves_failed_unknown_after_deadline(self):
        store = MemoryStore()
        now = {"t": 1000.0}
        poller = ct.StatusPoller(
            lambda: store,
            drop_timeout_s=100.0,
            max_retries=1,
            clock=lambda: now["t"],
        )
        fut = poller.register(1, "1", dispatch=lambda: None)
        poller.start()
        time.sleep(0.06)  # several ticks inside the deadline
        assert not fut.done()  # not classified early
        now["t"] = 1101.0  # past dispatch (t=1000) + drop_timeout (100)
        result = fut.result(timeout=5)
        assert result["outcome"] == "failed-unknown"
        assert "failed-unknown" in result["error"]
        assert "60s max event age" in result["error"]
        assert result["status_code"] is None
        assert result["body"] == {}
        poller.shutdown(wait=True)

    def test_drop_refires_exactly_once_then_resolves(self):
        # max_retries=5, but a status-less shard costs at most TWO executions:
        # the bound that keeps the client from reintroducing the 3 x 900 s
        # re-billing MaximumRetryAttempts: 0 exists to prevent (issue #151).
        store = MemoryStore()
        now = {"t": 0.0}
        fired: list[float] = []
        poller = ct.StatusPoller(
            lambda: store,
            drop_timeout_s=10.0,
            max_retries=5,
            clock=lambda: now["t"],
        )
        fut = poller.register(5, "5", dispatch=lambda: fired.append(now["t"]))
        poller.start()
        deadline = time.time() + 5
        while len(fired) < 1 and time.time() < deadline:
            time.sleep(0.005)
        now["t"] = 11.0  # first attempt drops -> the single re-fire
        deadline = time.time() + 5
        while len(fired) < 2 and time.time() < deadline:
            time.sleep(0.005)
        assert len(fired) == 2 and not fut.done()
        now["t"] = 22.0  # the re-fire drops too: terminal, no third execution
        result = fut.result(timeout=5)
        assert result["outcome"] == "failed-unknown"
        assert result["retries"] == 1
        time.sleep(0.1)
        assert len(fired) == 2
        poller.shutdown(wait=True)

    def test_drop_refire_recovers_the_shard(self):
        store = MemoryStore()
        now = {"t": 0.0}
        fired: list[float] = []
        poller = ct.StatusPoller(
            lambda: store,
            drop_timeout_s=10.0,
            max_retries=2,
            clock=lambda: now["t"],
        )
        fut = poller.register(5, "5", dispatch=lambda: fired.append(now["t"]))
        poller.start()
        deadline = time.time() + 5
        while len(fired) < 1 and time.time() < deadline:
            time.sleep(0.005)
        now["t"] = 11.0  # first attempt drops -> re-fire, not resolution
        deadline = time.time() + 5
        while len(fired) < 2 and time.time() < deadline:
            time.sleep(0.005)
        assert len(fired) == 2 and not fut.done()
        # The re-fire's execution lands its status: the shard recovers.
        _put_status(store, 5)
        result = fut.result(timeout=5)
        assert result["retries"] == 1
        assert "outcome" not in result
        poller.shutdown(wait=True)

    def test_client_event_run_counts_a_drop_failed(self, catalog, status_store, monkeypatch):
        # End-to-end: the invoke is accepted (202) but no status ever lands;
        # the shard's future raises ShardError with the DISTINCT outcome in
        # the payload, status() buckets it failed, and the drop cost exactly
        # one re-fire even at max_retries=5.
        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        stub = EventStubLambdaClient(status_store, drop={_WORDS[0]})
        handle = _run(catalog, client=stub, max_retries=5).dispatch(transport="event")
        with pytest.raises(_shard_error(), match="failed-unknown") as excinfo:
            handle.futures[_WORDS[0]].result(timeout=10)
        assert excinfo.value.payload["outcome"] == "failed-unknown"
        handle.results(return_exceptions=True)
        assert handle.status() == {"pending": 0, "ok": 2, "failed": 1}
        attempts = [e for _, _, e in stub.cell_events() if e["shard_key"] == _WORDS[0]]
        assert len(attempts) == 2  # one bounded re-fire, not five


# -- agg invocation="event" (issue #327 phase 6) -----------------------------------


class TestAggEventMode:
    """``agg(..., invocation="event")``: the same blocking call-shape, return
    value, and tail as sync, with Event dispatch + status-object resolution
    underneath (espg ruling, session 2026-07-29). The benchmark harness and CI
    stay on sync — their metrics come from invoke responses."""

    @staticmethod
    def _agg(
        catalog_file,
        monkeypatch,
        stub,
        invocation,
        config=None,
        workers=3,
        clamp=None,
        **agg_kwargs,
    ):
        # ``workers`` is the requested ``max_workers``; ``clamp`` the stubbed
        # preflight (``compute_available_workers``) result, default ``workers``.
        from unittest.mock import MagicMock

        import boto3

        from zagg import hive, runner
        from zagg.concurrency import ConcurrencyReport

        session = MagicMock()
        session.client.side_effect = lambda service, **k: (
            stub if service == "lambda" else MagicMock()
        )
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: session)
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 900)
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                workers if clamp is None else clamp,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )
        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: dict(_CREDS))
        monkeypatch.setattr(hive, "read_manifest", lambda *a, **k: None)
        return runner.agg(
            config if config is not None else default_config("atl06"),
            catalog=catalog_file,
            store=_STORE,
            backend="lambda",
            invocation=invocation,
            function_name="process-shard-test",
            max_workers=workers,
            **agg_kwargs,
        )

    @pytest.fixture
    def catalog_file(self, catalog, tmp_path):
        p = tmp_path / "shardmap.json"
        p.write_text(json.dumps(catalog))
        return str(p)

    def test_agg_event_parity_with_sync(self, catalog_file, monkeypatch, status_store):
        from test_client import StubLambdaClient

        sync_stub = StubLambdaClient()
        sync_summary = self._agg(catalog_file, monkeypatch, sync_stub, "sync")
        event_stub = EventStubLambdaClient(status_store)
        event_summary = self._agg(catalog_file, monkeypatch, event_stub, "event")

        # Same return value shape and counts.
        assert set(event_summary) == set(sync_summary)
        for key in (
            "total_cells",
            "cells_with_data",
            "cells_error",
            "total_obs",
            "backend",
            "store_path",
            "function_name",
        ):
            assert event_summary[key] == sync_summary[key], key

        # Same cell events modulo InvocationType (+ per-run identity).
        sync_cells = {e["shard_key"]: e for _, _, e in sync_stub.cell_events()}
        event_cells = {e["shard_key"]: e for _, _, e in event_stub.cell_events()}
        assert set(event_cells) == set(sync_cells) == set(_WORDS)
        for key in _WORDS:
            mine, theirs = dict(event_cells[key]), dict(sync_cells[key])
            assert mine.pop("run_id") and theirs.pop("run_id")
            assert mine == theirs
        assert all(t == "Event" for _, t, _ in event_stub.cell_events())
        assert all(t == "RequestResponse" for _, t, _ in sync_stub.cell_events())

        # Same tail: identical worker-invoke mode sequence (ping, setup,
        # finalize backstop, coverage, stats).
        assert [m for m in event_stub.modes() if m] == [m for m in sync_stub.modes() if m]

    def test_agg_event_records_a_failed_shard_like_sync(
        self, catalog_file, monkeypatch, status_store
    ):
        from test_client import StubLambdaClient

        sync_summary = self._agg(
            catalog_file,
            monkeypatch,
            StubLambdaClient(fail={_WORDS[1]}),
            "sync",
            max_retries=1,
        )
        event_summary = self._agg(
            catalog_file,
            monkeypatch,
            EventStubLambdaClient(status_store, fail={_WORDS[1]}),
            "event",
            max_retries=1,
        )
        for key in ("cells_error", "cells_with_data", "total_obs"):
            assert event_summary[key] == sync_summary[key], key
        assert event_summary["cells_error"] == 1
        (bad,) = [r for r in event_summary["results"] if r.get("error")]
        assert bad["error"] == "boom" and bad["shard_key"] == _WORDS[1]

    def test_agg_event_drop_is_a_failed_cell(self, catalog_file, monkeypatch, status_store):
        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        summary = self._agg(
            catalog_file,
            monkeypatch,
            EventStubLambdaClient(status_store, drop={_WORDS[0]}),
            "event",
            max_retries=1,
        )
        assert summary["cells_error"] == 1
        (bad,) = [r for r in summary["results"] if r.get("error")]
        assert bad["outcome"] == "failed-unknown"

    def test_agg_event_summary_carries_the_breaker(self, catalog_file, monkeypatch, status_store):
        # Defaults, three shards, 3 requested workers clamped to 2 by the
        # preflight: the guard paces at the clamp (not ``max_workers``) and
        # nothing trips; no ramp key (2026-10-14).
        from zagg import runner

        widths: list[int] = []
        real = runner.DispatchGuard
        monkeypatch.setattr(
            runner, "DispatchGuard", lambda *a, **k: widths.append(k["workers"]) or real(*a, **k)
        )
        summary = self._agg(
            catalog_file, monkeypatch, EventStubLambdaClient(status_store), "event", clamp=2
        )
        assert summary["breaker"] is None and "ramp" not in summary
        assert widths == [2]
        sync = self._agg(
            catalog_file, monkeypatch, __import__("test_client").StubLambdaClient(), "sync"
        )
        assert sync["breaker"] is None and "ramp" not in sync  # no poller, no guard

    def test_a_tripped_breaker_refuses_the_rest_and_skips_the_tail(
        self, catalog_file, monkeypatch, status_store
    ):
        from zagg import runner

        monkeypatch.setattr(ct, "drop_timeout_s", lambda timeout_s: 0.05)
        tail_calls: list[str] = []
        for name in ("_invoke_lambda_families_sweep", "_invoke_lambda_stage_sweep"):
            monkeypatch.setattr(
                runner, name, lambda *a, _n=name, **k: tail_calls.append(_n) or {"fired": 0}
            )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_icechunk_finalize",
            lambda *a, **k: tail_calls.append("_invoke_lambda_icechunk_finalize") or {"tag": "t"},
        )
        # Control: the same knobs, nothing dropped — the tail runs.
        control = self._agg(
            catalog_file,
            monkeypatch,
            EventStubLambdaClient(status_store, record=True),
            "event",
            max_retries=1,
            dispatch=_TRIP_KNOBS,
            clamp=1,
        )
        assert control["breaker"] is None and control["cells_with_data"] == 3
        assert "_invoke_lambda_icechunk_finalize" in tail_calls
        assert any(c.endswith("_sweep") for c in tail_calls)
        tail_calls.clear()

        # The second cell invoke never writes a status: one leaf landed, the
        # next unit timed out, the breaker trips and the third is refused. A
        # fresh status store, so the control run's objects cannot stand in
        # for the dropped unit's.
        fresh = MemoryStore()
        monkeypatch.setattr(ct, "open_status_store", lambda prefix, kwargs: fresh)
        # 3 requested workers, a preflight clamp of 1: the guard's width is the
        # clamp, else all three shards fire before the trip and none is refused.
        stub = DropNthStub(fresh, drop_nth=2, record=True)
        summary = self._agg(
            catalog_file, monkeypatch, stub, "event", max_retries=1, dispatch=_TRIP_KNOBS, clamp=1
        )
        assert summary["breaker"] == {
            "tripped": True,
            "rule": "timeout-ratio",
            "at_result": 2,
            "width": 1,
        }
        assert summary["cells_with_data"] == 1 and summary["cells_error"] == 2
        outcomes = sorted(r.get("outcome") for r in summary["results"] if r.get("error"))
        assert outcomes == ["failed-unknown", "not-dispatched"]
        assert stub.cell_invokes == 2  # the refused unit was never invoked
        # The tail legs are cancelled; the run record and coverage still land.
        assert tail_calls == []
        assert summary["families_sweep"] is None and summary["stage_sweep"] is None
        assert summary["icechunk_finalize"] == {
            "skipped": "circuit breaker tripped (timeout-ratio)"
        }
        # The run's tail verdict names the trip (issue #610's key, #652's reason).
        assert any(
            "circuit breaker tripped (timeout-ratio)" in r for r in summary["tail_sweep_short"]
        )
        assert "circuit breaker on (invocation='event')" in summary["dispatch_pacing"]
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "coverage" in modes

    def test_a_bad_dispatch_block_is_refused_before_any_invoke(
        self, catalog_file, monkeypatch, status_store
    ):
        stub = EventStubLambdaClient(status_store)
        with pytest.raises(ValueError, match="can never trip"):
            self._agg(
                catalog_file, monkeypatch, stub, "event", dispatch={"breaker": {"store_fatal": 30}}
            )
        assert stub.events == []

    # -- the breaker on the pool transports (issue #652 phase 6) ----------------

    @staticmethod
    def _tail_recorder(monkeypatch):
        from zagg import runner

        calls: list[str] = []
        for name in ("_invoke_lambda_families_sweep", "_invoke_lambda_stage_sweep"):
            monkeypatch.setattr(
                runner, name, lambda *a, _n=name, **k: calls.append(_n) or {"fired": 0}
            )
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_icechunk_finalize",
            lambda *a, **k: calls.append("icechunk_finalize") or {"tag": "t"},
        )
        return calls

    @staticmethod
    def _cell_results(monkeypatch, make):
        """Route the async channel's per-cell invoke to ``make(shard_key)``; count calls."""
        from zagg import runner

        calls: list[int] = []

        def fake(client, block, shard_key, *a, **k):
            calls.append(int(shard_key))
            return {"shard_key": int(shard_key), "lambda_duration": 1.0, **make(int(shard_key))}

        monkeypatch.setattr(runner, "_invoke_lambda_cell", fake)
        return calls

    def _assert_tripped_without_a_tail(self, summary, tail, rule):
        assert summary["breaker"]["tripped"] and summary["breaker"]["rule"] == rule
        assert summary["breaker"]["at_result"] == 1 and summary["breaker"]["width"] == 1
        outcomes = sorted(str(r.get("outcome")) for r in summary["results"])
        assert outcomes == ["None", "not-dispatched", "not-dispatched"]
        assert summary["cells_error"] == 3 and summary["cells_with_data"] == 0
        assert tail == []  # no families pass, no staged sweep, no Icechunk finalize
        assert summary["families_sweep"] is None and summary["stage_sweep"] is None
        assert summary["icechunk_finalize"] == {"skipped": f"circuit breaker tripped ({rule})"}
        assert any(f"circuit breaker tripped ({rule})" in r for r in summary["tail_sweep_short"])

    def test_async_timeouts_trip_and_stop_invoking(self, catalog_file, monkeypatch, status_store):
        # agg's DEFAULT transport (invocation="async", the #151 result-object
        # channel): the first unit's deadline miss trips; the rest never run.
        from zagg.dispatch_guard import NO_RESULT_PREFIX

        tail = self._tail_recorder(monkeypatch)
        calls = self._cell_results(
            monkeypatch,
            lambda k: {"status_code": None, "body": {}, "error": f"{NO_RESULT_PREFIX} 990s"},
        )
        summary = self._agg(
            catalog_file,
            monkeypatch,
            EventStubLambdaClient(status_store),
            "async",
            dispatch=_TRIP_KNOBS,
            clamp=1,
        )
        assert len(calls) == 1
        self._assert_tripped_without_a_tail(summary, tail, "timeout-ratio")
        assert summary["dispatch_pacing"] == guard_pacing("invocation='async'")

    def test_async_store_fatal_results_trip_and_stop_invoking(
        self, catalog_file, monkeypatch, status_store
    ):
        tail = self._tail_recorder(monkeypatch)
        error = "window 2018 failed: HTTP status client error (403 Forbidden)"
        calls = self._cell_results(
            monkeypatch,
            lambda k: {
                "status_code": 500,
                "body": {"error": error, "store_error_class": "store-fatal"},
                "error": error,
            },
        )
        summary = self._agg(
            catalog_file,
            monkeypatch,
            EventStubLambdaClient(status_store),
            "async",
            dispatch={"breaker": {"store_fatal": 1, "store_fatal_first": 20}},
            clamp=1,
        )
        assert len(calls) == 1
        self._assert_tripped_without_a_tail(summary, tail, "store-fatal")

    def test_sync_store_fatal_results_trip_and_stop_invoking(
        self, catalog_file, monkeypatch, status_store
    ):
        # invocation="sync": PR #658's real 500 envelope, read off the response.
        tail = self._tail_recorder(monkeypatch)
        stub = _store_fatal_stub()
        summary = self._agg(
            catalog_file,
            monkeypatch,
            stub,
            "sync",
            dispatch=_STORE_FATAL_KNOBS,
            clamp=1,
            max_retries=1,
        )
        assert len(stub.cell_events()) == 1
        self._assert_tripped_without_a_tail(summary, tail, "store-fatal")

    def test_agg_event_records_each_result_once(self, catalog_file, monkeypatch, status_store):
        # The ruling: the poller records Event results, so the pool gate must
        # not record them again (two identities, ``unit`` vs ``shard_key``,
        # would never dedupe). Capture the guard the runner builds.
        from zagg import runner

        guards: list = []
        real = runner.DispatchGuard
        monkeypatch.setattr(
            runner, "DispatchGuard", lambda *a, **k: guards.append(real(*a, **k)) or guards[-1]
        )
        summary = self._agg(catalog_file, monkeypatch, EventStubLambdaClient(status_store), "event")
        (guard,) = guards
        assert len(summary["results"]) == 3 and len(guard._records) == 3

    def test_sync_timeouts_trip_and_stop_invoking(self, catalog_file, monkeypatch, status_store):
        # invocation="sync" (RequestResponse): Lambda's own "Task timed out".
        from test_client import StubLambdaClient

        tail = self._tail_recorder(monkeypatch)
        stub = StubLambdaClient(timeout=set(_WORDS))
        summary = self._agg(
            catalog_file, monkeypatch, stub, "sync", dispatch=_TRIP_KNOBS, clamp=1, max_retries=1
        )
        assert len(stub.cell_events()) == 1
        self._assert_tripped_without_a_tail(summary, tail, "timeout-ratio")
        assert summary["dispatch_pacing"] == guard_pacing("invocation='sync'")

    @pytest.mark.parametrize("invocation", ["async", "sync"])
    def test_a_healthy_pool_run_is_unchanged(
        self, catalog_file, monkeypatch, status_store, invocation
    ):
        # Same invokes and the same summary apart from ``breaker: None``: the
        # guarded run against one with the breaker knocked out of the loop.
        from test_client import StubLambdaClient

        from zagg import dispatch as dispatch_mod
        from zagg import runner

        loop = dispatch_mod.dispatch

        def unguarded(*a, guard=None, refuse=None, **k):
            return loop(*a, **k)

        def run(guarded):
            tail = self._tail_recorder(monkeypatch)
            calls = None
            if invocation == "async":  # the sync stub answers RequestResponse itself
                calls = self._cell_results(
                    monkeypatch,
                    lambda k: {"status_code": 200, "body": {"total_obs": 7}, "error": None},
                )
            monkeypatch.setattr(runner, "dispatch", loop if guarded else unguarded)
            stub = StubLambdaClient()
            summary = self._agg(catalog_file, monkeypatch, stub, invocation)
            cells = [e["shard_key"] for _, _, e in stub.cell_events()]
            return summary, sorted(calls if calls is not None else cells), tail

        guarded, invoked_g, tail_g = run(True)
        bare, invoked_b, tail_b = run(False)
        assert invoked_g == invoked_b and len(invoked_g) == 3
        assert guarded["breaker"] is None and bare["breaker"] is None
        assert tail_g == tail_b
        # The whole summary, bar what varies run to run: the clocks, the
        # run-id-keyed record path, and the results' completion order and
        # per-unit wall clocks (compared below with those set aside).
        volatile = {"fanout_s", "finalize_s", "setup_s", "wall_time_s", "run_stats_path"}

        def steady(summary):
            return {k: v for k, v in summary.items() if k not in volatile | {"results"}}

        def units(summary):
            rows = [{k: v for k, v in r.items() if k != "wall_time"} for r in summary["results"]]
            return sorted(rows, key=lambda r: r["shard_key"])

        assert set(guarded) == set(bare)
        assert steady(guarded) == steady(bare) and units(guarded) == units(bare)

    def test_windowed_units_resolve_on_both_windows(self, catalog_file, monkeypatch, status_store):
        # The ONE seam where the channel's two halves derive the same object
        # name independently: runner registers with window["label"], the worker
        # half reads it off event["window"]["label"] in build_shard_status. A
        # disagreement is silent (the client waits out the whole drop window and
        # reports failed-unknown for a shard that succeeded), and nothing joined
        # the two before — the helper and worker tests each cover one side
        # (review finding, PR #343). Locally testable: one event run over a
        # 2-window explicit schedule.
        cfg = default_config("atl06")
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "unit": "window",  # the per-window fan-out is the seam under test
            "time_field": "h_li",  # a declared column, so validate_config passes
            "epoch": "2018-01-01T00:00:00Z",
            "windows": [
                {"label": "w1", "start": "2020-01-01T00:00:00Z", "end": "2021-01-01T00:00:00Z"},
                {"label": "w2", "start": "2021-01-01T00:00:00Z", "end": "2022-01-01T00:00:00Z"},
            ],
        }
        stub = EventStubLambdaClient(status_store)
        summary = self._agg(catalog_file, monkeypatch, stub, "event", config=cfg)
        # 3 shards x 2 windows, every unit resolved from its own status object.
        assert summary["total_cells"] == 6
        assert summary["cells_error"] == 0
        assert summary["cells_with_data"] == 6
        cells = stub.cell_events()
        assert len(cells) == 6
        assert sorted(e["window"]["label"] for _, _, e in cells) == ["w1"] * 3 + ["w2"] * 3
        # Both halves agreed on the per-window key: no shard clobbered another.
        listed = sorted(str(m["path"]) for b in obstore.list(status_store) for m in b)
        assert [k for k in listed if k.startswith("shard-")] == sorted(
            ct.shard_status_key(w, window=lbl) for w in _WORDS for lbl in ("w1", "w2")
        )

    def test_bogus_invocation_refused(self, catalog_file, monkeypatch, status_store):
        with pytest.raises(ValueError, match="Unknown invocation"):
            self._agg(
                catalog_file, monkeypatch, EventStubLambdaClient(status_store), "carrier-pigeon"
            )

    def test_raster_fanout_refuses_event(self):
        from zagg import runner

        with pytest.raises(ValueError, match="Unknown invocation"):
            runner.RasterStrategy()._run_lambda_shards(
                default_config("atl06"),
                [],
                {},
                None,
                "s3://b/x.zarr",
                times_us=[],
                overwrite=False,
                max_workers=1,
                region="us-west-2",
                function_name="f",
                max_retries=1,
                output_credentials=None,
                output_endpoint_url=None,
                invocation="event",
            )

    def test_temporal_fanout_refuses_event(self):
        from zagg import runner

        cfg = default_config("atl06")
        cfg.output["format"] = "parquet"
        with pytest.raises(ValueError, match="Unknown invocation"):
            runner._run_lambda_events(
                cfg,
                [],
                "s3://b/x.parquet",
                max_workers=1,
                region="us-west-2",
                function_name="f",
                invocation="event",
            )


# -- reattach (issue #327 phase 5) ------------------------------------------------


class TestAttach:
    """``Run.attach(store, run_id)`` rebuilds a handle from the dispatch
    manifest + current status objects; the post-run tail runs only when not
    already recorded. Observe-only: reads, no re-dispatch (D8 intact)."""

    def test_attach_post_run_resolves_and_skips_the_tail(self, catalog, status_store):
        # A completed live run left manifest + statuses + tail marker behind.
        live = EventStubLambdaClient(status_store)
        _run(catalog, client=live).dispatch(transport="event").results()
        run_id = live.cell_events()[0][2]["run_id"]

        fresh = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, run_id, lambda_client=fresh)
        results = handle.results()
        assert set(results) == set(_WORDS)
        assert all(r["body"]["total_obs"] == 7 for r in results.values())
        assert handle.status() == {"pending": 0, "ok": 3, "failed": 0}
        handle.wait(timeout=10)
        # The tail was recorded (tail.json): the reattached handle re-fires
        # NOTHING of it except the Icechunk finalize (issue #582) — the marker
        # precedes the finalize in the tail, so a client that died between the
        # two left the run untagged; the finalize is idempotent, so a tagged
        # run costs one no-op invoke. Off the config's knob: attach never held
        # the dispatch's init record, and the event says so.
        assert fresh.modes() == ["icechunk_finalize"]
        (_n, kind, event) = fresh.events[0]
        assert kind == "RequestResponse" and event["run_id"] == run_id
        assert event["store_path"] == _STORE and event["icechunk_init"] is None
        # Newest-only: a later run on the repo leaves this one to its tag.
        assert event["newest_only"] is True
        # The stub's canned body is a fail-open record, and wait() did not raise.
        assert "unexpected icechunk_finalize body" in handle.icechunk_finalize["error"]

    def test_attach_mid_run_resolves_late_shards_and_runs_the_tail(self, status_store):
        # Mid-run state: manifest + two settled shards; the third lands later.
        # The dispatcher pinned ``commit: "leaf"`` into the manifest's config.
        _put_manifest(status_store, "midrun", _WORDS, config=_pinned_config("leaf"))
        body = {"total_obs": 7, "duration_s": 1.0, "stats": {"schema_version": 1}}
        _put_status(status_store, _WORDS[0], body=dict(body))
        _put_status(status_store, _WORDS[1], body=dict(body))

        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "midrun", lambda_client=stub)
        assert handle.futures[_WORDS[0]].result(timeout=10)["body"]["total_obs"] == 7
        assert not handle.futures[_WORDS[2]].done()
        _put_status(status_store, _WORDS[2], body=dict(body))  # the fleet finishes
        handle.results()
        handle.wait(timeout=10)
        # No tail marker existed, so the SAME worker-invoke tail ran (D8):
        # finalize backstop + coverage + stats — and zero cell re-dispatches.
        modes = [m for m in stub.modes() if m]
        assert "finalize" in modes and "stats" in modes
        assert modes[-1] == "icechunk_finalize"  # the tail's last invoke, off the knob
        assert stub.cell_events() == []
        # ... and the stats leg recorded the marker: a second attach skips the
        # tail and fires only the idempotent finalize.
        again = EventStubLambdaClient(status_store)
        again_handle = Run.attach(_STORE, "midrun", lambda_client=again)
        again_handle.results()
        again_handle.wait(timeout=10)
        assert again.modes() == ["icechunk_finalize"]

    def test_attach_on_a_slim_manifest_takes_the_shards_from_the_statuses(
        self, catalog, status_store, monkeypatch
    ):
        # A large hive run's manifest has no shard list (issue #588). Through
        # the real halves: the dispatcher sends the slim block, the stub
        # worker writes it, and attach rebuilds the run from its config with
        # the shard set read off the status objects.
        from test_client import _force_dispatch_manifest

        _force_dispatch_manifest(monkeypatch, "slim")
        live = EventStubLambdaClient(status_store)
        handle = _run(catalog, client=live).dispatch(transport="event")
        handle.results()
        assert handle.dispatch_manifest == "slim"
        run_id = live.cell_events()[0][2]["run_id"]
        manifest = ct.read_dispatch_manifest("ignored", {})
        assert manifest["shards"] is None and manifest["shards_omitted"] == 3
        assert manifest["config"]["output"]["icechunk"]["commit"] == "leaf"

        fresh = EventStubLambdaClient(status_store)
        attached = Run.attach(_STORE, run_id, lambda_client=fresh)
        assert attached.dispatch_manifest == "slim" and attached.unreported_shards == 0
        results = attached.results()
        assert set(results) == set(_WORDS)
        assert all(r["body"]["total_obs"] == 7 for r in results.values())
        attached.wait(timeout=10)
        # Every shard is accounted for, so this is the ordinary recorded-tail
        # attach: nothing re-fires but the idempotent finalize.
        assert fresh.modes() == ["icechunk_finalize"]
        assert fresh.events[0][2]["config"] == manifest["config"]

    def test_attach_on_a_slim_manifest_mid_run_runs_no_tail(self, status_store, caplog):
        # Two of three shards have reported: the handle covers those, says
        # one is unaccounted for, and runs NO tail — its run record and
        # marker would stand for the whole run with that shard missing.
        _put_manifest(status_store, "slimrun", _WORDS, config=_pinned_config("leaf"), slim=True)
        body = {"total_obs": 7, "duration_s": 1.0, "stats": {"schema_version": 1}}
        _put_status(status_store, _WORDS[0], body=dict(body))
        _put_status(status_store, _WORDS[1], status="failed", error="boom", status_code=500)

        stub = EventStubLambdaClient(status_store)
        with caplog.at_level("WARNING"):
            handle = Run.attach(_STORE, "slimrun", lambda_client=stub)
        assert "attach again once the rest have" in caplog.text
        assert handle.dispatch_manifest == "slim" and handle.unreported_shards == 1
        assert set(handle.futures) == {_WORDS[0], _WORDS[1]}
        results = handle.results(return_exceptions=True)
        assert results[_WORDS[0]]["body"]["total_obs"] == 7
        assert isinstance(results[_WORDS[1]], _shard_error())
        handle.wait(timeout=10)
        assert stub.events == [] and handle.icechunk_finalize is None
        assert ct.tail_recorded("ignored", {}) is False

        # The fleet finishes: the next attach names all three and runs the
        # same worker-invoke tail a full manifest's would.
        _put_status(status_store, _WORDS[2], body=dict(body))
        again = EventStubLambdaClient(status_store)
        done = Run.attach(_STORE, "slimrun", lambda_client=again)
        assert done.unreported_shards == 0 and set(done.futures) == set(_WORDS)
        done.results(return_exceptions=True)
        done.wait(timeout=10)
        modes = [m for m in again.modes() if m]
        assert "finalize" in modes and "stats" in modes
        assert modes[-1] == "icechunk_finalize" and again.cell_events() == []

    def test_attach_on_a_slim_manifest_past_the_deadline_says_the_tail_is_lost(
        self, status_store, caplog
    ):
        # A shard with no status by the drop deadline (killed at the timeout,
        # a lost invoke) never reports, and a slim manifest cannot name it:
        # still no tail, and the warning says attach cannot recover it.
        _put_manifest(
            status_store, "slimold", _WORDS, dispatched_at="2020-01-01T00:00:00+00:00", slim=True
        )
        for word in _WORDS[:2]:
            _put_status(status_store, word, body={"total_obs": 7})
        stub = EventStubLambdaClient(status_store)
        with caplog.at_level("WARNING"):
            handle = Run.attach(_STORE, "slimold", lambda_client=stub)
        assert handle.unreported_shards == 1 and set(handle.futures) == set(_WORDS[:2])
        assert "the other 1 never reported and attach cannot run this run's tail" in caplog.text
        assert "attach again once the rest have" not in caplog.text
        handle.results()
        handle.wait(timeout=10)
        assert stub.events == [] and ct.tail_recorded("ignored", {}) is False

    def test_attach_on_a_slim_manifest_before_any_status_refuses(self, status_store):
        _put_manifest(status_store, "early", _WORDS, slim=True)
        with pytest.raises(ValueError, match="is slim .its 3 shards are not listed"):
            Run.attach(_STORE, "early", lambda_client=EventStubLambdaClient(status_store))

    def test_a_full_manifest_attach_is_unchanged(self, status_store):
        # A full manifest never reads the status listing for its shard set: a
        # status-less shard is still registered (and resolves at the deadline).
        _put_manifest(status_store, "fullrun", _WORDS, config=_pinned_config("leaf"))
        _put_status(status_store, _WORDS[0], body={"total_obs": 7})
        handle = Run.attach(_STORE, "fullrun", lambda_client=EventStubLambdaClient(status_store))
        assert handle.dispatch_manifest == "full" and handle.unreported_shards == 0
        assert set(handle.futures) == set(_WORDS)
        assert not handle.futures[_WORDS[2]].done()
        for word in _WORDS[1:]:  # let the run finish so the poller shuts down
            _put_status(status_store, word, body={"total_obs": 7})
        handle.results()
        handle.wait(timeout=10)

    def test_reported_shards_names_only_shard_status_objects(self, status_store):
        _put_manifest(status_store, "names", _WORDS, slim=True)
        obstore.put(status_store, ct.TAIL_NAME, b"{}")
        obstore.put(status_store, ct.shard_status_key(7, window="2019-01"), b"{}")
        assert ct.reported_shards("ignored", {}) == []
        _put_status(status_store, _WORDS[0])
        _put_status(status_store, _WORDS[2])
        assert ct.reported_shards("ignored", {}) == sorted([_WORDS[0], _WORDS[2]])

    def test_attach_never_finalizes_a_ladder_run(self, status_store):
        # Either Lambda dispatcher under ``sweep: "stages"`` (``_run_lambda``,
        # ``Run.dispatch`` — issue #588) pins ``commit: "ladder"``: its last
        # commits land in the staged sweep chained AFTER the tail marker, so
        # the finalize is that dispatcher's alone — attach fires none on
        # either path (mid-run tail, then recorded tail), and its mid-run
        # tail chains no staged sweep of its own (observe-only): the rollup
        # trigger fires for the leaves, no stage invoke does.
        from zagg.telemetry import build_record

        _put_manifest(
            status_store, "ladder", _WORDS, config=_pinned_config("ladder", sweep="stages")
        )
        for word in _WORDS:
            body = {"total_obs": 7, "duration_s": 1.0}
            body["stats"] = build_record(shard_key=int(word), metadata=dict(body))
            _put_status(status_store, word, body=body)
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "ladder", lambda_client=stub)
        handle.results()
        handle.wait(timeout=10)
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "icechunk_finalize" not in modes
        sweeps = [e for _, _, e in stub.events if e.get("mode") == "sweep"]
        assert len(sweeps) == 1 and "stage" not in sweeps[0]  # the rollup, no stage
        assert handle.stage_sweep is None
        assert "staged sweep" in handle.icechunk_finalize["skipped"]
        again = EventStubLambdaClient(status_store)
        again_handle = Run.attach(_STORE, "ladder", lambda_client=again)
        again_handle.results()
        again_handle.wait(timeout=10)
        assert again.events == []
        assert "staged sweep" in again_handle.icechunk_finalize["skipped"]

    def test_attach_never_finalizes_a_leaf_pinned_staged_run(self, status_store):
        # An explicit ``commit: "leaf"`` under ``sweep: "stages"``: the
        # dispatcher still chains the staged sweep, whose nodes commit
        # overview refs under ``"leaf"`` too — so the gate is on the sweep,
        # not the mode, and attach fires no finalize (review, PR #591).
        from zagg.telemetry import build_record

        _put_manifest(
            status_store, "leafstg", _WORDS, config=_pinned_config("leaf", sweep="stages")
        )
        for word in _WORDS:
            body = {"total_obs": 7, "duration_s": 1.0}
            body["stats"] = build_record(shard_key=int(word), metadata=dict(body))
            _put_status(status_store, word, body=body)
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "leafstg", lambda_client=stub)
        handle.results()
        handle.wait(timeout=10)
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "icechunk_finalize" not in modes
        assert "staged sweep" in handle.icechunk_finalize["skipped"]

    def test_attach_never_finalizes_a_run_whose_knob_is_off(self, status_store):
        # ``output.icechunk: false`` in the dispatched config: no repo was
        # stood up, so a reattached handle fires no finalize on either path.
        from dataclasses import asdict

        cfg = default_config("atl06")
        cfg.output["icechunk"] = False
        _put_manifest(status_store, "knoboff", _WORDS, config=asdict(cfg))
        body = {"total_obs": 7, "duration_s": 1.0, "stats": {"schema_version": 1}}
        for word in _WORDS:
            _put_status(status_store, word, body=dict(body))
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "knoboff", lambda_client=stub)
        handle.results()
        handle.wait(timeout=10)
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "icechunk_finalize" not in modes
        assert handle.icechunk_finalize is None
        again = EventStubLambdaClient(status_store)
        again_handle = Run.attach(_STORE, "knoboff", lambda_client=again)
        again_handle.results()
        again_handle.wait(timeout=10)
        assert again.events == []

    def test_attach_failed_status_raises_without_redispatch(self, status_store):
        _put_manifest(status_store, "failedrun", _WORDS)
        body = {"total_obs": 7, "duration_s": 1.0}
        _put_status(status_store, _WORDS[0], body=dict(body))
        _put_status(status_store, _WORDS[1], body=dict(body))
        _put_status(status_store, _WORDS[2], status="failed", error="boom", status_code=500)
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "failedrun", lambda_client=stub)
        with pytest.raises(_shard_error(), match="boom"):
            handle.futures[_WORDS[2]].result(timeout=10)
        handle.results(return_exceptions=True)
        assert handle.status() == {"pending": 0, "ok": 2, "failed": 1}
        # Observe-only: attach holds no granule records, so no re-dispatch.
        assert stub.cell_events() == []

    def test_attach_classifies_stale_missing_shard_failed_unknown(self, status_store):
        # The drop deadline anchors at the manifest's dispatched_at: attaching
        # LONG after the fleet finished classifies a status-less shard
        # immediately instead of waiting a fresh window.
        _put_manifest(status_store, "stale", _WORDS, dispatched_at="2020-01-01T00:00:00+00:00")
        body = {"total_obs": 7, "duration_s": 1.0}
        _put_status(status_store, _WORDS[0], body=dict(body))
        _put_status(status_store, _WORDS[1], body=dict(body))
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "stale", lambda_client=stub)
        with pytest.raises(_shard_error(), match="failed-unknown") as excinfo:
            handle.futures[_WORDS[2]].result(timeout=10)
        assert excinfo.value.payload["outcome"] == "failed-unknown"

    def test_attach_reruns_the_tail_after_a_failed_finalize(
        self, catalog, status_store, monkeypatch
    ):
        # Reattaching after a failure is the headline Run.attach use case, and a
        # failed finalize is the case that needs another pass. The stats event
        # carries #335's finalize_error, so no tail marker is written and the
        # reattached handle re-fires the idempotent backstop (review, PR #343).
        from zagg import runner

        monkeypatch.setattr(runner, "_FINALIZE_BACKOFF_S", 0)
        live = EventStubLambdaClient(status_store, fail_modes={"finalize"})
        with pytest.warns(RuntimeWarning, match="finalize"):
            handle = _run(catalog, client=live).dispatch(transport="event")
            with pytest.raises(RuntimeError, match="finalize down"):
                handle.results()  # joins the tail, then re-raises (issue #335)
        run_id = live.cell_events()[0][2]["run_id"]
        (stats,) = [e for _, _, e in live.events if e.get("mode") == "stats"]
        assert stats["finalize_error"] == "finalize down"
        assert not ct.tail_recorded("ignored-by-the-fixture", {})

        fresh = EventStubLambdaClient(status_store)
        Run.attach(_STORE, run_id, lambda_client=fresh).results()
        assert "finalize" in [m for m in fresh.modes() if m]

    def test_attach_missing_manifest_raises(self, status_store):
        with pytest.raises(ValueError, match="no dispatch manifest"):
            Run.attach(_STORE, "nosuchrun", lambda_client=EventStubLambdaClient(status_store))

    def test_attach_windowed_config_refused(self, status_store):
        from dataclasses import asdict

        cfg = default_config("atl06")
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "time_field": "h_li",  # a declared column, so validate_config passes
            "epoch": "2018-01-01T00:00:00Z",
            "windows": [
                {"label": "w1", "start": "2020-01-01T00:00:00Z", "end": "2021-01-01T00:00:00Z"}
            ],
        }
        _put_manifest(status_store, "windowed", _WORDS, config=asdict(cfg))
        with pytest.raises(NotImplementedError, match="windowed"):
            Run.attach(_STORE, "windowed", lambda_client=EventStubLambdaClient(status_store))


# -- D8 audit (issue #327 phase 7) -------------------------------------------------


class TestD8Audit:
    def test_client_event_run_makes_no_store_writes(self, catalog, status_store, monkeypatch):
        # Dispatcher-never-writes: every PUT observed during a full v2 client
        # run must be attributable to the (stub) WORKER — the shard statuses,
        # the dispatch manifest, and the tail marker, each riding a worker
        # invoke. The client/poller side only LISTs and GETs (reads).
        puts: list[str] = []
        real_put = obstore.put

        def counting_put(store, key, data, *a, **k):
            puts.append(str(key))
            return real_put(store, key, data, *a, **k)

        monkeypatch.setattr(obstore, "put", counting_put)
        stub = EventStubLambdaClient(status_store)
        handle = _run(catalog, client=stub).dispatch(transport="event")
        handle.results()
        handle.wait(timeout=10)
        expected = [ct.MANIFEST_NAME, ct.TAIL_NAME] + [ct.shard_status_key(w) for w in _WORDS]
        assert sorted(puts) == sorted(expected)

    def test_client_event_run_with_a_staged_sweep_makes_no_store_writes(
        self, catalog, status_store, monkeypatch
    ):
        # Issue #588: the tail's staged sweep is the runner's invoke-and-poll
        # seam — the dispatcher LISTs the stage records and PUTs nothing.
        # Driven through the REAL fleet path with the barriers collapsed: no
        # stub worker writes a stage record, so every barrier expires, the
        # finisher is fired and expires too, and the finalize is withheld
        # (the completed-sweep gate) with the run left untagged.
        import functools

        import zagg.store as store_mod
        from zagg import runner

        real = runner._invoke_lambda_stage_sweep
        monkeypatch.setattr(
            runner,
            "_invoke_lambda_stage_sweep",
            functools.partial(real, barrier_timeout_s=0.05, total_barrier_budget_s=0.2),
        )
        # The fleet's record LIST opens the sweep's own status prefix through
        # the store factory; route it at the in-memory status store.
        monkeypatch.setattr(store_mod, "open_object_store", lambda path, *a, **k: status_store)
        # The dispatcher's ONE store read (issue #635): it declines a store
        # whose manifest declares no `/2` ladder before firing anything, so
        # the audited run needs a manifest to pass that gate — and the audit
        # is about PUTs, which this one is not. `hive` binds the store factory
        # at import, so route it THERE too, or `read_manifest` reaches for the
        # real `s3://test-bucket` and the gate's verdict depends on what the
        # network answers: a 404 on a CI runner declines the run, no network
        # at all fails open. Written before the PUT counter is installed.
        monkeypatch.setattr(hive, "open_object_store", lambda path, *a, **k: status_store)
        obstore.put(
            status_store,
            hive.MANIFEST_NAME,
            json.dumps(
                {
                    "shard_order": _D8_SHARD_ORDER,
                    "pyramid": {
                        "spec": PYRAMID_SPEC_V2,
                        "overviews": expand_overviews([9], parent_order=_D8_SHARD_ORDER),
                    },
                }
            ).encode(),
        )
        puts: list[str] = []
        real_put = obstore.put

        def counting_put(store, key, data, *a, **k):
            puts.append(str(key))
            return real_put(store, key, data, *a, **k)

        monkeypatch.setattr(obstore, "put", counting_put)
        cfg = default_config("atl06")
        cfg.output["sweep"] = "stages"
        cfg.output["grid"] = {**cfg.output["grid"], "chunk_inner": 9}
        stub = EventStubLambdaClient(status_store, record=True)
        handle = _run(catalog, client=stub, config=cfg).dispatch(transport="event")
        handle.results()
        handle.wait(timeout=30)
        assert handle._tail_error is None
        expected = [ct.MANIFEST_NAME, ct.TAIL_NAME] + [ct.shard_status_key(w) for w in _WORDS]
        assert sorted(puts) == sorted(expected)
        stage = [(t, e) for _, t, e in stub.events if e.get("mode") == "sweep" and e.get("stage")]
        assert stage and all(t == "Event" for t, _ in stage)  # fired, then polled
        assert {e["stage"]["role"] for _, e in stage} == {"stage", "finisher"}
        assert handle.stage_sweep["barrier_timed_out"] is True
        assert handle.icechunk_finalize == {"skipped": "staged sweep barrier timed out"}
        assert "icechunk_finalize" not in stub.modes()

    def test_attach_makes_no_store_writes(self, status_store, monkeypatch):
        # The audit above only covers Run.dispatch, but Run.attach is the one
        # client path that fires a tail from a FRESH process with no dispatch
        # state — the path most likely to grow a client-side write later
        # (review finding, PR #343). Same PUT counter, mid-run state so the
        # whole tail actually runs.
        _put_manifest(status_store, "d8attach", _WORDS)
        body = {"total_obs": 7, "duration_s": 1.0, "stats": {"schema_version": 1}}
        for word in _WORDS:
            _put_status(status_store, word, body=dict(body))
        puts: list[str] = []
        real_put = obstore.put

        def counting_put(store, key, data, *a, **k):
            puts.append(str(key))
            return real_put(store, key, data, *a, **k)

        monkeypatch.setattr(obstore, "put", counting_put)
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "d8attach", lambda_client=stub)
        handle.results()
        handle.wait(timeout=10)
        # The tail ran (finalize + rollups) and the ONLY PUT is the worker's
        # tail marker, riding the stats invoke.
        assert "finalize" in [m for m in stub.modes() if m]
        assert puts == [ct.TAIL_NAME]


def test_build_shard_status_carries_the_store_error_class_off_the_body():
    # The worker half (issue #651): the class rides the body into the status
    # object's own key -- a ``store-fatal`` unit is ``failed``, and a clean
    # body carries ``None`` so the key is always present.
    event = {"run_id": "r", "store_path": "s3://b/x.zarr", "shard_key": 5}
    body = {"error": "window 2019: refused", "store_error_class": "store-fatal"}
    _key, obj = ct.build_shard_status(event, _envelope(body, status=500))
    assert (obj["status"], obj["store_error_class"], obj["body"]["store_error_class"]) == (
        "failed",
        "store-fatal",
        "store-fatal",
    )
    _key, obj = ct.build_shard_status(event, _envelope({"total_obs": 1}))
    assert (obj["status"], obj["store_error_class"]) == ("ok", None)


def test_rows_from_status_reads_both_object_shapes(tmp_path):
    # The run-record pointer transport (issue #313) now also serves event runs
    # (issue #327): the worker assembles rows from the #151 envelope shape
    # (body as a JSON string) AND the v2 status-object shape (body as a dict);
    # the prefix's manifest/tail objects parse fine and contribute no rows.
    from zagg.telemetry import build_record, rows_from_status

    prefix = tmp_path / "out.zarr.status" / "run-r1"
    prefix.mkdir(parents=True)
    rec1 = build_record(shard_key=1, metadata={"total_obs": 5, "duration_s": 1.0})
    (prefix / "11.json").write_text(
        json.dumps({"statusCode": 200, "body": json.dumps({"stats": rec1})})
    )
    rec2 = build_record(shard_key=2, metadata={"total_obs": 3, "duration_s": 2.0})
    (prefix / "shard-2.json").write_text(
        json.dumps(
            {"schema_version": 1, "status": "ok", "attempt_id": "a", "body": {"stats": rec2}}
        )
    )
    (prefix / "manifest.json").write_text(json.dumps({"schema_version": 1, "run_id": "r1"}))
    (prefix / "tail.json").write_text(json.dumps({"status": "tail_done"}))
    rows = rows_from_status(str(prefix))
    assert sorted(r["shard_key"] for r in rows) == [1, 2]


# -- keys / prefix ---------------------------------------------------------------


class TestKeys:
    def test_run_status_prefix_is_a_store_sibling(self):
        assert ct.run_status_prefix("s3://b/out.zarr", "abc") == "s3://b/out.zarr.status/run-abc"
        assert ct.run_status_prefix("s3://b/out.zarr/", "abc").endswith("out.zarr.status/run-abc")

    def test_shard_status_key_decimal_and_window(self):
        assert ct.shard_status_key(12345) == "shard-12345.json"
        assert ct.shard_status_key(12345, window="2020") == "shard-12345_2020.json"
        with pytest.raises(ValueError, match="grammar"):
            ct.shard_status_key(1, window="../escape")

    def test_drop_timeout_formula(self):
        # function timeout + 60 s max event age + margin (issue #327 (a')).
        assert ct.drop_timeout_s(900) == 900 + 60 + 90


# -- circuit breaker through the poller (issue #652) -----------------------------------


def _guard(workers=None, **knobs):
    from zagg.dispatch_guard import DispatchGuard, GuardConfig

    cfg = GuardConfig.from_dict({"breaker": knobs}) if knobs else None
    return DispatchGuard(cfg, workers=workers)


def _ok_body():
    return {"total_obs": 7, "duration_total_s": 600.0}


class TestDispatchGuardSeam:
    """``StatusPoller`` paces by its ``guard`` (issue #652): the full worker
    width, a trip refuses every queued shard (``not-dispatched``, through the
    caller's failure policy) and re-fires nothing, and in-flight invokes still
    resolve from their status objects. Tick-driven with a fake clock, no threads."""

    @staticmethod
    def _register(poller, keys, fired):
        return {k: poller.register(k, str(k), dispatch=lambda k=k: fired.append(k)) for k in keys}

    def test_the_guard_paces_at_the_full_worker_width(self):
        # No ramp (2026-10-14 ruling): the first wave is the run's whole width,
        # and a freed slot refills in the same tick, never wider.
        store = MemoryStore()
        guard = _guard(workers=2)
        fired: list[int] = []
        poller = ct.StatusPoller(lambda: store, guard=guard, drop_timeout_s=1050.0)
        self._register(poller, range(1, 6), fired)
        poller._tick()
        assert fired == [1, 2]
        for k in (1, 2):
            _put_status(store, k, body=_ok_body())
        poller._tick()
        assert fired == [1, 2, 3, 4] and guard.width == 2 and guard.tripped is None

    def test_a_timeout_storm_trips_the_breaker_and_refuses_the_queue(self):
        store = MemoryStore()
        guard = _guard(workers=2, window=2, timeout_ratio=0.5)
        now = {"t": 1000.0}
        fired: list[int] = []
        failed: list[tuple] = []

        def on_failed(entry, result):
            failed.append((entry.shard_key, result))
            entry.future.set_result(result)

        poller = ct.StatusPoller(
            lambda: store,
            guard=guard,
            drop_timeout_s=100.0,
            max_retries=3,
            clock=lambda: now["t"],
            on_failed=on_failed,
        )
        futures = self._register(poller, range(1, 6), fired)
        poller._tick()
        assert fired == [1, 2]
        now["t"] = 1200.0  # both past the drop deadline: 2 of the last 2 timed out
        poller._tick()
        assert guard.tripped["rule"] == "timeout-ratio"
        # Nothing else is invoked: shard 1 dropped BEFORE the trip and was
        # re-queued (class (b)), so its re-fire is refused; shard 2's drop IS
        # the trip, so it resolves failed-unknown with no re-fire; the queue is
        # refused. All through the caller's failure policy.
        assert fired == [1, 2]
        assert all(f.done() for f in futures.values())
        outcomes = {k: (f.result()["outcome"], f.result()["retries"]) for k, f in futures.items()}
        assert outcomes == {
            1: ("not-dispatched", 0),  # one attempt made, the re-fire refused
            2: ("failed-unknown", 0),
            3: ("not-dispatched", -1),  # never attempted, as after a shutdown
            4: ("not-dispatched", -1),
            5: ("not-dispatched", -1),
        }
        assert sorted(k for k, _ in failed) == [1, 2, 3, 4, 5]
        refusal = futures[3].result()
        assert "timeout-ratio" in refusal["error"] and refusal["status_code"] is None

    def test_in_flight_invokes_finish_after_a_trip(self):
        store = MemoryStore()
        guard = _guard(workers=2, window=2, timeout_ratio=0.4)
        now = {"t": 1000.0}
        fired: list[int] = []
        poller = ct.StatusPoller(
            lambda: store, guard=guard, drop_timeout_s=95.0, clock=lambda: now["t"]
        )
        futures = self._register(poller, range(1, 6), fired)
        poller._tick()  # 1, 2 fire at t=1000
        _put_status(store, 1, body=_ok_body())
        now["t"] = 1010.0
        poller._tick()  # 1 lands; 3 fires at t=1010
        assert fired == [1, 2, 3]
        now["t"] = 1100.0  # 2's deadline (1095) passed, 3's (1105) has not
        poller._tick()
        assert guard.tripped["rule"] == "timeout-ratio"  # 1 of the last 2
        assert fired == [1, 2, 3]
        assert futures[2].result()["outcome"] == "failed-unknown"
        assert futures[4].result()["outcome"] == futures[5].result()["outcome"] == "not-dispatched"
        assert not futures[3].done()  # in flight: it cannot be recalled
        _put_status(store, 3, body=_ok_body())
        poller._tick()
        assert futures[3].result()["status_code"] == 200  # ... and its result still lands

    def test_windows_of_one_shard_each_count_toward_the_timeout_ratio(self):
        guard = _guard(workers=4, window=4, timeout_ratio=0.5)
        now = {"t": 1000.0}
        fired: list[str] = []
        poller = ct.StatusPoller(
            lambda: MemoryStore(), guard=guard, drop_timeout_s=100.0, clock=lambda: now["t"]
        )
        for w in ("2018", "2019", "2020", "2021"):
            poller.register(7, f"7/{w}", window=w, dispatch=lambda w=w: fired.append(w))
        poller._tick()
        assert len(fired) == 4
        now["t"] = 1200.0  # all four windows of shard 7 drop: 4 of the last 4
        poller._tick()
        assert len(guard._records) == 4 and guard.tripped["rule"] == "timeout-ratio"

    def test_store_fatal_statuses_trip_and_a_late_registration_is_refused(self):
        store = MemoryStore()
        guard = _guard(store_fatal=2, store_fatal_first=5)
        fired: list[int] = []
        poller = ct.StatusPoller(lambda: store, guard=guard, drop_timeout_s=1050.0)
        futures = self._register(poller, range(1, 5), fired)
        poller._tick()
        assert fired == [1, 2, 3, 4]  # all inside the unbounded first wave
        for k in (1, 2):
            _put_status(
                store,
                k,
                status="failed",
                status_code=500,
                error="window 2018 failed: HTTP status client error (403 Forbidden)",
                body={"error": "window 2018 failed: HTTP status client error (403 Forbidden)"},
            )
        poller._tick()
        assert guard.tripped["rule"] == "store-fatal" and guard.tripped["at_result"] == 2
        assert futures[1].result()["status_code"] == 500  # the failed status is terminal, as before
        late = poller.register(9, "9", dispatch=lambda: fired.append(9))
        poller._tick()
        assert late.result()["outcome"] == "not-dispatched" and 9 not in fired
        assert not futures[3].done() and not futures[4].done()  # left to finish

    def test_guard_and_max_in_flight_are_exclusive(self):
        with pytest.raises(ValueError, match=r"DispatchGuard\(workers"):
            ct.StatusPoller(
                lambda: MemoryStore(), guard=_guard(), drop_timeout_s=1.0, max_in_flight=3
            )

    def test_a_fixed_window_records_nothing(self):
        # No guard: the pre-#652 pacing (max_in_flight), never tripping.
        store = MemoryStore()
        fired: list[int] = []
        poller = ct.StatusPoller(lambda: store, drop_timeout_s=1050.0, max_in_flight=1)
        self._register(poller, (1, 2), fired)
        poller._tick()
        assert fired == [1]
        _put_status(store, 1, body=_ok_body())
        poller._tick()  # the freed slot refills in the same tick
        assert fired == [1, 2] and isinstance(poller._guard, ct.FixedWindow)
        assert poller._guard.tripped is None and poller._guard.breaker is None

    @staticmethod
    def _tripping_poller(now, **kwargs):
        # One-result window: the first drop trips timeout-ratio.
        guard = _guard(workers=4, window=1, timeout_ratio=0.5)
        store = kwargs.pop("store", None) or MemoryStore()
        poller = ct.StatusPoller(
            lambda: store,
            guard=guard,
            max_retries=3,
            clock=lambda: now["t"],
            **kwargs,
        )
        return poller, guard

    def test_an_invoke_fault_in_its_backoff_is_refused_after_a_trip(self):
        now = {"t": 1000.0}
        poller, guard = self._tripping_poller(now, drop_timeout_s=1.0)
        fired: list[int] = []

        def faulty():
            raise RuntimeError("TooManyRequestsException")

        poller.register(1, "1", dispatch=lambda: fired.append(1))
        faulted = poller.register(2, "2", dispatch=faulty)
        poller._tick()  # 1 fires; 2's invoke raises: class (c), backoff to t=1002
        now["t"] = 1001.5  # 1 drops (trip) while 2 is still inside its backoff
        poller._tick()
        assert guard.tripped["rule"] == "timeout-ratio" and not faulted.done()
        now["t"] = 1003.0
        poller._tick()  # past the backoff: refused, not re-invoked
        result = faulted.result()
        assert (result["outcome"], result["retries"]) == ("not-dispatched", 0)
        assert "circuit breaker tripped" in result["error"]
        assert "Event invoke failed" not in result["error"]

    def test_observe_only_entries_are_never_refused_after_a_trip(self):
        store = MemoryStore()
        now = {"t": 1000.0}
        poller, guard = self._tripping_poller(now, drop_timeout_s=100.0, store=store)
        fired: list[int] = []
        poller.register(1, "1", dispatch=lambda: fired.append(1))
        landed = poller.register(2, "2", dispatched_at=1100.0)  # attach-shaped
        lost = poller.register(3, "3", dispatched_at=1100.0)
        poller._tick()  # observe-only entries hold slots too, but are never fired
        assert fired == [1]
        now["t"] = 1150.0  # 1 drops: the trip
        poller._tick()
        assert guard.tripped is not None and not landed.done() and not lost.done()
        _put_status(store, 2, body=_ok_body())
        poller._tick()
        assert landed.result()["status_code"] == 200  # from its status object
        assert not lost.done()
        now["t"] = 1250.0
        poller._tick()
        assert lost.result()["outcome"] == "failed-unknown"  # at its drop deadline


# -- Future plumbing -------------------------------------------------------------


def test_register_returns_unresolved_future():
    poller = ct.StatusPoller(lambda: MemoryStore(), drop_timeout_s=10.0)
    fut = poller.register(1, "1", dispatch=lambda: None)
    assert isinstance(fut, Future)
    assert not fut.done()


def test_register_after_shutdown_resolves_instead_of_hanging():
    # An unbounded block is the worst failure mode for a blocking API: agg's
    # `fut.result()` has no timeout, so a Future nothing can resolve would hang
    # a pool thread — and its executor.shutdown() — forever (review, PR #343).
    store = MemoryStore()
    poller = ct.StatusPoller(lambda: store, drop_timeout_s=10.0)
    poller.start()
    poller.shutdown(wait=True)
    fut = poller.register(1, "1", dispatch=lambda: None)
    result = fut.result(timeout=1)
    assert result["outcome"] == "not-dispatched"
    assert "after the status poller shut down" in result["error"]
    # The refused entry is not tracked, and a late status object cannot revive it.
    assert poller._entries == {}


def test_register_after_shutdown_honors_the_failure_policy():
    # The client's policy (ShardError) must apply to the refusal too, so a
    # post-shutdown registration surfaces as a failed shard, not a silent ok.
    failures: list = []
    poller = ct.StatusPoller(
        lambda: MemoryStore(),
        drop_timeout_s=10.0,
        on_failed=lambda e, r: (
            failures.append(r),
            e.future.set_exception(RuntimeError(r["error"])),
        ),
    )
    poller.shutdown()
    fut = poller.register(1, "1", dispatch=lambda: None)
    with pytest.raises(RuntimeError, match="after the status poller shut down"):
        fut.result(timeout=1)
    assert len(failures) == 1
