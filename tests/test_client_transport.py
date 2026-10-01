"""Tests for the v2 Event transport (issue #327): status objects + poller.

No AWS, no network: Lambda traffic goes through a stub client whose Event
"workers" write status objects into an ``obstore`` MemoryStore — through the
REAL ``build_shard_status`` (the worker half), so the two halves of the
channel cannot drift — and the poller reads them back through the same
obstore functions the production path uses.
"""

import json
import threading
import time
from concurrent.futures import Future

import obstore
import pytest
from obstore.store import MemoryStore

import zagg.client_transport as ct
from zagg.client import Run
from zagg.config import default_config


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


def _pinned_config(commit):
    """The dispatched config as a manifest records it: ``output.icechunk.commit`` pinned."""
    from dataclasses import asdict

    cfg = default_config("atl06")
    cfg.output["icechunk"] = {"commit": commit}
    return asdict(cfg)


def _put_manifest(store, run_id, shards, dispatched_at=None, config=None):
    """Hand-craft one dispatch manifest (what the worker writes off setup)."""
    from dataclasses import asdict
    from datetime import datetime, timezone

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "shards": [str(int(w)) for w in shards],
        "semantic_hash": "ab" * 32,
        "dispatched_at": dispatched_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": {"short_name": "ATL06", "version": "006"},
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
    def _agg(catalog_file, monkeypatch, stub, invocation, config=None, **agg_kwargs):
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
                3,
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
            max_workers=3,
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

    def test_attach_never_finalizes_a_ladder_run(self, status_store):
        # ``runner._run_lambda`` under ``sweep: "stages"`` pins ``commit:
        # "ladder"``: its last commits land in the staged sweep chained AFTER
        # the tail marker, so the finalize is that dispatcher's alone — attach
        # fires none on either path (mid-run tail, then recorded tail).
        _put_manifest(status_store, "ladder", _WORDS, config=_pinned_config("ladder"))
        body = {"total_obs": 7, "duration_s": 1.0, "stats": {"schema_version": 1}}
        for word in _WORDS:
            _put_status(status_store, word, body=dict(body))
        stub = EventStubLambdaClient(status_store)
        handle = Run.attach(_STORE, "ladder", lambda_client=stub)
        handle.results()
        handle.wait(timeout=10)
        modes = [m for m in stub.modes() if m]
        assert "stats" in modes and "icechunk_finalize" not in modes
        assert "staged sweep" in handle.icechunk_finalize["skipped"]
        again = EventStubLambdaClient(status_store)
        again_handle = Run.attach(_STORE, "ladder", lambda_client=again)
        again_handle.results()
        again_handle.wait(timeout=10)
        assert again.events == []
        assert "staged sweep" in again_handle.icechunk_finalize["skipped"]

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
