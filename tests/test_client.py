"""Tests for the zagg.client facade (issue #326): Run / RunHandle / ShardError.

All Lambda traffic goes through a stub client object (no moto, no network):
the stub records every invoke and answers per-mode canned envelopes, so the
tests exercise the real payload construction, future resolution, error
surfacing, and the worker-invoke post-run tail.
"""

import copy
import errno
import importlib
import json
import re
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import pytest

import zagg.client
import zagg.concurrency
from zagg.client import Run, RunHandle, ShardError
from zagg.config import PipelineConfig, default_config

# -- fixtures ----------------------------------------------------------------

# HealpixGrid(parent_order=6, child_order=12, layout="fullsphere").signature();
# packed morton words whose decimal morton labels (the external form, issue
# #199) are -4211324 / -4211323 / -4211322. Mirrors tests/test_runner.py.
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


def _catalog_dict():
    return {
        "metadata": {"short_name": "ATL06", "version": "006"},
        "grid_signature": dict(_ATL06_SIG),
        "shard_keys": list(_WORDS),
        "granules": [[_rec(4), _rec(5), _rec(6)], [_rec(3)], [_rec(1), _rec(2)]],
    }


@pytest.fixture
def catalog():
    return _catalog_dict()


@pytest.fixture
def catalog_file(tmp_path, catalog):
    p = tmp_path / "shardmap.json"
    p.write_text(json.dumps(catalog))
    return str(p)


@pytest.fixture(autouse=True)
def _no_stats_verify(monkeypatch):
    # The post-run tail's run-stats invoke verifies the parquet landed with a
    # read-only poll (issue #313); zero window skips it so the finisher thread
    # never opens a (fake) store. TestTailTiming re-enables a scaled window so
    # the fire -> verify -> re-fire path is exercised somewhere (review
    # finding, PR #333).
    from zagg import runner

    monkeypatch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", 0)


class _Payload:
    def __init__(self, raw: bytes):
        self._raw = raw

    def read(self):
        return self._raw


def _envelope(body: dict, status=200):
    raw = json.dumps({"statusCode": status, "body": json.dumps(body)}).encode()
    return {"Payload": _Payload(raw), "FunctionError": None}


class StubLambdaClient:
    """boto3-Lambda-shaped stub: records invokes, answers canned envelopes."""

    def __init__(self, *, fail=(), benign=(), timeout=(), delays=None, mode_delay=0):
        self.events: list[tuple[str, str, dict]] = []
        self._lock = threading.Lock()
        self._fail = set(fail)
        self._benign = set(benign)
        self._timeout = set(timeout)
        self._delays = delays or {}
        self._mode_delay = mode_delay
        self.gate: threading.Event | None = None  # holds cell invokes open

    def invoke(self, **kwargs):
        event = json.loads(kwargs["Payload"])
        with self._lock:
            self.events.append((kwargs["FunctionName"], kwargs["InvocationType"], event))
        if event.get("mode") is not None:  # ping/setup/finalize/coverage/stats/sweep
            # A non-zero mode_delay stands in for a real tail round trip, so a
            # test that fails to join the finisher observes a truncated tail
            # deterministically rather than racily.
            time.sleep(self._mode_delay)
            return _envelope({"zagg_version": "stub"})
        key = event["shard_key"]
        if self.gate is not None:
            self.gate.wait(5)
        time.sleep(self._delays.get(key, 0))
        if key in self._timeout:
            return {
                "Payload": _Payload(b"Task timed out after 900.00 seconds"),
                "FunctionError": "Unhandled",
            }
        if key in self._fail:
            return _envelope({"error": "boom"}, status=500)
        if key in self._benign:
            return _envelope({"error": "No granules found"})
        return _envelope({"total_obs": 7, "duration_s": 1.5})

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


# -- construction ------------------------------------------------------------


class TestFromConfig:
    def test_shardmap_path_and_dict_are_equivalent(self, catalog, catalog_file):
        stub = StubLambdaClient()
        by_dict = _run(catalog, client=stub)
        by_path = _run(catalog_file, client=stub)
        assert len(by_dict) == len(by_path) == 3
        assert by_dict.store == by_path.store == _STORE

    def test_config_dict_is_loaded_and_validated(self, catalog):
        from dataclasses import asdict

        cfg_dict = asdict(default_config("atl06"))
        run = _run(catalog, client=StubLambdaClient(), config=cfg_dict)
        assert isinstance(run.config, PipelineConfig)

    def test_config_yaml_path(self, tmp_path, catalog):
        from dataclasses import asdict

        import yaml

        p = tmp_path / "cfg.yaml"
        p.write_text(yaml.safe_dump(asdict(default_config("atl06"))))
        run = _run(catalog, client=StubLambdaClient(), config=str(p))
        assert run.function_name == "process-shard-test"

    def test_store_falls_back_to_config(self, catalog):
        cfg = default_config("atl06")
        cfg.output["store"] = "s3://cfg-bucket/cfg.zarr"
        run = Run.from_config(cfg, shardmap=catalog, lambda_client=StubLambdaClient())
        assert run.store == "s3://cfg-bucket/cfg.zarr"

    def test_missing_shardmap_raises(self):
        with pytest.raises(ValueError, match="No shardmap"):
            Run.from_config(default_config("atl06"), store=_STORE)

    def test_missing_store_raises(self, catalog):
        with pytest.raises(ValueError, match="No store path"):
            Run.from_config(default_config("atl06"), shardmap=catalog)

    def test_non_s3_store_raises(self, catalog):
        with pytest.raises(ValueError, match="s3://"):
            Run.from_config(default_config("atl06"), shardmap=catalog, store="./local.zarr")

    def test_grid_signature_mismatch_raises(self, catalog):
        cfg = default_config("atl06")
        cfg.output["grid"]["parent_order"] = 7
        with pytest.raises(ValueError, match="different grid"):
            Run.from_config(cfg, shardmap=catalog, store=_STORE)

    def test_non_phase5_shardmap_dict_raises(self):
        with pytest.raises(ValueError, match="Phase-5"):
            Run.from_config(default_config("atl06"), shardmap={"shard_keys": []}, store=_STORE)

    def test_temporal_config_refused(self):
        cfg = PipelineConfig(pipeline={"type": "temporal"})
        with pytest.raises(NotImplementedError, match="temporal"):
            Run.from_config(cfg, shardmap={}, store=_STORE)

    def test_raster_config_refused(self, catalog):
        cfg = default_config("atl06")
        cfg.data_source["reader"] = "raster"
        with pytest.raises(NotImplementedError, match="raster"):
            Run.from_config(cfg, shardmap=catalog, store=_STORE)

    def test_windowed_config_refused(self, catalog):
        cfg = default_config("atl06")
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00Z",
            "windows": [
                {"label": "w1", "start": "2020-01-01T00:00:00Z", "end": "2021-01-01T00:00:00Z"}
            ],
        }
        with pytest.raises(NotImplementedError, match="windowed"):
            Run.from_config(cfg, shardmap=catalog, store=_STORE)


class TestSubmissionValidation:
    """A mutated ``PipelineConfig`` is cross-validated at the submission seam.

    ``from_config`` used to validate only its dict and path inputs, so the
    observed failure (issue #472) had nothing re-check the ``02_write`` graft:
    ``default_config`` validated the hive base template, the notebook then
    grafted ``atl03_tdigest_located_healpix``'s ``temporal:`` variables onto it
    without ``output.time_source``, and the config error only surfaced one
    Lambda invoke per shard later, in the worker's refusal.
    """

    def _graft(self):
        base = default_config("atl03_tdigest_healpix_hive", validate=False)
        located = default_config("atl03_tdigest_located_healpix", validate=False)
        base.aggregation["variables"] = copy.deepcopy(located.aggregation["variables"])
        return base, located

    def test_grafted_config_refused_before_any_invoke(self, catalog):
        from zagg.time_axis import TOC_NO_CLOCK_ERROR

        cfg, _ = self._graft()
        stub = StubLambdaClient()
        with pytest.raises(ValueError, match=re.escape(TOC_NO_CLOCK_ERROR)):
            Run.from_config(
                cfg,
                shardmap=catalog,
                store=_STORE,
                function_name="process-shard-test",
                lambda_client=stub,
                source_credentials=_CREDS,
            )
        assert stub.events == []  # refused at submission, nothing dispatched

    def test_grafted_config_with_its_clock_constructs(self, catalog):
        # Positive control: grafting the clock block and its column too is the
        # correct form, and it still builds a Run (no false refusal).
        cfg, located = self._graft()
        cfg.output["time_source"] = dict(located.output["time_source"])
        field = cfg.output["time_source"]["field"]
        cfg.data_source["variables"][field] = located.data_source["variables"][field]
        cfg.output["grid"] = dict(_ATL06_SIG)
        run = Run.from_config(
            cfg,
            shardmap=catalog,
            store=_STORE,
            function_name="process-shard-test",
            lambda_client=StubLambdaClient(),
            source_credentials=_CREDS,
        )
        assert len(run) == 3


# -- dispatch fan-out --------------------------------------------------------


class TestDispatch:
    def test_fanout_one_sync_invoke_per_shard(self, catalog):
        stub = StubLambdaClient()
        handle = _run(catalog, client=stub).dispatch()
        assert isinstance(handle, RunHandle)
        assert len(handle) == 3
        assert set(handle.futures) == set(_WORDS)
        results = handle.results()
        assert all(r["body"]["total_obs"] == 7 for r in results.values())
        cells = stub.cell_events()
        assert len(cells) == 3
        # v1 transport: the existing synchronous RequestResponse invoke, no
        # async result channel (issue #326 / ratified on #265).
        assert all(t == "RequestResponse" for _, t, _ in cells)
        assert all("result_url" not in e for _, _, e in cells)
        assert all(n == "process-shard-test" for n, _, _ in cells)

    def test_cell_event_payload_shape(self, catalog):
        from zagg.semantics import semantic_hash

        stub = StubLambdaClient()
        handle = _run(catalog, client=stub).dispatch(shard_keys=[_WORDS[1]])
        handle.results()
        (_, _, event) = stub.cell_events()[0]
        # The complete key set, not a spot check: a key added to or dropped
        # from the cell event has to fail here (review finding, PR #333).
        assert set(event) == {
            "chunk_idx",
            "shard_key",
            "parent_order",
            "child_order",
            "granule_urls",
            "store_path",
            "s3_credentials",
            "config",
            "handoff",
            "run_id",
            "submap",
            # The fleet's leaf identity gate, armed as on _run_local (hive,
            # no overwrite): the RUN config's D19 digest rides with it.
            "skip_if_current",
            "semantic_hash",
        }
        assert event["skip_if_current"] is True
        assert event["semantic_hash"] == semantic_hash(default_config("atl06"))
        assert event["shard_key"] == _WORDS[1]
        assert event["parent_order"] == 6
        assert event["child_order"] == 12
        assert event["granule_urls"] == [_rec(3)["s3"]]
        assert event["store_path"] == _STORE
        assert event["s3_credentials"] == _CREDS
        assert event["config"]["output"]["grid"]["parent_order"] == 6
        assert event["handoff"] == "arrow"
        assert event["run_id"]
        assert event["submap"] == {
            "grid_signature": _ATL06_SIG,
            "metadata": {"short_name": "ATL06", "version": "006"},
            "granules": [_rec(3)],
        }

    def test_overwrite_disarms_the_fleet_gate(self, catalog):
        # The operator's hammer disarms the gate on the facade exactly as on
        # _run_local and _run_lambda (issue #388): no gate keys ride.
        stub = StubLambdaClient()
        _run(catalog, client=stub, overwrite=True).dispatch(shard_keys=[_WORDS[1]]).results()
        (_, _, event) = stub.cell_events()[0]
        assert "skip_if_current" not in event and "semantic_hash" not in event

    def test_icechunk_commit_ships_pinned_per_leaf(self, catalog):
        # The facade chains no staged sweep, so the ref ladder never runs:
        # even under ``sweep: "stages"`` an unset commit must reach the init
        # and every worker as the per-leaf commit, never the ladder whose
        # sidecars nothing would gather (issue #580 review finding).
        cfg = default_config("atl06")
        cfg.output["sweep"] = "stages"
        stub = StubLambdaClient()
        _run(catalog, client=stub, config=cfg).dispatch(shard_keys=[_WORDS[1]]).results()
        events = [e for _, _, e in stub.events if e.get("mode") in (None, "icechunk_init")]
        assert {e.get("mode") for e in events} == {None, "icechunk_init"}
        for event in events:
            assert event["config"]["output"]["icechunk"] == {"commit": "leaf"}
        assert "icechunk" not in cfg.output  # the caller's config is untouched

    def test_pairless_report_stays_out_of_the_cell_submap(self, catalog):
        # The sibling join's exclusion report (issue #425) holds one entry per
        # unpaired granule — unbounded in the catalog size — so it must not
        # ride every cell event: it counts against the 256 KB async cap, whose
        # only remedy is silently dropping the whole submap block, and
        # ``write_leaf_submap`` strips it again on the way out anyway (review
        # finding, PR #432).
        catalog["metadata"]["pairless"] = [{"id": f"g{i}", "missing": "l2a"} for i in range(500)]
        catalog["metadata"]["sibling_asset"] = "l2a"
        stub = StubLambdaClient()
        _run(catalog, client=stub).dispatch(shard_keys=[_WORDS[1]]).results()
        (_, _, event) = stub.cell_events()[0]
        meta = event["submap"]["metadata"]
        assert "pairless" not in meta
        # The small identity fields still ride along.
        assert meta["sibling_asset"] == "l2a"
        assert meta["short_name"] == "ATL06"

    def test_hive_setup_handshake_precedes_cells(self, catalog):
        stub = StubLambdaClient()
        run = _run(catalog, client=stub)
        handle = run.dispatch()
        handle.wait(timeout=10)
        modes = stub.modes()
        first_cell = modes.index(None)
        assert modes[:first_cell] == [
            "ping",
            "setup",
            "icechunk_init",
        ]  # fail-fast ping, then manifest write
        setup_invocations = [(t, e["mode"]) for _, t, e in stub.events if e.get("mode")]
        assert ("RequestResponse", "ping") in setup_invocations
        assert ("Event", "setup") in setup_invocations
        # The companion init blocks the fan-out (issue #580): synchronous.
        assert ("RequestResponse", "icechunk_init") in setup_invocations
        # ... and its record is KEPT and threaded into the tail's run-record
        # write, so this dispatcher populates the run-level icechunk columns
        # the same way runner._run_lambda does — a failed init has to be
        # recorded, not invisible (D9).
        (stats_event,) = [e for _, _, e in stub.events if e.get("mode") == "stats"]
        assert stats_event["icechunk_init"] == run._icechunk_init
        assert run._icechunk_init is not None
        # ... and the run's finalize (issue #582) is the LAST invoke of the
        # tail — synchronous, after every per-leaf commit landed — carrying
        # the init record so a split ratchet is reported back.
        assert modes[-1] == "icechunk_finalize"
        assert ("RequestResponse", "icechunk_finalize") in setup_invocations
        (fin_event,) = [e for _, _, e in stub.events if e.get("mode") == "icechunk_finalize"]
        assert fin_event["icechunk_init"] == run._icechunk_init
        assert fin_event["run_id"] == stats_event["run_id"]
        # ... with the very config dispatch() pinned and sent to the init.
        (init_event,) = [e for _, _, e in stub.events if e.get("mode") == "icechunk_init"]
        assert fin_event["config"] == init_event["config"]
        # The finalize outcome is surfaced on the handle, not write-only: the
        # stub's bare envelope is a fail-open error the caller can read.
        assert "unexpected icechunk_finalize body" in handle.icechunk_finalize["error"]
        # Post-run tail (all worker invokes, D8): finalize backstop + fail-open
        # coverage/stats rollups.
        assert "finalize" in modes
        assert "stats" in modes
        assert "coverage" in modes

    def test_flat_setup_and_no_finalize_by_default(self, catalog):
        cfg = default_config("atl06")
        cfg.output["store_layout"] = "flat"
        stub = StubLambdaClient()
        _run(catalog, client=stub, config=cfg).dispatch().wait(timeout=10)
        modes = stub.modes()
        assert modes[0] == "setup"
        assert stub.events[0][1] == "RequestResponse"  # flat template write is sync
        assert stub.events[0][2]["child_order"] == 12
        assert "ping" not in modes
        assert "finalize" not in modes  # consolidate_metadata defaults off (issue #191)

    def test_shard_keys_subset_by_label_and_int(self, catalog):
        stub = StubLambdaClient()
        run = _run(catalog, client=stub)
        by_label = run.dispatch(shard_keys=[_LABELS[2]])
        assert set(by_label.futures) == {_WORDS[2]}
        by_int = run.dispatch(shard_keys=[_WORDS[0], _WORDS[1]])
        assert set(by_int.futures) == {_WORDS[0], _WORDS[1]}
        by_label.results(), by_int.results()

    def test_unknown_shard_key_raises(self, catalog):
        run = _run(catalog, client=StubLambdaClient())
        with pytest.raises(ValueError, match="not in shardmap"):
            run.dispatch(shard_keys=[123])


# -- concurrency preflight ----------------------------------------------------


class TestConcurrencyPreflight:
    """`dispatch()` sizes the pool with agg's probe, degrading on denial.

    espg ruling on PR #333, option (a): a notebook caller who can invoke the
    workers can normally read the account concurrency too, so probe by default;
    but a narrow-permission deployment (cryocloud: worker role holds the perms,
    caller only deploys) must degrade, not fail.
    """

    @staticmethod
    def _dispatch(catalog, monkeypatch, *, probe=None, **dispatch_kwargs):
        """Dispatch with NO injected client.

        Returns ``(pool_width, probe_calls, declared_pool)`` -- the fan-out
        width the ``ThreadPoolExecutor`` was built with, the probe's call
        record, and the ``max_pool_connections`` handed to the shared client's
        ``Config`` (``None`` if no client was built).
        """
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner

        stub = StubLambdaClient()
        seen: dict = {}

        def _client(service, **k):
            if service != "lambda":
                return MagicMock()
            if "config" in k:  # the fan-out client; the probe's carries none
                seen["pool"] = k["config"].max_pool_connections
            return stub

        session = MagicMock()
        session.client.side_effect = _client
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: session)

        calls: list = []
        if probe is not None:

            def _probe(requested, lambda_client, cloudwatch_client, function_name, **k):
                calls.append((requested, function_name, k.get("fd_bound")))
                return probe(requested)

            monkeypatch.setattr(runner, "compute_available_workers", _probe)

        widths: list = []

        class _TrackedPool(ThreadPoolExecutor):
            def __init__(self, *a, **k):
                widths.append(k.get("max_workers"))
                super().__init__(*a, **k)

        monkeypatch.setattr(zagg.client, "ThreadPoolExecutor", _TrackedPool)
        run = Run.from_config(
            default_config("atl06"),
            shardmap=catalog,
            store=_STORE,
            function_name="process-shard-test",
            source_credentials=_CREDS,
        )
        run.dispatch(**dispatch_kwargs).results()
        return widths[0], calls, seen.get("pool")

    @staticmethod
    def _report():
        from zagg.concurrency import ConcurrencyReport

        return ConcurrencyReport(
            account_limit=1000,
            current_concurrent=0,
            padding=100,
            available=900,
            function_reserved=None,
        )

    def test_probe_sizes_the_pool(self, catalog, monkeypatch):
        width, calls, _ = self._dispatch(
            catalog, monkeypatch, probe=lambda requested: (2, self._report())
        )
        assert width == 2  # the probe's clamp, not _DEFAULT_MAX_WORKERS
        # It asks for the work size and names the dispatch target.
        assert calls == [(3, "process-shard-test", True)]

    def test_probe_denial_falls_back_with_a_warning(self, catalog, monkeypatch):
        # Distinguish the fallback from the shard-count clamp by shrinking the
        # default below the shard count.
        monkeypatch.setattr(zagg.client, "_DEFAULT_MAX_WORKERS", 2)

        def _denied(requested):
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "no cloudwatch for you"}},
                "GetMetricStatistics",
            )

        with pytest.warns(RuntimeWarning, match="AccessDenied"):
            width, calls, _ = self._dispatch(catalog, monkeypatch, probe=_denied)
        assert width == 2  # degraded to the default, run still dispatched
        assert calls == [(3, "process-shard-test", True)]

    def test_explicit_max_workers_skips_the_probe(self, catalog, monkeypatch):
        def _must_not_run(requested):
            raise AssertionError("probe ran despite an explicit max_workers")

        width, calls, _ = self._dispatch(catalog, monkeypatch, probe=_must_not_run, max_workers=1)
        assert width == 1
        assert calls == []

    def test_injected_client_skips_the_probe(self, catalog, monkeypatch):
        # The stub-client seam: a test double must not have to answer
        # GetAccountSettings/GetMetricStatistics (nor the STS identity probe).
        from zagg import runner

        def _must_not_run(*a, **k):
            raise AssertionError("probe ran with an injected lambda_client")

        monkeypatch.setattr(runner, "compute_available_workers", _must_not_run)
        handle = _run(catalog, client=StubLambdaClient()).dispatch()
        handle.results()
        assert len(handle) == 3

    def test_sync_probes_fd_bound(self, catalog, monkeypatch):
        # The sync pool holds one socket per in-flight shard: FD-bounded.
        assert self._probe_fd_bound(catalog, monkeypatch, transport="sync") is True

    def test_event_window_is_not_fd_bound(self, catalog, monkeypatch):
        # The event transport holds no connections, so its pacing window is
        # sized by account headroom alone (issue #375).
        assert self._probe_fd_bound(catalog, monkeypatch, transport="event") is False

    def test_probe_body_forwards_fd_bound(self, catalog, monkeypatch):
        # The seam the two tests above bracket without crossing: they pin what
        # `dispatch` hands `_probe_workers`, and `TestFdBound` pins what
        # `compute_available_workers` does with it -- but neither runs the real
        # `_probe_workers` body, so deleting `fd_bound=fd_bound` at
        # `client.py:664` left the suite green (review finding). Drive the real
        # body and assert the value it forwards.
        from unittest.mock import MagicMock

        from zagg import runner

        seen: list = []

        def _probe(requested, lambda_client, cloudwatch_client, function_name, **k):
            seen.append(k.get("fd_bound"))
            return 7, self._report()

        run = Run.from_config(
            default_config("atl06"),
            shardmap=catalog,
            store=_STORE,
            function_name="process-shard-test",
            source_credentials=_CREDS,
        )
        monkeypatch.setattr(runner, "compute_available_workers", _probe)
        assert run._probe_workers(MagicMock(), 3, fd_bound=False) == 7
        assert run._probe_workers(MagicMock(), 3, fd_bound=True) == 7
        # Not `[None, None]`: the kwarg reaches the probe, and it is the
        # caller's value, not `compute_available_workers`' own default.
        assert seen == [False, True]

    def test_pool_cap_clamped_when_window_exceeds_fd_bound(self, catalog, monkeypatch):
        # A fan-out width above RLIMIT_NOFILE must not reach the shared
        # client's DECLARED pool (PR #378, question (5) ruling). Forced here
        # by an explicit max_workers, which skips the probe so `workers`
        # carries no fd term at all -- the same shape the event transport
        # reaches through the probe when it passes fd_bound=False (issue
        # #375; that wiring is pinned by test_event_window_is_not_fd_bound,
        # which this test deliberately does not re-derive). The cap is a
        # RETENTION bound, not an EMFILE guard: botocore passes only
        # `maxsize` to urllib3 (block=False), so an oversized declaration
        # allocates nothing on its own -- it is the number of sockets the
        # pool would KEEP once a concurrent dispatch loop opened them.
        monkeypatch.setattr(zagg.concurrency, "fd_safe_max_workers", lambda: 2)
        width, _, pool = self._dispatch(catalog, monkeypatch, max_workers=5)
        # The clamp stops at the DECLARED pool: the fan-out width is still the
        # shard-count clamp of the requested 5, un-touched by the fd bound --
        # re-clamping it there would undo issue #375.
        assert (pool, width) == (2, 3)

    def test_pool_cap_unchanged_below_fd_bound(self, catalog, monkeypatch):
        monkeypatch.setattr(zagg.concurrency, "fd_safe_max_workers", lambda: 100)
        width, _, pool = self._dispatch(catalog, monkeypatch, max_workers=2)
        assert (pool, width) == (2, 2)

    @staticmethod
    def _probe_fd_bound(catalog, monkeypatch, *, transport):
        """The ``fd_bound`` ``dispatch`` hands the probe for ``transport``.

        Intercepts at ``_probe_workers`` and aborts the dispatch there with a
        sentinel: the wiring under test is decided before any invoke, so the
        event transport's runtime (status store + poller) need not be stood up
        to observe it.
        """
        from unittest.mock import MagicMock

        import boto3

        class _ProbeReachedError(Exception):
            pass

        session = MagicMock()
        session.client.side_effect = lambda service, **k: MagicMock()
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: session)

        seen: dict = {}

        def _probe(self, session, n, *, fd_bound=True):
            seen["fd_bound"] = fd_bound
            raise _ProbeReachedError

        monkeypatch.setattr(Run, "_probe_workers", _probe)
        run = Run.from_config(
            default_config("atl06"),
            shardmap=catalog,
            store=_STORE,
            function_name="process-shard-test",
            source_credentials=_CREDS,
        )
        with pytest.raises(_ProbeReachedError):
            run.dispatch(transport=transport)
        return seen["fd_bound"]


# -- parity with the runner's lambda path -------------------------------------


class TestRunnerParity:
    """The PR's central claim, made differential (review finding, PR #333).

    Spot-checking hand-picked keys cannot catch a divergence the facade grows
    later, so drive the SAME config + shardmap through ``runner.agg(backend=
    "lambda", invocation="sync")`` against an equivalent stub and compare the
    cell events field-by-field — with **no carve-outs** now that
    ``_run_lambda`` threads ``driver`` too (espg ruling on PR #333): the
    ``driver: s3`` and ``driver: https`` configs both have to match whole.
    """

    @staticmethod
    def _agg_stub(catalog_file, monkeypatch, cfg=None, **agg_kwargs):
        """Drive ``runner.agg(backend="lambda")`` against a fresh stub; return it."""
        from unittest.mock import MagicMock

        import boto3

        from zagg import hive, runner
        from zagg.concurrency import ConcurrencyReport

        stub = StubLambdaClient()
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
        # The hive manifest checker (issue #274) polls the output store; keep
        # this run read-free rather than reaching for a bucket that isn't there.
        monkeypatch.setattr(hive, "read_manifest", lambda *a, **k: None)
        agg_kwargs.setdefault("invocation", "sync")
        runner.agg(
            cfg if cfg is not None else default_config("atl06"),
            catalog=catalog_file,
            store=_STORE,
            backend="lambda",
            function_name="process-shard-test",
            max_workers=3,
            **agg_kwargs,
        )
        return stub

    @classmethod
    def _agg_cell_events(cls, catalog_file, monkeypatch, cfg=None, **agg_kwargs):
        stub = cls._agg_stub(catalog_file, monkeypatch, cfg=cfg, **agg_kwargs)
        return {e["shard_key"]: e for _, _, e in stub.cell_events()}

    @pytest.mark.parametrize("driver", ["s3", "https"])
    def test_cell_events_match_the_runner_lambda_path(
        self, catalog, catalog_file, monkeypatch, driver
    ):
        cfg = default_config("atl06")
        cfg.data_source["driver"] = driver
        agg_events = self._agg_cell_events(catalog_file, monkeypatch, cfg=cfg)
        stub = StubLambdaClient()
        _run(catalog, client=stub, config=cfg).dispatch().results()
        client_events = {e["shard_key"]: e for _, _, e in stub.cell_events()}

        assert set(client_events) == set(agg_events) == set(_WORDS)
        for key in _WORDS:
            mine, theirs = dict(client_events[key]), dict(agg_events[key])
            # run_id is a fresh uuid per dispatch by construction; both paths
            # carry one and the worker only echoes it into the stats record.
            assert mine.pop("run_id") and theirs.pop("run_id")
            assert mine == theirs  # every other field, both drivers
        # And the driver actually moved the href — otherwise the equality above
        # would pass on two identically-wrong url lists.
        href = _rec(3)["s3"] if driver == "s3" else _rec(3)["https"]
        assert client_events[_WORDS[1]]["granule_urls"] == [href]
        assert agg_events[_WORDS[1]]["granule_urls"] == [href]

    def test_paired_asset_entries_reach_both_paths(self, tmp_path, monkeypatch):
        """A paired-asset map's sibling hrefs (issue #425) ride the facade's
        cell events too: the agg path resolved through
        ``_resolve_granule_entries`` while the client still resolved bare urls,
        so every paired run would have failed on the missing sibling handle
        (review finding, PR #432)."""
        paired = _catalog_dict()
        sib = {"id": "s4", "s3": "s3://bucket/s4.h5", "https": "https://h/s4.h5"}
        paired["granules"] = [[{**_rec(4), "assets": {"l2a": sib}}], [_rec(3)], [_rec(1)]]
        path = tmp_path / "paired.json"
        path.write_text(json.dumps(paired))

        agg_events = self._agg_cell_events(str(path), monkeypatch)
        stub = StubLambdaClient()
        _run(paired, client=stub).dispatch().results()
        client_events = {e["shard_key"]: e for _, _, e in stub.cell_events()}

        entry = {"url": _rec(4)["s3"], "assets": {"l2a": sib["s3"]}}
        assert client_events[_WORDS[0]]["granule_urls"] == [entry]
        assert agg_events[_WORDS[0]]["granule_urls"] == [entry]
        # Single-asset cells stay plain url strings on both paths.
        assert client_events[_WORDS[1]]["granule_urls"] == [_rec(3)["s3"]]
        assert agg_events[_WORDS[1]]["granule_urls"] == [_rec(3)["s3"]]


# -- dispatch manifest (issue #327 phase 2) ------------------------------------


class TestDispatchManifestBlock:
    """The setup invoke carries the ``run_manifest`` block (issue #327): the
    worker records the run's shard set + identity at the status prefix, so
    every lambda run is reattachable by run id — client and agg alike."""

    @staticmethod
    def _setup_block(stub):
        (setup,) = [e for _, _, e in stub.events if e.get("mode") == "setup"]
        return setup["run_manifest"]

    def test_client_setup_carries_the_block(self, catalog):
        from zagg.semantics import semantic_hash

        stub = StubLambdaClient()
        run = _run(catalog, client=stub)
        run.dispatch().wait(timeout=10)
        block = self._setup_block(stub)
        assert sorted(block["shards"]) == sorted(str(w) for w in _WORDS)
        assert block["run_id"] == stub.cell_events()[0][2]["run_id"]
        assert block["semantic_hash"] == semantic_hash(run.config)
        assert block["dataset"] == {"short_name": "ATL06", "version": "006"}
        assert block["dispatched_at"]  # dispatcher clock, worker copies verbatim

    def test_flat_setup_also_carries_the_block(self, catalog):
        cfg = default_config("atl06")
        cfg.output["store_layout"] = "flat"
        stub = StubLambdaClient()
        _run(catalog, client=stub, config=cfg).dispatch().wait(timeout=10)
        assert sorted(self._setup_block(stub)["shards"]) == sorted(str(w) for w in _WORDS)

    def test_agg_setup_block_matches_the_client(self, catalog, catalog_file, monkeypatch):
        agg_stub = TestRunnerParity._agg_stub(catalog_file, monkeypatch)
        theirs = self._setup_block(agg_stub)
        stub = StubLambdaClient()
        _run(catalog, client=stub).dispatch().wait(timeout=10)
        mine = self._setup_block(stub)
        # run_id / dispatched_at are per-dispatch by construction.
        for block in (mine, theirs):
            assert block.pop("run_id") and block.pop("dispatched_at")
        assert mine == theirs

    def test_subset_dispatch_lists_only_dispatched_shards(self, catalog):
        stub = StubLambdaClient()
        _run(catalog, client=stub).dispatch(shard_keys=[_WORDS[1]]).wait(timeout=10)
        assert self._setup_block(stub)["shards"] == [str(_WORDS[1])]

    def test_async_setup_size_gate_drops_the_block(self, monkeypatch):
        # The hive setup invoke is a 256 KB-capped Event; an oversized shard
        # list drops the block (never fatal) — attach degrades to statuses.
        from zagg import runner

        stub = StubLambdaClient()
        monkeypatch.setattr(runner, "_ASYNC_PAYLOAD_CAP_BYTES", 10)
        runner._invoke_lambda_setup_async(
            stub,
            "fn",
            _STORE,
            config_dict={},
            run_manifest={"run_id": "r", "shards": ["1"]},
        )
        (_, _, event) = stub.events[0]
        assert "run_manifest" not in event

    def test_sync_setup_has_no_size_gate(self):
        # The flat setup invoke is synchronous (6 MB cap): the block always rides.
        from zagg import runner

        stub = StubLambdaClient()
        runner._invoke_lambda_setup(
            stub,
            "fn",
            _STORE,
            parent_order=6,
            child_order=12,
            overwrite=False,
            config_dict={},
            run_manifest={"run_id": "r", "shards": ["1"]},
        )
        (_, _, event) = stub.events[0]
        assert event["run_manifest"] == {"run_id": "r", "shards": ["1"]}


# -- futures -----------------------------------------------------------------


class TestFutures:
    def test_as_completed_yields_in_completion_order(self, catalog):
        slow = {_WORDS[0]: 0.25, _WORDS[1]: 0.25}
        stub = StubLambdaClient(delays=slow)
        handle = _run(catalog, client=stub).dispatch(max_workers=3)
        first = next(iter(handle.as_completed()))
        assert first is handle.futures[_WORDS[2]]  # the undelayed shard lands first
        handle.results()

    @pytest.mark.parametrize("drain", ["as_completed", "progress", "results", "raise_first"])
    def test_draining_joins_the_post_run_tail(self, catalog, drain):
        # Regression (review finding, PR #333): the documented flow never calls
        # wait(), so the finisher used to be cut off at process exit with the
        # coverage/stats rollups undispatched. Draining any harvest iterator
        # joins the tail, so the loop alone completes the run.
        if drain == "progress":
            pytest.importorskip("tqdm")
        stub = StubLambdaClient(mode_delay=0.05)
        handle = _run(catalog, client=stub).dispatch()
        if drain == "results":
            handle.results()
        elif drain == "raise_first":
            handle.raise_first()
        else:
            for fut in getattr(handle, drain)():
                fut.result()
        # Whole tail landed, in order, with no wait() call anywhere above.
        assert stub.modes()[-4:] == ["finalize", "coverage", "stats", "icechunk_finalize"]
        assert not handle._finisher.is_alive()

    def test_tail_exception_surfaces_and_still_shuts_the_pool_down(self, catalog, monkeypatch):
        # Regression (review finding, PR #333): _post_run's plumbing used to sit
        # outside every try, so a crash there died in threading.excepthook while
        # wait() reported a clean run and the pool was never shut down.
        from zagg import runner

        def _boom(*a, **k):
            raise RuntimeError("tail blew up")

        monkeypatch.setattr(runner, "_lambda_result_rows", _boom)
        stub = StubLambdaClient()
        run = _run(catalog, client=stub)
        pools: list = []

        class _TrackedPool(ThreadPoolExecutor):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                pools.append(self)

        monkeypatch.setattr(zagg.client, "ThreadPoolExecutor", _TrackedPool)
        handle = run.dispatch()
        with pytest.raises(RuntimeError, match="tail blew up"):
            handle.wait(timeout=10)
        # ... and the same failure is what a plain drain surfaces.
        with pytest.raises(RuntimeError, match="tail blew up"):
            handle.results()
        # try/finally ran: the pool refuses new work.
        with pytest.raises(RuntimeError, match="shutdown"):
            pools[0].submit(len, "")

    def test_transport_exception_still_gets_a_failure_row(self, catalog, monkeypatch):
        # Regression (review finding, PR #333): a shard failing with something
        # other than ShardError — fd exhaustion here, likewise the payload-cap
        # ValueError or a botocore error escaping the retry loop — contributed
        # no run-stats row at all, vanishing from telemetry while status() still
        # counted it failed.
        from zagg import runner

        real_rows = runner._lambda_result_rows
        captured: dict = {}

        def _capture(results, *, run_id=None):
            out = real_rows(results, run_id=run_id)
            captured["rows"] = out[0]
            return out

        monkeypatch.setattr(runner, "_lambda_result_rows", _capture)

        class _FdExhaustedStub(StubLambdaClient):
            def invoke(self, **kwargs):
                event = json.loads(kwargs["Payload"])
                if event.get("mode") is None and event["shard_key"] == _WORDS[0]:
                    # What botocore surfaces at the fd ceiling; runner's
                    # raise_for_fd_exhaustion re-raises it past the retry loop.
                    raise OSError(errno.EMFILE, "Too many open files")
                return super().invoke(**kwargs)

        handle = _run(catalog, client=_FdExhaustedStub()).dispatch()
        with pytest.raises(OSError, match="Too many open files"):
            handle.futures[_WORDS[0]].result()
        mapped = handle.results(return_exceptions=True)  # also joins the tail
        assert isinstance(mapped[_WORDS[0]], OSError)
        assert handle.status() == {"pending": 0, "ok": 2, "failed": 1}
        # All three shards are in the run record, the dropped one as a failure.
        rows = captured["rows"]
        assert sorted(r["shard_key"] for r in rows) == sorted(_WORDS)
        (bad,) = [r for r in rows if r["shard_key"] == _WORDS[0]]
        assert bad["success"] is False
        assert "Too many open files" in bad["error"]

    def test_finalize_failure_warns_immediately(self, catalog, monkeypatch):
        # espg ruling on PR #333 (middle option): the failure is warned the
        # moment it happens — not only when the harvest loop finally joins — so
        # a notebook shows it in-stream; the tail still runs and the error still
        # re-raises. The stats leg is gated open so the assertion lands while
        # the tail is demonstrably still in flight.
        from zagg import runner

        gate = threading.Event()

        def _fail(*a, **k):
            raise RuntimeError("finalize down")

        monkeypatch.setattr(runner, "_invoke_lambda_finalize", _fail)
        monkeypatch.setattr(runner, "_dispatch_run_stats", lambda *a, **k: gate.wait(10))
        # Shared helper's fixed backoff (issue #335), collapsed for the test.
        monkeypatch.setattr(runner, "_FINALIZE_BACKOFF_S", 0)

        def _finalize_warnings(caught):
            return [str(w.message) for w in caught if "manifest backstop" in str(w.message)]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            handle = _run(catalog, client=StubLambdaClient()).dispatch()
            deadline = time.time() + 10
            # Two: one per attempt of the shared retry (issue #335).
            while len(_finalize_warnings(caught)) < 2:
                assert time.time() < deadline, "no finalize warning while the tail ran"
                time.sleep(0.01)
            assert handle._finisher.is_alive()  # warned before the join, not at it
            msg = _finalize_warnings(caught)[-1]  # the terminal warning
            assert _STORE in msg and "idempotent" in msg
            assert "handle.wait()" in msg  # this path's re-raise note
            gate.set()
        # Re-raise semantics unchanged.
        with pytest.raises(RuntimeError, match="finalize down"):
            handle.wait(timeout=10)

    def test_finalize_retry_heals_and_stays_clean(self, catalog, monkeypatch):
        """Issue #335: the facade shares agg's retry, so a transient finalize
        failure (throttle, cold-start timeout) leaves a CLEAN handle — warned,
        but nothing recorded and nothing re-raised from wait()."""
        from zagg import runner

        outcomes = iter([RuntimeError("throttled"), None])
        real = runner._invoke_lambda_finalize

        def _flaky(*a, **k):
            e = next(outcomes, None)
            if e is not None:
                raise e
            return real(*a, **k)

        monkeypatch.setattr(runner, "_invoke_lambda_finalize", _flaky)
        monkeypatch.setattr(runner, "_FINALIZE_BACKOFF_S", 0)
        stub = StubLambdaClient()
        with pytest.warns(RuntimeWarning, match=r"finalize \(manifest backstop\) failed"):
            handle = _run(catalog, client=stub).dispatch()
            handle.wait(timeout=10)  # no raise: the retry healed it
        assert handle._finalize_error is None and handle._tail_error is None
        assert "finalize" in stub.modes()  # the retry's invoke really landed

    def test_finalize_failure_wins_over_a_tail_crash(self, catalog, monkeypatch):
        # Two distinct channels: the D6 manifest backstop failure is the one
        # raised when both a finalize failure and a tail crash are recorded.
        from zagg import runner

        def _fail(*a, **k):
            raise RuntimeError("finalize down")

        def _boom(*a, **k):
            raise RuntimeError("tail blew up")

        monkeypatch.setattr(runner, "_invoke_lambda_finalize", _fail)
        monkeypatch.setattr(runner, "_lambda_result_rows", _boom)
        monkeypatch.setattr(runner, "_FINALIZE_BACKOFF_S", 0)
        handle = _run(catalog, client=StubLambdaClient()).dispatch()
        with pytest.raises(RuntimeError, match="finalize down"):
            handle.wait(timeout=10)
        assert isinstance(handle._tail_error, RuntimeError)

    def test_break_out_of_the_loop_leaves_the_tail_to_wait(self, catalog):
        # The documented escape hatch: an undrained iterator does not join, so
        # wait() stays the explicit form (and the finisher is a daemon, so an
        # abandoned handle cannot wedge interpreter exit).
        stub = StubLambdaClient(mode_delay=0.05)
        handle = _run(catalog, client=stub).dispatch()
        for fut in handle.as_completed():
            fut.result()
            break
        handle.wait(timeout=10)
        assert "stats" in stub.modes()

    def test_error_payload_surfaces_as_shard_error(self, catalog):
        stub = StubLambdaClient(fail={_WORDS[1]})
        handle = _run(catalog, client=stub).dispatch()
        fut = handle.futures[_WORDS[1]]
        with pytest.raises(ShardError, match="boom") as excinfo:
            fut.result()
        err = excinfo.value
        assert err.shard_key == _WORDS[1]
        assert err.label == _LABELS[1]
        assert err.payload["status_code"] == 500
        assert err.payload["error"] == "boom"
        assert "lambda_duration" in err.payload  # timings ride the payload

    def test_function_error_timeout_surfaces(self, catalog):
        stub = StubLambdaClient(timeout={_WORDS[0]})
        handle = _run(catalog, client=stub).dispatch()
        with pytest.raises(ShardError, match="timeout") as excinfo:
            handle.futures[_WORDS[0]].result()
        assert excinfo.value.payload["timeout"] is True

    def test_status_counts_and_len(self, catalog):
        stub = StubLambdaClient(fail={_WORDS[1]})
        stub.gate = threading.Event()
        handle = _run(catalog, client=stub).dispatch()
        assert len(handle) == 3
        assert handle.status() == {"pending": 3, "ok": 0, "failed": 0}
        stub.gate.set()
        handle.wait(timeout=10)
        assert handle.status() == {"pending": 0, "ok": 2, "failed": 1}
        assert "3 shards" in repr(handle)

    def test_benign_no_data_counts_ok(self, catalog):
        stub = StubLambdaClient(benign={_WORDS[2]})
        handle = _run(catalog, client=stub).dispatch()
        assert handle.results()[_WORDS[2]]["error"] == "No granules found"
        assert handle.status() == {"pending": 0, "ok": 3, "failed": 0}

    def test_results_raises_or_maps_exceptions(self, catalog):
        stub = StubLambdaClient(fail={_WORDS[0]})
        handle = _run(catalog, client=stub).dispatch()
        with pytest.raises(ShardError):
            handle.results()
        mapped = handle.results(return_exceptions=True)
        assert isinstance(mapped[_WORDS[0]], ShardError)
        assert mapped[_WORDS[1]]["status_code"] == 200

    def test_raise_first(self, catalog):
        failing = StubLambdaClient(fail={_WORDS[2]})
        handle = _run(catalog, client=failing).dispatch()
        with pytest.raises(ShardError):
            handle.raise_first()
        clean = _run(catalog, client=StubLambdaClient()).dispatch()
        assert clean.raise_first() is None


# -- tail timing -------------------------------------------------------------


class TestTailTiming:
    def test_wait_timeout_must_clear_the_stats_verify_window(self, catalog, monkeypatch):
        # With the verify window zeroed everywhere else, nothing in the suite
        # sees the tail's real floor: the run-stats leg fires, polls for the
        # parquet over _RUN_STATS_VERIFY_WINDOW_S (20 s shipped), then re-fires
        # once and polls again — so a successful run can spend ~2x the window
        # in the tail and a "generous" wait(timeout=30) reports a false
        # timeout. Scaled 100x down here, with the poll faked so no store is
        # opened (review finding, PR #333).
        from zagg import runner

        window = 0.2
        monkeypatch.setattr(runner, "_RUN_STATS_VERIFY_WINDOW_S", window)

        def _never_visible(store_path, key, store_kwargs, *, window_s=None):
            time.sleep(window)  # stands in for the read-only HEAD poll
            return False

        monkeypatch.setattr(runner, "_await_run_stats_object", _never_visible)
        stub = StubLambdaClient()
        handle = _run(catalog, client=stub).dispatch()
        with pytest.raises(TimeoutError, match="still in flight"):
            handle.wait(timeout=window / 4)  # the "obviously generous" choice
        handle.wait(timeout=10)  # > 2x the window: clean
        # Fire -> verify -> re-fire: the stats invoke went out twice.
        assert stub.modes().count("stats") == 2


class TestCostRollup:
    def test_cost_usd_prices_billed_durations(self, catalog):
        from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC

        handle = _run(catalog, client=StubLambdaClient()).dispatch()
        handle.results()
        total = sum(f.result().get("lambda_duration") or 0.0 for f in handle.futures.values())
        assert handle.cost_usd() == pytest.approx(total * 4.0 * LAMBDA_PRICE_PER_GB_SEC)

    def test_cost_usd_none_without_memory(self):
        handle = RunHandle({}, store_path="s3://b/s.zarr")
        assert handle.cost_usd() is None


class TestProgressAsync:
    def test_drains_and_joins_tail_without_blocking(self, catalog):
        stub = StubLambdaClient(mode_delay=0.05)
        handle = _run(catalog, client=stub).dispatch()
        thread = handle.progress_async(leave=False, disable=True)
        thread.join(timeout=30)
        assert not thread.is_alive()
        assert handle.status()["pending"] == 0
        # Draining on the thread still joined the post-run tail.
        assert stub.modes()[-4:] == ["finalize", "coverage", "stats", "icechunk_finalize"]


# -- tqdm optionality --------------------------------------------------------


class TestTqdmOptional:
    def test_module_imports_without_tqdm(self, monkeypatch):
        # zagg core must import fine without tqdm (issue #326): blocking the
        # package and re-executing the module proves nothing at module level
        # touches it.
        monkeypatch.setitem(sys.modules, "tqdm", None)
        monkeypatch.setitem(sys.modules, "tqdm.auto", None)
        importlib.reload(zagg.client)

    def test_progress_without_tqdm_raises_with_hint(self, catalog, monkeypatch):
        handle = _run(catalog, client=StubLambdaClient()).dispatch()
        handle.results()
        monkeypatch.setitem(sys.modules, "tqdm", None)
        monkeypatch.setitem(sys.modules, "tqdm.auto", None)
        with pytest.raises(ImportError, match="analysis"):
            handle.progress()

    def test_progress_iterates_futures(self, catalog):
        pytest.importorskip("tqdm")
        handle = _run(catalog, client=StubLambdaClient()).dispatch()
        seen = [fut.result()["shard_key"] for fut in handle.progress(leave=False)]
        assert sorted(seen) == sorted(_WORDS)
