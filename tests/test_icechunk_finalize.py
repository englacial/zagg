"""Run finalize for the Icechunk companion repo (issue #582 phase 2, spec §11.4).

The tag, the finalize commit's metadata, retention, idempotency, the local
and Lambda dispatcher seams, the handler mode and the config knob.
"""

from __future__ import annotations

import json
import logging

import pytest

from zagg import icechunk_refs
from zagg.config import default_config
from zagg.icechunk_finalize import TAG_PREFIX, finalize_repo, resolve_retain_runs, run_tag


@pytest.fixture
def cfg():
    cfg = default_config("atl06", validate=False)
    cfg.output["store_layout"] = "hive"
    cfg.output["grid"] = {
        **cfg.output.get("grid", {}),
        "type": "healpix",
        "parent_order": 4,
        "child_order": 6,
        "chunk_inner": 5,
    }
    return cfg


@pytest.fixture
def repo(cfg, tmp_path):
    """An initialized local repo; ``(root, grid)``."""
    from zagg.grids import from_config

    root = str(tmp_path / "store")
    grid = from_config(cfg, parent_order=4)
    icechunk_refs.init_repo(root, grid, cfg, run_id="r0", store_kwargs={})
    return root, grid


def _finalize(root, run_id, **kw):
    kw.setdefault("semantic_hash", "h")
    kw.setdefault("store_kwargs", {})
    return finalize_repo(root, run_id=run_id, **kw)


def _open(root):
    return icechunk_refs.open_repo(root, store_kwargs={})


def _messages(repo):
    return [s.message for s in repo.ancestry(branch="main")]


class TestFinalize:
    def test_tags_a_run_identifying_commit(self, repo):
        from zagg import __version__

        root, _grid = repo
        out = _finalize(root, "r1", split_ratchet=None)
        r = _open(root)
        assert out["tag"] == run_tag("r1") == "run-r1" and out["tagged"] is True
        assert out["path"] == f"{root}/icechunk" and out["commit_s"] >= 0.0
        assert r.lookup_tag("run-r1") == out["snapshot"]
        info = r.lookup_snapshot(out["snapshot"])
        assert info.message == "finalize r1"
        # The metadata IS the run record: identity, the ladder knobs, the
        # retention counts.
        block = icechunk_refs.read_block(root, store_kwargs={})
        assert info.metadata == {
            "run_id": "r1",
            "semantic_hash": "h",
            "zagg_version": __version__,
            "commit": block["commit"],
            "commit_order": block["commit_order"],
            "split_order": block["split_order"],
            "retain_runs": 0,
            "tags_deleted": 0,
            "snapshots_expired": 0,
            "gc": None,
            "retention_error": None,
        }
        # K = 0: nothing expired, nothing collected, the init history intact.
        assert out["gc"] is None and out["snapshots_expired"] == 0
        assert _messages(r)[:2] == ["finalize r1", "init r0"]

    def test_rerun_is_idempotent(self, repo):
        root, _grid = repo
        first = _finalize(root, "r1")
        again = _finalize(root, "r1", retain_runs=1)
        assert again["tagged"] is False and again["snapshot"] == first["snapshot"]
        assert again["tags_deleted"] == 0 and again["gc"] is None
        assert _messages(_open(root)).count("finalize r1") == 1

    def test_a_concurrent_tag_reads_the_winners(self, repo, monkeypatch):
        # Two live finalizes of one run: the other lands its tag between this
        # one's list_tags check and create_tag, whose retry then raises the
        # real AlreadyExistsError — the loser returns the winner's tag.
        import icechunk

        root, _grid = repo
        real = icechunk.Repository.create_tag

        def racing(self, tag, snapshot_id):
            real(self, tag, snapshot_id)  # the winner
            real(self, tag, snapshot_id)  # this finalize: already exists

        monkeypatch.setattr(icechunk.Repository, "create_tag", racing)
        out = _finalize(root, "r1")
        assert out["tagged"] is False and out["snapshot"] == _open(root).lookup_tag("run-r1")

    def test_a_tag_failure_without_the_tag_raises(self, repo, monkeypatch):
        import icechunk

        def broken(self, tag, snapshot_id):
            raise icechunk.IcechunkError("storage down")

        monkeypatch.setattr(icechunk.Repository, "create_tag", broken)
        with pytest.raises(icechunk.IcechunkError, match="storage down"):
            _finalize(repo[0], "r1")

    def test_missing_repo_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not initialized"):
            _finalize(str(tmp_path / "nope"), "r1")

    def test_split_ratchet_is_reported_not_rewritten(self, repo, caplog):
        root, _grid = repo
        before = len(_messages(_open(root)))
        with caplog.at_level(logging.WARNING, logger="zagg.icechunk_finalize"):
            out = _finalize(root, "r1", split_ratchet={"from": 4, "to": 3})
        assert out["rewrite_pending"] == {"from": 4, "to": 3}
        assert "rewrite_manifests" in caplog.text
        assert len(_messages(_open(root))) == before + 1  # the finalize commit only


def _empty_commit(root, message):
    """One content-free commit on ``main``, as a later run's init or a leaf's would land."""
    session = _open(root).writable_session("main")
    session.commit(message, allow_empty=True)


class TestNewestOnly:
    """``newest_only`` (the ``Run.attach`` finalize): only the repo's newest run is tagged."""

    def test_tags_the_newest_run(self, repo):
        root, _grid = repo
        _empty_commit(root, "leaf 123")  # a leaf commit names no run
        out = _finalize(root, "r0", newest_only=True)
        assert out["tagged"] is True and "skipped" not in out
        assert _open(root).lookup_tag("run-r0") == out["snapshot"]

    @pytest.mark.parametrize("later", ["init r1", "finalize r1"])
    def test_skips_when_a_later_run_committed(self, repo, later):
        root, _grid = repo
        _finalize(root, "rprev")  # an earlier tag retention would otherwise drop
        _empty_commit(root, later)
        before = _messages(_open(root))
        out = _finalize(root, "r0", newest_only=True, retain_runs=1)
        assert out["skipped"] == "a later run has committed since run r0"
        assert out["tagged"] is False and out["snapshot"] is None
        assert out["tags_deleted"] == 0 and out["gc"] is None
        r = _open(root)
        assert _messages(r) == before  # no commit
        assert set(r.list_tags()) == {"run-rprev"}  # no tag, no retention

    def test_a_repeat_run_with_an_unchanged_block_is_the_newest(self, repo):
        # The usual repeat run changes nothing at init, yet its (empty) ``init``
        # commit still marks it as the newest run: the reattached finalize
        # tags it rather than skipping (issue #582).
        root, grid = repo
        _finalize(root, "r0")
        cfg = grid.config
        icechunk_refs.init_repo(root, grid, cfg, run_id="r1", store_kwargs={})
        _empty_commit(root, "leaf 123")
        out = _finalize(root, "r1", newest_only=True)
        assert out["tagged"] is True and "skipped" not in out
        assert _messages(_open(root))[:3] == ["finalize r1", "leaf 123", "init r1"]

    def test_existing_tag_is_a_no_op(self, repo):
        root, _grid = repo
        first = _finalize(root, "r0")
        _empty_commit(root, "init r1")
        again = _finalize(root, "r0", newest_only=True)
        assert again["tagged"] is False and again["snapshot"] == first["snapshot"]
        assert "skipped" not in again


class TestRetention:
    def _runs(self, root, n, retain_runs, *, leaf=False):
        """``n`` finalized runs; ``leaf`` lands one ``leaf r{i}`` commit before each."""
        outs = []
        for i in range(1, n + 1):
            if leaf:
                _open(root).writable_session("main").commit(f"leaf r{i}", allow_empty=True)
            outs.append(_finalize(root, f"r{i}", retain_runs=retain_runs))
        return outs

    def test_zero_keeps_every_run(self, repo):
        root, _grid = repo
        self._runs(root, 3, 0)
        r = _open(root)
        assert sorted(r.list_tags()) == ["run-r1", "run-r2", "run-r3"]
        assert _messages(r)[:4] == ["finalize r3", "finalize r2", "finalize r1", "init r0"]

    def test_k_keeps_the_k_newest_and_collects_the_rest(self, repo):
        root, _grid = repo
        outs = self._runs(root, 3, 2, leaf=True)
        r = _open(root)
        assert sorted(r.list_tags()) == ["run-r2", "run-r3"]
        # r1: no earlier tag -> nothing to expire; r2: r1 retained (K - 1 =
        # 1), the init commits older than r1's finalize expire; r3: r1's tag
        # dropped, everything older than r2's finalize expires and is
        # collected.
        assert [o["tags_deleted"] for o in outs] == [0, 0, 1]
        assert outs[0]["gc"] is None and outs[0]["snapshots_expired"] == 0
        assert outs[1]["snapshots_expired"] >= 1 and outs[1]["gc"]["snapshots_deleted"] >= 1
        assert outs[2]["snapshots_expired"] >= 1 and outs[2]["gc"]["bytes_deleted"] > 0
        # Everything older than the oldest retained finalize (r2's) is
        # squashed into it — r2's own leaf commit included; the newest run
        # keeps its intermediate commit until it ages past a later cutoff.
        assert _messages(r) == ["finalize r3", "leaf r3", "finalize r2", "Repository initialized"]
        assert r.lookup_tag("run-r2") == outs[1]["snapshot"]

    def test_a_retention_error_still_commits_and_tags(self, repo, monkeypatch, caplog):
        # Retention is fail-open (review finding): a lost delete_tag race
        # (RefNotFoundError on icechunk 2.2.2) is recorded, never costs the tag.
        import icechunk

        root, _grid = repo
        self._runs(root, 2, 0)

        def boom(self, tag):
            raise RuntimeError(f"ref not found `{tag}`")

        monkeypatch.setattr(icechunk.Repository, "delete_tag", boom)
        with caplog.at_level(logging.WARNING, logger="zagg.icechunk_finalize"):
            out = _finalize(root, "r3", retain_runs=1)
        assert out["tagged"] is True and out["tags_deleted"] == 0
        assert out["retention_error"] == "RuntimeError: ref not found `run-r2`"
        assert "fail-open" in caplog.text
        r = _open(root)
        assert r.lookup_tag("run-r3") == out["snapshot"]
        info = r.lookup_snapshot(out["snapshot"])
        assert info.message == "finalize r3"
        assert info.metadata["retention_error"] == out["retention_error"]

    def test_only_run_tags_are_ever_deleted(self, repo):
        root, _grid = repo
        r = _open(root)
        r.create_tag("keep-me", r.lookup_branch("main"))
        self._runs(root, 3, 1)
        assert sorted(_open(root).list_tags()) == ["keep-me", "run-r3"]

    def test_k_one_never_uses_now_as_the_cutoff(self, repo):
        # With K = 1 nothing is retained; the cutoff is the newest DROPPED
        # tag's time, so that tag's own snapshot survives one more finalize
        # rather than "everything older than now" expiring — which would
        # collect a concurrent writer's in-flight objects.
        root, _grid = repo
        first = _finalize(root, "r1", retain_runs=1)
        assert first["gc"] is None  # no earlier tag: nothing to expire
        second = _finalize(root, "r2", retain_runs=1)
        assert second["tags_deleted"] == 1
        assert TAG_PREFIX + "r1" not in _open(root).list_tags()
        # r1's finalize snapshot IS the cutoff: not older than it, so it is
        # still in the ancestry; only the init commits before it expired.
        ids = {s.id for s in _open(root).ancestry(branch="main")}
        assert first["snapshot"] in ids and second["snapshots_expired"] >= 1
        third = _finalize(root, "r3", retain_runs=1)
        assert first["snapshot"] not in {s.id for s in _open(root).ancestry(branch="main")}
        assert third["snapshots_expired"] >= 1


class TestKnob:
    def test_retain_runs_is_validated(self, cfg):
        from zagg.config import get_icechunk_options, validate_config

        cfg.output["icechunk"] = {"retain_runs": 3}
        validate_config(cfg)
        assert get_icechunk_options(cfg)["retain_runs"] == 3
        assert resolve_retain_runs(cfg) == 3
        for bad in (-1, True, "3", 1.5):
            cfg.output["icechunk"] = {"retain_runs": bad}
            with pytest.raises(ValueError, match="retain_runs"):
                validate_config(cfg)

    def test_default_keeps_every_run(self, cfg):
        assert resolve_retain_runs(cfg) == 0
        cfg.output["icechunk"] = True
        assert resolve_retain_runs(cfg) == 0
        cfg.output["icechunk"] = {"commit": "leaf"}
        assert resolve_retain_runs(cfg) == 0


class TestLocalFinalize:
    def test_none_error_and_record(self, cfg, repo, caplog):
        from zagg import runner

        root, _grid = repo
        assert runner._finalize_icechunk_local(cfg, root, "r1", None, "h", {}) is None
        out = runner._finalize_icechunk_local(cfg, root, "r1", {"created": True}, "h", {})
        assert out["tag"] == "run-r1" and out["tagged"] is True
        # A failed init still records the finalize outcome apart from it.
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            out = runner._finalize_icechunk_local(
                cfg, str(root) + "-missing", "r2", {"error": "RuntimeError: x"}, "h", {}
            )
        assert set(out) == {"error"} and "fail-open, issue #582" in caplog.text


class _Payload:
    def __init__(self, raw: bytes):
        self._raw = raw

    def read(self):
        return self._raw


def _envelope(body: dict, status: int = 200, function_error: str | None = None) -> dict:
    raw = json.dumps({"statusCode": status, "body": json.dumps(body)}).encode()
    out: dict = {"Payload": _Payload(raw)}
    if function_error:
        out["FunctionError"] = function_error
    return out


class _Client:
    def __init__(self, response=None, raise_exc=None):
        self.events: list = []
        self._response = response
        self._raise = raise_exc

    def invoke(self, **kwargs):
        self.events.append((kwargs["InvocationType"], json.loads(kwargs["Payload"])))
        if self._raise is not None:
            raise self._raise
        return self._response


class TestLambdaFinalizeInvoke:
    def _call(self, client):
        from zagg import runner

        return runner._invoke_lambda_icechunk_finalize(
            client,
            "fn",
            "s3://b/p",
            config_dict={"x": 1},
            run_id="r1",
            icechunk_init={"path": "s3://b/p/icechunk", "split_ratchet": {"from": 4, "to": 3}},
            output_creds_event={"accessKeyId": "a", "secretAccessKey": "s"},
        )

    def test_event_shape_and_record(self):
        body = {
            "ok": True,
            "mode": "icechunk_finalize",
            "path": "s3://b/p/icechunk",
            "tag": "run-r1",
            "snapshot": "SNAP",
            "tagged": True,
            "retain_runs": 0,
            "tags_deleted": 0,
            "snapshots_expired": 0,
            "gc": None,
            "rewrite_pending": {"from": 4, "to": 3},
            "commit_s": 0.01,
        }
        client = _Client(_envelope(body))
        out = self._call(client)
        assert out.pop("invoke_s") >= 0.0
        assert out == {k: v for k, v in body.items() if k not in ("ok", "mode")}
        ((kind, event),) = client.events
        assert kind == "RequestResponse"  # the summary carries the tag
        assert event == {
            "mode": "icechunk_finalize",
            "store_path": "s3://b/p",
            "run_id": "r1",
            "config": {"x": 1},
            "icechunk_init": {"path": "s3://b/p/icechunk", "split_ratchet": {"from": 4, "to": 3}},
            "output_credentials": {"accessKeyId": "a", "secretAccessKey": "s"},
        }

    def test_newest_only_rides_the_event(self):
        from zagg import runner

        client = _Client(_envelope({"ok": True, "tag": "run-r1", "skipped": "later"}))
        out = runner._invoke_lambda_icechunk_finalize(
            client, "fn", "s3://b/p", config_dict={}, run_id="r1", newest_only=True
        )
        assert out["skipped"] == "later"
        ((_kind, event),) = client.events
        assert event["newest_only"] is True

    @pytest.mark.parametrize(
        "response, raise_exc, match",
        [
            (_envelope({"error": "boom", "mode": "icechunk_finalize"}, status=500), None, "boom"),
            (_envelope({"error": "Missing shard_key"}, status=400), None, "statusCode 400"),
            (_envelope({}, function_error="Unhandled"), None, "RuntimeError"),
            (None, ConnectionError("throttled"), "throttled"),
            (_envelope({"zagg_version": "stub"}), None, "unexpected icechunk_finalize body"),
            (_envelope({"ok": True, "mode": "icechunk_finalize"}), None, "unexpected"),
        ],
    )
    def test_failures_are_fail_open(self, response, raise_exc, match, caplog):
        with caplog.at_level(logging.WARNING, logger="zagg.runner"):
            out = self._call(_Client(response, raise_exc))
        assert set(out) == {"error"} and match in out["error"]
        assert "fail-open, issue #582" in caplog.text


@pytest.fixture(scope="module")
def handler_mod():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py"
    spec = importlib.util.spec_from_file_location("zagg_lambda_handler_582", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestHandlerMode:
    def _event(self, root, cfg, mode, **extra):
        from dataclasses import asdict

        return {"mode": mode, "store_path": root, "run_id": "r1", "config": asdict(cfg), **extra}

    def test_init_then_finalize(self, handler_mod, cfg, tmp_path):
        from zagg.semantics import semantic_hash

        root = str(tmp_path / "store")
        init = handler_mod.lambda_handler(
            self._event(root, cfg, "icechunk_init", parent_order=4), None
        )
        assert init["statusCode"] == 200, init
        init_body = json.loads(init["body"])
        resp = handler_mod.lambda_handler(
            self._event(root, cfg, "icechunk_finalize", icechunk_init=init_body), None
        )
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert body["ok"] and body["mode"] == "icechunk_finalize"
        assert body["tag"] == "run-r1" and body["tagged"] is True
        assert body["rewrite_pending"] is None
        r = _open(root)
        assert r.lookup_tag("run-r1") == body["snapshot"]
        # The hash is computed worker-side from the forwarded config.
        assert r.lookup_snapshot(body["snapshot"]).metadata["semantic_hash"] == semantic_hash(cfg)
        again = json.loads(
            handler_mod.lambda_handler(self._event(root, cfg, "icechunk_finalize"), None)["body"]
        )
        assert again["tagged"] is False and again["snapshot"] == body["snapshot"]

    def test_newest_only_rides_the_event(self, handler_mod, cfg, repo):
        root, _grid = repo
        _empty_commit(root, "init r2")  # a later run started after r1
        resp = handler_mod.lambda_handler(
            self._event(root, cfg, "icechunk_finalize", newest_only=True), None
        )
        assert resp["statusCode"] == 200, resp
        body = json.loads(resp["body"])
        assert body["skipped"] and body["tagged"] is False and body["tag"] == "run-r1"
        assert not _open(root).list_tags()
        # Without the field the dispatcher's finalize tags as before.
        body = json.loads(
            handler_mod.lambda_handler(self._event(root, cfg, "icechunk_finalize"), None)["body"]
        )
        assert body["tagged"] is True

    def test_run_id_is_required(self, handler_mod, cfg, repo):
        root, _grid = repo
        event = self._event(root, cfg, "icechunk_finalize")
        del event["run_id"]
        resp = handler_mod.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        assert "run_id" in json.loads(resp["body"])["error"]
        assert not _open(root).list_tags()

    def test_before_init_is_500_never_raises(self, handler_mod, cfg, tmp_path):
        resp = handler_mod.lambda_handler(
            self._event(str(tmp_path / "store"), cfg, "icechunk_finalize"), None
        )
        assert resp["statusCode"] == 500
        assert json.loads(resp["body"])["mode"] == "icechunk_finalize"
