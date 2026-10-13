"""Tests for the notebook dispatch wrapper + CLI cost gate (issue #298).

Covers the pre-invoke ceiling preview (in-tree shardmap, no AWS), the
tqdm-present and tqdm-absent progress paths, the :class:`~zagg.notebook.RunView`
HTML report, the non-blocking notebook ``run`` wrapper, and the blocking
CLI-only ``--yes``-skippable confirm gate.
"""

import sys
from pathlib import Path

import pytest
from test_live_store_templates import WINDOWED_TEMPLATES

from zagg import notebook
from zagg.config import default_config, validate_config
from zagg.dispatch import LAMBDA_PRICE_PER_GB_SEC_BY_ARCH

# 4-shard ATL03 shardmap checked into the tree (benchmark fixture).
SHARDMAP = str(Path(__file__).parent / "data" / "benchmark" / "shardmaps" / "sm_healpix_o9.json")


def _windowed_cfg(unit=None):
    """A hive config windowed yearly over SHARDMAP (4 shards, 8 windows each).

    ``unit=None`` leaves ``output.windowing.unit`` unset, so it resolves to
    the ``"shard"`` default of issue #586 — the shape issue #648 mispriced.
    """
    cfg = default_config("atl06")
    cfg.data_source = {
        "reader": "h5coro",
        "driver": "s3",
        "groups": ["g1"],
        "coordinates": {"latitude": "/lat", "longitude": "/lon"},
        "variables": {"h_li": "/h", "s_li": "/s", "delta_time": "/dt"},
    }
    cfg.output["store_layout"] = "hive"
    cfg.output["windowing"] = {
        "schedule": "yearly",
        "time_field": "delta_time",
        "epoch": "2018-01-01T00:00:00Z",
        "scale": "gps",
    }
    if unit is not None:
        cfg.output["windowing"]["unit"] = unit
    validate_config(cfg)
    return cfg


def _raster_cfg(store, layout="hive"):
    """A windowed raster config -- the ``RasterStrategy.run`` call site.

    ``layout="flat"`` is the shape ``validate_config`` refuses (windowing is
    hive-only) but ``max_cost_preview`` still prices, since it validates
    nothing.
    """
    from zagg.config import load_config_from_dict

    return load_config_from_dict(
        {
            "data_source": {
                "reader": "raster",
                "bands": {"red": {"asset": "red", "dtype": "uint16"}},
            },
            "output": {
                "grid": {"type": "healpix", "parent_order": 9, "child_order": 19},
                "store": store,
                "store_layout": layout,
                "windowing": {"schedule": "monthly"},
            },
        }
    )


#: s3 store for the lambda-backend drives below (the backend refuses a local path).
_STORE = "s3://out/x.zarr"


def _lambda_dispatch_env(monkeypatch, cells=4):
    """Stub the AWS seams of the lambda fan-out, leaving the real dispatch.

    Mirrors the ``lambda_env`` fixture of ``tests/test_runner_concurrency.py``
    (the harness behind its ``test_on_progress_fires_per_completed_shard``)
    plus the hive lifecycle's invokes, so ``notebook.run`` drives the real
    :func:`~zagg.runner._run_lambda` -- its own expansion, its own
    ``n = len(cells)``, its own per-completion ticks -- with nothing but the
    invokes mocked.
    """
    from unittest.mock import MagicMock

    import boto3

    import zagg.grids as grids_mod
    from zagg import hive, runner
    from zagg.concurrency import ConcurrencyReport

    grid = MagicMock()
    grid.signature.return_value = {}
    grid.spatial_signature.return_value = {}
    grid.block_index.side_effect = lambda k: (k,)
    monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: grid)
    monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
    monkeypatch.setattr(
        runner,
        "get_nsidc_s3_credentials",
        lambda: {"accessKeyId": "AKIA", "secretAccessKey": "s", "sessionToken": "t"},
    )
    monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 900)
    monkeypatch.setattr(runner, "_invoke_lambda_ping", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_invoke_lambda_setup_async", lambda *a, **k: "full")
    monkeypatch.setattr(runner, "_invoke_lambda_icechunk_init", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
    # The hive manifest checker (issue #274) polls the output store; keep the
    # run read-free rather than reaching for a bucket that is not there
    # (``tests/test_client.py`` does the same for its parity drives).
    monkeypatch.setattr(hive, "read_manifest", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "compute_available_workers",
        lambda requested, *a, **k: (
            cells,
            ConcurrencyReport(
                account_limit=1000,
                current_concurrent=0,
                padding=100,
                available=900,
                function_reserved=None,
            ),
        ),
    )
    monkeypatch.setattr(
        runner,
        "_invoke_lambda_cell",
        lambda *a, **k: {
            "status_code": 200,
            "body": {"total_obs": 5},
            "error": None,
            "lambda_duration": 1.0,
            "shard_key": 0,
        },
    )


def _preview_stub():
    return {
        "n_units": 4,
        "memory_gb": 4.0,
        "arch": "arm64",
        "timeout_s": 900,
        "max_cost_usd": 0.192,
    }


def _lambda_summary():
    return {
        "backend": "lambda",
        "store_path": "s3://bucket/x.zarr",
        "total_cells": 3,
        "cells_with_data": 2,
        "cells_error": 1,
        "total_obs": 100,
        "wall_time_s": 12.5,
        "lambda_time_s": 30.0,
        "cost": {"max_cost_usd": 0.5, "estimated_cost_usd": None, "actual_cost_usd": 0.01},
        "results": [
            {"shard_key": 1, "status_code": 200, "error": None},
            {"shard_key": 2, "status_code": 200, "error": "No granules found"},
            {"shard_key": 3, "status_code": 500, "error": "boom <tag>"},
        ],
    }


class TestMaxCostPreview:
    def test_preview_from_in_tree_shardmap(self):
        cfg = default_config("atl06")
        preview = notebook.max_cost_preview(cfg, SHARDMAP)
        assert preview["n_units"] == 4
        assert preview["memory_gb"] == 4.0
        assert preview["arch"] == "arm64"
        assert preview["timeout_s"] == 900
        rate = LAMBDA_PRICE_PER_GB_SEC_BY_ARCH["arm64"]
        assert preview["max_cost_usd"] == pytest.approx(4 * rate * 4.0 * 900)

    def test_max_cells_clamps_units(self):
        cfg = default_config("atl06")
        preview = notebook.max_cost_preview(cfg, SHARDMAP, max_cells=2)
        assert preview["n_units"] == 2

    def test_worker_variant_scales_ceiling(self):
        cfg = default_config("atl06")
        base = notebook.max_cost_preview(cfg, SHARDMAP)
        cfg.worker = {"memory": 8192}
        assert notebook.max_cost_preview(cfg, SHARDMAP)["max_cost_usd"] == pytest.approx(
            2 * base["max_cost_usd"]
        )

    def test_no_catalog_raises(self):
        cfg = default_config("atl06")
        cfg.catalog = None
        with pytest.raises(ValueError, match="catalog"):
            notebook.max_cost_preview(cfg)

    def test_format_max_cost_line(self):
        line = notebook.format_max_cost(_preview_stub())
        assert "Max cost ceiling: ~$0.19" in line
        assert "4 units x 4 GB x 900s, arm64" in line

    def test_temporal_config_has_no_shardmap_ceiling(self):
        # A temporal run's fan-out unit is the event, not a catalog shard, so
        # there is no shardmap ceiling to preview -- raise rather than bill a
        # meaningless shard-based figure (issue #298 fold).
        cfg = default_config("atl06")
        cfg.pipeline = {"type": "temporal"}
        with pytest.raises(ValueError, match="temporal runs take events="):
            notebook.max_cost_preview(cfg, SHARDMAP)

    def test_raster_nonwindowed_counts_selected_cells(self, monkeypatch):
        # reader: raster on a flat (non-hive) layout fans out one unit per
        # selected cell (cells == _select_cells); the spatial windowed
        # expansion must never run on a raster config.
        from zagg import runner

        cfg = default_config("atl06")
        cfg.data_source["reader"] = "raster"
        cfg.output["store_layout"] = "flat"
        monkeypatch.setattr(
            runner,
            "_windowed_units",
            lambda *a, **k: pytest.fail("spatial _windowed_units ran on a raster config"),
        )
        preview = notebook.max_cost_preview(cfg, SHARDMAP)
        assert preview["n_units"] == 4

    def test_windowed_raster_uses_raster_units(self, monkeypatch):
        # A windowed hive raster mirrors RasterStrategy.run: the (shard, window)
        # count comes from _raster_windowed_units, never the spatial
        # _windowed_units -- their membership rules differ (issue #247 fold).
        from zagg import runner

        cfg = default_config("atl06")
        cfg.data_source["reader"] = "raster"
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "monthly"}
        monkeypatch.setattr(
            runner, "_raster_windowed_units", lambda cells, windowing: [object()] * 7
        )
        monkeypatch.setattr(
            runner,
            "_windowed_units",
            lambda *a, **k: pytest.fail("spatial _windowed_units ran on a raster config"),
        )
        preview = notebook.max_cost_preview(cfg, SHARDMAP)
        assert preview["n_units"] == 7

    def test_unvalidated_flat_windowed_raster_counts_selected_cells(self):
        # The raster arm's non-hive branch (issue #648 fold): windowing is
        # hive-only for any config that passed ``validate_config``, but this
        # function validates nothing and the CLI gate prices through it before
        # ``agg`` validates -- so the branch is live defense, and it counts
        # cells exactly as the pre-#648 preview did rather than fanning out on
        # a layout the run is about to refuse.
        cfg = _raster_cfg("s3://out/r.zarr", layout="flat")
        with pytest.raises(ValueError, match="store_layout: hive"):
            validate_config(cfg)
        assert notebook.max_cost_preview(cfg, SHARDMAP)["n_units"] == 4

    def test_windowed_shard_unit_prices_one_invoke_per_shard(self):
        # issue #648: ``unit: shard`` (the #586 default) emits every window
        # from ONE invoke, so the ceiling is per shard. The preview took the
        # per-window expansion unconditionally and overcounted by the window
        # count -- 8x on this fixture, ~9x on the 0.60.0 yearly templates.
        preview = notebook.max_cost_preview(_windowed_cfg(), SHARDMAP)
        assert preview["n_units"] == 4

    def test_windowed_window_unit_prices_one_invoke_per_shard_window(self):
        preview = notebook.max_cost_preview(_windowed_cfg(unit="window"), SHARDMAP)
        assert preview["n_units"] == 32  # 4 shards x 8 yearly windows

    @pytest.mark.parametrize("name", WINDOWED_TEMPLATES)
    def test_packaged_windowed_template_preview_equals_its_dispatch(self, monkeypatch, name):
        # issue #648's third test bullet, on the templates the bug actually
        # shipped on: issue #638 made these yearly-windowed from birth, and the
        # reported probe was one of them. The hand-built ``_windowed_cfg``
        # cannot catch a template whose windowing block resolves to something
        # the classifier reads differently, so preview the PACKAGED config
        # (the ``WINDOWED_TEMPLATES`` list of tests/test_live_store_templates.py)
        # and compare it against the real lambda dispatch's ticks.
        ticks = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                ticks.append((done, total))

            def close(self):
                pass

        cfg = default_config(name)
        preview = notebook.max_cost_preview(cfg, SHARDMAP)
        assert preview["n_units"] == 4  # one invoke per shard, NOT per window
        _lambda_dispatch_env(monkeypatch)
        monkeypatch.setattr(notebook, "_make_progress", lambda total, desc="shards": _Recorder())
        notebook.run(cfg, catalog=SHARDMAP, store=_STORE, backend="lambda")
        assert len(ticks) == preview["n_units"]
        assert ticks[-1] == (4, 4)

    @pytest.mark.parametrize(
        "cfg_fn",
        [
            lambda: default_config("atl06"),
            _windowed_cfg,
            lambda: _windowed_cfg(unit="window"),
        ],
        ids=["unwindowed", "unit-shard", "unit-window"],
    )
    def test_preview_equals_the_dispatched_unit_count(self, cfg_fn):
        # The docstring's promise, pinned (issue #648): the previewed ceiling
        # equals the one the run logs before fan-out. Scope, because both
        # sides read the same helper (issue #648 fold): this pins only that
        # the preview adds no step of its own on top of it -- a WRONG
        # expansion moves both sides together and still passes. The
        # arithmetic itself is guarded by the two counting tests above
        # (4 vs 32) and by the packaged-template drive.
        from zagg import runner

        cfg = cfg_fn()
        cells = runner._select_cells(runner._load_catalog(SHARDMAP))
        dispatched = len(runner._expand_units(cells, cfg))
        assert notebook.max_cost_preview(cfg, SHARDMAP)["n_units"] == dispatched

    def test_every_dispatch_path_expands_through_the_shared_helper(self, monkeypatch, tmp_path):
        # The identity above is only worth anything while EVERY path that
        # counts units still routes through _expand_units: one that inlined
        # its own fan would drift silently. Pinned behaviorally (issue #648
        # fold) with a sentinel in place of the helper, aborting each path AT
        # the expansion the way ``tests/test_client.py``'s ``_probe_fd_bound``
        # aborts at the probe -- a source-text assertion passed on a
        # commented-out or result-discarded call and failed on a rename.
        # ``max_cost_preview`` is in the loop too: it is the call site that
        # actually regressed.
        from zagg import hive, runner
        from zagg.processing import raster as raster_mod

        class _ExpansionReachedError(Exception):
            pass

        seen = []

        def _sentinel(cells, config):
            seen.append(list(cells))
            raise _ExpansionReachedError

        monkeypatch.setattr(runner, "_expand_units", _sentinel)
        _lambda_dispatch_env(monkeypatch)
        # The local dispatcher writes the hive manifest before it expands, and
        # the raster path reads the catalog's acquisition datetimes; neither is
        # what this test is about.
        monkeypatch.setattr(hive, "build_manifest", lambda *a, **k: {})
        monkeypatch.setattr(hive, "ensure_manifest", lambda store, manifest, **k: manifest)
        monkeypatch.setattr(raster_mod, "raster_time_index", lambda granules, **k: ({"dt": 0}, [0]))
        drives = {
            "preview": lambda: notebook.max_cost_preview(_windowed_cfg(), SHARDMAP),
            "local": lambda: runner.agg(
                _windowed_cfg(), catalog=SHARDMAP, store=str(tmp_path / "l.zarr"), backend="local"
            ),
            "lambda": lambda: runner.agg(
                _windowed_cfg(), catalog=SHARDMAP, store=_STORE, backend="lambda"
            ),
            "raster": lambda: runner.agg(
                _raster_cfg(str(tmp_path / "r.zarr")), catalog=SHARDMAP, backend="local"
            ),
        }
        for name, drive in drives.items():
            with pytest.raises(_ExpansionReachedError):
                drive()
            assert len(seen) == 1, name
            assert len(seen.pop()) == 4, name  # the run's selected cells, unexpanded


class TestProgressFactory:
    def test_tqdm_present_returns_bar(self):
        # tqdm rides the test env's dependency closure (earthaccess -> pqdm).
        progress = notebook._make_progress(10)
        assert isinstance(progress, notebook._TqdmProgress)
        progress.update(3, 10, 1.5)
        assert progress._bar.n == 3
        progress.update(10, 10, None)
        assert progress._bar.n == 10
        progress.close()

    def test_tqdm_bar_adopts_runner_total(self):
        # An unsized wrapper (total unknown up front) adopts the callback's
        # authoritative total on the first tick.
        progress = notebook._make_progress(0)
        progress.update(1, 7, None)
        assert progress._bar.total == 7
        progress.close()

    def test_tqdm_absent_falls_back_to_logging(self, monkeypatch, caplog):
        # None in sys.modules makes `from tqdm.auto import tqdm` raise
        # ImportError -- the "not installed" path without uninstalling.
        monkeypatch.setitem(sys.modules, "tqdm", None)
        monkeypatch.setitem(sys.modules, "tqdm.auto", None)
        progress = notebook._make_progress(10)
        assert isinstance(progress, notebook._LogProgress)
        with caplog.at_level("INFO", logger="zagg.notebook"):
            progress.update(3, 10, 0.5)
            progress.update(10, 10, 1.25)
        assert "3/10" in caplog.text
        assert "10/10" in caplog.text
        assert "$1.25" in caplog.text
        progress.close()

    def test_logging_fallback_handles_unmetered_cost(self, monkeypatch, caplog):
        monkeypatch.setitem(sys.modules, "tqdm", None)
        monkeypatch.setitem(sys.modules, "tqdm.auto", None)
        progress = notebook._make_progress(2)
        with caplog.at_level("INFO", logger="zagg.notebook"):
            progress.update(2, 2, None)
        assert "2/2" in caplog.text
        assert "$" not in caplog.text


class TestRunView:
    def test_repr_html_renders_cost_block(self):
        html_out = notebook.RunView(_lambda_summary())._repr_html_()
        assert "$0.5000" in html_out  # max ceiling
        assert "n/a" in html_out  # estimated: None placeholder until #297/#299
        assert "$0.0100" in html_out  # actual rollup

    def test_repr_html_failures_escaped_and_benign_excluded(self):
        html_out = notebook.RunView(_lambda_summary())._repr_html_()
        assert "boom &lt;tag&gt;" in html_out
        # Benign "nothing to do" outcomes are not failures (matches the
        # runner's error counting).
        assert "No granules found" not in html_out

    def test_local_summary_shows_no_metered_cost(self):
        view = notebook.RunView(
            {
                "backend": "local",
                "store_path": "./x.zarr",
                "total_cells": 1,
                "cells_with_data": 1,
                "cells_error": 0,
                "total_obs": 5,
                "wall_time_s": 1.0,
                "results": [],
            }
        )
        html_out = view._repr_html_()
        assert "no metered cost" in html_out
        assert "$" not in html_out

    def test_dict_access_delegates(self):
        view = notebook.RunView(_lambda_summary())
        assert view["backend"] == "lambda"
        assert view.get("missing") is None
        assert "with_data=2" in repr(view)


class TestRunWrapper:
    def test_drives_progress_from_agg_callback(self, monkeypatch):
        recorded = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                recorded.append((done, total, cost_usd))

            def close(self):
                recorded.append("closed")

        def fake_agg(config, *, on_progress=None, **kwargs):
            for i in (1, 2):
                on_progress(i, 2, 0.25 * i)
            return _lambda_summary()

        monkeypatch.setattr("zagg.runner.agg", fake_agg)
        monkeypatch.setattr(notebook, "_make_progress", lambda total, desc="shards": _Recorder())
        view = notebook.run(default_config("atl06"), catalog=SHARDMAP, backend="local")
        assert isinstance(view, notebook.RunView)
        assert recorded == [(1, 2, 0.25), (2, 2, 0.5), "closed"]

    def test_lambda_backend_prints_ceiling_and_never_prompts(self, monkeypatch, capsys):
        # The notebook path is informational only (ratified on issue #298):
        # any input() call is a failure.
        import builtins

        def _no_prompt(*a, **k):
            raise AssertionError("notebook path must never prompt")

        monkeypatch.setattr(builtins, "input", _no_prompt)
        monkeypatch.setattr("zagg.runner.agg", lambda config, **k: _lambda_summary())
        view = notebook.run(default_config("atl06"), catalog=SHARDMAP, backend="lambda")
        out = capsys.readouterr().out
        assert "Max cost ceiling" in out
        assert view["backend"] == "lambda"

    def test_progress_closed_when_agg_raises(self, monkeypatch):
        closed = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                pass

            def close(self):
                closed.append(True)

        def fake_agg(config, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr("zagg.runner.agg", fake_agg)
        monkeypatch.setattr(notebook, "_make_progress", lambda total, desc="shards": _Recorder())
        with pytest.raises(RuntimeError, match="boom"):
            notebook.run(default_config("atl06"), catalog=SHARDMAP, backend="local")
        assert closed == [True]

    def test_events_run_sizes_bar_from_event_count(self, monkeypatch):
        totals = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                pass

            def close(self):
                pass

        def _factory(total, desc="shards"):
            totals.append(total)
            return _Recorder()

        monkeypatch.setattr("zagg.runner.agg", lambda config, **k: {"backend": "local"})
        monkeypatch.setattr(notebook, "_make_progress", _factory)
        notebook.run(default_config("atl06"), events=[{"event_key": "a"}, {"event_key": "b"}])
        assert totals == [2]

    def test_events_generator_not_exhausted_by_count(self, monkeypatch):
        # A one-shot generator must survive to agg: run() counts it once for the
        # bar total, then rebinds the materialized list so TemporalStrategy's
        # list(events) still sees every event (issue #298 fold).
        totals = []
        seen = {}

        class _Recorder:
            def update(self, done, total, cost_usd):
                pass

            def close(self):
                pass

        def _factory(total, desc="shards"):
            totals.append(total)
            return _Recorder()

        def _fake_agg(config, *, events=None, on_progress=None, **k):
            seen["events"] = list(events)
            return {"backend": "local"}

        monkeypatch.setattr("zagg.runner.agg", _fake_agg)
        monkeypatch.setattr(notebook, "_make_progress", _factory)
        gen = (e for e in ({"event_key": "a"}, {"event_key": "b"}, {"event_key": "c"}))
        notebook.run(default_config("atl06"), events=gen)
        assert totals == [3]
        assert seen["events"] == [
            {"event_key": "a"},
            {"event_key": "b"},
            {"event_key": "c"},
        ]

    @pytest.mark.parametrize(
        ("cfg_fn", "units", "noun"),
        [
            (lambda: default_config("atl06"), 4, "shards"),
            (_windowed_cfg, 4, "shards"),
            (lambda: _windowed_cfg(unit="window"), 32, "shard-windows"),
        ],
        ids=["unwindowed", "unit-shard", "unit-window"],
    )
    def test_bar_is_sized_and_labelled_in_the_dispatch_unit(self, monkeypatch, cfg_fn, units, noun):
        # issue #648: the bar total came from the per-window preview, so a
        # bulk windowed run of ONE invoke pre-sized to 0/9 and jumped when it
        # returned. Total is now the invoke count, and the label follows the
        # unit -- a bar saying "shards" must not tick per (shard, window).
        # Driven through the REAL ``_run_lambda`` with only the invokes stubbed
        # (issue #648 fold): the ticks come from the dispatch, and the expected
        # counts are hand-written (4 shards, x8 yearly windows on this
        # fixture), so neither side is re-derived from ``_expand_units``.
        sized = []
        ticks = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                ticks.append((done, total))

            def close(self):
                pass

        def _factory(total, desc="shards"):
            sized.append((total, desc))
            return _Recorder()

        _lambda_dispatch_env(monkeypatch)
        monkeypatch.setattr(notebook, "_make_progress", _factory)
        notebook.run(cfg_fn(), catalog=SHARDMAP, store=_STORE, backend="lambda")
        assert sized == [(units, noun)]
        # Ends exactly at its pre-sized total: no stall short, no overshoot.
        assert ticks[-1] == (units, units)
        assert len(ticks) == units

    def test_events_bar_is_labelled_in_events(self, monkeypatch):
        sized = []

        class _Recorder:
            def update(self, done, total, cost_usd):
                pass

            def close(self):
                pass

        monkeypatch.setattr("zagg.runner.agg", lambda config, **k: {"backend": "local"})
        monkeypatch.setattr(
            notebook,
            "_make_progress",
            lambda total, desc="shards": sized.append(desc) or _Recorder(),
        )
        notebook.run(default_config("atl06"), events=[{"event_key": "a"}])
        assert sized == ["events"]


class TestConfirmMaxCost:
    def test_yes_answer_proceeds(self, capsys):
        assert notebook.confirm_max_cost(_preview_stub(), prompt=lambda q: "y") is True
        assert "Max cost ceiling" in capsys.readouterr().out

    def test_default_empty_answer_declines(self, capsys):
        assert notebook.confirm_max_cost(_preview_stub(), prompt=lambda q: "") is False

    def test_no_answer_declines(self, capsys):
        assert notebook.confirm_max_cost(_preview_stub(), prompt=lambda q: "n") is False

    def test_assume_yes_skips_prompt_but_prints(self, capsys):
        def _boom(q):
            raise AssertionError("--yes must skip the prompt")

        assert notebook.confirm_max_cost(_preview_stub(), assume_yes=True, prompt=_boom) is True
        assert "Max cost ceiling" in capsys.readouterr().out


class TestCliGate:
    """The __main__ gate blocks a Lambda fan-out until confirmed; --yes skips."""

    def _drive(self, monkeypatch, argv, answer=None, assume_called=None):
        import zagg.__main__ as cli

        called = {"agg": False}

        def _fake_agg(config, **kwargs):
            called["agg"] = True
            return _lambda_summary()

        cfg = default_config("atl06")
        cfg.catalog = SHARDMAP
        monkeypatch.setattr(cli, "agg", _fake_agg)
        monkeypatch.setattr(cli, "load_config", lambda path: cfg)
        if answer is not None:
            import builtins

            monkeypatch.setattr(builtins, "input", lambda q: answer)
        monkeypatch.setattr("sys.argv", argv)
        return cli, called

    def test_declined_gate_aborts_before_agg(self, monkeypatch, capsys):
        cli, called = self._drive(
            monkeypatch,
            ["zagg", "--config", "c.yaml", "--backend", "lambda"],
            answer="",
        )
        with pytest.raises(SystemExit) as exc_info:
            cli.main()
        assert exc_info.value.code == 1
        assert called["agg"] is False
        assert "Aborted" in capsys.readouterr().out

    def test_confirmed_gate_proceeds(self, monkeypatch, capsys):
        cli, called = self._drive(
            monkeypatch,
            ["zagg", "--config", "c.yaml", "--backend", "lambda"],
            answer="y",
        )
        cli.main()
        assert called["agg"] is True

    def test_yes_flag_skips_prompt(self, monkeypatch, capsys):
        import builtins

        def _boom(q):
            raise AssertionError("--yes must skip the prompt")

        monkeypatch.setattr(builtins, "input", _boom)
        cli, called = self._drive(
            monkeypatch,
            ["zagg", "--config", "c.yaml", "--backend", "lambda", "--yes"],
        )
        cli.main()
        assert called["agg"] is True
        assert "Max cost ceiling" in capsys.readouterr().out

    def test_local_backend_never_gated(self, monkeypatch, capsys):
        import builtins

        def _boom(q):
            raise AssertionError("local backend must not prompt")

        monkeypatch.setattr(builtins, "input", _boom)
        cli, called = self._drive(monkeypatch, ["zagg", "--config", "c.yaml"])
        cli.main()
        assert called["agg"] is True

    def test_temporal_backend_skips_gate(self, monkeypatch, capsys):
        # Temporal configs have no shardmap ceiling (max_cost_preview raises)
        # and no CLI events source, so the gate skips them outright: no prompt,
        # no ceiling printed, straight to agg (issue #298 fold).
        import builtins

        import zagg.__main__ as cli

        called = {"agg": False}

        def _fake_agg(config, **kwargs):
            called["agg"] = True
            return _lambda_summary()

        def _boom(q):
            raise AssertionError("temporal runs have no shardmap ceiling -- no prompt")

        cfg = default_config("atl06")
        cfg.catalog = SHARDMAP
        cfg.pipeline = {"type": "temporal"}
        monkeypatch.setattr(cli, "agg", _fake_agg)
        monkeypatch.setattr(cli, "load_config", lambda path: cfg)
        monkeypatch.setattr(builtins, "input", _boom)
        monkeypatch.setattr("sys.argv", ["zagg", "--config", "c.yaml", "--backend", "lambda"])
        cli.main()
        assert called["agg"] is True
        assert "Max cost ceiling" not in capsys.readouterr().out
