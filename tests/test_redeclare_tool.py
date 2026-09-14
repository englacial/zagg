"""``tools/redeclare_dense_ladder.py`` — the S1 dense-ladder re-declaration tool.

Dry-run is the default and must never write; ``--execute`` goes through the
pinned :func:`zagg.sweep_overview.declare_pyramid` path. Store fixtures reuse
the ``TestDeclarePyramid`` helpers (the /1-declared-never-materialized and
declared-off shapes are the live atl03/gedi manifests in miniature).
"""

import json
import sys
from pathlib import Path

import pytest
import yaml

from zagg.hive import MANIFEST_NAME, read_manifest
from zagg.pyramid import PYRAMID_SPEC_V2

REPO = Path(__file__).parent.parent
sys.path.insert(0, str(REPO / "tools"))

import redeclare_dense_ladder as tool  # noqa: E402  (tools/ is not installed)
from test_sweep_overview import _make_leaf, _run_record, _write_manifest  # noqa: E402

#: A minimal FULL pipeline config (``load_config`` validates sections), whose
#: aggregation block mirrors ``test_sweep_overview._leaf_cfg`` so the leaf
#: probe on the shared fixtures passes presence/dtype/shape checks.
CONFIG_DICT = {
    "data_source": {
        "reader": "h5coro",
        "driver": "s3",
        "coordinates": {"latitude": "/lat", "longitude": "/lon"},
        "variables": {"h_ph": "/h"},
    },
    "aggregation": {
        "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
        "variables": {
            "count": {"function": "len", "source": "h_ph", "dtype": "int32", "fill_value": 0},
            "h_mean": {"function": "mean", "source": "h_ph", "dtype": "float32"},
            "h_min": {"function": "min", "source": "h_ph", "dtype": "float32"},
            "h_tdigest": {
                "kind": "ragged",
                "function": "zagg.stats.tdigest.build_tdigest",
                "source": "h_ph",
                "inner_shape": [2],
                "dtype": "float32",
                "fill_value": 0,
            },
        },
    },
    "output": {"store_layout": "flat"},
}

CELLS = {"-311": {0: [1.0, 2.0]}}


def _store(root, *, orders=(1, 0)):
    """A committed-leaf store carrying a prior /1 declaration (or declared-off)."""
    _write_manifest(root, orders=orders)
    _make_leaf(root, "-311", CELLS["-311"])
    _run_record(root, ("-311",))


def _config_yaml(tmp_path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(CONFIG_DICT))
    return str(path)


def _other_config() -> dict:
    """A config differing in the semantic core (a reducer), not in ``output.*``."""
    other = json.loads(json.dumps(CONFIG_DICT))
    other["aggregation"]["variables"]["h_min"]["function"] = "max"
    return other


def _stamp_semantic_hash(root, config_dict: dict) -> None:
    """Give the fixture manifest a ``semantic_hash`` — the live stores carry one,
    so without this every ``--execute`` test runs the unverified (pre-#299) branch."""
    from zagg.config import load_config_from_dict
    from zagg.semantics import semantic_hash

    manifest = json.loads((root / MANIFEST_NAME).read_text())
    manifest["semantic_hash"] = semantic_hash(load_config_from_dict(config_dict))
    (root / MANIFEST_NAME).write_text(json.dumps(manifest))


class TestDryRun:
    def test_prints_diff_and_writes_nothing(self, tmp_path, capsys):
        _store(tmp_path)  # /1 [1, 0], never materialized (the atl03 shape)
        before = (tmp_path / MANIFEST_NAME).read_bytes()
        rc = tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "DRY-RUN: nothing was written" in out
        assert "manifest pyramid diff" in out
        assert '+ "spec": "zagg-pyramid/2"' in out
        assert (tmp_path / MANIFEST_NAME).read_bytes() == before

    def test_ladder_table_lists_every_order_to_zero(self, tmp_path, capsys):
        _store(tmp_path)
        tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])
        out = capsys.readouterr().out
        assert "2 -> [3]" in out and "1 -> [2]" in out and "0 -> [1]" in out

    def test_refuses_without_a_ladder_knob(self, tmp_path):
        # No --overviews and no /2 knob in the config: the derivation falls to
        # /1, which is exactly what this tool exists to replace — refuse.
        _store(tmp_path)
        with pytest.raises(SystemExit, match="derives no /2 ladder"):
            tool.main([str(tmp_path), "--config", _config_yaml(tmp_path)])

    def test_reports_the_leaf_probe_verdict(self, tmp_path, capsys):
        _store(tmp_path)
        tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])
        out = capsys.readouterr().out
        assert "leaf probe: " in out
        assert "REFUSED" not in out

    def test_leaf_probe_refusal_is_a_verdict_not_a_traceback(self, tmp_path, capsys):
        """Field drift refuses ``--execute``; the dry run must say so, not raise."""
        _store(tmp_path)
        drifted = json.loads(json.dumps(CONFIG_DICT))
        drifted["aggregation"]["variables"]["absent_field"] = {
            "function": "mean",
            "source": "h_ph",
            "dtype": "float32",
        }
        path = tmp_path / "drifted.yaml"
        path.write_text(yaml.safe_dump(drifted))
        rc = tool.main([str(tmp_path), "--config", str(path), "--overviews", "3"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "leaf probe: REFUSED — --execute WILL FAIL" in out
        assert "absent_field" in out

    def test_identical_declaration_reports_a_no_op(self, tmp_path, capsys):
        _store(tmp_path)
        argv = [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"]
        tool.main(argv + ["--execute"])
        capsys.readouterr()
        assert tool.main(argv) == 0
        out = capsys.readouterr().out
        assert "IDENTICAL — --execute would be a no-op" in out
        assert "manifest pyramid diff" not in out

    def test_prior_actuals_do_not_fake_a_pending_put(self, tmp_path, capsys):
        """A store already swept once must still dry-run as a no-op.

        ``declare_pyramid`` copies prior ``materialized`` actuals onto the block
        before comparing, so a dry run that diffed the raw derived block would
        print their removal and promise a PUT that ``--execute`` never makes.
        """
        _store(tmp_path)
        argv = [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"]
        tool.main(argv + ["--execute"])
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["pyramid"]["overview"]["materialized"] = {"orders": [2], "cells": {"2": 4}}
        (tmp_path / MANIFEST_NAME).write_text(json.dumps(manifest))
        capsys.readouterr()
        assert tool.main(argv) == 0
        out = capsys.readouterr().out
        assert "preserves them verbatim" in out
        assert "IDENTICAL — --execute would be a no-op" in out
        assert "manifest pyramid diff" not in out

    def test_semantic_mismatch_is_loud_but_read_only(self, tmp_path, capsys):
        _store(tmp_path)
        _stamp_semantic_hash(tmp_path, _other_config())
        before = (tmp_path / MANIFEST_NAME).read_bytes()
        rc = tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])
        assert rc == 0
        assert "MISMATCH" in capsys.readouterr().out
        assert (tmp_path / MANIFEST_NAME).read_bytes() == before


class TestExecute:
    def test_installs_the_dense_ladder_over_v1(self, tmp_path):
        _store(tmp_path)
        rc = tool.main(
            [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3", "--execute"]
        )
        assert rc == 0
        block = read_manifest(str(tmp_path))["pyramid"]
        assert block["spec"] == PYRAMID_SPEC_V2
        assert [e["node"] for e in block["overviews"]] == [2, 1, 0]
        assert "materialized" not in block["overview"]

    def test_installs_over_declared_off(self, tmp_path):
        _store(tmp_path, orders=())  # the gedi shape: orders []
        tool.main(
            [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3", "--execute"]
        )
        block = read_manifest(str(tmp_path))["pyramid"]
        assert block["spec"] == PYRAMID_SPEC_V2
        assert [e["node"] for e in block["overviews"]] == [2, 1, 0]

    def test_execute_refuses_a_semantic_mismatch(self, tmp_path):
        """The fixtures write no ``semantic_hash``; the live manifests do.

        With one present, ``--execute`` must take the *comparing* branch of
        ``_semantic_guard`` and refuse a config that did not build the store —
        the whole reason the tool demands the ORIGINAL δ=4096 config.
        """
        _store(tmp_path)
        _stamp_semantic_hash(tmp_path, _other_config())
        before = (tmp_path / MANIFEST_NAME).read_bytes()
        with pytest.raises(ValueError, match="did not build this store"):
            tool.main(
                [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3", "--execute"]
            )
        assert (tmp_path / MANIFEST_NAME).read_bytes() == before

    def test_execute_matching_semantic_hash_installs(self, tmp_path):
        """The same gate must not false-refuse the intended retrofit: the
        ``output.pyramid`` edit is outside the semantic core."""
        _store(tmp_path)
        _stamp_semantic_hash(tmp_path, CONFIG_DICT)
        tool.main(
            [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3", "--execute"]
        )
        block = read_manifest(str(tmp_path))["pyramid"]
        assert [e["node"] for e in block["overviews"]] == [2, 1, 0]

    def test_execute_refuses_anon(self, tmp_path):
        _store(tmp_path)
        with pytest.raises(SystemExit, match="cannot run with --anon"):
            tool.main(
                [
                    str(tmp_path),
                    "--config",
                    _config_yaml(tmp_path),
                    "--overviews",
                    "3",
                    "--execute",
                    "--anon",
                ]
            )


class TestStoreRootRefusals:
    """The two pre-derivation manifest gates (they sit after the version gate,
    which is what ``TestVersionGate`` short-circuits before reaching them)."""

    def test_no_manifest_is_not_a_hive_store_root(self, tmp_path):
        with pytest.raises(SystemExit, match="not a hive store root"):
            tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])

    def test_manifest_without_orders_refuses(self, tmp_path):
        (tmp_path / MANIFEST_NAME).write_text(json.dumps({"spec": "morton-hive/1"}))
        with pytest.raises(SystemExit, match="declares no shard_order/cell_order"):
            tool.main([str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3"])


class TestAnon:
    def test_anon_threads_skip_signature_into_the_store_read(self, tmp_path, monkeypatch, capsys):
        """``--anon``'s only use is a public-bucket dry run: pin the key name."""
        from zagg import hive

        seen: dict = {}
        real = hive.read_manifest

        def spy(store_root, **store_kwargs):
            seen.update(store_kwargs)
            return real(store_root)

        monkeypatch.setattr(hive, "read_manifest", spy)
        _store(tmp_path)
        rc = tool.main(
            [str(tmp_path), "--config", _config_yaml(tmp_path), "--overviews", "3", "--anon"]
        )
        assert rc == 0
        assert seen == {"region": "us-west-2", "skip_signature": True}
        assert "DRY-RUN: nothing was written" in capsys.readouterr().out


class TestVersionGate:
    def test_pre_052_classifier_refuses(self, monkeypatch, tmp_path):
        from zagg import semantics

        monkeypatch.setattr(semantics, "COMPOSABILITY_CLASSES", ("none", "approximate", "exact"))
        with pytest.raises(SystemExit, match="packed"):
            tool.main([str(tmp_path), "--config", "unused.yaml"])
