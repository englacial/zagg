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

    def test_semantic_mismatch_is_loud_but_read_only(self, tmp_path, capsys):
        import obstore

        from zagg.config import load_config_from_dict
        from zagg.semantics import semantic_hash
        from zagg.store import open_object_store

        _store(tmp_path)
        other = dict(CONFIG_DICT, aggregation=json.loads(json.dumps(CONFIG_DICT["aggregation"])))
        other["aggregation"]["variables"]["h_min"]["function"] = "max"
        manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
        manifest["semantic_hash"] = semantic_hash(load_config_from_dict(other))
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
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


class TestVersionGate:
    def test_pre_052_classifier_refuses(self, monkeypatch, tmp_path):
        from zagg import semantics

        monkeypatch.setattr(semantics, "COMPOSABILITY_CLASSES", ("none", "approximate", "exact"))
        with pytest.raises(SystemExit, match="packed"):
            tool.main([str(tmp_path), "--config", "unused.yaml"])
