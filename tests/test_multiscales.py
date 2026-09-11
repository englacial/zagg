"""The ``multiscales`` convention surface: the manifest discovery mirror
(issue #392, spec §4.9) and the stock-tool-legible companion group
(issue #394, spec §4.10)."""

import json

import obstore
import pytest

from zagg.config import PipelineConfig
from zagg.grids.healpix import HealpixGrid
from zagg.hive import MANIFEST_NAME, build_manifest, ensure_manifest, read_manifest
from zagg.multiscales import (
    ARTIFACT_COLUMN,
    ARTIFACT_OVERVIEW,
    GROUP_ATTR,
    GROUP_NAME,
    LEVEL_ATTR,
    MULTISCALES_SPEC,
    manifest_multiscales,
    multiscales_block,
    write_multiscales_group,
)
from zagg.pyramid import PYRAMID_SPEC_V2, declared_fields, expand_overviews, overview_block_v2
from zagg.store import open_object_store
from zagg.sweep_overview import PYRAMID_SPEC, _update_manifest_pyramid, declare_pyramid
from zagg.sweep_stages import run_finisher

SHARD_ORDER = 2
CELL_ORDER = 4


def _cfg(pyramid=None):
    """A grid-carrying config on the (2, 4) window; ``chunk_inner`` 3."""
    config = PipelineConfig(
        aggregation={
            "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
            "variables": {
                "count": {"function": "len", "dtype": "int32", "fill_value": 0},
                "h_mean": {"function": "mean", "dtype": "float32"},
                "h_tdigest": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest",
                    "inner_shape": [2],
                    "dtype": "float32",
                    "fill_value": 0,
                },
            },
        }
    )
    config.output["grid"] = {
        "type": "healpix",
        "parent_order": SHARD_ORDER,
        "child_order": CELL_ORDER,
        "chunk_inner": 3,
        "sharded": True,
    }
    if pyramid is not None:
        config.output["pyramid"] = pyramid
    return config


def _grid(config):
    return HealpixGrid(SHARD_ORDER, CELL_ORDER, config=config, chunk_inner=3, sharded=True)


def _v2_block(cfg):
    """A real ``/2`` pyramid block through the production grammar path."""
    fields, excluded = declared_fields(cfg)
    knob = cfg.output.get("pyramid") or {}
    levels = expand_overviews([3], parent_order=SHARD_ORDER)
    return overview_block_v2(knob, levels, ("cascade", 1), fields, excluded)


def _v2_manifest():
    """The manifest keys the mirror derivation reads, all well-formed."""
    return {
        "shard_order": SHARD_ORDER,
        "cell_order": CELL_ORDER,
        "dataset": {"short_name": "MS"},
        "pyramid": _v2_block(_cfg(pyramid={"overviews": 3})),
    }


def _flatten(manifest, path, value):
    """Replace one dotted manifest key with ``value`` — a hand-edit mis-paste."""
    *parents, leaf = path.split(".")
    node = manifest
    for parent in parents:
        node = node[parent]
    node[leaf] = value


class TestMultiscalesBlock:
    def test_mirror_projects_the_v2_block(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        ms = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER, name="T")
        assert isinstance(ms, list) and len(ms) == 1  # ome-style: a LIST of one
        (entry,) = ms
        assert entry["spec"] == MULTISCALES_SPEC
        assert entry["name"] == "T"
        # datasets mirror the recorded overviews verbatim, finest first, with
        # the artifact kind: the shard-order entry is the §4.6 column, every
        # ladder entry an ancestor overview.
        assert entry["datasets"] == [
            {"order": 2, "cells": [3], "artifact": ARTIFACT_COLUMN},
            {"order": 1, "cells": [2], "artifact": ARTIFACT_OVERVIEW},
            {"order": 0, "cells": [1], "artifact": ARTIFACT_OVERVIEW},
        ]
        # the flat per-order lookup: keys are exactly the orders present.
        assert entry["order2res"] == {"2": [3], "1": [2], "0": [1]}
        # native source data is the base, never a dataset entry (§4.5: a
        # member at the base data's own order would BE the base data).
        assert entry["base"] == {"order": SHARD_ORDER, "cells": [CELL_ORDER]}
        assert entry["fields"] == {
            "count": "exact",
            "h_mean": "none",
            "h_tdigest": "approximate",
        }
        assert entry["fold"] == {"fold_source": "cascade", "exact_levels": 1}

    def test_leaves_regime_carries_no_exact_levels(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        block["overview"].pop("exact_levels")
        block["overview"]["fold_source"] = "leaves"
        (entry,) = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER)
        assert entry["fold"] == {"fold_source": "leaves"}

    @pytest.mark.parametrize(
        "pyramid",
        [
            None,
            "off",  # hand-edited non-dict
            {"spec": PYRAMID_SPEC, "overview": {"orders": []}},  # declared-off /1
            {"spec": PYRAMID_SPEC, "overview": {"orders": [1, 0], "spacing": 2}},  # /1
            {"spec": PYRAMID_SPEC_V2, "overview": {}},  # /2 marker, no overviews
            {"spec": PYRAMID_SPEC_V2, "overviews": []},  # never-empty rule
            {"spec": "zagg-pyramid/3", "overviews": [{"node": 1, "cells": [2]}]},
        ],
    )
    def test_nothing_to_mirror(self, pyramid):
        assert multiscales_block(pyramid, shard_order=2, cell_order=4) is None

    def test_mirror_is_canonical_json(self):
        block = _v2_block(_cfg(pyramid={"overviews": 3}))
        ms = multiscales_block(block, shard_order=SHARD_ORDER, cell_order=CELL_ORDER)
        assert json.loads(json.dumps(ms)) == ms


class TestManifestMultiscales:
    def test_derives_from_manifest_orders_and_dataset(self):
        cfg = _cfg(pyramid={"overviews": 3})
        manifest = build_manifest(_grid(cfg), dataset={"short_name": "MS", "version": "1"})
        ms = manifest_multiscales(manifest)
        assert ms == manifest["multiscales"]
        assert ms[0]["name"] == "MS"

    def test_missing_orders_warns_never_raises(self, caplog):
        manifest = {"pyramid": {"spec": PYRAMID_SPEC_V2, "overviews": [{"node": 1, "cells": [2]}]}}
        with caplog.at_level("WARNING"):
            assert manifest_multiscales(manifest) is None
        assert "mirror derivation skipped" in caplog.text

    @pytest.mark.parametrize(
        ("path", "value"),
        [
            ("dataset", "MS"),  # flattened to its own short_name
            ("pyramid.overview", "off"),  # family dict flattened to a token
            ("pyramid.overview.fields", {"count": "exact"}),  # the {name: class} summary
        ],
    )
    def test_hand_edited_scalars_warn_never_raise(self, path, value, caplog):
        # Every ``.get`` chain in the derivation raises AttributeError on a
        # scalar, not KeyError/TypeError: the guard must skip the mirror, not
        # crash build_manifest/declare_pyramid (the manifest is hand-editable).
        manifest = _v2_manifest()
        assert manifest_multiscales(manifest) is not None
        _flatten(manifest, path, value)
        with caplog.at_level("WARNING"):
            assert manifest_multiscales(manifest) is None
        assert "mirror derivation skipped" in caplog.text


class TestTemplateTime:
    def test_default_v2_manifest_carries_the_mirror(self):
        # No knob at all: the issue #384 default flip declares /2 at the
        # resolved chunk order, and the mirror rides along.
        manifest = build_manifest(_grid(_cfg()), dataset={"short_name": "MS", "version": "1"})
        assert manifest["pyramid"]["spec"] == PYRAMID_SPEC_V2
        (entry,) = manifest["multiscales"]
        assert entry["order2res"] == {"2": [3], "1": [2], "0": [1]}
        assert entry["datasets"][0] == {"order": 2, "cells": [3], "artifact": ARTIFACT_COLUMN}

    @pytest.mark.parametrize("pyramid", [False, {"orders": [1]}])
    def test_v1_and_declared_off_manifests_carry_no_key(self, pyramid):
        manifest = build_manifest(_grid(_cfg(pyramid=pyramid)), dataset={})
        assert manifest["pyramid"]["spec"] == PYRAMID_SPEC
        assert "multiscales" not in manifest


class TestDeclarePyramid:
    def _store(self, root, cfg):
        ensure_manifest(str(root), build_manifest(_grid(cfg), dataset={"short_name": "MS"}))

    def test_retrofit_installs_the_mirror(self, tmp_path):
        # Template under /1 (explicit orders), retrofit to /2: the mirror
        # appears with the declaration. No leaves on purpose — the field
        # probe skips loudly and declaration remains legal (issue #358).
        self._store(tmp_path, _cfg(pyramid={"orders": [1]}))
        assert "multiscales" not in read_manifest(str(tmp_path))
        summary = declare_pyramid(str(tmp_path), _cfg(pyramid={"overviews": 3}))
        assert summary["updated"] is True and summary["multiscales"] is True
        manifest = read_manifest(str(tmp_path))
        assert manifest["multiscales"] == manifest_multiscales(manifest)
        assert manifest["multiscales"][0]["order2res"] == {"2": [3], "1": [2], "0": [1]}

    def test_identical_redeclaration_is_no_put(self, tmp_path, monkeypatch):
        cfg = _cfg(pyramid={"overviews": 3})
        self._store(tmp_path, cfg)
        puts = []
        real_put = obstore.put
        monkeypatch.setattr(obstore, "put", lambda *a, **k: (puts.append(a), real_put(*a, **k))[1])
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["updated"] is False and summary["previous"] == "identical"
        assert summary["multiscales"] is True
        assert puts == []

    def test_pre_mirror_v2_store_gains_the_key(self, tmp_path):
        # A store declared /2 before issue #392: pyramid block identical,
        # mirror absent — the one "identical" case that still PUTs.
        cfg = _cfg(pyramid={"overviews": 3})
        self._store(tmp_path, cfg)
        manifest = read_manifest(str(tmp_path))
        del manifest["multiscales"]
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        summary = declare_pyramid(str(tmp_path), cfg)
        assert summary["previous"] == "identical" and summary["updated"] is True
        manifest = read_manifest(str(tmp_path))
        assert manifest["multiscales"] == manifest_multiscales(manifest)

    def test_declaring_off_removes_the_mirror(self, tmp_path):
        self._store(tmp_path, _cfg(pyramid={"overviews": 3}))
        assert "multiscales" in read_manifest(str(tmp_path))
        summary = declare_pyramid(str(tmp_path), _cfg(pyramid=False))
        assert summary["updated"] is True and summary["multiscales"] is False
        assert "multiscales" not in read_manifest(str(tmp_path))

    def test_sweep_manifest_rmws_preserve_the_mirror(self, tmp_path):
        # Declaration is not the last writer: two whole-manifest RMWs run
        # after it on a /2 store and dump the WHOLE dict back — the sweep's
        # fail-open ``materialized`` update and the finisher's per-entry
        # actuals. The derived mirror must ride through both byte-for-byte
        # (§4.9: sweep actuals never enter it), and the failure mode is
        # silent, so pin it here rather than trust the two call sites.
        self._store(tmp_path, _cfg(pyramid={"overviews": 3}))
        before = read_manifest(str(tmp_path))["multiscales"]
        assert _update_manifest_pyramid(str(tmp_path), {0: "leaves"}, {})
        manifest = read_manifest(str(tmp_path))
        assert manifest["pyramid"]["overview"]["materialized"]["orders"] == [0]
        assert manifest["multiscales"] == before == manifest_multiscales(manifest)
        actuals = {
            order: {
                "regime": "stage-merge",
                "merges_from_raw": 2,
                "source_children": {"folded": 4, "missing": 0, "unreadable": 0},
            }
            for order in (1, 0)
        }
        out = run_finisher(str(tmp_path), manifest, {}, actuals, run_id="t")
        assert out["manifest_updated"] is True
        final = read_manifest(str(tmp_path))
        assert final["pyramid"]["overviews"][0]["actuals"]["regime"] == "leaf-column"
        assert final["multiscales"] == before == manifest_multiscales(final)


#: The occupied order-2 shards the companion fixtures declare (one parent
#: node at order 1, one at order 0 — base ``-3``).
SHARDS = ("-311", "-312")


def _coverage(root, decimals=SHARDS):
    from zagg.grids.morton import morton_word
    from zagg.hive import build_root_coverage, write_root_coverage

    envelope = build_root_coverage([morton_word(d) for d in decimals], SHARD_ORDER)
    write_root_coverage(str(root), envelope)


def _run_record(root, decimals=SHARDS):
    """One run-record parquet naming ``decimals`` as completed shards (D20)."""
    from zagg.grids.morton import morton_word
    from zagg.telemetry import build_record, flatten_record, write_run_parquet

    rows = [
        flatten_record(
            build_record(shard_key=morton_word(d), metadata={"total_obs": 1, "duration_s": 1.0})
        )
        for d in decimals
    ]
    write_run_parquet(str(root), rows, run_id="r1")


class TestCompanionGroup:
    """The issue #394 stock-tool-legible companion (spec §4.10)."""

    def _store(self, root):
        ensure_manifest(
            str(root),
            build_manifest(
                _grid(_cfg(pyramid={"overviews": 3})), dataset={"short_name": "MS", "version": "1"}
            ),
        )

    #: The §4.10 member maps this geometry derives: the order-1 ancestors of
    #: the two occupied shards collapse to one node, order 0 to the base.
    MEMBERS = {
        "1": {"-31": "-3/1/all.zarr/2"},
        "0": {"-3": "-3/all.zarr/1"},
    }

    def test_companion_is_stock_zarr_walkable(self, tmp_path):
        import zarr

        self._store(tmp_path)
        _coverage(tmp_path)
        summary = write_multiscales_group(str(tmp_path))
        assert summary == {
            "written": True,
            "orders": [1, 0],
            "members": {"1": 1, "0": 1},
            "members_source": "coverage.moc",
            "window": "all",
        }
        # The stock-tool claim, asserted with stock zarr: open the reserved
        # root child, walk the children, read the attrs — zero zagg code.
        group = zarr.open_group(str(tmp_path / GROUP_NAME), mode="r")
        manifest = read_manifest(str(tmp_path))
        assert group.attrs["multiscales"] == manifest["multiscales"]
        stamp = group.attrs[GROUP_ATTR]
        assert stamp["spec"] == MULTISCALES_SPEC
        assert stamp["window"] == "all" and stamp["members_source"] == "coverage.moc"
        assert stamp["generated_at"]
        assert sorted(k for k, _ in group.members()) == ["0", "1"]
        for order, members in self.MEMBERS.items():
            level = group[order].attrs[LEVEL_ATTR]
            assert level["spec"] == MULTISCALES_SPEC
            assert level["order"] == int(order)
            assert level["cells"] == [int(order) + 1]  # the §4.4 ladder: k + d
            assert level["artifact"] == ARTIFACT_OVERVIEW
            assert level["window"] == "all"
            assert level["members"] == members

    def test_metadata_only_no_arrays_no_data(self, tmp_path):
        # §4.10: references, never copies — the companion holds exactly one
        # zarr.json group document per node and nothing else.
        self._store(tmp_path)
        _coverage(tmp_path)
        write_multiscales_group(str(tmp_path))
        objects = sorted(
            str(p.relative_to(tmp_path / GROUP_NAME))
            for p in (tmp_path / GROUP_NAME).rglob("*")
            if p.is_file()
        )
        assert objects == ["0/zarr.json", "1/zarr.json", "zarr.json"]

    def test_consolidated_metadata_inlines_the_children(self, tmp_path):
        # §4.10: one GET of the root document walks the whole tree — the
        # inlined child documents are byte-identical to the standalone ones.
        self._store(tmp_path)
        _coverage(tmp_path)
        write_multiscales_group(str(tmp_path))
        root_doc = json.loads((tmp_path / GROUP_NAME / "zarr.json").read_text())
        consolidated = root_doc["consolidated_metadata"]
        assert consolidated["kind"] == "inline" and consolidated["must_understand"] is False
        for order in ("1", "0"):
            child = json.loads((tmp_path / GROUP_NAME / order / "zarr.json").read_text())
            assert consolidated["metadata"][order] == child
            assert child["zarr_format"] == 3 and child["node_type"] == "group"

    def test_run_record_fallback_discovers_the_same_members(self, tmp_path):
        self._store(tmp_path)
        _run_record(tmp_path)
        summary = write_multiscales_group(str(tmp_path))
        assert summary["members_source"] == "run-records"
        for order, members in self.MEMBERS.items():
            doc = json.loads((tmp_path / GROUP_NAME / order / "zarr.json").read_text())
            assert doc["attributes"][LEVEL_ATTR]["members"] == members

    def test_empty_store_writes_empty_member_sets(self, tmp_path, caplog):
        # Declared-but-unoccupied is legal; the companion says so honestly.
        self._store(tmp_path)
        with caplog.at_level("WARNING"):
            summary = write_multiscales_group(str(tmp_path))
        assert summary["written"] is True and summary["members"] == {"1": 0, "0": 0}
        assert "empty member sets" in caplog.text

    def test_refresh_overwrites_in_place(self, tmp_path):
        # The ratchet self-heal: a later write with more coverage replaces
        # the member sets; the layout never accumulates debris.
        self._store(tmp_path)
        _coverage(tmp_path, decimals=("-311",))
        write_multiscales_group(str(tmp_path))
        _coverage(tmp_path)  # unions in -312
        summary = write_multiscales_group(str(tmp_path))
        assert summary["members"] == {"1": 1, "0": 1}
        doc = json.loads((tmp_path / GROUP_NAME / "1" / "zarr.json").read_text())
        assert doc["attributes"][LEVEL_ATTR]["members"] == self.MEMBERS["1"]

    def test_refuses_a_store_without_a_v2_declaration(self, tmp_path):
        ensure_manifest(
            str(tmp_path), build_manifest(_grid(_cfg(pyramid={"orders": [1]})), dataset={})
        )
        with pytest.raises(ValueError, match="no zagg-pyramid/2"):
            write_multiscales_group(str(tmp_path))
        assert not (tmp_path / GROUP_NAME).exists()

    def test_windowed_without_all_time_is_gated(self, tmp_path, caplog):
        # v1 mirrors the all-time fold only (issue #394 decision (2)): a
        # windowed store must declare all_time to get a companion.
        self._store(tmp_path)
        manifest = read_manifest(str(tmp_path))
        manifest["temporal"] = {"schedule": "yearly", "time_field": "t"}
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        with caplog.at_level("INFO"):
            summary = write_multiscales_group(str(tmp_path))
        assert summary == {"written": False, "reason": "windowed store without all_time"}
        assert not (tmp_path / GROUP_NAME).exists()
        # A legal store shape the finisher re-visits every run: the reason is
        # logged at INFO and returned, never warned about (§4.10).
        (gated,) = [r for r in caplog.records if "declares no all_time" in r.message]
        assert gated.levelname == "INFO"
        manifest["pyramid"]["overview"]["all_time"] = True
        obstore.put(open_object_store(str(tmp_path)), MANIFEST_NAME, json.dumps(manifest).encode())
        assert write_multiscales_group(str(tmp_path))["written"] is True

    def test_finisher_refreshes_the_companion_fail_open(self, tmp_path, monkeypatch):
        # The issue #394 writer seam: the designated finisher refreshes the
        # companion when the run touched shards (gated on by_shard like the
        # root-MOC refresh), and its failure never fails the finisher.
        self._store(tmp_path)
        manifest = read_manifest(str(tmp_path))
        out = run_finisher(str(tmp_path), manifest, {}, {}, run_id="t")
        assert "multiscales_group" not in out  # no shards -> no refresh
        assert not (tmp_path / GROUP_NAME).exists()
        out = run_finisher(str(tmp_path), manifest, {d: None for d in SHARDS}, {}, run_id="t")
        assert out["multiscales_group"] is True
        doc = json.loads((tmp_path / GROUP_NAME / "1" / "zarr.json").read_text())
        # The finisher's own root-MOC refresh (step 1) feeds step 2b.
        assert doc["attributes"][LEVEL_ATTR]["members"] == self.MEMBERS["1"]
        import zagg.multiscales as multiscales

        def boom(*a, **k):
            raise RuntimeError("companion down")

        monkeypatch.setattr(multiscales, "write_multiscales_group", boom)
        out = run_finisher(str(tmp_path), manifest, {d: None for d in SHARDS}, {}, run_id="t")
        assert out["multiscales_group"] is False  # fail-open: finisher survived

    def test_cli_write_multiscales(self, tmp_path, capsys):
        from zagg.sweep import main

        self._store(tmp_path)
        _coverage(tmp_path)
        assert main([str(tmp_path), "--write-multiscales"]) == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["written"] is True and summary["members_source"] == "coverage.moc"

    def test_product_name_multiscales_is_reserved(self):
        from zagg.hive import validate_product_name

        with pytest.raises(ValueError, match="reserved for the multiscales companion"):
            validate_product_name("multiscales")
