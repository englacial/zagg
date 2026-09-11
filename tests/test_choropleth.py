"""Rollup → GeoJSON choropleth exporter (issue #301).

Pins the exporter's contract on local fixture stores (no live S3): rollup-
preferred resolution with per-feature provenance, the leaf-sidecar fallback
on a partial/stale rollup tree (the live ATL03 demo store's shape), the
``--order`` rollup-node mode, and GeoJSON validity — ``[lon, lat]`` order
(mortie returns ``[lat, lon]``), closed CCW rings, antimeridian splitting,
shapely round-trip. One env-gated slow smoke runs against a published store.
"""

import json
import os

import obstore
import pytest
from shapely.geometry import shape

from zagg.choropleth import export_choropleth, main
from zagg.grids.morton import morton_word
from zagg.hive import MANIFEST_NAME, build_root_coverage, shard_leaf_path, write_root_coverage
from zagg.store import open_object_store
from zagg.sweep import run_sweep
from zagg.telemetry import build_record, merge, write_sidecar

SHARD_ORDER = 2


def _write_manifest(root, shard_order=SHARD_ORDER):
    manifest = {
        "spec": "morton-hive/1",
        "dataset": {"short_name": "TEST", "version": "1"},
        "cell_order": shard_order + 2,
        "shard_order": shard_order,
        "split_schedule": [1] * shard_order,
        "pyramid": {"orders": [], "aggregation": {}},
        "generated_at": "2026-01-01T00:00:00+00:00",
    }
    obstore.put(open_object_store(str(root)), MANIFEST_NAME, json.dumps(manifest).encode())


def _record(decimal, *, n_obs=10, window=None):
    return build_record(
        shard_key=morton_word(decimal),
        metadata={"total_obs": n_obs, "cells_with_data": 2, "duration_s": 0.5},
        granule_ids=[f"g-{decimal}-{window}"],
        window=window,
    )


def _put_leaf(root, decimal, *, window=None, **kwargs):
    leaf = shard_leaf_path(str(root), morton_word(decimal), window=window)
    rec = _record(decimal, window=window, **kwargs)
    write_sidecar(leaf, rec)
    return rec


def _put_coverage(root, decimals):
    envelope = build_root_coverage([morton_word(d) for d in decimals], SHARD_ORDER)
    write_root_coverage(str(root), envelope)


def _store(tmp_path, decimals=("-311", "-312", "-321"), sweep=True):
    _write_manifest(tmp_path)
    recs = {d: _put_leaf(tmp_path, d) for d in decimals}
    _put_coverage(tmp_path, decimals)
    if sweep:
        run_sweep(str(tmp_path), [(morton_word(d), None) for d in decimals], families=("stats",))
    return recs


def _by_morton(collection):
    return {f["properties"]["morton"]: f for f in collection["features"]}


class TestLeafMode:
    def test_swept_store_serves_rollups(self, tmp_path):
        recs = _store(tmp_path)
        fc = export_choropleth(str(tmp_path))
        assert fc["type"] == "FeatureCollection"
        feats = _by_morton(fc)
        assert set(feats) == set(recs)
        for d, rec in recs.items():
            props = feats[d]["properties"]
            assert props["source"] == "rollup"
            assert props["order"] == SHARD_ORDER
            assert props["n_leaves"] == 1
            assert props["n_obs"] == rec["n_obs"]
            assert props["cells_with_data"] == rec["cells_with_data"]
            assert props["n_granules"] == rec["n_granules"]
            assert props["duration_s"] == rec["duration_s"]

    def test_unswept_store_falls_back_to_leaf_sidecars(self, tmp_path):
        recs = _store(tmp_path, sweep=False)
        feats = _by_morton(export_choropleth(str(tmp_path)))
        for d, rec in recs.items():
            props = feats[d]["properties"]
            assert props["source"] == "leaf_stats"
            assert props["n_obs"] == rec["n_obs"]

    def test_windowed_leaves_merge_in_fallback(self, tmp_path):
        _write_manifest(tmp_path)
        recs = [
            _put_leaf(tmp_path, "-311", window=w, n_obs=n) for w, n in (("2019", 3), ("2020", 4))
        ]
        _put_coverage(tmp_path, ["-311"])
        props = _by_morton(export_choropleth(str(tmp_path)))["-311"]["properties"]
        assert props["source"] == "leaf_stats"
        assert props["n_leaves"] == 2
        assert props["n_obs"] == merge(recs)["n_obs"] == 7

    def test_covered_shard_without_stats_is_emitted_null(self, tmp_path):
        _store(tmp_path, decimals=("-311",))
        _put_coverage(tmp_path, ["-311", "-322"])  # -322 has coverage, no artifacts
        feats = _by_morton(export_choropleth(str(tmp_path)))
        props = feats["-322"]["properties"]
        assert props["source"] == "missing"
        assert props["n_obs"] is None and props["est_cost_usd"] is None
        assert shape(feats["-322"]["geometry"]).is_valid

    def test_partial_rollup_tree_mixes_sources(self, tmp_path):
        _store(tmp_path)
        # Drop one shard's rollup: the ATL03 shape (rollups for some leaves only).
        (tmp_path / "-3" / "1" / "2" / "stats.rollup.json").unlink()
        feats = _by_morton(export_choropleth(str(tmp_path)))
        assert feats["-312"]["properties"]["source"] == "leaf_stats"
        assert feats["-311"]["properties"]["source"] == "rollup"

    def test_missing_manifest_or_coverage_raises(self, tmp_path):
        with pytest.raises(ValueError, match="not a hive store root"):
            export_choropleth(str(tmp_path))
        _write_manifest(tmp_path)
        with pytest.raises(ValueError, match="coverage.moc"):
            export_choropleth(str(tmp_path))


class TestOrderMode:
    def test_fresh_interior_rollups_serve_the_coarse_layer(self, tmp_path):
        recs = _store(tmp_path)
        fc = export_choropleth(str(tmp_path), order=1)
        feats = _by_morton(fc)
        assert set(feats) == {"-31", "-32"}
        direct = merge([recs["-311"], recs["-312"]])
        props = feats["-31"]["properties"]
        assert props["source"] == "rollup"
        assert props["order"] == 1
        assert props["n_leaves"] == 2
        assert props["n_obs"] == direct["n_obs"]
        assert props["est_cost_usd"] == direct["est_cost_usd"]

    def test_stale_interior_rollup_folds_from_below(self, tmp_path):
        recs = _store(tmp_path)
        # A later run lands a new shard (sidecar + coverage) with NO re-sweep:
        # the stored -31 rollup's generation (2 leaves) < 3 covered shards.
        recs["-313"] = _put_leaf(tmp_path, "-313", n_obs=100)
        _put_coverage(tmp_path, list(recs))
        feats = _by_morton(export_choropleth(str(tmp_path), order=1))
        props = feats["-31"]["properties"]
        assert props["source"] == "mixed"  # two shard rollups + one leaf fallback
        assert props["n_leaves"] == 3
        assert props["n_obs"] == merge(list(recs.values()))["n_obs"] - recs["-321"]["n_obs"]

    def test_bare_interior_node_folds_children_rollups(self, tmp_path):
        _store(tmp_path)
        # The live ATL03 tree has interior nodes with no rollup object at all.
        (tmp_path / "-3" / "1" / "stats.rollup.json").unlink()
        props = _by_morton(export_choropleth(str(tmp_path), order=1))["-31"]["properties"]
        assert props["source"] == "rollup"  # folded from the shard-node rollups
        assert props["n_leaves"] == 2

    def test_order_bounds_checked(self, tmp_path):
        _store(tmp_path)
        with pytest.raises(ValueError, match="order must be in"):
            export_choropleth(str(tmp_path), order=SHARD_ORDER + 1)


class TestGeometry:
    def test_lonlat_order_and_closed_ccw_ring(self, tmp_path):
        from mortie import mort2polygon

        _store(tmp_path, decimals=("311",))
        geom = _by_morton(export_choropleth(str(tmp_path), step=1))["311"]["geometry"]
        assert geom["type"] == "Polygon"
        ring = geom["coordinates"][0]
        assert ring[0] == ring[-1]
        expected = {(lon, lat) for lat, lon in mort2polygon(morton_word("311"), step=1)}
        assert {(x, y) for x, y in ring} == expected  # swapped, not mortie's (lat, lon)
        poly = shape(geom)
        assert poly.is_valid and poly.exterior.is_ccw

    def test_antimeridian_cell_splits_to_multipolygon(self, tmp_path):
        _store(tmp_path, decimals=("-111",))  # crosses the antimeridian
        geom = _by_morton(export_choropleth(str(tmp_path)))["-111"]["geometry"]
        assert geom["type"] == "MultiPolygon"
        assert len(geom["coordinates"]) == 2
        lons = [x for part in geom["coordinates"] for ring in part for x, _y in ring]
        assert all(-180.0 <= x <= 180.0 for x in lons)
        assert shape(geom).is_valid

    def test_every_feature_shapely_round_trips_through_json(self, tmp_path):
        _store(tmp_path)
        fc = json.loads(json.dumps(export_choropleth(str(tmp_path))))
        for feature in fc["features"]:
            assert shape(feature["geometry"]).is_valid


class TestCli:
    def test_writes_loadable_geojson(self, tmp_path, capsys):
        _store(tmp_path)
        out = tmp_path / "out.geojson"
        assert main([str(tmp_path), "-o", str(out)]) == 0
        fc = json.loads(out.read_text())
        assert len(fc["features"]) == 3
        assert fc["zagg_choropleth"]["order"] == SHARD_ORDER

    def test_order_flag_and_stdout(self, tmp_path, capsys):
        _store(tmp_path)
        assert main([str(tmp_path), "--order", "1"]) == 0
        fc = json.loads(capsys.readouterr().out)
        assert {f["properties"]["morton"] for f in fc["features"]} == {"-31", "-32"}


LIVE_STORE = os.environ.get("ZAGG_CHOROPLETH_LIVE_STORE")


@pytest.mark.slow
@pytest.mark.skipif(
    not LIVE_STORE,
    reason="set ZAGG_CHOROPLETH_LIVE_STORE to a published store root, e.g. "
    "s3://us-west-2.opendata.source.coop/englacial/zagg/demo/atl03_tdigest_o9.zarr",
)
class TestLiveSmoke:
    def test_leaf_and_coarse_exports_are_valid(self):
        from collections import Counter

        kwargs = {"skip_signature": True, "region": "us-west-2"}
        fc = export_choropleth(LIVE_STORE, store_kwargs=kwargs)
        assert fc["features"]
        counts = Counter(f["properties"]["source"] for f in fc["features"])
        print(f"\nlive leaf export: {len(fc['features'])} features, sources {dict(counts)}")
        for feature in fc["features"][:50]:
            assert shape(feature["geometry"]).is_valid
        coarse = export_choropleth(
            LIVE_STORE, order=max(fc["zagg_choropleth"]["shard_order"] - 3, 0), store_kwargs=kwargs
        )
        assert coarse["features"]
        counts = Counter(f["properties"]["source"] for f in coarse["features"])
        print(f"live coarse export: {len(coarse['features'])} features, sources {dict(counts)}")
