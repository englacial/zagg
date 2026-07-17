"""Tests for the raster (S2) release benchmark harness + series (issue #250).

No AWS: manifest/catalog consistency, the pure stage-rollup helper, the series
append core, and the offline ``--dry-run`` plan path. The live dispatch
(``run_target`` with ``dry_run=False``) needs credentials and is exercised
operationally, like the point release leg.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / ".github" / "scripts"
BENCH = REPO / "tests" / "data" / "benchmark"
sys.path.insert(0, str(SCRIPTS))

import raster_series as rs  # noqa: E402
import run_raster_benchmark as rrb  # noqa: E402

# --- manifest + pinned catalog consistency ----------------------------------


def test_raster_targets_manifest_consistent():
    manifest, base = rrb.load_targets(str(BENCH / "targets_raster_neon.json"))
    assert manifest["aoi"]["file"] and (base / manifest["aoi"]["file"]).exists()
    assert manifest["dispatch"]["function_name"] and manifest["dispatch"]["region"]
    assert list(manifest["targets"]) == ["raster_s2_neon_2025"]
    t = manifest["targets"]["raster_s2_neon_2025"]
    assert (base / t["config"]).exists()
    assert (base / t["catalog"]).exists()
    assert t["pipeline"] == "raster"
    # The config must actually route to the raster pipeline and stay on the
    # lambda-compatible layout (fullsphere; no hive until issue #237).
    from zagg.config import get_layout, get_store_layout, load_config

    cfg = load_config(str(base / t["config"]))
    assert (cfg.data_source or {}).get("reader") == "raster"
    assert get_layout(cfg) == "fullsphere"
    assert get_store_layout(cfg) == "flat"


def test_pinned_s2_catalog_carries_raster_entries():
    # The pinned catalog is the fixed granule set (offline, no STAC): 2025
    # Earth Search c1 items over the SERC box, with the raster entry fields
    # (assets + datetime + time_key) ShardMap building and the time index need.
    from zagg.catalog.sources import Catalog

    cat = Catalog.from_geoparquet(str(BENCH / "catalogs" / "cat_s2_neon_2025.parquet"))
    assert len(cat) > 0
    meta = cat.metadata
    assert meta["start_date"] == "2025-01-01" and meta["end_date"] == "2025-12-31"
    assert meta["time_key"] == "s2:datatake_id"
    recs = cat.granule_records()
    assert all(r.get("assets") and r.get("datetime") and r.get("time_key") for r in recs)
    assert set(recs[0]["assets"]) == {"red", "green", "blue", "nir", "scl"}


@pytest.mark.slow
def test_dry_run_builds_shardmap_offline(tmp_path):
    # End-to-end offline: the harness builds the whole-AOI S2 shardmap from the
    # pinned catalog and emits a plan record with null metrics. Slow (mortie
    # shardmap build), gated like the point-leg equivalent.
    out = tmp_path / "m.json"
    rc = rrb.main(
        [
            "--targets",
            str(BENCH / "targets_raster_neon.json"),
            "--dry-run",
            "--event",
            "release",
            "--commit",
            "test",
            "--ref",
            "v0.0.0",
            "--out-json",
            str(out),
            "--artifacts-dir",
            str(tmp_path / "art"),
        ]
    )
    assert rc == 0
    runs = json.loads(out.read_text())
    assert len(runs) == 1
    run = runs[0]
    assert run["n_shards"] > 0
    assert run["grid_size"] == "o9" and run["parent_order"] == 9
    # Dry run dispatches nothing: no stage dicts, and the series read of this
    # record leaves the stage columns null.
    assert "stage_max" not in run
    df = rs.records_to_frame(runs)
    assert df.iloc[0]["stage_fetch_s"] is None or str(df.iloc[0]["stage_fetch_s"]) == "nan"


def test_live_dispatch_requires_store_prefix(tmp_path):
    with pytest.raises(SystemExit, match="store-prefix"):
        rrb.main(
            [
                "--targets",
                str(BENCH / "targets_raster_neon.json"),
                "--event",
                "release",
                "--out-json",
                str(tmp_path / "m.json"),
            ]
        )


# --- stage rollup ------------------------------------------------------------


def _body(stages=None, write=None, timesteps=3, duration=100.0):
    body = {"timesteps": timesteps, "duration_s": duration}
    if stages is not None or write is not None:
        body["phase_timings"] = {"sample": duration - (write or 0.0), "write": write}
        body["phase_timings"]["stages"] = stages
    return body


def test_stage_rollup_maxes_seconds_and_sums_counts():
    b1 = _body(
        stages={
            "open": 10.0,
            "geometry": 2.0,
            "fetch": 50.0,
            "decode": 30.0,
            "gather": 15.0,
            "assets": 40,
            "tiles": 120,
            "geom_hits": 4,
        },
        write=5.0,
    )
    b2 = _body(
        stages={
            "open": 8.0,
            "geometry": 3.0,
            "fetch": 70.0,
            "decode": 20.0,
            "gather": 12.0,
            "assets": 35,
            "tiles": 100,
            "geom_hits": 30,
        },
        write=9.0,
    )
    stage_max, stage_counts = rrb.stage_rollup([b1, b2])
    # Straggler max per stage second (never a sum -- work volume, not wall)...
    assert stage_max == {
        "open": 10.0,
        "geometry": 3.0,
        "fetch": 70.0,
        "decode": 30.0,
        "gather": 15.0,
        "write": 9.0,
    }
    # ...counts are run totals.
    assert stage_counts == {"assets": 75, "tiles": 220, "geom_hits": 34}


def test_stage_rollup_unprofiled_bodies_yield_empty():
    # A pre-#256 worker returns no phase_timings: the rollup must come back
    # empty (-> null series cells), never zero-fake a measurement.
    stage_max, stage_counts = rrb.stage_rollup([{"timesteps": 2, "duration_s": 50.0}])
    assert stage_max == {} and stage_counts == {}
    # An unknown future stage stays out of the rollup (schema-stable).
    odd = _body(stages={"open": 1.0, "warp": 9.0, "assets": 2})
    stage_max2, counts2 = rrb.stage_rollup([odd])
    assert "warp" not in stage_max2 and stage_max2["open"] == 1.0
    assert counts2 == {"assets": 2}


def test_median_helper():
    assert rrb._median([]) is None
    assert rrb._median([3.0]) == 3.0
    assert rrb._median([1.0, 3.0]) == 2.0
    assert rrb._median([1.0, 2.0, 9.0]) == 2.0


# --- dispatch transport (mocked boto, no AWS) -------------------------------


class _FakePayload:
    def __init__(self, text: str):
        self._text = text

    def read(self):
        return self._text.encode("utf-8")


class _FakeLambdaClient:
    """Records each shard's event and replays a scripted invoke envelope."""

    def __init__(self, responder):
        self._responder = responder
        self.events: list[dict] = []

    def invoke(self, **kwargs):
        # boto's invoke uses PascalCase kwargs (FunctionName/InvocationType/Payload).
        event = json.loads(kwargs["Payload"])
        self.events.append(event)
        return self._responder(event)


def _envelope(body: dict, status: int = 200) -> dict:
    # The nested Lambda proxy shape the harness unwraps: outer JSON with a
    # string ``body`` that is itself JSON.
    return {"Payload": _FakePayload(json.dumps({"statusCode": status, "body": json.dumps(body)}))}


def _dispatch_config():
    from types import SimpleNamespace

    return SimpleNamespace(
        data_source={"reader": "raster"}, output={"store_layout": "flat"}, pipeline="raster"
    )


def _install_fake_lambda(monkeypatch, responder) -> _FakeLambdaClient:
    # ``_dispatch_shards`` imports boto3 internally, so patch the attribute.
    client = _FakeLambdaClient(responder)
    monkeypatch.setattr("boto3.client", lambda *a, **k: client)
    return client


def _dispatch_one(monkeypatch, responder, shard_key=1):
    client = _install_fake_lambda(monkeypatch, responder)
    cells = [(shard_key, [{"assets": {"red": "u"}, "time_key": "t1"}])]
    results = rrb._dispatch_shards(
        cells,
        _dispatch_config(),
        {"t1": ["a"], "t2": ["b"]},  # t2 belongs to another shard's slice
        "s3://bucket/x.zarr",
        region="us-west-2",
        function_name="process-shard",
    )
    return client, results


def test_dispatch_shards_happy_path(monkeypatch):
    client, results = _dispatch_one(
        monkeypatch, lambda event: _envelope({"duration_s": 120.0, "timesteps": 3}), shard_key=7
    )
    assert len(results) == 1
    r = results[0]
    assert r["error"] is None and r["body"]["duration_s"] == 120.0
    # The event is the runner's raster envelope: mode/profile/int key, the
    # store path, and ONLY this shard's own time-index slice (never global).
    event = client.events[0]
    assert event["mode"] == "process_raster" and event["profile"] is True
    assert event["shard_key"] == 7 and isinstance(event["shard_key"], int)
    assert event["store_path"] == "s3://bucket/x.zarr"
    assert event["time_index"] == {"t1": ["a"]}


def test_dispatch_shards_function_error(monkeypatch):
    _client, results = _dispatch_one(
        monkeypatch,
        lambda event: {"Payload": _FakePayload("boom traceback"), "FunctionError": "Unhandled"},
    )
    assert results[0]["error"].startswith("Lambda error:") and "body" not in results[0]


def test_dispatch_shards_non_200_surfaces_body_error(monkeypatch):
    _client, results = _dispatch_one(
        monkeypatch, lambda event: _envelope({"error": "shard blew up"}, status=500)
    )
    assert results[0]["error"] == "shard blew up" and "body" not in results[0]


def test_dispatch_shards_exception_becomes_error_string(monkeypatch):
    def _boom(event):
        raise RuntimeError("network down")

    _client, results = _dispatch_one(monkeypatch, _boom)
    assert results[0]["error"] == "network down" and "body" not in results[0]


# --- raster series append core ----------------------------------------------


def _record(commit="c0", target="raster_s2_neon_2025", event="release", **over):
    r = {
        "target": target,
        "timestamp": "2026-07-17T00:00:00Z",
        "commit": commit,
        "ref": "v0.32.0",
        "event": event,
        "pr_number": None,
        "aoi": "NEON SERC AOP box",
        "collection": "sentinel-2-c1-l2a",
        "grid_type": "healpix",
        "grid_size": "o9",
        "parent_order": 9,
        "child_order": 19,
        "n_shards": 4,
        "n_shards_ok": 4,
        "n_shards_error": 0,
        "timesteps": 70,
        "slabs_written": 250,
        "shardmap_build_s": 2.0,
        "template_s": 1.1,
        "lambda_seconds": 2036.0,
        "gb_seconds": 8144.0,
        "cost_usd": 0.1086,
        "total_wall_s": 694.1,
        "fanout_s": 693.0,
        "worker_max_s": 687.0,
        "worker_median_s": 480.0,
        "memory_gb": 4.0,
        "price_per_gb_sec": 1.33334e-05,
        "zagg_version": "0.32.0",
        "per_shard_granules": [10, 20, 25, 30],
        # Distinct values so a column transposition can't pass unnoticed.
        "stage_max": {
            "open": 38.0,
            "geometry": 12.0,
            "fetch": 512.0,
            "decode": 301.0,
            "gather": 154.0,
            "write": 41.0,
        },
        "stage_counts": {"assets": 425, "tiles": 1300, "geom_hits": 81},
    }
    r.update(over)
    return r


def test_flatten_spreads_stages_and_drops_nested():
    flat = rs.flatten_record(_record())
    assert flat["stage_open_s"] == 38.0
    assert flat["stage_fetch_s"] == 512.0
    assert flat["stage_write_s"] == 41.0
    assert flat["count_tiles"] == 1300
    assert "stage_max" not in flat and "stage_counts" not in flat
    assert "per_shard_granules" not in flat
    # Missing dicts (dry run / unprofiled) -> null cells, not a KeyError.
    flat2 = rs.flatten_record(_record(stage_max=None, stage_counts=None))
    assert flat2["stage_decode_s"] is None and flat2["count_assets"] is None


def test_records_to_frame_column_stable_and_unknown_stage_stays_json():
    df = rs.records_to_frame([_record(stage_max={"open": 1.0, "warp": 9.0})])
    assert list(df.columns) == rs.RASTER_COLUMNS
    assert df.iloc[0]["stage_open_s"] == 1.0
    assert "stage_warp_s" not in df.columns


def test_append_dedups_and_main_retains_release_only(tmp_path):
    existing = rs.records_to_frame([_record(commit="c0", cost_usd=0.10)])
    updated = rs.append_records(existing, [_record(commit="c0", cost_usd=0.20)])
    assert len(updated) == 1 and updated.iloc[0]["cost_usd"] == 0.20

    records = [
        _record(commit="c1", event="release"),
        _record(commit="c2", event="merge"),  # dropped: not a release
    ]
    recs = tmp_path / "recs.json"
    recs.write_text(json.dumps(records))
    series = tmp_path / "raster_series.parquet"
    assert rs.main(["--series", str(series), "--records", str(recs)]) == 0
    df = rs.load_series(series)
    assert len(df) == 1 and df.iloc[0]["commit"] == "c1"
