"""Runner-level tests for the raster pipeline (issue #218 phase 4).

``agg(backend="local")`` end-to-end over a ShardMap manifest and synthetic
GeoTIFFs: strategy selection, template emission, shard fan-out, slab writes,
and the shipped ``sentinel2_l2a`` template config.
"""

import json
import logging

import numpy as np
import pytest
from pyproj import CRS, Transformer
from test_raster import ORIGIN, RES, TRANSFORM, UTM18, _index_raster, _write_tiff
from zarr import open_array
from zarr.errors import ContainsGroupError

from zagg.catalog.shardmap import ShardMap
from zagg.config import default_config, load_config_from_dict, validate_config
from zagg.grids import HealpixGrid, from_config
from zagg.runner import agg

T0 = "2026-07-13T16:02:20+00:00"
T1 = "2026-07-18T16:02:20+00:00"


def _cfg(tmp_path):
    return load_config_from_dict(
        {
            "data_source": {
                "reader": "raster",
                "bands": {
                    "red": {
                        "asset": "red",
                        "dtype": "uint16",
                        "fill_value": 0,
                        "scale": 0.0001,
                        "offset": -0.1,
                    }
                },
                "nodata": 0,
            },
            "output": {
                "grid": {"type": "healpix", "parent_order": 10, "child_order": 16},
                "store": str(tmp_path / "out.zarr"),
            },
        }
    )


def _shard_for_raster_at(dx):
    """Order-10 shard covering a 96x96 raster whose origin is offset ``dx`` m east."""
    from mortie import clip2order, geo2mort

    to_wgs = Transformer.from_crs(CRS(UTM18), CRS("EPSG:4326"), always_xy=True)
    lon, lat = to_wgs.transform(ORIGIN[0] + dx + 480.0, ORIGIN[1] - 480.0)
    leaf = geo2mort(np.array([lat]), np.array([lon]), order=29, points=True)
    return int(clip2order(10, leaf)[0])


def _shard_for_raster():
    return _shard_for_raster_at(0.0)


def _entry(gid, href, dt, time_key):
    return {
        "id": gid,
        "s3": None,
        "https": None,
        "assets": {"red": href},
        "datetime": dt,
        "time_key": time_key,
    }


@pytest.fixture
def manifest(tmp_path):
    data = _index_raster()
    _write_tiff(tmp_path / "t0.tif", data)
    _write_tiff(tmp_path / "t1.tif", np.full((96, 96), 555, dtype=np.uint16))
    cfg = _cfg(tmp_path)
    shard = _shard_for_raster()
    grid = from_config(cfg, populated_shards=[shard])
    entries = [
        _entry("g0", str(tmp_path / "t0.tif"), T0, "dt-1"),
        _entry("g1", str(tmp_path / "t1.tif"), T1, "dt-2"),
    ]
    sm = ShardMap(grid.spatial_signature(), [shard], [entries], {"collection": "s2-test"})
    path = str(tmp_path / "shardmap.json")
    sm.to_json(path)
    return cfg, path, shard, data


class TestRasterAgg:
    def test_end_to_end_local(self, tmp_path, manifest):
        cfg, sm_path, shard, data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert summary["total_cells"] == 1
        assert summary["cells_with_data"] == 1
        assert summary["cells_error"] == 0
        assert summary["timesteps"] == 2
        assert summary["total_obs"] == 2

        grid = from_config(cfg, populated_shards=[shard])
        store_path = cfg.output["store"]
        red = open_array(store_path + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        from zagg.processing.raster import _shard_cell_range

        start, stop = _shard_cell_range(grid, shard)
        got = red[0, start:stop]
        np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
        assert (got[~valid] == 0).all()
        assert (red[1, start:stop][valid] == 555).all()
        # scale/offset ride as CF attrs, data stays exact DN.
        assert red.attrs["scale_factor"] == 0.0001
        assert red.attrs["add_offset"] == -0.1
        # time coordinate matches the two datatakes, ascending.
        tarr = open_array(
            store_path + f"/{grid.group_path}/time", zarr_format=3, consolidated=False
        )
        assert tarr.shape == (2,) and tarr[0] < tarr[1]

    def test_debug_logging_emits_stage_stats(self, tmp_path, manifest, caplog):
        # Issue #249, local flavor: stages are collected + logged per shard
        # only when debug logging is on (espg-ratified on the issue).
        cfg, sm_path, _shard, _data = manifest
        with caplog.at_level(logging.DEBUG, logger="zagg.runner"):
            agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        stage_msgs = [r.message for r in caplog.records if "stages:" in r.message]
        assert len(stage_msgs) == 1  # one shard -> one debug line
        assert "'assets': 2" in stage_msgs[0]  # 2 timesteps x 1 band
        assert "'geom_hits': 1" in stage_msgs[0]  # one shared source grid

    def test_no_debug_no_stage_logging(self, tmp_path, manifest, caplog):
        cfg, sm_path, _shard, _data = manifest
        with caplog.at_level(logging.INFO, logger="zagg.runner"):
            agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert not [r for r in caplog.records if "stages:" in r.message]

    def test_local_profile_rolls_up_stages(self, tmp_path, manifest):
        # Issue #250: profile=True rolls the per-shard issue #249 stage stats
        # (plus the timed write bucket) into the summary; off by default so the
        # unprofiled summary stays byte-identical.
        cfg, sm_path, _shard, _data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2, profile=True)
        stages = summary["worker_stage_max"]
        assert set(stages) >= {"open", "geometry", "fetch", "decode", "gather", "write"}
        assert all(v >= 0.0 for v in stages.values())
        counts = summary["worker_stage_counts"]
        assert counts["assets"] == 2  # 2 timesteps x 1 band
        assert summary["template_s"] >= 0.0
        bare = agg(cfg, catalog=sm_path, backend="local", max_workers=2, overwrite=True)
        assert "worker_stage_max" not in bare and "worker_stage_counts" not in bare

    def test_multi_shard_disjoint_slabs(self, tmp_path):
        # Two rasters ~7 km apart feed two DISTINCT order-10 shards in one run;
        # each shard must read back its OWN values at its OWN cell range. A
        # block_index/cells_per_shard off-by-one would surface one shard's DNs
        # in the other's range and fail here (the single-shard fixture can't
        # catch it — there is only one block).
        cfg = _cfg(tmp_path)
        data0 = _index_raster()
        _write_tiff(tmp_path / "s0.tif", data0)
        data1 = np.full((96, 96), 777, dtype=np.uint16)
        _write_tiff(tmp_path / "s1.tif", data1, origin=(ORIGIN[0] + 7000.0, ORIGIN[1]))
        shard0 = _shard_for_raster_at(0.0)
        shard1 = _shard_for_raster_at(7000.0)
        assert shard0 != shard1
        grid = from_config(cfg, populated_shards=[shard0, shard1])
        sm = ShardMap(
            grid.spatial_signature(),
            [shard0, shard1],
            [
                [_entry("g0", str(tmp_path / "s0.tif"), T0, "dt-1")],
                [_entry("g1", str(tmp_path / "s1.tif"), T0, "dt-1")],
            ],
            {"collection": "s2-test"},
        )
        path = str(tmp_path / "sm2.json")
        sm.to_json(path)

        summary = agg(cfg, catalog=path, backend="local", max_workers=2)
        assert summary["total_cells"] == 2
        assert summary["cells_with_data"] == 2
        assert summary["cells_error"] == 0
        assert summary["timesteps"] == 1  # one shared datatake
        assert summary["total_obs"] == 2  # two shard x timestep slabs written

        from zagg.processing.raster import _shard_cell_range

        store_path = cfg.output["store"]
        red = open_array(store_path + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        transform1 = (RES, 0.0, ORIGIN[0] + 7000.0, 0.0, -RES, ORIGIN[1])
        for shard, data, transform in (
            (shard0, data0, TRANSFORM),
            (shard1, data1, transform1),
        ):
            cells = grid.children(shard)
            rows, cols, valid = grid.sample(cells, UTM18, transform, (96, 96))
            assert valid.any()
            start, stop = _shard_cell_range(grid, shard)
            got = red[0, start:stop]
            np.testing.assert_array_equal(got[valid], data[rows[valid], cols[valid]])
            assert (got[~valid] == 0).all()

    def test_signature_mismatch_raises(self, tmp_path, manifest):
        # A ShardMap built under a different grid (parent 9 / child 15) than the
        # run config (parent 10 / child 16) must be refused by _check_signature.
        cfg, _sm_path, shard, _data = manifest
        other = HealpixGrid(9, 15, layout="fullsphere")
        sm = ShardMap(
            other.spatial_signature(),
            [shard],
            [[_entry("g0", "x.tif", T0, "dt-1")]],
            {},
        )
        path = str(tmp_path / "mismatch.json")
        sm.to_json(path)
        with pytest.raises(ValueError, match="different grid"):
            agg(cfg, catalog=path, backend="local")

    def test_overwrite_and_rerun(self, tmp_path, manifest):
        # Rerun/overwrite is the only way to add timesteps today. overwrite=True
        # re-emits the template cleanly (idempotent, then rewrites on a changed
        # time index); overwrite=False refuses to clobber a store whose template
        # differs from the run — a silently-different append is exactly what the
        # single-writer resize path (still future work) must eventually own.
        cfg, sm_path, shard, _data = manifest
        agg(cfg, catalog=sm_path, backend="local", max_workers=2, overwrite=True)
        # A second overwrite=True run over the identical store rewrites cleanly.
        agg(cfg, catalog=sm_path, backend="local", max_workers=2, overwrite=True)

        # A manifest with a DIFFERENT time index: one datatake instead of two.
        grid = from_config(cfg, populated_shards=[shard])
        one = ShardMap(
            grid.spatial_signature(),
            [shard],
            [[_entry("g0", str(tmp_path / "t0.tif"), T0, "dt-1")]],
            {"collection": "s2-test"},
        )
        one_path = str(tmp_path / "one.json")
        one.to_json(one_path)
        # overwrite=False over the existing (2-timestep) store refuses to clobber.
        with pytest.raises(ContainsGroupError):
            agg(cfg, catalog=one_path, backend="local", max_workers=2, overwrite=False)
        # overwrite=True with the changed time index rewrites cleanly (1 step).
        summary = agg(cfg, catalog=one_path, backend="local", max_workers=2, overwrite=True)
        assert summary["timesteps"] == 1

    def test_dry_run(self, manifest):
        cfg, sm_path, _shard, _data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", dry_run=True)
        assert summary["dry_run"] is True and summary["total_cells"] == 1

    def test_all_shards_error_raises(self, tmp_path, manifest):
        # Point every band at an asset key absent from every granule entry, so
        # each shard's sample_item_async raises: the run must fail loudly rather
        # than return a success-shaped (all-fill) summary that a caller ignoring
        # cells_error would read as an empty AOI.
        cfg, sm_path, _shard, _data = manifest
        cfg.data_source["bands"] = {"green": {"asset": "green", "dtype": "uint16", "fill_value": 0}}
        with pytest.raises(RuntimeError, match="all .* raster shard"):
            agg(cfg, catalog=sm_path, backend="local", max_workers=2)

    def test_lambda_backend_requires_s3_store(self, manifest):
        cfg, sm_path, _shard, _data = manifest
        with pytest.raises(ValueError, match="s3://"):
            agg(cfg, catalog=sm_path, backend="lambda")


class _FakeLambdaClient:
    """Scripted boto3 lambda client: records events, returns canned envelopes."""

    def __init__(self, responder):
        self.responder = responder
        self.events = []

    def invoke(self, **kwargs):
        import io

        event = json.loads(kwargs["Payload"])
        self.events.append(event)
        resp = self.responder(event)
        out = {"Payload": io.BytesIO(json.dumps(resp).encode())}
        if resp.get("__function_error__"):
            out["FunctionError"] = "Unhandled"
        return out


def _lifecycle(responder):
    """Wrap a per-shard responder with canned ping/setup answers (issue #264).

    The lambda lifecycle is ping → sync raster setup → fan-out; tests that
    exercise the fan-out answer the two lifecycle invokes canonically and
    route only ``mode="process_raster"`` events to ``responder``.
    """

    def _respond(event):
        if event["mode"] == "ping":
            return {
                "statusCode": 200,
                "body": json.dumps({"ok": True, "mode": "ping", "zagg_version": "test"}),
            }
        if event["mode"] == "setup":
            body = {
                "ok": True,
                "mode": "setup",
                "pipeline": "raster",
                "timesteps": len(event["times_us"]),
            }
            return {"statusCode": 200, "body": json.dumps(body)}
        return responder(event)

    return _respond


class TestRasterLambdaBackend:
    # Issue #264: the lambda path never opens or writes the store from the
    # orchestrator (the template rides the sync setup invoke), so these tests
    # no longer intercept open_store for the s3 URL — a regression to an
    # orchestrator-side write would surface as a real-S3 attempt.

    def test_events_and_summary(self, manifest, monkeypatch, tmp_path):
        import boto3

        cfg, sm_path, shard, _data = manifest

        def responder(event):
            assert event["mode"] == "process_raster"
            assert event["shard_key"] == shard
            assert set(event["time_index"]) == {"dt-1", "dt-2"}
            assert event["config"]["data_source"]["reader"] == "raster"
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2
        )
        assert summary["backend"] == "lambda"
        assert summary["total_cells"] == 1 and summary["cells_with_data"] == 1
        assert summary["cells_error"] == 0
        assert summary["total_obs"] == 2
        assert [e["mode"] for e in fake.events] == ["ping", "setup", "process_raster"]
        # No profile -> the payload stays byte-identical (no key) and the
        # profile rollups stay out of the summary (issue #250).
        assert "profile" not in fake.events[-1]
        assert "worker_stage_max" not in summary
        # Always-on telemetry rollups are null-safe on a body without them.
        assert summary["lambda_time_s"] is None and summary["max_memory_mb"] is None
        assert summary["template_s"] >= 0.0

    def test_lifecycle_order_and_setup_event_pinned(self, manifest, monkeypatch):
        # The wire-level pin of the raster lifecycle (issue #264): ping →
        # sync setup → fan-out, with the EXACT setup event on the wire —
        # config + times_us (plain ints, the catalog-derived coordinate) +
        # overwrite — and no orchestrator store write anywhere.
        import boto3

        import zagg.runner as runner_mod
        from zagg.processing.raster import raster_time_index

        cfg, sm_path, shard, _data = manifest
        _idx, times_us = raster_time_index(runner_mod._load_catalog(sm_path)["granules"])

        def responder(event):
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        real_open = runner_mod.open_store

        def guard_open(path, **kw):
            assert not path.startswith("s3://"), "orchestrator opened the s3 store"
            return real_open(path, **kw)

        monkeypatch.setattr(runner_mod, "open_store", guard_open)
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert [e["mode"] for e in fake.events] == ["ping", "setup", "process_raster"]
        ping, setup = fake.events[0], fake.events[1]
        expected_config = {
            "data_source": cfg.data_source,
            "output": cfg.output,
            "pipeline": cfg.pipeline,
        }
        assert ping["store_path"] == "s3://bucket/out.zarr"
        assert ping["config"] == expected_config
        assert setup == {
            "mode": "setup",
            "store_path": "s3://bucket/out.zarr",
            "overwrite": False,
            "config": expected_config,
            "times_us": [int(t) for t in times_us],
        }
        assert all(isinstance(t, int) for t in setup["times_us"])  # JSON-safe int64

    def test_stale_ping_fails_fast_no_setup(self, manifest, monkeypatch):
        # A pre-#252 function doesn't know mode="ping" (400 fall-through with
        # zero writes): the raster dispatch must raise the redeploy message
        # before the setup invoke or any worker.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            if event["mode"] == "ping":
                return {"statusCode": 400, "body": json.dumps({"error": "shard_key required"})}
            raise AssertionError(f"unexpected invoke after failed ping: {event['mode']}")

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="redeploy"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert [e["mode"] for e in fake.events] == ["ping"]

    def test_stale_setup_echo_fails_fast_no_fanout(self, manifest, monkeypatch):
        # A function that knows ping (#252) but predates the raster setup
        # branch (#264) falls through to the point-path template and echoes
        # "layout" instead of "pipeline": the dispatcher must refuse with the
        # redeploy message before any worker writes into the wrong template.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            if event["mode"] == "ping":
                return {
                    "statusCode": 200,
                    "body": json.dumps({"ok": True, "mode": "ping", "zagg_version": "old"}),
                }
            if event["mode"] == "setup":
                return {
                    "statusCode": 200,
                    "body": json.dumps({"ok": True, "mode": "setup", "layout": "flat"}),
                }
            raise AssertionError(f"unexpected invoke after stale setup: {event['mode']}")

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="issue #264 raster setup branch"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert [e["mode"] for e in fake.events] == ["ping", "setup"]

    def test_lambda_profile_threads_key_and_rolls_up(self, manifest, monkeypatch, tmp_path):
        # Issue #250: profile=True rides the event; the summary rolls the
        # workers' phase_timings (stages straggler-maxed + write bucket,
        # counts summed) and the billed-duration / peak-RSS telemetry.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            assert event["profile"] is True
            body = {
                "timesteps": len(event["time_index"]),
                "cells_with_data": 4096,
                "duration_s": 12.5,
                "max_memory_mb": 2890.0,
                "phase_timings": {
                    "sample": 10.0,
                    "write": 2.5,
                    "stages": {
                        "open": 1.0,
                        "geometry": 0.5,
                        "fetch": 6.0,
                        "decode": 2.0,
                        "gather": 0.5,
                        "assets": 4,
                        "tiles": 12,
                        "geom_hits": 1,
                    },
                },
            }
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        summary = agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            profile=True,
        )
        assert summary["worker_stage_max"] == {
            "open": 1.0,
            "geometry": 0.5,
            "fetch": 6.0,
            "decode": 2.0,
            "gather": 0.5,
            "write": 2.5,
        }
        assert summary["worker_stage_counts"] == {"assets": 4, "tiles": 12, "geom_hits": 1}
        assert summary["lambda_time_s"] == 12.5
        assert summary["worker_max_s"] == 12.5 and summary["worker_median_s"] == 12.5
        assert summary["max_memory_mb"] == 2890.0

    def test_output_credentials_normalized_and_endpoint_threaded(
        self, manifest, monkeypatch, tmp_path
    ):
        # snake_case creds + a custom output endpoint must reach the worker event
        # as the handler-required camelCase block (accessKeyId/secretAccessKey)
        # with endpointUrl threaded in — parity with the spatial/temporal paths.
        # The ping + setup lifecycle invokes (issue #264) carry the same block:
        # the setup invoke is the template WRITE, so a dropped key there would
        # 403 the handler against an external (R2/MinIO) target.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            output_credentials={
                "aws_access_key_id": "AKIA_SNAKE",
                "aws_secret_access_key": "SECRET_SNAKE",
            },
            output_endpoint_url="https://custom.r2.example.com",
        )
        assert [e["mode"] for e in fake.events] == ["ping", "setup", "process_raster"]
        for event in fake.events:
            block = event["output_credentials"]
            assert block["accessKeyId"] == "AKIA_SNAKE"
            assert block["secretAccessKey"] == "SECRET_SNAKE"
            assert block["endpointUrl"] == "https://custom.r2.example.com"

    def test_all_lambda_shards_error_raises(self, manifest, monkeypatch, tmp_path):
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            return {"statusCode": 500, "body": json.dumps({"error": "boom"})}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="boom"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")

    def test_lambda_function_error_is_shard_error(self, manifest, monkeypatch, tmp_path):
        # A Lambda FunctionError (timeout/OOM/unhandled) is a deterministic shard
        # error — recorded, never retried. Single shard -> all-error RuntimeError.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            return {"__function_error__": True, "errorMessage": "boom in worker"}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="Lambda error"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        # deterministic FunctionError -> no retry
        assert [e["mode"] for e in fake.events] == ["ping", "setup", "process_raster"]

    def test_lambda_transient_retry_then_success(self, manifest, monkeypatch, tmp_path):
        # A transient invoke fault (Connection reset) on the first attempt retries
        # with backoff and succeeds on the second -> success, two recorded invokes.
        import boto3

        import zagg.runner as runner_mod

        cfg, sm_path, _shard, _data = manifest
        calls = {"n": 0}

        def responder(event):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("Connection reset by peer")
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        monkeypatch.setattr(runner_mod.time, "sleep", lambda *a: None)
        summary = agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert summary["cells_error"] == 0
        assert summary["cells_with_data"] == 1
        # first shard invoke raised (transient), retried once
        assert [e["mode"] for e in fake.events] == [
            "ping",
            "setup",
            "process_raster",
            "process_raster",
        ]

    def test_lambda_dense_layout_fails_fast(self, manifest, monkeypatch, tmp_path):
        # A dense-layout config on the lambda backend must be refused up front,
        # before any invoke — the fake client records zero events.
        import boto3

        cfg, sm_path, _shard, _data = manifest
        cfg.output["grid"]["layout"] = "dense"

        fake = _FakeLambdaClient(lambda event: {"statusCode": 200, "body": "{}"})
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(ValueError, match="fullsphere"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert fake.events == []

    def test_lambda_datetime_only_time_index(self, tmp_path, monkeypatch):
        # Granules without a time_key fall back to the datetime string as the
        # group key; the worker event's time_index must be keyed by that string.
        import boto3

        cfg = _cfg(tmp_path)
        data = _index_raster()
        _write_tiff(tmp_path / "d0.tif", data)
        shard = _shard_for_raster()
        grid = from_config(cfg, populated_shards=[shard])
        entries = [
            {
                "id": "g0",
                "s3": None,
                "https": None,
                "assets": {"red": str(tmp_path / "d0.tif")},
                "datetime": T0,
            }
        ]
        sm = ShardMap(grid.spatial_signature(), [shard], [entries], {"collection": "s2-test"})
        sm_path = str(tmp_path / "dtonly.json")
        sm.to_json(sm_path)

        seen = {}

        def responder(event):
            seen["time_index"] = event["time_index"]
            body = {"timesteps": 1, "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")
        assert set(seen["time_index"]) == {T0}


class TestInvokeLambdaRasterSetupEvent:
    """Pin the ACTUAL raster setup event on the wire (issue #264) and the
    stale-deployment echo guard: the sync RequestResponse invoke carries
    config + times_us (plain ints) + overwrite, and any success body that
    does not echo ``"pipeline": "raster"`` — a deployed function without the
    raster setup branch fell through to the point-path template — raises the
    redeploy message. Mirrors ``TestInvokeLambdaSetupEvent`` (test_hive)."""

    @staticmethod
    def _invoke(client, config_dict, **kw):
        from zagg.runner import _invoke_lambda_raster_setup

        _invoke_lambda_raster_setup(
            client,
            "process-shard",
            "s3://out/product",
            config_dict=config_dict,
            times_us=np.array([1752422540000000, 1752854540000000], dtype=np.int64),
            **kw,
        )
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_event_matches_baseline(self, tmp_path):
        from test_hive import _wire_client

        cfg = _cfg(tmp_path)
        config_dict = {"data_source": cfg.data_source, "output": cfg.output}
        client = _wire_client({"ok": True, "mode": "setup", "pipeline": "raster", "timesteps": 2})
        event = self._invoke(client, config_dict)
        assert client.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"
        assert event == {
            "mode": "setup",
            "store_path": "s3://out/product",
            "overwrite": False,
            "config": config_dict,
            "times_us": [1752422540000000, 1752854540000000],
        }

    def test_creds_and_overwrite_threaded(self, tmp_path):
        from test_hive import _wire_client

        creds = {"accessKeyId": "AK", "secretAccessKey": "SK"}
        client = _wire_client({"ok": True, "mode": "setup", "pipeline": "raster", "timesteps": 2})
        event = self._invoke(client, {"data_source": {}}, overwrite=True, output_creds_event=creds)
        assert event["overwrite"] is True
        assert event["output_credentials"] == creds

    def test_non_200_raises(self, tmp_path):
        from test_hive import _wire_client

        with pytest.raises(RuntimeError, match="Lambda raster setup error"):
            self._invoke(
                _wire_client({"error": "boom", "mode": "setup"}, status_code=500),
                {"data_source": {}},
            )

    def test_missing_pipeline_echo_raises_redeploy(self, tmp_path):
        # A stale function's flat branch answers 200 with the layout echo but
        # no "pipeline" key: the dispatcher must refuse with the redeploy
        # remedy rather than fan workers into a point-path template.
        from test_hive import _wire_client

        with pytest.raises(RuntimeError, match="redeploy"):
            self._invoke(
                _wire_client({"ok": True, "mode": "setup", "layout": "flat"}),
                {"data_source": {}},
            )


class TestShippedTemplate:
    def test_sentinel2_l2a_config_loads_and_validates(self):
        cfg = default_config("sentinel2_l2a")
        validate_config(cfg)
        assert cfg.data_source["reader"] == "raster"
        assert cfg.data_source["bands"]["scl"]["dtype"] == "uint8"
        assert cfg.output["grid"]["child_order"] == 19
