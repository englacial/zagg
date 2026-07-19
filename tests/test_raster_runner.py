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
                # Pinned: these tests pin the FLAT (time, cells) write path;
                # since issue #253 an unpinned healpix raster config resolves
                # hive. Hive tests override this to "hive" per test.
                "store_layout": "flat",
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
    grid = from_config(cfg)
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

        grid = from_config(cfg)
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
        grid = from_config(cfg)
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
        grid = from_config(cfg)
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
    #
    # Issue #286: the shard fan-out now DEFAULTS to the async result-object
    # channel; these tests pin ``invocation="sync"`` so they remain the
    # byte-identical synchronous-transport regression suite (no result_url on
    # the shard event, RequestResponse invoke). The async default is covered by
    # TestRasterLambdaAsyncBackend / TestInvokeLambdaRaster.

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
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            invocation="sync",
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
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", invocation="sync")
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
        with pytest.raises(RuntimeError, match="redeploy") as excinfo:
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                invocation="sync",
            )
        # The shared ping is pipeline-neutral (issue #264): a raster operator
        # must not get hive-worded guidance for a raster run.
        assert "hive" not in str(excinfo.value).lower()
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
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                invocation="sync",
            )
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
            invocation="sync",
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
            invocation="sync",
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
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                invocation="sync",
            )

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
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                invocation="sync",
            )
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
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", invocation="sync"
        )
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
        # The dense layout was removed (issue #88): a config still selecting it
        # is refused at grid construction, before any invoke — the fake client
        # records zero events.
        import boto3

        cfg, sm_path, _shard, _data = manifest
        cfg.output["grid"]["layout"] = "dense"

        fake = _FakeLambdaClient(lambda event: {"statusCode": 200, "body": "{}"})
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(ValueError, match="fullsphere"):
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                invocation="sync",
            )
        assert fake.events == []

    def test_lambda_datetime_only_time_index(self, tmp_path, monkeypatch):
        # Granules without a time_key fall back to the datetime string as the
        # group key; the worker event's time_index must be keyed by that string.
        import boto3

        cfg = _cfg(tmp_path)
        data = _index_raster()
        _write_tiff(tmp_path / "d0.tif", data)
        shard = _shard_for_raster()
        grid = from_config(cfg)
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
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", invocation="sync")
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

        # A generic 500 can't be told apart from a raising pre-#264 flat branch,
        # so the message must carry the redeploy hint.
        with pytest.raises(RuntimeError, match="Lambda raster setup error") as excinfo:
            self._invoke(
                _wire_client({"error": "boom", "mode": "setup"}, status_code=500),
                {"data_source": {}},
            )
        assert "redeploy" in str(excinfo.value)

        # The legitimate ContainsGroupError overwrite-refusal is the only non-200
        # a correctly-deployed function returns on the normal path; it must
        # surface the store message WITHOUT the inapplicable redeploy hedge.
        err = "A group exists in store 's3://out/product' at path ''"
        with pytest.raises(RuntimeError, match="Lambda raster setup error") as excinfo:
            self._invoke(
                _wire_client({"error": err, "mode": "setup"}, status_code=500),
                {"data_source": {}},
            )
        msg = str(excinfo.value)
        assert "redeploy" not in msg
        assert "A group exists in store" in msg

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


class TestRasterHiveLocalBackend:
    """Local raster hive runs (issue #247 phase 3): manifest, leaves, coverage."""

    def test_defaulted_layout_dispatches_hive(self, tmp_path, manifest):
        # Issue #253 phase 4: an OMITTED store_layout on a healpix raster
        # config resolves hive (the grid-keyed, pipeline-agnostic default) and
        # dispatches down the issue #247 hive path — manifest + stamped leaf,
        # no flat global template at the store root.
        from pathlib import Path

        from zagg import hive

        cfg, sm_path, shard, _data = manifest
        del cfg.output["store_layout"]  # the fixture pins flat; drop for the default
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert summary["cells_with_data"] == 1 and summary["cells_error"] == 0

        store_path = cfg.output["store"]
        assert hive.read_manifest(store_path)["spec"] == "morton-hive/1"
        stamp = hive.read_commit(hive.shard_leaf_path(store_path, shard))
        assert stamp and stamp["complete"]
        assert "zarr.json" not in {p.name for p in Path(store_path).iterdir()}  # D5

    def test_schedule_none_end_to_end(self, tmp_path, manifest):
        from pathlib import Path

        from zagg import hive

        cfg, sm_path, shard, data = manifest
        cfg.output["store_layout"] = "hive"
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert summary["cells_with_data"] == 1 and summary["cells_error"] == 0

        store_path = cfg.output["store"]
        # Root manifest: /1 spec (schedule none), no temporal block.
        m = hive.read_manifest(store_path)
        assert m["spec"] == "morton-hive/1" and "temporal" not in m
        # No store-root zarr objects (D5): only the manifest, the root
        # coverage.moc, and the digit tree.
        root_children = sorted(p.name for p in Path(store_path).iterdir())
        assert "zarr.json" not in root_children
        assert {"morton_hive.json", "coverage.moc"} <= set(root_children)
        # One bare leaf carrying the FULL time axis, stamped /1.
        leaf = hive.shard_leaf_path(store_path, shard)
        stamp = hive.read_commit(leaf)
        assert stamp and stamp["complete"] and stamp["spec"] == "morton-hive/1"
        assert "window" not in stamp and "time_range" not in stamp
        grid = from_config(cfg)
        red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
        assert red.shape == (2, grid.cells_per_shard)
        cells = grid.children(shard)
        rows, cols, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
        np.testing.assert_array_equal(red[0, :][valid], data[rows[valid], cols[valid]])
        assert (red[1, :][valid] == 555).all()
        # Root coverage.moc (default-on for hive) covers the shard, no
        # time_range on an unwindowed store.
        cov = hive.read_root_coverage(store_path)
        assert cov is not None and "time_range" not in cov
        np.testing.assert_array_equal(
            hive.root_coverage_words(cov), np.asarray([shard], dtype=np.uint64)
        )
        # Stats sidecar (issue #297): SIBLING of the leaf, success only; the
        # local backend carries no lambda config / caller identity.
        from zagg.telemetry import read_sidecar

        record = read_sidecar(leaf)
        assert record["schema_version"] == 1 and record["success"] is True
        assert record["shard_key"] == int(shard)
        assert record["invoked_by"] is None and record["lambda"] is None
        assert {"sample", "write"} <= set(record["phase_timings"])

    def test_windowed_daily_leaves(self, tmp_path, manifest):
        from zagg import hive

        cfg, sm_path, shard, data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        # Two datatakes on different days -> two (shard, window) units.
        assert summary["total_cells"] == 2
        assert summary["cells_with_data"] == 2 and summary["cells_error"] == 0

        store_path = cfg.output["store"]
        m = hive.read_manifest(store_path)
        assert m["spec"] == "morton-hive/2"
        assert m["temporal"]["schedule"] == "daily"
        assert m["temporal"]["time_field"] == "datetime"  # the resolved field
        grid = from_config(cfg)
        for label, instant, value_check in (
            ("20260713", T0, None),
            ("20260718", T1, 555),
        ):
            leaf = hive.shard_leaf_path(store_path, shard, window=label)
            stamp = hive.read_commit(leaf)
            assert stamp and stamp["spec"] == "morton-hive/2"
            assert stamp["window"] == label
            assert stamp["time_range"] == [instant, instant]
            red = open_array(leaf + f"/{grid.group_path}/red", zarr_format=3, consolidated=False)
            assert red.shape == (1, grid.cells_per_shard)
            if value_check is not None:
                cells = grid.children(shard)
                _r, _c, valid = grid.sample(cells, UTM18, TRANSFORM, (96, 96))
                assert (red[0, :][valid] == value_check).all()
        # Root summary unions the two windowed stamps' time ranges (D15).
        cov = hive.read_root_coverage(store_path)
        assert cov["time_range"] == [T0, T1]


class TestRasterHiveLambdaBackend:
    """Lambda raster hive dispatch (issue #247 phase 4): lifecycle + events.

    Pinned ``invocation="sync"`` (issue #286): these exercise the hive
    lifecycle under the synchronous shard transport. The async shard fan-out
    (default) is covered by TestRasterLambdaAsyncBackend.
    """

    def test_lifecycle_and_event_shapes(self, manifest, monkeypatch):
        # The hive manifest rides the issue #252 hybrid lifecycle: ping (read-
        # only precheck) -> async setup (manifest write) -> per-unit process
        # invokes (no time_index; window on windowed units) -> finalize
        # backstop -> fire-and-forget coverage. Scripted responder, no writes.
        import boto3

        cfg, sm_path, shard, _data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}

        def responder(event):
            mode = event["mode"]
            if mode == "ping":
                body = {"ok": True, "mode": "ping", "zagg_version": "test"}
                return {"statusCode": 200, "body": json.dumps(body)}
            if mode in ("setup", "coverage"):
                return {"statusCode": 200, "body": "{}"}  # Event invokes: unread
            if mode == "finalize":
                body = {"ok": True, "mode": "finalize", "layout": "hive"}
                return {"statusCode": 200, "body": json.dumps(body)}
            assert mode == "process_raster"
            label = event["window"]["label"]
            body = {
                "shard_key": event["shard_key"],
                "timesteps": 1,
                "cells_with_data": 7,
                "time_range": {
                    "20260713": [T0, T0],
                    "20260718": [T1, T1],
                }[label],
            }
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        summary = agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            invocation="sync",
        )
        assert summary["total_cells"] == 2  # two (shard, window) units
        assert summary["cells_with_data"] == 2

        modes = [e["mode"] for e in fake.events]
        assert modes[:2] == ["ping", "setup"]
        assert modes[-2:] == ["finalize", "coverage"]
        assert modes[2:-2] == ["process_raster", "process_raster"]
        # Lifecycle events carry the manifest inputs (config + parent_order +
        # dataset identity), mirroring the aggregation hive path.
        for ev in fake.events[:2] + [fake.events[-2]]:
            assert ev["config"]["output"]["store_layout"] == "hive"
            assert ev["parent_order"] == 10
            assert "dataset" in ev
        # Hive process events: no time_index (leaf-local axis), window payload
        # with the daily labels, one unit per (shard, window).
        procs = fake.events[2:-2]
        assert all("time_index" not in ev for ev in procs)
        assert sorted(ev["window"]["label"] for ev in procs) == ["20260713", "20260718"]
        assert all(ev["shard_key"] == shard for ev in procs)
        # Root coverage rides serialized in the event, with the D15 time union.
        cov = fake.events[-1]["coverage"]
        assert cov["encoding"] == "ranges"
        assert cov["time_range"] == [T0, T1]

    def test_defaulted_layout_dispatches_hive(self, manifest, monkeypatch):
        # Issue #253 phase 4, lambda dispatcher: a healpix raster config with
        # NO store_layout key runs the hive lifecycle (ping -> async setup ->
        # hive process events -> finalize/coverage), not the issue #264 flat
        # sync-setup path. The default resolves in get_store_layout on both
        # ends — the events forward the config untouched, no injected key.
        import boto3

        cfg, sm_path, shard, _data = manifest
        del cfg.output["store_layout"]  # the fixture pins flat; drop for the default

        def responder(event):
            mode = event["mode"]
            if mode == "ping":
                body = {"ok": True, "mode": "ping", "zagg_version": "test"}
                return {"statusCode": 200, "body": json.dumps(body)}
            if mode in ("setup", "coverage"):
                return {"statusCode": 200, "body": "{}"}  # Event invokes: unread
            if mode == "finalize":
                body = {"ok": True, "mode": "finalize", "layout": "hive"}
                return {"statusCode": 200, "body": json.dumps(body)}
            assert mode == "process_raster"
            body = {
                "shard_key": event["shard_key"],
                "timesteps": 2,
                "cells_with_data": 7,
                "time_range": [T0, T1],
            }
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        summary = agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            invocation="sync",
        )
        assert summary["cells_with_data"] == 1

        modes = [e["mode"] for e in fake.events]
        assert modes[:2] == ["ping", "setup"]
        assert modes[-2:] == ["finalize", "coverage"]
        procs = [e for e in fake.events if e["mode"] == "process_raster"]
        # Hive-shaped process events: leaf-local time axis, no flat time_index;
        # schedule none -> no window payload.
        assert procs == [e for e in fake.events[2:-2]]
        assert all("time_index" not in ev and "window" not in ev for ev in procs)
        assert all(ev["shard_key"] == shard for ev in procs)
        # No sync flat raster setup: no event carries the flat template's
        # times_us, and the forwarded config still has no store_layout key.
        assert all("times_us" not in ev for ev in fake.events)
        for ev in fake.events[:2] + [fake.events[-2]]:
            assert "store_layout" not in ev["config"]["output"]

    def test_all_failed_finalizes_before_raise(self, manifest, monkeypatch):
        # All-shards-failed still runs the finalize backstop BEFORE the
        # all-failed raise, mirroring the local backend: on Lambda the
        # pre-dispatch setup write is a droppable retries-0 async invoke, so
        # finalize is the only reliable manifest write and must run even when
        # every shard errored. Nothing succeeded, so no coverage invoke fires.
        import boto3

        cfg, sm_path, _shard, _data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}

        def responder(event):
            mode = event["mode"]
            if mode == "process_raster":
                return {"statusCode": 500, "body": json.dumps({"error": "boom"})}
            return {"statusCode": 200, "body": "{}"}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="boom"):
            agg(
                cfg,
                catalog=sm_path,
                store="s3://bucket/out.zarr",
                backend="lambda",
                max_workers=2,
                invocation="sync",
            )

        modes = [e["mode"] for e in fake.events]
        # Every shard 500s (two daily windows), so the run raises — but the
        # finalize backstop lands after the process events and before the raise.
        assert "finalize" in modes
        assert "coverage" not in modes  # nothing succeeded -> done empty
        proc_idx = [i for i, m in enumerate(modes) if m == "process_raster"]
        assert proc_idx and modes.index("finalize") > max(proc_idx)

    def test_flat_process_events_unchanged(self, manifest, monkeypatch):
        # Flat PROCESS events are byte-identical to pre-#247 runs — exactly
        # the pre-hive key set, no window/hive keys — inside the issue #264
        # lifecycle (ping -> sync raster setup -> fan-out), and no hive
        # lifecycle invokes (no finalize/coverage) ride a flat run.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", invocation="sync")
        assert [e["mode"] for e in fake.events] == ["ping", "setup", "process_raster"]
        assert set(fake.events[-1]) == {
            "mode",
            "shard_key",
            "granules",
            "config",
            "store_path",
            "time_index",
        }

    def test_parity_with_local_backend(self, manifest, monkeypatch, tmp_path):
        # Both dispatchers, same inputs -> identical leaf sets and stamps
        # (bar the write clocks): the lambda run drives the REAL handler
        # (deployment/aws/lambda_handler.py) with s3:// paths remapped onto
        # tmp_path, the local run writes directly; then compare.
        import importlib.util
        from pathlib import Path
        from unittest.mock import MagicMock

        import boto3

        import zagg.hive as hive
        import zagg.store as store_mod

        spec = importlib.util.spec_from_file_location(
            "zagg_lambda_handler_parity",
            Path(__file__).parent.parent / "deployment" / "aws" / "lambda_handler.py",
        )
        handler_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handler_mod)

        cfg, sm_path, shard, _data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}

        s3root = tmp_path / "s3root"

        def _translate(path):
            return str(s3root / path.removeprefix("s3://")) if path.startswith("s3://") else path

        real_open_store = store_mod.open_store
        real_open_object = hive.open_object_store
        monkeypatch.setattr(
            store_mod, "open_store", lambda path, **kw: real_open_store(_translate(path))
        )
        monkeypatch.setattr(
            hive, "open_object_store", lambda path, **kw: real_open_object(_translate(path))
        )
        # The handler binds open_store at its own import; patch that binding too.
        monkeypatch.setattr(
            handler_mod, "open_store", lambda path, **kw: real_open_store(_translate(path))
        )

        def responder(event):
            return handler_mod.lambda_handler(event, MagicMock())

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        lam = agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            invocation="sync",
        )
        assert lam["cells_with_data"] == 2 and lam["cells_error"] == 0
        lam_root = str(s3root / "bucket/out.zarr")

        loc_root = str(tmp_path / "local_out.zarr")
        loc = agg(cfg, catalog=sm_path, store=loc_root, backend="local", max_workers=2)
        assert loc["cells_with_data"] == 2

        def _leaves(root):
            return sorted(
                str(p.relative_to(root)) for p in Path(root).rglob("*.zarr") if p.is_dir()
            )

        assert _leaves(lam_root) == _leaves(loc_root) != []
        for rel in _leaves(loc_root):
            a = hive.read_commit(f"{lam_root}/{rel}")
            b = hive.read_commit(f"{loc_root}/{rel}")
            assert a is not None and b is not None
            a.pop("written_at"), b.pop("written_at")
            assert a == b
        # Manifests agree on the frozen keys (generated_at differs).
        ma, mb = hive.read_manifest(lam_root), hive.read_manifest(loc_root)
        for key in ("spec", "dataset", "cell_order", "shard_order", "temporal"):
            assert ma.get(key) == mb.get(key)


class TestRasterHiveIdempotency:
    """Window re-run + backfill (D13) and the flat-raster default pin
    (issue #247 phase 5)."""

    def _snapshot(self, root):
        from pathlib import Path

        out = {}
        for p in Path(root).rglob("*"):
            if p.is_file():
                out[str(p.relative_to(root))] = p.read_bytes()
        return out

    def _one_granule_map(self, tmp_path, cfg, shard, href, dt, key, name):
        grid = from_config(cfg)
        sm = ShardMap(
            grid.spatial_signature(),
            [shard],
            [[_entry("g", href, dt, key)]],
            {"collection": "s2-test"},
        )
        path = str(tmp_path / name)
        sm.to_json(path)
        return path

    def test_window_rerun_and_backfill(self, tmp_path, manifest):
        from pathlib import Path

        from zagg import hive

        cfg, sm_path, shard, _data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}
        store_path = cfg.output["store"]
        t1_map = self._one_granule_map(
            tmp_path, cfg, shard, str(tmp_path / "t1.tif"), T1, "dt-2", "t1only.json"
        )
        t0_map = self._one_granule_map(
            tmp_path, cfg, shard, str(tmp_path / "t0.tif"), T0, "dt-1", "t0only.json"
        )

        # Full run: both daily windows land.
        agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        leaf_a = hive.shard_leaf_path(store_path, shard, window="20260713")
        leaf_b = hive.shard_leaf_path(store_path, shard, window="20260718")
        before_a = self._snapshot(leaf_a)
        manifest_bytes = Path(store_path, "morton_hive.json").read_bytes()

        # Re-dispatch window B only: leaf B is replaced wholesale, sibling
        # window A stays byte-untouched, the manifest is not rewritten.
        stamp_b_before = hive.read_commit(leaf_b)
        agg(cfg, catalog=t1_map, backend="local", max_workers=2)
        assert self._snapshot(leaf_a) == before_a
        stamp_b_after = hive.read_commit(leaf_b)
        assert stamp_b_after["complete"] and stamp_b_after["window"] == "20260718"
        assert stamp_b_after["time_range"] == stamp_b_before["time_range"]
        assert Path(store_path, "morton_hive.json").read_bytes() == manifest_bytes

        # Backfill into a FRESH store: start with the later window, then add
        # the earlier one — a new leaf appears, the committed later leaf and
        # the manifest stay byte-untouched (D13: no resize, no manifest touch).
        cfg.output["store"] = str(tmp_path / "backfill.zarr")
        store2 = cfg.output["store"]
        agg(cfg, catalog=t1_map, backend="local", max_workers=2)
        leaf_b2 = hive.shard_leaf_path(store2, shard, window="20260718")
        before_b2 = self._snapshot(leaf_b2)
        manifest2 = Path(store2, "morton_hive.json").read_bytes()

        agg(cfg, catalog=t0_map, backend="local", max_workers=2)
        leaf_a2 = hive.shard_leaf_path(store2, shard, window="20260713")
        assert hive.read_commit(leaf_a2)["window"] == "20260713"  # new earlier leaf
        assert self._snapshot(leaf_b2) == before_b2  # committed leaf untouched
        assert Path(store2, "morton_hive.json").read_bytes() == manifest2
        # The root summary now spans both windows (cache union, D15).
        cov = hive.read_root_coverage(store2)
        assert cov["time_range"] == [T0, T1]

    def test_flat_pin_no_hive_objects(self, tmp_path, manifest):
        # Explicit store_layout: flat (the fixture pin; since issue #253 the
        # healpix default is hive) -> the flat (time, cells) store, with no
        # hive artifacts anywhere in the tree: flat output is object-for-
        # object what pre-#247 runs wrote (the flat write path is untouched;
        # this pins the absence of new objects).
        from pathlib import Path

        cfg, sm_path, _shard, _data = manifest
        summary = agg(cfg, catalog=sm_path, backend="local", max_workers=2)
        assert summary["cells_with_data"] == 1
        store = Path(cfg.output["store"])
        grid = from_config(cfg)
        # The run-level stats parquet (issue #297) now rides at the store
        # root on every backend; still no hive artifacts anywhere in the tree.
        root_names = sorted(p.name for p in store.iterdir())
        parquets = [n for n in root_names if n.startswith("stats_") and n.endswith(".parquet")]
        assert len(parquets) == 1
        assert root_names == sorted(["zarr.json", grid.group_path, parquets[0]])
        names = {p.name for p in store.rglob("*")}
        assert "morton_hive.json" not in names
        assert "coverage.moc" not in names


class TestInvokeLambdaRaster:
    """Sync/async transport of ``_invoke_lambda_raster`` (issue #218/#286).

    The raster twin of ``TestInvokeLambdaCell``: the sync path reads the worker
    envelope off the ``RequestResponse`` payload (byte-identical to the pre-#286
    transport); the async path (``result_url``) flips to a fire-and-forget
    ``Event`` invoke and polls the worker-mirrored result object. A
    ``FunctionError`` / non-200 is a deterministic shard error, never retried;
    only transient client-side invoke faults back off.
    """

    _EVENT = {
        "mode": "process_raster",
        "shard_key": 12345,
        "granules": [{"assets": {"red": "s3://b/t0.tif"}, "time_key": "dt-1"}],
        "config": {"data_source": {"reader": "raster"}},
        "store_path": "s3://out/x.zarr",
    }

    def _client(self, body=None, status=200, function_error=False):
        import io
        from unittest.mock import MagicMock

        client = MagicMock()
        if function_error:
            payload = MagicMock()
            payload.read.return_value = b"Task timed out after 900.00 seconds"
            client.invoke.return_value = {"FunctionError": "Unhandled", "Payload": payload}
        else:
            body = body if body is not None else {"timesteps": 2, "duration_s": 1.5}
            env = {"statusCode": status, "body": json.dumps(body)}
            client.invoke.return_value = {"Payload": io.BytesIO(json.dumps(env).encode())}
        return client

    def test_sync_reads_response_and_omits_result_url(self):
        from zagg import runner

        client = self._client(body={"timesteps": 2, "duration_s": 1.5})
        result = runner._invoke_lambda_raster(
            client, dict(self._EVENT), function_name="process-shard"
        )
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert client.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"
        assert "result_url" not in event  # byte-identical to the pre-#286 event
        assert result == {"error": None, "body": {"timesteps": 2, "duration_s": 1.5}}

    def test_sync_function_error_is_shard_error_not_retried(self):
        from zagg import runner

        client = self._client(function_error=True)
        result = runner._invoke_lambda_raster(
            client, dict(self._EVENT), function_name="process-shard", max_retries=3
        )
        assert client.invoke.call_count == 1  # deterministic, never retried (#119)
        assert result["body"] == {}
        assert "Lambda error" in result["error"]

    def test_sync_non_200_surfaces_body_error(self):
        from zagg import runner

        client = self._client(body={"error": "boom"}, status=500)
        result = runner._invoke_lambda_raster(
            client, dict(self._EVENT), function_name="process-shard"
        )
        assert result["error"] == "boom"
        assert result["body"] == {"error": "boom"}

    def test_async_event_invoke_carries_result_url_and_polls(self):
        from zagg import runner

        client = self._client()  # sync payload is ignored on the async path
        fetched = (
            {"statusCode": 200, "body": json.dumps({"timesteps": 2, "duration_s": 2.0})},
            None,
        )
        result = runner._invoke_lambda_raster(
            client,
            dict(self._EVENT),
            function_name="process-shard",
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=lambda: fetched,
            poll_timeout_s=10,
        )
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert client.invoke.call_args.kwargs["InvocationType"] == "Event"
        assert event["result_url"] == "s3://out/x.zarr.status/run1/12345.json"
        assert result == {"error": None, "body": {"timesteps": 2, "duration_s": 2.0}}

    def test_async_tolerates_result_landing_after_a_delay(self, monkeypatch):
        from zagg import runner

        monkeypatch.setattr(runner.time, "sleep", lambda *a: None)
        landed = {"statusCode": 200, "body": json.dumps({"timesteps": 2})}
        seen = {"n": 0}

        def fetch():
            seen["n"] += 1
            return (landed, None) if seen["n"] >= 3 else None  # two misses, then lands

        client = self._client()
        result = runner._invoke_lambda_raster(
            client,
            dict(self._EVENT),
            function_name="process-shard",
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=fetch,
            poll_timeout_s=30,
        )
        assert client.invoke.call_count == 1  # ONE Event invoke, then polled
        assert seen["n"] >= 3
        assert result["body"] == {"timesteps": 2}

    def test_async_missing_result_at_deadline_is_error_without_reinvoke(self):
        from zagg import runner

        client = self._client()
        result = runner._invoke_lambda_raster(
            client,
            dict(self._EVENT),
            function_name="process-shard",
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=lambda: None,  # never lands
            poll_timeout_s=0.0,  # first miss is already past the deadline
        )
        assert client.invoke.call_count == 1  # a still-running shard is NOT re-dispatched
        assert result["body"] == {}
        assert "no worker result" in result["error"]

    def test_async_non_200_result_surfaces_error_like_sync(self):
        # A landed non-200 envelope is a shard error, exactly as the sync branch
        # maps ``raw.get("statusCode") != 200`` (parity, review fold).
        from zagg import runner

        client = self._client()
        landed = ({"statusCode": 500, "body": json.dumps({"error": "boom"})}, None)
        result = runner._invoke_lambda_raster(
            client,
            dict(self._EVENT),
            function_name="process-shard",
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=lambda: landed,
            poll_timeout_s=10,
        )
        assert result["error"] == "boom"
        assert result["body"] == {"error": "boom"}

    def test_async_oversized_payload_raises_before_dispatch(self):
        from zagg import runner

        fat = dict(
            self._EVENT,
            granules=[{"blob": "x" * (runner._ASYNC_PAYLOAD_CAP_BYTES + 1)}],
        )
        client = self._client()
        with pytest.raises(ValueError, match="async dispatch budget"):
            runner._invoke_lambda_raster(
                client,
                fat,
                function_name="process-shard",
                result_url="s3://out/x.zarr.status/run1/12345.json",
                result_fetch=lambda: None,
                poll_timeout_s=10,
            )
        client.invoke.assert_not_called()

    def test_async_transient_invoke_fault_retries_then_polls(self, monkeypatch):
        from zagg import runner

        monkeypatch.setattr(runner.time, "sleep", lambda *a: None)
        client = self._client()
        client.invoke.side_effect = [
            Exception("Connection reset by peer"),  # transient -> retry
            {"StatusCode": 202},  # Event accepted
        ]
        fetched = ({"statusCode": 200, "body": json.dumps({"timesteps": 1})}, None)
        result = runner._invoke_lambda_raster(
            client,
            dict(self._EVENT),
            function_name="process-shard",
            max_retries=3,
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=lambda: fetched,
            poll_timeout_s=10,
        )
        assert client.invoke.call_count == 2
        assert result == {"error": None, "body": {"timesteps": 1}}


class TestRasterLambdaAsyncBackend:
    """Async shard fan-out (issue #286): the DEFAULT lambda raster transport.

    Each shard fires ``InvocationType="Event"`` and the dispatcher polls a
    per-shard result object the worker mirrors, so a shard longer than a GitHub
    runner's ~4 min NAT idle tolerance no longer severs the dispatcher. The
    ping/setup lifecycle stays synchronous so the load-bearing template write
    lands before fan-out (issue #264). A real run's poll reads S3; here an
    in-memory result box (a patched ``_result_fetcher``) stands in -- no AWS,
    no obstore.
    """

    def _wire(self, monkeypatch, proc_body, *, delay=0, timeout=720):
        import io

        import boto3

        import zagg.runner as runner_mod

        results: dict = {}
        calls: list = []  # (mode, InvocationType)
        events: list = []

        def _envelope(body):
            raw = json.dumps({"statusCode": 200, "body": json.dumps(body)}).encode()
            return {"Payload": io.BytesIO(raw)}

        class _AsyncFake:
            def invoke(self, **kwargs):
                event = json.loads(kwargs["Payload"])
                mode = event.get("mode")
                calls.append((mode, kwargs["InvocationType"]))
                events.append(event)
                if mode == "ping":
                    return _envelope({"ok": True, "mode": "ping", "zagg_version": "test"})
                if mode == "setup":
                    return _envelope(
                        {
                            "ok": True,
                            "mode": "setup",
                            "pipeline": "raster",
                            "timesteps": len(event.get("times_us", [])),
                        }
                    )
                if mode == "finalize":
                    return _envelope({"ok": True, "mode": "finalize", "layout": "hive"})
                if mode == "coverage":
                    return {"StatusCode": 202, "Payload": io.BytesIO(b"")}
                # #286: the shard fan-out is fire-and-forget Event + result_url.
                assert mode == "process_raster"
                assert kwargs["InvocationType"] == "Event"
                assert event.get("result_url")
                results[event["result_url"]] = proc_body(event)
                return {"StatusCode": 202, "Payload": io.BytesIO(b"")}

            def get_function_configuration(self, **kwargs):
                return {"Timeout": timeout}

        fake = _AsyncFake()
        fake.results, fake.calls, fake.events = results, calls, events  # type: ignore[attr-defined]
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)

        miss: dict = {}

        def fake_fetcher(box, prefix, creds, region, key):
            url = f"{prefix}/{key}"

            def fetch():
                if miss.get(url, 0) < delay:
                    miss[url] = miss.get(url, 0) + 1
                    return None
                resp = results.get(url)
                return (resp, None) if resp is not None else None

            return fetch

        monkeypatch.setattr(runner_mod, "_result_fetcher", fake_fetcher)
        monkeypatch.setattr(runner_mod.time, "sleep", lambda *a: None)
        return fake

    def _flat_body(self, event):
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"timesteps": len(event["time_index"]), "cells_with_data": 4096, "duration_s": 3.0}
            ),
        }

    def test_default_async_threads_result_channel(self, manifest, monkeypatch):
        cfg, sm_path, _shard, _data = manifest
        fake = self._wire(monkeypatch, self._flat_body)
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2
        )
        # Lifecycle stays synchronous; ONLY the shard fan-out is Event (#264/#286).
        assert fake.calls == [
            ("ping", "RequestResponse"),
            ("setup", "RequestResponse"),
            ("process_raster", "Event"),
        ]
        proc = fake.events[-1]
        assert proc["result_url"].startswith("s3://bucket/out.zarr.status/")
        assert proc["result_url"].endswith(".json")
        # Summary reads back from the polled result object, same shape as sync.
        assert summary["backend"] == "lambda"
        assert summary["cells_with_data"] == 1 and summary["cells_error"] == 0
        assert summary["total_obs"] == 2
        assert summary["lambda_time_s"] == 3.0

    def test_result_object_named_by_shard_label(self, manifest, monkeypatch):
        import zagg.runner as runner_mod

        cfg, sm_path, shard, _data = manifest
        fake = self._wire(monkeypatch, self._flat_body)
        agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2)
        grid = from_config(cfg)
        proc = fake.events[-1]
        name = proc["result_url"].rsplit("/", 1)[1]
        assert name == f"{runner_mod.shard_label(grid, shard)}.json"

    def test_tolerates_delayed_result_object(self, manifest, monkeypatch):
        # The object appears only after two polls miss: the dispatcher keeps
        # polling (issue #286) rather than recording the shard failed.
        cfg, sm_path, _shard, _data = manifest
        self._wire(monkeypatch, self._flat_body, delay=2)
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2
        )
        assert summary["cells_with_data"] == 1 and summary["cells_error"] == 0
        assert summary["total_obs"] == 2

    def test_sync_invocation_omits_result_channel(self, manifest, monkeypatch):
        # Belt-and-suspenders on the byte-identical guarantee: invocation="sync"
        # keeps the shard on RequestResponse with no result_url on the event.
        import boto3

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            return {
                "statusCode": 200,
                "body": json.dumps({"timesteps": len(event["time_index"])}),
            }

        fake = _FakeLambdaClient(_lifecycle(responder))
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        agg(
            cfg,
            catalog=sm_path,
            store="s3://bucket/out.zarr",
            backend="lambda",
            max_workers=2,
            invocation="sync",
        )
        proc = [e for e in fake.events if e["mode"] == "process_raster"][0]
        assert "result_url" not in proc

    def test_hive_result_objects_suffix_window_label(self, manifest, monkeypatch):
        # Hive windowed units (issue #247): two windows of one shard get
        # DISTINCT result objects (label suffixed), so their async envelopes
        # cannot clobber each other. Setup/coverage stay Event (issue #252),
        # ping/finalize sync, and the two shards are Event (#286).
        import zagg.runner as runner_mod

        cfg, sm_path, shard, _data = manifest
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "daily"}

        def proc_body(event):
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"shard_key": event["shard_key"], "timesteps": 1, "cells_with_data": 7}
                ),
            }

        fake = self._wire(monkeypatch, proc_body)
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2
        )
        procs = [e for e in fake.events if e["mode"] == "process_raster"]
        assert len(procs) == 2  # two (shard, window) units
        assert all(it == "Event" for m, it in fake.calls if m == "process_raster")
        grid = from_config(cfg)
        lbl = runner_mod.shard_label(grid, shard)
        names = sorted(e["result_url"].rsplit("/", 1)[1] for e in procs)
        assert names == sorted([f"{lbl}_20260713.json", f"{lbl}_20260718.json"])
        assert summary["cells_with_data"] == 2 and summary["cells_error"] == 0
