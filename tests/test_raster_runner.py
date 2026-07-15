"""Runner-level tests for the raster pipeline (issue #218 phase 4).

``agg(backend="local")`` end-to-end over a ShardMap manifest and synthetic
GeoTIFFs: strategy selection, template emission, shard fan-out, slab writes,
and the shipped ``sentinel2_l2a`` template config.
"""

import json

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


class TestRasterLambdaBackend:
    def test_events_and_summary(self, manifest, monkeypatch, tmp_path):
        import boto3

        import zagg.runner as runner_mod

        cfg, sm_path, shard, _data = manifest

        def responder(event):
            assert event["mode"] == "process_raster"
            assert event["shard_key"] == shard
            assert set(event["time_index"]) == {"dt-1", "dt-2"}
            assert event["config"]["data_source"]["reader"] == "raster"
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        # Template emission targets the store path; keep it local by
        # intercepting open_store for the s3 URL.
        real_open = runner_mod.open_store

        def fake_open(path, **kw):
            if path.startswith("s3://"):
                return str(tmp_path / "lambda_out.zarr")
            return real_open(path, **kw)

        monkeypatch.setattr(runner_mod, "open_store", fake_open)
        summary = agg(
            cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda", max_workers=2
        )
        assert summary["backend"] == "lambda"
        assert summary["total_cells"] == 1 and summary["cells_with_data"] == 1
        assert summary["cells_error"] == 0
        assert summary["total_obs"] == 2
        assert len(fake.events) == 1

    def test_output_credentials_normalized_and_endpoint_threaded(
        self, manifest, monkeypatch, tmp_path
    ):
        # snake_case creds + a custom output endpoint must reach the worker event
        # as the handler-required camelCase block (accessKeyId/secretAccessKey)
        # with endpointUrl threaded in — parity with the spatial/temporal paths.
        import boto3

        import zagg.runner as runner_mod

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            body = {"timesteps": len(event["time_index"]), "cells_with_data": 4096}
            return {"statusCode": 200, "body": json.dumps(body)}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        real_open = runner_mod.open_store
        monkeypatch.setattr(
            runner_mod,
            "open_store",
            lambda path, **kw: (
                str(tmp_path / "creds.zarr") if path.startswith("s3://") else real_open(path, **kw)
            ),
        )
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
        assert len(fake.events) == 1
        block = fake.events[0]["output_credentials"]
        assert block["accessKeyId"] == "AKIA_SNAKE"
        assert block["secretAccessKey"] == "SECRET_SNAKE"
        assert block["endpointUrl"] == "https://custom.r2.example.com"

    def test_all_lambda_shards_error_raises(self, manifest, monkeypatch, tmp_path):
        import boto3

        import zagg.runner as runner_mod

        cfg, sm_path, _shard, _data = manifest

        def responder(event):
            return {"statusCode": 500, "body": json.dumps({"error": "boom"})}

        fake = _FakeLambdaClient(responder)
        monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)
        real_open = runner_mod.open_store
        monkeypatch.setattr(
            runner_mod,
            "open_store",
            lambda path, **kw: (
                str(tmp_path / "x.zarr") if path.startswith("s3://") else real_open(path, **kw)
            ),
        )
        with pytest.raises(RuntimeError, match="boom"):
            agg(cfg, catalog=sm_path, store="s3://bucket/out.zarr", backend="lambda")


class TestShippedTemplate:
    def test_sentinel2_l2a_config_loads_and_validates(self):
        cfg = default_config("sentinel2_l2a")
        validate_config(cfg)
        assert cfg.data_source["reader"] == "raster"
        assert cfg.data_source["bands"]["scl"]["dtype"] == "uint8"
        assert cfg.output["grid"]["child_order"] == 19
