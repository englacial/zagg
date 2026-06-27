"""Tests for the runner module (Python API)."""

import json

import pytest

from zagg.config import default_config
from zagg.grids import HealpixGrid, RectilinearGrid, from_config
from zagg.runner import _check_signature, _load_catalog, _select_cells, agg


@pytest.fixture
def atl06_config():
    return default_config("atl06")


def _rec(n):
    return {"id": f"g{n}", "s3": f"s3://bucket/granule{n}.h5", "https": f"https://h/granule{n}.h5"}


# HealpixGrid(parent_order=6, child_order=12, layout="fullsphere").signature()
_ATL06_SIG = {
    "type": "healpix",
    "indexing_scheme": "nested",
    "parent_order": 6,
    "child_order": 12,
    "layout": "fullsphere",
}


@pytest.fixture
def catalog_file(tmp_path):
    """A minimal Phase-5 ShardMap JSON for testing."""
    catalog = {
        "metadata": {"short_name": "ATL06", "total_shards": 3, "total_granules": 6},
        "grid_signature": _ATL06_SIG,
        "shard_keys": [-4211324, -4211323, -4211322],
        "granules": [[_rec(4), _rec(5), _rec(6)], [_rec(3)], [_rec(1), _rec(2)]],
    }
    p = tmp_path / "catalog.json"
    p.write_text(json.dumps(catalog))
    return str(p)


class TestRunValidation:
    def test_missing_catalog_raises(self, atl06_config):
        with pytest.raises(ValueError, match="No catalog"):
            agg(atl06_config, store="./out.zarr")

    def test_missing_store_raises(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="No store path"):
            agg(atl06_config, catalog=catalog_file)

    def test_unknown_backend_raises(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="Unknown backend"):
            agg(atl06_config, catalog=catalog_file, store="./out.zarr", backend="magic")

    def test_lambda_requires_s3_store(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="s3://"):
            agg(atl06_config, catalog=catalog_file, store="./local.zarr", backend="lambda")


class TestDryRun:
    def test_dry_run_returns_summary(self, atl06_config, catalog_file):
        result = agg(atl06_config, catalog=catalog_file, store="./out.zarr", dry_run=True)
        assert result["dry_run"] is True
        assert result["total_cells"] == 3
        assert result["store_path"] == "./out.zarr"

    def test_dry_run_max_cells(self, atl06_config, catalog_file):
        result = agg(
            atl06_config, catalog=catalog_file, store="./out.zarr", dry_run=True, max_cells=2
        )
        assert result["total_cells"] == 2

    def test_dry_run_morton_cell(self, atl06_config, catalog_file):
        result = agg(
            atl06_config,
            catalog=catalog_file,
            store="./out.zarr",
            dry_run=True,
            morton_cell="-4211322",
        )
        assert result["total_cells"] == 1

    def test_dry_run_invalid_morton_cell(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="not in catalog"):
            agg(
                atl06_config,
                catalog=catalog_file,
                store="./out.zarr",
                dry_run=True,
                morton_cell="99999999",
            )


class TestSelectCells:
    def _data(self, n=3):
        return {
            "metadata": {},
            "grid_signature": {},
            "shard_keys": list(range(n)),
            "granules": [[_rec(i)] for i in range(n)],
        }

    def test_all_cells(self):
        pairs = _select_cells(self._data(3))
        assert [k for k, _ in pairs] == [0, 1, 2]

    def test_max_cells(self):
        pairs = _select_cells(self._data(3), max_cells=2)
        assert [k for k, _ in pairs] == [0, 1]

    def test_morton_cell(self):
        pairs = _select_cells(self._data(3), morton_cell="1")
        assert [k for k, _ in pairs] == [1]

    def test_invalid_morton_cell(self):
        with pytest.raises(ValueError, match="not in catalog"):
            _select_cells(self._data(2), morton_cell="99")


class TestLoadCatalog:
    def test_load(self, catalog_file):
        data = _load_catalog(catalog_file)
        assert "grid_signature" in data
        assert "shard_keys" in data
        assert "granules" in data
        assert len(data["shard_keys"]) == 3

    def test_old_format_rejected(self, tmp_path):
        # Pre-Phase-5: shard_keys/granules but no grid_signature.
        old = {"metadata": {}, "shard_keys": [0], "granules": [["s3://b/g.h5"]]}
        p = tmp_path / "old.json"
        p.write_text(json.dumps(old))
        with pytest.raises(ValueError, match="not a Phase-5 ShardMap"):
            _load_catalog(str(p))


class TestCheckSignature:
    """The shard-map reuse guard compares the *spatial* signature only (#89).

    A ShardMap is a spatial artifact, so it must validate any config that shares
    the spatial grid while declaring different aggregation fields, and still
    reject a genuinely different spatial grid. Old (full-signature) maps keep
    validating via a spatial-subset projection.
    """

    @staticmethod
    def _grid(name):
        return from_config(default_config(name))

    @staticmethod
    def _catalog(grid_signature):
        return {
            "metadata": {},
            "grid_signature": grid_signature,
            "shard_keys": [0],
            "granules": [[_rec(1)]],
        }

    def test_cross_aggregator_reuse_healpix(self):
        # Headline: a map built for tdigest validates a gain_bias run (same
        # parent11/chunk_inner13/child19 spatial grid, different agg fields).
        tdigest = self._grid("atl03_tdigest_healpix")
        gain_bias = self._grid("atl03_gain_bias_healpix")
        assert tdigest.signature() != gain_bias.signature()  # full sigs differ
        built = self._catalog(tdigest.spatial_signature())
        _check_signature(gain_bias, built)  # no raise
        # ... and the reverse.
        _check_signature(tdigest, self._catalog(gain_bias.spatial_signature()))

    def test_different_spatial_grid_raises_healpix(self):
        a = HealpixGrid(6, 12, layout="fullsphere")
        built = self._catalog(a.spatial_signature())
        # Different parent_order/child_order -> spatial mismatch -> raise.
        b = HealpixGrid(7, 13, layout="fullsphere")
        with pytest.raises(ValueError, match="different grid"):
            _check_signature(b, built)

    def test_old_full_signature_validates_and_reuses(self):
        # Back-compat: an OLD-style stored signature carrying output_fields (the
        # full signature) validates against a matching spatial grid AND is
        # reusable across differing agg fields (the projection drops output_fields).
        tdigest = self._grid("atl03_tdigest_healpix")
        gain_bias = self._grid("atl03_gain_bias_healpix")
        old = self._catalog(tdigest.signature())  # full sig (incl. output_fields)
        assert "output_fields" in old["grid_signature"]
        _check_signature(tdigest, old)  # same config: validates
        _check_signature(gain_bias, old)  # different agg fields: still reusable

    def test_none_signature_early_return(self):
        grid = self._grid("atl03_tdigest_healpix")
        _check_signature(grid, {"metadata": {}})  # no grid_signature key -> no raise

    def test_rectilinear_cross_aggregator_reuse(self):
        bounds = [359400, 4300740, 369400, 4310740]
        a = RectilinearGrid("EPSG:32618", 10, bounds, [250, 250], config=default_config("atl06"))
        b = RectilinearGrid(
            "EPSG:32618", 10, bounds, [250, 250], config=default_config("atl06_polar")
        )
        assert a.signature() != b.signature()
        _check_signature(b, self._catalog(a.spatial_signature()))  # no raise
        _check_signature(b, self._catalog(a.signature()))  # old full sig: also ok

    def test_rectilinear_different_spatial_grid_raises(self):
        bounds = [359400, 4300740, 369400, 4310740]
        a = RectilinearGrid("EPSG:32618", 10, bounds, [250, 250])
        built = self._catalog(a.spatial_signature())
        # Different resolution/shape -> spatial mismatch.
        b = RectilinearGrid("EPSG:32618", 20, bounds, [250, 250])
        with pytest.raises(ValueError, match="different grid"):
            _check_signature(b, built)
        # Different CRS -> spatial mismatch.
        c = RectilinearGrid("EPSG:3031", 10, bounds, [250, 250])
        with pytest.raises(ValueError, match="different grid"):
            _check_signature(c, built)


class TestDenseDeprecation:
    def test_dense_layout_emits_warning(self, atl06_config, catalog_file):
        atl06_config.output["grid"]["layout"] = "dense"
        atl06_config.catalog = catalog_file
        with pytest.warns(DeprecationWarning, match="dense.*deprecated"):
            agg(atl06_config, store="./out.zarr", dry_run=True)

    def test_fullsphere_layout_does_not_warn(self, atl06_config, catalog_file):
        atl06_config.output["grid"]["layout"] = "fullsphere"
        atl06_config.catalog = catalog_file
        import warnings as _w

        with _w.catch_warnings():
            _w.simplefilter("error", DeprecationWarning)
            agg(atl06_config, store="./out.zarr", dry_run=True)


class TestConfigFallbacks:
    def test_catalog_from_config(self, catalog_file, tmp_path):
        """Config.catalog is used when catalog= is not passed."""
        cfg = default_config("atl06")
        cfg.catalog = catalog_file
        result = agg(cfg, store="./out.zarr", dry_run=True)
        assert result["total_cells"] == 3

    def test_store_from_config(self, catalog_file):
        """Config output.store is used when store= is not passed."""
        cfg = default_config("atl06")
        cfg.output["store"] = "./configured.zarr"
        result = agg(cfg, catalog=catalog_file, dry_run=True)
        assert result["store_path"] == "./configured.zarr"


class TestOutputCredsEvent:
    """Normalization of the Lambda ``output_credentials`` event block."""

    def test_none_when_no_creds(self):
        from zagg.runner import _build_output_creds_event

        assert _build_output_creds_event(None, None, "us-west-2") is None

    def test_camelcase_passthrough(self):
        from zagg.runner import _build_output_creds_event

        creds = {"accessKeyId": "AKIA", "secretAccessKey": "s", "sessionToken": "t"}
        block = _build_output_creds_event(creds, None, "us-west-2")
        assert block == {
            "accessKeyId": "AKIA",
            "secretAccessKey": "s",
            "region": "us-west-2",
            "sessionToken": "t",
        }

    def test_endpoint_and_region_override(self):
        from zagg.runner import _build_output_creds_event

        creds = {"accessKeyId": "AKIA", "secretAccessKey": "s", "region": "eu-west-1"}
        block = _build_output_creds_event(creds, "https://r2.example", "us-west-2")
        assert block["endpointUrl"] == "https://r2.example"
        assert block["region"] == "eu-west-1"
        assert "sessionToken" not in block

    def test_snake_case_input(self):
        """boto / ``~/.aws/credentials`` spellings normalize to camelCase (#45)."""
        from zagg.runner import _build_output_creds_event

        creds = {
            "aws_access_key_id": "AKIA",
            "aws_secret_access_key": "s",
            "aws_session_token": "t",
            "region_name": "eu-west-1",
        }
        block = _build_output_creds_event(creds, None, "us-west-2")
        assert block == {
            "accessKeyId": "AKIA",
            "secretAccessKey": "s",
            "region": "eu-west-1",
            "sessionToken": "t",
        }

    def test_sts_pascalcase_input(self):
        """STS ``Credentials`` spellings normalize to camelCase (#45)."""
        from zagg.runner import _build_output_creds_event

        creds = {
            "AccessKeyId": "AKIA",
            "SecretAccessKey": "s",
            "SessionToken": "t",
            "Region": "eu-west-1",
        }
        block = _build_output_creds_event(creds, None, "us-west-2")
        assert block == {
            "accessKeyId": "AKIA",
            "secretAccessKey": "s",
            "region": "eu-west-1",
            "sessionToken": "t",
        }

    def test_missing_required_field_raises_clear_error(self):
        """A missing access key gives an actionable message, not a raw KeyError (#45)."""
        from zagg.runner import _build_output_creds_event

        creds = {"secretAccessKey": "s"}
        with pytest.raises(ValueError, match="accessKeyId"):
            _build_output_creds_event(creds, None, "us-west-2")

    def test_missing_both_required_fields_names_both(self):
        """Both missing fields are named in the error (#45)."""
        from zagg.runner import _build_output_creds_event

        with pytest.raises(ValueError, match="accessKeyId.*secretAccessKey"):
            _build_output_creds_event({"region": "us-west-2"}, None, "us-west-2")

    def test_empty_creds_returns_none(self):
        """An empty dict is treated as "no explicit creds", like None (#45)."""
        from zagg.runner import _build_output_creds_event

        assert _build_output_creds_event({}, None, "us-west-2") is None

    def test_endpoint_url_from_creds_flows_into_event(self):
        """``endpoint_url`` in the creds dict reaches the event block (#45)."""
        from zagg.runner import _build_output_creds_event

        creds = {
            "aws_access_key_id": "AKIA",
            "aws_secret_access_key": "s",
            "endpoint_url": "https://r2.example",
        }
        block = _build_output_creds_event(creds, None, "us-west-2")
        assert block["endpointUrl"] == "https://r2.example"

    def test_endpoint_param_takes_precedence_over_creds(self):
        """The explicit endpoint_url parameter wins over the creds dict (#45)."""
        from zagg.runner import _build_output_creds_event

        creds = {"accessKeyId": "AKIA", "secretAccessKey": "s", "endpointUrl": "https://from-creds"}
        block = _build_output_creds_event(creds, "https://from-param", "us-west-2")
        assert block["endpointUrl"] == "https://from-param"

    def test_first_truthy_spelling_wins(self):
        """A falsy spelling falls through to the next, mirroring the read path (#45)."""
        from zagg.runner import normalize_output_credentials

        creds = {"accessKeyId": "", "aws_access_key_id": "AKIA", "secretAccessKey": "s"}
        normalized = normalize_output_credentials(creds)
        assert normalized["accessKeyId"] == "AKIA"


class TestInvokeLambdaCellEvent:
    """The per-cell Lambda event uses the grid-neutral ``shard_key`` field, and
    only forwards the HEALPix-specific ``child_order`` when it is set (#24)."""

    _CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}

    def _captured_event(self, *, child_order):
        from unittest.mock import MagicMock

        from zagg.runner import _invoke_lambda_cell

        client = MagicMock()
        payload = MagicMock()
        payload.read.return_value = json.dumps(
            {"statusCode": 200, "body": json.dumps({"total_obs": 0, "duration_s": 0.0})}
        ).encode()
        client.invoke.return_value = {"Payload": payload, "FunctionError": None}
        _invoke_lambda_cell(
            client,
            (0,),
            12345,
            6,
            child_order,
            ["s3://b/g.h5"],
            "s3://out/x.zarr",
            self._CREDS,
            function_name="process-shard",
            config_dict=None,
            max_workers=4,
        )
        return json.loads(client.invoke.call_args.kwargs["Payload"])

    def test_healpix_event_uses_shard_key_and_keeps_child_order(self):
        event = self._captured_event(child_order=12)
        assert event["shard_key"] == 12345
        assert "parent_morton" not in event
        assert event["parent_order"] == 6
        assert event["child_order"] == 12

    def test_non_healpix_event_omits_child_order(self):
        # Rectilinear runs pass child_order=None; the field is dropped.
        event = self._captured_event(child_order=None)
        assert event["shard_key"] == 12345
        assert "child_order" not in event
        assert "parent_morton" not in event


class TestHandoffPassthrough:
    """`agg(handoff=...)` threads the carrier choice down to process_shard."""

    def test_process_and_write_forwards_handoff(self, monkeypatch, atl06_config):
        from zagg import runner

        captured = {}

        def fake_process_shard(grid, shard_key, urls, **kwargs):
            import pandas as pd

            captured["handoff"] = kwargs.get("handoff")
            return pd.DataFrame(), {"shard_key": shard_key, "error": None}

        monkeypatch.setattr(runner, "process_shard", fake_process_shard)
        runner._process_and_write(
            0,
            (0,),
            [_rec(1)],
            grid=None,
            s3_creds={},
            zarr_store=None,
            config=atl06_config,
            driver="s3",
            handoff="arrow",
        )
        assert captured["handoff"] == "arrow"

    def test_default_handoff_is_pandas(self, monkeypatch, atl06_config):
        from zagg import runner

        captured = {}

        def fake_process_shard(grid, shard_key, urls, **kwargs):
            import pandas as pd

            captured["handoff"] = kwargs.get("handoff")
            return pd.DataFrame(), {"shard_key": shard_key, "error": None}

        monkeypatch.setattr(runner, "process_shard", fake_process_shard)
        runner._process_and_write(
            0,
            (0,),
            [_rec(1)],
            grid=None,
            s3_creds={},
            zarr_store=None,
            config=atl06_config,
            driver="s3",
        )
        assert captured["handoff"] == "pandas"


def _stub_grid():
    from unittest.mock import MagicMock

    grid = MagicMock()
    grid.signature.return_value = {}
    grid.spatial_signature.return_value = {}
    grid.block_index.side_effect = lambda k: (k,)
    grid.emit_template.side_effect = lambda store, overwrite=False: store
    return grid


def _run_catalog():
    return {
        "metadata": {},
        "grid_signature": {},
        "shard_keys": [10, 11, 12, 13],
        "granules": [[{"s3": f"s3://b/g{i}.h5"}] for i in range(4)],
    }


class TestSummaryKeysByteIdentical:
    """The dispatch refactor (#63) must leave the run-summary dict keys -- and
    the data/error counting -- byte-identical for both backends.

    These pin the *structure* (key set) and the counters the dispatch loop now
    rolls up, against mocked per-cell work. Per-cell Lambda event payload bytes
    are pinned separately in ``TestInvokeLambdaCellEvent``.
    """

    _LOCAL_KEYS = {
        "total_cells",
        "cells_with_data",
        "cells_error",
        "total_obs",
        "wall_time_s",
        "store_path",
        "backend",
        "results",
    }
    _LAMBDA_KEYS = {
        "total_cells",
        "cells_with_data",
        "cells_error",
        "total_obs",
        "wall_time_s",
        "lambda_time_s",
        "gb_seconds",
        "price_per_gb_sec",
        "estimated_cost_usd",
        "store_path",
        "backend",
        "function_name",
        "results",
    }

    def test_local_summary_keys_and_counts(self, monkeypatch, atl06_config):
        import zagg.grids as grids_mod
        from zagg import runner

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "open_store", lambda *a, **k: object())
        monkeypatch.setattr(runner, "consolidate_metadata", lambda *a, **k: None)

        # 10,13 -> data; 11 -> raised (error, dropped from results); 12 ->
        # benign no-data meta (in results, not counted).
        def fake_paw(
            shard_key,
            chunk_idx,
            records,
            grid,
            s3_creds,
            zarr_store,
            config,
            driver=None,
            handoff="pandas",
        ):
            if shard_key == 11:
                raise RuntimeError("boom")
            if shard_key == 12:
                return {"shard_key": shard_key, "error": "No data after filtering"}
            return {"shard_key": shard_key, "total_obs": 7, "error": None}

        monkeypatch.setattr(runner, "_process_and_write", fake_paw)

        summary = runner._run_local(
            atl06_config,
            _run_catalog(),
            "./out.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=2,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert set(summary.keys()) == self._LOCAL_KEYS
        assert summary["backend"] == "local"
        assert summary["total_cells"] == 4
        assert summary["cells_with_data"] == 2
        assert summary["cells_error"] == 1
        assert summary["total_obs"] == 14
        assert len(summary["results"]) == 3  # raised cell excluded

    def test_local_threads_aoi_payload(self, monkeypatch, atl06_config):
        # When the manifest carries an aoi_mask list, _run_local threads each
        # shard's payload into _process_and_write; when it doesn't, the kwarg is
        # omitted entirely so the flag-off call is unchanged (issue #101).
        import zagg.grids as grids_mod
        from zagg import runner

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "open_store", lambda *a, **k: object())
        monkeypatch.setattr(runner, "consolidate_metadata", lambda *a, **k: None)

        seen = {}

        def fake_paw(shard_key, chunk_idx, records, grid, s3_creds, zarr_store, config, **kw):
            seen[int(shard_key)] = kw.get("aoi_payload", "OMITTED")
            return {"shard_key": shard_key, "total_obs": 1, "error": None}

        monkeypatch.setattr(runner, "_process_and_write", fake_paw)

        cat = _run_catalog()
        cat["aoi_mask"] = [[1, 2], [3], [], [4, 5]]  # parallel to shard_keys
        runner._run_local(
            atl06_config,
            cat,
            "./out.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert seen[10] == [1, 2]
        assert seen[13] == [4, 5]

        # No aoi_mask key -> kwarg omitted (legacy signature preserved).
        seen.clear()
        runner._run_local(
            atl06_config,
            _run_catalog(),
            "./out.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert all(v == "OMITTED" for v in seen.values())

    def test_lambda_refuses_aoi_mask(self, atl06_config):
        # The Lambda worker path can't fill the mask yet, so a flag-on Lambda run
        # must refuse loudly rather than emit an all-False (out-of-AOI) mask (#101).
        from zagg import runner

        atl06_config.output = {**atl06_config.output, "aoi_mask": True}
        with pytest.raises(NotImplementedError, match="not yet supported on the Lambda"):
            runner._run_lambda(
                atl06_config,
                _run_catalog(),
                "s3://out/x.zarr",
                12,
                max_cells=None,
                morton_cell=None,
                max_workers=1,
                overwrite=False,
                dry_run=False,
                region="us-west-2",
                function_name="fn",
            )

    def test_lambda_summary_keys_and_cost(self, monkeypatch, atl06_config):
        import boto3

        import zagg.grids as grids_mod
        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        from unittest.mock import MagicMock

        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                4,
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
                "body": {"total_obs": 3},
                "error": None,
                "lambda_duration": 2.0,
                "shard_key": 0,
            },
        )

        summary = runner._run_lambda(
            atl06_config,
            _run_catalog(),
            "s3://out/x.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1700,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
            function_name="process-shard",
        )
        assert set(summary.keys()) == self._LAMBDA_KEYS
        assert summary["backend"] == "lambda"
        assert summary["cells_with_data"] == 4
        assert summary["total_obs"] == 12
        # 4 cells x 2 s x 2 GB = 16 GB-s; cost = 16 * arm64 price.
        assert summary["lambda_time_s"] == 8.0
        assert summary["gb_seconds"] == 16.0
        assert summary["price_per_gb_sec"] == 0.0000133334
        assert summary["estimated_cost_usd"] == 16.0 * 0.0000133334

    def test_lambda_cost_byte_identical_with_mixed_durations(self, monkeypatch, atl06_config):
        """estimated_cost_usd must equal the pre-refactor arithmetic order:
        ``(sum(durations) * 2.0) * price`` computed once -- not a sum of
        per-cell ``cost_usd`` (which would diverge in the last FP ULP). Uses
        heterogeneous per-cell durations so the two orders actually differ.
        """
        import boto3

        import zagg.grids as grids_mod
        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        durations = iter([0.1, 0.2, 0.3, 12.7])

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        from unittest.mock import MagicMock

        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,  # 1 worker -> deterministic completion order for the iter()
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
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": next(durations),
                "shard_key": 0,
            },
        )

        summary = runner._run_lambda(
            atl06_config,
            _run_catalog(),
            "s3://out/x.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1700,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
            function_name="process-shard",
        )
        total = 0.1 + 0.2 + 0.3 + 12.7
        # The exact pre-refactor order: one multiply over the summed time.
        assert summary["gb_seconds"] == total * 2.0
        assert summary["estimated_cost_usd"] == (total * 2.0) * 0.0000133334
