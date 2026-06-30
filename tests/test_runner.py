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

    def _captured_event(self, *, child_order, profile=False, handoff="pandas"):
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
            handoff=handoff,
            profile=profile,
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

    def test_profile_flag_adds_event_key(self):
        # issue #100 phase 2: --profile forwards "profile": true into the event.
        event = self._captured_event(child_order=12, profile=True)
        assert event["profile"] is True

    def test_default_event_has_no_profile_key(self):
        # Default (profile off): event payload is byte-identical to pre-profile;
        # no "profile" key is added.
        event = self._captured_event(child_order=12, profile=False)
        assert "profile" not in event

    def test_handoff_adds_event_key(self):
        # issue #130: a non-default handoff forwards "handoff" into the event so
        # the deployed worker selects the arro3 arrow carrier. (The runner forwards
        # the string opaquely; the worker validates it.)
        event = self._captured_event(child_order=12, handoff="arrow")
        assert event["handoff"] == "arrow"

    def test_default_handoff_event_has_no_handoff_key(self):
        # Default (pandas): event payload is byte-identical to the pre-handoff
        # path; no "handoff" key is added (#130).
        event = self._captured_event(child_order=12, handoff="pandas")
        assert "handoff" not in event


class TestInvokeLambdaCellRetry:
    """Retry policy (#119): deterministic ``FunctionError``s (OOM / runtime
    crash) return immediately -- they are never re-invoked -- while transient
    client-side faults (throttle/network) still retry with backoff.
    """

    _CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}

    def _invoke(self, client, *, max_retries=3):
        from zagg.runner import _invoke_lambda_cell

        return _invoke_lambda_cell(
            client, (0,), 12345, 6, 12,
            ["s3://b/g.h5"], "s3://out/x.zarr", self._CREDS,
            function_name="process-shard", config_dict=None, max_workers=4,
            max_retries=max_retries,
        )

    def _function_error_response(self, error_payload):
        from unittest.mock import MagicMock

        payload = MagicMock()
        payload.read.return_value = error_payload.encode()
        return {"Payload": payload, "FunctionError": "Unhandled"}

    def test_oom_function_error_not_retried(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = self._function_error_response(
            '{"errorType": "Runtime.OutOfMemory"}'
        )
        # max_retries=5 proves the deterministic return is invariant to the
        # budget -- a FunctionError no longer consumes attempts at all (#119).
        result = self._invoke(client, max_retries=5)
        # A deterministic FunctionError is invoked exactly once, regardless of
        # max_retries, and the recorded error reflects the OOM (not masked).
        assert client.invoke.call_count == 1
        assert result["retries"] == 0
        assert result["error"].startswith("Lambda OOM:")
        assert result["status_code"] is None

    def test_timeout_function_error_not_retried(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = self._function_error_response(
            "Task timed out after 720.00 seconds"
        )
        # Timeouts already returned immediately pre-#119; pin that the shared
        # simplified branch keeps that behavior (single invoke, timeout=True).
        result = self._invoke(client, max_retries=5)
        assert client.invoke.call_count == 1
        assert result["retries"] == 0
        assert result["timeout"] is True
        assert result["error"].startswith("Lambda timeout:")

    def test_generic_function_error_not_retried(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = self._function_error_response(
            '{"errorType": "RuntimeError", "errorMessage": "boom"}'
        )
        result = self._invoke(client)
        assert client.invoke.call_count == 1
        assert result["retries"] == 0
        assert result["error"].startswith("Lambda error (Unhandled):")

    def test_transient_client_error_retried_with_backoff(self, monkeypatch):
        from unittest.mock import MagicMock

        import zagg.runner as runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))

        ok_payload = MagicMock()
        ok_payload.read.return_value = json.dumps(
            {"statusCode": 200, "body": json.dumps({"total_obs": 7, "duration_s": 1.0})}
        ).encode()
        ok = {"Payload": ok_payload, "FunctionError": None}

        client = MagicMock()
        client.invoke.side_effect = [
            Exception("TooManyRequestsException: Rate exceeded"),
            ok,
        ]
        result = self._invoke(client)
        # Transient fault retried: invoked twice, slept once with backoff.
        assert client.invoke.call_count == 2
        assert len(sleeps) == 1
        assert result["status_code"] == 200
        assert result["retries"] == 1

    def test_non_retryable_client_error_breaks(self, monkeypatch):
        from unittest.mock import MagicMock

        import zagg.runner as runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))

        client = MagicMock()
        client.invoke.side_effect = Exception("AccessDeniedException")
        result = self._invoke(client)
        # A non-transient client error is not retried (break), no backoff sleep.
        assert client.invoke.call_count == 1
        assert sleeps == []
        assert result["error"] == "AccessDeniedException"


class TestMaxRetriesPassthrough:
    """`agg(max_retries=...)` threads the per-cell retry budget down to
    ``_invoke_lambda_cell`` on the lambda backend (#119), default 3."""

    def _drive(self, monkeypatch, atl06_config, catalog_file, **agg_kwargs):
        import boto3

        import zagg.grids as grids_mod
        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        captured = {}

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials",
                            lambda: {"accessKeyId": "a", "secretAccessKey": "s",
                                     "sessionToken": "t"})
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        from unittest.mock import MagicMock
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            runner, "compute_available_workers",
            lambda requested, *a, **k: (
                1,
                ConcurrencyReport(account_limit=1000, current_concurrent=0,
                                  padding=100, available=900, function_reserved=None),
            ),
        )

        def _fake_cell(*a, **k):
            captured["max_retries"] = k.get("max_retries")
            return {"status_code": 200, "body": {"total_obs": 1}, "error": None,
                    "lambda_duration": 1.0, "shard_key": 0}

        monkeypatch.setattr(runner, "_invoke_lambda_cell", _fake_cell)
        agg(atl06_config, catalog=catalog_file, store="s3://out/x.zarr",
            backend="lambda", **agg_kwargs)
        return captured

    def test_max_retries_threaded_end_to_end(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file, max_retries=1)
        assert captured["max_retries"] == 1

    def test_default_max_retries_is_three(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file)
        assert captured["max_retries"] == 3


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

    def test_default_handoff_is_arrow(self, monkeypatch, atl06_config):
        # issue #130: arro3/arrow is the default carrier (faster + lighter on dense
        # shards); pandas remains available via an explicit handoff="pandas".
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
        assert captured["handoff"] == "arrow"


class TestProcessAndWriteStreaming:
    """Issue #91: the non-sharded ``_process_and_write`` streams each chunk through a
    ``write_chunk`` callback (no ``chunk_results`` accumulation). Drive a fake
    ``process_shard`` that streams 1 and K>1 chunks through the callback and assert
    the dense ``chunk_idx`` sequence + ragged keying (shard_key at K=1, block-index
    key at K>1) — the runner-level analogue of the lambda streaming test."""

    def _run(self, monkeypatch, atl06_config, *, chunks_per_shard, chunks):
        from unittest.mock import MagicMock

        import pandas as pd

        from zagg import runner

        cap = {"dense": [], "ragged": [], "write_chunk": None, "chunk_results": None}

        def fake_process_shard(grid, shard_key, urls, **kwargs):
            cap["write_chunk"] = kwargs.get("write_chunk")
            cap["chunk_results"] = kwargs.get("chunk_results")
            for block_index, carrier, ragged in chunks:
                kwargs["write_chunk"](block_index, carrier, ragged)
            return pd.DataFrame(), {"shard_key": shard_key, "error": None}

        grid = MagicMock()
        grid.sharded = False
        grid.chunks_per_shard = chunks_per_shard
        grid.chunk_grid_shape = (4,)

        monkeypatch.setattr(runner, "process_shard", fake_process_shard)
        monkeypatch.setattr(
            runner,
            "write_dataframe_to_zarr",
            lambda c, st, *, grid, chunk_idx: cap["dense"].append(chunk_idx),
        )
        monkeypatch.setattr(
            runner,
            "write_ragged_to_zarr",
            lambda r, st, *, grid, shard_key: cap["ragged"].append(shard_key),
        )
        # _block_index_key on a 1-D grid is block_index[0].
        monkeypatch.setattr(runner, "_block_index_key", lambda b, g: int(b[0]))
        runner._process_and_write(
            5,
            (5,),
            [_rec(1)],
            grid=grid,
            s3_creds={},
            zarr_store=None,
            config=atl06_config,
            driver="s3",
        )
        return cap

    def test_k1_streams_ragged_keyed_by_shard_key(self, monkeypatch, atl06_config):
        import pandas as pd

        cap = self._run(
            monkeypatch,
            atl06_config,
            chunks_per_shard=1,
            chunks=[((5,), pd.DataFrame(), {"h": ([], [])})],
        )
        # Streaming seam wired: callback passed, no accumulation sink.
        assert callable(cap["write_chunk"]) and cap["chunk_results"] is None
        assert cap["dense"] == [(5,)]
        assert cap["ragged"] == [5]  # K=1 -> keyed by shard_key

    def test_k_gt_1_streams_ragged_keyed_by_block_index(self, monkeypatch, atl06_config):
        import pandas as pd

        cap = self._run(
            monkeypatch,
            atl06_config,
            chunks_per_shard=3,
            chunks=[
                ((0,), pd.DataFrame(), {}),
                ((1,), pd.DataFrame(), {"h": ([], [])}),
                ((2,), pd.DataFrame(), {}),
            ],
        )
        assert cap["dense"] == [(0,), (1,), (2,)]
        assert cap["ragged"] == [0, 1, 2]  # K>1 -> keyed by _block_index_key


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
        "setup_s",
        "fanout_s",
        "finalize_s",
        "function_timeout_s",
        "worker_max_s",
        "worker_median_s",
        "worker_pstdev_s",
        "worker_pct_timeout",
        "max_memory_mb",
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
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
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
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
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


def _run_lambda_with_durations(
    monkeypatch,
    atl06_config,
    durations,
    *,
    timeout=720,
    profile=False,
    phase_timings=None,
    memories=None,
):
    """Drive ``_run_lambda`` over synthetic per-cell durations.

    Returns the summary dict. ``durations`` is consumed one per cell (the
    _run_catalog() has 4 cells); ``timeout`` stubs the function Timeout read.
    ``profile``/``phase_timings`` exercise the phase-2 opt-in path: when
    ``phase_timings`` is set it is attached to each cell result body.
    ``memories`` (issue #120), when given, is consumed one per cell and attached
    as ``body["max_memory_mb"]`` so the peak-memory rollup can be pinned.
    """
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
    monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: timeout)
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
    it = iter(durations)
    mem_it = iter(memories) if memories is not None else None

    def _fake_cell(*a, **k):
        body = {"total_obs": 1}
        if phase_timings is not None:
            body["phase_timings"] = phase_timings
        if mem_it is not None:
            body["max_memory_mb"] = next(mem_it)
        return {
            "status_code": 200,
            "body": body,
            "error": None,
            "lambda_duration": next(it),
            "shard_key": 0,
        }

    monkeypatch.setattr(runner, "_invoke_lambda_cell", _fake_cell)
    return runner._run_lambda(
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
        profile=profile,
    )


class TestWorkerRuntimeStats:
    """Phase 1 of issue #100: always-on worker-runtime distribution stats and
    orchestrator phase brackets in the lambda summary."""

    def test_worker_stats_pinned_against_synthetic_durations(self, monkeypatch, atl06_config):
        import statistics

        durations = [10.0, 20.0, 30.0, 100.0]
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, durations, timeout=720)
        assert summary["function_timeout_s"] == 720
        assert summary["worker_max_s"] == 100.0
        assert summary["worker_median_s"] == statistics.median(durations)
        assert summary["worker_pstdev_s"] == statistics.pstdev(durations)
        assert summary["worker_pct_timeout"] == 100.0 / 720

    def test_worker_pct_timeout_tracks_function_timeout(self, monkeypatch, atl06_config):
        summary = _run_lambda_with_durations(
            monkeypatch, atl06_config, [180.0, 60.0, 60.0, 60.0], timeout=900
        )
        assert summary["function_timeout_s"] == 900
        assert summary["worker_pct_timeout"] == 180.0 / 900

    def test_empty_durations_degrade_to_none(self, monkeypatch, atl06_config):
        # All cells report zero/falsy lambda_duration -> no distribution.
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, [0, 0, 0, 0], timeout=720)
        assert summary["worker_max_s"] is None
        assert summary["worker_median_s"] is None
        assert summary["worker_pstdev_s"] is None
        assert summary["worker_pct_timeout"] is None
        # function_timeout_s is still populated even with no durations.
        assert summary["function_timeout_s"] == 720

    def test_orchestrator_brackets_present_and_nonnegative(self, monkeypatch, atl06_config):
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0])
        for key in ("setup_s", "fanout_s", "finalize_s"):
            assert key in summary
            assert summary[key] >= 0.0


class TestWorkerMemory:
    """Issue #120: the lambda summary rolls up the straggler's peak RSS from the
    per-cell ``body['max_memory_mb']`` the worker stamps."""

    def test_max_memory_is_straggler_across_cells(self, monkeypatch, atl06_config):
        summary = _run_lambda_with_durations(
            monkeypatch,
            atl06_config,
            [1.0, 2.0, 3.0, 4.0],
            memories=[800.0, 1963.0, 1200.0, 900.0],
        )
        assert summary["max_memory_mb"] == 1963.0  # max, mirroring the wall-time framing

    def test_max_memory_none_when_unreported(self, monkeypatch, atl06_config):
        # No worker stamped memory (e.g. an older deployed layer) -> null, not 0.
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0])
        assert summary["max_memory_mb"] is None


class TestGetFunctionTimeout:
    """``_get_function_timeout_s`` reads the configured Timeout, or falls back
    to the template default on any failure (issue #100)."""

    def test_reads_timeout_from_client(self):
        from unittest.mock import MagicMock

        from zagg.runner import _get_function_timeout_s

        client = MagicMock()
        client.get_function_configuration.return_value = {"Timeout": 720}
        assert _get_function_timeout_s(client, "process-shard") == 720
        client.get_function_configuration.assert_called_once_with(FunctionName="process-shard")

    def test_falls_back_on_error(self):
        from unittest.mock import MagicMock

        from zagg.runner import _DEFAULT_FUNCTION_TIMEOUT_S, _get_function_timeout_s

        client = MagicMock()
        client.get_function_configuration.side_effect = RuntimeError("AccessDenied")
        assert _get_function_timeout_s(client, "process-shard") == _DEFAULT_FUNCTION_TIMEOUT_S

    def test_falls_back_on_missing_key(self):
        from unittest.mock import MagicMock

        from zagg.runner import _DEFAULT_FUNCTION_TIMEOUT_S, _get_function_timeout_s

        # Response without a "Timeout" key -> KeyError -> fallback.
        client = MagicMock()
        client.get_function_configuration.return_value = {}
        assert _get_function_timeout_s(client, "process-shard") == _DEFAULT_FUNCTION_TIMEOUT_S

    def test_falls_back_on_non_integer(self):
        from zagg.runner import _DEFAULT_FUNCTION_TIMEOUT_S, _get_function_timeout_s

        class _Client:
            def get_function_configuration(self, **kwargs):
                return {"Timeout": "not-a-number"}

        assert _get_function_timeout_s(_Client(), "process-shard") == _DEFAULT_FUNCTION_TIMEOUT_S


class TestProfilePlumbing:
    """Phase 2 of issue #100: the opt-in --profile path. Default runs stay
    byte-identical (no profile event key, no worker_phase_max summary key); when
    set, the per-cell ``phase_timings`` roll up into ``worker_phase_max``."""

    def test_default_run_omits_worker_phase_max(self, monkeypatch, atl06_config):
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0])
        assert "worker_phase_max" not in summary

    def test_profile_run_rolls_up_phase_max(self, monkeypatch, atl06_config):
        # Every cell reports the same phase_timings; the rollup is the per-phase
        # max across cells.
        summary = _run_lambda_with_durations(
            monkeypatch,
            atl06_config,
            [1.0, 2.0, 3.0, 4.0],
            profile=True,
            phase_timings={"read": 5.0, "index": 1.0, "aggregate": 2.0},
        )
        assert summary["worker_phase_max"] == {"read": 5.0, "index": 1.0, "aggregate": 2.0}

    def test_profile_run_with_no_phase_timings_is_empty(self, monkeypatch, atl06_config):
        # profile=True but workers emitted no phase_timings (e.g. handler bridge
        # not yet wired): the key is present but empty, never raising.
        summary = _run_lambda_with_durations(
            monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0], profile=True
        )
        assert summary["worker_phase_max"] == {}

    def test_agg_threads_profile_into_run_lambda(self, monkeypatch, atl06_config):
        from zagg import runner

        captured = {}

        def fake_run_lambda(*a, **k):
            captured["profile"] = k.get("profile")
            return {}

        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(runner, "_run_lambda", fake_run_lambda)
        runner.agg(
            atl06_config,
            catalog="ignored",
            store="s3://out/x.zarr",
            backend="lambda",
            profile=True,
        )
        assert captured["profile"] is True

    def test_agg_default_profile_is_false(self, monkeypatch, atl06_config):
        from zagg import runner

        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner,
            "_run_lambda",
            lambda *a, **k: captured.update(profile=k.get("profile")) or {},
        )
        runner.agg(atl06_config, catalog="ignored", store="s3://out/x.zarr", backend="lambda")
        assert captured["profile"] is False

    def test_agg_threads_handoff_into_run_lambda(self, monkeypatch, atl06_config):
        # issue #130: agg(handoff=...) reaches the lambda backend (it was local-only).
        from zagg import runner

        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner, "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(
            atl06_config, catalog="ignored", store="s3://out/x.zarr",
            backend="lambda", handoff="arrow",
        )
        assert captured["handoff"] == "arrow"

    def test_agg_default_handoff_is_arrow_on_lambda(self, monkeypatch, atl06_config):
        # issue #130: agg() defaults to the arro3/arrow carrier on the lambda backend.
        from zagg import runner

        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner, "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(atl06_config, catalog="ignored", store="s3://out/x.zarr", backend="lambda")
        assert captured["handoff"] == "arrow"


class TestWorkerPhaseTimings:
    """``process_shard(profile=...)`` emits ``phase_timings`` only when set, and
    leaves the default metadata unchanged otherwise (issue #100 phase 2)."""

    def _run(self, monkeypatch, *, profile, with_data=True):
        import numpy as np

        from zagg.processing import worker

        # Stub the read/group/aggregate seams so process_shard runs without I/O.
        monkeypatch.setattr(worker._processing, "_make_url_rewriter", lambda d: lambda u: u)

        class _H5:
            def __init__(self, *a, **k):
                pass

            def close(self):
                pass

        monkeypatch.setattr(worker._processing, "h5coro", type("M", (), {"H5Coro": _H5}))
        monkeypatch.setattr(
            worker._processing,
            "_read_group",
            lambda *a, **k: object() if with_data else None,
        )
        monkeypatch.setattr(
            worker,
            "_concat_and_group",
            lambda reads, grid, handoff: ({"leaf_id": np.array([0])}, {0: slice(0, 1)}, 1),
        )
        monkeypatch.setattr(worker, "_has_vector_fields", lambda config: False)
        monkeypatch.setattr(worker, "_eval_chunk_precompute", lambda config, pooled: {})
        monkeypatch.setattr(worker, "_pool_chunk_columns", lambda *a, **k: {})
        monkeypatch.setattr(
            worker,
            "_aggregate_chunk_cells",
            lambda *a, **k: ({}, {}, {}, 1),
        )
        monkeypatch.setattr(
            worker, "_build_output", lambda *a, **k: __import__("pandas").DataFrame()
        )

        from unittest.mock import MagicMock

        grid = MagicMock()
        grid.chunks_per_shard = 1
        grid.block_index.return_value = (0,)
        grid.children.return_value = np.array([0])
        del grid.iter_chunks  # force the K==1 fallback path

        from zagg.config import default_config

        _df, meta = worker.process_shard(
            grid,
            0,
            ["s3://b/g.h5"],
            s3_credentials={"accessKeyId": "a"},
            config=default_config("atl06"),
            driver="s3",
            h5coro_driver=object(),
            profile=profile,
        )
        return meta

    def test_no_phase_timings_by_default(self, monkeypatch):
        meta = self._run(monkeypatch, profile=False)
        assert "phase_timings" not in meta

    def test_phase_timings_present_when_profiled(self, monkeypatch):
        meta = self._run(monkeypatch, profile=True)
        assert set(meta["phase_timings"]) == {"read", "index", "aggregate"}
        for v in meta["phase_timings"].values():
            assert v >= 0.0

    def test_phase_timings_on_no_data_path(self, monkeypatch):
        # Even the "No data after filtering" early return carries read timing
        # when profiling (index/aggregate never ran).
        meta = self._run(monkeypatch, profile=True, with_data=False)
        assert meta["error"] == "No data after filtering"
        assert set(meta["phase_timings"]) == {"read"}
