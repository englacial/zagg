"""Tests for the runner module (Python API)."""

import json

import pytest

from zagg.config import default_config
from zagg.grids import HealpixGrid, RectilinearGrid, from_config
from zagg.runner import (
    _check_signature,
    _lambda_dispatch_order,
    _load_catalog,
    _select_cells,
    agg,
)


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


# Packed morton words (the canonical catalog form) whose decimal morton
# strings — the external form ``--morton-cell`` takes since issue #199 — are
# -4211324 / -4211323 / -4211322.
_CATALOG_WORDS = [11828422946311897094, 11828141471335186438, 11827859996358475782]


@pytest.fixture
def catalog_file(tmp_path):
    """A minimal Phase-5 ShardMap JSON for testing."""
    catalog = {
        "metadata": {"short_name": "ATL06", "total_shards": 3, "total_granules": 6},
        "grid_signature": _ATL06_SIG,
        "shard_keys": _CATALOG_WORDS,
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

    def test_dry_run_absent_morton_cell(self, atl06_config, catalog_file):
        # A well-formed decimal morton id that isn't in the catalog.
        with pytest.raises(ValueError, match="not in catalog"):
            agg(
                atl06_config,
                catalog=catalog_file,
                store="./out.zarr",
                dry_run=True,
                morton_cell="-4211321",
            )

    def test_dry_run_malformed_morton_cell(self, atl06_config, catalog_file):
        # HEALPix catalogs take decimal morton ids (issue #199): a raw packed
        # word (digits outside 1..4) is rejected with a pointed message.
        with pytest.raises(ValueError, match="not a decimal morton id"):
            agg(
                atl06_config,
                catalog=catalog_file,
                store="./out.zarr",
                dry_run=True,
                morton_cell="11827859996358475782",
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
        assert sorted(k for k, _ in pairs) == [0, 1, 2]

    def test_max_cells(self):
        pairs = _select_cells(self._data(3), max_cells=2)
        assert sorted(k for k, _ in pairs) == [0, 1]

    def test_shuffle_is_deterministic(self):
        # Seeded from the selected shard keys (issue #197): same catalog and
        # selection -> same order on a rerun/resume.
        assert _select_cells(self._data(50)) == _select_cells(self._data(50))

    def test_shuffle_permutes_selection(self):
        # Fan-out order is a permutation of the selection, not morton order.
        pairs = _select_cells(self._data(50))
        keys = [k for k, _ in pairs]
        assert sorted(keys) == list(range(50))
        assert keys != list(range(50))

    def test_shuffle_after_max_cells_truncation(self):
        # max_cells keeps its morton-first-N semantics: truncate, then shuffle.
        pairs = _select_cells(self._data(50), max_cells=10)
        assert sorted(k for k, _ in pairs) == list(range(10))

    def test_morton_cell(self):
        # A non-healpix signature keeps the stringified-int parse.
        pairs = _select_cells(self._data(3), morton_cell="1")
        assert [k for k, _ in pairs] == [1]

    def test_invalid_morton_cell(self):
        with pytest.raises(ValueError, match="not in catalog"):
            _select_cells(self._data(2), morton_cell="99")

    def test_morton_cell_healpix_decimal_round_trip(self):
        # HEALPix catalogs store packed words; --morton-cell takes the decimal
        # morton string and matches after the parse-back (issue #199).
        data = {
            "metadata": {},
            "grid_signature": {"type": "healpix"},
            "shard_keys": [11827859996358475782, 11828141471335186438],
            "granules": [[_rec(1)], [_rec(2)]],
        }
        pairs = _select_cells(data, morton_cell="-4211322")
        assert [k for k, _ in pairs] == [11827859996358475782]

    def test_morton_cell_healpix_rejects_packed_word(self):
        data = {
            "metadata": {},
            "grid_signature": {"type": "healpix"},
            "shard_keys": [11827859996358475782],
            "granules": [[_rec(1)]],
        }
        with pytest.raises(ValueError, match="not a decimal morton id"):
            _select_cells(data, morton_cell="11827859996358475782")

    def test_morton_cell_miss_on_legacy_catalog_hints_regenerate(self):
        # A pre-#199 catalog stores legacy signed-i64 keys; a well-formed
        # decimal id then misses every packed-word comparison. Hard break — no
        # shim — but the error says why (review finding, PR #205).
        data = {
            "metadata": {},
            "grid_signature": {"type": "healpix"},
            "shard_keys": [-4211322, -4211323],
            "granules": [[_rec(1)], [_rec(2)]],
        }
        with pytest.raises(ValueError, match="legacy signed decimal ids"):
            _select_cells(data, morton_cell="-4211322")

    def test_morton_cell_miss_on_packed_catalog_stays_bare(self):
        # A genuinely absent id in a packed-word catalog keeps the plain
        # message — the legacy hint fires only when the keys look legacy.
        data = {
            "metadata": {},
            "grid_signature": {"type": "healpix"},
            "shard_keys": [11827859996358475782],
            "granules": [[_rec(1)]],
        }
        with pytest.raises(ValueError, match="not in catalog$"):
            _select_cells(data, morton_cell="-4211321")


class TestSafeLabel:
    """Issue #199 (review finding, PR #205): error-reporting label rendering
    must never raise — re-rendering the malformed key that failed a cell would
    abort the accumulation loop precisely while reporting that failure."""

    def test_falls_back_to_digits_on_invalid_key(self):
        from zagg.runner import _safe_label

        g = HealpixGrid(parent_order=6, child_order=12, config=default_config("atl06"))
        # 0 is the empty sentinel: shard_label raises; _safe_label renders digits.
        assert _safe_label(g, 0) == "0"

    def test_renders_decimal_for_valid_key(self):
        from zagg.runner import _safe_label

        g = HealpixGrid(parent_order=6, child_order=12, config=default_config("atl06"))
        assert _safe_label(g, 11827859996358475782) == "-4211322"


class TestLambdaDispatchOrder:
    def _data(self, n=50):
        # Distinct granule counts (cell i has i+1 granules): the regime where
        # an exact-count sort would fully undo the issue #197 shuffle.
        return {
            "metadata": {},
            "grid_signature": {},
            "shard_keys": list(range(n)),
            "granules": [[_rec(j) for j in range(i + 1)] for i in range(n)],
        }

    def test_bucketed_descending_and_shuffle_survives(self):
        # Composed lambda ordering: seeded shuffle, then coarse-bucket sort.
        cells = _lambda_dispatch_order(_select_cells(self._data(50)))
        assert sorted(k for k, _ in cells) == list(range(50))
        buckets = [len(urls).bit_length() for _, urls in cells]
        assert buckets == sorted(buckets, reverse=True)
        # Within the largest bucket (counts 32..50, i.e. keys 31..49) the
        # shuffle must survive the sort: order is not morton-sorted.
        top = [k for k, urls in cells if len(urls).bit_length() == 6]
        assert sorted(top) == list(range(31, 50))
        assert top != sorted(top)

    def test_composed_order_is_deterministic(self):
        once = _lambda_dispatch_order(_select_cells(self._data(50)))
        again = _lambda_dispatch_order(_select_cells(self._data(50)))
        assert once == again


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


class TestDenseRemoval:
    def test_dense_layout_rejected(self, atl06_config, catalog_file):
        # The dense layout was removed (issue #88): a config selecting it is
        # rejected at validation, and grid construction refuses it too
        # (test_grids.py::test_healpix_explicit_dense_raises).
        from zagg.config import validate_config

        atl06_config.output["grid"]["layout"] = "dense"
        atl06_config.catalog = catalog_file
        with pytest.raises(ValueError, match="fullsphere"):
            validate_config(atl06_config)

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

    def _captured_event(self, *, child_order, profile=False, aoi_payload=None, handoff="pandas"):
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
            profile=profile,
            aoi_payload=aoi_payload,
            handoff=handoff,
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

    def test_aoi_payload_adds_event_key(self):
        # issue #101: a flag-on Lambda run forwards the per-shard mask payload
        # under the "aoi_payload" event key for the worker to expand.
        event = self._captured_event(child_order=12, aoi_payload=[1, 2, 3])
        assert event["aoi_payload"] == [1, 2, 3]

    def test_default_event_has_no_aoi_payload_key(self):
        # Default (flag off): no "aoi_payload" key, so the event stays
        # byte-identical to the pre-feature path (issue #101).
        event = self._captured_event(child_order=12)
        assert "aoi_payload" not in event

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
            client,
            (0,),
            12345,
            6,
            12,
            ["s3://b/g.h5"],
            "s3://out/x.zarr",
            self._CREDS,
            function_name="process-shard",
            config_dict=None,
            max_workers=4,
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


class TestInvokeLambdaCellAsync:
    """Async dispatch (issue #151): ``result_url`` switches the invoke to
    ``InvocationType="Event"`` and the result comes from polling the
    worker-written envelope instead of the (discarded) response payload."""

    _CREDS = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}

    def _invoke(self, client, result_fetch, *, max_retries=3, poll_timeout_s=10.0):
        from zagg.runner import _invoke_lambda_cell

        return _invoke_lambda_cell(
            client,
            (0,),
            12345,
            6,
            12,
            ["s3://b/g.h5"],
            "s3://out/x.zarr",
            self._CREDS,
            function_name="process-shard",
            config_dict=None,
            max_workers=4,
            max_retries=max_retries,
            result_url="s3://out/x.zarr.status/run1/12345.json",
            result_fetch=result_fetch,
            poll_timeout_s=poll_timeout_s,
        )

    @staticmethod
    def _envelope(body):
        return {"statusCode": 200, "body": json.dumps(body)}

    def test_event_invoke_carries_result_url(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}
        self._invoke(client, lambda: self._envelope({"total_obs": 1, "duration_s": 1.0}))
        kwargs = client.invoke.call_args.kwargs
        assert kwargs["InvocationType"] == "Event"
        event = json.loads(kwargs["Payload"])
        assert event["result_url"] == "s3://out/x.zarr.status/run1/12345.json"

    def test_result_envelope_maps_to_sync_result_shape(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}
        result = self._invoke(
            client,
            lambda: self._envelope({"total_obs": 7, "duration_s": 3.5, "max_memory_mb": 800.0}),
        )
        assert result["status_code"] == 200
        assert result["body"]["total_obs"] == 7
        assert result["lambda_duration"] == 3.5
        assert result["error"] is None
        assert result["timeout"] is False
        assert result["retries"] == 0
        assert result["granule_count"] == 1

    def test_polls_until_result_lands(self, monkeypatch):
        from unittest.mock import MagicMock

        import zagg.runner as runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))
        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}
        fetch = MagicMock(
            side_effect=[None, None, self._envelope({"total_obs": 1, "duration_s": 1.0})]
        )
        result = self._invoke(client, fetch)
        assert fetch.call_count == 3
        assert len(sleeps) == 2  # slept between the misses
        assert result["status_code"] == 200

    def test_missing_result_at_deadline_records_error_without_retry(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}
        # poll_timeout_s=0 -> the first miss is already past the deadline.
        result = self._invoke(client, lambda: None, max_retries=5, poll_timeout_s=0.0)
        # One Event invoke, no re-invoke: a missing result is deterministic
        # (worker timeout / OOM / crash), mirroring the sync FunctionError rule.
        assert client.invoke.call_count == 1
        assert result["status_code"] is None
        assert "no worker result within" in result["error"]
        # Self-diagnosing against a deployed worker that predates result_url
        # support: the error names the remedies (redeploy / invocation="sync").
        assert "predates result_url support" in result["error"]
        assert 'invocation="sync"' in result["error"]

    def test_worker_error_envelope_surfaces_body_error(self):
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}
        result = self._invoke(
            client,
            lambda: {"statusCode": 500, "body": json.dumps({"error": "Failed to write zarr"})},
        )
        assert result["status_code"] == 500
        assert result["error"] == "Failed to write zarr"

    def test_fetch_fault_is_contained_not_reinvoked(self):
        # A poll-side fault (S3 blip / missing s3:GetObject) must never escape
        # into the invoke retry classifier -- that would re-dispatch a shard
        # that is still running. It's treated as a miss; at the deadline the
        # persistent cause is surfaced instead of a phantom "worker crash".
        from unittest.mock import MagicMock

        client = MagicMock()
        client.invoke.return_value = {"StatusCode": 202}

        def denied():
            raise Exception("PermissionDenied: s3:GetObject")

        result = self._invoke(client, denied, max_retries=5, poll_timeout_s=0.0)
        assert client.invoke.call_count == 1  # never re-dispatched
        assert "no worker result within" in result["error"]
        assert "PermissionDenied" in result["error"]

    def test_transient_dispatch_error_still_retried(self, monkeypatch):
        from unittest.mock import MagicMock

        import zagg.runner as runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))
        client = MagicMock()
        client.invoke.side_effect = [
            Exception("TooManyRequestsException: Rate exceeded"),
            {"StatusCode": 202},
        ]
        result = self._invoke(client, lambda: self._envelope({"total_obs": 1, "duration_s": 1.0}))
        # The throttled *dispatch* is transient and retried; the poll then runs.
        assert client.invoke.call_count == 2
        assert result["status_code"] == 200
        assert result["retries"] == 1

    def test_oversized_async_payload_raises_before_dispatch(self):
        # Event invokes cap at 256 KB (vs 6 MB sync); the pre-flight fails with
        # a remedy instead of surfacing Lambda's raw
        # RequestEntityTooLargeException. Realistic trigger: a large strict-AOI
        # aoi_payload (issue #101).
        from unittest.mock import MagicMock

        from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES, _invoke_lambda_cell

        client = MagicMock()
        big_aoi = list(range(_ASYNC_PAYLOAD_CAP_BYTES // 4))  # >250 KB serialized
        with pytest.raises(ValueError, match='pass invocation="sync"') as excinfo:
            _invoke_lambda_cell(
                client,
                (0,),
                12345,
                6,
                12,
                ["s3://b/g.h5"],
                "s3://out/x.zarr",
                self._CREDS,
                function_name="process-shard",
                config_dict=None,
                max_workers=4,
                aoi_payload=big_aoi,
                result_url="s3://out/x.zarr.status/run1/-12345.json",
                result_fetch=lambda: None,
                poll_timeout_s=10.0,
                label="-12345",
            )
        # The message names the cell by its rendered label (issue #199), not
        # the raw packed-word digits.
        assert "cell -12345 " in str(excinfo.value)
        client.invoke.assert_not_called()

    def test_oversized_payload_allowed_on_sync_path(self):
        # The same event is fine synchronously (6 MB request cap) -- the gate
        # applies only to Event dispatch, so invocation="sync" is a real remedy.
        from unittest.mock import MagicMock

        from zagg.runner import _ASYNC_PAYLOAD_CAP_BYTES, _invoke_lambda_cell

        payload = MagicMock()
        payload.read.return_value = json.dumps(
            {"statusCode": 200, "body": json.dumps({"total_obs": 0, "duration_s": 0.0})}
        ).encode()
        client = MagicMock()
        client.invoke.return_value = {"Payload": payload, "FunctionError": None}
        big_aoi = list(range(_ASYNC_PAYLOAD_CAP_BYTES // 4))
        result = _invoke_lambda_cell(
            client,
            (0,),
            12345,
            6,
            12,
            ["s3://b/g.h5"],
            "s3://out/x.zarr",
            self._CREDS,
            function_name="process-shard",
            config_dict=None,
            max_workers=4,
            aoi_payload=big_aoi,
        )
        assert client.invoke.call_count == 1
        assert result["status_code"] == 200


class TestResultFetcher:
    """The lazy per-run result store + fetch closure (issue #151)."""

    def test_fetch_reads_written_envelope_and_none_when_absent(self, tmp_path):
        import obstore

        from zagg.runner import _result_fetcher
        from zagg.store import open_object_store

        prefix = str(tmp_path / "x.zarr.status" / "run1")
        box: dict = {}
        fetch = _result_fetcher(box, prefix, None, "us-west-2", "12345.json")
        assert fetch() is None  # nothing written yet
        writer = open_object_store(prefix)
        obstore.put(writer, "12345.json", json.dumps({"statusCode": 200, "body": "{}"}).encode())
        assert fetch() == {"statusCode": 200, "body": "{}"}
        assert "store" in box  # built lazily, cached for subsequent cells

    def test_poller_store_uses_short_retry_config(self, monkeypatch):
        """The poller loop owns retrying (issue #186): its store must carry
        the short ``_POLL_RETRY_CONFIG``, not the paced store-level default —
        one fetch blocking for minutes would silently overrun the poll
        deadline, which is only checked between fetches."""
        import zagg.runner as runner

        captured: dict = {}

        def fake_open(prefix, **kwargs):
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(runner, "open_object_store", fake_open)
        monkeypatch.setattr(runner, "_fetch_result", lambda store, key: None)
        fetch = runner._result_fetcher(
            {}, "s3://bucket/x.zarr.status/run1", None, "us-west-2", "1.json"
        )
        assert fetch() is None
        assert captured["retry_config"] == runner._POLL_RETRY_CONFIG


class TestMaxRetriesPassthrough:
    """`agg(max_retries=...)` threads the per-cell retry budget down to
    ``_invoke_lambda_cell`` on the lambda backend (#119), default 3."""

    def _drive(self, monkeypatch, atl06_config, catalog_file, **agg_kwargs):
        import boto3

        import zagg.grids as grids_mod
        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        captured = {}

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
                1,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )

        def _fake_cell(*a, **k):
            captured["max_retries"] = k.get("max_retries")
            return {
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": 0,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_cell", _fake_cell)
        agg(
            atl06_config,
            catalog=catalog_file,
            store="s3://out/x.zarr",
            backend="lambda",
            **agg_kwargs,
        )
        return captured

    def test_max_retries_threaded_end_to_end(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file, max_retries=1)
        assert captured["max_retries"] == 1

    def test_default_max_retries_is_three(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file)
        assert captured["max_retries"] == 3


class TestInvocationPassthrough:
    """`agg(invocation=...)` selects async (default) vs legacy sync dispatch
    (issue #151); async threads the per-shard result channel into the cell
    invoke. Same mocked drive as ``TestMaxRetriesPassthrough``."""

    def _drive(self, monkeypatch, atl06_config, catalog_file, *, grid_factory=None, **agg_kwargs):
        from unittest.mock import MagicMock

        import boto3

        import zagg.grids as grids_mod
        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        captured = {}
        grid_factory = grid_factory or _stub_grid

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: grid_factory())
        monkeypatch.setattr(runner, "_invoke_lambda_setup", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: None)
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )

        def _fake_cell(*a, **k):
            captured.update(
                result_url=k.get("result_url"),
                result_fetch=k.get("result_fetch"),
                poll_timeout_s=k.get("poll_timeout_s"),
                label=k.get("label"),
            )
            return {
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "lambda_duration": 1.0,
                "shard_key": 0,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_cell", _fake_cell)
        agg(
            atl06_config,
            catalog=catalog_file,
            store="s3://out/x.zarr",
            backend="lambda",
            **agg_kwargs,
        )
        return captured

    def test_default_async_threads_result_channel(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file)
        # <store>.status/<run_id>/<shard_label>.json, run_id unique per run.
        assert captured["result_url"].startswith("s3://out/x.zarr.status/")
        assert captured["result_url"].endswith(".json")
        # The object is named by the grid's shard label (issue #199) — the stub
        # labels key k as "-{k}" — never the raw packed-word digits alone.
        name = captured["result_url"].rsplit("/", 1)[1]
        assert name in {f"-{w}.json" for w in _CATALOG_WORDS}
        assert callable(captured["result_fetch"])
        # _drive pins the function timeout at 720 -> deadline 720 + margin.
        from zagg.runner import _ASYNC_POLL_MARGIN_S

        assert captured["poll_timeout_s"] == 720 + _ASYNC_POLL_MARGIN_S

    def test_sync_invocation_omits_result_channel(self, monkeypatch, atl06_config, catalog_file):
        captured = self._drive(monkeypatch, atl06_config, catalog_file, invocation="sync")
        assert captured["result_url"] is None
        assert captured["result_fetch"] is None
        assert captured["poll_timeout_s"] is None

    @staticmethod
    def _raising_label_grid():
        g = _stub_grid()
        g.shard_label.side_effect = ValueError("invalid word")
        return g

    def test_sync_malformed_key_label_falls_back(self, monkeypatch, atl06_config, catalog_file):
        # Review finding (PR #205): on SYNC runs the label never becomes a
        # path (no status key), so a malformed key must not raise out of
        # _cell_work — an exception there is RUN-fatal on the Lambda path.
        # It falls back to the raw digits instead.
        captured = self._drive(
            monkeypatch,
            atl06_config,
            catalog_file,
            invocation="sync",
            grid_factory=self._raising_label_grid,
        )
        assert captured["label"] in {str(w) for w in _CATALOG_WORDS}

    def test_async_malformed_key_is_fail_loud(self, monkeypatch, atl06_config, catalog_file):
        # On ASYNC runs the label names the status object — a path component —
        # so the same malformed key fails loudly before dispatch.
        with pytest.raises(ValueError, match="invalid word"):
            self._drive(
                monkeypatch,
                atl06_config,
                catalog_file,
                grid_factory=self._raising_label_grid,
            )

    def test_unknown_invocation_raises(self, atl06_config, catalog_file):
        with pytest.raises(ValueError, match="Unknown invocation"):
            agg(
                atl06_config,
                catalog=catalog_file,
                store="s3://out/x.zarr",
                backend="lambda",
                invocation="poll",
            )


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
    the dense + ragged writes both land at each chunk's own block index (the
    ragged vlen array is block-addressed like the dense arrays — issue #209) —
    the runner-level analogue of the lambda streaming test."""

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
            lambda r, st, *, grid, chunk_idx: cap["ragged"].append(chunk_idx),
        )
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

    def test_k1_streams_ragged_at_chunk_block(self, monkeypatch, atl06_config):
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
        assert cap["ragged"] == [(5,)]  # same block as the dense write (issue #209)

    def test_k_gt_1_streams_ragged_at_each_chunk_block(self, monkeypatch, atl06_config):
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
        assert cap["ragged"] == [(0,), (1,), (2,)]  # K>1 -> each chunk's own block


def _stub_grid():
    from unittest.mock import MagicMock

    grid = MagicMock()
    grid.signature.return_value = {}
    grid.spatial_signature.return_value = {}
    grid.block_index.side_effect = lambda k: (k,)
    # Deterministic decimal-string-shaped labels (issue #199) so status keys
    # built off the stub are real strings.
    grid.shard_label.side_effect = lambda k: f"-{int(k)}"
    grid.emit_template.side_effect = lambda store, overwrite=False: store
    return grid


def _run_catalog():
    return {
        "metadata": {},
        "grid_signature": {},
        "shard_keys": [10, 11, 12, 13],
        "granules": [[{"s3": f"s3://b/g{i}.h5"}] for i in range(4)],
    }


class TestClampedDataSource:
    """Dispatcher-side per-cell granule_workers clamp (issue #184, item 1's
    simple ``min(K, n_granules)`` case): a copy carrying the clamped width
    only when clamping actually lowers K, else None (shared config rides
    through untouched, keeping unclamped payloads byte-identical)."""

    def test_min_policy(self):
        from zagg.runner import _clamped_data_source

        ds = {"granule_workers": 4}
        assert _clamped_data_source(ds, 2) == {"granule_workers": 2}
        assert _clamped_data_source(ds, 4) is None
        assert _clamped_data_source(ds, 9) is None
        assert ds == {"granule_workers": 4}  # never mutated in place

    def test_default_k_applies(self):
        # The issue #185 default (K=4) clamps without an explicit key.
        from zagg.runner import _clamped_data_source

        assert _clamped_data_source({}, 2) == {"granule_workers": 2}
        assert _clamped_data_source({}, 4) is None
        # A degenerate 0-granule cell still sends a valid width (>= 1).
        assert _clamped_data_source({}, 0) == {"granule_workers": 1}

    def test_serial_k1_never_clamped(self):
        from zagg.runner import _clamped_data_source

        assert _clamped_data_source({"granule_workers": 1}, 5) is None


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
        # Container-telemetry rollup (issue #171); additive, None-valued when
        # no worker envelope carried telemetry (older deployed workers).
        "worker_cold_starts",
        "worker_warm_starts",
        "worker_rss_start_max_by_gen",
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

    def test_lambda_threads_aoi_payload(self, monkeypatch, atl06_config):
        # When the manifest carries an aoi_mask list, _run_lambda threads each
        # shard's payload into _invoke_lambda_cell; when it doesn't, the kwarg is
        # omitted so the per-cell invoke is byte-identical to the flag-off path
        # (issue #101).
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

        seen = {}

        def fake_cell(client, chunk_idx, shard_key, *a, **kw):
            seen[int(shard_key)] = kw.get("aoi_payload", "OMITTED")
            return {
                "shard_key": shard_key,
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "timeout": False,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_cell", fake_cell)

        atl06_config.output = {**atl06_config.output, "aoi_mask": True}
        cat = _run_catalog()
        cat["aoi_mask"] = [[1, 2], [3], [], [4, 5]]  # parallel to shard_keys
        runner._run_lambda(
            atl06_config,
            cat,
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
        assert seen[10] == [1, 2]
        assert seen[13] == [4, 5]

        # No aoi_mask key -> kwarg omitted (legacy event preserved).
        seen.clear()
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
        assert all(v == "OMITTED" for v in seen.values())

    @staticmethod
    def _clamp_catalog():
        # Per-shard record counts 2 / 6 / 6 / 1 against the default K=4:
        # shards 10 and 13 clamp, 11 passes through (above K). Shard 12 has 6
        # records but only 3 with an s3 href — the clamp must count the
        # RESOLVED urls (what the worker reads), not the raw records, so it
        # clamps to 3 (PR #187 review finding: len(records) under-clamps a
        # partially-resolvable cell).
        cat = _run_catalog()
        cat["granules"] = [
            [{"s3": f"s3://b/g0_{j}.h5"} for j in range(2)],
            [{"s3": f"s3://b/g1_{j}.h5"} for j in range(6)],
            [
                {"s3": f"s3://b/g2_{j}.h5"} if j < 3 else {"https": f"https://h/g2_{j}.h5"}
                for j in range(6)
            ],
            [{"s3": "s3://b/g3_0.h5"}],
        ]
        return cat

    def test_local_clamps_granule_workers_per_cell(self, monkeypatch, atl06_config):
        # Issue #184 (item 1): _run_local threads a per-cell config whose
        # granule_workers is min(K, n_granules); cells at/above K get the
        # SHARED config object untouched.
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

        seen, shared = {}, {}

        def fake_paw(shard_key, chunk_idx, records, grid, s3_creds, zarr_store, config, **kw):
            seen[int(shard_key)] = config.data_source.get("granule_workers", "ABSENT")
            shared[int(shard_key)] = config is atl06_config
            return {"shard_key": shard_key, "total_obs": 1, "error": None}

        monkeypatch.setattr(runner, "_process_and_write", fake_paw)

        runner._run_local(
            atl06_config,
            self._clamp_catalog(),
            "./out.zarr",
            12,
            max_cells=None,
            morton_cell=None,
            max_workers=1,
            overwrite=False,
            dry_run=False,
            region="us-west-2",
        )
        assert seen == {10: 2, 11: "ABSENT", 12: 3, 13: 1}
        assert shared[11]  # unclamped cell: shared config object as-is

    def test_lambda_clamps_granule_workers_per_cell(self, monkeypatch, atl06_config):
        # Issue #184 (item 1): _run_lambda's per-cell event config carries
        # min(K, n_granules); cells at/above K keep the shared config_dict
        # byte-identical (no granule_workers key injected).
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

        seen = {}

        def fake_cell(client, chunk_idx, shard_key, *a, **kw):
            seen[int(shard_key)] = kw["config_dict"]["data_source"].get("granule_workers", "ABSENT")
            return {
                "shard_key": shard_key,
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "timeout": False,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_cell", fake_cell)

        runner._run_lambda(
            atl06_config,
            self._clamp_catalog(),
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
        assert seen == {10: 2, 11: "ABSENT", 12: 3, 13: 1}

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
        # 4 cells x 2 s x 4 GB = 32 GB-s; cost = 32 * arm64 price.
        assert summary["lambda_time_s"] == 8.0
        assert summary["gb_seconds"] == 32.0
        assert summary["price_per_gb_sec"] == 0.0000133334
        assert summary["estimated_cost_usd"] == 32.0 * 0.0000133334

    def test_lambda_cost_byte_identical_with_mixed_durations(self, monkeypatch, atl06_config):
        """estimated_cost_usd must equal the pre-refactor arithmetic order:
        ``(sum(durations) * 4.0) * price`` computed once -- not a sum of
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
        assert summary["gb_seconds"] == total * 4.0
        assert summary["estimated_cost_usd"] == (total * 4.0) * 0.0000133334


# ---------------------------------------------------------------------------
# Pipeline strategy dispatch (issue #12, Phase 5)
# ---------------------------------------------------------------------------


def _temporal_config():
    from zagg.config import load_config_from_dict

    return load_config_from_dict(
        {
            "pipeline": {"type": "temporal"},
            "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
            "aggregation": {
                "variables": {
                    "max_t2m": {
                        "variable": "T2M",
                        "collection": "merra2",
                        "spatial_func": "max",
                        "temporal_reducer": "max",
                        "mask": "full",
                    }
                }
            },
            "output": {"format": "tabular", "store": "."},
        }
    )


def _synthetic_events():
    """Two synthetic events feeding ``process_event`` (max-T2M over time)."""
    xr = pytest.importorskip("xarray")
    import numpy as np

    lat = np.array([-70.0, -69.5])
    lon = np.array([0.0, 0.5])
    time = np.array(["2020-01-01T00", "2020-01-01T03"], dtype="datetime64[ns]")
    coords = {"time": time, "lat": lat, "lon": lon}
    events = []
    for key, peak in (("storm1", 5.0), ("storm2", 9.0)):
        event_mask = xr.DataArray(np.ones((2, 2, 2)), dims=["time", "lat", "lon"], coords=coords)
        temp = xr.DataArray(
            np.stack([np.full((2, 2), 1.0), np.full((2, 2), peak)]),
            dims=["time", "lat", "lon"],
            coords=coords,
        )
        collections = {"merra2": xr.Dataset({"T2M": temp})}
        areas = xr.DataArray(np.ones((2, 2)), dims=["lat", "lon"], coords={"lat": lat, "lon": lon})
        events.append((key, event_mask, collections, {"cell_areas": areas}))
    return events


def _patch_tabular_s3(monkeypatch):
    """Stub ``obstore`` + ``S3Store`` for an in-memory tabular put (no live S3).

    ``write_tabular`` imports both lazily from the real ``obstore`` package, so
    patch them at their source. Records the bucket the ``S3Store`` was opened
    for and the ``(key, payload)`` of the single put into ``captured``. Mirrors
    the existing mocked-AWS test style.
    """
    import obstore
    import obstore.store

    captured: dict = {}

    def _fake_s3store(bucket, **opts):
        captured["bucket"] = bucket
        captured["opts"] = opts
        return object()

    def _fake_put(store, key, payload):
        captured["key"] = key
        captured["payload"] = payload

    monkeypatch.setattr(obstore.store, "S3Store", _fake_s3store)
    monkeypatch.setattr(obstore, "put", _fake_put)
    return captured


def _uri_events():
    """Two URI-shaped events for the lambda temporal backend (Phase 8)."""
    return [
        {
            "event_key": "storm1",
            "event_mask_uri": "s3://b/masks/storm1.nc",
            "collection_uris": {"merra2": "s3://b/merra2.zarr"},
            "static_uris": {"cell_areas": "s3://b/areas.nc"},
        },
        {
            "event_key": "storm2",
            "event_mask_uri": "s3://b/masks/storm2.nc",
            "collection_uris": {"merra2": "s3://b/merra2.zarr"},
            "static_uris": {"cell_areas": "s3://b/areas.nc"},
        },
    ]


def _temporal_s3_config():
    from zagg.config import load_config_from_dict

    cfg = _temporal_config()
    d = {
        "pipeline": {"type": "temporal"},
        "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
        "aggregation": cfg.aggregation,
        "output": {"format": "parquet", "store": "s3://out/events.parquet"},
    }
    return load_config_from_dict(d)


class TestInvokeLambdaEvent:
    """Payload/retry contract of ``_invoke_lambda_event`` (issue #12, Phase 8).

    The payload must match ``_handle_process_event``'s expected params exactly;
    retries share ``zagg.dispatch.LAMBDA_RETRY`` (one classifier list);
    ``FunctionError``s are deterministic and never retried; async re-keys the
    polled envelope to ``event_key``."""

    _EV = {
        "event_key": "storm1",
        "event_mask_uri": "s3://b/masks/storm1.nc",
        "collection_uris": {"merra2": "s3://b/merra2.zarr"},
        "static_uris": {"cell_areas": "s3://b/areas.nc"},
    }

    def _client(self, body=None, function_error=None):
        from unittest.mock import MagicMock

        client = MagicMock()
        payload = MagicMock()
        if function_error:
            payload.read.return_value = function_error.encode()
            client.invoke.return_value = {"FunctionError": "Unhandled", "Payload": payload}
        else:
            body = body or {"ok": True, "results": {"max_t2m": 5.0}, "duration_s": 1.5}
            payload.read.return_value = json.dumps(
                {"statusCode": 200, "body": json.dumps(body)}
            ).encode()
            client.invoke.return_value = {"Payload": payload}
        return client

    def test_payload_matches_process_event_contract(self):
        from zagg import runner

        client = self._client()
        result = runner._invoke_lambda_event(
            client,
            dict(self._EV, s3_credentials={"accessKeyId": "a"}, input_credentials="unsigned"),
            function_name="process-shard",
            config_dict={"pipeline": {"type": "temporal"}},
            output_creds_event={"accessKeyId": "w"},
        )
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert event["mode"] == "process_event"
        assert event["event_key"] == "storm1"
        assert event["event_mask_uri"] == "s3://b/masks/storm1.nc"
        assert event["collection_uris"] == {"merra2": "s3://b/merra2.zarr"}
        assert event["static_uris"] == {"cell_areas": "s3://b/areas.nc"}
        assert event["config"] == {"pipeline": {"type": "temporal"}}
        assert event["return_results"] is True
        assert event["s3_credentials"] == {"accessKeyId": "a"}
        assert event["input_credentials"] == "unsigned"
        assert event["output_credentials"] == {"accessKeyId": "w"}
        assert "store_path" not in event  # driver writes; worker must not
        assert client.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"
        assert result["event_key"] == "storm1"
        assert result["status_code"] == 200
        assert result["body"]["results"]["max_t2m"] == 5.0
        assert result["lambda_duration"] == 1.5
        assert result["error"] is None

    def test_optional_keys_omitted_when_absent(self):
        from zagg import runner

        client = self._client()
        runner._invoke_lambda_event(
            client,
            dict(self._EV),
            function_name="process-shard",
            config_dict={},
        )
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert "s3_credentials" not in event
        assert "input_credentials" not in event
        assert "output_credentials" not in event
        assert "result_url" not in event

    def test_function_error_not_retried(self):
        from zagg import runner

        client = self._client(function_error="Task timed out after 720 seconds")
        result = runner._invoke_lambda_event(
            client,
            dict(self._EV),
            function_name="process-shard",
            config_dict={},
            max_retries=3,
        )
        assert client.invoke.call_count == 1
        assert result["timeout"] is True
        assert "Lambda timeout" in result["error"]
        assert result["retries"] == 0

    def test_transient_error_retried_with_backoff(self, monkeypatch):
        from zagg import runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))
        client = self._client()
        good = client.invoke.return_value
        client.invoke.side_effect = [
            Exception("TooManyRequestsException: Rate exceeded"),
            good,
        ]
        result = runner._invoke_lambda_event(
            client,
            dict(self._EV),
            function_name="process-shard",
            config_dict={},
            max_retries=3,
        )
        assert client.invoke.call_count == 2
        assert len(sleeps) == 1
        assert result["status_code"] == 200
        assert result["retries"] == 1

    def test_unretryable_error_breaks_out(self, monkeypatch):
        from zagg import runner

        sleeps = []
        monkeypatch.setattr(runner.time, "sleep", lambda s: sleeps.append(s))
        client = self._client()
        client.invoke.side_effect = Exception("AccessDeniedException")
        result = runner._invoke_lambda_event(
            client,
            dict(self._EV),
            function_name="process-shard",
            config_dict={},
            max_retries=3,
        )
        assert client.invoke.call_count == 1
        assert sleeps == []
        assert result["status_code"] is None
        assert "AccessDeniedException" in result["error"]

    def test_async_polls_and_rekeys_envelope(self):
        from zagg import runner

        client = self._client()
        fetched = {
            "statusCode": 200,
            "body": json.dumps(
                {"results": {"max_t2m": 5.0}, "timesteps_processed": 2, "duration_s": 2.0}
            ),
        }
        result = runner._invoke_lambda_event(
            client,
            dict(self._EV),
            function_name="process-shard",
            config_dict={},
            result_url="s3://out/events.parquet.status/run/storm1.json",
            result_fetch=lambda: fetched,
            poll_timeout_s=5,
        )
        event = json.loads(client.invoke.call_args.kwargs["Payload"])
        assert client.invoke.call_args.kwargs["InvocationType"] == "Event"
        assert event["result_url"] == "s3://out/events.parquet.status/run/storm1.json"
        assert result["event_key"] == "storm1"
        assert "shard_key" not in result and "granule_count" not in result
        assert result["body"]["results"]["max_t2m"] == 5.0
        assert result["lambda_duration"] == 2.0

    def test_async_payload_over_cap_raises(self):
        from zagg import runner

        client = self._client()
        fat = dict(self._EV, static_uris={"blob": "x" * (runner._ASYNC_PAYLOAD_CAP_BYTES + 1)})
        with pytest.raises(ValueError, match="async dispatch budget"):
            runner._invoke_lambda_event(
                client,
                fat,
                function_name="process-shard",
                config_dict={},
                result_url="s3://out/x.status/run/storm1.json",
                result_fetch=lambda: None,
                poll_timeout_s=5,
            )
        client.invoke.assert_not_called()


class TestTemporalLambdaStrategy:
    """``backend="lambda"`` temporal fan-out (issue #12, Phase 8): one
    ``process_event`` invoke per event, rows collected driver-side, one tabular
    write, per-event failure isolation, spatial machinery reused (preflight
    probe seams, LambdaExecutor, LAMBDA_RETRY)."""

    def _drive(self, monkeypatch, *, events=None, fake_invoke=None, config=None, **agg_kwargs):
        from unittest.mock import MagicMock

        import boto3

        from zagg import runner
        from zagg.concurrency import ConcurrencyReport

        captured = {"invokes": [], "written": None}

        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                min(requested, 4),
                ConcurrencyReport(
                    account_limit=1000,
                    current_concurrent=0,
                    padding=100,
                    available=900,
                    function_reserved=None,
                ),
            ),
        )

        def _default_invoke(client, ev, **kwargs):
            captured["invokes"].append((ev, kwargs))
            return {
                "event_key": ev["event_key"],
                "status_code": 200,
                "body": {
                    "results": {"max_t2m": 5.0},
                    "timesteps_processed": 2,
                    "duration_s": 1.0,
                    "meta": {"timesteps_processed": 2, "n_specs": 1, "collections": ["merra2"]},
                },
                "wall_time": 1.0,
                "lambda_duration": 1.0,
                "error": None,
                "retries": 0,
                "timeout": False,
            }

        monkeypatch.setattr(runner, "_invoke_lambda_event", fake_invoke or _default_invoke)

        def _fake_write(config, store_path, rows, **kwargs):
            captured["written"] = {"store_path": store_path, "rows": rows, "kwargs": kwargs}
            return store_path

        monkeypatch.setattr(runner, "_write_tabular_output", _fake_write)

        summary = agg(
            _temporal_s3_config() if config is None else config,
            backend="lambda",
            events=_uri_events() if events is None else events,
            **agg_kwargs,
        )
        return summary, captured

    def test_one_invoke_per_event_and_rows_written_once(self, monkeypatch):
        summary, captured = self._drive(monkeypatch)
        assert len(captured["invokes"]) == 2
        keys = sorted(ev["event_key"] for ev, _ in captured["invokes"])
        assert keys == ["storm1", "storm2"]
        # the driver writes the collected rows exactly once, local-shape rows
        written = captured["written"]
        assert written["store_path"] == "s3://out/events.parquet"
        assert len(written["rows"]) == 2
        row = next(r for r in written["rows"] if r["event_key"] == "storm1")
        assert row["results"]["max_t2m"] == 5.0
        assert row["meta"]["timesteps_processed"] == 2
        assert row["meta"]["n_specs"] == 1  # full worker meta passes through
        assert summary["backend"] == "lambda"
        assert summary["total_events"] == 2
        assert summary["events_with_data"] == 2
        assert summary["events_error"] == 0
        assert summary["timesteps_processed"] == 4
        assert summary["output_path"] == "s3://out/events.parquet"
        assert summary["gb_seconds"] == pytest.approx(2.0 * 4.0)  # 2 s x 4 GB
        assert summary["results"] == written["rows"]
        assert summary["failures"] == []

    def test_credentials_provider_fetched_once_and_injected(self, monkeypatch):
        # data_source.credentials_provider resolves through the registry, is
        # called once, and fills s3_credentials on events lacking their own;
        # per-event credentials take precedence (issue #213 Phase 4).
        from zagg import registry as zagg_registry

        calls = {"n": 0}

        def _provider():
            calls["n"] += 1
            return {"accessKeyId": "shared"}

        zagg_registry.register_credential_provider("test_provider", _provider, replace=True)
        try:
            cfg = _temporal_s3_config()
            cfg.data_source["credentials_provider"] = "test_provider"
            events = _uri_events()
            events[0]["s3_credentials"] = {"accessKeyId": "per-event"}
            _, captured = self._drive(monkeypatch, config=cfg, events=events)
            creds_by_key = {
                ev["event_key"]: ev.get("s3_credentials") for ev, _ in captured["invokes"]
            }
            assert calls["n"] == 1
            assert creds_by_key["storm1"] == {"accessKeyId": "per-event"}
            assert creds_by_key["storm2"] == {"accessKeyId": "shared"}
        finally:
            zagg_registry.CREDENTIAL_PROVIDERS._entries.pop("test_provider", None)

    def test_unknown_credentials_provider_fails_before_invoking(self, monkeypatch):
        cfg = _temporal_s3_config()
        cfg.data_source["credentials_provider"] = "not_a_provider"
        with pytest.raises(KeyError, match="not_a_provider"):
            self._drive(monkeypatch, config=cfg)

    def test_temporal_summary_carries_container_rollup(self, monkeypatch):
        # Issue #171: the temporal path aggregates the same worker container
        # telemetry as the spatial path (additive summary fields, shared helper).
        def _invoke(client, ev, **kwargs):
            gen = 1 if ev["event_key"] == "storm1" else 2
            return {
                "event_key": ev["event_key"],
                "status_code": 200,
                "body": {
                    "results": {"max_t2m": 5.0},
                    "timesteps_processed": 2,
                    "duration_s": 1.0,
                    "meta": {"timesteps_processed": 2},
                    "container_cold": gen == 1,
                    "container_generation": gen,
                    "rss_start_mb": 300.0 * gen,
                },
                "wall_time": 1.0,
                "lambda_duration": 1.0,
                "error": None,
                "retries": 0,
                "timeout": False,
            }

        summary, _ = self._drive(monkeypatch, fake_invoke=_invoke)
        assert summary["worker_cold_starts"] == 1
        assert summary["worker_warm_starts"] == 1
        assert summary["worker_rss_start_max_by_gen"] == {1: 300.0, 2: 600.0}

    def test_async_wiring_threads_result_channel(self, monkeypatch):
        summary, captured = self._drive(monkeypatch)  # invocation defaults to async
        for ev, kwargs in captured["invokes"]:
            url = kwargs["result_url"]
            assert url.startswith("s3://out/events.parquet.status/")
            assert url.endswith(f"/{ev['event_key']}.json")
            assert callable(kwargs["result_fetch"])
            assert kwargs["poll_timeout_s"] == 720 + 90.0
        assert summary["function_timeout_s"] == 720

    def test_sync_invocation_omits_result_channel(self, monkeypatch):
        _, captured = self._drive(monkeypatch, invocation="sync")
        for _, kwargs in captured["invokes"]:
            assert "result_url" not in kwargs
            assert "result_fetch" not in kwargs

    def test_one_failed_event_does_not_kill_the_run(self, monkeypatch):
        def _invoke(client, ev, **kwargs):
            if ev["event_key"] == "storm1":
                return {
                    "event_key": "storm1",
                    "status_code": None,
                    "body": {},
                    "wall_time": 1.0,
                    "lambda_duration": 0,
                    "error": "Lambda timeout: Task timed out",
                    "retries": 0,
                }
            return {
                "event_key": ev["event_key"],
                "status_code": 200,
                "body": {"results": {"max_t2m": 9.0}, "timesteps_processed": 2},
                "wall_time": 1.0,
                "lambda_duration": 1.0,
                "error": None,
                "retries": 0,
                "timeout": False,
            }

        summary, captured = self._drive(monkeypatch, fake_invoke=_invoke)
        assert summary["events_error"] == 1
        assert summary["events_with_data"] == 1
        (row,) = captured["written"]["rows"]  # only the good event lands a row
        assert row["event_key"] == "storm2"
        # the failure keeps its error detail in the summary
        (failure,) = summary["failures"]
        assert failure["event_key"] == "storm1"
        assert "timeout" in failure["error"]

    def test_rejects_tuple_events(self, monkeypatch):
        with pytest.raises(ValueError, match="dicts with 'event_key'"):
            self._drive(monkeypatch, events=_synthetic_events())

    def test_rejects_non_s3_store(self, monkeypatch):
        from zagg.config import load_config_from_dict

        cfg = _temporal_s3_config()
        d = {
            "pipeline": {"type": "temporal"},
            "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
            "aggregation": cfg.aggregation,
            "output": {"format": "parquet", "store": "./events.parquet"},
        }
        with pytest.raises(ValueError, match="s3:// store path"):
            agg(load_config_from_dict(d), backend="lambda", events=_uri_events())

    def test_rejects_non_tabular_store(self, monkeypatch):
        from zagg.config import load_config_from_dict

        cfg = _temporal_s3_config()
        d = {
            "pipeline": {"type": "temporal"},
            "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
            "aggregation": cfg.aggregation,
            "output": {"format": "parquet", "store": "s3://out/events"},
        }
        with pytest.raises(ValueError, match="tabular store path"):
            agg(load_config_from_dict(d), backend="lambda", events=_uri_events())

    def test_rejects_default_zarr_format(self, monkeypatch):
        # output.format left at the "zarr" default would silently skip the
        # post-fan-out tabular write; fail before invoking anything.
        from zagg.config import load_config_from_dict

        cfg = _temporal_s3_config()
        d = {
            "pipeline": {"type": "temporal"},
            "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
            "aggregation": cfg.aggregation,
            "output": {"store": "s3://out/events.parquet"},  # no format key
        }
        with pytest.raises(ValueError, match="output.format"):
            agg(load_config_from_dict(d), backend="lambda", events=_uri_events())

    def test_dry_run_reports_lambda_backend(self, monkeypatch):
        summary = agg(
            _temporal_s3_config(),
            backend="lambda",
            events=_uri_events(),
            dry_run=True,
        )
        assert summary == {
            "dry_run": True,
            "total_events": 2,
            "n_specs": 1,
            "store_path": "s3://out/events.parquet",
            "backend": "lambda",
        }


class TestStrategyDispatch:
    def test_spatial_uses_spatial_strategy(self, atl06_config):
        from zagg.runner import SpatialStrategy, _get_strategy

        assert isinstance(_get_strategy("spatial"), SpatialStrategy)

    def test_temporal_and_event_share_temporal_strategy(self):
        from zagg.runner import TemporalStrategy, _get_strategy

        assert isinstance(_get_strategy("temporal"), TemporalStrategy)
        assert isinstance(_get_strategy("event"), TemporalStrategy)

    def test_agg_spatial_still_routes_through_spatial_path(self, monkeypatch, atl06_config):
        # Byte-identical guard at the seam: a spatial config dispatches into the
        # unchanged spatial path. We assert agg() delegates to SpatialStrategy
        # (the summary itself is pinned by TestSummaryKeysByteIdentical).
        from zagg import runner

        called = {}

        def fake_run(self, config, **kwargs):
            called["cls"] = type(self).__name__
            return {"ok": True}

        monkeypatch.setattr(runner.SpatialStrategy, "run", fake_run)
        out = runner.agg(atl06_config, catalog="c.json", store="./out.zarr")
        assert called["cls"] == "SpatialStrategy"
        assert out == {"ok": True}


class TestTemporalStrategy:
    def test_runs_events_via_local_executor(self):
        from zagg.runner import agg

        events = _synthetic_events()
        summary = agg(_temporal_config(), events=events)
        assert summary["backend"] == "local"
        assert summary["total_events"] == 2
        assert summary["events_with_data"] == 2
        assert summary["events_error"] == 0
        by_key = {r["event_key"]: r for r in summary["results"]}
        assert by_key["storm1"]["results"]["max_t2m"] == pytest.approx(5.0)
        assert by_key["storm2"]["results"]["max_t2m"] == pytest.approx(9.0)

    def test_max_cells_truncates_events(self):
        from zagg.runner import agg

        summary = agg(_temporal_config(), events=_synthetic_events(), max_cells=1)
        assert summary["total_events"] == 1

    def test_collection_options_applied_on_local_backend(self):
        # In-memory event tuples skip the reader, so the local path applies the
        # config's per-collection options itself (issue #213 Phase 3): a
        # derived variable declared in the config is visible to the specs.
        from zagg.config import load_config_from_dict
        from zagg.runner import agg

        cfg_dict = {
            "pipeline": {"type": "temporal"},
            "data_source": {
                "reader": "xarray_s3",
                "collections": {"merra2": {"derived": {"t2m_double": "T2M * 2"}}},
            },
            "aggregation": {
                "variables": {
                    "max_double": {
                        "variable": "t2m_double",
                        "collection": "merra2",
                        "spatial_func": "max",
                        "temporal_reducer": "max",
                        "mask": "full",
                    }
                }
            },
            "output": {"format": "tabular", "store": "."},
        }
        summary = agg(load_config_from_dict(cfg_dict), events=_synthetic_events())
        by_key = {r["event_key"]: r for r in summary["results"]}
        assert by_key["storm1"]["results"]["max_double"] == pytest.approx(10.0)
        assert by_key["storm2"]["results"]["max_double"] == pytest.approx(18.0)

    def test_dry_run_summary(self):
        from zagg.runner import agg

        summary = agg(_temporal_config(), events=_synthetic_events(), dry_run=True)
        assert summary["dry_run"] is True
        assert summary["total_events"] == 2
        assert summary["n_specs"] == 1

    def test_missing_events_raises(self):
        from zagg.runner import agg

        with pytest.raises(ValueError, match="requires events="):
            agg(_temporal_config())

    def test_unknown_backend_rejected(self):
        # lambda is supported since Phase 8 (TestTemporalLambdaStrategy); an
        # unknown backend still fails fast.
        from zagg.runner import agg

        with pytest.raises(ValueError, match="Unknown backend"):
            agg(_temporal_config(), events=_synthetic_events(), backend="cluster")

    def test_failing_event_counted_as_error(self):
        # A spec referencing a missing variable makes process_event raise; the
        # event is counted as an error and the run continues (tagged-envelope
        # contract, mirroring the spatial local path).
        from zagg.config import load_config_from_dict
        from zagg.runner import agg

        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "bad": {
                            "variable": "NOPE",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                            "mask": "full",
                        }
                    }
                },
                "output": {"format": "tabular", "store": "."},
            }
        )
        summary = agg(cfg, events=_synthetic_events())
        assert summary["events_error"] == 2
        assert summary["events_with_data"] == 0

    def test_tabular_store_writes_parquet_and_reports_path(self, tmp_path):
        # Phase 6: a temporal run whose store path names a tabular file persists
        # the event rows through TabularWriter and reports output_path.
        import pandas as pd

        from zagg.config import load_config_from_dict
        from zagg.runner import agg

        path = tmp_path / "events.parquet"
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "max_t2m": {
                            "variable": "T2M",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                            "mask": "full",
                        }
                    }
                },
                "output": {"format": "parquet", "store": str(path)},
            }
        )
        summary = agg(cfg, events=_synthetic_events())
        assert summary["output_path"] == str(path)
        assert path.exists()
        back = pd.read_parquet(path).set_index("event_key")
        assert back.loc["storm1", "max_t2m"] == pytest.approx(5.0)
        assert back.loc["storm2", "max_t2m"] == pytest.approx(9.0)

    def test_directory_store_leaves_rows_in_memory_only(self):
        # A bare-directory store (the default) writes no file; output_path is None
        # and the in-memory results are unchanged (back-compat with Phase 5).
        from zagg.runner import agg

        summary = agg(_temporal_config(), events=_synthetic_events())
        assert summary["output_path"] is None
        assert len(summary["results"]) == 2

    def test_s3_tabular_store_puts_single_object(self, monkeypatch):
        # Remote tabular output (issue #12, Phase 7b): an s3:// store serialises
        # the single Parquet object and puts it via obstore -- the same S3 stack
        # the Zarr store uses, no local-filesystem mangling of the URI.
        import io

        import pandas as pd

        from zagg.config import load_config_from_dict
        from zagg.runner import agg

        captured = _patch_tabular_s3(monkeypatch)

        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "max_t2m": {
                            "variable": "T2M",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                            "mask": "full",
                        }
                    }
                },
                "output": {"format": "parquet", "store": "s3://bucket/events.parquet"},
            }
        )
        creds = {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"}
        summary = agg(cfg, events=_synthetic_events(), output_credentials=creds)
        assert summary["output_path"] == "s3://bucket/events.parquet"
        assert captured["bucket"] == "bucket"
        assert captured["key"] == "events.parquet"
        assert captured["opts"]["access_key_id"] == "a"
        # the payload is real Parquet bytes (PAR1 magic) for the two events
        assert captured["payload"][:4] == b"PAR1"
        back = pd.read_parquet(io.BytesIO(captured["payload"])).set_index("event_key")
        assert back.loc["storm1", "max_t2m"] == pytest.approx(5.0)

    def test_all_error_run_writes_no_file(self, tmp_path):
        # When no event produces a row, the (column-less) tabular write is skipped
        # and output_path is None -- the run still reports its error counts.
        from zagg.config import load_config_from_dict
        from zagg.runner import agg

        path = tmp_path / "events.parquet"
        cfg = load_config_from_dict(
            {
                "pipeline": {"type": "temporal"},
                "data_source": {"reader": "xarray_s3", "collections": ["merra2"]},
                "aggregation": {
                    "variables": {
                        "bad": {
                            "variable": "NOPE",
                            "collection": "merra2",
                            "spatial_func": "max",
                            "temporal_reducer": "max",
                            "mask": "full",
                        }
                    }
                },
                "output": {"format": "parquet", "store": str(path)},
            }
        )
        summary = agg(cfg, events=_synthetic_events())
        assert summary["events_error"] == 2
        assert summary["output_path"] is None
        assert not path.exists()


def _run_lambda_with_durations(
    monkeypatch,
    atl06_config,
    durations,
    *,
    timeout=720,
    profile=False,
    phase_timings=None,
    memories=None,
    containers=None,
    **run_kwargs,
):
    """Drive ``_run_lambda`` over synthetic per-cell durations.

    Returns the summary dict. ``durations`` is consumed one per cell (the
    _run_catalog() has 4 cells); ``timeout`` stubs the function Timeout read.
    ``profile``/``phase_timings`` exercise the phase-2 opt-in path: when
    ``phase_timings`` is set it is attached to each cell result body.
    ``memories`` (issue #120), when given, is consumed one per cell and attached
    as ``body["max_memory_mb"]`` so the peak-memory rollup can be pinned.
    ``containers`` (issue #171), when given, is consumed one per cell and merged
    into each body (container_cold/container_generation/rss_start_mb dicts) so
    the container-telemetry rollup can be pinned.
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
    cont_it = iter(containers) if containers is not None else None

    def _fake_cell(*a, **k):
        body = {"total_obs": 1}
        if phase_timings is not None:
            body["phase_timings"] = phase_timings
        if mem_it is not None:
            body["max_memory_mb"] = next(mem_it)
        if cont_it is not None:
            body.update(next(cont_it))
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
        **run_kwargs,
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


class TestForceCold:
    """``force_cold`` (issue #171): merge a per-run ``ZAGG_COLD_EPOCH`` env
    marker pre-fan-out so every warm sandbox is invalidated, preserving the
    existing environment; failures raise instead of degrading to warm. The
    poll accepts only this update's states (marker match), so a stale
    ``Successful`` from the prior update never returns early."""

    class _FakeLambdaClient:
        """Stateful double mirroring the real API: every
        ``get_function_configuration`` response carries BOTH ``Environment``
        and ``LastUpdateStatus`` (the AWS shape), and the environment only
        reflects the update after ``lag`` post-update reads (eventual
        consistency)."""

        def __init__(self, env=None, statuses=("InProgress", "Successful"), lag=0):
            self.env = dict(env or {})
            self.pending_env = None
            self.statuses = list(statuses)
            self.lag = lag  # reads before the new env becomes visible
            self.update_calls = []
            self.update_error = None
            self._updated = False

        def get_function_configuration(self, FunctionName):  # noqa: N803 (boto3 API)
            if self._updated:
                if self.lag > 0:
                    self.lag -= 1
                    # Stale read: prior env, prior (terminal) status.
                    return {
                        "Environment": {"Variables": dict(self.env)},
                        "LastUpdateStatus": "Successful",
                    }
                self.env = dict(self.pending_env)
                status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
                return {
                    "Environment": {"Variables": dict(self.env)},
                    "LastUpdateStatus": status,
                }
            return {
                "Environment": {"Variables": dict(self.env)},
                "LastUpdateStatus": "Successful",  # prior update's terminal state
            }

        def update_function_configuration(self, FunctionName, Environment):  # noqa: N803
            if self.update_error is not None:
                err, self.update_error = self.update_error, None
                raise err
            self.update_calls.append(Environment["Variables"])
            self.pending_env = dict(Environment["Variables"])
            self._updated = True

    def test_merges_marker_and_preserves_env(self):
        from zagg.runner import _force_cold_containers

        client = self._FakeLambdaClient(env={"MALLOC_ARENA_MAX": "2"})
        _force_cold_containers(client, "process-shard", poll_interval_s=0)
        sent = client.update_calls[0]
        assert sent["MALLOC_ARENA_MAX"] == "2"
        assert len(sent["ZAGG_COLD_EPOCH"]) == 32  # uuid4 hex, unique per run

    def test_stale_successful_from_prior_update_is_not_accepted(self):
        # Eventual consistency: the first post-update reads still show the
        # PRIOR env with LastUpdateStatus=Successful. The poll must keep
        # waiting for the marker, not return on the stale terminal state.
        from zagg.runner import _force_cold_containers

        client = self._FakeLambdaClient(statuses=("InProgress", "Successful"), lag=2)
        _force_cold_containers(client, "process-shard", poll_interval_s=0)
        # 1 env read + 2 stale + InProgress + Successful = 5 reads minimum;
        # early acceptance of a stale Successful would have used only 2.
        assert client.lag == 0

    def test_resource_conflict_retries_until_free(self):
        from zagg.runner import _force_cold_containers

        class _ConflictError(Exception):
            response = {"Error": {"Code": "ResourceConflictException"}}

        client = self._FakeLambdaClient()
        client.update_error = _ConflictError("update in progress")
        _force_cold_containers(client, "process-shard", poll_interval_s=0)
        assert len(client.update_calls) == 1  # succeeded on the retry

    def test_raises_when_update_denied(self):
        import pytest

        from zagg.runner import _force_cold_containers

        class _DeniedError(Exception):
            response = {"Error": {"Code": "AccessDeniedException"}}

        client = self._FakeLambdaClient()
        client.update_error = _DeniedError("AccessDenied")
        with pytest.raises(RuntimeError, match="UpdateFunctionConfiguration"):
            _force_cold_containers(client, "process-shard", poll_interval_s=0)

    def test_raises_when_configuration_unreadable(self):
        import pytest

        from zagg.runner import _force_cold_containers

        class _Client:
            def get_function_configuration(self, FunctionName):  # noqa: N803
                raise RuntimeError("AccessDenied")

        with pytest.raises(RuntimeError, match="GetFunctionConfiguration"):
            _force_cold_containers(_Client(), "process-shard", poll_interval_s=0)

    def test_raises_on_failed_update(self):
        import pytest

        from zagg.runner import _force_cold_containers

        client = self._FakeLambdaClient(statuses=("Failed",))
        with pytest.raises(RuntimeError, match="LastUpdateStatus=Failed"):
            _force_cold_containers(client, "process-shard", poll_interval_s=0)

    def test_raises_when_update_never_lands(self):
        import pytest

        from zagg.runner import _force_cold_containers

        client = self._FakeLambdaClient(statuses=("InProgress",))
        with pytest.raises(RuntimeError, match="warm containers"):
            _force_cold_containers(client, "process-shard", wait_s=0, poll_interval_s=0)

    def test_superseded_by_concurrent_update_counts_as_success(self):
        # A concurrent run's update replaced our marker after ours was
        # accepted; Lambda serializes configuration updates, so every warm
        # sandbox was invalidated regardless -- the poll must count that as
        # success, not spin to deadline (review finding, PR #172).
        from zagg.runner import _force_cold_containers

        class _Client:
            def __init__(self):
                self.updated = False

            def get_function_configuration(self, FunctionName):  # noqa: N803
                if self.updated:
                    return {
                        "Environment": {"Variables": {"ZAGG_COLD_EPOCH": "someone-else"}},
                        "LastUpdateStatus": "Successful",
                    }
                return {"Environment": {"Variables": {}}, "LastUpdateStatus": "Successful"}

            def update_function_configuration(self, FunctionName, Environment):  # noqa: N803
                self.updated = True

        _force_cold_containers(_Client(), "process-shard", poll_interval_s=0)

    def test_conflict_past_deadline_reports_conflict_not_permissions(self):
        import pytest

        from zagg.runner import _force_cold_containers

        class _ConflictError(Exception):
            response = {"Error": {"Code": "ResourceConflictException"}}

        class _Client:
            def get_function_configuration(self, FunctionName):  # noqa: N803
                return {"Environment": {"Variables": {}}, "LastUpdateStatus": "Successful"}

            def update_function_configuration(self, FunctionName, Environment):  # noqa: N803
                raise _ConflictError("in flight")

        with pytest.raises(RuntimeError, match="another configuration update"):
            _force_cold_containers(_Client(), "process-shard", wait_s=0, poll_interval_s=0)

    def test_run_lambda_forces_cold_before_dispatch(self, monkeypatch, atl06_config):
        from zagg import runner

        calls = []
        monkeypatch.setattr(runner, "_force_cold_containers", lambda client, fn: calls.append(fn))
        _run_lambda_with_durations(monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0], force_cold=True)
        assert calls == ["process-shard"]

    def test_opt_out_never_touches_configuration(self, monkeypatch, atl06_config):
        from zagg import runner

        calls = []
        monkeypatch.setattr(runner, "_force_cold_containers", lambda client, fn: calls.append(fn))
        _run_lambda_with_durations(
            monkeypatch, atl06_config, [1.0, 2.0, 3.0, 4.0], force_cold=False
        )
        assert calls == []

    def test_agg_defaults_force_cold_off(self):
        # espg's decision on the PR #172 plan: default False. force_cold is
        # the explicit certification-run tool (it needs a broad write
        # permission and chills every caller's warm pool), not a tax on every
        # agg() call; routine ratchet protection is worker-side instead.
        import inspect

        from zagg.runner import agg

        assert inspect.signature(agg).parameters["force_cold"].default is False


class TestContainerTelemetrySummary:
    """Issue #171 (detect-and-report): the runner rolls the workers' container
    telemetry into additive summary fields -- cold/warm counts and the max
    start-RSS per sandbox generation (the #169 ratchet made visible)."""

    def test_rollup_counts_and_ratchet(self):
        from zagg.runner import _container_telemetry_summary

        bodies = [
            {"container_cold": True, "container_generation": 1, "rss_start_mb": 310.0},
            {"container_cold": False, "container_generation": 2, "rss_start_mb": 959.0},
            {"container_cold": False, "container_generation": 2, "rss_start_mb": 640.0},
            {"container_cold": False, "container_generation": 3, "rss_start_mb": 1650.0},
        ]
        stats = _container_telemetry_summary(bodies)
        assert stats["worker_cold_starts"] == 1
        assert stats["worker_warm_starts"] == 3
        # Max per generation, ordered: the ratchet signature (climbing start-RSS).
        assert stats["worker_rss_start_max_by_gen"] == {1: 310.0, 2: 959.0, 3: 1650.0}

    def test_no_telemetry_is_none_not_zero(self):
        # Older deployed workers stamp no container fields: the rollup must be
        # None (no data), never 0 cold / 0 warm (which would read as all-warm).
        from zagg.runner import _container_telemetry_summary

        stats = _container_telemetry_summary([{"total_obs": 1}, {}])
        assert stats == {
            "worker_cold_starts": None,
            "worker_warm_starts": None,
            "worker_rss_start_max_by_gen": None,
        }

    def test_mixed_fleet_counts_only_reporting_workers(self):
        # A mid-rollout fleet (some workers redeployed, some not): only the
        # envelopes that carry telemetry are counted, and a missing
        # rss_start_mb (non-Linux fallback) is skipped, not treated as 0.
        from zagg.runner import _container_telemetry_summary

        bodies = [
            {"container_cold": True, "container_generation": 1, "rss_start_mb": None},
            {"total_obs": 1},  # pre-telemetry worker
        ]
        stats = _container_telemetry_summary(bodies)
        assert stats["worker_cold_starts"] == 1
        assert stats["worker_warm_starts"] == 0
        assert stats["worker_rss_start_max_by_gen"] == {}

    def test_lambda_summary_carries_rollup(self, monkeypatch, atl06_config):
        containers = [
            {"container_cold": True, "container_generation": 1, "rss_start_mb": 300.0},
            {"container_cold": True, "container_generation": 1, "rss_start_mb": 320.0},
            {"container_cold": False, "container_generation": 2, "rss_start_mb": 1100.0},
            {"container_cold": False, "container_generation": 2, "rss_start_mb": 900.0},
        ]
        summary = _run_lambda_with_durations(
            monkeypatch, atl06_config, [1.0, 1.0, 1.0, 1.0], containers=containers
        )
        assert summary["worker_cold_starts"] == 2
        assert summary["worker_warm_starts"] == 2
        assert summary["worker_rss_start_max_by_gen"] == {1: 320.0, 2: 1100.0}

    def test_lambda_summary_without_telemetry_is_none(self, monkeypatch, atl06_config):
        # Default fake bodies carry no container fields -> the additive keys
        # exist but are None, so downstream consumers see "no data".
        summary = _run_lambda_with_durations(monkeypatch, atl06_config, [1.0, 1.0, 1.0, 1.0])
        assert summary["worker_cold_starts"] is None
        assert summary["worker_warm_starts"] is None
        assert summary["worker_rss_start_max_by_gen"] is None


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
            runner,
            "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(
            atl06_config,
            catalog="ignored",
            store="s3://out/x.zarr",
            backend="lambda",
            handoff="arrow",
        )
        assert captured["handoff"] == "arrow"

    def test_agg_default_handoff_is_arrow_on_lambda(self, monkeypatch, atl06_config):
        # issue #132: with no kwarg and no config field, agg() resolves the carrier
        # from get_handoff(config), which defaults to arrow.
        from zagg import runner

        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner,
            "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(atl06_config, catalog="ignored", store="s3://out/x.zarr", backend="lambda")
        assert captured["handoff"] == "arrow"

    def test_agg_reads_handoff_from_config(self, monkeypatch, atl06_config):
        # issue #132: with no kwarg, the carrier comes from aggregation.handoff so a
        # nullable-source pipeline can declare pandas in its YAML.
        from zagg import runner

        atl06_config.aggregation["handoff"] = "pandas"
        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner,
            "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(atl06_config, catalog="ignored", store="s3://out/x.zarr", backend="lambda")
        assert captured["handoff"] == "pandas"

    def test_agg_handoff_kwarg_overrides_config(self, monkeypatch, atl06_config):
        # issue #132: an explicit handoff= kwarg wins over aggregation.handoff,
        # mirroring the driver precedence.
        from zagg import runner

        atl06_config.aggregation["handoff"] = "pandas"
        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner,
            "_run_lambda",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(
            atl06_config,
            catalog="ignored",
            store="s3://out/x.zarr",
            backend="lambda",
            handoff="arrow",
        )
        assert captured["handoff"] == "arrow"

    def test_agg_reads_handoff_from_config_on_local(self, monkeypatch, atl06_config):
        # issue #132: the config-derived carrier reaches the local backend too
        # (resolution is backend-agnostic, before the local/lambda split).
        from zagg import runner

        atl06_config.aggregation["handoff"] = "pandas"
        captured = {}
        monkeypatch.setattr(runner, "_load_catalog", lambda p: _run_catalog())
        monkeypatch.setattr(
            runner,
            "_run_local",
            lambda *a, **k: captured.update(handoff=k.get("handoff")) or {},
        )
        runner.agg(atl06_config, catalog="ignored", store="./out.zarr", backend="local")
        assert captured["handoff"] == "pandas"


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
        # This test pins the worker's phase brackets, not the read backend:
        # pin the hierarchical delegation seam the _read_group stub intercepts
        # (the issue #170 default otherwise resolves to inline, which reads
        # through its own chunk-aligned path and hits the _H5 stub).
        from zagg.index.hierarchical import HierarchicalIndex

        monkeypatch.setattr(worker, "index_from_config", lambda cfg: HierarchicalIndex())
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
            lambda *a, **k: ({}, {}, {}, {}, 1),
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


class TestConsolidationGate:
    """Issue #191: metadata consolidation is opt-in. With the flag off (default)
    the local runner skips the ``consolidate_metadata`` call and the Lambda
    dispatcher skips the ``mode: "finalize"`` invoke entirely; with the flag on
    both still run, byte-identical to the pre-#191 behavior."""

    def _local_consolidate_calls(self, monkeypatch, atl06_config, *, enabled):
        import zagg.grids as grids_mod
        from zagg import runner

        monkeypatch.setattr(
            runner,
            "get_nsidc_s3_credentials",
            lambda: {"accessKeyId": "a", "secretAccessKey": "s", "sessionToken": "t"},
        )
        monkeypatch.setattr(grids_mod, "from_config", lambda *a, **k: _stub_grid())
        monkeypatch.setattr(runner, "open_store", lambda *a, **k: object())
        calls = []
        monkeypatch.setattr(runner, "consolidate_metadata", lambda *a, **k: calls.append(1))
        monkeypatch.setattr(
            runner,
            "_process_and_write",
            lambda shard_key, *a, **k: {"shard_key": shard_key, "total_obs": 1, "error": None},
        )
        if enabled:
            atl06_config.output = {**atl06_config.output, "consolidate_metadata": True}
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
        return len(calls)

    def test_local_skips_consolidation_by_default(self, monkeypatch, atl06_config):
        assert self._local_consolidate_calls(monkeypatch, atl06_config, enabled=False) == 0

    def test_local_consolidates_when_enabled(self, monkeypatch, atl06_config):
        assert self._local_consolidate_calls(monkeypatch, atl06_config, enabled=True) == 1

    def _lambda_finalize_calls(self, monkeypatch, atl06_config, *, enabled):
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
        monkeypatch.setattr(runner, "_get_function_timeout_s", lambda *a, **k: 720)
        calls = []
        monkeypatch.setattr(runner, "_invoke_lambda_finalize", lambda *a, **k: calls.append(1))
        from unittest.mock import MagicMock

        monkeypatch.setattr(boto3, "Session", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            runner,
            "compute_available_workers",
            lambda requested, *a, **k: (
                1,
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
            lambda client, chunk_idx, shard_key, *a, **k: {
                "shard_key": shard_key,
                "status_code": 200,
                "body": {"total_obs": 1},
                "error": None,
                "timeout": False,
            },
        )
        if enabled:
            atl06_config.output = {**atl06_config.output, "consolidate_metadata": True}
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
        return len(calls)

    def test_lambda_skips_finalize_by_default(self, monkeypatch, atl06_config):
        assert self._lambda_finalize_calls(monkeypatch, atl06_config, enabled=False) == 0

    def test_lambda_finalizes_when_enabled(self, monkeypatch, atl06_config):
        assert self._lambda_finalize_calls(monkeypatch, atl06_config, enabled=True) == 1


class TestResolveSourceCredentials:
    """Provider-selected source credentials, both pipelines (issue #213 Phase 6)."""

    def test_default_is_nsidc_via_module_global(self, monkeypatch):
        from zagg import runner
        from zagg.config import PipelineConfig

        monkeypatch.setattr(runner, "get_nsidc_s3_credentials", lambda: {"accessKeyId": "n"})
        cfg = PipelineConfig(data_source={"reader": "h5coro"})
        assert runner._resolve_source_credentials(cfg) == {"accessKeyId": "n"}

    def test_named_provider_resolved_via_registry(self):
        from zagg import registry as zagg_registry
        from zagg import runner
        from zagg.config import PipelineConfig

        zagg_registry.register_credential_provider(
            "test_src_provider", lambda: {"accessKeyId": "p"}, replace=True
        )
        try:
            cfg = PipelineConfig(data_source={"credentials_provider": "test_src_provider"})
            assert runner._resolve_source_credentials(cfg) == {"accessKeyId": "p"}
        finally:
            zagg_registry.CREDENTIAL_PROVIDERS._entries.pop("test_src_provider", None)

    def test_unknown_provider_raises_with_name(self):
        from zagg import runner
        from zagg.config import PipelineConfig

        cfg = PipelineConfig(data_source={"credentials_provider": "not_a_provider"})
        with pytest.raises(KeyError, match="not_a_provider"):
            runner._resolve_source_credentials(cfg)
