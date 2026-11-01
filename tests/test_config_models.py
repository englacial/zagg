"""The pydantic template grammar (issue #677): the hash-stability contract and
the path-addressed refusals.

The models are a validated VIEW over the raw config dicts, never the stored
form: ``PipelineConfig`` keeps what the author wrote, so
``model_dump(exclude_unset=True)`` must reproduce every block key for key,
and a config rebuilt from that dump must canonicalize to the same semantic
JSON — on every packaged template, every ``tools/configs`` measurement arm,
every benchmark config, and every vendored run-record config. That pair is what keeps every
``semantic_hash`` pin in ``test_live_store_templates.py`` byte-identical.
"""

import json
from importlib import resources
from pathlib import Path

import pytest
import yaml

import zagg.configs
from zagg.config import (
    PipelineConfig,
    RasterConfig,
    SpatialConfig,
    TemporalConfig,
    default_config,
    load_config_from_dict,
    validate_config,
    validate_model,
)
from zagg.semantics import canonical_semantic_json, semantic_hash

REPO = Path(__file__).parents[1]
PACKAGED = sorted(
    f.name.removesuffix(".yaml")
    for f in resources.files(zagg.configs).iterdir()
    if f.name.endswith(".yaml")
)
MEASURE_CONFIGS = sorted((REPO / "tools" / "configs").glob("*.yaml"))
RUN_RECORDS = sorted((REPO / "tests" / "data").glob("*.config.json"))
BENCHMARK = sorted((REPO / "tests" / "data" / "benchmark" / "configs").glob("*.yaml"))
BLOCKS = ("data_source", "aggregation", "output", "pipeline", "worker", "catalog", "bounds")


def _every_config():
    for name in PACKAGED:
        yield name, default_config(name)
    for path in MEASURE_CONFIGS + BENCHMARK:
        yield path.name, load_config_from_dict(yaml.safe_load(path.read_text()))
    for path in RUN_RECORDS:
        yield path.name, load_config_from_dict(json.loads(path.read_text()))


ALL = list(_every_config())


@pytest.mark.parametrize(("name", "cfg"), ALL, ids=[n for n, _ in ALL])
def test_dump_reproduces_the_raw_blocks(name, cfg):
    # The models inject no defaults and rewrite no values: the exclude_unset
    # dump IS the author's dict, block by block (an absent block dumps absent).
    # Compared as JSON text so a type rewrite (1 -> 1.0, True -> 1) fails too.
    dump = validate_model(cfg).model_dump(exclude_unset=True, mode="python")
    for block in BLOCKS:
        raw = getattr(cfg, block)
        if block in dump:
            assert json.dumps(dump[block], sort_keys=True, default=str) == json.dumps(
                raw, sort_keys=True, default=str
            ), block
        else:
            assert raw in (None, {}), block


@pytest.mark.parametrize(("name", "cfg"), ALL, ids=[n for n, _ in ALL])
def test_loaded_and_raw_canonicalize_identically(name, cfg):
    # Issue #677's phase 1 obligation: canonical_semantic_json(load(x)) ==
    # canonical_semantic_json(raw_dict(x)), with the loaded side rebuilt from
    # the model's dump so a default leaking into the hash would fail here.
    rebuilt = load_config_from_dict(validate_model(cfg).model_dump(exclude_unset=True))
    assert canonical_semantic_json(rebuilt) == canonical_semantic_json(cfg)
    assert semantic_hash(rebuilt) == semantic_hash(cfg)


@pytest.mark.parametrize("name", PACKAGED)
def test_typed_view_is_additive(name):
    cfg = default_config(name)
    model = cfg.model
    assert isinstance(model, (SpatialConfig, RasterConfig, TemporalConfig))
    # The raw dicts are untouched by the view.
    assert (
        cfg.data_source
        == yaml.safe_load(resources.files(zagg.configs).joinpath(f"{name}.yaml").read_text())[
            "data_source"
        ]
    )


def test_root_union_follows_validate_config_branching():
    assert isinstance(default_config("atl06").model, SpatialConfig)
    assert isinstance(default_config("sentinel2_l2a").model, RasterConfig)
    assert isinstance(default_config("merra2_storm").model, TemporalConfig)


class TestPathAddressedRefusals:
    def _cfg(self, **ds):
        return PipelineConfig(
            data_source={"variables": {"h": "/h"}, **ds},
            aggregation={"variables": {"n": {"function": "len", "source": "h", "dtype": "int32"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )

    def test_misspelled_top_level_key_names_its_path(self):
        with pytest.raises(
            ValueError, match=r"data_source\.filtres: Extra inputs are not permitted"
        ):
            validate_config(self._cfg(filtres=[]))

    def test_misspelled_nested_key_names_its_path(self):
        cfg = self._cfg(read_plan={"spatial_index": "segments", "padding": 1})
        with pytest.raises(ValueError, match=r"data_source\.read_plan\.padding: Extra inputs"):
            validate_config(cfg)

    def test_union_member_tag_never_leaks_into_the_path(self):
        cfg = self._cfg(variables={"h": "/h", "c": {"path": "/p", "column": 0, "slice": 1}})
        with pytest.raises(ValueError, match=r"data_source\.variables\.c\.slice: Extra inputs"):
            validate_config(cfg)

    def test_hyphenated_author_key_stays_in_the_path(self):
        # Tags are stripped by exact name, so an author key that merely looks
        # like one (``x-config``) still addresses the refusal.
        cfg = self._cfg(variables={"h": "/h", "x-config": {"path": "/p", "column": -1}})
        with pytest.raises(ValueError, match=r"data_source\.variables\.x-config\.column: "):
            validate_config(cfg)

    def test_cross_check_messages_carry_the_path_once(self):
        # The renderer adds ``data_source``; the cross-checks name only the
        # key under it, spelled like pydantic's own loc (``filters[i]``).
        cfg = self._cfg(levels={"p": {"path": "/p"}})
        with pytest.raises(ValueError, match=r"^data_source: base_level is required when levels"):
            validate_config(cfg)
        cfg = self._cfg(
            levels={"p": {"path": "/p"}},
            base_level="p",
            filters=[{"dataset": "/d", "op": "eq", "value": 0, "level": "zz"}],
        )
        with pytest.raises(
            ValueError, match=r"^data_source: filters\[0\]: level 'zz' is not a key in levels"
        ):
            validate_config(cfg)

    def test_list_index_renders_as_subscript(self):
        cfg = self._cfg(filters=[{"dataset": "/d", "op": "eq", "value": 0, "keeps": True}])
        with pytest.raises(ValueError, match=r"data_source\.filters\[0\]\.keeps: Extra inputs"):
            validate_config(cfg)

    def test_validator_messages_are_verbatim(self):
        # A ValueError raised inside a model validator is reported without
        # pydantic's "Value error, " prefix, so the rule reads as it always has.
        cfg = self._cfg(granule_workers=0)
        with pytest.raises(
            ValueError, match=r"data_source\.granule_workers: must be an integer >= 1"
        ):
            validate_config(cfg)

    def test_pydantic_never_escapes(self):
        import pydantic

        with pytest.raises(ValueError) as info:
            validate_config(self._cfg(bogus=1))
        assert not isinstance(info.value, pydantic.ValidationError)
        assert isinstance(info.value.__cause__, pydantic.ValidationError)


class TestBooleanSafeScalars:
    """pydantic's lax mode accepts True for an int and 5.0 for an int; the
    grammar's scalar types do not, and they keep the author's numeric type."""

    def _cfg(self, **ds):
        return TestPathAddressedRefusals()._cfg(**ds)

    @pytest.mark.parametrize("bad", [True, 2.0, "2", 0])
    def test_fan_out_widths_reject_non_ints(self, bad):
        with pytest.raises(ValueError, match="read_workers: must be an integer >= 1"):
            validate_config(self._cfg(read_workers=bad))

    def test_filter_value_keeps_its_type(self):
        cfg = self._cfg(filters=[{"dataset": "/d", "op": "eq", "value": 2}])
        model = validate_model(cfg)
        value = model.data_source.filters[0].value
        assert value == 2 and isinstance(value, int) and not isinstance(value, bool)
        cfg = self._cfg(filters=[{"dataset": "/d", "op": "eq", "value": 2.5}])
        assert validate_model(cfg).data_source.filters[0].value == 2.5

    def test_filter_value_rejects_bool(self):
        cfg = self._cfg(filters=[{"dataset": "/d", "op": "eq", "value": True}])
        with pytest.raises(ValueError, match=r"filters\[0\]\.value: must be numeric"):
            validate_config(cfg)


class TestDataSourceGrammar:
    def test_normalized_filters_round_trip(self):
        # The worker-side filter list carries explicit None/keep keys
        # (``windowed_cell_config``); the grammar accepts the normalized form.
        from zagg.config import filters_from_data_source

        ds = {
            "variables": {"t": "/t"},
            "filters": [{"dataset": "/d", "op": "in", "values": [1, 2]}, {"expression": "t > 0"}],
        }
        normalized = filters_from_data_source(ds)
        cfg = PipelineConfig(data_source={**ds, "filters": normalized})
        model = validate_model(cfg)
        assert [type(f).__name__ for f in model.data_source.filters] == [
            "StructuredFilter",
            "ExpressionFilter",
        ]
        assert model.model_dump(exclude_unset=True)["data_source"]["filters"] == normalized

    def test_structured_filter_operands(self):
        cfg = PipelineConfig(data_source={"filters": [{"dataset": "/d", "op": "in", "value": 1}]})
        with pytest.raises(ValueError, match="op 'in' requires a 'values' list"):
            validate_model(cfg)
        cfg = PipelineConfig(data_source={"filters": [{"dataset": "/d", "op": "lt"}]})
        with pytest.raises(ValueError, match="op 'lt' requires a scalar 'value'"):
            validate_model(cfg)
        cfg = PipelineConfig(
            data_source={"filters": [{"dataset": "/d", "op": "eq", "value": None}]}
        )
        with pytest.raises(ValueError, match=r"filters\[0\]: 'value' must be numeric \(got None\)"):
            validate_model(cfg)

    def test_segment_variable_names_on_the_model(self):
        cfg = default_config("atl03_waveform_chunk")
        assert cfg.model.data_source.segment_variable_names == {"dem_h"}
        assert default_config("atl06").model.data_source.segment_variable_names == set()

    def test_unknown_reader_stays_on_the_point_grammar(self):
        # Today's behavior: only ``raster`` selects another spatial grammar; a
        # plugin or unknown reader name on a spatial pipeline reads as h5coro.
        cfg = PipelineConfig(data_source={"reader": "generic", "variables": {"h": "/h"}})
        assert isinstance(validate_model(cfg), SpatialConfig)

    def test_collection_options_pass_unknown_keys_through(self):
        cfg = PipelineConfig(
            pipeline={"type": "temporal"},
            data_source={"reader": "xarray_s3", "collections": {"m": {"doi": "10.1/x"}}},
            aggregation={
                "variables": {
                    "t": {
                        "variable": "T",
                        "collection": "m",
                        "spatial_func": "max",
                        "temporal_reducer": "max",
                    }
                }
            },
        )
        model = validate_model(cfg)
        assert model.data_source.collections["m"].model_dump(exclude_unset=True) == {
            "doi": "10.1/x"
        }

    def test_raster_band_shape(self):
        cfg = PipelineConfig(
            data_source={"reader": "raster", "bands": {"red": {"asset": "red"}}},
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        with pytest.raises(ValueError, match=r"data_source\.bands\.red\.dtype: Field required"):
            validate_config(cfg)

    @pytest.mark.parametrize("key", ["fill_value", "scale", "offset"])
    def test_raster_band_numbers_refuse_null(self, key):
        cfg = default_config("sentinel2_l2a")
        band = next(iter(cfg.data_source["bands"]))
        cfg.data_source["bands"][band][key] = None
        with pytest.raises(
            ValueError, match=rf"data_source\.bands\.{band}: {key} must be a number \(got None\)"
        ):
            validate_config(cfg)


class TestAggregationGrammar:
    """Phase 2: the per-field grammar is the model's; the typed view is additive."""

    def test_typed_fields(self):
        model = default_config("atl03_tdigest_strata_healpix").model
        digest = model.aggregation.variables["h_tdigest_signal"]
        assert digest.kind == "ragged" and digest.inner_shape == [2]
        assert digest.location == "leaf_id" and digest.temporal == "per-centroid"
        assert model.aggregation.streaming.mode == "spill"
        assert model.aggregation.coordinates["morton"].dtype == "uint64"

    def test_misspelled_field_key_names_its_path(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.aggregation["variables"]["n"]["fil_value"] = 0
        with pytest.raises(ValueError, match=r"aggregation\.variables\.n\.fil_value: Extra inputs"):
            validate_config(cfg)

    def test_streaming_block_is_strict(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.aggregation["streaming"] = {"mode": "spill", "arena_backing": "x"}
        with pytest.raises(ValueError, match=r"streaming\.arena_backing: Extra inputs"):
            validate_config(cfg)
        cfg.aggregation["streaming"] = {"block_bytes": 1 << 20}
        with pytest.raises(ValueError, match="block_bytes only applies to mode: spill"):
            validate_config(cfg)

    def test_temporal_variables_are_typed(self):
        model = default_config("merra2_storm").model
        spec = model.aggregation.variables["anom_tqv_full"]
        assert spec.spatial_func == "weighted_mean" and spec.anomaly is True

    def test_raster_refuses_an_aggregation_block(self):
        cfg = default_config("sentinel2_l2a")
        cfg.aggregation = {"variables": {"x": {"function": "mean"}}}
        with pytest.raises(ValueError, match="raster pipelines take no aggregation section"):
            validate_config(cfg)

    def test_field_shim_prefixes_the_name(self):
        from zagg.config import _validate_output_kind

        with pytest.raises(
            ValueError, match=r"Variable 'f': kind 'vector' requires 'trailing_shape'"
        ):
            _validate_output_kind("f", {"function": "min", "kind": "vector"})

    @pytest.mark.parametrize(
        "meta",
        [
            {"function": "histogram", "kind": "vector", "trailing_shape": None},
            {"function": "min", "kind": "ragged", "inner_shape": None},
        ],
    )
    def test_explicit_null_shape_is_refused(self, meta):
        from zagg.config import _validate_output_kind

        with pytest.raises(ValueError, match=r"must be an int or a sequence of ints \(got None\)"):
            _validate_output_kind("f", meta)

    def test_overview_delta_schema_is_a_positive_int(self):
        from zagg.config.models_aggregation import FieldSpec

        prop = FieldSpec.model_json_schema()["properties"]["overview_delta"]
        # No null branch: the key is omitted for the default, never null.
        assert (prop["type"], prop["minimum"]) == ("integer", 1) and "anyOf" not in prop
        with pytest.raises(ValueError, match="overview_delta must be a positive int"):
            FieldSpec.model_validate(
                {"function": "min", "kind": "ragged", "inner_shape": [2], "overview_delta": 0}
            )

    @pytest.mark.parametrize(
        "where, key",
        [
            ("field", "params"),
            ("field", "function"),
            ("field", "source"),
            ("field", "dtype"),
            ("agg", "chunk_precompute"),
            ("agg", "variables"),
            ("agg", "coordinates"),
            ("agg", "handoff"),
            ("streaming", "mode"),
            ("streaming", "buffer_granules"),
        ],
    )
    def test_explicit_null_is_refused(self, where, key):
        cfg = TestPathAddressedRefusals()._cfg()
        agg = cfg.aggregation
        target = {"field": agg["variables"]["n"], "agg": agg, "streaming": {}}[where]
        if where == "streaming":
            agg["streaming"] = target
        target[key] = None
        with pytest.raises(ValueError, match=f"{key} must not be null \\(omit the key"):
            validate_config(cfg)

    def test_explicit_null_fill_value_stays_valid(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.aggregation["variables"]["n"]["fill_value"] = None
        cfg.aggregation["coordinates"] = {"morton": {"dtype": "uint64", "fill_value": None}}
        validate_config(cfg)

    def test_non_list_composition_lanes_is_a_value_error(self):
        from zagg.config.models_aggregation import Aggregation

        comp = {
            "function": "zagg.stats.composition.pack_composition",
            "dtype": "uint64",
            "fill_value": 0,
            "attrs": {"composition": {"lanes": 5}},
        }
        with pytest.raises(ValueError, match=r"attrs\.composition\.lanes must be the"):
            Aggregation.model_validate({"variables": {"c": comp}})

    def test_coordinate_dtype_is_optional_and_checked(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.aggregation["coordinates"] = {"x": {}}
        validate_config(cfg)
        cfg.aggregation["coordinates"] = {"x": {"dtype": "bogus"}}
        with pytest.raises(
            ValueError, match=r"coordinates\.x\.dtype: dtype 'bogus' is not a valid"
        ):
            validate_config(cfg)


class TestOutputGrammar:
    """Phase 3: the output block's shape and its intra-block couplings are the model's."""

    def test_typed_output(self):
        model = default_config("atl03_tdigest_strata_healpix").model
        out = model.output
        assert out.grid.type == "healpix" and out.grid.parent_order == 9
        assert out.windowing.schedule == "yearly" and out.windowing.declares_schedule
        assert out.pyramid.overviews == [16, 15, 14, 13]
        assert out.resolved_store_layout == "hive" and out.sweep_enabled is True
        rect = default_config("atl03").model.output
        assert rect.grid.type == "rectilinear" and rect.resolved_store_layout == "flat"

    def test_misspelled_output_key_names_its_path(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["stor"] = "."
        with pytest.raises(ValueError, match=r"output\.stor: Extra inputs are not permitted"):
            validate_config(cfg)
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["grid"]["chunk_iner"] = 8
        with pytest.raises(ValueError, match=r"output\.grid\.chunk_iner: Extra inputs"):
            validate_config(cfg)

    def test_grid_union_follows_type(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["grid"] = {"type": "rectilinear", "crs": "EPSG:3031", "resolution": 100}
        with pytest.raises(ValueError, match=r"output\.grid\.bounds: Field required"):
            validate_config(cfg)
        cfg.output["grid"] = {"child_order": 12}
        with pytest.raises(ValueError, match=r"output\.grid\.type: Field required"):
            validate_config(cfg)

    def test_explicit_windows_are_typed_and_point_sugar_rides(self):
        cfg = default_config("atl03_tdigest_strata_healpix")
        cfg.output["windowing"] = {
            "schedule": "explicit",
            "time_field": "delta_time",
            "epoch": "2018-01-01T00:00:00Z",
            "scale": "gps",
            "windows": [
                {"label": "a", "start": "2019-01-01", "end": "2019-02-01"},
                {"label": "b", "timestamp": "2019-06-01T12:00:00Z"},
            ],
        }
        validate_config(cfg)
        windows = cfg.model.output.windowing.windows
        assert [type(w).__name__ for w in windows] == ["WindowRange", "WindowPoint"]
        cfg.output["windowing"]["windows"].append({"label": "c", "timestamp": "x", "width": 1})
        with pytest.raises(
            ValueError, match=r"windows\[2\]: explicit point window entry carries unsupported key"
        ):
            validate_config(cfg)

    def test_raster_couplings_live_on_the_raster_kind(self):
        cfg = default_config("sentinel2_l2a")
        cfg.output["grid"]["sharded"] = True
        with pytest.raises(ValueError, match="read-modify-write"):
            validate_config(cfg)
        cfg = default_config("sentinel2_l2a")
        cfg.output["windowing"] = {"schedule": "yearly", "unit": "shard"}
        with pytest.raises(ValueError, match="unit does not apply to raster"):
            validate_config(cfg)

    def test_time_encoding_is_raster_only(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["time_encoding"] = "toc"
        with pytest.raises(ValueError, match="applies to raster"):
            validate_config(cfg)
        cfg.output["time_encoding"] = "microseconds"
        validate_config(cfg)

    def test_hive_companions_need_hive(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["store_layout"] = "flat"
        for key in ("coverage_moc", "leaf_versions", "icechunk"):
            cfg.output[key] = True
            with pytest.raises(ValueError, match="store_layout: hive"):
                validate_config(cfg)
            del cfg.output[key]
        cfg.output["sweep"] = "none"  # a spelled opt-out on a flat store is legal
        validate_config(cfg)

    def test_dump_keeps_yaml_datetimes(self):
        import datetime

        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {
            "schedule": "yearly",
            "time_field": "h",
            "epoch": datetime.datetime(2018, 1, 1),
        }
        validate_config(cfg)
        dump = validate_model(cfg).model_dump(exclude_unset=True, mode="python")
        assert dump["output"]["windowing"]["epoch"] == datetime.datetime(2018, 1, 1)
        cfg.output["windowing"]["epoch"] = datetime.date(2018, 1, 1)  # unquoted YAML date
        validate_config(cfg)
        dump = validate_model(cfg).model_dump(exclude_unset=True, mode="python")
        assert type(dump["output"]["windowing"]["epoch"]) is datetime.date

    def test_numeric_epoch_is_refused_by_path(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["store_layout"] = "hive"
        cfg.output["windowing"] = {"schedule": "yearly", "time_field": "h", "epoch": 1514764800}
        with pytest.raises(
            ValueError, match=r"output\.windowing\.epoch: must be an ISO-8601 instant string"
        ):
            validate_config(cfg)

    @pytest.mark.parametrize("order", ["3", 3.0, True])
    def test_pyramid_orders_are_strict_ints(self, order):
        cfg = default_config("atl03_tdigest_healpix_hive")
        cfg.output["pyramid"] = {"orders": [order]}
        with pytest.raises(
            ValueError, match=r"output\.pyramid\.orders\[0\]: must be a non-negative"
        ):
            validate_config(cfg)

    def test_summarize_dumps_key_for_key(self):
        cfg = default_config("atl03_tdigest_healpix_hive")
        field = next(iter(cfg.aggregation["variables"]))
        decl = {"as": f"{field}_sum", "function": "sum"}
        cfg.output["pyramid"] = {"summarize": {field: decl}}
        validate_config(cfg)
        dump = validate_model(cfg).model_dump(exclude_unset=True)
        assert dump["output"]["pyramid"]["summarize"] == {field: decl}
        rebuilt = PipelineConfig(**{k: dump.get(k) for k in BLOCKS})
        validate_config(rebuilt)

    @pytest.mark.parametrize(
        "block, key",
        [
            ("pyramid", "all_time"),
            ("pyramid", "spacing"),
            ("windowing", "epoch"),
            ("icechunk", "commit"),
            ("time_source", "scale"),
        ],
    )
    def test_explicit_null_in_an_output_block_is_refused(self, block, key):
        cfg = default_config("atl03_tdigest_strata_healpix")
        cfg.output.setdefault(block, {})
        if block == "time_source":
            cfg.output[block] = {"field": "delta_time", "epoch": "2018-01-01T00:00:00Z"}
        cfg.output[block][key] = None
        with pytest.raises(ValueError, match=rf"output\.{block}: {key} must not be null"):
            validate_config(cfg)

    def test_explicit_null_on_an_output_knob(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["touch"] = None
        with pytest.raises(ValueError, match=r"^output: touch must not be null"):
            validate_config(cfg)
        cfg = TestPathAddressedRefusals()._cfg()
        # The accessors document a present-but-null key as the default here.
        for key in ("store_layout", "coverage_moc", "sweep", "leaf_versions", "icechunk"):
            cfg.output[key] = None
        cfg.output["pyramid"] = cfg.output["windowing"] = None
        validate_config(cfg)

    @pytest.mark.parametrize("key", ["sharded", "chunk_inner", "emit_cell_ids"])
    def test_explicit_null_on_a_grid_key_is_refused(self, key):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["grid"][key] = None
        with pytest.raises(ValueError, match=rf"output\.grid: {key} must not be null"):
            validate_config(cfg)
        cfg = default_config("atl03")
        cfg.output["grid"]["chunk_shape"] = None
        with pytest.raises(ValueError, match=r"output\.grid: chunk_shape must not be null"):
            validate_config(cfg)

    def test_null_grid_is_refused(self):
        cfg = default_config("atl03")
        cfg.output["grid"] = None
        with pytest.raises(ValueError, match=r"output\.grid: must be a mapping"):
            validate_config(cfg)
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.output["grid"].update(layout=None, cell_ids_encoding=None)  # documented nulls
        validate_config(cfg)

    def test_closed_vocabularies_export_their_enums(self):
        from zagg.config.accessors import TOUCH_POLICIES
        from zagg.config.models_output import Output, Pyramid, TimeSource, Windowing
        from zagg.sweep_overview import FOLD_SOURCES
        from zagg.time_axis import TIME_ENCODINGS, TOC_SOURCE_SCALES
        from zagg.windows import EPOCH_SCALES, UNIT_SECONDS

        def enum(model, key):
            return model.model_json_schema()["properties"][key]["enum"]

        assert enum(Output, "touch") == list(TOUCH_POLICIES)
        assert enum(Output, "time_encoding") == list(TIME_ENCODINGS)
        assert enum(Windowing, "scale") == list(EPOCH_SCALES)
        assert enum(Windowing, "units") == enum(TimeSource, "units") == list(UNIT_SECONDS)
        assert enum(TimeSource, "scale") == list(TOC_SOURCE_SCALES)
        assert enum(Pyramid, "fold_source") == list(FOLD_SOURCES)

    def test_raster_summarize_is_cross_checked(self):
        cfg = default_config("sentinel2_l2a")
        cfg.output["pyramid"] = {"summarize": {"B04": {"as": "b4_mean"}}}
        with pytest.raises(ValueError, match="summarize names unknown field 'B04'"):
            validate_config(cfg)


class TestRunGrammar:
    """Phase 4: ``pipeline``, ``worker``, ``catalog`` and ``bounds`` are typed per kind."""

    def test_typed_run_blocks(self):
        model = default_config("gedi01b_waveform_healpix_hive").model
        assert model.pipeline.type == "spatial"
        assert model.worker.memory == 4096 and model.worker.extra_disk is True
        temporal = default_config("merra2_storm").model
        assert temporal.pipeline.type == "temporal"

    def test_pipeline_literal_is_per_kind(self):
        # The exported union no longer overlaps: a spatial config cannot carry
        # a temporal pipeline literal and vice versa (phase 1 review finding).
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.pipeline = {"type": "event"}
        with pytest.raises(ValueError, match=r"aggregation\.variables"):
            validate_config(cfg)  # the temporal grammar now applies
        cfg.pipeline = {"type": "spatial", "mode": "x"}
        with pytest.raises(ValueError, match=r"pipeline\.mode: Extra inputs"):
            validate_config(cfg)

    def test_worker_and_bounds_keep_their_messages(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.worker = {"memory": 512}
        with pytest.raises(
            ValueError, match=r"worker\.memory: must be one of \[2048, 4096, 8192\]"
        ):
            validate_config(cfg)
        cfg.worker = "big"
        with pytest.raises(ValueError, match="worker must be a mapping"):
            validate_config(cfg)
        cfg.worker = None
        cfg.bounds = {"temporal": {"start_date": "2019-01-01"}}
        with pytest.raises(ValueError, match="start_date and end_date"):
            validate_config(cfg)
        cfg.bounds = {"spatial": [0, 0, 1, 1], "temporal": {"start_date": "a", "end_date": "b"}}
        validate_config(cfg)
        assert validate_model(cfg).model_dump(exclude_unset=True)["bounds"] == cfg.bounds

    def test_pipeline_literals_match_the_engine_set(self):
        # Three spellings of one set (no import path joins them without a cycle).
        from typing import get_args

        from zagg.config.core import _PIPELINE_TYPES
        from zagg.config.models_run import SpatialPipeline, TemporalPipeline

        literals = {
            *get_args(SpatialPipeline.model_fields["type"].annotation),
            *get_args(TemporalPipeline.model_fields["type"].annotation),
        }
        assert literals == _PIPELINE_TYPES

    def test_root_models_are_complete_at_import(self):
        # A fresh interpreter: an earlier validate would have rebuilt them lazily.
        import subprocess
        import sys

        code = (
            "from zagg.config.models import RasterConfig, SpatialConfig, TemporalConfig\n"
            "assert all(m.__pydantic_complete__ for m in "
            "(SpatialConfig, RasterConfig, TemporalConfig))"
        )
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_catalog_must_be_a_path_string(self):
        cfg = TestPathAddressedRefusals()._cfg()
        cfg.catalog = 7
        with pytest.raises(ValueError, match=r"catalog: Input should be a valid string"):
            validate_config(cfg)

    def test_temporal_requires_a_variable(self):
        cfg = PipelineConfig(
            pipeline={"type": "temporal"},
            data_source={"reader": "xarray_s3", "collections": ["m"]},
            aggregation={"variables": {}},
            output={"store": "."},
        )
        with pytest.raises(ValueError, match="temporal pipeline requires aggregation.variables"):
            validate_config(cfg)
