"""The pydantic template grammar (issue #677): the hash-stability contract and
the path-addressed refusals.

The models are a validated VIEW over the raw config dicts, never the stored
form: ``PipelineConfig`` keeps what the author wrote, so
``model_dump(exclude_unset=True)`` must reproduce every block key for key,
and a config rebuilt from that dump must canonicalize to the same semantic
JSON — on every packaged template, every ``tools/configs`` measurement arm,
and every vendored run-record config. That pair is what keeps every
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
BLOCKS = ("data_source", "aggregation", "output", "pipeline", "worker", "catalog", "bounds")


def _every_config():
    for name in PACKAGED:
        yield name, default_config(name)
    for path in MEASURE_CONFIGS:
        yield path.name, load_config_from_dict(yaml.safe_load(path.read_text()))
    for path in RUN_RECORDS:
        yield path.name, load_config_from_dict(json.loads(path.read_text()))


ALL = list(_every_config())


@pytest.mark.parametrize(("name", "cfg"), ALL, ids=[n for n, _ in ALL])
def test_dump_reproduces_the_raw_blocks(name, cfg):
    # The models inject no defaults and rewrite no values: the exclude_unset
    # dump IS the author's dict, block by block (an absent block dumps absent).
    dump = validate_model(cfg).model_dump(exclude_unset=True, mode="python")
    for block in BLOCKS:
        raw = getattr(cfg, block)
        if block in dump:
            assert dump[block] == raw, block
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
        cfg = self._cfg(read_plan={"spatial_index": "segments", "padd": 1})
        with pytest.raises(ValueError, match=r"data_source\.read_plan\.padd: Extra inputs"):
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
