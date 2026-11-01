"""The exported template grammar (issue #677 phase 5): JSON schema, CLI, docs,
registry hints, and the semantic-hash diff."""

import json
import sys
from importlib import resources
from pathlib import Path

import jsonschema
import pytest
import yaml

import zagg.configs
from zagg.config import default_config, load_config_from_dict
from zagg.config.schema import BLOCKS, HINT_KEY, json_schema, schema_json
from zagg.semantics import diff, semantic_hash

REPO = Path(__file__).parents[1]
sys.path.insert(0, str(REPO / "tools"))

PACKAGED = sorted(
    f.name.removesuffix(".yaml")
    for f in resources.files(zagg.configs).iterdir()
    if f.name.endswith(".yaml")
)
MEASURE_CONFIGS = sorted((REPO / "tools" / "configs").glob("*.yaml"))


@pytest.fixture(scope="module")
def validator():
    schema = json_schema()
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema)


@pytest.mark.parametrize("name", PACKAGED)
def test_packaged_templates_validate_under_a_stock_validator(validator, name):
    raw = yaml.safe_load(resources.files(zagg.configs).joinpath(f"{name}.yaml").read_text())
    assert [e.message for e in validator.iter_errors(raw)] == []


@pytest.mark.parametrize("path", MEASURE_CONFIGS, ids=[p.name for p in MEASURE_CONFIGS])
def test_measurement_configs_validate_under_a_stock_validator(validator, path):
    assert [e.message for e in validator.iter_errors(yaml.safe_load(path.read_text()))] == []


def test_schema_refuses_a_misspelled_key(validator):
    raw = yaml.safe_load(resources.files(zagg.configs).joinpath("atl06.yaml").read_text())
    raw["data_source"]["filtres"] = []
    errors = list(validator.iter_errors(raw))
    assert errors and any("filtres" in e.message for e in errors)


@pytest.mark.parametrize("block", sorted(BLOCKS))
def test_block_export(block):
    schema = json_schema(block, registry_hints=False)
    jsonschema.Draft202012Validator.check_schema(schema)
    assert "$defs" in schema or "properties" in schema


def test_unknown_block_is_refused():
    with pytest.raises(ValueError, match="unknown block"):
        json_schema("bogus")


def test_registry_hints_ride_the_capability_fields():
    schema = json_schema("aggregation")
    temporal = schema["$defs"]["TemporalFieldSpec"]["properties"]
    hinted = temporal["spatial_func"][HINT_KEY]
    assert {"max", "weighted_mean"} <= {h["name"] for h in hinted}
    assert "Registered spatial_func names" in temporal["spatial_func"]["description"]
    # Hints, never a closed set: plugins register more.
    assert "enum" not in temporal["spatial_func"]
    plain = json_schema("aggregation", registry_hints=False)
    assert HINT_KEY not in plain["$defs"]["TemporalFieldSpec"]["properties"]["spatial_func"]


def test_schema_json_is_stable():
    assert schema_json() == schema_json()
    assert json.loads(schema_json("output", registry_hints=False)) == json_schema(
        "output", registry_hints=False
    )


def test_cli_prints_the_schema(capsys):
    from zagg.config.__main__ import main

    assert main(["--schema", "--block", "data_source", "--no-registry-hints"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out == json_schema("data_source", registry_hints=False)
    with pytest.raises(SystemExit):
        main([])


def test_docs_page_is_current():
    import render_schema_docs

    assert render_schema_docs.render() == render_schema_docs.OUT.read_text(), (
        "docs/template_grammar.md is stale: run tools/render_schema_docs.py"
    )


class TestSemanticDiff:
    """``semantics.diff``: which changed keys move the product identity."""

    def _pair(self, name="atl03_tdigest_strata_healpix"):
        return default_config(name), default_config(name)

    def test_identical_configs_diff_empty(self):
        a, b = self._pair()
        assert diff(a, b) == {"changed_keys": [], "hash_affecting": [], "hash_neutral": []}

    def test_packaging_keys_are_neutral(self):
        a, b = self._pair()
        b.data_source["shard_workers"] = 8
        b.output["grid"]["parent_order"] = 8
        b.aggregation["streaming"] = {"mode": "merge"}
        b.worker = {"memory": 2048, "extra_disk": True}
        b.aggregation["variables"]["h_tdigest_signal"]["overview_delta"] = 64
        d = diff(a, b)
        assert d["hash_affecting"] == []
        assert set(d["hash_neutral"]) == {
            "data_source.shard_workers",
            "output.grid.parent_order",
            "aggregation.streaming.mode",
            "worker.memory",
            "aggregation.variables.h_tdigest_signal.overview_delta",
        }
        assert semantic_hash(a) == semantic_hash(b)

    def test_semantic_keys_are_affecting(self):
        a, b = self._pair()
        b.aggregation["variables"]["count"]["dtype"] = "int32"
        b.data_source["filters"][0]["value"] = -1
        d = diff(a, b)
        assert set(d["hash_affecting"]) == {
            "aggregation.variables.count.dtype",
            "data_source.filters[0].value",
        }
        assert d["hash_neutral"] == []
        assert semantic_hash(a) != semantic_hash(b)

    def test_resolved_output_knobs_follow_the_core(self):
        # ``output.windowing`` is folded RESOLVED (§8.3): a changed schedule
        # moves the hash, a changed dispatch ``unit`` under the same block does not.
        a, b = self._pair()
        b.output["windowing"]["unit"] = "window"
        d = diff(a, b)
        assert d["hash_neutral"] == ["output.windowing.unit"] and d["hash_affecting"] == []
        c = default_config("atl03_tdigest_strata_healpix")
        c.output["windowing"]["schedule"] = "monthly"
        d = diff(a, c)
        assert d["hash_affecting"] == ["output.windowing.schedule"]

    def test_absent_default_and_explicit_default_are_neutral(self):
        # The pipeline.type and sharded disciplines: spelling the default is
        # not a product change.
        a, b = self._pair("atl06")
        b.pipeline = {"type": "spatial"}
        b.output["grid"]["sharded"] = True
        d = diff(a, b)
        assert d["hash_affecting"] == []
        assert set(d["hash_neutral"]) == {"output.grid.sharded"} | (
            {"pipeline.type"} if a.pipeline != b.pipeline else set()
        )

    def test_added_and_removed_keys_are_reported(self):
        a, b = self._pair("atl06")
        del b.aggregation["variables"]["h_q75"]
        b.data_source["read_workers"] = 2
        d = diff(a, b)
        assert "aggregation.variables.h_q75.function" in d["hash_affecting"]
        assert d["hash_neutral"] == ["data_source.read_workers"]

    @pytest.mark.parametrize(
        "name, edit",
        [
            (
                "atl03_tdigest_strata_healpix",
                lambda c: c.data_source["filters"][0].update(value=-2.0),
            ),
            (
                "atl03_tdigest_strata_healpix",
                lambda c: c.data_source["filters"][0].update(column=False),
            ),
            ("atl06", lambda c: c.data_source.update(filters=[])),
            ("atl06", lambda c: c.aggregation["variables"]["count"].update(params={})),
            (
                "atl03_tdigest_strata_healpix",
                lambda c: c.aggregation["variables"]["h_tdigest_signal"].update(inner_shape=(2,)),
            ),
        ],
        ids=["int-to-float", "int-to-bool", "absent-to-empty-list", "empty-params", "tuple-shape"],
    )
    def test_diff_reads_leaves_as_the_hash_does(self, name, edit):
        # Leaves compare by canonical JSON (``-2`` vs ``-2.0``, ``0`` vs
        # ``false``), an empty container is a leaf, a tuple equals its list.
        a, b = self._pair(name)
        edit(b)
        d = diff(a, b)
        assert bool(d["hash_affecting"]) == (semantic_hash(a) != semantic_hash(b))
        assert bool(d["changed_keys"]) == (semantic_hash(a) != semantic_hash(b))

    def test_run_record_config_diffs_against_its_template(self):
        # The vendored run record vs today's template: count's dtype and the
        # windowing block are the only hash-affecting differences (issue #626/#638).
        live = load_config_from_dict(
            json.loads((REPO / "tests" / "data" / "atl03_tdigest_o9.config.json").read_text())
        )
        d = diff(live, default_config("atl03_tdigest_strata_healpix"))
        affecting = {p.split("[")[0] for p in d["hash_affecting"]}
        assert "aggregation.variables.count.dtype" in affecting
        assert all(
            p.startswith(("output.windowing", "aggregation.variables.count")) for p in affecting
        )
