"""The packaged templates that carry the live stores' build configs (issue #547).

``atl03_tdigest_strata_healpix`` and ``gedi01b_waveform_healpix_hive`` carry,
key for key, the configs that built ``atl03_tdigest_o9_v3`` and
``gedi_flux_o9`` (the GEDI one recovered from the store's run record, espg
ruling 2026-09-13; the ATL03 store was rebuilt from the packaged template on
the 0.57.0 fleet, issue #560) -- with ONE deliberate exception since issue
#626: ``count`` is ``int64`` in the templates and ``int32`` in the stores, so
a default build no longer appends to either store (below). The anchor is the
store MANIFEST's frozen hash, vendored for ATL03 in
``tests/data/ca_atl03_tdigest_o9_v3_morton_hive.json``. The v3 store was born
after the issue #499 index epoch, so its frozen hash is the int32 config's
CURRENT digest -- the one ``sweep_overview._semantic_guard`` and
``hive._frozen_matches`` compare. The pre-epoch stores -- ``gedi_flux_o9``,
and the v1 ``atl03_tdigest_o9`` retained as the comparison store (its
manifest stays vendored in ``tests/data/ca_atl03_tdigest_o9_morton_hive.json``)
-- are frozen at the LEGACY digests, so each pin carries both columns and an
append there lands only after ``declare_pyramid`` migrates the store. A
canonicalization change or a template edit that moves either hash surfaces
here, not at the operator's console.

**The issue #626 epoch.** ``count`` is ``int64`` in every packaged template
(espg ruling 2026-10-09: the int32 ladder wrapped past 2^31 at orders 1 and
0 of the v3 store), and the dtype sits inside the hashed ``aggregation``
block, so both templates' digests moved while the live stores keep the
int32 ones. **The issue #638 epoch** adds one more deliberate divergence for
ATL03: the hive templates are windowed yearly from birth, and
``output.windowing`` is a D13 leaf-shaping key (a windowed store is a
different product), so the strata template's digest moved again while GEDI's
-- unwindowed, see ``test_gedi_stays_unwindowed_until_shot_rate_membership``
-- did not. Until each store is rebuilt as a new product (v3 stays the
comparison store, as v1 did), a default build hashes to the template pin
(``SEMANTIC_PINS``) and CANNOT append to the live store
(``LIVE_STORE_HASHES``) -- ``hive._frozen_matches`` refuses it by design.
``test_template_diverges_from_the_live_store_by_count_and_windowing_alone``
is the proof that nothing else moved: re-declaring ``count`` as ``int32``
and dropping the windowing block reproduces every live digest exactly. When
the rebuilt stores land, their manifests replace the vendored ones and the
two tables collapse back into one.

The knob pins cover what the hash cannot see but the redeclare tool consumes
(``overview_delta``, the orders) and the uniform-δ ruling across every
digest-bearing template.
"""

import json
from importlib import resources
from pathlib import Path

import pytest

import zagg.configs
from zagg.config import default_config, load_config
from zagg.semantics import semantic_hash, semantic_hash_legacy

CA_MANIFEST = Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_v3_morton_hive.json"
CA_V1_MANIFEST = Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json"

#: (template, pre-epoch ``semantic_hash``, epoch-2 ``semantic_hash``,
#: build-time sidecar store) — what a default build of the TEMPLATE hashes
#: to, read across the issue #499 index epoch (the epoch-2 column is the one
#: ``sweep_overview._semantic_guard`` and ``hive._frozen_matches`` compare;
#: ``declare_pyramid`` migrates a pre-epoch store from the first column to the
#: second). The legacy digest hashed the sidecar ``store`` URL, so ATL03's is
#: pinned under the location the v1 store was BUILT from: the packaged
#: template reads the source.coop copy instead (``SIDECAR_STORE``, moved
#: 2026-09-17). Values computed 2026-10-09 from the int64 ``count`` templates
#: (issue #626), and for ATL03 recomputed 2026-10-12 with the yearly
#: ``output.windowing`` block (issue #638; a hashed D13 key); they are NOT the
#: live stores' digests until the rebuild.
SEMANTIC_PINS = [
    (
        "atl03_tdigest_strata_healpix",
        "d3c16792fa6e423cec52b6bc4dfe2583379e0e50360babb3acc8d5d935cd56f0",
        "252c1011d347ffa436afefe3d7a5358bcc0438d988c78fa989183fa526778417",
        "s3://sliderule-public-cors/zagg-index/ATL03/007",
    ),
    (
        "gedi01b_waveform_healpix_hive",
        "a914ec2c2f143b8ebd78e90427f49115ecc94805c8061b1467ec9794daa49411",
        "cbee565ae2d30876fbfe341ce9d2d7758c0562a13cd4136c53c101deba4242c7",
        None,
    ),
]

#: (template, pre-epoch ``semantic_hash``, epoch-2 ``semantic_hash``,
#: build-time sidecar store, vendored current-epoch manifest) — the LIVE
#: STORES' frozen digests, the int32-``count`` identity the templates carried
#: before issue #626: the v1 ATL03 comparison store's and the v3 store's
#: manifests on disk, and ``gedi_flux_o9``'s ``morton_hive.json`` (read
#: 2026-09-13, no vendored manifest; the same known-answer pair
#: ``tests/test_semantics.py`` pins from the run record).
LIVE_STORE_HASHES = [
    (
        "atl03_tdigest_strata_healpix",
        json.loads(CA_V1_MANIFEST.read_text())["semantic_hash"],
        json.loads(CA_MANIFEST.read_text())["semantic_hash"],
        "s3://sliderule-public-cors/zagg-index/ATL03/007",
        CA_MANIFEST,
    ),
    (
        "gedi01b_waveform_healpix_hive",
        "4f8287947a83abd38519372c047e7f4c62c0479d64bc72f6d512eda413d88f63",
        "337b2c3acac928c4b1b708e5895081b407d03001325ca756eb6c600c02b11e96",
        None,
        None,
    ),
]

#: (template, digest fields, (parent, child, chunk_inner), overview_delta) —
#: the hash-invisible knobs ``redeclare_dense_ladder.py`` consumes. ATL03's
#: digests declare the v4 fold budget of 128 centroids (issue #638: the stored
#: 15/16 columns at ~12 GB rather than ~95 GB at 512); GEDI keeps 512 (issue
#: #424) pending its own measurement.
DECLARED_PINS = [
    (
        "atl03_tdigest_strata_healpix",
        ["h_tdigest_signal", "h_tdigest_noise"],
        (9, 19, 13),
        128,
    ),
    (
        "gedi01b_waveform_healpix_hive",
        ["rx_flux"],
        (9, 18, 12),
        512,
    ),
]

#: The public sidecar cache (issue #499, moved 2026-09-17): a ``demo/*`` key,
#: inside the fleet execution role's source.coop grant.
SIDECAR_STORE = "s3://us-west-2.opendata.source.coop/englacial/zagg/demo/sidecar/ATL03/007"

#: Every packaged template, plus the out-of-package measurement arm configs
#: that mirror the strata template (issue #586; the four #602 arms).
PACKAGED_TEMPLATES = sorted(
    f.name.removesuffix(".yaml")
    for f in resources.files(zagg.configs).iterdir()
    if f.name.endswith(".yaml")
)
MEASURE_CONFIGS = sorted((Path(__file__).parents[1] / "tools" / "configs").glob("*.yaml"))

#: Every packaged template carrying a digest field shares one centroid budget.
DIGEST_TEMPLATES = [
    "atl03_tdigest_healpix",
    "atl03_tdigest_healpix_hive",
    "atl03_tdigest_located_healpix",
    "atl03_tdigest_strata_healpix",
    "gedi01b_waveform_healpix_hive",
]


def _legacy_hash(cfg, build_store):
    # The legacy digest saw the sidecar store URL; hash under the build-time one.
    if build_store is not None:
        cfg.data_source["index"]["store"] = build_store
    return semantic_hash_legacy(cfg)


@pytest.mark.parametrize(("name", "legacy", "current", "build_store"), SEMANTIC_PINS)
def test_template_hashes_are_pinned(name, legacy, current, build_store):
    # Both columns of a default build's digest, so a canonicalization drift on
    # either side of the issue #499 epoch fails here by name. The relocated
    # sidecar cache is invisible to the current digest and visible to the
    # legacy one, which is the epoch's whole point.
    cfg = default_config(name)
    assert semantic_hash(cfg) == current
    if build_store is not None:
        assert semantic_hash_legacy(cfg) != legacy
    assert _legacy_hash(cfg, build_store) == legacy


@pytest.mark.parametrize(
    ("name", "legacy", "current", "build_store", "manifest"), LIVE_STORE_HASHES
)
def test_template_diverges_from_the_live_store_by_count_and_windowing_alone(
    name, legacy, current, build_store, manifest
):
    # Issue #626: a default build does not reproduce the live store's frozen
    # digest, so the append path refuses it (``_frozen_matches`` compares the
    # current-epoch digest when both sides carry one) -- and the ONLY keys
    # behind that are count's dtype and, for ATL03, the yearly windowing
    # block (issue #638): int32 back in and the block out, every live digest
    # returns. GEDI carries no windowing block, so count is its one divergence.
    from zagg.hive import _frozen_matches

    # The refusal is checked against the vendored live manifest, every other
    # frozen key held at its live value so the hash is the only difference.
    # GEDI has no vendored manifest (and a pre-epoch hash, which
    # ``_frozen_matches`` never matches), so its row pins the digests alone.
    live = json.loads(manifest.read_text()) if manifest is not None else None
    cfg = default_config(name)
    assert semantic_hash(cfg) != current
    if live is not None:
        assert not _frozen_matches(live, {**live, "semantic_hash": semantic_hash(cfg)})
    cfg.aggregation["variables"]["count"]["dtype"] = "int32"
    cfg.output.pop("windowing", None)
    assert semantic_hash(cfg) == current
    assert _legacy_hash(cfg, build_store) == legacy
    if live is not None:
        assert _frozen_matches(live, {**live, "semantic_hash": semantic_hash(cfg)})


@pytest.mark.parametrize(("name", "digests", "orders", "overview_delta"), DECLARED_PINS)
def test_template_pins_the_live_orders_and_fold_knobs(name, digests, orders, overview_delta):
    cfg = default_config(name)
    grid = cfg.output["grid"]
    assert (grid["parent_order"], grid["child_order"], grid["chunk_inner"]) == orders
    for field in digests:
        meta = cfg.aggregation["variables"][field]
        assert meta["params"]["delta"] == 4096
        assert meta.get("temporal") == "per-centroid"
        # overview_delta is hash-invisible but the redeclare tool writes it.
        assert meta.get("overview_delta") == overview_delta


#: The hive templates that declare the leaf ladder (issue #620 phase 4, espg's
#: ruling on PR #622 question (2)): the v4 ATL03 tier ``[16, 15, 14, 13]`` —
#: contiguous from cells 16 down to shard order 9 + 4 — the plain hive
#: template following it exactly, and GEDI carrying the same relation to its
#: own shard order (its cells are order 18, so its finest member is two orders
#: above the base where ATL03's is three).
LADDER_TEMPLATES = [
    "atl03_tdigest_strata_healpix",
    "atl03_tdigest_healpix_hive",
    "gedi01b_waveform_healpix_hive",
]
LADDER = [16, 15, 14, 13]


@pytest.mark.parametrize("name", LADDER_TEMPLATES)
def test_hive_templates_declare_the_ladder_and_chain_the_staged_sweep(name):
    # The ladder is the zagg-pyramid/2 grammar as shipped — validated at load
    # (default_config runs validate_config), expanded to the fixed every-order
    # ladder above the shard, and a leaf column plan the worker writes — and
    # because a hive store that declares a ladder chains the staged sweep by
    # default (issue #620, ``zagg.config.get_sweep_mode``), a run from any of
    # these templates folds its ladder in the run tail -- and the templates
    # spell ``sweep: stages`` anyway, because the Icechunk commit regime keys
    # on the explicit key (PR #622 ruling (1)): spelled, a templated run
    # commits refs up the ladder rather than per leaf (espg, PR #639).
    from zagg.column import leaf_column_plan
    from zagg.config import SWEEP_STAGES, get_pyramid, get_sweep_mode
    from zagg.grids import from_config
    from zagg.pyramid import expand_overviews
    from zagg.sweep_overview import build_pyramid_block

    cfg = default_config(name)
    assert cfg.output["sweep"] == "stages"  # spelled: the ladder commit regime
    assert get_pyramid(cfg)["overviews"] == LADDER  # all_time resolves true when windowed (#623)
    grid = from_config(cfg)
    shard = int(cfg.output["grid"]["parent_order"])
    assert LADDER[-1] == shard + 4
    assert get_sweep_mode(cfg, grid) == SWEEP_STAGES
    block = build_pyramid_block(cfg, shard, chunk_order=grid.chunk_order)
    assert block["spec"] == "zagg-pyramid/2"
    assert block["overviews"] == expand_overviews(LADDER, parent_order=shard)
    resolutions, fields = leaf_column_plan(cfg, grid)
    assert resolutions == list(range(16, shard - 1, -1))  # the contiguous column tier
    assert fields  # at least one composable field rides the ladder


#: The hive templates that are windowed yearly from birth (issue #638, espg
#: ruling 2026-10-12). GEDI is not among them -- see
#: ``test_gedi_stays_unwindowed_until_shot_rate_membership``.
WINDOWED_TEMPLATES = ["atl03_tdigest_strata_healpix", "atl03_tdigest_healpix_hive"]
WINDOWING = {
    "schedule": "yearly",
    "time_field": "delta_time",
    "epoch": "2018-01-01T00:00:00+00:00",
    "scale": "gps",
    "units": "seconds",
    "windows": None,
}


@pytest.mark.parametrize("name", WINDOWED_TEMPLATES)
def test_atl03_hive_templates_are_windowed_yearly(name):
    # Validated at load (default_config runs validate_config) and resolved
    # through ``get_windowing``, so the epoch spelling canonicalizes. Neither
    # ``all_time`` nor ``icechunk`` is spelled: a windowed config resolves the
    # ``all`` row (issue #623) and a windowed hive store gets its Icechunk repo
    # at birth (issue #604), one manifest per row. The clock is the one
    # ``time_source`` declares where the template has one, and the per-photon
    # ``delta_time`` column is declared under ``data_source.variables`` -- the
    # base-rate column window membership is decided on.
    from zagg.config import get_icechunk, get_pyramid, get_windowing, get_windowing_unit
    from zagg.icechunk_rows import DEFAULT_ROWS_PER_MANIFEST

    cfg = default_config(name)
    assert get_windowing(cfg) == WINDOWING
    assert get_windowing_unit(cfg) == "shard"
    assert "all_time" not in cfg.output["pyramid"] and "icechunk" not in cfg.output
    assert get_pyramid(cfg)["all_time"] is True  # resolved, not spelled (issue #623)
    assert cfg.data_source["variables"]["delta_time"] == "/{group}/heights/delta_time"
    if "time_source" in cfg.output:
        source = cfg.output["time_source"]
        assert (source["field"], source["scale"], source["units"]) == (
            "delta_time",
            "gps",
            "seconds",
        )
    assert get_icechunk(cfg) is True
    from zagg.icechunk_refs import resolve_options

    shard = int(cfg.output["grid"]["parent_order"])
    assert resolve_options(cfg, shard)["rows_per_manifest"] == DEFAULT_ROWS_PER_MANIFEST
    # Deleting the block is the opt-out: an unwindowed build stays legal.
    del cfg.output["windowing"]
    from zagg.config import validate_config

    validate_config(cfg)
    assert get_windowing(cfg) is None


def test_gedi_stays_unwindowed_until_shot_rate_membership():
    # GEDI's clock is shot-rate -- ``delta_time`` is declared on the
    # ``shots`` level and broadcast to sample rate -- and window membership is
    # decided on a BASE-rate ``data_source.variables`` column only
    # (``_validate_windowing``), so the #638 block is refused by name on this
    # template. It stays unwindowed, and this test says why: when level-rate
    # membership lands, this assertion is the one to flip.
    from zagg.config import get_windowing, validate_config

    cfg = default_config("gedi01b_waveform_healpix_hive")
    assert get_windowing(cfg) is None
    assert "delta_time" in cfg.data_source["levels"]["shots"]["variables"]
    cfg.output["windowing"] = {k: v for k, v in WINDOWING.items() if k != "windows"}
    with pytest.raises(ValueError, match="non-base .segment-rate. level"):
        validate_config(cfg)


@pytest.mark.parametrize(
    "name,budget",
    (
        ("atl03_tdigest_strata_healpix", 128),
        ("atl03_tdigest_healpix_hive", 128),
        ("gedi01b_waveform_healpix_hive", 512),
    ),
)
def test_overview_delta_is_the_v4_budget_on_atl03_and_unchanged_on_gedi(name, budget):
    # Every ragged (digest) field of the ATL03 hive templates declares the v4
    # overview budget of 128 centroids (issue #638); GEDI's waveform digests
    # keep 512 pending a measurement of their own. The leaf ``delta`` is 4096
    # throughout (``test_every_digest_template_shares_the_uniform_delta``),
    # and ``overview_delta`` is hash-invisible
    # (``zagg.semantics.VARIABLE_PACKAGING_KEYS``), so no pin above moved for it.
    from zagg.semantics import VARIABLE_PACKAGING_KEYS

    assert "overview_delta" in VARIABLE_PACKAGING_KEYS
    cfg = default_config(name)
    ragged = {k: m for k, m in cfg.aggregation["variables"].items() if m.get("kind") == "ragged"}
    assert ragged
    assert {m["overview_delta"] for m in ragged.values()} == {budget}
    cfg.aggregation["variables"][next(iter(ragged))]["overview_delta"] = (
        512 if budget == 128 else 128
    )
    assert semantic_hash(cfg) == semantic_hash(default_config(name))


@pytest.mark.parametrize("name", LADDER_TEMPLATES)
def test_an_older_store_opts_out_by_its_own_declaration_not_by_sweep(name):
    # The leaf worker writes its column, and the Icechunk leaf seam indexes
    # its levels, from the CONFIG's ladder (``leaf_column_plan`` /
    # ``leaf_level_cells``), never the store's manifest -- so ``sweep: true``
    # stops the staged fan-out but still writes undeclared [16..9] columns and
    # stages refs for levels a manifest-built repo lacks (review round 3 on PR
    # #622). The opt-out is ``output.pyramid`` set back to the store's own
    # declaration: ``false`` writes no column and indexes no leaf level, and
    # the v3 store's ``{overviews: 13}`` indexes exactly its 13.
    from zagg.column import leaf_column_plan, leaf_level_cells
    from zagg.config import SWEEP_FAMILIES, get_sweep_mode
    from zagg.grids import from_config

    cfg = default_config(name)
    grid = from_config(cfg)
    cfg.output["sweep"] = True
    assert leaf_level_cells(cfg, grid) == LADDER  # sweep alone is no opt-out
    del cfg.output["sweep"]
    cfg.output["pyramid"] = False
    assert leaf_column_plan(cfg, grid) is None and leaf_level_cells(cfg, grid) == []
    assert get_sweep_mode(cfg, grid) == SWEEP_FAMILIES
    cfg.output["pyramid"] = {"overviews": 13}
    assert leaf_level_cells(cfg, grid) == [13]


def test_the_ladder_is_hash_invisible():
    # ``output.pyramid`` is outside the D19 semantic core, so declaring the
    # ladder moved no template digest: SEMANTIC_PINS above still hold (their
    # own test), and the declared-off config hashes identically.
    for name in LADDER_TEMPLATES:
        cfg = default_config(name)
        off = default_config(name)
        off.output["pyramid"] = False
        assert semantic_hash(cfg) == semantic_hash(off)


def test_atl03_template_reads_its_sidecars_from_source_coop():
    # Read machinery outside the semantic core (issue #499): the hash pins
    # above hold across the relocation. Pinned exactly: the value must stay
    # under demo/*, the prefix the fleet can read and write.
    cfg = default_config("atl03_tdigest_strata_healpix")
    assert cfg.data_source["index"]["store"] == SIDECAR_STORE


@pytest.mark.parametrize("name", DIGEST_TEMPLATES)
def test_every_digest_template_shares_the_uniform_delta(name):
    cfg = default_config(name)
    ragged = [m for m in cfg.aggregation["variables"].values() if m.get("kind") == "ragged"]
    assert ragged
    assert {m["params"]["delta"] for m in ragged} == {4096}


def _count_fields(cfg):
    # The ``len`` fields; a raster template declares no ``variables`` at all.
    return [m for m in cfg.aggregation.get("variables", {}).values() if m.get("function") == "len"]


@pytest.mark.parametrize("name", [*PACKAGED_TEMPLATES, *MEASURE_CONFIGS])
def test_every_template_declares_count_int64(name):
    # Issue #626 ruling: ``count`` (the ``len`` field) is int64 in every
    # template, so no ladder of any packaged product wraps past 2^31. A
    # template without a ``len`` field passes vacuously; the next test pins
    # that the ruling is not vacuous overall.
    cfg = load_config(str(name)) if isinstance(name, Path) else default_config(name)
    assert {m["dtype"] for m in _count_fields(cfg)} <= {"int64"}


def test_count_bearing_templates_are_the_expected_set():
    has_count = {name for name in PACKAGED_TEMPLATES if _count_fields(default_config(name))}
    assert has_count >= {*DIGEST_TEMPLATES, "atl03", "atl06", "atl06_nullable", "atl06_polar"}
