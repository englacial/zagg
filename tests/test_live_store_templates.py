"""The packaged templates that ARE the live stores' build configs (issue #547).

``atl03_tdigest_strata_healpix`` and ``gedi01b_waveform_healpix_hive`` carry,
key for key, the configs that built ``atl03_tdigest_o9`` and ``gedi_flux_o9``
(recovered from the stores' run records; espg ruling 2026-09-13: the packaged
template matches what is live, so a default build APPENDS instead of refusing
on the frozen ``semantic_hash``). The anchor is the store MANIFEST's frozen
hash, vendored for ATL03 in
``tests/data/ca_atl03_tdigest_o9_morton_hive.json``. Since the issue #499
index epoch that anchor is the template's LEGACY digest: what
``sweep_overview._semantic_guard`` and ``hive._frozen_matches`` compare is the
current one, so each pin carries both columns and the append lands after
``declare_pyramid`` migrates the store. A canonicalization change or a
template edit that moves either hash surfaces here, not at the operator's
console.

The knob pins cover what the hash cannot see but the redeclare tool consumes
(``overview_delta``, the orders) and the uniform-δ ruling across every
digest-bearing template.
"""

import json
from pathlib import Path

import pytest

from zagg.config import default_config
from zagg.semantics import semantic_hash, semantic_hash_legacy

CA_MANIFEST = Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json"

#: (template, store manifest ``semantic_hash``, epoch-2 hash, build-time
#: sidecar store) — the frozen-identity anchor, read across the issue #499
#: index epoch. The stored column is what ``morton_hive.json`` carries on disk
#: today and what the template's LEGACY digest reproduces; the second is what
#: a default build hashes to now and what ``declare_pyramid`` migrates the
#: store to. Same known-answer pairs ``tests/test_semantics.py`` pins from the
#: run records — here they anchor the TEMPLATES rather than the vendored
#: configs. The legacy digest hashed the sidecar ``store`` URL, so ATL03's is
#: pinned under the location the store was BUILT from: the packaged template
#: reads the 2026-09-17 source.coop copy instead (``SIDECAR_PREFIX``).
SEMANTIC_PINS = [
    (
        "atl03_tdigest_strata_healpix",
        json.loads(CA_MANIFEST.read_text())["semantic_hash"],
        "aacfe1e387d2289276572ac941449d4042a174ccc9976528af530d2993b2258a",
        "s3://sliderule-public-cors/zagg-index/ATL03/007",
    ),
    (
        "gedi01b_waveform_healpix_hive",
        # gedi_flux_o9 morton_hive.json, read 2026-09-13 (no vendored manifest).
        "4f8287947a83abd38519372c047e7f4c62c0479d64bc72f6d512eda413d88f63",
        "337b2c3acac928c4b1b708e5895081b407d03001325ca756eb6c600c02b11e96",
        None,
    ),
]

#: (template, digest fields, (parent, child, chunk_inner), overview_delta) —
#: the hash-invisible knobs ``redeclare_dense_ladder.py`` consumes. Every
#: digest declares the 512 fold budget (issue #424).
DECLARED_PINS = [
    (
        "atl03_tdigest_strata_healpix",
        ["h_tdigest_signal", "h_tdigest_noise"],
        (9, 19, 13),
        512,
    ),
    (
        "gedi01b_waveform_healpix_hive",
        ["rx_flux"],
        (9, 18, 12),
        512,
    ),
]

#: The anonymously readable sidecar cache (issue #499, copied 2026-09-17).
SIDECAR_PREFIX = "s3://us-west-2.opendata.source.coop/englacial/zagg/sidecar/"

#: Every packaged template carrying a digest field shares one centroid budget.
DIGEST_TEMPLATES = [
    "atl03_tdigest_healpix",
    "atl03_tdigest_healpix_hive",
    "atl03_tdigest_located_healpix",
    "atl03_tdigest_strata_healpix",
    "gedi01b_waveform_healpix_hive",
]


@pytest.mark.parametrize(("name", "stored", "migrated", "build_store"), SEMANTIC_PINS)
def test_template_reproduces_the_live_store_semantic_hash(name, stored, migrated, build_store):
    # The template still IS the config that built the store: its pre-epoch
    # digest is the hash the manifest carries on disk. Issue #499 moved the
    # index block off the core, so a default build now hashes to the migration
    # target instead -- the append lands once ``declare_pyramid`` has rewritten
    # the frozen key, which is the one place that value moves. The relocated
    # sidecar cache is invisible to the current digest and visible to the
    # legacy one, which is the epoch's whole point.
    cfg = default_config(name)
    assert semantic_hash(cfg) == migrated
    if build_store is not None:
        assert semantic_hash_legacy(cfg) != stored
        cfg.data_source["index"]["store"] = build_store
    assert semantic_hash_legacy(cfg) == stored


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


def test_atl03_template_reads_its_sidecars_from_source_coop():
    # Read machinery outside the semantic core (issue #499): the hash pins
    # above hold across the relocation, and this is the public copy.
    cfg = default_config("atl03_tdigest_strata_healpix")
    assert cfg.data_source["index"]["store"].startswith(SIDECAR_PREFIX)


@pytest.mark.parametrize("name", DIGEST_TEMPLATES)
def test_every_digest_template_shares_the_uniform_delta(name):
    cfg = default_config(name)
    ragged = [m for m in cfg.aggregation["variables"].values() if m.get("kind") == "ragged"]
    assert ragged
    assert {m["params"]["delta"] for m in ragged} == {4096}
