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
int32 ones. Until each store is rebuilt as a new product (v3 stays the
comparison store, as v1 did), a default build hashes to the template pin
(``SEMANTIC_PINS``) and CANNOT append to the live store
(``LIVE_STORE_HASHES``) -- ``hive._frozen_matches`` refuses it by design.
``test_template_diverges_from_the_live_store_by_the_count_dtype_alone`` is
the proof that nothing else moved: re-declaring ``count`` as ``int32``
reproduces every live digest exactly. When the rebuilt stores land, their
manifests replace the vendored ones and the two tables collapse back into
one.

The knob pins cover what the hash cannot see but the redeclare tool consumes
(``overview_delta``, the orders) and the uniform-δ ruling across every
digest-bearing template.
"""

import json
from pathlib import Path

import pytest

from zagg.config import default_config
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
#: (issue #626); they are NOT the live stores' digests until the rebuild.
SEMANTIC_PINS = [
    (
        "atl03_tdigest_strata_healpix",
        "b29d9fce5914717d40031d90f103bf3e32ecd7a46a10b972699612678f688228",
        "95906aa9e342a159c43b3d19d6822c55683752e5605ebae31944b0844cd68f17",
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

#: The public sidecar cache (issue #499, moved 2026-09-17): a ``demo/*`` key,
#: inside the fleet execution role's source.coop grant.
SIDECAR_STORE = "s3://us-west-2.opendata.source.coop/englacial/zagg/demo/sidecar/ATL03/007"

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
def test_template_diverges_from_the_live_store_by_the_count_dtype_alone(
    name, legacy, current, build_store, manifest
):
    # Issue #626: a default build does not reproduce the live store's frozen
    # digest, so the append path refuses it (``_frozen_matches`` compares the
    # current-epoch digest when both sides carry one) -- and the ONLY key
    # behind that is count's dtype: int32 back in, every live digest returns.
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
