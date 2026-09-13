"""The packaged templates that ARE the live stores' build configs (issue #547).

``atl03_tdigest_strata_healpix`` and ``gedi01b_waveform_healpix_hive`` carry,
key for key, the configs that built ``atl03_tdigest_o9`` and ``gedi_flux_o9``
(recovered from the stores' run records; espg ruling 2026-09-13: the packaged
template matches what is live, so a default build APPENDS instead of refusing
on the frozen ``semantic_hash``). The anchor is the store MANIFEST's frozen
hash — what ``sweep_overview._semantic_guard`` and ``hive._frozen_matches``
compare — vendored for ATL03 in ``tests/data/ca_atl03_tdigest_o9_morton_hive.json``.
A canonicalization change or a template edit that moves either hash surfaces
here, not at the operator's console.

The knob pins cover what the hash cannot see but the redeclare tool consumes
(``overview_delta``, the orders) and the uniform-δ ruling across every
digest-bearing template.
"""

import json
from pathlib import Path

import pytest

from zagg.config import default_config
from zagg.semantics import semantic_hash

CA_MANIFEST = Path(__file__).parent / "data" / "ca_atl03_tdigest_o9_morton_hive.json"

#: (template, store manifest ``semantic_hash``, digest fields, (parent, child, chunk_inner))
LIVE_STORES = [
    (
        "atl03_tdigest_strata_healpix",
        json.loads(CA_MANIFEST.read_text())["semantic_hash"],
        ["h_tdigest_signal", "h_tdigest_noise"],
        (9, 19, 13),
    ),
    (
        "gedi01b_waveform_healpix_hive",
        # gedi_flux_o9 morton_hive.json, read 2026-09-13 (no vendored manifest).
        "4f8287947a83abd38519372c047e7f4c62c0479d64bc72f6d512eda413d88f63",
        ["rx_flux"],
        (9, 18, 12),
    ),
]

#: Every packaged template carrying a digest field shares one centroid budget.
DIGEST_TEMPLATES = [
    "atl03_tdigest_healpix",
    "atl03_tdigest_healpix_hive",
    "atl03_tdigest_located_healpix",
    "atl03_tdigest_strata_healpix",
    "gedi01b_waveform_healpix_hive",
]


@pytest.mark.parametrize(("name", "expected", "_d", "_o"), LIVE_STORES)
def test_template_reproduces_the_live_store_semantic_hash(name, expected, _d, _o):
    assert semantic_hash(default_config(name)) == expected


@pytest.mark.parametrize(("name", "_h", "digests", "orders"), LIVE_STORES)
def test_template_pins_the_live_orders_and_fold_knobs(name, _h, digests, orders):
    cfg = default_config(name)
    grid = cfg.output["grid"]
    assert (grid["parent_order"], grid["child_order"], grid["chunk_inner"]) == orders
    for field in digests:
        meta = cfg.aggregation["variables"][field]
        assert meta["params"]["delta"] == 4096
        assert meta.get("temporal") == "per-centroid"
        # overview_delta is hash-invisible but the redeclare tool writes it.
        assert meta.get("overview_delta") in (512, None)


@pytest.mark.parametrize("name", DIGEST_TEMPLATES)
def test_every_digest_template_shares_the_uniform_delta(name):
    cfg = default_config(name)
    ragged = [m for m in cfg.aggregation["variables"].values() if m.get("kind") == "ragged"]
    assert ragged
    assert {m["params"]["delta"] for m in ragged} == {4096}
