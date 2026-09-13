"""The committed build configs of the live stores reproduce their frozen semantic hashes.

``data/store_configs/*.build_config.yaml`` are the ``--config`` inputs to
``tools/redeclare_dense_ladder.py`` (issue #547 step 1). The tool's semantic
guard refuses a config whose D19 ``semantic_hash`` differs from the store
manifest's, so these pins are what keep the files usable as the semantic core
evolves: a canonicalization change that moves either hash surfaces here, not
at the operator's console.
"""

from pathlib import Path

import pytest

from zagg.config import load_config
from zagg.semantics import semantic_hash

STORE_CONFIGS = Path(__file__).resolve().parents[1] / "data" / "store_configs"

#: (file, manifest ``semantic_hash``, digest fields, ``overview_delta`` on those
#: fields or ``None``, ``(child_order, chunk_inner)``) — read off the live
#: manifests / run records on 2026-09-13. Everything past the hash is invisible
#: to ``semantic_hash`` (packaging keys, issue #424) yet lands verbatim in the
#: block ``redeclare_dense_ladder.py`` writes, or gates its derivation.
LIVE_STORES = [
    (
        "atl03_tdigest_o9.build_config.yaml",
        "b9b15fdde78f147c15c929da8ca93de21930ad5c552ae082c5d8998fb83ada21",
        ["h_tdigest_signal", "h_tdigest_noise"],
        512,
        (19, 13),
    ),
    (
        "gedi_flux_o9.build_config.yaml",
        "4f8287947a83abd38519372c047e7f4c62c0479d64bc72f6d512eda413d88f63",
        ["rx_flux"],
        None,
        (18, 12),
    ),
]


@pytest.mark.parametrize(("name", "expected", "digests", "overview_delta", "orders"), LIVE_STORES)
def test_build_config_reproduces_the_store_semantic_hash(
    name, expected, digests, overview_delta, orders
):
    config = load_config(str(STORE_CONFIGS / name))
    assert semantic_hash(config) == expected


@pytest.mark.parametrize(("name", "expected", "digests", "overview_delta", "orders"), LIVE_STORES)
def test_build_config_pins_the_live_delta_and_parent_order(
    name, expected, digests, overview_delta, orders
):
    import yaml

    raw = yaml.safe_load((STORE_CONFIGS / name).read_text())
    for field in digests:
        variable = raw["aggregation"]["variables"][field]
        assert variable["params"]["delta"] == 4096
        assert variable.get("overview_delta") == overview_delta
    grid = raw["output"]["grid"]
    assert grid["parent_order"] == 9
    assert (grid["child_order"], grid["chunk_inner"]) == orders
