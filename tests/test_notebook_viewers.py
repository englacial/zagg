"""The reader notebooks' viewers read a tensor in the dtype its field allows (issue #624).

moczarr >= 0.9 refuses an integer ``read_tensors`` dtype over a spec §2.0
``weights: "flux"`` field, which is what the Binder ``hhdc_viewer`` hit on the
GEDI leaf once its moczarr floor moved to 0.10. ``notebooks/viewers.py`` is no
package, so it is loaded from its file; moczarr and the notebook runtime are not
zagg test dependencies, so this skips where they are absent.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

mz = pytest.importorskip("moczarr")
pytest.importorskip("matplotlib")
pytest.importorskip("ipywidgets")
from zarr.storage import LocalStore  # noqa: E402

REPO_ROOT = Path(__file__).parent.parent
SPEC = REPO_ROOT / "tests" / "data" / "spec"


def _viewers():
    spec = importlib.util.spec_from_file_location("viewers", REPO_ROOT / "notebooks" / "viewers.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _leaf(name):
    """The fixture's leaf store and the field path of its digest, from its expected.json."""
    expected = json.loads((SPEC / f"{name}.expected.json").read_text())
    return LocalStore(SPEC / name / expected["leaf"]), expected["group"]


@pytest.mark.parametrize(
    ("name", "field", "dtype"),
    [("flux", "rx_flux", "float32"), ("minimal", "h_tdigest", "uint32")],
)
def test_tensor_dtype_follows_the_weights_declaration(name, field, dtype):
    store, group = _leaf(name)
    assert _viewers().tensor_dtype(store, f"{group}/{field}") == dtype


def test_flux_leaf_reads_in_the_chosen_dtype():
    """The default the notebook relied on is refused; the chosen dtype reads through."""
    store, group = _leaf("flux")
    field = f"{group}/rx_flux"
    kwargs = {"n_bins": 256, "resolution": 1.0, "fit": "degrade_resolution"}
    with pytest.raises(ValueError, match=r"weights 'flux'"):
        next(iter(mz.read_tensors(store, field, **kwargs)))
    dtype = _viewers().tensor_dtype(store, field)
    blocks = list(mz.read_tensors(store, field, dtype=dtype, **kwargs))
    assert blocks
    for tensor, _mask, _window, _word in blocks:
        assert np.issubdtype(tensor.dtype, np.floating)
        assert float(tensor.sum(dtype="float64")) > 0
