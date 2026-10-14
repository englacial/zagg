"""A refused leaf write ends the unit at that window (issue #651).

Split out of ``test_template_windowed_build`` so its two full builds are a
``--dist loadfile`` work unit of their own (issue #669). That is not the
unqualified win it looks like: pytest-xdist orders file scopes by DESCENDING
item count by default (``--loadscope-reorder``, on since 3.8.0), so this
two-item module is dispatched last of the suite's 120 scopes and starts when
the queue is nearly empty, which puts its whole duration on the critical
path. Inside the build module these same two cases sat at queue position ~89.
PR #670 has the measured walls; this layout is not yet a measured improvement.

Each case builds the strata template through a leaf store that raises on the
first chunk PUT of the second window: a store-fatal shape ends the unit there,
a retried 5xx keeps today's per-window isolation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from obstore.exceptions import GenericError
from test_template_windowed_build import SITES, TEMPLATES, WINDOWS, _build, _shard
from zarr.storage import WrapperStore

from zagg import hive
from zagg.config import default_config


class _RefusingStore(WrapperStore):
    """The leaf store, raising ``error`` on its first chunk PUT."""

    def __init__(self, store, error, state):
        super().__init__(store)
        self._error, self._state = error, state

    async def set(self, key, value):
        if "/c/" in key and not self._state.get("raised"):
            self._state["raised"] = True
            raise self._error
        await super().set(key, value)


#: The two shapes the classifier tells apart: object_store could not parse the
#: body (the post-mortem's class; no status to key on), and a 5xx the client
#: already retried for the full policy.
_FATAL = "Generic S3 error: Got invalid DeleteObjects response: EOF while parsing"
_RETRIED = (
    "Error after 12 retries in 180s, max_retries:12, retry_timeout:180s, "
    "source:HTTP status server error (503 Service Unavailable) for url"
)


@pytest.mark.parametrize(
    ("message", "store_error_class", "emitted"),
    [(_FATAL, "store-fatal", ["2018", "2019"]), (_RETRIED, "transient", list(WINDOWS))],
    ids=["store-fatal", "transient"],
)
def test_a_refused_leaf_write_ends_the_unit_at_that_window(
    monkeypatch, tmp_path, message, store_error_class, emitted
):
    """A fatal-shaped error on the first chunk PUT of the second window ends the
    unit there: no later window is aggregated or opened, the earlier window
    keeps its stamp, the failed one is unstamped debris, and the result names
    the class. The retried shape keeps the per-window isolation (issue #586):
    the window fails alone and the rest of the shard lands.
    """
    import zagg.store as store_mod

    name = TEMPLATES[0]
    shard0 = _shard(SITES[0], int(default_config(name).output["grid"]["parent_order"]))
    root = str(tmp_path / name)
    leaf = {w: hive.shard_leaf_path(root, shard0, window=w) for w in WINDOWS}
    real_open, opens, state = store_mod.open_store, [], {}

    def _open(path, *args, **kwargs):
        opens.append(str(path))
        store = real_open(path, *args, **kwargs)
        if str(path).startswith(leaf["2019"]) and not kwargs.get("read_only"):
            return _RefusingStore(store, GenericError(message), state)
        return store

    metas, real_seam = {}, hive.process_and_write_hive

    def _seam(shard_key, *args, **kwargs):
        metas[int(shard_key)] = meta = real_seam(shard_key, *args, **kwargs)
        return meta

    # ``sinks()`` is the argument to ``aggregate``: one call per aggregated window.
    aggregated, real_sinks = [], hive._LeafUnit.sinks

    def _sinks(self):
        aggregated.append((int(self.shard_key), self.label))
        return real_sinks(self)

    monkeypatch.setattr(store_mod, "open_store", _open)
    monkeypatch.setattr(hive, "process_and_write_hive", _seam)
    monkeypatch.setattr(hive._LeafUnit, "sinks", _sinks)
    _cfg, built, shards = _build(monkeypatch, tmp_path, name)
    assert str(built) == root and shards[0] == shard0 and state["raised"]

    meta, other = metas[shard0], metas[shards[1]]
    assert [m["window"] for m in meta["windows"]] == emitted
    assert [w for s, w in aggregated if s == shard0] == emitted
    assert meta["store_error_class"] == meta["windows"][1]["store_error_class"] == store_error_class
    assert message in meta["windows"][1]["error"]
    assert other["error"] is None and [m["window"] for m in other["windows"]] == list(WINDOWS)
    # The window before the failure keeps its stamp; the failed one is debris.
    assert hive.resolve_leaf(leaf["2018"])[1] is not None
    assert hive.resolve_leaf(leaf["2019"])[1] is None
    later = [w for w in WINDOWS if w > "2019"]  # 2020 and the #653 stub window 2021
    if store_error_class == "store-fatal":
        # The bound: the unit returned after the failing window -- the windows
        # after it were never aggregated, never opened, never written.
        assert meta["windows_unattempted"] == later
        for w in later:
            assert not any(o.startswith(leaf[w]) for o in opens)
            assert not Path(leaf[w]).exists()
    else:
        assert "windows_unattempted" not in meta
        for w in later:
            assert hive.resolve_leaf(leaf[w])[1] is not None
