"""The #653 guard skips exactly the blocks zarr would have deleted (issue #653).

Split out of ``test_template_windowed_build`` so its two builds run on their
own ``--dist loadfile`` worker (issue #669). The strata template is built
twice: through the store that refuses chunk deletes (the guard on), and with
``all_fill`` patched to ``False`` through a permissive store (zarr's own
pre-#653 delete-on-all-fill). A skipped block and a deleted one both leave no
key, so the two object sets must match exactly. The single-digest template
has no all-fill slab, so it has nothing to compare.
"""

from __future__ import annotations

import pytest
import test_template_windowed_build as windowed


@pytest.fixture(scope="module")
def twins(tmp_path_factory):
    name = windowed.TEMPLATES[0]
    windowed._NoChunkDeletes.attempted, windowed._NoChunkDeletes.deletes = [], []
    with pytest.MonkeyPatch.context() as monkeypatch:
        _, guarded, _ = windowed._build(
            monkeypatch, tmp_path_factory.mktemp("guarded-store"), name, refuse_chunk_deletes=True
        )
    with pytest.MonkeyPatch.context() as monkeypatch:
        # The pre-#653 writers: the guard off, so every all-fill slab goes to
        # zarr, which deletes its key. Every call site looks ``all_fill`` up on
        # this module at call time, so one patch reaches them all.
        import zagg.processing.write

        monkeypatch.setattr(zagg.processing.write, "all_fill", lambda array, values: False)
        _, permissive, _ = windowed._build(
            monkeypatch, tmp_path_factory.mktemp("permissive-store"), name
        )
    return guarded, permissive


def test_skipped_blocks_leave_no_object_either_way(twins):
    """A skipped all-fill block and zarr's delete of it both leave no key — and
    the guard never skipped a block zarr would have written.

    The twin runs with ``all_fill`` patched to ``False``, so it is zarr's own
    pre-#653 behaviour (delete-on-all-fill); key-set equality therefore means
    the guard skipped exactly the blocks zarr would have deleted, no more.
    """
    guarded, permissive = twins
    assert windowed._objects(guarded) == windowed._objects(permissive)
