"""arro3 interop gates for mortie's typed ``morton_index`` Arrow surface (issue #135).

mortie 0.8.4 (espg/mortie#94) exports the ``morton_index`` extension type over
the Arrow PyCapsule C Data Interface — no pyarrow required. These tests gate the
design before the carrier adopts it: the extension metadata must survive exactly
the arro3-core ops the write path uses (``Array.from_arrow``,
``Table.from_pydict`` → ``column(name)`` → ``combine_chunks()``), the words must
round-trip byte-equal with the null↔sentinel mapping intact, and none of it may
import pyarrow.
"""

import subprocess
import sys
import textwrap

import numpy as np
import pytest

EXT_KEY = "ARROW:extension:name"
EXT_NAME = "mortie.morton_index"


@pytest.fixture
def words():
    """Packed uint64 words incl. the all-zero empty sentinel (an Arrow null) and
    a southern-hemisphere word with bit 63 set (the #71 sign hazard)."""
    leaf = np.array([123456789, 0, 42], dtype=np.uint64)
    leaf[2] = np.uint64(1) << np.uint64(63) | np.uint64(42)
    return leaf


def _typed_array(words):
    from arro3.core import Array
    from mortie import MortonIndexArray

    return Array.from_arrow(MortonIndexArray.from_words(words))


class TestArro3InteropGates:
    """The four phase-1 gates from the issue #135 plan."""

    def test_array_from_arrow_carries_extension_name(self, words):
        # Gate (a): MortonIndexArray -> arro3 Array keeps the extension type.
        arr = _typed_array(words)
        assert dict(arr.field.metadata_str).get(EXT_KEY) == EXT_NAME

    def test_metadata_survives_table_plumbing(self, words):
        # Gate (b): the exact ops _build_output/_iter_carrier_columns use —
        # Table.from_pydict -> column(name) -> combine_chunks().
        from arro3.core import Array, Table

        tbl = Table.from_pydict(
            {"morton": _typed_array(words), "x": Array.from_numpy(np.zeros(len(words)))}
        )
        col = tbl.column("morton").combine_chunks()
        assert dict(col.field.metadata_str).get(EXT_KEY) == EXT_NAME
        # The sibling plain column carries no extension metadata.
        plain = tbl.column("x").combine_chunks()
        assert EXT_KEY not in dict(plain.field.metadata_str)

    def test_import_c_array_roundtrips_words(self, words):
        # Gate (c): words come back byte-equal; the all-zero sentinel survives
        # its trip through an Arrow null (the mortie null<->sentinel mapping).
        import mortie.arrow as ma
        from arro3.core import Array, Table

        tbl = Table.from_pydict(
            {"morton": _typed_array(words), "x": Array.from_numpy(np.zeros(len(words)))}
        )
        col = tbl.column("morton").combine_chunks()
        back = ma.import_c_array(col)
        assert back.dtype == np.uint64
        np.testing.assert_array_equal(back, words)
        # bit-63 word stored intact (non-negative as uint64; #71).
        assert (back.view(np.int64) < 0).any()

    def test_path_needs_no_pyarrow(self):
        # Gate (d): the whole typed path runs with pyarrow unimportable. Must be
        # a subprocess — in this env pyarrow is installed and mortie registers
        # its pyarrow extension type eagerly at import, so blocking after the
        # fact would prove nothing.
        script = textwrap.dedent(
            """
            import sys

            class _BlockPyarrow:
                def find_spec(self, name, path=None, target=None):
                    if name == "pyarrow" or name.startswith("pyarrow."):
                        raise ModuleNotFoundError("pyarrow blocked for this test")
                    return None

            sys.meta_path.insert(0, _BlockPyarrow())

            import numpy as np
            import mortie.arrow as ma
            from arro3.core import Array, Table
            from mortie import MortonIndexArray

            words = np.array([123456789, 0, (1 << 63) | 42], dtype=np.uint64)
            arr = Array.from_arrow(MortonIndexArray.from_words(words))
            tbl = Table.from_pydict({"morton": arr})
            col = tbl.column("morton").combine_chunks()
            assert dict(col.field.metadata_str).get("ARROW:extension:name") == (
                "mortie.morton_index"
            )
            back = ma.import_c_array(col)
            assert np.array_equal(back, words)
            assert "pyarrow" not in sys.modules, "pyarrow leaked onto the typed path"
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
        )
        assert result.returncode == 0, result.stderr
