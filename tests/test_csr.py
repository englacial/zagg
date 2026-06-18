"""Tests for the CSR (ragged) writer/reader — issue #48, phase 3."""

import numpy as np
import pytest
from zarr.storage import MemoryStore

from zagg.csr import iter_csr_cells, read_csr, write_csr


class TestWriteCsr:
    def test_round_trip_basic(self):
        """Write and read back variable-length per-cell payloads."""
        store = MemoryStore()
        payloads = [
            np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),  # cell 0: 2 rows
            np.array([[5.0, 6.0]], dtype=np.float32),  # cell 2: 1 row
            np.array([[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]], dtype=np.float32),  # cell 5: 3 rows
        ]
        cell_ids = [0, 2, 5]
        write_csr(store, "tdigest", payloads, cell_ids)
        csr = read_csr(store, "tdigest")

        assert csr["values"].shape == (6, 2)
        assert len(csr["offsets"]) == 4  # n_populated + 1
        np.testing.assert_array_equal(csr["cell_ids"], [0, 2, 5])
        np.testing.assert_array_equal(csr["offsets"], [0, 2, 3, 6])

    def test_round_trip_per_cell_slices(self):
        """Per-cell reconstruction via iter_csr_cells is exact."""
        store = MemoryStore()
        payloads = [
            np.array([[1.0, 10.0]], dtype=np.float32),
            np.array([[2.0, 20.0], [3.0, 30.0]], dtype=np.float32),
        ]
        cell_ids = [7, 3]
        write_csr(store, "field", payloads, cell_ids)
        csr = read_csr(store, "field")
        decoded = iter_csr_cells(csr)

        assert decoded[0][0] == 7
        np.testing.assert_array_almost_equal(decoded[0][1], [[1.0, 10.0]])
        assert decoded[1][0] == 3
        np.testing.assert_array_almost_equal(decoded[1][1], [[2.0, 20.0], [3.0, 30.0]])

    def test_empty_payloads_skipped(self):
        """write_csr with all-empty payloads writes empty arrays (no crash)."""
        store = MemoryStore()
        write_csr(store, "empty_field", [], [])
        csr = read_csr(store, "empty_field")
        assert csr["values"].size == 0
        np.testing.assert_array_equal(csr["offsets"], [0])
        assert csr["cell_ids"].size == 0

    def test_mixed_empty_non_empty(self):
        """Empty arrays in values_list are silently skipped."""
        store = MemoryStore()
        payloads = [
            np.array([], dtype=np.float32).reshape(0, 2),
            np.array([[1.0, 2.0]], dtype=np.float32),
            np.array([], dtype=np.float32).reshape(0, 2),
        ]
        cell_ids = [0, 1, 2]
        write_csr(store, "sparse", payloads, cell_ids)
        csr = read_csr(store, "sparse")
        # Only cell 1 has data.
        np.testing.assert_array_equal(csr["cell_ids"], [1])
        np.testing.assert_array_equal(csr["offsets"], [0, 1])
        assert csr["values"].shape[0] == 1

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError, match="same length"):
            write_csr(MemoryStore(), "f", [np.array([1.0])], [0, 1])

    def test_inconsistent_inner_shape_raises(self):
        payloads = [
            np.array([[1.0, 2.0]], dtype=np.float32),  # inner (2,)
            np.array([[3.0, 4.0, 5.0]], dtype=np.float32),  # inner (3,) — mismatch
        ]
        with pytest.raises(ValueError, match="Inconsistent inner shape"):
            write_csr(MemoryStore(), "f", payloads, [0, 1])

    def test_csr_invariant_offsets_last_equals_len_values(self):
        """offsets[-1] must equal len(values) (standard CSR invariant)."""
        store = MemoryStore()
        payloads = [np.arange(6, dtype=np.float32).reshape(3, 2)]
        write_csr(store, "inv", payloads, [4])
        csr = read_csr(store, "inv")
        assert csr["offsets"][-1] == len(csr["values"])

    def test_single_cell_1d_payload(self):
        """1-D per-cell arrays (scalar inner type) are written/read correctly."""
        store = MemoryStore()
        payloads = [np.array([1.0, 2.0, 3.0], dtype=np.float32)]
        write_csr(store, "scalar_inner", payloads, [0])
        csr = read_csr(store, "scalar_inner")
        np.testing.assert_array_equal(csr["values"], [1.0, 2.0, 3.0])
        np.testing.assert_array_equal(csr["offsets"], [0, 3])
        np.testing.assert_array_equal(csr["cell_ids"], [0])

    def test_dtype_preserved(self):
        """The dtype parameter controls the values array dtype."""
        store = MemoryStore()
        payloads = [np.array([[1, 2]], dtype=np.int32)]
        write_csr(store, "ints", payloads, [0], dtype="int32")
        csr = read_csr(store, "ints")
        assert csr["values"].dtype == np.dtype("int32")
