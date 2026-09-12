"""Spec-conformance tests against the committed golden fixtures (issue #340).

``docs/specification.md`` §7: the fixtures under ``tests/data/spec/`` (two
tiny hive stores written by ``tools/generate_spec_fixtures.py`` through the
production write path) are part of the store contract. Every test here binds
one of the spec's normative claims to those committed bytes, two ways:

- **through the shipping readers** (``zagg.readers.tdigest_tensor`` +
  ``zagg.stats.composition``), so the reader cannot drift from the fixtures;
- **through spec-text-only decoders** (struct + zstd + hashlib — no zagg
  read path), so the spec's byte recipes (§1.4 wire framing, §1.5 shard
  index, §5 O11 hashes) are proven sufficient to decode the store without
  importing zagg — the moczarr acceptance criterion.

The expected values in ``*.expected.json`` were computed from the fixture
generator's INPUTS (the arrays handed to the writers), so writer, reader,
and spec text are pinned against each other through the committed bytes.
"""

import base64
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pytest
import zarr
from numcodecs import Zstd
from zarr.storage import LocalStore

from zagg.coverage_toc import coverage_toc, coverage_toc_digest
from zagg.readers.tdigest_tensor import read_cell, read_locations
from zagg.stats.composition import counts_from_composition, unpack_composition

SPEC_DATA = Path(__file__).parent / "data" / "spec"
#: Every fixture with a LEAF — the whole leaf-shaped suite runs over each.
#: ``column`` is here because its leaf is ``minimal``'s inputs written by the
#: same production path: the column writer must leave that leaf untouched
#: (it holds the same payload ``bytes`` objects the leaf staged), and running
#: the suite over both is what asserts it.
FIXTURES = ("minimal", "kitchen_sink", "column")
#: The manifest-only §4.5 declaration fixture (issue #382) — deliberately NOT
#: in ``FIXTURES``: it has no leaf, so nothing leaf-shaped applies to it.
PYRAMID = "pyramid"
#: (fixture, ragged field, element dtype, inner shape) — every committed
#: ``zagg-ragged/1`` array, payload and located siblings alike.
RAGGED_ARRAYS = [
    ("minimal", "h_tdigest", "float32", (2,)),
    ("column", "h_tdigest", "float32", (2,)),
    ("kitchen_sink", "h_tdigest_signal", "float32", (2,)),
    ("kitchen_sink", "h_tdigest_noise", "float32", (2,)),
    ("kitchen_sink", "h_tdigest_signal_locations", "uint64", ()),
    ("kitchen_sink", "h_tdigest_noise_locations", "uint64", ()),
    ("flux", "rx_flux", "float32", (2,)),
    ("temporal", "h_tdigest", "float32", (2,)),
    ("temporal", "h_tdigest_locations", "uint64", ()),
    ("temporal", "h_tdigest_times", "uint64", ()),
]
SENTINEL = 2**64 - 1

#: FROZEN §5 combined digests, as literals — the anti-circularity pin (the
#: ``FROZEN_COMBINED_4111`` precedent, espg/moczarr ``ba804e6``). Both the
#: fixture generator and every recomputation here run the §5 recipe, so a
#: changed length-prefix width, joiner, hash function, or key set would agree
#: with itself and sail through. These literals are what such a change has to
#: get past — and §5 is the recipe moczarr adopts verbatim, so a silent zagg
#: change is a silent cross-implementation divergence.
FROZEN_COMBINED = {
    "minimal": "2f4ff37de621de05962ab720cec05fd643757977f1afbd0e859ca588a143b72e",
    # kitchen_sink re-pinned under the authalic convention (mortie >=0.9.8,
    # issue #438): its located point words moved, so the fixture's two
    # ``*_locations`` arrays -- and only those -- hash differently.
    "kitchen_sink": "7ee8ad9278eaa6ccbfa95aed978d09768ff0865b569c025b6f8e2b91e46843c7",
    # column/'s LEAF is minimal's, byte for byte — the same literal is the
    # cross-fixture pin that writing a column perturbs nothing.
    "column": "2f4ff37de621de05962ab720cec05fd643757977f1afbd0e859ca588a143b72e",
    # The §2.0 flux fixture (issue #424) — asserted by TestFluxDeclaration
    # (flux is not in FIXTURES: the leaf-shaped suite hardcodes h_tdigest).
    "flux": "910a9b12d34b4d072f454b2dd1cc232a6a9a92b536a8d26f5395154a1c02bc32",
    # The §8 temporal fixture (issue #443) — asserted by
    # TestTemporalDeclaration (raster_toc is not in FIXTURES: it carries no
    # ragged array at all, so nothing the leaf-shaped suite asserts applies).
    "raster_toc": "42263e046ecf4d71de8460063b38e6f15522e245cdc2182ce3a0acaf35db7e4e",
    # The §8.2/§8.3/§9 companion fixture (issue #410) — asserted by
    # TestTemporalCompanions (temporal/ is not in FIXTURES: the leaf-shaped
    # suite's digest assertions assume an UNLOCATED h_tdigest).
    # Re-pinned for the §10.5 gap cell (issue #489): the last cell's clock
    # moved, so its two toc companions -- and only those -- hash differently.
    "temporal": "6c7a0c5a3f54684b67e7e2daa66e29ffcf1af102833e7b3c4a4f989e36752887",
}
#: The same pin over the §4.6 COLUMN artifact of the ``column/`` fixture (not
#: its leaf, which is ``minimal``'s): the only committed store whose §5 key
#: shape is multi-group (``"5/h_tdigest"``, ``"4/morton"``, …).
FROZEN_COMBINED_COLUMN = "061277118c8cc4c0b8ae3c62fe6515fefa8f9ccb717bd3104f4c8797edbcc918"
#: FROZEN per-array digests, one per §5.2 element kind: a vlen digest payload
#: array, a vlen uint64 locations sibling, and two fixed-width arrays.
FROZEN_ARRAYS = {
    ("minimal", "6/h_tdigest"): "ba43774865b69f60aaf42875c9c4a72edaca6058bd9a4fb95760d43f203c9d4c",
    ("minimal", "6/morton"): "f6282635e373d534ef4d91166d306441049e7bbed3d7d2ec8add306f62274d06",
    (
        "kitchen_sink",
        "6/h_tdigest_signal_locations",
    ): "11e0a598b7f695d7fe816ecc853deba86d2006b8470bb94a49acc36db9de8af9",
    (
        "kitchen_sink",
        "6/composition",
    ): "04886a9dfb60c8a53de48202dc3d5ac694698864825426f084be961aa678acd5",
    ("flux", "6/rx_flux"): "3ba141cb29b1771dee4c4c3f2aadec42b7de69e075d4334d6d7187301dc59eb8",
    # The §8 uint64 toc-word time coordinate (issue #443).
    (
        "raster_toc",
        "6/time",
    ): "551f7be5718086c2ca1d379e1d1581368d21e4ca1433ce3ae4ae397e11b08f7f",
    # The §8.2 dense per-cell companion and the §8.3 ragged per-centroid
    # sibling (issue #410) — one frozen literal per companion element kind.
    # Both re-pinned for the §10.5 gap cell (issue #489); the h_tdigest
    # payload and its locations sibling are untouched by that offset.
    (
        "temporal",
        "6/observed",
    ): "b8c48a74213a1a06fdaa554dbe580c222bc83dddf71264bcf88ff9084fb44402",
    (
        "temporal",
        "6/h_tdigest_times",
    ): "03a1e44b982a553c8a419f66563d712b7f070c5f0761e1982f9912740ffee468",
}


def _decode_shard(path: Path, k: int) -> dict:
    """``{chunk_ordinal: raw framed bytes}`` per the §1.5 index recipe."""
    blob = path.read_bytes()
    index = np.frombuffer(blob[-(16 * k + 4) : -4], dtype="<u8").reshape(k, 2)
    chunks = {}
    for ordinal, (offset, nbytes) in enumerate(index):
        if int(offset) == SENTINEL:
            assert int(nbytes) == SENTINEL
            continue
        chunks[ordinal] = bytes(Zstd().decode(blob[int(offset) : int(offset) + int(nbytes)]))
    return chunks


def _decode_framing(raw: bytes, n_cells: int) -> list:
    """The §1.4 recipe: u32 cell count, then u32 length + payload per cell."""
    (count,) = struct.unpack_from("<I", raw, 0)
    assert count == n_cells
    payloads, pos = [], 4
    for _ in range(count):
        (length,) = struct.unpack_from("<I", raw, pos)
        pos += 4
        payloads.append(raw[pos : pos + length])
        pos += length
    assert pos == len(raw)
    return payloads


def _expected(name):
    return json.loads((SPEC_DATA / f"{name}.expected.json").read_text())


def _leaf_dir(name, exp) -> Path:
    return SPEC_DATA / name / exp["leaf"]


def _leaf_store(name, exp):
    return LocalStore(str(_leaf_dir(name, exp)))


def _array_meta(name, exp, field) -> dict:
    """The array's raw ``zarr.json`` — unnormalized, straight off disk."""
    return json.loads((_leaf_dir(name, exp) / exp["group"] / field / "zarr.json").read_text())


def _digest_expectations(exp):
    """Yield ``(field, cell_index, expected (k, 2) float32 digest)``."""
    for cell in exp["cells"]:
        for field in cell:
            if field.startswith("h_tdigest") and not field.endswith(("_locations", "_times")):
                want = np.array(cell[field], dtype=np.float32).reshape(-1, 2)
                yield field, cell["index"], want


class TestRaggedAttrs:
    """§1.2 — the self-describing ``ragged`` attrs block."""

    @pytest.mark.parametrize(("name", "field", "dtype", "inner"), RAGGED_ARRAYS)
    def test_spec_marker_and_element(self, name, field, dtype, inner):
        exp = _expected(name)
        block = _array_meta(name, exp, field)["attributes"]["ragged"]
        assert block["spec"] == "zagg-ragged/1"
        assert block["element"] == {"dtype": dtype, "shape": [-1, *inner]}

    def test_locations_binding_declared_not_named(self):
        exp = _expected("kitchen_sink")
        for stratum in ("signal", "noise"):
            payload = _array_meta("kitchen_sink", exp, f"h_tdigest_{stratum}")["attributes"]
            assert payload["ragged"]["locations"] == f"h_tdigest_{stratum}_locations"
            # Provenance attrs ride the payload array only (§1.2/§3.3)...
            assert payload["stratum"] == stratum
            assert payload["signal_threshold"] == 2
            sibling = _array_meta("kitchen_sink", exp, f"h_tdigest_{stratum}_locations")
            # ...and the sibling carries no user attrs and no locations key.
            assert set(sibling["attributes"]) == {"ragged"}
            assert "locations" not in sibling["attributes"]["ragged"]

    def test_unlocated_field_records_nothing(self):
        exp = _expected("minimal")
        assert "locations" not in _array_meta("minimal", exp, "h_tdigest")["attributes"]["ragged"]


class TestRaggedCodecChain:
    """§1.3/§1.5 — codec chain and the sharded storage geometry."""

    @pytest.mark.parametrize(("name", "field", "dtype", "inner"), RAGGED_ARRAYS)
    def test_sharded_vlen_zstd3_chain(self, name, field, dtype, inner):
        exp = _expected(name)
        meta = _array_meta(name, exp, field)
        assert meta["fill_value"] == ""
        assert meta["data_type"] in ("variable_length_bytes", "bytes")
        cells = exp["cells_per_chunk"] * exp["chunks_per_shard"]
        assert meta["chunk_grid"]["configuration"]["chunk_shape"] == [cells]
        (sharding,) = meta["codecs"]
        assert sharding["name"] == "sharding_indexed"
        cfg = sharding["configuration"]
        assert cfg["chunk_shape"] == [exp["cells_per_chunk"]]
        assert [c["name"] for c in cfg["codecs"]] == ["vlen-bytes", "zstd"]
        assert cfg["codecs"][1]["configuration"] == {"level": 3, "checksum": False}
        assert [c["name"] for c in cfg["index_codecs"]] == ["bytes", "crc32c"]
        assert cfg["index_location"] == "end"


class TestWireFraming:
    """§1.4/§1.5 — spec-text-only decode of the committed shard objects.

    No zagg read path: the shard index suffix, the zstd inner chunks, and
    the u32 vlen framing are parsed exactly as the spec describes, and the
    decoded payloads must equal the committed expectations.
    """

    @pytest.mark.parametrize(("name", "field", "dtype", "inner"), RAGGED_ARRAYS)
    def test_payloads_decode_per_spec(self, name, field, dtype, inner):
        exp = _expected(name)
        per_chunk = exp["cells_per_chunk"]
        chunks = _decode_shard(
            _leaf_dir(name, exp) / exp["group"] / field / "c" / "0", exp["chunks_per_shard"]
        )
        # The empty chunk is absent from the shard index (the §1.5 sentinel).
        assert exp["empty_chunk"] not in chunks
        decoded: dict[int, np.ndarray] = {}
        for ordinal, raw in chunks.items():
            for local, payload in enumerate(_decode_framing(raw, per_chunk)):
                cell = ordinal * per_chunk + local
                decoded[cell] = np.frombuffer(payload, dtype=f"<{np.dtype(dtype).str[1:]}")
                decoded[cell] = decoded[cell].reshape(-1, *inner)
        source = field.removesuffix("_locations")
        for cell in exp["cells"]:
            key = field if field.endswith("_locations") else source
            want = cell.get(key if key in cell else field)
            if want is None:
                continue
            got = decoded.get(cell["index"], np.empty((0, *inner), dtype=dtype))
            if dtype == "uint64":
                np.testing.assert_array_equal(got, np.array([int(w) for w in want], np.uint64))
            else:
                np.testing.assert_array_equal(
                    got, np.array(want, dtype=np.float32).reshape(-1, *inner)
                )

    def test_populated_chunks_only_no_ghost_payloads(self):
        # Cells outside the expected set decode as b"" (the fill) — §1.1.
        exp = _expected("minimal")
        chunks = _decode_shard(
            _leaf_dir("minimal", exp) / "6" / "h_tdigest" / "c" / "0", exp["chunks_per_shard"]
        )
        populated = {c["index"] for c in exp["cells"]}
        for ordinal, raw in chunks.items():
            for local, payload in enumerate(_decode_framing(raw, exp["cells_per_chunk"])):
                cell = ordinal * exp["cells_per_chunk"] + local
                assert (len(payload) > 0) == (cell in populated)


class TestDigestPayload:
    """§2 — centroid semantics through the shipping reader."""

    @pytest.mark.parametrize("name", FIXTURES)
    def test_read_cell_matches_expected(self, name):
        exp = _expected(name)
        store = _leaf_store(name, exp)
        for field, cell_index, want in _digest_expectations(exp):
            got = read_cell(store, f"{exp['group']}/{field}", cell_index)
            np.testing.assert_array_equal(got, want)
            assert got.dtype == np.float32

    @pytest.mark.parametrize("name", FIXTURES)
    def test_absent_cell_decodes_zero_length(self, name):
        exp = _expected(name)
        store = _leaf_store(name, exp)
        field = "h_tdigest_signal" if name == "kitchen_sink" else "h_tdigest"
        empty_cell = exp["empty_chunk"] * exp["cells_per_chunk"]
        assert read_cell(store, f"{exp['group']}/{field}", empty_cell).shape == (0, 2)

    @pytest.mark.parametrize("name", FIXTURES)
    def test_means_sorted_and_weights_are_exact_counts(self, name):
        # §2.1 over the recorded expectations; bound to the store through
        # test_read_cell_matches_expected above, which reads the same values
        # back with the shipping reader.
        exp = _expected(name)
        for cell in exp["cells"]:
            counts = {"h_tdigest": cell["count"]}
            if name == "kitchen_sink":
                counts = {
                    "h_tdigest_signal": cell["n_signal"],
                    "h_tdigest_noise": cell["count"] - cell["n_signal"],
                }
            for field, n in counts.items():
                digest = np.array(cell.get(field, []), dtype=np.float32).reshape(-1, 2)
                assert np.all(np.diff(digest[:, 0]) >= 0), f"{field} means not ascending"
                assert digest[:, 1].sum() == n, f"{field} weights != exact count"
                assert np.all(digest[:, 1] >= 1) or digest.size == 0

    def test_locations_row_aligned_with_payload(self):
        exp = _expected("kitchen_sink")
        store = _leaf_store("kitchen_sink", exp)
        seen = 0
        for stratum in ("signal", "noise"):
            field = f"{exp['group']}/h_tdigest_{stratum}"
            by_cell = {
                c["index"]: np.array(
                    [int(w) for w in c[f"h_tdigest_{stratum}_locations"]], dtype=np.uint64
                )
                for c in exp["cells"]
            }
            for cell in exp["cells"]:
                got = read_cell(store, f"{field}_locations", cell["index"])
                np.testing.assert_array_equal(got.reshape(-1), by_cell[cell["index"]])
                assert got.reshape(-1).shape[0] == len(cell[f"h_tdigest_{stratum}"])
                seen += 1
        assert seen == 2 * len(exp["cells"])

    def test_read_locations_binds_through_attrs(self):
        exp = _expected("kitchen_sink")
        store = _leaf_store("kitchen_sink", exp)
        rows = list(read_locations(store, f"{exp['group']}/h_tdigest_signal"))
        got = sorted(np.concatenate([locs for _w, _rc, locs in rows]).tolist())
        want = sorted(int(w) for c in exp["cells"] for w in c["h_tdigest_signal_locations"])
        assert got == want


class TestFluxDeclaration:
    """§2.0 — the weights declaration, pinned on the committed `flux/` store.

    The counts side of the contract needs no new fixture: `minimal/` predates
    §2.0 and is deliberately not regenerated, so its absent `weights` key IS
    the committed absent-key ⇒ counts pin (issue #424).
    """

    def test_weights_key_is_a_sibling_of_the_ragged_block(self):
        exp = _expected("flux")
        attrs = _array_meta("flux", exp, "rx_flux")["attributes"]
        assert attrs["weights"] == "flux"
        # A sibling key, never inside the versioned block: the block is
        # retired wholesale under /2, a sibling survives the migration.
        assert "weights" not in attrs["ragged"]
        assert attrs["ragged"]["spec"] == "zagg-ragged/1"

    def test_calibration_provenance_recorded(self):
        exp = _expected("flux")
        attrs = _array_meta("flux", exp, "rx_flux")["attributes"]
        assert attrs["gain"] == exp["gain"]
        assert {"name", "version"} <= set(attrs["gain"])

    def test_fold_arrays_re_declare_weights_and_gain(self):
        """A fold's own payload arrays carry §2.0 through (review, issue #424).

        The overview/column writer reconstructs its template from the manifest
        field entry alone, so a declaration that stops at the leaf leaves every
        folded array reading as ``counts`` — and the fold gate then refuses the
        whole cascade above the finest level.
        """
        exp = _expected("flux")
        column = _leaf_dir("flux", exp).parent / "all.pyramid.zarr"
        groups = sorted(p.name for p in column.iterdir() if p.is_dir())
        assert groups  # the fixture's §4.6 column, folded from the flux leaf
        for group in groups:
            meta = json.loads((column / group / "rx_flux" / "zarr.json").read_text())
            attrs = meta["attributes"]
            assert attrs["weights"] == "flux"
            assert attrs["gain"] == exp["gain"]
            assert "weights" not in attrs["ragged"]  # sibling key, never inside

    def test_absent_key_reads_as_counts_on_the_committed_minimal(self):
        from zagg.grids.base import weights_declaration

        exp = _expected("minimal")
        attrs = _array_meta("minimal", exp, "h_tdigest")["attributes"]
        assert "weights" not in attrs
        assert weights_declaration(attrs) == "counts"

    def test_flux_weights_are_positive_fractional_reals(self):
        exp = _expected("flux")
        store = _leaf_store("flux", exp)
        non_integer = 0
        for cell in exp["cells"]:
            got = read_cell(store, f"{exp['group']}/rx_flux", cell["index"])
            want = np.array(cell["rx_flux"], dtype=np.float32).reshape(-1, 2)
            np.testing.assert_array_equal(got, want)
            assert np.all(got[:, 1] > 0)  # §2.0: positive, no zero-weight rows
            assert np.all(np.diff(got[:, 0]) >= 0)  # §2.1 ordering unchanged
            total = float(got[:, 1].astype(np.float64).sum())
            assert total == cell["flux_sum"]
            non_integer += int(total != round(total))
        assert non_integer  # flux sums are photoelectron estimates, not counts

    def test_leaf_is_stamped_and_manifest_marked(self):
        exp = _expected("flux")
        attrs = json.loads((_leaf_dir("flux", exp) / "zarr.json").read_text())["attributes"]
        assert attrs["morton_hive_commit"]["complete"] is True
        manifest = json.loads((SPEC_DATA / "flux" / "morton_hive.json").read_text())
        assert manifest["spec"] == "morton-hive/1"

    def test_per_array_hashes_match_golden(self):
        exp = _expected("flux")
        got = TestContentHashes._hash_leaf(_leaf_dir("flux", exp))
        assert got == exp["content_hashes"]["arrays"]

    def test_frozen_digests_pin_the_recipe(self):
        # flux is not in FIXTURES (the leaf-shaped suite hardcodes h_tdigest),
        # so its frozen literals are asserted here.
        exp = _expected("flux")
        assert exp["content_hashes"]["combined"] == FROZEN_COMBINED["flux"]
        arrays = exp["content_hashes"]["arrays"]
        assert arrays["6/rx_flux"] == FROZEN_ARRAYS[("flux", "6/rx_flux")]
        combined = hashlib.sha256("\n".join(sorted(arrays.values())).encode()).hexdigest()
        assert combined == exp["content_hashes"]["combined"]


class TestCoordinateAndDense:
    """§1.1 — the `morton` coordinate and the dense `count` field, off the store.

    The fixtures *record* `morton` and `count` per populated cell, and §7 tells
    an external reader to reproduce them; without these asserts a change to the
    coordinate (ordering, dtype, nested-vs-ring) or to a dense write would
    leave the recorded values wrong and this suite green — while moczarr, whose
    whole addressing path keys on `morton`, would break (review finding).
    """

    @staticmethod
    def _open(name, exp, field):
        return zarr.open_array(_leaf_store(name, exp), path=f"{exp['group']}/{field}", mode="r")

    @pytest.mark.parametrize("name", FIXTURES)
    def test_morton_coordinate_matches_expected(self, name):
        exp = _expected(name)
        words = [int(w) for w in np.asarray(self._open(name, exp, "morton")[:])]
        assert self._open(name, exp, "morton").dtype == np.uint64
        assert len(words) == exp["cells_per_chunk"] * exp["chunks_per_shard"]
        for cell in exp["cells"]:
            assert words[cell["index"]] == int(cell["morton"])

    @pytest.mark.parametrize("name", FIXTURES)
    def test_morton_ascends_per_written_chunk_and_is_fill_where_absent(self, name):
        # The coordinate array gets the same §1.5 sub-shard sparsity as every
        # other array: the empty inner chunk's slots hold the 0 fill, so a
        # reader MUST NOT assume `morton` is dense across the shard.
        exp = _expected(name)
        per_chunk = exp["cells_per_chunk"]
        words = [int(w) for w in np.asarray(self._open(name, exp, "morton")[:])]
        for ordinal in range(exp["chunks_per_shard"]):
            chunk = words[ordinal * per_chunk : (ordinal + 1) * per_chunk]
            if ordinal == exp["empty_chunk"]:
                assert chunk == [0] * per_chunk
            else:
                assert all(b > a for a, b in zip(chunk, chunk[1:], strict=False))

    @pytest.mark.parametrize("name", FIXTURES)
    def test_count_field_matches_expected(self, name):
        exp = _expected(name)
        arr = self._open(name, exp, "count")
        assert arr.dtype == np.int32
        counts = np.asarray(arr[:])
        by_cell = {c["index"]: c["count"] for c in exp["cells"]}
        for cell in range(counts.shape[0]):
            assert int(counts[cell]) == by_cell.get(cell, 0)


class TestComposition:
    """§3 — the packed composition word on the committed store."""

    def _array(self):
        exp = _expected("kitchen_sink")
        store = _leaf_store("kitchen_sink", exp)
        return exp, zarr.open_array(store, path=f"{exp['group']}/composition", mode="r")

    def test_attrs_block(self):
        exp, arr = self._array()
        assert arr.attrs["composition"] == {
            "spec": "zagg-composition/1",
            "lanes": ["land", "ocean", "sea_ice", "land_ice", "inland_water", "low", "med", "high"],
            "of": "h_tdigest_signal",
            "threshold": 2,
        }

    def test_attrs_block_binds_to_the_writer_constants(self):
        # §3.3: spec/lanes are writer-stamped, so the committed block must
        # equal the module constants — not just the literal above (which the
        # generator's own config could otherwise have dictated).
        from zagg.stats.composition import COMPOSITION_SPEC, LANES

        _exp, arr = self._array()
        block = arr.attrs["composition"]
        assert block["spec"] == COMPOSITION_SPEC
        assert block["lanes"] == list(LANES)

    def test_words_match_expected_and_empty_cells_zero(self):
        exp, arr = self._array()
        words = np.asarray(arr[:])
        by_cell = {c["index"]: int(c["composition"]) for c in exp["cells"]}
        for cell in range(words.shape[0]):
            assert int(words[cell]) == by_cell.get(cell, 0)

    def test_golden_word_single_photon(self):
        # §3.1: one signal photon, confs (4, -1, 0, 3, 1) at threshold 2.
        exp, arr = self._array()
        (golden,) = [c for c in exp["cells"] if c["n_signal"] == 1 and c["count"] == 1]
        word = int(arr[golden["index"]])
        assert word == 0xFF000000FF0000FF
        np.testing.assert_array_equal(unpack_composition(word), [255, 0, 0, 255, 0, 0, 0, 255])
        np.testing.assert_array_equal(counts_from_composition(word, 1), [1, 0, 0, 1, 0, 0, 0, 1])

    def test_n_signal_is_the_of_digests_total_weight(self):
        # §3.3: the ``of`` linkage — count recovery keys on the signal
        # digest's summed weights, which equal the recorded n_signal.
        exp, _arr = self._array()
        store = _leaf_store("kitchen_sink", exp)
        for cell in exp["cells"]:
            digest = read_cell(store, f"{exp['group']}/h_tdigest_signal", cell["index"])
            assert int(digest[:, 1].sum()) == cell["n_signal"]


class TestContentHashes:
    """§5 — the O11 recipe, reimplemented from spec text only.

    Three legs, because the recipe is what moczarr adopts verbatim: the
    decoded-value hasher below (recomputed over the written store), an
    independent recomputation of the vlen digests from the shard objects
    alone, and the :data:`FROZEN_COMBINED` / :data:`FROZEN_ARRAYS` literals —
    the only leg that can fail when a recipe change is made consistently on
    both sides.
    """

    @staticmethod
    def _element_bytes(element) -> bytes:
        """The §5.2 element→bytes normalization, from the spec table.

        ``None`` (an unwritten vlen cell may decode as ``None``) is
        zero-length; a `/1` cell is bytes as-is; a typed `/2` ndarray cell is
        C-contiguous little-endian bytes at its dtype; anything else raises
        rather than hashing something wrong.
        """
        if element is None:
            return b""
        if isinstance(element, bytes | bytearray | memoryview):
            return bytes(element)
        if isinstance(element, str):
            return element.encode()
        if isinstance(element, np.ndarray):
            values = np.ascontiguousarray(element)
            if values.dtype.byteorder == ">":
                values = values.astype(values.dtype.newbyteorder("<"))
            return values.tobytes()
        raise ValueError(f"vlen element of type {type(element).__name__} has no O11 recipe")

    @staticmethod
    def _hash_leaf(leaf: Path) -> dict:
        group = zarr.open_group(LocalStore(str(leaf)), mode="r", zarr_format=3)
        hashes = {}
        for key, node in group.members(max_depth=None):
            if not isinstance(node, zarr.Array):
                continue
            values = np.ascontiguousarray(node[...])
            if values.dtype.kind == "O":
                digest = hashlib.sha256()
                for element in values.ravel(order="C"):
                    payload = TestContentHashes._element_bytes(element)
                    digest.update(len(payload).to_bytes(8, "little"))
                    digest.update(payload)
                hashes[key] = digest.hexdigest()
                continue
            if values.dtype.byteorder == ">":
                values = values.astype(values.dtype.newbyteorder("<"))
            hashes[key] = hashlib.sha256(values.tobytes()).hexdigest()
        return hashes

    @staticmethod
    def _vlen_digest_from_shard_bytes(name, exp, field) -> str:
        """The §5.2 vlen digest with NO zarr and no shared helper.

        Payloads come from the §1.4/§1.5 byte recipes (``_decode_shard`` /
        ``_decode_framing``), so this is a second, independent implementation
        of the recipe — an absent inner chunk (the §1.5 sentinel) contributes
        its cells' zero lengths, which is what makes the digest cover the
        cell *grid* rather than only the payloads.
        """
        chunks = _decode_shard(
            _leaf_dir(name, exp) / exp["group"] / field / "c" / "0", exp["chunks_per_shard"]
        )
        per_chunk = exp["cells_per_chunk"]
        digest = hashlib.sha256()
        for ordinal in range(exp["chunks_per_shard"]):
            raw = chunks.get(ordinal)
            payloads = _decode_framing(raw, per_chunk) if raw is not None else [b""] * per_chunk
            for payload in payloads:
                digest.update(len(payload).to_bytes(8, "little"))
                digest.update(payload)
        return digest.hexdigest()

    @pytest.mark.parametrize("name", FIXTURES)
    def test_per_array_hashes_match_golden(self, name):
        exp = _expected(name)
        assert self._hash_leaf(_leaf_dir(name, exp)) == exp["content_hashes"]["arrays"]

    @pytest.mark.parametrize(("name", "field", "dtype", "inner"), RAGGED_ARRAYS)
    def test_vlen_digest_from_shard_bytes_only(self, name, field, dtype, inner):
        # The §5.2 vlen recipe reproduced from the shard objects alone — the
        # decoded-value hasher above and the generator both go through zarr,
        # so this is the leg that proves the spec text suffices.
        exp = _expected(name)
        want = exp["content_hashes"]["arrays"][f"{exp['group']}/{field}"]
        assert self._vlen_digest_from_shard_bytes(name, exp, field) == want

    @pytest.mark.parametrize("name", FIXTURES)
    def test_combined_hash_recipe(self, name):
        exp = _expected(name)
        arrays = exp["content_hashes"]["arrays"]
        combined = hashlib.sha256("\n".join(sorted(arrays.values())).encode()).hexdigest()
        assert combined == exp["content_hashes"]["combined"]

    @pytest.mark.parametrize("name", FIXTURES)
    def test_recorded_arrays_map_is_key_sorted(self, name):
        # Regeneration must be diff-clean: member enumeration yields
        # concurrently, so the generator sorts the map before recording it.
        arrays = _expected(name)["content_hashes"]["arrays"]
        assert list(arrays) == sorted(arrays)

    def test_element_bytes_normalization(self):
        # §5.2's table: None ≡ b"", a typed /2 ndarray cell normalizes to
        # C-contiguous little-endian bytes, anything else raises.
        norm = TestContentHashes._element_bytes
        assert norm(None) == b""
        assert norm(bytearray(b"ab")) == b"ab"
        assert norm(memoryview(b"ab")) == b"ab"
        assert norm("ab") == b"ab"
        little = np.array([[1.5, 2.0]], dtype="<f4")
        assert norm(np.asfortranarray(little.astype(">f4"))) == little.tobytes()
        with pytest.raises(ValueError, match="no O11 recipe"):
            norm(3.5)

    @pytest.mark.parametrize("name", FIXTURES)
    def test_frozen_digests_pin_the_recipe(self, name):
        # Not self-certified: the literals are the pin. Every other assertion
        # here (and the generator's) runs the same recipe on both sides, so
        # only a frozen value catches a recipe change made consistently.
        exp = _expected(name)
        assert exp["content_hashes"]["combined"] == FROZEN_COMBINED[name]
        for (fixture, key), frozen in FROZEN_ARRAYS.items():
            if fixture == name:
                assert exp["content_hashes"]["arrays"][key] == frozen

    def test_the_column_store_leaf_is_unperturbed(self):
        # Writing a column must not touch the leaf beside it — the writer
        # folds from the SAME payload ``bytes`` objects the leaf staged, so an
        # in-place fold would corrupt it. column/'s leaf is minimal's inputs
        # through the same production path: identical committed bytes.
        assert _expected("column")["content_hashes"] == _expected("minimal")["content_hashes"]
        assert _expected("column")["cells"] == _expected("minimal")["cells"]


class TestStoreEnvelope:
    """The fixture leaves are complete, stamped stores (D4; §4.3 debris rule)."""

    @pytest.mark.parametrize("name", FIXTURES)
    def test_leaf_is_stamped_and_unclassified(self, name):
        exp = _expected(name)
        attrs = json.loads((_leaf_dir(name, exp) / "zarr.json").read_text())["attributes"]
        assert attrs["morton_hive_commit"]["complete"] is True
        # Source leaves carry no role key — absence means source (§4.3).
        assert "role" not in attrs

    @pytest.mark.parametrize("name", FIXTURES)
    def test_manifest_spec_marker(self, name):
        manifest = json.loads((SPEC_DATA / name / "morton_hive.json").read_text())
        assert manifest["spec"] == "morton-hive/1"


def _pyramid_block():
    """The ``pyramid/`` fixture's committed manifest declaration (§4.5)."""
    manifest = json.loads((SPEC_DATA / PYRAMID / "morton_hive.json").read_text())
    return manifest["pyramid"]


class TestPyramidV2Declaration:
    """§4.5 — the ``zagg-pyramid/2`` overviews declaration (issue #382).

    The collapsed grammar: a leaf resolution list plus the §4.4 fixed
    every-order ladder, recorded fully expanded at block level. Every
    assertion decodes the committed ``pyramid/`` manifest from spec text
    alone; the expected values come from the generator's INPUTS
    (``tools/generate_spec_fixtures.py``), never read back through zagg.
    """

    def test_marker_and_block_shape(self):
        block = _pyramid_block()
        assert block["spec"] == "zagg-pyramid/2"
        overview = block["overview"]
        # §4.5: under /2 the schedule is the BLOCK-level `overviews` list (the
        # store-wide product declaration); the `overview` family dict keeps
        # one sweep leg's execution regime and never carries the schedule.
        # orders/spacing do not exist anywhere in a /2 block. With a non-empty
        # schedule, all_time and fields MUST be present in the family dict.
        assert block["overviews"] and "overviews" not in overview
        assert "orders" not in overview and "spacing" not in overview
        assert overview["all_time"] is False
        assert set(overview["fields"]) == set(_expected(PYRAMID)["fields"])

    def test_overviews_are_the_fully_expanded_list(self):
        # §4.5: the manifest records the EXPANDED (node, cells) list —
        # readers never re-derive the ladder — while the config declared
        # leaf resolutions only.
        exp = _expected(PYRAMID)
        levels = _pyramid_block()["overviews"]
        # `actuals` is the ADDITIVE per-entry key of §4.5 (issue #384; a
        # reader MUST tolerate additional keys) — the declaration itself is
        # the (node, cells) pair, compared exactly.
        assert [{"node": e["node"], "cells": e["cells"]} for e in levels] == exp["overviews"]
        assert all(isinstance(e["cells"], list) for e in levels)
        assert isinstance(exp["declared"]["overviews"], list)  # the raw knob: ints only
        assert all(isinstance(r, int) for r in exp["declared"]["overviews"])

    def test_leaf_entry_carries_the_declared_resolutions(self):
        # §4.5: every declared resolution materializes at the shard node —
        # the first entry IS the leaf entry, strictly inside the window.
        exp = _expected(PYRAMID)
        leaf = _pyramid_block()["overviews"][0]
        assert leaf["node"] == exp["shard_order"]
        assert leaf["cells"] == exp["declared"]["overviews"]
        assert all(b < a for a, b in zip(leaf["cells"], leaf["cells"][1:]))
        assert all(exp["shard_order"] < r < exp["cell_order"] for r in leaf["cells"])

    def test_fixed_ladder_law_decodes_per_spec(self):
        # §4.4: above the shard the schedule is LAW — with d = base - shard
        # (base the coarsest leaf resolution), every order k from shard - 1
        # down to 0 inclusive carries exactly one member at k + d. Every
        # store roots at order 0.
        exp = _expected(PYRAMID)
        levels = _pyramid_block()["overviews"]
        s = exp["shard_order"]
        d = levels[0]["cells"][-1] - s
        assert d >= 1
        assert [{"node": e["node"], "cells": e["cells"]} for e in levels[1:]] == [
            {"node": k, "cells": [k + d]} for k in range(s - 1, -1, -1)
        ]
        assert levels[-1]["node"] == 0

    def test_slab_lengths_decode_per_spec(self):
        # §4.4: a member r at an order-k node holds 4^(r - k) cells.
        exp = _expected(PYRAMID)
        for entry, slabs in zip(_pyramid_block()["overviews"], exp["slabs"]):
            assert [4 ** (r - entry["node"]) for r in entry["cells"]] == slabs

    def test_per_entry_actuals_decode_per_spec(self):
        # §4.5 (issue #384): materialization actuals nest inside the level
        # entry that owns them — regime, merges-from-raw (2 for every
        # upfront merge level, NEVER 3; 1 for gathers and the leaf column),
        # `source_children` on the stage regimes only, and unpinned
        # timestamp/run-id values a reader tolerates.
        exp = _expected(PYRAMID)
        for entry in _pyramid_block()["overviews"]:
            expected = exp["actuals"][str(entry["node"])]
            actuals = entry["actuals"]
            assert actuals["regime"] == expected["regime"]
            assert actuals["merges_from_raw"] == expected["merges_from_raw"]
            assert actuals["merges_from_raw"] <= 2  # gen 3 is append-later only
            if expected["regime"] == "leaf-column":
                assert "source_children" not in actuals
            else:
                assert actuals["source_children"] == expected["source_children"]
            assert "generated_at" in actuals

    def test_fold_declaration_keys_ride_along(self):
        # PR #379's declaration keys are revision-independent (§4.5).
        exp = _expected(PYRAMID)
        overview = _pyramid_block()["overview"]
        assert overview["fold_source"] == exp["fold_source"]
        assert overview["exact_levels"] == exp["exact_levels"]

    def test_v1_era_actuals_preserved_across_the_revision_bump(self):
        # §4.5: on a /2 store the block-level materialized map is the /1-era
        # inventory, preserved verbatim by the declare_pyramid retrofit.
        exp = _expected(PYRAMID)
        actuals = _pyramid_block()["overview"]["materialized"]
        assert actuals["orders"] == exp["materialized"]["orders"]
        # JSON has no integer keys: fold_sources is string-keyed (§4.5).
        assert actuals["fold_sources"] == exp["materialized"]["fold_sources"]
        assert set(actuals["fold_sources"]) <= {str(k) for k in actuals["orders"]}
        assert actuals["generated_at"]  # present; value is sweep-time truth

    def test_none_class_entry_is_class_only(self):
        # The recorded absence (D24 option A) is revision-independent.
        assert _pyramid_block()["overview"]["fields"]["h_mean"] == {"class": "none"}

    def test_default_derivation_matches_the_spec_formula(self):
        # §4.5's derived default (knob omitted -> one leaf resolution at the
        # chunk order, then the same §4.4 fixed ladder), computed from the
        # spec text for this geometry, must equal the committed expectation —
        # the formula on the page and the one the fixture records cannot
        # drift apart. The CODE binding (zagg's own default_overviews) is
        # pinned in tests/test_pyramid.py::TestDefaultOverviews.
        exp = _expected(PYRAMID)
        s, chunk = exp["shard_order"], exp["chunk_order"]
        d = chunk - s
        formula = [{"node": s, "cells": [chunk]}] + [
            {"node": k, "cells": [k + d]} for k in range(s - 1, -1, -1)
        ]
        assert formula == exp["default_overviews"]

    def test_v1_compat_constant_depth_rule(self):
        # §4.4: /1 is the special case cells = [node + (c - s)]. The leaf
        # fixtures' committed pyramid blocks predate the §4.5 /1 grammar
        # (their §7 conformance claim covers §1–§3 and §5 only), so the /1
        # rule is pinned against the production builder instead: without a
        # levels knob the block stays /1, never carries a levels key, and
        # every declared order's one implied member is the constant-depth
        # one — the /2 spelling of the same store.
        from zagg.config import PipelineConfig
        from zagg.sweep_overview import build_pyramid_block

        s, c = _expected(PYRAMID)["shard_order"], _expected(PYRAMID)["cell_order"]
        cfg = PipelineConfig(
            aggregation={
                "coordinates": {"morton": {"dtype": "uint64", "fill_value": 0}},
                "variables": {
                    "count": {"function": "len", "source": "h", "dtype": "int32", "fill_value": 0}
                },
            },
            output={"store_layout": "hive"},
        )
        block = build_pyramid_block(cfg, shard_order=s)
        assert block["spec"] == "zagg-pyramid/1"
        assert "overviews" not in block  # /1 never carries the /2 schedule key
        overview = block["overview"]
        # The whole derived /1 schedule for shard order 3, by VALUE.
        assert overview["orders"] == [1] and overview["spacing"] == 2
        assert overview["all_time"] is False
        assert overview["fold_source"] == "cascade" and overview["exact_levels"] == 1
        # ...and the constant-depth member each declared order implies, also
        # by value: with s = 3, c = 6 the /2 spelling of this store's one
        # declared order is [{node: 1, cells: [4]}].
        assert (s, c) == (3, 6)
        assert [c - (s - k) for k in overview["orders"]] == [4]

    def test_manifest_envelope(self):
        manifest = json.loads((SPEC_DATA / PYRAMID / "morton_hive.json").read_text())
        assert manifest["spec"] == "morton-hive/1"
        assert manifest["shard_order"] == _expected(PYRAMID)["shard_order"]
        assert manifest["cell_order"] == _expected(PYRAMID)["cell_order"]


class TestMultiscalesMirror:
    """§4.9 — the ``multiscales`` discovery mirror (issue #392).

    Every assertion decodes the committed ``pyramid/`` manifest from spec
    text alone; expected values come from the generator's INPUTS.
    """

    def _mirror(self):
        manifest = json.loads((SPEC_DATA / PYRAMID / "morton_hive.json").read_text())
        return manifest["multiscales"]

    def test_marker_and_shape(self):
        # §4.9: a list of ONE multiscale object, spec-marked, name from the
        # manifest's dataset identity.
        mirror = self._mirror()
        assert mirror == _expected(PYRAMID)["multiscales"]
        (entry,) = mirror
        assert entry["spec"] == "zagg-multiscales/1"
        assert entry["name"] == "SPEC_FIXTURE"

    def test_datasets_project_the_pyramid_block(self):
        # §4.9: datasets mirror pyramid.overviews VERBATIM (finest first),
        # with the artifact kind by the node-order rule — "column" at the
        # shard order, "overview" above it. The pyramid block wins on any
        # disagreement, so agreement here is the conformance claim.
        exp = _expected(PYRAMID)
        (entry,) = self._mirror()
        levels = _pyramid_block()["overviews"]
        assert [{"order": e["node"], "cells": e["cells"]} for e in levels] == [
            {"order": d["order"], "cells": d["cells"]} for d in entry["datasets"]
        ]
        for d in entry["datasets"]:
            assert d["artifact"] == ("column" if d["order"] == exp["shard_order"] else "overview")

    def test_order2res_is_the_flat_per_order_lookup(self):
        # §4.9: string keys (JSON has no int keys), key set exactly the
        # orders present, values agreeing with datasets entry for entry.
        (entry,) = self._mirror()
        assert entry["order2res"] == {str(d["order"]): d["cells"] for d in entry["datasets"]}

    def test_base_is_the_native_resolution_never_a_dataset(self):
        # §4.9: the native source data is the base key — not a datasets
        # entry (a member at the base data's own order would BE the data).
        exp = _expected(PYRAMID)
        (entry,) = self._mirror()
        assert entry["base"] == {"order": exp["shard_order"], "cells": [exp["cell_order"]]}
        # membership, not whole-list inequality: the native resolution is not
        # a MEMBER of any level entry's cells — least of all the leaf entry's,
        # which shares the base's order key in order2res (§4.9's union rule).
        for d in entry["datasets"]:
            assert exp["cell_order"] not in d["cells"]
        assert exp["cell_order"] not in entry["order2res"][str(exp["shard_order"])]

    def test_fields_and_fold_project_the_family_dict(self):
        # §4.9: {name: class} projected from the §4.5 D24 map; declared fold
        # provenance from the family dict (exact_levels present exactly when
        # declared there).
        (entry,) = self._mirror()
        overview = _pyramid_block()["overview"]
        assert entry["fields"] == {n: m["class"] for n, m in overview["fields"].items()}
        assert entry["fold"] == {
            "fold_source": overview["fold_source"],
            "exact_levels": overview["exact_levels"],
        }

    def test_absence_is_the_pre_convention_pin(self):
        # §4.9: the key is present exactly when the pyramid block is /2 —
        # column/ (a /2 store committed before §4.9) pins absence as the
        # legal pre-convention state, and raster_toc/ pins that /1 never
        # carries it.
        for name in ("column", "raster_toc"):
            manifest = json.loads((SPEC_DATA / name / "morton_hive.json").read_text())
            assert "multiscales" not in manifest


#: The §4.6 leaf-column fixture (issue #383): the ``minimal`` inputs plus an
#: explicit ``output.pyramid.overviews: 5`` knob, so the committed store holds
#: a leaf AND the column its worker wrote beside it.
COLUMN = "column"


def _column_dir(exp) -> Path:
    return (SPEC_DATA / COLUMN / exp["leaf"]).parent / exp["column"]["object"]


class TestColumnArtifact:
    """§4.6 leaf columns: the committed bytes against the spec grammar."""

    def test_basename_role_and_attrs_grammar(self):
        exp = _expected(COLUMN)
        col = exp["column"]
        # D23 window stem + the `.pyramid` marker; `all` = the unwindowed leaf.
        assert col["object"] == "all.pyramid.zarr"
        attrs = json.loads((_column_dir(exp) / "zarr.json").read_text())["attributes"]
        assert attrs["role"] == "column"
        assert attrs["zagg_column"] == col["zagg_column"]
        block = attrs["zagg_column"]
        assert block["spec"] == "zagg-column/1"
        assert block["node"] == exp["shard"] and block["order"] == exp["shard_order"]
        assert block["source_cell_order"] == exp["cell_order"]
        assert block["window"] == "all"
        # §4.6's decode-without-the-manifest keys on an approximate entry —
        # including ``overview_delta``, the budget this column's fold actually
        # compressed at (issue #424), which the committed bytes must carry for
        # a spec-and-fixtures reader to see it.
        entry = block["fields"]["h_tdigest"]
        assert entry["class"] == "approximate" and entry["method"] == "tdigest_kway"
        assert {"delta", "overview_delta", "dtype", "inner_shape"} <= set(entry)
        assert entry["overview_delta"] == exp["delta"]

    def test_stamp_is_present_and_field_pinned(self):
        exp = _expected(COLUMN)
        attrs = json.loads((_column_dir(exp) / "zarr.json").read_text())["attributes"]
        stamp = attrs["morton_hive_commit"]
        got = {k: v for k, v in stamp.items() if k != "written_at"}
        assert got == exp["column"]["commit"]
        # cells_with_data counts the FINEST group (its order is named in attrs).
        assert attrs["zagg_column"]["cells_with_data_order"] == 5
        # Leaf cells 0, 2, 5, 15 (counts 40, 1, 5, 300) fold to res-5 rows
        # [41, 5, 0, 300] — 0 and 2 share row 0 — so three rows are populated.
        assert stamp["cells_with_data"] == 3

    def test_group_set_and_provenance_slots(self):
        exp = _expected(COLUMN)
        groups = exp["column"]["zagg_column"]["groups"]
        # Declared base (5) + the node-order member (4); nothing else on this
        # geometry (no interior ladder rung between them).
        assert sorted(groups, key=int, reverse=True) == ["5", "4"]
        assert groups["5"]["n_cells"] == 4 and groups["4"]["n_cells"] == 1
        for entry in groups.values():
            assert entry["regime"] == "leaf-column"
            assert entry["merges_from_raw"] == 1
            assert "source_children" not in entry  # rides cascade only (§4.3)

    def test_groups_decode_and_match_expected(self):
        exp = _expected(COLUMN)
        store = LocalStore(str(_column_dir(exp)))
        for res, want in exp["column"]["groups"].items():
            group = zarr.open_group(store, path=res, mode="r", zarr_format=3)
            assert [str(w) for w in group["morton"][:]] == want["morton"]
            assert [int(c) for c in group["count"][:]] == want["count"]
            for payload, rows in zip(group["h_tdigest"][:], want["h_tdigest"], strict=True):
                got = np.frombuffer(bytes(payload) if payload is not None else b"", "<f4").reshape(
                    -1, 2
                )
                np.testing.assert_array_equal(got, np.array(rows, "<f4").reshape(-1, 2))

    def test_base_group_is_the_from_leaves_fold(self):
        exp = _expected(COLUMN)
        # The §4.6 parity contract: column bytes == the sweep-kernel fold of
        # the COMMITTED leaf at the same resolution (merges-from-raw 1).
        from zagg.sweep_overview import decode_digest, fold_dense, fold_digests

        leaf = zarr.open_group(_leaf_store(COLUMN, exp), path=exp["group"], mode="r", zarr_format=3)
        store = LocalStore(str(_column_dir(exp)))
        factor = 4 ** (exp["cell_order"] - 5)
        base = zarr.open_group(store, path="5", mode="r", zarr_format=3)
        np.testing.assert_array_equal(
            base["count"][:], fold_dense(leaf["count"][:], factor, "sum", 0)
        )
        payloads = leaf["h_tdigest"][:]
        for j, stored in enumerate(base["h_tdigest"][:]):
            cell = [
                decode_digest(bytes(p), "float32", (2,))
                for p in payloads[j * factor : (j + 1) * factor]
                if p is not None and len(p)
            ]
            want = fold_digests(cell, delta=exp["delta"], dtype="float32") if cell else b""
            assert bytes(stored) == want

    def test_node_member_is_the_whole_footprint_aggregate(self):
        exp = _expected(COLUMN)
        store = LocalStore(str(_column_dir(exp)))
        node = zarr.open_group(store, path="4", mode="r", zarr_format=3)
        leaf_counts = [c["count"] for c in exp["cells"]]
        assert node["count"].shape == (1,)
        assert int(node["count"][0]) == sum(leaf_counts)
        # One cell, one word: the node's own morton word.
        assert node["morton"].shape == (1,)
        assert str(node["morton"][0]) == exp["column"]["groups"]["4"]["morton"][0]

    def test_sidecar_matches_the_recorded_hashes(self):
        exp = _expected(COLUMN)
        # Recomputed LIVE over the committed column (the multi-group
        # ``{order}/{field}`` key shape no leaf fixture exercises) — comparing
        # the two committed files to each other would only catch a botched
        # regeneration, since one generator run wrote both.
        node_dir = (SPEC_DATA / COLUMN / exp["leaf"]).parent
        record = json.loads((node_dir / "all.pyramid.stats.json").read_text())
        arrays = TestContentHashes._hash_leaf(_column_dir(exp))
        combined = hashlib.sha256("\n".join(sorted(arrays.values())).encode()).hexdigest()
        assert arrays == exp["column"]["content_hashes"]["arrays"]
        assert combined == exp["column"]["content_hashes"]["combined"]
        assert combined == FROZEN_COMBINED_COLUMN  # the recipe-drift pin (§5)
        assert record["content_hashes"] == exp["column"]["content_hashes"]
        assert record["cells_with_data"] == exp["column"]["commit"]["cells_with_data"]

    def test_manifest_declares_the_v2_schedule(self):
        exp = _expected(COLUMN)
        manifest = json.loads((SPEC_DATA / COLUMN / "morton_hive.json").read_text())
        block = manifest["pyramid"]
        assert block["spec"] == "zagg-pyramid/2"
        assert block["overviews"][0] == {"node": exp["shard_order"], "cells": [5]}
        assert exp["declared"] == {"overviews": 5}


class TestFixtureSemanticHash:
    """The committed ``semantic_hash`` must be reproducible from the config
    that built the fixture (issue #415).

    Nothing else in the suite reads that manifest field — the §5 hashes cover
    decoded values, not identity — so before this pin a semantic-core edit
    could ship fixtures carrying a digest no zagg reproduces, and moczarr
    would vendor them (espg/moczarr#19/#20) with every parity gate green. The
    D19 hash epoch is exactly the change that would have done it.
    """

    @staticmethod
    def _generator():
        """The fixture generator as a module (the ``test_content_hash`` precedent)."""
        import importlib.util

        path = Path(__file__).parent.parent / "tools" / "generate_spec_fixtures.py"
        spec = importlib.util.spec_from_file_location("zagg_spec_fixture_generator", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    @staticmethod
    def _recorded(name):
        return json.loads((SPEC_DATA / name / "morton_hive.json").read_text())["semantic_hash"]

    #: Every fixture this class recomputes, and how its config is rebuilt.
    #: ``test_every_fixture_is_covered`` refuses a fixture that is not here.
    COVERED = (
        "minimal",
        "kitchen_sink",
        "column",
        "flux",
        PYRAMID,
        "raster_toc",
        "temporal",
        "demoted",
    )

    @pytest.mark.parametrize(
        "name,kwargs",
        [
            ("minimal", {"kitchen_sink": False}),
            ("kitchen_sink", {"kitchen_sink": True}),
            ("column", {"kitchen_sink": False, "pyramid": {"overviews": 5}}),
            # The §2.0 flux fixture (issue #424) landed after the epoch's
            # phase 3, so its committed digest was pre-epoch until now.
            ("flux", {"kitchen_sink": False, "flux": True}),
            # The §4.3 demotions fixture (issue #518) is the kitchen-sink
            # store under a hand-mangled MANIFEST pyramid block — the config
            # (and so the identity) is kitchen_sink's own.
            ("demoted", {"kitchen_sink": True}),
        ],
    )
    def test_leaf_fixture_hash_is_reproducible(self, name, kwargs):
        from zagg.semantics import semantic_hash

        gen = self._generator()
        assert self._recorded(name) == semantic_hash(gen._config(**kwargs))

    def test_the_raster_toc_fixture_hash_is_reproducible(self):
        # The §8 fixture (issue #443) is built from its own config literal
        # rather than `_config`, so the builder hands it over through
        # `_raster_toc_config`. It also pins the one INCLUDED output key that
        # is not leaf-shaping: `time_encoding: toc` is in the core, so this
        # digest is not the same product as the default-encoding raster.
        from zagg.semantics import semantic_hash

        gen = self._generator()
        assert self._recorded("raster_toc") == semantic_hash(gen._raster_toc_config())

    def test_the_temporal_fixture_hash_is_reproducible(self):
        # The §8.2/§8.3 fixture (issue #410) merged in carrying a pre-epoch
        # digest — the third fixture this gate has caught (after `flux/` and
        # `raster_toc/`) — and was regenerated at the sync. Its config is its
        # own literal too, handed over through `_temporal_config`.
        from zagg.semantics import semantic_hash

        gen = self._generator()
        assert self._recorded("temporal") == semantic_hash(gen._temporal_config())

    def test_every_fixture_is_covered(self):
        # The gate on the gate: a fixture added without a hash pin ships a
        # digest nothing reproduces, which is exactly how `flux/` and
        # `raster_toc/` came to carry pre-epoch values while the suite stayed
        # green. Adding a fixture now fails here until it is pinned above.
        recorded = {
            p.parent.relative_to(SPEC_DATA).as_posix()
            for p in SPEC_DATA.glob("**/morton_hive.json")
            if json.loads(p.read_text()).get("semantic_hash")
        }
        assert recorded == set(self.COVERED)

    def test_the_pyramid_fixture_hash_is_reproducible(self):
        # Its manifest is built from the /1 config and then retrofitted by
        # declare_pyramid under the /2 one, so the recorded hash pins BOTH
        # halves at once: the digest reproduces, and the two configs agree —
        # which is the property that keeps the retrofit legal (the pyramid
        # block is deliberately outside the semantic core, issue #415).
        from zagg.semantics import semantic_hash

        gen = self._generator()
        cfg_v1 = gen._config(False)
        cfg_v2 = gen._config(False, pyramid=gen.PYRAMID_KNOB)
        for cfg in (cfg_v1, cfg_v2):
            cfg.aggregation["variables"].update(gen.PYRAMID_EXTRA_VARIABLES)
            cfg.output["grid"] = dict(gen.PYRAMID_GRID)
        assert semantic_hash(cfg_v1) == semantic_hash(cfg_v2)
        assert self._recorded(PYRAMID) == semantic_hash(cfg_v1)


class TestFixtureGranuleIdentity:
    """The OTHER half of the identity pair: each leaf's ``granules.json``
    sibling (issue #388) must self-pair and be recorded in the CANONICAL
    granule-id space (issue #415's granule ruling).

    Nothing else in the suite reads that object —
    :class:`TestFixtureSemanticHash` recomputes ``semantic_hash`` only — so the
    epoch's edits to the committed siblings were covered by no assertion at
    all. Both properties matter to a reader: a sibling whose digest does not
    pair with its own id list is rejected as ``unrecorded-ids``
    (``zagg.dedup._sibling_ids``), which silently disarms the contraction guard
    on that leaf; and an id list still in the pre-epoch href space would pair
    with nothing a post-epoch run plans.
    """

    #: Every committed sibling, enumerated over the tree the way
    #: ``test_every_fixture_is_covered`` is, so a new fixture cannot ship one
    #: unguarded.
    SIBLINGS = sorted(
        p.relative_to(SPEC_DATA).as_posix() for p in SPEC_DATA.glob("**/granules.json")
    )

    def test_every_leaf_fixture_carries_the_sibling(self):
        # The gate on the gate, on ``test_every_fixture_is_covered``'s pattern:
        # a fixture with a leaf but no recorded id list ships a leaf the
        # contraction guard cannot protect. ``pyramid/`` is manifest-only (no
        # leaf, hence no sibling), which is why this keys on the leaf.
        want = {
            f"{name}/{Path(_expected(name)['leaf']).parent.as_posix()}/granules.json"
            for name in TestFixtureSemanticHash.COVERED
            if _expected(name).get("leaf")
        }
        assert set(self.SIBLINGS) == want

    @pytest.mark.parametrize("rel", SIBLINGS)
    def test_the_sibling_self_pairs_on_canonical_ids(self, rel):
        from zagg.telemetry import GRANULE_IDS_SPEC, canonical_granule_ids, granules_sha256

        sibling = json.loads((SPEC_DATA / rel).read_text())
        ids = sibling["granule_ids"]
        assert sibling["spec"] == GRANULE_IDS_SPEC
        assert granules_sha256(ids) == sibling["granules_sha256"]
        # Already canonical, and sorted as the writer records them: the epoch is
        # a fixed point on these ids, so re-canonicalizing moves nothing.
        assert canonical_granule_ids(ids) == ids == sorted(ids)


class TestTemporalDeclaration:
    """§8 — the temporal declaration, pinned on the committed `raster_toc/`.

    The absent-declaration ⇒ legacy side needs no new fixture: the four
    digest fixtures carry no ``temporal`` key anywhere, which is the pin.
    """

    def _time_meta(self):
        exp = _expected("raster_toc")
        meta = json.loads(
            (_leaf_dir("raster_toc", exp) / exp["group"] / "time" / "zarr.json").read_text()
        )
        return exp, meta

    def test_declaration_grammar_and_dtype(self):
        exp, meta = self._time_meta()
        attrs = meta["attributes"]
        assert attrs == exp["time_attrs"]
        # §8: exactly the #410-ruled {spec, shape, grammar revision} -- the
        # committed bytes carry NO per-store epoch, timescale, or quantum
        # keys, and the grammar is a {name}/{major} revision token, never a
        # documentation URL or a release stamp.
        assert attrs["temporal"] == {
            "spec": "zagg-toc/1",
            "shape": "coordinate",
            "grammar": "mortie-toc/1",
        }
        # §8.1: uint64 words, one per timestep, no CF pair to mislead a
        # units/calendar-decoding client.
        assert meta["data_type"] == "uint64"
        assert set(attrs) == {"temporal"}
        assert meta["shape"] == [len(exp["time_words"])]

    def test_stored_words_match_the_golden(self):
        exp = _expected("raster_toc")
        store = _leaf_store("raster_toc", exp)
        words = zarr.open_array(
            store, path=f"{exp['group']}/time", zarr_format=3, consolidated=False
        )[:]
        assert words.dtype == np.uint64
        np.testing.assert_array_equal(words, np.array(exp["time_words"], dtype=np.uint64))
        # These particular words ascend, but that is INCIDENTAL to this
        # fixture: its three groups are days apart, so no envelope start can
        # lead the row before it. §8.1 does NOT promise ascending stored
        # words -- the span-lead counterexample is pinned by
        # test_raster_pipeline.py::TestTocTimeIndex::
        # test_a_leading_span_puts_the_stored_words_out_of_row_order.
        np.testing.assert_array_equal(np.sort(words), words)

    def test_decode_matches_the_golden_bounds(self):
        from zagg.readers import read_time_axis

        exp = _expected("raster_toc")
        lo, hi = read_time_axis(_leaf_store("raster_toc", exp), exp["group"])
        want = np.array(exp["time_bounds_ns"], dtype="int64")
        np.testing.assert_array_equal(lo.astype("int64"), want[:, 0])
        np.testing.assert_array_equal(hi.astype("int64"), want[:, 1])

    def test_bounds_conservatively_contain_the_real_acquisitions(self):
        # The §8.1 honesty claim, on committed bytes: a range never narrows
        # its acquisition, and a single-instant acquisition never widens.
        exp = _expected("raster_toc")
        bounds = np.array(exp["time_bounds_ns"], dtype="int64")
        for (start_ns, end_ns), acq in zip(bounds, exp["acquisitions"], strict=True):
            real_lo = np.datetime64(acq["start"], "ns").astype("int64")
            real_hi = np.datetime64(acq["end"], "ns").astype("int64")
            assert start_ns <= real_lo
            if real_lo == real_hi:
                assert start_ns == end_ns == real_lo  # exact timestamp word
            else:
                assert end_ns > real_hi  # exclusive envelope end

    def test_both_word_variants_are_committed(self):
        from mortie import toc_is_range

        exp = _expected("raster_toc")
        words = np.array(exp["time_words"], dtype=np.uint64)
        kinds = np.asarray(toc_is_range(words), dtype=bool)
        assert kinds.any() and not kinds.all()

    def test_window_predicate_runs_on_the_stored_words(self):
        from zagg.readers import time_axis_overlaps

        exp = _expected("raster_toc")
        store = _leaf_store("raster_toc", exp)
        arr = zarr.open_array(store, path=f"{exp['group']}/time", zarr_format=3, consolidated=False)
        mask = time_axis_overlaps(
            arr[:], dict(arr.attrs), "2025-06-18T00:00:00", "2025-06-19T00:00:00"
        )
        np.testing.assert_array_equal(mask, [False, True, False])

    def test_bands_and_morton_decode(self):
        exp = _expected("raster_toc")
        store = _leaf_store("raster_toc", exp)
        for name, want in exp["bands"].items():
            got = zarr.open_array(
                store, path=f"{exp['group']}/{name}", zarr_format=3, consolidated=False
            )[:]
            assert got.shape == (len(exp["time_words"]), exp["cells_per_shard"])
            np.testing.assert_array_equal(got, np.array(want, dtype=got.dtype))
        morton = zarr.open_array(
            store, path=f"{exp['group']}/morton", zarr_format=3, consolidated=False
        )[:]
        np.testing.assert_array_equal(morton, np.array(exp["morton"], dtype=np.uint64))

    def test_absent_declaration_on_every_pre_section_8_fixture(self):
        from zagg.time_axis import temporal_declaration

        for name in (*FIXTURES, "flux"):
            exp = _expected(name)
            attrs = _array_meta(name, exp, "morton")["attributes"]
            assert temporal_declaration(attrs) is None

    def test_leaf_is_stamped_and_manifest_marked(self):
        exp = _expected("raster_toc")
        attrs = json.loads((_leaf_dir("raster_toc", exp) / "zarr.json").read_text())["attributes"]
        assert attrs["morton_hive_commit"]["complete"] is True
        manifest = json.loads((SPEC_DATA / "raster_toc" / "morton_hive.json").read_text())
        assert manifest["spec"] == "morton-hive/1"

    def test_frozen_digests_pin_the_recipe(self):
        exp = _expected("raster_toc")
        got = TestContentHashes._hash_leaf(_leaf_dir("raster_toc", exp))
        assert got == exp["content_hashes"]["arrays"]
        assert exp["content_hashes"]["combined"] == FROZEN_COMBINED["raster_toc"]
        assert got["6/time"] == FROZEN_ARRAYS[("raster_toc", "6/time")]
        combined = hashlib.sha256("\n".join(sorted(got.values())).encode()).hexdigest()
        assert combined == exp["content_hashes"]["combined"]


class TestTemporalCompanions:
    """§8.2/§8.3 — the temporal companions, pinned on the committed
    `temporal/` store.

    The absent-declaration side needs no new fixture: `minimal/`,
    `kitchen_sink/`, `column/` and `flux/` carry no `temporal` key on any
    array, which is the §8.4 schema-evolution pin.
    """

    def _attrs(self, field):
        exp = _expected("temporal")
        return exp, _array_meta("temporal", exp, field)["attributes"]

    def test_per_cell_declaration_and_dtype(self):
        exp, attrs = self._attrs("observed")
        assert attrs["temporal"] == exp["declarations"]["observed"]
        assert attrs["temporal"] == {
            "spec": "zagg-toc/1",
            "shape": "per-cell",
            "grammar": "mortie-toc/1",
        }
        meta = _array_meta("temporal", exp, "observed")
        # §8.2: dense uint64 on the cells axis, reserved fill 0, aligned with
        # `morton` — same shape, no sibling geometry of its own.
        assert meta["data_type"] == "uint64"
        assert meta["fill_value"] == 0
        assert meta["shape"] == _array_meta("temporal", exp, "morton")["shape"]

    def test_per_centroid_declaration_rides_the_sibling(self):
        exp, payload = self._attrs("h_tdigest")
        # The payload carries the BINDING and no declaration (§8.3) — and the
        # binding is a sibling key, never inside the versioned ragged block.
        assert payload["times"] == exp["times_binding"] == "h_tdigest_times"
        assert "times" not in payload["ragged"]
        assert "temporal" not in payload
        sibling = _array_meta("temporal", exp, payload["times"])["attributes"]
        assert sibling["temporal"] == exp["declarations"]["h_tdigest_times"]
        assert sibling["temporal"]["shape"] == "per-centroid"
        # A sibling carries the spec-owned declaration and nothing else.
        assert set(sibling) == {"ragged", "temporal"}

    def test_sibling_rows_align_with_the_payload(self):
        exp = _expected("temporal")
        store = _leaf_store("temporal", exp)
        for cell in exp["cells"]:
            digest = read_cell(store, f"{exp['group']}/h_tdigest", cell["index"])
            words = read_cell(store, f"{exp['group']}/h_tdigest_times", cell["index"])
            assert words.dtype == np.uint64
            assert words.shape == (digest.shape[0],)
            np.testing.assert_array_equal(words, np.array(cell["h_tdigest_times"], dtype=np.uint64))

    def test_stored_per_cell_words_match_the_golden(self):
        exp = _expected("temporal")
        store = _leaf_store("temporal", exp)
        observed = zarr.open_array(
            store, path=f"{exp['group']}/observed", zarr_format=3, consolidated=False
        )[:]
        assert observed.dtype == np.uint64
        populated = {cell["index"]: int(cell["observed"]) for cell in exp["cells"]}
        for index, word in enumerate(observed):
            # §8.2: 0 is reserved for a cell the writer never observed, and a
            # populated cell never stores it.
            assert int(word) == populated.get(index, 0)
            if index in populated:
                assert int(word) != 0

    def test_both_word_variants_are_committed_in_both_shapes(self):
        from mortie import toc_is_range

        exp = _expected("temporal")
        per_cell = np.array([c["observed"] for c in exp["cells"]], dtype=np.uint64)
        per_centroid = np.concatenate(
            [np.array(c["h_tdigest_times"], dtype=np.uint64) for c in exp["cells"]]
        )
        for words in (per_cell, per_centroid):
            kinds = np.asarray(toc_is_range(words), dtype=bool)
            assert kinds.any() and not kinds.all()

    def test_words_conservatively_contain_their_members(self):
        """§8.2/§8.3's whole claim, on committed bytes.

        A merged word's envelope contains every member instant; a
        single-member word is that instant exactly, never widened.
        """
        from mortie import to_datetime64, toc2time

        def bounds(words):
            lo, hi = toc2time(np.asarray(words, dtype=np.uint64))
            return (
                np.asarray(to_datetime64(lo)).astype("int64"),
                np.asarray(to_datetime64(hi)).astype("int64"),
            )

        exp = _expected("temporal")
        for cell in exp["cells"]:
            lo, hi = bounds(cell["h_tdigest_times"])
            spans = np.array(cell["centroid_spans_ns"], dtype="int64")
            assert (lo <= spans[:, 0]).all()
            single = spans[:, 0] == spans[:, 1]
            assert (hi[single] == spans[single, 1]).all()  # exact instants
            assert (hi[~single] > spans[~single, 1]).all()  # exclusive ends
            # The per-cell word is an envelope of the whole cell, and so of
            # every per-centroid envelope beneath it.
            cell_lo, cell_hi = bounds([cell["observed"]])
            obs = np.array(cell["obs_span_ns"], dtype="int64")
            assert cell_lo[0] <= obs[0]
            assert cell_hi[0] == obs[1] if obs[0] == obs[1] else cell_hi[0] > obs[1]
            assert cell_lo[0] <= lo.min() and cell_hi[0] >= hi.max()

    def test_window_predicate_runs_on_the_stored_words(self):
        # §8.2/§8.3: selection is the grammar's overlap predicate on the
        # stored words -- no decode, no sort, no bisection.
        from mortie import from_datetime64, toc_overlaps

        exp = _expected("temporal")
        cell = exp["cells"][0]
        obs = np.array(cell["obs_span_ns"], dtype="int64").astype("datetime64[ns]")
        lo, hi = (int(w) for w in from_datetime64(obs))
        words = np.array(cell["h_tdigest_times"], dtype=np.uint64)
        assert np.asarray(toc_overlaps(words, lo, hi + 1), dtype=bool).all()
        far = int(from_datetime64(np.array(["2099-01-01"], dtype="datetime64[ns]"))[0])
        assert not np.asarray(toc_overlaps(words, far, far + 10**9), dtype=bool).any()

    def test_the_production_kernel_reproduces_the_committed_words(self):
        """§7 parity: the aggregation kernel emits the committed bytes.

        The `temporal/` fixture was generated one PR ahead of the kernel, with
        its words computed in the generator and handed to the production write
        path. This drives the generator's OWN inputs through the production
        reducer instead (``build_tdigest(..., temporal=)`` and
        ``zagg.stats.toc.cell_envelope``) and asserts the result equals the
        committed expectations word for word — so a kernel change that moved a
        single word fails here rather than silently diverging from the spec
        bytes external readers decode against (issue #410, CLAUDE.md §4).

        The generator is imported rather than re-implemented: its clock, its
        RNG, and its cell plan are the inputs, so nothing here can drift from
        the fixture it checks.
        """
        import importlib.util

        from zagg.grids import HealpixGrid
        from zagg.grids.morton import morton_word
        from zagg.stats.tdigest import build_tdigest
        from zagg.stats.toc import cell_envelope
        from zagg.time_axis import observation_words

        root = Path(__file__).resolve().parent.parent
        spec = importlib.util.spec_from_file_location(
            "_spec_fixture_gen", root / "tools" / "generate_spec_fixtures.py"
        )
        gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen)

        exp = _expected("temporal")
        cfg = gen._temporal_config()
        grid = HealpixGrid(4, 6, layout="fullsphere", config=cfg, chunk_inner=5, sharded=True)
        children = grid.children(morton_word(gen.SHARD_KEY))
        rng = np.random.default_rng(410)
        # The fixture's clock is ns since the Unix epoch; express it as offsets
        # from a declared epoch so the words come from the PRODUCTION encoder.
        epoch = "2018-01-01T00:00:00"
        epoch_ns = np.datetime64(epoch, "ns").astype("int64")

        plan = [(0, 0, 40), (0, 2, 1), (1, 1, 5), (3, 3, 300)]
        assert len(plan) == len(exp["cells"])
        for ordinal, ((chunk, local, n), expected) in enumerate(zip(plan, exp["cells"])):
            cell_index = chunk * grid.cells_per_chunk + local
            assert cell_index == expected["index"]
            cell_word = int(children[cell_index])
            h = np.round(rng.normal(30.0, 5.0, n), 3).astype(np.float64)
            words = np.asarray(gen._point_words(grid, cell_word, n, rng))
            order = np.argsort(h, kind="stable")
            h, words = h[order], words[order]
            times_ns = gen._obs_times_ns(n, ordinal)
            if ordinal == gen.TEMPORAL_GAP_CELL:
                # The §10.5 gap cell's clock offset, taken from the generator
                # rather than restated, exactly as the rest of this loop is
                # (issue #489).
                times_ns = times_ns + gen._temporal_gap_offset_ns()
            toc = observation_words(
                (times_ns - epoch_ns) / 1e9, epoch=epoch, scale="gps", units="seconds"
            )
            digest, locs, per_centroid = build_tdigest(h, gen.DELTA, locations=words, temporal=toc)
            np.testing.assert_array_equal(
                locs, np.array(expected["h_tdigest_locations"], dtype=np.uint64)
            )
            np.testing.assert_array_equal(
                per_centroid, np.array(expected["h_tdigest_times"], dtype=np.uint64)
            )
            assert int(cell_envelope(toc)) == int(expected["observed"])
            np.testing.assert_allclose(
                digest, np.array(expected["h_tdigest"], dtype=np.float32), rtol=0, atol=0
            )

    def test_absent_declaration_on_every_pre_companion_fixture(self):
        from zagg.time_axis import temporal_declaration

        for name in (*FIXTURES, "flux"):
            exp = _expected(name)
            for field in ("morton", "count"):
                assert temporal_declaration(_array_meta(name, exp, field)["attributes"]) is None
        # ...and no payload array binds a temporal sibling.
        assert (
            "times" not in _array_meta("minimal", _expected("minimal"), "h_tdigest")["attributes"]
        )

    def test_both_companions_fold_into_the_leaf_column(self):
        """Both companions ride every §4.6 column group, per-centroid.

        espg-ruled 2026-08-17 (amending ruling 3 on issue #410): the temporal
        channel is **per-centroid at every level**, symmetric with located — so
        each column group carries the digest and BOTH siblings, and there is no
        per-cell overview companion anywhere in the tree. §8.4's shape-coarsening
        reduction stays licensed for producers that want it; zagg's digest
        pyramids do not use it.

        The dense per-cell field (``observed``, the §8.2 shape) is absent from
        every group on purpose: its fold law is the grammar's join over a cell
        group rather than its own reducer, so it is D24 class ``none`` and exists
        at native resolution only.
        """
        exp = _expected("temporal")
        column = _leaf_dir("temporal", exp).parent / "all.pyramid.zarr"
        groups = sorted(p.name for p in column.iterdir() if p.is_dir())
        assert groups
        for group in groups:
            arrays = sorted(p.name for p in (column / group).iterdir() if p.is_dir())
            assert arrays == [
                "count",
                "h_tdigest",
                "h_tdigest_locations",
                "h_tdigest_times",
                "morton",
            ], f"column group {group}"

    def test_the_columns_companions_are_row_aligned_and_declared(self):
        """§1.1 row alignment and the §8.3/§9 declarations, on committed bytes.

        The column is a *fold*, so its words are the merge's own output rather
        than the leaf's — this is the one place in the fixture set where a folded
        companion is pinned. Every populated payload row has exactly one word in
        each sibling, and each sibling declares the words IT holds.
        """
        from zagg.grids.base import located_declaration
        from zagg.time_axis import temporal_declaration

        exp = _expected("temporal")
        column = _leaf_dir("temporal", exp).parent / "all.pyramid.zarr"
        store = LocalStore(str(column))
        for group in sorted(p.name for p in column.iterdir() if p.is_dir()):
            payload = zarr.open_array(
                store, path=f"{group}/h_tdigest", zarr_format=3, consolidated=False
            )[:]
            sibs = {
                name: zarr.open_array(
                    store, path=f"{group}/{name}", zarr_format=3, consolidated=False
                )
                for name in ("h_tdigest_locations", "h_tdigest_times")
            }
            assert located_declaration(dict(sibs["h_tdigest_locations"].attrs))["shape"] == (
                "per-centroid"
            )
            assert temporal_declaration(dict(sibs["h_tdigest_times"].attrs)) == {
                "spec": "zagg-toc/1",
                "shape": "per-centroid",
                "grammar": "mortie-toc/1",
            }
            populated = 0
            for i, raw in enumerate(payload):
                rows = len(np.frombuffer(bytes(raw), "<f4")) // 2
                if not rows:
                    continue
                populated += 1
                for name, arr in sibs.items():
                    words = np.frombuffer(bytes(arr[:][i]), "<u8")
                    assert words.shape == (rows,), f"{group}/{name} row {i}"
                    # §8.2's reserved 0 never appears in a real word.
                    assert words.all()
            assert populated, f"column group {group} folded no populated cell"

    def test_the_columns_words_reproduce_the_leafs_cell_by_cell(self):
        """The fold identities, on the committed column bytes (§8.3, §9.1).

        The row-alignment test above cannot tell a correct fold from one that
        swapped the two siblings: the grammars accept each other's words, so a
        swap survives ``shape == (rows,)`` and a nonzero check, and the
        declarations it would contradict live in attrs, which do not move when
        the bytes do. So pin the VALUES — per column cell, against the leaf cells
        it folded:

        * the toc **cell envelope** over a column cell's words equals the
          envelope over every leaf instant under it. This is the identity
          :func:`zagg.stats.tdigest._centroid_envelopes` names as what survives
          an arbitrary fold tree (a per-centroid vector is indexed by the
          centroid partition, so the words themselves do not).
        * the located **hull** — ``mortie.common_ancestor`` over the column
          cell's words equals the hull over the leaf's, §9.1's containment claim
          read at cell granularity.

        This is the fixture set's only golden for a *merge*-produced companion,
        so these are the checks that make it load-bearing for moczarr.
        """
        from mortie import common_ancestor

        from zagg.stats.toc import cell_envelope

        exp = _expected("temporal")
        leaf_order = int(exp["group"])
        leaf_store = _leaf_store("temporal", exp)
        leaf = {
            name: zarr.open_array(
                leaf_store, path=f"{exp['group']}/{name}", zarr_format=3, consolidated=False
            )[:]
            for name in ("h_tdigest_locations", "h_tdigest_times")
        }
        column = _leaf_dir("temporal", exp).parent / "all.pyramid.zarr"
        store = LocalStore(str(column))
        reducers = {
            "h_tdigest_locations": lambda w: int(common_ancestor(w)),
            "h_tdigest_times": lambda w: int(cell_envelope(w)),
        }
        checked = 0
        for group in sorted(p.name for p in column.iterdir() if p.is_dir()):
            factor = 4 ** (leaf_order - int(group))
            for name, reduce in reducers.items():
                words = zarr.open_array(
                    store, path=f"{group}/{name}", zarr_format=3, consolidated=False
                )[:]
                for j, raw in enumerate(words):
                    got = np.frombuffer(bytes(raw), "<u8")
                    members = np.concatenate(
                        [
                            np.frombuffer(bytes(m), "<u8")
                            for m in leaf[name][j * factor : (j + 1) * factor]
                        ]
                    )
                    if not len(got):
                        assert not len(members), f"{group}/{name} cell {j} dropped its members"
                        continue
                    assert reduce(got) == reduce(members), f"{group}/{name} cell {j}"
                    checked += 1
        assert checked, "no populated column cell — the assertions would be vacuous"

    def test_leaf_is_stamped_and_manifest_marked(self):
        exp = _expected("temporal")
        attrs = json.loads((_leaf_dir("temporal", exp) / "zarr.json").read_text())["attributes"]
        assert attrs["morton_hive_commit"]["complete"] is True
        manifest = json.loads((SPEC_DATA / "temporal" / "morton_hive.json").read_text())
        assert manifest["spec"] == "morton-hive/1"

    def test_frozen_digests_pin_the_recipe(self):
        exp = _expected("temporal")
        got = TestContentHashes._hash_leaf(_leaf_dir("temporal", exp))
        assert got == exp["content_hashes"]["arrays"]
        assert exp["content_hashes"]["combined"] == FROZEN_COMBINED["temporal"]
        for field in ("6/observed", "6/h_tdigest_times"):
            assert got[field] == FROZEN_ARRAYS[("temporal", field)]
        combined = hashlib.sha256("\n".join(sorted(got.values())).encode()).hexdigest()
        assert combined == exp["content_hashes"]["combined"]


class TestLocatedDeclaration:
    """§9 — the located declaration, pinned on the committed `temporal/`.

    The absent-key ⇒ §2.2 side is pinned by `kitchen_sink/`, which predates
    this revision and is deliberately not regenerated: its located siblings
    carry no `located` key, and a reader must read them as §2.2 verbatim
    rather than refuse them.
    """

    def test_declaration_rides_the_sibling_that_holds_the_words(self):
        exp = _expected("temporal")
        payload = _array_meta("temporal", exp, "h_tdigest")["attributes"]
        assert payload["ragged"]["locations"] == "h_tdigest_locations"
        assert "located" not in payload
        sibling = _array_meta("temporal", exp, "h_tdigest_locations")["attributes"]
        assert sibling["located"] == exp["declarations"]["h_tdigest_locations"]
        assert sibling["located"] == {
            "spec": "zagg-located/1",
            "shape": "per-centroid",
            "grammar": "mortie-morton/1",
        }
        assert set(sibling) == {"ragged", "located"}

    def test_declared_words_are_the_2_2_words(self):
        # §9 self-describes what §2.2 already pinned: the declaration changes
        # no byte of the channel it declares.
        from mortie import is_point, orders_of

        exp = _expected("temporal")
        store = _leaf_store("temporal", exp)
        # The shipping reader binds the sibling through the payload's attrs
        # (§1.2), so what §9 declares is what a reader actually returns.
        rows = list(read_locations(store, f"{exp['group']}/h_tdigest"))
        got = sorted(np.concatenate([locs for _w, _rc, locs in rows]).tolist())
        want = sorted(int(w) for c in exp["cells"] for w in c["h_tdigest_locations"])
        assert got == want
        merged_orders: list[int] = []
        for cell in exp["cells"]:
            digest = read_cell(store, f"{exp['group']}/h_tdigest", cell["index"])
            words = read_cell(store, f"{exp['group']}/h_tdigest_locations", cell["index"])
            assert words.dtype == np.uint64
            assert words.shape == (digest.shape[0],)
            np.testing.assert_array_equal(
                words, np.array(cell["h_tdigest_locations"], dtype=np.uint64)
            )
            # The split is the fixture's RECORDED member runs, never the
            # payload weight: §2.2/§9.1 key a word's claim on the word, and
            # under a "flux" payload (§2.0) a weight is not a member count.
            # These bytes give every observation its own instant, so a
            # single-instant span is a single-member run.
            spans = np.array(cell["centroid_spans_ns"], dtype="int64")
            unmerged = spans[:, 0] == spans[:, 1]
            points = np.asarray(is_point(words), dtype=bool)
            orders = np.asarray(orders_of(words))
            # An unmerged centroid keeps its observation's order-29 POINT
            # word; the merged rows are where coarser ancestors appear.
            assert unmerged.any()
            assert points[unmerged].all()
            assert (orders[unmerged] == 29).all()
            merged_orders.extend(int(o) for o in orders[~unmerged])
        # Heterogeneous orders in one committed array is the §9.1 claim a
        # reader must not assume away — decoded per word, never per array.
        assert merged_orders and min(merged_orders) < 29

    def test_absent_declaration_on_the_pre_section_9_fixture(self):
        from zagg.grids.base import located_declaration

        exp = _expected("kitchen_sink")
        for stratum in ("signal", "noise"):
            attrs = _array_meta("kitchen_sink", exp, f"h_tdigest_{stratum}_locations")["attributes"]
            assert "located" not in attrs
            assert located_declaration(attrs) is None

    def test_unknown_declaration_is_refused(self):
        from zagg.grids.base import located_declaration

        exp = _expected("temporal")
        block = dict(_array_meta("temporal", exp, "h_tdigest_locations")["attributes"]["located"])
        for bad in ({"spec": "zagg-located/2"}, {"shape": "per-cell"}, {"grammar": "geohash/1"}):
            with pytest.raises(ValueError):
                located_declaration({"located": {**block, **bad}})


class TestRootCoverageTemporalSection:
    """§10 — the ``zagg-coverage-toc/1`` section, on the committed sidecar.

    ``temporal/`` is the only fixture with a root coverage object at all:
    every other fixture store declares no temporal field, so a sweep of one
    produces no section. Their *lack* of the object is §10's absence rule
    pinned as bytes, asserted below.
    """

    def _envelope(self):
        return json.loads((SPEC_DATA / "temporal" / "coverage.moc").read_text())

    def test_the_carrier_still_reads_as_a_plain_root_moc(self):
        # §10 adds ONE key to the `morton-moc/1` carrier; a reader that knows
        # nothing about §10 must decode the object exactly as before.
        from zagg.coverage import load_coverage
        from zagg.grids.morton import morton_word
        from zagg.hive import root_coverage_words

        envelope = self._envelope()
        assert envelope["spec"] == "morton-moc/1"
        assert envelope["encoding"] == "ranges"
        assert envelope["ranges"] == [[_expected("temporal")["shard"]] * 2]
        assert load_coverage(str(SPEC_DATA / "temporal")) == envelope
        assert set(root_coverage_words(envelope)) == {
            int(morton_word(_expected("temporal")["shard"]))
        }

    def test_section_grammar(self):
        exp = _expected("temporal")["root_coverage"]
        section = self._envelope()["temporal"]
        assert section["spec"] == exp["spec"] == "zagg-coverage-toc/1"
        assert section["source"] == "sweep"
        assert section["fields"] == exp["fields"]
        # Words are DECIMAL STRINGS: a uint64 exceeds 2^53 and a float-based
        # JSON parser would mangle a raw number, exactly as for the ranges.
        assert section["shards"] == exp["shards"]
        assert all(isinstance(w, str) for w in section["shards"].values())
        digest = section["digest"]
        assert digest["element"] == {"dtype": "float32", "shape": [-1, 2]}
        assert (digest["encoding"], digest["weights"], digest["value"]) == (
            "base64",
            "counts",
            "toc-ns",
        )
        assert digest["delta"] == exp["digest"]["delta"]

    def test_digest_decodes_through_the_native_grammars(self):
        """§10.3: the payload is §2.1 bytes and the sibling is §8.3 words.

        Decoded here with the SPEC-TEXT recipe (base64, then the §1.4 raw
        little-endian buffer at the declared dtype) — no zagg decoder — so the
        "zero new grammar" claim is what is being asserted.
        """
        exp = _expected("temporal")["root_coverage"]["digest"]
        block = self._envelope()["temporal"]["digest"]
        payload = np.frombuffer(
            base64.b64decode(block["payload"]), dtype=np.dtype("float32").newbyteorder("<")
        ).reshape(-1, 2)
        words = np.frombuffer(
            base64.b64decode(block["times"]), dtype=np.dtype("uint64").newbyteorder("<")
        )
        assert len(payload) == len(words) == block["centroids"]
        np.testing.assert_array_equal(payload, np.array(exp["centroids"], dtype=np.float32))
        np.testing.assert_array_equal(words, np.array(exp["times"], dtype=np.uint64))
        # §2.1: rows ascend by mean, and every weight is a positive count.
        assert (np.diff(payload[:, 0]) >= 0).all()
        assert (payload[:, 1] > 0).all()

    def test_weight_conservation(self):
        """§10.3: `sum(weights)` is the store's temporal observation count."""
        exp = _expected("temporal")
        block = self._envelope()["temporal"]["digest"]
        payload, _words = coverage_toc_digest(self._envelope())
        total = exp["root_coverage"]["obs_total"]
        assert total == sum(cell["count"] for cell in exp["cells"])
        assert float(payload[:, 1].sum()) == block["weight_total"] == float(total)

    def test_shard_word_conservatively_contains_every_instant(self):
        """§10.2's whole claim, on committed bytes.

        Every observation instant in the shard falls inside that shard's one
        envelope word, and the grammar's own overlap predicate says so for
        each instant without any decoding by the caller.
        """
        from mortie import from_datetime64, to_datetime64, toc2time, toc_overlaps

        exp = _expected("temporal")
        word = coverage_toc(self._envelope())[exp["shard"]]
        lo, hi = (int(np.asarray(to_datetime64(b)).astype("int64")) for b in toc2time(word))
        instants = np.array(
            [int(t) for cell in exp["cells"] for t in cell["obs_span_ns"]], dtype="int64"
        )
        assert (lo <= instants).all() and (instants < hi).all()
        internal = np.asarray(from_datetime64(instants.astype("datetime64[ns]")), dtype=np.uint64)
        for t in internal:
            assert bool(np.asarray(toc_overlaps(np.array([word]), int(t), int(t) + 1))[0])

    def test_the_value_axis_is_the_envelope_midpoint(self):
        """§10.3: column 0 is derived from the words, not from the observations.

        Each contributing centroid enters the fold at the midpoint of its own
        §8.3 envelope, so a weight-1 centroid — whose word is a timestamp and
        whose ``toc2time`` envelope is a point — carries that EXACT instant,
        and every other mean is a convex combination of midpoints and so lies
        inside its own centroid's word. Both hold to the float32 quantum the
        section documents, which is why the words, never the means, are the
        exact temporal claim.
        """
        from mortie import toc2time

        payload, words = coverage_toc_digest(self._envelope())
        start, end = (np.asarray(b, dtype=np.float64) for b in toc2time(np.asarray(words)))
        means = payload[:, 0].astype(np.float64)
        # The one exact arm: weight-1 rows sit on their word's instant.
        single = payload[:, 1] == 1
        assert single.any()
        assert (start[single] == end[single]).all()
        np.testing.assert_array_equal(payload[single, 0], start[single].astype(np.float32))
        # Everything else: inside its own envelope, up to float32 rounding
        # (~2^-24 relative — roughly ten minutes at present-day magnitudes).
        quantum = np.abs(means) * 2.0**-23
        assert ((means >= start - quantum) & (means <= end + quantum)).all()

    def test_the_root_words_reduce_to_the_shard_word(self):
        # The tier-2 companion and the tier-1 map are two views of the same
        # join: reducing the digest's per-centroid envelopes reproduces the
        # shard's envelope word exactly (mortie's semilattice, spec §8.4).
        from mortie import toc_reduce

        exp = _expected("temporal")
        _payload, words = coverage_toc_digest(self._envelope())
        assert int(toc_reduce(words)) == coverage_toc(self._envelope())[exp["shard"]]

    def test_the_shard_word_is_the_join_of_the_committed_leaf_words(self):
        # Derived from the LEAF bytes, not from the sidecar: a writer that
        # folded the wrong thing fails here instead of agreeing with itself.
        from mortie import toc_reduce

        exp = _expected("temporal")
        leaf = np.concatenate(
            [np.array(cell["h_tdigest_times"], dtype=np.uint64) for cell in exp["cells"]]
        )
        assert int(toc_reduce(leaf)) == coverage_toc(self._envelope())[exp["shard"]]

    @pytest.mark.parametrize(
        "name", ["minimal", "kitchen_sink", "column", "flux", "raster_toc", "pyramid"]
    )
    def test_non_temporal_fixtures_carry_no_root_coverage_object(self, name):
        # §10's absence rule, pinned as bytes — WITH its precondition, which
        # is what makes the missing object evidence of the rule rather than
        # of the generator simply never having been pointed at these trees.
        # §10.5 extends the same pin to the word-set cover sibling.
        from zagg.coverage_toc import temporal_fields

        manifest = json.loads((SPEC_DATA / name / "morton_hive.json").read_text())
        assert temporal_fields(manifest) == {}
        assert not (SPEC_DATA / name / "coverage.moc").exists()
        assert not (SPEC_DATA / name / "coverage.toc").exists()

    def _cover(self):
        return json.loads((SPEC_DATA / "temporal" / "coverage.toc").read_text())

    def test_the_cover_sibling_decodes_to_the_expected_word_set(self):
        # §10.5 on committed bytes: the object gates on its own spec marker,
        # the carrier's section points at it, and the word set equals the
        # expectation DERIVED from the generator's inputs (never read back).
        from zagg.coverage_toc import COVER_KEY, cover_words, load_cover

        exp = _expected("temporal")["cover"]
        obj = self._cover()
        section = self._envelope()["temporal"]
        assert obj["spec"] == exp["spec"] == "zagg-coverage-toc-cover/1"
        assert section[COVER_KEY] == obj["spec"]
        assert obj["order"] == self._envelope()["order"]
        assert obj["temporal_order"] == exp["temporal_order"]
        loaded = load_cover(obj)
        assert loaded is not None
        words = cover_words(loaded)
        shard = _expected("temporal")["shard"]
        assert set(words) == {shard}
        assert [str(int(w)) for w in words[shard]] == exp["words"]
        # NOT one bucket that swallows the fixture: the committed cover is a
        # word SET, which is the only shape the parity and containment claims
        # below can discriminate (issue #489).
        assert len(words[shard]) == exp["count"] >= 2

    def test_the_cover_declares_the_10_5_grammar(self):
        # The keys §10.5 REQUIRES on the object, on committed bytes — an
        # external reader decodes the buffer with them: the cap the writer
        # coarsens against, the fields the words came from, the element kind
        # and its encoding, and each block's own `count`, which §10.5 makes a
        # MUST-check against the buffer's length.
        exp = _expected("temporal")["cover"]
        obj = self._cover()
        shard = _expected("temporal")["shard"]
        assert obj["cap"] == exp["cap"] == 512
        assert obj["fields"] == exp["fields"] == ["h_tdigest"]
        assert obj["element"] == exp["element"] == {"dtype": "uint64", "shape": [-1]}
        assert obj["encoding"] == exp["encoding"] == "base64"
        block = obj["shards"][shard]
        assert block["count"] == exp["count"]
        # Spec text only — no zagg decoder — exactly as §5's recipes are read.
        raw = base64.b64decode(block["words"])
        assert len(raw) == 8 * block["count"]
        assert [str(int(w)) for w in np.frombuffer(raw, "<u8")] == exp["words"]

    def test_the_cover_preserves_the_gap_between_the_clusters(self):
        # §10.5's never-bridge law on committed bytes. The fixture's last cell
        # sits whole buckets past the others, and `gap_ns` is the aligned
        # interval between them DERIVED from the member instants — a cover
        # that bridged it (or quantized everything into one bucket) answers
        # True here while still passing parity and containment (issue #489).
        from mortie import TOC_MAX_NS, toc_overlaps

        from zagg.coverage_toc import cover_words

        exp = _expected("temporal")
        words = np.asarray(cover_words(self._cover())[exp["shard"]], dtype=np.uint64)
        start, end = (int(v) for v in exp["cover"]["gap_ns"])
        assert start < end
        assert not bool(np.atleast_1d(toc_overlaps(words, start, end)).any())
        # The predicate is answering about THESE words, not False for
        # everything: every one of them hits a window over the whole scale.
        assert bool(np.atleast_1d(toc_overlaps(words, 0, TOC_MAX_NS)).all())

    def test_the_cover_satisfies_the_parity_invariant(self):
        # §10.5: toc_reduce(cover words) == toc_reduce(quantize(tier-1 word))
        # at the shard's effective order — the cross-object consistency claim.
        from mortie import toc_reduce

        from zagg.coverage_toc import TEMPORAL_COVER_ORDER, cover_words, quantize_words

        obj = self._cover()
        shard = _expected("temporal")["shard"]
        words = cover_words(obj)[shard]
        order = obj["shards"][shard].get("temporal_order", obj["temporal_order"])
        assert order == TEMPORAL_COVER_ORDER
        tier1 = coverage_toc(self._envelope())[shard]
        assert int(toc_reduce(words)) == int(toc_reduce(quantize_words([tier1], order)))

    def test_the_cover_conservatively_contains_every_member_instant(self):
        # §10.5's widening-only law, on the same derived instants §8.3's
        # containment is asserted from: every real member instant the
        # generator recorded (Unix-epoch ns, converted to the §8 internal
        # scale exactly as the other containment tests do) lies inside the
        # committed cover.
        from mortie import from_datetime64, toc2time

        from zagg.coverage_toc import cover_words

        exp = _expected("temporal")
        words = cover_words(self._cover())[exp["shard"]]
        start, end = toc2time(words)
        start, end = np.atleast_1d(start), np.atleast_1d(end)
        for cell in exp["cells"]:
            spans = np.array(cell["centroid_spans_ns"], dtype="int64")
            internal = np.asarray(
                from_datetime64(spans.reshape(-1).astype("datetime64[ns]")), dtype=np.uint64
            )
            for t in internal:
                assert bool(np.any((start.astype(np.uint64) <= t) & (t < end.astype(np.uint64))))


class TestDemotionAttrs:
    """§4.3 ``demotions``: the packed rail's record, on committed bytes (#518).

    The ``demoted/`` fixture (not in ``FIXTURES``: its expected record is the
    demotion surface, not the leaf suite's) is the kitchen-sink store swept
    under a ``/1`` manifest whose ``of`` divisor is mis-declared ``none`` —
    the exact-from-leaves level folds composition cleanly, the cascade above
    it cannot, and only the attrs distinguish the two all-fill readings.
    """

    def _overview(self, exp, which, path=""):
        store = LocalStore(str(SPEC_DATA / "demoted" / exp[which]["object"]))
        return zarr.open_group(store, path=path, mode="r", zarr_format=3)

    def test_the_demoted_level_records_the_rail(self):
        exp = _expected("demoted")
        block = dict(self._overview(exp, "demoted").attrs)["zagg_overview"]
        assert block["fold_source"] == "cascade"
        assert block["demotions"] == exp["demoted"]["demotions"]
        [record] = block["demotions"]
        # The record grammar: ``field``/``class``/``reason``/``contributors``
        # always, ``of`` naming the §3.3 linkage, ``cells`` whenever the
        # demotion left output cells at the fill word no surviving contributor
        # covers — the case here, since a cascade's children own disjoint
        # spans — and readers MUST tolerate additional keys (§4.3).
        assert record["field"] == "composition" and record["class"] == "packed"
        assert record["reason"] in ("word-missing", "divisor-missing")
        assert record["reason"] == "divisor-missing"
        assert record["contributors"] == 1
        assert record["of"] == "h_tdigest_signal"
        # The child's whole span, and the level is one child wide: 4 of the
        # node's 16 cells, matching the all-fill word slab below.
        assert record["cells"] == 4

    def test_the_clean_level_carries_no_demotions_key(self):
        # Absent, never empty: a level the rail did not fire at is
        # byte-identical to a pre-#518 writer's.
        exp = _expected("demoted")
        block = dict(self._overview(exp, "clean").attrs)["zagg_overview"]
        assert block["fold_source"] == "leaves"
        assert "demotions" not in block

    def test_the_bytes_alone_cannot_say_why(self):
        # The record's reason to exist: the divisor is absent from BOTH
        # levels (class ``none`` never materializes, §4.4), and the demoted
        # level's composition is all fill while the clean level's is
        # populated — indistinguishable from emptiness without the attrs.
        exp = _expected("demoted")
        clean = self._overview(exp, "clean", path="5")
        demoted = self._overview(exp, "demoted", path="4")
        assert "h_tdigest_signal" not in dict(clean.arrays())
        assert "h_tdigest_signal" not in dict(demoted.arrays())
        assert int(clean["composition"][:].astype("uint64").sum()) > 0
        assert int(demoted["composition"][:].astype("uint64").sum()) == 0
