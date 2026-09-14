"""Semantic-core hash canonicalization (issue #299 phase 1, D19 / §8.3).

The §8.3 obligations: syntactic edits (whitespace, key order, comments) never
change the hash; packaging-knob edits (orders, chunking, worker size, read
machinery, carrier) never change it; any semantic edit does.
"""

import copy

import pytest
import yaml

from zagg.config import PipelineConfig, default_config
from zagg.semantics import (
    canonical_semantic_json,
    semantic_core,
    semantic_fingerprint,
    semantic_hash,
)


def _cfg(**overrides) -> PipelineConfig:
    cfg = default_config("atl06")
    for dotted, value in overrides.items():
        parts = dotted.split("__")
        target = getattr(cfg, parts[0])
        for p in parts[1:-1]:
            target = target.setdefault(p, {})
        target[parts[-1]] = value
    return cfg


class TestCanonicalization:
    def test_hash_shape(self):
        h = semantic_hash(_cfg())
        assert len(h) == 64 and int(h, 16) >= 0
        assert semantic_fingerprint(h) == h[:12]

    def test_deterministic(self):
        assert semantic_hash(_cfg()) == semantic_hash(_cfg())

    def test_yaml_syntax_never_changes_hash(self):
        # Same semantics, different comments/whitespace/key order.
        a = yaml.safe_load(
            "data_source:\n"
            "  reader: h5coro\n"
            "  groups: [gt1l]\n"
            "  variables: {h_li: /p}\n"
            "  coordinates: {latitude: /lat, longitude: /lon}\n"
            "aggregation:\n"
            "  variables:\n"
            "    c: {function: len, source: h_li, dtype: int32}\n"
            "output:\n"
            "  grid: {type: healpix, parent_order: 6, child_order: 12}\n"
        )
        b = yaml.safe_load(
            "# a comment\n"
            "output:\n"
            "  grid:\n"
            "    child_order:   12\n"
            "    parent_order:  6\n"
            "    type: healpix   # trailing comment\n"
            "aggregation:\n"
            "  variables:\n"
            "    c:\n"
            "      dtype: int32\n"
            "      source: h_li\n"
            "      function: len\n"
            "data_source:\n"
            "  coordinates: {longitude: /lon, latitude: /lat}\n"
            "  variables: {h_li: /p}\n"
            "  groups: [gt1l]\n"
            "  reader: h5coro\n"
        )
        ca = PipelineConfig(**a)
        cb = PipelineConfig(**b)
        assert canonical_semantic_json(ca) == canonical_semantic_json(cb)
        assert semantic_hash(ca) == semantic_hash(cb)

    def test_packaging_knobs_never_change_hash(self):
        base = semantic_hash(_cfg())
        packaging = [
            # The D24 resolution axis, untouched by the issue #415 epoch:
            # hashing the orders would make o8 and o9 runs different products
            # and block mixed-order processing.
            _cfg(output__grid__parent_order=9),
            _cfg(output__grid__child_order=19),
            _cfg(output__grid__chunk_inner=11),
            _cfg(output__store_layout="hive"),
            _cfg(output__store="s3://elsewhere/prefix"),
            _cfg(output__product_name="renamed"),
            _cfg(output__consolidate_metadata=True),
            _cfg(output__coverage_moc=False),
            _cfg(output__sweep=False),
            # issue #501: output.touch declares a LIFECYCLE policy for the
            # destination -- when to refresh LastModified on a skip run. It
            # cannot change a byte of any leaf, so it must never move the
            # hash. This holds by CONSTRUCTION, not by an exclusion: `output`
            # is allowlist-shaped (OUTPUT_LEAF_SHAPING_KEYS + output.grid), so
            # a new key is out unless someone adds it. That is exactly why it
            # needs a regression guard -- the property is currently true only
            # because nobody added it to the allowlist.
            _cfg(output__touch="never"),
            _cfg(output__touch="always"),
            _cfg(aggregation__handoff="pandas"),
            _cfg(data_source__reader="xarray"),
            _cfg(data_source__driver="https"),
        ]
        for cfg in packaging:
            assert semantic_hash(cfg) == base
        # Worker sizing (issue #235) and read knobs are packaging too.
        cfg = _cfg()
        cfg.worker = {"memory": 8192, "extra_disk": True}
        assert semantic_hash(cfg) == base

    def test_streaming_block_is_packaging_in_every_spelling(self):
        # espg-ruled 2026-08-17 (the PR #475 D19 question, option (b)): the
        # whole aggregation.streaming block is packaging, on condition of the
        # law-equivalence contract (docs/design/sparse_coverage.md, D19) —
        # every streaming regime lands within the documented approximation
        # law of the pooled path, so all spellings share one identity. The
        # design record stated this twice while the code hashed the block as
        # spelled; this pin is the drift-proofing.
        base = semantic_hash(_cfg())
        spellings = [
            {},
            {"mode": "spill"},
            {"mode": "merge", "buffer_granules": 7},
            {"mode": "spill", "block_bytes": 1 << 26},
            {"buffer_granules": 20},
        ]
        for block in spellings:
            cfg = _cfg(aggregation__streaming=block)
            assert semantic_hash(cfg) == base
            assert "streaming" not in semantic_core(cfg)["aggregation"]

    def test_emit_cell_ids_is_packaging(self):
        # The issue #304 D16 transition hatch. It DOES add a `cell_ids` array
        # to every leaf, so it met the epoch's leaf-shaping criterion and was
        # briefly folded in — and espg ruled it back OUT (2026-08-17, PR #420
        # question (4)(b)) for the consequence the criterion does not price:
        # the hatch is scheduled for REMOVAL, after which a store built with it
        # ON would carry a digest no legal config can reproduce. A leaf's array
        # inventory is verified by READING the leaf (the same precedent that
        # keeps output.pyramid out), so the digest buys nothing here and costs a
        # permanently unverifiable identity.
        base = semantic_hash(_cfg())
        cfg = _cfg(output__grid__emit_cell_ids=True)
        assert semantic_hash(cfg) == base
        assert "emit_cell_ids" not in semantic_core(cfg)["grid"]

    def test_worker_fan_out_width_is_packaging(self):
        # The issue #415 epoch, half (7)(b): D19's ratified exclusion list
        # already named "worker size" as packaging, but the keys were never in
        # DATA_SOURCE_PACKAGING_KEYS — so the seams' fallback hash was
        # CLAMP-sensitive (PR #397 question (7)). Both spellings are excluded:
        # canonical shard_workers (issue #232) and legacy granule_workers.
        base = semantic_hash(_cfg())
        assert semantic_hash(_cfg(data_source__granule_workers=1)) == base
        assert semantic_hash(_cfg(data_source__shard_workers=8)) == base

    def test_dispatch_clamp_never_moves_the_hash(self):
        # The exact mechanism question (7) reported: the dispatcher hands each
        # cell a data_source clamped to min(K, n_granules) (issue #184), and
        # the worker-side fallback hashes THAT config. A 2-granule cell must
        # not hash differently from the run.
        from dataclasses import replace

        from zagg.runner import _clamped_data_source

        cfg = _cfg()
        clamped = _clamped_data_source(cfg.data_source, 2)
        assert clamped is not None and clamped["granule_workers"] == 2
        assert semantic_hash(replace(cfg, data_source=clamped)) == semantic_hash(cfg)

    def test_credentials_provider_is_packaging(self):
        # The issue #415 epoch's third passenger, espg-ruled 2026-08-17 (issue
        # #449): the provider selects HOW source bytes are fetched -- which
        # registry name mints the DAAC credentials the dispatcher attaches to
        # every event (issue #213 Phase 4) -- never WHAT is computed, the same
        # D19 class as reader/driver/read_plan, and the same class as the read
        # knobs and as `anonymous`, already excluded -- the same class, not one
        # knob under two names (`anonymous` is read only by the raster
        # source-store kwargs, this key only by the point/temporal paths).
        from zagg.semantics import DATA_SOURCE_PACKAGING_KEYS

        assert "credentials_provider" in DATA_SOURCE_PACKAGING_KEYS
        cfg = _cfg(data_source__credentials_provider="lpdaac")
        assert "credentials_provider" not in semantic_core(cfg)["data_source"]
        assert semantic_hash(cfg) == semantic_hash(_cfg())

    def test_a_credential_migration_never_rehashes(self):
        # The trap the ruling exists to close, and the reason it is ruled at
        # the epoch rather than later: the MERRA-2/gesdisc path moves an
        # existing store from one provider registration to another over
        # UNCHANGED data. Adding, dropping, or swapping the key must all be
        # one product -- otherwise every leaf reads `semantic-mismatch` and the
        # store rewrites itself to produce the same bytes.
        base = semantic_hash(_cfg())
        assert semantic_hash(_cfg(data_source__credentials_provider="gesdisc")) == base
        assert semantic_hash(_cfg(data_source__credentials_provider="lpdaac")) == semantic_hash(
            _cfg(data_source__credentials_provider="gesdisc")
        )
        # ...and a config carrying BOTH auth keys drops both. They are two
        # knobs of one class on disjoint paths -- `anonymous` is read only by
        # the raster source-store kwargs, the provider only by the point and
        # temporal paths -- so this pins canonicalization, not a run shape.
        both = _cfg(data_source__credentials_provider="gesdisc")
        both.data_source["anonymous"] = True
        assert semantic_hash(both) == base

    @pytest.mark.parametrize(
        "key,value",
        [("read_workers", 16), ("write_buffer", 4), ("source_region", "us-east-1")],
    )
    def test_byte_movement_knobs_are_packaging(self, key, value):
        # The epoch's fourth ruling (espg, 2026-08-17, PR #420 question (2)(c)):
        # the same class as credentials_provider -- each selects how bytes are
        # fetched or moved, never what is computed. read_workers (issue #170) is
        # the third fan-out width beside shard/granule_workers; write_buffer
        # bounds the live slabs under the streamed raster sink; source_region
        # sits in the SAME dict literal as the already-excluded `anonymous`
        # (runner's src_kwargs), so hashing one and not the other split one
        # decision across both sides of the D19 line.
        from zagg.semantics import DATA_SOURCE_PACKAGING_KEYS

        assert key in DATA_SOURCE_PACKAGING_KEYS
        cfg = _cfg(**{f"data_source__{key}": value})
        assert key not in semantic_core(cfg)["data_source"]
        assert semantic_hash(cfg) == semantic_hash(_cfg())

    @pytest.mark.parametrize(
        "key,before,after",
        [
            ("read_workers", 8, 2),
            ("write_buffer", 1, 8),
            ("source_region", "us-west-2", "us-east-1"),
        ],
    )
    def test_a_machinery_migration_never_rehashes(self, key, before, after):
        # The trap each exclusion closes, and the reason the epoch is the moment
        # to close it: retuning a pool width, a buffer bound, or a source region
        # over an EXISTING store must be one product. Otherwise every leaf reads
        # `semantic-mismatch` and the store rewrites itself to produce the same
        # bytes -- which is what espg measured on 2026-08-17 for the FAN-OUT
        # WIDTHS, when two GEDI flux builds of shard 5347294481781620745
        # produced identical total_obs (23,353,274) and cells_with_data (27,727)
        # in the exact single-block spill regime and still hashed apart on
        # read_workers and the two *_workers spellings. The measurement stops
        # there: write_buffer and source_region are read only on the raster path
        # and a flux build is the point path, so those two are excluded on the
        # argument (mechanism, loud failure, one dict literal with `anonymous`)
        # rather than on the anecdote -- which is why this pins all three.
        base = semantic_hash(_cfg())
        assert semantic_hash(_cfg(**{f"data_source__{key}": before})) == base
        assert semantic_hash(_cfg(**{f"data_source__{key}": after})) == base

    def test_semantic_edits_always_change_hash(self):
        base = semantic_hash(_cfg())
        cfg = _cfg()
        cfg.aggregation["variables"]["h_min"]["function"] = "max"
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.aggregation["variables"]["count"]["dtype"] = "int64"
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.aggregation["variables"]["extra"] = {
            "function": "len",
            "source": "h_li",
            "dtype": "int32",
        }
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["groups"] = ["gt1l"]
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["quality_filter"]["value"] = 1
        assert semantic_hash(cfg) != base
        cfg = _cfg()
        cfg.data_source["variables"]["h_li"] = "/other/path"
        assert semantic_hash(cfg) != base

    def test_grid_family_is_semantic(self):
        # Grid TYPE (+ indexing scheme) is identity; the orders are not (D24).
        # The one leaf-shaping grid knob rides resolved (issue #415 epoch);
        # `emit_cell_ids` is packaging by ruling, so it never appears.
        healpix = semantic_core(_cfg())
        assert healpix["grid"] == {
            "type": "healpix",
            "indexing_scheme": "nested",
            "sharded": True,
        }
        rect = default_config("atl06_polar")
        rect = copy.deepcopy(rect)
        rect.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        # F1: rect folds in the spatially-defining params (crs/resolution/
        # bounds) — D24's resolution-axis exclusion is HEALPix-only. The
        # sharded default is grid-family-shaped, exactly as grids.from_config
        # resolves it: HEALPix defaults on (issue #215), rect defaults off.
        assert semantic_core(rect)["grid"] == {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
            "sharded": False,
        }
        assert semantic_hash(rect) != semantic_hash(_cfg())

    def test_rect_resolution_changes_hash(self):
        # F1: two rect products differing only in resolution (or CRS) are
        # different products — they must not collide on semantic_hash.
        base = default_config("atl06_polar")
        base = copy.deepcopy(base)
        base.output["grid"] = {
            "type": "rectilinear",
            "crs": "EPSG:3031",
            "resolution": 100,
            "bounds": [0, 0, 1000, 1000],
        }
        finer = copy.deepcopy(base)
        finer.output["grid"]["resolution"] = 50
        assert semantic_hash(finer) != semantic_hash(base)
        other_crs = copy.deepcopy(base)
        other_crs.output["grid"]["crs"] = "EPSG:3413"
        assert semantic_hash(other_crs) != semantic_hash(base)

    def test_null_packaging_values_drop_out(self):
        # A present-but-null read knob (YAML `driver:`) is identical to an
        # absent one — the canonical core omits nulls.
        a, b = _cfg(), _cfg()
        b.data_source["read_plan"] = None
        assert semantic_hash(a) == semantic_hash(b)

    def test_nested_null_values_drop_out(self):
        # F2: an explicit null NESTED inside a semantic dict (YAML `value:`)
        # hashes identically to the key being absent — pruning is recursive.
        absent = _cfg()
        absent.data_source["quality_filter"] = {"dataset": "/q"}
        null_valued = _cfg()
        null_valued.data_source["quality_filter"] = {"dataset": "/q", "value": None}
        assert semantic_hash(null_valued) == semantic_hash(absent)
        # ...but a real value (0 included) is content, not an absent key.
        zero_valued = _cfg()
        zero_valued.data_source["quality_filter"] = {"dataset": "/q", "value": 0}
        assert semantic_hash(zero_valued) != semantic_hash(absent)

    def test_golden_hash_pin(self):
        # F4: a fully-inline config with fixed dicts, pinned to its exact
        # digest (re-pinned when pipeline.type joined the core, espg-ruled
        # 2026-07-21; re-pinned at the issue #415 hash epoch, when the
        # leaf-shaping output knobs joined; re-pinned once more when espg ruled
        # `emit_cell_ids` back out of the core, 2026-08-17). This catches
        # accidental
        # canonicalization drift — any change to key ordering, null pruning,
        # the packaging-key sets, or the JSON separators would move this hash,
        # so an unintended edit fails loudly. Moving it deliberately is a
        # compatibility event: every store's frozen manifest key changes with
        # it (see the migration note in docs/hive_layout.md).
        cfg = PipelineConfig(
            data_source={
                "reader": "h5coro",
                "groups": ["gt1l", "gt2l"],
                "variables": {"h_li": "/land_ice_segments/h_li"},
                "coordinates": {
                    "latitude": "/land_ice_segments/latitude",
                    "longitude": "/land_ice_segments/longitude",
                },
                "quality_filter": {"dataset": "/land_ice_segments/atl06_quality_summary"},
            },
            aggregation={
                "handoff": "arrow",
                "variables": {
                    "count": {"function": "len", "source": "h_li", "dtype": "int32"},
                    "h_mean": {"function": "mean", "source": "h_li", "dtype": "float32"},
                },
            },
            output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
        )
        assert semantic_hash(cfg) == (
            "fb15224f7265938d71039aeb6a0516f72e565609cd1993a25d41f7dc862a53a9"
        )

    def test_pipeline_type_is_semantic(self):
        # espg-ruled on the PR #316 review (2026-07-21): a temporal engine
        # over the same aggregation block is a DIFFERENT product. Absent
        # normalizes to the spatial default, so existing configs hash stably.
        base = _cfg()
        explicit = _cfg()
        explicit.pipeline = {"type": "spatial"}
        assert semantic_hash(base) == semantic_hash(explicit)
        temporal = _cfg()
        temporal.pipeline = {"type": "temporal"}
        assert semantic_hash(temporal) != semantic_hash(base)
        assert semantic_core(temporal)["pipeline"] == {"type": "temporal"}

    def test_fingerprint_rejects_short_input(self):
        with pytest.raises(ValueError, match="not a semantic hash"):
            semantic_fingerprint("abc")


class TestLeafShapingOutputKnobs:
    """The issue #415 hash epoch, half (8)(c): the ``output`` knobs that change
    what a leaf CONTAINS (or which artifacts sit beside it) are identity now.

    Before the epoch the whole ``output`` block was outside the core, so the
    issue #388 skip gate's identity pair could not see a leaf-shape change and
    a config-changed rerun read ``equal`` — PR #397 question (8).
    """

    WINDOWING = {"schedule": "annual", "time_field": "delta_time", "epoch": "2018-01-01"}
    TIME_SOURCE = {
        "field": "delta_time",
        "epoch": "2018-01-01T00:00:00",
        "scale": "gps",
        "units": "seconds",
    }

    def test_sharded_flip_is_identity(self):
        # The knob question (8) checked by hand: flipping it changes the
        # leaf's object set while the granule set holds.
        assert semantic_hash(_cfg(output__grid__sharded=False)) != semantic_hash(_cfg())

    def test_an_explicit_default_hashes_as_absence(self):
        # Resolved through the accessors, never as spelled: HEALPix output
        # defaults to sharded (issue #215), so spelling the default must not
        # mint a new product — the pipeline.type discipline (§8.3).
        base = semantic_hash(_cfg())
        assert semantic_hash(_cfg(output__grid__sharded=True)) == base
        assert semantic_hash(_cfg(output__aoi_mask=False)) == base
        assert semantic_hash(_cfg(output__windowing={"schedule": "none"})) == base

    def test_aoi_mask_is_identity(self):
        # Masks cells outside the AOI: the leaf's VALUES differ (issue #101).
        assert semantic_hash(_cfg(output__aoi_mask=True)) != semantic_hash(_cfg())

    def test_windowing_is_identity(self):
        # A windowed and an unwindowed store over the same granules shared a
        # hash before the epoch (issue #246, D13).
        assert semantic_hash(_cfg(output__windowing=self.WINDOWING)) != semantic_hash(_cfg())

    def test_windowing_is_folded_in_its_normalized_form(self):
        # get_windowing canonicalizes the epoch, so two spellings of the same
        # instant are one product (§8.3) — the same obligation the rest of the
        # core meets by pruning nulls.
        spelled = _cfg(output__windowing={**self.WINDOWING, "epoch": "2018-01-01T00:00:00Z"})
        assert semantic_hash(spelled) == semantic_hash(_cfg(output__windowing=self.WINDOWING))
        # ...and a real boundary change is a different product.
        moved = _cfg(output__windowing={**self.WINDOWING, "epoch": "2019-01-01"})
        assert semantic_hash(moved) != semantic_hash(_cfg(output__windowing=self.WINDOWING))

    def test_the_pyramid_block_stays_out(self):
        # Deliberately excluded even though the leaf column IS leaf-shaping:
        # the column half is verified by reading the artifact
        # (hive.leaf_column_expectation, the specification's §4.6 posture),
        # D11 keeps the block out of the manifest's frozen keys, and
        # sweep_overview.declare_pyramid exists to add a declaration to the
        # config that built the store — hashing it would refuse the retrofit
        # the tool is for.
        base = semantic_hash(_cfg())
        for knob in (
            False,
            {"overviews": [9]},
            {"spacing": 2},
            {"orders": [3]},
            {"all_time": True},
            {"summarize": {"h_li": {"as": "h_li_digest"}}},
        ):
            assert semantic_hash(_cfg(output__pyramid=knob)) == base

    def test_the_documented_constants_match_the_core(self):
        # The constants are the module's public statement of the epoch's
        # scope; this pins them to what semantic_core actually builds, so the
        # two cannot drift.
        from zagg.semantics import GRID_LEAF_SHAPING_KEYS, OUTPUT_LEAF_SHAPING_KEYS

        # Every key must be PRESENT in the built core for the comparison to
        # mean anything, and each is pruned when it resolves to None — so the
        # config has to declare all of them: a schedule for `windowing`, and a
        # temporal companion plus its clock for `time_source` (issue #410).
        core = semantic_core(
            _cfg(
                output__windowing=self.WINDOWING,
                output__time_source=self.TIME_SOURCE,
                aggregation__variables__h_mean__temporal="per-centroid",
            )
        )
        assert set(core["output"]) == set(OUTPUT_LEAF_SHAPING_KEYS)
        assert set(core["grid"]) - {"type", "indexing_scheme"} == set(GRID_LEAF_SHAPING_KEYS)

    @pytest.mark.parametrize(
        "block",
        [
            "yes",  # shape fault: not a mapping
            5,
            {"schedule": "annual"},  # grammar fault: no time_field/epoch
            {"schedule": "explicit", "windows": [{"label": "x"}]},
        ],
    )
    def test_the_core_stays_total_on_an_out_of_grammar_block(self, block):
        # validate_config refuses these by name; the hash must not be the
        # thing that raises — the Lambda worker builds its config without
        # validate_config, and semantic_core must not start raising on a
        # config that hashed before the epoch. Such a block hashes AS SPELLED,
        # and stably: the same bad block twice is the same digest, and it does
        # not collide with the unwindowed store.
        digest = semantic_hash(_cfg(output__windowing=block))
        assert len(digest) == 64
        assert digest == semantic_hash(_cfg(output__windowing=block))
        assert digest != semantic_hash(_cfg())


def _digest_cfg(**meta_extra) -> PipelineConfig:
    """A minimal config carrying one ragged digest field ``d``."""
    return PipelineConfig(
        data_source={
            "reader": "h5coro",
            "groups": ["g"],
            "variables": {"h": "/p"},
            "coordinates": {"latitude": "/lat", "longitude": "/lon"},
        },
        aggregation={
            "variables": {
                "d": {
                    "kind": "ragged",
                    "function": "zagg.stats.tdigest.build_tdigest",
                    "source": "h",
                    "inner_shape": [2],
                    "dtype": "float32",
                    "params": {"delta": 8192},
                    **meta_extra,
                }
            }
        },
        output={"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}},
    )


class TestWeightsAndOverviewDeltaHashing:
    """Issue #424: default weights and overview_delta never move a hash."""

    def test_weights_counts_hashes_as_absent(self):
        # The §2.0 absent-key default: explicit and absent spellings are the
        # same bytes, so they must be the same product identity.
        assert semantic_hash(_digest_cfg(weights="counts")) == semantic_hash(_digest_cfg())

    def test_weights_flux_is_semantic(self):
        gain = {"gain": {"name": "g", "version": "1"}}
        base = _digest_cfg(attrs=dict(gain))
        flux = _digest_cfg(weights="flux", attrs=dict(gain))
        assert semantic_hash(flux) != semantic_hash(base)
        assert semantic_core(flux)["aggregation"]["variables"]["d"]["weights"] == "flux"

    def test_overview_delta_is_packaging(self):
        # The pyramid-fold budget shapes overview artifacts only; the base
        # product identity must not move when it is declared (issue #424).
        assert semantic_hash(_digest_cfg(overview_delta=512)) == semantic_hash(_digest_cfg())
        core = semantic_core(_digest_cfg(overview_delta=512))
        assert "overview_delta" not in core["aggregation"]["variables"]["d"]


class TestTimeEncodingHashing:
    """Issue #443: the §8 time-coordinate encoding is output-defining."""

    def _raster_cfg(self, encoding=None) -> PipelineConfig:
        output = {"grid": {"type": "healpix", "parent_order": 10, "child_order": 16}}
        if encoding:
            output["time_encoding"] = encoding
        return PipelineConfig(
            data_source={"reader": "raster", "bands": {"red": {"asset": "red", "dtype": "uint16"}}},
            output=output,
        )

    def test_default_encoding_hashes_as_absent(self):
        # The §8 absent-key default: an explicit `microseconds` is the same
        # stored axis, so it must be the same product identity — which is
        # also what keeps every pre-#443 config's hash byte-identical.
        assert semantic_hash(self._raster_cfg("microseconds")) == semantic_hash(self._raster_cfg())
        assert "time_encoding" not in semantic_core(self._raster_cfg("microseconds"))

    def test_toc_is_semantic(self):
        toc = self._raster_cfg("toc")
        assert semantic_hash(toc) != semantic_hash(self._raster_cfg())
        assert semantic_core(toc)["time_encoding"] == "toc"

    def test_packaged_s2_config_declares_toc(self):
        cfg = default_config("sentinel2_l2a")
        assert semantic_core(cfg)["time_encoding"] == "toc"


class TestTimeSourceHashing:
    """Issue #410: the per-observation clock is output-defining.

    A temporal companion's stored toc words ARE the declared column converted
    from the declared epoch on the declared scale, so ``output.time_source`` is
    output-defining in the strongest sense — more directly than
    ``time_encoding``, which only changes how one axis is spelled.

    It composes into the **merged** issue #415 hash epoch (PR #420) rather than
    standing beside it: the clock is one of that epoch's leaf-shaping ``output``
    knobs, folded resolved through its accessor under ``core["output"]``, which
    is the same discipline ``aoi_mask`` and ``windowing`` follow there.
    """

    def _cfg(self, temporal=None, clock=None, windowing=None) -> PipelineConfig:
        meta = {
            "kind": "ragged",
            "function": "zagg.stats.tdigest.build_tdigest",
            "inner_shape": [2],
            "dtype": "float32",
            "source": "h_ph",
        }
        if temporal:
            meta["temporal"] = temporal
        output: dict = {"grid": {"type": "healpix", "parent_order": 6, "child_order": 12}}
        if clock:
            output["time_source"] = clock
        if windowing:
            output["windowing"] = windowing
        return PipelineConfig(
            data_source={"variables": {"h_ph": "/h_ph", "delta_time": "/delta_time"}},
            aggregation={"coordinates": {}, "variables": {"h_tdigest": meta}},
            output=output,
        )

    CLOCK = {
        "field": "delta_time",
        "epoch": "2018-01-01T00:00:00",
        "scale": "gps",
        "units": "seconds",
    }

    def test_it_composes_into_the_epochs_output_block(self):
        # Placement, pinned: `time_source` is one of the issue #415 epoch's
        # leaf-shaping `output` knobs (OUTPUT_LEAF_SHAPING_KEYS), not a
        # top-level core key. It sits there and `time_encoding` does not
        # because no store carries a temporal companion yet, so placing this
        # one coherently costs nothing, where relocating `time_encoding` would
        # move digests that already exist (the shipped S2 config declares toc).
        from zagg.semantics import OUTPUT_LEAF_SHAPING_KEYS

        assert "time_source" in OUTPUT_LEAF_SHAPING_KEYS
        core = semantic_core(self._cfg("per-centroid", self.CLOCK))
        assert core["output"]["time_source"] == self.CLOCK
        assert "time_source" not in core

    def test_a_clock_no_field_consumes_does_not_move_the_hash(self):
        # Declaring a clock that nothing reads moves no store byte, so it must
        # not move the product identity either — the same absent-key discipline
        # `time_encoding: microseconds` gets.
        assert semantic_hash(self._cfg(clock=self.CLOCK)) == semantic_hash(self._cfg())
        assert "time_source" not in semantic_core(self._cfg(clock=self.CLOCK))["output"]

    def test_the_clock_is_semantic_once_a_companion_declares(self):
        declared = self._cfg("per-centroid", self.CLOCK)
        assert semantic_core(declared)["output"]["time_source"] == self.CLOCK
        assert semantic_hash(declared) != semantic_hash(self._cfg("per-centroid"))

    def test_a_different_epoch_is_a_different_product(self):
        # The failure this exists to refuse: a re-run under a corrected epoch
        # writes different words for the same observations, so it must not reuse
        # the identity of the store it contradicts.
        moved = dict(self.CLOCK, epoch="2019-01-01T00:00:00")
        assert semantic_hash(self._cfg("per-centroid", moved)) != semantic_hash(
            self._cfg("per-centroid", self.CLOCK)
        )

    def test_scale_and_units_are_semantic_too(self):
        base = semantic_hash(self._cfg("per-centroid", self.CLOCK))
        for changed in (dict(self.CLOCK, scale="tai"), dict(self.CLOCK, units="days")):
            assert semantic_hash(self._cfg("per-centroid", changed)) != base

    def test_recorded_resolved_so_the_windowing_fallback_is_one_product(self):
        # `toc_source` falls back to a continuous-scale `output.windowing`, so
        # the explicit block and the fallback are two spellings of ONE clock and
        # must hash identically. Recording the RESOLVED value is what makes that
        # true rather than accidental.
        windowed = self._cfg(
            "per-centroid",
            windowing={
                "schedule": "yearly",
                "time_field": "delta_time",
                "epoch": "2018-01-01T00:00:00",
                "scale": "gps",
            },
        )
        clock = semantic_core(windowed)["output"]["time_source"]
        assert clock["field"] == "delta_time" and clock["scale"] == "gps"

    def test_configs_declaring_no_companion_hash_unchanged(self):
        # A packaged config that declares no companion must not gain the key —
        # that is what keeps every store built before #410 verifiable.
        for name in ("atl06", "atl03_tdigest_healpix", "atl06_polar", "sentinel2_l2a"):
            assert "time_source" not in semantic_core(default_config(name)).get("output", {})

    def test_the_shipped_companion_templates_carry_their_clock(self):
        # ...and the two that DO declare one carry it, resolved. These are the
        # products whose identity must be born with the clock in it (the
        # California rebuild), so the key is asserted present rather than merely
        # allowed.
        for name in ("atl03_tdigest_located_healpix", "gedi01b_waveform_healpix_hive"):
            clock = semantic_core(default_config(name))["output"]["time_source"]
            assert clock["field"] == "delta_time" and clock["scale"] == "gps"


class TestGediTemplateFluxDeclaration:
    """The packaged GEDI template's §2.0 flux declaration is a deliberate
    identity flip (issue #424; espg-ruled 2026-08-24).

    The rx_flux weight column has ALWAYS been (count - noise_mean) * gain
    floats; what changed is that the template now says so (``weights: flux``
    plus the required gain-provenance mapping). ``weights: "flux"`` is
    output-defining, so the template's semantic hash moves — both forms are
    pinned as constants so the flip stays visible and cannot recur silently.
    Stores built from the prior counts-defaulting form (the SERC flux store
    and probes) keep THEIR configs and THEIR hash; only new builds declare.
    """

    # The shipped template, weights: flux + attrs.gain mapping (this change).
    FLUX_HASH = "7e0bf1818efa9259194bb43d0d1eaf7f23b5d9122d00b14797e28f1a5d9d02dd"
    # The prior counts-defaulting form: no weights key, flat gain_name /
    # gain_version / gain attrs. This is what the SERC stores were built from,
    # but `_prior_form()` only RECONSTRUCTS it — from HEAD's template, so it
    # tracks "this template minus this PR's flip", not a frozen snapshot. The
    # LITERAL below is the freeze: a later semantic edit to the template (a
    # `delta` change, a filter, `time_source`) moves the reconstruction, this
    # constant fails, and whoever makes the edit has to decide deliberately
    # rather than drift past it.
    COUNTS_HASH = "c812c910f86057f9899aef3128a685d2ef78c5cbe26e7c90ae044f4d77feb4bd"

    def _prior_form(self) -> PipelineConfig:
        cfg = default_config("gedi01b_waveform_healpix_hive")
        rx = cfg.aggregation["variables"]["rx_flux"]
        del rx["weights"]
        del rx["attrs"]["gain"]
        rx["attrs"].update(
            {"gain_name": "unit", "gain_version": "gedi01b-v002-placeholder", "gain": 1.0}
        )
        return cfg

    def test_the_shipped_template_hash_is_pinned(self):
        assert semantic_hash(default_config("gedi01b_waveform_healpix_hive")) == self.FLUX_HASH

    def test_the_prior_counts_form_hashes_differently(self):
        assert semantic_hash(self._prior_form()) == self.COUNTS_HASH
        assert self.FLUX_HASH != self.COUNTS_HASH
