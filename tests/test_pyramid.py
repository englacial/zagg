"""The collapsed pyramid overviews grammar (issue #382).

Grammar-only coverage for :mod:`zagg.pyramid` after the espg grammar-collapse
ruling (PR #389 thread): ``output.pyramid.overviews`` declares leaf cell
resolutions only (scalar sugar, strict descent, strictly inside the shard's
resolution window); everything above the shard is the fixed every-order
ladder down to node 0, expanded by :func:`zagg.pyramid.expand_overviews`
into the ``(node, cells)`` list the manifest records. Declaration/manifest
behavior lives in ``tests/test_sweep_overview.py``; the spec fixture in
``tests/test_spec_conformance.py``.
"""

import pytest

from zagg.pyramid import (
    column_tier_gaps,
    default_overviews,
    expand_overviews,
    normalize_overviews,
    validate_overviews,
)

#: The reference 19/13/9 geometry from the issue #381 design record.
REF = {"parent_order": 9, "child_order": 19}


class TestNormalizeOverviews:
    def test_scalar_is_sugar_for_one_resolution(self):
        assert normalize_overviews(13) == [13]

    def test_list_kept_in_order_and_intified(self):
        assert normalize_overviews([16, 13]) == [16, 13]

    @pytest.mark.parametrize("raw", [None, {}, [], "13", 13.0])
    def test_non_int_shapes_refused(self, raw):
        with pytest.raises(ValueError, match="an int or a non-empty list of ints"):
            normalize_overviews(raw)

    @pytest.mark.parametrize("raw", [True, [True], [13, False]])
    def test_booleans_refused(self, raw):
        # YAML `true` is a bool, and bool IS an int in Python — a typo'd
        # `overviews: true` must not launder into a resolution-1 declaration.
        with pytest.raises(ValueError, match="an int or a non-empty list of ints"):
            normalize_overviews(raw)

    @pytest.mark.parametrize(
        "raw",
        [
            [{"node": 9, "cells": [13]}],
            [{"node": 9, "cells": 13}, {"node": 7, "cells": 11}],
        ],
    )
    def test_pair_spelling_refused_by_name(self, raw):
        # The {node, cells} grammar was collapsed away: the ladder above the
        # shard is law, so pair entries refuse with a pointed message.
        with pytest.raises(ValueError, match="pair spelling was collapsed away"):
            normalize_overviews(raw)

    def test_string_members_refused(self):
        with pytest.raises(ValueError, match="an int or a non-empty list of ints"):
            normalize_overviews([13, "11"])


class TestValidateOverviews:
    def test_single_and_multi_resolution_lists_are_valid(self):
        validate_overviews([13], **REF)
        validate_overviews([14, 13], **REF)
        validate_overviews([18, 17, 16], **REF)

    def test_dense_default_and_live_store_shapes_are_contiguous(self):
        # The omitted-knob default (one resolution at the chunk order) and the
        # two live stores (`--overviews 13` on 9/19, `--overviews 12` on 9/18)
        # are one-member lists: the fixed ladder fills [shard, base) by
        # itself, so the column tier steps by one down to the shard order.
        default_overviews(9, 13, child_order=19)
        validate_overviews([13], **REF)
        validate_overviews([12], parent_order=9, child_order=18)
        validate_overviews([13, 12, 11, 10], **REF)

    @pytest.mark.parametrize(
        "resolutions, tier, missing",
        [
            ([13, 12, 10], [13, 12, 10, 9], [11]),
            ([12, 10], [12, 10, 9], [11]),
            ([13, 11], [13, 11, 10, 9], [12]),
            ([16, 13], [16, 13, 12, 11, 10, 9], [14, 15]),
        ],
    )
    def test_gapped_column_tier_refused_by_name(self, resolutions, tier, missing):
        # espg ruling (PR #567 thread, 2026-09-17): every ladder level whose
        # cells are at or above the shard order IS the leaf column tier
        # (§4.6), so it must be contiguous from the finest leaf resolution
        # down to the shard order; the message names the tier and the gap.
        with pytest.raises(ValueError, match="column tier must be contiguous") as exc:
            validate_overviews(resolutions, **REF)
        assert f"cells at or above shard order 9 are {tier}, missing {missing}" in str(exc.value)

    def test_gaps_below_the_shard_order_are_not_the_tier(self):
        # The stage-merge levels (cells < shard order) MAY gap: a hand-built
        # ladder missing node 1 has a contiguous tier and reports nothing.
        levels = [
            {"node": 3, "cells": [6, 5, 4]},
            {"node": 2, "cells": [3]},
            {"node": 0, "cells": [1]},
        ]
        assert column_tier_gaps(levels, 3) == ([6, 5, 4, 3], [])
        assert column_tier_gaps(expand_overviews([6, 4], parent_order=3), 3) == ([6, 4, 3], [5])
        assert column_tier_gaps([{"node": 1, "cells": [2]}], 3) == ([], [])

    @pytest.mark.parametrize("resolutions", [[13, 16], [13, 13]])
    def test_not_strictly_descending_refused(self, resolutions):
        with pytest.raises(ValueError, match="must strictly descend"):
            validate_overviews(resolutions, **REF)

    @pytest.mark.parametrize("bad", [9, 8, 19, 20])
    def test_outside_the_shard_window_refused(self, bad):
        # Strictly between parent_order and child_order: the shard-order
        # aggregate is writer-side (never declared) and the base data is the
        # store itself.
        with pytest.raises(ValueError, match="not strictly between"):
            validate_overviews([bad], **REF)

    def test_boundary_interior_is_legal(self):
        validate_overviews([18], **REF)
        validate_overviews([10], **REF)


class TestExpandOverviews:
    def test_leaf_entry_plus_fixed_ladder_to_zero(self):
        # d = base - parent = 4: every order below the shard carries k + 4.
        assert expand_overviews([13], parent_order=9) == [
            {"node": 9, "cells": [13]},
            {"node": 8, "cells": [12]},
            {"node": 7, "cells": [11]},
            {"node": 6, "cells": [10]},
            {"node": 5, "cells": [9]},
            {"node": 4, "cells": [8]},
            {"node": 3, "cells": [7]},
            {"node": 2, "cells": [6]},
            {"node": 1, "cells": [5]},
            {"node": 0, "cells": [4]},
        ]

    def test_multi_resolution_leaf_entry(self):
        # Every declared resolution materializes at the leaf; the ladder is
        # fixed by the COARSEST one (the base).
        levels = expand_overviews([14, 13], parent_order=9)
        assert levels[0] == {"node": 9, "cells": [14, 13]}
        assert levels[1:] == expand_overviews([13], parent_order=9)[1:]

    def test_every_order_no_parity_cases(self):
        # Option D: every store roots at order 0, even and odd shard orders
        # alike — no parity rule.
        for parent, base in ((6, 8), (9, 13), (3, 4)):
            nodes = [e["node"] for e in expand_overviews([base], parent_order=parent)]
            assert nodes == list(range(parent, -1, -1))

    def test_two_numbers_determine_the_ladder(self):
        # The above-shard schedule is a pure function of (parent_order, d).
        d = 3
        for parent in (4, 7):
            ladder = expand_overviews([parent + d], parent_order=parent)[1:]
            assert ladder == [{"node": k, "cells": [k + d]} for k in range(parent - 1, -1, -1)]


class TestDefaultOverviews:
    def test_reference_geometry(self):
        levels = default_overviews(9, 13, child_order=19)
        assert levels == expand_overviews([13], parent_order=9)
        assert levels[0] == {"node": 9, "cells": [13]} and levels[-1] == {"node": 0, "cells": [4]}

    def test_spec_fixture_geometry(self):
        assert default_overviews(3, 5, child_order=6) == [
            {"node": 3, "cells": [5]},
            {"node": 2, "cells": [4]},
            {"node": 1, "cells": [3]},
            {"node": 0, "cells": [2]},
        ]

    def test_k_one_geometry_refuses(self):
        # chunk_order == parent_order (K == 1): no strictly-interior default
        # exists — declare explicitly or declare the pyramid off.
        with pytest.raises(ValueError, match="not strictly between"):
            default_overviews(4, 4, child_order=6)

    def test_chunk_order_at_child_order_refuses(self):
        # The base entry would BE the base data.
        with pytest.raises(ValueError, match="not strictly between"):
            default_overviews(9, 19, child_order=19)


class TestConfigWiring:
    """``output.pyramid.overviews`` through ``validate_config`` (issue #382)."""

    def _cfg(self, **output):
        from zagg.config import default_config

        cfg = default_config("atl06")  # grid: parent_order 6, child_order 12
        cfg.output.update(output)
        return cfg

    def _validate(self, pyramid):
        from zagg.config import validate_config

        validate_config(self._cfg(store_layout="hive", pyramid=pyramid))

    def test_valid_overviews_knob(self):
        self._validate({"overviews": [8, 7]})
        self._validate({"overviews": 8})  # scalar sugar

    def test_overviews_with_orders_or_spacing_refused(self):
        for extra in ({"orders": [4]}, {"spacing": 2}):
            with pytest.raises(ValueError, match="declare overviews OR orders/spacing"):
                self._validate({"overviews": [9], **extra})

    def test_grammar_errors_surface_through_validate_config(self):
        with pytest.raises(ValueError, match="an int or a non-empty list of ints"):
            self._validate({"overviews": []})
        with pytest.raises(ValueError, match="pair spelling was collapsed away"):
            self._validate({"overviews": [{"node": 4, "cells": 6}]})
        with pytest.raises(ValueError, match="must strictly descend"):
            self._validate({"overviews": [7, 9]})
        with pytest.raises(ValueError, match="not strictly between"):
            self._validate({"overviews": [6]})  # == parent_order
        with pytest.raises(ValueError, match="not strictly between"):
            self._validate({"overviews": [12]})  # == child_order
        with pytest.raises(ValueError, match=r"column tier must be contiguous.*missing \[8\]"):
            self._validate({"overviews": [9, 7]})  # tier [9, 7, 6] skips 8

    def test_overviews_require_hive_layout(self):
        from zagg.config import validate_config

        with pytest.raises(ValueError, match="output.pyramid requires"):
            validate_config(
                self._cfg(
                    store_layout="flat",
                    coverage_moc=False,
                    sweep=False,
                    pyramid={"overviews": [9]},
                )
            )

    def test_missing_child_order_refuses_by_name(self):
        from zagg.config import _validate_pyramid

        cfg = self._cfg(store_layout="hive", pyramid={"overviews": [9]})
        cfg.output["grid"] = {"type": "healpix", "parent_order": 6}
        with pytest.raises(ValueError, match="output.grid.child_order is required"):
            _validate_pyramid(cfg)
