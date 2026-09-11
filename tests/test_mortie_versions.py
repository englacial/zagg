"""Drift guard: no mortie version zagg's prose quotes may outrun the floor.

zagg names mortie release numbers in three different voices, and only one of
them is a live obligation:

1. the **package floor** — ``mortie>=…`` in ``[project.dependencies]``, the
   single source (``deployment/aws/build_layer.sh`` derives ``MORTIE_SPEC``
   from it, issue #322);
2. **arrival notes** in narrative docs and docstrings ("added in mortie
   0.8.4") — historically true, permanently satisfied because the floor
   already exceeds them, and enforced by nothing at runtime;
3. the one **runtime gate**, :data:`zagg.grids.aoi.MIN_MORTIE_VERSION`, which
   ``_assert_mortie_version`` turns into a ``RuntimeError``.

The failure mode worth guarding is (2) drifting *above* (1): prose promising
an API the dependency floor does not guarantee, which reaches a user as an
``AttributeError`` where the docs said otherwise. So every mortie version
quoted under ``docs/`` or ``src/`` must be at or below the floor, and the
``aoi_mask`` family — the sites that document (3) and therefore cannot be
de-versioned — must quote the enforced constant exactly wherever they state it
as a *requirement* (``mortie >= 1.0.0``). Arrival voice in those same files
("shipped in 0.8.2") is history, not a restatement of the gate, so it is held
to the floor like any other citation and not to the constant. (This file is in
neither ``SCAN_ROOT``, so nothing checks that sentence against the constant it
quotes — a ``MIN_MORTIE_VERSION`` bump has to update it by hand; the loose
citation examples further down are deliberately synthetic and stay put.)

Scope note — two blind spots, both deliberate and both needing a human eye on a
``MIN_MORTIE_VERSION`` bump. A citation is recognized only where the release
number is lexically attached to the word ``mortie``: bare literals further along
a sentence ("espg/mortie#59 + #70, shipped in 0.8.2", ``src/zagg/grids/aoi.py``)
are invisible here, because no regex separates them from the other version
numbers prose carries. And whether a wrapped citation is seen depends on the
words between ``mortie`` and the number, so reflowing a paragraph can move one
in or out of scope.
"""

import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

from zagg.grids.aoi import MIN_MORTIE_VERSION

REPO_ROOT = Path(__file__).parent.parent

# Prose scanned for citations: narrative docs plus the shipped package
# (docstrings, comments, bundled config YAML). ``pyproject.toml`` is
# deliberately outside both roots — it is the floor these citations are
# measured against, not one of them.
SCAN_ROOTS = (REPO_ROOT / "docs", REPO_ROOT / "src")
SCAN_SUFFIXES = {".md", ".py", ".yaml", ".yml"}

# The ``aoi_mask`` family: the six sites documenting the runtime gate.
AOI_MASK_FILES = ("docs/aoi_mask.md", "src/zagg/grids/aoi.py")

# ``mortie`` followed by a release number. A missed spelling is a *silent*
# pass, the one failure mode a drift guard cannot afford, so the separator and
# operator sets are deliberately loose: ``mortie >= 0.8.3``, ``mortie ≥ 0.8.4``,
# ``mortie>=0.8.3``, ``mortie==0.9.3`` (how a pin gets written up — the
# ``lambda`` extra is nothing but ``==`` pins), ``mortie ~= 0.9``, bare
# ``mortie 0.8.1``, ``mortie (0.9.0)`` / ``mortie: 0.9.0`` / ``mortie-0.9.0``,
# ``mortie version 0.9.0``, sentence-initial ``Mortie 0.9.0``, and the
# backticked variants (the backticks fall outside the match). Whitespace spans
# newlines so a wrapped citation is not a blind spot.
#
# Deliberately *not* matched: ``espg/mortie#89`` (an issue reference is not a
# version — ``#`` is outside the separator class), "frozen for mortie 1.x" (no
# second numeric component), and upper bounds like ``mortie < 1.0``, which
# promise no API and so are not citations this guard is about.
#
# Group 1 is the comparison operator, present only in **requirement** voice
# ("zagg needs at least this"); arrival voice ("added in mortie 0.8.4") leaves
# it ``None``. That distinction is what scopes the ``aoi_mask`` equality rule.
_CITATION = re.compile(
    r"mortie[\s(:,_-]*(?:version|release)?\s*(>=|==|~=|>|≥)?\s*v?(\d+\.\d+(?:\.\d+)*)",
    re.IGNORECASE,
)


def _cited_versions(text, requirement_voice_only=False):
    """Every mortie release ``text`` names, as ``(Version, line number)``."""
    return [
        (Version(m.group(2)), text.count("\n", 0, m.start(2)) + 1)
        for m in _CITATION.finditer(text)
        if m.group(1) or not requirement_voice_only
    ]


def _scan(requirement_voice_only=False):
    """Every mortie citation under the scan roots, as ``(path, line, Version)``."""
    sites = []
    for root in SCAN_ROOTS:
        for path in sorted(root.rglob("*")):
            if path.suffix not in SCAN_SUFFIXES or not path.is_file():
                continue
            rel = path.relative_to(REPO_ROOT).as_posix()
            text = path.read_text(encoding="utf-8")
            for version, line in _cited_versions(text, requirement_voice_only):
                sites.append((rel, line, version))
    return sites


def _above(sites, floor):
    """The comparison the guard makes — PEP 440 ordering, never string compare."""
    return [s for s in sites if s[2] > floor]


def _mortie_floor():
    """The ``>=`` floor from ``[project.dependencies]``."""
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    reqs = [Requirement(d) for d in pyproject["project"]["dependencies"]]
    # Every mortie requirement, not the first: a marker-split pair would leave
    # half the matrix measured against the other half's floor. Names are
    # normalized so a legal ``Mortie>=…`` is found rather than raising blank.
    mortie = [r for r in reqs if canonicalize_name(r.name) == "mortie"]
    assert len(mortie) == 1, f"expected exactly one mortie requirement in pyproject, got {mortie}"
    # ``"*" not in`` keeps a wildcard pin (``mortie==0.9.*``) out of
    # ``Version`` — it reaches the assertion below instead of ``InvalidVersion``.
    lower = [
        Version(s.version)
        for s in mortie[0].specifier
        if s.operator in (">=", "==", "~=") and "*" not in s.version
    ]
    assert len(lower) == 1, (
        f"expected one concrete mortie lower bound in pyproject, got {lower} from {mortie[0]}"
    )
    return lower[0]


class TestQuotedVersionsAgainstFloor:
    """Nothing zagg's prose promises may exceed what the floor guarantees."""

    def test_scan_finds_the_versioned_sites(self):
        # A regex that silently matched nothing would let every other test in
        # this module pass forever, so pin that the scan has real work to do.
        # Only the count, deliberately: naming the files would ratchet them in
        # place, and *deleting* an unenforced per-feature minimum is the very
        # edit this PR argues for. The actual is 16, across docs/aoi_mask.md,
        # docs/design/sparse_coverage.md, docs/morton_arrow.md,
        # docs/ragged_layout.md, src/zagg/configs/atl03_tdigest_healpix.yaml,
        # and src/zagg/{grids/aoi,grids/healpix,grids/morton,readers/_layout}.py.
        sites = _scan()
        assert len(sites) >= 12, f"mortie citation scan went blind: {sites}"

    def test_no_citation_outruns_the_floor(self):
        floor = _mortie_floor()
        over = _above(_scan(), floor)
        assert not over, (
            f"prose quotes a mortie version above the pyproject floor {floor}: {over}. "
            "Either raise the floor in [project.dependencies] (an espg-authorized "
            "dependency change) or restate the prose against a shipped release."
        )

    def test_runtime_gate_is_at_or_below_the_floor(self):
        floor = _mortie_floor()
        assert Version(MIN_MORTIE_VERSION) <= floor, (
            f"zagg.grids.aoi.MIN_MORTIE_VERSION ({MIN_MORTIE_VERSION}) is above the "
            f"pyproject floor ({floor}) — aoi_mask would raise on a conforming install"
        )

    def test_aoi_mask_prose_matches_the_runtime_gate(self):
        # These six sites document a gate that raises (``aoi.py`` lines 66-80),
        # so they keep requirement voice — and must quote the constant itself.
        # Requirement voice only: the same two files also carry arrival-voice
        # history ("shipped in 0.8.2"), which is factually correct at a version
        # *below* the gate and must not be dragged into an equality rule.
        family = [s for s in _scan(requirement_voice_only=True) if s[0] in AOI_MASK_FILES]
        assert len(family) >= 6, f"aoi_mask citations vanished from the scan: {family}"
        wrong = [s for s in family if s[2] != Version(MIN_MORTIE_VERSION)]
        assert not wrong, (
            f"aoi_mask prose disagrees with MIN_MORTIE_VERSION ({MIN_MORTIE_VERSION}): {wrong}"
        )


class TestGuardFires:
    """A guard that cannot fail is worse than no guard — prove each part bites."""

    def test_a_citation_above_the_floor_is_caught(self):
        floor = _mortie_floor()
        text = "the ragged reader rides `mortie >= 99.0.0`, whose contract is normative"
        sites = [("docs/synthetic.md", line, v) for v, line in _cited_versions(text)]
        assert [str(s[2]) for s in sites] == ["99.0.0"]
        assert _above(sites, floor) == sites

    def test_ordering_is_pep440_not_lexicographic(self):
        # The case a string compare gets backwards: "0.10.0" < "0.9.5" as text.
        assert "0.10.0" < "0.9.5"
        assert Version("0.10.0") > Version("0.9.5")
        sites = [("docs/synthetic.md", 1, Version("0.10.0"))]
        assert _above(sites, Version("0.9.5")) == sites
        assert _above(sites, Version("0.10.0")) == []

    def test_a_wrapped_citation_reports_the_version_line(self):
        # The word and the number can land on different lines (``_layout.py``,
        # ``morton.py``). The site printed has to be where the *number* is, or
        # a failure sends the reader to a line that carries no version at all.
        assert _cited_versions("added in mortie\n0.9.0") == [(Version("0.9.0"), 2)]

    def test_issue_references_are_not_versions(self):
        assert _cited_versions("(espg/mortie#89, espg/mortie#100) and mortie 1.x") == []
        # Widening the operator set must not drag these in: an issue number is
        # not a release, and an upper bound is not a promise about an API.
        assert _cited_versions("gated on mortie#116, and frozen for mortie 1.x") == []
        assert _cited_versions("pinned mortie < 1.0 while the API settles") == []

    def test_every_citation_spelling_is_recognized(self):
        text = (
            "`mortie >= 0.8.3` and mortie ≥ 0.8.4 and `mortie>=0.9.3` and "
            "mortie 0.8.1 and added in mortie\n0.9.0"  # wrapped by a line break
        )
        assert [str(v) for v, _ in _cited_versions(text)] == [
            "0.8.3",
            "0.8.4",
            "0.9.3",
            "0.8.1",
            "0.9.0",
        ]

    def test_the_spellings_that_used_to_pass_silently_are_caught(self):
        # Each of these read as no citation at all before the pattern was
        # widened — a doc could promise an unshipped API and stay green.
        for spelling in (
            "Mortie 0.10.0 adds the flat export",  # sentence-initial
            "the layer pins mortie==0.10.0",  # how a pin gets written up
            "mortie ~= 0.10.0",
            "mortie > 0.10.0",
            "the cover entry points (mortie (0.10.0))",
            "see mortie: 0.10.0",
            "shipped in mortie, 0.10.0",
            "the mortie-0.10.0 wheel",
            "needs mortie version 0.10.0",
            "requires at least mortie release 0.10.0",
            "tagged mortie v0.10.0",
        ):
            assert [str(v) for v, _ in _cited_versions(spelling)] == ["0.10.0"], spelling

    def test_requirement_voice_is_told_from_arrival_voice(self):
        # The split the ``aoi_mask`` equality rule rides on: only a citation
        # carrying a comparison operator states an obligation.
        text = "aoi_mask needs mortie >= 0.8.3; the MOC cap shipped in mortie 0.8.2"
        assert [str(v) for v, _ in _cited_versions(text)] == ["0.8.3", "0.8.2"]
        assert [str(v) for v, _ in _cited_versions(text, requirement_voice_only=True)] == ["0.8.3"]

    def test_the_floor_line_is_not_scanned_as_a_citation(self):
        # The regex would happily match ``mortie>={floor}`` in pyproject.toml —
        # reading the floor back as a citation would make the comparison
        # self-satisfying — so assert the two mechanisms that actually exclude
        # it, rather than a path check no scanned suffix could ever satisfy.
        floor = _mortie_floor()
        assert [v for v, _ in _cited_versions(f'    "mortie>={floor}",')] == [floor]
        assert ".toml" not in SCAN_SUFFIXES
        pyproject = REPO_ROOT / "pyproject.toml"
        assert not any(pyproject.is_relative_to(root) for root in SCAN_ROOTS)
