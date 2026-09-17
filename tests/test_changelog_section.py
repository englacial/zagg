"""Tests for the release-time changelog generator (.github/scripts/changelog_section.py).

Pure/offline: a synthetic CHANGELOG plus a hand-rolled ``gh pr list`` payload.
Pins the four behaviours publish.yml's ``changelog`` job relies on: the
``[Unreleased]`` body is drained into ``### Notes``; PR bullets are windowed,
sync-merge-filtered, de-duplicated and merge-time sorted; a tag already present
is a no-op; an empty ``[Unreleased]`` yields no Notes sub-heading.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / ".github" / "scripts"))

import changelog_section as cs  # noqa: E402

CHANGELOG = """\
# Changelog

Sections are generated at release.

## [Unreleased]

- **hand note one** (#1)
  continuation line
  - nested bullet

- hand note two

## [0.53.0] - 2026-09-10

- older ([#500](https://x/pull/500)) by @espg
"""

TAG_TIME = "2026-09-17T12:00:00+00:00"
PREV_TIME = "2026-09-10T00:00:00+00:00"


def _pr(number, title, merged, author="espg"):
    return {
        "number": number,
        "title": title,
        "url": f"https://github.com/englacial/zagg/pull/{number}",
        "author": {"login": author},
        "mergedAt": merged,
    }


PRS = [
    _pr(570, "later fix", "2026-09-16T09:00:00Z"),
    _pr(566, "earlier feature", "2026-09-12T09:00:00Z", author="bot"),
    _pr(566, "earlier feature", "2026-09-12T09:00:00Z"),  # duplicate row
    _pr(567, "merge main into claude/x", "2026-09-13T09:00:00Z"),  # sync noise
    _pr(568, "Merge branch 'main' into y", "2026-09-13T10:00:00Z"),  # sync noise
    _pr(499, "before the window", "2026-09-10T00:00:00Z"),  # == prev tag time
    _pr(580, "after the tag", "2026-09-18T00:00:00Z"),
]


def test_bullets_are_windowed_filtered_deduplicated_and_sorted():
    assert cs.pr_bullets(PRS, PREV_TIME, TAG_TIME) == [
        "- earlier feature ([#566](https://github.com/englacial/zagg/pull/566)) by @bot",
        "- later fix ([#570](https://github.com/englacial/zagg/pull/570)) by @espg",
    ]


def test_only_branch_sync_shapes_are_dropped_not_titles_starting_with_merge():
    # The filter sees a PR title, not a merge-commit subject, so a bare ^Merge\b
    # would silently eat real release notes -- and a dropped PR leaves no trace.
    keep = [
        "Merge the dense and sparse readers into one path",
        "Merged shard maps are now cached",
        "MERGE sort for t-digest centroids",
        "merge-order law for k-way t-digest",
    ]
    drop = [
        "merge main into claude/x",
        "Merge main",
        "Merge branch 'main' into y",
        "Merge remote-tracking branch 'origin/main'",
        "Merge pull request #12 from espg/x",
        "merge origin/main into claude/x",
        "Merge upstream into fork",
    ]
    assert [t for t in keep if cs._SYNC_TITLE.match(t)] == []
    assert [t for t in drop if not cs._SYNC_TITLE.match(t)] == []


def _numbers(bullets):
    return [ln.split("[#")[1].split("]")[0] for ln in bullets]


def test_first_release_has_no_lower_bound():
    assert _numbers(cs.pr_bullets(PRS, None, TAG_TIME)) == ["499", "566", "570"]


def test_merge_commit_membership_beats_the_time_window():
    # The real 0.54.0 case: the tag sits ON a PR's squash commit and GitHub
    # stamps mergedAt one second after that commit's time, so a time window
    # drops it. With --revs the merge commit decides; a row without one still
    # falls back to the window.
    on_tag = _pr(569, "the PR the tag sits on", "2026-09-17T12:00:01Z") | {
        "mergeCommit": {"oid": "aaa"}
    }
    late = _pr(580, "after the tag", "2026-09-16T00:00:00Z") | {"mergeCommit": {"oid": "zzz"}}
    windowed = _pr(570, "no merge commit, in window", "2026-09-16T09:00:00Z")
    got = cs.pr_bullets([on_tag, late, windowed], PREV_TIME, TAG_TIME, revs={"aaa", "bbb"})
    assert _numbers(got) == ["570", "569"]
    assert _numbers(cs.pr_bullets([on_tag, late], PREV_TIME, TAG_TIME)) == ["580"]


def test_render_drains_unreleased_into_notes_above_the_pr_list():
    out = cs.render(CHANGELOG, "0.54.0", "2026-09-17", ["- later fix ([#570](u)) by @espg"])
    assert out is not None
    assert out.split("## [0.53.0] - 2026-09-10")[0] == (
        "# Changelog\n\nSections are generated at release.\n\n"
        "## [Unreleased]\n\n"
        "## [0.54.0] - 2026-09-17\n\n"
        "### Notes\n\n"
        "- **hand note one** (#1)\n  continuation line\n  - nested bullet\n\n- hand note two\n\n"
        "### Merged pull requests\n\n"
        "- later fix ([#570](u)) by @espg\n\n"
    )
    # The tail is untouched, and [Unreleased] is now empty.
    assert out.endswith(
        "## [0.53.0] - 2026-09-10\n\n- older ([#500](https://x/pull/500)) by @espg\n"
    )
    _, body, _ = cs._split_unreleased(out)
    assert not any(ln.strip() for ln in body)


def test_keep_a_changelog_subheadings_drain_without_an_empty_notes_heading():
    # CHANGELOG.md says the format is based on Keep a Changelog, so ### Added /
    # ### Fixed under [Unreleased] is the obvious thing to reach for. They are
    # headings already: nesting them under an empty "### Notes" flattens the
    # grouping it was meant to express.
    kac = CHANGELOG.replace(
        "- **hand note one** (#1)\n  continuation line\n  - nested bullet\n\n- hand note two\n",
        "### Added\n\n- a thing\n\n### Fixed\n\n- a bug\n",
    )
    out = cs.render(kac, "0.54.0", "2026-09-17", ["- later fix ([#570](u)) by @espg"])
    assert out is not None
    assert "### Notes" not in out
    assert (
        "## [0.54.0] - 2026-09-17\n\n"
        "### Added\n\n- a thing\n\n### Fixed\n\n- a bug\n\n"
        "### Merged pull requests\n\n- later fix ([#570](u)) by @espg\n" in out
    )
    # Plain bullets still get the Notes heading.
    assert "### Notes" in (cs.render(CHANGELOG, "0.54.0", "2026-09-17", []) or "")


def test_render_is_idempotent_once_the_tag_is_present():
    once = cs.render(CHANGELOG, "0.54.0", "2026-09-17", ["- x ([#1](u)) by @a"])
    assert once is not None
    assert cs.render(once, "0.54.0", "2026-09-17", ["- y ([#2](u)) by @b"]) is None
    assert cs.render(CHANGELOG, "0.53.0", "2026-09-10", []) is None


def test_render_with_empty_unreleased_omits_notes():
    empty = CHANGELOG.replace(
        "- **hand note one** (#1)\n  continuation line\n  - nested bullet\n\n- hand note two\n", ""
    )
    out = cs.render(empty, "0.54.0", "2026-09-17", [])
    assert out is not None
    assert "### Notes" not in out
    assert (
        "## [0.54.0] - 2026-09-17\n\n### Merged pull requests\n\n- (none recorded)\n\n## [0.53.0]"
        in out
    )


def test_replaying_an_older_tag_inserts_in_version_order_without_draining_notes():
    # The 0.47.0-0.53.0 gap: a workflow_dispatch replay for a skipped tag must
    # land below the newer sections and leave [Unreleased] alone.
    out = cs.render(CHANGELOG, "0.52.0", "2026-08-25", ["- old ([#450](u)) by @espg"])
    assert out is not None
    assert "### Notes" not in out
    assert "- hand note two\n\n## [0.53.0] - 2026-09-10" in out  # Unreleased intact
    assert out.endswith(
        "## [0.53.0] - 2026-09-10\n\n- older ([#500](https://x/pull/500)) by @espg\n\n"
        "## [0.52.0] - 2026-08-25\n\n### Merged pull requests\n\n- old ([#450](u)) by @espg\n"
    )
    # ...and the oldest of all goes last.
    out = cs.render(CHANGELOG, "0.1.0", "2026-01-01", [])
    assert out is not None
    assert out.endswith(
        "## [0.1.0] - 2026-01-01\n\n### Merged pull requests\n\n- (none recorded)\n"
    )


def test_render_refuses_a_changelog_without_unreleased():
    with pytest.raises(SystemExit, match="Unreleased"):
        cs.render("# Changelog\n\n## [0.1.0] - 2026-01-01\n", "0.2.0", "2026-01-02", [])


def test_main_writes_once_and_uses_the_fallback(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG)
    prs = tmp_path / "prs.json"
    prs.write_text(json.dumps([_pr(580, "after the tag", "2026-09-18T00:00:00Z")]))
    fallback = tmp_path / "fallback.txt"
    fallback.write_text("- commit subject (abc1234)\nnot a bullet\n")
    argv = [
        "--tag", "0.54.0", "--tag-time", TAG_TIME, "--prev-tag-time", PREV_TIME,
        "--prs", str(prs), "--fallback", str(fallback), "--changelog", str(changelog),
    ]  # fmt: skip
    assert cs.main(argv) == 0
    first = changelog.read_text()
    assert "### Merged pull requests\n\n- commit subject (abc1234)\n" in first
    assert "## [0.54.0] - 2026-09-17" in first
    assert cs.main(argv) == 0
    assert changelog.read_text() == first


@pytest.mark.parametrize("tag", ["v0.55.0", "0.54.0.1", "0.55", "latest"])
def test_main_refuses_a_tag_it_cannot_place(tag, tmp_path):
    # Without N.N.N the insert point is 0 -- top of the file AND the
    # [Unreleased] notes drained into it. A mistyped workflow_dispatch replay
    # must fail red, not steal the next release's notes.
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG)
    prs = tmp_path / "prs.json"
    prs.write_text("[]")
    with pytest.raises(SystemExit, match="is not N.N.N"):
        cs.main([
            "--tag", tag, "--tag-time", TAG_TIME,
            "--prs", str(prs), "--changelog", str(changelog),
        ])  # fmt: skip
    assert changelog.read_text() == CHANGELOG


def test_unmatched_revs_are_reported_for_a_warning(capsys, tmp_path):
    # GitHub's search index lags a merge, so the PR the tag sits on can be
    # absent from the rows entirely. Membership is only ever evaluated over the
    # rows returned, so the gap has to be counted separately or it is silent.
    on_tag = _pr(569, "indexed", "2026-09-17T12:00:01Z") | {"mergeCommit": {"oid": "aaa"}}
    assert cs.unmatched_revs([on_tag], {"aaa", "bbb", "ccc"}) == {"bbb", "ccc"}
    assert cs.unmatched_revs([on_tag], {"aaa"}) == set()

    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG)
    prs = tmp_path / "prs.json"
    prs.write_text(json.dumps([on_tag]))
    revs = tmp_path / "revs.txt"
    revs.write_text("aaa\nbbb\n")
    assert cs.main([
        "--tag", "0.54.0", "--tag-time", TAG_TIME, "--prev-tag-time", PREV_TIME,
        "--prs", str(prs), "--revs", str(revs), "--changelog", str(changelog),
    ]) == 0  # fmt: skip
    out = capsys.readouterr().out
    assert "::warning::1/2 commits in the tag range" in out
    assert "bbb" in out


def test_fallback_fires_when_the_rev_list_set_is_empty_not_the_search(tmp_path):
    # The search returned rows -- they are just all outside PREV..TAG. The
    # fallback keys off the rev-list-filtered set, so the pre-rendered subjects
    # still fill the section instead of "(none recorded)".
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG)
    prs = tmp_path / "prs.json"
    other = _pr(560, "another release", "2026-09-16T09:00:00Z") | {"mergeCommit": {"oid": "zzz"}}
    prs.write_text(json.dumps([other]))
    revs = tmp_path / "revs.txt"
    revs.write_text("aaa\n")
    fallback = tmp_path / "commits.txt"
    fallback.write_text("- a subject (abc1234)\n")
    assert cs.main([
        "--tag", "0.54.0", "--tag-time", TAG_TIME, "--prev-tag-time", PREV_TIME,
        "--prs", str(prs), "--revs", str(revs), "--fallback", str(fallback),
        "--changelog", str(changelog),
    ]) == 0  # fmt: skip
    assert "- a subject (abc1234)" in changelog.read_text()
    assert "another release" not in changelog.read_text()
