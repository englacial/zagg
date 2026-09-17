"""Pin the workflow half of the release-time changelog wiring.

``tests/test_changelog_section.py`` covers the generator; the two halves that
live in YAML have no other cover. Both are load-bearing and silent when wrong:

* ``publish.yml``'s ``changelog`` job must run BESIDE the build/publish chain
  (a changelog problem must not fail a release) with write access to contents
  and pull requests, and must never fail the release past generation.
* ``lint.yml``'s ``changelog-guard`` must keep exactly two escape hatches -- the
  ``changelog`` label and a ``changelog/`` head branch (the bot's own fallback
  PR, which would otherwise be refused by the guard it triggers).

Pure/offline: ``yaml.safe_load`` over the two files.
"""

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO = Path(__file__).resolve().parents[1]
PUBLISH = REPO / ".github" / "workflows" / "publish.yml"
LINT = REPO / ".github" / "workflows" / "lint.yml"


def _load(path):
    return yaml.safe_load(path.read_text())


def _triggers(wf):
    # PyYAML reads the `on:` key as the boolean True (YAML 1.1).
    return wf.get("on", wf.get(True))


def _run(steps):
    return "\n".join(str(s.get("run", "")) for s in steps)


def test_both_workflows_parse():
    assert _load(PUBLISH)["jobs"] and _load(LINT)["jobs"]


def test_changelog_job_runs_beside_the_release_chain():
    jobs = _load(PUBLISH)["jobs"]
    changelog = jobs["changelog"]
    # No `needs` in either direction: the release does not wait on it, and it
    # does not wait on the release (so a PyPI failure still records the section).
    assert "needs" not in changelog
    for name, job in jobs.items():
        assert "changelog" not in (job.get("needs") or []), f"{name} must not need changelog"
    assert changelog["permissions"] == {"contents": "write", "pull-requests": "write"}
    # It works on main, not on the tag: the section is committed to the branch.
    checkout = changelog["steps"][0]
    assert checkout["uses"].startswith("actions/checkout")
    assert checkout["with"]["ref"] == "main"
    assert checkout["with"]["fetch-depth"] == 0


def test_changelog_job_never_fails_the_release_past_generation():
    steps = _load(PUBLISH)["jobs"]["changelog"]["steps"]
    generate = next(s for s in steps if s.get("name") == "Generate the release section")
    commit = next(s for s in steps if s.get("name", "").startswith("Commit to main"))
    # Generation is allowed to fail red (a bad tag must not be guessed at);
    # everything past it ends in a ::warning::, never a non-zero exit.
    assert "set -euo pipefail" in generate["run"]
    assert "set -uo pipefail" in commit["run"]
    assert "::warning::" in commit["run"]
    assert "exit 1" not in commit["run"]


def test_changelog_guard_keeps_both_escape_hatches():
    guard = _load(LINT)["jobs"]["changelog-guard"]
    condition = " ".join(guard["if"].split())
    assert "!contains(github.event.pull_request.labels.*.name, 'changelog')" in condition
    assert "!startsWith(github.head_ref, 'changelog/')" in condition
    assert "&&" in condition  # both hatches, independently
    assert "CHANGELOG.md" in _run(guard["steps"])


def test_lint_reruns_the_guard_when_the_label_is_toggled():
    lint = _load(LINT)
    types = _triggers(lint)["pull_request"]["types"]
    assert {"labeled", "unlabeled"} <= set(types)
    # ...but ruff must not re-run on a label toggle (it would re-post its
    # inline comments with nothing in the diff changed).
    ruff = lint["jobs"]["ruff"]["if"]
    assert "labeled" in ruff and "unlabeled" in ruff
