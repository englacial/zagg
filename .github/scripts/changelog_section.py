"""Write the release section of CHANGELOG.md from merged pull-request titles.

Called by the ``changelog`` job in ``.github/workflows/publish.yml`` on every
``*.*.*`` tag (the repo's release trigger) so PRs never hand-edit CHANGELOG.md
and never conflict on it. Given the tag, its bounds in time, and the merged PRs
``gh pr list`` returned, it inserts

    ## [TAG] - YYYY-MM-DD

    ### Notes
    <whatever ``## [Unreleased]`` held -- moved here verbatim, leaving it empty>

    ### Merged pull requests
    - <title> ([#N](url)) by @author      <- merge-time ascending

right after ``## [Unreleased]``. A PR belongs to the release iff its merge
commit is in ``--revs`` (``git rev-list PREV..TAG``) -- exact, and immune to
GitHub stamping ``mergedAt`` a second after the merge commit's own time, which
a time window would drop for the PR the tag sits on. Rows without a merge
commit fall back to ``(prev-tag-time, tag-time]``. Branch-sync merges
(``merge main ...`` / ``Merge ...``) and duplicates are dropped; when no PR
survives, ``--fallback`` (pre-rendered bullets, e.g. commit subjects) fills the
list. Re-running for a tag already in the file is a no-op (exit 0, nothing
written), so a ``workflow_dispatch`` replay cannot duplicate a section. Stdlib
only: the runner calls it with bare python3.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

UNRELEASED = "## [Unreleased]"
#: Branch-sync noise, not release notes.
_SYNC_TITLE = re.compile(r"(?i:merge main)|Merge\b")


def _parse_time(value: str) -> datetime:
    """An ISO-8601 timestamp (``git log --format=%cI`` or GitHub's ``...Z``)."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def pr_bullets(
    prs: list[dict], prev_tag_time: str | None, tag_time: str, revs: set[str] | None = None
) -> list[str]:
    """One ``- title ([#N](url)) by @login`` per PR in the release.

    Membership is the merge commit being in ``revs`` when both are known,
    else ``mergedAt`` in ``(prev_tag_time, tag_time]``. Sync merges skipped,
    de-duplicated by number, merge-time ascending.
    """
    lower = _parse_time(prev_tag_time) if prev_tag_time else None
    upper = _parse_time(tag_time)
    seen: set[int] = set()
    kept: list[tuple[datetime, int, str]] = []
    for pr in prs:
        merged = pr.get("mergedAt")
        if not merged:
            continue
        when = _parse_time(merged)
        oid = (pr.get("mergeCommit") or {}).get("oid")
        if revs is not None and oid:
            if oid not in revs:
                continue
        elif when > upper or (lower is not None and when <= lower):
            continue
        number = int(pr["number"])
        title = pr["title"].strip()
        if number in seen or _SYNC_TITLE.match(title):
            continue
        seen.add(number)
        author = (pr.get("author") or {}).get("login") or "unknown"
        kept.append((when, number, f"- {title} ([#{number}]({pr['url']})) by @{author}"))
    return [line for _, _, line in sorted(kept)]


def _split_unreleased(text: str) -> tuple[list[str], list[str], list[str]]:
    """(head incl. the Unreleased heading, its body, the rest) of a changelog."""
    lines = text.splitlines()
    try:
        start = lines.index(UNRELEASED)
    except ValueError as exc:
        raise SystemExit(f"{UNRELEASED!r} heading not found") from exc
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    return lines[: start + 1], lines[start + 1 : end], lines[end:]


def _strip_blank(lines: list[str]) -> list[str]:
    while lines and not lines[0].strip():
        lines = lines[1:]
    while lines and not lines[-1].strip():
        lines = lines[:-1]
    return lines


def render(text: str, tag: str, date: str, bullets: list[str]) -> str | None:
    """The changelog with ``## [tag]`` inserted, or None if it already has one."""
    if re.search(rf"^## \[{re.escape(tag)}\]", text, re.MULTILINE):
        return None
    head, unreleased, rest = _split_unreleased(text)
    notes = _strip_blank(unreleased)
    section = [f"## [{tag}] - {date}", ""]
    if notes:
        section += ["### Notes", "", *notes, ""]
    section += ["### Merged pull requests", "", *(bullets or ["- (none recorded)"]), ""]
    return "\n".join([*head, "", *section, *rest]).rstrip("\n") + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tag", required=True)
    ap.add_argument("--tag-time", required=True, help="ISO-8601 commit time of --tag")
    ap.add_argument("--prev-tag-time", help="ISO-8601 commit time of the previous tag (if any)")
    ap.add_argument("--prs", required=True, type=Path, help="gh pr list --json output")
    ap.add_argument("--revs", type=Path, help="git rev-list PREV..TAG output (exact membership)")
    ap.add_argument("--fallback", type=Path, help="pre-rendered bullets used when no PR matches")
    ap.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    args = ap.parse_args(argv)

    revs = set(args.revs.read_text().split()) if args.revs else None
    prs = json.loads(args.prs.read_text())
    bullets = pr_bullets(prs, args.prev_tag_time, args.tag_time, revs)
    if not bullets and args.fallback and args.fallback.exists():
        bullets = [ln for ln in args.fallback.read_text().splitlines() if ln.startswith("- ")]
    date = _parse_time(args.tag_time).date().isoformat()
    updated = render(args.changelog.read_text(), args.tag, date, bullets)
    if updated is None:
        print(f"{args.changelog}: [{args.tag}] already present, nothing to do")
        return 0
    args.changelog.write_text(updated)
    print(f"{args.changelog}: wrote [{args.tag}] - {date} ({len(bullets)} bullets)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
