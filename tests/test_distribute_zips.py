"""Test the release distribution script (.github/scripts/distribute_zips.sh).

Runs the real script with a stub ``aws`` on PATH (no network), and asserts it
uploads the four zips + SHA256SUMS under the minor prefix and maintains the
versions.json index at the destination root. The versions.json read-modify-write
is the part with real logic, so it's covered against both the seed (absent) and
merge paths.

Since issue #497 the script also supports Source Cooperative under a key prefix
(``publish.yml`` is not pointed at it yet), so the ``--prefix`` layout and the
``bucket-owner-full-control`` ACL that destination requires (issue #495 phase 1)
are covered too -- including the in-account bucket case that runs today, where
the ACL must NOT be sent.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / ".github" / "scripts" / "distribute_zips.sh"
WORKFLOW = REPO / ".github" / "workflows" / "publish.yml"

# A stub `aws` CLI: logs every `s3 cp`, fails the versions.json *download* unless
# a seed exists in $SEED_DIR, and captures every *upload* into $CAPTURE_DIR so the
# test can read back what the script produced. The download failure carries the
# CLI's real 404 wording, because the script now discriminates on it; $READ_ERROR
# overrides it to stand in for a failure that is NOT a clean miss.
STUB_AWS = """#!/bin/bash
set -euo pipefail
if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  SRC="$3"; DST="$4"
  echo "$*" >> "$AWS_LOG"
  if [[ "$SRC" == s3://* ]]; then
    # download: serve a seeded versions.json if present, else fail (not found).
    base="$(basename "$SRC")"
    if [ -f "$SEED_DIR/$base" ]; then cp "$SEED_DIR/$base" "$DST"; exit 0; fi
    echo "${READ_ERROR:-fatal error: An error occurred (404) when calling the \
HeadObject operation: Key \\"$SRC\\" does not exist}" >&2
    exit 1
  else
    # upload: capture under the destination key.
    key="${DST#s3://}"; key="${key#*/}"
    mkdir -p "$CAPTURE_DIR/$(dirname "$key")"
    cp "$SRC" "$CAPTURE_DIR/$key"
    exit 0
  fi
fi
exit 0
"""


#: The destination issue #497 prepares for -- Source Cooperative, under the same
#: <prefix>/<minor>/<zip> layout publish_mirror.sh writes and stand_up.sh's
#: DIST_PREFIX already reads. Not yet what publish.yml passes.
MIRROR_BUCKET = "us-west-2.opendata.source.coop"
MIRROR_PREFIX = "englacial/zagg/lambda"


def _prepare(
    tmp_path, *, seed_versions=None, bucket=MIRROR_BUCKET, prefix=MIRROR_PREFIX, read_error=None
):
    """Build the stub harness; return (argv, env, destination root).

    The root is the captured tree under ``prefix``, so a caller asserting on
    ``root / "0.3" / ...`` is asserting the prefixed key, not just the tail.
    ``read_error`` makes the index download fail with something other than a
    clean miss, for the callers that assert the script refuses to seed on it.
    """
    if not shutil.which("sha256sum"):
        pytest.skip("sha256sum not available")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "aws").write_text(STUB_AWS)
    (bindir / "aws").chmod(0o755)

    seed_dir = tmp_path / "seed"
    seed_dir.mkdir()
    if seed_versions is not None:
        (seed_dir / "versions.json").write_text(json.dumps(seed_versions))
    capture = tmp_path / "capture"
    capture.mkdir()

    zips = tmp_path / "zips"
    zips.mkdir()
    for name in (
        "lambda_layer_arm64.zip",
        "lambda_layer_x86_64.zip",
        "lambda_function_arm64_py312.zip",
        "lambda_function_x86_64_py312.zip",
    ):
        (zips / name).write_bytes(b"dummy-" + name.encode())

    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "AWS_LOG": str(tmp_path / "aws.log"),
        "SEED_DIR": str(seed_dir),
        "CAPTURE_DIR": str(capture),
    }
    if read_error is not None:
        env["READ_ERROR"] = read_error
    argv = ["bash", str(SCRIPT), "--minor", "0.3", "--tag", "0.3.1", "--bucket", bucket]
    if prefix:
        argv += ["--prefix", prefix]
    argv += ["--dir", str(zips)]
    return argv, env, (capture / prefix if prefix else capture)


def _run(tmp_path, **kwargs):
    """Run the script against the stub and return (destination root, aws log)."""
    argv, env, root = _prepare(tmp_path, **kwargs)
    subprocess.run(argv, check=True, env=env, cwd=tmp_path)
    return root, (tmp_path / "aws.log").read_text()


def test_uploads_four_zips_and_sums(tmp_path):
    root, log = _run(tmp_path)
    for name in (
        "lambda_layer_arm64.zip",
        "lambda_layer_x86_64.zip",
        "lambda_function_arm64_py312.zip",
        "lambda_function_x86_64_py312.zip",
        "SHA256SUMS",
    ):
        assert (root / "0.3" / name).exists(), f"{name} not uploaded under {MIRROR_PREFIX}/0.3/"


def test_versions_index_seeds_when_absent(tmp_path):
    root, _ = _run(tmp_path)  # no seed -> download fails -> seed {"minors": []}
    index = json.loads((root / "versions.json").read_text())
    assert index["minors"] == ["0.3"]
    assert index["latest"] == "0.3"
    assert index["latest_tag"] == "0.3.1"


def test_versions_index_merges_and_sorts(tmp_path):
    root, _ = _run(tmp_path, seed_versions={"minors": ["0.1", "0.10", "0.2"]})
    index = json.loads((root / "versions.json").read_text())
    # New minor merged; sorted numerically (0.10 > 0.3, not lexically); latest correct.
    assert index["minors"] == ["0.1", "0.2", "0.3", "0.10"]
    assert index["latest"] == "0.10"


def test_a_read_failure_that_is_not_a_miss_never_reseeds_the_index(tmp_path):
    # Fold review: the index read used to swallow every failure into the seed
    # branch, and the very next statement PUTs the seed back over the good index.
    # A throttle, a 5xx, an expired session or a typo'd --prefix would drop every
    # published minor with no DeleteObject in sight. Only a miss may seed.
    denied = "An error occurred (403) when calling the HeadObject operation: Forbidden"
    argv, env, root = _prepare(tmp_path, read_error=f"fatal error: {denied}")
    result = subprocess.run(argv, env=env, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode != 0
    # ...and it says why: the CLI's own message survives instead of 2>/dev/null.
    assert "versions.json" in result.stderr and "403" in result.stderr
    assert not (root / "versions.json").exists(), "a failed read must not republish the index"


def test_index_lives_under_the_prefix_not_the_bucket_root(tmp_path):
    # The mirror bucket's root is another organization's namespace -- the index
    # belongs beside the minors, and stand_up.sh reads it there (dist_root()).
    # Asserted on the s3:// ARGUMENTS rather than as a substring of the log, so
    # the test states the destination instead of relying on how the line reads.
    root, log = _run(tmp_path)
    targets = {arg for ln in log.splitlines() for arg in ln.split() if arg.startswith("s3://")}
    index = f"s3://{MIRROR_BUCKET}/{MIRROR_PREFIX}/versions.json"
    assert {t for t in targets if t.endswith("/versions.json")} == {index}
    assert (root / "versions.json").exists()


def test_every_upload_hands_the_object_to_the_bucket_owner(tmp_path):
    # Source Cooperative requires x-amz-acl: bucket-owner-full-control on every
    # write (issue #495 phase 1); without it the first PUT is AccessDenied. It
    # must reach the index too, not just the zips -- the index is re-PUT on
    # every release.
    _, log = _run(tmp_path)
    uploads = [ln for ln in log.splitlines() if not ln.split()[2].startswith("s3://")]
    assert len(uploads) == 6, f"expected 4 zips + SHA256SUMS + versions.json, got: {uploads}"
    for line in uploads:
        assert "--acl bucket-owner-full-control" in line, f"ACL missing on upload: {line}"
    # ...and never on the READ: `aws s3 cp s3://... ./versions.json` takes no ACL.
    downloads = [ln for ln in log.splitlines() if ln.split()[2].startswith("s3://")]
    assert downloads and all("--acl" not in ln for ln in downloads)


def test_the_scripts_published_bucket_list_matches_the_fleets():
    # Fold review: the list lives in two places -- here and in
    # zagg.store._PUBLISHED_BUCKETS -- and the drift is silent in the expensive
    # direction. If the mirror moves (or a second published bucket is added on
    # the fleet's side) this script's `case` stops matching, $ACL stays empty,
    # and the first PUT is AccessDenied at release time, on a tag, after PyPI
    # has already published. The ACL half is exactly what issue #496 established
    # you cannot get wrong by halves.
    from zagg.store import _PUBLISHED_BUCKETS

    match = re.search(r'^PUBLISHED_BUCKETS="([^"]*)"', SCRIPT.read_text(), re.MULTILINE)
    assert match, f"PUBLISHED_BUCKETS not found in {SCRIPT}"
    assert set(match.group(1).split()) == set(_PUBLISHED_BUCKETS)


def test_no_acl_against_a_bucket_we_own(tmp_path):
    # Keyed on the destination, mirroring zagg.store._PUBLISHED_BUCKETS: the
    # release role holds s3:PutObjectAcl on the mirror ONLY, so sending the
    # header at the in-account dist bucket would 403 rather than help.
    root, log = _run(tmp_path, bucket="sliderule-public-cors", prefix="")
    assert "--acl" not in log
    assert (root / "0.3" / "SHA256SUMS").exists()
    assert (root / "versions.json").exists()


def test_refuses_a_published_bucket_without_a_prefix(tmp_path):
    # Fold review: the only thing keeping a release out of the mirror bucket's
    # root -- another organization's namespace, and outside the grant -- was the
    # workflow's `vars.LAMBDA_DIST_PREFIX != ''` gate. Flipping LAMBDA_DIST_BUCKET
    # to the mirror is one `gh variable set`, so the script holds the invariant
    # itself and the workflow gate is defence in depth. Refused before any upload:
    # no --dir contents are needed to reach it.
    result = subprocess.run(
        ["bash", str(SCRIPT), "--minor", "0.3", "--bucket", MIRROR_BUCKET, "--dir", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "--prefix is required" in result.stderr


def test_errors_when_zip_count_wrong(tmp_path):
    if not shutil.which("sha256sum"):
        pytest.skip("sha256sum not available")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "aws").write_text("#!/bin/bash\nexit 0\n")
    (bindir / "aws").chmod(0o755)
    zips = tmp_path / "zips"
    zips.mkdir()
    (zips / "lambda_layer_arm64.zip").write_bytes(b"x")  # only 1 of 4
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}
    result = subprocess.run(
        ["bash", str(SCRIPT), "--minor", "0.3", "--bucket", "b", "--dir", str(zips)],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "expected 4 zips" in result.stderr


# --- publish.yml release-job coupling (issue #497 phase 2) ------------------
# `deploy-prod` publish-layer-versions straight out of what `distribute` staged,
# so retargeting the distribution destination silently breaks the prod deploy
# unless BOTH jobs follow it. These pin the coupling itself rather than the
# literal destination, which is a repo-variable and moves with the mirror.
#
# Fold review: they pin the *use* of each destination variable, not only its
# declaration -- binding DIST_PREFIX in `env` and leaving `--layer-key` alone is
# the likeliest way to half-apply the wiring, and it points publish-layer-version
# at an un-prefixed key that is outside the grant.


def _publish_jobs():
    import yaml

    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _step_with(job, needle):
    step = next((s for s in job["steps"] if needle in s.get("run", "")), None)
    assert step is not None, f"no step of {job.get('name', '?')} runs {needle}"
    return step


def _arg(run, flag):
    """The quoted value ``flag`` is given in a shell invocation ("" if absent)."""
    match = re.search(rf'{re.escape(flag)}\s+"([^"]*)"', run)
    return match.group(1) if match else ""


def test_prod_deploy_gates_on_everything_distribute_does():
    # The "no unstaged-layer deploy" safety rests on distribute's skip
    # propagating, so a var that can make distribute skip must make deploy-prod
    # skip too -- a partial config must skip, never half-deploy prod.
    jobs = _publish_jobs()
    gates = {
        name: set(re.findall(r"vars\.(\w+)", jobs[name]["if"]))
        for name in jobs
        if "if" in jobs[name]
    }
    assert gates["distribute"], "distribute is ungated -- a release with no AWS config would fail"
    assert gates["distribute"] <= gates["deploy-prod"]


def test_prod_deploy_carries_every_destination_var_distribute_uses():
    # Whatever LAMBDA_DIST_* distribute writes to, deploy-prod must read from --
    # otherwise the layer key it publishes points at the old destination.
    jobs = _publish_jobs()
    dist = _step_with(jobs["distribute"], "distribute_zips.sh")["run"]
    prod = _step_with(jobs["deploy-prod"], "deploy_lambda.sh")
    written = {var for var in re.findall(r"vars\.(\w+)", dist) if var.startswith("LAMBDA_DIST_")}
    # env NAME -> the vars.* it is bound to, so the binding can be followed into
    # the command rather than stopping at the `env` block.
    bound = {name: set(re.findall(r"vars\.(\w+)", str(v))) for name, v in prod["env"].items()}
    read = set().union(*bound.values())
    assert written and written <= read, f"deploy-prod does not follow {written - read}"
    # Bound is not used: a destination variable declared in `env` and never
    # referenced by the command leaves the deploy pointing at the old layout.
    for name, sources in bound.items():
        if sources & written:
            assert re.search(rf"\$\{{?{name}\b", prod["run"]), (
                f"deploy-prod binds {name} but its command never uses it, so the "
                f"layer it publishes does not follow {sorted(sources & written)}"
            )
    # ...and the prefix distribute writes UNDER has to reach the layer KEY
    # specifically (the bucket reaches --layer-bucket instead). Vacuous until the
    # workflow half of phase 2 lands and `distribute` grows its --prefix.
    prefix_vars = set(re.findall(r"vars\.(\w+)", _arg(dist, "--prefix")))
    layer_key = _arg(prod["run"], "--layer-key")
    for name, sources in bound.items():
        if sources & prefix_vars:
            assert name in layer_key, (
                f"deploy-prod's --layer-key ({layer_key!r}) is not built from {name}, so "
                f"publish-layer-version reads the un-prefixed key distribute never wrote"
            )
