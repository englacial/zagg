"""Test the release distribution script (.github/scripts/distribute_zips.sh).

Runs the real script with a stub ``aws`` on PATH (no network), and asserts it
uploads the four zips + SHA256SUMS under the minor prefix and maintains the
versions.json index at the destination root. The versions.json read-modify-write
is the part with real logic, so it's covered against both the seed (absent) and
merge paths.

Since issue #497 the destination is Source Cooperative under a key prefix, so
the ``--prefix`` layout and the ``bucket-owner-full-control`` ACL that Source
Cooperative requires (issue #495 phase 1) are covered too -- including the
in-account bucket case, where the ACL must NOT be sent.
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
# test can read back what the script produced.
STUB_AWS = """#!/bin/bash
set -euo pipefail
if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  SRC="$3"; DST="$4"
  echo "$*" >> "$AWS_LOG"
  if [[ "$SRC" == s3://* ]]; then
    # download: serve a seeded versions.json if present, else fail (not found).
    base="$(basename "$SRC")"
    if [ -f "$SEED_DIR/$base" ]; then cp "$SEED_DIR/$base" "$DST"; exit 0; fi
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


#: The release destination since issue #497: Source Cooperative, under the same
#: <prefix>/<minor>/<zip> layout publish_mirror.sh writes and stand_up.sh's
#: DIST_PREFIX already reads.
MIRROR_BUCKET = "us-west-2.opendata.source.coop"
MIRROR_PREFIX = "englacial/zagg/lambda"


def _run(tmp_path, *, seed_versions=None, bucket=MIRROR_BUCKET, prefix=MIRROR_PREFIX):
    """Run the script against the stub and return (destination root, aws log).

    The root is the captured tree under ``prefix``, so a caller asserting on
    ``root / "0.3" / ...`` is asserting the prefixed key, not just the tail.
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
    argv = ["bash", str(SCRIPT), "--minor", "0.3", "--tag", "0.3.1", "--bucket", bucket]
    if prefix:
        argv += ["--prefix", prefix]
    argv += ["--dir", str(zips)]
    subprocess.run(argv, check=True, env=env, cwd=tmp_path)
    log = (tmp_path / "aws.log").read_text()
    return capture / prefix if prefix else capture, log


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


def test_no_acl_against_a_bucket_we_own(tmp_path):
    # Keyed on the destination, mirroring zagg.store._PUBLISHED_BUCKETS: the
    # release role holds s3:PutObjectAcl on the mirror ONLY, so sending the
    # header at the in-account dist bucket would 403 rather than help.
    root, log = _run(tmp_path, bucket="sliderule-public-cors", prefix="")
    assert "--acl" not in log
    assert (root / "0.3" / "SHA256SUMS").exists()
    assert (root / "versions.json").exists()


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


def _publish_jobs():
    import yaml

    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _step_with(job, needle):
    return next(s for s in job["steps"] if needle in s.get("run", ""))


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
    written = {
        var
        for var in re.findall(
            r"vars\.(\w+)", _step_with(jobs["distribute"], "distribute_zips.sh")["run"]
        )
        if var.startswith("LAMBDA_DIST_")
    }
    read = {
        var
        for value in _step_with(jobs["deploy-prod"], "deploy_lambda.sh")["env"].values()
        for var in re.findall(r"vars\.(\w+)", str(value))
    }
    assert written and written <= read, f"deploy-prod does not follow {written - read}"
