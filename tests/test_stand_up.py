"""Test the backend standup script (deployment/aws/stand_up.sh) — issue #174.

Same harness as test_distribute_zips.py: run the real script with a stub
``aws`` on PATH (no network, nothing deployed) and assert the artifact
resolution logic. The stale-defaults failure mode (2026-07-06 forensics) was a
version derived from git state that was never staged, silently falling back to
the retired source.coop mirror and dying as a CloudFormation NoSuchKey — so
the load-bearing cases are: an unstaged minor fails fast (before any
``cloudformation deploy``) with the staged minors listed, a staged minor
deploys from the distribution bucket's ``<minor>/<zip>`` layout, and the
confirm prompt gates the stack call unless ``--yes`` is passed.
"""

import json
import os
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "deployment" / "aws" / "stand_up.sh"

# A stub `aws` CLI: logs every call, serves `s3api head-object` from the files
# under $STAGED_DIR (staged release artifacts), streams a seeded versions.json
# for `s3 cp s3://.../versions.json -`, and no-ops everything else (including
# `cloudformation deploy` — the log is how tests assert it ran or didn't).
STUB_AWS = """#!/bin/bash
set -euo pipefail
echo "$*" >> "$AWS_LOG"
if [ "$1" = "s3api" ] && [ "$2" = "head-object" ]; then
  key=""
  while [ $# -gt 0 ]; do
    case "$1" in --key) key="$2"; shift 2 ;; *) shift ;; esac
  done
  [ -f "$STAGED_DIR/$key" ] || exit 254
  exit 0
fi
if [ "$1" = "s3" ] && [ "$2" = "cp" ] && [ "${4:-}" = "-" ]; then
  [ -f "$SEED_DIR/versions.json" ] || exit 1
  cat "$SEED_DIR/versions.json"
  exit 0
fi
exit 0
"""


def _run(tmp_path, *args, env_extra=None, stdin="", staged=("0.14",), versions=None, drop=None):
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    (bindir / "aws").write_text(STUB_AWS)
    (bindir / "aws").chmod(0o755)

    staged_dir = tmp_path / "staged"
    for minor in staged:
        d = staged_dir / minor
        d.mkdir(parents=True, exist_ok=True)
        (d / "lambda_layer_arm64.zip").write_bytes(b"dummy")
        (d / "lambda_function_arm64_py312.zip").write_bytes(b"dummy")
    if drop is not None:  # simulate a partially staged minor
        (staged_dir / drop).unlink()

    seed_dir = tmp_path / "seed"
    seed_dir.mkdir(exist_ok=True)
    if versions is None:
        versions = {"minors": list(staged), "latest": staged[-1] if staged else None}
    (seed_dir / "versions.json").write_text(json.dumps(versions))

    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "AWS_LOG": str(tmp_path / "aws.log"),
        "STAGED_DIR": str(staged_dir),
        "SEED_DIR": str(seed_dir),
        "OUTPUT_BUCKET": "my-results",
        **(env_extra or {}),
    }
    result = subprocess.run(
        ["bash", str(SCRIPT), *args],
        env=env,
        input=stdin,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    log_path = tmp_path / "aws.log"
    log = log_path.read_text() if log_path.exists() else ""
    return result, log


def test_unstaged_minor_fails_before_any_stack_call(tmp_path):
    # The 2026-07-06 failure mode: a derived-but-never-staged minor (0.15) must
    # die at the HEAD check with the staged minors listed — NOT reach
    # CloudFormation and roll back on NoSuchKey.
    result, log = _run(
        tmp_path,
        "--yes",
        env_extra={"LAMBDA_VERSION": "0.15"},
        staged=("0.14",),
        versions={"minors": ["0.13", "0.14"], "latest": "0.14"},
    )
    assert result.returncode != 0
    assert "not staged" in result.stdout
    assert "0.13 0.14" in result.stdout  # actionable: the staged minors
    assert "LAMBDA_VERSION" in result.stdout
    assert "cloudformation" not in log


def test_partially_staged_minor_fails_before_any_stack_call(tmp_path):
    # Both keys are HEAD-verified (review fold): a minor with the layer staged
    # but the function zip missing must still fail before any stack call.
    result, log = _run(
        tmp_path,
        "--yes",
        env_extra={"LAMBDA_VERSION": "0.14"},
        drop="0.14/lambda_function_arm64_py312.zip",
    )
    assert result.returncode != 0
    assert "not staged" in result.stdout
    assert "lambda_function_arm64_py312.zip" in result.stdout
    assert "cloudformation" not in log


def test_staged_minor_deploys_from_distribution_bucket(tmp_path):
    # Happy path: the current release layout (sliderule-public-cors/<minor>/<zip>)
    # is what reaches `cloudformation deploy` — no source.coop anywhere.
    result, log = _run(tmp_path, "--yes", env_extra={"LAMBDA_VERSION": "0.14"})
    assert result.returncode == 0, result.stdout + result.stderr
    (deploy,) = [ln for ln in log.splitlines() if ln.startswith("cloudformation deploy")]
    assert "ArtifactBucket=sliderule-public-cors" in deploy
    assert "LayerS3Key=0.14/lambda_layer_arm64.zip" in deploy
    assert "FunctionS3Key=0.14/lambda_function_arm64_py312.zip" in deploy
    assert "source.coop" not in log and "source.coop" not in result.stdout
    # The resolved artifacts are echoed before the stack call (issue #174).
    assert "s3://sliderule-public-cors/0.14/lambda_layer_arm64.zip" in result.stdout


def test_latest_resolves_from_versions_index(tmp_path):
    result, log = _run(
        tmp_path,
        "--yes",
        env_extra={"LAMBDA_VERSION": "latest"},
        staged=("0.13", "0.14"),
        versions={"minors": ["0.13", "0.14"], "latest": "0.14"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "LayerS3Key=0.14/lambda_layer_arm64.zip" in log


def test_confirm_prompt_aborts_without_yes(tmp_path):
    # No --yes and a "n" answer: the script echoes the resolved artifacts and
    # stops before any deploy.
    result, log = _run(tmp_path, env_extra={"LAMBDA_VERSION": "0.14"}, stdin="n\n")
    assert result.returncode != 0
    assert "Aborted" in result.stdout
    assert "cloudformation" not in log


def test_confirm_prompt_proceeds_on_y(tmp_path):
    result, log = _run(tmp_path, env_extra={"LAMBDA_VERSION": "0.14"}, stdin="y\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "cloudformation deploy" in log


def test_no_input_and_no_yes_aborts(tmp_path):
    # Unattended run without --yes (EOF on stdin) must fail safe, not deploy —
    # and still print the actionable abort message (review fold: a bare `read`
    # under `set -e` used to exit on EOF before reaching the message).
    result, log = _run(tmp_path, env_extra={"LAMBDA_VERSION": "0.14"}, stdin="")
    assert result.returncode != 0
    assert "Aborted (pass --yes" in result.stdout
    assert "cloudformation" not in log


def test_unknown_flag_rejected(tmp_path):
    result, log = _run(tmp_path, "--force", env_extra={"LAMBDA_VERSION": "0.14"})
    assert result.returncode != 0
    assert "unknown argument" in result.stdout
    assert "cloudformation" not in log
