"""Test the release distribution script (.github/scripts/distribute_zips.sh).

Runs the real script with a stub ``aws`` on PATH (no network), and asserts it
uploads the four zips + SHA256SUMS under the minor prefix and maintains the
top-level versions.json index. The versions.json read-modify-write is the part
with real logic, so it's covered against both the seed (absent) and merge paths.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / ".github" / "scripts" / "distribute_zips.sh"

# A stub `aws` CLI: logs every `s3 cp`, fails the versions.json *download* unless
# a seed exists in $SEED_DIR, and captures every *upload* into $CAPTURE_DIR so the
# test can read back what the script produced.
STUB_AWS = """#!/bin/bash
set -euo pipefail
if [ "$1" = "s3" ] && [ "$2" = "cp" ]; then
  SRC="$3"; DST="$4"
  echo "$SRC -> $DST" >> "$AWS_LOG"
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


def _run(tmp_path, *, seed_versions=None):
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
    subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "--minor",
            "0.3",
            "--tag",
            "0.3.1",
            "--bucket",
            "sliderule-public-cors",
            "--dir",
            str(zips),
        ],
        check=True,
        env=env,
        cwd=tmp_path,
    )
    log = (tmp_path / "aws.log").read_text()
    return capture, log


def test_uploads_four_zips_and_sums(tmp_path):
    capture, log = _run(tmp_path)
    for name in (
        "lambda_layer_arm64.zip",
        "lambda_layer_x86_64.zip",
        "lambda_function_arm64_py312.zip",
        "lambda_function_x86_64_py312.zip",
        "SHA256SUMS",
    ):
        assert (capture / "0.3" / name).exists(), f"{name} not uploaded under 0.3/"


def test_versions_index_seeds_when_absent(tmp_path):
    capture, _ = _run(tmp_path)  # no seed -> download fails -> seed {"minors": []}
    index = json.loads((capture / "versions.json").read_text())
    assert index["minors"] == ["0.3"]
    assert index["latest"] == "0.3"
    assert index["latest_tag"] == "0.3.1"


def test_versions_index_merges_and_sorts(tmp_path):
    capture, _ = _run(tmp_path, seed_versions={"minors": ["0.1", "0.10", "0.2"]})
    index = json.loads((capture / "versions.json").read_text())
    # New minor merged; sorted numerically (0.10 > 0.3, not lexically); latest correct.
    assert index["minors"] == ["0.1", "0.2", "0.3", "0.10"]
    assert index["latest"] == "0.10"


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
