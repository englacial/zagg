"""Test the shared in-place Lambda deploy script (.github/scripts/deploy_lambda.sh).

Runs the real script with a stub ``aws`` (no AWS) and asserts it issues the four
calls in the required order with the right function/layer wiring: publish a layer
version, point the function at it, wait, then update the code. The ordering (wait
between the config + code updates) and the layer-from-S3 wiring are the parts that
matter for a correct in-place deploy.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / ".github" / "scripts" / "deploy_lambda.sh"

# Stub `aws`: log the full arg line; emit a LayerVersionArn on stdout for the
# publish-layer-version call (the script captures it).
STUB_AWS = """#!/bin/bash
echo "$*" >> "$AWS_LOG"
if [ "$2" = "publish-layer-version" ]; then
  echo "arn:aws:lambda:us-west-2:1:layer:demo-deps:7"
fi
exit 0
"""


def test_deploy_sequence(tmp_path):
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "aws").write_text(STUB_AWS)
    (bindir / "aws").chmod(0o755)
    fn_zip = tmp_path / "lambda_function_arm64_py312.zip"
    fn_zip.write_bytes(b"zip")

    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "AWS_LOG": str(tmp_path / "aws.log"),
    }
    subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "--function",
            "process-shard-test",
            "--layer-bucket",
            "sliderule-public",
            "--layer-key",
            "lambda-test/abc/lambda_layer_arm64.zip",
            "--function-zip",
            str(fn_zip),
            "--region",
            "us-west-2",
        ],
        check=True,
        env=env,
    )
    log = (tmp_path / "aws.log").read_text().splitlines()
    # Four calls, in order.
    assert "lambda publish-layer-version" in log[0]
    assert "process-shard-test-deps" in log[0]  # layer named after the function
    assert "S3Key=lambda-test/abc/lambda_layer_arm64.zip" in log[0]
    assert "lambda update-function-configuration" in log[1]
    assert "arn:aws:lambda:us-west-2:1:layer:demo-deps:7" in log[1]  # uses published ARN
    assert "lambda wait function-updated" in log[2]  # settle before code update
    assert "lambda update-function-code" in log[3]
    assert f"fileb://{fn_zip}" in log[3]


def test_missing_required_arg_errors(tmp_path):
    result = subprocess.run(
        ["bash", str(SCRIPT), "--function", "f"],  # missing the rest
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
