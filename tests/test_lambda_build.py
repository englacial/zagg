"""Tests for Lambda build system: import resolution and size constraints.

These tests verify that:
1. All imports needed by lambda_handler.py are available
2. The function code build script works and produces output within size budget
3. The zagg package can be imported as Lambda would see it
"""

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent

# AWS Lambda limits
LAMBDA_UNZIPPED_LIMIT = 250 * 1024 * 1024  # 250MB combined (layer + function)

# Budget allocation — layer gets most of the space, function code should be small
FUNCTION_SIZE_BUDGET = 30 * 1024 * 1024  # 30MB for function code


class TestLambdaImports:
    """Verify all imports needed by the Lambda handler are available."""

    def test_handler_direct_imports(self):
        """lambda_handler.py top-level imports must all resolve."""
        import json  # noqa: F401
        import logging  # noqa: F401
        import os  # noqa: F401
        from typing import Any, Dict  # noqa: F401

        from obstore.auth.boto3 import Boto3CredentialProvider  # noqa: F401
        from obstore.store import S3Store  # noqa: F401
        from zarr.storage import ObjectStore  # noqa: F401

    def test_handler_zagg_imports(self):
        """zagg.processing imports used by lambda_handler must resolve."""
        from zagg.processing import process_morton_cell, write_dataframe_to_zarr  # noqa: F401

    def test_zagg_schema_imports(self):
        """zagg.schema imports used transitively must resolve."""
        from zagg.schema import xdggs_spec, xdggs_zarr_template  # noqa: F401

    def test_zarr_codecs_available(self):
        """Zarr codecs (numcodecs/blosc) must be importable for data writing."""
        import numcodecs  # noqa: F401

    def test_pydantic_zarr_available(self):
        """pydantic-zarr is needed for Zarr template creation."""
        import pydantic_zarr  # noqa: F401

    def test_pyyaml_available(self):
        """pyyaml is needed for config loading."""
        import yaml  # noqa: F401

    def test_h5coro_available(self):
        """h5coro is needed for reading HDF5 from S3."""
        import h5coro  # noqa: F401

    def test_mortie_available(self):
        """mortie is needed for morton code operations."""
        import mortie  # noqa: F401


class TestFunctionBuild:
    """Test that the function code build script works correctly."""

    @pytest.fixture
    def build_script(self):
        return REPO_ROOT / "deployment" / "aws" / "build_function.sh"

    def test_build_script_exists(self, build_script):
        assert build_script.exists(), f"Build script missing: {build_script}"

    def test_build_script_executable(self, build_script):
        assert build_script.stat().st_mode & 0o111, "build_function.sh is not executable"

    def test_layer_build_script_exists(self):
        script = REPO_ROOT / "deployment" / "aws" / "build_layer_v14.sh"
        assert script.exists(), f"Layer build script missing: {script}"

    def test_arm64_build_script_exists(self):
        script = REPO_ROOT / "deployment" / "aws" / "build_arm64_layer.sh"
        assert script.exists(), f"ARM64 build script missing: {script}"

    @pytest.mark.slow
    def test_function_build_succeeds(self, build_script, tmp_path):
        """Run the function build and verify it completes successfully.

        This test is slow (~30s) because it downloads and installs packages.
        Run with: pytest -m slow
        """
        result = subprocess.run(
            ["bash", str(build_script)],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=REPO_ROOT,
        )
        assert result.returncode == 0, (
            f"Build failed:\nstdout: {result.stdout[-2000:]}\nstderr: {result.stderr[-2000:]}"
        )

        # Verify output zip was created
        builds_dir = REPO_ROOT / "deployment" / "builds"
        zips = list(builds_dir.glob("lambda_function_*.zip"))
        assert len(zips) > 0, f"No function zip found in {builds_dir}"

    @pytest.mark.slow
    def test_function_build_size(self, build_script):
        """Function code zip must fit within size budget.

        Run with: pytest -m slow
        """
        builds_dir = REPO_ROOT / "deployment" / "builds"
        zips = list(builds_dir.glob("lambda_function_*.zip"))
        if not zips:
            pytest.skip("No build artifact found — run test_function_build_succeeds first")

        # Check unzipped size by extracting to temp dir
        import tempfile
        import zipfile

        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(zips[0]) as zf:
                zf.extractall(tmp)
            # Sum all file sizes
            total = sum(f.stat().st_size for f in Path(tmp).rglob("*") if f.is_file())

        assert total < FUNCTION_SIZE_BUDGET, (
            f"Function code {total / 1024 / 1024:.1f}MB exceeds "
            f"{FUNCTION_SIZE_BUDGET / 1024 / 1024:.0f}MB budget"
        )


class TestLambdaHandlerSyntax:
    """Verify the Lambda handler file is valid Python."""

    def test_handler_parses(self):
        """lambda_handler.py must be valid Python."""
        handler = REPO_ROOT / "deployment" / "aws" / "lambda_handler.py"
        assert handler.exists()
        compile(handler.read_text(), str(handler), "exec")

    def test_handler_has_entry_point(self):
        """lambda_handler.py must define lambda_handler function."""
        handler = REPO_ROOT / "deployment" / "aws" / "lambda_handler.py"
        source = handler.read_text()
        assert "def lambda_handler(" in source

    def test_invoke_script_parses(self):
        """invoke_lambda.py must be valid Python."""
        invoker = REPO_ROOT / "deployment" / "aws" / "invoke_lambda.py"
        assert invoker.exists()
        compile(invoker.read_text(), str(invoker), "exec")


class TestPackageConsistency:
    """Verify dependency specifications are consistent across build scripts."""

    @staticmethod
    def _lambda_extra_pin(pkg):
        import tomllib

        config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        dep = next(d for d in config["project"]["optional-dependencies"]["lambda"]
                   if d.startswith(f"{pkg}=="))
        return dep.split("==", 1)[1]

    @staticmethod
    def _pinned_in_script(pkg, script_path):
        import re

        match = re.search(rf'{re.escape(pkg)}==([0-9][^\s"\']+)', script_path.read_text())
        return match.group(1) if match else None

    def _assert_lockstep(self, pkg, script_name):
        extra_pin = self._lambda_extra_pin(pkg)
        script_pin = self._pinned_in_script(pkg, REPO_ROOT / "deployment" / "aws" / script_name)
        assert script_pin, f"{script_name} does not pin {pkg}"
        assert script_pin == extra_pin, (
            f"{script_name} pins {pkg}=={script_pin} but [lambda] extra pins {pkg}=={extra_pin}"
        )

    def test_numpy_version_consistent(self):
        """Lambda [extra] and ARM64 build script must pin the same numpy version."""
        self._assert_lockstep("numpy", "build_arm64_layer.sh")

    def test_pandas_version_consistent(self):
        """Lambda [extra] and both build scripts must pin the same pandas version."""
        self._assert_lockstep("pandas", "build_layer_v14.sh")
        self._assert_lockstep("pandas", "build_arm64_layer.sh")

    def test_h5coro_version_consistent(self):
        """Lambda [extra] and both build scripts must pin the same h5coro version."""
        self._assert_lockstep("h5coro", "build_layer_v14.sh")
        self._assert_lockstep("h5coro", "build_arm64_layer.sh")
