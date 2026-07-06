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

    def test_h5coro_hidefix_available(self):
        """h5coro-hidefix ships the compiled reader for the sidecar backend (issue #149).

        importorskip, not a bare import: the pinned 0.2.0 is not on PyPI until
        upstream cuts that release, so an env that could not install it yet
        skips here instead of failing the whole suite.
        """
        pytest.importorskip("h5coro_hidefix")


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
        script = REPO_ROOT / "deployment" / "aws" / "build_layer.sh"
        assert script.exists(), f"Layer build script missing: {script}"

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


class TestTemplateEnvironment:
    """The CloudFormation template must wire the glibc allocator tunables (#143).

    Set as Lambda ``Environment`` variables (they take effect at libc init, so a
    runtime ``mallopt``/``malloc_trim`` from Python is not enough), driven by
    CloudFormation Parameters so they stay tunable without a template edit.
    """

    @staticmethod
    def _load_template():
        import yaml

        class _CfnLoader(yaml.SafeLoader):
            pass

        def _cfn_multi(loader, tag_suffix, node):
            # Treat CloudFormation short-form intrinsics (!Ref, !Sub, !GetAtt,
            # !If, ...) as generic {TagSuffix: value} mappings so PyYAML can
            # parse the template without choking on the unknown tags.
            if isinstance(node, yaml.ScalarNode):
                return {tag_suffix: loader.construct_scalar(node)}
            if isinstance(node, yaml.SequenceNode):
                return {tag_suffix: loader.construct_sequence(node)}
            return {tag_suffix: loader.construct_mapping(node)}

        _CfnLoader.add_multi_constructor("!", _cfn_multi)
        template = REPO_ROOT / "deployment" / "aws" / "template.yaml"
        return yaml.load(template.read_text(), Loader=_CfnLoader)

    def test_process_fn_carries_malloc_tunables(self):
        tpl = self._load_template()
        env = tpl["Resources"]["ProcessFn"]["Properties"]["Environment"]["Variables"]
        assert env["MALLOC_ARENA_MAX"] == {"Ref": "MallocArenaMax"}
        assert env["MALLOC_TRIM_THRESHOLD_"] == {"Ref": "MallocTrimThreshold"}

    def test_malloc_parameters_have_expected_defaults(self):
        params = self._load_template()["Parameters"]
        assert params["MallocArenaMax"]["Default"] == "2"
        assert params["MallocTrimThreshold"]["Default"] == "0"

    def test_self_recycle_env_defaults(self):
        # issue #171: the worker self-recycle knobs ride the function
        # environment with template defaults (1400 MB on the 2047 MB cap --
        # ~650 MB of headroom over the observed ~700-1100 MB/invocation
        # retention -- and a generation cap of 1: recycle after every
        # invocation, the cold-every-time posture; issue #175).
        tpl = self._load_template()
        params = tpl["Parameters"]
        assert params["RecycleRssMb"]["Default"] == "1400"
        assert params["RecycleMaxInvocations"]["Default"] == "1"
        env = tpl["Resources"]["ProcessFn"]["Properties"]["Environment"]["Variables"]
        assert env["ZAGG_RECYCLE_RSS_MB"] == {"Ref": "RecycleRssMb"}
        assert env["ZAGG_RECYCLE_MAX_INVOCATIONS"] == {"Ref": "RecycleMaxInvocations"}

    def test_execution_role_grants_zagg_index_store(self):
        # issue #160: inline write-back + sidecar reads target the public
        # zagg-index prefix; the shared execution role gets Get/Put scoped to
        # exactly that prefix (never the bucket), in BOTH copies of the role
        # (inline ExecutionRole here, admin-created execution_role.yaml).
        arn = "arn:aws:s3:::sliderule-public-cors/zagg-index/*"

        def _index_statements(role_props):
            stmts = role_props["Policies"][0]["PolicyDocument"]["Statement"]
            return [s for s in stmts if s.get("Resource") == arn]

        tpl_role = self._load_template()["Resources"]["ExecutionRole"]["Properties"]
        matches = _index_statements(tpl_role)
        assert len(matches) == 1
        assert sorted(matches[0]["Action"]) == ["s3:GetObject", "s3:PutObject"]

        import yaml

        class _CfnLoader(yaml.SafeLoader):
            pass

        def _cfn_multi(loader, tag_suffix, node):
            if isinstance(node, yaml.ScalarNode):
                return {tag_suffix: loader.construct_scalar(node)}
            if isinstance(node, yaml.SequenceNode):
                return {tag_suffix: loader.construct_sequence(node)}
            return {tag_suffix: loader.construct_mapping(node)}

        _CfnLoader.add_multi_constructor("!", _cfn_multi)
        role_tpl = REPO_ROOT / "deployment" / "aws" / "execution_role.yaml"
        ext = yaml.load(role_tpl.read_text(), Loader=_CfnLoader)
        ext_role = ext["Resources"]["ExecutionRole"]["Properties"]
        ext_matches = _index_statements(ext_role)
        assert len(ext_matches) == 1
        assert sorted(ext_matches[0]["Action"]) == ["s3:GetObject", "s3:PutObject"]

    def test_extract_fn_mirrors_process_fn(self):
        # issue #148: extraction is both a mode of ProcessFn and a dedicated
        # twin function (own concurrency pool for full-archive runs). The twin
        # must stay in lockstep with ProcessFn -- same handler/code/layer/role/
        # timeout/memory/env -- differing only in FunctionName.
        resources = self._load_template()["Resources"]
        process = resources["ProcessFn"]["Properties"]
        extract = resources["ExtractFn"]["Properties"]
        assert extract["FunctionName"] == {"Sub": "${FunctionName}-extract"}
        for key in (
            "Handler",
            "Runtime",
            "Architectures",
            "MemorySize",
            "Timeout",
            "Role",
            "Layers",
            "Environment",
            "Code",
        ):
            assert extract[key] == process[key], f"ExtractFn.{key} diverges from ProcessFn"

    def test_extract_fn_async_config_mirrors_process_fn(self):
        # The twin's EventInvokeConfig must stay in lockstep too (issues #148 /
        # #151): without it, Lambda's async defaults would re-run a failed
        # extraction up to 2 more times at up to 900 s each.
        resources = self._load_template()["Resources"]
        process = dict(resources["ProcessFnAsyncConfig"]["Properties"])
        extract = dict(resources["ExtractFnAsyncConfig"]["Properties"])
        assert extract.pop("FunctionName") == {"Ref": "ExtractFn"}
        assert process.pop("FunctionName") == {"Ref": "ProcessFn"}
        assert extract == process  # Qualifier, retries, event age identical

    def test_async_event_invoke_config_pins_retries(self):
        # issue #151: the runner's async dispatch relies on service retries
        # being 0 (a re-run of a deterministic failure re-fails at extra cost
        # and can write into a store the caller has moved on from) and on the
        # event age staying UNDER the runner's poll margin, so no first
        # delivery can start after the runner stops listening (a late run
        # would write into the store post-finalize).
        from zagg.runner import _ASYNC_POLL_MARGIN_S

        props = self._load_template()["Resources"]["ProcessFnAsyncConfig"]["Properties"]
        assert props["FunctionName"] == {"Ref": "ProcessFn"}
        assert props["MaximumRetryAttempts"] == 0
        assert props["MaximumEventAgeInSeconds"] == 60  # API minimum
        assert props["MaximumEventAgeInSeconds"] < _ASYNC_POLL_MARGIN_S


class TestPackageConsistency:
    """Verify dependency specifications are consistent across build scripts."""

    @staticmethod
    def _lambda_extra_pin(pkg):
        import tomllib

        config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        dep = next(
            d
            for d in config["project"]["optional-dependencies"]["lambda"]
            if d.startswith(f"{pkg}==")
        )
        return dep.split("==", 1)[1]

    @staticmethod
    def _core_floor(pkg):
        import tomllib

        config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        dep = next(d for d in config["project"]["dependencies"] if d.startswith(f"{pkg}>="))
        return dep.split(">=", 1)[1]

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
        """Lambda [extra] and the build script must pin the same numpy version."""
        self._assert_lockstep("numpy", "build_layer.sh")

    def test_pandas_version_consistent(self):
        """Lambda [extra] and the build script must pin the same pandas version."""
        self._assert_lockstep("pandas", "build_layer.sh")

    def test_h5coro_version_consistent(self):
        """Lambda [extra] and the build script must pin the same h5coro version."""
        self._assert_lockstep("h5coro", "build_layer.sh")

    def test_h5coro_lambda_pin_satisfies_core_floor(self):
        """The [lambda] exact pin must not fall below the core h5coro floor."""
        from packaging.version import Version

        lambda_pin = Version(self._lambda_extra_pin("h5coro"))
        core_floor = Version(self._core_floor("h5coro"))
        assert lambda_pin >= core_floor, (
            f"[lambda] pins h5coro=={lambda_pin} but core requires h5coro>={core_floor}"
        )

    def test_h5coro_hidefix_version_consistent(self):
        """Lambda [extra] and the build script must pin the same h5coro-hidefix version."""
        self._assert_lockstep("h5coro-hidefix", "build_layer.sh")

    def test_h5coro_hidefix_lambda_pin_satisfies_core_floor(self):
        """The [lambda] exact pin must not fall below the core h5coro-hidefix floor."""
        from packaging.version import Version

        lambda_pin = Version(self._lambda_extra_pin("h5coro-hidefix"))
        core_floor = Version(self._core_floor("h5coro-hidefix"))
        assert lambda_pin >= core_floor, (
            f"[lambda] pins h5coro-hidefix=={lambda_pin} "
            f"but core requires h5coro-hidefix>={core_floor}"
        )
