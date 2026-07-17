"""Tests for Lambda build system: import resolution and size constraints.

These tests verify that:
1. All imports needed by lambda_handler.py are available
2. The function code build script works and produces output within size budget
3. The zagg package can be imported as Lambda would see it
"""

import re
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

    def test_process_fn_declares_runtime_env_vars(self):
        # Existence-only (espg call, 2026-07-07): values/defaults are
        # operator-tunable, and pinning them turned every tuning change into
        # a test edit. What this still guards is the #144 failure class --
        # an env var the deployed runtime consumes (glibc allocator tunables,
        # #143; worker self-recycle knobs, #171) silently never reaching the
        # function's Environment block.
        tpl = self._load_template()
        env = tpl["Resources"]["ProcessFn"]["Properties"]["Environment"]["Variables"]
        for var in (
            "MALLOC_ARENA_MAX",
            "MALLOC_TRIM_THRESHOLD_",
            "ZAGG_RECYCLE_RSS_MB",
            "ZAGG_RECYCLE_MAX_INVOCATIONS",
        ):
            assert var in env, f"ProcessFn.Environment must declare {var}"

    def test_execution_role_grants_public_cors_bucket(self):
        # The shared execution role gets Get/Put/Delete on the whole public
        # sliderule-public-cors bucket -- deliberate scope (espg, PR #176):
        # virtual-index write-back + sidecar reads (zagg-index/*, issue #160)
        # AND worker-written output zarr stores (e.g. zagg-examples/*) -- in
        # BOTH copies of the role (inline ExecutionRole here, admin-created
        # execution_role.yaml).
        arn = "arn:aws:s3:::sliderule-public-cors/*"
        actions = ["s3:DeleteObject", "s3:GetObject", "s3:PutObject"]

        def _index_statements(role_props):
            stmts = role_props["Policies"][0]["PolicyDocument"]["Statement"]
            return [s for s in stmts if s.get("Resource") == arn]

        tpl_role = self._load_template()["Resources"]["ExecutionRole"]["Properties"]
        matches = _index_statements(tpl_role)
        assert len(matches) == 1
        assert sorted(matches[0]["Action"]) == actions

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
        assert sorted(ext_matches[0]["Action"]) == actions

    def test_metric_filters_publish_recycle_error_split(self):
        # issue #175: under RecycleMaxInvocations=1 every async invocation
        # self-exits, so Lambda's raw Errors metric is pure noise. The
        # template publishes the real-vs-expected split to zagg/lambda, per
        # function, gated on CreateLogMetricFilters (fresh stacks: Lambda
        # creates the implicit log groups only on first invocation, and
        # MetricFilter requires the group to exist).
        tpl = self._load_template()
        assert tpl["Parameters"]["CreateLogMetricFilters"]["Default"] == "true"
        assert tpl["Conditions"]["ShouldCreateMetricFilters"] == {
            "Equals": [{"Ref": "CreateLogMetricFilters"}, "true"]
        }
        expected = {
            "ProcessSelfRecycleFilter": ("/aws/lambda/${FunctionName}", "ProcessSelfRecycleCount"),
            "ProcessWorkerErrorFilter": ("/aws/lambda/${FunctionName}", "ProcessWorkerErrorCount"),
            "ExtractSelfRecycleFilter": (
                "/aws/lambda/${FunctionName}-extract",
                "ExtractSelfRecycleCount",
            ),
            "ExtractWorkerErrorFilter": (
                "/aws/lambda/${FunctionName}-extract",
                "ExtractWorkerErrorCount",
            ),
        }
        for name, (group, metric) in expected.items():
            fltr = tpl["Resources"][name]
            assert fltr["Type"] == "AWS::Logs::MetricFilter"
            assert fltr["Condition"] == "ShouldCreateMetricFilters"
            props = fltr["Properties"]
            assert props["LogGroupName"] == {"Sub": group}
            (mt,) = props["MetricTransformations"]
            assert mt["MetricNamespace"] == "zagg/lambda"
            assert mt["MetricName"] == metric
            assert mt["MetricValue"] == "1"
            assert mt["DefaultValue"] == 0

    @staticmethod
    def _filter_matches(pattern, line):
        # Evaluator for the CloudWatch Logs term-filter subset the template
        # uses: quoted terms only; a leading ? on every term means OR,
        # otherwise all terms must appear in the line.
        terms = re.findall(r'(\??)"([^"]*)"', pattern)
        assert terms, f"unparsed filter pattern: {pattern!r}"
        # CloudWatch defines no mixed ?/plain term list; keep the template
        # within the uniform subset this evaluator models (review fold).
        assert len({q for q, _ in terms}) == 1, f"mixed ?/plain terms: {pattern!r}"
        any_mode = terms[0][0] == "?"
        hits = [t in line for _, t in terms]
        return any(hits) if any_mode else all(hits)

    def test_metric_filter_patterns_are_disjoint(self):
        # The recycle signature must NEVER count as a real error: a
        # self-recycle logs ZAGG_SELF_RECYCLE at [INFO] and exits 0, which
        # the runtime reports as "Runtime exited without providing a reason"
        # -- distinct from a real nonzero exit's "Runtime exited with error".
        res = self._load_template()["Resources"]
        recycle = res["ProcessSelfRecycleFilter"]["Properties"]["FilterPattern"]
        errors = res["ProcessWorkerErrorFilter"]["Properties"]["FilterPattern"]
        # The Extract twins carry byte-identical patterns.
        assert res["ExtractSelfRecycleFilter"]["Properties"]["FilterPattern"] == recycle
        assert res["ExtractWorkerErrorFilter"]["Properties"]["FilterPattern"] == errors

        recycle_lines = [
            # the handler's structured line (lambda_handler._maybe_self_recycle)
            "[INFO]\t2026-07-06T22:00:00Z\treq-1\t"
            "ZAGG_SELF_RECYCLE rss_mb=1450 generation=1 threshold=1",
            # the runtime's report for the recycle's clean os._exit(0)
            "RequestId: req-1 Error: Runtime exited without providing a reason Runtime.ExitError",
        ]
        error_lines = [
            "[ERROR]\t2026-07-06T22:00:00Z\treq-2\tFailed to write async result to s3://b/k: boom",
            "Traceback (most recent call last):",
            "2026-07-06T22:00:00Z req-3 Task timed out after 900.00 seconds",
            "REPORT RequestId: req-4\tStatus: error\tError Type: Runtime.OutOfMemory",
            "RequestId: req-5 Error: Runtime exited with error: exit status 1 Runtime.ExitError",
        ]
        assert self._filter_matches(recycle, recycle_lines[0])
        for line in recycle_lines:
            assert not self._filter_matches(errors, line)
        for line in error_lines:
            assert self._filter_matches(errors, line)
        # ordinary INFO traffic matches neither metric
        quiet = "[INFO]\t2026-07-06T22:00:00Z\treq-6\tLambda invocation started"
        assert not self._filter_matches(recycle, quiet)
        assert not self._filter_matches(errors, quiet)

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


class TestWorkerSizeVariants:
    """The Fn::ForEach worker-size variants must expand as ratified (issue #235).

    The template declares the 6 pre-provisioned variants (memories 2048/4096/
    8192, each with a default-512 and a -disk /tmp twin) via two
    ``Fn::ForEach`` loops under the ``AWS::LanguageExtensions`` transform.
    ``_expand_foreach`` renders those loops the way the transform does —
    textual ``${Identifier}`` substitution plus ``!Ref Identifier``
    replacement per collection value — so these tests pin the concrete
    function set (names, memories, /tmp sizes) without a deploy.
    """

    _SIZES = ("2048", "4096", "8192")
    _DISK_TMP = {"2048": 4096, "4096": 6144, "8192": 10240}
    _LAMBDA_EPHEMERAL_CEILING_MB = 10240

    @staticmethod
    def _expand_foreach(tpl, section):
        """Expand ``Fn::ForEach::*`` keys of ``section`` to concrete entries."""

        def _subst(node, ident, value):
            if isinstance(node, dict):
                if node == {"Ref": ident}:
                    return value
                return {_subst(k, ident, value): _subst(v, ident, value) for k, v in node.items()}
            if isinstance(node, list):
                return [_subst(item, ident, value) for item in node]
            if isinstance(node, str):
                return node.replace("${" + ident + "}", value)
            return node

        out = {}
        for key, node in section.items():
            if not key.startswith("Fn::ForEach::"):
                out[key] = node
                continue
            ident, collection, fragment = node
            if isinstance(collection, dict) and "Ref" in collection:
                collection = tpl["Parameters"][collection["Ref"]]["Default"].split(",")
            for value in collection:
                for frag_key, frag_val in fragment.items():
                    expanded_key = _subst(frag_key, ident, value)
                    assert expanded_key not in out, f"duplicate logical id {expanded_key}"
                    out[expanded_key] = _subst(frag_val, ident, value)
        return out

    @classmethod
    def _resolve_find_in_map(cls, tpl, node):
        map_name, top_key, second_key = node["FindInMap"]
        return tpl["Mappings"][map_name][top_key][second_key]

    def _expanded_resources(self):
        tpl = TestTemplateEnvironment._load_template()
        return tpl, self._expand_foreach(tpl, tpl["Resources"])

    def test_language_extensions_transform_declared(self):
        # Fn::ForEach only exists under the macro; without the Transform the
        # loops would deploy as (invalid) literal resources.
        tpl = TestTemplateEnvironment._load_template()
        assert tpl["Transform"] == "AWS::LanguageExtensions"

    def test_size_list_and_disk_mapping_stay_in_sync(self):
        # One source of truth for the sizes: the CommaDelimitedList default.
        # The -disk /tmp mapping must cover exactly those sizes.
        tpl = TestTemplateEnvironment._load_template()
        sizes = tuple(tpl["Parameters"]["WorkerMemorySizes"]["Default"].split(","))
        assert sizes == self._SIZES
        assert set(tpl["Mappings"]["WorkerDiskTmp"]) == set(sizes)

    def test_foreach_expands_to_six_variants(self):
        # The ratified matrix: 3 memories x {default 512 /tmp, -disk /tmp =
        # memory + 2048} -> 6 functions; the top -disk size sits exactly at
        # Lambda's EphemeralStorage ceiling (no clamping).
        tpl, resources = self._expanded_resources()
        for size in self._SIZES:
            std = resources[f"WorkerFn{size}"]["Properties"]
            assert std["FunctionName"] == {"Sub": f"${{FunctionName}}-{size}"}
            assert std["MemorySize"] == size
            assert "EphemeralStorage" not in std  # default 512 MB /tmp
            disk = resources[f"WorkerFn{size}Disk"]["Properties"]
            assert disk["FunctionName"] == {"Sub": f"${{FunctionName}}-{size}-disk"}
            assert disk["MemorySize"] == size
            tmp_mb = self._resolve_find_in_map(tpl, disk["EphemeralStorage"]["Size"])
            assert tmp_mb == self._DISK_TMP[size] == int(size) + 2048
        assert self._DISK_TMP["8192"] == self._LAMBDA_EPHEMERAL_CEILING_MB

    def test_variants_mirror_process_fn(self):
        # Same lockstep contract as ExtractFn (test_extract_fn_mirrors_
        # process_fn): variants share code/layer/role/timeout/env with
        # ProcessFn, differing only in FunctionName, MemorySize, and (disk
        # trio) EphemeralStorage.
        _, resources = self._expanded_resources()
        process = resources["ProcessFn"]["Properties"]
        for size in self._SIZES:
            for logical in (f"WorkerFn{size}", f"WorkerFn{size}Disk"):
                variant = resources[logical]["Properties"]
                for key in (
                    "Handler",
                    "Runtime",
                    "Architectures",
                    "Timeout",
                    "Role",
                    "Layers",
                    "Environment",
                    "Code",
                ):
                    assert variant[key] == process[key], f"{logical}.{key} diverges from ProcessFn"

    def test_variant_async_configs_mirror_process_fn(self):
        # issue #151 hygiene on every variant: retries 0, event age 60.
        _, resources = self._expanded_resources()
        process = dict(resources["ProcessFnAsyncConfig"]["Properties"])
        process.pop("FunctionName")
        for size in self._SIZES:
            for logical in (f"WorkerFn{size}", f"WorkerFn{size}Disk"):
                cfg = dict(resources[f"{logical}AsyncConfig"]["Properties"])
                assert cfg.pop("FunctionName") == {"Ref": logical}
                assert cfg == process  # Qualifier, retries, event age identical

    def test_variant_metric_filters_mirror_process_fn(self):
        # issue #175 split on every variant's log group, with per-function
        # metric names (unique across the whole template) and patterns
        # byte-identical to the unsuffixed function's.
        _, resources = self._expanded_resources()
        recycle = resources["ProcessSelfRecycleFilter"]["Properties"]["FilterPattern"]
        errors = resources["ProcessWorkerErrorFilter"]["Properties"]["FilterPattern"]
        for size in self._SIZES:
            for logical, group_suffix, metric_stem in (
                (f"WorkerFn{size}", f"-{size}", f"Worker{size}"),
                (f"WorkerFn{size}Disk", f"-{size}-disk", f"Worker{size}Disk"),
            ):
                for kind, pattern, metric in (
                    ("SelfRecycleFilter", recycle, f"{metric_stem}SelfRecycleCount"),
                    ("WorkerErrorFilter", errors, f"{metric_stem}WorkerErrorCount"),
                ):
                    fltr = resources[f"{logical}{kind}"]
                    assert fltr["Type"] == "AWS::Logs::MetricFilter"
                    assert fltr["Condition"] == "ShouldCreateMetricFilters"
                    assert fltr["DependsOn"] == logical
                    props = fltr["Properties"]
                    assert props["LogGroupName"] == {
                        "Sub": f"/aws/lambda/${{FunctionName}}{group_suffix}"
                    }
                    assert props["FilterPattern"] == pattern
                    (mt,) = props["MetricTransformations"]
                    assert mt["MetricNamespace"] == "zagg/lambda"
                    assert mt["MetricName"] == metric
                    assert mt["MetricValue"] == "1"
                    assert mt["DefaultValue"] == 0
        names = [
            fltr["Properties"]["MetricTransformations"][0]["MetricName"]
            for fltr in resources.values()
            if isinstance(fltr, dict) and fltr.get("Type") == "AWS::Logs::MetricFilter"
        ]
        assert len(names) == len(set(names)) == 16  # 8 functions x 2 filters

    def test_outputs_expose_variant_arns(self):
        tpl = TestTemplateEnvironment._load_template()
        outputs = self._expand_foreach(tpl, tpl["Outputs"])
        for size in self._SIZES:
            assert outputs[f"WorkerFn{size}Arn"]["Value"] == {"GetAtt": f"WorkerFn{size}.Arn"}
            assert outputs[f"WorkerFn{size}DiskArn"]["Value"] == {
                "GetAtt": f"WorkerFn{size}Disk.Arn"
            }


class TestLayerExtraParity:
    """The ``lambda`` extra pins and build_layer.sh must actually stay in sync.

    The script's comments say "keep the pin in sync with the lambda extra",
    but nothing enforced it: async-tiff (issue #218) was pinned in pyproject
    yet absent from the layer build, shipping a 0.27.0 layer whose
    ``mode="process_raster"`` worker died on ``No module named 'async_tiff'``.
    This pins the contract.
    """

    def test_every_lambda_extra_pin_is_in_build_layer(self):
        import tomllib

        pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        pins = pyproject["project"]["optional-dependencies"]["lambda"]
        script = (REPO_ROOT / "deployment" / "aws" / "build_layer.sh").read_text()
        missing = []
        for pin in pins:
            m = re.match(r"([A-Za-z0-9._-]+)==([A-Za-z0-9.]+)$", pin)
            if not m:  # unpinned entries (cramjam, astropy) aren't layer-exact
                continue
            if f'"{pin}"' not in script:
                missing.append(pin)
        assert not missing, (
            f"lambda-extra pins absent from deployment/aws/build_layer.sh: {missing} "
            "(the layer would ship without them — see issue #218's async-tiff gap)"
        )
