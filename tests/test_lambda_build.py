"""Tests for Lambda build system: import resolution and size constraints.

These tests verify that:
1. All imports needed by lambda_handler.py are available
2. The function code build script works and produces output within size budget
3. The zagg package can be imported as Lambda would see it
"""

import inspect
import json
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

        importorskip, not a bare import: the floor can sit ahead of what PyPI
        has published, so an env that could not install it yet skips here
        instead of failing the whole suite.

        The signature assertion pins the >=0.3.2 floor's reason: the worker
        forwards io_stats to every backend ungated (``worker.py`` read_kwargs,
        issue #374), and 0.3.1's read_group raised TypeError on every sidecar
        group read — silently, as a caught per-group error reported as "No data
        after filtering". Fails loudly wherever a below-floor hidefix installs.
        """
        pytest.importorskip("h5coro_hidefix")

        from h5coro_hidefix.zagg_backend import SidecarIndex

        assert "io_stats" in inspect.signature(SidecarIndex.read_group).parameters


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


def _statement_resources(statement):
    """A policy statement's Resource ARNs -- scalar or list, ``!Sub`` flattened.

    Flattening the one-key intrinsic matters: several of the role's statements
    build their ARN with ``!Sub``, which the loader below parses to
    ``{"Sub": "arn:..."}``. Skipping those would let a parameterized ARN --
    ``!Sub "arn:aws:s3:::${PublishBucket}/englacial/zagg/index/*"`` -- slip
    past the "nothing outside demo/" guard the helper exists to feed.
    """
    resource = statement.get("Resource")
    entries = resource if isinstance(resource, list) else [resource]
    arns = []
    for entry in entries:
        if isinstance(entry, str):
            arns.append(entry)
        elif isinstance(entry, dict) and len(entry) == 1:
            (body,) = entry.values()
            if isinstance(body, str):
                arns.append(body)
    return arns


def _statement_actions(statement):
    """A policy statement's Action entries, whether scalar or list.

    Normalizing first is load-bearing: ``"s3:Foo" in statement["Action"]`` is a
    substring test rather than a membership test when Action is a scalar
    string, and this template carries both shapes.
    """
    action = statement.get("Action")
    if isinstance(action, list):
        return action
    return [action] if action is not None else []


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
        # AND worker-written output zarr stores (e.g. zagg-examples/*). This is
        # the STAGING half of the identity model in issue #495; the published
        # half is asserted in test_execution_role_is_the_published_identity
        # (named, not "the next test" -- issue #502 slotted a bucket-level
        # ListBucket pin between them).
        arn = "arn:aws:s3:::sliderule-public-cors/*"
        role = self._load_template()["Resources"]["ExecutionRole"]["Properties"]
        stmts = role["Policies"][0]["PolicyDocument"]["Statement"]
        matches = [s for s in stmts if s.get("Resource") == arn]
        assert len(matches) == 1
        assert sorted(matches[0]["Action"]) == ["s3:DeleteObject", "s3:GetObject", "s3:PutObject"]

    def test_execution_role_lists_the_public_cors_bucket(self):
        # Bucket-level ListBucket is what makes S3 answer 404 NoSuchKey rather
        # than 403 AccessDenied on an absent key. The sidecar read path catches
        # the 404 ONLY -- obstore's NotFoundError subclasses FileNotFoundError,
        # and that ``return None`` is what selects on_miss=fallback -- so
        # without this grant a missing sidecar hard-errors where it should have
        # quietly taken the slow route (issue #502).
        role = self._load_template()["Resources"]["ExecutionRole"]["Properties"]
        stmts = role["Policies"][0]["PolicyDocument"]["Statement"]
        # Resource and Action both go through the normalizing helpers: the
        # grant is IAM-identical whether either is written as a scalar or a
        # one-element list, so neither shape should fail this spuriously --
        # matching on the raw Resource would report the grant MISSING when it
        # is present and correct. The list-of-one also keeps the bare-bucket
        # ARN pinned (not .../*, and not a superset).
        bucket = [
            s for s in stmts if _statement_resources(s) == ["arn:aws:s3:::sliderule-public-cors"]
        ]
        assert len(bucket) == 1, "the execution role must list sliderule-public-cors"
        assert _statement_actions(bucket[0]) == ["s3:ListBucket"]
        # An explicit Deny here would pass every other assertion in this test
        # while overriding the bucket policy's public ListBucket grant.
        assert bucket[0]["Effect"] == "Allow"
        # Unconditional on purpose, same reasoning as the source.coop grant: a
        # GetObject evaluation carries no s3:prefix context key, so a condition
        # on it never matches during a GET and every absent object 403s anyway.
        assert "Condition" not in bucket[0], (
            "an s3:prefix condition on ListBucket makes absent objects 403 "
            "instead of 404, which defeats on_miss=fallback on the sidecar path"
        )

    def test_execution_role_is_the_published_identity(self):
        # Issue #495 (revised): the fleet publishes to Source Cooperative as
        # ITSELF -- no assumable role, no injected credentials, no one-hour
        # role-chaining ceiling. zagg's data model is communal (one datacube,
        # appended by many), and staging vs published is a difference in store
        # MATURITY, not identity, so this is the same policy shape as the
        # sliderule-public-cors grant above pointed at a durable destination.
        tpl = self._load_template()
        stmts = tpl["Resources"]["ExecutionRole"]["Properties"]["Policies"][0]["PolicyDocument"][
            "Statement"
        ]

        published = "arn:aws:s3:::us-west-2.opendata.source.coop/englacial/zagg/demo/*"
        objects = [s for s in stmts if s.get("Resource") == published]
        assert len(objects) == 1, "the execution role must reach zagg's published prefix"
        # DeleteObject is deliberate: store overwrite and manifest cleanup need
        # it, and the bucket is versioned, so a delete leaves a marker rather
        # than destroying bytes. PutObjectAcl is load-bearing: every write to
        # this bucket carries x-amz-acl: bucket-owner-full-control, and S3
        # evaluates that against s3:PutObjectAcl on PutObject AND on
        # CreateMultipartUpload -- without it the first published PUT 403s.
        # The multipart pair covers the failure path PutObject does not:
        # obstore's own abort (it holds the UploadId) would otherwise 403 and
        # leak parts billed to Source Cooperative.
        assert sorted(objects[0]["Action"]) == [
            "s3:AbortMultipartUpload",
            "s3:DeleteObject",
            "s3:GetObject",
            "s3:ListMultipartUploadParts",
            "s3:PutObject",
            "s3:PutObjectAcl",
        ]
        # Scoped to englacial/zagg/demo/*, not englacial/zagg/* or englacial/*
        # (espg, 2026-08-20): the fleet publishes demo stores, while
        # englacial/zagg/lambda/* and englacial/zagg/benchmarks/* belong to the
        # CI release role under issue #497.
        assert not any(
            s.get("Resource")
            in (
                "arn:aws:s3:::us-west-2.opendata.source.coop/englacial/*",
                "arn:aws:s3:::us-west-2.opendata.source.coop/englacial/zagg/*",
            )
            for s in stmts
        )
        # PutObjectAcl is granted on the published prefix ONLY: the canned ACL
        # is not sent to buckets we own, and sliderule-public-cors' bucket
        # policy is not ours to change, so granting it there would be
        # unexplained privilege.
        acl_grants = [s for s in stmts if "s3:PutObjectAcl" in _statement_actions(s)]
        assert [s["Resource"] for s in acl_grants] == [published]

        # Nothing outside demo/ -- lambda/* and benchmarks/* belong to the CI
        # release role under issue #497, not to the fleet, and the sidecar
        # index cache is NOT moving here (espg, 2026-08-20: a different bucket
        # under a different org, post-MVP).
        reachable = {r for s in stmts for r in _statement_resources(s) if "source.coop/" in r}
        assert reachable == {published}

        bucket = [
            s for s in stmts if s.get("Resource") == "arn:aws:s3:::us-west-2.opendata.source.coop"
        ]
        assert len(bucket) == 1
        # s3:ListBucket ONLY. ListBucketMultipartUploads was dropped (espg,
        # 2026-08-20): it lists in-progress uploads bucket-wide and s3:prefix
        # cannot constrain it, so Source Cooperative's data-upload docs omit it
        # and their bucket policy does not grant it -- holding it here would be
        # denied cross-account anyway. Leaked parts age out on their 7-day
        # lifecycle rule; obstore's own abort (AbortMultipartUpload, granted on
        # the objects above) covers the failure path we can actually clean up.
        # Normalized through the helper on purpose: the point is the ABSENCE of
        # ListBucketMultipartUploads, so rewriting the grant as the
        # IAM-identical one-element list must not fail this spuriously.
        assert _statement_actions(bucket[0]) == ["s3:ListBucket"]
        # Unconditional on purpose (PR #496 review): S3 answers 404 for an
        # absent key only when the caller holds s3:ListBucket on the bucket,
        # and a GetObject evaluation carries no s3:prefix context key -- so an
        # s3:prefix condition would make every absent object 403, which zagg's
        # 404-only absence checks do not catch.
        assert "Condition" not in bucket[0], (
            "an s3:prefix condition on ListBucket makes absent objects 403 "
            "instead of 404, and zagg's absence checks catch 404 only"
        )

        # No assumable upload role survives from the first cut of phase 3.
        assert not any("SourceCoop" in name for name in tpl["Resources"])
        assert not any("SourceCoop" in name for name in tpl["Parameters"])

    def test_execution_role_name_is_a_stable_parameter(self):
        # The ARN becomes a cross-organization contract once Source Cooperative
        # names it in their bucket policy, so the role cannot keep a
        # CloudFormation-generated name. It must stay a PARAMETER, though:
        # zagg-backend-test is a second stack from this same template and a
        # hardcoded name would collide on CREATE.
        tpl = self._load_template()
        assert tpl["Parameters"]["ExecutionRoleName"]["Default"] == "zagg-lambda-execution"
        role = tpl["Resources"]["ExecutionRole"]
        assert role["Properties"]["RoleName"] == {"Ref": "ExecutionRoleName"}
        standup = (REPO_ROOT / "deployment" / "aws" / "stand_up.sh").read_text()
        # A named IAM role needs CAPABILITY_NAMED_IAM; tightening that back to
        # CAPABILITY_IAM would fail only against live AWS.
        assert "CAPABILITY_NAMED_IAM" in standup
        # ...and the name must be reachable from the standup path, or a second
        # stack in the same account cannot avoid the collision.
        assert 'ExecutionRoleName="$EXECUTION_ROLE_NAME"' in standup

    def test_execution_role_backdoor_is_gone(self):
        # espg's ruling (issue #495): CreateExecutionRole/ExecutionRoleArn was
        # a hatch for users without iam:CreateRole, it never worked, and
        # nothing used it. The supported posture is "an admin stands up the
        # template". Removal has to be complete -- a surviving !If or a stale
        # env var in stand_up.sh would silently reintroduce an unnamed role,
        # and an unnamed role breaks publishing on Source Cooperative's side.
        tpl = self._load_template()
        assert "CreateExecutionRole" not in tpl["Parameters"]
        assert "ExecutionRoleArn" not in tpl["Parameters"]
        assert "ShouldCreateRole" not in tpl["Conditions"]
        assert "Condition" not in tpl["Resources"]["ExecutionRole"]
        assert tpl["Outputs"]["RoleArn"]["Value"] == {"GetAtt": "ExecutionRole.Arn"}

        raw = (REPO_ROOT / "deployment" / "aws" / "template.yaml").read_text()
        for token in ("ShouldCreateRole", "ExecutionRoleArn", "CreateExecutionRole"):
            assert token not in raw, f"{token} survives in template.yaml"
        standup = (REPO_ROOT / "deployment" / "aws" / "stand_up.sh").read_text()
        for token in ("CREATE_ROLE", "ROLE_ARN", "CreateExecutionRole", "ExecutionRoleArn"):
            assert token not in standup, f"{token} survives in stand_up.sh"
        for stale in (
            REPO_ROOT / "deployment" / "aws" / "execution_role.yaml",
            REPO_ROOT / "deployment" / "aws" / "EXECUTION_ROLE.md",
            REPO_ROOT / "docs" / "deployment" / "execution-role.md",
        ):
            assert not stale.exists(), f"{stale.name} documents a path that no longer exists"
        # A nav entry pointing at a deleted page breaks the docs build.
        assert "execution-role.md" not in (REPO_ROOT / "mkdocs.yml").read_text()

    def test_every_function_uses_the_stack_role_directly(self):
        # The five !If [ShouldCreateRole, ...] sites are gone; each function
        # (base, -extract, and the Fn::ForEach worker variants) must reference
        # the role resource, not a parameter that no longer exists.
        tpl = self._load_template()
        roles = []
        for key, val in tpl["Resources"].items():
            if key.startswith("Fn::ForEach::"):
                for resource in val[2].values():
                    if resource.get("Type") == "AWS::Lambda::Function":
                        roles.append(resource["Properties"]["Role"])
            elif val.get("Type") == "AWS::Lambda::Function":
                roles.append(val["Properties"]["Role"])
        # Count, not just `all(...)`: a vacuous all() holds on a SUBSET, so
        # deleting a whole worker variant would leave this green. Five is what
        # the template yields -- base, -extract, and the three Fn::ForEach
        # blocks -- and five is the number of Role: sites the comment names.
        assert len(roles) == 5, roles
        assert all(r == {"GetAtt": "ExecutionRole.Arn"} for r in roles), roles

    def test_execution_role_permissions_are_fully_inline(self):
        # Salvaged from the deleted test_execution_role_gains_no_source_coop_access
        # (review finding): every assertion in this file reads the role's
        # inline Policies, and that reasoning is sound ONLY while the role has
        # no attached managed policy -- one could carry bucket access none of
        # these tests can see.
        role = self._load_template()["Resources"]["ExecutionRole"]
        assert "ManagedPolicyArns" not in json.dumps(role), (
            "the ExecutionRole attaches a managed policy -- its permissions are "
            "no longer fully described inline, so the assertions in this file "
            "can no longer see everything it grants"
        )

    def test_the_second_stack_command_overrides_the_role_name(self):
        # IAM role names are ACCOUNT-scoped, so the one in-repo command that
        # stands up a second stack must pass EXECUTION_ROLE_NAME or CREATE
        # fails with EntityAlreadyExists (review finding). stand_up.sh refuses
        # the combination up front; this pins that the documented command does
        # not hit that refusal.
        doc = (REPO_ROOT / "docs" / "deployment" / "benchmark-cicd.md").read_text()
        block = doc.split("## 8. The `process-shard-test` stack", 1)[1]
        block = block.split("```bash", 1)[1].split("```", 1)[0]
        assert "STACK_NAME=zagg-backend-test" in block
        assert "EXECUTION_ROLE_NAME=zagg-lambda-execution-test" in block
        standup = (REPO_ROOT / "deployment" / "aws" / "stand_up.sh").read_text()
        assert 'DEFAULT_EXECUTION_ROLE_NAME="zagg-lambda-execution"' in standup
        assert '[ "$STACK_NAME" != "$DEFAULT_STACK_NAME" ]' in standup

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
    ``_expand_foreach`` models the transform for the constructs this template
    uses — textual ``${Identifier}`` substitution plus ``!Ref Identifier``
    replacement per collection value, with the collection taken from the
    parameter's ``Default`` — so these tests pin the concrete function set
    (names, memories, /tmp sizes) for the default size list without a deploy.
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
                # Looped logical-id refs use the nested Fn::Sub form — the
                # ForEach transform does not substitute bare Ref strings
                # (issue #269; live-validated by the first real standup).
                assert cfg.pop("FunctionName") == {"Ref": {"Fn::Sub": logical}}
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
                # Test-env -disk variants (issue #272): the per-merge spill arm.
                (f"WorkerTestFn{size}Disk", f"-test-{size}-disk", f"WorkerTest{size}Disk"),
            ):
                for kind, pattern, metric in (
                    ("SelfRecycleFilter", recycle, f"{metric_stem}SelfRecycleCount"),
                    ("WorkerErrorFilter", errors, f"{metric_stem}WorkerErrorCount"),
                ):
                    fltr = resources[f"{logical}{kind}"]
                    assert fltr["Type"] == "AWS::Logs::MetricFilter"
                    assert fltr["Condition"] == "ShouldCreateMetricFilters"
                    # DependsOn forbids intrinsics, so looped filters order
                    # after their function implicitly instead: LogGroupName
                    # derives from Ref(function) == the function name
                    # (issue #269).
                    assert "DependsOn" not in fltr
                    props = fltr["Properties"]
                    assert props["LogGroupName"] == {
                        "Sub": [
                            "/aws/lambda/${WorkerName}",
                            {"WorkerName": {"Ref": {"Fn::Sub": logical}}},
                        ]
                    }
                    assert props["FilterPattern"] == pattern
                    (mt,) = props["MetricTransformations"]
                    assert mt["MetricNamespace"] == "zagg/lambda"
                    assert mt["MetricName"] == {"Sub": metric}
                    assert mt["MetricValue"] == "1"
                    assert mt["DefaultValue"] == 0
        names = [
            _n["Sub"] if isinstance(_n, dict) else _n
            for _n in (
                fltr["Properties"]["MetricTransformations"][0]["MetricName"]
                for fltr in resources.values()
                if isinstance(fltr, dict) and fltr.get("Type") == "AWS::Logs::MetricFilter"
            )
        ]
        assert len(names) == len(set(names)) == 22  # 11 functions x 2 (8 + 3 test-disk, #272)

    def test_outputs_expose_variant_arns(self):
        tpl = TestTemplateEnvironment._load_template()
        outputs = self._expand_foreach(tpl, tpl["Outputs"])
        for size in self._SIZES:
            # GetAtt on a looped logical id takes the two-element list form
            # with a nested Sub (issue #269) — the dotted-string form is not
            # substituted by the transform.
            assert outputs[f"WorkerFn{size}Arn"]["Value"] == {
                "GetAtt": [{"Sub": f"WorkerFn{size}"}, "Arn"]
            }
            assert outputs[f"WorkerFn{size}DiskArn"]["Value"] == {
                "GetAtt": [{"Sub": f"WorkerFn{size}Disk"}, "Arn"]
            }


class TestLayerExtraParity:
    """The ``lambda`` extra pins and build_layer.sh must actually stay in sync.

    The script's comments say "keep the pin in sync with the lambda extra",
    but nothing enforced it: async-tiff (issue #218) was pinned in pyproject
    yet absent from the layer build, shipping a 0.27.0 layer whose
    ``mode="process_raster"`` worker died on ``No module named 'async_tiff'``.
    This pins the contract.
    """

    # Distributions whose layer spec build_layer.sh *derives* from
    # ``[project.dependencies]`` instead of hard-coding (issue #322), keyed to the
    # shell fragment that does the deriving. No literal ``name==x.y.z`` string
    # exists for these, so the substring check below cannot apply — and the two
    # mechanisms are mutually exclusive: an exact pin added to the ``lambda`` extra
    # would not reach the layer, which the failure message has to say out loud.
    _DERIVED = {"mortie": "MORTIE_SPEC=$("}

    @staticmethod
    def _install_lines(script):
        """The script's ``$PIP install`` invocations, backslash continuations joined.

        Deriving a pin is only half the contract — it has to be handed to pip.
        The installs span continuation lines (``build_layer.sh:93-96``), so a
        raw per-line scan would miss most of them.
        """
        logical, buf = [], ""
        for line in script.splitlines():
            if line.endswith("\\"):
                buf += line[:-1]
                continue
            logical.append(buf + line)
            buf = ""
        if buf:
            logical.append(buf)
        return [line for line in logical if "$PIP install" in line]

    def test_every_lambda_extra_pin_is_in_build_layer(self):
        """Every exact ``lambda`` pin reaches the layer via ``lambda_pin`` derivation.

        Since PR #436 the script derives each exact pin from the extra at build
        time (extending issue #322's mortie pattern), so the contract inverts:
        every pinned name must be *fetched* via ``$(lambda_pin name)``, and no
        literal ``name==x.y.z`` may exist in the script in any quoting — a
        literal is a second declaration site, exactly the drift this closes.

        Deriving alone is not enough: the assignment block sits far from the
        installs, so a pin can be fetched and never passed to pip — the issue
        #218 async-tiff gap in a new costume. The name -> shell-var mapping is
        parsed out of the script (not hard-coded) so renaming a var cannot
        quietly drop its install check, and each var must appear quoted on a
        ``$PIP install`` line.
        """
        import tomllib

        pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        pins = pyproject["project"]["optional-dependencies"]["lambda"]
        script = (REPO_ROOT / "deployment" / "aws" / "build_layer.sh").read_text()
        pin_vars = {
            name: var
            for var, name in re.findall(
                r"^([A-Z0-9_]+_PIN)=\$\(lambda_pin (.+)\)$", script, re.MULTILINE
            )
        }
        installs = "\n".join(self._install_lines(script))
        missing = []
        uninstalled = []
        literal = []
        derived = []
        for pin in pins:
            m = re.match(r"([A-Za-z0-9._-]+)==([A-Za-z0-9.]+)$", pin)
            if not m:  # unpinned entries (cramjam, astropy) aren't layer-exact
                continue
            name = m.group(1)
            if name in self._DERIVED:
                derived.append(pin)
                continue
            var = pin_vars.get(name)
            if var is None:
                missing.append(name)
            elif f'"${var}"' not in installs:
                uninstalled.append(f"{name} (${var})")
            # Unquoted and single-quoted args are as common as double-quoted
            # ones here (`fastparquet cramjam`, `obspec`), so anchor on a name
            # boundary + a version-ish tail instead of a leading quote.
            if re.search(rf"(?<![\w.-]){re.escape(name)}==\d", script):
                literal.append(name)
        assert not missing, (
            f"lambda-extra pins not derived in deployment/aws/build_layer.sh: {missing} "
            "(the layer would ship without them — see issue #218's async-tiff gap; "
            "fetch each with NAME_PIN=$(lambda_pin <name>))"
        )
        assert not uninstalled, (
            f"lambda-extra pins derived but never installed: {uninstalled} — the layer "
            'ships without them (issue #218). Pass each as "$NAME_PIN" on a $PIP '
            "install line."
        )
        assert not literal, (
            f"build_layer.sh hard-codes {literal} — exact pins are single-sourced from "
            "the lambda extra via lambda_pin (PR #436); a literal pin is a second "
            "declaration site and will drift"
        )
        assert not derived, (
            f"{derived} is pinned in the lambda extra, but build_layer.sh derives that "
            "spec from [project.dependencies] (issue #322), so the exact pin never "
            "reaches the layer. Either move the pin to [project.dependencies], or "
            "derive it from the extra via lambda_pin instead."
        )

    def test_lambda_pin_derivation_resolves_every_exact_pin(self):
        """Execute the script's ``lambda_pin`` python: its output IS the extra's pin.

        The parity test above matches text; this one runs the actual extraction
        snippet out of build_layer.sh against the real pyproject.toml, so a
        broken derivation (renamed table, quoting slip, a name it cannot find)
        fails here instead of at layer-build time.
        """
        import subprocess
        import sys
        import tomllib

        script = (REPO_ROOT / "deployment" / "aws" / "build_layer.sh").read_text()
        m = re.search(r"lambda_pin\(\)\s*\{\s*\$PYTHON -c '(.*?)'", script, re.DOTALL)
        assert m, "lambda_pin() helper missing from build_layer.sh"
        code = m.group(1)
        pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        pins = [
            p
            for p in pyproject["project"]["optional-dependencies"]["lambda"]
            if "==" in p and p.split("==")[0].strip() not in self._DERIVED
        ]
        assert pins  # the extra stopped pinning anything: the layer is unpinned
        for pin in pins:
            name = pin.split("==")[0].strip()
            out = subprocess.run(
                [sys.executable, "-c", code, str(REPO_ROOT / "pyproject.toml"), name],
                capture_output=True,
                text=True,
            )
            assert out.returncode == 0, f"lambda_pin({name}) failed: {out.stderr}"
            assert out.stdout.strip() == pin, (
                f"lambda_pin({name}) derived {out.stdout.strip()!r}, extra declares {pin!r}"
            )

    @staticmethod
    def _release(version):
        """``"0.3.10"`` -> ``(0, 3, 10)``, so pins order numerically not lexically."""
        return tuple(int(part) for part in version.split("."))

    def test_every_lambda_extra_pin_satisfies_its_core_floor(self):
        """A ``lambda`` exact pin must not contradict the core floor for the same dist.

        The parity check above compares the pin to build_layer.sh; nothing
        compared it to ``[project.dependencies]``. A floor bumped past its pin
        (core ``>=0.3.2`` against extra ``==0.3.1``) makes ``zagg[lambda]``
        outright unresolvable — an extra carries the base requirements too, so
        the two specs intersect to nothing — which is why a floor bump and its
        pin are one atomic change and cannot be landed half each.
        """
        import tomllib

        pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        floors = {}
        for dep in pyproject["project"]["dependencies"]:
            m = re.match(r"([A-Za-z0-9._-]+)>=([0-9][0-9.]*)$", dep)
            if m:
                floors[m.group(1)] = m.group(2)
        checked = []
        for pin in pyproject["project"]["optional-dependencies"]["lambda"]:
            m = re.match(r"([A-Za-z0-9._-]+)==([0-9][0-9.]*)$", pin)
            if not m or m.group(1) not in floors:
                continue  # unpinned entries, or layer-only dists with no core floor
            name, exact = m.group(1), m.group(2)
            assert self._release(exact) >= self._release(floors[name]), (
                f"lambda extra pins {name}=={exact}, below the [project.dependencies] "
                f"floor >={floors[name]} — zagg[lambda] would be unresolvable"
            )
            checked.append(name)
        # Guard the regexes above: a naming/spec drift that matched nothing
        # would make this test vacuously green.
        assert "h5coro-hidefix" in checked

    def test_derived_specs_still_come_from_project_dependencies(self):
        """The derivation the test above exempts must actually exist and resolve."""
        import tomllib

        pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
        deps = pyproject["project"]["dependencies"]
        script = (REPO_ROOT / "deployment" / "aws" / "build_layer.sh").read_text()
        for name, fragment in self._DERIVED.items():
            assert fragment in script, (
                f"build_layer.sh no longer derives {name}'s spec ({fragment!r} gone) — "
                "drop it from _DERIVED so the pin-parity check covers it again"
            )
            assert any(re.match(rf"{re.escape(name)}($|[\s<>=!~\[])", d) for d in deps), (
                f"{name} is derived from [project.dependencies] but is not declared "
                "there — the layer build would fail on an empty spec"
            )
