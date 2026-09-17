"""Test the shared in-place Lambda deploy script (.github/scripts/deploy_lambda.sh).

Runs the real script with a stub ``aws`` (no AWS) and asserts it issues the four
calls in the required order with the right function/layer wiring: publish a layer
version, point the function at it, wait, then update the code. The ordering (wait
between the config + code updates) and the layer-from-S3 wiring are the parts that
matter for a correct in-place deploy. Since issue #341 the script also updates
the worker-size variant family (``${FN}-<mem>[-disk]``) from the same layer/zip:
each variant is probed with get-function-configuration; missing variants are
skipped with a note, probe failures (IAM) warn loudly and skip.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / ".github" / "scripts" / "deploy_lambda.sh"

#: The script's default update set (issue #341): the template.yaml
#: WorkerMemorySizes matrix plus the same-zip ``-extract`` twin (ExtractFn).
FAMILY = ["-2048", "-4096", "-8192", "-2048-disk", "-4096-disk", "-8192-disk", "-extract"]

#: The subset the TEST stack provisions (template.yaml's WorkerTestDiskVariants
#: loop, -disk only), which lambda-benchmark.yml pins via --variants so the
#: enumerated deploy role never AccessDenies a probe (fold review, issue #341).
TEST_STACK_VARIANTS = ["-2048-disk", "-4096-disk", "-8192-disk"]

# Stub `aws`: log the full arg line; emit a LayerVersionArn on stdout for the
# publish-layer-version call (the script captures it). get-function-configuration
# probes succeed only for functions listed in $AWS_EXISTING (space-separated;
# unset/empty means every probe succeeds); others 404 like the real CLI.
STUB_AWS = """#!/bin/bash
echo "$*" >> "$AWS_LOG"
if [ "$2" = "publish-layer-version" ]; then
  echo "arn:aws:lambda:us-west-2:1:layer:demo-deps:7"
fi
if [ "$2" = "get-function-configuration" ] && [ -n "${AWS_EXISTING+x}" ]; then
  case " $AWS_EXISTING " in
    *" $4 "*) exit 0 ;;
    *) echo "An error occurred (ResourceNotFoundException) when calling the GetFunctionConfiguration operation: Function not found: $4" >&2; exit 254 ;;
  esac
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
    # Five calls, in order.
    assert "lambda publish-layer-version" in log[0]
    assert "process-shard-test-deps" in log[0]  # layer named after the function
    assert "S3Key=lambda-test/abc/lambda_layer_arm64.zip" in log[0]
    assert "lambda update-function-configuration" in log[1]
    assert "arn:aws:lambda:us-west-2:1:layer:demo-deps:7" in log[1]  # uses published ARN
    assert "lambda wait function-updated" in log[2]  # settle before code update
    assert "lambda update-function-code" in log[3]
    assert f"fileb://{fn_zip}" in log[3]
    # Async-invoke hygiene (issue #151): retries pinned to 0, event age under
    # the runner's poll margin (see ProcessFnAsyncConfig in template.yaml).
    assert "lambda put-function-event-invoke-config" in log[4]
    assert "--maximum-retry-attempts 0" in log[4]
    assert "--maximum-event-age-in-seconds 60" in log[4]


def test_event_invoke_config_failure_is_nonfatal(tmp_path):
    # issue #151: the deploy role may not yet carry
    # lambda:PutFunctionEventInvokeConfig; a denied config call must warn, not
    # fail the deploy (the pipeline works without it, just with default async
    # service retries).
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "aws").write_text(
        STUB_AWS.replace(
            "exit 0",
            'if [ "$2" = "put-function-event-invoke-config" ]; then exit 1; fi\nexit 0',
        )
    )
    (bindir / "aws").chmod(0o755)
    fn_zip = tmp_path / "fn.zip"
    fn_zip.write_bytes(b"zip")

    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "AWS_LOG": str(tmp_path / "aws.log"),
    }
    result = subprocess.run(
        ["bash", str(SCRIPT), "--function", "f", "--layer-bucket", "b", "--layer-key", "k"]
        + ["--function-zip", str(fn_zip), "--region", "us-west-2"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0
    assert "WARN: could not set event-invoke config" in result.stderr


def test_missing_required_arg_errors(tmp_path):
    result = subprocess.run(
        ["bash", str(SCRIPT), "--function", "f"],  # missing the rest
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0


def _run_deploy(tmp_path, stub=STUB_AWS, extra_args=(), existing=None):
    """Run the script with the stub aws; return (result, aws log lines)."""
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")
    bindir = tmp_path / "bin"
    bindir.mkdir(parents=True)
    (bindir / "aws").write_text(stub)
    (bindir / "aws").chmod(0o755)
    fn_zip = tmp_path / "fn.zip"
    fn_zip.write_bytes(b"zip")
    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "AWS_LOG": str(tmp_path / "aws.log"),
    }
    if existing is not None:
        env["AWS_EXISTING"] = " ".join(existing)
    result = subprocess.run(
        ["bash", str(SCRIPT), "--function", "process-shard-test", "--layer-bucket", "b"]
        + ["--layer-key", "k", "--function-zip", str(fn_zip), "--region", "us-west-2"]
        + list(extra_args),
        capture_output=True,
        text=True,
        env=env,
    )
    log = (tmp_path / "aws.log").read_text().splitlines() if (tmp_path / "aws.log").exists() else []
    return result, log


def _deployed(log):
    """Function names whose code was updated, in call order."""
    return [
        line.split("--function-name ")[1].split()[0]
        for line in log
        if "update-function-code" in line
    ]


def test_variant_family_deployed_when_present(tmp_path):
    # Issue #341: the whole worker-size family updates from the same zip. On a
    # stack that provisions a subset of the DEFAULT_VARIANTS list, the absent
    # ones 404 and are skipped quietly.
    existing = [f"process-shard-test{s}" for s in ("-2048-disk", "-4096-disk", "-8192-disk")]
    result, log = _run_deploy(tmp_path, existing=existing)
    assert result.returncode == 0
    # ONE layer publish, shared by the whole family.
    assert len([line for line in log if "publish-layer-version" in line]) == 1
    # Base first, then each existing variant; missing variants skipped.
    assert _deployed(log) == ["process-shard-test", *existing]
    # Every default-family variant was probed.
    probed = [line for line in log if "get-function-configuration" in line]
    assert len(probed) == len(FAMILY)
    for missing in ("-2048", "-4096", "-8192", "-extract"):
        assert f"variant process-shard-test{missing} does not exist; skipping" in result.stdout


def test_extract_twin_deployed_when_present(tmp_path):
    # The prod shape: the -extract twin exists and gets the same layer + zip as
    # the base. Before this it was updated only at standup (issue #341).
    existing = ["process-shard-test", "process-shard-test-extract"]
    result, log = _run_deploy(tmp_path, existing=existing)
    assert result.returncode == 0
    assert "process-shard-test-extract" in _deployed(log)
    assert "STALE" not in result.stderr


def test_test_deploy_variant_set_probes_nothing_absent(tmp_path):
    # Fold review: the workflow now pins --variants to the test-stack shape, so
    # the plain-memory trio is never probed. Before, the enumerated deploy role
    # AccessDenied those three probes (not 404), landing on the WARN branch and
    # printing "it may now be STALE" on EVERY green deploy for functions that
    # will never exist -- the one channel whose job is to make real staleness
    # visible, crying wolf 3x per deploy.
    existing = [f"process-shard-test{s}" for s in TEST_STACK_VARIANTS]
    stub = STUB_AWS.replace(
        'echo "An error occurred (ResourceNotFoundException) when calling the '
        'GetFunctionConfiguration operation: Function not found: $4" >&2; exit 254',
        'echo "An error occurred (AccessDeniedException) ..." >&2; exit 254',
    )
    result, log = _run_deploy(
        tmp_path,
        stub=stub,
        existing=existing,
        extra_args=["--variants", " ".join(TEST_STACK_VARIANTS)],
    )
    assert result.returncode == 0
    assert _deployed(log) == ["process-shard-test", *existing]
    assert len([line for line in log if "get-function-configuration" in line]) == len(
        TEST_STACK_VARIANTS
    )
    assert "STALE" not in result.stderr
    assert "does not exist; skipping" not in result.stdout


def test_denied_update_aborts_the_family_loop(tmp_path):
    # The granted-Get/denied-Update shape: the probe succeeds, the update does
    # not. `set -e` aborts mid-family (base plus some variants updated, the rest
    # stale) and the job goes red -- deliberate, and documented in the script:
    # a red deploy is recoverable and the CodeSha256 guard refuses to benchmark
    # the half-updated family.
    stub = STUB_AWS.replace(
        "exit 0\n",
        'if [ "$2" = "update-function-code" ] && [ "$4" = "process-shard-test-4096-disk" ]; then\n'
        '  echo "An error occurred (AccessDeniedException) ..." >&2; exit 254\nfi\nexit 0\n',
        1,
    )
    result, log = _run_deploy(
        tmp_path,
        stub=stub,
        existing=None,
        extra_args=["--variants", " ".join(TEST_STACK_VARIANTS)],
    )
    assert result.returncode != 0
    # The loop stopped at the denied variant: -8192-disk was never attempted.
    assert "process-shard-test-8192-disk" not in "\n".join(log)


def test_variant_probe_failure_warns_and_skips(tmp_path):
    # A non-404 probe failure (e.g. the deploy role's IAM scoped to the base
    # name only) must warn loudly and continue — never fail the base deploy,
    # never silently skip (issue #341).
    stub = STUB_AWS.replace(
        'echo "An error occurred (ResourceNotFoundException) when calling the '
        'GetFunctionConfiguration operation: Function not found: $4" >&2; exit 254',
        'echo "An error occurred (AccessDeniedException) ..." >&2; exit 254',
    )
    result, log = _run_deploy(tmp_path, stub=stub, existing=[])
    assert result.returncode == 0
    assert _deployed(log) == ["process-shard-test"]  # base still deployed
    assert "WARN: could not probe variant" in result.stderr
    assert "lambda:GetFunctionConfiguration" in result.stderr


def test_variants_override(tmp_path):
    # --variants replaces the default family; empty string deploys base only.
    result, log = _run_deploy(tmp_path, extra_args=["--variants", "-4096-disk"], existing=None)
    assert result.returncode == 0
    assert _deployed(log) == ["process-shard-test", "process-shard-test-4096-disk"]

    result2, log2 = _run_deploy(tmp_path / "sub2", extra_args=["--variants", ""])
    assert result2.returncode == 0
    assert _deployed(log2) == ["process-shard-test"]


# --- IAM drift guard (issue #341) -------------------------------------------
# The CI/CD roles' Update statements enumerate exact function ARNs (a
# ${...FunctionName}* wildcard was proposed and declined), so every variant
# suffix the deploy_lambda.sh family loop can target must have a matching
# enumerated ARN in deployment/aws/benchmark_cicd.yaml or the deploy
# WARN-and-skips it as stale.

TEMPLATE = REPO / "deployment" / "aws" / "template.yaml"
CICD = REPO / "deployment" / "aws" / "benchmark_cicd.yaml"
BENCH_WORKFLOW = REPO / ".github" / "workflows" / "lambda-benchmark.yml"


def _load_cfn(path):
    """Parse a CFN template, mapping short-form intrinsics (!Sub, !Ref, ...)
    to {TagSuffix: value} dicts (same pattern as tests/test_lambda_build.py)."""
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
    return yaml.load(path.read_text(), Loader=_CfnLoader)


def _effective_worker_memory_sizes():
    """The memory sizes the DEPLOYED stacks actually provision.

    ``WorkerMemorySizes`` is a ``CommaDelimitedList`` *parameter*, so reading its
    ``Default`` is only the effective matrix while nothing overrides it. It is
    determinable here rather than assumed: ``stand_up.sh`` is the one in-repo
    standup path and its ``--parameter-overrides`` list does not name the
    parameter, which
    :func:`test_standup_does_not_override_the_worker_memory_matrix` pins. A stack
    stood up BY HAND with a different list is the stated residual — it would
    provision variants nothing here probes or enumerates (the template's own
    comment makes it a non-free-form knob: each size needs a matching
    ``WorkerDiskTmp`` entry and memory+2048 <= 10240).
    """
    tpl = _load_cfn(TEMPLATE)
    return [s.strip() for s in tpl["Parameters"]["WorkerMemorySizes"]["Default"].split(",")]


def test_standup_does_not_override_the_worker_memory_matrix():
    # Fold review: the drift guard compares against the parameter DEFAULT. That
    # is the effective deployed matrix only while no standup path overrides it --
    # assert that, so the boundary is guarded instead of assumed.
    standup = (REPO / "deployment" / "aws" / "stand_up.sh").read_text()
    assert "WorkerMemorySizes" not in standup, (
        "stand_up.sh now sets WorkerMemorySizes, so the deployed variant matrix "
        "no longer equals template.yaml's parameter Default -- point "
        "_effective_worker_memory_sizes() at the override (issue #341)"
    )


def _template_variant_suffixes():
    """(test_suffixes, prod_suffixes) stamped by template.yaml's Fn::ForEach
    loops over the WorkerMemorySizes matrix; test suffixes are relative to
    TestFunctionName (= ``${FunctionName}-test``, the deploy target)."""
    tpl = _load_cfn(TEMPLATE)
    sizes = _effective_worker_memory_sizes()
    test, prod = set(), set()
    for key, val in tpl["Resources"].items():
        if not key.startswith("Fn::ForEach::"):
            continue
        for resource in val[2].values():
            fn = resource.get("Properties", {}).get("FunctionName")
            pattern = fn.get("Sub", "") if isinstance(fn, dict) else ""
            if "${WorkerMemory}" not in pattern:
                continue
            suffix = pattern.removeprefix("${FunctionName}")
            for size in sizes:
                stamped = suffix.replace("${WorkerMemory}", size)
                if stamped.startswith("-test"):
                    test.add(stamped.removeprefix("-test"))
                else:
                    prod.add(stamped)
    assert test and prod, f"no Fn::ForEach variant loops found in {TEMPLATE}"
    return test, prod


def _template_named_twins():
    """Suffixes of the separately-NAMED functions template.yaml stamps off the
    same code zip (not worker-size variants): today just ``-extract``
    (``ExtractFn``, issue #148). These run the deployed zip, so #341's rule
    ("deploy every function that runs the code zip") covers them."""
    tpl = _load_cfn(TEMPLATE)
    twins = set()
    for key, val in tpl["Resources"].items():
        if key.startswith("Fn::ForEach::") or val.get("Type") != "AWS::Lambda::Function":
            continue
        fn = val.get("Properties", {}).get("FunctionName")
        pattern = fn.get("Sub", "") if isinstance(fn, dict) else ""
        if not pattern.startswith("${FunctionName}-") or "${WorkerMemory}" in pattern:
            continue
        twins.add(pattern.removeprefix("${FunctionName}"))
    assert twins, f"no same-zip named twins found in {TEMPLATE}"
    return twins


def _statement_arns(role, sid):
    """The !Sub ARN strings of the role's Sid=<sid> policy statement."""
    tpl = _load_cfn(CICD)
    statements = tpl["Resources"][role]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    stmt = next(s for s in statements if s.get("Sid") == sid)
    resource = stmt["Resource"]
    items = resource if isinstance(resource, list) else [resource]
    return [item["Sub"] if isinstance(item, dict) else item for item in items]


def _script_default_variants():
    match = re.search(r'^DEFAULT_VARIANTS="([^"]*)"', SCRIPT.read_text(), re.MULTILINE)
    assert match, f"DEFAULT_VARIANTS not found in {SCRIPT}"
    return set(match.group(1).split())


def _workflow_variants():
    """The ``--variants`` suffix set lambda-benchmark.yml's deploy step pins."""
    match = re.search(r'--variants "([^"]*)"', BENCH_WORKFLOW.read_text())
    assert match, f"deploy step in {BENCH_WORKFLOW} passes no --variants"
    return set(match.group(1).split())


def test_test_deploy_workflow_pins_the_test_stack_variant_set():
    # Fold review: the per-merge deploy must probe EXACTLY what the test stack
    # provisions. Probing the prod default meant three AccessDenied probes (the
    # deploy role enumerates only the -disk trio) landing on the loud
    # "may now be STALE" branch on every green deploy, for functions that will
    # never exist -- the same "nobody reads the warning" failure as issue #341.
    test_suffixes, _ = _template_variant_suffixes()
    assert _workflow_variants() == test_suffixes, (
        f"the --variants list in {BENCH_WORKFLOW}'s deploy step and the "
        f"WorkerTestDiskVariants loop in {TEMPLATE} disagree -- update whichever "
        f"is stale, or the deploy either skips a provisioned variant (leaving it "
        f"STALE, issue #341) or warns about one that does not exist"
    )
    # ...and every pinned suffix is enumerated on the deploy role, so no probe
    # in the pinned set can AccessDeny.
    arns = _statement_arns("DeployRole", "UpdateTestFunction")
    for suffix in sorted(_workflow_variants()):
        want = "function:${TestFunctionName}" + suffix
        assert any(a.endswith(want) for a in arns), (
            f"{BENCH_WORKFLOW} deploys test variant '{suffix}' but "
            f"DeployRole.UpdateTestFunction in {CICD} has no ARN ending '{want}'"
        )


def test_deploy_role_enumerates_test_stack_variants():
    # Test-stack shape: base + every WorkerTestDiskVariants function.
    test_suffixes, _ = _template_variant_suffixes()
    arns = _statement_arns("DeployRole", "UpdateTestFunction")
    assert any(a.endswith("function:${TestFunctionName}") for a in arns), (
        f"DeployRole.UpdateTestFunction in {CICD} must keep the base "
        "function:${TestFunctionName} ARN"
    )
    for suffix in sorted(test_suffixes):
        want = "function:${TestFunctionName}" + suffix
        assert any(a.endswith(want) for a in arns), (
            f"deploy_lambda.sh targets test variant '{suffix}' but "
            f"DeployRole.UpdateTestFunction in {CICD} has no ARN ending "
            f"'{want}' -- add the enumerated ARN (wildcards declined, issue #341)"
        )


def test_release_role_enumerates_prod_variant_family():
    # Prod family: base + plain and -disk variants for every memory size.
    _, prod_suffixes = _template_variant_suffixes()
    arns = _statement_arns("ReleaseRole", "UpdateProdFunction")
    assert any(a.endswith("function:${BenchmarkFunctionName}") for a in arns), (
        f"ReleaseRole.UpdateProdFunction in {CICD} must keep the base "
        "function:${BenchmarkFunctionName} ARN"
    )
    for suffix in sorted(prod_suffixes | _script_default_variants()):
        want = "function:${BenchmarkFunctionName}" + suffix
        assert any(a.endswith(want) for a in arns), (
            f"deploy_lambda.sh targets prod variant '{suffix}' but "
            f"ReleaseRole.UpdateProdFunction in {CICD} has no ARN ending "
            f"'{want}' -- add the enumerated ARN (wildcards declined, issue #341)"
        )


def test_script_default_family_matches_template_matrix():
    # The script's DEFAULT_VARIANTS and template.yaml's same-zip functions are
    # two copies of one list: the prod worker-size loops PLUS the separately
    # named twins (-extract). A size or a twin added to one must land in both.
    _, prod_suffixes = _template_variant_suffixes()
    assert _script_default_variants() == prod_suffixes | _template_named_twins(), (
        f"DEFAULT_VARIANTS in {SCRIPT} and the same-zip functions in {TEMPLATE} "
        f"(WorkerMemorySizes variant loops + named twins like ExtractFn) disagree "
        f"-- update whichever is stale (issue #341)"
    )


def test_extract_twin_is_deployed_and_enumerated():
    # espg ruling on the fold review: the -extract twin runs the same code zip
    # and was never redeployed, so `${FunctionName}-extract` sat on standup-time
    # code permanently -- issue #341's failure mode verbatim, on the live
    # full-archive extraction pool (issue #148). It is now in the deploy set and
    # enumerated on the release role. The INVOKE role deliberately is NOT
    # extended: the twin is reached by explicit function_name=, never by the
    # benchmark harness's suffix resolution.
    assert "-extract" in _template_named_twins()
    assert "-extract" in _script_default_variants(), (
        f"template.yaml stamps a -extract twin off the same code zip but {SCRIPT} "
        f"does not deploy it -- it would stay on standup-time code (issue #341)"
    )
    prod = _statement_arns("ReleaseRole", "UpdateProdFunction")
    want = "function:${BenchmarkFunctionName}-extract"
    assert any(a.endswith(want) for a in prod), (
        f"ReleaseRole.UpdateProdFunction in {CICD} has no ARN ending '{want}' -- "
        f"the release deploy would WARN-and-skip the twin as STALE (issue #341)"
    )
    invoke = _statement_arns("BenchmarkInvokeRole", "InvokeBenchmarkFunctions")
    assert not [a for a in invoke if a.endswith("-extract")], (
        "the benchmark harness never invokes the -extract twin; keep it out of "
        "InvokeBenchmarkFunctions (least privilege, issue #341)"
    )
    # The test stack has no -test-extract twin, so the pinned test-deploy set
    # must not probe one (it would AccessDeny and cry STALE -- see the
    # test-deploy variant-set guard above).
    assert "-extract" not in _workflow_variants()


def test_invoke_role_enumerates_both_families():
    # The invoke role dispatches (and CodeSha256-probes, issue #341) whatever
    # variant a target's ``worker:`` block resolves, against either the prod or
    # the test base function -- so it must enumerate both full families.
    test_suffixes, prod_suffixes = _template_variant_suffixes()
    arns = _statement_arns("BenchmarkInvokeRole", "InvokeBenchmarkFunctions")
    # The named twins in DEFAULT_VARIANTS (-extract) are DEPLOY targets, not
    # harness-resolvable variants -- no ``worker:`` block can name one, so the
    # invoke role must NOT enumerate them (least privilege, issue #341).
    resolvable = _script_default_variants() - _template_named_twins()
    families = [
        ("${BenchmarkFunctionName}", sorted(prod_suffixes | resolvable)),
        ("${TestFunctionName}", sorted(test_suffixes)),
    ]
    for base, suffixes in families:
        for suffix in ["", *suffixes]:
            want = f"function:{base}{suffix}"
            assert any(a.endswith(want) for a in arns), (
                f"the benchmark harness can invoke/probe '{base}{suffix}' but "
                f"BenchmarkInvokeRole.InvokeBenchmarkFunctions in {CICD} has no ARN "
                f"ending '{want}' -- add the enumerated ARN (wildcards declined, issue #341)"
            )


def test_benchmark_targets_resolve_only_provisioned_test_variants():
    # Fold review: enumeration opened a reachable seam. ``_validate_worker``
    # accepts ``{"memory": 4096}`` with NO ``extra_disk``, which resolves
    # ``${TestFunctionName}-4096`` -- a name the test stack does not provision and
    # the invoke role does not enumerate, so the sequence was probe ->
    # AccessDenied -> warn-and-continue -> invoke -> AccessDenied again, as an
    # opaque dispatch failure. The runtime half is now a loud pre-dispatch refusal
    # (check_variant_current hard-fails when the base probes but the variant does
    # not); this is the static half, so the "one targets.json edit away" case goes
    # red in CI instead of costing an invoke to diagnose.
    import json
    import sys

    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    import run_benchmark

    manifest = json.loads((REPO / "tests" / "data" / "benchmark" / "targets.json").read_text())
    test_suffixes, _ = _template_variant_suffixes()
    arns = _statement_arns("BenchmarkInvokeRole", "InvokeBenchmarkFunctions")
    checked = 0
    for section in ("targets", "provisional_targets"):
        for name, target in manifest.get(section, {}).items():
            worker = target.get("worker") if isinstance(target, dict) else None
            if not worker:
                continue
            checked += 1
            # Resolve off an EMPTY base so the result is the bare suffix, using
            # the harness's own rule rather than a copy of it.
            suffix = run_benchmark.resolve_variant("", worker)
            assert suffix in test_suffixes, (
                f"benchmark target '{name}' has worker {worker!r}, which resolves "
                f"'${{TestFunctionName}}{suffix}' -- a variant template.yaml does not "
                f"provision for the test stack (it stamps {sorted(test_suffixes)}). The "
                f"per-merge run would refuse at the pre-dispatch guard (issue #341); "
                f"either set extra_disk, or provision + enumerate the variant."
            )
            want = "function:${TestFunctionName}" + suffix
            assert any(a.endswith(want) for a in arns), (
                f"benchmark target '{name}' resolves '{want}' but "
                f"BenchmarkInvokeRole.InvokeBenchmarkFunctions in {CICD} does not "
                f"enumerate it -- the probe and the invoke would both AccessDeny"
            )
    assert checked, "no benchmark target declares a worker: block -- guard is vacuous"


def test_role_statements_enumerate_exact_arns():
    # The issue #341 ruling: enumerated exact ARNs, no wildcard, in all three
    # function-scoped statements (ConcurrencyProbe's "*" stays -- its actions
    # take no resource-level scoping).
    statements = (
        ("DeployRole", "UpdateTestFunction"),
        ("ReleaseRole", "UpdateProdFunction"),
        ("BenchmarkInvokeRole", "InvokeBenchmarkFunctions"),
    )
    for role, sid in statements:
        for arn in _statement_arns(role, sid):
            assert "*" not in arn, (
                f"{role}.{sid} in {CICD} must enumerate exact ARNs, found wildcard: {arn}"
            )


# --- prefix coupling: distribute stages the key deploy-prod reads back --------
# LAMBDA_DIST_PREFIX is hand-typed, and distribute_zips.sh trims its --prefix
# (issue #497 / PR #504), so a raw `vars.LAMBDA_DIST_PREFIX` in deploy-prod's
# --layer-key builds a DIFFERENT key from the one `distribute` staged -- and the
# failure lands after PyPI has published. publish.yml normalizes once, in
# github-release's outputs. The script half of --prefix is not on this branch
# (it arrives with PR #504), so this pins the workflow expression rather than
# running both sides end to end.
PUBLISH_WORKFLOW = REPO / ".github" / "workflows" / "publish.yml"

NORMALIZED_PREFIX = "needs.github-release.outputs.prefix"


def _publish_jobs():
    import yaml

    return yaml.safe_load(PUBLISH_WORKFLOW.read_text())["jobs"]


def test_github_release_normalizes_the_dist_prefix_once():
    step = next(s for s in _publish_jobs()["github-release"]["steps"] if s.get("id") == "get_tag")
    assert step["env"]["DIST_PREFIX"] == "${{ vars.LAMBDA_DIST_PREFIX }}"
    # Both ends trimmed, mirroring distribute_zips.sh's own --prefix handling.
    assert 'P="${DIST_PREFIX#/}"; P="${P%/}"' in step["run"]
    assert 'echo "prefix=$P" >> "$GITHUB_OUTPUT"' in step["run"]
    assert _publish_jobs()["github-release"]["outputs"]["prefix"] == "${{ %s }}" % (
        "steps.get_tag.outputs.prefix"
    )


def test_distribute_and_deploy_prod_read_the_same_normalized_prefix():
    jobs = _publish_jobs()
    distribute = "".join(str(s.get("run", "")) for s in jobs["distribute"]["steps"])
    prod_step = next(
        s for s in jobs["deploy-prod"]["steps"] if "--layer-key" in str(s.get("run", ""))
    )
    assert f'--prefix "${{{{ {NORMALIZED_PREFIX} }}}}"' in distribute
    assert prod_step["env"]["DIST_PREFIX"] == "${{ %s }}" % NORMALIZED_PREFIX
    # deploy-prod builds distribute's key out of that same value.
    assert '--layer-key "${DIST_PREFIX:+$DIST_PREFIX/}${MINOR}/' in prod_step["run"]
    # Neither job may reach past the normalization for the raw variable.
    assert "vars.LAMBDA_DIST_PREFIX" not in distribute
    assert "vars.LAMBDA_DIST_PREFIX" not in str(prod_step)
