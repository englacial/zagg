#!/bin/bash
# Stand up the zagg Lambda backend in your own AWS account, end to end.
#
# Lambda code (the deps layer + function zips) is read from the public
# distribution bucket the release pipeline stages to (s3://sliderule-public-cors,
# keys <minor>/<zip> — publish.yml's distribute job / distribute_zips.sh).
# CloudFormation requires that code to live in a SAME-REGION bucket:
#
#   * In us-west-2 (where the distribution bucket lives) the stack reads it
#     straight from the source — no staging bucket of your own needed.
#   * In any other region, provide STAGING_BUCKET (a bucket you own in that
#     region); the zips are copied from the source into it first.
#
# Artifacts are keyed by zagg MINOR version (0.N.x -> 0.N). Run from a zagg git
# clone and the minor is read from the latest tag (no install needed); otherwise
# set LAMBDA_VERSION (e.g. LAMBDA_VERSION=0.2). Set LAMBDA_VERSION=latest to read
# the newest published minor from the bucket's versions.json (so a clone/
# pip-install isn't hard-pinned to its build-time version). Whatever it resolves
# to, the layer/function keys are verified to EXIST on the bucket before any
# stack call — an unstaged minor fails fast with the staged minors listed,
# instead of surfacing as a CloudFormation NoSuchKey rollback (issue #174).
#
# The script echoes the resolved bucket/keys/version and asks for confirmation
# before deploying; pass --yes to skip the prompt (unattended runs).
#
# By default the stack creates its own Lambda execution role (needs
# iam:CreateRole — fine if you admin your own account). In IAM-constrained
# accounts (e.g. an SSO power-user), set CREATE_ROLE=false and pass ROLE_ARN, an
# execution role an admin made once (see execution_role.yaml / EXECUTION_ROLE.md).
#
# Usage:
#   OUTPUT_BUCKET=my-results ./stand_up.sh                              # us-west-2, stack makes the role
#   OUTPUT_BUCKET=my-results ./stand_up.sh --yes                        # no confirm prompt
#   REGION=us-east-1 OUTPUT_BUCKET=my-results STAGING_BUCKET=my-stage ./stand_up.sh
#   CREATE_ROLE=false ROLE_ARN=arn:aws:iam::123:role/zagg-exec OUTPUT_BUCKET=my-results ./stand_up.sh
#
# Requires: aws CLI (configured).

set -euo pipefail

ASSUME_YES=false
while [ $# -gt 0 ]; do
    case "$1" in
        --yes|-y) ASSUME_YES=true; shift ;;
        *) echo "ERROR: unknown argument '$1' (the script is env-driven; the only flag is --yes)"; exit 2 ;;
    esac
done

# Verbose by default: echo each AWS command (copy-pasteable) before running it.
# The command runs in the foreground, so its stdout/stderr stream straight to you.
run() { echo "+ $(printf '%q ' "$@")"; "$@"; }

ARCH="${ARCH:-arm64}"                                # arm64 | x86_64
STACK_NAME="${STACK_NAME:-zagg-backend}"
FUNCTION_NAME="${FUNCTION_NAME:-process-shard}"      # e.g. process-shard-test for a test stack
REGION="${REGION:-us-west-2}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:?Set OUTPUT_BUCKET to the bucket where results go}"
CREATE_BUCKET="${CREATE_BUCKET:-false}"              # true => the stack creates OUTPUT_BUCKET
CREATE_ROLE="${CREATE_ROLE:-true}"                   # true => the stack creates the exec role
ROLE_ARN="${ROLE_ARN:-}"                             # required only when CREATE_ROLE=false
STAGING_BUCKET="${STAGING_BUCKET:-}"                 # required only outside the mirror region

# When the stack can't create IAM roles (e.g. an SSO power-user without
# iam:CreateRole), set CREATE_ROLE=false and pass ROLE_ARN — an execution role
# an account admin created once (see execution_role.yaml / EXECUTION_ROLE.md).
if [ "$CREATE_ROLE" != "true" ] && [ -z "$ROLE_ARN" ]; then
    echo "ERROR: CREATE_ROLE=$CREATE_ROLE but ROLE_ARN is empty."
    echo "       Set ROLE_ARN to a pre-existing Lambda execution role ARN, or set"
    echo "       CREATE_ROLE=true to have the stack create one (needs iam:CreateRole)."
    exit 1
fi

# Distribution source (issue #25; source.coop mirror retired in issue #174).
# The public CORS bucket the release pipeline stages to: listable (a
# versions.json index), keyed by minor as <minor>/<zip> (publish.yml's
# distribute job / distribute_zips.sh). Override to self-host a copy.
DIST_BUCKET="${DIST_BUCKET:-sliderule-public-cors}"
DIST_PREFIX="${DIST_PREFIX:-}"                       # keys: [<prefix>/]<minor>/<zip>
DIST_REGION="${DIST_REGION:-us-west-2}"

dist_key()   { local p="$DIST_PREFIX"; [ -n "$p" ] && p="$p/"; echo "${p}${1}/${2}"; }  # minor, zip
dist_root()  { local p="$DIST_PREFIX"; [ -n "$p" ] && p="$p/"; echo "${p}${1}"; }       # versions.json path

case "$ARCH" in
    arm64)  LAYER_ZIP="lambda_layer_arm64.zip";  FUNC_ZIP="lambda_function_arm64_py312.zip" ;;
    x86_64) LAYER_ZIP="lambda_layer_x86_64.zip"; FUNC_ZIP="lambda_function_x86_64_py312.zip" ;;
    *) echo "ERROR: ARCH must be arm64 or x86_64 (got '$ARCH')"; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Resolve the lambda minor version (0.N). Order: LAMBDA_VERSION=latest (read the
# CORS bucket's versions.json) -> explicit LAMBDA_VERSION -> the repo's git tag
# (works from a fresh clone) -> the installed zagg.
if [ "${LAMBDA_VERSION:-}" = "latest" ]; then
    MINOR="$(aws s3 cp "s3://$DIST_BUCKET/$(dist_root versions.json)" - --region "$DIST_REGION" --no-sign-request 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["latest"])' 2>/dev/null || true)"
    [ -z "$MINOR" ] && { echo "ERROR: could not read 'latest' from s3://$DIST_BUCKET/$(dist_root versions.json)"; exit 1; }
    echo "Resolved LAMBDA_VERSION=latest -> $MINOR"
elif [ -n "${LAMBDA_VERSION:-}" ]; then
    MINOR="$LAMBDA_VERSION"
else
    MINOR="$(git -C "$SCRIPT_DIR" describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' | cut -d. -f1,2)"
    [ -z "$MINOR" ] && MINOR="$(python3 -c 'import zagg; print(".".join(zagg.__version__.split(".")[:2]))' 2>/dev/null || true)"
    if [ -z "$MINOR" ]; then
        echo "ERROR: could not determine the lambda minor version. Run from a zagg git"
        echo "       clone (uses the latest tag), or install zagg, or set LAMBDA_VERSION"
        echo "       (e.g. LAMBDA_VERSION=0.2, or LAMBDA_VERSION=latest)."
        exit 1
    fi
fi
echo "zagg lambda artifacts: $MINOR ($ARCH), region $REGION"

# Refuse to guess (issue #174): the resolved minor must actually be staged on
# the distribution bucket. A HEAD on the layer key catches a phantom version
# (e.g. one derived from post-tag main) here, with an actionable message,
# instead of as a CloudFormation NoSuchKey mid-update.
SRC_BUCKET="$DIST_BUCKET"; SRC_REGION="$DIST_REGION"
SRC_LAYER_KEY="$(dist_key "$MINOR" "$LAYER_ZIP")"; SRC_FUNC_KEY="$(dist_key "$MINOR" "$FUNC_ZIP")"
if ! aws s3api head-object --bucket "$DIST_BUCKET" --key "$SRC_LAYER_KEY" --region "$DIST_REGION" --no-sign-request >/dev/null 2>&1; then
    echo "ERROR: minor $MINOR is not staged: s3://$DIST_BUCKET/$SRC_LAYER_KEY does not exist."
    STAGED="$(aws s3 cp "s3://$DIST_BUCKET/$(dist_root versions.json)" - --region "$DIST_REGION" --no-sign-request 2>/dev/null \
        | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin).get("minors", [])))' 2>/dev/null || true)"
    [ -n "$STAGED" ] && echo "       Staged minors on s3://$DIST_BUCKET: $STAGED"
    echo "       Set LAMBDA_VERSION to a staged minor, or LAMBDA_VERSION=latest for the"
    echo "       newest published one (versions.json)."
    exit 1
fi

echo ""
echo "Resolved deployment artifacts:"
echo "  version:  $MINOR ($ARCH)"
echo "  layer:    s3://$SRC_BUCKET/$SRC_LAYER_KEY"
echo "  function: s3://$SRC_BUCKET/$SRC_FUNC_KEY"
echo "  stack:    $STACK_NAME ($REGION)"
if [ "$ASSUME_YES" != "true" ]; then
    read -r -p "Proceed with deploy? [y/N] " REPLY
    case "$REPLY" in
        [Yy]|[Yy][Ee][Ss]) ;;
        *) echo "Aborted (pass --yes to skip this prompt)."; exit 1 ;;
    esac
fi

if [ "$REGION" = "$SRC_REGION" ]; then
    # Same region as the source — deploy straight from it, no staging bucket.
    echo "Deploying directly from s3://$SRC_BUCKET/$SRC_LAYER_KEY"
    ARTIFACT_BUCKET="$SRC_BUCKET"
    LAYER_S3KEY="$SRC_LAYER_KEY"
    FUNC_S3KEY="$SRC_FUNC_KEY"
else
    # Different region — CloudFormation needs a same-region bucket, so copy the
    # zips from the source into the user's STAGING_BUCKET first.
    if [ -z "$STAGING_BUCKET" ]; then
        echo "ERROR: REGION=$REGION is not the source region ($SRC_REGION)."
        echo "       CloudFormation reads Lambda code from a SAME-REGION bucket, so the public"
        echo "       source can't be used directly here. Provide STAGING_BUCKET (a bucket you"
        echo "       own in $REGION); the zips will be copied into it from the source."
        echo "       (Or deploy in us-west-2 to skip staging entirely.)"
        exit 1
    fi
    if ! aws s3api head-bucket --bucket "$STAGING_BUCKET" --region "$REGION" 2>/dev/null; then
        echo "ERROR: staging bucket '$STAGING_BUCKET' not found/accessible in $REGION."
        echo "       Create it first:  aws s3 mb s3://$STAGING_BUCKET --region $REGION"
        exit 1
    fi
    TMPDIR="$(mktemp -d)"; trap 'rm -rf "$TMPDIR"' EXIT
    echo "Copying zips from s3://$SRC_BUCKET into s3://$STAGING_BUCKET/ ..."
    run aws s3 cp "s3://$SRC_BUCKET/$SRC_LAYER_KEY" "$TMPDIR/$LAYER_ZIP" --region "$SRC_REGION" --no-sign-request
    run aws s3 cp "s3://$SRC_BUCKET/$SRC_FUNC_KEY"  "$TMPDIR/$FUNC_ZIP"  --region "$SRC_REGION" --no-sign-request
    run aws s3 cp "$TMPDIR/$LAYER_ZIP" "s3://$STAGING_BUCKET/$LAYER_ZIP" --region "$REGION"
    run aws s3 cp "$TMPDIR/$FUNC_ZIP"  "s3://$STAGING_BUCKET/$FUNC_ZIP"  --region "$REGION"
    ARTIFACT_BUCKET="$STAGING_BUCKET"
    LAYER_S3KEY="$LAYER_ZIP"
    FUNC_S3KEY="$FUNC_ZIP"
fi

echo "Deploying stack $STACK_NAME..."
run aws cloudformation deploy \
    --template-file "$SCRIPT_DIR/template.yaml" \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        Architecture="$ARCH" \
        FunctionName="$FUNCTION_NAME" \
        ArtifactBucket="$ARTIFACT_BUCKET" \
        LayerS3Key="$LAYER_S3KEY" \
        FunctionS3Key="$FUNC_S3KEY" \
        OutputBucketName="$OUTPUT_BUCKET" \
        CreateOutputBucket="$CREATE_BUCKET" \
        CreateExecutionRole="$CREATE_ROLE" \
        ExecutionRoleArn="$ROLE_ARN"

echo ""
run aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query 'Stacks[0].Outputs' --output table

echo ""
echo "Done. Backend stood up as stack '$STACK_NAME' in $REGION."
