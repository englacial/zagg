#!/bin/bash
# Stand up the zagg Lambda backend in your own AWS account, end to end.
#
# Lambda code (the deps layer + function zips) is read from the public source.coop
# mirror. CloudFormation requires that code to live in a SAME-REGION S3 bucket:
#
#   * In us-west-2 (where the mirror lives) the stack reads it straight from the
#     mirror — no staging bucket of your own needed.
#   * In any other region, provide STAGING_BUCKET (a bucket you own in that
#     region); the zips are copied from the mirror into it first.
#
# The mirror is keyed by zagg MINOR version (0.N.x -> 0.N). Run from a zagg git
# clone and the minor is read from the latest tag (no install needed); otherwise
# set LAMBDA_VERSION (e.g. LAMBDA_VERSION=0.2).
#
# By default the stack creates its own Lambda execution role (needs
# iam:CreateRole — fine if you admin your own account). In IAM-constrained
# accounts (e.g. an SSO power-user), set CREATE_ROLE=false and pass ROLE_ARN, an
# execution role an admin made once (see execution_role.yaml / EXECUTION_ROLE.md).
#
# Usage:
#   OUTPUT_BUCKET=my-results ./stand_up.sh                              # us-west-2, stack makes the role
#   REGION=us-east-1 OUTPUT_BUCKET=my-results STAGING_BUCKET=my-stage ./stand_up.sh
#   CREATE_ROLE=false ROLE_ARN=arn:aws:iam::123:role/zagg-exec OUTPUT_BUCKET=my-results ./stand_up.sh
#
# Requires: aws CLI (configured).

set -euo pipefail

# Verbose by default: echo each AWS command (copy-pasteable) before running it.
# The command runs in the foreground, so its stdout/stderr stream straight to you.
run() { echo "+ $(printf '%q ' "$@")"; "$@"; }

ARCH="${ARCH:-arm64}"                                # arm64 | x86_64
STACK_NAME="${STACK_NAME:-zagg-backend}"
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

# Public mirror (override to self-host a copy).
MIRROR_BUCKET="${MIRROR_BUCKET:-us-west-2.opendata.source.coop}"
MIRROR_PREFIX="${MIRROR_PREFIX:-englacial/zagg/lambda}"
MIRROR_REGION="${MIRROR_REGION:-us-west-2}"

case "$ARCH" in
    arm64)  LAYER_ZIP="lambda_layer_arm64.zip";  FUNC_ZIP="lambda_function_arm64_py312.zip" ;;
    x86_64) LAYER_ZIP="lambda_layer_x86_64.zip"; FUNC_ZIP="lambda_function_x86_64_py312.zip" ;;
    *) echo "ERROR: ARCH must be arm64 or x86_64 (got '$ARCH')"; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Resolve the lambda minor version (0.N). Order: explicit override -> the repo's
# git tag (works from a fresh clone, no install needed) -> the installed zagg.
if [ -n "${LAMBDA_VERSION:-}" ]; then
    MINOR="$LAMBDA_VERSION"
else
    MINOR="$(git -C "$SCRIPT_DIR" describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' | cut -d. -f1,2)"
    [ -z "$MINOR" ] && MINOR="$(python3 -c 'import zagg; print(".".join(zagg.__version__.split(".")[:2]))' 2>/dev/null || true)"
    if [ -z "$MINOR" ]; then
        echo "ERROR: could not determine the lambda minor version. Run from a zagg git"
        echo "       clone (uses the latest tag), or install zagg, or set LAMBDA_VERSION"
        echo "       (e.g. LAMBDA_VERSION=0.2)."
        exit 1
    fi
fi

LAYER_KEY="$MIRROR_PREFIX/$MINOR/$LAYER_ZIP"
FUNC_KEY="$MIRROR_PREFIX/$MINOR/$FUNC_ZIP"
echo "zagg lambda artifacts: $MINOR ($ARCH), region $REGION"

if [ "$REGION" = "$MIRROR_REGION" ]; then
    # Same region as the mirror — deploy straight from it, no staging bucket.
    echo "Region matches the mirror: deploying directly from s3://$MIRROR_BUCKET/$MIRROR_PREFIX/$MINOR/"
    ARTIFACT_BUCKET="$MIRROR_BUCKET"
    LAYER_S3KEY="$LAYER_KEY"
    FUNC_S3KEY="$FUNC_KEY"
else
    # Different region — CloudFormation needs a same-region bucket, so copy the
    # zips from the mirror into the user's STAGING_BUCKET first.
    if [ -z "$STAGING_BUCKET" ]; then
        echo "ERROR: REGION=$REGION is not the mirror region ($MIRROR_REGION)."
        echo "       CloudFormation reads Lambda code from a SAME-REGION bucket, so the public"
        echo "       mirror can't be used directly here. Provide STAGING_BUCKET (a bucket you"
        echo "       own in $REGION); the zips will be copied into it from the mirror."
        echo "       (Or deploy in us-west-2 to skip staging entirely.)"
        exit 1
    fi
    if ! aws s3api head-bucket --bucket "$STAGING_BUCKET" --region "$REGION" 2>/dev/null; then
        echo "ERROR: staging bucket '$STAGING_BUCKET' not found/accessible in $REGION."
        echo "       Create it first:  aws s3 mb s3://$STAGING_BUCKET --region $REGION"
        exit 1
    fi
    TMPDIR="$(mktemp -d)"; trap 'rm -rf "$TMPDIR"' EXIT
    echo "Copying zips from the mirror into s3://$STAGING_BUCKET/ ..."
    run aws s3 cp "s3://$MIRROR_BUCKET/$LAYER_KEY" "$TMPDIR/$LAYER_ZIP" --region "$MIRROR_REGION" --no-sign-request
    run aws s3 cp "s3://$MIRROR_BUCKET/$FUNC_KEY"  "$TMPDIR/$FUNC_ZIP"  --region "$MIRROR_REGION" --no-sign-request
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
