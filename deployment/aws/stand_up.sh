#!/bin/bash
# Stand up the zagg Lambda backend in your own AWS account, end to end.
#
# Downloads the pre-built layer + function zips from a GitHub Release, stages
# them in a same-region S3 bucket (CloudFormation requires Lambda code to live
# in a same-region bucket), then deploys template.yaml — creating the IAM role,
# dependency layer, and the Lambda function.
#
# Usage:
#   OUTPUT_BUCKET=my-results-bucket ./stand_up.sh
#   OUTPUT_BUCKET=my-bucket CREATE_BUCKET=true REGION=us-east-1 ./stand_up.sh
#
# Requires: aws CLI (configured), curl.

set -euo pipefail

# --- Override knobs (the "hosted zip path" lives here) ----------------------
REPO="${ZAGG_REPO:-englacial/zagg}"
RELEASE_TAG="${RELEASE_TAG:-latest}"                 # GitHub Release to pull zips from
ARTIFACT_BASE_URL="${ARTIFACT_BASE_URL:-}"           # set to self-host the zips instead of a Release
ARCH="${ARCH:-arm64}"                                # arm64 | x86_64
STACK_NAME="${STACK_NAME:-zagg-backend}"
REGION="${REGION:-us-west-2}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:?Set OUTPUT_BUCKET to the bucket where results go}"
CREATE_BUCKET="${CREATE_BUCKET:-false}"              # true => stack creates OUTPUT_BUCKET
STAGING_BUCKET="${STAGING_BUCKET:-$OUTPUT_BUCKET}"   # where the zips are uploaded for deploy

case "$ARCH" in
    arm64)  LAYER_ZIP="lambda_layer_arm64.zip";  FUNC_ZIP="lambda_function_arm64_py312.zip" ;;
    x86_64) LAYER_ZIP="lambda_layer_x86_64.zip"; FUNC_ZIP="lambda_function_x86_64_py311.zip" ;;
    *) echo "ERROR: ARCH must be arm64 or x86_64 (got '$ARCH')"; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# --- 1. Download the hosted zips -------------------------------------------
if [ -n "$ARTIFACT_BASE_URL" ]; then
    LAYER_URL="$ARTIFACT_BASE_URL/$LAYER_ZIP"
    FUNC_URL="$ARTIFACT_BASE_URL/$FUNC_ZIP"
elif [ "$RELEASE_TAG" = "latest" ]; then
    LAYER_URL="https://github.com/$REPO/releases/latest/download/$LAYER_ZIP"
    FUNC_URL="https://github.com/$REPO/releases/latest/download/$FUNC_ZIP"
else
    LAYER_URL="https://github.com/$REPO/releases/download/$RELEASE_TAG/$LAYER_ZIP"
    FUNC_URL="https://github.com/$REPO/releases/download/$RELEASE_TAG/$FUNC_ZIP"
fi

echo "Downloading artifacts ($ARCH):"
echo "  $LAYER_URL"
curl -fsSL "$LAYER_URL" -o "$TMPDIR/$LAYER_ZIP"
echo "  $FUNC_URL"
curl -fsSL "$FUNC_URL" -o "$TMPDIR/$FUNC_ZIP"

# --- 2. Ensure the staging bucket exists in REGION -------------------------
if ! aws s3api head-bucket --bucket "$STAGING_BUCKET" --region "$REGION" 2>/dev/null; then
    if [ "$CREATE_BUCKET" = "true" ]; then
        echo "Creating bucket $STAGING_BUCKET in $REGION..."
        if [ "$REGION" = "us-east-1" ]; then
            aws s3api create-bucket --bucket "$STAGING_BUCKET" --region "$REGION"
        else
            aws s3api create-bucket --bucket "$STAGING_BUCKET" --region "$REGION" \
                --create-bucket-configuration "LocationConstraint=$REGION"
        fi
    else
        echo "ERROR: bucket '$STAGING_BUCKET' not found/accessible in $REGION."
        echo "       Pre-create it, or re-run with CREATE_BUCKET=true."
        exit 1
    fi
fi

# --- 3. Stage the zips ------------------------------------------------------
echo "Uploading zips to s3://$STAGING_BUCKET/ ..."
aws s3 cp "$TMPDIR/$LAYER_ZIP" "s3://$STAGING_BUCKET/$LAYER_ZIP" --region "$REGION"
aws s3 cp "$TMPDIR/$FUNC_ZIP"  "s3://$STAGING_BUCKET/$FUNC_ZIP"  --region "$REGION"

# --- 4. Deploy the stack ----------------------------------------------------
echo "Deploying stack $STACK_NAME..."
aws cloudformation deploy \
    --template-file "$SCRIPT_DIR/template.yaml" \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        Architecture="$ARCH" \
        ArtifactBucket="$STAGING_BUCKET" \
        LayerS3Key="$LAYER_ZIP" \
        FunctionS3Key="$FUNC_ZIP" \
        OutputBucketName="$OUTPUT_BUCKET" \
        CreateOutputBucket="$CREATE_BUCKET"

# --- 5. Show outputs --------------------------------------------------------
echo ""
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query 'Stacks[0].Outputs' --output table

echo ""
echo "Done. Backend stood up as stack '$STACK_NAME' in $REGION."
