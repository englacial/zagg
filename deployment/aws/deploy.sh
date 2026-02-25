#!/bin/bash
# Deploy Lambda artifacts to AWS from the latest CI build.
#
# Usage:
#   ./deploy.sh                    # deploy layer + function (arm64, default)
#   ./deploy.sh --arch x86_64      # deploy x86_64 instead
#   ./deploy.sh --function-only    # skip layer publish
#   ./deploy.sh --dry-run          # show what would happen
#
# Requires: aws CLI, gh CLI (authenticated)

set -e

FUNCTION_NAME="process-morton-cell"
REGION="us-west-2"
ARCH="arm64"
FUNCTION_ONLY=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --function-only) FUNCTION_ONLY=true ;;
        --dry-run)       DRY_RUN=true ;;
        x86_64)          ARCH="x86_64" ;;
        arm64)           ARCH="arm64" ;;
    esac
done

case "$ARCH" in
    arm64)  RUNTIME="python3.12"; LAYER_NAME="magg-deps-arm64" ;;
    x86_64) RUNTIME="python3.11"; LAYER_NAME="magg-deps-x86_64" ;;
esac

echo "============================================================"
echo "Lambda Deploy"
echo "  Function: $FUNCTION_NAME | Arch: $ARCH | Runtime: $RUNTIME"
echo "============================================================"

# Download latest CI artifacts
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo ""
echo "Downloading artifacts from latest CI run..."
gh run download --name "lambda-layer-${ARCH}" --dir "$TMPDIR/layer" 2>&1 || {
    echo "ERROR: Could not download layer artifact. Is there a successful Lambda Build run?"
    exit 1
}
gh run download --name "lambda-function-${ARCH}" --dir "$TMPDIR/function" 2>&1 || {
    echo "ERROR: Could not download function artifact."
    exit 1
}

LAYER_ZIP=$(ls "$TMPDIR"/layer/*.zip 2>/dev/null | head -1)
FUNC_ZIP=$(ls "$TMPDIR"/function/*.zip 2>/dev/null | head -1)

echo "  Layer:    ${LAYER_ZIP##*/} ($(du -h "$LAYER_ZIP" | cut -f1))"
echo "  Function: ${FUNC_ZIP##*/} ($(du -h "$FUNC_ZIP" | cut -f1))"

# --- Publish layer ---
if [ "$FUNCTION_ONLY" = false ]; then
    echo ""
    echo "Publishing layer: $LAYER_NAME"
    if [ "$DRY_RUN" = true ]; then
        echo "  [DRY RUN] aws lambda publish-layer-version --layer-name $LAYER_NAME ..."
    else
        LAYER_ARN=$(aws lambda publish-layer-version \
            --layer-name "$LAYER_NAME" \
            --compatible-runtimes "$RUNTIME" \
            --compatible-architectures "$ARCH" \
            --zip-file "fileb://$LAYER_ZIP" \
            --region "$REGION" \
            --query 'LayerVersionArn' --output text)
        echo "  Published: $LAYER_ARN"
    fi
fi

# --- Deploy function code ---
echo ""
echo "Deploying function code..."
if [ "$DRY_RUN" = true ]; then
    echo "  [DRY RUN] aws lambda update-function-code ..."
else
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$FUNC_ZIP" \
        --architectures "$ARCH" \
        --region "$REGION" \
        --query '{CodeSize:CodeSize,LastModified:LastModified}' --output table

    echo "  Waiting for update..."
    aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"

    # Update runtime + layer if we published one
    if [ "$FUNCTION_ONLY" = false ] && [ -n "$LAYER_ARN" ]; then
        echo "  Updating configuration..."
        aws lambda update-function-configuration \
            --function-name "$FUNCTION_NAME" \
            --runtime "$RUNTIME" \
            --layers "$LAYER_ARN" \
            --region "$REGION" \
            --query '{Runtime:Runtime,Arch:Architectures[0],Layers:Layers[*].Arn}' --output table
    fi
fi

# --- Verify ---
if [ "$DRY_RUN" = false ]; then
    echo ""
    aws lambda get-function-configuration \
        --function-name "$FUNCTION_NAME" --region "$REGION" \
        --query '{Runtime:Runtime,Arch:Architectures[0],CodeSize:CodeSize,LastModified:LastModified,Layers:Layers[*].Arn}' \
        --output table
fi

echo ""
echo "Done!"
