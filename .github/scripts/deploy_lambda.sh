#!/bin/bash
# Update a Lambda function in place from freshly-built zips (issue #25): publish a
# new layer version from an S3-staged layer zip, point the function at it, then
# update the function code. publish-layer-version (not a bare update-function-code)
# is required so a deps/layer change is actually picked up, and the layer is read
# from S3 because the zip can exceed Lambda's 50 MB direct-upload cap.
#
# Shared by the release path (publish.yml -> production) and the benchmark
# test-deploy path (lambda-benchmark.yml -> process-shard-test).
#
# Usage:
#   deploy_lambda.sh --function NAME --layer-bucket B --layer-key K \
#       --function-zip PATH --region R
#
# Requires: aws CLI (creds in env). arm64 / python3.12 only (the deployed target).
set -euo pipefail

FUNCTION="" LAYER_BUCKET="" LAYER_KEY="" FUNCTION_ZIP="" REGION=""
while [ $# -gt 0 ]; do
  case "$1" in
    --function) FUNCTION="$2"; shift 2 ;;
    --layer-bucket) LAYER_BUCKET="$2"; shift 2 ;;
    --layer-key) LAYER_KEY="$2"; shift 2 ;;
    --function-zip) FUNCTION_ZIP="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
: "${FUNCTION:?--function required}" "${LAYER_BUCKET:?--layer-bucket required}" \
  "${LAYER_KEY:?--layer-key required}" "${FUNCTION_ZIP:?--function-zip required}" \
  "${REGION:?--region required}"

LAYER_ARN=$(aws lambda publish-layer-version \
  --layer-name "${FUNCTION}-deps" \
  --content "S3Bucket=${LAYER_BUCKET},S3Key=${LAYER_KEY}" \
  --compatible-architectures arm64 \
  --compatible-runtimes python3.12 \
  --region "$REGION" \
  --query LayerVersionArn --output text)

# Config update (new layer) and code update can't overlap -- wait between them.
aws lambda update-function-configuration \
  --function-name "$FUNCTION" --layers "$LAYER_ARN" --region "$REGION"
aws lambda wait function-updated --function-name "$FUNCTION" --region "$REGION"
aws lambda update-function-code \
  --function-name "$FUNCTION" --zip-file "fileb://${FUNCTION_ZIP}" --publish --region "$REGION"

# Async-invoke hygiene (issue #151): the runner dispatches with
# InvocationType=Event and polls for worker-written results; Lambda's async
# defaults would re-run a timed-out/OOM'd shard twice with delays, so pin
# retries to 0, and keep event age under the runner's 90 s poll margin so no
# delivery starts after the runner stops listening (mirrors
# ProcessFnAsyncConfig in deployment/aws/template.yaml -- keep in sync).
# Warn-only: the deploy role may not yet carry
# lambda:PutFunctionEventInvokeConfig, and the pipeline still works (just
# noisier on worker crashes) without it.
aws lambda put-function-event-invoke-config \
  --function-name "$FUNCTION" --maximum-retry-attempts 0 \
  --maximum-event-age-in-seconds 60 --region "$REGION" \
  || echo "WARN: could not set event-invoke config on $FUNCTION (needs lambda:PutFunctionEventInvokeConfig); async service retries stay at the default" >&2

echo "deployed $FUNCTION (layer $LAYER_ARN)"
