#!/bin/bash
# Publish the release Lambda zips to the public distribution bucket (issue #25),
# keyed by zagg MINOR version (0.N.x -> 0.N), with a per-minor SHA256SUMS and a
# top-level versions.json index. stand_up.sh lists/reads versions.json to resolve
# "latest" instead of being hard-pinned to whatever was current at pip-install
# time. Idempotent: re-running a release overwrites that minor's objects.
#
# Usage:
#   distribute_zips.sh --minor 0.2 --tag 0.2.3 --bucket sliderule-public-cors \
#       --dir ./zips [--region us-west-2]
#
# --dir holds the four release zips (lambda_layer_{arm64,x86_64}.zip,
# lambda_function_{arm64,x86_64}_*.zip). Requires: aws CLI (write creds in env),
# python3, sha256sum.
set -euo pipefail

MINOR="" TAG="" BUCKET="" DIR="" REGION="us-west-2"
while [ $# -gt 0 ]; do
  case "$1" in
    --minor) MINOR="$2"; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --dir) DIR="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
: "${MINOR:?--minor required}" "${BUCKET:?--bucket required}" "${DIR:?--dir required}"

shopt -s nullglob
zips=("$DIR"/lambda_layer_arm64.zip "$DIR"/lambda_layer_x86_64.zip \
      "$DIR"/lambda_function_arm64_*.zip "$DIR"/lambda_function_x86_64_*.zip)
if [ "${#zips[@]}" -ne 4 ]; then
  echo "expected 4 zips in $DIR, found ${#zips[@]}: ${zips[*]:-none}" >&2
  exit 1
fi

for z in "${zips[@]}"; do
  aws s3 cp "$z" "s3://$BUCKET/$MINOR/$(basename "$z")" --region "$REGION"
done

( cd "$DIR" && sha256sum lambda_layer_*.zip lambda_function_*.zip > SHA256SUMS )
aws s3 cp "$DIR/SHA256SUMS" "s3://$BUCKET/$MINOR/SHA256SUMS" --region "$REGION"

# Merge this minor into the top-level index (read-modify-write; absent => seed).
aws s3 cp "s3://$BUCKET/versions.json" ./versions.json --region "$REGION" 2>/dev/null \
  || echo '{"minors": []}' > versions.json
python3 - "$MINOR" "$TAG" <<'PY'
import json, sys
minor, tag = sys.argv[1], sys.argv[2]
d = json.load(open("versions.json"))
minors = set(d.get("minors", [])) | {minor}
ordered = sorted(minors, key=lambda m: tuple(int(x) for x in m.split(".")))
d["minors"] = ordered
d["latest"] = ordered[-1]
if tag:
    d["latest_tag"] = tag
json.dump(d, open("versions.json", "w"), indent=2)
PY
aws s3 cp ./versions.json "s3://$BUCKET/versions.json" --region "$REGION"

echo "distributed minor $MINOR (tag ${TAG:-n/a}) -> s3://$BUCKET/$MINOR/"
