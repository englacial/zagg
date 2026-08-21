#!/bin/bash
# Publish the release Lambda zips to the public distribution bucket (issue #25),
# keyed by zagg MINOR version (0.N.x -> 0.N), with a per-minor SHA256SUMS and a
# versions.json index at the destination root. stand_up.sh lists/reads
# versions.json to resolve "latest" instead of being hard-pinned to whatever was
# current at pip-install time. Idempotent: re-running a release overwrites that
# minor's objects.
#
# Since issue #497 the destination is Source Cooperative, not the in-account
# `sliderule-public-cors` bucket: that bucket cannot host public data under
# NASA's clearance posture and is being retired (issue #499). --prefix produces
# publish_mirror.sh's layout (<prefix>/<minor>/<zip>), and stand_up.sh already
# reads it via DIST_PREFIX, so the write side and the read side match.
#
# Usage:
#   distribute_zips.sh --minor 0.2 --tag 0.2.3 \
#       --bucket us-west-2.opendata.source.coop --prefix englacial/zagg/lambda \
#       --dir ./zips [--region us-west-2]
#
# --dir holds the four release zips (lambda_layer_{arm64,x86_64}.zip,
# lambda_function_{arm64,x86_64}_*.zip). Requires: aws CLI (write creds in env),
# python3, sha256sum.
set -euo pipefail

MINOR="" TAG="" BUCKET="" PREFIX="" DIR="" REGION="us-west-2"
while [ $# -gt 0 ]; do
  case "$1" in
    --minor) MINOR="$2"; shift 2 ;;
    --tag) TAG="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --prefix) PREFIX="${2%/}"; shift 2 ;;
    --dir) DIR="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
: "${MINOR:?--minor required}" "${BUCKET:?--bucket required}" "${DIR:?--dir required}"

BASE="s3://$BUCKET${PREFIX:+/$PREFIX}"

# Source Cooperative requires every object be handed to the bucket owner (issue
# #495 phase 1): without x-amz-acl: bucket-owner-full-control the first PUT is
# AccessDenied. Keyed on the DESTINATION bucket, mirroring
# zagg.store._PUBLISHED_BUCKETS on the fleet's side -- the header is not sent to
# buckets we own, and the release role holds s3:PutObjectAcl on the published
# destination only (benchmark_cicd.yaml, issue #497), so sending it
# unconditionally would 403 against the in-account dist bucket. Expanded
# unquoted (fixed flags, no spaces) so the empty default vanishes -- same idiom
# as stand_up.sh's DIST_SIGN, since `set -u` + bash 3.2 rejects empty arrays.
PUBLISHED_BUCKETS="us-west-2.opendata.source.coop"   # == zagg.store._PUBLISHED_BUCKETS
ACL=""
case " $PUBLISHED_BUCKETS " in
  *" $BUCKET "*) ACL="--acl bucket-owner-full-control" ;;
esac

# A published bucket's root is another organization's namespace, and it is
# outside the release role's grant, so writing there 403s mid-release with no
# explanation. The workflow gates on the prefix too, but that gate is one
# `gh variable set` away from being the only thing standing between a release
# and the bucket root -- hold the invariant here as well, where the keys are
# actually built.
if [ -n "$ACL" ] && [ -z "$PREFIX" ]; then
  echo "refusing to publish to $BUCKET root: --prefix is required for a published bucket" >&2
  exit 2
fi

shopt -s nullglob
# All four are globs (note the trailing * on the layer names) so nullglob drops
# any that's missing -- the count check then catches it here, not at `aws s3 cp`.
zips=("$DIR"/lambda_layer_arm64*.zip "$DIR"/lambda_layer_x86_64*.zip \
      "$DIR"/lambda_function_arm64_*.zip "$DIR"/lambda_function_x86_64_*.zip)
if [ "${#zips[@]}" -ne 4 ]; then
  echo "expected 4 zips in $DIR, found ${#zips[@]}: ${zips[*]:-none}" >&2
  exit 1
fi

for z in "${zips[@]}"; do
  aws s3 cp "$z" "$BASE/$MINOR/$(basename "$z")" --region "$REGION" $ACL
done

( cd "$DIR" && sha256sum lambda_layer_*.zip lambda_function_*.zip > SHA256SUMS )
aws s3 cp "$DIR/SHA256SUMS" "$BASE/$MINOR/SHA256SUMS" --region "$REGION" $ACL

# Merge this minor into the root index (read-modify-write; absent => seed). The
# read must be able to tell "not published yet" from "denied": the release role
# holds an UNCONDITIONED s3:ListBucket on the destination (issue #497) so an
# absent key answers 404 rather than 403 -- otherwise a permissions fault would
# reseed the index here and silently drop every published minor from it.
aws s3 cp "$BASE/versions.json" ./versions.json --region "$REGION" 2>/dev/null \
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
aws s3 cp ./versions.json "$BASE/versions.json" --region "$REGION" $ACL

echo "distributed minor $MINOR (tag ${TAG:-n/a}) -> $BASE/$MINOR/"
