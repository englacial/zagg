#!/bin/bash
# Mirror the CI-built Lambda zips to the public source.coop bucket, keyed by
# zagg MINOR version. Run by a maintainer after a release (with write creds to
# the mirror bucket in the environment or an aws profile).
#
# Layout produced:
#   <prefix>/<minor>/lambda_layer_{arm64,x86_64}.zip
#   <prefix>/<minor>/lambda_function_{arm64,x86_64}_py312.zip
#   <prefix>/<minor>/SHA256SUMS
#   <prefix>/<minor>/README.md
#
# Usage:
#   ./publish_mirror.sh 0.2                  # latest successful Lambda Build run
#   ./publish_mirror.sh 0.2 --run 27312613736
#   ./publish_mirror.sh 0.2 --dir ./zips     # local dir holding the 4 zips
#   ./publish_mirror.sh 0.2 --creds sc.json  # source.coop write creds from JSON
#
# --creds points at a JSON file of source.coop write credentials (so they don't
# have to be exported by hand). It accepts the same AWS key spellings the rest of
# zagg does — camelCase (accessKeyId), boto snake_case (aws_access_key_id), and
# STS PascalCase (AccessKeyId) — and exports them for the aws CLI. Without it the
# aws CLI falls back to the ambient environment / profile as before. Flags may be
# combined, e.g. `--dir ./zips --creds sc.json`.
#
# Requires: aws CLI (write creds to MIRROR_BUCKET); gh (unless --dir); python3
# (only when --creds is used).

set -euo pipefail

MINOR="${1:?usage: publish_mirror.sh <minor> [--run <id> | --dir <dir>] [--creds <json>]}"; shift || true

REPO="${MIRROR_SRC_REPO:-englacial/zagg}"
MIRROR_BUCKET="${MIRROR_BUCKET:-us-west-2.opendata.source.coop}"
MIRROR_PREFIX="${MIRROR_PREFIX:-englacial/zagg/lambda}"
MIRROR_REGION="${MIRROR_REGION:-us-west-2}"

# Source.coop repo root (one level up from the lambda artifacts), where the
# version-independent LICENSE + index README live.
MIRROR_REPO_PREFIX="${MIRROR_REPO_PREFIX:-${MIRROR_PREFIX%/lambda}}"
REPO_TOP="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel 2>/dev/null || true)"

RUN_ID=""; ZIP_DIR=""; CREDS_FILE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --run) RUN_ID="${2:?--run needs a run id}"; shift 2 ;;
        --dir) ZIP_DIR="${2:?--dir needs a directory}"; shift 2 ;;
        --creds) CREDS_FILE="${2:?--creds needs a json file}"; shift 2 ;;
        *) echo "ERROR: unknown argument '$1'"; exit 1 ;;
    esac
done

# Optional: load source.coop write credentials from a JSON file so they don't
# have to be exported by hand. Accepts the common AWS key spellings (camelCase,
# boto snake_case, STS PascalCase), matching zagg's own credential handling, and
# exports them for the aws CLI calls below. sessionToken is optional.
if [ -n "$CREDS_FILE" ]; then
    [ -f "$CREDS_FILE" ] || { echo "ERROR: --creds file not found: $CREDS_FILE" >&2; exit 1; }
    CREDS_EXPORTS="$(python3 - "$CREDS_FILE" <<'PY'
import json, shlex, sys

with open(sys.argv[1]) as fh:
    data = json.load(fh)

ALIASES = {
    "AWS_ACCESS_KEY_ID":     ("accessKeyId", "aws_access_key_id", "AccessKeyId"),
    "AWS_SECRET_ACCESS_KEY": ("secretAccessKey", "aws_secret_access_key", "SecretAccessKey"),
    "AWS_SESSION_TOKEN":     ("sessionToken", "aws_session_token", "SessionToken"),
}

def pick(keys):
    for k in keys:
        v = data.get(k)
        if v:
            return str(v)
    return None

resolved = {env: pick(keys) for env, keys in ALIASES.items()}
missing = [e for e in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not resolved[e]]
if missing:
    sys.stderr.write("ERROR: --creds JSON missing %s\n" % ", ".join(missing))
    sys.exit(1)

for env, val in resolved.items():
    if val:
        print("export %s=%s" % (env, shlex.quote(val)))
PY
    )" || { echo "ERROR: could not load credentials from $CREDS_FILE" >&2; exit 1; }
    eval "$CREDS_EXPORTS"
    echo "Loaded source.coop credentials from $CREDS_FILE"
fi

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
STAGE="$WORK/$MINOR"; mkdir -p "$STAGE"

if [ -n "$ZIP_DIR" ]; then
    cp "$ZIP_DIR"/lambda_layer_*.zip "$ZIP_DIR"/lambda_function_*.zip "$STAGE/"
    PROV="local directory $ZIP_DIR"
else
    if [ -z "$RUN_ID" ]; then
        RUN_ID="$(gh run list --repo "$REPO" --workflow 'Lambda Build' --limit 20 \
            --json databaseId,conclusion --jq '[.[]|select(.conclusion=="success")][0].databaseId')"
    fi
    echo "Pulling zips from Lambda Build run $RUN_ID ($REPO)..."
    gh run download "$RUN_ID" --repo "$REPO" --dir "$WORK/dl"
    find "$WORK/dl" -name '*.zip' -exec cp {} "$STAGE/" \;
    SHA="$(gh api "repos/$REPO/actions/runs/$RUN_ID" --jq .head_sha)"
    PROV="Lambda Build run $RUN_ID, commit $SHA"
fi

# Require all four arch zips.
for z in lambda_layer_arm64.zip lambda_function_arm64_py312.zip \
         lambda_layer_x86_64.zip lambda_function_x86_64_py312.zip; do
    [ -f "$STAGE/$z" ] || { echo "ERROR: missing $z"; exit 1; }
done

# Portable checksum: Linux ships sha256sum, macOS ships shasum.
if command -v sha256sum >/dev/null 2>&1; then
    ( cd "$STAGE" && sha256sum *.zip > SHA256SUMS )
else
    ( cd "$STAGE" && shasum -a 256 *.zip > SHA256SUMS )
fi
cat > "$STAGE/README.md" <<MD
# zagg lambda artifacts — $MINOR

For the zagg \`$MINOR.x\` release line.

- **Source:** $PROV
- **Mirrored:** $(date -u +%Y-%m-%d)
- **Runtime:** python3.12, manylinux_2_28
- **Layer deps:** numpy, pandas, fastparquet, cramjam, shapely, pyproj, odc-geo,
  affine, cachetools, h5coro, mortie. (earthaccess is orchestrator-only and NOT
  in the layer; zarr/obstore/pydantic-zarr ship in the function code.)
- **Verify:** \`sha256sum -c SHA256SUMS\` (\`shasum -a 256 -c SHA256SUMS\` on macOS)
- **License:** MIT — see \`LICENSE\` at the repository root.

zagg benchmark outputs will also be hosted in this source.coop repository.
MD

DEST="s3://$MIRROR_BUCKET/$MIRROR_PREFIX/$MINOR"
echo "Uploading to $DEST/ ..."
for z in "$STAGE"/*.zip; do
    aws s3 cp "$z" "$DEST/$(basename "$z")" --region "$MIRROR_REGION"
done
aws s3 cp "$STAGE/SHA256SUMS" "$DEST/SHA256SUMS" --region "$MIRROR_REGION" --content-type text/plain
aws s3 cp "$STAGE/README.md"  "$DEST/README.md"  --region "$MIRROR_REGION" --content-type text/markdown

# --- Repo-root metadata (version-independent): code license + index README ---
REPO_DEST="s3://$MIRROR_BUCKET/$MIRROR_REPO_PREFIX"
if [ -n "$REPO_TOP" ] && [ -f "$REPO_TOP/LICENSE" ]; then
    echo "Publishing repo-root LICENSE + index to $REPO_DEST/ ..."
    aws s3 cp "$REPO_TOP/LICENSE" "$REPO_DEST/LICENSE" --region "$MIRROR_REGION" --content-type text/plain
    cat > "$WORK/REPO_README.md" <<MD
# zagg public artifacts

Build artifacts and benchmark outputs for
[zagg](https://github.com/englacial/zagg).

- **Code license:** MIT — see [\`LICENSE\`](./LICENSE) in this repository.
- **\`lambda/\`** — prebuilt AWS Lambda layer + function zips, keyed by zagg minor
  version (e.g. \`lambda/$MINOR/\`). Consumed by \`deployment/aws/stand_up.sh\`.
- **\`benchmarks/\`** — zagg benchmark outputs (forthcoming).

Last updated: $(date -u +%Y-%m-%d).
MD
    aws s3 cp "$WORK/REPO_README.md" "$REPO_DEST/README.md" --region "$MIRROR_REGION" --content-type text/markdown
else
    echo "WARNING: repo LICENSE not found (REPO_TOP='$REPO_TOP'); skipping repo-root metadata."
fi

echo ""
echo "Done. Mirror updated:"
aws s3 ls "$DEST/" --region "$MIRROR_REGION"
